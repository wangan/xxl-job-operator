package controller

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

const ExecutorFinalizer = "xxljob.io/executor-finalizer"

// XxlJobExecutorReconciler reconciles a XxlJobExecutor object.
type XxlJobExecutorReconciler struct {
	client.Client
	APIReader                client.Reader
	Scheme                   *runtime.Scheme
	ClientFactory            ClientFactory
	DefaultProxyExecutorAddr string
}

// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobexecutors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobexecutors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobexecutors/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;services;endpoints;namespaces,verbs=get;list;watch

func (r *XxlJobExecutorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var execCR xxljobv1alpha1.XxlJobExecutor
	if err := r.Get(ctx, req.NamespacedName, &execCR); err != nil {
		if apierrors.IsNotFound(err) {
			cleanupExecutorMetrics(req.Namespace, req.Name)
			purgeExecutorTokenCache(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	// Handle Deletion via Finalizer (fixes P0-3, P0-6, P1-2, R4-P1-3, and R5-P2-6)
	if !execCR.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&execCR, ExecutorFinalizer) {
			forceOrNsTerminating := shouldBypassDependencyCheck(ctx, reader, &execCR)
			if !forceOrNsTerminating {
				taskCount, err := r.countReferencingTasks(ctx, &execCR)
				if err != nil {
					return ctrl.Result{}, err
				}
				if taskCount > 0 {
					logger.Info("Delaying XxlJobExecutor deletion until referencing XxlJobTasks are removed", "dependentTasks", taskCount)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}

			bypassRemote := shouldBypassRemoteCleanup(ctx, reader, &execCR)
			adminCR, xxlCli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory, execCR.Spec.AdminRef, execCR.Namespace)
			if err != nil {
				if execCR.Status.AdminURL != "" {
					_ = r.cleanupExecutorOnPreviousAdmin(ctx, reader, &execCR, execCR.Status.AdminURL, forceOrNsTerminating, bypassRemote)
				}
				if !bypassRemote {
					logger.Error(err, "Unable to resolve XxlJobAdmin during Executor deletion; add annotation xxljob.io/force-delete=true to bypass")
					return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
				}
				logger.Info("Bypassing remote JobGroup cleanup because namespace is terminating or force-delete/timeout is active")
			} else {
				currentAdminURL := ResolveAdminURL(adminCR)
				sameAdmin := execCR.Status.AdminURL == "" ||
					strings.TrimRight(execCR.Status.AdminURL, "/") == strings.TrimRight(currentAdminURL, "/")
				if !sameAdmin {
					if prevErr := r.cleanupExecutorOnPreviousAdmin(ctx, reader, &execCR, execCR.Status.AdminURL, forceOrNsTerminating, bypassRemote); prevErr != nil {
						logger.Error(prevErr, "Failed to clean up executor on previous XxlJobAdmin during deletion, will retry", "recordedAdminURL", execCR.Status.AdminURL)
						return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
					}
				}
				if cleanErr := r.cleanupExecutorOnAdmin(ctx, xxlCli, &execCR, sameAdmin, forceOrNsTerminating, bypassRemote); cleanErr != nil {
					r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Remove JobGroup rejected by XXL-JOB Admin: %v; remove remaining tasks or set annotation xxljob.io/force-delete=true", cleanErr))
					logger.Error(cleanErr, "Failed to remove JobGroup from XXL-JOB Admin, will retry", "jobGroupId", execCR.Status.JobGroupID)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}

			controllerutil.RemoveFinalizer(&execCR, ExecutorFinalizer)
			if err := r.Update(ctx, &execCR); err != nil {
				return ctrl.Result{}, err
			}
			cleanupExecutorMetrics(req.Namespace, req.Name)
			purgeExecutorTokenCache(req.Namespace, req.Name)
		}
		return ctrl.Result{}, nil
	}

	// Enforce same-namespace adminRef (fixes P1-7)
	if execCR.Spec.AdminRef.Namespace != "" && execCR.Spec.AdminRef.Namespace != execCR.Namespace {
		r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Cross-namespace adminRef (%s/%s) is not allowed", execCR.Spec.AdminRef.Namespace, execCR.Spec.AdminRef.Name))
		return ctrl.Result{}, nil
	}

	// Validate proxyServiceAddress scheme, host, and empty/root path (fixes R4-P0-2 & R5-P2-7)
	if strings.TrimSpace(execCR.Spec.ProxyServiceAddress) != "" {
		if err := validateProxyServiceBaseURL(execCR.Spec.ProxyServiceAddress); err != nil {
			r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Invalid proxyServiceAddress: %v", err))
			return ctrl.Result{}, nil
		}
	}

	if !controllerutil.ContainsFinalizer(&execCR, ExecutorFinalizer) {
		controllerutil.AddFinalizer(&execCR, ExecutorFinalizer)
		if err := r.Update(ctx, &execCR); err != nil {
			return ctrl.Result{}, err
		}
	}

	// If the executor is migrating across different XxlJobAdmin instances, deregister old endpoints and remove
	// the old JobGroup on the previous Admin BEFORE resolving/validating the new target Admin so missing/broken
	// new Admin configs never leave stale registrations on the old Admin or deadlock old Admin deletion.
	prevAdminURL := strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
	prevAdminCleaned := false
	migratingPrevAdmin, migCheckErr := isMigratingFromPreviousAdmin(ctx, reader, execCR.Namespace, execCR.Spec.AdminRef.Name, prevAdminURL)
	if migCheckErr != nil {
		return ctrl.Result{}, migCheckErr
	}
	if migratingPrevAdmin {
		if cleanErr := r.cleanupExecutorOnPreviousAdmin(ctx, reader, &execCR, prevAdminURL, false, false); cleanErr != nil {
			logger.Error(cleanErr, "Failed to clean up executor on previous XxlJobAdmin during cross-Admin migration, will retry",
				"previousAdminURL", prevAdminURL, "previousJobGroupId", execCR.Status.JobGroupID)
			r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Failed to clean up executor on previous XxlJobAdmin (%s): %v", prevAdminURL, cleanErr))
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		prevAdminCleaned = clearPreviousAdminStatus(&execCR)
	}

	adminCR, xxlCli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory, execCR.Spec.AdminRef, execCR.Namespace)
	if err != nil {
		r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Failed to resolve XxlJobAdmin: %v", err), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	adminURL := ResolveAdminURL(adminCR)

	// In K8sJobProxy mode, require an explicit non-default accessToken on XxlJobAdmin for tenant isolation (fixes R4-P0-2)
	if execCR.Spec.Mode == xxljobv1alpha1.ExecutorModeK8sJobProxy {
		if _, err := ResolveAdminExplicitAccessToken(ctx, reader, adminCR); err != nil {
			r.updateErrorStatus(ctx, &execCR, err.Error(), prevAdminCleaned)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	isCrossAdminMigration := migratingPrevAdmin || (prevAdminURL != "" && prevAdminURL != strings.TrimRight(adminURL, "/"))
	if !migratingPrevAdmin && isCrossAdminMigration {
		if cleanErr := r.cleanupExecutorOnPreviousAdmin(ctx, reader, &execCR, prevAdminURL, false, false); cleanErr != nil {
			logger.Error(cleanErr, "Failed to clean up executor on previous XxlJobAdmin during cross-Admin migration, will retry",
				"previousAdminURL", prevAdminURL, "previousJobGroupId", execCR.Status.JobGroupID)
			r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Failed to clean up executor on previous XxlJobAdmin (%s): %v", prevAdminURL, cleanErr))
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		prevAdminCleaned = clearPreviousAdminStatus(&execCR)
	}

	port := execCR.Spec.ExecutorPort
	if port <= 0 {
		port = 9999
	}

	var activeAddresses []string
	drainedSet := make(map[string]bool)
	runningDrainedSet := make(map[string]bool)
	registryKeepAliveSet := make(map[string]bool)
	removedSet := make(map[string]bool)
	var registryErrors []string

	prevDrainedSet := make(map[string]bool, len(execCR.Status.DrainedAddresses))
	if !isCrossAdminMigration {
		for _, addr := range execCR.Status.DrainedAddresses {
			prevDrainedSet[addr] = true
		}
	}
	prevRegCondBefore := meta.FindStatusCondition(execCR.Status.Conditions, "RegistryHealthy")
	prevRegUnhealthy := prevRegCondBefore != nil && prevRegCondBefore.Status == metav1.ConditionFalse

	// Default to addressType = 1 (Operator-managed address_list) when PodSelector, ServiceDNS, StaticAddresses,
	// or K8sJobProxy is used so address changes take effect immediately in XXL-JOB Admin without waiting for the
	// 30s JobRegistryMonitorHelper thread or being overwritten by dying Pod heartbeats (fixes P0-3).
	addressType := resolveExecutorAddressType(&execCR)

	proxyBaseAddr := strings.TrimSpace(execCR.Spec.ProxyServiceAddress)
	if proxyBaseAddr == "" {
		proxyBaseAddr = r.DefaultProxyExecutorAddr
	}
	if proxyBaseAddr == "" {
		proxyBaseAddr = "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/"
	}

	switch execCR.Spec.Mode {
	case xxljobv1alpha1.ExecutorModeK8sJobProxy:
		// Stream 3: K8s Native Job Proxy mode with tenant-scoped routing path /proxy/<ns>/<executor>/ (fixes P0-7 & R4-P0-2)
		proxyAddr := buildScopedProxyURL(proxyBaseAddr, execCR.Namespace, execCR.Name)
		activeAddresses = []string{proxyAddr}

	default:
		// Stream 1 (Resident) & Stream 2 (Sidecar)
		if execCR.Spec.UseServiceDNS && execCR.Spec.ServiceName != "" {
			dnsURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/", execCR.Spec.ServiceName, execCR.Namespace, port)
			activeAddresses = []string{dnsURL}
		} else if len(execCR.Spec.StaticAddresses) > 0 {
			for _, a := range execCR.Spec.StaticAddresses {
				activeAddresses = append(activeAddresses, ensureTrailingSlash(a))
			}
			sort.Strings(activeAddresses)
		} else if execCR.Spec.PodSelector != nil {
			selector, err := metav1.LabelSelectorAsSelector(execCR.Spec.PodSelector)
			if err != nil {
				r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("Invalid podSelector: %v", err), prevAdminCleaned)
				return ctrl.Result{}, nil
			}
			var podList corev1.PodList
			if err := r.List(ctx, &podList, client.InNamespace(execCR.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
				if prevAdminCleaned {
					r.updateErrorStatus(ctx, &execCR, fmt.Sprintf("List pods failed: %v", err), true)
				}
				return ctrl.Result{}, err
			}

			for i := range podList.Items {
				pod := &podList.Items[i]
				if isK8sJobProxyManagedPod(pod) {
					continue
				}
				if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
					continue
				}
				if !isPodInjectedWhenRequired(pod, execCR.Spec.Mode) {
					continue
				}
				podAddr := formatPodExecutorURL(pod.Status.PodIP, port)

				// Proactive Graceful Drain (fixes P0-3, R5-P1-5, R6-P1-10, R6-P2-8, R7-P1-8, R8-P1-5):
				// 1. If Pod is Terminating or NotReady, immediately exclude it from activeAddresses so it is
				//    removed from xxl_job_group.address_list right away (zero new triggers routed to it).
				// 2. Under addressType=1, while the Pod is still in PodRunning phase within a bounded drain/keep-alive
				//    window (terminationGracePeriodSeconds+60s for Terminating pods, or <=15m for NotReady pods),
				//    keep its heartbeat in xxl_job_registry (Status.RegistryKeepAliveAddresses) so XXL-JOB's
				//    JobCompleteHelper does NOT falsely mark >10m running tasks as lost or trigger duplicate retries.
				// 3. When the keep-alive window expires under addressType=1 but the Pod is still PodRunning, omit it
				//    from registryKeepAliveSet (stopping Operator heartbeats) without calling RemoveExecutor on every
				//    reconcile to avoid fighting the container's own 30s heartbeat (fixes R7-P1-8). Once the Pod exits
				//    PodRunning or is deleted, prevAll removes it once from xxl_job_registry (fixes R8-P1-5).
				if !pod.DeletionTimestamp.IsZero() || !isPodReady(pod) {
					drainedSet[podAddr] = true
					runningDrainedSet[podAddr] = true
					if addressType == 1 && isPodWithinKeepAliveWindow(pod) {
						registryKeepAliveSet[podAddr] = true
					} else if addressType == 0 {
						removedSet[podAddr] = true
						// Only invoke RemoveExecutor on the transition into DrainedAddresses (or when the previous
						// reconcile recorded a registry error) instead of spamming /api/registryRemove on every reconcile (fixes R10-P2-2).
						if !prevDrainedSet[podAddr] || prevRegUnhealthy {
							if err := xxlCli.RemoveExecutor(ctx, execCR.Spec.AppName, podAddr); err != nil {
								RegistryHeartbeatErrorsTotal.WithLabelValues(execCR.Namespace, execCR.Name).Inc()
								registryErrors = append(registryErrors, err.Error())
							}
						}
					}
					continue
				}

				activeAddresses = append(activeAddresses, podAddr)
			}
			sort.Strings(activeAddresses)
		}
	}

	// Register active addresses and draining-keepalive Pod addresses in xxl_job_registry when backed by
	// K8sJobProxy, PodSelector, or addressType=0 (reinforced by RegistryHeartbeatRunner every 20s)
	// (fixes R4-P0-1, R5-P1-2, R5-P1-5, R6-P2-8, R8-P1-5).
	shouldHeartbeatRegistry := shouldHeartbeatExecutorRegistry(&execCR)
	activeSet := make(map[string]bool, len(activeAddresses))
	for _, addr := range activeAddresses {
		activeSet[addr] = true
		if shouldHeartbeatRegistry {
			if err := xxlCli.RegisterExecutor(ctx, execCR.Spec.AppName, addr); err != nil {
				RegistryHeartbeatErrorsTotal.WithLabelValues(execCR.Namespace, execCR.Name).Inc()
				registryErrors = append(registryErrors, err.Error())
			}
		}
	}
	for addr := range registryKeepAliveSet {
		if !activeSet[addr] && shouldHeartbeatRegistry {
			if err := xxlCli.RegisterExecutor(ctx, execCR.Spec.AppName, addr); err != nil {
				RegistryHeartbeatErrorsTotal.WithLabelValues(execCR.Namespace, execCR.Name).Inc()
				registryErrors = append(registryErrors, err.Error())
			}
		}
	}

	// Deregister Pods that were previously in RegisteredAddresses, DrainedAddresses, or RegistryKeepAliveAddresses
	// and are no longer active or running in the cluster from xxl_job_registry (fixes R5-P1-5, R6-P1-10, R7-P1-8, R8-P1-5, R10-P1-1).
	usesExecutorRegistry := shouldHeartbeatRegistry || (addressType == 0 && execCR.Spec.PodSelector != nil)
	if !isCrossAdminMigration && usesExecutorRegistry && isExecutorAppNameSynced(&execCR) {
		prevAll := append([]string{}, execCR.Status.RegisteredAddresses...)
		prevAll = append(prevAll, execCR.Status.DrainedAddresses...)
		prevAll = append(prevAll, execCR.Status.RegistryKeepAliveAddresses...)
		for _, prevAddr := range prevAll {
			stillRunningUnderManualGroup := addressType == 1 && runningDrainedSet[prevAddr]
			if !activeSet[prevAddr] && !registryKeepAliveSet[prevAddr] && !stillRunningUnderManualGroup && !removedSet[prevAddr] {
				removedSet[prevAddr] = true
				if err := xxlCli.RemoveExecutor(ctx, execCR.Spec.AppName, prevAddr); err != nil {
					RegistryHeartbeatErrorsTotal.WithLabelValues(execCR.Namespace, execCR.Name).Inc()
					registryErrors = append(registryErrors, err.Error())
					// Retain in drainedSet when RemoveExecutor fails transiently so the next reconcile retries removal (fixes R8-P1-5).
					drainedSet[prevAddr] = true
				}
			}
		}
	}

	var drainedAddresses []string
	for addr := range drainedSet {
		drainedAddresses = append(drainedAddresses, addr)
	}
	sort.Strings(drainedAddresses)

	var keepAliveAddresses []string
	for addr := range registryKeepAliveSet {
		keepAliveAddresses = append(keepAliveAddresses, addr)
	}
	sort.Strings(keepAliveAddresses)

	// When addressType == 1 and activeAddresses is empty (e.g., all pods are terminating or NotReady),
	// write the sentinel URL /proxy/_unavailable/ instead of downgrading to addressType=0 so dying pods
	// in xxl_job_registry are NEVER re-admitted by XXL-JOB Admin (fixes N-P0-2).
	addressListForAdmin := strings.Join(activeAddresses, ",")
	if addressType == 1 && addressListForAdmin == "" {
		addressListForAdmin = buildUnavailableSentinelURL(proxyBaseAddr)
	}

	desiredGroup := xxlclient.JobGroup{
		AppName:     execCR.Spec.AppName,
		Title:       execCR.Spec.Title,
		AddressType: addressType,
		AddressList: addressListForAdmin,
	}

	syncedGroup, err := xxlCli.EnsureJobGroup(ctx, desiredGroup)
	if err != nil {
		r.updateErrorStatusWithAddresses(ctx, &execCR, activeAddresses, drainedAddresses, keepAliveAddresses,
			fmt.Sprintf("Sync JobGroup with XXL-JOB Admin failed: %v", err), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// If Spec.AppName changed in-place on the same Admin (producing a new JobGroupID), migrate any owned tasks
	// in the previous JobGroup to syncedGroup.ID and remove the orphaned previous JobGroup before overwriting Status.JobGroupID (fixes R11-P1-1).
	if !isCrossAdminMigration && execCR.Status.JobGroupID > 0 && execCR.Status.JobGroupID != syncedGroup.ID {
		if migErr := r.migrateAndCleanupPreviousJobGroupOnSameAdmin(ctx, xxlCli, &execCR, execCR.Status.JobGroupID, syncedGroup.ID); migErr != nil {
			r.updateErrorStatusWithAddresses(ctx, &execCR, activeAddresses, drainedAddresses, keepAliveAddresses,
				fmt.Sprintf("Failed to migrate/clean up previous JobGroup %d on XXL-JOB Admin: %v", execCR.Status.JobGroupID, migErr))
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	msg := fmt.Sprintf("Synced with XXL-JOB Admin (jobGroupId=%d, mode=%s, active=%d)",
		syncedGroup.ID, execCR.Spec.Mode, len(activeAddresses))

	registryHealthy := len(registryErrors) == 0
	prevRegCond := meta.FindStatusCondition(execCR.Status.Conditions, "RegistryHealthy")
	regCondChanged := prevRegCond == nil ||
		(registryHealthy && prevRegCond.Status != metav1.ConditionTrue) ||
		(!registryHealthy && prevRegCond.Status != metav1.ConditionFalse)

	prevAppCond := meta.FindStatusCondition(execCR.Status.Conditions, "AppNameSynced")
	appCondChanged := prevAppCond == nil || prevAppCond.Status != metav1.ConditionTrue || prevAppCond.Message != execCR.Spec.AppName

	// Only write Status to APIServer if any meaningful field changed (prevents hot loops, fixes P0-1)
	statusChanged := execCR.Status.JobGroupID != syncedGroup.ID ||
		execCR.Status.AdminURL != adminURL ||
		!execCR.Status.Synced ||
		!reflect.DeepEqual(execCR.Status.RegisteredAddresses, activeAddresses) ||
		!reflect.DeepEqual(execCR.Status.DrainedAddresses, drainedAddresses) ||
		!reflect.DeepEqual(execCR.Status.RegistryKeepAliveAddresses, keepAliveAddresses) ||
		execCR.Status.ActivePods != len(activeAddresses) ||
		execCR.Status.TerminatingPodsDrained != len(drainedAddresses) ||
		execCR.Status.Message != msg ||
		regCondChanged ||
		appCondChanged

	if statusChanged {
		now := metav1.Now()
		execCR.Status.JobGroupID = syncedGroup.ID
		execCR.Status.AdminURL = adminURL
		execCR.Status.Synced = true
		execCR.Status.RegisteredAddresses = activeAddresses
		execCR.Status.DrainedAddresses = drainedAddresses
		execCR.Status.RegistryKeepAliveAddresses = keepAliveAddresses
		execCR.Status.ActivePods = len(activeAddresses)
		execCR.Status.TerminatingPodsDrained = len(drainedAddresses)
		execCR.Status.LastSyncTime = &now
		execCR.Status.Message = msg
		meta.SetStatusCondition(&execCR.Status.Conditions, metav1.Condition{
			Type:               "Synced",
			Status:             metav1.ConditionTrue,
			Reason:             "ReconcileSucceeded",
			Message:            msg,
			LastTransitionTime: now,
		})
		meta.SetStatusCondition(&execCR.Status.Conditions, metav1.Condition{
			Type:               "AppNameSynced",
			Status:             metav1.ConditionTrue,
			Reason:             "AppNameSynced",
			Message:            execCR.Spec.AppName,
			LastTransitionTime: now,
		})
		if registryHealthy {
			meta.SetStatusCondition(&execCR.Status.Conditions, metav1.Condition{
				Type:               "RegistryHealthy",
				Status:             metav1.ConditionTrue,
				Reason:             "HeartbeatSucceeded",
				Message:            "All executor registry heartbeats succeeded",
				LastTransitionTime: now,
			})
		} else {
			meta.SetStatusCondition(&execCR.Status.Conditions, metav1.Condition{
				Type:               "RegistryHealthy",
				Status:             metav1.ConditionFalse,
				Reason:             "HeartbeatFailed",
				Message:            strings.Join(registryErrors, "; "),
				LastTransitionTime: now,
			})
		}

		if err := r.Status().Update(ctx, &execCR); err != nil {
			return ctrl.Result{}, err
		}
	}

	if !registryHealthy {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func extractSyncedExecutorAppName(execCR *xxljobv1alpha1.XxlJobExecutor) string {
	if execCR == nil {
		return ""
	}
	if cond := meta.FindStatusCondition(execCR.Status.Conditions, "AppNameSynced"); cond != nil && cond.Status == metav1.ConditionTrue {
		return strings.TrimSpace(cond.Message)
	}
	return ""
}

func isExecutorAppNameSynced(execCR *xxljobv1alpha1.XxlJobExecutor) bool {
	if execCR == nil {
		return false
	}
	syncedAppName := extractSyncedExecutorAppName(execCR)
	if syncedAppName != "" && syncedAppName != execCR.Spec.AppName {
		return false
	}
	return true
}

func clearPreviousAdminStatus(execCR *xxljobv1alpha1.XxlJobExecutor) bool {
	if execCR == nil {
		return false
	}
	changed := execCR.Status.JobGroupID != 0 ||
		execCR.Status.AdminURL != "" ||
		len(execCR.Status.RegisteredAddresses) > 0 ||
		len(execCR.Status.DrainedAddresses) > 0 ||
		len(execCR.Status.RegistryKeepAliveAddresses) > 0 ||
		execCR.Status.ActivePods != 0 ||
		execCR.Status.TerminatingPodsDrained != 0 ||
		meta.FindStatusCondition(execCR.Status.Conditions, "AppNameSynced") != nil
	execCR.Status.JobGroupID = 0
	execCR.Status.AdminURL = ""
	execCR.Status.RegisteredAddresses = nil
	execCR.Status.DrainedAddresses = nil
	execCR.Status.RegistryKeepAliveAddresses = nil
	execCR.Status.ActivePods = 0
	execCR.Status.TerminatingPodsDrained = 0
	meta.RemoveStatusCondition(&execCR.Status.Conditions, "AppNameSynced")
	return changed
}

const maxNotReadyKeepAliveDuration = 15 * time.Minute

func isPodWithinKeepAliveWindow(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if !pod.DeletionTimestamp.IsZero() {
		graceSec := int64(3600)
		if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds > 0 {
			graceSec = *pod.Spec.TerminationGracePeriodSeconds
		}
		maxDrainDuration := time.Duration(graceSec+60) * time.Second
		return time.Since(pod.DeletionTimestamp.Time) <= maxDrainDuration
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status != corev1.ConditionTrue {
			if !cond.LastTransitionTime.IsZero() && time.Since(cond.LastTransitionTime.Time) > maxNotReadyKeepAliveDuration {
				return false
			}
		}
	}
	return true
}

func formatPodExecutorURL(podIP string, port int32) string {
	if port <= 0 {
		port = 9999
	}
	return fmt.Sprintf("http://%s/", net.JoinHostPort(strings.TrimSpace(podIP), strconv.Itoa(int(port))))
}

// validateProxyServiceBaseURL validates that rawURL is a clean http(s)://host[:port][/] base URL
// without userinfo credentials, subpaths (such as /proxy/), query parameters, or fragments (fixes R4-P0-2, R5-P2-7, R6-P1-14).
func validateProxyServiceBaseURL(rawURL string) error {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("malformed URL %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https in %q", rawURL)
	}
	if u.User != nil {
		return fmt.Errorf("must not contain userinfo credentials in %q", rawURL)
	}
	if u.Host == "" {
		return fmt.Errorf("host must not be empty in %q", rawURL)
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("must only specify scheme://host[:port]/ without subpath %q", u.Path)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must not contain query parameters or fragments in %q", rawURL)
	}
	return nil
}

func (r *XxlJobExecutorReconciler) cleanupExecutorOnPreviousAdmin(
	ctx context.Context,
	reader client.Reader,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	prevAdminURL string,
	forceOrNsTerminating bool,
	bypassRemote bool,
) error {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if prevAdminURL == "" {
		return nil
	}
	var adminList xxljobv1alpha1.XxlJobAdminList
	if err := r.List(ctx, &adminList, client.InNamespace(execCR.Namespace)); err != nil {
		return err
	}
	var matchedAdmin *xxljobv1alpha1.XxlJobAdmin
	var deletingMatch *xxljobv1alpha1.XxlJobAdmin
	for i := range adminList.Items {
		item := &adminList.Items[i]
		if strings.TrimRight(ResolveAdminURL(item), "/") != prevAdminURL {
			continue
		}
		if item.DeletionTimestamp.IsZero() {
			matchedAdmin = item
			break
		}
		if deletingMatch == nil {
			deletingMatch = item
		}
	}
	if matchedAdmin == nil {
		matchedAdmin = deletingMatch
	}
	if matchedAdmin == nil {
		return nil
	}
	bypassPrev := bypassRemote || (!matchedAdmin.DeletionTimestamp.IsZero() && shouldBypassRemoteCleanup(ctx, reader, matchedAdmin))
	_, prevCli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory,
		xxljobv1alpha1.LocalObjectReference{Name: matchedAdmin.Name}, execCR.Namespace)
	if err != nil {
		if bypassPrev {
			return nil
		}
		return err
	}
	// When cleaning up on a previous Admin (either during live cross-Admin migration or when deleting an executor
	// whose AdminRef was already pointed to a different Admin), ignore business rejections on RemoveJobGroup (e.g., if
	// manual tasks still exist on the old Admin) while propagating transport/HTTP errors so transient network failures are retried (fixes R10-P1-1).
	if err := r.cleanupExecutorOnAdminInternal(ctx, prevCli, execCR, true, true, true, bypassPrev); err != nil {
		if bypassPrev {
			return nil
		}
		return err
	}
	return nil
}

func (r *XxlJobExecutorReconciler) cleanupExecutorOnAdmin(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	deregisterRecordedAddresses bool,
	forceOrNsTerminating bool,
	bypassRemote bool,
) error {
	return r.cleanupExecutorOnAdminInternal(ctx, xxlCli, execCR, deregisterRecordedAddresses, false, forceOrNsTerminating, bypassRemote)
}

func (r *XxlJobExecutorReconciler) deterministicProxyAddress(execCR *xxljobv1alpha1.XxlJobExecutor) string {
	if execCR == nil || execCR.Spec.Mode != xxljobv1alpha1.ExecutorModeK8sJobProxy {
		return ""
	}
	proxyBaseAddr := strings.TrimSpace(execCR.Spec.ProxyServiceAddress)
	if proxyBaseAddr == "" {
		proxyBaseAddr = r.DefaultProxyExecutorAddr
	}
	if proxyBaseAddr == "" {
		proxyBaseAddr = "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/"
	}
	return buildScopedProxyURL(proxyBaseAddr, execCR.Namespace, execCR.Name)
}

func (r *XxlJobExecutorReconciler) cleanupExecutorOnAdminInternal(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	deregisterRecordedAddresses bool,
	isPreviousAdmin bool,
	forceOrNsTerminating bool,
	bypassRemote bool,
) error {
	addressType := resolveExecutorAddressType(execCR)
	usesExecutorRegistry := shouldHeartbeatExecutorRegistry(execCR) || (addressType == 0 && execCR.Spec.PodSelector != nil)
	proxyAddr := r.deterministicProxyAddress(execCR)

	deregisterForAppName := func(appName string) {
		if appName == "" {
			return
		}
		deregistered := make(map[string]bool)
		for _, addr := range execCR.Status.RegisteredAddresses {
			if addr != "" && !deregistered[addr] {
				deregistered[addr] = true
				_ = xxlCli.RemoveExecutor(ctx, appName, addr)
			}
		}
		for _, addr := range execCR.Status.DrainedAddresses {
			if addr != "" && !deregistered[addr] {
				deregistered[addr] = true
				_ = xxlCli.RemoveExecutor(ctx, appName, addr)
			}
		}
		for _, addr := range execCR.Status.RegistryKeepAliveAddresses {
			if addr != "" && !deregistered[addr] {
				deregistered[addr] = true
				_ = xxlCli.RemoveExecutor(ctx, appName, addr)
			}
		}
		if proxyAddr != "" && !deregistered[proxyAddr] {
			_ = xxlCli.RemoveExecutor(ctx, appName, proxyAddr)
		}
	}

	prevSyncedAppName := extractSyncedExecutorAppName(execCR)
	if deregisterRecordedAddresses && usesExecutorRegistry {
		if !isPreviousAdmin || prevSyncedAppName == "" {
			deregisterForAppName(execCR.Spec.AppName)
		}
		if prevSyncedAppName != "" && prevSyncedAppName != execCR.Spec.AppName {
			deregisterForAppName(prevSyncedAppName)
		} else if isPreviousAdmin && prevSyncedAppName != "" {
			deregisterForAppName(prevSyncedAppName)
		}
		if execCR.Status.JobGroupID > 0 {
			if prevGrp, grpErr := xxlCli.GetJobGroupByID(ctx, execCR.Status.JobGroupID); grpErr == nil &&
				prevGrp != nil && prevGrp.AppName != "" && prevGrp.AppName != execCR.Spec.AppName && prevGrp.AppName != prevSyncedAppName {
				deregisterForAppName(prevGrp.AppName)
			}
		}
	} else if !deregisterRecordedAddresses && proxyAddr != "" && execCR.Spec.AppName != "" {
		// Even when Status.RegisteredAddresses belonged to a previous Admin, K8sJobProxy's deterministic
		// scoped proxy URL on the current Admin can be safely deregistered.
		_ = xxlCli.RemoveExecutor(ctx, execCR.Spec.AppName, proxyAddr)
	}

	removeGroupByID := func(groupID int, lookupAppName string) error {
		if groupID <= 0 {
			return nil
		}
		// When migrating across Admins (execCR is not being deleted), stop and remove any Operator-owned tasks
		// belonging to referencing XxlJobTasks in groupID on the previous Admin first so RemoveJobGroup
		// succeeds cleanly without leaking the old JobGroup on the previous Admin (fixes R11-P1-1).
		if execCR.DeletionTimestamp.IsZero() {
			if err := r.removeOwnedTasksInGroup(ctx, xxlCli, execCR, groupID); err != nil && !bypassRemote {
				return err
			}
		}
		err := xxlCli.RemoveJobGroup(ctx, groupID)
		if err != nil && isAdminBusinessRejection(err) && (execCR.DeletionTimestamp.IsZero() || forceOrNsTerminating) {
			// Retry once after ensuring all Operator-owned tasks referencing this executor in groupID are removed (fixes R11-P1-1).
			if cleanErr := r.removeOwnedTasksInGroup(ctx, xxlCli, execCR, groupID); cleanErr == nil {
				err = xxlCli.RemoveJobGroup(ctx, groupID)
			}
		}
		if err != nil {
			// If Admin returned a business error (ReturnT.Code=500), check via GetJobGroupByID (or GetJobGroupByAppName when
			// groupID came from an appName lookup and GetJobGroupByID failed) whether the JobGroup was already deleted in Admin;
			// if so, treat RemoveJobGroup as idempotent success. Never fall back to GetJobGroupByAppName when GetJobGroupByID
			// succeeded and proved groupID still exists (e.g. after Spec.AppName was modified before deletion).
			alreadyDeleted := false
			if isAdminBusinessRejection(err) {
				if existingByID, getErr := xxlCli.GetJobGroupByID(ctx, groupID); getErr == nil {
					alreadyDeleted = (existingByID == nil)
				} else if lookupAppName != "" {
					if existingGrp, appErr := xxlCli.GetJobGroupByAppName(ctx, lookupAppName); appErr == nil &&
						(existingGrp == nil || existingGrp.ID != groupID) {
						alreadyDeleted = true
					}
				}
			}
			if !alreadyDeleted {
				// Do not silently bypass explicit business rejections from a reachable Admin (e.g., manual tasks still exist)
				// unless force-delete=true or the Namespace itself is terminating (fixes R5-P2-6 & R6-P1-5).
				canBypassErr := forceOrNsTerminating || (bypassRemote && !isAdminBusinessRejection(err))
				if !canBypassErr {
					return err
				}
			}
		}
		return nil
	}

	cleanedGroupIDs := make(map[int]bool)
	if deregisterRecordedAddresses && execCR.Status.JobGroupID > 0 {
		if err := removeGroupByID(execCR.Status.JobGroupID, ""); err != nil {
			return err
		}
		cleanedGroupIDs[execCR.Status.JobGroupID] = true
	}

	// Also clean up any JobGroup created on this Admin under Spec.AppName (e.g., if Spec.AppName was renamed in-place
	// and EnsureJobGroup created the new group before Status.Update completed, or Status.JobGroupID was not yet recorded)
	// and under prevSyncedAppName when Status.JobGroupID was unset.
	var candidateAppNames []string
	if !isPreviousAdmin {
		if execCR.Spec.AppName != "" {
			candidateAppNames = append(candidateAppNames, execCR.Spec.AppName)
		}
		if deregisterRecordedAddresses && execCR.Status.JobGroupID <= 0 && prevSyncedAppName != "" && prevSyncedAppName != execCR.Spec.AppName {
			candidateAppNames = append(candidateAppNames, prevSyncedAppName)
		}
	} else if execCR.Status.JobGroupID <= 0 {
		if prevSyncedAppName != "" {
			candidateAppNames = append(candidateAppNames, prevSyncedAppName)
		} else if execCR.Spec.AppName != "" {
			candidateAppNames = append(candidateAppNames, execCR.Spec.AppName)
		}
	}

	for _, appName := range candidateAppNames {
		existingGrp, err := xxlCli.GetJobGroupByAppName(ctx, appName)
		if err != nil {
			if bypassRemote {
				continue
			}
			return err
		}
		if existingGrp != nil && existingGrp.ID > 0 && !cleanedGroupIDs[existingGrp.ID] {
			if err := removeGroupByID(existingGrp.ID, appName); err != nil {
				return err
			}
			cleanedGroupIDs[existingGrp.ID] = true
		}
	}
	return nil
}

func (r *XxlJobExecutorReconciler) listReferencingTasks(ctx context.Context, execCR *xxljobv1alpha1.XxlJobExecutor) ([]xxljobv1alpha1.XxlJobTask, error) {
	var list xxljobv1alpha1.XxlJobTaskList
	if err := r.List(ctx, &list, client.InNamespace(execCR.Namespace)); err != nil {
		return nil, err
	}
	var matched []xxljobv1alpha1.XxlJobTask
	for i := range list.Items {
		item := list.Items[i]
		refNS := item.Spec.ExecutorRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		if refNS == execCR.Namespace && item.Spec.ExecutorRef.Name == execCR.Name {
			matched = append(matched, item)
		}
	}
	return matched, nil
}

func (r *XxlJobExecutorReconciler) findOwnedTaskInGroup(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	task *xxljobv1alpha1.XxlJobTask,
	groupID int,
) (*xxlclient.JobInfo, error) {
	ownerKey := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	if task.Status.JobID > 0 {
		existingJob, err := xxlCli.GetJobInfoByID(ctx, groupID, task.Status.JobID)
		if err != nil {
			return nil, err
		}
		if existingJob != nil && xxlclient.IsOwnerCompatible(existingJob.JobDesc, ownerKey) {
			return existingJob, nil
		}
	}
	return xxlCli.FindJobInfoByOwnerKey(ctx, groupID, ownerKey)
}

func (r *XxlJobExecutorReconciler) removeOwnedTasksInGroup(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	groupID int,
) error {
	if groupID <= 0 {
		return nil
	}
	xxlCli.InvalidateGroupSnapshot(groupID)
	tasks, err := r.listReferencingTasks(ctx, execCR)
	if err != nil {
		return err
	}
	for i := range tasks {
		targetJob, err := r.findOwnedTaskInGroup(ctx, xxlCli, &tasks[i], groupID)
		if err != nil {
			return err
		}
		if targetJob == nil {
			continue
		}
		_ = xxlCli.StopJobInfo(ctx, targetJob.ID)
		if err := xxlCli.RemoveJobInfo(ctx, targetJob.ID); err != nil && !isAdminBusinessRejection(err) {
			return err
		}
	}
	return nil
}

func (r *XxlJobExecutorReconciler) migrateAndCleanupPreviousJobGroupOnSameAdmin(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	prevGroupID, newGroupID int,
) error {
	if prevGroupID <= 0 || prevGroupID == newGroupID {
		return nil
	}
	xxlCli.InvalidateGroupSnapshot(prevGroupID)
	xxlCli.InvalidateGroupSnapshot(newGroupID)

	addressType := resolveExecutorAddressType(execCR)
	usesExecutorRegistry := shouldHeartbeatExecutorRegistry(execCR) || (addressType == 0 && execCR.Spec.PodSelector != nil)
	if usesExecutorRegistry {
		deregisterOld := func(oldAppName string) {
			if oldAppName == "" || oldAppName == execCR.Spec.AppName {
				return
			}
			for _, addr := range execCR.Status.RegisteredAddresses {
				_ = xxlCli.RemoveExecutor(ctx, oldAppName, addr)
			}
			for _, addr := range execCR.Status.DrainedAddresses {
				_ = xxlCli.RemoveExecutor(ctx, oldAppName, addr)
			}
			for _, addr := range execCR.Status.RegistryKeepAliveAddresses {
				_ = xxlCli.RemoveExecutor(ctx, oldAppName, addr)
			}
		}
		prevSyncedAppName := extractSyncedExecutorAppName(execCR)
		deregisterOld(prevSyncedAppName)
		prevGrp, grpErr := xxlCli.GetJobGroupByID(ctx, prevGroupID)
		if grpErr != nil && !isAdminBusinessRejection(grpErr) {
			return grpErr
		}
		if prevGrp != nil && prevGrp.AppName != prevSyncedAppName {
			deregisterOld(prevGrp.AppName)
		}
	}

	migrateTasksOnce := func() error {
		tasks, err := r.listReferencingTasks(ctx, execCR)
		if err != nil {
			return err
		}
		for i := range tasks {
			targetJob, err := r.findOwnedTaskInGroup(ctx, xxlCli, &tasks[i], prevGroupID)
			if err != nil {
				return err
			}
			if targetJob == nil {
				continue
			}
			updated := *targetJob
			updated.JobGroup = newGroupID
			if err := xxlCli.UpdateJobInfo(ctx, updated); err != nil {
				return err
			}
			if tasks[i].Status.JobGroupID != newGroupID {
				taskCopy := tasks[i].DeepCopy()
				taskCopy.Status.JobGroupID = newGroupID
				_ = r.Status().Update(ctx, taskCopy)
			}
		}
		return nil
	}

	if err := migrateTasksOnce(); err != nil {
		return err
	}
	err := xxlCli.RemoveJobGroup(ctx, prevGroupID)
	if err != nil && isAdminBusinessRejection(err) {
		xxlCli.InvalidateGroupSnapshot(prevGroupID)
		if migErr := migrateTasksOnce(); migErr == nil {
			err = xxlCli.RemoveJobGroup(ctx, prevGroupID)
		}
	}
	if err != nil && !isAdminBusinessRejection(err) {
		return err
	}
	return nil
}

func (r *XxlJobExecutorReconciler) countReferencingTasks(ctx context.Context, execCR *xxljobv1alpha1.XxlJobExecutor) (int, error) {
	var list xxljobv1alpha1.XxlJobTaskList
	if err := r.List(ctx, &list, client.InNamespace(execCR.Namespace)); err != nil {
		return 0, err
	}
	execAdminURL := strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
	count := 0
	for i := range list.Items {
		item := &list.Items[i]
		refNS := item.Spec.ExecutorRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		if refNS == execCR.Namespace && item.Spec.ExecutorRef.Name == execCR.Name {
			count++
			continue
		}
		// Protect against premature RemoveJobGroup when a task was repointed to a different executor (or is deleting)
		// while its remote XXL-JOB Admin task is still recorded in execCR.Status.JobGroupID.
		if execCR.Status.JobGroupID > 0 && item.Status.JobGroupID == execCR.Status.JobGroupID {
			taskAdminURL := strings.TrimRight(strings.TrimSpace(item.Status.AdminURL), "/")
			sameRecordedAdmin := execAdminURL == "" || taskAdminURL == "" || execAdminURL == taskAdminURL
			if !sameRecordedAdmin {
				continue
			}
			if !item.DeletionTimestamp.IsZero() {
				count++
				continue
			}
			if refNS == execCR.Namespace && item.Spec.ExecutorRef.Name != "" {
				var targetExec xxljobv1alpha1.XxlJobExecutor
				getErr := r.Get(ctx, types.NamespacedName{Namespace: refNS, Name: item.Spec.ExecutorRef.Name}, &targetExec)
				if apierrors.IsNotFound(getErr) {
					count++
				} else if getErr == nil && targetExec.DeletionTimestamp.IsZero() {
					migratingCrossApp := targetExec.Spec.AppName != execCR.Spec.AppName
					migratingCrossAdmin := execAdminURL != "" && taskAdminURL != "" && targetExec.Spec.AdminRef.Name != execCR.Spec.AdminRef.Name
					if migratingCrossApp || migratingCrossAdmin {
						count++
					}
				}
			}
		}
	}
	return count, nil
}

func (r *XxlJobExecutorReconciler) updateErrorStatus(ctx context.Context, execCR *xxljobv1alpha1.XxlJobExecutor, msg string, metaChanged ...bool) {
	hasMetaChange := len(metaChanged) > 0 && metaChanged[0]
	if !execCR.Status.Synced && execCR.Status.Message == msg && !hasMetaChange {
		return
	}
	apply := func(target *xxljobv1alpha1.XxlJobExecutor) {
		if hasMetaChange {
			clearPreviousAdminStatus(target)
		}
		target.Status.Synced = false
		target.Status.Message = msg
		meta.SetStatusCondition(&target.Status.Conditions, metav1.Condition{
			Type:               "Synced",
			Status:             metav1.ConditionFalse,
			Reason:             "ReconcileFailed",
			Message:            msg,
			LastTransitionTime: metav1.Now(),
		})
	}
	apply(execCR)
	if err := r.Status().Update(ctx, execCR); apierrors.IsConflict(err) && hasMetaChange {
		var latest xxljobv1alpha1.XxlJobExecutor
		if getErr := r.Get(ctx, types.NamespacedName{Namespace: execCR.Namespace, Name: execCR.Name}, &latest); getErr == nil {
			apply(&latest)
			_ = r.Status().Update(ctx, &latest)
		}
	}
}

func (r *XxlJobExecutorReconciler) updateErrorStatusWithAddresses(
	ctx context.Context,
	execCR *xxljobv1alpha1.XxlJobExecutor,
	activeAddresses, drainedAddresses, keepAliveAddresses []string,
	msg string,
	metaChanged ...bool,
) {
	hasMetaChange := len(metaChanged) > 0 && metaChanged[0]
	addrChanged := !reflect.DeepEqual(execCR.Status.RegisteredAddresses, activeAddresses) ||
		!reflect.DeepEqual(execCR.Status.DrainedAddresses, drainedAddresses) ||
		!reflect.DeepEqual(execCR.Status.RegistryKeepAliveAddresses, keepAliveAddresses) ||
		execCR.Status.ActivePods != len(activeAddresses) ||
		execCR.Status.TerminatingPodsDrained != len(drainedAddresses)
	if !execCR.Status.Synced && execCR.Status.Message == msg && !addrChanged && !hasMetaChange {
		return
	}
	apply := func(target *xxljobv1alpha1.XxlJobExecutor) {
		if hasMetaChange {
			clearPreviousAdminStatus(target)
		}
		target.Status.RegisteredAddresses = activeAddresses
		target.Status.DrainedAddresses = drainedAddresses
		target.Status.RegistryKeepAliveAddresses = keepAliveAddresses
		target.Status.ActivePods = len(activeAddresses)
		target.Status.TerminatingPodsDrained = len(drainedAddresses)
		target.Status.Synced = false
		target.Status.Message = msg
		meta.SetStatusCondition(&target.Status.Conditions, metav1.Condition{
			Type:               "Synced",
			Status:             metav1.ConditionFalse,
			Reason:             "ReconcileFailed",
			Message:            msg,
			LastTransitionTime: metav1.Now(),
		})
	}
	apply(execCR)
	if err := r.Status().Update(ctx, execCR); apierrors.IsConflict(err) && hasMetaChange {
		var latest xxljobv1alpha1.XxlJobExecutor
		if getErr := r.Get(ctx, types.NamespacedName{Namespace: execCR.Namespace, Name: execCR.Name}, &latest); getErr == nil {
			apply(&latest)
			_ = r.Status().Update(ctx, &latest)
		}
	}
}

// TransformPodForCache strips heavy PodSpec fields, unrelated Annotations, ManagedFields, and ContainerStatuses
// from cached Pods while preserving the injection metadata (Annotations["xxljob.io/injected"] and container names)
// required by isPodInjectedWhenRequired and XxlJobExecutorReconciler (fixes R4-P1-4, R6-P1-10, R7-P0-1).
// Callers that require full Pod.Spec or Pod.Annotations (such as ProxyExecutorServer /log) MUST read via mgr.GetAPIReader().
func TransformPodForCache(obj any) (any, error) {
	if pod, ok := obj.(*corev1.Pod); ok {
		pod.ManagedFields = nil
		var slimAnnotations map[string]string
		if pod.Annotations != nil {
			for _, k := range []string{"xxljob.io/injected", "xxljob.io/mode"} {
				if v, exists := pod.Annotations[k]; exists {
					if slimAnnotations == nil {
						slimAnnotations = make(map[string]string, 2)
					}
					slimAnnotations[k] = v
				}
			}
		}
		pod.Annotations = slimAnnotations
		var slimContainers []corev1.Container
		if len(pod.Spec.Containers) > 0 {
			slimContainers = make([]corev1.Container, len(pod.Spec.Containers))
			for i := range pod.Spec.Containers {
				slimContainers[i] = corev1.Container{Name: pod.Spec.Containers[i].Name}
			}
		}
		pod.Spec = corev1.PodSpec{
			TerminationGracePeriodSeconds: pod.Spec.TerminationGracePeriodSeconds,
			Containers:                    slimContainers,
		}
		pod.Status = corev1.PodStatus{
			Phase:      pod.Status.Phase,
			PodIP:      pod.Status.PodIP,
			Conditions: pod.Status.Conditions,
		}
		return pod, nil
	}
	return cache.TransformStripManagedFields()(obj)
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isPodInjectedWhenRequired verifies that a Pod belonging to a Sidecar-mode XxlJobExecutor actually
// received the sidecar container (via webhook mutation or static PodSpec) before its address is added
// to address_list, without blocking Resident-mode Pods even after TransformPodForCache (fixes R6-P1-12 & R7-P0-1).
func isPodInjectedWhenRequired(pod *corev1.Pod, mode xxljobv1alpha1.ExecutorMode) bool {
	if pod == nil {
		return false
	}
	if mode == xxljobv1alpha1.ExecutorModeSidecar {
		if pod.Labels != nil && pod.Labels["xxljob.io/sidecar-injected"] == "true" {
			return true
		}
		if pod.Annotations != nil && pod.Annotations["xxljob.io/injected"] == "true" {
			return true
		}
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == "xxl-job-sidecar" {
				return true
			}
		}
		return false
	}
	return true
}

func podExecutorLifecyclePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
			newPod, ok2 := e.ObjectNew.(*corev1.Pod)
			if !ok1 || !ok2 {
				return true
			}
			if oldPod.Status.PodIP != newPod.Status.PodIP ||
				oldPod.Status.Phase != newPod.Status.Phase ||
				isPodReady(oldPod) != isPodReady(newPod) ||
				oldPod.DeletionTimestamp.IsZero() != newPod.DeletionTimestamp.IsZero() ||
				!reflect.DeepEqual(oldPod.Labels, newPod.Labels) {
				return true
			}
			oldInj := oldPod.Annotations != nil && oldPod.Annotations["xxljob.io/injected"] == "true"
			newInj := newPod.Annotations != nil && newPod.Annotations["xxljob.io/injected"] == "true"
			return oldInj != newInj
		},
	}
}

func ensureTrailingSlash(u string) string {
	u = strings.TrimSpace(u)
	if u != "" && !strings.HasSuffix(u, "/") {
		return u + "/"
	}
	return u
}

func buildScopedProxyURL(baseAddr, namespace, executorName string) string {
	base := strings.TrimRight(strings.TrimSpace(baseAddr), "/")
	if idx := strings.Index(base, "/proxy/"); idx != -1 {
		base = base[:idx]
	}
	return fmt.Sprintf("%s/proxy/%s/%s/", base, namespace, executorName)
}

func buildUnavailableSentinelURL(baseAddr string) string {
	base := strings.TrimRight(strings.TrimSpace(baseAddr), "/")
	if base == "" {
		return xxlclient.DefaultUnavailableSentinelAddress
	}
	if idx := strings.Index(base, "/proxy/"); idx != -1 {
		base = base[:idx]
	}
	return fmt.Sprintf("%s/proxy/%s/", base, proxyexecutor.UnavailableExecutorSentinel)
}

func isK8sJobProxyManagedPod(pod *corev1.Pod) bool {
	if pod == nil || pod.Labels == nil {
		return false
	}
	return pod.Labels[proxyexecutor.LabelManagedBy] == proxyexecutor.ManagedByValue &&
		pod.Labels[proxyexecutor.LabelJobID] != ""
}

func (r *XxlJobExecutorReconciler) findExecutorsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok || isK8sJobProxyManagedPod(pod) {
		return nil
	}
	var list xxljobv1alpha1.XxlJobExecutorList
	if err := r.List(ctx, &list, client.InNamespace(pod.Namespace)); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.PodSelector == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(item.Spec.PodSelector)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.Namespace,
					Name:      item.Name,
				},
			})
		}
	}
	return requests
}

func (r *XxlJobExecutorReconciler) findDeletingExecutorsForTask(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	var list xxljobv1alpha1.XxlJobExecutorList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		if !item.DeletionTimestamp.IsZero() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.Namespace,
					Name:      item.Name,
				},
			})
		}
	}
	return requests
}

func taskForDeletingExecutorPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldTask, ok1 := e.ObjectOld.(*xxljobv1alpha1.XxlJobTask)
			newTask, ok2 := e.ObjectNew.(*xxljobv1alpha1.XxlJobTask)
			if !ok1 || !ok2 {
				return false
			}
			return oldTask.Spec.ExecutorRef != newTask.Spec.ExecutorRef ||
				oldTask.Status.JobGroupID != newTask.Status.JobGroupID ||
				oldTask.Status.AdminURL != newTask.Status.AdminURL
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
	}
}

func (r *XxlJobExecutorReconciler) findExecutorsForAdmin(ctx context.Context, obj client.Object) []reconcile.Request {
	adminCR, ok := obj.(*xxljobv1alpha1.XxlJobAdmin)
	if !ok || adminCR == nil {
		return nil
	}
	candidateURLs := make(map[string]bool, 2)
	if u := strings.TrimRight(ResolveAdminURL(adminCR), "/"); u != "" {
		candidateURLs[u] = true
	}
	if su := strings.TrimRight(strings.TrimSpace(adminCR.Status.AdminURL), "/"); su != "" {
		candidateURLs[su] = true
	}
	var list xxljobv1alpha1.XxlJobExecutorList
	if err := r.List(ctx, &list, client.InNamespace(adminCR.Namespace)); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		refNS := item.Spec.AdminRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		itemAdminURL := strings.TrimRight(strings.TrimSpace(item.Status.AdminURL), "/")
		if (refNS == adminCR.Namespace && item.Spec.AdminRef.Name == adminCR.Name) ||
			(itemAdminURL != "" && candidateURLs[itemAdminURL]) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.Namespace,
					Name:      item.Name,
				},
			})
		}
	}
	return requests
}

func resolveExecutorAddressType(execCR *xxljobv1alpha1.XxlJobExecutor) int {
	if execCR == nil {
		return 1
	}
	if execCR.Spec.AddressType != nil {
		return *execCR.Spec.AddressType
	}
	if execCR.Spec.Mode != xxljobv1alpha1.ExecutorModeK8sJobProxy &&
		!execCR.Spec.UseServiceDNS &&
		len(execCR.Spec.StaticAddresses) == 0 &&
		execCR.Spec.PodSelector == nil {
		return 0
	}
	return 1
}

func shouldHeartbeatExecutorRegistry(execCR *xxljobv1alpha1.XxlJobExecutor) bool {
	if execCR == nil {
		return false
	}
	if execCR.Spec.Mode == xxljobv1alpha1.ExecutorModeK8sJobProxy {
		return true
	}
	addressType := resolveExecutorAddressType(execCR)
	return addressType == 1 &&
		execCR.Spec.PodSelector != nil &&
		!execCR.Spec.UseServiceDNS &&
		len(execCR.Spec.StaticAddresses) == 0
}

func adminForExecutorPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldAdmin, ok1 := e.ObjectOld.(*xxljobv1alpha1.XxlJobAdmin)
			newAdmin, ok2 := e.ObjectNew.(*xxljobv1alpha1.XxlJobAdmin)
			if !ok1 || !ok2 {
				return false
			}
			return oldAdmin.Generation != newAdmin.Generation ||
				!reflect.DeepEqual(oldAdmin.Annotations, newAdmin.Annotations) ||
				oldAdmin.DeletionTimestamp.IsZero() != newAdmin.DeletionTimestamp.IsZero() ||
				oldAdmin.Status.Phase != newAdmin.Status.Phase ||
				oldAdmin.Status.AdminURL != newAdmin.Status.AdminURL ||
				(oldAdmin.Status.ReadyReplicas == 0) != (newAdmin.Status.ReadyReplicas == 0)
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
	}
}

// SetupWithManager sets up the controller with the Manager and watches Pod lifecycle events.
func (r *XxlJobExecutorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 4}).
		For(&xxljobv1alpha1.XxlJobExecutor{}, builder.WithPredicates(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.AnnotationChangedPredicate{},
				deletionTimestampChangedPredicate(),
			),
		)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.findExecutorsForPod), builder.WithPredicates(podExecutorLifecyclePredicate())).
		Watches(&xxljobv1alpha1.XxlJobTask{}, handler.EnqueueRequestsFromMapFunc(r.findDeletingExecutorsForTask), builder.WithPredicates(taskForDeletingExecutorPredicate())).
		Watches(&xxljobv1alpha1.XxlJobAdmin{}, handler.EnqueueRequestsFromMapFunc(r.findExecutorsForAdmin), builder.WithPredicates(adminForExecutorPredicate())).
		Complete(r)
}
