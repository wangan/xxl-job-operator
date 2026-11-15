package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
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

const (
	TaskFinalizer = "xxljob.io/task-finalizer"
	// AdoptJobIDAnnotation allows an XxlJobTask to explicitly adopt a pre-existing unmarked job in XXL-JOB Admin.
	AdoptJobIDAnnotation = "xxljob.io/adopt-job-id"
)

// XxlJobTaskReconciler reconciles a XxlJobTask object.
type XxlJobTaskReconciler struct {
	client.Client
	APIReader     client.Reader
	Scheme        *runtime.Scheme
	ClientFactory ClientFactory
}

// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobtasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=update

func (r *XxlJobTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var task xxljobv1alpha1.XxlJobTask
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	execNS := task.Spec.ExecutorRef.Namespace
	if execNS == "" {
		execNS = task.Namespace
	}
	var execCR xxljobv1alpha1.XxlJobExecutor
	var execErr error
	if execNS != task.Namespace {
		execErr = fmt.Errorf("cross-namespace executorRef (%s/%s) is not allowed", execNS, task.Spec.ExecutorRef.Name)
	} else {
		execErr = r.Get(ctx, types.NamespacedName{Namespace: execNS, Name: task.Spec.ExecutorRef.Name}, &execCR)
	}

	// Handle Deletion via Finalizer:
	// 1. First stop & remove the job from XXL-JOB Admin so no new triggers can be fired during deletion (fixes R6-P2-3, R7-P1-4, R8-P1-8).
	// 2. Then suspend and delete any incomplete batch/v1.Jobs spawned for this task across any executor (fixes R5-P2-5, R6-P2-1, R6-P2-14).
	if !task.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&task, TaskFinalizer) {
			ownerKey := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
			bypassRemote := shouldBypassRemoteCleanup(ctx, reader, &task)
			if !bypassRemote {
				if execErr != nil {
					if apierrors.IsNotFound(execErr) || execNS != task.Namespace {
						logger.Info("Referenced XxlJobExecutor unavailable or invalid; attempting cleanup via recorded Status.AdminURL if available")
						if cleanErr := r.cleanupTaskOnPreviousAdmin(ctx, reader, &task, "", task.Status.AdminURL, task.Status.JobGroupID, task.Status.JobID, ownerKey); cleanErr != nil {
							logger.Error(cleanErr, "Failed to clean up task on recorded XxlJobAdmin during deletion, will retry", "recordedAdminURL", task.Status.AdminURL)
							return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
						}
					} else {
						return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
					}
				} else {
					fallbackAdminName := ""
					if execCR.Spec.AdminRef.Namespace == "" || execCR.Spec.AdminRef.Namespace == task.Namespace {
						fallbackAdminName = execCR.Spec.AdminRef.Name
					}
					adminCR, xxlCli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory, execCR.Spec.AdminRef, execCR.Namespace)
					if err != nil {
						if task.Status.AdminURL != "" && (task.Status.JobID > 0 || task.Status.JobGroupID > 0) {
							delTaskMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
							migratingPrev := execCR.Spec.AdminRef.Namespace != "" && execCR.Spec.AdminRef.Namespace != task.Namespace
							if !migratingPrev && fallbackAdminName != "" {
								if mig, migErr := isMigratingFromPreviousAdmin(ctx, reader, execCR.Namespace, fallbackAdminName, task.Status.AdminURL, delTaskMeta); migErr == nil && mig {
									migratingPrev = true
								}
							}
							if migratingPrev {
								if cleanErr := r.cleanupTaskOnPreviousAdmin(ctx, reader, &task, fallbackAdminName, task.Status.AdminURL, task.Status.JobGroupID, task.Status.JobID, ownerKey); cleanErr != nil {
									logger.Error(cleanErr, "Failed to clean up task on previous XxlJobAdmin during deletion while target Admin is unavailable, will retry", "recordedAdminURL", task.Status.AdminURL)
									return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
								}
								task.Status.JobID = 0
								task.Status.JobGroupID = 0
								task.Status.AdminURL = ""
								task.Status.LastExecutionJobName = ""
								meta.RemoveStatusCondition(&task.Status.Conditions, ConditionAdminEndpointSynced)
								_ = r.Status().Update(ctx, &task)
							}
						}
						logger.Error(err, "Unable to resolve XxlJobAdmin during Task deletion; add annotation xxljob.io/force-delete=true to bypass")
						return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
					}
					currentAdminURL := ResolveAdminURL(adminCR)
					// Guard against cross-Admin ID collision on deletion (fixes R7-P1-4 & R8-P1-8):
					// If Status.AdminURL was recorded against a different Admin (or a replaced DB / recreated Admin UID),
					// clean up the old job on that previous Admin (when distinct) and only remove by ownerKey on the current Admin.
					delTaskMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
					adminReplaced := isAdminInstanceOrDBReplaced(adminCR, delTaskMeta)
					sameAdmin := !adminReplaced && (task.Status.AdminURL == "" ||
						strings.TrimRight(task.Status.AdminURL, "/") == strings.TrimRight(currentAdminURL, "/") ||
						isSameManagedAdminDBUpdate(adminCR, task.Status.AdminURL, delTaskMeta))
					if sameAdmin {
						if err := r.removeOwnedTaskFromAdmin(ctx, xxlCli, &task, execCR.Status.JobGroupID, ownerKey); err != nil {
							logger.Error(err, "Failed to remove JobInfo from XXL-JOB Admin during deletion, will retry", "jobId", task.Status.JobID)
							return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
						}
					} else {
						if task.Status.AdminURL != "" && strings.TrimRight(task.Status.AdminURL, "/") != strings.TrimRight(currentAdminURL, "/") {
							if err := r.cleanupTaskOnPreviousAdmin(ctx, reader, &task, fallbackAdminName, task.Status.AdminURL, task.Status.JobGroupID, task.Status.JobID, ownerKey); err != nil {
								logger.Error(err, "Failed to clean up task on previous XxlJobAdmin during deletion, will retry", "recordedAdminURL", task.Status.AdminURL)
								return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
							}
						}
						if execCR.Status.JobGroupID > 0 && !isAdminInstanceOrDBReplaced(adminCR, extractSyncedAdminEndpointMeta(execCR.Status.Conditions)) {
							ownerOnlyTask := xxljobv1alpha1.XxlJobTask{ObjectMeta: task.ObjectMeta}
							if err := r.removeOwnedTaskFromAdmin(ctx, xxlCli, &ownerOnlyTask, execCR.Status.JobGroupID, ownerKey); err != nil {
								logger.Error(err, "Failed to remove JobInfo by ownerKey from current XXL-JOB Admin during deletion, will retry")
								return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
							}
						}
					}
				}
			}

			if err := r.cleanupActiveTaskJobs(ctx, reader, &task); err != nil {
				logger.Error(err, "Failed to clean up active batch Jobs during XxlJobTask deletion, will retry")
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}

			controllerutil.RemoveFinalizer(&task, TaskFinalizer)
			if err := r.Update(ctx, &task); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if execNS != task.Namespace {
		r.updateErrorStatus(ctx, &task, execErr.Error())
		return ctrl.Result{}, nil
	}

	ownerKey := fmt.Sprintf("%s/%s", task.Namespace, task.Name)
	prevAdminURL := strings.TrimRight(strings.TrimSpace(task.Status.AdminURL), "/")
	prevAdminCleaned := false
	fallbackExecAdminName := ""
	if execErr == nil && (execCR.Spec.AdminRef.Namespace == "" || execCR.Spec.AdminRef.Namespace == task.Namespace) {
		fallbackExecAdminName = execCR.Spec.AdminRef.Name
	}
	cleanPreviousTaskIfNeeded := func() (*ctrl.Result, error) {
		if prevAdminURL == "" || (task.Status.JobID <= 0 && task.Status.JobGroupID <= 0) {
			return nil, nil
		}
		if cleanErr := r.cleanupTaskOnPreviousAdmin(ctx, reader, &task, fallbackExecAdminName, prevAdminURL, task.Status.JobGroupID, task.Status.JobID, ownerKey); cleanErr != nil {
			logger.Error(cleanErr, "Failed to clean up task on previous XxlJobAdmin during cross-Admin migration, will retry",
				"previousAdminURL", prevAdminURL, "previousJobId", task.Status.JobID)
			r.updateErrorStatus(ctx, &task, fmt.Sprintf("Failed to clean up task on previous XxlJobAdmin (%s): %v", prevAdminURL, cleanErr))
			res := ctrl.Result{RequeueAfter: 5 * time.Second}
			return &res, nil
		}
		prevAdminCleaned = prevAdminCleaned ||
			task.Status.JobID != 0 ||
			task.Status.JobGroupID != 0 ||
			task.Status.AdminURL != "" ||
			task.Status.LastExecutionJobName != "" ||
			meta.FindStatusCondition(task.Status.Conditions, ConditionAdminEndpointSynced) != nil
		task.Status.JobID = 0
		task.Status.JobGroupID = 0
		task.Status.AdminURL = ""
		task.Status.LastExecutionJobName = ""
		meta.RemoveStatusCondition(&task.Status.Conditions, ConditionAdminEndpointSynced)
		prevAdminURL = ""
		return nil, nil
	}

	if execErr != nil {
		if apierrors.IsNotFound(execErr) && prevAdminURL != "" && (task.Status.JobID > 0 || task.Status.JobGroupID > 0) {
			prevDeleting, delCheckErr := isPreviousAdminDeleting(ctx, reader, task.Namespace, prevAdminURL)
			if delCheckErr != nil {
				return ctrl.Result{}, delCheckErr
			}
			if prevDeleting {
				if !controllerutil.ContainsFinalizer(&task, TaskFinalizer) {
					controllerutil.AddFinalizer(&task, TaskFinalizer)
					if err := r.Update(ctx, &task); err != nil {
						return ctrl.Result{}, err
					}
				}
				if res, cleanErr := cleanPreviousTaskIfNeeded(); cleanErr != nil || res != nil {
					if res != nil {
						return *res, nil
					}
					return ctrl.Result{}, cleanErr
				}
			}
		}
		r.updateErrorStatus(ctx, &task, fmt.Sprintf("Failed to find XxlJobExecutor %s/%s: %v", execNS, task.Spec.ExecutorRef.Name, execErr), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !controllerutil.ContainsFinalizer(&task, TaskFinalizer) {
		controllerutil.AddFinalizer(&task, TaskFinalizer)
		if err := r.Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}

	// If the task's target executor points to a different XxlJobAdmin than Status.AdminURL, stop and delete the old
	// task on the previous Admin BEFORE resolving the new Admin or waiting for the target executor to sync JobGroupID,
	// preventing orphan CRON triggers on the old Admin and preventing old Admin deletion deadlocks.
	prevTaskMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
	if prevAdminURL != "" && (task.Status.JobID > 0 || task.Status.JobGroupID > 0) {
		execAdminNS := execCR.Spec.AdminRef.Namespace
		if execAdminNS == "" {
			execAdminNS = execCR.Namespace
		}
		if execAdminNS == task.Namespace {
			migratingPrevAdmin, migCheckErr := isMigratingFromPreviousAdmin(ctx, reader, execCR.Namespace, execCR.Spec.AdminRef.Name, prevAdminURL, prevTaskMeta)
			if migCheckErr != nil {
				return ctrl.Result{}, migCheckErr
			}
			if migratingPrevAdmin {
				if res, cleanErr := cleanPreviousTaskIfNeeded(); cleanErr != nil || res != nil {
					if res != nil {
						return *res, nil
					}
					return ctrl.Result{}, cleanErr
				}
				prevTaskMeta = nil
			}
		}
	}

	adminCR, xxlCli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory, execCR.Spec.AdminRef, execCR.Namespace)
	if err != nil {
		r.updateErrorStatus(ctx, &task, fmt.Sprintf("Failed to resolve XxlJobAdmin: %v", err), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	adminURL := ResolveAdminURL(adminCR)
	taskAdminReplaced := isAdminInstanceOrDBReplaced(adminCR, prevTaskMeta)
	sameManagedDB := !taskAdminReplaced && isSameManagedAdminDBUpdate(adminCR, prevAdminURL, prevTaskMeta)

	if prevAdminURL != "" && prevAdminURL != strings.TrimRight(adminURL, "/") && !sameManagedDB && (task.Status.JobID > 0 || task.Status.JobGroupID > 0) {
		if res, cleanErr := cleanPreviousTaskIfNeeded(); cleanErr != nil || res != nil {
			if res != nil {
				return *res, nil
			}
			return ctrl.Result{}, cleanErr
		}
		prevTaskMeta = nil
		taskAdminReplaced = false
	}

	execAdminURL := strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
	execMeta := extractSyncedAdminEndpointMeta(execCR.Status.Conditions)
	execAlignedWithAdmin := !isAdminInstanceOrDBReplaced(adminCR, execMeta) && (execAdminURL == "" ||
		execAdminURL == strings.TrimRight(adminURL, "/") ||
		isSameManagedAdminDBUpdate(adminCR, execAdminURL, execMeta))
	if execCR.Status.JobGroupID <= 0 || !execCR.Status.Synced || !isExecutorAppNameSynced(&execCR) || !execAlignedWithAdmin {
		r.updateErrorStatus(ctx, &task, fmt.Sprintf("Waiting for XxlJobExecutor %s/%s to sync JobGroupID", execCR.Namespace, execCR.Name), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	glueType := strings.TrimSpace(task.Spec.GlueType)
	if glueType == "" || strings.EqualFold(glueType, "K8S_JOB") {
		glueType = "BEAN"
	}

	var adoptJobID int
	if task.Annotations != nil {
		if rawAdopt := strings.TrimSpace(task.Annotations[AdoptJobIDAnnotation]); rawAdopt != "" {
			if parsed, err := strconv.Atoi(rawAdopt); err == nil && parsed > 0 {
				adoptJobID = parsed
			}
		}
	}

	effectiveJobID := task.Status.JobID
	effectivePrevJobGroup := task.Status.JobGroupID
	effectivePrevAdminURL := task.Status.AdminURL
	if taskAdminReplaced {
		xxlCli.InvalidateGroupSnapshot(0)
		xxlCli.InvalidateGroupSnapshot(execCR.Status.JobGroupID)
		if task.Status.JobGroupID > 0 {
			xxlCli.InvalidateGroupSnapshot(task.Status.JobGroupID)
		}
		effectiveJobID = 0
		effectivePrevJobGroup = 0
		effectivePrevAdminURL = ""
	} else if sameManagedDB {
		effectivePrevAdminURL = adminURL
	}

	desiredInfo := xxlclient.JobInfo{
		ID:                     effectiveJobID,
		JobGroup:               execCR.Status.JobGroupID,
		JobDesc:                task.Spec.JobDesc,
		Author:                 task.Spec.Author,
		AlarmEmail:             task.Spec.AlarmEmail,
		ScheduleType:           task.Spec.ScheduleType,
		ScheduleConf:           task.Spec.ScheduleConf,
		MisfireStrategy:        task.Spec.MisfireStrategy,
		ExecutorRouteStrategy:  task.Spec.ExecutorRouteStrategy,
		ExecutorHandler:        task.Spec.ExecutorHandler,
		ExecutorParam:          task.Spec.ExecutorParam,
		ExecutorBlockStrategy:  task.Spec.ExecutorBlockStrategy,
		ExecutorTimeout:        task.Spec.ExecutorTimeout,
		ExecutorFailRetryCount: task.Spec.ExecutorFailRetryCount,
		GlueType:               glueType,
		GlueSource:             task.Spec.GlueSource,
		GlueRemark:             task.Spec.GlueRemark,
		ChildJobID:             task.Spec.ChildJobID,
		AllowGlueTypeRecreate:  hasAllowGlueTypeRecreateAnnotation(task.Annotations),
		PreviousJobGroup:       effectivePrevJobGroup,
		PreviousAdminURL:       effectivePrevAdminURL,
		OwnerKey:               ownerKey,
		AdoptJobID:             adoptJobID,
	}

	syncedInfo, err := xxlCli.EnsureJobInfo(ctx, desiredInfo, task.Spec.TriggerStatus)
	if err != nil {
		r.updateErrorStatus(ctx, &task, fmt.Sprintf("Sync JobInfo with XXL-JOB Admin failed: %v", err), prevAdminCleaned)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	msg := fmt.Sprintf("Synced with XXL-JOB Admin (jobId=%d, jobGroupId=%d, triggerStatus=%d)",
		syncedInfo.ID, execCR.Status.JobGroupID, syncedInfo.TriggerStatus)

	prevSyncedMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
	adminUIDChanged := prevSyncedMeta != nil && prevSyncedMeta.AdminUID != "" && adminCR != nil && adminCR.UID != "" &&
		prevSyncedMeta.AdminUID != string(adminCR.UID)
	adminReplacedOnSync := isAdminInstanceOrDBReplaced(adminCR, prevSyncedMeta)

	resetLastExec := task.Status.LastExecutionJobName != "" &&
		((task.Status.JobID != 0 && task.Status.JobID != syncedInfo.ID) ||
			(task.Status.AdminURL != "" && !sameManagedDB && strings.TrimRight(task.Status.AdminURL, "/") != strings.TrimRight(adminURL, "/")) ||
			adminUIDChanged ||
			adminReplacedOnSync)

	endpointMetaMsg := encodeSyncedAdminEndpointMeta(adminCR, adminURL)
	prevEndpointCond := meta.FindStatusCondition(task.Status.Conditions, ConditionAdminEndpointSynced)
	endpointCondChanged := endpointMetaMsg != "" && (prevEndpointCond == nil || prevEndpointCond.Status != metav1.ConditionTrue || prevEndpointCond.Message != endpointMetaMsg)

	// Only write Status when changed (fixes P0-1)
	statusChanged := task.Status.JobID != syncedInfo.ID ||
		task.Status.JobGroupID != execCR.Status.JobGroupID ||
		task.Status.AdminURL != adminURL ||
		resetLastExec ||
		!task.Status.Synced ||
		task.Status.TriggerStatus != syncedInfo.TriggerStatus ||
		task.Status.Message != msg ||
		endpointCondChanged

	if statusChanged {
		now := metav1.Now()
		if resetLastExec {
			task.Status.LastExecutionJobName = ""
		}
		task.Status.JobID = syncedInfo.ID
		task.Status.JobGroupID = execCR.Status.JobGroupID
		task.Status.AdminURL = adminURL
		task.Status.Synced = true
		task.Status.TriggerStatus = syncedInfo.TriggerStatus
		task.Status.LastSyncTime = &now
		task.Status.Message = msg
		meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
			Type:               "Synced",
			Status:             metav1.ConditionTrue,
			Reason:             "ReconcileSucceeded",
			Message:            msg,
			LastTransitionTime: now,
		})
		if endpointMetaMsg != "" {
			meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
				Type:               ConditionAdminEndpointSynced,
				Status:             metav1.ConditionTrue,
				Reason:             "EndpointSynced",
				Message:            endpointMetaMsg,
				LastTransitionTime: now,
			})
		}

		if err := r.Status().Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *XxlJobTaskReconciler) removeOwnedTaskFromAdmin(
	ctx context.Context,
	xxlCli *xxlclient.Client,
	task *xxljobv1alpha1.XxlJobTask,
	execGroupID int,
	ownerKey string,
) error {
	findOwnedJob := func() (*xxlclient.JobInfo, error) {
		if task.Status.JobID > 0 {
			j, err := xxlCli.GetJobInfoByID(ctx, task.Status.JobGroupID, task.Status.JobID)
			if err != nil {
				return nil, err
			}
			if j == nil && execGroupID > 0 && execGroupID != task.Status.JobGroupID {
				j, err = xxlCli.GetJobInfoByID(ctx, execGroupID, task.Status.JobID)
				if err != nil {
					return nil, err
				}
			}
			if j != nil && xxlclient.IsOwnerCompatible(j.JobDesc, ownerKey) {
				return j, nil
			}
		}
		if task.Status.JobGroupID > 0 {
			byOwner, err := xxlCli.FindJobInfoByOwnerKey(ctx, task.Status.JobGroupID, ownerKey)
			if err != nil {
				return nil, err
			}
			if byOwner != nil {
				return byOwner, nil
			}
		}
		if execGroupID > 0 && execGroupID != task.Status.JobGroupID {
			byOwner, err := xxlCli.FindJobInfoByOwnerKey(ctx, execGroupID, ownerKey)
			if err != nil {
				return nil, err
			}
			if byOwner != nil {
				return byOwner, nil
			}
		}
		return nil, nil
	}

	invalidateGroups := func() {
		if task.Status.JobGroupID > 0 {
			xxlCli.InvalidateGroupSnapshot(task.Status.JobGroupID)
		}
		if execGroupID > 0 && execGroupID != task.Status.JobGroupID {
			xxlCli.InvalidateGroupSnapshot(execGroupID)
		}
	}

	targetJob, err := findOwnedJob()
	if err != nil {
		return err
	}
	if targetJob == nil {
		invalidateGroups()
		targetJob, err = findOwnedJob()
		if err != nil {
			return err
		}
	}
	if targetJob == nil {
		return nil
	}
	_ = xxlCli.StopJobInfo(ctx, targetJob.ID)
	if err := xxlCli.RemoveJobInfo(ctx, targetJob.ID); err != nil {
		if isAdminBusinessRejection(err) {
			invalidateGroups()
			if stillThere, findErr := findOwnedJob(); findErr == nil && stillThere == nil {
				return nil
			}
		}
		return err
	}
	return nil
}

func (r *XxlJobTaskReconciler) cleanupTaskOnPreviousAdmin(
	ctx context.Context,
	reader client.Reader,
	task *xxljobv1alpha1.XxlJobTask,
	fallbackAdminName, prevAdminURL string,
	prevJobGroup, prevJobID int,
	ownerKey string,
) error {
	prevAdminURL = strings.TrimRight(strings.TrimSpace(prevAdminURL), "/")
	if task == nil || prevAdminURL == "" || (prevJobID <= 0 && prevJobGroup <= 0) {
		return nil
	}
	savedMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
	matchedAdmin, prevCli, err := resolveReachablePreviousAdminClient(
		ctx, r.Client, reader, r.ClientFactory,
		task.Namespace, prevAdminURL, savedMeta, fallbackAdminName,
	)
	if matchedAdmin != nil && isRetiredManagedAdminEndpoint(matchedAdmin, prevAdminURL, savedMeta) {
		return nil
	}
	if matchedAdmin == nil && err == nil {
		return nil
	}
	bypassPrev := matchedAdmin != nil && !matchedAdmin.DeletionTimestamp.IsZero() && shouldBypassRemoteCleanup(ctx, reader, matchedAdmin)
	if err != nil {
		if bypassPrev {
			return nil
		}
		return err
	}
	if prevCli == nil {
		return nil
	}
	findOnPrev := func() (*xxlclient.JobInfo, error) {
		if prevJobID > 0 {
			existingJob, err := prevCli.GetJobInfoByID(ctx, prevJobGroup, prevJobID)
			if err != nil {
				return nil, err
			}
			if existingJob != nil && xxlclient.IsOwnerCompatible(existingJob.JobDesc, ownerKey) {
				return existingJob, nil
			}
		}
		if prevJobGroup > 0 {
			byOwner, err := prevCli.FindJobInfoByOwnerKey(ctx, prevJobGroup, ownerKey)
			if err != nil {
				return nil, err
			}
			if byOwner != nil {
				return byOwner, nil
			}
		}
		return nil, nil
	}

	targetJob, err := findOnPrev()
	if err != nil {
		if bypassPrev {
			return nil
		}
		return err
	}
	if targetJob == nil && prevJobGroup > 0 {
		prevCli.InvalidateGroupSnapshot(prevJobGroup)
		targetJob, err = findOnPrev()
		if err != nil {
			if bypassPrev {
				return nil
			}
			return err
		}
	}
	if targetJob != nil {
		_ = prevCli.StopJobInfo(ctx, targetJob.ID)
		if err := prevCli.RemoveJobInfo(ctx, targetJob.ID); err != nil {
			if isAdminBusinessRejection(err) {
				prevCli.InvalidateGroupSnapshot(prevJobGroup)
				if stillThere, findErr := findOnPrev(); findErr == nil && stillThere == nil {
					return nil
				}
			}
			if bypassPrev {
				return nil
			}
			return err
		}
	}
	return nil
}

func jobMatchesDeletedTaskFallback(j *batchv1.Job, task *xxljobv1alpha1.XxlJobTask, taskMeta *syncedAdminEndpointMeta) bool {
	if j == nil || task == nil {
		return false
	}
	expectedExec := strings.TrimSpace(task.Spec.ExecutorRef.Name)
	if expectedExec != "" {
		if j.Annotations != nil {
			if execAnnot := strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationExecutorName]); execAnnot != "" && execAnnot != expectedExec {
				return false
			}
		}
		if j.Labels != nil {
			if execLbl := strings.TrimSpace(j.Labels[proxyexecutor.LabelExecutorName]); execLbl != "" && execLbl != proxyexecutor.SanitizeLabel(expectedExec) {
				return false
			}
		}
	}
	taskAdminURL := strings.TrimRight(strings.TrimSpace(task.Status.AdminURL), "/")
	if j.Annotations != nil {
		jobAdminUID := strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationAdminUID])
		if taskMeta != nil && taskMeta.AdminUID != "" && jobAdminUID != "" && taskMeta.AdminUID != jobAdminUID {
			return false
		}
		jobDBHash := strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationAdminDatasourceHash])
		if taskMeta != nil && taskMeta.DatasourceHash != "" && jobDBHash != "" && taskMeta.DatasourceHash != jobDBHash {
			return false
		}
		jobAdminURL := strings.TrimRight(strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationAdminURL]), "/")
		if taskAdminURL != "" && jobAdminURL != "" && jobAdminURL != taskAdminURL {
			jobAdminName := strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationAdminName])
			sameManaged := taskMeta != nil &&
				taskMeta.AdminName != "" &&
				(jobAdminName == "" || jobAdminName == taskMeta.AdminName) &&
				taskMeta.DatasourceHash != "" &&
				jobDBHash == taskMeta.DatasourceHash
			if !sameManaged {
				return false
			}
		}
	}
	return true
}

// cleanupActiveTaskJobs suspends and deletes all incomplete batch/v1.Jobs for this task across any executor or historical jobId in the namespace (fixes R6-P2-1, R6-P2-14, R9-P1-2).
func (r *XxlJobTaskReconciler) cleanupActiveTaskJobs(ctx context.Context, reader client.Reader, task *xxljobv1alpha1.XxlJobTask) error {
	matchLabels := client.MatchingLabels{
		proxyexecutor.LabelManagedBy: proxyexecutor.ManagedByValue,
	}
	var jobList batchv1.JobList
	if err := reader.List(ctx, &jobList, client.InNamespace(task.Namespace), matchLabels); err != nil {
		return err
	}
	policy := metav1.DeletePropagationForeground
	suspendTrue := true
	liveQueues := make(map[[2]string]bool)
	candidateClearQueues := make(map[[2]string]bool)
	taskMeta := extractSyncedAdminEndpointMeta(task.Status.Conditions)
	for i := range jobList.Items {
		j := &jobList.Items[i]
		taskNameAnnot := ""
		if j.Annotations != nil {
			taskNameAnnot = strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationTaskName])
		}
		belongsToTask := false
		if taskNameAnnot != "" {
			belongsToTask = (taskNameAnnot == task.Name)
		} else if task.Status.JobID > 0 && j.Labels != nil && j.Labels[proxyexecutor.LabelJobID] == strconv.Itoa(task.Status.JobID) {
			belongsToTask = jobMatchesDeletedTaskFallback(j, task, taskMeta)
		}
		if !belongsToTask {
			if j.DeletionTimestamp.IsZero() && j.Spec.Suspend != nil && *j.Spec.Suspend && j.Labels != nil {
				succeeded, failed, _ := proxyexecutor.EvaluateJobCompletion(j)
				if !succeeded && !failed && j.Labels[proxyexecutor.LabelExecutorName] != "" && j.Labels[proxyexecutor.LabelJobID] != "" {
					liveQueues[[2]string{j.Labels[proxyexecutor.LabelExecutorName], j.Labels[proxyexecutor.LabelJobID]}] = true
				}
			}
			continue
		}
		if j.Labels != nil && j.Labels[proxyexecutor.LabelExecutorName] != "" && j.Labels[proxyexecutor.LabelJobID] != "" {
			candidateClearQueues[[2]string{j.Labels[proxyexecutor.LabelExecutorName], j.Labels[proxyexecutor.LabelJobID]}] = true
		}
		succeeded, failed, _ := proxyexecutor.EvaluateJobCompletion(j)
		if !j.DeletionTimestamp.IsZero() || succeeded || failed {
			continue
		}
		if j.Spec.Suspend == nil || !*j.Spec.Suspend {
			patch := client.MergeFrom(j.DeepCopy())
			j.Spec.Suspend = &suspendTrue
			_ = r.Patch(ctx, j, patch)
		}
		if err := r.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if task.Status.JobID > 0 {
		jobIDStr := strconv.Itoa(task.Status.JobID)
		candidateClearQueues[[2]string{proxyexecutor.SanitizeLabel(task.Spec.ExecutorRef.Name), jobIDStr}] = true
		for _, execLabel := range activeSerialQueueExecutorsForJobID(task.Namespace, jobIDStr) {
			candidateClearQueues[[2]string{execLabel, jobIDStr}] = true
		}
	}
	for qk := range candidateClearQueues {
		if !liveQueues[qk] {
			clearSerialQueueDepth(task.Namespace, qk[0], qk[1])
		}
	}
	return nil
}

func (r *XxlJobTaskReconciler) updateErrorStatus(ctx context.Context, task *xxljobv1alpha1.XxlJobTask, msg string, metaChanged ...bool) {
	hasMetaChange := len(metaChanged) > 0 && metaChanged[0]
	if !task.Status.Synced && task.Status.Message == msg && !hasMetaChange {
		return
	}
	apply := func(target *xxljobv1alpha1.XxlJobTask) {
		if hasMetaChange {
			target.Status.JobID = 0
			target.Status.JobGroupID = 0
			target.Status.AdminURL = ""
			target.Status.LastExecutionJobName = ""
			meta.RemoveStatusCondition(&target.Status.Conditions, ConditionAdminEndpointSynced)
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
	apply(task)
	if err := r.Status().Update(ctx, task); err != nil {
		if apierrors.IsConflict(err) && hasMetaChange {
			var latest xxljobv1alpha1.XxlJobTask
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, &latest); getErr == nil {
				apply(&latest)
				if retryErr := r.Status().Update(ctx, &latest); retryErr == nil {
					return
				}
			}
		}
		log.FromContext(ctx).V(1).Info("Failed to update XxlJobTask error status", "task", task.Name, "err", err)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *XxlJobTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 4}).
		For(&xxljobv1alpha1.XxlJobTask{}, builder.WithPredicates(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.AnnotationChangedPredicate{},
				deletionTimestampChangedPredicate(),
			),
		)).
		Watches(
			&xxljobv1alpha1.XxlJobExecutor{},
			handler.EnqueueRequestsFromMapFunc(r.mapExecutorToTasks),
			builder.WithPredicates(executorStatusChangedPredicate()),
		).
		Watches(
			&xxljobv1alpha1.XxlJobAdmin{},
			handler.EnqueueRequestsFromMapFunc(r.mapAdminToTasks),
			builder.WithPredicates(adminForExecutorPredicate()),
		).
		Complete(r)
}

func (r *XxlJobTaskReconciler) mapAdminToTasks(ctx context.Context, obj client.Object) []reconcile.Request {
	adminCR, ok := obj.(*xxljobv1alpha1.XxlJobAdmin)
	if !ok {
		return nil
	}
	candidateURLs := make(map[string]bool, 4)
	if u := strings.TrimRight(ResolveAdminURL(adminCR), "/"); u != "" {
		candidateURLs[u] = true
	}
	if su := strings.TrimRight(strings.TrimSpace(adminCR.Status.AdminURL), "/"); su != "" {
		candidateURLs[su] = true
	}
	for _, pu := range extractPreviousAdminURLs(adminCR) {
		if pu != "" {
			candidateURLs[pu] = true
		}
	}
	var execList xxljobv1alpha1.XxlJobExecutorList
	_ = r.List(ctx, &execList, client.InNamespace(adminCR.Namespace))
	matchingExecNames := make(map[string]bool)
	for i := range execList.Items {
		ex := &execList.Items[i]
		refNS := ex.Spec.AdminRef.Namespace
		if refNS == "" {
			refNS = ex.Namespace
		}
		if refNS == adminCR.Namespace && ex.Spec.AdminRef.Name == adminCR.Name {
			matchingExecNames[ex.Name] = true
		}
	}

	var list xxljobv1alpha1.XxlJobTaskList
	if err := r.List(ctx, &list, client.InNamespace(adminCR.Namespace)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		refNS := item.Spec.ExecutorRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		taskAdminURL := strings.TrimRight(strings.TrimSpace(item.Status.AdminURL), "/")
		if (refNS == adminCR.Namespace && matchingExecNames[item.Spec.ExecutorRef.Name]) ||
			(taskAdminURL != "" && candidateURLs[taskAdminURL]) {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.Namespace,
					Name:      item.Name,
				},
			})
		}
	}
	return reqs
}

func (r *XxlJobTaskReconciler) mapExecutorToTasks(ctx context.Context, obj client.Object) []reconcile.Request {
	execCR, ok := obj.(*xxljobv1alpha1.XxlJobExecutor)
	if !ok {
		return nil
	}
	var list xxljobv1alpha1.XxlJobTaskList
	if err := r.List(ctx, &list, client.InNamespace(execCR.Namespace)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		item := &list.Items[i]
		refNS := item.Spec.ExecutorRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		if refNS == execCR.Namespace &&
			(item.Spec.ExecutorRef.Name == execCR.Name ||
				(execCR.Status.JobGroupID > 0 && item.Status.JobGroupID == execCR.Status.JobGroupID)) {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.Namespace,
					Name:      item.Name,
				},
			})
		}
	}
	return reqs
}

func executorStatusChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			execCR, ok := e.Object.(*xxljobv1alpha1.XxlJobExecutor)
			return ok && execCR.Status.Synced && execCR.Status.JobGroupID > 0
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldExec, ok1 := e.ObjectOld.(*xxljobv1alpha1.XxlJobExecutor)
			newExec, ok2 := e.ObjectNew.(*xxljobv1alpha1.XxlJobExecutor)
			if !ok1 || !ok2 {
				return false
			}
			oldEndpointMsg := ""
			if c := meta.FindStatusCondition(oldExec.Status.Conditions, ConditionAdminEndpointSynced); c != nil {
				oldEndpointMsg = c.Message
			}
			newEndpointMsg := ""
			if c := meta.FindStatusCondition(newExec.Status.Conditions, ConditionAdminEndpointSynced); c != nil {
				newEndpointMsg = c.Message
			}
			return oldExec.Status.JobGroupID != newExec.Status.JobGroupID ||
				oldExec.Status.AdminURL != newExec.Status.AdminURL ||
				oldExec.Status.Synced != newExec.Status.Synced ||
				oldExec.Spec.AdminRef != newExec.Spec.AdminRef ||
				oldExec.Spec.AppName != newExec.Spec.AppName ||
				oldExec.DeletionTimestamp.IsZero() != newExec.DeletionTimestamp.IsZero() ||
				extractSyncedExecutorAppName(oldExec) != extractSyncedExecutorAppName(newExec) ||
				oldEndpointMsg != newEndpointMsg
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
	}
}
