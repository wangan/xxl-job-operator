package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

// K8sJobCallbackReconciler is the single leader-elected controller responsible for:
//  1. Delivering completion or deletion callbacks (/api/callback) to XXL-JOB Admin BEFORE marking
//     xxljob.io/callback-sent=true and removing xxljob.io/callback-guard finalizer (fixes N-P0-4 & R4-P1-2).
//  2. Advancing persistent SERIAL_EXECUTION queues via APIReader (single writer, no cache lag) by unsuspending
//     the single earliest queued batch/v1.Job in strict logId FIFO order (fixes P1-1 & R4-P1-1).
//  3. Detecting Pod creation failures (e.g. PodSecurity Admission FailedCreate events) so stuck Jobs
//     fail fast and do not block SERIAL_EXECUTION queues (fixes R5-P2-2).
type K8sJobCallbackReconciler struct {
	client.Client
	APIReader     client.Reader
	Scheme        *runtime.Scheme
	ClientFactory ClientFactory
}

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=update

func (r *K8sJobCallbackReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	var job batchv1.Job
	if err := reader.Get(ctx, req.NamespacedName, &job); err != nil {
		if apierrors.IsNotFound(err) {
			clearCallbackPendingByJobKey(req.Namespace, req.Name)
			if advErr := recoverSerialQueuesOnDeletedJob(ctx, reader, r.Client, req.Namespace, req.Name); advErr != nil {
				logger.Error(advErr, "Failed to recover SERIAL_EXECUTION queue after Job disappeared", "job", req.NamespacedName)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if job.Labels == nil || job.Labels[proxyexecutor.LabelManagedBy] != proxyexecutor.ManagedByValue {
		return ctrl.Result{}, nil
	}

	execLabel := job.Labels[proxyexecutor.LabelExecutorName]
	jobIDLabel := job.Labels[proxyexecutor.LabelJobID]

	// Case 1: The Job is being deleted (/kill, COVER_EARLY, Task deletion, TTL, or kubectl delete) (fixes R4-P1-2, R5-P2-5, R7-P2-7, R8-P1-1)
	if !job.ObjectMeta.DeletionTimestamp.IsZero() {
		if job.Spec.Suspend == nil || !*job.Spec.Suspend {
			patch := client.MergeFrom(job.DeepCopy())
			suspend := true
			job.Spec.Suspend = &suspend
			_ = r.Patch(ctx, &job, patch)
		}

		if controllerutil.ContainsFinalizer(&job, proxyexecutor.JobCallbackFinalizer) {
			callbackSent := job.Annotations != nil && job.Annotations[proxyexecutor.AnnotationCallbackSent] == "true"
			if !callbackSent {
				markCallbackPending(job.Namespace, execLabel, job.Name)
				succeeded, failed, msg := proxyexecutor.EvaluateJobCompletion(&job)
				if !succeeded && !failed {
					failed = true
					if job.Annotations != nil && job.Annotations[proxyexecutor.AnnotationFailureMessage] != "" {
						msg = job.Annotations[proxyexecutor.AnnotationFailureMessage]
					} else {
						msg = fmt.Sprintf("K8s Job %s/%s was killed or deleted before completion", job.Namespace, job.Name)
					}
				}
				if err := r.deliverCallback(ctx, reader, &job, succeeded, failed, msg); err != nil {
					if !shouldBypassRemoteCleanup(ctx, reader, &job) {
						logger.Error(err, "Failed to deliver callback for deleting K8s Job, will retry", "job", job.Name)
						return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
					}
					logger.Info("Bypassing callback error for deleting K8s Job due to namespace termination or timeout", "job", job.Name)
				}
				clearCallbackPending(job.Namespace, execLabel, job.Name)
			} else {
				clearCallbackPending(job.Namespace, execLabel, job.Name)
			}

			// Retain JobCallbackFinalizer while the deleting Job still has active or terminating Pods so the Job stays
			// in etcd until its Pods exit and AdvanceSerialQueueWithReader can wake the next queued SERIAL_EXECUTION Job (fixes R8-P1-1).
			if proxyexecutor.HasActiveOrTerminatingPods(&job) && !shouldBypassRemoteCleanup(ctx, reader, &job) {
				if !callbackSent {
					if err := r.persistJobCallbackState(ctx, reader, &job, true, false); err != nil && !apierrors.IsNotFound(err) {
						return ctrl.Result{}, err
					}
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}

			if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
				if !callbackSent {
					_ = r.persistJobCallbackState(ctx, reader, &job, true, false)
				}
				logger.Error(err, "Failed to advance SERIAL_EXECUTION queue on Job deletion", "job", job.Name)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}

			if err := r.persistJobCallbackState(ctx, reader, &job, true, true); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
				logger.Error(err, "Failed to advance SERIAL_EXECUTION queue after removing finalizer on deleting Job", "job", job.Name)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			return ctrl.Result{}, nil
		}

		clearCallbackPending(job.Namespace, execLabel, job.Name)
		if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
			logger.Error(err, "Failed to advance SERIAL_EXECUTION queue on Job deletion", "job", job.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Case 2: The Job is still active or suspended
	succeeded, failed, msg := proxyexecutor.EvaluateJobCompletion(&job)
	if !succeeded && !failed {
		// If this Job is suspended:
		//  - For SERIAL_EXECUTION, check via APIReader whether it is the head of the queue.
		//  - For non-SERIAL_EXECUTION (DISCARD_LATER / COVER_EARLY), if it has remained suspended for >=30s
		//    (beyond handleRun's 20s claimCtx), the trigger was interrupted or aborted and must be deleted with a 500 callback (fixes R7-P1-3, R8-P1-3).
		if job.Spec.Suspend != nil && *job.Spec.Suspend {
			blockStrategy := ""
			if job.Annotations != nil {
				blockStrategy = strings.ToUpper(strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationBlockStrategy]))
			}
			if blockStrategy != "" && blockStrategy != "SERIAL_EXECUTION" {
				if job.CreationTimestamp.IsZero() || time.Since(job.CreationTimestamp.Time) < 30*time.Second {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				failed = true
				msg = fmt.Sprintf("K8s Job %s/%s remained suspended after interrupted %s trigger and was aborted", job.Namespace, job.Name, blockStrategy)
				patch := client.MergeFrom(job.DeepCopy())
				if job.Annotations == nil {
					job.Annotations = make(map[string]string)
				}
				job.Annotations[proxyexecutor.AnnotationFailureMessage] = msg
				if patchErr := r.Patch(ctx, &job, patch); patchErr != nil && !apierrors.IsNotFound(patchErr) {
					return ctrl.Result{}, patchErr
				}

				policy := metav1.DeletePropagationForeground
				if delErr := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &policy}); delErr != nil && !apierrors.IsNotFound(delErr) {
					return ctrl.Result{}, delErr
				}
				if getErr := reader.Get(ctx, req.NamespacedName, &job); getErr != nil {
					return ctrl.Result{}, client.IgnoreNotFound(getErr)
				}
			} else {
				if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
		} else if failedMsg, isFailedCreate := r.checkJobFailedCreateEvent(ctx, reader, &job); isFailedCreate {
			failed = true
			msg = failedMsg
			patch := client.MergeFrom(job.DeepCopy())
			suspend := true
			job.Spec.Suspend = &suspend
			if job.Annotations == nil {
				job.Annotations = make(map[string]string)
			}
			job.Annotations[proxyexecutor.AnnotationFailureMessage] = failedMsg
			if patchErr := r.Patch(ctx, &job, patch); patchErr != nil && !apierrors.IsNotFound(patchErr) {
				return ctrl.Result{}, patchErr
			}

			policy := metav1.DeletePropagationForeground
			if delErr := r.Delete(ctx, &job, &client.DeleteOptions{PropagationPolicy: &policy}); delErr != nil && !apierrors.IsNotFound(delErr) {
				return ctrl.Result{}, delErr
			}
			if getErr := reader.Get(ctx, req.NamespacedName, &job); getErr != nil {
				return ctrl.Result{}, client.IgnoreNotFound(getErr)
			}
		} else if job.Status.Active == 0 && job.Status.Succeeded == 0 {
			// Requeue while an unsuspended Job still has 0 active Pods so asynchronous FailedCreate Events
			// written after the Job status update are always detected without watching cluster-wide Events (fixes R6-P1-6).
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		} else {
			return ctrl.Result{}, nil
		}
	}

	// Case 3: The Job has completed (succeeded or failed, or was just aborted via Case 2).
	// Deliver /api/callback to XXL-JOB Admin BEFORE unsuspending the next SERIAL_EXECUTION Job so on the
	// healthy path Job N's completion callback strictly precedes Job N+1's execution in XXL-JOB Admin.
	callbackSent := job.Annotations != nil && job.Annotations[proxyexecutor.AnnotationCallbackSent] == "true"
	if callbackSent && !controllerutil.ContainsFinalizer(&job, proxyexecutor.JobCallbackFinalizer) {
		clearCallbackPending(job.Namespace, execLabel, job.Name)
		if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
			logger.Error(err, "Failed to advance SERIAL_EXECUTION queue", "job", job.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	if !callbackSent {
		markCallbackPending(job.Namespace, execLabel, job.Name)

		// Send callback to XXL-JOB Admin FIRST; only mark callback-sent=true after Callback() succeeds (fixes N-P0-4).
		if err := r.deliverCallback(ctx, reader, &job, succeeded, failed, msg); err != nil {
			if !shouldBypassCompletedJobCleanup(ctx, reader, &job) {
				// If all Pods of this completed Job have already exited, still advance the local K8s serial queue
				// so a transient XXL-JOB Admin outage does not stall queued K8s executions while callback retries continue.
				if !proxyexecutor.HasActiveOrTerminatingPods(&job) {
					_ = proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel)
				}
				logger.Error(err, "Failed to send completion callback to XXL-JOB Admin, will retry", "job", job.Name)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			logger.Info("Bypassing completion callback error for K8s Job due to namespace termination or force-delete/timeout", "job", job.Name)
		}
		clearCallbackPending(job.Namespace, execLabel, job.Name)
	} else {
		clearCallbackPending(job.Namespace, execLabel, job.Name)
	}

	hadActiveOrTerminatingPods := proxyexecutor.HasActiveOrTerminatingPods(&job)
	// Retain JobCallbackFinalizer while a failed/timed-out Job (e.g. DeadlineExceeded / BackoffLimitExceeded) still has
	// active or terminating Pods so the controller keeps polling until those Pods exit and wakes the next queued SERIAL_EXECUTION Job (fixes R9-P1-1, R11-P1-3).
	if hadActiveOrTerminatingPods && !shouldBypassCompletedJobCleanup(ctx, reader, &job) {
		if !callbackSent {
			if err := r.persistJobCallbackState(ctx, reader, &job, true, false); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	if !hadActiveOrTerminatingPods {
		if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
			if !callbackSent {
				_ = r.persistJobCallbackState(ctx, reader, &job, true, false)
			}
			logger.Error(err, "Failed to advance SERIAL_EXECUTION queue", "job", job.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}

	// Remove callback-guard finalizer once callback has been delivered and all Pods have exited (or cleanup was bypassed)
	// so TTL controller can GC the completed Job cleanly, then advance the serial queue if HasActiveOrTerminatingPods
	// was bypassed via shouldBypassCompletedJobCleanup (fixes R11-P1-3).
	if err := r.persistJobCallbackState(ctx, reader, &job, true, true); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if hadActiveOrTerminatingPods {
		if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, r.Client, job.Namespace, execLabel, jobIDLabel); err != nil {
			logger.Error(err, "Failed to advance SERIAL_EXECUTION queue after removing finalizer", "job", job.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}
	return ctrl.Result{}, nil
}

func (r *K8sJobCallbackReconciler) persistJobCallbackState(
	ctx context.Context,
	reader client.Reader,
	job *batchv1.Job,
	markCallbackSent bool,
	removeFinalizer bool,
) error {
	current := job.DeepCopy()
	for attempt := 0; attempt < 4; attempt++ {
		if markCallbackSent {
			if current.Annotations == nil {
				current.Annotations = make(map[string]string)
			}
			current.Annotations[proxyexecutor.AnnotationCallbackSent] = "true"
		}
		if removeFinalizer {
			controllerutil.RemoveFinalizer(current, proxyexecutor.JobCallbackFinalizer)
		}
		err := r.Update(ctx, current)
		if err == nil {
			*job = *current
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		var latest batchv1.Job
		if getErr := reader.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, &latest); getErr != nil {
			return getErr
		}
		current = &latest
	}
	return fmt.Errorf("conflict retrying callback state update for job %s/%s", job.Namespace, job.Name)
}

// checkJobFailedCreateEvent inspects Events in the Job's namespace to detect if the Job Controller failed
// to create Pods (e.g., due to PodSecurity Admission violations, missing ServiceAccount, or persistent quota errors) (fixes R5-P2-2 & R6-P1-6).
func (r *K8sJobCallbackReconciler) checkJobFailedCreateEvent(ctx context.Context, reader client.Reader, job *batchv1.Job) (string, bool) {
	if job.Status.Active > 0 || job.Status.Succeeded > 0 {
		return "", false
	}
	var eventList corev1.EventList
	if err := reader.List(ctx, &eventList, client.InNamespace(job.Namespace)); err != nil {
		return "", false
	}
	for i := range eventList.Items {
		ev := &eventList.Items[i]
		if ev.InvolvedObject.Kind != "Job" || ev.InvolvedObject.Name != job.Name {
			continue
		}
		if job.UID != "" && ev.InvolvedObject.UID != "" && ev.InvolvedObject.UID != job.UID {
			continue
		}
		if !strings.EqualFold(ev.Type, corev1.EventTypeWarning) || ev.Reason != "FailedCreate" {
			continue
		}
		// Give transient single-occurrence ResourceQuota contention a brief retry window measured from the Event's
		// first timestamp (or Job StartTime) rather than Job CreationTimestamp so queued SERIAL_EXECUTION Jobs
		// are not prematurely aborted on their first quota contention (fixes R7-P2-6).
		lowerMsg := strings.ToLower(ev.Message)
		if strings.Contains(lowerMsg, "exceeded quota") && ev.Count <= 1 {
			refTime := ev.FirstTimestamp.Time
			if refTime.IsZero() && !ev.EventTime.IsZero() {
				refTime = ev.EventTime.Time
			}
			if refTime.IsZero() && job.Status.StartTime != nil {
				refTime = job.Status.StartTime.Time
			}
			if refTime.IsZero() {
				refTime = job.CreationTimestamp.Time
			}
			if !refTime.IsZero() && time.Since(refTime) < 30*time.Second {
				continue
			}
		}
		return fmt.Sprintf("K8s Job %s/%s failed to create Pod (%s): %s", job.Namespace, job.Name, ev.Reason, ev.Message), true
	}
	return "", false
}

func (r *K8sJobCallbackReconciler) deliverCallback(
	ctx context.Context,
	reader client.Reader,
	job *batchv1.Job,
	succeeded, failed bool,
	msg string,
) error {
	// Enforce strict single-namespace isolation: callbacks for a batch/v1.Job may ONLY inspect CRs in job.Namespace
	// regardless of any user/pod-editable annotations (fixes EG2-09).
	ns := job.Namespace
	var taskName string
	var execName string
	var origAdminName string
	var origAdminURL string
	var origAdminUID string
	var origAdminDBHash string
	var logID int64
	var logDateTime int64

	if job.Annotations != nil {
		if annotNS := strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationTaskNamespace]); annotNS == "" || annotNS == ns {
			taskName = strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationTaskName])
		}
		execName = strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationExecutorName])
		origAdminName = strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationAdminName])
		origAdminURL = strings.TrimRight(strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationAdminURL]), "/")
		origAdminUID = strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationAdminUID])
		origAdminDBHash = strings.TrimSpace(job.Annotations[proxyexecutor.AnnotationAdminDatasourceHash])
		logID, _ = strconv.ParseInt(job.Annotations[proxyexecutor.AnnotationLogID], 10, 64)
		logDateTime, _ = strconv.ParseInt(job.Annotations[proxyexecutor.AnnotationLogDateTime], 10, 64)
	}
	if logID <= 0 && job.Labels != nil {
		logID, _ = strconv.ParseInt(job.Labels[proxyexecutor.LabelLogID], 10, 64)
	}
	if logID <= 0 {
		return nil
	}

	if taskName != "" && execName == "" {
		var task xxljobv1alpha1.XxlJobTask
		if err := reader.Get(ctx, types.NamespacedName{Namespace: ns, Name: taskName}, &task); err == nil {
			taskExecNS := task.Spec.ExecutorRef.Namespace
			if (taskExecNS == "" || taskExecNS == ns) && task.Spec.ExecutorRef.Name != "" {
				execName = task.Spec.ExecutorRef.Name
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Fallback if XxlJobTask was already deleted: resolve XxlJobExecutor from AnnotationExecutorName or LabelExecutorName (fixes R5-P2-5)
	if execName == "" && job.Labels != nil {
		execName = job.Labels[proxyexecutor.LabelExecutorName]
	}

	xxlCli, matchedAdmin, err := r.resolveCallbackAdminClient(ctx, reader, ns, execName, origAdminName, origAdminURL, origAdminUID, origAdminDBHash)
	bypassOriginAdmin := matchedAdmin != nil && !matchedAdmin.DeletionTimestamp.IsZero() && shouldBypassRemoteCleanup(ctx, reader, matchedAdmin)
	if err != nil {
		if bypassOriginAdmin {
			return nil
		}
		return err
	}
	if xxlCli == nil {
		return nil
	}

	code := xxlclient.SuccessCode
	if proxyexecutor.IsJobDeadlineExceeded(job) {
		code = xxlclient.TimeoutCode
	} else if failed || !succeeded {
		code = xxlclient.FailCode
	}

	err = xxlCli.Callback(ctx, []xxlclient.HandleCallbackParam{
		{
			LogID:         logID,
			LogDateTim:    logDateTime,
			HandleCode:    code,
			HandleMsg:     msg,
			ExecuteResult: msg,
		},
	})
	if xxlclient.IsTerminalCallbackRejection(err) {
		// Treat "repeate callback" / "log item not found" from XXL-JOB Admin as idempotent completion so a retry after
		// a transient K8s Status/Annotation update conflict never loops forever or leaks finalizers/metrics (fixes R10-P1-2).
		return nil
	}
	if err != nil && bypassOriginAdmin {
		return nil
	}
	return err
}

func (r *K8sJobCallbackReconciler) resolveCallbackAdminClient(
	ctx context.Context,
	reader client.Reader,
	namespace, execName, origAdminName, origAdminURL, origAdminUID, origAdminDBHash string,
) (*xxlclient.Client, *xxljobv1alpha1.XxlJobAdmin, error) {
	// 1. When the Job recorded its originating XxlJobAdmin at trigger time, route /api/callback strictly to that
	// originating Admin even if XxlJobExecutor.Spec.AdminRef was migrated or XxlJobAdmin.Spec.ExternalURL / Port /
	// ContextPath was edited in place while the Job was in flight (fixes EG2-09).
	if origAdminName != "" || origAdminURL != "" {
		var savedMeta *syncedAdminEndpointMeta
		if origAdminName != "" || origAdminUID != "" || origAdminDBHash != "" {
			savedMeta = &syncedAdminEndpointMeta{
				AdminName:        origAdminName,
				AdminUID:         origAdminUID,
				AdminURL:         origAdminURL,
				ManagedInCluster: origAdminDBHash != "",
				DatasourceHash:   origAdminDBHash,
			}
		}
		if origAdminName != "" {
			adminCR, cli, err := ResolveAdminAndClientWithReader(
				ctx, r.Client, reader, r.ClientFactory,
				xxljobv1alpha1.LocalObjectReference{Name: origAdminName},
				namespace,
			)
			sameUID := origAdminUID == "" || adminCR == nil || adminCR.UID == "" || string(adminCR.UID) == origAdminUID
			sameDB := !isAdminInstanceOrDBReplaced(adminCR, savedMeta)
			if err == nil && sameUID && sameDB {
				resolvedURL := strings.TrimRight(ResolveAdminURL(adminCR), "/")
				if origAdminURL == "" || resolvedURL == origAdminURL || isSameManagedAdminDBUpdate(adminCR, origAdminURL, savedMeta) {
					return cli, adminCR, nil
				}
			} else if adminCR != nil && sameUID && sameDB {
				// Admin CR exists, so err is a Secret/client error (even if apierrors.IsNotFound(err) is true for a missing Secret);
				// propagate err when URLs match (or same managed DB) so K8sJobCallbackReconciler retries instead of dropping the callback.
				resolvedURL := strings.TrimRight(ResolveAdminURL(adminCR), "/")
				if origAdminURL == "" || resolvedURL == origAdminURL || isSameManagedAdminDBUpdate(adminCR, origAdminURL, savedMeta) {
					return nil, adminCR, err
				}
			} else if err != nil && adminCR == nil && !apierrors.IsNotFound(err) {
				return nil, nil, err
			}
			if adminCR != nil && (!sameUID || !sameDB) {
				resolvedURL := strings.TrimRight(ResolveAdminURL(adminCR), "/")
				if origAdminURL == "" || resolvedURL == origAdminURL || isRetiredManagedAdminEndpoint(adminCR, origAdminURL, savedMeta) {
					return nil, nil, nil
				}
			}
		}
		if origAdminURL != "" {
			matchedAdmin, cli, err := resolveReachablePreviousAdminClient(
				ctx, r.Client, reader, r.ClientFactory,
				namespace, origAdminURL, savedMeta, origAdminName,
			)
			if matchedAdmin != nil && origAdminUID != "" && matchedAdmin.UID != "" && string(matchedAdmin.UID) != origAdminUID {
				return nil, nil, nil
			}
			if matchedAdmin != nil && isSameManagedAdminDBUpdate(matchedAdmin, origAdminURL, savedMeta) {
				updatedAdmin, updatedCli, updatedErr := ResolveAdminAndClientWithReader(
					ctx, r.Client, reader, r.ClientFactory,
					xxljobv1alpha1.LocalObjectReference{Name: matchedAdmin.Name},
					namespace,
				)
				return updatedCli, updatedAdmin, updatedErr
			}
			if matchedAdmin != nil && (isRetiredManagedAdminEndpoint(matchedAdmin, origAdminURL, savedMeta) ||
				(strings.TrimRight(ResolveAdminURL(matchedAdmin), "/") == origAdminURL && isAdminInstanceOrDBReplaced(matchedAdmin, savedMeta))) {
				return nil, nil, nil
			}
			if matchedAdmin != nil || err != nil {
				return cli, matchedAdmin, err
			}
		}
		// Originating XxlJobAdmin CR was removed from the namespace; never misroute logId onto a different Admin.
		return nil, nil, nil
	}

	// 2. Legacy fallback when originating Admin annotations were not stamped on the Job.
	if execName == "" {
		return nil, nil, nil
	}
	var execCR xxljobv1alpha1.XxlJobExecutor
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: execName}, &execCR); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	adminCR, cli, err := ResolveAdminAndClientWithReader(ctx, r.Client, reader, r.ClientFactory, execCR.Spec.AdminRef, namespace)
	if err != nil {
		return nil, adminCR, err
	}
	return cli, adminCR, nil
}

func recoverSerialQueuesOnDeletedJob(ctx context.Context, reader client.Reader, writer client.Client, namespace, deletedJobName string) error {
	matchLabels := client.MatchingLabels{
		proxyexecutor.LabelManagedBy: proxyexecutor.ManagedByValue,
	}
	if strings.HasPrefix(deletedJobName, "xxljob-") {
		parts := strings.Split(deletedJobName, "-")
		if len(parts) == 4 && parts[2] != "" {
			if _, err := strconv.Atoi(parts[2]); err == nil {
				matchLabels[proxyexecutor.LabelJobID] = parts[2]
			}
		}
	}
	var jobList batchv1.JobList
	if err := reader.List(ctx, &jobList, client.InNamespace(namespace), matchLabels); err != nil {
		return err
	}
	type queueKey struct {
		execLabel  string
		jobIDLabel string
	}
	seen := make(map[queueKey]bool)
	var firstErr error
	for i := range jobList.Items {
		j := &jobList.Items[i]
		if !j.DeletionTimestamp.IsZero() || j.Spec.Suspend == nil || !*j.Spec.Suspend || j.Labels == nil {
			continue
		}
		bs := strings.ToUpper(strings.TrimSpace(j.Annotations[proxyexecutor.AnnotationBlockStrategy]))
		if bs != "" && bs != "SERIAL_EXECUTION" {
			continue
		}
		qk := queueKey{
			execLabel:  j.Labels[proxyexecutor.LabelExecutorName],
			jobIDLabel: j.Labels[proxyexecutor.LabelJobID],
		}
		if qk.jobIDLabel == "" || seen[qk] {
			continue
		}
		seen[qk] = true
		if err := proxyexecutor.AdvanceSerialQueueWithReader(ctx, reader, writer, namespace, qk.execLabel, qk.jobIDLabel); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func shouldBypassCompletedJobCleanup(ctx context.Context, reader client.Reader, job *batchv1.Job) bool {
	if job == nil {
		return true
	}
	if shouldBypassRemoteCleanup(ctx, reader, job) {
		return true
	}
	if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() {
		if time.Since(job.Status.CompletionTime.Time) >= MaxFinalizerRetryDuration {
			return true
		}
	}
	for _, cond := range job.Status.Conditions {
		if (cond.Type == batchv1.JobComplete || cond.Type == batchv1.JobFailed) &&
			cond.Status == corev1.ConditionTrue &&
			!cond.LastTransitionTime.IsZero() &&
			time.Since(cond.LastTransitionTime.Time) >= MaxFinalizerRetryDuration {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the K8sJobCallbackReconciler with label filtering on managed batch/v1.Jobs.
func (r *K8sJobCallbackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	managedJobPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		return labels != nil && labels[proxyexecutor.LabelManagedBy] == proxyexecutor.ManagedByValue
	})
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: 4}).
		For(&batchv1.Job{}, builder.WithPredicates(managedJobPredicate)).
		Complete(r)
}
