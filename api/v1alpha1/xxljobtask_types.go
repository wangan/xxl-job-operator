package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// K8sJobTemplateSpec defines the Kubernetes Job template used when the task runs under K8sJobProxy mode.
type K8sJobTemplateSpec struct {
	// Image is the container image to run for the batch job. When omitted and the task uses a GLUE_* glueType,
	// a per-language fallback runtime image is selected automatically.
	Image string `json:"image,omitempty"`
	// AutomountServiceAccountToken controls whether the ServiceAccount token is mounted into the job pod (default: false).
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`
	// Command overrides the container entrypoint.
	Command []string `json:"command,omitempty"`
	// Args provides arguments to the container entrypoint.
	Args []string `json:"args,omitempty"`
	// Env specifies environment variables passed to the job container.
	Env []corev1.EnvVar `json:"env,omitempty"`
	// Resources defines CPU/Memory requests and limits for the job container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// ServiceAccountName specifies the K8s ServiceAccount for the job pod.
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// RestartPolicy for the job pod (defaults to Never).
	RestartPolicy corev1.RestartPolicy `json:"restartPolicy,omitempty"`
	// BackoffLimit specifies retries at the K8s Job level (defaults to 0 to let XXL-JOB manage retries).
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`
	// ActiveDeadlineSeconds caps the maximum execution duration of the K8s Job.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`
	// TTLSecondsAfterFinished automatically cleans up finished K8s Jobs/Pods after the specified seconds (default: 3600).
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
	// ImagePullPolicy specifies the image pull policy for the job container.
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// ImagePullSecrets is an optional list of references to secrets in the same namespace for pulling the image.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	// SecurityContext holds container-level security attributes (e.g., for PodSecurity Standards restricted profile).
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`
	// PodSecurityContext holds pod-level security attributes and common container settings.
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`
	// NodeSelector is a selector which must be true for the job pod to fit on a node.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Tolerations allow the job pod to schedule onto nodes with matching taints.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// Affinity specifies the scheduling constraints of the job pod.
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// Volumes represents a list of volumes that can be mounted by containers belonging to the job pod.
	Volumes []corev1.Volume `json:"volumes,omitempty"`
	// VolumeMounts are pod volumes to mount into the job-runner container's filesystem.
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
	// PodLabels are additional labels merged onto the created Job Pod template.
	PodLabels map[string]string `json:"podLabels,omitempty"`
	// PodAnnotations are additional annotations merged onto the created Job Pod template.
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`
}

// XxlJobTaskSpec defines the desired state of XxlJobTask (maps to XXL-JOB JobInfo).
type XxlJobTaskSpec struct {
	// ExecutorRef references the XxlJobExecutor CR that owns this scheduled task.
	ExecutorRef LocalObjectReference `json:"executorRef"`
	// JobDesc is the task description displayed in XXL-JOB Admin.
	JobDesc string `json:"jobDesc"`
	// Author is the owner/responsible person for this task.
	Author string `json:"author"`
	// AlarmEmail is the comma-separated list of alert notification emails.
	AlarmEmail string `json:"alarmEmail,omitempty"`
	// ScheduleType specifies the trigger type: CRON, FIX_RATE, or NONE.
	ScheduleType string `json:"scheduleType"`
	// ScheduleConf is the CRON expression (e.g., "0 */5 * * * ?") or fixed rate seconds.
	ScheduleConf string `json:"scheduleConf,omitempty"`
	// MisfireStrategy defines behavior on misfire: DO_NOTHING (default) or FIRE_ONCE_NOW.
	MisfireStrategy string `json:"misfireStrategy,omitempty"`
	// ExecutorRouteStrategy defines routing: FIRST, LAST, ROUND, RANDOM, CONSISTENT_HASH,
	// LEAST_FREQUENTLY_USED, LEAST_RECENTLY_USED, FAILOVER, BUSYOVER, SHARDING_BROADCAST.
	ExecutorRouteStrategy string `json:"executorRouteStrategy,omitempty"`
	// ExecutorHandler is the @XxlJob bean handler name (or script/job identifier; optional for GLUE_* script tasks).
	ExecutorHandler string `json:"executorHandler,omitempty"`
	// ExecutorParam is the execution parameter string passed to the job handler.
	ExecutorParam string `json:"executorParam,omitempty"`
	// ExecutorBlockStrategy defines concurrency control: SERIAL_EXECUTION, DISCARD_LATER, COVER_EARLY.
	ExecutorBlockStrategy string `json:"executorBlockStrategy,omitempty"`
	// ExecutorTimeout is the task execution timeout in seconds (0 = no timeout).
	ExecutorTimeout int `json:"executorTimeout,omitempty"`
	// ExecutorFailRetryCount is the number of automatic retries on failure.
	ExecutorFailRetryCount int `json:"executorFailRetryCount,omitempty"`
	// GlueType specifies execution mode: BEAN (default), GLUE_SHELL, GLUE_PYTHON, GLUE_NODEJS, or K8S_JOB.
	GlueType string `json:"glueType,omitempty"`
	// GlueSource holds the inline script content when GlueType is a GLUE_* script mode.
	GlueSource string `json:"glueSource,omitempty"`
	// GlueRemark is the version remark for GLUE script updates.
	GlueRemark string `json:"glueRemark,omitempty"`
	// ChildJobID is a comma-separated list of child job IDs triggered on completion.
	ChildJobID string `json:"childJobId,omitempty"`
	// TriggerStatus controls whether the task schedule is enabled (true = RUNNING, false = STOP).
	TriggerStatus bool `json:"triggerStatus"`
	// K8sJobTemplate defines the Kubernetes Job template when running via K8sJobProxy mode.
	K8sJobTemplate *K8sJobTemplateSpec `json:"k8sJobTemplate,omitempty"`
}

// XxlJobTaskStatus defines the observed state of XxlJobTask.
type XxlJobTaskStatus struct {
	// JobID is the numeric job ID assigned by XXL-JOB Admin (xxl_job_info.id).
	JobID int `json:"jobId,omitempty"`
	// JobGroupID is the numeric executor group ID resolved from ExecutorRef.
	JobGroupID int `json:"jobGroupId,omitempty"`
	// AdminURL caches the resolved XXL-JOB Admin URL for safe cleanup during deletion.
	AdminURL string `json:"adminURL,omitempty"`
	// Synced indicates whether the task configuration is synchronized with XXL-JOB Admin.
	Synced bool `json:"synced"`
	// TriggerStatus reflects the current trigger status in Admin (1 = RUNNING, 0 = STOP).
	TriggerStatus int `json:"triggerStatus,omitempty"`
	// LastSyncTime is the timestamp of the last successful reconciliation with Admin.
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// LastExecutionJobName records the most recent Kubernetes batch/v1.Job created for this task (in K8sJobProxy mode).
	LastExecutionJobName string `json:"lastExecutionJobName,omitempty"`
	// Message provides human-readable status or error details.
	Message string `json:"message,omitempty"`
	// Conditions represent the latest available observations of XxlJobTask's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Executor",type="string",JSONPath=".spec.executorRef.name"
// +kubebuilder:printcolumn:name="Handler",type="string",JSONPath=".spec.executorHandler"
// +kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.scheduleConf"
// +kubebuilder:printcolumn:name="JobID",type="integer",JSONPath=".status.jobId"
// +kubebuilder:printcolumn:name="Running",type="boolean",JSONPath=".spec.triggerStatus"
// +kubebuilder:printcolumn:name="Synced",type="boolean",JSONPath=".status.synced"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// XxlJobTask is the Schema for the xxljobtasks API.
type XxlJobTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XxlJobTaskSpec   `json:"spec,omitempty"`
	Status XxlJobTaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XxlJobTaskList contains a list of XxlJobTask.
type XxlJobTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XxlJobTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XxlJobTask{}, &XxlJobTaskList{})
}
