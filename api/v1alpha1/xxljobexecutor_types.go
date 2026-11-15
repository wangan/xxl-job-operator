package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExecutorMode defines the architectural stream used by the executor group in Kubernetes.
type ExecutorMode string

const (
	// ExecutorModeResident represents Stream 1: Resident executor pods running in K8s,
	// with Operator-assisted active endpoint registration and PreStop graceful draining.
	ExecutorModeResident ExecutorMode = "Resident"

	// ExecutorModeSidecar represents Stream 2: Zero-code-change business pods injected with
	// an xxl-job-sidecar proxy container via MutatingAdmissionWebhook.
	ExecutorModeSidecar ExecutorMode = "Sidecar"

	// ExecutorModeK8sJobProxy represents Stream 3: Serverless/Proxy executor mode where
	// XXL-JOB triggers are translated into ephemeral Kubernetes batch/v1 Jobs on demand.
	ExecutorModeK8sJobProxy ExecutorMode = "K8sJobProxy"
)

// XxlJobExecutorSpec defines the desired state of XxlJobExecutor (maps to XXL-JOB JobGroup).
type XxlJobExecutorSpec struct {
	// AdminRef references the XxlJobAdmin instance managing this executor group.
	AdminRef LocalObjectReference `json:"adminRef"`
	// AppName is the unique XXL-JOB executor AppName (e.g., "order-service-executor").
	AppName string `json:"appName"`
	// Title is the human-readable name of the executor group in XXL-JOB Admin.
	Title string `json:"title"`
	// Mode specifies the execution stream: Resident, Sidecar, or K8sJobProxy.
	Mode ExecutorMode `json:"mode"`
	// AddressType in XXL-JOB Admin: 0 = Auto registration, 1 = Manual / Operator-managed address list.
	// If nil, Operator automatically selects the optimal strategy based on Mode.
	AddressType *int `json:"addressType,omitempty"`
	// PodSelector selects resident or sidecar pods belonging to this executor group
	// so the Operator can watch Pod readiness and proactively drain terminating pods.
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`
	// ExecutorPort is the netty/embed HTTP port exposed by the executor (default: 9999).
	ExecutorPort int32 `json:"executorPort,omitempty"`
	// UseServiceDNS registers stable Kubernetes Service DNS instead of ephemeral Pod IPs when true.
	UseServiceDNS bool `json:"useServiceDNS,omitempty"`
	// ServiceName is the Kubernetes Service name when UseServiceDNS is true.
	ServiceName string `json:"serviceName,omitempty"`
	// StaticAddresses allows specifying explicit executor addresses when AddressType=1 and no PodSelector is used.
	StaticAddresses []string `json:"staticAddresses,omitempty"`
	// ProxyServiceAddress overrides the Operator's built-in K8sJobProxy executor address when Mode=K8sJobProxy.
	ProxyServiceAddress string `json:"proxyServiceAddress,omitempty"`
	// GracefulDrainSeconds is the preStop wait window after deregistering a Pod from XXL-JOB Admin (default: 15).
	GracefulDrainSeconds int32 `json:"gracefulDrainSeconds,omitempty"`
	// AllowedServiceAccounts lists the Kubernetes ServiceAccount names that XxlJobTasks under this executor
	// are permitted to use in K8sJobProxy mode (defaults to allowing only "default" when empty; "*" allows any SA in the namespace).
	AllowedServiceAccounts []string `json:"allowedServiceAccounts,omitempty"`
	// PodSecurityLevel is the Pod Security Standard level (privileged, baseline, restricted) that K8sJobProxy
	// Job pods created for tasks of this executor must satisfy before the Operator creates them (default: restricted).
	PodSecurityLevel string `json:"podSecurityLevel,omitempty"`
	// SidecarEnableGlue enables local GLUE script execution in injected sidecars of this executor (default: false).
	SidecarEnableGlue bool `json:"sidecarEnableGlue,omitempty"`
}

// XxlJobExecutorStatus defines the observed state of XxlJobExecutor.
type XxlJobExecutorStatus struct {
	// JobGroupID is the numeric ID assigned by XXL-JOB Admin (xxl_job_group.id).
	JobGroupID int `json:"jobGroupId,omitempty"`
	// AdminURL caches the resolved XXL-JOB Admin URL for safe cleanup during deletion.
	AdminURL string `json:"adminURL,omitempty"`
	// Synced indicates whether the executor group is synchronized with XXL-JOB Admin.
	Synced bool `json:"synced"`
	// RegisteredAddresses lists the currently active executor URLs registered in XXL-JOB Admin.
	RegisteredAddresses []string `json:"registeredAddresses,omitempty"`
	// RegistryKeepAliveAddresses lists draining-but-still-running pod URLs (addressType=1 only) whose
	// xxl_job_registry heartbeat is kept alive by the RegistryHeartbeatRunner until the pod terminates.
	RegistryKeepAliveAddresses []string `json:"registryKeepAliveAddresses,omitempty"`
	// DrainedAddresses lists the currently terminating/unready pod URLs that have been drained.
	DrainedAddresses []string `json:"drainedAddresses,omitempty"`
	// ActivePods is the count of ready, non-terminating pods matching PodSelector.
	ActivePods int `json:"activePods,omitempty"`
	// TerminatingPodsDrained is the count of currently terminating/unready pods drained.
	TerminatingPodsDrained int `json:"terminatingPodsDrained,omitempty"`
	// LastSyncTime is the timestamp of the last successful reconciliation with Admin.
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// Message provides human-readable status or error details.
	Message string `json:"message,omitempty"`
	// Conditions represent the latest available observations of XxlJobExecutor's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="AppName",type="string",JSONPath=".spec.appName"
// +kubebuilder:printcolumn:name="Mode",type="string",JSONPath=".spec.mode"
// +kubebuilder:printcolumn:name="GroupID",type="integer",JSONPath=".status.jobGroupId"
// +kubebuilder:printcolumn:name="ActivePods",type="integer",JSONPath=".status.activePods"
// +kubebuilder:printcolumn:name="Synced",type="boolean",JSONPath=".status.synced"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// XxlJobExecutor is the Schema for the xxljobexecutors API.
type XxlJobExecutor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XxlJobExecutorSpec   `json:"spec,omitempty"`
	Status XxlJobExecutorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XxlJobExecutorList contains a list of XxlJobExecutor.
type XxlJobExecutorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XxlJobExecutor `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XxlJobExecutor{}, &XxlJobExecutorList{})
}
