package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LocalObjectReference references a Kubernetes resource in the same or specified namespace.
type LocalObjectReference struct {
	// Name of the referent.
	Name string `json:"name"`
	// Namespace of the referent. Defaults to the current resource's namespace if empty.
	Namespace string `json:"namespace,omitempty"`
}

// SecretKeyReference selects a key of a Secret in the same namespace.
type SecretKeyReference struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Key in the Secret data map.
	Key string `json:"key"`
}

// DatasourceSpec defines MySQL database connection settings for XXL-JOB Admin.
type DatasourceSpec struct {
	// URL is the JDBC connection URL, e.g., jdbc:mysql://mysql:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8
	URL string `json:"url"`
	// Username for database authentication.
	Username string `json:"username"`
	// PasswordSecretRef references a Secret key containing the database password.
	PasswordSecretRef *SecretKeyReference `json:"passwordSecretRef,omitempty"`
	// Password plaintext fallback (recommended only for dev/test; prefer PasswordSecretRef in production).
	Password string `json:"password,omitempty"`
}

// AuthSpec defines XXL-JOB Admin login credentials and executor accessToken.
type AuthSpec struct {
	// Username for XXL-JOB Admin web login (defaults to "admin").
	Username string `json:"username,omitempty"`
	// PasswordSecretRef references a Secret key containing the Admin web login password.
	PasswordSecretRef *SecretKeyReference `json:"passwordSecretRef,omitempty"`
	// Password plaintext fallback for Admin login (defaults to "123456" if omitted).
	Password string `json:"password,omitempty"`
	// AccessTokenSecretRef references a Secret key containing the XXL-JOB-ACCESS-TOKEN.
	AccessTokenSecretRef *SecretKeyReference `json:"accessTokenSecretRef,omitempty"`
	// AccessToken plaintext fallback for executor communication (defaults to "default_token").
	AccessToken string `json:"accessToken,omitempty"`
}

// XxlJobAdminSpec defines the desired state of XxlJobAdmin.
type XxlJobAdminSpec struct {
	// Image is the container image for XXL-JOB Admin (e.g., xuxueli/xxl-job-admin:2.4.1).
	Image string `json:"image,omitempty"`
	// Replicas is the desired number of Admin pods (supports HA via DB lock xxl_job_lock).
	Replicas *int32 `json:"replicas,omitempty"`
	// Port is the HTTP listen port of XXL-JOB Admin (default: 8080).
	Port int32 `json:"port,omitempty"`
	// ContextPath is the servlet context path (default: /xxl-job-admin).
	ContextPath string `json:"contextPath,omitempty"`
	// ExternalURL is an optional externally reachable URL if Admin is not deployed in-cluster.
	ExternalURL string `json:"externalURL,omitempty"`
	// Datasource configures the MySQL backend required by XXL-JOB Admin.
	Datasource DatasourceSpec `json:"datasource,omitempty"`
	// Auth configures web console credentials and XXL-JOB-ACCESS-TOKEN.
	Auth AuthSpec `json:"auth,omitempty"`
	// ServiceType specifies the Kubernetes Service type (ClusterIP, NodePort, LoadBalancer).
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`
	// ExtraJavaOpts allows passing extra JVM flags (e.g., -Xms512m -Xmx512m).
	ExtraJavaOpts string `json:"extraJavaOpts,omitempty"`
	// Resources defines CPU/Memory resource requests and limits for Admin pods.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// XxlJobAdminStatus defines the observed state of XxlJobAdmin.
type XxlJobAdminStatus struct {
	// Phase indicates the high-level lifecycle phase: Pending, Running, Degraded.
	Phase string `json:"phase,omitempty"`
	// ReadyReplicas is the number of ready Admin replicas.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// AdminURL is the resolved cluster-internal or external base URL for API calls.
	AdminURL string `json:"adminURL,omitempty"`
	// ObservedGeneration is the last generation reconciled by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions represent the latest available observations of XxlJobAdmin's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="AdminURL",type="string",JSONPath=".status.adminURL"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// XxlJobAdmin is the Schema for the xxljobadmins API.
type XxlJobAdmin struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XxlJobAdminSpec   `json:"spec,omitempty"`
	Status XxlJobAdminStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XxlJobAdminList contains a list of XxlJobAdmin.
type XxlJobAdminList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XxlJobAdmin `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XxlJobAdmin{}, &XxlJobAdminList{})
}
