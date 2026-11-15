package webhook

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
)

const (
	// LabelInject is the Pod label matched by MutatingWebhookConfiguration.objectSelector ("true").
	LabelInject = "xxljob.io/inject"
	// LabelSidecarInjected is set on Pods after the sidecar container is injected.
	LabelSidecarInjected = "xxljob.io/sidecar-injected"
	// AnnotationMode specifies the injection mode: "resident" or "sidecar".
	AnnotationMode = "xxljob.io/mode"
	// AnnotationExecutorRef optionally specifies the XxlJobExecutor CR name in the same namespace.
	AnnotationExecutorRef = "xxljob.io/executor"
	// AnnotationAppName specifies the XXL-JOB executor appName (e.g., "order-service-executor").
	AnnotationAppName = "xxljob.io/app-name"
	// AnnotationAdminURL specifies the XXL-JOB Admin base URL.
	AnnotationAdminURL = "xxljob.io/admin-url"
	// AnnotationAccessToken specifies an explicit non-default XXL-JOB-ACCESS-TOKEN.
	AnnotationAccessToken = "xxljob.io/access-token"
	// AnnotationAccessTokenSecretName specifies the Secret name in the Pod's namespace holding XXL-JOB-ACCESS-TOKEN.
	AnnotationAccessTokenSecretName = "xxljob.io/access-token-secret-name"
	// AnnotationAccessTokenSecretKey specifies the key within AnnotationAccessTokenSecretName (default: "access-token").
	AnnotationAccessTokenSecretKey = "xxljob.io/access-token-secret-key"
	// AnnotationExecutorPort specifies the executor listen port (default: "9999").
	AnnotationExecutorPort = "xxljob.io/executor-port"
	// AnnotationTargetHTTPPort specifies the business container HTTP port in sidecar mode (default: "8080").
	AnnotationTargetHTTPPort = "xxljob.io/target-http-port"
	// AnnotationContainerName optionally selects the target business container name when mutating pods.
	AnnotationContainerName = "xxljob.io/container-name"
	// AnnotationDefaultContainer is the standard kubectl annotation for selecting the default container.
	AnnotationDefaultContainer = "kubectl.kubernetes.io/default-container"
	// AnnotationGracefulDrainSeconds specifies the preStop drain wait window in seconds (default: "15").
	AnnotationGracefulDrainSeconds = "xxljob.io/graceful-drain-seconds"
	// AnnotationAddressType optionally indicates "0" (auto-registration) or "1" (Operator-managed address_list, default).
	AnnotationAddressType = "xxljob.io/address-type"
	// AnnotationEnableGlue explicitly enables GLUE script execution in the injected sidecar ("true").
	AnnotationEnableGlue = "xxljob.io/enable-glue"
	// AnnotationSidecarEnableGlue is an alias annotation for enabling GLUE script execution in the sidecar ("true").
	AnnotationSidecarEnableGlue = "xxljob.io/sidecar-enable-glue"
	// AnnotationSidecarSharedSecret optionally overrides the per-pod shared secret between sidecar and business container.
	AnnotationSidecarSharedSecret = "xxljob.io/sidecar-shared-secret"
	// AnnotationInjectedStatus marks that the pod has been mutated by xxl-job-operator.
	AnnotationInjectedStatus = "xxljob.io/injected"

	SidecarContainerName   = "xxl-job-sidecar"
	SharedVolumeName       = "xxl-job-shared"
	SharedMountPath        = "/xxl-job/shared"
	SidecarSpoolVolumeName = "xxl-job-sidecar-spool"
	SidecarSpoolMountPath  = "/xxl-job/spool"
	SidecarCallbackDir     = "/xxl-job/spool/callbacks"
)

var knownInfraSidecarNames = map[string]bool{
	SidecarContainerName: true,
	"istio-proxy":        true,
	"envoy":              true,
	"linkerd-proxy":      true,
	"vault-agent":        true,
}

// PodExecutorMutator mutates Pods annotated with xxljob.io/mode=resident or xxljob.io/mode=sidecar.
type PodExecutorMutator struct {
	SidecarImage       string
	DefaultAdminURL    string
	DefaultAccessToken string
	K8sClient          client.Reader
}

// NewPodExecutorMutator creates a new PodExecutorMutator with sensible defaults.
// Note: DefaultAccessToken never falls back to "default_token" (fixes R5-P0-1).
func NewPodExecutorMutator(sidecarImage, defaultAdminURL, defaultAccessToken string) *PodExecutorMutator {
	if sidecarImage == "" {
		sidecarImage = "ghcr.io/xxl-job/xxl-job-operator-sidecar:latest"
	}
	defaultAccessToken = sanitizeWebhookToken(defaultAccessToken)
	if defaultAccessToken == "default_token" {
		defaultAccessToken = ""
	}
	return &PodExecutorMutator{
		SidecarImage:       sidecarImage,
		DefaultAdminURL:    defaultAdminURL,
		DefaultAccessToken: defaultAccessToken,
	}
}

func sanitizeWebhookToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.ContainsAny(trimmed, "\r\n") {
		return trimmed
	}
	return strings.NewReplacer("\r", "", "\n", "").Replace(trimmed)
}

// Default implements admission.CustomDefaulter for corev1.Pod.
func (m *PodExecutorMutator) Default(ctx context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("expected *corev1.Pod, got %T", obj)
	}
	_, err := m.MutatePodOrError(ctx, pod, pod.Namespace)
	return err
}

// Handle implements admission.Handler for webhook HTTP serving.
// Uses a baseline Pod round-trip before mutation so unknown fields in newer Kubernetes versions are never stripped (fixes R6-P2-18),
// and denies admission when sidecar/executor resolution fails instead of silently failing open (fixes R6-P1-12).
func (m *PodExecutorMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	baselinePod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, baselinePod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	baselineBytes, err := json.Marshal(baselinePod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	pod := baselinePod.DeepCopy()
	ns := pod.Namespace
	if ns == "" {
		ns = req.Namespace
	}
	mutated, err := m.MutatePodOrError(ctx, pod, ns)
	if err != nil {
		return admission.Denied(err.Error())
	}
	if !mutated {
		return admission.Allowed("no mutation required")
	}

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(baselineBytes, marshaledPod)
}

// MutatePod applies Resident or Sidecar mutations in-place on a Pod.
func (m *PodExecutorMutator) MutatePod(pod *corev1.Pod) bool {
	return m.MutatePodWithContext(context.Background(), pod, pod.Namespace)
}

// MutatePodWithContext applies Resident or Sidecar mutations in-place on a Pod, resolving XxlJobExecutor
// and XxlJobAdmin Secret references in the Pod's namespace when K8sClient is configured (fixes R5-P0-1).
func (m *PodExecutorMutator) MutatePodWithContext(ctx context.Context, pod *corev1.Pod, namespace string) bool {
	mutated, _ := m.MutatePodOrError(ctx, pod, namespace)
	return mutated
}

// MutatePodOrError applies Resident or Sidecar mutations in-place on a Pod and returns an explicit error
// when admission must be denied (e.g., unauthenticated sidecar injection, missing referenced executor, or ambiguous match) (fixes R6-P1-12, R6-P2-21).
func (m *PodExecutorMutator) MutatePodOrError(ctx context.Context, pod *corev1.Pod, namespace string) (bool, error) {
	if pod.Annotations == nil && pod.Labels == nil {
		return false, nil
	}
	// Never mutate batch/v1.Job Pods managed by K8sJobProxy so a batch task cannot borrow another
	// Resident/Sidecar executor's SecretKeyRef or sidecar container via xxljob.io/* metadata.
	if pod.Labels != nil &&
		pod.Labels["app.kubernetes.io/managed-by"] == "xxl-job-operator" &&
		pod.Labels["xxljob.io/job-id"] != "" {
		return false, nil
	}
	if pod.Annotations != nil && pod.Annotations[AnnotationInjectedStatus] == "true" {
		return false, nil
	}

	var mode string
	if pod.Annotations != nil {
		mode = strings.ToLower(strings.TrimSpace(pod.Annotations[AnnotationMode]))
	}
	if mode == "" && pod.Labels != nil {
		mode = strings.ToLower(strings.TrimSpace(pod.Labels[AnnotationMode]))
	}
	if mode != "resident" && mode != "sidecar" {
		return false, nil
	}
	if len(pod.Spec.Containers) == 0 {
		return false, nil
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	if _, err := selectTargetContainer(pod); err != nil {
		return false, err
	}

	appName := strings.TrimSpace(pod.Annotations[AnnotationAppName])
	adminURL := strings.TrimSpace(pod.Annotations[AnnotationAdminURL])
	accessToken := sanitizeWebhookToken(pod.Annotations[AnnotationAccessToken])
	if accessToken == "default_token" {
		accessToken = ""
	}

	var tokenSecretRef *corev1.SecretKeySelector
	if secName := strings.TrimSpace(pod.Annotations[AnnotationAccessTokenSecretName]); secName != "" {
		secKey := strings.TrimSpace(pod.Annotations[AnnotationAccessTokenSecretKey])
		if secKey == "" {
			secKey = "access-token"
		}
		tokenSecretRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secName},
			Key:                  secKey,
		}
	}

	execPort := parsePortOrDefault(pod.Annotations[AnnotationExecutorPort], 0)
	drainSecs := parsePortOrDefault(pod.Annotations[AnnotationGracefulDrainSeconds], 0)
	enableGlue := strings.EqualFold(strings.TrimSpace(pod.Annotations[AnnotationEnableGlue]), "true") ||
		strings.EqualFold(strings.TrimSpace(pod.Annotations[AnnotationSidecarEnableGlue]), "true")
	var crAddressType *int

	// Resolve from XxlJobExecutor & XxlJobAdmin CRs in the same namespace when K8sClient is available
	if m.K8sClient != nil && namespace != "" {
		matchedExec, err := m.findMatchingExecutor(ctx, pod, namespace, appName)
		if err != nil {
			return false, err
		}
		if matchedExec != nil {
			if appName == "" {
				appName = matchedExec.Spec.AppName
			}
			if execPort <= 0 && matchedExec.Spec.ExecutorPort > 0 {
				execPort = int(matchedExec.Spec.ExecutorPort)
			}
			if drainSecs <= 0 && matchedExec.Spec.GracefulDrainSeconds > 0 {
				drainSecs = int(matchedExec.Spec.GracefulDrainSeconds)
			}
			if matchedExec.Spec.SidecarEnableGlue {
				enableGlue = true
			}
			if matchedExec.Spec.AddressType != nil {
				crAddressType = matchedExec.Spec.AddressType
			}
			adminNS := matchedExec.Spec.AdminRef.Namespace
			if adminNS == "" {
				adminNS = matchedExec.Namespace
			}
			if adminNS != matchedExec.Namespace {
				return false, fmt.Errorf("cross-namespace adminRef (%s/%s) on XxlJobExecutor %s/%s is not allowed",
					adminNS, matchedExec.Spec.AdminRef.Name, matchedExec.Namespace, matchedExec.Name)
			}
			if matchedExec.Spec.AdminRef.Name != "" {
				var adminCR xxljobv1alpha1.XxlJobAdmin
				if err := m.K8sClient.Get(ctx, types.NamespacedName{Namespace: adminNS, Name: matchedExec.Spec.AdminRef.Name}, &adminCR); err != nil {
					return false, fmt.Errorf("failed to get XxlJobAdmin %s/%s: %w", adminNS, matchedExec.Spec.AdminRef.Name, err)
				}
				if adminURL == "" {
					adminURL = resolveAdminURLFromCR(&adminCR)
				}
				// Only resolve SecretKeyRef from XxlJobAdmin; never copy plaintext Admin spec tokens into Pod specs (fixes R6-P1-14).
				if tokenSecretRef == nil && accessToken == "" {
					if adminCR.Spec.Auth.AccessTokenSecretRef != nil && adminCR.Spec.Auth.AccessTokenSecretRef.Name != "" {
						secKey := strings.TrimSpace(adminCR.Spec.Auth.AccessTokenSecretRef.Key)
						if secKey == "" {
							secKey = "access-token"
						}
						tokenSecretRef = &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: adminCR.Spec.Auth.AccessTokenSecretRef.Name},
							Key:                  secKey,
						}
					}
				}
			}
		}
	}

	if appName == "" && pod.Labels != nil {
		appName = strings.TrimSpace(pod.Labels["app"])
	}
	if appName == "" {
		return false, fmt.Errorf("xxl-job pod mutation requires %s or a matching XxlJobExecutor", AnnotationAppName)
	}
	if err := validateWebhookAppName(appName); err != nil {
		return false, err
	}

	if adminURL == "" {
		adminURL = m.DefaultAdminURL
	}
	if adminURL == "" {
		ns := namespace
		if ns == "" {
			ns = "default"
		}
		adminURL = fmt.Sprintf("http://xxl-job-admin.%s.svc.cluster.local:8080/xxl-job-admin", ns)
	}
	if err := validateWebhookAdminURLs(adminURL); err != nil {
		return false, err
	}

	if tokenSecretRef == nil && accessToken == "" {
		accessToken = m.DefaultAccessToken
	}

	if execPort <= 0 {
		execPort = 9999
	}
	if drainSecs <= 0 {
		drainSecs = 15
	}
	targetPort := parsePortOrDefault(pod.Annotations[AnnotationTargetHTTPPort], 8080)

	switch mode {
	case "resident":
		m.mutateResidentPod(pod, appName, adminURL, accessToken, tokenSecretRef, execPort, drainSecs, crAddressType)
	case "sidecar":
		// Sidecar mode MUST have a valid SecretKeyRef or explicit non-default accessToken (fixes R5-P0-1 & R6-P1-12)
		if tokenSecretRef == nil && accessToken == "" {
			return false, fmt.Errorf("sidecar injection denied: a valid SecretKeyRef or explicit non-default accessToken is required")
		}
		m.mutateSidecarPod(pod, appName, adminURL, accessToken, tokenSecretRef, execPort, targetPort, drainSecs, enableGlue, crAddressType)
	}

	// Ensure TerminationGracePeriodSeconds accommodates 2x drain window + buffer (fixes R6-P1-11)
	minGrace := int64(2*drainSecs + 15)
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds < minGrace {
		pod.Spec.TerminationGracePeriodSeconds = &minGrace
	}

	pod.Annotations[AnnotationInjectedStatus] = "true"
	return true, nil
}

func (m *PodExecutorMutator) findMatchingExecutor(ctx context.Context, pod *corev1.Pod, namespace, appName string) (*xxljobv1alpha1.XxlJobExecutor, error) {
	// When AnnotationExecutorRef is explicitly set, only use Get and fail closed on error (fixes R6-P2-21).
	if execName := strings.TrimSpace(pod.Annotations[AnnotationExecutorRef]); execName != "" {
		var execCR xxljobv1alpha1.XxlJobExecutor
		if err := m.K8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: execName}, &execCR); err != nil {
			return nil, fmt.Errorf("failed to get referenced XxlJobExecutor %s/%s: %w", namespace, execName, err)
		}
		if !execCR.DeletionTimestamp.IsZero() {
			return nil, fmt.Errorf("referenced XxlJobExecutor %s/%s is terminating", namespace, execName)
		}
		if execCR.Spec.Mode == xxljobv1alpha1.ExecutorModeK8sJobProxy {
			return nil, fmt.Errorf("referenced XxlJobExecutor %s/%s has mode %q (not Resident/Sidecar)", namespace, execName, execCR.Spec.Mode)
		}
		return &execCR, nil
	}
	var list xxljobv1alpha1.XxlJobExecutorList
	if err := m.K8sClient.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list XxlJobExecutors in namespace %s: %w", namespace, err)
	}
	var matches []*xxljobv1alpha1.XxlJobExecutor
	for i := range list.Items {
		item := &list.Items[i]
		// Skip terminating or K8sJobProxy executors during implicit matching so replacing an executor
		// while the previous CR awaits finalizers never causes "ambiguous XxlJobExecutor match" errors (fixes EG2-14).
		if !item.DeletionTimestamp.IsZero() || item.Spec.Mode == xxljobv1alpha1.ExecutorModeK8sJobProxy {
			continue
		}
		matched := false
		if item.Spec.PodSelector != nil && pod.Labels != nil {
			if sel, err := metav1.LabelSelectorAsSelector(item.Spec.PodSelector); err == nil && sel.Matches(labels.Set(pod.Labels)) {
				matched = true
			}
		}
		if !matched && appName != "" && item.Spec.AppName == appName {
			matched = true
		}
		if matched {
			matches = append(matches, item)
		}
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("ambiguous XxlJobExecutor match for pod in namespace %s (%d executors matched); set %s explicitly", namespace, len(matches), AnnotationExecutorRef)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return nil, nil
}

// resolveAdminURLFromCR computes the Admin base URL purely from XxlJobAdmin.Spec without reading Status.AdminURL (fixes R6-P1-8).
func resolveAdminURLFromCR(admin *xxljobv1alpha1.XxlJobAdmin) string {
	if strings.TrimSpace(admin.Spec.ExternalURL) != "" {
		return strings.TrimRight(strings.TrimSpace(admin.Spec.ExternalURL), "/")
	}
	port := admin.Spec.Port
	if port <= 0 {
		port = 8080
	}
	ctxPath := strings.TrimSpace(admin.Spec.ContextPath)
	if ctxPath == "" {
		ctxPath = "/xxl-job-admin"
	}
	if !strings.HasPrefix(ctxPath, "/") {
		ctxPath = "/" + ctxPath
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", admin.Name, admin.Namespace, port, strings.TrimRight(ctxPath, "/"))
}

func validateWebhookAppName(appName string) error {
	if len(appName) == 0 || len(appName) > 64 {
		return fmt.Errorf("invalid %s %q: length must be between 1 and 64 characters", AnnotationAppName, appName)
	}
	first := appName[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || (first >= '0' && first <= '9')) {
		return fmt.Errorf("invalid %s %q: must start with an ASCII letter or digit", AnnotationAppName, appName)
	}
	for i := 0; i < len(appName); i++ {
		c := appName[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("invalid %s %q: must contain only [a-zA-Z0-9._-]", AnnotationAppName, appName)
		}
	}
	return nil
}

func validateWebhookAdminURLs(rawAdminURLs string) error {
	validCount := 0
	for _, part := range strings.Split(rawAdminURLs, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		for _, r := range trimmed {
			if unicode.IsSpace(r) || r < 0x20 || r == 0x7f || strings.ContainsRune(`'"`+"`$;&|<>()\\", r) {
				return fmt.Errorf("invalid %s entry %q: must not contain whitespace, quotes, control, or shell characters", AnnotationAdminURL, trimmed)
			}
		}
		u, err := url.Parse(trimmed)
		if err != nil {
			return fmt.Errorf("malformed %s entry %q: %w", AnnotationAdminURL, trimmed, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("invalid %s entry %q: scheme must be http or https", AnnotationAdminURL, trimmed)
		}
		if u.Host == "" {
			return fmt.Errorf("invalid %s entry %q: host must not be empty", AnnotationAdminURL, trimmed)
		}
		if u.User != nil {
			return fmt.Errorf("invalid %s entry %q: must not embed userinfo credentials", AnnotationAdminURL, trimmed)
		}
		if u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(trimmed, "?#") {
			return fmt.Errorf("invalid %s entry %q: must not contain query parameters or fragments", AnnotationAdminURL, trimmed)
		}
		if strings.Contains(u.Path, "..") {
			return fmt.Errorf("invalid %s entry %q: path must not contain '..'", AnnotationAdminURL, trimmed)
		}
		validCount++
	}
	if validCount == 0 {
		return fmt.Errorf("invalid %s %q: at least one http(s) URL is required", AnnotationAdminURL, rawAdminURLs)
	}
	return nil
}

// selectTargetContainer selects the target business container, honoring AnnotationContainerName or
// kubectl.kubernetes.io/default-container and skipping known infrastructure sidecars (fixes R6-P2-19).
func selectTargetContainer(pod *corev1.Pod) (*corev1.Container, error) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("pod has no containers")
	}
	preferred := ""
	if pod.Annotations != nil {
		preferred = strings.TrimSpace(pod.Annotations[AnnotationContainerName])
		if preferred == "" {
			preferred = strings.TrimSpace(pod.Annotations[AnnotationDefaultContainer])
		}
	}
	if preferred != "" {
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == preferred {
				return &pod.Spec.Containers[i], nil
			}
		}
		return nil, fmt.Errorf("target container %q specified in annotations not found in pod", preferred)
	}
	for i := range pod.Spec.Containers {
		if !knownInfraSidecarNames[pod.Spec.Containers[i].Name] {
			return &pod.Spec.Containers[i], nil
		}
	}
	return &pod.Spec.Containers[0], nil
}

func (m *PodExecutorMutator) mutateResidentPod(
	pod *corev1.Pod,
	appName, adminURL, accessToken string,
	tokenSecretRef *corev1.SecretKeySelector,
	execPort, drainSecs int,
	crAddressType *int,
) {
	c, err := selectTargetContainer(pod)
	if err != nil || c == nil {
		return
	}
	downwardEnvs := buildDownwardAPIEnvs(appName, strings.TrimRight(adminURL, "/"), accessToken, tokenSecretRef, execPort)
	c.Env = mergeEnvVars(c.Env, downwardEnvs)

	// Inject PreStop hook:
	// - When Operator manages addressType=1 (default), traffic is drained immediately from xxl_job_group.address_list
	//   as soon as DeletionTimestamp is set. Sleeping FIRST keeps xxl_job_registry alive during the drain window so
	//   >10m running tasks are not falsely marked as lost by JobCompleteHelper, and calls /api/registryRemove with bounded
	//   timeouts (-T 3 -t 1 / -m 3) right before container exit (fixes R5-P1-5 & R6-P1-13).
	// - When addressType == 0 (from XxlJobExecutor CR or xxljob.io/address-type="0"), /api/registryRemove runs before sleep.
	if c.Lifecycle == nil {
		c.Lifecycle = &corev1.Lifecycle{}
	}
	if c.Lifecycle.PreStop == nil {
		deregisterSnippet := `TOKEN="${XXL_JOB_ACCESS_TOKEN:-${XXL_JOB_ACCESSTOKEN}}"; ` +
			`TOKEN=$(printf '%s' "${TOKEN}" | tr -d '\r\n' 2>/dev/null || printf '%s' "${TOKEN}"); ` +
			`TOKEN="${TOKEN#"${TOKEN%%[![:space:]]*}"}"; TOKEN="${TOKEN%"${TOKEN##*[![:space:]]}"}"; ` +
			`SAFE_APPNAME=$(printf '%s' "${XXL_JOB_EXECUTOR_APPNAME}" | tr -cd 'a-zA-Z0-9._-' 2>/dev/null || printf '%s' "${XXL_JOB_EXECUTOR_APPNAME}"); ` +
			`HOST_IP="${POD_IP}"; case "${HOST_IP}" in *:*) HOST_IP="[${HOST_IP}]" ;; esac; ` +
			`PAYLOAD='{"registryGroup":"EXECUTOR","registryKey":"'"${SAFE_APPNAME}"'","registryValue":"http://'"${HOST_IP}"':'"${XXL_JOB_EXECUTOR_PORT}"'/"}'; ` +
			`for ADMIN_URL in $(printf '%s' "${XXL_JOB_ADMIN_ADDRESSES}" | tr ',' ' '); do ` +
			`ADMIN_URL=${ADMIN_URL%%/}; ` +
			`case "${ADMIN_URL}" in http://*|https://*) ` +
			`wget -qO- -T 3 -t 1 --header="Content-Type: application/json" --header="XXL-JOB-ACCESS-TOKEN: ${TOKEN}" ` +
			`--post-data="${PAYLOAD}" "${ADMIN_URL}/api/registryRemove" >/dev/null 2>&1 || ` +
			`curl -s -m 3 -X POST -H "Content-Type: application/json" -H "XXL-JOB-ACCESS-TOKEN: ${TOKEN}" ` +
			`-d "${PAYLOAD}" "${ADMIN_URL}/api/registryRemove" >/dev/null 2>&1 || true ;; esac; ` +
			`done`

		isAutoReg := strings.TrimSpace(pod.Annotations[AnnotationAddressType]) == "0"
		if crAddressType != nil {
			isAutoReg = (*crAddressType == 0)
		}
		var preStopCmd string
		if isAutoReg {
			preStopCmd = fmt.Sprintf("%s; sleep %d", deregisterSnippet, drainSecs)
		} else {
			preStopCmd = fmt.Sprintf("sleep %d; %s", drainSecs, deregisterSnippet)
		}
		c.Lifecycle.PreStop = &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", preStopCmd},
			},
		}
	}
}

func (m *PodExecutorMutator) mutateSidecarPod(
	pod *corev1.Pod,
	appName, adminURL, accessToken string,
	tokenSecretRef *corev1.SecretKeySelector,
	execPort, targetPort, drainSecs int,
	enableGlue bool,
	crAddressType *int,
) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == SidecarContainerName {
			return
		}
	}

	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[LabelSidecarInjected] = "true"

	// Add shared volume for scripts/logs between business container and sidecar, plus a dedicated
	// sidecar-only spool volume for callback persistence that is NEVER mounted into the business container (fixes R7-P1-1).
	hasSharedVol := false
	hasSpoolVol := false
	for _, v := range pod.Spec.Volumes {
		switch v.Name {
		case SharedVolumeName:
			hasSharedVol = true
		case SidecarSpoolVolumeName:
			hasSpoolVol = true
		}
	}
	if !hasSharedVol {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: SharedVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}
	if !hasSpoolVol {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: SidecarSpoolVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	// Mount shared volume and per-pod shared secret into primary business container
	targetContainer, err := selectTargetContainer(pod)
	if err != nil || targetContainer == nil {
		return
	}
	hasSharedMount := false
	for _, vm := range targetContainer.VolumeMounts {
		if vm.Name == SharedVolumeName || vm.MountPath == SharedMountPath {
			hasSharedMount = true
			break
		}
	}
	if !hasSharedMount {
		targetContainer.VolumeMounts = append(targetContainer.VolumeMounts, corev1.VolumeMount{
			Name:      SharedVolumeName,
			MountPath: SharedMountPath,
		})
	}

	// Preserve existing XXL_JOB_SIDECAR_SHARED_SECRET (both Value and ValueFrom) on targetContainer if already configured (fixes R7-P2-2 & R8-P2-3).
	sharedSecret := strings.TrimSpace(pod.Annotations[AnnotationSidecarSharedSecret])
	var sharedSecretValueFrom *corev1.EnvVarSource
	if sharedSecret == "" {
		for _, env := range targetContainer.Env {
			if env.Name == "XXL_JOB_SIDECAR_SHARED_SECRET" {
				if strings.TrimSpace(env.Value) != "" {
					sharedSecret = strings.TrimSpace(env.Value)
				} else if env.ValueFrom != nil {
					sharedSecretValueFrom = env.ValueFrom.DeepCopy()
				}
				break
			}
		}
	}
	var sharedSecretEnv corev1.EnvVar
	if sharedSecretValueFrom != nil {
		sharedSecretEnv = corev1.EnvVar{
			Name:      "XXL_JOB_SIDECAR_SHARED_SECRET",
			ValueFrom: sharedSecretValueFrom,
		}
	} else {
		if sharedSecret == "" {
			sharedSecret = generateSidecarSharedSecret()
		}
		sharedSecretEnv = corev1.EnvVar{
			Name:  "XXL_JOB_SIDECAR_SHARED_SECRET",
			Value: sharedSecret,
		}
	}
	targetContainer.Env = mergeEnvVars(targetContainer.Env, []corev1.EnvVar{sharedSecretEnv})

	// Ensure business container does not exit at t=0 on SIGTERM while Sidecar is draining active jobs (fixes R6-P1-11).
	if targetContainer.Lifecycle == nil {
		targetContainer.Lifecycle = &corev1.Lifecycle{}
	}
	if targetContainer.Lifecycle.PreStop == nil {
		targetContainer.Lifecycle.PreStop = &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", fmt.Sprintf("sleep %d", drainSecs)},
			},
		}
	}

	sidecarEnvs := buildDownwardAPIEnvs(appName, strings.TrimRight(adminURL, "/"), accessToken, tokenSecretRef, execPort)
	sidecarEnvs = append(sidecarEnvs,
		corev1.EnvVar{
			Name:  "XXL_JOB_TARGET_HTTP_PORT",
			Value: strconv.Itoa(targetPort),
		},
		corev1.EnvVar{
			Name:  "XXL_JOB_GRACEFUL_DRAIN_SECONDS",
			Value: strconv.Itoa(drainSecs),
		},
		*sharedSecretEnv.DeepCopy(),
		corev1.EnvVar{
			Name:  "XXL_JOB_SIDECAR_CALLBACK_DIR",
			Value: SidecarCallbackDir,
		},
	)
	if enableGlue {
		sidecarEnvs = append(sidecarEnvs, corev1.EnvVar{
			Name:  "XXL_JOB_SIDECAR_ENABLE_GLUE",
			Value: "true",
		})
	}
	isAutoReg := strings.TrimSpace(pod.Annotations[AnnotationAddressType]) == "0"
	if crAddressType != nil {
		isAutoReg = (*crAddressType == 0)
	}
	if isAutoReg {
		sidecarEnvs = append(sidecarEnvs, corev1.EnvVar{
			Name:  "XXL_JOB_DEREGISTER_BEFORE_DRAIN",
			Value: "true",
		})
	}

	// Sidecar /preStop authenticates with XXL-JOB-ACCESS-TOKEN and waits up to drainSecs for active jobs to finish (fixes R7-P2-3, R8-P2-3, R10-P2-2).
	preStopTimeout := drainSecs + 10
	preStopCmd := fmt.Sprintf(
		`TOKEN="${XXL_JOB_ACCESS_TOKEN:-${XXL_JOB_ACCESSTOKEN}}"; `+
			`TOKEN=$(printf '%%s' "${TOKEN}" | tr -d '\r\n' 2>/dev/null || printf '%%s' "${TOKEN}"); `+
			`TOKEN="${TOKEN#"${TOKEN%%%%[![:space:]]*}"}"; TOKEN="${TOKEN%%"${TOKEN##*[![:space:]]}"}"; `+
			`wget -qO- -T %d -t 1 --header="XXL-JOB-ACCESS-TOKEN: ${TOKEN}" --post-data='' http://127.0.0.1:%d/preStop >/dev/null 2>&1 || `+
			`curl -s -m %d -X POST -H "XXL-JOB-ACCESS-TOKEN: ${TOKEN}" http://127.0.0.1:%d/preStop >/dev/null 2>&1 || sleep %d`,
		preStopTimeout, execPort, preStopTimeout, execPort, drainSecs,
	)

	runAsNonRoot := true
	allowPrivilegeEscalation := false
	readOnlyRootFS := true

	sidecar := corev1.Container{
		Name:  SidecarContainerName,
		Image: m.SidecarImage,
		Ports: []corev1.ContainerPort{
			{Name: "xxl-executor", ContainerPort: int32(execPort), Protocol: corev1.ProtocolTCP},
		},
		Env: sidecarEnvs,
		VolumeMounts: []corev1.VolumeMount{
			{Name: SharedVolumeName, MountPath: SharedMountPath},
			{Name: SidecarSpoolVolumeName, MountPath: SidecarSpoolMountPath},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromInt(execPort),
				},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       5,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot:             &runAsNonRoot,
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFS,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
		Lifecycle: &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"/bin/sh", "-c", preStopCmd},
				},
			},
		},
	}

	pod.Spec.Containers = append(pod.Spec.Containers, sidecar)
}

func generateSidecarSharedSecret() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "xxljob-sidecar-fallback-secret"
	}
	return hex.EncodeToString(b[:])
}

func buildDownwardAPIEnvs(
	appName, adminURL, accessToken string,
	tokenSecretRef *corev1.SecretKeySelector,
	execPort int,
) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
			},
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
		{
			Name:  "XXL_JOB_ADMIN_ADDRESSES",
			Value: adminURL,
		},
	}

	// Prefer Kubernetes SecretKeyRef over plaintext token (fixes R5-P0-1).
	if tokenSecretRef != nil {
		envs = append(envs,
			corev1.EnvVar{
				Name: "XXL_JOB_ACCESSTOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: tokenSecretRef.DeepCopy(),
				},
			},
			corev1.EnvVar{
				Name: "XXL_JOB_ACCESS_TOKEN",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: tokenSecretRef.DeepCopy(),
				},
			},
		)
	} else if accessToken != "" && accessToken != "default_token" {
		envs = append(envs,
			corev1.EnvVar{
				Name:  "XXL_JOB_ACCESSTOKEN",
				Value: accessToken,
			},
			corev1.EnvVar{
				Name:  "XXL_JOB_ACCESS_TOKEN",
				Value: accessToken,
			},
		)
	}

	envs = append(envs,
		corev1.EnvVar{
			Name:  "XXL_JOB_EXECUTOR_APPNAME",
			Value: appName,
		},
		corev1.EnvVar{
			Name:  "XXL_JOB_EXECUTOR_IP",
			Value: "$(POD_IP)",
		},
		corev1.EnvVar{
			Name:  "XXL_JOB_EXECUTOR_PORT",
			Value: strconv.Itoa(execPort),
		},
	)
	return envs
}

// mergeEnvVars places Operator-injected prerequisite variables (such as POD_IP and XXL_JOB_ADMIN_ADDRESSES)
// first in dependency order—substituting any same-named user overrides in place—followed by remaining user
// variables so Kubernetes $(POD_IP) and $(XXL_JOB_ADMIN_ADDRESSES) references expand inside user env vars (fixes EG2-14).
func mergeEnvVars(existing, additions []corev1.EnvVar) []corev1.EnvVar {
	existingByName := make(map[string]corev1.EnvVar, len(existing))
	for _, e := range existing {
		existingByName[e.Name] = e
	}
	placed := make(map[string]bool, len(existing)+len(additions))
	out := make([]corev1.EnvVar, 0, len(existing)+len(additions))
	for _, add := range additions {
		if placed[add.Name] {
			continue
		}
		if override, hasOverride := existingByName[add.Name]; hasOverride {
			out = append(out, override)
		} else {
			out = append(out, add)
		}
		placed[add.Name] = true
	}
	for _, e := range existing {
		if !placed[e.Name] {
			out = append(out, e)
			placed[e.Name] = true
		}
	}
	return out
}

func parsePortOrDefault(val string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil || n <= 0 || n > 65535 {
		return def
	}
	return n
}
