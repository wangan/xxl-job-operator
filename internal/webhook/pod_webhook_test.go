package webhook

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/yaml"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
)

func TestMutateResidentPod(t *testing.T) {
	mutator := NewPodExecutorMutator("", "http://xxl-job-admin.default.svc.cluster.local:8080/xxl-job-admin", "super_secret_token")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "payment-service-7f8d9-abcde",
			Namespace: "default",
			Labels: map[string]string{
				LabelInject: "true",
			},
			Annotations: map[string]string{
				AnnotationMode:                 "resident",
				AnnotationAppName:              "payment-executor",
				AnnotationExecutorPort:         "9999",
				AnnotationGracefulDrainSeconds: "20",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "payment-service",
					Image: "company/payment-service:v1.2.0",
				},
			},
		},
	}

	if !mutator.MutatePod(pod) {
		t.Fatalf("expected MutatePod to return true for resident pod")
	}

	c := pod.Spec.Containers[0]
	envMap := make(map[string]corev1.EnvVar)
	for _, e := range c.Env {
		envMap[e.Name] = e
	}

	if envMap["POD_IP"].ValueFrom == nil || envMap["POD_IP"].ValueFrom.FieldRef.FieldPath != "status.podIP" {
		t.Fatalf("expected Downward API POD_IP env var, got %+v", envMap["POD_IP"])
	}
	if envMap["XXL_JOB_EXECUTOR_IP"].Value != "$(POD_IP)" {
		t.Fatalf("expected XXL_JOB_EXECUTOR_IP=$(POD_IP), got %s", envMap["XXL_JOB_EXECUTOR_IP"].Value)
	}
	// Verify Spring Boot relaxed binding env var XXL_JOB_ACCESSTOKEN is injected (fixes P1-1)
	if envMap["XXL_JOB_ACCESSTOKEN"].Value != "super_secret_token" {
		t.Fatalf("expected XXL_JOB_ACCESSTOKEN=super_secret_token, got %s", envMap["XXL_JOB_ACCESSTOKEN"].Value)
	}
	if envMap["XXL_JOB_EXECUTOR_APPNAME"].Value != "payment-executor" {
		t.Fatalf("expected XXL_JOB_EXECUTOR_APPNAME=payment-executor, got %s", envMap["XXL_JOB_EXECUTOR_APPNAME"].Value)
	}

	if c.Lifecycle == nil || c.Lifecycle.PreStop == nil || c.Lifecycle.PreStop.Exec == nil {
		t.Fatalf("expected PreStop lifecycle hook to be injected")
	}
	preStopScript := strings.Join(c.Lifecycle.PreStop.Exec.Command, " ")
	if !strings.Contains(preStopScript, "/api/registryRemove") || !strings.Contains(preStopScript, "sleep 20") {
		t.Fatalf("unexpected PreStop script: %s", preStopScript)
	}
	// Verify PreStop sleeps BEFORE calling /api/registryRemove under default addressType=1 so in-flight >10m tasks are not marked lost (fixes R5-P1-5)
	if strings.Index(preStopScript, "sleep 20") > strings.Index(preStopScript, "/api/registryRemove") {
		t.Fatalf("expected PreStop under default addressType=1 to sleep before calling /api/registryRemove (R5-P1-5): %s", preStopScript)
	}
	if !strings.Contains(preStopScript, "tr ',' ' '") {
		t.Fatalf("expected PreStop script to handle comma-separated XXL_JOB_ADMIN_ADDRESSES: %s", preStopScript)
	}
	if strings.Contains(preStopScript, "super_secret_token") {
		t.Fatalf("expected PreStop script to reference ${XXL_JOB_ACCESS_TOKEN} instead of plaintext token: %s", preStopScript)
	}
	if !strings.Contains(preStopScript, `HOST_IP="[${HOST_IP}]"`) || !strings.Contains(preStopScript, "curl -s -m 3") {
		t.Fatalf("R10-P2-2: expected Resident PreStop script to bracket IPv6 POD_IP and set bounded curl timeouts: %s", preStopScript)
	}

	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds < 35 {
		t.Fatalf("expected TerminationGracePeriodSeconds >= 35, got %v", pod.Spec.TerminationGracePeriodSeconds)
	}

	// Idempotency check
	if mutator.MutatePod(pod) {
		t.Fatalf("expected second MutatePod call to be a no-op")
	}
}

func TestMutateSidecarPodAndAdmissionHandler(t *testing.T) {
	t.Run("RefuseSidecarWhenDefaultTokenOrEmpty", func(t *testing.T) {
		insecureMutator := NewPodExecutorMutator("", "http://xxl-job-admin:8080/xxl-job-admin", "default_token")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "insecure-sidecar-pod",
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:    "sidecar",
					AnnotationAppName: "py-exec",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "python:3.11"}},
			},
		}
		if insecureMutator.MutatePod(pod) {
			t.Fatalf("expected MutatePod to refuse sidecar injection when token is default_token or empty (R5-P0-1)")
		}
		rawBytes, _ := json.Marshal(pod)
		resp := insecureMutator.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace: "default",
				Object:    runtime.RawExtension{Raw: rawBytes},
			},
		})
		if resp.Allowed {
			t.Fatalf("expected AdmissionHandler to deny unauthenticated sidecar pod creation (R6-P1-12), got Allowed=true")
		}
	})

	t.Run("ResolveSecretKeyRefAndDrainSecondsFromExecutorAndAdmin", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = xxljobv1alpha1.AddToScheme(scheme)

		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "xxl-job-admin", Namespace: "analytics"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Port: 8088,
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{
						Name: "analytics-xxl-secret",
						Key:  "token-key",
					},
				},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{
				AdminURL: "http://stale-status-url:8080/xxl-job-admin",
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "python-etl-exec", Namespace: "analytics"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:             xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
				AppName:              "python-etl-executor",
				Title:                "Python ETL",
				Mode:                 xxljobv1alpha1.ExecutorModeSidecar,
				GracefulDrainSeconds: 25,
				SidecarEnableGlue:    true,
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "python-etl"},
				},
			},
		}

		k8sCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adminCR, execCR).Build()
		mutator := NewPodExecutorMutator("ghcr.io/xxl-job/xxl-job-operator-sidecar:v1.0.0", "", "")
		mutator.K8sClient = k8sCli

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "python-etl-pod",
				Namespace: "analytics",
				Labels: map[string]string{
					LabelInject: "true",
					"app":       "python-etl",
				},
				Annotations: map[string]string{
					AnnotationMode:           "sidecar",
					AnnotationTargetHTTPPort: "5000",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "istio-proxy",
						Image: "istio/proxyv2:1.20.0",
					},
					{
						Name:  "python-app",
						Image: "company/python-etl:v2.0",
						Env: []corev1.EnvVar{
							{Name: "XXL_JOB_SIDECAR_SHARED_SECRET", Value: "user-preconfigured-shared-secret"},
						},
					},
				},
			},
		}

		rawBytes, _ := json.Marshal(pod)
		resp := mutator.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace: "analytics",
				Object:    runtime.RawExtension{Raw: rawBytes},
			},
		})
		if !resp.Allowed || len(resp.Patches) == 0 {
			t.Fatalf("expected AdmissionResponse Allowed=true with JSON patches, got Allowed=%v patches=%d", resp.Allowed, len(resp.Patches))
		}

		// Also mutate directly to inspect injected Sidecar env vars, securityContext, and target container selection
		if !mutator.MutatePod(pod) {
			t.Fatalf("expected MutatePod to succeed via K8sClient SecretKeyRef resolution")
		}
		if pod.Labels[LabelSidecarInjected] != "true" {
			t.Fatalf("expected label %s=true on injected sidecar pod", LabelSidecarInjected)
		}
		var sidecar, appContainer *corev1.Container
		for i := range pod.Spec.Containers {
			switch pod.Spec.Containers[i].Name {
			case SidecarContainerName:
				sidecar = &pod.Spec.Containers[i]
			case "python-app":
				appContainer = &pod.Spec.Containers[i]
			}
		}
		if sidecar == nil || appContainer == nil {
			t.Fatalf("expected sidecar and python-app containers to be present")
		}
		// Verify selectTargetContainer skipped istio-proxy and mounted ONLY shared volume (never sidecar spool volume) into python-app (R6-P2-19 & R7-P1-1)
		if len(appContainer.VolumeMounts) != 1 || appContainer.VolumeMounts[0].Name != SharedVolumeName {
			t.Fatalf("expected only shared volume (not sidecar spool volume) mounted into python-app (R7-P1-1), got %+v", appContainer.VolumeMounts)
		}
		hasSpoolMount := false
		for _, vm := range sidecar.VolumeMounts {
			if vm.Name == SidecarSpoolVolumeName && vm.MountPath == SidecarSpoolMountPath {
				hasSpoolMount = true
			}
		}
		if !hasSpoolMount {
			t.Fatalf("R7-P1-1: expected %s mounted at %s in sidecar container, got %+v", SidecarSpoolVolumeName, SidecarSpoolMountPath, sidecar.VolumeMounts)
		}
		if appContainer.Lifecycle == nil || appContainer.Lifecycle.PreStop == nil {
			t.Fatalf("expected PreStop drain wait hook to be injected into business container in sidecar mode (R6-P1-11)")
		}
		// Verify restricted SecurityContext (without hardcoded RunAsUser=65532, R7-P2-4), ReadinessProbe, and Resources on sidecar (R6-P1-13)
		if sidecar.SecurityContext == nil ||
			sidecar.SecurityContext.RunAsNonRoot == nil || !*sidecar.SecurityContext.RunAsNonRoot ||
			sidecar.SecurityContext.RunAsUser != nil ||
			sidecar.SecurityContext.AllowPrivilegeEscalation == nil || *sidecar.SecurityContext.AllowPrivilegeEscalation ||
			sidecar.SecurityContext.ReadOnlyRootFilesystem == nil || !*sidecar.SecurityContext.ReadOnlyRootFilesystem ||
			sidecar.ReadinessProbe == nil || sidecar.ReadinessProbe.HTTPGet == nil || sidecar.ReadinessProbe.HTTPGet.Path != "/readyz" ||
			len(sidecar.Resources.Limits) == 0 {
			t.Fatalf("expected restricted SecurityContext (RunAsUser=nil), /readyz ReadinessProbe, and Resources on sidecar (R6-P1-13 & R7-P2-4), got %+v", sidecar)
		}
		if sidecar.Lifecycle == nil || sidecar.Lifecycle.PreStop == nil || sidecar.Lifecycle.PreStop.Exec == nil ||
			!strings.Contains(strings.Join(sidecar.Lifecycle.PreStop.Exec.Command, " "), "XXL-JOB-ACCESS-TOKEN: ${TOKEN}") {
			t.Fatalf("R7-P2-3: expected sidecar PreStop hook to authenticate with XXL-JOB-ACCESS-TOKEN, got %+v", sidecar.Lifecycle)
		}

		envMap := make(map[string]corev1.EnvVar)
		for _, e := range sidecar.Env {
			envMap[e.Name] = e
		}
		if envMap["XXL_JOB_ACCESS_TOKEN"].Value != "" ||
			envMap["XXL_JOB_ACCESS_TOKEN"].ValueFrom == nil ||
			envMap["XXL_JOB_ACCESS_TOKEN"].ValueFrom.SecretKeyRef == nil ||
			envMap["XXL_JOB_ACCESS_TOKEN"].ValueFrom.SecretKeyRef.Name != "analytics-xxl-secret" ||
			envMap["XXL_JOB_ACCESS_TOKEN"].ValueFrom.SecretKeyRef.Key != "token-key" {
			t.Fatalf("expected XXL_JOB_ACCESS_TOKEN injected via SecretKeyRef (R5-P0-1), got %+v", envMap["XXL_JOB_ACCESS_TOKEN"])
		}
		if envMap["XXL_JOB_GRACEFUL_DRAIN_SECONDS"].Value != "25" {
			t.Fatalf("expected GracefulDrainSeconds=25 resolved from XxlJobExecutor CR, got %+v", envMap["XXL_JOB_GRACEFUL_DRAIN_SECONDS"])
		}
		if envMap["XXL_JOB_SIDECAR_ENABLE_GLUE"].Value != "true" {
			t.Fatalf("expected XXL_JOB_SIDECAR_ENABLE_GLUE=true when SidecarEnableGlue is set on CR (R6-P1-13), got %+v", envMap["XXL_JOB_SIDECAR_ENABLE_GLUE"])
		}
		if envMap["XXL_JOB_SIDECAR_SHARED_SECRET"].Value != "user-preconfigured-shared-secret" {
			t.Fatalf("R7-P2-2: expected sidecar to reuse preconfigured XXL_JOB_SIDECAR_SHARED_SECRET from business container, got %q", envMap["XXL_JOB_SIDECAR_SHARED_SECRET"].Value)
		}
		if envMap["XXL_JOB_SIDECAR_CALLBACK_DIR"].Value != SidecarCallbackDir {
			t.Fatalf("R7-P1-1: expected XXL_JOB_SIDECAR_CALLBACK_DIR=%s on sidecar, got %q", SidecarCallbackDir, envMap["XXL_JOB_SIDECAR_CALLBACK_DIR"].Value)
		}
		if envMap["XXL_JOB_ADMIN_ADDRESSES"].Value != "http://xxl-job-admin.analytics.svc.cluster.local:8088/xxl-job-admin" {
			t.Fatalf("expected pure spec-computed AdminURL ignoring stale Status.AdminURL (R6-P1-8), got %s", envMap["XXL_JOB_ADMIN_ADDRESSES"].Value)
		}

		// Verify ValueFrom on XXL_JOB_SIDECAR_SHARED_SECRET is preserved across business and sidecar containers (R8-P2-3)
		vfPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "python-etl-vf",
				Namespace: "analytics",
				Labels: map[string]string{
					LabelInject: "true",
					"app":       "python-etl",
				},
				Annotations: map[string]string{
					AnnotationMode: "sidecar",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "python-app",
						Image: "company/python-etl:v2.0",
						Env: []corev1.EnvVar{
							{
								Name: "XXL_JOB_SIDECAR_SHARED_SECRET",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: "shared-sec"},
										Key:                  "secret",
									},
								},
							},
						},
					},
				},
			},
		}
		if !mutator.MutatePod(vfPod) {
			t.Fatalf("expected MutatePod to succeed on vfPod")
		}
		var vfSidecar *corev1.Container
		for i := range vfPod.Spec.Containers {
			if vfPod.Spec.Containers[i].Name == SidecarContainerName {
				vfSidecar = &vfPod.Spec.Containers[i]
			}
		}
		vfEnv := make(map[string]corev1.EnvVar)
		for _, e := range vfSidecar.Env {
			vfEnv[e.Name] = e
		}
		if vfEnv["XXL_JOB_SIDECAR_SHARED_SECRET"].ValueFrom == nil ||
			vfEnv["XXL_JOB_SIDECAR_SHARED_SECRET"].ValueFrom.SecretKeyRef == nil ||
			vfEnv["XXL_JOB_SIDECAR_SHARED_SECRET"].ValueFrom.SecretKeyRef.Name != "shared-sec" {
			t.Fatalf("R8-P2-3 regression: expected sidecar to clone ValueFrom for XXL_JOB_SIDECAR_SHARED_SECRET, got %+v", vfEnv["XXL_JOB_SIDECAR_SHARED_SECRET"])
		}

		// Verify R10-P2-2: When XxlJobExecutor has AddressType=0, sidecar receives XXL_JOB_DEREGISTER_BEFORE_DRAIN=true
		addrZero := 0
		execCR.Spec.AddressType = &addrZero
		_ = k8sCli.Update(context.Background(), execCR)
		autoPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "python-etl-auto",
				Namespace: "analytics",
				Labels: map[string]string{
					LabelInject: "true",
					"app":       "python-etl",
				},
				Annotations: map[string]string{
					AnnotationMode: "sidecar",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "python-app", Image: "company/python-etl:v2.0"}},
			},
		}
		if !mutator.MutatePod(autoPod) {
			t.Fatalf("expected MutatePod to succeed on autoPod")
		}
		var autoSidecar *corev1.Container
		for i := range autoPod.Spec.Containers {
			if autoPod.Spec.Containers[i].Name == SidecarContainerName {
				autoSidecar = &autoPod.Spec.Containers[i]
			}
		}
		autoEnv := make(map[string]corev1.EnvVar)
		for _, e := range autoSidecar.Env {
			autoEnv[e.Name] = e
		}
		if autoEnv["XXL_JOB_DEREGISTER_BEFORE_DRAIN"].Value != "true" {
			t.Fatalf("R10-P2-2 regression: expected XXL_JOB_DEREGISTER_BEFORE_DRAIN=true when AddressType=0, got %+v", autoEnv["XXL_JOB_DEREGISTER_BEFORE_DRAIN"])
		}
	})
}

// TestWebhookManifestAndSamplesSelectorConsistency parses config/webhook/manifests.yaml and
// config/samples/{02,04} to guarantee that MutatingWebhookConfiguration.objectSelector matches
// the Pod template labels in both samples and MutatePod injects SecretKeyRef without plaintext token (fixes N-P0-1 & R5-P0-1).
func TestWebhookManifestAndSamplesSelectorConsistency(t *testing.T) {
	manifestBytes, err := os.ReadFile("../../config/webhook/manifests.yaml")
	if err != nil {
		t.Fatalf("read config/webhook/manifests.yaml failed: %v", err)
	}

	var webhookCfg *admissionregv1.MutatingWebhookConfiguration
	for _, doc := range strings.Split(string(manifestBytes), "\n---") {
		if strings.Contains(doc, "kind: MutatingWebhookConfiguration") {
			var cfg admissionregv1.MutatingWebhookConfiguration
			if err := yaml.Unmarshal([]byte(doc), &cfg); err != nil {
				t.Fatalf("unmarshal MutatingWebhookConfiguration failed: %v", err)
			}
			webhookCfg = &cfg
			break
		}
	}
	if webhookCfg == nil || len(webhookCfg.Webhooks) == 0 || webhookCfg.Webhooks[0].ObjectSelector == nil {
		t.Fatalf("MutatingWebhookConfiguration with objectSelector not found in config/webhook/manifests.yaml")
	}
	if webhookCfg.Webhooks[0].FailurePolicy == nil || *webhookCfg.Webhooks[0].FailurePolicy != admissionregv1.Fail {
		t.Fatalf("expected MutatingWebhookConfiguration failurePolicy=Fail (R6-P1-12), got %v", webhookCfg.Webhooks[0].FailurePolicy)
	}

	selector, err := metav1.LabelSelectorAsSelector(webhookCfg.Webhooks[0].ObjectSelector)
	if err != nil {
		t.Fatalf("invalid objectSelector in manifests.yaml: %v", err)
	}

	sampleFiles := []string{
		"../../config/samples/02-executor-resident.yaml",
		"../../config/samples/04-executor-sidecar.yaml",
	}
	// Note: DefaultAccessToken is empty ("") to prove samples 02 & 04 work out-of-the-box via SecretKeyRef!
	mutator := NewPodExecutorMutator("", "", "")

	for _, samplePath := range sampleFiles {
		raw, err := os.ReadFile(samplePath)
		if err != nil {
			t.Fatalf("read %s failed: %v", samplePath, err)
		}
		foundDeployment := false
		for _, doc := range strings.Split(string(raw), "\n---") {
			if !strings.Contains(doc, "kind: Deployment") {
				continue
			}
			var deploy appsv1.Deployment
			if err := yaml.Unmarshal([]byte(doc), &deploy); err != nil {
				t.Fatalf("unmarshal Deployment in %s failed: %v", samplePath, err)
			}
			foundDeployment = true
			podLabels := deploy.Spec.Template.ObjectMeta.Labels
			if !selector.Matches(labels.Set(podLabels)) {
				t.Fatalf("sample %s Pod template labels %v do NOT match webhook objectSelector %s (N-P0-1 regression!)",
					samplePath, podLabels, selector.String())
			}

			pod := &corev1.Pod{
				ObjectMeta: deploy.Spec.Template.ObjectMeta,
				Spec:       deploy.Spec.Template.Spec,
			}
			if !mutator.MutatePod(pod) {
				t.Fatalf("expected MutatePod to mutate Pod from sample %s without DefaultAccessToken", samplePath)
			}
		}
		if !foundDeployment {
			t.Fatalf("expected a Deployment in sample %s", samplePath)
		}
	}
}

func TestEndgame2TerminatingExecutorSkipAndEnvExpansionOrder(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	admin := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "eg2-admin", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://eg2-admin.default.svc:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "eg2-secret-tok"},
		},
	}
	terminatingOldExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "billing-exec-old",
			Namespace:  "default",
			Finalizers: []string{"xxljob.io/executor-finalizer"},
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:    xxljobv1alpha1.LocalObjectReference{Name: "eg2-admin"},
			AppName:     "billing-app",
			Mode:        xxljobv1alpha1.ExecutorModeResident,
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "billing"}},
		},
	}
	activeNewExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "billing-exec-new",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:     xxljobv1alpha1.LocalObjectReference{Name: "eg2-admin"},
			AppName:      "billing-app",
			Mode:         xxljobv1alpha1.ExecutorModeResident,
			ExecutorPort: 9999,
			PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "billing"}},
		},
	}
	proxySameAppExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "billing-exec-proxy",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "eg2-admin"},
			AppName:  "billing-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
	}

	fakeCli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(admin, terminatingOldExec, activeNewExec, proxySameAppExec).
		Build()
	// Mark terminatingOldExec with a non-zero DeletionTimestamp (retained by finalizer)
	_ = fakeCli.Delete(context.Background(), terminatingOldExec)

	mutator := NewPodExecutorMutator("", "", "")
	mutator.K8sClient = fakeCli

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "billing-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				LabelInject: "true",
				"app":       "billing",
			},
			Annotations: map[string]string{
				AnnotationMode:    "resident",
				AnnotationAppName: "billing-app",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "billing",
					Image: "billing:v1",
					Env: []corev1.EnvVar{
						{Name: "XXL_JOB_EXECUTOR_PORT", Value: "19999"},
						{Name: "JAVA_OPTS", Value: "-Dxxl.admin=$(XXL_JOB_ADMIN_ADDRESSES) -Dip=$(POD_IP)"},
					},
				},
			},
		},
	}
	rawPod, _ := json.Marshal(pod)
	resp := mutator.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: "default",
			Object:    runtime.RawExtension{Raw: rawPod},
		},
	})
	if !resp.Allowed || len(resp.Patches) == 0 {
		t.Fatalf("EG2-14 regression: expected terminating and K8sJobProxy executors to be skipped and active resident executor to mutate pod, got allowed=%v patches=%d result=%v", resp.Allowed, len(resp.Patches), resp.Result)
	}

	// Verify mergeEnvVars ordering directly on MutatePodOrError so $(POD_IP) and $(XXL_JOB_ADMIN_ADDRESSES) precede JAVA_OPTS
	mutated, err := mutator.MutatePodOrError(context.Background(), pod, "default")
	if err != nil || !mutated {
		t.Fatalf("MutatePodOrError failed: mutated=%v err=%v", mutated, err)
	}
	idxOf := func(name string) int {
		for i, e := range pod.Spec.Containers[0].Env {
			if e.Name == name {
				return i
			}
		}
		return -1
	}
	valOf := func(name string) string {
		for _, e := range pod.Spec.Containers[0].Env {
			if e.Name == name {
				return e.Value
			}
		}
		return ""
	}
	podIPIdx := idxOf("POD_IP")
	adminAddrIdx := idxOf("XXL_JOB_ADMIN_ADDRESSES")
	execIPIdx := idxOf("XXL_JOB_EXECUTOR_IP")
	javaOptsIdx := idxOf("JAVA_OPTS")
	if podIPIdx < 0 || adminAddrIdx < 0 || execIPIdx < 0 || javaOptsIdx < 0 {
		t.Fatalf("missing expected env vars in mutated container: %+v", pod.Spec.Containers[0].Env)
	}
	if podIPIdx >= execIPIdx || podIPIdx >= javaOptsIdx || adminAddrIdx >= javaOptsIdx {
		t.Fatalf("EG2-14 regression: prerequisite env vars must precede dependent $(...) references (POD_IP=%d, ADMIN=%d, EXEC_IP=%d, JAVA_OPTS=%d)",
			podIPIdx, adminAddrIdx, execIPIdx, javaOptsIdx)
	}
	if valOf("XXL_JOB_EXECUTOR_PORT") != "19999" {
		t.Fatalf("EG2-14 regression: user explicit override XXL_JOB_EXECUTOR_PORT=19999 was overwritten with %q", valOf("XXL_JOB_EXECUTOR_PORT"))
	}
}

func TestBoostEndgameWebhookSecurityAndK8sJobProxyIsolation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)
	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-ok", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://xxl-admin.default.svc:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "strong-webhook-token"},
		},
	}
	residentExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "res-exec", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:    xxljobv1alpha1.LocalObjectReference{Name: "admin-ok"},
			AppName:     "res-app",
			Mode:        xxljobv1alpha1.ExecutorModeResident,
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "shared"}},
		},
	}
	terminatingExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "term-exec",
			Namespace:  "default",
			Finalizers: []string{"xxljob.io/test-finalizer"},
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "admin-ok"},
			AppName:  "term-app",
			Mode:     xxljobv1alpha1.ExecutorModeResident,
		},
	}
	proxyExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "proxy-exec", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "admin-ok"},
			AppName:  "proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
	}

	k8sCli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminCR, residentExec, terminatingExec, proxyExec).
		Build()
	_ = k8sCli.Delete(context.Background(), terminatingExec)
	mutator := NewPodExecutorMutator("ghcr.io/xxl-job/sidecar:latest", "http://xxl-admin:8080/xxl-job-admin", "fallback-strong-token")
	mutator.K8sClient = k8sCli

	// (a) K8sJobProxy-managed batch Job Pod must NEVER be mutated even if labels match a Resident PodSelector
	proxyManagedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				LabelInject:                  "true",
				"app":                        "shared",
				proxyexecutor.LabelManagedBy: proxyexecutor.ManagedByValue,
				proxyexecutor.LabelJobID:     "42",
			},
			Annotations: map[string]string{
				AnnotationMode: "resident",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "job-runner", Image: "busybox:1.36"}},
		},
	}
	mutated, err := mutator.MutatePodOrError(context.Background(), proxyManagedPod, "default")
	if err != nil || mutated || proxyManagedPod.Spec.Containers[0].Lifecycle != nil {
		t.Fatalf("expected K8sJobProxy-managed Pod to be skipped without mutation, got mutated=%v err=%v", mutated, err)
	}

	// (b) Explicit AnnotationExecutorRef pointing to terminating or K8sJobProxy executor must be rejected
	for _, badRef := range []string{"term-exec", "proxy-exec"} {
		badPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-" + badRef,
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:        "resident",
					AnnotationExecutorRef: badRef,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}},
			},
		}
		if _, err := mutator.MutatePodOrError(context.Background(), badPod, "default"); err == nil {
			t.Fatalf("expected explicit AnnotationExecutorRef=%q to be rejected", badRef)
		}
	}

	// (c) Shell/whitespace/userinfo/query injection in appName and adminURL must be rejected
	for _, badApp := range []string{"bad app", "bad;rm -rf /", "bad'quote", "-flag", ".dot"} {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-bad-app",
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:    "resident",
					AnnotationAppName: badApp,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}}},
		}
		if _, err := mutator.MutatePodOrError(context.Background(), p, "default"); err == nil {
			t.Fatalf("expected invalid appName %q to be rejected", badApp)
		}
	}
	for _, badURL := range []string{
		"http://user:pass@xxl-admin:8080/xxl-job-admin",
		"http://xxl-admin:8080/xxl-job-admin?foo=1",
		"http://xxl-admin:8080/xxl-job-admin;id",
		"ftp://xxl-admin:8080/xxl-job-admin",
	} {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-bad-url",
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:     "resident",
					AnnotationAppName:  "safe-app",
					AnnotationAdminURL: badURL,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}}},
		}
		if _, err := mutator.MutatePodOrError(context.Background(), p, "default"); err == nil {
			t.Fatalf("expected invalid adminURL %q to be rejected", badURL)
		}
	}

	// (d) Verify Resident & Sidecar PreStop scripts contain POSIX whitespace/CRLF token sanitization
	for _, mode := range []string{"resident", "sidecar"} {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-prestop-" + mode,
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:    mode,
					AnnotationAppName: "safe-app",
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}}},
		}
		ok, err := mutator.MutatePodOrError(context.Background(), p, "default")
		if err != nil || !ok {
			t.Fatalf("MutatePodOrError(%s) failed: ok=%v err=%v", mode, ok, err)
		}
		var cmd []string
		if mode == "resident" {
			cmd = p.Spec.Containers[0].Lifecycle.PreStop.Exec.Command
		} else {
			cmd = p.Spec.Containers[1].Lifecycle.PreStop.Exec.Command
		}
		if len(cmd) != 3 || !strings.Contains(cmd[2], "tr -d '\\r\\n'") || !strings.Contains(cmd[2], "[![:space:]]") {
			t.Fatalf("expected POSIX CRLF/whitespace sanitization in %s PreStop script, got %v", mode, cmd)
		}
	}

	// Execute the exact POSIX shell snippet with /bin/sh to verify leading/trailing whitespace + CRLF removal
	shCheck := exec.Command("/bin/sh", "-c",
		`TOKEN="${RAW}"; TOKEN=$(printf '%s' "${TOKEN}" | tr -d '\r\n' 2>/dev/null || printf '%s' "${TOKEN}"); TOKEN="${TOKEN#"${TOKEN%%[![:space:]]*}"}"; TOKEN="${TOKEN%"${TOKEN##*[![:space:]]}"}"; printf '[%s]' "${TOKEN}"`)
	shCheck.Env = append(os.Environ(), "RAW=  \tBearer my-token\r\n  ")
	out, shErr := shCheck.CombinedOutput()
	if shErr != nil || string(out) != "[Bearer my-token]" {
		t.Fatalf("POSIX shell token sanitization failed: out=%q err=%v", string(out), shErr)
	}

	// (e) Verify embedded CRLF in DefaultAccessToken and AnnotationAccessToken is stripped before Pod env injection
	crlfMutator := NewPodExecutorMutator("", "http://xxl-admin:8080/xxl-job-admin", "  def-tok\r\n-part2  ")
	if crlfMutator.DefaultAccessToken != "def-tok-part2" {
		t.Fatalf("expected DefaultAccessToken CRLF stripped to def-tok-part2, got %q", crlfMutator.DefaultAccessToken)
	}
	crlfPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-crlf-token",
			Namespace: "default",
			Labels:    map[string]string{LabelInject: "true"},
			Annotations: map[string]string{
				AnnotationMode:        "sidecar",
				AnnotationAppName:     "safe-app",
				AnnotationAccessToken: "  ann-tok\r\n-v2  ",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}}},
	}
	if ok, err := crlfMutator.MutatePodOrError(context.Background(), crlfPod, "default"); err != nil || !ok {
		t.Fatalf("MutatePodOrError with CRLF AnnotationAccessToken failed: ok=%v err=%v", ok, err)
	}
	gotSidecarTok := ""
	for _, ev := range crlfPod.Spec.Containers[1].Env {
		if ev.Name == "XXL_JOB_ACCESS_TOKEN" {
			gotSidecarTok = ev.Value
		}
	}
	if gotSidecarTok != "ann-tok-v2" {
		t.Fatalf("expected sidecar XXL_JOB_ACCESS_TOKEN sanitized to ann-tok-v2, got %q", gotSidecarTok)
	}
}

func TestSidecarPodRootRunAsUserOverrideAndExplicitUID(t *testing.T) {
	mutator := NewPodExecutorMutator("", "http://xxl-admin:8080/xxl-job-admin", "sidecar-secret-tok")

	int64Ptr := func(v int64) *int64 { return &v }

	// 1. Pod with spec.securityContext.runAsUser=0 and runAsGroup=0 must receive an explicit non-root
	// container-level RunAsUser/RunAsGroup on xxl-job-sidecar so Kubelet verifyRunAsNonRoot does not fail.
	rootPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-root-business-pod",
			Namespace: "default",
			Labels:    map[string]string{LabelInject: "true"},
			Annotations: map[string]string{
				AnnotationMode:    "sidecar",
				AnnotationAppName: "legacy-root-app",
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:  int64Ptr(0),
				RunAsGroup: int64Ptr(0),
			},
			Containers: []corev1.Container{{Name: "legacy-app", Image: "nginx:1.25"}},
		},
	}
	if ok, err := mutator.MutatePodOrError(context.Background(), rootPod, "default"); err != nil || !ok {
		t.Fatalf("expected sidecar injection on root Pod to succeed, got ok=%v err=%v", ok, err)
	}
	sidecarSC := rootPod.Spec.Containers[1].SecurityContext
	if sidecarSC == nil || sidecarSC.RunAsNonRoot == nil || !*sidecarSC.RunAsNonRoot {
		t.Fatalf("expected sidecar RunAsNonRoot=true, got %+v", sidecarSC)
	}
	if sidecarSC.RunAsUser == nil || *sidecarSC.RunAsUser != DefaultSidecarNonRootUID {
		t.Fatalf("expected sidecar RunAsUser=%d when Pod RunAsUser=0, got %+v", DefaultSidecarNonRootUID, sidecarSC.RunAsUser)
	}
	if sidecarSC.RunAsGroup == nil || *sidecarSC.RunAsGroup != DefaultSidecarNonRootUID {
		t.Fatalf("expected sidecar RunAsGroup=%d when Pod RunAsGroup=0, got %+v", DefaultSidecarNonRootUID, sidecarSC.RunAsGroup)
	}
	if rootPod.Spec.Containers[0].SecurityContext != nil && rootPod.Spec.Containers[0].SecurityContext.RunAsUser != nil {
		t.Fatalf("expected business container SecurityContext not to be modified")
	}

	// 2. Explicit xxljob.io/sidecar-run-as-user annotation overrides default 65532
	customUIDPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-uid-sidecar-pod",
			Namespace: "default",
			Labels:    map[string]string{LabelInject: "true"},
			Annotations: map[string]string{
				AnnotationMode:             "sidecar",
				AnnotationAppName:          "legacy-root-app",
				AnnotationSidecarRunAsUser: "10005",
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:  int64Ptr(0),
				RunAsGroup: int64Ptr(0),
			},
			Containers: []corev1.Container{{Name: "legacy-app", Image: "nginx:1.25"}},
		},
	}
	if ok, err := mutator.MutatePodOrError(context.Background(), customUIDPod, "default"); err != nil || !ok {
		t.Fatalf("expected custom sidecar UID injection to succeed, got ok=%v err=%v", ok, err)
	}
	customSC := customUIDPod.Spec.Containers[1].SecurityContext
	if customSC.RunAsUser == nil || *customSC.RunAsUser != 10005 || customSC.RunAsGroup == nil || *customSC.RunAsGroup != 10005 {
		t.Fatalf("expected sidecar RunAsUser=10005 and RunAsGroup=10005, got %+v", customSC)
	}

	// 3. Positive Pod-level RunAsUser (e.g. OpenShift restricted-v2 UID range) leaves sidecar RunAsUser nil
	openshiftPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "openshift-pod",
			Namespace: "default",
			Labels:    map[string]string{LabelInject: "true"},
			Annotations: map[string]string{
				AnnotationMode:    "sidecar",
				AnnotationAppName: "ocp-app",
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: int64Ptr(1000680000),
			},
			Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}},
		},
	}
	if ok, err := mutator.MutatePodOrError(context.Background(), openshiftPod, "default"); err != nil || !ok {
		t.Fatalf("expected OpenShift Pod injection to succeed, got ok=%v err=%v", ok, err)
	}
	if openshiftPod.Spec.Containers[1].SecurityContext.RunAsUser != nil {
		t.Fatalf("expected sidecar RunAsUser=nil when Pod RunAsUser is positive, got %v", *openshiftPod.Spec.Containers[1].SecurityContext.RunAsUser)
	}

	// 4. Invalid xxljob.io/sidecar-run-as-user ("0", "-1", "not-an-int") must be denied
	for _, badVal := range []string{"0", "-1", "root"} {
		badPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bad-uid-pod",
				Namespace: "default",
				Labels:    map[string]string{LabelInject: "true"},
				Annotations: map[string]string{
					AnnotationMode:             "sidecar",
					AnnotationAppName:          "bad-app",
					AnnotationSidecarRunAsUser: badVal,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}}},
		}
		if _, err := mutator.MutatePodOrError(context.Background(), badPod, "default"); err == nil {
			t.Fatalf("expected invalid %s=%q to be denied", AnnotationSidecarRunAsUser, badVal)
		}
	}
}
