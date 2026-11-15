package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
)

const (
	AdminFinalizer                 = "xxljob.io/admin-finalizer"
	AnnotationAdminCredentialsHash = "xxljob.io/credentials-hash"
)

// XxlJobAdminReconciler reconciles a XxlJobAdmin object.
type XxlJobAdminReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobadmins,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobadmins/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobadmins/finalizers,verbs=update
// +kubebuilder:rbac:groups=xxljob.io,resources=xxljobexecutors;xxljobtasks,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *XxlJobAdminReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var admin xxljobv1alpha1.XxlJobAdmin
	if err := r.Get(ctx, req.NamespacedName, &admin); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	// Handle Deletion via Finalizer: block deletion while dependent XxlJobExecutors still exist (fixes P0-6, P1-2, R4-P1-3)
	if !admin.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&admin, AdminFinalizer) {
			if !shouldBypassDependencyCheck(ctx, reader, &admin) {
				dependentCount, err := r.countReferencingExecutors(ctx, &admin)
				if err != nil {
					return ctrl.Result{}, err
				}
				if dependentCount > 0 {
					logger.Info("Delaying XxlJobAdmin deletion until referencing XxlJobExecutors are removed", "dependentExecutors", dependentCount)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}
			controllerutil.RemoveFinalizer(&admin, AdminFinalizer)
			if err := r.Update(ctx, &admin); err != nil {
				return ctrl.Result{}, err
			}
			purgeAdminCaches(ctx, r.Client, reader, &admin)
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&admin, AdminFinalizer) {
		controllerutil.AddFinalizer(&admin, AdminFinalizer)
		if err := r.Update(ctx, &admin); err != nil {
			return ctrl.Result{}, err
		}
	}

	if strings.TrimSpace(admin.Spec.ExternalURL) != "" {
		if err := validateExternalAdminURL(admin.Spec.ExternalURL); err != nil {
			admin.Status.Phase = "Pending"
			meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "ReconcileFailed",
				Message:            err.Error(),
				LastTransitionTime: metav1.Now(),
			})
			_ = r.Status().Update(ctx, &admin)
			return ctrl.Result{}, err
		}
	}

	// If ExternalURL is set and Image is omitted, treat as external XXL-JOB Admin instance
	// and garbage-collect any previously created in-cluster Deployment/Service owned by this CR (fixes EG2-06).
	if strings.TrimSpace(admin.Spec.ExternalURL) != "" && strings.TrimSpace(admin.Spec.Image) == "" {
		if err := r.cleanupOwnedInClusterResources(ctx, &admin); err != nil {
			return ctrl.Result{}, err
		}
		adminURL := ResolveAdminURL(&admin)
		desiredEndpointMeta := encodeSyncedAdminEndpointMeta(&admin, adminURL)
		prevEndpointCond := meta.FindStatusCondition(admin.Status.Conditions, ConditionAdminEndpointSynced)
		endpointCondChanged := desiredEndpointMeta != "" && (prevEndpointCond == nil || prevEndpointCond.Status != metav1.ConditionTrue || prevEndpointCond.Message != desiredEndpointMeta)
		if admin.Status.Phase != "Running" ||
			admin.Status.ReadyReplicas != 0 ||
			admin.Status.AdminURL != adminURL ||
			admin.Status.ObservedGeneration != admin.Generation ||
			endpointCondChanged {
			prevAdminURL := admin.Status.AdminURL
			if prevAdminURL != "" && strings.TrimRight(strings.TrimSpace(prevAdminURL), "/") != strings.TrimRight(strings.TrimSpace(adminURL), "/") {
				recordPreviousAdminURLCondition(&admin, prevAdminURL)
			} else {
				recordRetiredAdminEndpointCondition(&admin)
			}
			syncAdminCurrentEndpointCondition(&admin, adminURL)
			admin.Status.Phase = "Running"
			admin.Status.ReadyReplicas = 0
			admin.Status.AdminURL = adminURL
			admin.Status.ObservedGeneration = admin.Generation
			meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "ExternalAdminConfigured",
				Message:            fmt.Sprintf("Using external XXL-JOB Admin at %s", admin.Status.AdminURL),
				LastTransitionTime: metav1.Now(),
			})
			if err := r.Status().Update(ctx, &admin); err != nil {
				return ctrl.Result{}, err
			}
			if prevAdminURL != "" && prevAdminURL != adminURL {
				evictClientCacheByAdminURL(prevAdminURL)
			} else if endpointCondChanged {
				evictClientCacheByAdminURL(adminURL)
			}
		}
		if admin.Spec.Auth.PasswordSecretRef != nil || admin.Spec.Auth.AccessTokenSecretRef != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// 1. Reconcile Admin Deployment via CreateOrUpdate (preserving HPA replicas if Spec.Replicas is nil, fixes P1-3 & P1-6)
	desiredDeploy, err := r.buildDeployment(ctx, &admin)
	if err != nil {
		admin.Status.Phase = "Pending"
		meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ReconcileFailed",
			Message:            err.Error(),
			LastTransitionTime: metav1.Now(),
		})
		_ = r.Status().Update(ctx, &admin)
		return ctrl.Result{}, err
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desiredDeploy.Name,
			Namespace: desiredDeploy.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		if deploy.Labels == nil {
			deploy.Labels = desiredDeploy.Labels
		}
		if admin.Spec.Replicas != nil || deploy.Spec.Replicas == nil {
			deploy.Spec.Replicas = desiredDeploy.Spec.Replicas
		}
		deploy.Spec.Selector = desiredDeploy.Spec.Selector
		if deploy.Spec.Template.ObjectMeta.Labels == nil {
			deploy.Spec.Template.ObjectMeta.Labels = make(map[string]string)
		}
		for k, v := range desiredDeploy.Spec.Template.ObjectMeta.Labels {
			deploy.Spec.Template.ObjectMeta.Labels[k] = v
		}
		if len(desiredDeploy.Spec.Template.ObjectMeta.Annotations) > 0 {
			if deploy.Spec.Template.ObjectMeta.Annotations == nil {
				deploy.Spec.Template.ObjectMeta.Annotations = make(map[string]string)
			}
			prevHash := deploy.Spec.Template.ObjectMeta.Annotations[AnnotationAdminCredentialsHash]
			newHash := desiredDeploy.Spec.Template.ObjectMeta.Annotations[AnnotationAdminCredentialsHash]
			if prevHash != newHash {
				secretReader := r.APIReader
				if secretReader == nil {
					secretReader = r.Client
				}
				invalidateSecretCache(secretReader, admin.Namespace, admin.Spec.Datasource.PasswordSecretRef)
				invalidateSecretCache(secretReader, admin.Namespace, admin.Spec.Auth.PasswordSecretRef)
				invalidateSecretCache(secretReader, admin.Namespace, admin.Spec.Auth.AccessTokenSecretRef)
			}
			for k, v := range desiredDeploy.Spec.Template.ObjectMeta.Annotations {
				deploy.Spec.Template.ObjectMeta.Annotations[k] = v
			}
		} else if deploy.Spec.Template.ObjectMeta.Annotations != nil {
			delete(deploy.Spec.Template.ObjectMeta.Annotations, AnnotationAdminCredentialsHash)
		}
		desiredContainer := desiredDeploy.Spec.Template.Spec.Containers[0]
		if len(deploy.Spec.Template.Spec.Containers) == 0 {
			deploy.Spec.Template.Spec = desiredDeploy.Spec.Template.Spec
		} else {
			c := &deploy.Spec.Template.Spec.Containers[0]
			c.Name = desiredContainer.Name
			c.Image = desiredContainer.Image
			c.Command = desiredContainer.Command
			c.Args = desiredContainer.Args
			c.Env = desiredContainer.Env
			c.Resources = desiredContainer.Resources
			c.Ports = desiredContainer.Ports
			c.ReadinessProbe = desiredContainer.ReadinessProbe
			c.LivenessProbe = desiredContainer.LivenessProbe
		}
		if r.Scheme != nil {
			return controllerutil.SetControllerReference(&admin, deploy, r.Scheme)
		}
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Reconcile Admin Service via CreateOrUpdate
	desiredSvc, err := r.buildService(&admin)
	if err != nil {
		return ctrl.Result{}, err
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desiredSvc.Name,
			Namespace: desiredSvc.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if svc.Labels == nil {
			svc.Labels = desiredSvc.Labels
		}
		svc.Spec.Type = desiredSvc.Spec.Type
		svc.Spec.Selector = desiredSvc.Spec.Selector
		svc.Spec.Ports = desiredSvc.Spec.Ports
		if r.Scheme != nil {
			return controllerutil.SetControllerReference(&admin, svc, r.Scheme)
		}
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Update Status only if changed (prevents hot reconcile loops, fixes P0-1)
	var latestDeploy appsv1.Deployment
	_ = r.Get(ctx, types.NamespacedName{Namespace: admin.Namespace, Name: desiredDeploy.Name}, &latestDeploy)
	readyReplicas := latestDeploy.Status.ReadyReplicas
	phase := "Pending"
	condStatus := metav1.ConditionFalse
	reason := "WaitingForReplicas"
	if readyReplicas > 0 {
		phase = "Running"
		condStatus = metav1.ConditionTrue
		reason = "ReplicasReady"
	}
	adminURL := ResolveAdminURL(&admin)
	desiredEndpointMeta := encodeSyncedAdminEndpointMeta(&admin, adminURL)
	prevEndpointCond := meta.FindStatusCondition(admin.Status.Conditions, ConditionAdminEndpointSynced)
	endpointCondChanged := desiredEndpointMeta != "" && (prevEndpointCond == nil || prevEndpointCond.Status != metav1.ConditionTrue || prevEndpointCond.Message != desiredEndpointMeta)

	if admin.Status.Phase != phase ||
		admin.Status.ReadyReplicas != readyReplicas ||
		admin.Status.AdminURL != adminURL ||
		admin.Status.ObservedGeneration != admin.Generation ||
		endpointCondChanged {
		prevAdminURL := admin.Status.AdminURL
		if prevAdminURL != "" && strings.TrimRight(strings.TrimSpace(prevAdminURL), "/") != strings.TrimRight(strings.TrimSpace(adminURL), "/") {
			recordPreviousAdminURLCondition(&admin, prevAdminURL)
		} else {
			recordRetiredAdminEndpointCondition(&admin)
		}
		syncAdminCurrentEndpointCondition(&admin, adminURL)
		admin.Status.Phase = phase
		admin.Status.ReadyReplicas = readyReplicas
		admin.Status.AdminURL = adminURL
		admin.Status.ObservedGeneration = admin.Generation
		meta.SetStatusCondition(&admin.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             condStatus,
			Reason:             reason,
			Message:            fmt.Sprintf("XXL-JOB Admin URL: %s (ReadyReplicas=%d)", adminURL, readyReplicas),
			LastTransitionTime: metav1.Now(),
		})

		if err := r.Status().Update(ctx, &admin); err != nil {
			return ctrl.Result{}, err
		}
		if prevAdminURL != "" && prevAdminURL != adminURL {
			evictClientCacheByAdminURL(prevAdminURL)
		} else if endpointCondChanged {
			evictClientCacheByAdminURL(adminURL)
		}
	}
	if admin.Spec.Datasource.PasswordSecretRef != nil ||
		admin.Spec.Auth.PasswordSecretRef != nil ||
		admin.Spec.Auth.AccessTokenSecretRef != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func validateExternalAdminURL(rawURL string) error {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil
	}
	for _, r := range trimmed {
		if unicode.IsSpace(r) || r < 0x20 || r == 0x7f || strings.ContainsRune(`'"`+"`$;&|<>()\\", r) {
			return fmt.Errorf("invalid externalURL %q: contains forbidden whitespace, control, or shell characters", rawURL)
		}
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("malformed externalURL %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("externalURL %q must use http or https scheme", rawURL)
	}
	if u.Host == "" {
		return fmt.Errorf("externalURL %q must specify a host", rawURL)
	}
	if u.User != nil {
		return fmt.Errorf("externalURL %q must not embed userinfo credentials", rawURL)
	}
	if u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(trimmed, "?#") {
		return fmt.Errorf("externalURL %q must not contain query parameters or fragments", rawURL)
	}
	return nil
}

func normalizeAndValidateContextPath(rawPath string) (string, error) {
	if rawPath == "" {
		return "/xxl-job-admin", nil
	}
	for _, r := range rawPath {
		if unicode.IsSpace(r) || r < 0x20 || r == 0x7f || strings.ContainsRune(`'"`+"`$;&|<>()?#"+"\\", r) {
			return "", fmt.Errorf("invalid contextPath %q: must not contain whitespace, quotes, shell metacharacters, query, or fragment characters", rawPath)
		}
	}
	if strings.Contains(rawPath, "..") || strings.Contains(rawPath, "--") || strings.Contains(rawPath, "//") {
		return "", fmt.Errorf("invalid contextPath %q: must not contain '..', '--', or '//'", rawPath)
	}
	normalized := rawPath
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	if len(normalized) > 1 {
		normalized = strings.TrimRight(normalized, "/")
	}
	return normalized, nil
}

func (r *XxlJobAdminReconciler) countReferencingExecutors(ctx context.Context, admin *xxljobv1alpha1.XxlJobAdmin) (int, error) {
	var list xxljobv1alpha1.XxlJobExecutorList
	if err := r.List(ctx, &list, client.InNamespace(admin.Namespace)); err != nil {
		return 0, err
	}

	candidateURLs := make(map[string]bool, 4)
	if u := strings.TrimRight(ResolveAdminURL(admin), "/"); u != "" {
		candidateURLs[u] = true
	}
	if su := strings.TrimRight(strings.TrimSpace(admin.Status.AdminURL), "/"); su != "" {
		candidateURLs[su] = true
	}
	for _, pu := range extractPreviousAdminURLs(admin) {
		if pu != "" {
			candidateURLs[pu] = true
		}
	}

	var adminList xxljobv1alpha1.XxlJobAdminList
	if err := r.List(ctx, &adminList, client.InNamespace(admin.Namespace)); err != nil {
		return 0, err
	}
	for i := range adminList.Items {
		other := &adminList.Items[i]
		if other.Name == admin.Name || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if ou := strings.TrimRight(ResolveAdminURL(other), "/"); ou != "" {
			delete(candidateURLs, ou)
		}
		if osu := strings.TrimRight(strings.TrimSpace(other.Status.AdminURL), "/"); osu != "" {
			delete(candidateURLs, osu)
		}
	}

	matchesSavedEndpointAdmin := func(conds []metav1.Condition) (matched bool, hasExplicitIdentity bool) {
		saved := extractSyncedAdminEndpointMeta(conds)
		if saved == nil || strings.TrimSpace(saved.AdminName) == "" {
			return false, false
		}
		if saved.AdminName != admin.Name {
			return false, true
		}
		if saved.AdminUID != "" && admin.UID != "" && saved.AdminUID != string(admin.UID) {
			return false, true
		}
		return true, true
	}

	count := 0
	for i := range list.Items {
		item := &list.Items[i]
		refNS := item.Spec.AdminRef.Namespace
		if refNS == "" {
			refNS = item.Namespace
		}
		if refNS == admin.Namespace && item.Spec.AdminRef.Name == admin.Name {
			count++
			continue
		}
		itemAdminURL := strings.TrimRight(strings.TrimSpace(item.Status.AdminURL), "/")
		hasRemoteExecState := item.Status.JobGroupID > 0 ||
			len(item.Status.RegisteredAddresses) > 0 ||
			len(item.Status.DrainedAddresses) > 0 ||
			len(item.Status.RegistryKeepAliveAddresses) > 0
		if !hasRemoteExecState {
			continue
		}
		if matched, explicit := matchesSavedEndpointAdmin(item.Status.Conditions); explicit {
			if matched {
				count++
			}
			continue
		}
		if itemAdminURL != "" && candidateURLs[itemAdminURL] {
			count++
		}
	}

	var taskList xxljobv1alpha1.XxlJobTaskList
	if err := r.List(ctx, &taskList, client.InNamespace(admin.Namespace)); err != nil {
		return 0, err
	}
	for i := range taskList.Items {
		t := &taskList.Items[i]
		if t.Status.JobID <= 0 && t.Status.JobGroupID <= 0 {
			continue
		}
		if matched, explicit := matchesSavedEndpointAdmin(t.Status.Conditions); explicit {
			if matched {
				count++
			}
			continue
		}
		taskAdminURL := strings.TrimRight(strings.TrimSpace(t.Status.AdminURL), "/")
		if taskAdminURL != "" && candidateURLs[taskAdminURL] {
			count++
		}
	}

	return count, nil
}

func (r *XxlJobAdminReconciler) buildDeployment(ctx context.Context, admin *xxljobv1alpha1.XxlJobAdmin) (*appsv1.Deployment, error) {
	image := strings.TrimSpace(admin.Spec.Image)
	if image == "" {
		image = "xuxueli/xxl-job-admin:2.4.1"
	}
	replicas := int32(2)
	if admin.Spec.Replicas != nil {
		replicas = *admin.Spec.Replicas
	}
	port := admin.Spec.Port
	if port <= 0 {
		port = 8080
	}
	contextPath, err := normalizeAndValidateContextPath(admin.Spec.ContextPath)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       "xxl-job-admin",
		"app.kubernetes.io/instance":   admin.Name,
		"app.kubernetes.io/managed-by": "xxl-job-operator",
	}

	envVars := []corev1.EnvVar{
		{Name: "TZ", Value: "Asia/Shanghai"},
		{Name: "SPRING_DATASOURCE_URL", Value: admin.Spec.Datasource.URL},
		{Name: "SPRING_DATASOURCE_USERNAME", Value: admin.Spec.Datasource.Username},
	}

	if admin.Spec.Datasource.PasswordSecretRef != nil {
		envVars = append(envVars, corev1.EnvVar{
			Name: "SPRING_DATASOURCE_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: admin.Spec.Datasource.PasswordSecretRef.Name},
					Key:                  admin.Spec.Datasource.PasswordSecretRef.Key,
				},
			},
		})
	} else {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "SPRING_DATASOURCE_PASSWORD",
			Value: admin.Spec.Datasource.Password,
		})
	}

	if admin.Spec.Auth.AccessTokenSecretRef != nil {
		secSelector := &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: admin.Spec.Auth.AccessTokenSecretRef.Name},
			Key:                  admin.Spec.Auth.AccessTokenSecretRef.Key,
		}
		envVars = append(envVars,
			corev1.EnvVar{
				Name:      "XXL_JOB_ACCESSTOKEN",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: secSelector},
			},
			corev1.EnvVar{
				Name:      "XXL_JOB_ACCESS_TOKEN",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: secSelector.DeepCopy()},
			},
		)
	} else {
		token := sanitizeTokenValue(admin.Spec.Auth.AccessToken)
		if token == "" {
			token = "default_token"
		}
		envVars = append(envVars,
			corev1.EnvVar{
				Name:  "XXL_JOB_ACCESSTOKEN",
				Value: token,
			},
			corev1.EnvVar{
				Name:  "XXL_JOB_ACCESS_TOKEN",
				Value: token,
			},
		)
	}

	// Only pass non-sensitive server flags in PARAMS; database credentials and accessToken are bound directly
	// by Spring Boot from SPRING_DATASOURCE_* and XXL_JOB_ACCESSTOKEN environment variables so secrets never
	// leak in /proc/*/cmdline or break on whitespace (fixes R5-P2-1).
	params := fmt.Sprintf("--server.port=%d --server.servlet.context-path=%s", port, contextPath)
	envVars = append(envVars, corev1.EnvVar{Name: "PARAMS", Value: params})
	if admin.Spec.ExtraJavaOpts != "" {
		envVars = append(envVars, corev1.EnvVar{Name: "JAVA_OPTS", Value: admin.Spec.ExtraJavaOpts})
	}

	probePath := strings.TrimRight(contextPath, "/") + "/toLogin"

	var podAnnotations map[string]string
	credHash, err := r.computeCredentialsHash(ctx, admin)
	if err != nil {
		return nil, err
	}
	if credHash != "" {
		podAnnotations = map[string]string{
			AnnotationAdminCredentialsHash: credHash,
		}
	}

	var adminCommand []string
	if admin.Spec.Auth.AccessTokenSecretRef != nil && admin.Spec.Auth.AccessTokenSecretRef.Name != "" {
		adminCommand = buildAdminContainerCommand()
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      admin.Name,
			Namespace: admin.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "xxl-job-admin",
							Image:     image,
							Command:   adminCommand,
							Env:       envVars,
							Resources: admin.Spec.Resources,
							Ports: []corev1.ContainerPort{
								{Name: "http", ContainerPort: port, Protocol: corev1.ProtocolTCP},
							},
							ReadinessProbe: buildHTTPProbe(probePath, port, 10, 10),
							LivenessProbe:  buildHTTPProbe(probePath, port, 30, 15),
						},
					},
				},
			},
		},
	}

	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(admin, deploy, r.Scheme); err != nil {
			return nil, err
		}
	}
	return deploy, nil
}

// buildAdminContainerCommand normalizes XXL_JOB_ACCESSTOKEN and XXL_JOB_ACCESS_TOKEN in container memory
// (stripping CR/LF and trimming leading/trailing whitespace identically to Operator sanitizeTokenValue,
// ProxyExecutor, Sidecar, and PreStop hooks) before exec'ing the XXL-JOB Admin JVM without exposing
// secret plaintext in Deployment.Spec or /proc/*/cmdline.
func buildAdminContainerCommand() []string {
	script := `norm_tok() { v="$1"; cr="$(printf '\r')"; lf="$(printf '\nx')"; lf="${lf%x}"; ` +
		`while [ "${v#*"$cr"}" != "$v" ]; do v="${v%%"$cr"*}${v#*"$cr"}"; done; ` +
		`while [ "${v#*"$lf"}" != "$v" ]; do v="${v%%"$lf"*}${v#*"$lf"}"; done; ` +
		`v="${v#"${v%%[![:space:]]*}"}"; v="${v%"${v##*[![:space:]]}"}"; printf '%s' "$v"; }; ` +
		`if [ -n "${XXL_JOB_ACCESSTOKEN+x}" ]; then XXL_JOB_ACCESSTOKEN="$(norm_tok "$XXL_JOB_ACCESSTOKEN")"; export XXL_JOB_ACCESSTOKEN; fi; ` +
		`if [ -n "${XXL_JOB_ACCESS_TOKEN+x}" ]; then XXL_JOB_ACCESS_TOKEN="$(norm_tok "$XXL_JOB_ACCESS_TOKEN")"; export XXL_JOB_ACCESS_TOKEN; fi; ` +
		`exec java ${JAVA_OPTS:-} ${LOG_HOME:+-DLOG_HOME="$LOG_HOME"} -jar /app.jar ${PARAMS:-}`
	return []string{"/bin/sh", "-c", script}
}

// computeCredentialsHash computes a salted SHA-256 digest of all Secret values consumed by the Admin Pod
// (Datasource.PasswordSecretRef and Auth.AccessTokenSecretRef) so rotating a Secret triggers a rolling restart
// of the Admin Deployment, while returning an error on transient Secret read failures so a transient API hiccup
// never writes "<missing>" or causes spurious Deployment rollouts (fixes R6-P1-9 & R7-P2-10).
func (r *XxlJobAdminReconciler) computeCredentialsHash(ctx context.Context, admin *xxljobv1alpha1.XxlJobAdmin) (string, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if reader == nil {
		return "", nil
	}
	refs := []*xxljobv1alpha1.SecretKeyReference{
		admin.Spec.Datasource.PasswordSecretRef,
		admin.Spec.Auth.PasswordSecretRef,
		admin.Spec.Auth.AccessTokenSecretRef,
	}
	hasAnyRef := false
	h := sha256.New()
	for idx, ref := range refs {
		if ref == nil || ref.Name == "" || ref.Key == "" {
			continue
		}
		hasAnyRef = true
		var sec corev1.Secret
		if err := reader.Get(ctx, types.NamespacedName{Namespace: admin.Namespace, Name: ref.Name}, &sec); err != nil {
			return "", fmt.Errorf("read referenced Secret %s/%s for credentials hash: %w", admin.Namespace, ref.Name, err)
		}
		val, ok := sec.Data[ref.Key]
		if !ok {
			return "", fmt.Errorf("key %q not found in Secret %s/%s for credentials hash", ref.Key, admin.Namespace, ref.Name)
		}
		if ref == admin.Spec.Auth.AccessTokenSecretRef && sanitizeTokenValue(string(val)) == "" {
			return "", fmt.Errorf("accessToken key %q in Secret %s/%s must not be empty or whitespace-only", ref.Key, admin.Namespace, ref.Name)
		}
		_, _ = fmt.Fprintf(h, "%d:%s/%s/%s=%s;", idx, sec.UID, ref.Name, ref.Key, string(val))
	}
	if !hasAnyRef {
		return "", nil
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func buildHTTPProbe(path string, port, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   path,
				Port:   intstr.FromInt32(port),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: initialDelay,
		TimeoutSeconds:      1,
		PeriodSeconds:       period,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
}

func (r *XxlJobAdminReconciler) buildService(admin *xxljobv1alpha1.XxlJobAdmin) (*corev1.Service, error) {
	port := admin.Spec.Port
	if port <= 0 {
		port = 8080
	}
	svcType := admin.Spec.ServiceType
	if svcType == "" {
		svcType = corev1.ServiceTypeClusterIP
	}
	labels := map[string]string{
		"app.kubernetes.io/name":       "xxl-job-admin",
		"app.kubernetes.io/instance":   admin.Name,
		"app.kubernetes.io/managed-by": "xxl-job-operator",
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      admin.Name,
			Namespace: admin.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	if r.Scheme != nil {
		if err := controllerutil.SetControllerReference(admin, svc, r.Scheme); err != nil {
			return nil, err
		}
	}
	return svc, nil
}

func (r *XxlJobAdminReconciler) cleanupOwnedInClusterResources(ctx context.Context, admin *xxljobv1alpha1.XxlJobAdmin) error {
	var deploy appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: admin.Namespace, Name: admin.Name}, &deploy); err == nil {
		if deploy.DeletionTimestamp.IsZero() && metav1.IsControlledBy(&deploy, admin) {
			if delErr := r.Delete(ctx, &deploy); delErr != nil && !apierrors.IsNotFound(delErr) {
				return delErr
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	var svc corev1.Service
	if err := r.Get(ctx, types.NamespacedName{Namespace: admin.Namespace, Name: admin.Name}, &svc); err == nil {
		if svc.DeletionTimestamp.IsZero() && metav1.IsControlledBy(&svc, admin) {
			if delErr := r.Delete(ctx, &svc); delErr != nil && !apierrors.IsNotFound(delErr) {
				return delErr
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *XxlJobAdminReconciler) findDeletingAdminsForExecutor(ctx context.Context, obj client.Object) []reconcile.Request {
	if obj == nil {
		return nil
	}
	var adminList xxljobv1alpha1.XxlJobAdminList
	if err := r.List(ctx, &adminList, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range adminList.Items {
		item := &adminList.Items[i]
		if !item.DeletionTimestamp.IsZero() {
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

func syncedEndpointConditionChanged(oldConds, newConds []metav1.Condition) bool {
	oldCond := meta.FindStatusCondition(oldConds, ConditionAdminEndpointSynced)
	newCond := meta.FindStatusCondition(newConds, ConditionAdminEndpointSynced)
	if (oldCond == nil) != (newCond == nil) {
		return true
	}
	if oldCond == nil {
		return false
	}
	return oldCond.Status != newCond.Status || oldCond.Message != newCond.Message
}

func executorForDeletingAdminPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldExec, ok1 := e.ObjectOld.(*xxljobv1alpha1.XxlJobExecutor)
			newExec, ok2 := e.ObjectNew.(*xxljobv1alpha1.XxlJobExecutor)
			if !ok1 || !ok2 {
				return false
			}
			return oldExec.Spec.AdminRef != newExec.Spec.AdminRef ||
				oldExec.Status.AdminURL != newExec.Status.AdminURL ||
				oldExec.Status.JobGroupID != newExec.Status.JobGroupID ||
				syncedEndpointConditionChanged(oldExec.Status.Conditions, newExec.Status.Conditions)
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
	}
}

func taskForDeletingAdminPredicate() predicate.Predicate {
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
			return oldTask.Status.AdminURL != newTask.Status.AdminURL ||
				oldTask.Status.JobID != newTask.Status.JobID ||
				syncedEndpointConditionChanged(oldTask.Status.Conditions, newTask.Status.Conditions)
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *XxlJobAdminReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&xxljobv1alpha1.XxlJobAdmin{}, builder.WithPredicates(
			predicate.Or(
				predicate.GenerationChangedPredicate{},
				predicate.AnnotationChangedPredicate{},
				deletionTimestampChangedPredicate(),
			),
		)).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(
			&xxljobv1alpha1.XxlJobExecutor{},
			handler.EnqueueRequestsFromMapFunc(r.findDeletingAdminsForExecutor),
			builder.WithPredicates(executorForDeletingAdminPredicate()),
		).
		Watches(
			&xxljobv1alpha1.XxlJobTask{},
			handler.EnqueueRequestsFromMapFunc(r.findDeletingAdminsForExecutor),
			builder.WithPredicates(taskForDeletingAdminPredicate()),
		).
		Complete(r)
}
