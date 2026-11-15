package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

func setupTestAdminServer(t *testing.T, failCallback *atomic.Bool) (
	*httptest.Server,
	*sync.Mutex,
	map[string]map[string]bool,
	map[int]xxlclient.JobGroup,
	map[int]xxlclient.JobInfo,
	*[]xxlclient.HandleCallbackParam,
) {
	t.Helper()
	var mu sync.Mutex
	registries := make(map[string]map[string]bool)
	groups := make(map[int]xxlclient.JobGroup)
	jobs := make(map[int]xxlclient.JobInfo)
	var callbacks []xxlclient.HandleCallbackParam
	nextGroupID := 1
	nextJobID := 500

	mux := http.NewServeMux()
	mux.HandleFunc("/login", func(w http.ResponseWriter, _ *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "ok", Path: "/"})
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/api/registry", func(w http.ResponseWriter, r *http.Request) {
		var p xxlclient.RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		if registries[p.RegistryKey] == nil {
			registries[p.RegistryKey] = make(map[string]bool)
		}
		registries[p.RegistryKey][p.RegistryValue] = true
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/api/registryRemove", func(w http.ResponseWriter, r *http.Request) {
		var p xxlclient.RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		if registries[p.RegistryKey] != nil {
			delete(registries[p.RegistryKey], p.RegistryValue)
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/api/callback", func(w http.ResponseWriter, r *http.Request) {
		if failCallback != nil && failCallback.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("admin restarting"))
			return
		}
		var cbs []xxlclient.HandleCallbackParam
		_ = json.NewDecoder(r.Body).Decode(&cbs)
		mu.Lock()
		callbacks = append(callbacks, cbs...)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobgroup/pageList", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		appName := r.FormValue("appname")
		mu.Lock()
		var list []xxlclient.JobGroup
		for _, g := range groups {
			if appName == "" || g.AppName == appName {
				list = append(list, g)
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.JobGroupPageResult{Data: list})
	})
	mux.HandleFunc("/jobgroup/save", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		addrType, _ := strconv.Atoi(r.FormValue("addressType"))
		mu.Lock()
		id := nextGroupID
		nextGroupID++
		groups[id] = xxlclient.JobGroup{
			ID:          id,
			AppName:     r.FormValue("appname"),
			Title:       r.FormValue("title"),
			AddressType: addrType,
			AddressList: r.FormValue("addressList"),
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobgroup/update", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		addrType, _ := strconv.Atoi(r.FormValue("addressType"))
		mu.Lock()
		groups[id] = xxlclient.JobGroup{
			ID:          id,
			AppName:     r.FormValue("appname"),
			Title:       r.FormValue("title"),
			AddressType: addrType,
			AddressList: r.FormValue("addressList"),
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobgroup/remove", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		mu.Lock()
		grp, exists := groups[id]
		if !exists {
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 500, Msg: "group not found"})
			return
		}
		for _, j := range jobs {
			if j.JobGroup == grp.ID {
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 500, Msg: "拒绝删除：该执行器下仍存在任务"})
				return
			}
		}
		delete(groups, id)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobinfo/pageList", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		handler := r.FormValue("executorHandler")
		grpFilter, _ := strconv.Atoi(r.FormValue("jobGroup"))
		mu.Lock()
		var list []xxlclient.JobInfo
		for _, j := range jobs {
			if (handler == "" || j.ExecutorHandler == handler) && (grpFilter <= 0 || j.JobGroup == grpFilter) {
				list = append(list, j)
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.JobInfoPageResult{RecordsTotal: len(list), Data: list})
	})
	mux.HandleFunc("/jobinfo/add", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		grp, _ := strconv.Atoi(r.FormValue("jobGroup"))
		mu.Lock()
		id := nextJobID
		nextJobID++
		jobs[id] = xxlclient.JobInfo{
			ID:              id,
			JobGroup:        grp,
			JobDesc:         r.FormValue("jobDesc"),
			Author:          r.FormValue("author"),
			ScheduleType:    r.FormValue("scheduleType"),
			ScheduleConf:    r.FormValue("scheduleConf"),
			ExecutorHandler: r.FormValue("executorHandler"),
			GlueType:        r.FormValue("glueType"),
		}
		mu.Unlock()
		rawID, _ := json.Marshal(strconv.Itoa(id))
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200, Content: rawID})
	})
	mux.HandleFunc("/jobinfo/update", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		grp, _ := strconv.Atoi(r.FormValue("jobGroup"))
		mu.Lock()
		if j, exists := jobs[id]; exists {
			if grp > 0 {
				j.JobGroup = grp
			}
			if desc := r.FormValue("jobDesc"); desc != "" {
				j.JobDesc = desc
			}
			jobs[id] = j
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobinfo/start", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		mu.Lock()
		j := jobs[id]
		j.TriggerStatus = 1
		jobs[id] = j
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobinfo/stop", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		mu.Lock()
		j := jobs[id]
		j.TriggerStatus = 0
		jobs[id] = j
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	mux.HandleFunc("/jobinfo/remove", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, _ := strconv.Atoi(r.FormValue("id"))
		mu.Lock()
		delete(jobs, id)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &mu, registries, groups, jobs, &callbacks
}

func TestControllersEndToEnd(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	var failCallback atomic.Bool
	adminSrv, mu, registries, groups, jobs, callbacks := setupTestAdminServer(t, &failCallback)

	// Omit Spec.Replicas (nil) so HPA preservation can be verified (fixes P1-3)
	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xxl-job-admin",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Image:       "xuxueli/xxl-job-admin:2.4.1",
			ExternalURL: adminSrv.URL,
			Datasource: xxljobv1alpha1.DatasourceSpec{
				URL:      "jdbc:mysql://mysql:3306/xxl_job",
				Username: "root",
				Password: "password",
			},
			Auth: xxljobv1alpha1.AuthSpec{
				Username:    "admin",
				Password:    "123456",
				AccessToken: "strong_tenant_token",
			},
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "order-pod-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "order-executor"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.10",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}

	// pod2 is NotReady (simulating terminating/draining pod)
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "order-pod-2",
			Namespace: "default",
			Labels:    map[string]string{"app": "order-executor"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.244.0.11",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "order-executor",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:     xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
			AppName:      "order-service-executor",
			Title:        "OrderExec",
			Mode:         xxljobv1alpha1.ExecutorModeResident,
			ExecutorPort: 9999,
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "order-executor"},
			},
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			DrainedAddresses: []string{"http://10.244.0.99:9999/"},
		},
	}

	// K8sJobProxy executor CR to verify R4-P0-1 (registration in xxl_job_registry) and R4-P0-2 (token & URL validation)
	proxyExecCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "k8s-proxy-exec",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
			AppName:  "k8s-job-proxy-executor",
			Title:    "ProxyExec",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
	}

	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "order-timeout-close",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "order-executor"},
			JobDesc:               "Close Expired Unpaid Orders",
			Author:                "order-team",
			ScheduleType:          "CRON",
			ScheduleConf:          "0 */1 * * * ?",
			ExecutorHandler:       "closeExpiredOrderHandler",
			ExecutorRouteStrategy: "SHARDING_BROADCAST",
			TriggerStatus:         true,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
		WithObjects(adminCR, pod1, pod2, execCR, proxyExecCR, taskCR).
		Build()

	ctx := context.Background()

	// 1. Reconcile XxlJobAdmin & verify HPA replica preservation (fixes P1-3)
	adminReconciler := &XxlJobAdminReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxl-job-admin"}}); err != nil {
		t.Fatalf("XxlJobAdminReconciler failed: %v", err)
	}

	var deploy appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxl-job-admin"}, &deploy); err != nil {
		t.Fatalf("expected Admin Deployment to be created: %v", err)
	}
	// R5-P2-1: Verify PARAMS does not contain cleartext password or accessToken
	for _, envVar := range deploy.Spec.Template.Spec.Containers[0].Env {
		if envVar.Name == "PARAMS" {
			if strings.Contains(envVar.Value, "spring.datasource.password") ||
				strings.Contains(envVar.Value, "xxl.job.accessToken") ||
				strings.Contains(envVar.Value, "strong_tenant_token") {
				t.Fatalf("R5-P2-1 regression: PARAMS must not contain cleartext credentials: %s", envVar.Value)
			}
		}
	}
	hpaReplicas := int32(5)
	deploy.Spec.Replicas = &hpaReplicas
	if err := k8sClient.Update(ctx, &deploy); err != nil {
		t.Fatalf("simulate HPA scale failed: %v", err)
	}
	if _, err := adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxl-job-admin"}}); err != nil {
		t.Fatalf("second XxlJobAdminReconciler failed: %v", err)
	}
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxl-job-admin"}, &deploy)
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 5 {
		t.Fatalf("expected HPA-scaled replicas=5 to be preserved when Spec.Replicas is nil, got %v", deploy.Spec.Replicas)
	}

	// 2. Reconcile XxlJobExecutor (Resident & K8sJobProxy) and verify R4-P0-1 & R5-P1-5 registry heartbeats
	// Note: 10.244.0.11 is NOT pre-populated in registries so we prove Reconcile actively registers draining-but-running pods!
	mu.Lock()
	registries["order-service-executor"] = map[string]bool{
		"http://10.244.0.99:9999/": true, // dead pod no longer running -> must be removed
	}
	mu.Unlock()

	execReconciler := &XxlJobExecutorReconciler{
		Client:                   k8sClient,
		Scheme:                   scheme,
		DefaultProxyExecutorAddr: "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/",
	}
	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("XxlJobExecutorReconciler failed: %v", err)
	}

	var updatedExec xxljobv1alpha1.XxlJobExecutor
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-executor"}, &updatedExec)
	if !updatedExec.Status.Synced || updatedExec.Status.JobGroupID != 1 {
		t.Fatalf("expected Executor synced with JobGroupID=1, got %+v", updatedExec.Status)
	}
	if updatedExec.Status.ActivePods != 1 || updatedExec.Status.TerminatingPodsDrained != 1 {
		t.Fatalf("expected ActivePods=1 and TerminatingPodsDrained=1, got %+v", updatedExec.Status)
	}

	// Verify AddressType=1 excludes draining pod2 (10.244.0.11) from AddressList, while xxl_job_registry
	// retains running draining pod2 (R5-P1-5) and removes dead pod (10.244.0.99)
	mu.Lock()
	if groups[1].AddressType != 1 || groups[1].AddressList != "http://10.244.0.10:9999/" {
		t.Fatalf("expected JobGroup to use AddressType=1 with active pod address, got %+v", groups[1])
	}
	if !registries["order-service-executor"]["http://10.244.0.10:9999/"] ||
		!registries["order-service-executor"]["http://10.244.0.11:9999/"] ||
		registries["order-service-executor"]["http://10.244.0.99:9999/"] {
		t.Fatalf("expected xxl_job_registry to keep active (10.244.0.10) + draining-running (10.244.0.11) and remove dead (10.244.0.99) per R5-P1-5, got %+v",
			registries["order-service-executor"])
	}
	mu.Unlock()

	// Reconcile K8sJobProxy executor and verify its scoped proxy URL is registered in xxl_job_registry (fixes R4-P0-1!)
	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "k8s-proxy-exec"}}); err != nil {
		t.Fatalf("K8sJobProxy Reconcile failed: %v", err)
	}
	expectedScopedProxyURL := "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/proxy/default/k8s-proxy-exec/"
	mu.Lock()
	if !registries["k8s-job-proxy-executor"][expectedScopedProxyURL] {
		t.Fatalf("R4-P0-1 regression: expected K8sJobProxy URL %q to be registered in xxl_job_registry so >10min jobs are not marked lost, got %+v",
			expectedScopedProxyURL, registries["k8s-job-proxy-executor"])
	}
	mu.Unlock()

	// 2a. Test R4-P0-2 & R5-P2-7: Reject proxyServiceAddress with subpath, query, or non-http scheme, and reject Admin with "default_token"
	badProxyExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "injected-proxy-exec",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:            xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
			AppName:             "injected-exec",
			Title:               "InjectedExec",
			Mode:                xxljobv1alpha1.ExecutorModeK8sJobProxy,
			ProxyServiceAddress: "http://proxy:9999/proxy/other-ns/other-exec/",
		},
	}
	_ = k8sClient.Create(ctx, badProxyExec)
	_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "injected-proxy-exec"}})
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "injected-proxy-exec"}, badProxyExec)
	if badProxyExec.Status.Synced || !strings.Contains(badProxyExec.Status.Message, "without subpath") {
		t.Fatalf("expected proxyServiceAddress with subpath to be rejected (R4-P0-2/R5-P2-7), got %+v", badProxyExec.Status)
	}
	if err := validateProxyServiceBaseURL("ftp://proxy:9999"); err == nil {
		t.Fatalf("R5-P2-7: expected ftp:// scheme to be rejected")
	}
	if err := validateProxyServiceBaseURL("http://proxy:9999?x=1"); err == nil {
		t.Fatalf("R5-P2-7: expected query string in proxyServiceAddress to be rejected")
	}

	defaultTokenAdmin := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "insecure-admin",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: adminSrv.URL,
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: "jdbc:mysql://mysql:3306/xxl_job", Username: "root"},
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "default_token"},
		},
	}
	insecureProxyExec := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "insecure-proxy-exec",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "insecure-admin"},
			AppName:  "insecure-exec",
			Title:    "InsecureExec",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
	}
	_ = k8sClient.Create(ctx, defaultTokenAdmin)
	_ = k8sClient.Create(ctx, insecureProxyExec)
	_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "insecure-proxy-exec"}})
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "insecure-proxy-exec"}, insecureProxyExec)
	if insecureProxyExec.Status.Synced || !strings.Contains(insecureProxyExec.Status.Message, "non-default accessToken") {
		t.Fatalf("expected K8sJobProxy referencing default_token Admin to be rejected (R4-P0-2), got %+v", insecureProxyExec.Status)
	}

	// Re-reconcile order-executor with the exact same cluster state: ResourceVersion MUST NOT change (fixes P0-1 hot loop!)
	rvBefore := updatedExec.ResourceVersion
	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("second XxlJobExecutorReconciler failed: %v", err)
	}
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-executor"}, &updatedExec)
	if updatedExec.ResourceVersion != rvBefore || updatedExec.Status.TerminatingPodsDrained != 1 {
		t.Fatalf("hot-loop detected: ResourceVersion changed (%s -> %s) or TerminatingPodsDrained incremented (%d)",
			rvBefore, updatedExec.ResourceVersion, updatedExec.Status.TerminatingPodsDrained)
	}

	// 2b. Test N-P0-2: When ALL pods are draining/NotReady, group MUST stay AddressType=1 with sentinel URL
	var currentPod1 corev1.Pod
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-pod-1"}, &currentPod1)
	currentPod1.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	_ = k8sClient.Status().Update(ctx, &currentPod1)

	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("reconcile with 0 ready pods failed: %v", err)
	}
	mu.Lock()
	if groups[1].AddressType != 1 || !strings.HasSuffix(groups[1].AddressList, "/proxy/_unavailable/") {
		t.Fatalf("expected AddressType=1 with /proxy/_unavailable/ sentinel when all pods are draining (fixes N-P0-2), got %+v", groups[1])
	}
	mu.Unlock()

	// 3. Reconcile XxlJobTask
	taskReconciler := &XxlJobTaskReconciler{Client: k8sClient, Scheme: scheme}
	if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-timeout-close"}}); err != nil {
		t.Fatalf("XxlJobTaskReconciler failed: %v", err)
	}

	var updatedTask xxljobv1alpha1.XxlJobTask
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-timeout-close"}, &updatedTask)
	if !updatedTask.Status.Synced || updatedTask.Status.JobID != 500 || updatedTask.Status.TriggerStatus != 1 {
		t.Fatalf("expected XxlJobTask synced with JobID=500 and TriggerStatus=1, got %+v", updatedTask.Status)
	}

	// 4. Test K8sJobCallbackReconciler retry when Admin callback fails (N-P0-4), SERIAL queue advancement (P1-1),
	// and callback + queue advancement when a running Job is deleted (R4-P1-2)!
	completedBatchJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "xxljob-exec-500-7777",
			Namespace:  "default",
			Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
			Labels: map[string]string{
				proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
				proxyexecutor.LabelExecutorName: "order-executor",
				proxyexecutor.LabelJobID:        "500",
				proxyexecutor.LabelLogID:        "7777",
			},
			Annotations: map[string]string{
				proxyexecutor.AnnotationTaskNamespace: "default",
				proxyexecutor.AnnotationTaskName:      "order-timeout-close",
				proxyexecutor.AnnotationLogID:         "7777",
				proxyexecutor.AnnotationLogDateTime:   "1700000000000",
				proxyexecutor.AnnotationCallbackSent:  "false",
			},
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		},
	}
	suspendTrue := true
	queuedBatchJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "xxljob-exec-500-7778",
			Namespace:  "default",
			Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
			Labels: map[string]string{
				proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
				proxyexecutor.LabelExecutorName: "order-executor",
				proxyexecutor.LabelJobID:        "500",
				proxyexecutor.LabelLogID:        "7778",
			},
			Annotations: map[string]string{
				proxyexecutor.AnnotationTaskNamespace: "default",
				proxyexecutor.AnnotationTaskName:      "order-timeout-close",
				proxyexecutor.AnnotationLogID:         "7778",
				proxyexecutor.AnnotationLogDateTime:   "1700000005000",
				proxyexecutor.AnnotationCallbackSent:  "false",
			},
		},
		Spec: batchv1.JobSpec{
			Suspend: &suspendTrue,
		},
	}
	queuedBatchJob2 := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "xxljob-exec-500-7779",
			Namespace:  "default",
			Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
			Labels: map[string]string{
				proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
				proxyexecutor.LabelExecutorName: "order-executor",
				proxyexecutor.LabelJobID:        "500",
				proxyexecutor.LabelLogID:        "7779",
			},
			Annotations: map[string]string{
				proxyexecutor.AnnotationTaskNamespace: "default",
				proxyexecutor.AnnotationTaskName:      "order-timeout-close",
				proxyexecutor.AnnotationLogID:         "7779",
				proxyexecutor.AnnotationLogDateTime:   "1700000010000",
				proxyexecutor.AnnotationCallbackSent:  "false",
			},
		},
		Spec: batchv1.JobSpec{
			Suspend: &suspendTrue,
		},
	}
	_ = k8sClient.Create(ctx, completedBatchJob)
	_ = k8sClient.Create(ctx, queuedBatchJob)
	_ = k8sClient.Create(ctx, queuedBatchJob2)

	// 4a. Simulate Admin callback failure: callback-sent MUST remain "false" so retry succeeds later (fixes N-P0-4)
	failCallback.Store(true)
	jobCbReconciler := &K8sJobCallbackReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme}
	res, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7777"}})
	if err != nil || res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter when Admin callback fails, got res=%+v err=%v", res, err)
	}
	var afterFailJob batchv1.Job
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7777"}, &afterFailJob)
	if afterFailJob.Annotations[proxyexecutor.AnnotationCallbackSent] == "true" {
		t.Fatalf("N-P0-4 regression: Job marked callback-sent=true despite Admin callback failure!")
	}
	// Verify queuedBatchJob (7778) was unsuspended while 7779 stayed suspended (fixes P1-1 & R4-P1-1)
	var afterAdvanceJob batchv1.Job
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7778"}, &afterAdvanceJob)
	if afterAdvanceJob.Spec.Suspend != nil && *afterAdvanceJob.Spec.Suspend {
		t.Fatalf("expected queued SERIAL_EXECUTION Job 7778 to be unsuspended after 7777 completed")
	}
	var check7779 batchv1.Job
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7779"}, &check7779)
	if check7779.Spec.Suspend == nil || !*check7779.Spec.Suspend {
		t.Fatalf("expected Job 7779 to remain suspended while 7778 is running")
	}

	// 4b. Admin recovers -> Reconcile 7777 retries and succeeds, marking callback-sent="true" and removing finalizer
	failCallback.Store(false)
	if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7777"}}); err != nil {
		t.Fatalf("K8sJobCallbackReconciler retry failed: %v", err)
	}
	mu.Lock()
	if len(*callbacks) != 1 || (*callbacks)[0].LogID != 7777 || (*callbacks)[0].HandleCode != 200 {
		t.Fatalf("expected K8sJobCallbackReconciler to deliver callback for logId=7777 on retry, got %+v", *callbacks)
	}
	mu.Unlock()

	var updatedBatchJob batchv1.Job
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7777"}, &updatedBatchJob)
	if updatedBatchJob.Annotations[proxyexecutor.AnnotationCallbackSent] != "true" ||
		controllerutil.ContainsFinalizer(&updatedBatchJob, proxyexecutor.JobCallbackFinalizer) {
		t.Fatalf("expected AnnotationCallbackSent=true and finalizer removed on Job 7777 after successful callback")
	}

	// 4c. Test R4-P1-2: Delete running Job 7778 before completion (e.g. /kill or kubectl delete) ->
	// Reconciler MUST send failure callback (HandleCode=500) for 7778, unsuspend queued Job 7779, and remove finalizer!
	if err := k8sClient.Delete(ctx, &afterAdvanceJob); err != nil {
		t.Fatalf("delete running job 7778 failed: %v", err)
	}
	if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7778"}}); err != nil {
		t.Fatalf("reconcile deleting job 7778 failed: %v", err)
	}
	mu.Lock()
	if len(*callbacks) != 2 || (*callbacks)[1].LogID != 7778 || (*callbacks)[1].HandleCode != 500 {
		t.Fatalf("expected failure callback (HandleCode=500) for deleted running Job 7778 (fixes R4-P1-2), got %+v", *callbacks)
	}
	mu.Unlock()
	_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7779"}, &check7779)
	if check7779.Spec.Suspend != nil && *check7779.Spec.Suspend {
		t.Fatalf("R4-P1-2 regression: expected queued Job 7779 to be unsuspended after running Job 7778 was deleted!")
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-500-7778"}, &afterAdvanceJob); !apierrors.IsNotFound(err) {
		t.Fatalf("expected deleted Job 7778 to have finalizer removed and be garbage-collected, got err=%v", err)
	}

	// 5. Test Deletion Order Protection AND R4-P1-3 (10-minute timeout MUST NOT bypass dependent task check!)
	if err := k8sClient.Delete(ctx, &updatedExec); err != nil {
		t.Fatalf("delete execCR failed: %v", err)
	}
	// Backdate DeletionTimestamp by 15 minutes via Get interceptor to ensure Reconcile observes >10m elapsed while Task still exists
	past15m := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	backdatedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
		WithObjects(&xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "order-executor",
				Namespace:         "default",
				DeletionTimestamp: &past15m,
				Finalizers:        []string{ExecutorFinalizer},
			},
			Spec:   updatedExec.Spec,
			Status: updatedExec.Status,
		}, &updatedTask).
		Build()
	backdatedExecReconciler := &XxlJobExecutorReconciler{Client: backdatedClient, APIReader: backdatedClient, Scheme: scheme}
	var observeBackdated xxljobv1alpha1.XxlJobExecutor
	if err := backdatedClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-executor"}, &observeBackdated); err != nil ||
		observeBackdated.DeletionTimestamp == nil || time.Since(observeBackdated.DeletionTimestamp.Time) < 14*time.Minute {
		t.Fatalf("expected backdated DeletionTimestamp >14m on order-executor, got %+v err=%v", observeBackdated.DeletionTimestamp, err)
	}
	if _, err := backdatedExecReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("reconcile backdated deleting execCR failed: %v", err)
	}
	var stillExistingExec xxljobv1alpha1.XxlJobExecutor
	if err := backdatedClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-executor"}, &stillExistingExec); err != nil {
		t.Fatalf("R4-P1-3 regression: expected XxlJobExecutor to remain while dependent XxlJobTask exists even after 15m: %v", err)
	}
	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("reconcile deleting execCR failed: %v", err)
	}

	// Delete XxlJobTask first -> removes job 500 from Admin and deletes Task CR
	if err := k8sClient.Delete(ctx, &updatedTask); err != nil {
		t.Fatalf("delete taskCR failed: %v", err)
	}
	if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-timeout-close"}}); err != nil {
		t.Fatalf("reconcile deleting taskCR failed: %v", err)
	}
	mu.Lock()
	if _, exists := jobs[500]; exists {
		t.Fatalf("expected job 500 to be removed from Admin after XxlJobTask deletion")
	}
	mu.Unlock()

	// Now reconcile XxlJobExecutor again -> dependent tasks are 0, so it removes group from Admin and finishes deletion
	if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "order-executor"}}); err != nil {
		t.Fatalf("reconcile deleting execCR after task removal failed: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "order-executor"}, &stillExistingExec); !apierrors.IsNotFound(err) {
		t.Fatalf("expected XxlJobExecutor to be completely deleted after dependent tasks were removed, got err=%v", err)
	}

	// 6. Test P1-2: Deleting an entire Namespace when Admin is unreachable must NOT deadlock finalizers
	now := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	deadTask := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "stuck-task",
			Namespace:  "terminating-ns",
			Finalizers: []string{TaskFinalizer},
		},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "missing-exec"},
			JobDesc:         "Task in terminating namespace",
			Author:          "dev",
			ScheduleType:    "CRON",
			ExecutorHandler: "h",
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 999},
	}
	terminatingNS := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "terminating-ns",
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes"},
		},
	}
	_ = k8sClient.Create(ctx, terminatingNS)
	_ = k8sClient.Create(ctx, deadTask)
	_ = k8sClient.Delete(ctx, deadTask)

	if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "terminating-ns", Name: "stuck-task"}}); err != nil {
		t.Fatalf("reconcile task in terminating namespace failed: %v", err)
	}
	var checkDeadTask xxljobv1alpha1.XxlJobTask
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "terminating-ns", Name: "stuck-task"}, &checkDeadTask); !apierrors.IsNotFound(err) {
		t.Fatalf("expected TaskFinalizer to be released when namespace is terminating (fixes P1-2), got err=%v", err)
	}

	// 7. Test R5-P2-2 & R6-P1-6: FailedCreate Warning Event on unsuspended Job (with requeue when Event arrives late)
	t.Run("FailedCreateWarningEventCallbackAndQueueAdvance", func(t *testing.T) {
		failedCreateJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-501-8801",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "501",
					proxyexecutor.LabelLogID:        "8801",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "deleted-task",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationLogID:         "8801",
					proxyexecutor.AnnotationLogDateTime:   "1700000020000",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
		}
		queuedAfterFailedCreate := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-501-8802",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "501",
					proxyexecutor.LabelLogID:        "8802",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "deleted-task",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationLogID:         "8802",
					proxyexecutor.AnnotationLogDateTime:   "1700000025000",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspendTrue,
			},
		}
		_ = k8sClient.Create(ctx, failedCreateJob)
		_ = k8sClient.Create(ctx, queuedAfterFailedCreate)

		// First reconcile BEFORE FailedCreate Warning Event arrives: must return RequeueAfter=15s (fixes R6-P1-6)
		preRes, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-8801"}})
		if err != nil || preRes.RequeueAfter != 15*time.Second {
			t.Fatalf("R6-P1-6: expected RequeueAfter=15s while unsuspended Job has 0 active pods and no Event yet, got res=%+v err=%v", preRes, err)
		}

		quotaEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "quota-exceeded-event",
				Namespace: "default",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Job",
				Name: "xxljob-exec-501-8801",
			},
			Type:    corev1.EventTypeWarning,
			Reason:  "FailedCreate",
			Count:   2,
			Message: "pods \"xxljob-exec-501-8801-abc\" is forbidden: exceeded quota",
		}
		_ = k8sClient.Create(ctx, quotaEvent)

		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-8801"}}); err != nil {
			t.Fatalf("reconcile FailedCreate job failed: %v", err)
		}
		mu.Lock()
		found8801 := false
		for _, cb := range *callbacks {
			if cb.LogID == 8801 && cb.HandleCode == 500 && strings.Contains(cb.HandleMsg, "exceeded quota") {
				found8801 = true
			}
		}
		mu.Unlock()
		if !found8801 {
			t.Fatalf("R5-P2-2: expected 500 callback with quota error for Job 8801, got %+v", *callbacks)
		}
		var check8802 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-8802"}, &check8802)
		if check8802.Spec.Suspend != nil && *check8802.Spec.Suspend {
			t.Fatalf("R5-P2-2: expected queued Job 8802 to be unsuspended after 8801 failed with FailedCreate")
		}
		// Mark 8802 failed with Reason=DeadlineExceeded and verify callback sends HandleCode=502 (fixes R6-P2-5)
		check8802.Status.Failed = 1
		check8802.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline"},
		}
		_ = k8sClient.Status().Update(ctx, &check8802)
		_, _ = jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-8802"}})
		mu.Lock()
		found8802Timeout := false
		for _, cb := range *callbacks {
			if cb.LogID == 8802 && cb.HandleCode == xxlclient.TimeoutCode {
				found8802Timeout = true
			}
		}
		mu.Unlock()
		if !found8802Timeout {
			t.Fatalf("R6-P2-5: expected HandleCode=502 (TimeoutCode) for DeadlineExceeded Job 8802, got %+v", *callbacks)
		}
		_ = k8sClient.Delete(ctx, &check8802)
		_, _ = jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-8802"}})
	})

	// 8. Test R5-P2-5: Deleting XxlJobTask terminates active K8s Jobs and still delivers callback via AnnotationExecutorName
	t.Run("DeleteTaskCleansUpActiveJobsAndDeliversCallback", func(t *testing.T) {
		taskWithRunningJob := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "cleanup-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				JobDesc:         "Cleanup Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "cleanupHandler",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 601, Synced: true},
		}
		activeJobForTask := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-601-9901",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "601",
					proxyexecutor.LabelLogID:        "9901",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "cleanup-task",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationLogID:         "9901",
					proxyexecutor.AnnotationLogDateTime:   "1700000030000",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
		}
		_ = k8sClient.Create(ctx, taskWithRunningJob)
		_ = k8sClient.Create(ctx, activeJobForTask)
		_ = k8sClient.Delete(ctx, taskWithRunningJob)

		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "cleanup-task"}}); err != nil {
			t.Fatalf("reconcile deleting cleanup-task failed: %v", err)
		}
		// Now reconcile the terminated Job 9901 AFTER cleanup-task CR is gone
		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-601-9901"}}); err != nil {
			t.Fatalf("reconcile terminated job 9901 failed: %v", err)
		}
		mu.Lock()
		found9901 := false
		for _, cb := range *callbacks {
			if cb.LogID == 9901 && cb.HandleCode == 500 {
				found9901 = true
			}
		}
		mu.Unlock()
		if !found9901 {
			t.Fatalf("R5-P2-5: expected 500 callback for active Job 9901 terminated when XxlJobTask was deleted, got %+v", *callbacks)
		}
	})

	// 9. Test R5-P1-2 & R6-P1-10: Concurrent RegistryHeartbeatRunner across 50 executors with real 15ms per-request Admin latency
	// (50 * 15ms = 750ms if sequential, but < 400ms with MaxConcurrency=16)
	t.Run("ConcurrentRegistryHeartbeatRunnerPerformance", func(t *testing.T) {
		var perfMu sync.Mutex
		perfRegistries := make(map[string]map[string]bool)
		perfMux := http.NewServeMux()
		perfMux.HandleFunc("/api/registry", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(15 * time.Millisecond)
			var p xxlclient.RegistryParam
			_ = json.NewDecoder(r.Body).Decode(&p)
			perfMu.Lock()
			if perfRegistries[p.RegistryKey] == nil {
				perfRegistries[p.RegistryKey] = make(map[string]bool)
			}
			perfRegistries[p.RegistryKey][p.RegistryValue] = true
			perfMu.Unlock()
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		perfSrv := httptest.NewServer(perfMux)
		defer perfSrv.Close()

		perfAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "perf-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: perfSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "perf_token"},
			},
		}
		_ = k8sClient.Create(ctx, perfAdmin)

		for i := 0; i < 50; i++ {
			name := "perf-exec-" + strconv.Itoa(i)
			ex := &xxljobv1alpha1.XxlJobExecutor{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: xxljobv1alpha1.XxlJobExecutorSpec{
					AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "perf-admin"},
					AppName:  "perf-app-" + strconv.Itoa(i),
					Title:    "Perf" + strconv.Itoa(i),
					Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
				},
				Status: xxljobv1alpha1.XxlJobExecutorStatus{
					Synced:              true,
					JobGroupID:          100 + i,
					RegisteredAddresses: []string{"http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/proxy/default/" + name + "/"},
				},
			}
			_ = k8sClient.Create(ctx, ex)
		}
		runner := &RegistryHeartbeatRunner{
			Client:         k8sClient,
			APIReader:      k8sClient,
			MaxConcurrency: 16,
		}
		start := time.Now()
		if err := runner.RunOnce(ctx); err != nil {
			t.Fatalf("RegistryHeartbeatRunner.RunOnce failed: %v", err)
		}
		elapsed := time.Since(start)
		// Sequential execution of 50 * 15ms would take >= 750ms; concurrent execution with MaxConcurrency=16 takes ~4 batches (~60-150ms)
		if elapsed >= 500*time.Millisecond {
			t.Fatalf("R5-P1-2: expected concurrent RegistryHeartbeatRunner across 50*15ms executors to finish < 500ms, took %v", elapsed)
		}
		perfMu.Lock()
		if !perfRegistries["perf-app-49"]["http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/proxy/default/perf-exec-49/"] {
			t.Fatalf("R5-P1-2: expected perf-exec-49 to be registered by RegistryHeartbeatRunner")
		}
		perfMu.Unlock()
	})

	// 10. Test R6-P1-5 (RemoveJobGroup idempotency vs active manual tasks), R6-P1-8 (ResolveAdminURL port update), and R6-P1-9 (Secret rotation hash + grace window)
	t.Run("RemoveJobGroupIdempotencyAndResolveAdminURLAndTokenRotation", func(t *testing.T) {
		// (a) R6-P1-8: ResolveAdminURL must reflect Spec.Port changes immediately even when Status.AdminURL is set
		portAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "internal-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Port:        8080,
				ContextPath: "/xxl-job-admin",
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{
				AdminURL: "http://internal-admin.default.svc.cluster.local:8080/xxl-job-admin",
			},
		}
		portAdmin.Spec.Port = 9090
		if gotURL := ResolveAdminURL(portAdmin); gotURL != "http://internal-admin.default.svc.cluster.local:9090/xxl-job-admin" {
			t.Fatalf("R6-P1-8: expected ResolveAdminURL to reflect updated Spec.Port=9090, got %s", gotURL)
		}

		// (b) R6-P1-5: Deleting an Executor whose JobGroup was already removed in Admin succeeds idempotently
		alreadyGoneExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "already-gone-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
				AppName:  "already-gone-app",
				Title:    "GoneExec",
				Mode:     xxljobv1alpha1.ExecutorModeResident,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 8888, // Does not exist in groups map -> /jobgroup/remove returns 500
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, alreadyGoneExec)
		_ = k8sClient.Delete(ctx, alreadyGoneExec)
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "already-gone-exec"}}); err != nil {
			t.Fatalf("reconcile already-gone-exec failed: %v", err)
		}
		var checkGone xxljobv1alpha1.XxlJobExecutor
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "already-gone-exec"}, &checkGone); !apierrors.IsNotFound(err) {
			t.Fatalf("R6-P1-5: expected ExecutorFinalizer to be released when JobGroup was already deleted in Admin, got %v", err)
		}

		// Conversely, when the JobGroup still exists in Admin and has a manual task (causing /jobgroup/remove to return 500),
		// ExecutorFinalizer must NOT be bypassed without force-delete
		mu.Lock()
		groups[77] = xxlclient.JobGroup{ID: 77, AppName: "busy-group-app", Title: "BusyGroup", AddressType: 1}
		jobs[7701] = xxlclient.JobInfo{ID: 7701, JobGroup: 77, ExecutorHandler: "manualTask"}
		mu.Unlock()

		busyExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "busy-group-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
				AppName:  "busy-group-app",
				Title:    "BusyGroup",
				Mode:     xxljobv1alpha1.ExecutorModeResident,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 77,
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, busyExec)
		_ = k8sClient.Delete(ctx, busyExec)
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "busy-group-exec"}})
		var checkBusy xxljobv1alpha1.XxlJobExecutor
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "busy-group-exec"}, &checkBusy); err != nil || !controllerutil.ContainsFinalizer(&checkBusy, ExecutorFinalizer) {
			t.Fatalf("R5-P2-6 / R6-P1-5: expected ExecutorFinalizer to be retained when JobGroup still has manual tasks in Admin")
		}

		// (c) R6-P1-9: Secret rotation updates Deployment credentials-hash annotation & preserves old token in grace window
		rotSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-admin-secret", Namespace: "default"},
			Data: map[string][]byte{
				"token": []byte("token-v1"),
			},
		}
		rotAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image: "xuxueli/xxl-job-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					Password: "pwd",
				},
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: "rot-admin-secret", Key: "token"},
				},
			},
		}
		rotExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "rot-admin"},
				AppName:  "rot-exec-app",
				Title:    "RotExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, rotSecret)
		_ = k8sClient.Create(ctx, rotAdmin)
		_ = k8sClient.Create(ctx, rotExec)

		_, _ = adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "rot-admin"}})
		_, _ = adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "rot-admin"}})
		var rotDeploy1 appsv1.Deployment
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "rot-admin"}, &rotDeploy1)
		hash1 := rotDeploy1.Spec.Template.Annotations[AnnotationAdminCredentialsHash]
		if hash1 == "" {
			t.Fatalf("R6-P1-9: expected %s annotation on Admin Deployment template", AnnotationAdminCredentialsHash)
		}

		// Validate v1 token before rotation
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "rot-exec", "token-v1"); err != nil {
			t.Fatalf("expected token-v1 to validate: %v", err)
		}

		// Rotate Secret to token-v2
		rotSecret.Data["token"] = []byte("token-v2")
		_ = k8sClient.Update(ctx, rotSecret)
		_, _ = adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "rot-admin"}})
		var rotDeploy2 appsv1.Deployment
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "rot-admin"}, &rotDeploy2)
		hash2 := rotDeploy2.Spec.Template.Annotations[AnnotationAdminCredentialsHash]
		if hash2 == "" || hash2 == hash1 {
			t.Fatalf("R6-P1-9: expected Deployment credentials-hash annotation to change after Secret rotation (%q vs %q)", hash1, hash2)
		}

		// Both token-v2 (new) and token-v1 (in 10m grace period) must validate!
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "rot-exec", "token-v2"); err != nil {
			t.Fatalf("R6-P1-9: expected new token-v2 to validate immediately on rotation: %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "rot-exec", "token-v1"); err != nil {
			t.Fatalf("R6-P1-9: expected previous token-v1 to remain valid during rotation grace period: %v", err)
		}
	})

	// 11. Test Round 7 Fixes:
	// - R7-P0-1: TransformPodForCache + isPodInjectedWhenRequired (Resident & Sidecar)
	// - R7-P1-3: Orphaned suspended non-SERIAL Job cleanup & callback after 30s
	// - R7-P1-4: Cross-Admin XxlJobTask deletion guard (must not delete unowned job in new Admin)
	// - R7-P1-5: EnsureJobGroup failure still persists shrunk RegisteredAddresses & DrainedAddresses
	t.Run("Round7ControllerFixes", func(t *testing.T) {
		// (a) R7-P0-1: TransformPodForCache + isPodInjectedWhenRequired
		residentInjectedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "resident-injected-pod",
				Namespace:   "default",
				Labels:      map[string]string{"app": "res-app", "xxljob.io/inject": "true"},
				Annotations: map[string]string{"xxljob.io/injected": "true", "xxljob.io/mode": "Resident", "heavy-anno": "strip-me"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.244.2.10",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		transformedObj, err := TransformPodForCache(residentInjectedPod.DeepCopy())
		if err != nil {
			t.Fatalf("TransformPodForCache failed: %v", err)
		}
		transformedResPod := transformedObj.(*corev1.Pod)
		if !isPodInjectedWhenRequired(transformedResPod, xxljobv1alpha1.ExecutorModeResident) {
			t.Fatalf("R7-P0-1 regression: Resident pod with xxljob.io/inject=true rejected after TransformPodForCache!")
		}
		if _, exists := transformedResPod.Annotations["heavy-anno"]; exists {
			t.Fatalf("expected TransformPodForCache to strip unrelated annotations")
		}

		sidecarPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "sidecar-injected-pod",
				Namespace:   "default",
				Labels:      map[string]string{"app": "sidecar-app", "xxljob.io/inject": "true"},
				Annotations: map[string]string{"xxljob.io/injected": "true", "xxljob.io/mode": "Sidecar"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "busybox:1.36"},
					{Name: "xxl-job-sidecar", Image: "sidecar:latest"},
				},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.244.2.11",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		transformedSidecarObj, err := TransformPodForCache(sidecarPod.DeepCopy())
		if err != nil {
			t.Fatalf("TransformPodForCache failed on sidecarPod: %v", err)
		}
		transformedSidecarPod := transformedSidecarObj.(*corev1.Pod)
		if !isPodInjectedWhenRequired(transformedSidecarPod, xxljobv1alpha1.ExecutorModeSidecar) {
			t.Fatalf("R7-P0-1 regression: Sidecar pod with xxl-job-sidecar container rejected after TransformPodForCache!")
		}

		uninjectedSidecarPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "uninjected-sidecar-pod",
				Namespace: "default",
				Labels:    map[string]string{"app": "sidecar-app"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox:1.36"}},
			},
		}
		transformedUninjObj, _ := TransformPodForCache(uninjectedSidecarPod)
		if isPodInjectedWhenRequired(transformedUninjObj.(*corev1.Pod), xxljobv1alpha1.ExecutorModeSidecar) {
			t.Fatalf("expected uninjected Pod without sidecar container to be rejected in Sidecar mode")
		}

		// (b) R7-P1-3: Orphaned suspended non-SERIAL Job older than 30s is failed with callback and cleaned up
		orphanSuspendedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "xxljob-exec-501-9950",
				Namespace:         "default",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-45 * time.Second)),
				Finalizers:        []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "501",
					proxyexecutor.LabelLogID:        "9950",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "deleted-task",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationBlockStrategy: "DISCARD_LATER",
					proxyexecutor.AnnotationLogID:         "9950",
					proxyexecutor.AnnotationLogDateTime:   "1700000040000",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspendTrue,
			},
		}
		if err := k8sClient.Create(ctx, orphanSuspendedJob); err != nil {
			t.Fatalf("create orphanSuspendedJob failed: %v", err)
		}
		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "xxljob-exec-501-9950"}}); err != nil {
			t.Fatalf("reconcile orphanSuspendedJob failed: %v", err)
		}
		mu.Lock()
		found9950 := false
		for _, cb := range *callbacks {
			if cb.LogID == 9950 && cb.HandleCode == 500 && strings.Contains(cb.HandleMsg, "remained suspended after interrupted") {
				found9950 = true
			}
		}
		mu.Unlock()
		if !found9950 {
			t.Fatalf("R7-P1-3: expected 500 callback for orphaned suspended non-SERIAL Job 9950, got %+v", *callbacks)
		}

		// (c) R7-P1-4: Deleting XxlJobTask after Executor switched to a different Admin must NOT delete unowned job with same ID
		mu.Lock()
		jobs[789] = xxlclient.JobInfo{
			ID:              789,
			JobGroup:        2,
			JobDesc:         "Unrelated Manual Job In New Admin",
			ExecutorHandler: "unrelatedHandler",
		}
		mu.Unlock()
		switchedAdminTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "switched-admin-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				JobDesc:         "Switched Admin Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "switchedHandler",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      789,
				JobGroupID: 2,
				AdminURL:   "http://old-retired-admin.default.svc.cluster.local:8080/xxl-job-admin",
				Synced:     true,
			},
		}
		if err := k8sClient.Create(ctx, switchedAdminTask); err != nil {
			t.Fatalf("create switchedAdminTask failed: %v", err)
		}
		if err := k8sClient.Delete(ctx, switchedAdminTask); err != nil {
			t.Fatalf("delete switchedAdminTask failed: %v", err)
		}
		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "switched-admin-task"}}); err != nil {
			t.Fatalf("reconcile deleting switchedAdminTask failed: %v", err)
		}
		mu.Lock()
		_, job789StillExists := jobs[789]
		mu.Unlock()
		if !job789StillExists {
			t.Fatalf("R7-P1-4 regression: deleting XxlJobTask after Admin switch deleted unrelated job 789 in new Admin!")
		}

		// (d) R7-P1-5: When EnsureJobGroup fails repeatedly with the same error while Pod addresses change,
		// updateErrorStatusWithAddresses MUST persist the updated RegisteredAddresses and DrainedAddresses.
		invalidTitleExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "err-persist-exec",
				Namespace: "default",
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
				AppName:  "err-persist-app",
				Title:    "TitleWayTooLongForAdminValidationMoreThan12Runes",
				Mode:     xxljobv1alpha1.ExecutorModeResident,
				StaticAddresses: []string{
					"http://10.244.9.1:9999/",
					"http://10.244.9.2:9999/",
				},
			},
		}
		if err := k8sClient.Create(ctx, invalidTitleExec); err != nil {
			t.Fatalf("create invalidTitleExec failed: %v", err)
		}
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "err-persist-exec"}})
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "err-persist-exec"}})
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "err-persist-exec"}, invalidTitleExec)
		if len(invalidTitleExec.Status.RegisteredAddresses) != 2 {
			t.Fatalf("expected 2 RegisteredAddresses on initial EnsureJobGroup error, got %+v", invalidTitleExec.Status.RegisteredAddresses)
		}
		// Now shrink StaticAddresses from 2 to 1 while Title is still invalid (same EnsureJobGroup error message!)
		invalidTitleExec.Spec.StaticAddresses = []string{"http://10.244.9.1:9999/"}
		if err := k8sClient.Update(ctx, invalidTitleExec); err != nil {
			t.Fatalf("update invalidTitleExec failed: %v", err)
		}
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "err-persist-exec"}})
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "err-persist-exec"}, invalidTitleExec)
		if len(invalidTitleExec.Status.RegisteredAddresses) != 1 || invalidTitleExec.Status.ActivePods != 1 {
			t.Fatalf("R7-P1-5 regression: expected RegisteredAddresses to shrink to 1 even when EnsureJobGroup fails with identical error, got %+v", invalidTitleExec.Status)
		}
	})

	// 12. Test Round 8 Fixes:
	// - R8-P1-1: Deleting SERIAL_EXECUTION Job with Active=1 retains JobCallbackFinalizer until Pod finishes terminating, then advances queue
	// - R8-P1-5: RegistryHeartbeatRunner skips UseServiceDNS/StaticAddresses under addressType=1
	// - R8-P1-6: ValidateExecutorAccessToken rejects empty/"default_token" without APIServer reads and respects cooldown on cache miss
	// - R8-P1-8: Cross-Admin XxlJobTask migration cleans up old task on previous Admin CR
	t.Run("Round8ControllerFixes", func(t *testing.T) {
		// (a) R8-P1-1: Deleting running SERIAL_EXECUTION Job1 (Status.Active=1) while Job2 is queued (suspend=true)
		suspFalse := false
		serialDelJob1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-601-10001",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "601",
					proxyexecutor.LabelLogID:        "10001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "10001",
					proxyexecutor.AnnotationLogDateTime:   "1700000100001",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspFalse,
			},
			Status: batchv1.JobStatus{
				Active: 1,
			},
		}
		serialQueuedJob2 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-601-10002",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "601",
					proxyexecutor.LabelLogID:        "10002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "10002",
					proxyexecutor.AnnotationLogDateTime:   "1700000100002",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspendTrue,
			},
		}
		_ = k8sClient.Create(ctx, serialDelJob1)
		_ = k8sClient.Status().Update(ctx, serialDelJob1)
		_ = k8sClient.Create(ctx, serialQueuedJob2)

		// Delete Job1 while Active=1
		_ = k8sClient.Delete(ctx, serialDelJob1)
		resDel1, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: serialDelJob1.Name}})
		if err != nil || resDel1.RequeueAfter == 0 {
			t.Fatalf("R8-P1-1: expected RequeueAfter > 0 while deleting Job1 still has Active=1, got res=%+v err=%v", resDel1, err)
		}
		// Verify Job1 still exists in etcd (JobCallbackFinalizer retained) and Job2 is still suspended
		var checkJob1 batchv1.Job
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: serialDelJob1.Name}, &checkJob1); err != nil {
			t.Fatalf("R8-P1-1 regression: Job1 was GC'd prematurely while Active=1: %v", err)
		}
		var checkJob2 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: serialQueuedJob2.Name}, &checkJob2)
		if checkJob2.Spec.Suspend == nil || !*checkJob2.Spec.Suspend {
			t.Fatalf("R8-P1-1: expected Job2 to remain suspended while Job1 Pod is still active")
		}

		// Now Job1's Pod finishes terminating (Active=0)
		checkJob1.Status.Active = 0
		_ = k8sClient.Status().Update(ctx, &checkJob1)
		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: serialDelJob1.Name}}); err != nil {
			t.Fatalf("R8-P1-1: second reconcile after Active=0 failed: %v", err)
		}
		// Verify Job2 is now unsuspended and Job1 is deleted!
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: serialQueuedJob2.Name}, &checkJob2)
		if checkJob2.Spec.Suspend != nil && *checkJob2.Spec.Suspend {
			t.Fatalf("R8-P1-1 regression: Job2 remained permanently suspended after deleting Job1's Pod exited!")
		}

		// (b) R8-P1-5: shouldHeartbeatExecutorRegistry returns false for UseServiceDNS / StaticAddresses under addressType=1
		dnsExec := &xxljobv1alpha1.XxlJobExecutor{
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				Mode:          xxljobv1alpha1.ExecutorModeResident,
				UseServiceDNS: true,
				ServiceName:   "my-svc",
			},
		}
		if shouldHeartbeatExecutorRegistry(dnsExec) {
			t.Fatalf("R8-P1-5 regression: expected shouldHeartbeatExecutorRegistry=false for UseServiceDNS under addressType=1")
		}

		// (c) R8-P1-6: ValidateExecutorAccessToken rejects empty and default_token immediately
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "rot-exec", ""); err == nil {
			t.Fatalf("R8-P1-6 regression: expected empty token to be rejected immediately")
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "rot-exec", "default_token"); err == nil {
			t.Fatalf("R8-P1-6 regression: expected default_token to be rejected immediately")
		}

		// (d) R8-P1-8: Cross-Admin XxlJobTask migration cleans up old task on previous Admin CR
		adminSrvB, _, _, _, jobsB, _ := setupTestAdminServer(t, &failCallback)
		adminB := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "admin-b", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: adminSrvB.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		execB := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "exec-b", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:        xxljobv1alpha1.LocalObjectReference{Name: "admin-b"},
				AppName:         "exec-b-app",
				Title:           "ExecB",
				Mode:            xxljobv1alpha1.ExecutorModeResident,
				StaticAddresses: []string{"http://10.244.8.8:9999/"},
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 3,
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, adminB)
		_ = k8sClient.Create(ctx, execB)
		_ = k8sClient.Status().Update(ctx, execB)

		// Create an owned job 888 on the primary admin (xxl-job-admin, URL = adminSrv.URL)
		mu.Lock()
		jobs[888] = xxlclient.JobInfo{
			ID:              888,
			JobGroup:        2,
			JobDesc:         xxlclient.FormatOwnedJobDesc("Cross Admin Migrating Task", "default/migrating-task"),
			ExecutorHandler: "migHandler",
			TriggerStatus:   1,
		}
		mu.Unlock()

		migTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "migrating-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "exec-b"},
				JobDesc:         "Cross Admin Migrating Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 0 * * * ?",
				ExecutorHandler: "migHandler",
				TriggerStatus:   true,
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      888,
				JobGroupID: 2,
				AdminURL:   adminSrv.URL,
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, migTask)
		_ = k8sClient.Status().Update(ctx, migTask)

		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "migrating-task"}}); err != nil {
			t.Fatalf("reconcile migrating-task failed: %v", err)
		}
		mu.Lock()
		_, oldJob888Exists := jobs[888]
		mu.Unlock()
		if oldJob888Exists || len(jobsB) == 0 {
			t.Fatalf("R8-P1-8 regression: expected old job 888 on previous Admin to be removed and created on admin-b, got oldExists=%v newCount=%d", oldJob888Exists, len(jobsB))
		}
	})

	// 13. Test Round 9 Fixes:
	// - R9-P1-1: Non-deleted failed Job (DeadlineExceeded) with Terminating=1 sends 502 callback first, retains Finalizer, and wakes next SERIAL Job only after Terminating=0
	// - R9-P1-2: cleanupActiveTaskJobs cleans up active K8s Jobs across historical jobIds for the same AnnotationTaskName, and cleanupTaskOnPreviousAdmin falls back to OwnerKey
	// - R9-P1-5: shouldHeartbeatExecutorRegistry returns false when addressType=0
	// - R9-P2-1 & R9-P2-2: Negative token cache on missing Secret and computeCredentialsHash missing key validation
	t.Run("Round9ControllerFixes", func(t *testing.T) {
		// (a) R9-P1-1: DeadlineExceeded Job with Terminating=1 vs queued SERIAL_EXECUTION Job
		suspFalse := false
		termOne := int32(1)
		deadlineJob1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-701-11001",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "701",
					proxyexecutor.LabelLogID:        "11001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "11001",
					proxyexecutor.AnnotationLogDateTime:   "1700000110001",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspFalse,
			},
			Status: batchv1.JobStatus{
				Terminating: &termOne,
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
						Reason: "DeadlineExceeded",
					},
				},
			},
		}
		deadlineJob2 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-exec-701-11002",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "701",
					proxyexecutor.LabelLogID:        "11002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "k8s-proxy-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "11002",
					proxyexecutor.AnnotationLogDateTime:   "1700000110002",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspendTrue,
			},
		}
		_ = k8sClient.Create(ctx, deadlineJob1)
		_ = k8sClient.Status().Update(ctx, deadlineJob1)
		_ = k8sClient.Create(ctx, deadlineJob2)

		resDL, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: deadlineJob1.Name}})
		if err != nil || resDL.RequeueAfter == 0 {
			t.Fatalf("R9-P1-1: expected RequeueAfter > 0 while DeadlineExceeded Job1 still has Terminating=1, got res=%+v err=%v", resDL, err)
		}
		var checkDL1, checkDL2 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: deadlineJob1.Name}, &checkDL1)
		if checkDL1.Annotations[proxyexecutor.AnnotationCallbackSent] != "true" || len(checkDL1.Finalizers) == 0 {
			t.Fatalf("R9-P1-1: expected callback-sent=true and JobCallbackFinalizer retained while Terminating=1, got annotations=%v finalizers=%v", checkDL1.Annotations, checkDL1.Finalizers)
		}
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: deadlineJob2.Name}, &checkDL2)
		if checkDL2.Spec.Suspend == nil || !*checkDL2.Spec.Suspend {
			t.Fatalf("R9-P1-1 regression: Job2 was unsuspended while timed-out Job1 still had Terminating=1")
		}

		// Now Terminating reaches 0 -> next Reconcile wakes Job2 and removes JobCallbackFinalizer on Job1
		termZero := int32(0)
		checkDL1.Status.Terminating = &termZero
		_ = k8sClient.Status().Update(ctx, &checkDL1)
		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: deadlineJob1.Name}}); err != nil {
			t.Fatalf("R9-P1-1: reconcile after Terminating=0 failed: %v", err)
		}
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: deadlineJob1.Name}, &checkDL1)
		if len(checkDL1.Finalizers) != 0 {
			t.Fatalf("R9-P1-1: expected JobCallbackFinalizer to be removed once Terminating=0, got %v", checkDL1.Finalizers)
		}
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: deadlineJob2.Name}, &checkDL2)
		if checkDL2.Spec.Suspend != nil && *checkDL2.Spec.Suspend {
			t.Fatalf("R9-P1-1 regression: expected Job2 to be unsuspended once Job1 Terminating=0")
		}

		// (b) R9-P1-2: Cross-Admin migration cleans up old Admin BEFORE creating on new Admin; and deleting XxlJobTask cleans up active K8s Jobs across historical jobIds
		// Create an unreachable old Admin CR (admin-c) and verify that migrating r9-mig-task from admin-c to xxl-job-admin (k8s-proxy-exec)
		// fails closed BEFORE creating the job on xxl-job-admin!
		closedSrvC := httptest.NewServer(http.NotFoundHandler())
		closedSrvC.Close()
		adminC := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "admin-c", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: closedSrvC.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		_ = k8sClient.Create(ctx, adminC)

		mu.Lock()
		jobsCountBeforeMig := len(jobs)
		mu.Unlock()

		r9MigTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "r9-mig-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				JobDesc:         "R9 Cross Admin Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 0 * * * ?",
				ExecutorHandler: "r9MigHandler",
				TriggerStatus:   true,
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      999,
				JobGroupID: 3,
				AdminURL:   closedSrvC.URL, // unreachable closed server
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, r9MigTask)
		_ = k8sClient.Status().Update(ctx, r9MigTask)

		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r9-mig-task"}}); err != nil {
			t.Fatalf("unexpected error from Reconcile on transient old-Admin failure: %v", err)
		}
		mu.Lock()
		jobsCountAfterFailedMig := len(jobs)
		mu.Unlock()
		if jobsCountAfterFailedMig != jobsCountBeforeMig {
			t.Fatalf("R9-P1-2 regression: EnsureJobInfo created task on new Admin before old Admin cleanup succeeded (%d -> %d)", jobsCountBeforeMig, jobsCountAfterFailedMig)
		}

		// Once old Admin CR (admin-c) is removed from namespace, migration to k8s-proxy-exec succeeds
		_ = k8sClient.Delete(ctx, adminC)
		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r9-mig-task"}}); err != nil {
			t.Fatalf("reconcile r9-mig-task after old Admin removal failed: %v", err)
		}

		oldIDActiveJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xxljob-exec-old999-11003",
				Namespace: "default",
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "k8s-proxy-exec",
					proxyexecutor.LabelJobID:        "999", // historical jobId before migration
					proxyexecutor.LabelLogID:        "11003",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "r9-mig-task",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspFalse,
			},
			Status: batchv1.JobStatus{
				Active: 1,
			},
		}
		_ = k8sClient.Create(ctx, oldIDActiveJob)
		var migTaskToDel xxljobv1alpha1.XxlJobTask
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r9-mig-task"}, &migTaskToDel)
		_ = k8sClient.Delete(ctx, &migTaskToDel)
		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r9-mig-task"}}); err != nil {
			t.Fatalf("R9-P1-2: reconcile deleting r9-mig-task failed: %v", err)
		}
		var checkOldIDJob batchv1.Job
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: oldIDActiveJob.Name}, &checkOldIDJob); !apierrors.IsNotFound(err) {
			t.Fatalf("R9-P1-2 regression: expected active Job under historical jobId=999 for r9-mig-task to be deleted on Task deletion, got err=%v", err)
		}

		// (c) R9-P1-5: shouldHeartbeatExecutorRegistry returns false when addressType=0
		addrTypeZero := 0
		autoRegPodSelectorExec := &xxljobv1alpha1.XxlJobExecutor{
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				Mode:        xxljobv1alpha1.ExecutorModeSidecar,
				AddressType: &addrTypeZero,
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "auto-sidecar"}},
			},
		}
		if shouldHeartbeatExecutorRegistry(autoRegPodSelectorExec) {
			t.Fatalf("R9-P1-5 regression: expected shouldHeartbeatExecutorRegistry=false when addressType=0")
		}

		// (d) R9-P2-2: computeCredentialsHash fails when Secret exists but lacks the referenced key
		badKeySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "incomplete-sec", Namespace: "default"},
			Data:       map[string][]byte{"other-key": []byte("val")},
		}
		_ = k8sClient.Create(ctx, badKeySecret)
		badRefAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-ref-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: "incomplete-sec", Key: "missing-token-key"},
				},
			},
		}
		if _, err := adminReconciler.computeCredentialsHash(ctx, badRefAdmin); err == nil {
			t.Fatalf("R9-P2-2 regression: expected computeCredentialsHash to fail when Secret key is missing")
		}
	})

	// 14. Test Round 10 Fixes:
	// - R10-P1-1: RegistryHeartbeatRunner skips unsynced/cross-Admin executors; XxlJobExecutor cross-Admin migration cleans up old Admin; deletion falls back to GetJobGroupByAppName when JobGroupID=0
	// - R10-P1-2: Terminal callback rejection ("log repeate callback.") converges idempotently, removes finalizer, and wakes next SERIAL_EXECUTION Job
	// - R10-P2-5: Bounded token cache pruning (maxTokenCacheEntries) and 5s negative Secret error caching
	t.Run("Round10ControllerFixes", func(t *testing.T) {
		// (a) R10-P1-1: RegistryHeartbeatRunner skips unsynced/cross-Admin executors, and XxlJobExecutor cross-Admin migration cleans up old Admin
		oldExecSrv, oldExecMu, oldExecRegs, oldExecGroups, _, _ := setupTestAdminServer(t, &failCallback)
		oldExecMu.Lock()
		oldExecGroups[77] = xxlclient.JobGroup{
			ID:          77,
			AppName:     "mig-exec-app",
			Title:       "MigExec",
			AddressType: 1,
			AddressList: "http://10.244.9.9:9999/",
		}
		oldExecRegs["mig-exec-app"] = map[string]bool{"http://10.244.9.9:9999/": true}
		oldExecMu.Unlock()

		oldExecAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "old-exec-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: oldExecSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		migExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "mig-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:        xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"}, // target new Admin
				AppName:         "mig-exec-app",
				Title:           "MigExec",
				Mode:            xxljobv1alpha1.ExecutorModeK8sJobProxy,
				ExecutorPort:    9999,
				StaticAddresses: []string{"http://10.244.9.10:9999/"},
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID:          77,
				AdminURL:            oldExecSrv.URL, // previously synced to oldExecAdmin
				RegisteredAddresses: []string{"http://10.244.9.9:9999/"},
				Synced:              true,
			},
		}
		_ = k8sClient.Create(ctx, oldExecAdmin)
		_ = k8sClient.Create(ctx, migExec)
		_ = k8sClient.Status().Update(ctx, migExec)

		// Before migExec reconciles against xxl-job-admin, RegistryHeartbeatRunner.RunOnce must skip migExec
		// because migExec.Status.AdminURL (oldExecSrv.URL) != ResolveAdminURL(xxl-job-admin) (adminSrv.URL)!
		hbRunner := &RegistryHeartbeatRunner{
			Client: k8sClient,
		}
		hbRunner.RunOnce(ctx)
		mu.Lock()
		prematureRegs := len(registries["mig-exec-app"])
		mu.Unlock()
		if prematureRegs != 0 {
			t.Fatalf("R10-P1-1 regression: RegistryHeartbeatRunner pushed heartbeat to new Admin before XxlJobExecutor migrated")
		}

		// Now reconcile migExec: it must clean up oldExecSrv (group 77 and registry 10.244.9.9) and create on xxl-job-admin
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig-exec"}}); err != nil {
			t.Fatalf("R10-P1-1: reconcile mig-exec failed: %v", err)
		}
		oldExecMu.Lock()
		_, oldGrp77Exists := oldExecGroups[77]
		oldRegCount := len(oldExecRegs["mig-exec-app"])
		oldExecMu.Unlock()
		if oldGrp77Exists || oldRegCount != 0 {
			t.Fatalf("R10-P1-1 regression: expected old Admin group 77 and registries to be cleaned up on cross-Admin executor migration, got grpExists=%v regCount=%d",
				oldGrp77Exists, oldRegCount)
		}

		// Also verify deletion fallback by AppName when Status.JobGroupID == 0
		var migExecAfter xxljobv1alpha1.XxlJobExecutor
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "mig-exec"}, &migExecAfter)
		newGrpID := migExecAfter.Status.JobGroupID
		migExecAfter.Status.JobGroupID = 0 // simulate lost Status.JobGroupID before deletion
		_ = k8sClient.Status().Update(ctx, &migExecAfter)
		_ = k8sClient.Delete(ctx, &migExecAfter)
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "mig-exec"}}); err != nil {
			t.Fatalf("R10-P1-1: delete mig-exec with JobGroupID=0 failed: %v", err)
		}
		mu.Lock()
		_, newGrpStillExists := groups[newGrpID]
		mu.Unlock()
		if newGrpStillExists {
			t.Fatalf("R10-P1-1 regression: expected deletion with JobGroupID=0 to fall back to GetJobGroupByAppName and delete group %d", newGrpID)
		}

		// (b) R10-P1-2: Terminal callback rejection ("log repeate callback.") converges idempotently and wakes next SERIAL_EXECUTION Job
		repeatAdminMux := http.NewServeMux()
		repeatAdminMux.HandleFunc("/api/callback", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 500, Msg: "log repeate callback."})
		})
		repeatAdminSrv := httptest.NewServer(repeatAdminMux)
		defer repeatAdminSrv.Close()

		repeatAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "repeat-cb-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: repeatAdminSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		repeatExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "repeat-cb-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "repeat-cb-admin"},
				AppName:  "repeat-cb-exec",
				Title:    "RepeatCB",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, repeatAdmin)
		_ = k8sClient.Create(ctx, repeatExec)

		suspFalse := false
		repeatJob1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-repeat-801-12001",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "repeat-cb-exec",
					proxyexecutor.LabelJobID:        "801",
					proxyexecutor.LabelLogID:        "12001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "repeat-cb-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "12001",
					proxyexecutor.AnnotationLogDateTime:   "1700000120001",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspFalse,
			},
			Status: batchv1.JobStatus{
				Succeeded: 1,
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobComplete,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		repeatJob2 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-repeat-801-12002",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "repeat-cb-exec",
					proxyexecutor.LabelJobID:        "801",
					proxyexecutor.LabelLogID:        "12002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "repeat-cb-exec",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "12002",
					proxyexecutor.AnnotationLogDateTime:   "1700000120002",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspendTrue,
			},
		}
		_ = k8sClient.Create(ctx, repeatJob1)
		_ = k8sClient.Status().Update(ctx, repeatJob1)
		_ = k8sClient.Create(ctx, repeatJob2)

		resRep, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: repeatJob1.Name}})
		if err != nil || resRep.RequeueAfter != 0 {
			t.Fatalf("R10-P1-2 regression: expected terminal 'log repeate callback.' rejection to converge with RequeueAfter=0, got res=%+v err=%v", resRep, err)
		}
		var checkRep1, checkRep2 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: repeatJob1.Name}, &checkRep1)
		if checkRep1.Annotations[proxyexecutor.AnnotationCallbackSent] != "true" || len(checkRep1.Finalizers) != 0 {
			t.Fatalf("R10-P1-2 regression: expected callback-sent=true and finalizer removed on repeat callback, got annotations=%v finalizers=%v",
				checkRep1.Annotations, checkRep1.Finalizers)
		}
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: repeatJob2.Name}, &checkRep2)
		if checkRep2.Spec.Suspend != nil && *checkRep2.Spec.Suspend {
			t.Fatalf("R10-P1-2 regression: expected queued SERIAL_EXECUTION Job2 to be unsuspended after terminal callback rejection on Job1")
		}

		// (c) R10-P2-5: Verify bounded token cache pruning (maxTokenCacheEntries)
		secretCacheMu.Lock()
		for i := 0; i < maxTokenCacheEntries+20; i++ {
			secretCache["ns/sec-"+strconv.Itoa(i)] = cachedTokenEntry{
				token:     "tok",
				expiresAt: time.Now().Add(10 * time.Second),
			}
		}
		pruneTokenCacheMapLocked(secretCache, time.Now())
		cacheLen := len(secretCache)
		secretCacheMu.Unlock()
		if cacheLen >= maxTokenCacheEntries {
			t.Fatalf("R10-P2-5 regression: expected pruneTokenCacheMapLocked to bound secretCache below %d, got %d", maxTokenCacheEntries, cacheLen)
		}
	})

	// 25. Test R11-P1-1, R11-P1-3, R11-P2-1:
	//  (a) XxlJobTaskReconciler waits for XxlJobExecutor to sync AdminURL before cross-Admin EnsureJobInfo,
	//      and XxlJobExecutor cross-Admin migration removes owned tasks on previous Admin so old JobGroup is not leaked
	//  (b) In-place Spec.AppName change migrates owned tasks to new JobGroupID and removes old JobGroup on same Admin
	//  (c) Completed/failed SERIAL_EXECUTION Job with bypassed phantom Terminating pods advances serial queue after removing finalizer
	//  (d) Secret trailing newline trimming, prevToken preservation across secretCache, and Auth.PasswordSecretRef in credentials hash
	t.Run("Round11CrossAdminExecutorWithTasksAndSerialBypassAndSecretTrimming", func(t *testing.T) {
		r11SrvA, r11MuA, _, r11GroupsA, _, _ := setupTestAdminServer(t, &failCallback)
		r11SrvB, r11MuB, _, r11GroupsB, _, _ := setupTestAdminServer(t, &failCallback)

		r11AdminA := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-admin-a", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: r11SrvA.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		r11AdminB := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-admin-b", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: r11SrvB.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "secret_token"},
			},
		}
		_ = k8sClient.Create(ctx, r11AdminA)
		_ = k8sClient.Create(ctx, r11AdminB)

		r11Exec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-mig-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "r11-admin-a"},
				AppName:  "r11-mig-app-v1",
				Title:    "R11 Mig Exec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, r11Exec)
		// Two reconciles (first adds finalizer, second syncs JobGroup on admin-a)
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}})
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}})

		r11Task := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-mig-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "r11-mig-exec"},
				JobDesc:         "R11 Mig Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 */10 * * * ?",
				ExecutorHandler: "r11MigHandler",
				TriggerStatus:   true,
			},
		}
		_ = k8sClient.Create(ctx, r11Task)
		_, _ = taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-task"}})
		_, _ = taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-task"}})

		// Switch r11Exec.Spec.AdminRef to r11-admin-b, and run taskReconciler BEFORE execReconciler:
		// taskReconciler must wait (Synced=false) because r11Exec.Status.AdminURL still points to r11-admin-a!
		var latestR11Exec xxljobv1alpha1.XxlJobExecutor
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}, &latestR11Exec)
		oldGroupIDOnA := latestR11Exec.Status.JobGroupID
		latestR11Exec.Spec.AdminRef = xxljobv1alpha1.LocalObjectReference{Name: "r11-admin-b"}
		_ = k8sClient.Update(ctx, &latestR11Exec)

		resPremature, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-task"}})
		if err != nil || resPremature.RequeueAfter == 0 {
			t.Fatalf("R11-P1-1 regression: expected taskReconciler to wait when execCR.Status.AdminURL has not yet synced to new Admin, got res=%+v err=%v", resPremature, err)
		}

		// Now run execReconciler: it must remove the owned task on r11-admin-a and delete oldGroupIDOnA on r11-admin-a cleanly!
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}}); err != nil {
			t.Fatalf("R11-P1-1: execReconciler cross-Admin migration failed: %v", err)
		}
		r11MuA.Lock()
		_, stillOnAdminA := r11GroupsA[oldGroupIDOnA]
		r11MuA.Unlock()
		if stillOnAdminA {
			t.Fatalf("R11-P1-1 regression: expected old JobGroup %d on r11-admin-a to be removed during cross-Admin migration even when referencing XxlJobTasks existed", oldGroupIDOnA)
		}

		// Verify mapExecutorToTasks enqueues r11-mig-task
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}, &latestR11Exec)
		mappedReqs := taskReconciler.mapExecutorToTasks(ctx, &latestR11Exec)
		if len(mappedReqs) != 1 || mappedReqs[0].Name != "r11-mig-task" {
			t.Fatalf("R11-P1-1 regression: expected mapExecutorToTasks to enqueue r11-mig-task, got %+v", mappedReqs)
		}

		// Reconcile r11-mig-task now that r11-mig-exec is synced on r11-admin-b
		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-task"}}); err != nil {
			t.Fatalf("R11-P1-1: taskReconciler after executor migration failed: %v", err)
		}

		// (b) Change Spec.AppName in-place on r11-admin-b and verify old JobGroup on r11-admin-b is migrated and cleaned up
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}, &latestR11Exec)
		oldGroupOnB := latestR11Exec.Status.JobGroupID
		latestR11Exec.Spec.AppName = "r11-mig-app-v2"
		_ = k8sClient.Update(ctx, &latestR11Exec)
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "r11-mig-exec"}}); err != nil {
			t.Fatalf("R11-P1-1: execReconciler in-place AppName change failed: %v", err)
		}
		r11MuB.Lock()
		_, oldGrpStillOnB := r11GroupsB[oldGroupOnB]
		r11MuB.Unlock()
		if oldGrpStillOnB {
			t.Fatalf("R11-P1-1 regression: expected previous JobGroup %d on r11-admin-b to be cleaned up after in-place AppName change", oldGroupOnB)
		}

		// (c) R11-P1-3: Completed/failed SERIAL_EXECUTION Job with phantom Terminating=1 and force-delete=true
		// bypasses pod cleanup wait, removes finalizer, and immediately unsuspends the next queued SERIAL_EXECUTION Job!
		termOne := int32(1)
		suspFalse := false
		bypassJob1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-r11-bypass-901-13001",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "order-executor",
					proxyexecutor.LabelJobID:        "901",
					proxyexecutor.LabelLogID:        "13001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "order-executor",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "13001",
					proxyexecutor.AnnotationLogDateTime:   "1700000130001",
					proxyexecutor.AnnotationCallbackSent:  "false",
					AnnotationForceDelete:                 "true",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspFalse},
			Status: batchv1.JobStatus{
				Failed:      1,
				Terminating: &termOne,
				Conditions: []batchv1.JobCondition{
					{
						Type:    batchv1.JobFailed,
						Status:  corev1.ConditionTrue,
						Reason:  "DeadlineExceeded",
						Message: "Job was active longer than specified deadline",
					},
				},
			},
		}
		bypassJob2 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-r11-bypass-901-13002",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "order-executor",
					proxyexecutor.LabelJobID:        "901",
					proxyexecutor.LabelLogID:        "13002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "order-executor",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "13002",
					proxyexecutor.AnnotationLogDateTime:   "1700000130002",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendTrue},
		}
		_ = k8sClient.Create(ctx, bypassJob1)
		_ = k8sClient.Status().Update(ctx, bypassJob1)
		_ = k8sClient.Create(ctx, bypassJob2)

		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: bypassJob1.Name}}); err != nil {
			t.Fatalf("R11-P1-3: reconcile bypassJob1 failed: %v", err)
		}
		var checkBypass2 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: bypassJob2.Name}, &checkBypass2)
		if checkBypass2.Spec.Suspend != nil && *checkBypass2.Spec.Suspend {
			t.Fatalf("R11-P1-3 regression: expected bypassJob2 to be unsuspended immediately after bypassJob1 removed JobCallbackFinalizer despite phantom Terminating=1")
		}

		// (d) R11-P2-1: Secret trailing newline trimming and prevToken propagation from secretCache to tokenCache
		newlineSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-newline-sec", Namespace: "default"},
			Data: map[string][]byte{
				"access-token": []byte("tok-v1-newline\n"),
				"admin-pass":   []byte("pass-v1\n"),
			},
		}
		_ = k8sClient.Create(ctx, newlineSec)
		r11SecAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-sec-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: r11SrvA.URL,
				Auth: xxljobv1alpha1.AuthSpec{
					PasswordSecretRef:    &xxljobv1alpha1.SecretKeyReference{Name: "r11-newline-sec", Key: "admin-pass"},
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: "r11-newline-sec", Key: "access-token"},
				},
			},
		}
		_ = k8sClient.Create(ctx, r11SecAdmin)
		r11SecExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-sec-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "r11-sec-admin"},
				AppName:  "r11-sec-app",
				Title:    "R11 Sec Exec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, r11SecExec)

		tok1, err := ResolveAdminExplicitAccessToken(ctx, k8sClient, r11SecAdmin)
		if err != nil || tok1 != "tok-v1-newline" {
			t.Fatalf("R11-P2-1 regression: expected trailing newline to be trimmed from Secret token, got %q err=%v", tok1, err)
		}
		hash1, err := adminReconciler.computeCredentialsHash(ctx, r11SecAdmin)
		if err != nil || hash1 == "" {
			t.Fatalf("R11-P2-1: computeCredentialsHash failed: %v", err)
		}

		// Rotate both access-token and admin-pass in Secret, invalidate secretCache (simulating XxlJobAdminReconciler),
		// and read via ResolveAdminExplicitAccessToken BEFORE ValidateExecutorAccessToken:
		// ValidateExecutorAccessToken must still accept "tok-v1-newline" via secretPrevToken!
		newlineSec.Data["access-token"] = []byte("tok-v2-newline\n")
		newlineSec.Data["admin-pass"] = []byte("pass-v2\n")
		_ = k8sClient.Update(ctx, newlineSec)
		hash2, err := adminReconciler.computeCredentialsHash(ctx, r11SecAdmin)
		if err != nil || hash2 == hash1 {
			t.Fatalf("R11-P2-1 regression: expected computeCredentialsHash to change when Auth.PasswordSecretRef/AccessTokenSecretRef rotates")
		}
		invalidateSecretCache(k8sClient, "default", r11SecAdmin.Spec.Auth.AccessTokenSecretRef)
		_, _, _ = ResolveAdminExplicitAccessTokens(ctx, k8sClient, r11SecAdmin)
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v1-newline"); err != nil {
			t.Fatalf("R11-P2-1 regression: expected ValidateExecutorAccessToken to accept previous token from secretCache rotation grace window, got %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v2-newline"); err != nil {
			t.Fatalf("R11-P2-1 regression: expected ValidateExecutorAccessToken to accept new trimmed token, got %v", err)
		}

		// (e) Round 12 Fixes:
		//  1. Deleting SERIAL_EXECUTION Job with force-delete=true and stuck Active/Terminating=1 pods advances the serial queue after removing finalizer
		//  2. In-place Spec.AppName change deregisters recorded addresses under previous AppName from xxl_job_registry
		//  3. ValidateExecutorAccessToken forces fresh Secret read when tokenCache missed on warm secretCache, and preserves prevToken across transient Secret missing/negative cache
		delBypassNow := metav1.Now()
		delBypassJob1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "xxljob-r12-delbypass-902-14001",
				Namespace:         "default",
				DeletionTimestamp: &delBypassNow,
				Finalizers:        []string{proxyexecutor.JobCallbackFinalizer, "kubernetes.io/test-hold"},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "order-executor",
					proxyexecutor.LabelJobID:        "902",
					proxyexecutor.LabelLogID:        "14001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "order-executor",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "14001",
					proxyexecutor.AnnotationLogDateTime:   "1700000140001",
					proxyexecutor.AnnotationCallbackSent:  "false",
					AnnotationForceDelete:                 "true",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspFalse},
			Status: batchv1.JobStatus{
				Active:      1,
				Terminating: &termOne,
			},
		}
		delBypassJob2 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-r12-delbypass-902-14002",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "order-executor",
					proxyexecutor.LabelJobID:        "902",
					proxyexecutor.LabelLogID:        "14002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationExecutorName:  "order-executor",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
					proxyexecutor.AnnotationLogID:         "14002",
					proxyexecutor.AnnotationLogDateTime:   "1700000140002",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendTrue},
		}
		_ = k8sClient.Create(ctx, delBypassJob1)
		_ = k8sClient.Status().Update(ctx, delBypassJob1)
		_ = k8sClient.Delete(ctx, delBypassJob1)
		_ = k8sClient.Create(ctx, delBypassJob2)

		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delBypassJob1.Name}}); err != nil {
			t.Fatalf("R12-P1-1: reconcile delBypassJob1 failed: %v", err)
		}
		var checkDelBypass2 batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: delBypassJob2.Name}, &checkDelBypass2)
		if checkDelBypass2.Spec.Suspend != nil && *checkDelBypass2.Spec.Suspend {
			t.Fatalf("R12-P1-1 regression: expected delBypassJob2 to be unsuspended after deleting Job1 with force-delete=true removed JobCallbackFinalizer")
		}

		// Verify ValidateExecutorAccessToken when tokenCache entry expires while secretCache still holds stale token:
		// rotating Secret without invalidating secretCache must still succeed on first ValidateExecutorAccessToken call!
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r11-newline-sec"}, newlineSec)
		newlineSec.Data["access-token"] = []byte("tok-v3-rotated\n")
		_ = k8sClient.Update(ctx, newlineSec)
		tokenCacheMu.Lock()
		for k := range tokenCache {
			if strings.HasSuffix(k, "|default/r11-sec-exec") {
				delete(tokenCache, k) // simulate tokenCache expiration while secretCache still holds tok-v2-newline
			}
		}
		tokenCacheMu.Unlock()
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v3-rotated"); err != nil {
			t.Fatalf("R12-P2-1 regression: expected ValidateExecutorAccessToken to force fresh Secret read when tokenCache missed on warm secretCache, got %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v2-newline"); err != nil {
			t.Fatalf("R12-P2-1 regression: expected previous token tok-v2-newline to remain valid during rotation grace period, got %v", err)
		}

		// Verify negative-cache Secret deletion & recreate rotation preserves prevToken grace window
		// and invalidateSecretCache directly invalidates tokenCache without manual cache deletion:
		_ = k8sClient.Delete(ctx, newlineSec)
		invalidateSecretCache(k8sClient, "default", &xxljobv1alpha1.SecretKeyReference{Name: "r11-newline-sec", Key: "access-token"})
		// Trigger negative cache entry while Secret is temporarily missing
		_ = ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v3-rotated")
		// Recreate Secret with tok-v4-recreated and invalidate via invalidateSecretCache
		recreatedSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-newline-sec", Namespace: "default"},
			Data:       map[string][]byte{"access-token": []byte("tok-v4-recreated")},
		}
		_ = k8sClient.Create(ctx, recreatedSec)
		invalidateSecretCache(k8sClient, "default", &xxljobv1alpha1.SecretKeyReference{Name: "r11-newline-sec", Key: "access-token"})
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v4-recreated"); err != nil {
			t.Fatalf("R12-P2-1 regression: expected recreated token tok-v4-recreated to succeed after invalidateSecretCache, got %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v3-rotated"); err != nil {
			t.Fatalf("R12-P2-1 regression: expected tok-v3-rotated to remain valid after negative-cache Secret recreate rotation, got %v", err)
		}

		// Verify switching Executor Spec.AdminRef.Name to a different Admin does NOT leak old Admin's token into prevToken grace window:
		otherAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-other-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: "http://other-admin:8080/xxl-job-admin",
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "other-admin-token"},
			},
		}
		_ = k8sClient.Create(ctx, otherAdmin)
		var secExec xxljobv1alpha1.XxlJobExecutor
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "r11-sec-exec"}, &secExec)
		secExec.Spec.AdminRef.Name = "r12-other-admin"
		_ = k8sClient.Update(ctx, &secExec)
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "other-admin-token"); err != nil {
			t.Fatalf("expected other-admin-token to succeed after AdminRef switch, got %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", "r11-sec-exec", "tok-v4-recreated"); err == nil {
			t.Fatalf("expected previous Admin's token tok-v4-recreated to be rejected after executor switched AdminRef to r12-other-admin")
		}

		// Verify XxlJobTaskReconciler and RegistryHeartbeatRunner wait when XxlJobExecutor.Spec.AppName is modified
		// in-place before XxlJobExecutorReconciler has synced the new AppName:
		renameWaitExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-rename-wait-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "r11-admin-a"},
				AppName:  "r12-wait-app-v1",
				Title:    "R12WaitExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, renameWaitExec)
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: renameWaitExec.Name}})
		_, _ = execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: renameWaitExec.Name}})
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: renameWaitExec.Name}, renameWaitExec)
		if !renameWaitExec.Status.Synced || !isExecutorAppNameSynced(renameWaitExec) {
			t.Fatalf("expected Synced=true and isExecutorAppNameSynced=true after XxlJobExecutorReconciler synced r12-wait-app-v1")
		}
		// Now mutate Spec.AppName in-place WITHOUT running XxlJobExecutorReconciler yet:
		renameWaitExec.Spec.AppName = "r12-wait-app-v2"
		_ = k8sClient.Update(ctx, renameWaitExec)
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: renameWaitExec.Name}, renameWaitExec)
		if isExecutorAppNameSynced(renameWaitExec) {
			t.Fatalf("expected isExecutorAppNameSynced=false while Spec.AppName=r12-wait-app-v2 differs from AppNameSynced condition")
		}
		renameWaitTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-rename-wait-task", Namespace: "default", Finalizers: []string{TaskFinalizer}},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: renameWaitExec.Name},
				JobDesc:         "Rename Wait Task",
				Author:          "tester",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 0 12 * * ?",
				ExecutorHandler: "renameWaitHandler",
			},
		}
		_ = k8sClient.Create(ctx, renameWaitTask)
		waitRes, waitErr := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: renameWaitTask.Name}})
		if waitErr != nil || waitRes.RequeueAfter != 5*time.Second {
			t.Fatalf("expected XxlJobTaskReconciler to wait (RequeueAfter=5s) while executor AppName is mid-rename, got res=%+v err=%v", waitRes, waitErr)
		}

		// Verify cleanupExecutorOnAdmin does NOT falsely treat a rejected RemoveJobGroup as alreadyDeleted=true
		// when Spec.AppName was changed before deletion but GetJobGroupByID proves targetGroupID still exists:
		var rejectGroupRemoveID atomic.Int32
		rejectAdminMux := http.NewServeMux()
		rejectAdminMux.HandleFunc("/login", func(w http.ResponseWriter, _ *http.Request) {
			http.SetCookie(w, &http.Cookie{Name: "XXL_JOB_LOGIN_IDENTITY", Value: "ok"})
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		rejectAdminMux.HandleFunc("/jobgroup/pageList", func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			appname := r.FormValue("appname")
			if appname == "" || appname == "orig-app-still-exists" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"recordsTotal": 1,
					"data": []xxlclient.JobGroup{
						{ID: 77, AppName: "orig-app-still-exists", Title: "OrigGrp", AddressType: 1},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"recordsTotal": 0, "data": []xxlclient.JobGroup{}})
		})
		rejectAdminMux.HandleFunc("/jobinfo/pageList", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"recordsTotal": 0, "data": []xxlclient.JobInfo{}})
		})
		rejectAdminMux.HandleFunc("/jobgroup/remove", func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			id, _ := strconv.Atoi(r.FormValue("id"))
			rejectGroupRemoveID.Store(int32(id))
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 500, Msg: "manual tasks still exist in group"})
		})
		rejectAdminMux.HandleFunc("/api/registryRemove", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		rejectAdminSrv := httptest.NewServer(rejectAdminMux)
		defer rejectAdminSrv.Close()

		rejectAdminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-reject-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: rejectAdminSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "reject-admin-tok"},
			},
		}
		_ = k8sClient.Create(ctx, rejectAdminCR)
		delRenamedExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "r12-del-renamed-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: rejectAdminCR.Name},
				AppName:  "unsynced-renamed-app",
				Title:    "DelRenamedExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 77,
				AdminURL:   rejectAdminSrv.URL,
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, delRenamedExec)
		_ = k8sClient.Status().Update(ctx, delRenamedExec)
		_ = k8sClient.Delete(ctx, delRenamedExec)

		delRes, delErr := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delRenamedExec.Name}})
		if delErr != nil || delRes.RequeueAfter == 0 {
			t.Fatalf("expected deleting executor to requeue when RemoveJobGroup is rejected and GetJobGroupByID(77) still exists, got res=%+v err=%v", delRes, delErr)
		}
		var verifyStillHasFinalizer xxljobv1alpha1.XxlJobExecutor
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: delRenamedExec.Name}, &verifyStillHasFinalizer); err != nil {
			t.Fatalf("expected deleting executor CR to still exist with finalizer when RemoveJobGroup(77) was rejected, got err=%v", err)
		}

		// Verify unauthenticated forceRefresh on sibling executor does NOT wipe lastForcedAt cooldown on other executors:
		sibExecA := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-sib-exec-a", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "r11-sec-admin"},
				AppName:  "r12-sib-app-a",
				Title:    "SibExecA",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		sibExecB := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r12-sib-exec-b", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "r11-sec-admin"},
				AppName:  "r12-sib-app-b",
				Title:    "SibExecB",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, sibExecA)
		_ = k8sClient.Create(ctx, sibExecB)
		// Warm both executors in tokenCache
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", sibExecA.Name, "tok-v4-recreated"); err != nil {
			t.Fatalf("expected initial token check on sibExecA to succeed: %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", sibExecB.Name, "tok-v4-recreated"); err != nil {
			t.Fatalf("expected initial token check on sibExecB to succeed: %v", err)
		}
		// Trigger bad token on sibExecA -> records lastForcedAt on sibExecA
		_ = ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", sibExecA.Name, "wrong-token-1")
		// Trigger bad token on sibExecB -> must NOT wipe sibExecA's lastForcedAt in tokenCache
		_ = ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", sibExecB.Name, "wrong-token-2")
		var sibAEntry cachedTokenEntry
		var sibAExists bool
		tokenCacheMu.Lock()
		for k, v := range tokenCache {
			if strings.HasSuffix(k, "|default/"+sibExecA.Name) {
				sibAEntry = v
				sibAExists = true
				break
			}
		}
		tokenCacheMu.Unlock()
		if !sibAExists || sibAEntry.lastForcedAt.IsZero() {
			t.Fatalf("expected sibExecA tokenCache entry and lastForcedAt cooldown to remain intact after bad-token forceRefresh on sibExecB, got exists=%v entry=%+v", sibAExists, sibAEntry)
		}
	})

	// 18. Test Endgame Round 2 Fixes (EG2-01, EG2-04, EG2-05, EG2-06, EG2-07, EG2-08, EG2-09):
	t.Run("Endgame2LifecycleSecurityCallbackAndMetrics", func(t *testing.T) {
		// (a) EG2-01: Verify CRD YAML validation rules and restricted sample securityContext
		execCRDYAML, err := os.ReadFile("../../config/crd/bases/xxljob.io_xxljobexecutors.yaml")
		if err != nil {
			t.Fatalf("read xxljobexecutors CRD failed: %v", err)
		}
		if strings.Contains(string(execCRDYAML), "self == oldSelf") {
			t.Fatalf("EG2-01 regression: xxljobexecutors CRD still blocks in-place spec.appName updates via self == oldSelf")
		}
		adminCRDYAML, err := os.ReadFile("../../config/crd/bases/xxljob.io_xxljobadmins.yaml")
		if err != nil {
			t.Fatalf("read xxljobadmins CRD failed: %v", err)
		}
		if !strings.Contains(string(adminCRDYAML), "has(self.datasource.url)") {
			t.Fatalf("EG2-01 regression: xxljobadmins CRD missing safe has(self.datasource.url) CEL guard for externalURL mode")
		}
		taskCRDYAML, err := os.ReadFile("../../config/crd/bases/xxljob.io_xxljobtasks.yaml")
		if err != nil {
			t.Fatalf("read xxljobtasks CRD failed: %v", err)
		}
		if !strings.Contains(string(taskCRDYAML), "size(self) == 0 || size(self) >= 4") {
			t.Fatalf("EG2-01 regression: xxljobtasks CRD glueRemark missing empty-or->=4 CEL validation")
		}
		sample03YAML, err := os.ReadFile("../../config/samples/03-executor-k8sjob-proxy.yaml")
		if err != nil {
			t.Fatalf("read 03-executor-k8sjob-proxy.yaml failed: %v", err)
		}
		if !strings.Contains(string(sample03YAML), "runAsUser: 65534") {
			t.Fatalf("EG2-01 regression: 03-executor-k8sjob-proxy.yaml missing non-root runAsUser for restricted PSS")
		}

		// (b) EG2-05: ValidateExecutorAccessToken rejects Resident/Sidecar/terminating executors & isolates recreated XxlJobAdmin by UID
		uidAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eg2-uid-admin",
				Namespace: "default",
				UID:       types.UID("admin-uid-v1"),
			},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: adminSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "tok-uid-v1"},
			},
		}
		resModeExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-resident-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: uidAdmin.Name},
				AppName:  "eg2-resident-app",
				Title:    "ResidentExec",
				Mode:     xxljobv1alpha1.ExecutorModeResident,
			},
		}
		proxyModeExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "eg2-proxy-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: uidAdmin.Name},
				AppName:  "eg2-proxy-app",
				Title:    "ProxyExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, uidAdmin)
		_ = k8sClient.Create(ctx, resModeExec)
		_ = k8sClient.Create(ctx, proxyModeExec)

		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", resModeExec.Name, "tok-uid-v1"); err == nil {
			t.Fatalf("EG2-05 regression: expected ValidateExecutorAccessToken to reject Resident-mode XxlJobExecutor")
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", proxyModeExec.Name, "tok-uid-v1"); err != nil {
			t.Fatalf("expected ValidateExecutorAccessToken to accept K8sJobProxy executor: %v", err)
		}

		// Delete and recreate uidAdmin with the SAME name but a new UID and token: old tok-uid-v1 must NOT inherit grace period!
		_ = k8sClient.Delete(ctx, uidAdmin)
		recreatedAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "eg2-uid-admin",
				Namespace: "default",
				UID:       types.UID("admin-uid-v2"),
			},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: adminSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "tok-uid-v2"},
			},
		}
		_ = k8sClient.Create(ctx, recreatedAdmin)
		// Even BEFORE any caller sends tok-uid-v2, sending the old tok-uid-v1 must immediately fail (UID mismatch invalidates cache)
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", proxyModeExec.Name, "tok-uid-v1"); err == nil {
			t.Fatalf("EG2-05 regression: deleted XxlJobAdmin's token tok-uid-v1 accepted via stale tokenCache before tok-uid-v2 was ever sent!")
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", proxyModeExec.Name, "tok-uid-v2"); err != nil {
			t.Fatalf("expected new UID admin token tok-uid-v2 to succeed: %v", err)
		}
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", proxyModeExec.Name, "tok-uid-v1"); err == nil {
			t.Fatalf("EG2-05 regression: deleted XxlJobAdmin's token tok-uid-v1 leaked into recreated same-name XxlJobAdmin via prevToken!")
		}

		// Mark proxyModeExec terminating -> ValidateExecutorAccessToken must reject immediately, and Reconcile deletion must clean metrics (EG2-04)
		markCallbackPending("default", proxyModeExec.Name, "eg2-pending-job-1")
		proxyexecutor.OnSerialQueueDepth("default", proxyModeExec.Name, "901", 3)
		_ = k8sClient.Delete(ctx, proxyModeExec)
		if err := ValidateExecutorAccessToken(ctx, k8sClient, k8sClient, "default", proxyModeExec.Name, "tok-uid-v2"); err == nil {
			t.Fatalf("EG2-05 regression: expected terminating XxlJobExecutor to be rejected by ValidateExecutorAccessToken")
		}
		if _, err := execReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: proxyModeExec.Name}}); err != nil {
			t.Fatalf("reconcile deleting proxyModeExec failed: %v", err)
		}
		metricsMu.Lock()
		_, hasPendingCB := pendingCallbackJobs["default/eg2-pending-job-1"]
		_, hasSerialSeries := activeSerialQueueSeries["default/"+proxyModeExec.Name+"/901"]
		metricsMu.Unlock()
		if hasPendingCB || hasSerialSeries {
			t.Fatalf("EG2-04 regression: expected executor deletion to clean pendingCallbackJobs (%v) and activeSerialQueueSeries (%v)", hasPendingCB, hasSerialSeries)
		}

		// (c) EG2-06: In-cluster -> External XxlJobAdmin transition deletes owned Deployment/Service, resets ReadyReplicas, and trims token
		transAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-trans-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image: "xuxueli/xxl-job-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					Password: "pwd",
				},
				Auth: xxljobv1alpha1.AuthSpec{
					AccessToken: "  trimmed-inline-token\n",
				},
			},
		}
		_ = k8sClient.Create(ctx, transAdmin)
		if _, err := adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: transAdmin.Name}}); err != nil {
			t.Fatalf("initial reconcile transAdmin failed: %v", err)
		}
		if _, err := adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: transAdmin.Name}}); err != nil {
			t.Fatalf("second reconcile transAdmin failed: %v", err)
		}
		var createdDep appsv1.Deployment
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: transAdmin.Name}, &createdDep); err != nil {
			t.Fatalf("expected in-cluster Deployment for transAdmin: %v", err)
		}
		foundTrimmedTok := false
		for _, ev := range createdDep.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == "XXL_JOB_ACCESSTOKEN" && ev.Value == "trimmed-inline-token" {
				foundTrimmedTok = true
			}
		}
		if !foundTrimmedTok {
			t.Fatalf("EG2-06 regression: expected buildDeployment to trim whitespace on inline AccessToken")
		}
		createdDep.Status.ReadyReplicas = 2
		_ = k8sClient.Status().Update(ctx, &createdDep)
		_, _ = adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: transAdmin.Name}})

		// Transition transAdmin to ExternalURL mode (Image = "")
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: transAdmin.Name}, transAdmin)
		transAdmin.Spec.Image = ""
		transAdmin.Spec.ExternalURL = adminSrv.URL
		transAdmin.Generation++
		_ = k8sClient.Update(ctx, transAdmin)
		if _, err := adminReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: transAdmin.Name}}); err != nil {
			t.Fatalf("transition reconcile transAdmin failed: %v", err)
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: transAdmin.Name}, &createdDep); !apierrors.IsNotFound(err) {
			t.Fatalf("EG2-06 regression: expected owned in-cluster Deployment to be deleted when transitioning to externalURL, got err=%v", err)
		}
		var checkSvc corev1.Service
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: transAdmin.Name}, &checkSvc); !apierrors.IsNotFound(err) {
			t.Fatalf("EG2-06 regression: expected owned in-cluster Service to be deleted when transitioning to externalURL, got err=%v", err)
		}
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: transAdmin.Name}, transAdmin)
		if transAdmin.Status.ReadyReplicas != 0 {
			t.Fatalf("EG2-06 regression: expected ReadyReplicas=0 after transitioning to externalURL, got %d", transAdmin.Status.ReadyReplicas)
		}

		// (d) EG2-07 & EG2-08: clearPreviousAdminStatus resets address slices, predicates fire on deletion,
		// and XxlJobTask deletion cleans remote job even if Spec.ExecutorRef.Namespace was invalidly edited!
		dummyExec := &xxljobv1alpha1.XxlJobExecutor{
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID:          5,
				AdminURL:            "http://old-admin",
				RegisteredAddresses: []string{"http://10.0.0.1:9999/"},
				ActivePods:          1,
			},
		}
		if !clearPreviousAdminStatus(dummyExec) || len(dummyExec.Status.RegisteredAddresses) != 0 || dummyExec.Status.ActivePods != 0 {
			t.Fatalf("EG2-07 regression: clearPreviousAdminStatus failed to reset RegisteredAddresses/ActivePods: %+v", dummyExec.Status)
		}
		nowTS := metav1.Now()
		delPred := deletionTimestampChangedPredicate()
		oldObj := &xxljobv1alpha1.XxlJobTask{}
		newObj := &xxljobv1alpha1.XxlJobTask{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &nowTS}}
		if !delPred.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
			t.Fatalf("EG2-08 regression: deletionTimestampChangedPredicate did not fire when DeletionTimestamp became non-zero")
		}
		if !executorStatusChangedPredicate().Delete(event.DeleteEvent{Object: dummyExec}) {
			t.Fatalf("EG2-08 regression: executorStatusChangedPredicate.Delete returned false")
		}

		mu.Lock()
		jobs[991] = xxlclient.JobInfo{
			ID:              991,
			JobGroup:        1,
			JobDesc:         xxlclient.FormatOwnedJobDesc("Invalid NS Deleted Task", "default/eg2-invalid-ns-del-task"),
			ExecutorHandler: "invNsDelHandler",
			TriggerStatus:   1,
		}
		mu.Unlock()
		invNsTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "eg2-invalid-ns-del-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Namespace: "kube-system", Name: "other-exec"},
				JobDesc:         "Invalid NS Deleted Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 0 1 * * ?",
				ExecutorHandler: "invNsDelHandler",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      991,
				JobGroupID: 1,
				AdminURL:   adminSrv.URL,
				Synced:     true,
			},
		}
		_ = k8sClient.Create(ctx, invNsTask)
		_ = k8sClient.Status().Update(ctx, invNsTask)
		_ = k8sClient.Delete(ctx, invNsTask)
		if _, err := taskReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: invNsTask.Name}}); err != nil {
			t.Fatalf("reconcile deleting invNsTask failed: %v", err)
		}
		mu.Lock()
		_, job991StillExists := jobs[991]
		mu.Unlock()
		if job991StillExists {
			t.Fatalf("EG2-08 regression: deleting XxlJobTask orphaned remote job 991 when Spec.ExecutorRef.Namespace was invalidly edited")
		}

		// (e) EG2-09: K8sJobCallbackReconciler routes in-flight Job /api/callback to originating Admin via stamped annotations
		// even after XxlJobExecutor migrated to another Admin, and never misroutes if originating Admin CR was deleted
		var noFail atomic.Bool
		origSrv, origMu, _, _, _, origCBs := setupTestAdminServer(t, &noFail)
		defer origSrv.Close()
		destSrv, destMu, _, _, _, destCBs := setupTestAdminServer(t, &noFail)
		defer destSrv.Close()

		origAdminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-orig-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: origSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "orig-tok"},
			},
		}
		destAdminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-dest-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: destSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "dest-tok"},
			},
		}
		migratedCbExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-migrated-cb-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: destAdminCR.Name}, // already migrated to destAdminCR!
				AppName:  "eg2-migrated-cb-app",
				Title:    "MigratedCbExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		_ = k8sClient.Create(ctx, origAdminCR)
		_ = k8sClient.Create(ctx, destAdminCR)
		_ = k8sClient.Create(ctx, migratedCbExec)

		inFlightJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "eg2-inflight-cb-job-1",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: migratedCbExec.Name,
					proxyexecutor.LabelJobID:        "801",
					proxyexecutor.LabelLogID:        "88001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "kube-system", // tampered cross-ns annotation must be ignored
					proxyexecutor.AnnotationExecutorName:  migratedCbExec.Name,
					proxyexecutor.AnnotationAdminName:     origAdminCR.Name,
					proxyexecutor.AnnotationAdminURL:      origSrv.URL,
					proxyexecutor.AnnotationLogID:         "88001",
					proxyexecutor.AnnotationLogDateTime:   "1700000880001",
					proxyexecutor.AnnotationCallbackSent:  "false",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded: 1,
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				},
			},
		}
		_ = k8sClient.Create(ctx, inFlightJob)
		_ = k8sClient.Status().Update(ctx, inFlightJob)

		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightJob.Name}}); err != nil {
			t.Fatalf("reconcile inFlightJob failed: %v", err)
		}
		origMu.Lock()
		gotOrig := len(*origCBs)
		origMu.Unlock()
		destMu.Lock()
		gotDest := len(*destCBs)
		destMu.Unlock()
		if gotOrig != 1 || gotDest != 0 {
			t.Fatalf("EG2-09 regression: expected in-flight Job callback routed strictly to origAdmin (gotOrig=%d, gotDest=%d)", gotOrig, gotDest)
		}

		// If origAdminCR exists but its AccessTokenSecretRef is temporarily NotFound, K8sJobCallbackReconciler MUST retry (not drop callback)!
		origAdminCR.Spec.Auth.AccessToken = ""
		origAdminCR.Spec.Auth.AccessTokenSecretRef = &xxljobv1alpha1.SecretKeyReference{Name: "temporarily-missing-secret", Key: "token"}
		_ = k8sClient.Update(ctx, origAdminCR)
		inFlightMissingSec := inFlightJob.DeepCopy()
		inFlightMissingSec.Name = "eg2-inflight-cb-missing-sec"
		inFlightMissingSec.ResourceVersion = ""
		inFlightMissingSec.Labels[proxyexecutor.LabelLogID] = "88009"
		inFlightMissingSec.Annotations[proxyexecutor.AnnotationLogID] = "88009"
		inFlightMissingSec.Annotations[proxyexecutor.AnnotationCallbackSent] = "false"
		inFlightMissingSec.Finalizers = []string{proxyexecutor.JobCallbackFinalizer}
		_ = k8sClient.Create(ctx, inFlightMissingSec)
		_ = k8sClient.Status().Update(ctx, inFlightMissingSec)
		resMissingSec, errMissingSec := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightMissingSec.Name}})
		if errMissingSec != nil || resMissingSec.RequeueAfter == 0 {
			t.Fatalf("EG2-09 regression: expected retry RequeueAfter when origAdmin Secret is temporarily NotFound, got res=%+v err=%v", resMissingSec, errMissingSec)
		}
		var checkMissingSecJob batchv1.Job
		_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: inFlightMissingSec.Name}, &checkMissingSecJob)
		if checkMissingSecJob.Annotations[proxyexecutor.AnnotationCallbackSent] == "true" {
			t.Fatalf("EG2-09 regression: callback was silently dropped when origAdmin Secret was temporarily NotFound!")
		}

		// Delete origAdminCR and reconcile a second in-flight Job from origAdmin: must NOT misroute to destAdmin!
		_ = k8sClient.Delete(ctx, origAdminCR)
		inFlightJob2 := inFlightJob.DeepCopy()
		inFlightJob2.Name = "eg2-inflight-cb-job-2"
		inFlightJob2.ResourceVersion = ""
		inFlightJob2.Labels[proxyexecutor.LabelLogID] = "88002"
		inFlightJob2.Annotations[proxyexecutor.AnnotationLogID] = "88002"
		inFlightJob2.Annotations[proxyexecutor.AnnotationCallbackSent] = "false"
		_ = k8sClient.Create(ctx, inFlightJob2)
		_ = k8sClient.Status().Update(ctx, inFlightJob2)
		if _, err := jobCbReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightJob2.Name}}); err != nil {
			t.Fatalf("reconcile inFlightJob2 after origAdmin deletion failed: %v", err)
		}
		destMu.Lock()
		gotDestAfterDel := len(*destCBs)
		destMu.Unlock()
		if gotDestAfterDel != 0 {
			t.Fatalf("EG2-09 regression: misrouted deleted origAdmin's logId onto destAdmin (%d callbacks)", gotDestAfterDel)
		}
	})
}

func TestBoostEndgameControllerConvergence(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// -------------------------------------------------------------------------
	// 1. K8sJobCallbackReconciler:
	//    - Healthy path delivers /api/callback strictly BEFORE unsuspending next SERIAL_EXECUTION Job
	//    - Transient callback failure when !HasActiveOrTerminatingPods still advances local serial queue
	//    - External force-delete (IsNotFound) clears pendingCallbackJobs AND recovers waiting serial queue
	//    - Completed/Failed Job past MaxFinalizerRetryDuration bypasses stuck pod drain / callback error
	// -------------------------------------------------------------------------
	t.Run("CallbackOrderingForceDeleteSerialRecoveryAndTimeoutBypass", func(t *testing.T) {
		var orderMu sync.Mutex
		var timeline []string
		failCB := atomic.Bool{}

		cbSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/api/callback" {
				if failCB.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("admin down"))
					return
				}
				var cbs []xxlclient.HandleCallbackParam
				_ = json.NewDecoder(r.Body).Decode(&cbs)
				for _, cb := range cbs {
					orderMu.Lock()
					timeline = append(timeline, "callback:"+strconv.FormatInt(cb.LogID, 10))
					orderMu.Unlock()
				}
				_, _ = w.Write([]byte(`{"code":200}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":200}`))
		}))
		defer cbSrv.Close()

		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "cb-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: cbSrv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "cb-strong-token"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "cb-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "cb-admin"},
				AppName:  "cb-exec-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			WithObjects(adminCR, execCR).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if j, ok := obj.(*batchv1.Job); ok && j.Name == proxyexecutor.BuildJobName("cb-exec", 42, 1002) && j.Spec.Suspend != nil && !*j.Spec.Suspend {
						orderMu.Lock()
						timeline = append(timeline, "unsuspend:"+j.Name)
						orderMu.Unlock()
					}
					return c.Update(ctx, obj, opts...)
				},
			}).
			Build()

		reconciler := &K8sJobCallbackReconciler{
			Client:    k8sCli,
			APIReader: k8sCli,
			Scheme:    scheme,
		}

		boolPtr := func(b bool) *bool { return &b }
		makeSerialJob := func(logID int64, suspended, completed bool) *batchv1.Job {
			name := proxyexecutor.BuildJobName("cb-exec", 42, logID)
			j := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:       name,
					Namespace:  "default",
					Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
					Labels: map[string]string{
						proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
						proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel("cb-exec"),
						proxyexecutor.LabelJobID:        "42",
						proxyexecutor.LabelLogID:        strconv.FormatInt(logID, 10),
					},
					Annotations: map[string]string{
						proxyexecutor.AnnotationLogID:         strconv.FormatInt(logID, 10),
						proxyexecutor.AnnotationLogDateTime:   "1700000000000",
						proxyexecutor.AnnotationCallbackSent:  "false",
						proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
						proxyexecutor.AnnotationAdminName:     "cb-admin",
						proxyexecutor.AnnotationAdminURL:      cbSrv.URL,
					},
				},
				Spec: batchv1.JobSpec{
					Suspend: boolPtr(suspended),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers:    []corev1.Container{{Name: "job-runner", Image: "busybox:1.36"}},
						},
					},
				},
			}
			if completed {
				j.Status.Succeeded = 1
				j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			}
			return j
		}

		// (1a) Healthy path: Job 1001 completed, Job 1002 suspended -> callback:1001 MUST precede unsuspend:Job1002
		job1001 := makeSerialJob(1001, false, true)
		job1002 := makeSerialJob(1002, true, false)
		_ = k8sCli.Create(ctx, job1001)
		_ = k8sCli.Status().Update(ctx, job1001)
		_ = k8sCli.Create(ctx, job1002)

		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job1001.Name}}); err != nil {
			t.Fatalf("Reconcile job1001 failed: %v", err)
		}
		orderMu.Lock()
		gotOrder := append([]string{}, timeline...)
		orderMu.Unlock()
		expectedOrder := []string{"callback:1001", "unsuspend:" + job1002.Name}
		if len(gotOrder) != 2 || gotOrder[0] != expectedOrder[0] || gotOrder[1] != expectedOrder[1] {
			t.Fatalf("expected healthy serial order %v, got %v", expectedOrder, gotOrder)
		}

		// (1b) Transient callback outage when Job 1002 finishes with 0 active/terminating pods:
		// local serial queue still unsuspends Job 1003 while Job 1002 retains finalizer & pending metric.
		failCB.Store(true)
		var fresh1002 batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: job1002.Name}, &fresh1002)
		fresh1002.Status.Succeeded = 1
		fresh1002.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		_ = k8sCli.Status().Update(ctx, &fresh1002)

		job1003 := makeSerialJob(1003, true, false)
		_ = k8sCli.Create(ctx, job1003)

		res1002, err1002 := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job1002.Name}})
		if err1002 != nil || res1002.RequeueAfter == 0 {
			t.Fatalf("expected RequeueAfter on transient callback outage, got res=%+v err=%v", res1002, err1002)
		}
		var check1003 batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: job1003.Name}, &check1003)
		if check1003.Spec.Suspend != nil && *check1003.Spec.Suspend {
			t.Fatalf("expected Job 1003 to be unsuspended locally even while Job 1002 callback retries")
		}

		// (1c) Force-delete Job 1002 & Job 1003 externally (finalizers=[]) while Job 1004 is suspended in queue:
		// Reconcile on IsNotFound MUST clear pendingCallbackJobs AND self-heal the serial queue to unsuspend Job 1004!
		job1004 := makeSerialJob(1004, true, false)
		_ = k8sCli.Create(ctx, job1004)
		for _, delName := range []string{job1002.Name, job1003.Name} {
			var dj batchv1.Job
			if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: delName}, &dj); err == nil {
				dj.Finalizers = nil
				_ = k8sCli.Update(ctx, &dj)
				_ = k8sCli.Delete(ctx, &dj)
			}
		}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job1002.Name}}); err != nil {
			t.Fatalf("Reconcile deleted job1002 failed: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job1003.Name}}); err != nil {
			t.Fatalf("Reconcile deleted job1003 failed: %v", err)
		}
		metricsMu.Lock()
		_, leakedPending := pendingCallbackJobs["default/"+job1002.Name]
		metricsMu.Unlock()
		if leakedPending {
			t.Fatalf("expected clearCallbackPendingByJobKey to clear pendingCallbackJobs for force-deleted Job 1002")
		}
		var check1004 batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: job1004.Name}, &check1004)
		if check1004.Spec.Suspend != nil && *check1004.Spec.Suspend {
			t.Fatalf("expected recoverSerialQueuesOnDeletedJob to self-heal and unsuspend Job 1004 after Job 1003 force-delete")
		}

		// (1d) Completed/Failed Job past MaxFinalizerRetryDuration bypasses stuck pod drain & callback error
		stuckJob := makeSerialJob(1005, false, false)
		oldTransition := metav1.NewTime(time.Now().Add(-MaxFinalizerRetryDuration - time.Minute))
		terminatingOne := int32(1)
		stuckJob.Status.Terminating = &terminatingOne
		stuckJob.Status.Failed = 1
		stuckJob.Status.Conditions = []batchv1.JobCondition{{
			Type:               batchv1.JobFailed,
			Status:             corev1.ConditionTrue,
			Reason:             "DeadlineExceeded",
			LastTransitionTime: oldTransition,
		}}
		_ = k8sCli.Create(ctx, stuckJob)
		_ = k8sCli.Status().Update(ctx, stuckJob)
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: stuckJob.Name}}); err != nil {
			t.Fatalf("Reconcile stuckJob failed: %v", err)
		}
		var afterStuck batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: stuckJob.Name}, &afterStuck)
		if controllerutil.ContainsFinalizer(&afterStuck, proxyexecutor.JobCallbackFinalizer) {
			t.Fatalf("expected shouldBypassCompletedJobCleanup to release JobCallbackFinalizer after MaxFinalizerRetryDuration")
		}
	})

	// -------------------------------------------------------------------------
	// 2. XxlJobAdminReconciler & admin_helper.go:
	//    - ExternalURL & ContextPath injection validation (reconciler + ResolveAdminAndClientWithReader)
	//    - Whitespace ExternalURL does NOT delete owned in-cluster Deployment/Service
	//    - 3-tier pruneTokenCacheMapLocked preserves active prevToken grace window entries
	//    - Cross-AdminRef cascade guard delays Admin deletion while repointed Executor/Task still references Status.AdminURL
	// -------------------------------------------------------------------------
	t.Run("AdminSecurityValidationWhitespaceCacheGraceAndCascadeGuard", func(t *testing.T) {
		for _, badURL := range []string{
			"http://admin:secret@xxl:8080/xxl-job-admin",
			"http://xxl:8080/xxl-job-admin?debug=1",
			"http://xxl:8080/xxl-job-admin#frag",
			"http://xxl:8080/xxl;rm",
			"file:///etc/passwd",
		} {
			if err := validateExternalAdminURL(badURL); err == nil {
				t.Fatalf("expected validateExternalAdminURL(%q) to fail", badURL)
			}
		}
		for _, badCtx := range []string{"/../admin", "/xxl--admin", "/xxl//admin", "/xxl?a=1", "/xxl;id"} {
			if _, err := normalizeAndValidateContextPath(badCtx); err == nil {
				t.Fatalf("expected normalizeAndValidateContextPath(%q) to fail", badCtx)
			}
		}

		// Whitespace ExternalURL with empty Image must deploy in-cluster Admin (NOT treat as external and delete Deployment)
		wsAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "ws-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: "   \t  ",
				Datasource:  xxljobv1alpha1.DatasourceSpec{URL: "jdbc:mysql://mysql:3306/xxl_job", Username: "root", Password: "pw"},
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "ws-strong-token"},
			},
		}
		badExtAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-ext-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: "http://user:pw@xxl:8080/xxl-job-admin",
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "bad-ext-token"},
			},
		}
		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
			WithObjects(wsAdmin, badExtAdmin).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: wsAdmin.Name}}); err != nil {
			t.Fatalf("Reconcile wsAdmin failed: %v", err)
		}
		var createdDeploy appsv1.Deployment
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: wsAdmin.Name}, &createdDeploy); err != nil {
			t.Fatalf("expected whitespace ExternalURL to create in-cluster Deployment, got err=%v", err)
		}

		// ResolveAdminAndClientWithReader must also reject bad ExternalURL
		if _, _, err := ResolveAdminAndClientWithReader(ctx, k8sCli, k8sCli, nil, xxljobv1alpha1.LocalObjectReference{Name: badExtAdmin.Name}, "default"); err == nil {
			t.Fatalf("expected ResolveAdminAndClientWithReader to reject invalid ExternalURL")
		}

		// 3-tier pruneTokenCacheMapLocked: active prevToken rotation grace entry must survive flood of negative/regular entries
		now := time.Now()
		testMap := make(map[string]cachedTokenEntry)
		testMap["grace-entry"] = cachedTokenEntry{
			token:          "new-tok",
			expiresAt:      now.Add(10 * time.Second),
			prevToken:      "old-tok",
			prevValidUntil: now.Add(5 * time.Minute),
		}
		for i := 0; i < maxTokenCacheEntries+20; i++ {
			testMap["churn-"+strconv.Itoa(i)] = cachedTokenEntry{
				token:     "tok",
				expiresAt: now.Add(25 * time.Second),
			}
			pruneTokenCacheMapLocked(testMap, now)
		}
		if _, ok := testMap["grace-entry"]; !ok {
			t.Fatalf("expected 3-tier pruneTokenCacheMapLocked to protect active prevToken rotation grace entry from churn eviction")
		}

		// Cross-AdminRef cascade guard: admin-old deletion waits while repointed executor/task Status.AdminURL still points to admin-old
		oldAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "cascade-admin-old",
				Namespace:  "default",
				Finalizers: []string{AdminFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: "http://old-admin.default.svc:8080/xxl-job-admin",
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "old-tok"},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{
				AdminURL: "http://old-admin.default.svc:8080/xxl-job-admin",
			},
		}
		repointedExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "repointed-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "cascade-admin-new"},
				AppName:  "repointed-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 5,
				AdminURL:   "http://old-admin.default.svc:8080/xxl-job-admin",
			},
		}
		repointedTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "repointed-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:  xxljobv1alpha1.LocalObjectReference{Name: "repointed-exec"},
				JobDesc:      "Migrating Task",
				Author:       "dev",
				ScheduleType: "CRON",
				ScheduleConf: "0 * * * * ?",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      88,
				JobGroupID: 5,
				AdminURL:   "http://old-admin.default.svc:8080/xxl-job-admin",
			},
		}
		_ = k8sCli.Create(ctx, oldAdmin)
		_ = k8sCli.Status().Update(ctx, oldAdmin)
		_ = k8sCli.Create(ctx, repointedExec)
		_ = k8sCli.Status().Update(ctx, repointedExec)
		_ = k8sCli.Create(ctx, repointedTask)
		_ = k8sCli.Status().Update(ctx, repointedTask)
		_ = k8sCli.Delete(ctx, oldAdmin)

		// Blocked while repointedExec and repointedTask still reference oldAdmin in Status.AdminURL
		resDel1, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: oldAdmin.Name}})
		if err != nil || resDel1.RequeueAfter == 0 {
			t.Fatalf("expected deleting oldAdmin to wait while repointedExec/repointedTask still reference oldAdmin URL, got res=%+v err=%v", resDel1, err)
		}

		// Clear repointedExec status -> STILL blocked by repointedTask!
		repointedExec.Status.JobGroupID = 0
		repointedExec.Status.AdminURL = "http://new-admin.default.svc:8080/xxl-job-admin"
		_ = k8sCli.Status().Update(ctx, repointedExec)
		resDel2, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: oldAdmin.Name}})
		if err != nil || resDel2.RequeueAfter == 0 {
			t.Fatalf("expected deleting oldAdmin to still wait while repointedTask references oldAdmin URL, got res=%+v err=%v", resDel2, err)
		}

		// Clear repointedTask status -> oldAdmin finalizer removed and caches purged!
		repointedTask.Status.JobID = 0
		repointedTask.Status.AdminURL = "http://new-admin.default.svc:8080/xxl-job-admin"
		_ = k8sCli.Status().Update(ctx, repointedTask)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: oldAdmin.Name}}); err != nil {
			t.Fatalf("Reconcile oldAdmin after dependents migrated failed: %v", err)
		}
		var checkDeleted xxljobv1alpha1.XxlJobAdmin
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: oldAdmin.Name}, &checkDeleted); !apierrors.IsNotFound(err) {
			t.Fatalf("expected oldAdmin to finish deletion once all dependents migrated, got err=%v", err)
		}
	})

	// -------------------------------------------------------------------------
	// 3. XxlJobExecutorReconciler & XxlJobTaskReconciler:
	//    - Cross-executor countReferencingTasks delays RemoveJobGroup until repointed task Status.JobGroupID migrates
	//    - prevAdminCleaned persists cleared Status when new Admin sync fails (preventing double-delete on old Admin)
	//    - adminForExecutorPredicate wakes executors when Admin transitions ReadyReplicas 0 -> 1 or Phase Pending -> Running
	// -------------------------------------------------------------------------
	t.Run("ExecutorAndTaskCrossRefStatusPersistenceAndPredicates", func(t *testing.T) {
		var noFail atomic.Bool
		srvA, muA, _, groupsA, jobsA, _ := setupTestAdminServer(t, &noFail)
		defer srvA.Close()

		adminA := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-admin-a", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvA.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "tok-a"},
			},
		}
		// Unreachable Admin B (closed server)
		closedSrv := httptest.NewServer(http.NotFoundHandler())
		closedURL := closedSrv.URL
		closedSrv.Close()
		adminBroken := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-admin-broken", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: closedURL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "tok-b"},
			},
		}

		execOld := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-exec-old", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminA.Name},
				AppName:  "boost-app-old",
				Title:    "OldExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		execNew := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-exec-new", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminA.Name},
				AppName:  "boost-app-new",
				Title:    "NewExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
			WithObjects(adminA, adminBroken, execOld, execNew).
			Build()

		execRec := &XxlJobExecutorReconciler{
			Client:                   k8sCli,
			APIReader:                k8sCli,
			Scheme:                   scheme,
			DefaultProxyExecutorAddr: "http://proxy.xxl.svc:9999/",
		}
		taskRec := &XxlJobTaskReconciler{
			Client:    k8sCli,
			APIReader: k8sCli,
			Scheme:    scheme,
		}

		// Sync both executors and create a task on execOld
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execOld.Name}})
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execNew.Name}})

		task := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-mig-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execOld.Name},
				JobDesc:         "Boost Mig Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "migHandler",
			},
		}
		_ = k8sCli.Create(ctx, task)
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task.Name}}); err != nil {
			t.Fatalf("initial task reconcile failed: %v", err)
		}

		// Repoint task.Spec.ExecutorRef -> execNew (WITHOUT reconciling task yet) and delete execOld:
		// countReferencingTasks MUST block execOld deletion because task.Status.JobGroupID still equals execOld.Status.JobGroupID!
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task.Name}, task)
		task.Spec.ExecutorRef.Name = execNew.Name
		_ = k8sCli.Update(ctx, task)
		_ = k8sCli.Delete(ctx, execOld)

		resExecDel, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execOld.Name}})
		if err != nil || resExecDel.RequeueAfter == 0 {
			t.Fatalf("expected execOld deletion to wait while repointed task Status.JobGroupID still references execOld, got res=%+v err=%v", resExecDel, err)
		}

		// Reconcile task so it migrates to execNew -> now execOld deletion succeeds cleanly!
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task.Name}}); err != nil {
			t.Fatalf("reconcile task to execNew failed: %v", err)
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execOld.Name}}); err != nil {
			t.Fatalf("reconcile execOld deletion after task migration failed: %v", err)
		}
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execOld.Name}, &xxljobv1alpha1.XxlJobExecutor{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected execOld to finish deletion after task migrated, got err=%v", err)
		}

		// Set LastExecutionJobName on task, then repoint execNew -> adminBroken:
		// execNew cleans up on adminA, fails to reach adminBroken, and MUST persist cleared JobGroupID=0 / AdminURL=""!
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task.Name}, task)
		oldTaskJobID := task.Status.JobID
		task.Status.LastExecutionJobName = "xxljob-old-run-1"
		_ = k8sCli.Status().Update(ctx, task)

		// Repoint task to another executor on adminBroken to test task prevAdminCleaned too!
		execBroken := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "boost-exec-broken-target",
				Namespace: "default",
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminBroken.Name},
				AppName:  "boost-broken-app",
				Title:    "BrokenExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 99,
				AdminURL:   closedURL,
				Synced:     true,
				Conditions: []metav1.Condition{{
					Type:               "AppNameSynced",
					Status:             metav1.ConditionTrue,
					Reason:             "AppNameSynced",
					Message:            "boost-broken-app",
					LastTransitionTime: metav1.Now(),
				}},
			},
		}
		_ = k8sCli.Create(ctx, execBroken)
		_ = k8sCli.Status().Update(ctx, execBroken)

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task.Name}, task)
		task.Spec.ExecutorRef.Name = execBroken.Name
		_ = k8sCli.Update(ctx, task)
		resTaskBroken, errTaskBroken := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task.Name}})
		if errTaskBroken != nil || resTaskBroken.RequeueAfter == 0 {
			t.Fatalf("expected task reconcile to requeue after failing to reach adminBroken, got res=%+v err=%v", resTaskBroken, errTaskBroken)
		}
		var checkTaskAfterFail xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task.Name}, &checkTaskAfterFail)
		if checkTaskAfterFail.Status.Synced || checkTaskAfterFail.Status.JobID != 0 || checkTaskAfterFail.Status.AdminURL != "" || checkTaskAfterFail.Status.LastExecutionJobName != "" {
			t.Fatalf("expected prevAdminCleaned to persist Synced=false and zeroed JobID/AdminURL/LastExecutionJobName after cleaning adminA, got %+v", checkTaskAfterFail.Status)
		}
		muA.Lock()
		_, jobStillOnA := jobsA[oldTaskJobID]
		muA.Unlock()
		if jobStillOnA {
			t.Fatalf("expected old task jobId=%d removed from adminA", oldTaskJobID)
		}

		// Now repoint execNew -> adminBroken and verify execNew also persists cleared JobGroupID=0 / AdminURL=""
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execNew.Name}, execNew)
		oldGroupID := execNew.Status.JobGroupID
		execNew.Spec.AdminRef.Name = adminBroken.Name
		_ = k8sCli.Update(ctx, execNew)
		resExecBroken, errExecBroken := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execNew.Name}})
		if errExecBroken != nil || resExecBroken.RequeueAfter == 0 {
			t.Fatalf("expected execNew reconcile to requeue after failing to reach adminBroken, got res=%+v err=%v", resExecBroken, errExecBroken)
		}
		var checkExecAfterFail xxljobv1alpha1.XxlJobExecutor
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execNew.Name}, &checkExecAfterFail)
		if checkExecAfterFail.Status.Synced || checkExecAfterFail.Status.JobGroupID != 0 || checkExecAfterFail.Status.AdminURL != "" {
			t.Fatalf("expected prevAdminCleaned on executor to persist Synced=false and zeroed JobGroupID/AdminURL, got %+v", checkExecAfterFail.Status)
		}
		muA.Lock()
		_, grpStillOnA := groupsA[oldGroupID]
		muA.Unlock()
		if grpStillOnA {
			t.Fatalf("expected old groupId=%d removed from adminA", oldGroupID)
		}

		// Verify adminForExecutorPredicate wakes on ReadyReplicas 0 -> 1 and Phase Pending -> Running,
		// while ignoring no-op status heartbeats
		pred := adminForExecutorPredicate()
		admOld := &xxljobv1alpha1.XxlJobAdmin{Status: xxljobv1alpha1.XxlJobAdminStatus{Phase: "Pending", ReadyReplicas: 0}}
		admReady := &xxljobv1alpha1.XxlJobAdmin{Status: xxljobv1alpha1.XxlJobAdminStatus{Phase: "Running", ReadyReplicas: 1}}
		admSame := &xxljobv1alpha1.XxlJobAdmin{Status: xxljobv1alpha1.XxlJobAdminStatus{Phase: "Running", ReadyReplicas: 2}}
		if !pred.Update(event.UpdateEvent{ObjectOld: admOld, ObjectNew: admReady}) {
			t.Fatalf("expected adminForExecutorPredicate to trigger when Admin transitions Pending/0 -> Running/1")
		}
		if pred.Update(event.UpdateEvent{ObjectOld: admReady, ObjectNew: admSame}) {
			t.Fatalf("expected adminForExecutorPredicate to ignore 1 -> 2 ReadyReplicas steady-state scaling")
		}
	})

	// -------------------------------------------------------------------------
	// 4. Simultaneous Admin Delete + Dependent Repoint & Shared Secret Rotation Grace Preservation:
	//    - Deleting oldAdmin while simultaneously repointing exec/task to newAdmin cleans up oldAdmin's remote DB
	//      rows (via deletingMatch fallback) before releasing AdminFinalizer
	//    - purgeAdminCaches preserves shared Secret rotation grace window (prevToken) when another active Admin
	//      shares the same SecretKeyReference
	// -------------------------------------------------------------------------
	t.Run("SimultaneousAdminDeleteAndRepointPlusSharedSecretGracePreservation", func(t *testing.T) {
		var noFail atomic.Bool
		srvOld, muOld, _, groupsOld, jobsOld, _ := setupTestAdminServer(t, &noFail)
		defer srvOld.Close()
		srvNew, _, _, _, _, _ := setupTestAdminServer(t, &noFail)
		defer srvNew.Close()

		sharedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-shared-tok-sec", Namespace: "default"},
			Data:       map[string][]byte{"token": []byte("shared-tok-v1")},
		}
		simAdminOld := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-sim-admin-old", Namespace: "default", UID: "uid-sim-old"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvOld.URL,
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: sharedSecret.Name, Key: "token"},
				},
			},
		}
		simAdminNew := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-sim-admin-new", Namespace: "default", UID: "uid-sim-new"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvNew.URL,
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: sharedSecret.Name, Key: "token"},
				},
			},
		}
		simExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-sim-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: simAdminOld.Name},
				AppName:  "boost-sim-app",
				Title:    "SimExec",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		simExecTarget := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-sim-exec-new", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: simAdminNew.Name},
				AppName:  "boost-sim-app-new",
				Title:    "SimExecNew",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
			WithObjects(sharedSecret, simAdminOld, simAdminNew, simExec, simExecTarget).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.xxl.svc:9999/"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simAdminOld.Name}})
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simAdminNew.Name}})
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simExec.Name}})
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simExecTarget.Name}})

		simTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "boost-sim-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: simExec.Name},
				JobDesc:         "Sim Task",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "simHandler",
			},
		}
		_ = k8sCli.Create(ctx, simTask)
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simTask.Name}})

		// Populate secretCache with v1, then rotate Secret to v2 so secretCache holds prevToken="shared-tok-v1"
		if err := ValidateExecutorAccessToken(ctx, k8sCli, k8sCli, "default", simExecTarget.Name, "shared-tok-v1"); err != nil {
			t.Fatalf("expected initial shared-tok-v1 validation to pass: %v", err)
		}
		sharedSecret.Data["token"] = []byte("shared-tok-v2")
		_ = k8sCli.Update(ctx, sharedSecret)
		if err := ValidateExecutorAccessToken(ctx, k8sCli, k8sCli, "default", simExecTarget.Name, "shared-tok-v2"); err != nil {
			t.Fatalf("expected rotated shared-tok-v2 validation to pass: %v", err)
		}

		// Simultaneously repoint simTask -> simExecTarget, simExec -> simAdminNew, AND delete simAdminOld!
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: simTask.Name}, simTask)
		oldSimJobID := simTask.Status.JobID
		simTask.Spec.ExecutorRef.Name = simExecTarget.Name
		_ = k8sCli.Update(ctx, simTask)

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: simExec.Name}, simExec)
		oldSimGroupID := simExec.Status.JobGroupID
		simExec.Spec.AdminRef.Name = simAdminNew.Name
		_ = k8sCli.Update(ctx, simExec)

		_ = k8sCli.Delete(ctx, simAdminOld)
		resHold, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simAdminOld.Name}})
		if err != nil || resHold.RequeueAfter == 0 {
			t.Fatalf("expected simAdminOld deletion to hold while repointed dependents still reference srvOld, got res=%+v err=%v", resHold, err)
		}

		// Reconcile simTask and simExec: even though simAdminOld has DeletionTimestamp != nil,
		// both MUST clean up their remote JobInfo and JobGroup on srvOld!
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simTask.Name}}); err != nil {
			t.Fatalf("simTask migration reconcile failed: %v", err)
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simExec.Name}}); err != nil {
			t.Fatalf("simExec migration reconcile failed: %v", err)
		}

		muOld.Lock()
		_, jobLeftOnOld := jobsOld[oldSimJobID]
		_, grpLeftOnOld := groupsOld[oldSimGroupID]
		muOld.Unlock()
		if jobLeftOnOld || grpLeftOnOld {
			t.Fatalf("expected deleting simAdminOld remote DB cleaned up before finalizer release, got jobLeft=%v grpLeft=%v", jobLeftOnOld, grpLeftOnOld)
		}

		// Now simAdminOld finishes deleting and calls purgeAdminCaches
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: simAdminOld.Name}}); err != nil {
			t.Fatalf("simAdminOld final deletion reconcile failed: %v", err)
		}
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: simAdminOld.Name}, &xxljobv1alpha1.XxlJobAdmin{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected simAdminOld deleted after dependents migrated, got err=%v", err)
		}

		// Verify sharedSecret rotation grace window (prevToken="shared-tok-v1") was NOT wiped for surviving simAdminNew / simExec!
		if err := ValidateExecutorAccessToken(ctx, k8sCli, k8sCli, "default", simExec.Name, "shared-tok-v1"); err != nil {
			t.Fatalf("expected surviving Admin's shared Secret rotation grace window for shared-tok-v1 to be preserved after deleting simAdminOld, got: %v", err)
		}
	})

	// -------------------------------------------------------------------------
	// 5. Cross-Admin Migration to Missing/Unsynced Target, Force-Deleted Originating Admin Callback Bypass,
	//    and Inline Admin Deployment Token CRLF Sanitization:
	//    - Repointing Executor/Task to a not-yet-created or unsynced new Admin/Executor cleans up oldAdmin
	//      immediately and clears Status without deadlocking oldAdmin deletion
	//    - K8sJobCallbackReconciler bypasses unreachable force-deleted originating Admin without 10m stall,
	//      and delivers callbacks via origAdminURL fallback when terminating Admin is still reachable
	//    - XxlJobAdmin Deployment strips embedded CRLF from inline AccessToken
	// -------------------------------------------------------------------------
	t.Run("CrossAdminMigrationToMissingOrUnsyncedTargetAndCallbackForceDeleteBypass", func(t *testing.T) {
		var noFail atomic.Bool
		srvOld, muOld, _, groupsOld, jobsOld, callbacksOld := setupTestAdminServer(t, &noFail)
		defer srvOld.Close()
		srvNew, _, _, _, _, _ := setupTestAdminServer(t, &noFail)
		defer srvNew.Close()

		adminOld := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-missing-admin-old", Namespace: "default", UID: "uid-mig-old"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvOld.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "old-strong-tok"},
			},
		}
		adminNew := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-unsynced-admin-new", Namespace: "default", UID: "uid-mig-new"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvNew.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "new-strong-tok"},
			},
		}
		exec1 := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-missing-exec-1", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminOld.Name},
				AppName:  "mig-missing-app-1",
				Title:    "MigExec1",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		// execUnsyncedNew points to adminNew, intentionally NOT reconciled yet (JobGroupID=0, Synced=false)
		execUnsyncedNew := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-unsynced-exec-2", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminNew.Name},
				AppName:  "mig-unsynced-app-2",
				Title:    "MigExec2",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		inlineCRLFAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "inline-crlf-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					Password: "pwd",
				},
				Auth: xxljobv1alpha1.AuthSpec{AccessToken: "  inline-tok\r\n-clean  "},
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			WithObjects(adminOld, adminNew, exec1, execUnsyncedNew, inlineCRLFAdmin).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.xxl.svc:9999/"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		// Verify inline AccessToken CRLF sanitization in Deployment env vars
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inlineCRLFAdmin.Name}})
		var crlfDeploy appsv1.Deployment
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: inlineCRLFAdmin.Name}, &crlfDeploy); err != nil {
			t.Fatalf("expected inline-crlf-admin Deployment created: %v", err)
		}
		gotEnvTok := ""
		for _, ev := range crlfDeploy.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == "XXL_JOB_ACCESSTOKEN" {
				gotEnvTok = ev.Value
			}
		}
		if gotEnvTok != "inline-tok-clean" {
			t.Fatalf("expected inline CRLF AccessToken sanitized to inline-tok-clean, got %q", gotEnvTok)
		}

		// Sync adminOld, exec1, task1 (to missing future Admin), and task2 (to unsynced execUnsyncedNew)
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminOld.Name}})
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminNew.Name}})
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: exec1.Name}})

		task1 := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-task-missing-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: exec1.Name},
				JobDesc:         "Task1",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "h1",
				TriggerStatus:   true,
			},
		}
		task2 := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "mig-task-unsynced-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: exec1.Name},
				JobDesc:         "Task2",
				Author:          "dev",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "h2",
				TriggerStatus:   true,
			},
		}
		_ = k8sCli.Create(ctx, task1)
		_ = k8sCli.Create(ctx, task2)
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task1.Name}})
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task2.Name}})

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task1.Name}, task1)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task2.Name}, task2)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: exec1.Name}, exec1)
		oldJobID1 := task1.Status.JobID
		oldJobID2 := task2.Status.JobID
		oldGrpID := exec1.Status.JobGroupID
		if oldJobID1 <= 0 || oldJobID2 <= 0 || oldGrpID <= 0 {
			t.Fatalf("expected task1/task2/exec1 synced on adminOld, got j1=%d j2=%d g=%d", oldJobID1, oldJobID2, oldGrpID)
		}

		// Repoint task2 -> execUnsyncedNew (on adminNew, still unsynced with JobGroupID=0!)
		task2.Spec.ExecutorRef.Name = execUnsyncedNew.Name
		_ = k8sCli.Update(ctx, task2)

		// Repoint exec1 -> "admin-does-not-exist-yet" (missing target Admin!) AND delete adminOld!
		exec1.Spec.AdminRef.Name = "admin-does-not-exist-yet"
		_ = k8sCli.Update(ctx, exec1)
		_ = k8sCli.Delete(ctx, adminOld)

		// adminOld must initially hold finalizer while task1/task2/exec1 still record srvOld.URL
		resHold, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminOld.Name}})
		if err != nil || resHold.RequeueAfter == 0 {
			t.Fatalf("expected adminOld deletion to hold before dependents clean up, got res=%+v err=%v", resHold, err)
		}

		// Reconcile task2 (waiting on unsynced execUnsyncedNew), task1 (target Admin missing), and exec1 (target Admin missing):
		// ALL THREE must immediately clean up srvOld and clear Status.AdminURL/JobGroupID/JobID!
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task2.Name}})
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: task1.Name}})
		_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: exec1.Name}})

		muOld.Lock()
		_, j1Left := jobsOld[oldJobID1]
		_, j2Left := jobsOld[oldJobID2]
		_, grpLeft := groupsOld[oldGrpID]
		muOld.Unlock()
		if j1Left || j2Left || grpLeft {
			t.Fatalf("expected old remote jobs/group removed from srvOld despite missing/unsynced new target, got j1=%v j2=%v grp=%v", j1Left, j2Left, grpLeft)
		}

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task1.Name}, task1)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: task2.Name}, task2)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: exec1.Name}, exec1)
		if task1.Status.AdminURL != "" || task1.Status.JobID != 0 || task2.Status.AdminURL != "" || task2.Status.JobID != 0 ||
			exec1.Status.AdminURL != "" || exec1.Status.JobGroupID != 0 {
			t.Fatalf("expected cleared previous Admin status on task1=%+v task2=%+v exec1=%+v", task1.Status, task2.Status, exec1.Status)
		}

		// Verify callback via origAdminURL fallback to terminating (still reachable) adminOld succeeds!
		cbJobURLOnly := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "cb-job-terminating-url-only",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: exec1.Name,
					proxyexecutor.LabelJobID:        "901",
					proxyexecutor.LabelLogID:        "77001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationAdminURL: srvOld.URL,
					proxyexecutor.AnnotationLogID:    "77001",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, cbJobURLOnly)
		_ = k8sCli.Status().Update(ctx, cbJobURLOnly)
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cbJobURLOnly.Name}}); err != nil {
			t.Fatalf("expected callback via origAdminURL deletingMatch fallback to succeed: %v", err)
		}
		muOld.Lock()
		gotCB := len(*callbacksOld)
		muOld.Unlock()
		if gotCB == 0 {
			t.Fatalf("expected callback delivered to terminating adminOld via origAdminURL fallback")
		}

		// Now adminOld must finish deleting cleanly WITHOUT deadlocking!
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminOld.Name}}); err != nil {
			t.Fatalf("expected adminOld deletion to converge without deadlock, got err=%v", err)
		}
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminOld.Name}, &xxljobv1alpha1.XxlJobAdmin{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected adminOld deleted cleanly, got err=%v", err)
		}

		// Verify force-deleted unreachable originating Admin does NOT stall K8sJobCallbackReconciler for 10m
		deadSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		deadURL := deadSrv.URL
		deadSrv.Close()

		forceDeadAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "force-dead-origin-admin",
				Namespace:  "default",
				Finalizers: []string{AdminFinalizer},
				Annotations: map[string]string{
					AnnotationForceDelete: "true",
				},
			},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: deadURL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "dead-tok"},
			},
		}
		_ = k8sCli.Create(ctx, forceDeadAdmin)
		_ = k8sCli.Delete(ctx, forceDeadAdmin)

		cbJobDeadAdmin := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "cb-job-force-dead-admin",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: exec1.Name,
					proxyexecutor.LabelJobID:        "902",
					proxyexecutor.LabelLogID:        "77002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationAdminName: forceDeadAdmin.Name,
					proxyexecutor.AnnotationAdminURL:  deadURL,
					proxyexecutor.AnnotationLogID:     "77002",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, cbJobDeadAdmin)
		_ = k8sCli.Status().Update(ctx, cbJobDeadAdmin)

		resDeadCB, errDeadCB := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cbJobDeadAdmin.Name}})
		if errDeadCB != nil || resDeadCB.RequeueAfter != 0 {
			t.Fatalf("expected force-deleted unreachable originating Admin callback to bypass immediately, got res=%+v err=%v", resDeadCB, errDeadCB)
		}
		var afterDeadJob batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: cbJobDeadAdmin.Name}, &afterDeadJob)
		if controllerutil.ContainsFinalizer(&afterDeadJob, proxyexecutor.JobCallbackFinalizer) {
			t.Fatalf("expected JobCallbackFinalizer removed immediately when originating Admin is force-deleted")
		}
	})
}
