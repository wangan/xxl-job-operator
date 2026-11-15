package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
	"k8s.io/apimachinery/pkg/api/meta"
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

func TestCodexReviewFixesForegroundDeleteInPlaceURLTokenNormAndServiceDNS(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// Subtest 1: RBAC manifest jobs/finalizers + Controller DeletePropagationForeground (Issue #1)
	t.Run("Issue1_RBACJobsFinalizersAndControllerForegroundDelete", func(t *testing.T) {
		rbacBytes, err := os.ReadFile(filepath.Join("..", "..", "config", "manager", "manager.yaml"))
		if err != nil {
			t.Fatalf("read config/manager/manager.yaml: %v", err)
		}
		if !strings.Contains(string(rbacBytes), "jobs/finalizers") {
			t.Fatalf("expected config/manager/manager.yaml ClusterRole to grant batch jobs/finalizers update permission")
		}

		srv, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srv.Close()

		var delMu sync.Mutex
		deletedPolicies := make(map[string]metav1.DeletionPropagation)

		admin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "fg-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "fg-tok"},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: srv.URL},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "fg-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "fg-admin"},
				AppName:  "fg-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{JobGroupID: 1, AdminURL: srv.URL, Synced: true},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "fg-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "fg-exec"},
				JobDesc:         "FG Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "fgHandler",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 500, JobGroupID: 1, AdminURL: srv.URL, Synced: true},
		}

		suspendTrue := true
		suspendFalse := false
		staleSuspendedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "fg-stale-suspended-job",
				Namespace:         "default",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
				Finalizers:        []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "fg-exec",
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "9101",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskName:      "fg-task",
					proxyexecutor.AnnotationExecutorName:  "fg-exec",
					proxyexecutor.AnnotationAdminName:     "fg-admin",
					proxyexecutor.AnnotationAdminURL:      srv.URL,
					proxyexecutor.AnnotationLogID:         "9101",
					proxyexecutor.AnnotationBlockStrategy: "DISCARD_LATER",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendTrue},
		}
		failedCreateJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "fg-failed-create-job",
				Namespace:  "default",
				UID:        types.UID("uid-fc-job"),
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "fg-exec",
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "9102",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskName:     "fg-task",
					proxyexecutor.AnnotationExecutorName: "fg-exec",
					proxyexecutor.AnnotationAdminName:    "fg-admin",
					proxyexecutor.AnnotationAdminURL:     srv.URL,
					proxyexecutor.AnnotationLogID:        "9102",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendFalse},
		}
		fcEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "fc-ev-1", Namespace: "default"},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Job",
				Namespace: "default",
				Name:      failedCreateJob.Name,
				UID:       failedCreateJob.UID,
			},
			Type:    corev1.EventTypeWarning,
			Reason:  "FailedCreate",
			Message: "pods \"fg-failed-create-job-abc\" is forbidden: violates PodSecurity \"restricted:latest\"",
			Count:   2,
		}
		activeTaskJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "fg-active-task-job",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: "fg-exec",
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "9103",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      "fg-task",
					proxyexecutor.AnnotationExecutorName:  "fg-exec",
					proxyexecutor.AnnotationAdminName:     "fg-admin",
					proxyexecutor.AnnotationAdminURL:      srv.URL,
					proxyexecutor.AnnotationLogID:         "9103",
				},
			},
			Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{Active: 1},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(admin, execCR, taskCR, staleSuspendedJob, failedCreateJob, fcEvent, activeTaskJob).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, cli client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if job, ok := obj.(*batchv1.Job); ok {
						delOpts := &client.DeleteOptions{}
						delOpts.ApplyOptions(opts)
						delMu.Lock()
						if delOpts.PropagationPolicy != nil {
							deletedPolicies[job.Name] = *delOpts.PropagationPolicy
						} else {
							deletedPolicies[job.Name] = ""
						}
						delMu.Unlock()
					}
					return cli.Delete(ctx, obj, opts...)
				},
			}).
			Build()

		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: staleSuspendedJob.Name}}); err != nil {
			t.Fatalf("reconcile staleSuspendedJob: %v", err)
		}
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: failedCreateJob.Name}}); err != nil {
			t.Fatalf("reconcile failedCreateJob: %v", err)
		}

		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		_ = k8sCli.Delete(ctx, taskCR)
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("reconcile deleting taskCR: %v", err)
		}

		delMu.Lock()
		defer delMu.Unlock()
		for _, jn := range []string{staleSuspendedJob.Name, failedCreateJob.Name, activeTaskJob.Name} {
			if deletedPolicies[jn] != metav1.DeletePropagationForeground {
				t.Fatalf("expected controller Delete on %s to pass DeletePropagationForeground, got %q", jn, deletedPolicies[jn])
			}
		}
	})

	// Subtest 2: In-place XxlJobAdmin ExternalURL modification (Issue #4)
	t.Run("Issue4_InPlaceAdminURLModificationCleansOldEndpointAndRoutesInFlightCallbacks", func(t *testing.T) {
		var failOldServer atomic.Bool
		srvOldRaw, muOld, regOld, groupsOld, jobsOld, cbsOld := setupTestAdminServer(t, nil)
		defer srvOldRaw.Close()
		srvOld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if failOldServer.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("transient old admin outage"))
				return
			}
			srvOldRaw.Config.Handler.ServeHTTP(w, r)
		}))
		defer srvOld.Close()

		srvNew, muNew, _, groupsNew, jobsNew, cbsNew := setupTestAdminServer(t, nil)
		defer srvNew.Close()

		admin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "inplace-admin",
				Namespace: "default",
				UID:       types.UID("uid-inplace-admin"),
			},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvOld.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "inplace-tok"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "inplace-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "inplace-admin"},
				AppName:  "inplace-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "inplace-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "inplace-exec"},
				JobDesc:         "Inplace URL Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "inplaceHandler",
				TriggerStatus:   true,
				K8sJobTemplate:  &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(admin, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		// Initial sync on srvOld
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: admin.Name}}); err != nil {
			t.Fatalf("initial admin reconcile: %v", err)
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("initial exec reconcile: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("initial task reconcile: %v", err)
		}
		muOld.Lock()
		if len(groupsOld) != 1 || len(jobsOld) != 1 {
			muOld.Unlock()
			t.Fatalf("expected 1 group and 1 job on srvOld, got groups=%d jobs=%d", len(groupsOld), len(jobsOld))
		}
		muOld.Unlock()

		// Spawn an in-flight batch/v1.Job triggered by srvOld BEFORE editing Spec.ExternalURL
		inFlightJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "inplace-inflight-job",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: execCR.Name,
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "66001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace: "default",
					proxyexecutor.AnnotationTaskName:      taskCR.Name,
					proxyexecutor.AnnotationExecutorName:  execCR.Name,
					proxyexecutor.AnnotationAdminName:     admin.Name,
					proxyexecutor.AnnotationAdminURL:      srvOld.URL,
					proxyexecutor.AnnotationAdminUID:      string(admin.UID),
					proxyexecutor.AnnotationLogID:         "66001",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, inFlightJob)
		_ = k8sCli.Status().Update(ctx, inFlightJob)

		// Edit admin.Spec.ExternalURL in place -> srvNew.URL, and reconcile XxlJobAdminReconciler FIRST!
		var curAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: admin.Name}, &curAdmin)
		curAdmin.Spec.ExternalURL = srvNew.URL
		curAdmin.Generation++
		_ = k8sCli.Update(ctx, &curAdmin)

		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: admin.Name}}); err != nil {
			t.Fatalf("admin reconcile after in-place ExternalURL edit: %v", err)
		}
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: admin.Name}, &curAdmin)
		if curAdmin.Status.AdminURL != srvNew.URL {
			t.Fatalf("expected Status.AdminURL=%s, got %s", srvNew.URL, curAdmin.Status.AdminURL)
		}
		prevCond := meta.FindStatusCondition(curAdmin.Status.Conditions, ConditionPreviousAdminURLs)
		if prevCond == nil || !strings.Contains(prevCond.Message, srvOld.URL) {
			t.Fatalf("expected ConditionPreviousAdminURLs to record %s, got %+v", srvOld.URL, prevCond)
		}

		// While srvOld transiently returns 500, Task migration MUST requeue without creating duplicate schedule on srvNew!
		failOldServer.Store(true)
		retryRes, retryErr := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		if retryErr == nil && retryRes.RequeueAfter == 0 {
			t.Fatalf("expected task reconcile to retry (RequeueAfter > 0 or err != nil) when previous Admin URL returns transient 500, got res=%+v", retryRes)
		}
		var failedTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &failedTask)
		if failedTask.Status.Synced {
			t.Fatalf("expected task Status.Synced=false while previous Admin cleanup is failing")
		}
		muNew.Lock()
		newJobsDuringOutage := len(jobsNew)
		muNew.Unlock()
		if newJobsDuringOutage != 0 {
			t.Fatalf("expected 0 jobs created on srvNew before srvOld cleanup succeeds, got %d", newJobsDuringOutage)
		}

		// Restore srvOld:
		// 1) In-flight Job callback MUST be delivered to srvOld (NOT srvNew!), even though admin.Status.AdminURL is srvNew!
		failOldServer.Store(false)
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightJob.Name}}); err != nil {
			t.Fatalf("in-flight callback reconcile: %v", err)
		}
		muOld.Lock()
		gotOldCBs := len(*cbsOld)
		muOld.Unlock()
		muNew.Lock()
		gotNewCBs := len(*cbsNew)
		muNew.Unlock()
		if gotOldCBs != 1 || gotNewCBs != 0 {
			t.Fatalf("expected in-flight callback routed to srvOld (1) and not srvNew (0), got old=%d new=%d", gotOldCBs, gotNewCBs)
		}

		// 2) Reconcile Executor & Task -> old JobInfo, registry, and JobGroup removed from srvOld; new group & job synced on srvNew!
		// First reconcile Task (removes old JobInfo from srvOld; waits for Executor to sync on srvNew),
		// then Executor (removes old JobGroup & registry from srvOld and creates new JobGroup on srvNew), then Task again!
		_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile to srvNew: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("task reconcile to srvNew: %v", err)
		}

		muOld.Lock()
		remOldJobs := len(jobsOld)
		remOldGroups := len(groupsOld)
		remOldRegs := len(regOld["inplace-app"])
		muOld.Unlock()
		if remOldJobs != 0 || remOldGroups != 0 || remOldRegs != 0 {
			t.Fatalf("expected srvOld completely cleaned after in-place URL edit, got jobs=%d groups=%d regs=%d",
				remOldJobs, remOldGroups, remOldRegs)
		}
		muNew.Lock()
		newGroupsCnt := len(groupsNew)
		newJobsCnt := len(jobsNew)
		muNew.Unlock()
		if newGroupsCnt != 1 || newJobsCnt != 1 {
			t.Fatalf("expected 1 group and 1 job synced on srvNew, got groups=%d jobs=%d", newGroupsCnt, newJobsCnt)
		}

		// 3) Simulate Operator restart (clearing in-memory endpointAuthCache) AFTER Executor & Task migrated to srvNew,
		// and verify a long-running in-flight Job from srvOld STILL delivers callback to srvOld via ConditionPreviousAdminURLs!
		endpointAuthMu.Lock()
		endpointAuthCache = make(map[string]cachedEndpointAuthSnapshot)
		endpointAuthOrder = nil
		endpointAuthMu.Unlock()

		lateInFlightJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "inplace-late-inflight-job",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: execCR.Name,
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "66002",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName: execCR.Name,
					proxyexecutor.AnnotationAdminName:    admin.Name,
					proxyexecutor.AnnotationAdminURL:     srvOld.URL,
					proxyexecutor.AnnotationAdminUID:     string(admin.UID),
					proxyexecutor.AnnotationLogID:        "66002",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, lateInFlightJob)
		_ = k8sCli.Status().Update(ctx, lateInFlightJob)
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: lateInFlightJob.Name}}); err != nil {
			t.Fatalf("late in-flight callback reconcile after cache flush: %v", err)
		}
		muOld.Lock()
		gotLateOldCBs := len(*cbsOld)
		muOld.Unlock()
		if gotLateOldCBs != 2 {
			t.Fatalf("expected late in-flight callback routed to srvOld via ConditionPreviousAdminURLs (total=2), got %d", gotLateOldCBs)
		}

		// 4) Switching ExternalURL back to srvOld deduplicates current URL out of ConditionPreviousAdminURLs and records srvNew
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: admin.Name}, &curAdmin)
		curAdmin.Spec.ExternalURL = srvOld.URL
		curAdmin.Generation++
		_ = k8sCli.Update(ctx, &curAdmin)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: admin.Name}}); err != nil {
			t.Fatalf("admin reconcile switching back to srvOld: %v", err)
		}
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: admin.Name}, &curAdmin)
		prevCondAfterSwitchBack := meta.FindStatusCondition(curAdmin.Status.Conditions, ConditionPreviousAdminURLs)
		if prevCondAfterSwitchBack == nil || strings.Contains(prevCondAfterSwitchBack.Message, srvOld.URL) || !strings.Contains(prevCondAfterSwitchBack.Message, srvNew.URL) {
			t.Fatalf("expected ConditionPreviousAdminURLs to contain only %s after switching back to %s, got %+v",
				srvNew.URL, srvOld.URL, prevCondAfterSwitchBack)
		}
	})

	// Subtest 3: AccessTokenSecretRef CRLF/whitespace normalization in Admin container + empty token rejection (Issue #5)
	t.Run("Issue5_AccessTokenSecretRefContainerNormalizationAndEmptyRejection", func(t *testing.T) {
		srv, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srv.Close()

		dbSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "norm-db-sec", Namespace: "default"},
			Data:       map[string][]byte{"password": []byte("dbpass")},
		}
		tokSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "norm-tok-sec", Namespace: "default"},
			Data:       map[string][]byte{"token": []byte("  \tsec-admin-tok\r\n  ")},
		}
		admin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "norm-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image: "xuxueli/xxl-job-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					PasswordSecretRef: &xxljobv1alpha1.SecretKeyReference{
						Name: "norm-db-sec",
						Key:  "password",
					},
				},
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{
						Name: "norm-tok-sec",
						Key:  "token",
					},
				},
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(dbSec, tokSec, admin).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: admin.Name}}); err != nil {
			t.Fatalf("reconcile in-cluster admin with CRLF-padded Secret token: %v", err)
		}

		var deploy appsv1.Deployment
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: admin.Name}, &deploy); err != nil {
			t.Fatalf("get admin deployment: %v", err)
		}
		cmdSlice := deploy.Spec.Template.Spec.Containers[0].Command
		if len(cmdSlice) < 3 || cmdSlice[0] != "/bin/sh" || cmdSlice[1] != "-c" {
			t.Fatalf("expected /bin/sh -c container Command wrapper on Admin Deployment, got %+v", cmdSlice)
		}

		// Execute the exact container Command wrapper with a stub `java` binary in PATH and verify normalized env vars!
		binDir := t.TempDir()
		stubJava := filepath.Join(binDir, "java")
		stubScript := "#!/bin/sh\nprintf '%s|%s|%s' \"$XXL_JOB_ACCESSTOKEN\" \"$XXL_JOB_ACCESS_TOKEN\" \"$*\"\n"
		if err := os.WriteFile(stubJava, []byte(stubScript), 0o755); err != nil {
			t.Fatalf("write stub java: %v", err)
		}
		shEnv := []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"JAVA_OPTS=-Xmx256m",
		}
		for _, ev := range deploy.Spec.Template.Spec.Containers[0].Env {
			if ev.ValueFrom != nil && ev.ValueFrom.SecretKeyRef != nil && ev.ValueFrom.SecretKeyRef.Name == tokSec.Name {
				shEnv = append(shEnv, ev.Name+"="+string(tokSec.Data[ev.ValueFrom.SecretKeyRef.Key]))
			} else if ev.Value != "" {
				shEnv = append(shEnv, ev.Name+"="+ev.Value)
			}
		}
		shCmd := exec.Command(cmdSlice[0], cmdSlice[1:]...)
		shCmd.Env = shEnv
		out, err := shCmd.CombinedOutput()
		if err != nil {
			t.Fatalf("execute buildAdminContainerCommand failed: %v (out=%q)", err, string(out))
		}
		expectedOut := "sec-admin-tok|sec-admin-tok|-Xmx256m -jar /app.jar --server.port=8080 --server.servlet.context-path=/xxl-job-admin"
		if string(out) != expectedOut {
			t.Fatalf("expected normalized Admin java invocation %q, got %q", expectedOut, string(out))
		}

		// Verify whitespace-only Secret is rejected by BOTH XxlJobAdminReconciler and ResolveAdminAndClientWithReader
		tokSec.Data["token"] = []byte("  \r\n\t  ")
		_ = k8sCli.Update(ctx, tokSec)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: admin.Name}}); err == nil {
			t.Fatalf("expected XxlJobAdminReconciler to reject whitespace-only AccessTokenSecretRef")
		}
		if _, _, err := ResolveAdminAndClientWithReader(ctx, k8sCli, k8sCli, nil, xxljobv1alpha1.LocalObjectReference{Name: admin.Name}, "default"); err == nil {
			t.Fatalf("expected ResolveAdminAndClientWithReader to reject whitespace-only AccessTokenSecretRef")
		}
	})

	// Subtest 4: UseServiceDNS backend-status-linked registry maintenance & heartbeats (Issue #6)
	t.Run("Issue6_UseServiceDNSBackendLinkedRegistryAndHeartbeats", func(t *testing.T) {
		var regCalls atomic.Int32
		srvRaw, mu, registries, groups, _, _ := setupTestAdminServer(t, nil)
		defer srvRaw.Close()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/registry" {
				regCalls.Add(1)
			}
			srvRaw.Config.Handler.ServeHTTP(w, r)
		}))
		defer srv.Close()

		admin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "dns-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "dns-tok"},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: srv.URL},
		}
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dns-exec-svc", Namespace: "default"},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "dns-worker"},
				Ports:    []corev1.ServicePort{{Port: 9999}},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "dns-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:      xxljobv1alpha1.LocalObjectReference{Name: "dns-admin"},
				AppName:       "dns-worker-app",
				Mode:          xxljobv1alpha1.ExecutorModeResident,
				UseServiceDNS: true,
				ServiceName:   "dns-exec-svc",
				ExecutorPort:  9999,
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(admin, svc, execCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}).
			Build()

		execRec := &XxlJobExecutorReconciler{Client: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		hbRunner := &RegistryHeartbeatRunner{Client: k8sCli, APIReader: k8sCli}
		dnsURL := "http://dns-exec-svc.default.svc.cluster.local:9999/"

		// Phase 1: 0 backends -> sentinel address_list, ConditionServiceBackendActive=False, 0 registry heartbeats
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("Phase 1 reconcile (0 backends): %v", err)
		}
		_ = hbRunner.RunOnce(ctx)
		if regCalls.Load() != 0 {
			t.Fatalf("expected 0 /api/registry calls when Service has 0 backends, got %d", regCalls.Load())
		}
		var curExec xxljobv1alpha1.XxlJobExecutor
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &curExec)
		cond := meta.FindStatusCondition(curExec.Status.Conditions, ConditionServiceBackendActive)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NoActiveBackends" {
			t.Fatalf("expected ConditionServiceBackendActive=False/NoActiveBackends, got %+v", cond)
		}
		mu.Lock()
		grp1 := groups[curExec.Status.JobGroupID]
		mu.Unlock()
		if !strings.Contains(grp1.AddressList, proxyexecutor.UnavailableExecutorSentinel) {
			t.Fatalf("expected _unavailable sentinel in address_list when 0 backends, got %q", grp1.AddressList)
		}

		// Phase 2: 1 ready backend Pod + Endpoints -> dnsURL in address_list & registered + heartbeated by RunOnce
		backendPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dns-worker-0",
				Namespace: "default",
				Labels:    map[string]string{"app": "dns-worker"},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.244.1.25",
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
		ep := &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "dns-exec-svc", Namespace: "default"},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{
					IP:        "10.244.1.25",
					TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "dns-worker-0"},
				}},
			}},
		}
		_ = k8sCli.Create(ctx, backendPod)
		_ = k8sCli.Create(ctx, ep)

		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("Phase 2 reconcile (1 ready backend): %v", err)
		}
		beforeHB := regCalls.Load()
		if beforeHB == 0 {
			t.Fatalf("expected Reconcile to register %s in /api/registry when backend becomes ready", dnsURL)
		}
		if err := hbRunner.RunOnce(ctx); err != nil {
			t.Fatalf("Phase 2 RunOnce: %v", err)
		}
		if regCalls.Load() <= beforeHB {
			t.Fatalf("expected RegistryHeartbeatRunner.RunOnce to refresh %s heartbeat (%d -> %d)", dnsURL, beforeHB, regCalls.Load())
		}
		mu.Lock()
		isRegistered := registries["dns-worker-app"][dnsURL]
		grp2 := groups[curExec.Status.JobGroupID]
		mu.Unlock()
		if !isRegistered || grp2.AddressList != dnsURL {
			t.Fatalf("expected dnsURL registered=%v and address_list=%q, got %q", isRegistered, dnsURL, grp2.AddressList)
		}

		// Phase 3: Backend transitions to draining (PodReady=False, NotReadyAddresses) ->
		// address_list switches immediately to _unavailable sentinel, while keep-alive heartbeat continues!
		backendPod.Status.Conditions = []corev1.PodCondition{{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
		}}
		_ = k8sCli.Status().Update(ctx, backendPod)
		ep.Subsets = []corev1.EndpointSubset{{
			NotReadyAddresses: []corev1.EndpointAddress{{
				IP:        "10.244.1.25",
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "dns-worker-0"},
			}},
		}}
		_ = k8sCli.Update(ctx, ep)

		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("Phase 3 reconcile (draining backend): %v", err)
		}
		beforeDrainHB := regCalls.Load()
		_ = hbRunner.RunOnce(ctx)
		if regCalls.Load() <= beforeDrainHB {
			t.Fatalf("expected RegistryHeartbeatRunner.RunOnce to keep-alive heartbeat %s during drain", dnsURL)
		}
		mu.Lock()
		grp3 := groups[curExec.Status.JobGroupID]
		stillRegDuringDrain := registries["dns-worker-app"][dnsURL]
		mu.Unlock()
		if !strings.Contains(grp3.AddressList, proxyexecutor.UnavailableExecutorSentinel) || !stillRegDuringDrain {
			t.Fatalf("expected sentinel address_list (%q) + active registry keep-alive (%v) during drain", grp3.AddressList, stillRegDuringDrain)
		}

		// Phase 4: Last backend exits -> RunOnce stops heartbeating immediately AND Reconcile removes dnsURL from xxl_job_registry!
		_ = k8sCli.Delete(ctx, backendPod)
		ep.Subsets = nil
		_ = k8sCli.Update(ctx, ep)

		callsBeforeEmptyHB := regCalls.Load()
		_ = hbRunner.RunOnce(ctx)
		if regCalls.Load() != callsBeforeEmptyHB {
			t.Fatalf("expected RegistryHeartbeatRunner.RunOnce to skip heartbeat immediately when 0 backends remain")
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("Phase 4 reconcile (0 backends after exit): %v", err)
		}
		mu.Lock()
		regAfterExit := registries["dns-worker-app"][dnsURL]
		mu.Unlock()
		if regAfterExit {
			t.Fatalf("expected %s removed from xxl_job_registry via /api/registryRemove when all backends exited", dnsURL)
		}

		// Phase 5: ServiceTypeExternalName (no Selector, no Endpoints object) MUST be treated as active (readyCount=1)!
		extSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "ext-dns-svc", Namespace: "default"},
			Spec: corev1.ServiceSpec{
				Type:         corev1.ServiceTypeExternalName,
				ExternalName: "executor.corp.internal",
			},
		}
		_ = k8sCli.Create(ctx, extSvc)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &curExec)
		curExec.Spec.ServiceName = "ext-dns-svc"
		_ = k8sCli.Update(ctx, &curExec)

		extDNSURL := "http://ext-dns-svc.default.svc.cluster.local:9999/"
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("Phase 5 reconcile (ExternalName service): %v", err)
		}
		beforeExtHB := regCalls.Load()
		if err := hbRunner.RunOnce(ctx); err != nil {
			t.Fatalf("Phase 5 RunOnce (ExternalName): %v", err)
		}
		if regCalls.Load() <= beforeExtHB {
			t.Fatalf("expected RunOnce to heartbeat ExternalName Service %s", extDNSURL)
		}
		mu.Lock()
		grpExt := groups[curExec.Status.JobGroupID]
		regExt := registries["dns-worker-app"][extDNSURL]
		mu.Unlock()
		if grpExt.AddressList != extDNSURL || !regExt {
			t.Fatalf("expected ExternalName Service address_list=%q and registered=%v, got %q", extDNSURL, regExt, grpExt.AddressList)
		}
	})

	// Subtest 5: Inline AccessToken preserves native ENTRYPOINT, resolvePreviousAdminClient guards,
	// and XxlJobTask deletion cleans previous Admin on broken target ref + scopes LabelJobID fallback to AdminURL
	t.Run("ReviewRound2_InlineEntrypoint_PrevClientGuards_And_TaskDeletionIsolation", func(t *testing.T) {
		// (a) Inline AccessToken (no AccessTokenSecretRef) MUST leave Containers[0].Command nil
		dbSecInline := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "inline-db-sec", Namespace: "default"},
			Data:       map[string][]byte{"password": []byte("dbpass")},
		}
		adminInline := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "inline-admin", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image: "custom/distroless-xxl-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:               "jdbc:mysql://mysql:3306/xxl_job",
					Username:          "root",
					PasswordSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: "inline-db-sec", Key: "password"},
				},
				Auth: xxljobv1alpha1.AuthSpec{
					AccessToken: "  inline-tok\r\n ",
				},
			},
		}
		k8sInline := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(dbSecInline, adminInline).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}).
			Build()
		inlineRec := &XxlJobAdminReconciler{Client: k8sInline, APIReader: k8sInline, Scheme: scheme}
		if _, err := inlineRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminInline.Name}}); err != nil {
			t.Fatalf("reconcile inline-token admin: %v", err)
		}
		var inlineDeploy appsv1.Deployment
		if err := k8sInline.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminInline.Name}, &inlineDeploy); err != nil {
			t.Fatalf("get inline admin deployment: %v", err)
		}
		if len(inlineDeploy.Spec.Template.Spec.Containers[0].Command) != 0 {
			t.Fatalf("expected Containers[0].Command to remain unset when AccessTokenSecretRef is nil, got %+v",
				inlineDeploy.Spec.Template.Spec.Containers[0].Command)
		}

		// (b) resolvePreviousAdminClient rejects whitespace-only AccessTokenSecretRef & resolves pre-upgrade svc host via fallbackAdminName
		emptyTokSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "prev-ws-sec", Namespace: "default"},
			Data:       map[string][]byte{"token": []byte(" \t\r\n ")},
		}
		prevAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "pre-upg-admin", Namespace: "default", UID: types.UID("uid-pre")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Port: 8081, // edited in place from 8080 -> 8081 while Status.AdminURL was empty pre-upgrade
				Auth: xxljobv1alpha1.AuthSpec{
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: "prev-ws-sec", Key: "token"},
				},
			},
		}
		k8sPrev := fake.NewClientBuilder().WithScheme(scheme).WithObjects(emptyTokSec, prevAdmin).Build()
		oldInternalURL := "http://pre-upg-admin.default.svc.cluster.local:8080/xxl-job-admin"
		matchedPrev, prevCli, prevErr := resolvePreviousAdminClient(ctx, k8sPrev, k8sPrev, nil, "default", oldInternalURL, nil, "pre-upg-admin")
		if matchedPrev == nil || prevErr == nil || prevCli != nil {
			t.Fatalf("expected resolvePreviousAdminClient to match pre-upg-admin via fallbackAdminName and reject whitespace-only Secret, got admin=%v cli=%v err=%v",
				matchedPrev, prevCli, prevErr)
		}
		emptyTokSec.Data["token"] = []byte("valid-prev-tok\n")
		_ = k8sPrev.Update(ctx, emptyTokSec)
		matchedPrev2, prevCli2, prevErr2 := resolvePreviousAdminClient(ctx, k8sPrev, k8sPrev, nil, "default", oldInternalURL, nil, "pre-upg-admin")
		if prevErr2 != nil || matchedPrev2 == nil || prevCli2 == nil {
			t.Fatalf("expected resolvePreviousAdminClient to succeed once Secret has non-empty token, got admin=%v cli=%v err=%v",
				matchedPrev2, prevCli2, prevErr2)
		}

		// (c) XxlJobTask deletion when execCR.Spec.AdminRef points to a missing Admin still cleans previous Admin schedule immediately,
		// AND cleanupActiveTaskJobs scopes LabelJobID fallback to task.Status.AdminURL + executor!
		srvA, muA, _, _, jobsA, _ := setupTestAdminServer(t, nil)
		defer srvA.Close()

		adminTaskA := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "task-del-admin-a", Namespace: "default", UID: types.UID("uid-del-a")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvA.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "del-tok"},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: srvA.URL},
		}
		execTask := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "task-del-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "task-del-admin-a"},
				AppName:  "task-del-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskDel := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "task-del-mig", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "task-del-exec"},
				JobDesc:         "Delete During Broken Migration",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				TriggerStatus:   true,
				ExecutorHandler: "delHandler",
			},
		}
		k8sDel := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(adminTaskA, execTask, taskDel).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		execRecDel := &XxlJobExecutorReconciler{Client: k8sDel, APIReader: k8sDel, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRecDel := &XxlJobTaskReconciler{Client: k8sDel, APIReader: k8sDel, Scheme: scheme}
		_, _ = execRecDel.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execTask.Name}})
		_, _ = taskRecDel.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskDel.Name}})

		var syncedDelTask xxljobv1alpha1.XxlJobTask
		_ = k8sDel.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskDel.Name}, &syncedDelTask)
		oldJobID := syncedDelTask.Status.JobID
		if oldJobID <= 0 {
			t.Fatalf("expected task-del-mig synced on admin-a, got status=%+v", syncedDelTask.Status)
		}

		// Create 2 active Jobs WITHOUT AnnotationTaskName sharing LabelJobID=oldJobID:
		// 1) jobOwnedOldAdmin from srvA.URL -> MUST be deleted when task-del-mig is deleted
		// 2) jobForeignAdmin from http://foreign-admin:8080/xxl-job-admin -> MUST NOT be deleted!
		jobIDStr := strconv.Itoa(oldJobID)
		suspendFalse := false
		jobOwnedOldAdmin := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-owned-old-admin",
				Namespace: "default",
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execTask.Name),
					proxyexecutor.LabelJobID:        jobIDStr,
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName: execTask.Name,
					proxyexecutor.AnnotationAdminURL:     srvA.URL,
				},
			},
			Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{Active: 1},
		}
		jobForeignAdmin := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-foreign-admin",
				Namespace: "default",
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execTask.Name),
					proxyexecutor.LabelJobID:        jobIDStr,
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName: execTask.Name,
					proxyexecutor.AnnotationAdminURL:     "http://foreign-admin:8080/xxl-job-admin",
				},
			},
			Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{Active: 1},
		}
		_ = k8sDel.Create(ctx, jobOwnedOldAdmin)
		_ = k8sDel.Create(ctx, jobForeignAdmin)

		// Point execTask.Spec.AdminRef to a nonexistent "missing-target-admin", then delete task-del-mig!
		var curExecDel xxljobv1alpha1.XxlJobExecutor
		_ = k8sDel.Get(ctx, types.NamespacedName{Namespace: "default", Name: execTask.Name}, &curExecDel)
		curExecDel.Spec.AdminRef.Name = "missing-target-admin"
		_ = k8sDel.Update(ctx, &curExecDel)

		_ = k8sDel.Delete(ctx, &syncedDelTask)
		// First reconcile: target Admin is missing, yet task-del-mig MUST immediately stop & delete oldJobID on srvA!
		_, _ = taskRecDel.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskDel.Name}})
		muA.Lock()
		_, stillOnAdminA := jobsA[oldJobID]
		muA.Unlock()
		if stillOnAdminA {
			t.Fatalf("expected XxlJobTask deletion to immediately remove jobId=%d from previous healthy Admin even when target AdminRef is missing", oldJobID)
		}

		// Restore AdminRef (or set force-delete) so deletion completes cleanupActiveTaskJobs; restore status fields on a fresh task
		// to test cleanupActiveTaskJobs scoping directly as well:
		testScopeTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "task-del-mig", Namespace: "default"},
			Spec:       taskDel.Spec,
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:    oldJobID,
				AdminURL: srvA.URL,
			},
		}
		if err := taskRecDel.cleanupActiveTaskJobs(ctx, k8sDel, testScopeTask); err != nil {
			t.Fatalf("cleanupActiveTaskJobs: %v", err)
		}
		if err := k8sDel.Get(ctx, types.NamespacedName{Namespace: "default", Name: "job-owned-old-admin"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected job-owned-old-admin (%s) to be deleted by cleanupActiveTaskJobs, got err=%v", srvA.URL, err)
		}
		if err := k8sDel.Get(ctx, types.NamespacedName{Namespace: "default", Name: "job-foreign-admin"}, &batchv1.Job{}); err != nil {
			t.Fatalf("expected job-foreign-admin (different AdminURL) NOT to be deleted by cleanupActiveTaskJobs, got err=%v", err)
		}
	})
}

func TestRound2SyncedEndpointJSONManagedTransitionsRestartAuthAndHardening(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// Subtest 1: Real end-to-end ConditionAdminEndpointSynced JSON wire format written by controllers and consumed by ProxyExecutor
	t.Run("EndToEndControllerToProxyExecutorSyncedEndpointJSON", func(t *testing.T) {
		srvA, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srvA.Close()
		srvB, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srvB.Close()

		adminA := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-admin-a", Namespace: "default", UID: types.UID("uid-e2e-a")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvA.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "e2e-tok"},
			},
		}
		adminB := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-admin-b", Namespace: "default", UID: types.UID("uid-e2e-b")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvB.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "e2e-tok"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-proxy-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "e2e-admin-a"},
				AppName:  "e2e-proxy-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-proxy-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "e2e-proxy-exec"},
				JobDesc:         "E2E JSON Wire Format Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				ExecutorHandler: "e2eHandler",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image:   "alpine:3.19",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"echo ok"},
				},
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(adminA, adminB, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		for _, n := range []string{adminA.Name, adminB.Name} {
			for i := 0; i < 2; i++ {
				if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: n}}); err != nil {
					t.Fatalf("reconcile admin %s: %v", n, err)
				}
			}
		}
		for i := 0; i < 2; i++ {
			if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
				t.Fatalf("reconcile exec: %v", err)
			}
		}
		for i := 0; i < 2; i++ {
			if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
				t.Fatalf("reconcile task: %v", err)
			}
		}

		var syncedTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		jobID := syncedTask.Status.JobID
		if jobID <= 0 {
			t.Fatalf("expected positive JobID after real controller reconcile, got %+v", syncedTask.Status)
		}

		proxySrv := proxyexecutor.NewServer(k8sCli, ":0", "default", "")
		proxySrv.ValidateToken = func(vCtx context.Context, ns, execName, tok string) error {
			return ValidateExecutorAccessToken(vCtx, k8sCli, k8sCli, ns, execName, tok)
		}
		ts := httptest.NewServer(proxySrv.Handler())
		defer ts.Close()

		postRun := func(p xxlclient.TriggerParam) xxlclient.ReturnT {
			t.Helper()
			b, _ := json.Marshal(p)
			req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/proxy/default/%s/run", ts.URL, execCR.Name), strings.NewReader(string(b)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "e2e-tok")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST /run failed: %v", err)
			}
			defer resp.Body.Close()
			var ret xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&ret)
			return ret
		}

		// 1) Pre-reconcile Spec.AdminRef switch (e2e-admin-a -> e2e-admin-b BEFORE controller reconciles Status.AdminURL):
		// ProxyExecutor MUST decode real JSON ConditionAdminEndpointSynced written by XxlJobTaskReconciler and stamp e2e-admin-a + uid-e2e-a!
		var liveExec xxljobv1alpha1.XxlJobExecutor
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &liveExec)
		liveExec.Spec.AdminRef.Name = "e2e-admin-b"
		_ = k8sCli.Update(ctx, &liveExec)

		if ret := postRun(xxlclient.TriggerParam{
			JobID:                 jobID,
			ExecutorHandler:       "e2eHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 701,
			LogDateTime:           1700000000701,
		}); !ret.IsSuccess() {
			t.Fatalf("expected pre-reconcile SERIAL trigger to succeed, got %+v", ret)
		}
		var preRecJob batchv1.Job
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: proxyexecutor.BuildJobName(execCR.Name, jobID, 701)}, &preRecJob); err != nil {
			t.Fatalf("get preRecJob: %v", err)
		}
		if preRecJob.Annotations[proxyexecutor.AnnotationAdminName] != "e2e-admin-a" ||
			preRecJob.Annotations[proxyexecutor.AnnotationAdminUID] != "uid-e2e-a" ||
			preRecJob.Annotations[proxyexecutor.AnnotationAdminURL] != strings.TrimRight(srvA.URL, "/") {
			t.Fatalf("expected pre-reconcile Job to stamp full JSON-decoded admin-a origin, got %+v", preRecJob.Annotations)
		}

		// 2) Complete controller migration to e2e-admin-b, simulate GC'd LastExecutionJobName=999999 on uid-e2e-b,
		// then recreate same-name/same-URL e2e-admin-b with new UID uid-e2e-b-v2 before controller reconciles Task:
		// COVER_EARLY logId=5 MUST read uid-e2e-b from real JSON ConditionAdminEndpointSynced and NOT be discarded!
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("migrate exec to admin-b: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("migrate task to admin-b: %v", err)
		}
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		jobIDOnB := syncedTask.Status.JobID
		syncedTask.Status.LastExecutionJobName = proxyexecutor.BuildJobName(execCR.Name, jobIDOnB, 999999)
		_ = k8sCli.Status().Update(ctx, &syncedTask)

		var curAdminB xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: "e2e-admin-b"}, &curAdminB)
		curAdminB.Finalizers = nil
		_ = k8sCli.Update(ctx, &curAdminB)
		_ = k8sCli.Delete(ctx, &curAdminB)
		recreatedB := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-admin-b", Namespace: "default", UID: types.UID("uid-e2e-b-v2")},
			Spec:       adminB.Spec,
			Status:     curAdminB.Status,
		}
		_ = k8sCli.Create(ctx, recreatedB)
		purgeExecutorTokenCache("default", execCR.Name)

		if ret := postRun(xxlclient.TriggerParam{
			JobID:                 jobIDOnB,
			ExecutorHandler:       "e2eHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 5,
			LogDateTime:           1700000005000,
		}); !ret.IsSuccess() {
			t.Fatalf("expected COVER_EARLY logId=5 on recreated same-URL Admin (uid-e2e-b-v2) to succeed over real controller JSON, got %+v", ret)
		}
	})

	// Subtest 2: Managed XxlJobAdmin in-place Spec.Port / Spec.ContextPath edit (same DB) vs Datasource.URL change
	t.Run("ManagedAdminInPlacePortAndContextPathSmoothTransitionAndCallbackRouting", func(t *testing.T) {
		sharedBackend, sharedMu, sharedReg, sharedGroups, sharedJobs, sharedCBs := setupTestAdminServer(t, nil)
		defer sharedBackend.Close()

		secondDBBackend, secondMu, _, secondGroups, secondJobs, _ := setupTestAdminServer(t, nil)
		defer secondDBBackend.Close()

		oldClusterURL := "http://mgr-inp-admin.default.svc.cluster.local:8080/xxl-job-admin"
		newClusterURL := "http://mgr-inp-admin.default.svc.cluster.local:8081/scheduler"
		thirdClusterURL := "http://mgr-inp-admin.default.svc.cluster.local:8082/scheduler"

		var activePort int32 = 8080
		var activePath = "/xxl-job-admin"
		var activeDB = "db1"
		var deadOldEndpointHits atomic.Int32

		routerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			activeRoute := fmt.Sprintf("/port%d%s", atomic.LoadInt32(&activePort), activePath)
			if !strings.HasPrefix(r.URL.Path, activeRoute+"/") && r.URL.Path != activeRoute {
				deadOldEndpointHits.Add(1)
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("connection refused on replaced in-cluster Service port/contextPath"))
				return
			}
			r2 := r.Clone(r.Context())
			r2.URL.Path = strings.TrimPrefix(r.URL.Path, activeRoute)
			if r2.URL.Path == "" {
				r2.URL.Path = "/"
			}
			if activeDB == "db1" {
				sharedBackend.Config.Handler.ServeHTTP(w, r2)
			} else {
				secondDBBackend.Config.Handler.ServeHTTP(w, r2)
			}
		}))
		defer routerSrv.Close()

		factory := func(cfg xxlclient.Config) (*xxlclient.Client, error) {
			var routePrefix string
			switch strings.TrimRight(cfg.BaseURL, "/") {
			case oldClusterURL:
				routePrefix = "/port8080/xxl-job-admin"
			case newClusterURL:
				routePrefix = "/port8081/scheduler"
			case thirdClusterURL:
				routePrefix = "/port8082/scheduler"
			default:
				return nil, fmt.Errorf("unexpected BaseURL %q", cfg.BaseURL)
			}
			routedCfg := cfg
			routedCfg.BaseURL = routerSrv.URL + routePrefix
			return xxlclient.NewClient(routedCfg)
		}

		managedAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "mgr-inp-admin", Namespace: "default", UID: types.UID("uid-mgr-inp")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image:       "xuxueli/xxl-job-admin:2.4.1",
				Port:        8080,
				ContextPath: "/xxl-job-admin",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					Password: "pwd",
				},
				Auth: xxljobv1alpha1.AuthSpec{AccessToken: "mgr-inp-tok"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "mgr-inp-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "mgr-inp-admin"},
				AppName:  "mgr-inp-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "mgr-inp-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "mgr-inp-exec"},
				JobDesc:         "Managed InPlace Port Edit Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				TriggerStatus:   true,
				ExecutorHandler: "mgrInpHandler",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image:   "alpine:3.19",
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{"echo ok"},
				},
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(managedAdmin, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory}
		hbRunner := &RegistryHeartbeatRunner{Client: k8sCli, APIReader: k8sCli, ClientFactory: factory}

		for i := 0; i < 2; i++ {
			if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}}); err != nil {
				t.Fatalf("initial admin reconcile: %v", err)
			}
		}
		var dep appsv1.Deployment
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}, &dep)
		dep.Status.ReadyReplicas = 1
		_ = k8sCli.Status().Update(ctx, &dep)
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}})

		for i := 0; i < 2; i++ {
			if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
				t.Fatalf("initial exec reconcile: %v", err)
			}
		}
		for i := 0; i < 2; i++ {
			if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
				t.Fatalf("initial task reconcile: %v", err)
			}
		}

		var syncedExec xxljobv1alpha1.XxlJobExecutor
		var syncedTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &syncedExec)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		initialGroupID := syncedExec.Status.JobGroupID
		initialJobID := syncedTask.Status.JobID
		if initialGroupID <= 0 || initialJobID <= 0 {
			t.Fatalf("expected initialGroupID>0 and initialJobID>0, got group=%d job=%d", initialGroupID, initialJobID)
		}

		// Spawn an in-flight batch/v1.Job BEFORE modifying Spec.Port and Spec.ContextPath
		proxySrv := proxyexecutor.NewServer(k8sCli, ":0", "default", "")
		proxySrv.ValidateToken = func(vCtx context.Context, ns, execName, tok string) error {
			return ValidateExecutorAccessToken(vCtx, k8sCli, k8sCli, ns, execName, tok)
		}
		ts := httptest.NewServer(proxySrv.Handler())
		defer ts.Close()
		trigBody, _ := json.Marshal(xxlclient.TriggerParam{
			JobID:           initialJobID,
			ExecutorHandler: "mgrInpHandler",
			LogID:           8801,
			LogDateTime:     1700000008801,
		})
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/proxy/default/%s/run", ts.URL, execCR.Name), strings.NewReader(string(trigBody)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "mgr-inp-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("trigger in-flight job before port edit: %v", err)
		}
		resp.Body.Close()
		inFlightJobName := proxyexecutor.BuildJobName(execCR.Name, initialJobID, 8801)

		// Now modify Spec.Port (8080 -> 8081) and Spec.ContextPath (/xxl-job-admin -> /scheduler) in place with unchanged Datasource.URL!
		var curAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}, &curAdmin)
		curAdmin.Spec.Port = 8081
		curAdmin.Spec.ContextPath = "/scheduler"
		curAdmin.Generation++
		_ = k8sCli.Update(ctx, &curAdmin)

		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}}); err != nil {
			t.Fatalf("reconcile admin after port/contextPath edit: %v", err)
		}
		// Immediately switch live cluster Service port/path to 8081 + /scheduler (old 8080/xxl-job-admin now refuses connections!)
		atomic.StoreInt32(&activePort, 8081)
		activePath = "/scheduler"

		// Heartbeat runner BEFORE ExecutorReconciler runs must still heartbeat to updated 8081/scheduler
		sharedMu.Lock()
		clear(sharedReg)
		sharedMu.Unlock()
		hbRunner.RunOnce(ctx)
		sharedMu.Lock()
		hbDelivered := len(sharedReg["mgr-inp-app"]) > 0
		sharedMu.Unlock()
		if !hbDelivered {
			t.Fatalf("expected RegistryHeartbeatRunner to keep heartbeating during same-DB managed Port/ContextPath transition")
		}

		// Reconcile Executor & Task: MUST NOT hit dead 8080/xxl-job-admin and MUST preserve existing JobGroupID & JobID in shared DB!
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("reconcile exec after port/contextPath edit: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("reconcile task after port/contextPath edit: %v", err)
		}
		if hits := deadOldEndpointHits.Load(); hits != 0 {
			t.Fatalf("expected 0 requests to dead old cluster Service port/path, got %d", hits)
		}

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &syncedExec)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		if syncedExec.Status.AdminURL != newClusterURL || syncedExec.Status.JobGroupID != initialGroupID || !syncedExec.Status.Synced {
			t.Fatalf("expected Executor to smoothly switch to %s preserving JobGroupID=%d, got %+v", newClusterURL, initialGroupID, syncedExec.Status)
		}
		if syncedTask.Status.AdminURL != newClusterURL || syncedTask.Status.JobID != initialJobID || !syncedTask.Status.Synced {
			t.Fatalf("expected Task to smoothly switch to %s preserving JobID=%d, got %+v", newClusterURL, initialJobID, syncedTask.Status)
		}
		sharedMu.Lock()
		_, groupStillPresent := sharedGroups[initialGroupID]
		_, jobStillPresent := sharedJobs[initialJobID]
		sharedMu.Unlock()
		if !groupStillPresent || !jobStillPresent {
			t.Fatalf("expected shared DB JobGroup (%v) and JobInfo (%v) never to be deleted across Port/ContextPath edit", groupStillPresent, jobStillPresent)
		}

		// Complete the in-flight Job created BEFORE the port/contextPath edit: callback MUST route to updated 8081/scheduler!
		var inFlightJob batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: inFlightJobName}, &inFlightJob)
		inFlightJob.Status.Succeeded = 1
		inFlightJob.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		_ = k8sCli.Status().Update(ctx, &inFlightJob)
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightJobName}}); err != nil {
			t.Fatalf("callback reconcile for in-flight job across port edit: %v", err)
		}
		if hits := deadOldEndpointHits.Load(); hits != 0 {
			t.Fatalf("expected callback NOT to hit dead old port, got %d hits", hits)
		}
		sharedMu.Lock()
		gotCBCount := len(*sharedCBs)
		var gotCBLogID int64
		if gotCBCount > 0 {
			gotCBLogID = (*sharedCBs)[0].LogID
		}
		sharedMu.Unlock()
		if gotCBCount != 1 || gotCBLogID != 8801 {
			t.Fatalf("expected in-flight callback logId=8801 delivered to updated managed endpoint, got count=%d logId=%d", gotCBCount, gotCBLogID)
		}

		// Contrast: Modifying Spec.Port (8081 -> 8082) AND Spec.Datasource.URL (db1 -> db2) MUST reset IDs and register in db2 without deadlocking on dead 8081!
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}, &curAdmin)
		curAdmin.Spec.Port = 8082
		curAdmin.Spec.Datasource.URL = "jdbc:mysql://mysql-v2:3306/xxl_job_v2"
		curAdmin.Generation++
		_ = k8sCli.Update(ctx, &curAdmin)
		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: managedAdmin.Name}})
		activeDB = "db2"
		atomic.StoreInt32(&activePort, 8082)

		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("reconcile exec after Datasource.URL + Port change: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("reconcile task after Datasource.URL + Port change: %v", err)
		}
		if hits := deadOldEndpointHits.Load(); hits != 0 {
			t.Fatalf("expected 0 requests to dead port 8081 when Datasource.URL also changed, got %d", hits)
		}
		secondMu.Lock()
		db2Groups := len(secondGroups)
		db2Jobs := len(secondJobs)
		secondMu.Unlock()
		if db2Groups != 1 || db2Jobs != 1 {
			t.Fatalf("expected fresh JobGroup and JobInfo created in new Datasource DB, got groups=%d jobs=%d", db2Groups, db2Jobs)
		}
	})

	// Subtest 3: Managed XxlJobAdmin switching to pure external mode (ExternalURL != "" && Image == "") never deadlocks on deleted Service
	t.Run("ManagedToPureExternalModeSwitchNeverDeadlocksOnDeletedClusterService", func(t *testing.T) {
		srvExt, muExt, _, extGroups, extJobs, extCBs := setupTestAdminServer(t, nil)
		defer srvExt.Close()

		oldSvcURL := "http://mode-switch-admin.default.svc.cluster.local:8080/xxl-job-admin"
		switchAdmin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "mode-switch-admin", Namespace: "default", UID: types.UID("uid-mode-switch")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image: "xuxueli/xxl-job-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{
					URL:      "jdbc:mysql://mysql:3306/xxl_job",
					Username: "root",
					Password: "pwd",
				},
				Auth: xxljobv1alpha1.AuthSpec{AccessToken: "switch-tok"},
			},
		}
		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(switchAdmin).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		for i := 0; i < 2; i++ {
			if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: switchAdmin.Name}}); err != nil {
				t.Fatalf("reconcile managed switchAdmin: %v", err)
			}
		}
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: switchAdmin.Name}, switchAdmin)
		syncedMetaMsg := encodeSyncedAdminEndpointMeta(switchAdmin, oldSvcURL)

		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "mode-switch-exec",
				Namespace:  "default",
				Finalizers: []string{ExecutorFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: switchAdmin.Name},
				AppName:  "mode-switch-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID:          77,
				AdminURL:            oldSvcURL,
				RegisteredAddresses: []string{"http://proxy.default.svc:9999/proxy/default/mode-switch-exec/"},
				Synced:              true,
				Conditions: []metav1.Condition{{
					Type:               ConditionAdminEndpointSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "EndpointSynced",
					Message:            syncedMetaMsg,
					LastTransitionTime: metav1.Now(),
				}},
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "mode-switch-task",
				Namespace:  "default",
				Finalizers: []string{TaskFinalizer},
			},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Mode Switch Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				TriggerStatus:   true,
				ExecutorHandler: "switchHandler",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      999,
				JobGroupID: 77,
				AdminURL:   oldSvcURL,
				Synced:     true,
				Conditions: []metav1.Condition{{
					Type:               ConditionAdminEndpointSynced,
					Status:             metav1.ConditionTrue,
					Reason:             "EndpointSynced",
					Message:            syncedMetaMsg,
					LastTransitionTime: metav1.Now(),
				}},
			},
		}
		suspendFalse := false
		orphanOldSvcJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-modeswitch-999-55",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execCR.Name),
					proxyexecutor.LabelJobID:        "999",
					proxyexecutor.LabelLogID:        "55",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationTaskNamespace:       "default",
					proxyexecutor.AnnotationTaskName:            taskCR.Name,
					proxyexecutor.AnnotationExecutorName:        execCR.Name,
					proxyexecutor.AnnotationAdminName:           switchAdmin.Name,
					proxyexecutor.AnnotationAdminUID:            string(switchAdmin.UID),
					proxyexecutor.AnnotationAdminURL:            oldSvcURL,
					proxyexecutor.AnnotationAdminDatasourceHash: proxyexecutor.DatasourceURLHash("jdbc:mysql://mysql:3306/xxl_job"),
					proxyexecutor.AnnotationLogID:               "55",
					proxyexecutor.AnnotationLogDateTime:         "1700000000055",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, execCR)
		_ = k8sCli.Create(ctx, taskCR)
		_ = k8sCli.Create(ctx, orphanOldSvcJob)

		// Switch XxlJobAdmin to pure ExternalURL mode (ExternalURL != "" && Image == "") -> deletes Deployment & Service!
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: switchAdmin.Name}, switchAdmin)
		switchAdmin.Spec.Image = ""
		switchAdmin.Spec.ExternalURL = srvExt.URL
		switchAdmin.Generation++
		_ = k8sCli.Update(ctx, switchAdmin)

		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: switchAdmin.Name}}); err != nil {
			t.Fatalf("reconcile admin on pure external switch: %v", err)
		}
		if err := k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: switchAdmin.Name}, &corev1.Service{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected owned Service deleted on pure external switch, got %v", err)
		}

		// Note: ClientFactory is nil (uses real HTTP client), so ANY attempt to dial mode-switch-admin.default.svc.cluster.local would fail!
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile after pure external switch must not deadlock on deleted Service: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("task reconcile after pure external switch must not deadlock on deleted Service: %v", err)
		}

		var gotExec xxljobv1alpha1.XxlJobExecutor
		var gotTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &gotExec)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &gotTask)
		if !gotExec.Status.Synced || gotExec.Status.AdminURL != strings.TrimRight(srvExt.URL, "/") {
			t.Fatalf("expected Executor migrated to external URL %s without deadlock, got %+v", srvExt.URL, gotExec.Status)
		}
		if !gotTask.Status.Synced || gotTask.Status.AdminURL != strings.TrimRight(srvExt.URL, "/") {
			t.Fatalf("expected Task migrated to external URL %s without deadlock, got %+v", srvExt.URL, gotTask.Status)
		}
		muExt.Lock()
		extGCount := len(extGroups)
		extJCount := len(extJobs)
		muExt.Unlock()
		if extGCount != 1 || extJCount != 1 {
			t.Fatalf("expected JobGroup and JobInfo registered on external Admin, got groups=%d jobs=%d", extGCount, extJCount)
		}

		// Reconcile the completed Job from the deleted internal Service: must NOT deadlock on DNS and must NOT misroute callback to srvExt!
		res, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: orphanOldSvcJob.Name}})
		if err != nil || res.RequeueAfter != 0 {
			t.Fatalf("expected callback reconcile on decommissioned internal Service Job to finish cleanly without retry, got res=%+v err=%v", res, err)
		}
		var cleanedJob batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: orphanOldSvcJob.Name}, &cleanedJob)
		if controllerutil.ContainsFinalizer(&cleanedJob, proxyexecutor.JobCallbackFinalizer) {
			t.Fatalf("expected JobCallbackFinalizer released on decommissioned internal Service Job")
		}
		muExt.Lock()
		extCBCount := len(*extCBs)
		muExt.Unlock()
		if extCBCount != 0 {
			t.Fatalf("expected 0 callbacks misrouted to new external Admin, got %d", extCBCount)
		}
	})

	// Subtest 4: Operator restart (endpointAuthCache cleared) when old Admin had empty Username (default "admin") & nil SecretRefs
	// and new Spec simultaneously modified ExternalURL + Username + PasswordSecretRef + AccessTokenSecretRef
	t.Run("OperatorRestartAuthFallbackPreservesDefaultUsernameAndNilSecretRefs", func(t *testing.T) {
		srvOldRaw, muOld, _, groupsOld, jobsOld, cbsOld := setupTestAdminServer(t, nil)
		defer srvOldRaw.Close()
		var oldAuthFailures atomic.Int32
		srvOld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				_ = r.ParseForm()
				if r.FormValue("userName") != "admin" || r.FormValue("password") != "123456" {
					oldAuthFailures.Add(1)
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte("wrong login credentials on srvOld"))
					return
				}
			}
			if strings.HasPrefix(r.URL.Path, "/api/") {
				if r.Header.Get(xxlclient.AccessTokenHeader) != "old-inline-tok" {
					oldAuthFailures.Add(1)
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte("wrong access token on srvOld"))
					return
				}
			}
			srvOldRaw.Config.Handler.ServeHTTP(w, r)
		}))
		defer srvOld.Close()

		srvNewRaw, muNew, _, groupsNew, jobsNew, _ := setupTestAdminServer(t, nil)
		defer srvNewRaw.Close()
		srvNew := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				_ = r.ParseForm()
				if r.FormValue("userName") != "new-super-admin" || r.FormValue("password") != "new-secret-pwd" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
			}
			if strings.HasPrefix(r.URL.Path, "/api/") {
				if r.Header.Get(xxlclient.AccessTokenHeader) != "new-secret-tok" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
			}
			srvNewRaw.Config.Handler.ServeHTTP(w, r)
		}))
		defer srvNew.Close()

		newCredSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "new-admin-creds", Namespace: "default"},
			Data: map[string][]byte{
				"password": []byte("new-secret-pwd"),
				"token":    []byte("new-secret-tok"),
			},
		}
		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "restart-auth-admin", Namespace: "default", UID: types.UID("uid-restart-auth")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvOld.URL,
				Auth: xxljobv1alpha1.AuthSpec{
					// Intentionally empty Username (default "admin") and nil PasswordSecretRef/AccessTokenSecretRef!
					AccessToken: "old-inline-tok",
				},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "restart-auth-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
				AppName:  "restart-auth-app",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "restart-auth-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Restart Auth Recovery Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 * * * * ?",
				TriggerStatus:   true,
				ExecutorHandler: "restartHandler",
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(newCredSec, adminCR, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, DefaultProxyExecutorAddr: "http://proxy.default.svc:9999"}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		for i := 0; i < 2; i++ {
			_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
			_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}})
			_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		}

		suspendFalse := false
		inFlightOldJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "xxljob-restartauth-500-91",
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execCR.Name),
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "91",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName: execCR.Name,
					proxyexecutor.AnnotationAdminName:    adminCR.Name,
					proxyexecutor.AnnotationAdminUID:     string(adminCR.UID),
					proxyexecutor.AnnotationAdminURL:     srvOld.URL,
					proxyexecutor.AnnotationLogID:        "91",
					proxyexecutor.AnnotationLogDateTime:  "1700000000091",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, inFlightOldJob)

		// Simultaneously edit ExternalURL AND switch Username="new-super-admin" + PasswordSecretRef + AccessTokenSecretRef
		// (keeping inline AccessToken="old-inline-tok" untouched as fallback when AccessTokenSecretRef is nil on old meta)
		var liveAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.ExternalURL = srvNew.URL
		liveAdmin.Spec.Auth.Username = "new-super-admin"
		liveAdmin.Spec.Auth.Password = "inline-new-pwd-that-must-not-leak"
		liveAdmin.Spec.Auth.PasswordSecretRef = &xxljobv1alpha1.SecretKeyReference{Name: newCredSec.Name, Key: "password"}
		liveAdmin.Spec.Auth.AccessTokenSecretRef = &xxljobv1alpha1.SecretKeyReference{Name: newCredSec.Name, Key: "token"}
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}}); err != nil {
			t.Fatalf("reconcile admin after ExternalURL+Auth update: %v", err)
		}

		// Simulate Operator restart: wipe in-memory endpointAuthCache and clientCache completely!
		endpointAuthMu.Lock()
		clear(endpointAuthCache)
		endpointAuthOrder = nil
		endpointAuthMu.Unlock()
		clientCacheMu.Lock()
		clear(clientCache)
		clear(clientCacheByAdmin)
		clientCacheOrder = nil
		clientCacheMu.Unlock()

		// Reconcile Task, Executor, and Callback after simulated Operator restart:
		// MUST authenticate to srvOld with Username="admin", Password="123456" (PasswordSecretRef=nil), Token="old-inline-tok" (AccessTokenSecretRef=nil)!
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("task reconcile after restart failed (leaked new credentials onto old endpoint?): %v", err)
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile after restart failed (leaked new credentials onto old endpoint?): %v", err)
		}
		// Run second task reconcile now that Executor has synced JobGroupID on srvNew
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("second task reconcile on srvNew failed: %v", err)
		}
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: inFlightOldJob.Name}}); err != nil {
			t.Fatalf("callback reconcile after restart failed: %v", err)
		}

		if failures := oldAuthFailures.Load(); failures != 0 {
			t.Fatalf("expected 0 auth failures on srvOld after Operator restart, got %d (new Username/SecretRef leaked!)", failures)
		}
		muOld.Lock()
		remOldGroups := len(groupsOld)
		remOldJobs := len(jobsOld)
		gotOldCBs := len(*cbsOld)
		muOld.Unlock()
		if remOldGroups != 0 || remOldJobs != 0 || gotOldCBs != 1 {
			t.Fatalf("expected srvOld cleaned (groups=%d, jobs=%d) and 1 in-flight callback delivered (cbs=%d)", remOldGroups, remOldJobs, gotOldCBs)
		}
		muNew.Lock()
		gotNewGroups := len(groupsNew)
		gotNewJobs := len(jobsNew)
		muNew.Unlock()
		if gotNewGroups != 1 || gotNewJobs != 1 {
			t.Fatalf("expected srvNew populated with migrated JobGroup (%d) and JobInfo (%d)", gotNewGroups, gotNewJobs)
		}
	})

	// Subtest 5: K8sJobCallbackReconciler holds finalizer/serial queue while FinalizerDeleteDependents is present,
	// and buildAdminContainerCommand passes -DLOG_HOME="$LOG_HOME" when LOG_HOME is set
	t.Run("CallbackForegroundDeletionFinalizerGuardAndBuildAdminLogHome", func(t *testing.T) {
		srv, mu, _, _, _, cbs := setupTestAdminServer(t, nil)
		defer srv.Close()

		admin := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "fg-cb-admin", Namespace: "default", UID: types.UID("uid-fg-cb")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srv.URL,
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "fg-cb-tok"},
			},
			Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: srv.URL},
		}
		now := metav1.Now()
		suspendFalse := false
		suspendTrue := true
		delJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              proxyexecutor.BuildJobName("fg-cb-exec", 44, 1),
				Namespace:         "default",
				DeletionTimestamp: &now,
				Finalizers:        []string{proxyexecutor.JobCallbackFinalizer, metav1.FinalizerDeleteDependents},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel("fg-cb-exec"),
					proxyexecutor.LabelJobID:        "44",
					proxyexecutor.LabelLogID:        "1",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName:  "fg-cb-exec",
					proxyexecutor.AnnotationAdminName:     admin.Name,
					proxyexecutor.AnnotationAdminUID:      string(admin.UID),
					proxyexecutor.AnnotationAdminURL:      srv.URL,
					proxyexecutor.AnnotationLogID:         "1",
					proxyexecutor.AnnotationLogDateTime:   "1700000000001",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
				},
			},
			Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{Active: 0, Terminating: nil},
		}
		nextJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       proxyexecutor.BuildJobName("fg-cb-exec", 44, 2),
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel("fg-cb-exec"),
					proxyexecutor.LabelJobID:        "44",
					proxyexecutor.LabelLogID:        "2",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName:  "fg-cb-exec",
					proxyexecutor.AnnotationAdminName:     admin.Name,
					proxyexecutor.AnnotationAdminUID:      string(admin.UID),
					proxyexecutor.AnnotationAdminURL:      srv.URL,
					proxyexecutor.AnnotationLogID:         "2",
					proxyexecutor.AnnotationLogDateTime:   "1700000000002",
					proxyexecutor.AnnotationBlockStrategy: "SERIAL_EXECUTION",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendTrue},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(admin, delJob, nextJob).
			WithStatusSubresource(&batchv1.Job{}).
			Build()
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		// Reconcile while FinalizerDeleteDependents ("foregroundDeletion") is still present:
		// Callback is delivered once, JobCallbackFinalizer is retained (RequeueAfter > 0), and nextJob stays suspended!
		res1, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delJob.Name}})
		if err != nil || res1.RequeueAfter == 0 {
			t.Fatalf("expected RequeueAfter > 0 while foregroundDeletion finalizer is still present, got res=%+v err=%v", res1, err)
		}
		var checkNext batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: nextJob.Name}, &checkNext)
		if checkNext.Spec.Suspend == nil || !*checkNext.Spec.Suspend {
			t.Fatalf("expected next SERIAL_EXECUTION Job to stay suspended while foregroundDeletion is still present")
		}
		mu.Lock()
		cbLen := len(*cbs)
		mu.Unlock()
		if cbLen != 1 {
			t.Fatalf("expected 1 callback delivered for deleting Job, got %d", cbLen)
		}

		// Simulate GC removing FinalizerDeleteDependents once dependent Pods are deleted:
		var liveDel batchv1.Job
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: delJob.Name}, &liveDel)
		liveDel.Finalizers = []string{proxyexecutor.JobCallbackFinalizer}
		_ = k8sCli.Update(ctx, &liveDel)

		res2, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delJob.Name}})
		if err != nil || res2.RequeueAfter != 0 {
			t.Fatalf("expected clean completion once foregroundDeletion is removed, got res=%+v err=%v", res2, err)
		}
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: nextJob.Name}, &checkNext)
		if checkNext.Spec.Suspend != nil && *checkNext.Spec.Suspend {
			t.Fatalf("expected next SERIAL_EXECUTION Job to unsuspend once foregroundDeletion was removed")
		}

		// Verify buildAdminContainerCommand passes -DLOG_HOME="$LOG_HOME" as a single argument even with spaces in LOG_HOME
		binDir := t.TempDir()
		stubJava := filepath.Join(binDir, "java")
		stubScript := "#!/bin/sh\nfor arg in \"$@\"; do printf '[%s]' \"$arg\"; done\n"
		if err := os.WriteFile(stubJava, []byte(stubScript), 0o755); err != nil {
			t.Fatalf("write stub java: %v", err)
		}
		cmdSlice := buildAdminContainerCommand()
		shCmd := exec.Command(cmdSlice[0], cmdSlice[1:]...)
		shCmd.Env = []string{
			"PATH=" + binDir + ":" + os.Getenv("PATH"),
			"JAVA_OPTS=-Xms128m",
			"LOG_HOME=/data/xxl logs",
			"PARAMS=--server.port=8080",
		}
		out, err := shCmd.CombinedOutput()
		if err != nil {
			t.Fatalf("execute buildAdminContainerCommand with LOG_HOME: %v (out=%q)", err, string(out))
		}
		expectedArgs := "[-Xms128m][-DLOG_HOME=/data/xxl logs][-jar][/app.jar][--server.port=8080]"
		if string(out) != expectedArgs {
			t.Fatalf("expected java args %q, got %q", expectedArgs, string(out))
		}
	})
}

func TestAdversarialReviewManagedSameURLDBSwapHybridAndRestartAuthRotation(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// Subtest 1: Same-URL Spec.Datasource.URL replacement on a managed XxlJobAdmin (db1 -> db2, Port & ContextPath unchanged)
	// MUST NOT delete unrelated JobGroup IDs in db2, MUST NOT overwrite unmarked manual JobInfo IDs in db2,
	// MUST reset LastExecutionJobName, and MUST NOT misroute in-flight db1 callbacks into db2!
	t.Run("SameURLManagedDatasourceSwapNeverCorruptsUnrelatedRowsOrCallbacksInNewDB", func(t *testing.T) {
		srvDB1, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srvDB1.Close()
		srvDB2, mu2, _, groups2, jobs2, cbs2 := setupTestAdminServer(t, nil)
		defer srvDB2.Close()

		// Seed db1 via client so nextGroupID advances and execCR gets JobGroupID=3 and taskCR gets JobID=500
		seedCli1, _ := xxlclient.NewClient(xxlclient.Config{BaseURL: srvDB1.URL, Username: "admin", Password: "123456", AccessToken: "sameurl-tok"})
		_, _ = seedCli1.EnsureJobGroup(ctx, xxlclient.JobGroup{AppName: "seed-1", Title: "Seed 1", AddressType: 1, AddressList: "http://127.0.0.1:9999/"})
		_, _ = seedCli1.EnsureJobGroup(ctx, xxlclient.JobGroup{AppName: "seed-2", Title: "Seed 2", AddressType: 1, AddressList: "http://127.0.0.1:9999/"})

		// Seed db2 via client with 3 unrelated JobGroups (so id=3 in db2 is "unrelated-critical-group" and nextGroupID=4!)
		// AND an unmarked manual task at id=500 ("Manual Billing Job" without [xjt:...] tag)!
		seedCli2, _ := xxlclient.NewClient(xxlclient.Config{BaseURL: srvDB2.URL, Username: "admin", Password: "123456", AccessToken: "sameurl-tok"})
		_, _ = seedCli2.EnsureJobGroup(ctx, xxlclient.JobGroup{AppName: "db2-other-1", Title: "DB2 Other 1", AddressType: 1, AddressList: "http://127.0.0.1:9999/"})
		_, _ = seedCli2.EnsureJobGroup(ctx, xxlclient.JobGroup{AppName: "db2-other-2", Title: "DB2 Other 2", AddressType: 1, AddressList: "http://127.0.0.1:9999/"})
		_, _ = seedCli2.EnsureJobGroup(ctx, xxlclient.JobGroup{AppName: "unrelated-critical-group", Title: "Critical Group", AddressType: 1, AddressList: "http://127.0.0.1:9999/"})
		_, _ = seedCli2.AddJobInfo(ctx, xxlclient.JobInfo{
			JobGroup:        3,
			JobDesc:         "Manual Billing Job",
			Author:          "finance",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "manualBilling",
			GlueType:        "BEAN",
		})

		db1URL := "jdbc:mysql://mysql-db1:3306/xxl_job"
		db2URL := "jdbc:mysql://mysql-db2:3306/xxl_job_v2"
		svcURL := "http://sameurl-db-admin.default.svc.cluster.local:8080/xxl-job-admin"

		var activeBackend atomic.Int32
		activeBackend.Store(1)

		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "sameurl-db-admin", Namespace: "default", UID: types.UID("uid-sameurl-db")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image:       "xuxueli/xxl-job-admin:2.4.1",
				Port:        8080,
				ContextPath: "/xxl-job-admin",
				Datasource:  xxljobv1alpha1.DatasourceSpec{URL: db1URL, Username: "root", Password: "pwd"},
				Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "sameurl-tok"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "sameurl-db-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
				AppName:  "my-managed-app",
				Title:    "My Managed App",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "sameurl-db-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Managed Declarative Task",
				Author:          "sre",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 */5 * * * ?",
				ExecutorHandler: "managedHandler",
			},
		}
		delTaskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "sameurl-del-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Task Deleted After DB Swap",
				Author:          "sre",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 */10 * * * ?",
				ExecutorHandler: "delHandler",
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(adminCR, execCR, taskCR, delTaskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		routingFactory := func(cfg xxlclient.Config) (*xxlclient.Client, error) {
			if strings.TrimRight(cfg.BaseURL, "/") != svcURL {
				return nil, fmt.Errorf("unexpected baseURL %s", cfg.BaseURL)
			}
			cp := cfg
			if activeBackend.Load() == 1 {
				cp.BaseURL = srvDB1.URL
			} else {
				cp.BaseURL = srvDB2.URL
			}
			return xxlclient.NewClient(cp)
		}

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: routingFactory}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: routingFactory}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: routingFactory}

		for i := 0; i < 2; i++ {
			_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
			_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}})
			_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
			_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delTaskCR.Name}})
		}

		var syncedExec xxljobv1alpha1.XxlJobExecutor
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &syncedExec)
		if syncedExec.Status.JobGroupID != 3 {
			t.Fatalf("expected db1 JobGroupID=3, got %d", syncedExec.Status.JobGroupID)
		}
		var syncedTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		if syncedTask.Status.JobID != 500 {
			t.Fatalf("expected db1 JobID=500, got %d", syncedTask.Status.JobID)
		}
		syncedTask.Status.LastExecutionJobName = proxyexecutor.BuildJobName(execCR.Name, 500, 999999)
		_ = k8sCli.Status().Update(ctx, &syncedTask)

		// Also seed an unmarked manual job at delTaskCR.Status.JobID (501) in db2 via client so nextJobID advances to 502!
		var syncedDelTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: delTaskCR.Name}, &syncedDelTask)
		seededDelID, _ := seedCli2.AddJobInfo(ctx, xxlclient.JobInfo{
			JobGroup:        3,
			JobDesc:         "Unmarked Manual Job In Group 3",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 2 * * ?",
			ExecutorHandler: "manualOps",
			GlueType:        "BEAN",
		})
		if seededDelID != syncedDelTask.Status.JobID {
			t.Fatalf("expected seeded manual job ID=%d in db2 to collide with delTaskCR JobID=%d", seededDelID, syncedDelTask.Status.JobID)
		}

		// Create an in-flight Job from db1 before swapping Datasource.URL
		suspendFalse := false
		db1Job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       proxyexecutor.BuildJobName(execCR.Name, 500, 7001),
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execCR.Name),
					proxyexecutor.LabelJobID:        "500",
					proxyexecutor.LabelLogID:        "7001",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName:        execCR.Name,
					proxyexecutor.AnnotationAdminName:           adminCR.Name,
					proxyexecutor.AnnotationAdminUID:            string(adminCR.UID),
					proxyexecutor.AnnotationAdminURL:            svcURL,
					proxyexecutor.AnnotationAdminDatasourceHash: proxyexecutor.DatasourceURLHash(db1URL),
					proxyexecutor.AnnotationLogID:               "7001",
					proxyexecutor.AnnotationLogDateTime:         "1700000007001",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, db1Job)

		// Swap ONLY Spec.Datasource.URL (db1 -> db2) on sameurl-db-admin (Port=8080 and ContextPath="/xxl-job-admin" unchanged!)
		var liveAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.Datasource.URL = db2URL
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		activeBackend.Store(2)

		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}}); err != nil {
			t.Fatalf("reconcile admin after Datasource.URL swap: %v", err)
		}

		// Delete delTaskCR BEFORE Executor reconciles against db2: MUST NOT delete unmarked manual job in db2!
		_ = k8sCli.Delete(ctx, &syncedDelTask)
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: delTaskCR.Name}}); err != nil {
			t.Fatalf("reconcile deleting task after Datasource.URL swap: %v", err)
		}

		// Reconcile taskCR BEFORE execCR syncs db2: taskCR MUST wait for execCR to sync JobGroupID on db2!
		waitRes, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		if err != nil || waitRes.RequeueAfter == 0 {
			t.Fatalf("expected taskCR to wait for execCR to sync against db2 first, got res=%+v err=%v", waitRes, err)
		}

		// Now reconcile execCR and taskCR on db2
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("reconcile execCR on db2: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("reconcile taskCR on db2: %v", err)
		}

		// Reconcile db1Job callback: MUST NOT deliver db1 logId=7001 into db2!
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: db1Job.Name}}); err != nil {
			t.Fatalf("reconcile db1Job callback after db swap: %v", err)
		}

		mu2.Lock()
		unrelatedGroup3, hasGroup3 := groups2[3]
		manualJob500, hasManual500 := jobs2[500]
		manualDelID, hasManualDelID := jobs2[syncedDelTask.Status.JobID]
		gotDB2CBs := len(*cbs2)
		mu2.Unlock()

		if !hasGroup3 || unrelatedGroup3.AppName != "unrelated-critical-group" {
			t.Fatalf("CRITICAL BUG: unrelated JobGroup 3 in db2 was deleted or overwritten: has=%v grp=%+v", hasGroup3, unrelatedGroup3)
		}
		if !hasManual500 || manualJob500.JobDesc != "Manual Billing Job" || manualJob500.ExecutorHandler != "manualBilling" {
			t.Fatalf("CRITICAL BUG: unmarked manual JobInfo 500 in db2 was overwritten: has=%v job=%+v", hasManual500, manualJob500)
		}
		if !hasManualDelID || manualDelID.JobDesc != "Unmarked Manual Job In Group 3" {
			t.Fatalf("CRITICAL BUG: deleting taskCR deleted unmarked manual job %d in db2: has=%v job=%+v", syncedDelTask.Status.JobID, hasManualDelID, manualDelID)
		}
		if gotDB2CBs != 0 {
			t.Fatalf("CRITICAL BUG: in-flight db1 callback was misrouted into db2 (%d callbacks)", gotDB2CBs)
		}

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: execCR.Name}, &syncedExec)
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		if syncedExec.Status.JobGroupID != 4 {
			t.Fatalf("expected execCR to create its own JobGroupID=4 in db2, got %d", syncedExec.Status.JobGroupID)
		}
		if syncedTask.Status.JobID == 500 || syncedTask.Status.JobID <= 0 {
			t.Fatalf("expected taskCR to create a new JobInfo distinct from manual 500 in db2, got %d", syncedTask.Status.JobID)
		}
		if syncedTask.Status.LastExecutionJobName != "" {
			t.Fatalf("expected LastExecutionJobName from db1 to be reset on db2 sync, got %q", syncedTask.Status.LastExecutionJobName)
		}
	})

	// Subtest 2: Hybrid Managed Mode (ExternalURL != "" && Image != "") <-> Cluster-Local <-> Pure External (Image == "")
	t.Run("HybridManagedTransitionsAndHybridToPureExternalSwitch", func(t *testing.T) {
		srvManaged, muMgr, _, mgrGroups, mgrJobs, mgrCBs := setupTestAdminServer(t, nil)
		defer srvManaged.Close()
		srvExt, muExt, _, extGroups, extJobs, _ := setupTestAdminServer(t, nil)
		defer srvExt.Close()

		svcURL := "http://hybrid-admin.default.svc.cluster.local:8080/xxl-job-admin"
		hybridV1 := "https://xxl-v1.corp.internal/xxl-job-admin"
		hybridV2 := "https://xxl-v2.corp.internal/xxl-job-admin"

		var currentLiveManagedURL atomic.Value
		currentLiveManagedURL.Store(svcURL)
		var retiredURLHits atomic.Int32

		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "hybrid-admin", Namespace: "default", UID: types.UID("uid-hybrid-1")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				Image:      "xuxueli/xxl-job-admin:2.4.1",
				Datasource: xxljobv1alpha1.DatasourceSpec{URL: "jdbc:mysql://mysql:3306/xxl_job", Username: "root", Password: "pwd"},
				Auth:       xxljobv1alpha1.AuthSpec{AccessToken: "hybrid-tok"},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "hybrid-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
				AppName:  "hybrid-app",
				Title:    "Hybrid App",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "hybrid-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Hybrid Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 */5 * * * ?",
				ExecutorHandler: "hybridHandler",
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(adminCR, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			Build()

		factory := func(cfg xxlclient.Config) (*xxlclient.Client, error) {
			u := strings.TrimRight(cfg.BaseURL, "/")
			if u == strings.TrimRight(srvExt.URL, "/") {
				return xxlclient.NewClient(cfg)
			}
			live := currentLiveManagedURL.Load().(string)
			if u == live {
				cp := cfg
				cp.BaseURL = srvManaged.URL
				return xxlclient.NewClient(cp)
			}
			retiredURLHits.Add(1)
			return nil, fmt.Errorf("dial retired endpoint %s: connection refused", u)
		}

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory}
		cbRec := &K8sJobCallbackReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme, ClientFactory: factory}

		for i := 0; i < 2; i++ {
			_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
			_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}})
			_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		}

		var syncedTask xxljobv1alpha1.XxlJobTask
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: taskCR.Name}, &syncedTask)
		origJobID := syncedTask.Status.JobID

		// Transition 1: Cluster-Local -> Hybrid V1 (ExternalURL=hybridV1, Image="xuxueli/xxl-job-admin:2.4.1", same DB)
		var liveAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.ExternalURL = hybridV1
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		currentLiveManagedURL.Store(hybridV1)

		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile Cluster-Local -> Hybrid V1: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("task reconcile Cluster-Local -> Hybrid V1: %v", err)
		}

		// Create in-flight Job stamped at Hybrid V1, then transition Hybrid V1 -> Hybrid V2 (same DB)!
		suspendFalse := false
		hybridJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:       proxyexecutor.BuildJobName(execCR.Name, origJobID, 9201),
				Namespace:  "default",
				Finalizers: []string{proxyexecutor.JobCallbackFinalizer},
				Labels: map[string]string{
					proxyexecutor.LabelManagedBy:    proxyexecutor.ManagedByValue,
					proxyexecutor.LabelExecutorName: proxyexecutor.SanitizeLabel(execCR.Name),
					proxyexecutor.LabelJobID:        strconv.Itoa(origJobID),
					proxyexecutor.LabelLogID:        "9201",
				},
				Annotations: map[string]string{
					proxyexecutor.AnnotationExecutorName:        execCR.Name,
					proxyexecutor.AnnotationAdminName:           adminCR.Name,
					proxyexecutor.AnnotationAdminUID:            string(adminCR.UID),
					proxyexecutor.AnnotationAdminURL:            hybridV1,
					proxyexecutor.AnnotationAdminDatasourceHash: proxyexecutor.DatasourceURLHash(adminCR.Spec.Datasource.URL),
					proxyexecutor.AnnotationLogID:               "9201",
					proxyexecutor.AnnotationLogDateTime:         "1700000009201",
				},
			},
			Spec: batchv1.JobSpec{Suspend: &suspendFalse},
			Status: batchv1.JobStatus{
				Succeeded:  1,
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
			},
		}
		_ = k8sCli.Create(ctx, hybridJob)

		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.ExternalURL = hybridV2
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		currentLiveManagedURL.Store(hybridV2)

		_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile Hybrid V1 -> Hybrid V2: %v", err)
		}
		if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
			t.Fatalf("task reconcile Hybrid V1 -> Hybrid V2: %v", err)
		}
		if _, err := cbRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: hybridJob.Name}}); err != nil {
			t.Fatalf("callback reconcile Hybrid V1 -> Hybrid V2: %v", err)
		}
		if hits := retiredURLHits.Load(); hits != 0 {
			t.Fatalf("expected 0 hits to retired endpoints across Hybrid transitions, got %d", hits)
		}
		muMgr.Lock()
		_, jobPreserved := mgrJobs[origJobID]
		_, grpPreserved := mgrGroups[1]
		gotHybridCBs := len(*mgrCBs)
		muMgr.Unlock()
		if !jobPreserved || !grpPreserved || gotHybridCBs != 1 {
			t.Fatalf("expected Hybrid shared DB state preserved (grp=%v job=%v) and 1 callback routed to Hybrid V2 (%d)", grpPreserved, jobPreserved, gotHybridCBs)
		}

		// Transition 3: Hybrid V2 (ManagedInCluster=true) -> Pure External (ExternalURL=srvExt.URL, Image="")
		// cleanupOwnedInClusterResources deletes the Deployment & Service backing Hybrid V2; MUST NOT deadlock on dead Hybrid V2!
		currentLiveManagedURL.Store("NONE")
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.ExternalURL = srvExt.URL
		liveAdmin.Spec.Image = ""
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)

		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}}); err != nil {
			t.Fatalf("admin reconcile Hybrid V2 -> Pure External: %v", err)
		}
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile Hybrid V2 -> Pure External deadlocked: %v", err)
		}
		for i := 0; i < 2; i++ {
			if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
				t.Fatalf("task reconcile Hybrid V2 -> Pure External deadlocked: %v", err)
			}
		}
		if hits := retiredURLHits.Load(); hits != 0 {
			t.Fatalf("expected 0 hits to retired Hybrid V2 endpoint after switching to Pure External, got %d", hits)
		}
		muExt.Lock()
		gotExtGroups := len(extGroups)
		gotExtJobs := len(extJobs)
		muExt.Unlock()
		if gotExtGroups != 1 || gotExtJobs != 1 {
			t.Fatalf("expected Pure External srvExt populated with migrated group (%d) and job (%d)", gotExtGroups, gotExtJobs)
		}
	})

	// Subtest 3: Operator restart after Auth SecretRef rotation (v1 -> v2) on srvOld before Task reconciled,
	// plus explicit Password="123456" default restoration when switching to srvNew (v3)
	t.Run("RestartAuthFallbackPrefersFresherAdminRecordedEndpointMetaAndExplicit123456", func(t *testing.T) {
		var expectedOldToken atomic.Value
		expectedOldToken.Store("tok-v1")
		var oldAuthFailures atomic.Int32

		rawOld, muOld, _, groupsOld, jobsOld, _ := setupTestAdminServer(t, nil)
		defer rawOld.Close()
		srvOld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login" {
				_ = r.ParseForm()
				if r.FormValue("userName") != "admin" || r.FormValue("password") != "123456" {
					oldAuthFailures.Add(1)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
			}
			if strings.HasPrefix(r.URL.Path, "/api/") {
				if r.Header.Get(xxlclient.AccessTokenHeader) != expectedOldToken.Load().(string) {
					oldAuthFailures.Add(1)
					w.WriteHeader(http.StatusForbidden)
					return
				}
			}
			rawOld.Config.Handler.ServeHTTP(w, r)
		}))
		defer srvOld.Close()

		srvNew, _, _, _, _, _ := setupTestAdminServer(t, nil)
		defer srvNew.Close()

		secV1 := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tok-sec-v1", Namespace: "default"}, Data: map[string][]byte{"token": []byte("tok-v1")}}
		secV2 := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tok-sec-v2", Namespace: "default"}, Data: map[string][]byte{"token": []byte("tok-v2")}}
		secV3 := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "tok-sec-v3", Namespace: "default"}, Data: map[string][]byte{"token": []byte("tok-v3")}}

		adminCR := &xxljobv1alpha1.XxlJobAdmin{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-restart-admin", Namespace: "default", UID: types.UID("uid-rot-restart")},
			Spec: xxljobv1alpha1.XxlJobAdminSpec{
				ExternalURL: srvOld.URL,
				Auth: xxljobv1alpha1.AuthSpec{
					Password:             "123456", // explicit default password on srvOld
					AccessTokenSecretRef: &xxljobv1alpha1.SecretKeyReference{Name: secV1.Name, Key: "token"},
				},
			},
		}
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-restart-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
				AppName:  "rot-restart-app",
				Title:    "Rot Restart App",
				Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		taskCR := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "rot-restart-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
				JobDesc:         "Rot Restart Task",
				Author:          "ops",
				ScheduleType:    "CRON",
				ScheduleConf:    "0 */5 * * * ?",
				ExecutorHandler: "rotHandler",
			},
		}

		k8sCli := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(secV1, secV2, secV3, adminCR, execCR, taskCR).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}).
			Build()

		adminRec := &XxlJobAdminReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		execRec := &XxlJobExecutorReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}
		taskRec := &XxlJobTaskReconciler{Client: k8sCli, APIReader: k8sCli, Scheme: scheme}

		for i := 0; i < 2; i++ {
			_, _ = adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}})
			_, _ = execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}})
			_, _ = taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}})
		}

		// Rotate srvOld token from v1 -> v2 on Admin CR and reconcile ONLY Admin (Task & Executor still hold stale v1 in Status.Conditions!)
		expectedOldToken.Store("tok-v2")
		var liveAdmin xxljobv1alpha1.XxlJobAdmin
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.Auth.AccessTokenSecretRef = &xxljobv1alpha1.SecretKeyReference{Name: secV2.Name, Key: "token"}
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}}); err != nil {
			t.Fatalf("reconcile admin after v1->v2 rotation: %v", err)
		}

		// Now switch Admin to srvNew with Username="new-admin", Password="new-inline-password", AccessTokenSecretRef=secV3
		_ = k8sCli.Get(ctx, types.NamespacedName{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
		liveAdmin.Spec.ExternalURL = srvNew.URL
		liveAdmin.Spec.Auth.Username = "new-admin"
		liveAdmin.Spec.Auth.Password = "new-inline-password"
		liveAdmin.Spec.Auth.AccessTokenSecretRef = &xxljobv1alpha1.SecretKeyReference{Name: secV3.Name, Key: "token"}
		liveAdmin.Generation++
		_ = k8sCli.Update(ctx, &liveAdmin)
		if _, err := adminRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: adminCR.Name}}); err != nil {
			t.Fatalf("reconcile admin after srvNew switch: %v", err)
		}

		// Simulate Operator restart: clear all in-memory caches!
		endpointAuthMu.Lock()
		clear(endpointAuthCache)
		endpointAuthOrder = nil
		endpointAuthMu.Unlock()
		clientCacheMu.Lock()
		clear(clientCache)
		clear(clientCacheByAdmin)
		clientCacheOrder = nil
		clientCacheMu.Unlock()
		secretCacheMu.Lock()
		clear(secretCache)
		secretCacheMu.Unlock()

		// Reconcile Executor & Task: MUST use Admin's fresher v2 SecretRef (NOT stale v1 on Executor/Task)
		// AND restore default "admin"/"123456" (NOT "new-admin"/"new-inline-password")!
		if _, err := execRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: execCR.Name}}); err != nil {
			t.Fatalf("exec reconcile after restart failed: %v", err)
		}
		for i := 0; i < 2; i++ {
			if _, err := taskRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: taskCR.Name}}); err != nil {
				t.Fatalf("task reconcile after restart failed: %v", err)
			}
		}
		if failures := oldAuthFailures.Load(); failures != 0 {
			t.Fatalf("expected 0 auth failures on srvOld after restart (expected v2 + admin/123456), got %d", failures)
		}
		muOld.Lock()
		remOldGroups := len(groupsOld)
		remOldJobs := len(jobsOld)
		muOld.Unlock()
		if remOldGroups != 0 || remOldJobs != 0 {
			t.Fatalf("expected srvOld fully cleaned up after restart, remaining groups=%d jobs=%d", remOldGroups, remOldJobs)
		}
	})
}
