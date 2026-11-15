package proxyexecutor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

func TestProxyExecutorEndToEndAndSecurity(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	task := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "report-batch-task",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
			JobDesc:         "Generate Daily Financial Report",
			Author:          "alice",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 1 * * ?",
			ExecutorHandler: "dailyReportHandler",
			TriggerStatus:   true,
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo 'Generating report...'; exit 0"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      205,
			JobGroupID: 10,
			Synced:     true,
		},
	}

	// Second task in the SAME namespace belonging to a different executor/Admin, sharing JobID=205 (tests P1-4)
	otherTask := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-admin-task",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "other-exec"},
			JobDesc:         "Other Team Task with Same JobID",
			Author:          "bob",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 0 2 * * ?",
			ExecutorHandler: "otherHandler",
			TriggerStatus:   true,
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo 'Other job'; exit 0"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      205,
			JobGroupID: 1,
			Synced:     true,
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		WithObjects(task, otherTask).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "")
	if srv.NeedLeaderElection() {
		t.Fatalf("expected NeedLeaderElection() == false so all replicas serve proxy requests")
	}

	srv.ValidateToken = func(_ context.Context, namespace, executorName, providedToken string) error {
		if namespace == "default" && executorName == "k8s-proxy-exec" && providedToken == "my_token" {
			return nil
		}
		if namespace == "default" && executorName == "other-exec" && providedToken == "other_token" {
			return nil
		}
		return fmt.Errorf("unauthorized token for %s/%s", namespace, executorName)
	}

	var logMu sync.Mutex
	podLogOutput := ""
	srv.FetchPodLog = func(_ context.Context, _, _, _ string) (string, error) {
		logMu.Lock()
		defer logMu.Unlock()
		return podLogOutput, nil
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	postJSONWithToken := func(path, token string, payload any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set(xxlclient.AccessTokenHeader, token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", path, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		if err := json.NewDecoder(resp.Body).Decode(&ret); err != nil {
			t.Fatalf("decode %s response failed: %v", path, err)
		}
		return ret
	}

	postJSON := func(path string, payload any) xxlclient.ReturnT {
		return postJSONWithToken(path, "my_token", payload)
	}

	// 1. Test N-P0-3 & R4-P0-2: Root paths and "default_token" on scoped paths must be rejected
	if ret := postJSONWithToken("/run", "", xxlclient.TriggerParam{JobID: 205, LogID: 1}); ret.IsSuccess() {
		t.Fatalf("expected root /run to be rejected (N-P0-3), got: %+v", ret)
	}
	if ret := postJSONWithToken("/kill", "", xxlclient.KillParam{JobID: 205}); ret.IsSuccess() {
		t.Fatalf("expected root /kill to be rejected (N-P0-3), got: %+v", ret)
	}
	if ret := postJSONWithToken("/proxy/default/k8s-proxy-exec/run", "default_token", xxlclient.TriggerParam{JobID: 205, LogID: 1}); ret.IsSuccess() {
		t.Fatalf("expected default_token on scoped /run to be rejected (R4-P0-2), got: %+v", ret)
	}

	// 2. Test N-P0-2: Sentinel /proxy/_unavailable/{beat,run} always returns Code=500
	if ret := postJSONWithToken("/proxy/_unavailable/beat", "", map[string]any{}); ret.IsSuccess() {
		t.Fatalf("expected sentinel /proxy/_unavailable/beat to return 500, got: %+v", ret)
	}
	if ret := postJSONWithToken("/proxy/_unavailable/run", "", xxlclient.TriggerParam{JobID: 205}); ret.IsSuccess() {
		t.Fatalf("expected sentinel /proxy/_unavailable/run to return 500, got: %+v", ret)
	}

	// 3. /beat via scoped path /proxy/default/k8s-proxy-exec/beat
	ret := postJSON("/proxy/default/k8s-proxy-exec/beat", map[string]any{})
	if !ret.IsSuccess() {
		t.Fatalf("/beat failed: %+v", ret)
	}

	// 4. Reject undeclared task even if it shares ExecutorHandler name ("dailyReportHandler") with a declared CR (fixes P0-4 & R4-P2)
	ret = postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
		JobID:           9999,
		ExecutorHandler: "dailyReportHandler",
		GlueType:        "GLUE_SHELL",
		GlueSource:      "id; uname -a",
		LogID:           8999,
	})
	if ret.IsSuccess() {
		t.Fatalf("expected undeclared JobID=9999 sharing handler name to be rejected")
	}

	// 5. /idleBeat before running job -> should be 200
	ret = postJSON("/proxy/default/k8s-proxy-exec/idleBeat", xxlclient.IdleBeatParam{JobID: 205})
	if !ret.IsSuccess() {
		t.Fatalf("/idleBeat expected idle (200), got: %+v", ret)
	}

	// 6. /run -> creates batch/v1.Job with executor hash, callback-guard finalizer, and persistent callback annotations
	ret = postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
		JobID:                 205,
		ExecutorHandler:       "dailyReportHandler",
		ExecutorParams:        "date=2026-11-09",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 9001,
		LogDateTime:           1700000000000,
	})
	if !ret.IsSuccess() {
		t.Fatalf("/run failed: %+v", ret)
	}

	expectedJobName := BuildJobName("k8s-proxy-exec", 205, 9001)
	var createdJob batchv1.Job
	if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expectedJobName}, &createdJob); err != nil {
		t.Fatalf("expected K8s Job %s to be created: %v", expectedJobName, err)
	}
	if createdJob.Annotations[AnnotationTaskName] != "report-batch-task" ||
		createdJob.Annotations[AnnotationLogID] != "9001" ||
		createdJob.Labels[LabelExecutorName] != "k8s-proxy-exec" ||
		!controllerutil.ContainsFinalizer(&createdJob, JobCallbackFinalizer) {
		t.Fatalf("expected callback annotations, executor label, and %s finalizer on Job, got annotations=%+v labels=%+v finalizers=%v",
			JobCallbackFinalizer, createdJob.Annotations, createdJob.Labels, createdJob.Finalizers)
	}

	// 7. Test P1-4: Another executor in the same namespace with the same JobID=205 is completely isolated!
	if otherIdle := postJSONWithToken("/proxy/default/other-exec/idleBeat", "other_token", xxlclient.IdleBeatParam{JobID: 205}); !otherIdle.IsSuccess() {
		t.Fatalf("expected other-exec /idleBeat for jobId=205 to be idle despite k8s-proxy-exec running jobId=205: %+v", otherIdle)
	}
	if otherKill := postJSONWithToken("/proxy/default/other-exec/kill", "other_token", xxlclient.KillParam{JobID: 205}); !otherKill.IsSuccess() {
		t.Fatalf("other-exec /kill failed: %+v", otherKill)
	}
	if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expectedJobName}, &createdJob); err != nil {
		t.Fatalf("k8s-proxy-exec's Job was unexpectedly deleted by other-exec's /kill: %v", err)
	}

	// 8. /idleBeat on k8s-proxy-exec while job is running -> should return 500 (busy)
	ret = postJSON("/proxy/default/k8s-proxy-exec/idleBeat", xxlclient.IdleBeatParam{JobID: 205})
	if ret.IsSuccess() {
		t.Fatalf("/idleBeat expected busy (500) while job is active")
	}

	// 9. Test /log when Pod has not produced any output yet -> ToLineNum must be FromLineNum - 1 (fixes P1-2)
	emptyLogRet := postJSON("/proxy/default/k8s-proxy-exec/log", xxlclient.LogParam{
		LogID:       9001,
		FromLineNum: 1,
	})
	var emptyLogRes xxlclient.LogResult
	_ = json.Unmarshal(emptyLogRet.Content, &emptyLogRes)
	if emptyLogRes.FromLineNum != 1 || emptyLogRes.ToLineNum != 0 || emptyLogRes.LogContent != "" || emptyLogRes.IsEnd {
		t.Fatalf("expected ToLineNum=0 and empty content before Pod logs exist, got: %+v", emptyLogRes)
	}

	// 10. Create corresponding Pod & populate log output
	logMu.Lock()
	podLogOutput = "line1: starting job\nline2: processing data\nline3: report completed\n"
	logMu.Unlock()

	jobPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedJobName + "-pod",
			Namespace: "default",
			Labels:    createdJob.Labels,
		},
		Spec: createdJob.Spec.Template.Spec,
	}
	_ = fakeK8s.Create(context.Background(), jobPod)

	logRet := postJSON("/proxy/default/k8s-proxy-exec/log", xxlclient.LogParam{
		LogID:       9001,
		FromLineNum: 2,
	})
	if !logRet.IsSuccess() {
		t.Fatalf("/log failed: %+v", logRet)
	}
	var logRes xxlclient.LogResult
	_ = json.Unmarshal(logRet.Content, &logRes)
	if logRes.FromLineNum != 2 || logRes.ToLineNum != 3 {
		t.Fatalf("unexpected log line range: %+v", logRes)
	}

	// 11. Mark Job 9001 completed and remove its finalizer; verify /kill on a new running job 9002
	// does NOT delete completed historical Job 9001 (fixes R4-P1-2).
	createdJob.Status.Succeeded = 1
	createdJob.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = fakeK8s.Status().Update(context.Background(), &createdJob)
	_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expectedJobName}, &createdJob)
	controllerutil.RemoveFinalizer(&createdJob, JobCallbackFinalizer)
	_ = fakeK8s.Update(context.Background(), &createdJob)

	// Start a second running Job 9002 and call /kill
	_ = postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
		JobID:                 205,
		ExecutorHandler:       "dailyReportHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 9002,
	})
	killRet := postJSON("/proxy/default/k8s-proxy-exec/kill", xxlclient.KillParam{JobID: 205})
	if !killRet.IsSuccess() {
		t.Fatalf("/kill failed: %+v", killRet)
	}
	// Completed Job 9001 must still exist!
	if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expectedJobName}, &createdJob); err != nil {
		t.Fatalf("completed historical Job 9001 was unexpectedly deleted by /kill (R4-P1-2 regression): %v", err)
	}
	// Clean up 9002 finalizer so 9002 finishes deleting
	job9002Name := BuildJobName("k8s-proxy-exec", 205, 9002)
	var job9002 batchv1.Job
	if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: job9002Name}, &job9002); err == nil {
		controllerutil.RemoveFinalizer(&job9002, JobCallbackFinalizer)
		_ = fakeK8s.Update(context.Background(), &job9002)
	}

	// 12. Test R4-P1-1: Multi-replica SERIAL_EXECUTION where 3 separate Server instances concurrently receive /run
	// Every Job created under SERIAL_EXECUTION starts with spec.suspend=true, and only the single leader
	// controller (AdvanceSerialQueueWithReader) unsuspends 1 Job at a time in strict logId FIFO order!
	var wg sync.WaitGroup
	for idx, logID := range []int64{9101, 9102, 9103} {
		wg.Add(1)
		go func(replicaIdx int, lid int64) {
			defer wg.Done()
			// Create an independent Server instance per request to simulate 3 separate Operator Proxy replicas
			replicaSrv := NewServer(fakeK8s, ":0", "default", "my_token")
			replicaTS := httptest.NewServer(replicaSrv.Handler())
			defer replicaTS.Close()

			b, _ := json.Marshal(xxlclient.TriggerParam{
				JobID:                 205,
				ExecutorHandler:       "dailyReportHandler",
				ExecutorBlockStrategy: "SERIAL_EXECUTION",
				LogID:                 lid,
			})
			req, _ := http.NewRequest(http.MethodPost, replicaTS.URL+"/proxy/default/k8s-proxy-exec/run", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "my_token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("replica %d /run failed: %v", replicaIdx, err)
				return
			}
			defer resp.Body.Close()
		}(idx, logID)
	}
	wg.Wait()

	// Before controller runs AdvanceSerialQueueWithReader, ALL 3 Jobs must be suspended (0 unsuspended)
	// Then when leader runs AdvanceSerialQueueWithReader, ONLY logId=9101 is unsuspended!
	if err := AdvanceSerialQueueWithReader(context.Background(), fakeK8s, fakeK8s, "default", "k8s-proxy-exec", "205"); err != nil {
		t.Fatalf("AdvanceSerialQueueWithReader failed: %v", err)
	}

	var serialList batchv1.JobList
	if err := fakeK8s.List(context.Background(), &serialList, client.InNamespace("default"), client.MatchingLabels{
		LabelExecutorName: "k8s-proxy-exec",
		LabelJobID:        "205",
	}); err != nil {
		t.Fatalf("list serial jobs failed: %v", err)
	}

	var unsuspendedLogs []string
	var activeJob batchv1.Job
	for _, j := range serialList.Items {
		if j.DeletionTimestamp.IsZero() && isJobRunning(&j) && (j.Spec.Suspend == nil || !*j.Spec.Suspend) {
			unsuspendedLogs = append(unsuspendedLogs, j.Labels[LabelLogID])
			activeJob = j
		}
	}
	if len(unsuspendedLogs) != 1 || unsuspendedLogs[0] != "9101" {
		t.Fatalf("expected only earliest Job 9101 to be unsuspended across 3 concurrent replicas, got %v", unsuspendedLogs)
	}

	// Mark 9101 complete and call AdvanceSerialQueueWithReader -> only 9102 must unsuspend, while 9103 stays suspended!
	activeJob.Status.Succeeded = 1
	activeJob.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := fakeK8s.Status().Update(context.Background(), &activeJob); err != nil {
		t.Fatalf("update activeJob status failed: %v", err)
	}
	if err := AdvanceSerialQueueWithReader(context.Background(), fakeK8s, fakeK8s, "default", "k8s-proxy-exec", "205"); err != nil {
		t.Fatalf("AdvanceSerialQueueWithReader after 9101 completion failed: %v", err)
	}

	_ = fakeK8s.List(context.Background(), &serialList, client.InNamespace("default"), client.MatchingLabels{
		LabelExecutorName: "k8s-proxy-exec",
		LabelJobID:        "205",
	})
	unsuspendedLogs = nil
	for _, j := range serialList.Items {
		if j.DeletionTimestamp.IsZero() && isJobRunning(&j) && (j.Spec.Suspend == nil || !*j.Spec.Suspend) {
			unsuspendedLogs = append(unsuspendedLogs, j.Labels[LabelLogID])
		}
	}
	if len(unsuspendedLogs) != 1 || unsuspendedLogs[0] != "9102" {
		t.Fatalf("expected FIFO next Job 9102 to be unsuspended while 9103 remains suspended, got %v", unsuspendedLogs)
	}

	// 13. Test R5-P2-4: Multi-replica concurrent DISCARD_LATER leaves at most 1 active Job
	t.Run("MultiReplicaConcurrentDiscardLater", func(t *testing.T) {
		discardTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "discard-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "discardHandler",
				ExecutorBlockStrategy: "DISCARD_LATER",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image: "alpine:3.19",
				},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 301, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), discardTask)

		var wgDiscard sync.WaitGroup
		var successCount int
		var muDiscard sync.Mutex
		for idx, lid := range []int64{9201, 9202, 9203} {
			wgDiscard.Add(1)
			go func(replicaIdx int, logID int64) {
				defer wgDiscard.Done()
				replicaSrv := NewServer(fakeK8s, ":0", "default", "my_token")
				replicaTS := httptest.NewServer(replicaSrv.Handler())
				defer replicaTS.Close()

				b, _ := json.Marshal(xxlclient.TriggerParam{
					JobID:                 301,
					ExecutorHandler:       "discardHandler",
					ExecutorBlockStrategy: "DISCARD_LATER",
					LogID:                 logID,
				})
				req, _ := http.NewRequest(http.MethodPost, replicaTS.URL+"/proxy/default/k8s-proxy-exec/run", bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set(xxlclient.AccessTokenHeader, "my_token")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Errorf("replica %d /run failed: %v", replicaIdx, err)
					return
				}
				defer resp.Body.Close()
				var ret xxlclient.ReturnT
				_ = json.NewDecoder(resp.Body).Decode(&ret)
				if ret.IsSuccess() {
					muDiscard.Lock()
					successCount++
					muDiscard.Unlock()
				}
			}(idx, lid)
		}
		wgDiscard.Wait()

		var activeDiscard batchv1.JobList
		_ = fakeK8s.List(context.Background(), &activeDiscard, client.InNamespace("default"), client.MatchingLabels{
			LabelExecutorName: "k8s-proxy-exec",
			LabelJobID:        "301",
		})
		runningCount := 0
		for i := range activeDiscard.Items {
			if activeDiscard.Items[i].DeletionTimestamp.IsZero() && isJobRunning(&activeDiscard.Items[i]) {
				runningCount++
			}
		}
		if runningCount != 1 || successCount != 1 {
			t.Fatalf("expected exactly 1 active Job and 1 HTTP 200 across 3 concurrent DISCARD_LATER replicas (R5-P2-4), got running=%d success=%d", runningCount, successCount)
		}
	})

	// 14. Test R5-P2-3 (GLUE_PYTHON runtime mapping) & R5-P2-10 (AllowedServiceAccounts whitelist)
	t.Run("GluePythonRuntimeAndServiceAccountWhitelist", func(t *testing.T) {
		execCR := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "k8s-proxy-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:               xxljobv1alpha1.LocalObjectReference{Name: "xxl-job-admin"},
				AppName:                "k8s-proxy-exec",
				Title:                  "Proxy Exec",
				Mode:                   xxljobv1alpha1.ExecutorModeK8sJobProxy,
				AllowedServiceAccounts: []string{"batch-runner-sa"},
			},
		}
		_ = fakeK8s.Create(context.Background(), execCR)

		pyTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "py-glue-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler: "pyGlueHandler",
				GlueType:        "GLUE_PYTHON",
				GlueSource:      "print('hello python')",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					ServiceAccountName: "cluster-admin-sa", // Not in AllowedServiceAccounts!
				},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 302, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), pyTask)

		// Attempt trigger with unauthorized ServiceAccount -> must be rejected (fixes R5-P2-10)
		unauthSARet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:           302,
			ExecutorHandler: "pyGlueHandler",
			LogID:           9301,
		})
		if unauthSARet.IsSuccess() {
			t.Fatalf("expected unauthorized serviceAccountName 'cluster-admin-sa' to be rejected (R5-P2-10)")
		}

		// Update task to use permitted ServiceAccount "batch-runner-sa"
		_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "py-glue-task"}, pyTask)
		pyTask.Spec.K8sJobTemplate.ServiceAccountName = "batch-runner-sa"
		_ = fakeK8s.Update(context.Background(), pyTask)

		allowedSARet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 302,
			ExecutorHandler:       "pyGlueHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9302,
		})
		if !allowedSARet.IsSuccess() {
			t.Fatalf("expected permitted serviceAccountName 'batch-runner-sa' to succeed, got %+v", allowedSARet)
		}

		var pyJob batchv1.Job
		if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: BuildJobName("k8s-proxy-exec", 302, 9302)}, &pyJob); err != nil {
			t.Fatalf("expected GLUE_PYTHON Job to be created: %v", err)
		}
		c := pyJob.Spec.Template.Spec.Containers[0]
		if c.Image != "python:3.11-slim" || len(c.Command) < 2 || c.Command[0] != "python3" {
			t.Fatalf("expected GLUE_PYTHON to select python:3.11-slim / python3 -c (R5-P2-3), got image=%s cmd=%v", c.Image, c.Command)
		}
		if pyJob.Spec.ActiveDeadlineSeconds == nil || *pyJob.Spec.ActiveDeadlineSeconds != DefaultJobActiveDeadlineSeconds {
			t.Fatalf("expected default ActiveDeadlineSeconds=%d (R5-P2-2), got %v", DefaultJobActiveDeadlineSeconds, pyJob.Spec.ActiveDeadlineSeconds)
		}
	})

	// 15. Test SanitizeLabel collision avoidance for long or special-char names (fixes R5-P3)
	t.Run("SanitizeLabelNoCollision", func(t *testing.T) {
		longPrefix := "very-long-executor-name-exceeding-sixty-three-characters-in-kubernetes-label-"
		l1 := SanitizeLabel(longPrefix + "alpha")
		l2 := SanitizeLabel(longPrefix + "beta")
		if len(l1) > 63 || len(l2) > 63 || l1 == l2 {
			t.Fatalf("expected distinct <=63 char sanitized labels for long names, got %q and %q", l1, l2)
		}
	})

	// 16. Test R6-P1-7: Barrier-synchronized concurrent DISCARD_LATER across replicas + non-conflict Status().Update failure
	t.Run("DiscardLaterBarrierAndNonConflictError", func(t *testing.T) {
		barrierTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "barrier-discard-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "barrierDiscardHandler",
				ExecutorBlockStrategy: "DISCARD_LATER",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image: "alpine:3.19",
				},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 401, Synced: true},
		}

		// Synchronize 2 concurrent replicas right before Create(Job) so both pass hasRunningJob() at the exact same instant
		arrived := make(chan struct{}, 2)
		release := make(chan struct{})
		var releaseOnce sync.Once
		var createdAllSuspended sync.Map

		interceptedK8s := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			WithObjects(barrierTask).
			WithInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if job, ok := obj.(*batchv1.Job); ok && job.Labels[LabelJobID] == "401" {
						createdAllSuspended.Store(job.Name, job.Spec.Suspend != nil && *job.Spec.Suspend)
						arrived <- struct{}{}
						if len(arrived) == 2 {
							releaseOnce.Do(func() { close(release) })
						}
						<-release
					}
					return c.Create(ctx, obj, opts...)
				},
			}).
			Build()

		var wg sync.WaitGroup
		var successCount int
		var mu sync.Mutex
		for _, lid := range []int64{9401, 9402} {
			wg.Add(1)
			go func(logID int64) {
				defer wg.Done()
				replicaSrv := NewServer(interceptedK8s, ":0", "default", "my_token")
				replicaTS := httptest.NewServer(replicaSrv.Handler())
				defer replicaTS.Close()

				b, _ := json.Marshal(xxlclient.TriggerParam{
					JobID:                 401,
					ExecutorHandler:       "barrierDiscardHandler",
					ExecutorBlockStrategy: "DISCARD_LATER",
					LogID:                 logID,
					LogDateTime:           1700000000401,
				})
				req, _ := http.NewRequest(http.MethodPost, replicaTS.URL+"/proxy/default/k8s-proxy-exec/run", bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set(xxlclient.AccessTokenHeader, "my_token")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Errorf("barrier replica /run failed: %v", err)
					return
				}
				defer resp.Body.Close()
				var ret xxlclient.ReturnT
				_ = json.NewDecoder(resp.Body).Decode(&ret)
				if ret.IsSuccess() {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(lid)
		}
		wg.Wait()

		// Both Jobs must have been created with spec.suspend=true initially so losing replica never spawns a Pod
		createdAllSuspended.Range(func(key, value any) bool {
			if suspended, _ := value.(bool); !suspended {
				t.Errorf("expected Job %v to be created with spec.suspend=true initially", key)
			}
			return true
		})

		var remaining batchv1.JobList
		_ = interceptedK8s.List(context.Background(), &remaining, client.InNamespace("default"), client.MatchingLabels{LabelJobID: "401"})
		unsuspendedCount := 0
		for _, j := range remaining.Items {
			if j.DeletionTimestamp.IsZero() && (j.Spec.Suspend == nil || !*j.Spec.Suspend) {
				unsuspendedCount++
			}
		}
		if successCount != 1 || unsuspendedCount != 1 {
			t.Fatalf("expected exactly 1 HTTP 200 and 1 unsuspended Job under barrier DISCARD_LATER (R6-P1-7), got success=%d unsuspended=%d", successCount, unsuspendedCount)
		}

		// Also verify non-conflict error from Status().Update fails closed and cleans up the created Job
		errTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "err-discard-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "errDiscardHandler",
				ExecutorBlockStrategy: "DISCARD_LATER",
				K8sJobTemplate:        &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 402, Synced: true},
		}
		failStatusK8s := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
			WithObjects(errTask).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ ...client.SubResourceUpdateOption) error {
					return fmt.Errorf("apiserver status update timeout")
				},
			}).
			Build()

		errSrv := NewServer(failStatusK8s, ":0", "default", "my_token")
		errTS := httptest.NewServer(errSrv.Handler())
		defer errTS.Close()

		b, _ := json.Marshal(xxlclient.TriggerParam{
			JobID:                 402,
			ExecutorHandler:       "errDiscardHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9409,
		})
		req, _ := http.NewRequest(http.MethodPost, errTS.URL+"/proxy/default/k8s-proxy-exec/run", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "my_token")
		resp, _ := http.DefaultClient.Do(req)
		var errRet xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&errRet)
		resp.Body.Close()
		if errRet.IsSuccess() {
			t.Fatalf("expected non-conflict Status().Update error under DISCARD_LATER to fail closed")
		}
		var leftover batchv1.JobList
		_ = failStatusK8s.List(context.Background(), &leftover, client.InNamespace("default"), client.MatchingLabels{LabelJobID: "402"})
		if len(leftover.Items) != 0 {
			t.Fatalf("expected aborted Job to be cleaned up on Status().Update failure, got %d leftover jobs", len(leftover.Items))
		}
	})

	// 17. Test R6-P1-14 (PodSecurity restricted defaults & SA token Secret rejection), R6-P2-4 (GLUE argv), R6-P2-5 (502 timeout), R6-P2-16 (AlreadyExists LogDateTime)
	t.Run("PodSecurityAndGlueArgvAndAlreadyExistsGuard", func(t *testing.T) {
		// Create a kubernetes.io/service-account-token Secret and verify mounting it is rejected
		saTokenSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "stolen-sa-token", Namespace: "default"},
			Type:       corev1.SecretTypeServiceAccountToken,
		}
		_ = fakeK8s.Create(context.Background(), saTokenSecret)

		badSecTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-sec-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler: "badSecHandler",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image: "alpine:3.19",
					Volumes: []corev1.Volume{
						{
							Name: "token-vol",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{SecretName: "stolen-sa-token"},
							},
						},
					},
				},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 403, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), badSecTask)

		secRet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:           403,
			ExecutorHandler: "badSecHandler",
			LogID:           9501,
		})
		if secRet.IsSuccess() {
			t.Fatalf("expected mounting kubernetes.io/service-account-token Secret to be rejected (R6-P1-14)")
		}

		// Verify GLUE_SHELL receives positional argv ($1=params, $2=shardIndex, $3=shardTotal) and restricted security defaults
		shTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "sh-glue-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler: "shGlueHandler",
				GlueType:        "GLUE_SHELL",
				GlueSource:      "echo $1 $2 $3",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 404, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), shTask)

		shRet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 404,
			ExecutorHandler:       "shGlueHandler",
			ExecutorParams:        "arg1_val",
			BroadcastIndex:        2,
			BroadcastTotal:        5,
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 9502,
			LogDateTime:           1700000009502,
		})
		if !shRet.IsSuccess() {
			t.Fatalf("expected GLUE_SHELL trigger to succeed, got %+v", shRet)
		}
		var shJob batchv1.Job
		if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: BuildJobName("k8s-proxy-exec", 404, 9502)}, &shJob); err != nil {
			t.Fatalf("get GLUE_SHELL job failed: %v", err)
		}
		podSpec := shJob.Spec.Template.Spec
		if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
			t.Fatalf("expected AutomountServiceAccountToken=false by default (R6-P1-14)")
		}
		if podSpec.SecurityContext == nil || podSpec.SecurityContext.RunAsNonRoot == nil || !*podSpec.SecurityContext.RunAsNonRoot {
			t.Fatalf("expected restricted PodSecurityContext RunAsNonRoot=true by default (R6-P1-14)")
		}
		c := podSpec.Containers[0]
		if len(c.Args) != 3 || c.Args[0] != "arg1_val" || c.Args[1] != "2" || c.Args[2] != "5" {
			t.Fatalf("expected GLUE positional Args [arg1_val, 2, 5] (R6-P2-4), got %v", c.Args)
		}

		// Verify AlreadyExists with matching LogDateTime succeeds, but conflicting LogDateTime returns 500 (R6-P2-16)
		sameRetry := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 404,
			ExecutorHandler:       "shGlueHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 9502,
			LogDateTime:           1700000009502,
		})
		if !sameRetry.IsSuccess() {
			t.Fatalf("expected idempotent retry with identical LogDateTime to return 200, got %+v", sameRetry)
		}
		conflictRetry := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 404,
			ExecutorHandler:       "shGlueHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 9502,
			LogDateTime:           1700009999999,
		})
		if conflictRetry.IsSuccess() {
			t.Fatalf("expected conflicting LogDateTime on existing logId to return 500 (R6-P2-16)")
		}

		// Verify IsJobDeadlineExceeded detects Reason=DeadlineExceeded (R6-P2-5)
		shJob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
		}
		if !IsJobDeadlineExceeded(&shJob) {
			t.Fatalf("expected IsJobDeadlineExceeded=true for Reason=DeadlineExceeded")
		}

		// Verify GLUE_NODEJS command includes "xxl-glue" argv[1] placeholder so process.argv[2..4] receives params/shardIdx/shardTotal (R7-P2-8)
		_, nodeCmd, nodeArgs, err := resolveGlueFallbackRuntime("GLUE_NODEJS", "console.log(process.argv)", xxlclient.TriggerParam{
			ExecutorParams: "node_p",
			BroadcastIndex: 1,
			BroadcastTotal: 3,
		})
		if err != nil || len(nodeCmd) != 4 || nodeCmd[3] != "xxl-glue" || len(nodeArgs) != 3 || nodeArgs[0] != "node_p" {
			t.Fatalf("expected GLUE_NODEJS cmd [node -e <script> xxl-glue] and args [node_p 1 3] (R7-P2-8), got cmd=%v args=%v err=%v", nodeCmd, nodeArgs, err)
		}

		// Verify gitRepo volume is rejected by baseline PodSecurity (R7-P2-5)
		gitRepoSpec := &corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "cve-gitrepo",
					VolumeSource: corev1.VolumeSource{
						GitRepo: &corev1.GitRepoVolumeSource{Repository: "https://example.com/repo.git"},
					},
				},
			},
			Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", gitRepoSpec, nil); err == nil {
			t.Fatalf("expected gitRepo volume to be rejected under baseline PodSecurity (R7-P2-5)")
		}
	})

	// 18. Test R8-P1-2 & R8-P1-3: Idempotent /run retry under DISCARD_LATER and COVER_EARLY does not self-discard or self-kill
	t.Run("HandleRunIdempotentRetryUnderDiscardLaterAndCoverEarly", func(t *testing.T) {
		dlRetryTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "dl-idempotent-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "dlIdempotentHandler",
				ExecutorBlockStrategy: "DISCARD_LATER",
				K8sJobTemplate:        &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 405, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), dlRetryTask)

		// Pre-create a suspended Job for logId=9601 as if attempt 1 crashed after Create(suspend=true)
		preJobName := BuildJobName("k8s-proxy-exec", 405, 9601)
		suspTrue := true
		preSuspendedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      preJobName,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: "k8s-proxy-exec",
					LabelJobID:        "405",
					LabelLogID:        "9601",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy: "DISCARD_LATER",
					AnnotationLogID:         "9601",
					AnnotationLogDateTime:   "1700000009601",
				},
				CreationTimestamp: metav1.Now(),
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspTrue,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
			},
		}
		_ = fakeK8s.Create(context.Background(), preSuspendedJob)

		// Attempt 2 with same (jobId=405, logId=9601) must NOT self-discard; it must resume claim & unsuspend
		dlRetry1 := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 405,
			ExecutorHandler:       "dlIdempotentHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9601,
			LogDateTime:           1700000009601,
		})
		if !dlRetry1.IsSuccess() {
			t.Fatalf("R8-P1-2 regression: idempotent retry of suspended DISCARD_LATER job self-discarded: %+v", dlRetry1)
		}
		var verifyDLJob batchv1.Job
		if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: preJobName}, &verifyDLJob); err != nil {
			t.Fatalf("get dl job failed: %v", err)
		}
		if verifyDLJob.Spec.Suspend != nil && *verifyDLJob.Spec.Suspend {
			t.Fatalf("R8-P1-2 regression: expected DISCARD_LATER job to be unsuspended on idempotent retry")
		}

		// Attempt 3 while already unsuspended & running with same logId=9601 must also return 200 idempotently
		dlRetry2 := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 405,
			ExecutorHandler:       "dlIdempotentHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9601,
			LogDateTime:           1700000009601,
		})
		if !dlRetry2.IsSuccess() {
			t.Fatalf("R8-P1-2 regression: idempotent retry of running DISCARD_LATER job failed: %+v", dlRetry2)
		}

		// Under COVER_EARLY, retrying the same (jobId=404, logId=9502) must NOT kill its own Job
		ceRetry := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 404,
			ExecutorHandler:       "shGlueHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 9502,
			LogDateTime:           1700000009502,
		})
		if !ceRetry.IsSuccess() {
			t.Fatalf("R8-P1-2 regression: idempotent retry under COVER_EARLY failed: %+v", ceRetry)
		}
		var verifyCEJob batchv1.Job
		if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: BuildJobName("k8s-proxy-exec", 404, 9502)}, &verifyCEJob); err != nil {
			t.Fatalf("R8-P1-2 regression: COVER_EARLY idempotent retry self-killed its own Job: %v", err)
		}
	})

	// 19. Test R8-P1-4: PodSecurity blocks Projected serviceAccountToken, InitContainers privilege escalation, HostPort, and unlisted default SA with automount=true
	t.Run("PodSecurityProjectedSATokenAndInitContainersAndHostPort", func(t *testing.T) {
		projectedSATokenSpec := &corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "proj-token",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{
									ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"},
								},
							},
						},
					},
				},
			},
			Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "restricted", projectedSATokenSpec, nil); err == nil {
			t.Fatalf("R8-P1-4 regression: expected projected serviceAccountToken volume to be rejected when automountServiceAccountToken=false")
		}

		privTrue := true
		initPrivSpec := &corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name:            "evil-init",
					Image:           "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{Privileged: &privTrue},
				},
			},
			Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", initPrivSpec, nil); err == nil {
			t.Fatalf("R8-P1-4 regression: expected privileged InitContainer to be rejected under baseline")
		}

		hostPortSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "c",
					Image: "alpine:3.19",
					Ports: []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", hostPortSpec, nil); err == nil {
			t.Fatalf("R8-P1-4 regression: expected non-zero HostPort to be rejected under baseline")
		}

		// Verify capabilities.add=[NET_BIND_SERVICE] without drop gets default drop=[ALL] under restricted
		capAddSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "c",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"NET_BIND_SERVICE"}},
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "restricted", capAddSpec, nil); err != nil {
			t.Fatalf("R8-P1-4 regression: expected capabilities.add=[NET_BIND_SERVICE] to receive default drop=[ALL] under restricted, got %v", err)
		}

		// Verify default SA with AutomountServiceAccountToken=true is rejected when Executor CR does not list "default" or "*"
		restrictedExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "no-default-sa-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:               xxljobv1alpha1.LocalObjectReference{Name: "prod-admin"},
				AppName:                "no-default-sa-exec",
				Title:                  "No Default SA",
				Mode:                   xxljobv1alpha1.ExecutorModeK8sJobProxy,
				AllowedServiceAccounts: []string{"dedicated-runner"},
			},
		}
		_ = fakeK8s.Create(context.Background(), restrictedExec)
		autoTrue := true
		defaultAutoTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "default-auto-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "no-default-sa-exec"},
				ExecutorHandler: "defaultAutoHandler",
				K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
					Image:                        "alpine:3.19",
					AutomountServiceAccountToken: &autoTrue,
				},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 406, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), defaultAutoTask)
		autoRet := postJSON("/proxy/default/no-default-sa-exec/run", xxlclient.TriggerParam{
			JobID:           406,
			ExecutorHandler: "defaultAutoHandler",
			LogID:           9602,
		})
		if autoRet.IsSuccess() {
			t.Fatalf("R8-P1-4 regression: expected default SA with automountServiceAccountToken=true to be rejected when not in AllowedServiceAccounts")
		}
	})

	// 20. Test R9-P1-1: Failed Job (DeadlineExceeded) with terminating/active Pod blocks SERIAL_EXECUTION and DISCARD_LATER until Pod exits
	t.Run("FailedJobWithTerminatingPodBlocksSerialQueueAndDiscardLater", func(t *testing.T) {
		r9Task := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "r9-deadline-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "r9DeadlineHandler",
				ExecutorBlockStrategy: "DISCARD_LATER",
				K8sJobTemplate:        &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 407, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), r9Task)

		suspFalse := false
		suspTrue := true
		termOne := int32(1)
		failedJobName := BuildJobName("k8s-proxy-exec", 407, 9701)
		failedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      failedJobName,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: "k8s-proxy-exec",
					LabelJobID:        "407",
					LabelLogID:        "9701",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy: "SERIAL_EXECUTION",
					AnnotationLogID:         "9701",
					AnnotationLogDateTime:   "1700000009701",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspFalse,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
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
		queuedJobName := BuildJobName("k8s-proxy-exec", 407, 9702)
		queuedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      queuedJobName,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: "k8s-proxy-exec",
					LabelJobID:        "407",
					LabelLogID:        "9702",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy: "SERIAL_EXECUTION",
					AnnotationLogID:         "9702",
					AnnotationLogDateTime:   "1700000009702",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspTrue,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
			},
		}
		_ = fakeK8s.Create(context.Background(), failedJob)
		_ = fakeK8s.Create(context.Background(), queuedJob)

		// While failedJob still has Terminating=1, AdvanceSerialQueueWithReader must NOT unsuspend queuedJob
		if err := AdvanceSerialQueueWithReader(context.Background(), fakeK8s, fakeK8s, "default", "k8s-proxy-exec", "407"); err != nil {
			t.Fatalf("AdvanceSerialQueueWithReader failed: %v", err)
		}
		var checkQueued batchv1.Job
		_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: queuedJobName}, &checkQueued)
		if checkQueued.Spec.Suspend == nil || !*checkQueued.Spec.Suspend {
			t.Fatalf("R9-P1-1 regression: queued SERIAL_EXECUTION job was unsuspended while timed-out Job still had terminating Pod")
		}

		// DISCARD_LATER must also reject new triggers while failedJob has Terminating=1
		dlRet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 407,
			ExecutorHandler:       "r9DeadlineHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9703,
			LogDateTime:           1700000009703,
		})
		if dlRet.IsSuccess() {
			t.Fatalf("R9-P1-1 regression: DISCARD_LATER allowed concurrent trigger while timed-out Job still had terminating Pod")
		}

		// Once Terminating reaches 0, AdvanceSerialQueueWithReader unsuspends queuedJob
		termZero := int32(0)
		failedJob.Status.Terminating = &termZero
		_ = fakeK8s.Status().Update(context.Background(), failedJob)
		if err := AdvanceSerialQueueWithReader(context.Background(), fakeK8s, fakeK8s, "default", "k8s-proxy-exec", "407"); err != nil {
			t.Fatalf("AdvanceSerialQueueWithReader after pod exit failed: %v", err)
		}
		_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: queuedJobName}, &checkQueued)
		if checkQueued.Spec.Suspend != nil && *checkQueued.Spec.Suspend {
			t.Fatalf("R9-P1-1 regression: expected queued SERIAL_EXECUTION job to be unsuspended once terminating Pod exited")
		}
	})

	// 21. Test R9-P1-3 & R9-P2-3: AlreadyExists aborted/completed Job is not resurrected, and EphemeralContainer RunAsNonRoot defaults
	t.Run("AlreadyExistsAbortedJobNotResurrectedAndEphemeralContainerDefaults", func(t *testing.T) {
		abortedJobName := BuildJobName("k8s-proxy-exec", 405, 9710)
		suspTrue := true
		abortedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      abortedJobName,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: "k8s-proxy-exec",
					LabelJobID:        "405",
					LabelLogID:        "9710",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy:  "DISCARD_LATER",
					AnnotationLogID:          "9710",
					AnnotationLogDateTime:    "1700000009710",
					AnnotationFailureMessage: "aborted due to interrupted trigger",
				},
				CreationTimestamp: metav1.Now(),
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspTrue,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
			},
		}
		_ = fakeK8s.Create(context.Background(), abortedJob)

		resurrectRet := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 405,
			ExecutorHandler:       "dlIdempotentHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 9710,
			LogDateTime:           1700000009710,
		})
		if resurrectRet.IsSuccess() {
			t.Fatalf("R9-P1-3 regression: expected retry of aborted Job with AnnotationFailureMessage to be rejected")
		}
		var verifyAborted batchv1.Job
		_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: abortedJobName}, &verifyAborted)
		if verifyAborted.Spec.Suspend == nil || !*verifyAborted.Spec.Suspend {
			t.Fatalf("R9-P1-3 regression: aborted Job was unsuspended/resurrected on AlreadyExists retry")
		}

		// R9-P2-3: When Containers have explicit runAsNonRoot=true and EphemeralContainer omits runAsNonRoot,
		// applyRestrictedDefaults sets pod-level RunAsNonRoot=true so restricted validation passes.
		nonRootTrue := true
		ephemeralSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "main",
					Image:           "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{RunAsNonRoot: &nonRootTrue},
				},
			},
			EphemeralContainers: []corev1.EphemeralContainer{
				{
					EphemeralContainerCommon: corev1.EphemeralContainerCommon{
						Name:  "debug",
						Image: "busybox:1.36",
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "restricted", ephemeralSpec, nil); err != nil {
			t.Fatalf("R9-P2-3 regression: expected EphemeralContainer without explicit RunAsNonRoot to inherit pod-level default RunAsNonRoot=true, got %v", err)
		}
	})

	// 22. Test R10-P1-3 & R10-P2-4: AdvanceSerialQueueWithReader candidate fallback, handleLog failure/error handling, negative LogID env normalization, and COVER_EARLY queue depth reset
	t.Run("Round10SerialQueueCandidateFallbackAndLogAndCoverEarlyMetrics", func(t *testing.T) {
		suspTrue := true
		execLbl := sanitizeLabel("k8s-proxy-exec")
		cand0Name := BuildJobName("k8s-proxy-exec", 408, 9801)
		cand1Name := BuildJobName("k8s-proxy-exec", 408, 9802)
		cand0 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cand0Name,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: execLbl,
					LabelJobID:        "408",
					LabelLogID:        "9801",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy: "SERIAL_EXECUTION",
					AnnotationLogID:         "9801",
					AnnotationLogDateTime:   "1700000009801",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspTrue,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
			},
		}
		cand1 := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cand1Name,
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: execLbl,
					LabelJobID:        "408",
					LabelLogID:        "9802",
				},
				Annotations: map[string]string{
					AnnotationBlockStrategy: "SERIAL_EXECUTION",
					AnnotationLogID:         "9802",
					AnnotationLogDateTime:   "1700000009802",
				},
			},
			Spec: batchv1.JobSpec{
				Suspend: &suspTrue,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
					},
				},
			},
		}
		_ = fakeK8s.Create(context.Background(), cand0)
		_ = fakeK8s.Create(context.Background(), cand1)

		// Create a split reader/writer where reader (fakeK8s) still lists [cand0, cand1] as suspended,
		// while writer (apiSideClient) has already deleted cand0 concurrently!
		apiSideClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cand1.DeepCopy()).Build()

		if err := AdvanceSerialQueueWithReader(context.Background(), fakeK8s, apiSideClient, "default", execLbl, "408"); err != nil {
			t.Fatalf("R10-P1-3: AdvanceSerialQueueWithReader failed: %v", err)
		}
		var checkCand1 batchv1.Job
		_ = apiSideClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: cand1Name}, &checkCand1)
		if checkCand1.Spec.Suspend != nil && *checkCand1.Spec.Suspend {
			t.Fatalf("R10-P1-3 regression: expected AdvanceSerialQueueWithReader to skip deleted/aborted cand0 and unsuspend cand1 in the same call")
		}

		// R10-P2-4 (a): handleLog returns AnnotationFailureMessage with IsEnd=true when 0 Pods exist on an aborted Job
		abortedJobForLog := cand0.DeepCopy()
		abortedJobForLog.Annotations[AnnotationFailureMessage] = "task aborted before Pod creation"
		_ = fakeK8s.Update(context.Background(), abortedJobForLog)
		logRet := postJSON("/proxy/default/k8s-proxy-exec/log", xxlclient.LogParam{
			LogDateTim:  1700000009801,
			LogID:       9801,
			FromLineNum: 1,
		})
		if !logRet.IsSuccess() {
			t.Fatalf("expected /log on aborted Pod-less Job to return ReturnT 200 with LogResult, got %+v", logRet)
		}
		var lr xxlclient.LogResult
		_ = json.Unmarshal(logRet.Content, &lr)
		if !lr.IsEnd || !strings.Contains(lr.LogContent, "task aborted before Pod creation") {
			t.Fatalf("R10-P2-4 regression: expected /log on aborted Pod-less Job to return IsEnd=true with AnnotationFailureMessage, got %+v", lr)
		}

		// R10-P2-4 (b): handleLog returns FailCode when FetchPodLog fails on a Running Pod
		runningPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-9802",
				Namespace: "default",
				Labels: map[string]string{
					LabelManagedBy:    ManagedByValue,
					LabelExecutorName: execLbl,
					LabelLogID:        "9802",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		_ = fakeK8s.Create(context.Background(), runningPod)
		srv.FetchPodLog = func(ctx context.Context, namespace, podName, containerName string) (string, error) {
			return "", fmt.Errorf("kubelet stream connection reset")
		}
		errLogRet := postJSON("/proxy/default/k8s-proxy-exec/log", xxlclient.LogParam{
			LogDateTim:  1700000009802,
			LogID:       9802,
			FromLineNum: 1,
		})
		if errLogRet.IsSuccess() {
			t.Fatalf("R10-P2-4 regression: expected /log to return FailCode when FetchPodLog fails on a Running Pod")
		}

		// R10-P2-4 (c): buildBatchJob normalizes XXL_JOB_LOG_ID env var when param.LogID <= 0
		builtJob, err := srv.buildBatchJob(xxlclient.TriggerParam{
			JobID:           408,
			ExecutorHandler: "demoJobHandler",
			LogID:           0,
			LogDateTime:     1700000009803,
		}, task)
		if err != nil {
			t.Fatalf("buildBatchJob failed: %v", err)
		}
		var gotEnvLogID string
		for _, env := range builtJob.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "XXL_JOB_LOG_ID" {
				gotEnvLogID = env.Value
			}
		}
		if gotEnvLogID == "" || gotEnvLogID == "0" {
			t.Fatalf("R10-P2-4 regression: expected XXL_JOB_LOG_ID env var to use normalized positive LogID (%s), got %q",
				builtJob.Annotations[AnnotationLogID], gotEnvLogID)
		}

		// R10-P2-4 (d): killRunningJobs resets OnSerialQueueDepth to 0
		lastDepth := -1.0
		OnSerialQueueDepth = func(ns, ex, jid string, depth float64) {
			lastDepth = depth
		}
		defer func() { OnSerialQueueDepth = nil }()
		_ = srv.killRunningJobs(context.Background(), "default", "k8s-proxy-exec", 408, "")
		if lastDepth != 0 {
			t.Fatalf("R10-P2-4 regression: expected killRunningJobs to reset OnSerialQueueDepth to 0, got %v", lastDepth)
		}
	})

	// 23. Test R11-P1-2 & R11-P2-2:
	//  (a) COVER_EARLY late duplicate/replay of an already-completed or already-failed trigger does NOT kill a newer running Job
	//  (b) findMatchingTask rejects an unmigrated XxlJobTask whose Status.JobGroupID or Status.AdminURL disagrees with XxlJobExecutor.Status
	//  (c) ApplyAndValidatePodSecurity under restricted/baseline rejects explicit container SeccompProfile.Type=Unconfined even when Pod SeccompProfile=RuntimeDefault
	t.Run("Round11CoverEarlyDuplicateReplayAndTaskConsistencyAndContainerSeccomp", func(t *testing.T) {
		r11Task := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-ce-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "k8s-proxy-exec"},
				ExecutorHandler:       "r11CeHandler",
				ExecutorBlockStrategy: "COVER_EARLY",
				K8sJobTemplate:        &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 411, Synced: true},
		}
		_ = fakeK8s.Create(context.Background(), r11Task)

		// Trigger #1 (logId=9911) runs and finishes callback delivery (retained in cluster for TTLSecondsAfterFinished)
		ret1 := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 411,
			ExecutorHandler:       "r11CeHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 9911,
			LogDateTime:           1700000009911,
		})
		if !ret1.IsSuccess() {
			t.Fatalf("expected trigger #1 (9911) to succeed, got %+v", ret1)
		}
		var job9911 batchv1.Job
		job9911Name := BuildJobName("k8s-proxy-exec", 411, 9911)
		_ = fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: job9911Name}, &job9911)
		job9911.Annotations[AnnotationCallbackSent] = "true"
		job9911.Status.Succeeded = 1
		job9911.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		_ = fakeK8s.Update(context.Background(), &job9911)
		_ = fakeK8s.Status().Update(context.Background(), &job9911)

		// Trigger #2 (logId=9912) starts and is currently running
		ret2 := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 411,
			ExecutorHandler:       "r11CeHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 9912,
			LogDateTime:           1700000009912,
		})
		if !ret2.IsSuccess() {
			t.Fatalf("expected trigger #2 (9912) to succeed, got %+v", ret2)
		}

		// Now a late duplicate replay for old Trigger #1 (logId=9911) arrives under COVER_EARLY:
		// It must return 500 ("already completed callback delivery") WITHOUT killing active Trigger #2 (9912)!
		replay9911 := postJSON("/proxy/default/k8s-proxy-exec/run", xxlclient.TriggerParam{
			JobID:                 411,
			ExecutorHandler:       "r11CeHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 9911,
			LogDateTime:           1700000009911,
		})
		if replay9911.IsSuccess() {
			t.Fatalf("expected late replay of completed logId=9911 to return 500, got %+v", replay9911)
		}
		var verify9912 batchv1.Job
		job9912Name := BuildJobName("k8s-proxy-exec", 411, 9912)
		if err := fakeK8s.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: job9912Name}, &verify9912); err != nil {
			t.Fatalf("R11-P1-2 regression: late COVER_EARLY duplicate replay for completed logId=9911 killed newer running Job 9912: %v", err)
		}

		// (b) R11-P1-2: Verify findMatchingTask rejects a task whose Status.JobGroupID mismatches XxlJobExecutor.Status.JobGroupID
		r11ExecSynced := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-strict-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AppName: "r11-strict-app",
				Mode:    xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 77,
				AdminURL:   "http://admin-b:8080/xxl-job-admin",
				Synced:     true,
			},
		}
		_ = fakeK8s.Create(context.Background(), r11ExecSynced)
		staleTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "r11-stale-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "r11-strict-exec"},
				ExecutorHandler: "staleHandler",
				K8sJobTemplate:  &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      412,
				JobGroupID: 12, // Stale group ID from previous executor/Admin
				AdminURL:   "http://admin-a:8080/xxl-job-admin",
				Synced:     true,
			},
		}
		_ = fakeK8s.Create(context.Background(), staleTask)
		staleRet := postJSON("/proxy/default/r11-strict-exec/run", xxlclient.TriggerParam{
			JobID:           412,
			ExecutorHandler: "staleHandler",
			LogID:           9913,
			LogDateTime:     1700000009913,
		})
		if staleRet.IsSuccess() {
			t.Fatalf("R11-P1-2 regression: expected findMatchingTask to reject stale XxlJobTask whose Status.JobGroupID/AdminURL disagrees with XxlJobExecutor.Status")
		}

		// (c) R11-P2-2: Verify restricted & baseline PodSecurity reject container-level SeccompProfile.Type=Unconfined even when Pod SeccompProfile=RuntimeDefault
		badContainerSeccompSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "job-runner",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileType("CustomNonStandard"),
						},
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "restricted", badContainerSeccompSpec, nil); err == nil {
			t.Fatalf("R11-P2-2 regression: expected restricted PodSecurity to reject container with non-standard SeccompProfile.Type even when pod SeccompProfile=RuntimeDefault")
		}

		// (d) R12-P2-2: Verify Localhost seccompProfile and appArmorProfile without localhostProfile are rejected
		emptyLocalhostSeccomp := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "job-runner",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeLocalhost,
						},
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", emptyLocalhostSeccomp, nil); err == nil {
			t.Fatalf("R12-P2-2 regression: expected baseline PodSecurity to reject SeccompProfileTypeLocalhost with nil LocalhostProfile")
		}
		emptyLocalhostAppArmor := &corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				AppArmorProfile: &corev1.AppArmorProfile{
					Type: corev1.AppArmorProfileTypeLocalhost,
				},
			},
			Containers: []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", emptyLocalhostAppArmor, nil); err == nil {
			t.Fatalf("R12-P2-2 regression: expected baseline PodSecurity to reject AppArmorProfileTypeLocalhost with nil LocalhostProfile")
		}

		// (e) R12-P1-1: Verify isJobBlockingExecution returns false once a completed Job with phantom Terminating=1 has callback-sent=true and no JobCallbackFinalizer
		termOne := int32(1)
		bypassedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bypassed-phantom-job",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationCallbackSent: "true",
				},
			},
			Status: batchv1.JobStatus{
				Failed:      1,
				Terminating: &termOne,
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if isJobBlockingExecution(bypassedJob) {
			t.Fatalf("R12-P1-1 regression: expected isJobBlockingExecution=false for completed Job with callback-sent=true and JobCallbackFinalizer removed")
		}

		// Verify force-delete=true on a deleting/completed Job returns isJobBlockingExecution=false even while JobCallbackFinalizer is still present:
		nowDel := metav1.Now()
		forceDelWithFinalizer := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "force-del-with-finalizer-job",
				Namespace:         "default",
				DeletionTimestamp: &nowDel,
				Finalizers:        []string{JobCallbackFinalizer},
				Annotations: map[string]string{
					"xxljob.io/force-delete": "true",
				},
			},
			Status: batchv1.JobStatus{
				Active:      1,
				Terminating: &termOne,
			},
		}
		if isJobBlockingExecution(forceDelWithFinalizer) {
			t.Fatalf("expected isJobBlockingExecution=false for deleting Job with force-delete=true even while JobCallbackFinalizer is still present")
		}

		// Verify legacy AppArmor/Seccomp annotations with empty localhost/ or custom values are rejected under baseline:
		validSpec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "job-runner", Image: "alpine:3.19"}},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", validSpec, map[string]string{
			"container.apparmor.security.beta.kubernetes.io/job-runner": "localhost/",
		}); err == nil {
			t.Fatalf("expected baseline PodSecurity to reject empty localhost/ AppArmor annotation")
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "baseline", validSpec, map[string]string{
			"seccomp.security.alpha.kubernetes.io/pod": "custom-invalid-profile",
		}); err == nil {
			t.Fatalf("expected baseline PodSecurity to reject invalid seccomp annotation")
		}

		// Verify restricted PodSecurity does not overwrite Pod-level SeccompProfile when all containers explicitly specify SeccompProfile:
		localProf := "my-seccomp.json"
		explicitContainerSeccompSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "job-runner",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						SeccompProfile: &corev1.SeccompProfile{
							Type:             corev1.SeccompProfileTypeLocalhost,
							LocalhostProfile: &localProf,
						},
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(context.Background(), fakeK8s, "default", "restricted", explicitContainerSeccompSpec, nil); err != nil {
			t.Fatalf("expected restricted PodSecurity to accept explicit container-level Localhost SeccompProfile, got %v", err)
		}
		if explicitContainerSeccompSpec.SecurityContext != nil && explicitContainerSeccompSpec.SecurityContext.SeccompProfile != nil {
			t.Fatalf("expected Pod-level SeccompProfile to remain nil when all containers explicitly configure SeccompProfile")
		}
	})

	// 16. Test Endgame Round 2 Fixes (EG2-10, EG2-11, EG2-12):
	// - Reject /run when XxlJobExecutor has Mode != K8sJobProxy or is terminating
	// - Default non-root RunAsUser (65534) on fallback GLUE image under restricted PSS & stamp originating Admin annotations
	// - Multi-replica COVER_EARLY CAS barrier & out-of-order older logId suppression
	// - CAP_ prefix / case normalization and restricted non-empty LocalhostProfile validation
	t.Run("Endgame2ModeGuardCoverEarlyCASAndPodSecurity", func(t *testing.T) {
		ctx := context.Background()

		// Two independent ProxyExecutor replicas sharing fakeK8s (APIServer) with separate in-memory jobLocks maps
		srvReplica1 := NewServer(fakeK8s, ":0", "default", "strong-proxy-token")
		tsReplica1 := httptest.NewServer(srvReplica1.Handler())
		defer tsReplica1.Close()

		srvReplica2 := NewServer(fakeK8s, ":0", "default", "strong-proxy-token")
		tsReplica2 := httptest.NewServer(srvReplica2.Handler())
		defer tsReplica2.Close()

		postToReplica := func(baseURL, execName string, param xxlclient.TriggerParam) xxlclient.ReturnT {
			body, _ := json.Marshal(param)
			req, _ := http.NewRequest(http.MethodPost, baseURL+"/proxy/default/"+execName+"/run", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "strong-proxy-token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("postToReplica failed: %v", err)
			}
			defer resp.Body.Close()
			var out xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&out)
			return out
		}

		// (a) EG2-10: Reject Resident-mode and terminating XxlJobExecutor CRs on /run
		residentExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-res-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AppName: "eg2-res-app",
				Mode:    xxljobv1alpha1.ExecutorModeResident,
			},
		}
		terminatingExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "eg2-term-exec",
				Namespace:  "default",
				Finalizers: []string{"xxljob.io/executor-finalizer"},
			},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AppName: "eg2-term-app",
				Mode:    xxljobv1alpha1.ExecutorModeK8sJobProxy,
			},
		}
		residentTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-res-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: residentExec.Name},
				ExecutorHandler: "resHandler",
				K8sJobTemplate:  &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{JobID: 501, Synced: true},
		}
		_ = fakeK8s.Create(ctx, residentExec)
		_ = fakeK8s.Create(ctx, terminatingExec)
		_ = fakeK8s.Delete(ctx, terminatingExec)
		_ = fakeK8s.Create(ctx, residentTask)
		resRet := postToReplica(tsReplica1.URL, residentExec.Name, xxlclient.TriggerParam{
			JobID:           501,
			ExecutorHandler: "resHandler",
			LogID:           30001,
			LogDateTime:     1700000030001,
		})
		if resRet.IsSuccess() || !strings.Contains(resRet.Msg, "does not accept K8sJobProxy triggers") {
			t.Fatalf("EG2-10 regression: expected /run to reject XxlJobExecutor with mode=Resident via validateProxyExecutorCR, got %+v", resRet)
		}
		termRet := postToReplica(tsReplica1.URL, terminatingExec.Name, xxlclient.TriggerParam{
			JobID:           501,
			ExecutorHandler: "resHandler",
			LogID:           30009,
			LogDateTime:     1700000030009,
		})
		if termRet.IsSuccess() || !strings.Contains(termRet.Msg, "terminating") {
			t.Fatalf("EG2-10 regression: expected /run to reject terminating XxlJobExecutor via validateProxyExecutorCR, got %+v", termRet)
		}

		// (b) EG2-10: Fallback GLUE image under restricted PSS gets RunAsUser=65534 & stamps Admin annotations
		glueProxyExec := &xxljobv1alpha1.XxlJobExecutor{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-glue-exec", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobExecutorSpec{
				AdminRef:         xxljobv1alpha1.LocalObjectReference{Name: "eg2-admin"},
				AppName:          "eg2-glue-app",
				Mode:             xxljobv1alpha1.ExecutorModeK8sJobProxy,
				PodSecurityLevel: "restricted",
			},
			Status: xxljobv1alpha1.XxlJobExecutorStatus{
				JobGroupID: 88,
				AdminURL:   "http://eg2-admin.default.svc:8080/xxl-job-admin/",
				Synced:     true,
			},
		}
		glueFallbackTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-glue-fallback-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: glueProxyExec.Name},
				ExecutorHandler: "glueFallback",
				GlueType:        "GLUE_PYTHON",
				GlueSource:      "print('ok')",
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      502,
				JobGroupID: 88,
				AdminURL:   "http://eg2-admin.default.svc:8080/xxl-job-admin",
				Synced:     true,
			},
		}
		_ = fakeK8s.Create(ctx, glueProxyExec)
		_ = fakeK8s.Create(ctx, glueFallbackTask)
		glueRet := postToReplica(tsReplica1.URL, glueProxyExec.Name, xxlclient.TriggerParam{
			JobID:                 502,
			ExecutorHandler:       "glueFallback",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 30002,
			LogDateTime:           1700000030002,
		})
		if !glueRet.IsSuccess() {
			t.Fatalf("expected fallback GLUE_PYTHON trigger to succeed: %s", glueRet.Msg)
		}
		var createdGlueJob batchv1.Job
		if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName(glueProxyExec.Name, 502, 30002)}, &createdGlueJob); err != nil {
			t.Fatalf("get createdGlueJob failed: %v", err)
		}
		if createdGlueJob.Spec.Template.Spec.SecurityContext == nil ||
			createdGlueJob.Spec.Template.Spec.SecurityContext.RunAsUser == nil ||
			*createdGlueJob.Spec.Template.Spec.SecurityContext.RunAsUser != 65534 {
			t.Fatalf("EG2-10 regression: expected fallback GLUE image under restricted PSS to default RunAsUser=65534, got %+v",
				createdGlueJob.Spec.Template.Spec.SecurityContext)
		}
		if createdGlueJob.Annotations[AnnotationAdminName] != "eg2-admin" ||
			createdGlueJob.Annotations[AnnotationAdminURL] != "http://eg2-admin.default.svc:8080/xxl-job-admin" {
			t.Fatalf("EG2-10 regression: expected stamped AnnotationAdminName and AnnotationAdminURL, got %v", createdGlueJob.Annotations)
		}

		// (c) EG2-11: Multi-replica COVER_EARLY concurrent race & out-of-order older logId suppression
		coverTask := &xxljobv1alpha1.XxlJobTask{
			ObjectMeta: metav1.ObjectMeta{Name: "eg2-cover-race-task", Namespace: "default"},
			Spec: xxljobv1alpha1.XxlJobTaskSpec{
				ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: glueProxyExec.Name},
				ExecutorHandler:       "coverRaceHandler",
				ExecutorBlockStrategy: "COVER_EARLY",
				K8sJobTemplate:        &xxljobv1alpha1.K8sJobTemplateSpec{Image: "alpine:3.19"},
			},
			Status: xxljobv1alpha1.XxlJobTaskStatus{
				JobID:      503,
				JobGroupID: 88,
				AdminURL:   "http://eg2-admin.default.svc:8080/xxl-job-admin",
				Synced:     true,
			},
		}
		_ = fakeK8s.Create(ctx, coverTask)

		postTo := func(baseURL string, param xxlclient.TriggerParam) xxlclient.ReturnT {
			return postToReplica(baseURL, glueProxyExec.Name, param)
		}

		// Fire logId=31002 first, then delayed out-of-order logId=31001 on the other replica:
		if r := postTo(tsReplica1.URL, xxlclient.TriggerParam{
			JobID:                 503,
			ExecutorHandler:       "coverRaceHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 31002,
			LogDateTime:           1700000031002,
		}); !r.IsSuccess() {
			t.Fatalf("expected COVER_EARLY logId=31002 to succeed: %s", r.Msg)
		}
		if r := postTo(tsReplica2.URL, xxlclient.TriggerParam{
			JobID:                 503,
			ExecutorHandler:       "coverRaceHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 31001,
			LogDateTime:           1700000031001,
		}); r.IsSuccess() {
			t.Fatalf("EG2-11 regression: out-of-order older COVER_EARLY logId=31001 succeeded and killed newer active Job!")
		}
		var verify31002 batchv1.Job
		if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName(glueProxyExec.Name, 503, 31002)}, &verify31002); err != nil ||
			!verify31002.DeletionTimestamp.IsZero() || (verify31002.Spec.Suspend != nil && *verify31002.Spec.Suspend) {
			t.Fatalf("EG2-11 regression: newer active Job 31002 was killed or remained suspended: err=%v job=%+v", err, verify31002)
		}

		// Now launch 6 concurrent COVER_EARLY triggers (logId=31101..31106) across the 2 independent replicas:
		var wg sync.WaitGroup
		for i := 1; i <= 6; i++ {
			wg.Add(1)
			lid := int64(31100 + i)
			targetURL := tsReplica1.URL
			if i%2 == 0 {
				targetURL = tsReplica2.URL
			}
			go func(u string, logID int64) {
				defer wg.Done()
				_ = postTo(u, xxlclient.TriggerParam{
					JobID:                 503,
					ExecutorHandler:       "coverRaceHandler",
					ExecutorBlockStrategy: "COVER_EARLY",
					LogID:                 logID,
					LogDateTime:           1700000000000 + logID,
				})
			}(targetURL, lid)
		}
		wg.Wait()

		var jobList503 batchv1.JobList
		_ = fakeK8s.List(ctx, &jobList503, client.InNamespace("default"), client.MatchingLabels{LabelJobID: "503"})
		var activeWinnerNames []string
		for i := range jobList503.Items {
			j := &jobList503.Items[i]
			if j.DeletionTimestamp.IsZero() && isJobRunning(j) {
				activeWinnerNames = append(activeWinnerNames, j.Name)
			}
		}
		expectedWinner := BuildJobName(glueProxyExec.Name, 503, 31106)
		if len(activeWinnerNames) != 1 || activeWinnerNames[0] != expectedWinner {
			t.Fatalf("EG2-11 regression: expected sole surviving COVER_EARLY Job %s across multi-replica burst, got %v", expectedWinner, activeWinnerNames)
		}

		// (d) EG2-12: CAP_ prefix normalization & restricted empty LocalhostProfile rejection
		capPrefixedSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "job-runner",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"cap_all"},
							Add:  []corev1.Capability{"CAP_NET_BIND_SERVICE"},
						},
					},
				},
			},
		}
		if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "restricted", capPrefixedSpec, nil); err != nil {
			t.Fatalf("EG2-12 regression: expected CAP_NET_BIND_SERVICE and cap_all to normalize cleanly under restricted PSS, got %v", err)
		}
		emptyRestrictedLocalhost := &corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: func() *bool { b := true; return &b }(),
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeLocalhost,
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "job-runner",
					Image: "alpine:3.19",
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: func() *bool { b := false; return &b }(),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
				},
			},
		}
		if err := validateRestrictedPodSecurity(emptyRestrictedLocalhost); err == nil {
			t.Fatalf("EG2-12 regression: expected validateRestrictedPodSecurity to reject empty LocalhostProfile")
		}
	})
}

func TestBoostEndgameProxyPodSecurityOverridesLabelsAndCoverEarlyGC(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	boolPtr := func(b bool) *bool { return &b }

	// (a) PodSecurity restricted: Pod runAsNonRoot=false overridden by Container runAsNonRoot=true
	validOverrideSpec := &corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(false),
		},
		Containers: []corev1.Container{
			{
				Name:  "job-runner",
				Image: "alpine:3.19",
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: boolPtr(true),
				},
			},
		},
		InitContainers: []corev1.Container{
			{
				Name:  "init-step",
				Image: "alpine:3.19",
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: boolPtr(true),
				},
			},
		},
	}
	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "restricted", validOverrideSpec, nil); err != nil {
		t.Fatalf("expected Pod runAsNonRoot=false with all containers explicitly setting runAsNonRoot=true to pass restricted PSS, got: %v", err)
	}

	// Reject when Pod runAsNonRoot=false and an initContainer omits runAsNonRoot
	missingInitOverride := validOverrideSpec.DeepCopy()
	missingInitOverride.InitContainers[0].SecurityContext.RunAsNonRoot = nil
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "restricted", missingInitOverride, nil); err == nil {
		t.Fatalf("expected restricted PSS to reject Pod runAsNonRoot=false when initContainer omits runAsNonRoot")
	}

	// Reject when Pod runAsNonRoot=false and 0 containers exist
	emptyContainersSpec := &corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: boolPtr(false)},
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "restricted", emptyContainersSpec, nil); err == nil {
		t.Fatalf("expected restricted PSS to reject Pod runAsNonRoot=false with 0 containers")
	}

	// (b) Empty Secret names, sysctl annotations, and Windows HostProcess / ContainerAdministrator
	emptyVolSecret := &corev1.PodSpec{
		Volumes:    []corev1.Volume{{Name: "sec", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "   "}}}},
		Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}},
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "privileged", emptyVolSecret, nil); err == nil {
		t.Fatalf("expected empty volume SecretName to be rejected")
	}
	emptyInitEnvSecret := &corev1.PodSpec{
		InitContainers: []corev1.Container{{
			Name:  "init",
			Image: "alpine:3.19",
			Env: []corev1.EnvVar{{
				Name: "S",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{Key: "k"},
				},
			}},
		}},
		Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}},
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "privileged", emptyInitEnvSecret, nil); err == nil {
		t.Fatalf("expected empty SecretKeyRef name in initContainer to be rejected")
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "baseline",
		&corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}}},
		map[string]string{"security.alpha.kubernetes.io/unsafe-sysctls": "kernel.shm_rmid_forced=1"}); err == nil {
		t.Fatalf("expected baseline PSS to reject unsafe-sysctls annotation")
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "baseline",
		&corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}}},
		map[string]string{"security.alpha.kubernetes.io/sysctls": "kernel.msgmax=8192"}); err == nil {
		t.Fatalf("expected baseline PSS to reject non-safe sysctl in sysctls annotation")
	}
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "baseline",
		&corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "alpine:3.19"}}},
		map[string]string{"security.alpha.kubernetes.io/sysctls": "net.ipv4.ip_local_port_range=1024 65535"}); err != nil {
		t.Fatalf("expected safe sysctl annotation to pass baseline PSS, got %v", err)
	}
	winAdmin := "ContainerAdministrator"
	if err := ApplyAndValidatePodSecurity(ctx, fakeK8s, "default", "restricted",
		&corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "c",
				Image: "alpine:3.19",
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot:   boolPtr(true),
					WindowsOptions: &corev1.WindowsSecurityContextOptions{RunAsUserName: &winAdmin},
				},
			}},
		}, nil); err == nil {
		t.Fatalf("expected restricted PSS to reject Windows ContainerAdministrator")
	}

	// (c) Fallback GLUE task with Pod runAsNonRoot=false + Container runAsNonRoot=true sets Container RunAsUser=65534,
	// strips xxljob.io/* from user PodLabels/PodAnnotations, filters handleLog by job-name,
	// and rejects delayed COVER_EARLY logId after newer LastExecutionJobName Job was TTL-GC'd.
	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "boost-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef:         xxljobv1alpha1.LocalObjectReference{Name: "admin"},
			AppName:          "boost-proxy-app",
			Mode:             xxljobv1alpha1.ExecutorModeK8sJobProxy,
			PodSecurityLevel: "restricted",
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 11,
			AdminURL:   "http://admin.default.svc:8080/xxl-job-admin",
			Synced:     true,
		},
	}
	glueTask := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "boost-glue-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "boost-proxy"},
			JobDesc:               "Boost GLUE",
			Author:                "dev",
			ScheduleType:          "CRON",
			ScheduleConf:          "0 * * * * ?",
			ExecutorBlockStrategy: "COVER_EARLY",
			GlueType:              "GLUE_SHELL",
			GlueSource:            "echo hi",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsNonRoot: boolPtr(false),
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: boolPtr(true),
				},
				PodLabels: map[string]string{
					"xxljob.io/managed-by": "spoofed",
					"xxljob.io/extra":      "forbidden",
					"team":                 "payments",
				},
				PodAnnotations: map[string]string{
					"xxljob.io/callback-sent": "true",
					"custom.org/note":         "kept",
				},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      701,
			JobGroupID: 11,
			AdminURL:   "http://admin.default.svc:8080/xxl-job-admin",
			Synced:     true,
		},
	}
	_ = fakeK8s.Create(ctx, execCR)
	_ = fakeK8s.Create(ctx, glueTask)

	srv := NewServer(fakeK8s, ":0", "default", "  boost-static-tok\r\n ")
	srv.FetchPodLog = func(_ context.Context, _, podName, _ string) (string, error) {
		return "pod=" + podName + "\n", nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/boost-proxy/"+action, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, " \tboost-static-tok  ")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	retRun := post("run", xxlclient.TriggerParam{
		JobID:                 701,
		GlueType:              "GLUE_SHELL",
		GlueSource:            "echo hi",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 90050,
	})
	if !retRun.IsSuccess() {
		t.Fatalf("expected GLUE run with container runAsNonRoot=true override to succeed, got %+v", retRun)
	}

	jobName90050 := BuildJobName("boost-proxy", 701, 90050)
	var job90050 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobName90050}, &job90050); err != nil {
		t.Fatalf("get job90050 failed: %v", err)
	}
	cSC := job90050.Spec.Template.Spec.Containers[0].SecurityContext
	if cSC == nil || cSC.RunAsUser == nil || *cSC.RunAsUser != 65534 {
		t.Fatalf("expected fallback GLUE container RunAsUser=65534 when overriding pod runAsNonRoot=false, got %+v", cSC)
	}
	podTplMeta := job90050.Spec.Template.ObjectMeta
	if podTplMeta.Labels[LabelManagedBy] != ManagedByValue || podTplMeta.Labels["xxljob.io/extra"] != "" || podTplMeta.Labels["team"] != "payments" {
		t.Fatalf("expected xxljob.io/* stripped/authoritative and custom PodLabels kept, got %+v", podTplMeta.Labels)
	}
	if job90050.Annotations[AnnotationCallbackSent] != "false" || podTplMeta.Annotations[AnnotationCallbackSent] != "" || podTplMeta.Annotations["custom.org/note"] != "kept" {
		t.Fatalf("expected xxljob.io/* annotations stripped from PodAnnotations (job=%+v, podTpl=%+v)", job90050.Annotations, podTplMeta.Annotations)
	}

	// Verify handleLog filters Pods strictly by job-name == jobName90050
	spoofPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrong-job-pod",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("boost-proxy"),
				LabelLogID:        "90050",
				"job-name":        "unrelated-job",
			},
		},
		Spec: job90050.Spec.Template.Spec,
	}
	realPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "matching-job-pod",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("boost-proxy"),
				LabelLogID:        "90050",
				"job-name":        jobName90050,
			},
		},
		Spec: job90050.Spec.Template.Spec,
	}
	_ = fakeK8s.Create(ctx, spoofPod)
	_ = fakeK8s.Create(ctx, realPod)

	logRet := post("log", xxlclient.LogParam{LogID: 90050, FromLineNum: 1})
	var logRes xxlclient.LogResult
	_ = json.Unmarshal(logRet.Content, &logRes)
	if !strings.Contains(logRes.LogContent, "pod=matching-job-pod") {
		t.Fatalf("expected handleLog to select only pod with job-name=%s, got %+v", jobName90050, logRes)
	}

	// Simulate TTL GC deleting job90050 after fast completion while LastExecutionJobName still records jobName90050
	job90050.Finalizers = nil
	_ = fakeK8s.Update(ctx, &job90050)
	_ = fakeK8s.Delete(ctx, &job90050)

	staleRet := post("run", xxlclient.TriggerParam{
		JobID:                 701,
		GlueType:              "GLUE_SHELL",
		GlueSource:            "echo stale",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 90049,
	})
	if staleRet.IsSuccess() {
		t.Fatalf("expected delayed COVER_EARLY logId=90049 to be rejected even after newer job 90050 was GC'd, got %+v", staleRet)
	}
	var refreshedTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "boost-glue-task"}, &refreshedTask)
	if refreshedTask.Status.LastExecutionJobName != jobName90050 {
		t.Fatalf("expected LastExecutionJobName=%s preserved after rejecting stale logId=90049, got %q",
			jobName90050, refreshedTask.Status.LastExecutionJobName)
	}

	// Foreign executor Job in same namespace with colliding sanitized LabelExecutorName + LabelJobID=701
	// MUST NOT block /idleBeat (hasRunningJob) or be deleted by /kill (killRunningJobsInternal)!
	foreignRunningJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foreign-colliding-label-job",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("boost-proxy"),
				LabelJobID:        "701",
				LabelLogID:        "99999",
			},
			Annotations: map[string]string{
				AnnotationExecutorName: "other-executor",
				AnnotationLogID:        "99999",
			},
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	_ = fakeK8s.Create(ctx, foreignRunningJob)

	idleRet := post("idleBeat", xxlclient.IdleBeatParam{JobID: 701})
	if !idleRet.IsSuccess() {
		t.Fatalf("expected foreign executor Job not to block /idleBeat for boost-proxy, got %+v", idleRet)
	}
	killRet := post("kill", xxlclient.KillParam{JobID: 701})
	if !killRet.IsSuccess() {
		t.Fatalf("expected /kill to succeed: %+v", killRet)
	}
	var checkForeign batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: foreignRunningJob.Name}, &checkForeign); err != nil {
		t.Fatalf("expected foreign executor Job %s to be preserved across /kill, got err=%v", foreignRunningJob.Name, err)
	}
}

func TestProxyForegroundDeleteAndCrossAdminJobCollisionIsolation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	ctx := context.Background()

	adminA := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "admin-a",
			Namespace: "default",
			UID:       types.UID("uid-a"),
		},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://admin-a:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "mig-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{
			AdminURL: "http://admin-a:8080/xxl-job-admin",
		},
	}
	adminB := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "admin-b",
			Namespace: "default",
			UID:       types.UID("uid-b"),
		},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://admin-b:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "mig-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{
			AdminURL: "http://admin-b:8080/xxl-job-admin",
		},
	}
	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-proxy",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "admin-a"},
			AppName:  "mig-proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 10,
			AdminURL:   "http://admin-a:8080/xxl-job-admin",
			Synced:     true,
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-task",
			Namespace: "default",
		},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "mig-proxy"},
			JobDesc:         "Migrated Task",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "migHandler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo ok"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      55,
			JobGroupID: 10,
			AdminURL:   "http://admin-a:8080/xxl-job-admin",
			Synced:     true,
		},
	}

	var delMu sync.Mutex
	deletedPolicies := make(map[string]metav1.DeletionPropagation)

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminA, adminB, execCR, taskCR).
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

	srv := NewServer(fakeK8s, ":0", "default", "mig-tok")
	srv.FetchPodLog = func(ctx context.Context, namespace, podName, containerName string) (string, error) {
		return "log-from-" + podName + "\n", nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/mig-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "mig-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Trigger COVER_EARLY 1000 then preempt with 1001 on admin-a -> verify Foreground delete on 1000
	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "migHandler",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 1000,
		LogDateTime:           1700000001000,
	}); !ret.IsSuccess() {
		t.Fatalf("expected admin-a logId=1000 to succeed, got %+v", ret)
	}
	job1000Name := BuildJobName("mig-proxy", 55, 1000)
	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "migHandler",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 1001,
		LogDateTime:           1700000001001,
	}); !ret.IsSuccess() {
		t.Fatalf("expected admin-a COVER_EARLY logId=1001 to succeed, got %+v", ret)
	}
	delMu.Lock()
	pol1000 := deletedPolicies[job1000Name]
	delMu.Unlock()
	if pol1000 != metav1.DeletePropagationForeground {
		t.Fatalf("expected COVER_EARLY preemption to delete %s with DeletePropagationForeground, got %q", job1000Name, pol1000)
	}

	// Mark admin-a's 1001 Job active with a Pod
	jobA1001Name := BuildJobName("mig-proxy", 55, 1001)
	var jobA1001 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobA1001Name}, &jobA1001); err != nil {
		t.Fatalf("get admin-a job 1001: %v", err)
	}
	if jobA1001.Annotations[AnnotationAdminUID] != "uid-a" || jobA1001.Annotations[AnnotationAdminName] != "admin-a" {
		t.Fatalf("expected admin-a origin annotations stamped on job, got %+v", jobA1001.Annotations)
	}
	jobA1001.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &jobA1001)
	_ = fakeK8s.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-admin-a-1001",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("mig-proxy"),
				LabelLogID:        "1001",
				"job-name":        jobA1001Name,
			},
		},
		Spec: jobA1001.Spec.Template.Spec,
	})

	// Also create a completed historical Job 1002 on admin-a (callback already sent)
	if ret := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1002,
		LogDateTime:     1700000001002,
	}); !ret.IsSuccess() {
		t.Fatalf("expected admin-a logId=1002 to succeed, got %+v", ret)
	}
	jobA1002Name := BuildJobName("mig-proxy", 55, 1002)
	var jobA1002 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobA1002Name}, &jobA1002)
	jobA1002.Annotations[AnnotationCallbackSent] = "true"
	_ = fakeK8s.Update(ctx, &jobA1002)
	jobA1002.Status.Succeeded = 1
	jobA1002.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	_ = fakeK8s.Status().Update(ctx, &jobA1002)

	// 2. Migrate mig-proxy and mig-task to admin-b while admin-a's Jobs 1001 (active) and 1002 (completed) still exist!
	var curExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mig-proxy"}, &curExec)
	curExec.Spec.AdminRef.Name = "admin-b"
	_ = fakeK8s.Update(ctx, &curExec)
	curExec.Status.AdminURL = "http://admin-b:8080/xxl-job-admin"
	curExec.Status.JobGroupID = 20
	_ = fakeK8s.Status().Update(ctx, &curExec)

	var curTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mig-task"}, &curTask)
	curTask.Status.AdminURL = "http://admin-b:8080/xxl-job-admin"
	curTask.Status.JobGroupID = 20
	curTask.Status.JobID = 55 // colliding auto-increment jobId on admin-b!
	curTask.Status.LastExecutionJobName = ""
	_ = fakeK8s.Status().Update(ctx, &curTask)

	// (a) /idleBeat on admin-b MUST NOT be blocked by admin-a's active Job 1001
	if idleRet := post("idleBeat", xxlclient.IdleBeatParam{JobID: 55}); !idleRet.IsSuccess() {
		t.Fatalf("expected /idleBeat on admin-b not to be blocked by admin-a's active Job, got %+v", idleRet)
	}

	// (b) Colliding (jobId=55, logId=1001) on admin-b with DIFFERENT logDateTime MUST succeed with origin-scoped Job name
	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "migHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1001,
		LogDateTime:           1700000999999,
	}); !ret.IsSuccess() {
		t.Fatalf("expected colliding (55, 1001) on admin-b to succeed via origin-scoped Job name, got %+v", ret)
	}
	originKeyB := CanonicalAdminOriginKey("admin-b", "http://admin-b:8080/xxl-job-admin", "uid-b")
	jobB1001Name := BuildJobName("mig-proxy", 55, 1001, originKeyB)
	if jobB1001Name == jobA1001Name {
		t.Fatalf("expected distinct Job name for admin-b collision, got identical %s", jobB1001Name)
	}
	var jobB1001 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobB1001Name}, &jobB1001); err != nil {
		t.Fatalf("expected origin-scoped Job %s to be created, got err=%v", jobB1001Name, err)
	}
	if jobB1001.Annotations[AnnotationAdminName] != "admin-b" || jobB1001.Annotations[AnnotationAdminUID] != "uid-b" {
		t.Fatalf("unexpected origin annotations on %s: %+v", jobB1001Name, jobB1001.Annotations)
	}

	// (c) Colliding (jobId=55, logId=1002) on admin-b with IDENTICAL logDateTime to admin-a's completed Job 1002
	// MUST spawn a fresh Job for admin-b (neither swallowed as idempotent nor rejected as already completed)!
	if ret := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1002,
		LogDateTime:     1700000001002,
	}); !ret.IsSuccess() {
		t.Fatalf("expected colliding (55, 1002) with identical logDateTime on admin-b to spawn new Job, got %+v", ret)
	}
	jobB1002Name := BuildJobName("mig-proxy", 55, 1002, originKeyB)
	var jobB1002 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobB1002Name}, &jobB1002); err != nil {
		t.Fatalf("expected admin-b Job %s created despite identical logDateTime on admin-a, got err=%v", jobB1002Name, err)
	}

	// (d) Idempotent retry of (55, 1001) on admin-b BEFORE and AFTER admin-a's Job 1001 is TTL-deleted
	// MUST recognize jobB1001 without spawning a duplicate Job!
	if dupRet := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "migHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1001,
		LogDateTime:           1700000999999,
	}); !dupRet.IsSuccess() || !strings.Contains(dupRet.Msg, "already exists") {
		t.Fatalf("expected idempotent retry before TTL GC to return already exists, got %+v", dupRet)
	}

	// Create Pod for admin-b's Job 1001 and verify /log isolates admin-b from admin-a
	_ = fakeK8s.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-admin-b-1001",
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("mig-proxy"),
				LabelLogID:        "1001",
				"job-name":        jobB1001Name,
			},
		},
		Spec: jobB1001.Spec.Template.Spec,
	})
	logRet := post("log", xxlclient.LogParam{LogID: 1001, FromLineNum: 1})
	var logRes xxlclient.LogResult
	_ = json.Unmarshal(logRet.Content, &logRes)
	if !strings.Contains(logRes.LogContent, "log-from-pod-admin-b-1001") || strings.Contains(logRes.LogContent, "pod-admin-a-1001") {
		t.Fatalf("expected /log on admin-b to read only admin-b's Pod log, got %+v", logRes)
	}

	// /kill on admin-b MUST delete admin-b's active Jobs with DeletePropagationForeground while preserving admin-a's active Job 1001!
	if killRet := post("kill", xxlclient.KillParam{JobID: 55}); !killRet.IsSuccess() {
		t.Fatalf("expected /kill on admin-b to succeed: %+v", killRet)
	}
	delMu.Lock()
	polB1001 := deletedPolicies[jobB1001Name]
	_, killedAdminA := deletedPolicies[jobA1001Name]
	delMu.Unlock()
	if polB1001 != metav1.DeletePropagationForeground {
		t.Fatalf("expected /kill to delete %s with DeletePropagationForeground, got %q", jobB1001Name, polB1001)
	}
	if killedAdminA {
		t.Fatalf("expected /kill on admin-b NOT to delete admin-a's active Job %s", jobA1001Name)
	}

	// Simulate TTL GC removing admin-a's Job 1002, then retry (55, 1002) on admin-b -> must still find jobB1002!
	jobA1002.Finalizers = nil
	_ = fakeK8s.Update(ctx, &jobA1002)
	_ = fakeK8s.Delete(ctx, &jobA1002)

	// Since /kill above deleted jobB1002 (whose finalizer was only callback-guard, or in fakeK8s retained by callback-guard),
	// verify retrying (55, 1002) after admin-a's Job 1002 TTL GC recognizes jobB1002 ("is currently being deleted")
	// and never spawns a duplicate base Job!
	base1002Name := BuildJobName("mig-proxy", 55, 1002)
	retryAfterGC := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1002,
		LogDateTime:     1700000001002,
	})
	if retryAfterGC.IsSuccess() || !strings.Contains(retryAfterGC.Msg, "currently being deleted") {
		t.Fatalf("expected retry of deleting jobB1002 after base Job GC to report deleting, got %+v", retryAfterGC)
	}
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: base1002Name}, &batchv1.Job{}); err == nil {
		t.Fatalf("expected no duplicate base Job %s spawned after TTL GC", base1002Name)
	}

	// Recreate admin-b with a new UID ("uid-b-v2") and trigger (55, 1003) -> then recreate with ("uid-b-v3") and trigger (55, 1003)
	var curAdminB xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "admin-b"}, &curAdminB)
	_ = fakeK8s.Delete(ctx, &curAdminB)
	adminBv2 := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "admin-b",
			Namespace: "default",
			UID:       types.UID("uid-b-v2"),
		},
		Spec:   adminB.Spec,
		Status: adminB.Status,
	}
	_ = fakeK8s.Create(ctx, adminBv2)
	if ret := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1003,
		LogDateTime:     1700000001003,
	}); !ret.IsSuccess() {
		t.Fatalf("expected admin-b (uid-b-v2) logId=1003 to succeed: %+v", ret)
	}
	// Simulate TTL GC NOT yet removing uid-b-v2's Job 1003 while admin-b is recreated with fresh DB ("uid-b-v3")
	_ = fakeK8s.Delete(ctx, adminBv2)
	adminBv3 := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "admin-b",
			Namespace: "default",
			UID:       types.UID("uid-b-v3"),
		},
		Spec:   adminB.Spec,
		Status: adminB.Status,
	}
	_ = fakeK8s.Create(ctx, adminBv3)
	if ret := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1003,
		LogDateTime:     1700000009999,
	}); !ret.IsSuccess() {
		t.Fatalf("expected recreated same-name admin-b (uid-b-v3) colliding logId=1003 to spawn distinct Job, got %+v", ret)
	}
	originKeyBv3 := CanonicalAdminOriginKey("admin-b", "http://admin-b:8080/xxl-job-admin", "uid-b-v3")
	jobBv3Name := BuildJobName("mig-proxy", 55, 1003, originKeyBv3)
	var jobBv3 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobBv3Name}, &jobBv3); err != nil {
		t.Fatalf("expected uid-b-v3 disambiguated Job %s to exist, got err=%v", jobBv3Name, err)
	}
	// Now delete uid-b-v2's base Job 1003 (simulating TTL GC) and send an idempotent retry for uid-b-v3's (55, 1003):
	// MUST find jobBv3Name instead of spawning a duplicate base Job!
	base1003Name := BuildJobName("mig-proxy", 55, 1003)
	var base1003 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: base1003Name}, &base1003)
	base1003.Finalizers = nil
	_ = fakeK8s.Update(ctx, &base1003)
	_ = fakeK8s.Delete(ctx, &base1003)

	if retryRet := post("run", xxlclient.TriggerParam{
		JobID:           55,
		ExecutorHandler: "migHandler",
		LogID:           1003,
		LogDateTime:     1700000009999,
	}); !retryRet.IsSuccess() || !strings.Contains(retryRet.Msg, "already exists") {
		t.Fatalf("expected idempotent retry after base Job TTL GC to locate %s, got %+v", jobBv3Name, retryRet)
	}
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: base1003Name}, &batchv1.Job{}); err == nil {
		t.Fatalf("expected no duplicate base Job %s spawned on retry after TTL GC", base1003Name)
	}
}

func TestCrossAdminSerialQueueHybridOriginAndRecreatedCoverEarly(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	ctx := context.Background()

	adminA := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-a", Namespace: "default", UID: types.UID("uid-a")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://admin-a:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "serial-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: "http://admin-a:8080/xxl-job-admin"},
	}
	adminB := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-b", Namespace: "default", UID: types.UID("uid-b")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: "http://admin-b:8080/xxl-job-admin",
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "serial-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: "http://admin-b:8080/xxl-job-admin"},
	}
	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "ser-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "admin-a"},
			AppName:  "ser-proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 1,
			AdminURL:   "http://admin-a:8080/xxl-job-admin",
			Synced:     true,
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "ser-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "ser-proxy"},
			JobDesc:         "Serial Task",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "serHandler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo ok"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      55,
			JobGroupID: 1,
			AdminURL:   "http://admin-a:8080/xxl-job-admin",
			Synced:     true,
			Conditions: []metav1.Condition{{
				Type:               ConditionAdminEndpointSynced,
				Status:             metav1.ConditionTrue,
				Reason:             "EndpointSynced",
				Message:            EncodeSyncedAdminEndpointMeta(adminA, "http://admin-a:8080/xxl-job-admin"),
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminA, adminB, execCR, taskCR).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "serial-tok")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/ser-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "serial-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Pre-reconcile Spec.AdminRef switch (admin-a -> admin-b while Status.AdminURL is still admin-a):
	// MUST stamp consistent admin-a origin (never Frankenstein hybrid {name: admin-b, uid: uid-b, url: admin-a}).
	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ser-proxy"}, &liveExec)
	liveExec.Spec.AdminRef.Name = "admin-b"
	_ = fakeK8s.Update(ctx, &liveExec)

	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "serHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 900,
		LogDateTime:           1700000000900,
	}); !ret.IsSuccess() {
		t.Fatalf("expected pre-reconcile admin-a SERIAL trigger 900 to succeed, got %+v", ret)
	}
	jobA900Name := BuildJobName("ser-proxy", 55, 900)
	var jobA900 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobA900Name}, &jobA900); err != nil {
		t.Fatalf("get jobA900: %v", err)
	}
	if jobA900.Annotations[AnnotationAdminName] != "admin-a" ||
		jobA900.Annotations[AnnotationAdminUID] != "uid-a" ||
		jobA900.Annotations[AnnotationAdminURL] != "http://admin-a:8080/xxl-job-admin" {
		t.Fatalf("expected consistent admin-a origin during pre-reconcile window, got hybrid %+v", jobA900.Annotations)
	}

	// Enqueue second SERIAL_EXECUTION Job 901 on admin-a, advance queue so 900 runs (Active=1) and 901 stays queued
	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "serHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 901,
		LogDateTime:           1700000000901,
	}); !ret.IsSuccess() {
		t.Fatalf("expected admin-a SERIAL trigger 901 to succeed: %+v", ret)
	}
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("ser-proxy"), "55"); err != nil {
		t.Fatalf("advance admin-a queue: %v", err)
	}
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobA900Name}, &jobA900)
	if jobA900.Spec.Suspend != nil && *jobA900.Spec.Suspend {
		t.Fatalf("expected admin-a head 900 to be unsuspended")
	}
	jobA900.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &jobA900)

	// 2. Complete migration to admin-b (JobID=55 collides!) while admin-a's 900 is Active=1 and 901 is queued!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ser-proxy"}, &liveExec)
	liveExec.Status.AdminURL = "http://admin-b:8080/xxl-job-admin"
	liveExec.Status.JobGroupID = 2
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ser-task"}, &liveTask)
	liveTask.Status.AdminURL = "http://admin-b:8080/xxl-job-admin"
	liveTask.Status.JobGroupID = 2
	liveTask.Status.JobID = 55
	liveTask.Status.LastExecutionJobName = ""
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:               ConditionAdminEndpointSynced,
		Status:             metav1.ConditionTrue,
		Reason:             "EndpointSynced",
		Message:            EncodeSyncedAdminEndpointMeta(adminB, "http://admin-b:8080/xxl-job-admin"),
		LastTransitionTime: metav1.Now(),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	// Enqueue SERIAL_EXECUTION logId=1 and logId=2 on admin-b
	for _, lid := range []int64{1, 2} {
		if ret := post("run", xxlclient.TriggerParam{
			JobID:                 55,
			ExecutorHandler:       "serHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 lid,
			LogDateTime:           1700000001000 + lid,
		}); !ret.IsSuccess() {
			t.Fatalf("expected admin-b SERIAL trigger %d to succeed: %+v", lid, ret)
		}
	}

	var depthMu sync.Mutex
	var recordedDepth float64 = -1
	prevHook := OnSerialQueueDepth
	OnSerialQueueDepth = func(_, _, _ string, depth float64) {
		depthMu.Lock()
		recordedDepth = depth
		depthMu.Unlock()
	}
	defer func() { OnSerialQueueDepth = prevHook }()

	// AdvanceSerialQueueWithReader MUST unsuspend admin-b's logId=1 despite admin-a's active 900 & queued 901!
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("ser-proxy"), "55"); err != nil {
		t.Fatalf("advance cross-admin serial queue: %v", err)
	}
	var jobB1, jobB2, jobA901 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("ser-proxy", 55, 1)}, &jobB1)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("ser-proxy", 55, 2)}, &jobB2)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("ser-proxy", 55, 901)}, &jobA901)
	if jobB1.Spec.Suspend != nil && *jobB1.Spec.Suspend {
		t.Fatalf("expected admin-b SERIAL head logId=1 to unsuspend immediately without blocking behind admin-a's active Job 900")
	}
	if jobB2.Spec.Suspend == nil || !*jobB2.Spec.Suspend {
		t.Fatalf("expected admin-b second SERIAL Job logId=2 to remain suspended behind logId=1")
	}
	if jobA901.Spec.Suspend == nil || !*jobA901.Spec.Suspend {
		t.Fatalf("expected admin-a queued SERIAL Job 901 to remain suspended behind admin-a's active Job 900")
	}
	depthMu.Lock()
	gotDepth := recordedDepth
	depthMu.Unlock()
	if gotDepth != 2 {
		t.Fatalf("expected combined remaining serial queue depth = 2 (admin-a 901 + admin-b 2), got %v", gotDepth)
	}

	// 3. Rolling-upgrade safety: origin-less legacy active Job conservatively blocks post-upgrade queue on jobId=77
	suspendFalse := false
	suspendTrue := true
	legacyActive := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildJobName("ser-proxy", 77, 50),
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("ser-proxy"),
				LabelJobID:        "77",
				LabelLogID:        "50",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "ser-proxy",
				AnnotationLogID:         "50",
				AnnotationBlockStrategy: "SERIAL_EXECUTION",
			},
		},
		Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
		Status: batchv1.JobStatus{Active: 1},
	}
	postUpgradeQueued := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildJobName("ser-proxy", 77, 51),
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("ser-proxy"),
				LabelJobID:        "77",
				LabelLogID:        "51",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "ser-proxy",
				AnnotationLogID:         "51",
				AnnotationBlockStrategy: "SERIAL_EXECUTION",
				AnnotationAdminName:     "admin-b",
				AnnotationAdminURL:      "http://admin-b:8080/xxl-job-admin",
				AnnotationAdminUID:      "uid-b",
			},
		},
		Spec: batchv1.JobSpec{Suspend: &suspendTrue},
	}
	_ = fakeK8s.Create(ctx, legacyActive)
	_ = fakeK8s.Create(ctx, postUpgradeQueued)
	_ = AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("ser-proxy"), "77")
	var postCheck batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: postUpgradeQueued.Name}, &postCheck)
	if postCheck.Spec.Suspend == nil || !*postCheck.Spec.Suspend {
		t.Fatalf("expected legacy origin-less active Job to conservatively block post-upgrade SERIAL Job")
	}

	// 4. Recreated same-name/same-URL Admin (uid-b -> uid-b-recreated) with TTL-GC'd LastExecutionJobName=500000:
	// COVER_EARLY trigger with fresh DB logId=3 MUST NOT be falsely discarded as superseded by 500000!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ser-task"}, &liveTask)
	liveTask.Status.LastExecutionJobName = BuildJobName("ser-proxy", 55, 500000) // already GC'd by TTL
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	var curB xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "admin-b"}, &curB)
	_ = fakeK8s.Delete(ctx, &curB)
	adminBRecreated := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-b", Namespace: "default", UID: types.UID("uid-b-recreated")},
		Spec:       adminB.Spec,
		Status:     adminB.Status,
	}
	_ = fakeK8s.Create(ctx, adminBRecreated)

	if coverRet := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "serHandler",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 3,
		LogDateTime:           1700000002003,
	}); !coverRet.IsSuccess() {
		t.Fatalf("expected COVER_EARLY logId=3 from recreated same-URL Admin (uid-b-recreated) not to be discarded by uid-b's GC'd 500000, got %+v", coverRet)
	}
}

func TestSyncedEndpointJSONStrictParsingForegroundDeleteAndOriginSpecificity(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// 1. extractSyncedEndpointOrigin strictly parses JSON and never matches arbitrary targetURL on malformed/empty payloads.
	for _, badMsg := range []string{
		"",
		"   ",
		"not-json-at-all",
		"name=admin-a\nuid=uid-a\nurl=http://admin-a:8080/xxl-job-admin",
		"{}",
		`{"username":"ops"}`,
	} {
		conds := []metav1.Condition{{
			Type:    ConditionAdminEndpointSynced,
			Status:  metav1.ConditionTrue,
			Message: badMsg,
		}}
		if got := extractSyncedEndpointOrigin(conds, "http://any-target:8080/xxl-job-admin"); got.hasAny() {
			t.Fatalf("expected malformed/empty condition %q to yield empty adminOrigin, got %+v", badMsg, got)
		}
	}

	adminManaged := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "mgr-admin", Namespace: "default", UID: types.UID("uid-mgr-1")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Port:        8080,
			ContextPath: "/xxl-job-admin",
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: "jdbc:mysql://mysql:3306/xxl_job"},
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "mgr-tok"},
		},
	}
	encodedJSON := EncodeSyncedAdminEndpointMeta(adminManaged, "http://mgr-admin.default.svc.cluster.local:8080/xxl-job-admin")
	validConds := []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: encodedJSON,
	}}
	if mismatch := extractSyncedEndpointOrigin(validConds, "http://other-admin:8080/xxl-job-admin"); mismatch.hasAny() {
		t.Fatalf("expected mismatched targetURL to yield empty adminOrigin, got %+v", mismatch)
	}
	matched := extractSyncedEndpointOrigin(validConds, "http://mgr-admin.default.svc.cluster.local:8080/xxl-job-admin")
	if matched.name != "mgr-admin" || matched.uid != "uid-mgr-1" || matched.dbHash == "" {
		t.Fatalf("expected full JSON origin fields decoded, got %+v", matched)
	}

	// 2. HasActiveOrTerminatingPods & AdvanceSerialQueueWithReader honor metav1.FinalizerDeleteDependents ("foregroundDeletion")
	// when a deleting Job's Status.Active is already 0 and Status.Terminating is nil while GC has not finished deleting Pods.
	now := metav1.Now()
	suspendFalse := false
	suspendTrue := true
	deletingWithFG := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              BuildJobName("fg-exec", 88, 10),
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{JobCallbackFinalizer, metav1.FinalizerDeleteDependents},
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("fg-exec"),
				LabelJobID:        "88",
				LabelLogID:        "10",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "fg-exec",
				AnnotationLogID:         "10",
				AnnotationBlockStrategy: "SERIAL_EXECUTION",
				AnnotationAdminName:     "mgr-admin",
				AnnotationAdminUID:      "uid-mgr-1",
				AnnotationAdminURL:      "http://mgr-admin.default.svc.cluster.local:8080/xxl-job-admin",
			},
		},
		Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
		Status: batchv1.JobStatus{Active: 0, Terminating: nil},
	}
	if !HasActiveOrTerminatingPods(deletingWithFG) {
		t.Fatalf("expected HasActiveOrTerminatingPods=true while deleting Job still carries foregroundDeletion finalizer")
	}
	queuedBehindFG := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildJobName("fg-exec", 88, 11),
			Namespace: "default",
			Finalizers: []string{
				JobCallbackFinalizer,
			},
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("fg-exec"),
				LabelJobID:        "88",
				LabelLogID:        "11",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "fg-exec",
				AnnotationLogID:         "11",
				AnnotationBlockStrategy: "SERIAL_EXECUTION",
				AnnotationAdminName:     "mgr-admin",
				AnnotationAdminUID:      "uid-mgr-1",
				AnnotationAdminURL:      "http://mgr-admin.default.svc.cluster.local:8080/xxl-job-admin",
			},
		},
		Spec: batchv1.JobSpec{Suspend: &suspendTrue},
	}
	fgClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(deletingWithFG, queuedBehindFG).
		WithStatusSubresource(&batchv1.Job{}).
		Build()

	if err := AdvanceSerialQueueWithReader(ctx, fgClient, fgClient, "default", SanitizeLabel("fg-exec"), "88"); err != nil {
		t.Fatalf("advance queue while foregroundDeletion present: %v", err)
	}
	var checkQueued batchv1.Job
	_ = fgClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: queuedBehindFG.Name}, &checkQueued)
	if checkQueued.Spec.Suspend == nil || !*checkQueued.Spec.Suspend {
		t.Fatalf("expected queued SERIAL_EXECUTION Job to stay suspended while predecessor holds foregroundDeletion finalizer")
	}

	// Simulate Kubernetes GarbageCollector removing FinalizerDeleteDependents once dependent Pods vanish
	var liveDel batchv1.Job
	_ = fgClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: deletingWithFG.Name}, &liveDel)
	liveDel.Finalizers = []string{JobCallbackFinalizer}
	_ = fgClient.Update(ctx, &liveDel)
	if HasActiveOrTerminatingPods(&liveDel) {
		t.Fatalf("expected HasActiveOrTerminatingPods=false once foregroundDeletion finalizer is removed and Active=0")
	}
	if err := AdvanceSerialQueueWithReader(ctx, fgClient, fgClient, "default", SanitizeLabel("fg-exec"), "88"); err != nil {
		t.Fatalf("advance queue after foregroundDeletion removed: %v", err)
	}
	_ = fgClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: queuedBehindFG.Name}, &checkQueued)
	if checkQueued.Spec.Suspend != nil && *checkQueued.Spec.Suspend {
		t.Fatalf("expected queued SERIAL_EXECUTION Job to unsuspend once foregroundDeletion finalizer was removed")
	}

	// 3. AdvanceSerialQueueWithReader deterministic partitioning across all 3! = 6 slice permutations when uid is empty.
	// Active Job A has {name: "adm", url: "http://a"}, Queued Job B has {name: "adm", url: "http://b"},
	// and Partial Queued Job P has {name: "adm"} (empty uid, empty url).
	// Regardless of slice order:
	//  - Job A ({name: "adm", url: "http://a"}, active) and Job B ({name: "adm", url: "http://b"}, suspended) MUST NEVER coalesce;
	//  - Job B MUST always unsuspend, while Job P deterministically attaches to Partition[0] ({name: "adm", url: "http://a"}) and stays suspended!
	perms := [][3]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
	for pIdx, perm := range perms {
		mkJobs := []*batchv1.Job{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("job-0-active-a-%d", pIdx),
					Namespace: "default",
					Labels: map[string]string{
						LabelManagedBy:    ManagedByValue,
						LabelExecutorName: SanitizeLabel("perm-exec"),
						LabelJobID:        "91",
						LabelLogID:        "1",
					},
					Annotations: map[string]string{
						AnnotationExecutorName:  "perm-exec",
						AnnotationLogID:         "1",
						AnnotationBlockStrategy: "SERIAL_EXECUTION",
						AnnotationAdminName:     "adm",
						AnnotationAdminURL:      "http://a",
					},
				},
				Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
				Status: batchv1.JobStatus{Active: 1},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("job-1-queued-b-%d", pIdx),
					Namespace: "default",
					Labels: map[string]string{
						LabelManagedBy:    ManagedByValue,
						LabelExecutorName: SanitizeLabel("perm-exec"),
						LabelJobID:        "91",
						LabelLogID:        "2",
					},
					Annotations: map[string]string{
						AnnotationExecutorName:  "perm-exec",
						AnnotationLogID:         "2",
						AnnotationBlockStrategy: "SERIAL_EXECUTION",
						AnnotationAdminName:     "adm",
						AnnotationAdminURL:      "http://b",
					},
				},
				Spec: batchv1.JobSpec{Suspend: &suspendTrue},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("job-2-partial-%d", pIdx),
					Namespace: "default",
					Labels: map[string]string{
						LabelManagedBy:    ManagedByValue,
						LabelExecutorName: SanitizeLabel("perm-exec"),
						LabelJobID:        "91",
						LabelLogID:        "3",
					},
					Annotations: map[string]string{
						AnnotationExecutorName:  "perm-exec",
						AnnotationLogID:         "3",
						AnnotationBlockStrategy: "SERIAL_EXECUTION",
						AnnotationAdminName:     "adm",
					},
				},
				Spec: batchv1.JobSpec{Suspend: &suspendTrue},
			},
		}
		permCli := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&batchv1.Job{}).Build()
		for _, idx := range perm {
			_ = permCli.Create(ctx, mkJobs[idx].DeepCopy())
		}
		if err := AdvanceSerialQueueWithReader(ctx, permCli, permCli, "default", SanitizeLabel("perm-exec"), "91"); err != nil {
			t.Fatalf("perm %v advance failed: %v", perm, err)
		}
		var gotB, gotP batchv1.Job
		_ = permCli.Get(ctx, client.ObjectKey{Namespace: "default", Name: mkJobs[1].Name}, &gotB)
		_ = permCli.Get(ctx, client.ObjectKey{Namespace: "default", Name: mkJobs[2].Name}, &gotP)
		if gotB.Spec.Suspend != nil && *gotB.Spec.Suspend {
			t.Fatalf("perm %v: expected {name:adm, url:http://b} Job to unsuspend independently of {name:adm, url:http://a}", perm)
		}
		if gotP.Spec.Suspend == nil || !*gotP.Spec.Suspend {
			t.Fatalf("perm %v: expected partial {name:adm} Job to deterministically join active http://a partition and stay suspended", perm)
		}
	}

	// 4. In-place managed XxlJobAdmin Port/ContextPath change preserves sameAdminOrigin (SERIAL_EXECUTION / DISCARD_LATER)
	// when Datasource.URL and UID are unchanged, while isolating when Datasource.URL changes.
	dbHash1 := DatasourceURLHash("jdbc:mysql://mysql:3306/xxl_job")
	dbHash2 := DatasourceURLHash("jdbc:mysql://mysql2:3306/xxl_job_v2")
	origOldPort := adminOrigin{
		name:   "mgr-admin",
		uid:    "uid-mgr-1",
		url:    "http://mgr-admin.default.svc.cluster.local:8080/xxl-job-admin",
		dbHash: dbHash1,
	}
	origNewPortAndPath := adminOrigin{
		name:   "mgr-admin",
		uid:    "uid-mgr-1",
		url:    "http://mgr-admin.default.svc.cluster.local:8081/scheduler",
		dbHash: dbHash1,
	}
	origDifferentDB := adminOrigin{
		name:   "mgr-admin",
		uid:    "uid-mgr-1",
		url:    "http://mgr-admin.default.svc.cluster.local:8081/scheduler",
		dbHash: dbHash2,
	}
	if !sameAdminOrigin(origOldPort, origNewPortAndPath) {
		t.Fatalf("expected same managed cluster-local Admin across Port/ContextPath edit with identical dbHash to match sameAdminOrigin")
	}
	if sameAdminOrigin(origOldPort, origDifferentDB) {
		t.Fatalf("expected managed cluster-local Admin with changed Datasource.URL (dbHash) NOT to match sameAdminOrigin")
	}
}

func TestManagedOriginSameURLDBSwapHybridAndGCdCoverEarlyPrefix(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	svcURL8080 := "http://mgr-adv.default.svc.cluster.local:8080/xxl-job-admin"
	svcURL8081 := "http://mgr-adv.default.svc.cluster.local:8081/scheduler"
	hybridURL := "https://xxl-ingress.corp.local/xxl-job-admin"
	db1 := "jdbc:mysql://mysql-db1:3306/xxl_job"
	db2 := "jdbc:mysql://mysql-db2:3306/xxl_job_v2"
	dbHash1 := DatasourceURLHash(db1)
	dbHash2 := DatasourceURLHash(db2)

	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "mgr-adv", Namespace: "default", UID: types.UID("uid-mgr-adv")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Image:       "xuxueli/xxl-job-admin:2.4.1",
			Port:        8080,
			ContextPath: "/xxl-job-admin",
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: db1},
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "adv-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: svcURL8080},
	}
	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "mgr-adv"},
			AppName:  "adv-proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 1,
			AdminURL:   svcURL8080,
			Synced:     true,
			Conditions: []metav1.Condition{{
				Type:    ConditionAdminEndpointSynced,
				Status:  metav1.ConditionTrue,
				Message: EncodeSyncedAdminEndpointMeta(adminCR, svcURL8080),
			}},
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: "adv-proxy"},
			JobDesc:         "Adv Task",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "advHandler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo ok"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      55,
			JobGroupID: 1,
			AdminURL:   svcURL8080,
			Synced:     true,
			Conditions: []metav1.Condition{{
				Type:    ConditionAdminEndpointSynced,
				Status:  metav1.ConditionTrue,
				Message: EncodeSyncedAdminEndpointMeta(adminCR, svcURL8080),
			}},
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminCR, execCR, taskCR).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "adv-tok\r\n")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/adv-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "adv-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Trigger origin-scoped COVER_EARLY winner logId=500 on :8080 (when base name is occupied by a foreign Admin's TTL Job),
	// then GC winner 500, edit managed Port/ContextPath to :8081/scheduler (same DB), and verify delayed logId=499 is rejected!
	suspendFalse := false
	foreignBaseJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildJobName("adv-proxy", 55, 500),
			Namespace: "default",
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("adv-proxy"),
				LabelJobID:        "55",
				LabelLogID:        "500",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "adv-proxy",
				AnnotationAdminName:     "other-admin",
				AnnotationAdminUID:      "uid-other",
				AnnotationAdminURL:      "http://other-admin:8080/xxl-job-admin",
				AnnotationLogID:         "500",
				AnnotationBlockStrategy: "COVER_EARLY",
				AnnotationCallbackSent:  "true",
			},
		},
		Spec: batchv1.JobSpec{Suspend: &suspendFalse},
		Status: batchv1.JobStatus{
			Succeeded:  1,
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		},
	}
	_ = fakeK8s.Create(ctx, foreignBaseJob)

	if ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "advHandler",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 500,
		LogDateTime:           1700000000500,
	}); !ret.IsSuccess() {
		t.Fatalf("expected COVER_EARLY logId=500 to succeed under origin-scoped name: %+v", ret)
	}
	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-task"}, &liveTask)
	scopedWinner500Name := liveTask.Status.LastExecutionJobName
	if scopedWinner500Name == "" || scopedWinner500Name == foreignBaseJob.Name {
		t.Fatalf("expected origin-scoped LastExecutionJobName distinct from %s, got %q", foreignBaseJob.Name, scopedWinner500Name)
	}
	// Simulate TTL garbage collection of scopedWinner500Name
	var winner500 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: scopedWinner500Name}, &winner500)
	winner500.Finalizers = nil
	_ = fakeK8s.Update(ctx, &winner500)
	_ = fakeK8s.Delete(ctx, &winner500)

	// Edit managed Admin Port/ContextPath to :8081/scheduler (same DB) and sync Executor + Task
	var liveAdmin xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mgr-adv"}, &liveAdmin)
	liveAdmin.Spec.Port = 8081
	liveAdmin.Spec.ContextPath = "/scheduler"
	_ = fakeK8s.Update(ctx, &liveAdmin)
	liveAdmin.Status.AdminURL = svcURL8081
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-proxy"}, &liveExec)
	liveExec.Status.AdminURL = svcURL8081
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: EncodeSyncedAdminEndpointMeta(&liveAdmin, svcURL8081),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-task"}, &liveTask)
	liveTask.Status.AdminURL = svcURL8081
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: EncodeSyncedAdminEndpointMeta(&liveAdmin, svcURL8081),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if staleRet := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "advHandler",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 499,
		LogDateTime:           1700000000499,
	}); staleRet.IsSuccess() {
		t.Fatalf("expected delayed COVER_EARLY logId=499 after Port/ContextPath edit to be superseded by GC'd origin-scoped winner 500")
	}

	// 2. Hybrid mode (ExternalURL != "" + Image != "" + same Datasource.URL) preserves continuity with :8081/scheduler!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mgr-adv"}, &liveAdmin)
	liveAdmin.Spec.ExternalURL = hybridURL
	_ = fakeK8s.Update(ctx, &liveAdmin)
	hybridMetaRaw := EncodeSyncedAdminEndpointMeta(&liveAdmin, hybridURL)
	hybridDecoded := DecodeSyncedAdminEndpointMeta([]metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: hybridMetaRaw}})
	if hybridDecoded == nil || !hybridDecoded.ManagedInCluster || hybridDecoded.DatasourceHash != dbHash1 {
		t.Fatalf("expected Hybrid managed Admin metadata to set ManagedInCluster=true and DatasourceHash=%s, got %+v", dbHash1, hybridDecoded)
	}
	// Sync Executor to hybridURL while Task is still on svcURL8081: triggers MUST remain consistent!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-proxy"}, &liveExec)
	liveExec.Status.AdminURL = hybridURL
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: hybridMetaRaw,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	if serRet := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "advHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 601,
		LogDateTime:           1700000000601,
	}); !serRet.IsSuccess() {
		t.Fatalf("expected SERIAL_EXECUTION trigger during Hybrid URL transition (same DB) to succeed: %+v", serRet)
	}
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("adv-proxy"), "55"); err != nil {
		t.Fatalf("advance hybrid db1 serial queue: %v", err)
	}
	var jobDB1 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("adv-proxy", 55, 601)}, &jobDB1)
	jobDB1.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &jobDB1)

	// 3. Same-URL Datasource.URL swap (db1 -> db2 on hybridURL):
	//    - While Executor synced db2 and Task still holds db1, findMatchingTask / isTaskConsistentWithExecutor rejects stale db1 Task!
	//    - Once Task syncs db2, resolveExecutorOrigin stamps dbHash2 and SERIAL_EXECUTION runs independently of db1's active Job!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mgr-adv"}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = db2
	_ = fakeK8s.Update(ctx, &liveAdmin)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-proxy"}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: EncodeSyncedAdminEndpointMeta(&liveAdmin, hybridURL),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	if mismatchRet := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "advHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 1,
		LogDateTime:           1700000001001,
	}); mismatchRet.IsSuccess() {
		t.Fatalf("expected trigger for undeclared/unreconciled task across db1->db2 swap to be rejected before Task syncs db2")
	}

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "adv-task"}, &liveTask)
	liveTask.Status.AdminURL = hybridURL
	liveTask.Status.LastExecutionJobName = ""
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: EncodeSyncedAdminEndpointMeta(&liveAdmin, hybridURL),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if db2Ret := post("run", xxlclient.TriggerParam{
		JobID:                 55,
		ExecutorHandler:       "advHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 1,
		LogDateTime:           1700000001002,
	}); !db2Ret.IsSuccess() {
		t.Fatalf("expected SERIAL_EXECUTION trigger on db2 after Task syncs db2 to succeed: %+v", db2Ret)
	}
	var jobDB2 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("adv-proxy", 55, 1)}, &jobDB2)
	if jobDB2.Annotations[AnnotationAdminDatasourceHash] != dbHash2 {
		t.Fatalf("expected db2 Job stamped with dbHash2=%s, got %q", dbHash2, jobDB2.Annotations[AnnotationAdminDatasourceHash])
	}
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("adv-proxy"), "55"); err != nil {
		t.Fatalf("advance db2 serial queue: %v", err)
	}
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: jobDB2.Name}, &jobDB2)
	if jobDB2.Spec.Suspend != nil && *jobDB2.Spec.Suspend {
		t.Fatalf("expected db2 SERIAL_EXECUTION Job to unsuspend independently of db1's active Job on the same URL")
	}
}

func TestRejectTriggerBeforeDatasourceStatusConverges(t *testing.T) {
	ctx := context.Background()
	// Intentionally register ONLY batchv1 + xxljobv1alpha1 (without corev1) to also verify minimal-scheme safety.
	minScheme := runtime.NewScheme()
	_ = batchv1.AddToScheme(minScheme)
	_ = xxljobv1alpha1.AddToScheme(minScheme)

	dbOld := "jdbc:mysql://mysql-old.default.svc:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8"
	dbNew := "jdbc:mysql://mysql-new.default.svc:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8"
	hashOld := DatasourceURLHash(dbOld)
	hashNew := DatasourceURLHash(dbNew)

	svcURL8080 := "http://ds-admin.default.svc.cluster.local:8080/xxl-job-admin"
	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "ds-admin", Namespace: "default", UID: types.UID("uid-ds-admin")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Image:       "xuxueli/xxl-job-admin:2.4.1",
			Port:        8080,
			ContextPath: "/xxl-job-admin",
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: dbOld},
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "ds-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{
			AdminURL: svcURL8080,
			Conditions: []metav1.Condition{{
				Type:    ConditionAdminEndpointSynced,
				Status:  metav1.ConditionTrue,
				Message: "",
			}},
		},
	}
	syncedMetaOld := EncodeSyncedAdminEndpointMeta(adminCR, svcURL8080)
	adminCR.Status.Conditions[0].Message = syncedMetaOld

	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "ds-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: "ds-admin"},
			AppName:  "ds-proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 2,
			AdminURL:   svcURL8080,
			Synced:     true,
			Conditions: []metav1.Condition{{
				Type:    ConditionAdminEndpointSynced,
				Status:  metav1.ConditionTrue,
				Message: syncedMetaOld,
			}},
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "ds-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:           xxljobv1alpha1.LocalObjectReference{Name: "ds-proxy"},
			ExecutorHandler:       "dsHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "busybox:1.36",
				Command: []string{"sh", "-c", "echo ok"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      42,
			JobGroupID: 2,
			AdminURL:   svcURL8080,
			Synced:     true,
			Conditions: []metav1.Condition{{
				Type:    ConditionAdminEndpointSynced,
				Status:  metav1.ConditionTrue,
				Message: syncedMetaOld,
			}},
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(minScheme).
		WithObjects(adminCR, execCR, taskCR).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "ds-tok")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(path string, body any) xxlclient.ReturnT {
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(string(raw)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("XXL-JOB-ACCESS-TOKEN", "ds-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// Baseline: trigger on dbOld succeeds and stamps hashOld.
	if baseRet := post("/proxy/default/ds-proxy/run", xxlclient.TriggerParam{
		JobID:                 42,
		ExecutorHandler:       "dsHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 100,
		LogDateTime:           1700000000100,
	}); !baseRet.IsSuccess() {
		t.Fatalf("expected baseline trigger on dbOld to succeed: %+v", baseRet)
	}
	var oldJob batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("ds-proxy", 42, 100)}, &oldJob); err != nil {
		t.Fatalf("get oldJob: %v", err)
	}
	if oldJob.Annotations[AnnotationAdminDatasourceHash] != hashOld {
		t.Fatalf("expected oldJob stamped with hashOld=%s, got %q", hashOld, oldJob.Annotations[AnnotationAdminDatasourceHash])
	}
	oldJob.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &oldJob)

	// Switch Admin Datasource.URL to dbNew BEFORE Executor or Task status converges (both still hold hashOld).
	var liveAdmin xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ds-admin"}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = dbNew
	_ = fakeK8s.Update(ctx, &liveAdmin)

	// Both routed /proxy/default/ds-proxy/run and legacy /run MUST reject stale JobID=42 and NEVER stamp hashNew.
	for _, p := range []string{"/proxy/default/ds-proxy/run", "/run"} {
		ret := post(p, xxlclient.TriggerParam{
			JobID:                 42,
			ExecutorHandler:       "dsHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 200,
			LogDateTime:           1700000000200,
		})
		if ret.IsSuccess() {
			t.Fatalf("path %s: expected trigger to be rejected before Executor/Task converge to dbNew, got %+v", p, ret)
		}
	}
	var jobList batchv1.JobList
	_ = fakeK8s.List(ctx, &jobList, client.InNamespace("default"))
	if len(jobList.Items) != 1 {
		t.Fatalf("expected no new Job created during pre-convergence db switch window, got %d Jobs", len(jobList.Items))
	}

	// Meanwhile, /kill for in-flight oldJob on dbOld still matches hashOld (from unconverged execCR condition) and deletes oldJob cleanly.
	if killRet := post("/proxy/default/ds-proxy/kill", xxlclient.KillParam{JobID: 42}); !killRet.IsSuccess() {
		t.Fatalf("expected /kill on in-flight dbOld Job before status convergence to succeed: %+v", killRet)
	}
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: oldJob.Name}, &oldJob)
	if oldJob.DeletionTimestamp.IsZero() {
		t.Fatalf("expected in-flight dbOld Job to receive DeletionTimestamp on /kill")
	}

	// Simultaneous Port (8080->8081) + Datasource.URL (dbNew) edit before convergence MUST also reject.
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ds-admin"}, &liveAdmin)
	liveAdmin.Spec.Port = 8081
	_ = fakeK8s.Update(ctx, &liveAdmin)
	if portSwitchRet := post("/proxy/default/ds-proxy/run", xxlclient.TriggerParam{
		JobID:                 42,
		ExecutorHandler:       "dsHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 201,
		LogDateTime:           1700000000201,
	}); portSwitchRet.IsSuccess() {
		t.Fatalf("expected trigger during simultaneous Port+Datasource switch before convergence to be rejected, got %+v", portSwitchRet)
	}

	// Once Executor AND Task converge to dbNew on :8081, triggers succeed and stamp hashNew!
	svcURL8081 := "http://ds-admin.default.svc.cluster.local:8081/xxl-job-admin"
	syncedMetaNew := EncodeSyncedAdminEndpointMeta(&liveAdmin, svcURL8081)

	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ds-proxy"}, &liveExec)
	liveExec.Status.AdminURL = svcURL8081
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: syncedMetaNew,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ds-task"}, &liveTask)
	liveTask.Status.AdminURL = svcURL8081
	liveTask.Status.LastExecutionJobName = ""
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: syncedMetaNew,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if convRet := post("/proxy/default/ds-proxy/run", xxlclient.TriggerParam{
		JobID:                 42,
		ExecutorHandler:       "dsHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 300,
		LogDateTime:           1700000000300,
	}); !convRet.IsSuccess() {
		t.Fatalf("expected trigger after Executor & Task converge to dbNew to succeed: %+v", convRet)
	}
	var newJob batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("ds-proxy", 42, 300)}, &newJob); err != nil {
		t.Fatalf("get newJob: %v", err)
	}
	if newJob.Annotations[AnnotationAdminDatasourceHash] != hashNew {
		t.Fatalf("expected converged Job stamped with hashNew=%s, got %q", hashNew, newJob.Annotations[AnnotationAdminDatasourceHash])
	}

	// Switching Managed -> Pure External (ExternalURL != "", Image == "") before Task/Executor converge must also reject.
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ds-admin"}, &liveAdmin)
	liveAdmin.Spec.ExternalURL = "http://ext-xxl.corp.local:8080/xxl-job-admin"
	liveAdmin.Spec.Image = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)
	if extRet := post("/proxy/default/ds-proxy/run", xxlclient.TriggerParam{
		JobID:                 42,
		ExecutorHandler:       "dsHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 301,
		LogDateTime:           1700000000301,
	}); extRet.IsSuccess() {
		t.Fatalf("expected Managed->PureExternal transition before status convergence to reject stale JobID, got %+v", extRet)
	}
}

func TestDatasourceURLHashCanonicalizationAndEquivalence(t *testing.T) {
	if got := DatasourceURLHash("   \t  "); got != "" {
		t.Fatalf("expected empty hash for whitespace URL, got %q", got)
	}

	// 1. Equivalent JDBC URLs must produce identical hashes.
	equivalentGroups := [][]string{
		{
			"jdbc:mysql://mysql.default.svc:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
			"  JDBC:MySQL://MYSQL.default.svc/xxl_job?serverTimezone=Asia%2FShanghai&characterEncoding=UTF-8&useUnicode=true&  ",
			"jdbc:mysql://mysql.default.svc:3306/xxl_job?CHARACTERENCODING=UTF-8&&useunicode=true&servertimezone=Asia/Shanghai",
			"jdbc:mysql://address=(protocol=tcp)(host=MYSQL.default.svc)(port=3306)/xxl_job?characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useUnicode=true",
			"jdbc:mysql://(host=mysql.default.svc,port=3306)/xxl_job?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8",
		},
		{
			"jdbc:mysql://[::1]:3306/xxl_job?serverTimezone=GMT+8&useSSL=true",
			"jdbc:mysql://[::1]/xxl_job?useSSL=false&useSSL=true&serverTimezone=GMT%2B8",
		},
		{
			"jdbc:mysql://db.default.svc:3306",
			"jdbc:mysql://db.default.svc/",
		},
		{
			"jdbc:postgresql://PG.default.svc/xxl_job?currentSchema=public&sslmode=disable",
			"jdbc:postgresql://pg.default.svc:5432/xxl_job?sslmode=disable&currentSchema=public",
		},
		{
			"jdbc:mysql://address=(protocol=pipe)(path=/var/run/mysqld/mysqld.sock)/xxl_job?useSSL=false&autoReconnect=true",
			"jdbc:mysql://address=(path=/var/run/mysqld/mysqld.sock)(protocol=pipe)/xxl_job?autoReconnect=true&useSSL=false",
		},
		{
			"jdbc:sqlserver://SQL.default.svc;databaseName=xxl_job;encrypt=true;trustServerCertificate=true",
			"jdbc:sqlserver://sql.default.svc:1433;trustServerCertificate=true;encrypt=true;DATABASENAME=xxl_job;",
			"jdbc:sqlserver://sql.default.svc:1433;database=xxl_job;encrypt=true;trustServerCertificate=true",
		},
		{
			"jdbc:sqlserver://sql.default.svc:1433;password={pa;ss};databaseName=xxl_job",
			"jdbc:sqlserver://SQL.default.svc;database=xxl_job;password={pa;ss}",
		},
		{
			"jdbc:mysql://u:p@[HOST1,host2:3306]/xxl_job?b=2&a=1",
			"jdbc:mysql://u:p@[host1:3306,HOST2]/xxl_job?a=1&b=2",
		},
		{
			"jdbc:mysql://localhost:3306/xxl_job?a=1&b=2",
			"jdbc:mysql:///xxl_job?b=2&a=1",
			"jdbc:mysql://:3306/xxl_job?b=2&a=1",
			"jdbc:mysql://address=(port=3306)/xxl_job?b=2&a=1",
		},
		{
			"jdbc:postgresql://localhost:5432/xxl_job?a=1&b=2",
			"jdbc:postgresql:///xxl_job?b=2&a=1",
			"jdbc:postgresql:xxl_job?b=2&a=1",
		},
		{
			"jdbc:oracle:thin:@//ora.default.svc:1521/xxl_job?a=1&b=2",
			"JDBC:ORACLE:THIN:@//ORA.default.svc/xxl_job?b=2&a=1",
		},
	}

	for gIdx, grp := range equivalentGroups {
		expected := DatasourceURLHash(grp[0])
		if expected == "" {
			t.Fatalf("group %d: expected non-empty hash for %q", gIdx, grp[0])
		}
		for _, candidate := range grp[1:] {
			got := DatasourceURLHash(candidate)
			if got != expected {
				t.Fatalf("group %d: expected %q (canon=%q) and %q (canon=%q) to produce identical hash, got %s vs %s",
					gIdx, grp[0], canonicalJDBCURL(grp[0]), candidate, canonicalJDBCURL(candidate), expected, got)
			}
		}
	}

	// 2. Distinct databases/targets must NEVER collide.
	distinctPairs := [][2]string{
		{
			"jdbc:mysql://mysql.default.svc:3306/xxl_job?useUnicode=true",
			"jdbc:mysql://mysql.default.svc:3306/xxl_job_v2?useUnicode=true",
		},
		{
			"jdbc:mysql://mysql.default.svc:3306/xxl_job",
			"jdbc:mysql://mysql.default.svc:3306/XXL_JOB",
		},
		{
			"jdbc:mysql://mysql.default.svc:3306/xxl_job",
			"jdbc:mysql://mysql.default.svc:3307/xxl_job",
		},
		{
			"jdbc:mysql://host1:3306,host2:3306/xxl_job",
			"jdbc:mysql://host2:3306,host1:3306/xxl_job",
		},
		{
			"jdbc:mysql://db.default.svc?DBNAME=xxl_job_a",
			"jdbc:mysql://db.default.svc?DBNAME=xxl_job_b",
		},
		{
			"jdbc:postgresql://pg.default.svc:5432/xxl_job?currentSchema=schema_a",
			"jdbc:postgresql://pg.default.svc:5432/xxl_job?currentSchema=schema_b",
		},
		{
			"jdbc:mysql+srv://cluster.default.svc/xxl_job",
			"jdbc:mysql://cluster.default.svc:3306/xxl_job",
		},
		{
			"jdbc:sqlserver://sql.default.svc:1433;databaseName=xxl_job_a;encrypt=true",
			"jdbc:sqlserver://sql.default.svc:1433;databaseName=xxl_job_b;encrypt=true",
		},
		{
			"jdbc:mysql://u:p@[host1,host2]/xxl_job",
			"jdbc:mysql://u:p@host1,host2/xxl_job",
		},
	}
	for pIdx, pair := range distinctPairs {
		h1 := DatasourceURLHash(pair[0])
		h2 := DatasourceURLHash(pair[1])
		if h1 == h2 {
			t.Fatalf("pair %d: expected distinct hashes for %q and %q, both produced %s", pIdx, pair[0], pair[1], h1)
		}
	}
}

func TestRollingUpgradeLegacyRawDatasourceHashAndModeTransitions(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	// Exact sample JDBC URL from config/samples/01-xxljob-admin.yaml whose pre-canonicalization hash differs from canonical hash.
	sampleURL := "jdbc:mysql://mysql.default.svc.cluster.local:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&serverTimezone=Asia/Shanghai"
	legacyHash := LegacyRawDatasourceURLHash(sampleURL)
	canonHash := DatasourceURLHash(sampleURL)
	if legacyHash == "" || canonHash == "" || legacyHash == canonHash {
		t.Fatalf("expected distinct non-empty legacyHash=%q and canonHash=%q for sampleURL", legacyHash, canonHash)
	}

	svcURL := "http://upg-admin.default.svc.cluster.local:8080/xxl-job-admin"
	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "upg-admin",
			Namespace: "default",
			UID:       "uid-upg-admin",
		},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Image:       "xuxueli/xxl-job-admin:2.4.1",
			Port:        8080,
			ContextPath: "/xxl-job-admin",
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: sampleURL},
		},
	}

	preUpgradeMetaRaw, _ := json.Marshal(SyncedAdminEndpointMeta{
		AdminName:        adminCR.Name,
		AdminUID:         string(adminCR.UID),
		AdminURL:         svcURL,
		ManagedInCluster: true,
		DatasourceHash:   legacyHash,
		AuthCaptured:     true,
	})
	preUpgradeCond := metav1.Condition{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: string(preUpgradeMetaRaw),
	}

	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "upg-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
			AppName:  "upg-proxy-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 1,
			AdminURL:   svcURL,
			Synced:     true,
			Conditions: []metav1.Condition{preUpgradeCond},
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "upg-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
			JobDesc:         "upg-task",
			Author:          "test",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "upgHandler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo upg"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      77,
			JobGroupID: 1,
			AdminURL:   svcURL,
			Synced:     true,
			Conditions: []metav1.Condition{preUpgradeCond},
		},
	}

	// Pre-upgrade in-flight SERIAL_EXECUTION Job carrying legacy raw DatasourceHash.
	suspendFalse := false
	preUpgradeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       BuildJobName("upg-proxy", 77, 500),
			Namespace:  "default",
			Finalizers: []string{JobCallbackFinalizer},
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("upg-proxy"),
				LabelJobID:        "77",
				LabelLogID:        "500",
			},
			Annotations: map[string]string{
				AnnotationTaskNamespace:       "default",
				AnnotationTaskName:            "upg-task",
				AnnotationExecutorName:        "upg-proxy",
				AnnotationAdminName:           adminCR.Name,
				AnnotationAdminUID:            string(adminCR.UID),
				AnnotationAdminURL:            svcURL,
				AnnotationAdminDatasourceHash: legacyHash,
				AnnotationLogID:               "500",
				AnnotationLogDateTime:         "1700000000500",
				AnnotationBlockStrategy:       "SERIAL_EXECUTION",
				AnnotationCallbackSent:        "false",
			},
		},
		Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
		Status: batchv1.JobStatus{Active: 1},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminCR, execCR, taskCR, preUpgradeJob).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "upg-tok")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/upg-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "upg-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Before Executor/Task reconcile after upgrade, /idleBeat sees preUpgradeJob and /run succeeds!
	if ib := post("idleBeat", xxlclient.IdleBeatParam{JobID: 77}); ib.IsSuccess() {
		t.Fatalf("expected /idleBeat to see running preUpgradeJob across rolling upgrade, got %+v", ib)
	}

	// Partial reconcile window: Executor reconciles canonical hash while Task still holds legacyHash.
	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: EncodeSyncedAdminEndpointMeta(adminCR, svcURL),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	if runRet := post("run", xxlclient.TriggerParam{
		JobID:                 77,
		ExecutorHandler:       "upgHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 501,
		LogDateTime:           1700000000501,
	}); !runRet.IsSuccess() {
		t.Fatalf("expected /run during Executor-converged / Task-pre-upgrade rolling window to succeed, got %+v", runRet)
	}

	// SERIAL_EXECUTION partition MUST group preUpgradeJob (legacyHash) and postUpgradeJob (canonHash + alt legacyHash)!
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("upg-proxy"), "77"); err != nil {
		t.Fatalf("AdvanceSerialQueueWithReader: %v", err)
	}
	var postUpgradeJob batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("upg-proxy", 77, 501)}, &postUpgradeJob); err != nil {
		t.Fatalf("get postUpgradeJob: %v", err)
	}
	if postUpgradeJob.Spec.Suspend == nil || !*postUpgradeJob.Spec.Suspend {
		t.Fatalf("expected postUpgradeJob to remain suspended while preUpgradeJob (legacyHash) is still Active=1")
	}

	// /kill matches and deletes preUpgradeJob (and postUpgradeJob) across the hash format upgrade!
	if killRet := post("kill", xxlclient.KillParam{JobID: 77}); !killRet.IsSuccess() {
		t.Fatalf("expected /kill to succeed: %+v", killRet)
	}
	var killedPre batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: preUpgradeJob.Name}, &killedPre)
	if killedPre.DeletionTimestamp.IsZero() {
		t.Fatalf("expected /kill to set DeletionTimestamp on preUpgradeJob carrying legacyHash")
	}

	// 2. Empty -> non-empty Datasource.URL on a Managed Admin before status convergence MUST reject stale JobID!
	var liveAdmin xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)

	emptyMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, svcURL)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: emptyMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: emptyMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	liveAdmin.Spec.Datasource.URL = "jdbc:mysql://new-db.default.svc:3306/xxl_job_fresh"
	_ = fakeK8s.Update(ctx, &liveAdmin)
	if emptyToFullRet := post("run", xxlclient.TriggerParam{
		JobID:                 77,
		ExecutorHandler:       "upgHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 502,
		LogDateTime:           1700000000502,
	}); emptyToFullRet.IsSuccess() {
		t.Fatalf("expected empty->non-empty Datasource.URL switch before convergence to reject stale JobID, got %+v", emptyToFullRet)
	}

	// 3. Pure External -> Hybrid Managed transition at the same ExternalURL before convergence MUST reject stale JobID!
	extURL := "http://hybrid-ext.corp.local:8080/xxl-job-admin"
	liveAdmin.Spec.ExternalURL = extURL
	liveAdmin.Spec.Image = ""
	liveAdmin.Spec.Datasource.URL = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)

	pureExtMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.AdminURL = extURL
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: pureExtMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.AdminURL = extURL
	liveTask.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: pureExtMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	liveAdmin.Spec.Image = "xuxueli/xxl-job-admin:2.4.1"
	liveAdmin.Spec.Datasource.URL = "jdbc:mysql://hybrid-db.default.svc:3306/xxl_job_hybrid"
	_ = fakeK8s.Update(ctx, &liveAdmin)
	if extToHybridRet := post("run", xxlclient.TriggerParam{
		JobID:                 77,
		ExecutorHandler:       "upgHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 503,
		LogDateTime:           1700000000503,
	}); extToHybridRet.IsSuccess() {
		t.Fatalf("expected PureExternal->HybridManaged transition before convergence to reject stale JobID, got %+v", extToHybridRet)
	}
}

func TestModeOriginIsolationZeroPodBgDrainAndHalfConvergedUpgrade(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	extURL := "http://mode-iso.corp.local:8080/xxl-job-admin"
	adminCR := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "mode-iso-adm", Namespace: "default", UID: types.UID("uid-mode-iso")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: extURL,
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "mode-iso-tok"},
		},
	}
	initMeta := EncodeSyncedAdminEndpointMeta(adminCR, extURL)
	adminCR.Status.AdminURL = extURL
	adminCR.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: initMeta,
	}}

	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "mode-iso-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminCR.Name},
			AppName:  "mode-iso-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 1,
			AdminURL:   extURL,
			Synced:     true,
			Conditions: []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: initMeta}},
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "mode-iso-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
			JobDesc:         "mode-iso",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "modeIsoHandler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo iso"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      88,
			JobGroupID: 1,
			AdminURL:   extURL,
			Synced:     true,
			Conditions: []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: initMeta}},
		},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminCR, execCR, taskCR).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "mode-iso-tok")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/mode-iso-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "mode-iso-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Trigger Pure-External Job -> stamps AnnotationAdminManaged="false"; then switch to Hybrid Managed (empty DB, then non-empty DB)
	// on the SAME URL and verify colliding (jobId=88, logId=1) & /idleBeat & DISCARD_LATER isolate all three origins!
	if r1 := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1,
		LogDateTime:           1700000000001,
	}); !r1.IsSuccess() {
		t.Fatalf("Pure-External /run failed: %+v", r1)
	}
	baseJobName := BuildJobName("mode-iso-proxy", 88, 1)
	var pureExtJob batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: baseJobName}, &pureExtJob); err != nil {
		t.Fatalf("get pureExtJob: %v", err)
	}
	if pureExtJob.Annotations[AnnotationAdminManaged] != "false" {
		t.Fatalf("expected AnnotationAdminManaged=false on Pure-External Job, got %+v", pureExtJob.Annotations)
	}
	pureExtJob.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &pureExtJob)

	// Switch in place to Hybrid Managed with empty Datasource.URL and converge Admin + Executor + Task
	var liveAdmin xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
	liveAdmin.Spec.Image = "xuxueli/xxl-job-admin:2.4.1"
	liveAdmin.Spec.Datasource.URL = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)
	emptyHybridMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: emptyHybridMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: emptyHybridMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.LastExecutionJobName = ""
	liveTask.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: emptyHybridMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if ib := post("idleBeat", xxlclient.IdleBeatParam{JobID: 88}); !ib.IsSuccess() {
		t.Fatalf("expected /idleBeat on Hybrid Managed empty-DB not to be blocked by active Pure-External Job, got %+v", ib)
	}
	if r2 := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1,
		LogDateTime:           1700000000002,
	}); !r2.IsSuccess() {
		t.Fatalf("expected colliding logId=1 on Hybrid Managed empty-DB to succeed via distinct origin key, got %+v", r2)
	}
	emptyHybridKey := CanonicalAdminOriginKey(adminCR.Name, extURL, string(adminCR.UID)) + "|db=empty"
	emptyHybridJobName := BuildJobName("mode-iso-proxy", 88, 1, emptyHybridKey)
	var emptyHybridJob batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: emptyHybridJobName}, &emptyHybridJob); err != nil {
		t.Fatalf("get emptyHybridJob (%s): %v", emptyHybridJobName, err)
	}
	if emptyHybridJob.Annotations[AnnotationAdminManaged] != "true" {
		t.Fatalf("expected AnnotationAdminManaged=true on Hybrid Managed empty-DB Job, got %+v", emptyHybridJob.Annotations)
	}
	emptyHybridJob.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &emptyHybridJob)

	// 2. Half-converged rolling-upgrade window:
	// Admin and Executor update to non-empty Datasource.URL (admMeta & execMeta = dbNew), while legacy Task has taskMeta == nil!
	// Stale JobID=88 MUST be rejected until Task also converges ConditionAdminEndpointSynced!
	dbNew := "jdbc:mysql://new-db.default.svc:3306/xxl_job_prod"
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = dbNew
	_ = fakeK8s.Update(ctx, &liveAdmin)
	dbNewMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: dbNewMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: dbNewMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = nil // Legacy unconverged XxlJobTask (taskMeta == nil)!
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if halfConvRet := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1,
		LogDateTime:           1700000000003,
	}); halfConvRet.IsSuccess() {
		t.Fatalf("expected half-converged window (Admin+Exec synced dbNew, legacy Task taskMeta==nil) to reject stale JobID, got %+v", halfConvRet)
	}

	// Also reject when Admin already synced dbNew (admMeta != nil) while BOTH Exec and Task still have nil Conditions:
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = nil
	_ = fakeK8s.Status().Update(ctx, &liveExec)
	if adminOnlyConvRet := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1,
		LogDateTime:           1700000000003,
	}); adminOnlyConvRet.IsSuccess() {
		t.Fatalf("expected half-converged window (Managed Admin synced dbNew, legacy Exec+Task nil) to reject stale JobID, got %+v", adminOnlyConvRet)
	}

	// Once Executor and Task converge ConditionAdminEndpointSynced to dbNew, trigger succeeds and isolates from emptyHybridJob & pureExtJob!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: dbNewMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: dbNewMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if fullConvRet := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 1,
		LogDateTime:           1700000000003,
	}); !fullConvRet.IsSuccess() {
		t.Fatalf("expected converged dbNew DISCARD_LATER trigger to succeed without blocking on Pure-External or empty-DB Jobs: %+v", fullConvRet)
	}

	// 3. Background-deleted Job with real UID (UID != "", as assigned by kube-apiserver), frozen Status.Active=1, and 0 owned Pods in etcd:
	// MUST NOT block /idleBeat, DISCARD_LATER (including LastExecutionJobName winner replacement), or SERIAL_EXECUTION for 10m!
	dbNewKey := CanonicalAdminOriginKey(adminCR.Name, extURL, string(adminCR.UID)) + "|db=" + DatasourceURLHash(dbNew)
	dbNewJob1Name := BuildJobName("mode-iso-proxy", 88, 1, dbNewKey)
	var dbNewJob1 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: dbNewJob1Name}, &dbNewJob1); err != nil {
		t.Fatalf("get dbNewJob1: %v", err)
	}
	dbNewJob1.UID = types.UID("uid-real-apiserver-dbnew-job1")
	_ = fakeK8s.Update(ctx, &dbNewJob1)
	dbNewJob1.Status.Active = 1 // Frozen Active=1 while 0 Pods exist in etcd!
	_ = fakeK8s.Status().Update(ctx, &dbNewJob1)
	// Default Background delete (no foregroundDeletion finalizer, no AnnotationBackgroundPodsTerminating)
	if err := fakeK8s.Delete(ctx, &dbNewJob1); err != nil {
		t.Fatalf("background delete dbNewJob1: %v", err)
	}

	if ibAfterBgDel := post("idleBeat", xxlclient.IdleBeatParam{JobID: 88}); !ibAfterBgDel.IsSuccess() {
		t.Fatalf("expected /idleBeat to succeed immediately when background-deleted real-UID Job has 0 Pods in etcd, got %+v", ibAfterBgDel)
	}
	if nextDiscardRet := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "DISCARD_LATER",
		LogID:                 2,
		LogDateTime:           1700000000004,
	}); !nextDiscardRet.IsSuccess() {
		t.Fatalf("expected DISCARD_LATER logId=2 not to hang 10m behind background-deleted 0-Pod winner Job, got %+v", nextDiscardRet)
	}

	// Also verify AdvanceSerialQueueWithReader unsuspends a queued SERIAL_EXECUTION Job immediately behind a real-UID 0-Pod deleting Job:
	dbNewJob2Name := BuildJobName("mode-iso-proxy", 88, 2)
	var dbNewJob2 batchv1.Job
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: dbNewJob2Name}, &dbNewJob2)
	dbNewJob2.UID = types.UID("uid-real-apiserver-dbnew-job2")
	_ = fakeK8s.Update(ctx, &dbNewJob2)
	dbNewJob2.Status.Active = 1
	_ = fakeK8s.Status().Update(ctx, &dbNewJob2)
	_ = fakeK8s.Delete(ctx, &dbNewJob2)
	if serRet := post("run", xxlclient.TriggerParam{
		JobID:                 88,
		ExecutorHandler:       "modeIsoHandler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 3,
		LogDateTime:           1700000000005,
	}); !serRet.IsSuccess() {
		t.Fatalf("expected SERIAL_EXECUTION logId=3 enqueue to succeed: %+v", serRet)
	}
	if err := AdvanceSerialQueueWithReader(ctx, fakeK8s, fakeK8s, "default", SanitizeLabel("mode-iso-proxy"), "88"); err != nil {
		t.Fatalf("AdvanceSerialQueueWithReader after 0-Pod background delete: %v", err)
	}
	var dbNewJob3 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("mode-iso-proxy", 88, 3)}, &dbNewJob3); err != nil {
		t.Fatalf("get dbNewJob3: %v", err)
	}
	if dbNewJob3.Spec.Suspend != nil && *dbNewJob3.Spec.Suspend {
		t.Fatalf("expected SERIAL_EXECUTION logId=3 to unsuspend immediately when predecessor real-UID deleting Job has 0 Pods in etcd")
	}
	dbNewJob3.Finalizers = nil
	_ = fakeK8s.Update(ctx, &dbNewJob3)
	_ = fakeK8s.Delete(ctx, &dbNewJob3)

	// Meanwhile, minimal scheme (Pod GVK unregistered) OR synthetic UID=="" Job conservatively falls back to HasActiveOrTerminatingPods=true:
	nowDel := metav1.Now()
	uidLessBgJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "syn-uidless-bg",
			Namespace:         "default",
			DeletionTimestamp: &nowDel,
			Finalizers:        []string{JobCallbackFinalizer},
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	if draining, err := isDeletingJobDrainingPods(ctx, fakeK8s, uidLessBgJob); err != nil || !draining {
		t.Fatalf("expected synthetic UID-less deleting Job with Active=1 to report draining=true, got draining=%v err=%v", draining, err)
	}
	minScheme := runtime.NewScheme()
	_ = batchv1.AddToScheme(minScheme)
	realUIDInMinScheme := uidLessBgJob.DeepCopy()
	realUIDInMinScheme.UID = "uid-min-scheme-job"
	minCli := fake.NewClientBuilder().WithScheme(minScheme).WithObjects(realUIDInMinScheme).Build()
	if draining, err := isDeletingJobDrainingPods(ctx, minCli, realUIDInMinScheme); err != nil || !draining {
		t.Fatalf("expected minimal-scheme client (Pod GVK unregistered) to conservatively fall back to draining=true, got draining=%v err=%v", draining, err)
	}

	// 4. Single-sided Pure-External ConditionAdminEndpointSynced on liveExec (with taskMeta == nil and admMeta == nil)
	// remains compatible with synthetic tests and Pure-External mode.
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminCR.Name}, &liveAdmin)
	liveAdmin.Spec.Image = ""
	liveAdmin.Spec.Datasource.URL = ""
	liveAdmin.Spec.ExternalURL = extURL
	_ = fakeK8s.Update(ctx, &liveAdmin)
	liveAdmin.Status.Conditions = nil
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	singleSidePureMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: singleSidePureMeta}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = nil
	liveTask.Status.LastExecutionJobName = ""
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if pureSingleRet := post("run", xxlclient.TriggerParam{
		JobID:           88,
		ExecutorHandler: "modeIsoHandler",
		LogID:           99,
		LogDateTime:     1700000000099,
	}); !pureSingleRet.IsSuccess() {
		t.Fatalf("expected single-sided Pure-External ConditionAdminEndpointSynced on liveExec with nil taskMeta/admMeta to succeed: %+v", pureSingleRet)
	}
}

func TestAdversarialHalfConvergedWindowsAnd1cf04b3OriginIsolation(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = xxljobv1alpha1.AddToScheme(scheme)

	extURL := "http://adv-r2-ext.example.com:8080/xxl-job-admin"
	otherURL := "http://adv-r2-other.example.com:8080/xxl-job-admin"
	jdbcOrder1 := "jdbc:mysql://mysql:3306/xxl_job?zParam=9&aParam=1&mParam=5"
	jdbcOrder2 := "jdbc:mysql://mysql:3306/xxl_job?mParam=5&zParam=9&aParam=1"
	rawHash1 := LegacyRawDatasourceURLHash(jdbcOrder1)
	canonHash := DatasourceURLHash(jdbcOrder1)

	adminA := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-r2-adm-a", Namespace: "default", UID: types.UID("uid-adv-r2-a")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			Image:       "xuxueli/xxl-job-admin:2.4.1",
			ExternalURL: extURL,
			Datasource:  xxljobv1alpha1.DatasourceSpec{URL: jdbcOrder1},
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "adv-r2-tok"},
		},
	}
	initAdmMeta := EncodeSyncedAdminEndpointMeta(adminA, extURL)
	adminA.Status = xxljobv1alpha1.XxlJobAdminStatus{
		AdminURL: extURL,
		Conditions: []metav1.Condition{{
			Type:    ConditionAdminEndpointSynced,
			Status:  metav1.ConditionTrue,
			Message: initAdmMeta,
		}},
	}
	adminB := &xxljobv1alpha1.XxlJobAdmin{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-r2-adm-b", Namespace: "default", UID: types.UID("uid-adv-r2-b")},
		Spec: xxljobv1alpha1.XxlJobAdminSpec{
			ExternalURL: otherURL,
			Auth:        xxljobv1alpha1.AuthSpec{AccessToken: "adv-r2-tok"},
		},
		Status: xxljobv1alpha1.XxlJobAdminStatus{AdminURL: otherURL},
	}

	execCR := &xxljobv1alpha1.XxlJobExecutor{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-r2-proxy", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobExecutorSpec{
			AdminRef: xxljobv1alpha1.LocalObjectReference{Name: adminA.Name},
			AppName:  "adv-r2-app",
			Mode:     xxljobv1alpha1.ExecutorModeK8sJobProxy,
		},
		Status: xxljobv1alpha1.XxlJobExecutorStatus{
			JobGroupID: 1,
			AdminURL:   extURL,
			Synced:     true,
		},
	}
	taskCR := &xxljobv1alpha1.XxlJobTask{
		ObjectMeta: metav1.ObjectMeta{Name: "adv-r2-task", Namespace: "default"},
		Spec: xxljobv1alpha1.XxlJobTaskSpec{
			ExecutorRef:     xxljobv1alpha1.LocalObjectReference{Name: execCR.Name},
			JobDesc:         "adv-r2-task",
			Author:          "ops",
			ScheduleType:    "CRON",
			ScheduleConf:    "0 * * * * ?",
			ExecutorHandler: "advR2Handler",
			K8sJobTemplate: &xxljobv1alpha1.K8sJobTemplateSpec{
				Image:   "alpine:3.19",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo ok"},
			},
		},
		Status: xxljobv1alpha1.XxlJobTaskStatus{
			JobID:      66,
			JobGroupID: 1,
			AdminURL:   extURL,
			Synced:     true,
		},
	}

	// Pre-upgrade 1cf04b3 Pure-External active Job (UID set, NO AnnotationAdminManaged, NO dbHash) on extURL!
	suspendFalse := false
	pure1cfActiveJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:       BuildJobName("adv-r2-proxy", 66, 9000),
			Namespace:  "default",
			UID:        types.UID("uid-pure-1cf-active"),
			Finalizers: []string{JobCallbackFinalizer},
			Labels: map[string]string{
				LabelManagedBy:    ManagedByValue,
				LabelExecutorName: SanitizeLabel("adv-r2-proxy"),
				LabelJobID:        "66",
				LabelLogID:        "9000",
			},
			Annotations: map[string]string{
				AnnotationExecutorName:  "adv-r2-proxy",
				AnnotationAdminName:     adminA.Name,
				AnnotationAdminUID:      string(adminA.UID),
				AnnotationAdminURL:      extURL,
				AnnotationLogID:         "9000",
				AnnotationLogDateTime:   "1700000009000",
				AnnotationBlockStrategy: "SERIAL_EXECUTION",
			},
		},
		Spec:   batchv1.JobSpec{Suspend: &suspendFalse},
		Status: batchv1.JobStatus{Active: 1},
	}

	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(adminA, adminB, execCR, taskCR, pure1cfActiveJob).
		WithStatusSubresource(&xxljobv1alpha1.XxlJobAdmin{}, &xxljobv1alpha1.XxlJobExecutor{}, &xxljobv1alpha1.XxlJobTask{}, &batchv1.Job{}).
		Build()

	srv := NewServer(fakeK8s, ":0", "default", "adv-r2-tok")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func(action string, body any) xxlclient.ReturnT {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/default/adv-r2-proxy/"+action, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "adv-r2-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed: %v", action, err)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Pre-reconcile AdminRef switch (exec.Spec.AdminRef -> adminB while task.Status.AdminURL is still adminA's extURL)
	// when adminA is Managed and taskMeta == nil -> MUST find adminA via activeURL and reject stale JobID=66!
	var liveExec xxljobv1alpha1.XxlJobExecutor
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Spec.AdminRef.Name = adminB.Name
	_ = fakeK8s.Update(ctx, &liveExec)
	if ret := post("run", xxlclient.TriggerParam{
		JobID:           66,
		ExecutorHandler: "advR2Handler",
		LogID:           1,
		LogDateTime:     1700000000001,
	}); ret.IsSuccess() {
		t.Fatalf("expected half-converged Managed adminA found via activeURL (despite Spec.AdminRef=adminB) to reject nil taskMeta, got %+v", ret)
	}
	liveExec.Spec.AdminRef.Name = adminA.Name
	_ = fakeK8s.Update(ctx, &liveExec)

	// 2. Single-sided liveTask synthetic test setup (liveTask has converged Managed ConditionAdminEndpointSynced, liveExec has NO Conditions)
	// MUST succeed, AND 1cf04b3 Pure-External active Job (pure1cfActiveJob) MUST be isolated from Managed non-empty DB!
	var liveTask xxljobv1alpha1.XxlJobTask
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: initAdmMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if ib := post("idleBeat", xxlclient.IdleBeatParam{JobID: 66}); !ib.IsSuccess() {
		t.Fatalf("expected /idleBeat on Hybrid Managed DB not to be blocked by 1cf04b3 Pure-External Job, got %+v", ib)
	}

	// Reorder JDBC params to unsorted jdbcOrder2 on adminA (preserving rawHash1 in LegacyDatasourceHash) while liveTask
	// still holds pre-upgrade rawHash1 -> resolveExecutorOrigin MUST preserve canonical canonHash + alt rawHash1!
	var liveAdmin xxljobv1alpha1.XxlJobAdmin
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminA.Name}, &liveAdmin)
	preUpgTaskBytes, _ := json.Marshal(SyncedAdminEndpointMeta{
		AdminName:        adminA.Name,
		AdminUID:         string(adminA.UID),
		AdminURL:         extURL,
		ManagedInCluster: true,
		DatasourceHash:   rawHash1,
		AuthCaptured:     true,
	})
	liveAdmin.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: string(preUpgTaskBytes),
	}}
	firstUpgMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: firstUpgMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminA.Name}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = jdbcOrder2
	_ = fakeK8s.Update(ctx, &liveAdmin)
	reorderedAdmMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: reorderedAdmMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: string(preUpgTaskBytes),
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if runRet := post("run", xxlclient.TriggerParam{
		JobID:                 66,
		ExecutorHandler:       "advR2Handler",
		ExecutorBlockStrategy: "SERIAL_EXECUTION",
		LogID:                 10,
		LogDateTime:           1700000000010,
	}); !runRet.IsSuccess() {
		t.Fatalf("expected single-sided liveTask trigger across multi-reorder JDBC equivalence to succeed: %+v", runRet)
	}
	var job10 batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: BuildJobName("adv-r2-proxy", 66, 10)}, &job10); err != nil {
		t.Fatalf("get job10: %v", err)
	}
	if job10.Annotations[AnnotationAdminDatasourceHash] != canonHash ||
		job10.Annotations[AnnotationAdminLegacyDatasourceHash] != rawHash1 {
		t.Fatalf("expected job10 to preserve canonical=%s and legacy=%s across multi-reorder, got %+v",
			canonHash, rawHash1, job10.Annotations)
	}

	// /kill on Hybrid Managed DB MUST delete job10 without touching 1cf04b3 Pure-External pure1cfActiveJob!
	if killRet := post("kill", xxlclient.KillParam{JobID: 66}); !killRet.IsSuccess() {
		t.Fatalf("expected /kill to succeed: %+v", killRet)
	}
	var checkPure batchv1.Job
	if err := fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: pure1cfActiveJob.Name}, &checkPure); err != nil || !checkPure.DeletionTimestamp.IsZero() {
		t.Fatalf("expected 1cf04b3 Pure-External Job %s NOT to be deleted by /kill on Hybrid Managed DB, err=%v delTS=%v",
			pure1cfActiveJob.Name, err, checkPure.DeletionTimestamp)
	}

	// 3. Minimal pre-mode-capture JSON on liveTask vs post-upgrade Hybrid Managed empty-DB on liveExec -> MUST reject!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminA.Name}, &liveAdmin)
	liveAdmin.Spec.Datasource.URL = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)
	empAdmMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: empAdmMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: empAdmMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: `{"adminName":"adv-r2-adm-a","adminUID":"uid-adv-r2-a","adminURL":"` + extURL + `"}`,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if asymRet := post("run", xxlclient.TriggerParam{
		JobID:           66,
		ExecutorHandler: "advR2Handler",
		LogID:           11,
		LogDateTime:     1700000000011,
	}); asymRet.IsSuccess() {
		t.Fatalf("expected pre-mode-capture taskMeta vs Managed empty-DB execMeta asymmetry to reject trigger, got %+v", asymRet)
	}

	// 4. Hybrid Managed -> Pure-External switch where XxlJobAdmin already reconciled ConditionAdminEndpointSynced +
	// ConditionPreviousAdminEndpoints while execMeta == nil and taskMeta == nil -> MUST reject until Task reconciles!
	var retiredSlice []SyncedAdminEndpointMeta
	if dec := DecodeSyncedAdminEndpointMeta([]metav1.Condition{{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: empAdmMeta}}); dec != nil {
		retiredSlice = append(retiredSlice, *dec)
	}
	retiredJSON, _ := json.Marshal(retiredSlice)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: adminA.Name}, &liveAdmin)
	liveAdmin.Spec.Image = ""
	liveAdmin.Spec.Datasource.URL = ""
	_ = fakeK8s.Update(ctx, &liveAdmin)
	newPureMeta := EncodeSyncedAdminEndpointMeta(&liveAdmin, extURL)
	liveAdmin.Status.Conditions = []metav1.Condition{
		{Type: ConditionAdminEndpointSynced, Status: metav1.ConditionTrue, Message: newPureMeta},
		{Type: ConditionPreviousAdminEndpoints, Status: metav1.ConditionTrue, Message: string(retiredJSON)},
	}
	_ = fakeK8s.Status().Update(ctx, &liveAdmin)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: execCR.Name}, &liveExec)
	liveExec.Status.Conditions = nil
	_ = fakeK8s.Status().Update(ctx, &liveExec)

	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = nil
	liveTask.Status.LastExecutionJobName = ""
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if mgrToPureRet := post("run", xxlclient.TriggerParam{
		JobID:           66,
		ExecutorHandler: "advR2Handler",
		LogID:           12,
		LogDateTime:     1700000000012,
	}); mgrToPureRet.IsSuccess() {
		t.Fatalf("expected Managed->Pure-External half-converged window (with ConditionPreviousAdminEndpoints & nil taskMeta) to reject stale JobID, got %+v", mgrToPureRet)
	}

	// Once Task reconciles newPureMeta, trigger succeeds!
	_ = fakeK8s.Get(ctx, client.ObjectKey{Namespace: "default", Name: taskCR.Name}, &liveTask)
	liveTask.Status.Conditions = []metav1.Condition{{
		Type:    ConditionAdminEndpointSynced,
		Status:  metav1.ConditionTrue,
		Message: newPureMeta,
	}}
	_ = fakeK8s.Status().Update(ctx, &liveTask)

	if convergedPureRet := post("run", xxlclient.TriggerParam{
		JobID:           66,
		ExecutorHandler: "advR2Handler",
		LogID:           12,
		LogDateTime:     1700000000012,
	}); !convergedPureRet.IsSuccess() {
		t.Fatalf("expected converged Pure-External Task to succeed after Managed->Pure-External transition, got %+v", convergedPureRet)
	}
}
