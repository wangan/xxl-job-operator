package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

func TestSidecarAgentLifecycleAndSecurity(t *testing.T) {
	t.Run("RejectEmptyOrDefaultToken", func(t *testing.T) {
		if _, err := NewAgent(Config{AdminURL: "http://admin:8080/xxl-job-admin", AccessToken: ""}); err == nil {
			t.Fatalf("expected NewAgent with empty AccessToken to fail (R5-P0-1)")
		}
		if _, err := NewAgent(Config{AdminURL: "http://admin:8080/xxl-job-admin", AccessToken: "default_token"}); err == nil {
			t.Fatalf("expected NewAgent with default_token to fail (R5-P0-1)")
		}
	})

	var mu sync.Mutex
	registered := make(map[string]bool)
	var callbacks []xxlclient.HandleCallbackParam
	var failFirstCallback atomic.Bool

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/api/registry", func(w http.ResponseWriter, r *http.Request) {
		var p xxlclient.RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		registered[p.RegistryValue] = true
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	adminMux.HandleFunc("/api/registryRemove", func(w http.ResponseWriter, r *http.Request) {
		var p xxlclient.RegistryParam
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		delete(registered, p.RegistryValue)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	adminMux.HandleFunc("/api/callback", func(w http.ResponseWriter, r *http.Request) {
		if failFirstCallback.CompareAndSwap(true, false) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var cbs []xxlclient.HandleCallbackParam
		_ = json.NewDecoder(r.Body).Decode(&cbs)
		mu.Lock()
		callbacks = append(callbacks, cbs...)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
	})
	adminSrv := httptest.NewServer(adminMux)
	defer adminSrv.Close()

	// 1. Verify GLUE execution is disabled by default (fixes R5-P0-1)
	t.Run("GlueDisabledByDefault", func(t *testing.T) {
		defaultAgent, err := NewAgent(Config{
			AdminURL:          adminSrv.URL,
			AccessToken:       "secret-sidecar-token",
			AppName:           "py-worker",
			PodIP:             "10.244.1.25",
			ExecutorPort:      9999,
			EnableGlue:        false,
			CallbackRetryWait: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent failed: %v", err)
		}
		srv := httptest.NewServer(defaultAgent.Handler())
		defer srv.Close()

		triggerBytes, _ := json.Marshal(xxlclient.TriggerParam{
			JobID:      70,
			LogID:      500,
			GlueType:   "GLUE_SHELL",
			GlueSource: "echo pwned",
		})
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", bytes.NewReader(triggerBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "secret-sidecar-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /run failed: %v", err)
		}
		resp.Body.Close()

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(callbacks)
			mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		mu.Lock()
		if len(callbacks) != 1 || callbacks[0].LogID != 500 || callbacks[0].HandleCode != 500 {
			t.Fatalf("expected GLUE_SHELL to fail with 500 when EnableGlue=false (R5-P0-1), got %+v", callbacks)
		}
		callbacks = nil
		mu.Unlock()
	})

	// 2. Test full lifecycle with EnableGlue=true, multi-Admin URLs (R5-P2-8), and callback retry (R5-P1-4)
	agent, err := NewAgent(Config{
		AdminURL:          "http://127.0.0.1:1," + adminSrv.URL, // first Admin unreachable, second succeeds (R5-P2-8)
		AccessToken:       "secret-sidecar-token",
		AppName:           "py-worker",
		PodIP:             "10.244.1.25",
		ExecutorPort:      9999,
		MaxLogEntries:     2,
		EnableGlue:        true,
		CallbackRetryWait: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewAgent failed: %v", err)
	}

	if err := agent.Register(context.Background()); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	mu.Lock()
	if !registered["http://10.244.1.25:9999/"] {
		t.Fatalf("expected address registered")
	}
	mu.Unlock()

	sidecarSrv := httptest.NewServer(agent.Handler())
	defer sidecarSrv.Close()

	t.Run("UnauthenticatedRunRejected", func(t *testing.T) {
		triggerBytes, _ := json.Marshal(xxlclient.TriggerParam{
			JobID:      77,
			LogID:      501,
			GlueType:   "GLUE_SHELL",
			GlueSource: "echo 'line1\nline2'",
		})
		unauthResp, err := http.Post(sidecarSrv.URL+"/run", "application/json", bytes.NewReader(triggerBytes))
		if err != nil {
			t.Fatalf("POST /run failed: %v", err)
		}
		var unauthRet xxlclient.ReturnT
		_ = json.NewDecoder(unauthResp.Body).Decode(&unauthRet)
		unauthResp.Body.Close()
		if unauthRet.IsSuccess() {
			t.Fatalf("expected unauthenticated /run to be rejected")
		}
	})

	t.Run("CallbackRetryAndExecution", func(t *testing.T) {
		failFirstCallback.Store(true) // First callback attempt returns HTTP 500, retry must succeed (fixes R5-P1-4)
		triggerBytes, _ := json.Marshal(xxlclient.TriggerParam{
			JobID:      77,
			LogID:      501,
			GlueType:   "GLUE_SHELL",
			GlueSource: "echo 'line1\nline2'",
		})
		req, _ := http.NewRequest(http.MethodPost, sidecarSrv.URL+"/run", bytes.NewReader(triggerBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, "secret-sidecar-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("authenticated POST /run failed: %v", err)
		}
		resp.Body.Close()

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(callbacks)
			mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}

		mu.Lock()
		if len(callbacks) != 1 || callbacks[0].LogID != 501 || callbacks[0].HandleCode != 200 {
			t.Fatalf("unexpected callbacks after retry: %+v", callbacks)
		}
		mu.Unlock()
	})

	t.Run("LogSlicingAndFIFOEviction", func(t *testing.T) {
		agent.storeLog(502, "log502")
		agent.storeLog(503, "log503") // 501 should be evicted because MaxLogEntries=2

		logReqBody, _ := json.Marshal(xxlclient.LogParam{LogID: 501, FromLineNum: 1})
		logReq, _ := http.NewRequest(http.MethodPost, sidecarSrv.URL+"/log", bytes.NewReader(logReqBody))
		logReq.Header.Set(xxlclient.AccessTokenHeader, "secret-sidecar-token")
		logResp, err := http.DefaultClient.Do(logReq)
		if err != nil {
			t.Fatalf("POST /log failed: %v", err)
		}
		var logRet xxlclient.ReturnT
		_ = json.NewDecoder(logResp.Body).Decode(&logRet)
		logResp.Body.Close()
		var logRes xxlclient.LogResult
		_ = json.Unmarshal(logRet.Content, &logRes)
		if logRes.ToLineNum != 0 || logRes.LogContent != "" {
			t.Fatalf("expected evicted log 501 to return empty content and ToLineNum=0, got %+v", logRes)
		}
	})

	t.Run("PreStopLoopbackEnforcementAndDeregister", func(t *testing.T) {
		// Non-loopback request without token must be rejected with 403 (fixes R5-P0-1)
		remoteReq := httptest.NewRequest(http.MethodPost, "/preStop", nil)
		remoteReq.RemoteAddr = "10.244.9.99:54321"
		rec := httptest.NewRecorder()
		agent.Handler().ServeHTTP(rec, remoteReq)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected non-loopback /preStop without token to return 403, got %d", rec.Code)
		}

		// Loopback request without token or shared secret must also be rejected with 403 (fixes R7-P2-3)
		unauthLoopbackResp, err := http.Post(sidecarSrv.URL+"/preStop", "application/json", nil)
		if err != nil {
			t.Fatalf("unauthenticated loopback POST /preStop failed: %v", err)
		}
		unauthLoopbackResp.Body.Close()
		if unauthLoopbackResp.StatusCode != http.StatusForbidden {
			t.Fatalf("R7-P2-3: expected loopback /preStop without token to return 403, got %d", unauthLoopbackResp.StatusCode)
		}

		// Authenticated loopback request with XXL-JOB-ACCESS-TOKEN succeeds and deregisters
		preStopReq, _ := http.NewRequest(http.MethodPost, sidecarSrv.URL+"/preStop", nil)
		preStopReq.Header.Set(xxlclient.AccessTokenHeader, "secret-sidecar-token")
		preStopResp, err := http.DefaultClient.Do(preStopReq)
		if err != nil {
			t.Fatalf("POST /preStop failed: %v", err)
		}
		preStopResp.Body.Close()
		if preStopResp.StatusCode != http.StatusOK {
			t.Fatalf("expected authenticated /preStop to return 200, got %d", preStopResp.StatusCode)
		}

		mu.Lock()
		if registered["http://10.244.1.25:9999/"] {
			t.Fatalf("expected address deregistered after authenticated /preStop")
		}
		mu.Unlock()

		// Once draining, /readyz must return 503, Register() must be a no-op, and /run must reject new triggers (R6-P1-11)
		readyResp, _ := http.Get(sidecarSrv.URL + "/readyz")
		readyResp.Body.Close()
		if readyResp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected /readyz to return 503 after /preStop, got %d", readyResp.StatusCode)
		}
		_ = agent.Register(context.Background())
		mu.Lock()
		if registered["http://10.244.1.25:9999/"] {
			t.Fatalf("R6-P1-11: expected Register() after /preStop to be a no-op")
		}
		mu.Unlock()
	})

	t.Run("TargetHTTPForwardingKillTimeoutAndPathTraversalGuard", func(t *testing.T) {
		var killCalled atomic.Bool
		var gotSharedSecret atomic.Value

		bizMux := http.NewServeMux()
		bizMux.HandleFunc("/job/fastHandler", func(w http.ResponseWriter, r *http.Request) {
			gotSharedSecret.Store(r.Header.Get(SidecarSecretHeader))
			_, _ = w.Write([]byte("fast ok"))
		})
		bizMux.HandleFunc("/job/slowKillHandler", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			_, _ = w.Write([]byte("terminated"))
		})
		bizMux.HandleFunc("/job/slowKillHandler/kill", func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(SidecarSecretHeader) == "my-shared-secret" {
				killCalled.Store(true)
			}
			w.WriteHeader(http.StatusOK)
		})
		bizMux.HandleFunc("/job/timeoutHandler", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
		})

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen 127.0.0.1:0 failed: %v", err)
		}
		bizSrv := &http.Server{Handler: bizMux}
		go func() { _ = bizSrv.Serve(ln) }()
		defer bizSrv.Close()
		_, portStr, _ := net.SplitHostPort(ln.Addr().String())
		targetPort, _ := strconv.Atoi(portStr)

		httpAgent, err := NewAgent(Config{
			AdminURL:          adminSrv.URL,
			AccessToken:       "secret-sidecar-token",
			SharedSecret:      "my-shared-secret",
			AppName:           "http-worker",
			PodIP:             "fd00::25", // IPv6 Pod IP (tests R6-P2-20)
			ExecutorPort:      9999,
			TargetHTTPPort:    targetPort,
			CallbackDir:       t.TempDir(),
			CallbackRetryWait: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent failed: %v", err)
		}
		if httpAgent.ExecutorAddress() != "http://[fd00::25]:9999/" {
			t.Fatalf("R6-P2-20: expected bracketed IPv6 executor URL, got %s", httpAgent.ExecutorAddress())
		}

		httpSidecarSrv := httptest.NewServer(httpAgent.Handler())
		defer httpSidecarSrv.Close()

		postSidecar := func(path string, payload any) xxlclient.ReturnT {
			b, _ := json.Marshal(payload)
			req, _ := http.NewRequest(http.MethodPost, httpSidecarSrv.URL+path, bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "secret-sidecar-token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST %s failed: %v", path, err)
			}
			defer resp.Body.Close()
			var ret xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&ret)
			return ret
		}

		// 1. Reject path traversal in ExecutorHandler (R6-P1-14)
		if ret := postSidecar("/run", xxlclient.TriggerParam{JobID: 81, LogID: 601, ExecutorHandler: "../admin/shutdown"}); ret.IsSuccess() {
			t.Fatalf("R6-P1-14: expected path traversal ExecutorHandler to be rejected")
		}

		// 2. Verify X-XXL-JOB-SIDECAR-SECRET header is sent to localhost business container (R6-P1-14)
		if ret := postSidecar("/run", xxlclient.TriggerParam{JobID: 81, LogID: 602, ExecutorHandler: "fastHandler"}); !ret.IsSuccess() {
			t.Fatalf("expected fastHandler trigger to succeed, got %+v", ret)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if v, _ := gotSharedSecret.Load().(string); v == "my-shared-secret" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if v, _ := gotSharedSecret.Load().(string); v != "my-shared-secret" {
			t.Fatalf("R6-P1-14: expected %s header to be forwarded, got %q", SidecarSecretHeader, v)
		}

		// 3. Verify /kill invokes cooperative POST /job/<handler>/kill on business container (R6-P1-11)
		_ = postSidecar("/run", xxlclient.TriggerParam{JobID: 82, LogID: 603, ExecutorHandler: "slowKillHandler"})
		time.Sleep(30 * time.Millisecond)
		_ = postSidecar("/kill", xxlclient.KillParam{JobID: 82})
		if !killCalled.Load() {
			t.Fatalf("R6-P1-11: expected /kill to invoke cooperative /job/slowKillHandler/kill on localhost business container")
		}

		// 4. Verify timeout returns HandleCode=502 (TimeoutCode, R6-P2-5)
		mu.Lock()
		callbacks = nil
		mu.Unlock()
		_ = postSidecar("/run", xxlclient.TriggerParam{
			JobID:           83,
			LogID:           604,
			ExecutorHandler: "timeoutHandler",
			ExecutorTimeout: 1,
		})
		deadline = time.Now().Add(2500 * time.Millisecond)
		foundTimeout502 := false
		for time.Now().Before(deadline) {
			mu.Lock()
			for _, cb := range callbacks {
				if cb.LogID == 604 && cb.HandleCode == xxlclient.TimeoutCode {
					foundTimeout502 = true
				}
			}
			mu.Unlock()
			if foundTimeout502 {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if !foundTimeout502 {
			t.Fatalf("R6-P2-5: expected timed-out sidecar execution to report HandleCode=502")
		}
	})

	t.Run("TotalLogMemoryBudgetCapAndSpoolTerminalCleanup", func(t *testing.T) {
		var repeatCallbackCount atomic.Int32
		termAdminMux := http.NewServeMux()
		termAdminMux.HandleFunc("/api/callback", func(w http.ResponseWriter, _ *http.Request) {
			repeatCallbackCount.Add(1)
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 500, Msg: "repeate callback"})
		})
		termAdminSrv := httptest.NewServer(termAdminMux)
		defer termAdminSrv.Close()

		spoolDir := t.TempDir()
		memAgent, err := NewAgent(Config{
			AdminURL:            termAdminSrv.URL,
			AccessToken:         "token-v2",
			PreviousAccessToken: "token-v1",
			AppName:             "mem-worker",
			PodIP:               "10.244.1.99",
			ExecutorPort:        9999,
			MaxLogEntries:       200,
			CallbackDir:         spoolDir,
			CallbackRetryWait:   5 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent failed: %v", err)
		}
		// Verify PreviousAccessToken is accepted during grace window (R7-P1-6)
		if !memAgent.isValidAccessToken("token-v1") || !memAgent.isValidAccessToken("token-v2") {
			t.Fatalf("R7-P1-6: expected both AccessToken and PreviousAccessToken to be accepted")
		}

		// Store 100 logs of 256 KiB each (25 MiB total if uncapped) -> must stay <= MaxTotalLogBytes (16 MiB, R7-P1-2)
		chunk := strings.Repeat("A", MaxSingleLogBytes)
		for i := int64(1); i <= 100; i++ {
			memAgent.storeLog(i, chunk)
		}
		memAgent.mu.Lock()
		totalBytes := memAgent.totalLogBytes
		retainedCount := len(memAgent.logs)
		memAgent.mu.Unlock()
		if totalBytes > MaxTotalLogBytes || retainedCount > 64 {
			t.Fatalf("R7-P1-2 regression: expected totalLogBytes <= %d (64 entries), got %d bytes (%d entries)",
				MaxTotalLogBytes, totalBytes, retainedCount)
		}

		// Verify deterministic spool filename cb-<logId>.json and terminal "repeate callback" cleanup (R7-P2-1 & R7-P3-2)
		memAgent.deliverCallbackWithRetry([]xxlclient.HandleCallbackParam{
			{LogID: 77701, HandleCode: 200, HandleMsg: "ok"},
		})
		if repeatCallbackCount.Load() != 1 {
			t.Fatalf("R7-P2-1: expected terminal 'repeate callback' rejection to stop on attempt 1 without 8 retries, got %d attempts", repeatCallbackCount.Load())
		}
		entries, _ := os.ReadDir(spoolDir)
		if len(entries) != 0 {
			t.Fatalf("R7-P2-1: expected spool file to be deleted after terminal 'repeate callback' response, got %d files", len(entries))
		}
	})

	t.Run("SidecarConcurrentCallbackSpoolAndIdleBeatDraining", func(t *testing.T) {
		var callbackHits atomic.Int32
		slowAdminMux := http.NewServeMux()
		slowAdminMux.HandleFunc("/api/callback", func(w http.ResponseWriter, _ *http.Request) {
			hits := callbackHits.Add(1)
			time.Sleep(40 * time.Millisecond)
			if hits == 1 {
				// Simulate transient error on first attempt so deliverCallbackWithRetry stays in flight while replaySpooledCallbacks runs
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		slowAdminSrv := httptest.NewServer(slowAdminMux)
		defer slowAdminSrv.Close()

		concSpoolDir := t.TempDir()
		concAgent, err := NewAgent(Config{
			AdminURL:          slowAdminSrv.URL,
			AccessToken:       "token-v2",
			AppName:           "conc-worker",
			PodIP:             "10.244.1.100",
			ExecutorPort:      9999,
			MaxLogEntries:     2,
			CallbackDir:       concSpoolDir,
			CallbackRetryWait: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			concAgent.deliverCallbackWithRetry([]xxlclient.HandleCallbackParam{
				{LogID: 88801, HandleCode: 200, HandleMsg: "ok"},
			})
		}()
		go func() {
			defer wg.Done()
			time.Sleep(15 * time.Millisecond)
			// Calling replaySpooledCallbacks while logID=88801 is already in flight must skip 88801 instead of racing (R8-P1-9)
			concAgent.replaySpooledCallbacks()
		}()
		wg.Wait()

		if callbackHits.Load() != 2 {
			t.Fatalf("R8-P1-9 regression: expected exactly 2 callback attempts (1 transient fail + 1 retry) without duplicate concurrent replay, got %d", callbackHits.Load())
		}

		// Verify /idleBeat returns 500 when draining is active (R8-P2-2)
		concAgent.StartDrain()
		concTS := httptest.NewServer(concAgent.Handler())
		defer concTS.Close()
		ibBody, _ := json.Marshal(xxlclient.IdleBeatParam{JobID: 1})
		ibReq, _ := http.NewRequest(http.MethodPost, concTS.URL+"/idleBeat", bytes.NewReader(ibBody))
		ibReq.Header.Set("Content-Type", "application/json")
		ibReq.Header.Set(xxlclient.AccessTokenHeader, "token-v2")
		ibResp, err := http.DefaultClient.Do(ibReq)
		if err != nil {
			t.Fatalf("idleBeat request failed: %v", err)
		}
		var ibRet xxlclient.ReturnT
		_ = json.NewDecoder(ibResp.Body).Decode(&ibRet)
		ibResp.Body.Close()
		if ibRet.IsSuccess() {
			t.Fatalf("R8-P2-2 regression: expected /idleBeat to return 500 when sidecar agent is draining")
		}

		// R9-P1-4: Verify FlushSpooledCallbacks() blocks if a background replay holds replayMu, ensuring newly spooled callbacks are flushed before exit
		concAgent.replayMu.Lock()
		newSpoolPath := concAgent.spoolCallbackFile([]xxlclient.HandleCallbackParam{
			{LogID: 88802, HandleCode: 200, HandleMsg: "flushed-after-lock"},
		})
		if newSpoolPath == "" {
			concAgent.replayMu.Unlock()
			t.Fatalf("failed to create spool file for 88802")
		}
		go func() {
			time.Sleep(30 * time.Millisecond)
			concAgent.replayMu.Unlock()
		}()
		concAgent.FlushSpooledCallbacks()
		entriesAfterFlush, _ := os.ReadDir(concSpoolDir)
		if len(entriesAfterFlush) != 0 {
			t.Fatalf("R9-P1-4 regression: expected FlushSpooledCallbacks() to block until replayMu was released and flush cb-88802.json, remaining files=%d", len(entriesAfterFlush))
		}
	})

	t.Run("Round10ReleaseRunningBeforeCallbackBackoffAndExecutingKillGuard", func(t *testing.T) {
		var cbAttempts atomic.Int32
		var deregisterCalls atomic.Int32
		cbGate := make(chan struct{})
		r10AdminMux := http.NewServeMux()
		r10AdminMux.HandleFunc("/api/callback", func(w http.ResponseWriter, _ *http.Request) {
			if cbAttempts.Add(1) == 1 {
				// Block first callback attempt until test inspects /idleBeat and DISCARD_LATER
				<-cbGate
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		r10AdminMux.HandleFunc("/api/registryRemove", func(w http.ResponseWriter, _ *http.Request) {
			deregisterCalls.Add(1)
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		})
		r10AdminSrv := httptest.NewServer(r10AdminMux)
		defer r10AdminSrv.Close()

		var killRequests atomic.Int32
		var lastKillLogID atomic.Int64
		blockRun := make(chan struct{})
		targetMux := http.NewServeMux()
		targetMux.HandleFunc("/job/fastHandler", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("fast done"))
		})
		targetMux.HandleFunc("/job/serialKillHandler", func(w http.ResponseWriter, _ *http.Request) {
			<-blockRun
			_, _ = w.Write([]byte("serial done"))
		})
		targetMux.HandleFunc("/job/serialKillHandler/kill", func(w http.ResponseWriter, r *http.Request) {
			killRequests.Add(1)
			var body struct {
				LogID int64 `json:"logId"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			lastKillLogID.Store(body.LogID)
			w.WriteHeader(http.StatusOK)
		})
		targetSrv := httptest.NewServer(targetMux)
		defer targetSrv.Close()
		_, portStr, _ := net.SplitHostPort(targetSrv.Listener.Addr().String())
		targetPort, _ := strconv.Atoi(portStr)

		r10Agent, err := NewAgent(Config{
			AdminURL:              r10AdminSrv.URL,
			AccessToken:           "token-r10",
			AppName:               "r10-worker",
			PodIP:                 "10.244.1.110",
			ExecutorPort:          9999,
			TargetHTTPPort:        targetPort,
			CallbackDir:           t.TempDir(),
			CallbackRetryWait:     10 * time.Millisecond,
			DeregisterBeforeDrain: true,
		})
		if err != nil {
			t.Fatalf("NewAgent failed: %v", err)
		}
		r10TS := httptest.NewServer(r10Agent.Handler())
		defer r10TS.Close()

		postR10 := func(path string, body any) xxlclient.ReturnT {
			raw, _ := json.Marshal(body)
			req, _ := http.NewRequest(http.MethodPost, r10TS.URL+path, bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "token-r10")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST %s failed: %v", path, err)
			}
			defer resp.Body.Close()
			var ret xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&ret)
			return ret
		}

		// 1. Trigger fastHandler (JobID=701, LogID=90001): execution completes immediately, then callback blocks on cbGate
		ret1 := postR10("/run", xxlclient.TriggerParam{
			JobID:                 701,
			ExecutorHandler:       "fastHandler",
			ExecutorBlockStrategy: "DISCARD_LATER",
			LogID:                 90001,
		})
		if !ret1.IsSuccess() {
			t.Fatalf("trigger 90001 failed: %+v", ret1)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && cbAttempts.Load() == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		if cbAttempts.Load() == 0 {
			t.Fatalf("expected callback attempt 1 to be in flight")
		}

		// R10-P1-4 (a): Even while callback 90001 is blocked in flight, JobID 701 must already be removed from a.running
		// so /idleBeat returns 200!
		ibRet := postR10("/idleBeat", xxlclient.IdleBeatParam{JobID: 701})
		if !ibRet.IsSuccess() {
			t.Fatalf("R10-P1-4 regression: expected /idleBeat to return 200 once job execution finished even while callback is retrying, got %+v", ibRet)
		}
		close(cbGate)
		waitCtx1, cancelWait1 := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelWait1()
		if !r10Agent.WaitForRunningJobs(waitCtx1) {
			t.Fatalf("expected WaitForRunningJobs to complete after callback retry")
		}

		// 2. R10-P1-4 (b): Queue 2 SERIAL_EXECUTION triggers (90002 executing, 90003 queued in serialCh), then /kill JobID=702.
		// Only 90002 (executing=true) should trigger /job/serialKillHandler/kill, NOT 90003.
		_ = postR10("/run", xxlclient.TriggerParam{
			JobID:                 702,
			ExecutorHandler:       "serialKillHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 90002,
		})
		// Wait until 90002 has set executing=true
		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			r10Agent.mu.Lock()
			e := r10Agent.running[702][90002]
			r10Agent.mu.Unlock()
			if e != nil && e.executing.Load() {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		_ = postR10("/run", xxlclient.TriggerParam{
			JobID:                 702,
			ExecutorHandler:       "serialKillHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 90003,
		})
		killRet := postR10("/kill", xxlclient.KillParam{JobID: 702})
		if !killRet.IsSuccess() {
			t.Fatalf("/kill 702 failed: %+v", killRet)
		}
		close(blockRun)
		waitCtx2, cancelWait2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelWait2()
		_ = r10Agent.WaitForRunningJobs(waitCtx2)

		if killRequests.Load() != 1 || lastKillLogID.Load() != 90002 {
			t.Fatalf("R10-P1-4 regression: expected exactly 1 target /kill notification for executing logID=90002 (not queued 90003), got count=%d logID=%v",
				killRequests.Load(), lastKillLogID.Load())
		}

		// 3. R10-P2-2: Verify /preStop with DeregisterBeforeDrain=true calls /api/registryRemove before and after drain
		preStopRet := postR10("/preStop", nil)
		if !preStopRet.IsSuccess() {
			t.Fatalf("preStop failed: %+v", preStopRet)
		}
		if deregisterCalls.Load() < 2 {
			t.Fatalf("R10-P2-2 regression: expected handlePreStop with DeregisterBeforeDrain=true to call /api/registryRemove both before and after drain (>=2 calls), got %d",
				deregisterCalls.Load())
		}
	})

	// 12. Test R11-P2-3: serialQueueWorker is not blocked by callback retry backoff when attempt #1 fails
	t.Run("Round11SerialQueueWorkerNotBlockedByCallbackRetryBackoff", func(t *testing.T) {
		var item2ExecDone atomic.Int64
		bizR11 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("r11 ok"))
			item2ExecDone.Add(1)
		}))
		defer bizR11.Close()
		var bizPortR11 int
		_, _ = fmt.Sscanf(bizR11.Listener.Addr().String(), "127.0.0.1:%d", &bizPortR11)

		var cbAttempts91001 atomic.Int32
		adminR11 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/callback" {
				var cbs []xxlclient.HandleCallbackParam
				_ = json.NewDecoder(r.Body).Decode(&cbs)
				if len(cbs) > 0 && cbs[0].LogID == 91001 {
					if cbAttempts91001.Add(1) == 1 {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
		}))
		defer adminR11.Close()

		r11Agent, err := NewAgent(Config{
			AppName:           "r11-sidecar-app",
			PodIP:             "127.0.0.1",
			ExecutorPort:      19996,
			TargetHTTPPort:    bizPortR11,
			TargetPathPrefix:  "/xxl-job",
			AdminURL:          adminR11.URL,
			AccessToken:       "r11-token",
			CallbackRetryWait: 400 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent R11 failed: %v", err)
		}
		r11Srv := httptest.NewServer(r11Agent.Handler())
		defer r11Srv.Close()

		postR11 := func(param xxlclient.TriggerParam) xxlclient.ReturnT {
			b, _ := json.Marshal(param)
			req, _ := http.NewRequest(http.MethodPost, r11Srv.URL+"/run", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "r11-token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("post /run failed: %v", err)
			}
			defer resp.Body.Close()
			var out xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&out)
			return out
		}

		start := time.Now()
		_ = postR11(xxlclient.TriggerParam{
			JobID:                 711,
			ExecutorHandler:       "r11SerialHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 91001,
		})
		_ = postR11(xxlclient.TriggerParam{
			JobID:                 711,
			ExecutorHandler:       "r11SerialHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 91002,
		})

		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) && item2ExecDone.Load() < 2 {
			time.Sleep(5 * time.Millisecond)
		}
		if item2ExecDone.Load() < 2 {
			t.Fatalf("R11-P2-3 regression: expected second SERIAL_EXECUTION item (91002) to execute without waiting for 91001's 400ms callback retry backoff (elapsed=%v, count=%d)",
				time.Since(start), item2ExecDone.Load())
		}

		waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelWait()
		if !r11Agent.WaitForRunningJobs(waitCtx) {
			t.Fatalf("R11-P2-3: expected WaitForRunningJobs to wait until background callback retry completed")
		}
		if !r11Agent.isValidAccessToken("  r11-token\n") {
			t.Fatalf("R12-P2-2: expected isValidAccessToken to accept token with surrounding whitespace/newline")
		}

		// Verify CallbackDir spool files are bounded by maxCallbackSpoolFiles (512):
		spoolBoundDir := t.TempDir()
		spoolAgent, err := NewAgent(Config{
			AppName:        "spool-bound-app",
			PodIP:          "127.0.0.1",
			ExecutorPort:   19997,
			TargetHTTPPort: bizPortR11,
			AdminURL:       adminR11.URL,
			AccessToken:    "r11-token",
			CallbackDir:    spoolBoundDir,
		})
		if err != nil {
			t.Fatalf("NewAgent spoolAgent failed: %v", err)
		}
		for i := 1; i <= maxCallbackSpoolFiles+10; i++ {
			_ = spoolAgent.spoolCallbackFile([]xxlclient.HandleCallbackParam{{LogID: int64(i), HandleCode: 200}})
		}
		entries, err := os.ReadDir(spoolBoundDir)
		if err != nil {
			t.Fatalf("ReadDir spoolBoundDir failed: %v", err)
		}
		if len(entries) > maxCallbackSpoolFiles {
			t.Fatalf("expected CallbackDir spool files to be bounded to <=%d, got %d", maxCallbackSpoolFiles, len(entries))
		}
		if _, err := os.Stat(filepath.Join(spoolBoundDir, "cb-1.json")); !os.IsNotExist(err) {
			t.Fatalf("expected oldest spooled file cb-1.json to be pruned once CallbackDir exceeded %d files", maxCallbackSpoolFiles)
		}
		// Mark cb-11.json (now the oldest remaining file) as in-flight and spool one more file:
		// cb-11.json must be preserved while cb-12.json is evicted so total count stays <= maxCallbackSpoolFiles.
		done11, ok11 := spoolAgent.tryBeginCallbackDelivery(11)
		if !ok11 {
			t.Fatalf("expected tryBeginCallbackDelivery(11) to succeed")
		}
		_ = spoolAgent.spoolCallbackFile([]xxlclient.HandleCallbackParam{{LogID: int64(maxCallbackSpoolFiles + 11), HandleCode: 200}})
		done11()
		entriesAfterInFlight, err := os.ReadDir(spoolBoundDir)
		if err != nil {
			t.Fatalf("ReadDir spoolBoundDir after inFlight failed: %v", err)
		}
		if len(entriesAfterInFlight) > maxCallbackSpoolFiles {
			t.Fatalf("expected CallbackDir spool files to remain <=%d when oldest file is in-flight, got %d", maxCallbackSpoolFiles, len(entriesAfterInFlight))
		}
		if _, err := os.Stat(filepath.Join(spoolBoundDir, "cb-11.json")); err != nil {
			t.Fatalf("expected in-flight cb-11.json to be preserved during pruning, got err=%v", err)
		}
		if _, err := os.Stat(filepath.Join(spoolBoundDir, "cb-12.json")); !os.IsNotExist(err) {
			t.Fatalf("expected next non-in-flight oldest file cb-12.json to be evicted when cb-11.json was in-flight")
		}
	})

	// 11. Test Endgame Round 2 Fixes (EG2-13):
	// - GLUE_SHELL process group kill on timeout terminates background child holding stdout open without hanging
	// - On-demand serialQueues/serialWorkerActive leave zero leaked entries after completion, and COVER_EARLY -> SERIAL_EXECUTION never overlaps
	// - CheckRedirect blocks 302 redirect SSRF and secret exfiltration
	// - Bounded /preStop still calls /api/registryRemove even when Admin /api/callback hangs
	t.Run("Endgame2ProcessGroupSerialCleanupRedirectAndBoundedPreStop", func(t *testing.T) {
		var cbMu sync.Mutex
		var gotCBs []xxlclient.HandleCallbackParam
		var deregistered atomic.Bool
		var hangCallbacks atomic.Bool
		unblockHangCh := make(chan struct{})

		eg2Admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/callback":
				if hangCallbacks.Load() {
					select {
					case <-r.Context().Done():
						return
					case <-unblockHangCh:
						return
					}
				}
				var p []xxlclient.HandleCallbackParam
				_ = json.NewDecoder(r.Body).Decode(&p)
				cbMu.Lock()
				gotCBs = append(gotCBs, p...)
				cbMu.Unlock()
				_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
			case "/api/registryRemove":
				deregistered.Store(true)
				_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
			default:
				_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{Code: 200})
			}
		}))
		defer func() {
			close(unblockHangCh)
			eg2Admin.CloseClientConnections()
			eg2Admin.Close()
		}()

		var externalRedirectHit atomic.Bool
		externalAttacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			externalRedirectHit.Store(true)
			w.WriteHeader(http.StatusOK)
		}))
		defer externalAttacker.Close()

		var activeBiz int32
		var maxConcurrentBiz int32
		bizListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen 127.0.0.1:0 failed: %v", err)
		}
		bizPort := bizListener.Addr().(*net.TCPAddr).Port
		bizSrv := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/redirectHandler") {
					http.Redirect(w, r, externalAttacker.URL+"/steal", http.StatusFound)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/kill") {
					w.WriteHeader(http.StatusOK)
					return
				}
				cur := atomic.AddInt32(&activeBiz, 1)
				for {
					prevMax := atomic.LoadInt32(&maxConcurrentBiz)
					if cur <= prevMax || atomic.CompareAndSwapInt32(&maxConcurrentBiz, prevMax, cur) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt32(&activeBiz, -1)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			}),
		}
		go func() { _ = bizSrv.Serve(bizListener) }()
		defer bizSrv.Close()

		eg2SpoolDir := t.TempDir()
		eg2Agent, err := NewAgent(Config{
			AppName:           "eg2-sidecar-app",
			PodIP:             "127.0.0.1",
			ExecutorPort:      19996,
			TargetHTTPPort:    bizPort,
			TargetPathPrefix:  "/job",
			AdminURL:          eg2Admin.URL,
			AccessToken:       "eg2-token",
			SharedSecret:      "eg2-local-secret",
			EnableGlue:        true,
			CallbackDir:       eg2SpoolDir,
			DrainTimeout:      150 * time.Millisecond,
			CallbackRetryWait: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewAgent eg2Agent failed: %v", err)
		}
		eg2Srv := httptest.NewServer(eg2Agent.Handler())
		defer eg2Srv.Close()

		postEG2 := func(param xxlclient.TriggerParam) xxlclient.ReturnT {
			b, _ := json.Marshal(param)
			req, _ := http.NewRequest(http.MethodPost, eg2Srv.URL+"/run", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(xxlclient.AccessTokenHeader, "eg2-token")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("postEG2 /run failed: %v", err)
			}
			defer resp.Body.Close()
			var out xxlclient.ReturnT
			_ = json.NewDecoder(resp.Body).Decode(&out)
			return out
		}

		// (a) GLUE_SHELL background child holding stdout pipe open must be killed on 1s timeout without hanging for 30s
		t0 := time.Now()
		_ = postEG2(xxlclient.TriggerParam{
			JobID:           951,
			GlueType:        "GLUE_SHELL",
			GlueSource:      "(sleep 30) & echo started; wait",
			ExecutorTimeout: 1,
			LogID:           95001,
		})
		waitGlueCtx, cancelGlue := context.WithTimeout(context.Background(), 4*time.Second)
		drainedGlue := eg2Agent.WaitForRunningJobs(waitGlueCtx)
		cancelGlue()
		if !drainedGlue {
			t.Fatalf("EG2-13 regression: GLUE_SHELL child process held stdout open past timeout (elapsed=%v)", time.Since(t0))
		}

		// (b) COVER_EARLY followed immediately by SERIAL_EXECUTION on same jobID=952 must NOT overlap,
		// and serialQueues/serialWorkerActive must shrink back to 0 once empty!
		_ = postEG2(xxlclient.TriggerParam{
			JobID:                 952,
			ExecutorHandler:       "overlapHandler",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 95002,
		})
		time.Sleep(15 * time.Millisecond)
		_ = postEG2(xxlclient.TriggerParam{
			JobID:                 952,
			ExecutorHandler:       "overlapHandler",
			ExecutorBlockStrategy: "SERIAL_EXECUTION",
			LogID:                 95003,
		})
		wait952, cancel952 := context.WithTimeout(context.Background(), 3*time.Second)
		if !eg2Agent.WaitForRunningJobs(wait952) {
			t.Fatalf("WaitForRunningJobs 952 timed out")
		}
		cancel952()
		if atomic.LoadInt32(&maxConcurrentBiz) > 1 {
			t.Fatalf("EG2-13 regression: SERIAL_EXECUTION overlapped in-flight COVER_EARLY handler (maxConcurrent=%d)", maxConcurrentBiz)
		}
		deadlineClean := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadlineClean) {
			eg2Agent.mu.Lock()
			qLen := len(eg2Agent.serialQueues)
			wLen := len(eg2Agent.serialWorkerActive)
			eg2Agent.mu.Unlock()
			if qLen == 0 && wLen == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		eg2Agent.mu.Lock()
		qLen := len(eg2Agent.serialQueues)
		wLen := len(eg2Agent.serialWorkerActive)
		eg2Agent.mu.Unlock()
		if qLen != 0 || wLen != 0 {
			t.Fatalf("EG2-13 regression: leaked serialQueues (%d) or serialWorkerActive (%d) after queue drained", qLen, wLen)
		}

		// (c) Redirect SSRF blocked and 302 treated as failure
		_ = postEG2(xxlclient.TriggerParam{
			JobID:           953,
			ExecutorHandler: "redirectHandler",
			LogID:           95004,
		})
		wait953, cancel953 := context.WithTimeout(context.Background(), 2*time.Second)
		_ = eg2Agent.WaitForRunningJobs(wait953)
		cancel953()
		if externalRedirectHit.Load() {
			t.Fatalf("EG2-13 regression: Sidecar httpClient followed 302 redirect to external host!")
		}
		cbMu.Lock()
		found95004Fail := false
		for _, cb := range gotCBs {
			if cb.LogID == 95004 && cb.HandleCode == xxlclient.FailCode {
				found95004Fail = true
			}
		}
		cbMu.Unlock()
		if !found95004Fail {
			t.Fatalf("EG2-13 regression: expected 302 redirect response to report FailCode callback")
		}

		// (d) Bounded /preStop still invokes /api/registryRemove even when Admin /api/callback hangs
		hangCallbacks.Store(true)
		_ = eg2Agent.spoolCallbackFile([]xxlclient.HandleCallbackParam{{LogID: 95999, HandleCode: 200}})
		preStopReq, _ := http.NewRequest(http.MethodPost, eg2Srv.URL+"/preStop", nil)
		preStopReq.Header.Set(xxlclient.AccessTokenHeader, "eg2-token")
		preStopStart := time.Now()
		preStopResp, err := http.DefaultClient.Do(preStopReq)
		if err != nil {
			t.Fatalf("/preStop request failed: %v", err)
		}
		preStopResp.Body.Close()
		if !deregistered.Load() {
			t.Fatalf("EG2-13 regression: /preStop failed to call Deregister when callback flush timed out")
		}
		if time.Since(preStopStart) > 12*time.Second {
			t.Fatalf("EG2-13 regression: /preStop was not bounded cleanly when /api/callback hung (elapsed=%v)", time.Since(preStopStart))
		}
	})
}

func TestBoostEndgameSidecarCoverEarlyOutOfOrderAndTokenCRLF(t *testing.T) {
	block500 := make(chan struct{})
	var executedLogIDs []int64
	var execMu sync.Mutex

	bizListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen biz failed: %v", err)
	}
	bizPort := bizListener.Addr().(*net.TCPAddr).Port
	bizSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/kill") {
				w.WriteHeader(http.StatusOK)
				return
			}
			var payload struct {
				JobID int   `json:"jobId"`
				LogID int64 `json:"logId"`
			}
			_ = json.NewDecoder(r.Body).Decode(&payload)
			execMu.Lock()
			executedLogIDs = append(executedLogIDs, payload.LogID)
			execMu.Unlock()
			if payload.LogID == 500 {
				select {
				case <-block500:
				case <-r.Context().Done():
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fmt.Sprintf("ok-%d\n", payload.LogID)))
		}),
	}
	go func() { _ = bizSrv.Serve(bizListener) }()
	defer bizSrv.Close()

	adminSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer adminSrv.Close()

	ag, err := NewAgent(Config{
		AdminURL:            adminSrv.URL,
		AccessToken:         "  boost-sidecar-token\r\n ",
		PreviousAccessToken: "\tboost-prev-token\r\n",
		AppName:             "boost-sidecar",
		PodIP:               "127.0.0.1",
		ExecutorPort:        19999,
		TargetHTTPPort:      bizPort,
		MaxLogEntries:       3,
	})
	if err != nil {
		t.Fatalf("NewAgent failed: %v", err)
	}
	srv := httptest.NewServer(ag.Handler())
	defer srv.Close()

	sendRun := func(tok string, p xxlclient.TriggerParam) xxlclient.ReturnT {
		raw, _ := json.Marshal(p)
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(xxlclient.AccessTokenHeader, tok)
		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			t.Fatalf("POST /run failed: %v", doErr)
		}
		defer resp.Body.Close()
		var ret xxlclient.ReturnT
		_ = json.NewDecoder(resp.Body).Decode(&ret)
		return ret
	}

	// 1. Trigger COVER_EARLY logId=500 with whitespace-padded header token (against CRLF-sanitized server config token)
	if ret := sendRun("  \tboost-sidecar-token  ", xxlclient.TriggerParam{
		JobID:                 1,
		ExecutorHandler:       "work",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 500,
	}); ret.Code != xxlclient.SuccessCode {
		t.Fatalf("expected logId=500 to succeed, got %+v", ret)
	}

	// Wait until logId=500 is actively executing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		execMu.Lock()
		n := len(executedLogIDs)
		execMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 2. Delayed older COVER_EARLY logId=499 while 500 is active must be rejected without killing 500
	ret499 := sendRun("\tboost-prev-token  ", xxlclient.TriggerParam{
		JobID:                 1,
		ExecutorHandler:       "work",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 499,
	})
	if ret499.Code != xxlclient.FailCode || !strings.Contains(ret499.Msg, "active logId=500") {
		t.Fatalf("expected stale active COVER_EARLY logId=499 rejection, got %+v", ret499)
	}

	// Allow 500 to finish cleanly
	close(block500)
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if !ag.WaitForRunningJobs(waitCtx) {
		t.Fatalf("timed out waiting for logId=500 to finish")
	}
	cancel()

	// 3. Delayed older COVER_EARLY logId=498 arriving AFTER 500 completed must ALSO be rejected via logJobID
	ret498 := sendRun("boost-sidecar-token", xxlclient.TriggerParam{
		JobID:                 1,
		ExecutorHandler:       "work",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 498,
	})
	if ret498.Code != xxlclient.FailCode || !strings.Contains(ret498.Msg, "completed logId=500") {
		t.Fatalf("expected stale completed COVER_EARLY logId=498 rejection, got %+v", ret498)
	}

	// 4. Different jobId=2 with smaller logId=10 must NOT be blocked by jobId=1's logId=500
	if retJob2 := sendRun("boost-sidecar-token", xxlclient.TriggerParam{
		JobID:                 2,
		ExecutorHandler:       "work",
		ExecutorBlockStrategy: "COVER_EARLY",
		LogID:                 10,
	}); retJob2.Code != xxlclient.SuccessCode {
		t.Fatalf("expected jobId=2 logId=10 to succeed independently, got %+v", retJob2)
	}

	// 5. Newer COVER_EARLY logIds (501, 502, 503) on jobId=1 succeed and bound logJobID/logs to MaxLogEntries=3
	for _, id := range []int64{501, 502, 503} {
		if ret := sendRun("boost-sidecar-token", xxlclient.TriggerParam{
			JobID:                 1,
			ExecutorHandler:       "work",
			ExecutorBlockStrategy: "COVER_EARLY",
			LogID:                 id,
		}); ret.Code != xxlclient.SuccessCode {
			t.Fatalf("expected newer logId=%d to succeed, got %+v", id, ret)
		}
		wCtx, wCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = ag.WaitForRunningJobs(wCtx)
		wCancel()
	}

	ag.mu.Lock()
	logsLen := len(ag.logs)
	logJobIDLen := len(ag.logJobID)
	seenLen := len(ag.seenLogs)
	ag.mu.Unlock()
	if logsLen > 3 || logJobIDLen > 3 || seenLen > 3 {
		t.Fatalf("expected logs/logJobID/seenLogs bounded by MaxLogEntries=3, got logs=%d logJobID=%d seen=%d", logsLen, logJobIDLen, seenLen)
	}
}
