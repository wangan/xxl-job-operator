package sidecar

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

const (
	DefaultMaxLogEntries = 200
	DefaultMaxQueueSize  = 100
	MaxSingleLogBytes    = 256 * 1024       // 256 KiB per job execution log in memory (fixes R7-P1-2)
	MaxTotalLogBytes     = 16 * 1024 * 1024 // 16 MiB total memory budget across all retained job logs (fixes R7-P1-2)
	MaxRequestBodyBytes  = 1 * 1024 * 1024  // 1 MiB cap for incoming trigger payloads

	// SidecarSecretHeader is sent to 127.0.0.1:<TargetHTTPPort> when SharedSecret is configured (fixes R6-P1-14).
	SidecarSecretHeader = "X-XXL-JOB-SIDECAR-SECRET"
)

// Config configures the lightweight XXL-JOB Sidecar proxy agent injected into non-Java or legacy pods.
type Config struct {
	AdminURL              string // Comma-separated XXL-JOB Admin URLs
	AccessToken           string // Mandatory non-default XXL-JOB-ACCESS-TOKEN (fixes R5-P0-1)
	PreviousAccessToken   string // Optional previous token accepted during Secret rotation grace window (fixes R7-P1-6)
	SharedSecret          string // Optional shared secret sent in X-XXL-JOB-SIDECAR-SECRET to localhost business container (fixes R6-P1-14)
	AppName               string
	PodIP                 string
	ExecutorPort          int
	TargetHTTPPort        int           // Localhost business container port (e.g., 8080)
	TargetPathPrefix      string        // Optional path prefix when forwarding to localhost (e.g., "/job/")
	MaxLogEntries         int           // Maximum number of job execution logs retained in memory (default: 200)
	MaxQueueSize          int           // Maximum queued SERIAL_EXECUTION triggers per jobID (default: 100, fixes R6-P1-11)
	CallbackDir           string        // Optional directory on sidecar-private emptyDir volume to spool pending callbacks (fixes R6-P1-11 & R7-P1-1)
	EnableGlue            bool          // Explicitly opt-in to GLUE script execution in sidecar (default: false, fixes R5-P0-1)
	DeregisterBeforeDrain bool          // Deregister from xxl_job_registry before waiting for active jobs when addressType=0 (fixes R10-P2-2)
	DrainTimeout          time.Duration // PreStop wait timeout for active jobs (default: 15s, fixes R5-P1-4)
	CallbackRetryWait     time.Duration // Initial backoff when retrying failed /api/callback (default: 100ms)
}

type runningEntry struct {
	logID         int64
	explicitLogID bool
	handler       string
	cancel        context.CancelFunc
	isSerial      bool
	executing     atomic.Bool
}

type queuedTrigger struct {
	queueCtx    context.Context
	queueCancel context.CancelFunc
	logID       int64
	param       xxlclient.TriggerParam
}

// Agent runs inside a Pod as a sidecar container:
// 1. Maintains heartbeat registration (/api/registry) with one or more XXL-JOB Admin instances
// 2. Exposes /beat, /idleBeat, /run, /kill, /log (authenticated with XXL-JOB-ACCESS-TOKEN), /healthz, /readyz, and loopback-only /preStop
// 3. Forwards /run triggers to 127.0.0.1:<TargetHTTPPort>/<handler> (or executes GLUE scripts only when EnableGlue=true).
type Agent struct {
	cfg        Config
	adminClis  []*xxlclient.Client
	httpClient *http.Client

	draining    atomic.Bool
	preStopOnce sync.Once
	replayMu    sync.Mutex

	mu                 sync.Mutex
	running            map[int]map[int64]*runningEntry // jobID -> logID -> runningEntry
	serialQueues       map[int][]queuedTrigger         // jobID -> bounded FIFO slice for SERIAL_EXECUTION
	serialWorkerActive map[int]bool                    // jobID -> whether on-demand serial worker is running
	execDoneNotify     chan struct{}                   // closed & replaced whenever a runningEntry finishes
	seenLogs           map[int64]bool                  // logID -> dedupe set
	logJobID           map[int64]int                   // explicit logID -> jobID (co-evicted with logs)
	inFlightCallbacks  map[int64]bool                  // logID -> active callback delivery guard (fixes R8-P1-9)
	pendingCallbacks   int
	logs               map[int64]string
	logOrder           []int64
	totalLogBytes      int
}

// NewAgent initializes a new Sidecar Agent.
func NewAgent(cfg Config) (*Agent, error) {
	token := sanitizeAccessToken(cfg.AccessToken)
	if token == "" || token == "default_token" {
		return nil, fmt.Errorf("sidecar agent requires an explicit non-default XXL-JOB-ACCESS-TOKEN (refusing empty or default_token)")
	}
	cfg.AccessToken = token
	cfg.PreviousAccessToken = sanitizeAccessToken(cfg.PreviousAccessToken)
	if cfg.PreviousAccessToken == "default_token" {
		cfg.PreviousAccessToken = ""
	}
	cfg.SharedSecret = sanitizeAccessToken(cfg.SharedSecret)

	if cfg.ExecutorPort <= 0 {
		cfg.ExecutorPort = 9999
	}
	if cfg.TargetPathPrefix == "" {
		cfg.TargetPathPrefix = "/job/"
	}
	if cfg.MaxLogEntries <= 0 {
		cfg.MaxLogEntries = DefaultMaxLogEntries
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = DefaultMaxQueueSize
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 15 * time.Second
	}
	if cfg.CallbackRetryWait <= 0 {
		cfg.CallbackRetryWait = 100 * time.Millisecond
	}
	if cfg.CallbackDir != "" {
		_ = os.MkdirAll(cfg.CallbackDir, 0o700)
	}

	var clis []*xxlclient.Client
	for _, rawURL := range strings.Split(cfg.AdminURL, ",") {
		trimmed := strings.TrimSpace(rawURL)
		if trimmed == "" {
			continue
		}
		cli, err := xxlclient.NewClient(xxlclient.Config{
			BaseURL:             trimmed,
			AccessToken:         cfg.AccessToken,
			PreviousAccessToken: cfg.PreviousAccessToken,
		})
		if err != nil {
			return nil, err
		}
		clis = append(clis, cli)
	}
	if len(clis) == 0 {
		return nil, fmt.Errorf("at least one valid XXL-JOB Admin URL is required")
	}

	a := &Agent{
		cfg:       cfg,
		adminClis: clis,
		// No fixed 60s client Timeout so tasks >60s are governed solely by param.ExecutorTimeout (fixes R5-P1-4),
		// and disable automatic HTTP redirects so a local handler cannot redirect Sidecar to external/link-local hosts
		// or leak X-XXL-JOB-SIDECAR-SECRET (fixes EG2-13).
		httpClient: &http.Client{
			Timeout: 0,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		running:            make(map[int]map[int64]*runningEntry),
		serialQueues:       make(map[int][]queuedTrigger),
		serialWorkerActive: make(map[int]bool),
		execDoneNotify:     make(chan struct{}),
		seenLogs:           make(map[int64]bool),
		logJobID:           make(map[int64]int),
		inFlightCallbacks:  make(map[int64]bool),
		logs:               make(map[int64]string),
	}
	if cfg.CallbackDir != "" {
		go a.replaySpooledCallbacks()
	}
	return a, nil
}

// StartDrain marks the Sidecar Agent as draining under a.mu so /readyz returns 503, /run atomically rejects
// any concurrent trigger before mutating a.running, and periodic Register() calls stop immediately (fixes R7-P3-1, EG2-13).
func (a *Agent) StartDrain() {
	a.mu.Lock()
	a.draining.Store(true)
	a.mu.Unlock()
}

// StartSpoolRetryLoop periodically replays any spooled callback files left in CallbackDir after transient
// Admin outages until ctx is cancelled (fixes R7-P2-1).
func (a *Agent) StartSpoolRetryLoop(ctx context.Context, interval time.Duration) {
	if a.cfg.CallbackDir == "" {
		return
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.replaySpooledCallbacks()
		}
	}
}

// IsDraining returns true once the Sidecar Agent has entered graceful shutdown (/preStop or SIGTERM).
func (a *Agent) IsDraining() bool {
	return a.draining.Load()
}

// ExecutorAddress returns the HTTP address registered with XXL-JOB Admin, properly bracketing IPv6 addresses (fixes R6-P2-20).
func (a *Agent) ExecutorAddress() string {
	hostPort := net.JoinHostPort(strings.TrimSpace(a.cfg.PodIP), strconv.Itoa(a.cfg.ExecutorPort))
	return fmt.Sprintf("http://%s/", hostPort)
}

// Handler returns the HTTP handler for the Sidecar Executor server.
func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/beat", a.withAuth(a.handleBeat))
	mux.HandleFunc("/idleBeat", a.withAuth(a.handleIdleBeat))
	mux.HandleFunc("/run", a.withAuth(a.handleRun))
	mux.HandleFunc("/kill", a.withAuth(a.handleKill))
	mux.HandleFunc("/log", a.withAuth(a.handleLog))
	mux.HandleFunc("/preStop", a.handlePreStop)
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)
	return mux
}

func (a *Agent) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *Agent) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if a.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func sanitizeAccessToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.ContainsAny(trimmed, "\r\n") {
		return trimmed
	}
	return strings.NewReplacer("\r", "", "\n", "").Replace(trimmed)
}

func (a *Agent) isValidAccessToken(token string) bool {
	token = sanitizeAccessToken(token)
	if token == "" {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.AccessToken)) == 1 {
		return true
	}
	if a.cfg.PreviousAccessToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.PreviousAccessToken)) == 1 {
		return true
	}
	return false
}

func (a *Agent) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "only POST is supported"})
			return
		}
		token := r.Header.Get(xxlclient.AccessTokenHeader)
		if !a.isValidAccessToken(token) {
			writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "invalid XXL-JOB-ACCESS-TOKEN"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)
		next(w, r)
	}
}

func isLoopbackRequest(remoteAddr string) bool {
	if remoteAddr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// Register sends an immediate /api/registry request to all configured XXL-JOB Admin URLs unless the agent is draining (fixes R5-P2-8 & R6-P1-11).
func (a *Agent) Register(ctx context.Context) error {
	if a.draining.Load() {
		return nil
	}
	addr := a.ExecutorAddress()
	var lastErr error
	success := 0
	for _, cli := range a.adminClis {
		if a.draining.Load() {
			return nil
		}
		if err := cli.RegisterExecutor(ctx, a.cfg.AppName, addr); err != nil {
			lastErr = err
		} else {
			success++
		}
	}
	if success == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

// Deregister sends an immediate /api/registryRemove request to all configured XXL-JOB Admin URLs (fixes R5-P2-8).
func (a *Agent) Deregister(ctx context.Context) error {
	a.StartDrain()
	addr := a.ExecutorAddress()
	var lastErr error
	success := 0
	for _, cli := range a.adminClis {
		if err := cli.RemoveExecutor(ctx, a.cfg.AppName, addr); err != nil {
			lastErr = err
		} else {
			success++
		}
	}
	if success == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

// WaitForRunningJobs waits until all in-flight jobs and pending callbacks complete or ctx expires (fixes R5-P1-4).
func (a *Agent) WaitForRunningJobs(ctx context.Context) bool {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.mu.Lock()
		activeJobs := len(a.running)
		pendingCB := a.pendingCallbacks
		a.mu.Unlock()
		if activeJobs == 0 && pendingCB == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (a *Agent) handleBeat(w http.ResponseWriter, _ *http.Request) {
	if a.draining.Load() {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "sidecar agent is draining"})
		return
	}
	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Msg: "sidecar beat ok"})
}

func (a *Agent) handleIdleBeat(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "sidecar agent is draining"})
		return
	}
	var param xxlclient.IdleBeatParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: fmt.Sprintf("invalid idleBeat body: %v", err)})
		return
	}
	a.mu.Lock()
	byLog := a.running[param.JobID]
	busy := len(byLog) > 0
	a.mu.Unlock()
	if busy {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "job is running or queued in sidecar"})
		return
	}
	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode})
}

func (a *Agent) serialQueueWorker(jobID int) {
	for {
		a.mu.Lock()
		q := a.serialQueues[jobID]
		if len(q) == 0 {
			delete(a.serialQueues, jobID)
			delete(a.serialWorkerActive, jobID)
			a.mu.Unlock()
			return
		}
		// Ensure any in-flight non-serial trigger (e.g. an earlier COVER_EARLY execution) for this jobID
		// finishes winding down before starting the next SERIAL_EXECUTION trigger (fixes EG2-13).
		var waitCh chan struct{}
		for _, entry := range a.running[jobID] {
			if !entry.isSerial {
				waitCh = a.execDoneNotify
				break
			}
		}
		if waitCh != nil {
			head := q[0]
			a.mu.Unlock()
			select {
			case <-waitCh:
			case <-head.queueCtx.Done():
				a.mu.Lock()
				if len(a.serialQueues[jobID]) > 0 && a.serialQueues[jobID][0].logID == head.logID {
					item := a.serialQueues[jobID][0]
					a.serialQueues[jobID][0] = queuedTrigger{}
					a.serialQueues[jobID] = a.serialQueues[jobID][1:]
					a.mu.Unlock()
					a.executeAndCallback(item.queueCtx, item.queueCancel, item.logID, item.param)
					continue
				}
				a.mu.Unlock()
			}
			continue
		}

		item := q[0]
		q[0] = queuedTrigger{}
		a.serialQueues[jobID] = q[1:]
		a.mu.Unlock()

		// If the queued trigger was cancelled by /kill or COVER_EARLY while waiting in the FIFO queue,
		// executeAndCallback immediately observes item.queueCtx.Err() != nil and reports termination.
		a.executeAndCallback(item.queueCtx, item.queueCancel, item.logID, item.param)
	}
}

func validateHandlerName(handler string) error {
	trimmed := strings.TrimSpace(handler)
	if trimmed == "" {
		return fmt.Errorf("executorHandler must not be empty")
	}
	if strings.ContainsAny(trimmed, "/\\?#") || strings.Contains(trimmed, "..") {
		return fmt.Errorf("invalid executorHandler %q: must not contain path separators, '..', query, or fragment characters", handler)
	}
	return nil
}

func (a *Agent) handleRun(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "sidecar agent is draining for shutdown"})
		return
	}

	var param xxlclient.TriggerParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: err.Error()})
		return
	}

	glueType := strings.ToUpper(strings.TrimSpace(param.GlueType))
	isGlue := strings.HasPrefix(glueType, "GLUE_") && param.GlueSource != ""
	if !isGlue && a.cfg.TargetHTTPPort > 0 {
		if err := validateHandlerName(param.ExecutorHandler); err != nil {
			writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: err.Error()})
			return
		}
	}

	hasExplicitLogID := param.LogID > 0
	logID := param.LogID
	if logID <= 0 {
		logID = time.Now().UnixNano()
		param.LogID = logID
	}

	blockStrategy := strings.ToUpper(strings.TrimSpace(param.ExecutorBlockStrategy))
	if blockStrategy == "" {
		blockStrategy = "SERIAL_EXECUTION"
	}

	var entriesToKill []*runningEntry

	a.mu.Lock()
	if a.draining.Load() {
		a.mu.Unlock()
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "sidecar agent is draining for shutdown"})
		return
	}
	// Idempotency guard: if logID is already running or has already been executed, return 200 OK without duplicate execution
	if a.seenLogs[logID] {
		a.mu.Unlock()
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Msg: "duplicate logId already accepted"})
		return
	}

	switch blockStrategy {
	case "DISCARD_LATER":
		if len(a.running[param.JobID]) > 0 {
			a.mu.Unlock()
			writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "discarded due to DISCARD_LATER block strategy"})
			return
		}
	case "COVER_EARLY":
		if hasExplicitLogID {
			if byLog, ok := a.running[param.JobID]; ok {
				for _, entry := range byLog {
					if entry.explicitLogID && entry.logID > logID {
						a.mu.Unlock()
						writeJSON(w, xxlclient.ReturnT{
							Code: xxlclient.FailCode,
							Msg:  fmt.Sprintf("discarded stale out-of-order COVER_EARLY trigger (logId=%d < active logId=%d)", logID, entry.logID),
						})
						return
					}
				}
			}
			for doneLogID, doneJobID := range a.logJobID {
				if doneJobID == param.JobID && doneLogID > logID {
					a.mu.Unlock()
					writeJSON(w, xxlclient.ReturnT{
						Code: xxlclient.FailCode,
						Msg:  fmt.Sprintf("discarded stale out-of-order COVER_EARLY trigger (logId=%d < completed logId=%d)", logID, doneLogID),
					})
					return
				}
			}
		}
		if byLog, ok := a.running[param.JobID]; ok {
			for _, entry := range byLog {
				entry.cancel()
				if entry.executing.Load() {
					entriesToKill = append(entriesToKill, entry)
				}
			}
		}
	case "SERIAL_EXECUTION":
		if len(a.serialQueues[param.JobID]) >= a.cfg.MaxQueueSize {
			a.mu.Unlock()
			writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: fmt.Sprintf("sidecar SERIAL_EXECUTION queue is full (max=%d)", a.cfg.MaxQueueSize)})
			return
		}
	}

	isSerial := blockStrategy == "SERIAL_EXECUTION"
	queueCtx, queueCancel := context.WithCancel(context.Background())
	if a.running[param.JobID] == nil {
		a.running[param.JobID] = make(map[int64]*runningEntry)
	}
	a.running[param.JobID][logID] = &runningEntry{
		logID:         logID,
		explicitLogID: hasExplicitLogID,
		handler:       strings.TrimSpace(param.ExecutorHandler),
		cancel:        queueCancel,
		isSerial:      isSerial,
	}
	a.seenLogs[logID] = true
	if hasExplicitLogID {
		a.logJobID[logID] = param.JobID
	}

	if isSerial {
		a.serialQueues[param.JobID] = append(a.serialQueues[param.JobID], queuedTrigger{
			queueCtx:    queueCtx,
			queueCancel: queueCancel,
			logID:       logID,
			param:       param,
		})
		if !a.serialWorkerActive[param.JobID] {
			a.serialWorkerActive[param.JobID] = true
			go a.serialQueueWorker(param.JobID)
		}
		a.mu.Unlock()
	} else {
		a.mu.Unlock()
		go a.executeAndCallback(queueCtx, queueCancel, logID, param)
	}

	if len(entriesToKill) > 0 {
		go a.notifyTargetKill(param.JobID, entriesToKill)
	}

	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Msg: "triggered via sidecar"})
}

func (a *Agent) executeAndCallback(queueCtx context.Context, queueCancel context.CancelFunc, logID int64, param xxlclient.TriggerParam) {
	// Keep logID in a.running[param.JobID] until execution actually finishes so handleIdleBeat and DISCARD_LATER
	// never falsely report idle while a killed/covered business handler is still winding down (fixes R6-P1-11),
	// and release it immediately before callback retry backoff once pendingCallbacks is incremented (fixes R10-P1-4).
	var releaseOnce sync.Once
	releaseRunning := func() {
		releaseOnce.Do(func() {
			queueCancel()
			a.mu.Lock()
			if byLog, ok := a.running[param.JobID]; ok {
				if entry, exists := byLog[logID]; exists {
					entry.executing.Store(false)
				}
				delete(byLog, logID)
				if len(byLog) == 0 {
					delete(a.running, param.JobID)
				}
			}
			if _, inLogs := a.logs[logID]; !inLogs && !a.isLogRunningLocked(logID) {
				delete(a.seenLogs, logID)
				delete(a.logJobID, logID)
			}
			close(a.execDoneNotify)
			a.execDoneNotify = make(chan struct{})
			a.mu.Unlock()
		})
	}
	defer releaseRunning()

	// Start the per-execution timeout timer ONLY when execution actually begins (after any SERIAL_EXECUTION queue wait, fixes R6-P1-11).
	execCtx := queueCtx
	var execCancel context.CancelFunc = func() {}
	if param.ExecutorTimeout > 0 {
		execCtx, execCancel = context.WithTimeout(queueCtx, time.Duration(param.ExecutorTimeout)*time.Second)
	}
	defer execCancel()

	var output string
	var execErr error

	a.mu.Lock()
	if err := execCtx.Err(); err != nil {
		a.mu.Unlock()
		execErr = fmt.Errorf("job cancelled before execution started: %w", err)
	} else {
		if byLog, ok := a.running[param.JobID]; ok {
			if entry, exists := byLog[logID]; exists {
				entry.executing.Store(true)
			}
		}
		a.mu.Unlock()

		glueType := strings.ToUpper(strings.TrimSpace(param.GlueType))
		if strings.HasPrefix(glueType, "GLUE_") && param.GlueSource != "" {
			if !a.cfg.EnableGlue {
				execErr = fmt.Errorf("GLUE script execution (%s) is disabled in sidecar by default; set XXL_JOB_SIDECAR_ENABLE_GLUE=true to enable", param.GlueType)
			} else {
				output, execErr = runBoundedGlueCommand(execCtx, glueType, param.GlueSource, param)
			}
		} else if a.cfg.TargetHTTPPort > 0 {
			escapedHandler := url.PathEscape(strings.TrimSpace(param.ExecutorHandler))
			targetURL := fmt.Sprintf("http://127.0.0.1:%d/%s/%s",
				a.cfg.TargetHTTPPort,
				strings.Trim(a.cfg.TargetPathPrefix, "/"),
				escapedHandler)
			payload, _ := json.Marshal(map[string]any{
				"jobId":          param.JobID,
				"executorParams": param.ExecutorParams,
				"logId":          param.LogID,
				"broadcastIndex": param.BroadcastIndex,
				"broadcastTotal": param.BroadcastTotal,
			})
			req, err := http.NewRequestWithContext(execCtx, http.MethodPost, targetURL, bytes.NewReader(payload))
			if err != nil {
				execErr = err
			} else {
				req.Header.Set("Content-Type", "application/json")
				if a.cfg.SharedSecret != "" {
					req.Header.Set(SidecarSecretHeader, a.cfg.SharedSecret)
				}
				resp, err := a.httpClient.Do(req)
				if err != nil {
					execErr = err
				} else {
					bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, MaxSingleLogBytes))
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
					resp.Body.Close()
					output = string(bodyBytes)
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						execErr = fmt.Errorf("business container returned HTTP %d: %s", resp.StatusCode, output)
					}
				}
			}
		} else {
			execErr = fmt.Errorf("neither GLUE script nor TargetHTTPPort configured for handler %s", param.ExecutorHandler)
		}
	}

	if len(output) > MaxSingleLogBytes {
		output = output[:MaxSingleLogBytes]
	}

	a.storeLog(logID, output)

	code := xxlclient.SuccessCode
	msg := "Success"
	if execErr != nil {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) && queueCtx.Err() == nil {
			code = xxlclient.TimeoutCode
			msg = fmt.Sprintf("job execution timed out after %ds: %v", param.ExecutorTimeout, execErr)
		} else {
			code = xxlclient.FailCode
			msg = execErr.Error()
		}
	}

	cbParam := []xxlclient.HandleCallbackParam{
		{
			LogID:         param.LogID,
			LogDateTim:    param.LogDateTime,
			HandleCode:    code,
			HandleMsg:     msg,
			ExecuteResult: msg,
		},
	}
	// Reserve pendingCallbacks and inFlightCallbacks[logID] BEFORE writing the spool file or releasing logID
	// from a.running so neither WaitForRunningJobs nor concurrent replaySpooledCallbacks/pruneCallbackSpoolDir
	// ever observes a race window, while releasing a.running immediately so callback retry backoff does not
	// block idleBeat/log/kill (fixes R10-P1-4).
	done, ok := a.tryBeginCallbackDelivery(param.LogID)
	spoolPath := a.spoolCallbackFile(cbParam)
	releaseRunning()
	if !ok {
		return
	}
	if a.deliverSpooledCallbackOnce(cbParam, spoolPath) {
		done()
		return
	}
	// If the first callback attempt failed, run the remaining backoff retries asynchronously holding done()
	// so serialQueueWorker is never blocked from executing the next queued SERIAL_EXECUTION trigger (fixes R11-P2-3).
	go func() {
		defer done()
		a.deliverSpooledCallbackRetries(cbParam, spoolPath, 7)
	}()
}

type limitedBuffer struct {
	buf bytes.Buffer
	max int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	origLen := len(p)
	remaining := l.max - l.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = l.buf.Write(p)
	}
	return origLen, nil
}

func runBoundedGlueCommand(ctx context.Context, glueType, script string, param xxlclient.TriggerParam) (string, error) {
	cmd, err := buildGlueCommand(ctx, glueType, script, param)
	if err != nil {
		return "", err
	}
	lb := &limitedBuffer{max: MaxSingleLogBytes}
	cmd.Stdout = lb
	cmd.Stderr = lb
	runErr := cmd.Run()
	return lb.buf.String(), runErr
}

func buildGlueCommand(ctx context.Context, glueType, script string, param xxlclient.TriggerParam) (*exec.Cmd, error) {
	posArgs := []string{
		param.ExecutorParams,
		strconv.Itoa(param.BroadcastIndex),
		strconv.Itoa(param.BroadcastTotal),
	}
	var bin string
	var args []string
	switch strings.ToUpper(strings.TrimSpace(glueType)) {
	case "GLUE_SHELL":
		bin = "/bin/sh"
		args = append([]string{"-c", script, "xxl-glue"}, posArgs...)
	case "GLUE_PYTHON":
		bin = "python3"
		args = append([]string{"-c", script}, posArgs...)
	case "GLUE_NODEJS":
		bin = "node"
		args = append([]string{"-e", script, "xxl-glue"}, posArgs...)
	case "GLUE_PHP":
		bin = "php"
		args = append([]string{"-r", script, "--"}, posArgs...)
	default:
		return nil, fmt.Errorf("unsupported glueType %q in sidecar", glueType)
	}
	if _, err := exec.LookPath(bin); err != nil {
		return nil, fmt.Errorf("interpreter %q for glueType %q not found in sidecar container: %w", bin, glueType, err)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	// Place the GLUE interpreter and any background subprocesses it spawns into a dedicated process group
	// and kill the entire group on timeout/kill so descendant processes never leak or hold stdout/stderr pipes open (fixes EG2-13).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err == nil {
				return nil
			}
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = 2 * time.Second
	return cmd, nil
}

const maxCallbackSpoolFiles = 512

func (a *Agent) spoolCallbackFile(cbParam []xxlclient.HandleCallbackParam) string {
	if a.cfg.CallbackDir == "" || len(cbParam) == 0 {
		return ""
	}
	data, err := json.Marshal(cbParam)
	if err != nil {
		return ""
	}
	// Deterministic per-logID spool filename written atomically via .tmp + os.Rename so concurrent
	// replaySpooledCallbacks never reads a truncated JSON file (fixes R7-P2-1 & R8-P1-9).
	fileName := fmt.Sprintf("cb-%d.json", cbParam[0].LogID)
	a.pruneCallbackSpoolDir(fileName)
	fullPath := filepath.Join(a.cfg.CallbackDir, fileName)
	tmpPath := fullPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return ""
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		return ""
	}
	return fullPath
}

func (a *Agent) pruneCallbackSpoolDir(keepFileName string) {
	entries, err := os.ReadDir(a.cfg.CallbackDir)
	if err != nil {
		return
	}
	type spoolCandidate struct {
		name  string
		logID int64
	}
	var jsonFiles []spoolCandidate
	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == keepFileName {
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			if info, err := entry.Info(); err == nil && now.Sub(info.ModTime()) > 5*time.Minute {
				_ = os.Remove(filepath.Join(a.cfg.CallbackDir, name))
			}
			continue
		}
		if !strings.HasPrefix(name, "cb-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		rawID := strings.TrimSuffix(strings.TrimPrefix(name, "cb-"), ".json")
		parsedID, _ := strconv.ParseInt(rawID, 10, 64)
		jsonFiles = append(jsonFiles, spoolCandidate{name: name, logID: parsedID})
	}
	if len(jsonFiles) < maxCallbackSpoolFiles {
		return
	}
	sort.Slice(jsonFiles, func(i, j int) bool {
		if jsonFiles[i].logID != jsonFiles[j].logID {
			return jsonFiles[i].logID < jsonFiles[j].logID
		}
		return jsonFiles[i].name < jsonFiles[j].name
	})
	toRemove := len(jsonFiles) - maxCallbackSpoolFiles + 1
	removed := 0
	for i := 0; i < len(jsonFiles) && removed < toRemove; i++ {
		cand := jsonFiles[i]
		a.mu.Lock()
		inFlight := cand.logID > 0 && a.inFlightCallbacks[cand.logID]
		a.mu.Unlock()
		if !inFlight {
			if err := os.Remove(filepath.Join(a.cfg.CallbackDir, cand.name)); err == nil || os.IsNotExist(err) {
				removed++
			}
		}
	}
}

func isTerminalCallbackRejection(err error) bool {
	return xxlclient.IsTerminalCallbackRejection(err)
}

// FlushSpooledCallbacks replays any spooled callback files in CallbackDir once, blocking if a background replay is in flight (fixes R9-P1-4).
func (a *Agent) FlushSpooledCallbacks() {
	a.FlushSpooledCallbacksContext(context.Background())
}

// FlushSpooledCallbacksContext replays any spooled callback files in CallbackDir within the deadline of ctx (fixes EG2-13).
func (a *Agent) FlushSpooledCallbacksContext(ctx context.Context) {
	a.replaySpooledCallbacksWithContext(ctx, true)
}

func (a *Agent) tryBeginCallbackDelivery(logID int64) (func(), bool) {
	a.mu.Lock()
	if logID > 0 {
		if a.inFlightCallbacks[logID] {
			a.mu.Unlock()
			return nil, false
		}
		a.inFlightCallbacks[logID] = true
	}
	a.pendingCallbacks++
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		if logID > 0 {
			delete(a.inFlightCallbacks, logID)
		}
		a.pendingCallbacks--
		a.mu.Unlock()
	}, true
}

func (a *Agent) replaySpooledCallbacks() {
	a.replaySpooledCallbacksWithContext(context.Background(), false)
}

func (a *Agent) replaySpooledCallbacksWithContext(ctx context.Context, blocking bool) {
	if a.cfg.CallbackDir == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}
	if blocking {
		a.replayMu.Lock()
	} else if !a.replayMu.TryLock() {
		return
	}
	defer a.replayMu.Unlock()

	entries, err := os.ReadDir(a.cfg.CallbackDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "cb-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		rawID := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "cb-"), ".json")
		parsedID, _ := strconv.ParseInt(rawID, 10, 64)
		done, ok := a.tryBeginCallbackDelivery(parsedID)
		if !ok {
			continue
		}
		fullPath := filepath.Join(a.cfg.CallbackDir, entry.Name())
		data, err := os.ReadFile(fullPath)
		if err != nil {
			done()
			continue
		}
		var cbParam []xxlclient.HandleCallbackParam
		if err := json.Unmarshal(data, &cbParam); err != nil || len(cbParam) == 0 {
			_ = os.Remove(fullPath)
			done()
			continue
		}
		a.deliverSpooledCallbackLockedContext(ctx, cbParam, fullPath, 2)
		done()
	}
}

func (a *Agent) deliverCallbackWithRetry(cbParam []xxlclient.HandleCallbackParam) {
	var logID int64
	if len(cbParam) > 0 {
		logID = cbParam[0].LogID
	}
	done, ok := a.tryBeginCallbackDelivery(logID)
	if !ok {
		return
	}
	defer done()
	spoolPath := a.spoolCallbackFile(cbParam)
	a.deliverSpooledCallbackLockedContext(context.Background(), cbParam, spoolPath, 8)
}

func (a *Agent) deliverSpooledCallbackOnce(cbParam []xxlclient.HandleCallbackParam, spoolPath string) bool {
	return a.deliverSpooledCallbackOnceContext(context.Background(), cbParam, spoolPath)
}

func (a *Agent) deliverSpooledCallbackOnceContext(ctx context.Context, cbParam []xxlclient.HandleCallbackParam, spoolPath string) bool {
	if len(a.adminClis) == 0 {
		if spoolPath != "" {
			_ = os.Remove(spoolPath)
		}
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false
	}
	for _, cli := range a.adminClis {
		if ctx.Err() != nil {
			return false
		}
		cbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := cli.Callback(cbCtx, cbParam)
		cancel()
		if err == nil || isTerminalCallbackRejection(err) {
			if spoolPath != "" {
				_ = os.Remove(spoolPath)
			}
			return true
		}
	}
	return false
}

func (a *Agent) deliverSpooledCallbackRetries(cbParam []xxlclient.HandleCallbackParam, spoolPath string, remainingAttempts int) {
	a.deliverSpooledCallbackRetriesContext(context.Background(), cbParam, spoolPath, remainingAttempts)
}

func (a *Agent) deliverSpooledCallbackRetriesContext(ctx context.Context, cbParam []xxlclient.HandleCallbackParam, spoolPath string, remainingAttempts int) {
	if ctx == nil {
		ctx = context.Background()
	}
	wait := a.cfg.CallbackRetryWait
	for attempt := 0; attempt < remainingAttempts; attempt++ {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if wait < 2*time.Second {
			wait *= 2
		}
		if a.deliverSpooledCallbackOnceContext(ctx, cbParam, spoolPath) {
			return
		}
	}
}

func (a *Agent) deliverSpooledCallbackLockedContext(ctx context.Context, cbParam []xxlclient.HandleCallbackParam, spoolPath string, maxAttempts int) {
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	if a.deliverSpooledCallbackOnceContext(ctx, cbParam, spoolPath) {
		return
	}
	if maxAttempts > 1 {
		a.deliverSpooledCallbackRetriesContext(ctx, cbParam, spoolPath, maxAttempts-1)
	}
}

func (a *Agent) storeLog(logID int64, content string) {
	if len(content) > MaxSingleLogBytes {
		content = content[:MaxSingleLogBytes]
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if old, exists := a.logs[logID]; exists {
		a.totalLogBytes -= len(old)
	} else {
		a.logOrder = append(a.logOrder, logID)
	}
	a.logs[logID] = content
	a.totalLogBytes += len(content)

	for len(a.logOrder) > a.cfg.MaxLogEntries || (a.totalLogBytes > MaxTotalLogBytes && len(a.logOrder) > 1) {
		evictID := a.logOrder[0]
		a.logOrder = a.logOrder[1:]
		if evictedContent, ok := a.logs[evictID]; ok {
			a.totalLogBytes -= len(evictedContent)
			delete(a.logs, evictID)
		}
		if !a.isLogRunningLocked(evictID) {
			delete(a.seenLogs, evictID)
			delete(a.logJobID, evictID)
		}
	}
}

func (a *Agent) isLogRunning(logID int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.isLogRunningLocked(logID)
}

func (a *Agent) isLogRunningLocked(logID int64) bool {
	for _, byLog := range a.running {
		if _, ok := byLog[logID]; ok {
			return true
		}
	}
	return false
}

// notifyTargetKill sends a best-effort cooperative POST /job/<escaped-handler>/kill notification to the
// localhost business container when an in-flight execution is cancelled via /kill or COVER_EARLY (fixes R6-P1-11, R10-P1-4).
func (a *Agent) notifyTargetKill(jobID int, entries []*runningEntry) {
	if a.cfg.TargetHTTPPort <= 0 {
		return
	}
	notified := make(map[int64]bool)
	for _, e := range entries {
		if e == nil || !e.executing.Load() || e.handler == "" || validateHandlerName(e.handler) != nil {
			continue
		}
		if notified[e.logID] {
			continue
		}
		notified[e.logID] = true
		killURL := fmt.Sprintf("http://127.0.0.1:%d/%s/%s/kill",
			a.cfg.TargetHTTPPort,
			strings.Trim(a.cfg.TargetPathPrefix, "/"),
			url.PathEscape(e.handler))
		payload, _ := json.Marshal(map[string]any{
			"jobId": jobID,
			"logId": e.logID,
		})
		killCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(killCtx, http.MethodPost, killURL, bytes.NewReader(payload))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			if a.cfg.SharedSecret != "" {
				req.Header.Set(SidecarSecretHeader, a.cfg.SharedSecret)
			}
			resp, doErr := a.httpClient.Do(req)
			if doErr == nil && resp != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
			}
		}
		cancel()
	}
}

func (a *Agent) handleKill(w http.ResponseWriter, r *http.Request) {
	var param xxlclient.KillParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: fmt.Sprintf("invalid kill body: %v", err)})
		return
	}
	var entriesToKill []*runningEntry
	a.mu.Lock()
	if byLog, ok := a.running[param.JobID]; ok {
		for _, entry := range byLog {
			entry.cancel()
			if entry.executing.Load() {
				entriesToKill = append(entriesToKill, entry)
			}
		}
	}
	a.mu.Unlock()

	if len(entriesToKill) > 0 {
		a.notifyTargetKill(param.JobID, entriesToKill)
	}
	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode})
}

func (a *Agent) handleLog(w http.ResponseWriter, r *http.Request) {
	var param xxlclient.LogParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: fmt.Sprintf("invalid log body: %v", err)})
		return
	}

	a.mu.Lock()
	rawLog := a.logs[param.LogID]
	isEnd := !a.isLogRunningLocked(param.LogID)
	a.mu.Unlock()

	fromLine := param.FromLineNum
	if fromLine < 1 {
		fromLine = 1
	}

	if rawLog == "" {
		res := xxlclient.LogResult{
			FromLineNum: fromLine,
			ToLineNum:   fromLine - 1,
			LogContent:  "",
			IsEnd:       isEnd,
		}
		raw, _ := json.Marshal(res)
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Content: raw})
		return
	}

	lines := strings.Split(strings.TrimSuffix(rawLog, "\n"), "\n")
	totalLines := len(lines)
	var content string
	toLine := totalLines
	if fromLine <= totalLines {
		content = strings.Join(lines[fromLine-1:], "\n") + "\n"
	} else {
		toLine = fromLine - 1
	}

	res := xxlclient.LogResult{
		FromLineNum: fromLine,
		ToLineNum:   toLine,
		LogContent:  content,
		IsEnd:       isEnd,
	}
	raw, _ := json.Marshal(res)
	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Content: raw})
}

// handlePreStop is invoked by the Kubernetes PreStop hook on 127.0.0.1:
//  1. Requires POST and loopback origin (or valid XXL-JOB-ACCESS-TOKEN) (fixes R5-P0-1 & R6-P2-22).
//  2. Atomically sets draining=true (so /readyz returns 503, /run rejects new triggers, and Register() stops heartbeating).
//  3. Waits up to DrainTimeout for active jobs and callbacks to finish BEFORE removing from xxl_job_registry
//     so >10m running tasks are not falsely marked as lost during graceful shutdown (fixes R5-P1-4, R5-P1-5, R6-P1-11).
func (a *Agent) handlePreStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "only POST is supported on /preStop"})
		return
	}
	token := r.Header.Get(xxlclient.AccessTokenHeader)
	hasValidToken := a.isValidAccessToken(token)
	hasValidSharedSecret := a.cfg.SharedSecret != "" &&
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(r.Header.Get(SidecarSecretHeader))), []byte(a.cfg.SharedSecret)) == 1
	// Require valid XXL-JOB-ACCESS-TOKEN (or loopback + SharedSecret) so untrusted co-located containers
	// cannot trigger premature Sidecar drain/deregistration without credentials (fixes R5-P0-1, R6-P2-22, R7-P2-3).
	if !hasValidToken && !(isLoopbackRequest(r.RemoteAddr) && hasValidSharedSecret) {
		w.WriteHeader(http.StatusForbidden)
		writeJSON(w, xxlclient.ReturnT{Code: xxlclient.FailCode, Msg: "forbidden: /preStop requires valid XXL-JOB-ACCESS-TOKEN or loopback X-XXL-JOB-SIDECAR-SECRET"})
		return
	}

	a.StartDrain()
	a.preStopOnce.Do(func() {
		if a.cfg.DeregisterBeforeDrain {
			deregCtx, deregCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = a.Deregister(deregCtx)
			deregCancel()
		}

		drainCtx, cancel := context.WithTimeout(context.Background(), a.cfg.DrainTimeout)
		defer cancel()
		a.WaitForRunningJobs(drainCtx)

		// Bound post-drain callback flush and deregistration so total /preStop time is strictly below
		// pod_webhook's preStopTimeout (drainSecs + 10s) even when Admin /api/callback hangs (fixes EG2-13).
		flushAfterCtx, flushAfterCancel := context.WithTimeout(context.Background(), 3*time.Second)
		a.FlushSpooledCallbacksContext(flushAfterCtx)
		flushAfterCancel()

		deregCtx, deregCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer deregCancel()
		_ = a.Deregister(deregCtx)
	})
	writeJSON(w, xxlclient.ReturnT{Code: xxlclient.SuccessCode, Msg: "drained and deregistered"})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	_ = json.NewEncoder(w).Encode(v)
}
