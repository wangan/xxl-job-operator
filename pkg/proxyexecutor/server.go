package proxyexecutor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

const (
	LabelJobID           = "xxljob.io/job-id"
	LabelLogID           = "xxljob.io/log-id"
	LabelExecutorName    = "xxljob.io/executor"
	LabelExecutorHandler = "xxljob.io/executor-handler"
	LabelManagedBy       = "app.kubernetes.io/managed-by"
	ManagedByValue       = "xxl-job-operator"

	// UnavailableExecutorSentinel is the path segment (/proxy/_unavailable/{beat,run,...}) used when an
	// addressType=1 JobGroup currently has zero ready executor pods in Kubernetes (fixes N-P0-2).
	UnavailableExecutorSentinel = "_unavailable"

	// JobCallbackFinalizer guards managed batch/v1.Jobs so that if a running or queued Job is deleted
	// (/kill, COVER_EARLY, TTL, or kubectl delete), K8sJobCallbackReconciler always delivers a failure/completion
	// callback to XXL-JOB Admin and advances any queued SERIAL_EXECUTION Jobs before the Job is removed (fixes R4-P1-2).
	JobCallbackFinalizer = "xxljob.io/callback-guard"

	// Annotations persisted on batch/v1.Job so callbacks and serial queues survive Operator restarts
	// and XxlJobTask deletion (fixes R5-P2-5).
	AnnotationTaskNamespace       = "xxljob.io/task-namespace"
	AnnotationTaskName            = "xxljob.io/task-name"
	AnnotationExecutorName        = "xxljob.io/executor-name"
	AnnotationAdminName           = "xxljob.io/admin-name"
	AnnotationAdminURL            = "xxljob.io/admin-url"
	AnnotationAdminUID            = "xxljob.io/admin-uid"
	AnnotationAdminDatasourceHash = "xxljob.io/admin-db-hash"
	AnnotationLogID               = "xxljob.io/log-id"
	AnnotationLogDateTime         = "xxljob.io/log-datetime"
	AnnotationCallbackSent        = "xxljob.io/callback-sent"
	AnnotationBlockStrategy       = "xxljob.io/block-strategy"
	AnnotationFailureMessage      = "xxljob.io/failure-message"

	ConditionAdminEndpointSynced = "AdminEndpointSynced"
	ConditionPreviousAdminURLs   = "PreviousAdminURLs"

	MaxLogReadBytes                 int64 = 4 * 1024 * 1024 // 4 MiB cap per /log request
	MaxRequestBodyBytes             int64 = 1 * 1024 * 1024 // 1 MiB cap per incoming HTTP trigger body
	DefaultJobActiveDeadlineSeconds int64 = 86400           // 24h fallback deadline so stuck pods never block SERIAL_EXECUTION forever (fixes R5-P2-2)
)

// TokenValidator validates XXL-JOB-ACCESS-TOKEN for a routed (namespace, executorName) request.
type TokenValidator func(ctx context.Context, namespace, executorName, providedToken string) error

// PodLogFetcher fetches logs for a given Pod (allows injecting a fake in tests or real client-go CoreV1 in cluster).
type PodLogFetcher func(ctx context.Context, namespace, podName, containerName string) (string, error)

// OnSerialQueueDepth is an optional callback invoked by AdvanceSerialQueueWithReader to report queue depth metrics.
var OnSerialQueueDepth func(namespace, executorLabel, jobIDLabel string, depth float64)

type refMutex struct {
	mu   sync.Mutex
	refs int
}

type keyedMutexMap struct {
	mu    sync.Mutex
	locks map[string]*refMutex
}

func (m *keyedMutexMap) lock(key string) func() {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*refMutex)
	}
	rm, ok := m.locks[key]
	if !ok {
		rm = &refMutex{}
		m.locks[key] = rm
	}
	rm.refs++
	m.mu.Unlock()

	rm.mu.Lock()
	return func() {
		rm.mu.Unlock()
		m.mu.Lock()
		rm.refs--
		if rm.refs <= 0 {
			delete(m.locks, key)
		}
		m.mu.Unlock()
	}
}

var serialQueueLocks keyedMutexMap

// routeContext carries tenant/executor routing extracted from /proxy/<namespace>/<executor>/<action>
// or the sentinel /proxy/_unavailable/<action>.
type routeContext struct {
	Namespace             string
	ExecutorName          string
	Action                string
	IsUnavailableSentinel bool
	IsRootPath            bool
}

// Server implements the stateless XXL-JOB Executor EmbedServer HTTP protocol (:9999)
// and translates triggers into native Kubernetes batch/v1 Jobs.
// Note:
//  1. Asynchronous completion/deletion callbacks (/api/callback) and SERIAL_EXECUTION queue advancement
//     are handled exclusively by the leader-elected K8sJobCallbackReconciler via APIReader (fixes N-P0-4, R4-P1-1, R4-P1-2).
//  2. All active state queries (idleBeat, DISCARD_LATER, killRunningJobs) use APIReader when configured
//     to avoid informer cache lag across replicas (fixes R4-P1-1).
type Server struct {
	ListenAddr       string
	DefaultNamespace string
	AccessToken      string
	K8sClient        client.Client
	APIReader        client.Reader
	Clientset        kubernetes.Interface
	ValidateToken    TokenValidator
	FetchPodLog      PodLogFetcher

	jobLocks keyedMutexMap
}

func (s *Server) lockJob(namespace, executorName string, jobID int) func() {
	key := fmt.Sprintf("%s/%s/%d", namespace, executorName, jobID)
	return s.jobLocks.lock(key)
}

// NewServer creates a new K8sJobProxy Executor server.
func NewServer(k8sClient client.Client, listenAddr, defaultNamespace, accessToken string) *Server {
	if listenAddr == "" {
		listenAddr = ":9999"
	}
	if defaultNamespace == "" {
		defaultNamespace = "default"
	}
	s := &Server{
		ListenAddr:       listenAddr,
		DefaultNamespace: defaultNamespace,
		AccessToken:      sanitizeHeaderToken(accessToken),
		K8sClient:        k8sClient,
		APIReader:        k8sClient,
	}
	s.FetchPodLog = s.defaultFetchPodLog
	return s
}

func (s *Server) apiReader() client.Reader {
	if s.APIReader != nil {
		return s.APIReader
	}
	return s.K8sClient
}

// NeedLeaderElection returns false so the stateless HTTP proxy executor server runs on all replicas
// and stays available during leader election transitions.
func (s *Server) NeedLeaderElection() bool {
	return false
}

// Handler returns the HTTP handler supporting:
//   - Scoped multi-tenant paths: /proxy/<namespace>/<executor>/{beat,idleBeat,run,kill,log}
//   - Sentinel path when 0 pods are ready: /proxy/_unavailable/{beat,idleBeat,run,kill,log}
//
// Root paths /{beat,idleBeat,run,kill,log} and "default_token" are rejected to enforce multi-tenant isolation (fixes N-P0-3 & R4-P0-2).
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeReturnT(w, xxlclient.FailCode, "only POST is supported", nil)
			return
		}

		rc, ok := parseRoute(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}

		// Sentinel unavailable route: always returns 500 so XXL-JOB Admin records a clear scheduling failure
		// and triggers retry/alarm instead of routing to terminating pods (fixes N-P0-2).
		if rc.IsUnavailableSentinel {
			writeReturnT(w, xxlclient.FailCode, "no ready executor pods available in kubernetes (all instances terminating or unready)", nil)
			return
		}

		// Reject unscoped root paths to prevent cross-tenant task triggers or kills (fixes N-P0-3).
		if rc.IsRootPath || rc.Namespace == "" || rc.ExecutorName == "" {
			writeReturnT(w, xxlclient.FailCode, "root proxy path is disabled; use tenant-scoped /proxy/<namespace>/<executor>/<action> endpoint", nil)
			return
		}

		providedToken := sanitizeHeaderToken(r.Header.Get(xxlclient.AccessTokenHeader))
		if providedToken == "" || providedToken == "default_token" {
			writeReturnT(w, xxlclient.FailCode, "refusing empty or default_token on multi-tenant K8sJobProxy endpoint; configure a strong accessToken on XxlJobAdmin", nil)
			return
		}

		expectedStaticToken := sanitizeHeaderToken(s.AccessToken)
		if expectedStaticToken != "" {
			if subtle.ConstantTimeCompare([]byte(providedToken), []byte(expectedStaticToken)) != 1 {
				writeReturnT(w, xxlclient.FailCode, "invalid XXL-JOB-ACCESS-TOKEN", nil)
				return
			}
		}
		if s.ValidateToken != nil {
			if err := s.ValidateToken(r.Context(), rc.Namespace, rc.ExecutorName, providedToken); err != nil {
				writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("access token validation failed: %v", err), nil)
				return
			}
		} else if expectedStaticToken == "" {
			writeReturnT(w, xxlclient.FailCode, "access token validation is required", nil)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)

		switch rc.Action {
		case "beat":
			s.handleBeat(w, r)
		case "idleBeat":
			s.handleIdleBeat(w, r, rc)
		case "run":
			s.handleRun(w, r, rc)
		case "kill":
			s.handleKill(w, r, rc)
		case "log":
			s.handleLog(w, r, rc)
		default:
			http.NotFound(w, r)
		}
	})
}

func parseRoute(urlPath string) (routeContext, bool) {
	trimmed := strings.Trim(urlPath, "/")
	parts := strings.Split(trimmed, "/")

	// Format 1: Root path /<action> (detected so Handler can return a clear 500 rejection)
	if len(parts) == 1 && isValidAction(parts[0]) {
		return routeContext{Action: parts[0], IsRootPath: true}, true
	}

	// Format 2: Sentinel path /proxy/_unavailable/<action>
	if len(parts) == 3 && parts[0] == "proxy" && parts[1] == UnavailableExecutorSentinel && isValidAction(parts[2]) {
		return routeContext{Action: parts[2], IsUnavailableSentinel: true}, true
	}

	// Format 3: Tenant-scoped path /proxy/<namespace>/<executor>/<action>
	if len(parts) == 4 && parts[0] == "proxy" && isValidAction(parts[3]) {
		return routeContext{
			Namespace:    parts[1],
			ExecutorName: parts[2],
			Action:       parts[3],
		}, true
	}
	return routeContext{}, false
}

func isValidAction(action string) bool {
	switch action {
	case "beat", "idleBeat", "run", "kill", "log":
		return true
	default:
		return false
	}
}

// Start implements manager.Runnable so controller-runtime Manager can manage its lifecycle.
func (s *Server) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("proxy-executor")
	httpSrv := &http.Server{
		Addr:              s.ListenAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("Starting K8sJobProxy Executor HTTP server", "addr", s.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// handleBeat responds to XXL-JOB Admin heartbeat checks.
func (s *Server) handleBeat(w http.ResponseWriter, _ *http.Request) {
	writeReturnT(w, xxlclient.SuccessCode, "beat ok", nil)
}

// handleIdleBeat checks via APIReader if any K8s Job (running or queued/suspended under SERIAL_EXECUTION)
// for the requested (namespace, executorName, jobId) is currently active (fixes P1-1, P1-4, R4-P1-1).
func (s *Server) handleIdleBeat(w http.ResponseWriter, r *http.Request, rc routeContext) {
	var param xxlclient.IdleBeatParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("invalid idleBeat body: %v", err), nil)
		return
	}

	origin := s.resolveExecutorOrigin(r.Context(), rc.Namespace, rc.ExecutorName, nil, nil)
	running, err := s.hasRunningJobForOrigin(r.Context(), rc.Namespace, rc.ExecutorName, param.JobID, "", origin)
	if err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("list k8s jobs failed: %v", err), nil)
		return
	}
	if running {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job for executor %s/%s jobId=%d is still running or queued", rc.Namespace, rc.ExecutorName, param.JobID), nil)
		return
	}
	writeReturnT(w, xxlclient.SuccessCode, "idle", nil)
}

// handleRun creates a Kubernetes batch/v1.Job for the triggered XXL-JOB task.
// All Jobs are initially created with spec.suspend=true:
//   - Under SERIAL_EXECUTION, Proxy NEVER unsuspends the Job directly; the leader-elected K8sJobCallbackReconciler
//     is the sole writer that reads via APIReader and unsuspends the single head-of-line Job in strict logId FIFO order (fixes R4-P1-1).
//   - Under DISCARD_LATER and COVER_EARLY, Proxy claims XxlJobTask.Status.LastExecutionJobName via optimistic concurrency
//     first and only unsuspends the winning Job afterwards, guaranteeing losing concurrent replicas never spawn a Pod (fixes R6-P1-7).
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request, rc routeContext) {
	var param xxlclient.TriggerParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("invalid run body: %v", err), nil)
		return
	}

	ctx := r.Context()
	unlock := s.lockJob(rc.Namespace, rc.ExecutorName, param.JobID)
	defer unlock()

	var execCR xxljobv1alpha1.XxlJobExecutor
	hasExecCR := false
	if rc.ExecutorName != "" {
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: rc.Namespace, Name: rc.ExecutorName}, &execCR); err == nil {
			if err := validateProxyExecutorCR(&execCR); err != nil {
				writeReturnT(w, xxlclient.FailCode, err.Error(), nil)
				return
			}
			hasExecCR = true
		} else if !apierrors.IsNotFound(err) {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("get XxlJobExecutor %s/%s failed: %v", rc.Namespace, rc.ExecutorName, err), nil)
			return
		}
	}

	var execPtr *xxljobv1alpha1.XxlJobExecutor
	if hasExecCR {
		execPtr = &execCR
	}
	task, err := s.findMatchingTask(ctx, rc, param, execPtr)
	if err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("lookup XxlJobTask failed: %v", err), nil)
		return
	}
	if task == nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("no declared XxlJobTask CR found for executor=%s/%s jobId=%d",
			rc.Namespace, rc.ExecutorName, param.JobID), nil)
		return
	}

	execName := task.Spec.ExecutorRef.Name
	if execName == "" {
		execName = rc.ExecutorName
	}

	if !hasExecCR && execName != "" {
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: execName}, &execCR); err == nil {
			if err := validateProxyExecutorCR(&execCR); err != nil {
				writeReturnT(w, xxlclient.FailCode, err.Error(), nil)
				return
			}
			hasExecCR = true
			execPtr = &execCR
			if !isTaskConsistentWithExecutor(task, &execCR) {
				writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("no declared XxlJobTask CR found for executor=%s/%s jobId=%d",
					task.Namespace, execName, param.JobID), nil)
				return
			}
		} else if !apierrors.IsNotFound(err) {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("get XxlJobExecutor %s/%s failed: %v", task.Namespace, execName, err), nil)
			return
		}
	}

	// Confused-deputy protection: verify non-default ServiceAccountName (or default SA when AutomountServiceAccountToken=true)
	// against XxlJobExecutor.Spec.AllowedServiceAccounts (fixes R5-P2-10, R8-P1-4).
	if task.Spec.K8sJobTemplate != nil {
		saName := strings.TrimSpace(task.Spec.K8sJobTemplate.ServiceAccountName)
		effectiveSA := saName
		if effectiveSA == "" {
			effectiveSA = "default"
		}
		automount := task.Spec.K8sJobTemplate.AutomountServiceAccountToken != nil && *task.Spec.K8sJobTemplate.AutomountServiceAccountToken
		if effectiveSA != "default" || automount {
			if !hasExecCR {
				writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("verify serviceAccountName failed: XxlJobExecutor %s/%s not found", task.Namespace, execName), nil)
				return
			}
			allowed := false
			for _, item := range execCR.Spec.AllowedServiceAccounts {
				trimmed := strings.TrimSpace(item)
				if trimmed == "*" || trimmed == effectiveSA {
					allowed = true
					break
				}
			}
			if !allowed {
				writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("serviceAccountName %q (automountServiceAccountToken=%v) is not permitted by XxlJobExecutor %s/%s spec.allowedServiceAccounts", effectiveSA, automount, task.Namespace, execName), nil)
				return
			}
		}
	}

	blockStrategy := strings.ToUpper(strings.TrimSpace(param.ExecutorBlockStrategy))
	if blockStrategy == "" {
		blockStrategy = strings.ToUpper(strings.TrimSpace(task.Spec.ExecutorBlockStrategy))
	}
	if blockStrategy == "" {
		blockStrategy = "SERIAL_EXECUTION"
	}

	origin := s.resolveExecutorOrigin(ctx, task.Namespace, execName, task, execPtr)

	// Build and validate the candidate Job before running blockStrategy checks so that:
	//  1. COVER_EARLY never kills running Jobs when the new trigger's template or PodSecurity is invalid, and
	//  2. Idempotent HTTP retries for the same (origin, jobId, logId) exclude their own deterministic k8sJob.Name from
	//     hasRunningJob / killRunningJobs instead of self-discarding or self-killing (fixes R8-P1-2).
	k8sJob, err := s.buildBatchJob(param, task)
	if err != nil {
		writeReturnT(w, xxlclient.FailCode, err.Error(), nil)
		return
	}
	k8sJob.Annotations[AnnotationBlockStrategy] = blockStrategy
	stampOriginatingAdminOrigin(k8sJob, origin)

	podSecLevel := "restricted"
	if hasExecCR && execCR.Spec.PodSecurityLevel != "" {
		podSecLevel = execCR.Spec.PodSecurityLevel
	}
	// Official fallback GLUE runtime images (busybox, python, node, php) default to UID 0 in OCI metadata.
	// When no explicit image or runAsUser was configured, assign a non-root UID (65534) under restricted PSS
	// so Kubelet does not fail container startup with CreateContainerConfigError (fixes EG2-10).
	usedFallbackGlueImage := task.Spec.K8sJobTemplate == nil || strings.TrimSpace(task.Spec.K8sJobTemplate.Image) == ""
	if usedFallbackGlueImage && (strings.TrimSpace(podSecLevel) == "" || strings.EqualFold(strings.TrimSpace(podSecLevel), "restricted")) {
		applyFallbackGlueNonRootUser(&k8sJob.Spec.Template.Spec)
	}
	if err := ApplyAndValidatePodSecurity(ctx, s.apiReader(), k8sJob.Namespace, podSecLevel, &k8sJob.Spec.Template.Spec, k8sJob.Spec.Template.Annotations); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("pod security validation (%s) failed: %v", podSecLevel, err), nil)
		return
	}

	// Resolve existing same-origin Job (across both primary BuildJobName and origin-disambiguated BuildJobName) or
	// switch k8sJob.Name to the origin-scoped deterministic name when primary BuildJobName is occupied by a Job from
	// a different XxlJobAdmin, BEFORE executing DISCARD_LATER or COVER_EARLY (fixes R11-P1-2 & cross-Admin collision).
	originScopedName := BuildJobName(execName, param.JobID, param.LogID, origin.key())
	matchedExisting, occupiedBaseName, findExistErr := s.findExistingTriggeredJobForOrigin(ctx, k8sJob, execName, param, origin)
	if findExistErr != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("check existing k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, findExistErr), nil)
		return
	}
	jobAlreadyExists := false
	if matchedExisting != nil {
		resumeClaim, handled := s.evaluateExistingTriggeredJob(w, matchedExisting, k8sJob, execName, matchedExisting.Namespace, matchedExisting.Name, param, blockStrategy)
		if handled {
			return
		}
		if resumeClaim {
			jobAlreadyExists = true
			k8sJob = matchedExisting
		}
	} else if occupiedBaseName {
		if origin.hasAny() && originScopedName != k8sJob.Name {
			k8sJob.Name = originScopedName
		}
	}

	effectiveLogID := parseJobLogID(k8sJob)
	switch blockStrategy {
	case "DISCARD_LATER":
		running, err := s.hasRunningJobForOrigin(ctx, task.Namespace, execName, param.JobID, k8sJob.Name, origin)
		if err != nil {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("check running k8s jobs for DISCARD_LATER failed: %v", err), nil)
			return
		}
		if running {
			writeReturnT(w, xxlclient.FailCode, "discarded due to DISCARD_LATER block strategy", nil)
			return
		}
	case "COVER_EARLY":
		superseded, err := s.killRunningJobsForCoverEarly(ctx, task.Namespace, execName, param.JobID, effectiveLogID, k8sJob)
		if err != nil {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("kill running k8s jobs for COVER_EARLY failed: %v", err), nil)
			return
		}
		if superseded {
			writeReturnT(w, xxlclient.FailCode, "discarded stale COVER_EARLY trigger superseded by newer active logId", nil)
			return
		}
	}

	// Always create the Job with spec.suspend=true first so that:
	//  - SERIAL_EXECUTION leaves unsuspending exclusively to the leader-elected K8sJobCallbackReconciler (fixes R4-P1-1)
	//  - DISCARD_LATER and COVER_EARLY never spawn a Pod before winning the cross-replica CAS claim (fixes R6-P1-7)
	if !jobAlreadyExists {
		suspend := true
		k8sJob.Spec.Suspend = &suspend

		if err := s.K8sClient.Create(ctx, k8sJob); err != nil {
			if apierrors.IsAlreadyExists(err) {
				var existing batchv1.Job
				if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: k8sJob.Namespace, Name: k8sJob.Name}, &existing); getErr != nil {
					writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("verify existing k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, getErr), nil)
					return
				}
				if origin.hasAny() && originScopedName != k8sJob.Name && !isSameTriggerOrigin(&existing, k8sJob, execName, param) {
					k8sJob.Name = originScopedName
					if retryErr := s.K8sClient.Create(ctx, k8sJob); retryErr != nil {
						if apierrors.IsAlreadyExists(retryErr) {
							if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: k8sJob.Namespace, Name: k8sJob.Name}, &existing); getErr != nil {
								writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("verify existing k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, getErr), nil)
								return
							}
							resumeClaim, handled := s.evaluateExistingTriggeredJob(w, &existing, k8sJob, execName, k8sJob.Namespace, k8sJob.Name, param, blockStrategy)
							if handled {
								return
							}
							if resumeClaim {
								k8sJob = &existing
							}
						} else {
							writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("create k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, retryErr), nil)
							return
						}
					}
				} else {
					resumeClaim, handled := s.evaluateExistingTriggeredJob(w, &existing, k8sJob, execName, k8sJob.Namespace, k8sJob.Name, param, blockStrategy)
					if handled {
						return
					}
					if resumeClaim {
						k8sJob = &existing
					}
				}
			} else {
				writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("create k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, err), nil)
				return
			}
		}
	}

	// Use an uncancelled bounded context for post-Create CAS claim and unsuspend so client timeout after Create
	// never leaves an unclaimed or permanently suspended orphan Job (fixes R6-P1-7).
	claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()

	// Cross-replica DISCARD_LATER & COVER_EARLY atomic winner claim via optimistic concurrency on XxlJobTask.Status (fixes R5-P2-4, R6-P1-7, EG2-11):
	prevLastExec := task.Status.LastExecutionJobName
	switch blockStrategy {
	case "DISCARD_LATER":
		won, err := s.claimDiscardLaterWinner(claimCtx, task, prevLastExec, k8sJob)
		if err != nil || !won {
			writeReturnT(w, xxlclient.FailCode, "discarded due to DISCARD_LATER block strategy (concurrent trigger won)", nil)
			return
		}
	case "COVER_EARLY":
		won, err := s.claimCoverEarlyWinner(claimCtx, task, execName, param.JobID, effectiveLogID, k8sJob)
		if err != nil || !won {
			writeReturnT(w, xxlclient.FailCode, "discarded COVER_EARLY trigger superseded by concurrent newer trigger", nil)
			return
		}
	default:
		if task.Status.LastExecutionJobName != k8sJob.Name {
			s.recordLastExecutionJobName(claimCtx, task, k8sJob.Name)
		}
	}

	if blockStrategy == "SERIAL_EXECUTION" {
		writeReturnT(w, xxlclient.SuccessCode, fmt.Sprintf("enqueued k8s job %s/%s (suspend=true) under SERIAL_EXECUTION", k8sJob.Namespace, k8sJob.Name), nil)
		return
	}

	if err := s.unsuspendJob(claimCtx, k8sJob); err != nil {
		s.cleanupAbortedJob(claimCtx, k8sJob)
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("unsuspend k8s job %s/%s failed: %v", k8sJob.Namespace, k8sJob.Name, err), nil)
		return
	}
	writeReturnT(w, xxlclient.SuccessCode, fmt.Sprintf("created k8s job %s/%s", k8sJob.Namespace, k8sJob.Name), nil)
}

func (s *Server) recordLastExecutionJobName(ctx context.Context, initialTask *xxljobv1alpha1.XxlJobTask, jobName string) {
	current := initialTask.DeepCopy()
	for attempt := 0; attempt < 3; attempt++ {
		current.Status.LastExecutionJobName = jobName
		if err := s.K8sClient.Status().Update(ctx, current); err == nil || !apierrors.IsConflict(err) {
			return
		}
		var latest xxljobv1alpha1.XxlJobTask
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: initialTask.Name}, &latest); err != nil {
			return
		}
		current = &latest
	}
}

func (s *Server) unsuspendJob(ctx context.Context, job *batchv1.Job) error {
	current := job.DeepCopy()
	for attempt := 0; attempt < 4; attempt++ {
		if !current.DeletionTimestamp.IsZero() {
			return fmt.Errorf("cannot unsuspend deleting job %s/%s", job.Namespace, job.Name)
		}
		if current.Annotations != nil && (current.Annotations[AnnotationFailureMessage] != "" || current.Annotations[AnnotationCallbackSent] == "true") {
			return fmt.Errorf("cannot unsuspend aborted or completed job %s/%s", job.Namespace, job.Name)
		}
		unSuspend := false
		current.Spec.Suspend = &unSuspend
		err := s.K8sClient.Update(ctx, current)
		if err == nil {
			*job = *current
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		var latest batchv1.Job
		if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: job.Name}, &latest); getErr != nil {
			return getErr
		}
		current = &latest
	}
	return fmt.Errorf("conflict retrying unsuspend for job %s/%s", job.Namespace, job.Name)
}

func (s *Server) cleanupAbortedJob(ctx context.Context, createdJob *batchv1.Job) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	current := createdJob.DeepCopy()
	for attempt := 0; attempt < 4; attempt++ {
		if !current.DeletionTimestamp.IsZero() {
			return
		}
		suspend := true
		current.Spec.Suspend = &suspend
		current.Finalizers = nil
		err := s.K8sClient.Update(cleanupCtx, current)
		if err == nil || apierrors.IsNotFound(err) {
			break
		}
		if !apierrors.IsConflict(err) {
			break
		}
		var latest batchv1.Job
		if getErr := s.apiReader().Get(cleanupCtx, client.ObjectKey{Namespace: createdJob.Namespace, Name: createdJob.Name}, &latest); getErr != nil {
			break
		}
		current = &latest
	}
	if !current.DeletionTimestamp.IsZero() {
		return
	}
	policy := metav1.DeletePropagationForeground
	_ = s.K8sClient.Delete(cleanupCtx, current, &client.DeleteOptions{PropagationPolicy: &policy})
}

// AdvanceSerialQueue inspects all managed batch/v1.Jobs for (namespace, executorLabel, jobIDLabel)
// and unsuspends the single earliest suspended Job if no unsuspended Job is running.
func AdvanceSerialQueue(ctx context.Context, k8sClient client.Client, namespace, executorLabel, jobIDLabel string) error {
	return AdvanceSerialQueueWithReader(ctx, k8sClient, k8sClient, namespace, executorLabel, jobIDLabel)
}

type serialOriginPartition struct {
	execKey      string
	origins      []adminOrigin
	stillRunning bool
	suspended    []*batchv1.Job
}

func (p *serialOriginPartition) matches(execKey string, orig adminOrigin) bool {
	if p.execKey != execKey {
		return false
	}
	for _, existing := range p.origins {
		if !sameAdminOrigin(existing, orig) {
			return false
		}
	}
	return true
}

func (p *serialOriginPartition) recordOrigin(orig adminOrigin) {
	for _, existing := range p.origins {
		if existing == orig {
			return
		}
	}
	p.origins = append(p.origins, orig)
}

func unsuspendSerialPartitionHead(
	ctx context.Context,
	reader client.Reader,
	writer client.Client,
	suspended []*batchv1.Job,
) (remaining int, unsuspendedAny bool, err error) {
	sort.Slice(suspended, func(i, k int) bool {
		logI := parseJobLogID(suspended[i])
		logK := parseJobLogID(suspended[k])
		if logI != logK {
			return logI < logK
		}
		if !suspended[i].CreationTimestamp.Equal(&suspended[k].CreationTimestamp) {
			return suspended[i].CreationTimestamp.Before(&suspended[k].CreationTimestamp)
		}
		return suspended[i].Name < suspended[k].Name
	})

	remaining = len(suspended)
	for idx := range suspended {
		head := suspended[idx].DeepCopy()
		unsuspended := false
		skipped := false
		for attempt := 0; attempt < 4; attempt++ {
			if !head.DeletionTimestamp.IsZero() || !isJobRunning(head) {
				skipped = true
				break
			}
			if head.Annotations != nil && (head.Annotations[AnnotationFailureMessage] != "" || head.Annotations[AnnotationCallbackSent] == "true") {
				skipped = true
				break
			}
			if head.Spec.Suspend == nil || !*head.Spec.Suspend {
				unsuspended = true
				break
			}
			unSuspend := false
			head.Spec.Suspend = &unSuspend
			updateErr := writer.Update(ctx, head)
			if updateErr == nil {
				unsuspended = true
				break
			}
			if apierrors.IsNotFound(updateErr) {
				skipped = true
				break
			}
			if !apierrors.IsConflict(updateErr) {
				return remaining, false, updateErr
			}
			var latest batchv1.Job
			if getErr := reader.Get(ctx, client.ObjectKey{Namespace: head.Namespace, Name: head.Name}, &latest); getErr != nil {
				if apierrors.IsNotFound(getErr) {
					skipped = true
					break
				}
				return remaining, false, getErr
			}
			head = &latest
		}
		if skipped {
			remaining = len(suspended) - (idx + 1)
			continue
		}
		if !unsuspended {
			return remaining, false, fmt.Errorf("conflict retrying unsuspend for serial queue head %s/%s", head.Namespace, head.Name)
		}
		remaining = len(suspended) - (idx + 1)
		return remaining, true, nil
	}
	return remaining, false, nil
}

// AdvanceSerialQueueWithReader lists Jobs directly via reader (e.g., mgr.GetAPIReader() to bypass informer cache lag)
// and uses writer to unsuspend the single earliest suspended SERIAL_EXECUTION Job per (executor, originating Admin, jobId)
// partition in strict logId FIFO order (fixes R4-P1-1, R6-P2-1, R6-P2-2, R8-P2-4, and cross-Admin SERIAL_EXECUTION isolation).
func AdvanceSerialQueueWithReader(ctx context.Context, reader client.Reader, writer client.Client, namespace, executorLabel, jobIDLabel string) error {
	if namespace == "" || jobIDLabel == "" {
		return nil
	}
	if reader == nil {
		reader = writer
	}

	queueKey := fmt.Sprintf("%s/%s/%s", namespace, executorLabel, jobIDLabel)
	unlock := serialQueueLocks.lock(queueKey)
	defer unlock()

	matchLabels := client.MatchingLabels{
		LabelManagedBy: ManagedByValue,
		LabelJobID:     jobIDLabel,
	}
	if executorLabel != "" {
		matchLabels[LabelExecutorName] = executorLabel
	}

	var jobList batchv1.JobList
	if err := reader.List(ctx, &jobList, client.InNamespace(namespace), matchLabels); err != nil {
		return err
	}

	// Process Jobs in deterministic origin specificity order (uid, dbHash, url, name, with canonical value tie-breaking)
	// so partial-origin Jobs (e.g. empty uid) never coalesce distinct origins or partition inconsistently based on slice order.
	ordered := make([]*batchv1.Job, len(jobList.Items))
	for i := range jobList.Items {
		ordered[i] = &jobList.Items[i]
	}
	sort.SliceStable(ordered, func(i, k int) bool {
		return adminOriginMoreSpecific(extractJobAdminOrigin(ordered[i]), extractJobAdminOrigin(ordered[k]))
	})

	var partitions []*serialOriginPartition
	legacyRunningByExec := make(map[string]bool)
	legacySuspendedByExec := make(map[string][]*batchv1.Job)
	var legacyExecOrder []string
	seenLegacyExec := make(map[string]bool)

	findOrCreatePartition := func(execKey string, orig adminOrigin) *serialOriginPartition {
		for _, p := range partitions {
			if p.matches(execKey, orig) {
				p.recordOrigin(orig)
				return p
			}
		}
		p := &serialOriginPartition{
			execKey: execKey,
			origins: []adminOrigin{orig},
		}
		partitions = append(partitions, p)
		return p
	}

	for _, j := range ordered {
		execKey := executorLabel
		if j.Annotations != nil {
			if ea := strings.TrimSpace(j.Annotations[AnnotationExecutorName]); ea != "" {
				execKey = ea
			}
		}
		orig := extractJobAdminOrigin(j)
		hasOrig := orig.hasAny()

		markRunning := func() {
			if hasOrig {
				p := findOrCreatePartition(execKey, orig)
				p.stillRunning = true
			} else {
				legacyRunningByExec[execKey] = true
			}
		}

		if !isJobRunning(j) {
			if HasActiveOrTerminatingPods(j) && isCompletedJobStillDrainingPods(j) {
				markRunning()
			}
			continue
		}
		// A deleting Job whose Pod is still active or terminating on the node must finish terminating
		// before the next SERIAL_EXECUTION Job is unsuspended, unless its callback/cleanup was already bypassed (fixes R6-P2-1 & R7-P2-7).
		if !j.DeletionTimestamp.IsZero() {
			if HasActiveOrTerminatingPods(j) && isCompletedJobStillDrainingPods(j) {
				markRunning()
			}
			continue
		}
		// Any unsuspended running Job for this (executor, origin, jobId) (even if triggered under another blockStrategy before a spec change)
		// must finish before unsuspending the next SERIAL_EXECUTION Job (fixes R8-P2-4).
		if j.Spec.Suspend == nil || !*j.Spec.Suspend {
			markRunning()
			continue
		}
		// Only non-aborted SERIAL_EXECUTION Jobs belong to the serial queue (fixes R6-P2-2, R9-P1-3).
		if j.Annotations != nil {
			if j.Annotations[AnnotationFailureMessage] != "" || j.Annotations[AnnotationCallbackSent] == "true" {
				continue
			}
			if bs := j.Annotations[AnnotationBlockStrategy]; bs != "" && bs != "SERIAL_EXECUTION" {
				continue
			}
		}
		if hasOrig {
			p := findOrCreatePartition(execKey, orig)
			p.suspended = append(p.suspended, j)
		} else {
			if !seenLegacyExec[execKey] {
				seenLegacyExec[execKey] = true
				legacyExecOrder = append(legacyExecOrder, execKey)
			}
			legacySuspendedByExec[execKey] = append(legacySuspendedByExec[execKey], j)
		}
	}

	var multiOriginLegacy []*serialOriginPartition
	for _, execKey := range legacyExecOrder {
		legSusp := legacySuspendedByExec[execKey]
		if len(legSusp) == 0 {
			continue
		}
		var execParts []*serialOriginPartition
		anyRunning := legacyRunningByExec[execKey]
		for _, p := range partitions {
			if p.execKey == execKey {
				execParts = append(execParts, p)
				if p.stillRunning {
					anyRunning = true
				}
			}
		}
		switch len(execParts) {
		case 0:
			partitions = append(partitions, &serialOriginPartition{
				execKey:      execKey,
				stillRunning: legacyRunningByExec[execKey],
				suspended:    legSusp,
			})
		case 1:
			// Rolling upgrade on the same Admin: fold pre-upgrade suspended Jobs into the single partition so logId FIFO is preserved.
			execParts[0].suspended = append(execParts[0].suspended, legSusp...)
		default:
			multiOriginLegacy = append(multiOriginLegacy, &serialOriginPartition{
				execKey:      execKey,
				stillRunning: anyRunning,
				suspended:    legSusp,
			})
		}
	}
	if len(multiOriginLegacy) > 0 {
		partitions = append(multiOriginLegacy, partitions...)
	}

	totalRemaining := 0
	for _, p := range partitions {
		if p.stillRunning || legacyRunningByExec[p.execKey] || len(p.suspended) == 0 {
			totalRemaining += len(p.suspended)
			continue
		}
		rem, unsuspendedAny, err := unsuspendSerialPartitionHead(ctx, reader, writer, p.suspended)
		if err != nil {
			return err
		}
		if unsuspendedAny && len(p.origins) == 0 {
			legacyRunningByExec[p.execKey] = true
		}
		totalRemaining += rem
	}

	if OnSerialQueueDepth != nil {
		OnSerialQueueDepth(namespace, executorLabel, jobIDLabel, float64(totalRemaining))
	}
	return nil
}

func isCompletedJobStillDrainingPods(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	if job.Annotations != nil && strings.EqualFold(strings.TrimSpace(job.Annotations["xxljob.io/force-delete"]), "true") {
		return false
	}
	if !job.DeletionTimestamp.IsZero() && time.Since(job.DeletionTimestamp.Time) >= 10*time.Minute {
		return false
	}
	if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() && time.Since(job.Status.CompletionTime.Time) >= 10*time.Minute {
		return false
	}
	for _, cond := range job.Status.Conditions {
		if (cond.Type == batchv1.JobComplete || cond.Type == batchv1.JobFailed) &&
			cond.Status == corev1.ConditionTrue &&
			!cond.LastTransitionTime.IsZero() &&
			time.Since(cond.LastTransitionTime.Time) >= 10*time.Minute {
			return false
		}
	}
	for _, f := range job.Finalizers {
		if f == JobCallbackFinalizer || (!job.DeletionTimestamp.IsZero() && f == metav1.FinalizerDeleteDependents) {
			return true
		}
	}
	// Once K8sJobCallbackReconciler has marked callback-sent=true and removed JobCallbackFinalizer
	// (e.g., via shouldBypassCompletedJobCleanup after force-delete or maxCompletedJobCleanupWait),
	// a lingering phantom Terminating status on the finished Job must not deadlock the serial queue (fixes R11-P1-3).
	if job.Annotations != nil && job.Annotations[AnnotationCallbackSent] == "true" {
		return false
	}
	return true
}

func parseJobLogID(job *batchv1.Job) int64 {
	if job.Annotations != nil {
		if v, err := strconv.ParseInt(job.Annotations[AnnotationLogID], 10, 64); err == nil && v > 0 {
			return v
		}
	}
	if job.Labels != nil {
		if v, err := strconv.ParseInt(job.Labels[LabelLogID], 10, 64); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

// handleKill terminates any active or queued Kubernetes batch/v1.Job for the specified (namespace, executorName, jobId).
func (s *Server) handleKill(w http.ResponseWriter, r *http.Request, rc routeContext) {
	var param xxlclient.KillParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("invalid kill body: %v", err), nil)
		return
	}

	unlock := s.lockJob(rc.Namespace, rc.ExecutorName, param.JobID)
	defer unlock()

	origin := s.resolveExecutorOrigin(r.Context(), rc.Namespace, rc.ExecutorName, nil, nil)
	if err := s.killRunningJobsForOrigin(r.Context(), rc.Namespace, rc.ExecutorName, param.JobID, "", origin); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("kill k8s jobs for executor=%s/%s jobId=%d failed: %v", rc.Namespace, rc.ExecutorName, param.JobID, err), nil)
		return
	}
	writeReturnT(w, xxlclient.SuccessCode, "killed k8s job", nil)
}

// handleLog fetches container logs from the Pod created by the K8s Job for logId and slices lines for XXL-JOB Admin.
func (s *Server) handleLog(w http.ResponseWriter, r *http.Request, rc routeContext) {
	var param xxlclient.LogParam
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("invalid log body: %v", err), nil)
		return
	}

	ctx := r.Context()
	logIDStr := strconv.FormatInt(param.LogID, 10)

	matchLabels := client.MatchingLabels{
		LabelManagedBy: ManagedByValue,
		LabelLogID:     logIDStr,
	}
	if rc.ExecutorName != "" {
		matchLabels[LabelExecutorName] = sanitizeLabel(rc.ExecutorName)
	}
	listOpts := []client.ListOption{
		client.InNamespace(rc.Namespace),
		matchLabels,
	}

	origin := s.resolveExecutorOrigin(ctx, rc.Namespace, rc.ExecutorName, nil, nil)

	// 1. Check Job status to determine IsEnd (fail closed on API error so Admin does not stop polling prematurely, fixes R6-P2-17)
	var jobList batchv1.JobList
	if err := s.apiReader().List(ctx, &jobList, listOpts...); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("list k8s jobs for logId=%d failed: %v", param.LogID, err), nil)
		return
	}
	var matchedJob *batchv1.Job
	excludedJobNames := make(map[string]bool)
	for i := range jobList.Items {
		item := &jobList.Items[i]
		if !matchesExecutorJobAnnotation(item, rc.ExecutorName) || !jobMatchesExecutorOrigin(item, origin) {
			excludedJobNames[item.Name] = true
			continue
		}
		matchedJob = item
		break
	}
	isEnd := false
	if matchedJob != nil {
		isEnd = !matchedJob.DeletionTimestamp.IsZero() || !isJobRunning(matchedJob) ||
			(matchedJob.Annotations != nil && (matchedJob.Annotations[AnnotationFailureMessage] != "" || matchedJob.Annotations[AnnotationCallbackSent] == "true"))
	} else {
		isEnd = true
	}

	// 2. Find Pod created by the Job (pick the latest Pod attempt if multiple retries exist)
	var podList corev1.PodList
	if err := s.apiReader().List(ctx, &podList, listOpts...); err != nil {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("list k8s pods for logId=%d failed: %v", param.LogID, err), nil)
		return
	}
	if len(podList.Items) > 0 && (matchedJob != nil || len(excludedJobNames) > 0) {
		filteredPods := podList.Items[:0]
		for i := range podList.Items {
			p := &podList.Items[i]
			podJobName := ""
			if p.Labels != nil {
				podJobName = p.Labels["job-name"]
			}
			if matchedJob != nil && podJobName != "" && podJobName != matchedJob.Name {
				continue
			}
			if matchedJob == nil && podJobName != "" && excludedJobNames[podJobName] {
				continue
			}
			filteredPods = append(filteredPods, *p)
		}
		podList.Items = filteredPods
	}

	var rawLog string
	if len(podList.Items) > 0 {
		sort.Slice(podList.Items, func(i, k int) bool {
			if !podList.Items[i].CreationTimestamp.Equal(&podList.Items[k].CreationTimestamp) {
				return podList.Items[k].CreationTimestamp.Before(&podList.Items[i].CreationTimestamp)
			}
			return podList.Items[i].Name > podList.Items[k].Name
		})
		pod := &podList.Items[0]
		containerName := "job-runner"
		if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Name != "" {
			containerName = pod.Spec.Containers[0].Name
		}
		if s.FetchPodLog != nil {
			fetched, err := s.FetchPodLog(ctx, pod.Namespace, pod.Name, containerName)
			if err != nil {
				if pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending {
					writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("fetch pod log for %s/%s failed: %v", pod.Namespace, pod.Name, err), nil)
					return
				}
			} else if fetched != "" {
				rawLog = fetched
				if int64(len(rawLog)) >= MaxLogReadBytes {
					rawLog += "\n... [log output truncated at 4 MiB limit] ...\n"
				}
			}
		}
	}

	if rawLog == "" && isEnd && matchedJob != nil {
		if matchedJob.Annotations != nil && matchedJob.Annotations[AnnotationFailureMessage] != "" {
			rawLog = matchedJob.Annotations[AnnotationFailureMessage]
		} else if _, failed, failMsg := EvaluateJobCompletion(matchedJob); failed && failMsg != "" && len(podList.Items) == 0 {
			rawLog = failMsg
		}
	}

	fromLine := param.FromLineNum
	if fromLine < 1 {
		fromLine = 1
	}

	// If no log lines are available yet, return ToLineNum = fromLine - 1 so Admin's next poll
	// (which requests toLineNum + 1) does not skip line 1 (fixes P1-2).
	if rawLog == "" {
		res := xxlclient.LogResult{
			FromLineNum: fromLine,
			ToLineNum:   fromLine - 1,
			LogContent:  "",
			IsEnd:       isEnd,
		}
		writeReturnT(w, xxlclient.SuccessCode, "", res)
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
	writeReturnT(w, xxlclient.SuccessCode, "", res)
}

func (s *Server) defaultFetchPodLog(ctx context.Context, namespace, podName, containerName string) (string, error) {
	if s.Clientset == nil {
		return "", fmt.Errorf("clientset not configured")
	}
	limitBytes := MaxLogReadBytes
	req := s.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  containerName,
		LimitBytes: &limitBytes,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, MaxLogReadBytes))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func isSameTriggerOrigin(existing, candidate *batchv1.Job, expectedExecName string, param xxlclient.TriggerParam) bool {
	if existing == nil || candidate == nil {
		return false
	}
	if !matchesExecutorJobAnnotation(existing, expectedExecName) {
		return false
	}
	hasExistOrigin := jobHasAdminOrigin(existing)
	hasCandOrigin := jobHasAdminOrigin(candidate)
	if hasExistOrigin && hasCandOrigin {
		return isSameOriginatingAdminJob(existing, candidate)
	}
	if hasExistOrigin != hasCandOrigin {
		if !hasExistOrigin && hasCandOrigin {
			// Allow seamless rolling-upgrade resume ONLY for an active/suspended pre-upgrade Job whose explicit
			// non-empty AnnotationLogDateTime matches param.LogDateTime; never mistake a completed or timestamp-less
			// historical Job for an idempotent trigger after cross-Admin migration.
			if !existing.DeletionTimestamp.IsZero() || !isJobRunning(existing) {
				return false
			}
			if existing.Annotations == nil || existing.Annotations[AnnotationCallbackSent] == "true" {
				return false
			}
			actualDT := strings.TrimSpace(existing.Annotations[AnnotationLogDateTime])
			if param.LogDateTime > 0 {
				return actualDT != "" && actualDT == strconv.FormatInt(param.LogDateTime, 10)
			}
			return actualDT == "0"
		}
		return false
	}
	return true
}

func (s *Server) findExistingTriggeredJobForOrigin(
	ctx context.Context,
	candidate *batchv1.Job,
	execName string,
	param xxlclient.TriggerParam,
	origin adminOrigin,
) (*batchv1.Job, bool, error) {
	baseName := candidate.Name
	scopedName := BuildJobName(execName, param.JobID, param.LogID, origin.key())

	var baseJob batchv1.Job
	hasBase := false
	if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: candidate.Namespace, Name: baseName}, &baseJob); err == nil {
		hasBase = true
		if isSameTriggerOrigin(&baseJob, candidate, execName, param) {
			return &baseJob, false, nil
		}
		if !origin.hasAny() {
			// No Admin origin available to disambiguate; let evaluateExistingTriggeredJob report the exact conflict.
			return &baseJob, false, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	if scopedName != baseName {
		var scopedJob batchv1.Job
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: candidate.Namespace, Name: scopedName}, &scopedJob); err == nil {
			return &scopedJob, hasBase, nil
		} else if !apierrors.IsNotFound(err) {
			return nil, hasBase, err
		}
	}

	if param.LogID > 0 {
		matchLabels := client.MatchingLabels{
			LabelManagedBy:    ManagedByValue,
			LabelExecutorName: sanitizeLabel(execName),
			LabelJobID:        strconv.Itoa(param.JobID),
			LabelLogID:        strconv.FormatInt(param.LogID, 10),
		}
		var list batchv1.JobList
		if err := s.apiReader().List(ctx, &list, client.InNamespace(candidate.Namespace), matchLabels); err != nil {
			return nil, hasBase, err
		}
		for i := range list.Items {
			j := &list.Items[i]
			if j.Name == baseName || j.Name == scopedName {
				continue
			}
			if isSameTriggerOrigin(j, candidate, execName, param) {
				found := *j
				return &found, hasBase, nil
			}
		}
	}

	return nil, hasBase, nil
}

func (s *Server) evaluateExistingTriggeredJob(
	w http.ResponseWriter,
	existing *batchv1.Job,
	candidate *batchv1.Job,
	expectedExecName string,
	jobNS, jobName string,
	param xxlclient.TriggerParam,
	blockStrategy string,
) (resumeClaim bool, handled bool) {
	if !matchesExecutorJobAnnotation(existing, expectedExecName) {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s already exists for a different executor (%q != %q)",
			jobNS, jobName, existing.Annotations[AnnotationExecutorName], expectedExecName), nil)
		return false, true
	}
	if candidate != nil && !isSameTriggerOrigin(existing, candidate, expectedExecName, param) {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s already exists for a different originating XxlJobAdmin",
			jobNS, jobName), nil)
		return false, true
	}
	if !existing.DeletionTimestamp.IsZero() {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s is currently being deleted", jobNS, jobName), nil)
		return false, true
	}
	expectedDateTime := strconv.FormatInt(param.LogDateTime, 10)
	actualDateTime := ""
	if existing.Annotations != nil {
		actualDateTime = existing.Annotations[AnnotationLogDateTime]
	}
	if actualDateTime != expectedDateTime {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s already exists with conflicting logDateTime (%q != %q)",
			jobNS, jobName, actualDateTime, expectedDateTime), nil)
		return false, true
	}
	if existing.Annotations != nil {
		if failMsg := existing.Annotations[AnnotationFailureMessage]; failMsg != "" {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s already failed: %s", jobNS, jobName, failMsg), nil)
			return false, true
		}
		if existing.Annotations[AnnotationCallbackSent] == "true" {
			writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s has already completed callback delivery", jobNS, jobName), nil)
			return false, true
		}
	}
	if _, failed, failMsg := EvaluateJobCompletion(existing); failed {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s already failed: %s", jobNS, jobName, failMsg), nil)
		return false, true
	}
	if isStaleOrphanSuspendedJob(existing) {
		writeReturnT(w, xxlclient.FailCode, fmt.Sprintf("k8s job %s/%s is a stale suspended orphan and is being aborted", jobNS, jobName), nil)
		return false, true
	}
	// If the existing Job is a non-SERIAL Job that is still suspended (e.g. previous trigger interrupted
	// before unsuspend completed), resume the claim/unsuspend flow instead of returning prematurely (fixes R7-P1-3, R8-P1-2, R9-P1-3).
	if existing.Spec.Suspend != nil && *existing.Spec.Suspend && blockStrategy != "SERIAL_EXECUTION" {
		return true, false
	}
	writeReturnT(w, xxlclient.SuccessCode, fmt.Sprintf("k8s job %s/%s already exists", jobNS, jobName), nil)
	return false, true
}

func isTaskConsistentWithExecutor(task *xxljobv1alpha1.XxlJobTask, execCR *xxljobv1alpha1.XxlJobExecutor) bool {
	if task == nil || execCR == nil {
		return true
	}
	if execCR.Status.JobGroupID > 0 && task.Status.JobGroupID > 0 && task.Status.JobGroupID != execCR.Status.JobGroupID {
		return false
	}
	execAdminURL := strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
	taskAdminURL := strings.TrimRight(strings.TrimSpace(task.Status.AdminURL), "/")
	execOrig := extractSyncedEndpointOrigin(execCR.Status.Conditions, execAdminURL)
	taskOrig := extractSyncedEndpointOrigin(task.Status.Conditions, taskAdminURL)
	if execOrig.uid != "" && taskOrig.uid != "" && execOrig.uid != taskOrig.uid {
		return false
	}
	if execOrig.dbHash != "" && taskOrig.dbHash != "" && execOrig.dbHash != taskOrig.dbHash {
		return false
	}
	if execAdminURL != "" && taskAdminURL != "" && taskAdminURL != execAdminURL {
		if execOrig.hasAny() && taskOrig.hasAny() && sameManagedAdminOrigin(execOrig, taskOrig) {
			return true
		}
		return false
	}
	return true
}

// findMatchingTask matches a declared, non-deleting XxlJobTask strictly by Status.JobID (never falling back to
// ExecutorHandler name, which could otherwise let an undeclared task in Admin borrow another CR's template),
// and verifies Status.JobGroupID / Status.AdminURL consistency against execCR when present (fixes R11-P1-2).
func (s *Server) findMatchingTask(
	ctx context.Context,
	rc routeContext,
	param xxlclient.TriggerParam,
	execCR *xxljobv1alpha1.XxlJobExecutor,
) (*xxljobv1alpha1.XxlJobTask, error) {
	if param.JobID <= 0 {
		return nil, nil
	}
	var list xxljobv1alpha1.XxlJobTaskList
	if err := s.apiReader().List(ctx, &list, client.InNamespace(rc.Namespace)); err != nil {
		return nil, err
	}

	for i := range list.Items {
		item := &list.Items[i]
		if !item.DeletionTimestamp.IsZero() {
			continue
		}
		execNS := item.Spec.ExecutorRef.Namespace
		if execNS == "" {
			execNS = item.Namespace
		}
		if execNS != item.Namespace {
			continue
		}
		if rc.ExecutorName != "" && item.Spec.ExecutorRef.Name != rc.ExecutorName {
			continue
		}
		if !isTaskConsistentWithExecutor(item, execCR) {
			continue
		}
		if item.Status.JobID == param.JobID {
			return item, nil
		}
	}
	return nil, nil
}

func (s *Server) claimDiscardLaterWinner(ctx context.Context, initialTask *xxljobv1alpha1.XxlJobTask, prevLastExec string, createdJob *batchv1.Job) (bool, error) {
	if initialTask.Status.LastExecutionJobName == createdJob.Name {
		return true, nil
	}
	current := initialTask.DeepCopy()
	for attempt := 0; attempt < 4; attempt++ {
		if current.Status.LastExecutionJobName != "" && current.Status.LastExecutionJobName != createdJob.Name {
			var winnerJob batchv1.Job
			if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: current.Status.LastExecutionJobName}, &winnerJob); getErr == nil {
				if isSameOriginatingAdminJob(&winnerJob, createdJob) && isJobBlockingExecution(&winnerJob) {
					break
				}
			} else if !apierrors.IsNotFound(getErr) {
				break
			}
			prevLastExec = current.Status.LastExecutionJobName
		}
		current.Status.LastExecutionJobName = createdJob.Name
		err := s.K8sClient.Status().Update(ctx, current)
		if err == nil {
			return true, nil
		}
		if !apierrors.IsConflict(err) {
			// On non-conflict error, re-read via APIReader to check if our update actually landed in etcd;
			// otherwise fail closed and clean up the created Job (fixes R6-P1-7).
			var latest xxljobv1alpha1.XxlJobTask
			if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: initialTask.Name}, &latest); getErr == nil &&
				latest.Status.LastExecutionJobName == createdJob.Name {
				return true, nil
			}
			break
		}
		var latest xxljobv1alpha1.XxlJobTask
		if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: initialTask.Name}, &latest); getErr != nil {
			break
		}
		if latest.Status.LastExecutionJobName == createdJob.Name {
			return true, nil
		}
		current = &latest
	}

	// Our Job lost the concurrent DISCARD_LATER race or failed to claim: strip its callback finalizer and delete it cleanly.
	s.cleanupAbortedJob(ctx, createdJob)
	return false, nil
}

func validateProxyExecutorCR(execCR *xxljobv1alpha1.XxlJobExecutor) error {
	if execCR == nil {
		return nil
	}
	if !execCR.DeletionTimestamp.IsZero() {
		return fmt.Errorf("XxlJobExecutor %s/%s is terminating", execCR.Namespace, execCR.Name)
	}
	if execCR.Spec.Mode != xxljobv1alpha1.ExecutorModeK8sJobProxy {
		return fmt.Errorf("XxlJobExecutor %s/%s mode %q does not accept K8sJobProxy triggers", execCR.Namespace, execCR.Name, execCR.Spec.Mode)
	}
	return nil
}

// SyncedAdminEndpointMeta persists non-sensitive endpoint, datasource hash, and SecretRef metadata
// in Status.Conditions[ConditionAdminEndpointSynced] so controllers and K8sJobProxy share an identical
// JSON schema without storing plaintext secrets in etcd.
type SyncedAdminEndpointMeta struct {
	AdminName              string                             `json:"adminName,omitempty"`
	AdminUID               string                             `json:"adminUID,omitempty"`
	AdminURL               string                             `json:"adminURL,omitempty"`
	ManagedInCluster       bool                               `json:"managedInCluster,omitempty"`
	DatasourceHash         string                             `json:"datasourceHash,omitempty"`
	AuthCaptured           bool                               `json:"authCaptured,omitempty"`
	Username               string                             `json:"username,omitempty"`
	UsedDefaultPassword    bool                               `json:"usedDefaultPassword,omitempty"`
	UsedDefaultAccessToken bool                               `json:"usedDefaultAccessToken,omitempty"`
	PasswordSecretRef      *xxljobv1alpha1.SecretKeyReference `json:"passwordSecretRef,omitempty"`
	AccessTokenSecretRef   *xxljobv1alpha1.SecretKeyReference `json:"accessTokenSecretRef,omitempty"`
}

// DatasourceURLHash computes a non-reversible hex SHA-256 prefix of a trimmed JDBC URL so controllers
// and K8sJobProxy can detect whether an in-cluster managed XxlJobAdmin still points to the same database
// without exposing JDBC query parameters in Status.Conditions or Job annotations.
func DatasourceURLHash(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:16])
}

// IsClusterLocalAdminServiceURL reports whether rawURL targets the cluster-internal Kubernetes Service
// (<adminName>.<namespace>.svc.cluster.local) owned by XxlJobAdmin.
func IsClusterLocalAdminServiceURL(rawURL, adminName, namespace string) bool {
	rawURL = strings.TrimSpace(rawURL)
	adminName = strings.TrimSpace(adminName)
	namespace = strings.TrimSpace(namespace)
	if rawURL == "" || adminName == "" || namespace == "" {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	expectedHost := fmt.Sprintf("%s.%s.svc.cluster.local", adminName, namespace)
	return strings.EqualFold(u.Hostname(), expectedHost)
}

// EncodeSyncedAdminEndpointMeta serializes non-sensitive XxlJobAdmin endpoint identity and credential
// reference metadata into JSON for ConditionAdminEndpointSynced.Message.
func EncodeSyncedAdminEndpointMeta(admin *xxljobv1alpha1.XxlJobAdmin, adminURL string) string {
	if admin == nil {
		return ""
	}
	normURL := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	isPureExternal := strings.TrimSpace(admin.Spec.ExternalURL) != "" && strings.TrimSpace(admin.Spec.Image) == ""
	managedInCluster := !isPureExternal
	dbHash := ""
	if !isPureExternal {
		dbHash = DatasourceURLHash(admin.Spec.Datasource.URL)
	}
	trimmedPw := strings.TrimSpace(admin.Spec.Auth.Password)
	sanitizedTok := sanitizeHeaderToken(admin.Spec.Auth.AccessToken)
	metaObj := SyncedAdminEndpointMeta{
		AdminName:              admin.Name,
		AdminUID:               string(admin.UID),
		AdminURL:               normURL,
		ManagedInCluster:       managedInCluster,
		DatasourceHash:         dbHash,
		AuthCaptured:           true,
		Username:               admin.Spec.Auth.Username,
		UsedDefaultPassword:    admin.Spec.Auth.PasswordSecretRef == nil && (trimmedPw == "" || trimmedPw == "123456"),
		UsedDefaultAccessToken: admin.Spec.Auth.AccessTokenSecretRef == nil && (sanitizedTok == "" || sanitizedTok == "default_token"),
	}
	if admin.Spec.Auth.PasswordSecretRef != nil {
		metaObj.PasswordSecretRef = admin.Spec.Auth.PasswordSecretRef.DeepCopy()
	}
	if admin.Spec.Auth.AccessTokenSecretRef != nil {
		metaObj.AccessTokenSecretRef = admin.Spec.Auth.AccessTokenSecretRef.DeepCopy()
	}
	raw, err := json.Marshal(metaObj)
	if err != nil {
		return ""
	}
	return string(raw)
}

// DecodeSyncedAdminEndpointMeta parses ConditionAdminEndpointSynced.Message from Status.Conditions.
func DecodeSyncedAdminEndpointMeta(conditions []metav1.Condition) *SyncedAdminEndpointMeta {
	for i := range conditions {
		c := &conditions[i]
		if c.Type != ConditionAdminEndpointSynced || strings.TrimSpace(c.Message) == "" {
			continue
		}
		var parsed SyncedAdminEndpointMeta
		if err := json.Unmarshal([]byte(c.Message), &parsed); err != nil {
			continue
		}
		if strings.TrimSpace(parsed.AdminName) == "" &&
			strings.TrimSpace(parsed.AdminUID) == "" &&
			strings.TrimSpace(parsed.AdminURL) == "" &&
			strings.TrimSpace(parsed.DatasourceHash) == "" &&
			!parsed.ManagedInCluster &&
			!parsed.AuthCaptured {
			continue
		}
		return &parsed
	}
	return nil
}

type adminOrigin struct {
	name   string
	url    string
	uid    string
	dbHash string
}

func (o adminOrigin) hasAny() bool {
	return o.name != "" || o.url != "" || o.uid != "" || o.dbHash != ""
}

func (o adminOrigin) key() string {
	base := CanonicalAdminOriginKey(o.name, o.url, o.uid)
	if o.dbHash == "" {
		return base
	}
	if base == "" {
		return "db=" + o.dbHash
	}
	return base + "|db=" + o.dbHash
}

func adminOriginMoreSpecific(a, b adminOrigin) bool {
	if (a.uid != "") != (b.uid != "") {
		return a.uid != ""
	}
	if (a.dbHash != "") != (b.dbHash != "") {
		return a.dbHash != ""
	}
	if (a.url != "") != (b.url != "") {
		return a.url != ""
	}
	if (a.name != "") != (b.name != "") {
		return a.name != ""
	}
	if a.uid != b.uid {
		return a.uid < b.uid
	}
	if a.dbHash != b.dbHash {
		return a.dbHash < b.dbHash
	}
	if a.url != b.url {
		return a.url < b.url
	}
	return a.name < b.name
}

// CanonicalAdminOriginKey produces a stable identity string for an originating XxlJobAdmin
// (incorporating CR name, normalized base URL, and CR UID when available) so Job names do not
// collide across independent Admin instances, in-place ExternalURL edits, or recreated Admin CRs.
func CanonicalAdminOriginKey(adminName, adminURL, adminUID string) string {
	name := strings.TrimSpace(adminName)
	u := strings.TrimRight(strings.TrimSpace(adminURL), "/")
	uid := strings.TrimSpace(adminUID)
	if name == "" && u == "" && uid == "" {
		return ""
	}
	return fmt.Sprintf("name=%s|url=%s|uid=%s", name, u, uid)
}

func resolveAdminURLForOrigin(admin *xxljobv1alpha1.XxlJobAdmin) string {
	if admin == nil {
		return ""
	}
	if ext := strings.TrimRight(strings.TrimSpace(admin.Spec.ExternalURL), "/"); ext != "" {
		return ext
	}
	port := admin.Spec.Port
	if port <= 0 {
		port = 8080
	}
	ctxPath := strings.TrimSpace(admin.Spec.ContextPath)
	if ctxPath == "" {
		ctxPath = "/xxl-job-admin"
	} else if !strings.HasPrefix(ctxPath, "/") {
		ctxPath = "/" + ctxPath
	}
	if len(ctxPath) > 1 {
		ctxPath = strings.TrimRight(ctxPath, "/")
	}
	if ctxPath == "/" {
		ctxPath = ""
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", admin.Name, admin.Namespace, port, ctxPath)
}

func extractSyncedEndpointOrigin(conds []metav1.Condition, targetURL string) adminOrigin {
	targetURL = strings.TrimRight(strings.TrimSpace(targetURL), "/")
	parsed := DecodeSyncedAdminEndpointMeta(conds)
	if parsed == nil {
		return adminOrigin{}
	}
	o := adminOrigin{
		name:   strings.TrimSpace(parsed.AdminName),
		url:    strings.TrimRight(strings.TrimSpace(parsed.AdminURL), "/"),
		uid:    strings.TrimSpace(parsed.AdminUID),
		dbHash: strings.TrimSpace(parsed.DatasourceHash),
	}
	if !o.hasAny() {
		return adminOrigin{}
	}
	if targetURL == "" {
		return o
	}
	if o.url == targetURL {
		return o
	}
	if o.url == "" && (o.name != "" || o.uid != "") {
		o.url = targetURL
		return o
	}
	return adminOrigin{}
}

func adminPreviouslyOwnedURL(admin *xxljobv1alpha1.XxlJobAdmin, targetURL string) bool {
	targetURL = strings.TrimRight(strings.TrimSpace(targetURL), "/")
	if admin == nil || targetURL == "" {
		return false
	}
	if strings.TrimRight(strings.TrimSpace(admin.Status.AdminURL), "/") == targetURL {
		return true
	}
	for i := range admin.Status.Conditions {
		c := &admin.Status.Conditions[i]
		if c.Type == ConditionPreviousAdminURLs {
			for _, part := range strings.Split(c.Message, ",") {
				if strings.TrimRight(strings.TrimSpace(part), "/") == targetURL {
					return true
				}
			}
		}
	}
	return false
}

func (s *Server) resolveExecutorOrigin(
	ctx context.Context,
	namespace, executorName string,
	task *xxljobv1alpha1.XxlJobTask,
	execCR *xxljobv1alpha1.XxlJobExecutor,
) adminOrigin {
	if execCR == nil && namespace != "" && executorName != "" {
		var fetched xxljobv1alpha1.XxlJobExecutor
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: executorName}, &fetched); err == nil {
			execCR = &fetched
		}
	}
	ns := namespace
	specAdminName := ""
	activeSyncedURL := ""
	if task != nil {
		if ns == "" {
			ns = task.Namespace
		}
		activeSyncedURL = strings.TrimRight(strings.TrimSpace(task.Status.AdminURL), "/")
	}
	if execCR != nil {
		if ns == "" {
			ns = execCR.Namespace
		}
		adminNS := execCR.Spec.AdminRef.Namespace
		if adminNS == "" || adminNS == execCR.Namespace {
			specAdminName = strings.TrimSpace(execCR.Spec.AdminRef.Name)
		}
		if activeSyncedURL == "" {
			activeSyncedURL = strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
		}
	}

	lookupConditionOrigin := func(targetURL string) adminOrigin {
		if task != nil {
			if co := extractSyncedEndpointOrigin(task.Status.Conditions, targetURL); co.hasAny() {
				return co
			}
		}
		if execCR != nil {
			if co := extractSyncedEndpointOrigin(execCR.Status.Conditions, targetURL); co.hasAny() {
				return co
			}
		}
		return adminOrigin{}
	}

	adminDBHashForURL := func(admin *xxljobv1alpha1.XxlJobAdmin, candidateURL string) string {
		if admin == nil {
			return ""
		}
		isPureExternal := strings.TrimSpace(admin.Spec.ExternalURL) != "" && strings.TrimSpace(admin.Spec.Image) == ""
		if isPureExternal {
			return ""
		}
		if candidateURL != "" &&
			candidateURL != resolveAdminURLForOrigin(admin) &&
			!IsClusterLocalAdminServiceURL(candidateURL, admin.Name, admin.Namespace) {
			return ""
		}
		return DatasourceURLHash(admin.Spec.Datasource.URL)
	}

	var specAdminCR xxljobv1alpha1.XxlJobAdmin
	hasSpecAdmin := false
	if specAdminName != "" && ns != "" {
		if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: ns, Name: specAdminName}, &specAdminCR); err == nil {
			hasSpecAdmin = true
		}
	}

	if hasSpecAdmin {
		specAdminURL := resolveAdminURLForOrigin(&specAdminCR)
		specAdminUID := strings.TrimSpace(string(specAdminCR.UID))
		if activeSyncedURL == "" || activeSyncedURL == specAdminURL || adminPreviouslyOwnedURL(&specAdminCR, activeSyncedURL) {
			effectiveURL := activeSyncedURL
			if effectiveURL == "" {
				effectiveURL = specAdminURL
			}
			effectiveDBHash := ""
			if effectiveURL == specAdminURL {
				effectiveDBHash = adminDBHashForURL(&specAdminCR, effectiveURL)
			} else if condOrigin := lookupConditionOrigin(effectiveURL); condOrigin.dbHash != "" &&
				condOrigin.name == specAdminCR.Name && (condOrigin.uid == "" || condOrigin.uid == specAdminUID) {
				effectiveDBHash = condOrigin.dbHash
			} else {
				effectiveDBHash = adminDBHashForURL(&specAdminCR, effectiveURL)
			}
			return adminOrigin{
				name:   specAdminCR.Name,
				url:    effectiveURL,
				uid:    specAdminUID,
				dbHash: effectiveDBHash,
			}
		}

		// Pre-reconcile transition: Spec.AdminRef or ExternalURL / Port / ContextPath changed before Status.AdminURL reconciled.
		// Resolve the Admin CR that actually owns activeSyncedURL so we never stamp a hybrid (specAdmin.Name/UID + prevAdmin.URL).
		if condOrigin := lookupConditionOrigin(activeSyncedURL); condOrigin.hasAny() {
			if condOrigin.name == specAdminCR.Name && (condOrigin.uid == "" || condOrigin.uid == specAdminUID) {
				dbHash := condOrigin.dbHash
				if dbHash == "" {
					dbHash = adminDBHashForURL(&specAdminCR, activeSyncedURL)
				}
				return adminOrigin{name: specAdminCR.Name, url: activeSyncedURL, uid: specAdminUID, dbHash: dbHash}
			}
			if condOrigin.uid == "" && condOrigin.name != "" && ns != "" {
				var prevCR xxljobv1alpha1.XxlJobAdmin
				if err := s.apiReader().Get(ctx, client.ObjectKey{Namespace: ns, Name: condOrigin.name}, &prevCR); err == nil {
					if resolveAdminURLForOrigin(&prevCR) == activeSyncedURL || adminPreviouslyOwnedURL(&prevCR, activeSyncedURL) {
						condOrigin.uid = strings.TrimSpace(string(prevCR.UID))
						if condOrigin.dbHash == "" {
							condOrigin.dbHash = adminDBHashForURL(&prevCR, activeSyncedURL)
						}
					}
				}
			}
			return condOrigin
		}
		if ns != "" {
			var adminList xxljobv1alpha1.XxlJobAdminList
			if err := s.apiReader().List(ctx, &adminList, client.InNamespace(ns)); err == nil {
				for i := range adminList.Items {
					item := &adminList.Items[i]
					if resolveAdminURLForOrigin(item) == activeSyncedURL || adminPreviouslyOwnedURL(item, activeSyncedURL) {
						return adminOrigin{
							name:   item.Name,
							url:    activeSyncedURL,
							uid:    strings.TrimSpace(string(item.UID)),
							dbHash: adminDBHashForURL(item, activeSyncedURL),
						}
					}
				}
			}
		}
		return adminOrigin{url: activeSyncedURL}
	}

	if condOrigin := lookupConditionOrigin(activeSyncedURL); condOrigin.hasAny() {
		return condOrigin
	}
	return adminOrigin{
		name: specAdminName,
		url:  activeSyncedURL,
	}
}

func stampOriginatingAdminOrigin(job *batchv1.Job, origin adminOrigin) {
	if job == nil {
		return
	}
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}
	if origin.name != "" {
		job.Annotations[AnnotationAdminName] = origin.name
	}
	if origin.url != "" {
		job.Annotations[AnnotationAdminURL] = origin.url
	}
	if origin.uid != "" {
		job.Annotations[AnnotationAdminUID] = origin.uid
	}
	if origin.dbHash != "" {
		job.Annotations[AnnotationAdminDatasourceHash] = origin.dbHash
	}
}

func stampOriginatingAdminAnnotations(job *batchv1.Job, task *xxljobv1alpha1.XxlJobTask, execCR *xxljobv1alpha1.XxlJobExecutor) {
	var origin adminOrigin
	if execCR != nil {
		adminNS := execCR.Spec.AdminRef.Namespace
		if adminNS == "" || adminNS == execCR.Namespace {
			origin.name = strings.TrimSpace(execCR.Spec.AdminRef.Name)
		}
		origin.url = strings.TrimRight(strings.TrimSpace(execCR.Status.AdminURL), "/")
	}
	if origin.url == "" && task != nil {
		origin.url = strings.TrimRight(strings.TrimSpace(task.Status.AdminURL), "/")
	}
	stampOriginatingAdminOrigin(job, origin)
}

func sanitizeHeaderToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.ContainsAny(trimmed, "\r\n") {
		return trimmed
	}
	return strings.NewReplacer("\r", "", "\n", "").Replace(trimmed)
}

func applyFallbackGlueNonRootUser(podSpec *corev1.PodSpec) {
	if podSpec == nil {
		return
	}
	nonRootUID := int64(65534)
	podHasRunAsUser := podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsUser != nil
	podExplicitFalse := podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsNonRoot != nil && !*podSpec.SecurityContext.RunAsNonRoot
	if podExplicitFalse {
		podHasNonRootUID := podHasRunAsUser && *podSpec.SecurityContext.RunAsUser > 0
		if !podHasNonRootUID {
			for i := range podSpec.Containers {
				sc := podSpec.Containers[i].SecurityContext
				if sc != nil && sc.RunAsNonRoot != nil && *sc.RunAsNonRoot && sc.RunAsUser == nil {
					uid := nonRootUID
					sc.RunAsUser = &uid
				}
			}
		}
		return
	}
	if podHasRunAsUser {
		return
	}
	for i := range podSpec.Containers {
		sc := podSpec.Containers[i].SecurityContext
		if sc != nil && (sc.RunAsUser != nil || (sc.RunAsNonRoot != nil && !*sc.RunAsNonRoot)) {
			return
		}
	}
	if podSpec.SecurityContext == nil {
		podSpec.SecurityContext = &corev1.PodSecurityContext{}
	}
	podSpec.SecurityContext.RunAsUser = &nonRootUID
}

func extractJobAdminOrigin(j *batchv1.Job) adminOrigin {
	if j == nil || j.Annotations == nil {
		return adminOrigin{}
	}
	return adminOrigin{
		name:   strings.TrimSpace(j.Annotations[AnnotationAdminName]),
		url:    strings.TrimRight(strings.TrimSpace(j.Annotations[AnnotationAdminURL]), "/"),
		uid:    strings.TrimSpace(j.Annotations[AnnotationAdminUID]),
		dbHash: strings.TrimSpace(j.Annotations[AnnotationAdminDatasourceHash]),
	}
}

func jobHasAdminOrigin(j *batchv1.Job) bool {
	return extractJobAdminOrigin(j).hasAny()
}

func sameManagedAdminOrigin(a, b adminOrigin) bool {
	if a.name == "" || a.name != b.name {
		return false
	}
	if a.uid != "" && b.uid != "" && a.uid != b.uid {
		return false
	}
	if a.dbHash == "" || b.dbHash == "" || a.dbHash != b.dbHash {
		return false
	}
	return a.url != "" && b.url != ""
}

func sameAdminOrigin(a, b adminOrigin) bool {
	if !a.hasAny() || !b.hasAny() {
		return true
	}
	if a.uid != "" && b.uid != "" && a.uid != b.uid {
		return false
	}
	if a.name != "" && b.name != "" && a.name != b.name {
		return false
	}
	if a.dbHash != "" && b.dbHash != "" && a.dbHash != b.dbHash {
		return false
	}
	if sameManagedAdminOrigin(a, b) {
		return true
	}
	if a.url != "" && b.url != "" && a.url != b.url {
		return false
	}
	return (a.uid != "" && a.uid == b.uid) ||
		(a.url != "" && a.url == b.url) ||
		(a.name != "" && a.name == b.name) ||
		(a.dbHash != "" && a.dbHash == b.dbHash)
}

// SameSyncedAdminOrigin reports whether two recorded endpoint status pairs (ConditionAdminEndpointSynced + Status.AdminURL)
// belong to the same logical XXL-JOB Admin & database origin (including in-place managed Port/ContextPath/Hybrid URL edits,
// while distinguishing replaced Datasource.URLs and recreated Admin UIDs).
func SameSyncedAdminOrigin(condsA []metav1.Condition, urlA string, condsB []metav1.Condition, urlB string) bool {
	urlA = strings.TrimRight(strings.TrimSpace(urlA), "/")
	urlB = strings.TrimRight(strings.TrimSpace(urlB), "/")
	origA := extractSyncedEndpointOrigin(condsA, urlA)
	if !origA.hasAny() && urlA != "" {
		origA = adminOrigin{url: urlA}
	}
	origB := extractSyncedEndpointOrigin(condsB, urlB)
	if !origB.hasAny() && urlB != "" {
		origB = adminOrigin{url: urlB}
	}
	return sameAdminOrigin(origA, origB)
}

func jobMatchesExecutorOrigin(j *batchv1.Job, origin adminOrigin) bool {
	return sameAdminOrigin(extractJobAdminOrigin(j), origin)
}

func isSameOriginatingAdminJob(a, b *batchv1.Job) bool {
	if a == nil || b == nil {
		return true
	}
	return sameAdminOrigin(extractJobAdminOrigin(a), extractJobAdminOrigin(b))
}

func executorShortHash(executorName string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(executorName)))
	return hex.EncodeToString(sum[:3]) // 6 hex chars
}

func jobScopeShortHash(executorName, originKey string) string {
	trimmedOrigin := strings.TrimSpace(originKey)
	if trimmedOrigin == "" {
		return executorShortHash(executorName)
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(executorName) + "\x00" + trimmedOrigin))
	return hex.EncodeToString(sum[:4]) // 8 hex chars
}

func parseOriginScopedJobLogIDForJobID(jobName string, jobID int) (int64, bool) {
	parts := strings.Split(strings.TrimSpace(jobName), "-")
	if len(parts) != 4 || parts[0] != "xxljob" {
		return 0, false
	}
	if len(parts[1]) != 6 && len(parts[1]) != 8 {
		return 0, false
	}
	for i := 0; i < len(parts[1]); i++ {
		ch := parts[1][i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return 0, false
		}
	}
	if parts[2] != strconv.Itoa(jobID) {
		return 0, false
	}
	lid, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || lid <= 0 {
		return 0, false
	}
	return lid, true
}

func isJobSupersededByJob(candidateLogID int64, candidateJob, otherJob *batchv1.Job) bool {
	if otherJob == nil || isStaleOrphanSuspendedJob(otherJob) {
		return false
	}
	if !isSameOriginatingAdminJob(candidateJob, otherJob) {
		return false
	}
	succeeded, failed, _ := EvaluateJobCompletion(otherJob)
	completedNormally := succeeded || failed ||
		(otherJob.DeletionTimestamp.IsZero() && otherJob.Annotations != nil && otherJob.Annotations[AnnotationCallbackSent] == "true")
	activeNonAborted := otherJob.DeletionTimestamp.IsZero() && isJobRunning(otherJob) &&
		(otherJob.Annotations == nil || (otherJob.Annotations[AnnotationFailureMessage] == "" && otherJob.Annotations[AnnotationCallbackSent] != "true"))
	if !activeNonAborted && !completedNormally {
		return false
	}
	otherLogID := parseJobLogID(otherJob)
	if candidateLogID > 0 && otherLogID > 0 && otherLogID != candidateLogID {
		if candidateJob != nil && candidateJob.Annotations != nil && otherJob.Annotations != nil {
			candExec := strings.TrimSpace(candidateJob.Annotations[AnnotationExecutorName])
			otherExec := strings.TrimSpace(otherJob.Annotations[AnnotationExecutorName])
			if candExec != "" && otherExec != "" && candExec != otherExec {
				return false
			}
		}
		return otherLogID > candidateLogID
	}
	if activeNonAborted && candidateJob != nil && otherLogID == candidateLogID && otherJob.Name != candidateJob.Name {
		return otherJob.Name > candidateJob.Name
	}
	return false
}

func (s *Server) claimCoverEarlyWinner(
	ctx context.Context,
	initialTask *xxljobv1alpha1.XxlJobTask,
	executorName string,
	jobID int,
	logID int64,
	createdJob *batchv1.Job,
) (bool, error) {
	current := initialTask.DeepCopy()
	claimed := false
	basePrefix := fmt.Sprintf("xxljob-%s-%d-", executorShortHash(executorName), jobID)
	expectedPrefixes := []string{basePrefix}
	if createdJob != nil {
		if idx := strings.LastIndexByte(createdJob.Name, '-'); idx > 0 {
			candPrefix := createdJob.Name[:idx+1]
			if candPrefix != basePrefix {
				expectedPrefixes = append(expectedPrefixes, candPrefix)
			}
		}
		if origKey := extractJobAdminOrigin(createdJob).key(); origKey != "" {
			scopedPrefix := fmt.Sprintf("xxljob-%s-%d-", jobScopeShortHash(executorName, origKey), jobID)
			if scopedPrefix != basePrefix {
				expectedPrefixes = append(expectedPrefixes, scopedPrefix)
			}
		}
	}
	for attempt := 0; attempt < 4; attempt++ {
		if current.Status.LastExecutionJobName == createdJob.Name {
			claimed = true
			break
		}
		if current.Status.LastExecutionJobName != "" {
			var winnerJob batchv1.Job
			if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: current.Status.LastExecutionJobName}, &winnerJob); getErr == nil {
				if isJobSupersededByJob(logID, createdJob, &winnerJob) {
					break
				}
			} else if apierrors.IsNotFound(getErr) {
				// Even if the newer winner Job already completed and was garbage-collected by TTLSecondsAfterFinished,
				// parse its logId from LastExecutionJobName on the same originating Admin so a delayed out-of-order
				// COVER_EARLY request never executes after a newer trigger finished, while never mistaking a recreated
				// same-URL Admin CR (different UID) for the same logId sequence.
				jobOrigin := extractJobAdminOrigin(createdJob)
				taskAdminURL := strings.TrimRight(strings.TrimSpace(current.Status.AdminURL), "/")
				taskSyncedOrigin := extractSyncedEndpointOrigin(current.Status.Conditions, taskAdminURL)
				sameRecordedAdmin := true
				if taskSyncedOrigin.hasAny() && jobOrigin.hasAny() {
					sameRecordedAdmin = sameAdminOrigin(jobOrigin, taskSyncedOrigin)
				} else if jobOrigin.url != "" || taskAdminURL != "" {
					sameRecordedAdmin = jobOrigin.url != "" && taskAdminURL != "" && jobOrigin.url == taskAdminURL
				}
				if sameRecordedAdmin && logID > 0 {
					candidatePrefixes := expectedPrefixes
					if taskKey := taskSyncedOrigin.key(); taskKey != "" {
						taskScopedPrefix := fmt.Sprintf("xxljob-%s-%d-", jobScopeShortHash(executorName, taskKey), jobID)
						candidatePrefixes = append(candidatePrefixes, taskScopedPrefix)
					}
					supersededByGCDWinner := false
					for _, pfx := range candidatePrefixes {
						if strings.HasPrefix(current.Status.LastExecutionJobName, pfx) {
							if winnerLogID, parseErr := strconv.ParseInt(strings.TrimPrefix(current.Status.LastExecutionJobName, pfx), 10, 64); parseErr == nil && winnerLogID > logID {
								supersededByGCDWinner = true
								break
							}
						}
					}
					if !supersededByGCDWinner && jobOrigin.dbHash != "" {
						if winnerLogID, ok := parseOriginScopedJobLogIDForJobID(current.Status.LastExecutionJobName, jobID); ok && winnerLogID > logID {
							supersededByGCDWinner = true
						}
					}
					if supersededByGCDWinner {
						break
					}
				}
			} else {
				break
			}
		}
		current.Status.LastExecutionJobName = createdJob.Name
		err := s.K8sClient.Status().Update(ctx, current)
		if err == nil {
			claimed = true
			break
		}
		if !apierrors.IsConflict(err) {
			var latest xxljobv1alpha1.XxlJobTask
			if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: initialTask.Name}, &latest); getErr == nil &&
				latest.Status.LastExecutionJobName == createdJob.Name {
				claimed = true
			}
			break
		}
		var latest xxljobv1alpha1.XxlJobTask
		if getErr := s.apiReader().Get(ctx, client.ObjectKey{Namespace: initialTask.Namespace, Name: initialTask.Name}, &latest); getErr != nil {
			break
		}
		current = &latest
	}
	if !claimed {
		s.cleanupAbortedJob(ctx, createdJob)
		return false, nil
	}

	// Re-sweep active older Jobs after winning the CAS barrier so any concurrent lower-logId Job created
	// on another replica during the race window is terminated before createdJob is unsuspended (fixes EG2-11).
	superseded, err := s.killRunningJobsForCoverEarly(ctx, initialTask.Namespace, executorName, jobID, logID, createdJob)
	if err != nil || superseded {
		s.cleanupAbortedJob(ctx, createdJob)
		return false, err
	}
	return true, nil
}

func resolveGlueFallbackRuntime(glueType, script string, param xxlclient.TriggerParam) (string, []string, []string, error) {
	defaultArgs := []string{
		param.ExecutorParams,
		strconv.Itoa(param.BroadcastIndex),
		strconv.Itoa(param.BroadcastTotal),
	}
	switch strings.ToUpper(strings.TrimSpace(glueType)) {
	case "GLUE_SHELL":
		return "busybox:1.36", []string{"/bin/sh", "-c", script, "xxl-glue"}, defaultArgs, nil
	case "GLUE_PYTHON":
		return "python:3.11-slim", []string{"python3", "-c", script}, defaultArgs, nil
	case "GLUE_NODEJS":
		return "node:20-alpine", []string{"node", "-e", script, "xxl-glue"}, defaultArgs, nil
	case "GLUE_PHP":
		return "php:8.2-cli-alpine", []string{"php", "-r", script, "--"}, defaultArgs, nil
	default:
		return "", nil, nil, fmt.Errorf("unsupported glueType %q without explicit k8sJobTemplate.image", glueType)
	}
}

func (s *Server) buildBatchJob(param xxlclient.TriggerParam, task *xxljobv1alpha1.XxlJobTask) (*batchv1.Job, error) {
	return s.buildBatchJobWithOrigin(param, task, adminOrigin{})
}

func (s *Server) buildBatchJobWithOrigin(param xxlclient.TriggerParam, task *xxljobv1alpha1.XxlJobTask, origin adminOrigin) (*batchv1.Job, error) {
	if task == nil {
		return nil, fmt.Errorf("XxlJobTask CR is required to build a Kubernetes batch Job")
	}
	namespace := task.Namespace
	if namespace == "" {
		namespace = s.DefaultNamespace
	}

	var tpl xxljobv1alpha1.K8sJobTemplateSpec
	if task.Spec.K8sJobTemplate != nil {
		tpl = *task.Spec.K8sJobTemplate.DeepCopy()
	}

	// Select interpreter, image, and positional argv ($1=params, $2=shardIndex, $3=shardTotal) by GlueType
	// when k8sJobTemplate.image is omitted (fixes R5-P2-3 & R6-P2-4).
	if tpl.Image == "" {
		glueType := strings.ToUpper(strings.TrimSpace(task.Spec.GlueType))
		if strings.HasPrefix(glueType, "GLUE_") && task.Spec.GlueSource != "" {
			img, cmd, defArgs, err := resolveGlueFallbackRuntime(glueType, task.Spec.GlueSource, param)
			if err != nil {
				return nil, err
			}
			tpl.Image = img
			if len(tpl.Command) == 0 {
				tpl.Command = cmd
			}
			if len(tpl.Args) == 0 {
				tpl.Args = defArgs
			}
		} else {
			return nil, fmt.Errorf("no k8sJobTemplate.image configured on XxlJobTask %s/%s (jobId=%d)", task.Namespace, task.Name, param.JobID)
		}
	}

	restartPolicy := tpl.RestartPolicy
	if restartPolicy == "" {
		restartPolicy = corev1.RestartPolicyNever
	}
	backoffLimit := int32(0)
	if tpl.BackoffLimit != nil {
		backoffLimit = *tpl.BackoffLimit
	}
	ttl := int32(3600)
	if tpl.TTLSecondsAfterFinished != nil {
		ttl = *tpl.TTLSecondsAfterFinished
	}

	var activeDeadline *int64
	if tpl.ActiveDeadlineSeconds != nil {
		activeDeadline = tpl.ActiveDeadlineSeconds
	} else if param.ExecutorTimeout > 0 {
		sec := int64(param.ExecutorTimeout)
		activeDeadline = &sec
	} else if task.Spec.ExecutorTimeout > 0 {
		sec := int64(task.Spec.ExecutorTimeout)
		activeDeadline = &sec
	} else {
		defDeadline := DefaultJobActiveDeadlineSeconds
		activeDeadline = &defDeadline
	}

	logID := param.LogID
	if logID <= 0 {
		logID = time.Now().UnixNano()
	}

	authoritativeEnv := []corev1.EnvVar{
		{Name: "XXL_JOB_ID", Value: strconv.Itoa(param.JobID)},
		{Name: "XXL_JOB_LOG_ID", Value: strconv.FormatInt(logID, 10)},
		{Name: "XXL_JOB_HANDLER", Value: param.ExecutorHandler},
		{Name: "XXL_JOB_PARAM", Value: param.ExecutorParams},
		{Name: "XXL_JOB_SHARDING_INDEX", Value: strconv.Itoa(param.BroadcastIndex)},
		{Name: "XXL_JOB_SHARDING_TOTAL", Value: strconv.Itoa(param.BroadcastTotal)},
	}
	reservedEnvNames := make(map[string]bool, len(authoritativeEnv))
	env := make([]corev1.EnvVar, 0, len(authoritativeEnv)+len(tpl.Env))
	for _, ae := range authoritativeEnv {
		reservedEnvNames[ae.Name] = true
		env = append(env, ae)
	}
	for _, ue := range tpl.Env {
		if !reservedEnvNames[ue.Name] {
			env = append(env, ue)
		}
	}

	execName := task.Spec.ExecutorRef.Name
	labels := map[string]string{
		LabelManagedBy:       ManagedByValue,
		LabelExecutorName:    SanitizeLabel(execName),
		LabelJobID:           strconv.Itoa(param.JobID),
		LabelLogID:           strconv.FormatInt(logID, 10),
		LabelExecutorHandler: SanitizeLabel(param.ExecutorHandler),
	}

	podLabels := make(map[string]string, len(tpl.PodLabels)+len(labels))
	for k, v := range tpl.PodLabels {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "xxljob.io/") {
			continue
		}
		podLabels[k] = v
	}
	for k, v := range labels {
		podLabels[k] = v
	}

	var podAnnotations map[string]string
	for k, v := range tpl.PodAnnotations {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), "xxljob.io/") {
			continue
		}
		if podAnnotations == nil {
			podAnnotations = make(map[string]string, len(tpl.PodAnnotations))
		}
		podAnnotations[k] = v
	}

	annotations := map[string]string{
		AnnotationTaskNamespace: task.Namespace,
		AnnotationTaskName:      task.Name,
		AnnotationExecutorName:  execName,
		AnnotationLogID:         strconv.FormatInt(logID, 10),
		AnnotationLogDateTime:   strconv.FormatInt(param.LogDateTime, 10),
		AnnotationCallbackSent:  "false",
	}

	// Include a short hash of (executorName, originating Admin identity) so Jobs from two different executors
	// or migrated/recreated Admins in the same namespace with identical (jobId, logId) never collide (fixes P1-4).
	jobName := BuildJobName(execName, param.JobID, logID, origin.key())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
			Finalizers:  []string{JobCallbackFinalizer},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   activeDeadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:           strings.TrimSpace(tpl.ServiceAccountName),
					AutomountServiceAccountToken: tpl.AutomountServiceAccountToken,
					RestartPolicy:                restartPolicy,
					SecurityContext:              tpl.PodSecurityContext,
					ImagePullSecrets:             tpl.ImagePullSecrets,
					NodeSelector:                 tpl.NodeSelector,
					Tolerations:                  tpl.Tolerations,
					Affinity:                     tpl.Affinity,
					Volumes:                      tpl.Volumes,
					Containers: []corev1.Container{
						{
							Name:            "job-runner",
							Image:           tpl.Image,
							ImagePullPolicy: tpl.ImagePullPolicy,
							Command:         tpl.Command,
							Args:            tpl.Args,
							Env:             env,
							Resources:       tpl.Resources,
							SecurityContext: tpl.SecurityContext,
							VolumeMounts:    tpl.VolumeMounts,
						},
					},
				},
			},
		},
	}
	return job, nil
}

// BuildJobName generates a deterministic Kubernetes Job name scoped to (executorName, adminOrigin, jobID, logID).
func BuildJobName(executorName string, jobID int, logID int64, adminOrigin ...string) string {
	originKey := ""
	if len(adminOrigin) > 0 {
		originKey = adminOrigin[0]
	}
	return fmt.Sprintf("xxljob-%s-%d-%d", jobScopeShortHash(executorName, originKey), jobID, logID)
}

func (s *Server) hasRunningJob(ctx context.Context, namespace, executorName string, jobID int, excludeJobName string) (bool, error) {
	return s.hasRunningJobForOrigin(ctx, namespace, executorName, jobID, excludeJobName, adminOrigin{})
}

func (s *Server) hasRunningJobForOrigin(ctx context.Context, namespace, executorName string, jobID int, excludeJobName string, origin adminOrigin) (bool, error) {
	matchLabels := client.MatchingLabels{
		LabelManagedBy: ManagedByValue,
		LabelJobID:     strconv.Itoa(jobID),
	}
	if executorName != "" {
		matchLabels[LabelExecutorName] = sanitizeLabel(executorName)
	}
	opts := []client.ListOption{
		client.InNamespace(namespace),
		matchLabels,
	}
	var jobList batchv1.JobList
	if err := s.apiReader().List(ctx, &jobList, opts...); err != nil {
		return false, err
	}
	for i := range jobList.Items {
		j := &jobList.Items[i]
		if excludeJobName != "" && j.Name == excludeJobName {
			continue
		}
		if !matchesExecutorJobAnnotation(j, executorName) || !jobMatchesExecutorOrigin(j, origin) {
			continue
		}
		if isJobBlockingExecution(j) {
			return true, nil
		}
	}
	return false, nil
}

func matchesExecutorJobAnnotation(job *batchv1.Job, executorName string) bool {
	if job == nil || executorName == "" || job.Annotations == nil {
		return true
	}
	execAnnot := strings.TrimSpace(job.Annotations[AnnotationExecutorName])
	return execAnnot == "" || execAnnot == executorName
}

func isJobBlockingExecution(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	if !job.DeletionTimestamp.IsZero() || !isJobRunning(job) {
		return HasActiveOrTerminatingPods(job) && isCompletedJobStillDrainingPods(job)
	}
	if HasActiveOrTerminatingPods(job) {
		return true
	}
	if isStaleOrphanSuspendedJob(job) {
		return false
	}
	if job.Annotations != nil && (job.Annotations[AnnotationFailureMessage] != "" || job.Annotations[AnnotationCallbackSent] == "true") {
		return false
	}
	return true
}

// HasActiveOrTerminatingPods reports whether a batch/v1.Job still has active or terminating Pods,
// including deleting Jobs that still hold the foregroundDeletion finalizer while GarbageCollector waits
// for dependent Pods to terminate and be removed (fixes R8-P1-1).
func HasActiveOrTerminatingPods(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	if job.Status.Active > 0 || (job.Status.Terminating != nil && *job.Status.Terminating > 0) {
		return true
	}
	if !job.DeletionTimestamp.IsZero() {
		for _, f := range job.Finalizers {
			if f == metav1.FinalizerDeleteDependents {
				return true
			}
		}
	}
	return false
}

func hasActiveOrTerminatingPods(job *batchv1.Job) bool {
	return HasActiveOrTerminatingPods(job)
}

func isStaleOrphanSuspendedJob(job *batchv1.Job) bool {
	if job == nil || job.Spec.Suspend == nil || !*job.Spec.Suspend {
		return false
	}
	if job.Annotations == nil {
		return false
	}
	bs := strings.ToUpper(strings.TrimSpace(job.Annotations[AnnotationBlockStrategy]))
	if bs == "" || bs == "SERIAL_EXECUTION" {
		return false
	}
	return !job.CreationTimestamp.IsZero() && time.Since(job.CreationTimestamp.Time) > 30*time.Second
}

// killRunningJobs suspends and deletes ONLY running or suspended (incomplete) Jobs for (namespace, executorName, jobID),
// preserving already-completed historical Jobs so their logs and pending callbacks are not lost (fixes R4-P1-2, R6-P2-1, R8-P1-2, R8-P2-6).
func (s *Server) killRunningJobs(ctx context.Context, namespace, executorName string, jobID int, excludeJobName string) error {
	_, err := s.killRunningJobsInternal(ctx, namespace, executorName, jobID, 0, nil, excludeJobName, adminOrigin{})
	return err
}

func (s *Server) killRunningJobsForOrigin(ctx context.Context, namespace, executorName string, jobID int, excludeJobName string, origin adminOrigin) error {
	_, err := s.killRunningJobsInternal(ctx, namespace, executorName, jobID, 0, nil, excludeJobName, origin)
	return err
}

func (s *Server) killRunningJobsForCoverEarly(
	ctx context.Context,
	namespace, executorName string,
	jobID int,
	candidateLogID int64,
	candidateJob *batchv1.Job,
) (bool, error) {
	excludeName := ""
	var origin adminOrigin
	if candidateJob != nil {
		excludeName = candidateJob.Name
		origin = extractJobAdminOrigin(candidateJob)
	}
	return s.killRunningJobsInternal(ctx, namespace, executorName, jobID, candidateLogID, candidateJob, excludeName, origin)
}

func (s *Server) killRunningJobsInternal(
	ctx context.Context,
	namespace, executorName string,
	jobID int,
	candidateLogID int64,
	candidateJob *batchv1.Job,
	excludeJobName string,
	origin adminOrigin,
) (bool, error) {
	killCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()

	matchLabels := client.MatchingLabels{
		LabelManagedBy: ManagedByValue,
		LabelJobID:     strconv.Itoa(jobID),
	}
	if executorName != "" {
		matchLabels[LabelExecutorName] = sanitizeLabel(executorName)
	}
	opts := []client.ListOption{
		client.InNamespace(namespace),
		matchLabels,
	}
	var jobList batchv1.JobList
	if err := s.apiReader().List(killCtx, &jobList, opts...); err != nil {
		return false, err
	}
	if candidateJob != nil {
		for i := range jobList.Items {
			job := &jobList.Items[i]
			if excludeJobName != "" && job.Name == excludeJobName {
				continue
			}
			if !matchesExecutorJobAnnotation(job, executorName) || !jobMatchesExecutorOrigin(job, origin) {
				continue
			}
			if isJobSupersededByJob(candidateLogID, candidateJob, job) {
				return true, nil
			}
		}
	}

	// Use Foreground deletion so Kubernetes GarbageCollector immediately terminates active/pending Job Pods
	// via foregroundDeletion even while JobCallbackFinalizer (xxljob.io/callback-guard) retains the Job
	// until Pods exit and callback delivery completes.
	policy := metav1.DeletePropagationForeground
	remainingOtherSuspended := 0
	for i := range jobList.Items {
		job := &jobList.Items[i]
		if excludeJobName != "" && job.Name == excludeJobName {
			continue
		}
		if !matchesExecutorJobAnnotation(job, executorName) {
			continue
		}
		if !jobMatchesExecutorOrigin(job, origin) {
			if job.DeletionTimestamp.IsZero() && isJobRunning(job) && job.Spec.Suspend != nil && *job.Spec.Suspend {
				if job.Annotations == nil || ((job.Annotations[AnnotationFailureMessage] == "" && job.Annotations[AnnotationCallbackSent] != "true") &&
					(job.Annotations[AnnotationBlockStrategy] == "" || job.Annotations[AnnotationBlockStrategy] == "SERIAL_EXECUTION")) {
					remainingOtherSuspended++
				}
			}
			continue
		}
		if !job.DeletionTimestamp.IsZero() || !isJobRunning(job) {
			continue
		}
		if job.Spec.Suspend == nil || !*job.Spec.Suspend {
			patch := client.MergeFrom(job.DeepCopy())
			suspend := true
			job.Spec.Suspend = &suspend
			_ = s.K8sClient.Patch(killCtx, job, patch)
		}
		if err := s.K8sClient.Delete(killCtx, job, &client.DeleteOptions{
			PropagationPolicy: &policy,
		}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if OnSerialQueueDepth != nil {
		OnSerialQueueDepth(namespace, sanitizeLabel(executorName), strconv.Itoa(jobID), float64(remainingOtherSuspended))
	}
	return false, nil
}

func isJobRunning(job *batchv1.Job) bool {
	s, f, _ := EvaluateJobCompletion(job)
	return !s && !f
}

// IsJobDeadlineExceeded returns true if the Kubernetes Job failed specifically due to ActiveDeadlineSeconds timeout (fixes R6-P2-5).
func IsJobDeadlineExceeded(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue && cond.Reason == "DeadlineExceeded" {
			return true
		}
	}
	return false
}

// EvaluateJobCompletion inspects a Kubernetes batch/v1.Job to determine if it has succeeded or failed.
func EvaluateJobCompletion(job *batchv1.Job) (succeeded bool, failed bool, msg string) {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return true, false, fmt.Sprintf("K8s Job %s/%s completed successfully", job.Namespace, job.Name)
		}
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return false, true, fmt.Sprintf("K8s Job %s/%s failed: %s (%s)", job.Namespace, job.Name, cond.Reason, cond.Message)
		}
	}
	if job.Status.Succeeded > 0 {
		return true, false, fmt.Sprintf("K8s Job %s/%s succeeded", job.Namespace, job.Name)
	}
	if job.Status.Failed > 0 && (job.Spec.BackoffLimit == nil || job.Status.Failed > *job.Spec.BackoffLimit) {
		return false, true, fmt.Sprintf("K8s Job %s/%s exceeded backoff limit", job.Namespace, job.Name)
	}
	return false, false, ""
}

// SanitizeLabel normalizes a string into a valid Kubernetes label value (<= 63 chars).
// When the input exceeds 63 chars or contains special characters that get replaced, a 6-char SHA256 suffix
// is appended so two distinct names never collide on the same sanitized label value (fixes R5-P3).
func SanitizeLabel(s string) string {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return "default"
	}
	var b strings.Builder
	modified := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
			modified = true
		}
	}
	cleaned := strings.Trim(b.String(), "-_.")
	if cleaned == "" {
		cleaned = "default"
		modified = true
	}
	if len(cleaned) <= 63 && !modified && cleaned == raw {
		return cleaned
	}
	sum := sha256.Sum256([]byte(raw))
	suffix := hex.EncodeToString(sum[:3]) // 6 hex chars
	prefix := cleaned
	if len(prefix) > 56 {
		prefix = strings.TrimRight(prefix[:56], "-_.")
	}
	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func sanitizeLabel(s string) string {
	return SanitizeLabel(s)
}

func writeReturnT(w http.ResponseWriter, code int, msg string, content any) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	var rawContent json.RawMessage
	if content != nil {
		rawContent, _ = json.Marshal(content)
	}
	_ = json.NewEncoder(w).Encode(xxlclient.ReturnT{
		Code:    code,
		Msg:     msg,
		Content: rawContent,
	})
}
