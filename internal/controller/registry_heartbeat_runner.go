package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/pkg/xxlclient"
)

// RegistryHeartbeatRunner is a dedicated leader-elected background Runnable that decouples
// periodic /api/registry heartbeats from single-item Reconcile loops (fixes R5-P1-2 & R6-P1-10).
// It concurrently refreshes all active (Status.RegisteredAddresses) and terminating-keepalive
// (Status.RegistryKeepAliveAddresses) executor addresses with per-Admin grouping, circuit breaking,
// bounded concurrency, and short per-request timeouts so a slow Admin never starves healthy Admins.
type RegistryHeartbeatRunner struct {
	Client         client.Client
	APIReader      client.Reader
	ClientFactory  ClientFactory
	Interval       time.Duration
	RequestTimeout time.Duration
	MaxConcurrency int
}

// NeedLeaderElection returns true so only the active leader sends periodic registry heartbeats.
func (r *RegistryHeartbeatRunner) NeedLeaderElection() bool {
	return true
}

// Start runs the periodic heartbeat loop until ctx is cancelled.
func (r *RegistryHeartbeatRunner) Start(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = 20 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_ = r.RunOnce(ctx)
		}
	}
}

type heartbeatTarget struct {
	execKey types.NamespacedName
	execCR  xxljobv1alpha1.XxlJobExecutor
	address string
}

type adminHeartbeatGroup struct {
	adminRef  xxljobv1alpha1.LocalObjectReference
	namespace string
	targets   []heartbeatTarget
}

// RunOnce performs one concurrent pass of /api/registry heartbeats across all XxlJobExecutors.
func (r *RegistryHeartbeatRunner) RunOnce(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("registry-heartbeat")
	var execList xxljobv1alpha1.XxlJobExecutorList
	if err := r.Client.List(ctx, &execList); err != nil {
		return err
	}

	groups := make(map[string]*adminHeartbeatGroup)
	var groupOrder []string

	for i := range execList.Items {
		execCR := execList.Items[i]
		if !execCR.DeletionTimestamp.IsZero() || !execCR.Status.Synced || !isExecutorAppNameSynced(&execCR) {
			continue
		}
		if !shouldHeartbeatExecutorRegistry(&execCR) {
			continue
		}
		if execCR.Spec.Mode != xxljobv1alpha1.ExecutorModeK8sJobProxy &&
			execCR.Spec.UseServiceDNS && strings.TrimSpace(execCR.Spec.ServiceName) != "" {
			readyCnt, keepAliveCnt, backendErr := evaluateServiceDNSBackends(ctx, r.Client, &execCR)
			addrType := resolveExecutorAddressType(&execCR)
			if backendErr == nil && readyCnt == 0 && (addrType != 1 || keepAliveCnt == 0) {
				continue
			}
		}
		addrSet := make(map[string]bool)
		for _, addr := range execCR.Status.RegisteredAddresses {
			if addr != "" {
				addrSet[addr] = true
			}
		}
		// Only heartbeat draining pods explicitly recorded in Status.RegistryKeepAliveAddresses
		// (Terminating + PodRunning under addressType=1), never re-registering addressType=0 drained pods (fixes R6-P1-10).
		for _, addr := range execCR.Status.RegistryKeepAliveAddresses {
			if addr != "" {
				addrSet[addr] = true
			}
		}
		if len(addrSet) == 0 {
			continue
		}

		// Enforce same-namespace adminRef boundary (fixes R7-P2-11)
		if execCR.Spec.AdminRef.Namespace != "" && execCR.Spec.AdminRef.Namespace != execCR.Namespace {
			continue
		}
		groupKey := fmt.Sprintf("%s/%s", execCR.Namespace, execCR.Spec.AdminRef.Name)
		grp, exists := groups[groupKey]
		if !exists {
			grp = &adminHeartbeatGroup{
				adminRef:  execCR.Spec.AdminRef,
				namespace: execCR.Namespace,
			}
			groups[groupKey] = grp
			groupOrder = append(groupOrder, groupKey)
		}

		execKey := types.NamespacedName{Namespace: execCR.Namespace, Name: execCR.Name}
		for addr := range addrSet {
			grp.targets = append(grp.targets, heartbeatTarget{
				execKey: execKey,
				execCR:  execCR,
				address: addr,
			})
		}
	}

	if len(groupOrder) == 0 {
		return nil
	}

	maxConc := r.MaxConcurrency
	if maxConc <= 0 {
		maxConc = 16
	}
	reqTimeout := r.RequestTimeout
	if reqTimeout <= 0 {
		reqTimeout = 3 * time.Second
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup

	var errMu sync.Mutex
	execErrors := make(map[types.NamespacedName][]string)
	execSeen := make(map[types.NamespacedName]xxljobv1alpha1.XxlJobExecutor)

	recordResult := func(t heartbeatTarget, err error) {
		errMu.Lock()
		defer errMu.Unlock()
		execSeen[t.execKey] = t.execCR
		if err != nil {
			execErrors[t.execKey] = append(execErrors[t.execKey], err.Error())
		}
	}

	for _, groupKey := range groupOrder {
		grp := groups[groupKey]
		resolveCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		adminCR, xxlCli, err := ResolveAdminAndClientWithReader(resolveCtx, r.Client, reader, r.ClientFactory, grp.adminRef, grp.namespace)
		cancel()
		if err != nil {
			for _, t := range grp.targets {
				RegistryHeartbeatErrorsTotal.WithLabelValues(t.execCR.Namespace, t.execCR.Name).Inc()
				recordResult(t, err)
			}
			logger.V(1).Info("Resolve admin failed during heartbeat", "adminGroup", groupKey, "err", err)
			continue
		}
		resolvedAdminURL := strings.TrimRight(ResolveAdminURL(adminCR), "/")

		// Per-Admin circuit breaker: if >= 5 consecutive requests to the same Admin fail in this cycle,
		// trip and fail-fast remaining targets for this Admin so a hanging Admin cannot monopolize global
		// worker slots, while a single success resets the consecutive failure counter (fixes R6-P1-10 & R7-P1-7).
		const adminCircuitBreakerThreshold = 5
		var adminFailCount atomic.Int32
		cli := xxlCli

		for _, t := range grp.targets {
			execMeta := extractSyncedAdminEndpointMeta(t.execCR.Status.Conditions)
			if isAdminInstanceOrDBReplaced(adminCR, execMeta) {
				continue
			}
			if t.execCR.Status.AdminURL != "" &&
				strings.TrimRight(t.execCR.Status.AdminURL, "/") != resolvedAdminURL &&
				!isSameManagedAdminDBUpdate(adminCR, t.execCR.Status.AdminURL, execMeta) {
				continue
			}
			target := t
			wg.Add(1)
			go func(c *xxlclient.Client) {
				defer wg.Done()
				if adminFailCount.Load() >= adminCircuitBreakerThreshold {
					RegistryHeartbeatErrorsTotal.WithLabelValues(target.execCR.Namespace, target.execCR.Name).Inc()
					recordResult(target, fmt.Errorf("circuit breaker open for unreachable admin %s", groupKey))
					return
				}

				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-sem }()

				if adminFailCount.Load() >= adminCircuitBreakerThreshold {
					RegistryHeartbeatErrorsTotal.WithLabelValues(target.execCR.Namespace, target.execCR.Name).Inc()
					recordResult(target, fmt.Errorf("circuit breaker open for unreachable admin %s", groupKey))
					return
				}

				hbCtx, hbCancel := context.WithTimeout(ctx, reqTimeout)
				defer hbCancel()

				if regErr := c.RegisterExecutor(hbCtx, target.execCR.Spec.AppName, target.address); regErr != nil {
					adminFailCount.Add(1)
					RegistryHeartbeatErrorsTotal.WithLabelValues(target.execCR.Namespace, target.execCR.Name).Inc()
					recordResult(target, regErr)
					logger.V(1).Info("Heartbeat RegisterExecutor failed", "executor", target.execCR.Name, "address", target.address, "err", regErr)
					return
				}
				adminFailCount.Store(0)
				recordResult(target, nil)
			}(cli)
		}
	}

	wg.Wait()

	// Synchronize RegistryHealthy condition on XxlJobExecutor.Status when transition occurs (fixes R6-P2-6)
	for execKey, snap := range execSeen {
		errs := execErrors[execKey]
		healthy := len(errs) == 0
		prevCond := meta.FindStatusCondition(snap.Status.Conditions, "RegistryHealthy")
		needUpdate := prevCond == nil ||
			(healthy && prevCond.Status != metav1.ConditionTrue) ||
			(!healthy && prevCond.Status != metav1.ConditionFalse)
		if !needUpdate {
			continue
		}
		var latest xxljobv1alpha1.XxlJobExecutor
		if err := r.Client.Get(ctx, execKey, &latest); err != nil {
			continue
		}
		now := metav1.Now()
		if healthy {
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               "RegistryHealthy",
				Status:             metav1.ConditionTrue,
				Reason:             "HeartbeatSucceeded",
				Message:            "All executor registry heartbeats succeeded",
				LastTransitionTime: now,
			})
		} else {
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               "RegistryHealthy",
				Status:             metav1.ConditionFalse,
				Reason:             "HeartbeatFailed",
				Message:            strings.Join(errs, "; "),
				LastTransitionTime: now,
			})
		}
		_ = r.Client.Status().Update(ctx, &latest)
	}

	return nil
}
