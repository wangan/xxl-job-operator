package controller

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
)

var (
	// RegistryHeartbeatErrorsTotal counts failed /api/registry or /api/registryRemove calls to XXL-JOB Admin.
	RegistryHeartbeatErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "xxljob_registry_heartbeat_errors_total",
			Help: "Total number of failed executor registry heartbeat or removal requests to XXL-JOB Admin.",
		},
		[]string{"namespace", "executor"},
	)

	// CallbackPendingJobs tracks the number of completed/deleting K8s Jobs awaiting /api/callback delivery.
	CallbackPendingJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "xxljob_callback_pending_jobs",
			Help: "Number of Kubernetes batch Jobs currently awaiting /api/callback delivery to XXL-JOB Admin.",
		},
		[]string{"namespace", "executor"},
	)

	// SerialQueueDepth tracks the number of suspended K8s Jobs waiting in a SERIAL_EXECUTION queue.
	SerialQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "xxljob_serial_queue_depth",
			Help: "Number of suspended Kubernetes batch Jobs queued under SERIAL_EXECUTION.",
		},
		[]string{"namespace", "executor", "job_id"},
	)

	metricsMu               sync.Mutex
	pendingCallbackJobs     = make(map[string]string)
	activeSerialQueueSeries = make(map[string][3]string)
)

func markCallbackPending(namespace, executorLabel, jobName string) {
	key := fmt.Sprintf("%s/%s", namespace, jobName)
	metricsMu.Lock()
	defer metricsMu.Unlock()
	if _, exists := pendingCallbackJobs[key]; !exists {
		pendingCallbackJobs[key] = executorLabel
		CallbackPendingJobs.WithLabelValues(namespace, executorLabel).Inc()
	}
}

func clearCallbackPending(namespace, executorLabel, jobName string) {
	key := fmt.Sprintf("%s/%s", namespace, jobName)
	metricsMu.Lock()
	defer metricsMu.Unlock()
	if prevExec, exists := pendingCallbackJobs[key]; exists {
		delete(pendingCallbackJobs, key)
		if prevExec != "" {
			executorLabel = prevExec
		}
		CallbackPendingJobs.WithLabelValues(namespace, executorLabel).Dec()
	}
}

func clearCallbackPendingByJobKey(namespace, jobName string) {
	key := fmt.Sprintf("%s/%s", namespace, jobName)
	metricsMu.Lock()
	defer metricsMu.Unlock()
	if prevExec, exists := pendingCallbackJobs[key]; exists {
		delete(pendingCallbackJobs, key)
		CallbackPendingJobs.WithLabelValues(namespace, prevExec).Dec()
	}
}

func clearSerialQueueDepth(namespace, executorLabel, jobIDLabel string) {
	seriesKey := fmt.Sprintf("%s/%s/%s", namespace, executorLabel, jobIDLabel)
	metricsMu.Lock()
	defer metricsMu.Unlock()
	delete(activeSerialQueueSeries, seriesKey)
	SerialQueueDepth.DeleteLabelValues(namespace, executorLabel, jobIDLabel)
}

func activeSerialQueueExecutorsForJobID(namespace, jobIDLabel string) []string {
	metricsMu.Lock()
	defer metricsMu.Unlock()
	var executors []string
	for _, labels := range activeSerialQueueSeries {
		if labels[0] == namespace && labels[2] == jobIDLabel {
			executors = append(executors, labels[1])
		}
	}
	return executors
}

func cleanupExecutorMetrics(namespace, executorName string) {
	execLabel := proxyexecutor.SanitizeLabel(executorName)
	nsPrefix := namespace + "/"

	metricsMu.Lock()
	defer metricsMu.Unlock()

	RegistryHeartbeatErrorsTotal.DeleteLabelValues(namespace, executorName)
	for key, prevExec := range pendingCallbackJobs {
		if len(key) > len(nsPrefix) && key[:len(nsPrefix)] == nsPrefix && (prevExec == execLabel || prevExec == executorName) {
			delete(pendingCallbackJobs, key)
		}
	}
	CallbackPendingJobs.DeleteLabelValues(namespace, execLabel)
	if execLabel != executorName {
		CallbackPendingJobs.DeleteLabelValues(namespace, executorName)
	}

	for seriesKey, labels := range activeSerialQueueSeries {
		if labels[0] == namespace && (labels[1] == execLabel || labels[1] == executorName) {
			delete(activeSerialQueueSeries, seriesKey)
			SerialQueueDepth.DeleteLabelValues(labels[0], labels[1], labels[2])
		}
	}
}

func init() {
	metrics.Registry.MustRegister(
		RegistryHeartbeatErrorsTotal,
		CallbackPendingJobs,
		SerialQueueDepth,
	)
	proxyexecutor.OnSerialQueueDepth = func(namespace, executorLabel, jobIDLabel string, depth float64) {
		seriesKey := fmt.Sprintf("%s/%s/%s", namespace, executorLabel, jobIDLabel)
		metricsMu.Lock()
		defer metricsMu.Unlock()
		if depth <= 0 {
			if _, exists := activeSerialQueueSeries[seriesKey]; exists {
				delete(activeSerialQueueSeries, seriesKey)
				SerialQueueDepth.DeleteLabelValues(namespace, executorLabel, jobIDLabel)
			}
			return
		}
		activeSerialQueueSeries[seriesKey] = [3]string{namespace, executorLabel, jobIDLabel}
		SerialQueueDepth.WithLabelValues(namespace, executorLabel, jobIDLabel).Set(depth)
	}
}
