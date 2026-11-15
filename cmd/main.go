package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	xxljobv1alpha1 "github.com/xxl-job/xxl-job-operator/api/v1alpha1"
	"github.com/xxl-job/xxl-job-operator/internal/controller"
	"github.com/xxl-job/xxl-job-operator/internal/webhook"
	"github.com/xxl-job/xxl-job-operator/pkg/proxyexecutor"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(xxljobv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var proxyBindAddr string
	var proxyServiceURL string
	var proxyAccessToken string
	var defaultNamespace string
	var watchNamespaces string
	var enableWebhooks bool
	var sidecarImage string
	var webhookDefaultAccessToken string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&proxyBindAddr, "proxy-executor-bind-address", ":9999", "The address the built-in K8sJobProxy Executor binds to.")
	flag.StringVar(&proxyServiceURL, "proxy-executor-service-url", "http://xxl-job-operator-proxy.xxl-job-system.svc.cluster.local:9999/", "The cluster-internal Service URL for K8sJobProxy Executor.")
	flag.StringVar(&proxyAccessToken, "proxy-executor-access-token", "", "Optional single-tenant gateway XXL-JOB-ACCESS-TOKEN verified by K8sJobProxy Executor in addition to per-executor Admin token.")
	flag.StringVar(&defaultNamespace, "default-namespace", "default", "Default namespace for K8s batch Jobs when task namespace is unspecified.")
	flag.StringVar(&watchNamespaces, "watch-namespaces", "", "Optional comma-separated list of namespaces to watch (empty = all namespaces).")
	flag.BoolVar(&enableWebhooks, "enable-webhooks", false, "Enable Pod MutatingAdmissionWebhook for Resident and Sidecar injection.")
	flag.StringVar(&sidecarImage, "sidecar-image", "ghcr.io/xxl-job/xxl-job-operator-sidecar:latest", "Sidecar container image injected in Sidecar mode.")
	flag.StringVar(&webhookDefaultAccessToken, "webhook-default-access-token", "", "Optional fallback non-default XXL-JOB-ACCESS-TOKEN injected by Pod MutatingWebhook when no Secret ref is configured.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Restrict batch/v1.Job informer cache by managed-by label and strip heavy fields from cached Pods (fixes P1-5 & R4-P1-4).
	cacheOpts := cache.Options{
		DefaultTransform: cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&batchv1.Job{}: {
				Label: labels.SelectorFromSet(labels.Set{
					proxyexecutor.LabelManagedBy: proxyexecutor.ManagedByValue,
				}),
			},
			&corev1.Pod{}: {
				Transform: controller.TransformPodForCache,
			},
		},
	}
	if strings.TrimSpace(watchNamespaces) != "" {
		nsMap := make(map[string]cache.Config)
		for _, ns := range strings.Split(watchNamespaces, ",") {
			if trimmed := strings.TrimSpace(ns); trimmed != "" {
				nsMap[trimmed] = cache.Config{}
			}
		}
		if len(nsMap) > 0 {
			cacheOpts.DefaultNamespaces = nsMap
		}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cacheOpts,
		Client:                        client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}},
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              "xxl-job-operator.xxljob.io",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	if err = (&controller.XxlJobAdminReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "XxlJobAdmin")
		os.Exit(1)
	}

	if err = (&controller.XxlJobExecutorReconciler{
		Client:                   mgr.GetClient(),
		APIReader:                mgr.GetAPIReader(),
		Scheme:                   mgr.GetScheme(),
		DefaultProxyExecutorAddr: proxyServiceURL,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "XxlJobExecutor")
		os.Exit(1)
	}

	if err = (&controller.XxlJobTaskReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "XxlJobTask")
		os.Exit(1)
	}

	if err = (&controller.K8sJobCallbackReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "K8sJobCallback")
		os.Exit(1)
	}

	// Register dedicated leader-only background RegistryHeartbeatRunner (fixes R5-P1-2)
	if err := mgr.Add(&controller.RegistryHeartbeatRunner{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
	}); err != nil {
		setupLog.Error(err, "Unable to register RegistryHeartbeatRunner")
		os.Exit(1)
	}

	// Register built-in stateless K8sJobProxy Executor HTTP Server (Stream 3)
	// Implements NeedLeaderElection() == false so all replicas serve proxy trigger requests.
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "Unable to create kubernetes clientset for pod log streaming")
		os.Exit(1)
	}

	proxySrv := proxyexecutor.NewServer(mgr.GetClient(), proxyBindAddr, defaultNamespace, proxyAccessToken)
	proxySrv.APIReader = mgr.GetAPIReader()
	proxySrv.Clientset = clientset
	proxySrv.ValidateToken = func(ctx context.Context, namespace, executorName, providedToken string) error {
		return controller.ValidateExecutorAccessToken(ctx, mgr.GetClient(), mgr.GetAPIReader(), namespace, executorName, providedToken)
	}

	if err := mgr.Add(proxySrv); err != nil {
		setupLog.Error(err, "Unable to register K8sJobProxy Executor server")
		os.Exit(1)
	}

	if enableWebhooks {
		mutator := webhook.NewPodExecutorMutator(sidecarImage, "", webhookDefaultAccessToken)
		mutator.K8sClient = mgr.GetClient()
		mgr.GetWebhookServer().Register("/mutate-v1-pod", &admission.Webhook{Handler: mutator})
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	cacheSyncCheck := func(req *http.Request) error {
		if !mgr.GetCache().WaitForCacheSync(req.Context()) {
			return fmt.Errorf("informer cache not yet synced")
		}
		return nil
	}
	if err := mgr.AddReadyzCheck("cache-sync", cacheSyncCheck); err != nil {
		setupLog.Error(err, "Unable to set up cache-sync ready check")
		os.Exit(1)
	}
	if enableWebhooks {
		if err := mgr.AddReadyzCheck("webhook", mgr.GetWebhookServer().StartedChecker()); err != nil {
			setupLog.Error(err, "Unable to set up webhook ready check")
			os.Exit(1)
		}
	}

	setupLog.Info("Starting xxl-job-operator manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}
}
