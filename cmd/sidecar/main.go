package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/xxl-job/xxl-job-operator/pkg/sidecar"
)

func main() {
	adminURL := getEnv("XXL_JOB_ADMIN_ADDRESSES", "http://xxl-job-admin:8080/xxl-job-admin")
	// Never default to "default_token" (fixes R5-P0-1)
	accessToken := strings.TrimSpace(getEnv("XXL_JOB_ACCESS_TOKEN", getEnv("XXL_JOB_ACCESSTOKEN", "")))
	if accessToken == "" || accessToken == "default_token" {
		log.Fatalf("Refusing to start sidecar with empty or default_token XXL_JOB_ACCESS_TOKEN")
	}

	appName := getEnv("XXL_JOB_EXECUTOR_APPNAME", "default-sidecar-executor")
	podIP := getEnv("POD_IP", "127.0.0.1")
	execPort, _ := strconv.Atoi(getEnv("XXL_JOB_EXECUTOR_PORT", "9999"))
	targetPort, _ := strconv.Atoi(getEnv("XXL_JOB_TARGET_HTTP_PORT", "8080"))
	drainSecs, _ := strconv.Atoi(getEnv("XXL_JOB_GRACEFUL_DRAIN_SECONDS", "15"))
	if drainSecs <= 0 {
		drainSecs = 15
	}
	enableGlue := strings.EqualFold(strings.TrimSpace(os.Getenv("XXL_JOB_SIDECAR_ENABLE_GLUE")), "true")
	deregisterBeforeDrain := strings.EqualFold(strings.TrimSpace(os.Getenv("XXL_JOB_DEREGISTER_BEFORE_DRAIN")), "true")
	sharedSecret := strings.TrimSpace(os.Getenv("XXL_JOB_SIDECAR_SHARED_SECRET"))
	prevToken := strings.TrimSpace(os.Getenv("XXL_JOB_PREVIOUS_ACCESS_TOKEN"))
	// Default callback spool directory is placed on the sidecar-private /xxl-job/spool volume (fixes R7-P1-1).
	callbackDir := strings.TrimSpace(getEnv("XXL_JOB_SIDECAR_CALLBACK_DIR", "/xxl-job/spool/callbacks"))

	agent, err := sidecar.NewAgent(sidecar.Config{
		AdminURL:              adminURL,
		AccessToken:           accessToken,
		PreviousAccessToken:   prevToken,
		SharedSecret:          sharedSecret,
		AppName:               appName,
		PodIP:                 podIP,
		ExecutorPort:          execPort,
		TargetHTTPPort:        targetPort,
		CallbackDir:           callbackDir,
		EnableGlue:            enableGlue,
		DeregisterBeforeDrain: deregisterBeforeDrain,
		DrainTimeout:          time.Duration(drainSecs) * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to initialize sidecar agent: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go agent.StartSpoolRetryLoop(ctx, 10*time.Second)

	// Heartbeat registration loop (every 25 seconds, matching XXL-JOB's 30s beat interval).
	// Stops immediately once agent.IsDraining() is true so /preStop deregistration is never undone (fixes R6-P1-11).
	go func() {
		_ = agent.Register(ctx)
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if agent.IsDraining() {
					return
				}
				_ = agent.Register(ctx)
			}
		}
	}()

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", execPort),
		Handler:           agent.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting XXL-JOB Sidecar Agent on :%d (appName=%s, podIP=%s)", execPort, appName, podIP)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Sidecar server failed: %v", err)
		}
	}()

	<-ctx.Done()
	agent.StartDrain()
	log.Println("Shutting down XXL-JOB Sidecar Agent, waiting for active jobs and deregistering...")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Duration(drainSecs)*time.Second)
	agent.WaitForRunningJobs(drainCtx)
	drainCancel()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	agent.FlushSpooledCallbacksContext(flushCtx)
	flushCancel()

	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = agent.Deregister(deregCtx)
	deregCancel()

	srvCtx, srvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer srvCancel()
	_ = srv.Shutdown(srvCtx)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
