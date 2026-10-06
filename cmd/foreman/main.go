// Command foreman runs the Foreman server: it pretends to be a high
// concurrency daemon towards Multica Server (fake client), pretends to be
// the server towards the official daemon inside every Job pod (fake
// server), and turns each claimed task into a Kubernetes Job.
//
// Design: docs/04-architecture.md §系统形态/§部署运行方式,
// docs/03-contracts.md §5.1 (configuration), ADR-002 (no PVC).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
)

// version is stamped at build time (make build-foreman).
var version = "dev"

const (
	// shutdownTimeout bounds the graceful deregister + HTTP drain window.
	shutdownTimeout = 20 * time.Second
	// readHeaderTimeout bounds request header reads (§4 control-plane).
	readHeaderTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "foreman:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":8080", "HTTP address for the Job daemon face and the ops endpoints")
	flag.Parse()

	getenv := os.Getenv
	started := time.Now()

	// Configuration (§5.1). Every loader is fail-fast: a bad value must be
	// refused at startup, never half-applied at runtime.
	obsCfg, err := observability.LoadConfig(getenv)
	if err != nil {
		return err
	}
	authCfg, err := auth.LoadConfig(getenv)
	if err != nil {
		return err
	}
	// Startup self-check: FOREMAN_JOB_TOKEN_TTL >= FOREMAN_TASK_MAX_DURATION.
	if err := authCfg.Validate(); err != nil {
		return err
	}
	serverToken, err := auth.LoadServerToken(getenv)
	if err != nil {
		return err
	}
	proxyCfg, err := proxy.LoadConfig(getenv)
	if err != nil {
		return err
	}
	proxyCfg.Version = version
	jbCfg, err := jobbuilder.LoadConfig(getenv)
	if err != nil {
		return err
	}
	schedCfg, err := scheduler.LoadConfig(getenv)
	if err != nil {
		return err
	}
	recCfg, err := recovery.LoadConfig(getenv)
	if err != nil {
		return err
	}

	// One JSON pipeline for the whole process. The shared base carries no
	// component: every component tags its own (and the modules logging
	// through slog.Default() get the same JSON + redaction handler).
	baseLog := observability.NewLogger(os.Stdout, obsCfg.LogLevel, "")
	slog.SetDefault(baseLog)
	log := baseLog.With("component", "foreman")

	kubeCfg, err := kubeConfig()
	if err != nil {
		return fmt.Errorf("kube config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(kubeCfg)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	reg := registry.New(time.Now)
	metrics := observability.NewMetrics()

	// Fake client: the server credential never leaves this process (F3).
	client := proxy.NewClient(proxyCfg, serverToken,
		proxy.WithMetrics(metrics),
		proxy.WithLogger(baseLog),
	)

	// Terminal reports that exhausted the retry budget are queued on the
	// node-local state root and re-sent by the reconciler (contract §4).
	pending, err := recovery.NewPendingReports(
		recovery.PendingReportsDir(getenv),
		recovery.WithPendingReportsGauge(metrics.PendingReports),
		recovery.WithPendingReportsLogger(baseLog),
	)
	if err != nil {
		return err
	}

	issuer, err := proxy.JobTokenIssuer(authCfg.Key, reg)
	if err != nil {
		return err
	}
	jbCfg.Issuer = issuer
	jbCfg.Nodes = reg

	// scheduler and recovery reference each other (Settler vs Reconciler);
	// the adapter breaks the construction cycle.
	late := &lateReconciler{}
	sched, err := scheduler.New(
		schedCfg,
		reg,
		scheduler.NewJobClient(clientset, schedCfg.JobNamespace),
		jobbuilder.NewBuilder(jbCfg),
		client,
		metrics,
		scheduler.WithPendingReports(pending),
		scheduler.WithReconciler(late),
		scheduler.WithLogger(baseLog),
	)
	if err != nil {
		return err
	}

	objects := recovery.NewK8sObjects(clientset, schedCfg.JobNamespace)
	reconciler, err := recovery.New(
		recCfg,
		reg,
		objects,
		client,
		sched,
		recovery.WithWatcher(objects),
		recovery.WithPendingReports(pending, client),
		recovery.WithMetrics(metrics),
		recovery.WithLogger(baseLog),
	)
	if err != nil {
		return err
	}
	late.r = reconciler

	// Fake server: S1–S30 for the Job daemons, plus the cluster-internal
	// ops surface (S25). Both share one listener on :8080.
	var ws *proxy.WSHandler
	if proxyCfg.WSEnabled {
		ws = proxy.NewWSHandler(issuer, reg, metrics)
	}
	fakeServer := proxy.NewFakeServer(issuer, reg, sched, client, ws, metrics)
	ops := observability.NewOpsHandler(metrics, reg, observability.NewK8sPodLogReader(clientset), observability.OpsConfig{
		Version:   version,
		JobImage:  jbCfg.JobImage + "@" + jbCfg.JobImageDigest,
		RuntimeID: client.RuntimeID,
		StartedAt: started,
		Logger:    baseLog,
	})
	mux := http.NewServeMux()
	mux.Handle("/api/", fakeServer)
	mux.Handle("/", ops)
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	run := func(component string, f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Loops return nil on cancellation; anything else is reported
			// and must not take the whole server down (its siblings keep
			// serving; the container's liveness probe owns restarts).
			if err := f(ctx); err != nil && ctx.Err() == nil {
				baseLog.Error("component.stopped", "component", component, "err", err.Error())
			}
		}()
	}

	run("scheduler", sched.Run)
	claimLoop := proxy.NewClaimLoop(proxyCfg, client, sched)
	run("claim", claimLoop.Run)
	run("heartbeat", proxy.NewHeartbeatLoop(client).Run)
	run("lease", proxy.NewLeaseKeeper(client, reg, sched, proxyCfg.LeaseRefreshInterval).Run)
	if proxyCfg.WSEnabled {
		run("ws-subscriber", proxy.NewWSSubscriber(client, claimLoop.Wake, metrics).Run)
	}
	run("recovery", reconciler.Run)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	log.Info("foreman.started",
		"version", version,
		"listen", *listen,
		"server_url", proxyCfg.ServerURL,
		"workspace_id", proxyCfg.WorkspaceID,
		"daemon_id", proxyCfg.DaemonID,
		"job_namespace", schedCfg.JobNamespace,
		"job_image", jbCfg.JobImage+"@"+jbCfg.JobImageDigest,
		"ws_enabled", proxyCfg.WSEnabled,
	)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	}

	// Shutdown: stop the loops, deregister the runtime (C14) so the server
	// releases it immediately, then drain HTTP.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	log.Info("foreman.stopping", "reason", "signal")
	if err := client.Deregister(shutdownCtx); err != nil {
		log.Warn("daemon.deregister_failed", "err", err.Error())
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http.shutdown_failed", "err", err.Error())
	}
	wg.Wait()
	log.Info("foreman.stopped")
	return nil
}

// lateReconciler breaks the scheduler<->recovery construction cycle:
// scheduler.New needs the Reconciler seam, recovery.New needs the scheduler
// as its Settler. Both then run against the same live objects.
type lateReconciler struct {
	r scheduler.Reconciler
}

func (l *lateReconciler) Reconcile(ctx context.Context) error {
	if l.r == nil {
		return nil
	}
	return l.r.Reconcile(ctx)
}

// kubeConfig prefers the in-cluster service account (the Deployment path)
// and falls back to the ambient kubeconfig for local runs.
func kubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}
