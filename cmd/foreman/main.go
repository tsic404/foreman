// Command foreman runs the Foreman server: it pretends to be a high
// concurrency daemon towards Multica Server (fake client), pretends to be
// the server towards the official daemon inside every Job pod (fake
// server), and turns each claimed task into a Kubernetes Job.
//
// Design: 01-scope.md 定位/能力面, ADR-002 (no PVC).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
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
	// readHeaderTimeout bounds request header reads.
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

	// Configuration. Every loader is fail-fast: a bad value must be
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
	// Startup self-check of the credential budget: the Job Token must cover
	// claim → boot → task → drain (02-contracts.md §6.1, ADR-017). Only here
	// are all three timeouts known, so the check lives on this seam.
	if err := authCfg.ValidateTokenBudget(schedCfg.JobBootTimeout, schedCfg.JobDrainTimeout); err != nil {
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

	// The drain upper bound is the registry's, because markTerminal writes it
	// once at the alive → draining transition (ADR-017); the delivery
	// confirmation window is too, because markDelivering arms it once at the
	// delivery commit (ADR-032).
	reg := registry.New(time.Now,
		registry.WithDrainTimeout(schedCfg.JobDrainTimeout),
		registry.WithDeliveryConfirmTimeout(schedCfg.DeliveryConfirmTimeout))
	// The catalog probe's identity lives beside the task registry (ADR-019):
	// it is what the Job Token probe branch, the probe handlers and the probe
	// lifecycle all read.
	probes := registry.NewProbeRegistry()
	startupCatalog := registry.NewStartupCatalog()
	slots := proxy.NewCatalogSlots()
	metrics := observability.NewMetrics()

	// Fake client: the server credential never leaves this process (F3).
	// The registration-discovery probe runner is wired late: the scheduler is
	// built after the client (it consumes the client as its server seam).
	var discoveryProbe proxy.DiscoveryProbeRunner
	client := proxy.NewClient(proxyCfg, serverToken,
		proxy.WithMetrics(metrics),
		proxy.WithLogger(baseLog),
		proxy.WithStartupCatalog(startupCatalog),
		proxy.WithCatalogSlots(slots),
		proxy.WithDiscoveryProbe(proxy.DiscoveryProbeFunc(func(ctx context.Context) (registry.Discovery, error) {
			if discoveryProbe == nil {
				return registry.Discovery{}, errors.New("scheduler not wired yet")
			}
			return discoveryProbe.RunDiscoveryProbe(ctx)
		})),
		// C4/C19/S8 address a task by the real runtime it was dispatched
		// under (ADR-025); the dispatch record is the entry's.
		proxy.WithTaskRuntime(func(taskID string) string {
			e, ok := reg.Get(taskID)
			if !ok {
				return ""
			}
			return e.RealRuntimeID
		}),
	)

	// Terminal reports that exhausted the retry budget are queued on the
	// node-local state root and re-sent through the scheduler's single
	// terminal face (contract §10, ADR-031).
	pending, err := scheduler.NewPendingReports(
		scheduler.PendingReportsDir(getenv),
		scheduler.WithPendingReportsGauge(metrics.PendingReports),
		scheduler.WithPendingReportsLogger(baseLog),
	)
	if err != nil {
		return err
	}

	issuer, err := proxy.JobTokenIssuer(authCfg.Key, reg, probes, authCfg.TokenTTL)
	if err != nil {
		return err
	}
	jbCfg.Issuer = issuer
	jbCfg.Nodes = reg
	jbCfg.Metrics = metrics
	jbCfg.Logger = baseLog
	// The probe Job Token TTL is budget + drain timeout, so the builder needs
	// the same bound the scheduler reclaims credentials under.
	jbCfg.JobDrainTimeout = schedCfg.JobDrainTimeout

	// Startup gate: an unreadable or unparseable job-overlay.yaml must refuse
	// to serve (CrashLoopBackOff), never fail at the first claim
	// (02-contracts.md §8.1).
	builder, err := jobbuilder.NewBuilder(jbCfg)
	if err != nil {
		return fmt.Errorf("job template: %w", err)
	}

	// scheduler and recovery reference each other (Settler vs Reconciler);
	// the adapter breaks the construction cycle.
	late := &lateReconciler{}
	sched, err := scheduler.New(
		schedCfg,
		reg,
		scheduler.NewJobClient(clientset, schedCfg.JobNamespace),
		builder,
		client,
		metrics,
		scheduler.WithPendingReports(pending),
		scheduler.WithReconciler(late),
		scheduler.WithRevoker(issuer),
		scheduler.WithProbeRegistry(probes),
		scheduler.WithLogger(baseLog),
		// The claim-time dispatch record S-4 matches on: ids move on a rebuild,
		// identities do not (§2.3).
		scheduler.WithRuntimeIdentity(client.RuntimeIdentity),
	)
	if err != nil {
		return err
	}
	discoveryProbe = sched

	objects := recovery.NewK8sObjects(clientset, schedCfg.JobNamespace)
	reconciler, err := recovery.New(
		recCfg,
		reg,
		objects,
		client,
		sched,
		recovery.WithWatcher(objects),
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
	fakeServer := proxy.NewFakeServer(issuer, reg, sched, client, ws, metrics, proxy.WithProbeRegistry(probes))
	reporter := proxy.NewModelCatalogReporter(proxyCfg, client, sched, slots, metrics, proxy.WithReporterLogger(baseLog))
	ops := observability.NewOpsHandler(metrics, reg, observability.NewK8sPodLogReader(clientset), observability.OpsConfig{
		Version:        version,
		JobImage:       jbCfg.JobImage,
		RuntimeIDs:     client.RuntimeIDs,
		RegisterSource: registerSourceView(client, slots),
		StartedAt:      started,
		Logger:         baseLog,
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

	// Startup orchestration (ADR-025): the startup probe runs before C1, so
	// the server sees the template environment's real runtimes. The listener
	// must already be serving — the probe daemon registers through it.
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *listen, err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()

	// §1.6 startup order: the profile snapshot is fetched before the startup
	// probe, so the probe's daemon reads S-3 as the truth for the profiles it
	// declares. 404 (old server) and failure are not fatal: S-3 answers 404 or
	// 503, and the probe continues with the builtin runtime.
	if err := client.RuntimeProfiles(ctx); err != nil {
		log.Warn("runtime.profiles_snapshot_failed", "err", err.Error())
	}

	discovery, err := sched.RunStartupProbe(ctx)
	if err != nil {
		// Fail-fast, visible: no registration inside the window means the
		// template (or the probe) is not runnable, and Foreman must not
		// declare a runtime set it never observed (AC-36). The exit record
		// repeats the event's reason class (02-contracts.md §9.1).
		fields := []any{"err", err.Error()}
		var failure *scheduler.StartupProbeFailure
		if errors.As(err, &failure) {
			fields = append(fields, "job_name", failure.JobName, "reason", failure.Reason)
		}
		log.Error("foreman.startup_aborted", fields...)
		return err
	}
	// The declaration is cached here; the claim loop's preflight sends the one
	// C1 of this startup (with the §1.5 backoff, C-1), so a transient failure
	// retries instead of exiting.
	client.Declare(discovery)
	log.Info("runtime.declared",
		"runtimes", len(discovery.Runtimes),
		"cli_version", discovery.CLIVersion,
		"failed_profiles", len(discovery.FailedProfiles))

	run("scheduler", sched.Run)
	claimLoop := proxy.NewClaimLoop(proxyCfg, client, sched)
	run("claim", claimLoop.Run)
	run("heartbeat", proxy.NewHeartbeatLoop(client, reporter).Run)
	run("lease", proxy.NewLeaseKeeper(client, reg, sched, proxyCfg.LeaseRefreshInterval).Run)
	refresher := proxy.NewProfileRefresher(client, proxy.WithProfileRefresherLogger(baseLog))
	if proxyCfg.WSEnabled {
		// §1.6: the refresh trigger is the C-20 invalidation hint. With WS
		// closed the loop is not started: profile changes degrade to
		// "effective on the next restart", as the deployment note says.
		run("ws-subscriber", proxy.NewWSSubscriber(client, claimLoop.Wake, metrics,
			proxy.WithProfileChangeNotify(refresher.Notify)).Run)
		run("profile-refresh", refresher.Run)
	}
	run("recovery", reconciler.Run)

	// §1.6 降级面: without the WS subscription the runtime profile list is
	// never re-fetched, so a profile change stays invisible until the next
	// restart. The deployment note says the same; the running log must not
	// claim otherwise.
	if !proxyCfg.WSEnabled {
		log.Warn("runtime.profiles_degraded",
			"detail", "FOREMAN_WS_ENABLED=false: runtime profile changes take effect only after a restart")
	}

	log.Info("foreman.started",
		"version", version,
		"listen", *listen,
		"server_url", proxyCfg.ServerURL,
		"workspace_id", proxyCfg.WorkspaceID,
		"daemon_id", proxyCfg.DaemonID,
		"job_namespace", schedCfg.JobNamespace,
		"job_image", jbCfg.JobImage,
		"max_jobs_per_node", schedCfg.MaxJobsPerNode,
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

// registerSourceView builds S31's read-only projection: the discovery behind
// the current declaration, the declaration entry by entry keyed by
// (provider, profile_id), every real runtime's catalog slot state, and the
// current S-3 replay source with its state (§9.3). It never triggers a
// discovery or a refresh.
func registerSourceView(client *proxy.Client, slots *proxy.CatalogSlots) func() any {
	return func() any {
		discovery := client.Declaration()
		refs := client.RuntimeRefs()
		declaration := make([]map[string]string, 0, len(refs))
		for _, ref := range refs {
			declaration = append(declaration, map[string]string{
				"name":            ref.Name,
				"provider":        ref.Provider,
				"profile_id":      ref.ProfileID,
				"real_runtime_id": ref.ID,
			})
		}
		runtimes := discovery.Runtimes
		if runtimes == nil {
			runtimes = []registry.DiscoveredRuntime{}
		}
		failed := discovery.FailedProfiles
		if failed == nil {
			failed = []json.RawMessage{}
		}
		return map[string]any{
			"runtimes":           runtimes,
			"cli_version":        discovery.CLIVersion,
			"failed_profiles":    failed,
			"declaration":        declaration,
			"declaration_source": client.DeclarationSource(),
			"runtime_profiles":   client.RuntimeProfileView(),
			"slots":              slots.Snapshot(refs, time.Now()),
		}
	}
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
