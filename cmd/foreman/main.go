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
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/tsic404/foreman/internal/auth"
	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/platform"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/recovery"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/scheduler"
	"github.com/tsic404/foreman/internal/state"
	"github.com/tsic404/foreman/internal/workspace"
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
	// The deployment's workspace shape (ADR-049): either per-workspace
	// (credential, daemon identity) pairs — the legacy single-workspace form
	// is a set of one — or a single user-scoped credential whose workspace set
	// is discovered at startup. The legacy single-workspace deployment keeps
	// its behaviour except for the workspace-set source and the exact-set
	// self-check (§1.1).
	deployment, err := workspace.LoadDeployment(getenv)
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
	// The user-scoped discovery form has no workspace identity in the
	// interface; the scheduler's single-workspace seams read this field, so it
	// is filled from the discovery result below.
	workspaces := workspace.Set{}
	if deployment.Scope == workspace.ScopeWorkspace {
		workspaces = deployment.Set
		schedCfg.Workspaces = workspaces.IDs()
		if schedCfg.WorkspaceID == "" {
			schedCfg.WorkspaceID = workspaces.IDs()[0]
		}
		jbCfg.WorkspaceIDs = workspaces.IDs()
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
	//
	// The shared state face (ADR-046 阶段一) is wired when FOREMAN_STATE_URL
	// is set: the Job Token entries, the settled records and the revocation
	// set then persist in the shared store, so a restart restores them
	// instead of rebuilding from K8s. Without the URL the no-op store keeps
	// the pre-stage-one behaviour (in-memory index + K8s rebuild) — the
	// shared state is an optional persistence face at stage one, never a
	// startup requirement (02-contracts.md §7 部署面).
	stateCfg, err := state.LoadConfig(getenv)
	if err != nil {
		return err
	}
	var shared state.Store
	if stateCfg.URL != "" {
		shared, err = state.New(stateCfg)
		if err != nil {
			return err
		}
		// Log the endpoint without its userinfo: a redis:// URL may embed the
		// credential, and the redactor only knows the token prefixes (§9.1:
		// never leak a credential into the log). authenticated reads the same
		// credential state.New applies (the dedicated password wins, else the
		// URL's own data-node credential), and the shipped REPLACE_ME
		// placeholder counts as absent: an unreplaced manifest must alarm,
		// not pass as protected.
		authenticated := stateAuthenticated(stateCfg)
		log.Info("state.configured", "endpoint", stateEndpoint(stateCfg.URL),
			"authenticated", authenticated)
		if !authenticated {
			// §10 安全: the store carries token digests, the mapping rows and
			// the delivery bodies, so it must be protected like a credential
			// store. An unauthenticated store is allowed (cluster-internal
			// topologies exist) but never silent.
			log.Warn("state.unauthenticated",
				"detail", "FOREMAN_STATE_PASSWORD is empty or still the REPLACE_ME placeholder, and the URL carries no data-node credential: the shared state store is unauthenticated")
		}
	}
	reg := registry.New(time.Now,
		registry.WithDrainTimeout(schedCfg.JobDrainTimeout),
		registry.WithDeliveryConfirmTimeout(schedCfg.DeliveryConfirmTimeout),
		registry.WithStateStore(shared),
		// §9.1: a shared-state failure is an event, never silent. The
		// in-memory transition stands (the K8s rebuild is the backstop).
		registry.WithStateErrorHook(func(op, taskID string, err error) {
			log.Warn("state.entry_failed", "op", op, "task_id", taskID, "err", err.Error())
		}))
	// The catalog probe's identity lives beside the task registry (ADR-019):
	// it is what the Job Token probe branch, the probe handlers and the probe
	// lifecycle all read.
	probes := registry.NewProbeRegistry()
	metrics := observability.NewMetrics()

	// The user-scoped discovery form resolves its workspace set before any
	// client exists: C-17 with the single credential, the manifest rule
	// applied, then one (credential, daemon identity) pair per discovered
	// workspace derived from the deployment base identity (ADR-049 decision
	// 3). Discovery failure refuses startup — never a half-configured service
	// (§1.1, AC-96).
	var discoveryEvent *proxy.Discovery
	if deployment.Scope == workspace.ScopeUser {
		// Discovery runs before any client exists: it defines the set.
		workspaces, discoveryEvent, err = discoverUserScope(context.Background(), deployment, proxyCfg, metrics, baseLog)
		if err != nil {
			return err
		}
		schedCfg.Workspaces = workspaces.IDs()
		if schedCfg.WorkspaceID == "" {
			schedCfg.WorkspaceID = workspaces.IDs()[0]
		}
		jbCfg.WorkspaceIDs = workspaces.IDs()
	}

	slotsByWorkspace := make(map[string]*proxy.CatalogSlots, workspaces.Len())

	// One fake client per workspace identity: every F→S call carries that
	// workspace's credential and daemon identity (§1.1). The
	// registration-discovery probe runner is wired late: the scheduler is
	// built after the clients (it consumes a client as its server seam).
	var discoveryProbe proxy.DiscoveryProbeRunner
	clients := make([]*proxy.Client, 0, workspaces.Len())
	tokenByWorkspace := make(map[string]string, workspaces.Len())
	for _, identity := range workspaces.All() {
		cfg := proxyCfg
		cfg.WorkspaceID = identity.WorkspaceID
		cfg.DaemonID = identity.DaemonID
		slots := proxy.NewCatalogSlots()
		slotsByWorkspace[identity.WorkspaceID] = slots
		tokenByWorkspace[identity.WorkspaceID] = identity.Token
		client := proxy.NewClient(cfg, identity.Token,
			proxy.WithMetrics(metrics),
			proxy.WithLogger(baseLog),
			proxy.WithCatalogSlots(slots),
			proxy.WithDiscoveryProbe(proxy.DiscoveryProbeFunc(func(ctx context.Context, workspaceID string) (registry.Discovery, error) {
				if discoveryProbe == nil {
					return registry.Discovery{}, errors.New("scheduler not wired yet")
				}
				return discoveryProbe.RunDiscoveryProbe(ctx, workspaceID)
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
		if discoveryEvent != nil {
			for _, ref := range discoveryEvent.Set {
				if ref.ID == identity.WorkspaceID {
					client.SetWorkspaceName(ref.Name)
					break
				}
			}
		}
		clients = append(clients, client)
	}
	clientSet, err := proxy.NewClientSet(clients...)
	if err != nil {
		return err
	}
	// Startup credential checks run before the policy gate: C-18 snapshots,
	// and the workspace-scoped form's C-17 exact-set self-check, which also
	// seeds each workspace's display name. The policy's partition keys
	// resolve against that startup result, so the checks must land first
	// (§8.5 分区键的启动期解析). Only a configuration error refuses startup;
	// a transient upstream failure warns and the loops converge (§1.1, §1.5,
	// AC-86). The signal context is created later, so the checks run on a
	// background context — the default signal handling still terminates a
	// start-up process.
	if err := runStartupSelfCheck(context.Background(), deployment.Scope, clientSet.All(), log); err != nil {
		return err
	}

	jbCfg.Workspaces = policyWorkspaceRefs(clientSet, workspaces)

	// The legacy single client handle stays for the router seams that only
	// need the shared server URL (passthrough, ops); every workspace-scoped
	// call goes through clientSet.
	client := clients[0]

	// Project identity (ADR-048): one adapter answers both platform queries —
	// the startup policy project name → identifier resolution the builder's
	// policy gate performs, and the claim-time issue → identifier lookup the
	// scheduler performs when a declared tier reads the project side. Each
	// query runs with its workspace's credential, and its cache is partitioned
	// by workspace (§4.1).
	projects := platform.NewProjectQueriesFor(proxyCfg.ServerURL, tokenByWorkspace)
	jbCfg.NameResolver = projects
	schedCfg.Project = projects

	// Terminal reports that exhausted the retry budget are queued on the
	// node-local state root and re-sent through the scheduler's single
	// terminal face (contract §10, ADR-031). Each entry also lands on a
	// Foreman-created Secret carrier, so it survives a process exit
	// (ADR-046).
	jobs := scheduler.NewJobClient(clientset, schedCfg.JobNamespace)
	pending, err := scheduler.NewPendingReports(
		scheduler.PendingReportsDir(getenv),
		scheduler.WithPendingReportsGauge(metrics.PendingReports),
		scheduler.WithPendingReportsLogger(baseLog),
		scheduler.WithPendingReportsCarrier(scheduler.NewSecretCarrierStore(jobs)),
	)
	if err != nil {
		return err
	}

	issuer, err := proxy.JobTokenIssuer(authCfg.Key, reg, probes, authCfg.TokenTTL, shared,
		auth.WithRevocationErrorHook(func(op string, err error) {
			log.Warn("state.revocation_failed", "op", op, "err", err.Error())
		}))
	if err != nil {
		return err
	}
	// ADR-046 阶段一: seed the in-process revocation cache from the shared
	// store, so a digest revoked before the restart is rejected after it. A
	// store miss leaves the per-request lookup as the fallback.
	if err := issuer.RestoreRevocations(context.Background()); err != nil {
		log.Warn("state.revocations_restore_failed", "err", err.Error())
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
	serverRouter := &workspaceServer{reg: reg, clients: clientSet}
	sched, err := scheduler.New(
		schedCfg,
		reg,
		jobs,
		builder,
		serverRouter,
		metrics,
		scheduler.WithPendingReports(pending),
		scheduler.WithReconciler(late),
		scheduler.WithRevoker(issuer),
		scheduler.WithProbeRegistry(probes),
		scheduler.WithLogger(baseLog),
		// The claim-time dispatch record S-4 matches on: ids move on a rebuild,
		// identities do not (§2.3).
		scheduler.WithRuntimeIdentity(clientSet.RuntimeIdentity),
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
		serverRouter,
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
	fakeServer := proxy.NewFakeServer(issuer, reg, sched, client, ws, metrics,
		proxy.WithProbeRegistry(probes), proxy.WithClientSet(clientSet))
	// The liveness view the health endpoint serves (§9.3): the loops register
	// as they start, and a critical loop that stops turns the probe red.
	health := observability.NewHealth()
	ops := observability.NewOpsHandler(metrics, reg, observability.NewK8sPodLogReader(clientset), observability.OpsConfig{
		Version:        version,
		JobImage:       jbCfg.JobImage,
		RuntimeIDs:     clientSet.RuntimeIDs,
		RegisterSource: registerSourceView(clientSet, slotsByWorkspace),
		Health:         health,
		WorkspaceIDs:   clientSet.IDs(),
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

	// The loops are supervised: a stop with a live context is a process-level
	// failure (§1.5) and takes the process down for a visible restart, while
	// the ordinary cancellation path stays a graceful shutdown.
	sup := newSupervisor(baseLog, health, stop)
	run := func(component string, f func(context.Context) error) { sup.run(ctx, component, f) }
	runWorkspace := run
	if workspaces.Len() > 1 {
		// A workspace identity's control plane failing at runtime is confined
		// to that workspace (ADR-047, AC-86): its loop backs off and retries,
		// while every other workspace keeps serving. Only a single-workspace
		// deployment keeps §1.5's process-level failure.
		runWorkspace = func(component string, f func(context.Context) error) {
			sup.run(ctx, component, func(ctx context.Context) error {
				return isolateWorkspaceLoop(ctx, component, baseLog, workspaceRetryDelay, f)
			})
		}
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
	//
	// Startup credential checks already ran before the policy gate (they seed
	// the display names the partition-key resolution reads), so the probe is
	// the next startup step.

	discoveries, err := sched.RunStartupProbe(ctx)
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
	// Each declaration is cached on its workspace's client; the claim loop's
	// preflight sends that workspace's C1 of this startup (with the §1.5
	// backoff, C-1), so a transient failure retries instead of exiting.
	served := make([]*proxy.Client, 0, len(discoveries))
	servedRefs := make([]workspace.Ref, 0, len(discoveries))
	unregisteredRefs := make([]workspace.Ref, 0, len(discoveries))
	for _, c := range clientSet.All() {
		discovery, ok := discoveries[c.WorkspaceID()]
		if !ok {
			// Discovered but not registered by the probe: no declaration, no
			// control plane, no service — and never a silent drop (§10 多
			// workspace 前置; AC-87). Each unregistered member also gets its
			// own warn line (§9.1 服务事件按条 warn).
			id, name := c.Workspace()
			log.Warn("workspace.probe_registration_absent",
				"workspace_id", c.WorkspaceID(),
				"workspace_name", name,
				"detail", "discovered workspace was not registered by the probe; it is not served")
			unregisteredRefs = append(unregisteredRefs, workspace.Ref{ID: id, Name: name})
			continue
		}
		c.Declare(discovery)
		served = append(served, c)
		id, name := c.Workspace()
		servedRefs = append(servedRefs, workspace.Ref{ID: id, Name: name})
		log.Info("runtime.declared",
			"workspace_id", c.WorkspaceID(),
			"runtimes", len(discovery.Runtimes),
			"cli_version", discovery.CLIVersion,
			"failed_profiles", len(discovery.FailedProfiles))
	}
	if len(served) == 0 {
		return errors.New("no configured workspace was declared by the startup probe")
	}
	// §9.1 服务事件: one line after the probe, with the effective service
	// scope and the discovered-but-unregistered members. The set is fixed for
	// the process lifetime, so this is emitted once. Manifest-excluded
	// workspaces never entered the discovery set, so they are reported at
	// discovery time, not here.
	proxy.LogServingEvent(log, servedRefs, unregisteredRefs)
	// Policy partitions outside the effective service scope are dead
	// configuration: no Job reads them, and they warn without refusing
	// startup (§8.5, AC-44/AC-98).
	servedIDs := make([]string, 0, len(servedRefs))
	for _, ref := range servedRefs {
		servedIDs = append(servedIDs, ref.ID)
	}
	builder.WarnDeadPartitions(servedIDs)

	// §4.3: rebuild the index before any loop that can create a Job or
	// converge an entry. The claim loop creates Jobs; a rebuild running
	// beside it can list one of those Jobs and write its payload-less
	// projection over the live entry — losing the payload, the token digest
	// and the boot/lease deadlines.
	if err := sched.Start(ctx); err != nil {
		log.Error("foreman.startup_aborted", "stage", "rebuild", "err", err.Error())
		return err
	}

	for _, c := range served {
		workspaceID := c.WorkspaceID()
		claimLoop := proxy.NewClaimLoop(proxyCfg, c, workspaceClaimScheduler{sched: sched, workspaceID: workspaceID})
		reporter := proxy.NewModelCatalogReporter(proxyCfg, c, sched, c.CatalogSlots(), metrics,
			proxy.WithReporterLogger(baseLog))
		runWorkspace("claim/"+workspaceID, claimLoop.Run)
		runWorkspace("heartbeat/"+workspaceID, proxy.NewHeartbeatLoop(c, reporter).Run)
		runWorkspace("lease/"+workspaceID, proxy.NewLeaseKeeper(c, reg, sched, proxyCfg.LeaseRefreshInterval,
			proxy.WithLeaseWorkspace(workspaceID)).Run)
		refresher := proxy.NewProfileRefresher(c, proxy.WithProfileRefresherLogger(baseLog))
		if proxyCfg.WSEnabled {
			// §1.6: the refresh trigger is the C-20 invalidation hint. With
			// WS closed the loop is not started: profile changes degrade to
			// "effective on the next restart", as the deployment note says.
			runWorkspace("ws-subscriber/"+workspaceID, proxy.NewWSSubscriber(c, claimLoop.Wake, metrics,
				proxy.WithProfileChangeNotify(refresher.Notify)).Run)
			runWorkspace("profile-refresh/"+workspaceID, refresher.Run)
		}
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
		"workspaces", workspaces.IDs(),
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

	// A component error recorded before the root context ended is fatal: the
	// credential/identity defect cannot be retried into working, and a
	// non-zero exit is what makes the Deployment restart (and eventually
	// CrashLoopBackOff) — the alarm §1.5 requires.
	fatalErr := sup.failure()
	reason := "signal"
	if fatalErr != nil {
		reason = "component failure"
	}

	// Shutdown: stop the loops, deregister the runtime (C14) so the server
	// releases it immediately, then drain HTTP.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	log.Info("foreman.stopping", "reason", reason)
	for _, c := range served {
		if err := c.Deregister(shutdownCtx); err != nil {
			log.Warn("daemon.deregister_failed", "workspace_id", c.WorkspaceID(), "err", err.Error())
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http.shutdown_failed", "err", err.Error())
	}
	// The loop drain shares the shutdown budget: kubelet SIGKILLs at the
	// deployment's terminationGracePeriodSeconds (30s), and the 20s budget
	// here stays inside it — an unbounded wait would leave no
	// foreman.stopped line at all.
	if !sup.wait(shutdownCtx) {
		log.Warn("loops.shutdown_timeout", "err", shutdownCtx.Err().Error())
	}
	// The shared-state writes run on an ordered writer outside the registry
	// lock; drain what is queued so the last transitions land before exit.
	if shared != nil {
		if err := reg.FlushState(shutdownCtx); err != nil {
			log.Warn("state.flush_incomplete", "err", err.Error())
		}
	}
	log.Info("foreman.stopped")
	if fatalErr != nil {
		return fatalErr
	}
	return nil
}

// workspaceRetryDelay is the isolation loop's backoff. Every runtime failure
// is confined to its workspace (§1.4, AC-86): the startup self-check is the
// only fail-fast face (§1.5), because a runtime credential failure in one
// workspace must not stop the others' heartbeats, claims and subscriptions.
const workspaceRetryDelay = 30 * time.Second

// isolateWorkspaceLoop runs one workspace's control loop. Any runtime failure
// — a 401/403 included — is logged and retried without touching the other
// workspaces; only the startup self-check decides that a credential cannot
// serve this deployment.
func isolateWorkspaceLoop(ctx context.Context, component string, log *slog.Logger, retryDelay time.Duration, f func(context.Context) error) error {
	for {
		err := f(ctx)
		if err == nil || ctx.Err() != nil {
			return err
		}
		log.Error("workspace loop stopped; retrying in isolation",
			"component", component, "err", err.Error())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryDelay):
		}
	}
}

// stateEndpoint renders the shared-state URL without its userinfo, so a
// credential embedded in the URL never reaches the log (§9.1).
func stateEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// statePlaceholderCredential is the shipped manifest's credential
// placeholder. It is a usable string, so an unreplaced deployment would
// authenticate with a value published in the repository; it counts as
// unauthenticated and raises state.unauthenticated instead.
const statePlaceholderCredential = "REPLACE_ME"

// stateAuthenticated reports whether the store's credential comes from a real
// value. The dedicated password wins over the URL's own data-node credential
// exactly as state.New applies them; an empty value or the shipped
// placeholder reads as unauthenticated (§10 安全: the store holds token
// digests, mapping rows and delivery bodies).
func stateAuthenticated(cfg state.Config) bool {
	if cfg.Password != "" {
		return cfg.Password != statePlaceholderCredential
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return false
	}
	if u.Query().Get("master_name") != "" {
		// A sentinel URL's userinfo authenticates to the sentinels, not to
		// the data node: the data node's credential is the password query
		// parameter, which is what state.New applies here.
		pw := u.Query().Get("password")
		return pw != "" && pw != statePlaceholderCredential
	}
	if u.User == nil {
		return false
	}
	pw, hasPassword := u.User.Password()
	if !hasPassword || pw == "" {
		// A username-only ACL entry is still an identity the store checks.
		return u.User.Username() != ""
	}
	return pw != statePlaceholderCredential
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

// workspaceClaimScheduler binds the shared scheduler to one workspace's claim
// budget and claim entry (§10 两级预算; §1.1 控制面逐套).
type workspaceClaimScheduler struct {
	sched       *scheduler.Scheduler
	workspaceID string
}

func (w workspaceClaimScheduler) ClaimBudget() int { return w.sched.ClaimBudgetFor(w.workspaceID) }

func (w workspaceClaimScheduler) OnClaim(ctx context.Context, task json.RawMessage) error {
	return w.sched.OnClaimFor(ctx, w.workspaceID, task)
}

// workspaceServer routes a task-addressed server call to the client of the
// entry's workspace: a settled or rebuilt entry always carries its workspace,
// so the credential is never borrowed across workspaces (§1.4 失败粒度含
// workspace).
type workspaceServer struct {
	reg     *registry.Registry
	clients *proxy.ClientSet
}

func (w *workspaceServer) clientFor(taskID string) (*proxy.Client, error) {
	if entry, ok := w.reg.Get(taskID); ok {
		if client, found := w.clients.For(entry.WorkspaceID); found {
			return client, nil
		}
		return nil, fmt.Errorf("no client for workspace %q", entry.WorkspaceID)
	}
	// A settled record keeps the workspace too; fall back to the only client
	// when the deployment serves one workspace.
	if settled, ok := w.reg.Settled(taskID); ok {
		if client, found := w.clients.For(settled.WorkspaceID); found {
			return client, nil
		}
	}
	if all := w.clients.All(); len(all) == 1 {
		return all[0], nil
	}
	return nil, fmt.Errorf("task %s has no workspace to route the server call", taskID)
}

func (w *workspaceServer) TaskStatus(ctx context.Context, taskID string) (string, error) {
	client, err := w.clientFor(taskID)
	if err != nil {
		return "", err
	}
	return client.TaskStatus(ctx, taskID)
}

func (w *workspaceServer) Forward(ctx context.Context, ep scheduler.Endpoint, taskID string, body []byte) (int, []byte, error) {
	client, err := w.clientFor(taskID)
	if err != nil {
		return 0, nil, err
	}
	return client.Forward(ctx, ep, taskID, body)
}

// ForwardInWorkspace routes a replay whose entry is gone by the workspace
// persisted with its queued report (§10). It never falls back to another
// workspace's credential: borrowing one would misattribute the terminal
// report across the boundary ADR-047 draws.
func (w *workspaceServer) ForwardInWorkspace(ctx context.Context, ep scheduler.Endpoint, taskID, workspaceID string, body []byte) (int, []byte, error) {
	client, found := w.clients.For(workspaceID)
	if !found {
		return 0, nil, fmt.Errorf("no client for workspace %q", workspaceID)
	}
	return client.Forward(ctx, ep, taskID, body)
}

// registerSourceView builds S31's read-only projection per workspace: the
// discovery behind the current declaration, the declaration entry by entry
// keyed by (provider, profile_id), every real runtime's catalog slot state,
// and the current S-3 replay source with its state (§9.3). It never triggers a
// discovery or a refresh.
func registerSourceView(clients *proxy.ClientSet, slots map[string]*proxy.CatalogSlots) func() any {
	return func() any {
		views := make(map[string]any, len(clients.All()))
		for _, client := range clients.All() {
			views[client.WorkspaceID()] = registerSourceViewOne(client, slots[client.WorkspaceID()])
		}
		return views
	}
}

func registerSourceViewOne(client *proxy.Client, slots *proxy.CatalogSlots) any {
	{
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
