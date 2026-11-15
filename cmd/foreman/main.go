// Command foreman runs the Foreman v2 server: it registers itself to the
// multica server as one daemon per workspace, claims tasks in batches, hands
// each to a long-lived registered multica daemon, and forwards the daemon's
// reports back to the server. There is no per-task Job and no per-task pod:
// a daemon registers once, stays alive across tasks, and receives one task at
// a time through the S6 claim face.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tsic404/foreman/internal/daemonauth"
	"github.com/tsic404/foreman/internal/dispatch"
	"github.com/tsic404/foreman/internal/lifecycle"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/proxy"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/workspace"
)

// version is stamped at build time (make build-foreman).
var version = "dev"

const (
	shutdownTimeout  = 20 * time.Second
	readHeaderTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "foreman:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":8080", "HTTP address for the daemon face")
	opsListen := flag.String("ops-listen", ":8081",
		"HTTP address for the ops surface (metrics, task projections). Empty disables the surface")
	flag.Parse()

	getenv := os.Getenv
	started := time.Now()

	obsCfg, err := observability.LoadConfig(getenv)
	if err != nil {
		return err
	}
	proxyCfg, err := proxy.LoadConfig(getenv)
	if err != nil {
		return err
	}
	proxyCfg.Version = version

	// The deployment's workspace shape: either per-workspace (credential,
	// daemon identity) pairs or a single user-scoped credential whose set is
	// discovered at startup.
	deployment, err := workspace.LoadDeployment(getenv)
	if err != nil {
		return err
	}

	// The daemon credential set: the long-lived daemons that register to
	// Foreman. A malformed or missing set refuses startup.
	daemonCreds, err := daemonauth.Load(getenv)
	if err != nil {
		return err
	}

	baseLog := observability.NewLogger(os.Stdout, obsCfg.LogLevel, "")
	slog.SetDefault(baseLog)
	log := baseLog.With("component", "foreman")

	metrics := observability.NewMetrics()

	// Resolve the workspace identities. The user-scoped form discovers its
	// workspace set from the server at startup (C17); the workspace-scoped
	// form is the configured set.
	var clients *proxy.ClientSet
	var workspaceIDs []string
	switch deployment.Scope {
	case workspace.ScopeWorkspace:
		workspaceIDs = deployment.Set.IDs()
		ids := deployment.Set.IDs()
		set := make([]*proxy.Client, 0, len(ids))
		for _, wsID := range ids {
			identity, _ := deployment.Set.Get(wsID)
			cfg := proxyCfg
			cfg.WorkspaceID = wsID
			cfg.DaemonID = identity.DaemonID
			set = append(set, proxy.NewClient(cfg, identity.Token,
				proxy.WithMetrics(metrics), proxy.WithLogger(baseLog)))
		}
		var cerr error
		clients, cerr = proxy.NewClientSet(set...)
		if cerr != nil {
			return cerr
		}
	case workspace.ScopeUser:
		// One credential, discovered workspace set, per-workspace derived
		// identity. Discovery is a startup gate: a credential that cannot
		// list its workspaces refuses startup.
		discClient := proxy.NewClient(proxyCfg, deployment.UserToken,
			proxy.WithMetrics(metrics), proxy.WithLogger(baseLog))
		discovered, derr := discClient.DiscoverWorkspaces(context.Background(), deployment.Manifest)
		if derr != nil {
			return fmt.Errorf("workspace discovery: %w", derr)
		}
		workspaceIDs = discovered.IDs()
		set := make([]*proxy.Client, 0, len(workspaceIDs))
		for _, wsID := range workspaceIDs {
			cfg := proxyCfg
			cfg.WorkspaceID = wsID
			cfg.DaemonID = workspace.DerivedDaemonID(deployment.BaseDaemonID, wsID)
			set = append(set, proxy.NewClient(cfg, deployment.UserToken,
				proxy.WithMetrics(metrics), proxy.WithLogger(baseLog)))
		}
		var cerr error
		clients, cerr = proxy.NewClientSet(set...)
		if cerr != nil {
			return cerr
		}
	}

	reg := registry.New()
	// Each workspace's dispatcher maps claimed tasks onto the daemons
	// registered to that workspace.
	dispatchers := make(map[string]*dispatch.Dispatcher, len(workspaceIDs))
	for _, wsID := range workspaceIDs {
		client, _ := clients.For(wsID)
		if client == nil {
			continue
		}
		d := dispatch.New(dispatch.Config{
			BootTimeout:     0, // configured via env below
			ReconcileInterval: 30 * time.Second,
		}, reg, client, metrics, dispatch.WithLogger(baseLog))
		dispatchers[wsID] = d
	}

	// The daemon face: long-lived daemons register, heartbeat and claim tasks.
	// A daemon's registration changes the runtime set Foreman declares to the
	// server, so the hook re-derives it per workspace; the claim loop's
	// preflight then re-runs C1 on the next round.
	redeclare := func() {
		for _, wsID := range workspaceIDs {
			client, _ := clients.For(wsID)
			if client == nil {
				continue
			}
			rts := reg.RuntimesForWorkspace(wsID)
			declared := make([]proxy.DeclaredRuntime, 0, len(rts))
			for _, rt := range rts {
				declared = append(declared, proxy.DeclaredRuntime{
					Name: rt.Name, Type: rt.Provider,
					ProfileID: rt.ProfileID,
				})
			}
			client.SetDeclared(declared)
		}
	}
	fakeServer := proxy.NewFakeServer(daemonCreds, reg, dispatcherSet(dispatchers),
		proxy.WithServerMetrics(metrics), proxy.WithServerLogger(baseLog),
		proxy.WithDaemonChangeHook(redeclare))

	// Capacity: how many tasks Foreman may claim now. v2 claims up to the
	// number of currently idle daemons across the workspaces — the number of
	// tasks it can actually dispatch.
	capacity := func() int {
		idle := 0
		for _, d := range dispatchers {
			idle += d.IdleDaemons()
		}
		return idle
	}

	// One control plane per workspace: claim loop + reconcile loop.
	planes := make([]*proxy.ControlPlane, 0, len(workspaceIDs))
	for _, wsID := range workspaceIDs {
		client, _ := clients.For(wsID)
		d := dispatchers[wsID]
		plane := proxy.NewControlPlane(proxy.ControlPlaneOptions{
			ProxyConfig: proxyCfg,
			Client:      client,
			Claimer:     d,
			Registry:    reg,
			Capacity:    capacity,
			Reconciler:  d,
		})
		planes = append(planes, plane)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	health := observability.NewHealth()
	supervisor := lifecycle.New(log, health, cancel)

	// The daemon face listener.
	jobMux := http.NewServeMux()
	jobMux.Handle("/", fakeServer)
	jobMux.Handle("GET /foreman/healthz", observability.NewHealthHandler(health, started))
	jobServer := &http.Server{
		Addr:              *listen,
		Handler:           jobMux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	// The ops surface (credential-gated): metrics, task projections.
	if *opsListen != "" {
		opsHandler := observability.NewOpsHandler(metrics, reg, observability.OpsConfig{
			Version:      version,
			WorkspaceIDs: workspaceIDs,
			Credentials:  obsCfg.OpsCredentials,
		})
		opsServer := &http.Server{
			Addr:              *opsListen,
			Handler:           opsHandler,
			ReadHeaderTimeout: readHeaderTimeout,
		}
		supervisor.Run(ctx, "ops-http", func(ctx context.Context) error {
			return serveHTTP(ctx, opsServer)
		})
	}

	// Run the control planes.
	for i, plane := range planes {
		for _, c := range plane.Components() {
			component := c
			planeName := fmt.Sprintf("ws%d-%s", i, component.Name)
			supervisor.Run(ctx, planeName, component.Run)
		}
	}

	// The Job face listener.
	supervisor.Run(ctx, "daemon-http", func(ctx context.Context) error {
		return serveHTTP(ctx, jobServer)
	})

	// Signal handling: on SIGTERM/SIGINT deregister from the server and drain.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		cancel()
	}()

	log.Info("foreman.started",
		"version", version, "workspaces", workspaceIDs, "listen", *listen,
		"ops_listen", *opsListen, "daemon_credential_id", daemonCreds.ID())

	<-ctx.Done()

	// Graceful shutdown: deregister every served identity, then drain.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	served := make([]lifecycle.Deregisterer, 0, len(clients.All()))
	for _, c := range clients.All() {
		served = append(served, c)
	}
	lifecycle.Shutdown(shutdownCtx, lifecycle.ShutdownOptions{
		Cancel: cancel,
		Served: served,
		Drain: func(ctx context.Context) {
			_ = jobServer.Shutdown(ctx)
		},
		Wait:  func(ctx context.Context) { supervisor.Wait(ctx) },
		Log:   log,
	})
	if err := supervisor.Failure(); err != nil {
		return err
	}
	return nil
}

// dispatcherSet adapts the per-workspace dispatchers to the fake server's
// single Dispatcher seam by routing on the task's workspace. Each dispatcher
// owns one workspace's tasks, so the routing is unambiguous.
type dispatcherSet map[string]*dispatch.Dispatcher

func (s dispatcherSet) Deliver(ctx context.Context, workspaceID, daemonID, runtimeID string, maxTasks int) (dispatch.Delivery, error) {
	d, ok := s[workspaceID]
	if !ok {
		return dispatch.Delivery{}, dispatch.ErrUnknownDaemon
	}
	return d.Deliver(ctx, workspaceID, daemonID, runtimeID, maxTasks)
}

func (s dispatcherSet) OnReport(ctx context.Context, ep dispatch.Endpoint, e registry.TaskEntry, body []byte, sessionID string) (int, []byte, error) {
	d, ok := s[e.WorkspaceID]
	if !ok {
		return 0, nil, fmt.Errorf("no dispatcher for workspace %s", e.WorkspaceID)
	}
	return d.OnReport(ctx, ep, e, body, sessionID)
}

func (s dispatcherSet) OnTaskVanished(ctx context.Context, taskID string) error {
	return nil
}

func (s dispatcherSet) OnBootTimeout(ctx context.Context, taskID string) error {
	return nil
}

// serveHTTP runs one HTTP server until ctx ends.
func serveHTTP(ctx context.Context, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}