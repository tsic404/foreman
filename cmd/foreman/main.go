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
	shutdownTimeout   = 20 * time.Second
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
	// Foreman. A malformed or missing set refuses startup. The user-scoped
	// discovery form derives one identity per discovered workspace, so its
	// bindings cannot be written down ahead of discovery — they are filled in
	// below once the workspace set is known.
	var daemonCreds *daemonauth.Store
	if deployment.Scope == workspace.ScopeUser {
		daemonCreds, err = daemonauth.LoadUnbound(getenv)
	} else {
		daemonCreds, err = daemonauth.Load(getenv)
	}
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
		// The workspace-scoped form validates every id it is configured with; the
		// discovered form must apply the same invariant, because these ids reach
		// log lines, derived daemon identities and filesystem paths.
		for _, ref := range discovered.Set {
			if !workspace.ValidWorkspaceID(ref.ID) {
				return fmt.Errorf("workspace discovery: server returned an unusable workspace id %q", ref.ID)
			}
		}
		if len(discovered.Excluded) > 0 {
			// The manifest hid workspaces the credential can see. Silence here
			// leaves an operator unable to tell "no workspaces" from "all
			// excluded by a stale manifest".
			log.Warn("workspaces excluded by the manifest", "count", len(discovered.Excluded))
		}
		// Bind each discovered workspace's identity, since the credential set
		// was loaded before the set was known.
		for _, id := range deployment.Identities(discovered.Set) {
			if !daemonCreds.Bind(id.DaemonID, id.WorkspaceID) {
				return fmt.Errorf("workspace discovery: no daemon credential named %q "+
					"(FOREMAN_DAEMON_TOKENS must key each identity as <base-daemon-id>-<workspace-id>)",
					id.DaemonID)
			}
		}
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
	// The dispatcher's timing bounds (boot bound, heartbeat sweep) come from
	// env; deploying Foreman without them silently disables both protections.
	dispCfg, err := dispatch.LoadConfig(getenv)
	if err != nil {
		return err
	}
	// Each workspace's dispatcher maps claimed tasks onto the daemons
	// registered to that workspace.
	dispatchers := make(map[string]*dispatch.Dispatcher, len(workspaceIDs))
	for _, wsID := range workspaceIDs {
		client, _ := clients.For(wsID)
		if client == nil {
			continue
		}
		// Each dispatcher is scoped to its own workspace: they share one
		// registry, so an unscoped dispatcher would count every workspace's
		// daemons as its own capacity.
		scoped := dispCfg
		scoped.WorkspaceID = wsID
		d := dispatch.New(scoped, reg, client, metrics,
			dispatch.WithLogger(baseLog),
			dispatch.WithRealRuntimeID(func(runtimeKey string) (string, bool) {
				// The server rewrites a runtime's display name (e.g. to the
				// device name), so names are not join keys. The daemon-side
				// runtime key is: it is unique per daemon runtime and carried
				// through C1's bind, so a multi-runtime daemon maps each task
				// to exactly the server runtime it declared.
				for _, ref := range client.RuntimeRefs() {
					if ref.RuntimeKey == runtimeKey && ref.ID != "" {
						return ref.ID, true
					}
				}
				return "", false
			}))
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
					ProfileID: rt.ProfileID, RuntimeKey: rt.RuntimeID,
					DeviceName: rt.DeviceName,
				})
			}
			client.SetDeclared(declared)
		}
	}
	fakeServer := proxy.NewFakeServer(daemonCreds, reg, dispatcherSet(dispatchers),
		proxy.WithServerMetrics(metrics), proxy.WithServerLogger(baseLog),
		proxy.WithDaemonChangeHook(redeclare),
		proxy.WithSkillResolver(func(ctx context.Context, taskID string, body []byte) (int, []byte, error) {
			e, ok := reg.Get(taskID)
			if !ok {
				// Skill bundles are resolved under the task's runtime, so unlike
				// a report there is nothing to forward without the binding.
				// Say "not bound" (404 downstream), not "gateway broke".
				return 0, nil, fmt.Errorf("%w: %s", proxy.ErrUnboundTask, taskID)
			}
			client, _ := clients.For(e.WorkspaceID)
			if client == nil {
				return 0, nil, fmt.Errorf("no client for task %s", taskID)
			}
			// The task's own runtime id, not any registered one: C-19's path is
			// runtime-scoped and the server only knows the runtime that claimed
			// this task.
			// e.RuntimeID is already the server's id when the bind resolved;
			// it stays the daemon key only while C-1 is still in flight, so
			// try the mapping and keep the original on a miss.
			runtimeID := e.RuntimeID
			if real := client.RealRuntimeID(runtimeID); real != "" {
				runtimeID = real
			}
			return client.ResolveSkillBundles(ctx, runtimeID, taskID, body)
		}),
		proxy.WithLeaseForwarder(func(ctx context.Context, workspaceID, taskID string) (bool, error) {
			e, ok := reg.Get(taskID)
			if !ok || e.RuntimeID == "" {
				return false, nil
			}
			client, _ := clients.For(e.WorkspaceID)
			if client == nil {
				return false, nil
			}
			return client.PrepareLease(ctx, e.RuntimeID, taskID)
		}),
		proxy.WithTaskForwarder(func(ctx context.Context, taskID string, body []byte) (int, []byte, error) {
			// A missing entry must not block the receipt: Foreman's registry is
			// in-memory, so a restart loses every binding while the daemons keep
			// running. The receipt is self-describing (task id + workspace from
			// the daemon credential) and the forward needs nothing else; the
			// upstream decides whether the task still exists.
			var client *proxy.Client
			if e, ok := reg.Get(taskID); ok {
				client, _ = clients.For(e.WorkspaceID)
			}
			if client == nil {
				// Unbound after a restart: fall back to the single served
				// workspace when there is exactly one, which is the common
				// deployment shape.
				if all := clients.All(); len(all) == 1 {
					client = all[0]
				}
			}
			if client == nil {
				return 0, nil, fmt.Errorf("no client for task %s", taskID)
			}
			// The task's own runtime id, not any registered one: C-19's path is
			// runtime-scoped and the server only knows the runtime that claimed
			// this task.
			return client.ContextReceipt(ctx, taskID, body)
		}),
		proxy.WithWorkspaceForwarders(
			func(ctx context.Context, workspaceID string) (int, []byte, error) {
				client, _ := clients.For(workspaceID)
				if client == nil {
					return 0, nil, fmt.Errorf("no client for workspace %s", workspaceID)
				}
				return client.WorkspaceRepos(ctx)
			},
			func(ctx context.Context, workspaceID string) (int, []byte, error) {
				client, _ := clients.For(workspaceID)
				if client == nil {
					return 0, nil, fmt.Errorf("no client for workspace %s", workspaceID)
				}
				return client.RuntimeProfiles(ctx)
			},
		))

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
			// Without this the loop falls back to a hardcoded 30s and
			// FOREMAN_RECONCILE_INTERVAL becomes a key that parses, validates,
			// and then changes nothing.
			Interval: dispCfg.ReconcileInterval,
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
			// The daemon face already gets this view; without it here the ops
			// healthz answers the documented "always ok" shape even when a
			// supervised loop has stopped, which is the opposite of what an
			// operator reading it would conclude.
			Health: health,
			// Without this the /foreman/register-source route was mounted but
			// always answered 404: the v1 wiring was lost in the v2 rewrite, so
			// an operator had no way to see which identities this process ended
			// up serving after discovery.
			RegisterSource: func() map[string]any {
				out := make(map[string]any, len(workspaceIDs))
				for _, wsID := range workspaceIDs {
					rts := reg.RuntimesForWorkspace(wsID)
					view := make([]map[string]any, 0, len(rts))
					for _, rt := range rts {
						view = append(view, map[string]any{
							"runtime_id": rt.RuntimeID,
							"daemon_id":  rt.DaemonID,
							"provider":   rt.Provider,
							"online":     rt.Online,
						})
					}
					out[wsID] = view
				}
				return out
			},
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
		Inflight: reg.Inflight,
		Wait:     func(ctx context.Context) { supervisor.Wait(ctx) },
		Log:      log,
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

func (s dispatcherSet) Deliver(ctx context.Context, workspaceID, daemonID string, runtimeIDs []string, maxTasks int) (dispatch.Delivery, error) {
	d, ok := s[workspaceID]
	if !ok {
		return dispatch.Delivery{}, dispatch.ErrUnknownDaemon
	}
	return d.Deliver(ctx, workspaceID, daemonID, runtimeIDs, maxTasks)
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
