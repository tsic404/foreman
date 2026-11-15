# Foreman

Kubernetes-native elastic scheduler for Multica tasks, **v2**.

Foreman sits between Multica Server and a set of long-lived Multica daemons. It
registers itself to the server as one daemon per workspace, claims tasks in
batches, and hands each claimed task to an already-running daemon that has
registered to Foreman. To the server it looks like a single high-concurrency
daemon; under the hood every task runs on a resident daemon that stays alive
across tasks.

**v2 removes the disposable task pod.** v1 turned every task into a Kubernetes
Job whose daemon lived and died with that task, which made each task pay a full
cold start (image pull, agent download, registration). v2 has no Job and no
per-task object at all: a daemon registers once with an operator-issued
credential, keeps running, and receives one task at a time through the S6 claim
face.

## How it works

```
Multica Server  ◄── fake client ──  Foreman  ── fake server ──►  resident daemon (×N)
                                     │
                                     └── no Kubernetes API: no Job, no Secret
```

- **Fake client** — registers itself with Multica Server as a daemon runtime
  (one identity per served workspace), claims tasks, renews leases, forwards
  status/usage/completion.
- **Fake server** — accepts long-lived daemons that present an operator-issued
  bearer credential, records their runtime, and hands each the task Foreman
  assigned to it.
- **Dispatcher** — picks the daemon for a claim: session affinity first (the
  daemon that already holds the task's `prior_session_id`), then the least busy
  online daemon, then registration order.
- **Multiple runtimes per daemon.** A daemon may declare several runtimes — the
  built-in agent it detects plus any custom runtime profiles the workspace
  defines, and those can differ in provider (say `omp` next to `codex`).
  Foreman declares every one of them to the server as separate runtimes under
  its single daemon identity, binds each to the server-assigned id, and routes
  a task to the exact runtime it was claimed for. A daemon claiming for all its
  runtimes at once is matched against the task's bound runtime, not against
  whichever id happens to come first.
- **No per-task credential.** v1 minted a short-lived Job Token per task; v2
  authenticates daemons with a deployment-configured credential set
  (`FOREMAN_DAEMON_TOKENS`).

## Build and deploy

```bash
make build-foreman        # bin/foreman — the scheduler process
make build-foreman-image  # container image (linux/$(ARCH))
make check                # gofmt + vet + tests + build
```


The image is published to GHCR by `.github/workflows/build-foreman.yml`
(linux/amd64 + linux/arm64): a `v*.*.*` tag push publishes exactly
`foreman:v<X.Y.Z>` and a `main` push exactly `foreman:latest`. A pull request
into `main` is checked by `pr-check.yml` — `go build ./...`,
`go test -race ./...`, and a two-platform image build that publishes nothing.

Deployment lives in `deploy/` and is applied with `kubectl apply -f deploy/`:
namespaces, the server/ops secrets, the daemon credential secret, the
Deployment and its two Services (daemon face `:8080`, ops surface `:8081`). See
`deploy/README.md` for the Secret values and the ops credential.

### Daemon credentials

The daemons that register to Foreman are configured as a credential set, not
discovered:

```yaml
FOREMAN_DAEMON_TOKENS: "worker-1=<token>,worker-2=<token>"
FOREMAN_DAEMON_TOKEN_WORKSPACES: "worker-1=<workspace-id>,worker-2=<workspace-id>"
```

Each daemon presents its token as a bearer credential on every control-plane
call. A daemon identity with no workspace binding refuses startup, and a token
shared by two identities refuses startup. Tokens are compared by SHA-256 digest
in constant time and are never logged.

## Protocol surface

Foreman speaks two faces. The **client face** (Foreman → Multica Server) is the
full daemon protocol subset a long-lived daemon needs; the **server face**
(resident daemon → Foreman) is the same subset plus the report endpoints.

Client face (C-numbers as in `internal/proxy`): `C1` register, `C2` heartbeat,
`C3` claim, `C4` prepare-lease, `C18` runtime profiles, `C19` skill bundles,
`C22` workspace repos, `C23` context receipt, and the report forwards `S8`,
`S10`–`S17`.

Server face: `S1` workspaces, `S2` repos, `S3` runtime profiles, `S4` register,
`S5` heartbeat, `S6` claim, `S7` prepare-lease, `S8` start, `S9` status,
`S10`–`S17` reports, `S18` recover-orphans, `S19` deregister, `S24` token renew,
`S26` skill bundles, `S28` workspace list, `S31` context receipt.

Validated end to end against Multica **v0.9.0** (backend + web + pgvector, run
with docker or podman) with a real `multica daemon` process: a task dispatched
by the server is claimed by Foreman, delivered to a resident daemon, executed by
the agent, and settled back on the server as `completed`.

Not implemented: the WebSocket push face (`S30`/`C20`) — Foreman polls on the
claim interval, which is functionally equivalent at the cost of latency — and
the model-catalog probe (`C21`), since the resident daemon resolves its models
through its own agent CLI discovery.

## Status

Early development, but the v2 path is proven end to end. `deploy/test-real-daemon.sh`
brings up a local Multica stack, extracts the official CLI from the backend
image, mints a token, creates a workspace and agent, starts Foreman and a real
daemon, binds them, triggers an autopilot, and asserts the task settles as
`completed` — on both docker and podman.

## License

MIT
