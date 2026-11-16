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
  online daemon, then registration order. Affinity needs two inputs, both
  upstream: a `prior_session_id` in the claim, and an S-16 `session` report
  from the daemon naming the runtime that holds the files. Multica **v0.9.0
  sends neither** on a fresh workspace — claims carry no `prior_session_id`
  and the daemon runs every task with `resume_session=false` — so in practice
  the dispatcher falls through to load and registration order. The affinity
  path is implemented and unit-tested, but on this version of the stack it is
  dormant rather than exercised.
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


The image is published to GHCR (linux/amd64 + linux/arm64) by two lines that
never write the same tag:

| Branch | Workflow | Publishes |
|---|---|---|
| `main` / `v*.*.*` tag | `build-foreman.yml` | `foreman:latest`, `foreman:v<X.Y.Z>` |
| `v2` | `build-foreman-v2.yml` | `foreman:v2` (movable; version stamped `v2-<sha7>`) |

A pull request into `main` is checked by `pr-check.yml` — `go build ./...`,
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

### Cancellation has no down-channel

A v1 task was a Job: cancelling it meant deleting the Job, which killed the
pod. A v2 task runs inside an already-resident daemon, and the daemon protocol
has no server-to-daemon cancel signal — no push frame, no claim-time flag, and
the daemon never polls task status while a run is in flight. S-15
`cancel-ack` only travels daemon → server, and the daemon only sends it when
something inside its own run decided to stop.

So a task cancelled on the server keeps running on the resident daemon and
reports its own terminal state (`complete`), while multica has already settled
it as `cancelled`. Foreman forwards what the daemon sends; it cannot invent a
cancel the daemon has no way to receive. Closing this needs a protocol
extension upstream (a cancel frame or a claim-time cancellation field) — it is
not fixable inside Foreman without patching the daemon CLI.

One case Foreman does handle: a daemon that stops a run on its own sends
`cancel-ack` (S-15). The server settles the task only when it was the one that
asked for the cancel, so that ack alone leaves the task `running` forever.
Foreman therefore reads the task's server status (C-13) and, if the server still
considers it live, forwards a synthetic `fail` carrying
`"error": "cancelled by daemon"` — verified end to end, which moves
`foreman_tasks_total{result="cancelled"}` and settles the task as `failed` with
that message. The protocol has no `cancel` report endpoint and no
cancel-flavoured `failure_reason`, so multica classifies the synthetic verdict
as `agent_error.unknown`; the daemon's own `cancel-ack` body has already been
forwarded verbatim beforehand, so no information is lost.

### Rotating the daemon credential leaves old instances hammering

The daemon credential is verified per request, so an instance running with a
revoked or reset token fails every call it makes. Because the claim loop polls
every few seconds, a forgotten instance produces a steady 401 stream at
`/api/daemon/tasks/claim` for as long as it lives — which reads like a server
auth fault when it is really one stale process. Observed in testing: eleven
401s in ten seconds, all from an instance that had been left running across a
database reset, and none from the instance that started a second later.
Foreman logs these as `daemon credential rejected` (WARN) with the reason, so
check for a duplicate process before suspecting the server.

## Status

Early development, but the v2 path is proven end to end against a real Multica
stack and a real resident daemon: a dispatched task is claimed, delivered, run
by the agent, and settled back on the server as `completed`.

One environment gotcha seen while testing: the daemon refuses to register any
runtime whose CLI is below its minimum version and logs
`skip registering runtime: version too old`. That runtime silently never
appears in the daemon's declaration, so Foreman cannot dispatch to it, while
the daemon's other runtimes register normally. Grep `daemon.log` for that line
when a declared runtime seems missing.

A second workspace is where the per-workspace accounting shows its seams. Two
defects found this way only exist with more than one served workspace: the idle
daemon count and the convergence round both walked the shared registry instead
of their own workspace, so one workspace claimed work it had no daemon to run
and every workspace converged every task. A single-workspace deployment cannot
reproduce either, so verify with a second workspace registered and served —
expect exactly one `claim.idle` and one `reconcile.round` per workspace, and
nothing tagged with one workspace's id answered by another's loop.

## License

MIT
