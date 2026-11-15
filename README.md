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

## Status

Early development. Protocol feasibility was validated against Multica daemon
`v0.6.0` in v1; v2 keeps the server-side protocol (C1–C22) and the daemon-facing
control-plane subset (S1, S4–S9, S19, S28) and drops the rest.

## License

MIT
