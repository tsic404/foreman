# Foreman

Kubernetes-native elastic scheduler for Multica tasks.

Foreman sits between Multica Server and Kubernetes: it claims tasks from the server, turns them into K8s Jobs, and reports results back. To the server it looks like a single high-concurrency daemon; under the hood every task runs in its own ephemeral pod on the cluster.

## How it works

```
Multica Server  ◄── fake client ──  Foreman  ── fake server ──►  Job pod (official daemon + agent)
                                     │
                                     └── k8s API: create / watch / delete Jobs
```

- **Fake client** — registers itself with Multica Server as a daemon runtime, claims tasks, forwards status/usage/completion.
- **Fake server** — accepts connections from the official `multica` daemon running inside each Job pod, dispatches the claimed task, receives progress reports.
- **Job orchestration** — one Job per task; node-local caches (repo mirror, session files) are reused via hostPath + soft node affinity, with graceful fallback to a cold start on any other node.

## Build and deploy

```bash
make build-foreman        # bin/foreman      — the scheduler process
make build-gc             # bin/foreman-gc   — node-local cache collector
make build-job-adapters   # bin/<adapter>    — the five reference Job adapters
make build-foreman-image  # Foreman image: server + Job supervisor + gc + adapters (linux/$(ARCH))
make check                # gofmt + vet + tests + build
```

The repo publishes one image, `foreman`, as a multi-platform manifest list
(`linux/amd64` + `linux/arm64`) through `.github/workflows/build-foreman.yml`:
a `v*.*.*` tag push publishes exactly `foreman:v<X.Y.Z>` and never touches
`latest`, every other run publishes `foreman:latest`. It carries the server,
the Job-side supervisor, `foreman-gc` and the reference Job execution adapters
(`foreman-job-cli`, `rsbx-remote`, `foreman-job-register`, `foreman-job-runner`,
`foreman-job-slots`). `.github/workflows/` is the whole release path.
`make build-foreman-image` is the single-platform local
equivalent (`ARCH=` picks the platform). Outside the release path, a pull
request into `main` is checked by `pr-check.yml` — `go build ./...`,
`go test -race ./...`, and a two-platform image build that publishes nothing.

The Job carrier is not built here: it is the externally published
`ghcr.io/tsic404/rsbx` image, pinned per attempt by digest. The carrier
preinstalls neither the Multica CLI nor an Agent CLI. At start-up the Job's
init container copies the supervisor from the Foreman image into an emptyDir,
and the supervisor fetches both frozen CLI artifacts through the Foreman
proxy (`/cli/...`) within the attempt's download budget before it reports
ready, registers or delivers.

The acceptance cases run against their own test multica cluster, deployed from
`testinfra/test-multica/` (deploy, reset, recycle, CLI profile — see its
README): a test-environment component, not part of `deploy/`.

Deployment lives in `deploy/` (namespaces, RBAC, Secrets, Deployment/Service,
`foreman-gc` DaemonSet) and is applied with `kubectl apply -f deploy/`; see
`deploy/README.md` for the Secret values, the digest-pinned rsbx carrier and
the supervisor runtime image, the CLI proxy face and the node-local state
root.

## Status

Early development. The protocol facts are re-verified against the Multica
release each acceptance round deploys (currently `v0.11.1`, see
`docs/upstream-anchors.md`).

## License

MIT
