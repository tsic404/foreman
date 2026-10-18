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
make build-foreman-image  # container image for the Deployment (linux/$(ARCH))
make build-job-image      # Job image: upstream multica CLI + foreman-gc + dumb-init
make check                # gofmt + vet + tests + build
```

Both images are built and published to GHCR as multi-platform manifest lists
(`linux/amd64` + `linux/arm64`) by the workflows in `.github/workflows/`, each
on its own release line: `build-foreman.yml` publishes `foreman` (a `v*.*.*`
tag push publishes exactly `foreman:v<X.Y.Z>` and never touches `latest`, a
`main` push exactly `foreman:latest`), `build-foreman-job.yml` publishes
`foreman-job` (tagged with the upstream daemon version it carries,
resolved from upstream's newest release at build time, `latest` from the newest
release) and skips a version that is already published. `.github/workflows/` is
the whole release path and needs nothing from this repo beyond the two
Containerfiles. The `make build-*-image` targets are the single-platform local
equivalents (`ARCH=` picks the platform). Outside the release path, a pull
request into `main` is checked by `pr-check.yml` — `go build ./...`,
`go test ./...`, and a two-platform image build that publishes nothing.

The acceptance cases run against their own test multica cluster, deployed from
`testinfra/test-multica/` (deploy, reset, recycle, CLI profile — see its
README): a test-environment component, not part of `deploy/`.

Deployment lives in `deploy/` (namespaces, RBAC, Secrets, Deployment/Service,
`foreman-gc` DaemonSet) and is applied with `kubectl apply -f deploy/`; see
`deploy/README.md` for the Secret values, the movable Job image reference and
the node-local state root.

## Status

Early development. The protocol feasibility has been validated against Multica daemon `v0.6.0`.

## License

MIT
