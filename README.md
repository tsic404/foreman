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
make build-foreman-image  # container image for the Deployment
make build-job-image      # Job image: upstream multica CLI + omp + foreman-gc
make check                # gofmt + vet + tests + build
```

Both images are built and published to GHCR by
`.github/workflows/build-images.yml` on every push to `main` and on `v*.*.*`
tags; `make resolve-upstream` and `make publish-images` are the local
equivalents of what that workflow runs, and `make smoke-job-image` verifies a
published Job image.

Deployment lives in `deploy/` (namespaces, RBAC, Secrets, Deployment/Service,
`foreman-gc` DaemonSet) and is applied with `kubectl apply -f deploy/`; see
`deploy/README.md` for the Secret values, the Job image digest pinning and the
node-local state root.

## Status

Early development. The protocol feasibility has been validated against Multica daemon `v0.6.0`.

## License

MIT
