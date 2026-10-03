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

## Status

Early development. The protocol feasibility has been validated against Multica daemon `v0.6.0`.

## License

MIT
