# Deploying Foreman (v2)

`deploy/` is the whole deployment surface. Everything is plain YAML —
`kubectl apply -f deploy/` installs it in one pass.

| File | Objects |
|---|---|
| `00-namespaces.yaml` | `Namespace foreman` (restricted PSA) |
| `10-secrets.yaml` | `Secret foreman-secret` (server + ops credential), `Secret foreman-daemons` (the daemon credential set), `Secret registry-tsic` (GHCR pull secret) |
| `20-rbac.yaml` | `ServiceAccount foreman` — no Role: v2 never calls the Kubernetes API, and the token is not mounted |
| `30-foreman.yaml` | `Deployment foreman` + `Service foreman` (daemon face `:8080`) + `Service foreman-ops` (`:8081`) |

v2 creates no Kubernetes objects of its own: there is no per-task Job, no
per-task Secret, no node-local cache DaemonSet, and no agent namespace. The
`foreman` namespace and Foreman's own Deployment are the whole installation.

```bash
kubectl apply -f deploy/
kubectl -n foreman rollout status deploy/foreman
```

## Two listeners

| Listener | Address | Content |
|---|---|---|
| daemon face | `:8080` (Service `foreman`) | `/api/*` for the resident daemons, plus `/foreman/healthz` |
| ops surface | `:8081` (Service `foreman-ops`) | `/metrics`, `/foreman/tasks*`, `/foreman/version` |

The daemon face publishes only `:8080`. Every ops request must present a bearer
credential:

```bash
TOKEN=$(kubectl -n foreman get secret foreman-secret -o jsonpath='{.data.FOREMAN_OPS_TOKEN}' | base64 -d)
kubectl -n foreman port-forward svc/foreman-ops 8081:8081 &
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/foreman/tasks
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/metrics | grep -cE '^foreman_'
```

Without a credential every route answers `401`.

## Secrets

`10-secrets.yaml` ships placeholders only; replace them before applying.

| Key | Meaning |
|---|---|
| `MULTICA_TOKEN` | the server credential (`mdt_`/`mul_`). Workspace-scoped form is bound to `MULTICA_WORKSPACE_ID`; the discovery form lists every workspace the credential sees. |
| `FOREMAN_OPS_TOKEN` | the ops-surface bearer credential. `REPLACE_ME` is refused at startup. |
| `FOREMAN_DAEMON_TOKENS` | the daemon credential set, `"<daemon-id>=<token>[, …]"`. |
| `FOREMAN_DAEMON_TOKEN_WORKSPACES` | each daemon identity's workspace, `"<daemon-id>=<workspace-id>[, …]"`. A daemon with no binding refuses startup. |

Generate a token with `head -c 32 /dev/urandom | xxd -p -c 64`.

## Daemon credentials

The resident daemons are configured, not discovered. Each presents its token as
a bearer credential on every control-plane call (register, heartbeat, claim,
report). The credential set is loaded once at startup:

- a token shared by two daemon identities refuses startup;
- a daemon identity with no workspace binding refuses startup;
- an empty set refuses startup — Foreman cannot serve a daemon it cannot
  authenticate.

Tokens are compared by SHA-256 digest in constant time and are never logged.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `MULTICA_SERVER_URL` | — | the multica server base URL |
| `MULTICA_WORKSPACE_ID` | — | the served workspace (workspace-scoped form) |
| `FOREMAN_DAEMON_ID` | `foreman` | the daemon identity Foreman registers under |
| `FOREMAN_CLAIM_INTERVAL` | `5s` | claim cadence |
| `FOREMAN_CLAIM_INTERVAL_IDLE` | `30s` | claim cadence when no daemon is idle |
| `FOREMAN_LEASE_REFRESH_INTERVAL` | `15s` | daemon-identity heartbeat cadence (C-2); refused above `20s`. The name is historical — task leases are renewed by C-4 from the convergence round below, not by this value |
| `FOREMAN_RECONCILE_INTERVAL` | `30s` | convergence round cadence. `0` is refused: it would disable both the boot bound and the offline sweep |
| `FOREMAN_HEARTBEAT_TIMEOUT` | `60s` | how long a runtime may stay silent before the sweep marks it offline. `0` disables the sweep |
| `FOREMAN_JOB_BOOT_TIMEOUT` | `60s` | claim → daemon start bound |
| `FOREMAN_TASK_MAX_DURATION` | — (unbounded) | how long one started task may run before each reconcile round reads the server's own status (C-13) and aligns to it. The server keeps the verdict — Foreman does not fail the task itself. Round cadence is `FOREMAN_RECONCILE_INTERVAL`, so set this comfortably above it |
| `FOREMAN_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |
| `FOREMAN_OPS_TOKEN` | — | the ops-surface credential |
| `FOREMAN_OPS_WORKSPACE_TOKENS` | — | per-workspace ops credentials, `"<workspace-id>=<token>[, …]"`. The order matters and the failure mode is misleading: a reversed pair loads fine (the workspace id becomes a token nobody presents), so every request with the intended token answers 401 rather than a configuration error. If a bound credential unexpectedly 401s, check the pair order first |
| `FOREMAN_WORKSPACES` | — | JSON identity list: names every served workspace's daemon identity explicitly (workspace-scoped form, alternative to `MULTICA_WORKSPACE_ID`) |
| `FOREMAN_WORKSPACES_FILE` | — | path to the same JSON list |
| `FOREMAN_WORKSPACES_MANIFEST` | — | JSON list of workspace ids the user-scoped credential may serve; workspaces outside it are excluded. Refuses startup when a listed workspace is not visible |
| `FOREMAN_WORKSPACES_MANIFEST_FILE` | — | path to the same manifest |


## Migrating an existing v1 Deployment

`kubectl apply` merges into the live object; it does not replace the container's
env list. A Deployment upgraded in place therefore **keeps every v1 variable**
it does not explicitly remove, and v2 rejects some of them — a v1
`FOREMAN_TASK_MAX_DURATION=86400` (bare seconds) crashes v2 at startup with
`time: missing unit in duration "86400"`, because v2 parses a Go duration.

Replace rather than apply when cutting over, so the env list is exactly what
the file says:

```bash
kubectl replace -f deploy/30-foreman.yaml
```

The v1 keys v2 no longer understands, which must not survive the cutover:

`FOREMAN_JOB_TOKEN_KEY`, `FOREMAN_JOB_TOKEN_TTL`, `FOREMAN_JOB_NAMESPACE`,
`FOREMAN_JOB_IMAGE`, `FOREMAN_MAX_INFLIGHT_JOBS`, `FOREMAN_CLAIM_BATCH_MAX`,
`FOREMAN_JOB_DRAIN_TIMEOUT`, `FOREMAN_JOB_TTL_SECONDS`, `FOREMAN_STATE_ROOT`,
`FOREMAN_REPO_CACHE_MODE`, `FOREMAN_PREFER_NODE_REUSE`,
`FOREMAN_MAX_JOBS_PER_NODE`, `FOREMAN_CACHE_MAX_BYTES`, `FOREMAN_CACHE_TTL`,
`FOREMAN_TASK_DIR_TTL`, `FOREMAN_GC_INTERVAL`, `FOREMAN_GC_QUIESCE`,
`FOREMAN_METRICS_ENABLED`, `FOREMAN_WS_ENABLED`.

Two further cutover traps:

- The `selector` is immutable. A live v1 Deployment selected on
  `app.kubernetes.io/name` alone cannot accept a manifest that also selects
  `app.kubernetes.io/component`.
- `strategy: Recreate` is required. v1's Job-per-task model and v2's resident
  daemons cannot share a rollout, so the switch costs a short outage.

## Daemons outside the cluster

The resident daemons usually run on other machines, so a ClusterIP is not
reachable from where they live. `30-foreman.yaml` therefore also publishes the
daemon face as `Service foreman-daemon` (NodePort `30080`) and, when an ingress
controller is present, as `foreman.tsic.top`. The daemons' `server_url` is
whichever of those they can reach; the ops face is never published.

## Post-install checks

```bash
kubectl -n foreman get pods
kubectl -n foreman logs deploy/foreman | grep -E 'foreman.started|daemon.registered'
curl -s localhost:8080/foreman/healthz   # via port-forward: status ok
```

A daemon's first registration logs `daemon.registered` with its `daemon_id`,
`runtime_id` and `workspace_id`.
