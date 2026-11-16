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
| `FOREMAN_LEASE_REFRESH_INTERVAL` | `15s` | prepare-lease renewal (refused above `20s`) |
| `FOREMAN_RECONCILE_INTERVAL` | `30s` | convergence round cadence |
| `FOREMAN_JOB_BOOT_TIMEOUT` | `60s` | claim → daemon start bound |
| `FOREMAN_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |
| `FOREMAN_OPS_TOKEN` | — | the ops-surface credential |

## Post-install checks

```bash
kubectl -n foreman get pods
kubectl -n foreman logs deploy/foreman | grep -E 'foreman.started|daemon.registered'
curl -s localhost:8080/foreman/healthz   # via port-forward: status ok
```

A daemon's first registration logs `daemon.registered` with its `daemon_id`,
`runtime_id` and `workspace_id`.
