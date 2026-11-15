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

## Local end-to-end test (docker or podman)

`test-real-daemon.sh` proves the whole dispatch chain against a real Multica
stack and a real `multica daemon` — no Kubernetes, no fakes:

```bash
bash deploy/test-real-daemon.sh          # reuse the running stack
bash deploy/test-real-daemon.sh --reset  # drop the stack's volumes first
docker compose -f deploy/test-compose.yaml down -v   # tear down afterwards
```

The script picks the engine automatically: `docker compose` when a docker
daemon answers, `podman compose` otherwise. It then, in order:

1. brings up `test-compose.yaml` — postgres/pgvector, the multica backend
   (migrations run on start), the web UI, and an nginx proxy;
2. extracts the official `multica` CLI out of the backend image and prints its
   version;
3. mints a personal access token (retrying the documented 429), creates the
   workspace and an agent, and binds the agent to Foreman's runtime;
4. builds and launches `bin/foreman`, then a real daemon **in an isolated
   `$HOME`** pointed at Foreman's daemon face;
5. triggers an autopilot and waits for the backend to log a **new**
   `INF task completed` — the full-chain proof — falling back to a start-only
   match so a partial chain is still visible.

Expected last line:

```
TASK COMPLETED: multica settled a task Foreman dispatched and forwarded end to end
```

`test-compose.yaml` reads `deploy/.env` (gitignored; copy `.env.example` and
fill it). The test is validated on both engines and against multica v0.9.0.

### Agent requirement

The daemon needs an agent CLI on `PATH`. `omp` needs one extra fixture: the
daemon pre-creates an empty `--session` file and omp refuses to resume it, so
the script drops a small shim in the isolated `$HOME` that seeds one session
header line before exec'ing the real binary. Foreman never touches it — it is
an agent-side fixture for the test environment only.

## Post-install checks

```bash
kubectl -n foreman get pods
kubectl -n foreman logs deploy/foreman | grep -E 'foreman.started|daemon.registered'
curl -s localhost:8080/foreman/healthz   # via port-forward: status ok
```

A daemon's first registration logs `daemon.registered` with its `daemon_id`,
`runtime_id` and `workspace_id`.
