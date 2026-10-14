# Test multica cluster

The execution environment of the TC-tech cases: one complete, isolated,
resettable multica deployment — server, persistence and frontend entry — as
`test-cases/_infra/test-multica-cluster.md` §1/§2/§3/§6 require. It is **not**
part of the Foreman deployment: `deploy/` stays the product's manifests, this
component is the test environment the acceptance runs against, plus the wiring
and self-check entries that point the instance under test at it.

Nothing sits in front of the server: the stable access point (§2) is the server
itself, one face per consumer (see §Addresses).

## Layout

| File | Purpose |
|---|---|
| `deploy.yaml` | The whole deployment: namespace, backend config, frontend entry config, PVCs, postgres/backend/web/proxy Deployments, Services |
| `deploy.sh` | Deployment entry (§6 部署): renders the entry origin, creates the Secret, applies, waits for the rollout |
| `reset.sh` | Reset entry (§6 重置): stops the instance under test, wipes runtime/task rows, rebuilds the §3 objects, runs the §2 gate |
| `wire.sh` | Wiring entry (§2 接线): points the instance under test at the access point and starts it |
| `bind-agent.sh` | Binds the test agent to the runtime the instance registers (the step between "the instance is up" and "tasks can be dispatched") |
| `recycle.sh` | Recycle entry (§6 回收): deletes the namespace, nothing else |
| `selfcheck.sh` | The §2 ①②③ environment gate — `reset.sh` and `wire.sh` run it at the pre-start moment |
| `multica-test` | CLI wrapper — the profile, with the platform's task environment cleared |
| `lib.sh` | Shared settings and helpers for the scripts |

## Addresses

| Face | In-cluster | Test host |
|---|---|---|
| Access point (§2, the server itself) | `http://test-multica-server:8080` | `http://127.0.0.1:30080` |
| Frontend entry (§2, web UI + `/ws`) | `http://test-multica-web` | `http://127.0.0.1:30300` |

The node shares the test host's network namespace, so the node ports in
`deploy.yaml` are reachable on the host itself: loopback is the stable form of
both faces, and the default. `TEST_MULTICA_HOST` (or
`TEST_MULTICA_SERVER_URL` / `TEST_MULTICA_ENTRY_URL`) overrides it for
consumers that are not on this host. The node's InternalIP is deliberately not
the default — it is the host's own address and moves underneath a running
deployment, which would strand both the entry origin baked into the server's
config and the address recorded in the CLI profile.

The access point is what the instance under test is wired to. `ACCESS_URL` is
its in-cluster face (`http://test-multica-server.<namespace>.svc:8080`) — the
value `MULTICA_SERVER_URL` must equal, and what `./wire.sh` writes; `SERVER_URL`
is its host face (the node port on loopback), which the CLI profile and the
host-side entries use. Both are that one server, and neither is the production
address.

The only host-specific value in the manifests is the entry origin
(`MULTICA_APP_URL` / `FRONTEND_ORIGIN` / `CORS_ALLOWED_ORIGINS`, because the
login redirects and CORS checks use it). It is also stamped into the backend's
pod template, so re-running `./deploy.sh` after the origin moves rolls the
server instead of leaving it answering with the old one.

## Version anchor (§1/§6)

Absolute, not drifting: `deploy.yaml` pins every image to the `v0.6.1` release
(`multica@2ea01ae4`) — `ghcr.io/multica-ai/multica-backend:v0.6.1`,
`ghcr.io/multica-ai/multica-web:v0.6.1`, with `pgvector/pgvector:pg17` and
`nginx:1.27-alpine` as the persistence and entry layers. The Job image must
carry the same release's CLI (`MULTICA_CLI_VERSION=0.6.1`, the CI default).

Upgrading means rebuilding the environment against a new anchor
(`./recycle.sh && ./deploy.sh`), never an in-place upgrade. The §2 gate asserts
the anchor from the server's own `/api/config` (self-check ③, two stages: a
missing field is a data-surface mismatch, not a version mismatch).

## Operating the environment

```bash
./deploy.sh                 # deploy / re-deploy the test cluster (idempotent)
./reset.sh                  # before every acceptance round
./wire.sh                   # point the instance under test at the access point
./bind-agent.sh             # after the instance registered its runtime
./recycle.sh                # tear the whole environment down

kubectl -n multica-test logs deploy/backend --tail=200     # troubleshooting (§6 排障)
kubectl -n multica-test get pods,svc
```

**Order matters.** `reset.sh` stops the instance under test and leaves the
runtime list empty on purpose (§2 ②), so the test agent cannot be bound yet;
`wire.sh` starts the instance at the access point; `bind-agent.sh` binds the
agent to the `provider=omp` runtime it registers. The full round is:

```
./deploy.sh                    # the test cluster itself
./reset.sh                     # stop, wipe, rebuild §3, §2 ①②③
./wire.sh                      # MULTICA_SERVER_URL → the access point, start
./bind-agent.sh                # agent ← the omp runtime the instance registered
<TC-tech cases>
```

`reset.sh` and `wire.sh` both run `./selfcheck.sh` — the §2 ①②③ gate — while
the instance is stopped: ① it points at the access point (never the production
address), ② the runtime list is empty, ③ the server reports the artifact
anchor. Any failure is `QA_BLOCKED` (exit 2); once the instance runs, ②'s
assertion becomes "exactly one `provider=omp` runtime", which
TC-tech-protocol-01 judges.

On a fresh environment the first `reset.sh` stops at ① — the Deployment still
carries the production address from `deploy/` — and `./wire.sh` is what fixes
it; from then on the round above runs through.

### Reset semantics

`reset.sh` stops the instance under test, deletes the test workspace's
**runtime rows and task rows**, then rebuilds the §3 objects; the account,
workspace, repository, agent and chat session are looked up by name and only
created when missing. It is idempotent and safe to run between rounds. The
autopilot is `bind-agent.sh`'s, not this entry's: the server refuses one whose
assignee has no runtime.

The wipe is SQL inside the persistence pod (`kubectl exec deploy/postgres`) —
the server has no API for deleting task history, and a runtime delete detaches
tasks instead of removing them.

## §3 objects

| Object | Name | Used by |
|---|---|---|
| Workspace + loginable account | `foreman-test` (`Foreman Test`), `acceptance@test.local` | task cancellation, board, chat entry |
| Test agent | `foreman-test-agent` | dispatch (its runtime is the one Foreman registers) |
| Repository | `https://github.com/tsic404/foreman` | agent checkout, cache cases |
| Autopilot (manual trigger) | `foreman-test-autopilot` | `./multica-test autopilot trigger <id>` |
| Chat entry | chat session `foreman-test-chat` | messages to the agent |
| Task status query surface | `./multica-test runtime list` / board at the entry origin | runtime, task and cancellation state |

The repository is the implementation repo, which the acceptance side can write
to with its own git credentials. The autopilot is created by `bind-agent.sh`
rather than by `reset.sh`: the server refuses an autopilot whose assignee agent
has no runtime, and the reset entry leaves the runtime list empty on purpose.

The login is deterministic: `APP_ENV=development` plus the fixed verification
code in the Secret. The account is created on first login; no password is
stored anywhere.

## CLI profile (§2 查询面凭据)

`reset.sh` writes `~/.multica/profiles/test-multica/config.json` — server URL,
entry URL, workspace id and a personal access token (`mul_`) minted by the
test server for that workspace. It never touches the production profile.

Run everything through the wrapper:

```bash
./multica-test runtime list --output json
./multica-test agent tasks <agent-id>
./multica-test autopilot trigger <autopilot-id>
```

The wrapper exists because a process that is itself running inside a Multica
task pod carries `MULTICA_SERVER_URL` / `MULTICA_WORKSPACE_ID` / `MULTICA_TOKEN`
in its environment (the CLI resolves the environment before the profile, so a
bare `multica --profile test-multica …` would talk to production) and a daemon
task marker in its working directory (which makes the CLI refuse a non-task
token). It clears both and then execs the CLI from a neutral directory.

Wiring inputs: `./wire.sh` reads all three out of this profile and writes them
into the instance under test (`MULTICA_SERVER_URL` → the access point,
`MULTICA_WORKSPACE_ID` → the workspace, `MULTICA_TOKEN` → the token). On demand:

```bash
./reset.sh --print-token        # the profile's token → MULTICA_TOKEN
./multica-test workspace list   # the workspace id → MULTICA_WORKSPACE_ID
```

## Boundaries

- The environment is reachable from the test network only: NodePorts on the
  test cluster's node, no ingress, no public DNS. The fixed development
  verification code makes every account on it loginnable — never expose it.
- Nothing here touches production: a separate namespace, a separate database,
  a separate workspace, and a separate CLI profile. `recycle.sh` deletes the
  namespace and stops there — the `foreman` and `multica-agents` namespaces are
  the instance under test's, not this component's.
- The proxy is the frontend entry: it forwards bytes and carries no protocol
  semantics. The instance under test reaches the server directly, at the access
  point (§2 接入点与接线).
