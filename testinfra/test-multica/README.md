# Test multica cluster

The execution environment of the TC-tech cases: one complete, isolated,
resettable multica deployment — server, persistence and frontend entry — as
`test-cases/_infra/test-multica-cluster.md` §1/§2/§3/§7 require. It is **not**
part of the Foreman deployment: `deploy/` stays the product's manifests, this
component is the test environment the acceptance runs against.

The fault-injection access path that sits in front of this server is a
separate component (`testinfra/injectproxy`); this directory delivers the
server itself and its direct address.

## Layout

| File | Purpose |
|---|---|
| `deploy.yaml` | The whole deployment: namespace, backend config, frontend entry config, PVCs, postgres/backend/web/proxy Deployments, Services |
| `deploy.sh` | Deployment entry (§7 部署): renders the entry origin, creates the Secret, applies, waits for the rollout |
| `reset.sh` | Reset entry (§7 重置): wipes runtime/task rows, rebuilds the §3 objects, asserts the reset-time self-checks |
| `bind-agent.sh` | Binds the test agent to the runtime Foreman registers (the step between "Foreman is up" and "tasks can be dispatched") |
| `recycle.sh` | Recycle entry (§7 回收): deletes the namespace, nothing else |
| `multica-test` | CLI wrapper — the profile, with the platform's task environment cleared |
| `lib.sh` | Shared settings and helpers for the scripts |

## Addresses

| Face | In-cluster | Test host |
|---|---|---|
| Direct address (§2 直连地址, the server itself) | `http://test-multica-server:8080` | `http://127.0.0.1:30080` |
| Frontend entry (§2, web UI + `/ws`) | `http://test-multica-web` | `http://127.0.0.1:30300` |

The node shares the test host's network namespace, so the node ports in
`deploy.yaml` are reachable on the host itself: loopback is the stable form of
both faces, and the default. `TEST_MULTICA_HOST` (or
`TEST_MULTICA_SERVER_URL` / `TEST_MULTICA_ENTRY_URL`) overrides it for
consumers that are not on this host. The node's InternalIP is deliberately not
the default — it is the host's own address and moves underneath a running
deployment, which would strand both the entry origin baked into the server's
config and the address recorded in the CLI profile.

The direct address is what the environment self-check and the fault-injection
access path's upstream use (`INJECT_UPSTREAM_URL`, in-cluster
`http://test-multica-server:8080`); the stable access point
`http://<test-multica-access>` is the injection path's own Service, not this
one.

The only host-specific value in the manifests is the entry origin
(`MULTICA_APP_URL` / `FRONTEND_ORIGIN` / `CORS_ALLOWED_ORIGINS`, because the
login redirects and CORS checks use it). It is also stamped into the backend's
pod template, so re-running `./deploy.sh` after the origin moves rolls the
server instead of leaving it answering with the old one.

## Version anchor (§1/§7)

Absolute, not drifting: `deploy.yaml` pins every image to the `v0.6.1` release
(`multica@2ea01ae4`) — `ghcr.io/multica-ai/multica-backend:v0.6.1`,
`ghcr.io/multica-ai/multica-web:v0.6.1`, with `pgvector/pgvector:pg17` and
`nginx:1.27-alpine` as the persistence and entry layers. The Job image must
carry the same release's CLI (`MULTICA_CLI_VERSION=0.6.1`, the CI default).

Upgrading means rebuilding the environment against a new anchor
(`./recycle.sh && ./deploy.sh`), never an in-place upgrade. The reset entry
asserts the anchor from the server's own `/api/config` (self-check ④).

## Operating the environment

```bash
./deploy.sh                 # deploy / re-deploy (idempotent)
./reset.sh                  # before every acceptance round
./bind-agent.sh             # after Foreman registered its runtime
./recycle.sh                # tear the whole environment down

kubectl -n multica-test logs deploy/backend --tail=200     # troubleshooting (§7 排障)
kubectl -n multica-test get pods,svc
```

**Order matters.** `reset.sh` leaves the runtime list empty on purpose (§1 可重置
/ self-check ③), so the test agent cannot be bound yet; `bind-agent.sh` binds
it to the `provider=omp` runtime Foreman registers when it starts. The full
round is:

```
./deploy.sh
./reset.sh                     # ③ empty runtime list, ④ version anchor
<wiring task: Foreman deployed with MULTICA_SERVER_URL → the access point>
./bind-agent.sh                # agent ← the omp runtime Foreman registered
<TC-tech cases>
```

`reset.sh` runs the self-checks this component owns (③ the empty runtime list,
④ the version anchor). ① the injection path's zero-injection state, ② the
instance under test pointing at the test access point, ⑤ the transparency probe
and the §6 NetworkPolicy enforcement check belong to the wiring task — they
need the injection path and the Foreman Deployment.

### Reset semantics

`reset.sh` deletes the test workspace's **runtime rows and task rows**, then
rebuilds the §3 objects; the account, workspace, repository, agent and chat
session are looked up by name and only created when missing. It is idempotent
and safe to run between rounds. The autopilot is `bind-agent.sh`'s, not this
entry's: the server refuses one whose assignee has no runtime.

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

Wiring inputs, on demand:

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
- The proxy is an entry, not a fault-injection surface: it forwards bytes and
  carries no protocol semantics. All injection lives in the access path.
