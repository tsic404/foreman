# Test multica cluster

The execution environment of the TC-tech cases: one complete, isolated,
resettable multica deployment — server, persistence and frontend entry. It is
**not** part of the Foreman deployment: `deploy/` stays the product's
manifests, this component is the test environment the acceptance runs
against, plus the wiring and self-check entries that point the instance under
test at it.

Nothing sits in front of the server: the stable access point is the server
itself, one face per consumer (see §Addresses).

## Layout

| File | Purpose |
|---|---|
| `deploy.yaml` | The whole deployment: namespace, backend config, frontend entry config, PVCs, postgres/backend/web/proxy Deployments, Services |
| `deploy.sh` | Deployment entry: renders the entry origin, creates the Secret, applies, waits for the rollout |
| `reset.sh` | Reset entry: stops the instance under test, wipes runtime/task rows, rebuilds the test objects, registers the model endpoint when its triple is supplied, runs the environment gate |
| `wire.sh` | Wiring entry: points the instance under test at the access point and starts it |
| `bind-agent.sh` | Binds the test agent to the runtime the instance registers (the step between "the instance is up" and "tasks can be dispatched") |
| `recycle.sh` | Recycle entry: deletes the namespace, nothing else |
| `selfcheck.sh` | The environment gate ①②③ — `reset.sh` and `wire.sh` run it at the pre-start moment |
| `multica-test` | CLI wrapper — the profile, with the platform's task environment cleared |
| `lib.sh` | Shared settings and helpers for the scripts, including the model-endpoint registration |

## Addresses

| Face | In-cluster | Test host |
|---|---|---|
| Access point (the server itself) | `http://test-multica-server:8080` | `http://127.0.0.1:30080` |
| Frontend entry (web UI + `/ws`) | `http://test-multica-web` | `http://127.0.0.1:30300` |

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

## Version anchor

Absolute, not drifting: `deploy.yaml` pins every image to the `v0.6.1` release
(`multica@2ea01ae4`) — `ghcr.io/multica-ai/multica-backend:v0.6.1`,
`ghcr.io/multica-ai/multica-web:v0.6.1`, with `pgvector/pgvector:pg17` and
`nginx:1.27-alpine` as the persistence and entry layers. The Job image is not
part of this pin: `build-foreman-job.yml` resolves upstream's newest release at
build time and tags the image with that daemon version (`latest` follows the
newest release), so the CLI a Job runs can be newer than the `v0.6.1` server.

Upgrading means rebuilding the environment against a new anchor
(`./recycle.sh && ./deploy.sh`), never an in-place upgrade. The environment
gate asserts the anchor from the server's own `/api/config` (self-check ③, two
stages: a missing field is a data-surface mismatch, not a version mismatch).

## Operating the environment

```bash
./deploy.sh                 # deploy / re-deploy the test cluster (idempotent)
./reset.sh                  # before every acceptance round
./wire.sh                   # point the instance under test at the access point
./bind-agent.sh             # after the instance registered its runtime
./recycle.sh                # tear the whole environment down

kubectl -n multica-test logs deploy/backend --tail=200     # troubleshooting
kubectl -n multica-test get pods,svc
```

**Order matters.** `reset.sh` stops the instance under test and leaves the
runtime list empty on purpose (gate ②), so the test agent cannot be bound yet;
`wire.sh` starts the instance at the access point; `bind-agent.sh` binds the
agent to the `provider=omp` runtime it registers. The full round is:

```
./deploy.sh                    # the test cluster itself
./reset.sh                     # stop, wipe, rebuild objects, gate ①②③
./wire.sh                      # MULTICA_SERVER_URL → the access point, start
./bind-agent.sh                # agent ← the omp runtime the instance registered
<TC-tech cases>
```

`reset.sh` and `wire.sh` both run `./selfcheck.sh` — the environment gate
①②③ — while the instance is stopped: ① it points at the access point (never
the production address), ② the runtime list is empty, ③ the server reports the
artifact anchor. Any failure is `QA_BLOCKED` (exit 2); once the instance runs,
②'s assertion becomes "exactly one `provider=omp` runtime", which
TC-tech-protocol-01 judges.

On a fresh environment the first `reset.sh` stops at ① — the Deployment still
carries the production address from `deploy/` — and `./wire.sh` is what fixes
it; from then on the round above runs through.

### Reset semantics

`reset.sh` stops the instance under test, deletes the test workspace's
**runtime rows and task rows**, then rebuilds the test objects; the account,
workspace, repository, agent and chat session are looked up by name and only
created when missing. It is idempotent and safe to run between rounds. The
provider registration below is applied in place for the same reason. The
autopilot is `bind-agent.sh`'s, not this entry's: the server refuses one whose
assignee has no runtime.

The wipe is SQL inside the persistence pod (`kubectl exec deploy/postgres`) —
the server has no API for deleting task history, and a runtime delete detaches
tasks instead of removing them.

## Objects the test env creates

| Object | Name | Used by |
|---|---|---|
| Workspace + loginable account | `foreman-test` (`Foreman Test`), `acceptance@test.local` | task cancellation, board, chat entry |
| Test agent | `foreman-test-agent` | dispatch (its runtime is the one Foreman registers) |
| Repository | `https://github.com/tsic404/foreman` | agent checkout, cache cases |
| Autopilot (manual trigger) | `foreman-test-autopilot` | `./multica-test autopilot trigger <id>` |
| Chat entry | chat session `foreman-test-chat` | messages to the agent |
| Task status query surface | `./multica-test runtime list` / board at the entry origin | runtime, task and cancellation state |
| Model provider registration | `foreman-test/<model id>` in `${FOREMAN_STATE_ROOT}/home/<workspace>/.omp/agent/models.yml`; Secret `test-multica-model-provider` | the test agent's model (only when the endpoint is supplied) |

The repository is the implementation repo, which the acceptance side can write
to with its own git credentials. The autopilot is created by `bind-agent.sh`
rather than by `reset.sh`: the server refuses an autopilot whose assignee agent
has no runtime, and the reset entry leaves the runtime list empty on purpose.

The login is deterministic: `APP_ENV=development` plus the fixed verification
code in the Secret. The account is created on first login; no password is
stored anywhere.

## Model endpoint (部署参数)

The test agent's tasks run in Foreman's Job pods, and the agent CLI inside a
pod can only finish a task when it has a model endpoint. The endpoint is a
deployment parameter of this environment, never a manual UI step: every round
rebuilds the test-multica namespace, so anything configured by hand would be
gone by the next one.

| Variable | Meaning |
|---|---|
| `TEST_MULTICA_MODEL_BASE_URL` | The endpoint's API root (e.g. `https://gateway.example/v1`) |
| `TEST_MULTICA_MODEL_ID` | The model id the endpoint serves — the bare id, not a selector |
| `TEST_MULTICA_MODEL_API_KEY` / `TEST_MULTICA_MODEL_API_KEY_FILE` | The credential: a value, or a file the operator mounts |
| `TEST_MULTICA_MODEL_API` | Optional: `openai-completions` (default) or `anthropic-messages` |
| `TEST_MULTICA_MODEL_PROVIDER` | Optional provider id (default `foreman-test`) |
| `TEST_MULTICA_STATE_ROOT` | Optional: fallback node state root when the instance Deployment cannot be read (default `/var/lib/foreman`); the Deployment's `FOREMAN_STATE_ROOT` wins |
| `TEST_MULTICA_MODEL_WAIT` | Optional: how long one carrier Pod may take, image pull included (default 600s) |
| `TEST_MULTICA_MODEL_IMAGE` | Optional: the shell image the carrier Pods run (default `alpine:3.20`) |

`./reset.sh` registers the triple when it is present; when it is absent it
removes any registration a previous round left behind, so the round really is
the baseline:

- **present** — one Secret (`test-multica-model-provider`) carries the
  registration; one one-shot Pod per schedulable Linux node materialises it as
  `<state-root>/home/<workspace>/.omp/agent/models.yml`, the node-local provider
  configuration the Job pods mount (`02-contracts.md §8.7`). The test agent's
  `model` then points at `<provider>/<model id>`. The file holds exactly one
  provider with one model, is owned by the Job's agent uid (1000) and is mode
  0600. The carrier Pods run a small public Alpine image (a shell and busybox
  are all they need), so the namespace admits hostPath pods (PSA `privileged`,
  see `deploy.yaml`).
- **absent** — the entry prints 「未配置模型端点」 and registers nothing: tasks
  keep the baseline behaviour (they converge to `process_failure` quickly), and
  no production endpoint, staging value or credential is ever substituted. The
  rollback is unconditional — every endpoint-less reset removes the node-local
  file, deletes the Secret (`--ignore-not-found`) and clears the agent's
  `model`, whether or not the agent row still carries the old selector. That is
  what keeps the guarantee when `recycle.sh` has deleted the namespace: the
  node-local file outlives it, and the recreated agent row carries no evidence
  of the previous registration.
- **partial** — the entry fails and names the missing part instead of guessing.

The credential travels only through the environment or a mounted file. It is
in no file of this repository, not in `deploy.yaml`, and not in any example
manifest; it reaches the carrier Pods through the Secret volume (never an
argument), and rests afterwards only in the node-local file the Job pods read —
the same trust level the design already gives the node home (ADR-018).

Idempotent: a second `./reset.sh` applies the same Secret in place, overwrites
the same file on each node and sets the same model on the same agent — one
provider registration and one test agent. Verify with:

```bash
kubectl -n multica-test get secret test-multica-model-provider \
	-o jsonpath='{.data.models\.yml}' | base64 -d     # one provider, one model
kubectl debug node/<node> --profile=sysadmin --image=debian:bookworm-slim -qi -- \
	cat /host/var/lib/foreman/home/<workspace>/.omp/agent/models.yml
./multica-test agent list --output json | jq '.[] | select(.name == "foreman-test-agent") | .model'
```

A stub endpoint — one that accepts a connection and never answers — only
lengthens the agent's life (the long-window cases); it produces no completion
terminal, so it is a diagnostic, never a default and never an acceptance
substitute.

## CLI profile (查询面凭据)

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
  point.
- The model endpoint registration is the one thing this component writes
  outside its own namespace: `reset.sh` puts it where the Job pods mount their
  provider configuration (the instance under test's node-local home), because
  that is the only face the agent CLI reads (`02-contracts.md §8.7`). The
  credential therefore rests in that node home (the design already gives the
  node home that trust level, ADR-018) and in the test namespace's Secret —
  both disposable, both test-only. Never point this parameter at a production
  endpoint or credential.
- **Explicit acceptance — the test namespace is admitted with PSA
  `privileged`.** The label is static in `deploy.yaml`, so it applies to every
  round, not only to rounds that supply the endpoint. The carrier Pods mount a
  node hostPath (the registration has to land in the Job home), which the
  baseline and restricted profiles forbid, and the namespace is disposable and
  reachable from the test network only. Narrowing the label to the carrier's
  lifetime would mean re-labelling a shared namespace around every reset and
  restoring labels this component does not own; the test environment accepts
  the loose profile as a fixed property instead. A deployment that cannot
  accept it must run the test environment on a dedicated cluster.
