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
| `deploy.yaml` | The multica deployment: namespace, backend config, frontend entry config, PVCs, postgres/backend/web/proxy Deployments, Services |
| `model-backend.yaml` | The keyless local model backend (ollama PVC, Service, Deployment) — applied by `deploy.sh` only when no endpoint triple is set |
| `deploy.sh` | Deployment entry: picks the model source, renders the entry origin, creates the Secret, applies, waits for the rollout |
| `reset.sh` | Reset entry: stops the instance under test, wipes runtime/task rows, rebuilds the test objects, binds the test agent's model source (endpoint triple when supplied, else the local backend), runs the environment gate |
| `wire.sh` | Wiring entry: points the instance under test at the access point and starts it |
| `bind-agent.sh` | Binds the test agent to the runtime the instance registers (the step between "the instance is up" and "tasks can be dispatched") |
| `recycle.sh` | Recycle entry: deletes the namespace, nothing else |
| `selfcheck.sh` | The environment gate ①②③④ — `reset.sh` and `wire.sh` run it at the pre-start moment |
| `multica-test` | CLI wrapper — the profile, with the platform's task environment cleared |
| `lib.sh` | Shared settings and helpers for the scripts, including the model-source precedence and the endpoint registration |

## Addresses

| Face | In-cluster | Test host |
|---|---|---|
| Access point (the server itself) | `http://test-multica-server:8080` | `http://127.0.0.1:30080` |
| Frontend entry (web UI + `/ws`) | `http://test-multica-web` | `http://127.0.0.1:30300` |
| Model backend (Job pods only) | `http://test-multica-ollama.multica-test.svc:11434` | — (not exposed) |

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

The model backend exists only in the round that has no endpoint triple: it is
not a face of the multica deployment — no host address, no NodePort, reached
only from the Job pods (`multica-agents`) at its Service name. The test agent
carries that base URL in its `custom_env` (`OLLAMA_BASE_URL`), which travels in
the claim payload and is layered onto the agent process — the Job template is
the instance under test's artifact and is not touched. The engine is keyless on
purpose: a test environment must produce real model output without a production
credential. `TEST_MULTICA_MODEL` overrides the served model (a full `name:tag`);
the value is rendered into `model-backend.yaml` by `./deploy.sh`, and
`./reset.sh` reads it back from the Deployment so the agent's selector always
names what the backend actually serves.

## Version anchor

Absolute, not drifting: the manifests pin every image to the `v0.6.1` release
(`multica@2ea01ae4`) — `deploy.yaml` carries the upstream images
(`ghcr.io/multica-ai/multica-backend:v0.6.1`,
`ghcr.io/multica-ai/multica-web:v0.6.1`, with `pgvector/pgvector:pg17` and
`nginx:1.27-alpine` as the persistence and entry layers), and
`model-backend.yaml` pins the local backend the same way
(`ollama/ollama:0.40.11`); that backend's model tag is the only value that may
follow the operator's `TEST_MULTICA_MODEL`. The Job image is not
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

Without an endpoint triple, the first `./deploy.sh` pulls the model into the
local backend's volume (hundreds of MB) before that Deployment reports ready;
re-deploys reuse it. `TEST_MULTICA_MODEL` selects a different model at deploy
time. The pull step's wait for the engine is bounded: if the engine does not
answer within 120s, the init container fails and prints the engine's own log, so
the pod log says why instead of leaving the pod in Init until the rollout budget
expires. With a triple the local backend is not part of the round at all: it is
not rendered, not applied and not waited for (§ Model endpoint).

**Order matters.** `reset.sh` stops the instance under test and leaves the
runtime list empty on purpose (gate ②), so the test agent cannot be bound yet;
`wire.sh` starts the instance at the access point; `bind-agent.sh` binds the
agent to the `provider=omp` runtime it registers. The full round is:

```
./deploy.sh                    # the test cluster itself
./reset.sh                     # stop, wipe, rebuild objects, gate ①②③④
./wire.sh                      # MULTICA_SERVER_URL → the access point, start
./bind-agent.sh                # agent ← the omp runtime the instance registered
<TC-tech cases>
```

`reset.sh` and `wire.sh` both run `./selfcheck.sh` — the environment gate
①②③④ — while the instance is stopped: ① it points at the access point (never
the production address), ② the runtime list is empty, ③ the server reports the
artifact anchor, ④ the selected model source serves the round (the supplied
endpoint, or the local backend and the model it serves). Any failure is
`QA_BLOCKED` (exit 2); once the instance runs, ②'s assertion becomes "exactly
one `provider=omp` runtime", which TC-tech-protocol-01 judges.

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

The test agent's model configuration is reconciled on every reset, not only on
creation, through one explicit precedence: the deployment-supplied endpoint
(§ Model endpoint) wins; without it the deployed local backend is the source —
`model` is set to `ollama/<family>` (the served tag without its `:tag` suffix)
and `custom_env` carries that backend's in-cluster base URL as
`OLLAMA_BASE_URL`; with neither, `model` is cleared and `custom_env` emptied.
Both fields travel in the claim payload's agent section — the Job daemon passes
`model` to the runtime and layers `custom_env` onto the agent process — so a
task can actually reach the model. The selector is deliberately untagged: a
fresh Job pod has no discovered local-engine catalog at startup, and a
colon-tagged selector would be rejected before discovery; the untagged form
resolves after discovery against the single served tag of that family (the
backend prunes other tags). Reconciling rather than create-only is what
repairs an agent left model-less by an earlier environment.

The wipe is SQL inside the persistence pod (`kubectl exec deploy/postgres`) —
the server has no API for deleting task history, and a runtime delete detaches
tasks instead of removing them.

## Objects the test env creates

| Object | Name | Used by |
|---|---|---|
| Workspace + loginable account | `foreman-test` (`Foreman Test`), `acceptance@test.local` | task cancellation, board, chat entry |
| Test agent | `foreman-test-agent` | dispatch (its runtime is the one Foreman registers) |
| Failure agent | `foreman-test-agent-failing` | the failed terminal class: same runtime, deliberately no model source, so every task it runs ends failed |
| Model backend (local mode only) | PVC `ollama-models`, Deployment/Service `test-multica-ollama`, model `qwen2.5:0.5b` (overridable) | the test agent's inference, token usage — absent when the endpoint triple is set |
| Repository | `https://github.com/tsic404/foreman` | agent checkout, cache cases |
| Autopilot (manual trigger) | `foreman-test-autopilot` (short), `foreman-test-autopilot-long` (long) | `./multica-test autopilot trigger <id>` |
| Chat entry | chat session `foreman-test-chat` | messages to the agent |
| Task status query surface | `./multica-test runtime list` / board at the entry origin | runtime, task and cancellation state |
| Model provider registration | `foreman-test/<model id>` in `${FOREMAN_STATE_ROOT}/home/<workspace>/.omp/agent/models.yml`; Secret `test-multica-model-provider` | the test agent's model (only when the endpoint is supplied) |

The repository is the implementation repo, which the acceptance side can write
to with its own git credentials. The autopilot is created by `bind-agent.sh`
rather than by `reset.sh`: the server refuses an autopilot whose assignee agent
has no runtime, and the reset entry leaves the runtime list empty on purpose.

### The failure agent

A task only ends failed if something actually fails it, and a test
environment must not depend on an operator improvising that by hand. The
failure agent is the declared way to build one: it is bound to the same
`provider=omp` runtime as the test agent, and `reset.sh` clears its `model`
and `custom_env` on every round — no selector, no backend address, whatever
the round's model source is. A dispatched task therefore reaches the Job's
daemon (the lifecycle face is real: claim, Job, registration, start) and the
agent process cannot run, so the task converges to a failed terminal and
Foreman reports it like any other failure.

That is the path the test environment already documents for a model-less
agent (`reset.sh`'s endpoint-less floor converges tasks to `process_failure`
quickly); the failure agent makes it a first-class object instead of a
side effect of an unconfigured round. `reset.sh` recreates it and clears it,
`bind-agent.sh` binds it, and nothing else configures it.

The user-perspective cases consume it through `tests/acceptance/uat-scenarios.sh`,
which also builds the cancelled and platform-timeout classes and records the
window the two terminal-class carriers judge.

The login is deterministic: `APP_ENV=development` plus the fixed verification
code in the Secret. The account is created on first login; no password is
stored anywhere.

## Task shapes (用例前置)

The carriers that read their tasks from the environment (cache, protocol,
recovery) judge a task triggered outside them; none of them creates one.
`bind-agent.sh` therefore provisions two manual autopilots whose `description`
is the run prompt:

| Shape | Autopilot | Prompt |
|---|---|---|
| short | `foreman-test-autopilot` | one `multica issue list` call, then `OK` |
| long | `foreman-test-autopilot-long` | `sleep ${TEST_MULTICA_LONG_TASK_SLEEP:-900}`, then `DONE` |

```bash
./multica-test autopilot list --output json       # the two autopilot ids and titles
./multica-test agent list --output json           # the test agent id (`foreman-test-agent`)
./multica-test autopilot trigger <autopilot-id>   # then: ./multica-test agent tasks <agent-id>
```

Every id below comes from those two listings; `reset.sh` prints the agent's name
only, and only when it creates it.

The short prompt carries the `multica issue list` call for one reason beyond
the reply: the agent's own Multica API call is the only traffic that reaches
Foreman's S29 passthrough, so it is what moves
`foreman_forward_total{endpoint="agent-api"}`. A task that only answers never
produces it, and G2's passthrough leg has no other observable face.

The four terminal classes of the lifecycle case are constructible from these
two shapes and the failure agent, without touching the repository:

- **completed** — trigger the short autopilot.
- **cancelled** — trigger the long autopilot and cancel it while it sleeps:

  ```bash
  ./multica-test autopilot trigger <long-autopilot-id>   # task id from: ./multica-test agent tasks <agent-id>
  ./multica-test issue cancel-task <task-id>
  ```

  Cancellation is server-side only; Foreman never initiates one, it discovers
  it on the daemon's next status poll.
- **timeout** — the Job's budget is the template's `spec.activeDeadlineSeconds`
  (`deploy/25-job-template.yaml`, 86400 shipped), read by Foreman at startup.
  Lower that value in the `foreman-job-template` ConfigMap, roll the
  Deployment, then trigger the long autopilot and let the Job outlive the
  budget: the Job ends with `DeadlineExceeded` and the projection carries
  `failure_reason=job_deadline_exceeded`.
- **failed** — dispatch the task to the failure agent (`foreman-test-agent-failing`,
  above): it carries no model source in every round, so the run reaches the Job
  daemon and then cannot start the agent process, and the task converges to
  `failed` with no `failure_reason` (a daemon self-report, not a Foreman-decided
  fault). `tests/acceptance/uat-scenarios.sh` does it by assigning an issue to
  that agent:

  ```bash
  ./multica-test issue assign <issue-id> --to <failure-agent-id>   # id from: ./multica-test agent list --output json
  ```

  No round mutates the test agent's model for this class any more: the failure
  is the fixture's guarantee, not a race with a reconfigured source.

`tests/acceptance/uat-scenarios.sh prepare` performs the same four
constructions itself — one issue per class, assigned to the test agent, or to
the failure agent for `failed` — and records the window the lifecycle and
observability carriers judge; the recipes above are the manual equivalent.

It is re-runnable in one workspace: each round builds its own issues and its
create waives the CLI's duplicate guard, because the round's issues stay active
after it (nothing in this fixture completes them) and the guard would otherwise
refuse the next round's first issue. The window is replaced atomically and
carries a `STATUS` marker — `partial` while a round is being built, `complete`
only once all four tasks exist — so a second `prepare` builds a fresh set, a
failed one leaves no readable window, and the next run heals it.

The long shape is also the in-flight set of the recovery case: trigger N long
tasks, inject the fault (`kubectl -n foreman rollout restart` or a Job pod
deletion), then judge the tasks that were live across the window.

## Model endpoint (部署参数)

The test agent's tasks run in Foreman's Job pods, and the agent CLI inside a
pod can only finish a task when it has a model endpoint. The endpoint is a
deployment parameter of this environment, never a manual UI step: every round
rebuilds the test-multica namespace, so anything configured by hand would be
gone by the next one. Supplying the triple takes precedence; with no triple the
keyless local backend `model-backend.yaml` declares is the model source (Reset
semantics above).

The two sources are alternatives for the whole round, so exactly one of them is
a prerequisite — never both:

| | Endpoint triple set | Endpoint triple unset |
|---|---|---|
| Objects `./deploy.sh` applies | `deploy.yaml` only | `deploy.yaml` and `model-backend.yaml` |
| Local model backend (PVC/Service/Deployment `ollama-models` / `test-multica-ollama`) | absent — not rendered, not applied, not waited for | present; its rollout is the slow one (model pull) |
| Objects `./reset.sh` writes | provider registration (Secret + node-local file), agent `model` = `<provider>/<model id>` | no registration; agent `model` = `ollama/<family>`, `custom_env.OLLAMA_BASE_URL` = the in-cluster address |
| Gate ④ (`./selfcheck.sh`) | asserts the endpoint is the selected source | asserts the local backend serves the model |

A half-set triple is refused by every entry (`./deploy.sh` included) with the
missing variable named: deploying would otherwise install a model source nobody
chose, and `./reset.sh` would reject the same environment.

`./deploy.sh` applies manifests; it does not prune. A deployment switched from
local mode to the endpoint therefore leaves the earlier backend objects behind —
they are not prerequisites of the new round, but `./recycle.sh && ./deploy.sh` is
the clean rebuild.

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
- **absent** — the entry prints 「未配置模型端点」 and registers nothing, and no
  production endpoint, staging value or credential is ever substituted. The
  rollback is unconditional — every endpoint-less reset removes the node-local
  file and deletes the Secret (`--ignore-not-found`), whether or not the agent
  row still carries the old selector. The agent then falls to the next step of
  the precedence: the local backend when `./deploy.sh` installed one (`model` =
  `ollama/<family>`, `custom_env` = its in-cluster address), otherwise `model`
  is cleared and `custom_env` emptied, so tasks keep the baseline behaviour
  (they converge to `process_failure` quickly). That is what keeps the
  guarantee when `recycle.sh` has deleted the namespace: the node-local file
  outlives it, and the recreated agent row carries no evidence of the previous
  registration.
- **partial** — the entry fails and names the missing part instead of guessing.

The credential travels only through the environment or a mounted file. It is
in no file of this repository, not in `deploy.yaml` or `model-backend.yaml`, and
not in any example manifest; it reaches the carrier Pods through the Secret
volume (never an argument), and rests afterwards only in the node-local file the
Job pods read — the same trust level the design already gives the node home
(ADR-018).

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
  The model backend has no host face at all: only in-cluster clients reach it.
- No production credential ever enters this environment, and the model backend
  is keyless for the same reason: a hosted provider would need a test key in
  the cluster and in the agent's `custom_env`.
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
