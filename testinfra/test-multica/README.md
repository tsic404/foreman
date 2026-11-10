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
| `model-backend.yaml` | The keyless ollama backend (PVC, Service, Deployment) — one of the two local model sources, applied by `deploy.sh` only when it is the selected one |
| `scripted-backend.yaml` | The scripted fixture backend (ConfigMap, Service, Deployment) — the default local model source; `deploy.sh` injects `scripted-agent.py` into it |
| `scripted-agent.py` | The scripted backend's server: a chat-completions endpoint that declares the task's window (one warmup call, then `sleep` holds) and never runs anything itself (§ Scripted agent backend) |
| `deploy.sh` | Deployment entry: picks the model source, renders the entry origin, creates the Secret, applies, waits for the rollout |
| `reset.sh` | Reset entry: stops the instance under test, wipes runtime/task rows, rebuilds the test objects, drops a stale pin witness, binds the test agent's model source (endpoint triple when supplied, else the selected local source), runs the environment gate |
| `wire.sh` | Wiring entry: points the instance under test at the access point and starts it |
| `bind-agent.sh` | Binds the test agent to the runtime the instance registers (the step between "the instance is up" and "tasks can be dispatched") |
| `pin-node.sh` | Placement entry: writes the job policy's nodeSelector axis so the test agent's Jobs land on one node (the same-node window cache-02 needs), rolls the instance and verifies the loaded file's hash; when the instance is stopped it records the expected startup line as a witness and exits 3, and `./wire.sh` (or `./pin-node.sh --verify`) checks it after start; `--off` restores the no-axis deployment |
| `recycle.sh` | Recycle entry: deletes the namespace, nothing else |
| `selfcheck.sh` | The environment gate ①②③④⑤ — `reset.sh` and `wire.sh` run it at the pre-start moment |
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
follow the operator's `TEST_MULTICA_MODEL`. The scripted backend is pinned too
(`scripted-backend.yaml`: `python:3.12.9-alpine3.20`) — a floating base would let
a node run a different server than the round was verified against. The Job image is not
part of this pin: `build-foreman-job.yml` resolves upstream's newest release at
build time and tags the image with that daemon version (`latest` follows the
newest release), so the CLI a Job runs can be newer than the `v0.6.1` server.

The Job image carries a **floor**, though, and it is not optional: the daemon
starts `omp` with `--session <path>` up to multica v0.6.1 and with
`--session-dir <dir>` from v0.6.2 on. `omp`'s `--session` is an alias of
`--resume` — strictly a resume — so an older daemon resumes the empty session
file it just created, the agent exits before its first turn, and every task
lands as `agent_error.process_failure` with no tool call. Deploy the image
built for the newest upstream release (`ghcr.io/tsic404/foreman-job:<daemon
version>`, e.g. `0.8.0`); `./selfcheck.sh` gate ⑤ refuses anything below
`0.6.2` with that root cause instead of letting the next round discover it as a
dead task.

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
./reset.sh                     # stop, wipe, rebuild objects, gate ①②③④⑤
./wire.sh                      # MULTICA_SERVER_URL → the access point, start
./bind-agent.sh                # agent ← the omp runtime the instance registered
<TC-tech cases>
```

`reset.sh` and `wire.sh` both run `./selfcheck.sh` — the environment gate
①②③④⑤ — while the instance is stopped: ① it points at the access point (never
the production address), ② the runtime list is empty, ③ the server reports the
artifact anchor, ④ the selected model source serves the round (the supplied
endpoint, or the local backend and the model it serves), ⑤ the Job image's
daemon can start `omp` (see §Version anchor — the gate runs the image's own
`multica version`, because a tag is not evidence). Any failure is `QA_BLOCKED`
(exit 2); once the instance runs, ②'s assertion becomes "exactly one
`provider=omp` runtime", which TC-tech-protocol-01 judges.

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
(§ Model endpoint) wins; without it the selected local source serves the round
(§ Scripted agent backend) — `model` is set to its served selector and the
registration the Job pods read is written through the same installer an
endpoint uses. For the scripted source that is `scripted/<id>` with an empty
`custom_env`; for the ollama source it is `ollama/<id>:<tag>` — the served tag
in full — with `custom_env.OLLAMA_BASE_URL` set to the engine's in-cluster
address. With neither, `model` is cleared and `custom_env` emptied. Both
fields travel in the claim payload's agent section — the Job daemon passes
`model` to the runtime and layers `custom_env` onto the agent process — so a
task can actually reach the model. A selector always names the full served
tag: omp resolves a provided selector against the catalog it reports at
startup, the catalog lists served tags (the scripted server reports the id it
was rendered with; the ollama engine lists `<id>:<tag>`), and a family form
without `:tag` matches no row there, so omp refuses it before the task starts
(`Model "ollama/<family>" not found`). The ollama backend prunes other tags of
the family, so the one served tag is the one the selector names. Reconciling
rather than create-only is what repairs an agent left model-less by an earlier
environment.

The wipe is SQL inside the persistence pod (`kubectl exec deploy/postgres`) —
the server has no API for deleting task history, and a runtime delete detaches
tasks instead of removing them.

## Objects the test env creates

| Object | Name | Used by |
|---|---|---|
| Workspace + loginable account | `foreman-test` (`Foreman Test`), `acceptance@test.local` | task cancellation, board, chat entry |
| Test agent | `foreman-test-agent` | dispatch (its runtime is the one Foreman registers) |
| Failure agent | `foreman-test-agent-failing` | the failed terminal class: same runtime, a model selector no round can resolve, so every task it runs ends failed |
| Scripted backend (the default local source) | ConfigMap `test-multica-scripted-server`, Service/Deployment `test-multica-scripted`, model `fixture:deterministic` (overridable) | deterministic task windows: the fixture declares one warmup call and then `sleep` holds, and the Job's `omp` executes them |
| Ollama backend (the other local source) | PVC `ollama-models`, Deployment/Service `test-multica-ollama`, model `qwen2.5:0.5b` (overridable) | real model output and token usage for a round that selects it |
| Repository | `https://github.com/tsic404/foreman` | agent checkout, cache cases |
| Autopilot (manual trigger) | `foreman-test-autopilot` (short), `foreman-test-autopilot-long` (long) | `./multica-test autopilot trigger <id>` |
| Chat entry | chat session `foreman-test-chat` | messages to the agent |
| Task status query surface | `./multica-test runtime list` / board at the entry origin | runtime, task and cancellation state |
| Model provider registration | `<provider>/<model id>` in `${FOREMAN_STATE_ROOT}/home/<workspace>/.omp/agent/models.yml`; Secret `test-multica-model-provider` | the test agent's model: the endpoint triple when supplied, else the selected local source (scripted by default) |

The repository is the implementation repo, which the acceptance side can write
to with its own git credentials. The autopilot is created by `bind-agent.sh`
rather than by `reset.sh`: the server refuses an autopilot whose assignee agent
has no runtime, and the reset entry leaves the runtime list empty on purpose.

### The failure agent

A task only ends failed if something actually fails it, and a test
environment must not depend on an operator improvising that by hand. The
failure agent is the declared way to build one: it is bound to the same
`provider=omp` runtime as the test agent, and `reset.sh` writes one fixed
`model` onto it on every round — `foreman-failing/absent`, a provider no round
registers — and clears its `custom_env`. A dispatched task therefore reaches
the Job's daemon (the lifecycle face is real: claim, Job, registration, start)
and `omp` refuses the selector before its first turn (`Model … not found`,
exit 1), so the task converges to a failed terminal as the daemon's own
report: `failure_reason` stays empty, and Foreman did not decide it.

**An empty model is not a failure.** The Job template mounts the workspace
home (`${FOREMAN_STATE_ROOT}/home/<workspace_id>`) into every agent of that
workspace, and the round's provider registration lives in that home
(`.omp/agent/models.yml`, §Model endpoint). `omp` with no `--model` resolves a
model out of that file by itself, so an agent whose `model` was cleared runs
the *test agent's* model and completes: the controlled experiment is to delete
the node's `models.yml`, dispatch to the failure agent, and observe the same
task end failed — restoring the file makes it complete again. The failure is
therefore the selector's unresolvability, not the absence of a registration.
`reset.sh` refuses a round whose own provider (`TEST_MULTICA_MODEL_PROVIDER` /
`TEST_MULTICA_SCRIPTED_PROVIDER`) is the fixture's: that registration would
answer the selector too, and the failed class would quietly disappear.

`reset.sh` rewrites the selector every round (the row survives the wipe, and a
leftover value would take the failure with it), `bind-agent.sh` binds the row,
and nothing else configures it. The user-perspective cases consume it through
`tests/acceptance/uat-scenarios.sh`, which also builds the cancelled and
platform-timeout classes and records the window the two terminal-class
carriers judge.

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
| long | `foreman-test-autopilot-long` | the fixture's declared window, then `DONE` |

Both shapes are executed by the scripted backend (§ Scripted agent backend),
which ignores the prompt text: turn 0 is one `multica issue list` call (the
fixture's `SCRIPTED_WARMUP_COMMAND`), turns 1 through `SCRIPTED_HOLD_TURNS` are
`sleep SCRIPTED_SLEEP`, and turn `SCRIPTED_HOLD_TURNS+1` is the final `DONE` —
the warmup never consumes a hold turn, so `HOLD_TURNS=1` still holds for one
full `SCRIPTED_SLEEP`. The task's own API call
is deliberately kept — it is the only observable face of G2's passthrough
counter; without it the counter would never move. The window's length is
therefore `SCRIPTED_SLEEP × SCRIPTED_HOLD_TURNS`, not the prompt's `sleep`: a
round that needs a longer in-flight task (recovery) raises
`TEST_MULTICA_SCRIPTED_SLEEP` / `TEST_MULTICA_SCRIPTED_HOLD_TURNS`. With the
ollama source selected, the short shape completes and the long shape cannot
hold a window (the model does not call tools); the carriers that need one
report their own blocked precondition rather than silently passing. The recovery
case's 900s window is therefore set with
`TEST_MULTICA_SCRIPTED_SLEEP=900 TEST_MULTICA_SCRIPTED_HOLD_TURNS=1`, not with
the prompt text: the prompt no longer decides anything a task does.

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
  above): its selector names a provider no round registers, so the run reaches
  the Job daemon and `omp` refuses before its first turn, and the task converges
  to `failed` with no `failure_reason` (a daemon self-report, not a
  Foreman-decided fault). `tests/acceptance/uat-scenarios.sh` does it by
  assigning an issue to that agent:

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

### Same-node window (cache-02)

`TC-tech-cache-02` judges two **independent** tasks that ran concurrently on
**one** node. Nothing in the fixture builds that by default, and the template
deliberately does not: the preferred-node item is soft and only a continuation
has one (the node its chat session learned), while `topologySpreadConstraints`
spreads independent pods of the agent apart. The placement knob the product
does design for this is the job policy's `nodeSelector` axis
(`02-contracts.md` §8.5, ADR-036) — a hard selector on the agent row, which also
suppresses the preferred-node item.

`./pin-node.sh` writes that policy and verifies the instance loaded it (the
startup log carries the file's sha256; the file is read once at startup, so a
pin that is not loaded is a silent no-op). Run it **after** `./reset.sh`: the
policy key is resolved against the server's agents at startup, and a key the
server does not have refuses the boot. `reset.sh` leaves the instance stopped,
so that pin is not loaded yet: the entry exits `3` and writes the expected
startup line to `$HOME/.foreman-job-policy-witness`, and `./wire.sh` compares it
with the started instance's log (run `./pin-node.sh --verify` to make the same
check on demand). A witness that is not checked is exactly the silent no-op the
entry exists to prevent, so `wire.sh` fails the round rather than starting a
case against an unloaded pin. `reset.sh` drops a witness left by an earlier
round when it rebuilds the test objects, so a stale expectation cannot fail the
next start.

```bash
kubectl get nodes                                  # the node both tasks should share
./pin-node.sh <node>                               # hard nodeSelector on the test agent's policy row
./multica-test autopilot trigger <long-id>         # two independent tasks (two issues, or two triggers)
./multica-test autopilot trigger <long-id>
TASK1=<id> TASK2=<id> tests/acceptance/tech-cache-02-cluster.sh
# the carrier waits for both tasks and captures the failing Job's pod; start it
# right after the second trigger, while the window is still open
./pin-node.sh --off                                # restore the no-axis policy
```

Both tasks carry the same hard selector, so the window is the pinned node by
construction rather than by luck; the carrier still verifies `node_name` from
the projection and blocks when they differ, which would now mean the policy was
not loaded. `./pin-node.sh --off` rolls the instance again and requires the
"policy file missing" startup line — the preferred-node item is suppressed
while the pin is on, so remove it before the rounds that judge that item.

**The second Job is refused, and that is a judged FAIL, not a block.** Pinning
makes the two Jobs *land* on one node, but the second one cannot start: the
node's daemon profile directory is shared by every Job of the workspace
(`<home>/.multica/profiles/foreman-job/daemon.lock`), and upstream's daemon
refuses a second instance for the same profile — `daemon profile "foreman-job"
is already running or starting`, the container exits 1, and the Job converges to
`job_failed` before any daemon registration. ADR-059 is ruled **A** (the profile
becomes the Job's credential volume), so this is a product defect with a known
fix, never an unconstructible window: the window was constructed (both Jobs on
the pinned node, in flight together) and the case's own criterion —
任务全部成功 — failed.

`tech-cache-02-cluster.sh` therefore waits for both tasks to settle and reports
**FAIL** with the failed task's projection, its Job pod's container exit code
and its pod log (the refusal line), plus the server-side run record. It blocks
only when the precondition itself is missing: different nodes, no overlap, or a
task that never settles. Run it right after the second trigger — it polls while
the window is open, which is the only moment the failing pod's log and exit code
are still observable (settlement deletes the Job). A later run still judges from
the projections and reports the pod observations as unavailable.

## Model endpoint (部署参数)

The test agent's tasks run in Foreman's Job pods, and the agent CLI inside a
pod can only finish a task when it has a model endpoint. The endpoint is a
deployment parameter of this environment, never a manual UI step: every round
rebuilds the test-multica namespace, so anything configured by hand would be
gone by the next one. Supplying the triple takes precedence; with no triple the
selected local source serves the round (§ Scripted agent backend).

Three sources exist and exactly one of them is a prerequisite of a round —
never two: the deployment-supplied endpoint, the scripted fixture backend (the
default local source), or the ollama engine:

| | Endpoint triple set | No triple, scripted (default) | No triple, ollama |
|---|---|---|---|
| Objects `./deploy.sh` applies | `deploy.yaml` only | `deploy.yaml` and `scripted-backend.yaml` | `deploy.yaml` and `model-backend.yaml` |
| Backend object | absent — not rendered, not applied, not waited for | ConfigMap/Service/Deployment `test-multica-scripted` | PVC/Service/Deployment `ollama-models` / `test-multica-ollama`; its rollout is the slow one (model pull) |
| Objects `./reset.sh` writes | provider registration (Secret + node-local file), agent `model` = `<provider>/<model id>` | the same registration for `scripted/<id>`, agent `custom_env` empty | no registration (omp's built-in provider); agent `model` = `ollama/<id>:<tag>`, `custom_env.OLLAMA_BASE_URL` = the in-cluster address |
| Gate ④ (`./selfcheck.sh`) | asserts the endpoint is the selected source | asserts the scripted server reports the declared id | asserts the engine serves the model |

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
  `ollama/<id>:<tag>`, `custom_env` = its in-cluster address), otherwise `model`
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

### Scripted agent backend

The window cases need a task that stays in flight while Foreman holds it: a
claim to cancel, a run to read the in-flight gauge from, an agent to carry
across a fault window, two tasks running at once. The fixture states the window
itself — `SCRIPTED_HOLD_TURNS` turns of `sleep SCRIPTED_SLEEP` — because the
daemon's first turn hands the agent a runtime brief rather than the issue body,
so a prompt-stated command is not something the model can rely on reading. A
model produces that process only when it emits the command as a tool call; a
model that answers in prose settles the task in seconds, and every carrier that
requires a live task then blocks on its own precondition.

`scripted-agent.py` is that model: a small chat-completions endpoint served in
the cluster by `scripted-backend.yaml` (Service/Deployment
`test-multica-scripted`; `./deploy.sh` injects the file into the ConfigMap). Its
decision is a function of the request only: the first turn answers with a
`bash` tool call carrying `SCRIPTED_WARMUP_COMMAND` (one Multica API call — the
G2 passthrough counter's only observable face), the turns up to
`SCRIPTED_HOLD_TURNS` carry `sleep SCRIPTED_SLEEP`, and the turn after the hold
is answered with the final text. Nothing is stored, so a retried request
decides the same way, and the prompt text the daemon sends is never parsed.

It is a served endpoint, not a substitute for the job-side half of a carrier:
the Job's real daemon and real `omp` resolve it through the same provider
registration a deployment-supplied endpoint uses, execute the command inside
the Job pod, and the cluster facts — claim, Job, pod, lifecycle projection —
stay what they are. The command runs in the Job pod, this server never runs it,
and the token usage it reports is what the round's usage metric counts.

| `TEST_MULTICA_MODEL_LOCAL_SOURCE` | Backend | Selector |
|---|---|---|
| `scripted` (default) | `test-multica-scripted` | `scripted/<id>` (`fixture:deterministic`) |
| `ollama` | `test-multica-ollama` | `ollama/<id>:<tag>` |

| Variable | Meaning |
|---|---|
| `TEST_MULTICA_MODEL_LOCAL_SOURCE` | Optional local source: `scripted` (default) or `ollama` |
| `TEST_MULTICA_SCRIPTED_DEPLOY` | Optional Deployment/Service name (default `test-multica-scripted`) |
| `TEST_MULTICA_SCRIPTED_MODEL` | Optional model id the server reports (default `fixture:deterministic`); rendered into the manifest and read back from the applied Deployment |
| `TEST_MULTICA_SCRIPTED_SLEEP` | Optional seconds per hold turn (default `60`), rendered into the applied Deployment |
| `TEST_MULTICA_SCRIPTED_HOLD_TURNS` | Optional hold turns per task (default `3`); the task settles on the turn after the last one |
| `TEST_MULTICA_SCRIPTED_WARMUP` | Optional first-turn command (default `multica issue list --output json`) — the task's own API call, which is what moves G2's passthrough counter |
| `TEST_MULTICA_SCRIPTED_PROVIDER` | Optional provider id (default `scripted`) |
| `TEST_MULTICA_SCRIPTED_API` | Optional wire dialect (default `openai-completions`) |
| `TEST_MULTICA_SCRIPTED_WAIT` | Optional rollout budget (default 180s) |

A round that wants genuine model output selects the ollama source; its small
model answers in prose, so the short shape completes and the long shape cannot
hold a window — that is a property of the model, not of the fixture.

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
