# Deploying Foreman

`deploy/` is the whole deployment surface from `02-contracts.md` §3 and §8.
Everything is plain YAML — `kubectl apply -f deploy/` installs it in one pass.

| File | Objects |
|---|---|
| `00-namespaces.yaml` | `Namespace foreman`, `Namespace multica-agents` |
| `10-secrets.yaml` | `Secret foreman-secret` (ns foreman), `Secret registry-tsic` (ns multica-agents + ns foreman) |
| `20-rbac.yaml` | `ServiceAccount foreman` + `Role`/`RoleBinding` (ns multica-agents), `ServiceAccount multica-omp`, `ServiceAccount foreman-gc` |
| `25-job-template.yaml` | `ConfigMap foreman-job-template` (ns foreman) — the required Job template, mounted by the Deployment |
| `26-job-policy.yaml` | `ConfigMap foreman-job-policy` (ns foreman) — the **optional** three-axis job policy, mounted by the Deployment |
| `30-foreman.yaml` | `Deployment foreman` (1 replica) + `Service foreman` (ClusterIP :8080) |
| `35-shared-state.yaml` | `StatefulSet foreman-redis` + `Service foreman-redis` — the shared state store (ADR-046 阶段一); **baseline only** — a deployment swaps in its own persistent Redis (or a managed service) and points `FOREMAN_STATE_URL` at it |
| `40-foreman-gc.yaml` | `DaemonSet foreman-gc` (ns multica-agents) |
| `optional/servicemonitor.yaml` | `ServiceMonitor foreman` — needs the Prometheus Operator CRDs |

```bash
kubectl apply -f deploy/                             # namespaces → secrets → rbac → workload
kubectl apply -f deploy/optional/servicemonitor.yaml # only with the Operator CRDs
kubectl -n foreman rollout status deploy/foreman
```

The Service is reachable inside the cluster as
`foreman.foreman.svc.cluster.local:8080` — that hostname is baked into every
Job by the job builder, so the Service name must stay `foreman` in namespace
`foreman`.

## Secrets

`10-secrets.yaml` carries **placeholders**; `kubectl apply -f deploy/` will
overwrite live values with them, so pick one of:

- edit the file before applying (replace every `REPLACE_ME`), or
- create the Secrets out of band and remove `10-secrets.yaml` from the apply set.

The shipped `FOREMAN_JOB_TOKEN_KEY` placeholder is deliberately not valid hex:
an un-replaced copy makes Foreman fail its startup self-check and CrashLoop,
rather than serve with a key anyone can read from the repository.

```bash
# Server credential: mdt_/mul_ token bound to FOREMAN_DAEMON_ID + workspace.
kubectl -n foreman create secret generic foreman-secret \
  --from-literal=MULTICA_TOKEN='mdt_...' \
  --from-literal=FOREMAN_JOB_TOKEN_KEY="$(head -c 32 /dev/urandom | xxd -p -c 64)" \
  --from-literal=FOREMAN_STATE_PASSWORD='<redis password, or empty for none>' \
  --dry-run=client -o yaml | kubectl apply -f -

# GHCR pull secret (ns multica-agents and ns foreman): a GitHub token with
# read:packages, because the ghcr.io/tsic404/foreman* packages are private.
kubectl -n multica-agents create secret docker-registry registry-tsic \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token> \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n foreman create secret docker-registry registry-tsic \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token> \
  --dry-run=client -o yaml | kubectl apply -f -
```

`FOREMAN_JOB_TOKEN_KEY` must decode to at least 32 bytes of hex (64 hex
characters); `MULTICA_TOKEN` must start with `mdt_` or `mul_`.

## Multi-workspace (optional)

One deployment can serve several workspaces (ADR-047): each workspace has its
own `(credential, daemon identity)` pair, and every control-plane action runs
once per pair.

- **Credentials**: either keep the single-workspace `MULTICA_WORKSPACE_ID` /
  `FOREMAN_DAEMON_ID` / `MULTICA_TOKEN` form (a legal one-workspace set), or set
  `FOREMAN_WORKSPACES_FILE` to a mounted file holding:
  ```json
  [
    {"workspace_id": "ws-a", "daemon_id": "foreman-a", "token": "mdt_..."},
    {"workspace_id": "ws-b", "daemon_id": "foreman-b", "token_env": "MULTICA_TOKEN_B"}
  ]
  ```
  `token_env` names the Secret key's env variable; `token` may be inlined. Both
  ids are unique, and no daemon identity may be shared across workspaces (F2).
  `FOREMAN_WORKSPACES` accepts the same JSON inline (mutually exclusive with the
  file form).
- **Serving scope**: the startup probe is still one Job; the template's daemon
  decides which workspaces it registers to (S-1 answers the configured set).
  Foreman serves exactly the workspaces it observed registering — a configured
  workspace the probe never registered is not served and logs
  `workspace.probe_registration_absent`.
- **Budget**: `FOREMAN_MAX_INFLIGHT_JOBS` is the global cap;
  `FOREMAN_MAX_INFLIGHT_JOBS_PER_WORKSPACE` adds the per-workspace cap (0 = none).
- **Policy**: partition the table under `workspaces.<workspace_id>`:
  ```yaml
  workspaces:
    ws-a: {agents: {...}, projects: {...}}
    ws-b: {agents: {...}, projects: {...}}
  ```
  The flat `agents` / `projects` form is the single-workspace form and refuses
  startup when more than one workspace is configured.
- **Node state**: every workspace-partitioned location in the template must be
  expressed through `<workspace_id>` (the shipped template does this for the
  home volume). A fixed literal makes the template correct for one workspace
  only — a deployment error Foreman does not correct.
- **Isolation**: cross-workspace Job Tokens, projections, delivery and probes are
  rejected (403 / 404); a credential failure retries only its workspace.

## Images

Two images come from this repo: the scheduler image for the Deployment, and the
Job image (upstream multica CLI + `foreman-gc` + `dumb-init`) that both the Job
pods and the `foreman-gc` DaemonSet run. Neither image carries an agent
backend: the Job template's init step installs `omp` into every Job at start-up
(ADR-033), which is why the shipped template mounts it at the
`MULTICA_OMP_PATH` the daemon reads. Both are published as **multi-platform
manifest lists** (`linux/amd64` + `linux/arm64`), so a cluster with nodes of
both architectures pulls the platform each node runs; nothing in `deploy/`
names an architecture.

Each image has its own release line and its own version semantics:

| Workflow | Image | Tags | Triggers |
|---|---|---|---|
| `.github/workflows/build-foreman.yml` | `ghcr.io/tsic404/foreman` | `v<X.Y.Z>` (tag push), `latest` (everything else) | `main` push, `v*.*.*` tag, manual dispatch |
| `.github/workflows/build-foreman-job.yml` | `ghcr.io/tsic404/foreman-job` | `<multica daemon version>`, `latest` | `main` push, hourly schedule, manual dispatch |

`foreman`'s `<version>` is the trigger's: a `v*.*.*` tag push publishes exactly
`foreman:v<X.Y.Z>`, any other run (a `main` push, a manual dispatch as its re-run
entry point) exactly `foreman:latest` — so a published version tag always names
the image built from that tag and stays a valid rollback target (ADR-012), and
no main push mints a version. `foreman-job`'s `<version>` is the upstream daemon
version the image carries, resolved at build time from upstream's newest release
(never from a repository variable) and verified against that release's checksums.

**`latest` never moves backwards**, in either line: it is pushed only by a run
that publishes newer content — for `foreman` a `main` push (a `v*.*.*` tag push
publishes its version tag and leaves `latest` alone, however old that tag is),
for `foreman-job` a run publishing exactly upstream's newest release. Both images
are referenced by movable tag with `imagePullPolicy: Always` (`FOREMAN_JOB_IMAGE`
defaults to `foreman-job:latest`), so a build of an older version would otherwise
roll the whole cluster back. The two lines are independent: a `foreman` tag push
never changes the Job image's version, and a Job-image run never builds the
scheduler image.

A Job-image run whose version is already published **skips** the build and the
push, so the schedule and `main` pushes normally cost one registry query. That
also means the automatic path never re-publishes an existing version tag: when a
`foreman-gc` change has to ship under an unchanged daemon version, run the
workflow manually with `force` — the version tag is republished, `latest` stays
where it is (this version is not upstream's newest release):

```bash
gh workflow run build-foreman-job.yml -f version=0.6.1 -f force=true
```

Both workflows need no secrets for the push (`GITHUB_TOKEN`, `packages: write`)
and resolve the upstream CLI artifact from its release checksum manifest, per
platform. Whichever image ends up as `FOREMAN_JOB_IMAGE` must carry:

- `/usr/bin/dumb-init` — every Job `command` starts with `dumb-init --` (it is
  the agent container's PID 1, ADR-022), and the build installs Alpine's
  `dumb-init` package for the default base. A base whose package sources cannot
  supply it, or a custom image named by a job template, leaves the container
  without it and the agent fails at exec (pod `Failed` → `job_failed`);
- the runtime the template's `omp` needs — the musl `omp` build links
  `libstdc++`/`libgcc_s`, so the image installs Alpine's `libstdc++` package
  (default base); a slimmer base that ships neither fails at exec;
- the busybox applets the reference template's init step uses (`wget`, `uname`,
  `sha256sum`) plus `sh`/`cp`/`chmod`/`chown` for `prepare` — the stock Alpine
  base has all of them.

`ADR-033` requires those contents, and §8.7 carries them as deployer
constraints. The release workflows only build and push: checking what a
published image contains is the design's acceptance face (`03-acceptance.md`
TC-tech-job-08 for the contents, TC-tech-provenance-01 for the CLI provenance),
run by QA against the image — not a post-push step in CI.

The Job image is referenced by a **movable tag** in two places that carry the
same literal (ADR-012):

- `FOREMAN_JOB_IMAGE` in `30-foreman.yaml` (written verbatim into every Job
  container's `image` — no digest concatenation), and
- the `image:` of the `foreman-gc` DaemonSet in `40-foreman-gc.yaml`
  (the DaemonSet runs the same image as the Job pods).

Rolling forward is one step: point `FOREMAN_JOB_IMAGE` at the new reference, or
let the tag itself move. Rolling back uses the version tags CI keeps publishing
— the scheduler's `v<ver>` and the Job image's `<multica version>`; `latest`
alone is never the only published tag. Every container in the Job template
defaults to `imagePullPolicy: Always`, which is what lets a moved tag reach a
node that already cached the reference; a deployment that pins a version tag can
set `imagePullPolicy: IfNotPresent` in the job template (§Job template below)
and accepts that a moved tag then skips cached nodes. `foreman-gc` pulls
`Always` for the same reason.

CI publishes each run's references (the `release-<image>-<version>` artifact and
the workflow summary) as the run's own record — nothing in `deploy/` is
backfilled from it.

Local builds are for development and staging clusters, never the release path:
`make build-job-image` builds one platform's image (`linux/$(ARCH)`, the host
architecture unless `ARCH=` says otherwise) and takes the upstream CLI archive
URL + SHA-256, so the built image can be checked against the upstream release
(AC-13 provenance):

```bash
make build-job-image \
  MULTICA_CLI_URL=https://github.com/multica-ai/multica/releases/download/v0.6.1/multica-cli-0.6.1-linux-amd64.tar.gz \
  MULTICA_CLI_SHA256=<sha256 from that release's checksums.txt>
```

Verification is not part of the release path: the workflows resolve the version,
build the multi-platform image and push it, and that is all. What the published
image contains is checked by the design's acceptance cases against the image
(TC-tech-job-08 / TC-tech-provenance-01), which is where the contents and
provenance assertions live. The release record reflects the push command's exit
code only — a non-zero exit does not prove that nothing landed, so check the
registry before re-running.

## Shared state store (stage one)

Foreman persists the **Job Token entries, the settled terminal records and
the token revocation set** in a shared state store (Redis), so a restart
restores them instead of rebuilding from K8s (ADR-046 阶段一,
`02-contracts.md` §4.3/§6.1, AC-73):

- `FOREMAN_STATE_URL` — the store's connection string. Two forms are
  accepted (§7 部署面):
  - **single endpoint** — `redis://host:port`, or `rediss://` for TLS;
  - **sentinel** — the same URL with `master_name=<name>` and one
    `addr=<sentinel:26379>` per additional sentinel, e.g.
    `redis://sentinel-a:26379?master_name=foreman&addr=sentinel-b:26379`.
    This form is served by a failover client: it resolves `<name>` through
    the sentinels and follows a failover to the new master.

  A deployment points this at its own Redis and replaces
  `35-shared-state.yaml`; the shipped manifest is a **single-pod
  baseline**. A topology that exposes neither form (e.g. a cluster that
  requires the cluster protocol) is fronted by one stable endpoint — a
  managed service endpoint or a proxy — because the connection face is a
  single URL.
- `FOREMAN_STATE_PASSWORD` — the store credential (from `foreman-secret`;
  the shipped `REPLACE_ME` placeholder must be replaced). An empty value is
  allowed only for a store that is otherwise protected. Foreman treats an
  empty value **and the literal `REPLACE_ME`** as unauthenticated (the latter
  is a usable string published in the repository, so leaving it unreplaced
  must alarm rather than serve), logs `state.unauthenticated`, because the
  store carries token digests, mapping rows and delivery bodies (§10 安全).
  A data-node credential embedded in `FOREMAN_STATE_URL` counts as
  authenticated, exactly as it is applied: the URL's userinfo for a single
  endpoint, the `password` query parameter for a sentinel URL (a sentinel
  URL's userinfo authenticates to the sentinels, not to the store).
- `FOREMAN_STATE_ENTRY_TTL` (default `48h`) — one entry's retention in the
  store. It must cover the Job Token TTL (`24h7m`: claim → boot → task →
  drain) plus the settled-record window (24h), which is why the default is
  48h.

Requirements (§7 部署面):

- **Persistence is mandatory.** Run the store with persistence on (AOF/RDB)
  and a failover-capable topology (the sentinel form above, or a managed
  service) when availability matters; the contract pins the *connection
  face*, not the topology — a single instance is the baseline.
- The store holds token digests, the mapping rows (including each row's
  delivery body and terminal verdict body, §4.3), and their terminal records:
  treat it as a **credential store** (isolation, authentication, transport
  protection). Those bodies never reach a K8s object, a log or `/foreman/tasks`
  (F3); the shared state face is where the design puts them, and they are
  deleted with the row at the credential reclaim.
- Requirements on the store's credential and reachability are fail-fast: an
  unparsable `FOREMAN_STATE_URL` refuses startup. A *reachable-but-empty*
  store is not an error — the K8s rebuild then owns recovery (§4.3's disposal
  table), which is also the behaviour when the store is lost.
- If `FOREMAN_STATE_URL` is unset, Foreman falls back to the pre-stage-one
  behaviour (in-memory index + full K8s rebuild on restart); AC-73's restart
  persistence then does not hold. Set the URL in every deployment that
  expects tokens and mapping to survive a restart.

## Node-local state

Job pods and `foreman-gc` share the node directory `${FOREMAN_STATE_ROOT}`
(default `/var/lib/foreman`):

- Job pods mount `${FOREMAN_STATE_ROOT}/{home,workspaces}`,
- `foreman-gc` mounts `${FOREMAN_STATE_ROOT}` at `/state`

The DaemonSet's `hostPath.path` and the Deployment's `FOREMAN_STATE_ROOT` must
be changed together; the collector's own view is the mount point, passed back
in as `FOREMAN_STATE_ROOT=/state`.

## Configuration

All keys with their contract defaults are spelled out in the Deployment env.
The ones a staging/test cluster usually overrides:

| Key | Staging value | Why |
|---|---|---|
| `MULTICA_SERVER_URL` | `http://stub:9999` | protocol stub instead of the real server |
| `FOREMAN_MAX_INFLIGHT_JOBS` | `2` | concurrency assertions |
| `FOREMAN_JOB_BOOT_TIMEOUT` | `60s` | boot-timeout assertions |
| `FOREMAN_LOG_LEVEL` | `debug` | heartbeat / WS assertions |
| `FOREMAN_POLICY_NAME_RESOLUTION_TIMEOUT` | `2s` | bounds the startup resolution of the policy file's keys (default `5s`); a window that runs out reads as `unavailable` |
| `FOREMAN_PROJECT_LOOKUP_TIMEOUT` | `1s` | bounds one read-only project lookup by issue id (default `2s`), keeping the claim → Job budget |

## Job template (required)

The Job's shape comes from one place: the ConfigMap `foreman-job-template`,
which `25-job-template.yaml` ships and the Deployment mounts at
`/etc/foreman/job-template/` as a **non-optional** read-only volume (key
`job-overlay.yaml`). The shipped content is the **reference template** — a byte
copy of the in-repo baseline `internal/jobbuilder/testdata/job-baseline.yaml`
Job document, the AC-20 authority — and a deployer replaces it with their own
complete `batch/v1 Job` (contract §8.1):

```bash
# The shipped reference template (this repo's deploy/25-job-template.yaml) is
# applied with everything else; to install a different one:
kubectl -n foreman create configmap foreman-job-template --from-file=job-overlay.yaml=./job-overlay.yaml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n foreman rollout restart deploy/foreman
```

**There is no built-in template.** A missing, unreadable or unparseable file
refuses startup, and the ConfigMap object itself missing blocks the pod before
Foreman's process ever runs (`CreateContainerConfigError`): deploy the ConfigMap
**before** upgrading Foreman (ADR-030).

Foreman writes exactly four things into that template:

1. the env keys `MULTICA_SERVER_URL`, `MULTICA_DAEMON_ID` and
   `MULTICA_DAEMON_MAX_CONCURRENT_TASKS` on `containers[0]` — the first entry in
   writing order, not a container matched by name; a same-name entry is
   replaced whole, so those three keys are Foreman's whatever the template
   says. `MULTICA_SERVER_URL` is `FOREMAN_JOB_SERVER_URL` (the Job-side entry
   point below, defaulting to the reference Service). `MULTICA_DAEMON_ID` reads
   the pod-template label `foreman.tsic.top/daemon-id`;
2. the name and the per-task metadata: `metadata.name` is the template's name
   **format** rendered per task (empty ⇒ `fm-<task_id>`), `metadata.namespace`
   is `FOREMAN_JOB_NAMESPACE`, and the A1.2 label/annotation set is written
   after clearing the `foreman.tsic.top/` domain (the four trigger-context keys
   are added or deleted to match the task). The **identity** name
   `fm-<task_id>` — the `daemon-id` label and annotation, the credential Secret
   `fm-<task_id>-cred` and the Job Token's `j` claim — never follows that
   display name;
3. the placeholders below;
4. the three-axis **policy** of `26-job-policy.yaml` when an axis hits:
   `containers[0].resources` (replaced as one block), the capability `add` list
   of `containers[0]` (appended to, `drop` kept), and the pod template's
   `nodeSelector` (merged key by key). Misses change nothing.

Everything else — containers, init containers, `command`/`args`, volumes,
mounts, `resources`, `securityContext`, `affinity`, `image`,
`serviceAccountName`, the rest of `env` — is used verbatim. Nothing is checked,
added or normalized.

| Placeholder | Value |
|---|---|
| `<task_id>` / `<agent_id>` / `<workspace_id>` | the task's ids |
| `<issue_id>` / `<issue-identifier>` / `<autopilot_run_id>` / `<chat_session_id>` | the task's trigger context |
| `<job runtime uuid>` / `<RFC3339>` / `<attempt>` | the Job runtime id / the claim time / the task's attempt number |
| `<job-image>` | `FOREMAN_JOB_IMAGE` |
| `<job-name>` / `<daemon-id>` | the final display name / the identity name `fm-<task_id>` |
| `<agent-name>` | the payload's raw `agent.name` (any characters; only the name face normalizes it) |
| `<preferred node>` | the node holding the resumed session's file (`session_id` of the task's previous run); with no resumed session — a first run — or no known node for it, or `FOREMAN_PREFER_NODE_REUSE=false`, or a `nodeSelector` policy hit, the whole affinity item carrying it is dropped. This literal is the substitution set's **one exception**: it is consumed only where it is *recognized* — a `values` entry of a preferred-affinity `matchExpressions` reading exactly this literal — so anywhere else it stays verbatim, and the apiserver judges that position at Job creation |
| `<issue-segment>` / `<agent-segment>` / `<task-key>` | **only inside `metadata.name`**: normalized (lowercase, non-`[a-z0-9]` runs folded to `-`, trimmed, truncated to 20 / 24 / 12) issue and agent segments, and the task id's dashes-stripped last 12 characters. Anywhere else they stay verbatim |

Unregistered `<...>` literals stay as written, so a template that writes
`image: <job-image>` reaches the apiserver unchanged and the Job never boots;
the reference template has the placeholder in every container it declares
(`agent`, `omp`, `prepare`).

**The template also provides the agent backend.** The Job image carries no
`omp` (ADR-033), so the shipped reference template's `omp` init container
fetches the architecture's musl build from upstream's newest release, verifies
it against that release's `SHA256SUMS.txt`, and leaves it where the agent
container mounts it as the `MULTICA_OMP_PATH` file. A deployer's own template
does the same job its own way — that is the template's choice, and nothing
checks it. Consequences to size for:

- every Job boot pays the download (the reference template takes the newest
  release each time), so `FOREMAN_JOB_BOOT_TIMEOUT` (default `300s`, §4.2) must
  cover a multi-minute fetch on a cold or slow link;
- the Job image must keep the runtime that binary links (`libstdc++` /
  `libgcc_s`); `ADR-033` lists it as an image content;
- a `MULTICA_OMP_PATH` that exists in neither the image nor a mounted volume
  leaves the daemon with no runtime to discover, which the startup probe reports
  as a failed profile.

- The file is read once at startup: an edit needs a Foreman restart, and the
  startup log records the template sha256 plus the accepted/refused verdict.
- Four things refuse startup (CrashLoopBackOff), each logged as its own class:
  a file that does not exist (the ConfigMap is there but has no
  `job-overlay.yaml` key), a file that cannot be read, a file that is not a
  `batch/v1 Job` — bad YAML, an unknown field (`contianers` is refused, never
  silently dropped), or a missing/mismatched `apiVersion`/`kind` — and a
  `metadata.name` that is not a legal name format. Nothing else is checked.
  There is no env switch and no fallback.

### Name format (`metadata.name`)

`metadata.name` is a format string, not a static name. Its character-level
closure is the four variables above plus lowercase letters, digits and `-`; it
must carry `<task-key>` or `<task_id>` (the unique task segment), render at most
63 characters at the worst case, and stay DNS-1123 at its minimum values. An
empty or absent `metadata.name` means the fallback `fm-<task_id>`. A non-empty
format outside the closure **refuses startup** rather than falling back — a
silent rewrite would let a deployer believe the custom format took effect.

`fm-<issue-segment>-<agent-segment>-<task-key>` is the readable shape the
reference template's comments suggest; the shipped reference template itself
keeps `fm-<task_id>`, which renders exactly the names of previous releases.

The name is **not a locator**: Foreman finds Jobs by the
`foreman.tsic.top/task-id` label (mapping rebuild, deletion, Secret sweeps, pod
ownership), never by name, so a custom format changes nothing but what
`kubectl get jobs` shows.

### Upgrade and rollback order

The template is a required deployment artifact, so the ConfigMap moves first:

- **Upgrade**: create/update the ConfigMap (the shipped reference template, or
  the deployer's own) **before** rolling out the new Foreman. A Foreman whose
  template is absent refuses to start — and with the ConfigMap object itself
  missing, kubelet blocks the pod before that.
- **Rollback**: roll the image back; the ConfigMap can stay. An older Foreman
  reads the same complete template, so no ConfigMap change is needed.

### Risk disclosure

Nothing in the template is validated, so a template's mistakes surface at
runtime instead of at startup:

- Job 内负载来自模型输出，等同于在节点上执行不可信代码
- Foreman 不对 Job 模板做任何校验
- 模板内容不构成安全边界
- Foreman 按策略文件向 Job 写入资源与能力字段，该提权面由 Foreman 自身写入
- 策略文件内容不构成安全边界

A template can therefore grant `privileged`, node paths, the node's container
runtime socket, `serviceAccountName` or `automountServiceAccountToken`: that is
the deployment's own decision and its own risk. Three consequences worth knowing
before copying the reference template:

- The Job Token reaches the daemon only if the template mounts the per-task
  Secret `fm-<task_id>-cred` (or another source of `config.json` at the profile
  path); without one the task fails with `job_auth_failed` — nothing checks
  this at startup.
- A template carrying its own `image` removes the image audit anchor:
  `/foreman/version` keeps reporting `FOREMAN_JOB_IMAGE`, which is then no
  longer what the Job runs.
- `MULTICA_DAEMON_ID` reads the pod-template label `foreman.tsic.top/daemon-id`,
  which Foreman writes with the identity name `fm-<task_id>`. A template that
  sets `manualSelector: true` (or otherwise replaces the Job's selector) makes
  the apiserver reject the Job at creation (`job_create_failed`) rather than
  producing an id-less daemon — the identity no longer depends on the Job
  controller's `job-name` label.

### Catalog probe and model catalog (startup)

Before Foreman registers with Multica Server it runs **one catalog probe Job**:
the same official daemon, rendered from the same template. The probe reports which
runtimes/providers the template environment really has, and that report is what
Foreman declares to the server; it also doubles as the template preflight.

- `FOREMAN_STARTUP_PROBE_TIMEOUT` (default = `FOREMAN_JOB_BOOT_TIMEOUT`) is the
  window. **No runtime registers inside it ⇒ Foreman exits non-zero**
  (`startup.probe_failed` carries the class: `job_create_failed` /
  `pod_not_started` / `daemon_not_registered` / `discovery_not_landed`). A
  failure to *land the catalog* is an **error-level** `startup.probe_failed`
  with `reason=discovery_not_landed`, but it does **not** stop startup:
  registration (S4) is the only exit criterion. With
  `FOREMAN_MODEL_CATALOG_ENABLED=false` registration alone completes startup and
  no failure event is emitted.
- **Template duty**: the probe needs its own credential and profile, so a
  template must express them with the two literals Foreman rewrites —
  profile `foreman-job` → `foreman-catalog`, Secret `fm-<task_id>-cred` →
  `fm-catalog-<request_id>-cred`. The rewrite reaches the profile where the
  template expresses it (each container's command line and the mount path the
  profile directory sits at) and the Secret where the token reaches that
  directory (the Secret references a pod volume carries). Every other position
  keeps the template's own text — an image reference reading `foreman-job`, the
  pull credential beside it, any env value or label. A template that does not
  express the two faces means the probe cannot authenticate, so startup exits
  non-zero (visible, not silent).
- **Credential duty**: the probe's Job Token sits in the template-shaped pod and
  the template's own processes can read it — do not add a process to the probe
  that must not hold it (same rule as the task Jobs).
- The advertised model catalog is discovered **at startup, on the single node
  the probe landed on, and replayed from cache** for later server commands
  (`FOREMAN_MODEL_CATALOG_CACHE_TTL`, default `6h`, stale caches still answer
  and only raise a warning). Keep the node pool homogeneous and keep the
  provider configuration under `${FOREMAN_STATE_ROOT}/home`; otherwise the
  directory you advertise describes a different environment than the task Jobs
  run in.
- Operations view: `curl -s localhost:8080/foreman/register-source` prints the
  discovered list, the declared runtimes, each runtime's catalog slot state and
  the current runtime-profile replay source (read-only — it never triggers a
  discovery).

### Runtime profiles and workspace state (ADR-043 / ADR-044)

Both are server-owned workspace state; Foreman carries the values and never
interprets them.

- **Runtime profiles**: Foreman fetches the workspace's profile list once
  **before** the startup probe (S-3 replays it byte for byte; a 404 from an old
  server stays 404, an unsuccessful fetch answers 503, which the daemon treats
  as best-effort). The probe's registration body is what Foreman declares, so a
  profile whose `command_name` the template image cannot resolve is never
  declared and never appears as a server-side runtime. The template/image must
  therefore provide every enabled profile's command. Enabling or disabling a
  profile at runtime is picked up from the server's
  `daemon:runtime_profiles_changed` event: Foreman re-fetches the list, makes
  the new list the S-3 source, runs one short registration-discovery probe, and
  only replaces the declaration after C-1 succeeds; entries that left the
  declaration are deregistered one by one (the server keeps the row offline).
  A failed refresh keeps the old declaration and the old S-3 source.
- **WS degradation**: the refresh trigger is the WS subscription. With
  `FOREMAN_WS_ENABLED=false`, or while the subscription is down, runtime
  profile changes do **not** take effect until the next Foreman restart. This
  is a known degradation, not a silent one: the restart re-fetches the list.
- **Workspace state**: C-1 seeds `settings` / `repos` / `repos_version` and
  every S-2 (repo checkout) triggers one C-22 refresh. Values are verbatim: the
  settings key set is the server's, a `{}` or explicit `null` is replayed as
  such, and a key the server did not send stays absent. A failed refresh
  replays the last successful snapshot (`workspace.state_refresh_failed`,
  metric `foreman_workspace_state_refreshes_total{result="stale_replay"}`) and
  never blocks the checkout. Settings and repository changes therefore apply to
  the next task, not to a running one.
- `FOREMAN_WORKSPACE_STATE_TIMEOUT` (default `5s`) bounds one C-22 refresh;
  concurrent S-2 requests share one in-flight call.
- Metrics: `foreman_runtime_profile_refreshes_total{result}` (`success` /
  `unchanged` / `failed`) and `foreman_workspace_state_refreshes_total{result}`
  (`success` / `stale_replay` / `failed`). Events: `runtime.profiles_refresh`,
  `runtime.profiles_refresh_failed`, `workspace.state_refresh_failed`; none
  carries a task field.
  - Profile refresh: one outcome per attempt, from every trigger (`change-event`
    and `runtime-not-found`). `success` = a changed list was committed by a new
    C-1; `unchanged` = the attempt completed without moving the declaration —
    an unchanged content projection, or a C-18 404 (the event's `c18` field
    separates them); `failed` = a step failed (C-18 error/5xx, probe failure,
    C-1 failure).
  - Workspace state refresh: `success` = the carrier was replaced;
    `stale_replay` = the attempt failed and the answer came from the last
    successful snapshot; `failed` = the request arrived with no seeded snapshot
    to answer from — §1.7's register-before window records one per request,
    since C-22 is never attempted before C-1 has seeded a snapshot. The S-2
    answer itself stays the designed empty pre-C1 body.

### Skill-bundle resolve cache (ADR-045)

The Job daemon resolves skill references against
`POST /api/daemon/runtimes/{rid}/tasks/{tid}/skill-bundles/resolve`. Foreman now
answers that call from a **process-local** cache before it ever reaches the
server: the key is the request payload bytes alone (the task and runtime ids
never enter it), so the same reference set resolved by another task is a hit.

- `FOREMAN_SKILL_CACHE_FRESH_TTL` (default `30m`) — a hit inside this window
  answers locally and issues no upstream request. Past it the entry is refreshed.
- `FOREMAN_SKILL_CACHE_DOWNGRADE_WINDOW` (default `6h`, `0` = off) — when the
  server is unreachable, a timeout, or answers 5xx, an entry still inside this
  age may answer instead. A non-zero value must be ≥ the fresh window (a
  shorter one could never apply, since a fresher entry answers locally). That
  answer is **error-level logged**, carries the `degraded` count, and is the
  only case where a cached body replaces a failed upstream; outside the window
  (or with no entry) the failure returns unchanged.
- `FOREMAN_SKILL_CACHE_RETENTION` (default `6h`, must be ≥ the fresh window) —
  an entry unused for this long is reclaimed.
- `FOREMAN_SKILL_CACHE_MAX_ENTRIES` / `FOREMAN_SKILL_CACHE_MAX_BYTES`
  (defaults `1024` / `256Mi`) — the excess is reclaimed least-recently-used
  first; a single response larger than the byte bound is not stored.
- The cache is **per process**: a restart starts empty and every resolve is a
  plain forward, which is also what a missing or damaged entry degrades to. It
  is never shared across replicas or nodes (single replica, NG7/NG11).
- Metrics: `foreman_skill_cache_resolves_total{result}` (`hit` / `miss` /
  `stale_hit` / `degraded` / `upstream_failure`), `foreman_skill_cache_entries`,
  `foreman_skill_cache_bytes`, `foreman_skill_cache_evictions_total{reason}`
  (`capacity` / `retention` / `damaged`). Events: `skill.cache_hit`,
  `skill.cache_stale`, `skill.cache_degraded` (error level),
  `skill.cache_upstream_failure`, `skill.cache_evicted`, `skill.cache_entry_damaged`.
  None of them carries the payload, a payload field name or a skill name.

## Job policy (optional)

`26-job-policy.yaml` ships a comment-only `policy.yaml`; the Deployment mounts
the ConfigMap at `/etc/foreman/policy/` as an **optional** read-only volume.
The file is a legal default when absent: all three axes read `None`, nothing is
injected, and the startup log carries a warn plus the fixed sentence
`策略文件在启动期一次性加载，运行中变更不生效`.

```yaml
agents:                      # keys are agent names (agent.name), exact match
  omp-default:
    resourcePolicy: projectWithAgent   # None | project | agent | projectWithAgent
    capPolicy: agent
    nodeSelectorPolicy: agent
    resources:               # this agent's definition bodies
      requests: { cpu: "1", memory: 2Gi }
      limits:   { cpu: "4", memory: 8Gi }
    capabilities:
      add: ["SYS_ADMIN"]     # appended to the container's own add list
    nodeSelector: { kubernetes.io/os: linux }
projects:                    # keys are project names, resolved at startup to
                             # platform project identifiers; definitions only
                             # (the runtime match is by identifier, not name)
  foreman:
    resources:
      requests: { cpu: "500m" }
    capabilities: { add: ["NET_ADMIN"] }
    nodeSelector: { disktype: ssd }
```

The three axes are independent, each reads its own policy field, and a miss
changes nothing:

| Axis | Hit ⇒ written to | Merge |
|---|---|---|
| resource | `containers[0].resources` | one block replaced; `projectWithAgent` merges the two definitions key by key, the agent winning a shared key |
| capability | `containers[0].securityContext.capabilities.add` | appended to the template's list, deduplicated; `drop` and every other `securityContext` field stay the template's |
| nodeSelector | the pod template's `nodeSelector` | merged key by key, the policy winning a shared key; a hit also drops the `<preferred node>` item (a hard constraint cannot be checked against the session's node) |

`initContainers` are never touched, and the catalog probe never reads the file.
`GET /foreman/tasks/{taskID}` plus the `policy.applied` log line carry the
per-task attribution (`resource_policy` / `cap_policy` / `node_selector_policy`,
the `*_source` outcome, `project_source`, `name_source`, `overridden_keys`,
`template_written_faces`, `soft_affinity_suppressed`).

Startup expects a present policy file to be well formed:

- **Refuses startup** (CrashLoopBackOff, logged by class): an unreadable file, a
  file that does not parse under strict decoding (an unknown key is refused), a
  definition that writes `capabilities.drop` (`Foreman` is the one writing
  capabilities, so an ignored `drop` would be a security intent that never took
  effect), a key the platform reports absent or ambiguous, and two project keys
  resolving to the same platform identifier.
- **Warns and continues**: a definition no axis references (dead config), a key
  the platform cannot resolve (unreachable / timeout / no such query — the
  agent section still matches by exact name, the project section then reads as
  no definition and there is no fallback to name matching), and a missing file.

The project section's names are resolved once at startup through the server's
read-only project list, and a task's missing project identifier is looked up
once by issue id. Both are user-API routes, so `MULTICA_TOKEN` must be a `mul_`
PAT for them to answer: a daemon-only `mdt_` token is rejected there, and the
project keys then log `policy.name_resolution … conclusion=unavailable` while
the boot continues (project tiers inject nothing; tasks and the agent section
are unaffected).

The file is loaded once: editing the ConfigMap needs a Foreman restart, and the
startup log records its sha256.

## Post-install checks

```bash
kubectl get ns foreman multica-agents
kubectl -n multica-agents get sa multica-omp
kubectl -n multica-agents get secret registry-tsic
kubectl -n foreman get secret foreman-secret
kubectl -n foreman get configmap foreman-job-template foreman-job-policy
kubectl -n foreman port-forward deploy/foreman 8080:8080 &
curl -s localhost:8080/foreman/healthz
curl -s localhost:8080/foreman/register-source
curl -s localhost:8080/metrics | grep -cE '^foreman_'
```

`/foreman/healthz` is the input of both probes: it answers `503` (and names the
stopped component) once a critical loop — claim, heartbeat, lease, recovery,
WS subscriber, profile-refresh (the last two only with
`FOREMAN_WS_ENABLED=true`) — has stopped. A loop that stops with an error while
the process is still serving is a configuration error (§1.5; today the
credential/identity mismatch `403`): the process logs `component.failed` and
exits non-zero, so the Deployment restarts it visibly instead of running
half-dead (§9.3).
