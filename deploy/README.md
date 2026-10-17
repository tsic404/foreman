# Deploying Foreman

`deploy/` is the whole deployment surface from `docs/03-contracts.md` §5.3.
Everything is plain YAML — `kubectl apply -f deploy/` installs it in one pass.

| File | Objects |
|---|---|
| `00-namespaces.yaml` | `Namespace foreman`, `Namespace multica-agents` |
| `10-secrets.yaml` | `Secret foreman-secret` (ns foreman), `Secret registry-tsic` (ns multica-agents + ns foreman) |
| `20-rbac.yaml` | `ServiceAccount foreman` + `Role`/`RoleBinding` (ns multica-agents), `ServiceAccount multica-omp`, `ServiceAccount foreman-gc` |
| `25-job-template.yaml` | `ConfigMap foreman-job-template` (ns foreman) — the required Job template, mounted by the Deployment |
| `30-foreman.yaml` | `Deployment foreman` (1 replica) + `Service foreman` (ClusterIP :8080) |
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

## Images

Two images come from this repo (`docs/04-architecture.md` §部署/运行方式): the
scheduler image for the Deployment, and the Job image (upstream multica CLI +
`omp` + `foreman-gc` + `dumb-init` + `nix`) that both the Job pods and the
`foreman-gc` DaemonSet run.
Both are published as **multi-platform manifest lists** (`linux/amd64` +
`linux/arm64`), so a cluster with nodes of both architectures pulls the platform
each node runs; nothing in `deploy/` names an architecture.

`.github/workflows/build-images.yml` builds and publishes both to GHCR — on
every push to `main` and on `v*.*.*` tags:

| Image | Tags | Platforms |
|---|---|---|
| `ghcr.io/tsic404/foreman` | `<version>`, `latest` | `linux/amd64`, `linux/arm64` |
| `ghcr.io/tsic404/foreman-job` | `<version>`, `latest` | `linux/amd64`, `linux/arm64` |

`<version>` is the pushed tag; on `main` it is the newest `v*.*.*` tag
reachable from the commit, falling back to the workflow's `DEFAULT_VERSION`
(`v0.1.0`).
`latest` follows `main` only: the shipped `FOREMAN_JOB_IMAGE` points at it,
while the version tags remain the rollback path (ADR-012).
The workflow needs no secrets for the push (`GITHUB_TOKEN`, `packages: write`)
and resolves the upstream Job-image artifacts from their release checksum
manifests, per platform; `MULTICA_CLI_VERSION` / `OMP_VERSION` repository
variables pin other upstream versions than the defaults in the workflow. `omp`
is a native binary, so each platform's asset must match the base image's libc:
`OMP_ASSET_AMD64` / `OMP_ASSET_ARM64` default to `omp-linux-musl-x64` /
`omp-linux-musl-arm64` for the musl base (`FOREMAN_BASE_IMAGE`, default
`alpine:3.20`); a glibc base needs `omp-linux-x64` / `omp-linux-arm64` as the
repository variables. The base must also carry the C++ runtime `omp` links: the
image installs Alpine's `libstdc++` package (which brings `libgcc`), and a
slimmer base that ships neither fails at exec. Both mismatches are caught by the
workflow's smoke step, which runs `omp` on the runner's platform and checks both
platforms' `multica` / `omp` against the upstream release checksums.

Whichever image ends up as `FOREMAN_JOB_IMAGE` must also carry
`/usr/bin/dumb-init`: every Job `command` starts with `dumb-init --` (it is the
agent container's PID 1, ADR-022), and the build installs Alpine's `dumb-init`
package for the default base. A base whose package sources cannot supply it, or
a custom image named by a job template, leaves the container without it and the
agent fails at exec (pod `Failed` → `job_failed`). `docs/03-contracts.md` §5.3
requires the contents, and §5.4's `FOREMAN_JOB_IMAGE` row carries it as a
deployer constraint. It is not one of the two mismatches the smoke step above
catches.

The Job image is referenced by a **movable tag** in two places that carry the
same literal (ADR-012):

- `FOREMAN_JOB_IMAGE` in `30-foreman.yaml` (written verbatim into every Job's
  `agent` and `prepare` containers — no digest concatenation), and
- the `image:` of the `foreman-gc` DaemonSet in `40-foreman-gc.yaml`
  (the DaemonSet runs the same image as the Job pods).

Rolling forward is one step: point `FOREMAN_JOB_IMAGE` at the new reference, or
let the tag itself move. Rolling back uses the version tags CI keeps publishing
(`v<ver>`) — `latest` alone is never the only published tag. Both containers in
the Job template default to `imagePullPolicy: Always`, which is what lets a
moved tag reach a node that already cached the reference; a deployment that
pins a version tag can set `imagePullPolicy: IfNotPresent` in the job template
(§Job template below) and accepts that a moved tag then skips cached nodes. `foreman-gc` pulls `Always` for the same reason.

CI publishes each run's references and digests (the `release-images-<version>`
artifact and the workflow summary) as the run's own record — nothing in
`deploy/` is backfilled from it.

Local builds are for development and staging clusters, never the release path:
`make build-job-image` builds one platform's image (`linux/$(ARCH)`, the host
architecture unless `ARCH=` says otherwise) and takes the upstream artifact URL
+ SHA-256 for the CLI archive and for `omp`, so the built image can be checked
against the upstream release (AC-13 provenance):

```bash
make build-job-image \
  MULTICA_CLI_URL=https://github.com/multica-ai/multica/releases/download/v0.6.1/multica-cli-0.6.1-linux-amd64.tar.gz \
  MULTICA_CLI_SHA256=<sha256 from that release's checksums.txt> \
  OMP_URL=https://github.com/can1357/oh-my-pi/releases/download/v18.6.1/omp-linux-musl-x64 \
  OMP_SHA256=<sha256 from that release's SHA256SUMS.txt>
```

What CI verifies about a published image is reproducible with the container CLI
alone:

```bash
# both platforms in the manifest list, each with its own digest
docker buildx imagetools inspect ghcr.io/tsic404/foreman-job:latest
# the platform the runner itself is: run it
docker run --rm --platform linux/amd64 --entrypoint omp ghcr.io/tsic404/foreman-job:latest --version
# the other platform's config, read without executing it (no emulation)
cid=$(docker create --platform linux/arm64 ghcr.io/tsic404/foreman-job:latest /bin/true)
docker cp "$cid:/usr/local/bin/multica" ./multica && sha256sum ./multica
docker rm "$cid"
```

The workflow's smoke step asserts: exactly two platform manifests, each with a
digest; the tag still resolving to the digest that run published; both
platforms' `multica` / `omp` matching their upstream release checksum;
`foreman-gc` present and executable in both configs; and `foreman` static and
built for the platform whose config carries it.

Two assertions §5.3's contents list implies are **not** in that step:
`/usr/bin/dumb-init` and `/usr/bin/nix` present and executable, and the image's
`CMD` empty. They belong to the build/publish surface (CI/ops), and this
workflow never runs on a pull request (pushes to `main`, `v*.*.*` tags, manual
dispatch) — so a PR cannot carry their result.

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

## Job template (required)

The Job's shape comes from one place: the ConfigMap `foreman-job-template`,
which `25-job-template.yaml` ships and the Deployment mounts at
`/etc/foreman/job-template/` as a **non-optional** read-only volume (key
`job-overlay.yaml`). The shipped content is the **reference template** — a byte
copy of the design repo's `test-cases/_infra/job-baseline.yaml` Job document,
the AC-20 authority — and a deployer replaces it with their own complete
`batch/v1 Job` (contract §5.4):

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

Foreman writes exactly three things into that template:

1. the env keys `MULTICA_SERVER_URL`, `MULTICA_DAEMON_ID` and
   `MULTICA_DAEMON_MAX_CONCURRENT_TASKS` on `containers[0]` — the first entry in
   writing order, not a container matched by name; a same-name entry is
   replaced whole, so those three keys are Foreman's whatever the template
   says;
2. the per-task metadata: `metadata.name`, `metadata.namespace`, and the §3.2
   label/annotation set (the `foreman.tsic.top/` domain is cleared first, and
   the four trigger-context keys are added or deleted to match the task);
3. the placeholders below.

Everything else — containers, init containers, `command`/`args`, volumes,
mounts, `resources`, `securityContext`, `affinity`, `image`,
`serviceAccountName`, the rest of `env` — is used verbatim. Nothing is checked,
added or normalized.

| Placeholder | Value |
|---|---|
| `<task_id>` / `<agent_id>` / `<workspace_id>` | the task's ids |
| `<issue_id>` / `<issue-identifier>` / `<autopilot_run_id>` / `<chat_session_id>` | the task's trigger context |
| `<job runtime uuid>` / `<RFC3339>` | the Job runtime id / the claim time |
| `<job-image>` | `FOREMAN_JOB_IMAGE` |
| `<preferred node>` | the node holding the resumed session's file (`session_id` of the task's previous run); with no resumed session — a first run — or no known node for it, or `FOREMAN_PREFER_NODE_REUSE=false`, the whole affinity item carrying it is dropped. This literal is the substitution set's **one exception**: it is consumed only where it is *recognized* — a `values` entry of a preferred-affinity `matchExpressions` reading exactly this literal — so anywhere else it stays verbatim, and the apiserver judges that position at Job creation |

Unregistered `<...>` literals stay as written, so a template that writes
`image: <job-image>` reaches the apiserver unchanged and the Job never boots;
the reference template has the placeholder in both container images.

- The file is read once at startup: an edit needs a Foreman restart, and the
  startup log records the template sha256 plus the accepted/refused verdict.
- Three things refuse startup (CrashLoopBackOff), each logged as its own class:
  a file that does not exist (the ConfigMap is there but has no
  `job-overlay.yaml` key), a file that cannot be read, and a file that is not a
  `batch/v1 Job` — bad YAML, an unknown field (`contianers` is refused, never
  silently dropped), or a missing/mismatched `apiVersion`/`kind`. Nothing else
  is checked. There is no env switch and no fallback.

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
- `MULTICA_DAEMON_ID` reads the `job-name` label the Job controller sets. A
  template that sets `manualSelector: true` (or otherwise replaces the Job's
  selector) stops the controller from adding its own labels, so that fieldRef
  resolves to an empty string and the daemon registers without an id — nothing
  checks the selector at startup.

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
  `fm-catalog-<request_id>-cred`. A template that does not means the probe
  cannot authenticate, so startup exits non-zero (visible, not silent).
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
  discovered list, the declared runtimes and each runtime's catalog slot state
  (read-only — it never triggers a discovery).

## Post-install checks

```bash
kubectl get ns foreman multica-agents
kubectl -n multica-agents get sa multica-omp
kubectl -n multica-agents get secret registry-tsic
kubectl -n foreman get secret foreman-secret
kubectl -n foreman port-forward deploy/foreman 8080:8080 &
curl -s localhost:8080/foreman/healthz
curl -s localhost:8080/foreman/register-source
curl -s localhost:8080/metrics | grep -cE '^foreman_'
```
