# Deploying Foreman

`deploy/` is the whole deployment surface from `02-contracts.md` §3 and §8.
Everything is plain YAML — `kubectl apply -f deploy/` installs it in one pass.

| File | Objects |
|---|---|
| `00-namespaces.yaml` | `Namespace foreman`, `Namespace multica-agents` |
| `10-secrets.yaml` | `Secret foreman-secret` (ns foreman), `Secret registry-tsic` (ns multica-agents + ns foreman), `Secret foreman-environment-registry` (ns foreman) |
| `20-rbac.yaml` | `ServiceAccount foreman` + `Role`/`RoleBinding` (ns multica-agents), the environment-registry `Role`/`RoleBinding` (ns foreman), `ServiceAccount multica-omp`, `ServiceAccount foreman-gc` |
| `25-job-template.yaml` | `ConfigMap foreman-job-template` (ns foreman) — the required Job template, mounted by the Deployment |
| `26-job-policy.yaml` | `ConfigMap foreman-job-policy` (ns foreman) — the **optional** three-axis job policy, mounted by the Deployment |
| `30-foreman.yaml` | `Deployment foreman` (1 replica) + `Service foreman` (ClusterIP :8080, the Job face only) + `Service foreman-ops` (ClusterIP :8081, the credential-gated ops surface) |
| `35-shared-state.yaml` | `StatefulSet foreman-redis` + `Service foreman-redis` — the shared state store (ADR-046 阶段一); **baseline only** — a deployment swaps in its own persistent Redis (or a managed service) and points `FOREMAN_STATE_URL` at it |
| `40-foreman-gc.yaml` | `DaemonSet foreman-gc` (ns multica-agents) |
| `optional/servicemonitor.yaml` | `ServiceMonitor foreman` — needs the Prometheus Operator CRDs; scrapes `Service foreman-ops` with the bearer credential |
| `optional/ops-networkpolicy.yaml` | `NetworkPolicy foreman-ops-ingress` — optional second fence: the ops port admits the `foreman` namespace only |
| `optional/agent-backend-mirror.sh` | no objects — renders the template ConfigMap with the `omp` origin repointed at a deployer mirror (§Agent backend origin) |

```bash
kubectl apply -f deploy/                             # namespaces → secrets → rbac → workload
kubectl apply -f deploy/optional/servicemonitor.yaml # only with the Operator CRDs
kubectl -n foreman rollout status deploy/foreman
```

The Service is reachable inside the cluster as
`foreman.foreman.svc.cluster.local:8080` — that hostname is baked into every
Job by the job builder, so the Service name must stay `foreman` in namespace
`foreman`.

## Operations surface

Foreman serves two listeners, and they are not interchangeable:

| Listener | Address | Content |
|---|---|---|
| Job face | `:8080` (Service `foreman`) | `/api/*` for the Job daemon, plus `/foreman/healthz` (the liveness probe input) |
| ops surface | `:8081` (Service `foreman-ops`) | `/metrics`, `/foreman/tasks*`, `/foreman/register-source`, `/foreman/version` |

The Job-facing Service publishes only `:8080`, and the Job face mounts only the
daemon API plus the liveness answer — a Job pod dialing its own server address
with its Job Token reaches no projection, payload preview, Job log or register
source (02-contracts.md S-25). `/foreman/healthz` carries the liveness verdict
alone (status, uptime, stopped component names): no task, workspace or runtime
identity field, so it is safe on the probe face a kubelet cannot authenticate.

Every ops request must present a bearer credential:

```bash
TOKEN=$(kubectl -n foreman get secret foreman-secret -o jsonpath='{.data.FOREMAN_OPS_TOKEN}' | base64 -d)
kubectl -n foreman port-forward svc/foreman-ops 8081:8081 &
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/foreman/tasks?workspace=<workspace-id>
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/metrics | grep -cE '^foreman_'
```

Without a credential (or with a Job Token, which is a credential of the other
face) every route answers `401`. `FOREMAN_OPS_TOKEN` is the deployment-wide
credential. Optional workspace-bound credentials narrow the task-facing routes
to one workspace and are refused on `/metrics` (its series carry a
per-workspace label):

```yaml
- name: FOREMAN_OPS_WORKSPACE_TOKENS
  value: "f311e8ff-…-…=<token>,<workspace-id>=<token>"
```

A workspace-bound credential naming a workspace the deployment does not serve
refuses startup. The workspace dimension of a request is resolved from the
credential's grant: a `?workspace=` value outside it is answered like a
workspace that does not exist (`404`), and a single-workspace grant is the
default dimension.

A workspace id is only unique inside one upstream, so a bare `workspace=token`
entry is bound to the single registered source that holds it; when zero or
several sources hold it the grant is refused (fail closed) rather than widened.
Name the pair explicitly to pin it:

```yaml
- name: FOREMAN_OPS_WORKSPACE_TOKENS
  value: "atlas/ws-core=<token>,harbor/ws-core=<token>"
```

`server/workspace=token` grants exactly that pair. A route reached with the
pair grant sees only rows whose `(server_id, workspace_id)` matches; a request
naming another server's identical workspace id answers `404`.

The bare form also carries the local ops read scope — `/foreman/tasks`, the
task detail, its logs and `/foreman/register-source` for that workspace, which
is how a single-workspace operator credential has always worked. The
`server/workspace` form is a management-surface grant only: it deliberately
does not grant those local reads, so a credential for a remote source can never
read this deployment's identically named workspace.

A Prometheus scraper must send the same bearer credential (e.g. the Prometheus
Operator `authorization.credentials` Secret reference); the `prometheus.io/*`
annotations on the Deployment and `Service foreman-ops` only locate the port
and path. `deploy/optional/servicemonitor.yaml` is the Operator form.

`deploy/optional/ops-networkpolicy.yaml` is an optional second fence: a
NetworkPolicy that admits the ops port only from the `foreman` namespace. Apply
it where the CNI enforces policies and the scraper can be placed there; the
credential face does not depend on it.

### Management surface (`/admin/*`)

The same ops listener also serves the management API the console reads. It has
its own route table under `/admin/` (so the ops route set above is unchanged)
and reuses the same bearer credentials: `FOREMAN_OPS_TOKEN` is the
deployment-wide administrator, `FOREMAN_OPS_WORKSPACE_TOKENS` narrows every
read and binding write to one workspace. A route never widens `?workspace_id=`
past the credential's grant; a cross-grant id answers `404`.

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/admin/servers
curl -s -H "Authorization: Bearer $TOKEN" -X POST localhost:8081/admin/servers \
  -d '{"server_id":"atlas","url":"https://multica.example.test","credential_ref":"atlas"}'
curl -s -H "Authorization: Bearer $TOKEN" -X POST localhost:8081/admin/servers/atlas/sync
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/admin/overview
```

| Route | Body / query | Answer |
|---|---|---|
| `GET /admin/servers` | — | the registered sources the grant reaches |
| `POST /admin/servers` | `{"server_id","url","credential_ref","allowed_workspaces","auto_sync","sync_period_s","retry_budget"}` | `201` and the source view |
| `PATCH /admin/servers/{id}` | `{"auto_sync","sync_period_s","retry_budget"}` | the updated source view |
| `POST /admin/servers/{id}/sync` | — | the sync report; a failed sync answers non-2xx and names the generation that stayed current |
| `GET /admin/servers/{id}/directory` | `?workspace_id=` | the current generation (workspaces, projects, agents, runtimes, tombstones) |
| `GET /admin/agents` | `?server_id=&workspace_id=` | agent rows with their management class and the two runtime groups |
| `PUT /admin/servers/{id}/workspaces/{ws}/agents/{agent}/binding` | `{"runtime_id","expected_runtime_id"}` | the binding outcome; only `applied` is `200` |
| `GET /admin/overview` | — | the four counts and the in-progress rows |
| `GET /admin/audit` | `?server_id=` | audit rows, newest first |

The CLI, Agent configuration and restricted discovery faces sit beside the
catalog routes on the same credential:

| Route | Body / query | Answer |
|---|---|---|
| `GET /admin/cli` | — | CLI release lines with their platform set, candidates and pointers |
| `POST /admin/cli` | `{"cli_id","kind","adapter","server_id","required_platforms","minimum_version","manifest_url","auto_check","check_period_s","auto_release"}` | `201` and the definition view |
| `GET /admin/cli/{cli_id}` | — | one definition view |
| `PATCH /admin/cli/{cli_id}/settings` | `{"auto_check","check_period_s","auto_release"}` | the updated view; checking and auto-release are independent |
| `POST /admin/cli/{cli_id}/check` | — | the check report; a manual check works with auto-check off |
| `POST /admin/cli/{cli_id}/versions` | `{"version","artifacts":[{"platform":{"os","arch"},"url","sha256","size"}]}` | `201` and the candidate list |
| `POST /admin/cli/{cli_id}/release` | `{"version","expected_generation","expected_release_generation"}` | `200` published, `409` stale candidate or release generation, `422` verification failed |
| `GET /admin/cli/{cli_id}/impact` | `?version=&generation=&release_generation=` | the read-only next-start preview; an untrusted scope is `known:false`, never a zero count |
| `GET /admin/cli/{cli_id}/artifacts/{sha256}` | — | the verified artifact bytes retained by the cache; an unknown digest is `404` |
| `GET /admin/cli-audit` | — | release/check audit, including the pre-release impact snapshot |
| `GET /admin/agent-configs` | `?server_id=&workspace_id=` | config revisions; the content is never returned |
| `PUT /admin/agent-configs/{server}/{workspace}/{backend}` | `{"profile","file_name","secret_ref","config"}` | the recorded revision; the virtual JSON is validated and discarded |
| `GET /admin/agent-configs/{server}/{workspace}/{backend}/projection` | `?profile=&attempt_id=` | the per-attempt read-only projection description |
| `GET /admin/agent-configs/audit` | — | config writes without content |
| `POST /admin/discovery/requests` | `{"server_id","workspace_id","runtime_id","profile","kind","cli_id","adapter","platform","config_revision"}` | the persisted request/result; duplicate upstream ids reconcile once |
| `GET /admin/discovery/requests` | `?server_id=&workspace_id=` | request history |
| `GET /admin/discovery/adapters` | — | the adapter trust table |
| `POST /admin/discovery/adapters/verify` | — | probe every adapter |
| `POST /admin/discovery/adapters/{name}/verify` | — | probe one adapter |
| `GET /admin/discovery/models` | `?server_id=&workspace_id=&runtime_id=&profile=` | the current model catalog; legal empty, unsupported and failed are distinct |

The three metadata documents live on the same writable emptyDir as the catalog
registry: `FOREMAN_CLI_STATE_FILE` (default
`/var/lib/foreman/cli-releases.json`), `FOREMAN_AGENT_CONFIG_STATE_FILE`
(`/var/lib/foreman/agent-configs.json`) and `FOREMAN_DISCOVERY_STATE_FILE`
(`/var/lib/foreman/discovery.json`). They carry release pointers, revisions and
redacted results only — never artifact bytes, configuration content or a
Secret value. Verified CLI bytes live in the separate
`FOREMAN_CLI_ARTIFACT_CACHE_DIR` (default `/var/lib/foreman/cli-cache`),
addressable only by release domain and frozen digest; a damaged entry is a
miss and is refetched from the registered source by the same digest.
`FOREMAN_OMP_BINARY` is optional and unset in the shipped manifest: the
reference image carries no Agent CLI. While it is unset, the adapter is
trusted by the bound probe of the exact published artifact — the same bytes a
Job runs — and the trust table flips to `trusted` when that probe passes
(`discovery.adapter name=omp status=trusted`). Set it only when the image
really installs an omp binary beside the server; the startup probe then uses
that path. Execution always uses the published, verified CLI artifact and the
exact configuration revision. It never reports an empty model list as
success. Codex and Claude stay `unsupported` until their command/protocol
surfaces are verified for the pinned versions.

A successful publish re-resolves its release domain: a source whose cold-start
declaration was empty registers its runtime as soon as the Agent CLI is
published, without a restart. A refused or timed-out registration retries with
backoff, and republishing the same version is a retry signal for a declaration
that never landed. A source already serving is not re-registered; its new
pointer takes effect on the next start, which the impact preview describes.

The Agent configuration surface is a prototype boundary: it validates a
fictional JSON document, records the digest/size/revision and never creates a
real Secret. The Job-side projection consumes the returned per-attempt
description; a production deployment still has to wire the selected revision
to its Secret manager.

The binding write needs an upstream that can make it conditional on the value
that was read. The pinned Multica API's `PUT /api/agents/{id}` is
unconditional — no version or `If-Match` — so the adapter reports the
capability as absent and this route answers `501` with the reason instead of
sending an unconditional body. A read-back compared afterwards cannot prove
that a competing edit did not happen between the read and the write, so the
surface stays read-only rather than risk overwriting another administrator.
Calls always carry the agent's workspace (the `workspace_id` query and the
`X-Workspace-ID` header); the pre-read, the write and the read-back all name
it, and an answer naming another agent or workspace is refused.

### Environment management (`/admin/environments`)

The management surface registers rsbxd targets and drives their lifecycle. A
registration is a **stable environment id**, never a Kubernetes Node or a Pod
affinity rule: selection uses only the registered trust domain, allowed
sources, capabilities and free slots. A target is stored as `pending` after a
failed verification and is never a scheduling candidate; only a successful
probe of identity, capabilities and capacity makes it active.

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/admin/environments
curl -s -H "Authorization: Bearer $TOKEN" -X POST localhost:8081/admin/environments -d '{
  "id":"build-a",
  "endpoint":"build-a.example.test:7443",
  "tls_ref":"ref/build-a",
  "state_root":"/srv/rsbx/build-a",
  "trust_domain":"build",
  "allowed_sources":["atlas"],
  "capabilities":["nix"],
  "concurrency":4,
  "expected_identity":"sha256:…"
}'
curl -s -H "Authorization: Bearer $TOKEN" -X POST localhost:8081/admin/environments/build-a/drain
curl -s -H "Authorization: Bearer $TOKEN" -X POST localhost:8081/admin/environments/build-a/resume
curl -s -H "Authorization: Bearer $TOKEN" -X DELETE localhost:8081/admin/environments/build-a
```

`endpoint` accepts the same forms the Job-side adapter opens: a bare
`host:port` or `tls://host:port` for the authenticated network channel, an
absolute socket path or `unix:///…` for a node-local socket, and
`ssh://[user@]host[:port]` or `cmd:<argv>` for a deployment that carries its
own ssh client. A form no carrier channel serves is refused at registration,
so a stored target is always one an attempt can reach. §Session transports in
the Job template section documents the projection each form needs.

| Route | Body / query | Answer |
|---|---|---|
| `GET /admin/environments` | `?trust_domain=&source_id=` | registrations the credential's sources reach |
| `POST /admin/environments` | the registration above | `201` and the environment view; a failed probe still stores the target as `pending` |
| `GET /admin/environments/{id}` | — | one environment view |
| `PATCH /admin/environments/{id}` | any registration field | the updated view; identity-affecting edits become `stale` and need `verify` |
| `POST /admin/environments/{id}/verify` | — | the verification outcome; a failure is a `200` view with the failure on its face |
| `POST /admin/environments/{id}/drain` | — | `draining`; new slots are refused, live sessions are not evicted |
| `POST /admin/environments/{id}/resume` | — | `active` when the verification is still valid |
| `DELETE /admin/environments/{id}` | — | `200` with the tombstone, or `409` with every retirement blocker |
| `GET /admin/environments/retired` | — | retirement tombstones the credential reaches |
| `GET /admin/environments/{id}/slots` | — | the active and isolated slot ledger |
| `POST /admin/environments/{id}/slots/{slot}/confirm` | — | clears an isolated slot after the remote close is confirmed |
| `GET /admin/environments/{id}/cleanups` | — | pending non-slot cleanups |
| `POST /admin/environments/{id}/cleanups/{cleanup}/confirm` | — | clears one confirmed cleanup |
| `GET /admin/environments/audit` | — | environment audit rows, newest first |

Registration, edit, verify, drain, resume, retirement and cleanup confirmation
need the deployment-wide credential; a workspace-bound credential may read
only the environments whose `allowed_sources` is empty (every source) or
intersects its source grant.
Retirement is refused while an active slot, an isolated slot with an
unconfirmed close, a pending cleanup or a published/drafted rule still
references the environment. It removes the registration and keeps a tombstone;
it never deletes remote files, sessions or Nix roots. A retired id cannot be
reused. A replacement physical tree at the same address must register a new
id, and an edit cannot move an existing id to another `state_root`.

The environment registry lives in the `foreman-environment-registry` Secret
(`FOREMAN_ENVIRONMENT_STATE_SECRET` / `FOREMAN_ENVIRONMENT_STATE_NAMESPACE`),
not on a Pod volume: registrations, isolated slots and retirement tombstones
survive a Deployment rollout or Pod eviction, and a retired id stays retired.
The Secret object ships without a `data` key; Foreman owns the
`environments.json` key and only needs `get`/`update` on that one object. A
missing or malformed Secret refuses startup instead of rebuilding an empty
pool. `FOREMAN_ENVIRONMENT_STATE_FILE` is a development/test fallback used only
when Foreman runs without a Kubernetes client; it is node-local and is not
part of the Deployment.

#### Writing the registry document by hand

The object ships without a `data` field on purpose (see the comment on it in
`deploy/10-secrets.yaml`): a later `kubectl apply -f deploy/` must never
overwrite the live registry, so Foreman creates the `environments.json` key
itself on the first accepted mutation. Only an out-of-band seed or repair
writes that key by hand; a stock installation never needs to.

`environments.json` is one JSON object. This is the empty document:

<!-- environment-registry-empty-document -->
```json
{
  "ledger_id": "",
  "carrier_uid": "",
  "generation": 0,
  "environments": [],
  "tombstones": []
}
```

Keep `ledger_id` and `carrier_uid` empty for a fresh registry. Startup mints a
ledger identity for the live Secret object, and the first committed write
stores that `ledger_id` together with the object's `metadata.uid` as
`carrier_uid`. A hand-written pair is adopted only when its `carrier_uid`
equals that `metadata.uid` — a uid the object only gets once it exists — so the
one case for copying a pair verbatim is restoring a document into the very
Secret it was written through. Overwriting a populated document with this
template also starts a new ledger instance, and probe evidence bound to the
old identity stops matching. Leave `generation` at 0: Foreman advances it on
every committed write. Add environments at runtime with
`POST /admin/environments` (the registration example and route table above)
rather than by hand: their entries carry verification and slot state that
Foreman owns and rewrites.

An array is not a document. Writing `environments.json` as `[]` refuses
startup with

```
json: cannot unmarshal array into Go value of type environment.environmentDocument
```

so every field belongs inside the object above. Seed the key in place
(`kubectl -n foreman edit secret foreman-environment-registry`, or a patch)
rather than by adding `data` to a manifest you keep applying: that copy would
overwrite whatever Foreman has written since.

`FOREMAN_ENVIRONMENT_TRUST_DOMAINS` is the comma-separated data-residency
allowlist. It has no implicit "allow everything" mode: when it is empty every
registration is refused, and a target whose domain was removed from the list
leaves the candidate pool after a restart until an operator either restores
the grant or moves the registration to an approved domain. The shipped
Deployment leaves it empty on purpose; set the domains this installation is
approved to use before registering a machine.

Verification runs the deployment-supplied helper named by
`FOREMAN_ENVIRONMENT_PROBE` directly (no shell). The helper receives the
target as one JSON document in `FOREMAN_ENVIRONMENT_TARGET`:

```json
{"endpoint":"build-a.example.test:7443","tls_ref":"ref/build-a",
 "state_root":"/srv/rsbx/build-a","trust_domain":"build",
 "allowed_sources":["atlas"],"capabilities":["nix"],"concurrency":4}
```

It answers on stdout:

```json
{"tree_id":"…","identity":"sha256:…","capabilities":["nix"],
 "capacity":8,"version":"rsbxd-…"}
```

`tree_id` is the persistent tree's ownership identity, not the endpoint;
duplicate trees are refused and the same endpoint with a new tree supersedes
the old registration. A `state_root` path is not an ownership key: two hosts
commonly use the same path, so different endpoints with the same root remain
separate trees. A tree id observed by a failed first attempt is only an
observation; it does not freeze the registration, so fixing the endpoint or
TLS identity and verifying again works under the same id. The helper must
report a non-empty identity, every declared capability and a capacity at least
the configured concurrency.
Unknown fields, trailing JSON, a nonzero exit, a timeout or malformed output
fail verification and leave the target `pending`. With no helper configured,
every registration stays pending rather than guessing reachability.

The management console itself is served from the same listener:

```bash
kubectl -n foreman port-forward svc/foreman-ops 8081:8081 &
# open http://localhost:8081/admin/ and paste the FOREMAN_OPS_TOKEN value
```

`GET /admin/` answers the console document and `GET /admin/assets/…` the
stylesheet and ES modules it loads; both are embedded in the binary, so the
deployment needs no extra volume or image. These two paths are the only
`/admin/` answers that do not require the bearer credential: the shell carries
no data, and a browser cannot attach an `Authorization` header to a
navigation. Everything it displays comes from the gated `/admin/*` routes
above, with the credential typed into the page. The page keeps that credential
in memory only — never a URL, cookie or browser storage — so a refresh asks
for it again. Do not bake `FOREMAN_OPS_TOKEN` into a page, a proxy rewrite or a
link: it is the deployment-wide administrator.

Each source credential is a separate file in `FOREMAN_SOURCE_CREDENTIALS_DIR`
(default `/etc/foreman/catalog-credentials`), named after the registration's
`credential_ref`. The catalog stores the reference only; the value is read per
request and never appears in a response, an audit row or a log. Mount that
directory from a Secret when registering sources:

```yaml
- name: FOREMAN_SOURCE_CREDENTIALS_DIR
  value: "/etc/foreman/catalog-credentials"
volumeMounts:
- {name: catalog-credentials, mountPath: /etc/foreman/catalog-credentials, readOnly: true}
volumes:
- name: catalog-credentials
  secret: {secretName: foreman-catalog-credentials, optional: true}
```

> Upgrading an install that shipped before the scheduler label: the Deployment
> selector now carries `app.kubernetes.io/component: scheduler` (without it
> `kubectl logs deploy/foreman` also matches the shared-state pod). A selector
> is immutable, so replace the object once — `kubectl -n foreman delete deploy
> foreman` — and apply the manifest again; the pod restarts, the Service and
> its name stay the same.

## Secrets

`10-secrets.yaml` carries **placeholders**; `kubectl apply -f deploy/` will
overwrite live values with them, so pick one of:

- edit the file before applying (replace every `REPLACE_ME`), or
- create the Secrets out of band and remove `10-secrets.yaml` from the apply set.

The shipped `FOREMAN_JOB_TOKEN_KEY` placeholder is deliberately not valid hex:
an un-replaced copy makes Foreman fail its startup self-check and CrashLoop,
rather than serve with a key anyone can read from the repository. The
`FOREMAN_OPS_TOKEN` placeholder (`REPLACE_ME`) is refused the same way: an
un-replaced copy CrashLoops instead of exposing the ops surface with a token
published in the repository.

```bash
# Server credential: mdt_/mul_ token. Workspace-scoped form: bound to
# FOREMAN_DAEMON_ID + workspace. Discovery form: the account's membership
# defines the served set (§Multi-workspace).
kubectl -n foreman create secret generic foreman-secret \
  --from-literal=MULTICA_TOKEN='mdt_...' \
  --from-literal=FOREMAN_JOB_TOKEN_KEY="$(head -c 32 /dev/urandom | xxd -p -c 64)" \
  --from-literal=FOREMAN_OPS_TOKEN="$(head -c 32 /dev/urandom | xxd -p -c 64)" \
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

One deployment can serve several workspaces (ADR-047, ADR-049). The server
credential's scope selects the deployment form; every control-plane action runs
once per `(credential, daemon identity, workspace)`.

### Discovery form (default): one user-scoped credential

Give the deployment one user-scoped credential and **no workspace id**:

```yaml
env:
  - name: MULTICA_TOKEN          # the account's credential (mul_/mdt_)
    valueFrom: {secretKeyRef: {name: foreman-secret, key: MULTICA_TOKEN}}
  - name: FOREMAN_DAEMON_ID
    value: "foreman"             # deployment-level base identity
```

At startup Foreman lists every workspace the credential can see (C-17) and
serves all of them; each workspace gets a deployment identity derived from the
base identity (`foreman-<workspace>`, stable across restarts, distinct across
workspaces). Zero workspace ids to hand-write.

Narrow the scope with an optional manifest — a JSON array of workspace ids:

```yaml
  - name: FOREMAN_WORKSPACES_MANIFEST_FILE
    value: /etc/foreman/workspaces.json
# /etc/foreman/workspaces.json: ["<workspace-uuid>", ...]
```

`FOREMAN_WORKSPACES_MANIFEST` accepts the same list inline (mutually exclusive
with the file form). Rules (ADR-049 decision 1): the discovery set is
`manifest ∩ visible`; a manifest entry the credential cannot see, an empty
manifest (`[]`), or an unreachable/rejected/empty C-17 answer refuses startup —
never a half-configured service. Workspaces visible but outside the manifest
are not registered or served, and the startup discovery event names them.

### Workspace-scoped form: one credential per workspace

Set `FOREMAN_WORKSPACES_FILE` (or `FOREMAN_WORKSPACES` inline) to a mounted
file holding one `(credential, workspace)` pair per workspace:

```json
[
  {"workspace_id": "ws-a", "daemon_id": "foreman-a", "token": "mdt_..."},
  {"workspace_id": "ws-b", "daemon_id": "foreman-b", "token_env": "MULTICA_TOKEN_B"}
]
```

`token_env` names the Secret key's env variable; `token` may be inlined. Both
ids are unique, and no daemon identity may be shared across workspaces (F2).
This form accepts **no manifest** — the configured pairs are the discovery set.
Each credential's C-17 answer must be exactly its configured workspace, else
startup is refused (a workspace-scoped credential that sees more is mis-scoped).

The legacy `MULTICA_WORKSPACE_ID` / `FOREMAN_DAEMON_ID` / `MULTICA_TOKEN` triple
is the workspace-scoped form with one pair.

> **Migrating a single-workspace deployment.** If its credential is a user
> token that sees more than one workspace, the exact-set check above refuses
> startup (it always did see more, but the check used to accept a superset).
> Either switch to the discovery form (drop `MULTICA_WORKSPACE_ID`; add a
> manifest to keep serving one workspace) or move to a workspace-scoped
> credential.

### Serving scope and events

- The startup probe is one Job. It registers to every workspace of S-1's answer
  (the discovery set) — the upstream daemon has no workspace whitelist, and the
  template cannot narrow the scope. Foreman neither filters nor pads that
  answer. The **effective service scope** is what the probe actually registered:
  a discovery-set member the probe never registered is not served, logs one
  `workspace.probe_registration_absent` warn and appears in the
  `workspace.serving` event.
- The probe Job is created only after the Job-side address
  (`FOREMAN_JOB_SERVER_URL`, the Service every Job pod dials) answers the health
  endpoint: on a cold start the pod is not Ready yet, the Service has no
  endpoint, and the probe's daemon — which does not retry, and whose Job ships
  `backoffLimit: 0` — would die on `connection refused` before it could
  register. The wait is one tenth of `FOREMAN_STARTUP_PROBE_TIMEOUT` and
  visible (`probe.endpoint_ready` / `probe.endpoint_wait_timeout`); if it runs
  out, the probe is created anyway and the probe's own window decides. A probe
  Job that reaches a terminal condition (failed or completed) decides the
  startup verdict at once instead of at the window's horizon.
- `workspace.discovery` (once) names the source (visible set / manifest /
  workspace-scoped pairs), the discovery set and the manifest-excluded
  workspaces. `workspace.discovery_failed` names the reason and the credential
  scope when startup is refused.
- The set is fixed for the process lifetime: `daemon:workspaces_changed` is
  received and ignored — membership changes take effect on the next restart.

### Failure surface

- Workspace-scoped credential: a failing credential or identity backs off only
  its own workspace; the others keep heartbeating, claiming and subscribing.
- User-scoped credential: a rejected credential (401/403) is a deployment-level
  failure — every workspace's control-plane loop backs off on the same
  credential; no request keeps using it.
- Runtime 401/403/404 are never judged configuration errors (§1.5): they back
  off and retry. Only the startup self-check (discovery, C-17 scope, TTL
  arithmetic) refuses startup.

### Budget, policy and node state

- **Budget**: `FOREMAN_MAX_INFLIGHT_JOBS` is the global cap;
  `FOREMAN_MAX_INFLIGHT_JOBS_PER_WORKSPACE` adds the per-workspace cap (0 = none).
- **Policy**: partition the table under `workspaces.<workspace_id>`:
  ```yaml
  workspaces:
    ws-a: {agents: {...}, projects: {...}}
    ws-b: {agents: {...}, projects: {...}}
  ```
  The flat `agents` / `projects` form is the single-workspace form and refuses
  startup when more than one workspace is configured. The partition key can be
  either the workspace id (unchanged, always works) or the workspace display
  name as reported by the server at startup — the two are resolved once at
  boot and never re-resolved. Two workspaces with the same display name make
  the name unusable as a key: writing it refuses startup, and the identifier
  form still works. Renaming a workspace after this restart invalidates the
  old name — the next start refuses until the key is updated. A display-name
  key over a workspace-scoped credential whose C-17 check failed transiently
  warns (`显示名权威暂不可用`) and boots without that partition; it resolves
  on the next restart.
- **Node state**: every workspace-partitioned location in the template must be
  expressed through `<workspace_id>` (the shipped template does this for the
  home volume). A fixed literal makes the template correct for one workspace
  only — a deployment error Foreman does not correct.
- **Isolation**: cross-workspace Job Tokens, projections, delivery and probes are
  rejected (403 / 404); a credential failure retries only its workspace (or the
  whole deployment for the user-scoped form).

## Images

Two images run a task: the Foreman image this repo publishes, and the external
rsbx carrier the Job executes. They have different jobs.

| Image | Built here | What it carries |
|---|---|---|
| `ghcr.io/tsic404/foreman` | yes | the server, the Job-side supervisor `foreman-job`, `foreman-gc`, and the reference Job execution adapters |
| `ghcr.io/tsic404/rsbx` | no | the rsbx client; no Multica CLI and no Agent CLI |

`foreman` is published as a **multi-platform manifest list** (`linux/amd64` +
`linux/arm64`) by `.github/workflows/build-foreman.yml`, which is the whole
release path. A `v*.*.*` tag push publishes exactly `foreman:v<X.Y.Z>` and
leaves `latest` alone; every other run (a `main` push or a manual dispatch as
its re-run entry point) publishes exactly `foreman:latest`. A published
version tag therefore always names the image built from that tag and stays a
valid rollback target (ADR-012).

**The Job carrier is frozen per attempt.** `FOREMAN_JOB_IMAGE` names the rsbx
repository *and* the verified index digest
(`ghcr.io/tsic404/rsbx@sha256:<digest>`); a value without a digest refuses the
claim, and the Job render never emits a movable carrier tag (V2-C08). The
shipped literal is the verified index. The rsbx index currently published
carries only a `linux/amd64` manifest, so a binding's client platform must
match a platform the digest actually publishes; the Job's node selector copies
the frozen platform's architecture. Before pinning a different rsbx digest,
check its manifest list; do not infer the platform from the node pool.

**The supervisor is not part of the carrier.** A `job-runtime` init container
runs from `FOREMAN_JOB_RUNTIME_IMAGE` (default `ghcr.io/tsic404/foreman:latest`)
and copies `foreman-job` and the reference Job adapters from `/usr/local/bin`
into an emptyDir; the carrier's agent container executes
`/run/foreman/runtime/foreman-job`. Point `FOREMAN_JOB_RUNTIME_IMAGE` at the
same release the Deployment runs — a digest or the matching version tag — so an
old Foreman does not hand a Job a newer supervisor. The runtime image carries
no CLI either.

**The Job's execution adapters ship in the runtime image.** `foreman-job-cli`
(CLI fetch), `rsbx-remote` (session open and close), `foreman-job-register`
(registration observation), `foreman-job-runner` (dispatch) and
`foreman-job-slots` (shared ledger) are the reference implementation of
`02-contracts.md` V2-C29. They are the default, not a requirement: a deployment
that supplies its own adapters points `FOREMAN_JOB_RUNTIME_IMAGE` at an image
carrying them under `/usr/local/bin` with the same names, and the supervisor
starts them the same way. The process, argument and environment conventions
between the supervisor and an adapter are an implementation detail of this
build, so a replacement must be built against this release's supervisor.

### Session transports and material

`rsbx-remote` and the management surface accept the same endpoint forms, and
the registration refuses a form the adapter cannot open:

| Form | Carrier channel | What the Job projects |
|---|---|---|
| `/abs/path/daemon.sock`, `unix:///abs/path/daemon.sock` | local socket | the socket's directory, at the same path, read-only |
| `host:port`, `tls://host:port` | authenticated network (`tls`) | `foreman-env-tls-<environment id>`, read-only |
| `ssh://[user@]host[:port]` | `ssh` stdio | nothing (`SSL_CERT_FILE` when a bundle is configured) |
| `cmd:<argv>` | explicit stdio command | nothing |

A bare `host:port` selects the authenticated network channel: the shipped
carrier carries no ssh client, so the historically accepted form must not
resolve to a program the image does not contain. A deployment that does carry
an ssh client names that channel explicitly. The carrier authenticates the
network channel with mutual TLS; termination and the ssh client's own identity,
proxy and host-key configuration stay the deployer's.

For the network channel the Job mounts exactly the selected environment's
material: create a Secret `foreman-env-tls-<environment id>` in
`FOREMAN_JOB_NAMESPACE` with the keys `ca.pem` (the trust anchor),
`cert.pem` (the client certificate), `key.pem` (its private key) and,
optionally, `server_name` (the name the client expects in the server's
certificate). `tls_ref` must still be the registration's `ref/<name>`; the
reference render mounts the Secret at `/run/foreman/tls/ref/<name>` and points
`FOREMAN_JOB_TLS_DIR` at `/run/foreman/tls`. A missing, incomplete or
unmatched set refuses the session before anything starts; a certificate the
server's CA does not accept fails the handshake, which the run shows as a
connection failure with zero business delivery (V2-C12).

**The carrier client's posture.** The rsbx client mounts the sandbox with
FUSE, so its container needs `/dev/fuse` and the privilege the device's cgroup
requires. The reference fixed render therefore runs the `agent` container as
root with `privileged: true`, and projects `/dev/fuse` plus exactly the unix
socket the chosen endpoint names — never its directory, so a runtime socket
that shares that directory (a `containerd.sock` under `/run`, say) does not
enter the Pod. The privilege stays with the sandbox boundary: the
business commands the carrier starts execute on the rsbx daemon's host, not in
the Pod, and the render keeps host namespaces and container-runtime sockets
out. The reference template in `25-job-template.yaml` is a different shape and
stays non-privileged; it serves only the legacy path that does not read this
render (see §Job template). A deployment that replaces the reference adapters
supplies its own posture with them.

Before it opens a session the reference `rsbx-remote` runs the deployment's own
environment probe once more and compares the observed tree identity and target
identity with the frozen binding. A tree that was replaced under the same
`state_root`, a target whose identity no longer matches, or a probe that cannot
observe the target refuses the session — ready is never answered from the
frozen input. This needs the probe reachable from the Job face at ready time;
without it the attempt fails closed instead of starting on an unverified tree.

**The CLIs arrive only through the proxy.** At start-up the supervisor fetches
the two frozen artifacts (the Multica CLI and the selected Agent CLI) from
`/cli/{kind}/{version}/{os}/{arch}?digest=...` on the Job-facing listener,
using the attempt's own Job token, and verifies platform, exact version,
digest, executable entry and its download address before it reports rsbx
ready, registers or delivers (V2-C09–C11). `FOREMAN_JOB_CLI_BUDGET_BYTES`
(default `536870912`, 512 MiB) is the attempt's shared download budget;
retries only re-fetch the same frozen artifact, and exhaustion fails the
attempt. The download limiter is separate from heartbeat, lease and result
budgets, so a saturated download window cannot reject control traffic
(V2-C28). There is no direct release-source, image-bundled or local-execution
fallback.

Verification also limits one downloaded artifact, but that limit is an
implementation choice of this build, not a capacity guarantee: do not size
other components against it, and expect it to move with the implementation.
The reference Deployment requests 512 MiB and limits the container to 1 GiB
because verification buffers one artifact before it enters the cache. Treat
both figures as this deployment's own resource recommendation, not as part of
the CLI release contract.

**The attempt-state volume carries a second copy of those artifacts.** The
carrier keeps its sandbox and a verified snapshot of every program it runs
locally under the same root, and the session's cache directory is one of those
entries, so an open session writes both frozen artifacts into the volume again.
The fixed render therefore sizes the volume as the attempt's download budget
plus `512Mi` of headroom for the sandbox, the session log and the local
records, with a `2Gi` floor; raising `FOREMAN_JOB_CLI_BUDGET_BYTES` raises the
volume with it. A budget so large that the budget plus the headroom no longer
fits an `int64` is refused at render time instead of wrapping to a negative
size and clamping back to the floor. A volume sized for the record alone
evicts the Job in the middle of a session — `64Mi` was smaller than one Agent
artifact, and the eviction reached the task as a failed terminal with no close
record. Leaving the volume unbounded is not the alternative: one Job would
then fill the node's disk.

Release and rollback are the same one-step operation as before: point the
Deployment at another `foreman` tag (and `FOREMAN_JOB_RUNTIME_IMAGE` at the
same release); the gc DaemonSet in `40-foreman-gc.yaml` runs the Foreman image
and pulls `Always`. Changing the rsbx carrier means changing
`FOREMAN_JOB_IMAGE` to another verified digest and restarting Foreman —
in-flight attempts keep the digest they froze.

Local builds are for development and staging clusters, never the release path:
`make build-foreman-image` builds one platform's image (`linux/$(ARCH)`, the
host architecture unless `ARCH=` says otherwise) containing the server, the
supervisor, `foreman-gc` and the five reference adapters. What a published
image contains is checked by the design's
acceptance cases; the release record reflects the push command's exit code
only — a non-zero exit does not prove that nothing landed, so check the
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
- `FOREMAN_STATE_GRACE` (default `30s`) — the outage window during which the
  local positive cache still adjudicates. Past it every non-terminal request
  fails closed (401/503). Keep it far below the Job Token TTL: a cached
  adjudication may not outlive the revocation window (§6.1).
- `FOREMAN_STATE_CALL_TIMEOUT` (default `2s`) — the store's own answer
  bound for one operation. A caller may stop waiting earlier, but this bound
  is what decides whether the store answered in time; it never follows a
  caller's deadline (`02-contracts.md` §10 共享状态不可用的判定).
- `FOREMAN_LEADER_LEASE_TTL` (default `10s`) and
  `FOREMAN_LEADER_TAKEOVER_WINDOW` (default `5s`) — the control-plane lease
  and the quiet period the new holder observes before it reconciles and
  claims. The takeover bound is the lease plus this window. The window must
  be at least one renewal tick (a third of the lease): the old holder learns
  it lost the lease on its next renewal, so a shorter window would let it
  keep acting after the new holder serves; such a pair is refused at
  start-up.
- `FOREMAN_INSTANCE_TTL` (default `30s`) — a replica's liveness row; the
  shutdown path uses it to decide whether this replica is the last one.
- `FOREMAN_TASK_LOCK_TTL` (default `120s`) — the per-task write lock's lease.
  It must cover the longest locked action (the claim chain: secret and Job
  create); a dead holder's lock expires after it and the convergence
  round takes the action over.

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

The `home` partition carries the session files a resumed task continues from
(`~/.multica/pi-sessions` and the `~/.omp` runtime state). Agent memory is a
profile-relative store, not a `home` entry, so it starts empty with every task.
**The daemon profile is not part of it.** Each task Job gets its own profile
directory inside that Job's credential volume (the `emptyDir` that also carries
the Job Token), so the profile and everything the provider caches under it
disappear with the pod:

> profile-relative state is per-Job and is not reused across tasks

That sentence is the fixed declaration required by ADR-059: the reference
template prints it in every task Job's startup log (the `prepare` init
container prints it to stdout), and this page states it here. Two consequences
a deployer should plan for:

- every task starts from an **empty profile**; provider-side cold-start cost is
  paid per task, not once per node. A cold start is a startup cost, not a
  failure by itself: it fails the task only if it crosses
  `FOREMAN_JOB_BOOT_TIMEOUT`, or if the profile it writes grows past the
  credential volume's `sizeLimit` (below). Size both bounds against your
  providers;
- two Jobs of the same workspace on one node no longer contend for a profile
  directory: each has its own volume, so the same-node concurrency that
  `TC-tech-cache-02` exercises is available again.

The credential volume is now the profile's bound. The reference template sizes
it `512Mi`, up from `16Mi` while the profile lived on the node: the old bound
carried the token file alone, this one also carries every provider
profile-relative store a task creates at cold start. `512Mi` is a starting
value, not a measurement — the reference deployment has not measured any
provider's cold-start footprint, and that footprint is provider- and
task-dependent. Measure the profile directory of a representative task and set
the bound above it; a pod whose `emptyDir` exceeds `sizeLimit` is evicted, the
Job does not retry it (`backoffLimit: 0`), and the task converges to a failed
terminal. The pod declares no `ephemeral-storage` request or limit, so the
bound is not in the scheduler's account: plan node capacity as `sizeLimit` per
concurrent Job by hand.

A profile directory left on the `home` partition by an earlier release is not
reclaimed as a whole. The collector's only paths into a profile root are the
four profile-anchored store rules: `hermes-sessions`, `dsh-sessions` and
`reasonix-state` under the agent-state retention (`FOREMAN_AGENT_STATE_TTL`),
and `hermes-state` under the memory retention (`FOREMAN_AGENT_MEMORY_TTL`). The
profile directory itself and the rest of its entries — `config.json`,
`daemon.pid`, `daemon.log`, provider configuration caches — are never
reclaimed, so a node that ran the earlier layout keeps them until an operator
removes them by hand. `foreman-gc` never creates directories; it only reclaims.

The DaemonSet's `hostPath.path` and the Deployment's `FOREMAN_STATE_ROOT` must
be changed together; the collector's own view is the mount point, passed back
in as `FOREMAN_STATE_ROOT=/state`.

The upstream daemon also caches resolved skill bundles on that node tree
(`workspaces/.skill-cache`), and its own GC is off in the Job template. The
collector reclaims those entries by retention (`FOREMAN_GC_SKILL_CACHE_TTL`,
default `720h`) and by a per-workspace size cap
(`FOREMAN_GC_SKILL_CACHE_MAX_BYTES`, default `10Gi`, least recently used
first). Both names carry the `GC` prefix to stay distinct from the proxy's
in-process `FOREMAN_SKILL_CACHE_*` settings. A reclaimed entry is a cache
miss, never a task failure; the next resolve re-fetches it.

## Configuration

All keys with their contract defaults are spelled out in the Deployment env.
The ones a staging/test cluster usually overrides:

| Key | Staging value | Why |
|---|---|---|
| `MULTICA_SERVER_URL` | `http://stub:9999` | protocol stub instead of the real server |
| `FOREMAN_MAX_INFLIGHT_JOBS` | `2` | concurrency assertions |
| `FOREMAN_JOB_BOOT_TIMEOUT` | `60s` | boot-timeout assertions |
| `FOREMAN_LOG_LEVEL` | `debug` | heartbeat / WS assertions |
| `FOREMAN_OPS_TOKEN` | a throwaway value | the ops surface answers `401` without it; `-ops-listen=""` disables the surface instead |
| `FOREMAN_POLICY_NAME_RESOLUTION_TIMEOUT` | `2s` | bounds the startup resolution of the policy file's keys (default `5s`); a window that runs out reads as `unavailable` |
| `FOREMAN_PROJECT_LOOKUP_TIMEOUT` | `1s` | bounds one read-only project lookup by issue id (default `2s`), keeping the claim → Job budget |

## Job template (required)

The V2 dispatch path does **not** read this template. When the rules binder
freezes an attempt, the Job's shape comes from the fixed render
(`internal/jobbuilder/fixed.go`): the digest-pinned rsbx carrier, the
supervisor init container and the frozen attempt binding. The template below
remains a required startup artifact because a claim without a frozen binding
(the pre-V2 fallback) still renders from it; a deployment that always binds an
environment never creates that shape. Do not point a legacy template at the
rsbx carrier: it executes the carrier's preinstalled `/usr/local/bin/multica`,
which V2-C08 forbids.

On that legacy path the Job's shape comes from one place: the ConfigMap
`foreman-job-template`, which `25-job-template.yaml` ships and the Deployment mounts at
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
| `<job-image>` | `FOREMAN_JOB_IMAGE` (legacy template path only; the fixed render uses the binding's `image@digest`) |
| `<job-name>` / `<daemon-id>` | the final display name / the identity name `fm-<task_id>` |
| `<agent-name>` | the payload's raw `agent.name` (any characters; only the name face normalizes it) |
| `<preferred node>` | the node holding the resumed session's file (`session_id` of the task's previous run); with no resumed session — a first run — or no known node for it, or `FOREMAN_PREFER_NODE_REUSE=false`, or a `nodeSelector` policy hit, the whole affinity item carrying it is dropped. This literal is the substitution set's **one exception**: it is consumed only where it is *recognized* — a `values` entry of a preferred-affinity `matchExpressions` reading exactly this literal — so anywhere else it stays verbatim, and the apiserver judges that position at Job creation |
| `<issue-segment>` / `<agent-segment>` / `<task-key>` | **only inside `metadata.name`**: normalized (lowercase, non-`[a-z0-9]` runs folded to `-`, trimmed, truncated to 20 / 24 / 12) issue and agent segments, and the task id's dashes-stripped last 12 characters. Anywhere else they stay verbatim |

Unregistered `<...>` literals stay as written, so a template that writes
`image: <job-image>` reaches the apiserver unchanged and the Job never boots;
the reference template has the placeholder in every container it declares
(`agent`, `omp`, `prepare`).

**On the legacy path the template also provides the agent backend.** The rsbx
carrier carries no `omp`, so the shipped reference template's `omp` init container
fetches the architecture's musl build from upstream's newest release, verifies
it against that release's `SHA256SUMS.txt`, and leaves it where the agent
container mounts it as the `MULTICA_OMP_PATH` file. A deployer's own template
does the same job its own way — that is the template's choice, and nothing
checks it. Consequences to size for:

- **the startup cost is per task, not per deployment.** `agent-bin` is the
  pod's `emptyDir` (size limit `1Gi`), not a node or shared volume, so the
  `omp` init container fetches the ≈200 MB asset again on every pod — N tasks
  booting together make N fetches, and the startup probe pays one too.
  Measured on the reference deployment at ≈5 MB/s, the download leg alone is
  ≈40 s, while the whole cold start — schedule, image pull, download,
  registration — is ≈262 s for a task boot (`foreman_job_boot_seconds`) and
  ≈270 s for the startup probe (`daemon.registered` minus `probe.started`).
  The two network legs, the image pull and the agent-backend download, both
  halve in speed with the link, so a link at half the measured rate
  approximately doubles the whole span: ≈540 s for the probe and ≈524 s for
  a task. Size both windows for the whole span, not for the download alone.
  The shipped `FOREMAN_STARTUP_PROBE_TIMEOUT` is `600s` (a missed window
  CrashLoops the deployment), while `FOREMAN_JOB_BOOT_TIMEOUT` stays `300s`
  (a missed bound fails that task with `job_boot_timeout`); raise it together
  with `FOREMAN_JOB_TOKEN_TTL` if your links need the same headroom, since
  the startup self-check refuses a TTL below boot + task + drain. Raising a
  bound is not a substitute for an origin the Job pod cannot download from at
  window scale: give such a deployment a cluster-reachable mirror instead
  (§Agent backend origin);
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

### Agent backend origin (deployer override)

The reference template's origin is a default, not a hard-coded deployment
decision: a deployer points the same template at a cluster-reachable mirror
without forking it. `optional/agent-backend-mirror.sh` reads the shipped
ConfigMap, rewrites exactly the `omp` init step's `base=` line and writes the
resulting ConfigMap:

```bash
# The mirror serves a release download directory's layout:
#   <base>/SHA256SUMS.txt  <base>/omp-linux-musl-x64  <base>/omp-linux-musl-arm64
FOREMAN_AGENT_BACKEND_BASE=http://ompmirror.multica-agents.svc:8080 \
  ./deploy/optional/agent-backend-mirror.sh | kubectl apply -f -
kubectl -n foreman rollout restart deploy/foreman
```

Everything else in the template — the checksum gate, the arch asset choice,
the install path and mode — stays the reference template's bytes. The script
refuses an empty origin, an origin carrying shell syntax, and a template whose
reference line has moved, so a mistyped or stale invocation installs nothing
instead of silently keeping the upstream origin.

- **The shipped file does not change.** `25-job-template.yaml` remains
  byte-identical to the AC-20 baseline; only the ConfigMap a deployment applies
  is rewritten. The override therefore leaves the reference-template window
  (AC-20's byte comparison no longer describes the deployment) — that window is
  the shipped template's, not a promise about a customized one.
- **The mirror is the deployment's own source.** The init step still verifies
  the asset against the manifest from the same base, so a mirror pins the
  release it serves — the rollback anchor the upstream origin lacks — at the
  cost of becoming the deployment's trust anchor (§8.7). Foreman still does not
  review, pin or record either.
- A later `kubectl apply -f deploy/` restores the shipped ConfigMap; re-apply
  the override (and restart Foreman) when that happens.

**Size the origin against the boot window.** The default
`FOREMAN_JOB_BOOT_TIMEOUT` is `300s`; at the 206 KB/s measured from a
slow-egress cluster node's Job pod, that window buys `300s × 206 KB/s ≈ 60 MiB`
of agent backend. The reference asset is 204 MiB — ≈17 min at that rate, 3.4×
the window before schedule, image pull and registration are counted — so the
Job ends `job_boot_timeout` before the init step installs anything. Served from
an in-cluster mirror the same 204 MiB asset was measured at ≈5 s and the Job
booted inside the window. A deployment whose Job pods cannot reach a fast
origin **must** provide a cluster-reachable artifact source with the release
manifest and both architecture assets preloaded, or accept that its tasks fail
at boot; raising the bound only stretches the wait. There is no node-level
cache to warm — `agent-bin` is each pod's `emptyDir`, so every pod still
fetches its own copy (§8.7, ADR-033).

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

- **Upgrade**: create/update the ConfigMap (the shipped reference template, its
  mirror-rendered copy (§Agent backend origin), or the deployer's own) **before**
  rolling out the new Foreman. A Foreman whose template is absent refuses to start
  — and with the ConfigMap object itself missing, kubelet blocks the pod before
  that.
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

- The Job Token reaches the daemon only if the template carries the per-task
  Secret `fm-<task_id>-cred` into the Job and puts the copied `config.json`
  where the daemon reads its profile's configuration (the reference template
  copies it into the profile directory inside that Job's credential volume);
  without one the task fails with `job_auth_failed` — nothing checks this at
  startup.
- On the legacy template path, a template carrying its own `image` removes the
  image audit anchor: `/foreman/version` keeps reporting `FOREMAN_JOB_IMAGE`,
  which is then no longer what the Job runs. The fixed render does not read
  the template's `image` at all.
- `MULTICA_DAEMON_ID` reads the pod-template label `foreman.tsic.top/daemon-id`,
  which Foreman writes with the identity name `fm-<task_id>`. A template that
  sets `manualSelector: true` (or otherwise replaces the Job's selector) makes
  the apiserver reject the Job at creation (`job_create_failed`) rather than
  producing an id-less daemon — the identity no longer depends on the Job
  controller's `job-name` label.

**The agent backend's source is a registered trade-off, not an oversight.** By
default the reference template resolves `omp` from upstream's newest release at
every Job boot and verifies it against that same release's manifest (`ADR-033`),
and Foreman neither reviews, pins nor records the version it receives. A
deployment can repoint the shipped template at its own mirror (§Agent backend
origin) — that moves the source, not the responsibility: the mirror becomes the
deployment's trust anchor and the pin. Either way it accepts:

- **no review and no rollback anchor on the Foreman side** — upstream's newest
  release reaches every Job created afterwards without a deploy change; rolling
  the backend back means editing the template or repointing it at a pinned
  mirror, and either way the deployment leaves the reference-template window,
  which nothing checks (§8.1);
- **a same-origin checksum, not an independent trust anchor** — manifest and
  asset come from the same origin, so verification catches a corrupted download
  but not a release replaced in place; template content is not a security
  boundary (§8.7);
- **`github.com` as a Job-start prerequisite beside the registry** — an
  unreachable origin makes the init container exit non-zero (pod `Failed` →
  task `failed`, `failure_reason=job_failed`), and an origin that stalls spends
  the boot window (`job_boot_timeout`); nothing falls back to an image-carried
  backend. An origin slower than that window's arithmetic is a deployment
  defect, not a tuning problem: provide a cluster-reachable mirror and size the
  two together (§Agent backend origin).

### Catalog probe and model catalog (startup)

Before Foreman registers with Multica Server it runs **one catalog probe Job**:
the same official daemon, rendered from the same template. The probe reports which
runtimes/providers the template environment really has, and that report is what
Foreman declares to the server; it also doubles as the template preflight.

- `FOREMAN_STARTUP_PROBE_TIMEOUT` (default = `FOREMAN_JOB_BOOT_TIMEOUT`; the
  shipped manifest sets `600s`) is the window. **No runtime registers inside
  it ⇒ Foreman exits non-zero**
  (`startup.probe_failed` carries the class: `job_create_failed` /
  `pod_not_started` / `daemon_not_registered` / `discovery_not_landed`). A
  failure to *land the catalog* is an **error-level** `startup.probe_failed`
  with `reason=discovery_not_landed`, but it does **not** stop startup:
  registration (S4) is the only exit criterion. With
  `FOREMAN_MODEL_CATALOG_ENABLED=false` registration alone completes startup and
  no failure event is emitted.
- **Why `600s`, and how to read the cost of a start.** The probe's own init
  pays the same per-pod agent-backend download as a task (§Risk disclosure),
  and it has to finish before the daemon can register; a missed window
  CrashLoops the deployment, so the shipped window is twice the task boot
  bound. On the measured cold path the probe registered 270 s after
  `probe.started` — a ≈40 s agent-backend download plus schedule, image pull
  and registration — and both network legs halve in speed with the link, so
  half the measured throughput approximately doubles that whole span to
  ≈540 s. Window and elapsed time are events, not estimates:
  `probe.started` carries the window as `budget` (nanoseconds, e.g.
  `600000000000` for `600s`) and `daemon.registered` its `ts`, so the two
  lines answer both questions.
- **Template duty**: the probe needs its own credential and profile, so a
  template must express them with the two literals Foreman rewrites —
  profile `foreman-job` → `foreman-catalog`, Secret `fm-<task_id>-cred` →
  `fm-catalog-<request_id>-cred`. The rewrite reaches the profile where the
  template expresses it (each container's command and arguments, and any mount
  path that names the profile) and the Secret where the token reaches that
  directory (the Secret references a pod volume carries). Every other position
  keeps the template's own text — an image reference reading `foreman-job`, the
  pull credential beside it, any env value or label. The reference template
  names the profile in the daemon's start line and in `prepare`'s script, and
  mounts the credential volume at the profile root; the probe's own profile
  directory is created inside the probe's own volume, so the two Jobs stay
  apart at the directory level by construction. A template that does not
  express the two faces means the probe cannot authenticate, so startup exits
  non-zero (visible, not silent).
- **Credential duty**: the probe's Job Token sits in the template-shaped pod and
  the template's own processes can read it — do not add a process to the probe
  that must not hold it (same rule as the task Jobs).
- **Control-plane scratch duty**: before the catalog probe, the control plane
  verifies a published release by materializing it with `os.MkdirTemp("", …)`
  under its own `TMPDIR` (`cmd/foreman/startup_discovery.go`), and the omp
  adapter does the same (`internal/discovery/builtin.go`). The foreman
  container runs with `readOnlyRootFilesystem: true`, so the Deployment must
  set `TMPDIR` and mount a writable, size-limited `emptyDir` there — the
  shipped manifest points it at `/tmp` with a `2Gi` limit, the same provision
  the Job template gives its carrier. Dropping either half fails every bound
  verification with `read-only file system` and leaves the adapter untrusted.
  The control-plane image must also carry the runtime the payload links:
  `images/foreman.Containerfile` installs `libstdc++`, because the bound probe
  executes the same omp binary the Job runs (ADR-033 puts the same requirement
  on the Job image). An image without it writes the artifact and then fails at
  the dynamic loader, so the adapter never reaches `trusted`.
- The advertised model catalog is discovered **at startup, on the single node
  the probe landed on, and replayed from cache** for later server commands
  (`FOREMAN_MODEL_CATALOG_CACHE_TTL`, default `6h`, stale caches still answer
  and only raise a warning). Keep the node pool homogeneous and keep the
  provider configuration under `${FOREMAN_STATE_ROOT}/home`; otherwise the
  directory you advertise describes a different environment than the task Jobs
  run in.
- Operations view: `curl -s -H "Authorization: Bearer $FOREMAN_OPS_TOKEN"
  localhost:8081/foreman/register-source` prints the discovered list, the
  declared runtimes, each runtime's catalog slot state and the current
  runtime-profile replay source (read-only — it never triggers a discovery).
  The answer is cropped to the credential's workspaces.

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
kubectl -n foreman port-forward deploy/foreman 8080:8080 8081:8081 &
curl -s localhost:8080/foreman/healthz            # liveness only, no credential
TOKEN=$(kubectl -n foreman get secret foreman-secret -o jsonpath='{.data.FOREMAN_OPS_TOKEN}' | base64 -d)
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/foreman/register-source
curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/metrics | grep -cE '^foreman_'
```

`/foreman/healthz` is the input of both probes: it answers `503` (and names the
stopped component) once a critical loop — claim, heartbeat, lease, recovery,
WS subscriber, profile-refresh (the last two only with
`FOREMAN_WS_ENABLED=true`) — has stopped. A loop that stops with an error while
the process is still serving is a configuration error (§1.5; today the
credential/identity mismatch `403`): the process logs `component.failed` and
exits non-zero, so the Deployment restarts it visibly instead of running
half-dead (§9.3).
