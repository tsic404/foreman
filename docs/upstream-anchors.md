# Upstream anchors

The design repo's §7 requires every upstream protocol fact Foreman relies on to
be anchored here as **repo + commit** and **file + symbol** — never a line
number, which drifts with the upstream version. A fact whose anchor moves is
re-verified before the next release; an unverified fact is implemented in the
defensive form instead of being relied on.

## Pinned artifact

`github.com/multica-ai/multica@2ea01ae4` — the `v0.6.1` release commit, the same
artifact the test cluster pins and the Job image carries.

## S-12 token usage (contracts §9.2 / ADR-009 decision 4)

Premise: the daemon's S-12 usage report is the wire source of the token
metric. Each entry carries the provider, model and the four token counters;
the optional cost figure is not consumed by Foreman.

| Fact | Anchor |
|---|---|
| The daemon posts `{"usage":[{provider, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd_ticks?}]}` to `POST /api/daemon/tasks/{taskId}/usage` | `server/internal/daemon/client.go` → `(*Client).ReportTaskUsage` |
| The usage entry carries the four token counters; `cost_usd_ticks` is optional | `server/internal/daemon/types.go` → `TaskUsageEntry` |

## Skill-bundle resolve (contracts §1.8 / §7, ADR-045)

Premise: the resolve response is content-addressed — the same reference set
yields the same response body — and the request payload bytes carry no task or
runtime identity, so a cache keyed on those bytes is sound across tasks and
runtimes.

| Fact | Anchor |
|---|---|
| The daemon resolves **one ref per request**, and the body is the ref set alone (`{"skills":[{"id","source","hash"}]}`) — no task, runtime, agent or workspace field | `server/internal/daemon/client.go` → `(*Client).ResolveSkillBundle` |
| The route is `POST /api/daemon/runtimes/{runtimeId}/tasks/{taskId}/skill-bundles/resolve` | `server/cmd/server/router.go` → the daemon route group's `skill-bundles/resolve` registration |
| The response is the bundle set for exactly the requested refs, in request order; a ref's identity is `(source, id)` | `server/internal/handler/daemon.go` → `(*Handler).ResolveTaskSkillBundles`; `server/internal/service/task.go` → `AgentSkillBundleKey` |
| The bundle hash is a deterministic function of the skill's content (sorted files, `v1` prefix) | `server/pkg/skillbundle/hash.go` → `BuildManifest` |
| Refs resolve **inside the task's agent scope**: a ref the agent does not hold is absent and the handler answers 404, never a different body | `server/internal/service/task.go` → `(*TaskService).LoadRequestedAgentSkillBundles` |
| Plugin-source refs are hash-pinned (mismatch ⇒ 409); workspace and builtin sources return the **current** bundle and hash — an edit between claim and resolve is not rejected | `server/internal/handler/daemon.go` → `(*Handler).ResolveTaskSkillBundles` |
| The daemon accepts a bundle whose hash differs from the claim-time ref: it checks source/id and the bundle's self-consistency only, and caches bundles on disk keyed by `(workspace, source, id, hash)` so a dispatch with an unchanged ref is served without the server | `server/internal/daemon/daemon.go` → `(*Daemon).resolveSkillBundle`; `server/internal/daemon/skill_cache.go` → `(*SkillBundleCache).Load`, `validateSkillBundle` |
| The on-disk bundle cache lives under `<workspaces root>/.skill-cache/v1/<workspace>/<source>/<id>/<hash>/bundle.json`; a missing entry is re-fetched on the next resolve, and the daemon's own GC skips that dot directory instead of reclaiming it | `server/internal/daemon/daemon.go` → `New` (skill cache root); `server/internal/daemon/skill_cache.go` → `(*SkillBundleCache).bundlePath`, `(*SkillBundleCache).Load`; `server/internal/daemon/gc.go` → `(*Daemon).runGC` (the dot-directory skip) |

**Scope of the premise.** Content addressing holds per reference set while the
referenced content is unchanged. A workspace skill edited between a task's claim
and its resolve makes the upstream serve the *current* bundle for the same
payload, so a cache hit inside the fresh window replays the bundle that landed
before the edit. That is the version-lag class the upstream daemon already has
(it reuses its on-disk bundle for an unchanged ref across dispatches); it is
bounded by `FOREMAN_SKILL_CACHE_FRESH_TTL`, and a claim taken after the edit
carries the new hash — a different payload, hence a different key. Re-verify this
anchor when the upstream changes the resolve semantics (ADR-045 复审触发).

## Project identity queries (contracts §4.1 / §8.5, ADR-048)

Premise: the platform exposes the two read-only project-identity lookups
ADR-048 relies on — a workspace project name resolved to its opaque project
identifier at startup, and an issue's project identifier read once per claim —
and Foreman can call them with its existing server credential.

| Fact | Anchor |
|---|---|
| The claim payload carries the project identifier at its **top-level** `project_id` (omitted when the task has no project); `project_title` / `project_description` are prompt context and are not the identity | `server/internal/handler/agent.go` → `AgentTaskResponse.ProjectID`, `AgentTaskResponse.ProjectTitle` |
| Listing a workspace's projects is `GET /api/projects?workspace_id=<uuid>`, answering `{"projects":[{id,title,…}],"total":N}`; the human-readable project name is `title` and the opaque identifier is `id` | `server/cmd/server/router.go` → the workspace-scoped `/api/projects` route group; `server/internal/handler/project.go` → `(*Handler).ListProjects`, `ProjectResponse` |
| No server-side project-name filter exists (the list accepts only `status` / `priority`), so a policy key is resolved by exact equality over the returned list: zero matches = absent, more than one = ambiguous | `server/internal/handler/project.go` → `(*Handler).ListProjects` |
| Reading one issue is `GET /api/issues/{id}?workspace_id=<uuid>`; the answer carries `project_id` (nullable — a task without a project), which is the claim-time answer | `server/cmd/server/router.go` → the `GET /api/issues/{id}` registration; `server/internal/handler/issue.go` → `(*Handler).GetIssue`, `IssueResponse.ProjectID` |
| Both routes are user-API routes: they authenticate the bearer token through `middleware.Auth` (PAT / JWT / task token) inside the workspace-membership group. A daemon-only `mdt_` token is not accepted there, so these two queries need the `mul_` PAT form (`MULTICA_TOKEN` accepts both); a rejected call converges on the documented unavailable/failure class, never on a silent empty answer | `server/cmd/server/router.go` → the `middleware.RequireWorkspaceMember` group and its `middleware.Auth`; `server/internal/middleware/auth.go` → `Auth` |
| There is **no** workspace-wide, visibility-independent agent-name lookup: `GET /api/agents` filters private agents by the caller's membership/ownership, so an absent answer is not the platform's authority. Foreman therefore concludes `unavailable` for agent policy keys (the documented degradation: warn, boot continues, agent section still matches by exact name). | `server/internal/handler/agent.go` → `(*Handler).ListAgents`; `server/internal/handler/agent_access.go` → `memberAllowedToViewAgent` |

**Scope of the premise.** The project name resolution above reads the project's
`title`; the design's excluded option was promoting the claim payload's
`project_title` to an identity name, not failing to resolve a configured name
through the platform's project list. Title uniqueness is not promised, which is
why more than one exact match refuses startup as ambiguous (ADR-048 decision 2)
rather than picking one.

## Multi-workspace credential scope and probe registration (contracts §0 / §1.1 / §1.2 / §7, ADR-049)

Premise: the workspace set a credential can act for is answered by C-17, the
daemon has no configuration that narrows its registration scope, and the
cross-workspace answer shapes are the ones ADR-049 decision 4 names. Verified
against the pinned artifact (`multica-ai/multica@2ea01ae4`).

| Fact | Anchor |
|---|---|
| `GET /api/daemon/workspaces` answers `[{id, name}]` by credential scope: a user-scoped PAT/JWT caller gets every workspace the user belongs to; a workspace-scoped daemon token (`mdt_`) gets only its bound workspace. The answer is an eventual-consistency read and is not an admission decision — downstream daemon APIs authorize each request themselves. | `server/internal/handler/daemon_workspace.go` → `(*Handler).ListDaemonWorkspaces`, `DaemonWorkspaceResponse` |
| The daemon has **no** workspace parameter and **no** workspace whitelist: its workspace set comes only from C-17, and the sync loop registers every workspace in the answer. | `server/internal/daemon/daemon.go` → `(*Daemon).syncWorkspacesFromAPI`, `(*Daemon).workspaceSyncLoop`; `server/internal/daemon/client.go` → `(*Client).ListWorkspaces`; `server/internal/daemon/config.go` (`WorkspacesRoot` is a path, not a scope) |
| Cross-workspace authorization answers `404 not found` (not 403): a daemon token whose workspace differs from the target, and a user who is not a member, both fall through `requireDaemonWorkspaceAccess` to a 404. | `server/internal/handler/daemon.go` → `(*Handler).requireDaemonWorkspaceAccess` |
| Registration (`POST /api/daemon/register`) requires `workspace_id` + `daemon_id` and at least one runtime or failed profile; a daemon-token workspace mismatch answers 404, a user-token non-member answers 404, and the register body names its workspace per registration. | `server/internal/handler/daemon.go` → `(*Handler).DaemonRegister`, `DaemonRegisterRequest` |
| Heartbeat (`POST /api/daemon/heartbeat`): an unknown runtime id answers 404 ("runtime not found"); a workspace the credential does not cover answers 404 through the workspace check. | `server/internal/handler/daemon.go` → `(*Handler).DaemonHeartbeat`, `(*Handler).requireDaemonRuntimeAccess` |
| Batch claim (`POST /api/daemon/tasks/claim`): `daemon_id` is required and a daemon-token identity mismatch answers 403; unknown or unauthorized runtime ids are **skipped** (not failed), so a claim naming another workspace's runtime answers 200 with an empty task set. | `server/internal/handler/daemon.go` → `(*Handler).ClaimTasksByRuntime` |
| Deregister (`POST /api/daemon/deregister`): runtimes the credential does not cover are skipped and the request still answers 200. | `server/internal/handler/daemon.go` → `(*Handler).DaemonDeregister`, `(*Handler).verifyDaemonWorkspaceAccess` |
| The register response carries one row per declared runtime; the row is keyed by `(workspace, daemon_id, provider, profile_id)` and the daemon identity is the request's `daemon_id`. | `server/internal/handler/daemon.go` → `(*Handler).DaemonRegister`; `server/pkg/db/generated/*` → the agent-runtime upsert queries |

**Scope of the premise.** These anchors settle the *client* side: which
workspace set a credential may act for, and what a cross-workspace request
answers. The probe-side registration remains Foreman-local by contract (S-4
captures it into the probe session; it is never forwarded upstream), so the
probe's own daemon identity never reaches the server — the server-side runtime
rows come only from Foreman's per-workspace C-1 with the deployment identity.
Re-verify when upstream changes the daemon route group or the C-17 answer shape
(protocol change ⇒ 提案, per 01-scope 变更触发条件).
