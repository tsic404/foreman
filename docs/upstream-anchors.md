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

**Scope of the premise.** Content addressing holds per reference set while the
referenced content is unchanged. A workspace skill edited between a task's claim
and its resolve makes the upstream serve the *current* bundle for the same
payload, so a cache hit inside the fresh window replays the bundle that landed
before the edit. That is the version-lag class the upstream daemon already has
(it reuses its on-disk bundle for an unchanged ref across dispatches); it is
bounded by `FOREMAN_SKILL_CACHE_FRESH_TTL`, and a claim taken after the edit
carries the new hash — a different payload, hence a different key. Re-verify this
anchor when the upstream changes the resolve semantics (ADR-045 复审触发).
