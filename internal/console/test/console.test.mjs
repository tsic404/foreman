// Executable form of 04-architecture.md §9 UI-01 / SYNC-02 / BIND-03 for the
// console. The expectations come from the frozen design anchor; the fixtures
// are the Management surface's documented answers.

import test from "node:test";
import assert from "node:assert/strict";

import { overviewModel, renderOverview } from "../assets/lib/overview.mjs";
import {
  agentKey,
  bindingEditor,
  bindingOutcome,
  bindingPayload,
  renderAgents,
  renderBindingResult,
} from "../assets/lib/agents.mjs";
import {
  directoryAfterSync,
  renderDirectory,
  renderSources,
  renderSyncNotice,
  syncOutcome,
  validateRegistration,
} from "../assets/lib/sources.mjs";
import {
  checkOutcome,
  classBadge,
  definitionForm,
  impactHTML,
  publishForm,
  publishOutcome,
  renderAgentConfigs,
  renderCLIDefinitions,
  renderCLIPage,
  renderDiscovery,
  renderPublishOutcome,
  validateCLISettings,
  validateConfig,
  validateDefinition,
  validateDiscovery,
  validateVersion,
  versionForm,
} from "../assets/lib/cli.mjs";

const OVERVIEW = {
  counts: {
    servers: 2,
    workspaces: 2,
    workspaces_with_foreman_agents: 1,
    agents: 2,
    environments: 0,
    environments_available: false,
  },
  in_progress: {
    available: true,
    agents: 2,
    attempts: 3,
    rows: [
      {
        server_id: "atlas",
        workspace_id: "core",
        agent_id: "dev",
        agent_name: "开发 Agent",
        issue_id: "iss-21",
        issue_identifier: "M-21",
        environment: "build-a",
        status: "running",
        started_at: "2026-11-20T12:00:00Z",
      },
      {
        server_id: "atlas",
        workspace_id: "core",
        agent_id: "dev",
        agent_name: "开发 Agent",
        issue_id: "iss-22",
        issue_identifier: "M-22",
        environment: "build-b",
        status: "running",
        started_at: "2026-11-20T12:05:00Z",
      },
      {
        // Same display name as atlas/core/dev: a different source, so a
        // different agent. Nothing may merge the two.
        server_id: "harbor",
        workspace_id: "core",
        agent_id: "dev",
        agent_name: "开发 Agent",
        issue_id: "iss-31",
        issue_identifier: "H-7",
        environment: "harbor-a",
        status: "running",
        started_at: "2026-11-20T12:09:00Z",
      },
    ],
  },
};

const SOURCES = [
  {
    server_id: "atlas",
    url: "https://atlas.example.test",
    credential_ref: "atlas",
    allowed_workspaces: ["core"],
    auto_sync: true,
    sync_period_s: 900,
    retry_budget: 3,
    connection: "ok",
    sync: "ok",
    stale: false,
    generation: 3,
    workspaces: 1,
    workspaces_with_foreman_agents: 1,
    agents: 2,
    managed_agents: 1,
    last_success_at: "2026-11-20T12:00:00Z",
    next_sync_at: "2026-11-20T12:15:00Z",
    failure_count: 0,
    write_back: { enabled: true },
  },
  {
    server_id: "harbor",
    url: "https://harbor.example.test",
    credential_ref: "harbor",
    allowed_workspaces: ["core"],
    auto_sync: false,
    sync_period_s: 900,
    retry_budget: 3,
    connection: "ok",
    sync: "failed",
    stale: true,
    generation: 2,
    workspaces: 1,
    workspaces_with_foreman_agents: 0,
    agents: 1,
    managed_agents: 0,
    last_error: "upstream read timed out",
    failure_count: 1,
    write_back: { enabled: true },
  },
];

const DIRECTORY = {
  server_id: "atlas",
  generation: 3,
  synced_at: "2026-11-20T12:00:00Z",
  sync: "ok",
  connection: "ok",
  stale: false,
  workspaces: [
    { server_id: "atlas", workspace_id: "core", name: "工程空间", agent_count: 2, foreman_agent_count: 1 },
    { server_id: "atlas", workspace_id: "lab", name: "实验空间", agent_count: 0, foreman_agent_count: 0 },
  ],
  projects: [
    { server_id: "atlas", workspace_id: "core", project_id: "p-1", title: "移动客户端" },
  ],
  agents: [
    {
      server_id: "atlas",
      workspace_id: "core",
      agent_id: "dev",
      name: "开发 Agent",
      runtime_id: "rt-foreman",
      runtime_bound: true,
      management: "foreman",
      tombstoned: false,
      foreman_runtimes: [
        { runtime_id: "rt-foreman", name: "Foreman · Codex", provider: "codex", status: "online" },
      ],
      unmanaged_runtimes: [
        { runtime_id: "rt-outside", name: "原生 daemon", provider: "codex", status: "online" },
      ],
      write_back: { enabled: true },
    },
    {
      server_id: "atlas",
      workspace_id: "core",
      agent_id: "ops",
      name: "运维 Agent",
      runtime_id: "rt-outside",
      runtime_bound: true,
      management: "unmanaged",
      tombstoned: false,
      foreman_runtimes: [],
      unmanaged_runtimes: [
        { runtime_id: "rt-outside", name: "原生 daemon", provider: "codex", status: "online" },
      ],
      write_back: { enabled: false, reason: "the source credential is revoked; binding is read-only" },
    },
  ],
  runtimes: [],
  tombstones: [],
};

// UI-01 ---------------------------------------------------------------------

test("UI-01 renders the four counts and marks an unavailable environment count as unknown", () => {
  const html = renderOverview(OVERVIEW, SOURCES);
  for (const [label, value] of [
    ["已连接 Multica Server", "2"],
    ["已对接 Workspace", "2"],
    ["采用 Foreman 的 Agent", "2"],
  ]) {
    assert.match(html, new RegExp(label), `missing tile ${label}`);
    assert.match(html, new RegExp(`>${value}<`), `tile ${label} does not show ${value}`);
  }
  assert.match(html, /运行环境/, "missing the environment tile");
  assert.match(html, /未知/, "an unavailable environment count must read 未知, not 0");
  assert.doesNotMatch(html, /运行环境[\s\S]{0,200}>0</, "the environment tile shows a fabricated zero");
});

test("UI-01 states the measurement basis of each tile so the numbers can be recomputed", () => {
  const model = overviewModel(OVERVIEW, SOURCES);
  const byKey = Object.fromEntries(model.tiles.map((tile) => [tile.key, tile]));
  assert.match(byKey.servers.caption, /Server ID/);
  assert.match(byKey.workspaces.caption, /\(Server, Workspace\)/);
  assert.match(byKey.agents.caption, /\(Server, Workspace, Agent\)/);
  assert.match(byKey.workspaces.caption, /其中 1 个/, "the workspace tile must break out its Foreman agents");
});

test("UI-01 counts in-flight agents by identity and rows by attempt", () => {
  const model = overviewModel(OVERVIEW, SOURCES);
  assert.equal(model.inProgress.agentCount, 2, "two agents share the name 开发 Agent but are distinct sources");
  assert.equal(model.inProgress.attemptCount, 3, "each attempt keeps its own row");
  const html = renderOverview(OVERVIEW, SOURCES);
  assert.match(html, /2 个 Agent/);
  assert.match(html, /3 条尝试/);
});

test("UI-01 exposes Server, Workspace, Issue and environment on hover, focus and click", () => {
  const html = renderOverview(OVERVIEW, SOURCES);
  const detail = html.match(/id="run-detail-0"[\s\S]*?<\/dl>/);
  assert.ok(detail, "the first attempt has no addressable detail block");
  for (const value of ["atlas", "core", "M-21", "build-a"]) {
    assert.ok(detail[0].includes(value), `the detail block omits ${value}`);
  }
  assert.match(html, /<button[^>]*data-act="run-detail"[^>]*aria-describedby="run-detail-0"/);
  assert.match(html, /class="run-detail"/, "the detail block must be reachable from CSS hover/focus");
});

test("UI-01 removes finished runs from the home page", () => {
  const finished = { ...OVERVIEW, in_progress: { available: true, agents: 0, attempts: 0, rows: [] } };
  const html = renderOverview(finished, SOURCES);
  assert.doesNotMatch(html, /class="run-row"/, "a finished run is still rendered");
  assert.match(html, /当前没有进行中的任务/);
});

test("UI-01 does not turn unavailable run facts into zero", () => {
  const unavailable = {
    counts: OVERVIEW.counts,
    in_progress: { available: false, agents: 0, attempts: 0, rows: [] },
  };
  const html = renderOverview(unavailable, SOURCES);
  assert.match(html, /运行事实不可用/);
  assert.doesNotMatch(html, /0 个 Agent/);
});

test("UI-01 keeps same-named sources and workspaces apart in the source snapshot", () => {
  const html = renderSources(SOURCES);
  assert.match(html, /atlas/);
  assert.match(html, /harbor/);
  assert.match(html, /同步失败/, "a failed source must say so instead of reading healthy");
  assert.doesNotMatch(html, /credential[-_]value|Bearer /, "the source table must not carry a credential");
});

// SYNC-02 -------------------------------------------------------------------

test("SYNC-02 names the generation that stays current when a sync fails", () => {
  const outcome = syncOutcome(502, {
    server_id: "harbor",
    result: "failed",
    generation: 2,
    kept_generation: 2,
    detail: "upstream read timed out",
  });
  assert.notEqual(outcome.tone, "ok", "a failed sync must not use the success tone");
  assert.equal(outcome.keepDirectory, true, "a failed sync must not clear the directory");
  const html = renderSyncNotice(outcome);
  assert.match(html, /第 2 代/);
  assert.match(html, /upstream read timed out/);
});

test("SYNC-02 keeps rendering the previous generation after a failed sync", () => {
  const outcome = syncOutcome(502, {
    server_id: "atlas",
    result: "failed",
    generation: 3,
    kept_generation: 3,
    detail: "truncated page",
  });
  const kept = directoryAfterSync(DIRECTORY, outcome);
  assert.equal(kept, DIRECTORY, "the previous directory object was dropped");
  const html = renderDirectory(kept);
  assert.match(html, /第 3 代/);
  assert.match(html, /工程空间/);
  assert.match(html, /开发 Agent/);
});

test("SYNC-02 shows no objects for a source that has not synced yet", () => {
  const empty = { server_id: "cedar", generation: 0, sync: "never", connection: "unverified", workspaces: [], projects: [], agents: [], runtimes: [], tombstones: [] };
  const html = renderDirectory(empty);
  assert.match(html, /尚未同步/);
  assert.doesNotMatch(html, /开发 Agent/, "the empty view fabricated a row");
});

test("SYNC-02 reports a completed sync with the new generation", () => {
  const outcome = syncOutcome(200, {
    server_id: "atlas",
    result: "ok",
    generation: 4,
    workspaces: 2,
    projects: 1,
    agents: 3,
    runtimes: 2,
  });
  assert.equal(outcome.tone, "ok");
  assert.equal(outcome.refresh, true);
  assert.match(renderSyncNotice(outcome), /第 4 代/);
});

test("SYNC-02 validation refuses a malformed registration before it reaches the service", () => {
  const invalid = validateRegistration({
    server_id: "Bad ID",
    url: "http://inline@atlas.example.test",
    credential_ref: "",
    allowed_workspaces: "core, ",
    auto_sync: true,
    sync_period_s: "5",
    retry_budget: "99",
  });
  for (const field of ["server_id", "url", "credential_ref", "allowed_workspaces", "sync_period_s", "retry_budget"]) {
    assert.ok(invalid.errors[field], `no field error for ${field}`);
  }
  assert.equal(invalid.payload, null);

  const valid = validateRegistration({
    server_id: "atlas",
    url: "https://atlas.example.test",
    credential_ref: "atlas",
    allowed_workspaces: "core, lab",
    auto_sync: true,
    sync_period_s: "900",
    retry_budget: "3",
  });
  assert.deepEqual(valid.errors, {});
  assert.deepEqual(valid.payload, {
    server_id: "atlas",
    url: "https://atlas.example.test",
    credential_ref: "atlas",
    allowed_workspaces: ["core", "lab"],
    auto_sync: true,
    sync_period_s: 900,
    retry_budget: 3,
  });
});

// BIND-03 -------------------------------------------------------------------

test("BIND-03 only applied is success", () => {
  const applied = bindingOutcome(200, { result: "applied" });
  assert.equal(applied.success, true);
  assert.equal(applied.tone, "ok");
  assert.match(renderBindingResult(applied), /class="call ok"/);

  const cases = [
    [409, { result: "conflict", detail: "the upstream binding changed since it was read" }],
    [409, { result: "not_applied", detail: "the upstream answer did not take effect" }],
    [422, { result: "rejected", detail: "refused" }],
    [504, { result: "unknown", detail: "read-back did not settle" }],
    [501, { result: "unsupported", detail: "the credential is revoked" }],
    [200, { result: "not_applied" }],
    [200, { result: "conflict" }],
  ];
  const titles = new Set();
  for (const [status, body] of cases) {
    const outcome = bindingOutcome(status, body);
    assert.equal(outcome.success, false, `${status}/${body.result} was reported as success`);
    assert.notEqual(outcome.tone, "ok", `${status}/${body.result} used the success tone`);
    assert.doesNotMatch(renderBindingResult(outcome), /class="call ok"/, `${status}/${body.result} rendered success`);
    titles.add(outcome.title);
  }
  assert.ok(titles.size >= 5, "the non-applied results do not read distinctly");
});

test("BIND-03 names each upstream status separately", () => {
  assert.match(bindingOutcome(409, { result: "conflict" }).title, /冲突|他人修改/);
  assert.match(bindingOutcome(409, { result: "not_applied" }).title, /未生效/);
  assert.match(bindingOutcome(422, { result: "rejected" }).title, /拒绝/);
  assert.match(bindingOutcome(504, { result: "unknown" }).title, /未知/);
  assert.match(bindingOutcome(501, { result: "unsupported" }).title, /只读|不可写/);
});

test("BIND-03 accepts the server's result spelling", () => {
  assert.equal(bindingOutcome(200, { Result: "applied" }).success, true);
  assert.equal(bindingOutcome(409, { Result: "conflict" }).success, false);
});

test("BIND-03 compares against the value the operator saw", () => {
  const agent = DIRECTORY.agents[0];
  assert.deepEqual(bindingPayload(agent, "rt-outside"), {
    runtime_id: "rt-outside",
    expected_runtime_id: "rt-foreman",
  });
});

test("BIND-03 offers two runtime groups with their exact upstream ids", () => {
  const html = bindingEditor(DIRECTORY.agents[0]);
  assert.match(html, /Foreman 已注册/);
  assert.match(html, /非管理/);
  assert.match(html, /value="rt-foreman"/);
  assert.match(html, /value="rt-outside"/);
});

test("BIND-03 addresses the row and its editor with the same identity", () => {
  const agent = DIRECTORY.agents[0];
  const unescape = (value) => value.replaceAll("&quot;", '"').replaceAll("&#39;", "'").replaceAll("&amp;", "&");
  const row = renderAgents([agent]).match(/data-act="edit-binding" data-id="([^"]*)"/);
  assert.ok(row, "the agent row has no binding action");
  const form = bindingEditor(agent).match(/<form id="binding-form" data-key="([^"]*)"/);
  assert.ok(form, "the binding form carries no identity for its submit");
  assert.equal(unescape(row[1]), agentKey(agent));
  assert.equal(unescape(form[1]), agentKey(agent), "the editor and its row disagree on the agent");
});

test("BIND-03 closes the editor with a reason when the source cannot be written", () => {
  const html = bindingEditor(DIRECTORY.agents[1]);
  assert.match(html, /credential is revoked/);
  assert.doesNotMatch(html, /data-act="submit-binding"/, "a read-only source still renders a submit button");
});

test("BIND-03 shows the management class and keeps the readiness action honest", () => {
  const html = renderAgents(DIRECTORY.agents);
  assert.match(html, /Foreman 托管/);
  assert.match(html, /非管理/);
  assert.match(html, /就绪检查接口尚未提供/, "the page must say the readiness API is missing instead of faking it");
});

test("BIND-03 escapes upstream names before they reach the page", () => {
  const hostile = {
    ...DIRECTORY.agents[0],
    name: '<img src=x onerror="alert(1)">',
  };
  const html = bindingEditor(hostile) + renderAgents([hostile]);
  assert.doesNotMatch(html, /<img src=x/);
  assert.match(html, /&lt;img src=x/);
});

// CLI-06 / CFG-07 / MODEL-09 ------------------------------------------------

const CLI = {
  cli_id: "omp",
  kind: "agent",
  adapter: "omp",
  server_id: "atlas",
  required_platforms: ["linux/amd64", "linux/arm64"],
  minimum_version: "1.0.0",
  current_version: "1.0.0",
  current_generation: 2,
  auto_check: true,
  check_period_s: 900,
  auto_release: false,
  check_generation: 4,
  candidates: ["1.0.0", "1.1.0"],
  releases: [],
  last_check_at: "2026-11-21T07:00:00Z",
};

test("CLI-06 shows separate check and auto-release switches with per-platform inputs", () => {
  const html = renderCLIDefinitions([CLI]);
  assert.match(html, /定时检查/);
  assert.match(html, /自动发布关闭/);
  assert.match(html, /linux\/amd64、linux\/arm64/);
  assert.match(html, /第 2 代/);
  const form = versionForm(CLI);
  assert.match(form, /data-platform="linux\/amd64"/);
  assert.match(form, /data-platform="linux\/arm64"/);
  assert.match(form, /SHA-256/);
});

test("CLI-06 refuses a credentialed artifact URL and duplicate platforms before the request", () => {
  const invalid = validateVersion("1.0.0", [
    { platform: { os: "linux", arch: "amd64" }, url: "https://ops:not-a-real-secret@mirror.test/a", sha256: "a".repeat(64), size: 1 },
    { platform: { os: "linux", arch: "amd64" }, url: "https://mirror.test/a", sha256: "a".repeat(64), size: 1 },
  ]);
  assert.equal(invalid.payload, null);
  assert.ok(invalid.errors.artifacts);
  assert.ok(invalid.errors["url_linux/amd64"]);
  const valid = validateVersion("1.0.0", [
    { platform: { os: "linux", arch: "amd64" }, url: "https://mirror.test/a", sha256: "a".repeat(64), size: 1 },
  ]);
  assert.equal(valid.payload.version, "1.0.0");
  assert.equal(valid.payload.artifacts[0].size, 1);
});

test("CLI-06 keeps a failed publish on the old pointer and a conflict distinct", () => {
  const published = publishOutcome(200, { result: "published", version: "2.0.0", generation: 3 });
  const failed = publishOutcome(422, { result: "failed", version: "2.0.0", kept_version: "1.0.0", reasons: ["linux/arm64: artifact is x86-64, not aarch64"] });
  const conflict = publishOutcome(409, { result: "conflict", version: "2.0.0", kept_version: "1.0.0", reasons: ["candidate set changed"] });
  assert.equal(published.tone, "ok");
  assert.equal(failed.tone, "danger");
  assert.equal(conflict.tone, "warn");
  assert.match(renderPublishOutcome(failed), /旧指针保持 1\.0\.0/);
  assert.match(renderPublishOutcome(conflict), /候选集已变化/);
  assert.notEqual(failed.title, conflict.title);
});

test("CLI-06 refuses a malformed definition before it reaches the service", () => {
  const invalid = validateDefinition({
    cli_id: "Bad ID",
    kind: "agent",
    adapter: "",
    server_id: "atlas",
    required_platforms: "linux/amd64, linux/amd64",
    manifest_url: "http://user:not-a-real-secret@mirror.test/manifest.json",
    check_period_s: "5",
    auto_check: "on",
    auto_release: "off",
  });
  for (const field of ["cli_id", "adapter", "required_platforms", "manifest_url", "check_period_s"]) {
    assert.ok(invalid.errors[field], `no field error for ${field}`);
  }
  assert.equal(invalid.payload, null);
  const valid = validateDefinition({
    cli_id: "omp",
    kind: "agent",
    adapter: "omp",
    server_id: "atlas",
    required_platforms: "linux/amd64, linux/arm64",
    manifest_url: "",
    check_period_s: "900",
    auto_check: "on",
    auto_release: "off",
  });
  assert.deepEqual(valid.errors, {});
  assert.deepEqual(valid.payload.required_platforms, [{ os: "linux", arch: "amd64" }, { os: "linux", arch: "arm64" }]);
  assert.equal(valid.payload.auto_check, true);
  assert.equal(valid.payload.auto_release, false);
  assert.match(definitionForm([{ server_id: "atlas", url: "https://atlas.example.test" }]), /atlas/);
  assert.deepEqual(validateCLISettings({ check_period_s: "0", auto_check: "on", auto_release: "off" }).errors, {
    check_period_s: "检查周期需要 ≥30 秒的整数。",
  });
});

test("V2-C05 never renders an unknown impact as zero", () => {
  const unknown = impactHTML({
    known: false,
    affected_agents: null,
    unknown: ["the source directory is stale: credential revoked"],
    agents: [],
    in_flight: [],
  });
  assert.match(unknown, /影响范围未知/);
  assert.match(unknown, /credential revoked/);
  assert.doesNotMatch(unknown, /影响 0 个/);
  const known = impactHTML({
    known: true,
    candidate_version: "2.0.0",
    current_version: "1.0.0",
    affected_agents: 0,
    agents: [],
    in_flight: [],
  });
  assert.match(known, /影响 0 个 Agent/);
});

test("V2-C05 names the next-start agents and keeps old runs on their frozen version", () => {
  const html = impactHTML({
    known: true,
    cli_id: "omp",
    kind: "agent",
    candidate_version: "2.0.0",
    current_version: "1.0.0",
    affected_agents: 1,
    agents: [
      { agent_id: "dev", agent_name: "开发 Agent", workspace_id: "core", workspace_name: "工程空间", runtime_id: "rt-omp", adapter: "omp", current_version: "1.0.0" },
    ],
    in_flight: [
      { task_id: "task-1", agent_id: "dev", agent_name: "开发 Agent", workspace_id: "core", version: "1.0.0", version_known: true, config_revision: 3, config_revision_known: true, started_at: "2026-11-21T07:00:00Z" },
    ],
  });
  assert.match(html, /开发 Agent/);
  assert.match(html, /工程空间/);
  assert.match(html, /task-1/);
  assert.match(html, /冻结版本/);
  assert.match(html, /1\.0\.0/);
  assert.match(html, /不热更新/);
});

test("V2-C05 keeps missing in-flight facts unknown instead of guessing", () => {
  const html = impactHTML({
    known: true,
    cli_id: "omp",
    kind: "agent",
    candidate_version: "2.0.0",
    current_version: "1.0.0",
    affected_agents: 1,
    agents: [],
    in_flight: [
      { task_id: "task-unknown", agent_id: "dev", workspace_id: "core", version_known: false, config_revision_known: false },
    ],
  });
  assert.match(html, /task-unknown/);
  assert.match(html, /未知/);
  assert.doesNotMatch(html, /无发布版本/);
  assert.doesNotMatch(html, /config_revision/);
});

test("V2-C05 submits the release generation the preview observed", () => {
  const html = publishForm({
    cli_id: "omp",
    kind: "agent",
    check_generation: 4,
    current_generation: 7,
    current_version: "1.0.0",
    candidates: ["1.0.0", "2.0.0"],
  });
  assert.match(html, /data-release-generation="7"/);
  assert.match(html, /data-generation="4"/);
});

test("CFG-07 renders references and revisions but no config content", () => {
  const html = renderAgentConfigs([
    {
      server_id: "atlas",
      workspace_id: "core",
      backend: "omp",
      profile: "primary",
      revision: 2,
      secret_ref: "omp-primary-config",
      file_name: "config.json",
      sha256: "b".repeat(64),
      size: 42,
      updated_at: "2026-11-21T07:00:00Z",
      updated_by: "ops",
    },
  ]);
  assert.match(html, /omp-primary-config/);
  assert.match(html, /第 2 修订/);
  assert.doesNotMatch(html, /fictional-provider-credential/);
  assert.match(html, /不创建真实 Secret/);
});

test("CFG-07 refuses an array, null, malformed JSON and a secret value reference", () => {
  const base = {
    server_id: "atlas",
    workspace_id: "core",
    backend: "omp",
    profile: "primary",
    secret_ref: "omp-primary-config",
    file_name: "config.json",
    config: `{"provider":"omp"}`,
  };
  for (const config of ["[]", "null", "{", ""]) {
    const result = validateConfig({ ...base, config });
    assert.equal(result.payload, null, `config ${config} was accepted`);
    assert.ok(result.errors.config);
  }
  const badRef = validateConfig({ ...base, secret_ref: "Bearer ey-not-a-reference" });
  assert.ok(badRef.errors.secret_ref);
  const valid = validateConfig(base);
  assert.equal(valid.payload.config.provider, "omp");
  assert.equal(valid.payload.secret_ref, "omp-primary-config");
});

test("MODEL-09 keeps non-empty, legal empty, unsupported and failed distinct", () => {
  const labels = ["succeeded_nonempty", "succeeded_empty", "unsupported", "failed"].map((value) => classBadge(value));
  assert.equal(new Set(labels).size, 4, "the four result classes share a label");
  const unsupported = renderDiscovery([], [], {
    filter: {},
    view: {
      available: false,
      class: "unsupported",
      models: [],
      detail: "codex app-server protocol is not verified for this build",
    },
  });
  assert.match(unsupported, /不支持 \/ 需配置/);
  assert.doesNotMatch(unsupported, /0 个模型/);
  const empty = renderDiscovery([], [], {
    filter: {},
    view: { available: true, class: "succeeded_empty", models: [], detail: "" },
  });
  assert.match(empty, /合法空列表/);
  const failed = renderDiscovery([], [], {
    filter: {},
    view: { available: false, class: "failed", models: [], detail: "bad JSON" },
  });
  assert.match(failed, /发现失败/);
  assert.match(failed, /不能当作空成功/);
});

test("MODEL-09 requires an environment and an adapter or CLI", () => {
  const invalid = validateDiscovery({
    server_id: "atlas",
    workspace_id: "core",
    runtime_id: "rt-omp",
    kind: "models",
    cli_id: "",
    adapter: "",
    platform: "linux/amd64",
  });
  assert.ok(invalid.errors.cli_id);
  assert.equal(invalid.payload, null);
  const valid = validateDiscovery({
    server_id: "atlas",
    workspace_id: "core",
    runtime_id: "rt-omp",
    kind: "models",
    cli_id: "omp",
    adapter: "",
    platform: "linux/amd64",
    config_revision: "",
  });
  assert.deepEqual(valid.errors, {});
  assert.equal(valid.payload.cli_id, "omp");
  assert.equal(valid.payload.config_revision, undefined);
});

test("the CLI page renders every management section without a placeholder", () => {
  const html = renderCLIPage({
    definitions: [CLI],
    audit: [],
    configs: [],
    configAudit: [],
    adapters: [{ name: "omp", kinds: ["models"], trusted: true }],
    requests: [],
    models: { filter: {}, view: null },
  });
  for (const section of ["CLI 发布线", "CLI 发布审计", "Agent 配置修订", "配置审计", "发现适配器", "发现请求", "模型目录"]) {
    assert.match(html, new RegExp(section));
  }
  assert.doesNotMatch(html, /由 CLI 与配置交付范围提供/);
});
