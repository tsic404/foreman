// Console controller. The page reads the Management surface under /admin/*
// with an operator-supplied bearer credential; the credential lives in this
// module's memory only (never a URL, cookie or browser storage).

import { attemptDetail, renderOverview } from "./lib/overview.mjs";
import {
  directoryAfterSync,
  renderAudit,
  renderDirectory,
  renderSources,
  renderSyncNotice,
  settingsForm,
  sourceForm,
  syncOutcome,
  validateRegistration,
  validateSettings,
} from "./lib/sources.mjs";
import {
  agentKey,
  bindingEditor,
  bindingOutcome,
  bindingPayload,
  renderAgents,
  renderBindingResult,
} from "./lib/agents.mjs";
import {
  classBadge,
  checkOutcome,
  configForm,
  definitionForm,
  discoveryForm,
  impactHTML,
  projectionForm,
  projectionHTML,
  publishForm,
  publishOutcome,
  renderCLIPage,
  renderPublishOutcome,
  cliSettingsForm,
  validateConfig,
  validateDefinition,
  validateDiscovery,
  validateCLISettings,
  validateVersion,
  versionForm,
} from "./lib/cli.mjs";
import {
  draftRules,
  emptyPreview,
  previewForm,
  previewRequest,
  previewSelection,
  readinessRequest,
  renderPreview,
  renderReadiness,
  renderRuleOutcome,
  renderRules,
  ruleEditor,
  ruleFromForm,
  ruleOutcome,
} from "./lib/rules.mjs";
import { callout, emptyState, esc } from "./lib/ui.mjs";

const PAGES = [
  {
    id: "overview",
    label: "运行总览",
    icon: "⌂",
    area: "OPERATIONS / OVERVIEW",
    title: "运行总览",
    subtitle: "观察已接入的资源，以及此刻正在工作的 Agent。",
  },
  {
    id: "sources",
    label: "Multica 管理",
    icon: "01",
    area: "ADMINISTRATION / SOURCES",
    title: "Multica 管理",
    subtitle: "连接 Server，同步 Workspace、Agent、Project 及 runtime 目录。",
  },
  {
    id: "agents",
    label: "Agent 管理",
    icon: "02",
    area: "ADMINISTRATION / AGENTS",
    title: "Agent 管理",
    subtitle: "按来源查看 Multica Agent，调整它在 Multica 上的 runtime。",
  },
  {
    id: "environments",
    label: "运行环境管理",
    icon: "03",
    area: "ADMINISTRATION / ENVIRONMENTS",
    title: "运行环境管理",
    subtitle: "由运行环境交付范围提供；本次只保留入口位置。",
    placeholder: true,
  },
  {
    id: "rules",
    label: "规则管理与预览",
    icon: "04",
    area: "ADMINISTRATION / RULES",
    title: "规则管理与预览",
    subtitle: "按 Server、Workspace、Agent 与 Project 选择远端环境；预览不占槽，也不改变 runtime。",
  },
  {
    id: "cli",
    label: "Agent CLI 与配置",
    icon: "05",
    area: "ADMINISTRATION / CLI",
    title: "Agent CLI 与配置",
    subtitle: "分平台发布 CLI，按作用域管理 Agent 配置修订，并在无业务 Job 时执行受限发现。",
  },
  {
    id: "audit",
    label: "日志与审计",
    icon: "06",
    area: "ADMINISTRATION / AUDIT",
    title: "日志与审计",
    subtitle: "按来源读取登记、同步与绑定写回的审计记录。",
  },
];

const state = {
  token: "",
  page: "overview",
  sources: [],
  overview: null,
  directory: null,
  syncNotice: null,
  pageNotice: "",
  renderedNotice: "", // the pending pageNotice consumed by the current render
  agents: [],
  agentFilter: { server: "", workspace: "" },
  agentDirectory: null,
  staleAgents: false,
  rules: {
    view: null,
    directory: null,
    filter: { server: "", workspace: "" },
    preview: null,
    previewValues: {},
  },
  cli: { definitions: [], audit: [] },
  configs: { rows: [], audit: [] },
  discovery: {
    adapters: [],
    requests: [],
    models: { view: null },
    filter: { server_id: "", workspace_id: "", runtime_id: "", profile: "" },
  },
};

const $ = (id) => document.getElementById(id);

class AuthRequired extends Error {}

function pageByID(id) {
  return PAGES.find((page) => page.id === id) ?? PAGES[0];
}

function currentPage() {
  const id = (location.hash || "#/overview").replace(/^#\/?/, "");
  return pageByID(id);
}

async function api(path, options = {}) {
  if (!state.token) {
    throw new AuthRequired();
  }
  const headers = { Accept: "application/json", Authorization: `Bearer ${state.token}` };
  let body;
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(options.body);
  }
  const response = await fetch(path, {
    method: options.method ?? "GET",
    headers,
    body,
    cache: "no-store",
    credentials: "omit",
  });
  let payload = null;
  if ((response.headers.get("content-type") ?? "").includes("json")) {
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
  }
  if (response.status === 401) {
    state.token = "";
    renderCredentialState();
    throw new AuthRequired();
  }
  return { status: response.status, body: payload };
}

function errorText(status, body) {
  const detail = body?.error || body?.detail || body?.Detail;
  return detail ? `HTTP ${status}：${detail}` : `HTTP ${status}`;
}

function failure(title, detail, action = "") {
  return `<div class="call danger" role="alert"><strong>${esc(title)}</strong><div>${esc(detail)}</div>${action ? `<div class="row">${action}</div>` : ""}</div>`;
}

function renderCredentialState() {
  const badge = $("credential-state");
  if (state.token) {
    badge.className = "badge ok";
    badge.textContent = "已连接";
  } else {
    badge.className = "badge gray";
    badge.textContent = "未连接";
  }
}

function renderNav() {
  const button = (page) => {
    const current = page.id === state.page ? 'aria-current="page"' : "";
    return `<button type="button" data-act="go" data-id="${page.id}" ${current}><i>${page.icon}</i>${esc(page.label)}</button>`;
  };
  $("navs").innerHTML = `<div class="group">工作台</div>${button(PAGES[0])}<div class="group">管理中心</div>${PAGES.slice(1).map(button).join("")}`;
}

function renderChrome(page) {
  $("title").textContent = page.title;
  $("subtitle").textContent = page.subtitle;
  $("area").textContent = page.area;
  $("crumb").textContent = `Foreman / ${page.title}`;
  $("scope").innerHTML = "";
}

function authPrompt() {
  return `<div class="panel"><div class="between"><h2>连接 Management surface</h2><span class="badge gray">需要 bearer 凭据</span></div>
    <p class="sub">这个页面读取本部署 <code>/admin/*</code> 的管理接口。输入运维 bearer 凭据后开始；凭据只保存在本页内存，刷新即失效，不写入 URL、Cookie 或浏览器存储。</p>
    <div class="row"><button type="button" class="primary" data-act="connect">输入凭据</button></div></div>`;
}

function notice(message) {
  $("notice").innerHTML = message;
}

async function renderOverviewPage(root) {
  root.innerHTML = emptyState("读取总览…");
  const overview = await api("/admin/overview");
  if (overview.status !== 200) {
    throw new Error(errorText(overview.status, overview.body));
  }
  state.overview = overview.body;
  root.innerHTML = (state.renderedNotice || "") + renderOverview(state.overview);
}

async function loadDirectory(serverID, workspaceID = "") {
  const query = workspaceID ? `?workspace_id=${encodeURIComponent(workspaceID)}` : "";
  const response = await api(`/admin/servers/${encodeURIComponent(serverID)}/directory${query}`);
  if (response.status === 200) {
    state.directory = { serverId: serverID, workspaceId: workspaceID, data: response.body };
    return true;
  }
  return false;
}

async function renderSourcesPage(root) {
  root.innerHTML = emptyState("读取来源…");
  const sources = await api("/admin/servers");
  if (sources.status !== 200) {
    throw new Error(errorText(sources.status, sources.body));
  }
  state.sources = Array.isArray(sources.body) ? sources.body : [];
  let html = state.renderedNotice || "";
  if (state.syncNotice) {
    html += renderSyncNotice(state.syncNotice);
  }
  html += renderSources(state.sources);
  if (state.directory) {
    html += renderDirectory(state.directory.data);
  }
  const audit = await api("/admin/audit");
  if (audit.status === 200) {
    html += renderAudit(audit.body?.entries ?? []);
  }
  root.innerHTML = html;
}

function agentFiltersHTML() {
  const servers = state.sources
    .map((source) => `<option value="${esc(source.server_id)}" ${source.server_id === state.agentFilter.server ? "selected" : ""}>${esc(source.server_id)} · ${esc(source.url)}</option>`)
    .join("");
  const workspaces = (state.agentDirectory?.workspaces ?? [])
    .map((workspace) => `<option value="${esc(workspace.workspace_id)}" ${workspace.workspace_id === state.agentFilter.workspace ? "selected" : ""}>${esc(workspace.name)} / ${esc(workspace.workspace_id)}</option>`)
    .join("");
  return `<div class="scope">
    <label>Multica Server<select data-act="agent-filter" data-field="server"><option value="">全部可见来源</option>${servers}</select></label>
    <label>Workspace<select data-act="agent-filter" data-field="workspace"><option value="">全部 Workspace</option>${workspaces}</select></label>
  </div>`;
}

async function renderAgentsPage(root) {
  root.innerHTML = emptyState("读取 Agent…");
  if (state.sources.length === 0) {
    const sources = await api("/admin/servers");
    if (sources.status !== 200) {
      throw new Error(errorText(sources.status, sources.body));
    }
    state.sources = Array.isArray(sources.body) ? sources.body : [];
  }
  if (!state.agentFilter.server && state.sources.length > 0) {
    state.agentFilter.server = state.sources[0].server_id;
  }
  if (state.agentFilter.server) {
    const directory = await api(`/admin/servers/${encodeURIComponent(state.agentFilter.server)}/directory`);
    state.agentDirectory = directory.status === 200 ? directory.body : null;
    if (directory.status === 404) {
      throw new Error("该来源不在授权范围，或已不存在。");
    }
  }
  const query = new URLSearchParams();
  if (state.agentFilter.server) query.set("server_id", state.agentFilter.server);
  if (state.agentFilter.workspace) query.set("workspace_id", state.agentFilter.workspace);
  const agents = await api(`/admin/agents?${query.toString()}`);
  if (agents.status !== 200) {
    throw new Error(errorText(agents.status, agents.body));
  }
  state.agents = Array.isArray(agents.body) ? agents.body : [];
  root.innerHTML = (state.renderedNotice || "") + agentFiltersHTML() + renderAgents(state.agents);
}

function rulesFiltersHTML() {
  const servers = state.sources
    .map((source) => `<option value="${esc(source.server_id)}" ${source.server_id === state.rules.filter.server ? "selected" : ""}>${esc(source.server_id)} · ${esc(source.url)}</option>`)
    .join("");
  const workspaces = (state.rules.directory?.workspaces ?? [])
    .map((workspace) => `<option value="${esc(workspace.workspace_id)}" ${workspace.workspace_id === state.rules.filter.workspace ? "selected" : ""}>${esc(workspace.name)} / ${esc(workspace.workspace_id)}</option>`)
    .join("");
  return `<div class="scope">
    <label>Multica Server<select data-act="rule-filter" data-field="server"><option value="">全部可见来源</option>${servers}</select></label>
    <label>Workspace<select data-act="rule-filter" data-field="workspace"><option value="">全部 Workspace</option>${workspaces}</select></label>
  </div>`;
}

function rulesContext() {
  return {
    serverID: state.rules.filter.server,
    workspaceID: state.rules.filter.workspace,
    agents: state.rules.directory?.agents ?? [],
  };
}

async function loadRuleDirectory(serverID) {
  if (!serverID) {
    state.rules.directory = null;
    return;
  }
  const response = await api(`/admin/servers/${encodeURIComponent(serverID)}/directory`);
  if (response.status === 200) {
    state.rules.directory = response.body;
    return;
  }
  state.rules.directory = null;
  if (response.status === 404) {
    throw new Error("该来源不在授权范围，或目录不可读。");
  }
}

async function renderRulesPage(root) {
  root.innerHTML = emptyState("读取规则…");
  await ensureSources();
  if (!state.rules.filter.server && state.sources.length > 0) {
    state.rules.filter.server = state.sources[0].server_id;
  }
  if (state.rules.filter.server) {
    await loadRuleDirectory(state.rules.filter.server);
  }
  const workspaces = state.rules.directory?.workspaces ?? [];
  if (workspaces.length > 0 && !workspaces.some((workspace) => workspace.workspace_id === state.rules.filter.workspace)) {
    state.rules.filter.workspace = workspaces[0].workspace_id;
  }
  const response = await api("/admin/rules");
  if (response.status !== 200) {
    throw new Error(errorText(response.status, response.body));
  }
  state.rules.view = response.body;
  const context = rulesContext();
  const preview = state.rules.preview ? renderPreview(state.rules.preview, context) : emptyPreview(context);
  root.innerHTML =
    (state.renderedNotice || "") +
    rulesFiltersHTML() +
    renderRules(state.rules.view, context) +
    `<section class="panel"><div class="between"><h2>任务预览 · ${esc(context.serverID)} / ${esc(context.workspaceID)}</h2></div>${previewForm(context, state.rules.previewValues)}</section>` +
    preview;
}

async function renderAuditPage(root) {
  root.innerHTML = emptyState("读取审计…");
  const audit = await api("/admin/audit");
  if (audit.status !== 200) {
    throw new Error(errorText(audit.status, audit.body));
  }
  root.innerHTML = (state.renderedNotice || "") + renderAudit(audit.body?.entries ?? []);
}

async function ensureSources() {
  if (state.sources.length > 0) {
    return;
  }
  const sources = await api("/admin/servers");
  if (sources.status !== 200) {
    throw new Error(errorText(sources.status, sources.body));
  }
  state.sources = Array.isArray(sources.body) ? sources.body : [];
}

async function renderCLIPagePage(root) {
  root.innerHTML = emptyState("读取 CLI、配置与发现…");
  await ensureSources();
  const [clis, cliAudit, configs, configAudit, adapters, requests] = await Promise.all([
    api("/admin/cli"),
    api("/admin/cli-audit"),
    api("/admin/agent-configs"),
    api("/admin/agent-configs/audit"),
    api("/admin/discovery/adapters"),
    api("/admin/discovery/requests"),
  ]);
  for (const [label, response] of [
    ["CLI", clis],
    ["CLI 审计", cliAudit],
    ["Agent 配置", configs],
    ["配置审计", configAudit],
    ["发现适配器", adapters],
    ["发现请求", requests],
  ]) {
    if (response.status !== 200) {
      throw new Error(`${label}：${errorText(response.status, response.body)}`);
    }
  }
  state.cli = {
    definitions: Array.isArray(clis.body) ? clis.body : [],
    audit: Array.isArray(cliAudit.body) ? cliAudit.body : [],
  };
  state.configs = {
    rows: Array.isArray(configs.body) ? configs.body : [],
    audit: Array.isArray(configAudit.body) ? configAudit.body : [],
  };
  state.discovery.adapters = Array.isArray(adapters.body) ? adapters.body : [];
  state.discovery.requests = Array.isArray(requests.body) ? requests.body : [];
  root.innerHTML =
    (state.renderedNotice || "") +
    renderCLIPage({
      definitions: state.cli.definitions,
      audit: state.cli.audit,
      configs: state.configs.rows,
      configAudit: state.configs.audit,
      adapters: state.discovery.adapters,
      requests: state.discovery.requests,
      models: { view: state.discovery.models.view, filter: state.discovery.filter },
      sources: state.sources,
    });
}

function findCLI(id) {
  return state.cli.definitions.find((definition) => definition.cli_id === id);
}

function configKey(config) {
  return JSON.stringify([config.server_id, config.workspace_id, config.backend, config.profile ?? ""]);
}

function findConfig(key) {
  return state.configs.rows.find((config) => configKey(config) === key);
}

function formValues(form) {
  return Object.fromEntries(new FormData(form));
}

function showFormErrors(form, errors) {
  form.insertAdjacentHTML("beforebegin", fieldErrors(errors));
  markInvalid(form, errors);
}

function dialogHead(title) {
  return `<div class="between"><h2>${esc(title)}</h2><button type="button" data-act="close-dialog">关闭</button></div>`;
}

async function fetchImpact(cliID, version, generation, releaseGeneration) {
  const query = new URLSearchParams();
  if (version) query.set("version", version);
  if (generation !== undefined && generation !== null && generation !== "") {
    query.set("generation", String(generation));
  }
  if (releaseGeneration !== undefined && releaseGeneration !== null && releaseGeneration !== "") {
    query.set("release_generation", String(releaseGeneration));
  }
  return api(`/admin/cli/${encodeURIComponent(cliID)}/impact?${query.toString()}`);
}

async function render() {
  const page = currentPage();
  state.page = page.id;
  // A notice set by the last action belongs to this render only: the next one
  // (a filter change, a navigation) must not repeat it.
  state.renderedNotice = state.pageNotice;
  state.pageNotice = "";
  hideTip();
  renderNav();
  renderChrome(page);
  notice("");
  const root = $("content");
  if (page.placeholder) {
    root.innerHTML = callout("入口位置已保留", "该管理面由后续交付提供；这里的页面不伪造它尚未提供的操作或数据。", "muted");
    return;
  }
  if (!state.token) {
    root.innerHTML = authPrompt();
    return;
  }
  root.setAttribute("aria-busy", "true");
  try {
    if (page.id === "overview") {
      await renderOverviewPage(root);
    } else if (page.id === "sources") {
      await renderSourcesPage(root);
    } else if (page.id === "agents") {
      await renderAgentsPage(root);
    } else if (page.id === "rules") {
      await renderRulesPage(root);
    } else if (page.id === "cli") {
      await renderCLIPagePage(root);
    } else {
      await renderAuditPage(root);
    }
  } catch (error) {
    if (error instanceof AuthRequired) {
      root.innerHTML = authPrompt();
    } else {
      root.innerHTML = failure("读取失败", error.message || String(error), '<button type="button" data-act="retry">重试</button>');
    }
  } finally {
    root.removeAttribute("aria-busy");
  }
}

function openDialog(html) {
  $("dialog-body").innerHTML = html;
  const dialog = $("dialog");
  if (!dialog.open) {
    dialog.showModal();
  }
}

function closeDialog() {
  const dialog = $("dialog");
  if (dialog.open) {
    dialog.close();
  }
}

function credentialDialog() {
  openDialog(`<div class="between"><h2>连接 Management surface</h2><button type="button" data-act="close-dialog">关闭</button></div>
    <p class="sub">输入本部署的 ops bearer 凭据。它只保存在本页内存，刷新即失效；页面不会把它写进 URL、Cookie 或浏览器存储。</p>
    <form id="credential-form" class="fields">
      <label>Bearer 凭据<input name="token" type="password" required autocomplete="off" spellcheck="false"></label>
      <div class="form-actions"><button type="submit" class="primary">连接</button><button type="button" data-act="close-dialog">取消</button></div>
    </form>
    <p id="credential-error" class="error" role="alert"></p>`);
}

function fieldErrors(errors) {
  return `<div class="call danger" role="alert"><strong>请先修正表单</strong><ul>${Object.values(errors).map((message) => `<li>${esc(message)}</li>`).join("")}</ul></div>`;
}

function markInvalid(form, errors) {
  for (const name of Object.keys(errors)) {
    const input = form.querySelector(`[name="${name}"]`);
    if (input) {
      input.setAttribute("aria-invalid", "true");
    }
  }
}

async function submitCredential(form) {
  const value = String(new FormData(form).get("token") ?? "").trim();
  const previous = state.token;
  state.token = value;
  try {
    const response = await api("/admin/servers");
    if (response.status !== 200) {
      throw new Error(errorText(response.status, response.body));
    }
    renderCredentialState();
    closeDialog();
    state.pageNotice = callout("已连接", "管理接口在当前凭据的授权范围内可用。");
    await render();
  } catch (error) {
    state.token = previous;
    renderCredentialState();
    $("credential-error").textContent = error instanceof AuthRequired ? "凭据未被接受（401）。" : `连接失败：${error.message}`;
  }
}

async function submitSource(form) {
  const values = Object.fromEntries(new FormData(form));
  const { errors, payload } = validateRegistration(values);
  if (payload === null) {
    form.insertAdjacentHTML("beforebegin", fieldErrors(errors));
    markInvalid(form, errors);
    return;
  }
  const response = await api("/admin/servers", { method: "POST", body: payload });
  if (response.status === 201) {
    closeDialog();
    state.pageNotice = callout("已登记来源", `${esc(payload.server_id)} 已保存；执行一次手动同步后，下游才会出现它的目录对象。`);
    await render();
    return;
  }
  const message = response.status === 409 ? "该 Server ID 已登记。" : errorText(response.status, response.body);
  form.insertAdjacentHTML("beforebegin", failure("登记未成功", message));
}

async function submitSettings(form) {
  const values = Object.fromEntries(new FormData(form));
  const { errors, payload } = validateSettings(values);
  if (payload === null) {
    form.insertAdjacentHTML("beforebegin", fieldErrors(errors));
    markInvalid(form, errors);
    return;
  }
  const serverID = form.dataset.id;
  const response = await api(`/admin/servers/${encodeURIComponent(serverID)}`, { method: "PATCH", body: payload });
  if (response.status === 200) {
    closeDialog();
    state.pageNotice = callout("设置已保存", `${esc(serverID)} 的自动同步设置已更新。`);
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", failure("设置未保存", errorText(response.status, response.body)));
}

function findAgent(key) {
  return state.agents.find((agent) => agentKey(agent) === key);
}

async function submitBinding(form) {
  const key = form.dataset.key;
  const agent = findAgent(key);
  const box = $("binding-editor");
  if (!agent || !box) {
    return;
  }
  const runtimeID = String(new FormData(form).get("runtime") ?? "");
  const payload = bindingPayload(agent, runtimeID);
  const response = await api(
    `/admin/servers/${encodeURIComponent(agent.server_id)}/workspaces/${encodeURIComponent(agent.workspace_id)}/agents/${encodeURIComponent(agent.agent_id)}/binding`,
    { method: "PUT", body: payload },
  );
  const outcome = bindingOutcome(response.status, response.body);
  box.querySelector("#binding-result")?.remove();
  form.insertAdjacentHTML("beforebegin", `<div id="binding-result">${renderBindingResult(outcome)}</div>`);
  for (const control of form.querySelectorAll("button, select")) {
    control.disabled = true;
  }
  // The service projects the read-back value either way; refresh the table
  // behind the dialog so the next view is the upstream truth, not the intent.
  state.staleAgents = true;
  if (outcome.success) {
    state.pageNotice = renderBindingResult(outcome);
  }
}

function ruleDraftList() {
  const view = state.rules.view;
  if (!view) {
    return [];
  }
  return draftRules(view);
}

function openRuleEditor(rule = {}) {
  openDialog(dialogHead(rule.id ? `编辑规则 ${rule.id}` : "添加环境规则") + ruleEditor(rule));
}

function openReadinessDialog(key) {
  const agent = findAgent(key);
  if (!agent) {
    return;
  }
  openDialog(
    dialogHead(`按 Issue 检查就绪 · ${agent.name}`) +
      `<p class="sub">${esc(agent.server_id)} / ${esc(agent.workspace_id)} / ${esc(agent.agent_id)}。检查只读：不认领、不注册、不下载、不占槽，也不改变绑定。</p>` +
      `<form id="readiness-form" class="fields" data-key="${esc(key)}">
        <label>Issue ID<input name="issue_id" required placeholder="issue-1" autocomplete="off"></label>
        <div class="form-actions"><button type="submit" class="primary">运行就绪检查</button><button type="button" data-act="close-dialog">取消</button></div>
        <p class="hint">Project 由 Issue 解析。结论分为当前可接单、配置阻断、暂时满载、状态未知和非托管；每一项都会给出证据时点和修复入口。</p>
      </form>
      <div id="readiness-result"></div>`,
  );
}

async function submitRuleDraft(form) {
  const values = formValues(form);
  const rule = ruleFromForm(values);
  if (rule.id === "") {
    showFormErrors(form, { id: "规则 ID 不能为空。" });
    return;
  }
  const existingID = form.dataset.id ?? "";
  const rules = ruleDraftList();
  const index = existingID === "" ? -1 : rules.findIndex((item) => item.id === existingID);
  if (rules.some((item, position) => item.id === rule.id && position !== index)) {
    showFormErrors(form, { id: "同一草稿里已经存在这个规则 ID。" });
    return;
  }
  if (index >= 0) {
    rules[index] = rule;
  } else {
    rules.push(rule);
  }
  const response = await saveRuleDraft(rules);
  if (response.status === 200) {
    state.rules.view = response.body;
    state.rules.preview = null;
    closeDialog();
    state.pageNotice = callout("草稿已保存", "发布前仍会重新校验；同级冲突或悬空引用会拒绝发布，已发布版本保持不变。");
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", renderRuleOutcome(ruleOutcome(response.status, response.body)));
}

async function saveRuleDraft(rules) {
  const revision = Number(state.rules.view?.revision ?? 0);
  return api("/admin/rules/draft", { method: "PUT", body: { base_revision: revision, rules } });
}

async function removeRule(id) {
  const rules = ruleDraftList().filter((rule) => rule.id !== id);
  const response = await saveRuleDraft(rules);
  if (response.status === 200) {
    state.rules.view = response.body;
    state.rules.preview = null;
    state.pageNotice = callout("草稿已更新", `规则 ${id} 已从草稿移除；发布后才会影响新任务。`);
  } else {
    state.pageNotice = renderRuleOutcome(ruleOutcome(response.status, response.body));
  }
  await render();
}

async function reloadRules() {
  const response = await api("/admin/rules");
  if (response.status === 200) {
    state.rules.view = response.body;
  }
}

async function publishRules() {
  const revision = Number(state.rules.view?.revision ?? 0);
  const response = await api("/admin/rules/publish", { method: "POST", body: { base_revision: revision } });
  if (response.status === 200) {
    state.rules.preview = null;
    state.pageNotice = callout("规则已发布", `新修订 r${Number(response.body?.revision ?? 0)}；回滚不改在途快照。`);
    await reloadRules();
  } else {
    state.pageNotice = renderRuleOutcome(ruleOutcome(response.status, response.body));
    await reloadRules();
  }
  await render();
}

async function rollbackRules(revision) {
  const base = Number(state.rules.view?.revision ?? 0);
  const response = await api("/admin/rules/rollback", {
    method: "POST",
    body: { revision: Number(revision), base_revision: base },
  });
  if (response.status === 200) {
    state.rules.preview = null;
    state.pageNotice = callout("已回滚", `历史修订 r${revision} 已重新校验并发布为 r${Number(response.body?.revision ?? 0)}。`);
    await reloadRules();
  } else {
    state.pageNotice = renderRuleOutcome(ruleOutcome(response.status, response.body));
    await reloadRules();
  }
  await render();
}

async function discardRules() {
  const published = state.rules.view?.published ?? [];
  const response = await saveRuleDraft(published);
  if (response.status === 200) {
    state.rules.view = response.body;
    state.rules.preview = null;
    state.pageNotice = callout("草稿已重置", "草稿内容恢复为当前已发布版本；发布修订没有变化。");
  } else {
    state.pageNotice = renderRuleOutcome(ruleOutcome(response.status, response.body));
  }
  await render();
}

async function submitRulePreview(form) {
  const context = rulesContext();
  const values = formValues(form);
  const selection = previewSelection(context, values);
  const payload = previewRequest({
    ...values,
    server_id: selection.server_id,
    workspace_id: selection.workspace_id,
    agent_id: selection.agent_id,
  });
  if (!payload.issue_id || !payload.agent_id) {
    showFormErrors(form, {
      ...(payload.issue_id ? {} : { issue_id: "需要 Issue ID。" }),
      ...(payload.agent_id ? {} : { agent_id: "需要选择一个 Agent。" }),
    });
    return;
  }
  const response = await api("/admin/rules/preview", { method: "POST", body: payload });
  if (response.status === 200) {
    state.rules.preview = response.body;
    state.rules.previewValues = values;
    state.pageNotice = callout("预览完成", "这是只读快照；实际派发会重新解析并原子预留。");
  } else {
    state.pageNotice = failure("预览未完成", errorText(response.status, response.body));
  }
  await render();
}

async function submitReadiness(form) {
  const agent = findAgent(form.dataset.key);
  if (!agent) {
    return;
  }
  const values = formValues(form);
  if (String(values.issue_id ?? "").trim() === "") {
    showFormErrors(form, { issue_id: "需要 Issue ID。" });
    return;
  }
  const payload = readinessRequest({
    ...values,
    server_id: agent.server_id,
    workspace_id: agent.workspace_id,
    agent_id: agent.agent_id,
  });
  const response = await api("/admin/readiness", { method: "POST", body: payload });
  const box = $("readiness-result");
  if (!box) {
    return;
  }
  if (response.status === 200) {
    box.innerHTML = renderReadiness(response.body, {
      serverID: agent.server_id,
      workspaceID: agent.workspace_id,
      agentID: agent.agent_id,
    });
  } else {
    box.innerHTML = failure("就绪检查未完成", errorText(response.status, response.body));
  }
}

async function doSync(serverID) {
  notice(callout("正在同步", `${esc(serverID)}：读取完整目录后才替换快照。`));
  const response = await api(`/admin/servers/${encodeURIComponent(serverID)}/sync`, { method: "POST" });
  const outcome = syncOutcome(response.status, response.body);
  state.syncNotice = outcome;
  if (outcome.refresh) {
    if (state.directory?.serverId === serverID) {
      await loadDirectory(serverID, state.directory.workspaceId);
    }
  } else {
    // A failed sync keeps the directory the page already shows (SYNC-02).
    state.directory = directoryAfterSync(state.directory, outcome);
  }
  await render();
}

async function toggleAuto(serverID) {
  const source = state.sources.find((item) => item.server_id === serverID);
  if (!source) {
    return;
  }
  const response = await api(`/admin/servers/${encodeURIComponent(serverID)}`, {
    method: "PATCH",
    body: { auto_sync: !source.auto_sync },
  });
  if (response.status === 200) {
    state.pageNotice = callout("已更新", `自动同步${source.auto_sync ? "已关闭" : "已开启"}；已有绑定与手动同步不受影响。`);
  } else {
    state.pageNotice = failure("更新未成功", errorText(response.status, response.body));
  }
  await render();
}

function workspaceFilterDialog(serverID) {
  const directory = state.directory?.serverId === serverID ? state.directory.data : null;
  const options = (directory?.workspaces ?? [])
    .map((workspace) => `<option value="${esc(workspace.workspace_id)}">${esc(workspace.name)} / ${esc(workspace.workspace_id)}</option>`)
    .join("");
  openDialog(`<div class="between"><h2>按 Workspace 过滤</h2><button type="button" data-act="close-dialog">关闭</button></div>
    <form id="workspace-filter-form" class="fields" data-id="${esc(serverID)}">
      <label>Workspace<select name="workspace_id"><option value="">全部 Workspace</option>${options}</select></label>
      <div class="form-actions"><button type="submit" class="primary">应用</button><button type="button" data-act="close-dialog">取消</button></div>
    </form>`);
}

function openBindingEditor(key) {
  const agent = findAgent(key);
  if (!agent) {
    return;
  }
  openDialog(bindingEditor(agent));
}

function openCLIDefinitionDialog() {
  openDialog(dialogHead("登记 CLI") + definitionForm(state.sources));
}

function openCLISettingsDialog(definition) {
  if (!definition) {
    return;
  }
  openDialog(dialogHead(`发布设置 · ${definition.cli_id}`) + cliSettingsForm(definition));
}

function openCLIVersionDialog(definition) {
  if (!definition) {
    return;
  }
  openDialog(dialogHead(`登记候选版本 · ${definition.cli_id}`) + versionForm(definition));
}

function openCLIPublishDialog(definition) {
  if (!definition) {
    return;
  }
  openDialog(dialogHead(`发布 · ${definition.cli_id}`) + publishForm(definition));
}

async function openCLIImpactDialog(cliID, version, generation, releaseGeneration) {
  const definition = findCLI(cliID);
  const candidate = version || definition?.current_version || definition?.candidates?.[0] || "";
  openDialog(dialogHead(`影响预览 · ${cliID}`) + emptyState("读取影响快照…"));
  const response = await fetchImpact(cliID, candidate, generation, releaseGeneration);
  if (response.status !== 200) {
    $("dialog-body").innerHTML =
      dialogHead(`影响预览 · ${cliID}`) +
      failure("影响预览未读取", errorText(response.status, response.body));
    return;
  }
  $("dialog-body").innerHTML =
    dialogHead(`影响预览 · ${cliID}`) +
    `<p class="sub">候选 ${esc(response.body?.candidate_version || candidate)}；这是只读快照，提交发布时仍会复验代际。</p>` +
    impactHTML(response.body);
}

function openConfigDialog(config) {
  openDialog(dialogHead(config ? "写入 Agent 配置修订" : "写入 Agent 配置") + configForm(config));
}

function openProjectionDialog(config) {
  if (!config) {
    return;
  }
  openDialog(dialogHead("per-attempt 只读投影") + projectionForm(config));
}

function openDiscoveryDialog() {
  openDialog(dialogHead("发起受限发现") + discoveryForm(state.sources));
}

function readArtifacts(form) {
  const artifacts = [];
  for (const fieldset of form.querySelectorAll("[data-platform]")) {
    const [os, arch] = String(fieldset.dataset.platform ?? "").split("/");
    const url = fieldset.querySelector('[name="url"]')?.value ?? "";
    const sha256 = fieldset.querySelector('[name="sha256"]')?.value ?? "";
    const rawSize = String(fieldset.querySelector('[name="size"]')?.value ?? "").trim();
    artifacts.push({
      platform: { os, arch },
      url,
      sha256,
      size: rawSize === "" ? 0 : Number(rawSize),
    });
  }
  return artifacts;
}

async function doCLICheck(cliID) {
  notice(callout("正在检查", `${esc(cliID)}：读取版本源并按检查设置决定是否自动发布。`));
  const response = await api(`/admin/cli/${encodeURIComponent(cliID)}/check`, { method: "POST" });
  if (response.status === 200) {
    state.pageNotice = checkOutcome(response.body);
  } else {
    state.pageNotice = failure("检查未完成", errorText(response.status, response.body));
  }
  await render();
}

async function verifyAdapters() {
  const response = await api("/admin/discovery/adapters/verify", { method: "POST" });
  if (response.status === 200) {
    state.discovery.adapters = Array.isArray(response.body) ? response.body : [];
    state.pageNotice = callout("适配器验证完成", "只把通过命令面探针的适配器标为可信；其余保持不支持/需配置。");
  } else {
    state.pageNotice = failure("适配器验证未完成", errorText(response.status, response.body));
  }
  await render();
}

async function verifyAdapter(name) {
  const response = await api(`/admin/discovery/adapters/${encodeURIComponent(name)}/verify`, { method: "POST" });
  if (response.status === 200) {
    const index = state.discovery.adapters.findIndex((adapter) => adapter.name === name);
    if (index >= 0) {
      state.discovery.adapters[index] = response.body;
    }
    state.pageNotice = callout(`适配器 ${name}`, response.body?.trusted ? "命令面已验证，可执行发现。" : "未通过验证，保持不支持。");
  } else {
    state.pageNotice = failure("适配器验证未完成", errorText(response.status, response.body));
  }
  await render();
}

async function submitCLIDefinition(form) {
  const { errors, payload } = validateDefinition(formValues(form));
  if (payload === null) {
    showFormErrors(form, errors);
    return;
  }
  const response = await api("/admin/cli", { method: "POST", body: payload });
  if (response.status === 201) {
    closeDialog();
    state.pageNotice = callout("已登记 CLI", `${esc(payload.cli_id)} 已保存；先登记候选版本，再检查或发布。`);
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", failure("登记未成功", errorText(response.status, response.body)));
}

async function submitCLISettings(form) {
  const { errors, payload } = validateCLISettings(formValues(form));
  if (payload === null) {
    showFormErrors(form, errors);
    return;
  }
  const cliID = form.dataset.id;
  const response = await api(`/admin/cli/${encodeURIComponent(cliID)}/settings`, { method: "PATCH", body: payload });
  if (response.status === 200) {
    closeDialog();
    state.pageNotice = callout("发布设置已保存", `${esc(cliID)} 的检查与自动发布开关已更新。`);
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", failure("设置未保存", errorText(response.status, response.body)));
}

async function submitCLIVersion(form) {
  const { errors, payload } = validateVersion(form.elements.version?.value ?? "", readArtifacts(form));
  if (payload === null) {
    showFormErrors(form, errors);
    return;
  }
  const cliID = form.dataset.id;
  const response = await api(`/admin/cli/${encodeURIComponent(cliID)}/versions`, { method: "POST", body: payload });
  if (response.status === 201) {
    closeDialog();
    state.pageNotice = callout("已登记候选版本", `${esc(cliID)} ${esc(payload.version)} 已登记；发布前仍会逐平台验证。`);
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", failure("候选版本未登记", errorText(response.status, response.body)));
}

async function submitCLIPublish(form) {
  const version = String(form.elements.version?.value ?? "").trim();
  if (version === "") {
    showFormErrors(form, { version: "版本不能为空。" });
    return;
  }
  const cliID = form.dataset.id;
  const response = await api(`/admin/cli/${encodeURIComponent(cliID)}/release`, {
    method: "POST",
    body: {
      version,
      expected_generation: Number(form.dataset.generation ?? 0),
      expected_release_generation: Number(form.dataset.releaseGeneration ?? 0),
    },
  });
  const outcome = publishOutcome(response.status, response.body);
  form.insertAdjacentHTML("beforebegin", renderPublishOutcome(outcome));
  for (const control of form.querySelectorAll("button, input")) {
    control.disabled = true;
  }
  state.pageNotice = renderPublishOutcome(outcome);
  await render();
}

async function submitAgentConfig(form) {
  const { errors, payload } = validateConfig(formValues(form));
  if (payload === null) {
    showFormErrors(form, errors);
    return;
  }
  const values = formValues(form);
  const serverID = String(values.server_id ?? "").trim();
  const workspaceID = String(values.workspace_id ?? "").trim();
  const backend = String(values.backend ?? "").trim();
  const response = await api(
    `/admin/agent-configs/${encodeURIComponent(serverID)}/${encodeURIComponent(workspaceID)}/${encodeURIComponent(backend)}`,
    { method: "PUT", body: payload },
  );
  if (response.status === 200) {
    closeDialog();
    state.pageNotice = callout("配置修订已记录", `第 ${Number(response.body?.revision ?? 0)} 修订已保存；内容未回显、未持久化。`);
    await render();
    return;
  }
  form.insertAdjacentHTML("beforebegin", failure("配置未保存", errorText(response.status, response.body)));
}

async function submitProjection(form) {
  const attemptID = String(new FormData(form).get("attempt_id") ?? "").trim();
  const query = new URLSearchParams({ attempt_id: attemptID });
  if (form.dataset.profile) {
    query.set("profile", form.dataset.profile);
  }
  const response = await api(
    `/admin/agent-configs/${encodeURIComponent(form.dataset.server)}/${encodeURIComponent(form.dataset.workspace)}/${encodeURIComponent(form.dataset.backend)}/projection?${query.toString()}`,
  );
  const box = $("dialog-body");
  box.querySelector("#projection-result")?.remove();
  if (response.status === 200) {
    box.insertAdjacentHTML("beforeend", `<div id="projection-result">${projectionHTML(response.body)}</div>`);
  } else {
    box.insertAdjacentHTML("beforeend", `<div id="projection-result">${failure("投影未读取", errorText(response.status, response.body))}</div>`);
  }
}

function discoveryResultHTML(request) {
  const result = request?.result;
  const classText = result ? classBadge(result.class) : "";
  const models = (result?.models ?? []).map((model) => esc(model.id)).join("、");
  const capabilities = (result?.capabilities ?? []).map((capability) => esc(capability.name)).join("、");
  return `<div class="call ${result?.class === "failed" ? "danger" : result?.class === "unsupported" ? "warn" : "ok"}" role="status">
    <strong>发现请求 ${esc(request?.id || "")} · ${esc(request?.state || "")}</strong>
    <div>${classText}${models ? `<div>模型：${models}</div>` : ""}${capabilities ? `<div>能力：${capabilities}</div>` : ""}${result?.detail ? `<div>${esc(result.detail)}</div>` : ""}</div>
  </div>`;
}

async function submitDiscovery(form) {
  const { errors, payload } = validateDiscovery(formValues(form));
  if (payload === null) {
    showFormErrors(form, errors);
    return;
  }
  const response = await api("/admin/discovery/requests", { method: "POST", body: payload });
  const box = $("dialog-body");
  box.querySelector("#discovery-result")?.remove();
  if (response.status === 200) {
    box.insertAdjacentHTML("beforeend", `<div id="discovery-result">${discoveryResultHTML(response.body)}</div>`);
    state.pageNotice = discoveryResultHTML(response.body);
    await render();
    return;
  }
  box.insertAdjacentHTML("beforeend", `<div id="discovery-result">${failure("发现未执行", errorText(response.status, response.body))}</div>`);
}

async function submitModelsFilter(form) {
  const values = formValues(form);
  const query = new URLSearchParams();
  for (const key of ["server_id", "workspace_id", "runtime_id", "profile"]) {
    const value = String(values[key] ?? "").trim();
    if (value !== "") {
      query.set(key, value);
    }
  }
  state.discovery.filter = {
    server_id: String(values.server_id ?? "").trim(),
    workspace_id: String(values.workspace_id ?? "").trim(),
    runtime_id: String(values.runtime_id ?? "").trim(),
    profile: String(values.profile ?? "").trim(),
  };
  const response = await api(`/admin/discovery/models?${query.toString()}`);
  if (response.status === 200) {
    state.discovery.models = { view: response.body };
  } else {
    state.pageNotice = failure("模型目录未读取", errorText(response.status, response.body));
  }
  await render();
}

function openAttemptDialog(index) {
  const attempt = state.overview?.in_progress?.rows?.[index];
  if (!attempt) {
    return;
  }
  openDialog(`<div class="between"><h2>${esc(attempt.agent_name || attempt.agent_id)} · 进行中</h2><button type="button" data-act="close-dialog">关闭</button></div>${attemptDetail(attempt)}`);
}

// The in-progress row's detail is reachable three ways (UI-01): hover and
// keyboard focus show this floating card, a click opens the same facts in the
// dialog. The card reads the row's own description element, so the two
// presentations cannot drift apart.
function hideTip() {
  $("tip").hidden = true;
}

function showTip(trigger) {
  const described = trigger.getAttribute("aria-describedby");
  const detail = described ? document.getElementById(described) : null;
  if (!detail) {
    return;
  }
  const tip = $("tip");
  tip.innerHTML = detail.innerHTML;
  tip.hidden = false;
  const box = trigger.getBoundingClientRect();
  const margin = 8;
  const beside = box.right + tip.offsetWidth + 16 < window.innerWidth;
  const left = beside
    ? box.right + 8
    : Math.max(margin, Math.min(box.left, window.innerWidth - tip.offsetWidth - margin));
  const below = box.bottom + tip.offsetHeight + margin;
  const top = beside
    ? Math.max(margin, Math.min(box.top - 16, window.innerHeight - tip.offsetHeight - margin))
    : below < window.innerHeight
      ? box.bottom + 5
      : Math.max(margin, box.top - tip.offsetHeight - 5);
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

for (const name of ["mouseover", "focusin"]) {
  document.addEventListener(name, (event) => {
    const trigger = event.target.closest?.('[data-act="run-detail"]');
    if (trigger) {
      showTip(trigger);
    }
  });
}

for (const name of ["mouseout", "focusout"]) {
  document.addEventListener(name, (event) => {
    if (event.target.closest?.('[data-act="run-detail"]')) {
      hideTip();
    }
  });
}

window.addEventListener("scroll", hideTip, true);
window.addEventListener("resize", hideTip);

document.addEventListener("click", (event) => {
  const control = event.target.closest("[data-act]");
  if (!control || control.tagName === "FORM") {
    return;
  }
  const action = control.dataset.act;
  const id = control.dataset.id ?? "";
  switch (action) {
    case "go":
      closeDialog();
      location.hash = `#/${id}`;
      return;
    case "connect":
      credentialDialog();
      return;
    case "close-dialog":
      closeDialog();
      return;
    case "retry":
      render();
      return;
    case "refresh-overview":
      render();
      return;
    case "run-detail":
      openAttemptDialog(Number(id));
      return;
    case "add-source":
      openDialog(`<div class="between"><h2>添加 Multica Server</h2><button type="button" data-act="close-dialog">关闭</button></div>${sourceForm()}`);
      return;
    case "sync":
      doSync(id);
      return;
    case "open-directory":
      loadDirectory(id).then((ok) => {
        if (!ok) {
          state.pageNotice = failure("目录读取失败", "该来源不在授权范围，或目录不可读。");
        }
        render();
      });
      return;
    case "edit-source": {
      const source = state.sources.find((item) => item.server_id === id);
      if (source) {
        openDialog(`<div class="between"><h2>同步设置 · ${esc(id)}</h2><button type="button" data-act="close-dialog">关闭</button></div>${settingsForm(source)}`);
      }
      return;
    }
    case "toggle-auto":
      toggleAuto(id);
      return;
    case "workspace-filter":
      workspaceFilterDialog(id);
      return;
    case "edit-binding":
      openBindingEditor(id);
      return;
    case "ready-check":
      openReadinessDialog(id);
      return;
    case "rule-add":
      openRuleEditor();
      return;
    case "rule-edit":
      openRuleEditor(ruleDraftList().find((rule) => rule.id === id));
      return;
    case "rule-remove":
      removeRule(id);
      return;
    case "rule-publish":
      publishRules();
      return;
    case "rule-rollback":
      rollbackRules(id);
      return;
    case "rule-discard":
      discardRules();
      return;
    case "add-cli":
      openCLIDefinitionDialog();
      return;
    case "cli-settings":
      openCLISettingsDialog(findCLI(id));
      return;
    case "cli-version":
      openCLIVersionDialog(findCLI(id));
      return;
    case "cli-publish":
      openCLIPublishDialog(findCLI(id));
      return;
    case "cli-impact": {
      const form = control.closest("form");
      const version = control.dataset.version || form?.elements?.version?.value || "";
      const generation = control.dataset.generation || form?.dataset?.generation || "";
      const releaseGeneration = control.dataset.releaseGeneration || form?.dataset?.releaseGeneration || "";
      openCLIImpactDialog(id, version, generation, releaseGeneration);
      return;
    }
    case "cli-check":
      doCLICheck(id);
      return;
    case "config-write":
      openConfigDialog(findConfig(id));
      return;
    case "config-projection":
      openProjectionDialog(findConfig(id));
      return;
    case "verify-adapters":
      verifyAdapters();
      return;
    case "verify-adapter":
      verifyAdapter(id);
      return;
    case "discovery-request":
      openDiscoveryDialog();
      return;
    default:
      return;
  }
});

document.addEventListener("change", (event) => {
  const control = event.target.closest("[data-act]");
  if (!control) {
    return;
  }
  if (control.dataset.act === "agent-filter") {
    state.agentFilter[control.dataset.field] = control.value;
    if (control.dataset.field === "server") {
      state.agentFilter.workspace = "";
    }
    render();
  } else if (control.dataset.act === "rule-filter") {
    state.rules.filter[control.dataset.field] = control.value;
    if (control.dataset.field === "server") {
      state.rules.filter.workspace = "";
      state.rules.directory = null;
    }
    state.rules.preview = null;
    render();
  }
});

document.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  event.preventDefault();
  if (form.id === "credential-form") {
    submitCredential(form);
  } else if (form.id === "source-form") {
    submitSource(form);
  } else if (form.id === "settings-form") {
    submitSettings(form);
  } else if (form.id === "binding-form") {
    submitBinding(form);
  } else if (form.id === "readiness-form") {
    submitReadiness(form);
  } else if (form.id === "rule-form") {
    submitRuleDraft(form);
  } else if (form.id === "rule-preview-form") {
    submitRulePreview(form);
  } else if (form.id === "cli-definition-form") {
    submitCLIDefinition(form);
  } else if (form.id === "cli-settings-form") {
    submitCLISettings(form);
  } else if (form.id === "cli-version-form") {
    submitCLIVersion(form);
  } else if (form.id === "cli-publish-form") {
    submitCLIPublish(form);
  } else if (form.id === "agent-config-form") {
    submitAgentConfig(form);
  } else if (form.id === "projection-form") {
    submitProjection(form);
  } else if (form.id === "discovery-form") {
    submitDiscovery(form);
  } else if (form.id === "models-filter-form") {
    submitModelsFilter(form);
  } else if (form.id === "workspace-filter-form") {
    loadDirectory(form.dataset.id, String(new FormData(form).get("workspace_id") ?? "")).then((ok) => {
      if (!ok) {
        state.pageNotice = failure("目录读取失败", "该 Workspace 不在授权范围，或目录不可读。");
      }
      closeDialog();
      render();
    });
  }
});

$("dialog").addEventListener("close", () => {
  if (state.staleAgents) {
    state.staleAgents = false;
    render();
  }
});

$("connect").addEventListener("click", () => credentialDialog());

window.addEventListener("hashchange", () => {
  state.syncNotice = null;
  render();
});

renderCredentialState();
render();
