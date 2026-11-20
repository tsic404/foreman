// Multica management page: the source table, the registration form, the sync
// verdict and the directory snapshot (04-architecture.md §3.1, SYNC-02).

import { badge, button, callout, emptyState, esc, panel, stamp, table } from "./ui.mjs";
import { connectionBadge, syncBadge } from "./overview.mjs";

const SERVER_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const CREDENTIAL_REF = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const MIN_PERIOD = 10;
const MAX_PERIOD = 86400;
const MAX_RETRY = 10;

const MANAGEMENT_LABELS = {
  foreman: ["Foreman 托管", ""],
  unmanaged: ["非管理", "gray"],
  unbound: ["未绑定", "warn"],
};

export function managementBadge(management) {
  const [label, tone] = MANAGEMENT_LABELS[management] ?? ["状态未知", "unknown"];
  return badge(label, tone);
}

export function sourceRow(source) {
  const allowed = (source.allowed_workspaces ?? []).join("、") || "全部可见 Workspace";
  const auto = source.auto_sync ? badge("已开启", "ok") : badge("已关闭", "gray");
  const writeBack = source.write_back?.enabled
    ? badge("可写", "ok")
    : badge(source.write_back?.reason || "只读", "gray");
  return [
    `<strong>${esc(source.server_id)}</strong><small>${esc(source.url)}</small>`,
    `${esc(source.credential_ref)}<small>${esc(allowed)}</small>`,
    connectionBadge(source.connection),
    `${syncBadge(source.sync)}${source.stale ? ` ${badge("保留快照", "warn")}` : ""}`,
    `${auto}<small>每 ${Number(source.sync_period_s ?? 0)}s · 重试预算 ${Number(source.retry_budget ?? 0)}</small>`,
    `${source.generation ?? 0}<small>${stamp(source.last_success_at)}</small>`,
    esc(source.last_error || "—"),
    writeBack,
    `${button("手动同步", "sync", source.server_id)} ${button("查看目录", "open-directory", source.server_id)} ${button("设置", "edit-source", source.server_id)} ${button(source.auto_sync ? "关闭自动同步" : "开启自动同步", "toggle-auto", source.server_id)}`,
  ];
}

export function renderSources(sources) {
  return panel(
    "已登记的 Multica Server",
    `<p class="sub">每个来源独立保存目录快照、同步代际与自动同步设置；关闭自动同步不会撤销已有绑定，也不禁用手动同步。</p>` +
      table(
        ["Server / 端点", "凭据引用 / 范围", "连接", "同步", "自动同步", "代际 / 最近成功", "最近错误", "绑定写权", "操作"],
        (Array.isArray(sources) ? sources : []).map(sourceRow),
        "尚未登记任何来源。",
      ),
    `<button type="button" class="primary" data-act="add-source">＋ 添加 Multica Server</button>`,
  );
}

export function sourceForm() {
  return `<form id="source-form" class="fields">
    <label>稳定 ID<input name="server_id" required placeholder="atlas" autocomplete="off"></label>
    <label>Server URL<input name="url" required placeholder="https://multica.example.test" autocomplete="off"></label>
    <label>凭据引用<input name="credential_ref" required placeholder="atlas" autocomplete="off"></label>
    <label>允许的 Workspace（逗号分隔，可留空）<input name="allowed_workspaces" placeholder="ws-id-a, ws-id-b" autocomplete="off"></label>
    <label>自动同步<select name="auto_sync"><option value="on">开启</option><option value="off">关闭，仅手动</option></select></label>
    <label>同步周期（秒，10–86400）<input name="sync_period_s" type="number" min="10" max="86400" value="300"></label>
    <label>重试预算（1–10）<input name="retry_budget" type="number" min="1" max="10" value="3"></label>
    <div class="form-actions"><button type="submit" class="primary">登记来源</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">只登记凭据引用；凭据值由部署挂载在凭据目录，页面从不显示或提交它。</p>
  </form>`;
}

export function settingsForm(source) {
  return `<form id="settings-form" class="fields" data-id="${esc(source.server_id)}">
    <label>自动同步<select name="auto_sync"><option value="on" ${source.auto_sync ? "selected" : ""}>开启</option><option value="off" ${source.auto_sync ? "" : "selected"}>关闭，仅手动</option></select></label>
    <label>同步周期（秒，10–86400）<input name="sync_period_s" type="number" min="10" max="86400" value="${esc(source.sync_period_s ?? 300)}"></label>
    <label>重试预算（1–10）<input name="retry_budget" type="number" min="1" max="10" value="${esc(source.retry_budget ?? 3)}"></label>
    <div class="form-actions"><button type="submit" class="primary">保存设置</button><button type="button" data-act="close-dialog">取消</button></div>
  </form>`;
}

function integerInRange(value, min, max) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= min && parsed <= max ? parsed : null;
}

// validateRegistration mirrors the registration face's own checks so a
// malformed form is corrected before it travels; the service stays the
// authority and its refusal is displayed as returned.
export function validateRegistration(values) {
  const errors = {};
  const serverID = String(values.server_id ?? "").trim();
  if (!SERVER_ID.test(serverID)) {
    errors.server_id = "1–64 位字母、数字、点、下划线或连字符，且以字母或数字开头。";
  }
  const url = String(values.url ?? "").trim();
  let parsed = null;
  try {
    parsed = new URL(url);
  } catch {
    parsed = null;
  }
  if (!parsed || (parsed.protocol !== "http:" && parsed.protocol !== "https:") || !parsed.host) {
    errors.url = "需要绝对 http(s) URL。";
  } else if (parsed.username || parsed.password) {
    errors.url = "URL 不得携带凭据。";
  }
  const credentialRef = String(values.credential_ref ?? "").trim();
  if (!CREDENTIAL_REF.test(credentialRef)) {
    errors.credential_ref = "只填凭据引用，不填凭据值。";
  }
  const rawWorkspaces = String(values.allowed_workspaces ?? "");
  const workspaces = [];
  if (rawWorkspaces.trim() !== "") {
    for (const part of rawWorkspaces.split(",")) {
      const workspaceID = part.trim();
      if (workspaceID === "") {
        errors.allowed_workspaces = "工作区 ID 不能为空；多项用逗号分隔。";
        continue;
      }
      if (!workspaces.includes(workspaceID)) {
        workspaces.push(workspaceID);
      }
    }
  }
  const period = integerInRange(values.sync_period_s, MIN_PERIOD, MAX_PERIOD);
  if (period === null) {
    errors.sync_period_s = `同步周期需要 ${MIN_PERIOD}–${MAX_PERIOD} 秒的整数。`;
  }
  const retry = integerInRange(values.retry_budget, 1, MAX_RETRY);
  if (retry === null) {
    errors.retry_budget = `重试预算需要 1–${MAX_RETRY} 的整数。`;
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: {
      server_id: serverID,
      url: parsed.toString().replace(/\/$/, ""),
      credential_ref: credentialRef,
      allowed_workspaces: workspaces,
      auto_sync: values.auto_sync !== "off",
      sync_period_s: period,
      retry_budget: retry,
    },
  };
}

export function validateSettings(values) {
  const errors = {};
  const period = integerInRange(values.sync_period_s, MIN_PERIOD, MAX_PERIOD);
  if (period === null) {
    errors.sync_period_s = `同步周期需要 ${MIN_PERIOD}–${MAX_PERIOD} 秒的整数。`;
  }
  const retry = integerInRange(values.retry_budget, 1, MAX_RETRY);
  if (retry === null) {
    errors.retry_budget = `重试预算需要 1–${MAX_RETRY} 的整数。`;
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: { auto_sync: values.auto_sync !== "off", sync_period_s: period, retry_budget: retry },
  };
}

// syncOutcome turns one POST /admin/servers/{id}/sync answer into the notice
// the page shows. A failed attempt keeps the generation the server names and
// never clears the directory the page is already showing (SYNC-02).
export function syncOutcome(status, report) {
  const body = report && typeof report === "object" ? report : {};
  const result = String(body.result ?? body.Result ?? "");
  const kept = Number(body.kept_generation ?? 0);
  const successful = status >= 200 && status < 300 && result === "ok";
  if (successful) {
    const generation = Number(body.generation ?? 0);
    return {
      tone: "ok",
      title: `同步完成 · 第 ${generation} 代`,
      detail: `Workspace ${body.workspaces ?? 0} · Project ${body.projects ?? 0} · Agent ${body.agents ?? 0} · runtime ${body.runtimes ?? 0} · 墓碑 ${body.tombstones ?? 0}`,
      generation,
      keptGeneration: 0,
      keepDirectory: false,
      refresh: true,
    };
  }
  if (status === 409) {
    return {
      tone: "warn",
      title: "同步正在进行",
      detail: body.detail || "同一来源已有同步在执行；目录保持不变，稍后重试。",
      generation: 0,
      keptGeneration: kept,
      keepDirectory: true,
      refresh: false,
    };
  }
  if (kept > 0) {
    return {
      tone: "danger",
      title: `同步失败 · 继续显示第 ${kept} 代快照`,
      detail: `${body.detail || `HTTP ${status}`}；本来源保留上一次完整快照，目录未清空，其他来源不受影响。`,
      generation: Number(body.generation ?? 0),
      keptGeneration: kept,
      keepDirectory: true,
      refresh: false,
    };
  }
  return {
    tone: "danger",
    title: `同步失败（HTTP ${status}）`,
    detail: `${body.detail || body.error || "上游读取失败"}；页面保留当前显示的目录。`,
    generation: Number(body.generation ?? 0),
    keptGeneration: 0,
    keepDirectory: true,
    refresh: false,
  };
}

export function renderSyncNotice(outcome) {
  return `<div class="call ${esc(outcome.tone)}" role="alert" data-tone="${esc(outcome.tone)}"><strong>${esc(outcome.title)}</strong><div>${esc(outcome.detail)}</div></div>`;
}

// directoryAfterSync answers which directory the page must render after a sync
// answer: the one it already had unless the answer earned a refresh.
export function directoryAfterSync(previous, outcome) {
  return outcome.keepDirectory ? previous : null;
}

function workspaceRows(directory) {
  return (directory.workspaces ?? []).map((workspace) => [
    `${esc(workspace.name)}<small>${esc(workspace.workspace_id)}</small>`,
    String(workspace.agent_count ?? 0),
    String(workspace.foreman_agent_count ?? 0),
  ]);
}

function projectRows(directory) {
  return (directory.projects ?? []).map((project) => [
    esc(project.title),
    esc(project.project_id),
    esc(project.workspace_id),
  ]);
}

function agentRows(directory) {
  return (directory.agents ?? []).map((agent) => [
    `${esc(agent.name)}<small>${esc(agent.agent_id)}</small>`,
    `${esc(agent.runtime_id || "未绑定")}`,
    managementBadge(agent.management),
    esc(agent.workspace_id),
    agent.tombstoned ? badge("已删除（墓碑）", "warn") : stamp(agent.synced_at),
  ]);
}

function runtimeRows(directory) {
  return (directory.runtimes ?? []).map((runtime) => [
    `${esc(runtime.name)}<small>${esc(runtime.runtime_id)}</small>`,
    esc(runtime.provider),
    esc(runtime.status),
    runtime.is_managed ? badge("Foreman 注册", "ok") : badge("非管理", "gray"),
    esc(runtime.workspace_id),
  ]);
}

function tombstoneRows(directory) {
  return (directory.tombstones ?? []).map((tombstone) => [
    esc(tombstone.kind),
    esc(tombstone.id),
    esc(tombstone.workspace_id),
    stamp(tombstone.removed_at),
    String(tombstone.generation ?? 0),
  ]);
}

export function renderDirectory(directory) {
  if (!directory) {
    return emptyState("尚未选择来源。选择一个来源查看它的当前目录代际。");
  }
  const generation = Number(directory.generation ?? 0);
  const header = `<div class="row">${badge(`第 ${generation} 代`, "")} ${syncBadge(directory.sync)} ${connectionBadge(directory.connection)} ${directory.stale ? badge("陈旧快照", "warn") : ""}<span class="sub">最近同步 ${stamp(directory.synced_at)}</span></div>`;
  if (generation === 0 || !directory.synced_at) {
    return panel(
      `同步目录 · ${directory.server_id}`,
      `${header}${emptyState("该来源尚未同步；登记后执行一次手动同步，下游才会出现它的 Workspace、Agent、Project 与 runtime。")}`,
    );
  }
  const body =
    header +
    (directory.last_error ? `<div class="call danger" role="alert"><strong>最近一次同步失败</strong><div>${esc(directory.last_error)}</div></div>` : "") +
    `<h3>Workspace</h3>` +
    table(["名称 / ID", "Agent", "其中 Foreman"], workspaceRows(directory), "该来源没有 Workspace。") +
    `<h3>Project</h3>` +
    table(["标题", "Project ID", "Workspace"], projectRows(directory), "该来源没有 Project。") +
    `<h3>Agent</h3>` +
    table(["名称 / ID", "当前 runtime", "管理归属", "Workspace", "同步/状态"], agentRows(directory), "该来源没有 Agent。") +
    `<h3>Runtime</h3>` +
    table(["名称 / ID", "Provider", "状态", "归属", "Workspace"], runtimeRows(directory), "该来源没有 runtime。") +
    `<h3>墓碑</h3>` +
    table(["类型", "对象", "Workspace", "移除时间", "代际"], tombstoneRows(directory), "没有已确认删除的对象。");
  return panel(
    `同步目录 · ${directory.server_id}`,
    body,
    `<button type="button" data-act="workspace-filter" data-id="${esc(directory.server_id)}">按 Workspace 过滤</button>`,
  );
}

export function renderAudit(entries) {
  const rows = (Array.isArray(entries) ? entries : []).map((entry) => [
    stamp(entry.at),
    esc(entry.action),
    `${esc(entry.server_id || "—")}<small>${esc(entry.workspace_id || "")}${entry.agent_id ? ` / ${esc(entry.agent_id)}` : ""}</small>`,
    entry.old_runtime_id || entry.new_runtime_id ? `${esc(entry.old_runtime_id || "—")} → ${esc(entry.new_runtime_id || "—")}` : "—",
    badge(entry.result, entry.result === "ok" ? "ok" : "warn"),
    esc(entry.detail || "—"),
  ]);
  return panel("审计", table(["时间", "动作", "来源 / 对象", "runtime 变更", "结果", "说明"], rows, "没有审计记录。"));
}

export function calloutHTML(title, body, tone = "") {
  return callout(title, body, tone);
}
