// Agent management page: the runtime binding editor and its read-back verdict
// (04-architecture.md §3.2, 02-contracts V2-C02, BIND-03). Only a read-back
// confirmed `applied` is success; every other class keeps its own reading.

import { badge, button, esc, panel, stamp, table } from "./ui.mjs";
import { managementBadge } from "./sources.mjs";

// The server's BindingOutcome is serialised without JSON tags today, so the
// wire carries `Result`; both spellings are read so a tag fix does not break
// the page.
function field(body, lower, upper) {
  const value = body?.[lower];
  return value === undefined ? body?.[upper] : value;
}

function resultOf(body) {
  return String(field(body, "result", "Result") ?? "");
}

function detailOf(body) {
  const detail = field(body, "detail", "Detail");
  return String(detail ?? body?.error ?? "");
}

export function bindingPayload(agent, runtimeID) {
  return {
    runtime_id: runtimeID,
    expected_runtime_id: agent.runtime_id ?? "",
  };
}

// agentKey names one row across the page's state: the three ids together, so
// two sources with the same agent id stay distinct. The form and the row
// button carry the same string; a lookup never guesses from a display name.
export function agentKey(agent) {
  return JSON.stringify([agent.server_id, agent.workspace_id, agent.agent_id]);
}

export function bindingOutcome(status, body) {
  const result = resultOf(body);
  const detail = detailOf(body);
  const ok = status >= 200 && status < 300;
  const outcome = (tone, title, success) => ({ tone, title, detail, success, status, result });
  if (ok && result === "applied") {
    return outcome("ok", "已回读确认：绑定已生效", true);
  }
  switch (result) {
    case "conflict":
      return outcome("warn", "冲突：上游绑定已被他人修改，本次未写入", false);
    case "not_applied":
      return outcome("warn", "未生效：上游仍是原 runtime", false);
    case "rejected":
      return outcome("danger", "上游拒绝：本次绑定未写入", false);
    case "unknown":
      return outcome("unknown", "结果未知：已回读一次仍未确认，请刷新核对", false);
    case "unsupported":
      return outcome("muted", "此来源不可写：绑定编辑已关闭", false);
    default:
      break;
  }
  if (status === 401) {
    return outcome("danger", "凭据未被接受：请重新连接", false);
  }
  if (status === 404) {
    return outcome("danger", "对象不在授权范围或已不存在", false);
  }
  if (ok) {
    return outcome("unknown", "结果未知：响应没有给出可确认的绑定结论", false);
  }
  return outcome("danger", `绑定未成功（HTTP ${status}）`, false);
}

export function renderBindingResult(outcome) {
  return `<div class="call ${esc(outcome.tone)}" role="alert" data-result="${esc(outcome.result)}" data-success="${outcome.success ? "yes" : "no"}"><strong>${esc(outcome.title)}</strong>${outcome.detail ? `<div>${esc(outcome.detail)}</div>` : ""}</div>`;
}

function runtimeOptions(options, current) {
  return options
    .map(
      (runtime) =>
        `<option value="${esc(runtime.runtime_id)}" ${runtime.runtime_id === current ? "selected" : ""}>${esc(runtime.name)} / ${esc(runtime.runtime_id)}</option>`,
    )
    .join("");
}

function impactText(agent) {
  return `新任务按新绑定派发；已确认的在途尝试仍按原绑定与原授权收敛。切换为「非管理」后 Foreman 不再为该 Agent 新建 Job，已有运行快照不变。`;
}

export function bindingEditor(agent) {
  const capability = agent.write_back ?? { enabled: false, reason: "该来源当前不可写。" };
  const source = `${agent.server_id} / ${agent.workspace_id}`;
  const head = `<div class="between"><h2>调整 Multica runtime</h2><span class="badge">${esc(source)}</span></div>`;
  const facts = `<dl>
    <dt>来源</dt><dd>${esc(agent.server_id)} / ${esc(agent.workspace_id)}</dd>
    <dt>Agent</dt><dd>${esc(agent.name)} / ${esc(agent.agent_id)}</dd>
    <dt>原绑定</dt><dd>${esc(agent.runtime_id || "未绑定")}</dd>
    <dt>影响</dt><dd>${esc(impactText(agent))}</dd>
  </dl>`;
  if (!capability.enabled) {
    return `<section class="panel" id="binding-editor">${head}${facts}<div class="call warn" role="status"><strong>绑定编辑已关闭</strong><div>${esc(capability.reason || "该来源当前不可写。")}</div></div></section>`;
  }
  const foreman = agent.foreman_runtimes ?? [];
  const unmanaged = agent.unmanaged_runtimes ?? [];
  if (foreman.length === 0 && unmanaged.length === 0) {
    return `<section class="panel" id="binding-editor">${head}${facts}<div class="call warn" role="status"><strong>没有可用 runtime</strong><div>该 Workspace 的目录里没有可以作为目标的 runtime。</div></div></section>`;
  }
  return `<section class="panel" id="binding-editor">${head}${facts}
    <form id="binding-form" data-key="${esc(agentKey(agent))}">
      <label>新绑定
        <select name="runtime">
          <optgroup label="Foreman 已注册">${runtimeOptions(foreman, agent.runtime_id)}</optgroup>
          <optgroup label="非管理 · 其他 Multica runtime">${runtimeOptions(unmanaged, agent.runtime_id)}</optgroup>
        </select>
      </label>
      <div class="form-actions">
        <button type="submit" class="primary" data-act="submit-binding">写回 Multica 并回读</button>
        <button type="button" data-act="close-dialog">取消</button>
      </div>
    </form>
  </section>`;
}

function agentRow(agent) {
  const capability = agent.write_back ?? { enabled: false, reason: "该来源当前不可写。" };
  const bind = capability.enabled
    ? button("调整 runtime", "edit-binding", agentKey(agent))
    : `<button type="button" disabled title="${esc(capability.reason || "只读")}">调整 runtime</button>`;
  const readiness = button("按 Issue 检查就绪", "ready-check", agentKey(agent));
  return [
    `${esc(agent.name)}<small>${esc(agent.agent_id)}</small>`,
    esc(agent.server_id),
    esc(agent.workspace_id),
    `${esc(agent.runtime_id || "未绑定")}<small>${agent.runtime_bound ? "已绑定" : "无绑定"}</small>`,
    managementBadge(agent.management),
    agent.tombstoned ? badge("已删除（墓碑）", "warn") : stamp(agent.synced_at),
    `${bind} ${readiness}${agent.tombstoned ? `<div class="hint">${esc(agent.write_back?.reason || "该 Agent 已在上游删除，绑定已关闭。")}</div>` : ""}`,
  ];
}

export function renderAgents(agents) {
  const rows = (Array.isArray(agents) ? agents : []).map(agentRow);
  return panel(
    "Multica Agent",
    `<p class="sub">按来源与 Workspace 查看 Agent 的实际绑定。runtime 候选分两组：Foreman 在此来源已注册的 runtime，以及其他非管理 runtime；两者都保留精确 ID。</p>` +
      `<div class="call" role="status"><strong>只回写 runtime 绑定</strong><div>写回经上游回读确认；拒绝、超时与其他管理员的竞争修改都不会显示成功。非管理不新建 Job，已有运行快照不变。</div></div>` +
      `<p class="sub">「按 Issue 检查就绪」只读展示当前可接单、配置阻断、暂时满载、状态未知或非托管，并逐项给出证据时点、主阻因与修复入口；它不占槽，也不改变绑定。</p>` +
      table(
        ["Agent / ID", "Server", "Workspace", "当前 runtime", "管理归属", "同步时间", "操作"],
        rows,
        "该筛选下没有 Agent。",
      ),
  );
}
