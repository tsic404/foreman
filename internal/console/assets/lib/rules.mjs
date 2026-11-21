// Environment rules and the per-Issue readiness check (04-architecture.md
// §3.2/§3.4, 02-contracts V2-C01). The page consumes the published management
// endpoints: it never re-derives which environment a task would get.

import { badge, button, callout, esc, panel, stamp, table } from "./ui.mjs";

export const OUTCOME_LABELS = {
  ready: ["当前可接单", "ok"],
  config_blocked: ["配置阻断", "danger"],
  temporarily_saturated: ["暂时满载", "warn"],
  unknown: ["状态未知", "unknown"],
  unmanaged: ["非托管", "gray"],
};

const LEVEL_LABELS = {
  global: "全局",
  server: "Server",
  workspace: "Workspace",
  "single-dimension": "Agent 或 Project",
  combined: "Agent × Project",
};

// Remediation paths name Management API routes. The console shows the page
// beside each route; an unknown path is displayed as text instead of a link
// that would leave the operator on a JSON endpoint.
const PAGE_FOR_PATH = {
  "/admin/agents": "agents",
  "/admin/servers": "sources",
  "/admin/environments": "environments",
  "/admin/rules": "rules",
  "/admin/cli": "cli",
  "/admin/agent-configs": "cli",
};

export function outcomeLabel(outcome) {
  return (OUTCOME_LABELS[outcome] ?? OUTCOME_LABELS.unknown)[0];
}

export function outcomeBadge(outcome) {
  const [label, tone] = OUTCOME_LABELS[outcome] ?? OUTCOME_LABELS.unknown;
  return badge(label, tone);
}

function checkBadge(status) {
  return status === "ready" ? badge("通过", "ok") : outcomeBadge(status);
}

export function ruleLevelLabel(level) {
  return LEVEL_LABELS[level] ?? String(level ?? "未知层级");
}

// The published rule shape carries the four conditions, not a derived level.
// The console classifies the scope the same way the resolver documents it:
// combined > single dimension > workspace > server > global.
export function ruleLevel(rule = {}) {
  if (!rule.server_id && !rule.workspace_id) {
    return "global";
  }
  if (!rule.workspace_id) {
    return "server";
  }
  if (rule.agent_id && rule.project_id) {
    return "combined";
  }
  if (rule.agent_id || rule.project_id) {
    return "single-dimension";
  }
  return "workspace";
}

export function remediationAction(remediation) {
  if (!remediation) {
    return "";
  }
  const target = PAGE_FOR_PATH[remediation.path];
  if (!target) {
    return `<span class="hint">${esc(remediation.label || remediation.path || "未提供修复入口")}</span>`;
  }
  return button(remediation.label || "去修复", "go", target);
}

function scopeText(rule) {
  return [rule.server_id, rule.workspace_id, rule.agent_id, rule.project_id]
    .map((value) => value || "*")
    .join(" / ");
}

function conditionText(rule) {
  const parts = [];
  if (rule.environment_id) {
    parts.push(`环境 ${rule.environment_id}`);
  }
  if (rule.trust_domain) {
    parts.push(`信任域 ${rule.trust_domain}`);
  }
  if (rule.required_capabilities?.length) {
    parts.push(`必需 ${rule.required_capabilities.join("、")}`);
  }
  if (rule.prefer_capabilities?.length) {
    parts.push(`偏好 ${rule.prefer_capabilities.join("、")}`);
  }
  return parts.length ? parts.join("；") : "本域内按偏好与空闲槽选择";
}

function ruleRow(rule, editable) {
  return [
    `${esc(ruleLevelLabel(rule.level ?? ruleLevel(rule)))} · ${esc(rule.id)}`,
    esc(scopeText(rule)),
    esc(conditionText(rule)),
    rule.enabled ? badge("启用", "ok") : badge("停用", "gray"),
    editable
      ? `${button("编辑", "rule-edit", rule.id)} ${button("移除", "rule-remove", rule.id)}`
      : '<span class="hint">已发布，只读</span>',
  ];
}

function rulesTable(rules, editable, emptyText) {
  return table(
    ["层级 / ID", "Server / Workspace / Agent / Project", "环境条件", "状态", "操作"],
    (Array.isArray(rules) ? rules : []).map((rule) => ruleRow(rule, editable)),
    emptyText,
  );
}

function sameRules(left, right) {
  const serialize = (rules) =>
    JSON.stringify(
      (Array.isArray(rules) ? rules : []).map((rule) => ({
        id: rule.id ?? "",
        server_id: rule.server_id ?? "",
        workspace_id: rule.workspace_id ?? "",
        agent_id: rule.agent_id ?? "",
        project_id: rule.project_id ?? "",
        environment_id: rule.environment_id ?? "",
        trust_domain: rule.trust_domain ?? "",
        required_capabilities: rule.required_capabilities ?? [],
        prefer_capabilities: rule.prefer_capabilities ?? [],
        enabled: rule.enabled === true,
      })),
    );
  return serialize(left) === serialize(right);
}

// DraftBaseRevision alone cannot distinguish "no draft yet" from a draft
// saved before the first publish (both carry 0). An empty wire draft identical
// to the published set is the no-draft answer; anything else was stored.
export function draftRules(state) {
  const published = Array.isArray(state?.published) ? state.published : [];
  const wireDraft = Array.isArray(state?.draft) ? state.draft : [];
  const stored = (state?.draft_base_revision ?? 0) > 0 || !sameRules(wireDraft, published);
  return (stored ? wireDraft : published).slice();
}

function findingRows(findings) {
  return (Array.isArray(findings) ? findings : []).map((finding) => [
    esc(finding.code || "finding"),
    esc(finding.rule_id || "—"),
    esc(finding.field || "—"),
    esc(finding.message || ""),
    remediationAction(finding.remediation),
  ]);
}

function findingsTable(findings, emptyText) {
  return table(["问题", "规则", "字段", "说明", "修复入口"], findingRows(findings), emptyText);
}

function findingsPanel(findings) {
  const rows = findingRows(findings);
  if (rows.length === 0) {
    return callout("草稿校验通过", "当前草稿没有阻止发布的问题。预览仍会按真实容量决定是否可接单。", "ok");
  }
  return `<div class="call danger" role="alert"><strong>草稿校验 · ${rows.length} 项阻止发布</strong>
    <div>冲突、悬空引用和已退役环境都会拒绝发布；已发布版本保持不变。</div></div>` +
    findingsTable(findings, "没有校验问题。");
}

function historyPanel(history) {
  const rows = (Array.isArray(history) ? history : [])
    .slice()
    .reverse()
    .map((revision) => [
      `r${esc(revision.revision)}${revision.rolled_back_from ? `<small>回滚自 r${esc(revision.rolled_back_from)}</small>` : ""}`,
      stamp(revision.published_at),
      esc(revision.actor || "—"),
      String((revision.rules ?? []).length),
      button("回滚到此版", "rule-rollback", String(revision.revision)),
    ]);
  return panel(
    "发布历史",
    `<p class="sub">回滚把历史内容重新校验后发布为新修订；不会恢复已退役环境，也不延长旧凭据。</p>` +
      table(["修订", "发布时间", "操作者", "规则数", "操作"], rows, "还没有已发布修订。"),
  );
}

export function renderRules(state, context = {}) {
  const published = Array.isArray(state?.published) ? state.published : [];
  const draft = draftRules(state);
  const findings = Array.isArray(state?.findings) ? state.findings : [];
  const hasDraft = !sameRules(draft, published);
  const revision = Number(state?.revision ?? 0);
  const draftBadge = hasDraft ? badge("草稿未发布", "warn") : badge("线上版本", "ok");

  const publishActions = `${draftBadge} ${button("发布草稿", "rule-publish", String(revision), "primary")} ${button("重置为已发布", "rule-discard", String(revision))}`;

  const header = panel(
    `环境规则 · 发布修订 ${revision}`,
    `<p class="sub">规则只选择环境，不改变 runtime 或固定 Job 模板。编辑先进入草稿；同级 Agent 与 Project 规则冲突、悬空引用或已退役环境都会拒绝发布。</p>` +
      rulesTable(published, false, "还没有已发布规则。") +
      `<div class="between"><h3>草稿</h3><div class="row">${publishActions}</div></div>` +
      rulesTable(draft, true, "草稿为空。"),
    button("＋ 添加规则", "rule-add"),
  );

  return header + panel("草稿校验", findingsPanel(findings), "") + historyPanel(state?.history);
}

export function ruleEditor(rule = {}) {
  const value = (key) => esc(rule[key] ?? "");
  const capabilities = (key) => esc((rule[key] ?? []).join(", "));
  const current = rule.id || "";
  return `<form id="rule-form" class="fields" data-id="${esc(current)}">
    <label>规则 ID<input name="id" required value="${value("id")}" placeholder="a5" autocomplete="off"></label>
    <label>Server<input name="server_id" value="${value("server_id")}" placeholder="atlas" autocomplete="off"></label>
    <label>Workspace<input name="workspace_id" value="${value("workspace_id")}" placeholder="core" autocomplete="off"></label>
    <label>Agent（可留空）<input name="agent_id" value="${value("agent_id")}" placeholder="dev" autocomplete="off"></label>
    <label>Project（可留空）<input name="project_id" value="${value("project_id")}" placeholder="p-1" autocomplete="off"></label>
    <label>指定环境（可留空）<input name="environment_id" value="${value("environment_id")}" placeholder="build-a" autocomplete="off"></label>
    <label>信任域（可留空）<input name="trust_domain" value="${value("trust_domain")}" placeholder="build" autocomplete="off"></label>
    <label>必需能力（逗号分隔）<input name="required_capabilities" value="${capabilities("required_capabilities")}" placeholder="nix, gpu" autocomplete="off"></label>
    <label>偏好能力（逗号分隔）<input name="prefer_capabilities" value="${capabilities("prefer_capabilities")}" placeholder="gpu" autocomplete="off"></label>
    <label>状态<select name="enabled"><option value="on" ${rule.enabled === false ? "" : "selected"}>启用</option><option value="off" ${rule.enabled === false ? "selected" : ""}>停用</option></select></label>
    <div class="form-actions"><button type="submit" class="primary">保存为草稿</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">Workspace 属于一个 Server；Agent 与 Project 是 Workspace 内的交叉条件。环境留空表示在本信任域的合格环境中按偏好与空闲槽选择。</p>
  </form>`;
}

function capabilityList(value) {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item !== "");
}

export function ruleFromForm(values) {
  return {
    id: String(values.id ?? "").trim(),
    server_id: String(values.server_id ?? "").trim(),
    workspace_id: String(values.workspace_id ?? "").trim(),
    agent_id: String(values.agent_id ?? "").trim(),
    project_id: String(values.project_id ?? "").trim(),
    environment_id: String(values.environment_id ?? "").trim(),
    trust_domain: String(values.trust_domain ?? "").trim(),
    required_capabilities: capabilityList(values.required_capabilities),
    prefer_capabilities: capabilityList(values.prefer_capabilities),
    enabled: values.enabled !== "off",
  };
}

function requestFields(values, keys) {
  const payload = {};
  for (const key of keys) {
    const value = String(values[key] ?? "").trim();
    if (value !== "") {
      payload[key] = value;
    }
  }
  return payload;
}

// The Issue is the authority for its project; the page sends the project only
// when the operator explicitly names an assumed scenario.
export function previewRequest(values) {
  return requestFields(values, ["server_id", "workspace_id", "agent_id", "issue_id", "project_id", "prior_session_id"]);
}

export function readinessRequest(values) {
  // The Issue is the authority for its project; a form-supplied project
  // would let the operator ask about a different task.
  return requestFields(values, ["server_id", "workspace_id", "agent_id", "issue_id"]);
}

export function ruleOutcome(status, body) {
  const ok = status >= 200 && status < 300;
  const findings = Array.isArray(body?.findings) ? body.findings : [];
  if (ok) {
    return { tone: "ok", title: "已保存", findings, body, success: true };
  }
  if (status === 401) {
    return { tone: "danger", title: "凭据未被接受：请重新连接", findings, body, success: false };
  }
  if (status === 403) {
    return { tone: "danger", title: "凭据没有部署范围的规则写权限", findings, body, success: false };
  }
  if (status === 404) {
    return { tone: "danger", title: "该历史修订已不在保留范围内", findings, body, success: false };
  }
  if (status === 409) {
    if (findings.length > 0) {
      return { tone: "warn", title: "发布被拒绝：草稿未通过校验", findings, body, success: false };
    }
    return { tone: "warn", title: "修订已变化：请刷新后重试", findings, body, success: false };
  }
  const detail = body?.error || body?.detail;
  return {
    tone: "danger",
    title: detail ? `未成功（HTTP ${status}）：${detail}` : `未成功（HTTP ${status}）`,
    findings,
    body,
    success: false,
  };
}

function primaryFinding(resolution) {
  if (resolution?.primary) {
    return resolution.primary;
  }
  const findings = Array.isArray(resolution?.findings) ? resolution.findings : [];
  return findings.length > 0 ? findings[0] : null;
}

function previewFindings(resolution) {
  const findings = Array.isArray(resolution?.findings) ? resolution.findings : [];
  if (findings.length === 0) {
    return "";
  }
  return panel(
    "全部解析问题",
    `<p class="sub">这些是服务端解析器对本次预览给出的具体问题；修复入口按对应管理面给出。</p>` +
      findingsTable(findings, "没有解析问题。"),
  );
}

export function renderRuleOutcome(outcome) {
  const detail = outcome.findings.length > 0
    ? table(
        ["问题", "规则", "说明", "修复入口"],
        outcome.findings.map((finding) => [
          esc(finding.code || "finding"),
          esc(finding.rule_id || "—"),
          esc(finding.message || ""),
          remediationAction(finding.remediation),
        ]),
        "没有明细。",
      )
    : "";
  return `<div class="call ${esc(outcome.tone)}" role="alert"><strong>${esc(outcome.title)}</strong>${detail}</div>`;
}

// An Agent is identified by its Server, Workspace and ID together. The same
// ID can exist in two Workspaces, so the select's value carries all three.
export function previewAgentKey(agent = {}) {
  return JSON.stringify([agent.server_id ?? "", agent.workspace_id ?? "", agent.agent_id ?? ""]);
}

function parsePreviewAgentKey(value) {
  if (typeof value !== "string" || !value.startsWith("[")) {
    return null;
  }
  try {
    const parts = JSON.parse(value);
    if (Array.isArray(parts) && parts.length === 3 && parts.every((part) => typeof part === "string")) {
      return { server_id: parts[0], workspace_id: parts[1], agent_id: parts[2] };
    }
  } catch {
    return null;
  }
  return null;
}

export function previewForm(context = {}, values = {}) {
  const chosen = parsePreviewAgentKey(values.agent_id)
    ? values.agent_id
    : previewAgentKey({
        server_id: context.serverID,
        workspace_id: context.workspaceID,
        agent_id: values.agent_id,
      });
  const agents = (context.agents ?? [])
    .map((agent) => {
      const key = previewAgentKey(agent);
      return `<option value="${esc(key)}" ${key === chosen ? "selected" : ""}>${esc(agent.name)} / ${esc(agent.workspace_id)} / ${esc(agent.agent_id)}</option>`;
    })
    .join("");
  return `<form id="rule-preview-form" class="fields">
    <label>Issue ID<input name="issue_id" required value="${esc(values.issue_id ?? "")}" placeholder="issue-1" autocomplete="off"></label>
    <label>Agent<select name="agent_id" required><option value="">选择 Agent</option>${agents}</select></label>
    <label>Project（假设场景才填写）<input name="project_id" value="${esc(values.project_id ?? "")}" placeholder="留空则从 Issue 读取" autocomplete="off"></label>
    <label>续跑会话 ID（首次运行留空）<input name="prior_session_id" value="${esc(values.prior_session_id ?? "")}" placeholder="以原端预算等待" autocomplete="off"></label>
    <div class="form-actions"><button type="submit" class="primary">运行只读预览</button></div>
    <p class="hint">预览与真实派发共用解析器，但不认领、不注册、不占槽；实际派发仍会复核容量、授权与配置代际。</p>
  </form>`;
}

// The selected Agent carries its own Server and Workspace; the page's filter
// only decides which directory the choices come from. Sending the filter's
// workspace with an agent from another workspace would preview a task that
// does not exist.
export function previewSelection(context = {}, values = {}) {
  const selected = parsePreviewAgentKey(values.agent_id);
  if (selected) {
    return {
      server_id: selected.server_id || context.serverID || "",
      workspace_id: selected.workspace_id || context.workspaceID || "",
      agent_id: selected.agent_id,
    };
  }
  const agent = (context.agents ?? []).find((candidate) => candidate.agent_id === values.agent_id);
  return {
    server_id: agent?.server_id || context.serverID || "",
    workspace_id: agent?.workspace_id || context.workspaceID || "",
    agent_id: agent?.agent_id || String(values.agent_id ?? "").trim(),
  };
}

function ruleDecisionRows(decisions) {
  return (Array.isArray(decisions) ? decisions : []).map((decision) => {
    const outcome = decision.selected
      ? badge("命中并选中", "ok")
      : decision.matched
        ? badge("命中但未选中", "warn")
        : badge("淘汰", "gray");
    return [
      esc(decision.rule_id || "—"),
      esc(ruleLevelLabel(decision.level)),
      outcome,
      esc(decision.reason || "—"),
    ];
  });
}

function environmentDecisionRows(decisions) {
  return (Array.isArray(decisions) ? decisions : []).map((decision) => [
    esc(decision.id || "—"),
    decision.selected ? badge("选中", "ok") : badge("淘汰", "gray"),
    String(decision.free_slots ?? "—"),
    esc(decision.reason || "—"),
  ]);
}

function previewFacts(resolution) {
  return [
    ["Server / Workspace", `${resolution.server_id || "—"} / ${resolution.workspace_id || "—"}`],
    ["Agent", resolution.agent_id || "—"],
    ["Issue / Project", `${resolution.issue_id || "—"} / ${resolution.project_id || "未归属项目"}`],
    ["实际 runtime", resolution.runtime_id ? `${resolution.runtime_id}（${resolution.provider || "provider 未知"}）` : "未确定"],
    ["管理归属", resolution.management || "未确定"],
    ["来源代际", `${resolution.source_generation ?? 0} · ${stamp(resolution.source_synced_at)}`],
    ["发布规则修订", `r${resolution.rules_revision ?? 0}${resolution.rule_id ? ` · 选中 ${resolution.rule_id}（${ruleLevelLabel(resolution.rule_level)}）` : ""}`],
    ["检查时点", stamp(resolution.checked_at)],
  ];
}

function selectedEnvironment(resolution) {
  const environment = resolution.environment;
  if (!environment) {
    return callout("未选中环境", "解析器没有给出可执行环境；原因见上面的规则与淘汰理由。", "warn");
  }
  return panel(
    `选中的环境 · ${environment.id}`,
    `<dl>
      <dt>端点</dt><dd>${esc(environment.endpoint || "—")}</dd>
      <dt>信任域</dt><dd>${esc(environment.trust_domain || "—")}</dd>
      <dt>能力</dt><dd>${esc((environment.capabilities ?? []).join("、") || "未声明")}</dd>
      <dt>容量</dt><dd>${esc(`${environment.free_slots ?? 0}/${environment.concurrency ?? 0}`)} 空闲</dd>
    </dl>`,
  );
}

function cliPanel(resolution) {
  const clis = resolution.clis ?? {};
  const rows = [
    ["Multica CLI", clis.multica_version || "未发布", clis.platform || "—", clis.multica_sha256 || "—"],
    [
      "Agent CLI",
      clis.agent_version || "未发布",
      clis.platform || "—",
      clis.agent_sha256 || "—",
    ],
  ];
  return panel(
    "本次 Job 的两份 CLI",
    `<p class="sub">平台取自发布清单的目标架构；Job 只准备这两份，不下载其他后端。</p>` +
      table(["发布线", "版本", "架构", "摘要"], rows, "没有 CLI 事实。"),
  );
}

function configPanel(resolution) {
  const config = resolution.config ?? {};
  return panel(
    "Agent 配置 Secret 引用",
    `<dl>
      <dt>配置修订</dt><dd>${config.known ? `r${config.revision ?? 0}` : "未知"}</dd>
      <dt>Secret 引用</dt><dd>${esc(config.secret_ref || "—")}</dd>
      <dt>只读文件名</dt><dd>${esc(config.file_name || "—")}</dd>
      <dt>摘要</dt><dd>${esc(config.sha256 || "—")}</dd>
      <dt>可读证据</dt><dd>${esc(config.readability || "未判定")}</dd>
    </dl>
    <p class="hint">页面只显示引用与版本，从不回显 Secret 内容。</p>`,
  );
}

function templatePanel(resolution) {
  const template = resolution.template ?? {};
  return panel(
    "固定模板 · 脱敏规格",
    `<dl>
      <dt>模板</dt><dd>${esc(template.name || "foreman-job")}</dd>
      <dt>命名空间</dt><dd>${esc(template.namespace || "—")}</dd>
      <dt>说明</dt><dd>${esc(template.note || "规则不改变固定 Job 模板")}</dd>
    </dl>
    <p class="hint">只展示脱敏后的模板身份；具体容器结构和凭据由固定的平台模板与本次绑定生成。</p>`,
  );
}

function resolutionDetails(resolution) {
  return (
    panel("解析事实", `<dl>${previewFacts(resolution).map(([key, value]) => `<dt>${esc(key)}</dt><dd>${esc(value)}</dd>`).join("")}</dl>`) +
    panel(
      "全部规则命中 / 淘汰",
      table(["规则", "层级", "结果", "理由"], ruleDecisionRows(resolution.rule_decisions), "没有发布规则。"),
    ) +
    panel(
      "候选环境",
      table(["环境", "结果", "快照空闲", "理由"], environmentDecisionRows(resolution.environment_decisions), "没有登记环境。"),
    ) +
    selectedEnvironment(resolution) +
    cliPanel(resolution) +
    configPanel(resolution) +
    templatePanel(resolution)
  );
}

export function renderPreview(resolution, context = {}) {
  if (!resolution) {
    return emptyPreview(context);
  }
  const primary = resolution.outcome === "ready" ? null : primaryFinding(resolution);
  const primaryCallout = primary
    ? `<div class="call ${resolution.outcome === "temporarily_saturated" ? "warn" : "danger"}" role="alert"><strong>主阻因 · ${esc(primary.code || "finding")}</strong>
        <div>${esc(primary.message || "")}</div>
        <div class="row">${remediationAction(primary.remediation)}</div></div>`
    : "";
  return (
    `<div class="between"><h2>任务预览 · ${esc(context.serverID || resolution.server_id || "")} / ${esc(context.workspaceID || resolution.workspace_id || "")}</h2>${outcomeBadge(resolution.outcome)}</div>` +
    callout(
      `结论：${outcomeLabel(resolution.outcome)}`,
      `预览不占槽，也不代表稍后仍可接单；实际派发必须重验容量、授权与配置代际。`,
      resolution.outcome === "ready" ? "ok" : resolution.outcome === "temporarily_saturated" ? "warn" : "danger",
    ) +
    primaryCallout +
    previewFindings(resolution) +
    resolutionDetails(resolution)
  );
}

export function emptyPreview() {
  return callout("还没有预览", "选择 Issue 与 Agent 后运行只读预览；它和真实派发共用同一解析器。", "muted");
}

function emptyReadiness() {
  return callout("还没有检查", "选择一个 Agent 并填写 Issue 后运行只读检查；它不占槽，也不改变绑定。", "muted");
}

export function renderReadiness(report, context = {}) {
  if (!report) {
    return emptyReadiness();
  }
  const primary = report.outcome === "ready" ? null : primaryFinding(report);
  const primaryCallout = primary
    ? `<div class="call ${report.outcome === "temporarily_saturated" ? "warn" : "danger"}" role="alert"><strong>主阻因 · ${esc(primary.code || "finding")}</strong>
        <div>${esc(primary.message || "")}</div>
        <div class="row">${remediationAction(primary.remediation)}</div></div>`
    : report.outcome === "ready"
      ? `<div class="call ok" role="status"><strong>没有主阻因</strong><div>所需证据齐全，快照时有容量；实际派发仍会重验并原子预留。</div></div>`
      : `<div class="call ${report.outcome === "temporarily_saturated" ? "warn" : "danger"}" role="alert"><strong>未给出主阻因</strong><div>服务端没有返回主阻因；请按逐项证据和修复入口处理。</div></div>`;
  const rows = (Array.isArray(report.checks) ? report.checks : []).map((check) => [
    `${esc(check.label)}<small>${esc(check.key)}</small>`,
    checkBadge(check.status),
    esc(check.detail || "—"),
    stamp(check.evidence_at),
    remediationAction(check.remediation),
  ]);
  return (
    `<div class="between"><h2>按 Issue 检查就绪 · ${esc(report.agent_id || context.agentID || "")}</h2>${outcomeBadge(report.outcome)}</div>` +
    callout(
      `结论：${outcomeLabel(report.outcome)}`,
      `规则命中只说明环境选择，不代表就绪。只读快照，不认领、不注册、不下载、不占槽，也不改变绑定；并发变化后以派发重验为准。检查时点 ${stamp(report.checked_at)}。`,
      report.outcome === "ready" ? "ok" : report.outcome === "temporarily_saturated" ? "warn" : "danger",
    ) +
    primaryCallout +
    panel(
      "逐项证据与修复入口",
      `<p class="sub">每一项都保留自己的状态、证据时点和主阻因；这里不把单项问题合并成一个绿色总分。</p>` +
        table(["检查项", "状态", "证据或阻因", "证据时点", "修复入口"], rows, "没有检查项。"),
    ) +
    resolutionDetails(report)
  );
}
