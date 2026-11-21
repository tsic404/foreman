// CLI release, Agent configuration and restricted discovery management page
// (04-architecture.md §3.5, §5, §7; 02-contracts V2-C04, V2-C05). The page
// renders only what the Go management surface returns. An unknown impact is
// never turned into zero, and an unsupported discovery adapter is never shown
// as an empty model list.

import { badge, button, callout, emptyState, esc, panel, stamp, table } from "./ui.mjs";

const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const DIGEST = /^[a-f0-9]{64}$/i;
const PLATFORM = /^(linux|darwin)\/(amd64|arm64)$/;

const KIND_LABELS = {
  multica: ["Multica CLI", ""],
  agent: ["Agent CLI", ""],
};

const RESULT_LABELS = {
  succeeded_nonempty: ["成功 · 非空", "ok"],
  succeeded_empty: ["成功 · 合法空列表", "ok"],
  unsupported: ["不支持 / 需配置", "warn"],
  failed: ["失败", "danger"],
};

const REQUEST_STATES = {
  queued: ["排队", "gray"],
  running: ["执行中", "warn"],
  completed: ["已完成", "ok"],
};

function trimmed(value) {
  return String(value ?? "").trim();
}

function integer(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function kindBadge(kind) {
  const [label, tone] = KIND_LABELS[kind] ?? ["未知类型", "unknown"];
  return badge(label, tone);
}

export function classBadge(resultClass) {
  const [label, tone] = RESULT_LABELS[resultClass] ?? ["结果未知", "unknown"];
  return badge(label, tone);
}

function requestStateBadge(state) {
  const [label, tone] = REQUEST_STATES[state] ?? ["状态未知", "unknown"];
  return badge(label, tone);
}

function action(label, act, id, extra = "") {
  return `<button type="button" data-act="${esc(act)}" data-id="${esc(id)}" ${extra}>${esc(label)}</button>`;
}

function sourceOptions(sources, selected = "") {
  const rows = (Array.isArray(sources) ? sources : [])
    .map(
      (source) =>
        `<option value="${esc(source.server_id)}" ${source.server_id === selected ? "selected" : ""}>${esc(source.server_id)} · ${esc(source.url)}</option>`,
    )
    .join("");
  return rows || `<option value="">尚无已登记来源</option>`;
}

function platformText(platform) {
  if (!platform) {
    return "—";
  }
  if (typeof platform === "string") {
    return platform;
  }
  return [platform.os, platform.arch, platform.libc].filter(Boolean).join("/");
}

function definitionRow(definition) {
  const current = definition.current_version
    ? `${esc(definition.current_version)}<small>第 ${Number(definition.current_generation ?? 0)} 代</small>`
    : badge("未发布", "gray");
  const candidates = (definition.candidates ?? []).map((version) => esc(version)).join("、") || "—";
  const switches = `${definition.auto_check ? badge("定时检查", "ok") : badge("检查关闭", "gray")} ${
    definition.auto_release ? badge("自动发布", "ok") : badge("自动发布关闭", "gray")
  }<small>每 ${Number(definition.check_period_s ?? 0)}s</small>`;
  const lastCheck = definition.last_check_at ? stamp(definition.last_check_at) : "—";
  const lastError = definition.last_check_error ? `<span class="error">${esc(definition.last_check_error)}</span>` : "—";
  const impactVersion = definition.current_version || definition.candidates?.[0] || "";
  const impact = impactVersion
    ? action("影响预览", "cli-impact", definition.cli_id, `data-version="${esc(impactVersion)}" data-generation="${Number(definition.check_generation ?? 0)}" data-release-generation="${Number(definition.current_generation ?? 0)}"`)
    : `<button type="button" disabled>影响预览</button>`;
  return [
    `<strong>${esc(definition.cli_id)}</strong><small>${esc(definition.server_id)}</small>`,
    `${kindBadge(definition.kind)}${definition.adapter ? `<small>${esc(definition.adapter)}</small>` : ""}`,
    esc((definition.required_platforms ?? []).join("、") || "—"),
    current,
    candidates,
    switches,
    `${lastCheck}<small>${lastError}</small>`,
    `${action("检查", "cli-check", definition.cli_id)} ${action("设置", "cli-settings", definition.cli_id)} ${action("登记版本", "cli-version", definition.cli_id)} ${action("发布", "cli-publish", definition.cli_id)} ${impact}`,
  ];
}

export function renderCLIDefinitions(definitions) {
  const rows = (Array.isArray(definitions) ? definitions : []).map(definitionRow);
  return panel(
    "CLI 发布线",
    `<p class="sub">按发布域、平台和类型分开管理。定时检查与自动发布是两个独立开关；检查关闭时仍可手动登记版本并发布。发布只在所需平台全部验证成功后切换指针。</p>` +
      table(
        ["CLI / 发布域", "类型 / 适配器", "必需平台", "当前版本", "候选", "检查 / 自动发布", "最近检查", "操作"],
        rows,
        "尚未登记任何 CLI。",
      ),
    `<button type="button" class="primary" data-act="add-cli">＋ 登记 CLI</button>`,
  );
}

export function renderCLIAudit(audit) {
  const rows = (Array.isArray(audit) ? audit : []).map((entry) => [
    stamp(entry.at),
    esc(entry.actor),
    esc(entry.action),
    `${esc(entry.cli_id)}${entry.version ? `<small>${esc(entry.version)}</small>` : ""}`,
    esc(entry.result),
    entry.impact_known ? `${entry.impact_agents === null ? "未知" : Number(entry.impact_agents ?? 0)}` : "未知",
    esc(entry.detail || "—"),
  ]);
  return panel(
    "CLI 发布审计",
    table(["时间", "操作者", "动作", "CLI / 版本", "结果", "影响 Agent", "说明"], rows, "尚无发布审计。"),
  );
}

export function definitionForm(sources) {
  return `<form id="cli-definition-form" class="fields">
    <label>CLI ID<input name="cli_id" required placeholder="omp" autocomplete="off"></label>
    <label>类型<select name="kind"><option value="agent">Agent CLI</option><option value="multica">Multica CLI</option></select></label>
    <label>发布域（Multica Server）<select name="server_id" required>${sourceOptions(sources)}</select></label>
    <label>适配器（Agent CLI 必填）<input name="adapter" placeholder="omp" autocomplete="off"></label>
    <label>必需平台（逗号分隔）<input name="required_platforms" required placeholder="linux/amd64, linux/arm64" autocomplete="off"></label>
    <label>最低版本（可留空）<input name="minimum_version" placeholder="1.0.0" autocomplete="off"></label>
    <label>版本清单 URL（可留空，仅手动登记）<input name="manifest_url" placeholder="https://mirror.example.test/manifest.json" autocomplete="off"></label>
    <label>检查周期（秒，≥30）<input name="check_period_s" type="number" min="30" value="21600"></label>
    <label>定时检查<select name="auto_check"><option value="off">关闭</option><option value="on">开启</option></select></label>
    <label>自动发布<select name="auto_release"><option value="off">关闭</option><option value="on">开启</option></select></label>
    <div class="form-actions"><button type="submit" class="primary">登记 CLI</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">制品 URL 只接受绝对 http(s) 地址，且不得携带凭据。每个平台分别登记，不能用 amd64 文件冒充 arm64。</p>
  </form>`;
}

export function cliSettingsForm(definition) {
  return `<form id="cli-settings-form" class="fields" data-id="${esc(definition.cli_id)}">
    <label>定时检查<select name="auto_check"><option value="on" ${definition.auto_check ? "selected" : ""}>开启</option><option value="off" ${definition.auto_check ? "" : "selected"}>关闭，仅手动</option></select></label>
    <label>检查周期（秒，≥30）<input name="check_period_s" type="number" min="30" value="${Number(definition.check_period_s ?? 21600)}"></label>
    <label>自动发布<select name="auto_release"><option value="on" ${definition.auto_release ? "selected" : ""}>开启</option><option value="off" ${definition.auto_release ? "" : "selected"}>关闭</option></select></label>
    <div class="form-actions"><button type="submit" class="primary">保存设置</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">关闭定时检查不会关闭手动检查或手动发布；自动发布只在候选版本高于当前版本时升级。</p>
  </form>`;
}

export function versionForm(definition) {
  const platforms = definition.required_platforms ?? [];
  const fields = platforms
    .map(
      (platform) => `<fieldset data-platform="${esc(platform)}">
        <legend>${esc(platform)}</legend>
        <label>下载 URL<input name="url" required placeholder="https://mirror.example.test/artifact" autocomplete="off"></label>
        <label>SHA-256<input name="sha256" required placeholder="64 位十六进制" autocomplete="off"></label>
        <label>大小（字节，可留空）<input name="size" type="number" min="0"></label>
      </fieldset>`,
    )
    .join("");
  if (platforms.length === 0) {
    return callout("没有必需平台", "该 CLI 定义没有平台，无法登记版本。", "warn");
  }
  return `<form id="cli-version-form" class="fields" data-id="${esc(definition.cli_id)}">
    <label>版本<input name="version" required placeholder="1.0.0" autocomplete="off"></label>
    ${fields}
    <div class="form-actions"><button type="submit" class="primary">登记候选版本</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">候选登记不改变发布指针；发布时仍会重新校验每个平台的摘要、大小与可执行文件架构。</p>
  </form>`;
}

export function publishForm(definition) {
  const candidates = definition.candidates ?? [];
  const options = candidates.map((version) => `<option value="${esc(version)}"></option>`).join("");
  const listID = `cli-candidates-${definition.cli_id}`;
  return `<form id="cli-publish-form" class="fields" data-id="${esc(definition.cli_id)}" data-generation="${Number(definition.check_generation ?? 0)}" data-release-generation="${Number(definition.current_generation ?? 0)}">
    <label>目标版本<input name="version" required list="${esc(listID)}" value="${esc(definition.current_version || candidates[0] || "")}" autocomplete="off"><datalist id="${esc(listID)}">${options}</datalist></label>
    <div class="call muted" role="status"><strong>原子发布</strong><div>所需平台全部验证成功才切换；失败保持 ${esc(definition.current_version || "当前无发布指针")}。回滚是发布另一个已验证版本，在途尝试仍使用启动时冻结的版本。</div></div>
    <div class="form-actions"><button type="submit" class="primary">验证并发布</button><button type="button" data-act="cli-impact" data-id="${esc(definition.cli_id)}" data-generation="${Number(definition.check_generation ?? 0)}" data-release-generation="${Number(definition.current_generation ?? 0)}">影响预览</button><button type="button" data-act="close-dialog">取消</button></div>
  </form>`;
}

export function validateDefinition(values) {
  const errors = {};
  const cliID = trimmed(values.cli_id);
  if (!ID.test(cliID)) {
    errors.cli_id = "1–64 位字母、数字、点、下划线或连字符，且以字母或数字开头。";
  }
  const kind = trimmed(values.kind);
  if (kind !== "agent" && kind !== "multica") {
    errors.kind = "类型必须是 Agent CLI 或 Multica CLI。";
  }
  const serverID = trimmed(values.server_id);
  if (!ID.test(serverID)) {
    errors.server_id = "选择已登记的发布域。";
  }
  const adapter = trimmed(values.adapter);
  if (kind === "agent" && !ID.test(adapter)) {
    errors.adapter = "Agent CLI 需要裸适配器名。";
  }
  const parsed = parsePlatformList(trimmed(values.required_platforms));
  if (parsed.error) {
    errors.required_platforms = parsed.error;
  }
  const minimumVersion = trimmed(values.minimum_version);
  const manifestURL = trimmed(values.manifest_url);
  if (manifestURL !== "" && !validHTTPURL(manifestURL)) {
    errors.manifest_url = "需要不带凭据的绝对 http(s) URL。";
  }
  const period = integer(values.check_period_s);
  if (period === null || period < 30) {
    errors.check_period_s = "检查周期需要 ≥30 秒的整数。";
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: {
      cli_id: cliID,
      kind,
      adapter: kind === "agent" ? adapter : "",
      server_id: serverID,
      required_platforms: parsed.platforms,
      minimum_version: minimumVersion,
      manifest_url: manifestURL,
      auto_check: values.auto_check !== "off",
      check_period_s: period,
      auto_release: values.auto_release !== "off",
    },
  };
}

export function validateCLISettings(values) {
  const errors = {};
  const period = integer(values.check_period_s);
  if (period === null || period < 30) {
    errors.check_period_s = "检查周期需要 ≥30 秒的整数。";
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: {
      auto_check: values.auto_check !== "off",
      check_period_s: period,
      auto_release: values.auto_release !== "off",
    },
  };
}

export function validateVersion(version, artifacts) {
  const errors = {};
  const value = trimmed(version);
  if (value === "") {
    errors.version = "版本不能为空。";
  }
  if (!Array.isArray(artifacts) || artifacts.length === 0) {
    errors.artifacts = "至少登记一个平台制品。";
  }
  const seen = new Set();
  for (const artifact of artifacts ?? []) {
    const platform = `${artifact?.platform?.os ?? ""}/${artifact?.platform?.arch ?? ""}`;
    if (!PLATFORM.test(platform)) {
      errors.artifacts = `平台 ${platform || "（空）"} 不受支持。`;
      continue;
    }
    if (seen.has(platform)) {
      errors.artifacts = `平台 ${platform} 重复登记。`;
    }
    seen.add(platform);
    if (!validHTTPURL(trimmed(artifact.url))) {
      errors[`url_${platform}`] = "需要不带凭据的绝对 http(s) URL。";
    }
    if (!DIGEST.test(trimmed(artifact.sha256))) {
      errors[`sha256_${platform}`] = "需要 64 位十六进制 SHA-256。";
    }
    if (artifact.size !== undefined && artifact.size !== null && (!Number.isInteger(artifact.size) || artifact.size < 0)) {
      errors[`size_${platform}`] = "大小需要非负整数或留空。";
    }
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: {
      version: value,
      artifacts: artifacts.map((artifact) => ({
        platform: {
          os: String(artifact.platform.os).toLowerCase(),
          arch: String(artifact.platform.arch).toLowerCase(),
        },
        url: trimmed(artifact.url),
        sha256: trimmed(artifact.sha256).toLowerCase(),
        size: Number.isInteger(artifact.size) ? artifact.size : 0,
      })),
    },
  };
}

export function validateConfig(values) {
  const errors = {};
  const serverID = trimmed(values.server_id);
  const workspaceID = trimmed(values.workspace_id);
  const backend = trimmed(values.backend);
  const profile = trimmed(values.profile);
  for (const [name, value] of [
    ["server_id", serverID],
    ["workspace_id", workspaceID],
    ["backend", backend],
  ]) {
    if (!ID.test(value)) {
      errors[name] = "需要 1–64 位字母、数字、点、下划线或连字符。";
    }
  }
  if (profile !== "" && !ID.test(profile)) {
    errors.profile = "profile 需要裸标识符。";
  }
  const secretRef = trimmed(values.secret_ref);
  if (!ID.test(secretRef)) {
    errors.secret_ref = "只填 Secret 引用，不填 Secret 值。";
  }
  const fileName = trimmed(values.file_name) || "config.json";
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\.json$/.test(fileName)) {
    errors.file_name = "只允许一个 .json 文件名。";
  }
  let config = null;
  const raw = String(values.config ?? "").trim();
  if (raw === "") {
    errors.config = "需要虚构 JSON 配置。";
  } else {
    try {
      config = JSON.parse(raw);
    } catch {
      errors.config = "配置不是合法 JSON。";
    }
    if (config === null || typeof config !== "object" || Array.isArray(config)) {
      errors.config = "配置必须是 JSON 对象。";
    }
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: { profile, file_name: fileName, secret_ref: secretRef, config },
  };
}

export function validateDiscovery(values) {
  const errors = {};
  const serverID = trimmed(values.server_id);
  const workspaceID = trimmed(values.workspace_id);
  const runtimeID = trimmed(values.runtime_id);
  for (const [name, value] of [
    ["server_id", serverID],
    ["workspace_id", workspaceID],
    ["runtime_id", runtimeID],
  ]) {
    if (!ID.test(value)) {
      errors[name] = "需要 1–128 位裸标识符。";
    }
  }
  const kind = trimmed(values.kind);
  if (kind !== "models" && kind !== "capabilities") {
    errors.kind = "类型必须是模型列表或能力。";
  }
  const cliID = trimmed(values.cli_id);
  const adapter = trimmed(values.adapter);
  if (cliID === "" && adapter === "") {
    errors.cli_id = "填写已登记的 CLI ID，或直接指定适配器。";
  }
  if (cliID !== "" && !ID.test(cliID)) {
    errors.cli_id = "CLI ID 需要裸标识符。";
  }
  if (adapter !== "" && !ID.test(adapter)) {
    errors.adapter = "适配器需要裸标识符。";
  }
  const platform = trimmed(values.platform).toLowerCase();
  if (platform !== "" && !PLATFORM.test(platform)) {
    errors.platform = "平台格式为 os/arch（linux|darwin / amd64|arm64）。";
  }
  const profile = trimmed(values.profile);
  if (profile !== "" && !ID.test(profile)) {
    errors.profile = "profile 需要裸标识符。";
  }
  let revision = 0;
  if (trimmed(values.config_revision) !== "") {
    const parsed = integer(values.config_revision);
    if (parsed === null || parsed < 0) {
      errors.config_revision = "配置修订需要非负整数或留空。";
    } else {
      revision = parsed;
    }
  }
  if (Object.keys(errors).length > 0) {
    return { errors, payload: null };
  }
  return {
    errors,
    payload: {
      server_id: serverID,
      workspace_id: workspaceID,
      runtime_id: runtimeID,
      profile,
      kind,
      cli_id: cliID,
      adapter: adapter === "" ? undefined : adapter,
      platform,
      config_revision: trimmed(values.config_revision) === "" ? undefined : revision,
    },
  };
}

export function publishOutcome(status, body) {
  const result = String(body?.result ?? "");
  const version = String(body?.version ?? "");
  const kept = String(body?.kept_version ?? "");
  const reasons = Array.isArray(body?.reasons) ? body.reasons.map((reason) => String(reason)) : [];
  if (status >= 200 && status < 300 && result === "published") {
    return {
      tone: "ok",
      title: `已发布 ${version} · 第 ${Number(body?.generation ?? 0)} 代`,
      detail: "发布指针已原子切换；下一次启动取得该版本，在途尝试仍使用原冻结版本。",
      reasons,
      status,
    };
  }
  if (status === 409 || result === "conflict") {
    return {
      tone: "warn",
      title: "候选集已变化，本次未切换",
      detail: kept ? `当前指针仍是 ${kept}；请重新读取影响预览后再提交。` : "请重新读取影响预览后再提交。",
      reasons,
      status,
    };
  }
  if (status === 422 || result === "failed") {
    return {
      tone: "danger",
      title: kept ? `验证未通过，旧指针保持 ${kept}` : "验证未通过，未建立发布指针",
      detail: "坏制品、缺失平台或摘要不符都不会晋升。",
      reasons,
      status,
    };
  }
  if (status === 404) {
    return { tone: "danger", title: "版本或 CLI 未登记", detail: "请先登记候选版本。", reasons, status };
  }
  if (status === 401) {
    return { tone: "danger", title: "凭据未被接受", detail: "请重新连接。", reasons, status };
  }
  return { tone: "danger", title: `发布未成功（HTTP ${status}）`, detail: "", reasons, status };
}

export function renderPublishOutcome(outcome) {
  const reasons = outcome.reasons.length
    ? `<ul>${outcome.reasons.map((reason) => `<li>${esc(reason)}</li>`).join("")}</ul>`
    : "";
  return `<div class="call ${esc(outcome.tone)}" role="alert" data-tone="${esc(outcome.tone)}"><strong>${esc(outcome.title)}</strong>${outcome.detail ? `<div>${esc(outcome.detail)}</div>` : ""}${reasons}</div>`;
}

export function checkOutcome(report) {
  const result = String(report?.result ?? "");
  const detail = String(report?.detail ?? "");
  if (result === "ok") {
    const publish = report?.publish ? renderPublishOutcome(publishOutcome(200, report.publish)) : "";
    return `<div class="call ok" role="status"><strong>检查完成 · 第 ${Number(report?.generation ?? 0)} 代</strong><div>候选 ${Number(report?.candidates ?? 0)} 个${detail ? ` · ${esc(detail)}` : ""}</div></div>${publish}`;
  }
  if (result === "no_source") {
    return `<div class="call warn" role="status"><strong>没有自动版本源</strong><div>${esc(detail || "只允许手动登记版本清单。")}</div></div>`;
  }
  if (result === "superseded") {
    return `<div class="call warn" role="status"><strong>检查结果已过期</strong><div>${esc(detail || "另一个检查先推进了候选集。")}</div></div>`;
  }
  return `<div class="call danger" role="alert"><strong>检查失败</strong><div>${esc(detail || "版本源不可读。")}</div></div>`;
}

export function impactHTML(impact) {
  if (!impact || impact.known !== true) {
    const reasons = (impact?.unknown ?? []).map((reason) => `<li>${esc(reason)}</li>`).join("");
    return `<div class="call warn" role="alert"><strong>影响范围未知</strong><div>目录陈旧、权限不足或运行事实缺失时，不会显示成零影响。</div>${reasons ? `<ul>${reasons}</ul>` : ""}</div>`;
  }
  const count = impact.affected_agents === null || impact.affected_agents === undefined ? "未知" : Number(impact.affected_agents);
  const agentRows = (impact.agents ?? []).map((agent) => [
    `${esc(agent.agent_name)}<small>${esc(agent.agent_id)}</small>`,
    esc(agent.workspace_name || agent.workspace_id),
    `${esc(agent.runtime_id)}${agent.adapter ? `<small>${esc(agent.adapter)}</small>` : ""}`,
    esc(agent.current_version || "—"),
  ]);
  const runRows = (impact.in_flight ?? []).map((run) => [
    `${esc(run.task_id)}<small>${esc(run.agent_name || run.agent_id)}</small>`,
    esc(run.workspace_id),
    run.version_known === true ? esc(run.version || "未发布") : badge("未知", "warn"),
    run.config_revision_known === true ? (run.config_revision ? String(run.config_revision) : "无配置") : badge("未知", "warn"),
    stamp(run.started_at),
  ]);
  return `<div class="call ok" role="status"><strong>下一次启动将取得 ${esc(impact.candidate_version)}</strong><div>${esc(impact.kind)} · ${esc(impact.cli_id)} · 当前 ${esc(impact.current_version || "未发布")}。影响 ${esc(count)} 个 Agent；在途尝试保持原版本，不热更新。</div></div>` +
    table(["Agent", "Workspace", "Runtime / 适配器", "当前版本"], agentRows, "没有下一次启动会取得该版本的 Agent。") +
    table(["在途任务", "Workspace", "冻结版本", "配置修订", "开始"], runRows, "没有仍在运行的旧版尝试。");
}

export function renderAgentConfigs(configs) {
  const rows = (Array.isArray(configs) ? configs : []).map((config) => {
    const key = JSON.stringify([config.server_id, config.workspace_id, config.backend, config.profile ?? ""]);
    return [
      `${esc(config.server_id)} / ${esc(config.workspace_id)}<small>${esc(config.backend)}${config.profile ? ` · ${esc(config.profile)}` : ""}</small>`,
      `第 ${Number(config.revision ?? 0)} 修订`,
      esc(config.secret_ref),
      `${esc(config.file_name)}<small>${esc(String(config.sha256 ?? "").slice(0, 12))} · ${Number(config.size ?? 0)} bytes</small>`,
      `${stamp(config.updated_at)}<small>${esc(config.updated_by || "")}</small>`,
      `${action("投影预览", "config-projection", key)} ${action("写新修订", "config-write", key)}`,
    ];
  });
  return panel(
    "Agent 配置修订",
    `<p class="sub">按 Server / Workspace / 后端 / profile 管理。只回显 Secret 引用、修订与摘要；原型只校验虚构 JSON 并记录修订，不保存内容、不创建真实 Secret。</p>` +
      table(["作用域", "修订", "Secret 引用", "文件 / 摘要", "更新", "操作"], rows, "尚无 Agent 配置修订。"),
    `<button type="button" class="primary" data-act="config-write" data-id="">＋ 写入配置</button>`,
  );
}

export function renderConfigAudit(audit) {
  const rows = (Array.isArray(audit) ? audit : []).map((entry) => [
    stamp(entry.at),
    esc(entry.actor),
    esc(entry.action),
    `${esc(entry.server_id)} / ${esc(entry.workspace_id)}<small>${esc(entry.backend)}${entry.profile ? ` · ${esc(entry.profile)}` : ""}</small>`,
    `第 ${Number(entry.revision ?? 0)} 修订`,
    esc(entry.result),
    esc(entry.detail || "—"),
  ]);
  return panel(
    "配置审计",
    table(["时间", "操作者", "动作", "作用域", "修订", "结果", "说明"], rows, "尚无配置审计。"),
  );
}

export function configForm(config = null) {
  const scope = config ?? {};
  return `<form id="agent-config-form" class="fields">
    <label>Multica Server<input name="server_id" required value="${esc(scope.server_id ?? "")}" placeholder="atlas" autocomplete="off"></label>
    <label>Workspace<input name="workspace_id" required value="${esc(scope.workspace_id ?? "")}" placeholder="core" autocomplete="off"></label>
    <label>后端<input name="backend" required value="${esc(scope.backend ?? "")}" placeholder="omp" autocomplete="off"></label>
    <label>profile（可留空）<input name="profile" value="${esc(scope.profile ?? "")}" placeholder="primary" autocomplete="off"></label>
    <label>Secret 引用<input name="secret_ref" required value="${esc(scope.secret_ref ?? "")}" placeholder="omp-primary-config" autocomplete="off"></label>
    <label>文件名<input name="file_name" value="${esc(scope.file_name ?? "config.json")}" autocomplete="off"></label>
    <label>虚构 JSON 配置<textarea name="config" rows="8" required placeholder='{"provider":"omp","region":"test"}'></textarea></label>
    <div class="form-actions"><button type="submit" class="primary">校验并记录修订</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">配置内容只用于校验和计算摘要，不会回显、写入审计或保存；也不会创建真实 Secret。运行投影只引用选定修订。</p>
  </form>`;
}

export function projectionForm(config) {
  return `<form id="projection-form" class="fields" data-server="${esc(config.server_id)}" data-workspace="${esc(config.workspace_id)}" data-backend="${esc(config.backend)}" data-profile="${esc(config.profile ?? "")}">
    <label>尝试 ID<input name="attempt_id" required placeholder="attempt-1" autocomplete="off"></label>
    <div class="form-actions"><button type="submit" class="primary">读取只读投影</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">每个尝试得到独立的只读文件名与 Secret 引用；已有尝试的投影不会被后续轮换改写。</p>
  </form>`;
}

export function projectionHTML(projection) {
  return `<div class="call ok" role="status"><strong>per-attempt 只读投影</strong><div>${esc(projection.mount_path)} · ${projection.read_only ? "只读" : "非只读"} · 第 ${Number(projection.revision ?? 0)} 修订</div></div>
    <dl>
      <dt>尝试</dt><dd>${esc(projection.attempt_id)}</dd>
      <dt>Secret 引用</dt><dd>${esc(projection.secret_ref)}</dd>
      <dt>Secret 名称</dt><dd>${esc(projection.secret_name)}</dd>
      <dt>文件</dt><dd>${esc(projection.file_name)}</dd>
      <dt>摘要</dt><dd>${esc(projection.sha256)}</dd>
    </dl>
    <p class="hint">原型只生成投影描述，不创建真实 Secret；部署的投影机制消费该描述。</p>`;
}

export function renderDiscovery(adapters, requests, models) {
  const adapterRows = (Array.isArray(adapters) ? adapters : []).map((adapter) => [
    esc(adapter.name),
    esc((adapter.kinds ?? []).join("、")),
    adapter.trusted ? badge("已验证", "ok") : badge("不支持 / 未验证", "warn"),
    esc(adapter.reason || "—"),
    `${stamp(adapter.verified_at)} ${action("验证", "verify-adapter", adapter.name)}`,
  ]);
  const requestRows = (Array.isArray(requests) ? requests : []).map((request) => {
    const result = request.result;
    return [
      `${esc(request.id)}<small>${esc(request.upstream_request_id || "无上游请求 ID")}</small>`,
      `${esc(request.server_id)} / ${esc(request.workspace_id)}<small>${esc(request.runtime_id)}${request.profile ? ` · ${esc(request.profile)}` : ""}</small>`,
      `${esc(request.kind)} · ${esc(request.adapter)}`,
      `${requestStateBadge(request.state)}${result ? ` ${classBadge(result.class)}` : ""}`,
      `第 ${Number(request.generation ?? 0)} 代${request.superseded ? ` ${badge("已过期", "warn")}` : ""}`,
      esc(result?.detail || "—"),
    ];
  });
  return (
    panel(
      "发现适配器",
      `<p class="sub">发现只在受限控制面执行，不创建探测 Pod、不占业务槽。首批只声明真正通过命令面验证的适配器；没有可信适配器时返回不支持。</p>` +
        table(["适配器", "类型", "信任", "原因", "最近验证 / 操作"], adapterRows, "尚未登记发现适配器。"),
      `<button type="button" data-act="verify-adapters">验证全部适配器</button>`,
    ) +
    panel(
      "发现请求",
      `<p class="sub">相同上游请求只执行一次；配置/CLI 代际变化后旧代晚到不覆盖新结果。失败不是空成功。</p>` +
        table(["请求", "环境", "类型 / 适配器", "状态", "代际", "说明"], requestRows, "尚无发现请求。"),
      `<button type="button" class="primary" data-act="discovery-request">＋ 发起发现</button>`,
    ) +
    panel(
      "模型目录",
      `<p class="sub">合法空列表与不支持/失败分开显示。没有可信适配器时，这里不会把不支持显示成空列表。</p>` +
        modelsFilterForm(models?.filter ?? {}) +
        modelCatalogHTML(models),
    )
  );
}

export function modelsFilterForm(filter = {}) {
  return `<form id="models-filter-form" class="fields">
    <label>Multica Server<input name="server_id" value="${esc(filter.server_id ?? "")}" placeholder="atlas" autocomplete="off"></label>
    <label>Workspace<input name="workspace_id" value="${esc(filter.workspace_id ?? "")}" placeholder="core" autocomplete="off"></label>
    <label>Runtime<input name="runtime_id" value="${esc(filter.runtime_id ?? "")}" placeholder="rt-omp" autocomplete="off"></label>
    <label>profile（可留空）<input name="profile" value="${esc(filter.profile ?? "")}" placeholder="primary" autocomplete="off"></label>
    <div class="form-actions"><button type="submit" class="primary">读取模型目录</button></div>
  </form>`;
}

function modelCatalogHTML(models) {
  if (!models || !models.view) {
    return emptyState("填写环境后读取模型目录。");
  }
  const view = models.view;
  const detail = view.detail ? `<div>${esc(view.detail)}</div>` : "";
  if (view.available === true) {
    const ids = (view.models ?? []).map((model) => esc(model.id)).join("、");
    if (view.class === "succeeded_empty") {
      return `<div class="call ok" role="status"><strong>成功 · 合法空列表</strong><div>该环境返回了合法空模型列表，不是执行失败。</div>${detail}</div>`;
    }
    return `<div class="call ok" role="status"><strong>成功 · 非空 · ${Number((view.models ?? []).length)} 个模型</strong><div>${ids}</div>${detail}</div>`;
  }
  if (view.class === "unsupported") {
    return `<div class="call warn" role="status"><strong>不支持 / 需配置</strong><div>没有可信适配器，或该后端不提供非交互枚举；这不是空模型列表。</div>${detail}</div>`;
  }
  if (view.class === "failed") {
    return `<div class="call danger" role="alert"><strong>发现失败</strong><div>执行、解析、认证或输出校验失败；不能当作空成功。</div>${detail}</div>`;
  }
  return `<div class="call unknown" role="status"><strong>尚无结果</strong><div>该环境还没有模型发现请求。</div></div>`;
}

export function discoveryForm(sources) {
  return `<form id="discovery-form" class="fields">
    <label>Multica Server<select name="server_id" required>${sourceOptions(sources)}</select></label>
    <label>Workspace<input name="workspace_id" required placeholder="core" autocomplete="off"></label>
    <label>注册 runtime<input name="runtime_id" required placeholder="rt-omp" autocomplete="off"></label>
    <label>profile（可留空）<input name="profile" placeholder="primary" autocomplete="off"></label>
    <label>类型<select name="kind"><option value="models">模型列表</option><option value="capabilities">能力</option></select></label>
    <label>已登记 CLI ID（可留空）<input name="cli_id" placeholder="omp" autocomplete="off"></label>
    <label>适配器（CLI ID 为空时必填）<input name="adapter" placeholder="omp" autocomplete="off"></label>
    <label>平台（可留空）<input name="platform" placeholder="linux/amd64" autocomplete="off"></label>
    <label>配置修订（可留空，自动读取）<input name="config_revision" type="number" min="0"></label>
    <div class="form-actions"><button type="submit" class="primary">执行一次发现</button><button type="button" data-act="close-dialog">取消</button></div>
    <p class="hint">命令在私有临时目录与受限环境执行，不启动业务 Job 或探测 Pod。omp 需要控制面可执行文件；codex/claude 在协议面验证前返回不支持。</p>
  </form>`;
}

export function renderCLIPage(model) {
  return (
    renderCLIDefinitions(model.definitions) +
    renderCLIAudit(model.audit) +
    renderAgentConfigs(model.configs) +
    renderConfigAudit(model.configAudit) +
    renderDiscovery(model.adapters, model.requests, model.models)
  );
}

function validHTTPURL(raw) {
  try {
    const parsed = new URL(raw);
    return (parsed.protocol === "http:" || parsed.protocol === "https:") && !parsed.username && !parsed.password && parsed.host !== "";
  } catch {
    return false;
  }
}

function parsePlatformList(raw) {
  if (raw === "") {
    return { platforms: null, error: "至少需要一个平台，例如 linux/amd64。" };
  }
  const platforms = [];
  const seen = new Set();
  for (const part of raw.split(",")) {
    const value = part.trim().toLowerCase();
    if (!PLATFORM.test(value)) {
      return { platforms: null, error: `平台 ${part.trim() || "（空）"} 不受支持；使用 linux|darwin / amd64|arm64。` };
    }
    if (seen.has(value)) {
      return { platforms: null, error: `平台 ${value} 重复。` };
    }
    seen.add(value);
    const [os, arch] = value.split("/");
    platforms.push({ os, arch });
  }
  return { platforms, error: "" };
}
