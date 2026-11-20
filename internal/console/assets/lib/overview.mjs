// Overview page: the four counts and the in-flight attempts (04-architecture.md
// §2). Every number is shown with the basis it was counted on, so a reader can
// recompute it from the same source list instead of trusting a total.

import { badge, emptyState, esc, panel, stamp } from "./ui.mjs";

export const TILES = [
  { key: "servers", label: "已连接 Multica Server" },
  { key: "workspaces", label: "已对接 Workspace" },
  { key: "agents", label: "采用 Foreman 的 Agent" },
  { key: "environments", label: "运行环境" },
];

const CONNECTION_LABELS = {
  ok: ["已连接", "ok"],
  unverified: ["未验证", "warn"],
  failed: ["失联", "warn"],
  unauthorized: ["凭据撤销", "danger"],
};

const SYNC_LABELS = {
  ok: ["同步正常", "ok"],
  failed: ["同步失败", "danger"],
  never: ["尚未同步", "gray"],
};

export function connectionBadge(connection) {
  const [label, tone] = CONNECTION_LABELS[connection] ?? ["状态未知", "unknown"];
  return badge(label, tone);
}

export function syncBadge(sync) {
  const [label, tone] = SYNC_LABELS[sync] ?? ["状态未知", "unknown"];
  return badge(label, tone);
}

export function overviewModel(view) {
  const counts = view?.counts ?? {};
  const withForeman = counts.workspaces_with_foreman_agents ?? 0;
  // An unwired environment registry answers false. It is unknown, never zero:
  // a fabricated zero would claim there is no capacity (§2 生产对失联显示陈旧/
  // 未知，不用零代替).
  const environmentsAvailable = counts.environments_available === true;
  const tiles = [
    {
      key: "servers",
      label: TILES[0].label,
      display: String(counts.servers ?? 0),
      caption: "按稳定 Server ID 去重，仅计连接有效的来源",
      target: "#/sources",
    },
    {
      key: "workspaces",
      label: TILES[1].label,
      display: String(counts.workspaces ?? 0),
      caption: `按 (Server, Workspace) 去重；其中 ${withForeman} 个有 Foreman Agent`,
      target: "#/sources",
    },
    {
      key: "agents",
      label: TILES[2].label,
      display: String(counts.agents ?? 0),
      caption: "按 (Server, Workspace, Agent) 去重，仅计实际绑定 Foreman runtime 的 Agent",
      target: "#/agents",
    },
    {
      key: "environments",
      label: TILES[3].label,
      display: environmentsAvailable ? String(counts.environments ?? 0) : "未知",
      caption: environmentsAvailable
        ? "按稳定环境 ID 去重，含排空中的环境"
        : "环境登记尚未接入本部署；未知不以 0 表示",
      target: "#/environments",
    },
  ];

  const inProgress = view?.in_progress ?? {};
  const rows = Array.isArray(inProgress.rows) ? inProgress.rows : [];
  const keys = new Set(rows.map((row) => `${row.server_id}\u0000${row.workspace_id}\u0000${row.agent_id}`));
  return {
    tiles,
    inProgress: {
      available: inProgress.available === true,
      // One agent with two attempts is one agent and two rows: the two counts
      // are computed on different keys (UI-01).
      agentCount: keys.size,
      attemptCount: rows.length,
      rows,
    },
  };
}

function tilesHTML(tiles) {
  return `<div class="cards">${tiles
    .map(
      (tile, index) => `<a class="tile" href="${esc(tile.target)}" data-tile="${esc(tile.key)}">
        <div class="row"><span>${esc(tile.label)}</span><span class="icon">${index + 1}</span></div>
        <strong>${esc(tile.display)}</strong>
        <small>${esc(tile.caption)}</small>
      </a>`,
    )
    .join("")}</div>`;
}

export function attemptDetail(attempt) {
  const facts = [
    ["Multica Server", attempt.server_id],
    ["Workspace", attempt.workspace_id],
    ["Agent", `${attempt.agent_name ?? ""} / ${attempt.agent_id ?? ""}`],
    ["Issue", `${attempt.issue_identifier ?? ""} · ${attempt.issue_id ?? ""}`],
    ["远程执行节点", attempt.environment || "未记录"],
    ["状态", attempt.status],
    ["开始", stamp(attempt.started_at)],
  ];
  return `<dl>${facts.map(([key, value]) => `<dt>${esc(key)}</dt><dd>${esc(value)}</dd>`).join("")}</dl>`;
}

function attemptsHTML(inProgress) {
  if (!inProgress.available) {
    return `<div class="call" role="status"><strong>运行事实不可用</strong><div>协调器的运行事实尚未接入；这里不显示 0，也不表示没有任务。</div></div>`;
  }
  if (inProgress.rows.length === 0) {
    return emptyState("当前没有进行中的任务。");
  }
  const rows = inProgress.rows
    .map(
      (attempt, index) => `<tr class="run-row">
        <td><button type="button" class="who" data-act="run-detail" data-id="${index}" aria-describedby="run-detail-${index}">${esc(attempt.agent_name || attempt.agent_id)}</button><small>${esc(attempt.agent_id)}</small></td>
        <td>${esc(attempt.issue_identifier)}<small>${esc(attempt.issue_id)}</small></td>
        <td>${esc(attempt.environment || "未记录")}</td>
        <td>${badge(attempt.status || "state")}</td>
        <td>${stamp(attempt.started_at)}</td>
      </tr>
      <tr class="run-detail-row"><td colspan="5"><div class="run-detail" id="run-detail-${index}">${attemptDetail(attempt)}</div></td></tr>`,
    )
    .join("");
  return `<div class="scroll"><table class="runs"><thead><tr><th>Agent</th><th>当前 Issue</th><th>执行环境</th><th>状态</th><th>开始</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}

// The home page is the four counts and the in-progress list, nothing else: a
// source table here would compete with the list the operator came for
// (04-architecture.md §2 卡片下面只有当前进行中的列表).
export function renderOverview(view) {
  const model = overviewModel(view);
  const { inProgress } = model;
  const header = inProgress.available
    ? `${inProgress.agentCount} 个 Agent · ${inProgress.attemptCount} 条尝试`
    : "运行事实不可用";
  return (
    tilesHTML(model.tiles) +
    panel(
      `正在进行 · ${header}`,
      `<p class="sub">悬停、键盘聚焦或点选 Agent，查看来源、Workspace、Issue 与执行环境。结束后从首页移出。</p>${attemptsHTML(inProgress)}`,
      `<button type="button" data-act="refresh-overview">刷新</button>`,
    )
  );
}
