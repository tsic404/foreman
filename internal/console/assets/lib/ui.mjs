// Shared markup helpers for the console. Every value that reaches a template
// goes through esc(), so an upstream name is displayed, never executed.

const ESCAPES = {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"};

export function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ESCAPES[char]);
}

export function badge(label, tone = "") {
  return `<span class="badge ${esc(tone)}">${esc(label)}</span>`;
}

export function panel(title, body, actions = "", attrs = "") {
  return `<section class="panel" ${attrs}><div class="between"><h2>${esc(title)}</h2><div class="row">${actions}</div></div>${body}</section>`;
}

export function callout(title, body, tone = "") {
  return `<div class="call ${esc(tone)}" role="status"><strong>${esc(title)}</strong>${body === "" ? "" : `<div>${body}</div>`}</div>`;
}

export function emptyState(text) {
  return `<p class="empty">${esc(text)}</p>`;
}

export function table(headers, rows, emptyText) {
  const head = headers.map((header) => `<th>${esc(header)}</th>`).join("");
  const body = rows.length === 0
    ? `<tr><td class="empty" colspan="${headers.length}">${esc(emptyText)}</td></tr>`
    : rows.map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join("")}</tr>`).join("");
  return `<div class="scroll"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

export function definitionList(rows) {
  return `<dl>${rows.map(([key, value]) => `<dt>${esc(key)}</dt><dd>${esc(value)}</dd>`).join("")}</dl>`;
}

// stamp renders a wire timestamp. A missing fact stays visibly missing instead
// of becoming a 0001 date the reader would count as data.
export function stamp(value) {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return esc(value);
  }
  return esc(parsed.toISOString().replace(".000Z", "Z").replace("T", " "));
}

export function button(label, action, id = "", tone = "", extra = "") {
  return `<button type="button" class="${esc(tone)}" data-act="${esc(action)}" data-id="${esc(id)}" ${extra}>${esc(label)}</button>`;
}
