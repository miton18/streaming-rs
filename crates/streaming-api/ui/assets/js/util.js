// DOM helpers (create once, update in place) and French formatting.

const SVG_NS = 'http://www.w3.org/2000/svg';

/** Create an element. `tag` may be prefixed with `svg:`. */
export function h(tag, props, ...children) {
  let el;
  if (tag.startsWith('svg:')) el = document.createElementNS(SVG_NS, tag.slice(4));
  else el = document.createElement(tag);
  if (props) {
    for (const [k, v] of Object.entries(props)) {
      if (v == null || v === false) continue;
      if (k === 'class') el.setAttribute('class', v);
      else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
      else if (k === 'ref') v(el);
      else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2).toLowerCase(), v);
      else if (k === 'html') el.innerHTML = v;
      else if (k === 'value' && 'value' in el) el.value = v;
      else if (k === 'checked' || k === 'disabled' || k === 'selected') el[k] = !!v;
      else el.setAttribute(k, v === true ? '' : String(v));
    }
  }
  append(el, children);
  return el;
}

function append(el, children) {
  for (const c of children) {
    if (c == null || c === false) continue;
    if (Array.isArray(c)) append(el, c);
    else if (c instanceof Node) el.appendChild(c);
    else el.appendChild(document.createTextNode(String(c)));
  }
}

/** Set text only when it changed (no needless layout, selection survives). */
export function text(el, v) {
  const s = v == null ? '' : String(v);
  if (el.textContent !== s) el.textContent = s;
}

export function attr(el, k, v) {
  if (v == null || v === false) { if (el.hasAttribute(k)) el.removeAttribute(k); return; }
  const s = v === true ? '' : String(v);
  if (el.getAttribute(k) !== s) el.setAttribute(k, s);
}

export function setClass(el, cls) { if (el.getAttribute('class') !== cls) el.setAttribute('class', cls); }
export function toggle(el, cls, on) { el.classList.toggle(cls, !!on); }
export function show(el, on) { if (el.hidden === !!on) el.hidden = !on; }
export function width(el, ratio) {
  const w = `${Math.max(0, Math.min(1, ratio || 0)) * 100}%`;
  if (el.style.width !== w) el.style.width = w;
}

/**
 * Keyed reconciliation of the children of `parent` (which must contain only keyed children).
 * Existing elements are reused and updated; new ones are created; stale ones removed.
 */
export function syncList(parent, items, keyOf, create, update) {
  const map = parent._keyed || (parent._keyed = new Map());
  const seen = new Set();
  items.forEach((item, i) => {
    const k = keyOf(item, i);
    seen.add(k);
    let el = map.get(k);
    const fresh = !el;
    if (fresh) { el = create(item, i); map.set(k, el); }
    update(el, item, i, fresh);
    const at = parent.children[i];
    if (at !== el) parent.insertBefore(el, at || null);
  });
  for (const [k, el] of map) {
    if (!seen.has(k)) { el.remove(); map.delete(k); }
  }
}

/** Replace children only when the signature changed. */
export function once(el, sig, build) {
  if (el._sig === sig) return false;
  el._sig = sig;
  el.replaceChildren(...[].concat(build()).filter(Boolean));
  return true;
}

// ------------------------------------------------------------------ icons
// Minimal stroke icons (Phosphor-like geometry), inline so there is no network dependency.
const ICONS = {
  overview: '<rect x="3.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.5"/>',
  cluster: '<rect x="3.5" y="4" width="17" height="6" rx="1.5"/><rect x="3.5" y="14" width="17" height="6" rx="1.5"/><path d="M7 7h.01M7 17h.01"/>',
  catalog: '<path d="M4 5.5A1.5 1.5 0 0 1 5.5 4H10l2 2h6.5A1.5 1.5 0 0 1 20 7.5v11a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 4 18.5z"/>',
  tenant: '<path d="M4 20V8l8-4 8 4v12"/><path d="M9 20v-6h6v6"/>',
  graph: '<circle cx="6" cy="12" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="18" cy="18" r="2.5"/><path d="M8.3 11l7.4-4M8.3 13l7.4 4"/>',
  warn: '<path d="M12 4 2.8 19.5h18.4z"/><path d="M12 10v4.5M12 17.2h.01"/>',
  x: '<path d="M6 6l12 12M18 6 6 18"/>',
  pause: '<path d="M9 5.5v13M15 5.5v13"/>',
  play: '<path d="M7.5 5v14l11-7z"/>',
  copy: '<rect x="8.5" y="8.5" width="11" height="11" rx="2"/><path d="M15.5 8.5V6a1.5 1.5 0 0 0-1.5-1.5H6A1.5 1.5 0 0 0 4.5 6v8A1.5 1.5 0 0 0 6 15.5h2.5"/>',
  link: '<path d="M14 4.5h5.5V10M19.5 4.5 11 13M18 14v4.5A1.5 1.5 0 0 1 16.5 20h-11A1.5 1.5 0 0 1 4 18.5v-11A1.5 1.5 0 0 1 5.5 6H10"/>',
  trash: '<path d="M4.5 7h15M9.5 7V5h5v2M6.5 7l1 12.5h9l1-12.5"/>',
  search: '<circle cx="11" cy="11" r="6.5"/><path d="m16 16 4 4"/>',
  check: '<path d="m5 12.5 4.5 4.5L19 7.5"/>',
  spinner: '<path d="M12 3.5a8.5 8.5 0 1 0 8.5 8.5"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  target: '<circle cx="12" cy="12" r="7.5"/><circle cx="12" cy="12" r="2.5"/>',
  fit: '<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
  api: '<path d="M8.5 7 3.5 12l5 5M15.5 7l5 5-5 5M13.5 5l-3 14"/>',
  plug: '<path d="M9 3.5v5M15 3.5v5M6.5 8.5h11V12a5.5 5.5 0 0 1-11 0zM12 17.5v3"/>',
};
export function icon(name, cls) {
  const s = document.createElementNS(SVG_NS, 'svg');
  s.setAttribute('viewBox', '0 0 24 24');
  s.setAttribute('fill', 'none');
  s.setAttribute('stroke', 'currentColor');
  s.setAttribute('stroke-width', '1.8');
  s.setAttribute('stroke-linecap', 'round');
  s.setAttribute('stroke-linejoin', 'round');
  s.setAttribute('aria-hidden', 'true');
  if (cls) s.setAttribute('class', cls);
  s.innerHTML = ICONS[name] || '';
  return s;
}

// ------------------------------------------------------------------ formatting (fr-FR)
const nf0 = new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const nfp = new Intl.NumberFormat('fr-FR', { style: 'percent', maximumFractionDigits: 0 });
const DASH = '–';

export function int(n) { return n == null || Number.isNaN(n) ? DASH : nf0.format(n); }

/** 12 345 -> "12,3 k", 1 234 -> "1 234". */
export function compact(n) {
  if (n == null || Number.isNaN(n)) return DASH;
  const a = Math.abs(n);
  if (a >= 1e9) return `${nf1.format(n / 1e9)}\u202fG`;
  if (a >= 1e6) return `${nf1.format(n / 1e6)}\u202fM`;
  if (a >= 1e4) return `${nf1.format(n / 1e3)}\u202fk`;
  return nf0.format(n);
}

/** Records per second. */
export function rate(n) {
  if (n == null || Number.isNaN(n)) return DASH;
  const a = Math.abs(n);
  if (a === 0) return '0/s';
  if (a < 10) return `${nf1.format(n)}/s`;
  if (a < 1e5) return `${nf0.format(n)}/s`;
  return `${compact(n)}/s`;
}

export function pct(r) { return r == null || Number.isNaN(r) ? DASH : nfp.format(r); }

export function bytes(n) {
  if (n == null) return DASH;
  const u = ['o', 'Ko', 'Mo', 'Go', 'To'];
  let i = 0; let v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i += 1; }
  return `${i === 0 ? nf0.format(v) : nf1.format(v)}\u00a0${u[i]}`;
}

/** Human duration from milliseconds. */
export function dur(ms) {
  if (ms == null || Number.isNaN(ms)) return DASH;
  const neg = ms < 0; const a = Math.abs(ms);
  let s;
  if (a < 1000) s = `${nf0.format(a)}\u00a0ms`;
  else if (a < 10000) s = `${nf1.format(a / 1000)}\u00a0s`;
  else if (a < 60000) s = `${nf0.format(Math.round(a / 1000))}\u00a0s`;
  else if (a < 3600000) { const m = Math.floor(a / 60000); const sec = Math.round((a % 60000) / 1000); s = `${m}\u00a0min${sec ? ` ${sec}\u00a0s` : ''}`; }
  else if (a < 86400000) { const hh = Math.floor(a / 3600000); const m = Math.round((a % 3600000) / 60000); s = `${hh}\u00a0h${m ? ` ${String(m).padStart(2, '0')}\u00a0min` : ''}`; }
  else { const d = Math.floor(a / 86400000); const hh = Math.round((a % 86400000) / 3600000); s = `${d}\u00a0j${hh ? ` ${hh}\u00a0h` : ''}`; }
  return neg ? `-${s}` : s;
}

const tf = new Intl.DateTimeFormat('fr-FR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const df = new Intl.DateTimeFormat('fr-FR', { day: '2-digit', month: 'short' });
const full = new Intl.DateTimeFormat('fr-FR', { dateStyle: 'full', timeStyle: 'medium' });

export function time(ms) {
  if (ms == null) return DASH;
  const d = new Date(ms);
  const today = new Date();
  const t = tf.format(d);
  if (d.toDateString() === today.toDateString()) return t;
  return `${df.format(d)} ${t}`;
}
export function fullTime(ms) { return ms == null ? '' : full.format(new Date(ms)); }

export function ago(ms, now) {
  if (ms == null) return DASH;
  const d = now - ms;
  if (d < 1500) return "à l'instant";
  return `il y a ${dur(d)}`;
}

/** `streaming_core::data::Windowed<u64>` -> `Windowed<u64>`. */
export function shortType(t) {
  if (t == null) return DASH;
  if (t === '*') return '*';
  return t.replace(/\b(?:[A-Za-z_][A-Za-z0-9_]*::)+/g, '');
}

export function plural(n, one, many) { return `${int(n)}\u00a0${n > 1 ? many : one}`; }

/** "worker-1, worker-2" -> "worker-1/2". */
export function workersLabel(ws) {
  if (!ws || ws.length === 0) return 'aucun worker';
  const list = [...ws].sort((a, b) => a.localeCompare(b, 'fr', { numeric: true }));
  if (list.length === 1) return list[0];
  let p = list[0];
  for (const w of list) { while (p && !w.startsWith(p)) p = p.slice(0, -1); }
  const cut = p.search(/[^a-zA-Z_-]*$/);
  const prefix = p.slice(0, cut);
  if (prefix.length >= 2) return prefix + list.map((w) => w.slice(prefix.length)).join('/');
  return list.join(', ');
}

// ------------------------------------------------------------------ states
const STATE_TONE = {
  running: 'success', paused: 'outline', draining: 'accent', starting: 'accent',
  stopped: 'neutral', failed: 'danger', pending: 'outline', deleting: 'danger',
};
export function stateTone(s) { return STATE_TONE[s] || 'neutral'; }

/** Update a `.tag` element's label and tone in place. */
export function setTag(el, label, tone, extra = '') {
  setClass(el, `tag tag-${tone}${extra ? ` ${extra}` : ''}`);
  text(el, label);
}

export function stateTag(state, extra) {
  const el = h('span');
  setTag(el, state || 'pending', stateTone(state || 'pending'), extra);
  return el;
}

/** Metric bar: busy / idle / backpressured. */
export function mbar(cls = '') {
  const el = h('div', { class: `mbar ${cls}`, role: 'img' }, h('span', { class: 'b' }), h('span', { class: 'i' }), h('span', { class: 'p' }));
  return el;
}
export function setMbar(el, busy, idle, bp) {
  const [b, i, p] = el.children;
  const tot = (busy || 0) + (idle || 0) + (bp || 0);
  const k = tot > 1 ? 1 / tot : 1;
  width(b, (busy || 0) * k); width(i, (idle || 0) * k); width(p, (bp || 0) * k);
  attr(el, 'aria-label', `busy ${pct(busy)}, idle ${pct(idle)}, backpressure ${pct(bp)}`);
  attr(el, 'title', `busy ${pct(busy)} · idle ${pct(idle)} · backpressure ${pct(bp)}`);
}

/** JSON pretty print with minimal highlighting (as DOM-safe HTML). */
export function jsonHtml(v) {
  const esc = (s) => s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  const src = esc(JSON.stringify(v, null, 2));
  return src.replace(/("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g, (m, str, colon, kw, n) => {
    if (str) return colon ? `<span class="k">${str}</span>${colon}` : `<span class="s">${str}</span>`;
    if (kw) return `<span class="b">${kw}</span>`;
    return `<span class="n">${n}</span>`;
  });
}

export function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}
