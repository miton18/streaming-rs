// Layered left-to-right DAG layout and an SVG + HTML renderer updated in place.

import { h, text, attr, toggle, setTag, mbar, setMbar, rate, pct, workersLabel, stateTone, icon } from './util.js';

export const NODE_W = 150;
export const NODE_H = 128;
const GAP_X = 40;
const GAP_Y = 20;
const DUMMY_H = 18;
const MIN_SCALE = 0.8;

// ------------------------------------------------------------------ layout

/**
 * ids: node ids in spec order; edges: [from, to].
 * Returns { pos: Map(id -> {x, y}), routes: Map("from\u0000to" -> [{x,y}...] centres of dummies), width, height }.
 */
export function layout(ids, edges) {
  const succ = new Map(ids.map((id) => [id, []]));
  const pred = new Map(ids.map((id) => [id, []]));
  const known = new Set(ids);
  const E = edges.filter(([a, b]) => known.has(a) && known.has(b) && a !== b);
  for (const [a, b] of E) { succ.get(a).push(b); pred.get(b).push(a); }

  // topological order (Kahn, spec order as tie-break); a cycle is broken by appending the rest
  const indeg = new Map(ids.map((id) => [id, pred.get(id).length]));
  const order = [];
  const queue = ids.filter((id) => indeg.get(id) === 0);
  const done = new Set();
  while (order.length < ids.length) {
    if (queue.length === 0) { const rest = ids.find((id) => !done.has(id)); queue.push(rest); }
    const n = queue.shift();
    if (done.has(n)) continue;
    done.add(n); order.push(n);
    for (const s of succ.get(n)) { indeg.set(s, indeg.get(s) - 1); if (indeg.get(s) === 0) queue.push(s); }
  }
  const rank = new Map(order.map((id, i) => [id, i]));
  const fwd = E.filter(([a, b]) => rank.get(a) < rank.get(b));

  // 1. longest-path layering from the sources
  const layer = new Map(ids.map((id) => [id, 0]));
  for (const n of order) for (const [a, b] of fwd) if (a === n) layer.set(b, Math.max(layer.get(b), layer.get(a) + 1));
  // 2. pull nodes towards their successors (shortens long edges from late-joining branches)
  for (let i = order.length - 1; i >= 0; i -= 1) {
    const n = order[i];
    const outs = fwd.filter(([a]) => a === n).map(([, b]) => layer.get(b));
    if (outs.length) { const target = Math.min(...outs) - 1; if (target > layer.get(n)) layer.set(n, target); }
  }

  // 3. dummy nodes for edges spanning several layers
  const items = new Map(); // id -> {id, layer, dummy, h}
  for (const id of ids) items.set(id, { id, layer: layer.get(id), dummy: false, h: NODE_H });
  const chains = []; // {from, to, path: [ids]}
  const up = new Map(ids.map((id) => [id, []]));
  const down = new Map(ids.map((id) => [id, []]));
  let dn = 0;
  for (const [a, b] of fwd) {
    const path = [a];
    for (let l = layer.get(a) + 1; l < layer.get(b); l += 1) {
      const d = `\u0001d${dn += 1}`;
      items.set(d, { id: d, layer: l, dummy: true, h: DUMMY_H });
      up.set(d, []); down.set(d, []);
      path.push(d);
    }
    path.push(b);
    for (let i = 0; i + 1 < path.length; i += 1) { down.get(path[i]).push(path[i + 1]); up.get(path[i + 1]).push(path[i]); }
    chains.push({ from: a, to: b, path });
  }

  const L = Math.max(0, ...[...items.values()].map((it) => it.layer)) + 1;
  let layers = Array.from({ length: L }, () => []);
  // initial order: DFS from the sources in spec order
  const seen = new Set();
  const visit = (n) => { if (seen.has(n)) return; seen.add(n); layers[items.get(n).layer].push(n); for (const s of down.get(n)) visit(s); };
  for (const id of order) if (up.get(id).length === 0) visit(id);
  for (const id of items.keys()) visit(id);

  // 4. crossing reduction: barycentric sweeps, keep the best ordering
  const crossings = (ls) => {
    let c = 0;
    for (let l = 0; l + 1 < ls.length; l += 1) {
      const idx = new Map(ls[l + 1].map((n, i) => [n, i]));
      const segs = [];
      ls[l].forEach((n, i) => { for (const s of down.get(n)) segs.push([i, idx.get(s)]); });
      for (let i = 0; i < segs.length; i += 1) for (let j = i + 1; j < segs.length; j += 1) {
        const [a1, b1] = segs[i]; const [a2, b2] = segs[j];
        if ((a1 - a2) * (b1 - b2) < 0) c += 1;
      }
    }
    return c;
  };
  const sortBy = (ls, l, nbrs, ref) => {
    const idx = new Map(ls[ref].map((n, i) => [n, i]));
    const cur = ls[l];
    const bary = new Map(cur.map((n, i) => {
      const ns = nbrs.get(n).filter((m) => idx.has(m));
      return [n, ns.length ? ns.reduce((s, m) => s + idx.get(m), 0) / ns.length : i];
    }));
    ls[l] = [...cur].sort((a, b) => bary.get(a) - bary.get(b) || cur.indexOf(a) - cur.indexOf(b));
  };
  let best = layers.map((x) => [...x]);
  let bestC = crossings(best);
  for (let it = 0; it < 10 && bestC > 0; it += 1) {
    for (let l = 1; l < L; l += 1) sortBy(layers, l, up, l - 1);
    for (let l = L - 2; l >= 0; l -= 1) sortBy(layers, l, down, l + 1);
    const c = crossings(layers);
    if (c < bestC) { bestC = c; best = layers.map((x) => [...x]); }
  }
  layers = best;

  // 5. vertical placement: align with neighbours, keep order and spacing
  const y = new Map();
  for (const ls of layers) { let acc = 0; for (const n of ls) { const it = items.get(n); y.set(n, acc + it.h / 2); acc += it.h + GAP_Y; } }
  const place = (ls, nbrs) => {
    const desired = ls.map((n) => { const ns = nbrs.get(n); return ns.length ? ns.reduce((s, m) => s + y.get(m), 0) / ns.length : y.get(n); });
    const pos = [];
    ls.forEach((n, i) => {
      const it = items.get(n);
      let v = desired[i];
      if (i > 0) { const pn = ls[i - 1]; const min = pos[i - 1] + items.get(pn).h / 2 + GAP_Y + it.h / 2; if (v < min) v = min; }
      pos.push(v);
    });
    // shift the block so that it is, on average, where its neighbours want it
    const shift = desired.reduce((s, d, i) => s + d - pos[i], 0) / (ls.length || 1);
    ls.forEach((n, i) => y.set(n, pos[i] + shift));
  };
  for (let pass = 0; pass < 6; pass += 1) {
    for (let l = 1; l < L; l += 1) place(layers[l], up);
    for (let l = L - 2; l >= 0; l -= 1) place(layers[l], down);
  }
  for (let l = 1; l < L; l += 1) place(layers[l], up);

  let minY = Infinity; let maxY = -Infinity;
  for (const [n, it] of items) { minY = Math.min(minY, y.get(n) - it.h / 2); maxY = Math.max(maxY, y.get(n) + it.h / 2); }
  if (!Number.isFinite(minY)) { minY = 0; maxY = NODE_H; }
  const pos = new Map();
  for (const id of ids) pos.set(id, { x: layer.get(id) * (NODE_W + GAP_X), y: y.get(id) - NODE_H / 2 - minY });
  const routes = new Map();
  for (const ch of chains) {
    routes.set(`${ch.from}\u0000${ch.to}`, ch.path.slice(1, -1).map((d) => ({ x: items.get(d).layer * (NODE_W + GAP_X), y: y.get(d) - minY })));
  }
  return { pos, routes, width: L * NODE_W + (L - 1) * GAP_X, height: maxY - minY, layers: L };
}

/** SVG path through the port points, horizontal tangents at every point. */
function edgePath(a, b, via) {
  const pts = [{ x: a.x + NODE_W, y: a.y + NODE_H / 2 }];
  for (const d of via) { pts.push({ x: d.x, y: d.y }); pts.push({ x: d.x + NODE_W, y: d.y }); }
  pts.push({ x: b.x - 8, y: b.y + NODE_H / 2 });
  let d = `M${pts[0].x},${pts[0].y}`;
  for (let i = 1; i < pts.length; i += 1) {
    const p = pts[i - 1]; const q = pts[i];
    if (i % 2 === 0 && via.length && i < pts.length - 1) { d += ` L${q.x},${q.y}`; continue; }
    const mx = (q.x - p.x) / 2;
    d += ` C${p.x + mx},${p.y} ${q.x - mx},${q.y} ${q.x},${q.y}`;
  }
  const p = pts[pts.length - 2]; const q = pts[pts.length - 1];
  return { d, label: { x: (p.x + q.x) / 2, y: (p.y + q.y) / 2 } };
}

const tone = (fill) => (fill >= 0.85 ? 'hi' : fill >= 0.5 ? 'mid' : 'lo');

// ------------------------------------------------------------------ renderer

export class DagView {
  constructor(onSelect) {
    this.onSelect = onSelect;
    this.mode = 'fit';
    this.sig = '';
    this.nodes = new Map();
    this.edges = new Map();
    this.scroller = h('div', { class: 'dag-scroll', onscroll: () => this.edges_() },
      this.stage = h('div', { class: 'dag-stage' },
        this.inner = h('div', { class: 'dag-inner' },
          this.svg = h('svg:svg', { class: 'dag-edges' }),
          this.labels = h('div'),
          this.cards = h('div', { role: 'list', 'aria-label': 'Nœuds du graphe' }))));
    this.hint = h('div', { class: 'dag-hint', hidden: true }, 'Faire défiler →');
    this.el = h('div', { class: 'dag-wrap' }, this.scroller, this.hint);
    const defs = h('svg:defs');
    for (const t of ['lo', 'mid', 'hi']) {
      defs.appendChild(h('svg:marker', { id: `arrow-${t}`, viewBox: '0 0 10 10', refX: '1', refY: '5', markerWidth: '10', markerHeight: '10', orient: 'auto', markerUnits: 'userSpaceOnUse' },
        h('svg:path', { d: 'M0,0 L10,5 L0,10 z', fill: t === 'lo' ? '#6c7589' : t === 'mid' ? '#b98b3e' : '#e0a33a' })));
    }
    this.svg.appendChild(defs);
    this.edgeLayer = h('svg:g');
    this.svg.appendChild(this.edgeLayer);
    this.ro = new ResizeObserver(() => this.fit());
    this.ro.observe(this.scroller);
  }

  destroy() { this.ro.disconnect(); }

  setMode(mode) { this.mode = mode; this.fit(); }

  fit() {
    if (!this.lay) return;
    const cs = getComputedStyle(this.scroller);
    const avail = this.scroller.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight);
    let s = 1;
    if (this.mode === 'fit') s = Math.max(MIN_SCALE, Math.min(1, avail / this.lay.width));
    this.scale = s;
    const w = `${Math.ceil(this.lay.width * s)}px`; const hh = `${Math.ceil(this.lay.height * s + 8)}px`;
    if (this.stage.style.width !== w) this.stage.style.width = w;
    if (this.stage.style.height !== hh) this.stage.style.height = hh;
    const tr = `scale(${s})`;
    if (this.inner.style.transform !== tr) this.inner.style.transform = tr;
    this.edges_();
  }

  /** Fade the sides that hide part of the diagram. */
  edges_() {
    const el = this.scroller;
    const max = el.scrollWidth - el.clientWidth;
    toggle(this.el, 'can-left', el.scrollLeft > 2);
    toggle(this.el, 'can-right', max - el.scrollLeft > 2);
    this.hint.hidden = !(max > 2 && el.scrollLeft < 4);
  }

  /** detail: the graph document from the API. */
  update(detail, selected) {
    const nodes = detail.nodes || [];
    const specEdges = (detail.edges && detail.edges.length ? detail.edges.map((e) => [e.from, e.to]) : (detail.spec?.edges || []));
    const ids = nodes.map((n) => n.id);
    const sig = `${ids.join('|')}#${specEdges.map((e) => e.join('>')).join('|')}`;
    if (sig !== this.sig) this.rebuild(ids, specEdges, sig);

    const byId = new Map(nodes.map((n) => [n.id, n]));
    for (const [id, card] of this.nodes) {
      const n = byId.get(id);
      if (n) this.updateNode(card, n, id === selected);
    }
    const fills = new Map((detail.edges || []).map((e) => [`${e.from}\u0000${e.to}`, e]));
    for (const [k, e] of this.edges) {
      const info = fills.get(k);
      const fill = Math.max(0, Math.min(1, info?.fill ?? 0));
      const t = tone(fill);
      const w = (1.4 + fill * 4.2).toFixed(2);
      attr(e.path, 'class', `e ${t}`);
      attr(e.path, 'stroke-width', w);
      attr(e.path, 'marker-end', `url(#arrow-${t})`);
      const dst = byId.get(e.to);
      const q = dst && dst.queue_cap ? ` (${dst.queue_len}/${dst.queue_cap})` : '';
      text(e.title, `${e.from} → ${e.to} · file d'entrée de ${e.to} : ${pct(fill)}${q}`);
      const showLabel = fill >= 0.05;
      e.label.hidden = !showLabel;
      if (showLabel) { text(e.label, pct(fill)); attr(e.label, 'class', `elabel ${t}`); }
    }
  }

  rebuild(ids, edges, sig) {
    this.sig = sig;
    this.lay = layout(ids, edges);
    this.cards.replaceChildren();
    this.labels.replaceChildren();
    this.edgeLayer.replaceChildren();
    this.nodes.clear(); this.edges.clear();
    const { pos, routes } = this.lay;
    this.inner.style.width = `${this.lay.width}px`;
    this.inner.style.height = `${this.lay.height}px`;
    attr(this.svg, 'width', this.lay.width); attr(this.svg, 'height', this.lay.height);
    for (const [a, b] of edges) {
      const pa = pos.get(a); const pb = pos.get(b);
      if (!pa || !pb) continue;
      const key = `${a}\u0000${b}`;
      const { d, label } = edgePath(pa, pb, routes.get(key) || []);
      const title = h('svg:title');
      const hit = h('svg:path', { d, class: 'hit' }, title);
      const path = h('svg:path', { d, class: 'e lo', 'stroke-width': '1.4', 'marker-end': 'url(#arrow-lo)' });
      this.edgeLayer.append(path, hit);
      const lab = h('div', { class: 'elabel', style: { left: `${label.x}px`, top: `${label.y}px` }, hidden: true });
      this.labels.appendChild(lab);
      this.edges.set(key, { from: a, to: b, path, title, label: lab });
    }
    for (const id of ids) {
      const p = pos.get(id);
      const card = this.makeNode(id);
      card.style.left = `${p.x}px`; card.style.top = `${p.y}px`;
      card.style.width = `${NODE_W}px`; card.style.height = `${NODE_H}px`;
      this.cards.appendChild(card);
      this.nodes.set(id, card);
    }
    this.fit();
  }

  makeNode(id) {
    const r = {};
    const card = h('button', { class: 'node', type: 'button', role: 'listitem', 'data-node': id, onclick: () => this.onSelect(id) },
      r.kick = h('div', { class: 'kick' }, r.kind = h('span'), ' · ', r.op = h('span', { class: 'op' })),
      h('div', { class: 'ttl' }, r.name = h('span', { class: 'name' }, id), r.pauseIc = icon('pause'), r.dot = h('span', { class: 'dot ok' }), r.tag = h('span')),
      r.bar = mbar(),
      h('div', { class: 'ratios' }, r.busy = h('span', { class: 't-busy' }), r.bp = h('span', { class: 't-bp' })),
      h('div', { class: 'rates' },
        h('span', null, h('i', null, 'in'), r.rin = h('b', { style: { fontWeight: 500 } })),
        h('span', null, h('i', null, 'out'), r.rout = h('b', { style: { fontWeight: 500 } }))),
      r.acts = h('div', { class: 'acts' }));
    r.pauseIc.setAttribute('width', '14'); r.pauseIc.setAttribute('height', '14');
    r.pauseIc.style.color = 'var(--color-neutral-300)'; r.pauseIc.style.flex = 'none';
    card._r = r;
    return card;
  }

  updateNode(card, n, selected) {
    const r = card._r;
    text(r.kind, n.kind || 'inconnu');
    text(r.op, n.op);
    attr(r.name, 'title', n.id);
    const st = n.state || 'pending';
    // running is the normal case: a dot is enough; anything else gets a tag
    const quiet = st === 'running' && !n.bottleneck && !n.paused;
    if (n.bottleneck) setTag(r.tag, 'Goulot', 'danger');
    else if (n.paused) setTag(r.tag, 'paused', 'outline');
    else setTag(r.tag, st, stateTone(st));
    r.tag.hidden = quiet;
    r.dot.hidden = !quiet;
    attr(r.dot, 'title', quiet ? 'running' : null);
    r.pauseIc.style.display = n.paused ? '' : 'none';
    setMbar(r.bar, n.busy, n.idle, n.backpressured);
    toggle(r.bar, 'off', st !== 'running' && st !== 'draining');
    text(r.busy, `busy ${pct(n.busy)}`);
    text(r.bp, `bp ${pct(n.backpressured)}`);
    r.bp.style.visibility = n.backpressured >= 0.005 ? '' : 'hidden';
    const isSource = n.kind === 'source';
    text(r.rin, isSource ? '–' : rate(n.in_per_sec));
    text(r.rout, rate(n.out_per_sec));
    const acts = `${n.actors || 0} acteur${(n.actors || 0) > 1 ? 's' : ''} · ${workersLabel(n.workers)}`;
    text(r.acts, acts);
    attr(r.acts, 'title', (n.workers || []).join(', '));
    toggle(card, 'selected', selected);
    toggle(card, 'bottleneck', n.bottleneck);
    toggle(card, 'paused', n.paused || st === 'paused');
    toggle(card, 'pending', st === 'pending' || st === 'stopped');
    toggle(card, 'failed', st === 'failed');
    attr(card, 'aria-pressed', selected ? 'true' : 'false');
    attr(card, 'aria-label', `${n.id}, ${n.kind || ''}, ${n.bottleneck ? 'goulot, ' : ''}${n.paused ? 'en pause, ' : ''}busy ${pct(n.busy)}, backpressure ${pct(n.backpressured)}, sortie ${rate(n.out_per_sec)}`);
  }

  /** Bring a node into view, taking an overlapping side panel into account. */
  scrollTo(id, overlay) {
    const c = this.nodes.get(id);
    if (!c || !this.scale) return;
    const el = this.scroller;
    const box = el.getBoundingClientRect();
    let visible = el.clientWidth;
    if (overlay) { const pl = overlay.getBoundingClientRect().left; if (pl < box.right) visible = Math.max(160, pl - box.left - 16); }
    const pad = parseFloat(getComputedStyle(el).paddingLeft) || 0;
    const stageLeft = this.stage.offsetLeft;
    const x = stageLeft + parseFloat(c.style.left) * this.scale;
    const w = NODE_W * this.scale;
    if (x < el.scrollLeft + pad || x + w > el.scrollLeft + visible - pad) {
      el.scrollLeft = Math.max(0, x + w / 2 - visible / 2);
    }
  }
}
