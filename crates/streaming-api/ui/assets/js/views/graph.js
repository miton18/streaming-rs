// Graph page: header + run state, bottleneck banner, DAG, node panel, tabs (slices, results, spec).

import {
  h, text, attr, toggle, show, syncList, setTag, setClass, stateTone, rate, int, pct, compact, time, fullTime, dur,
  shortType, icon, mbar, setMbar, jsonHtml, workersLabel,
} from '../util.js';
import { conn, paths, post, patch, del } from '../api.js';
import { DagView } from '../dag.js';
import { statCard, setStat } from './overview.js';

const RUN_STATES = [
  { v: 'running', label: 'Running' },
  { v: 'paused', label: 'Paused' },
  { v: 'draining', label: 'Draining' },
  { v: 'stopped', label: 'Stopped' },
];
const KV_SINK = 'std/kv-sink@1';

const isTsKey = (k) => /(^ts$|_ms$|^window_(start|end)$|_at$)/.test(k);
const looksTs = (v) => typeof v === 'number' && v > 1e12 && v < 1e13;

/** Compact one-line rendering of an output value. */
function valueNodes(v) {
  if (v === null || typeof v !== 'object' || Array.isArray(v)) return [h('span', null, typeof v === 'string' ? v : JSON.stringify(v))];
  const out = [];
  Object.entries(v).forEach(([k, x], i) => {
    if (i) out.push(h('span', { class: 'sep' }, '·'));
    let s;
    if ((isTsKey(k) && typeof x === 'number') || looksTs(x)) s = time(x);
    else if (typeof x === 'number') s = Number.isInteger(x) ? int(x) : String(Math.round(x * 1000) / 1000).replace('.', ',');
    else if (typeof x === 'string') s = x;
    else s = JSON.stringify(x);
    out.push(h('span', { class: 'jk' }, `${k} `), h('span', null, s));
  });
  return out;
}

function fmtPosition(p) {
  if (p == null) return '–';
  if (Array.isArray(p)) return p.join('.');
  return String(p);
}

export class GraphView {
  constructor(ctx, route) {
    this.ctx = ctx;
    this.t = route.tenant;
    this.g = route.graph;
    this.sel = route.node || null;
    this.tab = 'slices';
    this.title = `${route.tenant}/${route.graph}`;
    this.pendingState = null;
    this.data = null;
    this.build();
  }

  get key() { return `${this.t}/${this.g}`; }

  // ---------------------------------------------------------------- build
  build() {
    const r = this.r = {};
    this.dag = new DagView((id) => this.select(id === this.sel ? null : id));

    r.seg = h('div', { class: 'seg', role: 'radiogroup', 'aria-label': "État d'exécution demandé" },
      RUN_STATES.map((s) => h('button', { class: 'seg-opt', type: 'button', role: 'radio', 'aria-checked': 'false', 'data-v': s.v, onclick: () => this.setRunState(s.v) },
        h('span', { class: 'dot' }), s.label)));
    r.segOpts = [...r.seg.children];
    r.segOpts.forEach((b) => {
      const tone = { running: 'var(--color-success)', paused: 'var(--color-neutral-300)', draining: 'var(--color-accent)', stopped: 'var(--color-neutral-500)' }[b.dataset.v];
      b.firstChild.style.background = tone;
    });

    r.stats = {
      in: statCard('Entrée'), out: statCard('Sortie'), wm: statCard('Watermark'), topo: statCard('Topologie'), plan: statCard('Plan'),
    };

    this.el = h('div', { class: 'page' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' },
          h('div', { class: 'crumbs' }, h('a', { href: '#/' }, "Vue d'ensemble"), h('span', null, '/'), h('span', null, this.t)),
          h('div', { class: 'title-row' },
            r.h1 = h('h1', null, this.g),
            r.stateTag = h('span'),
            r.version = h('span', { class: 'tag tag-neutral tag-mono' }),
            r.transition = h('span', { class: 'muted', style: { fontSize: '13px' } }))),
        h('div', { class: 'actions' },
          r.seg,
          h('button', { class: 'btn btn-ghost danger', type: 'button', onclick: () => this.deleteGraph(), title: 'Supprimer le graphe' }, icon('trash'), 'Supprimer'))),
      r.missing = h('div', { class: 'card empty', hidden: true }),
      r.planErr = h('div', { class: 'banner banner-danger', role: 'alert', hidden: true }, icon('warn'), h('div', { class: 'grow' }, h('strong', null, 'Le graphe ne peut pas être planifié. '), r.planErrText = h('span'))),
      r.bneck = h('div', { class: 'banner banner-danger bneck', role: 'status', hidden: true },
        icon('warn'),
        h('div', { class: 'grow' },
          h('div', { class: 'head' }, h('strong', null, 'Goulot : ', r.bnNode = h('span')), r.bnKind = h('span'), r.bnBusy = h('span')),
          r.bnReason = h('div', { class: 'reason' }),
          r.bnHint = h('div', { class: 'hint' })),
        r.bnBtn = h('button', { class: 'btn btn-secondary btn-sm', type: 'button', onclick: () => this.data?.bottleneck && this.select(this.data.bottleneck.node) }, icon('target'), 'Voir le nœud')),
      h('div', { class: 'stats' }, r.stats.in, r.stats.out, r.stats.wm, r.stats.topo, r.stats.plan),
      r.dagCard = h('section', { class: 'card dag-card', 'aria-label': 'Topologie' },
        h('div', { class: 'dag-toolbar' },
          h('div', { style: { display: 'flex', alignItems: 'baseline', gap: '10px' } }, h('h3', null, 'Topologie'), r.dagMeta = h('span', { class: 'muted num', style: { fontSize: '12.5px' } })),
          h('div', { class: 'legend', 'aria-label': 'Légende' },
            h('span', null, h('span', { class: 'sw b' }), 'busy'),
            h('span', null, h('span', { class: 'sw i' }), 'idle'),
            h('span', null, h('span', { class: 'sw p' }), 'backpressure'),
            h('span', { title: "Épaisseur et couleur d'une arête : remplissage de la file d'entrée du nœud destination" },
              h('span', { class: 'ln', style: { height: '2px', background: 'var(--color-neutral-500)' } }),
              h('span', { class: 'ln', style: { height: '5px', background: 'var(--tone-bp)', width: '16px' } }), "file d'entrée")),
          r.zoom = h('div', { class: 'seg seg-sm', role: 'radiogroup', 'aria-label': 'Échelle du diagramme' },
            h('button', { class: 'seg-opt', type: 'button', role: 'radio', 'aria-checked': 'true', 'data-v': 'fit', onclick: () => this.setZoom('fit') }, 'Ajuster'),
            h('button', { class: 'seg-opt', type: 'button', role: 'radio', 'aria-checked': 'false', 'data-v': '100', onclick: () => this.setZoom('100') }, '100 %'))),
        h('div', { class: 'hr' }),
        this.dag.el),
      h('section', { class: 'section' },
        r.tabs = h('div', { class: 'tabs', role: 'tablist' },
          r.tabSlices = this.tabBtn('slices', 'Slices'),
          r.tabResults = this.tabBtn('results', 'Résultats'),
          r.tabSpec = this.tabBtn('spec', 'Description')),
        r.paneSlices = this.buildSlices(),
        r.paneResults = h('div', { role: 'tabpanel', class: 'section', style: { gap: 'var(--space-8)' } },
          r.resultsList = h('div', { style: { display: 'flex', flexDirection: 'column', gap: 'var(--space-8)' } }),
          r.resultsEmpty = h('div', { class: 'card empty', hidden: true }, 'Aucun nœud ', h('code', null, KV_SINK), ' dans ce graphe : rien à afficher.')),
        r.paneSpec = this.buildSpec()));
    this.panel = new NodePanel(this);
    this.applyTab();
  }

  tabBtn(id, label) {
    const b = h('button', { class: 'tab', role: 'tab', type: 'button', id: `tab-${id}`, 'aria-selected': 'false', onclick: () => { this.tab = id; this.applyTab(); this.ctx.refresh(); } },
      h('span', null, label), h('span', { class: 'tag tag-neutral', hidden: true }));
    return b;
  }

  applyTab() {
    const r = this.r;
    for (const [id, btn, pane] of [['slices', r.tabSlices, r.paneSlices], ['results', r.tabResults, r.paneResults], ['spec', r.tabSpec, r.paneSpec]]) {
      attr(btn, 'aria-selected', this.tab === id ? 'true' : 'false');
      attr(pane, 'aria-labelledby', `tab-${id}`);
      pane.hidden = this.tab !== id;
    }
    if (this.data) this.update(this.ctx.store);
  }

  buildSlices() {
    const r = this.r;
    const cols = [['Slice'], ['Worker', 'Worker qui rapporte la slice (et assignation du placement si elle diffère)'], ['État'], ['Epoch', 'Dernière epoch validée'],
      ['Position', 'Dernière position validée'], ['Watermark'], ['Commit', 'Durée moyenne d’un commit sur le dernier intervalle'],
      ['En vol', 'Epochs lues mais pas encore validées'], ['Fenced', 'Commits perdus au profit d’un autre worker'], ['Restarts'], ['Erreur', 'Dernière erreur de la slice']];
    const right = new Set([3, 6, 7, 8, 9]);
    return h('div', { role: 'tabpanel', class: 'section' },
      h('div', { class: 'table-wrap' }, h('table', { class: 'table' },
        h('thead', null, h('tr', null, cols.map(([c, t], i) => h('th', { class: right.has(i) ? 'r' : null, title: t || null }, c)))),
        r.sliceBody = h('tbody'))),
      r.sliceEmpty = h('div', { class: 'card empty', hidden: true }, 'Aucune slice planifiée.'));
  }

  buildSpec() {
    const r = this.r;
    return h('div', { role: 'tabpanel', class: 'section' },
      h('div', { class: 'section-head' },
        r.specMeta = h('div', { class: 'meta' }),
        h('div', { style: { display: 'flex', gap: 'var(--space-3)' } },
          h('button', { class: 'btn btn-secondary btn-sm', type: 'button', onclick: () => this.copySpec() }, icon('copy'), 'Copier le JSON'),
          r.dotLink = h('a', { class: 'btn btn-secondary btn-sm', href: paths.dot(this.t, this.g), target: '_blank', rel: 'noopener' }, icon('link'), 'Export DOT'))),
      r.specPre = h('pre', { class: 'code', tabindex: '0', 'aria-label': 'GraphSpec' }));
  }

  // ---------------------------------------------------------------- routing / actions
  setRoute(route) {
    if (route.node !== this.sel) {
      this.sel = route.node || null;
      this.update(this.ctx.store);
    }
  }

  select(id) {
    this.ctx.navigate(this.ctx.graphHref(this.t, this.g, id));
  }

  onEscape() { if (this.sel) this.select(null); }

  setZoom(v) {
    for (const b of this.r.zoom.children) attr(b, 'aria-checked', b.dataset.v === v ? 'true' : 'false');
    this.dag.setMode(v);
  }

  needs() {
    const n = { graph: [this.t, this.g] };
    if (this.tab === 'results' && this.data) {
      n.outputs = (this.data.nodes || []).filter((x) => x.op === KV_SINK).map((x) => [this.t, this.g, x.id]);
    }
    return n;
  }

  async setRunState(v) {
    const d = this.data;
    if (!d) return;
    const cur = d.desired?.state || d.state;
    if (cur === v && !this.pendingState) return;
    if (v === 'stopped') {
      const ok = await this.ctx.confirm({
        title: `Arrêter ${this.g} ?`,
        body: [h('p', { style: { margin: 0 } }, 'Plus aucun acteur ne tournera pour ', h('code', null, `${this.t}/${this.g}`), '. L’état et les positions sont conservés : le graphe repartira de son dernier commit quand il sera relancé.'),
          h('p', { style: { margin: '8px 0 0' } }, 'Pour vider ce qui est en vol avant l’arrêt, préférez ', h('b', null, 'Draining'), '.')],
        confirm: 'Arrêter le graphe', danger: true,
      });
      if (!ok) return;
    }
    this.pendingState = { v, at: Date.now() };
    this.updateSeg();
    try {
      await post(paths.state(this.t, this.g), { state: v });
      this.ctx.toast(`${this.g} : état demandé ${v}`);
    } catch (e) {
      this.pendingState = null;
      this.ctx.toast(`Changement d'état refusé : ${e.message}`, { error: true });
    }
    this.updateSeg();
    this.ctx.refresh();
  }

  async deleteGraph() {
    const ok = await this.ctx.confirm({
      title: `Supprimer ${this.t}/${this.g} ?`,
      body: [h('p', { style: { margin: 0 } }, 'Le graphe passe à l’état ', h('b', null, 'stopped'), ' et son entrée d’index est marquée en suppression. Le contrôleur efface ensuite son état (positions, fenêtres, résultats) puis l’entrée elle-même.'),
        h('p', { style: { margin: '8px 0 0' } }, 'Cette action est irréversible.')],
      confirm: 'Supprimer définitivement', danger: true,
    });
    if (!ok) return;
    try {
      await del(paths.graph(this.t, this.g));
      this.ctx.toast(`${this.t}/${this.g} : suppression demandée`);
      this.ctx.navigate('#/');
    } catch (e) {
      this.ctx.toast(`Suppression refusée : ${e.message}`, { error: true });
    }
  }

  async copySpec() {
    const spec = this.data?.spec;
    if (!spec) return;
    const txt = JSON.stringify(spec, null, 2);
    try {
      await navigator.clipboard.writeText(txt);
      this.ctx.toast('GraphSpec copiée dans le presse-papiers');
    } catch {
      const sel = window.getSelection(); const range = document.createRange();
      range.selectNodeContents(this.r.specPre); sel.removeAllRanges(); sel.addRange(range);
      this.ctx.toast('Copie impossible ici : le JSON est sélectionné, utilisez Ctrl+C', { error: true });
    }
  }

  updateSeg() {
    const d = this.data;
    const desired = d?.desired?.state || d?.state;
    if (this.pendingState && (desired === this.pendingState.v || Date.now() - this.pendingState.at > 10000)) this.pendingState = null;
    const target = this.pendingState?.v || desired;
    for (const b of this.r.segOpts) {
      attr(b, 'aria-checked', b.dataset.v === target ? 'true' : 'false');
      toggle(b, 'pending', this.pendingState?.v === b.dataset.v);
    }
  }

  // ---------------------------------------------------------------- update
  update(store) {
    const r = this.r;
    const det = store.detail && store.detail.key === this.key ? store.detail : null;
    if (det?.missing) {
      r.missing.hidden = false;
      r.missing.replaceChildren(h('div', { style: { fontSize: '15px', color: 'var(--color-text)', marginBottom: '6px' } }, `Graphe ${this.key} introuvable`),
        h('div', null, det.error || '', ' · ', h('a', { href: '#/' }, "Retour à la vue d'ensemble")));
      for (const el of [r.dagCard, r.tabs.parentElement, r.bneck, r.planErr]) el.hidden = true;
      r.stats.in.parentElement.hidden = true;
      this.panel.close();
      return;
    }
    r.missing.hidden = true;
    r.dagCard.hidden = false; r.tabs.parentElement.hidden = false; r.stats.in.parentElement.hidden = false;
    const d = det?.data;
    if (!d) return;
    this.data = d;
    const nodes = d.nodes || [];
    const now = conn.now();

    // header
    setTag(r.stateTag, d.state, stateTone(d.state));
    text(r.version, `v${d.version}`);
    attr(r.version, 'title', `Version ${d.version} de la description`);
    const desired = d.desired?.state;
    const observed = this.observedState(nodes);
    text(r.transition, desired && observed && observed !== desired ? `observé : ${observed}` : '');
    this.updateSeg();

    // stats
    const srcOut = nodes.filter((n) => n.kind === 'source').reduce((a, n) => a + (n.outPerSec || 0), 0);
    const sinkOut = nodes.filter((n) => n.kind === 'sink').reduce((a, n) => a + (n.outPerSec || 0), 0);
    setStat(r.stats.in, rate(srcOut), `${int(nodes.filter((n) => n.kind === 'source').length)} source(s)`);
    setStat(r.stats.out, rate(sinkOut), `${int(nodes.filter((n) => n.kind === 'sink').length)} sink(s)`);
    if (d.watermarkMs != null) {
      const lag = Math.max(0, now - d.watermarkMs);
      setStat(r.stats.wm, time(d.watermarkMs), `retard ${dur(lag)}`, fullTime(d.watermarkMs));
      r.stats.wm._r.s.style.color = lag > 30000 ? 'var(--tone-bp-text)' : '';
    } else setStat(r.stats.wm, '–', (d.slices || []).some((x) => x.slice.startsWith('src.')) ? 'slice source muette' : 'aucune slice source');
    const actors = nodes.reduce((a, n) => a + (n.actors || 0), 0);
    setStat(r.stats.topo, `${nodes.length} nœuds`, `${int(actors)} acteurs · ${int((d.slices || []).length)} slices`);
    const plan = d.plan;
    if (!plan) setStat(r.stats.plan, 'non planifié', 'en attente du contrôleur');
    else setStat(r.stats.plan, `v${plan.specVersion}`, plan.error ? 'en erreur' : `calculé à ${time(plan.plannedMs)}`, plan.plannedMs ? fullTime(plan.plannedMs) : null);
    r.stats.plan._r.s.style.color = plan?.error ? 'var(--color-danger-300)' : '';
    if (plan && plan.specVersion !== d.version) text(r.stats.plan._r.s, `retard sur v${d.version}`);

    r.planErr.hidden = !plan?.error;
    text(r.planErrText, plan?.error || '');

    this.updateBottleneck(d, nodes);

    // DAG
    this.dag.update(d, this.sel);
    const layers = this.dag.lay?.layers || 0;
    text(r.dagMeta, `${nodes.length} nœuds · ${(d.edges || []).length} arêtes · ${layers} couches`);

    // tabs
    const sinks = nodes.filter((n) => n.op === KV_SINK);
    const sliceTag = r.tabSlices.lastChild; sliceTag.hidden = false; text(sliceTag, (d.slices || []).length);
    const resTag = r.tabResults.lastChild; resTag.hidden = sinks.length === 0; text(resTag, sinks.length);
    if (this.tab === 'slices') this.updateSlices(d, now);
    if (this.tab === 'results') this.updateResults(store, sinks);
    if (this.tab === 'spec') this.updateSpec(d);

    // panel
    const sel = this.sel && nodes.find((n) => n.id === this.sel);
    toggle(this.el, 'with-panel', !!sel);
    if (sel) this.panel.show(sel, d); else this.panel.close();
    if (sel && this.scrolledFor !== sel.id) {
      this.scrolledFor = sel.id;
      requestAnimationFrame(() => this.dag.scrollTo(sel.id, this.panel.el));
    }
    if (!sel) this.scrolledFor = null;
  }

  observedState(nodes) {
    if (!nodes.length) return null;
    const order = ['failed', 'starting', 'paused', 'draining', 'running', 'stopped', 'pending'];
    const states = nodes.map((n) => n.state || 'pending');
    if (states.every((s) => s === 'pending')) return 'aucun acteur';
    return order.find((s) => states.includes(s)) || null;
  }

  updateBottleneck(d, nodes) {
    const r = this.r;
    const b = d.bottleneck;
    r.bneck.hidden = !b;
    if (!b) return;
    text(r.bnNode, b.node);
    setTag(r.bnKind, b.kind === 'commit' ? 'commits KV' : b.kind, 'danger');
    setTag(r.bnBusy, `busy ${pct(b.busy)}`, 'danger');
    text(r.bnReason, b.reason || '');
    r.bnReason.hidden = !b.reason;
    const node = nodes.find((n) => n.id === b.node);
    let hint;
    if (b.kind === 'commit') {
      hint = `Ce ne sont pas les opérateurs qui limitent le pipeline mais les commits KV de la slice partant de « ${b.node} » : chaque epoch doit être écrite dans le KV avant que la source puisse avancer, et ces écritures saturent. Pistes : espacer les commits (flushInterval), autoriser plus d'epochs en vol (maxInFlightEpochs) ou soulager le KV.`;
    } else {
      const ups = (d.edges || []).filter((e) => e.to === b.node).map((e) => e.from);
      hint = `« ${b.node} » travaille ${pct(b.busy)} du temps pendant que ${ups.length ? ups.map((u) => `« ${u} »`).join(', ') : "l'amont"} attend de la place : c'est lui qui fixe le débit du graphe${node ? ` (${rate(node.inPerSec)} en entrée, ${node.actors} acteur${node.actors > 1 ? 's' : ''})` : ''}.`;
    }
    text(r.bnHint, hint);
  }

  updateSlices(d, now) {
    const r = this.r;
    const slices = [...(d.slices || [])].sort((a, b) => a.slice.localeCompare(b.slice, 'fr', { numeric: true }));
    r.sliceEmpty.hidden = slices.length > 0;
    r.sliceBody.closest('.table-wrap').hidden = slices.length === 0;
    syncList(r.sliceBody, slices, (s) => s.slice, () => h('tr', null,
      h('td', { class: 'nowrap' }, h('code')), h('td', { class: 'nowrap' }), h('td', null, h('span')), h('td', { class: 'r' }),
      h('td', { class: 'nowrap' }, h('code')), h('td', { class: 'nowrap' }), h('td', { class: 'r nowrap' }), h('td', { class: 'r' }),
      h('td', { class: 'r' }), h('td', { class: 'r' }), h('td', { class: 'cell-ellipsis' })),
    (tr, s) => {
      const c = tr.children;
      text(c[0].firstChild, s.slice);
      const moved = s.worker && s.assignedTo && s.worker !== s.assignedTo;
      if (!s.worker) text(c[1], s.assignedTo ? `assignée à ${s.assignedTo}` : 'non assignée');
      else text(c[1], moved ? `${s.worker} · assignée à ${s.assignedTo}` : s.worker);
      c[1].style.color = !s.worker ? 'var(--color-neutral-400)' : moved ? 'var(--tone-bp-text)' : '';
      if (!s.worker) setTag(c[2].firstChild, 'non rapportée', 'outline');
      else setTag(c[2].firstChild, s.state, stateTone(s.state));
      attr(c[2], 'title', s.worker ? null : `état publié : ${s.state} (aucun worker vivant ne rapporte cette slice)`);
      text(c[3], int(s.epoch));
      const pos = s.position;
      const win = s.window;
      if (win) {
        text(c[4].firstChild, `fenêtre ${time(win.startMs)}–${time(win.endMs).slice(-8)}${win.complete ? '' : ' (en cours)'}`);
        attr(c[4], 'title', `dernière fenêtre émise : ${time(win.startMs)} → ${time(win.endMs)}${win.complete ? '' : ', émission en cours'} (position brute ${fmtPosition(pos)})`);
      } else {
        text(c[4].firstChild, fmtPosition(pos));
        attr(c[4], 'title', pos ? `position brute ${fmtPosition(pos)}` : null);
      }
      text(c[5], s.watermarkMs != null ? time(s.watermarkMs) : '–');
      attr(c[5], 'title', s.watermarkMs != null ? `retard ${dur(Math.max(0, now - s.watermarkMs))}` : null);
      text(c[6], s.commitMs != null && s.worker ? `${s.commitMs >= 100 ? int(s.commitMs) : (Math.round(s.commitMs * 10) / 10).toString().replace('.', ',')}\u00a0ms` : '–');
      c[6].style.color = s.commitMs >= 250 ? 'var(--color-danger-300)' : '';
      text(c[7], int(s.inFlightEpochs));
      text(c[8], int(s.fenced));
      c[8].style.color = s.fenced ? 'var(--tone-bp-text)' : '';
      text(c[9], int(s.restarts));
      c[9].style.color = s.restarts ? 'var(--tone-bp-text)' : '';
      text(c[10], s.lastError || '–');
      attr(c[10], 'title', s.lastError || null);
      c[10].style.color = s.lastError ? 'var(--color-danger-200)' : 'var(--color-neutral-400)';
    });
  }

  updateResults(store, sinks) {
    const r = this.r;
    r.resultsEmpty.hidden = sinks.length > 0;
    syncList(r.resultsList, sinks, (n) => n.id, (n) => {
      const x = {};
      const el = h('div', { class: 'section' },
        h('div', { class: 'section-head' },
          h('div', { style: { display: 'flex', alignItems: 'baseline', gap: '10px' } }, x.name = h('h3', null, n.id), h('code', { class: 'muted' }, n.op)),
          x.meta = h('div', { class: 'meta' })),
        x.wrap = h('div', { class: 'table-wrap', style: { maxHeight: '520px' } }, h('table', { class: 'table table-compact' },
          h('thead', null, h('tr', null, h('th', { style: { width: '120px' } }, 'Heure'), h('th', { style: { width: '22%' } }, 'Clé'), h('th', null, 'Valeur'))),
          x.body = h('tbody'))),
        x.empty = h('div', { class: 'card empty', hidden: true }, 'Aucune ligne écrite pour le moment.'));
      el._x = x;
      return el;
    }, (el, n) => {
      const x = el._x;
      const out = store.outputs[`${this.t}/${this.g}/${n.id}`];
      if (!out) { text(x.meta, 'Chargement…'); return; }
      const rows = out.rows || [];
      text(x.meta, out.error ? `Erreur : ${out.error}` : `${int(rows.length)} dernières lignes · ${rate(n.outPerSec)} · mis à jour ${time(out.at)}`);
      x.empty.hidden = rows.length > 0;
      x.wrap.hidden = rows.length === 0;
      const first = !x.populated;
      syncList(x.body, rows, (row) => row.id, () => h('tr', null, h('td', { class: 'nowrap' }), h('td', null, h('code')), h('td', { class: 'jv' })),
        (tr, row, i, fresh) => {
          const c = tr.children;
          if (fresh) {
            text(c[0], time(row.ts)); attr(c[0], 'title', fullTime(row.ts));
            text(c[1].firstChild, row.key ?? '–');
            c[2].replaceChildren(...valueNodes(row.value));
            attr(c[2], 'title', JSON.stringify(row.value));
            if (!first) { tr.classList.add('flash'); setTimeout(() => tr.classList.remove('flash'), 1700); }
          }
        });
      if (rows.length) x.populated = true;
    });
  }

  updateSpec(d) {
    const r = this.r;
    text(r.specMeta, `GraphSpec version ${d.spec?.version ?? d.version} · ${(d.spec?.nodes || []).length} nœuds · ${(d.spec?.edges || []).length} arêtes`);
    const sig = JSON.stringify(d.spec);
    if (r.specPre._sig !== sig) { r.specPre._sig = sig; r.specPre.innerHTML = jsonHtml(d.spec); }
  }

  destroy() { this.dag.destroy(); this.panel.close(); }
}

// ==================================================================== node panel

class NodePanel {
  constructor(view) {
    this.view = view;
    this.node = null;
    this.r = {};
    this.el = null;
  }

  ensure() {
    if (this.el) return;
    const r = this.r;
    this.el = h('aside', { class: 'panel', 'aria-label': 'Détails du nœud', role: 'complementary' },
      h('div', { class: 'panel-head' },
        h('div', { class: 'grow' },
          r.kick = h('div', { class: 'card-kicker' }),
          r.name = h('div', { class: 'name' }),
          h('div', { class: 'tags' }, r.state = h('span'), r.bn = h('span', { class: 'tag tag-danger' }, 'Goulot'), r.paused = h('span', { class: 'tag tag-outline' }, icon('pause'), 'nœud en pause'))),
        h('button', { class: 'btn btn-ghost btn-icon', type: 'button', title: 'Fermer (Échap)', 'aria-label': 'Fermer le panneau', onclick: () => this.view.select(null) }, icon('x'))),
      r.body = h('div', { class: 'panel-body' },
        r.desc = h('div', { class: 'desc' }),
        h('div', { class: 'ratio-line' },
          r.bar = mbar('lg'),
          h('div', { class: 'nums' }, r.nb = h('span', { class: 't-busy' }), r.ni = h('span'), r.np = h('span', { class: 't-bp' })),
          h('div', { class: 'muted', style: { fontSize: '11.5px' } }, "Ratios de l'instance la plus chargée (busy le plus élevé).")),
        h('div', { class: 'metrics' },
          r.mIn = this.metric('Entrée'), r.mOut = this.metric('Sortie'), r.mQ = this.metric("File d'entrée"),
          r.mRin = this.metric('Reçus'), r.mRout = this.metric('Émis'), r.mAct = this.metric('Acteurs'),
          r.mLate = this.metric('Late dropped'), r.mErr = this.metric('Erreurs')),
        r.err = h('div', { class: 'errbox', hidden: true }),
        h('div', { class: 'hr' }),
        h('div', { class: 'sub-row' }, h('div', { class: 'sub' }, 'Configuration'), r.cfgMeta = h('span', { class: 'muted', style: { fontSize: '12px' } })),
        r.form = new ConfigForm(this.view).el,
        h('div', { class: 'hr' }),
        h('div', { class: 'sub' }, 'Contrôle'),
        h('div', { class: 'form', style: { gap: '6px' } },
          h('div', null, r.pauseBtn = h('button', { class: 'btn btn-secondary', type: 'button', onclick: () => this.togglePause() })),
          r.pauseHint = h('span', { class: 'muted', style: { fontSize: '12px' } })),
        h('div', { class: 'hr' }),
        h('div', { class: 'sub-row' }, h('div', { class: 'sub' }, 'Instances'), r.instMeta = h('span', { class: 'muted', style: { fontSize: '12px' } })),
        r.instWrap = h('div', { class: 'table-wrap', style: { background: 'var(--color-bg)' } },
          h('table', { class: 'table table-compact' },
            h('thead', null, h('tr', null, ['Slice · worker', 'Rôle', 'État', 'Charge', 'Entrée', 'Sortie', 'File'].map((c, i) => h('th', { class: i >= 4 ? 'r' : null }, c)))),
            r.instBody = h('tbody'))),
        r.instEmpty = h('div', { class: 'muted', style: { fontSize: '12.5px' }, hidden: true }, 'Aucune instance en cours (graphe arrêté, worker hors ligne ou slice pas encore démarrée).'),
        h('div', { class: 'hr' }),
        h('dl', { class: 'kv' },
          h('dt', null, 'Opérateur'), h('dd', null, r.op = h('code')),
          h('dt', null, "Type d'entrée"), h('dd', null, r.tin = h('code')),
          h('dt', null, 'Type de sortie'), h('dd', null, r.tout = h('code')),
          h('dt', null, 'Workers'), r.workers = h('dd'))));
    r.cfg = r.form._ctl;
    for (const t of [r.paused]) t.firstChild.setAttribute('style', 'width:11px;height:11px');
    document.body.appendChild(this.el);
  }

  metric(label) {
    const r = {};
    const el = h('div', { class: 'm' }, h('div', { class: 'k' }, label), r.v = h('div', { class: 'v' }));
    el._r = r;
    return el;
  }

  close() {
    if (!this.el) return;
    this.el.remove();
    this.el = null;
    this.r = {};
    this.node = null;
  }

  show(n, d) {
    const fresh = !this.el;
    this.ensure();
    const r = this.r;
    const changed = this.node !== n.id;
    this.node = n.id;
    if (changed && !fresh) { r.body.scrollTop = 0; }
    text(r.kick, `${n.kind || 'genre inconnu'} · ${n.op}`);
    text(r.name, n.id);
    setTag(r.state, n.state || 'pending', stateTone(n.state || 'pending'));
    r.bn.hidden = !n.bottleneck;
    r.paused.hidden = !n.paused;
    text(r.desc, n.description || 'Pas de description pour cet opérateur.');
    text(r.op, n.op);
    text(r.tin, shortType(n.inputType)); attr(r.tin, 'title', n.inputType);
    text(r.tout, shortType(n.outputType)); attr(r.tout, 'title', n.outputType);
    text(r.workers, n.workers?.length ? n.workers.join(', ') : '–');
    setMbar(r.bar, n.busy, n.idle, n.backpressured);
    text(r.nb, n.waiting >= 0.005 ? `busy ${pct(n.busy)} (attente destination ${pct(n.waiting)})` : `busy ${pct(n.busy)}`); text(r.ni, `idle ${pct(n.idle)}`); text(r.np, `backpressure ${pct(n.backpressured)}`);
    const m = (el, v, bad) => { text(el._r.v, v); toggle(el, 'bad', !!bad); };
    m(r.mIn, n.kind === 'source' ? '–' : rate(n.inPerSec));
    m(r.mOut, rate(n.outPerSec));
    m(r.mQ, n.queueCap ? `${int(n.queueLen)} / ${int(n.queueCap)}` : '–', n.queueCap && n.queueLen / n.queueCap >= 0.85);
    attr(r.mQ, 'title', n.queueCap ? `${pct(n.queueLen / n.queueCap)} de la capacité (en chunks)` : null);
    m(r.mRin, compact(n.recordsIn)); attr(r.mRin, 'title', int(n.recordsIn));
    m(r.mRout, compact(n.recordsOut)); attr(r.mRout, 'title', int(n.recordsOut));
    m(r.mAct, int(n.actors));
    m(r.mLate, compact(n.lateDropped));
    toggle(r.mLate, 'warn', n.lateDropped > 0); attr(r.mLate, 'title', 'Enregistrements arrivés après la fermeture de leur fenêtre');
    m(r.mErr, compact(n.errors), n.errors > 0);
    r.err.hidden = !n.lastError;
    text(r.err, n.lastError ? `Dernière erreur : ${n.lastError}` : '');

    // pause / resume
    const pend = this.pausePending && this.pausePending.node === n.id && this.pausePending.paused !== n.paused && Date.now() - this.pausePending.at < 8000;
    if (!pend) this.pausePending = null;
    r.pauseBtn.replaceChildren(icon(n.paused ? 'play' : 'pause'), n.paused ? 'Reprendre le nœud' : 'Mettre le nœud en pause');
    r.pauseBtn.disabled = !!pend;
    text(r.pauseHint, n.paused ? 'Le nœud ne consomme plus ses entrées : la backpressure bloque tout l’amont.' : 'Un nœud en pause cesse de consommer ; l’amont se bloque par backpressure.');

    // instances
    const inst = [...(n.instances || [])].sort((a, b) => a.slice.localeCompare(b.slice, 'fr', { numeric: true }) || a.kind.localeCompare(b.kind));
    text(r.instMeta, `${inst.length} · ${workersLabel(n.workers)}`);
    r.instEmpty.hidden = inst.length > 0;
    r.instWrap.hidden = inst.length === 0;
    syncList(r.instBody, inst, (a) => `${a.slice}|${a.kind}|${a.worker}`, () => h('tr', null,
      h('td', { class: 'nowrap' }, h('code'), h('div', { class: 'muted', style: { fontSize: '11px' } })), h('td'), h('td', null, h('span')),
      h('td', null, h('div', { style: { display: 'flex', alignItems: 'center', gap: '6px' } }, mbar(), h('span', { class: 'num', style: { fontSize: '11px', minWidth: '28px' } }))),
      h('td', { class: 'r nowrap' }), h('td', { class: 'r nowrap' }), h('td', { class: 'r nowrap' })),
    (tr, a) => {
      const c = tr.children;
      text(c[0].firstChild, a.slice);
      text(c[0].lastChild, a.worker || '–');
      text(c[1], a.kind);
      setTag(c[2].firstChild, a.state, stateTone(a.state), 'tag-sm');
      const bar = c[3].firstChild.firstChild; bar.style.width = '44px';
      setMbar(bar, a.busy, a.idle, a.backpressured);
      text(c[3].firstChild.lastChild, pct(a.busy));
      text(c[4], a.kind === 'source' ? '–' : rate(a.inPerSec));
      text(c[5], rate(a.outPerSec));
      text(c[6], a.queueCap ? `${a.queueLen}/${a.queueCap}` : '–');
      toggle(c[6], 't-bp', a.queueCap && a.queueLen / a.queueCap >= 0.85);
    });

    r.cfg.sync(n, d);
    text(r.cfgMeta, `version ${d.version}`);
  }

  async togglePause() {
    const v = this.view;
    const n = (v.data?.nodes || []).find((x) => x.id === this.node);
    if (!n) return;
    const resume = n.paused;
    this.pausePending = { node: n.id, paused: !resume, at: Date.now() };
    this.r.pauseBtn.disabled = true;
    try {
      await post((resume ? paths.resume : paths.pause)(v.t, v.g, n.id));
      v.ctx.toast(resume ? `${n.id} : reprise demandée` : `${n.id} : pause demandée`);
    } catch (e) {
      this.pausePending = null;
      v.ctx.toast(`${resume ? 'Reprise' : 'Pause'} refusée : ${e.message}`, { error: true });
    }
    v.ctx.refresh();
  }
}

// ==================================================================== configuration form

/** Form generated from a node's `config` object; PATCH .../nodes/<node>/config. */
class ConfigForm {
  constructor(view) {
    this.view = view;
    this.node = null;
    this.base = null;    // JSON of the config the form was built from
    this.fields = [];    // {key, kind, input, original, err, isNew}
    this.r = {};
    const r = this.r;
    this.el = h('form', { class: 'form', novalidate: true, onsubmit: (e) => { e.preventDefault(); this.apply(); } },
      r.fields = h('div', { class: 'form', style: { gap: 'var(--space-6)' } }),
      r.none = h('div', { class: 'muted', style: { fontSize: '12.5px' }, hidden: true }, 'Ce nœud n’a pas de configuration.'),
      r.stale = h('div', { class: 'banner banner-info', hidden: true, style: { fontSize: '12.5px', padding: '8px 12px' } }, 'La configuration a changé sur le serveur pendant votre saisie. ', h('a', { href: '#', onclick: (e) => { e.preventDefault(); this.rebuild(this.lastNode); } }, 'Recharger')),
      h('details', { style: { fontSize: '12.5px' } },
        h('summary', { class: 'muted', style: { cursor: 'pointer' } }, 'Ajouter un champ'),
        h('div', { class: 'addfield', style: { marginTop: '8px' } },
          r.newKey = h('input', { class: 'input mono', placeholder: 'nom', 'aria-label': 'Nom du nouveau champ' }),
          r.newVal = h('input', { class: 'input mono', placeholder: 'valeur (JSON ou texte)', 'aria-label': 'Valeur du nouveau champ' }),
          h('button', { class: 'btn btn-secondary', type: 'button', onclick: () => this.addField() }, icon('plus'), 'Ajouter'))),
      r.error = h('div', { class: 'form-error', role: 'alert', hidden: true }, icon('warn'), h('div', null, h('div', null, "L'API a refusé la configuration :"), r.errText = h('code'))),
      r.actions = h('div', { class: 'form-actions' },
        r.apply = h('button', { class: 'btn btn-primary', type: 'submit', disabled: true }, 'Appliquer'),
        r.reset = h('button', { class: 'btn btn-ghost', type: 'button', disabled: true, onclick: () => this.rebuild(this.lastNode) }, 'Annuler les modifications'),
        r.status = h('span', { class: 'muted', style: { fontSize: '12px' } })));
    this.el._ctl = this;
  }

  dirty() { return this.fields.some((f) => f.isNew || this.valueOf(f).changed); }

  sync(n) {
    this.lastNode = n;
    const cfg = n.config && typeof n.config === 'object' && !Array.isArray(n.config) ? n.config : {};
    const json = JSON.stringify(cfg);
    if (this.node !== n.id) { this.rebuild(n); return; }
    if (json !== this.base) {
      if (!this.dirty()) this.rebuild(n);
      else this.r.stale.hidden = false;
    }
  }

  rebuild(n) {
    const r = this.r;
    this.node = n.id;
    const cfg = n.config && typeof n.config === 'object' && !Array.isArray(n.config) ? n.config : {};
    this.base = JSON.stringify(cfg);
    this.fields = [];
    r.fields.replaceChildren();
    for (const [k, v] of Object.entries(cfg)) this.addRow(k, v, false);
    r.none.hidden = this.fields.length > 0;
    r.stale.hidden = true;
    r.error.hidden = true;
    text(r.status, '');
    this.refreshState();
  }

  addRow(key, value, isNew) {
    const kind = typeof value === 'number' ? 'number' : typeof value === 'boolean' ? 'bool' : typeof value === 'string' ? 'string' : 'json';
    const f = { key, kind, original: value, isNew };
    const id = `cfg-${this.node}-${key}`.replace(/[^a-zA-Z0-9_-]/g, '_');
    const onInput = () => { f.errEl.hidden = true; toggle(f.input, 'invalid', false); this.refreshState(); };
    if (kind === 'bool') {
      f.input = h('input', { type: 'checkbox', id, checked: value, onchange: onInput });
      f.wrap = h('label', { class: 'toggle', for: id }, f.input, h('span', { class: 'track' }), f.lbl = h('span', null, value ? 'true' : 'false'));
    } else if (kind === 'number') {
      f.input = h('input', { class: 'input mono', type: 'number', id, step: Number.isInteger(value) ? '1' : 'any', value: String(value), oninput: onInput });
      f.wrap = f.input;
    } else if (kind === 'string') {
      f.input = h('input', { class: 'input mono', type: 'text', id, value, oninput: onInput, spellcheck: 'false' });
      f.wrap = f.input;
    } else {
      f.input = h('textarea', { class: 'input', id, rows: String(Math.min(8, JSON.stringify(value, null, 2).split('\n').length + 1)), oninput: onInput, spellcheck: 'false' });
      f.input.value = JSON.stringify(value, null, 2);
      f.wrap = f.input;
    }
    const tyLabel = { number: Number.isInteger(value) ? 'entier' : 'nombre', bool: 'booléen', string: 'texte', json: 'JSON' }[kind];
    f.row = h('div', { class: 'field' },
      h('label', { class: 'fl', for: id }, h('span', null, key), h('span', { class: 'ty' }, isNew ? `${tyLabel} · nouveau` : tyLabel)),
      f.wrap,
      f.errEl = h('div', { class: 'ferr', hidden: true }));
    this.fields.push(f);
    this.r.fields.appendChild(f.row);
    this.r.none.hidden = true;
    return f;
  }

  /** Parsed value of a field and whether it differs from the original. */
  valueOf(f) {
    if (f.kind === 'bool') return { ok: true, value: f.input.checked, changed: f.input.checked !== f.original };
    if (f.kind === 'number') {
      const raw = f.input.value.trim();
      if (raw === '' || Number.isNaN(Number(raw))) return { ok: false, error: 'nombre attendu', changed: true };
      const v = Number(raw);
      return { ok: true, value: v, changed: v !== f.original };
    }
    if (f.kind === 'string') return { ok: true, value: f.input.value, changed: f.input.value !== f.original };
    try {
      const v = JSON.parse(f.input.value);
      return { ok: true, value: v, changed: JSON.stringify(v) !== JSON.stringify(f.original) };
    } catch (e) {
      return { ok: false, error: `JSON invalide : ${e.message}`, changed: true };
    }
  }

  refreshState() {
    let n = 0;
    for (const f of this.fields) {
      const v = this.valueOf(f);
      const d = f.isNew || v.changed;
      if (d) n += 1;
      if (f.kind === 'bool') { toggle(f.wrap, 'dirty', d); text(f.lbl, f.input.checked ? 'true' : 'false'); } else toggle(f.input, 'dirty', d);
    }
    this.r.actions.hidden = this.fields.length === 0;
    this.r.apply.disabled = n === 0 || this.busy;
    this.r.reset.disabled = n === 0 || this.busy;
    text(this.r.status, n ? `${n} champ${n > 1 ? 's' : ''} modifié${n > 1 ? 's' : ''}` : '');
  }

  addField() {
    const r = this.r;
    const key = r.newKey.value.trim();
    if (!key) { r.newKey.focus(); return; }
    if (this.fields.some((f) => f.key === key)) { this.view.ctx.toast(`Le champ ${key} existe déjà`, { error: true }); return; }
    let v = r.newVal.value;
    try { v = JSON.parse(v); } catch { /* plain text */ }
    const f = this.addRow(key, v, true);
    r.newKey.value = ''; r.newVal.value = '';
    this.refreshState();
    f.input.focus();
  }

  async apply() {
    const r = this.r;
    const body = {};
    let bad = false;
    for (const f of this.fields) {
      const v = this.valueOf(f);
      if (!v.ok) { bad = true; f.errEl.hidden = false; text(f.errEl, v.error); toggle(f.input, 'invalid', true); continue; }
      if (f.isNew || v.changed) body[f.key] = v.value;
    }
    if (bad || !Object.keys(body).length) return;
    const v = this.view;
    const node = this.node;
    this.busy = true; this.refreshState();
    r.error.hidden = true;
    text(r.status, 'Envoi…');
    try {
      const res = await patch(paths.config(v.t, v.g, node), body);
      this.busy = false;
      // the next poll carries the merged config; until then keep the sent values as the baseline
      const merged = { ...JSON.parse(this.base), ...body };
      this.rebuild({ id: node, config: merged });
      v.ctx.toast(`Configuration de ${node} appliquée${res && res.version != null ? ` · version ${res.version}` : ''}`);
      v.ctx.refresh();
    } catch (e) {
      this.busy = false;
      r.error.hidden = false;
      text(r.errText, e.message);
      text(r.status, e.status ? `HTTP ${e.status}` : '');
      this.refreshState();
    }
  }
}
