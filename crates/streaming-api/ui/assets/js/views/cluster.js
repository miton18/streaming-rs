// Cluster: workers, placement grouped by worker, epoch and KV statistics.

import { h, text, attr, toggle, syncList, setTag, width, rate, int, pct, bytes, time, ago } from '../util.js';
import { conn } from '../api.js';
import { clusterStats, kvRates } from './overview.js';

const nf2 = new Intl.NumberFormat('fr-FR', { maximumFractionDigits: 2 });

function gauge(label) {
  const r = {};
  const el = h('div', { class: 'gauge' },
    h('div', { class: 'row' }, h('span', null, label), r.v = h('b')),
    h('div', { class: 'track' }, r.fill = h('div', { class: 'fill' })));
  el._r = r;
  return el;
}
function setGauge(el, ratio, label) {
  text(el._r.v, label);
  width(el._r.fill, ratio);
  toggle(el._r.fill, 'hot', ratio >= 0.85);
}

function workerCard() {
  const r = {};
  const el = h('div', { class: 'card wcard' },
    h('div', { class: 'top' },
      h('div', { style: { minWidth: 0 } }, h('div', { class: 'card-kicker' }, 'worker'), r.name = h('div', { class: 'card-title' })),
      r.tag = h('span')),
    r.facts = h('div', { class: 'facts' }),
    r.aside = h('div', { class: 'facts', hidden: true, title: 'Le code de ce worker déclare ces graphes autrement que le store, ou ne les déclare pas : il n’en exécute aucune slice. C’est l’état normal d’un ancien binaire pendant un déploiement.' }),
    h('div', { class: 'gauges' }, r.cpu = gauge('CPU'), r.mem = gauge('Mémoire')),
    r.counts = h('div', { class: 'facts' }, h('span', null, r.actors = h('b'), ' acteurs'), h('span', null, r.slices = h('b'), ' slices')),
    h('div', { class: 'hr' }),
    h('div', { class: 'sub' }, 'Consommation par tenant'),
    h('table', { class: 'table table-compact' },
      h('thead', null, h('tr', null, h('th', null, 'Tenant'), h('th', { class: 'r' }, 'Acteurs'), h('th', { class: 'r' }, 'Entrée'), h('th', { class: 'r', title: 'Somme des ratios busy des acteurs du tenant, attentes d’E/S comprises' }, 'Occupation'))),
      r.tbody = h('tbody')),
    r.none = h('div', { class: 'muted', style: { fontSize: '12.5px' } }, 'Aucun acteur.'));
  el._r = r;
  return el;
}

function updateWorker(el, w) {
  const r = el._r;
  text(r.name, w.worker);
  setTag(r.tag, w.alive ? 'en ligne' : 'hors ligne', w.alive ? 'success' : 'danger');
  toggle(el, 'dead', !w.alive);
  const facts = [`v${w.version || '?'}`, w.apiAddr ? `API ${w.apiAddr}` : 'sans API', `battement ${ago(w.heartbeatMs, conn.now())}`];
  text(r.facts, facts.join(' · '));
  attr(r.facts, 'title', `génération ${w.generation} (démarré ${time(w.generation)})`);
  const aside = w.aside || [];
  r.aside.hidden = aside.length === 0;
  text(r.aside, aside.length ? `En retrait de ${aside.length === 1 ? 'un graphe' : `${aside.length} graphes`} : ${aside.join(', ')}` : '');
  const load = w.load || {};
  setGauge(r.cpu, load.cpu || 0, pct(load.cpu || 0));
  setGauge(r.mem, load.memory || 0, `${pct(load.memory || 0)} · ${bytes(load.memoryBytes || 0)}`);
  text(r.actors, int(w.actors));
  text(r.slices, int(w.slices));
  const tenants = Object.entries(w.tenants || {}).sort((a, b) => a[0].localeCompare(b[0]));
  r.none.hidden = tenants.length > 0;
  r.tbody.parentElement.hidden = tenants.length === 0;
  syncList(r.tbody, tenants, ([t]) => t, () => {
    const tr = h('tr', null, h('td'), h('td', { class: 'r' }), h('td', { class: 'r' }), h('td', { class: 'r' }));
    return tr;
  }, (tr, [t, u]) => {
    const [a, b, c, d] = tr.children;
    text(a, t); text(b, int(u.actors)); text(c, rate(u.inPerSec)); text(d, nf2.format(u.busy || 0));
  });
}

export class ClusterView {
  constructor(ctx) {
    this.ctx = ctx;
    this.title = 'Cluster';
    this.stats = clusterStats();
    this.el = h('div', { class: 'page' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' }, h('div', { class: 'card-kicker' }, 'streaming-rs'), h('h1', null, 'Cluster')),
        this.meta = h('div', { class: 'muted num', style: { fontSize: '13px' } })),
      this.stats.el,
      h('section', { class: 'section' },
        h('div', { class: 'section-head' }, h('h2', null, 'Workers'), this.wmeta = h('div', { class: 'meta' })),
        this.grid = h('div', { class: 'grid-cards', style: { gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))' } })),
      h('section', { class: 'section' },
        h('div', { class: 'section-head' }, h('h2', null, 'Placement'), this.pmeta = h('div', { class: 'meta' })),
        h('div', { class: 'table-wrap' },
          h('table', { class: 'table' },
            h('thead', null, h('tr', null, h('th', null, 'Slice'), h('th', null, 'Tenant'), h('th', null, 'Graphe'))),
            this.ptbody = h('tbody'))),
        this.pempty = h('div', { class: 'card empty', hidden: true }, 'Aucune assignation.')),
      this.kvSec = h('section', { class: 'section' },
        h('div', { class: 'section-head' }, h('h2', null, 'KV')),
        h('div', { class: 'table-wrap' }, h('table', { class: 'table' },
          h('thead', null, h('tr', null, h('th', null, 'Clés'), h('th', null, 'Taille'), h('th', null, 'Lectures'), h('th', null, 'Écritures'))),
          h('tbody', null, this.kvRow = h('tr', null, h('td'), h('td'), h('td'), h('td')))))));
  }

  update(store) {
    this.stats.update(store);
    const c = store.cluster;
    if (!c) return;
    text(this.meta, `Epoch ${int(c.epoch)} · horloge serveur ${time(c.nowMs)}`);
    const alive = c.workers.filter((w) => w.alive).length;
    text(this.wmeta, `${alive} en ligne sur ${c.workers.length}`);
    const workers = [...c.workers].sort((a, b) => a.worker.localeCompare(b.worker, 'fr', { numeric: true }));
    syncList(this.grid, workers, (w) => w.worker, workerCard, updateWorker);

    // placement grouped by worker
    const p = c.placement || { assignments: {} };
    const groups = new Map();
    for (const w of workers) groups.set(w.worker, []);
    for (const [key, w] of Object.entries(p.assignments || {})) {
      if (!groups.has(w)) groups.set(w, []);
      groups.get(w).push(key);
    }
    const rows = [];
    const aliveSet = new Set(c.workers.filter((w) => w.alive).map((w) => w.worker));
    for (const [w, keys] of groups) {
      rows.push({ k: `w:${w}`, group: true, w, n: keys.length, alive: aliveSet.has(w) });
      keys.sort((a, b) => a.localeCompare(b, 'fr', { numeric: true }));
      for (const key of keys) {
        const [t, g, ...rest] = key.split('/');
        rows.push({ k: `s:${key}`, t, g, slice: rest.join('/'), w, alive: aliveSet.has(w) });
      }
    }
    text(this.pmeta, `version ${p.version ?? '–'}${p.computedBy ? ` · calculé par ${p.computedBy}` : ''}${p.computedMs ? ` à ${time(p.computedMs)}` : ''}`);
    this.pempty.hidden = Object.keys(p.assignments || {}).length > 0;
    syncList(this.ptbody, rows, (r) => r.k, (r) => {
      if (r.group) {
        const tr = h('tr', { class: 'group' }, h('td', { colspan: '3' }, h('span'), ' ', h('span', { class: 'muted', style: { fontWeight: 400 } }), ' ', h('span')));
        return tr;
      }
      return h('tr', null, h('td', null, h('code')), h('td'), h('td', null, h('a')));
    }, (tr, r) => {
      if (r.group) {
        const [name, n, tag] = tr.firstChild.querySelectorAll('span');
        text(name, r.w);
        text(n, `${r.n} slice${r.n > 1 ? 's' : ''}`);
        if (!r.alive) setTag(tag, 'hors ligne', 'danger', 'tag-sm'); else { tag.className = ''; text(tag, ''); }
        return;
      }
      const [a, b, cc] = tr.children;
      text(a.firstChild, r.slice);
      text(b, r.t);
      text(cc.firstChild, r.g);
      attr(cc.firstChild, 'href', this.ctx.graphHref(r.t, r.g));
      tr.style.opacity = r.alive ? '' : '0.7';
    });

    this.kvSec.hidden = !c.kv;
    if (c.kv) {
      const [a, b, cc, d] = this.kvRow.children;
      const kr = kvRates(c, store.clusterPrev);
      text(a, int(c.kv.keys)); text(b, bytes(c.kv.bytes));
      text(cc, `${int(c.kv.reads)}${kr ? ` · ${rate(kr.reads)}` : ''}`); text(d, `${int(c.kv.writes)}${kr ? ` · ${rate(kr.writes)}` : ''}`);
    }
  }
}
