// Vue d'ensemble: cluster summary + one card per graph, grouped by tenant.

import { h, text, attr, toggle, syncList, setTag, stateTone, rate, int, compact, bytes, time, dur, icon, pct } from '../util.js';
import { conn } from '../api.js';

export function statCard(label) {
  const r = {};
  const el = h('div', { class: 'card stat' }, h('div', { class: 'k' }, label), r.v = h('div', { class: 'v' }, '–'), r.s = h('div', { class: 's' }, ' '));
  el._r = r;
  return el;
}
export function setStat(el, v, s, title) {
  text(el._r.v, v);
  text(el._r.s, s || ' ');
  attr(el, 'title', title || null);
}

/** KV read/write rates from two consecutive cluster snapshots. */
export function kvRates(cur, prev) {
  if (!cur?.kv || !prev?.kv || cur.now_ms <= prev.now_ms) return null;
  const dt = (cur.now_ms - prev.now_ms) / 1000;
  return { reads: Math.max(0, (cur.kv.reads - prev.kv.reads) / dt), writes: Math.max(0, (cur.kv.writes - prev.kv.writes) / dt) };
}

export function clusterStats() {
  const s = {
    workers: statCard('Workers'), actors: statCard('Acteurs'), slices: statCard('Slices'),
    epoch: statCard('Epoch cluster'), kv: statCard('KV'), kvio: statCard('Débit KV'),
  };
  s.el = h('div', { class: 'stats' }, s.workers, s.actors, s.slices, s.epoch, s.kv, s.kvio);
  s.update = (store) => {
    const c = store.cluster;
    if (!c) return;
    const alive = c.workers.filter((w) => w.alive);
    const dead = c.workers.filter((w) => !w.alive);
    setStat(s.workers, `${alive.length} / ${c.workers.length}`, dead.length ? `hors ligne : ${dead.map((w) => w.worker).join(', ')}` : 'tous en ligne');
    s.workers._r.s.style.color = dead.length ? 'var(--color-danger-300)' : '';
    setStat(s.actors, int(alive.reduce((a, w) => a + (w.actors || 0), 0)), `sur ${alive.length} worker${alive.length > 1 ? 's' : ''}`);
    setStat(s.slices, int(alive.reduce((a, w) => a + (w.slices || 0), 0)), `placement v${c.placement?.version ?? '–'}`);
    setStat(s.epoch, int(c.epoch), c.placement?.computed_by ? `placement par ${c.placement.computed_by}` : ' ');
    s.kv.hidden = !c.kv; s.kvio.hidden = !c.kv;
    if (c.kv) {
      setStat(s.kv, `${compact(c.kv.keys)} clés`, bytes(c.kv.bytes));
      const r = kvRates(c, store.clusterPrev);
      setStat(s.kvio, r ? rate(r.reads) : '–', r ? `lectures · ${rate(r.writes)} en écriture` : 'lectures / écritures', `total : ${int(c.kv.reads)} lectures, ${int(c.kv.writes)} écritures`);
    }
  };
  return s;
}

export function graphCard(ctx) {
  const r = {};
  const el = h('a', { class: 'card gcard' },
    h('div', { class: 'top' },
      h('div', null, r.kicker = h('div', { class: 'card-kicker' }), r.title = h('div', { class: 'card-title' })),
      r.state = h('span')),
    h('div', { class: 'rates' },
      h('div', null, h('div', { class: 'k' }, 'Entrée'), r.in = h('div', { class: 'v' })),
      h('div', null, h('div', { class: 'k' }, 'Sortie'), r.out = h('div', { class: 'v' }))),
    r.counts = h('div', { class: 'counts' }),
    r.wm = h('div', { class: 'counts' }),
    r.foot = h('div', { class: 'foot' }, r.bn = h('span'), r.err = h('div', { class: 'err' })));
  el._r = r;
  el._ctx = ctx;
  return el;
}

export function updateGraphCard(el, g) {
  const r = el._r;
  attr(el, 'href', el._ctx.graphHref(g.tenant, g.graph));
  text(r.kicker, g.tenant);
  text(r.title, g.graph);
  attr(r.title, 'title', g.graph);
  setTag(r.state, g.state, stateTone(g.state));
  text(r.in, rate(g.in_per_sec));
  text(r.out, rate(g.out_per_sec));
  once(r.counts, `${g.nodes}|${g.actors}|${g.slices}|${g.version}`, () => [
    h('span', null, h('b', null, int(g.nodes)), ' nœuds'),
    h('span', null, h('b', null, int(g.actors)), ' acteurs'),
    h('span', null, h('b', null, int(g.slices)), ' slices'),
    h('span', null, 'version ', h('b', null, g.version))]);
  if (g.watermark_ms != null) text(r.wm, `Watermark ${time(g.watermark_ms)} · retard ${dur(Math.max(0, conn.now() - g.watermark_ms))}`);
  else text(r.wm, 'Watermark indisponible');
  if (g.bottleneck) {
    r.bn.hidden = false;
    setTag(r.bn, `Goulot : ${g.bottleneck.node}${g.bottleneck.kind === 'commit' ? ' (commits KV)' : ''} · busy ${pct(g.bottleneck.busy)}`, 'danger');
    attr(r.bn, 'title', g.bottleneck.reason || '');
  } else r.bn.hidden = true;
  r.err.hidden = !g.error;
  text(r.err, g.error ? `Plan : ${g.error}` : '');
  toggle(el, 'has-bottleneck', !!g.bottleneck);
  r.foot.hidden = !g.bottleneck && !g.error;
}

function once(el, sig, build) {
  if (el._sig === sig) return;
  el._sig = sig;
  el.replaceChildren(...build());
}

export class OverviewView {
  constructor(ctx) {
    this.ctx = ctx;
    this.title = "Vue d'ensemble";
    this.stats = clusterStats();
    this.el = h('div', { class: 'page' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' }, h('div', { class: 'card-kicker' }, 'streaming-rs'), h('h1', null, "Vue d'ensemble")),
        this.meta = h('div', { class: 'muted num', style: { fontSize: '13px' } })),
      h('section', { class: 'section', 'aria-label': 'Cluster' },
        h('div', { class: 'section-head' }, h('h2', null, 'Cluster'), h('a', { href: '#/cluster', style: { fontSize: '13px' } }, 'Détails du cluster')),
        this.stats.el),
      this.tenants = h('div', { style: { display: 'flex', flexDirection: 'column', gap: 'var(--space-10)' } }),
      this.empty = h('div', { class: 'card empty', hidden: true }, 'Aucun graphe soumis. Utilisez ', h('code', null, 'PUT /api/v1/tenants/{tenant}/graphs/{graph}'), ' pour en créer un.'));
  }

  update(store) {
    this.stats.update(store);
    text(this.meta, store.updatedAt ? `Mis à jour à ${time(store.updatedAt)}` : '');
    const tenants = store.tenants || [];
    this.empty.hidden = !store.tenants || tenants.some((t) => (store.graphs[t.tenant] || []).length);
    syncList(this.tenants, tenants, (t) => t.tenant, (t) => {
      const r = {};
      const el = h('section', { class: 'section' },
        h('div', { class: 'section-head' },
          h('div', { style: { display: 'flex', alignItems: 'center', gap: '10px' } }, icon('tenant'), r.name = h('h2', null, t.tenant)),
          r.meta = h('div', { class: 'meta' })),
        r.grid = h('div', { class: 'grid-cards' }));
      el.querySelector('svg').setAttribute('style', 'width:20px;height:20px;color:var(--color-neutral-400)');
      el._r = r;
      return el;
    }, (el, t) => {
      const r = el._r;
      const gs = store.graphs[t.tenant] || [];
      const bn = gs.filter((g) => g.bottleneck).length;
      text(r.meta, `${gs.length} graphe${gs.length > 1 ? 's' : ''} · ${int(t.actors)} acteurs · ${rate(t.in_per_sec)} en entrée${bn ? ` · ${bn} goulot${bn > 1 ? 's' : ''}` : ''}`);
      syncList(r.grid, gs, (g) => g.graph, () => graphCard(this.ctx), updateGraphCard);
    });
  }
}
