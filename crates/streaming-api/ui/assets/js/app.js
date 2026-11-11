// Shell: sidebar, hash router, 1 s polling loop (paused while hidden), dialogs and toasts.

import { h, text, attr, syncList, icon, setClass, time, dur, stateTone } from './util.js';
import { get, conn, paths } from './api.js';
import { OverviewView } from './views/overview.js';
import { ClusterView } from './views/cluster.js';
import { CatalogView } from './views/catalog.js';
import { GraphView } from './views/graph.js';
import { DesignView } from './views/design.js';

const POLL_MS = 1000;
const CATALOG_TTL = 15000;

const store = {
  tenants: null,      // [{tenant, graphs, actors, in_per_sec}]
  graphs: {},         // tenant -> [graph summary]
  cluster: null,
  clusterPrev: null,  // previous snapshot (KV rates)
  catalog: null,
  catalogAt: 0,
  detail: null,       // {key, data, missing, error}
  outputs: {},        // "t/g/node" -> {rows, at}
  updatedAt: 0,
};

// ------------------------------------------------------------------ routing
function parseRoute() {
  const raw = location.hash.replace(/^#/, '') || '/';
  const parts = raw.split('/').filter(Boolean).map((p) => { try { return decodeURIComponent(p); } catch { return p; } });
  if (parts[0] === 'cluster') return { name: 'cluster' };
  if (parts[0] === 'catalog') return { name: 'catalog' };
  if (parts[0] === 'design') return { name: 'design' };
  if (parts[0] === 't' && parts[2] === 'g' && parts[3]) {
    return { name: 'graph', tenant: parts[1], graph: parts[3], node: parts[4] === 'n' ? parts[5] || null : null };
  }
  return { name: 'overview' };
}
export function graphHref(t, g, node) {
  const e = encodeURIComponent;
  return `#/t/${e(t)}/g/${e(g)}${node ? `/n/${e(node)}` : ''}`;
}
function navigate(hash) { if (location.hash !== hash) location.hash = hash; }

// ------------------------------------------------------------------ toasts & dialog
const toasts = h('div', { class: 'toasts', role: 'status', 'aria-live': 'polite' });
function toast(msg, opts = {}) {
  const el = h('div', { class: `toast${opts.error ? ' err' : ''}` }, icon(opts.error ? 'warn' : 'check'), h('span', null, msg));
  el.firstChild.setAttribute('width', '16'); el.firstChild.setAttribute('height', '16');
  el.firstChild.style.color = opts.error ? 'var(--color-danger-300)' : 'var(--color-success)';
  toasts.appendChild(el);
  setTimeout(() => el.remove(), opts.error ? 7000 : 3800);
}

const dialog = h('dialog', { class: 'dialog', 'aria-labelledby': 'dlg-title' });
document.body.appendChild(dialog);
/** Nocturne confirmation dialog. Resolves true when confirmed. */
function confirmDialog({ title, body, confirm = 'Confirmer', danger = false }) {
  return new Promise((resolve) => {
    let done = false;
    const finish = (v) => { if (done) return; done = true; dialog.close(); resolve(v); };
    const cancelBtn = h('button', { class: 'btn btn-secondary', type: 'button', onclick: () => finish(false) }, 'Annuler');
    const okBtn = h('button', { class: `btn ${danger ? 'btn-danger' : 'btn-primary'}`, type: 'button', onclick: () => finish(true) }, confirm);
    dialog.replaceChildren(h('div', { class: 'dialog-body' },
      h('div', { class: 'dialog-title', id: 'dlg-title' }, title),
      h('div', { class: 'dialog-text' }, body),
      h('div', { class: 'dialog-actions' }, cancelBtn, okBtn)));
    dialog.onclose = () => finish(false);
    dialog.onclick = (e) => { if (e.target === dialog) finish(false); };
    dialog.showModal();
    (danger ? cancelBtn : okBtn).focus();
  });
}

// ------------------------------------------------------------------ shell
const ctx = {
  store, navigate, toast, confirm: confirmDialog, refresh: () => tick(), graphHref,
};

const sidebarRefs = {};
const sidebar = h('aside', { class: 'sidebar' },
  h('a', { class: 'brand', href: '#/', 'aria-label': 'streaming-rs, accueil' }, 'streaming', h('span', { class: 'acc' }, '-rs')),
  h('nav', { class: 'nav', 'aria-label': 'Navigation principale' },
    sidebarRefs.overview = h('a', { class: 'nav-item', href: '#/' }, icon('overview'), h('span', { class: 'grow' }, "Vue d'ensemble")),
    sidebarRefs.cluster = h('a', { class: 'nav-item', href: '#/cluster' }, icon('cluster'), h('span', { class: 'grow' }, 'Cluster'), sidebarRefs.clusterCount = h('span', { class: 'count' })),
    sidebarRefs.catalog = h('a', { class: 'nav-item', href: '#/catalog' }, icon('catalog'), h('span', { class: 'grow' }, 'Catalogue'), sidebarRefs.catalogCount = h('span', { class: 'count' })),
    sidebarRefs.design = h('a', { class: 'nav-item', href: '#/design' }, icon('catalog'), h('span', { class: 'grow' }, 'Concevoir')),
    h('div', { class: 'nav-label' }, 'Tenants'),
    sidebarRefs.tree = h('div', { style: { display: 'flex', flexDirection: 'column', gap: '2px' } }),
    sidebarRefs.treeEmpty = h('div', { class: 'muted', style: { fontSize: '12.5px', padding: '0 8.4px' } }, 'Chargement…')),
  h('div', { class: 'conn', role: 'status', 'aria-live': 'polite' },
    sidebarRefs.connDot = h('span', { class: 'dot' }),
    h('div', { class: 'grow' }, sidebarRefs.connLabel = h('b', null, 'Connexion…'), sidebarRefs.connSub = h('span', { class: 'num' }, ' '))));

const offline = h('div', { class: 'banner banner-danger offline', role: 'alert', hidden: true },
  icon('plug'), h('div', { class: 'grow' }, h('strong', null, 'API injoignable. '), offlineText()));
function offlineText() { return (sidebarRefs.offText = h('span')); }

const mainEl = h('main', { class: 'main', id: 'main' });
const content = h('div', { style: { display: 'contents' } });
mainEl.append(offline, content);
document.getElementById('app').replaceChildren(h('div', { class: 'app' }, sidebar, mainEl), toasts);

function updateSidebar(route) {
  const cur = (el, on) => attr(el, 'aria-current', on ? 'page' : null);
  cur(sidebarRefs.overview, route.name === 'overview');
  cur(sidebarRefs.cluster, route.name === 'cluster');
  cur(sidebarRefs.catalog, route.name === 'catalog');
  cur(sidebarRefs.design, route.name === 'design');
  const c = store.cluster;
  if (c) text(sidebarRefs.clusterCount, `${c.workers.filter((w) => w.alive).length}/${c.workers.length}`);
  if (store.catalog) text(sidebarRefs.catalogCount, store.catalog.length);

  const tenants = store.tenants || [];
  if (store.tenants) text(sidebarRefs.treeEmpty, 'Aucun tenant.');
  sidebarRefs.treeEmpty.hidden = !!(store.tenants && tenants.length);
  if (!store.tenants) sidebarRefs.treeEmpty.hidden = false;
  syncList(sidebarRefs.tree, tenants, (t) => t.tenant, (t) => {
    const r = {};
    const el = h('div', { style: { display: 'flex', flexDirection: 'column', gap: '2px' } },
      h('div', { class: 'nav-tenant' }, icon('tenant'), r.name = h('span', null, t.tenant), r.count = h('span', { class: 'count' })),
      r.list = h('div', { style: { display: 'flex', flexDirection: 'column', gap: '2px' } }));
    el._r = r;
    return el;
  }, (el, t) => {
    const r = el._r;
    const gs = store.graphs[t.tenant] || [];
    text(r.count, gs.length || t.graphs || 0);
    syncList(r.list, gs, (g) => g.graph, (g) => {
      const gr = {};
      const a = h('a', { class: 'nav-item nav-graph', href: graphHref(g.tenant, g.graph) },
        gr.name = h('span', { class: 'grow' }, g.graph), gr.gl = icon('warn', 'gl'), gr.dot = h('span', { class: 'dot' }));
      a._r = gr;
      return a;
    }, (a, g) => {
      const gr = a._r;
      const st = g.error ? 'failed' : g.state;
      const tone = { success: 'ok', danger: 'bad', accent: 'info' }[stateTone(st)] || '';
      setClass(gr.dot, `dot ${tone}`);
      gr.gl.style.display = g.bottleneck ? '' : 'none';
      attr(a, 'title', `${g.tenant}/${g.graph} · ${g.state}${g.bottleneck ? ` · goulot : ${g.bottleneck.node}` : ''}${g.error ? ` · ${g.error}` : ''}`);
      attr(a, 'aria-current', route.name === 'graph' && route.tenant === g.tenant && route.graph === g.graph ? 'page' : null);
    });
  });
}

function updateConn() {
  const ok = conn.ok;
  setClass(sidebarRefs.connDot, `dot ${ok === null ? '' : ok ? 'ok live' : 'bad'}`);
  text(sidebarRefs.connLabel, ok === null ? 'Connexion…' : ok ? 'API connectée' : 'API injoignable');
  const now = Date.now();
  if (ok) text(sidebarRefs.connSub, `Mise à jour ${time(conn.lastOk)}${document.hidden ? ' · en pause' : ''}`);
  else if (ok === false) text(sidebarRefs.connSub, conn.lastOk ? `Dernière réponse ${time(conn.lastOk)}` : 'Aucune réponse');
  offline.hidden = ok !== false;
  if (ok === false) {
    text(sidebarRefs.offText, `${conn.lastError && conn.lastError !== 'API injoignable' ? `${conn.lastError}. ` : ''}Hors ligne depuis ${dur(now - conn.downSince)}${conn.lastOk ? ` · données affichées du ${time(conn.lastOk)}` : ''}. Nouvelle tentative chaque seconde.`);
  }
}

// ------------------------------------------------------------------ views
let route = parseRoute();
let view = null;
let viewKey = '';

function mountView() {
  route = parseRoute();
  const key = route.name === 'graph' ? `graph:${route.tenant}/${route.graph}` : route.name;
  if (key !== viewKey) {
    if (view) view.destroy?.();
    viewKey = key;
    if (route.name === 'graph') view = new GraphView(ctx, route);
    else if (route.name === 'cluster') view = new ClusterView(ctx);
    else if (route.name === 'catalog') view = new CatalogView(ctx);
    else if (route.name === 'design') view = new DesignView(ctx);
    else view = new OverviewView(ctx);
    content.replaceChildren(view.el);
    mainEl.scrollTop = 0;
    document.title = view.title ? `${view.title} · streaming-rs` : 'streaming-rs';
  } else {
    view.setRoute?.(route);
  }
  render();
}

function render() {
  updateSidebar(route);
  updateConn();
  if (view) view.update(store);
}

// ------------------------------------------------------------------ polling
let timer = null;
let gen = 0;
async function tick() {
  clearTimeout(timer);
  if (document.hidden) { updateConn(); return; }
  const my = (gen += 1);
  const need = view?.needs?.() || {};
  const jobs = [];

  jobs.push(get(paths.tenants()).then(async (d) => {
    const tenants = d.tenants || [];
    const lists = await Promise.all(tenants.map((t) => get(paths.graphs(t.tenant))
      .then((x) => [t.tenant, x.graphs || []])
      .catch(() => [t.tenant, store.graphs[t.tenant] || []])));
    store.tenants = tenants;
    store.graphs = Object.fromEntries(lists);
  }).catch(() => {}));

  jobs.push(get(paths.cluster()).then((d) => {
    if (store.cluster && store.cluster.now_ms !== d.now_ms) store.clusterPrev = store.cluster;
    store.cluster = d;
    if (typeof d.now_ms === 'number') {
      const off = d.now_ms - Date.now();
      conn.clockOffset = conn.clockOffset ? conn.clockOffset * 0.7 + off * 0.3 : off;
    }
  }).catch(() => {}));

  if (need.graph) {
    const [t, g] = need.graph;
    const key = `${t}/${g}`;
    jobs.push(get(paths.graph(t, g)).then((d) => { store.detail = { key, data: d, at: Date.now() }; })
      .catch((e) => {
        if (!e.network) store.detail = { key, data: store.detail?.key === key ? store.detail.data : null, missing: e.status === 404, error: e.message };
      }));
  }
  for (const [t, g, n] of need.outputs || []) {
    const key = `${t}/${g}/${n}`;
    jobs.push(get(paths.outputs(t, g, n, 100)).then((d) => { store.outputs[key] = { rows: d.rows || [], at: Date.now() }; })
      .catch((e) => { if (!e.network) store.outputs[key] = { rows: store.outputs[key]?.rows || [], error: e.message, at: Date.now() }; }));
  }
  if (need.catalog && (!store.catalog || Date.now() - store.catalogAt > CATALOG_TTL)) {
    jobs.push(get(paths.catalog()).then((d) => { store.catalog = d.ops || []; store.catalogAt = Date.now(); }).catch(() => {}));
  }

  await Promise.allSettled(jobs);
  if (my !== gen) return; // a newer tick took over
  store.updatedAt = Date.now();
  render();
  timer = setTimeout(tick, POLL_MS);
}

document.addEventListener('visibilitychange', () => {
  if (document.hidden) { clearTimeout(timer); updateConn(); } else tick();
});
window.addEventListener('hashchange', () => { mountView(); tick(); });
window.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && !dialog.open && view?.onEscape) view.onEscape(e);
});
conn.listeners.add(() => updateConn());

mountView();
tick();
// prefetch the catalog once for the sidebar count
get(paths.catalog()).then((d) => { store.catalog = d.ops || []; store.catalogAt = Date.now(); updateSidebar(route); }).catch(() => {});
