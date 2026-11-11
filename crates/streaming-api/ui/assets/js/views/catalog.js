// Catalogue: registered operators with a text filter.

import { h, text, attr, syncList, setTag, shortType, icon } from '../util.js';

const KIND_TONE = { source: 'accent', sink: 'success', aggregate: 'warn', window: 'outline', key_by: 'outline', map: 'neutral' };

export class CatalogView {
  constructor(ctx) {
    this.ctx = ctx;
    this.title = 'Catalogue';
    this.q = '';
    this.el = h('div', { class: 'page' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' }, h('div', { class: 'card-kicker' }, 'streaming-rs'), h('h1', null, 'Catalogue')),
        h('div', { class: 'actions' },
          h('label', { class: 'input-search', style: { width: '320px', maxWidth: '100%' } },
            icon('search'),
            this.input = h('input', { class: 'input', type: 'search', placeholder: 'Filtrer : référence, type, description…', 'aria-label': 'Filtrer les opérateurs', oninput: (e) => { this.q = e.target.value; this.update(this.ctx.store); } })))),
      h('div', { class: 'section' },
        h('div', { class: 'section-head' }, this.meta = h('div', { class: 'meta' }, 'Chargement…')),
        h('div', { class: 'table-wrap' },
          h('table', { class: 'table' },
            h('thead', null, h('tr', null, h('th', null, 'Référence'), h('th', null, 'Genre'), h('th', null, 'Entrée'), h('th', null, 'Sortie'), h('th', null, 'Description'))),
            this.tbody = h('tbody'))),
        this.empty = h('div', { class: 'card empty', hidden: true })));
  }

  needs() { return { catalog: true }; }

  update(store) {
    const ops = store.catalog;
    if (!ops) return;
    const q = this.q.trim().toLowerCase();
    const list = q ? ops.filter((o) => [o.op, o.kind, o.input, o.output, o.description].some((v) => String(v ?? '').toLowerCase().includes(q))) : ops;
    text(this.meta, q ? `${list.length} sur ${ops.length} opérateurs` : `${ops.length} opérateurs`);
    this.empty.hidden = list.length > 0;
    text(this.empty, q ? `Aucun opérateur ne correspond à « ${this.q.trim()} ».` : 'Catalogue vide.');
    this.tbody.parentElement.parentElement.hidden = list.length === 0;
    syncList(this.tbody, list, (o) => o.op, () => h('tr', null,
      h('td', { class: 'nowrap' }, h('code')), h('td', null, h('span')), h('td', null, h('code')), h('td', null, h('code')), h('td', { class: 'soft' })),
    (tr, o) => {
      const [a, b, c, d, e] = tr.children;
      text(a.firstChild, o.op);
      setTag(b.firstChild, o.kind, KIND_TONE[o.kind] || 'neutral');
      text(c.firstChild, shortType(o.input)); attr(c.firstChild, 'title', o.input === '*' ? "* : n'importe quel type" : o.input);
      text(d.firstChild, shortType(o.output)); attr(d.firstChild, 'title', o.output === '*' ? "* : n'importe quel type" : o.output);
      text(e, o.description || '');
    });
  }
}
