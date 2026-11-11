// Pipeline designer: pick operators from the catalogue, fill their parameters,
// wire the steps, deploy. Builds a GraphSpec and PUTs it.
import { h, text, icon, shortType } from '../util.js';
import { put, paths } from '../api.js';

const KINDS = [
  ['source', 'Source'], ['map', 'Transformation'], ['key_by', 'Clé'],
  ['window', 'Fenêtre'], ['aggregate', 'Agrégat'], ['sink', 'Puits'],
];
const HINT = { duration: 'ex. 5m, 10s, 500ms', integer: 'entier', number: 'nombre', text: 'texte', secret: 'valeur, ou secret:<nom>' };
// What may follow what: an aggregate takes a window, a window takes a key.
const NEEDS = { window: 'key_by', aggregate: 'window' };

export class DesignView {
  constructor(ctx) {
    this.ctx = ctx;
    this.title = 'Concevoir';
    this.steps = [];
    this.seq = 0;
    const r = (this.refs = {});
    this.el = h('div', { class: 'page design' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' }, h('div', { class: 'card-kicker' }, 'streaming-rs'), h('h1', null, 'Concevoir un pipeline')),
        h('div', { class: 'actions' },
          h('button', { class: 'btn btn-secondary', type: 'button', onclick: () => this.template() }, icon('copy'), 'Exemple : comptage par clé'),
          r.deploy = h('button', { class: 'btn btn-primary', type: 'button', onclick: () => this.deploy() }, 'Déployer'))),
      h('div', { class: 'design-grid' },
        h('div', { class: 'section' },
          h('div', { class: 'card design-id' },
            this.field('Tenant', r.tenant = h('input', { class: 'input mono', value: 'acme', oninput: () => this.render() })),
            this.field('Graphe', r.graph = h('input', { class: 'input mono', value: 'mon-pipeline', oninput: () => this.render() }))),
          r.steps = h('div', { class: 'design-steps' }),
          h('div', null, h('button', { class: 'btn btn-secondary', type: 'button', onclick: () => this.add() }, '+ Ajouter une étape'))),
        h('div', { class: 'section' },
          h('div', { class: 'card' },
            h('div', { class: 'card-kicker' }, 'Vérifications'),
            r.checks = h('ul', { class: 'design-checks' })),
          h('div', { class: 'card' },
            h('div', { class: 'card-kicker' }, 'Description générée'),
            r.json = h('pre', { class: 'design-json' })))));
  }

  needs() { return { catalog: true }; }

  update(store) {
    const ops = store.catalog || [];
    if (ops.length === (this.ops || []).length) return;
    this.ops = ops;
    if (!this.steps.length) this.add();
    this.render();
  }

  field(label, control, hint) {
    return h('label', { class: 'design-field' }, h('span', { class: 'lbl' }, label), control, hint ? h('span', { class: 'hint' }, hint) : null);
  }

  opsOf(kind) { return (this.ops || []).filter((o) => o.kind === kind); }
  op(ref) { return (this.ops || []).find((o) => o.op === ref); }

  add(kind, ref, id, config, inputs) {
    const prev = this.steps[this.steps.length - 1];
    kind = kind || (prev ? 'map' : 'source');
    const first = this.opsOf(kind)[0];
    const step = {
      n: ++this.seq, kind, op: ref || (first ? first.op : ''), id: id || `etape-${this.seq}`,
      values: { ...(config || {}) }, raw: config && !this.op(ref)?.params?.length ? JSON.stringify(config) : '',
      inputs: inputs || (prev ? [prev.n] : []),
    };
    this.steps.push(step);
    this.render();
    return step;
  }

  template() {
    this.steps = [];
    const pick = (kind, like) => this.opsOf(kind).find((o) => o.op.includes(like)) || this.opsOf(kind)[0];
    const chain = [
      ['source', 'generator', 'evenements'], ['map', 'decode', 'decodage'], ['key_by', 'by-', 'par-cle'],
      ['window', 'tumbling', 'fenetre', { size: '5m' }], ['aggregate', 'count', 'comptage'],
      ['map', 'count-to-json', 'en-ligne'], ['sink', 'kv-sink', 'resultats'],
    ];
    for (const [kind, like, id, config] of chain) {
      const found = pick(kind, like);
      if (found) this.add(kind, found.op, id, config);
    }
  }

  spec() {
    const byN = new Map(this.steps.map((s) => [s.n, s]));
    const problems = [];
    const nodes = this.steps.map((s) => {
      const d = this.op(s.op);
      let config = {};
      if (d && d.params && d.params.length) {
        for (const p of d.params) {
          const v = s.values[p.name];
          if (v === undefined || v === '') {
            if (p.required) problems.push(`${s.id} : le paramètre « ${p.name} » est requis`);
            continue;
          }
          if (p.kind === 'integer' || p.kind === 'number') {
            const n = Number(v);
            if (Number.isNaN(n) || (p.kind === 'integer' && !Number.isInteger(n))) problems.push(`${s.id} : « ${p.name} » doit être un ${p.kind === 'integer' ? 'entier' : 'nombre'}`);
            config[p.name] = n;
          } else if (p.kind === 'boolean') config[p.name] = v === true || v === 'true';
          else if (p.kind === 'secret' && String(v).startsWith('secret:')) config[p.name] = { secret: String(v).slice(7) };
          else config[p.name] = v;
        }
      } else if (s.raw.trim()) {
        try { config = JSON.parse(s.raw); } catch { problems.push(`${s.id} : la configuration n'est pas du JSON valide`); }
      }
      const node = { id: s.id, op: s.op };
      if (config && Object.keys(config).length) node.config = config;
      return node;
    });
    const edges = [];
    const ids = new Set();
    for (const s of this.steps) {
      if (!/^[A-Za-z0-9_-]+$/.test(s.id)) problems.push(`« ${s.id} » : un identifiant n'accepte que lettres, chiffres, - et _`);
      if (ids.has(s.id)) problems.push(`l'identifiant « ${s.id} » est utilisé deux fois`);
      ids.add(s.id);
      if (!s.op) problems.push(`${s.id} : aucun opérateur de ce genre dans le catalogue`);
      const ups = s.inputs.map((n) => byN.get(n)).filter(Boolean);
      for (const u of ups) edges.push([u.id, s.id]);
      if (s.kind === 'source' && ups.length) problems.push(`${s.id} : une source n'a pas d'entrée`);
      if (s.kind !== 'source' && !ups.length) problems.push(`${s.id} : relier cette étape à une entrée`);
      if (NEEDS[s.kind] && !ups.some((u) => u.kind === NEEDS[s.kind])) problems.push(`${s.id} : ${s.kind === 'window' ? 'une fenêtre suit une clé' : 'un agrégat suit une fenêtre'}`);
    }
    if (!this.steps.some((s) => s.kind === 'source')) problems.push('il faut au moins une source');
    if (!this.steps.some((s) => s.kind === 'sink')) problems.push('il faut au moins un puits');
    const tenant = this.refs.tenant.value.trim();
    const graph = this.refs.graph.value.trim();
    if (!tenant || !graph) problems.push('le tenant et le nom du graphe sont requis');
    return { spec: { tenant, graph, nodes, edges }, problems };
  }

  render() {
    const r = this.refs;
    r.steps.replaceChildren(...this.steps.map((s, i) => this.card(s, i)));
    this.refresh();
  }

  // Cheap part of a render: what depends on the values, not on the structure.
  refresh() {
    const r = this.refs;
    const { spec, problems } = this.spec();
    text(r.json, JSON.stringify(spec, null, 2));
    r.checks.replaceChildren(...(problems.length
      ? problems.map((p) => h('li', { class: 'bad' }, p))
      : [h('li', { class: 'good' }, 'La description est cohérente : prête à déployer.')]));
    r.deploy.disabled = problems.length > 0;
  }

  card(s, index) {
    const d = this.op(s.op);
    const set = (k, v) => { s[k] = v; this.render(); };
    const kind = h('select', { class: 'input', onchange: (e) => { s.kind = e.target.value; const f = this.opsOf(s.kind)[0]; s.op = f ? f.op : ''; s.values = {}; s.raw = ''; if (s.kind === 'source') s.inputs = []; this.render(); } },
      ...KINDS.map(([v, label]) => h('option', { value: v, selected: v === s.kind }, label)));
    const op = h('select', { class: 'input mono', onchange: (e) => { s.values = {}; s.raw = ''; set('op', e.target.value); } },
      ...this.opsOf(s.kind).map((o) => h('option', { value: o.op, selected: o.op === s.op }, o.op)));
    const id = h('input', { class: 'input mono', value: s.id, oninput: (e) => { s.id = e.target.value.trim(); this.refresh(); }, onchange: () => this.render() });
    const earlier = this.steps.slice(0, index);
    const inputs = s.kind === 'source' ? null : h('div', { class: 'design-inputs' },
      ...earlier.map((u) => h('label', { class: 'chk' },
        h('input', { type: 'checkbox', checked: s.inputs.includes(u.n), onchange: (e) => { s.inputs = e.target.checked ? [...s.inputs, u.n] : s.inputs.filter((n) => n !== u.n); this.refresh(); } }),
        h('code', null, u.id))),
      earlier.length ? null : h('span', { class: 'hint' }, 'aucune étape en amont'));
    const params = d && d.params && d.params.length
      ? d.params.map((p) => {
        const common = { class: 'input mono', placeholder: p.default != null ? `défaut : ${typeof p.default === 'string' ? p.default : JSON.stringify(p.default)}` : (HINT[p.kind] || ''), oninput: (e) => { s.values[p.name] = e.target.type === 'checkbox' ? e.target.checked : e.target.value; this.refresh(); } };
        const control = p.kind === 'boolean'
          ? h('input', { type: 'checkbox', checked: s.values[p.name] === true, onchange: common.oninput })
          : h('input', { ...common, type: p.kind === 'integer' || p.kind === 'number' ? 'number' : 'text', value: s.values[p.name] ?? '' });
        return this.field(`${p.name}${p.required ? ' *' : ''}`, control, p.description);
      })
      : [this.field('Configuration (JSON, facultative)', h('textarea', { class: 'input mono design-raw', rows: 1, placeholder: '{ }', value: s.raw, oninput: (e) => { s.raw = e.target.value; this.refresh(); } }), "Cet opérateur ne décrit pas ses paramètres : laissez vide s'il n'en prend pas.")];
    return h('div', { class: 'card design-step' },
      h('div', { class: 'design-step-head' },
        h('span', { class: 'num' }, String(index + 1)),
        this.field('Identifiant', id),
        this.field('Genre', kind),
        this.field('Opérateur', op, d ? `${shortType(d.input)} → ${shortType(d.output)}` : null),
        h('button', { class: 'btn btn-ghost btn-icon', type: 'button', title: 'Retirer cette étape', 'aria-label': `Retirer ${s.id}`, onclick: () => { this.steps = this.steps.filter((x) => x !== s); for (const x of this.steps) x.inputs = x.inputs.filter((n) => n !== s.n); this.render(); } }, icon('trash'))),
      d && d.description ? h('div', { class: 'design-about' }, d.description) : null,
      inputs ? this.field('Entrées', inputs) : null,
      h('div', { class: 'design-params' }, ...params));
  }

  async deploy() {
    const { spec, problems } = this.spec();
    if (problems.length) return;
    try {
      await put(paths.graph(spec.tenant, spec.graph), spec);
      this.ctx.toast(`${spec.tenant}/${spec.graph} : déployé`);
      this.ctx.refresh();
      this.ctx.navigate(this.ctx.graphHref(spec.tenant, spec.graph));
    } catch (e) {
      this.ctx.toast(`Déploiement refusé : ${e.message}`, { error: true });
    }
  }
}
