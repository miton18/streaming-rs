// API: the OpenAPI document of /api/v1/openapi.json, browsable. Operations by
// tag with their parameters, bodies and responses; schemas as expandable
// trees; GET operations can be tried from the page.

import { h, text, icon, jsonHtml } from '../util.js';
import { get } from '../api.js';

const METHOD_TONE = { get: 'accent', post: 'success', put: 'warn', patch: 'warn', delete: 'danger' };
const REF = '#/components/schemas/';
const LOCATION = { path: 'chemin', query: 'requête', header: 'en-tête', cookie: 'cookie' };

const refName = (s) => (s && typeof s.$ref === 'string' && s.$ref.startsWith(REF) ? s.$ref.slice(REF.length) : null);

export class OpenApiView {
  constructor(ctx, route) {
    this.ctx = ctx;
    this.title = 'API';
    this.doc = null;
    this.q = '';
    this.open = new Set(route.op ? [route.op] : []);
    this.el = h('div', { class: 'page' },
      h('div', { class: 'page-head' },
        h('div', { class: 'title' },
          h('div', { class: 'card-kicker' }, 'streaming-rs'),
          h('div', { class: 'title-row' }, h('h1', null, 'API'), this.versions = h('span', { class: 'oa-versions' }))),
        h('div', { class: 'actions' },
          h('label', { class: 'input-search', style: { width: '320px', maxWidth: '100%' } },
            icon('search'),
            h('input', { class: 'input', type: 'search', placeholder: 'Filtrer : chemin, méthode, résumé…', 'aria-label': 'Filtrer les opérations', oninput: (e) => { this.q = e.target.value; this.renderOps(); } })),
          h('a', { class: 'btn btn-secondary', href: '/api/v1/openapi.json', download: 'openapi.json' }, icon('link'), 'openapi.json'))),
      this.about = h('p', { class: 'soft oa-about' }),
      this.status = h('div', { class: 'card empty' }, 'Chargement du document OpenAPI…'),
      this.ops = h('div', { class: 'oa-tags' }),
      this.schemasSection = h('div', { class: 'section', hidden: true },
        h('div', { class: 'section-head' }, h('h2', null, 'Schémas'), this.schemaMeta = h('div', { class: 'meta' })),
        this.schemas = h('div', { class: 'oa-schemas' })));
    this.load();
  }

  async load() {
    try {
      this.doc = await get('/api/v1/openapi.json');
    } catch (e) {
      this.status.className = 'errbox';
      text(this.status, `Document OpenAPI illisible : ${e.message}`);
      return;
    }
    this.status.hidden = true;
    const { info = {} } = this.doc;
    this.versions.replaceChildren(
      h('span', { class: 'tag tag-outline tag-mono' }, `v${info.version ?? '?'}`),
      h('span', { class: 'tag tag-accent tag-mono' }, `OpenAPI ${this.doc.openapi}`));
    text(this.about, info.description || '');
    this.renderOps();
    this.renderSchemas();
    if (this.open.size) this.ops.querySelector('.oa-op.open')?.scrollIntoView({ block: 'start' });
  }

  setRoute(route) {
    if (route.op && !this.open.has(route.op)) { this.open.add(route.op); this.renderOps(); }
  }

  update() {}

  operations() {
    const list = [];
    for (const [path, item] of Object.entries(this.doc.paths || {})) {
      for (const [method, op] of Object.entries(item)) list.push({ path, method, op, id: op.operationId || `${method} ${path}` });
    }
    return list;
  }

  renderOps() {
    if (!this.doc) return;
    const q = this.q.trim().toLowerCase();
    const all = this.operations();
    const list = q ? all.filter((o) => [o.path, o.method, o.op.summary, o.op.operationId, ...(o.op.tags || [])].some((v) => String(v ?? '').toLowerCase().includes(q))) : all;
    const tags = (this.doc.tags || []).map((t) => t.name);
    const byTag = new Map(tags.map((t) => [t, []]));
    for (const o of list) {
      const tag = o.op.tags?.[0] || 'autres';
      if (!byTag.has(tag)) byTag.set(tag, []);
      byTag.get(tag).push(o);
    }
    const sections = [];
    for (const [tag, ops] of byTag) {
      if (!ops.length) continue;
      ops.sort((a, b) => a.path.localeCompare(b.path) || methodOrder(a.method) - methodOrder(b.method));
      sections.push(h('div', { class: 'section' },
        h('div', { class: 'section-head' }, h('h2', null, tag), h('div', { class: 'meta' }, `${ops.length} opération${ops.length > 1 ? 's' : ''}`)),
        h('div', { class: 'oa-ops' }, ops.map((o) => this.operation(o)))));
    }
    if (!sections.length) sections.push(h('div', { class: 'card empty' }, `Aucune opération ne correspond à « ${this.q.trim()} ».`));
    this.ops.replaceChildren(...sections);
  }

  operation({ path, method, op, id }) {
    const body = h('div', { class: 'oa-op-body' });
    const el = h('div', { class: 'oa-op', 'data-op': id },
      h('button', { class: 'oa-op-head', type: 'button', 'aria-expanded': 'false', onclick: () => toggle() },
        h('span', { class: `tag tag-${METHOD_TONE[method] || 'neutral'} tag-mono oa-method` }, method.toUpperCase()),
        h('code', { class: 'oa-path' }, pathWithParams(path)),
        h('span', { class: 'oa-summary soft' }, op.summary || '')),
      body);
    const head = el.firstChild;
    const toggle = (force) => {
      const on = force ?? !el.classList.contains('open');
      el.classList.toggle('open', on);
      head.setAttribute('aria-expanded', String(on));
      if (on) { this.open.add(id); if (!body.firstChild) body.append(...this.operationBody(path, method, op)); } else this.open.delete(id);
      const hash = on ? `#/api/${encodeURIComponent(id)}` : '#/api';
      if (location.hash !== hash) history.replaceState(null, '', hash);
    };
    if (this.open.has(id)) toggle(true);
    return el;
  }

  operationBody(path, method, op) {
    const out = [];
    if (op.description) out.push(h('p', { class: 'oa-desc soft' }, op.description));
    out.push(h('div', { class: 'oa-meta muted' }, 'operationId ', h('code', null, op.operationId || '—')));

    const params = op.parameters || [];
    if (params.length) {
      out.push(h('div', { class: 'sub' }, 'Paramètres'),
        h('div', { class: 'table-wrap' }, h('table', { class: 'table table-compact' },
          h('thead', null, h('tr', null, h('th', null, 'Nom'), h('th', null, 'Où'), h('th', null, 'Type'), h('th', null, 'Description'))),
          h('tbody', null, params.map((p) => h('tr', null,
            h('td', { class: 'nowrap' }, h('code', null, p.name), p.required ? h('span', { class: 'oa-req', title: 'obligatoire' }, ' *') : null),
            h('td', { class: 'muted' }, LOCATION[p.in] || p.in),
            h('td', null, this.typeChip(p.schema)),
            h('td', { class: 'soft' }, p.description || constraints(this.resolve(p.schema)))))))));
    }

    if (op.requestBody) {
      out.push(h('div', { class: 'sub' }, `Corps de la requête${op.requestBody.required ? '' : ' (facultatif)'}`));
      for (const [type, media] of Object.entries(op.requestBody.content || {})) out.push(this.media(type, media.schema));
    }

    out.push(h('div', { class: 'sub' }, 'Réponses'));
    for (const [code, resp] of Object.entries(op.responses || {})) {
      const tone = code.startsWith('2') ? 'success' : code.startsWith('4') || code.startsWith('5') ? 'danger' : 'neutral';
      out.push(h('div', { class: 'oa-resp' },
        h('div', { class: 'oa-resp-head' }, h('span', { class: `tag tag-${tone} tag-mono` }, code), h('span', { class: 'soft' }, resp.description || '')),
        Object.entries(resp.content || {}).map(([type, media]) => this.media(type, media.schema))));
    }

    if (method === 'get') out.push(this.tryIt(path, op));
    return out;
  }

  media(type, schema) {
    return h('div', { class: 'oa-media' },
      h('div', { class: 'oa-media-head' }, h('span', { class: 'tag tag-outline tag-mono tag-sm' }, type), this.typeChip(schema)),
      this.tree(schema, 0, new Set()));
  }

  resolve(schema) {
    const name = refName(schema);
    return name ? this.doc.components?.schemas?.[name] ?? {} : schema ?? {};
  }

  // ---------------------------------------------------------------- types
  typeLabel(s) {
    if (s === true || s == null || (typeof s === 'object' && !Object.keys(s).length)) return 'any';
    const name = refName(s);
    if (name) return name;
    for (const key of ['anyOf', 'oneOf']) {
      if (Array.isArray(s[key])) return s[key].map((x) => this.typeLabel(x)).join(' | ');
    }
    if (Array.isArray(s.enum)) return s.enum.map((v) => JSON.stringify(v)).join(' | ');
    if (s.const !== undefined) return JSON.stringify(s.const);
    const types = Array.isArray(s.type) ? s.type : s.type ? [s.type] : [];
    const one = (t) => {
      if (t === 'array') {
        if (Array.isArray(s.prefixItems)) return `[${s.prefixItems.map((x) => this.typeLabel(x)).join(', ')}]`;
        return `${wrap(this.typeLabel(s.items))}[]`;
      }
      if (t === 'object' && !s.properties && s.additionalProperties && s.additionalProperties !== true) return `map<string, ${this.typeLabel(s.additionalProperties)}>`;
      if (t === 'object' && !s.properties) return 'object';
      if ((t === 'integer' || t === 'number') && s.format) return `${t} (${s.format})`;
      return t;
    };
    if (!types.length) return s.properties ? 'object' : 'any';
    return types.map(one).join(' | ');
  }

  typeChip(schema) {
    const name = refName(schema);
    if (!name) return h('code', { class: 'oa-type' }, this.typeLabel(schema));
    return h('a', { class: 'oa-type oa-ref', href: `#/api/schema/${encodeURIComponent(name)}`, onclick: (e) => { e.preventDefault(); this.showSchema(name); } }, name);
  }

  showSchema(name) {
    const el = this.schemas.querySelector(`[data-schema="${CSS.escape(name)}"]`);
    if (!el) return;
    el.open = true;
    el.scrollIntoView({ block: 'start', behavior: 'smooth' });
    el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash');
  }

  // Children of a schema worth expanding: properties, items, map values, alternatives.
  children(schema) {
    const s = this.resolve(schema);
    if (!s || typeof s !== 'object') return [];
    if (s.properties) {
      const req = new Set(s.required || []);
      return Object.entries(s.properties).map(([k, v]) => ({ key: k, schema: v, required: req.has(k) }));
    }
    for (const key of ['anyOf', 'oneOf']) {
      if (Array.isArray(s[key])) {
        const rich = s[key].filter((x) => this.expandable(x));
        if (rich.length === 1) return this.children(rich[0]);
        return rich.map((x, i) => ({ key: `variante ${i + 1}`, schema: x, variant: true }));
      }
    }
    if (Array.isArray(s.prefixItems)) return s.prefixItems.map((x, i) => ({ key: `[${i}]`, schema: x, required: true }));
    if (s.items && this.expandable(s.items)) return this.children(s.items);
    if (s.additionalProperties && typeof s.additionalProperties === 'object' && this.expandable(s.additionalProperties)) return this.children(s.additionalProperties);
    return [];
  }

  expandable(schema) {
    const s = this.resolve(schema);
    if (!s || typeof s !== 'object') return false;
    if (s.properties || s.prefixItems) return true;
    if ((s.anyOf || s.oneOf || []).some((x) => this.expandable(x))) return true;
    if (s.items) return this.expandable(s.items);
    if (s.additionalProperties && typeof s.additionalProperties === 'object') return this.expandable(s.additionalProperties);
    return false;
  }

  /** A schema's fields, one row each; nested schemas expand on demand. */
  tree(schema, depth, seen) {
    const rows = this.children(schema);
    const own = this.resolve(schema);
    if (!rows.length) {
      const c = constraints(own);
      const d = own.description;
      return d || c ? h('div', { class: 'oa-tree-leaf soft' }, [d, c].filter(Boolean).join(' · ')) : null;
    }
    const name = refName(schema);
    const inner = name ? new Set([...seen, name]) : seen;
    return h('div', { class: 'oa-tree' }, rows.map((row) => this.field(row, depth, inner)));
  }

  field({ key, schema, required, variant }, depth, seen) {
    const s = this.resolve(schema);
    const desc = (schema && schema.description) || s.description || '';
    const c = constraints(s);
    const line = h('div', { class: 'oa-field-line' },
      h('code', { class: `oa-key${variant ? ' muted' : ''}` }, key),
      required ? h('span', { class: 'oa-req', title: 'toujours présent' }, '*') : null,
      this.typeChip(schema),
      desc || c ? h('span', { class: 'oa-field-desc soft' }, [firstLine(desc), c].filter(Boolean).join(' · ')) : null);
    const name = refName(schema) || refName(s.items) || refName(s.additionalProperties);
    const recursive = name && seen.has(name);
    if (!this.expandable(schema) || recursive || depth > 8) return h('div', { class: 'oa-field' }, line);
    const details = h('details', { class: 'oa-field' }, h('summary', null, line));
    details.addEventListener('toggle', () => {
      if (details.open && details.childElementCount === 1) details.append(this.tree(schema, depth + 1, seen) || '');
    }, { once: false });
    return details;
  }

  renderSchemas() {
    const all = Object.entries(this.doc.components?.schemas || {});
    text(this.schemaMeta, `${all.length} schémas`);
    this.schemasSection.hidden = !all.length;
    this.schemas.replaceChildren(...all.map(([name, schema]) => {
      const el = h('details', { class: 'oa-schema', 'data-schema': name },
        h('summary', null, h('code', { class: 'oa-schema-name' }, name), h('span', { class: 'oa-type' }, this.typeLabel(schema)),
          schema.description ? h('span', { class: 'soft oa-field-desc' }, firstLine(schema.description)) : null));
      el.addEventListener('toggle', () => {
        if (el.open && el.childElementCount === 1) el.append(this.tree({ $ref: REF + name }, 0, new Set()) || h('div', { class: 'muted oa-tree-leaf' }, 'Aucun champ.'));
      });
      return el;
    }));
  }

  // ---------------------------------------------------------------- try it
  tryIt(path, op) {
    const params = op.parameters || [];
    const inputs = new Map();
    const result = h('div', { class: 'oa-try-result', hidden: true });
    const run = async (e) => {
      e.preventDefault();
      let url = path;
      const query = new URLSearchParams();
      for (const p of params) {
        const v = inputs.get(p.name).value.trim();
        if (p.in === 'path') {
          if (!v) { inputs.get(p.name).focus(); return; }
          url = url.replace(`{${p.name}}`, encodeURIComponent(v));
        } else if (p.in === 'query' && v) query.set(p.name, v);
      }
      const qs = query.toString();
      if (qs) url += `?${qs}`;
      result.hidden = false;
      result.replaceChildren(h('div', { class: 'muted' }, `GET ${url}…`));
      const started = performance.now();
      try {
        const res = await fetch(url, { headers: { Accept: 'application/json' }, cache: 'no-store' });
        const ms = Math.round(performance.now() - started);
        const raw = await res.text();
        let pretty;
        try { pretty = h('pre', { class: 'code', html: jsonHtml(JSON.parse(raw)) }); } catch { pretty = h('pre', { class: 'code' }, raw); }
        result.replaceChildren(
          h('div', { class: 'oa-resp-head' }, h('span', { class: `tag tag-${res.ok ? 'success' : 'danger'} tag-mono` }, String(res.status)), h('code', { class: 'muted' }, `GET ${url}`), h('span', { class: 'muted' }, `${ms} ms`)),
          pretty);
      } catch (err) {
        result.replaceChildren(h('div', { class: 'errbox' }, `GET ${url} : ${err.message}`));
      }
    };
    return h('form', { class: 'oa-try', onsubmit: run },
      h('div', { class: 'sub' }, 'Essayer'),
      params.length ? h('div', { class: 'oa-try-inputs' }, params.map((p) => h('div', { class: 'field' },
        h('label', { class: 'fl', for: `oa-${op.operationId}-${p.name}` }, h('span', null, p.name, p.required ? ' *' : ''), h('span', { class: 'ty' }, `${LOCATION[p.in] || p.in} · ${this.typeLabel(p.schema)}`)),
        inputs.set(p.name, h('input', { class: 'input', id: `oa-${op.operationId}-${p.name}`, required: p.required, placeholder: this.resolve(p.schema).default !== undefined ? String(this.resolve(p.schema).default) : '' })).get(p.name)))) : null,
      h('div', { class: 'form-actions' }, h('button', { class: 'btn btn-primary btn-sm', type: 'submit' }, icon('play'), 'Exécuter')),
      result);
  }
}

function methodOrder(m) { return ['get', 'put', 'post', 'patch', 'delete'].indexOf(m); }
function wrap(label) { return label.includes(' | ') ? `(${label})` : label; }
function firstLine(s) { return String(s || '').split('\n\n')[0].replace(/\s+/g, ' ').trim(); }

/** `/a/{b}` with the parameters highlighted. */
function pathWithParams(path) {
  return path.split(/(\{[^}]+\})/).filter(Boolean).map((part) => (part.startsWith('{') ? h('span', { class: 'oa-param' }, part) : part));
}

/** Validation constraints and default, in a few words. */
function constraints(s) {
  if (!s || typeof s !== 'object') return '';
  const out = [];
  if (s.pattern) out.push(`motif ${s.pattern}`);
  if (s.minLength != null || s.maxLength != null) out.push(`${s.minLength ?? 0}–${s.maxLength ?? '∞'} caractères`);
  if (s.minimum != null && s.minimum !== 0) out.push(`≥ ${s.minimum}`);
  if (s.maximum != null) out.push(`≤ ${s.maximum}`);
  if (s.default !== undefined) out.push(`défaut ${JSON.stringify(s.default)}`);
  return out.join(' · ');
}
