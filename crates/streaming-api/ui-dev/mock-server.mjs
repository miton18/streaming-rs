#!/usr/bin/env node
// Mock of the streaming-rs HTTP API (docs/http-api.md) for UI development.
//
//   node crates/streaming-api/ui-dev/mock-server.mjs [--port 8099]
//
// Serves ../ui at / and /assets/*, simulates 3 workers running the demo tenants with a small
// flow model (capacities, backpressure, bottleneck rule of the contract), accepts every write
// endpoint. Development knobs (not part of the API):
//   POST /__mock/outage    {"on": true}        every /api and /healthz request is dropped
//   POST /__mock/worker    {"worker": "worker-3", "alive": false}
//   POST /__mock/commit    {"on": true}        KV commits of src.events.* become slow
//   POST /__mock/orphan    {"graph": "globex/legacy-export"}  the code no longer declares the graph
//   POST /__mock/reset                         back to the initial state
// The same knobs accept GET with query parameters (?on=1, ?worker=..&alive=0) for curl.

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UI = path.resolve(HERE, '../ui');
const argPort = process.argv.indexOf('--port');
const PORT = Number(argPort > 0 ? process.argv[argPort + 1] : process.env.PORT || 8099);

// ------------------------------------------------------------------ catalog
const T = {
  raw: 'streaming_sim::demo::RawEvent',
  ev: 'streaming_sim::demo::Event',
  wc: 'streaming_core::data::Windowed<u64>',
  json: 'serde_json::value::Value',
  gts: 'alloc::vec::Vec<streaming_warp10::gts::Point>',
  pay: 'streaming_sim::fraud::Payment',
  login: 'streaming_sim::fraud::Login',
  sig: 'streaming_sim::fraud::Signal',
  unit: '()',
};
// cap: records/s one instance can take; ratio: out/in; fields: config schema
const OPS = {
  'demo/generator@1': { kind: 'source', input: T.unit, output: T.raw, description: 'Generates synthetic keyed events at a fixed rate, split over `partitions`.', cap: 40000, fields: { partitions: 'u32', ratePerSec: 'f64+', keys: 'u32+' } },
  'demo/decode@1': { kind: 'map', input: T.raw, output: T.ev, description: 'Decodes raw generator payloads into events.', cap: 3000, fields: {} },
  'demo/key-by@1': { kind: 'key_by', input: T.ev, output: T.ev, description: 'Keys events by their `key` field.', cap: 30000, fields: { field: 'string' } },
  'std/tumbling@1': { kind: 'window', input: '*', output: '*', description: 'Fixed-size, non-overlapping event-time windows.', cap: 40000, fields: { size: 'duration' } },
  'std/sliding@1': { kind: 'window', input: '*', output: '*', description: 'Fixed-size event-time windows starting every `slide`.', cap: 30000, fields: { size: 'duration', slide: 'duration' } },
  'std/count@1': { kind: 'aggregate', input: '*', output: T.wc, description: 'Counts the elements of each key in each window.', cap: 9000, emitCap: 4000, fields: {} },
  'std/identity@1': { kind: 'map', input: '*', output: '*', description: 'Forwards every element unchanged.', cap: 50000, fields: {} },
  'demo/to-row@1': { kind: 'map', input: T.wc, output: T.json, description: 'Turns a windowed count into a JSON row `{windowStart, count}`.', cap: 20000, fields: {} },
  'std/kv-sink@1': { kind: 'sink', input: T.json, output: T.unit, description: 'Writes every element to the KV, readable through `GET .../outputs/{node}`.', cap: 4000, fields: { ttl: 'duration' } },
  'demo/to-gts@1': { kind: 'map', input: T.wc, output: T.gts, description: 'Turns a windowed count into Warp 10 GTS points (one per label set).', cap: 20000, ratio: 20, fields: { class: 'string' } },
  'demo/slow-sink@1': { kind: 'sink', input: '*', output: T.unit, description: 'Discards its input after sleeping `latencyMs` per record: simulates a slow external system.', cap: 50000, fields: { latencyMs: 'u64' } },
  'demo/parse-json@1': { kind: 'map', input: T.raw, output: T.json, description: 'Parses a raw payload as JSON.', cap: 6000, fields: { strict: 'bool' } },
  'demo/enrich@1': { kind: 'map', input: '*', output: '*', description: 'Joins each element with a reference table held in memory.', cap: 7000, fields: { table: 'string', cacheSize: 'u32+', fields: 'json' } },
  'demo/filter@1': { kind: 'map', input: '*', output: '*', description: 'Keeps the elements whose `field` is at least `min`.', cap: 25000, ratio: 0.6, fields: { field: 'string', min: 'f64' } },
  'demo/score@1': { kind: 'map', input: '*', output: T.sig, description: 'Scores each element with a fraud model; emits a signal.', cap: 4500, fields: { model: 'string', threshold: 'f64+', explain: 'bool' } },
  'demo/format@1': { kind: 'map', input: T.wc, output: T.json, description: 'Formats a windowed aggregate as a JSON document.', cap: 20000, fields: { template: 'string' } },
  'warp10/sink@1': { kind: 'sink', input: T.gts, output: T.unit, description: 'Pushes GTS points to a Warp 10 instance (`endpoint`, token from the secret store).', cap: 12000, fields: { endpoint: 'string', batch: 'u32+' } },
};

const DEFAULT_SETTINGS = {
  maxOutOfOrderness: '500ms', idleTimeout: '30s', maxBatch: 1000, linger: '200ms',
  mailboxCapacity: 8, chunkSize: 256, flushInterval: '1s', maxInFlightEpochs: 2,
};

function initialGraphs() {
  const n = (id, op, config) => (config ? { id, op, config } : { id, op });
  return [
    {
      tenant: 'acme', graph: 'count-per-key', version: 3,
      nodes: [
        n('events', 'demo/generator@1', { partitions: 4, ratePerSec: 2000, keys: 50 }),
        n('decode', 'demo/decode@1'),
        n('by-key', 'demo/key-by@1', { field: 'key' }),
        n('win', 'std/tumbling@1', { size: '10s' }),
        n('count', 'std/count@1'),
        n('to-row', 'demo/to-row@1'),
        n('results', 'std/kv-sink@1'),
        n('to-gts', 'demo/to-gts@1', { class: 'demo.count' }),
        n('warp10', 'demo/slow-sink@1', { latencyMs: 0 }),
      ],
      edges: [['events', 'decode'], ['decode', 'by-key'], ['by-key', 'win'], ['win', 'count'], ['count', 'to-row'], ['to-row', 'results'], ['count', 'to-gts'], ['to-gts', 'warp10']],
    },
    {
      tenant: 'globex', graph: 'clicks', version: 1,
      nodes: [
        n('clicks', 'demo/generator@1', { partitions: 2, ratePerSec: 600, keys: 24 }),
        n('parse', 'demo/parse-json@1', { strict: false }),
        n('by-page', 'demo/key-by@1', { field: 'page' }),
        n('win', 'std/sliding@1', { size: '1m', slide: '10s' }),
        n('count', 'std/count@1'),
        n('format', 'demo/format@1', { template: 'page-views' }),
        n('views', 'std/kv-sink@1', { ttl: '1h' }),
      ],
      edges: [['clicks', 'parse'], ['parse', 'by-page'], ['by-page', 'win'], ['win', 'count'], ['count', 'format'], ['format', 'views']],
    },
    {
      tenant: 'globex', graph: 'fraud-scoring', version: 7,
      nodes: [
        n('payments', 'demo/generator@1', { partitions: 3, ratePerSec: 4500, keys: 400 }),
        n('parse-pay', 'demo/parse-json@1', { strict: true }),
        n('enrich-geo', 'demo/enrich@1', { table: 'geoip', cacheSize: 100000, fields: ['country', 'asn'] }),
        n('filter-small', 'demo/filter@1', { field: 'amount', min: 10 }),
        n('logins', 'demo/generator@1', { partitions: 2, ratePerSec: 1200, keys: 400 }),
        n('parse-login', 'demo/parse-json@1', { strict: true }),
        n('enrich-device', 'demo/enrich@1', { table: 'devices', cacheSize: 50000, fields: { fingerprint: true, os: true } }),
        n('union', 'std/identity@1'),
        n('score', 'demo/score@1', { model: 'gbt-v3', threshold: 0.82, explain: false }),
        n('by-account', 'demo/key-by@1', { field: 'account' }),
        n('win-1m', 'std/tumbling@1', { size: '1m' }),
        n('risk', 'std/count@1'),
        n('alerts-fmt', 'demo/format@1', { template: 'alert' }),
        n('alerts', 'std/kv-sink@1', { ttl: '24h' }),
        n('risk-gts', 'demo/to-gts@1', { class: 'fraud.risk' }),
        n('metrics', 'warp10/sink@1', { endpoint: 'https://warp10.globex.example/api/v0/update', batch: 500 }),
        n('by-merchant', 'demo/key-by@1', { field: 'merchant' }),
        n('win-5m', 'std/tumbling@1', { size: '5m' }),
        n('merchant-agg', 'std/count@1'),
        n('merchants', 'std/kv-sink@1', { ttl: '7d' }),
      ],
      edges: [
        ['payments', 'parse-pay'], ['parse-pay', 'enrich-geo'], ['enrich-geo', 'filter-small'], ['filter-small', 'union'],
        ['logins', 'parse-login'], ['parse-login', 'enrich-device'], ['enrich-device', 'union'],
        ['union', 'score'], ['score', 'by-account'], ['by-account', 'win-1m'], ['win-1m', 'risk'],
        ['risk', 'alerts-fmt'], ['alerts-fmt', 'alerts'], ['risk', 'risk-gts'], ['risk-gts', 'metrics'],
        ['score', 'by-merchant'], ['by-merchant', 'win-5m'], ['win-5m', 'merchant-agg'], ['merchant-agg', 'merchants'],
      ],
    },
    {
      tenant: 'globex', graph: 'legacy-export', version: 2,
      nodes: [n('orders', 'demo/generator@1', { partitions: 0, ratePerSec: 100, keys: 10 }), n('fmt', 'std/identity@1'), n('out', 'std/kv-sink@1')],
      edges: [['orders', 'fmt'], ['fmt', 'out']],
    },
  ];
}

// ------------------------------------------------------------------ helpers
const parseDuration = (s) => {
  if (typeof s === 'number') return s;
  const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$/.exec(String(s));
  if (!m) return null;
  return Number(m[1]) * { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 }[m[2]];
};
const jitter = (seed, t, amp = 0.04) => 1 + amp * Math.sin(t / 1700 + seed) + amp * 0.5 * Math.sin(t / 530 + seed * 3.1);
const hash = (s) => { let x = 2166136261; for (const c of s) { x ^= c.charCodeAt(0); x = Math.imul(x, 16777619); } return (x >>> 0) / 4294967296; };
const clamp = (v, a = 0, b = 1) => Math.max(a, Math.min(b, v));
const r3 = (v) => Math.round(v * 1000) / 1000;

function validateConfig(node, cfg) {
  const op = OPS[node.op];
  if (!op) return `node \`${node.id}\`: unknown operator \`${node.op}\``;
  if (cfg == null || typeof cfg !== 'object' || Array.isArray(cfg)) return `node \`${node.id}\` (${node.op}): invalid configuration: expected a map`;
  const names = Object.keys(op.fields);
  for (const [k, v] of Object.entries(cfg)) {
    const ty = op.fields[k];
    const pre = `node \`${node.id}\` (${node.op}): invalid configuration`;
    if (!ty) return `${pre}: unknown field \`${k}\`, ${names.length ? `expected one of ${names.map((x) => `\`${x}\``).join(', ')}` : 'this operator takes no configuration'}`;
    const show = JSON.stringify(v);
    if (ty === 'u32+' || ty === 'u32' || ty === 'u64') {
      if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) return `${pre}: ${k}: invalid value: ${show}, expected ${ty === 'u64' ? 'u64' : 'u32'}`;
      if (ty === 'u32+' && v === 0) return `${pre}: \`${k}\` must be positive`;
      if (k === 'partitions' && v > 64) return `${pre}: \`partitions\` must not exceed 64`;
      if (k === 'latencyMs' && v > 60000) return `${pre}: \`latencyMs\` must not exceed 60000`;
    } else if (ty === 'f64' || ty === 'f64+') {
      if (typeof v !== 'number') return `${pre}: ${k}: invalid type: ${typeof v === 'string' ? `string ${show}` : show}, expected f64`;
      if (ty === 'f64+' && v < 0) return `${pre}: \`${k}\` must not be negative`;
      if (k === 'ratePerSec' && v > 1e6) return `${pre}: \`ratePerSec\` must not exceed 1000000`;
    } else if (ty === 'duration') {
      const d = parseDuration(v);
      if (d == null) return `${pre}: ${k}: invalid duration ${show} (expected e.g. "500ms", "10s", "1m")`;
      if (d <= 0) return `${pre}: \`${k}\` must be positive`;
    } else if (ty === 'string') {
      if (typeof v !== 'string') return `${pre}: ${k}: invalid type: ${show}, expected a string`;
    } else if (ty === 'bool') {
      if (typeof v !== 'boolean') return `${pre}: ${k}: invalid type: ${show}, expected a boolean`;
    }
  }
  if (node.op === 'std/sliding@1' && parseDuration(cfg.slide) > parseDuration(cfg.size)) return `node \`${node.id}\` (${node.op}): invalid configuration: \`slide\` must not exceed \`size\``;
  return null;
}

// ------------------------------------------------------------------ simulation state
let S;
function reset() {
  const now = Date.now();
  S = {
    started: now,
    epoch: 41,
    outage: false,
    slowCommit: false,
    workers: ['worker-1', 'worker-2', 'worker-3'].map((w, i) => ({ worker: w, alive: true, generation: now - 3600e3 * (i + 1) - 1234 * i, deadSince: 0 })),
    placement: { version: 9, computedBy: 'worker-1', computedMs: now - 600e3, assignments: {} },
    tenants: new Map([
      ['acme', { tenant: 'acme', displayName: 'Acme Corporation', createdMs: now - 86400e3 }],
      ['globex', { tenant: 'globex', displayName: 'Globex', createdMs: now - 86400e3 }],
    ]),
    graphs: new Map(),
    actors: new Map(),   // counters per actor
    slices: new Map(),   // per-slice progress
    outputs: new Map(),  // "t/g/node" -> rows (newest first)
    kv: { keys: 1840, bytes: 2.4e6, reads: 1.2e6, writes: 3.4e5 },
    last: now,
  };
  for (const g of initialGraphs()) addGraph(g, Math.floor(now - 86400e3 * hash(g.graph)));
  place(now);
  prefillOutputs(now);
}

function planErrorOf(spec) {
  for (const n of spec.nodes) {
    if (!OPS[n.op]) return `unknown operator \`${n.op}\` (node \`${n.id}\`)`;
    if (OPS[n.op].kind === 'source' && (n.config?.partitions ?? 1) === 0) return `source \`${n.id}\` (${n.op}): cannot plan a source with 0 partitions`;
  }
  return null;
}

// the parameters an operator exposes, as the catalog describes them
const PARAM_KIND = { u32: 'integer', 'u32+': 'integer', u64: 'integer', f64: 'number', 'f64+': 'number', bool: 'boolean', duration: 'duration', string: 'string', json: 'json' };
function paramsOf(op) {
  return Object.entries(op.fields).map(([name, ty]) => ({ name, kind: PARAM_KIND[ty] || 'string', description: '', required: false, choices: [] }));
}

function addGraph(spec, updated) {
  const key = `${spec.tenant}/${spec.graph}`;
  const prev = S.graphs.get(key);
  const g = {
    spec: { tenant: spec.tenant, graph: spec.graph, version: spec.version, settings: { ...DEFAULT_SETTINGS, ...(spec.settings || {}) }, nodes: spec.nodes, edges: spec.edges },
    desired: prev?.desired || { state: 'running', pausedNodes: [], updatedMs: updated },
    code: prev?.code || spec.nodes, // the nodes as the code of the application built them
    updatedMs: updated,
    planError: planErrorOf(spec),
    plannedMs: updated + 800,
    transition: prev?.transition || null, // {state, at}
    deleting: false,
    lag: prev?.lag || 0,
  };
  S.graphs.set(key, g);
  return g;
}

/** Slices of a graph and the nodes (with actor kind) each one runs. */
function plan(g) {
  const { nodes, edges } = g.spec;
  if (g.planError) return [];
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const succ = new Map(nodes.map((n) => [n.id, []]));
  for (const [a, b] of edges) succ.get(a)?.push(b);
  const kindOf = (id) => OPS[byId.get(id)?.op]?.kind;
  const walk = (start, rootKind) => {
    const members = new Map([[start, rootKind]]);
    const stack = [...succ.get(start)];
    while (stack.length) {
      const n = stack.pop();
      if (members.has(n)) continue;
      const k = kindOf(n);
      members.set(n, k);
      if (k !== 'aggregate') stack.push(...succ.get(n));
    }
    return members;
  };
  const out = [];
  for (const n of nodes) {
    const k = kindOf(n.id);
    if (k === 'source') {
      const parts = n.config?.partitions ?? 1;
      for (let p = 0; p < parts; p += 1) out.push({ id: `src.${n.id}.${p}`, root: n.id, members: walk(n.id, 'source'), partitions: parts });
    } else if (k === 'aggregate') {
      out.push({ id: `agg.${n.id}`, root: n.id, members: walk(n.id, 'emitter'), partitions: 1 });
    }
  }
  return out;
}

function place(now) {
  const alive = S.workers.filter((w) => w.alive).map((w) => w.worker);
  const all = S.workers.map((w) => w.worker);
  const a = {};
  let i = 0;
  for (const [key, g] of S.graphs) {
    for (const sl of plan(g)) {
      const prevW = S.placement.assignments[`${key}/${sl.id}`];
      let w = prevW && alive.includes(prevW) ? prevW : null;
      if (!w) w = alive.length ? alive[(i + Math.floor(hash(key) * 3)) % alive.length] : all[0];
      a[`${key}/${sl.id}`] = w;
      i += 1;
    }
  }
  const changed = JSON.stringify(a) !== JSON.stringify(S.placement.assignments);
  if (changed) { S.placement = { version: S.placement.version + 1, computedBy: alive[0] || 'worker-1', computedMs: now, assignments: a }; S.epoch += 1; }
}

// ------------------------------------------------------------------ flow model
/** Everything a worker would report right now: actor and slice snapshots per graph. */
function snapshot(now) {
  const dt = Math.min(5, (now - S.last) / 1000);
  S.last = now;
  const aliveSet = new Set(S.workers.filter((w) => w.alive).map((w) => w.worker));
  const perGraph = new Map();
  for (const [key, g] of S.graphs) {
    const actors = [];
    const slices = [];
    const settings = g.spec.settings;
    const byId = new Map(g.spec.nodes.map((n) => [n.id, n]));
    const slicePlan = plan(g);
    const st = g.desired.state;
    // transitions: draining -> nothing after 4 s; stopped -> running goes through starting for 2 s
    let phase = st;
    if (g.transition) {
      const age = now - g.transition.at;
      if (g.transition.state === 'draining' && age > 4000) phase = 'stopped';
      if (g.transition.state === 'starting') phase = age < 2000 ? 'starting' : st;
    }
    if (st === 'stopped' || g.deleting) phase = 'stopped';
    const mailbox = settings.mailboxCapacity;
    const flushMs = parseDuration(settings.flushInterval) || 1000;
    const paused = new Set(g.desired.pausedNodes);
    let worstF = 1;

    for (const sl of slicePlan) {
      const assigned = S.placement.assignments[`${key}/${sl.id}`] || null;
      const reporting = assigned && aliveSet.has(assigned) && phase !== 'stopped' ? assigned : null;
      const members = sl.members;
      // per-instance demand along the slice
      const order = [...members.keys()];
      const preds = new Map(order.map((id) => [id, []]));
      for (const [a, b] of g.spec.edges) if (members.has(a) && members.has(b) && b !== sl.root) preds.get(b).push(a);
      // topological order inside the slice
      const topo = []; const seen = new Set();
      const visit = (id) => { if (seen.has(id)) return; seen.add(id); for (const p of preds.get(id)) visit(p); topo.push(id); };
      for (const id of order) visit(id);
      const demandIn = new Map(); const demandOut = new Map(); const cap = new Map();
      const rootNode = byId.get(sl.root);
      const seed = hash(`${key}/${sl.id}`) * 10;
      for (const id of topo) {
        const node = byId.get(id);
        const op = OPS[node.op];
        const kind = members.get(id);
        let din;
        if (id === sl.root && kind === 'source') {
          din = (node.config?.ratePerSec ?? 1000) / sl.partitions * jitter(seed, now, 0.05);
          demandIn.set(id, 0);
          demandOut.set(id, din);
          cap.set(id, op.cap);
          continue;
        }
        if (id === sl.root && kind === 'emitter') {
          // rows per second: distinct keys of the feeding sources / window length
          const win = g.spec.edges.find(([, b]) => b === id)?.[0];
          const wn = byId.get(win);
          const size = parseDuration(wn?.config?.slide ?? wn?.config?.size) || 10000;
          const srcKeys = g.spec.nodes.filter((n) => OPS[n.op]?.kind === 'source').reduce((a, n) => Math.max(a, n.config?.keys ?? 20), 0);
          const flowing = phase === 'running' ? 1 : 0;
          din = 0;
          demandIn.set(id, 0);
          demandOut.set(id, (srcKeys / (size / 1000)) * flowing * jitter(seed, now, 0.08));
          cap.set(id, op.emitCap || 4000);
          continue;
        }
        din = preds.get(id).reduce((a, p) => a + demandOut.get(p), 0);
        demandIn.set(id, din);
        let c = op.cap;
        if (node.op === 'demo/slow-sink@1') { const l = node.config?.latencyMs ?? 0; c = l > 0 ? 1000 / l : op.cap; }
        if (paused.has(id)) c = 0;
        cap.set(id, c);
        demandOut.set(id, kind === 'aggregate' ? 0 : din * (op.ratio ?? 1));
      }
      // throttle factor: the slice runs as fast as its slowest member allows
      let f = 1; let limiter = null;
      for (const id of topo) {
        const kind = members.get(id);
        const need = kind === 'source' || kind === 'emitter' ? demandOut.get(id) : demandIn.get(id);
        if (need <= 0) continue;
        const ratio = cap.get(id) / need;
        if (ratio < f) { f = ratio; limiter = id; }
      }
      // slow KV commits throttle the source slices
      let commitBusy = clamp((6 + 6 * hash(sl.id)) / flushMs * jitter(seed, now, 0.2), 0, 1);
      let commitMs = (6 + 6 * hash(sl.id)) * jitter(seed + 1, now, 0.15);
      if (S.slowCommit && sl.id.startsWith('src.') && key === 'acme/count-per-key') {
        commitMs = 870 * jitter(seed, now, 0.06); commitBusy = clamp(0.9 * jitter(seed, now, 0.03));
        if (f > 0.48) { f = 0.48; limiter = '__commit'; }
      }
      if (phase !== 'running' && phase !== 'draining') f = 0;
      const ancestors = new Set();
      if (limiter && limiter !== '__commit') { const up = (id) => { for (const p of preds.get(id)) { if (!ancestors.has(p)) { ancestors.add(p); up(p); } } }; up(limiter); }
      if (limiter === '__commit') ancestors.add(sl.root);
      if (sl.id.startsWith('src.')) worstF = Math.min(worstF, f);
      if (!reporting) continue;

      const actorState = phase === 'starting' ? 'starting' : phase === 'paused' ? 'paused' : phase === 'draining' ? 'draining' : 'running';
      for (const id of topo) {
        const node = byId.get(id);
        const kind = members.get(id);
        const c = cap.get(id);
        const k = `${key}/${sl.id}/${id}/${kind}`;
        const acc = S.actors.get(k) || { recordsIn: 0, recordsOut: 0, lateDropped: 0, errors: 0, lastError: null };
        S.actors.set(k, acc);
        const throttled = f < 0.999;
        let inR = demandIn.get(id) * f; let outR = demandOut.get(id) * f;
        if (phase === 'draining') { const age = (now - g.transition.at) / 4000; inR *= clamp(1 - age); outR *= clamp(1 - age); }
        let busy;
        if (kind === 'source' || kind === 'emitter') busy = c > 0 ? outR / c : 0;
        else busy = c > 0 ? inR / c : 0;
        if (id === limiter && throttled && !paused.has(id)) busy = clamp(0.955 * jitter(seed, now, 0.015));
        busy = clamp(busy * (1 + 0.04 * Math.sin(now / 900 + seed + id.length)));
        let bp = 0;
        if (throttled && ancestors.has(id) && actorState === 'running') bp = clamp(1 - busy - 0.03 - 0.02 * Math.sin(now / 1200 + seed));
        if (paused.has(id) || actorState !== 'running') { busy = actorState === 'draining' ? busy : 0; bp = actorState === 'draining' ? bp : 0; }
        if (paused.has(id)) { inR = 0; outR = 0; }
        const idle = clamp(1 - busy - bp);
        const nIn = kind === 'source' || kind === 'emitter' ? 0 : preds.get(id).length;
        const qcap = nIn * mailbox;
        let qlen = 0;
        if (qcap) {
          if ((ancestors.has(id) && throttled) || id === limiter || paused.has(id)) qlen = qcap - (Math.sin(now / 700 + seed) > 0.6 ? 1 : 0);
          else qlen = Math.round(qcap * clamp(busy * 0.35));
        }
        acc.recordsIn += (kind === 'source' ? 0 : inR) * dt;
        acc.recordsOut += (kind === 'aggregate' ? 0 : outR) * dt;
        if (node.op === 'demo/decode@1' && sl.id === 'src.events.2' && Math.random() < 0.02 * dt && inR > 0) {
          acc.errors += 1;
          acc.lastError = `decode: invalid UTF-8 sequence at byte ${Math.floor(Math.random() * 60)} (offset ${Math.floor(S.actors.get(`${key}/${sl.id}/${sl.root}/source`)?.recordsOut || 0)})`;
        }
        if (kind === 'aggregate' && throttled && Math.random() < 0.3 * dt) acc.lateDropped += Math.ceil(Math.random() * 4);
        actors.push({
          tenant: g.spec.tenant, graph: g.spec.graph, slice: sl.id, node: id, kind, op: node.op,
          state: paused.has(id) && actorState === 'running' ? 'paused' : actorState,
          recordsIn: Math.floor(acc.recordsIn), recordsOut: Math.floor(acc.recordsOut),
          inPerSec: r3(kind === 'source' ? 0 : inR), outPerSec: r3(kind === 'aggregate' ? 0 : outR),
          busy: r3(busy), idle: r3(idle), backpressured: r3(bp),
          queueLen: qlen, queueCap: qcap, lateDropped: acc.lateDropped, errors: acc.errors, lastError: acc.lastError,
          worker: reporting,
        });
      }
      // commit actor
      actors.push({
        tenant: g.spec.tenant, graph: g.spec.graph, slice: sl.id, node: sl.root, kind: 'commit', op: rootNode.op, state: actorState,
        recordsIn: 0, recordsOut: 0, inPerSec: 0, outPerSec: 0,
        busy: r3(actorState === 'running' ? commitBusy : 0), idle: r3(actorState === 'running' ? 1 - commitBusy : 1), backpressured: 0,
        queueLen: 0, queueCap: 0, lateDropped: 0, errors: 0, lastError: null, worker: reporting,
      });
      // slice progress
      const sp = S.slices.get(`${key}/${sl.id}`) || { epoch: 1000 + Math.floor(hash(sl.id) * 5000), commits: 0, acc: 0, restarts: sl.id === 'src.events.2' ? 1 : 0, fenced: sl.id === 'agg.risk' ? 2 : 0 };
      S.slices.set(`${key}/${sl.id}`, sp);
      if (actorState === 'running' && f > 0) {
        sp.acc += dt * 1000 * Math.min(1, f + 0.2) / Math.max(flushMs, commitMs);
        while (sp.acc >= 1) { sp.acc -= 1; sp.epoch += 1; sp.commits += 1; }
      }
      const srcActor = S.actors.get(`${key}/${sl.id}/${sl.root}/${sl.id.startsWith('src.') ? 'source' : 'emitter'}`);
      let position = null; let wm = null;
      const ooo = parseDuration(settings.maxOutOfOrderness) || 500;
      if (sl.id.startsWith('src.')) {
        position = [0, Math.floor(srcActor?.recordsOut || 0), 0];
        wm = Math.floor(now - ooo - 300 - g.lag * 1000 - 120 * hash(sl.id));
      } else {
        const win = g.spec.edges.find(([, b]) => b === sl.root)?.[0];
        const size = parseDuration(byId.get(win)?.config?.size) || 10000;
        const end = Math.floor((now - ooo - g.lag * 1000) / size) * size;
        position = [end, end - size, Math.floor((srcActor?.recordsOut || 0) % 50)];
        wm = end;
      }
      slices.push({
        tenant: g.spec.tenant, graph: g.spec.graph, slice: sl.id, state: actorState, epoch: sp.epoch, position, watermarkMs: wm,
        commits: sp.commits, commitMs: r3(commitMs), inFlightEpochs: f < 0.999 ? settings.maxInFlightEpochs : (Math.sin(now / 800 + seed) > 0.3 ? 1 : 0),
        fenced: sp.fenced, restarts: sp.restarts,
        lastError: sp.restarts ? 'slice restarted from epoch after: decode: invalid UTF-8 sequence' : null,
        worker: reporting,
      });
    }
    // event-time lag of the sources grows while they are throttled
    if (phase === 'running') g.lag = worstF < 0.999 ? g.lag + dt * (1 - worstF) : Math.max(0, g.lag - dt * 0.8);
    else if (phase === 'paused') g.lag += dt;
    perGraph.set(key, { actors, slices, slicePlan, phase });
  }
  // KV activity
  S.kv.reads += dt * (380 + 60 * Math.sin(now / 3000));
  S.kv.writes += dt * (140 + 30 * Math.sin(now / 2100));
  S.kv.keys = 1840 + Math.floor((now - S.started) / 4000);
  S.kv.bytes = 2.4e6 + (now - S.started) * 12;
  // re-place slices of dead workers after 6 s (member timeout + controller)
  for (const w of S.workers) if (!w.alive && w.deadSince && now - w.deadSince > 6000 && Object.values(S.placement.assignments).includes(w.worker)) place(now);
  return perGraph;
}

// ------------------------------------------------------------------ aggregation (contract rules)
const STATE_ORDER = ['failed', 'starting', 'paused', 'draining', 'running', 'stopped'];

function graphDetail(key, snap, now) {
  const g = S.graphs.get(key);
  const { actors, slices, slicePlan } = snap.get(key);
  const nodesOut = [];
  const paused = new Set(g.desired.pausedNodes);
  for (const n of g.spec.nodes) {
    const op = OPS[n.op];
    const inst = actors.filter((a) => a.node === n.id && a.kind !== 'commit');
    const hot = inst.reduce((m, a) => (!m || a.busy > m.busy ? a : m), null);
    const hasEmitter = inst.some((a) => a.kind === 'emitter');
    const sum = (arr, f) => arr.reduce((s, a) => s + a[f], 0);
    const ins = inst.filter((a) => a.kind !== 'emitter');
    const outs = hasEmitter ? inst.filter((a) => a.kind !== 'aggregate') : inst;
    let state = 'pending';
    if (inst.length) state = STATE_ORDER.find((s) => inst.some((a) => a.state === s)) || 'running';
    nodesOut.push({
      id: n.id, op: n.op, kind: op?.kind ?? null, description: op?.description ?? '',
      inputType: op?.input ?? null, outputType: op?.output ?? null, config: n.config ?? {},
      paused: paused.has(n.id), bottleneck: false, state,
      actors: inst.length, workers: [...new Set(inst.map((a) => a.worker))].sort(),
      recordsIn: sum(ins, 'recordsIn'), recordsOut: sum(outs, 'recordsOut'),
      inPerSec: r3(sum(ins, 'inPerSec')), outPerSec: r3(sum(outs, 'outPerSec')),
      busy: hot ? hot.busy : 0, idle: hot ? hot.idle : 0, backpressured: hot ? hot.backpressured : 0,
      queueLen: sum(inst, 'queueLen'), queueCap: sum(inst, 'queueCap'),
      lateDropped: sum(inst, 'lateDropped'), errors: sum(inst, 'errors'), lastError: inst.find((a) => a.lastError)?.lastError ?? null,
      instances: inst,
    });
  }
  const byId = new Map(nodesOut.map((n) => [n.id, n]));
  // bottleneck
  const preds = new Map(g.spec.nodes.map((n) => [n.id, []]));
  for (const [a, b] of g.spec.edges) preds.get(b).push(a);
  const upstream = (id) => { const s = new Set(); const go = (x) => { for (const p of preds.get(x)) if (!s.has(p)) { s.add(p); go(p); } }; go(id); return s; };
  const topoIdx = new Map(g.spec.nodes.map((n, i) => [n.id, i]));
  let bottleneck = null;
  const cands = nodesOut.filter((n) => n.busy >= 0.5 && n.backpressured < 0.3 && [...upstream(n.id)].some((u) => byId.get(u).backpressured >= 0.3));
  if (cands.length) {
    cands.sort((a, b) => b.busy - a.busy || topoIdx.get(b.id) - topoIdx.get(a.id));
    const c = cands[0];
    const up = [...upstream(c.id)].map((u) => byId.get(u)).filter((u) => u.backpressured >= 0.3).sort((a, b) => b.backpressured - a.backpressured)[0];
    bottleneck = { node: c.id, kind: c.kind, busy: c.busy, reason: `\`${c.id}\` is busy ${Math.round(c.busy * 100)}% while upstream \`${up.id}\` is backpressured ${Math.round(up.backpressured * 100)}%` };
  } else {
    const commit = actors.filter((a) => a.kind === 'commit' && a.busy >= 0.5).find((a) => (byId.get(a.node)?.backpressured ?? 0) >= 0.3);
    if (commit) {
      const root = byId.get(commit.node);
      bottleneck = { node: commit.node, kind: 'commit', busy: commit.busy, reason: `commit actor of slice \`${commit.slice}\` is busy ${Math.round(commit.busy * 100)}% while \`${commit.node}\` is backpressured ${Math.round(root.backpressured * 100)}%` };
    }
  }
  if (bottleneck && byId.has(bottleneck.node)) byId.get(bottleneck.node).bottleneck = true;
  const edges = g.spec.edges.map(([a, b]) => { const d = byId.get(b); return { from: a, to: b, fill: d.queueCap ? r3(d.queueLen / d.queueCap) : 0 }; });
  const reported = new Map(slices.map((s) => [s.slice, s]));
  const sliceList = slicePlan.map((sl) => {
    const s = reported.get(sl.id);
    const assigned = S.placement.assignments[`${key}/${sl.id}`] ?? null;
    if (s) return { ...s, assignedTo: assigned };
    return { tenant: g.spec.tenant, graph: g.spec.graph, slice: sl.id, state: 'starting', epoch: 0, position: null, watermarkMs: null, commits: 0, commitMs: 0, inFlightEpochs: 0, fenced: 0, restarts: 0, lastError: null, worker: null, assignedTo: assigned };
  });
  const src = sliceList.filter((s) => s.slice.startsWith('src.'));
  const watermark = src.length && src.every((s) => s.watermarkMs != null) ? Math.min(...src.map((s) => s.watermarkMs)) : null;
  return {
    tenant: g.spec.tenant, graph: g.spec.graph, version: g.spec.version, state: g.deleting ? 'stopped' : g.desired.state,
    orphan: !!g.orphan,
    spec: g.spec,
    plan: { specVersion: g.spec.version, partitions: Object.fromEntries(g.spec.nodes.filter((n) => OPS[n.op]?.kind === 'source').map((n) => [n.id, n.config?.partitions ?? 1])), slices: slicePlan.map((s) => s.id), plannedMs: g.plannedMs, error: g.planError },
    desired: g.desired,
    bottleneck, watermarkMs: watermark,
    nodes: nodesOut, edges, slices: sliceList,
  };
}

function summary(key, snap, now) {
  const d = graphDetail(key, snap, now);
  const g = S.graphs.get(key);
  return {
    tenant: d.tenant, graph: d.graph, version: d.version, state: d.state, orphan: d.orphan,
    nodes: d.nodes.length, slices: d.slices.length, actors: snap.get(key).actors.length,
    inPerSec: r3(d.nodes.filter((n) => n.kind === 'source').reduce((s, n) => s + n.outPerSec, 0)),
    outPerSec: r3(d.nodes.filter((n) => n.kind === 'sink').reduce((s, n) => s + n.outPerSec, 0)),
    bottleneck: d.bottleneck, watermarkMs: d.watermarkMs, updatedMs: g.updatedMs, error: g.planError,
  };
}

// ------------------------------------------------------------------ outputs
function sinkRowsFor(key, nodeId, windowStart, keys) {
  const g = S.graphs.get(key);
  const rows = [];
  const rate = g.spec.nodes.find((n) => OPS[n.op]?.kind === 'source')?.config?.ratePerSec ?? 100;
  for (let i = 0; i < keys; i += 1) {
    let k; let value;
    const c = Math.max(1, Math.round((rate * 10 / keys) * (0.6 + 0.8 * hash(`${windowStart}/${i}`))));
    if (key === 'acme/count-per-key') { k = `user-${String(i).padStart(2, '0')}`; value = { windowStart: windowStart, count: c }; }
    else if (key === 'globex/clicks') { k = `/products/${100 + i}`; value = { windowStart: windowStart, windowEnd: windowStart + 60000, page: `/products/${100 + i}`, views: Math.round(c / 4) }; }
    else if (nodeId === 'alerts') { k = `acc-${4000 + i * 7}`; value = { account: k, windowStart: windowStart, signals: 1 + Math.floor(hash(`${windowStart}a${i}`) * 6), score: Math.round((0.82 + 0.18 * hash(`${windowStart}s${i}`)) * 1000) / 1000, reasons: ['velocity', 'geo-mismatch'].slice(0, 1 + (i % 2)) }; }
    else { k = `m-${100 + i}`; value = { merchant: k, windowStart: windowStart, count: c * 3 }; }
    rows.push({ id: `agg.${nodeId}/${windowStart + 10000}.${windowStart}.${i}/${Math.floor(hash(`${k}${windowStart}`) * 0xffffffff).toString(16).padStart(8, '0')}`, ts: windowStart + 9999, key: k, value });
  }
  return rows;
}
const SINK_WINDOWS = { 'acme/count-per-key/results': [10000, 50], 'globex/clicks/views': [10000, 24], 'globex/fraud-scoring/alerts': [60000, 6], 'globex/fraud-scoring/merchants': [300000, 12] };
function prefillOutputs(now) {
  for (const [k, [size, keys]] of Object.entries(SINK_WINDOWS)) {
    const [t, g, n] = k.split('/');
    const rows = [];
    const lastEnd = Math.floor((now - 1500) / size) * size;
    for (let w = 4; w >= 1; w -= 1) rows.unshift(...sinkRowsFor(`${t}/${g}`, n, lastEnd - w * size, keys));
    rows.sort((a, b) => b.ts - a.ts || a.key.localeCompare(b.key));
    S.outputs.set(k, { rows, lastEnd: lastEnd - size });
  }
}
function advanceOutputs(now, snap) {
  for (const [k, [size, keys]] of Object.entries(SINK_WINDOWS)) {
    const [t, g, n] = k.split('/');
    const key = `${t}/${g}`;
    const gr = S.graphs.get(key);
    if (!gr) continue;
    const o = S.outputs.get(k);
    const end = Math.floor((now - 1500 - gr.lag * 1000) / size) * size;
    const sinkFlowing = snap.get(key)?.actors.some((a) => a.node === n && a.state === 'running' && a.inPerSec > 0);
    if (end > o.lastEnd && sinkFlowing) {
      const fresh = sinkRowsFor(key, n, end - size, keys);
      o.rows = [...fresh.sort((a, b) => a.key.localeCompare(b.key)), ...o.rows].slice(0, 2000);
      o.lastEnd = end;
    }
  }
}

function dot(key) {
  const g = S.graphs.get(key);
  const lines = [`digraph "${key}" {`, '  rankdir=LR;', '  node [shape=box, fontname="Inter"];'];
  for (const n of g.spec.nodes) lines.push(`  "${n.id}" [label="${n.id}\\n${n.op}"];`);
  for (const [a, b] of g.spec.edges) lines.push(`  "${a}" -> "${b}";`);
  lines.push('}');
  return `${lines.join('\n')}\n`;
}

// ------------------------------------------------------------------ cluster
function cluster(now, snap) {
  const workers = S.workers.map((w, i) => {
    const mine = []; const sl = new Set(); const tenants = {};
    for (const [, s] of snap) for (const a of s.actors) if (a.worker === w.worker) {
      mine.push(a); sl.add(`${a.tenant}/${a.graph}/${a.slice}`);
      const t = tenants[a.tenant] || (tenants[a.tenant] = { actors: 0, inPerSec: 0, busy: 0 });
      t.actors += 1; t.inPerSec += a.inPerSec; t.busy += a.busy;
    }
    for (const t of Object.values(tenants)) { t.inPerSec = r3(t.inPerSec); t.busy = r3(t.busy); }
    const busy = mine.reduce((s, a) => s + a.busy, 0);
    const cpu = w.alive ? clamp(0.04 + busy / 4 + 0.02 * Math.sin(now / 2000 + i)) : 0;
    const memBytes = Math.round((96 + mine.length * 3.2 + 8 * Math.sin(now / 5000 + i)) * 1048576);
    return {
      worker: w.worker, alive: w.alive, generation: w.generation,
      heartbeatMs: w.alive ? now - Math.floor(((now / 37) + i * 300) % 1000) : w.deadSince - 400,
      load: { cpu: r3(cpu), memory: r3(memBytes / (1024 * 1048576)), memoryBytes: memBytes },
      actors: w.alive ? mine.length : 0, slices: w.alive ? sl.size : 0,
      apiAddr: i === 0 ? '127.0.0.1:8080' : null, version: '0.1.0',
      tenants: w.alive ? tenants : {},
    };
  });
  return { epoch: S.epoch, nowMs: now, workers, placement: S.placement, kv: { keys: S.kv.keys, bytes: Math.round(S.kv.bytes), reads: Math.floor(S.kv.reads), writes: Math.floor(S.kv.writes) } };
}

// ------------------------------------------------------------------ HTTP
const MIME = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8', '.woff2': 'font/woff2', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.txt': 'text/plain; charset=utf-8' };

function send(res, status, body, type = 'application/json') {
  const data = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(data);
}
// Errors follow the server's contract: `message` is the template of `code`
// with the values of `context` (see src/error.rs). Validating operator
// configurations at PUT is mock-only, hence MOCK_CONFIG_INVALID, which the UI
// does not know and shows with the English message.
const TEMPLATES = {
  BODY_NOT_JSON: 'the request body is not valid JSON (line {line}, column {column})',
  BODY_INVALID: 'the request body is invalid: {detail} (line {line}, column {column})',
  GRAPH_EMPTY: 'the graph has no node',
  EDGE_UNKNOWN_NODE: 'edge `{from}` -> `{to}` references unknown node `{unknown}`',
  GRAPH_NOT_FOUND: 'unknown graph `{tenant}/{graph}`',
  TENANT_NOT_FOUND: 'unknown tenant `{tenant}`',
  TENANT_DISPLAY_NAME_CONFLICT: 'tenant `{tenant}` already exists with another display name',
  NODE_NOT_FOUND: 'unknown node `{node}` in graph `{tenant}/{graph}`',
  GRAPH_DECLARED_BY_CODE: 'graph `{tenant}/{graph}` is declared by the code of the application: remove it from the code to delete it',
  GRAPH_NOT_IN_CODE: 'graph `{tenant}/{graph}` is no longer declared by the code of the application: it can only be deleted',
  NODE_CONFIG_NOT_OBJECT: 'the configuration of node `{node}` in graph `{tenant}/{graph}` is not an object and cannot be patched',
  NODE_FIELD_NOT_EXPOSED: 'node `{node}` in graph `{tenant}/{graph}` does not expose the configuration field `{field}`',
  ROUTE_NOT_FOUND: 'nothing is served at `{path}`',
  MOCK_CONFIG_INVALID: '{detail}',
  MOCK_FAILURE: 'the mock failed: {detail}',
};
const textOf = (v) => (typeof v === 'string' ? v : Array.isArray(v) ? v.map(textOf).join(', ') : JSON.stringify(v));
const err = (res, status, code, context = {}) => send(res, status, {
  message: TEMPLATES[code].replace(/\{([^{}]*)\}/g, (all, k) => (k in context ? textOf(context[k]) : all)),
  code,
  context,
});

function serveStatic(res, urlPath) {
  let rel = urlPath === '/' ? '/index.html' : urlPath;
  let file = path.join(UI, path.normalize(rel).replace(/^(\.\.[/\\])+/, ''));
  if (!file.startsWith(UI) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    if (urlPath.startsWith('/assets/')) return err(res, 404, 'ROUTE_NOT_FOUND', { path: urlPath });
    file = path.join(UI, 'index.html');
  }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
  fs.createReadStream(file).pipe(res);
}

function readBody(req) {
  return new Promise((resolve) => {
    let s = '';
    req.on('data', (c) => { s += c; });
    req.on('end', () => {
      if (!s) return resolve({ ok: true, value: undefined });
      try { resolve({ ok: true, value: JSON.parse(s) }); } catch (e) { resolve({ ok: false, code: 'BODY_NOT_JSON', context: { line: 1, column: 0 } }); }
    });
  });
}

function bump(g) { S.epoch += 1; g.desired.updatedMs = Date.now(); }

async function handle(req, res) {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  const m = req.method;

  if (p.startsWith('/__mock/')) {
    const body = m === 'POST' ? (await readBody(req)).value || {} : {};
    const q = Object.fromEntries(url.searchParams);
    const on = (v) => v === true || v === '1' || v === 'true' || v === 'on';
    if (p === '/__mock/outage') { S.outage = on(body.on ?? q.on); return send(res, 200, { outage: S.outage }); }
    if (p === '/__mock/commit') { S.slowCommit = on(body.on ?? q.on); return send(res, 200, { slowCommit: S.slowCommit }); }
    if (p === '/__mock/worker') {
      const w = S.workers.find((x) => x.worker === (body.worker ?? q.worker));
      if (!w) return err(res, 404, 'ROUTE_NOT_FOUND', { path: p });
      w.alive = on(body.alive ?? q.alive);
      w.deadSince = w.alive ? 0 : Date.now();
      if (w.alive) { w.generation = Date.now(); }
      return send(res, 200, { worker: w.worker, alive: w.alive });
    }
    if (p === '/__mock/orphan') {
      const g = S.graphs.get(body.graph ?? q.graph);
      if (!g) return err(res, 404, 'ROUTE_NOT_FOUND', { path: p });
      g.orphan = true; g.desired.state = 'stopped'; bump(g);
      return send(res, 200, { orphan: true });
    }
    if (p === '/__mock/reset') { reset(); return send(res, 200, { reset: true }); }
    return err(res, 404, 'ROUTE_NOT_FOUND', { path: p });
  }

  const isApi = p.startsWith('/api/') || p === '/healthz';
  if (isApi && S.outage) { req.socket.destroy(); return undefined; }
  if (!isApi) return serveStatic(res, p);
  if (p === '/healthz') return send(res, 200, 'ok', 'text/plain');

  const now = Date.now();
  const snap = snapshot(now);
  advanceOutputs(now, snap);

  if (m === 'GET' && p === '/api/v1/cluster') return send(res, 200, cluster(now, snap));
  if (m === 'GET' && p === '/api/v1/catalog') {
    return send(res, 200, { ops: Object.entries(OPS).sort(([a], [b]) => a.localeCompare(b)).map(([op, d]) => ({ op, kind: d.kind, input: d.input, output: d.output, description: d.description, params: paramsOf(d) })) });
  }
  if (m === 'GET' && p === '/api/v1/tenants') {
    const ts = new Map([...S.tenants.values()].map((t) => [t.tenant, { ...t, graphs: 0, actors: 0, inPerSec: 0 }]));
    for (const [key] of S.graphs) {
      const s = summary(key, snap, now);
      const t = ts.get(s.tenant) || { tenant: s.tenant, graphs: 0, actors: 0, inPerSec: 0 };
      t.graphs += 1; t.actors += s.actors; t.inPerSec = r3(t.inPerSec + s.inPerSec);
      ts.set(s.tenant, t);
    }
    return send(res, 200, { tenants: [...ts.values()].sort((a, b) => a.tenant.localeCompare(b.tenant)) });
  }
  let mm = /^\/api\/v1\/tenants\/([^/]+)$/.exec(p);
  if (mm && m === 'PUT') {
    const t = decodeURIComponent(mm[1]);
    const b = await readBody(req);
    if (!b.ok) return err(res, 400, b.code, b.context);
    const displayName = (b.value?.displayName || '').trim() || undefined;
    const existing = S.tenants.get(t);
    if (existing) {
      if (existing.displayName !== displayName) return err(res, 409, 'TENANT_DISPLAY_NAME_CONFLICT', { tenant: t });
      return send(res, 200, existing);
    }
    const record = { tenant: t, ...(displayName ? { displayName } : {}), createdMs: now };
    S.tenants.set(t, record); S.epoch += 1;
    return send(res, 201, record);
  }
  mm = /^\/api\/v1\/tenants\/([^/]+)\/graphs$/.exec(p);
  if (mm && m === 'GET') {
    const t = decodeURIComponent(mm[1]);
    const list = [...S.graphs.keys()].filter((k) => k.startsWith(`${t}/`)).sort().map((k) => summary(k, snap, now));
    return send(res, 200, { graphs: list });
  }
  mm = /^\/api\/v1\/tenants\/([^/]+)\/graphs\/([^/]+)(\/.*)?$/.exec(p);
  if (!mm) return err(res, 404, 'ROUTE_NOT_FOUND', { path: p });
  const tenant = decodeURIComponent(mm[1]); const graph = decodeURIComponent(mm[2]); const rest = mm[3] || '';
  const key = `${tenant}/${graph}`;
  const g = S.graphs.get(key);

  if (!g || g.gone) return err(res, 404, 'GRAPH_NOT_FOUND', { tenant, graph });

  if (rest === '' && m === 'GET') return send(res, 200, graphDetail(key, snap, now));
  if (rest === '' && m === 'DELETE') {
    if (!g.orphan) return err(res, 409, 'GRAPH_DECLARED_BY_CODE', { tenant, graph });
    g.desired.state = 'stopped'; g.deleting = true; bump(g);
    setTimeout(() => { S.graphs.delete(key); S.epoch += 1; place(Date.now()); }, 3000);
    return send(res, 200, { deleting: true });
  }
  if (g.orphan && m !== 'GET') return err(res, 409, 'GRAPH_NOT_IN_CODE', { tenant, graph });
  if (rest === '/dot' && m === 'GET') return send(res, 200, dot(key), 'text/vnd.graphviz');
  mm = /^\/outputs\/([^/]+)$/.exec(rest);
  if (mm && m === 'GET') {
    const node = decodeURIComponent(mm[1]);
    const n = g.spec.nodes.find((x) => x.id === node);
    if (!n) return err(res, 404, 'NODE_NOT_FOUND', { tenant, graph, node });
    if (n.op !== 'std/kv-sink@1') return send(res, 200, { rows: [] });
    const limit = Math.min(2000, Math.max(1, Number(url.searchParams.get('limit')) || 200));
    return send(res, 200, { rows: (S.outputs.get(`${key}/${node}`)?.rows || []).slice(0, limit) });
  }
  if (rest === '/state' && m === 'POST') {
    const b = await readBody(req);
    if (!b.ok) return err(res, 400, b.code, b.context);
    const st = b.value?.state;
    if (!['running', 'paused', 'draining', 'stopped'].includes(st)) return err(res, 400, 'BODY_INVALID', { detail: `unknown variant ${JSON.stringify(String(st))}, expected one of \`running\`, \`paused\`, \`draining\`, \`stopped\``, line: 1, column: 0 });
    const prev = g.desired.state;
    g.desired.state = st;
    if (st === 'draining') g.transition = { state: 'draining', at: now };
    else if (st === 'running' && (prev === 'stopped' || prev === 'draining')) g.transition = { state: 'starting', at: now };
    else g.transition = null;
    bump(g);
    return send(res, 200, { state: st });
  }
  mm = /^\/nodes\/([^/]+)\/(pause|resume|config)$/.exec(rest);
  if (mm) {
    const node = decodeURIComponent(mm[1]);
    const n = g.spec.nodes.find((x) => x.id === node);
    if (!n) return err(res, 404, 'NODE_NOT_FOUND', { tenant, graph, node });
    if (mm[2] === 'pause' && m === 'POST') {
      if (!g.desired.pausedNodes.includes(node)) g.desired.pausedNodes = [...g.desired.pausedNodes, node].sort();
      bump(g); return send(res, 200, { pausedNodes: g.desired.pausedNodes });
    }
    if (mm[2] === 'resume' && m === 'POST') {
      g.desired.pausedNodes = g.desired.pausedNodes.filter((x) => x !== node);
      bump(g); return send(res, 200, { pausedNodes: g.desired.pausedNodes });
    }
    if (mm[2] === 'config' && m === 'PATCH') {
      const b = await readBody(req);
      if (!b.ok) return err(res, 400, b.code, b.context);
      if (!b.value || typeof b.value !== 'object' || Array.isArray(b.value)) return err(res, 400, 'BODY_INVALID', { detail: 'invalid type, expected a map', line: 1, column: 0 });
      const exposed = Object.keys(OPS[n.op]?.fields || {});
      const hidden = Object.keys(b.value).find((k) => !exposed.includes(k));
      if (hidden) return err(res, 400, 'NODE_FIELD_NOT_EXPOSED', { tenant, graph, node, field: hidden });
      const code = g.code.find((x) => x.id === node)?.config || {};
      const merged = { ...(n.config || {}) };
      for (const [k, v] of Object.entries(b.value)) {
        if (v !== null) merged[k] = v;
        else if (k in code) merged[k] = code[k];
        else delete merged[k];
      }
      const e = validateConfig(n, merged);
      if (e) return err(res, 400, 'MOCK_CONFIG_INVALID', { detail: e });
      const spec = { ...g.spec, version: g.spec.version + 1, nodes: g.spec.nodes.map((x) => (x.id === node ? { ...x, config: merged } : x)) };
      addGraph(spec, now);
      S.epoch += 1; place(now);
      return send(res, 200, { version: spec.version });
    }
  }
  return err(res, 404, 'ROUTE_NOT_FOUND', { path: p });
}

reset();
http.createServer((req, res) => {
  handle(req, res).catch((e) => { console.error(e); if (!res.headersSent) err(res, 500, 'MOCK_FAILURE', { detail: String(e) }); });
}).listen(PORT, () => console.log(`mock streaming-rs API on http://127.0.0.1:${PORT}`));
