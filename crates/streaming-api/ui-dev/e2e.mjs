#!/usr/bin/env node
// End-to-end check of the UI against the mock server (start it first, or pass --spawn).
//   node crates/streaming-api/ui-dev/e2e.mjs [--spawn] [--base http://127.0.0.1:8099] [--out dir]
// Exercises navigation, in-place polling (no node re-creation, hover and typed input survive),
// node selection, config edit + apply, API validation errors, pause/resume, run-state change with
// confirmation, results and description tabs, outage banner, commit bottleneck, dead worker,
// no graph edition (graphs come from the code).
import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const HERE = path.dirname(fileURLToPath(import.meta.url));
const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const BASE = arg('--base', 'http://127.0.0.1:8099');
const OUT = path.resolve(arg('--out', path.join(HERE, 'shots')));
fs.mkdirSync(OUT, { recursive: true });

let server = null;
if (process.argv.includes('--spawn')) {
  const port = new URL(BASE).port || '8099';
  server = spawn(process.execPath, [path.join(HERE, 'mock-server.mjs'), '--port', port], { stdio: 'ignore' });
  await new Promise((r) => setTimeout(r, 700));
}
const mock = async (knob, body = {}) => fetch(`${BASE}/__mock/${knob}`, { method: 'POST', body: JSON.stringify(body) }).then((r) => r.json());
await mock('reset');

const results = [];
let failed = 0;
async function step(name, fn) {
  try { await fn(); results.push(`ok   ${name}`); } catch (e) { failed += 1; results.push(`FAIL ${name}: ${e.message.split('\n')[0]}`); }
}
function assert(c, msg) { if (!c) throw new Error(msg); }

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
const consoleErrors = [];
let outage = false;
let expect400 = false;
page.on('console', (m) => {
  if (m.type() !== 'error' && m.type() !== 'warning') return;
  // the browser itself logs failed requests while the API is down on purpose
  if (outage && /Failed to load resource|ERR_EMPTY_RESPONSE|ERR_CONNECTION/.test(m.text())) return;
  // and the 400 of the deliberately invalid configuration
  if (expect400 && /status of 400/.test(m.text())) return;
  consoleErrors.push(`[${m.type()}] ${m.text()}`);
});
page.on('pageerror', (e) => consoleErrors.push(`[pageerror] ${e.message}`));
const shot = (name) => page.screenshot({ path: path.join(OUT, `${name}.png`) });
const wait = (ms) => page.waitForTimeout(ms);

await page.goto(`${BASE}/#/`);
await wait(1800);

await step('overview: one card per graph, grouped by tenant', async () => {
  const n = await page.locator('.gcard').count();
  assert(n === 4, `expected 4 graph cards, got ${n}`);
  const tenants = await page.locator('.page section h2').allTextContents();
  assert(tenants.includes('acme') && tenants.includes('globex'), `tenants: ${tenants}`);
  assert(await page.locator('.gcard', { hasText: 'legacy-export' }).locator('.err').isVisible(), 'plan error not shown');
  await shot('e2e-overview');
});

await step('navigation: cluster, catalogue with filter that survives polling', async () => {
  await page.click('.nav-item[href="#/cluster"]');
  await page.waitForSelector('.wcard');
  assert((await page.locator('.wcard').count()) === 3, 'worker cards');
  await page.click('.nav-item[href="#/catalog"]');
  await page.waitForSelector('.page tbody tr');
  await page.fill('input[type=search]', 'kv-sink');
  await wait(2200);
  assert((await page.inputValue('input[type=search]')) === 'kv-sink', 'typed filter lost');
  const rows = await page.locator('.page tbody tr').count();
  assert(rows === 1, `filter: expected 1 row, got ${rows}`);
  const t = await page.locator('.page tbody tr td:nth-child(3) code').getAttribute('title');
  assert(t === 'serde_json::value::Value', `full type in title: ${t}`);
});

await step('graph page: DAG with 9 nodes and 8 edges', async () => {
  await page.click('.nav-graph[href="#/t/acme/g/count-per-key"]');
  await page.waitForSelector('.node');
  assert((await page.locator('.node').count()) === 9, 'node count');
  assert((await page.locator('.dag-edges path.e').count()) === 8, 'edge count');
});

await step('polling updates in place: no node re-created, hover kept, values change', async () => {
  await page.evaluate(() => {
    window.__mut = 0;
    document.querySelectorAll('.node').forEach((n) => { n.dataset.mark = '1'; });
    new MutationObserver((ms) => { for (const m of ms) for (const r of m.removedNodes) if (r.classList?.contains('node')) window.__mut += 1; })
      .observe(document.querySelector('.dag-inner'), { childList: true, subtree: true });
  });
  const before = await page.locator('.node[data-node="decode"] .rates').textContent();
  await page.hover('.node[data-node="by-key"]');
  await wait(3200);
  const marks = await page.locator('.node[data-mark="1"]').count();
  const removed = await page.evaluate(() => window.__mut);
  const hovered = await page.evaluate(() => document.querySelector('.node[data-node="by-key"]').matches(':hover'));
  const after = await page.locator('.node[data-node="decode"] .rates').textContent();
  assert(marks === 9 && removed === 0, `nodes re-created (marks ${marks}, removed ${removed})`);
  assert(hovered, 'hover lost');
  assert(before !== after, `rates did not change (${before})`);
});

await step('node selection opens the side panel (and survives polling)', async () => {
  await page.click('.node[data-node="warp10"]');
  await page.waitForSelector('.panel');
  assert((await page.locator('.panel .name').textContent()) === 'warp10', 'panel title');
  assert(page.url().endsWith('/n/warp10'), `url ${page.url()}`);
  await wait(1500);
  assert(await page.locator('.panel').isVisible(), 'panel closed by polling');
  assert((await page.locator('.panel .panel-body tbody tr').count()) >= 1, 'instances table empty');
  assert(await page.locator('.node[data-node="warp10"].selected').count() === 1, 'node not marked selected');
});

await step('config edit + apply makes the bottleneck appear', async () => {
  const input = page.locator('.panel input#cfg-warp10-latencyMs');
  await input.fill('80');
  assert(await page.locator('.panel button[type=submit]').isEnabled(), 'apply disabled');
  await page.click('.panel button[type=submit]');
  await page.waitForSelector('.toast', { timeout: 3000 });
  await page.waitForSelector('.bneck:not([hidden])', { timeout: 6000 });
  const bn = await page.locator('.bneck').textContent();
  assert(bn.includes('warp10'), `banner: ${bn}`);
  await page.waitForSelector('.node[data-node="warp10"].bottleneck', { timeout: 3000 });
  assert((await input.inputValue()) === '80', 'value not kept after apply');
  await wait(1200);
  await shot('e2e-bottleneck-panel');
});

await step('API validation error is shown inline and typed input survives polling', async () => {
  const input = page.locator('.panel input#cfg-warp10-latencyMs');
  expect400 = true;
  await input.fill('99999999');
  await page.click('.panel button[type=submit]');
  await page.waitForSelector('.panel .form-error:not([hidden])', { timeout: 3000 });
  const msg = await page.locator('.panel .form-error').textContent();
  assert(/latencyMs/.test(msg), `error: ${msg}`);
  await wait(2200);
  assert((await input.inputValue()) === '99999999', 'typed value lost on poll');
  assert(await page.locator('.panel .form-error').isVisible(), 'error vanished on poll');
  await page.locator('.panel .form-error').scrollIntoViewIfNeeded();
  await shot('e2e-validation-error');
  await page.click('.panel button:has-text("Annuler les modifications")');
  expect400 = false;
  assert((await input.inputValue()) === '80', 'reset did not restore');
});

await step('pause / resume a node', async () => {
  await page.click('.panel button:has-text("Mettre le nœud en pause")');
  await page.waitForSelector('.node[data-node="warp10"].paused', { timeout: 4000 });
  assert(await page.locator('.panel .tag:has-text("nœud en pause")').isVisible(), 'panel pause tag');
  await wait(1200);
  await shot('e2e-node-paused');
  await page.click('.panel button:has-text("Reprendre le nœud")');
  await page.waitForSelector('.node[data-node="warp10"]:not(.paused)', { timeout: 4000 });
});

await step('polling pauses while the tab is hidden and resumes when visible', async () => {
  let n = 0;
  const onReq = (r) => { if (r.url().includes('/api/v1/')) n += 1; };
  page.on('request', onReq);
  await page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange')); });
  await wait(400); n = 0;
  await wait(2500);
  const hiddenCount = n;
  await page.evaluate(() => { delete document.hidden; document.dispatchEvent(new Event('visibilitychange')); });
  await wait(2200);
  page.off('request', onReq);
  assert(hiddenCount === 0, `${hiddenCount} API requests while hidden`);
  assert(n > 3, `polling did not resume (${n} requests)`);
});

await step('Escape closes the panel', async () => {
  await page.keyboard.press('Escape');
  await page.waitForSelector('.panel', { state: 'detached', timeout: 2000 });
});

await step('run state: Stopped asks for confirmation (cancel, then confirm), back to Running', async () => {
  await page.click('.seg-opt[data-v="stopped"]');
  await page.waitForSelector('dialog[open]');
  await wait(300);
  await shot('e2e-confirm-stop');
  await page.click('dialog[open] button:has-text("Annuler")');
  await wait(1200);
  assert((await page.getAttribute('.seg-opt[data-v="running"]', 'aria-checked')) === 'true', 'state changed after cancel');
  await page.click('.seg-opt[data-v="stopped"]');
  await page.click('dialog[open] button:has-text("Arrêter le graphe")');
  await page.waitForFunction(() => document.querySelector('.seg-opt[data-v="stopped"]').getAttribute('aria-checked') === 'true' && !document.querySelector('.seg-opt.pending'), null, { timeout: 5000 });
  await wait(1300);
  const st = await page.locator('.title-row .tag').first().textContent();
  assert(st === 'stopped', `state tag ${st}`);
  await page.click('.seg-opt[data-v="paused"]');
  assert(!(await page.locator('dialog[open]').count()), 'paused should not ask');
  await page.waitForFunction(() => document.querySelector('.seg-opt[data-v="paused"]').getAttribute('aria-checked') === 'true', null, { timeout: 5000 });
  await page.click('.seg-opt[data-v="running"]');
  await page.waitForFunction(() => document.querySelector('.seg-opt[data-v="running"]').getAttribute('aria-checked') === 'true' && !document.querySelector('.seg-opt.pending'), null, { timeout: 5000 });
});

await step('Résultats tab: live kv-sink table, newest first', async () => {
  await page.click('#tab-results');
  await page.waitForSelector('[aria-labelledby="tab-results"] tbody tr', { timeout: 4000 });
  const n = await page.locator('[aria-labelledby="tab-results"] tbody tr').count();
  assert(n > 10, `rows ${n}`);
  const times = await page.locator('[aria-labelledby="tab-results"] tbody tr td:first-child').allTextContents();
  const sorted = [...times].sort().reverse();
  assert(JSON.stringify(times) === JSON.stringify(sorted), 'not newest first');
  await page.locator('[aria-labelledby="tab-results"]').scrollIntoViewIfNeeded();
  await shot('e2e-results');
});

await step('Description tab: GraphSpec JSON, copy button, DOT link', async () => {
  await page.click('#tab-spec');
  const txt = await page.locator('pre.code').textContent();
  assert(txt.includes('"tenant": "acme"') && txt.includes('"edges"'), 'spec JSON');
  const href = await page.getAttribute('a:has-text("Export DOT")', 'href');
  assert(href === '/api/v1/tenants/acme/graphs/count-per-key/dot', `dot href ${href}`);
  const dot = await (await fetch(BASE + href)).text();
  assert(dot.startsWith('digraph'), 'dot body');
  await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.click('button:has-text("Copier le JSON")');
  await page.waitForSelector('.toast:has-text("copiée")', { timeout: 2000 });
});

await step('outage: banner shown, last data kept, recovery', async () => {
  await page.click('#tab-slices');
  outage = true;
  await mock('outage', { on: true });
  await page.waitForSelector('.offline:not([hidden])', { timeout: 7000 });
  assert((await page.locator('.node').count()) === 9, 'DAG lost during outage');
  assert((await page.locator('[aria-labelledby="tab-slices"] tbody tr').count()) === 5, 'slices lost during outage');
  await wait(1500);
  await shot('e2e-outage');
  await mock('outage', { on: false });
  await page.waitForSelector('.offline', { state: 'hidden', timeout: 7000 });
  outage = false;
});

await step('commit bottleneck explained', async () => {
  await page.locator('.node[data-node="warp10"]').click();
  await page.locator('.panel input#cfg-warp10-latencyMs').fill('0');
  await page.click('.panel button[type=submit]');
  await page.waitForSelector('.toast');
  await page.keyboard.press('Escape');
  await mock('commit', { on: true });
  await page.waitForFunction(() => (document.querySelector('.bneck:not([hidden])')?.textContent || '').includes('commits KV'), null, { timeout: 8000 });
  const t = await page.locator('.bneck').textContent();
  assert(t.includes('events') && /commits KV de la slice/.test(t), `banner ${t}`);
  await wait(1000);
  await shot('e2e-commit-bottleneck');
  await mock('commit', { on: false });
});

await step('wide 20-node graph renders without overlap', async () => {
  await page.click('.nav-graph[href="#/t/globex/g/fraud-scoring"]');
  await page.waitForFunction(() => document.querySelectorAll('.node').length === 20);
  const overlap = await page.evaluate(() => {
    const rs = [...document.querySelectorAll('.node')].map((n) => n.getBoundingClientRect());
    for (let i = 0; i < rs.length; i += 1) for (let j = i + 1; j < rs.length; j += 1) {
      const a = rs[i]; const b = rs[j];
      if (a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom) return true;
    }
    return false;
  });
  assert(!overlap, 'nodes overlap');
  await page.click('.seg-opt[data-v="100"]');
  await wait(400);
  await page.click('.seg-opt[data-v="fit"]');
});

await step('dead worker shown on cluster page', async () => {
  await mock('worker', { worker: 'worker-3', alive: false });
  await page.click('.nav-item[href="#/cluster"]');
  await page.waitForSelector('.wcard.dead', { timeout: 4000 });
  await wait(1200);
  await shot('e2e-cluster-dead-worker');
  await mock('worker', { worker: 'worker-3', alive: true });
});

await step('a worker of an older binary says which graphs it steps aside from', async () => {
  await mock('aside', { worker: 'worker-2', graphs: ['acme/count-per-key', 'globex/clicks'] });
  await page.waitForFunction(() => [...document.querySelectorAll('.wcard .facts')].some((el) => !el.hidden && el.textContent.includes('En retrait de 2 graphes : acme/count-per-key, globex/clicks')), null, { timeout: 4000 });
  await shot('e2e-cluster-worker-aside');
  await mock('aside', { worker: 'worker-2', graphs: [] });
  await page.waitForFunction(() => ![...document.querySelectorAll('.wcard .facts')].some((el) => !el.hidden && el.textContent.includes('En retrait')), null, { timeout: 4000 });
});

await step('graphs come from the code: no deletion, only exposed fields', async () => {
  await page.click('.nav-graph[href="#/t/globex/g/legacy-export"]');
  await page.waitForSelector('.node');
  assert(!(await page.locator('button:has-text("Supprimer")').isVisible()), 'a delete button is shown');
  assert(await page.locator('.nav-item[href="#/design"]').count() === 0, 'the designer is still listed');
  await page.goto(`${BASE}/#/t/acme/g/count-per-key/n/decode`);
  await page.waitForSelector('.panel');
  await page.waitForFunction(() => { const el = document.querySelector('.panel .form .muted'); return el && !el.hidden; }, null, { timeout: 4000 });
  assert(await page.locator('.panel .field').count() === 0, 'decode exposes no field');
  await page.keyboard.press('Escape');
});

await step('a graph that left the code is stopped and can be deleted, with confirmation', async () => {
  await mock('orphan', { graph: 'globex/legacy-export' });
  await page.click('.nav-graph[href="#/t/globex/g/legacy-export"]');
  await page.waitForSelector('.banner:has-text("plus déclaré par le code")', { timeout: 4000 });
  assert(!(await page.locator('.page-head .actions .seg').isVisible()), 'run state still offered');
  await wait(600);
  await shot('e2e-orphan-graph');
  await page.click('button:has-text("Supprimer le graphe et son état")');
  await page.waitForSelector('dialog[open]');
  await page.click('dialog[open] button:has-text("Supprimer définitivement")');
  await page.waitForFunction(() => location.hash === '#/', null, { timeout: 3000 });
  await page.waitForSelector('.nav-graph[href="#/t/globex/g/legacy-export"]', { state: 'detached', timeout: 7000 });
});

await step('1100 px: graph page and panel', async () => {
  await page.setViewportSize({ width: 1100, height: 800 });
  await page.goto(`${BASE}/#/t/acme/g/count-per-key/n/count`);
  await page.waitForSelector('.panel');
  await wait(1500);
  const hscroll = await page.evaluate(() => document.querySelector('.main').scrollWidth > document.querySelector('.main').clientWidth + 1);
  assert(!hscroll, 'main area overflows horizontally');
  await shot('e2e-1100-panel');
});

await step('no console errors', async () => {
  assert(consoleErrors.length === 0, consoleErrors.join(' | '));
});

await browser.close();
await mock('reset');
if (server) server.kill();
console.log(results.join('\n'));
console.log(failed ? `\n${failed} step(s) failed` : '\nall steps passed');
console.log(`screenshots in ${OUT}`);
process.exit(failed ? 1 : 0);
