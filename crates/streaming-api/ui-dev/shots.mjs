#!/usr/bin/env node
// Screenshots of every page at 1440x900 and 1100x800 against a running mock server.
//   node crates/streaming-api/ui-dev/shots.mjs [baseUrl] [outDir]
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');

const BASE = process.argv[2] || 'http://127.0.0.1:8099';
const OUT = path.resolve(process.argv[3] || 'shots');
fs.mkdirSync(OUT, { recursive: true });

const PAGES = [
  ['overview', '#/'],
  ['graph', '#/t/acme/g/count-per-key'],
  ['graph-node', '#/t/acme/g/count-per-key/n/warp10'],
  ['graph-wide', '#/t/globex/g/fraud-scoring'],
  ['graph-plan-error', '#/t/globex/g/legacy-export'],
  ['cluster', '#/cluster'],
  ['catalog', '#/catalog'],
];
const SIZES = [[1440, 900], [1100, 800]];
const only = process.env.ONLY ? process.env.ONLY.split(',') : null;
const full = !!process.env.FULL;

const browser = await chromium.launch();
const errors = [];
for (const [w, hgt] of SIZES) {
  const page = await browser.newPage({ viewport: { width: w, height: hgt }, deviceScaleFactor: 1 });
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') errors.push(`[${m.type()}] ${m.text()}`); });
  page.on('pageerror', (e) => errors.push(`[pageerror] ${e.message}`));
  for (const [name, hash] of PAGES) {
    if (only && !only.includes(name)) continue;
    await page.goto(`${BASE}/${hash}`);
    await page.waitForTimeout(2300);
    const file = path.join(OUT, `${name}-${w}.png`);
    if (full) {
      // the app scrolls inside <main>: screenshot its whole content
      const height = await page.evaluate(() => document.querySelector('.main').scrollHeight);
      await page.setViewportSize({ width: w, height: Math.max(hgt, Math.min(height, 4000)) });
      await page.waitForTimeout(300);
    }
    await page.screenshot({ path: file });
    if (full) await page.setViewportSize({ width: w, height: hgt });
    console.log(file);
  }
  await page.close();
}
await browser.close();
if (errors.length) { console.log('CONSOLE:'); for (const e of errors) console.log(e); }
