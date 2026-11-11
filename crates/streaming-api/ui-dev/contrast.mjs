#!/usr/bin/env node
// Contrast audit: every visible text node of each page against its effective background
// (first opaque ancestor background colour; translucent layers are composited).
//   node crates/streaming-api/ui-dev/contrast.mjs [baseUrl]
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const BASE = process.argv[2] || 'http://127.0.0.1:8099';
const PAGES = ['#/', '#/t/acme/g/count-per-key/n/warp10', '#/t/globex/g/fraud-scoring', '#/cluster', '#/catalog'];

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
let bad = 0;
for (const hash of PAGES) {
  await page.goto(`${BASE}/${hash}`);
  await page.waitForTimeout(2000);
  const res = await page.evaluate(() => {
    const parse = (c) => { const m = c.match(/rgba?\(([^)]+)\)/); if (!m) return null; const p = m[1].split(/[ ,/]+/).filter(Boolean).map(Number); return { r: p[0], g: p[1], b: p[2], a: p.length > 3 ? p[3] : 1 }; };
    const lum = ({ r, g, b }) => { const f = (v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; }; return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b); };
    const over = (top, under) => ({ r: top.r * top.a + under.r * (1 - top.a), g: top.g * top.a + under.g * (1 - top.a), b: top.b * top.a + under.b * (1 - top.a), a: 1 });
    // a canvas resolves color-mix()/oklab colours to rgb
    const cv = document.createElement('canvas').getContext('2d');
    const toRgb = (c) => { if (!c || c === 'transparent') return { r: 0, g: 0, b: 0, a: 0 }; const p = parse(c); if (p) return p; cv.fillStyle = '#000'; cv.fillStyle = c; cv.clearRect(0, 0, 1, 1); cv.fillRect(0, 0, 1, 1); const d = cv.getImageData(0, 0, 1, 1).data; return { r: d[0], g: d[1], b: d[2], a: d[3] / 255 }; };
    const bgOf = (el) => {
      const layers = [];
      for (let e = el; e; e = e.parentElement) {
        const cs = getComputedStyle(e);
        const c = toRgb(cs.backgroundColor);
        if (c.a > 0) { layers.push(c); if (c.a >= 1) break; }
      }
      let acc = { r: 20, g: 25, b: 37, a: 1 };
      for (let i = layers.length - 1; i >= 0; i -= 1) acc = over(layers[i], acc);
      return acc;
    };
    const out = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const seen = new Set();
    while (walker.nextNode()) {
      const t = walker.currentNode;
      const el = t.parentElement;
      if (!t.textContent.trim() || !el || seen.has(el)) continue;
      seen.add(el);
      const r = el.getBoundingClientRect();
      // disabled controls are exempt (WCAG 1.4.3)
      if (!r.width || !r.height || el.closest('[hidden]') || el.closest(':disabled')) continue;
      const cs = getComputedStyle(el);
      if (cs.visibility === 'hidden' || Number(cs.opacity) === 0) continue;
      let op = 1; for (let e = el; e; e = e.parentElement) op *= Number(getComputedStyle(e).opacity);
      const bg = bgOf(el);
      const fg = over({ ...toRgb(cs.color), a: toRgb(cs.color).a * op }, bg);
      const L1 = lum(fg); const L2 = lum(bg);
      const ratio = (Math.max(L1, L2) + 0.05) / (Math.min(L1, L2) + 0.05);
      if (ratio < 4.5) out.push(`${ratio.toFixed(2)}  "${t.textContent.trim().slice(0, 40)}"  ${el.tagName.toLowerCase()}.${el.className || ''}`);
    }
    return out;
  });
  console.log(`${hash}: ${res.length ? `${res.length} below 4.5:1` : 'ok'}`);
  for (const l of res.slice(0, 40)) console.log(`   ${l}`);
  bad += res.length;
}
await browser.close();
process.exit(bad ? 1 : 0);
