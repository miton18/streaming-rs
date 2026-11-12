// HTTP client for /api/v1 (same origin) and connection status tracking.

import { messageOf } from './errors-fr.js';

const BASE = '/api/v1';
const TIMEOUT_MS = 4000;

export class ApiError extends Error {
  /** `code` and `context` are those of the answer, when it has them. */
  constructor(message, status, network = false, code = null, context = {}) {
    super(message);
    this.status = status;
    this.network = network;
    this.code = code;
    this.context = context;
  }
}

/** Connection status, shared with the shell. */
export const conn = {
  ok: null,            // null until the first answer
  lastOk: 0,           // Date.now() of the last successful read
  downSince: 0,
  lastError: '',
  clockOffset: 0,      // server nowMs - local Date.now()
  listeners: new Set(),
  set(ok, err) {
    const was = this.ok;
    if (ok) { this.ok = true; this.lastOk = Date.now(); this.downSince = 0; this.lastError = ''; }
    else { if (this.ok !== false) this.downSince = Date.now(); this.ok = false; this.lastError = err || ''; }
    if (was !== this.ok) for (const l of this.listeners) l(this);
  },
  now() { return Date.now() + this.clockOffset; },
};

const enc = encodeURIComponent;
export const paths = {
  cluster: () => `${BASE}/cluster`,
  catalog: () => `${BASE}/catalog`,
  tenants: () => `${BASE}/tenants`,
  graphs: (t) => `${BASE}/tenants/${enc(t)}/graphs`,
  graph: (t, g) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}`,
  outputs: (t, g, n, limit = 100) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/outputs/${enc(n)}?limit=${limit}`,
  dot: (t, g) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/dot`,
  state: (t, g) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/state`,
  pause: (t, g, n) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/nodes/${enc(n)}/pause`,
  resume: (t, g, n) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/nodes/${enc(n)}/resume`,
  config: (t, g, n) => `${BASE}/tenants/${enc(t)}/graphs/${enc(g)}/nodes/${enc(n)}/config`,
};

async function request(method, url, body) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  let res;
  try {
    res = await fetch(url, {
      method,
      headers: body === undefined ? { Accept: 'application/json' } : { Accept: 'application/json', 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: ctl.signal,
      cache: 'no-store',
    });
  } catch (e) {
    clearTimeout(timer);
    throw new ApiError(e.name === 'AbortError' ? "délai dépassé (pas de réponse de l'API)" : 'API injoignable', 0, true);
  }
  clearTimeout(timer);
  const ct = res.headers.get('content-type') || '';
  let data = null;
  if (ct.includes('json')) { try { data = await res.json(); } catch { data = null; } } else { data = await res.text(); }
  if (!res.ok) {
    const body = data && typeof data === 'object' ? data : null;
    // the message is written in French from the code and context; the
    // server's English one is the fallback for a code this UI does not know
    const msg = messageOf(body) ?? (body?.message || (typeof data === 'string' && data ? data : `HTTP ${res.status}`));
    // a 502 or 504 comes from a proxy in front of the server: the API is down;
    // a 503 with a code is the API saying its store does not answer
    const down = res.status === 502 || res.status === 504 || (res.status === 503 && !body?.code);
    throw new ApiError(msg, res.status, down, body?.code ?? null, body?.context ?? {});
  }
  return data;
}

/** Read: updates the connection status. */
export async function get(url) {
  try {
    const d = await request('GET', url);
    conn.set(true);
    return d;
  } catch (e) {
    if (e.network) conn.set(false, e.message);
    else conn.set(true);
    throw e;
  }
}

export const post = (url, body = {}) => request('POST', url, body);
export const patch = (url, body) => request('PATCH', url, body);
export const put = (url, body) => request('PUT', url, body);
export const del = (url) => request('DELETE', url);
