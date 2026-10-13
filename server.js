// PaperFomo data server. No dependencies, Node 18+. Run: node server.js
// Fetches real market data from DEX Screener and GeckoTerminal, caches it, and serves it to the page.
// Virtual money only: nothing here touches wallets, keys, or transactions.
const http = require('http'), fs = require('fs'), path = require('path');
const PORT = +process.env.PORT || 3000;
const REFRESH = +process.env.REFRESH_MS || 30000;           // feed refresh interval
const MAXADDR = 150;                                         // tokens tracked per chain
const NET = { solana: { gt: 'solana' }, base: { gt: 'base' }, ethereum: { gt: 'eth' } };
const CHAINS = (process.env.CHAINS || 'solana').split(',').map(s => s.trim()).filter(c => NET[c]);
const DS = 'https://api.dexscreener.com', GT = 'https://api.geckoterminal.com/api/v2';
const W = ['m5', 'h1', 'h6', 'h24'];
const ADDR = /^(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})$/;
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ---- fetch with timeout, one retry, and 429 back-off ---- */
const hold = {};
async function getJSON(url) {
  const host = new URL(url).host;
  if (hold[host] > Date.now()) throw new Error('rate limited, backing off ' + host);
  for (let n = 0; n < 2; n++) {
    try {
      const r = await fetch(url, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(8000) });
      if (r.status === 429) { hold[host] = Date.now() + 60000; throw new Error('rate limited by ' + host); }
      if (r.status >= 500 && n === 0) { await sleep(600); continue; }
      if (!r.ok) throw new Error(host + ' returned HTTP ' + r.status);
      return await r.json();
    } catch (e) {
      if (n === 1 || /rate limit/.test(e.message)) throw e;
      await sleep(600);
    }
  }
}

/* ---- cache that also de-duplicates concurrent requests ---- */
const C = new Map();
function cached(key, ttl, fn) {
  const e = C.get(key);
  if (e && Date.now() - e.t < ttl) return e.p;
  const p = fn();
  C.set(key, { t: Date.now(), p });
  p.catch(() => { if (C.get(key) && C.get(key).p === p) C.delete(key); });
  return p;
}
setInterval(() => { for (const [k, e] of C) if (Date.now() - e.t > 300000) C.delete(k); }, 60000).unref();

/* ---- normalize a DEX Screener pair into the shape the page uses ---- */
function norm(chain, p, at, src) {
  return {
    chain, id: p.baseToken.address, name: p.baseToken.name || 'Unknown', sym: p.baseToken.symbol || '?',
    dex: (p.dexId || '') + (p.labels ? ' ' + p.labels.join(' ') : ''), pair: p.pairAddress, url: p.url,
    price: p.priceUsd != null ? +p.priceUsd : null, mc: p.marketCap ?? null, fdv: p.fdv ?? null,
    liq: p.liquidity?.usd ?? null,
    v: W.map(w => p.volume?.[w] ?? null),
    tx: W.map(w => [p.txns?.[w]?.buys ?? 0, p.txns?.[w]?.sells ?? 0]),
    ch: W.map(w => p.priceChange?.[w] ?? null),
    created: p.pairCreatedAt ?? null, src, pt: at
  };
}
// Turn a list of pairs into one token per base address (the most liquid pair wins)
function toTokens(chain, pairs, wanted, at) {
  const want = new Set(wanted.map(a => a.toLowerCase())), by = new Map();
  for (const p of pairs || []) {
    const a = p.baseToken?.address;
    if (!a || !want.has(a.toLowerCase())) continue;
    if (!by.has(a)) by.set(a, []);
    by.get(a).push(p);
  }
  const out = new Map();
  for (const [a, ps] of by) {
    ps.sort((x, y) => (y.liquidity?.usd ?? -1) - (x.liquidity?.usd ?? -1));
    const t = norm(chain, ps[0], at);
    const cs = ps.map(p => p.pairCreatedAt).filter(Boolean);
    t.created = cs.length ? Math.min(...cs) : null;
    out.set(a.toLowerCase(), t);
  }
  return out;
}
// Batch lookup, 30 addresses per request (DEX Screener limit)
async function dsTokens(chain, addrs) {
  const out = new Map(), failed = new Set(), at = Date.now();
  let err = null;
  for (let i = 0; i < addrs.length; i += 30) {
    const b = addrs.slice(i, i + 30);
    try {
      const pairs = await getJSON(`${DS}/tokens/v1/${chain}/${b.join(',')}`);
      for (const [k, t] of toTokens(chain, pairs, b, at)) out.set(k, t);
    } catch (e) { err = e.message; b.forEach(a => failed.add(a.toLowerCase())); }
  }
  return { out, failed, err };
}

/* ---- per-chain store, refreshed in the background ---- */
const store = {};
CHAINS.forEach(c => store[c] = { addrs: new Map(), tokens: new Map(), updatedAt: 0, error: 'warming up', stale: true });
function track(s, addr, src, created, pinned) {
  const k = addr.toLowerCase();
  let e = s.addrs.get(k);
  if (!e) { e = { a: addr, src: new Set(), created: null, pinned: false }; s.addrs.set(k, e); }
  e.src.add(src); if (created && (!e.created || created < e.created)) e.created = created; if (pinned) e.pinned = true;
  while (s.addrs.size > MAXADDR) {
    const old = [...s.addrs.entries()].find(([, v]) => !v.pinned);
    if (!old) break;
    s.addrs.delete(old[0]); s.tokens.delete(old[0]);
  }
}
async function cycle(chain) {
  const s = store[chain], errs = [];
  const [gt, pr] = await Promise.allSettled([
    cached('gt:' + chain, REFRESH - 1000, () => getJSON(`${GT}/networks/${NET[chain].gt}/new_pools?page=1`)),
    cached('profiles', REFRESH - 1000, () => getJSON(`${DS}/token-profiles/latest/v1`))
  ]);
  if (gt.status === 'fulfilled') {
    for (const d of gt.value?.data || []) {
      const id = d.relationships?.base_token?.data?.id;
      if (!id) continue;
      track(s, id.split('_').slice(1).join('_'), 'new pool', Date.parse(d.attributes?.pool_created_at) || null);
    }
  } else errs.push('new-pool feed: ' + gt.reason.message);
  if (pr.status === 'fulfilled') {
    for (const p of Array.isArray(pr.value) ? pr.value : [])
      if (p.chainId === chain && p.tokenAddress) track(s, p.tokenAddress, 'token profile');
  } else errs.push('token profiles: ' + pr.reason.message);

  const list = [...s.addrs.values()].map(e => e.a);
  const { out, failed, err } = await dsTokens(chain, list);
  if (err) errs.push('prices: ' + err);
  for (const [k, e] of s.addrs) {
    const t = out.get(k);
    if (t) {
      t.src = [...e.src];
      if (e.created && (!t.created || e.created < t.created)) t.created = e.created;
      s.tokens.set(k, t);
    } else if (!failed.has(k)) s.tokens.delete(k);   // provider has no pair for it
  }
  if (!err) s.updatedAt = Date.now();                // only a fully successful price refresh counts
  s.stale = !!err || !s.updatedAt;
  s.error = errs.join('; ') || null;
}
let running = false;
async function loop() {
  if (running) return; running = true;
  for (const c of CHAINS) { try { await cycle(c); } catch (e) { store[c].stale = true; store[c].error = e.message; } }
  running = false;
}

/* ---- HTTP ---- */
const hits = new Map();
const limited = ip => { const m = Math.floor(Date.now() / 60000), h = hits.get(ip); if (!h || h.m !== m) { hits.set(ip, { m, n: 1 }); return false; } return ++h.n > 240; };
const send = (res, code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }); res.end(JSON.stringify(obj)); };
const bad = (res, msg, code = 400) => send(res, code, { error: msg });

http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x'), q = Object.fromEntries(u.searchParams);
  try {
    if (u.pathname === '/' || u.pathname === '/index.html') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(fs.readFileSync(path.join(__dirname, 'index.html')));
    }
    if (!u.pathname.startsWith('/api/')) return bad(res, 'not found', 404);
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (limited(ip)) return bad(res, 'too many requests', 429);

    if (u.pathname === '/api/health')
      return send(res, 200, { ok: true, chains: CHAINS, now: Date.now(), feeds: Object.fromEntries(CHAINS.map(c => [c, { tokens: store[c].tokens.size, updatedAt: store[c].updatedAt, stale: store[c].stale, error: store[c].error }])) });

    if (u.pathname === '/api/tokens') {
      const cs = q.chain && store[q.chain] ? [q.chain] : CHAINS;
      const updatedAt = Math.min(...cs.map(c => store[c].updatedAt));
      return send(res, 200, {
        chains: CHAINS, updatedAt, now: Date.now(),
        stale: cs.some(c => store[c].stale) || Date.now() - updatedAt > REFRESH * 3,
        error: cs.map(c => store[c].error).filter(Boolean).join('; ') || null,
        tokens: cs.flatMap(c => [...store[c].tokens.values()])
      });
    }

    const chain = q.chain;
    if (!CHAINS.includes(chain)) return bad(res, 'unsupported chain');

    if (u.pathname === '/api/prices') {
      const ids = [...new Set((q.ids || '').split(',').filter(Boolean))].slice(0, 30);
      if (!ids.length || !ids.every(a => ADDR.test(a))) return bad(res, 'invalid ids');
      const tokens = await cached(`p:${chain}:${ids.slice().sort().join(',')}`, 8000, async () => {
        const { out, err } = await dsTokens(chain, ids);
        if (!out.size && err) throw new Error(err);
        return [...out.values()];
      });
      return send(res, 200, { tokens, at: Date.now() });
    }

    if (u.pathname === '/api/token') {
      if (!ADDR.test(q.address || '')) return bad(res, 'invalid address');
      const token = await cached(`t:${chain}:${q.address.toLowerCase()}`, 15000, async () => {
        const pairs = await getJSON(`${DS}/token-pairs/v1/${chain}/${q.address}`);
        const t = [...toTokens(chain, pairs, [q.address], Date.now()).values()][0];
        if (!t) { const e = new Error('No trading pairs found for that address on ' + chain); e.code = 404; throw e; }
        t.src = ['address lookup'];
        track(store[chain], t.id, 'address lookup', t.created, true);
        store[chain].tokens.set(t.id.toLowerCase(), t);
        return t;
      });
      return send(res, 200, { token });
    }

    if (u.pathname === '/api/ohlcv') {
      if (!/^[A-Za-z0-9]{20,100}$/.test(q.pool || '')) return bad(res, 'invalid pool');
      const candles = await cached(`o:${chain}:${q.pool}`, 60000, async () => {
        const j = await getJSON(`${GT}/networks/${NET[chain].gt}/pools/${q.pool}/ohlcv/minute?aggregate=15&limit=96&currency=usd`);
        const l = j?.data?.attributes?.ohlcv_list;
        if (!Array.isArray(l) || l.length < 2) throw new Error('no candle data for this pool');
        return l.slice().sort((a, b) => a[0] - b[0]);       // oldest first
      });
      return send(res, 200, { candles, source: 'GeckoTerminal', interval: '15m' });
    }
    return bad(res, 'not found', 404);
  } catch (e) {
    return bad(res, e.message, e.code || 502);
  }
}).listen(PORT, () => {
  console.log(`PaperFomo running at http://localhost:${PORT}  (chains: ${CHAINS.join(', ')})`);
  loop(); setInterval(loop, REFRESH);
});
