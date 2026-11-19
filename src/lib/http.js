// Minimal HTTP plumbing shared by the three services. No framework: each
// service owns its routes and process, which is what "independently runnable"
// requires.
import http from 'node:http';
import { AppError } from './errors.js';

export function createService({ name, port, routes, log = true }) {
  const server = http.createServer(async (req, res) => {
    const started = Date.now();
    const url = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`);
    cors(res);
    if (req.method === 'OPTIONS') {
      res.writeHead(204).end();
      return;
    }
    if (url.pathname === '/health') {
      return send(res, 200, { service: name, ok: true, at: new Date().toISOString() });
    }
    try {
      const match = findRoute(routes, req.method, url.pathname);
      if (!match) return send(res, 404, { error: 'not found', service: name, path: url.pathname });
      const body = await readBody(req);
      const query = Object.fromEntries(url.searchParams);
      const result = await match.handler({ req, res, params: match.params, query, body });
      if (res.writableEnded) return;
      if (result === undefined) return send(res, 204, {});
      // Handlers may opt into an explicit status with { __status, __body };
      // anything else is a normal 200 body (which may itself contain a
      // `status` business field, e.g. an ingest result).
      if (result && typeof result === 'object' && '__body' in result) {
        return send(res, result.__status ?? 200, result.__body);
      }
      send(res, 200, result);
    } catch (err) {
      const status = err instanceof AppError ? err.status : 500;
      if (status >= 500) console.error(`[${name}] ${req.method} ${url.pathname}`, err);
      send(res, status, { error: err.message, name: err.name, details: err.details ?? {} });
    } finally {
      if (log) console.log(`[${name}] ${req.method} ${url.pathname} ${res.statusCode} ${Date.now() - started}ms`);
    }
  });
  return {
    server,
    listen: () => new Promise((resolve) => server.listen(port, () => {
      resolve(server.address().port);
    })),
    close: () => new Promise((resolve) => server.close(resolve)),
    name,
  };
}

function findRoute(routes, method, pathname) {
  for (const route of routes) {
    if (route.method !== method) continue;
    const m = matchPath(route.path, pathname);
    if (m) return { handler: route.handler, params: m };
  }
  return null;
}

function matchPath(pattern, pathname) {
  const p = pattern.split('/');
  const a = pathname.split('/');
  if (p.length !== a.length) return null;
  const params = {};
  for (let i = 0; i < p.length; i++) {
    if (p[i].startsWith(':')) params[p[i].slice(1)] = decodeURIComponent(a[i]);
    else if (p[i] !== a[i]) return null;
  }
  return params;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    if (req.method === 'GET' || req.method === 'DELETE') return resolve({});
    let data = '';
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 8 * 1024 * 1024) {
        reject(new AppError('request body too large', 413));
        req.destroy();
        return;
      }
      data += chunk;
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch {
        reject(new AppError('body must be valid JSON', 400));
      }
    });
    req.on('error', reject);
  });
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

export function send(res, status, body) {
  if (status === 204) { res.writeHead(204).end(); return; }
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(payload) });
  res.end(payload);
}
