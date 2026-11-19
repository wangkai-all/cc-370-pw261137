// Launcher: starts the three independent services (each in its own process by
// default) and, optionally, the static web server that fronts them.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { readFile, stat } from 'node:fs/promises';
import http from 'node:http';
import { config } from './config.js';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');

const SERVICES = [
  { name: 'ingest', file: 'src/services/ingest-service.js', port: config.ingestPort, env: { INGEST_PORT: String(config.ingestPort) } },
  { name: 'scheduler', file: 'src/services/scheduler-service.js', port: config.schedulerPort, env: { SCHEDULER_PORT: String(config.schedulerPort) } },
  { name: 'query', file: 'src/services/query-service.js', port: config.queryPort, env: { QUERY_PORT: String(config.queryPort) } },
];

export async function startAll({ withWeb = true } = {}) {
  const children = [];
  for (const svc of SERVICES) {
    const child = spawn(process.execPath, [resolve(root, svc.file)], {
      cwd: root,
      env: { ...process.env, ...svc.env },
      stdio: ['ignore', 'inherit', 'inherit'],
    });
    child.on('exit', (code) => {
      if (code && code !== 0) console.error(`[${svc.name}] exited with code ${code}`);
    });
    children.push({ ...svc, child });
  }

  if (withWeb) {
    const web = await startWebServer();
    children.push({ name: 'web', child: web });
  }

  console.log('\nMetrics lineage console is starting:');
  console.log(`  ingest     http://127.0.0.1:${config.ingestPort}`);
  console.log(`  scheduler  http://127.0.0.1:${config.schedulerPort}`);
  console.log(`  query      http://127.0.0.1:${config.queryPort}`);
  if (withWeb) console.log(`  web UI     http://127.0.0.1:${config.webPort}`);
  console.log('\nPress Ctrl-C to stop.\n');

  const shutdown = () => {
    for (const c of children) {
      try { c.child.kill('SIGTERM'); } catch { /* already gone */ }
    }
    setTimeout(() => process.exit(0), 200).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  return { children, shutdown };
}

/** Tiny static file server + same-origin proxy so the UI needs no CORS setup. */
async function startWebServer() {
  const proxyMap = {
    '/api/ingest': `http://127.0.0.1:${config.ingestPort}`,
    '/api/scheduler': `http://127.0.0.1:${config.schedulerPort}`,
    '/api/query': `http://127.0.0.1:${config.queryPort}`,
  };
  const server = http.createServer(async (req, res) => {
    for (const [prefix, target] of Object.entries(proxyMap)) {
      if (req.url.startsWith(prefix)) {
        const upstream = new URL(req.url.slice(prefix.length) || '/', target);
        proxy(req, res, upstream);
        return;
      }
    }
    const path = req.url === '/' ? '/index.html' : req.url.split('?')[0];
    const file = resolve(root, 'public', `.${path}`);
    if (!file.startsWith(resolve(root, 'public'))) {
      res.writeHead(403).end('forbidden');
      return;
    }
    try {
      const info = await stat(file);
      if (!info.isFile()) throw new Error('not a file');
      const body = await readFile(file);
      const type = contentType(file);
      res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache' });
      res.end(body);
    } catch {
      res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found');
    }
  });
  await new Promise((r) => server.listen(config.webPort, r));
  const child = { kill: () => server.close() };
  return child;
}

function proxy(req, res, url) {
  const proxied = http.request({
    hostname: url.hostname, port: url.port, path: `${url.pathname}${url.search}`,
    method: req.method, headers: { ...req.headers, host: url.host },
  }, (up) => {
    res.writeHead(up.statusCode, up.headers);
    up.pipe(res);
  });
  proxied.on('error', (err) => {
    res.writeHead(502, { 'Content-Type': 'application/json' })
      .end(JSON.stringify({ error: `upstream unavailable: ${err.message}` }));
  });
  req.pipe(proxied);
}

function contentType(file) {
  if (file.endsWith('.html')) return 'text/html; charset=utf-8';
  if (file.endsWith('.js')) return 'text/javascript; charset=utf-8';
  if (file.endsWith('.css')) return 'text/css; charset=utf-8';
  if (file.endsWith('.json')) return 'application/json; charset=utf-8';
  if (file.endsWith('.svg')) return 'image/svg+xml';
  return 'application/octet-stream';
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const withWeb = !process.argv.includes('--no-web');
  await startAll({ withWeb });
}
