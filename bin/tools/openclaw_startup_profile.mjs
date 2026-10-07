// Opt-in fixture preload. Report aggregate SDK reads/CPU, never paths/content.
import fs from 'node:fs';
import childProcess from 'node:child_process';
import inspector from 'node:inspector';
import { flushCompileCache, getCompileCacheDir, syncBuiltinESMExports } from 'node:module';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { threadId } from 'node:worker_threads';

const codeRoot = resolve(process.env.LIGHTHOUSE_PROFILE_CODE_ROOT);
const modulePath = process.argv[1] ? relative(dirname(codeRoot), resolve(process.argv[1])) : '..';
const role = !isAbsolute(modulePath) && !modulePath.startsWith('..') ? modulePath : 'other';
const stats = {}, seen = new Map(), fileDescriptors = new Map();
const within = (root, target) => {
  const path = relative(root, target);
  return !isAbsolute(path) && path !== '..' && !path.startsWith('..' + sep);
};
const fileScope = file => {
  if (typeof file === 'number') return fileDescriptors.get(file);
  const target = file instanceof URL && file.protocol === 'file:' ? fileURLToPath(file) : file;
  if (typeof target !== 'string' || !isAbsolute(target)) return null;
  const key = resolve(target);
  return { key, scope: within(codeRoot, key) ? 'sdk' : within(dirname(dirname(codeRoot)), key) ? 'dependency'
    : process.env.OPENCLAW_STATE_DIR && within(resolve(process.env.OPENCLAW_STATE_DIR), key) ? 'state' : 'other' };
};
if (process.env.LIGHTHOUSE_PROFILE_SPAWNS === '1') {
  const samples = new Map();
  const commands = new Set(['node', 'node.exe', 'git', 'git.exe', 'powershell', 'powershell.exe', 'pwsh', 'pwsh.exe', 'where', 'where.exe', 'cmd', 'cmd.exe']);
  for (const method of ['spawnSync', 'execFileSync', 'execSync']) {
    const run = childProcess[method];
    childProcess[method] = function (...args) {
      const executable = method === 'execSync' ? 'shell' : String(args[0]).replaceAll('\\', '/').split('/').at(-1).toLowerCase();
      const file = [...(new Error().stack || '').matchAll(/([\w.-]+\.m?js):\d+:\d+/g)]
        .map(match => match[1]).find(name => name !== 'openclaw_startup_profile.mjs') || 'native';
      const key = JSON.stringify([method, commands.has(executable) || executable === 'shell' ? executable : 'other', file]);
      const sample = samples.get(key) || { calls: 0, ms: 0, max_ms: 0 };
      samples.set(key, sample);
      const started = performance.now();
      try { return run.apply(this, args); }
      finally {
        const elapsed = performance.now() - started;
        sample.calls++; sample.ms += elapsed; sample.max_ms = Math.max(sample.max_ms, elapsed);
      }
    };
  }
  syncBuiltinESMExports();
  const report = () => process.stderr.write('[SDKSpawnProfile] ' + JSON.stringify({ role, threadId,
    top: [...samples].sort((a, b) => b[1].ms - a[1].ms).slice(0, 20).map(([key, sample]) => ({
      call: JSON.parse(key), calls: sample.calls, ms: Math.round(sample.ms), max_ms: Math.round(sample.max_ms)
    })) }) + '\n');
  const timer = setInterval(report, 30000);
  timer.unref();
  process.once('exit', report);
}
if (process.env.LIGHTHOUSE_PROFILE_COMPILE_CACHE === '1') {
  const report = () => {
    const directory = getCompileCacheDir?.();
    const count = () => directory ? fs.readdirSync(directory, { withFileTypes: true }).filter(item => item.isFile()).length : 0;
    const before = count();
    let duration = 0;
    if (process.env.LIGHTHOUSE_FLUSH_COMPILE_CACHE === '1') {
      const started = performance.now();
      flushCompileCache();
      duration = performance.now() - started;
    }
    process.stderr.write('[CompileCacheProfile] ' + JSON.stringify({ role, threadId,
      enabled: Boolean(directory), before, after: count(), flush_ms: Math.round(duration * 10) / 10 }) + '\n');
  };
  const timer = setInterval(report, 30000);
  timer.unref();
  process.once('beforeExit', report);
}
if (process.env.LIGHTHOUSE_PROFILE_READS === '1') {
  // API spans can nest; their durations must not be added into a total.
  for (const method of ['readFileSync', 'openSync', 'closeSync', 'existsSync', 'statSync', 'lstatSync', 'chmodSync', 'fchmodSync']) {
    const run = fs[method];
    fs[method] = function (file, ...args) {
      const target = fileScope(file);
      if (!target) return run.call(this, file, ...args);
      const encoding = method === 'readFileSync' ? typeof args[0] === 'string' ? args[0] : 'buffer' : '';
      const name = [method, target.scope, encoding].filter(Boolean).join(':');
      const sample = stats[name] ??= { calls: 0, duplicate_calls: 0, bytes: 0, milliseconds: 0 };
      const key = method + ':' + target.key, started = performance.now();
      sample.calls++;
      if (seen.has(key)) sample.duplicate_calls++;
      if (seen.size < 20000) seen.set(key, true);
      try {
        const result = run.call(this, file, ...args);
        if (method === 'readFileSync') sample.bytes += Buffer.byteLength(result);
        if (method === 'openSync') fileDescriptors.set(result, target);
        return result;
      } finally {
        sample.milliseconds += performance.now() - started;
        if (method === 'closeSync') fileDescriptors.delete(file);
      }
    };
  }
  syncBuiltinESMExports();
  const report = () => process.stderr.write('[SDKReadProfile] ' + JSON.stringify({ role, threadId, stats }) + '\n');
  const timer = setInterval(report, 15000);
  timer.unref();
  process.once('exit', report);
}

if (process.env.LIGHTHOUSE_PROFILE_CPU === '1') {
  const session = new inspector.Session();
  session.connect();
  session.post('Profiler.enable');
  session.post('Profiler.start');
  let stopped = false;
  const stop = () => {
    if (stopped) return;
    stopped = true;
    session.post('Profiler.stop', (error, result) => {
      session.disconnect();
      if (error) return process.stderr.write('[SDKCPUProfile] unavailable\n');
      const nodes = new Map(result.profile.nodes.map(node => [node.id, node.callFrame]));
      const parents = new Map(result.profile.nodes.flatMap(node => (node.children || []).map(id => [id, node.id])));
      const times = new Map();
      result.profile.samples?.forEach((id, index) => {
        const frame = nodes.get(id), url = frame.url.replaceAll('\\', '/');
        const suffix = url.split('/node_modules/').at(-1);
        const packageName = url.includes('/node_modules/') ? suffix.split('/').slice(0, suffix.startsWith('@') ? 2 : 1).join('/') : url.startsWith('node:') ? 'node' : 'vm';
        const file = packageName !== 'vm' ? url.split('/').at(-1) : '';
        let caller = id, owner = '';
        while (parents.has(caller)) {
          caller = parents.get(caller);
          const ancestor = nodes.get(caller).url.replaceAll('\\', '/');
          if (ancestor.includes('/node_modules/')) { owner = ancestor.split('/').at(-1); break; }
        }
        const key = JSON.stringify([frame.functionName || '<anonymous>', packageName, file, owner]);
        times.set(key, (times.get(key) || 0) + result.profile.timeDeltas[index]);
      });
      const top = [...times].sort((a, b) => b[1] - a[1]).slice(0, 15)
        .map(([key, time]) => ({ name: JSON.parse(key), sample_ms: Math.round(time / 100) / 10 }));
      process.stderr.write('[SDKCPUProfile] ' + JSON.stringify({ role, threadId, top }) + '\n');
    });
  };
  const timer = setTimeout(stop, 55000);
  timer.unref();
  process.once('beforeExit', stop);
}
