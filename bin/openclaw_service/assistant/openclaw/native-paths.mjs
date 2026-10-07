// The pinned Windows plugin loader passes file URLs to CJS require. Native
// paths avoid its Jiti fallback and duplicate evaluation of the SDK graph.
import Module from 'node:module';
import { syncBuiltinESMExports } from 'node:module';
import childProcess from 'node:child_process';
import fs, { realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

if (process.platform === 'win32' && (process.env.LIGHTHOUSE_SDK_ROOT || process.argv[1])) {
  const roots = [process.env.LIGHTHOUSE_SDK_ROOT || dirname(resolve(process.argv[1])), process.env.LIGHTHOUSE_PLUGIN_DIR]
    .filter(Boolean).map(root => realpathSync.native(root));
  // Cache only immutable installed SDK code, never config, keys, sessions,
  // workspaces or plugins. Actual module imports retain Node's file checks.
  const codeRoot = resolve(roots[0], 'dist');
  // Mirror the pinned SDK fast path, disabled there whenever --import is used.
  const distUrl = pathToFileURL(codeRoot + sep).href;
  Module.registerHooks?.({ resolve(specifier, context, nextResolve) {
    if ((specifier.startsWith('./') || specifier.startsWith('../')) && specifier.endsWith('.js') &&
        context.parentURL?.startsWith(distUrl) && !context.conditions?.includes('require')) {
      const url = new URL(specifier, context.parentURL).href;
      if (url.startsWith(distUrl)) return { url, format: 'module', shortCircuit: true };
    }
    return nextResolve(specifier, context);
  } });
  // ponytail: exact commands from the pinned SDK only; a changed SDK keeps its
  // original probes. Never cache PID identities or current listening ports.
  const python = process.env.LIGHTHOUSE_PYTHON;
  const pidModule = resolve(codeRoot, 'pid-alive-BcyyC-CC.js');
  const portModule = resolve(codeRoot, 'windows-port-pids-CgzBZ5yT.js');
  if (python && isAbsolute(python) && fs.existsSync(python) &&
      fs.existsSync(pidModule) && fs.existsSync(portModule)) {
    const { r: listeners } = await import(pathToFileURL(resolve(codeRoot, 'ports-netstat-DPzRKOWx.js')).href);
    const { a: systemExe } = await import(pathToFileURL(resolve(codeRoot, 'windows-install-roots-BdGcwph2.js')).href);
    const spawn = childProcess.spawnSync, exec = childProcess.execFileSync;
    const pidUrl = pathToFileURL(pidModule).href + ':', portUrl = pathToFileURL(portModule).href + ':';
    const creationTime = `import ctypes, sys
from ctypes import wintypes as w
from datetime import datetime, timedelta, timezone
k = ctypes.WinDLL('kernel32', use_last_error=True)
k.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
k.OpenProcess.restype = w.HANDLE
k.GetExitCodeProcess.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
k.GetProcessTimes.argtypes = [w.HANDLE] + [ctypes.POINTER(w.FILETIME)] * 4
k.CloseHandle.argtypes = [w.HANDLE]
h = k.OpenProcess(0x1000, False, int(sys.argv[1]))
if not h: raise ctypes.WinError(ctypes.get_last_error())
try:
    code = w.DWORD()
    if not k.GetExitCodeProcess(h, ctypes.byref(code)) or code.value != 259: raise OSError('process exited')
    times = [w.FILETIME() for _ in range(4)]
    if not k.GetProcessTimes(h, *[ctypes.byref(value) for value in times]): raise ctypes.WinError(ctypes.get_last_error())
    ticks = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
    created = datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ticks // 10)
    print(created.isoformat(timespec='milliseconds'))
finally:
    k.CloseHandle(h)
`;
    const probe = (file, args, options) => {
      if (typeof file !== 'string' || basename(file).toLowerCase() !== 'powershell.exe' ||
          !Array.isArray(args) || options?.encoding !== 'utf8' || options.shell) return null;
      const stack = new Error().stack || '', flags = JSON.stringify(args.slice(0, -1));
      const command = args.at(-1);
      let match;
      if (flags === '["-NoProfile","-NonInteractive","-Command"]') {
        if (stack.includes(pidUrl)) match = /^\(Get-Process -Id ([1-9]\d*)\)\.StartTime\.ToString\('o'\)$/.exec(command);
        if (!match && stack.includes(portUrl)) match = /^\$process = Get-CimInstance Win32_Process -Filter "ProcessId = ([1-9]\d*)" -ErrorAction Stop; \[Console\]::Out\.Write\(\$process\.CreationDate\.ToUniversalTime\(\)\.ToString\("o"\)\)$/.exec(command);
        if (match && Number(match[1]) <= 0xffffffff) return { pid: match[1] };
      }
      if (stack.includes(portUrl) && flags === '["-NoProfile","-Command"]') {
        match = /^\(Get-NetTCPConnection -LocalPort ([1-9]\d*) -State Listen -ErrorAction SilentlyContinue \| Select-Object -ExpandProperty OwningProcess\)$/.exec(command);
        if (match && Number(match[1]) <= 65535) return { port: Number(match[1]) };
      }
      return null;
    };
    const read = (query, options) => {
      const result = query.pid
        ? spawn(python, ['-I', '-S', '-B', '-c', creationTime, query.pid], { ...options, timeout: Math.min(options.timeout || 1000, 1000), shell: false })
        : spawn(systemExe('netstat.exe'), ['-ano'], options);
      if (result.error || result.status !== 0) return null;
      if (query.pid && (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}\+00:00\s*$/.test(result.stdout) ||
          !Number.isFinite(Date.parse(result.stdout.trim())))) return null;
      if (query.port) {
        result.stdout = [...new Set(listeners(result.stdout, query.port).map(row => row.pid))].join('\n');
        if (result.output) result.output[1] = result.stdout;
      }
      return result;
    };
    const remaining = (options, started) => options.timeout > 0
      ? { ...options, timeout: Math.max(1, options.timeout - Math.ceil(performance.now() - started)) } : options;
    childProcess.spawnSync = function(file, args, options) {
      const query = probe(file, args, options);
      if (!query) return spawn.call(this, file, args, options);
      const started = performance.now();
      let result;
      try { result = read(query, options); } catch { /* Use the original bounded probe. */ }
      return result || spawn.call(this, file, args, remaining(options, started));
    };
    childProcess.execFileSync = function(file, args, options) {
      const query = probe(file, args, options);
      if (!query) return exec.call(this, file, args, options);
      const started = performance.now();
      let result;
      try { result = read(query, options); } catch { /* Use the original bounded probe. */ }
      return result ? result.stdout : exec.call(this, file, args, remaining(options, started));
    };
    // Refresh child_process named exports before installing the existing fs hooks.
    syncBuiltinESMExports();
  }
  const reads = new Map(), existence = new Map();
  let bytes = 0;
  const codePath = value => {
    if (typeof value !== 'string' || !isAbsolute(value)) return null;
    const key = resolve(value), path = relative(codeRoot, key);
    return !isAbsolute(path) && path !== '..' && !path.startsWith('..' + sep) ? key : null;
  };
  const read = fs.readFileSync, exists = fs.existsSync;
  fs.existsSync = function(file) {
    const key = codePath(file);
    if (key && existence.has(key)) return existence.get(key);
    const result = exists.call(this, file);
    if (key && existence.size < 20000) existence.set(key, result);
    return result;
  };
  fs.readFileSync = function(file, options) {
    const key = codePath(file);
    if (!key || options !== 'utf-8') return read.call(this, file, options);
    if (reads.has(key)) return reads.get(key);
    const result = read.call(this, file, options), size = Buffer.byteLength(result);
    if (bytes + size <= 32 * 1024 * 1024) { reads.set(key, result); bytes += size; }
    return result;
  };
  const original = Module._resolveFilename;
  Module._resolveFilename = function(request, ...args) {
    if (typeof request === 'string' && request.startsWith('file:')) {
      try {
        const target = realpathSync.native(fileURLToPath(request));
        if (roots.some(root => {
          const path = relative(root, target);
          return path === '' || (!isAbsolute(path) && path !== '..' && !path.startsWith('..' + sep));
        })) request = target;
      } catch {
        // Leave invalid/missing/unowned targets to the original resolver.
      }
    }
    return original.call(this, request, ...args);
  };
}
