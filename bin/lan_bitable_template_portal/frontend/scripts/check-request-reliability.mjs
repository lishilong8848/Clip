import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import ts from "typescript";

async function loadSource(name) {
  let source = await readFile(new URL(`../src/${name}`, import.meta.url), "utf8");
  if (name === 'api/client.ts') {
    const cache = await readFile(new URL('../src/api/readCache.ts', import.meta.url), 'utf8');
    const compiled = ts.transpileModule(cache, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext } }).outputText;
    source = source.replace("from './readCache'", `from 'data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}'`);
  }
  const { outputText } = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext } });
  return import(`data:text/javascript;base64,${Buffer.from(outputText).toString("base64")}`);
}
const events = [];
globalThis.window = {
  setTimeout, clearTimeout,
  dispatchEvent: event => events.push(event.type),
  addEventListener: () => {},
  location: { pathname: "/test", search: "", href: "http://localhost/test", assign: () => {}, reload: () => events.push("reload") },
};
globalThis.document = { body: { style: { overflow: "scroll" } } };
const { requestJson, requestBinaryJson } = await loadSource("api/client.ts");
const { resilientStorage } = await loadSource("browserStorage.ts");
const { acquireModal } = await loadSource("modalState.ts");
const { requestPageReload, registerNavigationGuard } = await loadSource("navigation.ts");
let confirmReload;
requestPageReload(proceed => { confirmReload = proceed; });
assert(!events.includes("reload"));
confirmReload();
assert(events.includes("reload"));
const removeGuard = registerNavigationGuard(() => false);
requestPageReload(() => { throw new Error("must respect active editor guard"); });
assert.equal(events.filter(event => event === "reload").length, 1);
removeGuard();
let warnings = 0;
Object.defineProperty(window, "localStorage", { get() { throw new Error("storage disabled"); } });
const storage = resilientStorage("localStorage", () => warnings++);
assert.equal(storage.getItem("unknown"), null);
storage.setItem("operation", "same-operation-id");
assert.equal(storage.getItem("operation"), "same-operation-id");
storage.removeItem("operation");
assert.equal(storage.getItem("operation"), null);
assert.equal(warnings, 3);
const parent = acquireModal(), child = acquireModal();
assert.equal(parent.isTop(), false);
assert.equal(child.isTop(), true);
parent.release();
assert.equal(document.body.style.overflow, "hidden");
child.release();
assert.equal(document.body.style.overflow, "scroll");
const next = acquireModal();
child.release();
assert.equal(document.body.style.overflow, "hidden");
next.release();

const { useGuardedPolling } = await loadSource('useGuardedPolling.ts');
const originalSetInterval = globalThis.setInterval, originalClearInterval = globalThis.clearInterval;
const intervals = new Set(), visibilityListeners = new Set(), finishes = [];
let started = 0, active = 0, maximum = 0;
document.hidden = false;
document.addEventListener = (_name, listener) => visibilityListeners.add(listener);
document.removeEventListener = (_name, listener) => visibilityListeners.delete(listener);
globalThis.setInterval = callback => { intervals.add(callback); return callback; };
globalThis.clearInterval = callback => intervals.delete(callback);
const polling = useGuardedPolling(() => new Promise(resolve => {
  started++; active++; maximum = Math.max(maximum, active);
  finishes.push(() => { active--; resolve(); });
}), 1000);
try {
  polling.update(true);
  await new Promise(setImmediate);
  assert.equal(started, 1);
  polling.stop(); polling.update(true);
  for (const callback of intervals) callback();
  await new Promise(setImmediate);
  assert.equal(started, 1, 'reopening must wait for the old request');
  finishes.shift()();
  await new Promise(setImmediate);
  assert.equal(started, 2, 'reopening must refresh after the old request finishes');
  assert.equal(maximum, 1);
  polling.stop(); finishes.shift()();
  await new Promise(setImmediate);
  assert.equal(started, 2, 'a stopped poller must not restart');
  document.hidden = true; polling.update(true);
  await new Promise(setImmediate);
  assert.equal(started, 2);
  document.hidden = false;
  for (const callback of visibilityListeners) callback();
  await new Promise(setImmediate);
  assert.equal(started, 3);
  polling.stop(); finishes.shift()();
  await new Promise(setImmediate);
  assert.equal(intervals.size, 0);
  assert.equal(visibilityListeners.size, 0);
} finally {
  polling.stop();
  globalThis.setInterval = originalSetInterval; globalThis.clearInterval = originalClearInterval;
}

const server = createServer((req, res) => {
  res.setHeader("Content-Type", "application/json");
  if (req.url === "/stall") { res.writeHead(200); res.write('{"data":'); return; }
  if (req.url === "/bad") { res.end("<html>Error</html>"); return; }
  if (req.url === "/array") { res.end("[]"); return; }
  if (req.url === "/null") { res.end("null"); return; }
  if (req.url === "/unauthorized") { res.writeHead(401); res.end("not JSON"); return; }
  if (req.url === "/unauthorized-broken") { res.writeHead(401); res.write('{"data":'); setTimeout(() => res.destroy(), 50); return; }
  if (req.url === "/empty") { res.writeHead(204); res.end(); return; }
  if (req.url === "/binary") {
    const chunks = [];
    req.on("data", data => chunks.push(data));
    req.on("end", () => res.end(JSON.stringify({ data: { body: Buffer.concat(chunks).toString(), type: req.headers["content-type"] } })));
    return;
  }
  res.end('{"ok":true,"data":{"id":1}}');
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const url = `http://127.0.0.1:${server.address().port}`;
try {
  assert.deepEqual(await requestJson(url), { id: 1 });
  assert.deepEqual(await requestJson(url + "/empty"), {});
  for (const route of ["/bad", "/array", "/null"]) {
    await assert.rejects(requestJson(url + route), error => error.status === 200 && error.message.includes("格式异常"));
  }
  await assert.rejects(requestJson(url + "/stall", { timeoutMs: 1000 }), /请求超时/);
  const controller = new AbortController();
  const pending = requestJson(url + "/stall", { signal: controller.signal });
  setTimeout(() => controller.abort(), 100);
  await assert.rejects(pending, /请求已取消/);
  await assert.rejects(requestBinaryJson(url + "/bad", "test"), /格式异常/);
  assert.deepEqual(await requestBinaryJson(url + "/binary", "image", { headers: { "Content-Type": "image/png" } }), { body: "image", type: "image/png" });
  await assert.rejects(requestJson(url + "/unauthorized"), error => error.authRequired && error.status === 401);
  await assert.rejects(requestJson(url + "/unauthorized-broken"), error => error.authRequired && error.status === 401);
  assert(events.includes("clipflow-auth-expired"));
  console.log("request reliability, storage fallback and modal ownership passed");
} finally {
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}
