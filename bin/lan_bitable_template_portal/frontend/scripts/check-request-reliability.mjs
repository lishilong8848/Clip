import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import ts from "typescript";

async function loadSource(name) {
  const source = await readFile(new URL(`../src/${name}`, import.meta.url), "utf8");
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
