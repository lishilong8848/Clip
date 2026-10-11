import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { chromium } from "playwright";
import { preview } from "vite";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outputDir = path.resolve(root, "../../../output/playwright/knowledge-upload");
await mkdir(outputDir, { recursive: true });

// ---------------------------------------------------------------------------
// uploadJson semantics (pre-abort + timeout/abort never means offline).
// These run on the real client.ts source (dynamic import, Node 24 type-stripped)
// so they validate the point-1 fix without depending on a freshly built dist.
// ---------------------------------------------------------------------------
// Load client.ts (transpiled, with readCache inlined via data URL) so we can exercise
// uploadJson's abort/offline semantics against the real source without a built dist.
async function loadClient() {
  let source = await readFile(new URL("../src/api/client.ts", import.meta.url), "utf8");
  const cache = await readFile(new URL("../src/api/readCache.ts", import.meta.url), "utf8");
  const compiled = ts.transpileModule(cache, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext } }).outputText;
  source = source.replace("from './readCache'", `from 'data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}'`);
  const { outputText } = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.ESNext } });
  return import(`data:text/javascript;base64,${Buffer.from(outputText).toString("base64")}`);
}

async function runUploadJsonSemanticsChecks() {
  const events = { offlineDispatches: 0 };
  globalThis.window = {
    setTimeout, clearTimeout,
    addEventListener: () => {},
    dispatchEvent: (event) => { if (event.type === "clipflow-api-offline") events.offlineDispatches += 1; return true; },
    location: { search: "", pathname: "/", protocol: "https:" },
  };

  let sendCount = 0;
  let networkErrorOnSend = false;
  let timeoutOnSend = false;
  class FakeXMLHttpRequest {
    constructor() { this.upload = { onprogress: null }; this.timeout = 0; this.status = 0; this.response = null; }
    open() {}
    send() {
      sendCount += 1;
      if (networkErrorOnSend) this.onerror?.();
      else if (timeoutOnSend) this.ontimeout?.();
      else this.onload?.();
    }
    abort() {}
  }
  globalThis.XMLHttpRequest = FakeXMLHttpRequest;

  const { uploadJson, ApiError } = await loadClient();

  // (a) Pre-aborted signal -> reject "已取消", never open/send a request, never offline.
  const preAborted = new AbortController();
  preAborted.abort();
  networkErrorOnSend = timeoutOnSend = false;
  sendCount = 0;
  events.offlineDispatches = 0;
  let preError = null;
  try {
    await uploadJson("/upload", new FormData(), { signal: preAborted.signal });
  } catch (e) { preError = e; }
  assert.ok(preError instanceof ApiError, "pre-aborted upload must reject with ApiError");
  assert.match(preError.message, /已取消/, "pre-aborted upload message must say cancelled");
  assert.equal(sendCount, 0, "pre-aborted signal must never call xhr.send");
  assert.equal(events.offlineDispatches, 0, "pre-aborted signal must not dispatch global offline");

  // (b) Timeout -> reject, but must NOT dispatch the global offline banner.
  timeoutOnSend = true;
  networkErrorOnSend = false;
  sendCount = 0;
  events.offlineDispatches = 0;
  let timeoutError = null;
  try {
    await uploadJson("/upload", new FormData());
  } catch (e) { timeoutError = e; }
  assert.ok(timeoutError instanceof ApiError, "timeout must reject with ApiError");
  assert.match(timeoutError.message, /超时/, "timeout message expected");
  assert.equal(sendCount, 1, "timeout still went through send");
  assert.equal(events.offlineDispatches, 0, "timeout must not dispatch global offline");

  // (c) Genuine network error -> DOES dispatch the global offline banner (requestJson parity).
  networkErrorOnSend = true;
  timeoutOnSend = false;
  sendCount = 0;
  events.offlineDispatches = 0;
  let netError = null;
  try {
    await uploadJson("/upload", new FormData());
  } catch (e) { netError = e; }
  assert.ok(netError instanceof ApiError && netError.offline, "network error must be marked offline");
  assert.equal(sendCount, 1, "network error went through send");
  assert.equal(events.offlineDispatches, 1, "genuine network error must dispatch global offline");

  // Restore Node globals so the browser suite is unaffected.
  delete globalThis.window;
  delete globalThis.XMLHttpRequest;
  console.log("[KnowledgeUpload] uploadJson preabort/timeout/offline semantics OK");
}
await runUploadJsonSemanticsChecks();

const ACCEPT = ".pdf,.docx,.xlsx,.xlsm,.md,.txt,.csv,.png,.jpg,.jpeg,.webp";
const server = await preview({ root, logLevel: "error", preview: { host: "127.0.0.1", port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;

let browser;
let context;
let page;
let failNextName = null;
let delayUploadMs = 0;

// Accumulated across all contexts/cases so early errors are never lost when a new page boots.
const accumulatedErrors = [];
const uploadCalls = [];

// Fixture: one replaceable knowledge document.
const documents = [
  { id: "kb-fixture", name: "公司制度.docx", owner_name: "隔离测试", can_edit: true, active_version: 1, version: 3, status: "ready", updated_at: 1791510000, size: 1200, chunks: 5 },
];
const doc = () => documents[0];

// Parse a multipart body into [{ filename, size }] so the mock can enforce the same
// per-file (100MiB) / batch (300MiB) / count (10) limits as the real backend route.
function parseMultipartParts(buffer) {
  const text = buffer.toString("utf8");
  const parts = [];
  const re = /filename="([^"]+)"/g;
  let hit;
  while ((hit = re.exec(text)) !== null) {
    const name = hit[1];
    // header terminator after the Content-Disposition line ends, then the payload
    const headEnd = text.indexOf("\r\n\r\n", hit.index);
    if (headEnd === -1) break;
    // payload runs until the closing CRLF + boundary
    const partEnd = text.indexOf("\r\n--", headEnd + 4);
    const size = (partEnd === -1 ? text.length : partEnd) - (headEnd + 4);
    parts.push({ filename: name, size });
  }
  return parts;
}

let BODY_LIMIT = 300 * 1024 * 1024;
let PER_FILE_LIMIT = 100 * 1024 * 1024;
let MAX_FILES = 10;

async function bootPage() {
  context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: "block" });
  page = await context.newPage();
  page.setDefaultTimeout(15000);
  uploadCalls.length = 0;

  await page.route("**/api/**", async route => {
    const req = route.request();
    const url = new URL(req.url());
    const method = req.method();
    const p = url.pathname;
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (p === "/api/health") return route.fulfill({ json: { ok: true, service: "clipflow_backend", instance_id: "isolated-knowledge-upload" } });
    if (p === "/api/auth/status") return ok({ logged_in: true, user: { open_id: "upload-fixture", role: "admin", name: "隔离测试" }, scope_options: [{ value: "E", label: "E楼" }] });
    if (p === "/api/assistant/appearance") return ok({ color: "#171717", size: 88, shape: "circle", snap_back: true });
    if (p === "/api/assistant/conversation") return ok({ conversation_id: "upload-fixture", enabled: true, configured: false, turns: [], can_manage_settings: true });
    if (p === "/api/assistant/knowledge") {
      return ok({ items: url.searchParams.get("deleted") === "1" ? [] : documents, total: documents.length, page: 1, page_size: 20, is_admin: true, revision: 3 });
    }
    if (/^\/api\/assistant\/knowledge\/documents\/[^/]+$/.test(p)) {
      return ok({ document: doc(), sections: [], page: 1, page_size: 20, total: 0 });
    }
    if (p === "/api/assistant/knowledge/files" && method === "POST") {
      const body = req.postDataBuffer() || Buffer.from("");
      // Chromium multipart carries raw UTF-8 filenames; never decodeURIComponent (a literal % would throw).
      const names = [...body.toString("utf8").matchAll(/filename="([^"]+)"/g)].map(m => m[1]);
      const query = Object.fromEntries(url.searchParams);
      uploadCalls.push({ names, query, body, method });
      // Mirror the real backend limits: per-file 100MiB, batch 300MiB, max 10 files.
      if (names.length > MAX_FILES) {
        return route.fulfill({ status: 413, json: { ok: false, error: "一次最多上传10个文件。" } });
      }
      const parts = parseMultipartParts(body);
      if (parts.some(p => p.size > PER_FILE_LIMIT)) {
        return route.fulfill({ status: 413, json: { ok: false, error: "单个文件不能超过100MiB。" } });
      }
      if (parts.reduce((sum, p) => sum + p.size, 0) > BODY_LIMIT) {
        return route.fulfill({ status: 413, json: { ok: false, error: "文件合计不能超过300MiB。" } });
      }
      if (delayUploadMs > 0) await new Promise(r => setTimeout(r, delayUploadMs));
      if (failNextName !== null) {
        const failed = failNextName;
        failNextName = null;
        return ok({
          items: names.filter(n => n !== failed).map(n => ({ name: n, status: "queued" })),
          errors: [{ name: failed, error: "隔离索引失败" }],
        });
      }
      return ok({ items: names.map(n => ({ name: n, status: "queued" })), errors: [] });
    }
    if (p === "/api/assistant/files" && method === "POST") {
      // Mock the assistant's own file upload so its attachment flow resolves without real writes.
      return ok({ files: [{ id: "assistant-mock-file", name: "assistant-attachment", size: 12, mime: "text/plain" }] });
    }
    if (p === "/api/repair-management/overview") return ok({});
    if (p === "/api/capacity/water/buildings") return ok({ scopes: [], snapshot: {} });
    return ok({});
  });

  page.on("pageerror", error => accumulatedErrors.push(`pageerror: ${error.message}`));
  page.on("console", message => {
    // Ignore the generic browser network-resource marker so environment noise never hides real JS errors.
    if (message.type() === "error" && !/Failed to load resource/.test(message.text())) {
      accumulatedErrors.push(`console: ${message.text()}`);
    }
  });
  await page.goto(origin + "/knowledge-base");
  await page.getByRole("button", { name: "上传文件", exact: true }).waitFor();
  return page;
}

async function closeCurrentContext() {
  if (context) {
    await context.close();
    context = null;
    page = null;
  }
}

async function screenshot(name) {
  await page.screenshot({ path: path.join(outputDir, name), fullPage: true, animations: "disabled" });
}

async function openUpload() {
  await page.getByRole("button", { name: "上传文件", exact: true }).click();
  await page.locator(".kb-upload").waitFor();
}

async function queueNames() {
  return page.locator(".kb-upload-list > li").evaluateAll(li => li.map(item => item.textContent || ""));
}

async function queueCount() {
  return page.locator(".kb-upload-list > li").count();
}

async function removeButton(filename) {
  return page.getByRole("button", { name: "移除" + filename, exact: true });
}

// Drop onto the dropzone. specs = [[name, sizeBytes] | [name, textContent], ...].
// Large files are constructed inside the page so no >100MiB payload crosses the Playwright protocol.
async function dropFiles(specs) {
  return page.locator(".kb-dropzone").evaluate((element, list) => {
    const data = new DataTransfer();
    for (const [name, size] of list) {
      const part = typeof size === "number" ? new ArrayBuffer(size) : size;
      data.items.add(new File([part], name, { type: "text/plain" }));
    }
    const event = new DragEvent("drop", { dataTransfer: data, bubbles: true, cancelable: true });
    element.dispatchEvent(event);
    return event.defaultPrevented;
  }, specs);
}

// File paste with a DataTransfer of Files; dispatch on a selector element or document.body when selector is null.
async function pasteFiles(selector, names) {
  return page.evaluate(([sel, names]) => {
    const data = new DataTransfer();
    for (const name of names) data.items.add(new File(["payload-" + name], name, { type: "text/plain" }));
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    (sel ? document.querySelector(sel) : document.body).dispatchEvent(event);
    return event.defaultPrevented;
  }, [selector, names]);
}

// Native plain-text ClipboardEvent paste on document.body.
async function pastePlainText(text) {
  return page.evaluate(text => {
    const data = new DataTransfer();
    data.setData("text/plain", text);
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    document.body.dispatchEvent(event);
    return event.defaultPrevented;
  }, text);
}

async function validationErrorText() {
  const alert = page.locator(".kb-upload .alert.error").first();
  return (await alert.count()) ? (await alert.innerText()) : "";
}

function uploadResponseWait() {
  return page.waitForResponse(r => r.url().includes("/api/assistant/knowledge/files") && r.request().method() === "POST");
}

// Click through the explicit confirm gate with a response wait registered BEFORE the triggering click,
// so fast mocked responses can never be missed.
async function confirmUpload() {
  const resp = uploadResponseWait();
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "确认上传" });
  await dialog.waitFor();
  await dialog.getByRole("button", { name: "确认", exact: true }).click();
  await resp;
}

try {
  browser = await chromium.launch({ headless: true });

  // ---- 1. Back button visible and navigates fallback home ----
  await bootPage();
  const backButton = page.getByRole("button", { name: "返回", exact: true });
  await backButton.waitFor();
  assert.ok(await backButton.isVisible(), "返回 button must be visible on /knowledge-base");
  await backButton.click();
  await page.waitForURL(u => new URL(u).pathname === "/");
  assert.equal(new URL(page.url()).pathname, "/", "返回 falls back home");
  await page.locator(".kb-page").waitFor({ state: "detached" });
  await screenshot("back-fallback.png");
  await closeCurrentContext();

  // ---- 2. Picker accept + queue accumulation + remove ----
  await bootPage();
  await openUpload();
  const input = page.getByLabel("选择知识库文件", { exact: true });
  await input.waitFor({ state: "attached" }); // hidden file input: default visible wait would time out
  assert.notEqual(await input.getAttribute("multiple"), null, "new upload picker must be multiple");
  const accept = await input.getAttribute("accept");
  for (const ext of ACCEPT.split(",")) {
    assert.ok(accept && accept.includes(ext), `accept attribute must contain ${ext}`);
  }
  await input.setInputFiles([
    { name: "甲.txt", mimeType: "text/plain", buffer: Buffer.from("甲") },
    { name: "乙.md", mimeType: "text/markdown", buffer: Buffer.from("# 乙") },
  ]);
  await page.locator(".kb-upload-list > li").first().waitFor();
  assert.equal(await queueCount(), 2, "picker selection must accumulate");
  assert.ok((await queueNames()).some(t => t.includes("甲.txt")));
  assert.ok((await queueNames()).some(t => t.includes("乙.md")));
  await (await removeButton("甲.txt")).click();
  assert.equal(await queueCount(), 1, "remove must delete only that file");
  assert.ok((await queueNames()).some(t => t.includes("乙.md")));
  await screenshot("picker-queue.png");
  await closeCurrentContext();

  // ---- 3. Native-button dropzone focus; drop + body paste accumulate; native search paste not hijacked ----
  await bootPage();
  await openUpload();
  const dropzone = page.locator(".kb-dropzone");
  await dropzone.waitFor();
  // .kb-dropzone is a native <button>: inherently keyboard focusable, no tabindex attribute required.
  await dropzone.focus();
  assert.equal(
    await page.evaluate(() => document.activeElement?.classList.contains("kb-dropzone") ?? false),
    true,
    "native button dropzone must be the active/focusable element"
  );
  assert.equal(await dropFiles([["drop-一.txt", "a"], ["drop-二.txt", "b"], ["drop-img.png", "c"]]), true, "drop on dropzone must be handled");
  await page.locator(".kb-upload-list > li").nth(2).waitFor();
  assert.equal(await queueCount(), 3, "drop must accumulate 3 files");
  assert.equal(await pasteFiles(null, ["paste-body.csv"]), true, "body file paste must be handled");
  await page.locator(".kb-upload-list > li").nth(3).waitFor();
  assert.equal(await queueCount(), 4, "body paste must accumulate");
  await (await removeButton("drop-一.txt")).click();
  assert.equal(await queueCount(), 3, "remove must drop only that file");
  // paste on the native search field must NOT be hijacked
  const search = page.getByLabel("搜索知识库", { exact: true });
  assert.equal(await search.count(), 1, "search field expected");
  const beforeSearchPaste = await queueCount();
  const preventedSearch = await search.evaluate((el, names) => {
    const data = new DataTransfer();
    for (const name of names) data.items.add(new File(["payload-" + name], name, { type: "text/plain" }));
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    el.dispatchEvent(event);
    return event.defaultPrevented;
  }, ["不应进入.txt"]);
  assert.equal(preventedSearch, false, "paste on search field must not be intercepted");
  assert.equal(await queueCount(), beforeSearchPaste, "search paste must not add queue item");
  await screenshot("drop-paste-accumulate.png");
  await closeCurrentContext();

  // ---- 4. Native plain-text paste; knowledge-base text-entry input paste not hijacked; same-name rejection;
  //         pending text auto-included by 确认上传; pending queue preserved when dialog paste arrives ----
  await bootPage();
  await openUpload();

  // plain-text native ClipboardEvent paste populates text mode (not merely filling the textarea)
  assert.equal(await pastePlainText("剪贴板纯文本 信息"), true, "plain text body paste must be handled (preventDefault)");
  assert.equal(await queueCount(), 0, "plain text paste must not become a file queue item");
  const textToggle = page.getByRole("button", { name: "粘贴文本", exact: true });
  assert.equal(await textToggle.getAttribute("aria-pressed"), "true", "plain paste must switch to text mode");
  const contentArea = page.getByLabel("文档内容", { exact: true });
  await contentArea.waitFor();
  await page.waitForFunction(txt => {
    const el = document.querySelector("textarea[aria-label='文档内容']");
    return !!el && el.value === txt;
  }, "剪贴板纯文本 信息");
  assert.equal(await contentArea.inputValue(), "剪贴板纯文本 信息", "native plain-text paste must populate content textarea");

  // knowledge-base text-entry input paste must NOT be hijacked (kept inside the textarea, no file/text injection)
  const beforeText = "待保留正文";
  await contentArea.fill(beforeText);
  const preventedText = await contentArea.evaluate((el, files) => {
    const data = new DataTransfer();
    for (const name of files) data.items.add(new File(["payload-" + name], name, { type: "text/plain" }));
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    el.dispatchEvent(event);
    return event.defaultPrevented;
  }, ["不应注入.txt"]);
  assert.equal(preventedText, false, "KB text-entry input paste must not be hijacked");
  assert.equal(await queueCount(), 0, "KB text-entry input paste must not add file queue");
  assert.equal(await contentArea.inputValue(), beforeText, "KB text-entry content must stay untouched");

  // same-name rejection preserves the earlier queue
  await page.getByRole("button", { name: "文件", exact: true }).click();
  await page.locator(".kb-dropzone").waitFor();
  assert.equal(await dropFiles([["同.txt", "第一份"]]), true, "first same-name file accepted");
  await page.locator(".kb-upload-list > li").first().waitFor();
  assert.equal(await queueCount(), 1);
  assert.equal(await dropFiles([["同.txt", "第二份"]]), true, "duplicate drop reaches handler (preventDefault)");
  assert.equal(await queueCount(), 1, "duplicate must not add a second item");
  assert.match(await validationErrorText(), /同名|移除|重命名/i, "same-name error message expected");
  await (await removeButton("同.txt")).click();
  assert.equal(await queueCount(), 0);

  // pending text is automatically included by 确认上传 (requestUpload -> addText) without explicit 加入待上传
  await page.getByRole("button", { name: "粘贴文本", exact: true }).click();
  const nameField = page.getByLabel("文档名称", { exact: true });
  await nameField.fill("自动纳入");
  await contentArea.fill("自动纳入内容");
  assert.equal(uploadCalls.length, 0, "no upload yet");
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "确认上传" });
  await dialog.waitFor();
  assert.equal(uploadCalls.length, 0, "confirm gate: dialog open must not upload yet");
  await page.waitForFunction(() => Array.from(document.querySelectorAll(".kb-upload-list > li")).some(li => li.textContent && li.textContent.includes("自动纳入.txt")));
  assert.ok((await queueNames()).some(t => t.includes("自动纳入.txt")), "pending text must be auto-added on 确认上传");
  await dialog.getByRole("button", { name: "取消", exact: true }).click();
  assert.equal(uploadCalls.length, 0, "cancelling auto-included upload must not POST");

  // pending queue preserved when a paste arrives while confirm dialog is open (inputBlocked)
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  await page.getByRole("dialog", { name: "确认上传" }).waitFor();
  assert.equal(await queueCount(), 1, "one queued file before dialog paste");
  const preventedDialogPaste = await pasteFiles(null, ["不应追加.txt"]);
  assert.equal(preventedDialogPaste, false, "paste while dialog open must not be hijacked");
  assert.equal(await queueCount(), 1, "pending queue must be preserved when dialog paste arrives");
  assert.ok(!(await queueNames()).some(t => t.includes("不应追加.txt")), "dialog paste must not append a file");
  await page.getByRole("dialog", { name: "确认上传" }).getByRole("button", { name: "取消", exact: true }).click();
  await screenshot("paste-guards.png");
  await closeCurrentContext();

  // ---- 4b. Real assistant isolation: plain-text paste + file paste/drop on the actual 询问灯塔助手
  //           textbox/panel never touch the KB pending files/text draft; assistant file upload route is
  //           mocked; panel closes via 收起助手 ----
  await bootPage();
  await openUpload();
  // seed a known KB text draft + pending file queue
  await page.getByRole("button", { name: "粘贴文本", exact: true }).click();
  const kbNameField = page.getByLabel("文档名称", { exact: true });
  const kbContentField = page.getByLabel("文档内容", { exact: true });
  await kbNameField.fill("待办知识库名称");
  await kbContentField.fill("待办知识库正文");
  assert.equal(await pasteFiles(null, ["待办文件.txt"]), true, "fixture KB pending file must be accepted");
  await page.locator(".kb-upload-list > li").first().waitFor();
  assert.equal(await queueCount(), 1, "fixture needs exactly one pending KB file");
  const kbQueueBefore = await queueNames();
  const kbNameBefore = await kbNameField.inputValue();
  const kbContentBefore = await kbContentField.inputValue();

  // open the real assistant used by the KB page
  await page.getByRole("button", { name: "打开灯塔助手", exact: true }).click();
  const assistantInput = page.getByRole("textbox", { name: "询问灯塔助手", exact: true });
  await assistantInput.waitFor();
  const panel = page.locator(".assistant-panel");
  await panel.waitFor();

  // plain-text paste on the actual assistant textbox must not change KB pending state
  await assistantInput.evaluate(el => {
    const data = new DataTransfer();
    data.setData("text/plain", "助手纯文本");
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    el.dispatchEvent(event);
    return event.defaultPrevented; // assistant may or may not preventDefault for plain text; only KB state matters
  });
  assert.deepEqual(await queueNames(), kbQueueBefore, "assistant plain paste must not change KB pending files");
  assert.equal(await queueCount(), kbQueueBefore.length, "assistant plain paste must not add KB queue items");
  assert.equal(await kbNameField.inputValue(), kbNameBefore, "assistant plain paste must not change KB text draft name");
  assert.equal(await kbContentField.inputValue(), kbContentBefore, "assistant plain paste must not change KB text draft content");

  // file paste on the actual assistant textbox; assistant MAY preventDefault for its own attachment support
  const kbQueueBeforeFile = await queueNames();
  const kbNameBeforeFile = await kbNameField.inputValue();
  const kbContentBeforeFile = await kbContentField.inputValue();
  const assistantFilePrevented = await assistantInput.evaluate(el => {
    const data = new DataTransfer();
    data.items.add(new File(["assistant-attach"], "助手附件.txt", { type: "text/plain" }));
    const event = new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true });
    el.dispatchEvent(event);
    return event.defaultPrevented;
  });
  void assistantFilePrevented; // do NOT assert false: assistant handles its own attachments
  await page.waitForTimeout(400); // let any mocked assistant upload settle
  assert.deepEqual(await queueNames(), kbQueueBeforeFile, "assistant file paste must not change KB pending files");
  assert.equal(await queueCount(), kbQueueBeforeFile.length, "assistant file paste must not add KB queue items");
  assert.equal(await kbNameField.inputValue(), kbNameBeforeFile, "assistant file paste must not change KB text draft name");
  assert.equal(await kbContentField.inputValue(), kbContentBeforeFile, "assistant file paste must not change KB text draft content");

  // file drop on the actual assistant panel; assistant MAY preventDefault for its own attachment support
  const kbQueueBeforeDrop = await queueNames();
  const kbNameBeforeDrop = await kbNameField.inputValue();
  const kbContentBeforeDrop = await kbContentField.inputValue();
  const assistantDropPrevented = await panel.evaluate(el => {
    const data = new DataTransfer();
    data.items.add(new File(["assistant-drop"], "助手拖放.txt", { type: "text/plain" }));
    const event = new DragEvent("drop", { dataTransfer: data, bubbles: true, cancelable: true });
    el.dispatchEvent(event);
    return event.defaultPrevented;
  });
  void assistantDropPrevented; // do NOT assert false: assistant handles its own attachments
  await page.waitForTimeout(400); // let any mocked assistant upload settle
  assert.deepEqual(await queueNames(), kbQueueBeforeDrop, "assistant drop must not change KB pending files");
  assert.equal(await queueCount(), kbQueueBeforeDrop.length, "assistant drop must not add KB queue items");
  assert.equal(await kbNameField.inputValue(), kbNameBeforeDrop, "assistant drop must not change KB text draft name");
  assert.equal(await kbContentField.inputValue(), kbContentBeforeDrop, "assistant drop must not change KB text draft content");

  // close the assistant
  await page.getByRole("button", { name: "收起助手", exact: true }).click();
  await page.locator(".assistant-panel").waitFor({ state: "detached" });
  await screenshot("assistant-input-isolation.png");
  await closeCurrentContext();

  // ---- 5. Plain text -> UTF8 txt multipart content + filename; explicit confirm gate ----
  await bootPage();
  await openUpload();
  await page.getByRole("button", { name: "粘贴文本", exact: true }).click();
  await page.getByLabel("文档名称", { exact: true }).fill("测试笔记");
  await page.getByLabel("文档内容", { exact: true }).fill("第一行\n第二行 知识库内容");
  assert.equal(uploadCalls.length, 0, "confirm gate: no upload before 加入待上传");
  await page.getByRole("button", { name: "加入待上传", exact: true }).click();
  await page.locator(".kb-upload-list > li").waitFor();
  assert.ok((await queueNames()).some(t => t.includes("测试笔记.txt")), "text mode queues a .txt with the entered name");
  assert.equal(uploadCalls.length, 0, "confirm gate: still no upload before 确认上传");
  // cancel the dialog first to prove the gate is explicit
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  let confirmDialog = page.getByRole("dialog", { name: "确认上传" });
  await confirmDialog.waitFor();
  assert.equal(uploadCalls.length, 0, "confirm gate: dialog must be explicitly confirmed");
  await confirmDialog.getByRole("button", { name: "取消", exact: true }).click();
  assert.equal(uploadCalls.length, 0, "confirm gate: cancelling must not upload");
  // confirm with a response wait registered before the click
  await confirmUpload();
  assert.equal(uploadCalls.length, 1, "exactly one upload POST expected");
  const call = uploadCalls[0];
  assert.deepEqual(call.query, {}, "add mode must not send document_id/version");
  assert.deepEqual(call.names, ["测试笔记.txt"]);
  const bodyText = call.body.toString("utf8");
  assert.ok(bodyText.includes("第一行"), "multipart body must contain UTF8 text content");
  assert.ok(bodyText.includes("知识库内容"), "multipart body must contain UTF8 text content");
  await screenshot("text-mode-upload.png");
  await page.locator(".kb-upload").waitFor({ state: "detached" });
  assert.equal(await page.locator(".kb-upload").count(), 0, "panel closes after full success");
  await closeCurrentContext();

  // ---- 6. Unsupported / count >10 / per-file 100MiB / aggregate 300MiB validation ----
  await bootPage();
  await openUpload();
  await page.locator(".kb-dropzone").waitFor();
  const input6 = page.getByLabel("选择知识库文件", { exact: true });
  await input6.waitFor({ state: "attached" });
  await input6.setInputFiles([
    { name: "ok-一.txt", mimeType: "text/plain", buffer: Buffer.from("a") },
    { name: "ok-二.txt", mimeType: "text/plain", buffer: Buffer.from("b") },
  ]);
  await page.locator(".kb-upload-list > li").nth(1).waitFor();
  assert.equal(await queueCount(), 2);

  // unsupported old .doc / .xls rejected with a conversion hint, earlier queue kept
  await input6.setInputFiles({ name: "legacy.doc", mimeType: "application/msword", buffer: Buffer.from("doc") });
  assert.equal(await queueCount(), 2, "unsupported .doc must not be added");
  assert.match(await validationErrorText(), /不支持|转换|格式|doc/i, "unsupported must show conversion hint");
  await input6.setInputFiles({ name: "legacy.xls", mimeType: "application/vnd.ms-excel", buffer: Buffer.from("xls") });
  assert.equal(await queueCount(), 2, "legacy .xls must not be added");
  assert.match(await validationErrorText(), /不支持|转换|格式|xls/i, "legacy .xls must show conversion hint");

  // User selection has no batch-count cap; requests are split internally.
  const many = Array.from({ length: 11 }, (_, i) => ({ name: `many-${i}.txt`, mimeType: "text/plain", buffer: Buffer.from("x") }));
  await input6.setInputFiles(many);
  assert.equal(await queueCount(), 13, 'all files are queued');
  assert.equal(await validationErrorText(), '');

  // single > 100MiB rejected (File built in-page so nothing huge crosses the protocol)
  assert.equal(await dropFiles([["big.txt", 100 * 1024 * 1024 + 1]]), true, "over-size drop reaches handler (preventDefault)");
  assert.equal(await queueCount(), 13, ">100MiB file must not be added");
  assert.match(await validationErrorText(), /100|MiB|大小/i, "per-file size message expected");

  // Total selection may exceed 300MiB; each network request remains bounded.
  const agg = Array.from({ length: 4 }, (_, i) => [`agg-${i + 1}.txt`, 80 * 1024 * 1024]);
  assert.equal(await dropFiles(agg), true, "aggregate drop reaches handler (preventDefault)");
  assert.equal(await queueCount(), 17, 'aggregate size no longer limits selection');
  assert.equal(await validationErrorText(), '');
  await screenshot("validation-100-300.png");
  await closeCurrentContext();

  // ---- 6b. Exactly 100MiB single file is accepted by client validation (queued), then removed.
  //          A full 100MiB POST is intentionally NOT round-tripped through Playwright's route
  //          transport (it exceeds the driver's buffer); client acceptance is what matters and
  //          the mock's server-style limits are exercised with small bodies in 6e below.
  await bootPage();
  await openUpload();
  await page.locator(".kb-dropzone").waitFor();
  assert.equal(await dropFiles([["exact-100.txt", 100 * 1024 * 1024]]), true, "100MiB drop reaches handler (preventDefault)");
  await page.locator(".kb-upload-list > li").first().waitFor();
  assert.equal(await queueCount(), 1, "exactly 100MiB is within per-file limit (accepted)");
  assert.equal(uploadCalls.length, 0, "accepted-but-not-confirmed upload must not POST");
  await screenshot("upload-100mib-accepted.png");
  await (await removeButton("exact-100.txt")).click();
  assert.equal(await queueCount(), 0, "100MiB file removed");
  await closeCurrentContext();

  // ---- 6c. Progress surface while a slow upload is in flight ----
  await bootPage();
  await openUpload();
  await page.locator(".kb-dropzone").waitFor();
  assert.equal(await dropFiles([["progress.txt", "payload"]]), true);
  await page.locator(".kb-upload-list > li").first().waitFor();
  delayUploadMs = 2500;
  const progressResp = uploadResponseWait();
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  const dialog6c = page.getByRole("dialog", { name: "确认上传" });
  await dialog6c.waitFor();
  await dialog6c.getByRole("button", { name: "确认", exact: true }).click();
  const progressBar = page.locator(".kb-upload-progress");
  await progressBar.waitFor({ timeout: 5000 });
  const progressValue = Number(await progressBar.getAttribute("aria-valuenow"));
  assert.ok(Number.isFinite(progressValue) && progressValue >= 0, "progress aria-valuenow must be a number in [0,100]");
  await progressResp;
  delayUploadMs = 0;
  assert.equal(uploadCalls.length, 1, "delayed upload must still send exactly one POST");
  await screenshot("upload-progress.png");
  await closeCurrentContext();

  // ---- 6d. Pre-abort (cancel confirm gate) never sends a request ----
  await bootPage();
  await openUpload();
  await page.locator(".kb-dropzone").waitFor();
  assert.equal(await dropFiles([["preabort.txt", "payload"]]), true);
  await page.locator(".kb-upload-list > li").first().waitFor();
  assert.equal(uploadCalls.length, 0);
  await page.getByRole("button", { name: "确认上传", exact: true }).click();
  const dialog6d = page.getByRole("dialog", { name: "确认上传" });
  await dialog6d.waitFor();
  await dialog6d.getByRole("button", { name: "取消", exact: true }).click();
  assert.equal(uploadCalls.length, 0, "cancelling before confirm must not send the upload request");
  assert.equal(await queueCount(), 1, "pending queue preserved after cancel");
  await screenshot("preabort-cancel.png");
  await closeCurrentContext();

  // ---- 6e. Server-style mock enforces its per-file / batch / count limits (small bodies). ----
  // Lower the mock limits temporarily so the enforcement path is exercised without multi-hundred-MiB
  // payloads; the fixture mirrors the backend's 100MiB / 300MiB / 10 constants by default.
  await bootPage();
  const savedLimits = { PER_FILE_LIMIT, BODY_LIMIT, MAX_FILES };
  PER_FILE_LIMIT = 64 * 1024;   // 64KiB per-file
  BODY_LIMIT = 128 * 1024;      // 128KiB total
  MAX_FILES = 2;
  const postStatus = ([bodies, opts]) => page.evaluate(([parts, query]) => {
    const form = new FormData();
    for (const p of parts) form.append("files", new Blob([p.content], { type: "text/plain" }), p.name);
    const url = `/api/assistant/knowledge/files${query ? "?" + new URLSearchParams(query).toString() : ""}`;
    return fetch(url, { method: "POST", body: form }).then(res => res.status);
  }, [bodies, opts]);
  // per-file over limit -> 413
  assert.equal(await postStatus([[{ name: "over.txt", content: "x".repeat(64 * 1024 + 1) }], {}]), 413, "mock must 413 a per-file over-limit body");
  // batch over limit (two 70KiB = 140KiB > 128KiB) -> 413
  assert.equal(await postStatus([[{ name: "a.txt", content: "x".repeat(70 * 1024) }, { name: "b.txt", content: "x".repeat(70 * 1024) }], {}]), 413, "mock must 413 a batch over-limit body");
  // count over MAX_FILES(2) -> 413
  assert.equal(await postStatus([[{ name: "1.txt", content: "a" }, { name: "2.txt", content: "b" }, { name: "3.txt", content: "c" }], {}]), 413, "mock must 413 over-count body");
  // within limits -> 200
  assert.equal(await postStatus([[{ name: "ok.txt", content: "fine" }], { document_id: "kb-fixture", version: "3" }]), 200, "mock must accept a within-limit body");
  PER_FILE_LIMIT = savedLimits.PER_FILE_LIMIT;
  BODY_LIMIT = savedLimits.BODY_LIMIT;
  MAX_FILES = savedLimits.MAX_FILES;
  await closeCurrentContext();

  // ---- 7. Replacement: same visible panel, exactly 1 file, multiple rejected, query retained ----
  await bootPage();
  await page.locator(".kb-list").waitFor();
  const replaceButton = page.getByRole("button", { name: "替换文件", exact: true }).first();
  await replaceButton.waitFor();
  await replaceButton.click();
  await page.locator(".kb-upload").waitFor();
  assert.ok((await page.locator(".kb-upload-head").innerText()).includes("替换"), "replacement panel must be the same .kb-upload in replace mode");
  const repInput = page.getByLabel("选择知识库文件", { exact: true });
  await repInput.waitFor({ state: "attached" });
  assert.equal(await repInput.getAttribute("multiple"), null, "replacement picker must not be multiple");
  // multiple files (via body paste) must be rejected, no silent first
  await pasteFiles(null, ["r1.txt", "r2.txt"]);
  await page.locator(".kb-upload-list > li, .kb-upload .alert.error").first().waitFor();
  assert.equal(await queueCount(), 0, "replacement with multiple files must be rejected, no silent first");
  assert.match(await validationErrorText(), /1|一个|替换/i, "replacement multi-file message expected");
  await repInput.setInputFiles({ name: "replace.docx", mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document", buffer: Buffer.from("new") });
  await page.locator(".kb-upload-list > li").waitFor();
  assert.equal(await queueCount(), 1, "single replacement file queued");
  await confirmUpload();
  assert.equal(uploadCalls.length, 1, "one replacement POST expected");
  assert.deepEqual(uploadCalls[0].query, { document_id: "kb-fixture", version: "3" }, "replacement must retain document_id and version");
  assert.deepEqual(uploadCalls[0].names, ["replace.docx"]);
  await screenshot("replacement.png");
  await closeCurrentContext();

  // ---- 8. Partial failure: preserve failed files, retry exactly the single failed file ----
  await bootPage();
  await openUpload();
  await page.getByLabel("选择知识库文件", { exact: true }).setInputFiles([
    { name: "失败A.txt", mimeType: "text/plain", buffer: Buffer.from("a") },
    { name: "成功B.txt", mimeType: "text/plain", buffer: Buffer.from("b") },
  ]);
  await page.locator(".kb-upload-list > li").nth(1).waitFor();
  assert.equal(await queueCount(), 2);
  failNextName = "失败A.txt";
  await confirmUpload();
  assert.equal(uploadCalls.length, 1, "first POST expected");
  await page.waitForFunction(() => document.querySelectorAll(".kb-upload-list > li").length === 1);
  assert.equal(await queueCount(), 1, "only failed file preserved after partial failure");
  assert.ok((await queueNames()).some(t => t.includes("失败A.txt")), "failed file kept for retry");
  await confirmUpload();
  assert.equal(uploadCalls.length, 2, "retry POST expected");
  assert.deepEqual(uploadCalls[1].names, ["失败A.txt"], "retry must send only failed files");
  await screenshot("partial-retry.png");
  await closeCurrentContext();

  // Folder enumeration reads every page and uploads >10 files as bounded requests.
  await bootPage();
  await openUpload();
  const fixtureFolder = path.join(outputDir, 'folder-fixture');
  await mkdir(path.join(fixtureFolder, 'nested'), { recursive: true });
  await writeFile(path.join(fixtureFolder, 'nested', 'guide.md'), '# Fixture');
  await writeFile(path.join(fixtureFolder, 'ignored.exe'), 'not executable');
  await page.getByLabel('选择知识库文件夹', { exact: true }).setInputFiles(fixtureFolder);
  assert.equal(await queueCount(), 1);
  assert.match((await queueNames())[0], /folder-fixture__nested__guide.md/);
  await (await removeButton('folder-fixture__nested__guide.md')).click();
  assert.notEqual(await page.getByLabel('选择知识库文件夹', { exact: true }).getAttribute('webkitdirectory'), null);
  await page.locator('.kb-dropzone').evaluate(el => {
    const files = Array.from({ length: 13 }, (_, i) => new File(['fixture'], `item-${i}.txt`));
    const leaves = files.map((file, i) => ({ isDirectory: false, isFile: true, fullPath: `/目录/子目录${i % 2}/${file.name}`, file: resolve => resolve(file) }));
    const folder = { isDirectory: true, isFile: false, createReader: () => {
      const pages = [leaves.slice(0, 5), leaves.slice(5), []];
      return { readEntries: resolve => resolve(pages.shift() || []) };
    }};
    const data = new DataTransfer();
    data.items.add(new File(['placeholder'], 'directory'));
    Object.defineProperty(data, 'items', { value: [{ webkitGetAsEntry: () => folder }] });
    el.dispatchEvent(new DragEvent('drop', { dataTransfer: data, bubbles: true, cancelable: true }));
  });
  await page.waitForFunction(() => document.querySelectorAll('.kb-upload-list > li').length === 13);
  failNextName = '目录__子目录0__item-0.txt';
  await confirmUpload();
  await page.waitForFunction(() => document.querySelectorAll('.kb-upload-list > li').length === 1);
  assert.deepEqual(uploadCalls.map(call => call.names.length), [10, 3]);
  await confirmUpload();
  await page.locator('.kb-upload').waitFor({ state: 'hidden' });
  assert.deepEqual(uploadCalls[2].names, ['目录__子目录0__item-0.txt']);
  await screenshot('folder-batches.png');
  await closeCurrentContext();
  assert.deepEqual(accumulatedErrors, [], `browser errors across cases:\n${accumulatedErrors.join("\n")}`);

  console.log(JSON.stringify({
    ok: true,
    screenshots: outputDir,
    checks: [
      "back visible + fallback home",
      "picker accept (multiple + all extensions)",
      "queue picker/drop/paste accumulation + remove",
      "native-button dropzone focus (no tabindex requirement)",
      "native search paste not hijacked",
      "KB text-entry input paste not hijacked",
      "assistant plain-text/file paste + drop on 询问灯塔助手/assistant-panel never touch KB pending (queue/text draft)",
      "native plain-text ClipboardEvent paste populates text mode (not merely filling textarea)",
      "same-name rejection keeps earlier queue",
      "pending text auto-included by 确认上传",
      "pending queue preserved when confirm dialog paste arrives",
      "plain text -> UTF8 txt multipart content + raw filename (no decodeURIComponent)",
      "explicit confirm gate (确认上传 then dialog 确认; cancel does nothing)",
      "uploadJson semantics: pre-aborted signal sends no request + never offline; timeout never offline; genuine network error offline (source-level)",
      "folder multi-page enumeration, sequential 10-file requests, retry only failed files, unlimited selection count/total with 100MiB per-file limit",
      "exactly 100MiB single file accepted by client validation (queued) + mock server-style per-file/batch/count limit enforcement (small bodies)",
      "upload progress surface appears with aria-valuenow while slow upload in flight",
      "pre-abort (cancel confirm gate) never sends a request and preserves queue",
      "replacement single-file / multi reject / document_id+version retained",
      "partial failure retries exactly the single failed file",
    ],
  }));
} catch (error) {
  if (page) await screenshot("failure.png").catch(() => {});
  console.error("[KnowledgeUploadBrowser] FAILED", error);
  throw error;
} finally {
  await browser?.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
