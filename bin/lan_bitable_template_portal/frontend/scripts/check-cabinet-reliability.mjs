import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../..");
const output = path.join(root, "output/playwright/reliability");
await mkdir(output, { recursive: true });
const reservation = createServer();
await new Promise(resolve => reservation.listen(0, "127.0.0.1", resolve));
const port = reservation.address().port;
await new Promise(resolve => reservation.close(resolve));
const base = `http://127.0.0.1:${port}`;
const server = spawn("python", ["bin/tools/cabinet_ui_fixture.py", String(port)], { cwd: root, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
let log = "", browser, page;
server.stdout.on("data", data => log += data);
server.stderr.on("data", data => log += data);
try {
  for (let i = 0; i < 120; i++) {
    try { if ((await fetch(base + "/api/auth/status")).ok) break; } catch {}
    if (server.exitCode !== null || i === 119) throw new Error(log || "fixture failed to start");
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  browser = await chromium.launch();
  for (const result of ["normal", "lost", "malformed"]) {
    const context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
    await context.addInitScript(() => {
      for (const name of ["getItem", "setItem", "removeItem"]) {
        Storage.prototype[name] = () => { throw new DOMException("Storage blocked", "SecurityError"); };
      }
    });
    page = await context.newPage();
    const errors = [], operations = [], polls = [];
    page.on("pageerror", error => errors.push(error.message));
    page.on("request", request => {
      if (request.method() === "GET" && request.url().includes("/writes/")) polls.push(request.url());
    });
    await page.route("**/api/cabinet-power/operations/**", async route => {
      if (route.request().method() !== "PATCH") return route.continue();
      operations.push(route.request().postDataJSON().operation_id);
      const response = await route.fetch();
      if (result === "lost") return route.abort("connectionreset");
      if (result === "malformed") return route.fulfill({ status: 200, contentType: "text/html", body: "<html>unexpected proxy page</html>" });
      return route.fulfill({ response });
    });
    await page.goto(base + "/cabinet-power?scope=D");
    await page.getByRole("button", { name: "机柜台账", exact: true }).click();
    await page.getByLabel("筛选包间", { exact: true }).selectOption("201");
    await page.locator(".filter-bar .search input").fill("B03");
    await page.getByRole("button", { name: "查询", exact: true }).click();
    await page.locator(".source-table tbody tr").filter({ hasText: "B03" }).waitFor();
    if (result === "normal") {
      for (const width of [360, 390, 768]) {
        await page.setViewportSize({ width, height: 844 });
        const labels = await page.locator(".source-table tbody td[data-label]").evaluateAll(nodes => nodes.filter(node => getComputedStyle(node).display !== "none").map(node => node.dataset.label));
        assert(labels.some(label => label.startsWith("当前操作 ·")));
        assert(labels.some(label => label.startsWith("历史操作 1 ·")));
        assert(await page.locator(".source-table-wrap").evaluate(node => node.scrollWidth <= node.clientWidth + 2));
        await page.locator(".source-table").evaluate(node => node.scrollIntoView({ block: "start" }));
        await page.screenshot({ path: path.join(output, `ledger-${width}.png`) });
      }
      await page.setViewportSize({ width: 1366, height: 900 });
    }
    await page.getByLabel("编辑记录", { exact: true }).first().click();
    await page.getByLabel("功率（W）").fill(String(15001 + ["normal", "lost", "malformed"].indexOf(result)));
    await page.getByRole("button", { name: "保存", exact: true }).click();
    await page.getByText("已保存到飞书，回读核验成功。", { exact: true }).waitFor({ timeout: 30000 });
    assert.equal(operations.length, 1, "a lost response must not create another operation");
    assert(polls.some(url => url.includes(operations[0])), "must verify the original operation");
    assert(await page.getByText(/浏览器无法保存恢复信息/).isVisible());
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log("cabinet storage-disabled save, lost/malformed responses and mobile history labels passed");
} catch (error) {
  if (page && !page.isClosed()) await page.screenshot({ path: path.join(output, "failure.png") });
  console.error(log.slice(-2500));
  throw error;
} finally {
  await browser?.close();
  if (server.exitCode === null) { const exited = once(server, "exit"); server.kill(); await exited; }
}
