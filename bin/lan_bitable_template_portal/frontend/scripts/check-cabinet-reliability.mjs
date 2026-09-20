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
  const exportContext = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  page = await exportContext.newPage();
  const exportErrors = [];
  page.on("pageerror", error => exportErrors.push(error.message));
  let lostExportResponse = false;
  await page.route("**/api/cabinet-power/exports", async route => {
    if (route.request().postDataJSON().scope !== "C" || lostExportResponse) return route.continue();
    lostExportResponse = true;
    await route.fetch();
    return route.abort("connectionreset");
  });
  await page.goto(base + "/cabinet-power");
  await page.getByRole("button", { name: "一键导出/上传所有楼栋" }).click();
  await page.getByRole("button", { name: "核验并继续" }).first().waitFor({ timeout: 120000 });
  assert(lostExportResponse, "C building response loss was not simulated");
  assert(await page.getByRole("button", { name: "核验并继续" }).first().isEnabled(), "C building retry must not wait for other buildings");
  await page.screenshot({ path: path.join(output, "all-building-export-retry.png") });
  await page.reload();
  await page.getByRole("button", { name: "核验并继续" }).first().waitFor({ timeout: 30000 });
  await page.unroute("**/api/cabinet-power/exports");
  for (let attempt = 0; attempt < 10; attempt++) {
    const buttons = page.getByRole("button", { name: "核验并继续" });
    const count = await buttons.count();
    if (!count) break;
    let clicked = false;
    for (let index = 0; index < count; index++) {
      if (await buttons.nth(index).isEnabled()) { await buttons.nth(index).click(); clicked = true; break; }
    }
    await page.waitForTimeout(clicked ? 500 : 1000);
  }
  await page.getByText("所有楼栋均已导出并上传多维表。", { exact: true }).waitFor({ timeout: 120000 });
  const history = await (await page.request.get(base + "/api/cabinet-power/export-history?scope=C")).json();
  assert.equal(history.data.items.length, 1, "retry must reuse the original C building export");
  assert.deepEqual(exportErrors, []);
  await page.screenshot({ path: path.join(output, "all-building-export.png") });
  let lostSingleResponse = false;
  await page.route("**/api/cabinet-power/exports", async route => {
    const payload = route.request().postDataJSON();
    if (payload.scope !== "B" || !payload.batch_id?.startsWith("single_") || lostSingleResponse) return route.continue();
    lostSingleResponse = true;
    await route.fetch();
    return route.abort("connectionreset");
  });
  await page.goto(base + "/cabinet-power?scope=B");
  await page.getByRole("button", { name: "导出", exact: true }).click();
  await page.getByRole("button", { name: "继续上次导出" }).waitFor({ timeout: 30000 });
  assert(lostSingleResponse, "single-building response loss was not simulated");
  await page.getByText("导出启动响应未确认，后台可能仍在执行。请点击“继续上次导出”核验原任务。", { exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, "single-building-export-retry.png") });
  await page.unroute("**/api/cabinet-power/exports");
  await page.goto(base + "/cabinet-power");
  await page.route("**/api/cabinet-power/bootstrap", async route => {
    await new Promise(resolve => setTimeout(resolve, 3000));
    await route.continue();
  });
  await page.route("**/api/cabinet-power/exports", async route => {
    await new Promise(resolve => setTimeout(resolve, 3000));
    await route.continue();
  });
  await page.goto(base + "/cabinet-power?scope=B");
  await page.getByText("正在准备导出数据…", { exact: true }).waitFor({ timeout: 1500 });
  assert.equal(await page.getByRole("button", { name: "继续上次导出", exact: true }).isEnabled(), false, "restored export button must stay disabled");
  await page.getByText("已上传多维", { exact: true }).waitFor({ timeout: 120000 });
  const bHistory = await (await page.request.get(base + "/api/cabinet-power/export-history?scope=B")).json();
  assert.equal(bHistory.data.items.length, 2, "single-building retry must not create a duplicate export");
  const singleRecord = bHistory.data.items.find(item => item.batch_id?.startsWith("single_"));
  assert(singleRecord, "single-building export is missing from history");
  await page.route("**/api/cabinet-power/overview**", async route => {
    const response = await route.fetch(); const body = await response.json();
    body.data.export_state = { has_export:true,is_stale:true,stale_reason:"通告汇总已变化" };
    await route.fulfill({ response, json:body });
  });
  await page.route("**/api/cabinet-power/export-history**", async route => {
    const response = await route.fetch(); const body = await response.json();
    body.data.items = (body.data.items || []).map(item => ({ ...item,is_stale:true,stale_reason:"通告汇总已变化" }));
    await route.fulfill({ response, json:body });
  });
  await page.reload();
  await page.getByText("最近导出已过期：通告汇总已变化。请重新导出。", { exact:true }).waitFor();
  await page.getByRole("button", { name: "导出历史" }).click();
  await page.getByText("已过期", { exact:true }).first().waitFor();
  await page.getByText("通告汇总已变化", { exact:true }).first().waitFor();
  const singleRow = page.locator(".mobile-card-table tr").filter({ has: page.locator(`a[href*="/exports/${singleRecord.export_id}/download"]`) });
  await singleRow.getByRole("button", { name: "清理导出文件" }).click();
  await page.getByRole("dialog", { name: "清理导出文件？" }).waitFor();
  await page.screenshot({ path: path.join(output, "export-cleanup-confirmation.png") });
  await page.getByRole("button", { name: "清理文件" }).click();
  await page.getByText("本地导出文件已清理，云端归档不受影响。", { exact: true }).waitFor();
  assert.equal(await page.locator(`a[href*="/exports/${singleRecord.export_id}/download"]`).count(), 0);
  const afterCleanup = await (await page.request.get(base + "/api/cabinet-power/export-history?scope=B")).json();
  assert.equal(afterCleanup.data.items.length, 2, "cleanup must retain export history");
  assert.equal(afterCleanup.data.items.find(item => item.export_id === singleRecord.export_id).file_available, false);
  assert(await page.getByText("本地已清理", { exact: true }).isVisible());
  await page.unroute("**/api/cabinet-power/overview**");
  await page.unroute("**/api/cabinet-power/export-history**");
  assert.deepEqual(exportErrors, []);
  await exportContext.close();

  const batchContext = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  page = await batchContext.newPage();
  const batchErrors = [];
  page.on("pageerror", error => batchErrors.push(error.message));
  const rows = Array.from({ length: 60 }, (_, index) => ({
    row_id:`row-${index + 1}`, scope:"E", room:"202", rack:`B${String(index + 1).padStart(2,"0")}`,
    rack_type:"服务器机柜", action:"上正式电", expected:"2026-09-19 10:00:00", actual:"",
    result:"成功", status:"ready", issues:[], edits:[], evidence_images:[], editable:true,
    confirmable:true, rollbackable:false, restorable:false, can_edit_notice_summary:true,
  }));
  const images = Array.from({ length: 25 }, (_, index) => ({
    image_id:`image-${index + 1}`, name:`确认截图-${index + 1}.png`, extension:".png",
    status:"done", suggestions:[], error:"",
  }));
  let batch = {
    batch_id:"batch-test", owner_id:"owner", source:"notice", status:"pending", version:1,
    created_at:"2026-09-19 10:00:00", scopes:["E"], allowed_scopes:["E"], rows, images,
    stats:{ total:60,new:60,confirmable:60,duplicate:0,conflict:0,invalid:0,completed:0,failed:0,rolled_back:0 },
    source_notice:{ notice_type:"上电通告",title:"E楼机柜上电通告",scope:"E",start_time:"2026-09-19 09:00:00",end_time:"2026-09-19 18:00:00",sent_at:"2026-09-19 09:01:00",ended_at:"",cabinet:"E-202包间B01至B60" },
    notice_counts:{declared:60,unique:60,directory_matched:60}, can_download_files:true,can_confirm_all:true,
  };
  let rejectPatch = true;
  const pixel = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=", "base64");
  await page.route("**/api/cabinet-power/batches/batch-test**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.includes("/images/")) return route.fulfill({ status:200,contentType:"image/png",body:pixel });
    if (route.request().method() === "PATCH") {
      if (rejectPatch) return route.abort("connectionreset");
      for (const patch of route.request().postDataJSON().rows || []) Object.assign(batch.rows.find(row => row.row_id === patch.row_id), patch);
      batch = { ...batch,version:batch.version + 1 };
    }
    return route.fulfill({ status:200,contentType:"application/json",body:JSON.stringify({ok:true,data:batch}) });
  });
  await page.goto(base + "/cabinet-power/batches?scope=E&batch_id=batch-test");
  await page.getByRole("heading", { name:"上下电待办详情" }).waitFor();
  assert.equal(await page.locator(".evidence-item").count(),12,"evidence gallery must paginate");
  assert((await page.locator(".evidence-match select").first().locator("option").count()) <= 51,"cabinet matcher must cap options");
  assert(await page.getByText("开始通告实际发送", { exact:true }).isVisible());
  await page.screenshot({ path:path.join(output,"batch-detail-1366.png"),fullPage:true });
  await page.getByRole("button", { name:"编辑记录",exact:true }).first().click();
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").fill("2026-09-19T12:34:56");
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").press("Tab");
  await page.getByText("自动保存失败，请重试", { exact:false }).waitFor({ timeout:10000 });
  await page.reload();
  await page.getByRole("dialog", { name:"恢复未保存的批次更正？" }).waitFor();
  rejectPatch = false;
  await page.getByRole("button", { name:"恢复更正",exact:true }).click();
  await page.getByText("更正已自动保存", { exact:true }).waitFor({ timeout:10000 });
  await page.getByRole("button", { name:"编辑记录",exact:true }).first().click();
  assert.equal(await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").inputValue(),"2026-09-19T12:34:56");
  await page.setViewportSize({ width:1024,height:768 });
  await page.screenshot({ path:path.join(output,"batch-detail-1024.png"),fullPage:true });
  rejectPatch = true;
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").fill("2026-09-19T13:34:56");
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").press("Tab");
  await page.getByText("自动保存失败，请重试", { exact:false }).waitFor({ timeout:10000 });
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByRole("button", { name:"关闭",exact:true }).click();
  await page.getByRole("button", { name:"返回待办",exact:true }).click();
  await page.getByRole("dialog", { name:"放弃未保存的批次修改？" }).waitFor();
  await page.getByRole("button", { name:"放弃修改",exact:true }).click();
  await page.goto(base + "/cabinet-power/batches?scope=E&batch_id=batch-test");
  await page.getByRole("heading", { name:"上下电待办详情" }).waitFor();
  assert.equal(await page.getByRole("dialog", { name:"恢复未保存的批次更正？" }).count(),0,"discarded draft must not return");
  assert.deepEqual(batchErrors, []);
  await batchContext.close();
  console.log("cabinet saves, export response recovery, five-building upload and layout passed");
} catch (error) {
  if (page && !page.isClosed()) await page.screenshot({ path: path.join(output, "failure.png") });
  console.error(log.slice(-2500));
  throw error;
} finally {
  await browser?.close();
  if (server.exitCode === null) { const exited = once(server, "exit"); server.kill(); await exited; }
}
