import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { mkdir, readFile } from "node:fs/promises";
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
  const powerContext=await browser.newContext({viewport:{width:1366,height:900}});
  page=await powerContext.newPage();
  await page.goto(base+'/cabinet-power?scope=B');
  await page.getByRole('button',{name:'原始平面图',exact:true}).click();
  await page.locator('.room-sidebar button').filter({hasText:'302 包间'}).click();
  await page.locator('.map-cell').filter({hasText:/^B04$/}).click();
  await page.locator('.rack-power').getByText('16000 W',{exact:true}).waitFor();
  await page.getByLabel('修改机柜功率',{exact:true}).click();
  const powerDialog=page.getByRole('dialog',{name:'修改机柜功率',exact:true});
  await powerDialog.getByLabel('机柜功率（W）',{exact:true}).fill('4000');
  await page.screenshot({path:path.join(output,'rack-power-edit.png')});
  await powerDialog.getByRole('button',{name:'保存',exact:true}).click();
  await powerDialog.waitFor({state:'hidden'});
  await page.locator('.rack-power').getByText('4000 W',{exact:true}).waitFor();
  await page.screenshot({path:path.join(output,'rack-power-history.png')});
  const corrected=await (await page.request.get(base+'/api/cabinet-power/operations?scope=B&room=302&rack=B04')).json();
  assert(corrected.data.items.filter(op=>!op.meta?.baseline_correction).every(op=>op.power===4000));
  await powerContext.close();
  const exportContext = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  await exportContext.addInitScript(() => {
    Object.defineProperty(Crypto.prototype, "randomUUID", { value: undefined, configurable: true });
  });
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
  await page.getByText(/^导出时间：20\d{2}-\d{2}-\d{2}/).waitFor();
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
    body.data.export_state = { has_export:true,is_stale:true,stale_reason:"机柜台账已变化" };
    await route.fulfill({ response, json:body });
  });
  await page.route("**/api/cabinet-power/export-history**", async route => {
    const response = await route.fetch(); const body = await response.json();
    body.data.items = (body.data.items || []).map(item => ({ ...item,is_stale:true,stale_reason:"机柜台账已变化" }));
    await route.fulfill({ response, json:body });
  });
  await page.reload();
  await page.getByText("最近导出已过期：机柜台账已变化。请重新导出。", { exact:true }).waitFor();
  await page.getByRole("button", { name: "导出历史" }).click();
  await page.getByText("已过期", { exact:true }).first().waitFor();
  await page.getByText("机柜台账已变化", { exact:true }).first().waitFor();
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
  await page.getByText("本地已清理", { exact: true }).waitFor();
  await page.unroute("**/api/cabinet-power/overview**");
  await page.unroute("**/api/cabinet-power/export-history**");
  assert.deepEqual(exportErrors, []);
  await exportContext.close();

  const batchContext = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  page = await batchContext.newPage();
  const batchErrors = [];
  page.on("pageerror", error => batchErrors.push(error.message));
  await page.goto(base + "/cabinet-power?scope=B");
  await page.getByRole("heading", {name:"B楼机柜上下电",exact:true}).waitFor();
  for(const [room,left,right] of [["216","B05","B10"],["247","B10","B05"]]){
    await page.getByRole("button",{name:"包间汇总",exact:true}).click();
    const summaryRow=page.locator(".mobile-card-table tbody tr").filter({hasText:`${room} 包间`});
    await summaryRow.getByRole("button",{name:"查看平面图",exact:true}).click();
    const leftRack=page.locator(`.map-canvas button[aria-label^='${left} ']`);
    const rightRack=page.locator(`.map-canvas button[aria-label^='${right} ']`);
    await leftRack.waitFor();await rightRack.waitFor();
    assert((await leftRack.boundingBox()).x<(await rightRack.boundingBox()).x,`B-${room} direction must follow its own drawing`);
    assert.equal(await page.locator(".map-canvas").getByRole("button",{name:/^[A-Z]\d{2} /}).count(),10);
    await page.screenshot({path:path.join(output,`b-${room}-floorplan.png`),fullPage:true});
    await page.locator(".map-canvas button[aria-label^='B06 ']").click();
    await page.getByRole("heading",{name:`${room} / B06`,exact:true}).waitFor();
    assert(await page.getByRole("button",{name:"登记上正式电",exact:true}).isEnabled());
    await page.keyboard.press("Escape");
  }
  await page.goto(base + "/cabinet-power?scope=A");
  await page.getByRole("heading", { name:"A楼机柜上下电",exact:true }).waitFor();
  for (const room of ["203","303","403"]) {
    await page.getByRole("button", {name:"包间汇总",exact:true}).click();
    const row = page.locator(".mobile-card-table tbody tr").filter({hasText:`${room} 包间`});
    await row.getByRole("button", {name:"查看平面图",exact:true}).click();
    await page.locator(".map-canvas").waitFor();
    await page.getByRole("button",{name:"A01 未上电/已下电",exact:true}).waitFor();
    assert.equal(await page.locator(".map-canvas button[aria-label$=' 未上电/已下电']").count(),28);
    await page.screenshot({path:path.join(output,`a-${room}-floorplan.png`),fullPage:true});
  }
  await page.goto(base + "/cabinet-power");
  await page.getByRole("heading", { name:"机柜上下电",exact:true }).waitFor();
  await page.getByRole("button", { name:"批量登记",exact:true }).click();
  await page.waitForURL(url => url.searchParams.get("origin") === "cabinet");
  await page.getByRole("button", { name:"返回",exact:true }).click();
  await page.waitForURL(url => url.pathname === "/cabinet-power" && !url.searchParams.get("scope"));
  await page.goto(base + "/cabinet-power?scope=E");
  await page.getByRole("heading", { name:"E楼机柜上下电" }).waitFor();
  for (const name of ["新增记录","登记机柜操作","多维表","归档表"])
    assert.equal(await page.getByRole("button", { name,exact:true }).count() + await page.getByRole("link", { name,exact:true }).count(),0,`${name} must not be shown`);
  await page.goto(base + "/cabinet-power/batches?scope=E&status=todo");
  await page.getByRole("heading", { name:"上下电待办" }).waitFor();
  await page.getByRole("button", { name:"批量登记",exact:true }).click();
  await page.waitForURL(url => url.searchParams.get("mode") === "new");
  assert.equal(new URL(page.url()).searchParams.get("status"),"todo","batch creation must retain the todo filter");
  await page.getByRole("button", { name:"返回",exact:true }).click();
  await page.waitForURL(url => url.pathname === "/cabinet-power/batches" && !url.searchParams.get("mode"));
  assert.equal(new URL(page.url()).searchParams.get("scope"),"E");
  assert.equal(new URL(page.url()).searchParams.get("status"),"todo","return must restore the todo filter");
  const beforeText=(await (await page.request.get(base+"/api/cabinet-power/batches?scope=E")).json()).data.total;
  await page.goto(base+"/cabinet-power/batches?scope=E&mode=new&status=todo");
  await page.getByRole("button",{name:"粘贴文本识别",exact:true}).click();
  const minimumText="EA118-E2-2\tA11\t测试电转正式电\t2026-09-16 16:02:59\t2026-09-14 16:03:16";
  const fullText="机房\t机房系统名称\t包间\t包间系统名称\t机架\t操作类型\t期望完成时间\t实际完成时间\t运营商机柜编号\t结果\nEA118\t南通综保区基地A\tE2-2.EA118\t"+minimumText+"\tA11\tSuccess";
  async function pasteText(text){
    await page.getByRole("textbox",{name:"粘贴机柜确认文本",exact:true}).evaluate((node,value)=>{
      const data=new DataTransfer();data.setData("text/plain",value);node.dispatchEvent(new ClipboardEvent("paste",{clipboardData:data,bubbles:true,cancelable:true}));
    },text);
  }
  await pasteText("EA118-E2-2\tA11\t测试电转正式电\t\t2026-09-14 16:03:16");
  await page.locator(".text-entry>[role=alert]").filter({hasText:"缺少期望完成时间"}).waitFor();
  assert.equal(await page.locator(".text-preview tbody tr").count(),0,"incomplete paste must not enter preview");
  assert.equal(await page.getByLabel("粘贴机柜确认文本",{exact:true}).inputValue(),"");
  for(const [index,text] of [minimumText,fullText].entries()){
    await pasteText(text);
    await page.locator(".text-preview tbody tr").nth(index).waitFor();
  }
  assert.equal(await page.getByLabel("粘贴机柜确认文本",{exact:true}).inputValue(),"");
  assert.equal(await page.locator(".text-entry table").count(),1,"pasted rows must not be rendered twice");
  await page.getByLabel("筛选粘贴记录",{exact:true}).selectOption({index:2});
  await page.getByRole("button",{name:"移除所选粘贴记录",exact:true}).click();
  await page.getByRole("dialog").getByRole("button",{name:"确认移除",exact:true}).click();
  assert.equal(await page.locator(".text-preview tbody tr").count(),1,"removing a paste removes its records");
  await pasteText(fullText);
  await page.locator(".text-preview tbody tr").nth(1).waitFor();
  assert.equal((await (await page.request.get(base+"/api/cabinet-power/batches?scope=E")).json()).data.total,beforeText,"preview must not create batches");
  assert.equal(await page.getByLabel("文本记录结果",{exact:true}).first().inputValue(),"");
  assert.equal(await page.getByLabel("文本记录结果",{exact:true}).nth(1).inputValue(),"成功");
  assert.equal(await page.getByLabel("文本记录期望完成时间",{exact:true}).first().inputValue(),"2026-09-16T16:02:59");
  assert.equal(await page.getByLabel("文本记录实际完成时间",{exact:true}).first().inputValue(),"2026-09-14T16:03:16");
  await page.getByText("本次粘贴中重复",{exact:true}).waitFor();
  const review=page.getByLabel("筛选核对状态",{exact:true});
  await review.selectOption('duplicates');
  await page.getByLabel('筛选粘贴记录',{exact:true}).selectOption({index:1});
  assert.equal(await review.locator('option[value="duplicates"]').innerText(),'重复（0）');
  assert.equal(await review.locator('option[value=""]').innerText(),'全部记录（1）');
  assert.equal(await page.locator('.text-preview tbody tr').count(),0);
  await page.getByText('当前筛选范围没有重复记录',{exact:true}).waitFor();
  await page.getByRole('button',{name:'查看全部重复（1）',exact:true}).click();
  await page.getByText('当前 1 条 / 全部 2 条',{exact:true}).waitFor();
  await page.getByLabel('筛选粘贴记录',{exact:true}).selectOption({index:2});
  assert.equal(await review.locator('option[value="duplicates"]').innerText(),'重复（1）');
  assert.equal(await page.locator('.text-preview tbody tr').count(),1);
  await page.getByLabel('搜索粘贴记录',{exact:true}).fill('B99');
  assert.equal(await review.locator('option[value="duplicates"]').innerText(),'重复（0）');
  assert.equal(await review.locator('option[value="issues"]').innerText(),'待核对（0）');
  await page.getByRole('button',{name:'查看全部重复（1）',exact:true}).click();
  assert.equal(await page.getByLabel('搜索粘贴记录',{exact:true}).inputValue(),'');
  await review.selectOption('');
  await page.screenshot({path:path.join(output,"text-paste-preview.png"),fullPage:true});
  await page.reload();
  await page.locator(".text-preview tbody tr").nth(1).waitFor();
  let textResponseLost=false;const textRequests=[];
  await page.route("**/api/cabinet-power/batches",async route=>{
    if(route.request().method()!=="POST"||route.request().postDataJSON().source!=="text")return route.continue();
    textRequests.push(route.request().postDataJSON().request_id);
    const response=await route.fetch();
    if(!textResponseLost){textResponseLost=true;return route.abort("connectionreset");}
    return route.fulfill({response});
  });
  await page.getByRole("button",{name:/^识别并创建待办/}).click();
  await page.locator(".batch-page>.notice.danger").waitFor();
  await page.getByRole("button",{name:/^识别并创建待办/}).click();
  await page.waitForURL(url=>Boolean(url.searchParams.get("batch_id")));
  await page.getByRole("heading",{name:"上下电待办详情",exact:true}).waitFor();
  assert.equal(textRequests.length,2);assert.equal(textRequests[0],textRequests[1]);
  const textBatch=(await (await page.request.get(base+"/api/cabinet-power/batches/"+new URL(page.url()).searchParams.get("batch_id"))).json()).data;
  assert.equal(textBatch.source,"text");assert.equal(textBatch.rows.length,2);assert.equal(textBatch.text_sources.length,2);
  assert.equal((await (await page.request.get(base+"/api/cabinet-power/batches?scope=E")).json()).data.total,beforeText+1);
  await page.unroute("**/api/cabinet-power/batches");
  const densityContext=await browser.newContext({viewport:{width:1366,height:900}});
  const density=await densityContext.newPage();
  const densityErrors=[];density.on('pageerror',error=>densityErrors.push(error.message));
  await density.goto(base+"/cabinet-power/batches?scope=E&mode=new");
  await density.getByRole("button",{name:"粘贴文本识别",exact:true}).click();
  for(let index=0;index<100;index++){
    const text=Array.from({length:5},(_,row)=>`EA118-E2-2\tA${String(row+1).padStart(2,'0')}\t测试电转正式电\t2026-09-16 16:02:59\t2026-09-14 16:03:16\tSuccess`).join('\n');
    await density.getByLabel("粘贴机柜确认文本",{exact:true}).evaluate((node,value)=>{
      const data=new DataTransfer();data.setData("text/plain",value);node.dispatchEvent(new ClipboardEvent("paste",{clipboardData:data,bubbles:true,cancelable:true}));
    },text);
    await density.getByRole('status').filter({hasText:`${index+1} 次粘贴 · ${(index+1)*5} 条记录`}).waitFor();
  }
  assert.equal(await density.locator('.text-preview tbody tr').count(),25);
  assert.equal(await density.locator('.text-entry table').count(),1);
  assert.equal(await density.getByLabel('筛选粘贴记录',{exact:true}).locator('option').count(),101);
  for(const viewport of [{width:1366,height:900},{width:1280,height:720}]){
    await density.setViewportSize(viewport);
    const tableHeight=await density.locator('.text-preview').evaluate(node=>({client:node.clientHeight,scroll:node.scrollHeight}));
    assert(tableHeight.scroll<=tableHeight.client+2,'all 25 rows must expand without an inner vertical scrollbar');
    const tableBounds=await density.locator('.text-preview').boundingBox();
    const lastRowBounds=await density.locator('.text-preview tbody tr').last().boundingBox();
    assert(lastRowBounds.y+lastRowBounds.height<=tableBounds.y+tableBounds.height+2,'last row must be inside the expanded table');
    const bounds=await density.locator('.text-footer').boundingBox();
    assert(bounds.y+bounds.height<=viewport.height+2,'creation controls must remain visible');
    assert(await density.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'page must not overflow horizontally');
    await density.screenshot({path:path.join(output,`text-500-${viewport.width}.png`),fullPage:true});
  }
  await density.locator('.text-preview tbody tr').last().evaluate(node=>node.scrollIntoView({block:'center'}));
  const expandedLastRow=await density.locator('.text-preview tbody tr').last().boundingBox();
  const expandedFooter=await density.locator('.text-footer').boundingBox();
  assert(expandedLastRow.y+expandedLastRow.height<=expandedFooter.y+2,'page scrolling must reveal the last row above the footer');
  await density.getByRole('navigation',{name:'文本记录分页'}).getByRole('button',{name:'1',exact:true}).click();
  await density.getByLabel('搜索粘贴记录',{exact:true}).fill('A03');
  await density.getByText('当前 100 条 / 全部 500 条',{exact:true}).waitFor();
  await density.getByLabel('筛选粘贴记录',{exact:true}).selectOption({index:100});
  await density.getByText('当前 1 条 / 全部 500 条',{exact:true}).waitFor();
  await density.getByRole('button',{name:'移除所选粘贴记录',exact:true}).click();
  await density.getByRole('dialog').getByRole('button',{name:'取消',exact:true}).click();
  assert.equal(await density.getByLabel('筛选粘贴记录',{exact:true}).locator('option').count(),101);
  await density.getByRole('button',{name:'移除所选粘贴记录',exact:true}).click();
  await density.getByRole('dialog').getByRole('button',{name:'确认移除',exact:true}).click();
  await density.getByText('当前 99 条 / 全部 495 条',{exact:true}).waitFor();
  await density.getByLabel('筛选核对状态',{exact:true}).selectOption('duplicates');
  await density.getByText('当前 98 条 / 全部 495 条',{exact:true}).waitFor();
  await density.getByRole('button',{name:/^识别并创建待办/}).click();
  await density.waitForURL(url=>Boolean(url.searchParams.get('batch_id')));
  const densityBatch=(await (await density.request.get(base+'/api/cabinet-power/batches/'+new URL(density.url()).searchParams.get('batch_id'))).json()).data;
  assert.equal(densityBatch.rows.length,495,'create must include all pages, not just filtered rows');
  assert.equal(densityBatch.text_sources.length,99);
  assert.deepEqual(densityErrors,[]);
  await densityContext.close();
  const rows = Array.from({ length: 60 }, (_, index) => ({
    row_id:`row-${index + 1}`, scope:"E", room:"202", rack:`B${String(index + 1).padStart(2,"0")}`,
    rack_type:"服务器机柜", action:"上正式电", expected:"", actual:"",
    result:"成功", status:"ready", issues:[], edits:[], evidence_images:[], editable:true,
    confirmable:true, rollbackable:false, restorable:false,
  }));
  const images = Array.from({ length: 25 }, (_, index) => ({
    image_id:`image-${index + 1}`, name:`确认截图-${index + 1}.png`, extension:".png",
    status:"done", suggestions:[], error:"",
  }));
  rows[0].evidence_images=["image-1","image-2","image-3"];
  rows[0].proof_files=[{file_id:"pdf-1",name:"机柜确认单.pdf",available:true,can_download:true}];
  let batch = {
    batch_id:"batch-test", owner_id:"owner", source:"notice", status:"pending", version:1,
    created_at:"2026-09-19 10:00:00", scopes:["E"], allowed_scopes:["E"], rows, images,
    stats:{ total:60,new:60,confirmable:60,duplicate:0,conflict:0,invalid:0,completed:0,failed:0,rolled_back:0 },
    source_notice:{ notice_type:"上电通告",title:"E楼机柜上电通告",scope:"E",start_time:"2026-09-19 09:00:00",end_time:"2026-09-19 18:00:00",sent_at:"2026-09-19 09:01:00",ended_at:"",cabinet:"E-202包间B01至B60" },
    notice_counts:{declared:60,unique:60,directory_matched:60}, can_download_files:true,can_confirm_all:true,
  };
  let rejectPatch = true;
  const pixel = process.argv[2] ? await readFile(process.argv[2]) : Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=", "base64");
  await page.route("**/api/cabinet-power/batches/batch-test**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname.includes("/images/")) return route.fulfill({ status:200,contentType:"image/png",body:pixel });
    if (route.request().method() === "PATCH") {
      if(rejectPatch === "conflict" || rejectPatch === "overlap") {
        const overlap=rejectPatch === "overlap";rejectPatch=false;
        batch={...batch,version:batch.version+1};
        batch.rows[0].supplier_rack="REMOTE-RACK";
        if(overlap)batch.rows[0].type_detail="其他用户已保存";
        return route.fulfill({status:409,json:{error:"批次已被其他操作更新，请重新载入"}});
      }
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
  assert.equal(await page.getByText("不计入通告汇总", { exact:true }).count(),0);
  assert.equal(await page.getByRole("columnheader", { name:"证明",exact:true }).count(),1);
  assert.equal(await page.locator(".records-table tbody .proof-cell").first().locator(".row-thumb").count(),2);
  assert.equal(await page.locator(".records-table tbody .proof-cell").first().getByRole("link",{name:"机柜确认单.pdf"}).count(),1);
  await page.getByRole("button",{name:"查看全部 3 张证明",exact:true}).click();
  await page.getByRole("dialog",{name:"确认截图原图"}).waitFor();
  assert(await page.getByText("3 / 3",{exact:true}).isVisible());
  await page.getByRole("button",{name:"上一张证明",exact:true}).click();
  assert(await page.getByText("2 / 3",{exact:true}).isVisible());
  await page.getByRole("button",{name:"关闭原图",exact:true}).click();
  assert.equal(await page.getByRole("columnheader", { name:"期望完成",exact:true }).count(),0);
  assert.equal(await page.locator(".source-details").first().getAttribute("open"),null);
  assert(await page.locator(".batch-summary").evaluate(node => node.getBoundingClientRect().height < 100));
  await page.locator(".source-details > summary").first().click();
  assert(await page.getByText("开始通告实际发送", { exact:true }).isVisible());
  await page.locator(".source-details > summary").first().click();
  await page.screenshot({ path:path.join(output,"batch-detail-1366.png"),fullPage:true });
  await page.getByRole("button", { name:"编辑记录",exact:true }).first().click();
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").fill("2026-09-19T12:34:56");
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByLabel("实际完成时间").press("Tab");
  await page.getByText("自动保存失败，请重试", { exact:false }).first().waitFor({ timeout:10000 });
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
  await page.getByText("自动保存失败，请重试", { exact:false }).first().waitFor({ timeout:10000 });
  await page.getByRole("dialog", { name:/编辑 E楼 202 B01/ }).getByRole("button", { name:"关闭",exact:true }).click();
  await page.getByRole("button", { name:"返回待办",exact:true }).click();
  await page.getByRole("dialog", { name:"放弃未保存的批次修改？" }).waitFor();
  await page.getByRole("button", { name:"放弃修改",exact:true }).click();
  await page.goto(base + "/cabinet-power/batches?scope=E&batch_id=batch-test");
  await page.getByRole("heading", { name:"上下电待办详情" }).waitFor();
  assert.equal(await page.getByRole("dialog", { name:"恢复未保存的批次更正？" }).count(),0,"discarded draft must not return");
  batch.source_notice.deleted_at = "2026-09-21 18:00:00";
  batch.stats.confirmable = 0;
  batch.rows.forEach(row => { row.editable = false; row.confirmable = false; });
  await page.reload();
  await page.locator(".summary-title").getByText("来源通告已删除", {exact:true}).waitFor();
  assert(await page.getByRole("button", {name:"确认整批",exact:true}).isDisabled());
  assert.equal(await page.getByText(/条上下电通告联动失败/).count(),0);
  await page.screenshot({path:path.join(output,"batch-deleted-1024.png"),fullPage:true});
  batch.source_notice.deleted_at="";batch.rows.forEach(row=>row.editable=true);rejectPatch="conflict";
  await page.reload();
  await page.getByRole("button",{name:"编辑记录",exact:true}).first().click();
  let dialog=page.getByRole("dialog",{name:/编辑 E楼 202 B01/});
  await dialog.getByLabel("类型明细",{exact:true}).fill("本次更正");
  await dialog.getByLabel("类型明细",{exact:true}).press("Tab");
  await page.getByText("更正已自动保存",{exact:true}).waitFor();
  assert.equal(batch.rows[0].supplier_rack,"REMOTE-RACK");
  assert.equal(batch.rows[0].type_detail,"本次更正");
  rejectPatch="overlap";
  await dialog.getByLabel("类型明细",{exact:true}).fill("冲突后保留本次");
  await dialog.getByLabel("类型明细",{exact:true}).press("Tab");
  await dialog.getByText("请选择保留的内容",{exact:true}).waitFor();
  assert.equal(await dialog.getByLabel("类型明细",{exact:true}).inputValue(),"冲突后保留本次");
  await page.screenshot({path:path.join(output,"batch-save-conflict.png")});
  await dialog.getByRole("button",{name:"保留本次",exact:true}).click();
  await page.getByText("更正已自动保存",{exact:true}).waitFor();
  await dialog.getByRole("button",{name:"核对原图",exact:true}).click();
  await page.locator(".proof-panel img").first().waitFor();
  for(const width of [1366,1024,760]){
    await page.setViewportSize({width,height:900});
    assert(await dialog.evaluate(node=>node.scrollWidth<=node.clientWidth+2),"editor must not overflow");
    await page.screenshot({path:path.join(output,`batch-proof-editor-${width}.png`)});
  }
  await dialog.getByRole("button",{name:"关闭",exact:true}).click();
  batch={...batch,version:batch.version+1,source:"image",rows:[],stats:{total:0},images:images.slice(0,2).map((image,index)=>({...image,status:"recognizing",phase:index?"queued":"running"}))};
  await page.setViewportSize({width:1366,height:900});await page.reload();
  await page.getByText("图片识别中 · 已完成 0/2 张",{exact:true}).waitFor();
  assert.equal(await page.getByRole("button",{name:"删除空批次",exact:true}).count(),0);
  await page.screenshot({path:path.join(output,"image-recognition-progress.png")});
  batch.images[0].status="done";batch.version++;
  await page.getByText("图片识别中 · 已完成 1/2 张",{exact:true}).waitFor({timeout:10000});
  batch.images[1].status="failed";batch.images[1].error="截图识别失败";batch.version++;
  await page.getByRole("button",{name:"删除空批次",exact:true}).waitFor({timeout:10000});
  await page.getByRole("button",{name:"补全机柜",exact:true}).first().click();
  await page.getByRole("form",{name:"补全截图机柜"}).waitFor();
  await page.screenshot({path:path.join(output,"image-correction.png"),fullPage:true});
  assert.deepEqual(batchErrors, []);
  await batchContext.close();
  const listContext=await browser.newContext({viewport:{width:1366,height:900}});
  await listContext.addInitScript(()=>{
    const original=window.fetch.bind(window);
    window.__listAborts=0;
    window.fetch=(input,options={})=>{
      if(new URL(String(input),location.href).pathname==='/api/cabinet-power/batches'){
        options.signal?.addEventListener('abort',()=>window.__listAborts++);
        // Deliver late responses even after cancellation to exercise the sequence guard.
        return original(input,{...options,signal:undefined});
      }
      return original(input,options);
    };
  });
  page=await listContext.newPage();
  const listErrors=[];
  page.on('pageerror',error=>listErrors.push(error.message));
  let held;
  const listResponse=(scope,currentPage)=>({ok:true,data:{items:[{batch_id:`list-${scope}`,title:`${scope}-latest`,scopes:[scope],status:'pending'}],total:40,page:currentPage,page_size:20}});
  await page.route('**/api/cabinet-power/batches?*',async route=>{
    const query=new URL(route.request().url()).searchParams;
    if(held&&query.get('scope')===(held.scope||'A')){held.resolve(route);return;}
    await route.fulfill({json:listResponse(query.get('scope'),Number(query.get('page')||1))});
  });
  await page.goto(base+'/cabinet-power/batches?scope=E&status=todo');
  await page.getByText('E-latest',{exact:true}).waitFor();
  for(const lateError of [false,true]){
    held=Promise.withResolvers();
    await page.locator('.filters select').first().selectOption('A');
    await page.getByRole('button',{name:'查询',exact:true}).click();
    const oldRoute=await held.promise;
    await page.locator('.filters select').first().selectOption('B');
    await page.getByLabel('开始日期',{exact:true}).fill('2026-09-01');
    await page.getByRole('button',{name:'查询',exact:true}).click();
    await page.getByText('B-latest',{exact:true}).waitFor();
    const oldResponse=page.waitForResponse(response=>response.url()===oldRoute.request().url());
    await oldRoute.fulfill(lateError?{status:500,json:{ok:false,error:'stale list failure'}}:{json:listResponse('A',1)});
    await (await oldResponse).finished();
    await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
    assert.equal(await page.locator('.filters select').first().inputValue(),'B');
    assert(await page.getByText('B-latest',{exact:true}).isVisible());
    assert.equal(await page.locator('.notice.danger').count(),0);
    assert(await page.getByRole('button',{name:'刷新',exact:true}).isEnabled());
  }
  held=Promise.withResolvers();
  await page.locator('.filters select').first().selectOption('A');
  await page.getByRole('button',{name:'查询',exact:true}).click();
  const staleRoute=await held.promise;
  held={...Promise.withResolvers(),scope:'B'};
  await page.locator('.filters select').first().selectOption('B');
  await page.getByRole('button',{name:'查询',exact:true}).click();
  const currentRoute=await held.promise;
  const staleResponse=page.waitForResponse(response=>response.url()===staleRoute.request().url());
  await staleRoute.fulfill({status:500,json:{ok:false,error:'stale error while loading'}});
  await (await staleResponse).finished();
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  assert(await page.getByRole('button',{name:'刷新',exact:true}).isDisabled());
  assert.equal(await page.locator('.notice.danger').count(),0);
  const currentResponse=page.waitForResponse(response=>response.url()===currentRoute.request().url());
  await currentRoute.fulfill({json:listResponse('B',1)});
  await (await currentResponse).finished();
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  assert(await page.getByRole('button',{name:'刷新',exact:true}).isEnabled());
  held=undefined;
  await page.locator('.pagination').getByRole('button',{name:'2',exact:true}).click();
  await page.locator('.pagination button.active').filter({hasText:'2'}).waitFor();
  await page.screenshot({path:path.join(output,'batch-list-latest-query.png'),fullPage:true});
  held=Promise.withResolvers();
  await page.locator('.filters select').first().selectOption('A');
  await page.getByRole('button',{name:'查询',exact:true}).click();
  const abandoned=await held.promise;
  await page.getByRole('button',{name:'批量登记',exact:true}).click();
  await page.getByRole('heading',{name:'机柜批量登记',exact:true}).waitFor();
  await abandoned.fulfill({status:500,json:{ok:false,error:'abandoned list failure'}});
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  assert.equal(await page.locator('.notice.danger').count(),0);
  assert(await page.evaluate(()=>window.__listAborts>=3));
  assert.deepEqual(listErrors,[]);
  await listContext.close();
  console.log("cabinet saves, export response recovery, five-building upload and layout passed");
} catch (error) {
  if (page && !page.isClosed()) await page.screenshot({ path: path.join(output, "failure.png") });
  console.error(log.slice(-2500));
  throw error;
} finally {
  await browser?.close();
  if (server.exitCode === null) { const exited = once(server, "exit"); server.kill(); await exited; }
}
