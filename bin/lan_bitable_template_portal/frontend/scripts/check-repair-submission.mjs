import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.resolve(root, "../../../output/playwright/repair-submission");
await mkdir(output, { recursive: true });
let writes = 0, status = "processing", unavailable = false, browser;
const entry = `
import {createApp,h,ref} from 'vue';
import {useRepairSubmission} from '/src/composables/useRepairSubmission.ts';
import Status from '/src/components/RepairSubmissionStatus.vue';
import Projects from '/src/components/RepairManagementPage.vue';
import Followups from '/src/components/RepairFollowupPanel.vue';
import '/src/global.css';
const mode=new URLSearchParams(location.search).get('mode');
createApp({setup(){
 const result=ref('');
 const task=useRepairSubmission(()=> 'test:A', r=> result.value='已核验 '+r.record_id);
 const submit=async()=>{try{await task.submit('/api/test-submit',{method:'POST',body:JSON.stringify({operation_id:'stable-id',fields:{text:'test'}})})}catch{}};
 return ()=>mode==='project'?h(Projects,{scope:'A',scopeOptions:[{value:'A',label:'A楼'}]}):mode==='followup'?h(Followups,{scope:'A',summaryRecordId:'recProject',summaryTitle:'隔离维修跟进',embedded:false}):h('main',{style:'max-width:1000px;margin:40px auto'},[
 h('h2','维修提交恢复检查'),h('button',{disabled:!!task.pending.value,onClick:submit},'保存记录'),
 task.pending.value&&h(Status,{text:task.message.value,detail:task.detail.value,checking:task.checking.value,failed:task.status.value==='failed',onCheck:task.check,onCopy:task.copyInput,onDismiss:task.dismissFailed}),
 h('p',result.value)]);
}}).mount('#app');`;
const vite = await createServer({ configFile: path.join(root, "vite.config.ts"), server: { host: "127.0.0.1", port: 0 }, plugins: [{
  name: "isolated-repair-check",
  resolveId(id) { if (id === "/__repair_check.js") return "\0repair-check"; },
  load(id) { if (id === "\0repair-check") return entry; },
  configureServer(server) {
    server.middlewares.use((req, res, next) => {
      if (req.url.startsWith("/__repair_check?")) {
        res.setHeader("Content-Type", "text/html");
        res.end('<html lang="zh-CN"><head><meta charset="utf-8"><style>*{box-sizing:border-box}body{font-family:"Microsoft YaHei",sans-serif;background:#eef3f8}</style></head><body><div id="app"></div><script type="module" src="/__repair_check.js"></script></body></html>'); return;
      }
      if (!req.url.startsWith("/api/")) return next();
      res.setHeader("Content-Type", "application/json");
      if (req.url === "/api/test-submit") { writes++; res.writeHead(200); res.write('{"data":'); setTimeout(() => res.destroy(), 30); return; }
      if (req.url === "/api/repair-management/records/recSaved" && req.method === "PUT") {
        writes++;
        res.end('{"data":{"record_id":"recSaved","fields":{}}}'); return;
      }
      if (req.url.includes("/operations/")) {
        if (unavailable) { res.statusCode = 503; res.end('{"ok":false,"error":"核验暂时不可用"}'); return; }
        res.end(JSON.stringify({ data: { status, retryable: status === "failed", error: status === "failed" ? "云端未保留本次修改" : "", result: status === "completed" ? { record_id: "recSaved", fields: {} } : null } })); return;
      }
      res.end(JSON.stringify({ data: { records: [], fields: [], total: 0, items: [], scope_options: [{value:"A",label:"A楼"}], sources: [], tasks: [], pending_count: 0 } }));
    });
  },
}] });
try {
  await vite.listen();
  const base = `http://127.0.0.1:${vite.httpServer.address().port}`;
  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1366, height: 900 } });
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto(base + "/__repair_check?mode=test");
  await page.getByRole("button", { name: "保存记录", exact: true }).click();
  await page.locator(".submission-status").waitFor();
  assert(await page.getByRole("button", { name: "保存记录", exact: true }).isDisabled());
  await page.reload();
  await page.locator(".submission-status").waitFor();
  assert(await page.getByRole("button", { name: "保存记录", exact: true }).isDisabled());
  unavailable = true;
  await page.getByRole("button", { name: "核验结果", exact: true }).click();
  await page.getByText("处理详情", { exact: true }).click();
  await page.getByText("核验暂时不可用", { exact: true }).waitFor();
  assert.equal(writes, 1);
  unavailable = false; status = "completed";
  await page.getByRole("button", { name: "核验结果", exact: true }).click();
  await page.getByText("已核验 recSaved", { exact: true }).waitFor();
  assert(await page.getByRole("button", { name: "保存记录", exact: true }).isEnabled());
  assert.equal(writes, 1);
  status = "failed";
  await page.evaluate(() => sessionStorage.setItem("repair-submission:test:A", JSON.stringify({
    id: "repair-update", body: JSON.stringify({ operation_id: "repair-update", source_repair_ids: ["recTarget"] }),
    path: "/api/repair-management/records/recSaved", method: "PUT",
  })));
  await page.reload();
  await page.getByText("已核验 recSaved", { exact: true }).waitFor();
  assert.equal(writes, 2);
  await page.reload();
  assert.equal(writes, 2);
  status = "processing";
  await page.evaluate(() => {
    const pending = JSON.stringify({ id: "pending-record", body: "{}", path: "unused", method: "POST" });
    sessionStorage.setItem("repair-submission:project:A", pending);
    sessionStorage.setItem("repair-submission:followup:A:recProject", pending);
  });
  for (const mode of ["project", "followup"]) {
    await page.goto(base + `/__repair_check?mode=${mode}`);
    await page.locator(".submission-status").first().waitFor();
    await page.screenshot({ path: path.join(output, `${mode}-pending.png`), fullPage: true });
    assert(await page.locator("body").evaluate(node => node.scrollWidth <= innerWidth + 2));
  }
  assert.deepEqual(errors, []);
  console.log(`Repair pending submission, response loss, single retry, reload recovery and desktop layouts passed; writes=${writes}.`);
} finally {
  await browser?.close();
  await vite.close();
}
