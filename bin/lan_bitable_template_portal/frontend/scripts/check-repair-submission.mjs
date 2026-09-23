import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.resolve(root, "../../../output/playwright/repair-submission");
await mkdir(output, { recursive: true });
let writes = 0, operationRecoveries = 0, status = "processing", unavailable = false, browser;
const writeOperations = [];
const projectFields = {"故障发生时间":"2026-09-18 09:27", "故障维修原因":"过滤器堵塞", "故障发生现象描述":"压差过大",
  "所属专业":"暖通", "所属数据中心/楼栋-使用":"南通A楼", "关联事件单":"recEvent", "设备检修关联":"recTarget", "检修通告名称":"测试检修"};
const project = {record_id:"recSaved",title:"测试维修单",source_event_id:"recEvent",source_repair_ids:["recTarget"],
  raw_fields:projectFields,display_fields:projectFields,record_version:"v1",building_codes:["A"],followup_count:0};
const projectMeta = Object.keys(projectFields).map(field_name => ({field_name,field_id:field_name,field_type:1,ui_type:"Text",editable:!['关联事件单','设备检修关联','检修通告名称'].includes(field_name)}));
const followupMeta = [{field_name:'维修进展描述',field_id:'progress',field_type:1,ui_type:'Text',editable:true}];
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
 const submit=async()=>{try{const update=mode==='update';await task.submit(update?'/api/repair-management/records/recSaved':'/api/test-submit',{method:update?'PUT':'POST',body:JSON.stringify({operation_id:'stable-id',fields:{text:'test'}})})}catch{}};
 return ()=>mode.startsWith('project')?h(Projects,{scope:'A',focusRecordId:mode==='project-update'?'recSaved':'',scopeOptions:[{value:'A',label:'A楼'}]}):mode==='followup'?h(Followups,{scope:'A',summaryRecordId:'recProject',summaryTitle:'隔离维修跟进',embedded:false}):h('main',{style:'max-width:1000px;margin:40px auto'},[
 h('h2','维修提交恢复检查'),h('button',{disabled:!!task.pending.value&&!(task.overwrite.value&&task.status.value==='failed'),onClick:submit},'保存记录'),
 task.pending.value&&task.status.value!=='failed'&&h(Status,{text:task.message.value,checking:task.checking.value,onCheck:task.check}),
 task.pending.value&&task.status.value==='failed'&&h('p','保存失败，请重新保存。'),
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
        let body = "";
        req.on("data", chunk => { body += chunk; });
        req.on("end", () => {
          writeOperations.push(JSON.parse(body).operation_id);
          res.end('{"data":{"record_id":"recSaved","fields":{}}}');
        });
        return;
      }
      if (req.url === '/api/repair-management/followups' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', () => {
          writeOperations.push(JSON.parse(body).operation_id);
          res.end(JSON.stringify({data:{record_id:'recNewFollowup',fields:{'维修进展描述':'恢复后的跟进填写'}}}));
        });
        return;
      }
      if (req.url.startsWith('/api/repair-management/followups') && req.method === 'GET') {
        res.end(JSON.stringify({data:{records:[],fields:followupMeta,total:0,shared_fields:{}}})); return;
      }
      if (req.url.startsWith('/api/repair-management/records') && req.method === 'GET') {
        res.end(JSON.stringify({data:{record:project,records:[project],fields:projectMeta,total:1}})); return;
      }
      if (req.url.includes("/operations/")) {
        if (req.method === 'POST') operationRecoveries++;
        if (unavailable) { res.statusCode = 503; res.end('{"ok":false,"error":"核验暂时不可用"}'); return; }
        res.end(JSON.stringify({ data: { status, retryable: status === "failed", error: status === "failed" ? "保存未完成，可修改后重新保存。" : "", result: status === "completed" ? { record_id: "recSaved", fields: {} } : null } })); return;
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
  await page.getByRole("button", { name: "刷新状态", exact: true }).click();
  await page.getByRole("button", { name: "刷新状态", exact: true }).waitFor();
  assert.equal(writes, 1);
  unavailable = false; status = "completed";
  await page.getByRole("button", { name: "刷新状态", exact: true }).click();
  await page.getByText("已核验 recSaved", { exact: true }).waitFor();
  assert(await page.getByRole("button", { name: "保存记录", exact: true }).isEnabled());
  assert.equal(writes, 1);
  assert.equal(operationRecoveries, 0);
  status = "failed";
  await page.evaluate(() => sessionStorage.setItem("repair-submission:test:A", JSON.stringify({
    id: "repair-update", body: JSON.stringify({ operation_id: "repair-update", source_repair_ids: ["recTarget"] }),
    path: "/api/repair-management/records/recSaved", method: "PUT",
  })));
  await page.goto(base + '/__repair_check?mode=update');
  await page.getByText("保存失败，请重新保存。", { exact: true }).waitFor();
  assert(await page.getByRole('button',{name:'保存记录',exact:true}).isEnabled());
  assert.equal(await page.getByText('待核实',{exact:false}).count(),0);
  assert.equal(writes, 1);
  await page.getByRole('button',{name:'保存记录',exact:true}).click();
  await page.locator('.submission-status').waitFor({state:'hidden'});
  assert.equal(writes,2);
  await page.reload();
  assert.equal(writes, 2);
  status = "processing";
  await page.evaluate(() => {
    const pending = JSON.stringify({ id: "pending-record", body: "{}", path: "unused", method: "POST" });
    sessionStorage.setItem("repair-submission:project:A", pending);
  });
  for (const mode of ["project"]) {
    await page.goto(base + `/__repair_check?mode=${mode}`);
    await page.locator(".submission-status").first().waitFor();
    await page.screenshot({ path: path.join(output, `${mode}-pending.png`), fullPage: true });
    assert(await page.locator("body").evaluate(node => node.scrollWidth <= innerWidth + 2));
  }
  status = 'failed';
  await page.evaluate(() => sessionStorage.setItem('repair-submission:project:A',JSON.stringify({
    id:'interrupted-update',method:'PUT',path:'/api/repair-management/records/recSaved',body:JSON.stringify({
      source_event_id:'recEvent',source_repair_ids:['recTarget'],fields:{'故障发生现象描述':'保存失败后保留的填写'}
    })
  })));
  await page.goto(base + '/__repair_check?mode=project-update');
  await page.getByRole('button',{name:'更改事件检修关联',exact:true}).click();
  await page.waitForFunction(() => [...document.querySelectorAll('textarea')].some(node => node.value === '保存失败后保留的填写'));
  const reselect = page.getByRole('button',{name:'重新选择',exact:true});
  assert.equal(await reselect.count(),2);
  assert(await reselect.nth(0).isEnabled());
  assert(await reselect.nth(1).isEnabled());
  assert(await page.getByRole('button',{name:'保存修改',exact:true}).isEnabled());
  assert.equal(await page.getByText('待核实',{exact:true}).count(),0);
  assert.equal(await page.evaluate(()=>sessionStorage.getItem('repair-submission:project:A')),null);
  await page.screenshot({path:path.join(output,'project-update-editable.png'),fullPage:true});
  await page.getByRole('button',{name:'保存修改',exact:true}).click();
  await page.locator('.submission-status').first().waitFor({state:'hidden'});
  await page.waitForFunction(() => document.body.textContent.includes('维修项目已保存'));
  assert.equal(writeOperations.length,2);
  assert.notEqual(writeOperations[1],'interrupted-update');
  assert.notEqual(writeOperations[1],writeOperations[0]);
  await page.goto(base + '/__repair_check?mode=followup');
  await page.getByLabel('维修进展描述',{exact:true}).fill('直接保存的跟进填写');
  assert.equal(await page.getByText('待核实',{exact:true}).count(),0);
  assert.equal(await page.getByText('处理详情',{exact:true}).count(),0);
  const followupSave = page.getByRole('button',{name:'新增跟进记录',exact:true}).last();
  assert(await followupSave.isEnabled());
  await followupSave.click();
  await page.waitForFunction(() => document.body.textContent.includes('维修跟进记录已新增'));
  assert.equal(writeOperations.length,3);
  assert.deepEqual(errors, []);
  console.log(`Repair project recovery and direct followup save passed; writes=${writes}.`);
} finally {
  await browser?.close();
  await vite.close();
}
