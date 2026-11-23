import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = await mkdtemp(path.join(os.tmpdir(), 'workbench-role-layout-'));
console.log('Isolated role/layout checks: '+output);
// Load the same styles as the application, including the shell and current theme.
const main = await readFile(path.join(root, 'src/workspace-v2/main.ts'), 'utf8');
const styles = [...main.matchAll(/import '([^']+\.css)';/g)]
  .map(([, name]) => `import '${path.posix.normalize('/src/workspace-v2/' + name)}';`).join('\n');
const fixture = `<template><NConfigProvider><NDialogProvider><NMessageProvider>
  <div class="v2-shell"><main class="v2-main">
    <WorkbenchHome :user="user" :scope="scope" :scopes="scopes" @open="url=>opened(url)" />
  </main></div></NMessageProvider></NDialogProvider></NConfigProvider></template>
  <script setup>
  import {ref,nextTick} from 'vue';
  import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
  import WorkbenchHome from '/src/workspace-v2/workbench/WorkbenchHome.vue';
  import {applyNightMode} from '/src/workspace-v2/theme';
  ${styles}
  document.documentElement.classList.add('workspace-v2');
  const user=ref({}),scope=ref('A'),scopes=ref([]);
  window.setAccount=async account=>{user.value=account;scopes.value=(account.testScopes||['A']).map(value=>({value,label:value+'楼'}));scope.value='A';await nextTick();};
  window.setScope=async value=>{scope.value=value;await nextTick();};
  window.setNightMode=async value=>{applyNightMode(value);await nextTick();};
  const opened=url=>{window.opened=url;};
  </script>`;
const entry = `import {createApp} from 'vue';import Fixture from '/@role-layout.vue';createApp(Fixture).mount('#app');`;
const server = await createServer({root, base:'/', configFile:path.join(root,'vite.workspace-v2.config.ts'), logLevel:'error',
  cacheDir:path.join(output, 'vite'), optimizeDeps:{entries:[], include:['vue','naive-ui','lucide-vue-next']},
  server:{host:'127.0.0.1',port:0}, plugins:[{
    name:'isolated-role-layout', enforce:'pre',
    resolveId(id) { if (id.startsWith('/@role-layout.')) return id; },
    load(id) { if (id==='/@role-layout.vue') return fixture; if (id==='/@role-layout.js') return entry; },
  }],
});
await server.listen();
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
let browser;
try {
  browser = await chromium.launch({headless:true});
  const page = await browser.newPage({viewport:{width:1800,height:1100},reducedMotion:'reduce'});
  await page.clock.setFixedTime(new Date('2026-11-18T03:00:00Z'));
  const requests=[], unexpected=[], errors=[], checks=[];
  page.on('pageerror', error=>errors.push(error.stack||error.message));
  let empty=false, failure=0, partial=false, permissionVersion=1, repairSize=1, riskSize=1, noticeSize=1;
  let truncatedRepairPage=false,liveGuard=false;
  await page.route('**/*', async route=>{
    const request=route.request(), url=new URL(request.url()), scope=url.searchParams.get('scope')||'A';
    if (url.origin!==origin) { unexpected.push('external '+url.origin); return route.abort(); }
    if (url.pathname==='/fixture') return route.fulfill({contentType:'text/html',body:
      '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="page-back-slot"></div><div id="app"></div><script type="module" src="/@role-layout.js"></script>'});
    if (!url.pathname.startsWith('/api/')) return route.continue();
    requests.push([request.method(),url.pathname]);
    if (request.method()!=='GET') {unexpected.push(request.method()+' '+url.pathname); return route.abort();}
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/repair-management/health')return ok({clean:true});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(url.pathname==='/api/repair-management/sync-status')return ok({status:'completed'});
    if (url.pathname==='/api/repair-management/records' || url.pathname.startsWith('/api/repair-management/records/repair-'+scope)) {
      const record={record_id:url.searchParams.get('focus_record_id')||url.pathname.split('/records/')[1]||'repair-'+scope,title:'隔离维修待办',building:scope,workflow:'维修中',
        fields:{'维修名称':'隔离维修待办','所属数据中心/楼栋-使用':scope+'楼'},raw_fields:{}};
      return ok(url.pathname.endsWith('/records')?{fields:[],records:[record],total:1}:{record});
    }
    if(url.pathname==='/api/repair-management/followups')return ok({fields:[],records:[],total:0,shared_fields:{}});
    if (url.pathname==='/api/assistant/icon-weather') return ok({available:false});
    if (url.pathname==='/api/personal-calendar/weather') return ok({warning_ready:true,alerts:[],guard_actions:[]});
    if (url.pathname==='/api/personal-calendar') return ok({month:url.searchParams.get('month'),days:{},roster_dates:['2026-11-18'],unavailable:[]});
    if (url.pathname.startsWith('/api/workspace-layouts/')) return ok({revision:1,value:{version:1,widgets:[
      {kind:'shortcuts',links:['accounts','learning','event','learning','cabinet']},
    ]}});
    if (url.pathname==='/api/events/monthly') {
      if (failure) return route.fulfill({status:failure,json:{ok:false,error:'isolated failure'}});
      return ok({scope,month:'2026-11',snapshot_exists:true,records:empty?[]:[{
        id:'event-'+scope,title:'隔离处理中事件',status:'处理中',occurrence_time:'2026-11-18 10:00',level:'I2',
      }]});
    }
    if (url.pathname==='/api/events/overview') return ok({scope,records:[],stats:{},items:[]});
    if (url.pathname==='/api/repair-management/status') {
      const limit=Number(url.searchParams.get('limit')), offset=Number(url.searchParams.get('offset')||0);
      assert.ok(limit>0);
      const total=empty?0:partial?2*limit+1:repairSize;
      const records=Array.from({length:Math.max(0,Math.min(limit,total-offset))},(_,i)=>{
        // A non-final API page must be full. Completed records occupy slots but are not todos.
        const completed=partial && (i>0 || offset===2*limit);
        return {record_id:'repair-'+scope+(offset+i?'-'+(offset+i):''),
          title:offset===limit&&i===0?'隔离维修下一页':'隔离维修待办'+(i?' '+(i+1):''),
          building:scope,specialty:'电气',workflow:completed?'已完成':'维修中',is_completed:completed};
      });
      return ok({total,records:truncatedRepairPage?records.slice(0,1):records});
    }
    if (url.pathname==='/api/workbench') return ok({ongoing:empty?[]:Array.from({length:noticeSize},(_,i)=>
      noticeSize===1?{active_item_id:'change-live',work_type:'change',title:'隔离变更只读项',scope,status:'开始'}
      :{active_item_id:'notice-live-'+i,source_record_id:i?'source-'+i:'maintenance-plan',work_type:'maintenance',title:i?'隔离进行中通告 '+i:'隔离维保计划',scope,status:'开始'}),
      records:empty?[]:Array.from({length:noticeSize},(_,i)=>({record_id:i?'plan-'+i:'maintenance-plan',work_type:'maintenance',title:'隔离维保计划'+(i?' '+i:''),source_progress:'未开始'})),
      ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
    if (url.pathname==='/api/workbench-events') return ok({items:empty?[]:[{event_id:'notice-live',state:'active',metadata:{summary:'隔离事件通告'}}]});
    if (url.pathname==='/api/risk-inspections') return ok({total:empty?0:riskSize,items:empty?[]:Array.from({length:riskSize},(_,i)=>({id:'risk-'+i,name:'隔离专项排查'+(i?' '+(i+1):''),total:2,approved:1,submitted:1,status:'active'}))});
    if (url.pathname==='/api/critical-guard/tasks') return ok({tasks:[...(liveGuard?[{task_id:'guard-active',title:'现场重保',complete:false,status:'published'}]:[]),{task_id:'guard-done',title:'隔离已完成重保',complete:true,status:'published'}]});
    if (url.pathname==='/api/cabinet-power/batches') return ok({total:1,items:[{batch_id:'cab-done',title:'隔离已完成机柜',is_todo:false}]});
    if (url.pathname==='/api/drills') return ok({items:[{drill_id:'drill-done',name:'隔离已完成演练',status:'published',execution:{status:'synced'}}]});
    if (url.pathname==='/api/engineer/mop/bootstrap') return ok({notices:[]});
    if (url.pathname==='/api/capacity/water/records') return ok({total:0,records:[]});
    if (url.pathname==='/api/pre-shift-checks') return ok({scope,scopes:['A'],can_manage:false,roster_ready:false,server_time:'2026-11-18T11:00:00+08:00',definition:{items:[],version:1},tasks:[]});
    if (url.pathname==='/api/learning/bootstrap') return ok({self_scope:'A',self_person:{id:'self'},can_answer:true,today:'2026-11-18'});
    if (url.pathname==='/api/learning/papers') return ok({total:0,page_size:20,items:[]});
    if (url.pathname==='/api/learning/profile') return ok({today_summary:{assigned:15,task_answered:4},summary:{assigned:30,task_answered:9}});
    if (url.pathname==='/api/learning/workbench-summary') return ok({scopes:scope==='ALL'?['A','B']:[scope],summary:{已完成:4,待答:2,答题中:1,逾期:0,待核对:0,未到时:0}});
    if (url.pathname==='/api/signatures/management/account') return ok({person:{is_self:true,building:'A楼',signature_status:'signed'}});
    if (url.pathname==='/api/auth/permission-requests/admin') return ok({items:[{request_id:'permission',name:'隔离权限申请 '+permissionVersion,status:'pending'}]});
    if (url.pathname==='/api/tool-self-check/workspace/history') return ok({sessions:[]});
    if (url.pathname==='/api/cold-source-monitor') return ok({items:[{scope:'A',mode:'机械制冷',date:'2026-11-18',temperature:16}]});
    if (url.pathname==='/api/daily-tasks') return ok({scope,date:'2026-11-18',stats:{total:5,completed:3,ongoing:1,attention:1}});
    unexpected.push(url.pathname); return route.fulfill({status:404,json:{ok:false,error:'No isolated fixture'}});
  });
  await page.goto(origin+'/fixture',{waitUntil:'domcontentloaded',timeout:60_000});
  await page.waitForFunction(()=>!!window.setAccount,undefined,{timeout:60_000});
  const full=['event','repair_management','learning','maintenance','change','repair','power','risk_inspection','critical_guard','cabinet','drills','engineer_mop','daily','water','accounts','signatures'];
  const actor=(id,category,modules=full,extra={})=>({open_id:id,category,modules,role:'building',home_scope:'A',enabled:true,
    ...(category==='engineer'?{specialties:['电气']}:{}),...extra});
  const settle=()=>page.waitForFunction(()=>{
    const home=document.querySelector('.wb-home');
    return home&&!home.querySelector('.wb-todo-panel[aria-busy="true"]')&&!/正在读取|待加载/.test(home.textContent);
  });
  const set=async account=>{await page.evaluate(account=>window.setAccount(account),account);await settle();};
  const todo=page.getByRole('region',{name:'待办办理',exact:true});
  const queue=todo.getByRole('list',{name:'待办模块',exact:true});
  const tiles=queue.locator('.wb-task-tile');
  const tile=key=>queue.locator('.wb-task-tile[data-group="'+key+'"]');
  const dialog=page.getByRole('dialog',{name:'模块待办条目',exact:true});
  const rowsOf=id=>dialog.locator('.wb-task-row[data-source="'+id+'"]');
  const health=todo.locator('.queue-health');
  const more=todo.getByRole('button',{name:'读取更多待办',exact:true});
  async function open(key){
    await tile(key).waitFor();
    if(await tile(key).getAttribute('aria-expanded')!=='true')await tile(key).click();
    await dialog.waitFor();
  }
  async function close(){
    await dialog.getByRole('button',{name:'关闭待办条目'}).click();await dialog.waitFor({state:'hidden'});
  }
  async function counts(total,showAggregate=false){
    assert.equal(await tiles.evaluateAll(items=>items.reduce((n,item)=>n+Number(item.dataset.count),0)),total);
    const badge=todo.locator('.queue-title .wb-task-count');
    if(showAggregate)assert.equal(await badge.getAttribute('aria-label'),'已读取 '+total+' 项');
    else assert.equal(await badge.count(),0,'Unfiltered views can rely on the individual tile counts');
  }
  async function checkInlineRepair(scope){
    await open('repair_management');
    const before=page.url();
    const focused=request=>{
      const url=new URL(request.url());
      return url.pathname==='/api/repair-management/records' && url.searchParams.get('scope')===scope
        && url.searchParams.get('focus_record_id')==='repair-'+scope;
    };
    await Promise.all([
      page.waitForRequest(focused),
      dialog.getByRole('button',{name:'办理 隔离维修待办',exact:true}).click(),
    ]);
    const panel=page.getByRole('region',{name:'办理 隔离维修待办',exact:true});
    await panel.locator('[data-module-widget="repair_management"]').waitFor();
    assert.equal(page.url(),before,'opening an exact record keeps the current workbench');
    await panel.getByRole('navigation',{name:'维修办理内容'}).getByText(/跟进记录/).waitFor();
    await panel.getByRole('button',{name:'独立页面',exact:true}).click();
    await page.waitForURL(url=>url.searchParams.get('__page')==='/repair-management');
    const params=new URL(page.url()).searchParams;
    assert.equal(params.get('scope'),scope);
    assert.equal(params.get('record_id'),'repair-'+scope);
    assert.equal(params.get('workbench_return'),scope);
    await page.goBack();
    await panel.getByRole('button',{name:'关闭待办办理',exact:true}).click();
    await panel.waitFor({state:'detached'});
  }

  await set(actor('manager-single','manager'));
  assert.match(await page.locator('.wb-identity').innerText(),/园区经理/);
  assert.equal(await todo.getByText('隔离维保计划',{exact:true}).count(),0,'single-building manager must retain the manager notice boundary');
  assert.equal(await tile('notices:维保').count(),0);
  await open('notices:变更');
  const managerNotice=rowsOf('notices').filter({hasText:'隔离变更只读项'});
  const viewNotice=managerNotice.getByRole('button',{name:'查看 隔离变更只读项',exact:true});
  assert.equal(await managerNotice.getByRole('button').count(),1,'manager has only the read action on this notice');
  await viewNotice.click();
  const readOnlyDetail=page.getByRole('region',{name:'查看 隔离变更只读项',exact:true});
  await readOnlyDetail.getByText('开始时间',{exact:true}).waitFor();
  assert.deepEqual(await readOnlyDetail.getByRole('button').evaluateAll(buttons=>buttons.map(button=>button.getAttribute('aria-label')||button.textContent.trim())),['切换待办','关闭只读详情']);
  assert.equal(await page.locator('[data-module-widget]').count(),0,'viewing a notice must not mount a business editor');
  await readOnlyDetail.getByRole('button',{name:'关闭只读详情'}).click();
  checks.push('single-building manager classification, maintenance exclusion and read-only notice details');

  liveGuard=true;
  await set(actor('group-priority','manager'));
  assert.deepEqual((await tiles.evaluateAll(items=>items.map(item=>item.dataset.group))).slice(0,4),['events','critical_guard','notices:变更','risk_inspection']);
  const dutyStart=requests.length;
  await set(actor('shared-duty','duty'));
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await settle();
  assert.equal(await page.getByText(/画像|个人学练/).count(),0);
  assert.ok(!requests.slice(dutyStart).some(([,url])=>url.startsWith('/api/learning/')));
  assert.equal(await page.getByText('隔离已完成演练',{exact:true}).count(),0);
  assert.equal(await rowsOf('drills').count(),0);
  await open('notices:变更');
  assert.deepEqual((await tiles.evaluateAll(items=>items.map(item=>item.dataset.group))).slice(0,3),['events','critical_guard','notices:变更'],'one event tile leads the duty desk');
  assert.equal(await tile('event_notices').count(),0);
  liveGuard=false;
  assert.equal(await tile('notices:维保').count(),0,'plans alone do not create ongoing todos');
  await rowsOf('notices').getByRole('button',{name:'更新／结束 隔离变更只读项',exact:true}).waitFor();
  checks.push('shared duty hides learning including saved preferences and retains both notice actions in their own tiles');

  for (const [category,scopes,expected] of [
    ['manager',['A','B'],['事件管理','维修项目','画像学练','重保管理','专项排查']],
    ['supervisor',['A'],['事件管理','维修项目','机柜管理','画像学练','重保管理','专项排查']],
    ['engineer',['A'],['维修项目','专项排查','通告办理','事件管理']],
    ['engineer',['A','B'],['维修项目','专项排查','事件管理','通告办理']],
    ['duty',['A'],['通告办理','演练管理','维保单办理','水耗管理']],
    ['administrator',['A'],['账号与权限','签名管理']],
  ]) {
    await set(actor('layout-'+category+'-'+scopes.length,category,full,{testScopes:scopes,can_manage_accounts:category==='administrator'}));
    const labels=await page.locator('.wb-shortcut').allTextContents();
    assert.deepEqual(labels.map(label=>label.trim()).sort(),expected.sort());
    assert.equal(new Set(labels).size,labels.length);
    assert.equal(await page.locator('.wb-source-card').count(),0,'one business overview replaces separate source cards');
    assert.equal(await queue.count(),1);
    assert.equal(await todo.getByLabel('业务分类').count(),0);
    for (const width of [1800,1440,1024,800,390]) {
      await page.setViewportSize({width,height:1100}); await settle();
      const geometry=await page.evaluate(()=>{
        const rect=element=>{const r=element.getBoundingClientRect();return {top:r.top,bottom:r.bottom,left:r.left,right:r.right,height:r.height,width:r.width};};
        return {overflow:document.documentElement.scrollWidth>innerWidth,
          controls:[...document.querySelectorAll('.wb-shortcut,.wb-task-tile,.queue-filters input')].map(rect),
          emptyCheck:document.querySelector('.wb-check-panel .v2-check-task > .n-empty')?rect(document.querySelector('.wb-check-panel')):null,
          rows:[...document.querySelectorAll('.wb-task-tile')].map(rect)};
      });
      assert.equal(geometry.overflow,false,category+' overflow at '+width);
      assert.ok(geometry.controls.every(rect=>rect.height<=96),category+' controls must stay compact at '+width);
      if (geometry.emptyCheck) assert.ok(geometry.emptyCheck.height<=140,category+' empty pre-shift check at '+width+': '+geometry.emptyCheck.height+'px');
      const boxes=geometry.rows;
      assert.ok(boxes.length,category+' has tasks to measure');
      assert.equal(new Set(boxes.map(box=>Math.round(box.left))).size>1,boxes.length>1,'available tiles share a row');
      assert.ok(boxes.every((box,i)=>boxes.slice(i+1).every(next=>box.right<=next.left+1||box.bottom<=next.top+1)),'module tiles never overlap');
      if ([1800,1440,390].includes(width)) await page.screenshot({path:path.join(output,category+'-'+scopes.length+'-'+width+'.png'),fullPage:true,animations:'disabled'});
    }
  }
  checks.push('six role/scope combinations at five widths: one tile grid, no overlaps or horizontal overflow, compact controls');

  await set(actor('engineer-restricted','engineer',['learning','cabinet','accounts']));
  assert.equal(await page.locator('.wb-shortcut').count(),0,'job recommendations cannot create module grants');
  assert.equal(await todo.count(),0);
  await set(actor('engineer-action','engineer',['repair_management','risk_inspection']));
  await checkInlineRepair('A');
  checks.push('recommendations require real module grants; exact-record action and return scope preserved');

  repairSize=12; riskSize=7;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle();
  await counts(19);
  const localReads=requests.length;
  await open('risk_inspection');assert.equal(await rowsOf('risk_inspection').count(),7);
  await open('repair_management');assert.equal(await rowsOf('repair_management').count(),12);
  const titles=await dialog.locator('.task-title strong').allTextContents();
  assert.deepEqual(titles.map(s=>s.length),titles.map(s=>s.length).sort((a,b)=>a-b),'short titles lead equally important rows');
  assert.equal(requests.length,localReads,'opening loaded groups makes no requests');
  const lastTitle='隔离维修待办 12';
  const selectedRequest=page.waitForRequest(req=>new URL(req.url()).pathname==='/api/repair-management/records' && new URL(req.url()).searchParams.get('focus_record_id')==='repair-A-11');
  await dialog.getByRole('button',{name:'办理 '+lastTitle,exact:true}).click();await selectedRequest;
  const lastCase=page.getByRole('region',{name:'办理 '+lastTitle,exact:true});
  await lastCase.locator('[data-module-widget=repair_management]').waitFor();
  await lastCase.getByRole('navigation',{name:'维修办理内容'}).getByText(/跟进记录/).waitFor();
  await lastCase.getByRole('button',{name:'关闭待办办理',exact:true}).click();await settle();
  await todo.getByLabel('搜索待办').fill('维修 7');await counts(1,true);
  await open('repair_management');assert.equal(await rowsOf('repair_management').count(),1);await close();
  await todo.getByLabel('搜索待办').fill('');await counts(19);
  assert.equal(await more.count(),0);
  await open('repair_management');
  repairSize=11;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));await settle();
  assert.equal(await rowsOf('repair_management').count(),11);
  assert.equal(await tile('repair_management').getAttribute('data-count'),'11','refresh updates both open group and tile');
  assert.equal(await dialog.getByRole('button',{name:'办理 '+lastTitle,exact:true}).count(),0);
  repairSize=12;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));await settle();
  assert.equal(await rowsOf('repair_management').count(),12);
  for (const width of [1440,1800,390]){
    await page.setViewportSize({width,height:1000});await settle();
    const rect=await dialog.boundingBox();assert(rect.x>=0 && rect.x+rect.width<=width+1);
    await page.screenshot({path:path.join(output,'dense-modules-'+width+'.png'),animations:'disabled'});
  }
  await set(actor('multi-scope-counts','engineer',['repair_management','risk_inspection'],{testScopes:['A','B']}));
  await counts(19);await open('repair_management');
  await page.evaluate(()=>window.setScope('B')); await settle(); await counts(19);
  assert.equal(await dialog.count(),0,'changing scope closes the previous group');
  await checkInlineRepair('B');
  checks.push('all loaded rows accessible beyond eight, short titles first, search, refresh, no overflow, and scope reset');

  noticeSize=7;
  const beforeDuty=requests.length;
  await set(actor('combined-notices','duty'));await counts(21,true);
  const noticeReads=requests.length;
  await open('events');await rowsOf('events').getByRole('button',{name:'办理 隔离处理中事件'}).waitFor();
  assert.equal(await tile('events').getAttribute('data-count'),'2');
  await rowsOf('event_notices').getByRole('button',{name:'办理 隔离事件通告'}).waitFor();
  await open('notices:维保');assert.equal(await rowsOf('notices').count(),7);
  assert.equal(await rowsOf('notices').getByText('隔离维保计划',{exact:true}).count(),1,'linked plan and active notice appear once');
  await close();
  assert.equal(requests.length,noticeReads);
  assert.equal(await more.count(),0);
  assert.ok(!requests.slice(beforeDuty).some(([,url])=>url.startsWith('/api/learning/')));
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle(); await counts(21,true);
  await todo.getByLabel('搜索待办').fill('事件通告'); await counts(1,true);
  await open('events');assert.equal(await rowsOf('event_notices').count(),1);
  checks.push('both event sources appear under one tile, linked notices deduplicate, duty grants and module keyword search preserved');
  noticeSize=1; repairSize=1; partial=true;
  await set(actor('mixed-pagination','engineer',['repair_management','risk_inspection']));
  await counts(8);
  const repairPages=requests.filter(([,url])=>url==='/api/repair-management/status').length;
  const riskPages=requests.filter(([,url])=>url==='/api/risk-inspections').length;
  await open('repair_management');
  await dialog.getByRole('button',{name:'继续读取'}).click();
  await dialog.getByRole('button',{name:'办理 隔离维修下一页',exact:true}).waitFor();await settle();
  await counts(9);
  assert.ok(requests.filter(([,url])=>url==='/api/repair-management/status').length>repairPages);
  assert.equal(requests.filter(([,url])=>url==='/api/risk-inspections').length,riskPages,'another group makes no extra API reads');
  await dialog.getByRole('button',{name:'继续读取'}).click();
  await dialog.getByRole('button',{name:'继续读取'}).waitFor({state:'detached'});
  assert.equal(await more.count(),0);
  checks.push('open-group remote pagination and globally discoverable remaining pages');
  riskSize=1;
  truncatedRepairPage=true;
  await set(actor('truncated-repair','engineer',['repair_management']));
  assert.equal(await todo.locator('.queue-title .wb-task-count').count(),0,'a truncated non-final page must not appear as zero');
  assert.equal(await rowsOf('repair_management').count(),0,'unverified rows from an incomplete response must not become todos');
  await health.locator('summary').click();
  truncatedRepairPage=false;
  await health.getByRole('button',{name:'重试待跟进维修',exact:true}).click();
  await open('repair_management');await dialog.getByText('隔离维修待办',{exact:true}).waitFor();
  partial=false; empty=true;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await todo.getByRole('status').filter({hasText:'暂无待办'}).waitFor();
  assert.equal(await queue.count(),0);
  checks.push('truncated pages cannot create tasks and are retryable; empty queue is compact');

  empty=false; failure=500;
  await set(actor('failed-source','manager',['event']));
  await health.locator('summary').click();
  failure=0; await health.getByRole('button',{name:'重试处理中事件',exact:true}).click();
  await open('events');await dialog.getByText('隔离处理中事件',{exact:true}).waitFor();
  failure=403;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await tile('events').waitFor({state:'detached'});
  assert.equal(await health.count(),0,'revoked source no longer offers retry');
  assert.equal(await todo.getByText('隔离处理中事件',{exact:true}).count(),0);
  checks.push('failed sources remain visible with retry; permission denial clears stale rows and removes the details action');
  failure=0;

  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:true}));
  await open('accounts');await dialog.getByRole('button',{name:'办理 隔离权限申请 1',exact:true}).waitFor();
  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:false}));
  assert.equal(await page.getByText('隔离权限申请 1',{exact:true}).count(),0);
  permissionVersion=2;
  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:true}));
  await open('accounts');await dialog.getByRole('button',{name:'办理 隔离权限申请 2',exact:true}).waitFor();
  checks.push('account-management grant changes reset and reload the same-account workbench');

  await page.setViewportSize({width:1440,height:1100});
  await set(actor('theme-check','supervisor'));
  const urgent=page.getByRole('region',{name:'紧急事项',exact:true});
  await urgent.getByRole('button',{name:'I2事件，办理 隔离处理中事件',exact:true}).click();
  const eventPanel=page.getByRole('region',{name:'办理 隔离处理中事件',exact:true});
  await eventPanel.locator('[data-module-widget="event"]').waitFor();
  await eventPanel.getByRole('button',{name:'关闭待办办理',exact:true}).click();
  await eventPanel.waitFor({state:'hidden'});
  checks.push('I2 events remain prominent and open the matching record');
  const surface=todo;
  const light=await surface.evaluate(element=>getComputedStyle(element).backgroundColor);
  await page.evaluate(()=>window.setNightMode(true));
  const dark=await surface.evaluate(element=>getComputedStyle(element).backgroundColor);
  assert.notEqual(light,dark,'workbench follows existing dark theme');
  await page.screenshot({path:path.join(output,'dark-1440.png'),fullPage:true,animations:'disabled'});
  checks.push('existing light/dark theme surface is inherited');
  assert.ok(requests.every(([method])=>method==='GET'));
  assert.deepEqual(unexpected,[]); assert.deepEqual(errors,[]);
  await writeFile(path.join(output,'results.json'),JSON.stringify({checks,apiReads:requests.length,unexpected,errors},null,2));
  console.log(checks.join('\n'));
  console.log('PASS: isolated GET-only fixtures; no real service or message requests. Screenshots and results: '+output);
} finally {await browser?.close(); await server.close();}
