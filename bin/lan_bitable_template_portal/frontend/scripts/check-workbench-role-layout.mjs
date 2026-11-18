import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
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
  ${styles}
  document.documentElement.classList.add('workspace-v2');
  const user=ref({}),scope=ref('A'),scopes=ref([]);
  window.setAccount=async account=>{user.value=account;scopes.value=(account.testScopes||['A']).map(value=>({value,label:value+'楼'}));scope.value='A';await nextTick();};
  window.setScope=async value=>{scope.value=value;await nextTick();};
  const opened=url=>{window.opened=url;};
  </script>`;
const entry = `import {createApp} from 'vue';import Fixture from '/@role-layout.vue';createApp(Fixture).mount('#app');`;
const server = await createServer({root, configFile:false, logLevel:'error',
  cacheDir:path.join(output, 'vite'), optimizeDeps:{include:['vue','naive-ui','lucide-vue-next']},
  server:{host:'127.0.0.1',port:0}, plugins:[{
    name:'isolated-role-layout', enforce:'pre',
    resolveId(id) { if (id.startsWith('/@role-layout.')) return id; },
    load(id) { if (id==='/@role-layout.vue') return fixture; if (id==='/@role-layout.js') return entry; },
  }, vue()],
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
  await page.route('**/*', async route=>{
    const request=route.request(), url=new URL(request.url()), scope=url.searchParams.get('scope')||'A';
    if (url.origin!==origin) { unexpected.push('external '+url.origin); return route.abort(); }
    if (url.pathname==='/fixture') return route.fulfill({contentType:'text/html',body:
      '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/@role-layout.js"></script>'});
    if (!url.pathname.startsWith('/api/')) return route.continue();
    requests.push([request.method(),url.pathname]);
    if (request.method()!=='GET') {unexpected.push(request.method()+' '+url.pathname); return route.abort();}
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if (url.pathname.startsWith('/api/workspace-layouts/')) return ok({revision:1,value:{version:1,widgets:[
      {kind:'shortcuts',links:['accounts','learning','event','learning','cabinet']},
    ]}});
    if (url.pathname==='/api/events/monthly') {
      if (failure) return route.fulfill({status:failure,json:{ok:false,error:'isolated failure'}});
      return ok({scope,month:'2026-11',snapshot_exists:true,records:empty?[]:[{
        id:'event-'+scope,title:'隔离处理中事件',status:'处理中',occurrence_time:'2026-11-18 10:00',level:'I2',
      }]});
    }
    if (url.pathname==='/api/repair-management/status') {
      const next=Number(url.searchParams.get('offset')||0)>0;
      return ok({total:empty?0:partial?100:repairSize,records:empty?[]:Array.from({length:partial?1:repairSize},(_,i)=>({
        record_id:next?'next-page':'repair-'+scope+(i?'-'+i:''),title:next?'隔离维修下一页':'隔离维修待办'+(i?' '+(i+1):''),building:scope,workflow:'维修中',
      }))});
    }
    if (url.pathname==='/api/workbench') return ok({ongoing:empty?[]:Array.from({length:noticeSize},(_,i)=>
      noticeSize===1?{active_item_id:'change-live',work_type:'change',title:'隔离变更只读项',scope,status:'开始'}
      :{active_item_id:'notice-live-'+i,source_record_id:i?'source-'+i:'maintenance-plan',work_type:'maintenance',title:i?'隔离进行中通告 '+i:'隔离维保计划',scope,status:'开始'}),
      records:empty?[]:Array.from({length:noticeSize},(_,i)=>({record_id:i?'plan-'+i:'maintenance-plan',work_type:'maintenance',title:'隔离维保计划'+(i?' '+i:''),source_progress:'未开始'})),
      ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
    if (url.pathname==='/api/workbench-events') return ok({items:empty?[]:[{event_id:'notice-live',state:'active',metadata:{summary:'隔离事件通告'}}]});
    if (url.pathname==='/api/risk-inspections') return ok({total:empty?0:riskSize,items:empty?[]:Array.from({length:riskSize},(_,i)=>({id:'risk-'+i,name:'隔离专项排查'+(i?' '+(i+1):''),total:2,approved:1,submitted:1,status:'active'}))});
    if (url.pathname==='/api/critical-guard/tasks') return ok({tasks:[{task_id:'guard-done',title:'隔离已完成重保',complete:true,status:'published'}]});
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
  const actor=(id,category,modules=full,extra={})=>({open_id:id,category,modules,role:'building',home_scope:'A',enabled:true,...extra});
  const settle=()=>page.waitForFunction(()=>{
    const home=document.querySelector('.wb-home');
    return home&&!/正在读取|待加载/.test(home.textContent);
  });
  const set=async account=>{await page.evaluate(account=>window.setAccount(account),account);await settle();};
  const todo=page.getByRole('region',{name:/待办事项/});
  const overview=page.locator('.wb-focus');
  const focusCard=label=>overview.locator('.wb-focus-card').filter({has:page.getByText(label,{exact:true})});
  const taskModule=id=>todo.locator('.wb-task-module[data-source="'+id+'"]');
  const moduleRows=id=>taskModule(id).locator('.wb-task-row').count();
  const counts=async(shown,total)=>{
    assert.equal(await todo.locator('.wb-task-row').count(),shown);
    assert.match(await todo.locator('.wb-todo-toolbar .wb-task-count').innerText(),new RegExp('已显示 '+shown+' / '+total+' 项'));
  };

  await set(actor('manager-single','manager'));
  assert.match(await page.locator('.wb-identity').innerText(),/园区经理/);
  assert.equal(await todo.getByText('隔离维保计划',{exact:true}).count(),0,'single-building manager must retain the manager notice boundary');
  assert.equal(await todo.getByRole('button',{name:/隔离变更只读项/}).count(),0);
  assert.ok(await todo.locator('.wb-read-only').count()>0);
  await focusCard('通告信息').click();
  const drawer=page.locator('.n-drawer');
  await drawer.getByText('隔离变更只读项',{exact:true}).waitFor();
  assert.equal(await drawer.getByRole('button',{name:/办理|开始|更新|打开原模块/}).count(),0);
  await page.keyboard.press('Escape');
  checks.push('single-building manager classification, maintenance exclusion and read-only notice drawer');

  const dutyStart=requests.length;
  await set(actor('shared-duty','duty'));
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await settle();
  assert.equal(await page.getByText(/画像|个人学练/).count(),0);
  assert.ok(!requests.slice(dutyStart).some(([,url])=>url.startsWith('/api/learning/')));
  assert.equal(await page.getByText('隔离已完成演练',{exact:true}).count(),0);
  assert.equal(await focusCard('待完成演练').count(),0);
  assert.equal(await todo.getByRole('list',{name:'计划通告',exact:true}).count(),1);
  assert.equal(await todo.getByRole('list',{name:'进行中通告',exact:true}).count(),1);
  checks.push('shared duty hides learning including saved preferences; completed sources collapse; notice groups preserved');

  for (const [category,scopes,expected] of [
    ['manager',['A','B'],['事件管理','维修项目','画像学练','重保管理','专项排查']],
    ['supervisor',['A'],['事件管理','维修项目','机柜管理','画像学练','重保管理','专项排查']],
    ['engineer',['A'],['维修项目','专项排查']],
    ['engineer',['A','B'],['维修项目','专项排查','事件管理']],
    ['duty',['A'],['通告办理','演练管理','维护单管理','水耗管理']],
    ['administrator',['A'],['账号与权限','签名管理']],
  ]) {
    await set(actor('layout-'+category+'-'+scopes.length,category,full,{testScopes:scopes,can_manage_accounts:category==='administrator'}));
    const labels=await page.locator('.wb-shortcut').allTextContents();
    assert.deepEqual(labels.map(label=>label.trim()).sort(),expected.sort());
    assert.equal(new Set(labels).size,labels.length);
    assert.equal(await page.locator('.wb-source-card').count(),0,'one business overview replaces separate source cards');
    assert.equal(await overview.count(),1);
    const sourceLabels=await overview.locator('.wb-focus-card > span').allTextContents();
    assert.equal(new Set(sourceLabels).size,sourceLabels.length);
    for (const width of [1800,1440,1024,800,390]) {
      await page.setViewportSize({width,height:1100}); await settle();
      const geometry=await page.evaluate(()=>{
        const rect=element=>{const r=element.getBoundingClientRect();return {top:r.top,bottom:r.bottom,left:r.left,right:r.right,height:r.height,width:r.width};};
        return {overflow:document.documentElement.scrollWidth>innerWidth,
          controls:[...document.querySelectorAll('.wb-shortcut,.wb-task-row,.wb-task-filters select,.wb-task-filters input')].map(rect),
          emptyCheck:document.querySelector('.wb-check-panel .v2-check-task > .n-empty')?rect(document.querySelector('.wb-check-panel')):null,
          grids:[...document.querySelectorAll('.wb-module-grid')].map(grid=>({width:grid.getBoundingClientRect().width,
            cards:[...grid.querySelectorAll('.wb-task-module,.wb-period-card,.wb-cold-status,.wb-focus,.wb-check-panel,.wb-shortcut-panel')].map(rect),
            bars:[...grid.querySelectorAll('.wb-todo-toolbar,.wb-period-header,.wb-todo-panel > .wb-queue-status,.wb-todo-panel > .wb-list-footer')].map(rect)}))};
      });
      assert.equal(geometry.overflow,false,category+' overflow at '+width);
      assert.ok(geometry.controls.every(rect=>rect.height<=96),category+' controls must stay compact at '+width);
      if (geometry.emptyCheck) assert.ok(geometry.emptyCheck.height<=140,'empty pre-shift check stays compact and keeps its controls');
      for (const grid of geometry.grids) {
        const columns=new Set(grid.cards.map(card=>Math.round(card.left))).size;
        if ([1800,1440,390].includes(width)) assert.equal(columns,Math.min(grid.cards.length,width===1800?3:width===1440?2:1),category+' module columns at '+width);
        const boxes=[...grid.cards,...grid.bars];
        for (let i=0;i<boxes.length;i++) for (let j=i+1;j<boxes.length;j++) {
          const a=boxes[i],b=boxes[j];
          assert.ok(a.right<=b.left+1||b.right<=a.left+1||a.bottom<=b.top+1||b.bottom<=a.top+1,'module cards never overlap');
        }
        if (columns>1) assert.ok(grid.cards.every(card=>card.width<grid.width*.8),'business modules must not span full rows');
      }
      if ([1800,1440,390].includes(width)) await page.screenshot({path:path.join(output,category+'-'+scopes.length+'-'+width+'.png'),fullPage:true,animations:'disabled'});
    }
  }
  checks.push('six role/scope combinations: 1800 three-column, 1440 two-column, 390 single-column modules; 1024/800 overflow and compact controls; chart/cold/overview cards share the grid');

  await set(actor('engineer-restricted','engineer',['event','learning','cabinet','accounts']));
  assert.equal(await page.locator('.wb-shortcut').count(),0,'job recommendations cannot create module grants');
  assert.equal(await todo.count(),0);
  assert.equal(await overview.count(),0);
  await set(actor('engineer-action','engineer',['repair_management','risk_inspection']));
  await todo.getByRole('button',{name:'办理 隔离维修待办',exact:true}).click();
  assert.match(await page.evaluate(()=>window.opened),/record_id=repair-A.*workbench_return=A/);
  checks.push('recommendations require real module grants; exact-record action and return scope preserved');

  repairSize=7; riskSize=7;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle();
  await counts(8,14);
  assert.equal(await moduleRows('repair_management'),4); assert.equal(await moduleRows('risk_inspection'),4);
  const localReads=requests.length;
  await taskModule('repair_management').getByRole('button',{name:'显示更多待跟进维修',exact:true}).click();
  await counts(11,14);
  assert.equal(await moduleRows('risk_inspection'),4,'one module expansion leaves the other limit alone');
  assert.equal(requests.length,localReads,'expanding already-loaded rows makes no API calls');
  await todo.getByLabel('业务分类').selectOption('repair_management');
  await todo.getByLabel('搜索待办').fill('7');
  await counts(1,1);
  assert.equal(await todo.locator('.wb-task-module').count(),1);
  await todo.getByLabel('搜索待办').fill(''); await todo.getByLabel('业务分类').selectOption('');
  await counts(8,14);
  await todo.getByRole('button',{name:'只看逾期',exact:true}).click();
  assert.equal(await todo.locator('.wb-task-row').count(),0);
  await todo.getByRole('button',{name:'只看逾期',exact:true}).click(); await counts(8,14);
  await todo.getByRole('button',{name:'显示更多待办',exact:true}).click(); await counts(14,14);
  assert.equal(await todo.getByRole('button',{name:/显示更多/}).count(),0);
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle(); await counts(14,14);
  for (const width of [1440,1800,390]) {
    await page.setViewportSize({width,height:1100}); await settle();
    await page.screenshot({path:path.join(output,'dense-modules-'+width+'.png'),fullPage:true,animations:'disabled'});
  }
  await set(actor('multi-scope-counts','engineer',['repair_management','risk_inspection'],{testScopes:['A','B']}));
  await todo.getByRole('button',{name:'显示更多待办',exact:true}).click(); await counts(14,14);
  await page.evaluate(()=>window.setScope('B')); await settle(); await counts(8,14);
  await todo.getByRole('button',{name:'办理 隔离维修待办',exact:true}).click();
  assert.match(await page.evaluate(()=>window.opened),/scope=B&record_id=repair-B.*workbench_return=B/);
  checks.push('per-source visible counts, independent expansion, filtering/search/overdue reset, refresh persistence and scope reset match rendered rows');

  noticeSize=7;
  const beforeDuty=requests.length;
  await set(actor('combined-notices','duty'));
  await counts(4,14);
  assert.equal(await todo.locator('.wb-task-module').count(),1);
  assert.equal(await todo.getByRole('list',{name:'计划通告',exact:true}).count(),1);
  assert.equal(await todo.getByRole('list',{name:'进行中通告',exact:true}).count(),1);
  assert.equal(await todo.getByText('隔离维保计划',{exact:true}).count(),1,'linked plan and active notice stay deduplicated');
  const noticeReads=requests.length;
  for (let i=0;i<3;i++) await taskModule('notices').getByRole('button',{name:'显示更多通告待办',exact:true}).click();
  await counts(14,14);
  assert.equal(requests.length,noticeReads);
  assert.equal(await todo.getByRole('button',{name:/显示更多/}).count(),0);
  assert.ok(!requests.slice(beforeDuty).some(([,url])=>url.startsWith('/api/learning/')));
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle(); await counts(14,14);
  await todo.getByLabel('业务分类').selectOption('event_notices'); await counts(1,1);
  assert.equal(await todo.getByRole('list',{name:'计划通告',exact:true}).count(),0);
  checks.push('duty notice subgroups share a four-row initial budget, keep both states discoverable, deduplicate linked notices and expand without phantom pagination');
  noticeSize=1; repairSize=1; partial=true;
  await set(actor('mixed-pagination','engineer',['repair_management','risk_inspection']));
  await counts(5,8);
  const repairPages=requests.filter(([,url])=>url==='/api/repair-management/status').length;
  const riskPages=requests.filter(([,url])=>url==='/api/risk-inspections').length;
  await todo.getByRole('button',{name:'显示更多待办',exact:true}).click();
  await todo.getByRole('button',{name:'办理 隔离维修下一页',exact:true}).waitFor(); await settle();
  await counts(9,9);
  assert.ok(requests.filter(([,url])=>url==='/api/repair-management/status').length>repairPages);
  assert.equal(requests.filter(([,url])=>url==='/api/risk-inspections').length,riskPages,'local rows in another module do not trigger its API');
  assert.match(await taskModule('repair_management').locator('.wb-task-count').innerText(),/已读取/);
  checks.push('global display-more expands local rows and independently reads the paginated module; partial counts remain explicitly loaded counts');
  riskSize=1;
  partial=true;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle();
  assert.equal(await focusCard('待跟进维修').locator('strong').innerText(),'—','partial page is never a total');
  await focusCard('待跟进维修').click();
  const repairReads=requests.filter(([,url])=>url==='/api/repair-management/status').length;
  await drawer.getByRole('button',{name:'读取更多明细',exact:true}).click();
  await page.waitForFunction(()=>!document.querySelector('.n-drawer')?.textContent.includes('正在读取'));
  assert.ok(requests.filter(([,url])=>url==='/api/repair-management/status').length>repairReads);
  await page.keyboard.press('Escape');
  partial=false; empty=true;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle();
  assert.equal(await overview.count(),0,'empty/completed overview does not reserve rail space');
  assert.equal(await page.locator('.wb-task-module').count(),0);
  await todo.getByRole('status').getByText('当前没有未完成的待办。',{exact:true}).waitFor();
  checks.push('partial counts are unknown; empty/completed business overview and rail collapse');

  empty=false; failure=500;
  await set(actor('failed-source','manager',['event']));
  assert.equal(await focusCard('处理中事件').locator('strong').innerText(),'—');
  await focusCard('处理中事件').click();
  await drawer.getByRole('button',{name:'重试',exact:true}).waitFor();
  failure=0; await drawer.getByRole('button',{name:'重试',exact:true}).click();
  await drawer.getByText('隔离处理中事件',{exact:true}).waitFor();
  await page.keyboard.press('Escape');
  failure=403;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed'))); await settle();
  assert.equal(await focusCard('处理中事件').isDisabled(),true);
  assert.equal(await todo.getByText('隔离处理中事件',{exact:true}).count(),0);
  checks.push('failed sources remain visible with retry; permission denial clears stale rows and disables details');
  failure=0;

  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:true}));
  await todo.getByRole('button',{name:'办理 隔离权限申请 1',exact:true}).waitFor();
  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:false}));
  assert.equal(await page.getByText('隔离权限申请 1',{exact:true}).count(),0);
  permissionVersion=2;
  await set(actor('admin-permission','administrator',['accounts'],{can_manage_accounts:true}));
  await todo.getByRole('button',{name:'办理 隔离权限申请 2',exact:true}).waitFor();
  checks.push('account-management grant changes reset and reload the same-account workbench');

  await page.setViewportSize({width:1440,height:1100});
  await set(actor('theme-check','supervisor'));
  const light=await overview.evaluate(element=>getComputedStyle(element).backgroundColor);
  await page.evaluate(()=>document.documentElement.dataset.v2Theme='dark');
  const dark=await overview.evaluate(element=>getComputedStyle(element).backgroundColor);
  assert.notEqual(light,dark,'workbench follows existing dark theme');
  await page.screenshot({path:path.join(output,'dark-1440.png'),fullPage:true,animations:'disabled'});
  checks.push('existing light/dark theme surface is inherited');
  assert.ok(requests.every(([method])=>method==='GET'));
  assert.deepEqual(unexpected,[]); assert.deepEqual(errors,[]);
  await writeFile(path.join(output,'results.json'),JSON.stringify({checks,apiReads:requests.length,unexpected,errors},null,2));
  console.log(checks.join('\n'));
  console.log('PASS: isolated GET-only fixtures; no real service or message requests. Screenshots and results: '+output);
} finally {await browser?.close(); await server.close();}
