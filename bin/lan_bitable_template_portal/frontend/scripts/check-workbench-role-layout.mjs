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
    <div v-if="repairText" class="preview-fixture" style="width:min(100%,420px)"><RepairTextPreview :value="repairText" /></div>
  </main></div></NMessageProvider></NDialogProvider></NConfigProvider></template>
  <script setup>
  import {ref,nextTick} from 'vue';
  import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
  import WorkbenchHome from '/src/workspace-v2/workbench/WorkbenchHome.vue';
  import RepairTextPreview from '/src/workspace-v2/RepairTextPreview.vue';
  import {applyNightMode} from '/src/workspace-v2/theme';
  ${styles}
  document.documentElement.classList.add('workspace-v2');
  const user=ref({}),scope=ref('A'),scopes=ref([]),repairText=ref('');
  window.setAccount=async account=>{user.value=account;scopes.value=(account.testScopes||['A']).map(value=>({value,label:value+'楼'}));scope.value='A';await nextTick();};
  window.setScope=async value=>{scope.value=value;await nextTick();};
  window.setNightMode=async value=>{applyNightMode(value);await nextTick();};
  window.setRepairText=value=>{repairText.value=value;};
  const opened=url=>{window.opened=url;};
  </script>`;
const entry = `import {createApp} from 'vue';
  import LoadingIndicator from '/src/components/LoadingIndicator.vue';
  import UiTransition from '/src/components/UiTransition.vue';
  import Fixture from '/@role-layout.vue';
  createApp(Fixture).component('LoadingIndicator',LoadingIndicator).component('UiTransition',UiTransition).mount('#app');`;
const server = await createServer({root, base:'/', configFile:path.join(root,'vite.workspace-v2.config.ts'), logLevel:'error',
  cacheDir:path.join(output, 'vite'),
  optimizeDeps:{entries:[], include:['vue','naive-ui','lucide-vue-next','@tanstack/vue-virtual','gridstack']},
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
  let truncatedRepairPage=false,liveGuard=false,mixedProfessions=false;
  let checkFixture=false, checkSubmitted=false, calendarFailure=false, planFixture=false, inspectionOnDuty=false, ownRepair='';
  let personalAssigned=30, pendingLearning=false, learningSaved=false, failLearningSave=false, denseRoster=false;
  let fixtureAccount, delegatedCalendar=false, checkRosterChanged=false, answerFixture=false, answerSaved=false;
  const learningPaper=()=>({id:'desk-paper',person_id:'self',scope:'A',date:'2026-11-18',mode:'daily',
    status:answerSaved?'completed':'pending',version:answerSaved?2:1,stats:{answered:Number(answerSaved),total:1},
    questions:[{id:'desk-question',version:'1',stem:'隔离工作台答题',type:'single',bank:'written',options:[{id:'yes',text:'隔离选项'}],
      ...(answerSaved?{attempt:{correct:true,option_ids:['yes'],submitted_at:'2026-11-18T11:00:00+08:00'}}:{})}]});
  const checkItems=[{id:'valves',text:'核查机房阀门编号、位置和现场状态'},{id:'keys',text:'核对值班钥匙'}];
  await page.route('**/*', async route=>{
    const request=route.request(), url=new URL(request.url()), scope=url.searchParams.get('scope')||'A';
    if (url.origin!==origin) { unexpected.push('external '+url.origin); return route.abort(); }
    if (url.pathname==='/fixture') return route.fulfill({contentType:'text/html',body:
      '<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="page-back-slot"></div><div id="app"></div><script type="module" src="/@role-layout.js"></script>'});
    if (!url.pathname.startsWith('/api/')) return route.continue();
    requests.push([request.method(),url.pathname,scope]);
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(answerFixture&&url.pathname==='/api/learning/papers/desk-paper/start'&&request.method()==='POST'){
      assert.equal(request.postDataJSON().person_id,'self');
      return ok({started_at:'2026-11-18T11:00:00+08:00'});
    }
    if(answerFixture&&url.pathname==='/api/learning/papers/desk-paper/answer'&&request.method()==='POST'){
      const body=request.postDataJSON();
      assert.equal(body.person_id,'self');
      assert.equal(body.question_id,'desk-question');
      assert.deepEqual(body.option_ids,['yes']);
      answerSaved=true;
      return ok(learningPaper());
    }
    if(checkFixture&&url.pathname==='/api/pre-shift-checks/submit'&&request.method()==='POST'){
      const body=request.postDataJSON();
      assert.equal(body.task_id,'fixture-check');
      assert.equal(body.answers.length,checkItems.length);
      assert(body.answers.every(answer=>answer.status==='normal'));
      checkSubmitted=true;
      return ok({});
    }
    if(url.pathname==='/api/learning/issues/issue-review-fixture/question'&&url.searchParams.get('workbench')==='1'){
      const question={id:'review-question',type:'single',status:'published',version:4,stem:'隔离原题',
        options:[{id:'first',text:'原答案'},{id:'second',text:'待修改答案'}],correct_option_ids:['first'],analysis:'原解析'};
      if(request.method()==='GET')return ok({editable:true,question,issues_version:'fixture-v1',reason:'核对正确答案'});
      if(request.method()==='PUT'){
        const edits=request.postDataJSON();
        assert.equal(edits.issues_version,'fixture-v1');
        assert.equal(edits.stem,'隔离修订后的题干');
        assert.deepEqual(edits.correct_option_ids,['second']);
        if(failLearningSave)return route.fulfill({status:503,json:{ok:false,error:'隔离保存失败'}});
        learningSaved=true;
        return ok({question:{...question,...edits,version:5},resolved:true});
      }
    }
    if (request.method()!=='GET') {unexpected.push(request.method()+' '+url.pathname); return route.abort();}
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
    if (url.pathname==='/api/personal-calendar/weather') return calendarFailure?route.fulfill({status:503,json:{ok:false,error:'fixture unavailable'}}):
      ok({warning_ready:true,alerts:[],guard_level:'一级',guard_actions:[]});
    if (url.pathname==='/api/personal-calendar') return calendarFailure?route.fulfill({status:503,json:{ok:false,error:'fixture unavailable'}}):
      ok({month:url.searchParams.get('month'),days:{'2026-11-18':{items:delegatedCalendar?[{
        key:'delegated-plan',title:'隔离代登录维保日程',kind:'maintenance',scope,
      }]:[],staff:[]}},
        roster_dates:['2026-11-18'],unavailable:[],
        handover:{current:{date:'2026-11-18',shift:'白',end:'2026-11-18T17:50:00+08:00',verified:true,staff:denseRoster?['H','A','B','C','D','E','110'].map(building=>({scope:building,people:[{name:building+'人员甲'},{name:building+'人员乙'},{name:building+'人员丙'}]})):[{scope,people:[
          {name:scope+'楼排班测试',system_position:'值班员'},{name:'刘冉',system_position:'值班长'},{name:'薛小明',position:'运维实习生',duty_kind:'长白'},
        ]}]}}});
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
    if (url.pathname==='/api/daily-inspection/status') {
      const viewScope=url.searchParams.get('view_scope');
      return ok({view_scope:viewScope, viewer_on_duty:inspectionOnDuty, can_edit:true,
        duty:{scope:viewScope,date:'2026-11-18',shift:'白'},next_round:1,round_limit:2});
    }
    if (url.pathname==='/api/repair-management/status') {
      const limit=Number(url.searchParams.get('limit')), offset=Number(url.searchParams.get('offset')||0);
      assert.ok(limit>0);
      if (mixedProfessions) {
        const records=[['电气','强电'],['暖通','暖通'],['通用','']].map(([label,specialty],index)=>({
          record_id:'repair-'+scope+'-profession-'+index,title:scope+'楼'+label+'维修',building:scope,
          specialty,workflow:'维修中',is_completed:false,
        }));
        return ok({total:records.length,records:records.slice(offset,offset+limit)});
      }
      const total=empty?0:partial?2*limit+1:repairSize;
      const records=Array.from({length:Math.max(0,Math.min(limit,total-offset))},(_,i)=>{
        // A non-final API page must be full. Completed records occupy slots but are not todos.
        const completed=partial && (i>0 || offset===2*limit);
        return {record_id:'repair-'+scope+(offset+i?'-'+(offset+i):''),
          title:offset===limit&&i===0?'隔离维修下一页':'隔离维修待办'+(i?' '+(i+1):''),
          ...(ownRepair&&i===0?{assignee_open_id:ownRepair}:{}),
          building:scope,specialty:'电气',workflow:completed?'已完成':'维修中',is_completed:completed};
      });
      return ok({total,records:truncatedRepairPage?records.slice(0,1):records});
    }
    if (url.pathname==='/api/workbench') return ok({scope,ongoing:empty?[]:Array.from({length:noticeSize},(_,i)=>
      noticeSize===1?{active_item_id:'change-live',work_type:'change',title:'隔离变更只读项',scope,status:'开始'}
      :{active_item_id:'notice-live-'+i,source_record_id:i?'source-'+i:'maintenance-plan',work_type:'maintenance',title:i?'隔离进行中通告 '+i:'隔离维保计划',scope,status:'开始'}),
      records:empty?[]:Array.from({length:planFixture?23:noticeSize},(_,i)=>({record_id:i?'plan-'+i:'maintenance-plan',work_type:'maintenance',title:'隔离维保计划'+(i?' '+i:'')+(planFixture&&i===19?' · '+'长通告名称用于核对单行省略效果'.repeat(12):''),source_progress:'未开始',
        ...(planFixture?{building_codes:[scope],plan_start_time:'2026-11-18',plan_end_time:i===20?'2026-11-23':i===21?'2026-11-17':i===22?'':'2026-11-19'}:{})})),
      ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
    if (url.pathname==='/api/workbench-events') return ok({items:empty?[]:[{event_id:'notice-live',state:'active',metadata:{summary:'隔离事件通告'}}]});
    if (url.pathname==='/api/risk-inspections') return ok({total:empty?0:riskSize,items:empty?[]:Array.from({length:riskSize},(_,i)=>({id:'risk-'+i,name:'隔离专项排查'+(i?' '+(i+1):''),total:2,approved:1,submitted:1,status:'active'}))});
    if (url.pathname==='/api/critical-guard/tasks') return ok({tasks:[...(liveGuard?[{task_id:'guard-active',title:'现场重保',complete:false,status:'published'}]:[]),{task_id:'guard-done',title:'隔离已完成重保',complete:true,status:'published'}]});
    if (url.pathname==='/api/cabinet-power/batches') return ok({total:1,items:[{batch_id:'cab-done',title:'隔离已完成机柜',is_todo:false}]});
    if (url.pathname==='/api/drills') return ok({items:[{drill_id:'drill-done',name:'隔离已完成演练',status:'published',execution:{status:'synced'}}]});
    if (url.pathname==='/api/engineer/mop/bootstrap') return ok({notices:[]});
    if (url.pathname==='/api/capacity/water/records') return ok({total:0,records:[]});
    if (url.pathname==='/api/pre-shift-checks') return ok({scope,scopes:[scope],can_manage:false,roster_ready:checkFixture,
      viewer_on_roster:checkFixture&&!checkRosterChanged,viewer_active_shift:'白',server_time:'2026-11-18T11:00:00+08:00',
      definition:{items:checkFixture?checkItems:[],version:1},tasks:checkFixture?[{
        id:'fixture-check',scope,shift:'白',day:'2026-11-18',items:checkItems,definition_version:1,
        due:Date.parse('2026-11-18T08:50:00+08:00')/1000,deadline:Date.parse('2026-11-18T12:00:00+08:00')/1000,
        can_submit:!checkSubmitted,submitted:checkSubmitted?'2026-11-18T11:00:00+08:00':null,
        answers:checkItems.map(item=>({id:item.id,status:'normal',note:''})),files:[],
      }]:[]});
    if (url.pathname==='/api/learning/bootstrap') return ok({self_scope:'A',self_person:{id:'self',name:'隔离学员'},can_answer:true,today:'2026-11-18',
      ...(answerFixture?{can_view_buildings:false,daily_duty:{date:'2026-11-18',scope:'A',shift:'白',end_at:'2026-11-18T18:00:00+08:00'},settings:{enabled:true}}:{})});
    if (url.pathname==='/api/learning/papers') return ok({total:Number(answerFixture),page_size:20,items:answerFixture?[learningPaper()]:[]});
    if (url.pathname==='/api/learning/profile') {
      const summary={assigned:answerFixture?1:personalAssigned,task_answered:answerFixture?Number(answerSaved):0};
      return ok({today_summary:summary,summary});
    }
    if (url.pathname==='/api/learning/issues/workbench-questions') {
      const items=pendingLearning&&!learningSaved?[{question_id:'review-question',issue_id:'issue-review-fixture',stem:'隔离原题',feedback_count:1}]:[];
      return ok({items,total:items.length,page:1,page_size:20});
    }
    if (url.pathname==='/api/learning/workbench-summary') {
      const scopes=['ALL','CAMPUS'].includes(scope)?(fixtureAccount?.testScopes||['A']):[scope];
      const summary={已完成:4,待答:2,答题中:1,逾期:0,待核对:0,未到时:0};
      const date=url.searchParams.get('date');
      return ok({scopes,summary,from:date,to:date,people:date?[
        {name:'隔离已作答',scope:'A',shift:'白',answered:8,assigned:15,status:'答题中'},
        {name:'隔离未作答',scope:'B',shift:'白',answered:0,assigned:15,status:'待答'},
        {name:'隔离已完成',scope:'B',shift:'夜',answered:15,assigned:15,status:'已完成'},
      ].filter(person=>scopes.includes(person.scope)):undefined});
    }
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
  const set=async account=>{fixtureAccount=account;await page.evaluate(account=>window.setAccount(account),account);await settle();};
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
      page.waitForRequest(focused,{timeout:60000}),
      dialog.getByRole('button',{name:'办理 隔离维修待办',exact:true}).click(),
    ]).catch(async error=>{
      await page.screenshot({path:path.join(output,'repair-opening-failure.png'),fullPage:true});
      console.error(await page.locator('.task-dialog').innerText({timeout:1000}).catch(()=>'(no dialog)'),errors,requests.slice(-12));
      throw error;
    });
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
  assert.equal(await page.locator('.period-card[data-card=personal]').count(),0,'shared duty has no empty personal modules');
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
    ['duty',['A'],['通告办理','演练管理','水耗管理']],
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
  await set(actor('learning-detail','manager',['learning'],{testScopes:['A','B']}));
  const team=page.getByRole('button',{name:'画像学练情况汇总',exact:true});
  await team.click();
  const detail=page.locator('.learning-detail');
  await detail.getByText('隔离已作答',{exact:true}).waitFor();
  assert.equal(await detail.locator('tbody tr').count(),3);
  await detail.getByRole('combobox',{name:'学练明细状态'}).selectOption('unanswered');
  assert.equal(await detail.locator('tbody tr').count(),1);
  assert.match(await detail.locator('tbody tr').innerText(),/隔离未作答[\s\S]*0\s*\/\s*15[\s\S]*未作答/);
  await detail.getByRole('combobox',{name:'学练明细状态'}).selectOption('all');
  await detail.getByRole('combobox',{name:'学练明细楼栋'}).selectOption('B');
  assert.equal(await detail.locator('tbody tr').count(),2);
  await page.screenshot({path:path.join(output,'learning-detail-mobile.png'),animations:'disabled'});
  await page.keyboard.press('Escape');
  await detail.waitFor({state:'hidden'});
  await set(actor('learning-supervisor','supervisor',['learning'],{level:'L3'}));
  await team.click();
  await detail.getByText('隔离已作答',{exact:true}).waitFor();
  assert.equal(await detail.locator('tbody tr').count(),1);
  await page.keyboard.press('Escape');
  await detail.waitFor({state:'hidden'});
  checks.push('learning details show all authorized buildings by default and filter to unanswered people, with no old account data');
  personalAssigned=0;
  await set(actor('learning-no-personal-content','manager',['learning']));
  await page.getByRole('button',{name:'画像学练情况汇总',exact:true}).waitFor();
  assert.equal(await page.locator('.period-card[data-card=personal]').getByRole('button',{name:'打开画像学练'}).count(),0);
  pendingLearning=true;
  await set(actor('learning-review-only','manager',['learning']));
  const personalReview=page.getByRole('article',{name:'个人 · 待处理题目'});
  await personalReview.getByRole('button',{name:/待处理题目.*1 题/}).click();
  const reviewDialog=page.locator('.desk-review-dialog');
  await reviewDialog.getByRole('button',{name:/隔离原题/}).click();
  await reviewDialog.getByRole('textbox',{name:'题干'}).fill('隔离修订后的题干');
  await reviewDialog.getByRole('radio',{name:'选项 B 为正确答案'}).check();
  await page.screenshot({path:path.join(output,'learning-review-mobile.png'),animations:'disabled'});
  failLearningSave=true;
  await reviewDialog.getByRole('button',{name:'保存并处理反馈'}).click();
  await reviewDialog.getByText('隔离保存失败',{exact:true}).waitFor();
  assert.equal(await reviewDialog.getByRole('textbox',{name:'题干'}).inputValue(),'隔离修订后的题干');
  failLearningSave=false;
  await reviewDialog.getByRole('button',{name:'保存并处理反馈'}).click();
  await reviewDialog.getByText('题目已保存，反馈已处理。',{exact:true}).waitFor();
  await reviewDialog.getByText('暂无待处理题目',{exact:true}).waitFor();
  await reviewDialog.locator('.n-card-header__close').click();
  await reviewDialog.waitFor({state:'hidden'});
  assert.equal(await personalReview.getByRole('button',{name:/待处理题目/}).count(),0);
  personalAssigned=30;pendingLearning=false;
  checks.push('personal card stays visible; successful question edits remove only the resolved item');

  await set(actor('engineer-restricted','engineer',['learning','cabinet','accounts']));
  assert.equal(await page.locator('.wb-shortcut').count(),0,'job recommendations cannot create module grants');
  await todo.getByText('暂无待办',{exact:true}).waitFor();
  await set(actor('engineer-action','engineer',['repair_management','risk_inspection']));
  await checkInlineRepair('A');
  checks.push('recommendations require real module grants; exact-record action and return scope preserved');

  mixedProfessions=true;
  const engineer=actor('engineer-professions','engineer',['repair_management'],{level:'L2',testScopes:['A','B']});
  await set(engineer);
  await open('repair_management');
  await dialog.getByText('A楼电气维修',{exact:true}).waitFor();
  await dialog.getByText('A楼通用维修',{exact:true}).waitFor();
  assert.equal(await dialog.getByText('A楼暖通维修',{exact:true}).count(),0);
  await set({...engineer,specialties:['暖通']});
  await open('repair_management');
  await dialog.getByText('A楼暖通维修',{exact:true}).waitFor();
  await dialog.getByText('A楼通用维修',{exact:true}).waitFor();
  assert.equal(await dialog.getByText('A楼电气维修',{exact:true}).count(),0);
  await page.evaluate(()=>window.setScope('B')); await settle();
  assert.equal(await dialog.count(),0);
  await open('repair_management');
  await dialog.getByText('B楼暖通维修',{exact:true}).waitFor();
  assert.equal(await dialog.getByText(/A楼.+维修|B楼电气维修/).count(),0);
  mixedProfessions=false;
  await set(actor('engineer-action','engineer',['repair_management','risk_inspection']));
  checks.push('same-account professional changes reset cached tasks, keep shared work, and isolate building switches');

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
  assert.equal(await todo.getByLabel('搜索待办').count(),0);
  assert.equal(await todo.locator('.queue-toolbar').getByRole('button',{name:/^(卡片显示|表单显示)$/}).count(),0);
  await counts(19);
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
    const rect=await dialog.boundingBox();assert(rect.x>=0 && rect.x+rect.width<=width+1, JSON.stringify({width,rect}));
    await page.screenshot({path:path.join(output,'dense-modules-'+width+'.png'),animations:'disabled'});
  }
  await set(actor('multi-scope-counts','engineer',['repair_management','risk_inspection'],{testScopes:['A','B']}));
  await counts(19);await open('repair_management');
  await page.evaluate(()=>window.setScope('B')); await settle(); await counts(19);
  assert.equal(await dialog.count(),0,'changing scope closes the previous group');
  await checkInlineRepair('B');
  checks.push('all loaded rows accessible beyond eight, short titles first, refresh, no overflow, and scope reset');

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
  await open('events');assert.equal(await rowsOf('event_notices').count(),1);
  checks.push('both event sources appear under one tile, linked notices deduplicate, and duty grants are preserved');
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
  await page.getByRole('region',{name:'工作状态'}).locator('.period-card').waitFor({state:'detached'});
  checks.push('truncated pages are retryable; empty work-state cards are hidden');

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
  await page.evaluate(()=>window.setNightMode(false));

  const mopStart=requests.length;
  await set(actor('mop-d','duty',['engineer_mop'],{home_scope:'D',testScopes:['D']}));
  await page.locator('.wb-shortcut').filter({hasText:'维保单办理'}).waitFor();
  assert.deepEqual(requests.slice(mopStart).filter(([,url])=>url==='/api/engineer/mop/bootstrap').map(([, , scope])=>scope),['D']);
  await set(actor('mop-a','duty',['engineer_mop']));
  assert.equal(await page.locator('.wb-shortcut').filter({hasText:'维保单办理'}).count(),0);
  checks.push('maintenance-sheet access is displayed in D/E only');

  checkFixture=true;
  await page.setViewportSize({width:390,height:860});
  await set(actor('fixture-check-duty','operator',['pre_shift'],{level:'L4'}));
  const personalCard=page.locator('.period-card[data-card=personal]');
  await personalCard.getByRole('button',{name:/白班班前检查/}).waitFor();
  assert.equal(await page.getByRole('searchbox',{name:'全局搜索'}).count(),0);
  assert.equal(await page.getByRole('region',{name:'班前检查',exact:true}).count(),0);
  assert.equal(await page.getByRole('region',{name:'紧急事项',exact:true}).count(),0);
  checkRosterChanged=true;
  await personalCard.getByRole('button',{name:/白班班前检查/}).click();
  const checkCase=page.getByRole('region',{name:'办理 白班班前检查'});
  await checkCase.locator('legend').filter({hasText:checkItems[0].text}).waitFor().catch(async error=>{
    await page.screenshot({path:path.join(output,'pre-shift-failure.png'),fullPage:true});
    console.error(await checkCase.innerText({timeout:1000}).catch(()=>'(no dialog)'),errors,requests.slice(-12));
    throw error;
  });
  assert.equal(await checkCase.locator('legend').first().evaluate(el=>getComputedStyle(el).fontSize),'14px');
  await page.screenshot({path:path.join(output,'pre-shift-mobile.png'),animations:'disabled'});
  for(const button of await checkCase.getByText('无异常',{exact:true}).all())await button.click();
  await checkCase.getByRole('button',{name:'提交检查',exact:true}).click();
  await tile('pre_shift').waitFor({state:'detached'});
  await personalCard.getByRole('button',{name:/白班班前检查/}).waitFor({state:'detached'});
  assert(checkSubmitted);
  await checkCase.getByRole('button',{name:'关闭待办办理'}).click();
  await checkCase.waitFor({state:'hidden'});
  checks.push('the personal card opens the exact pre-shift form on mobile, even when roster indicators are stale; the allowed submission clears the queue');
  checkRosterChanged=false;
  checkFixture=false;

  answerFixture=true;
  await set(actor('fixture-personal-answer','operator',['learning'],{level:'L4'}));
  const deskUrl=page.url();
  await personalCard.getByRole('button',{name:'打开画像学练',exact:true}).click();
  const learningCase=page.getByRole('region',{name:'办理 我的画像学练'});
  await learningCase.getByText('隔离工作台答题',{exact:true}).waitFor({timeout:30000}).catch(async error=>{
    await page.screenshot({path:path.join(output,'inline-learning-failure.png')});
    console.error(await page.locator('.task-dialog').innerText({timeout:1000}).catch(()=>'(no dialog)'),errors,requests.slice(-12),unexpected);
    throw error;
  });
  await learningCase.getByText('隔离选项',{exact:true}).click();
  await page.screenshot({path:path.join(output,'inline-learning-mobile.png'),animations:'disabled'});
  await learningCase.getByRole('button',{name:'确认作答',exact:true}).click();
  await learningCase.getByRole('button',{name:'已确认作答',exact:true}).waitFor();
  await learningCase.getByRole('button',{name:'关闭待办办理',exact:true}).click();
  await personalCard.getByText('1 / 1',{exact:true}).waitFor();
  assert.equal(page.url(),deskUrl);
  assert(answerSaved);
  answerFixture=false;
  checks.push('personal learning opens in the workbench, answers on mobile and refreshes the progress after closing');

  inspectionOnDuty=true;
  const operator=actor('independent-inspection','operator',['daily_inspection'],{level:'L4',home_scope:'D',testScopes:['D']});
  await set(operator);
  await personalCard.getByRole('button',{name:/D楼白班第 1 轮巡检/}).waitFor();
  assert.equal(await page.locator('.period-card[data-card=building]').getByText(/第 1 轮巡检/).count(),0);
  inspectionOnDuty=false;
  await set({...operator,open_id:'inspection-off-duty'});
  assert.equal(await personalCard.getByRole('button',{name:/第 1 轮巡检/}).count(),0);
  checks.push('inspection authorization works independently of pre-shift access and unassigned shifts show no inspection');

  ownRepair='personal-repair-assignment';
  await set(actor(ownRepair,'operator',['repair_management'],{level:'L4'}));
  await personalCard.getByText('隔离维修待办',{exact:true}).waitFor();
  assert.equal(await page.locator('.period-card[data-card=building]').getByText('隔离维修待办',{exact:true}).count(),0);
  ownRepair='';
  checks.push('a personally assigned repair is shown once in the personal card');

  delegatedCalendar=true;
  checkFixture=true;
  checkSubmitted=false;
  ownRepair='delegated-operator';
  await set(actor(ownRepair,'operator',['learning','pre_shift','repair_management'],{level:'L4',name:'隔离代登录目标',
    impersonation:{active:true,grant_id:'fixture-delegation'}}));
  await personalCard.getByText('隔离维修待办',{exact:true}).waitFor();
  await personalCard.getByText(/0\s*\/\s*30/).waitFor();
  await todo.getByText('班前检查',{exact:true}).waitFor();
  await page.getByText('隔离代登录维保日程',{exact:true}).waitFor();
  await page.getByText('戒备等级 一级',{exact:true}).waitFor();
  await page.getByText('A楼排班测试',{exact:true}).waitFor();
  const delegatedRoster=page.locator('.workbench-roster');
  assert.equal(await delegatedRoster.getByRole('button',{name:/查看A楼排班测试的基本信息/}).locator('.leader-mark').count(),0);
  assert.equal(await delegatedRoster.getByRole('button',{name:/查看刘冉的基本信息/}).locator('.leader-mark').count(),1);
  await personalCard.getByRole('button',{name:/白班班前检查/}).click();
  const delegatedCase=page.getByRole('dialog',{name:'待办办理'});
  await delegatedCase.waitFor();
  assert.equal(await delegatedCase.getByRole('button',{name:'提交检查',exact:true}).count(),0);
  await delegatedCase.getByRole('button',{name:'关闭只读详情'}).click();
  await delegatedCase.waitFor({state:'hidden'});
  delegatedCalendar=false;
  checkFixture=false;
  ownRepair='';
  checks.push('delegated operator sees personal learning and tasks, scoped agenda, duty names and guard; pre-shift cannot be submitted');

  planFixture=true;
  await page.setViewportSize({width:1440,height:1000});
  await set(actor('period-panels','manager',['maintenance','change','learning','event','repair_management'],{level:'L1'}));
  const fold=page.getByRole('button',{name:/未开始通告/}).first();
  await fold.waitFor();
  assert.equal(await fold.getAttribute('aria-expanded'),'false');
  const overview=page.getByRole('region',{name:'工作状态'});
  assert.equal(await overview.getByText(/^隔离维保计划 19/).count(),0);
  await fold.click();
  await overview.getByText('隔离维保计划',{exact:true}).first().waitFor();
  await overview.getByRole('button',{name:/展开其余 18 项/}).first().click();
  for(const index of [20,21,22]) assert.equal(await overview.getByText('隔离维保计划 '+index,{exact:true}).count(),0);
  const campusCard=overview.getByRole('article',{name:'园区 · 事件',exact:true});
  const plannedCard=overview.getByRole('article',{name:'楼栋 · 未开始通告',exact:true});
  const buildingRepairCard=overview.getByRole('article',{name:'楼栋 · 维修跟进',exact:true});
  await buildingRepairCard.waitFor();
  assert.equal(await overview.locator('.period-card').first().getAttribute('data-module'),'events','the event module is first');
  const hue=element=>getComputedStyle(element).getPropertyValue('--tile-hue').trim();
  assert.equal(await campusCard.evaluate(hue),await tile('events').evaluate(hue),'the larger event module shares the tile color');
  const activeNotice=overview.locator('[data-state=ongoing]');
  await activeNotice.first().locator('.scope-tag').getByText(/进行中/).waitFor();
  const [campusBox,plannedBox]=await Promise.all([campusCard.boundingBox(),plannedCard.boundingBox()]);
  assert(campusBox&&plannedBox&&campusBox.height<plannedBox.height-40,'the short module keeps its own height');
  assert(await plannedCard.locator('.period-entries').evaluate(el=>el.scrollHeight>el.clientHeight),'long modules scroll inside their list');
  const longTitle=plannedCard.locator('.period-title').filter({hasText:/^隔离维保计划 19/});
  assert(await longTitle.evaluate(el=>el.scrollWidth>el.clientWidth&&getComputedStyle(el).whiteSpace==='nowrap'
    &&getComputedStyle(el).textOverflow==='ellipsis'&&el.closest('button').title.includes(el.textContent)));
  await page.screenshot({path:path.join(output,'period-cards-desktop.png'),animations:'disabled'});
  checks.push('planned notices start folded, only near deadlines qualify, and work-state cards fit their contents');
  const periodUrl=page.url();
  await campusCard.getByRole('button',{name:/隔离处理中事件/}).click();
  await eventPanel.locator('[data-module-widget="event"]').waitFor();
  await eventPanel.getByRole('button',{name:'关闭待办办理',exact:true}).click();
  await eventPanel.waitFor({state:'hidden'});
  await buildingRepairCard.getByRole('button',{name:/隔离维修待办/}).click();
  const buildingRepair=page.getByRole('region',{name:'办理 隔离维修待办',exact:true});
  await buildingRepair.getByRole('navigation',{name:'维修办理内容'}).getByText(/跟进记录/).waitFor();
  await buildingRepair.getByRole('button',{name:'关闭待办办理',exact:true}).click();
  await buildingRepair.waitFor({state:'hidden'});
  assert.equal(page.url(),periodUrl);
  checks.push('campus events and building repair records open their exact in-place workbench cases');

  const roster=page.getByRole('region',{name:'交接班人员'});
  await roster.getByText('A楼排班测试',{exact:true}).waitFor();
  assert.equal(await roster.getByRole('button',{name:/查看A楼排班测试/}).getAttribute('title'),'值班员');
  assert.equal(await roster.getByRole('button',{name:/查看刘冉/}).getAttribute('title'),'值班长');
  assert.match(await roster.locator('.roster-shift.current-shift .person-key').first().getAttribute('aria-label'),/^查看刘冉/);
  assert.equal(await roster.getByRole('button',{name:/查看薛小明/}).getAttribute('title'),'长白实习生');
  assert.equal(await roster.getByText(/长白实习|11\/18|白班|夜班/).count(),0);
  const guard=page.getByRole('region',{name:'天气预警与戒备'});
  await guard.getByText('戒备等级 一级',{exact:true}).waitFor();
  calendarFailure=true;
  await guard.getByRole('button',{name:'刷新',exact:true}).click();
  await guard.getByText('显示上次识别结果',{exact:true}).waitFor();
  await guard.getByText('戒备等级 一级',{exact:true}).waitFor();
  checks.push('roster titles follow system positions, and weather guard retains its verified result during a transient outage');

  await set(actor('calendar-recovery','operator',['event']));
  await tile('events').waitFor();
  await page.getByRole('button',{name:'重试排班',exact:true}).waitFor();
  await page.getByRole('button',{name:'重试日程',exact:true}).waitFor();
  calendarFailure=false;
  await page.getByRole('button',{name:'重试排班',exact:true}).click();
  await page.getByRole('button',{name:'重试日程',exact:true}).click();
  await page.getByRole('region',{name:'交接班人员'}).getByText('A楼排班测试',{exact:true}).waitFor();
  await page.getByRole('button',{name:'重试日程',exact:true}).waitFor({state:'detached'});
  checks.push('calendar and roster outages leave usable todos and can each recover independently');

  const longText='检查机柜供电和设备状态，确认过程记录完整。'.repeat(12)+'全文结束。';
  await page.evaluate(text=>window.setRepairText(text),longText);
  const repairPreview=page.locator('.preview-fixture .repair-preview-text');
  await page.locator('.preview-fixture [aria-expanded=false]').waitFor();
  assert(await repairPreview.locator('span').evaluate(el=>el.scrollHeight>el.clientHeight));
  await repairPreview.hover();
  const previewPopover=page.locator('.repair-preview-popover');
  await previewPopover.waitFor();
  assert.equal(await previewPopover.innerText(),longText);
  await page.setViewportSize({width:390,height:850});
  await repairPreview.click();
  assert.equal(await repairPreview.getAttribute('aria-expanded'),'true');
  assert(await repairPreview.locator('span').evaluate(el=>el.scrollHeight<=el.clientHeight+1));
  checks.push('long repair text is clamped, hover reveals full text and touch can expand it');

  denseRoster=true;calendarFailure=false;
  await page.setViewportSize({width:1366,height:768});
  await page.evaluate(()=>{window.setRepairText('');document.querySelector('.v2-shell').classList.add('has-topnav');});
  await set(actor('full-screen-desk','manager',full,{testScopes:['H','A','B','C','D','E']}));
  await page.evaluate(()=>window.setScope('CAMPUS'));
  const compactRoster=page.getByRole('region',{name:'交接班人员',exact:true});
  for (const building of ['H楼','A楼','B楼','C楼','D楼','E楼','110站'])
    await compactRoster.getByRole('rowheader',{name:building,exact:true}).waitFor();
  assert.equal(await compactRoster.locator('.roster-row:not(.roster-columns)').count(),7);
  const columnGeometry=()=>page.evaluate(()=>{
    const status=document.querySelector('.desk-supplement');
    return {left:status.getBoundingClientRect().bottom,
      right:document.querySelector('.day-planner').getBoundingClientRect().bottom,
      guardBottom:document.querySelector('.weather-guard').getBoundingClientRect().bottom,
      rosterTop:document.querySelector('.roster-module').getBoundingClientRect().top,
      footer:document.querySelector('.desk-shortcuts').getBoundingClientRect().top};
  });
  const assertAligned=async()=>{
    const geometry=await columnGeometry();
    const rosterGap=geometry.rosterTop-geometry.guardBottom;
    assert(Math.abs(geometry.left-geometry.right)<2&&geometry.footer>=geometry.left&&rosterGap>=0&&rosterGap<=16,JSON.stringify(geometry));
  };
  await assertAligned();
  await page.screenshot({path:path.join(output,'workbench-balanced-1366.png'),fullPage:true,animations:'disabled',style:'.n-message-container{visibility:hidden}'});
  await compactRoster.getByRole('button',{name:'排班详情',exact:true}).click();
  const fullRoster=page.locator('.wb-roster-dialog');
  await fullRoster.getByText('E人员甲',{exact:true}).waitFor();
  assert.equal(await fullRoster.locator('.roster-row:not(.roster-columns)').count(),7);
  await fullRoster.locator('.n-card-header__close').click();
  await fullRoster.waitFor({state:'hidden'});
  await page.setViewportSize({width:1366,height:650});
  await assertAligned();
  checks.push('seven sites are visible inline below the warning panel and desktop columns stay aligned');
  assert.deepEqual(requests.filter(([method])=>method!=='GET').map(([method,url])=>[method,url]),[
    ['PUT','/api/learning/issues/issue-review-fixture/question'],
    ['PUT','/api/learning/issues/issue-review-fixture/question'],
    ['POST','/api/pre-shift-checks/submit'],
    ['POST','/api/learning/papers/desk-paper/start'],
    ['POST','/api/learning/papers/desk-paper/answer'],
  ]);
  assert.deepEqual(unexpected,[]); assert.deepEqual(errors,[]);
  await writeFile(path.join(output,'results.json'),JSON.stringify({checks,apiReads:requests.length,unexpected,errors},null,2));
  console.log(checks.join('\n'));
  console.log('PASS: isolated fixtures; no real service or message requests. Screenshots and results: '+output);
} finally {await browser?.close(); await server.close();}
