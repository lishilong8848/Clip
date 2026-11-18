import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/workbench-redesign');
const fixture=`<template><NConfigProvider><NDialogProvider><NMessageProvider>
  <HomePage :user="user" :scope="scope" :scopes="scopes" :explicit-scope="explicitScope" view="home" @open="open" />
  </NMessageProvider></NDialogProvider></NConfigProvider></template>
  <script setup>
  import {ref} from 'vue';
  import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
  import HomePage from '/src/workspace-v2/HomePage.vue';
  import {runNavigationAction} from '/src/workspace-v2/navigation';
  import '/src/workspace-v2/workspace.css';
  import '/src/workspace-v2/night.css';
  document.documentElement.classList.add('workspace-v2');
  const user=ref({}),scope=ref('A'),scopes=ref([]),explicitScope=ref(true);
  window.setAccount=(account,options,explicit)=>{user.value=account;scopes.value=options;scope.value=options[0].value;explicitScope.value=explicit;};
  function open(url){runNavigationAction(url,()=>{window.opened=url;if(new URL(url,location.origin).pathname==='/'&&!url.includes('mode')&&!url.includes('view')){scope.value=new URL(url,location.origin).searchParams.get('scope');explicitScope.value=true;}});}
  </script>`;
const entry=`import {createApp} from 'vue'; import Fixture from '/@workbench-check.vue'; createApp(Fixture).mount('#app');`;
const html=`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">
  <div id="app"></div><script type="module" src="/@workbench-check.js"></script>`;
const server=await createServer({
  root,configFile:false,logLevel:'error',cacheDir:path.join(root,'node_modules/.vite-workbench-check'),
  optimizeDeps:{include:['vue','naive-ui','lucide-vue-next']},
  server:{host:'127.0.0.1',port:0},
  plugins:[{name:'workbench-fixture',enforce:'pre',resolveId(id){if (id.startsWith('/@workbench-check.')) return id;},
    load(id){if(id==='/@workbench-check.vue')return fixture;if(id==='/@workbench-check.js')return entry;}},vue()],
});
await server.listen();
const origin='http://127.0.0.1:'+server.httpServer.address().port;
let browser;
try {
  browser=await chromium.launch({headless:true});
  await mkdir(output,{recursive:true});
  const page=await browser.newPage({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
  await page.clock.setFixedTime(new Date('2026-11-18T03:00:00Z'));
  const errors=[], requests=[], unexpected=[];
  page.on('pageerror',error=>errors.push(error.stack||error.message));
  let failure='', exempt=true, hold=false, release, checkMember='', practiceFinished=false;
  const periodDates=['2026-11-18','2026-11-17','2026-11-10'];
  const selfPerson={id:'one',name:'隔离学练人员',scopes:['A'],active:true};
  await page.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),scope=url.searchParams.get('scope');
    if(url.origin!==origin)return route.abort();
    if(url.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:html});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    requests.push([req.method(),url.pathname,scope,url.searchParams]);
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(req.method()!=='GET') {unexpected.push(req.method()+' '+url.pathname);return route.abort();}
    if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
    if(url.pathname==='/api/events/monthly'){
      const month=url.searchParams.get('month');
      if(url.searchParams.get('date_field')==='occurrence_time'){
        const dates=scope==='B'?[periodDates[0],...periodDates]:periodDates;
        return ok({scope,month,snapshot_exists:true,records:dates.filter(date=>date.startsWith(month)).map((date,i)=>({
          record_id:scope+'-period-'+i,title:scope+'楼周期事件',occurrence_time:date+' 10:00',status:i?'已恢复':'处理中',level:i?'I3':'I2',
        }))});
      }
      if(hold&&scope==='A'&&!url.searchParams.has('date_field'))await new Promise(resolve=>release=resolve);
      if(failure&&scope==='B'&&!url.searchParams.has('date_field'))return route.fulfill({status:Number(failure),json:{ok:false,error:'fixture'}});
      return ok({scope,month,snapshot_exists:true,records:[{id:'event-'+scope,title:scope+'楼隔离事件',status:'处理中'}]});
    }
    if(url.pathname==='/api/history-summary'){
      assert.ok(['maintenance','change','repair','all'].includes(url.searchParams.get('work_type')));
      const month=url.searchParams.get('month'),isB=scope==='B';
      return ok({scope,month,days:periodDates.filter(date=>date.startsWith(month)).map((date,i)=>({
        date,stats:{started:(isB?2:1)+i,updated:0,ended:1},
      }))});
    }
    if(url.pathname==='/api/workbench'){
      const planPage=Number(url.searchParams.get('records_page')||1);
      const type=url.searchParams.get('work_type');
      if(type!=='all'){
        const title=({change:'变更',repair:'检修',power:'上下电'})[type];
        return ok({ongoing:[{work_type:type,active_item_id:scope+type,scope,title:scope+'楼隔离'+title,status:'开始'}],records:[],
          ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
      }
      const plan=(id,title,source_progress)=>({record_id:id,source_record_id:id,title,work_type:'maintenance',source_progress});
      return ok({ongoing:[{...plan('linked','计划和进行中同一条','未开始'),active_item_id:'active-1',status:'开始'}],ongoing_pagination:{total_pages:1},
        records:planPage===1?[plan('linked','计划和进行中同一条','未开始'),plan('first','本月计划通告','未开始'),plan('completed','已结束的计划','已结束')]:[plan('second','下一页计划通告','未开始')],
        records_pagination:{total_pages:2}});
    }
    if(url.pathname==='/api/repair-management/status')return ok({total:1,records:[{record_id:'repair-'+scope,title:scope+'楼隔离维修',building:scope,workflow:'维修中'}]});
    if(url.pathname==='/api/critical-guard/tasks')return ok({tasks:scope==='A'
      ? [{task_id:'guard-live',title:'本楼重保任务',complete:false,status:'published'},{task_id:'guard-done',title:'已完成重保',complete:true,status:'published'}] : []});
    if(url.pathname==='/api/risk-inspections')return ok({total:1,items:[{id:'risk-1',name:'隔离专项排查',total:2,approved:1,submitted:1,status:'active'}]});
    if(url.pathname==='/api/cabinet-power/batches')return ok({total:2,items:[{batch_id:'cab-live',title:'机柜待确认',is_todo:true},{batch_id:'cab-done',title:'机柜已确认',is_todo:false}]});
    if(url.pathname==='/api/capacity/water/records')return ok({total:0,records:[]});
    if(url.pathname==='/api/drills')return ok({items:[{drill_id:'drill-live',name:'待完成演练甲',status:'published'},
      {drill_id:'drill-done',name:'已完成演练甲',status:'published',execution:{status:'synced'}}]});
    if(url.pathname==='/api/engineer/mop/bootstrap')return ok({notices:[
      {notice_key:'mop-live',title:'待处理维护单甲',status:'开始'},{notice_key:'mop-done',title:'已上传维护单甲',mop_binding:{id:'bound'},mop_uploaded:true,status:'开始'}]});
    if(url.pathname==='/api/workbench-events')return ok({items:[{event_id:'event-active',state:'active',metadata:{summary:'进行中事件通告甲'}},{event_id:'event-ended',state:'ended'}]});
    if(url.pathname==='/api/learning/workbench-summary')return ok({scopes:scope==='ALL'?['A','B']:[scope],
      summary:{已完成:4,待答:2,答题中:1,逾期:0,待核对:0,未到时:0}});
    if(url.pathname==='/api/cold-source-monitor'){
      assert.equal(url.searchParams.get('status_only'),'1');
      return ok({items:[{scope:'A',mode:'机械制冷',date:'2026-11-18',temperature:16},{scope:'B',mode:'自然冷却',date:'2026-11-18',temperature:14},
        {scope:'E',mode:'越权楼栋状态',date:'2026-11-18',temperature:17}]});
    }
    if(url.pathname==='/api/auth/permission-requests/admin')return ok({items:[{request_id:'perm-1',name:'权限待审核人',status:'pending'}]});
    if(url.pathname==='/api/tool-self-check/workspace/history')return ok({sessions:[{id:'tool-session',history_items:[
      {id:'asset-A',asset_name:'工具待审核',scope:'A',order_status:'review_pending'},
      {id:'asset-B',asset_name:'工具已归档',scope:'A',order_status:'submitted',archive_status:'archived'}]}]});
    if(url.pathname==='/api/pre-shift-checks'){
      const item={id:'check-1',text:'隔离检查项目'},server_time='2026-11-18T10:00:00+08:00';
      return ok({scope,scopes:['A'],can_manage:false,roster_ready:!!checkMember,server_time,
        definition:{items:checkMember?[{...item,frequency:{kind:'shift'}}]:[],version:1},
        tasks:checkMember?[{id:'check',scope,shift:'白',day:'2026-11-18',definition_version:1,
          due:Date.parse(server_time)/1000,deadline:Date.parse('2026-11-18T18:00:00+08:00')/1000,
          items:[item],people:[{open_id:checkMember,name:'隔离值班人员'}],can_submit:true,can_upload_handover:false,submitted:''}]:[]});
    }
    if(url.pathname==='/api/learning/bootstrap')return ok({self_scope:'A',self_person:selfPerson,can_answer:true,today:periodDates[0],...(exempt?{daily_exemption:{status:'exempt',resume_date:'2026-12-17'}}:{})});
    if(url.pathname==='/api/learning/papers')return ok({total:3,page_size:20,items:[
      {person_id:'one',id:'required',mode:'daily',status:'pending',scope:'A',stats:{total:15,answered:0}},
      {person_id:'one',id:'practice',mode:'practice',status:practiceFinished?'completed':'pending',scope:'A',stats:{total:1,answered:Number(practiceFinished)}},
      {person_id:'one',id:'flagged',mode:'daily',daily_exempt:true,status:'pending',scope:'A',stats:{total:15,answered:0}},
    ]});
    if(url.pathname==='/api/learning/profile'){
      assert.equal(scope,'A');
      assert.equal(url.searchParams.get('person_id'),'one');
      const from=url.searchParams.get('from'),to=url.searchParams.get('to');
      assert.ok(periodDates[0]===to && ['2026-11-18','2026-11-16','2026-11-01'].includes(from));
      return ok({today_summary:{assigned:15,task_answered:4},summary:{assigned:from==='2026-11-16'?30:60,task_answered:from==='2026-11-16'?9:23}});
    }
    if(url.pathname==='/api/signatures/management/account')return ok({person:{is_self:true,building:'A楼',signature_status:'signed'}});
    if(url.pathname==='/api/daily-tasks')return ok({scope,date:url.searchParams.get('date'),stats:{total:5,completed:3,ongoing:1,attention:1}});
    unexpected.push(url.pathname);return route.fulfill({status:404,json:{ok:false,error:'No fixture'}});
  });
  await page.goto(origin+'/fixture');
  await page.waitForFunction(()=>!!window.setAccount);
  const account=(id,category,modules=['event','repair_management','learning'])=>({open_id:id,category,modules,role:'building',home_scope:'A',enabled:true});
  const options=ids=>ids.map(value=>({value,label:value+'楼'}));
  const set=(actor,ids,explicit=true)=>page.evaluate(([user,scopes,explicit])=>window.setAccount(user,scopes,explicit),[actor,options(ids),explicit]);
  await page.evaluate(()=>{
    localStorage.setItem('clipflow:workspace-layout:v1:engineer',JSON.stringify({version:1,widgets:[
      {id:'main',kind:'shortcuts',links:['learning','accounts','learning'],x:0,y:0},
      {id:'tasks',kind:'tasks',taskKinds:['learning'],x:0,y:1},
    ]}));
  });
  await set(account('engineer','other'),['A']);
  await page.getByRole('heading',{name:'需要关注',exact:true}).waitFor();
  await page.waitForFunction(()=>document.querySelector('.wb-shortcut')?.textContent.includes('画像学练'));
  assert.equal(await page.locator('.wb-shortcuts').getByText('账号与权限').count(),0);
  assert.equal(await page.locator('.wb-shortcuts').getByText('画像学练').count(),1);
  assert.equal(await page.locator('.wb-scope select').count(),0);
  const todo=page.getByRole('region',{name:/待办事项/});
  const task=name=>todo.getByRole('button',{name:'办理 '+name,exact:true});
  await task('自主练习').waitFor();
  await task('A楼隔离维修').waitFor();
  assert.equal(await page.getByText('每日学练',{exact:true}).count(),0);
  await page.setViewportSize({width:390,height:844});
  await task('自主练习').click();
  assert.equal(await page.evaluate(()=>window.opened),'/learning?view=practice&paper_id=practice&workbench_return=A');
  assert.equal(await task('自主练习').count(),1,'opening a todo does not complete it');
  practiceFinished=true;
  await todo.getByRole('button',{name:'刷新',exact:true}).click();
  await task('自主练习').waitFor({state:'hidden'});
  await page.setViewportSize({width:1440,height:1000});
  exempt=false;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await page.getByText('每日学练',{exact:true}).first().waitFor();
  assert.equal(await page.getByText('每日学练',{exact:true}).count(),1,'per-paper daily_exempt still excludes the task');

  const period=page.getByRole('region',{name:'工作状态'});
  const card=label=>period.locator('.wb-period-card').filter({has:page.getByRole('heading',{name:label,exact:true})});
  const marker=async(label,value)=>{
    const locator=card(label).locator('.wb-period-value strong');
    await locator.filter({hasText:new RegExp('^'+value+'$')}).waitFor();
  };
  await set(account('period-review','other',['daily','event','maintenance','learning']),['A','B']);
  await marker('每日任务','5');
  await marker('个人学练','4 / 15');
  await marker('事件','1');
  await marker('通告办理记录','2');
  await task('本月计划通告').waitFor();
  assert.equal(await task('计划和进行中同一条').count(),1);
  assert.equal(await task('已结束的计划').count(),0);
  await task('本月计划通告').click();
  assert.equal(await page.evaluate(()=>window.opened),'/workbench-lite?scope=A&work_type=maintenance&month=11%E6%9C%88&record_id=first&notice_action=start&workbench_return=A');
  await task('本月计划通告').waitFor();
  await todo.getByLabel('业务分类').selectOption('notices');
  await todo.getByLabel('搜索待办').fill('下一页');
  await todo.getByRole('button',{name:'显示更多待办'}).click();
  await task('下一页计划通告').waitFor();
  await todo.getByLabel('搜索待办').fill('');
  await todo.getByLabel('业务分类').selectOption('');
  assert.equal(await card('每日任务').locator('meter[aria-label="已完成"]').getAttribute('value'),'3');
  const dailyReads=()=>requests.filter(([,url])=>url==='/api/daily-tasks').length;
  const initialDaily=dailyReads();
  await period.getByRole('button',{name:'本周',exact:true}).click();
  await marker('事件','2');await marker('通告办理记录','5');await marker('个人学练','9 / 30');
  assert.equal(await card('每日任务').count(),0);
  assert.equal(await card('事件').locator('meter[aria-label="已恢复"]').getAttribute('value'),'1');
  await period.getByRole('button',{name:'本月',exact:true}).click();
  await marker('事件','3');await marker('通告办理记录','9');await marker('个人学练','23 / 60');
  assert.equal(await card('事件').locator('meter[aria-label="已恢复"]').getAttribute('value'),'2');
  assert.equal(dailyReads(),initialDaily,'week and month do not request a daily snapshot');
  await page.locator('.wb-scope select').selectOption('B');
  await marker('事件','4');await marker('通告办理记录','12');
  assert.ok(requests.some(([,url,scope,query])=>url==='/api/events/monthly'&&scope==='B'&&query.get('date_field')==='occurrence_time'));
  assert.ok(requests.some(([,url,scope,query])=>url==='/api/history-summary'&&scope==='B'&&query.get('work_type')==='maintenance'));
  await period.getByRole('button',{name:'打开事件',exact:true}).click();
  assert.equal(new URL(await page.evaluate(()=>window.opened),origin).searchParams.get('scope'),'B');
  await period.getByRole('button',{name:'今日',exact:true}).click();
  await marker('事件','2');await marker('每日任务','5');
  assert.ok(requests.some(([,url,scope])=>url==='/api/daily-tasks'&&scope==='B'));

  hold=true;
  await set(account('manager','manager'),['A','B']);
  await page.waitForFunction(()=>document.querySelector('.wb-focus h2')?.textContent==='园区关注');
  const until=Date.now()+5000;
  while(!release&&Date.now()<until)await new Promise(resolve=>setTimeout(resolve,25));
  assert.ok(release,'A request was held to exercise stale responses');
  await page.locator('.wb-scope select').selectOption('B');
  hold=false;release();
  const eventCard=page.locator('.wb-focus-card').filter({hasText:'处理中事件'});
  await eventCard.getByText('待处理',{exact:true}).waitFor();
  await eventCard.click();
  await page.locator('.n-drawer').getByText('B楼隔离事件',{exact:true}).waitFor();
  assert.equal(await page.getByText('A楼隔离事件',{exact:true}).count(),0);
  failure='500';await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await eventCard.getByText('数据待更新',{exact:true}).waitFor();
  failure='403';await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await todo.getByRole('status').getByText('处理中事件 当前无权查看',{exact:false}).waitFor();
  assert.equal(await page.getByText('B楼隔离事件',{exact:true}).count(),0);
  await page.keyboard.press('Escape');
  await page.getByRole('button',{name:'打开原模块',exact:true}).waitFor({state:'hidden'});
  failure='';

  checkMember='operator';
  await set(account('operator','operator',['learning','water']),['A']);
  await page.getByText('当前账号 · 值班人员 · 白班',{exact:true}).waitFor();
  await page.getByText('无异常',{exact:true}).click();
  const previous=await page.evaluate(()=>window.opened);
  await page.locator('.wb-shortcuts').getByRole('button',{name:'水耗管理'}).click();
  await page.getByText('检查尚未提交',{exact:true}).waitFor();
  assert.equal(await page.evaluate(()=>window.opened),previous,'an unfinished pre-shift check blocks navigation');
  await page.getByRole('button',{name:'继续填写'}).click();
  assert.equal(await page.locator('[data-v2-layout-protected=true]').count(),1);
  await page.locator('.wb-shortcuts').getByRole('button',{name:'水耗管理'}).click();
  await page.getByRole('button',{name:'放弃并离开'}).click();
  assert.equal(await page.locator('[data-v2-layout-protected=true]').count(),0);
  checkMember='';

  const full=['event','repair_management','learning','maintenance','change','repair','power','risk_inspection','critical_guard','cabinet','drills','engineer_mop','daily','water','accounts','signatures'];
  await set(account('campus-dashboard','manager',full),['A','B'],false);
  await task('B楼隔离维修').waitFor();
  await task('A楼隔离维修').waitFor();
  await marker('画像学练情况汇总','7');
  await todo.getByText('A楼隔离变更',{exact:true}).waitFor();
  await todo.getByText('B楼隔离上下电',{exact:true}).waitFor();
  assert.equal(await todo.getByRole('button',{name:/A楼隔离变更|B楼隔离上下电/}).count(),0);
  assert.equal(await todo.getByText('本月计划通告',{exact:true}).count(),0,'经理不显示维保通告');
  assert.equal(await todo.getByText('已完成重保',{exact:true}).count(),0);
  await todo.getByText('本楼重保任务',{exact:true}).waitFor();
  await page.getByText('自然冷却',{exact:true}).waitFor();
  assert.equal(await page.getByText('越权楼栋状态',{exact:true}).count(),0);
  assert.equal(await page.locator('.wb-shortcuts').getByText('通告办理').count(),0);
  await page.locator('.wb-scope select').selectOption('B');
  await todo.getByText('B楼隔离变更',{exact:true}).waitFor();
  assert.equal(await todo.getByText('A楼隔离变更',{exact:true}).count(),0);
  assert.equal(await todo.getByText('本楼重保任务',{exact:true}).count(),0);

  await set(account('building-dashboard','supervisor',full),['A','B']);
  await task('机柜待确认').waitFor();
  await marker('画像学练情况汇总','7');
  assert.equal(await page.locator('.wb-scope select').count(),0);
  assert.equal(await todo.getByText('机柜已确认',{exact:true}).count(),0);
  assert.equal(await todo.getByText('B楼隔离维修',{exact:true}).count(),0);
  await task('机柜待确认').click();
  assert.match(await page.evaluate(()=>window.opened),/scope=A&batch_id=cab-live/);

  await set(account('engineer-dashboard','engineer',full),['A']);
  await task('隔离专项排查').waitFor();
  await task('A楼隔离维修').waitFor();
  assert.equal(await todo.locator('.wb-task-row').count(),2);

  const dutyReadStart=requests.length;
  await set(account('shared-desk','duty',full),['A']);
  await todo.getByRole('list',{name:'计划通告',exact:true}).getByRole('button',{name:'开始 本月计划通告',exact:true}).waitFor();
  await todo.getByRole('list',{name:'进行中通告',exact:true}).getByText('进行中事件通告甲',{exact:true}).waitFor();
  assert.equal(await todo.getByText('计划和进行中同一条',{exact:true}).count(),1);
  await task('待完成演练甲').waitFor();
  await task('待处理维护单甲').waitFor();
  assert.equal(await todo.getByText('已完成演练甲',{exact:true}).count(),0);
  assert.equal(await todo.getByText('已上传维护单甲',{exact:true}).count(),0);
  assert.ok(!requests.slice(dutyReadStart).some(([,url])=>url.startsWith('/api/learning/')));

  await set({...account('admin-dashboard','administrator',full),tool_management_enabled:true,can_manage_accounts:true},['A']);
  await task('工具待审核').waitFor();
  await task('权限待审核人').waitFor();
  assert.equal(await todo.getByText('工具已归档',{exact:true}).count(),0);

  for(const [profile,category,scopes] of [
    ['manager','manager',['A','B']],['supervisor','supervisor',['A']],
    ['engineer','engineer',['A']],['duty','duty',['A']],
    ['specialist','engineer',['A','B']],['administrator','administrator',['A','B']],
  ]){
    await set(account(profile,category),scopes);
    for(const width of [1440,1024,800,390]){
      await page.setViewportSize({width,height:1000});
      await page.waitForFunction(()=>document.querySelector('.wb-home'));
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'no page overflow at '+width);
      if(width===1440||width===390){
        await page.evaluate(()=>window.scrollTo(0,0));
        await page.screenshot({path:path.join(output,profile+'-'+width+'.png'),animations:'disabled'});
      }
    }
  }
  await page.evaluate(()=>document.documentElement.dataset.v2Theme='dark');
  assert.ok(await page.locator('.wb-primary').evaluate(element=>element.getBoundingClientRect().height>=48));
  assert.ok(requests.every(([method])=>method==='GET'));
  assert.deepEqual(unexpected,[]);
  assert.deepEqual(errors,[]);
  const before=await readFile(path.join(root,'src/workspace-v2/HomePage.vue'),'utf8');
  assert.match(before,/<BusinessTodos /);assert.match(before,/<TaskWorkspace /);
  console.log('Workbench roles, responsive layouts, todo filters and pagination, exact record dispatch and completion refresh passed.');
} finally { await browser?.close(); await server.close(); }
