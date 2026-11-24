import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { adaptEmbeddedNavigation } from './workspace-embedded-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,process.env.WORKBENCH_REDESIGN_OUTPUT||'../../../output/playwright/workbench-redesign');
const fixture=`<template><NConfigProvider><NDialogProvider><NMessageProvider>
  <div v-show="shown" class="workbench-fixture-account"><WorkbenchWeather :identity="user.open_id"/></div>
  <KeepAlive :key="user.open_id">
    <WorkbenchHome v-if="shown" :user="user" :scope="scope" :scopes="scopes" :explicit-scope="explicitScope" @open="open" />
  </KeepAlive>
  </NMessageProvider></NDialogProvider></NConfigProvider></template>
  <script setup>
  import {ref} from 'vue';
  import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
  import WorkbenchHome from '/src/workspace-v2/workbench/WorkbenchHome.vue';
  import WorkbenchWeather from '/src/workspace-v2/workbench/WorkbenchWeather.vue';
  import {runNavigationAction} from '/src/workspace-v2/navigation';
  import '/src/workspace-v2/workspace.css';
  import '/src/workspace-v2/night.css';
  document.documentElement.classList.add('workspace-v2');
  const user=ref({}),scope=ref('A'),scopes=ref([]),explicitScope=ref(true),shown=ref(true);
  window.showWorkbench=value=>{shown.value=value;};
  window.setAccount=(account,options,explicit)=>{user.value=account;scopes.value=options;scope.value=options[0].value;explicitScope.value=explicit;};
  function open(url){runNavigationAction(url,()=>{window.opened=url;if(new URL(url,location.origin).pathname==='/'&&!url.includes('mode')&&!url.includes('view')){scope.value=new URL(url,location.origin).searchParams.get('scope');explicitScope.value=true;}});}
  </script>`;
const entry=`import {createApp} from 'vue'; import Fixture from '/@workbench-check.vue';
  import LoadingIndicator from '/src/components/LoadingIndicator.vue'; import UiTransition from '/src/components/UiTransition.vue';
  createApp(Fixture).component('LoadingIndicator', LoadingIndicator).component('UiTransition', UiTransition).mount('#app');`;
const html=`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">
  <div id="app"></div><script type="module" src="/@workbench-check.js"></script>`;
const server=await createServer({
  root,configFile:false,logLevel:'error',cacheDir:path.join(root,'node_modules/.vite-workbench-check'),
  optimizeDeps:{include:['vue','naive-ui','lucide-vue-next']},
  server:{host:'127.0.0.1',port:0,hmr:false},
  plugins:[{name:'workbench-fixture',enforce:'pre',resolveId(id, importer){
    if (id.startsWith('/@workbench-check.')) return id;
    const source=importer?.replaceAll('\\','/');
    if(source?.includes('/src/')){
      if(id.endsWith('/VnetBackButton.vue'))return path.join(root,'src/workspace-v2/ModuleBackButton.vue');
      if(id.endsWith('/VnetSelect.vue')&&!source.endsWith('/SingleSelect.vue'))return path.join(root,'src/workspace-v2/SingleSelect.vue');
    }
  },
    load(id){if(id==='/@workbench-check.vue')return fixture;if(id==='/@workbench-check.js')return entry;},
    transform(code,id){if(id.endsWith('.vue')&&/[\\/]src[\\/]/.test(id))return adaptEmbeddedNavigation(code,id.replaceAll('\\','/'));}},vue()],
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
  let currentAccount={}, holdAgenda=false, releaseAgenda;
  let dailyDesk=false;
  let aggregateTrial=false, lateForecast=false, missingWeather=false;
  let pausedReads;
  let repairTrial=false, historyClosed=false;
  let mopDelayed=false, riskDenied=false, mopRetryFailures=0;
  let embeddedTest=false, inlineRead;
  let waterTrial=false,waterSaved=false;
  const waterWrites=[];
  const waterRow=()=>({record_id:'water-home',statistic_date:'2026-11-18',meter:'工作台验收水表',
    frequency:'日',shift:'白',meter_value:waterSaved?1250:null,photo_count:1});
  const periodDates=['2026-11-18','2026-11-17','2026-11-10'];
  const selfPerson={id:'one',name:'隔离学练人员',scopes:['A'],active:true};
  await page.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),scope=url.searchParams.get('scope');
    if(url.origin!==origin)return route.abort();
    if(url.pathname==='/fixture')return route.fulfill({contentType:'text/html',body:html});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    requests.push([req.method(),url.pathname,scope,url.searchParams]);
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(waterTrial&&url.pathname==='/api/capacity/water/records/water-home'&&req.method()==='PATCH'){
      waterWrites.push(req.postDataJSON());
      if(waterWrites.length===1)return route.fulfill({status:400,json:{ok:false,error:'隔离保存失败'}});
      waterSaved=true;
      return ok({record_id:'water-home',warnings:[]});
    }
    if(req.method()!=='GET') {unexpected.push(req.method()+' '+url.pathname);return route.abort();}
    const paused=inlineRead;
    if(paused?.matches(url)){paused.started(url);await paused.until;}
    if(url.pathname==='/api/assistant/icon-weather')return ok({
      available:true,date:'2026-11-18',temperature:16,humidity:62,description:'多云',
      hourly:Array.from({length:12},(_,i)=>({
        time:new Date(Date.parse('2026-11-18T'+(lateForecast?'22':'11')+':00:00+08:00')+i*3600000).toISOString(),
        temp:lateForecast?15-i:15+i,
      })).filter((_,i)=>!missingWeather||i!==4),
      forecast:[{date:'2026-11-18',minimum:9,maximum:19,description:'多云'},
        {date:'2026-11-19',minimum:10,maximum:18,description:'小雨'}],
    });
    if(url.pathname==='/api/personal-calendar'){
      if(holdAgenda&&url.searchParams.get('view')!=='roster')await new Promise(resolve=>releaseAgenda=resolve);
      const allowed=building=>scope==='ALL'||scope===building;
      const staff=(building,name,detail={})=>({scope:building,people:[{name,...detail}]});
      const current=[staff('A','验收值班员',{open_id:'ou_fixtureA1',group:'A班',position:'值班员'}),staff('A','白班第二人'),
        staff('A','隔离长白实习生',{open_id:'ou_fixtureIntern',group:'A班',position:'运维实习生',duty_kind:'长白'}),staff('B','B楼当班人员')].filter(row=>allowed(row.scope));
      const incoming=[staff('A','夜班验收人'),staff('B','B楼当晚值班员')].filter(row=>allowed(row.scope));
      const sealing={kind:'封网',title:'隔离封网安排',start:'2026-11-18T10:00:00+08:00',end:'2026-11-19T13:00:00+08:00',scopes:['A']};
      return ok({
      month:url.searchParams.get('month'),unavailable:scope==='B'?['封网']:[],
      seal_status:scope==='B'?'unconfigured':'ready',seal_windows:dailyDesk&&allowed('A')?[sealing]:[],
      handover:dailyDesk?{current:{date:'2026-11-18',shift:'白',verified:true,staff:current},
        incoming:{date:'2026-11-18',shift:'夜',verified:true,staff:incoming}}:undefined,
      roster_dates:dailyDesk&&url.searchParams.get('month')==='2026-11'?['2026-11-18']:[],
      days:dailyDesk&&url.searchParams.get('month')==='2026-11'?{'2026-11-18':{
        items:scope==='A'?[{kind:'封网',title:'隔离封网安排',start:'2026-11-18T10:00:00+08:00'},
          {kind:'风险排查',source:'智航',title:'隔离智航风险',start:'2026-11-18T09:30:00+08:00'},
          ...Array.from({length:18},(_,i)=>({kind:'变更',title:'隔离晚间事项 '+(i+1),start:'2026-11-18T19:00:00+08:00'}))]:[],
        staff:scope==='A'||scope==='ALL'?[
          {scope:'A',shift:'白班',names:['验收值班员']},
          {scope:'A',shift:'白',names:['验收值班员','白班第二人']},
          {scope:'A',shift:'night',names:['夜班验收人']},
          {scope:'A',shift:'长白',names:['长白验收人']},
          ...(scope==='ALL'?[{scope:'B',shift:'夜班',names:['B楼当晚值班员']}]:[]),
        ]:[{scope:'B',shift:'夜班',names:['B楼当晚值班员']}],
      },'2026-11-19':{items:[],staff:[{scope:'A',shift:'白班',names:['明日值班员']}]} }:{},
    });}
    if(url.pathname==='/api/personal-calendar/weather')return ok({
      warning_ready:true,
      alerts:[{id:'yellow',title:'大风预警',guard_level:'三级'}],guard_level:'三级',guard_actions:['关注临时设施'],
    });
    if(url.pathname==='/api/change-confirmations')return ok({can_confirm:true,items:[
      {state:'awaiting_confirmation',target_record_id:'ali-1',title:'本日变更截图待确认',building:'B楼'},
      {state:'confirmed',target_record_id:'ali-2',title:'已确认变更',building:'A楼'},
    ]});
    if(url.pathname==='/api/daily-inspection/status')return ok({
      view_scope:url.searchParams.get('view_scope'),duty:{scope:'D',date:'2026-11-18',shift:'白'},
      next_round:2,round_limit:2,opens_at:Date.parse('2026-11-18T16:00:00+08:00'),
    });
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
      if(pausedReads&&scope==='A')await new Promise(resolve=>pausedReads.push(resolve));
      if(failure&&scope==='B'&&!url.searchParams.has('date_field'))return route.fulfill({status:Number(failure),json:{ok:false,error:'fixture'}});
      return ok({scope,month,snapshot_exists:true,records:[
        {id:'event-'+scope,title:scope+'楼隔离事件',specialty:'电气',status:'处理中'},
        ...(dailyDesk?[{id:'hvac-'+scope,title:scope+'楼暖通事件',specialty:'暖通',status:'处理中'}]:[])]});
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
      if(url.searchParams.get('sections')==='records'&&url.searchParams.get('records_page_size')==='0'){
        const row=(building,title,progress)=>({record_id:'plan-'+building+progress,work_type:'maintenance',title,building_codes:[building],
          source_progress:progress,owner_level:'L4',specialty:'电气',planned_start_time:'2026-11-18 08:00',planned_end_time:'2026-11-19 18:00'});
        const records=url.searchParams.get('month')==='11月'?
          [row('A','隔离明日截止维保','未开始'),row('B','B楼待开始维保','未开始'),row('A','隔离已完成计划','已完成')]:[];
        if(currentAccount.open_id==='period-review'&&url.searchParams.get('month')==='11月')
          records.push(
            {...row('A','隔离逾期维保','未开始'),record_id:'plan-overdue',planned_start_time:'2026-11-12 09:00',planned_end_time:'2026-11-13 18:00'},
            {...row('A','跨月未发检修','进行中'),record_id:'repair-unsent',work_type:'repair',planned_start_time:'2026-10-12 09:00',planned_end_time:'2026-10-13 18:00'},
            {...row('A','已发检修通告','未开始'),record_id:'repair-sent',work_type:'repair',planned_end_time:'',repair_notice_linked:true});
        return ok({scope,source_snapshot_ready:true,records:records.filter(row=>scope==='ALL'||row.building_codes.includes(scope))});
      }
      if(aggregateTrial && url.searchParams.get('sections')==='ongoing'){
        const ongoing=[
          {active_item_id:'today-a',title:'A楼今日进行中',start_time:'2026-11-18 08:00',building_codes:['A']},
          {active_item_id:'late-b',title:'B楼前期仍未结束',start_time:'2026-10-29 08:00',end_time:'2026-10-29 09:00',building_codes:['B']},
          {active_item_id:'two-floors',title:'跨楼进行中',start_time:'2026-11-18 08:00',building_codes:['A','B']},
          {active_item_id:'closed',title:'已结束的旧通告',start_time:'2026-11-18 07:00',building_codes:['A'],status:'结束'},
          {active_item_id:'future',title:'次日才开始的通告',start_time:'2026-11-19 08:00',building_codes:['A']},
        ].filter(row=>scope==='ALL'||(scope==='CAMPUS'?row.building_codes.length>1:row.building_codes.includes(scope)));
        return ok({ongoing:ongoing.map(row=>({work_type:'maintenance',status:'开始',...row})),source_snapshot_ready:false});
      }
      if(type!=='all'){
        const title=({change:'变更',repair:'检修',power:'上下电'})[type];
        return ok({ongoing:[{work_type:type,active_item_id:scope+type,scope,title:scope+'楼隔离'+title,status:'开始',start_time:'2026-11-18 09:30'}],records:[],
          ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
      }
      if(currentAccount.category==='manager'&&!currentAccount.level&&!dailyDesk){
        return ok({ongoing:Object.entries({change:'变更',repair:'检修',power:'上下电',maintenance:'维保'}).map(([work_type,title])=>({
          work_type,active_item_id:scope+work_type,scope,title:scope+'楼隔离'+title,status:'开始',
        })),ongoing_pagination:{total_pages:1}});
      }
      if(dailyDesk){
        const plan=(id,title,work_type,plan_time,owner_level,created_by_open_id)=>({
          record_id:scope+'-'+id,source_record_id:scope+'-'+id,scope,title,work_type,source_progress:'未开始',
          active_item_id:scope+'-active-'+id,status:'开始',specialty:work_type==='change'?'暖通':'电气',
          start_time:plan_time,planned_start_time:plan_time,
          planned_end_time:plan_time.slice(0,10)+' 17:30',
          owner_level,created_by_open_id,
        });
        return ok({ongoing_pagination:{total_pages:1},ongoing:[
          plan('adjust','隔离设备调整','adjust','2026-11-18 15:30','L4','basic-desk'),
          plan('power','UPS 支路上电确认','power','2026-11-18 15:30','L4','basic-desk'),
          plan('change','冷冻水系统运行参数调整','change','2026-11-18 10:00','L4','basic-desk'),
          plan('maintenance','配电柜保护定值复核','maintenance','2026-11-20 09:30','L2','other-person'),
          {...plan('closed','已结束的通告','maintenance','2026-11-20 11:00','L2','other-person'),status:'结束'},
        ],source_snapshot_ready:false});
      }
      const plan=(id,title,source_progress)=>({record_id:id,source_record_id:id,title,work_type:'maintenance',source_progress});
      return ok({ongoing:[{...plan('linked','计划和进行中同一条','未开始'),active_item_id:'active-1',status:'开始'}],ongoing_pagination:{total_pages:1},
        records:planPage===1?[plan('linked','计划和进行中同一条','未开始'),plan('first','本月计划通告','未开始'),plan('completed','已结束的计划','已结束')]:[plan('second','下一页计划通告','未开始')],
        records_pagination:{total_pages:2}});
    }
    if(url.pathname==='/api/repair-management/status'){
      const records=[{record_id:'repair-'+scope,title:scope+'楼隔离维修',building:scope,workflow:'维修中',specialty:'电气',repair_start_time:'2026-10-26 09:29'}];
      if(repairTrial&&scope==='A'){
        assert.equal(url.searchParams.get('period'),'all');
        assert.equal(url.searchParams.get('state'),'all');
        for(let i=1;i<=21;i++)records.push({record_id:'new-'+i,title:'今日测试维修'+i,building:'A',state:'in_progress',
          repair_start_time:String(Date.parse('2026-11-18T09:30:00+08:00')),workflow:'维修中',specialty:'电气'});
        records.push({record_id:'old-repair',title:'跨月待跟进项目',building:'A',state:historyClosed?'completed':'in_progress',
          repair_start_time:'2026-10-26 09:29',latest_followup_time:'2026-11-18 12:20',workflow:'维修完成待回访',specialty:'电气'});
      }
      return ok({total:records.length,records});
    }
    if(url.pathname==='/api/critical-guard/tasks')return ok({tasks:scope==='A'
      ? [{task_id:'guard-live',title:'本楼重保任务',complete:false,status:'published'},{task_id:'guard-done',title:'已完成重保',complete:true,status:'published'}] : []});
    if(url.pathname==='/api/risk-inspections'){
      if(pausedReads&&scope==='A')await new Promise(resolve=>pausedReads.push(resolve));
      return riskDenied || scope==='CAMPUS'
        ? route.fulfill({status:403,json:{ok:false,error:'隔离权限校验'}})
        : ok({total:1,page:1,is_manager:true,items:[{id:'risk-1',name:'隔离专项排查',total:2,approved:1,submitted:1,status:'active'}]});
    }
    if(url.pathname==='/api/risk-inspections/risk-1')return ok({id:'risk-1',name:'隔离专项排查',fields:[],people:[]});
    if(url.pathname==='/api/cabinet-power/batches')return ok({total:2,items:[{batch_id:'cab-live',title:'机柜待确认',is_todo:true},{batch_id:'cab-done',title:'机柜已确认',is_todo:false}]});
    if(url.pathname==='/api/capacity/water/records')return ok({total:waterTrial?1:0,records:waterTrial?[waterRow()]:[],snapshot:{exists:true}});
    if(url.pathname==='/api/capacity/water/bootstrap')return ok({
      scope:'A',options:{meters:['工作台验收水表'],frequencies:['日'],shifts:['白']},
      snapshot:{exists:true},permissions:{can_create:false},
    });
    if(url.pathname==='/api/capacity/water/records/water-home')return ok({...waterRow(),version:'isolated-version',
      photos:[{image_id:'photo-home',name:'验收附件.png'}],edit_policy:{can_edit:true,remaining_edits:2},
    });
    if(url.pathname.startsWith('/api/capacity/water/images/'))return route.fulfill({status:404,body:''});
    if(['/api/drills','/api/drills/bootstrap'].includes(url.pathname))return ok({people:[],items:[
      {drill_id:'drill-done',name:'已完成演练甲',status:'published',execution:{status:'synced'}},
      {drill_id:'drill-live',name:'待完成演练甲',status:'published'},
    ]});
    if(url.pathname==='/api/drills/drill-live/execution')return ok({execution:{status:'draft',version:1}});
    if(url.pathname==='/api/drills/drill-live/preview')return ok({});
    if(url.pathname==='/api/engineer/mop/bootstrap'){
      if(mopDelayed&&url.searchParams.get('view')==='todos')return ok({notices:[],snapshot_pending:true});
      if(mopRetryFailures>0&&scope==='A'&&url.searchParams.get('view')==='todos'){
        mopRetryFailures--;
        return route.fulfill({status:503,json:{ok:false,error:'隔离临时读取失败'}});
      }
      return ok({notices:[
        {notice_key:'mop-live',title:'待处理维护单甲',status:'开始'},{notice_key:'mop-done',title:'已上传维护单甲',mop_binding:{id:'bound'},mop_uploaded:true,status:'开始'}]});
    }
    if(url.pathname==='/api/workbench-events')return ok({items:[{event_id:'event-active',state:'active',metadata:{summary:'进行中事件通告甲'}},{event_id:'event-ended',state:'ended'}]});
    if(url.pathname==='/api/learning/workbench-summary')return ok({scopes:scope==='ALL'?['A','B']:[scope],
      summary:{已完成:4,待答:2,答题中:1,逾期:0,待核对:0,未到时:0}});
    if(url.pathname==='/api/auth/permission-requests/admin')return ok({items:[{request_id:'perm-1',name:'权限待审核人',status:'pending'}]});
    if(url.pathname==='/api/tool-self-check/workspace/history')return ok({sessions:[{id:'tool-session',history_items:[
      {id:'asset-A',asset_name:'工具待审核',scope:'A',order_status:'review_pending'},
      {id:'asset-B',asset_name:'工具已归档',scope:'A',order_status:'submitted',archive_status:'archived'}]}]});
    if(url.pathname==='/api/pre-shift-checks'){
      const item={id:'check-1',text:'隔离检查项目'},server_time='2026-11-18T10:00:00+08:00';
      return ok({scope,scopes:['A'],can_manage:false,roster_ready:!!checkMember,server_time,
        viewer_on_roster:checkMember===currentAccount.open_id,viewer_active_shift:checkMember===currentAccount.open_id?'白':'',
        definition:{items:checkMember?[{...item,frequency:{kind:'shift'}}]:[],version:1},
        tasks:checkMember?[{id:'check',scope,shift:'白',day:'2026-11-18',definition_version:1,
          due:Date.parse(server_time)/1000,deadline:Date.parse('2026-11-18T18:00:00+08:00')/1000,
          items:[item],people:[{open_id:checkMember,name:'隔离值班人员'}],duty_names:['隔离值班人员'],can_submit:true,can_upload_handover:false,submitted:''}]:[]});
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
    if(!embeddedTest&&!paused?.matches(url))unexpected.push(url.pathname);
    return route.fulfill({status:404,json:{ok:false,error:'No fixture'}});
  });
  await page.goto(origin+'/fixture',{timeout:60000});
  await page.waitForFunction(()=>!!window.setAccount);
  const account=(id,category,modules=['event','repair_management','learning'])=>({open_id:id,category,modules,role:'building',home_scope:'A',enabled:true,
    ...(category==='engineer'?{specialties:['电气']}:{}),
  });
  const options=ids=>ids.map(value=>({value,label:value+'楼'}));
  const set=(actor,ids,explicit=true)=>{
    currentAccount=actor;
    return page.evaluate(([user,scopes,explicit])=>window.setAccount(user,scopes,explicit),[actor,options(ids),explicit]);
  };
  await page.evaluate(()=>{
    localStorage.setItem('clipflow:workspace-layout:v1:engineer',JSON.stringify({version:1,widgets:[
      {id:'main',kind:'shortcuts',links:['learning','accounts','learning'],x:0,y:0},
      {id:'tasks',kind:'tasks',taskKinds:['learning'],x:0,y:1},
    ]}));
  });
  await set(account('engineer','other'),['A']);
  await page.getByRole('heading',{name:'待办办理',exact:true}).waitFor();
  await page.waitForFunction(()=>document.querySelector('.wb-shortcut')?.textContent.includes('画像学练'));
  assert.equal(await page.locator('.wb-shortcuts').getByText('账号与权限').count(),0);
  assert.equal(await page.locator('.wb-shortcuts').getByText('画像学练').count(),1);
  assert.equal(await page.locator('.wb-scope select').count(),0);
  const todo=page.getByRole('region',{name:'待办办理',exact:true});
  const tiles=todo.locator('.wb-task-tile');
  const tile=key=>todo.locator('.wb-task-tile[data-group="'+key+'"]');
  const pop=page.getByRole('dialog',{name:'模块待办条目',exact:true});
  const task=name=>pop.getByRole('button',{name:'办理 '+name,exact:true});
  async function reveal(key){
    const control=tile(key);
    await control.waitFor();
    if(await control.getAttribute('aria-expanded')!=='true')await control.click();
    await pop.waitFor();
  }
  async function settleQueue(){
    await page.waitForFunction(()=>document.querySelector('.queue-panel')?.getAttribute('aria-busy')==='false');
  }
  async function absent(key,name){
    await settleQueue();
    if(!await tile(key).count())return;
    await reveal(key);
    assert.equal(await pop.getByText(name,{exact:true}).count(),0,name+' is not available in '+key);
  }
  const dialog=title=>page.getByRole('region',{name:'办理 '+title,exact:true});
  async function closeTask(title){
    const close=dialog(title).getByRole('button',{name:'关闭待办办理'});
    await close.click();
    try{await dialog(title).waitFor({state:'hidden',timeout:4000});}
    catch(error){console.error({title,errors,visible:await dialog(title).innerText(),panels:await page.getByRole('dialog').allTextContents()});throw error;}
    embeddedTest=false;
  }
  async function closeWhileReading(source,title,matches,notice=false){
    embeddedTest=true;
    let started,release;
    const reading=new Promise(resolve=>started=resolve);
    inlineRead={matches,started,until:new Promise(resolve=>release=resolve)};
    try{
      await reveal(source);
      await (notice?pop.getByRole('button',{name:'更新／结束 '+title,exact:true}):task(title)).click();
      const panel=notice?page.locator('.v2-notice-editor'):dialog(title);
      await panel.waitFor({timeout:5000});
      const url=await Promise.race([reading,panel.waitFor({state:'hidden',timeout:5000}).then(()=>null)]);
      assert(url,title+' must start reading before it is closed');
      await closeTask(title);
      return url;
    }finally{inlineRead=undefined;release();}
  }
  await reveal('learning');
  await task('自主练习').waitFor();
  await reveal('repair_management');
  await task('A楼隔离维修').waitFor();
  assert.equal(await page.getByText('每日学练',{exact:true}).count(),0);
  await page.setViewportSize({width:390,height:844});
  embeddedTest=true;
  await reveal('learning');
  await task('自主练习').click();
  await dialog('自主练习').locator('[data-module-widget=learning]').waitFor();
  assert.equal(new URL(page.url()).pathname,'/fixture');
  await closeTask('自主练习');
  await reveal('learning');
  assert.equal(await task('自主练习').count(),1,'opening a todo does not complete it');
  practiceFinished=true;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await task('自主练习').waitFor({state:'hidden'});
  await page.setViewportSize({width:1440,height:1000});
  exempt=false;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await reveal('learning');
  await pop.getByText('每日学练',{exact:true}).waitFor();
  assert.equal(await page.getByText('每日学练',{exact:true}).count(),1,'per-paper daily_exempt still excludes the task');

  const period=page.getByRole('region',{name:'工作状态'});
  await set(account('period-review','other',['daily','event','maintenance','repair','learning']),['A','B']);
  const personalCard=period.getByRole('article',{name:'个人卡片'});
  await period.getByRole('button',{name:'今日',exact:true}).click();
  await personalCard.getByText(/已答.*4 \/ 15/).waitFor();
  const buildingCard=period.getByRole('article',{name:'楼栋卡片'});
  await buildingCard.getByRole('button',{name:/隔离明日截止维保/}).waitFor();
  await buildingCard.getByRole('region',{name:'进行中通告'}).getByText('已发检修通告',{exact:true}).waitFor();
  assert.equal(await period.getByText('隔离已完成计划',{exact:true}).count(),0);
  await period.getByRole('button',{name:'表单显示'}).click();
  assert.equal(await period.locator('.period-grid').getAttribute('data-view'),'table');
  await period.getByRole('button',{name:'卡片显示'}).click();
  const status=period.getByRole('group',{name:'工作状态筛选'});
  await status.getByRole('button',{name:'逾期 1',exact:true}).click();
  await buildingCard.getByRole('button',{name:/跨月未发检修.*已逾期/}).waitFor();
  assert.equal(await period.getByText('隔离逾期维保',{exact:true}).count(),0);
  await status.getByRole('button',{name:'今明截止 1',exact:true}).click();
  await buildingCard.getByRole('button',{name:/隔离明日截止维保.*明日截止/}).waitFor();
  assert.equal(await period.getByText('隔离逾期维保',{exact:true}).count(),0);
  await status.getByRole('button',{name:'未开始 2',exact:true}).click();
  await buildingCard.getByRole('region',{name:'未开始通告'}).getByText('跨月未发检修',{exact:true}).waitFor();
  assert.equal(await buildingCard.getByText('已发检修通告',{exact:true}).count(),0);
  await reveal('notices:维保');
  await task('计划和进行中同一条').waitFor();
  assert.equal(await task('计划和进行中同一条').count(),1);
  await absent('notices:维保','已结束的计划');
  await absent('notices:维保','本月计划通告');
  await pop.press('Escape');
  await pop.waitFor({state:'hidden'});
  await status.getByRole('button',{name:/^进行中 [1-9]/}).click();
  embeddedTest=true;
  const statusNotice=buildingCard.getByRole('button',{name:/计划和进行中同一条/});
  await statusNotice.waitFor({timeout:5000});
  assert.match(await statusNotice.innerText(),/办理/);
  const [noticeDetail]=await Promise.all([
    page.waitForRequest(req=>new URL(req.url()).pathname==='/api/workbench/lite-detail'&&new URL(req.url()).searchParams.get('active_item_id')==='active-1',{timeout:20000}),
    statusNotice.click({timeout:6000}),
  ]);
  assert.equal(new URL(noticeDetail.url()).searchParams.get('scope'),'A');
  assert.equal(new URL(page.url()).pathname,'/fixture');
  await closeTask('计划和进行中同一条');
  await reveal('notices:维保');
  await task('计划和进行中同一条').waitFor();
  assert.equal(await todo.getByRole('button',{name:'读取更多待办'}).count(),0);
  await pop.press('Escape');
  await pop.waitFor({state:'hidden'});
  await status.getByRole('button',{name:/^全部 /}).click();
  await period.getByRole('button',{name:'本周',exact:true}).click();
  await personalCard.getByText(/已答.*9 \/ 30/).waitFor();
  await period.getByRole('button',{name:'本月',exact:true}).click();
  await personalCard.getByText(/已答.*23 \/ 60/).waitFor();
  await buildingCard.getByRole('region',{name:'未开始通告'}).getByText('隔离逾期维保',{exact:true}).waitFor();
  await page.locator('.wb-scope select').selectOption('B');
  await personalCard.getByText('本楼暂无个人题单').waitFor();
  await buildingCard.getByRole('button',{name:/B楼待开始维保/}).waitFor();
  assert.equal(await buildingCard.getByText('隔离明日截止维保',{exact:true}).count(),0);
  await status.getByRole('button',{name:'逾期 0',exact:true}).click();
  await period.getByText('本周期暂无此类事项',{exact:true}).waitFor();
  assert.ok(requests.some(([,url,scope,query])=>url==='/api/workbench'&&scope==='B'&&query.get('sections')==='records'));

  hold=true;
  await set(account('manager','manager'),['A','B']);
  await todo.waitFor();
  const until=Date.now()+5000;
  while(!release&&Date.now()<until)await new Promise(resolve=>setTimeout(resolve,25));
  assert.ok(release,'A request was held to exercise stale responses');
  await page.locator('.wb-scope select').selectOption('B');
  hold=false;release();
  await reveal('events');
  const eventRow=pop.getByText('B楼隔离事件',{exact:true});
  await eventRow.waitFor();
  assert.equal(await page.getByText('A楼隔离事件',{exact:true}).count(),0);
  failure='500';await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await todo.locator('.queue-health summary').waitFor();
  await eventRow.waitFor();
  failure='403';await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  await eventRow.waitFor({state:'hidden'});
  assert.equal(await todo.getByText(/当前无权查看/).count(),0);
  assert.equal(await page.getByText('B楼隔离事件',{exact:true}).count(),0);
  failure='';

  checkMember='operator';
  const hRosterRead=page.waitForResponse(response=>response.url().includes('/api/pre-shift-checks?')&&new URL(response.url()).searchParams.get('scope')==='H');
  await set({...account('h-off','operator',['maintenance']),home_scope:'H',level:'L4'},['H']);
  await hRosterRead;
  assert.equal(await page.locator('.wb-check-panel').count(),0,'an off-duty H operator has no pre-shift card');
  assert.equal(await personalCard.getByRole('button',{name:/班前检查/}).count(),0);
  await set(account('operator','operator',['learning','water']),['A']);
  await page.getByText('值班人员 · A楼 · 白班',{exact:true}).waitFor();
  await page.setViewportSize({width:393,height:852});
  const shiftArea=page.locator('.wb-check-panel');
  await shiftArea.getByRole('button',{name:'提交检查'}).waitFor();
  await personalCard.getByRole('button',{name:/班前检查.*待检查/}).click();
  assert(await shiftArea.evaluate(el=>document.activeElement===el));
  await personalCard.getByRole('button',{name:'打开画像学练'}).waitFor();
  assert.equal(await shiftArea.getByLabel('当班人员').innerText(),'隔离值班人员');
  assert(await shiftArea.evaluate(area=>area.getBoundingClientRect().width>=innerWidth-40 &&
    area.querySelector('.v2-check-task').getBoundingClientRect().width>=innerWidth-70),
    'Mobile shift checks and form body use the available width');
  await page.setViewportSize({width:1440,height:1000});
  const constrained=page.locator('.wb-home');
  await constrained.evaluate(area=>{area.style.maxWidth='480px';});
  const bounds=await constrained.evaluate(area=>{
    const main=area.querySelector('.desk-content').getBoundingClientRect(), aside=area.querySelector('.day-planner').getBoundingClientRect();
    return {taskWidth:main.width,asideTop:aside.top,taskBottom:main.bottom};
  });
  assert(bounds.taskWidth>=475 && bounds.asideTop>=bounds.taskBottom,
    'The workbench switches to one column when its container is narrow');
  await constrained.evaluate(area=>{area.style.removeProperty('max-width');});
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

  const full=['event','repair_management','learning','maintenance','change','repair','adjust','power','risk_inspection','critical_guard','cabinet','drills','engineer_mop','daily','water','accounts','signatures'];
  await set(account('campus-dashboard','manager',full),['A','B'],false);
  await reveal('repair_management');
  await task('B楼隔离维修').waitFor();
  await reveal('repair_management');
  await task('A楼隔离维修').waitFor();
  await period.getByText(/已完成.*4 \/ 7/).waitFor();
  await reveal('notices:变更');
  await pop.getByText('A楼隔离变更',{exact:true}).waitFor();
  await reveal('notices:上下电');
  await pop.getByText('B楼隔离上下电',{exact:true}).waitFor();
  for(const key of ['notices:变更','notices:上下电']){
    await reveal(key);assert.equal(await pop.getByRole('button',{name:/办理 /}).count(),0);
  }
  await reveal('notices:变更');
  await pop.getByRole('button',{name:'查看 A楼隔离变更'}).click();
  const readOnlyNotice=page.getByRole('region',{name:'查看 A楼隔离变更',exact:true});
  await readOnlyNotice.getByText('开始时间',{exact:true}).waitFor();
  await readOnlyNotice.getByRole('button',{name:'关闭只读详情'}).click();
  await readOnlyNotice.waitFor({state:'hidden'});
  await absent('notices:维保','本月计划通告');
  await absent('critical_guard','已完成重保');
  await todo.getByLabel('搜索待办').fill('本楼重保任务');
  await reveal('critical_guard');
  await pop.getByText('本楼重保任务',{exact:true}).waitFor();
  await todo.getByLabel('搜索待办').fill('');
  assert.equal(await page.getByText('冷源运行状态',{exact:true}).count(),0);
  assert.equal(await page.locator('.wb-shortcuts').getByText('通告办理').count(),0);
  await page.locator('.wb-scope select').selectOption('B');
  await reveal('notices:变更');
  await pop.getByText('B楼隔离变更',{exact:true}).waitFor();
  await absent('notices:变更','A楼隔离变更');
  await absent('critical_guard','本楼重保任务');

  await set(account('building-dashboard','supervisor',full),['A','B']);
  await reveal('cabinet');
  await task('机柜待确认').waitFor();
  await period.getByText(/已完成.*4 \/ 7/).waitFor();
  assert.equal(await page.locator('.wb-scope select').count(),0);
  await absent('cabinet','机柜已确认');
  await absent('repair_management','B楼隔离维修');
  embeddedTest=true;
  await reveal('cabinet');
  await task('机柜待确认').click();
  await dialog('机柜待确认').locator('[data-module-widget=cabinet]').waitFor();
  await closeTask('机柜待确认');
  await closeWhileReading('critical_guard','本楼重保任务',url=>url.pathname==='/api/critical-guard/bootstrap');
  await closeWhileReading('risk_inspection','隔离专项排查',url=>url.pathname==='/api/risk-inspections/risk-1');

  await set(account('engineer-dashboard','engineer',full),['A']);
  await reveal('risk_inspection');
  await task('隔离专项排查').waitFor();
  await reveal('repair_management');
  await task('A楼隔离维修').waitFor();
  await reveal('events');await task('A楼隔离事件').waitFor();

  const dutyReadStart=requests.length;
  await set(account('shared-desk','duty',full),['A']);
  await reveal('notices:维保');
  await pop.getByRole('button',{name:'更新／结束 计划和进行中同一条',exact:true}).waitFor();
  await reveal('events');
  await pop.getByText('进行中事件通告甲',{exact:true}).waitFor();
  await reveal('notices:维保');
  assert.equal(await pop.getByText('计划和进行中同一条',{exact:true}).count(),1);
  await reveal('drills');
  await task('待完成演练甲').waitFor();
  await reveal('engineer_mop');
  await task('待处理维护单甲').waitFor();
  assert.equal(await tiles.last().getAttribute('data-source'),'drills');
  assert.ok(requests.slice(dutyReadStart).some(([,url,,params])=>url==='/api/engineer/mop/bootstrap'&&params.get('view')==='todos'));
  await absent('drills','已完成演练甲');
  await absent('engineer_mop','已上传维护单甲');
  await personalCard.getByRole('button',{name:'打开画像学练'}).waitFor();
  assert.ok(!requests.slice(dutyReadStart).some(([,url])=>url==='/api/learning/papers'));
  await closeWhileReading('notices:维保','计划和进行中同一条',url=>url.pathname==='/api/workbench/lite-detail',true);

  const drillReads=requests.length;
  const drillPath='/api/drills/drill-live/execution';
  const drillExecution=await closeWhileReading('drills','待完成演练甲',url=>url.pathname===drillPath);
  assert.equal(drillExecution.searchParams.get('scope'),'A');
  await closeWhileReading('engineer_mop','待处理维护单甲',url=>url.pathname==='/api/engineer/mop/bootstrap'&&url.searchParams.get('view')!=='todos');
  embeddedTest=true;
  await reveal('drills');
  await task('待完成演练甲').click();
  const drillPanel=dialog('待完成演练甲');
  const drillDate=drillPanel.locator('.execution-panel input[type=date]:enabled');
  await drillDate.fill('2026-11-19');
  const switchDrill=drillPanel.getByRole('button',{name:'切换待办'});
  const choices=drillPanel.getByRole('region',{name:'选择另一条待办'});
  await switchDrill.click();
  await choices.getByRole('searchbox').fill('待处理维护单甲');
  await choices.getByRole('button',{name:/待处理维护单甲/}).click();
  await page.getByRole('button',{name:'继续编辑',exact:true}).click();
  assert.equal(await drillDate.inputValue(),'2026-11-19','An unsaved drill remains open with its draft');
  await switchDrill.click();
  await choices.getByRole('button',{name:/待处理维护单甲/}).click();
  await page.getByRole('button',{name:'放弃修改并继续',exact:true}).click();
  await drillPanel.waitFor({state:'hidden'});
  assert.equal(await page.getByRole('dialog',{name:'待办办理窗口',exact:true}).count(),1);
  await dialog('待处理维护单甲').locator('[data-module-widget=engineer_mop]').waitFor();
  await closeTask('待处理维护单甲');
  assert.equal(new URL(page.url()).pathname,'/fixture');
  assert.ok(!requests.slice(drillReads).some(([,url])=>url.includes('/drill-done/')));

  await set({...account('admin-dashboard','administrator',full),tool_management_enabled:true,can_manage_accounts:true},['A']);
  await reveal('tool_management');
  await task('工具待审核').waitFor();
  await reveal('accounts');
  await task('权限待审核人').waitFor();
  await absent('tool_management','工具已归档');

  for(const [profile,category,scopes] of [
    ['manager','manager',['A','B']],['supervisor','supervisor',['A']],
    ['engineer','engineer',['A']],['duty','duty',['A']],
    ['specialist','engineer',['A','B']],['administrator','administrator',['A','B']],
  ]){
    await set(account(profile,category),scopes);
    for(const width of [1440,1024,800,390]){
      await page.setViewportSize({width,height:1000});
      await page.waitForFunction(()=>document.querySelector('.wb-home'));
      const overflow=await page.evaluate(()=>{
        if(document.documentElement.scrollWidth<=innerWidth)return [];
        return [...document.querySelectorAll('body *')].filter(el=>el.getBoundingClientRect().right>innerWidth+1)
          .slice(0,8).map(el=>el.tagName+'.'+el.getAttribute('class')+': '+Math.round(el.getBoundingClientRect().right));
      });
      assert.deepEqual(overflow,[],profile+' overflow at '+width);
      if(width===1440||width===390){
        await page.evaluate(()=>window.scrollTo(0,0));
        await page.screenshot({path:path.join(output,profile+'-'+width+'.png'),animations:'disabled'});
      }
    }
  }
  dailyDesk=true;
  holdAgenda=true;
  mopDelayed=true; riskDenied=true;
  await page.setViewportSize({width:1440,height:1000});
  await set({...account('global-desk','manager',full),name:'陈晨',level:'L0',can_manage_accounts:true},['A','B'],false);
  await page.getByRole('region',{name:'交接班人员'}).getByRole('rowheader',{name:'B楼'}).waitFor();
  holdAgenda=false;
  releaseAgenda?.();
  const health=todo.locator('.desk-queue-health');
  await health.getByText(/类待办待更新/).waitFor();
  assert.equal(await health.getAttribute('open'),null);
  assert.equal(await todo.getByText(/明细尚未读全|当前无权查看|数据待更新/).count(),0);
  await health.locator('summary').click();
  await health.getByRole('button',{name:'重试待处理维保单'}).waitFor();
  assert.equal(await health.getByText('专项排查待办',{exact:true}).count(),0);
  mopDelayed=false; riskDenied=false; mopRetryFailures=1;
  await health.getByRole('button',{name:'重试待处理维保单'}).click();
  await reveal('engineer_mop');
  await task('待处理维护单甲').first().waitFor();
  assert.equal(mopRetryFailures,0);
  const overview=page.getByRole('region',{name:'工作状态',exact:true});
  await overview.getByRole('heading',{name:'画像学练情况汇总',exact:true}).waitFor();
  await overview.getByText(/已完成.*4 \/ 7/).waitFor();
  await settleQueue();
  await reveal('notices:设备调整');
  await pop.getByText('隔离设备调整',{exact:true}).first().waitFor();
  await pop.press('Escape');await pop.waitFor({state:'hidden'});
  assert(await tile('notices:设备调整').evaluate(el=>document.activeElement===el));
  assert.equal(await page.getByText('全域待办总数',{exact:true}).count(),0);
  assert.equal(await page.locator('.wb-check-panel').count(),0);
  const agenda=page.getByRole('complementary',{name:'我的日程'});
  await reveal('notices:变更');
  await pop.getByText('冷冻水系统运行参数调整',{exact:true}).first().waitFor();
  assert.equal(await page.getByRole('searchbox',{name:'全局搜索'}).count(),0);
  const weatherKey=page.getByRole('button',{name:'查看今日天气和未来10小时气温'});
  await weatherKey.click();
  const weather=page.getByRole('dialog',{name:'今日天气和未来10小时气温'});
  await weather.getByText('16°C',{exact:true}).waitFor();
  const trend=weather.getByRole('group',{name:/未来 10 小时气温趋势/});
  await trend.waitFor();
  assert.equal(await trend.locator('.weather-point').count(),10,'each of the coming ten hours has exactly one point');
  const firstPoint=await trend.locator('.weather-point').first().boundingBox();
  await page.mouse.move(firstPoint.x+firstPoint.width/2,firstPoint.y+firstPoint.height/2);
  await trend.getByText('12:00 · 16°C',{exact:true}).waitFor();
  await trend.focus();
  await trend.press('ArrowRight');
  await trend.getByText('13:00 · 17°C',{exact:true}).waitFor();
  await trend.press('Escape');
  await trend.locator('output').waitFor({state:'detached'});
  assert.equal(await page.getByRole('button',{name:'天气详情'}).count(),0);
  const roster=agenda.getByRole('region',{name:'交接班人员'});
  await roster.getByRole('rowheader',{name:'A楼'}).waitFor();
  await roster.getByRole('rowheader',{name:'B楼'}).waitFor();
  const intern=roster.getByRole('button',{name:'查看隔离长白实习生的基本信息（长白实习生）'});
  assert(await intern.evaluate(el=>el.classList.contains('is-intern')));
  await roster.getByRole('button',{name:'卡片显示',exact:true}).click();
  await roster.locator('[data-view=cards]').getByText('接班人员',{exact:true}).first().waitFor();
  await intern.click();
  const internDialog=page.getByRole('dialog',{name:'隔离长白实习生的基本信息'});
  await internDialog.getByText('长白',{exact:true}).waitFor();
  assert.equal(await internDialog.getByRole('link',{name:'飞书对话'}).getAttribute('href'),'https://applink.feishu.cn/client/chat/open?openId=ou_fixtureIntern');
  await page.locator('.title-line h1').click();
  await roster.getByRole('button',{name:'表单显示',exact:true}).click();
  assert.equal(await roster.getByRole('table').getByRole('columnheader').count(),3);
  await roster.getByRole('button',{name:'查看验收值班员的基本信息'}).click();
  const person=page.getByRole('dialog',{name:'验收值班员的基本信息'});
  await person.getByText('A班',{exact:true}).waitFor();
  assert.equal(await person.getByRole('link',{name:'飞书对话'}).getAttribute('href'),'https://applink.feishu.cn/client/chat/open?openId=ou_fixtureA1');
  await page.locator('.title-line h1').click();
  await reveal('notices:变更');
  await pop.getByRole('region',{name:'A楼待办'}).waitFor();
  await pop.getByRole('region',{name:'B楼待办'}).waitFor();
  await pop.getByRole('button',{name:'卡片显示'}).click();
  assert.equal(await pop.locator('.queue-list').getAttribute('data-view'),'cards');
  await pop.press('Escape');
  const beforeReturn=requests.length;
  await page.evaluate(()=>window.showWorkbench(false));
  await todo.waitFor({state:'detached'});
  await page.evaluate(()=>window.showWorkbench(true));
  await todo.waitFor();
  await roster.getByRole('rowheader',{name:'B楼'}).waitFor();
  assert.equal(requests.length,beforeReturn,'returning to the workbench reuses loaded data within the freshness window');
  await page.getByLabel('工作台楼栋').selectOption('B');
  await agenda.getByRole('button',{name:/2026-11-18.*项日程/}).waitFor();
  await roster.getByText('B楼当晚值班员',{exact:true}).waitFor();
  await page.getByLabel('工作台楼栋').selectOption('A');
  const futureDate=agenda.getByRole('button',{name:/^2026-11-20/});
  await futureDate.hover();
  const preview=page.getByRole('region',{name:'2026-11-20 通告预览'});
  await preview.getByText('配电柜保护定值复核',{exact:true}).waitFor();
  assert.equal(await preview.getByText('已结束的通告').count(),0);
  assert.equal(await preview.getByText(/B楼/).count(),0);
  assert.equal(await futureDate.getAttribute('aria-pressed'),'false');
  await agenda.getByRole('button',{name:/^2026-11-19/}).click();
  await agenda.getByRole('heading',{name:/11 \/ 19/}).waitFor();
  assert.equal(await agenda.getByLabel('选中日期天气').count(),0);
  const aRoster=roster.getByRole('row').filter({has:page.getByRole('rowheader',{name:'A楼',exact:true})});
  await aRoster.getByRole('cell').nth(0).getByText('验收值班员',{exact:true}).waitFor();
  assert.equal(await aRoster.getByText('验收值班员',{exact:true}).count(),1);
  await aRoster.getByRole('cell').nth(1).getByText('夜班验收人',{exact:true}).waitFor();
  assert.equal(await roster.getByText(/明日值班员|B楼当晚值班员/).count(),0);
  await agenda.getByRole('button',{name:'展开月历',exact:true}).click();
  await agenda.getByRole('button',{name:'上个月',exact:true}).click();
  await agenda.getByRole('button',{name:/^2026-10-01/}).waitFor();
  await aRoster.getByText('验收值班员',{exact:true}).waitFor();
  await page.getByLabel('工作台楼栋').selectOption('B');
  await roster.getByText('B楼当晚值班员',{exact:true}).waitFor();
  assert.equal(await roster.getByText('验收值班员',{exact:true}).count(),0);
  await page.getByLabel('工作台楼栋').selectOption('A');
  await agenda.getByRole('button',{name:'今天',exact:true}).click();
  await agenda.getByText('隔离封网安排',{exact:true}).waitFor();
  const sealFilter=overview.getByRole('button',{name:'封网 1',exact:true});
  await sealFilter.click();
  const sealList=overview.getByLabel('周期内封网');
  await sealList.getByText('封网中',{exact:true}).waitFor();
  assert.equal(await sealList.getByRole('button').count(),0,'freeze windows are informational');
  assert.equal(await overview.getByText('隔离封网安排',{exact:true}).count(),1);
  await overview.getByRole('button',{name:/^全部 /}).click();
  await agenda.getByText('隔离智航风险',{exact:true}).waitFor();
  const openAgenda=agenda.getByRole('button',{name:/展开全部 .* 项日程/});
  const railHeight=(await agenda.boundingBox()).height;
  const agendaDialog=page.getByRole('dialog',{name:'当天全部日程'});
  await openAgenda.click();
  await agendaDialog.waitFor();
  await agendaDialog.getByText('隔离晚间事项 18',{exact:true}).waitFor();
  assert.equal(await agenda.getByRole('region',{name:'所选日期日程'}).locator('.timeline li').count(),3);
  assert(await agendaDialog.locator('.agenda-scroll').evaluate(el=>el.scrollHeight>el.clientHeight&&getComputedStyle(el).overflowY==='auto'));
  assert.ok(Math.abs((await agenda.boundingBox()).height-railHeight)<3,'the full agenda does not extend the calendar rail');
  await page.setViewportSize({width:390,height:844});
  const agendaBounds=await agendaDialog.boundingBox();
  await page.screenshot({path:path.join(output,'agenda-mobile.png'),animations:'disabled'});
  assert(agendaBounds.x>=8&&agendaBounds.x+agendaBounds.width<=382&&agendaBounds.height<844,'the agenda dialog fits a phone '+JSON.stringify(agendaBounds));
  await agendaDialog.press('Escape');
  await agendaDialog.waitFor({state:'hidden'});
  await page.setViewportSize({width:1440,height:1000});
  embeddedTest=true;
  await openAgenda.click();
  const scheduledDetail=page.waitForRequest(request=>new URL(request.url()).pathname==='/api/workbench/lite-detail');
  await agendaDialog.getByRole('button',{name:'冷冻水系统运行参数调整',exact:true}).click();
  const detailQuery=new URL((await scheduledDetail).url()).searchParams;
  assert.equal(detailQuery.get('active_item_id'),'A-active-change');
  assert.equal(detailQuery.get('scope'),'A');
  await closeTask('冷冻水系统运行参数调整');
  await agenda.getByText('验收值班员',{exact:true}).waitFor();
  assert.equal(await agenda.getByText(/未来 12 小时气温/).count(),0);
  assert.equal(await agenda.getByText('16°C',{exact:true}).count(),0,'temperature is only shown in the masthead');
  await agenda.getByText('大风预警',{exact:true}).waitFor();
  await agenda.getByText('戒备等级 三级',{exact:true}).waitFor();
  await page.getByLabel('工作台楼栋').selectOption('ALL');
  await roster.getByRole('rowheader',{name:'B楼'}).waitFor();
  await page.setViewportSize({width:390,height:844});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'workbench fits a phone viewport');
  const badge=await page.locator('.notice-count').boundingBox(), bell=await page.locator('.notice-count').locator('..').boundingBox();
  assert(badge&&bell&&badge.x+badge.width<=bell.x+bell.width+1&&badge.x+badge.width<=390,'unread badge stays inside the bell and the 390px viewport');
  await page.mouse.move(0,0);
  await page.screenshot({path:path.join(output,'daily-mobile.png'),animations:'disabled',fullPage:true});
  await page.setViewportSize({width:1440,height:1320});
  await page.mouse.move(5,5);
  await preview.waitFor({state:'hidden'});
  await page.evaluate(()=>{
    document.activeElement?.blur();
    document.querySelector('#app').style.padding='18px 22px';
    window.scrollTo(0,0);
  });
  await page.screenshot({path:path.join(output,'daily-desktop.png'),animations:'disabled'});
  await page.evaluate(()=>{document.querySelector('#app').style.padding='';});
  await page.setViewportSize({width:1440,height:1000});
  await page.getByLabel('工作台楼栋').selectOption('A');
  await page.locator('.wb-scope select').selectOption('B');
  await roster.getByText('B楼当晚值班员',{exact:true}).waitFor();
  await agenda.getByText('封网表未接入',{exact:true}).waitFor();
  assert.equal(await agenda.getByText('隔离封网安排',{exact:true}).count(),0,'switching the workbench building changes the calendar');
  await page.getByRole('navigation',{name:'常用业务'}).getByRole('button',{name:'维修项目',exact:true}).click();
  assert.equal(new URL(await page.evaluate(()=>window.opened),origin).searchParams.get('scope'),'B');
  await absent('repair_management','A楼隔离维修');
  await reveal('repair_management');
  await task('B楼隔离维修').waitFor();
  await page.locator('.wb-scope select').selectOption('A');
  await page.getByRole('button',{name:/消息通知/}).click();
  await page.getByRole('button',{name:'打开未读清单'}).click();
  assert.equal(await todo.getByRole('button',{name:/^未读/}).getAttribute('aria-pressed'),'true');
  await closeWhileReading('repair_management','A楼隔离维修',url=>url.pathname==='/api/repair-management/records'&&url.searchParams.get('focus_record_id')==='repair-A');
  assert.equal(new URL(page.url()).pathname,'/fixture');
  await absent('repair_management','A楼隔离维修');
  await todo.getByRole('button',{name:'全部',exact:true}).click();
  await reveal('repair_management');
  await task('A楼隔离维修').waitFor();

  await set({...account('basic-desk','other',['maintenance','change','power','repair_management']),level:'L4'},['A','B']);
  await reveal('notices:变更');
  await pop.getByText('冷冻水系统运行参数调整',{exact:true}).waitFor();
  await absent('notices:维保','配电柜保护定值复核');
  assert.equal(await page.locator('.wb-scope select').count(),0);
  assert.equal(await todo.getByRole('button',{name:'删除单据'}).count(),0);
  await set({...account('engineer-desk','engineer',full),level:'L2'},['A','B'],false);
  await reveal('notices:维保');
  await pop.getByText('配电柜保护定值复核',{exact:true}).waitFor();
  await absent('notices:变更','冷冻水系统运行参数调整');
  await reveal('events');await task('A楼隔离事件').waitFor();
  await absent('events','A楼暖通事件');
  await set({...account('hvac-engineer','engineer',full),level:'L2',specialties:['暖通']},['A','B'],false);
  await reveal('notices:变更');
  await pop.getByText('冷冻水系统运行参数调整',{exact:true}).waitFor();
  await absent('notices:维保','配电柜保护定值复核');
  await reveal('events');await task('A楼暖通事件').waitFor();
  await absent('events','A楼隔离事件');
  await set({...account('supervisor-desk','supervisor',full),level:'L3'},['A','B'],false);
  await reveal('notices:变更');
  await pop.getByText('冷冻水系统运行参数调整',{exact:true}).waitFor();
  assert.equal(await page.locator('.wb-scope select').count(),0);
  await reveal('notices:维保');await task('配电柜保护定值复核').waitFor();
  await absent('repair_management','B楼隔离维修');
  await set({...account('manager-desk','manager',full),level:'L1',can_manage_accounts:true},['A','B'],false);
  await overview.getByRole('heading',{name:'画像学练情况汇总'}).waitFor();
  assert.equal(await page.getByRole('heading',{name:/交接待审|账号权限待审批/}).count(),0);
  await set({...account('h-duty','duty',full),home_scope:'H',level:'L4'},['H']);
  await reveal('ali_confirmation');
  await pop.getByRole('button',{name:'确认截图 本日变更截图待确认'}).waitFor();
  await absent('ali_confirmation','已确认变更');
  await set({...account('h-duty-admin','duty',full),home_scope:'H',role:'admin',level:'L4'},['A','H'],false);
  await overview.getByRole('heading',{name:'画像学练情况汇总'}).waitFor();
  await set({...account('d-duty','duty',full),home_scope:'D',level:'L4'},['D','E']);
  await reveal('inspection');
  await pop.getByText('D楼白班第 2 轮巡检',{exact:true}).waitFor();
  await personalCard.getByRole('button',{name:/D楼白班第 2 轮巡检/}).waitFor();
  await personalCard.getByRole('button',{name:/班前检查/}).waitFor();
  await personalCard.getByRole('button',{name:'打开画像学练'}).waitFor();
  assert.equal(await page.locator('.wb-scope select').count(),0);
  checkMember='d-operator';
  await set({...account(checkMember,'operator',full),home_scope:'D',level:'L4'},['D']);
  await personalCard.getByRole('button',{name:/D楼白班第 2 轮巡检/}).waitFor();
  const offDutyRead=page.waitForResponse(response=>response.url().includes('/api/pre-shift-checks?')&&new URL(response.url()).searchParams.get('scope')==='D');
  await set({...account('d-off','operator',full),home_scope:'D',level:'L4'},['D']);
  await offDutyRead;
  assert.equal(await personalCard.getByRole('button',{name:/班前检查|轮巡检/}).count(),0);
  checkMember='';
  waterTrial=true;
  await set({...account('water-desk','other',['water']),level:'L4'},['A']);
  await reveal('water');
  const recentQuery=requests.find(([_,url,scope,q])=>url==='/api/capacity/water/records'&&scope==='A'&&q.get('latest_only')==='1')?.[3];
  assert.equal(recentQuery?.get('start_date'),'2026-10-20');
  assert.equal(recentQuery?.get('end_date'),'2026-11-18');
  await closeWhileReading('water','工作台验收水表',url=>url.pathname==='/api/capacity/water/records/water-home');
  await reveal('water');
  await task('工作台验收水表').click();
  const input=dialog('工作台验收水表').getByLabel('本次水表读数',{exact:true});
  await input.fill('1250');
  await input.press('Enter');
  await dialog('工作台验收水表').getByText('隔离保存失败',{exact:false}).waitFor();
  assert.equal(await input.inputValue(),'1250');
  await input.press('Enter');
  await dialog('工作台验收水表').locator('.water-notice').filter({hasText:'已保存'}).waitFor();
  assert.equal(waterWrites.length,2);
  assert.equal(waterWrites[1].meter_value,1250);
  await closeTask('工作台验收水表');
  await tile('water').waitFor({state:'detached'});
  waterTrial=false;
  repairTrial=true;
  await page.setViewportSize({width:1440,height:900});
  await set(account('repair-desk','engineer',['repair_management']),['A']);
  const list=pop.getByRole('region',{name:'待办清单',exact:true});
  await tile('repair_management').waitFor();
  await settleQueue();
  const documentHeight=()=>page.evaluate(()=>document.documentElement.scrollHeight);
  const heightBeforeList=await documentHeight();
  await reveal('repair_management');
  await task('跨月待跟进项目').waitFor();
  assert(await list.evaluate(el=>el.scrollHeight>el.clientHeight),'a long group scrolls inside the popover');
  assert.equal(await documentHeight(),heightBeforeList,'a long group must not extend the document');
  await todo.getByRole('button',{name:'今日',exact:true}).click();
  await reveal('repair_management');
  await task('今日测试维修21').waitFor();
  assert.equal(await list.getByRole('button',{name:/办理 今日测试维修/}).count(),21,'today view includes all loaded repair tasks');
  await absent('repair_management','跨月待跟进项目');
  await todo.getByRole('button',{name:/^历史未完维修/}).click();
  await absent('repair_management','今日测试维修1');
  embeddedTest=true;
  const oldProjectRequest=page.waitForRequest(request=>{
    const url=new URL(request.url());
    return url.pathname==='/api/repair-management/records'&&url.searchParams.get('focus_record_id')==='old-repair';
  });
  await reveal('repair_management');
  await task('跨月待跟进项目').click();
  assert.equal(new URL((await oldProjectRequest).url()).searchParams.get('scope'),'A');
  historyClosed=true;
  await closeTask('跨月待跟进项目');
  await todo.locator('.wb-task-tile[data-group="repair_management"][data-count="1"]').waitFor();
  await absent('repair_management','跨月待跟进项目');
  repairTrial=false;

  aggregateTrial=true;
  const scopePicker=page.getByLabel('工作台楼栋',{exact:true});
  const todayButton=todo.getByRole('button',{name:'今日',exact:true});
  await set({...account('both-aggregate','administrator',['maintenance']),level:'L0'},['CAMPUS','A','B','ALL']);
  for(const scope of ['CAMPUS','ALL','A']){
    await scopePicker.selectOption(scope);
    await todayButton.click();
    await reveal('notices:维保');
    await task('A楼今日进行中').waitFor();
    await task('跨楼进行中').waitFor();
    assert.equal(await task('B楼前期仍未结束').count(),scope==='A'?0:1);
    assert.equal(await task('已结束的旧通告').count(),0);
    assert.equal(await task('次日才开始的通告').count(),0);
  }
  await set({...account('scoped-aggregate','manager',['maintenance']),level:'L1'},['CAMPUS','A','D']);
  await todayButton.click();
  await reveal('notices:维保');
  await task('A楼今日进行中').waitFor();
  await task('跨楼进行中').waitFor();
  await absent('notices:维保','B楼前期仍未结束');
  pausedReads=[];
  try {
    await set({...account('slow-sources','supervisor',['maintenance','event','risk_inspection']),level:'L3'},['A']);
    await reveal('notices:维保');
    await task('A楼今日进行中').waitFor({timeout:4000});
    assert(pausedReads.length>=2,'a slower event and risk request are still in flight when notices appear');
  } finally {
    const reads=pausedReads;
    pausedReads=undefined;
    reads.forEach(resolve=>resolve());
  }
  aggregateTrial=false;
  lateForecast=true;
  await page.clock.setFixedTime(new Date('2026-11-18T14:30:00Z'));
  await set(account('visual-check','manager'),['A','B']);
  await page.setViewportSize({width:390,height:844});
  await page.evaluate(()=>{
    document.documentElement.dataset.v2Theme='dark';
    document.querySelector('.workbench-fixture-account').style.paddingLeft='205px';
  });
  await weatherKey.click();
  await trend.waitFor();
  await page.waitForFunction(()=>{
    const panel=document.querySelector('.workbench-forecast');
    const rect=panel?.getBoundingClientRect();
    return rect&&rect.left>=7&&rect.right<=innerWidth-7;
  });
  assert.equal(await trend.locator('.weather-point').count(),10,'the hourly window crosses midnight');
  const midnight=await trend.locator('.weather-point').nth(1).boundingBox();
  await trend.dispatchEvent('pointerdown',{pointerType:'touch',clientX:midnight.x+midnight.width/2,clientY:midnight.y+midnight.height/2});
  await trend.getByText('次日 00:00 · 13°C',{exact:true}).waitFor();
  missingWeather=true;
  await set(account('weather-gap','manager'),['A','B']);
  await weather.waitFor({state:'hidden'});
  await weatherKey.click();
  await page.waitForFunction(()=>document.querySelectorAll('.weather-plot .weather-point').length===9);
  assert.equal(await trend.locator('.weather-point').count(),9,'missing hours are not estimated');
  assert.equal(await trend.locator('polyline').count(),2,'a missing hour leaves a gap in the line');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  assert.ok(await tiles.first().evaluate(element=>element.getBoundingClientRect().height>=44));
  assert.deepEqual(requests.filter(([method])=>method!=='GET').map(([method,url])=>[method,url]),[
    ['PATCH','/api/capacity/water/records/water-home'],['PATCH','/api/capacity/water/records/water-home'],
  ]);
  assert.deepEqual(unexpected,[]);
  assert.deepEqual(errors,[]);
  const before=await readFile(path.join(root,'src/workspace-v2/HomePage.vue'),'utf8');
  const shell=await readFile(path.join(root,'src/workspace-v2/Workspace.vue'),'utf8');
  assert.match(before,/<BusinessTodos /);assert.match(shell,/<KeepAlive[^>]*>\s*<WorkbenchHome /);
  console.log('Workbench roles, responsive layouts, todo filters and pagination, exact record dispatch and completion refresh passed.');
} finally { await browser?.close(); await server.close(); }
