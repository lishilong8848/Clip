import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { adaptBrowserReferences } from './workspace-build-adapter.mjs';
import { checkCustomization } from './check-workspace-custom-flow.mjs';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/workspace-v2');
await mkdir(output, { recursive: true });
const sample = `const a = window.location.pathname; const b = 'window.location.href'; const c = obj.location.search; window.history.replaceState({}, '', '/learning'); window.open('/signature');`;
const adapted = adaptBrowserReferences(sample, 'test.ts', '/adapter.ts');
assert.match(adapted, /__v2Location.pathname/);
assert.match(adapted, /'window.location.href'/);
assert.match(adapted, /obj.location.search/);
assert.match(adapted, /__v2History.replaceState/);
assert.match(adapted, /__v2Open/);
const shadowed=adaptBrowserReferences(`const history=ref([]); const location={name:'local'}; function f(window){return window.location.name + history.value + location.name;} window.history.back();`,'shadow.ts','/browser.ts');
assert.match(shadowed,/history.value/);assert.match(shadowed,/location.name/);assert.match(shadowed,/window.location.name/);assert.match(shadowed,/__v2History.back/);
const chatAdapter=adaptBrowserReferences('function isNearBottom(){return thread.value.scrollTop<80;} function scrollBottom(){target.scrollTop=target.scrollHeight;} const path=location.pathname;','/components/LighthouseAssistant.vue?vue&type=script&setup=true&lang.ts','/browser.ts');assert.match(chatAdapter,/__v2ShouldFollow/);assert.match(chatAdapter,/__v2FollowBottom/);
const eventAdapter=adaptBrowserReferences('function returnToOverview(){ loadEvents(); } const title="事件态势与楼栋入口";','/components/EventManagementPage.vue','/browser.ts');assert.doesNotMatch(eventAdapter,/__v2ReturnHome/);assert.match(eventAdapter,/"事件管理"/);assert.match(eventAdapter,/loadEvents\(\)/);
const manifest = JSON.parse(await readFile(path.join(root, 'dist/assets/workspace-v2/entry.json'), 'utf8'));
assert.match(manifest.entry, /^app-.*\.js$/);
for (const name of [manifest.entry, ...manifest.css]) await readFile(path.join(root, 'dist/assets/workspace-v2', name));
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const entry = '/assets/workspace-v2/index.html';
const browser = await chromium.launch({ headless: true });
const errors = [], requests = [], writes = [];
const layouts=layoutApiFixture();
let user = { open_id:'ou_v2_fixture', name:'验收管理员', role:'admin', level:'L1', category:'manager', modules:['event','maintenance','change','repair','polling','adjust','power','repair_management','learning','signatures','accounts','water','cabinet','daily','drills','critical_guard','plan_convergence','links','knowledge','engineer_mop','life_guide','handover'] };
let scopes = [{value:'ALL',label:'全部授权楼栋'}, ...['A','B','C','D','E','H','110'].map(value=>({value,label:value==='110'?'110站':value+'楼'}))];
let loggedIn = true, failOverview = false, failTasks = false;
let noticeFailure='';
const expectedNoticeErrors=new Set();
let assistantTurn=null;
let permissionApplication=null,permissionNotifications=0;
const fullModules=[...user.modules];
const overview = Object.fromEntries(scopes.map(({value},i)=>[value, { event_total:i?3:21,event_processing:i?1:3,
  maintenance_pending:i?12:72,maintenance_ongoing:i?2:12,change_pending:i?2:12,change_ongoing:i?1:6,
  repair_pending:2,repair_ongoing:1,polling_pending:1,polling_ongoing:0,adjust_pending:1,adjust_ongoing:0,power_pending:0,power_ongoing:0,
  ongoing_titles:[],ongoing_title_count:i?4:19 }]));
const noticeFields = `<form id="lite-notice-form"><input name="scope" type="hidden" value="A"><input name="work_type" type="hidden" value="maintenance"><input name="notice_type" type="hidden" value="维保通告"><input name="source_record_id" type="hidden" value="plan_fixture"><input name="building_codes" type="checkbox" value="A" checked><label><span>名称</span><input name="title" required value="隔离测试通告"></label><label><span>通告内容</span><textarea name="content" required></textarea></label><button name="submit_action" value="start">发送开始</button></form>`;
async function context() {
  const context=await browser.newContext({ viewport:{width:1440,height:1000},reducedMotion:process.argv.includes('--no-motion')?'reduce':'no-preference' });
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin && !['data:','blob:'].includes(url.protocol))return route.abort();
    if(url.pathname==='/workbench-lite'||url.pathname.startsWith('/polling-work-order'))throw new Error('V2 must not load a legacy document');
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(await layouts.handle(route,user.open_id))return;
    requests.push({path:url.pathname,params:Object.fromEntries(url.searchParams),method:req.method()});
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?user:{},scope_options:loggedIn?scopes:[],login_url:'/api/auth/login?next='+encodeURIComponent(url.searchParams.get('next')||'/')});
    if(url.pathname==='/api/pre-shift-checks')return ok({scopes:['A'],scope:'A',can_manage:['L0','L1','L3'].includes(user.level),definition:{version:0,items:[]},tasks:[],roster_ready:false,roster_error:''});
    if(url.pathname==='/api/auth/logout'){loggedIn=false;return ok({});}
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'v2-isolated'}});
    if(url.pathname==='/api/workbench/lite-detail')return ok({fragments:{detail:noticeFields}});
    if(url.pathname==='/api/jobs/job_fixture')return ok({job_id:'job_fixture',phase:'success',message:'已提交原业务任务'});
    if(url.pathname==='/api/scope-overview')return failOverview?route.fulfill({status:503,json:{ok:false,error:'测试资料读取失败'}}):ok({scopes:overview,source_snapshot_ready:true,last_loaded_at:'2026-10-12 08:00'});
    if(url.pathname==='/api/workbench')return failTasks?route.fulfill({status:503,json:{ok:false,error:'测试待办读取失败'}}):ok({records:[{record_id:'plan_fixture',title:'A楼冷水机组月度维护',work_type:'maintenance',source_progress:'未开始',status:'未开始',scope:'A',start_time:'2026-10-12 09:00',display_fields:{'计划开始维护时间':'2026-10-12 09:00','计划结束维护时间':'2026-10-15 18:30'}},{record_id:'ended_fixture',title:'已结束验收通告',source_progress:'已结束'},{record_id:'source_active_fixture',title:'重复的进行中计划',source_progress:'进行中'}],ongoing:[{active_item_id:'active_fixture',source_record_id:'source_active_fixture',title:'A楼BA系统资料维护',work_type:'maintenance',status:'开始',scope:'A',start_time:'2026-10-12 08:30',end_time:'2026-10-12 18:30'}],stats:{},record_type_counts:{maintenance:1},records_pagination:{total_pages:1,total:3},ongoing_pagination:{total_pages:1,total:1}});
    if(url.pathname==='/api/history-summary')return ok({days:[{date:'2026-10-12',items:[{record_id:'history_fixture',title:'A楼BA系统资料维护',work_type:'maintenance',status:'开始'}]}]});
    if(url.pathname==='/api/repair-management/status')return ok({records:[],total:0,stats:{total:0,without_followup:0,in_progress:0,completed_total:0}});
    if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(url.pathname==='/api/workbench-actions'){
      writes.push(req.postDataJSON());
      if(noticeFailure==='unmatched'){noticeFailure='';expectedNoticeErrors.add('Failed to load resource: the server responded with a status of 409 (Conflict)');return route.fulfill({status:409,json:{ok:false,error:'计划收敛未匹配',error_code:'confirmation_required',details:{kind:'plan_convergence_unmatched',confirmation:{fixture:true}}}});}
      if(noticeFailure==='network'){noticeFailure='';expectedNoticeErrors.add('Failed to load resource: net::ERR_FAILED');return route.abort('failed');}
      return ok({job_id:'job_fixture',message:'已提交原业务任务'});
    }
    if(url.pathname==='/api/auth/accounts')return ok({items:[],scope_options:scopes,module_options:[],policy:{level:'L1',modules:user.modules,can_manage_accounts:true}});
    if(url.pathname==='/api/auth/permissions')return ok({users:[],scope_options:scopes});
    if(url.pathname==='/api/auth/permission-approvers')return ok({items:[{open_id:'ou_admin_approval_fixture',name:'指定审批管理员',level:'L1',scopes:['A','B'],modules:['maintenance'],level_options:[{value:'L4',label:'L4 普通账号'}],category_options:[{value:'operator',label:'值班员'}]}],current:{level:'L4',category:'operator',scope_rules:['A'],modules:['maintenance']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}],module_options:[{value:'maintenance',label:'维保通告'}]});
    if(url.pathname==='/api/auth/permission-requests/current')return ok({request:permissionApplication||{}});
    if(url.pathname==='/api/auth/permission-requests'&&req.method()==='POST'){
      const input=req.postDataJSON();assert.equal(input.approver_open_id,'ou_admin_approval_fixture');assert.deepEqual(input.scopes,['A','B']);assert.equal(input.requested_access.level,'L4');
      permissionApplication={...input,request_id:'permission_fixture_request',requested_scopes:input.scopes,status:'pending',notification_status:'failed',notification_error:'隔离测试：飞书暂不可用'};
      return ok({request:permissionApplication});
    }
    if(url.pathname==='/api/auth/permission-requests/permission_fixture_request/notify'){
      permissionNotifications++;permissionApplication.notification_status='sent';permissionApplication.notification_error='';return ok({queued:true});
    }
    if(url.pathname==='/api/learning/bootstrap')return ok({self_person:{id:'person_fixture',name:user.name},self_scope:'A',can_answer:true,can_view_buildings:true,is_admin:true,scopes:[{value:'A',label:'A楼'}],today:'2026-10-12',settings:{},summary:{},sync:{}});
    if(url.pathname==='/api/learning/papers')return ok({items:[],today:null});
    if(url.pathname==='/api/learning/profile')return ok({summary:{answered:12,wrong:2,accuracy:80},today_summary:{assigned:15,task_answered:6,answered_people:4,completed_people:2,not_started_people:1},people:[],published:true});
    if(url.pathname==='/api/drills')return ok({items:[]});
    if(url.pathname==='/api/critical-guard/tasks')return ok({tasks:[]});
    if(url.pathname==='/api/cabinet-power/batches')return ok({items:[],total:0});
    if(url.pathname==='/api/cabinet-power/buildings')return ok({buildings:'ABCDE'.split('').map(scope=>({scope,bootstrap_status:'succeeded',counts:{total:100,formal:70,test:10,off:20,unknown:0},record_count:8,inventory_only:0}))});
    if(url.pathname==='/api/cabinet-power/overview')return ok({version:'fixture',counts:{total:100,formal:70,test:10,off:20,unknown:0},rooms:[],racks:[],issues:[],record_count:8,sheet_formats:[]});
    if(url.pathname==='/api/capacity/water/bootstrap')return ok({summary:{record_count:3,total_usage:72,photo_records:3},snapshot:{},meters:[]});
    if(url.pathname==='/api/capacity/water/buildings')return ok({snapshot:{exists:true},scopes:['A','B','C','D','E','H'].map(value=>({value,month_total_usage:72,month_record_count:3})),month:'2026-10'});
    if(/^\/api\/plan-convergence\/(maintenance|change)\/records$/.test(url.pathname))return ok([]);
    if(url.pathname==='/api/link-directory')return ok({items:[],total:0,can_manage:true});
    if(url.pathname==='/api/plan-convergence/bootstrap')return ok({blocks:{items:[],loaded_at:0},settings:{},rules:[],configured:false});
    if(url.pathname==='/api/assistant/appearance')return ok({appearance:{color:'#101416',size:80,shape:'circle',snap_back:true}});
    if(url.pathname==='/api/assistant/conversation')return ok({conversation_id:'isolated-assistant',enabled:true,configured:true,busy:assistantTurn?.status==='pending',turns:assistantTurn?[assistantTurn]:[],models:[],can_manage_settings:false});
    if(url.pathname==='/api/assistant/conversation/stamp')return ok({stamp:'isolated'});
    if(url.pathname==='/api/engineer/mop/bootstrap')return ok({notices:[],mop_candidates:[],warnings:[]});
    return ok({items:[],records:[],fields:[],scopes:[],options:[],stats:{},settings:{},people:[],projects:[],summary:{}});
  });
  context.on('page',page=>{
    page.setDefaultTimeout(15000);
    page.on('pageerror',err=>{errors.push({url:page.url(),message:err.message,stack:err.stack});console.log('PAGE ERROR',err.message);});
    page.on('console',message=>{
      if(message.type()!=='error'||message.text().includes('503 (Service Unavailable)'))return;
      if(message.location().url.startsWith(origin+'/api/workbench-actions')&&expectedNoticeErrors.delete(message.text()))return;
      // The legacy login request is cancelled by the expected full-page return to V2.
      const url=new URL(page.url());
      if(url.pathname==='/'&&url.searchParams.get('auth_notice')==='cancelled'&&/^ApiError: (Failed to fetch|暂时无法连接服务，请检查网络连接后重试。)/.test(message.text()))return;
      errors.push({url:page.url(),message:message.text()});console.log('CONSOLE',message.text());
    });
  });
  return context;
}
try {
  if(!process.argv.includes('--duty')){
  const ctx=await context(),page=await ctx.newPage();
  await page.goto(origin+'/');
  await page.getByRole('button',{name:'切换新版',exact:true}).waitFor();
  assert.equal(await page.locator('.v2-shell').count(),0);
  await page.getByRole('button',{name:'切换新版',exact:true}).hover();
  await page.locator('link[data-workspace-preload][rel="modulepreload"]').waitFor({state:'attached'});
  assert.equal(await page.locator('link[data-workspace-preload][rel="stylesheet"]').count(),0,'preparing V2 must not apply V2 styles to the old page');
  await page.getByRole('button',{name:'切换新版',exact:true}).click();
  await page.locator('.v2-dashboard').waitFor();
  const originalUser=user;user={...user,open_id:'ou_second_person_fixture',name:'另一名验收人员'};
  await page.goto(origin+'/');await page.getByRole('button',{name:'切换新版',exact:true}).waitFor();assert.equal(new URL(page.url()).pathname,'/','version preference must not cross accounts');user=originalUser;
  await page.goto(origin+'/');await page.locator('.v2-dashboard').waitFor();
  await page.getByText('A楼BA系统资料维护',{exact:true}).first().waitFor();
  assert.equal(new URL(page.url()).pathname,entry);
  assert.equal(await page.evaluate(()=>localStorage.getItem('clipflow:frontend-version:ou_v2_fixture')),'workspace');
  assert.equal(await page.locator('.v2-sidebar').getByRole('menuitem',{name:'通告办理',exact:true}).count(),1);
  assert.equal(await page.locator('.v2-sidebar').getByRole('menuitem',{name:/^(维保通告|变更通告|检修通告|设备轮巡|设备调整|上下电通告|灯塔助手|楼栋待办)$/}).count(),0,'no duplicate module entries');
  assert.equal(await page.locator('.v2-metrics,.v2-charts,.v2-business-overviews,.v2-priority,.v2-functions').count(),0,'home is an action surface, without dashboard panels or duplicate lists');
  assert.equal(await page.getByRole('navigation',{name:'常用操作'}).getByRole('button').count(),4);
  assert.equal(await page.getByText('已结束验收通告',{exact:true}).count(),0);
  assert.equal(await page.getByText('重复的进行中计划',{exact:true}).count(),0);
  const handleRow=page.locator('.v2-task-row').filter({hasText:'A楼BA系统资料维护'});
  assert.equal(await handleRow.locator('button').count(),0,'a work row is one keyboard-operable action, not nested buttons');
  await handleRow.locator('.v2-task-action').click();
  await page.locator('.v2-notice-editor').waitFor();
  assert.match(page.url(),/active_item_id=active_fixture/,'workbench action opens the existing notice form for the exact record');
  await page.goto(origin+entry);await page.locator('.v2-dashboard').waitFor();
  await page.getByText('A楼冷水机组月度维护',{exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'刷新工作台',exact:true}).count(),0,'dashboard has no redundant refresh action');
  const panelStart=requests.length;
  await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter').click();await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter input').fill('演练');await page.locator('.n-base-select-option').getByText('演练管理',{exact:true}).click();
  await page.getByText('当前月份 · 已发布演练',{exact:true}).waitFor();
  assert.equal(await page.locator('.v2-inline-stats').count(),0,'selected business shows its pending list, not dashboard counters');
  assert(requests.slice(panelStart).some(r=>r.path==='/api/drills'));
  assert.equal(requests.slice(panelStart).some(r=>r.path==='/api/critical-guard/tasks'||r.path.startsWith('/api/plan-convergence/')),false,'only the selected pending category loads');
  await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter').click();await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter input').fill('维保');await page.locator('.n-base-select-option').getByText('维保通告',{exact:true}).click();
  await page.getByText('A楼BA系统资料维护',{exact:true}).waitFor();
  await page.locator('.n-base-select-menu:visible').waitFor({state:'hidden'});
  await page.waitForFunction(()=>!document.querySelector('.v2-dashboard-heading [data-v2-action]'));
  for(const width of [1366,1440,1920,360,390,430]){
    await page.setViewportSize({width,height:width<700?844:1000});
    await page.waitForFunction(()=>[...document.querySelectorAll('.grid-stack-item')].every(el=>{const box=el.getBoundingClientRect();return box.left>=-1&&box.right<=innerWidth+1;}));
    await page.screenshot({path:path.join(output,`home-${width}.png`),fullPage:true});
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1);
    assert.equal(overflow,false,`viewport overflow at ${width}`);
    assert.equal(await page.locator('.v2-mobile-brief').count(),width<761?1:0,'only mobile home mounts the compact overview');
  }
  assert.equal(await page.locator('.v2-task-action').getByText('办理',{exact:true}).count(),0,'mobile home is attention-first, not a duplicate handling queue');
  await page.locator('.v2-widget[aria-label="待处理事项"]').getByRole('button',{name:'查看待办',exact:true}).click();
  await page.locator('.v2-home:not(.v2-task-embedded)').waitFor();
  await page.locator('.v2-task-row').filter({hasText:'A楼冷水机组月度维护'}).waitFor();
  assert.equal(await page.locator('.v2-mobile-brief').count(),0,'mobile tasks does not duplicate overview data');
  assert(await page.locator('.v2-task-action').getByText('办理',{exact:true}).count()>0);
  await page.screenshot({path:path.join(output,'tasks-mobile.png'),fullPage:true});
  await page.locator('.v2-mobile-nav').getByRole('button',{name:'首页',exact:true}).click();await page.locator('.v2-dashboard').waitFor();
  try{await checkCustomization(page,output,layouts);}catch(error){
    await page.screenshot({path:path.join(output,'customization-failure.png'),fullPage:true});
    console.error('Customization geometry:',await page.locator('.grid-stack-item').evaluateAll(items=>items.map(el=>({id:el.dataset.widgetId,attributes:[...el.attributes].filter(a=>a.name.startsWith('gs-')).map(a=>[a.name,a.value]),node:el.gridstackNode&&{x:el.gridstackNode.x,y:el.gridstackNode.y,w:el.gridstackNode.w,h:el.gridstackNode.h,minH:el.gridstackNode.minH},box:{top:el.getBoundingClientRect().top,bottom:el.getBoundingClientRect().bottom}}))));
    throw error;
  }
  assert.equal(await page.locator('.v2-sidebar').getByRole('menuitem',{name:'账号与权限',exact:true}).count(),0,'settings lives in the personal page');
  await page.getByRole('button',{name:'打开个人页面',exact:true}).click();
  await page.getByRole('heading',{name:'验收管理员',exact:true}).waitFor();
  await page.getByRole('button',{name:'退出登录',exact:true}).waitFor();
  await page.getByRole('button',{name:'账号与设置',exact:true}).waitFor();
  assert.equal(await page.locator('.v2-context,.v2-sidebar-bottom').count(),0,'authorization is not repeated around the profile');
  await page.locator('.v2-main').evaluate(async el=>{await Promise.allSettled(el.getAnimations({subtree:true}).filter(a=>a.effect?.getTiming().iterations!==Infinity).map(a=>a.finished));});
  await page.locator('.n-message').first().waitFor({state:'hidden'});
  await page.screenshot({path:path.join(output,'profile-desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});await page.screenshot({path:path.join(output,'profile-mobile.png'),fullPage:true});
  await page.locator('.v2-mobile-nav').getByRole('button',{name:'首页',exact:true}).click();await page.locator('.v2-dashboard').waitFor();
  assert.equal(await page.locator('.v2-widget-scope:visible').count(),0,'mobile hides repeated scope labels');
  await page.setViewportSize({width:430,height:844});
  await page.locator('.v2-mobile-nav').getByRole('button',{name:'功能',exact:true}).click();
  await page.getByRole('heading',{name:'全部功能',exact:true}).waitFor();
  await page.waitForFunction(()=>!document.querySelector('.v2-mobile-nav [data-v2-action]'));
  assert.equal(await page.locator('.v2-functions').getByRole('button',{name:'通告办理',exact:true}).count(),1);
  assert.equal(await page.locator('.v2-functions').getByRole('button',{name:'灯塔助手',exact:true}).count(),0);
  for(const width of [360,390,430,1440]){await page.setViewportSize({width,height:width<700?844:1000});await page.screenshot({path:path.join(output,`functions-${width}.png`),fullPage:true});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,`functions overflow at ${width}`);}
  await page.locator('.v2-functions').getByRole('button',{name:'风险与重保',exact:true}).focus();await page.keyboard.press('Enter');await page.locator('.guard-page').waitFor();assert.equal(await page.locator('.guard-page .landing-view').count(),0);
  assert.equal(await page.locator('#page-back-slot').count(),0);await page.getByRole('menuitem',{name:'个人工作台',exact:true}).click();await page.locator('.v2-dashboard').waitFor();
  await page.setViewportSize({width:1440,height:1000});
  await page.getByRole('button',{name:'切换旧版',exact:true}).click();
  await page.getByRole('button',{name:'切换新版',exact:true}).waitFor();
  assert.equal(new URL(page.url()).pathname,'/');
  assert.equal(await page.locator('.v2-shell').count(),0);
  await page.getByRole('button',{name:'切换新版',exact:true}).click();
  await page.locator('.v2-dashboard').waitFor();
  await page.getByRole('menuitem',{name:'事件管理',exact:true}).click();await page.locator('.v2-event-charts').waitFor();
  assert.equal(await page.locator('#page-back-slot').count(),0);await page.getByRole('menuitem',{name:'个人工作台',exact:true}).click();await page.locator('.v2-dashboard').waitFor();
  await page.goto(origin+entry+'?__page=/workbench-lite&scope=A&work_type=maintenance');
  await page.locator('.v2-notice-open').first().click();
  await page.getByRole('textbox',{name:'通告内容',exact:true}).fill('隔离验收，不发送真实通告');
  await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'关闭',exact:true}).click();
  await page.getByText('当前填写尚未保存',{exact:true}).waitFor();
  await page.getByRole('button',{name:'继续填写',exact:true}).click();
  assert.match(page.url(),/workbench-lite/);
  await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();
  assert.equal(writes.length,0,'preview must not submit');
  await page.getByRole('button',{name:'确认发送开始',exact:true}).click();
  await page.getByText('已提交原业务任务',{exact:true}).waitFor();
  assert.equal(writes.length,1);assert.equal(writes[0].action,'start');assert.equal(writes[0].scope,'A');
  assert.equal(writes[0].patch.content,'隔离验收，不发送真实通告');
  await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'返回列表',exact:true}).click();
  noticeFailure='unmatched';
  await page.getByRole('button',{name:'手填维保通告',exact:true}).click();
  await page.getByRole('textbox',{name:'通告内容',exact:true}).fill('第一次填写');
  await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('button',{name:'确认发送开始',exact:true}).click();
  await page.getByRole('button',{name:'返回核对',exact:true}).click();
  await page.getByRole('textbox',{name:'通告内容',exact:true}).fill('取消确认后更正的内容');
  await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('button',{name:'确认发送开始',exact:true}).click();
  await page.getByText('已提交原业务任务',{exact:true}).waitFor();
  assert.equal(writes.at(-1).patch.content,'取消确认后更正的内容','cancelled confirmation must not retain the old frozen payload');
  await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'返回列表',exact:true}).click();
  noticeFailure='network';
  await page.getByRole('button',{name:'手填维保通告',exact:true}).click();
  await page.getByRole('textbox',{name:'通告内容',exact:true}).fill('网络中断后原样重试');
  await page.getByRole('button',{name:'预览并确认开始',exact:true}).click();await page.getByRole('button',{name:'确认发送开始',exact:true}).click();
  await page.getByRole('button',{name:'核验并重试原提交',exact:true}).waitFor();
  assert.equal(await page.getByRole('textbox',{name:'通告内容',exact:true}).count(),0,'unknown write result replaces editing with the original task recovery view');
  const previous=structuredClone(writes.at(-1));
  await page.getByRole('button',{name:'核验并重试原提交',exact:true}).click();await page.getByText('已提交原业务任务',{exact:true}).waitFor();
  assert.deepEqual(writes.at(-1),previous,'retry keeps both original operation ID and fields');
  await page.locator('.v2-notice-editor-footer').getByRole('button',{name:'返回列表',exact:true}).click();
  await page.getByRole('menuitem',{name:'个人工作台'}).click();
  await page.locator('.v2-dashboard').waitFor();
  assert.equal(await page.locator('.v2-dashboard .v2-local-scope').count(),0,'dashboard has no page-level building selector');
  assert.match(await page.locator('.v2-dashboard .v2-widget-scope').first().innerText(),/全部(?:授权)?楼栋/,'notice A-building navigation must not replace the workbench scope');
  await page.getByRole('menuitem',{name:'风险与重保',exact:true}).click();await page.locator('.guard-page').waitFor();assert.match(page.url(),/scope=A/);assert.equal(await page.locator('.guard-page .landing-view').count(),0);
  assert.equal(await page.locator('#page-back-slot').count(),0);await page.getByRole('menuitem',{name:'个人工作台',exact:true}).click();await page.locator('.v2-dashboard').waitFor();
  await page.evaluate(()=>{const link=document.createElement('a');link.href='/workbench-lite?scope=A&work_type=maintenance';document.body.append(link);link.click();link.remove();});
  await page.locator('.v2-notice-entry').first().waitFor();
  assert.equal(await page.locator('iframe').count(),0,'returning uses the new Vue notice page');
  await page.getByRole('menuitem',{name:'个人工作台'}).click();await page.locator('.v2-dashboard').waitFor();
  await page.locator('.v2-local-scope').click();await page.locator('.n-base-select-option').getByText('全部楼栋',{exact:true}).click();
  await page.getByRole('menuitem',{name:'水耗管理',exact:true}).click();
  await page.locator('.water-overview-metrics').getByText('近30天日统计耗水量',{exact:true}).waitFor();
  assert.equal(requests.some(r=>r.path==='/api/capacity/water/bootstrap'&&['ALL','CAMPUS'].includes(r.params.scope)),false,'global water reads authorized building endpoints');
  await page.evaluate(()=>{const link=document.createElement('a');link.href='/?scope=A';document.body.append(link);link.click();link.remove();});await page.locator('.v2-dashboard').waitFor();
  await page.getByRole('menuitem',{name:'个人工作台'}).click();assert.match(await page.locator('.v2-local-scope').innerText(),/A楼/);
  user={...user,open_id:'ou_scope_isolation_fixture',name:'范围隔离验收'};await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));await page.locator('.v2-person').getByText('范围隔离验收',{exact:true}).waitFor();assert.match(await page.locator('.v2-local-scope').innerText(),/全部楼栋/,'scope memory does not cross accounts');
  user=originalUser;await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));await page.locator('.v2-person').getByText(originalUser.name,{exact:true}).waitFor();
  await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter').click();await page.locator('.v2-widget[aria-label="待处理事项"] .v2-task-filter input').fill('机柜');await page.locator('.n-base-select-option').getByText('机柜管理',{exact:true}).click();
  await page.locator('.v2-widget[aria-label="待处理事项"]').getByRole('button',{name:'查看完整列表',exact:true}).click();await page.waitForURL('**__page=%2Fcabinet-power%2Fbatches**');await page.locator('.batch-page').waitFor();assert.match(page.url(),/status=todo/,'cabinet pending list opens pending batches directly');
  await page.goto(origin+entry+'?__page=/admin/settings');
  await page.locator('.accounts').waitFor();
  await page.screenshot({path:path.join(output,'accounts.png'),fullPage:true});
  const modulePages = [
    ['/?mode=events&scope=A', '.event-page'], ['/repair-management?scope=A', '.repair-management-page'],
    ['/repair-status?scope=A', '.repair-status-page'], ['/engineer/mop?scope=A', '.engineer-mop'],
    ['/cabinet-power', '.cabinet-page'], ['/water-management?scope=A', '.v2-water'],
    ['/critical-guard', '.guard-page'], ['/drill-management', '.drill-page'],
    ['/learning?view=overview', '.learning-page'], ['/plan-convergence', '.pc-page'],
    ['/knowledge-base', '.kb-page'], ['/link-directory', '.link-directory-page'],
    ['/signature-management', '.signature-management'], ['/admin/history-memory', '.v2-history'],
  ];
  for (const [target,selector] of modulePages) {
    const url=new URL(target,origin), params=new URLSearchParams(url.search);params.set('__page',url.pathname);
    await page.goto(origin+entry+'?'+params);
    await page.locator(selector).waitFor();
    if(target.includes('mode=events'))await page.locator('.v2-event-charts').waitFor();
    assert.equal(new URL(page.url()).pathname,entry,`${target} stays in independent frontend`);
    const name=target.replace(/[^a-z0-9]+/gi,'-');
    for(const width of [1366,390]){
      await page.setViewportSize({width,height:width<700?844:960});
      await page.screenshot({path:path.join(output,`module${name}-${width}.png`),fullPage:true});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,`${target} overflow at ${width}`);
      const header=await page.locator('.v2-header').boundingBox(),surface=await page.locator(selector).boundingBox();
      assert(surface.y>=header.y+header.height-1,`${target} content must not start underneath the main header at ${width}`);
    }
  }
  await ctx.close();
  }

  for(const [level,category,title] of [['L0','other','个人工作台'],['L1','duty','个人工作台'],['L2','engineer','工程工作台'],['L3','supervisor','主管工作台'],['L4','duty','值班工作台'],['L4','operator','我的工作台']]){
    if(process.argv.includes('--duty')&&!(category==='duty'&&level==='L4'))continue;
    user={open_id:`ou_${level}_${category}`,name:'验收'+category,role:['L0','L1'].includes(level)?'admin':'building',level,category,modules:fullModules};
    scopes=['L0','L1'].includes(level)?[{value:'ALL',label:'全部授权楼栋'},...['A','B','C','D','E','H'].map(value=>({value,label:value+'楼'}))]:[{value:'A',label:'A楼'}];
    const roleContext=await context(),rolePage=await roleContext.newPage();
    const roleRequestStart=requests.length;
    await rolePage.goto(origin+entry);await rolePage.locator('.v2-dashboard').waitFor();
    assert.equal(await rolePage.locator('.v2-metrics,.v2-charts,.v2-business-overviews,.v2-inline-stats').count(),0);
    if(category==='operator')await rolePage.getByText('已答 6 / 15 题',{exact:true}).waitFor();
    for(const width of [1440,390]){await rolePage.setViewportSize({width,height:width<700?844:1000});await rolePage.screenshot({path:path.join(output,`role-${level}-${category}-${width}.png`),fullPage:true});assert.equal(await rolePage.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);}
    assert.equal(requests.slice(roleRequestStart).some(r=>r.path==='/api/capacity/water/bootstrap'||r.path.startsWith('/api/cabinet-power/')),false,'home does not fetch unused dashboard data');
    if(category==='duty'&&level==='L4'){
      await rolePage.getByRole('navigation',{name:'常用操作'}).getByRole('button',{name:'水耗管理'}).click();await rolePage.locator('.v2-water').waitFor();
      assert.match(rolePage.url(),/water-management/);
      assert.equal(await rolePage.getByRole('button',{name:'返回工作台',exact:true}).count(),0);
      await rolePage.getByRole('navigation',{name:'值班常用业务'}).getByRole('button',{name:'风险管理'}).click();try{await rolePage.locator('.guard-page').waitFor();}catch(error){await rolePage.screenshot({path:path.join(output,'duty-risk-failure.png'),fullPage:true});console.log('DUTY',rolePage.url(),(await rolePage.locator('body').innerText()).slice(0,2200));throw error;}assert.equal(await rolePage.locator('.guard-page .landing-view').count(),0,'building opens directly');assert(requests.some(r=>r.path==='/api/critical-guard/tasks'&&r.params.scope==='A'));
      await rolePage.getByRole('navigation',{name:'值班常用业务'}).getByRole('button',{name:'演练管理'}).click();await rolePage.locator('.drill-page').waitFor();
      assert.equal(await rolePage.locator('#page-back-slot').count(),0);await rolePage.locator('.v2-mobile-nav').getByRole('button',{name:'首页',exact:true}).click();await rolePage.locator('.v2-dashboard').waitFor();
    }
    if(category==='duty'&&level==='L1')assert.equal(await rolePage.getByRole('navigation',{name:'值班常用业务'}).count(),0);
    if(category==='supervisor'){await rolePage.getByRole('navigation',{name:'常用操作'}).getByRole('button',{name:'计划收敛审查'}).click();await rolePage.locator('.pc-page').waitFor();}
    await roleContext.close();
  }

  user={open_id:'ou_read_failure_fixture',name:'读取异常验收',role:'admin',level:'L1',category:'manager',modules:fullModules};failTasks=true;
  const failedContext=await context(),failedPage=await failedContext.newPage();await failedPage.goto(origin+entry);await failedPage.getByText('测试待办读取失败',{exact:false}).first().waitFor();assert.equal(await failedPage.getByText('当前分类暂无待处理事项',{exact:true}).count(),0,'failed reads are not presented as no pending work');failTasks=false;await failedPage.getByRole('button',{name:'重试',exact:true}).click();await failedPage.getByText('A楼BA系统资料维护',{exact:true}).waitFor();await failedContext.close();

  user={open_id:'ou_person_fixture',name:'验收成员',role:'building',level:'L4',category:'operator',modules:['maintenance'],login_method:'password',personnel_record_id:'person_fixture'};scopes=[{value:'A',label:'A楼'}];
  const ordinary=await context(),member=await ordinary.newPage();
  const start=requests.length;
  await member.goto(origin+entry+'?__page=/learning');
  await member.getByText('没有该功能权限',{exact:true}).waitFor();
  assert.equal(requests.slice(start).some(r=>r.path.startsWith('/api/learning/')),false);
  assert.equal(await member.locator('.lighthouse-root').count(),0);
  await member.goto(origin+entry+'?__page=/&account=password');
  await member.getByText('验收成员',{exact:true}).last().waitFor();
  assert.equal(await member.locator('#personnel-name, #personnel-reset-name').count(),0);
  await member.goto(origin+entry+'?view=permission');
  await member.getByRole('heading',{name:'本人权限申请',exact:true}).waitFor();
  assert.equal(await member.getByRole('button',{name:'提交并发送飞书审批',exact:true}).isDisabled(),true,'an approver is mandatory');
  await member.getByLabel('上级审批人',{exact:true}).click();await member.locator('.n-base-select-option').getByText('指定审批管理员 · L1',{exact:true}).click();
  await member.getByRole('checkbox',{name:'B楼',exact:true}).check();await member.getByRole('textbox',{name:'申请原因',exact:true}).fill('跨楼协作');
  await member.getByRole('button',{name:'提交并发送飞书审批',exact:true}).click();
  await member.getByRole('button',{name:'重试发送审批表单',exact:true}).waitFor();
  await member.reload();await member.getByRole('button',{name:'重试发送审批表单',exact:true}).waitFor();
  assert.equal(await member.getByRole('checkbox',{name:'B楼',exact:true}).isChecked(),true,'application restores after reload');
  await member.getByRole('button',{name:'重试发送审批表单',exact:true}).click();assert.equal(permissionNotifications,1);
  permissionApplication.status='approved';
  await member.getByText(/已通过，权限已生效/).waitFor({timeout:15000});
  await member.screenshot({path:path.join(output,'permission-approval.png'),fullPage:true});
  permissionApplication=null;
  await member.getByRole('menuitem',{name:'通告办理',exact:true}).click();await member.locator('.v2-notices').waitFor();assert.match(member.url(),/work_type=maintenance/,'single notice grant opens that type directly');
  await member.goto(origin+entry+'?__page=/workbench-lite&scope=E');await member.getByText('没有该功能权限',{exact:true}).waitFor();
  await member.goto(origin+entry+'?__page=/workbench-lite&scope=A');await member.locator('.v2-notices').waitFor();user={...user,modules:[]};await member.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));await member.getByText('没有该功能权限',{exact:true}).waitFor();assert.equal(await member.locator('.v2-notices').count(),0,'withdrawn module unmounted');
  await ordinary.close();

  user={open_id:'ou_assistant_fixture',name:'会话验收',role:'building',level:'L4',category:'operator',modules:['assistant']};scopes=[{value:'A',label:'A楼'}];
  assistantTurn={operation_id:'isolated-turn',question:'展示隔离验收消息',status:'pending',answer:'',process:[]};
  const chatContext=await context(),chatPage=await chatContext.newPage();await chatPage.goto(origin+entry);
  assert.equal(await chatPage.locator('.v2-sidebar').getByRole('menuitem',{name:'灯塔助手',exact:true}).count(),0);
  await chatPage.getByRole('button',{name:'打开灯塔助手',exact:true}).click();await chatPage.locator('.assistant-shell').waitFor();await chatPage.getByText('展示隔离验收消息',{exact:true}).waitFor();
  for(const width of [1440,390]){await chatPage.setViewportSize({width,height:width<700?844:1000});await chatPage.screenshot({path:path.join(output,`assistant-${width}.png`),fullPage:true});const box=await chatPage.locator('.assistant-shell').boundingBox();assert(box.x>=0&&box.x+box.width<=width+1,'assistant remains within viewport');}
  await chatPage.getByRole('button',{name:'收起助手',exact:true}).click();await chatPage.locator('.assistant-shell').waitFor({state:'hidden'});await chatContext.close();assistantTurn=null;

  user={open_id:'guest_fixture',name:'机柜访客',role:'guest',category:'other',level:'',modules:['cabinet']};scopes=[{value:'A',label:'A楼'}];
  const guests=await context(),guest=await guests.newPage();await guest.goto(origin+entry);await guest.locator('.cabinet-page').waitFor();assert.equal(await guest.getByRole('button',{name:'一键导出/上传所有楼栋'}).count(),0);assert.equal(await guest.getByRole('button',{name:'打开灯塔助手'}).count(),0);assert.equal(await guest.locator('.v2-sidebar,.v2-mobile-nav').count(),0,'guest has no irrelevant module navigation');await guests.close();

  loggedIn=false;
  const anonymous=await context(),login=await anonymous.newPage();
  await login.goto(origin+entry+'?login=password');
  await login.getByRole('tab',{name:'账号密码登录'}).waitFor();
  await login.setViewportSize({width:390,height:844});
  await login.screenshot({path:path.join(output,'login-mobile.png'),fullPage:true});
  assert.equal(await login.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await login.goto(origin+'/?login=feishu&auth_notice=cancelled&next='+encodeURIComponent(entry+'?__page=/repair-management&scope=A'));
  await login.waitForURL('**/assets/workspace-v2/index.html?**');
  await login.getByText('已取消飞书授权',{exact:false}).first().waitFor();
  await anonymous.close();
  assert.deepEqual(errors,[]);
  console.log('PASS: task-focused home without dashboards or unused reads, single refresh, active/pending dedup, retry, role shortcuts, flat module menu/mobile directory, direct event and building pages, scope retention+account isolation, 6 role variants, 14 business pages on PC/mobile, 6 home viewports, revoked scope/module, native submit+dirty guard, current-person password, guest, login cancellation and assistant. No real business requests.');
} finally { await browser.close(); await new Promise(resolve=>server.httpServer.close(resolve)); }
