import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir,readFile} from 'node:fs/promises';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/legacy-module-entry');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],riskScopes=[],writes=[],reads=[];
const special=[
  {id:'risk_inspection',label:'风险排查',path:'/risk-inspections',api:'/api/risk-inspections',surface:'.risk-page'},
  {id:'work_handover',label:'人员工作交接',path:'/work-handover',api:'/api/work-handovers',surface:'.work-handover'},
  {id:'learning',label:'画像学练',path:'/learning',api:'/api/learning',surface:'.learning-page'},
  {id:'plan_convergence',label:'计划收敛审查',path:'/plan-convergence',api:'/api/plan-convergence',surface:'.pc-page'},
];
const v2Labels=['风险排查','计划收敛审查','人员工作交接'];
const toolLabels=['共享知识库','南通文件','每日任务清单','上/下电通告','设备轮巡','设备调整','交接班审核页'];
const fullModules=['maintenance','critical_guard','daily','knowledge','power','polling','adjust','handover',...special.map(item=>item.id)];
const viewports=[{width:1440,height:900},{width:402,height:874},{width:440,height:956}];
const scopeOptions=['A','B'].map(value=>({value,label:value+'楼'}));
let page,modules=fullModules,category='supervisor';
const v2Url=path=>{const target=new URL(path,origin),url=new URL('/assets/workspace-v2/index.html',origin);url.search=target.search;url.searchParams.set('__page',target.pathname);return url.href;};
async function fitAndCapture(name){
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),name+': no horizontal overflow');
  await page.screenshot({path:path.join(output,name+'.png'),fullPage:true});
}
async function clickVisible(locator,label){
  await locator.scrollIntoViewIfNeeded();
  await locator.click({trial:true});
  assert(await locator.evaluate(el=>{const r=el.getBoundingClientRect();return r.width>0&&r.height>0&&r.left>=0&&r.right<=innerWidth+1&&r.top>=0&&r.bottom<=innerHeight+1;}),label+': entire clickable control fits the viewport');
  await locator.click();
}
async function legacyBack(expected){
  const back=page.locator('.vnet-back-button:visible');
  await back.waitFor();assert.equal(await back.count(),1,'only one legacy back button');
  await clickVisible(back,'legacy back');
  await page.waitForURL(origin+expected);
}
async function assertSpecialMenu(expected){
  await page.getByRole('heading',{name:'专项任务',exact:true}).waitFor();
  assert.deepEqual(await page.locator('.tool-card').evaluateAll(nodes=>nodes.map(node=>node.getAttribute('aria-label'))),expected.map(label=>'选择'+label));
  for(const item of special)assert.equal(await page.getByRole('button',{name:'选择'+item.label,exact:true}).count(),expected.includes(item.label)?1:0,item.id+': legacy grant filtering');
  assert.equal(await page.getByRole('button',{name:'选择重保管理',exact:true}).count(),0,'critical guard is not a special task');
}
async function assertV2Groups(expected){
  await page.getByRole('heading',{name:'全部功能',exact:true}).waitFor();
  const group=page.locator('.v2-sidebar .n-menu-item-group').filter({has:page.locator('.n-menu-item-group-title[aria-label="专项功能"]')});
  const functions=page.locator('.v2-function-groups > section').filter({has:page.getByRole('heading',{name:'专项功能',exact:true})});
  assert.equal(await group.count(),expected.length?1:0,'sidebar group requires at least one grant');
  assert.equal(await functions.count(),expected.length?1:0,'mobile function group requires at least one grant');
  assert.deepEqual((await group.locator('.n-menu-item-content-header').allTextContents()).map(text=>text.trim()),expected);
  assert.deepEqual((await functions.getByRole('button').allTextContents()).map(text=>text.trim()),expected);
  for(const item of special){
    assert.equal(await page.locator('.v2-sidebar .n-menu-item').filter({hasText:item.label}).count(),modules.includes(item.id)?1:0,item.id+': V2 sidebar grant filtering');
    assert.equal(await page.locator('.v2-functions').getByRole('button',{name:item.label,exact:true}).count(),modules.includes(item.id)?1:0,item.id+': V2 function grant filtering');
  }
  const learning=page.locator('.v2-function-groups > section').filter({has:page.getByRole('heading',{name:'学习与资料',exact:true})});
  assert.equal(await learning.getByRole('button',{name:'画像学练',exact:true}).count(),modules.includes('learning')?1:0,'learning remains in learning/resources');
  const learningNav=page.locator('.v2-sidebar .n-menu-item-group').filter({has:page.locator('.n-menu-item-group-title[aria-label="学习与资料"]')});
  assert.equal(await learningNav.locator('.n-menu-item').filter({hasText:'画像学练'}).count(),modules.includes('learning')?1:0);
}
try{
  const context=await browser.newContext({viewport:viewports[0],reducedMotion:'reduce',serviceWorkers:'block'});
  // This fixture visits both UIs; do not let V2's remembered preference redirect a legacy case.
  await context.addInitScript(()=>{if(location.pathname!=='/assets/workspace-v2/index.html')localStorage.setItem('clipflow:frontend-version:ou_entry_fixture','legacy');});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(url.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
    if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({body:await readFile(path.join(root,'public/assets/vnet-logo.png')),contentType:'image/png'});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    if(req.method()!=='GET'){writes.push(req.url());return route.abort();}
    reads.push(url.pathname);
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_entry_fixture',name:'隔离测试',role:'building',level:category==='supervisor'?'L3':'L4',category,home_scope:'A',scope_rules:['A','B'],modules},scope_options:scopeOptions,default_scope:'A'});
    if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
    if(url.pathname==='/api/pre-shift-checks')return ok({server_time:'2026-10-21T09:00:00+08:00',scope:url.searchParams.get('scope'),scopes:['A','B'],definition:{version:1,items:[]},tasks:[],roster_ready:true,can_manage:false});
    if(url.pathname==='/api/handover-links')return ok({links:{}});
    if(url.pathname==='/api/scope-overview')return ok({scopes:{A:{},B:{}},source_snapshot_ready:true});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'entry-fixture'}});
    if(url.pathname==='/api/critical-guard/bootstrap')return ok({scopes:['A','B'].map(value=>({value,label:value+'楼',authorized:true,pending:0,completed:0}))});
    if(url.pathname==='/api/risk-inspections'){riskScopes.push(url.searchParams.get('scope'));return ok({items:[],total:0,page:1,is_manager:category==='supervisor',can_edit_template:false});}
    if(url.pathname==='/api/work-handovers/people')return ok({items:[],modules:[],scopes:['A','B'],default_modules:[],self:{home_scope:'A'}});
    if(url.pathname==='/api/work-handovers')return ok({items:[],total:0});
    if(url.pathname==='/api/learning/bootstrap')return ok({is_admin:false,can_answer:false,can_view_buildings:true,scope:'A',scopes:scopeOptions,settings:{enabled:true},sync:{status:'ready',pending:0},today:'2026-10-21'});
    if(url.pathname==='/api/learning/profile')return ok({person:null,summary:{},buildings:[],people:[],topics:[],trend:[],banks:[]});
    if(url.pathname==='/api/plan-convergence/bootstrap')return ok({is_admin:false,catalog_ready:true,points_ready:false,blocks:{items:[],loaded_at:0}});
    if(url.pathname==='/api/plan-convergence/rulesets')return ok([]);
    return ok({items:[],records:[],stats:{},summary:{}});
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  for(const viewport of viewports){
    const {width}=viewport;modules=fullModules;category='supervisor';
    await page.setViewportSize(viewport);
    await page.goto(origin+'/');
    await page.locator('.module-pre_shift').waitFor();
    assert(await page.locator('.module-card').first().evaluate(node=>node.classList.contains('module-pre_shift')),'checklist is the first authorized home card');
    await fitAndCapture('home-'+width);
    const card=page.locator('.module-risk');await card.waitFor();
    assert.equal(await card.getByRole('button').count(),1,'risk has one homepage entry, not a separate inspection shortcut');
    await card.getByRole('button',{name:'进入风险管理',exact:true}).click();
    await page.getByRole('button',{name:'选择重保管理',exact:true}).waitFor();
    assert.deepEqual(await page.locator('.tool-card').evaluateAll(nodes=>nodes.map(node=>node.getAttribute('aria-label'))),['选择重保管理'],'risk management contains only critical guard');
    await fitAndCapture('risk-menu-'+width);
    await clickVisible(page.getByRole('button',{name:'选择重保管理',exact:true}),'critical guard');
    await page.getByRole('heading',{name:'选择楼栋',exact:true}).waitFor();
    assert.equal(await page.locator('.building-grid .building-card').count(),2);
    await legacyBack('/?entry=risk');
    await legacyBack('/');
    await clickVisible(page.getByRole('button',{name:'进入专项功能',exact:true}),'special home card');
    await assertSpecialMenu(special.map(item=>item.label));
    await fitAndCapture('special-menu-'+width);
    await clickVisible(page.getByRole('button',{name:'选择风险排查',exact:true}),'inspection entry');
    await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    assert.equal(await page.locator('.scope-card').count(),2);
    assert.equal(await page.locator('.scope-summary-strip').count(),0,'inspection must not show unrelated notice counts');
    await fitAndCapture('risk-buildings-'+width);
    await clickVisible(page.locator('.scope-card').filter({hasText:'B楼'}).getByRole('button',{name:'进入风险排查',exact:true}),'B building inspection');
    await page.waitForURL('**/risk-inspections?scope=B');
    await page.getByRole('region',{name:'风险排查',exact:true}).waitFor();
    await page.getByRole('tab',{name:'发布管理',exact:true}).waitFor();
    const scopedRead=page.waitForResponse(response=>{const url=new URL(response.url());return url.pathname==='/api/risk-inspections'&&url.searchParams.get('view')==='published'&&url.searchParams.get('scope')==='B';});
    await clickVisible(page.getByRole('region',{name:'风险排查',exact:true}).getByRole('button',{name:'刷新',exact:true}),'refresh selected building');
    assert.equal((await scopedRead).status(),200,'manager publication list uses the selected B building');
    await fitAndCapture('risk-page-'+width);
    await clickVisible(page.getByRole('region',{name:'风险排查',exact:true}).getByRole('button',{name:'返回',exact:true}),'inspection back');
    await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    await legacyBack('/?entry=special');
    await assertSpecialMenu(special.map(item=>item.label));
    for(const item of special.filter(item=>item.id!=='risk_inspection')){
      await clickVisible(page.getByRole('button',{name:'选择'+item.label,exact:true}),item.label);
      await page.waitForURL(origin+item.path);
      await page.locator(item.surface).waitFor();
      if(item.id==='learning')await page.getByRole('button',{name:'选择人员',exact:true}).waitFor();
      if(item.id==='plan_convergence')await page.locator('.pc-page[aria-busy="false"]').waitFor();
      await fitAndCapture(item.id+'-'+width);
      if(item.id==='work_handover')await clickVisible(page.locator(item.surface).getByRole('button',{name:'返回',exact:true}),item.label+' back');
      else await legacyBack('/?entry=special');
      await page.waitForURL(origin+'/?entry=special');
      await assertSpecialMenu(special.map(item=>item.label));
      await fitAndCapture(item.id+'-returned-'+width);
    }
    await page.goto(origin+'/risk-inspections');
    await clickVisible(page.getByRole('region',{name:'风险排查',exact:true}).getByRole('button',{name:'返回',exact:true}),'unscoped inspection back');
    await page.waitForURL(origin+'/?entry=special');
    await assertSpecialMenu(special.map(item=>item.label));
    await page.goto(origin+'/?entry=tools');
    await page.getByRole('heading',{name:'选择其它功能入口',exact:true}).waitFor();
    assert.deepEqual(await page.locator('.tool-card').evaluateAll(nodes=>nodes.map(node=>node.getAttribute('aria-label'))),toolLabels.map(label=>'选择'+label),'other tools retain knowledge/documents/daily and no moved special entries');
    for(const item of special)assert.equal(await page.getByRole('button',{name:'选择'+item.label,exact:true}).count(),0,item.id+': removed from other tools');
    await fitAndCapture('tools-'+width);
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).click();
    await page.getByRole('heading',{name:'选择楼栋查看每日任务',exact:true}).waitFor();
    assert.equal(await page.locator('.scope-card').count(),2);
    await legacyBack('/?entry=tools');
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).waitFor();
    await page.goto(v2Url('/?view=functions'));
    await assertV2Groups(v2Labels);
    await fitAndCapture('v2-groups-'+width);
    for(const item of special){
      const entry=width>760?page.locator('.v2-sidebar .n-menu-item').filter({hasText:item.label}):page.locator('.v2-functions').getByRole('button',{name:item.label,exact:true});
      await clickVisible(entry,'V2 '+item.label);
      await page.waitForFunction(path=>new URL(location.href).searchParams.get('__page')===path,item.path);
      await page.locator(item.surface).waitFor();
      assert.equal(new URL(page.url()).searchParams.has('entry'),false,'V2 never navigates to the legacy group');
      await fitAndCapture('v2-'+item.id+'-'+width);
      await page.goto(v2Url('/?view=functions'));await assertV2Groups(v2Labels);
    }
    category='engineer';
    for(const allowed of [...special.map(item=>[item.id]),['critical_guard','daily','knowledge'],[]]){
      modules=allowed;
      const caseName=allowed.join('-')||'none',expected=special.filter(item=>allowed.includes(item.id));
      await page.goto(origin+'/');
      await page.getByRole('heading',{name:'业务模块',exact:true}).waitFor();
      assert.equal(await page.locator('.module-pre_shift').count(),0,'navigation does not grant engineer checklist submission');
      assert.equal(await page.locator('.module-special').count(),expected.length?1:0,caseName+': legacy special home gate');
      assert.equal(await page.locator('.module-risk').count(),allowed.includes('critical_guard')?1:0,caseName+': legacy risk home gate');
      assert.equal(await page.locator('.module-tools').count(),allowed.some(id=>['daily','knowledge'].includes(id))?1:0,caseName+': legacy tools home gate');
      if(expected.length)await clickVisible(page.getByRole('button',{name:'进入专项功能',exact:true}),'single-grant special card');
      else await page.goto(origin+'/?entry=special');
      await assertSpecialMenu(expected.map(item=>item.label));
      await fitAndCapture('legacy-permission-'+caseName+'-'+width);
      await page.goto(origin+'/?entry=risk');
      await page.getByRole('heading',{name:'选择风险管理业务',exact:true}).waitFor();
      assert.equal(await page.locator('.tool-card').count(),allowed.includes('critical_guard')?1:0,caseName+': direct risk menu cannot expose another module');
      await page.goto(origin+'/?entry=tools');
      await page.getByRole('heading',{name:'选择其它功能入口',exact:true}).waitFor();
      assert.deepEqual(await page.locator('.tool-card').evaluateAll(nodes=>nodes.map(node=>node.getAttribute('aria-label'))),allowed.includes('knowledge')?['选择共享知识库','选择南通文件','选择每日任务清单']:[],caseName+': other-tools grants stay separate');
      await page.goto(v2Url('/?view=functions'));
      await assertV2Groups(v2Labels.filter(label=>expected.some(item=>item.label===label)));
      await fitAndCapture('v2-permission-'+caseName+'-'+width);
      // Direct links must remain guarded, not merely disappear from navigation.
      for(const item of special.filter(item=>!allowed.includes(item.id))){
        for(const url of [origin+item.path,v2Url(item.path)]){
          const before=reads.filter(path=>path.startsWith(item.api)).length;
          await page.goto(url);
          await page.getByText('没有该功能权限',{exact:true}).waitFor();
          assert.equal(await page.locator(item.surface).count(),0,item.id+': denied component is not mounted');
          assert.equal(reads.filter(path=>path.startsWith(item.api)).length,before,item.id+': denied links never read business data');
        }
      }
    }
    console.log(`PASS navigation viewport ${viewport.width}x${viewport.height}: legacy/V2 entries, returns and six permission cases`);
  }
  assert(riskScopes.filter(scope=>scope==='B').length>=viewports.length,'each viewport forwards the selected B building to the existing API');
  assert(riskScopes.every(scope=>scope===null||scope===''||['A','B'].includes(scope)),'no ungranted building reaches the risk API');
  assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
  console.log('PASS: real-dist special navigation, checklist-first order, critical-guard/tools separation, all four legacy returns, V2 three-module grouping, six permission cases with denied-link/data isolation at 1440x900/402x874/440x956. No business writes.');
}catch(error){if(page)await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw error;}
finally{await browser.close();await server.close();}
