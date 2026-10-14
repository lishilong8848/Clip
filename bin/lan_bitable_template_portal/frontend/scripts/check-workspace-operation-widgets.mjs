import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/operation-widgets');await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:960},reducedMotion:'reduce'});
const user={open_id:'operation-fixture',name:'操作组件验收',level:'L4',role:'user',category:'engineer',modules:['maintenance','repair_management']};
const project={record_id:'repair-fixture',title:'A楼配电设备检修',building:'A楼',building_codes:['A'],workflow:'维修中',progress_percent:25,followup_count:2,followup_state_verified:true};
const errors=[],reads=[];
let noticeActionAvailable=true;
await context.route('**/*',route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
  assert.equal(req.method(),'GET','choosing a widget operation never sends or saves a notice');reads.push(url.pathname+url.search);
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:[{value:'A',label:'A楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'widget-fixture'}});
  if(url.pathname==='/api/workbench')return ok({records:[{record_id:'plan-fixture',title:'A楼冷机月度维护',source_progress:'未开始'},{record_id:'plan-ended',title:'已结束计划',source_progress:'已结束'}],ongoing:[{active_item_id:'active-fixture',title:'A楼BA系统资料维护',status:'开始'}],source_snapshot_ready:true});
  if(url.pathname==='/api/workbench/lite-detail'){
    const ongoing=url.searchParams.has('active_item_id');
    return ok({fragments:{detail:`<form id="lite-notice-form"><input name="scope" type="hidden" value="A"><input name="work_type" type="hidden" value="maintenance"><input name="notice_type" type="hidden" value="维保通告"><input name="source_record_id" type="hidden" value="plan-fixture">${ongoing?'<input name="target_record_id" type="hidden" value="active-fixture">':''}<label><span>名称</span><input name="title" required value="A楼测试维护"></label><label><span>通告内容</span><textarea name="content" required>维护内容</textarea></label>${(ongoing?noticeActionAvailable?['update','end']:['update']:['start']).map(action=>`<button name="submit_action" value="${action}">${action}</button>`).join('')}</form>`}});
  }
  if(url.pathname==='/api/workbench/draft')return ok({draft:null});
  if(url.pathname==='/api/repair-management/status')return ok({records:[project],stats:{total:1,in_progress:1,completed_total:0}});
  if(url.pathname==='/api/repair-management/records')return ok({records:[project],fields:[],total:1,offset:0});
  if(url.pathname==='/api/repair-management/records/repair-fixture')return ok({record:{...project,record_version:'v1',display_fields:{},raw_fields:{}}});
  if(url.pathname==='/api/repair-management/followups')return ok({fields:[],records:[],total:0,shared_fields:{}});
  if(url.pathname==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
  return ok({items:[],records:[],fields:[],stats:{},people:[],summary:{}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html';
const notice=()=>page.locator('.v2-widget[aria-label="通告办理"]');
const repair=()=>page.locator('.v2-widget[aria-label="维修跟进"]');
try{
  await page.goto(entry);await page.getByRole('button',{name:'自定义工作台',exact:true}).waitFor();
  await page.getByRole('button',{name:'自定义工作台',exact:true}).click();
  for(const title of ['通告办理','维修跟进']){
    await page.getByRole('button',{name:'添加组件',exact:true}).click();
    await page.locator('.v2-library-item').filter({has:page.getByText(title,{exact:true})}).click();
    await page.locator('.v2-widget-editor').getByRole('button',{name:'应用设置',exact:true}).click();
  }
  await page.getByRole('button',{name:'保存布局',exact:true}).click();await page.reload();
  await notice().scrollIntoViewIfNeeded();await notice().getByRole('button',{name:'结束',exact:true}).waitFor();
  assert.equal(await notice().getByText('已结束计划',{exact:true}).count(),0);
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('clipflow:workspace-layout:v1:operation-fixture')));
  assert(saved.widgets.some(item=>item.kind==='notice_actions')&&saved.widgets.some(item=>item.kind==='repair_actions'));
  await page.screenshot({path:path.join(output,'notice-actions.png')});
  await notice().getByRole('button',{name:'结束',exact:true}).click();
  await page.locator('.v2-notice-editor .n-radio-button--checked').getByText('结束',{exact:true}).waitFor();
  assert(reads.some(url=>url.includes('/api/workbench/lite-detail?')&&url.includes('active_item_id=active-fixture')));
  await page.goto(entry);await notice().scrollIntoViewIfNeeded();await notice().getByRole('button',{name:'填写开始',exact:true}).click();
  await page.locator('.v2-notice-editor .n-radio-button--checked').getByText('开始',{exact:true}).waitFor();
  assert(reads.some(url=>url.includes('/api/workbench/lite-detail?')&&url.includes('record_id=plan-fixture')));
  await page.goto(entry);await repair().scrollIntoViewIfNeeded();await repair().getByRole('button',{name:'填写跟进',exact:true}).click();
  await page.locator('.v2-repair-detail').getByRole('heading',{name:'本次跟进',exact:true}).waitFor();
  assert(reads.some(url=>url.includes('/api/repair-management/status?')&&url.includes('state=all')),'reuse the existing unfinished status query');
  assert(reads.some(url=>url.includes('/api/repair-management/followups?')&&url.includes('repair-fixture')));
  await page.goto(entry);await repair().scrollIntoViewIfNeeded();await repair().getByRole('button',{name:'维修单',exact:true}).click();
  await page.locator('.v2-repair-project-form:visible').waitFor();
  noticeActionAvailable=false;await page.goto(entry+'?__page=/workbench-lite&scope=A&work_type=maintenance&active_item_id=active-fixture&notice_action=end');
  await page.getByText(/该记录当前不能结束/).waitFor();
  assert.equal(await page.locator('.v2-notice-editor .n-radio-button--checked').count(),0,'no silent replacement when requested action is unavailable');
  for(const width of [1366,390]){
    await page.setViewportSize({width,height:width<761?844:900});await page.goto(entry);await notice().scrollIntoViewIfNeeded();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    await page.screenshot({path:path.join(output,'widgets-'+width+'.png')});
  }
  user.modules=['learning'];await page.reload();await page.getByRole('button',{name:'自定义工作台',exact:true}).waitFor();
  assert.equal(await notice().count()+await repair().count(),0,'revoked modules cannot retain operation widgets');
  assert.deepEqual(errors,[]);console.log('PASS: optional operation widgets, persisted layout, authorized modules, exact notice actions, no implicit writes, repair followup/project routing and responsive layout.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
