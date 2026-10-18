import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdir,readFile} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=process.cwd(),repo=path.resolve(root,'../../..'),shots=path.join(repo,'output/playwright/business-todos');
await mkdir(shots,{recursive:true});
const generated=spawnSync(path.join(repo,'bin/.venv/Scripts/python.exe'),['-c',`
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import NoticeCommandRegressionBase,VALID_STARTS
t=NoticeCommandRegressionBase();t.setUp()
try:
 row={**VALID_STARTS['maintenance'],'active_item_id':'active-1','record_id':'target-1','target_record_id':'target-1','title':'A楼维保进行中','work_type':'maintenance','scope':'A','building_codes':['A'],'status':'开始','site_photo_count':1}
 html=render_workbench_lite(payload={'records':[],'ongoing':[row]},session={'role':'building','user':{'name':'隔离验收'},'allowed_modules':['maintenance']},scope='A',work_type='maintenance',month='10月',active_item_id='active-1',scope_options=[{'value':'A','label':'A楼'}])
 print(json.dumps(extract_workbench_lite_fragments(html)['detail'],ensure_ascii=False))
finally:t.tmp.cleanup()
`],{cwd:repo,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'},maxBuffer:4*1024*1024});
assert.equal(generated.status,0,generated.stderr);const noticeHtml=JSON.parse(generated.stdout);
const server=await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,entry=origin+'/assets/workspace-v2/index.html?__page=/&view=tasks&scope=A';
const browser=await chromium.launch({headless:true}),layouts=layoutApiFixture(),contexts=[],errors=[],reads=[],writes=[];
const allModules=['maintenance','repair_management','critical_guard','risk_inspection','water','learning','signatures'];
const rowNames=['A楼维保进行中','A楼维修未结束','A楼重保检查','A楼风险排查','A楼水表待填写','每日学练','填写个人签名'];
const water={record_id:'water-1',scope:'A',statistic_date:'2026-10-17',frequency:'日',shift:'白',meter:'A楼水表待填写',meter_value:null,photo_count:1};
const repair={record_id:'repair-1',title:'A楼维修未结束',scope:'A',building:'A楼',building_codes:['A'],workflow:'维修中',progress_percent:40,is_completed:false,record_version:'v1',display_fields:{'维修名称':'A楼维修未结束','故障维修原因':'设备异常','所属数据中心/楼栋-使用':'A楼'},raw_fields:{}};
const guard={task_id:'guard-1',name:'A楼重保检查',complete:false,status:'published',scopes:['A'],sheet_types:['检查清单'],created_at:'2026-10-17'};
const question={id:'question-1',question_id:'question-1',stem:'隔离测试选择题',type:'single',type_label:'单选题',bank:'written',options:[{id:'a',text:'选项甲'},{id:'b',text:'选项乙'}],attachments:[]};
const paper={id:'paper-1',date:'2026-10-17',scope:'A',person_id:'person-alice',person:{id:'person-alice',name:'验收人员'},mode:'daily',status:'pending',stats:{total:1,answered:0},questions:[question],shortage:{},version:1,started_at:'2026-10-17 08:00',legacy:false};
let page;
async function client(owner='alice',width=1440,modules=allModules){
  const context=await browser.newContext({viewport:{width,height:width<700?640:960},reducedMotion:'reduce'});contexts.push(context);
  const state={owner,modules,failRepair:false,failSave:false,doneWater:false,signed:false,readDelay:0};
  await context.route('**/*',async route=>{
    const req=route.request(),u=new URL(req.url()),p=u.pathname;if(u.origin!==origin)return route.abort();
    if(p==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
    if(!p.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(p.startsWith('/api/workspace-layouts/')){if(state.failSave&&req.method()==='PUT')return route.fulfill({status:503,json:{ok:false,error:'隔离测试：保存暂不可用'}});if(await layouts.handle(route,owner))return;}
    reads.push({path:p,query:Object.fromEntries(u.searchParams),method:req.method()});
    if(state.readDelay&&['/api/workbench','/api/repair-management/status'].includes(p))await new Promise(resolve=>setTimeout(resolve,state.readDelay));
    if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'todo-fixture'}});
    if(p==='/api/auth/status')return ok({logged_in:true,default_scope:'A',user:{open_id:owner,name:'验收人员',role:'building',level:'L4',category:'engineer',modules:state.modules},scope_options:[{value:'A',label:'A楼'}]});
    if(p==='/api/workbench')return ok({source_snapshot_ready:true,records:[],ongoing:[{active_item_id:'active-1',record_id:'target-1',work_type:'maintenance',scope:'A',title:rowNames[0],status:'开始'}],records_pagination:{page:1,total_pages:1,total:0},ongoing_pagination:{page:1,total_pages:1,total:1}});
    if(p==='/api/workbench/lite-detail')return ok({fragments:{detail:noticeHtml}});
    if(p==='/api/repair-management/status'){
      if(state.failRepair)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：维修待办读取失败'}});
      return ok({records:Number(u.searchParams.get('offset')||0)?[{...repair,record_id:'repair-2',title:'第二页维修项目'}]:[repair],total:51,stats:{total:51,in_progress:51}});
    }
    if(p==='/api/repair-management/records')return ok({fields:[{field_name:'维修名称',field_type:1,editable:true}],records:[repair],total:1});
    if(p==='/api/repair-management/records/repair-1')return ok({record:repair});
    if(p==='/api/repair-management/followups')return ok({fields:[{field_name:'维修进展描述',field_type:1,editable:true}],records:[],total:0,shared_fields:{}});
    if(p==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(p==='/api/critical-guard/bootstrap')return ok({catalog:{sheets:[{name:'检查清单',title:'检查清单',kind:'check',items:[{key:'g1',name:'现场检查',title:'现场检查'}]}]},scope_options:[{value:'A',label:'A楼'}]});
    if(p==='/api/critical-guard/tasks')return ok({tasks:[guard]});
    if(p==='/api/critical-guard/tasks/guard-1')return ok({...guard,responses:[{response_id:'guard-response',task_id:guard.task_id,scope:'A',sheet_type:'检查清单',cells:{checks:{},template_items:[{key:'g1',name:'现场检查',title:'现场检查'}]},version:1,status:'pending',selected_signers:[]}]});
    if(p==='/api/risk-inspections')return ok({items:[{id:'risk-1',name:'A楼风险排查',total:1,submitted:0,approved:0,status:'active',deadline:1792202400000}],total:1,page:1,is_manager:false});
    if(p==='/api/risk-inspections/risk-1')return ok({id:'risk-1',name:'A楼风险排查',status:'active',fields:[{key:'f1',field_name:'排查结果',type:1,role:'input',required:true}],people:[{id:owner,name:'验收人员',scope:'A',version:1,items:[{id:'item-1',title:'设备状态检查',status:'pending',values:{},files:[]}]}],self_id:owner,is_manager:false,is_reviewer:false,jobs:[],audit:[],deadline:1792202400000});
    if(p==='/api/capacity/water/bootstrap')return ok({scope:'A',options:{meters:[water.meter],frequencies:['日'],shifts:['白','夜']},snapshot:{exists:true,snapshot_version:1},permissions:{is_admin:true,can_create:true}});
    if(p==='/api/capacity/water/records')return ok({records:[{...water,meter_value:state.doneWater?123:null}],total:1,snapshot:{exists:true}});
    if(p==='/api/capacity/water/records/water-1'){
      if(req.method()==='PATCH'){writes.push({path:p,body:req.postDataJSON()});state.doneWater=true;return ok({record_id:'water-1',warnings:[]});}
      return ok({...water,meter_value:state.doneWater?123:null,version:'v1',photos:[{image_id:'photo-1',name:'现场.png'}],edit_policy:{is_admin:true,can_edit:true},previous_value_text:'100'});
    }
    const ownPaper={...paper,person_id:'person-'+owner,person:{id:'person-'+owner,name:'验收人员'}};
    if(p==='/api/learning/bootstrap')return ok({self_person:ownPaper.person,self_scope:'A',can_answer:true,can_view_buildings:false,scopes:[{value:'A',label:'A楼'}],today:'2026-10-17',settings:{},summary:{},sync:{}});
    if(p==='/api/learning/papers')return ok({items:owner==='bob'?[]:[ownPaper],total:owner==='bob'?0:1,page_size:20,page:1});
    if(p==='/api/learning/papers/paper-1')return ok(ownPaper);
    if(p==='/api/signatures/management/account')return ok({person:{is_self:true,can_request:true,name:'验收人员',building:'A',record_id:'person-signature',signature_status:state.signed?'signed':'unsigned'}});
    if(p==='/api/signatures/management/self-start'){writes.push({path:p,body:req.postDataJSON()});return ok({link_url:'/signature?request_id=fixture-sign#token=fixture-only-token'});}
    if(p==='/api/signatures/management/request')return ok({name:'验收人员',status:'ready',building:'A',expires_at:1999999999});
    if(req.method()!=='GET'){errors.push('Unexpected write '+p);return route.fulfill({status:400,json:{ok:false,error:'隔离测试禁止其他写入'}});}
    return ok({items:[],records:[],fields:[],people:[],changes:[],stats:{},options:{},settings:{},summary:{},total:0});
  });
  const page=await context.newPage();page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));page.on('console',e=>{if(e.type()==='error'&&!e.text().startsWith('Failed to load resource:'))errors.push(e.text());});await page.goto(entry);
  await page.locator('.v2-todo-group').first().waitFor();await page.waitForFunction(()=>!document.querySelector('.v2-todo-toolbar .n-base-selection--disabled'));
  return {page,state,context};
}
const group=(page,label)=>page.getByRole('region',{name:label+'待办',exact:true});
const labels=page=>page.locator('.v2-todo-group>header h3').allTextContents();
async function close(page){
  const drawer=page.locator('.water-mobile-drawer:visible').getByRole('button',{name:'关闭水耗编辑',exact:true});
  if(await drawer.isVisible())return drawer.click();
  return page.getByRole('button',{name:'关闭待办办理',exact:true}).click();
}
async function select(page,name){await page.locator('.v2-todo-toolbar .n-base-selection').click();await page.locator('.n-base-select-option').filter({hasText:name}).click();}
try{
  const a=await client();page=a.page;
  for(const name of rowNames)await page.locator('.v2-todo-row').filter({hasText:name}).waitFor();
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:900});
    a.state.readDelay=1400;
    await page.locator('.v2-todo-row').first().evaluate(el=>window.refreshRow=el);
    await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
    await page.waitForTimeout(200);
    assert(await page.evaluate(()=>window.refreshRow.isConnected),'background refresh retains the existing row DOM');
    assert.equal(await page.locator('.v2-todo-skeleton').count(),0,'loaded lists do not return to skeletons');
    assert(await group(page,'通告').getByRole('button',{name:/A楼维保进行中/}).isVisible());
    await page.waitForFunction(()=>!document.querySelector('.v2-todo-group .n-spin'));a.state.readDelay=0;
  }
  await page.setViewportSize({width:1920,height:1000});
  for(const width of [360,760,1120]){
    await page.locator('.v2-business-todos').evaluate((el,w)=>el.style.width=w+'px',width);
    await page.waitForTimeout(100);
    assert(await page.locator('.v2-todo-list').first().evaluate(el=>el.querySelector('.v2-todo-row').getBoundingClientRect().width>=el.getBoundingClientRect().width-2),'ordered business todos remain full-width rows at every component width');
  }
  await page.locator('.v2-business-todos').evaluate(el=>el.style.removeProperty('width'));
  await page.setViewportSize({width:1440,height:900});
  assert.equal((await labels(page)).length,7);
  await group(page,'维修项目').getByRole('button',{name:'继续读取待办',exact:true}).click();await page.getByRole('button',{name:/第二页维修项目/}).waitFor();
  assert(reads.some(r=>r.path==='/api/repair-management/status'&&r.query.offset==='50'));
  await page.screenshot({path:path.join(shots,'all-desktop.png'),fullPage:true});
  await page.locator('.v2-todo-toolbar .n-base-selection').click();
  for(const name of ['通告','维修项目','重保填写','风险排查','水耗填写','我的学练','我的签名'])await page.locator('.n-base-select-option').filter({hasText:name}).click();
  for(const name of ['维修项目','通告'])await page.locator('.n-base-select-option').filter({hasText:name}).click();
  assert.match(await page.locator('.n-base-select-option').filter({hasText:'维修项目'}).innerText(),/1/);
  assert.match(await page.locator('.n-base-select-option').filter({hasText:'通告'}).innerText(),/2/);
  await page.keyboard.press('Escape');await page.waitForFunction(()=>document.querySelectorAll('.v2-todo-group').length===2);
  await page.waitForTimeout(300);assert.deepEqual(await labels(page),['1维修项目','2通告']);
  const b=await client();assert.deepEqual(await labels(b.page),['1维修项目','2通告'],'new browser reuses server selection order');
  const c=await client('bob');assert.equal((await labels(c.page)).length,7,'different account keeps its own defaults');
  const phone=await client('alice',360);assert.equal((await labels(phone.page)).length,7,'mobile layout has independent selection');
  await phone.page.screenshot({path:path.join(shots,'all-mobile-360.png'),fullPage:true});
  assert(!await phone.page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  for(const width of [1440,375]){
    page=width===1440?a.page:phone.page;if(width===375)await page.setViewportSize({width:375,height:667});
    const originalUrl=page.url();
    await page.locator('.v2-todo-row').filter({hasText:rowNames[0]}).click();await page.locator('.v2-notice-editor:visible').waitFor();
    assert.equal(page.url(),originalUrl,'notice edit remains in the workspace');
    await page.screenshot({path:path.join(shots,'notice-handler-'+width+'.png')});
    await page.locator('.v2-notice-editor:visible').getByRole('button',{name:'关闭',exact:true}).click();await close(page);
    await page.locator('.v2-todo-row').filter({hasText:rowNames[1]}).click();await page.locator('.v2-repair-detail').getByRole('heading',{name:'本次跟进',exact:true}).waitFor();
    assert.equal(page.url(),originalUrl);await close(page);
  }
  page=phone.page;await group(page,'重保填写').getByRole('button',{name:/A楼重保检查/}).click();await page.locator('.v2-todo-handler .guard-page').waitFor();
  await page.waitForFunction(()=>document.querySelector('.v2-todo-handler')?.textContent.includes('检查清单'));
  assert(reads.some(r=>r.path==='/api/critical-guard/tasks/guard-1'&&r.query.scope==='A'));
  await page.locator('.v2-todo-handler').getByRole('button',{name:'异常',exact:true}).click();await close(page);await page.getByText('当前表格有未保存修改',{exact:true}).waitFor();await page.getByRole('button',{name:'放弃并继续',exact:true}).click();
  await group(page,'风险排查').getByRole('button',{name:/A楼风险排查/}).click();await page.locator('.v2-todo-handler').getByText('设备状态检查',{exact:true}).first().waitFor();
  assert(reads.some(r=>r.path==='/api/risk-inspections/risk-1'));await close(page);
  await group(page,'我的学练').getByRole('button',{name:/每日学练/}).click();await page.locator('.v2-todo-handler').getByRole('heading',{name:/隔离测试选择题/}).waitFor();await close(page);
  await group(page,'我的签名').getByRole('button',{name:/填写个人签名/}).click();await page.locator('.v2-todo-handler').getByRole('button',{name:'开始签名',exact:true}).click();
  const canvas=page.locator('.v2-todo-handler canvas');await canvas.waitFor();assert.equal(new URL(page.url()).searchParams.get('view'),'tasks','signature credential stays inside the handler');
  const box=await canvas.boundingBox();assert(box&&box.x>=0&&box.x+box.width<=375&&box.height>=200,'signature canvas fits mobile');await page.mouse.move(box.x+40,box.y+60);await page.mouse.down();await page.mouse.move(box.x+180,box.y+100,{steps:8});await page.mouse.up();
  const ink=await canvas.evaluate(c=>{const data=c.getContext('2d').getImageData(0,0,c.width,c.height).data;let minX=c.width,maxX=0,minY=c.height,maxY=0;for(let y=0;y<c.height;y++)for(let x=0;x<c.width;x++)if(data[(y*c.width+x)*4+3]>100){minX=Math.min(minX,x);maxX=Math.max(maxX,x);minY=Math.min(minY,y);maxY=Math.max(maxY,y);}return {width:maxX-minX,height:maxY-minY};});assert(ink.width>100&&ink.height<70,'embedded portrait signature follows the pointer without 90-degree conversion');
  await close(page);await page.getByText('签名尚未保存',{exact:true}).waitFor();await page.getByRole('button',{name:'继续签名',exact:true}).click();await page.screenshot({path:path.join(shots,'signature-handler-mobile.png')});await close(page);await page.getByRole('button',{name:'放弃并离开',exact:true}).click();
  await group(page,'水耗填写').getByRole('button',{name:/A楼水表待填写/}).click();await page.getByLabel('本次水表读数',{exact:true}).waitFor();
  await page.getByLabel('本次水表读数',{exact:true}).fill('123');await page.getByRole('button',{name:'关闭水耗编辑',exact:true}).click();await page.getByText('放弃未保存的水耗填写？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续填写',exact:true}).click();
  await page.screenshot({path:path.join(shots,'water-handler-mobile.png')});
  await page.getByRole('button',{name:'保存记录',exact:true}).click();await page.getByRole('button',{name:'关闭水耗编辑',exact:true}).waitFor({state:'hidden'});await close(page);
  await page.waitForFunction(()=>!Array.from(document.querySelectorAll('.v2-todo-row')).some(el=>el.textContent.includes('A楼水表待填写')));
  assert(writes.some(w=>w.path==='/api/capacity/water/records/water-1'&&w.body.meter_value===123));
  a.state.failRepair=true;page=a.page;await page.getByRole('button',{name:'刷新业务待办',exact:true}).click();await group(page,'维修项目').getByText('隔离测试：维修待办读取失败',{exact:false}).waitFor();
  assert(await group(page,'通告').getByRole('button',{name:/A楼维保进行中/}).isVisible(),'one failed category leaves other categories usable');
  assert(await group(page,'维修项目').getByRole('button',{name:/A楼维修未结束/}).isVisible(),'failed refresh preserves previous records');
  a.state.failRepair=false;await group(page,'维修项目').getByRole('button',{name:'重新读取',exact:true}).click();
  const restricted=await client('restricted',390,['water']);assert.deepEqual(await labels(restricted.page),['1水耗填写']);
  assert.equal(await restricted.page.locator('.v2-todo-group').count(),1);assert(!await restricted.page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  const failed=await client('save-retry');page=failed.page;failed.state.failSave=true;await select(page,'我的签名');await page.keyboard.press('Escape');await page.getByRole('button',{name:'重试保存选择',exact:true}).waitFor();assert.equal((await labels(page)).length,6);failed.state.failSave=false;await page.getByRole('button',{name:'重试保存选择',exact:true}).click();await page.getByRole('button',{name:'重试保存选择',exact:true}).waitFor({state:'hidden'});const recovered=await client('save-retry');assert.equal((await labels(recovered.page)).length,6);
  const home=await client('home');page=home.page;await page.goto(entry.replace('view=tasks','view=home'));await page.locator('.v2-dashboard .v2-business-todos .v2-todo-row').first().waitFor();await page.waitForFunction(()=>!document.querySelector('.v2-business-todos .n-spin'));
  await page.setViewportSize({width:1920,height:1000});
  const taskWidget=page.locator('.grid-stack-item[data-widget-kind="tasks"]').first();
  for(const [width,columns] of [[4,1],[6,2],[12,3]]){
    await taskWidget.evaluate((el,w)=>el.closest('.grid-stack').gridstack.update(el,{w}),width);
    await page.waitForTimeout(350);
    assert.equal(await taskWidget.locator('.v2-todo-list').first().evaluate(el=>getComputedStyle(el).gridTemplateColumns.split(' ').length),columns,'workbench todos adapt to the resized widget, while the standalone page keeps full-width rows');
    await taskWidget.screenshot({path:path.join(shots,'widget-columns-'+columns+'.png')});
  }
  await page.screenshot({path:path.join(shots,'workspace-desktop.png'),fullPage:true});
  const homeUrl=page.url();
  for(const [label,title,readyText] of [['重保填写','A楼重保检查','检查清单'],['风险排查','A楼风险排查','设备状态检查'],['我的学练','每日学练','隔离测试选择题'],['水耗填写','A楼水表待填写','本次水表读数']]){
    await group(page,label).getByRole('button',{name:new RegExp(title)}).click();await page.locator('.v2-todo-handler').getByText(readyText,{exact:false}).first().waitFor();assert.equal(page.url(),homeUrl);await close(page);
  }
  await group(page,'我的签名').getByRole('button',{name:/填写个人签名/}).click();await page.locator('.v2-todo-handler').getByRole('button',{name:'开始签名',exact:true}).click();await page.locator('.v2-todo-handler canvas').waitFor();assert.equal(page.url(),homeUrl);await close(page);
  for(const current of [home,phone]){
    page=current.page;
    await page.locator('.v2-todo-toolbar .n-base-selection').click();
    for(const name of ['维修项目','重保填写','风险排查','水耗填写','我的学练','我的签名'])await page.locator('.n-base-select-option').filter({hasText:name}).click();
    assert.deepEqual(await labels(page),['1通告']);
    const authReads=reads.filter(r=>r.path==='/api/auth/status').length;
    await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));
    await page.waitForFunction(()=>!document.querySelector('.v2-todo-group .n-spin'));
    await page.waitForTimeout(300);
    assert(reads.filter(r=>r.path==='/api/auth/status').length>authReads,'exercise the real auth-status refresh');
    assert.deepEqual(await labels(page),['1通告'],'same permissions must not reset an uncommitted single selection');
    await page.keyboard.press('Escape');
    await page.waitForFunction(()=>!document.querySelector('.v2-todo-toolbar .n-base-selection--disabled'));
    await page.reload();await page.locator('.v2-todo-group').first().waitFor();
    await page.waitForFunction(()=>!document.querySelector('.v2-todo-toolbar .n-base-selection--disabled'));
    assert.deepEqual(await labels(page),['1通告'],'single category survives account-layout save and reload');
  }
  assert.deepEqual(errors,[]);console.log('PASS: numbered multi-select order, account/PC/mobile isolation, pagination, seven real module handlers without navigation, water save+unsaved guard, isolated failure and permissions. All APIs mocked. Screenshots: '+shots);
}catch(error){if(page){await page.screenshot({path:path.join(shots,'failure.png'),fullPage:true});console.error((await page.locator('body').innerText()).slice(-6500));}console.error(errors);throw error;}
finally{for(const context of contexts)await context.close();await browser.close();await server.close();}
