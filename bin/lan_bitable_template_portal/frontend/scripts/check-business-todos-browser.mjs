import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdir,readFile} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const homeFlow=process.argv.includes('--home-flow');
const root=process.cwd(),repo=path.resolve(root,'../../..'),shots=path.join(repo,homeFlow?'output/playwright/business-todos-home':'output/playwright/business-todos');
await mkdir(shots,{recursive:true});
const generated=spawnSync(path.join(repo,'bin/.venv/Scripts/python.exe'),['-c',`
import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import NoticeCommandRegressionBase,VALID_STARTS
t=NoticeCommandRegressionBase();t.setUp()
try:
 row={**VALID_STARTS['maintenance'],'active_item_id':'active-1','record_id':'target-1','target_record_id':'target-1','title':'A楼维保进行中','work_type':'maintenance','scope':'A','building_codes':['A'],'status':'开始','site_photo_count':1}
 plan={**VALID_STARTS['maintenance'],'record_id':'plan-only','source_record_id':'plan-only','title':'A楼计划单独办理','work_type':'maintenance','scope':'A','building_codes':['A'],'source_progress':'未开始'}
 def detail(**params):
  html=render_workbench_lite(payload={'records':[plan],'ongoing':[row]},session={'role':'building','user':{'name':'隔离验收'},'allowed_modules':['maintenance']},scope='A',work_type='maintenance',month='10月',scope_options=[{'value':'A','label':'A楼'}],**params)
  return extract_workbench_lite_fragments(html)['detail']
 print(json.dumps({'active':detail(active_item_id='active-1'),'plan':detail(record_id='plan-only')},ensure_ascii=False))
finally:t.tmp.cleanup()
`],{cwd:repo,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'},maxBuffer:4*1024*1024});
if(generated.status!==0)throw new Error(generated.stderr||generated.error?.message||'无法生成通告测试表单');
const noticeHtml=JSON.parse(generated.stdout);
const server=await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,entry=origin+'/assets/workspace-v2/index.html?__page=/&view=tasks&scope=A';
const browser=await chromium.launch({headless:true}),layouts=layoutApiFixture(),contexts=[],errors=[],reads=[],writes=[];
const allModules=['maintenance','repair_management','critical_guard','risk_inspection','water','learning','signatures'];
const rowNames=['A楼维保进行中','A楼维修未结束','A楼重保检查','A楼风险排查','A楼水表待填写','每日学练','填写个人签名'];
const today=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai'}).format(new Date());
const water={record_id:'water-1',scope:'A',statistic_date:homeFlow?today:'2026-10-17',frequency:'日',shift:'白',meter:'A楼水表待填写',meter_value:null,photo_count:1};
const repair={record_id:'repair-1',title:'A楼维修未结束',scope:'A',building:'A楼',building_codes:['A'],workflow:'维修中',progress_percent:40,is_completed:false,record_version:'v1',display_fields:{'维修名称':'A楼维修未结束','故障维修原因':'设备异常','所属数据中心/楼栋-使用':'A楼'},raw_fields:{}};
const guard={task_id:'guard-1',name:'A楼重保检查',complete:false,status:'published',scopes:['A'],sheet_types:['检查清单'],created_at:'2026-10-17'};
const guardResponse={response_id:'guard-response',task_id:guard.task_id,scope:'A',sheet_type:'检查清单',cells:{checks:{},template_items:[{key:'g1',name:'现场检查',title:'现场检查'}]},version:1,status:'pending',selected_signers:[]};
const question={id:'question-1',question_id:'question-1',stem:'隔离测试选择题',type:'single',type_label:'单选题',bank:'written',options:[{id:'a',text:'选项甲'},{id:'b',text:'选项乙'}],attachments:[]};
const paper={id:'paper-1',date:'2026-10-17',scope:'A',person_id:'person-alice',person:{id:'person-alice',name:'验收人员'},mode:'daily',status:'pending',stats:{total:1,answered:0},questions:[question],shortage:{},version:1,started_at:'2026-10-17 08:00',legacy:false};
let page;
async function client(owner='alice',width=1440,modules=allModules,buildings=['A'],category='other'){
  const context=await browser.newContext({viewport:{width,height:width<700?640:960},reducedMotion:'reduce'});contexts.push(context);
  const state={owner,modules,failRepair:false,failSave:false,doneWater:false,signed:false,readDelay:0,failOnce:new Set(),doneNotice:false,doneRepair:false,doneGuard:false,noticeRetried:false,noticeJob:false};
  const scopeOptions=buildings.map(value=>({value,label:value+'楼'}));
  const reject=(route,message,status=400)=>route.fulfill({status,json:{ok:false,error:message}});
  await context.route('**/*',async route=>{
    const req=route.request(),u=new URL(req.url()),p=u.pathname;if(u.origin!==origin)return route.abort();
    if(p==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
    if(!p.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(p.startsWith('/api/workspace-layouts/')){if(state.failSave&&req.method()==='PUT')return route.fulfill({status:503,json:{ok:false,error:'隔离测试：保存暂不可用'}});if(await layouts.handle(route,owner))return;}
    reads.push({path:p,query:Object.fromEntries(u.searchParams),method:req.method(),owner});
    const scope=u.searchParams.get('scope');
    if(homeFlow&&scope&&!buildings.includes(scope))return reject(route,'隔离测试：楼栋越权',403);
    const record=scope==='B'?{...water,record_id:'water-B',scope:'B',meter:'B楼水表待填写'}:water;
    const attempt=()=>{
      const body=req.postDataJSON();
      writes.push({path:p,body,owner});
      return body;
    };
    if(state.readDelay&&['/api/workbench','/api/repair-management/status'].includes(p))await new Promise(resolve=>setTimeout(resolve,state.readDelay));
    if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'todo-fixture'}});
    if(p==='/api/auth/status')return ok({logged_in:true,default_scope:buildings[0],user:{open_id:owner,name:'验收人员',role:'building',level:'L4',category,modules:state.modules},scope_options:scopeOptions});
    if(p==='/api/pre-shift-checks')return ok({scope,scopes:buildings,roster_ready:true,definition:{items:[],version:1},tasks:[]});
    if(p==='/api/workbench'){
      const ongoing=state.doneNotice||scope==='B'?[]:[{active_item_id:'active-1',record_id:'target-1',work_type:'maintenance',scope:'A',title:rowNames[0],status:'开始'}];
      const records=category==='duty'?[{record_id:'plan-only',source_record_id:'plan-only',work_type:'maintenance',scope:'A',title:'A楼计划单独办理',source_progress:'未开始'}]:[];
      if(state.planSent)ongoing.push({...records[0],active_item_id:'plan-active',record_id:'plan-target',status:'开始'});
      return ok({source_snapshot_ready:true,records,ongoing,records_pagination:{page:1,total_pages:1,total:records.length},ongoing_pagination:{page:1,total_pages:1,total:ongoing.length}});
    }
    if(p==='/api/workbench/lite-detail')return ok({fragments:{detail:u.searchParams.get('record_id')==='plan-only'?noticeHtml.plan:noticeHtml.active}});
    if(homeFlow&&p==='/api/workbench-events')return ok({user_id:owner,items:[{event_id:'evt-active',state:'active',metadata:{summary:'隔离事件通告'},text:'隔离事件正在处理'},{event_id:'evt-ended',state:'ended',metadata:{summary:'已结束隔离事件'}}]});
    if(homeFlow&&p==='/api/workbench-events/options')return ok({user_id:owner,robots:[],scopes:buildings});
    if(homeFlow&&p==='/api/workbench/dingtalk-options')return ok({robots:[{id:'fixture-robot',name:'隔离机器人',scopes:['A'],types:[]}],selected_robot_ids:['fixture-robot']});
    if(homeFlow&&p==='/api/workbench/draft'&&req.method()==='PUT'){
      const body=req.postDataJSON();
      return ok({draft:{client_revision:body.client_revision,fields:body.fields,version:1}});
    }
    if(homeFlow&&p==='/api/workbench-actions'&&req.method()==='POST'){
      const body=attempt();
      if(category==='duty'&&body.action==='start'&&body.source_record_id==='plan-only'){
        state.planSent=true;
        return ok({job_id:'notice-plan-job',phase:'queued'});
      }
      if(state.noticeJob)return reject(route,'本用例只允许创建一条通告任务');
      state.noticeJob=true;
      return ok({job_id:'notice-job',phase:'queued'});
    }
    if(homeFlow&&p==='/api/jobs/notice-job/retry'&&req.method()==='POST'){
      attempt();state.noticeRetried=true;state.doneNotice=true;
      return ok({job_id:'notice-job',phase:'queued'});
    }
    if(homeFlow&&p==='/api/jobs/notice-job')return ok({job_id:'notice-job',...(state.noticeRetried?{phase:'success'}:{phase:'failed',error_retryable:true,error:'隔离测试：通告首次投递失败'})});
    if(homeFlow&&p==='/api/jobs/notice-plan-job')return ok({job_id:'notice-plan-job',phase:'success'});
    if(p==='/api/repair-management/status'){
      if(state.failRepair)return route.fulfill({status:503,json:{ok:false,error:'隔离测试：维修待办读取失败'}});
      if(homeFlow){
        const records=state.doneRepair||scope==='B'?[]:[repair];
        return ok({records,total:records.length,stats:{total:records.length,in_progress:records.length}});
      }
      return ok({records:Number(u.searchParams.get('offset')||0)?[{...repair,record_id:'repair-2',title:'第二页维修项目'}]:[repair],total:51,stats:{total:51,in_progress:51}});
    }
    if(p==='/api/repair-management/records')return ok({fields:[{field_name:'维修名称',field_type:1,editable:true}],records:[repair],total:1});
    if(p==='/api/repair-management/records/repair-1')return ok({record:homeFlow?{...repair,source_repair_ids:['repair-notice'],display_fields:{...repair.display_fields,'维修结束时间（2026）-L':today+' 09:00'}}:repair});
    if(homeFlow&&p==='/api/repair-management/ledger-candidates')return ok({records:[{record_id:'device-1','设备编号':'A-101','设备名称':'配电设备','机楼':'A楼'}],total:1,page:1,options:{},cache:{ready:true}});
    if(p==='/api/repair-management/followups'){
      if(homeFlow&&req.method()==='POST'){
        const body=attempt();
        if(state.failOnce.delete('repair')){
          state.repairOperation=body.operation_id;
          return reject(route,'隔离测试：维修保存响应待核验',503);
        }
        state.doneRepair=Number(body.fields['维修进度'])===1;
        return ok({record_id:'followup-1',fields:body.fields,record_version:'v2'});
      }
      return ok({fields:[{field_name:'维修进展描述',field_type:1,editable:true},{field_name:'维修进度',field_type:2,editable:true}],records:[],total:0,shared_fields:{}});
    }
    if(homeFlow&&state.repairOperation&&p==='/api/repair-management/operations/'+state.repairOperation){
      return ok(req.method()==='POST'?{status:'failed',retryable:true,error:'隔离测试：已核验维修跟进未写入'}:{status:'uncertain'});
    }
    if(p==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
    if(p==='/api/critical-guard/bootstrap')return ok({catalog:{sheets:[{name:'检查清单',title:'检查清单',kind:'check',items:[{key:'g1',name:'现场检查',title:'现场检查'}]}]},scope_options:scopeOptions});
    if(p==='/api/critical-guard/tasks')return ok({tasks:scope==='B'?[]:[{...guard,complete:state.doneGuard}]});
    if(p==='/api/critical-guard/tasks/guard-1')return ok({...guard,responses:[guardResponse]});
    if(homeFlow&&p==='/api/critical-guard/responses/guard-response'&&req.method()==='PUT'){
      const body=attempt();
      if(state.failOnce.delete('guard'))return reject(route,'隔离测试：重保未提交');
      state.doneGuard=body.generate_image===true;
      return ok({...guardResponse,cells:body.cells,version:2,status:'submitted'});
    }
    if(p==='/api/risk-inspections')return ok({items:[{id:'risk-1',name:'A楼风险排查',total:1,submitted:0,approved:0,status:'active',deadline:1792202400000}],total:1,page:1,is_manager:false});
    if(p==='/api/risk-inspections/risk-1')return ok({id:'risk-1',name:'A楼风险排查',status:'active',fields:[{key:'f1',field_name:'排查结果',type:1,role:'input',required:true}],people:[{id:owner,name:'验收人员',scope:'A',version:1,items:[{id:'item-1',title:'设备状态检查',status:'pending',values:{},files:[]}]}],self_id:owner,is_manager:false,is_reviewer:false,jobs:[],audit:[],deadline:1792202400000});
    if(p==='/api/capacity/water/bootstrap')return ok({scope:record.scope,options:{meters:[record.meter],frequencies:['日'],shifts:['白','夜']},snapshot:{exists:true,snapshot_version:1},permissions:{is_admin:true,can_create:true}});
    if(p==='/api/capacity/water/records')return ok({records:[{...record,meter_value:scope!=='B'&&state.doneWater?123:null}],total:1,snapshot:{exists:true}});
    if(p==='/api/capacity/water/records/'+record.record_id){
      if(req.method()==='PATCH'){
        attempt();
        if(state.failOnce.delete('water'))return reject(route,'隔离测试：水表读数未保存');
        state.doneWater=true;
        return ok({record_id:record.record_id,warnings:[]});
      }
      return ok({...record,meter_value:state.doneWater?123:null,version:'v1',photos:[{image_id:'photo-1',name:'现场.png'}],edit_policy:{is_admin:true,can_edit:true},previous_value_text:'100'});
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
  const page=await context.newPage();page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));page.on('console',e=>{if(e.type()==='error'&&!e.text().startsWith('Failed to load resource:'))errors.push(e.text());});
  await page.goto(homeFlow?entry.replace('view=tasks&scope=A','view=home&scope='+buildings[0]):entry);
  if(homeFlow)await page.locator('.wb-todo-panel').waitFor();
  else {await page.locator('.v2-todo-group').first().waitFor();await page.waitForFunction(()=>!document.querySelector('.v2-todo-toolbar .n-base-selection--disabled'));}
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
async function checkHomeFlow(){
  const desktop=await client('workbench-operator',1440,allModules,['A','B']);
  page=desktop.page;
  const button=name=>page.getByRole('button',{name,exact:true});
  const todo=name=>page.locator('.wb-todo-panel').getByRole('button',{name:'办理 '+name,exact:true});
  const noOverflow=async()=>assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  const returnAfter=async name=>{
    await button('返回工作台').click();
    await page.locator('.wb-todo-panel').waitFor();
    assert.equal(new URL(page.url()).searchParams.get('scope'),'A');
    await todo(rowNames[3]).waitFor();
    await page.locator('.wb-todo-toolbar .wb-link:enabled').waitFor();
    await todo(name).waitFor({state:'hidden'});
  };
  const called=(route,scope='A')=>assert(reads.some(r=>r.path===route&&r.query.scope===scope),route+' should load the selected record');
  const theme=()=>page.locator('.wb-primary').evaluate(el=>{
    const css=getComputedStyle(el);
    return [css.backgroundColor,css.color,getComputedStyle(document.querySelector('.wb-todo-panel')).backgroundColor];
  });
  const checkThemeLinks=()=>page.waitForFunction(()=>{
    const link=document.querySelector('.wb-task-action span:last-child');
    const tab=document.querySelector('.wb-period-select [aria-pressed="true"]');
    const accent=getComputedStyle(document.documentElement).getPropertyValue('--v2-palette-accent-text').trim();
    const rgb='rgb('+[1,3,5].map(index=>parseInt(accent.slice(index,index+2),16)).join(', ')+')';
    return link&&tab&&getComputedStyle(link).color===rgb&&getComputedStyle(tab).color===rgb;
  });
  for(const name of [rowNames[0],rowNames[1],rowNames[2],rowNames[4]])await todo(name).waitFor();
  await noOverflow();
  const toolbar=await page.locator('.wb-todo-toolbar').boundingBox();
  assert(toolbar.height<=38,'desktop title, filters and refresh share one compact row');
  assert((await todo(rowNames[1]).boundingBox()).height<=42,'desktop tasks stay compact');
  await checkThemeLinks();
  await page.screenshot({path:path.join(shots,'home-desktop.png')});
  const defaultTheme=await theme();
  await button('导航配色').click();
  await button('松绿薄荷').click();
  await page.waitForFunction(()=>document.querySelector('.workspace-v2')?.dataset.v2Palette==='mint');
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('.wb-primary')).backgroundColor==='rgb(8, 122, 97)');
  const mintTheme=await theme();
  await checkThemeLinks();
  assert.notEqual(mintTheme[0],defaultTheme[0],'workbench action inherits the selected palette');
  if(!await page.getByRole('switch',{name:'夜间模式'}).isVisible())await button('导航配色').click();
  await page.getByRole('switch',{name:'夜间模式'}).click();
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('.wb-todo-panel')).backgroundColor==='rgb(28, 36, 32)'
    &&getComputedStyle(document.querySelector('.wb-primary')).backgroundColor==='rgb(104, 219, 177)');
  assert.notEqual((await theme())[0],mintTheme[0]);
  await checkThemeLinks();
  await button('导航配色').click();
  await page.mouse.move(1000,80);
  await page.locator('.v2-theme-picker').waitFor({state:'hidden'});
  await page.screenshot({path:path.join(shots,'home-mint-dark.png'),fullPage:true});

  await page.locator('.wb-scope select').selectOption('B');
  await todo('B楼水表待填写').waitFor();
  assert.equal(await todo(rowNames[4]).count(),0);
  await page.locator('.wb-scope select').selectOption('A');
  await todo(rowNames[0]).click();
  const editor=page.locator('.v2-notice-editor:visible');
  await editor.waitFor();
  assert(reads.some(r=>r.path==='/api/workbench/lite-detail'&&r.query.scope==='A'&&r.query.active_item_id==='active-1'));
  await editor.getByText('结束',{exact:true}).last().click();
  await editor.getByRole('checkbox',{name:'隔离机器人'}).check();
  await button('预览并确认结束').click();
  await button('确认发送结束').click();
  await editor.getByText('隔离测试：通告首次投递失败',{exact:true}).waitFor();
  await button('重试原任务').click();
  await editor.getByRole('status',{name:'通告办理完成',exact:true}).waitFor();
  const noticeWrites=writes.filter(w=>w.owner===desktop.state.owner&&w.path==='/api/workbench-actions');
  assert.equal(noticeWrites.length,1);
  assert.equal(noticeWrites[0].body.action,'end');
  assert.equal(noticeWrites[0].body.target_record_id,'target-1');
  assert.deepEqual(noticeWrites[0].body.dingtalk_robot_ids,['fixture-robot']);
  await editor.getByRole('button',{name:'返回列表',exact:true}).click();
  await returnAfter(rowNames[0]);
  await todo(rowNames[1]).waitFor();

  desktop.state.failOnce.add('repair');
  await todo(rowNames[1]).click();
  const repairDrawer=page.locator('.v2-repair-detail');
  await repairDrawer.getByRole('heading',{name:'本次跟进',exact:true}).waitFor();
  called('/api/repair-management/records/repair-1');
  const description=repairDrawer.getByRole('textbox',{name:'维修进展描述',exact:true});
  await description.fill('隔离验收：更换完成，复测正常');
  const progress=repairDrawer.getByPlaceholder('0–100');
  await progress.fill('100');
  await progress.press('Tab');
  await button('选择台账设备').click();
  const picker=page.locator('.record-picker-dialog');
  await picker.getByRole('checkbox').first().check();
  await picker.getByRole('button',{name:'确认',exact:true}).click();
  await button('保存本次跟进').click();
  await page.getByText('隔离测试：维修保存响应待核验',{exact:false}).first().waitFor();
  assert.equal(await description.inputValue(),'隔离验收：更换完成，复测正常');
  assert(await button('保存本次跟进').isDisabled());
  await button('核验原任务').click();
  await page.getByText('隔离测试：已核验维修跟进未写入',{exact:false}).first().waitFor();
  await button('保存本次跟进').click();
  await page.getByText('维修跟进记录已新增。',{exact:false}).first().waitFor();
  const repairWrites=writes.filter(w=>w.path==='/api/repair-management/followups').map(w=>w.body);
  assert.equal(repairWrites.length,2);
  for(const body of repairWrites){
    assert.equal(body.summary_record_id,'repair-1');
    assert.equal(Number(body.fields['维修进度']),1);
    assert.deepEqual(body.ledger_device_ids,['device-1']);
  }
  await button('关闭维修项目').click();
  await returnAfter(rowNames[1]);

  const phone=await client('workbench-mobile',390,allModules);
  page=phone.page;
  await todo(rowNames[2]).waitFor();
  await noOverflow();
  const mobileSearch=await page.getByRole('searchbox',{name:'搜索待办'}).boundingBox();
  assert(mobileSearch.height>=44&&mobileSearch.width>=90,'mobile filters retain usable touch targets');
  assert((await todo(rowNames[2]).boundingBox()).height>=44);
  await page.screenshot({path:path.join(shots,'home-mobile.png')});
  phone.state.failOnce.add('guard');
  await todo(rowNames[2]).click();
  await button('异常').click();
  called('/api/critical-guard/tasks/guard-1');
  const guardNote=page.getByPlaceholder('异常备注必填');
  await guardNote.fill('隔离验收：检查项待整改');
  await button('生成图片').click();
  await page.getByText('隔离测试：重保未提交',{exact:false}).first().waitFor();
  assert.equal(await guardNote.inputValue(),'隔离验收：检查项待整改');
  await button('生成图片').click();
  await page.getByText('检查图片已生成。',{exact:false}).first().waitFor();
  const guardWrites=writes.filter(w=>w.path==='/api/critical-guard/responses/guard-response').map(w=>w.body);
  assert.equal(guardWrites.length,2);
  assert.deepEqual(guardWrites[0],guardWrites[1]);
  assert.equal(guardWrites[1].scope,'A');
  await returnAfter(rowNames[2]);

  phone.state.failOnce.add('water');
  await todo(rowNames[4]).click();
  const reading=page.getByLabel('本次水表读数',{exact:true});
  await reading.waitFor();
  called('/api/capacity/water/records/water-1');
  await reading.pressSequentially('123');
  await noOverflow();
  await page.screenshot({path:path.join(shots,'water-mobile.png')});
  await button('保存记录').click();
  await page.getByText('隔离测试：水表读数未保存',{exact:false}).first().waitFor();
  assert.equal(await reading.inputValue(),'123');
  await button('保存记录').click();
  await reading.waitFor({state:'hidden'});
  const waterWrites=writes.filter(w=>w.path==='/api/capacity/water/records/water-1').map(w=>w.body);
  assert.equal(waterWrites.length,2);
  assert(waterWrites.every(body=>body.scope==='A'&&body.meter_value===123));
  await returnAfter(rowNames[4]);
  await todo(rowNames[0]).waitFor();
  assert(!desktop.state.doneWater&&!desktop.state.doneGuard);

  const restricted=await client('workbench-water-only',320,['water'],['B']);
  page=restricted.page;
  await todo('B楼水表待填写').waitFor();
  await noOverflow();
  assert.equal(await page.locator('.wb-todo-panel .wb-task-row').count(),1);
  assert(reads.filter(r=>r.owner===restricted.state.owner&&r.query.scope).every(r=>r.query.scope==='B'));
  const prohibited=entry.replace('__page=/&view=tasks&scope=A','__page=/repair-management&record_id=repair-1&scope=A');
  await page.goto(prohibited);
  await page.getByText('没有该功能权限',{exact:true}).waitFor();
  assert(!reads.some(r=>r.owner===restricted.state.owner&&/workbench\/lite-detail|repair-management|critical-guard/.test(r.path)));

  const duty=await client('shared-duty-home',390,['maintenance','learning','event'],['A'],'duty');
  page=duty.page;
  const homeUrl=page.url();
  const plans=page.getByRole('list',{name:'计划通告',exact:true});
  const ongoing=page.getByRole('list',{name:'进行中通告',exact:true});
  const plannedNotice=plans.getByRole('button',{name:'开始 A楼计划单独办理',exact:true});
  await ongoing.getByRole('button',{name:'更新／结束 A楼维保进行中',exact:true}).waitFor();
  await plannedNotice.waitFor();
  await page.screenshot({path:path.join(shots,'shared-duty-overview-mobile.png')});
  await plannedNotice.click();
  const inlineEditor=page.locator('.v2-notice-editor:visible');
  await inlineEditor.getByRole('button',{name:'预览并确认开始',exact:true}).waitFor();
  assert.equal(page.url(),homeUrl);
  assert(reads.some(r=>r.owner===duty.state.owner&&r.path==='/api/workbench/lite-detail'&&r.query.record_id==='plan-only'&&r.query.scope==='A'));
  await inlineEditor.getByRole('checkbox',{name:'本次维保不使用工单'}).check();
  await inlineEditor.getByRole('checkbox',{name:'隔离机器人'}).check();
  await button('预览并确认开始').click();
  await button('确认发送开始').click();
  await inlineEditor.getByRole('status',{name:'通告办理完成',exact:true}).waitFor();
  await inlineEditor.getByRole('button',{name:'返回列表',exact:true}).click();
  await inlineEditor.waitFor({state:'hidden'});
  await ongoing.getByRole('button',{name:'更新／结束 A楼计划单独办理',exact:true}).waitFor();
  assert.match(await ongoing.getByRole('button',{name:'更新／结束 A楼计划单独办理',exact:true}).innerText(),/进行中/);
  assert.doesNotMatch(await ongoing.getByRole('button',{name:'更新／结束 A楼计划单独办理',exact:true}).innerText(),/未开始/);
  assert.equal(await plans.getByText('A楼计划单独办理',{exact:true}).count(),0);
  await ongoing.getByRole('button',{name:'办理 隔离事件通告',exact:true}).click();
  const eventModal=page.locator('.wb-inline-event');
  await eventModal.getByText('隔离事件正在处理',{exact:false}).first().waitFor();
  assert.equal(await eventModal.getByText('新建事件通告',{exact:true}).count(),0);
  assert.equal(await eventModal.getByText('已结束隔离事件',{exact:true}).count(),0);
  await page.keyboard.press('Escape');
  await eventModal.waitFor({state:'hidden'});
  await ongoing.getByRole('button',{name:'更新／结束 A楼维保进行中',exact:true}).click();
  await inlineEditor.getByText('结束',{exact:true}).last().click();
  await inlineEditor.getByRole('checkbox',{name:'隔离机器人'}).check();
  await button('预览并确认结束').click();
  await button('确认发送结束').click();
  await inlineEditor.getByText('隔离测试：通告首次投递失败',{exact:true}).waitFor();
  await button('重试原任务').click();
  await inlineEditor.getByRole('status',{name:'通告办理完成',exact:true}).waitFor();
  await inlineEditor.getByRole('button',{name:'返回列表',exact:true}).click();
  await ongoing.getByRole('button',{name:'更新／结束 A楼维保进行中',exact:true}).waitFor({state:'hidden'});
  await ongoing.getByText('A楼计划单独办理',{exact:true}).waitFor();
  assert.equal(page.url(),homeUrl);
  assert(!reads.some(r=>r.owner===duty.state.owner&&r.path.startsWith('/api/learning/')));
  assert(writes.some(w=>w.owner===duty.state.owner&&w.path==='/api/workbench-actions'&&w.body.target_record_id==='target-1'));
  await noOverflow();
  await page.screenshot({path:path.join(shots,'shared-duty-mobile.png')});
  assert.deepEqual(errors,[]);
  console.log('PASS: home → exact notice / repair / critical guard / water records, failed submissions retained, retries complete and return clears tasks, scope and account isolation, desktop/mobile and shared theme. Screenshots: '+shots);
}
try{
  if(homeFlow)await checkHomeFlow();
  else {
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
  for(const name of ['通告','维修项目','重保填写','专项排查','水耗填写','我的学练','我的签名'])await page.locator('.n-base-select-option').filter({hasText:name}).click();
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
  await group(page,'专项排查').getByRole('button',{name:/A楼风险排查/}).click();await page.locator('.v2-todo-handler').getByText('设备状态检查',{exact:true}).first().waitFor();
  assert(reads.some(r=>r.path==='/api/risk-inspections/risk-1'));await close(page);
  await group(page,'我的学练').getByRole('button',{name:/每日学练/}).click();await page.locator('.v2-todo-handler').getByRole('heading',{name:/隔离测试选择题/}).waitFor();await close(page);
  await group(page,'我的签名').getByRole('button',{name:/填写个人签名/}).click();await page.locator('.v2-todo-handler').getByRole('button',{name:'开始签名',exact:true}).click();
  const canvas=page.locator('.v2-todo-handler canvas');await canvas.waitFor();assert.equal(new URL(page.url()).searchParams.get('view'),'tasks','signature credential stays inside the handler');
  const box=await canvas.boundingBox();assert(box&&box.x>=0&&box.x+box.width<=375&&box.height>=200,'signature canvas fits mobile');await page.mouse.move(box.x+40,box.y+60);await page.mouse.down();await page.mouse.move(box.x+180,box.y+100,{steps:8});await page.mouse.up();
  const ink=await canvas.evaluate(c=>{const data=c.getContext('2d').getImageData(0,0,c.width,c.height).data;let minX=c.width,maxX=0,minY=c.height,maxY=0;for(let y=0;y<c.height;y++)for(let x=0;x<c.width;x++)if(data[(y*c.width+x)*4+3]>100){minX=Math.min(minX,x);maxX=Math.max(maxX,x);minY=Math.min(minY,y);maxY=Math.max(maxY,y);}return {width:maxX-minX,height:maxY-minY};});assert(ink.width>100&&ink.height<70,'embedded portrait signature follows the pointer without 90-degree conversion');
  await close(page);await page.getByText('签名尚未保存',{exact:true}).waitFor();await page.getByRole('button',{name:'继续签名',exact:true}).click();await page.screenshot({path:path.join(shots,'signature-handler-mobile.png')});await close(page);await page.getByRole('button',{name:'放弃并离开',exact:true}).click();
  await group(page,'水耗填写').getByRole('button',{name:/A楼水表待填写/}).click();await page.getByLabel('本次水表读数',{exact:true}).waitFor();
  const waterReading=page.getByLabel('本次水表读数',{exact:true});
  await waterReading.pressSequentially('123');
  assert.equal(await waterReading.inputValue(),'123','keyboard input stays focused inside the business task dialog');
  await page.getByRole('button',{name:'关闭水耗编辑',exact:true}).click();await page.getByText('放弃未保存的水耗填写？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续填写',exact:true}).click();
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
  for(const [label,title,readyText] of [['重保填写','A楼重保检查','检查清单'],['专项排查','A楼风险排查','设备状态检查'],['我的学练','每日学练','隔离测试选择题'],['水耗填写','A楼水表待填写','本次水表读数']]){
    await group(page,label).getByRole('button',{name:new RegExp(title)}).click();await page.locator('.v2-todo-handler').getByText(readyText,{exact:false}).first().waitFor();assert.equal(page.url(),homeUrl);await close(page);
  }
  await group(page,'我的签名').getByRole('button',{name:/填写个人签名/}).click();await page.locator('.v2-todo-handler').getByRole('button',{name:'开始签名',exact:true}).click();await page.locator('.v2-todo-handler canvas').waitFor();assert.equal(page.url(),homeUrl);await close(page);
  for(const current of [home,phone]){
    page=current.page;
    await page.locator('.v2-todo-toolbar .n-base-selection').click();
    for(const name of ['维修项目','重保填写','专项排查','水耗填写','我的学练','我的签名'])await page.locator('.n-base-select-option').filter({hasText:name}).click();
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
  }
}catch(error){if(page){await page.screenshot({path:path.join(shots,'failure.png'),fullPage:true});console.error((await page.locator('body').innerText()).slice(-6500));}console.error(errors);throw error;}
finally{for(const context of contexts)await context.close();await browser.close();await server.close();}
