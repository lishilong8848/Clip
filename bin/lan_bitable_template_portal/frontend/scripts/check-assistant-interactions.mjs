import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/assistant-interactions');
await mkdir(output,{recursive:true});
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:width<700?844:1000},reducedMotion:'reduce'});
  const layouts=layoutApiFixture();
  let version=1,turns=[],messageWrites=[],planWrites=[];
  const turn=(id,extra)=>({operation_id:id,question:'整理工作安排',answer:'请补充以下信息',status:'completed',at:Date.now()/1000,scopes:['A'],sources:[],...extra});
  const ask=(id,clarification)=>{turns=[turn(id,{clarification})];version++;};
  ask('ask-a',{options:['新员工','工程师']});
  const state=()=>({conversation_id:'fixture-interactions',enabled:true,configured:true,busy:false,turns,models:[],can_manage_settings:false});
  await context.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url());
   if(url.origin!==origin)return route.abort();
   if(!url.pathname.startsWith('/api/'))return route.continue();
   if(await layouts.handle(route,'ou_interactions'))return;
   const ok=data=>route.fulfill({json:{ok:true,data}}),body=()=>req.postDataJSON();
   if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_interactions',name:'隔离验收',role:'admin',level:'L0',modules:['assistant']},scope_options:[{value:'A',label:'A楼'}]});
   if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-interactions'}});
   if(url.pathname==='/api/assistant/conversation')return ok(state());
   if(url.pathname==='/api/assistant/conversation/stamp')return ok({stamp:String(version)});
   if(url.pathname==='/api/assistant/messages'){
    messageWrites.push(body());await new Promise(resolve=>setTimeout(resolve,80));
    const next=turn(body().operation_id,{question:body().question,answer:'已收到补充'});turns.push(next);version++;
    return ok({turn:next,run_id:''});
   }
   if(url.pathname.startsWith('/api/assistant/plans/')){
    const plan=turns.at(-1).plan,payload=body();planWrites.push({method:req.method(),payload});
    assert.equal(payload.version,plan.version);plan.version++;
    if(payload.action==='edit'){
     plan.status='needs_input';plan.fields=[{name:'step0.recipient',path:'recipient',label:'收件人',type:'text',value:'测试收件人',required:true},{name:'step0.text',path:'text',label:'消息正文',type:'textarea',value:'原消息',required:true}];
    }else if(req.method()==='PATCH'){
     plan.operations[0].body.text=payload.values['step0.text'];plan.fields=[];plan.status='awaiting_confirmation';
    }else{plan.status='completed';plan.can_edit=false;plan.results=[{ok:true}];}
    version++;return ok(plan);
   }
   if(req.method()!=='GET')return route.fulfill({status:400,json:{ok:false,error:'unexpected test write'}});
   return ok({items:[],records:[],settings:{},enabled:true});
  });
  const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  async function open(){
   await page.goto(origin+'/assets/workspace-v2/index.html?__page=/&view=assistant');
   const launcher=page.getByRole('button',{name:'打开灯塔助手',exact:true});
   await page.locator('.assistant-panel, .assistant-launcher').first().waitFor();
   if(!await page.locator('.assistant-panel').count()){
    await launcher.waitFor();
    for(const close of await page.getByRole('button',{name:'收起此提示',exact:true}).all()){
     if(await close.isVisible())await close.click();
    }
    await launcher.click();
   }
   await page.getByRole('textbox',{name:'询问灯塔助手',exact:true}).waitFor();
  }
  await open();
  const composer=page.getByRole('textbox',{name:'询问灯塔助手',exact:true});await composer.fill('尚未发送的草稿');
  await page.getByRole('radio',{name:'工程师',exact:true}).check();
  await page.getByRole('textbox',{name:'补充说明（可选）',exact:true}).fill('加入现场实例');
  await page.getByRole('button',{name:'补充并继续',exact:true}).click();await page.getByText('已收到补充',{exact:true}).waitFor();
  assert.equal(messageWrites.length,1);assert.equal(messageWrites[0].clarification_operation_id,'ask-a');
  assert.equal(messageWrites[0].question,'工程师\n加入现场实例');assert.deepEqual(messageWrites[0].file_ids,[]);assert.deepEqual(messageWrites[0].commands,[]);
  assert.equal(await composer.inputValue(),'尚未发送的草稿','question replies must not consume the composer draft');
  assert(await page.locator('.assistant-question-form').getByRole('button',{name:'补充并继续',exact:true}).isDisabled(),'older question controls are stale');
  ask('ask-date',{options:[],input_type:'date'});await open();
  await page.locator('.assistant-question-form input[type=date]').fill('2026-10-17');
  await page.getByRole('button',{name:'补充并继续',exact:true}).click();await page.getByText('已收到补充',{exact:true}).waitFor();
  assert.equal(messageWrites.at(-1).clarification_operation_id,'ask-date');assert.equal(messageWrites.at(-1).question,'2026-10-17');
  ask('ask-multiple',{options:['内容','时间','人员'],multiple:true});await open();
  await page.getByRole('checkbox',{name:'内容',exact:true}).check();await page.getByRole('checkbox',{name:'人员',exact:true}).check();
  await page.getByRole('button',{name:'补充并继续',exact:true}).click();await page.getByText('已收到补充',{exact:true}).waitFor();
  assert.equal(messageWrites.at(-1).question,'内容、人员');
  turns=[turn('send-preview',{answer:'请核对后确认',plan:{id:'plan-fixture',version:1,title:'发送测试消息',status:'awaiting_confirmation',confirm_label:'确认发送',can_edit:true,fields:[],results:[],operations:[{name:'发送飞书消息',api_id:'POST /api/feishu-workpartner/messages/send',body:{recipient:'测试收件人',text:'原消息'}}]}})];version++;
  await open();assert.equal(planWrites.length,0);await page.getByRole('button',{name:'确认发送',exact:true}).waitFor();
  await page.getByRole('button',{name:'返回修改',exact:true}).click();await page.getByRole('textbox',{name:'消息正文',exact:true}).fill('修改后再发送');
  await page.getByRole('button',{name:'补充并继续',exact:true}).click();await page.getByRole('button',{name:'确认发送',exact:true}).waitFor();
  assert(planWrites.every(item=>item.method==='PATCH'),'editing cannot execute sending');
  await page.locator('.operation-plan').screenshot({path:path.join(output,'send-preview-'+width+'.png')});
  await page.getByRole('button',{name:'确认发送',exact:true}).click();await page.locator('.operation-plan[data-plan-status=completed]').waitFor();
  assert.equal(planWrites.filter(item=>item.method==='POST').length,1);assert.equal(planWrites.at(-1).payload.stage,'review');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await context.close();
 }
 assert.deepEqual(errors,[]);console.log('PASS: native choices/multiple/date fields, current-question binding, stale controls, composer isolation and editable send/confirm flow on PC/mobile. Every API mocked.');
}finally{await browser.close();await server.close();}
