import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/risk-sheet');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:process.env.CLIPFLOW_RISK_BUILD_DIR||'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];
let page;
const versions=['legacy','v2'].filter(value=>!process.env.CLIPFLOW_RISK_VERSION||value===process.env.CLIPFLOW_RISK_VERSION);
const widths=[1440,390,375,360].filter(value=>!process.env.CLIPFLOW_RISK_WIDTH||value===Number(process.env.CLIPFLOW_RISK_WIDTH));
try{
 assert(versions.length&&widths.length,'Select a supported risk UI version and viewport');
 for(const version of versions){
  for(const width of widths){
   const fields=[{key:'f1',field_name:'排查结果',type:1,role:'input',required:true},
    {key:'f2',field_name:'风险等级',type:3,role:'input',property:{options:[{name:'无异常'},{name:'有风险'}]}},
    {key:'f3',field_name:'现场照片',type:17,role:'input'},
    {key:'f4',field_name:'排查日期',type:5,role:'input'},
    {key:'f5',field_name:'发现异常',type:7,role:'input'},
    {key:'f6',field_name:'异常数量',type:2,role:'input'}];
   if(width===1440)fields[1].property.options.push({name:'跨行\n选项'});
   const rid='a'.repeat(32),people=[{id:'alice',name:'排查人员甲',scope:'A',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'pending',values:{},files:[]}]},
    {id:'bob',name:'排查人员乙',scope:'B',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'submitted',values:{f1:'B楼接线检查正常，已核对现场标识。',f2:'无异常',f4:1792152000000,f5:false,f6:0},files:[]}]}];
   let role='admin',reviewerId='reviewer',reviewerVersion=1,detailReads=0,loseSaveResponse=true,rejectHistorySave=true,failTemplateRead=false;
   let templateVersion='v1',requiredWrites=0;
   let losePublication=version==='v2'&&width===1440,frozenPublication=null;
   const history={id:'c'.repeat(32),source_release_id:rid,name:'标准配电检查模板',revision:1,version:'history-v1',enabled:true,created:Date.now()/1000,updated:Date.now()/1000,creator:'发布管理员',scopes:['A'],legacy:false,cloud:{synced:false},sync_status:{status:'failed',error:'隔离测试备份失败'},
    fields:[{key:'number',field_id:'number',field_name:'排查任务编号',type:1,role:'number',is_primary:true},...structuredClone(fields)],items:[{title:'复用检查项',values:{f1:'检查接线与标识'}}]};
   const basics=[{...structuredClone(history),id:'d'.repeat(32),kind:'base',name:'电气基础模板',items:[],version:'base-v1'},
    {...structuredClone(history),id:'e'.repeat(32),kind:'base',name:'暖通基础模板',items:[],version:'hvac-v1'}];
   const data=()=>({id:rid,name:'十月设备专项风险排查',created:Date.now()/1000,deadline:Date.now()+86400000,status:'active',fields,owner:'发布管理员',reviewer:reviewerId==='reviewer'?'审核主管':'发布管理员',reviewer_id:reviewerId,reviewer_version:reviewerVersion,
    people:structuredClone(role==='alice'?[people[0]]:people),is_manager:role!=='alice',can_manage_release:role!=='alice',is_reviewer:role===reviewerId,self_id:role,jobs:[],audit:[],can_claim:role==='admin'&&!people.some(person=>person.id==='admin'),claim_scopes:['A','B']});
   const ctx=await browser.newContext({viewport:{width,height:width===360?640:width===375?667:width===390?844:900},reducedMotion:'reduce'}),layouts=layoutApiFixture();
   await ctx.route('**/*',async route=>{
    const req=route.request(),u=new URL(req.url());
    if(u.origin!==origin)return route.abort();
    if(u.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
    if(u.pathname==='/assets/vnet-logo.png')return route.fulfill({body:await readFile(path.join(root,'public/assets/vnet-logo.png')),contentType:'image/png'});
    if(!u.pathname.startsWith('/api/'))return route.continue();
    if(await layouts.handle(route,role))return;
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+role,name:role,level:role==='admin'?'L0':role==='reviewer'?'L3':'L4',role:role==='admin'?'admin':'building',home_scope:'A',scope_rules:['A','B'],modules:role==='admin'?['risk_inspection','critical_guard','daily']:['risk_inspection']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}],default_scope:'A'});
    if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'risk-fixture'}});
    if(!u.pathname.startsWith('/api/risk-inspections')){assert.equal(req.method(),'GET','No unrelated writes');return ok({items:[],records:[],scopes:{},summary:{},total:0});}
    const p=u.pathname.slice('/api/risk-inspections'.length);
    if(req.method()!=='GET')writes.push({version,width,path:p});
    if(p==='/template'){assert.equal(req.method(),'GET','Creating a template must not edit the old cloud template');return ok({fields:history.fields,version:templateVersion});}
    if(p==='/base-templates'){
     if(req.method()==='POST'){const body=req.postDataJSON();assert(!basics.some(template=>template.id===body.id));basics.push({...structuredClone(basics[0]),...body,kind:'base',version:'new-base-v1',revision:1});return ok(basics.at(-1));}
     return ok({items:basics.map(template=>({...template,item_count:template.items.length,field_count:template.fields.length})),page:1,total:basics.length});
    }
    if(p.startsWith('/base-templates/')){
     const template=basics.find(template=>p==='/base-templates/'+template.id);assert(template);
     if(failTemplateRead){failTemplateRead=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试模板读取失败'}});}
     if(req.method()==='PUT'){const body=req.postDataJSON();assert.equal(body.version,template.version);Object.assign(template,body,{revision:template.revision+1,version:'base-v'+(++requiredWrites+1)});}
     return ok(template);
    }
    if(p==='/templates')return ok({items:[{...history,item_count:history.items.length,field_count:history.fields.length}],total:1,page:1});
    if(p==='/templates/'+history.id){if(req.method()==='PUT'){const body=req.postDataJSON();assert.equal(body.version,history.version);assert.equal(body.fields.find(field=>field.key==='f1').required,false,'History keeps the explicitly optional requirement');assert.deepEqual(body.fields.find(field=>field.key==='f2').property.options,history.fields.find(field=>field.key==='f2').property.options,'History requirement changes must preserve existing options');if(width===1440)assert.equal(body.fields.find(field=>field.key==='f2').required,true);if(rejectHistorySave){rejectHistorySave=false;return route.fulfill({status:503,json:{ok:false,error:'模板保存暂未确认，请稍后重试'}});}Object.assign(history,body,{revision:history.revision+1,version:'history-v'+(history.revision+1)});}return ok(history);}
    if(p==='/people')return ok({items:[{id:'alice',name:'排查人员甲',employee_no:'001',scopes:['A'],home_scope:'H',can_review:false},{id:'reviewer',name:'审核主管',employee_no:'002',scopes:['A','B'],home_scope:'A',can_review:true},{id:'admin',name:'发布管理员',employee_no:'003',scopes:['A','B'],home_scope:'A',can_review:true}]});
    if(!p&&req.method()==='GET')return ok({items:[{id:rid,name:'十月设备专项风险排查',deadline:Date.now()+86400000,status:'active',total:2,submitted:0,approved:0,people_count:2,reviewer:'审核主管',can_claim:data().can_claim}],total:1,page:1,is_manager:role!=='alice',can_edit_template:role==='admin'});
    if(!p&&req.method()==='POST'){const body=req.postDataJSON();assert.match(body.id,/^[a-f0-9]{32}$/);assert.equal(body.template_version,body.history_template_id?history.version:basics.find(template=>template.id===body.base_template_id)?.version);assert.equal(body.reviewer_id,'reviewer');assert.deepEqual(body.people,[{id:'alice',scope:'A'}]);assert.equal(body.items[0].title,body.history_template_id?'复用检查项':'现场设备状态检查');if(body.history_template_id){assert.equal(body.items[0].values.f1,'检查接线与标识');assert(!body.items[0].values.f3);assert(!body.items[0].values.f4);}if(losePublication){losePublication=false;frozenPublication=body;return route.fulfill({status:503,json:{ok:false,error:'隔离测试发布回执丢失'}});}if(frozenPublication){assert.deepEqual(body,frozenPublication,'Uncertain publication retries the exact same identity and payload');frozenPublication=null;}return ok(data());}
    if(frozenPublication&&p==='/'+frozenPublication.id)return route.fulfill({status:503,json:{ok:false,error:'隔离测试发布暂无法核验'}});
    if(p.endsWith('/files'))return ok({id:'f'.repeat(32),name:u.searchParams.get('name'),size:req.postDataBuffer().length,field_id:'f3'});
    if(p.endsWith('/claim')){assert.equal(role,'admin');assert.deepEqual(req.postDataJSON(),{scope:'A'});if(!people.some(person=>person.id==='admin'))people.push({id:'admin',name:'发布管理员',scope:'A',version:1,revoked:false,items:[{id:'own-item',title:'本人检查项',status:'pending',values:{},files:[]}]});return ok(data());}
    if(p.endsWith('/reviewer')){const body=req.postDataJSON();assert.equal(body.version,reviewerVersion);reviewerId=body.reviewer_id;reviewerVersion++;return ok(data());}
    if(p.endsWith('/retry')){assert.deepEqual(req.postDataJSON(),{kind:'template_sync'},'Backup retry must not retry messages');return ok(data());}
    if(p.endsWith('/actions')){
     const body=req.postDataJSON(),person=people.find(v=>p.includes('/people/'+v.id+'/'));
     assert.equal(body.version,person.version);
     const item=person.items[0];
     if(body.action==='submit'&&!body.values.f1)return route.fulfill({status:400,json:{ok:false,error:'请填写：排查结果'}});
     if(['save','submit'].includes(body.action)){item.values=Object.fromEntries(Object.entries(body.values).sort(([a],[b])=>b.localeCompare(a)));item.files=(item.values.f3||[]).map(id=>({id,name:'fixture.png'}));if(body.action==='submit'){item.status='submitted';item.submitted_at=Date.now()/1000;}}
     if(body.action==='return'){item.status='returned';item.reason=body.reason;}
     if(body.action==='approve'){item.status='approved';item.reviewed_by='审核主管';item.reviewed_at=Date.now()/1000;}
     person.version++;if(body.action==='save'&&loseSaveResponse){loseSaveResponse=false;return route.fulfill({status:503,json:{ok:false,error:'保存回执未收到，请重新读取核对'}});}return ok(data());
    }
    if(p==='/'+rid)detailReads++;
    return ok(data());
   });
   page=await ctx.newPage();page.on('pageerror',e=>errors.push(e.message));page.on('dialog',dialog=>{errors.push('Unexpected browser dialog: '+dialog.type());void dialog.dismiss();});await page.clock.install();
   const url=version==='v2'?origin+'/assets/workspace-v2/index.html?__page=/risk-inspections':origin+'/risk-inspections';
   async function cancelNavigation(){
    const current=page.url();
    await page.evaluate(()=>{const target=new URL(location.href);if(target.pathname==='/assets/workspace-v2/index.html')target.searchParams.set('__page','/');else{target.pathname='/';target.search='';}history.pushState({},'',target.href);window.dispatchEvent(new PopStateEvent('popstate'));});
    await page.getByRole('button',{name:/^继续(编辑|填写)$/}).click();assert.equal(page.url(),current,'Cancelling departure retains the current task');
   }
   if(version==='legacy'){
    await page.goto(origin);await page.getByRole('button',{name:'进入风险管理',exact:true}).waitFor();
    assert.equal(await page.locator('.module-risk .module-secondary-action').count(),0,'Risk entry is inside the risk menu, not under its home card');
    assert.equal(await page.locator('.module-tools .module-secondary-action').count(),0,'Daily tasks are inside the tools menu');
    await page.getByRole('button',{name:'进入风险管理',exact:true}).click();await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();
    await page.getByRole('button',{name:'选择重保管理',exact:true}).waitFor();
    await page.screenshot({path:path.join(output,`legacy-${width}-entry.png`),fullPage:width>=700});
    await page.getByRole('button',{name:'选择风险排查',exact:true}).click();await page.locator('.scope-card').first().getByRole('button',{name:'进入风险排查',exact:true}).click();await page.locator('.risk-task-list').waitFor();
    await page.getByRole('button',{name:'返回',exact:true}).click();await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    await page.goto(origin+'/?entry=tools');
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).waitFor();
   }
   await page.goto(url);await page.locator('.risk-task-list button').first().waitFor();
   await page.getByRole('textbox',{name:'搜索排查任务',exact:true}).fill('没有匹配的演示任务');await page.getByText('本页没有符合条件的任务',{exact:true}).waitFor();
   await page.getByRole('button',{name:'清除筛选',exact:true}).click();await page.locator('.risk-task-list button').first().waitFor();
   await page.getByRole('button',{name:'基础模板',exact:true}).click();await page.locator('.history-list article').nth(1).waitFor();
   assert.equal(await page.locator('.history-list article').count(),2,'Multiple base templates are independently selectable');
   await page.locator('.history-list article').first().getByRole('button',{name:'查看 / 编辑',exact:true}).click();await page.locator('.history-schema summary').click();
   const schemaHeaders=await page.locator('.risk-sheet th').allTextContents();
   assert(basics[0].fields.every((field,index)=>schemaHeaders[index+1].includes(field.field_name)),'Base template preserves the original column order');
   assert.equal(await page.locator('.risk-sheet tbody tr').count(),4,'Metadata is shown by row, not as a vertical field list');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Template scroll stays inside the sheet');
   await page.screenshot({path:path.join(output,`${version}-${width}-template.png`),fullPage:width>=700});
   if(version==='v2'&&width===1440){
    for(const theme of ['dark','light']){
     await page.evaluate(value=>localStorage.setItem('clipflow:workspace-night-mode:ou_admin',value),theme);await page.goto(url);
     await page.getByRole('button',{name:'基础模板',exact:true}).click();await page.locator('.history-list article').first().getByRole('button',{name:'查看 / 编辑',exact:true}).click();await page.locator('.history-schema summary').click();
     assert.equal(await page.evaluate(()=>document.documentElement.dataset.v2Theme),theme);
     if(theme==='dark'){
      const colors=await page.locator('.risk-sheet th').first().evaluate(el=>({background:getComputedStyle(el).backgroundColor,color:getComputedStyle(el).color}));
      const luminance=value=>value.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
      assert((luminance(colors.color)+.05)/(luminance(colors.background)+.05)>=4.5,'Template headers remain readable in night mode');
      await page.screenshot({path:path.join(output,'v2-1440-template-dark.png'),fullPage:true});
     }
    }
   }
   await page.getByRole('button',{name:'修改字段 排查结果',exact:true}).click();await page.getByRole('textbox',{name:'历史字段名称',exact:true}).waitFor();assert.equal(await page.getByRole('switch',{name:'历史字段是否必填',exact:true}).getAttribute('aria-checked'),'true');await page.locator('.risk-history-field-modal').getByRole('button',{name:'取消',exact:true}).click();
   const requiredSwitch=page.getByRole('switch',{name:'排查结果是否必填',exact:true});
   await requiredSwitch.waitFor();assert.equal(await requiredSwitch.getAttribute('aria-checked'),'true');assert.deepEqual(await page.locator('.risk-sheet-required>span').allTextContents(),['必填','不必填','不必填','不必填','不必填','不必填'],'Requirement labels stay outside the switch for readable contrast');assert.equal(await page.locator('.risk-sheet-required').first().evaluate(element=>getComputedStyle(element).columnGap),'8px','Requirement labels have spacing inside table-rendered cells');
   await requiredSwitch.click();assert.equal(requiredWrites,0,'Template edits remain local until saved');await cancelNavigation();
   for(const expected of [false,true]){if(expected)await requiredSwitch.click();await page.getByRole('button',{name:'保存新版本',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});assert.equal(await requiredSwitch.getAttribute('aria-checked'),String(expected));}
   await page.getByRole('button',{name:'模板列表',exact:true}).click();await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.getByRole('textbox',{name:'历史模板名称',exact:true}).fill('第三个基础模板');await page.getByRole('button',{name:'保存基础模板',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});assert.equal(basics.length,3);
   await page.getByRole('button',{name:'排查列表',exact:true}).click();await page.locator('.risk-task-list button').first().waitFor();
   await page.getByRole('button',{name:'发布排查',exact:true}).click();await page.locator('.history-list article').nth(2).waitFor();assert.equal(await page.locator('.risk-publish').count(),0,'Template must be explicitly selected before publication');
   failTemplateRead=true;await page.locator('.history-list article').first().getByRole('button',{name:'选用并填写',exact:true}).click();await page.getByText('隔离测试模板读取失败',{exact:false}).waitFor();assert.equal(await page.locator('.risk-publish').count(),0);
   await page.locator('.history-list article').first().getByRole('button',{name:'选用并填写',exact:true}).click();await page.locator('.risk-publish').waitFor();
   await page.getByPlaceholder('例如：10月电气设备专项排查').fill('隔离测试发布');
   const due=new Date(Date.now()+86400000*7);const dueText=`${due.getFullYear()}-${String(due.getMonth()+1).padStart(2,'0')}-${String(due.getDate()).padStart(2,'0')} 18:00:00`;
   await page.locator('.risk-publish>.risk-fields .n-date-picker input').fill(dueText);await page.locator('.risk-publish>.risk-fields .n-date-picker input').press('Enter');
   await page.locator('.risk-publish>.risk-fields>label').nth(2).locator('.n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByPlaceholder('事项 1：设备、部件或需要检查的内容').fill('现场设备状态检查');
   await page.locator('.risk-publish .risk-sheet').getByRole('textbox',{name:'排查结果',exact:true}).pressSequentially('本次统一检查要求');
   assert.equal(await page.locator('.risk-publish .risk-sheet').getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'本次统一检查要求','Editing a cell retains focus through immutable row updates');
   await page.getByRole('button',{name:'添加事项',exact:true}).click();await page.getByRole('textbox',{name:'排查事项 2',exact:true}).fill('临时第二项');
   await page.getByRole('button',{name:'移除事项 2',exact:true}).click();
   assert.equal(await page.getByRole('textbox',{name:'排查事项 1',exact:true}).inputValue(),'现场设备状态检查','Removing another row cannot change the first item');
   if(version==='v2'&&width===1440){for(let i=0;i<10;i++)await page.getByRole('button',{name:'添加事项',exact:true}).click();await page.getByRole('textbox',{name:'排查事项 11',exact:true}).fill('最后一项');assert.equal(await page.locator('.risk-publish .risk-sheet tbody tr').count(),1,'Only the current sheet page is rendered');await page.getByRole('button',{name:'移除事项 11',exact:true}).click();for(let i=10;i>1;i--)await page.getByRole('button',{name:'移除事项 '+i,exact:true}).click();assert.equal(await page.getByRole('textbox',{name:'排查事项 1',exact:true}).inputValue(),'现场设备状态检查');}
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Publishing table cannot widen the page');
   if(width===1440){const tops=await page.locator('.risk-publish>.risk-fields>label').evaluateAll(nodes=>nodes.map(el=>el.getBoundingClientRect().top));assert(Math.max(...tops)-Math.min(...tops)<2,'Publishing controls share one desktop row');}
   if(version==='v2'&&width<700){const button=page.getByRole('button',{name:'确认发布',exact:true});await button.scrollIntoViewIfNeeded();const action=await button.boundingBox(),nav=await page.locator('.v2-mobile-nav').boundingBox();assert(action.y>=56&&action.y+action.height<=nav.y-2,'The entire publish button stays above the mobile navigation');}
   await page.screenshot({path:path.join(output,`${version}-${width}-publish.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'确认发布',exact:true}).click();
   assert((await page.locator('.risk-confirm').boundingBox()).width<=Math.min(500,width),'Confirmation dialog must remain compact on desktop and fit mobile');
   await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   if(version==='v2'&&width===1440){await page.getByText('发布结果尚未确认，原内容已锁定保留。',{exact:true}).waitFor();assert(await page.getByRole('textbox',{name:'排查表名',exact:true}).isDisabled(),'Uncertain publication is frozen');assert(await page.getByRole('button',{name:'排查列表',exact:true}).isDisabled(),'Cannot discard an unconfirmed publication');await page.getByRole('button',{name:'按原内容重试',exact:true}).click();}
   await page.locator('.risk-feedback-table').waitFor();await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.getByRole('button',{name:'历史模板库',exact:true}).click();await page.getByRole('button',{name:'查看 / 编辑',exact:true}).click();
   await page.locator('.history-schema summary').click();
   assert((await page.locator('.history-schema').boundingBox()).width<=width,'History fields must scroll inside the template, not outside the viewport');
   await page.getByRole('button',{name:'修改字段 排查结果',exact:true}).focus();await page.getByRole('button',{name:'修改字段 排查结果',exact:true}).click();
   await page.getByRole('textbox',{name:'历史字段名称',exact:true}).fill('排查结果');assert.equal(await page.getByRole('switch',{name:'历史字段是否必填',exact:true}).getAttribute('aria-checked'),'true');await page.getByRole('button',{name:'应用到当前草稿',exact:true}).click();
   await page.locator('.risk-history-field-modal').waitFor({state:'hidden'});const historyRequired=page.getByRole('switch',{name:'排查结果是否必填',exact:true});await historyRequired.click();assert.equal(await historyRequired.getAttribute('aria-checked'),'false');assert.equal(history.fields.find(field=>field.key==='f1').required,true,'History requirement changes remain local until saved');
   if(width===1440){await page.getByRole('button',{name:'修改字段 风险等级',exact:true}).click();await page.getByRole('switch',{name:'历史字段是否必填',exact:true}).click();await page.getByRole('button',{name:'应用到当前草稿',exact:true}).click();await page.locator('.risk-history-field-modal').waitFor({state:'hidden'});assert.equal(await page.getByRole('switch',{name:'风险等级是否必填',exact:true}).getAttribute('aria-checked'),'true');}
   await page.getByRole('button',{name:'重试备份',exact:true}).click();await page.getByText('已安排重试模板备份。',{exact:true}).waitFor();
   await page.getByRole('textbox',{name:'历史模板名称',exact:true}).fill('标准配电检查模板（修订）');await cancelNavigation();await page.getByRole('button',{name:'保存新版本',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();
   await page.locator('.n-dialog [role=alert]').filter({hasText:'模板保存暂未确认'}).waitFor();
   await page.getByRole('button',{name:'确认保存',exact:true}).click();
   await page.getByText('模板已保存，已有排查不受影响。',{exact:true}).waitFor();
   await page.screenshot({path:path.join(output,`${version}-${width}-history.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'复用发布',exact:true}).click();await page.locator('.risk-publish').waitFor();
   await cancelNavigation();
   assert.equal(await page.locator('.risk-publish>.risk-fields .n-date-picker input').inputValue(),'');
   assert(await page.getByRole('button',{name:'确认发布',exact:true}).isDisabled(),'Recipients, reviewer and deadline must be selected again');
   await page.locator('.risk-publish>.risk-fields .n-date-picker input').fill(dueText);await page.locator('.risk-publish>.risk-fields .n-date-picker input').press('Enter');
   await page.locator('.risk-publish>.risk-fields>label').nth(2).locator('.n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByRole('button',{name:'确认发布',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback-table').waitFor();await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table').waitFor();
   const table=page.locator('.risk-feedback-table');
   if(width===1440){const offset=await page.evaluate(()=>{const counts=document.querySelector('.risk-table-counts').getBoundingClientRect(),filters=document.querySelector('.risk-table-filters').getBoundingClientRect();return Math.abs(counts.y+counts.height/2-filters.y-filters.height/2);});assert(offset<2,'Counts and filters share one desktop row');}
   assert.equal(await table.locator('tbody tr').count(),2,'Both people are visible at once');
   assert.match(await table.innerText(),/B楼接线检查正常/);
   const bobRow=table.locator('tbody tr').filter({hasText:'排查人员乙'});
   assert.equal(await bobRow.locator('td').nth(7).innerText(),'否','False is a real answer, not missing');
   assert.equal(await bobRow.locator('td').nth(8).innerText(),'0','Zero is a real answer, not missing');
   await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('现场标识');
   assert.equal(await table.locator('tbody tr').count(),1,'Feedback text participates in local filtering');
   assert.match(await table.locator('tbody').innerText(),/排查人员乙/);
   await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('');
   for(const next of ['发布管理员','审核主管']){
    await page.getByRole('button',{name:'更换审核人',exact:true}).click();
    await page.locator('.risk-confirm .n-base-selection').click();await page.getByText(next+' · A楼、B楼',{exact:true}).click();
    await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
    await page.getByText('审核人已更换，待审核事项将通知新审核人。',{exact:true}).waitFor();
    await page.locator('.risk-confirm').waitFor({state:'hidden'});
   }
   await page.screenshot({path:path.join(output,`${version}-${width}-management.png`),fullPage:width>=700});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'No page overflow');
   role='alice';
   if(version==='legacy'){await page.goto(origin+'/?entry=risk');await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'选择重保管理',exact:true}).count(),0,'Unassigned risk modules remain hidden');}
   await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table tbody tr').first().waitFor();
   assert.equal(await page.locator('.risk-feedback-table tbody tr').count(),1);
   assert.equal(await page.getByText('排查人员乙',{exact:true}).count(),0,'Private rows must not leak to ordinary users');
   await page.getByRole('button',{name:width<700?'查看 排查人员甲的反馈':/处理 排查人员甲/}).click();await page.locator('.risk-feedback').waitFor();
   await page.getByRole('textbox',{name:'排查结果',exact:true}).focus();const focusedReads=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,focusedReads,'Focused feedback input must retain the existing refresh protection');
   assert.equal(await page.getByRole('button',{name:'撤销该人员'}).count(),0);
   assert.equal(await page.getByRole('button',{name:'更换审核人',exact:true}).count(),0);
   await page.getByRole('button',{name:'提交审核',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-confirm [role=alert]').filter({hasText:'请填写：排查结果'}).waitFor();
   await page.locator('.risk-confirm').getByRole('button',{name:'取消',exact:true}).click();
   await page.locator('.risk-confirm').waitFor({state:'hidden'});
   await page.getByRole('textbox',{name:'排查结果',exact:true}).fill('绝缘、防护及连接状态已核对，无异常。');
   await page.waitForFunction(()=>document.querySelector('.risk-page button[aria-label="刷新"]')?.disabled);
   await page.keyboard.press('Escape');
   await page.getByRole('button',{name:'继续填写',exact:true}).click();
   assert(await page.locator('.risk-feedback').isVisible(),'Cancelling close keeps the feedback form');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'绝缘、防护及连接状态已核对，无异常。','Unsaved input is preserved');
   await page.locator('.risk-feedback .n-base-selection').click();await page.getByText('无异常',{exact:true}).last().click();
   await page.locator('.risk-file-zone input[type=file]').setInputFiles({name:'fixture.png',mimeType:'image/png',buffer:Buffer.from('isolated attachment')});
   await page.locator('.risk-feedback').getByRole('link',{name:'fixture.png'}).waitFor();
   await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('保存回执未收到，请重新读取核对',{exact:false}).waitFor();
   await page.getByRole('button',{name:'重新读取，保留填写',exact:true}).click();await page.getByText('已读取最新状态，未提交的填写仍保留，请核对后继续。',{exact:true}).waitFor();
   assert.equal(await page.locator('.risk-feedback [data-v2-feedback]').count(),0,'Feedback remains inside the drawer and cannot cover mobile save actions');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'绝缘、防护及连接状态已核对，无异常。');
   assert(await page.locator('.risk-page').getByRole('button',{name:'刷新',exact:true}).isEnabled(),'Equivalent saved values must not leave a dirty draft when field order changes');
   await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('草稿已保存。',{exact:true}).waitFor();
   await page.screenshot({path:path.join(output,`${version}-${width}-feedback.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'提交审核',exact:true}).click();const before=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,before,'Confirmation must freeze background detail refresh');await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').getByText('待审核',{exact:true}).waitFor();
   role='reviewer';await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.getByRole('button',{name:/处理 排查人员甲/}).click();await page.locator('.risk-feedback').waitFor();
   await page.getByRole('button',{name:'审核通过',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').filter({hasText:'审核通过'}).waitFor();
   assert.equal(people[0].items[0].status,'approved');
   await page.screenshot({path:path.join(output,`${version}-${width}-review.png`),fullPage:width>=700});
   role='admin';
   people[0].items.push({id:'second-item',title:'另一设备检查',status:'approved',values:{f1:'这是同一人员的第二项独立反馈。'},files:[]});
   for(let index=0;index<6;index++)people.push({id:'preview-'+index,name:'示例人员'+(index+1),scope:index%2?'B':'A',version:1,revoked:false,items:[{id:'item',title:index%2?'冷却系统及环境检查':'配电柜状态检查',status:['pending','submitted','returned','approved'][index%4],values:{f1:index%3?'设备运行正常，现场标识完整。':'发现标识松动，已安排现场核对。',f2:index%3?'无异常':'有风险',f4:1792152000000,f5:index%3===0,f6:index%3?0:1},reason:index%4===2?'补充现场照片后重新提交。':'',files:[]}]});
   await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table tbody tr').nth(8).waitFor();
   await page.getByRole('button',{name:'查看 排查人员甲的反馈 · 另一设备检查',exact:true}).click();
   assert.equal(await page.locator('.risk-feedback .risk-readonly').first().innerText(),'这是同一人员的第二项独立反馈。','Opening another item from the same person must not show the first item');
   await page.keyboard.press('Escape');await page.locator('.risk-feedback').waitFor({state:'hidden'});
   await page.locator('.n-drawer-mask').waitFor({state:'hidden'});await page.locator('.risk-page .n-spin-body').waitFor({state:'hidden'});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Wide fields scroll inside the table');
   await page.screenshot({path:path.join(output,`${version}-${width}-table.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.getByRole('tab',{name:'我的排查任务',exact:true}).click();await page.getByRole('button',{name:'领取并填写',exact:true}).click();
   await page.getByRole('button',{name:'领取本人任务',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback').getByRole('textbox',{name:'排查结果',exact:true}).waitFor();
   assert.equal(await page.locator('.risk-feedback-table tbody tr').count(),1,'My tasks only renders the current account, not other assignees');
   assert.match(await page.locator('.risk-feedback-drawer').innerText(),/发布管理员/);
   await page.waitForFunction(()=>{const drawer=document.querySelector('.n-drawer'),rect=drawer?.getBoundingClientRect();return rect&&rect.x>=-1&&rect.right<=innerWidth+1&&!document.querySelector('.n-modal-mask');});
   await page.screenshot({path:path.join(output,`${version}-${width}-own-task.png`),fullPage:width>=700});
   await ctx.close();
   console.log(`PASS risk inspection ${version} ${width}px`);
  }
 }
 assert.deepEqual(errors,[]);assert.equal(writes.filter(w=>w.path.endsWith('/actions')).length,versions.length*widths.length*5);
 console.log(`PASS risk inspection ${versions.join('/')} at ${widths.join(', ')}px: selected browser workflows and screenshots. No real cloud writes.`);
}catch(e){if(page&&!page.isClosed())await page.screenshot({path:path.join(output,'failure.png'),fullPage:false});throw e;}
finally{await browser.close();await server.httpServer.close();}
