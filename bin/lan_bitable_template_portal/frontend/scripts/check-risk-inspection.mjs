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
    {key:'f4',field_name:'排查日期',type:5,role:'inspected_at'},
    {key:'f5',field_name:'发现异常',type:7,role:'input'},
    {key:'f6',field_name:'异常数量',type:2,role:'input'}];
   if(width===1440)fields[1].property.options.push({name:'跨行\n选项'});
   const rid='a'.repeat(32),people=[{id:'alice',name:'排查人员甲',scope:'A',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'pending',values:{},files:[]}]},
    {id:'bob',name:'排查人员乙',scope:'B',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'submitted',values:{f1:'B楼接线检查正常，已核对现场标识。',f2:'无异常',f3:['b'.repeat(32)],f4:1792152000000,f5:false,f6:0},files:[{id:'b'.repeat(32),name:'接线照片.png'}]}]}];
   let role='admin',reviewerId='reviewer',reviewerVersion=1,detailReads=0,loseSaveResponse=true,rejectHistorySave=true,failTemplateRead=false;
   let templateVersion='v1',requiredWrites=0,deleted=false,rejectDeletion=true;
   let losePublication=version==='v2'&&width===1440,frozenPublication=null;
   const history={id:'c'.repeat(32),source_release_id:rid,name:'标准配电检查模板',revision:1,version:'history-v1',enabled:true,created:Date.now()/1000,updated:Date.now()/1000,creator:'发布管理员',scopes:['A'],legacy:false,cloud:{synced:false},sync_status:{status:'failed',error:'隔离测试备份失败'},
    fields:[{key:'number',field_id:'number',field_name:'排查任务编号',type:1,role:'number',is_primary:true},...structuredClone(fields)],items:[{title:'复用检查项',values:{f1:'检查接线与标识'}}]};
   const basics=[{...structuredClone(history),id:'d'.repeat(32),kind:'base',name:'电气基础模板',items:[],version:'base-v1'},
    {...structuredClone(history),id:'e'.repeat(32),kind:'base',name:'暖通基础模板',items:[],version:'hvac-v1'}];
   let libraryFields=structuredClone(history.fields),libraryWrites=0,rejectLibrarySave=true;
   const data=()=>({id:rid,name:'十月设备专项风险排查',created:Date.now()/1000,deadline:Date.now()+86400000,status:'active',fields,owner:'发布管理员',reviewer:reviewerId==='reviewer'?'审核主管':'发布管理员',reviewer_id:reviewerId,reviewer_version:reviewerVersion,
    people:structuredClone(role==='alice'?[people[0]]:people),is_manager:role!=='alice',can_manage_release:role!=='alice',can_delete_release:role!=='alice',is_reviewer:role===reviewerId,self_id:role,jobs:[],audit:[],can_claim:role==='admin'&&!people.some(person=>person.id==='admin'),claim_scopes:['A','B']});
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
    if(p==='/template'){
     if(req.method()==='POST'){
      const body=req.postDataJSON();assert.equal(role,'admin');assert.equal(body.version,templateVersion);assert.equal(body.confirmed,true);
      if(rejectLibrarySave){rejectLibrarySave=false;return route.fulfill({status:503,json:{ok:false,error:'字段库暂未保存，请重试'}});}
      const old=libraryFields.find(field=>field.key===body.field_id);
      if(body.action==='delete'){assert(old&&!old.is_primary);libraryFields=libraryFields.filter(field=>field.key!==body.field_id);}
      else{const key=old?.key||'library'+(libraryWrites+1);const field={key,field_id:key,field_name:body.field_name,type:body.type,role:body.role,required:body.required,is_primary:!!old?.is_primary,property:[3,4].includes(body.type)?{options:body.options.map(name=>({name}))}:{}};if(old)libraryFields=libraryFields.map(item=>item.key===key?field:item);else libraryFields.push(field);}
      templateVersion='library-v'+(++libraryWrites);
     }
     return ok({fields:libraryFields,version:templateVersion});
    }
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
    if(p==='/people')return ok({self_id:role,items:[{id:'alice',name:'排查人员甲',employee_no:'001',scopes:['A'],home_scope:'A',can_review:false},{id:'reviewer',name:'审核主管',employee_no:'002',scopes:['A','B'],home_scope:'A',can_review:true,category:'manager',can_receive_final:true},{id:'admin',name:'发布管理员',employee_no:'003',scopes:['A','B'],home_scope:'A',can_review:true,can_receive_final:false}]});
    if(!p&&req.method()==='GET')return ok({items:deleted?[]:[{id:rid,name:'十月设备专项风险排查',can_delete_release:role!=='alice',deadline:Date.now()+86400000,status:'active',total:2,submitted:0,approved:0,people_count:2,reviewer:'审核主管',can_claim:data().can_claim}],total:deleted?0:1,page:1,is_manager:role!=='alice',can_edit_template:role==='admin'});
    if(!p&&req.method()==='POST'){const body=req.postDataJSON();assert.match(body.id,/^[a-f0-9]{32}$/);assert.equal(body.template_version,body.history_template_id?history.version:basics.find(template=>template.id===body.base_template_id)?.version);assert.deepEqual(body.reviewer_ids,['admin']);assert.deepEqual(body.manager_ids,['reviewer']);assert.deepEqual(body.people,[{id:'alice',scopes:['A']}]);assert.equal(body.items[0].title,body.history_template_id?'复用检查项':'现场设备状态检查');if(body.history_template_id){assert.equal(body.items[0].values.f1,'检查接线与标识');assert(!body.items[0].values.f3);assert(!body.items[0].values.f4);}if(losePublication){losePublication=false;frozenPublication=body;return route.fulfill({status:503,json:{ok:false,error:'隔离测试发布回执丢失'}});}if(frozenPublication){assert.deepEqual(body,frozenPublication,'Uncertain publication retries the exact same identity and payload');frozenPublication=null;}return ok(data());}
    if(frozenPublication&&p==='/'+frozenPublication.id)return route.fulfill({status:503,json:{ok:false,error:'隔离测试发布暂无法核验'}});
    if(p.endsWith('/files'))return ok({id:'f'.repeat(32),name:u.searchParams.get('name'),size:req.postDataBuffer().length,field_id:'f3'});
    if(p.endsWith('/claim')){assert.equal(role,'admin');assert.deepEqual(req.postDataJSON(),{scope:'A'});if(!people.some(person=>person.id==='admin'))people.push({id:'admin',name:'发布管理员',scope:'A',version:1,revoked:false,items:[{id:'own-item',title:'本人检查项',status:'pending',values:{},files:[]}]});return ok(data());}
    if(p.endsWith('/reviewer')){const body=req.postDataJSON();assert.equal(body.version,reviewerVersion);assert.equal(body.reviewer_ids.length,1,'Replacing reviewers after clearing the selector must send only the new reviewer: '+JSON.stringify(body));reviewerId=body.reviewer_ids[0];reviewerVersion++;return ok(data());}
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
    if(p==='/'+rid&&req.method()==='DELETE'){
     assert.notEqual(role,'alice');assert.deepEqual(req.postDataJSON(),{});
     if(rejectDeletion){rejectDeletion=false;return route.fulfill({status:409,json:{ok:false,error:'本任务正在同步或发送通知，请稍后再删除'}});}
     deleted=true;return route.fulfill({status:503,json:{ok:false,error:'删除回执丢失'}});
    }
    if(p==='/'+rid){detailReads++;if(deleted)return route.fulfill({status:410,json:{ok:false,error:'排查任务已删除，不能继续填写或操作。'}});}
    return ok(data());
   });
   page=await ctx.newPage();page.on('pageerror',e=>errors.push(e.message));page.on('dialog',dialog=>{errors.push('Unexpected browser dialog: '+dialog.type());void dialog.dismiss();});await page.clock.install();
   const url=version==='v2'?origin+'/assets/workspace-v2/index.html?__page=/risk-inspections':origin+'/risk-inspections';
   const feedbackRows=()=>page.locator(width>=700?'.risk-feedback-table tbody tr':'.risk-feedback-card');
   const feedbackItem=row=>width>=700?row.locator('td').nth(1).locator('.risk-table-text'):row.locator('.risk-feedback-title');
   async function cancelNavigation(){
    const current=page.url();
    await page.evaluate(()=>{const target=new URL(location.href);if(target.pathname==='/assets/workspace-v2/index.html')target.searchParams.set('__page','/');else{target.pathname='/';target.search='';}history.pushState({},'',target.href);window.dispatchEvent(new PopStateEvent('popstate'));});
    await page.getByRole('button',{name:/^继续(编辑|填写)$/}).click();assert.equal(page.url(),current,'Cancelling departure retains the current task');
    await page.locator('.n-modal-mask').waitFor({state:'hidden'});
   }
   if(version==='legacy'){
    await page.goto(origin);await page.getByRole('button',{name:'进入风险管理',exact:true}).waitFor();
    assert.equal(await page.locator('.module-risk .module-secondary-action').count(),0,'Risk entry is inside the risk menu, not under its home card');
    assert.equal(await page.locator('.module-tools .module-secondary-action').count(),0,'Daily tasks are inside the tools menu');
    await page.getByRole('button',{name:'进入风险管理',exact:true}).click();await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();
    await page.getByRole('button',{name:'选择重保管理',exact:true}).waitFor();
    await page.screenshot({path:path.join(output,`legacy-${width}-entry.png`),fullPage:width>=700});
    await page.getByRole('button',{name:'选择风险排查',exact:true}).click();await page.locator('.scope-card').first().getByRole('button',{name:'进入风险排查',exact:true}).click();await page.locator('.risk-task-list:visible,.risk-mobile-tasks:visible').waitFor();
    await page.getByRole('button',{name:'返回',exact:true}).click();await page.getByRole('heading',{name:'选择楼栋进入风险排查',exact:true}).waitFor();
    await page.goto(origin+'/?entry=tools');
    await page.getByRole('button',{name:'选择每日任务清单',exact:true}).waitFor();
   }
   await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().waitFor();
   await page.screenshot({path:path.join(output,`${version}-${width}-tasks.png`),fullPage:width>=700});
   if(width===1440)assert(await page.locator('.risk-task-list').evaluate(el=>[...el.querySelectorAll('.n-scrollbar-container')].every(node=>node.scrollWidth<=node.clientWidth+1)),'Desktop task columns, deadline and actions fit without horizontal scrolling');
   await page.getByRole('textbox',{name:'搜索排查任务',exact:true}).fill('没有匹配的演示任务');await page.getByText('本页没有符合条件的任务',{exact:true}).filter({visible:true}).waitFor();
   await page.getByRole('button',{name:'清除筛选',exact:true}).click();await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().waitFor();
   const taskSurface=()=>width>=700?page.locator('.risk-task-list tbody tr').first().locator('td').nth(1):page.locator('.risk-mobile-task').first().locator('p');
   const initialReads=detailReads;
   await taskSurface().click({modifiers:['Control']});assert.equal(detailReads,initialReads,'Modified row clicks do not navigate');
   await taskSurface().evaluate(element=>{const range=document.createRange();range.selectNodeContents(element);getSelection().removeAllRanges();getSelection().addRange(range);element.click();getSelection().removeAllRanges();});
   assert.equal(detailReads,initialReads,'Selecting row text does not navigate');
   await taskSurface().click();await page.locator('.risk-feedback-table').waitFor();
   assert.equal(detailReads,initialReads+1,'Clicking a task row or mobile card opens its original detail once');
   await page.getByRole('button',{name:'排查列表',exact:true}).click();await page.locator('.risk-task-name:visible').waitFor();
   const keyboardReads=detailReads;await page.locator('.risk-task-name:visible').focus();await page.keyboard.press('Enter');await page.locator('.risk-feedback-table').waitFor();
   assert.equal(detailReads,keyboardReads+1,'The existing keyboard action opens once without row bubbling');
   await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.getByRole('button',{name:'基础模板',exact:true}).click();
   assert.equal(await page.locator('.risk-field-picker').count(),0,'Both versions enter the template list');
   await page.locator('.history-list article').nth(1).waitFor();
   assert.equal(await page.locator('.history-list article').count(),2,'Multiple base templates are independently selectable');
   await page.locator('.history-list article').first().getByRole('button',{name:'查看 / 编辑',exact:true}).click();await page.locator('.risk-field-picker').waitFor();
   assert.deepEqual(await page.locator('.risk-selected-name strong').allTextContents(),basics[0].fields.filter(field=>!field.is_primary&&field.role==='input').map(field=>field.field_name),'Existing templates retain editable field selection and order');
   const panes=await page.locator('.risk-field-pane').evaluateAll(nodes=>nodes.map(el=>{const r=el.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,right:r.right};}));
   if(width===1440){assert(Math.abs(panes[0].y-panes[1].y)<2);assert(panes[0].right<panes[1].x,'Desktop library is left, selected fields are right');}else assert(panes[1].y>panes[0].y,'Mobile panels stack in reading order');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Field picker stays inside the page');
   await page.locator('.risk-field-picker .n-spin-body').waitFor({state:'hidden'});
   await page.screenshot({path:path.join(output,`${version}-${width}-template.png`),fullPage:width>=700});
   if(version==='v2'&&width===1440){
    for(const theme of ['dark','light']){
     await page.evaluate(value=>localStorage.setItem('clipflow:workspace-night-mode:ou_admin',value),theme);await page.goto(url);
     await page.getByRole('button',{name:'基础模板',exact:true}).click();await page.locator('.history-list article').first().getByRole('button',{name:'查看 / 编辑',exact:true}).click();await page.locator('.risk-field-picker').waitFor();
     assert.equal(await page.evaluate(()=>document.documentElement.dataset.v2Theme),theme);
     if(theme==='dark'){
      const colors=await page.locator('.risk-selected-name strong').first().evaluate(el=>{let node=el;while(node){const background=getComputedStyle(node).backgroundColor;if(background.startsWith('rgb('))return {background,color:getComputedStyle(el).color};node=node.parentElement;}return {background:'rgb(255,255,255)',color:getComputedStyle(el).color};});
      const luminance=value=>value.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);
      assert((luminance(colors.color)+.05)/(luminance(colors.background)+.05)>=4.5,'Selected field names remain readable in night mode');
      await page.screenshot({path:path.join(output,'v2-1440-template-dark.png'),fullPage:true});
     }
    }
   }
   await page.getByRole('button',{name:'修改字段 排查结果',exact:true}).click();await page.getByRole('textbox',{name:'字段库名称',exact:true}).waitFor();assert.equal(await page.getByRole('switch',{name:'字段库是否必填',exact:true}).getAttribute('aria-checked'),'true');await page.locator('.risk-history-field-modal').getByRole('button',{name:'取消',exact:true}).click();
   const requiredSwitch=page.getByRole('switch',{name:'排查结果是否必填',exact:true});
   await requiredSwitch.waitFor();assert.equal(await requiredSwitch.getAttribute('aria-checked'),'true');assert.equal(await page.locator('.risk-field-required>span').count(),5,'Each input retains a required control; automatic dates have none');
   await requiredSwitch.click();assert.equal(requiredWrites,0,'Template edits remain local until saved');await cancelNavigation();
   for(const expected of [false,true]){if(expected)await requiredSwitch.click();await page.getByRole('button',{name:'保存新版本',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});assert.equal(await requiredSwitch.getAttribute('aria-checked'),String(expected));}
   await page.getByRole('button',{name:'排查列表',exact:true}).click();await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().waitFor();
   await page.getByRole('button',{name:'发布排查',exact:true}).click();await page.locator('.history-list article').nth(1).waitFor();assert.equal(await page.locator('.risk-publish').count(),0,'Template must be explicitly selected before publication');
   await page.getByRole('button',{name:'新建基础模板',exact:true}).waitFor();assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Template creation entry fits the chooser');await page.screenshot({path:path.join(output,`${version}-${width}-choose-create.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.locator('.risk-field-picker').waitFor();
   const originalFields=structuredClone(basics[0].fields);
   await page.getByRole('button',{name:'添加字段',exact:true}).click();await page.getByRole('textbox',{name:'字段库名称',exact:true}).fill('现场说明');await page.getByRole('button',{name:'保存字段',exact:true}).click();
   await page.locator('.risk-history-field-modal').getByText('字段库暂未保存，请重试',{exact:true}).waitFor();assert.equal(libraryWrites,0);assert.equal(await page.getByRole('textbox',{name:'字段库名称',exact:true}).inputValue(),'现场说明','Failed library saves retain the field draft');
   await page.getByRole('button',{name:'保存字段',exact:true}).click();await page.locator('.risk-history-field-modal').waitFor({state:'hidden'});
   assert.equal(libraryWrites,1);assert.equal(await page.locator('.risk-selected-fields>li').count(),0,'Saving a library field does not implicitly select it; fixed fields stay hidden in both versions');
   await page.getByRole('button',{name:'修改字段 现场说明',exact:true}).click();await page.getByRole('textbox',{name:'字段库名称',exact:true}).fill('现场补充说明');await page.getByRole('button',{name:'保存字段',exact:true}).click();await page.locator('.risk-history-field-modal').waitFor({state:'hidden'});
   await page.getByRole('button',{name:'模板列表',exact:true}).click();await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.getByRole('button',{name:'添加字段 现场补充说明',exact:true}).waitFor();
   assert.equal(libraryWrites,2,'Added and edited library fields survive reopening');
   await page.getByRole('button',{name:'添加字段',exact:true}).click();await page.getByRole('textbox',{name:'字段库名称',exact:true}).fill('待移除字段');await page.getByRole('button',{name:'保存字段',exact:true}).click();await page.locator('.risk-history-field-modal').waitFor({state:'hidden'});
   await page.getByRole('button',{name:'删除库字段 待移除字段',exact:true}).click();await page.getByRole('button',{name:'确认删除',exact:true}).click();await page.getByRole('button',{name:'删除库字段 待移除字段',exact:true}).waitFor({state:'hidden'});assert.equal(libraryWrites,4);
   assert.deepEqual(basics[0].fields,originalFields,'Field library changes cannot mutate previously generated templates');
   for(const name of ['排查结果','风险等级','现场补充说明'])await page.getByRole('button',{name:'添加字段 '+name,exact:true}).click();
   await page.getByRole('button',{name:'移除已选字段 风险等级',exact:true}).click();assert.equal(libraryWrites,4,'Removing a selected field must not delete the library field');
   assert(await page.getByRole('button',{name:'添加字段 风险等级',exact:true}).isEnabled());
   await page.getByRole('textbox',{name:'基础模板名称',exact:true}).fill('第三个基础模板');await cancelNavigation();
   const generate=page.getByRole('button',{name:'生成基础模板',exact:true});await generate.scrollIntoViewIfNeeded();
   if(version==='v2'&&width<700){const button=await generate.boundingBox(),nav=await page.locator('.v2-mobile-nav').boundingBox();assert(button.y>=56&&button.y+button.height<=nav.y-2,'Generate button stays above mobile navigation');}
   await page.screenshot({path:path.join(output,`${version}-${width}-field-picker.png`),fullPage:width>=700});
   await generate.click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});assert.equal(basics.length,3);
   assert.deepEqual(basics[2].fields.map(field=>field.field_name),['排查任务编号','排查日期','排查结果','现场补充说明'],'Generated templates retain fixed fields and selected inputs');
   assert.equal(writes.filter(write=>write.version===version&&write.width===width&&write.path==='').length,0,'Saving a base template must not publish an inspection');
   await page.getByRole('button',{name:'选用并填写',exact:true}).click();await page.locator('.risk-publish').waitFor();await page.getByText('基础模板：第三个基础模板 · V1',{exact:true}).waitFor();
   await page.getByRole('button',{name:'更换基础模板',exact:true}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();await page.locator('.history-list article').nth(2).waitFor();
   failTemplateRead=true;await page.locator('.history-list article').first().getByRole('button',{name:'选用并填写',exact:true}).click();await page.getByText('隔离测试模板读取失败',{exact:false}).waitFor();assert.equal(await page.locator('.risk-publish').count(),0);
   await page.locator('.history-list article').first().getByRole('button',{name:'选用并填写',exact:true}).click();await page.locator('.risk-publish').waitFor();
   await page.getByPlaceholder('例如：10月电气设备专项排查').fill('隔离测试发布');
   const due=new Date(Date.now()+86400000*7);const dueText=`${due.getFullYear()}-${String(due.getMonth()+1).padStart(2,'0')}-${String(due.getDate()).padStart(2,'0')} 18:00:00`;
   await page.locator('.risk-publish>.risk-fields .n-date-picker input').fill(dueText);await page.locator('.risk-publish>.risk-fields .n-date-picker input').press('Enter');
   assert.match(await page.locator('.n-select[aria-label="审核人"]').innerText(),/发布管理员/);
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.locator('.n-base-select-option:visible').first().waitFor({state:'hidden'});
   await page.locator('.n-select[aria-label="接收经理"] .n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByPlaceholder('事项 1：设备、部件或需要检查的内容').fill('现场设备状态检查');
   await page.locator('.risk-publish .risk-sheet').getByRole('textbox',{name:'排查结果',exact:true}).pressSequentially('本次统一检查要求');
   assert.equal(await page.locator('.risk-publish .risk-sheet').getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'本次统一检查要求','Editing a cell retains focus through immutable row updates');
   await page.getByRole('button',{name:'添加事项',exact:true}).click();await page.getByRole('textbox',{name:'排查事项 2',exact:true}).fill('临时第二项');
   await page.getByRole('button',{name:'移除事项 2',exact:true}).click();
   assert.equal(await page.getByRole('textbox',{name:'排查事项 1',exact:true}).inputValue(),'现场设备状态检查','Removing another row cannot change the first item');
   if(version==='v2'&&width===1440){for(let i=0;i<10;i++)await page.getByRole('button',{name:'添加事项',exact:true}).click();await page.getByRole('textbox',{name:'排查事项 11',exact:true}).fill('最后一项');assert.equal(await page.locator('.risk-publish .risk-sheet tbody tr').count(),1,'Only the current sheet page is rendered');await page.getByRole('button',{name:'移除事项 11',exact:true}).click();for(let i=10;i>1;i--)await page.getByRole('button',{name:'移除事项 '+i,exact:true}).click();assert.equal(await page.getByRole('textbox',{name:'排查事项 1',exact:true}).inputValue(),'现场设备状态检查');}
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Publishing table cannot widen the page');
   if(width===1440){const tops=await page.locator('.risk-publish>.risk-fields').first().locator(':scope>label').evaluateAll(nodes=>nodes.map(el=>el.getBoundingClientRect().top));assert(Math.max(...tops)-Math.min(...tops)<2,'Core publishing controls share one desktop row');}
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
   assert.match(await page.locator('.n-select[aria-label="审核人"]').innerText(),/发布管理员/);
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.locator('.n-base-select-option:visible').first().waitFor({state:'hidden'});
   await page.locator('.n-select[aria-label="接收经理"] .n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByRole('button',{name:'确认发布',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback-table').waitFor();await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().click();await page.locator('.risk-feedback-table').waitFor();
   const table=page.locator('.risk-feedback-table');
   if(width===1440){const offset=await page.evaluate(()=>{const counts=document.querySelector('.risk-table-counts').getBoundingClientRect(),filters=document.querySelector('.risk-table-filters').getBoundingClientRect();return Math.abs(counts.y+counts.height/2-filters.y-filters.height/2);});assert(offset<2,'Counts and filters share one desktop row');}
   assert.equal(await feedbackRows().count(),2,'Both people are visible at once');
   assert.match(await table.innerText(),/B楼接线检查正常/);
   const bobRow=feedbackRows().filter({hasText:'排查人员乙'});
   if(width>=700){assert.equal(await bobRow.locator('td').nth(6).innerText(),'否','False is a real answer, not missing');assert.equal(await bobRow.locator('td').nth(7).innerText(),'0','Zero is a real answer, not missing');assert.equal(await table.getByRole('columnheader',{name:'排查日期',exact:true}).count(),0,'Automatic dates stay hidden');}
   else{assert.equal(await table.locator('table').count(),0,'Mobile feedback uses summaries instead of a horizontally scrolling table');assert.equal(await bobRow.locator('.risk-feedback-summary>div').count(),2);const controls=await bobRow.locator('.risk-feedback-meta button').evaluateAll(nodes=>nodes.map(node=>{const rect=node.getBoundingClientRect();return rect.y+rect.height/2;}));assert(Math.abs(controls[0]-controls[1])<2,'Person and action share a compact row');}
   await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('现场标识');
   assert.equal(await feedbackRows().count(),1,'Feedback text participates in local filtering');
   assert.match((await feedbackRows().allTextContents()).join(' '),/排查人员乙/);
   await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('');
   const bobItem=feedbackItem(bobRow);
   await bobItem.click({modifiers:['Control']});assert.equal(await page.locator('.risk-feedback').count(),0,'Modified feedback clicks do not open an editor');
   await bobItem.evaluate(element=>{const range=document.createRange();range.selectNodeContents(element);getSelection().removeAllRanges();getSelection().addRange(range);element.click();getSelection().removeAllRanges();});
   assert.equal(await page.locator('.risk-feedback').count(),0,'Feedback text stays selectable');
   await page.evaluate(()=>document.addEventListener('click',event=>{if(event.target.closest('a'))event.preventDefault();},{once:true}));
   await bobRow.getByRole('link',{name:'接线照片.png',exact:true}).click();assert.equal(await page.locator('.risk-feedback').count(),0,'Opening an attachment does not open its feedback drawer');
   await bobItem.click();await page.locator('.risk-feedback').waitFor();
   assert.match(await page.locator('.risk-feedback-drawer').innerText(),/排查人员乙/);
   assert.equal(await page.locator('.risk-feedback [data-risk-field="f5"] .risk-readonly').innerText(),'否','False remains an explicit answer');
   assert.equal(await page.locator('.risk-feedback [data-risk-field="f6"] .risk-readonly').innerText(),'0','Zero remains an explicit answer');
   assert.equal(await page.getByRole('button',{name:'提交审核',exact:true}).count(),0,'Row activation preserves read-only permissions for another person');
   await page.keyboard.press('Escape');await page.locator('.risk-feedback').waitFor({state:'hidden'});await page.locator('.n-drawer-mask').waitFor({state:'hidden'});
   for(const next of ['发布管理员','审核主管']){
    await page.getByRole('button',{name:'更换审核人',exact:true}).click();
    await page.locator('.risk-confirm .n-select[aria-label="新审核人"]').waitFor();
    while(await page.locator('.risk-confirm .n-tag__close').count())await page.locator('.risk-confirm .n-tag__close').last().click();await page.locator('.risk-confirm .n-base-suffix').click();await page.getByText(next+' · '+(next==='发布管理员'?'003':'002'),{exact:true}).click();await page.keyboard.press('Escape');
    assert.deepEqual(await page.locator('.risk-confirm .n-tag').allTextContents(),[next+' · '+(next==='发布管理员'?'003':'002')],'Cleared reviewer selection contains only the chosen reviewer');
    await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
    await page.getByText('审核人已更换，待审核事项将通知新审核人。',{exact:true}).waitFor();
    await page.locator('.risk-confirm').waitFor({state:'hidden'});
    assert.equal(reviewerId,next==='发布管理员'?'admin':'reviewer','Reviewer selection must be committed before changing accounts');
   }
   await page.screenshot({path:path.join(output,`${version}-${width}-management.png`),fullPage:width>=700});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'No page overflow');
   role='alice';fields[2].required=true;
   if(version==='legacy'){await page.goto(origin+'/?entry=risk');await page.getByRole('button',{name:'选择风险排查',exact:true}).waitFor();assert.equal(await page.getByRole('button',{name:'选择重保管理',exact:true}).count(),0,'Unassigned risk modules remain hidden');}
   await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().click();await feedbackRows().first().waitFor();
   assert.equal(await feedbackRows().count(),1);
   assert.equal(await page.getByText('排查人员乙',{exact:true}).count(),0,'Private rows must not leak to ordinary users');
   await feedbackItem(feedbackRows().first()).click();await page.locator('.risk-feedback').waitFor();
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).isEditable(),true,'Clicking an assigned feedback row opens the original editable form');
   await page.getByRole('textbox',{name:'排查结果',exact:true}).focus();const focusedReads=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,focusedReads,'Focused feedback input must retain the existing refresh protection');
   assert.equal(await page.getByRole('button',{name:'撤销该人员'}).count(),0);
   assert.equal(await page.getByRole('button',{name:'更换审核人',exact:true}).count(),0);
   const beforeIncomplete=writes.length;
   await page.getByRole('button',{name:'提交审核',exact:true}).click();await page.locator('.risk-missing-summary').getByText('还有 2 项必填信息待补充',{exact:true}).waitFor();
   assert.equal(writes.length,beforeIncomplete,'Missing answers cannot issue an action request');assert.equal(await page.locator('.risk-confirm').count(),0,'Missing answers do not open the confirmation');
   await page.waitForFunction(()=>document.activeElement?.getAttribute('aria-label')==='排查结果');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).getAttribute('aria-invalid'),'true');
   const errorDescription=await page.getByRole('textbox',{name:'排查结果',exact:true}).getAttribute('aria-describedby');assert(errorDescription);assert.equal(await page.locator('[id="'+errorDescription+'"]').innerText(),'请填写排查结果');
   await page.getByRole('textbox',{name:'排查结果',exact:true}).fill('   ');await page.getByRole('button',{name:'提交审核',exact:true}).click();assert.equal(writes.length,beforeIncomplete,'Whitespace cannot bypass required validation');
   await page.getByRole('textbox',{name:'排查结果',exact:true}).fill('绝缘、防护及连接状态已核对，无异常。');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).getAttribute('aria-invalid'),null,'Correcting a field clears its error');
   assert.equal(await page.locator('.risk-feedback [data-risk-field="f1"] .n-input--error-status').count(),0,'A corrected field no longer uses the error appearance');
   await page.getByRole('button',{name:'提交审核',exact:true}).click();await page.waitForFunction(()=>document.activeElement?.classList.contains('risk-file-zone'));
   await page.locator('.risk-missing-summary').getByRole('button',{name:'现场照片',exact:true}).click();assert.equal(writes.length,beforeIncomplete,'Missing attachment links only locate the upload control');
   await page.screenshot({path:path.join(output,`${version}-${width}-missing.png`),fullPage:width>=700});
   await page.waitForFunction(()=>document.querySelector('.risk-page button[aria-label="刷新"]')?.disabled);
   await page.keyboard.press('Escape');
   await page.getByRole('button',{name:'继续填写',exact:true}).click();
   assert(await page.locator('.risk-feedback').isVisible(),'Cancelling close keeps the feedback form');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'绝缘、防护及连接状态已核对，无异常。','Unsaved input is preserved');
   await page.locator('.risk-feedback .n-base-selection').click();await page.getByText('无异常',{exact:true}).last().click();
   await page.locator('.risk-file-zone input[type=file]').setInputFiles({name:'fixture.png',mimeType:'image/png',buffer:Buffer.from('isolated attachment')});
   await page.locator('.risk-feedback').getByRole('link',{name:'fixture.png'}).waitFor();
   await page.locator('.risk-missing-summary').waitFor({state:'hidden'});
   await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('保存回执未收到，请重新读取核对',{exact:false}).waitFor();
   await page.getByRole('button',{name:'重新读取，保留填写',exact:true}).click();await page.getByText('已读取最新状态，未提交的填写仍保留，请核对后继续。',{exact:true}).waitFor();
   assert.equal(await page.locator('.risk-feedback [data-v2-feedback]').count(),0,'Feedback remains inside the drawer and cannot cover mobile save actions');
   assert.equal(await page.getByRole('textbox',{name:'排查结果',exact:true}).inputValue(),'绝缘、防护及连接状态已核对，无异常。');
   assert(await page.locator('.risk-page').getByRole('button',{name:'刷新',exact:true}).isEnabled(),'Equivalent saved values must not leave a dirty draft when field order changes');
   await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('草稿已保存。',{exact:true}).waitFor();
   await page.screenshot({path:path.join(output,`${version}-${width}-feedback.png`),fullPage:width>=700});
   await page.getByRole('button',{name:'提交审核',exact:true}).click();const before=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,before,'Confirmation must freeze background detail refresh');await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').getByText('待审核',{exact:true}).waitFor();
   role='reviewer';await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().click();await page.getByRole('button',{name:/处理 排查人员甲/}).click();await page.locator('.risk-feedback').waitFor();
   await page.getByRole('button',{name:'审核通过',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').filter({hasText:'审核通过'}).waitFor();
   assert.equal(people[0].items[0].status,'approved');
   await page.screenshot({path:path.join(output,`${version}-${width}-review.png`),fullPage:width>=700});
   await page.goto(url);await page.getByRole('button',{name:'基础模板',exact:true}).click();await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.locator('.risk-field-picker').waitFor();
   assert.equal(await page.getByRole('button',{name:'添加字段',exact:true}).count(),0,'Building managers cannot add shared library fields');assert.equal(await page.locator('.risk-field-tools').count(),0,'Building managers cannot edit or delete shared library fields');
   await page.getByRole('button',{name:'添加字段 排查结果',exact:true}).click();assert.equal(await page.locator('.risk-selected-fields>li').count(),1,'Both versions hide fixed fields while retaining the full schema');
   await page.getByRole('button',{name:'排查列表',exact:true}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();await page.locator('.n-modal-mask').waitFor({state:'hidden'});
   role='admin';
   people[0].items.push({id:'second-item',title:'另一设备检查',status:'approved',values:{f1:'这是同一人员的第二项独立反馈。'},files:[]});
   for(let index=0;index<6;index++)people.push({id:'preview-'+index,name:'示例人员'+(index+1),scope:index%2?'B':'A',version:1,revoked:false,items:[{id:'item',title:index%2?'冷却系统及环境检查':'配电柜状态检查',status:['pending','submitted','returned','approved'][index%4],values:{f1:index%3?'设备运行正常，现场标识完整。':'发现标识松动，已安排现场核对。',f2:index%3?'无异常':'有风险',f4:1792152000000,f5:index%3===0,f6:index%3?0:1},reason:index%4===2?'补充现场照片后重新提交。':'',files:[]}]});
   await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().click();await feedbackRows().nth(8).waitFor();
   assert.match(await feedbackRows().first().innerText(),/退回修改/,'Returned feedback is placed ahead of completed feedback');
   await feedbackItem(feedbackRows().filter({hasText:'另一设备检查'})).click();
   assert.equal(await page.locator('.risk-feedback .risk-readonly').first().innerText(),'这是同一人员的第二项独立反馈。','Opening another item from the same person must not show the first item');
   await page.keyboard.press('Escape');await page.locator('.risk-feedback').waitFor({state:'hidden'});
   await page.locator('.n-drawer-mask').waitFor({state:'hidden'});await page.locator('.risk-page .n-spin-body').waitFor({state:'hidden'});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Wide fields scroll inside the table');
   await page.screenshot({path:path.join(output,`${version}-${width}-table.png`),fullPage:width>=700});
   if(width<700){
    for(let index=0;index<15;index++)people.push({id:'page-'+index,name:'分页人员'+(index+1),scope:'A',version:1,revoked:false,items:[{id:'item',title:'分页检查项',status:'approved',values:{f1:'已核对'},files:[]}]});
    await page.getByRole('button',{name:'刷新',exact:true}).click();await feedbackRows().nth(19).waitFor();assert.equal(await feedbackRows().count(),20,'Mobile summaries retain pagination');
    await page.locator('.risk-feedback-pagination .n-pagination-item').filter({hasText:/^2$/}).click();assert.equal(await feedbackRows().count(),4,'The second page only shows the remaining records');
    await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('分页人员15');assert.equal(await feedbackRows().count(),1,'Searching returns to the first result page');await page.getByRole('textbox',{name:'搜索排查反馈',exact:true}).fill('');assert.equal(await feedbackRows().count(),20);
    if(version==='v2'&&width===360){
     await page.evaluate(()=>localStorage.setItem('clipflow:workspace-night-mode:ou_admin','dark'));await page.goto(url);await page.locator('.risk-mobile-tasks button:visible').first().click();await feedbackRows().first().waitFor();
     assert.equal(await page.evaluate(()=>document.documentElement.dataset.v2Theme),'dark');const colors=await feedbackRows().first().evaluate(node=>({background:getComputedStyle(node).backgroundColor,color:getComputedStyle(node).color}));const luminance=value=>value.match(/[\d.]+/g).slice(0,3).map(Number).map(v=>v/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i],0);assert((Math.max(luminance(colors.color),luminance(colors.background))+.05)/(Math.min(luminance(colors.color),luminance(colors.background))+.05)>=4.5,'Mobile summary text stays readable in night mode');
     await page.screenshot({path:path.join(output,'v2-360-summary-dark.png')});await page.evaluate(()=>localStorage.setItem('clipflow:workspace-night-mode:ou_admin','light'));await page.goto(url);await page.locator('.risk-mobile-tasks button:visible').first().click();await feedbackRows().first().waitFor();
    }
   }
   await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.getByRole('tab',{name:'我的排查任务',exact:true}).click();await page.getByRole('button',{name:'领取并填写',exact:true}).click();
   await page.getByRole('button',{name:'领取本人任务',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback').getByRole('textbox',{name:'排查结果',exact:true}).waitFor();
   assert.equal(await feedbackRows().count(),1,'My tasks only renders the current account, not other assignees');
   assert.match(await page.locator('.risk-feedback-drawer').innerText(),/发布管理员/);
   await page.waitForFunction(()=>{const drawer=document.querySelector('.n-drawer'),rect=drawer?.getBoundingClientRect();return rect&&rect.x>=-1&&rect.right<=innerWidth+1&&!document.querySelector('.n-modal-mask');});
   await page.screenshot({path:path.join(output,`${version}-${width}-own-task.png`),fullPage:width>=700});
   role='alice';await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().waitFor();
   assert.equal(await page.getByRole('button',{name:/^删除任务/}).count(),0,'Ordinary assignees have no task deletion action');
   role='admin';await page.goto(url);await page.locator('.risk-task-list button:visible,.risk-mobile-tasks button:visible').first().waitFor();
   const beforeDeleteReads=detailReads;
   await page.getByRole('button',{name:'删除任务：十月设备专项风险排查',exact:true}).filter({visible:true}).click();
   const deleteDialog=page.locator('.risk-confirm');await deleteDialog.getByText(/多维结果表保留/).waitFor();
   assert.equal(detailReads,beforeDeleteReads,'Delete retains its independent confirmation and does not open task details');
   const deletedWrites=()=>writes.filter(write=>write.path==='/'+rid).length,beforeDelete=deletedWrites();
   await deleteDialog.getByRole('button',{name:'取消',exact:true}).click();assert.equal(deletedWrites(),beforeDelete);
   await page.getByRole('button',{name:'删除任务：十月设备专项风险排查',exact:true}).filter({visible:true}).click();
   await deleteDialog.getByRole('button',{name:'确认删除',exact:true}).click();await deleteDialog.getByText(/正在同步或发送通知/).waitFor();
   assert.equal(deleted,false,'Live cloud work keeps the task intact');
   await deleteDialog.getByRole('button',{name:'确认删除',exact:true}).click();
   await page.getByText('排查任务已删除；已有反馈、历史模板和多维结果表保留。',{exact:true}).waitFor();
   assert.equal(deletedWrites(),beforeDelete+2,'Lost receipt is resolved by reading, without replaying deletion');
   assert.equal(await page.locator('.risk-mobile-task').count(),0);assert.equal(people[0].items[0].status,'approved','Existing feedback is retained');
   await page.screenshot({path:path.join(output,`${version}-${width}-deleted.png`),fullPage:width>=700});
   await ctx.close();
   console.log(`PASS risk inspection ${version} ${width}px`);
  }
 }
 assert.deepEqual(errors,[]);assert.equal(writes.filter(w=>w.path.endsWith('/actions')).length,versions.length*widths.length*4);
 console.log(`PASS risk inspection ${versions.join('/')} at ${widths.join(', ')}px: selected browser workflows and screenshots. No real cloud writes.`);
}catch(e){
 if(page&&!page.isClosed()){
  await page.screenshot({path:path.join(output,'failure.png'),fullPage:false});
  console.error(await page.evaluate(()=>Array.from(document.querySelectorAll('.v2-main,.v2-mobile-nav,.risk-page,.risk-publish,.risk-footer,.risk-footer button')).map(el=>{
   const r=el.getBoundingClientRect(),s=getComputedStyle(el);
   return {class:el.className,text:el.matches('button')?el.textContent:undefined,x:r.x,y:r.y,width:r.width,height:r.height,client:el.clientHeight,scroll:el.scrollHeight,top:el.scrollTop,overflow:s.overflow,position:s.position,paddingBottom:s.paddingBottom,marginBottom:s.marginBottom};
  })));
 }
 throw e;
}
finally{await browser.close();await server.httpServer.close();}
