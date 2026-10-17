import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/risk-table');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:process.env.CLIPFLOW_RISK_BUILD_DIR||'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];
let page;
try{
 for(const version of ['legacy','v2']){
  for(const width of [1440,390,360]){
   const fields=[{key:'f1',field_name:'排查结果',type:1,role:'input',required:true},
    {key:'f2',field_name:'风险等级',type:3,role:'input',property:{options:[{name:'无异常'},{name:'有风险'}]}},
    {key:'f3',field_name:'现场照片',type:17,role:'input'},
    {key:'f4',field_name:'排查日期',type:5,role:'input'},
    {key:'f5',field_name:'发现异常',type:7,role:'input'},
    {key:'f6',field_name:'异常数量',type:2,role:'input'}];
   const rid='a'.repeat(32),people=[{id:'alice',name:'排查人员甲',scope:'A',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'pending',values:{},files:[]}]},
    {id:'bob',name:'排查人员乙',scope:'B',version:1,revoked:false,items:[{id:'item',title:'配电柜状态检查',status:'submitted',values:{f1:'B楼接线检查正常，已核对现场标识。',f2:'无异常',f4:1792152000000,f5:false,f6:0},files:[]}]}];
   let role='admin',reviewerId='reviewer',reviewerVersion=1,detailReads=0,loseSaveResponse=true,rejectHistorySave=true;
   const history={id:'c'.repeat(32),source_release_id:rid,name:'标准配电检查模板',revision:1,version:'history-v1',enabled:true,created:Date.now()/1000,updated:Date.now()/1000,creator:'发布管理员',scopes:['A'],legacy:false,cloud:{synced:false},sync_status:{status:'failed',error:'隔离测试备份失败'},
    fields:[{key:'number',field_id:'number',field_name:'排查任务编号',type:1,role:'number',is_primary:true},...structuredClone(fields)],items:[{title:'复用检查项',values:{f1:'检查接线与标识'}}]};
   const data=()=>({id:rid,name:'十月设备专项风险排查',created:Date.now()/1000,deadline:Date.now()+86400000,status:'active',fields,owner:'发布管理员',reviewer:reviewerId==='reviewer'?'审核主管':'发布管理员',reviewer_id:reviewerId,reviewer_version:reviewerVersion,
    people:structuredClone(role==='alice'?[people[0]]:people),is_manager:role!=='alice',can_manage_release:role!=='alice',is_reviewer:role===reviewerId,self_id:role,jobs:[],audit:[]});
   const ctx=await browser.newContext({viewport:{width,height:width===360?640:900},reducedMotion:'reduce'}),layouts=layoutApiFixture();
   await ctx.route('**/*',async route=>{
    const req=route.request(),u=new URL(req.url());
    if(u.origin!==origin)return route.abort();
    if(u.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
    if(u.pathname==='/assets/vnet-logo.png')return route.fulfill({body:await readFile(path.join(root,'public/assets/vnet-logo.png')),contentType:'image/png'});
    if(!u.pathname.startsWith('/api/'))return route.continue();
    if(await layouts.handle(route,role))return;
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+role,name:role,level:role==='admin'?'L0':role==='reviewer'?'L3':'L4',role:role==='admin'?'admin':'building',home_scope:'A',scope_rules:['A','B'],modules:['risk_inspection']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}],default_scope:'A'});
    if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'risk-fixture'}});
    if(!u.pathname.startsWith('/api/risk-inspections')){assert.equal(req.method(),'GET','No unrelated writes');return ok({items:[],records:[],scopes:{},summary:{},total:0});}
    const p=u.pathname.slice('/api/risk-inspections'.length);
    if(req.method()!=='GET')writes.push({version,width,path:p});
    if(p==='/template')return ok({fields,version:'v1'});
    if(p==='/templates')return ok({items:[{...history,item_count:history.items.length,field_count:history.fields.length}],total:1,page:1});
    if(p==='/templates/'+history.id){if(req.method()==='PUT'){const body=req.postDataJSON();assert.equal(body.version,history.version);if(rejectHistorySave){rejectHistorySave=false;return route.fulfill({status:503,json:{ok:false,error:'模板保存暂未确认，请稍后重试'}});}Object.assign(history,body,{revision:history.revision+1,version:'history-v'+(history.revision+1)});}return ok(history);}
    if(p==='/people')return ok({items:[{id:'alice',name:'排查人员甲',employee_no:'001',scopes:['A'],home_scope:'H',can_review:false},{id:'reviewer',name:'审核主管',employee_no:'002',scopes:['A','B'],home_scope:'A',can_review:true},{id:'admin',name:'发布管理员',employee_no:'003',scopes:['A','B'],home_scope:'A',can_review:true}]});
    if(!p&&req.method()==='GET')return ok({items:[{id:rid,name:'十月设备专项风险排查',deadline:Date.now()+86400000,status:'active',total:2,submitted:0,approved:0,people_count:2,reviewer:'审核主管'}],total:1,page:1,is_manager:role!=='alice',can_edit_template:role==='admin'});
    if(!p&&req.method()==='POST'){const body=req.postDataJSON();assert.match(body.id,/^[a-f0-9]{32}$/);assert.equal(body.template_version,body.history_template_id?history.version:'v1');assert.equal(body.reviewer_id,'reviewer');assert.deepEqual(body.people,[{id:'alice',scope:'A'}]);assert.equal(body.items[0].title,body.history_template_id?'复用检查项':'现场设备状态检查');if(body.history_template_id){assert.equal(body.items[0].values.f1,'检查接线与标识');assert(!body.items[0].values.f3);assert(!body.items[0].values.f4);}return ok(data());}
    if(p.endsWith('/files'))return ok({id:'f'.repeat(32),name:u.searchParams.get('name'),size:req.postDataBuffer().length,field_id:'f3'});
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
   page=await ctx.newPage();page.on('pageerror',e=>errors.push(e.message));await page.clock.install();
   const url=version==='v2'?origin+'/assets/workspace-v2/index.html?__page=/risk-inspections':origin+'/risk-inspections';
   await page.goto(url);await page.locator('.risk-task-list button').first().waitFor();
   await page.getByRole('button',{name:'发布排查',exact:true}).click();await page.locator('.risk-publish').waitFor();
   await page.getByPlaceholder('例如：10月电气设备专项排查').fill('隔离测试发布');
   const due=new Date(Date.now()+86400000*7);const dueText=`${due.getFullYear()}-${String(due.getMonth()+1).padStart(2,'0')}-${String(due.getDate()).padStart(2,'0')} 18:00:00`;
   await page.locator('.risk-publish>.risk-fields .n-date-picker input').fill(dueText);await page.locator('.risk-publish>.risk-fields .n-date-picker input').press('Enter');
   await page.locator('.risk-publish>.risk-fields>label').nth(2).locator('.n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByPlaceholder('事项 1：设备、部件或需要检查的内容').fill('现场设备状态检查');
   if(width===1440){const tops=await page.locator('.risk-publish>.risk-fields>label').evaluateAll(nodes=>nodes.map(el=>el.getBoundingClientRect().top));assert(Math.max(...tops)-Math.min(...tops)<2,'Publishing controls share one desktop row');}
   await page.screenshot({path:path.join(output,`${version}-${width}-publish.png`),fullPage:true});
   await page.getByRole('button',{name:'确认发布',exact:true}).click();
   assert((await page.locator('.risk-confirm').boundingBox()).width<=Math.min(500,width),'Confirmation dialog must remain compact on desktop and fit mobile');
   await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback-table').waitFor();await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.getByRole('button',{name:'历史模板库',exact:true}).click();await page.getByRole('button',{name:'查看 / 编辑',exact:true}).click();
   await page.getByRole('button',{name:'重试备份',exact:true}).click();await page.getByText('已安排重试模板备份。',{exact:true}).waitFor();
   await page.getByRole('textbox',{name:'历史模板名称',exact:true}).fill('标准配电检查模板（修订）');await page.getByRole('button',{name:'保存新版本',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();
   await page.locator('.n-dialog [role=alert]').filter({hasText:'模板保存暂未确认'}).waitFor();
   await page.getByRole('button',{name:'确认保存',exact:true}).click();
   await page.getByText('历史模板新版本已保存，已有排查不受影响。',{exact:true}).waitFor();
   await page.screenshot({path:path.join(output,`${version}-${width}-history.png`),fullPage:true});
   await page.getByRole('button',{name:'复用发布',exact:true}).click();await page.locator('.risk-publish').waitFor();
   assert.equal(await page.locator('.risk-publish>.risk-fields .n-date-picker input').inputValue(),'');
   assert(await page.getByRole('button',{name:'确认发布',exact:true}).isDisabled(),'Recipients, reviewer and deadline must be selected again');
   await page.locator('.risk-publish>.risk-fields .n-date-picker input').fill(dueText);await page.locator('.risk-publish>.risk-fields .n-date-picker input').press('Enter');
   await page.locator('.risk-publish>.risk-fields>label').nth(2).locator('.n-base-selection').click();await page.getByText('审核主管 · A楼、B楼',{exact:true}).click();
   await page.locator('.risk-publish>.risk-fields>label').nth(3).locator('.n-base-selection').click();await page.getByText('排查人员甲 · 001',{exact:true}).click();await page.keyboard.press('Escape');
   await page.getByRole('button',{name:'确认发布',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-feedback-table').waitFor();await page.getByRole('button',{name:'排查列表',exact:true}).click();
   await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table').waitFor();
   const table=page.locator('.risk-feedback-table');
   if(width===1440){const counts=await page.locator('.risk-table-counts').boundingBox(),filters=await page.locator('.risk-table-filters').boundingBox();assert(Math.abs(counts.y+counts.height/2-filters.y-filters.height/2)<2,'Counts and filters share one desktop row');}
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
   await page.screenshot({path:path.join(output,`${version}-${width}-management.png`),fullPage:true});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'No page overflow');
   role='alice';await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table tbody tr').first().waitFor();
   assert.equal(await page.locator('.risk-feedback-table tbody tr').count(),1);
   assert.equal(await page.getByText('排查人员乙',{exact:true}).count(),0,'Private rows must not leak to ordinary users');
   await page.getByRole('button',{name:width<700?'查看 排查人员甲的反馈':/处理 排查人员甲/}).click();await page.locator('.risk-feedback').waitFor();
   await page.getByRole('textbox',{name:'排查结果',exact:true}).focus();const focusedReads=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,focusedReads,'Focused feedback input must retain the existing refresh protection');
   assert.equal(await page.getByRole('button',{name:'撤销该人员'}).count(),0);
   assert.equal(await page.getByRole('button',{name:'更换审核人',exact:true}).count(),0);
   await page.getByRole('button',{name:'提交审核',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-confirm [role=alert]').filter({hasText:'请填写：排查结果'}).waitFor();
   await page.locator('.risk-confirm').getByRole('button',{name:'取消',exact:true}).click();
   await page.getByRole('textbox',{name:'排查结果',exact:true}).fill('绝缘、防护及连接状态已核对，无异常。');
   page.once('dialog',dialog=>dialog.dismiss());
   await page.keyboard.press('Escape');
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
   await page.screenshot({path:path.join(output,`${version}-${width}-feedback.png`),fullPage:true});
   await page.getByRole('button',{name:'提交审核',exact:true}).click();const before=detailReads;await page.clock.fastForward(16000);assert.equal(detailReads,before,'Confirmation must freeze background detail refresh');await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').getByText('待审核',{exact:true}).waitFor();
   role='reviewer';await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.getByRole('button',{name:/处理 排查人员甲/}).click();await page.locator('.risk-feedback').waitFor();
   await page.getByRole('button',{name:'审核通过',exact:true}).click();await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();
   await page.locator('.risk-response .n-tag').filter({hasText:'审核通过'}).waitFor();
   assert.equal(people[0].items[0].status,'approved');
   await page.screenshot({path:path.join(output,`${version}-${width}-review.png`),fullPage:true});
   role='admin';
   people[0].items.push({id:'second-item',title:'另一设备检查',status:'approved',values:{f1:'这是同一人员的第二项独立反馈。'},files:[]});
   for(let index=0;index<6;index++)people.push({id:'preview-'+index,name:'示例人员'+(index+1),scope:index%2?'B':'A',version:1,revoked:false,items:[{id:'item',title:index%2?'冷却系统及环境检查':'配电柜状态检查',status:['pending','submitted','returned','approved'][index%4],values:{f1:index%3?'设备运行正常，现场标识完整。':'发现标识松动，已安排现场核对。',f2:index%3?'无异常':'有风险',f4:1792152000000,f5:index%3===0,f6:index%3?0:1},reason:index%4===2?'补充现场照片后重新提交。':'',files:[]}]});
   await page.goto(url);await page.locator('.risk-task-list button').first().click();await page.locator('.risk-feedback-table tbody tr').nth(8).waitFor();
   await page.getByRole('button',{name:'查看 排查人员甲的反馈 · 另一设备检查',exact:true}).click();
   assert.equal(await page.locator('.risk-feedback .risk-readonly').first().innerText(),'这是同一人员的第二项独立反馈。','Opening another item from the same person must not show the first item');
   await page.keyboard.press('Escape');await page.locator('.risk-feedback').waitFor({state:'hidden'});
   await page.locator('.n-drawer-mask').waitFor({state:'hidden'});await page.locator('.risk-page .n-spin-body').waitFor({state:'hidden'});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2),'Wide fields scroll inside the table');
   await page.screenshot({path:path.join(output,`${version}-${width}-table.png`),fullPage:true});
   await ctx.close();
  }
 }
 assert.deepEqual(errors,[]);assert.equal(writes.filter(w=>w.path.endsWith('/actions')).length,30);
 console.log('PASS risk inspection: legacy/v2, PC/mobile, history edit/reuse, recipient reset, reviewer reassignment, visible errors, frozen confirmation, lost-response recovery, attachment, draft, submit, review. No real cloud writes.');
}catch(e){if(page&&!page.isClosed())await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});throw e;}
finally{await browser.close();await server.httpServer.close();}
