import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';
const root=fileURLToPath(new URL('..',import.meta.url)),output=path.resolve(root,'../../../output/playwright/account-workflow');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'}),errors=[],writes=[],reads=[],layouts=layoutApiFixture();
const user={open_id:'accounts-fixture',name:'隔离验收主管',role:'building',level:'L3',category:'supervisor',modules:['accounts','maintenance','learning'],can_manage_accounts:true};
const scopes=[{value:'A',label:'A楼'},{value:'B',label:'B楼'}],modules=[{value:'maintenance',label:'维保通告',group:'通告办理'},{value:'learning',label:'画像学练',group:'业务管理'}];
const categories=[{value:'operator',label:'值班员'},{value:'engineer',label:'工程师'}],levels=[{value:'L4',label:'L4 本楼基础'}];
const account={id:'rec-fixture',record_id:'rec-fixture',open_id:'ou_fixtureA',name:'隔离验收人员',employee_no:'TEST',kind:'external',position:'值班员',category:'operator',home_scope:'A',own_scopes:['A'],scopes:['A'],modules:['learning'],level:'L4',role:'building',enabled:true,can_edit:true,can_edit_access:true,can_set_password:true,can_delete:true,revision:'rev1'};
const application=id=>({request_id:id,name:id==='rich'?'完整权限申请人':'楼栋申请人',open_id:'ou_fixtureA',status:'pending',created_at:'2026-10-14 10:00',reason:'隔离验收申请',requested_scopes:['A'],requested_scope_labels:['A楼'],...(id==='rich'?{requested_access:{level:'L4',category:'operator',scopes:['A'],modules:['learning']}}:{})});
let requests=[application('rich'),application('scope')],saveFailure=0,reviewFailure=0,delayReview,selfRequest=null;
await context.route('**/*',async route=>{
 const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
 if(await layouts.handle(route,user.open_id))return;
 const ok=data=>route.fulfill({json:{ok:true,data}}),fail=(status,message)=>route.fulfill({status,json:{ok:false,error:message}});
 if(req.method()==='GET')reads.push(url.pathname);else writes.push({path:url.pathname,body:req.postDataJSON()});
 if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
 if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'account-workflow-test'}});
 if(url.pathname==='/api/auth/accounts')return ok({items:[account],scope_options:scopes,module_options:modules,category_options:categories,level_options:levels,policy:{level:user.level,scope_rules:['A','B'],modules:user.modules}});
 if(url.pathname==='/api/auth/accounts/permissions'){
  if(saveFailure)return fail(saveFailure,'隔离验收保存失败');Object.assign(account,req.postDataJSON().changes[0]);account.revision='rev2';return ok({changed:1});
 }
 if(url.pathname==='/api/auth/permission-requests/admin')return ok({items:requests.filter(row=>url.searchParams.get('status')==='all'||row.status===url.searchParams.get('status'))});
 if(url.pathname.endsWith('/approve')||url.pathname.endsWith('/reject')){
  if(delayReview)await delayReview;
  if(reviewFailure)return fail(reviewFailure,'隔离审批结果待核验');
  const id=url.pathname.split('/').at(-2),row=requests.find(row=>row.request_id===id);row.status=url.pathname.endsWith('/approve')?'approved':'rejected';return ok({request:row,notification:{ok:true,queued:true}});
 }
 if(url.pathname.endsWith('/bulk-approve')){requests[0].status='approved';return ok({items:[requests[0]],failed:[{request_id:'scope',error:'隔离测试权限已变化'}]});}
 if(url.pathname==='/api/auth/permission-approvers')return ok({items:[{open_id:'ou_fixtureBoss',name:'隔离上级',level:'L1',scopes:['A','B'],modules:user.modules,level_options:levels,category_options:categories}],current:{level:selfRequest?.status==='approved'?'L3':'L4',category:'operator',scope_rules:['A'],modules:['learning']},scope_options:scopes,module_options:modules});
 if(url.pathname==='/api/auth/permission-requests/current')return ok({request:selfRequest});
 if(url.pathname==='/api/auth/permission-requests'&&req.method()==='POST'){selfRequest={...req.postDataJSON(),request_id:'self-fixture',status:'pending',approver_name:'隔离上级'};return fail(503,'隔离测试响应丢失');}
 if(req.method()!=='GET'){errors.push('Unexpected write: '+url.pathname);return route.abort();}
 return ok({items:[],users:[],records:[],scope_options:scopes,settings:{}});
});
const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html';
try{
 await page.goto(entry+'?__page=/admin/settings');await page.getByRole('button',{name:'管理隔离验收人员',exact:true}).click();
 const editor=page.getByRole('dialog',{name:'隔离验收人员'});
 await editor.getByRole('combobox',{name:'岗位分类',exact:true}).selectOption('engineer');
 await editor.getByRole('button',{name:'关闭账号详情'}).click();await page.getByRole('button',{name:'继续编辑',exact:true}).click();
 assert.equal(await editor.getByRole('combobox',{name:'岗位分类',exact:true}).inputValue(),'engineer');
 assert(await editor.getByRole('button',{name:'保存启用状态',exact:true}).isDisabled(),'state-only save must not discard access edits');
 saveFailure=400;await editor.getByRole('button',{name:'保存权限',exact:true}).click();await editor.getByRole('alert').filter({hasText:'隔离验收保存失败'}).waitFor();
 assert.equal(await editor.getByRole('combobox',{name:'岗位分类',exact:true}).inputValue(),'engineer','failed saves preserve edits');
 saveFailure=409;await editor.getByRole('button',{name:'保存权限',exact:true}).click();await editor.getByRole('button',{name:'重新读取账号'}).waitFor();
 assert(await editor.getByRole('button',{name:'保存权限',exact:true}).isDisabled());
 await editor.getByRole('button',{name:'重新读取账号'}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-editor select[aria-label="岗位分类"]')?.value==='operator');
 assert.equal(await editor.getByRole('combobox',{name:'岗位分类',exact:true}).inputValue(),'operator');
 saveFailure=0;await editor.getByRole('combobox',{name:'岗位分类',exact:true}).selectOption('engineer');
 await editor.getByRole('button',{name:'保存权限',exact:true}).click();await editor.waitFor({state:'hidden'});
 assert.equal(account.category,'engineer');
 await page.getByRole('button',{name:'权限申请',exact:true}).click();await page.getByText('完整权限申请人',{exact:true}).waitFor();
 assert(!reads.includes('/api/auth/permissions'),'V2 must not fetch an unrestricted legacy directory');
 await page.getByRole('row').filter({hasText:'完整权限申请人'}).getByRole('button',{name:'核对并审批'}).click();
 let modal=page.locator('.v2-review-modal');await modal.waitFor();
 assert((await modal.innerText()).includes('完整模块权限'));assert((await modal.innerText()).includes('画像学练'));
 assert.equal(await modal.getByRole('checkbox').count(),0,'rich grants are reviewed as a complete immutable request');
 const before=writes.length;await modal.getByRole('button',{name:'取消',exact:true}).click();assert.equal(writes.length,before,'preview does not approve');
 await page.getByRole('row').filter({hasText:'完整权限申请人'}).getByRole('button',{name:'核对并审批'}).click();
 let release;delayReview=new Promise(resolve=>release=resolve);
 await modal.getByRole('button',{name:'确认批准'}).click();await page.waitForTimeout(100);
 assert(await modal.getByRole('button',{name:'取消',exact:true}).isDisabled());
 assert.equal(writes.filter(row=>row.path.endsWith('/rich/approve')).length,1);
 release();delayReview=null;await modal.waitFor({state:'hidden'});await page.getByText('申请已通过，权限已生效',{exact:true}).waitFor();
 assert.equal(requests[0].status,'approved');
 await page.getByRole('row').filter({hasText:'楼栋申请人'}).getByRole('button',{name:'拒绝',exact:true}).click();
 await modal.getByRole('button',{name:'确认拒绝'}).click();await modal.getByRole('alert').filter({hasText:'请填写拒绝原因'}).waitFor();
 assert.equal(writes.filter(row=>row.path.endsWith('/scope/reject')).length,0);
 await modal.getByRole('textbox',{name:'拒绝原因'}).fill('隔离验收');reviewFailure=503;
 await modal.getByRole('button',{name:'确认拒绝'}).click();await modal.getByText(/提交结果尚未确认/).waitFor();
 assert(await modal.getByRole('button',{name:'确认拒绝'}).isDisabled());
 await modal.getByRole('button',{name:'刷新申请结果',exact:true}).click();reviewFailure=0;
 await modal.waitFor({state:'hidden'});
 requests=[application('rich'),application('scope')];await page.getByRole('button',{name:'刷新结果',exact:true}).click();
 await page.getByRole('checkbox',{name:'选择完整权限申请人的申请'}).check();await page.getByRole('checkbox',{name:'选择楼栋申请人的申请'}).check();
 await page.getByRole('button',{name:'审批所选',exact:true}).click();assert.equal(await modal.locator('.v2-review-detail').count(),2);
 await modal.getByRole('button',{name:'确认批准'}).click();await modal.waitFor({state:'hidden'});
 await page.getByText(/已处理 1 条，未完成 1 条/).waitFor();
 assert(await page.getByRole('checkbox',{name:'选择楼栋申请人的申请'}).isChecked(),'partial failure keeps the failed request selected');
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:width<700?844:900});
  if(width<700){const box=await page.locator('.v2-permission-review tbody tr').first().boundingBox();assert(box.x>=0&&box.x+box.width<=width,'mobile request details stay entirely on screen');}
  await page.getByRole('row').filter({hasText:'楼栋申请人'}).getByRole('button',{name:'核对并审批'}).click();
  await modal.waitFor();await page.waitForTimeout(250);
  await page.screenshot({path:path.join(output,'review-'+width+'.png'),animations:'disabled'});
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await modal.getByRole('button',{name:'取消',exact:true}).click();
 }
 await page.setViewportSize({width:1440,height:900});await page.goto(entry+'?view=permission');
 await page.getByLabel('上级审批人',{exact:true}).click();await page.locator('.n-base-select-option').filter({hasText:'隔离上级'}).click();
 await page.getByRole('textbox',{name:'申请原因'}).fill('未提交的申请');await page.getByRole('button',{name:'返回',exact:true}).click();
 await page.getByRole('button',{name:'继续填写',exact:true}).click();
 assert.equal(await page.getByRole('textbox',{name:'申请原因'}).inputValue(),'未提交的申请');
 await page.getByRole('button',{name:'提交并发送飞书审批',exact:true}).click();await page.getByText(/提交结果尚未确认/).waitFor();
 assert(await page.getByRole('button',{name:'提交并发送飞书审批',exact:true}).isDisabled());
 await page.getByRole('button',{name:'刷新申请结果',exact:true}).click();await page.getByText(/等待上级审批/).waitFor();
 assert(await page.getByRole('button',{name:'提交并发送飞书审批',exact:true}).isDisabled(),'recovered pending request must not be sent twice');
 assert.equal(writes.filter(row=>row.path==='/api/auth/permission-requests').length,1);
 selfRequest.status='approved';await page.locator('.v2-access-current .n-tag').filter({hasText:'L3'}).waitFor();
 Object.assign(user,{level:'L1',role:'admin',category:'engineer',modules:['accounts','maintenance','learning','signatures']});
 Object.assign(account,{name:'同级只读人员',level:'L1',role:'admin',category:'manager',can_edit:false,can_edit_access:false,
  can_set_password:false,can_delete:false,can_impersonate:false,can_access_signature:false});
 for(const version of ['legacy','v2']) for(const viewport of [{width:402,height:874},{width:440,height:956},{width:1440,height:900}]){
  await page.setViewportSize(viewport);await page.goto(version==='v2'?entry+'?__page=/admin/settings':origin+'/admin/settings');
  const beforeReads=reads.length,beforeWrites=writes.length;
  await page.getByRole('button',{name:'管理同级只读人员',exact:true}).click();
  const readonly=page.getByRole('dialog',{name:'同级只读人员'});await readonly.waitFor();
  await readonly.evaluate(el=>Promise.all(el.getAnimations({subtree:true}).map(animation=>animation.finished.catch(()=>{}))));
  assert(await readonly.getByRole('button',{name:'保存启用状态',exact:true}).isDisabled());
  assert.equal(await readonly.getByRole('button',{name:'保存权限',exact:true}).count(),0);
  assert.equal(await readonly.locator('.account-password,.account-signature-editor').count(),0);
  assert.match(await readonly.locator('dl').innerText(),/姓名密码登录/,'login method is not determined by password-edit permission');
  assert.doesNotMatch(await readonly.innerText(),/未关联可使用密码登录/,'readonly access is not a missing login identity');
  assert(!reads.slice(beforeReads).some(path=>path.startsWith('/api/signatures/')),'personnel view must not read peer signatures');
  assert.equal(writes.length,beforeWrites,'viewing a peer must not write anything');
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
  await page.screenshot({path:path.join(output,'peer-readonly-'+version+'-'+viewport.width+'.png')});
  await readonly.getByRole('button',{name:'关闭账号详情'}).click();
 }
 assert.deepEqual(errors,[]);
 console.log('PASS: scoped supervisor review, complete grants preview, cancel/confirm, busy/unknown result guards, editor dirty protection and failure recovery, no legacy directory fetch, desktop/mobile. All writes mocked.');
}catch(error){await page.screenshot({path:path.join(output,'failure.png')});console.error(errors);throw error;}
finally{await context.close();await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
