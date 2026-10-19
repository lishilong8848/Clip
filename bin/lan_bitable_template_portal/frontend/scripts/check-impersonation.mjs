import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

// Default: real production entries. --source validates the same roots without a dist build.
const root=fileURLToPath(new URL('..',import.meta.url)),source=process.argv.includes('--source');
const output=path.resolve(root,'../../../output/playwright',process.env.CLIPFLOW_IMPERSONATION_OUTPUT||'impersonation');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true}),reports=[];
const scopes=[{value:'A',label:'A楼'}],targetId='person/002 +中';
const admin={open_id:'ou_imp_admin',name:'测试管理员',role:'admin',level:'L0',category:'manager',can_manage_accounts:true,modules:['accounts','learning'],home_scope:'A',scopes:['A']};
const target={open_id:'ou_imp_target',name:'测试值班员',role:'building',level:'L4',category:'operator',modules:['learning','assistant'],home_scope:'A',scopes:['A']};
const account={id:targetId,record_id:targetId,open_id:target.open_id,name:target.name,employee_no:'TEST-2',kind:'external',position:'值班员',category:'operator',home_scope:'A',own_scopes:['A'],scopes:['A'],modules:['learning'],level:'L4',role:'building',enabled:true,can_edit:true,can_edit_access:true,can_impersonate:true,can_set_password:false,can_delete:false,revision:'fixture-1'};
const grant=(extra={})=>({id:'test-grant-hash-not-a-credential',operator_name:admin.name,target_name:target.name,expires_at:Math.floor(Date.now()/1000)+1200,active:true,...extra});
function sourceEntry(version){return {name:'impersonation-offline-root',configureServer(server){server.middlewares.use(async(req,res,next)=>{
  const pathname=req.url?.split('?')[0];
  if(!['/','/admin/settings','/assets/workspace-v2/index.html','/__client.html'].includes(pathname))return next();
  const entry=pathname==='/__client.html'?null:version==='v2'?'/src/workspace-v2/main.ts':'/src/main.ts';
  const html=`<!doctype html><html lang="zh-CN"><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>代登录离线验收</title></head><body><div id="app"></div>${entry?`<script type="module" src="${entry}"></script>`:''}</body></html>`;
  res.setHeader('Content-Type','text/html');res.end(await server.transformIndexHtml(req.url,html));
});}};}
async function start(version,clientOnly=false){
  const dev=source||clientOnly;
  const server=dev?await createServer({root,configFile:version==='v2'?path.join(root,'vite.workspace-v2.config.ts'):false,
    plugins:[sourceEntry(version),...version==='legacy'?[vue()]:[]],publicDir:path.join(root,'public'),cacheDir:path.join(output,'vite-cache-'+version),
    server:{host:'127.0.0.1',port:0,hmr:false,proxy:{}},optimizeDeps:{noDiscovery:true,include:['vue','naive-ui','lucide-vue-next']},logLevel:'error'}):
    await preview({root,configFile:false,logLevel:'error',build:{outDir:process.env.CLIPFLOW_IMPERSONATION_BUILD_DIR||'dist'},preview:{host:'127.0.0.1',port:0}});
  if(dev)await server.listen();
  return {server,origin:'http://127.0.0.1:'+server.httpServer.address().port,close:()=>dev?server.close():new Promise(resolve=>server.httpServer.close(resolve))};
}
async function checkTransport(){
  const host=await start('legacy',true),context=await browser.newContext({serviceWorkers:'block'}),requests=[];
  const grantId=grant().id;
  try{
    await context.route('**/*',async route=>{
      const req=route.request(),url=new URL(req.url());if(url.origin!==host.origin)return route.abort();
      if(!url.pathname.startsWith('/api/'))return route.continue();
      const headers=await req.allHeaders(),header=headers['x-clipflow-impersonation']||'';
      const cookieGrant=/(?:^|;\s*)fixture_impersonation=replacement(?:;|$)/.test(headers.cookie||'')?'replacement-grant':/(?:^|;\s*)fixture_impersonation=active(?:;|$)/.test(headers.cookie||'')?grantId:'';
      requests.push({path:url.pathname,method:req.method(),header,contentType:req.headers()['content-type']||''});
      if(url.pathname==='/api/auth/impersonation/stop'&&header&&header!==cookieGrant)return route.fulfill({status:409,json:{ok:false,error:'此标签页的代登录已变化，不能退出另一会话。'}});
      if(req.method()!=='GET'&&url.pathname!=='/api/auth/impersonation/stop'&&header!==cookieGrant)return route.fulfill({status:409,json:{ok:false,error:'身份已变化，请刷新后重新操作。'}});
      if(url.pathname==='/api/fixture-personal')return route.fulfill({status:403,json:{ok:false,error:'此操作必须由本人完成，不能代答题。'}});
      if(url.pathname==='/api/fixture-file')return route.fulfill({body:'fixture-file',contentType:'application/octet-stream'});
      return route.fulfill({json:{ok:true,data:{accepted:true,revision:0,value:null}}});
    });
    const old=await context.newPage(),current=await context.newPage();
    for(const page of [old,current]){await page.goto(host.origin+'/__client.html');await page.evaluate(async()=>{window.api=await import('/src/api/client.ts');});}
    await context.addCookies([{name:'fixture_impersonation',value:'active',url:host.origin,httpOnly:true,sameSite:'Lax'}]);
    await current.evaluate(id=>window.api.setImpersonationContext(id),grantId);
    const invoke=page=>page.evaluate(async()=>{try{await window.api.requestJson('/api/fixture-write',{method:'POST',body:'{}'});return 'accepted';}catch(e){return e.status;}});
    assert.equal(await invoke(old),409,'Old administrator tab must not submit with the shared target cookie');
    assert.equal(await invoke(current),'accepted');
    await current.evaluate(async()=>{
      const body=new FormData();body.append('file',new File(['fixture'],'photo.txt'));
      await window.api.uploadJson('/api/fixture-upload',body);
      await window.api.requestBinaryJson('/api/fixture-binary',new Uint8Array([1,2]));
      await window.api.requestFile('/api/fixture-file',{method:'POST',body:'{}'});
      await window.api.requestJson('/api/workspace-layouts/desktop/workspace');
      try{await window.api.requestJson('/api/workspace-layouts/desktop/workspace',{method:'PUT',body:'{}'});throw new Error('layout write was accepted');}catch(e){if(e.status!==403)throw e;}
      try{await window.api.requestJson('/api/fixture-personal',{method:'POST',body:'{}'});throw new Error('personal action was accepted');}catch(e){if(e.status!==403||!e.message.includes('本人'))throw e;}
    });
    for(const row of requests.filter(row=>['/api/fixture-upload','/api/fixture-binary','/api/fixture-file'].includes(row.path)))assert.equal(row.header,grantId);
    assert(requests.find(row=>row.path==='/api/fixture-upload').contentType.startsWith('multipart/form-data; boundary='),'Browser controls the upload boundary');
    assert(!requests.find(row=>row.path==='/api/fixture-binary').contentType.includes('json'),'Raw binary content type is preserved');
    assert(!requests.some(row=>row.method==='PUT'),'No target layout auto-save reaches the server');
    await context.addCookies([{name:'fixture_impersonation',value:'replacement',url:host.origin,httpOnly:true,sameSite:'Lax'}]);
    assert.equal(await current.evaluate(async()=>{try{await window.api.requestJson('/api/auth/impersonation/stop',{method:'POST',body:'{}'});return 'accepted';}catch(e){return e.status;}}),409,'A stale grant cannot stop the replacement cookie session');
    assert.equal((await context.cookies()).find(cookie=>cookie.name==='fixture_impersonation').value,'replacement');
    await context.clearCookies();assert.equal(await invoke(current),409,'Old target tab must not submit as the restored administrator');
    await current.evaluate(async()=>{window.api.setImpersonationContext('');await window.api.requestJson('/api/fixture-write',{method:'POST',body:'{}',headers:{'X-Clipflow-Impersonation':'caller-must-not-forge-context'}});});
    assert.equal(requests.at(-1).header,'');
    reports.push({case:'transport',passed:true,requests});
    console.log('PASS transport: current grant, stale tabs, upload/binary, read-only layouts');
  }finally{await context.close();await host.close();}
}
async function inspectEditorFooter(editor,viewport){
  const evidence=await editor.locator('.editor-footer').evaluate(el=>{
    const box=node=>{const r=node.getBoundingClientRect();return {left:r.left,right:r.right,top:r.top,bottom:r.bottom,width:r.width,height:r.height};};
    return {footer:box(el),dialog:box(el.closest('.account-editor')),scrollWidth:el.scrollWidth,clientWidth:el.clientWidth,
      buttons:[...el.querySelectorAll('button')].map(button=>({...box(button),text:button.textContent.trim(),scrollWidth:button.scrollWidth,clientWidth:button.clientWidth}))};
  });
  const {footer,dialog,buttons}=evidence;
  assert(footer.left>=Math.max(0,dialog.left)-1&&footer.right<=Math.min(viewport.width,dialog.right)+1,'Editor footer stays within the dialog and viewport horizontally');
  assert(evidence.scrollWidth<=evidence.clientWidth+1,'Editor footer has no internal horizontal overflow');
  assert(footer.top>=0&&footer.bottom<=Math.min(viewport.height,dialog.bottom)+1,'Editor footer stays vertically reachable');
  for(const button of buttons){
    assert(button.left>=footer.left-1&&button.right<=footer.right+1,'Every footer button fits horizontally: '+button.text);
    assert(button.scrollWidth<=button.clientWidth+1,'Footer button text must not overflow: '+button.text);
    if(viewport.width<=600)assert(button.height>=44,'Mobile footer button preserves a 44px target: '+button.text);
  }
  return evidence;
}
async function verifyEditorError(page,editor,message,version){
  const error=editor.getByRole('alert').filter({hasText:message});await error.waitFor();
  assert(await error.evaluate(el=>!!el.closest('.account-editor')),'The rejected impersonation error remains inside the editor');
  const inlineBoundary=await error.evaluate(el=>!!el.closest('[data-v2-inline-feedback]'));
  if(version==='v2'&&source&&!inlineBoundary)return {checked:false,reason:'Source editor has no V2 adapter boundary; toast verification requires the built entry.'};
  if(version==='v2')assert(inlineBoundary,'The built teleported editor retains its inline-feedback boundary');
  // Allow the real feedback observer to run before checking that no toast was promoted.
  await page.waitForTimeout(150);
  assert.equal(await page.locator('[data-v2-feedback]:visible,[popover]:visible').filter({hasText:message}).count(),0,'Editor error must not become a top-layer feedback popover');
  assert.equal(await page.locator('.n-message:visible,.n-notification:visible').count(),0,'Editor rejection must not create an NMessage or notification');
  return {checked:true,inlineBoundary};
}
async function checkPage(version,viewport){
  console.log('Checking '+version+' '+viewport.width+' ('+(source?'source':'dist')+')');
  const host=await start(version),context=await browser.newContext({viewport,reducedMotion:'reduce',serviceWorkers:'block'}),errors=[],animationWarnings=[],writes=[],editorFooters=[],editorFeedback=[],layout=layoutApiFixture();
  const page=await context.newPage();page.setDefaultTimeout(20000);page.on('pageerror',e=>{
    // Existing legacy cross-document transitions reject when screenshots skip animation.
    // Keep this narrowly classified evidence; all other runtime errors remain failures.
    if(e.message==='Transition was skipped')animationWarnings.push(e.message);else errors.push(e.message);
  });
  await context.addInitScript(()=>{window.__documentMarker=crypto.randomUUID();});
  let info=null,serverGrantId='',enterFail=true,stopFail=false,holdEnter=null,releaseEnter,holdStop=null,releaseStop;
  const entry=version==='v2'?'/assets/workspace-v2/index.html?__page=/admin/settings&tab=permissions':'/admin/settings?tab=permissions&frontend=legacy';
  const home=version==='v2'?'/assets/workspace-v2/index.html':'/?frontend=legacy';
  const tag=version+'-'+viewport.width;
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());if(url.origin!==host.origin)return route.abort('blockedbyclient');
    if(url.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}}),fail=(status,error)=>route.fulfill({status,json:{ok:false,error}});
    if(req.method()!=='GET')writes.push({path:url.pathname,body:req.postDataJSON(),header:req.headers()['x-clipflow-impersonation']||''});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:info?{...target,impersonation:info}:admin,scope_options:scopes,default_scope:'A'});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'impersonation-fixture'}});
    if(url.pathname==='/api/auth/accounts')return ok({items:[account,{...account,id:'protected',name:'不可代登录人员',can_impersonate:false}],scope_options:scopes,module_options:[{value:'learning',label:'画像学练'}],category_options:[{value:'operator',label:'值班员'}],level_options:[{value:'L4',label:'L4'}],policy:{level:'L0',scope_rules:['A'],modules:admin.modules}});
    if(url.pathname.endsWith('/impersonate')){
      assert.equal(url.pathname,'/api/auth/accounts/'+encodeURIComponent(targetId)+'/impersonate');assert.deepEqual(req.postDataJSON(),{});
      if(enterFail)return fail(403,'隔离验证：目前不能代登录该人员');
      if(holdEnter)await holdEnter;info=grant();serverGrantId=info.id;return ok({redirect_url:'/'});
    }
    if(url.pathname==='/api/auth/impersonation/stop'){
      assert.deepEqual(req.postDataJSON(),{});if(holdStop)await holdStop;
      if(serverGrantId&&req.headers()['x-clipflow-impersonation']!==serverGrantId)return fail(409,'隔离验证：旧标签页不能退出新的代登录');
      if(stopFail)return fail(503,'隔离验证：退出结果尚未确认');info=null;serverGrantId='';return ok({redirect_url:'/?admin=accounts'});
    }
    if(url.pathname.startsWith('/api/workspace-layouts/')){assert.equal(req.method(),'GET','Impersonation cannot write target layouts');return layout.handle(route,info?target.open_id:admin.open_id);}
    assert.equal(req.method(),'GET','No unrelated writes');
    if(info&&url.pathname.startsWith('/api/assistant/'))errors.push('Private assistant fetched during impersonation: '+url.pathname);
    return ok({items:[],users:[],records:[],scope_options:scopes,settings:{},summary:{},scopes:{},links:{},total:0});
  });
  const banner=()=>page.getByRole('region',{name:'管理员临时代登录'});
  const markers=()=>page.evaluate(()=>window.__documentMarker);
  const begin=async()=>{await page.getByRole('button',{name:'临时代登录测试值班员',exact:true}).first().click();return page.getByRole('dialog',{name:'临时以测试值班员身份查看？'});};
  try{
    await page.goto(host.origin+entry);await page.getByRole('button',{name:'临时代登录测试值班员',exact:true}).first().waitFor({timeout:120000});
    assert(await page.getByRole('button',{name:'临时代登录不可代登录人员',exact:true}).isDisabled());assert.equal(await banner().count(),0);
    let dialog=await begin();assert((await dialog.innerText()).includes('20分钟'));assert((await dialog.innerText()).includes('不能代签、代审批、代答题'));
    await page.screenshot({path:path.join(output,tag+'-confirm.png'),animations:'disabled'});
    await dialog.getByRole('button',{name:'取消',exact:true}).click();assert.equal(writes.length,0,'Cancel never sends a request');
    await page.getByRole('button',{name:'管理测试值班员',exact:true}).click();
    const editor=page.getByRole('dialog',{name:'测试值班员',exact:true});
    await editor.getByRole('checkbox',{name:'启用登录',exact:true}).uncheck();
    editorFooters.push(await inspectEditorFooter(editor,viewport));
    await editor.getByRole('button',{name:'临时代登录',exact:true}).click();
    dialog=page.getByRole('dialog',{name:'临时以测试值班员身份查看？'});
    const original=await markers();await dialog.getByRole('button',{name:'确认临时代登录',exact:true}).click();
    editorFeedback.push(await verifyEditorError(page,editor,'隔离验证：目前不能代登录该人员',version));assert.equal(await markers(),original,'Rejected entry never navigates');
    editorFooters.push(await inspectEditorFooter(editor,viewport));
    await page.screenshot({path:path.join(output,tag+'-editor-error.png'),animations:'disabled'});
    enterFail=false;holdEnter=new Promise(resolve=>releaseEnter=resolve);
    await editor.getByRole('button',{name:'临时代登录',exact:true}).click();
    dialog=page.getByRole('dialog',{name:'临时以测试值班员身份查看？'});
    await dialog.getByRole('button',{name:'确认临时代登录',exact:true}).click();await page.waitForTimeout(150);
    assert.equal(await markers(),original);assert(await page.getByRole('button',{name:'临时代登录测试值班员',exact:true}).first().isDisabled());
    await page.evaluate(()=>{window.__administratorDraft={private:'must be discarded'};});releaseEnter();holdEnter=null;
    await banner().waitFor();assert.notEqual(await markers(),original,'Successful entry is a hard navigation');
    assert.equal(await page.evaluate(()=>window.__administratorDraft),undefined);
    assert.equal(new URL(page.url()).pathname,version==='v2'?'/assets/workspace-v2/index.html':'/');
    await banner().getByText('正在以测试值班员身份查看 · 实际操作人测试管理员',{exact:true}).waitFor();
    assert.match(await banner().getByLabel('代登录剩余时间').innerText(),/^\d\d:\d\d$/);
    assert.equal(await page.locator('.assistant-widget,.lighthouse-assistant').count(),0);
    await page.screenshot({path:path.join(output,tag+'-active.png'),animations:'disabled'});
    const geometry=await banner().evaluate(el=>{const r=el.getBoundingClientRect(),b=el.querySelector('button').getBoundingClientRect(),main=document.querySelector('.v2-main,.page-content')?.getBoundingClientRect();return {left:r.left,right:r.right,bottom:r.bottom,mainTop:main?.top,buttonHeight:b.height,width:innerWidth,overflow:document.documentElement.scrollWidth>innerWidth+1};});
    assert(geometry.left>=0&&geometry.right<=geometry.width+1&&!geometry.overflow,'Banner fits the viewport');assert(geometry.buttonHeight>=44);
    assert(geometry.bottom<=geometry.mainTop+1,'Banner does not cover business controls');
    assert(!await page.evaluate(id=>[localStorage,sessionStorage].some(store=>Object.values(store).some(value=>value.includes(id))),info.id),'Grant ID stays only in document memory');
    stopFail=true;const activeDocument=await markers();await banner().getByRole('button',{name:'退出代登录',exact:true}).click();
    await banner().getByRole('alert').waitFor();assert.equal(await markers(),activeDocument);assert.equal(writes.at(-1).header,info.id,'Actual stop POST includes the current grant header');
    stopFail=false;holdStop=new Promise(resolve=>releaseStop=resolve);await banner().getByRole('button',{name:'重试退出代登录',exact:true}).click();
    await page.waitForTimeout(100);assert(await banner().getByRole('button',{name:'正在退出…',exact:true}).isDisabled());assert.equal(await markers(),activeDocument);
    releaseStop();holdStop=null;await page.getByRole('button',{name:'临时代登录测试值班员',exact:true}).first().waitFor();
    assert.notEqual(await markers(),activeDocument);assert.equal(await banner().count(),0);
    const exitUrl=new URL(page.url());assert.equal(version==='v2'?exitUrl.searchParams.get('__page'):exitUrl.pathname,'/admin/settings');assert.equal(exitUrl.searchParams.get('tab'),'permissions');
    info=grant({expires_at:Math.floor(Date.now()/1000)+4});const expiredId=info.id;serverGrantId=info.id;stopFail=false;const beforeExpiry=writes.length;
    await page.goto(host.origin+home);await banner().waitFor();serverGrantId='replacement-grant';
    await banner().getByRole('alert').filter({hasText:'旧标签页不能退出新的代登录'}).waitFor();await banner().getByText('代登录已失效，仅可退出',{exact:true}).waitFor();
    assert.equal(await page.locator('.v2-main,.page-content').count(),0,'Expired sessions remove business controls');
    assert.equal(writes.at(-1).header,expiredId,'Automatic expiry stop retains the OLD grant header');assert.equal(serverGrantId,'replacement-grant','Failed old expiry must not exit the newer grant');
    assert.equal(writes.length,beforeExpiry+1);await page.waitForTimeout(2200);assert.equal(writes.length,beforeExpiry+1,'Expiry attempts stop once, not continuously');
    await page.screenshot({path:path.join(output,tag+'-expired.png'),animations:'disabled'});
    info=grant({active:false,reason:'管理员已撤销'});serverGrantId=info.id;await page.reload();await banner().getByText(/管理员已撤销/).waitFor();assert.equal(writes.length,beforeExpiry+1,'Inactive grants do not auto-submit');
    stopFail=false;await banner().getByRole('button',{name:'退出代登录',exact:true}).click();await page.getByRole('button',{name:'临时代登录测试值班员',exact:true}).first().waitFor();
    assert.equal(context.pages().length,1,'No new window is opened');assert.deepEqual(errors,[]);
    reports.push({case:tag,passed:true,geometry,editorFooters,editorFeedback,writes,animationWarnings});
    console.log('PASS '+tag);
  }catch(error){await page.screenshot({path:path.join(output,tag+'-failure.png')});await writeFile(path.join(output,tag+'-failure.json'),JSON.stringify({error:error.message,errors,editorFooters,editorFeedback,writes},null,2));console.error(tag,errors);throw error;}
  finally{releaseEnter?.();releaseStop?.();await context.close();await host.close();}
}
try{
  await checkTransport();
  for(const version of ['legacy','v2'])for(const viewport of [{width:1440,height:900},{width:360,height:640}])if(!process.env.CLIPFLOW_IMPERSONATION_CASE||(version+'-'+viewport.width)===process.env.CLIPFLOW_IMPERSONATION_CASE)await checkPage(version,viewport);
  const suffix=process.env.CLIPFLOW_IMPERSONATION_CASE?'-'+process.env.CLIPFLOW_IMPERSONATION_CASE:'';
  await writeFile(path.join(output,(source?'source-results':'dist-results')+suffix+'.json'),JSON.stringify(reports,null,2));
  console.log(`PASS (${source?'source roots':'dist entries'}; ${reports.map(r=>r.case).join(', ')}): headers, stale tabs, enter/exit, cancellation, errors, pending, hard navigation, disabled accounts and expiry retry. All APIs isolated; no backend or cloud calls. Screenshots: ${output}`);
}finally{await browser.close();}
