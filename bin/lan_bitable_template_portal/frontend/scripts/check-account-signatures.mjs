import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/account-signatures');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:950},reducedMotion:'reduce'});
const user={open_id:'self-fixture',name:'隔离验收主管',role:'building',level:'L3',category:'supervisor',modules:['accounts','signatures','maintenance'],can_manage_accounts:true};
const scopes=[{value:'E',label:'E楼'}];
const account={id:'recSub',record_id:'recSub',open_id:'sub-fixture',name:'隔离验收下属',employee_no:'TEST',kind:'external',position:'值班员',category:'operator',home_scope:'E',own_scopes:['E'],scopes:['E'],modules:['signatures'],level:'L4',role:'building',enabled:true,can_edit:true,can_edit_access:true,can_set_password:true,can_delete:true,revision:'rev1'};
const self={source:'staff',record_id:'recSelf',name:user.name,employee_no:'TEST2',building:'E楼',has_signature:true,signature_status:'signed',can_preview:true,can_request:true,is_self:true,request:{status:'completed',saved_at:1791500000}};
const subordinate={...self,record_id:'recSub',name:account.name,is_self:false,request:null};
const image=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=','base64');
let previews=0,posts=[],errors=[],failRead=false,failSend=false,lostReply=false,shared=false;
await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());
  if(url.origin!==origin&&!['data:','blob:'].includes(url.protocol))return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(req.method()==='POST')posts.push({path:url.pathname,body:req.postDataJSON()});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'signature-test'}});
  if(url.pathname==='/api/auth/accounts')return ok({items:[account],scope_options:scopes,policy:{...user,scope_rules:['E']},level_options:[{value:'L4',label:'L4'}],category_options:[{value:'operator',label:'值班员'}],module_options:[]});
  if(url.pathname==='/api/signatures/management/account'){
    if(failRead)return route.fulfill({status:503,json:{ok:false,error:'隔离读取失败'}});
    return ok(shared?{person:null,reason:'值班账号为共享账号，没有个人签名。'}:{person:url.searchParams.get('open_id')?subordinate:self,reason:''});
  }
  if(url.pathname==='/api/signatures/management/preview'){
    previews++;assert(url.searchParams.get('_rt'));return route.fulfill({contentType:'image/png',body:image});
  }
  if(url.pathname==='/api/signatures/management/self-start')return ok({link_url:'/signature?request_id=self-request#token=fixture-token'});
  if(url.pathname==='/api/signatures/management/request'){
    assert.equal(req.headers()['x-signature-token'],'fixture-token');return ok({request_id:'self-request',name:user.name,building:'E楼',status:'ready'});
  }
  if(url.pathname==='/api/signatures/management/submit'){
    assert.equal(req.postDataJSON().confirmed_self,true);self.request={status:'completed',saved_at:1791501000};return ok(self.request);
  }
  if(url.pathname==='/api/signatures/management/requests'){
    if(lostReply){lostReply=false;return route.abort('failed');}
    if(failSend)return ok({status:'send_failed',error:'隔离测试消息未发送'});
    subordinate.request={status:'sent',created_at:1791500000,expires_at:1791586400};return ok(subordinate.request);
  }
  if(req.method()!=='GET')throw new Error('Unexpected write '+url.pathname);
  return ok({items:[],users:[],records:[],settings:{}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html';
try{
  await page.goto(entry+'?view=my');
  let panel=page.locator('.account-signature');await panel.getByText('已有可用签名',{exact:true}).waitFor();
  assert.equal(previews,0,'personal metadata must not download signature images');
  assert(!(await panel.innerText()).includes('E楼楼'));
  await panel.getByRole('button',{name:'查看签名',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.as-preview-img')?.naturalWidth>0);
  assert.equal(previews,1);
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:width<700?844:950});await panel.scrollIntoViewIfNeeded();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    if(width<700)assert((await panel.getByRole('button',{name:'重新签名',exact:true}).boundingBox()).height>=44);
    await page.screenshot({path:path.join(output,'personal-'+width+'.png')});
  }
  await page.setViewportSize({width:1440,height:950});
  await panel.getByRole('button',{name:'重新签名',exact:true}).click();
  await page.locator('.signature-page canvas').waitFor();
  assert.equal(posts.filter(p=>p.path.endsWith('/self-start')).length,1);
  assert.equal(posts.filter(p=>p.path.endsWith('/requests')).length,0);
  const canvas=page.locator('.signature-page canvas'),box=await canvas.boundingBox();
  await page.mouse.move(box.x+40,box.y+60);await page.mouse.down();await page.mouse.move(box.x+160,box.y+95,{steps:8});await page.mouse.up();
  await page.getByRole('checkbox',{name:/我确认由/}).check();
  await page.getByRole('button',{name:'保存到签名表',exact:true}).click();
  await page.getByText('签名已完成',{exact:true}).waitFor();
  assert.equal(posts.filter(p=>p.path.endsWith('/submit')).length,1);

  await page.goto(entry+'?__page=/admin/settings');
  await page.getByRole('button',{name:'管理隔离验收下属',exact:true}).click();
  const editor=page.getByRole('dialog',{name:'隔离验收下属'});panel=editor.locator('.account-signature');
  await panel.getByText('已有可用签名',{exact:true}).waitFor();
  assert.equal(await panel.getByRole('button',{name:'重新签名',exact:true}).count(),0);
  await panel.getByRole('button',{name:'发起签名',exact:true}).click();
  assert.equal(posts.filter(p=>p.path.endsWith('/requests')).length,0,'opening confirmation does not send');
  failSend=true;await panel.getByRole('button',{name:'确认发送',exact:true}).click();
  await panel.getByText(/隔离测试消息未发送/).waitFor();
  assert.equal(await panel.getByText('签名请求已发送，请该人员通过链接完成签名。',{exact:true}).count(),0);
  failSend=false;await panel.getByRole('button',{name:'确认发送',exact:true}).click();
  await panel.getByText('签名请求已发送，请该人员通过链接完成签名。',{exact:true}).waitFor();
  assert.equal(posts.filter(p=>p.path.endsWith('/requests')).length,2);
  assert.equal(posts.at(-1).body.record_id,'recSub');
  await panel.getByRole('button',{name:'发起签名',exact:true}).click();
  lostReply=true;await panel.getByRole('button',{name:'确认发送',exact:true}).click();
  await panel.getByText(/如不确定对方是否收到/).waitFor();
  const uncertainId=posts.at(-1).body.operation_id;
  await panel.getByRole('button',{name:'确认发送',exact:true}).click();
  await panel.getByText('签名请求已发送，请该人员通过链接完成签名。',{exact:true}).waitFor();
  assert.equal(posts.at(-1).body.operation_id,uncertainId,'lost response retry retains idempotency identity');
  await page.screenshot({path:path.join(output,'manager-request.png')});
  await editor.getByRole('button',{name:'关闭账号详情',exact:true}).click();

  shared=true;await page.goto(entry+'?view=my');panel=page.locator('.account-signature');
  await panel.getByText(/共享账号/).waitFor();assert.equal(await panel.getByRole('button',{name:'重新签名'}).count(),0);
  shared=false;failRead=true;await page.reload();await panel.getByText('隔离读取失败',{exact:true}).waitFor();
  failRead=false;await panel.getByRole('button',{name:'重试',exact:true}).click();await panel.getByText('已有可用签名',{exact:true}).waitFor();
  assert.deepEqual(errors,[]);
  console.log('PASS: personal signature metadata/preview, full re-sign and save flow, scoped account detail request/confirmation/failure/success, shared account exclusion, error recovery, desktop/mobile. All external writes mocked.');
}catch(error){await page.screenshot({path:path.join(output,'failure.png')});throw error;}
finally{await context.close();await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
