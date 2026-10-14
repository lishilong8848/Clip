import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { build } from 'vite';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/workspace-v2-feedback');
// Build in memory: do not replace the production assets used by the running project.
const result = await build({ root, configFile: path.join(root,'vite.workspace-v2.config.ts'), logLevel:'error', build:{write:false} });
const assets = new Map(result.output.map(item=>['/assets/workspace-v2/'+item.fileName,item.type==='chunk'?item.code:item.source]));
const types = { '.html':'text/html; charset=utf-8', '.js':'text/javascript', '.css':'text/css', '.json':'application/json', '.png':'image/png', '.svg':'image/svg+xml' };
const server = createServer(async(req,res)=>{
  const url=new URL(req.url,'http://fixture');
  let data=assets.get(url.pathname);
  if(data===undefined&&url.pathname.startsWith('/assets/')&&!url.pathname.includes('..')){
    try{data=await readFile(path.join(root,'public',url.pathname));}catch{/* Unknown test assets return 404. */}
  }
  if(data===undefined){res.writeHead(404);res.end();return;}
  res.setHeader('content-type',types[path.extname(url.pathname)]||'application/octet-stream');res.end(data);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port,entry='/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];let loggedIn=false,noticeReads=0,failNotice=true;
try{
  await mkdir(output,{recursive:true});
  const context=await browser.newContext({viewport:{width:1440,height:960}});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'toast-fixture'}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?{open_id:'ou_toast_fixture',name:'提示验收',role:'admin',level:'L1',modules:['maintenance']}: {},scope_options:loggedIn?[{value:'A',label:'A楼'}]:[],login_url:'/api/auth/login'});
    if(url.pathname==='/api/auth/password/people')return ok({items:[{id:'rec_toast_fixture',name:'提示验收',building:'A楼',employee_no:'fixture',account_nature:'外部',login_method:'password',selectable:true,needs_setup:false}]});
    if(req.method()!=='GET'){
      writes.push(url.pathname);
      assert.equal(url.pathname,'/api/auth/password/login','no real business operation is submitted');
      return route.fulfill({status:401,json:{ok:false,error:'姓名或密码不正确'}});
    }
    if(url.pathname.startsWith('/api/workbench')){
      noticeReads++;
      if(failNotice)return route.fulfill({status:400,json:{ok:false,error:'通告读取失败，请重试'}});
      return ok({records:[],ongoing:[],records_pagination:{total:0},ongoing_pagination:{total:0},fragments:{list:'',detail:''}});
    }
    return ok({items:[],records:[],options:[]});
  });
  const page=await context.newPage();page.setDefaultTimeout(20000);
  page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin+entry+'?login=password');
  await page.locator('.pwd-form button[type=submit]').click();
  const loginAlert=page.locator('.error-message[data-v2-feedback-visible=true]');
  await loginAlert.waitFor();
  assert.match(await loginAlert.innerText(),/请先选择姓名/);
  assert.deepEqual(writes,[]);
  await page.locator('#personnel-name-select').fill('提示验收');
  await page.getByRole('option',{name:'提示验收 · A楼 · fixture',exact:true}).click();
  await page.locator('#personnel-password').fill('fixtureWrong1');
  await page.locator('.pwd-form button[type=submit]').click();
  await page.waitForFunction(()=>{const input=document.querySelector('#personnel-password');return input===document.activeElement&&input.selectionEnd-input.selectionStart===13;});
  await loginAlert.waitFor();
  assert.equal(await loginAlert.getAttribute('data-v2-feedback-tone'),'error');
  await page.waitForTimeout(400);
  const box=await loginAlert.boundingBox();assert(box.x>1000&&box.y>700,'login errors are bottom-right');
  await page.screenshot({path:path.join(output,'login-toast-desktop.png')});
  await page.locator('#personnel-password').fill('changed');
  assert.equal(await page.locator('.error-message').count(),0,'editing clears the original validation state and toast');
  loggedIn=true;
  await page.goto(origin+entry+'?__page=/workbench-lite&scope=A&work_type=maintenance');
  const noticeAlert=page.locator('.v2-notice-feedback[data-v2-feedback-visible=true]');
  await noticeAlert.waitFor();
  assert.equal(await noticeAlert.getAttribute('data-v2-feedback-tone'),'error','compiled NAlert preserves its exact error type');
  const reads=noticeReads;failNotice=false;
  await noticeAlert.getByRole('button',{name:'重新读取',exact:true}).click();
  await page.waitForFunction(()=>!document.querySelector('.v2-notice-feedback'));
  assert(noticeReads>reads,'retry continues to call the original read API');
  assert.deepEqual(writes,['/api/auth/password/login']);
  assert.deepEqual(errors,[]);
  console.log('PASS: in-memory production build; full login validation/focus/clear; compiled notice error color and original retry; production assets and business data unchanged.');
}finally{
  await browser.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
}
