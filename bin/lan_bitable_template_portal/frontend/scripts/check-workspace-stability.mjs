import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { preview } from 'vite';
import { chromium } from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/workspace-v2');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:960}});
const user={open_id:'ou_stability',name:'隔离验收账号',role:'admin',level:'L1',modules:['plan_convergence','handover']};
const status=()=>({logged_in:loggedIn,user:loggedIn?user:{},scope_options:loggedIn?[{value:'A',label:'A楼'}]:[],login_url:'/api/auth/login'});
let loggedIn=true,holdAuth=false,pendingAuth,holdHandover=false,pendingHandover,planReads=0,planRevision=1;
const errors=[];
const page=await context.newPage();
page.setDefaultTimeout(10000);
page.on('pageerror',error=>errors.push(error.message));
await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());
  if(url.origin!==origin)return route.abort();
  if(!url.pathname.startsWith('/api/'))return route.continue();
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'isolated-stability'}});
  if(url.pathname==='/api/auth/status'){
    if(holdAuth){pendingAuth={route,data:status()};return;}
    return ok(status());
  }
  if(url.pathname==='/api/auth/logout'){loggedIn=false;return ok({});}
  assert.equal(req.method(),'GET','only the fixture logout may write');
  if(url.pathname==='/api/handover-links'){
    if(holdHandover){pendingHandover=route;return;}
    return ok({links:{A:'https://example.invalid/account-a-review'}});
  }
  if(url.pathname==='/api/plan-convergence/maintenance/records'){
    planReads++;
    return ok([{record_id:'fixture-'+planRevision,name:'待核对事项第'+planRevision+'版',building:'A楼',check_status:'pending'}]);
  }
  if(url.pathname==='/api/plan-convergence/change/records')return ok([]);
  return ok({items:[],records:[],options:[]});
});
const home=origin+'/assets/workspace-v2/index.html';
try{
  // A tab opened in the background must load its visible widgets when focused.
  await page.addInitScript(()=>{window.fixtureHidden=true;Object.defineProperty(document,'hidden',{get:()=>window.fixtureHidden,configurable:true});});
  await page.goto(home);
  await page.getByRole('heading',{name:'个人工作台',exact:true}).waitFor();
  await page.waitForTimeout(300);
  assert.equal(planReads,0);
  await page.evaluate(()=>{window.fixtureHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await page.getByText('待核对事项第1版',{exact:true}).waitFor();
  await page.evaluate(()=>{window.fixtureHidden=true;document.dispatchEvent(new Event('visibilitychange'));});
  planRevision=2;
  await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));
  const hiddenReads=planReads;
  await page.waitForTimeout(100);
  assert.equal(planReads,hiddenReads,'hidden widgets do not issue background reads');
  await page.evaluate(()=>{window.fixtureHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await page.getByText('待核对事项第2版',{exact:true}).waitFor();
  const freshReads=planReads;
  await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
  await page.waitForTimeout(100);
  assert.equal(planReads,freshReads,'returning to a fresh widget does not repeat its business reads');

  // A previously started auth refresh must never undo an explicit logout.
  await page.getByRole('button',{name:'打开个人页面',exact:true}).click();
  holdAuth=true;
  const authStarted=page.waitForRequest(r=>new URL(r.url()).pathname==='/api/auth/status');
  await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));
  await authStarted;
  await page.getByRole('button',{name:'退出登录',exact:true}).click();
  await page.getByRole('tab',{name:'账号密码登录',exact:true}).waitFor();
  assert(pendingAuth);
  holdAuth=false;
  await pendingAuth.route.fulfill({json:{ok:true,data:pendingAuth.data}}).catch(()=>{});
  await page.waitForTimeout(250);
  assert.equal(await page.getByRole('button',{name:'打开个人页面',exact:true}).count(),0,'late auth response resurrected the signed-out account');
  assert.equal(await page.locator('.v2-sidebar').count(),0);
  await page.screenshot({path:path.join(output,'stability-logout.png')});

  // The same route must not show the previous person's review links while loading.
  loggedIn=true;
  await page.goto(home+'?view=handover');
  await page.evaluate(()=>{window.fixtureHidden=false;document.dispatchEvent(new Event('visibilitychange'));});
  await page.getByRole('link',{name:'A楼审核',exact:true}).waitFor();
  user.open_id='ou_stability_b';user.name='另一位隔离账号';holdHandover=true;
  await page.evaluate(()=>window.dispatchEvent(new Event('auth-account-changed')));
  await page.getByRole('button',{name:'打开个人页面',exact:true}).getByText(user.name,{exact:true}).waitFor();
  assert.equal(await page.getByRole('link',{name:'A楼审核',exact:true}).count(),0,'old account review links were retained');
  assert(pendingHandover,'new account must load its own review links');
  await pendingHandover.fulfill({json:{ok:true,data:{links:{}}}});
  await page.getByText('暂无可用审核入口',{exact:true}).waitFor();
  assert.deepEqual(errors,[]);
  console.log('PASS: background-tab recovery, hidden invalidation, logout/auth race and account-isolated handover links. No real business requests.');
}catch(error){await page.screenshot({path:path.join(output,'stability-failure.png'),fullPage:true});throw error;}
finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
