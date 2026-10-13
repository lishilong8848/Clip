import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url)),output=path.resolve(root,'../../../output/playwright/workspace-v2');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`,entry='/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true});
const errors=[],writes=[];let loggedIn=false,pending;
try{
  const context=await browser.newContext({viewport:{width:1440,height:960}});
  await context.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'auth-motion-fixture'}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?{open_id:'ou_auth_fixture',personnel_record_id:'rec_auth_fixture',name:'登录验收',login_method:'password',role:'user',level:'L4',modules:['learning']}: {},scope_options:loggedIn?[{value:'A',label:'A楼'}]:[],login_url:'/api/auth/login'});
    if(url.pathname==='/api/auth/password/people')return ok({items:[{id:'rec_auth_fixture',name:'登录验收',building:'A楼',employee_no:'fixture',account_nature:'外部',login_method:'password',selectable:true,needs_setup:false}]});
    if(request.method()==='POST'){
      assert(['/api/auth/password/login','/api/auth/password/change'].includes(url.pathname),`unexpected write ${url.pathname}`);
      writes.push(url.pathname);pending=route;return;
    }
    return ok({items:[],records:[],options:[]});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  const submit=()=>page.locator('.pwd-form button[type=submit]');
  async function choose(){await page.locator('#personnel-name-select').fill('登录验收');await page.getByRole('option',{name:'登录验收 · A楼 · fixture',exact:true}).click();}
  async function answerFailure(){assert(pending,'a real form request was captured');const route=pending;pending=undefined;await route.fulfill({status:401,json:{ok:false,error:'姓名或密码不正确'}});}
  await page.goto(origin+entry+'?login=password');await submit().waitFor();
  await page.evaluate(()=>{window.invalidMotion=[];new MutationObserver(records=>{for(const r of records)if(r.target.matches('.pwd-form button[type=submit]')&&r.target.dataset.v2Action)window.invalidMotion.push(r.target.dataset.v2Action);}).observe(document.body,{subtree:true,attributes:true,attributeFilter:['data-v2-action']});});
  await submit().click();await page.getByText('请先选择姓名。',{exact:true}).waitFor();await page.waitForTimeout(1400);
  assert.deepEqual(writes,[]);assert.deepEqual(await page.evaluate(()=>window.invalidMotion),[],'invalid form never enters submission animation');
  assert.equal(await submit().evaluate(el=>getComputedStyle(el).color),'rgb(180, 35, 50)','client validation uses a red retry state, never a green check');
  const box=await page.locator('.login-panel').boundingBox();assert(box.width>=480&&box.width<=560);assert(Math.abs(box.x-(1440-box.x-box.width))<20,'desktop login is centered without sidebar offset');
  await choose();await submit().click();await page.getByText('请输入密码。',{exact:true}).waitFor();assert.deepEqual(writes,[]);
  await page.locator('#personnel-password').fill('fixtureWrong1');
  await submit().click();await page.waitForFunction(()=>document.querySelector('.pwd-form button[type=submit]').disabled);
  const version=page.getByRole('button',{name:'切换旧版',exact:true});await page.waitForFunction(()=>document.querySelector('.v2-header-actions button').disabled);
  assert.equal(await version.isDisabled(),true);const before=page.url();await version.evaluate(el=>el.click());assert.equal(page.url(),before);
  await answerFailure();await page.waitForFunction(()=>document.querySelector('.pwd-form button[type=submit]').dataset.v2Action==='error',null,{timeout:800});await page.waitForFunction(()=>{const el=document.querySelector('#personnel-password');return document.activeElement===el&&el.selectionEnd-el.selectionStart===13;});
  assert.equal(await version.isDisabled(),false);await page.screenshot({path:path.join(output,'login-validation-desktop.png'),fullPage:true});
  await page.waitForFunction(()=>!document.querySelector('.pwd-form button[type=submit]').dataset.v2Action);
  await page.locator('#personnel-password').fill('anotherWrong1');assert.equal(await page.locator('.error-message').count(),0);await submit().click();await page.waitForFunction(()=>document.querySelector('.pwd-form button[type=submit]').disabled);
  await page.locator('.v2-brand').focus();await answerFailure();await page.waitForTimeout(500);
  assert.equal(await page.locator('.v2-brand').evaluate(el=>document.activeElement===el),true,'manual focus movement is not overridden');
  await version.click();await page.getByRole('button',{name:'继续填写',exact:true}).click();assert.equal(page.url(),before);assert.equal(await page.locator('#personnel-password').inputValue(),'anotherWrong1');
  assert.equal(await page.evaluate(()=>JSON.stringify({...localStorage,...sessionStorage}).includes('anotherWrong1')),false,'no credentials are persisted');
  await version.click();await page.getByRole('button',{name:'放弃填写并切换',exact:true}).click();await page.waitForURL(url=>url.pathname==='/');
  await choose();await page.locator('#personnel-password').fill('legacyWrong1');await submit().click();await page.waitForFunction(()=>document.querySelector('.frontend-version-switch').disabled);
  const oldUrl=page.url();await page.locator('.frontend-version-switch').evaluate(el=>el.click());assert.equal(page.url(),oldUrl);await answerFailure();
  await page.getByRole('button',{name:'切换新版',exact:true}).click();await page.getByRole('button',{name:'放弃填写并切换',exact:true}).click();await page.waitForURL(url=>url.pathname===entry);
  assert.equal(await page.locator('#personnel-password').inputValue(),'');
  await page.setViewportSize({width:390,height:844});await page.screenshot({path:path.join(output,'login-validation-mobile.png'),fullPage:true});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  loggedIn=true;await page.goto(origin+entry+'?account=password');await page.locator('#personnel-current-password').fill('oldFixture');await page.locator('#personnel-change-new-password').fill('newFixture123');await page.locator('#personnel-change-confirm-password').fill('newFixture123');
  await version.click();await page.getByRole('button',{name:'继续填写',exact:true}).click();
  await submit().click();await page.waitForFunction(()=>document.querySelector('.v2-header-actions button').disabled);assert.equal(await version.isDisabled(),true);assert(writes.includes('/api/auth/password/change'));
  await answerFailure();await page.waitForFunction(()=>{const el=document.querySelector('#personnel-current-password');return document.activeElement===el&&el.selectionEnd-el.selectionStart===10;});
  assert.deepEqual(errors,[]);
  console.log('PASS: real login validation, no false success, busy/dirty version guards in both versions, password focus and user override, centered desktop/mobile, change-password guard; isolated responses only.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
