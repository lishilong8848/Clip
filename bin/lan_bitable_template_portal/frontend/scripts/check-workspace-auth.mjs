import {openWorkspaceRail,selectWorkspaceAccount,selectWorkspaceSetting} from './workspace-account-actions.mjs';
import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url)),output=path.resolve(root,'../../../output/playwright/workspace-v2');
await mkdir(output,{recursive:true});
const server=await preview({root,logLevel:'error',
  ...(process.env.CLIPFLOW_PORTAL_PREVIEW_DIR?{build:{outDir:process.env.CLIPFLOW_PORTAL_PREVIEW_DIR}}:{}),
  preview:{host:'127.0.0.1',port:0}});
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
    if(url.pathname==='/api/auth/status')return ok({logged_in:loggedIn,user:loggedIn?{open_id:'ou_auth_fixture',personnel_record_id:'rec_auth_fixture',name:'登录验收',login_method:'password',role:'user',level:'L4',modules:['learning','knowledge','cabinet','maintenance','repair_management']}: {},scope_options:loggedIn?[{value:'A',label:'A楼'}]:[],login_url:'/api/auth/login'});
    if(url.pathname==='/api/auth/password/people')return ok({items:[{id:'rec_auth_fixture',name:'登录验收',employee_no:'fixture',can_reset_password:true,login_method:'password',selectable:true,needs_setup:false}]});
    if(url.pathname==='/api/auth/login'&&request.method()==='GET')return route.fulfill({status:204});
    if(url.pathname==='/api/auth/guest'&&request.method()==='POST')return route.fulfill({status:204});
    if(request.method()==='POST'){
      assert(['/api/auth/password/login','/api/auth/password/change'].includes(url.pathname),`unexpected write ${url.pathname}`);
      writes.push(url.pathname);pending=route;return;
    }
    return ok({items:[],records:[],options:[]});
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  const submit=()=>page.locator('.pwd-form button[type=submit]');
  async function choose(){await page.locator('#personnel-name-select').fill('登录验收');await page.getByRole('option',{name:'登录验收 · fixture',exact:true}).click();}
  async function answerFailure(){assert(pending,'a real form request was captured');const route=pending;pending=undefined;await route.fulfill({status:401,json:{ok:false,error:'姓名或密码不正确'}});}
  await page.goto(origin+entry);
  const tabs=page.getByRole('tablist',{name:'登录方式'});
  await tabs.waitFor();
  assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'panorama');
  assert.deepEqual((await tabs.getByRole('tab').allTextContents()).map(label=>label.trim()),['飞书授权登录','姓名登录','访客登录']);
  assert.equal(await page.getByText('备用登录',{exact:true}).count(),0);
  await page.screenshot({path:path.join(output,'unified-login-desktop.png')});
  const feishu=page.getByRole('link',{name:'飞书授权登录',exact:true});
  await Promise.all([page.waitForResponse(response=>new URL(response.url()).pathname==='/api/auth/login'),feishu.click()]);
  await page.setViewportSize({width:390,height:480});
  await page.goto(origin+entry+'?auth_notice=cancelled');
  await page.getByRole('status').filter({hasText:'已取消飞书授权'}).waitFor();
  await tabs.getByRole('tab',{name:'飞书授权登录',exact:true}).focus();
  await page.keyboard.press('End');
  await Promise.all([page.waitForResponse(response=>new URL(response.url()).pathname==='/api/auth/guest'),
    page.getByRole('button',{name:'访客登录',exact:true}).click()]);
  await page.setViewportSize({width:1440,height:960});
  await page.goto(origin+entry+'?login=password');await submit().waitFor();
  await page.evaluate(()=>{window.invalidMotion=[];new MutationObserver(records=>{for(const r of records)if(r.target.matches('.pwd-form button[type=submit]')&&r.target.dataset.v2Action)window.invalidMotion.push(r.target.dataset.v2Action);}).observe(document.body,{subtree:true,attributes:true,attributeFilter:['data-v2-action']});});
  await submit().click();await page.getByText('请先选择姓名。',{exact:true}).waitFor();await page.waitForTimeout(1400);
  assert.deepEqual(writes,[]);assert.deepEqual(await page.evaluate(()=>window.invalidMotion),[],'invalid form never enters submission animation');
  assert.equal(await submit().evaluate(el=>getComputedStyle(el).color),'rgb(180, 35, 50)','client validation uses a red retry state, never a green check');
  const box=await page.locator('.session-entry').boundingBox();assert(box.width>=480&&box.width<=530);assert(Math.abs(box.x-(1440-box.x-box.width))<20,'desktop login is centered without sidebar offset');
  await choose();await submit().click();await page.getByText('请输入密码。',{exact:true}).waitFor();assert.deepEqual(writes,[]);
  await page.locator('#personnel-password').fill('fixtureWrong1');
  await submit().click();await page.waitForFunction(()=>document.querySelector('.pwd-form button[type=submit]').disabled);
  const version=page.getByRole('button',{name:'切换旧版',exact:true});await page.waitForFunction(()=>[...document.querySelectorAll('.v2-header-actions button')].find(el=>el.textContent.trim()==='切换旧版')?.disabled);
  assert.equal(await version.isDisabled(),true);const before=page.url();await version.evaluate(el=>el.click());assert.equal(page.url(),before);
  assert.equal(await submit().getAttribute('data-v2-action'),null,'login keeps its ordinary loading button without morphing');
  await answerFailure();await page.waitForFunction(()=>{const el=document.querySelector('#personnel-password');return document.activeElement===el&&el.selectionEnd-el.selectionStart===13;});
  assert.equal(await submit().getAttribute('data-v2-action'),null);assert.equal(await submit().evaluate(el=>getComputedStyle(el).color),'rgb(180, 35, 50)');
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
  loggedIn=true;await page.setViewportSize({width:1440,height:900});await page.goto(origin+entry+'?view=functions');
  const topNav=await openWorkspaceRail(page);
  await topNav.getByRole('img',{name:'世纪互联 VNET'}).waitFor();
  const knowledge=topNav.getByRole('button',{name:'知识中心',exact:true});
  await page.setViewportSize({width:1024,height:768});
  await knowledge.focus();
  await page.keyboard.press('ArrowDown');
  const menu=topNav.getByRole('region',{name:'知识中心',exact:true});
  await menu.waitFor();
  assert(await menu.getByRole('button',{name:'画像学练',exact:true}).count());
  assert(await menu.evaluate(el=>{const box=el.getBoundingClientRect();return box.left>=0&&box.right<=innerWidth&&box.top>=60;}));
  await page.keyboard.press('Escape');
  await menu.waitFor({state:'hidden'});
  for(const width of [800,1024,1280,1440]){
    await page.setViewportSize({width,height:900});
    await page.waitForFunction(()=>{
      const nav=document.querySelector('.v2-topnav'), strip=nav.querySelector('.topnav-tabs');
      const box=nav.getBoundingClientRect(), stripBox=strip.getBoundingClientRect();
      const shown=[...strip.querySelectorAll('.topnav-tab:not([data-overflow])')].map(el=>el.getBoundingClientRect());
      return box.x===0&&box.width===innerWidth&&box.height<=66&&new Set(shown.map(el=>el.top)).size<=1
        && getComputedStyle(strip).overflowX==='clip'&&shown.every(el=>el.right<=stripBox.right+1)
        && document.documentElement.scrollWidth<=innerWidth;
    });
  }
  const apps=topNav.getByRole('button',{name:'全部应用',exact:true});
  const appsPanel=page.getByRole('dialog',{name:'全部应用',exact:true});
  const beforeApps=page.url();
  await apps.click();
  await appsPanel.getByRole('heading',{name:'全部功能',exact:true}).waitFor();
  assert.equal(page.url(),beforeApps,'opening the directory leaves the current page in place');
  assert.equal(await appsPanel.getByRole('button',{name:'账号与权限',exact:true}).count(),0);
  const appSearch=appsPanel.getByRole('searchbox',{name:'搜索已授权功能'});
  await appSearch.fill('画像学练');
  await appsPanel.getByRole('button',{name:'画像学练',exact:true}).waitFor();
  await page.screenshot({path:path.join(output,'platform-navigation-desktop.png')});
  await page.keyboard.press('Escape');
  await appsPanel.waitFor({state:'hidden'});
  assert.equal(await apps.evaluate(el=>el===document.activeElement),true);
  await apps.click();
  await page.locator('.v2-main').click({position:{x:10,y:300}});
  await appsPanel.waitFor({state:'hidden'});
  assert.equal(page.url(),beforeApps);
  await apps.click();
  await appsPanel.getByRole('button',{name:'画像学练',exact:true}).click();
  await appsPanel.waitFor({state:'hidden'});
  await page.waitForURL(url=>url.href!==beforeApps);
  await page.goto(origin+entry+'?account=password');await page.locator('#personnel-current-password').fill('oldFixture');await page.locator('#personnel-change-new-password').fill('newFixture123');await page.locator('#personnel-change-confirm-password').fill('newFixture123');
  await selectWorkspaceAccount(page,'切换旧版');await page.getByRole('button',{name:'继续填写',exact:true}).click();
  await page.mouse.move(800,300);await page.waitForTimeout(250);await submit().click();await openWorkspaceRail(page);await page.getByRole('button',{name:/^账户菜单：/}).click();assert.equal(await page.locator('.n-dropdown-menu').getByRole('menuitem',{name:'切换旧版',exact:true}).getAttribute('aria-disabled'),'true');await page.keyboard.press('Escape');assert(writes.includes('/api/auth/password/change'));
  await answerFailure();await page.waitForFunction(()=>!document.querySelector('.pwd-form button[type=submit]')?.disabled);assert.equal(await page.locator('#personnel-current-password').inputValue(),'oldFixture','changing account-menu focus never discards the password form');
  assert.deepEqual(await page.evaluate(()=>window.invalidMotion||[]),[],'password forms never use three-stage motion');
  assert.deepEqual(errors,[]);
  console.log('PASS: three login entries, password validation and both version guards, default panorama palette, single-line permission-filtered nav, and small viewport; isolated responses only.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
