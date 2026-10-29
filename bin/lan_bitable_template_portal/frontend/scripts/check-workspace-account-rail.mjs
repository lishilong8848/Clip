import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {accountFixture} from './responsive-audit-account-fixtures.mjs';
import {collabFixture} from './responsive-audit-collab-fixtures.mjs';
import {workspaceExtensionFixture} from './workspace-extension-fixtures.mjs';
import './check-workspace-palettes.mjs';
const root=fileURLToPath(new URL('..',import.meta.url)),out=path.resolve(root,'../../../output/playwright/account-rail');
await mkdir(out,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:900}}),page=await context.newPage();
page.setDefaultTimeout(15000);
let logged=true,user={open_id:'rail-fixture',name:'布局验收管理员',role:'admin',level:'L0',category:'manager',login_method:'password',modules:['accounts','learning','maintenance','critical_guard','signatures']};
const errors=[],writes=[];page.on('pageerror',e=>errors.push(e.message));
const scopes=['A','B','C','D','E','H'].map(value=>({value,label:value+'楼'}));
let archiveStatus='failed',archiveFail=false,slowWeather=false,releaseWeather;
const weather=()=>({paused:true,running:false,phase:'idle',last_result:{archive_failed:1},tasks:[{task_id:'weather-1',warning_title:'南通市气象台发布台风蓝色预警',archive_status:archiveStatus,archive_error:archiveStatus==='failed'?'MOP 文件上传失败: 1061004 - forbidden.':''},{task_id:'weather-2',warning_title:'其它未解决任务',archive_status:'failed',archive_error:'独立失败'}]});
await context.route('**/*',async route=>{
 const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
 const ok=data=>route.fulfill({json:{ok:true,data}});if(req.method()!=='GET')writes.push(url.pathname);
 if(url.pathname==='/api/auth/status')return ok({logged_in:logged,user:logged?user:{},scope_options:logged?scopes:[]});
 if(url.pathname==='/api/auth/logout'){logged=false;return ok({});}
 if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'rail-test'}});
 if(url.pathname==='/api/critical-guard/weather-status'){const snapshot=weather();if(slowWeather){slowWeather=false;await new Promise(r=>releaseWeather=r);}return ok(snapshot);}
 if(url.pathname.endsWith('/weather-1/archive-retry')){if(archiveFail)return ok({status:'failed',error:'归档仍失败'});archiveStatus='archived';return ok({status:'archived',record_id:'rec-archive'});}
 for(const fixture of [workspaceExtensionFixture,accountFixture,collabFixture]){const value=await fixture({url,method:req.method(),body:req.postData()?JSON.parse(req.postData()):{}});if(value)return route.fulfill(value.json?{status:value.status||200,json:value.json}:{status:value.status||200,body:value.body||'',contentType:value.contentType||'text/plain'});}
 if(req.method()!=='GET'&&!url.pathname.startsWith('/api/workspace-layouts/'))throw new Error('Unexpected write '+url.pathname);
 return ok({items:[],records:[],users:[],tasks:[],scopes:[],settings:{},stats:{},revision:0,value:null});
});
const entry=origin+'/assets/workspace-v2/index.html',rail=page.locator('.v2-rail'),main=page.locator('.v2-main');
async function openRail(){if(page.viewportSize().width<=760)await page.getByRole('button',{name:'展开导航',exact:true}).click();else await page.locator('.v2-rail-brand').hover();await page.waitForFunction(()=>Math.round(document.querySelector('.v2-rail').getBoundingClientRect().width)===240&&document.querySelector('.v2-rail').getBoundingClientRect().x===0);}
async function account(action){await openRail();await page.getByRole('button',{name:'账户菜单：'+user.name,exact:true}).click();await page.locator('.n-dropdown-menu').getByText(action,{exact:true}).click();}
try{
 await page.goto(entry+'?view=my');await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
 assert.equal(await page.locator('.v2-header').count(),0);
 assert.equal(Math.round((await rail.boundingBox()).width),72);
 const initial=await main.boundingBox();await openRail();
 assert.deepEqual(await main.boundingBox(),initial,'hover never resizes business workspace');
 assert.equal(await rail.locator('.v2-rail-user .v2-rail-label').first().evaluate(e=>getComputedStyle(e).transitionDelay),'0.08s');
 const footer=await page.locator('.v2-rail-footer').boundingBox();await page.locator('.v2-rail-scroll').evaluate(e=>e.scrollTop=e.scrollHeight);
 assert.deepEqual(await page.locator('.v2-rail-footer').boundingBox(),footer,'account area stays fixed while list scrolls');
 await page.mouse.move(800,250);await page.waitForFunction(()=>Math.round(document.querySelector('.v2-rail').getBoundingClientRect().width)===72);
 assert.deepEqual(await main.boundingBox(),initial);
 await account('系统设置');await page.getByRole('heading',{name:'后台状态',exact:true}).waitFor();
 assert.equal(await page.getByRole('combobox',{name:'设置分类'}).count(),0);
 assert.equal(await page.getByRole('button',{name:'返回个人页面'}).count(),0);
 await openRail();await rail.getByRole('menuitem',{name:'账号与权限',exact:true}).click();await page.getByRole('searchbox',{name:'搜索账号',exact:true}).waitFor();
 await page.screenshot({path:path.join(out,'desktop-settings.png')});
 await account('个人信息');await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
 await openRail();await page.getByRole('button',{name:'导航配色',exact:true}).click();
 for(const dark of [false,true]){
  const toggle=page.getByRole('switch',{name:'夜间模式'});if(await toggle.getAttribute('aria-checked')!==String(dark))await toggle.click();
  const sides=new Set();for(const name of ['钴蓝银白','黑金','朱砂红橙','松绿薄荷','石墨冰青','莓红石白']){await page.getByRole('button',{name,exact:true}).click();sides.add(await rail.evaluate(e=>getComputedStyle(e).backgroundImage));}
  assert.equal(sides.size,6,'every palette visibly changes navigation');
 }
 await page.getByRole('switch',{name:'夜间模式'}).click();await page.getByRole('button',{name:'松绿薄荷',exact:true}).click();await main.click({position:{x:600,y:100}});
 await page.reload();await page.getByRole('heading',{name:user.name,exact:true}).waitFor();assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'mint');
 for(const viewport of [{width:402,height:874},{width:440,height:956},{width:360,height:640},{width:1440,height:900}]){
  await page.setViewportSize(viewport);await page.mouse.move(viewport.width-10,300);await page.waitForTimeout(300);
  if(viewport.width<=760){
   assert.equal(await rail.count(),0,'mobile uses bottom navigation only');assert(await page.locator('.v2-mobile-nav').getByRole('button',{name:'我的',exact:true}).isVisible());
   await page.getByRole('button',{name:'系统设置',exact:true}).click();await page.getByRole('heading',{name:'后台状态',exact:true}).waitFor();
   await page.getByRole('combobox',{name:'设置分类',exact:true}).click();await page.locator('.n-base-select-menu').getByText('账号与权限',{exact:true}).click();await page.getByRole('searchbox',{name:'搜索账号',exact:true}).waitFor();
   await page.locator('.v2-mobile-nav').getByRole('button',{name:'我的',exact:true}).click();await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
  }else{await openRail();const rect=await page.locator('.v2-rail-footer').boundingBox();assert(rect.y>=0&&rect.y+rect.height<=viewport.height+1);assert.equal(await rail.evaluate(e=>getComputedStyle(e).borderBottomRightRadius),'20px');}
  await page.screenshot({path:path.join(out,`navigation-${viewport.width}.png`)});
  assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
 }
 await page.setViewportSize({width:1440,height:900});user={...user,open_id:'member-rail',name:'普通人员',role:'building',level:'L4',modules:['learning']};await page.goto(entry+'?view=my');await page.getByRole('heading',{name:user.name,exact:true}).waitFor();
 assert.equal(await page.locator('html').getAttribute('data-v2-palette'),'cobalt');await openRail();await page.getByRole('button',{name:'账户菜单：'+user.name}).click();assert.equal(await page.locator('.n-dropdown-menu').getByText('系统设置',{exact:true}).count(),0);
 await page.locator('.n-dropdown-menu').getByText('切换账号',{exact:true}).click();await page.getByRole('button',{name:'取消',exact:true}).click();assert(!writes.includes('/api/auth/logout'));
 await account('切换账号');await page.getByRole('button',{name:'继续',exact:true}).click();await page.locator('#personnel-name-select').waitFor();assert.equal(writes.filter(v=>v==='/api/auth/logout').length,1);
 await page.clock.install();logged=true;user={...user,open_id:'admin-again',name:'管理员',role:'admin',level:'L0',modules:['accounts','critical_guard']};await page.goto(entry+'?__page=/critical-guard&mode=admin');
 const failure=page.locator('.weather-archive-failures>div').filter({hasText:'台风蓝色预警'});await failure.waitFor();
 archiveFail=true;await failure.getByRole('button',{name:'重试归档'}).click();await page.getByText('归档仍失败',{exact:true}).first().waitFor();assert(await failure.isVisible(),'HTTP 200 with failed result must not clear warning');
 archiveFail=false;slowWeather=true;await page.clock.fastForward(31_000);for(let i=0;i<100&&!releaseWeather;i++)await new Promise(r=>setTimeout(r,20));assert(releaseWeather,'old status request is in flight');await failure.getByRole('button',{name:'重试归档'}).click();await failure.waitFor({state:'hidden'});releaseWeather();await page.waitForTimeout(200);assert.equal(await failure.count(),0,'late failed snapshot cannot restore successful task warning');
 assert(await page.locator('.weather-archive-failures').getByText('其它未解决任务',{exact:false}).isVisible());
 assert.deepEqual(errors,[]);console.log('PASS: floating 72/240 rail, 80ms labels, 20px corners, fixed PC footer, mobile bottom navigation, account/settings routes and permissions, 12 themes, account isolation, mobile layouts, logout, archive retry outcomes.');
}catch(error){await page.screenshot({path:path.join(out,'failure.png'),fullPage:true});console.error(await page.locator('body').innerText());throw error;}
finally{await context.close();await browser.close();await new Promise(r=>server.httpServer.close(r));}
