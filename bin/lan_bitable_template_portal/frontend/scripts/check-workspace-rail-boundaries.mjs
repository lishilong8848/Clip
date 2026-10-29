import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
const root=fileURLToPath(new URL('..',import.meta.url)),out=path.resolve(root,'../../../output/playwright/rail-boundaries');await mkdir(out,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[],contexts=[];
async function make(options){
 const context=await browser.newContext(options);contexts.push(context);
 await context.route('**/*',route=>{const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();assert.equal(req.method(),'GET','navigation review must never write business data');
 const ok=data=>route.fulfill({json:{ok:true,data}});
 if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'rail-boundary',name:'导航边界检查',role:'building',level:'L4',modules:['learning'],login_method:'password'},scope_options:[{value:'A',label:'A楼'}]});
 if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'rail-boundary'}});
 return ok({items:[],settings:{},scopes:[]});});
 const page=await context.newPage();page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/assets/workspace-v2/index.html?view=my');await page.getByRole('heading',{name:'导航边界检查',exact:true}).waitFor();return page;
}
async function expanded(page){await page.waitForFunction(()=>{const e=document.querySelector('.v2-rail'),r=e.getBoundingClientRect();return e.dataset.collapsed==='false'&&Math.round(r.width)===240&&Math.round(r.x)===0;});}
try{
 const page=await make({viewport:{width:1440,height:900}}),rail=page.locator('.v2-rail');
 await page.mouse.move(900,200);await page.keyboard.press('Tab');await expanded(page);
 await page.keyboard.press('Escape');await page.waitForFunction(()=>document.querySelector('.v2-rail').dataset.collapsed==='true');
 // Existing cabinet dialogs use z-index 140; the rail must stay below them.
 await page.evaluate(()=>{const d=document.createElement('div');d.id='existing-dialog-probe';d.style.cssText='position:fixed;inset:0;z-index:140;background:white';d.innerHTML='<button style="position:absolute;left:12px;top:12px;width:60px;height:44px">对话框操作</button>';document.body.append(d);});
 assert(await page.evaluate(()=>!!document.elementFromPoint(36,36)?.closest('#existing-dialog-probe')),'existing modal controls must cover the rail');
 await page.getByRole('button',{name:'对话框操作'}).click();await page.locator('#existing-dialog-probe').evaluate(e=>e.remove());
 await page.locator('.v2-rail-brand').hover();await expanded(page);await page.getByRole('button',{name:'导航配色',exact:true}).click();await page.locator('.v2-theme-picker').waitFor();
 await page.setViewportSize({width:402,height:874});await page.locator('.v2-rail').waitFor({state:'detached'});await page.locator('.v2-theme-picker').waitFor({state:'hidden'});
 for(const viewport of [{width:402,height:874},{width:440,height:956},{width:360,height:640}]){
  await page.setViewportSize(viewport);assert.equal(await page.locator('.v2-rail,.v2-mobile-menu,.v2-rail-backdrop').count(),0,'phone has no side navigation');
  for(const name of ['首页','待办','功能','我的'])assert(await page.locator('.v2-mobile-nav').getByRole('button',{name,exact:true}).isVisible());
  assert(await page.getByRole('button',{name:'导航配色',exact:true}).isVisible());await page.getByRole('button',{name:'账户菜单：导航边界检查',exact:true}).click();
  for(const name of ['切换账号','退出登录','切换旧版'])assert(await page.locator('.n-dropdown-menu').getByText(name,{exact:true}).isVisible());
  assert.equal(await page.locator('.n-dropdown-menu').getByText('系统设置',{exact:true}).count(),0,'member receives no system settings access');await page.keyboard.press('Escape');
  await page.screenshot({path:path.join(out,`mobile-my-${viewport.width}.png`)});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
 }
 await page.getByRole('button',{name:'导航配色',exact:true}).click();await page.getByRole('button',{name:'黑金',exact:true}).click();
 await page.goto(origin+'/assets/workspace-v2/index.html?view=permission');await page.waitForFunction(()=>document.documentElement.dataset.v2Palette==='gold');assert.equal(await page.locator('.v2-theme-toggle').count(),0,'theme loads on pages with no theme picker');
 await page.setViewportSize({width:1440,height:900});await page.locator('.v2-rail').waitFor();await page.locator('.v2-rail-brand').hover();await expanded(page);
 assert.equal(await page.locator('.v2-rail').evaluate(e=>getComputedStyle(e).borderTopLeftRadius),'20px');
 const phone=await make({viewport:{width:956,height:440},hasTouch:true,isMobile:true});assert.equal(await phone.locator('.v2-rail').count(),0,'landscape phone still has no sidebar');assert(await phone.locator('.v2-mobile-nav').isVisible());assert(!await phone.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await phone.screenshot({path:path.join(out,'mobile-landscape.png')});
 const touch=await make({viewport:{width:1440,height:900},hasTouch:true});assert.equal(await touch.evaluate(()=>matchMedia('(hover:hover)').matches),false);
 await touch.locator('.v2-rail-brand').getByRole('button',{name:'展开导航',exact:true}).tap();await expanded(touch);await touch.screenshot({path:path.join(out,'touch-desktop.png')});
 await touch.locator('.v2-rail').getByRole('button',{name:'收起导航',exact:true}).tap();await touch.waitForFunction(()=>document.querySelector('.v2-rail').dataset.collapsed==='true');
 assert.deepEqual(errors,[]);console.log('PASS: existing modal layering, non-hover PC opening, 20px corners, no phone sidebar, mobile account/theme entry, per-page theme restore, resize cleanup, 402/440/360 layouts; no business writes.');
}catch(error){for(let i=0;i<contexts.length;i++){const page=contexts[i].pages()[0];if(page)await page.screenshot({path:path.join(out,`failure-${i}.png`)});}throw error;}
finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
