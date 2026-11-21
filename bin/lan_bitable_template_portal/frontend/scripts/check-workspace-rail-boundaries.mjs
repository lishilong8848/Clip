import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
const root=fileURLToPath(new URL('..',import.meta.url)),out=path.resolve(root,'../../../output/playwright/rail-boundaries');await mkdir(out,{recursive:true});
const bundle=process.env.CLIPFLOW_WORKSPACE_BUILD_DIR;
const server=await preview({root,configFile:false,logLevel:'error',
 ...(bundle?{base:'/assets/workspace-v2/',build:{outDir:bundle}}:{}),preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[],contexts=[];
async function make(options){
 const context=await browser.newContext(options);contexts.push(context);
 await context.route('**/*',async route=>{const req=route.request(),url=new URL(req.url());if(url.origin!==origin)return route.abort();
 if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
 if(!url.pathname.startsWith('/api/'))return route.continue();assert.equal(req.method(),'GET','navigation review must never write business data');
 const ok=data=>route.fulfill({json:{ok:true,data}});
 if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'rail-boundary',name:'导航边界检查',role:'building',level:'L4',home_scope:'A',modules:['learning','knowledge','maintenance','plan_convergence'],login_method:'password'},scope_options:[{value:'A',label:'A楼'}]});
 if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'rail-boundary'}});
 if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
 if(url.pathname==='/api/workbench')return ok({ongoing:[{active_item_id:'notice-1',building:'A楼',scope:'A',owner_open_id:'rail-boundary',work_type:'maintenance',title:'A楼维护进行中',status:'开始'}],records:[],ongoing_pagination:{total_pages:1},records_pagination:{total_pages:1}});
 if(url.pathname==='/api/history-summary')return ok({scope:url.searchParams.get('scope'),month:url.searchParams.get('month'),days:[]});
 return ok({items:[],settings:{},scopes:[]});});
 const page=await context.newPage();page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/assets/workspace-v2/index.html?view=my');await page.getByRole('heading',{name:'导航边界检查',exact:true}).waitFor();return page;
}
try {
 const page=await make({viewport:{width:1440,height:900}}),nav=page.getByRole('navigation',{name:'功能导航',exact:true});
 await nav.waitFor();
 const notices=nav.locator('[data-group="notices"]'),library=nav.locator('[data-group="learning"]');
 await notices.focus();await page.keyboard.press('ArrowDown');
 assert.equal(await notices.getAttribute('aria-expanded'),'true');
 assert(await nav.locator('#nav-panel-notices').evaluate(el=>el.contains(document.activeElement)));
 const noticeMenu=await nav.locator('#nav-panel-notices').boundingBox(), noticeTab=await notices.boundingBox();
 assert(noticeMenu.width<=225 && Math.abs(noticeMenu.x-noticeTab.x)<=3,'menu is compact and anchored');
 await library.click();assert.equal(await notices.getAttribute('aria-expanded'),'false');
 assert.equal(await library.getAttribute('aria-expanded'),'true','switching categories replaces the prior menu');
 const entries=nav.locator('#nav-panel-learning');
 for(const key of ['learning','knowledge','documents']){
   assert(await entries.locator('[data-nav-entry="'+key+'"]').isVisible(),key+' is available after one click');
 }
 assert.equal(await entries.locator('[data-nav-entry]').count(),3,'only authorized knowledge entries are listed');
 assert.equal(await entries.locator('button[aria-expanded]').count(),0,'category labels do not require another click');
 await page.keyboard.press('Escape');
 assert(await library.evaluate(el=>el===document.activeElement),'Escape restores the trigger');
 // Existing dialogs use z-index 140. Their controls must stay above the header.
 await page.evaluate(()=>{const d=document.createElement('div');d.id='existing-dialog-probe';d.style.cssText='position:fixed;inset:0;z-index:140;background:white';d.innerHTML='<button style="position:absolute;left:12px;top:12px;width:60px;height:44px">对话框操作</button>';document.body.append(d);});
 assert(await page.evaluate(()=>!!document.elementFromPoint(36,36)?.closest('#existing-dialog-probe')));
 await page.getByRole('button',{name:'对话框操作'}).click();await page.locator('#existing-dialog-probe').evaluate(e=>e.remove());
 await page.getByRole('button',{name:'夜间模式',exact:true}).click();
 await page.setViewportSize({width:402,height:874});
 await nav.waitFor({state:'detached'});await page.locator('.v2-mobile-nav').waitFor();
 assert.equal(await page.locator('html').getAttribute('data-v2-theme'),'dark');
 const phone=await make({viewport:{width:390,height:844},hasTouch:true,isMobile:true});
 await phone.locator('.v2-mobile-nav').getByRole('button',{name:'功能',exact:true}).tap();
 await phone.getByRole('heading',{name:'全部功能'}).waitFor();
 await phone.locator('.v2-functions [data-nav-entry="notices"]').tap();
 await phone.waitForURL(url=>url.searchParams.get('__page')==='/workbench-lite');
 await phone.setViewportSize({width:844,height:390});
 await phone.locator('.v2-mobile-nav').waitFor();assert(!await phone.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
 await phone.screenshot({path:path.join(out,'mobile-landscape.png')});
 const touch=await make({viewport:{width:1440,height:900},hasTouch:true});
 const button=touch.getByRole('navigation',{name:'功能导航',exact:true}).locator('[data-group="notices"]');
 await button.tap();await touch.locator('#nav-panel-notices').waitFor();
 await touch.screenshot({path:path.join(out,'touch-desktop.png')});await button.tap();assert.equal(await button.getAttribute('aria-expanded'),'false');
 assert.deepEqual(errors,[]);console.log('PASS: top navigation categories, focus restoration, dialog stacking, touch rotation and display mode.');
}catch(error){for(let i=0;i<contexts.length;i++){const page=contexts[i].pages()[0];if(page)await page.screenshot({path:path.join(out,`failure-${i}.png`)});}throw error;}
finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
