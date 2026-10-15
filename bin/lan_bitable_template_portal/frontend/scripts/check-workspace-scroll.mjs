import assert from 'node:assert/strict';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {execFileSync} from 'node:child_process';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const baseline=process.argv.includes('--baseline');
const site=process.env.CLIPFLOW_SCROLL_SITE||path.resolve(root,baseline?'dist':'../../../output/workspace-scroll-preview');
const output=path.resolve(root,'../../../output/playwright/workspace-scroll');
const distPath='bin/lan_bitable_template_portal/frontend/dist/assets/workspace-v2/';
const oldManifest=baseline?JSON.parse(execFileSync('git',['show','HEAD:'+distPath+'entry.json'],{cwd:root,encoding:'utf8'})):null;
const oldCss=baseline?oldManifest.css.map(file=>execFileSync('git',['show','HEAD:'+distPath+file],{cwd:root,encoding:'utf8',maxBuffer:10*1024*1024})).join('\n'):'';
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:site},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:900},reducedMotion:'reduce'});
const errors=[],measurements=[];
const blocks=Array.from({length:24},(_,i)=>({blockId:String(i+1),blockName:'A楼设备计划核对 '+(i+1),status:'1',startTime:'2026-10-15 08:00'}));
await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.origin!==origin)return route.abort();
  if(baseline&&/^\/assets\/workspace-v2\/res-[\w-]+\.css$/.test(url.pathname))return route.fulfill({contentType:'text/css',body:oldCss});
  if(url.pathname==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
  if(!url.pathname.startsWith('/api/'))return route.continue();
  assert.equal(request.method(),'GET','layout verification must never submit business data');
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'scroll-fixture',name:'滚动验收',role:'admin',level:'L1',category:'manager',modules:['plan_convergence','learning','accounts','knowledge','drills','critical_guard','cabinet','engineer_mop','water']},scope_options:['A','B'].map(value=>({value,label:value+'楼'}))});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'scroll-fixture'}});
  if(url.pathname==='/api/plan-convergence/bootstrap')return ok({is_admin:true,catalog_ready:true,blocks:{items:blocks}});
  if(url.pathname==='/api/plan-convergence/rulesets')return ok([]);
  if(/^\/api\/plan-convergence\/blocks\/\d+$/.test(url.pathname))return ok({...blocks[0],...Object.fromEntries(Array.from({length:40},(_,i)=>['核对补充字段'+i,'完整的设备说明 '+i])),alarmBlockDetailResultList:Array.from({length:25},(_,i)=>({blockDetailId:'detail'+i,classifyModel:'电气设备',spaceModel:'A楼第'+(i+1)+'机房',relateConfig:'设备运行状态告警',instances:'设备A-'+i}))});
  return ok({items:[],records:[],settings:{},stats:{}});
});
const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
const entry=origin+'/assets/workspace-v2/index.html?__page=/plan-convergence';
const box=selector=>page.locator(selector).boundingBox();
async function wheel(selector,delta){const r=await box(selector);assert(r);await page.mouse.move(r.x+Math.min(r.width/2,180),r.y+Math.min(r.height/2,150));await page.mouse.wheel(0,delta);await page.waitForTimeout(200);}
async function scrollers(selector){return page.locator(selector).evaluate(root=>[root,...root.querySelectorAll('*')].filter(el=>el.clientHeight>0&&el.scrollHeight>el.clientHeight+2&&/auto|scroll/.test(getComputedStyle(el).overflowY)).map(el=>({class:el.className,height:el.clientHeight,scrollHeight:el.scrollHeight})));}
try{
  await page.goto(entry);await page.locator('.pc-record').first().waitFor();
  await page.locator('.pc-record').first().click();await page.getByRole('button',{name:'查看记录全部信息'}).waitFor();
  await page.screenshot({path:path.join(output,(baseline?'before':'after')+'-desktop.png')});
  const main=page.locator('.v2-main');
  await wheel('.pc-main .pc-table-wrap',600);
  assert(await main.evaluate(el=>el.scrollTop>100),'wheel over the detail table scrolls the work area');
  assert.equal(await page.evaluate(()=>window.scrollY),0,'desktop header does not move');
  await page.getByRole('button',{name:'查看记录全部信息'}).click();
  await page.locator('.pc-dialog-body').waitFor();
  const original=await main.evaluate(el=>el.scrollTop);
  await page.locator('.pc-dialog-body').evaluate(el=>el.scrollTop=el.scrollHeight);
  await wheel('.pc-dialog-body',700);
  measurements.push({test:'modal-background',before:original,after:await main.evaluate(el=>el.scrollTop)});
  if(!baseline)assert.equal(await main.evaluate(el=>el.scrollTop),original,'modal at bottom must not move the work area behind it');
  await page.screenshot({path:path.join(output,(baseline?'before':'after')+'-plan-dialog.png')});
  await page.keyboard.press('Escape');await page.locator('.pc-dialog').waitFor({state:'detached'});
  assert.equal(await main.evaluate(el=>getComputedStyle(el).overflowY),'auto','closing the dialog restores main scrolling');
  const table=page.locator('.pc-main>.pc-table-wrap');
  await table.locator('table').evaluate(el=>el.style.minWidth='1800px');
  await table.scrollIntoViewIfNeeded();
  const tableBox=await table.boundingBox(),beforeHorizontal=await main.evaluate(el=>el.scrollTop);
  await page.mouse.move(tableBox.x+100,Math.max(100,tableBox.y+25));await page.mouse.wheel(500,0);await page.waitForTimeout(200);
  assert(await table.evaluate(el=>el.scrollLeft>0),'wide detail table retains horizontal scrolling');
  assert.equal(await main.evaluate(el=>el.scrollTop),beforeHorizontal,'horizontal scrolling does not move the page vertically');
  await table.locator('table').evaluate(el=>el.style.minWidth='');

  // Stress the actual V2 shared table/modal CSS with a bounded, read-only fixture.
  await page.evaluate(()=>{
    const main=document.querySelector('.v2-main');
    const fixture=document.createElement('section');fixture.id='scroll-contract';
    fixture.innerHTML='<h2>共享滚动布局验收</h2><div class="sharing-table"><table><tbody>'+Array.from({length:40},(_,i)=>'<tr><td style="height:32px">已分页的人员明细 '+i+'</td></tr>').join('')+'</tbody></table></div>';
    main.prepend(fixture);main.scrollTop=0;
  });
  const paged=await scrollers('#scroll-contract');measurements.push({test:'paged-list',scrollers:paged});
  if(!baseline)assert.equal(paged.length,0,'paged table has no additional vertical scroll region');
  await page.evaluate(()=>{
    const holder=document.createElement('div');holder.id='modal-contract';
    holder.innerHTML='<div class="n-modal n-card v2-widget-library" style="position:fixed;left:30%;top:120px;width:600px;height:400px;z-index:9999;display:flex;flex-direction:column;background:white"><div class="n-card-header" style="flex:none;padding:12px">滚动布局验收</div><div class="n-card-content" style="flex:1;min-height:0"><div class="sharing-table"><table><tbody>'+Array.from({length:60},(_,i)=>'<tr><td style="height:32px">列表内容 '+i+'</td></tr>').join('')+'</tbody></table></div></div><div class="n-card__footer" style="padding:12px;flex:none">操作区域</div></div>';
    document.body.append(holder);document.body.style.overflow='hidden';
  });
  const modal=await scrollers('#modal-contract');measurements.push({test:'modal-layers',scrollers:modal});
  if(!baseline){assert.equal(modal.length,1,'modal has only one vertical scroll region');assert.equal(modal[0].class,'n-card-content');}
  const modalBody=page.locator('#modal-contract .n-card-content');
  await modalBody.evaluate(el=>{el.tabIndex=0;el.scrollTop=0;});await modalBody.focus();await page.keyboard.press('PageDown');await page.waitForTimeout(250);
  if(!baseline)assert(await modalBody.evaluate(el=>el.scrollTop>0),'modal body remains keyboard-scrollable');
  await page.locator('#modal-contract .n-card-content').evaluate(el=>el.scrollTop=el.scrollHeight);
  await page.screenshot({path:path.join(output,(baseline?'before':'after')+'-modal-contract.png')});
  await page.evaluate(()=>{document.getElementById('modal-contract').remove();document.body.style.overflow='';document.getElementById('scroll-contract').remove();});

  await page.evaluate(()=>{
    const main=document.querySelector('.v2-main'),fixture=document.createElement('section');fixture.id='handoff-contract';
    fixture.innerHTML='<h2>内层表单滚动验收</h2><div class="water-editor-body" style="height:180px"><div style="height:1000px">表单内容</div></div><div style="height:1000px">工作区后续内容</div>';
    const inner=fixture.querySelector('.water-editor-body');
    function scopes(rules){for(const rule of rules){if(rule.cssRules)scopes(rule.cssRules);for(const m of (rule.selectorText||'').matchAll(/\.water-editor-body\[(data-v-[\w-]+)\]/g))inner.setAttribute(m[1],'');}}
    for(const sheet of document.styleSheets)scopes(sheet.cssRules);
    main.prepend(fixture);main.scrollTop=0;inner.scrollTop=inner.scrollHeight;
  });
  await wheel('#handoff-contract .water-editor-body',500);
  measurements.push({test:'nested-edge-handoff',scrollTop:await main.evaluate(el=>el.scrollTop),behavior:await page.locator('#handoff-contract .water-editor-body').evaluate(el=>getComputedStyle(el).overscrollBehaviorY)});
  if(!baseline)assert(await main.evaluate(el=>el.scrollTop>100),'nested form yields to main at its boundary');
  await page.evaluate(()=>document.getElementById('handoff-contract').remove());

  for(const width of [390,768,1366,1920]){
    await page.setViewportSize({width,height:width<761?844:900});
    await page.goto(entry);await page.locator('.pc-record').first().waitFor();
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'no page-wide horizontal scrollbar');
    if(width<761){
      await wheel('.pc-records',1000);
      assert(await page.locator('.pc-records').evaluate(el=>el.scrollTop>0),'mobile selector keeps useful bounded scrolling');
      await page.locator('.pc-records').evaluate(el=>el.scrollTop=el.scrollHeight);
      await wheel('.pc-records',600);
      assert(await page.evaluate(()=>window.scrollY>0),'mobile list hands wheel to page at its boundary');
    }else{
      const top=await main.evaluate(el=>el.scrollTop);
      await wheel('.v2-sidebar',600);
      assert.equal(await main.evaluate(el=>el.scrollTop),top,'left navigation remains independent');
    }
    await page.screenshot({path:path.join(output,(baseline?'before':'after')+'-'+width+'.png')});
  }
  assert.deepEqual(errors,[]);
  await writeFile(path.join(output,(baseline?'before':'after')+'.json'),JSON.stringify(measurements,null,2));
  console.log(JSON.stringify(measurements));
  console.log(baseline?'BASELINE: recorded existing scroll layers and edge behavior; no business writes.':'PASS: isolated V2 work area, table wheel handoff, one modal scroller, background lock, sidebar independence and responsive geometry. No business writes.');
}catch(error){await page.screenshot({path:path.join(output,'failure.png')});console.error(errors);throw error;}
finally{await browser.close();await server.close();}
