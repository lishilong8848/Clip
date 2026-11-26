import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';
import {workspaceAuditPages,auditUrl} from './responsive-audit-pages.mjs';

// Run against scripts/responsive-audit-server.mjs. Require its fixture identity
// before visiting routes; the browser can only reach this loopback origin.
const origin=new URL(process.argv[2]);
assert.equal(origin.hostname,'127.0.0.1');
const health=await (await fetch(new URL('/api/health',origin))).json();
assert.equal(health.instance_id,'responsive-isolated-preview');
const out=path.resolve(process.argv[3]||'../../../output/playwright/soft-ui');
await mkdir(out,{recursive:true});
const pages=workspaceAuditPages.map(([id,,url])=>[id,url]);
const shots=new Set(['home','functions','notice-maintenance','repairs','profile','handover','guard','cabinet','tool','inspection','cold','eop','documents','zhihang','login','water','work-handover']);
const profiles=[['desktop',1440,900],['mobile',393,852],['mobile-max',430,932],['tablet',820,1180],['dark',1440,900]];
const results=[];
const browser=await chromium.launch();
async function scan([profile,width,height]){
  const context=await browser.newContext({viewport:{width,height},hasTouch:width<=820,reducedMotion:'reduce'});
  // The print routes automatically open the native print dialog. In the
  // isolated browser verify the rendered document without blocking inspection.
  await context.addInitScript(()=>{window.print=()=>{document.documentElement.dataset.auditPrintRequested='true';};});
  if(profile==='dark')await context.addInitScript(()=>{if(window.top!==window)return;for(const id of ['ou_responsive_fixture','fixture-guest','visitor'])localStorage.setItem('clipflow:workspace-night-mode:'+id,'dark');});
  await context.route(url=>url.origin!==origin.origin,route=>route.abort());
  const page=await context.newPage();page.setDefaultTimeout(18000);
  let errors=[],unavailable=new Set();
  page.on('pageerror',e=>errors.push(e.message));
  page.on('response',response=>{
    const {pathname}=new URL(response.url());
    if(response.status()>=400&&pathname.startsWith('/api/'))unavailable.add(response.status()+' '+pathname);
  });
  try{
    for(const [id,logical] of pages){
      errors=[];unavailable=new Set();
      try{
        await page.goto(auditUrl(origin.origin,'v2',logical),{waitUntil:'domcontentloaded',timeout:15000});
        if(id==='morning-print')await page.getByRole('table',{name:'EA118 H楼晨会'}).waitFor();
        if(id==='drill-print')await page.locator('.print-sheet').waitFor();
        await page.waitForFunction(()=>[...document.querySelectorAll('.v2-main > *')].some(el=>
          !el.matches('.v2-breadcrumb,.v2-loading,.v2-alert,.v2-duty-tabs,.guest-session,.v2-workbench-return,.async-page-state') &&
          el.getBoundingClientRect().height>0 && (el.innerText?.trim().length>3 || el.querySelector('iframe,canvas'))));
        await page.waitForTimeout(180);
        const result=await page.evaluate(()=>{
          const main=document.querySelector('.v2-main'),root=document.documentElement;
          const rendered=[...main.children].filter(el=>el.getBoundingClientRect().height>0);
          const box=main.getBoundingClientRect();
          const sparseBars=innerWidth<1100?[]:[...main.querySelectorAll('header,nav,[role=toolbar],[class*="toolbar"],[class*="actions"]')].flatMap(el=>{
            const bounds=el.getBoundingClientRect();
            if(bounds.width<main.clientWidth*.65||bounds.height<16||el.closest('[role=dialog],td,footer'))return [];
            const controls=[...el.querySelectorAll('button,a,input,select,textarea,[role=combobox]')].filter(node=>node.getBoundingClientRect().width);
            if(!controls.length||controls.length>2||controls.some(node=>!node.matches('button,a')))return [];
            const label=el.innerText.replace(/\s/g,''),buttons=controls.map(node=>node.innerText.replace(/\s/g,''));
            return label===buttons.join('') && controls.reduce((sum,node)=>sum+node.getBoundingClientRect().width,0)<bounds.width*.3
              ? [{bar:el.className,buttons}]:[];
          });
          return {root:rendered.map(el=>el.className).join(' '),theme:root.dataset.v2Theme,
            material:getComputedStyle(root).getPropertyValue('--soft-raised').trim(),width:innerWidth,
            docWidth:root.scrollWidth,mainWidth:main.clientWidth,contentWidth:main.scrollWidth,left:Math.round(box.left),
            sparseBars,noAccess:/没有该功能权限|没有找到此页面/.test(main.innerText),rail:!!document.querySelector('.v2-sidebar,.v2-rail')};
        });
        const failures=[...errors];
        if(result.noAccess||result.rail||!result.material||Math.abs(result.left)>1||result.docWidth>width+1||result.contentWidth>result.mainWidth+2)failures.push('route or geometry');
        if(result.sparseBars.length)failures.push('isolated full-width actions: '+result.sparseBars.map(bar=>bar.bar).join(', '));
        if(result.theme!==(profile==='dark'?'dark':'light'))failures.push('wrong appearance');
        const controls=await page.locator('main button:visible,main [role=combobox]:visible,main input:visible,main [role=tab]:visible,[role=dialog] button:visible').evaluateAll(nodes=>
          nodes.map(el=>(el.getAttribute('aria-label')||el.getAttribute('title')||el.innerText||el.getAttribute('placeholder')||el.type||el.tagName).trim()));
        if(id==='links'&&profile==='mobile'){
          await page.getByRole('button',{name:'新增链接',exact:true}).click();
          const dialog=page.getByRole('dialog');
          await dialog.locator('input:visible').first().focus();
          await page.evaluate(()=>{
            Object.defineProperty(visualViewport,'height',{configurable:true,value:470});
            visualViewport.dispatchEvent(new Event('resize'));
          });
          await page.waitForFunction(()=>document.documentElement.hasAttribute('data-v2-keyboard'));
          const box=await dialog.boundingBox();
          if(!box||box.y+box.height>471)failures.push('keyboard covers link editor');
          await page.evaluate(()=>{delete visualViewport.height;visualViewport.dispatchEvent(new Event('resize'));});
          await page.keyboard.press('Escape');
          await dialog.waitFor({state:'hidden'});
        }
        results.push({profile,id,...result,controls,unavailable:[...unavailable],failures});
        if(failures.length||shots.has(id))await page.screenshot({path:path.join(out,profile+'-'+id+'.png'),animations:'disabled'});
      }catch(e){
        const visible=(await page.locator('main').first().innerText({timeout:2000}).catch(()=>'')).slice(0,280);
        results.push({profile,id,visible,unavailable:[...unavailable],failures:[e.message.split('\n')[0],...errors]});
      }
    }
  }finally{await context.close();}
}
try{
  // Keep browser load comparable to an ordinary user opening a page.
  for(let i=0;i<profiles.length;i+=2)await Promise.all(profiles.slice(i,i+2).map(scan));
}finally{await browser.close();}
const failed=results.filter(item=>item.failures.length);
await writeFile(path.join(out,'routes.json'),JSON.stringify(results,null,2));
console.log(results.length+' rendered route checks, '+failed.length+' failures');
for(const row of failed)console.log(row.profile,row.id,row.root,row.mainWidth,row.contentWidth,row.failures);
assert.equal(failed.length,0);
