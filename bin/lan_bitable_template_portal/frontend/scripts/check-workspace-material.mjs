import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';
import {auditPages,auditUrl} from './responsive-audit-pages.mjs';

// Run against scripts/responsive-audit-server.mjs. Require its fixture identity
// before visiting routes; the browser can only reach this loopback origin.
const origin=new URL(process.argv[2]);
assert.equal(origin.hostname,'127.0.0.1');
const health=await (await fetch(new URL('/api/health',origin))).json();
assert.equal(health.instance_id,'responsive-isolated-preview');
const out=path.resolve('../../../output/playwright/soft-ui');
await mkdir(out,{recursive:true});
const pages=[...auditPages.map(([id,,url])=>[id,url]),
  ['functions','/?view=functions'],['tasks','/?view=tasks'],['access','/?view=permission'],['guest','/cabinet-power?__audit=guest'],
  ['tool','/risk-inspections?view=tool-self-check&scope=A'],['inspection','/daily-inspection?scope=D'],
  ['cold','/cold-source-monitor?scope=D'],['eop','/eop-scenarios?scope=D'],['documents','/document-library'],
  ['zhihang','/zhihang-data'],['signature','/signature?record_id=test-record'],['work-order','/polling-work-order?token=test-token'],
  ...['status','development','handover','mop','feishu','dingtalk','pressure'].map(tab=>['settings-'+tab,'/admin/settings?tab='+tab])];
const shots=new Set(['home','functions','notice-maintenance','repairs','profile','handover','guard','cabinet','tool','inspection','cold','eop','documents','zhihang','login','water','work-handover']);
const profiles=[['desktop',1440,900],['mobile',375,812],['tablet',820,1180],['dark',1440,900]];
const results=[];
const browser=await chromium.launch();
async function scan([profile,width,height]){
  const context=await browser.newContext({viewport:{width,height},hasTouch:['mobile','tablet'].includes(profile),reducedMotion:'reduce'});
  if(profile==='dark')await context.addInitScript(()=>{if(window.top!==window)return;localStorage.setItem('clipflow:workspace-night-mode:ou_responsive_fixture','dark');localStorage.setItem('clipflow:workspace-night-mode:visitor','dark');});
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url());
    if(url.origin!==origin.origin)return route.abort();
    if(url.pathname==='/api/auth/status'){
      const response=await route.fetch(),body=await response.json();
      if(body.data?.user?.role==='admin')body.data.user.tool_management_enabled=true;
      return route.fulfill({response,json:body});
    }
    return route.continue();
  });
  const page=await context.newPage();page.setDefaultTimeout(5500);
  let errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  try{
    for(const [id,logical] of pages){
      errors=[];
      try{
        await page.goto(auditUrl(origin.origin,'v2',logical),{waitUntil:'domcontentloaded',timeout:15000});
        await page.waitForFunction(()=>[...document.querySelectorAll('.v2-main > *')].some(el=>
          !el.matches('.v2-breadcrumb,.v2-loading,.v2-alert,.v2-duty-tabs,.v2-workbench-return,.async-page-state') &&
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
        results.push({profile,id,...result,failures});
        if(failures.length||shots.has(id))await page.screenshot({path:path.join(out,profile+'-'+id+'.png'),animations:'disabled'});
      }catch(e){results.push({profile,id,failures:[e.message.split('\n')[0],...errors]});}
    }
  }finally{await context.close();}
}
try{await Promise.all(profiles.map(scan));}finally{await browser.close();}
const failed=results.filter(item=>item.failures.length);
await writeFile(path.join(out,'routes.json'),JSON.stringify(results,null,2));
console.log(results.length+' rendered route checks, '+failed.length+' failures');
for(const row of failed)console.log(row.profile,row.id,row.root,row.mainWidth,row.contentWidth,row.failures);
assert.equal(failed.length,0);
