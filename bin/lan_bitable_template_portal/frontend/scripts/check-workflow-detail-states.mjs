import assert from 'node:assert/strict';
import {mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';
import {auditUrl} from './responsive-audit-pages.mjs';
import {detailCases} from './workflow-detail-cases.mjs';
const [origin,output,...selected]=process.argv.slice(2);
assert.equal(new URL(origin).origin,origin);
assert.equal(new URL(origin).hostname,'127.0.0.1');
const expected=process.env.WORKFLOW_DETAIL_FIXTURE_ID||'workflow-detail-isolated';
assert(['workflow-detail-isolated','responsive-isolated-preview'].includes(expected),'Unknown fixture identity');
const health=await fetch(origin+'/api/health',{redirect:'error'});
assert(health.ok);
assert.equal((await health.json()).instance_id,expected);
for(const name of selected)assert(Object.hasOwn(detailCases,name),'Unknown case '+name);
const browser=await chromium.launch();
let failures=0;
try{
  for(const [group,test] of Object.entries(detailCases)){
    if(selected.length&&!selected.includes(group))continue;
    await Promise.all(['桌面','手机'].map(async device=>{
      const dest=path.resolve(output,group,device);
      await mkdir(dest,{recursive:true});
      const context=await browser.newContext({viewport:device==='手机'?{width:390,height:844}:{width:1440,height:900},isMobile:device==='手机',hasTouch:device==='手机',reducedMotion:'reduce',serviceWorkers:'block'});
      const page=await context.newPage(),states=[],errors=[],api=[],blocked=[];
      const evidence={group,device,origin,fixture:expected,viewport:page.viewportSize(),startedAt:new Date().toISOString(),states,errors,api,blocked};
      const save=status=>writeFile(path.join(dest,'states.json'),JSON.stringify({...evidence,status},null,2));
      page.on('response',response=>{
        const url=new URL(response.url());
        if(url.origin===origin&&url.pathname.startsWith('/api/'))api.push({method:response.request().method(),path:url.pathname+url.search,status:response.status()});
      });
      page.setDefaultTimeout(9000);
      page.on('pageerror',e=>errors.push(e.message));
      // Replay the entry page's transition cancellation handler, omitted by the development entry.
      const entry=await readFile(new URL('../src/workspace-v2/entry.html',import.meta.url),'utf8');
      await page.addInitScript(entry.match(/<script>([\s\S]*?)<\/script>/)[1]);
      await context.route('**/*',route=>{
        const url=new URL(route.request().url());
        if(url.origin===origin)return route.continue();
        blocked.push(url.origin);return route.abort();
      });
      const button=(name,container=page)=>container.getByRole('button',{name,exact:typeof name==='string'});
      const click=(name,container)=>button(name,container).click();
      async function go(route,ready='#app > :not(.entry-loading)'){
        await page.goto(auditUrl(origin,'v2',route),{waitUntil:'domcontentloaded',timeout:60000});
        await page.locator(ready).first().waitFor({state:'attached',timeout:60000});
      }
      async function shot(label,focus){
        await focus.first().waitFor();
        const target=focus.first();
        await target.evaluate(el=>{const r=el.getBoundingClientRect();if(r.y<0||r.y>innerHeight*.74)el.scrollIntoView({block:'start',behavior:'instant'});});
        await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
        const file=String(states.length+1).padStart(2,'0')+'-'+label+'.png';
        const width=await page.evaluate(()=>document.documentElement.scrollWidth);
        assert(width<=page.viewportSize().width+1,label+' 横向溢出 '+width);
        await page.screenshot({path:path.join(dest,file),animations:'disabled'});
        const focusText=await target.evaluate(el=>(el.innerText||el.value||el.getAttribute('aria-label')||'').trim().slice(0,600));
        states.push({label,file,width,focusText,url:page.url()});await save('running');
      }
      const ok=(route,data)=>route.fulfill({json:{ok:true,data}});
      const patch=(glob,fn)=>page.route(origin+'/'+glob.replace(/^\*\*\//,''),async route=>{const response=await route.fetch({maxRedirects:0});const body=await response.json();assert(body.ok,glob);await ok(route,fn(body.data,route.request())||body.data);});
      try{await test({p:page,go,shot,click,b:button,patch,ok,device,origin});assert.deepEqual(errors,[]);await save('passed');await rm(path.join(dest,'failure.json'),{force:true});await rm(path.join(dest,'问题现场.png'),{force:true});console.log('PASS',group,device,states.length);}
      catch(e){failures++;const message=e.message;const controls=await page.locator('main button:visible,[role=dialog] button:visible').allTextContents().catch(()=>[]);
        await page.screenshot({path:path.join(dest,'问题现场.png')}).catch(()=>{});
        await save('failed');
        await writeFile(path.join(dest,'failure.json'),JSON.stringify({message,controls,errors,states},null,2));
        console.error('FAIL',group,device,message.slice(0,390),controls.slice(-15).join('|').slice(-550));
      }finally{await context.close();}
    }));
  }
}finally{await browser.close();}
if(failures)process.exitCode=1;

