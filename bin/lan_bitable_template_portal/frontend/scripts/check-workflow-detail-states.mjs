import assert from 'node:assert/strict';
import {mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {chromium} from 'playwright';
import {auditUrl} from './responsive-audit-pages.mjs';
import {detailCases} from './workflow-detail-cases.mjs';
const [origin,output,...selected]=process.argv.slice(2);
assert.equal(new URL(origin).hostname,'127.0.0.1');
assert.equal((await (await fetch(origin+'/api/health')).json()).instance_id,'workflow-detail-isolated');
const browser=await chromium.launch();
let failures=0;
try{
  for(const [group,test] of Object.entries(detailCases)){
    if(selected.length&&!selected.includes(group))continue;
    await Promise.all(['桌面','手机'].map(async device=>{
      const dest=path.resolve(output,group,device);
      await mkdir(dest,{recursive:true});
      const context=await browser.newContext({viewport:device==='手机'?{width:390,height:844}:{width:1440,height:900},isMobile:device==='手机',hasTouch:device==='手机',reducedMotion:'reduce'});
      const page=await context.newPage(),states=[],errors=[];
      page.setDefaultTimeout(9000);
      page.on('pageerror',e=>errors.push(e.message));
      // Replay the entry page's transition cancellation handler, omitted by the development entry.
      const entry=await readFile(new URL('../src/workspace-v2/entry.html',import.meta.url),'utf8');
      await page.addInitScript(entry.match(/<script>([\s\S]*?)<\/script>/)[1]);
      await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
      const button=(name,container=page)=>container.getByRole('button',{name,exact:typeof name==='string'});
      const click=(name,container)=>button(name,container).click();
      async function go(route){
        await page.goto(auditUrl(origin,'v2',route),{waitUntil:'domcontentloaded',timeout:60000});
        await page.locator('#app > *').first().waitFor({state:'attached',timeout:60000});
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
        states.push({label,file,width});await writeFile(path.join(dest,'states.json'),JSON.stringify({states,errors},null,2));
      }
      const ok=(route,data)=>route.fulfill({json:{ok:true,data}});
      const patch=(glob,fn)=>page.route(glob,async route=>{const response=await route.fetch();const body=await response.json();assert(body.ok,glob);await ok(route,fn(body.data,route.request())||body.data);});
      try{await test({p:page,go,shot,click,b:button,patch,ok,device,origin});assert.deepEqual(errors,[]);await rm(path.join(dest,'failure.json'),{force:true});await rm(path.join(dest,'问题现场.png'),{force:true});console.log('PASS',group,device,states.length);}
      catch(e){failures++;const message=e.message;const controls=await page.locator('main button:visible,[role=dialog] button:visible').allTextContents().catch(()=>[]);
        await page.screenshot({path:path.join(dest,'问题现场.png')}).catch(()=>{});
        await writeFile(path.join(dest,'failure.json'),JSON.stringify({message,controls,errors,states},null,2));
        console.error('FAIL',group,device,message.slice(0,390),controls.slice(-15).join('|').slice(-550));
      }finally{await context.close();}
    }));
  }
}finally{await browser.close();}
if(failures)process.exitCode=1;

