import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';
import {collabFixture} from './responsive-audit-collab-fixtures.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const entry='/assets/workspace-v2/index.html';
try {
  for(const reducedMotion of ['no-preference','reduce']) {
    const context=await browser.newContext({viewport:{width:402,height:874},reducedMotion});
    const errors=[],calls=[];
    let holdManifest=false,releaseManifest;
    await context.route('**/*',async route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin!==origin)return route.abort();
      if(url.pathname===entry.replace('index.html','entry.json')&&holdManifest) {
        await new Promise(resolve=>{releaseManifest=resolve;});
        return route.continue();
      }
      if(!url.pathname.startsWith('/api/'))return route.continue();
      calls.push(url.pathname);
      assert.equal(request.method(),'GET','navigation does not write business data');
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend'}});
      if(url.pathname==='/api/auth/status')return route.fulfill({json:{ok:true,data:{logged_in:true,
        user:{open_id:'ou_navigation_fixture',name:'导航验收',role:'building',level:'L4',category:'engineer',modules:['maintenance'],home_scope:'A'},
        scope_options:[{value:'A',label:'A楼'}]}}});
      if(url.pathname==='/api/handover-links')return route.fulfill({status:403,json:{ok:false,error_code:'module_access_denied'}});
      for(const fixture of [coreFixture,collabFixture]) {
        const result=await fixture({url,method:request.method(),body:{}});
        if(result)return route.fulfill({status:result.status||200,...(result.json?{json:result.json}:{contentType:result.contentType,body:result.body})});
      }
      return route.fulfill({json:{ok:true,data:{items:[],records:[],scopes:{},links:{}}}});
    });
    const page=await context.newPage();
    page.on('pageerror',error=>{errors.push(error.message);console.error('Navigation error:',page.url(),error.name,error.message);});
    const policy=()=>page.evaluate(()=>Array.from(document.styleSheets).flatMap(sheet=>Array.from(sheet.cssRules))
      .filter(rule=>rule.cssText.startsWith('@view-transition')).map(rule=>rule.cssText));
    const checkPolicy=async()=>{const rules=await policy();assert(rules.length);assert(rules.every(rule=>/navigation:\s*auto/.test(rule)),JSON.stringify(rules));};
    const rejectedTransition=async()=>{
      await page.evaluate(()=>{
        for(const type of ['pageswap','pagereveal']) {
          const event=new Event(type);
          Object.defineProperty(event,'viewTransition',{value:Object.fromEntries(['ready','finished','updateCallbackDone']
            .map(state=>[state,Promise.reject(new DOMException('Transition was aborted because of invalid state. ViewTransition opt-in disabled','InvalidStateError'))]))});
          dispatchEvent(event);
        }
      });
      await page.waitForTimeout(50);
    };
    await page.goto(origin+'/?frontend=legacy');
    await page.locator('.home-shell').waitFor();
    await checkPolicy();await rejectedTransition();
    holdManifest=true;
    await page.getByRole('button',{name:'切换新版',exact:true}).click();
    await page.waitForURL('**/assets/workspace-v2/index.html*',{waitUntil:'commit'});
    await page.locator('#entry-status').waitFor();
    await checkPolicy();
    await rejectedTransition();
    assert.equal(typeof releaseManifest,'function');
    holdManifest=false;releaseManifest();
    await page.locator('.v2-shell').waitFor();
    await checkPolicy();await rejectedTransition();
    for(let round=0;round<2;round++) {
      await page.getByRole('button',{name:'切换旧版',exact:true}).click();
      await page.locator('.home-shell').waitFor();
      await checkPolicy();
      const legacyRejections=await page.evaluate(()=>['AbortError','InvalidStateError'].map(name=>{
        const event=new PromiseRejectionEvent('unhandledrejection',{cancelable:true,promise:Promise.resolve(),
          reason:new DOMException(name==='AbortError'?'Transition was skipped':'Transition was aborted because of invalid state. ViewTransition opt-in disabled',name)});
        dispatchEvent(event);return event.defaultPrevented;
      }));
      assert.deepEqual(legacyRejections,[true,false],'legacy handles only the exact browser cancellation, not application errors');
      await page.getByRole('button',{name:'切换新版',exact:true}).click();
      await page.locator('.v2-shell').waitFor();
      await checkPolicy();
    }
    for(const url of ['/?frontend=legacy&entry=pre_shift',entry+'?__page=%2F&pre_shift=1']) {
      await page.goto(origin+url);
      await page.locator(url.startsWith(entry)?'.v2-shell':'.app-shell').waitFor();
      await page.waitForTimeout(200);
    }
    assert.equal(calls.filter(path=>path==='/api/handover-links').length,0,'home/pre-shift never request an ungranted handover module');
    assert.deepEqual(errors,[],'cancelled transition promises do not become page errors');
    const unhandled=await page.evaluate(()=>{
      const event=new PromiseRejectionEvent('unhandledrejection',{cancelable:true,promise:Promise.resolve(),
        reason:new DOMException('Transition was aborted because of invalid state. ViewTransition opt-in disabled','InvalidStateError')});
      dispatchEvent(event);return event.defaultPrevented;
    });
    assert.equal(unhandled,false,'unrelated rejections are not suppressed globally');
    await context.close();
  }
  console.log('PASS: legacy/v2 native switching, delayed entry CSS, cancelled transition promises, unrelated errors, reduced motion and no forbidden home/pre-shift reads.');
} finally {await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
