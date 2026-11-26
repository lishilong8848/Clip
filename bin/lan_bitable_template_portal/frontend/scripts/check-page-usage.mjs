import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { chromium } from 'playwright';
import ts from 'typescript';

const source = name => readFile(new URL(name,import.meta.url),'utf8');
const catalog = JSON.parse(await source('../../usage_pages.json'));
const scripts = new Map();
for (const name of ['usageHtml','usageRoutes','usageEvents']) {
  const {outputText} = ts.transpileModule(await source('../src/'+name+'.ts'), {
    compilerOptions:{target:ts.ScriptTarget.ES2020,module:ts.ModuleKind.ESNext},
  });
  scripts.set('/assets/'+name,outputText);
}
const browser = await chromium.launch();
try {
  const context = await browser.newContext();
  await context.addInitScript(() => {
    if (location.pathname === '/idb') Storage.prototype.setItem = () => { throw new Error('disabled'); };
  });
  const sent = [];
  let access = 'member', module = 'learning', pageCode = 'review', ack = 'offline';
  let held, heldRequest;
  await context.route('**/*', async route => {
    const request = route.request(), u = new URL(request.url());
    assert.equal(u.origin,'http://usage.fixture');
    if (u.pathname === '/api/usage/events') {
      const {events} = request.postDataJSON();
      assert.equal(request.method(),'POST');
      assert.deepEqual(Object.keys(request.postDataJSON()),['events']);
      assert.ok(events.length > 0 && events.length <= 50);
      assert.equal(request.headers().referer,'http://usage.fixture/');
      for (const e of events) {
        assert.deepEqual(Object.keys(e).sort(),['client','event_id','module','occurred_at','page']);
        assert.ok(catalog[e.module]?.pages[e.page]);
        assert.match(e.event_id,/^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/);
        assert.ok(Number.isSafeInteger(e.occurred_at));
      }
      sent.push(events);
      if (ack === 'hold' && !held) {
        await new Promise(resolve => { held = resolve; heldRequest?.(); });
      }
      if (ack === 'offline') return route.abort();
      if (ack === 'invalid') return route.fulfill({contentType:'text/html',body:'<html>login</html>'});
      return route.fulfill({json:{ok:ack !== 'failed',data:{accepted:events.length - (ack === 'partial' ? 1 : 0)}}});
    }
    if (u.pathname === '/usage_pages.json') return route.fulfill({contentType:'application/javascript',body:'export default '+JSON.stringify(catalog)});
    if (scripts.has(u.pathname)) return route.fulfill({contentType:'application/javascript',body:scripts.get(u.pathname)});
    return route.fulfill({contentType:'text/html',body:`<body data-usage-access="${access}" data-usage-module="${module}" data-usage-page="${pageCode}">
      <title>SECRET-TITLE</title><input value="SECRET-NAME"><script type="module" src="/assets/usageHtml"></script></body>`});
  });
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror',e=>errors.push(e.message));
  const go = (path = '/') => page.goto('http://usage.fixture'+path+'?token=SECRET-QUERY#SECRET-FRAGMENT');
  const signal = (p,code,view,tier) => p.evaluate(([m,v,t]) => {
    Object.assign(document.body.dataset,{usageModule:m,usagePage:v,usageAccess:t});
    document.dispatchEvent(new Event('portal-usage-page'));
  },[code,view,tier]);
  const pending = p => p.evaluate(() => Object.keys(localStorage).filter(k=>k.startsWith('clipflow:usage:v1:')).map(k=>JSON.parse(localStorage[k])));
  async function until(fn) {
    for(let i=0;i<200;i++){ if(await fn()) return; await new Promise(r=>setTimeout(r,25)); }
    throw new Error('Timed out: '+fn);
  }
  await go();
  await until(()=>sent.length);
  const [original] = await pending(page);
  assert.equal(original.page,'review');
  for(const reply of ['partial','failed','invalid']) {
    const n = sent.length;
    ack = reply;
    await page.evaluate(()=>window.dispatchEvent(new Event('online')));
    await until(()=>sent.length>n);
    assert.deepEqual(await pending(page),[original]);
  }

  access='public';module='auth';pageCode='password';ack='good';
  const firstAnonymous=sent.length;
  await go('/login');
  await until(async()=> (await pending(page)).length===1 && sent.length>firstAnonymous);
  assert.deepEqual(await pending(page),[original]);
  assert.ok(sent.slice(firstAnonymous).flat().every(e=>e.module==='auth'));
  await signal(page,'cabinet','racks','guest');
  await until(async()=> (await pending(page)).length===1 && sent.at(-1)[0].page==='racks');
  assert.deepEqual(await pending(page),[original]);
  await signal(page,'auth','permission','member');
  await until(async()=>!(await pending(page)).length);
  assert.deepEqual(sent.flat().findLast(e=>e.event_id===original.event_id),original);

  const n=sent.length;
  await signal(page,'auth','permission','member'); // repaint
  await signal(page,'__proto__','constructor','member');
  await signal(page,'learning','unknown/SECRET','member');
  await page.waitForTimeout(120);
  assert.equal(sent.length,n);
  ack='offline';
  await page.evaluate(()=>{
    for(let i=0;i<107;i++){
      Object.assign(document.body.dataset,{usageModule:'learning',usagePage:i%2?'today':'history'});
      document.dispatchEvent(new Event('portal-usage-page'));
    }
  });
  await until(()=>sent.length>n);
  assert.equal((await pending(page)).length,107);
  ack='good';
  const drainStart=sent.length;
  await page.evaluate(()=>window.dispatchEvent(new Event('online')));
  await until(async()=>!(await pending(page)).length);
  assert.deepEqual(sent.slice(drainStart).map(batch=>batch.length),[50,50,7]);

  // Concurrent collection while another tab's ACK is outstanding.
  const second=await context.newPage();
  module='';pageCode='';access='member';
  await second.goto('http://usage.fixture/');
  ack='hold';
  const waiting=new Promise(resolve=>{heldRequest=resolve;});
  await signal(page,'learning','review','member');
  await waiting;
  await signal(second,'documents','main','member');
  ack='good';held();
  await until(async()=>!(await pending(second)).length);
  assert.ok(sent.flat().some(e=>e.module==='documents'));
  await second.close();

  // IndexedDB fallback survives closing the collecting tab; verify original timestamp and UUID.
  ack='offline';access='public';module='work_order';pageCode='steps';
  const index=sent.length;
  await go('/idb');
  await until(()=>sent.slice(index).flat().some(e=>e.module==='work_order'));
  const idbEvent=sent.slice(index).flat().find(e=>e.module==='work_order');
  await page.close();
  const restored=await context.newPage();
  module='';pageCode='';ack='good';
  const next=sent.length;
  await restored.goto('http://usage.fixture/idb');
  await until(()=>sent.length>next);
  assert.deepEqual(sent[next],[idbEvent]);
  assert.deepEqual(errors,[]);
  const routes = await restored.evaluate(async()=>{
    const {usageRoute}=await import('/assets/usageRoutes');
    return [['/learning','q=SECRET'],['/unknown','view=home'],['/','view=SECRET'],['/admin/settings','tab=constructor'],
      ['/cabinet-power/batches','batch_id=SECRET'],['/risk-inspections','view=tool-self-check']].map(([p,q])=>usageRoute(p,new URLSearchParams(q)));
  });
  assert.deepEqual(routes,['learning','','','','cabinet/batch_detail','tool_management']);
  console.log('PASS receipts, ACK, 50-item batches, privacy, concurrent tabs, offline reopen, public/guest/private isolation, unknown routes');
} finally { await browser.close(); }
