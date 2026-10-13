import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const script = await readFile(new URL('../public/assets/connection-guard.js', import.meta.url), 'utf8');
const output = new URL('../../../../output/playwright/connection-guard/', import.meta.url);
await mkdir(output, { recursive: true });
const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(req.url.startsWith('/child')
    ? '<input value="Child filling"><script>' + script + '</script>'
    : '<input id="draft" value="Retained filling"><button id="action">Local action</button><script'+(req.url.startsWith('/workspace')?' data-workspace-v2="1"':'')+'>' + script + '</script>');
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  let instance = 'first', fail = false, probes = 0;
  await page.route(base + '/api/health**', route => {
    probes++;
    return fail ? route.abort('failed') : route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: instance } });
  });
  await page.goto(base);
  await page.waitForFunction(() => !!window.ClipFlowConnectionGuard);
  await page.waitForTimeout(100);
  await page.locator('#draft').fill('User edits remain intact');
  fail = true;
  await page.evaluate(() => window.ClipFlowConnectionGuard.check());
  await page.evaluate(() => window.ClipFlowConnectionGuard.check());
  await page.locator('#clipflow-connection-warning').waitFor();
  assert(await page.locator('#action').isVisible(), 'transient loss must not hide the business page');
  assert.equal(await page.locator('#draft').inputValue(), 'User edits remain intact');
  assert.equal(await page.locator('#clipflow-connection-guard').count(), 0);
  await page.screenshot({ path: fileURLToPath(new URL('retained-page.png', output)) });
  fail = false;
  await page.evaluate(() => window.ClipFlowConnectionGuard.check());
  assert.equal(await page.locator('#clipflow-connection-warning').count(), 0, 'same instance recovery is automatic');

  const before = probes;
  await page.evaluate(() => { const frame = document.createElement('iframe'); frame.src = '/child?_assistant_frame=1'; document.body.append(frame); });
  await page.frameLocator('iframe').locator('input').waitFor();
  assert.equal(probes, before, 'embedded notices share the parent probe');
  assert(await page.locator('iframe').evaluate(el => el.contentWindow.ClipFlowConnectionGuard === window.ClipFlowConnectionGuard));
  await page.locator('iframe').evaluate(el => el.contentWindow.dispatchEvent(new Event('clipflow-api-offline')));
  await page.waitForTimeout(100);
  assert.equal(probes, before + 1, 'embedded errors trigger exactly one parent probe');

  instance = 'restarted';
  await page.evaluate(() => window.ClipFlowConnectionGuard.check());
  await page.locator('#clipflow-connection-guard').waitFor();
  assert((await page.locator('#clipflow-connection-guard').innerText()).includes('已重启'));
  assert.equal(await page.locator('#draft').inputValue(), 'User edits remain intact');
  assert.deepEqual(errors, []);
  const workspace=await browser.newPage();
  await workspace.clock.install();
  await workspace.addInitScript(()=>{window.fetch=(_url,options)=>new Promise((resolve,reject)=>{window.probeSignal=options.signal;window.finishProbe=()=>resolve(new Response(JSON.stringify({ok:true,service:'clipflow_backend',instance_id:'slow-fixture'}),{headers:{'content-type':'application/json'}}));options.signal.addEventListener('abort',()=>reject(new DOMException('Aborted','AbortError')));});});
  await workspace.goto(base+'/workspace');await workspace.waitForFunction(()=>!!window.probeSignal);
  await workspace.clock.fastForward(6000);assert.equal(await workspace.evaluate(()=>window.probeSignal.aborted),false,'a busy but running V2 backend is not rejected at five seconds');
  await workspace.evaluate(()=>window.finishProbe());await workspace.waitForTimeout(20);assert.equal(await workspace.locator('#clipflow-connection-warning').count(),0);
  await workspace.evaluate(()=>{void window.ClipFlowConnectionGuard.check();});await workspace.clock.fastForward(15001);await workspace.clock.fastForward(16001);await workspace.locator('#clipflow-connection-warning').waitFor();
  assert.match(await workspace.locator('#clipflow-connection-warning').innerText(),/暂忙或网络暂不可达/);assert.equal(await workspace.locator('#draft').inputValue(),'Retained filling');
  await workspace.close();
  console.log('[ConnectionGuardBrowser] OK: transient loss retains input, recovery removes warning, shared iframe probe, restart still requires refresh.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
