// Synthetic HTTP/SSE only; --real-page builds the full workspace under output, never dist.
import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { build, createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { adaptBrowserReferences } from './workspace-build-adapter.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/assistant-concurrent-stream');
const realBuild = path.join(output, 'workspace-build');
const turns = [], runs = new Map(), requests = [], errors = [];
const chinese = '\u8fd9\u662f\u6b63\u5728\u5b9e\u65f6\u751f\u6210\u7684\u4e2d\u6587\u56de\u7b54\uff0c\u65b0\u5185\u5bb9\u9010\u8bcd\u6de1\u5165\uff0c\u4e0d\u91cd\u64ad\u6574\u6bb5\u6587\u5b57\u3002';
let revision = 0, runNumber = 0;
function emit(run, event) {
  run.events.push(event);
  for (const response of run.streams) response.write('data: ' + JSON.stringify(event) + '\n\n');
}
function delta(run, text) {
  run.firstDeltaAt ||= Date.now();
  run.answer += text;
  emit(run, { type: 'text-delta', id: 'text-' + run.id, delta: text });
}
function finish(run, status = 'completed', answer = run.answer) {
  run.finishedAt = Date.now();
  run.status = status;
  Object.assign(run.turn, { status, answer }); revision++;
  emit(run, { type: 'text-end', id: 'text-' + run.id });
  emit(run, { type: 'data-turn', data: { ...run.turn }, transient: true });
  emit(run, { type: 'finish' });
  for (const response of run.streams) response.end('data: [DONE]\n\n');
  run.streams.clear();
}
function start(lane, body, previous) {
  const active = [...runs.values()].find(run => run.lane === lane && run.status === 'pending');
  if (active) finish(active, 'superseded');
  const id = 'fixture-run-' + ++runNumber;
  const turn = previous || { operation_id: body.operation_id, question: body.question, at: Date.now() / 1000 };
  Object.assign(turn, { run_id: id, execution_lane: lane, status: 'pending', answer: '', attempt_id: body.attempt_id, process: [] });
  if (!previous) turns.push(turn);
  const run = { id, lane, turn, status: 'pending', answer: '', events: [], streams: new Set() }; runs.set(id, run); revision++;
  emit(run, { type: 'start', messageId: 'reply-' + id, messageMetadata: { run_id: id, operation_id: turn.operation_id } });
  emit(run, { type: 'text-start', id: 'text-' + id });
  return { conversation_id: 'shared-account-history', run_id: id, turn: { ...turn } };
}
function snapshot(lane) {
  const active = [...runs.values()].find(run => run.lane === lane && run.status === 'pending');
  return { conversation_id: 'shared-account-history', turns: structuredClone(turns), enabled: true, configured: true,
    model_name: 'Offline fixture', model_options: [], can_manage_settings: false, query_scopes: ['E'],
    busy: !!active, active_run_id: active?.id || '', sync_stamp: String(revision) };
}
const html = '<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Offline assistant stream</title></head><body><div id="app"></div><script type="module" src="/__assistant-concurrent.ts"></script></body></html>';
const moduleSource = `
import {createApp,h} from '/node_modules/.vite/deps/vue.js';
import Assistant from '/src/components/LighthouseAssistant.vue';
import {requestJson,assistantClientId,requestBinaryJson,uploadJson,requestFile} from '/src/api/client.ts';
import '/src/workspace-v2/workspace.css';
import '/src/workspace-v2/motionBlocks.css';
document.documentElement.classList.add('workspace-v2');
const app=createApp({render:()=>h(Assistant,{userId:'shared-fixture-person',userName:'Offline fixture'})});
app.mount('#app');
window.fixture={vm:app._instance.subTree.component.setupState,requestJson,assistantClientId,requestBinaryJson,uploadJson,requestFile};
if(new URLSearchParams(location.search).has('preview')){
  await window.fixture.vm.openAssistant();window.fixture.vm.draft='preview';await window.fixture.vm.send();
}
`;
const server = await createServer({ root, configFile: false, publicDir: false, logLevel: 'error',
  cacheDir: path.join(output, 'vite-cache'), server: { host: '127.0.0.1', port: 0 },
  plugins: [{ name: 'isolated-concurrent-assistant', enforce: 'pre',
    resolveId(source, importer) {
      if (source.endsWith('/LighthouseReply.vue') && importer?.replaceAll('\\', '/').endsWith('/components/LighthouseAssistant.vue')) return path.join(root, 'src/workspace-v2/AnimatedReply.vue');
      if (source === '/__assistant-concurrent.ts') return source;
    },
    load(id) { if (id === '/__assistant-concurrent.ts') return moduleSource.replace("'/node_modules/.vite/deps/vue.js'", "'vue'"); },
    configureServer(dev) {
      dev.middlewares.use(async (request, response, next) => {
        const url = new URL(request.url, 'http://fixture.invalid');
        if (url.pathname === '/__assistant-concurrent') { response.setHeader('Content-Type', 'text/html'); response.end(await dev.transformIndexHtml(url.pathname, html)); return; }
        if (url.pathname.startsWith('/assets/workspace-v2/') && /^[\w.-]+$/.test(url.pathname.slice('/assets/workspace-v2/'.length))) {
          const name = url.pathname.slice('/assets/workspace-v2/'.length);
          try { response.setHeader('Content-Type', name.endsWith('.js') ? 'application/javascript' : name.endsWith('.css') ? 'text/css' : name.endsWith('.json') ? 'application/json' : 'text/html'); response.end(await readFile(path.join(realBuild, name))); }
          catch { response.statusCode = 404; response.end(); } return;
        }
        if (url.pathname === '/assets/connection-guard.js') { response.setHeader('Content-Type', 'application/javascript'); response.end(''); return; }
        if (/^\/assets\/[\w.-]+\.(png|svg|webp|jpg)$/.test(url.pathname)) {
          try { response.setHeader('Content-Type', url.pathname.endsWith('.svg') ? 'image/svg+xml' : url.pathname.endsWith('.png') ? 'image/png' : 'image/webp'); response.end(await readFile(path.join(root, 'public', url.pathname))); }
          catch { response.statusCode = 404; response.end(); } return;
        }
        if (!url.pathname.startsWith('/api/')) return next();
        const lane = request.headers['x-lighthouse-client'];
        requests.push({ path: url.pathname, method: request.method, lane, type: request.headers['content-type'] });
        const reply = (data, status = 200) => { response.statusCode = status; response.setHeader('Content-Type', 'application/json'); response.end(JSON.stringify({ ok: status < 400, data })); };
        if (url.pathname.startsWith('/api/assistant/') && !/^[A-Za-z0-9_-]{16,80}$/.test(lane || '')) return reply({ error: 'Missing execution channel' }, 400);
        let body = '';
        for await (const part of request) body += part;
        const value = request.headers['content-type']?.includes('application/json') && body ? JSON.parse(body) : {};
        if (url.pathname === '/api/auth/status') return reply({ logged_in: true, user: { open_id: 'shared-fixture-person', name: 'Offline fixture', role: 'member', level: 'L4', modules: ['assistant'] }, scope_options: [{ value: 'E', label: 'E' }] });
        if (url.pathname === '/api/assistant/fixture-download') { response.setHeader('Content-Type', 'text/plain'); response.end('fixture'); return; }
        if (url.pathname === '/api/assistant/conversation') return reply(snapshot(lane));
        if (url.pathname === '/api/assistant/conversation/stamp') return reply({ stamp: String(revision) });
        if (url.pathname === '/api/assistant/messages') {
          const result = start(lane, value); reply(result);
          if (value.question === 'preview') setTimeout(() => { const run = runs.get(result.run_id); delta(run, '## Live reply\n\n' + chinese.repeat(5)); finish(run); }, 200);
          return;
        }
        const match = url.pathname.match(/^\/api\/assistant\/runs\/([^/]+)\/(stream|cancel|resume)$/);
        if (match) {
          const run = runs.get(match[1]);
          if (!run || run.lane !== lane) return reply({ error: 'Different execution channel' }, 403);
          if (match[2] === 'cancel') { finish(run, 'stopped'); return reply({ run_id: run.id, status: 'stopped' }); }
          if (match[2] === 'resume') return reply(start(lane, { ...run.turn }, run.turn));
          response.setHeader('Content-Type', 'text/event-stream'); response.setHeader('Cache-Control', 'no-cache');
          response.setHeader('x-vercel-ai-ui-message-stream', 'v1'); response.flushHeaders();
          run.streams.add(response);
          for (const event of run.events) response.write('data: ' + JSON.stringify(event) + '\n\n');
          if (run.status !== 'pending') { response.end('data: [DONE]\n\n'); run.streams.delete(response); }
          response.on('close', () => run.streams.delete(response)); return;
        }
        if (url.pathname === '/api/assistant/appearance') return reply({ shape: 'cercle', color: 'encre', size: 56 });
        if (url.pathname === '/api/assistant/files') return reply({ files: [{ id: 'a'.repeat(32), name: 'fixture.txt', url: '/api/assistant/files/' + 'a'.repeat(32) }] });
        if (url.pathname === '/api/assistant/intelligence-capabilities') return reply({ research: true });
        if (url.pathname === '/api/assistant/voice') return reply({ stt: false, tts: false });
        return reply({ items: [], records: [], available: false });
      });
    },
  }, vue(), { name: 'fixture-scroll-adapter', enforce: 'post', transform(code, id) {
    if (!id.replaceAll('\\', '/').includes('/components/LighthouseAssistant.vue') || id.includes('type=style')) return;
    return adaptBrowserReferences(code, id, path.join(root, 'src/workspace-v2/browser.ts').replaceAll('\\', '/'));
  } }],
});
let browser;
try {
  await mkdir(output, { recursive: true }); await server.listen();
  const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
  console.log('Offline fixture: ' + origin + '/__assistant-concurrent?preview');
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 } });
  await context.addInitScript(() => { try { window.initialClient = sessionStorage.getItem('lighthouse_client'); } catch {} });
  await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
  async function page(viewport) {
    const target = await context.newPage(); if (viewport) await target.setViewportSize(viewport);
    target.setDefaultTimeout(10000); target.on('pageerror', error => errors.push(error.message));
    await target.goto(origin + '/__assistant-concurrent');
    await target.waitForFunction(() => !!window.fixture);
    await target.evaluate(() => window.fixture.vm.openAssistant()); return target;
  }
  const desktop = await page(), mobile = await page({ width: 390, height: 844 });
  const laneA = await desktop.evaluate(() => fixture.assistantClientId()), laneB = await mobile.evaluate(() => fixture.assistantClientId());
  assert.notEqual(laneA, laneB, 'tabs for the same person must have independent channels');
  for (const lane of [laneA, laneB]) assert.match(lane, /^[A-Za-z0-9_-]{16,80}$/);
  async function send(target, question) {
    await target.getByRole('textbox', { name: '\u8be2\u95ee\u706f\u5854\u52a9\u624b' }).fill(question);
    await target.getByRole('button', { name: '\u53d1\u9001\u95ee\u9898', exact: true }).click();
    await target.waitForFunction(() => !fixture.vm.sending && !!fixture.vm.state.active_run_id);
    const id = await target.evaluate(() => fixture.vm.state.active_run_id);
    await target.waitForFunction(() => fixture.vm.streamChat?.status === 'streaming'); return runs.get(id);
  }
  let runA = await send(desktop, 'first desktop question');
  const popup = desktop.waitForEvent('popup'); await desktop.evaluate(url => window.open(url, '_blank'), origin + '/__assistant-concurrent');
  const copied = await popup; copied.on('pageerror', error => errors.push(error.message)); await copied.waitForFunction(() => !!window.fixture);
  assert.equal(await copied.evaluate(() => window.initialClient), laneA, 'window.open really copies sessionStorage before claiming');
  const copiedLane = await copied.evaluate(() => fixture.assistantClientId()); assert.notEqual(copiedLane, laneA, 'the copied page yields its inherited channel');
  assert.equal(await desktop.evaluate(() => fixture.assistantClientId()), laneA, 'the original running page keeps its channel');
  await copied.evaluate(() => fixture.vm.openAssistant()); const copiedRun = await send(copied, 'copied tab question');
  assert.equal(copiedRun.lane, copiedLane, 'the first copied-tab submit uses its resolved channel');
  await copied.evaluate(() => fixture.vm.stopAnswer()); assert.equal(runA.status, 'pending', 'stopping the copied tab cannot stop its opener'); await copied.close();
  await mobile.evaluate(() => fixture.vm.readState(false, true));
  assert.equal(await mobile.evaluate(() => fixture.vm.state.busy), false, 'another tab cannot make this channel busy');
  const runB = await send(mobile, 'parallel mobile question');
  assert.equal(await mobile.evaluate(id => fixture.vm.state.turns.find(turn => turn.run_id === id).status, runA.id), 'pending', 'sending in another channel does not supersede its visible turn');
  assert.equal(runA.turn.status, 'pending');
  delta(runA, chinese); delta(runB, 'Independent mobile stream.');
  await desktop.waitForFunction(({ id, text }) => fixture.vm.state.turns.find(turn => turn.run_id === id)?.answer === text, { id: runA.id, text: chinese });
  await mobile.waitForFunction(id => fixture.vm.state.turns.find(turn => turn.run_id === id)?.answer === 'Independent mobile stream.', runB.id);
  assert.equal(runA.turn.answer, '', 'visible text arrives before persisted/final business state');
  assert.equal(await desktop.locator('.turn').last().locator('.v2-reply [hidden]').count(), 0, 'true streaming has no artificial replay queue');
  assert.equal(await desktop.locator('.turn').last().locator('.v2-reply-caret').count(), 1);
  assert(await desktop.locator('.turn').last().locator('.v2-word.cword').count() > 0, 'Chinese stream keeps word fade');
  assert(await desktop.locator('.turn').last().locator('.v2-word').evaluateAll(nodes => nodes.every(node => !/^[\u3400-\u9fff]{4}/u.test(node.textContent))));
  delta(runA, chinese);
  await desktop.waitForFunction(({ id, text }) => fixture.vm.state.turns.find(turn => turn.run_id === id)?.answer === text, { id: runA.id, text: runA.answer });
  assert(await desktop.locator('.turn').first().locator('.v2-word').evaluateAll(nodes => nodes.some(node => parseFloat(node.style.animationDelay) < 0)), 'successive tokens continue the previous fade');
  await desktop.evaluate(() => fixture.vm.readState(false, true));
  assert.equal(await desktop.evaluate(id => fixture.vm.state.turns.find(turn => turn.run_id === id).answer, runA.id), runA.answer, 'stale GET cannot rewind the live answer');
  emit(runA, { type: 'data-turn', data: { ...runA.turn, answer: '\u8fd9', process: [] }, transient: true });
  emit(runA, { type: 'data-turn', data: { ...runA.turn, answer: 'different pending answer '.repeat(20), process: [] }, transient: true });
  await desktop.waitForTimeout(100);
  assert.equal(await desktop.evaluate(id => fixture.vm.state.turns.find(turn => turn.run_id === id).answer, runA.id), runA.answer, 'pending data-turn cannot rewind the answer');
  await desktop.evaluate(() => fixture.vm.uploadFile(new File(['fixture'], 'fixture.txt', { type: 'text/plain' })));
  await desktop.evaluate(() => fixture.uploadJson('/api/assistant/files', new FormData()));
  await desktop.evaluate(() => fixture.requestBinaryJson('/api/assistant/fixture-binary', new Uint8Array([1, 2]), { headers: { 'Content-Type': 'application/octet-stream' } }));
  assert.equal(await desktop.evaluate(async () => (await fixture.requestFile('/api/assistant/fixture-download')).text()), 'fixture');
  await desktop.evaluate(() => fixture.requestJson('/api/fixture/read'));
  assert.equal(requests.find(request => request.path === '/api/fixture/read').lane, undefined, 'channel header stays scoped to assistant APIs');
  const previousRun = runA; runA = await send(desktop, 'desktop follow-up');
  assert.equal(previousRun.status, 'superseded', 'a follow-up replaces this channel\'s active run');
  assert.equal(runB.status, 'pending', 'a follow-up leaves the other channel running');
  assert.equal(await desktop.evaluate(id => fixture.vm.state.turns.find(turn => turn.run_id === id).status, runB.id), 'pending', 'follow-up preserves the other pending bubble');
  const beforeCancel = requests.length; await desktop.evaluate(() => fixture.vm.stopAnswer());
  assert(requests.slice(beforeCancel).some(request => request.path === '/api/assistant/runs/' + runA.id + '/cancel' && request.lane === laneA));
  assert.equal(runB.turn.status, 'pending', 'stopping desktop leaves mobile running');
  delta(runB, ' Still running.');
  await mobile.waitForFunction(id => fixture.vm.state.turns.find(turn => turn.run_id === id)?.answer.endsWith('Still running.'), runB.id);
  const verified = '\u5df2\u6821\u9a8c\u7684\u6700\u7ec8\u7ed3\u679c\u3002'; finish(runB, 'completed', verified);
  await mobile.waitForFunction(({ id, text }) => { const turn = fixture.vm.state.turns.find(turn => turn.run_id === id); return turn?.answer === text && turn.status === 'completed'; }, { id: runB.id, text: verified });
  await mobile.evaluate(id => {
    const turn = fixture.vm.state.turns.find(turn => turn.run_id === id);
    fixture.vm.replaceState({ ...fixture.vm.state, turns: fixture.vm.state.turns.map(item => item === turn ? { ...item, status: 'pending', answer: '' } : { ...item }) });
  }, runB.id);
  assert.equal(await mobile.evaluate(id => fixture.vm.state.turns.find(turn => turn.run_id === id).status, runB.id), 'completed', 'late pending read cannot reopen a finished answer');
  await mobile.waitForFunction(() => !document.querySelector('.v2-reply[data-revealing="true"]') && [...document.querySelectorAll('.v2-word')].every(node => node.getAnimations().every(animation => animation.playState === 'finished')));
  assert.equal(await mobile.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'mobile reply stays inside the viewport');
  await mobile.screenshot({ path: path.join(output, 'mobile-verified.png') });
  await desktop.evaluate(id => { const turn = fixture.vm.state.turns.find(turn => turn.run_id === id); turn.resume_available = true; return fixture.vm.resumeResearch(turn); }, runA.id);
  const resumed = runs.get(await desktop.evaluate(() => fixture.vm.state.active_run_id));
  assert.notEqual(resumed.id, runA.id, 'resume uses a new run in the same channel');
  await desktop.waitForFunction(() => fixture.vm.streamChat?.status === 'streaming');
  delta(resumed, ('## Live reply\n\n' + chinese + '\n\n').repeat(45));
  await desktop.waitForFunction(() => { const thread = document.querySelector('.thread'); return thread.scrollHeight > thread.clientHeight + 200 && thread.scrollHeight - thread.clientHeight - thread.scrollTop < 3; });
  const thread = desktop.locator('.thread'); await thread.hover(); await desktop.mouse.wheel(0, -250); await desktop.waitForTimeout(180);
  const top = await thread.evaluate(node => node.scrollTop); delta(resumed, chinese.repeat(4)); await desktop.waitForTimeout(450);
  assert(Math.abs(await thread.evaluate(node => node.scrollTop) - top) < 3, 'manual upward scroll pauses follow');
  await thread.evaluate(node => { node.scrollTop = node.scrollHeight; }); delta(resumed, chinese);
  await desktop.waitForFunction(() => { const thread = document.querySelector('.thread'); return thread.scrollHeight - thread.clientHeight - thread.scrollTop < 3; });
  await desktop.screenshot({ path: path.join(output, 'desktop-live.png') });
  await desktop.emulateMedia({ reducedMotion: 'reduce' }); delta(resumed, '\n\n' + chinese);
  await desktop.waitForFunction(text => fixture.vm.state.turns.find(turn => turn.run_id === fixture.vm.state.active_run_id)?.answer === text, resumed.answer);
  await desktop.waitForFunction(() => !document.querySelector('.turn:last-child .v2-word'));
  assert.equal(await desktop.locator('.turn').last().locator('.v2-word').count(), 0, 'reduced motion renders without word animation');
  finish(resumed); await desktop.waitForFunction(() => fixture.vm.state.busy === false);
  const savedLane = await desktop.evaluate(() => sessionStorage.getItem('lighthouse_client'));
  await desktop.reload(); await desktop.waitForFunction(() => !!window.fixture);
  assert.equal(await desktop.evaluate(() => fixture.assistantClientId()), savedLane, 'reload keeps the tab channel');
  await mobile.evaluate(() => sessionStorage.setItem('lighthouse_client', 'invalid value'));
  await mobile.reload(); await mobile.waitForFunction(() => !!window.fixture);
  const repairedLane = await mobile.evaluate(() => fixture.assistantClientId()); assert.match(repairedLane, /^[A-Za-z0-9_-]{16,80}$/); assert.notEqual(repairedLane, laneB);
  const blocked = await context.newPage(); await blocked.addInitScript(() => { window.BroadcastChannel = undefined; Object.defineProperty(Crypto.prototype, 'randomUUID', { value: undefined }); Storage.prototype.getItem = () => { throw new DOMException('Blocked', 'SecurityError'); }; Storage.prototype.setItem = () => { throw new DOMException('Blocked', 'SecurityError'); }; });
  blocked.on('pageerror', error => errors.push(error.message)); await blocked.goto(origin + '/__assistant-concurrent'); await blocked.waitForFunction(() => !!window.fixture);
  assert.equal(await blocked.evaluate(() => fixture.assistantClientId()), await blocked.evaluate(() => fixture.assistantClientId()), 'blocked storage retains one in-memory channel');
  assert.match(await blocked.evaluate(() => fixture.assistantClientId()), /^[a-f0-9]{32}$/, 'HTTP without randomUUID still gets a random channel');
  for (const request of requests.filter(request => request.path.startsWith('/api/assistant/'))) assert.match(request.lane, /^[A-Za-z0-9_-]{16,80}$/, request.path);
  assert(requests.some(request => request.path === '/api/assistant/files' && request.type.startsWith('multipart/form-data; boundary=')), 'upload keeps its browser-generated multipart boundary');
  assert.deepEqual(errors, []);
  console.log('PASS: independent channels, real incremental SSE, stale snapshots, verified final replacement, cancel/resume, upload headers, Chinese word fade, caret, scroll, reduced motion and storage recovery.');
  if (process.argv.includes('--real-page')) {
    console.log('Building the full workspace into the isolated output directory.');
    const previousOutput = process.env.CLIPFLOW_WORKSPACE_BUILD_DIR; process.env.CLIPFLOW_WORKSPACE_BUILD_DIR = realBuild;
    try { await build({ root, configFile: path.join(root, 'vite.workspace-v2.config.ts'), logLevel: 'error' }); }
    finally { if (previousOutput === undefined) delete process.env.CLIPFLOW_WORKSPACE_BUILD_DIR; else process.env.CLIPFLOW_WORKSPACE_BUILD_DIR = previousOutput; }
    const realContext = await browser.newContext({ viewport: { width: 1440, height: 960 } });
    await realContext.addInitScript(() => { try { window.initialClient = sessionStorage.getItem('lighthouse_client'); } catch {} });
    await realContext.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    realContext.on('page', target => { target.setDefaultTimeout(15000); target.on('pageerror', error => errors.push(error.message)); });
    const url = origin + '/assets/workspace-v2/index.html?view=assistant';
    const realDesktop = await realContext.newPage(); await realDesktop.goto(url);
    await realDesktop.locator('.v2-shell').waitFor();
    await realDesktop.getByRole('button', { name: '\u6253\u5f00\u706f\u5854\u52a9\u624b', exact: true }).click();
    async function sendOnRealPage(target, question) {
      await target.getByRole('textbox', { name: '\u8be2\u95ee\u706f\u5854\u52a9\u624b' }).fill(question);
      const posted = target.waitForResponse(response => new URL(response.url()).pathname === '/api/assistant/messages' && response.request().method() === 'POST');
      const streamed = target.waitForResponse(response => /^\/api\/assistant\/runs\/[^/]+\/stream$/.test(new URL(response.url()).pathname));
      await target.getByRole('button', { name: '\u53d1\u9001\u95ee\u9898', exact: true }).click();
      const result = await (await posted).json(), run = runs.get(result.data.run_id);
      assert.equal(new URL((await streamed).url()).pathname, '/api/assistant/runs/' + run.id + '/stream');
      return run;
    }
    const realA = await sendOnRealPage(realDesktop, 'real page desktop stream');
    const answerA = realDesktop.locator('.turn').filter({ hasText: 'real page desktop stream' }).locator('.assistant-rich-reply');
    await realDesktop.evaluate(() => {
      window.assistantFrames = []; const until = performance.now() + 600;
      function sample() {
        const turn = [...document.querySelectorAll('.turn')].find(turn => turn.querySelector('.user-bubble')?.textContent.includes('real page desktop stream'));
        const reply = turn?.querySelector('.assistant-rich-reply'), word = reply?.querySelector('.v2-word');
        window.assistantFrames.push({ at: Date.now(), length: reply?.textContent.trim().length || 0, opacity: word ? Number(getComputedStyle(word).opacity) : null, carets: reply?.querySelectorAll('.v2-reply-caret').length || 0 });
        if (performance.now() < until) requestAnimationFrame(sample);
      }
      requestAnimationFrame(sample);
    });
    delta(realA, chinese); await answerA.getByText(chinese, { exact: true }).waitFor();
    const firstVisibleAt = Date.now(); assert.equal(realA.status, 'pending', 'full page shows text before the model finishes');
    assert.equal(await answerA.locator('[hidden]').count(), 0);
    assert.equal(await answerA.locator('.v2-reply-caret').count(), 1);
    assert.equal(await answerA.locator('.v2-word').first().evaluate(node => getComputedStyle(node).animationDuration), '0.38s');
    delta(realA, chinese); await answerA.getByText(realA.answer, { exact: true }).waitFor();
    assert(await answerA.locator('.v2-word').evaluateAll(nodes => nodes.some(node => parseFloat(node.style.animationDelay) < 0)), 'full-page token updates preserve running fades');
    await realDesktop.screenshot({ path: path.join(output, 'real-page-desktop-live.png') });
    const opened = realDesktop.waitForEvent('popup'); await realDesktop.evaluate(url => window.open(url, '_blank'), url);
    const realMobile = await opened; await realMobile.setViewportSize({ width: 390, height: 844 }); await realMobile.locator('.v2-shell').waitFor();
    await realMobile.getByRole('textbox', { name: '\u8be2\u95ee\u706f\u5854\u52a9\u624b' }).waitFor();
    assert.equal(await realMobile.evaluate(() => window.initialClient), realA.lane, 'the real page opens with cloned storage');
    const realB = await sendOnRealPage(realMobile, 'real page copied mobile stream');
    assert.notEqual(realB.lane, realA.lane, 'the first real-page submit has already resolved the collision');
    assert.equal(realA.status, 'pending', 'the original full-page stream survives the copied tab');
    assert.equal(await realDesktop.evaluate(() => sessionStorage.getItem('lighthouse_client')), realA.lane);
    const answerB = realMobile.locator('.turn').filter({ hasText: 'real page copied mobile stream' }).locator('.assistant-rich-reply');
    delta(realB, chinese); await answerB.getByText(chinese, { exact: true }).waitFor();
    const mobileFirstVisibleAt = Date.now();
    assert.equal(await realMobile.locator('.v2-mobile-nav').isVisible(), true, 'test uses the real mobile workspace shell');
    await realMobile.screenshot({ path: path.join(output, 'real-page-mobile-live.png') });
    const frames = await realDesktop.evaluate(() => window.assistantFrames);
    await realDesktop.reload(); await realDesktop.getByRole('textbox', { name: '\u8be2\u95ee\u706f\u5854\u52a9\u624b' }).waitFor();
    await answerA.getByText(realA.answer, { exact: true }).waitFor();
    assert.equal(await realDesktop.evaluate(() => sessionStorage.getItem('lighthouse_client')), realA.lane, 'reload restores the running channel instead of allocating another one');
    await realDesktop.getByRole('button', { name: '\u505c\u6b62\u751f\u6210', exact: true }).click();
    await realDesktop.getByRole('button', { name: '\u505c\u6b62\u751f\u6210', exact: true }).waitFor({ state: 'hidden' });
    assert.equal(realB.status, 'pending', 'real-page stop does not cancel the copied mobile tab');
    delta(realB, chinese); await answerB.getByText(realB.answer, { exact: true }).waitFor();
    finish(realB, 'completed', verified); await answerB.getByText(verified, { exact: true }).waitFor();
    assert(firstVisibleAt < realA.finishedAt && mobileFirstVisibleAt < realB.finishedAt, 'both real pages render partial text before their terminal response');
    await realMobile.waitForFunction(() => !document.querySelector('.v2-reply[data-revealing="true"]') && [...document.querySelectorAll('.v2-word')].every(node => node.getAnimations().every(animation => animation.playState === 'finished')));
    assert.equal(await answerB.locator('.v2-reply-caret').isVisible(), false);
    assert.equal(await realMobile.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await realMobile.screenshot({ path: path.join(output, 'real-page-mobile-final.png') });
    assert(frames.some(frame => frame.opacity > 0 && frame.opacity < 1), 'frame samples contain a visible word fade');
    await writeFile(path.join(output, 'real-page-timing.json'), JSON.stringify({ url, desktop: { firstTokenLatencyMs: firstVisibleAt - realA.firstDeltaAt, firstVisibleAt, terminalAt: realA.finishedAt }, mobile: { firstTokenLatencyMs: mobileFirstVisibleAt - realB.firstDeltaAt, firstVisibleAt: mobileFirstVisibleAt, finalAt: realB.finishedAt }, frames, differentChannels: realA.lane !== realB.lane, reloadPreservedChannel: true, originalUnaffectedByCopy: true }, null, 2));
    assert.deepEqual(errors, []); await realContext.close();
    console.log('PASS: full production workspace page on desktop/mobile, actual window.open storage copy, independent first requests, live SSE word fades, refresh recovery and isolated stop; synthetic APIs only.');
  }
  if (process.argv.includes('--responsively')) {
    await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['C:/Users/l1773/.codex/scripts/responsively-preview.mjs', origin + '/__assistant-concurrent?preview', path.join(output, 'responsively')], { stdio: 'inherit', windowsHide: true });
      child.on('error', reject); child.on('exit', code => code === 0 ? resolve() : reject(new Error('Responsively exited ' + code)));
    });
  }
} catch (error) {
  const details = [];
  for (const context of browser?.contexts() || []) for (const target of context.pages()) {
    if (!target.isClosed() && target.url().includes('/assets/workspace-v2/')) {
      details.push(await target.evaluate(() => ({ url: location.href, turns: [...document.querySelectorAll('.turn')].map(turn => ({ question: turn.querySelector('.user-bubble')?.textContent, answer: turn.querySelector('.assistant-rich-reply')?.textContent?.slice(0, 600) })), pageErrors: [...document.querySelectorAll('[role="alert"]')].map(node => node.textContent) })));
      await target.screenshot({ path: path.join(output, 'real-page-failure-' + details.length + '.png') });
    }
  }
  await writeFile(path.join(output, 'real-page-failure.json'), JSON.stringify({ error: error.message, errors, details }, null, 2));
  throw error;
} finally {
  for (const run of runs.values()) for (const response of run.streams) response.end();
  await browser?.close(); await server.close();
}
