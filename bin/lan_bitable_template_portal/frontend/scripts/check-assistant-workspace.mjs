import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createServer, preview } from 'vite';
import { chromium } from 'playwright';
import { listenTestServer } from './test-server.mjs';

// All API responses in this harness are isolated fixtures. No production service is contacted.
const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.join(process.env.TEMP || root, 'lighthouse-workspace-check');
const requests = [], owners = new Map();
const builtDirectory = process.argv.find(value => value.startsWith('--built-dir='))?.slice('--built-dir='.length);
const markdownMode = process.argv.includes('--markdown');
const photoMode = process.argv.includes('--photos');
const photoFile = { id: 'c'.repeat(32), name: '原文步骤_工作表行2_现场照片.jpg', url: '/api/assistant/files/' + 'c'.repeat(32), is_image: true, inline_image: true };
const markdownFile = { id: 'b'.repeat(32), name: '完整回答.md', url: '/api/assistant/files/' + 'b'.repeat(32) };
const markdownText = '# 完整答复\n\n- 检查项目甲\n- 检查项目乙\n\n```python\nprint("保留中文")\n```\n\n' + '完整正文。\n'.repeat(800) + '\n最后一行。\n<img src=x onerror="window.unsafe=true">';
let voiceAvailable = true, voiceAdmin = true, newReply = false, failEdit = false, deferUpload, deferWorkspace;
function state(owner) {
  if (!owners.has(owner)) owners.set(owner, {
    documents: [{ id: 'doc-' + owner, title: '交接说明', revision: 1, content: '原文\n<img src=x onerror="window.unsafe=true">', updated_at: 1792400000 }],
    packages: [], memories: [], versions: [],
  });
  return owners.get(owner);
}
const receipt = { id: 'a'.repeat(32), name: '录音.webm', url: '/api/assistant/files/' + 'a'.repeat(32) };
const speechConfig = { allowed_origins: ['https://speech.example.com'], stt: { enabled: false, endpoint: '', model: '', has_key: false }, tts: { enabled: false, endpoint: '', model: '', voice: '', has_key: false } };
const capabilities = () => ({ stt: voiceAvailable, tts: voiceAvailable, transcription: { available: voiceAvailable, note: '语音识别尚未配置' }, synthesis: { available: voiceAvailable, note: '语音播报尚未配置' }, can_manage: voiceAdmin, ...(voiceAdmin ? { configuration: speechConfig } : {}) });
async function api(req, res) {
  const url = new URL(req.url, 'http://fixture.local');
  const owner = req.headers['x-clipflow-impersonation'] || 'fixture-a', saved = state(owner);
  let body = '';
  for await (const chunk of req) { if (url.pathname !== '/api/assistant/files') body += chunk; }
  const payload = body ? JSON.parse(body) : {};
  requests.push({ path: url.pathname, method: req.method, owner, payload });
  const send = (data, status = 200) => { res.statusCode = status; res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(status === 200 ? { ok: true, data } : { ok: false, error: data })); };
  if (url.pathname === photoFile.url) { res.setHeader('Content-Type','image/png');res.end(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aNNkAAAAASUVORK5CYII=','base64'));return; }
  if (url.pathname === '/api/auth/status') return send({ logged_in: true, user: { open_id: owner, name: '验收账号', modules: ['assistant'], role: 'admin' } });
  if (url.pathname === '/api/assistant/voice') {
    if (req.method === 'PUT') return send(speechConfig);
    if (payload.action === 'transcribe') return send({ text: '转写后的草稿' });
    if (payload.action === 'synthesize') return send(receipt);
    return send(capabilities());
  }
  if (url.pathname === '/api/assistant/settings') return send({ enabled: true, models: [], can_manage_shared: voiceAdmin });
  if (url.pathname === '/api/assistant/files') { if (deferUpload) await deferUpload; return send({ files: [receipt] }); }
  if (url.pathname === markdownFile.url) {
    if (url.searchParams.get('preview') === '1') return send({ ...markdownFile, kind: 'markdown', text: markdownText, truncated: false });
    res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
    res.setHeader('Content-Disposition', "attachment; filename=answer.md");
    return res.end(markdownText);
  }
  if (url.pathname === '/api/assistant/intelligence-capabilities') return send({ voice: capabilities(), documents: true, packages: true, memory: true, research: true, vision: false, sandbox: { available: false } });
  if (url.pathname === '/api/assistant/workspace') {
    if (['memory.save', 'memory.delete'].includes(payload.action)) assert.equal(payload.confirm_memory, true);
    else assert.equal(payload.confirm_memory, undefined);
    if (deferWorkspace) await deferWorkspace;
    const [kind, action] = payload.action.split('.'), group = { document: 'documents', package: 'packages', memory: 'memories' }[kind];
    const items = saved[group], item = items.find(row => row.id === payload.id);
    if (action === 'list') return send({ items, next_offset: null });
    if (action === 'create' || action === 'save' && !item) {
      const created = { id: kind + '-' + items.length, revision: 1, ...payload }; delete created.action; delete created.confirm_memory;
      items.push(created); return send(created);
    }
    if (action === 'read') return send(item);
    if (action === 'versions') return send({ items: [{ revision: 1 }, { revision: 2 }], next_offset: null });
    if (action === 'diff') { assert('from_version' in payload); return send({ diff: '- 原文\n+ 修改后' }); }
    if (action === 'export') return send(receipt);
    if (['edit', 'restore', 'update', 'delete', 'save'].includes(action)) {
      if (!item || payload.revision !== item.revision || failEdit && action === 'edit') { failEdit = false; return send('版本已变化，请重新读取', 409); }
      if (action === 'delete') { items.splice(items.indexOf(item), 1); return send({ deleted: true }); }
      if (action === 'edit') {
        assert(!('content' in payload), 'whole saves must still use an explicit old/new revision edit');
        if (!item.content.includes(payload.old_text)) return send('原文未匹配', 409);
        item.content = item.content.replace(payload.old_text, payload.new_text);
      } else if (action === 'restore') { assert('version' in payload); item.content = '恢复后的内容'; }
      else Object.assign(item, payload);
      item.revision++; return send(item);
    }
    return send('未实现的测试操作', 400);
  }
  if (url.pathname === '/api/assistant/conversation') return send({ conversation_id: 'conversation-' + owner, configured: true, enabled: true, can_manage_settings: voiceAdmin, busy: false, turns: [
    { operation_id: 'reply', run_id: 'complete-run', status: 'completed', question: '问候', answer: '这是已完成的回复。', ...(markdownMode ? { output_files: [markdownFile] } : photoMode ? {output_files:[photoFile]} : {}) },
    { operation_id: 'research', run_id: 'research-run', status: 'stopped', question: '/深入研究 测试问题', answer: '研究尚未完成。', resume_available: true },
    ...(newReply ? [{ operation_id: 'new-reply', run_id: 'new-run', status: 'completed', question: '新问题', answer: '新增完成回复' }] : []),
  ] });
  if (url.pathname === '/api/assistant/runs/research-run/resume') return send({ conversation_id: 'conversation-' + owner, run_id: 'resumed-run', turn: { operation_id: 'research', run_id: 'resumed-run', status: 'pending', question: '/深入研究 测试问题', answer: '' } });
  if (url.pathname === '/api/assistant/messages') return send({ conversation_id: 'conversation-' + owner, run_id: 'ordinary-run', turn: { operation_id: payload.operation_id, run_id: 'ordinary-run', status: 'completed', question: payload.question, answer: '普通问题回答' } });
  if (url.pathname.endsWith('/stream')) { res.setHeader('Content-Type', 'text/event-stream'); return res.end('data: [DONE]\n\n'); }
  if (url.pathname === '/api/assistant/appearance') return send({});
  if (url.pathname === '/api/assistant/commands') return send({ items: [] });
  if (url.pathname.endsWith('/stamp')) return send({ stamp: '' });
  return send({ items: [] });
}
const html = `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>助手工作区验收</title><body><div id="app"></div><script type="module">
import {createApp,h,ref} from 'vue';
import Assistant from '/src/components/LighthouseAssistant.vue';
import UiTransition from '/src/components/UiTransition.vue';
import {setImpersonationContext} from '/src/api/client.ts';
import {setModulePermissions} from '/src/api/modulePermissions.ts';
import {installActionMotion} from '/src/workspace-v2/actionMotion.ts';
const owner=ref('fixture-a');setImpersonationContext(owner.value);setModulePermissions(['assistant']);
sessionStorage.setItem('lighthouse_pos:fixture-a:open','true');
window.setFixtureOwner=value=>{setImpersonationContext(value);owner.value=value};
const app=createApp({setup:()=>()=>h(Assistant,{userId:owner.value,userName:'验收账号'})});app.component('UiTransition',UiTransition);app.mount('#app');installActionMotion();
if(new URLSearchParams(location.search).has('workspace')){const timer=setInterval(()=>{const button=document.querySelector('[aria-label="助手工作区"]');if(button){button.click();clearInterval(timer)}},100)}
if(new URLSearchParams(location.search).has('markdown')){const timer=setInterval(()=>{const button=document.querySelector('[aria-label="预览 完整回答.md"]');if(button){button.click();clearInterval(timer)}},100)}
</script></body></html>`;
const attachFixture = vite => { vite.middlewares.use(async (req, res, next) => {
  if (req.url.startsWith('/__assistant-workspace-check')) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    const built = builtDirectory && (await readFile(path.join(builtDirectory, 'assistant.html'), 'utf8')).replace('<div id="clipflow-lighthouse-widget"', '<div data-user-id="fixture-a" data-user-name="验收账号" id="clipflow-lighthouse-widget"').replace('<head>', '<head><script>sessionStorage.setItem("lighthouse_pos:fixture-a:open","true");</script>');
    return res.end(built || await vite.transformIndexHtml(req.url, html));
  }
  if (req.url.startsWith('/api/')) { try { await api(req, res); } catch (cause) { res.statusCode = 500; res.end(JSON.stringify({ ok: false, error: String(cause) })); } return; }
  next();
}); };
const fixturePlugin = { name: 'assistant-check-fixture', configureServer: attachFixture, configurePreviewServer: attachFixture };
const server = builtDirectory ? await preview({ root, logLevel: 'error', build: { outDir: builtDirectory }, preview: { host: '127.0.0.1', port: 0 }, plugins: [fixturePlugin] }) : await createServer({ root, logLevel: 'error', server: { host: '127.0.0.1', port: process.env.PORT ? Number(process.env.PORT) : 0 }, plugins: [fixturePlugin] });
if (!builtDirectory) await listenTestServer(server);
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
if (process.argv.includes('--serve')) { console.log(origin + '/__assistant-workspace-check?' + (markdownMode ? 'markdown=1' : photoMode ? 'photos=1' : 'workspace=1')); }
else if (photoMode) {
  const browser = await chromium.launch({headless:true});
  try {
    await mkdir(output,{recursive:true});
    const page=await browser.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(origin+'/__assistant-workspace-check');
    for(const size of [{width:402,height:874},{width:440,height:956},{width:1440,height:900}]){
      await page.setViewportSize(size);
      const img=page.locator('.output-file.photo-file img');await img.waitFor();
      await page.waitForFunction(()=>document.querySelector('.output-file.photo-file img')?.naturalWidth>0);
      const box=await img.boundingBox();assert(box.x>=0&&box.x+box.width<=size.width&&box.height<=160);
      assert.equal(await page.getByRole('button',{name:'预览 '+photoFile.name,exact:true}).count(),1);
      await page.screenshot({path:path.join(output,'source-photo-'+size.width+'.png'),fullPage:true});
    }
    assert.deepEqual(errors,[]);console.log('PASS: source photo thumbnails and row captions at 402/440/1440; all APIs isolated.');
  } finally {await browser.close();if(builtDirectory)await new Promise(resolve=>server.httpServer.close(resolve));else await server.close();}
}
else if (markdownMode) {
  const browser = await chromium.launch({ headless: true });
  try {
    await mkdir(output, { recursive: true });
    const page = await browser.newPage(), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(origin + '/__assistant-workspace-check');
    for (const size of [{ width: 360, height: 640 }, { width: 402, height: 874 }, { width: 440, height: 956 }, { width: 1440, height: 900 }]) {
      await page.setViewportSize(size);
      await page.getByRole('button', { name: '预览 完整回答.md', exact: true }).click();
      const panel = page.getByLabel('文件预览', { exact: true });
      await panel.getByRole('heading', { name: '完整答复' }).waitFor();
      assert.equal(await panel.locator('li').count(), 2);
      assert.match(await panel.locator('pre code').innerText(), /保留中文/);
      assert(await panel.getByText('最后一行。', { exact: true }).count());
      await panel.getByRole('button', { name: '原文', exact: true }).click();
      assert.equal(await panel.locator('.source-text').textContent(), markdownText);
      await panel.getByRole('button', { name: '预览', exact: true }).click();
      const box = await panel.boundingBox();
      assert(box.x >= 0 && box.x + box.width <= size.width, 'Markdown preview must stay within the screen');
      if (size.width <= 700) {
        assert(box.width >= size.width - 60, 'Phone preview must use the conversation width, not squeeze it to a strip');
        assert.equal(await page.locator('.assistant-panel').isVisible(), false);
      }
      const downloadEvent = page.waitForEvent('download');
      await panel.getByRole('link', { name: '下载 完整回答.md' }).click();
      const download = await downloadEvent;
      assert.equal(await readFile(await download.path(), 'utf8'), markdownText);
      await page.screenshot({ path: path.join(output, (builtDirectory ? 'built-' : '') + 'markdown-' + size.width + '.png'), fullPage: true });
      await panel.getByRole('button', { name: '关闭文件预览' }).click();
      await panel.waitFor({ state: 'detached' });
    }
    assert.equal(await page.evaluate(() => window.unsafe), undefined);
    assert.deepEqual(errors, []);
    console.log('PASS: actual assistant Markdown preview, headings/lists/code, exact source/download, XSS rejection, close and bounds at 360/402/440/1440px.');
    console.log('Screenshots: ' + output);
  } finally {
    await browser.close();
    if (builtDirectory) await new Promise(resolve => server.httpServer.close(resolve));
    else await server.close();
  }
}
else if (builtDirectory) {
  const browser = await chromium.launch({ headless: true });
  try {
    await mkdir(output, { recursive: true }); voiceAvailable = false;
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } }), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(origin + '/__assistant-workspace-check');
    await page.getByRole('button', { name: '助手工作区', exact: true }).click();
    await page.getByRole('button', { name: /交接说明/ }).click();
    await page.getByLabel('文档内容', { exact: true }).waitFor();
    for (const size of [{ width: 360, height: 740 }, { width: 402, height: 874 }, { width: 440, height: 956 }, { width: 1440, height: 900 }]) {
      await page.setViewportSize(size);
      assert(await page.locator('.assistant-workspace').evaluate(node => node.getBoundingClientRect().right <= innerWidth));
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await page.screenshot({ path: path.join(output, 'built-workspace-' + size.width + '.png'), fullPage: true });
    }
    assert.equal(await page.evaluate(() => window.unsafe), undefined);
    assert.deepEqual(errors, []);
    console.log('PASS: production assistant assets, private workspace API fixture, safe text, and screenshots at 360/402/440/1440px.');
    console.log('Screenshots: ' + output);
  } finally { await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
}
else {
  let browser;
  try {
    await mkdir(output, { recursive: true });
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ viewport: { width: 1365, height: 900 }, reducedMotion: 'no-preference' });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', cause => errors.push(cause.message));
    await page.addInitScript(() => {
      window.voiceTracksStopped = 0; window.voiceRecorders = []; window.voicePlayers = [];
      const granted = () => ({ getTracks: () => [{ stop() { window.voiceTracksStopped++; } }] });
      Object.defineProperty(navigator, 'mediaDevices', { configurable: true, value: { getUserMedia: async () => {
        if (window.denyMicrophone) throw new DOMException('Denied', 'NotAllowedError');
        if (window.delayMicrophone) return new Promise(resolve => { window.releaseMicrophone = () => resolve(granted()); });
        return granted();
      } } });
      const originalFetch = window.fetch;
      window.fetch = (url, init) => { if (url === '/api/assistant/files' && init?.body instanceof FormData) { const file = init.body.get('files'); window.lastVoiceUpload = { name: file.name, type: file.type, size: file.size }; } return originalFetch(url, init); };
      window.MediaRecorder = class {
        static isTypeSupported(type) { return window.supportedAudio ? window.supportedAudio.includes(type) : true; }
        constructor(stream, options) { this.state = 'inactive'; this.mimeType = window.actualRecorderMime || options.mimeType; window.voiceRecorders.push(this); }
        start(timeslice) { this.timeslice = timeslice; this.state = 'recording'; }
        stop() { this.state = 'inactive'; const data = new Blob([new Uint8Array(window.largeRecording ? 20 * 1024 * 1024 + 1 : 20)], { type: this.mimeType }); queueMicrotask(() => { this.ondataavailable?.({ data }); this.onstop?.(); }); }
      };
      window.Audio = class {
        constructor(url) { this.url = url; window.voicePlayers.push(this); }
        async play() { if (window.denyPlayback) throw new Error('Denied'); }
        pause() { this.paused = true; } removeAttribute() {} load() {}
      };
    });
    await page.goto(origin + '/__assistant-workspace-check');
    const byName = name => page.getByRole('button', { name, exact: true });
    const openWorkspace = async () => { await byName('助手工作区').click(); await idle(); };
    const idle = async () => { await page.locator('.workspace-body[aria-busy="false"]').waitFor(); };
    const closeWorkspace = async () => { await byName('关闭工作区').click(); await page.locator('.assistant-workspace').waitFor({ state: 'detached' }); };
    const confirm = async () => { await page.locator('.confirm-modal').getByRole('button', { name: '确认', exact: true }).click(); };
    await byName('深入研究').waitFor();
    await page.waitForFunction(() => !document.querySelector('[aria-label="深入研究"]').disabled);
    await byName('深入研究').click();
    assert.equal(await page.locator('#assistant-question').inputValue(), '/深入研究 ');
    await openWorkspace();
    await page.getByRole('button', { name: /交接说明/ }).click(); await idle();
    assert.equal(await page.locator('.workspace-body').getByText('暂无文档', { exact: true }).count(), 0);
    await page.getByLabel('原文', { exact: true }).fill('原文'); await page.getByLabel('替换为', { exact: true }).fill('修改后');
    failEdit = true; await byName('应用局部修改').click(); await idle();
    assert.match(await page.locator('.workspace-error').innerText(), /版本/);
    assert.equal(await page.getByLabel('原文', { exact: true }).inputValue(), '原文');
    assert.equal(await byName('应用局部修改').getAttribute('data-v2-action'), null);
    await byName('应用局部修改').click(); await idle();
    assert.match(await page.getByLabel('文档内容', { exact: true }).inputValue(), /修改后/);
    await page.getByLabel('全文编辑', { exact: true }).check(); await page.getByLabel('文档内容', { exact: true }).fill('用户明确保存的全文');
    await byName('保存全文').click(); await idle();
    assert.equal(requests.filter(row => row.payload.action === 'document.edit').at(-1).payload.old_text.startsWith('修改后'), true);
    await byName('历史版本').click(); await idle(); await page.getByLabel('历史版本', { exact: true }).selectOption('1');
    await byName('查看').click(); await idle(); await byName('对比当前版本').click(); await idle();
    assert.match(await page.locator('.version-text').innerText(), /修改后/);
    await byName('恢复').click(); await confirm(); await idle();
    await byName('导出').click(); await idle(); assert.equal(await page.getByRole('link', { name: '下载文档' }).getAttribute('href'), receipt.url);
    await byName('返回列表').click(); await idle(); await byName('新建文档').click();
    await page.getByLabel('名称', { exact: true }).fill('新文档'); await page.getByLabel('文档内容', { exact: true }).fill('纯文本');
    await byName('保存').click(); await idle(); await byName('删除').click(); await confirm(); await idle();
    await byName('资料包').click(); await idle(); await byName('新建资料包').click();
    await page.getByLabel('名称', { exact: true }).fill('资料包一'); await page.getByLabel('处理说明').fill('按附件整理'); await byName('保存').click(); await idle();
    await page.getByLabel('处理说明').fill('按附件整理并核对'); await byName('保存').click(); await idle();
    assert(requests.filter(row => row.payload.action === 'package.update').at(-1).payload.revision);
    await byName('删除').click(); await confirm(); await idle();
    await byName('个人记忆').click(); await idle(); await byName('新建个人记忆').click(); await page.getByLabel('个人偏好', { exact: true }).fill('我喜欢简短回复');
    const before = requests.filter(row => row.payload.action === 'memory.save').length;
    await byName('保存').click(); assert.equal(requests.filter(row => row.payload.action === 'memory.save').length, before);
    await page.locator('.confirm-modal').getByRole('button', { name: '取消', exact: true }).click();
    await byName('保存').click(); await confirm(); await idle();
    await page.getByRole('button', { name: /我喜欢简短回复/ }).click(); await idle(); await page.getByLabel('个人偏好', { exact: true }).fill('我喜欢列表');
    await byName('保存').click(); await confirm(); await idle();
    await page.getByRole('button', { name: /我喜欢列表/ }).click(); await idle(); await byName('删除').click(); await confirm(); await idle();
    await page.locator('.speech-settings summary').click();
    await page.locator('.speech-settings input[type="password"]').first().fill('fixture-key-only');
    await byName('保存语音配置').click(); await confirm(); await idle();
    assert.deepEqual(await page.locator('.speech-settings input[type="password"]').evaluateAll(nodes => nodes.map(node => node.value)), ['', '']);
    assert.equal(await page.evaluate(() => JSON.stringify(sessionStorage).includes('fixture-key-only')), false);
    await page.setViewportSize({ width: 360, height: 740 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.screenshot({ path: path.join(output, 'workspace-360.png'), fullPage: true });
    assert(await page.locator('.assistant-workspace').evaluate(node => node.getBoundingClientRect().right <= innerWidth));
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await page.keyboard.press('Escape'); await page.locator('.assistant-workspace').waitFor({ state: 'detached' });
    await page.setViewportSize({ width: 1365, height: 900 });
    assert.equal(requests.filter(row => row.payload.action === 'synthesize').length, 0, 'opening and new completed turns never auto-synthesize');
    await byName('朗读回复').click(); await page.waitForFunction(() => window.voicePlayers.length === 1);
    await byName('停止播报').click(); assert(await page.evaluate(() => window.voicePlayers[0].paused));
    await page.locator('#assistant-question').fill('原有草稿');
    await byName('语音输入').click(); await byName('停止录音并转写').waitFor(); await byName('停止录音并转写').click();
    await page.waitForFunction(() => document.querySelector('#assistant-question').value.includes('转写后的草稿'));
    assert.equal(requests.filter(row => row.path === '/api/assistant/messages').length, 0);
    assert.equal(await page.evaluate(() => window.voiceRecorders[0].timeslice), 1000);
    for (const format of [{ mime: 'audio/mp4', suffix: '.m4a' }, { mime: 'audio/ogg;codecs=opus', suffix: '.ogg' }]) {
      await page.evaluate(mime => { window.supportedAudio = [mime]; }, format.mime);
      await byName('语音输入').click(); await byName('停止录音并转写').click();
      await page.waitForFunction(suffix => window.lastVoiceUpload.name.endsWith(suffix), format.suffix);
      assert.equal(await page.evaluate(() => window.lastVoiceUpload.type), format.mime);
      await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    }
    await page.evaluate(() => { window.supportedAudio = undefined; window.actualRecorderMime = 'audio/mp4'; });
    await byName('语音输入').click(); await byName('停止录音并转写').click();
    await page.waitForFunction(() => window.lastVoiceUpload.type === 'audio/mp4');
    assert.match(await page.evaluate(() => window.lastVoiceUpload.name), /\.m4a$/);
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await page.evaluate(() => { window.actualRecorderMime = undefined; });
    await page.evaluate(() => { window.denyMicrophone = true; }); await byName('语音输入').click();
    await page.getByRole('alert').filter({ hasText: '麦克风权限' }).waitFor();
    await page.evaluate(() => { window.denyMicrophone = false; window.largeRecording = true; });
    const uploadsBefore = requests.filter(row => row.path === '/api/assistant/files').length;
    await byName('语音输入').click(); await byName('停止录音并转写').click(); await page.getByRole('alert').filter({ hasText: '20MiB' }).waitFor();
    assert.equal(requests.filter(row => row.path === '/api/assistant/files').length, uploadsBefore);
    await page.evaluate(() => { window.largeRecording = false; window.delayMicrophone = true; });
    await byName('语音输入').click(); await page.evaluate(() => window.setFixtureOwner('fixture-b'));
    await page.evaluate(() => window.releaseMicrophone());
    assert.equal(requests.filter(row => row.path === '/api/assistant/files').length, uploadsBefore);
    await page.evaluate(() => { window.delayMicrophone = false; });
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    let releaseUpload; deferUpload = new Promise(resolve => { releaseUpload = resolve; });
    await byName('语音输入').click(); await byName('停止录音并转写').click();
    await page.waitForFunction(() => document.querySelector('.voice-status')?.textContent.includes('转写'));
    while (requests.filter(row => row.path === '/api/assistant/files').length === uploadsBefore) await new Promise(resolve => setTimeout(resolve, 20));
    const transcriptions = requests.filter(row => row.payload.action === 'transcribe').length;
    await page.evaluate(() => window.setFixtureOwner('fixture-c')); releaseUpload(); deferUpload = undefined;
    await page.waitForTimeout(100); assert.equal(requests.filter(row => row.payload.action === 'transcribe').length, transcriptions);
    await byName('收起助手').click(); await byName('打开灯塔助手').click();
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await byName('继续研究').click();
    await page.waitForFunction(() => !document.querySelector('[aria-label="继续研究"]:disabled'));
    assert.equal(requests.filter(row => row.path.endsWith('/research-run/resume')).length, 1);
    assert.equal(await page.locator('.assistant-error').filter({ hasText: '恢复响应' }).count(), 0);
    assert.equal(requests.filter(row => row.path === '/api/assistant/messages').length, 0);
    voiceAdmin = false;
    await byName('收起助手').click(); await byName('打开灯塔助手').click();
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await openWorkspace(); assert.equal(await page.locator('.speech-settings').count(), 0); await closeWorkspace();
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await byName('语音输入').click(); await byName('停止录音并转写').click();
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await byName('朗读回复').click(); await byName('停止播报').waitFor(); await byName('停止播报').click();
    const beforeCloseUploads = requests.filter(row => row.path === '/api/assistant/files').length;
    await byName('语音输入').click(); await byName('收起助手').click();
    assert.equal(requests.filter(row => row.path === '/api/assistant/files').length, beforeCloseUploads);
    assert(await page.evaluate(() => window.voiceTracksStopped > 0));
    await byName('打开灯塔助手').click(); await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    await page.clock.install();
    await byName('语音输入').click(); await page.clock.fastForward(120000);
    await page.waitForFunction(() => !document.querySelector('[aria-label="语音输入"]').disabled);
    assert.equal(requests.filter(row => row.path === '/api/assistant/files').length, beforeCloseUploads + 1);
    await page.evaluate(() => { window.denyPlayback = true; }); await byName('朗读回复').click();
    await page.getByRole('alert').filter({ hasText: '播放未完成' }).waitFor();
    await page.evaluate(() => { window.denyPlayback = false; });
    const synthesesBeforeReply = requests.filter(row => row.payload.action === 'synthesize').length;
    newReply = true;
    await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
    await page.getByText('新增完成回复', { exact: true }).waitFor();
    assert.equal(requests.filter(row => row.payload.action === 'synthesize').length, synthesesBeforeReply, 'new final reply stays silent');
    const finalReply = page.locator('article.turn').filter({ hasText: '新增完成回复' });
    await finalReply.getByRole('button', { name: '朗读回复', exact: true }).click();
    await finalReply.getByRole('button', { name: '停止播报', exact: true }).waitFor();
    assert.equal(await page.locator('[aria-label="停止播报"][aria-pressed="true"]').count(), 1);
    assert.equal(requests.filter(row => row.payload.action === 'synthesize').length, synthesesBeforeReply + 1);
    await byName('收起助手').click(); assert(await page.evaluate(() => window.voicePlayers.at(-1).paused));
    await byName('打开灯塔助手').click();
    assert.equal(await page.locator('[aria-label="停止播报"][aria-pressed="true"]').count(), 0);
    assert.equal(requests.filter(row => row.payload.action === 'synthesize').length, synthesesBeforeReply + 1, 'reopening never restarts playback');
    voiceAvailable = false;
    await byName('收起助手').click(); await byName('打开灯塔助手').click();
    await page.waitForFunction(() => {
      const button = document.querySelector('[aria-label="语音输入"]');
      return button?.disabled && button.parentElement.title.includes('未配置');
    });
    assert.match(await page.locator('[aria-label="语音输入"]').locator('..').getAttribute('title'), /未配置/);
    await page.locator('#assistant-question').fill('普通问题直接提交');
    await byName('发送问题').click();
    await page.waitForFunction(() => document.querySelector('#assistant-question').value === '');
    assert.equal(requests.filter(row => row.path === '/api/assistant/messages').at(-1).payload.question, '普通问题直接提交', 'normal questions do not require or acquire a research prefix');
    voiceAdmin = true;
    await byName('收起助手').click(); await byName('打开灯塔助手').click();
    await byName('模型设置').click(); await byName('添加模型').click();
    const modelName = page.getByLabel('模型名称', { exact: true }), imageInput = page.locator('input[name="image_input"]');
    for (const name of ['GLM', 'Qwen', 'Deepseek']) {
      await modelName.fill('WanWu/' + name + '-Auto');
      await page.waitForFunction(() => document.querySelector('input[name="image_input"]').disabled);
      assert.equal(await imageInput.isChecked(), false);
      await page.getByText('此模型仅支持文字；图片可提取 OCR 文字，场景识别请切换 MiniMax。', { exact: true }).waitFor();
      await modelName.fill('WanWu/MiniMax-Auto');
      await page.waitForFunction(() => !document.querySelector('input[name="image_input"]').disabled);
      await imageInput.check();
    }
    await byName('返回会话').click(); await confirm();
    assert.equal(await page.evaluate(() => window.unsafe), undefined);
    await page.screenshot({ path: path.join(output, 'assistant-desktop.png'), fullPage: true });
    assert.deepEqual(errors, []);
    console.log('PASS: document CRUD/versions/explicit full saves, conflict retention, packages, confirmed memory CRUD, admin key cleanup, 360px bounds, draft-only transcription, WebM/M4A/Ogg MIME, 120s/20MiB/permission handling, account races and close cleanup, nonadmin speech, playback default-off/click-only/failure, research resume, unavailable voice, and MiniMax-only vision controls.');
    console.log('Screenshots: ' + output);
  } finally { await browser?.close(); await server.close(); }
}
