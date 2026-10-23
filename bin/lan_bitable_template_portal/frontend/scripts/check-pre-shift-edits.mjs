import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {build, preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

// Synthetic HTTP fixtures also work in Responsively: KEEP_PREVIEW=1 node this-file.mjs.
// PREVIEW_ONLY=1 skips assertions; fixture/scenario/legacy/role query parameters select a view.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const project = path.resolve(root, '../../..');
const outDir = path.join(project, 'output/pre-shift-edits-build');
const output = path.join(project, 'output/playwright/pre-shift-edits');
const keepPreview = process.env.KEEP_PREVIEW === '1';
await mkdir(output, {recursive: true});
const python = path.join(project, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const photos = JSON.parse(execFileSync(python, ['-c', `
import datetime as dt,sys,tempfile,base64,json
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'bin'))
from PIL import Image,ImageDraw
from lan_bitable_template_portal.pre_shift_photo import render_handover,TZ
with tempfile.TemporaryDirectory() as folder:
    raw,stamped=Path(folder)/'source.png',Path(folder)/'stamp.jpg'
    image=Image.new('RGB',(1600,1200),'#708090')
    ImageDraw.Draw(image).rectangle((900,100,1400,1000),fill='#a5b5c5')
    image.save(raw)
    current=dt.datetime(2026,10,14,9,tzinfo=TZ)
    render_handover(raw,stamped,'A',current,capture={'captured_at':current.timestamp()*1000})
    print(json.dumps({'source':base64.b64encode(raw.read_bytes()).decode(),'stamped':base64.b64encode(stamped.read_bytes()).decode()}))
`], {cwd: project, encoding: 'utf8', maxBuffer: 12 * 1024 * 1024}));
const sourcePhoto = Buffer.from(photos.source, 'base64');
const stampedPhoto = Buffer.from(photos.stamped, 'base64');
await writeFile(path.join(output, 'watermarked-fixture.jpg'), stampedPhoto);
const items = Array.from({length: 24}, (_, i) => ({id: `item-${String(i + 1).padStart(2, '0')}`,
  text: i === 23 ? '核对最后一项设备运行状态、现场遗留事项及当班作业安排' : `核对第${i + 1}项设备运行状态与检查记录`}));
const lastItem = items.at(-1);
const now = Date.parse('2026-10-14T09:00:00+08:00');
const fixtures = new Map();
const report = {started: new Date().toISOString(), cases: [], failures: [], apiErrors: [], layoutIssues: [], output};
const clone = value => structuredClone(value);
function makeTask(id, completed, editable, history = false) {
  return {id, scope: 'A', day: history ? '2026-10-13' : '2026-10-14', shift: '白',
    due: now / 1000 - (history ? 86400 : 600), deadline: now / 1000 + (history ? -72000 : 36000),
    definition_version: 1, items: clone(items), people: [{name: '合成检查员'}],
    submitted: completed ? (history ? '2026-10-13T08:55:00+08:00' : '2026-10-14T08:55:00+08:00') : '',
    actor_name: '合成检查员', can_submit: !completed && editable, can_upload_handover: !completed && editable,
    can_edit: completed && editable, revision: 1, updated_at: '', updated_by: '', updated_by_name: '',
    answers: completed ? items.map(item => ({...item, status: 'normal', note: ''})) : [], files: [],
    handover_photo: completed ? {id: `${id}-handover`, name: '原交接班照片.jpg', item_id: '', kind: 'handover',
      is_image: true, can_delete: editable, size: stampedPhoto.length, watermarked: true} : null,
    normal_count: completed ? 24 : 0, abnormal_count: 0, sync_status: 'synced'};
}
function fixtureFor(params) {
  const key = params.get('fixture') || 'responsively';
  if (!fixtures.has(key)) {
    const editable = params.get('role') !== 'readonly';
    fixtures.set(key, {key, editable, tasks: {
      current: makeTask('current', params.get('scenario') !== 'pending' && params.has('scenario'), editable),
      history: makeTask('history', true, editable, true)}, requests: [], stages: new Map(), serial: 0, conflictNext: false});
    if (params.get('scenario') === 'completed-missing') {
      fixtures.get(key).tasks.current.handover_photo = null;
      fixtures.get(key).tasks.current.can_upload_handover = editable;
    }
  }
  return fixtures.get(key);
}
function bump(task) {
  task.revision++;
  task.updated_at = '2026-10-14T09:05:00+08:00';
  task.updated_by = 'fixture-editor'; task.updated_by_name = '合成修改人'; task.sync_status = 'pending';
}
async function bodyOf(req) {
  const chunks = []; let length = 0;
  for await (const chunk of req) {length += chunk.length; assert(length < 22 * 1024 * 1024); chunks.push(chunk);}
  return Buffer.concat(chunks);
}
async function fixtureApi(req, res, next) {
  const url = new URL(req.url, 'http://127.0.0.1');
  if (!url.pathname.startsWith('/api/')) return next();
  const params = new URL(req.headers.referer || '/', 'http://127.0.0.1').searchParams;
  const fixture = fixtureFor(params);
  const event = {method: req.method, path: url.pathname, query: Object.fromEntries(url.searchParams)};
  fixture.requests.push(event);
  const send = (data, status = 200) => {
    event.status = status; res.statusCode = status; res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(status < 400 ? {ok: true, data} : {ok: false, error: data}));
  };
  const taskFor = id => {assert(Object.hasOwn(fixture.tasks, id), `unknown task ${id}`); return fixture.tasks[id];};
  const revisionOK = (task, value) => !task.submitted || Number(value) === task.revision && value !== null;
  try {
    if (req.method !== 'GET' && !fixture.editable) return send('当前账号不可修改该次检查。', 403);
    if (url.pathname === '/api/pre-shift-checks' && req.method === 'GET') {
      assert.equal(url.searchParams.get('scope'), 'A');
      return send({scope: 'A', scopes: ['A'], can_manage: false, server_time: new Date(now).toISOString(),
        definition: {version: 1, items}, roster_ready: true, roster_error: '', tasks: [fixture.tasks.current]});
    }
    if (url.pathname === '/api/pre-shift-checks/history' && req.method === 'GET') {
      assert.equal(url.searchParams.get('scope'), 'A'); assert.equal(url.searchParams.get('page_size'), '20');
      return send({scope: 'A', page: 1, page_size: 20, total: 1, items: [fixture.tasks.history]});
    }
    const taskMatch = url.pathname.match(/^\/api\/pre-shift-checks\/tasks\/([^/]+)$/);
    if (taskMatch) {
      const task = taskFor(decodeURIComponent(taskMatch[1]));
      if (req.method === 'GET') return send(task);
      assert.equal(req.method, 'PUT');
      const body = JSON.parse((await bodyOf(req)).toString()); event.body = body;
      assert.deepEqual(Object.keys(body).sort(), ['answers', 'revision']); assert(Number.isInteger(body.revision));
      if (fixture.conflictNext) {fixture.conflictNext = false; bump(task); return send('记录已被其他人员修改，请重新读取后核对。', 409);}
      if (!revisionOK(task, body.revision)) return send('记录版本已变更，请重新读取。', 409);
      assert.deepEqual(body.answers.map(answer => answer.id), items.map(item => item.id));
      assert(body.answers.every(answer => ['normal', 'abnormal'].includes(answer.status) &&
        typeof answer.note === 'string' && (answer.status !== 'abnormal' || answer.note.trim())));
      task.answers = body.answers.map(answer => ({...items.find(item => item.id === answer.id), ...answer}));
      task.normal_count = task.answers.filter(answer => answer.status === 'normal').length;
      task.abnormal_count = 24 - task.normal_count; bump(task); return send(task);
    }
    if (url.pathname === '/api/pre-shift-checks/photo-preview') {
      if (req.method === 'DELETE') {
        const body = JSON.parse((await bodyOf(req)).toString()); assert(fixture.stages.delete(body.preview_id)); return send({removed: true});
      }
      assert.equal(req.method, 'POST'); const q = event.query, task = taskFor(q.task_id);
      if (!revisionOK(task, q.revision)) return send('记录版本已变更，请重新读取。', 409);
      assert(['item', 'handover'].includes(q.kind)); assert.equal(q.retain, '1');
      assert(q.kind === 'handover' ? q.item_id === '' : items.some(item => item.id === q.item_id));
      if (q.replace_file_id) assert.equal(task.handover_photo?.id, q.replace_file_id);
      const source = await bodyOf(req); assert(source.equals(sourcePhoto), 'real filechooser supplies the synthetic image once');
      const id = `preview-${++fixture.serial}`; fixture.stages.set(id, {...q, source});
      return send({preview_id: id, preferences: {version: 0, style: {}}, original_time: '2026-10-14 09:00',
        time_origin: 'exif', time_source: '照片拍摄信息', image: `data:image/png;base64,${photos.source}`,
        width: 1600, height: 1200, existing_watermark: false, recognition: 'not_detected',
        options: {enabled: true, existing: false, scale: .4, title: '合成班前检查', area: 'A楼',
          time: '2026-10-14 09:00', weather: '', location: '', logo: ''}});
    }
    if (url.pathname === '/api/pre-shift-checks/files') {
      assert.equal(req.method, 'POST'); const body = JSON.parse((await bodyOf(req)).toString()); event.body = body;
      assert.deepEqual(Object.keys(body).sort(), ['preview_id', 'remember_style', 'watermark']);
      const stage = fixture.stages.get(body.preview_id); assert(stage, 'preview is consumed exactly once');
      const task = taskFor(stage.task_id);
      if (!revisionOK(task, stage.revision)) return send('记录版本已变更，请重新读取。', 409);
      assert.equal(body.watermark.enabled, true); assert.equal(body.watermark.time, '2026-10-14 09:00');
      fixture.stages.delete(body.preview_id); bump(task);
      const file = {id: `saved-${++fixture.serial}`, name: stage.name.replace(/\.png$/i, '.jpg'), item_id: stage.item_id,
        kind: stage.kind, size: stampedPhoto.length, is_image: true, can_delete: true, watermarked: true,
        revision: task.revision, delivery_status: 'pending', watermark: {audit: {display_time: body.watermark.time,
          original_time: body.watermark.time, modified: false, source: '照片拍摄信息'}}};
      if (stage.kind === 'handover') {
        if (task.handover_photo) assert.equal(stage.replace_file_id, task.handover_photo.id);
        task.handover_photo = file;
        task.can_upload_handover = false;
      } else task.files.push(file);
      event.task_id = task.id; event.item_id = stage.item_id; event.file_id = file.id; return send(file);
    }
    const fileMatch = url.pathname.match(/^\/api\/pre-shift-checks\/files\/([^/]+)$/);
    if (fileMatch) {
      const id = decodeURIComponent(fileMatch[1]);
      const task = Object.values(fixture.tasks).find(task => task.files.some(file => file.id === id) || task.handover_photo?.id === id);
      assert(task, `file belongs to this isolated fixture: ${id}`);
      if (req.method === 'DELETE') {
        assert.equal(url.searchParams.get('task_id'), task.id);
        if (!revisionOK(task, url.searchParams.get('revision'))) return send('记录版本已变更，请重新读取。', 409);
        task.files = task.files.filter(file => file.id !== id);
        if (task.handover_photo?.id === id) task.handover_photo = null;
        bump(task); return send({revision: task.revision});
      }
      assert.equal(req.method, 'GET'); event.status = 200;
      res.setHeader('Content-Type', 'image/jpeg'); res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Disposition', "inline; filename*=UTF-8''fixture-watermarked.jpg"); return res.end(stampedPhoto);
    }
    throw new Error(`Unexpected fixture API: ${req.method} ${url.pathname}`);
  } catch (error) {
    report.apiErrors.push({fixture: fixture.key, ...event, error: error.message}); return send(error.message, 500);
  }
}

await build({root, configFile: false, publicDir: false, logLevel: 'error', plugins: [{
  name: 'pre-shift-edits-isolated', enforce: 'post',
  resolveId(id) {if (id === 'virtual:check-edits') return '\0check-edits';},
  load(id) {
    if (id !== '\0check-edits') return;
    return `import {createApp,h} from 'vue';
      import {NConfigProvider,NMessageProvider,NDialogProvider} from 'naive-ui';
      import Check from '/src/workspace-v2/dashboard/PreShiftCheck.vue';
      import Legacy from '/src/components/PreShiftChecklistPage.vue';
      import '/src/global.css';import '/src/workspace-v2/workspace.css';
      const p=new URLSearchParams(location.search),legacy=p.has('legacy');
      if(!legacy)document.documentElement.classList.add('workspace-v2');
      Object.defineProperty(navigator,'geolocation',{configurable:true,value:{getCurrentPosition:(_ok,fail)=>fail({code:1})}});
      window.__fixtureSubmits=0;document.addEventListener('submit',()=>window.__fixtureSubmits++,true);
      const user={open_id:'fixture-user',name:'合成检查员',level:'L4',category:'operator',home_scope:'A',modules:['pre_shift'],scope_rules:['A']};
      createApp({render:()=>h(NConfigProvider,{},()=>h(NMessageProvider,{},()=>h(NDialogProvider,{},()=>legacy?h(Legacy,{user}):h(Check,{user,registerGuard:()=>()=>{}}))))}).mount('#app');`;
  },
  generateBundle(_options, bundle) {
    const entry = Object.values(bundle).find(item => item.type === 'chunk' && item.isEntry);
    const css = Object.values(bundle).filter(item => item.fileName.endsWith('.css'));
    this.emitFile({type: 'asset', fileName: 'index.html', source: `<!doctype html><html lang="zh-CN"><head>
      <meta charset="UTF-8"><title>班前检查隔离回归</title><meta name="viewport" content="width=device-width,initial-scale=1">
      <meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; font-src 'self' data:; object-src 'none'">
      ${css.map(item => `<link rel="stylesheet" href="/${item.fileName}">`).join('')}
      <style>*{box-sizing:border-box}body{margin:0;padding:10px;background:#f5f6f7}#stage{width:1120px;max-width:100%;height:calc(100dvh - 20px);overflow:auto;padding:12px;border:1px solid #dedee3;border-radius:6px;background:white}</style>
      </head><body><main id="stage"><div id="page-back-slot"></div><div id="app"></div></main><script type="module" src="/${entry.fileName}"></script></body></html>`});
  },
}, vue()], build: {outDir, emptyOutDir: true, cssCodeSplit: false, rollupOptions: {input: 'virtual:check-edits'}}});
const server = await preview({root, configFile: false, logLevel: 'error', build: {outDir},
  plugins: [{name: 'synthetic-pre-shift-api', configurePreviewServer(server) {server.middlewares.use(fixtureApi);}}],
  preview: {host: '127.0.0.1', port: 0, strictPort: true}});
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
report.preview = `${origin}/?fixture=responsively&scenario=completed`;
await writeFile(path.join(output, 'preview.json'), JSON.stringify({url: report.preview,
  pending: `${origin}/?fixture=responsively-pending`, legacy: `${origin}/?fixture=responsively-legacy&legacy&scenario=completed`,
  screenshotDirectory: output, api: 'Synthetic HTTP middleware; no production calls'}, null, 2));
console.log(`Synthetic preview: ${report.preview}`);

async function layout(page, label, scope = page.locator('#stage')) {
  const issues = await scope.evaluate(root => {
    const issues = [], visible = el => !!el.getClientRects().length;
    if (document.documentElement.scrollWidth > innerWidth + 1) issues.push('document horizontal overflow');
    if (root.matches('.check-record-editor,.v2-photo-editor,.v2-check-photo-preview')) {
      const r = root.getBoundingClientRect();
      if (r.top < -1 || r.bottom > innerHeight + 1 || r.left < -1 || r.right > innerWidth + 1) {
        issues.push(`modal outside viewport: ${JSON.stringify({top: r.top, bottom: r.bottom, left: r.left, right: r.right, height: innerHeight})}`);
      }
    }
    if (root.matches('.check-record-editor')) {
      const outer = root.closest('.n-scrollbar-container');
      if (outer && outer.scrollHeight > outer.clientHeight + 2) {
        issues.push(`outer modal scrollbar has excess range: ${outer.scrollHeight}/${outer.clientHeight}, scrollTop=${outer.scrollTop}`);
      }
    }
    for (const el of [root, ...root.querySelectorAll('.n-card__content,.n-card-content,.v2-check-item,.check-record-item,.v2-check-file-actions,.check-record-footer')]) {
      if (visible(el) && el.scrollWidth > el.clientWidth + 2) issues.push(`${el.className}: horizontal overflow ${el.scrollWidth}/${el.clientWidth}`);
    }
    for (const group of root.querySelectorAll('.v2-check-file-actions,.check-record-footer,.v2-photo-editor-footer')) {
      const buttons = [...group.querySelectorAll('button')].filter(visible);
      for (let i = 0; i < buttons.length; i++) for (let j = i + 1; j < buttons.length; j++) {
        const a = buttons[i].getBoundingClientRect(), b = buttons[j].getBoundingClientRect();
        if (Math.min(a.right, b.right) - Math.max(a.left, b.left) > 1 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 1) issues.push('overlapping action buttons');
      }
    }
    for (const row of root.querySelectorAll('fieldset.v2-check-item,fieldset.check-record-item')) {
      const legend = row.querySelector('legend')?.getBoundingClientRect(), result = row.querySelector('.v2-check-result')?.getBoundingClientRect();
      if (legend && result && Math.min(legend.right, result.right) - Math.max(legend.left, result.left) > 1 &&
        Math.min(legend.bottom, result.bottom) - Math.max(legend.top, result.top) > 1) issues.push('overlapping question and answer controls');
    }
    return issues;
  });
  if (issues.length) {
    const ancestors = await scope.evaluate(el => {
      const rows = [];
      for (let node = el; node && node !== document.body; node = node.parentElement) {
        rows.push({class: node.className, top: node.getBoundingClientRect().top, scrollTop: node.scrollTop,
          clientHeight: node.clientHeight, scrollHeight: node.scrollHeight, overflowY: getComputedStyle(node).overflowY});
      }
      return rows;
    });
    report.layoutIssues.push({label, issues, ancestors});
  }
  await page.screenshot({path: path.join(output, `${label}.png`), fullPage: true});
}
async function clickUncovered(button) {
  await button.click({trial: true});
  assert(await button.evaluate(el => {const r = el.getBoundingClientRect(); return el.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2));}), 'button center is not covered');
  await button.click();
}
const count = (fixture, method, suffix) => fixture.requests.filter(event => event.method === method && event.path.endsWith(suffix)).length;
async function upload(page, zone, fixture, label, expectedTask, expectedItem, replaceId = '') {
  const previews = count(fixture, 'POST', '/photo-preview'), commits = count(fixture, 'POST', '/files');
  const oldRevision = fixture.tasks[expectedTask].revision;
  const [chooser] = await Promise.all([page.waitForEvent('filechooser'),
    clickUncovered(zone.getByRole('button', {name: replaceId ? '替换照片' : '上传照片', exact: true}))]);
  assert.equal(await chooser.element().getAttribute('capture'), null, 'album picker is not forced camera');
  await chooser.setFiles({name: `${label}.png`, mimeType: 'image/png', buffer: sourcePhoto});
  const editor = page.locator('.v2-photo-editor'); await editor.waitFor();
  const canvas = editor.getByLabel('照片水印预览', {exact: true});
  await page.waitForFunction(() => {const el = document.querySelector('.v2-photo-editor canvas'); return el?.width === 1600 && el.getContext('2d').getImageData(0, 0, 1, 1).data[3] > 0;});
  const before = await canvas.evaluate(el => el.toDataURL());
  await editor.getByRole('textbox', {name: '水印标题', exact: true}).fill('合成照片回归检查');
  await page.waitForFunction(old => document.querySelector('.v2-photo-editor canvas').toDataURL() !== old, before);
  await layout(page, `${label}-watermark`, editor);
  assert.equal(count(fixture, 'POST', '/files'), commits, 'preview does not commit');
  await editor.getByRole('button', {name: '确认水印并上传', exact: true}).click();
  await editor.waitFor({state: 'hidden'});
  const viewer = page.locator('.v2-check-photo-preview'); await viewer.waitFor();
  await page.waitForFunction(() => {const el = document.querySelector('.v2-check-photo-preview img'); return el?.complete && el.naturalWidth === 1600;});
  await layout(page, `${label}-saved-photo`, viewer);
  await viewer.getByRole('button', {name: '完成', exact: true}).click(); await viewer.waitFor({state: 'hidden'});
  assert.equal(count(fixture, 'POST', '/photo-preview'), previews + 1);
  assert.equal(count(fixture, 'POST', '/files'), commits + 1, 'exactly one commit');
  const previewRequest = fixture.requests.filter(event => event.path.endsWith('/photo-preview') && event.method === 'POST').at(-1);
  assert.equal(previewRequest.query.task_id, expectedTask); assert.equal(previewRequest.query.item_id, expectedItem);
  assert.equal(previewRequest.query.replace_file_id || '', replaceId);
  if (fixture.tasks[expectedTask].submitted) assert.equal(previewRequest.query.revision, String(oldRevision));
  const saved = fixture.requests.filter(event => event.method === 'POST' && event.path.endsWith('/files')).at(-1);
  assert.equal(saved.task_id, expectedTask); assert.equal(saved.item_id, expectedItem);
  assert.equal(fixture.stages.size, 0); return saved.file_id;
}
async function openCurrent(page) {
  await page.getByRole('button', {name: '修改记录', exact: true}).click();
  const editor = page.locator('.check-record-editor'); await editor.locator('.check-record-item').nth(23).waitFor();
  assert.equal(await editor.getByText('修改检查记录', {exact: true}).count(), 1); await editorReady(page); return editor;
}
async function editorReady(page) {
  await page.waitForFunction(() => [...document.querySelectorAll('.check-record-footer button')]
    .some(button => button.textContent.includes('保存检查结果') && !button.disabled));
}
async function selectAbnormal(row) {
  await row.locator('.v2-check-abnormal').click();
  assert(await row.getByRole('radio', {name: '异常', exact: true}).isChecked(), 'visible label checks the underlying radio');
}
async function reloadConflict(page, editor, draft) {
  await editor.locator('.check-record-item').last().getByRole('textbox').fill(draft);
  await editor.getByRole('button', {name: '保存检查结果', exact: true}).click();
  await editor.getByRole('alert').filter({hasText: '记录版本已变更'}).waitFor();
  assert.equal(await editor.locator('.check-record-item').last().getByRole('textbox').inputValue(), draft);
  await editor.getByRole('button', {name: '重新读取', exact: true}).click();
  await page.getByRole('button', {name: '放弃修改', exact: true}).click();
  await editorReady(page);
}
async function closeEditor(page, editor, discard = false) {
  await editor.getByRole('button', {name: '关闭', exact: true}).click();
  if (discard) await page.getByRole('button', {name: '放弃修改', exact: true}).click();
  await editor.waitFor({state: 'hidden'});
}
async function openHistory(page) {
  await page.getByRole('button', {name: /^(历史检查记录|历史记录)$/}).click();
  const history = page.getByRole('region', {name: '历史检查记录', exact: true});
  await history.locator('.n-collapse-item__header').first().click(); return history;
}
async function pendingCase(page, fixture, label) {
  const rows = page.locator('.v2-check-items fieldset'); await rows.nth(23).waitFor(); assert.equal(await rows.count(), 24);
  const last = rows.last(); assert.match(await last.locator('legend').innerText(), new RegExp(lastItem.text));
  await last.getByRole('button', {name: `${lastItem.text}附件`, exact: true}).click();
  const zone = last.getByRole('group', {name: '检查附件上传区', exact: true});
  await zone.getByRole('button', {name: '上传照片', exact: true}).waitFor();
  await layout(page, `${label}-last-expanded`);
  const fileId = await upload(page, zone, fixture, label, 'current', lastItem.id);
  await last.locator('.v2-check-photo-thumb').waitFor();
  assert.deepEqual(fixture.tasks.current.files.map(file => [file.id, file.item_id]), [[fileId, lastItem.id]]);
  assert.equal(await rows.locator('.v2-check-photo-thumb').count(), 1, 'no cross-item photo');
  assert.equal(await page.evaluate(() => window.__fixtureSubmits), 0, 'photo controls never submit the form');
  assert.equal(fixture.requests.filter(event => event.method === 'PUT' || event.path.endsWith('/submit')).length, 0);
  await layout(page, `${label}-last-uploaded`);
}
async function completedCase(page, fixture, label) {
  let editor = await openCurrent(page);
  let last = editor.locator('.check-record-item').last();
  await selectAbnormal(last);
  await editor.getByRole('button', {name: '保存检查结果', exact: true}).click();
  await editor.getByRole('alert').filter({hasText: '请逐项选择检查结果，并填写异常项备注。'}).waitFor();
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 0, 'abnormal note is validated before PUT');
  const note = last.getByRole('textbox'); await note.fill('版本冲突前的合成草稿');
  fixture.conflictNext = true;
  const reads = count(fixture, 'GET', '/tasks/current');
  await editor.getByRole('button', {name: '保存检查结果', exact: true}).click();
  await editor.getByRole('alert').filter({hasText: '记录已被其他人员修改'}).waitFor();
  assert.equal(await note.inputValue(), '版本冲突前的合成草稿');
  assert(await editor.getByRole('button', {name: '保存检查结果', exact: true}).isDisabled());
  assert.equal(count(fixture, 'GET', '/tasks/current'), reads, '409 does not reload or erase draft');
  await layout(page, `${label}-conflict`, editor);
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 1, '409 never automatically retries');
  await editor.getByRole('button', {name: '重新读取', exact: true}).click();
  await page.getByRole('button', {name: '继续填写', exact: true}).click();
  assert.equal(await note.inputValue(), '版本冲突前的合成草稿');
  await editor.getByRole('button', {name: '重新读取', exact: true}).click();
  await page.getByRole('button', {name: '放弃修改', exact: true}).click();
  await editorReady(page);
  await selectAbnormal(last);
  await note.fill('人工重读后的合成异常说明');
  await editor.getByRole('button', {name: '保存检查结果', exact: true}).click();
  await editor.getByRole('status').filter({hasText: '检查结果已保存'}).waitFor();
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 2);
  assert.equal(fixture.tasks.current.answers.at(-1).note, '人工重读后的合成异常说明');
  assert(fixture.tasks.current.answers.slice(0, -1).every(answer => answer.status === 'normal' && !answer.note));
  const savedAnswers = clone(fixture.tasks.current.answers);
  await note.fill('附件保存时仍未提交的草稿');
  const fileId = await upload(page, last.getByRole('group', {name: '检查附件上传区', exact: true}), fixture, `${label}-edit`, 'current', lastItem.id);
  await editor.getByRole('status').filter({hasText: '附件已保存'}).waitFor();
  await last.getByRole('button', {name: '上传照片', exact: true}).waitFor();
  assert.equal(await note.inputValue(), '附件保存时仍未提交的草稿');
  assert.deepEqual(fixture.tasks.current.answers, savedAnswers, 'photo commit does not save answer draft');
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 2);
  await closeEditor(page, editor, true);
  assert(fixture.tasks.current.files.some(file => file.id === fileId), 'canceling answer edits keeps saved attachments');
  editor = await openCurrent(page); last = editor.locator('.check-record-item').last();
  assert.equal(await last.getByRole('textbox').inputValue(), '人工重读后的合成异常说明');
  const file = fixture.tasks.current.files.find(file => file.id === fileId), revision = fixture.tasks.current.revision;
  await last.getByRole('button', {name: `移除${file.name}`, exact: true}).click();
  await page.getByRole('button', {name: '移除', exact: true}).click();
  await page.getByRole('button', {name: '移除', exact: true}).waitFor({state: 'hidden'});
  await last.locator('.v2-check-photo-thumb').waitFor({state: 'hidden'});
  await editor.getByRole('status').filter({hasText: '附件已保存'}).waitFor();
  const deleted = fixture.requests.find(event => event.method === 'DELETE' && event.path.endsWith(`/${fileId}`));
  assert.deepEqual(deleted.query, {task_id: 'current', revision: String(revision)});
  assert.equal(fixture.tasks.current.files.length, 0);
  await editorReady(page);
  const acknowledged = await upload(page, last.getByRole('group', {name: '检查附件上传区', exact: true}),
    fixture, `${label}-remote-delete`, 'current', lastItem.id);
  await editorReady(page);
  assert(fixture.tasks.current.files.some(file => file.id === acknowledged));
  fixture.tasks.current.files = []; bump(fixture.tasks.current);
  await reloadConflict(page, editor, '远端删除附件后的本地草稿');
  assert.equal(await last.locator('.v2-check-photo-thumb').count(), 0, 'acknowledged local receipt cannot resurrect remotely deleted photo');
  assert.deepEqual(fixture.tasks.current.answers, savedAnswers);
  const oldHandover = fixture.tasks.current.handover_photo.id;
  const handover = editor.locator('.check-record-handover');
  await handover.getByRole('button', {name: '替换照片', exact: true}).waitFor();
  const replacement = await upload(page, handover, fixture, `${label}-replace`, 'current', '', oldHandover);
  await handover.getByRole('button', {name: '替换照片', exact: true}).waitFor();
  assert.equal(fixture.tasks.current.handover_photo.id, replacement);
  assert.equal(await handover.locator('.v2-check-photo-thumb').count(), 1, 'replacement does not duplicate handover');
  assert.deepEqual(fixture.tasks.current.answers, savedAnswers);
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 3, 'only the deliberate stale answer attempt was added');
  await layout(page, `${label}-replaced`, editor);
  await editorReady(page);
  fixture.tasks.current.handover_photo = {...fixture.tasks.current.handover_photo,
    id: 'remote-handover', name: '服务端当前交接班照片.jpg'};
  bump(fixture.tasks.current);
  await reloadConflict(page, editor, '远端替换照片后的本地草稿');
  assert.equal(await handover.locator('.v2-check-photo-thumb').count(), 1);
  assert.match(await handover.locator('.v2-check-photo-thumb img').getAttribute('src'), /\/remote-handover$/);
  assert.equal(await handover.getByRole('link', {name: '服务端当前交接班照片.jpg', exact: true}).count(), 1,
    'only the server replacement is shown, not the acknowledged local receipt');
  await layout(page, `${label}-remote-current`, editor);
  await closeEditor(page, editor);
  assert.equal(fixture.tasks.current.handover_photo.id, 'remote-handover');
  const history = await openHistory(page);
  await history.getByRole('button', {name: '修改记录', exact: true}).click();
  editor = page.locator('.check-record-editor'); await editor.locator('.check-record-item').nth(23).waitFor();
  assert.equal(count(fixture, 'GET', '/tasks/history'), 1, 'history opens the same editor using its own task');
  const historyLast = editor.locator('.check-record-item').last();
  await selectAbnormal(historyLast);
  await historyLast.getByRole('textbox').fill('历史记录合成备注');
  await editor.getByRole('button', {name: '保存检查结果', exact: true}).click();
  await editor.getByRole('status').filter({hasText: '检查结果已保存'}).waitFor();
  assert.equal(fixture.tasks.history.answers.at(-1).note, '历史记录合成备注');
  assert.deepEqual(fixture.tasks.current.answers, savedAnswers, 'history does not mutate current task');
  await layout(page, `${label}-history-edit`, editor); await closeEditor(page, editor);
  assert.equal(await page.evaluate(() => window.__fixtureSubmits), 0);
}
async function readonlyCase(page, fixture, label) {
  await page.getByText('本次已完成', {exact: true}).waitFor();
  assert.equal(await page.getByRole('button', {name: '修改记录', exact: true}).count(), 0);
  const history = await openHistory(page); await history.getByText('原交接班照片.jpg', {exact: true}).waitFor();
  assert.equal(await page.getByRole('button', {name: '修改记录', exact: true}).count(), 0);
  assert.equal(await page.getByRole('button', {name: /^(上传照片|替换照片|保存检查结果)$/}).count(), 0);
  assert.equal(await page.locator('input[type=file]:enabled').count(), 0, 'no enabled hidden uploader');
  assert.equal(fixture.requests.filter(event => event.method !== 'GET').length, 0, 'readonly makes no hidden write requests');
  await layout(page, `${label}-readonly`);
}
async function missingHandoverCase(page, fixture, label) {
  await page.getByText('本次已完成', {exact: true}).waitFor();
  const answers = clone(fixture.tasks.current.answers);
  const fileId = await upload(page, page.locator('.v2-check-handover'), fixture, label, 'current', '');
  await page.getByText('本班已上传', {exact: true}).waitFor();
  assert.equal(fixture.tasks.current.handover_photo.id, fileId);
  assert.equal(await page.locator('.check-record-editor').count(), 0, 'first completed handover uses original main control');
  assert.equal(count(fixture, 'GET', '/tasks/current'), 0);
  assert.equal(count(fixture, 'PUT', '/tasks/current'), 0);
  assert.deepEqual(fixture.tasks.current.answers, answers);
  assert.equal(await page.locator('.v2-check-handover').getByRole('button', {name: /^(上传照片|替换照片)$/}).count(), 0);
  assert.equal(await page.evaluate(() => window.__fixtureSubmits), 0);
  await layout(page, `${label}-main-uploaded`);
}

let browser;
try {
  if (process.env.PREVIEW_ONLY !== '1') {
    browser = await chromium.launch({headless: true});
    for (const legacy of [false, true]) for (const viewport of [
      {width: 402, height: 874}, {width: 440, height: 956}, {width: 1440, height: 900}, {width: 360, height: 640},
    ]) for (const scenario of ['pending', 'completed', 'completed-missing', 'readonly']) {
      const label = `${legacy ? 'legacy' : 'v2'}-${viewport.width}x${viewport.height}-${scenario}`;
      if (process.env.CHECK_CASE && process.env.CHECK_CASE !== label) continue;
      const context = await browser.newContext({viewport, reducedMotion: 'reduce', timezoneId: 'Asia/Shanghai', serviceWorkers: 'block'});
      const errors = [], external = [];
      await context.route('**/*', route => {
        if (new URL(route.request().url()).origin !== origin) {external.push(route.request().url()); return route.abort();}
        return route.continue();
      });
      const page = await context.newPage(); page.setDefaultTimeout(12000); page.on('pageerror', error => errors.push(error.message));
      const params = new URLSearchParams({fixture: label, scenario: scenario === 'readonly' ? 'completed' : scenario,
        role: scenario === 'readonly' ? 'readonly' : 'editor'});
      if (legacy) params.set('legacy', '');
      const fixture = fixtureFor(params);
      try {
        await page.goto(`${origin}/?${params}`);
        await ({pending: pendingCase, completed: completedCase, 'completed-missing': missingHandoverCase, readonly: readonlyCase}[scenario])(page, fixture, label);
        assert.deepEqual(errors, []); assert.deepEqual(external, []);
        assert.deepEqual(report.apiErrors.filter(error => error.fixture === label), []);
        assert.deepEqual(report.layoutIssues.filter(issue => issue.label.startsWith(label + '-')), [], 'layout assertions');
        report.cases.push(label); console.log(`PASS ${label}`);
      } catch (error) {
        await page.screenshot({path: path.join(output, `${label}-failure.png`), fullPage: true}).catch(() => {});
        report.failures.push({case: label, error: error.stack, browserErrors: errors, external}); console.error(`FAIL ${label}: ${error.message}`);
      } finally {
        await context.close();
        await writeFile(path.join(output, 'report.json'), JSON.stringify(report, null, 2));
      }
    }
    assert(report.cases.length + report.failures.length > 0, 'CHECK_CASE must match a matrix case');
  }
} finally {
  await browser?.close();
  await writeFile(path.join(output, 'report.json'), JSON.stringify(report, null, 2));
  if (keepPreview) {
    console.log(`KEEP_PREVIEW ready: ${report.preview}\nReport: ${path.join(output, 'report.json')}`);
    await new Promise(resolve => {process.once('SIGINT', resolve); process.once('SIGTERM', resolve);});
  }
  await server.close();
}
console.log(`${report.cases.length} passed, ${report.failures.length} failed; synthetic HTTP only; official dist unchanged.`);
if (report.failures.length || report.apiErrors.length || report.layoutIssues.length) process.exitCode = 1;
