import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { createServer as createViteServer } from 'vite';
import vuePlugin from '@vitejs/plugin-vue';

const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(scriptsDir, '..');
const projectRoot = path.resolve(frontendRoot, '../../..');
const componentsDir = path.join(frontendRoot, 'src/components');
const componentFile = 'LighthouseCabinetTextCreate.vue';

const output = path.join(projectRoot, 'output/playwright/assistant-text-create');
const vuePkg = path.join(frontendRoot, 'node_modules/vue');
const lucidePkg = path.join(frontendRoot, 'node_modules/lucide-vue-next');
await mkdir(output, { recursive: true });

const entry = `
import { createApp, h, reactive } from 'vue'
import LighthouseCabinetTextCreate from '@components/${componentFile}'

let app = null
let emitted = []
const state = reactive({ field: { actions: [], scopes: [] }, value: {}, disabled: false, planId: 'plan-1', planVersion: 1 })

function mount() {
  if (app) app.unmount()
  emitted = []
  const el = document.getElementById('app')
  app = createApp({
    setup() {
      return () => h(LighthouseCabinetTextCreate, {
        id: 'text-create',
        field: state.field,
        modelValue: state.value,
        disabled: state.disabled,
        planId: state.planId,
        planVersion: state.planVersion,
        'onUpdate:modelValue': (v) => {
          emitted.push(JSON.parse(JSON.stringify(v)))
          state.value = JSON.parse(JSON.stringify(v))
        },
      })
    },
  })
  app.mount(el)
}

window.__mount = (fixture) => {
  state.field = fixture.field
  state.value = fixture.value || {}
  state.disabled = !!fixture.disabled
  state.planId = fixture.planId || 'plan-1'
  state.planVersion = fixture.planVersion ?? 1
  mount()
}
window.__setModel = (v) => { state.value = JSON.parse(JSON.stringify(v)) }
window.__setDisabled = (d) => { state.disabled = !!d }
window.__getState = () => ({
  emitted,
  value: JSON.parse(JSON.stringify(state.value)),
  latest: emitted.length ? emitted[emitted.length - 1] : JSON.parse(JSON.stringify(state.value)),
})
window.__mount((window.__initialFixture) || { field: { actions: [], scopes: [] }, value: {} })
`;

const html = `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1" /><title>LighthouseCabinetTextCreate</title><style>html,body,#app{margin:0;min-height:100%}body{background:#fff}</style></head><body><div id="app"></div><script type="module" src="/main.mts"></script></body></html>`;

await writeFile(path.join(output, 'index.html'), html);
await writeFile(path.join(output, 'main.mts'), entry);

const server = await createViteServer({
  root: output,
  configFile: false,
  plugins: [vuePlugin()],
  resolve: {
    alias: [
      { find: 'vue', replacement: vuePkg },
      { find: 'lucide-vue-next', replacement: lucidePkg },
      { find: '@components', replacement: componentsDir },
    ],
  },
  server: {
    host: '127.0.0.1',
    port: 0,
    fs: { allow: [frontendRoot, output, projectRoot] },
  },
  appType: 'spa',
});
await server.listen();
const address = server.httpServer?.address();
const port = typeof address === 'object' && address ? address.port : 5173;
const base = `http://127.0.0.1:${port}`;

const actions = ['上正式电', '上测试电', '测试电转正式电', '正式电转测试电', '下正式电', '下测试电'];
const field = {
  name: 'step_cabinet_text_create',
  actions,
  scopes: ['E', 'A'],
};
const fixture = { field, value: {}, disabled: false, planId: 'plan-1', planVersion: 1 };

/* 模拟后端：把每行 scope|room|rack|action|expected|actual|result 解析成机柜记录 */
function rowsForSource(source) {
  const lines = String(source.text || '').split('\n').map((l) => l.trimEnd()).filter(Boolean);
  return lines.map((line, idx) => {
    const parts = line.split('|');
    const [, , rack, action = '上正式电', expected = '2026-10-02 10:00:00', actual = '2026-10-02 09:00:00', result = '成功'] = parts;
    const scope = parts[0] || 'E';
    const room = parts[1] || '202';
    return {
      text_id: source.id,
      text_row: idx,
      scope: String(scope),
      room: String(room),
      rack: String(rack),
      action: String(action),
      expected: String(expected),
      actual: String(actual),
      result: String(result),
      failure_reason: result === '失败' ? '原因待补充' : '',
      supplier_rack: String(rack) + '-S',
      rack_type: '网络机柜',
      type_detail: '',
      type_resolution: '',
      row_id: `row-${source.id}-${idx}`,
      issues: [],
    };
  });
}

const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    await context.addInitScript((fixtureData) => {
      window.__initialFixture = fixtureData;
    }, fixture);
    const page = await context.newPage();
    const errors = [];
    const nativeRequests = [];
    const previewBodies = [];
    page.on('pageerror', (err) => errors.push(err.message));
    page.on('console', (msg) => {
      if (msg.type() === 'error' && !msg.text().startsWith('Failed to load resource')) errors.push(msg.text());
    });
    const reqFailures = [];
    page.on('requestfailed', (req) => reqFailures.push(`${req.method()} ${req.url()} :: ${req.failure()?.errorText}`));

    /* 通用 api 拦截：仅匹配服务器真 API（pathname 以 /api/ 开头，避免命中 @fs 源码路径） */
    await page.route((url) => new URL(url).pathname.startsWith('/api/'), (route) => {
      const req = route.request();
      nativeRequests.push({ method: req.method(), url: req.url() });
      if (req.method() !== 'GET') return route.fulfill({ status: 418, json: { error: 'forbidden business write' } });
      return route.fulfill({ json: { ok: true, data: [] } });
    });

    /* 专用预览路由（后注册优先）：仅允许 cabinet-text-preview POST */
    await page.route((url) => new URL(url).pathname.endsWith('/cabinet-text-preview'), async (route) => {
      const req = route.request();
      if (req.method() !== 'POST') return route.fulfill({ status: 405, json: { error: 'method not allowed' } });
      const body = req.postDataJSON();
      previewBodies.push(body);
      const brokenDetected = body.sources.some((s) => String(s.text || '').includes('BROKEN'));
      const slowDetected = body.sources.some((s) => String(s.text || '').includes('SLOW'));
      assert.equal(body.version, fixture.planVersion, '预览必须携带 planVersion');
      assert.equal(body.field, fixture.field.name, '预览必须携带 field.name');
      assert.ok(Array.isArray(body.sources) && body.sources.length > 0, '预览必须携带 sources');
      if (brokenDetected) {
        await new Promise((r) => setTimeout(r, 60));
        return route.fulfill({ status: 400, json: { ok: false, error: '识别失败：内容不完整' } });
      }
      const allRows = [];
      for (const source of body.sources) allRows.push(...rowsForSource(source));
      await new Promise((r) => setTimeout(r, slowDetected ? 400 : 120));
      return route.fulfill({ json: { ok: true, data: { rows: allRows } } });
    });

    await page.goto(base);
    try {
      await page.waitForFunction(() => typeof window.__mount === 'function', null, { timeout: 5000 });
    } catch {
      console.error('entry did not initialize. errors=', errors, 'reqFailures=', reqFailures, 'nativeRequests=', nativeRequests);
      throw new Error('window.__mount unavailable');
    }

    const state = () => page.evaluate(() => window.__getState());
    const mount = (fixtureData) => page.evaluate((data) => window.__mount(data), fixtureData);
    const setModel = (v) => page.evaluate((data) => window.__setModel(data), v);
    const setDisabled = (d) => page.evaluate((data) => window.__setDisabled(!!data), d);

    const editor = page.locator('[aria-label="粘贴机柜确认文本"]');
    const refreshBtn = page.getByRole('button', { name: '重新核对' });
    const rowsLoc = page.locator('.rows-table tbody tr');
    const waitRows = (n) => page.waitForFunction((count) => document.querySelectorAll('.rows-table tbody tr').length === count, n);

    const paste = async (pageHandle, text) => {
      await pageHandle.locator('[aria-label="粘贴机柜确认文本"]').evaluate((el, value) => {
        const clip = new DataTransfer();
        clip.setData('text/plain', value);
        el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: clip, bubbles: true, cancelable: true }));
      }, text);
    };
    const line = (rack, action, result) => `E|202|${rack}|${action}|2026-10-02 10:00:00|2026-10-02 09:00:00|${result}`;

    /* ---------- 场景1：多次粘贴累积 + 失败保留文本 + 识别中禁用提交 ---------- */
    await mount(fixture);
    await paste(page, `${line('A11','上正式电','成功')}\n${line('A12','下测试电','失败')}`);
    await page.locator('text=识别中，请稍候').waitFor();
    assert.equal((await state()).latest.rows.length, 0, '识别中必须发出空 rows 以禁用父级提交');
    await waitRows(2);
    assert.equal(await editor.inputValue(), '', '成功后应清空输入框');

    await paste(page, line('B21','上测试电','成功'));
    await waitRows(3);
    assert.equal((await state()).latest.rows.length, 3, '多次粘贴应累积');
    assert.equal(await page.locator('.table-scroll').isVisible(), true, '有记录时显示表格');

    const brokenText = 'BROKEN 内容不完整';
    await paste(page, brokenText);
    const brokenError = page.getByRole('alert');
    await brokenError.waitFor({ timeout: 5000 });
    assert.match(await brokenError.innerText(), /识别失败：内容不完整/, '失败应显示识别错误提示');
    assert.equal(await editor.inputValue(), brokenText, '失败时应保留输入文本');
    assert.equal(await rowsLoc.count(), 3, '失败不应新增记录');

    /* ---------- 场景2：25 行分页（含上一页/下一页 disabled） ---------- */
    await mount(fixture);
    const many = Array.from({ length: 30 }, (_, i) => `E|202|R${String(i).padStart(2, '0')}|上正式电|2026-10-02 10:00:00|2026-10-02 09:00:00|成功`).join('\n');
    await paste(page, many);
    await page.waitForFunction((n) => document.querySelector('.count')?.textContent?.includes(`全部 ${n} 条`), 30);
    await page.locator('.pagination').waitFor();
    assert.equal(await rowsLoc.count(), 25, '第一页应显示 25 条');
    assert.match(await page.locator('.pagination').innerText(), /1 \/ 2/, '应显示第 1 / 2 页');
    assert.equal(await page.getByRole('button', { name: '上一页' }).isDisabled(), true, '第一页上一页应禁用');
    assert.equal(await page.getByRole('button', { name: '下一页' }).isDisabled(), false, '第一页下一页应可用');
    await page.getByRole('button', { name: '下一页' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.rows-table tbody tr').length === 5);
    assert.match(await page.locator('.pagination').innerText(), /2 \/ 2/, '第二页应显示第 2 / 2 页');
    assert.equal(await page.getByRole('button', { name: '下一页' }).isDisabled(), true, '最后一页下一页应禁用');
    assert.equal(await page.getByRole('button', { name: '上一页' }).isDisabled(), false, '第二页上一页应可用');
    await page.getByRole('button', { name: '上一页' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.rows-table tbody tr').length === 25);

    /* ---------- 场景3：操作/时间编辑 + 结果失败必填失败原因 ---------- */
    await mount(fixture);
    await paste(page, line('C31','上正式电','成功'));
    await waitRows(1);
    const actionSel = page.getByLabel('操作类型 C31');
    await actionSel.selectOption('下测试电');
    const expectedInput = page.getByLabel('期望完成时间 C31');
    const actualInput = page.getByLabel('实际完成时间 C31');
    assert.equal(await expectedInput.getAttribute('step'), '1', '时间为秒精度 step=1');
    await expectedInput.fill('2026-10-02T13:05:32');
    await actualInput.fill('2026-10-02T13:10:32');
    let s = await state();
    assert.equal(s.latest.rows[0].action, '下测试电', '操作类型可编辑');
    assert.equal(s.latest.rows[0].expected, '2026-10-02 13:05:32', '期望时间秒精度保留');
    assert.equal(s.latest.rows[0].actual, '2026-10-02 13:10:32', '实际时间秒精度保留');

    const resultSel = page.getByLabel('结果 C31');
    await resultSel.selectOption('失败');
    const failureInput = page.getByLabel('失败原因 C31');
    assert.notEqual(await failureInput.getAttribute('required'), null, '失败原因应必填');
    await failureInput.fill('隔离测试失败原因');
    s = await state();
    assert.equal(s.latest.rows[0].result, '失败', '结果可编辑');
    assert.equal(s.latest.rows[0].failure_reason, '隔离测试失败原因', '失败原因可编辑');

    /* ---------- 场景4：删除 + 卸载重挂 + 再粘贴：被删行不复活，保留编辑 ---------- */
    await mount(fixture);
    await paste(page, `${line('D41','上正式电','成功')}\n${line('D42','上测试电','成功')}`);
    await waitRows(2);
    await page.getByRole('button', { name: '移除记录 D41' }).click();
    await waitRows(1);
    s = await state();
    assert.equal(s.latest.rows.length, 1, '删除单行生效');
    assert.equal(s.latest.rows[0].rack, 'D42', '剩余记录应为未删除行');

    await page.getByLabel('操作类型 D42').selectOption('下正式电');
    const retained = (await state()).latest;
    assert.equal(retained.rows[0].action, '下正式电', '编辑应已保留在 modelValue');
    retained.sources[0].$query = { ref: 'query_form_frozen_paste', path: ['sources', '0'] };

    /* 卸载重挂：以明确保留的 sources+rows 重新挂载（模拟父级失去 hidden） */
    await mount({ field: fixture.field, value: retained, disabled: false, planId: 'plan-1', planVersion: 1 });
    await waitRows(1);

    /* 再粘贴新来源：不得把被删的 D41 加回来，也不得丢手动编辑 */
    await paste(page, line('D51','上正式电','成功'));
    await waitRows(2);
    s = await state();
    assert.equal(s.latest.rows.length, 2, '重挂后再粘贴不得复活被删行');
    const racks = s.latest.rows.map(r => r.rack).sort();
    assert.deepEqual(racks, ['D42', 'D51'], '仅应保留剩余行 + 新来源行');
    assert.equal(s.latest.rows.find(r => r.rack === 'D42').action, '下正式电', '原编辑应保留');
    assert.deepEqual(s.latest.sources[0].$query, retained.sources[0].$query, '返回修改的大段粘贴须保留完整来源引用');

    /* 删除某次最后一行时移除其 source：空 rows 不再被当作未解析来源 */
    await page.getByRole('button', { name: '移除记录 D51' }).click();
    await page.getByRole('button', { name: '移除记录 D42' }).click();
    await waitRows(0);
    s = await state();
    assert.equal(s.latest.sources.length, 0, '删光某次最后一行后其 source 应一并移除');
    assert.equal(s.latest.rows.length, 0, '空 rows 不代表未解析（来源已移除）');

    /* ---------- 场景5：整次粘贴删除（来源下拉）+ 删除来源 aria 用人类标签 ---------- */
    await mount(fixture);
    await paste(page, line('E51','上正式电','成功'));
    await waitRows(1);
    await page.locator('#text-create-source').click();
    await page.getByRole('option', { name: '第 1 次 · 1 条 · E楼 202', exact: true }).click();
    const delSourceBtn = page.locator('.source-picker .icon-button');
    const delAria = await delSourceBtn.getAttribute('aria-label');
    assert.match(delAria, /第 1 次/, '删除来源 aria-label 应用人类标签');
    assert.ok(!delAria.includes('src-'), '删除来源 aria-label 不应含内部 sourceFilter id');
    await delSourceBtn.click();
    await waitRows(0);
    s = await state();
    assert.equal(s.latest.rows.length, 0, '整次粘贴删除应清空记录');

    await paste(page, line('E51','上正式电','成功'));
    await waitRows(1);
    await paste(page, line('E52','上正式电','成功'));
    await waitRows(2);
    await page.locator('#text-create-source').click();
    await page.getByRole('option', { name: '第 1 次 · 1 条 · E楼 202', exact: true }).click();
    await page.getByRole('button', { name: '移除记录 E51', exact: true }).click();
    await page.getByRole('button', { name: '移除记录 E52', exact: true }).waitFor();
    assert.equal((await state()).latest.rows.length, 1, '移除当前来源最后一行后应恢复显示剩余来源');

    /* ---------- 场景6：父级回填 model（删改后恢复/留痕），不循环 emit ---------- */
    await mount(fixture);
    await setModel({
      sources: [{ id: 'src-parent', text: '父级文本' }],
      rows: [
        { text_id: 'src-parent', text_row: 0, scope: 'E', room: '202', rack: 'P01', action: '下正式电', expected: '2026-10-02 10:00:00', actual: '2026-10-02 10:20:00', result: '成功', failure_reason: '', supplier_rack: 'SS', rack_type: '网络机柜', type_detail: '', type_resolution: '' },
        { text_id: 'src-parent', text_row: 1, scope: 'A', room: '101', rack: 'P02', action: '上测试电', expected: '', actual: '', result: '', failure_reason: '', supplier_rack: '', rack_type: '', type_detail: '', type_resolution: '' },
      ],
    });
    await waitRows(2);
    s = await state();
    const before = s.emitted.length;
    assert.equal(s.latest.rows.length, 2, '父级回填应呈现两条记录');
    assert.equal(await page.getByLabel('操作类型 P01').inputValue(), '下正式电', '父级回填的操作应反映');
    await new Promise((r) => setTimeout(r, 250));
    s = await state();
    assert.ok(s.emitted.length <= before + 1, '父级回填不应循环 emit（最多同步一次）');

    /* ---------- 场景7：禁用手态与整页无横向溢出 ---------- */
    await mount(fixture);
    await setModel({
      sources: [{ id: 'src-disabled', text: '文本' }],
      rows: [{ text_id: 'src-disabled', text_row: 0, scope: 'E', room: '202', rack: 'Q01', action: '上正式电', expected: '', actual: '', result: '', failure_reason: '', supplier_rack: '', rack_type: '', type_detail: '', type_resolution: '' }],
    });
    await waitRows(1);
    await setDisabled(true);
    assert.equal(await editor.isDisabled(), true, '禁用状态下文本文本域禁用');
    assert.equal(await page.getByLabel('操作类型 Q01').isDisabled(), true, '禁用状态下行编辑禁用');
    assert.equal(await page.getByRole('button', { name: '重新核对' }).count(), 0, '已物化来源无需重新核对');
    await page.locator('.lh-text-create').first().waitFor();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true, '整页不应横向溢出（table 内部滚动）');
    await setDisabled(false);

    /* ---------- 场景8：初次 modelValue sources 自动识别（不创建业务） ---------- */
    await mount({
      field, planId: 'plan-1', planVersion: 1, disabled: false,
      value: {
        sources: [{ id: 'src-init', text: `${line('I01','上正式电','成功')}\n${line('I02','下测试电','失败')}` }],
        rows: [],
      },
    });
    await page.locator('text=识别中，请稍候').waitFor();
    await waitRows(2);
    s = await state();
    assert.equal(s.latest.rows.length, 2, '初始 sources 应自动识别出识别行');
    assert.equal(s.latest.rows[0].rack, 'I01', '自动识别应填充来源行');
    assert.equal(s.latest.rows[1].rack, 'I02', '自动识别应填充全部来源行');
    assert.equal(await page.getByRole('button', { name: '重新核对' }).count(), 0, '识别完成后该来源已物化，不再显示重新核对');

    /* ---------- 场景9：识别中外部替换 model：旧响应不得覆盖新值/焦点/emit ---------- */
    await mount(fixture);
    const slowText = `${line('S91','上正式电','成功')} SLOW`;
    await paste(page, slowText);
    await page.locator('text=识别中，请稍候').waitFor();
    await setModel({
      sources: [{ id: 'src-ext', text: '外部替换' }],
      rows: [{ text_id: 'src-ext', text_row: 0, scope: 'E', room: '101', rack: 'EXT01', action: '上测试电', expected: '', actual: '', result: '', failure_reason: '', supplier_rack: '', rack_type: '', type_detail: '', type_resolution: '' }],
    });
    await waitRows(1);
    await new Promise((r) => setTimeout(r, 500)); /* 等待旧 SLOW 响应返回 */
    s = await state();
    assert.equal(s.latest.rows.length, 1, '旧响应不得覆盖外部替换后的值');
    assert.equal(s.latest.rows[0].rack, 'EXT01', '仍应保持外部替换的明确保留记录');
    assert.equal(await page.getByRole('button', { name: '重新核对' }).count(), 0, '外部替换后来源已物化，不重新核对');
    assert.equal(await page.locator('text=识别中，请稍候').count(), 0, '外部替换后识别中状态应清除');

    /* ---------- 场景10：识别中卸载：旧响应不得写入新挂载 / 无运行错误 ---------- */
    await mount(fixture);
    await paste(page, `${line('U1','上正式电','成功')} SLOW`);
    await page.locator('text=识别中，请稍候').waitFor();
    await mount({ field: fixture.field, value: { sources: [], rows: [] }, disabled: false, planId: 'plan-1', planVersion: 1 });
    await new Promise((r) => setTimeout(r, 500)); /* 等待旧响应返回 */
    s = await state();
    assert.equal(s.latest.rows.length, 0, '卸载后旧响应不得写入新挂载');
    assert.equal(await rowsLoc.count(), 0, '新挂载不应出现残留行');

    /* ---------- 无业务写入检查 ---------- */
    for (const req of nativeRequests) {
      assert.equal(req.method, 'GET', '不应产生除预览外的业务请求');
    }
    for (const body of previewBodies) {
      assert.ok(body.field, '预览请求必须带 field');
      assert.ok(Array.isArray(body.sources), '预览请求必须带 sources');
    }
    assert.deepEqual(errors, [], `页面无运行时错误 (${width}px)`);
    assert.deepEqual(reqFailures, [], `无请求失败 (${width}px)`);

    await page.screenshot({ path: path.join(output, `text-create-${width}.png`), fullPage: true });
    await context.close();
    console.log(`LighthouseCabinetTextCreate ${width}px: all scenarios passed`);
  }
  console.log('LighthouseCabinetTextCreate isolated-vite playwright suite OK');
} finally {
  await browser.close();
  await server.close();
}
