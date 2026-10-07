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
const componentFile = 'LighthouseCabinetProof.vue';

const output = path.join(projectRoot, 'output/playwright/assistant-proof');
const vuePkg = path.join(frontendRoot, 'node_modules/vue');
const lucidePkg = path.join(frontendRoot, 'node_modules/lucide-vue-next');
await mkdir(output, { recursive: true });

/* 隔离 Vite 临时入口：onUpdate 同时回填 state.value 作为真实 v-model */
const entry = `
import { createApp, h, reactive } from 'vue'
import LighthouseCabinetProof from '@components/${componentFile}'

let app = null
let emitted = []
let loadOptions = []
const state = reactive({ field: { images: [], rows: [], actions: [] }, value: {}, disabled: false })

function mount() {
  if (app) app.unmount()
  emitted = []
  loadOptions = []
  const el = document.getElementById('app')
  app = createApp({
    setup() {
      return () => h(LighthouseCabinetProof, {
        id: 'proof',
        field: state.field,
        modelValue: state.value,
        disabled: state.disabled,
        'onUpdate:modelValue': (v) => {
          emitted.push(JSON.parse(JSON.stringify(v)))
          state.value = JSON.parse(JSON.stringify(v))
        },
        'onLoad-options': (payload) => {
          loadOptions.push(JSON.parse(JSON.stringify(payload)))
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
  mount()
}
window.__setModel = (v) => { state.value = JSON.parse(JSON.stringify(v)) }
window.__setDisabled = (d) => { state.disabled = !!d }
window.__setField = (f) => { state.field = f }
window.__getState = () => ({
  emitted,
  loadOptions,
  value: JSON.parse(JSON.stringify(state.value)),
  latest: emitted.length ? emitted[emitted.length - 1] : JSON.parse(JSON.stringify(state.value)),
})
window.__mount((window.__initialFixture) || { field: { images: [], rows: [], actions: [] }, value: {} })
`;

const html = `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1" /><title>LighthouseCabinetProof</title><style>html,body,#app{margin:0;min-height:100%}body{background:#fff}</style></head><body><div id="app"></div><script type="module" src="/main.mts"></script></body></html>`;

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

const PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=',
  'base64'
);

/* 真实六种操作：上正式电、上测试电、测试电转正式电、正式电转测试电、下正式电、下测试电 */
const actions = ['上正式电', '上测试电', '测试电转正式电', '正式电转测试电', '下正式电', '下测试电'];
const rows = [
  {
    row_id: 'row-A11', label: 'E楼202/A11', scope: 'E', room: '202', rack: 'A11',
    fields: { action: '', expected: '', actual: '', supplier_rack: '', result: '成功', failure_reason: '' },
  },
  {
    row_id: 'row-A12', label: 'E楼202/A12', scope: 'E', room: '202', rack: 'A12',
    fields: { action: '下测试电', expected: '2026-10-02 08:00:00', actual: '2026-10-02 08:30:00', supplier_rack: 'S-OLD', result: '成功', failure_reason: '' },
  },
];
const field = {
  actions,
  images: [
    {
      image_id: 'img-1', name: '截图A', url: '/api/cabinet-power/batches/b1/images/img-1', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-1',
      candidates: [
        { index: 0, label: '识别A-A11', scope: 'E', room: '202', rack: 'A11', row_id: 'row-A11', fields: { action: '上正式电', expected: '2026-10-02 09:00:00', actual: '', supplier_rack: 'S-A11' } },
        { index: 1, label: '识别A-A12', scope: 'E', room: '202', rack: 'A12', row_id: 'row-A12', fields: { action: '上测试电', expected: '2026-10-02 10:00:00', actual: '2026-10-02 10:05:00', supplier_rack: '' } },
      ],
    },
    {
      image_id: 'img-2', name: '截图B', url: '/api/cabinet-power/batches/b1/images/img-2', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-2',
      candidates: [
        { index: 0, label: '识别B-A11', scope: 'E', room: '202', rack: 'A11', row_id: 'row-A11', fields: { action: '下正式电', expected: '2026-10-02 11:00:00', actual: '', supplier_rack: '' } },
        { index: 1, label: '识别B-X17', scope: 'A', room: '101', rack: 'X17', row_id: 'row-X17', fields: { action: '测试电转正式电', expected: '2026-10-02 12:00:00', actual: '2026-10-02 12:10:00', supplier_rack: 'S-X' } },
      ],
    },
  ],
  rows,
};
const fixture = { field, value: {}, disabled: false };

/* 同名图片 + 同名候选：验证标签唯一、不内部 id */
const dupField = {
  actions,
  images: [
    {
      image_id: 'img-x', name: 'A', url: '/api/cabinet-power/batches/b1/images/img-x', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-x',
      candidates: [
        { index: 0, label: '同名', scope: 'E', room: '202', rack: 'A11', row_id: 'row-A11', fields: {} },
        { index: 1, label: '同名', scope: 'E', room: '202', rack: 'A11', row_id: 'row-A11', fields: {} },
      ],
    },
    { image_id: 'img-y', name: 'A', url: '/api/cabinet-power/batches/b1/images/img-y', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-y', candidates: [] },
    { image_id: 'img-z', name: 'A', url: '/api/cabinet-power/batches/b1/images/img-z', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-z', candidates: [] },
  ],
  rows,
};

/* 无 actions + 恶意/异常 URL */
const badField = {
  actions: [],
  images: [
    { image_id: 'img-bad', name: '坏图', url: 'https://evil.example.com/x.png', thumbnail_url: 'data:image/png;base64,AAAA' },
    { image_id: 'img-hash', name: '带锚点', url: '/api/cabinet-power/batches/b1/images/img-hash#frag', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-hash?x=2' },
    { image_id: 'img-thumb', name: '合法缩略', url: '/api/cabinet-power/batches/b1/images/img-thumb?thumbnail=1', thumbnail_url: '/api/cabinet-power/batches/b1/images/img-thumb?thumbnail=1' },
  ],
  rows,
};

/* 补全模式：native_cabinet_correct=true，仅未关联候选 */
const completionRows = [
  {
    row_id: 'dir-E-A11', label: 'E楼202/A11', scope: 'E', room: '202', rack: 'A11', rack_type: '整柜',
    fields: { action: '', expected: '', actual: '', supplier_rack: '', result: '', failure_reason: '', type_detail: '' },
  },
  {
    row_id: 'dir-E-A12', label: 'E楼202/A12', scope: 'E', room: '202', rack: 'A12', rack_type: '半柜',
    fields: { action: '', expected: '', actual: '', supplier_rack: '', result: '', failure_reason: '', type_detail: '' },
  },
  {
    row_id: 'dir-A-X17', label: 'A楼101/X17', scope: 'A', room: '101', rack: 'X17', rack_type: '整柜',
    fields: { action: '', expected: '', actual: '', supplier_rack: '', result: '', failure_reason: '', type_detail: '' },
  },
];
const completionField = {
  native_cabinet_correct: true,
  scopes: ['E', 'A'],
  directory_scope: 'E',
  actions,
  images: [
    {
      image_id: 'cimg-1', name: '补图1', url: '/api/cabinet-power/batches/b1/images/cimg-1', thumbnail_url: '/api/cabinet-power/batches/b1/images/cimg-1',
      candidates: [
        // row_id 已失效（stale）：应通过三元组 E/202/A11 匹配 dir-E-A11
        { index: 0, label: '识别补A11', scope: 'E', room: '202', rack: 'A11', row_id: 'stale-row', fields: { action: '上正式电', expected: '2026-10-02 09:00:00', actual: '', supplier_rack: 'S-NEW', result: '成功', type_detail: '新增整柜' } },
        // row_id 有效，结果未知（result 空）→ 结果应保持空
        { index: 1, label: '识别补A12', scope: 'E', room: '202', rack: 'A12', row_id: 'dir-E-A12', fields: { action: '上测试电', expected: '2026-10-02 10:00:00', actual: '2026-10-02 10:05:00', supplier_rack: '', result: '', type_detail: '半柜补柜' } },
      ],
    },
    {
      image_id: 'cimg-2', name: '补图2', url: '/api/cabinet-power/batches/b1/images/cimg-2', thumbnail_url: '/api/cabinet-power/batches/b1/images/cimg-2',
      candidates: [
        { index: 0, label: '识别补X17', scope: 'A', room: '101', rack: 'X17', row_id: 'dir-A-X17', fields: { action: '测试电转正式电', expected: '2026-10-02 12:00:00', actual: '', supplier_rack: 'S-A', result: '失败', type_detail: 'A楼整柜' } },
      ],
    },
  ],
  rows: completionRows,
};

const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    await context.addInitScript((fixture) => {
      window.__initialFixture = fixture;
    }, fixture);
    const page = await context.newPage();
    const errors = [];
    const nativeRequests = [];
    page.on('pageerror', (err) => errors.push(err.message));
    await page.route('**/api/**', (route) => {
      const req = route.request();
      nativeRequests.push({ method: req.method(), url: req.url() });
      if (req.method() !== 'GET') return route.fulfill({ status: 418, json: { error: 'forbidden business write' } });
      return route.fulfill({ contentType: 'image/png', body: PNG });
    });

    await page.goto(base);

    const state = () => page.evaluate(() => window.__getState());
    const mount = (fixtureData) => page.evaluate((data) => window.__mount(data), fixtureData);
    const setModel = (v) => page.evaluate((data) => window.__setModel(data), v);
    const setDisabled = (d) => page.evaluate((data) => window.__setDisabled(!!data), d);
    const setField = (f) => page.evaluate((data) => window.__setField(data), f);

    const imageBtn = page.locator('#proof-image');
    const candidateBtn = page.locator('#proof-candidate');
    const rackBtn = page.locator('#proof-rack');
    const scopeBtn = page.locator('#proof-scope');
    const pickOption = (name) => page.getByRole('option', { name, exact: true }).click();
    const pickImage = async (label) => { await imageBtn.click(); await pickOption(label); };
    const pickCandidate = async (label) => { await candidateBtn.click(); await pickOption(label); };
    const pickRack = async (label) => { await rackBtn.click(); await pickOption(label); };
    const pickScope = async (label) => { await scopeBtn.click(); await pickOption(label); };

    /* ---------- 场景1：初始化不 emit，不改已有初值 ---------- */
    await mount({ field, value: {
      image_id: 'img-1', candidate_index: 0, row_id: 'row-A11',
      fields: { action: '上正式电', expected: '2026-10-02 09:00:00', actual: '', supplier_rack: 'S-A11', result: '成功', failure_reason: '' },
      attach: true, review_times: false, review_business: false,
    }, disabled: false });
    assert.equal((await state()).emitted.length, 0, '初始化不应 emit');
    assert.deepEqual((await state()).latest.fields, {
      action: '上正式电', expected: '2026-10-02 09:00:00', actual: '', supplier_rack: 'S-A11', result: '成功', failure_reason: '',
    }, '初始化不应改已有初值');
    assert.match(await imageBtn.innerText(), /1\. 截图A/, '图片标签用显示序号+文件名');

    /* ---------- 场景2：两图两柜切换清理旧建议 + 防旧时间残留 ---------- */
    await mount({ field, value: {}, disabled: false });
    await pickImage('1. 截图A');
    await pickCandidate('识别A-A11');
    let s = await state();
    assert.equal(s.latest.image_id, 'img-1');
    assert.equal(s.latest.candidate_index, 0);
    assert.equal(s.latest.row_id, 'row-A11');
    assert.equal(s.latest.fields.expected, '2026-10-02 09:00:00');
    assert.equal(s.latest.fields.action, '上正式电');
    assert.equal(s.latest.fields.supplier_rack, 'S-A11');
    assert.equal(s.latest.fields.result, '成功');

    await pickImage('2. 截图B');
    assert.equal((await state()).latest.row_id, '', '切换图片必须重新确认目标机柜');
    assert.deepEqual((await state()).latest.fields, {}, '切换图片不残留旧机柜字段');
    s = await state();
    assert.equal(s.latest.candidate_index, -1, '切图后候选应重置为仅关联证明');
    assert.equal(s.latest.row_id, '', '切换图片后不自动沿用上一机柜');
    assert.deepEqual(s.latest.fields, {}, '尚未选择机柜时不显示其他机柜的旧值');

    await pickCandidate('识别B-A11');
    s = await state();
    assert.equal(s.latest.fields.expected, '2026-10-02 11:00:00');
    assert.equal(s.latest.fields.action, '下正式电');
    assert.equal(s.latest.fields.supplier_rack, '', '候选供应商为空时保持空');

    await pickImage('1. 截图A');
    await pickCandidate('识别A-A12');
    s = await state();
    assert.equal(s.latest.row_id, 'row-A12', '候选 row_id 在 rows 中应自动选中');
    assert.equal(s.latest.fields.expected, '2026-10-02 10:00:00', '同柜候选非空 expected 覆盖');
    assert.equal(s.latest.fields.actual, '2026-10-02 10:05:00', '同柜候选非空 actual 覆盖');
    assert.equal(s.latest.fields.action, '下测试电', '原行 action 非空则不覆盖');
    assert.equal(s.latest.fields.supplier_rack, 'S-OLD', '原行 supplier 非空则不覆盖');

    await pickCandidate('识别A-A11');
    await pickRack('E楼202/A12');
    s = await state();
    assert.equal(s.latest.fields.expected, '2026-10-02 08:00:00', '不同柜候选不覆盖 expected（保持目标行原值）');
    assert.equal(s.latest.fields.action, '下测试电', '不同柜候选不覆盖 action（保持目标行原值）');
    assert.equal(s.latest.fields.actual, '2026-10-02 08:30:00', '不同柜候选不覆盖 actual（防止旧识别时间残留）');
    assert.equal(await page.locator('.lhp-mismatch').count(), 1, '错配时应显示仅关联证明提示');

    /* ---------- 场景3：候选 row_id 不在 rows → 不自动选第一个机柜 / 无越权行；字段清空 ---------- */
    await mount({ field, value: {}, disabled: false });
    await pickImage('2. 截图B');
    await pickCandidate('识别B-A11');
    s = await state();
    assert.equal(s.latest.fields.expected, '2026-10-02 11:00:00', '先选带时间的匹配候选');
    await pickCandidate('识别B-X17');
    s = await state();
    assert.equal(s.latest.row_id, '', '候选 row_id 不在 rows 时应保持空');
    assert.deepEqual(s.latest.fields, {}, 'row_id 不在 rows 时字段必须清空为 {}');
    assert.match(await rackBtn.innerText(), /请选择机柜/);
    await rackBtn.click();
    const rackOptions = await page.locator('.vnet-select-menu [role="option"]').allInnerTexts();
    assert.deepEqual(rackOptions, ['E楼202/A11', 'E楼202/A12'], '机柜仅来自 rows，无越权行');
    await page.keyboard.press('Escape');

    /* ---------- 场景4：datetime-local step=1 秒精度 ---------- */
    await mount({ field, value: { image_id: '', candidate_index: -1, row_id: 'row-A11', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    const expectedInput = page.getByLabel('期望完成时间');
    assert.equal(await expectedInput.getAttribute('step'), '1', 'datetime-local 需要秒精度');
    await expectedInput.fill('2026-10-02T13:05:32');
    s = await state();
    assert.equal(s.latest.fields.expected, '2026-10-02 13:05:32', '秒精度实时保留');

    /* ---------- 场景5：结果失败显示必填失败原因；未选机柜业务禁用 ---------- */
    const resultSelect = page.getByLabel('结果', { exact: true });
    await resultSelect.selectOption('成功');
    assert.equal(await page.getByLabel('失败原因', { exact: true }).count(), 0, '结果非失败不显示失败原因');
    await resultSelect.selectOption('失败');
    const failureInput = page.getByLabel('失败原因', { exact: true });
    assert.notEqual(await failureInput.getAttribute('required'), null, '失败原因应 required');
    await failureInput.fill('隔离测试失败原因');
    s = await state();
    assert.equal(s.latest.fields.result, '失败');
    assert.equal(s.latest.fields.failure_reason, '隔离测试失败原因');
    // 未选机柜时业务字段禁用
    await mount({ field, value: { image_id: 'img-1', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    for (const business of ['操作类型', '期望完成时间', '实际完成时间', '结果']) {
      assert.equal(await page.getByLabel(business, { exact: true }).isDisabled(), true, `未选机柜时 ${business} 禁用`);
    }

    /* ---------- 场景6：attach 取消 → 业务禁用/两核对隐藏、fields 重置原值、提示；重勾恢复建议 ---------- */
    await mount({ field, value: {
      image_id: 'img-1', candidate_index: 0, row_id: 'row-A11',
      fields: { action: '上正式电', expected: '2026-10-02 09:00:00', actual: '', supplier_rack: 'S-A11', result: '成功', failure_reason: '' },
      attach: true, review_times: true, review_business: true,
    }, disabled: false });
    const attachCheck = page.getByLabel('关联证明', { exact: true });
    assert.equal(await attachCheck.isChecked(), true, 'attach 默认勾选');
    await attachCheck.uncheck();
    s = await state();
    assert.equal(s.latest.attach, false, '取消关联证明');
    assert.deepEqual(s.latest.fields, { action: '', expected: '', actual: '', supplier_rack: '', result: '成功', failure_reason: '' }, '取消 attach fields 重置原机柜值');
    assert.equal(s.latest.review_times, false, '取消 attach 核对时间重置 false');
    assert.equal(s.latest.review_business, false, '取消 attach 核对操作与结果重置 false');
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), true, '取消 attach 业务禁用');
    assert.equal(await page.getByLabel('核对时间', { exact: true }).isDisabled(), true, '取消 attach 核对禁用');
    assert.equal(await page.getByLabel('核对操作与结果', { exact: true }).isDisabled(), true, '取消 attach 核对禁用');
    assert.ok((await page.locator('.lhp-notice').allInnerTexts()).join('\n').includes('仅移除本柜截图关联，不改操作内容'), '显示只移除关联不改操作内容');
    // 重新勾选恢复选择建议
    await attachCheck.check();
    s = await state();
    assert.equal(s.latest.attach, true);
    assert.equal(s.latest.fields.expected, '2026-10-02 09:00:00', '重勾恢复候选建议');
    assert.equal(s.latest.fields.action, '上正式电', '重勾恢复候选建议');
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), false, '重勾后业务解除禁用');

    /* ---------- 场景7：核对项默认 false，切图后重置 ---------- */
    await mount({ field, value: {}, disabled: false });
    await pickImage('1. 截图A');
    await pickCandidate('识别A-A11');
    s = await state();
    assert.equal(s.latest.review_times, false);
    assert.equal(s.latest.review_business, false);
    await page.getByLabel('核对时间', { exact: true }).check();
    assert.equal((await state()).latest.review_times, true);
    await pickImage('2. 截图B');
    s = await state();
    assert.equal(s.latest.review_times, false, '切图后核对时间重置为 false');
    assert.equal(s.latest.review_business, false, '切图后核对操作与结果重置为 false');

    /* ---------- 场景8：预览原图、关闭、Esc；异常 URL 拒绝 ---------- */
    await mount({ field, value: { image_id: 'img-1', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    await page.getByRole('button', { name: '查看原图' }).click();
    const dialog = page.locator('dialog[open]');
    await dialog.waitFor();
    const imgSrc = await dialog.locator('img').getAttribute('src');
    assert.ok(imgSrc.startsWith(base + '/api/cabinet-power/batches/b1/images/img-1'), '预览仅使用本站 /api/.../images/... 路径');
    await page.keyboard.press('Escape');
    assert.equal(await dialog.count(), 0, 'Esc 应关闭原生 dialog');
    await page.getByRole('button', { name: '查看原图' }).click();
    await dialog.waitFor();
    await page.getByRole('button', { name: '关闭预览' }).click();
    assert.equal(await dialog.count(), 0, '关闭按钮应关闭原生 dialog');
    // 异常 URL：禁止查看原图 / 缩略图
    await mount({ field: badField, value: { image_id: 'img-bad', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    assert.equal(await page.getByRole('button', { name: '查看原图' }).count(), 0, '外域 URL 拒绝原图');
    assert.equal(await page.locator('.lhp-thumb img').count(), 0, 'data:/外域缩略图不渲染');
    await mount({ field: badField, value: { image_id: 'img-hash', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    assert.equal(await page.getByRole('button', { name: '查看原图' }).count(), 0, '带 fragment/非法 query 拒绝');
    assert.equal(await page.locator('.lhp-thumb img').count(), 0, '非法 query 缩略图不渲染');
    await mount({ field: badField, value: { image_id: 'img-thumb', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false }, disabled: false });
    assert.equal(await page.locator('.lhp-thumb img').count(), 1, '?thumbnail=1 缩略图允许');
    await page.getByRole('button', { name: '查看原图' }).click();
    await dialog.waitFor();
    assert.equal(await dialog.locator('img').getAttribute('src'), base + '/api/cabinet-power/batches/b1/images/img-thumb?thumbnail=1', 'thumbnail=1 query 保留');

    /* ---------- 场景9：actions 只来自 field，不编造恢复/退运 ---------- */
    await mount({ field, value: {}, disabled: false });
    const actionOptions = await page.getByLabel('操作类型', { exact: true }).locator('option').allInnerTexts();
    assert.deepEqual(actionOptions, ['请选择', ...actions], '操作仅用 field.actions 六种真实操作');
    assert.ok(!actionOptions.includes('恢复') && !actionOptions.includes('退运'), '不得编造恢复/退运');
    await mount({ field: badField, value: {}, disabled: false });
    const emptyActionOptions = await page.getByLabel('操作类型', { exact: true }).locator('option').allInnerTexts();
    assert.deepEqual(emptyActionOptions, ['请选择'], 'actions 为空时无编造选项');

    /* ---------- 场景10：同名标签唯一（显示序号+文件名；候选去重） ---------- */
    await mount({ field: dupField, value: {}, disabled: false });
    await imageBtn.click();
    const dupImageOptions = await page.locator('.vnet-select-menu [role="option"]').allInnerTexts();
    assert.deepEqual(dupImageOptions, ['1. A', '2. A', '3. A'], '同名图片用显示序号区分，标签唯一');
    await page.keyboard.press('Escape');
    await pickImage('1. A');
    await candidateBtn.click();
    const dupCandidateOptions = await page.locator('.vnet-select-menu [role="option"]').allInnerTexts();
    assert.deepEqual(dupCandidateOptions, ['仅关联证明', '同名', '同名 #2'], '同名候选去重为 同名/同名 #2');
    await page.keyboard.press('Escape');

    /* ---------- 场景11：父级更新 model / disabled 透过 Vue 生效 ---------- */
    await mount({ field, value: {}, disabled: false });
    await setModel({ image_id: 'img-1', candidate_index: -1, row_id: '', fields: {}, attach: true, review_times: false, review_business: false });
    await page.waitForFunction(() => document.getElementById('proof-image')?.textContent?.includes('1. 截图A'));
    assert.equal(await page.getByLabel('关联证明', { exact: true }).isChecked(), true, '父级 model 回填后组件反映');
    await setDisabled(true);
    assert.equal(await page.getByLabel('期望完成时间', { exact: true }).isDisabled(), true, '父级 disabled 生效');
    assert.equal(await imageBtn.isDisabled(), true, '父级 disabled 禁用图片选择');
    await setDisabled(false);

    /* ============ 补全模式（native_cabinet_correct=true） ============ */

    /* 补N1：楼栋选择事件；隐藏原关联/核对；手工补全命名 */
    await mount({ field: completionField, value: {}, disabled: false });
    assert.equal(await page.getByLabel('关联证明', { exact: true }).count(), 0, '补全模式隐藏关联证明复选框');
    assert.equal(await page.getByLabel('核对时间', { exact: true }).count(), 0, '补全模式隐藏核对时间复选框');
    assert.equal(await page.getByLabel('核对操作与结果', { exact: true }).count(), 0, '补全模式隐藏核对操作与结果复选框');
    await pickImage('1. 补图1');
    await candidateBtn.click();
    const compCandOptions = await page.locator('.vnet-select-menu [role="option"]').allInnerTexts();
    assert.ok(compCandOptions.includes('手工补全'), '补全模式手工候选叫手工补全');
    assert.ok(!compCandOptions.includes('仅关联证明'), '补全模式不显示仅关联证明');
    await page.keyboard.press('Escape');
    await pickScope('E');
    s = await state();
    assert.equal(s.latest.scope, 'E', '选择楼栋后 value 含 scope');
    assert.equal(s.latest.row_id, '', '选择楼栋后清空 row_id');
    assert.deepEqual(s.latest.fields, {}, '选择楼栋后清空 fields');
    assert.equal(s.latest.attach, undefined, '补全模式值不含 attach');
    assert.equal(s.latest.review_times, undefined, '补全模式值不含 review_times');
    assert.equal(s.latest.review_business, undefined, '补全模式值不含 review_business');
    assert.deepEqual(s.loadOptions, [{ scope: 'E' }], '选择楼栋后发 load-options {scope}');

    /* 补N2：onMounted 若 scope 有效但与 directory_scope 不同也发 load-options；scope 不符不能选目标 */
    await mount({ field: { ...completionField, directory_scope: 'A' }, value: { scope: 'E', image_id: 'cimg-1', candidate_index: 0, row_id: '', fields: {} }, disabled: false });
    await page.waitForFunction(() => document.getElementById('proof-scope')?.textContent?.includes('E'));
    s = await state();
    assert.deepEqual(s.loadOptions, [{ scope: 'E' }], 'onMounted scope 有效但 directory_scope 不同发 load-options');
    assert.equal(await rackBtn.isDisabled(), true, '目录 scope 不符不能选目标机柜');
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), true, '目录 scope 不符业务禁用');

    /* 补N3：目录返回 + 三元组匹配失效 row_id + 新柜只读类型 + 已知结果采用 */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('识别补A11');
    s = await state();
    assert.equal(s.latest.row_id, 'dir-E-A11', '候选 row_id 失效时用三元组匹配目录行');
    assert.equal(s.latest.fields.action, '上正式电', '匹配候选采纳 action');
    assert.equal(s.latest.fields.expected, '2026-10-02 09:00:00', '匹配候选采纳 expected');
    assert.equal(s.latest.fields.supplier_rack, 'S-NEW', '匹配候选采纳 supplier');
    assert.equal(s.latest.fields.result, '成功', '候选明确成功采用');
    assert.equal(s.latest.fields.type_detail, '新增整柜', '匹配候选采纳 type_detail');
    const rackTypeInput = page.getByLabel('机柜类型', { exact: true });
    assert.equal(await rackTypeInput.getAttribute('readonly'), '', '机柜类型只读');
    assert.equal(await rackTypeInput.inputValue(), '整柜', '机柜类型显示所选行 rack_type');
    assert.equal(await page.getByLabel('类型明细', { exact: true }).isDisabled(), false, 'type_detail 可填写');

    /* 补N4：结果未知保持空；选择不匹配机柜不采用候选字段 */
    await pickCandidate('识别补A12');
    s = await state();
    assert.equal(s.latest.row_id, 'dir-E-A12', '有效 row_id 直接选中');
    assert.equal(s.latest.fields.result, '', '候选结果未知时保持空');
    assert.equal(s.latest.fields.action, '上测试电', '匹配候选仍采纳 action');
    await pickCandidate('识别补A11');
    await pickRack('E楼202/A12');
    s = await state();
    assert.equal(s.latest.fields.action, '', '选择不匹配机柜不采用候选字段（防错填）');
    assert.equal(s.latest.fields.supplier_rack, '', '选择不匹配机柜不残留候选 supplier');

    /* 补N5：失败原因必填 */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('识别补A11');
    await page.getByLabel('结果', { exact: true }).selectOption('失败');
    assert.notEqual(await page.getByLabel('失败原因', { exact: true }).getAttribute('required'), null, '补全模式失败原因必填');

    /* 补N6：切换楼栋/图片清空旧值 */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('识别补A11');
    assert.equal((await state()).latest.fields.action, '上正式电');
    await pickScope('A');
    s = await state();
    assert.equal(s.latest.row_id, '', '切换楼栋后清空 row_id');
    assert.deepEqual(s.latest.fields, {}, '切换楼栋后清空 fields');
    await pickScope('E');
    await pickImage('2. 补图2');
    s = await state();
    assert.equal(s.latest.candidate_index, -1, '切换图片后候选重置');
    assert.equal(s.latest.row_id, '', '切换图片后清空 row_id');
    assert.deepEqual(s.latest.fields, {}, '切换图片后清空 fields');

    /* 补N7：目录刷新保留手填（已选行仍在同楼目录不丢手填） */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('识别补A11');
    await page.getByLabel('供应商机柜号', { exact: true }).fill('手动柜号');
    assert.equal((await state()).latest.fields.supplier_rack, '手动柜号');
    const refreshedCompletion = JSON.parse(JSON.stringify(completionField));
    await setField(refreshedCompletion);
    await page.waitForFunction(() => window.__getState().latest.fields.supplier_rack === '手动柜号');
    s = await state();
    assert.equal(s.latest.row_id, 'dir-E-A11', '刷新同楼目录保留已选行');
    assert.equal(s.latest.fields.supplier_rack, '手动柜号', '刷新同楼目录不丢手填');
    assert.equal(s.latest.fields.action, '上正式电', '已选行保留候选值');

    /* 补N8：手工候选所选行在目录刷新后被移除 → 清空 row_id/fields（防止提交按钮保持可点） */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('手工补全');
    await pickRack('E楼202/A11');
    s = await state();
    assert.equal(s.latest.row_id, 'dir-E-A11', '手工候选 + 手动选柜应先选中目录行');
    const removedCompletion = JSON.parse(JSON.stringify(completionField));
    removedCompletion.rows = removedCompletion.rows.filter((r) => r.row_id !== 'dir-E-A11');
    await setField(removedCompletion);
    await page.waitForFunction(() => window.__getState().latest.row_id === '');
    s = await state();
    assert.equal(s.latest.row_id, '', '目录刷新移除已选手工行后清空 row_id');
    assert.deepEqual(s.latest.fields, {}, '目录刷新移除已选手工行后清空 fields');

    /* 补N9：补全模式目录候选严格按 local.scope 筛选（其他楼行不显示/不被选中） */
    await mount({ field: completionField, value: {}, disabled: false });
    await pickScope('E');
    await rackBtn.click();
    const scopeRowOptions = await page.locator('.vnet-select-menu [role="option"]').allInnerTexts();
    assert.deepEqual(scopeRowOptions, ['E楼202/A11', 'E楼202/A12'], '补全模式仅显示当前楼栋目录行，不显示其他楼');
    await page.keyboard.press('Escape');

    /* 补N10：三元组仅接受唯一匹配，重复三元组不得取第一个 */
    const ambiguousCompletion = JSON.parse(JSON.stringify(completionField));
    ambiguousCompletion.rows = [
      ...completionRows,
      { ...completionRows[0], row_id: 'dir-E-A11-dup', label: 'E楼202/A11 副本' },
    ];
    await mount({ field: ambiguousCompletion, value: {}, disabled: false });
    await pickScope('E');
    await pickImage('1. 补图1');
    await pickCandidate('识别补A11'); // row_id=stale-row，三元组 E/202/A11 现在有两条
    s = await state();
    assert.equal(s.latest.row_id, '', '三元组重复时不取第一个，不自动选中');
    assert.deepEqual(s.latest.fields, {}, '三元组重复时字段清空');

    /* 补N11：fieldDisabled 在补全模式检查 directoryAligned 与真实 currentRow */
    // directory_scope 不符 → 即使已选行也禁用字段
    await mount({ field: { ...completionField, directory_scope: 'A' }, value: { scope: 'E', image_id: 'cimg-1', candidate_index: 0, row_id: 'dir-E-A11', fields: {} }, disabled: false });
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), true, '目录 scope 不符时业务禁用（即使已选行）');
    // row_id 存在但不在当前目录 → 禁用字段（真实 currentRow 为空）
    await mount({ field: completionField, value: { scope: 'E', image_id: 'cimg-1', candidate_index: 0, row_id: 'missing-row', fields: {} }, disabled: false });
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), true, '所选行不在目录时业务禁用（真实 currentRow 为空）');
    await setDisabled(true);
    assert.equal(await page.getByLabel('操作类型', { exact: true }).isDisabled(), true, 'disabled 时字段禁用');
    await setDisabled(false);

    /* 补N12：onMounted 且 disabled 时不发 load-options；按钮恢复后可重试 */
    await mount({ field: { ...completionField, directory_scope: 'A' }, value: { scope: 'E', image_id: 'cimg-1', candidate_index: 0, row_id: '', fields: {} }, disabled: true });
    assert.deepEqual((await state()).loadOptions, [], 'disabled 时 onMounted 不能发 load-options');
    await setDisabled(false);
    await page.getByRole('button', { name: '读取机柜目录' }).click();
    s = await state();
    assert.deepEqual(s.loadOptions, [{ scope: 'E' }], '按钮恢复后点击可重试发 load-options');

    /* ---------- 渲染整洁性 ---------- */
    await mount({ field, value: {
      image_id: 'img-1', candidate_index: 1, row_id: 'row-A12',
      fields: { action: '下测试电', expected: '2026-10-02 10:00:00', actual: '2026-10-02 10:05:00', supplier_rack: 'S-OLD', result: '失败', failure_reason: '测试失败原因' },
      attach: true, review_times: true, review_business: true,
    }, disabled: false });
    await page.locator('.lhp-proof').waitFor();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true, '不应横向溢出');
    const tooSmall = await page.evaluate(() =>
      Array.from(document.querySelectorAll('.lhp-proof button,input:not([type="checkbox"]):not([type="radio"]),select'))
        .some((el) => { const r = el.getBoundingClientRect(); return r.height > 0 && r.height < 40; })
    );
    assert.equal(tooSmall, false, '按钮与输入触点高度应 >= 40');
    const smallCheckRow = await page.evaluate(() =>
      Array.from(document.querySelectorAll('.lhp-check'))
        .some((el) => { const r = el.getBoundingClientRect(); return r.height > 0 && r.height < 40; })
    );
    assert.equal(smallCheckRow, false, '勾选行（触达区）应 >= 40');
    await page.screenshot({ path: path.join(output, `proof-${width}.png`), fullPage: true });

    /* ---------- 无业务写入检查 ---------- */
    for (const req of nativeRequests) {
      assert.equal(req.method, 'GET', '不应产生业务写请求');
      assert.ok(new URL(req.url).pathname.startsWith('/api/cabinet-power/batches/'), '仅允许本机图片预览路径');
    }
    assert.deepEqual(errors, [], `页面无运行时错误 (${width}px)`);

    await context.close();
    console.log(`LighthouseCabinetProof ${width}px: all scenarios passed`);
  }
  console.log('LighthouseCabinetProof isolated-vite playwright suite OK');
} finally {
  await browser.close();
  await server.close();
}
