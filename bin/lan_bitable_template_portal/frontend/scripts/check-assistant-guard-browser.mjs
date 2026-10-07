import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');

// 重保(关键守卫)控制元数据由真实后端函数派生，不导入/不运行任何后端业务或云服务：
// _guard_frontend_fields 使用原生模板布局；check 类型(灾害专项含 weather)输出
// check_date/checks(23 项点号字面键)/suggestions/weather，文件模式(materials)仅 check_date。
// cells 以 JSON 从 stdin 传入，避免临时转义注入的脆弱做法。
const derive = (sheetType, cellsObj) => {
  const code = `import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.lighthouse_api import _guard_frontend_fields
cells=json.loads(sys.stdin.read())
print(json.dumps(_guard_frontend_fields(${JSON.stringify(sheetType)}, cells), ensure_ascii=False))`;
  return JSON.parse(execFileSync(python, ['-c', code], {
    input: JSON.stringify(cellsObj),
    cwd: root,
    env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' },
    encoding: 'utf8',
  }));
};

// 23 个点号字面键检查项，label=类别+内容；灾害专项模板额外带 weather 子字段。
const templateItems = Array.from({ length: 23 }, (_, i) => ({ key: `guard.${String(i + 1).padStart(2, '0')}`, category: `类别${i + 1}`, content: `内容${i + 1}` }));
const cellsForDerive = { template_items: templateItems, check_date: '2026-10-01', machine_room: 'M-101', template_revision: 'rev-1', customized: true, source_file_id: 'sf-check-1', revision: 'r-9' };
// 上面的 cellsForDerive 是“派生用模板数据”之源；实际界面数据 value 另行构造(含 weather/suggestions、冻结系统值)。
const guardControl = derive('灾害专项', cellsForDerive);
const fileControl = derive('物资检查清单', {});
const templateControl = JSON.parse(execFileSync(python, ['-c', "import sys,json; sys.path.insert(0,'bin'); from lan_bitable_template_portal.lighthouse_api import _guard_template_frontend_fields; print(json.dumps(_guard_template_frontend_fields('设备安全'), ensure_ascii=False))"], {
  cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8' }, encoding: 'utf8',
}));

const ITEMS = Object.fromEntries(templateItems.map(it => [it.key, { status: 'normal', note: '' }]));
// 2 项异常(带旧备注)，其余 normal。
ITEMS['guard.06'] = { status: 'abnormal', note: '原有异常备注' };
ITEMS['guard.15'] = { status: 'abnormal', note: '原有异常备注' };

const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-guard');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });

const makeCheckField = () => ({
  type: 'object', name: 'cells', path: 'cells', label: '重保检查表', required: true, native_guard: true,
  value: {
    check_date: '2026-10-01',
    machine_room: 'M-101',
    template_revision: 'rev-1',
    customized: true,
    source_file_id: 'sf-check-1',
    revision: 'r-9',
    template_items: templateItems,
    weather: { level1: '', level2: '', current: '' },
    suggestions: '待整改',
    checks: JSON.parse(JSON.stringify(ITEMS)),
  },
  children: guardControl.children,
});
const makeFileField = () => ({
  type: 'object', name: 'cells', path: 'cells', label: '物资检查清单', required: true, native_guard: true,
  value: { check_date: '2026-10-01', source_file_id: 'sf-file-1', machine_room: 'M-2' },
  children: fileControl.children,
});

try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    const health = await context.request.get(base + '/api/health');
    assert.equal((await health.json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'guard-check', status: 'needs_input', title: '重保检查表填写', version: 1, fields: [makeCheckField()], operations: [], results: [] };
    const conversation = () => ({ ok: true, data: { conversation_id: 'guard-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'guard-fixture', question: '请完成检查表', answer: '请补充信息。', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', async route => { await new Promise(resolve => setTimeout(resolve, 250)); return route.fulfill({ json: conversation() }); });
    const routePlan = (id) => page.route(`**/api/assistant/plans/${id}`, route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await routePlan('guard-check');
    await routePlan('guard-file');
    await routePlan('guard-template');
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    await page.screenshot({ path: path.join(output, `guard-${width}-before-asserts.png`), fullPage: true });

    // --- 检查表(check) ---
    const rootGrp = form.getByRole('group', { name: '重保检查表', exact: true });
    await rootGrp.waitFor();
    // 隐藏的冻结系统值不得渲染为可编辑控件。
    for (const hidden of ['machine_room', 'template_revision', 'template_items', 'customized', 'source_file_id', 'revision'])
      assert.equal(await form.getByRole('textbox', { name: new RegExp('^' + hidden + '$'), exact: true }).count(), 0, `${hidden} 不应暴露输入`);

    const date = form.getByLabel('检查日期', { exact: true });
    assert.equal(await date.inputValue(), '2026-10-01');
    await date.fill('2026-10-02');

    const checks = form.getByRole('group', { name: '检查项', exact: true });
    await checks.waitFor();
    const itemGroups = () => checks.getByRole('group', { name: /^类别\d+ · 内容\d+$/ });
    assert.equal(await itemGroups().count(), 10, '第 1 页应渲染 10 项');
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /第 1 \/ 3 页 · 共 23 项/);

    // 第 1 页:编辑首项(变异常+备注)。
    const g1 = checks.getByRole('group', { name: '类别1 · 内容1', exact: true });
    await g1.getByLabel('检查结果', { exact: true }).selectOption({ label: '异常' });
    await g1.getByLabel('备注/异常说明', { exact: true }).fill('首项已备注');

    // 搜索:定位特定异常项,不清数据;清空后恢复完整列表。
    const search = checks.getByLabel('查找检查项', { exact: true });
    await search.fill('类别6');
    assert.equal(await itemGroups().count(), 1);
    assert.equal(await itemGroups().first().getAttribute('aria-label'), '类别6 · 内容6');
    await itemGroups().first().getByLabel('备注/异常说明', { exact: true }).fill('搜索中补充备注');
    await search.fill('');
    assert.equal(await itemGroups().count(), 10);
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /共 23 项/);
    // 搜索/清空不清数据:guard.06 的补充备注仍在第 1 页(索引 06<=10)。
    assert.equal(await checks.getByRole('group', { name: '类别6 · 内容6', exact: true }).getByLabel('备注/异常说明', { exact: true }).inputValue(), '搜索中补充备注');

    // 第 2 页:编辑异常项 15,页码仍有效。
    await checks.getByRole('button', { name: '下一页', exact: true }).click();
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /第 2 \/ 3 页 · 共 23 项/);
    assert.equal(await itemGroups().count(), 10);
    const g15 = checks.getByRole('group', { name: '类别15 · 内容15', exact: true });
    assert.equal(await g15.getByLabel('检查结果', { exact: true }).inputValue(), 'abnormal');
    await g15.getByLabel('备注/异常说明', { exact: true }).fill('异常已更新');

    // 第 3 页:编辑末项,页码仍有效。
    await checks.getByRole('button', { name: '下一页', exact: true }).click();
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /第 3 \/ 3 页 · 共 23 项/);
    assert.equal(await itemGroups().count(), 3);
    const g23 = checks.getByRole('group', { name: '类别23 · 内容23', exact: true });
    await g23.getByLabel('检查结果', { exact: true }).selectOption({ label: '异常' });
    await g23.getByLabel('备注/异常说明', { exact: true }).fill('末项备注');

    // weather + suggestions。
    await form.getByLabel('一级极端天气', { exact: true }).fill('大风');
    await form.getByLabel('二级极端天气', { exact: true }).fill('寒潮');
    await form.getByLabel('整改建议', { exact: true }).fill('整改建议已更新');

    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `guard-${width}.png`), fullPage: true });
    // 第 3 页(末页)时下一页必须禁用;不能用点击禁用按钮(Playwright 会等 30s 超时),
    // 用 isDisabled 断言,再上一页验证回到第 2 页,随后下一页回到第 3 页。
    assert.equal(await checks.getByRole('button', { name: '下一页', exact: true }).isDisabled(), true);
    await checks.getByRole('button', { name: '上一页', exact: true }).click();
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /第 2 \/ 3 页/);
    await checks.getByRole('button', { name: '下一页', exact: true }).click();
    assert.match(await checks.locator('.lh-sf-page-txt').innerText(), /第 3 \/ 3 页/);
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    const cells = saved[0].values.cells;
    assert.equal(Object.keys(cells.checks).length, 23, '提交值必须保留全部 23 项');
    assert.equal(cells.check_date, '2026-10-02');
    assert.equal(cells.machine_room, 'M-101');
    assert.equal(cells.template_revision, 'rev-1');
    assert.equal(cells.customized, true);
    assert.equal(cells.source_file_id, 'sf-check-1');
    assert.equal(cells.revision, 'r-9');
    assert.equal(cells.weather.level1, '大风');
    assert.equal(cells.weather.level2, '寒潮');
    assert.equal(cells.suggestions, '整改建议已更新');
    assert.equal(cells.checks['guard.01'].status, 'abnormal');
    assert.equal(cells.checks['guard.01'].note, '首项已备注');
    assert.equal(cells.checks['guard.02'].status, 'normal');
    assert.deepEqual(cells.checks['guard.06'], { status: 'abnormal', note: '搜索中补充备注' });
    assert.deepEqual(cells.checks['guard.15'], { status: 'abnormal', note: '异常已更新' });
    assert.deepEqual(cells.checks['guard.23'], { status: 'abnormal', note: '末项备注' });
    // 其它页未编辑项原样保留(默认 normal 空备注)。
    assert.deepEqual(cells.checks['guard.10'], { status: 'normal', note: '' });

    // --- 文件模式(materials):仅 date 控件,保留 source_file_id ---
    plan = { id: 'guard-file', status: 'needs_input', title: '物资检查清单', version: 1, fields: [makeFileField()], operations: [], results: [] };
    await page.reload();
    await form.waitFor();
    const fileRoot = page.locator('.plan-form').getByRole('group', { name: '物资检查清单', exact: true });
    await fileRoot.waitFor();
    assert.equal(await page.locator('.assistant-panel').evaluate(el => el.classList.contains('expanded')), false, '单个日期字段不应展开大窗');
    for (const absent of ['检查项', '整改建议', '极端天气'])
      assert.equal(await page.locator('.plan-form').getByRole('group', { name: new RegExp('^' + absent + '$'), exact: true }).count(), 0, `文件模式不应有 ${absent}`);
    assert.equal(await page.locator('.plan-form').getByRole('textbox', { name: /^machine_room$/, exact: true }).count(), 0);
    assert.equal(await page.locator('.plan-form').getByRole('textbox', { name: /^source_file_id$/, exact: true }).count(), 0);
    const fdate = page.locator('.plan-form').getByLabel('检查日期', { exact: true });
    assert.equal(await fdate.inputValue(), '2026-10-01');
    await fdate.fill('2026-10-05');
    await page.locator('.plan-form').getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 2);
    const fileCells = saved[1].values.cells;
    assert.equal(fileCells.check_date, '2026-10-05');
    assert.equal(fileCells.source_file_id, 'sf-file-1');
    assert.equal(fileCells.machine_room, 'M-2');

    plan = { id: 'guard-template', status: 'needs_input', title: '编辑楼栋检查模板', version: 1,
      fields: [{ ...templateControl, name: 'items', path: 'items', label: 'A楼检查模板', required: true, value: structuredClone(templateItems) }], operations: [], results: [] };
    await page.reload();
    const editor = page.locator('.plan-form').getByRole('group', { name: 'A楼检查模板', exact: true });
    await editor.waitFor();
    assert.equal(await editor.getByLabel('key', { exact: true }).count(), 0);
    assert.equal(await editor.locator('.lh-sf-entry').count(), 10);
    await editor.getByLabel('检查内容', { exact: true }).first().fill('修改第一项');
    await editor.getByRole('button', { name: '下一页', exact: true }).click();
    await editor.getByRole('button', { name: '下一页', exact: true }).click();
    await editor.getByLabel('检查内容', { exact: true }).last().fill('修改末项');
    await editor.getByRole('button', { name: '添加A楼检查模板', exact: true }).click();
    await editor.getByLabel('检查内容', { exact: true }).last().fill('新增楼内检查');
    await editor.getByLabel('分类', { exact: true }).last().fill('楼内');
    await editor.getByRole('button', { name: '删除第 22 项', exact: true }).click();
    await page.screenshot({ path: path.join(output, `guard-template-${width}.png`), fullPage: true });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.locator('.plan-form').getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const items = saved[2].values.items;
    assert.equal(items.length, 23);
    assert.equal(items[0].content, '修改第一项');
    assert.equal(items.find(item => item.key === 'guard.23').content, '修改末项');
    assert.equal(items.some(item => item.key === 'guard.22'), false);
    assert.match(items.at(-1).key, /^[a-f0-9]{32}$/);
    assert.equal(items.at(-1).content, '新增楼内检查');
    assert.equal(items.at(-1).category, '楼内');
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant guard ${width}px: 23 checks, first/second/third page edits, search/clear keeps data, weather/suggestions, frozen hidden preserved, file-mode date-only OK`);
  }
} finally {
  await browser.close();
}
