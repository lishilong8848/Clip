import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');

// 维修子字段由项目的真实后端元数据生成函数派生（不导入/不运行任何后端业务/云服务）：
// _repair_frontend_fields 把每个可编辑维修字段生成 children 元数据，其中
// percentage 位于子字段自身(control["percentage"]=名称为维修进度)，repair_field 子对象不含 percentage。
const code = `import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.lighthouse_api import _repair_frontend_fields
metas=[
 {"field_name":"故障发生时间","field_type":5,"ui_type":"DateTime","options":[],"editable":True,"required":True},
 {"field_name":"维修进度","field_type":2,"ui_type":"Progress","options":[],"editable":True,"required":True},
 {"field_name":"维修原因","field_type":1,"ui_type":"Text","options":[],"editable":True,"required":True},
 {"field_name":"备注","field_type":1,"ui_type":"Text","options":[],"editable":True,"required":True},
]
print(json.dumps(_repair_frontend_fields(metas, unlinked=False), ensure_ascii=False))`;
const repairControl = JSON.parse(execFileSync(python, ['-c', code], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8' }));

const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-literal-fields');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });

// 助手把动态飞书字段名（如“设备.型号”）作为直接对象键展示。
// literal_key=true 的子字段必须按字面键读写，不得把“.”拆成嵌套对象；
// 同时保留其它未编辑键（含 $query 引用）与普通嵌套路径。
// 维修子字段（故障时间/进度/原因/备注）由上方 repairControl.children 派生，
// 保留原值：故障时间纪元毫秒、进度小数、原因富文本[{text}]。
const device = {
  name: 'device', path: 'device', label: '设备及型号', type: 'object', required: true,
  value: {
    '设备.型号': '原型号',
    '设备': { 型号: '保留嵌套' },
    '$query': { ref: 'sheet', keyword: 'A' },
    location: { room: '原房间', note: '保留备注' },
    '故障发生时间': 1790821800000,
    '维修进度': 0.58,
    '维修原因': [{ text: '原原因' }],
    '备注': '未改',
  },
  children: [
    // 单独保留的字面键 / 普通嵌套路径 fixture（来自前端元数据约定，非后端生成）。
    { path: '设备.型号', label: '设备型号', type: 'text', literal_key: true, required: true },
    { path: 'location.room', label: '房间', type: 'text', required: true },
    ...repairControl.children,
  ],
};
const fields = [device];
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 }, timezoneId: 'Asia/Shanghai' });
    const health = await context.request.get(base + '/api/health');
    assert.equal((await health.json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'literal-test', status: 'needs_input', title: '隔离设备登记', version: 1, fields, operations: [], results: [] };
    const conversation = () => ({ ok: true, data: { conversation_id: 'fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'device-fixture', question: '登记设备及型号', answer: '请补充信息。', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', async route => { await new Promise(resolve => setTimeout(resolve, 250)); return route.fulfill({ json: conversation() }); });
    await page.route('**/api/assistant/plans/literal-test', route => {
      assert.equal(route.request().method(), 'PATCH');
      saved.push(route.request().postDataJSON());
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    const panel = page.locator('.assistant-panel');
    await panel.evaluate(el => new Promise(resolve => {
      const frames = [], started = performance.now();
      function sample() {
        const rect = el.getBoundingClientRect();
        frames.push({ left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom });
        if (performance.now() - started < 400) requestAnimationFrame(sample); else resolve(frames);
      }
      sample();
    })).then(frames => { for (const rect of frames) assert(rect.left >= 10 && rect.top >= 10 && rect.right <= width - 10 && rect.bottom <= 990, 'resizing must stay in viewport'); });

    // 断言前先保存诊断截图，便于定位标签/选择器问题。
    await page.screenshot({ path: path.join(output, `literal-${width}-before-asserts.png`), fullPage: true });

    // 字面键字段：编辑应直接更新“设备.型号”这一个键。
    const literal = form.getByLabel('设备型号', { exact: true });
    assert.equal(await literal.inputValue(), '原型号');
    await literal.fill('新型号');
    // 普通嵌套路径字段：编辑 location.room，保留 location.note。
    const room = form.getByLabel('房间', { exact: true });
    assert.equal(await room.inputValue(), '原房间');
    await room.fill('新房间');

    // 原生维修（后端派生元数据）：RepairFieldControl 必填 label 含星号、
    // 且输入本身不带 aria-label，故按其稳定包装器 data-field-name + 控件
    // [data-repair-control] 精确定位，而不是放宽断言。
    // 故障时间纪元 → 本地字符串(亚洲/上海) 2026-10-01T10:30。
    const date = form.locator('.repair-field-control[data-field-name="故障发生时间"] [data-repair-control]');
    assert.equal(await date.inputValue(), '2026-10-01T10:30');
    await date.fill('2026-10-01T11:30');
    // 原生维修：进度 0.58 → 界面显示 58%，编辑 75 -> 发射 0.75（percentage 在子字段自身）。
    const progress = form.locator('.repair-field-control[data-field-name="维修进度"] [data-repair-control]');
    assert.equal(await progress.inputValue(), '58');
    await progress.fill('75');
    // 富文本 [{text}] 解码为纯文本展示，未编辑则保留原始数组。
    const reason = form.locator('.repair-field-control[data-field-name="维修原因"] [data-repair-control]');
    assert.equal(await reason.inputValue(), '原原因');

    // 桌面与窄屏都不横向溢出。
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `literal-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    const value = saved[0].values;
    assert.equal(Object.hasOwn(value, '设备.型号'), false, '普通嵌套路径字段名不得出现在顶层');
    const deviceValue = value.device;
    // 字面键“设备.型号”仍是单一键，未拆成 { 设备: { 型号 } }，嵌套同名键原样保留。
    assert.equal(Object.hasOwn(deviceValue, '设备.型号'), true, '字面键必须以直接对象键保留');
    assert.equal(Object.hasOwn(deviceValue, '设备'), true, '嵌套同名键必须保留');
    assert.equal(deviceValue['设备.型号'], '新型号');
    assert.equal(deviceValue['设备'].型号, '保留嵌套');
    // $query 引用原样保留。
    assert.deepEqual(deviceValue.$query, { ref: 'sheet', keyword: 'A' });
    // 普通嵌套路径仍然工作，且兄弟键未丢失。
    assert.equal(deviceValue.location.room, '新房间');
    assert.equal(deviceValue.location.note, '保留备注');
    // 原生维修字段：日期改后发射本地字符串、进度改为 0.75。
    assert.equal(deviceValue['故障发生时间'], '2026-10-01T11:30');
    assert.equal(deviceValue['维修进度'], '0.75');
    // 未编辑字段保留原始值：富文本数组与普通文本原样。
    assert.deepEqual(deviceValue['维修原因'], [{ text: '原原因' }]);
    assert.equal(deviceValue['备注'], '未改');
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant literal fields ${width}px: literal key kept, nested/$query siblings intact, native repair date/progress/richtext OK, overflow OK`);
  }
} finally {
  await browser.close();
}