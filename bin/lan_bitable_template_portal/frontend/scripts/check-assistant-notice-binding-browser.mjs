import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const result = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,tempfile,asyncio
sys.path.insert(0,'bin')
from pathlib import Path
from unittest.mock import Mock
from fastapi import FastAPI,Request
from clipflow_backend.api_models import WorkbenchActionRequest
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from test_lighthouse_agent_workflows import ACTOR,Store
from test_lighthouse_notice_workflows import VALID_STARTS
tmp=tempfile.TemporaryDirectory();store=Store(Path(tmp.name)/'s.sqlite3')
assistant=LighthouseAssistant(store,Mock(side_effect=AssertionError('No queries')))
app=FastAPI()
@app.post('/api/workbench-actions')
async def submit(body:WorkbenchActionRequest): raise AssertionError('No writes')
agent=PortalAgent(assistant,PortalAPICatalog(app),LighthouseFiles(store))
draft={**VALID_STARTS['repair'],'title':'粘贴的检修标题','location':'','start_time':'','end_time':''}
p=agent.prepare(ACTOR,{'operations':[{'api_id':'POST /api/workbench-actions','body':{'command_format':'notice_command','scope':'A','action':'start','work_type':'repair','manual':True,'manual_binding_choice':'unbound','patch':draft}}]},'notice-binding-browser',[])
print(json.dumps(agent.public_plan(p,ACTOR),ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(result.status, 0, result.stderr);
const original = JSON.parse(result.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-notice-binding');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [];
    let plan = structuredClone(original), writes = 0, failPrefill = false, prefillCalls = 0;
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/workbench-actions', route => { writes++; return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'binding-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'binding-fixture', question: '填写检修通告', answer: '请核对通告。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/**', async route => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith('/options')) {
        assert.equal(url.searchParams.get('field'), 'step0.source_record_id');
        assert.equal(url.searchParams.get('scope'), 'A');
        plan = structuredClone(plan);
        plan.version++;
        plan.fields.find(field => field.path === 'source_record_id').options = [
          { value: 'src-one', label: 'A楼计划一 · 未开始' }, { value: 'src-two', label: 'A楼计划二 · 未开始' },
        ];
        return route.fulfill({ json: { ok: true, data: plan } });
      }
      const body = route.request().postDataJSON();
      if (url.pathname.endsWith('/notice-prefill')) {
        prefillCalls++;
        assert.equal(body.scope, 'A');
        assert.equal(body.version, plan.version);
        await new Promise(resolve => setTimeout(resolve, 400));
        if (failPrefill) return route.fulfill({ status: 502, json: { ok: false, error: '隔离预填读取失败' } });
        const day = body.source_record_id === 'src-one' ? '01' : '02';
        return route.fulfill({ json: { ok: true, data: { version: plan.version, fields: {
          title: '来源标题不覆盖粘贴标题', location: '来源位置' + day,
          start_time: `2026-10-${day}T09:00`, end_time: `2026-09-${day}T08:00`,
        } } } });
      }
      assert.equal(route.request().method(), 'PATCH');
      saves.push(body);
      plan = { ...plan, fields: [], version: plan.version + 1, status: 'awaiting_confirmation', can_edit: true };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.locator('.assistant-launcher, .assistant-panel').waitFor();
    const open = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    if (await open.isVisible()) await open.click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    const choice = form.getByLabel('计划通告关联', { exact: true });
    const source = form.getByLabel('选择计划通告', { exact: true });
    const title = form.getByLabel('标题', { exact: true });
    const location = form.getByLabel('地点', { exact: true });
    const expected = form.getByLabel('期望完成时间', { exact: true });
    const occurred = form.getByLabel('发现故障时间', { exact: true });
    const proceed = form.getByRole('button', { name: '补充并继续', exact: true });
    assert.equal(await choice.inputValue(), 'unbound');
    assert.equal(await source.count(), 0);
    await choice.selectOption('bind');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    await source.selectOption('src-one');
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor();
    assert.equal(await proceed.isDisabled(), true);
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor({ state: 'hidden' });
    assert.equal(await title.inputValue(), '粘贴的检修标题');
    assert.equal(await location.inputValue(), '来源位置01');
    assert.equal(await expected.inputValue(), '2026-10-01T09:00');
    assert.equal(await occurred.inputValue(), '2026-09-01T08:00');
    await location.fill('手动保留地点');
    await source.selectOption('src-two');
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor({ state: 'hidden' });
    assert.equal(await location.inputValue(), '手动保留地点');
    assert.equal(await expected.inputValue(), '2026-10-02T09:00');
    await choice.selectOption('unbound');
    assert.equal(await source.count(), 0);
    assert.equal(await expected.inputValue(), '');
    assert.equal(await location.inputValue(), '手动保留地点');
    assert.equal(await title.inputValue(), '粘贴的检修标题');
    await choice.selectOption('bind');
    failPrefill = true;
    await source.selectOption('src-one');
    await form.getByText('隔离预填读取失败', { exact: true }).waitFor();
    assert.equal(await proceed.isDisabled(), true);
    failPrefill = false;
    await form.getByRole('button', { name: '重新读取关联资料', exact: true }).click();
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor({ state: 'hidden' });
    assert.equal(await expected.inputValue(), '2026-10-01T09:00');
    assert.equal(await location.inputValue(), '手动保留地点');
    await location.fill('');
    await source.selectOption('src-two');
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor({ state: 'hidden' });
    await source.selectOption('src-one');
    await form.getByText('正在读取关联资料…', { exact: true }).waitFor({ state: 'hidden' });
    assert.equal(await location.inputValue(), '', 'manual clearing is preserved across repeated selections');
    await location.fill('手动保留地点');
    assert.equal(await form.evaluate(el => el.checkValidity()), true);
    assert.equal(await form.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.doesNotMatch(await form.innerText(), /src-one|src-two|query_form_|source_record_id/);
    await page.screenshot({ path: path.join(output, `binding-${width}.png`), fullPage: true });
    await proceed.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saves.at(-1).values['step0.manual_binding_choice'], 'bind');
    assert.equal(saves.at(-1).values['step0.source_record_id'], 'src-one');
    assert.equal(saves.at(-1).values['step0.patch'].location, '手动保留地点');
    assert.equal(saves.at(-1).values['step0.patch'].title, '粘贴的检修标题');
    assert.equal(prefillCalls, 6);
    assert.equal(writes, 0);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Notice binding ${width}px: selection/prefill/manual edits/unbound/retry/native dates/no writes/no overflow OK`);
  }
} finally { await browser.close(); }
