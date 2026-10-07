import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const generated = spawnSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-c', `
import sys,json,asyncio
sys.path.insert(0,'bin')
from test_lighthouse_download_workflows import DownloadWorkflowTests,ACTOR
async def main():
    f=DownloadWorkflowTests();f.setUp()
    try:
        plans={}
        for mode in ('single','batch','partial','drill'):
            if mode=='partial':
                f.batch.update(status='failed',error='C楼导出失败')
                f.batch['items']['C']={'scope':'C','status':'failed'}
            done=await f.execute(f.prepare('export' if mode=='single' else 'batch' if mode=='partial' else mode))
            plans[mode]=f.public(done)
        result=await f.agent._invoke(ACTOR,{'api_id':'GET /api/critical-guard/tasks/{task_id}','path_params':{'task_id':'guard-task'},'params':{'scope':'E'}},f.fixture.request)
        query=plans['batch']['results'][0]['downloads']+plans['drill']['results'][0]['downloads']+result['downloads']
        print(json.dumps({'plans':plans,'query':query},ensure_ascii=False))
    finally:f.fixture.tmp.cleanup()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
assert.equal(generated.status, 0, generated.stderr);
const { plans, query } = JSON.parse(generated.stdout), base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-downloads');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 }, acceptDownloads: true });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [];
    let mode = 'single', nativeDownloads = 0, writes = 0;
    page.on('pageerror', error => errors.push(error.message));
    await context.route('**/api/cabinet-power/exports/**/download', route => {
      assert.equal(route.request().method(), 'GET'); nativeDownloads++;
      return route.fulfill({ contentType: 'application/vnd.ms-excel.sheet.macroEnabled.12', headers: { 'Content-Disposition': 'attachment; filename="fixture.xlsm"' }, body: Buffer.from('isolated-download') });
    });
    await page.route('**/api/cabinet-power/exports', route => { writes++; return route.abort(); });
    await page.route('**/api/drills/*/generate', route => { writes++; return route.abort(); });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'downloads-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'downloads-fixture', question: '查看生成文件', answer: mode === 'query' ? '已生成文件见下方。' : '请查看操作结果。', status: 'completed',
        plan: mode === 'query' ? null : plans[mode], downloads: mode === 'query' ? [...query,
          { name: '恶意外部链接', url: 'https://invalid.example/download' }, { name: '恶意脚本', url: 'javascript:alert(1)' },
          { name: '不相关接口', url: '/api/admin/settings' }, { name: '路径越界', url: '/api/cabinet-power/exports/../download' }] : [] }],
    } } }));
    for (const current of ['single', 'batch', 'partial', 'drill', 'query']) {
      mode = current;
      await page.goto(base); await page.locator('.assistant-launcher, .assistant-panel').waitFor();
      const open = page.getByRole('button', { name: '打开灯塔助手', exact: true }); if (await open.isVisible()) await open.click();
      const files = page.getByLabel('生成文件', { exact: true }); await files.waitFor();
      if (mode === 'query') {
        assert.equal(await files.getByRole('link').count(), 5);
        await files.getByRole('button', { name: /更多文件/ }).click();
        assert.equal(await files.getByRole('link').count(), query.length);
        assert.match((await files.getByRole('link').allTextContents()).join(' '), /重保检查表图片/);
        await files.getByRole('button', { name: '收起文件' }).click();
        assert.equal(await files.getByRole('link').count(), 5);
      } else {
        const links = plans[mode].results[0].downloads;
        assert.equal(await files.getByRole('link').count(), links.length);
        assert.deepEqual(await files.getByRole('link').evaluateAll(items => items.map(item => item.getAttribute('href'))), links.map(item => item.url));
        if (mode === 'partial') {
          assert.match(await page.locator('.operation-plan').innerText(), /C楼导出失败/);
          assert.doesNotMatch(await page.locator('.plan-results').innerText(), /业务处理已完成/);
        }
      }
      assert.equal(await files.evaluate(el => el.scrollWidth <= el.clientWidth + 1), true);
      assert.doesNotMatch(await files.innerHTML(), /javascript:|invalid.example|\/api\/admin|\.\.\/download/);
      assert.equal(await files.getByRole('link').first().getAttribute('target'), '_blank');
      await page.screenshot({ path: path.join(output, `${mode}-${width}.png`), fullPage: true });
      if (mode === 'single') {
        const downloaded = page.waitForEvent('download');
        await files.getByRole('link').first().click();
        assert.equal((await downloaded).suggestedFilename(), 'fixture.xlsm');
        assert.equal(nativeDownloads, 1);
      }
    }
    assert.equal(writes, 0); assert.deepEqual(errors, []); await context.close();
  }
  console.log('Download UI desktop/mobile passed: single/five/partial/drill/query files, native download click, unsafe links filtered, no writes.');
} finally { await browser.close(); }
