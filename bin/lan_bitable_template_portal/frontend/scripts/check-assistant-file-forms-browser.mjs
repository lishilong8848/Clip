import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const base = 'http://127.0.0.1:19003';
const output = path.resolve('../../../output/playwright/assistant-file-forms');
await mkdir(output, { recursive: true });
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/l9sAAAAASUVORK5CYII=', 'base64');
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [], uploads = [], deletes = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => { if (request.method() === 'DELETE') deletes.push(request.url()); });
    const first = { id: 'a'.repeat(32), name: '原始证明.png', url: '/api/assistant/files/' + 'a'.repeat(32), is_image: true, size: png.length };
    const files = new Map([[first.id, first]]);
    const initial = [
      { name: 'single', path: 'file', section: 'files', label: '确认截图', type: 'file', required: true, maxItems: 1, value: [first.id], selected_files: [first] },
      { name: 'multi', path: 'files', section: 'files', label: '补充资料', type: 'file', required: false, maxItems: 10, value: [], selected_files: [] },
    ];
    let plan = { id: 'file-form', title: '核对附件', status: 'needs_input', version: 1, fields: structuredClone(initial), operations: [], results: [] };
    const conversation = () => ({ ok: true, data: { conversation_id: 'file-form', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'file-turn', question: '补充业务附件', answer: '请核对', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: conversation() }));
    await page.route('**/api/assistant/files/*', route => route.fulfill({ contentType: 'image/png', body: png }));
    let failSecond = true;
    await page.route('**/api/assistant/files', async route => {
      const data = route.request().postDataBuffer().toString('utf8');
      const name = /filename="([^"]+)"/.exec(data)?.[1];
      assert.ok(name);
      uploads.push(name);
      await new Promise(resolve => setTimeout(resolve, 250));
      if ((name === '乙.txt' && failSecond) || name === '失败.png') {
        failSecond = false;
        return route.fulfill({ status: 503, json: { ok: false, error: '隔离上传失败' } });
      }
      const id = uploads.length.toString(16).padStart(32, '0');
      const file = { id, name, url: '/api/assistant/files/' + id, is_image: name.endsWith('.png'), size: 4 };
      files.set(id, file);
      return route.fulfill({ json: { ok: true, data: { files: [file] } } });
    });
    await page.route('**/api/assistant/plans/file-form', route => {
      const body = route.request().postDataJSON();
      assert.equal(body.version, plan.version);
      if (body.action === 'edit') {
        plan = { ...plan, status: 'needs_input', version: plan.version + 1, can_edit: false, fields: initial.map(field => {
          const ids = saved.at(-1)[field.name];
          return { ...field, value: ids, selected_files: ids.map(id => files.get(id)) };
        }) };
      } else {
        saved.push(body.values);
        const ids = [...body.values.single, ...body.values.multi];
        plan = { ...plan, status: 'awaiting_confirmation', version: plan.version + 1, fields: [], can_edit: true,
          operations: [{ api_id: 'POST /api/upload-fixture', name: '上传所选附件', files: { files: ids }, selected_files: ids.map(id => files.get(id)) }] };
      }
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    assert.equal(await page.getByLabel('确认截图', { exact: true }).getAttribute('multiple'), null);
    assert.notEqual(await page.getByLabel('补充资料', { exact: true }).getAttribute('multiple'), null);
    await form.getByText(first.name, { exact: true }).waitFor();
    await page.waitForFunction(() => document.querySelector('.plan-file img')?.naturalWidth === 1);
    await page.getByLabel('补充资料', { exact: true }).setInputFiles([
      { name: '甲.txt', mimeType: 'text/plain', buffer: Buffer.from('甲') },
      { name: '乙.txt', mimeType: 'text/plain', buffer: Buffer.from('乙') },
    ]);
    await form.getByRole('status').waitFor();
    await page.getByText(/已上传 1\/2/).waitFor();
    await form.getByText('甲.txt', { exact: true }).waitFor();
    assert.equal(await form.getByText('乙.txt', { exact: true }).count(), 0);
    await page.getByLabel('补充资料', { exact: true }).setInputFiles({ name: '乙.txt', mimeType: 'text/plain', buffer: Buffer.from('乙') });
    await form.getByText('乙.txt', { exact: true }).waitFor();
    await page.getByLabel('补充资料', { exact: true }).setInputFiles([
      { name: '丙.txt', mimeType: 'text/plain', buffer: Buffer.from('丙') },
      { name: '丁.txt', mimeType: 'text/plain', buffer: Buffer.from('丁') },
    ]);
    await form.getByText('丁.txt', { exact: true }).waitFor();
    await page.waitForFunction(w => {
      const r = document.querySelector('.assistant-panel').getBoundingClientRect();
      return Math.abs(r.width - Math.min(760, w - 48)) <= 1 && Math.abs(r.height - 820) <= 1;
    }, width);
    for (const name of ['丙.txt', '丁.txt']) await form.getByRole('button', { name: '移除' + name, exact: true }).click();
    await form.getByRole('button', { name: '移除甲.txt', exact: true }).click();
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '返回修改', exact: true }).click();
    await page.reload();
    await form.getByText('乙.txt', { exact: true }).waitFor();
    await form.getByText(first.name, { exact: true }).waitFor();
    await form.getByRole('button', { name: '移除乙.txt', exact: true }).click();
    await page.getByLabel('确认截图', { exact: true }).setInputFiles({ name: '失败.png', mimeType: 'image/png', buffer: png });
    await page.getByText(/已上传 0\/1/).waitFor();
    await form.getByText(first.name, { exact: true }).waitFor();
    await page.getByLabel('确认截图', { exact: true }).setInputFiles({ name: '新的完整证明图片.png', mimeType: 'image/png', buffer: png });
    await form.getByText('新的完整证明图片.png', { exact: true }).waitFor();
    assert.equal(await form.getByText(first.name, { exact: true }).count(), 0);
    await page.screenshot({ path: path.join(output, `selected-${width}.png`) });
    const layout = await form.evaluate(el => ({ width: el.clientWidth, scroll: el.scrollWidth,
      overflow: [...el.querySelectorAll('*')].filter(node => node.getBoundingClientRect().right > el.getBoundingClientRect().right + 1)
        .map(node => ({ tag: node.tagName, class: node.className, width: node.getBoundingClientRect().width })) }));
    assert.ok(layout.scroll <= layout.width + 1, JSON.stringify(layout));
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '返回修改', exact: true }).waitFor();
    assert.deepEqual(saved.at(-1).multi, []);
    assert.equal(files.get(saved.at(-1).single[0]).name, '新的完整证明图片.png');
    assert.deepEqual(deletes, []);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Attachment forms ${width}px: visible names/previews, partial failure, append/remove/replace and reopen/reload OK`);
  }
} finally {
  await browser.close();
}
