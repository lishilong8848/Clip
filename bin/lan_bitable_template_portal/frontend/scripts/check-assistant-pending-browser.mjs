import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const base = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19002';
const output = fileURLToPath(new URL('../../../../output/playwright/assistant-pending/', import.meta.url));
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const health = await context.request.get(base + '/api/health');
  assert.equal((await health.json()).instance_id, 'isolated-ai-preview', 'never submit to production');
  const response = await context.request.get(base + '/api/assistant/pending?q=' + encodeURIComponent('现在有哪些未完成工作'));
  assert.equal(response.status(), 200);
  const data = (await response.json()).data;
  const groups = Object.fromEntries(data.groups.map(group => [group.key, group]));
  assert.equal(groups.notices.count, 2);
  assert(groups.notices.items.some(row => row.title.includes('去年开始')));
  assert.equal(groups.events.count, 1);
  assert.equal(groups.events.items[0].title, 'A楼隔离冷水机组测试事件');
  assert.equal(groups.repairs.count, 1);
  assert.equal(data.complete, true);
  const reset = await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } });
  assert.equal(reset.status(), 200);
  assert.equal((await reset.json()).ok, true);

  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  const input = page.getByRole('textbox', { name: '询问灯塔助手' });
  await input.fill('现在还有哪些未完成工作？');
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  const reply = page.locator('.turn').last().locator('.assistant-rich-reply');
  await reply.getByRole('heading', { name: '未完成工作', exact: true }).waitFor();
  assert((await reply.innerText()).includes('A楼去年开始尚未结束的维保'));
  assert((await reply.innerText()).includes('E楼未结束上电通告'));
  assert(!(await reply.innerText()).includes('未命名事件'));
  assert.equal(await reply.locator('table').count(), 4);
  assert(await reply.locator('strong').count() > 0, 'Markdown bold is actual strong text');
  assert(!(await reply.innerText()).includes('**查询范围'), 'do not display Markdown markers');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const r = document.querySelector('.assistant-panel')?.getBoundingClientRect();
    return r && r.right <= innerWidth && r.bottom <= innerHeight;
  });
  assert(await page.locator('.assistant-panel').evaluate(el => el.scrollWidth <= el.clientWidth + 1));
  await mkdir(output, { recursive: true });
  await page.locator('.thread').evaluate(el => { el.scrollTop = 0; });
  await page.screenshot({ path: output + 'mobile.png' });
  assert.deepEqual(errors, []);
  console.log('Pending API + production UI passed: older/all-type notices, named/scoped events, unfinished-only groups, actual bold/tables, and narrow-screen containment.');
} finally {
  await browser.close();
}
