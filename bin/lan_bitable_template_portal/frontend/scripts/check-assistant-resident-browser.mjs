import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const base = process.env.LIGHTHOUSE_RESIDENT_URL;
assert(base && /^http:\/\/127\.0\.0\.1:\d+$/.test(base));
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
await context.addCookies([{ name: 'sid', value: 'isolated', url: base }]);
const errors = [], posts = [];
const page = await context.newPage();
page.on('pageerror', e => errors.push(e.message));
page.on('request', request => {
  if (request.method() === 'POST' && new URL(request.url()).pathname === '/api/assistant/messages') posts.push(request.postDataJSON());
});
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-resident');
await mkdir(output, { recursive: true });
async function health() {
  const response = await context.request.get(base + '/api/health');
  assert(response.ok());
  const value = await response.json();
  assert.equal(value.instance_id, 'isolated-lighthouse-resident', 'Never write to a production portal');
  return value;
}
async function conversation() {
  const response = await context.request.get(base + '/api/assistant/conversation');
  assert(response.ok(), await response.text());
  return (await response.json()).data;
}
async function waitFor(check, timeout = 120000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  throw new Error('Native PC acceptance condition timed out');
}
async function mode(delay) {
  const response = await context.request.post(base + '/__fixture/mode', { data: { delay } });
  assert(response.ok());
  return response.json();
}
async function ask(question) {
  await page.locator('#assistant-question').fill(question);
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  await waitFor(async () => (await page.locator('#assistant-question').inputValue()) === '', 8000);
  assert(await page.locator('#assistant-question').isEditable());
}
async function completed() {
  await waitFor(async () => {
    const value = await conversation();
    const turn = value.turns.at(-1);
    if (turn?.status === 'failed') throw new Error(turn.error);
    return turn?.status === 'completed' && !value.busy;
  });
  await page.getByRole('button', { name: '重新生成回答', exact: true }).last().waitFor();
}
try {
  await health();
  const original = await conversation();
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor();
  await page.locator('#assistant-question').waitFor();
  await waitFor(() => page.locator('#assistant-question').isEnabled());
  await ask('查询D楼维修项目列表');
  await completed();
  assert.match(await page.locator('.turn').last().innerText(), /D楼维修项目共2条/);
  console.log('[ResidentPC] native reply and input clearing passed');
  await page.screenshot({ path: path.join(output, 'native-answer.png') });

  await page.locator('.composer input[type=file]').setInputFiles({
    name: 'native-proof.txt', mimeType: 'text/plain', buffer: Buffer.from('isolated attachment receipt') });
  await waitFor(async () => (await page.locator('.draft-files').innerText()).includes('已就绪'));
  await ask('结合附件查询D楼维修项目列表');
  await completed();
  const attached = (await conversation()).turns.at(-1).attachments;
  assert.equal(attached.length, 1);
  const download = await context.request.get(base + attached[0].url);
  assert(download.ok());
  assert.equal(await download.text(), 'isolated attachment receipt');
  const anonymous = await browser.newContext();
  assert.equal((await anonymous.request.get(base + attached[0].url)).status(), 401);
  await anonymous.close();
  console.log('[ResidentPC] private attachment upload/download passed');

  await page.locator('#assistant-model').selectOption('Alternate');
  await waitFor(async () => (await conversation()).model_name === 'Alternate');
  assert.equal((await conversation()).conversation_id, original.conversation_id);
  await ask('切换模型后查询D楼维修项目列表');
  await completed();
  const switched = await conversation();
  assert.equal(switched.turns.at(-1).model_name, 'Alternate');
  assert.equal(switched.conversation_id, original.conversation_id);
  console.log('[ResidentPC] model switch retained conversation/context');
  const before = await health();
  const sent = posts.length;
  assert((await context.request.post(base + '/__fixture/restart')).ok());
  await waitFor(async () => {
    try { return (await health()).generation > before.generation; } catch { return false; }
  });
  await page.reload();
  if (!(await page.locator('.assistant-panel').count())) await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await waitFor(() => page.locator('#assistant-question').isEnabled());
  const after = await health();
  assert.equal(after.service_pid, before.service_pid);
  assert.equal(after.gateways[0].pid, before.gateways[0].pid);
  assert.equal((await conversation()).conversation_id, original.conversation_id);
  assert.equal(posts.length, sent, 'Portal restart must not replay messages');
  console.log('[ResidentPC] portal restart retained host/gateway/history without replay');

  let pending = await mode(20);
  await ask('慢速查询D楼维修项目列表');
  await waitFor(async () => (await health()).model_calls > pending.model_calls);
  await page.getByRole('button', { name: '停止生成', exact: true }).click();
  await waitFor(async () => (await conversation()).turns.at(-1).status === 'stopped', 15000);
  await mode(0);
  await page.getByRole('button', { name: '继续回答', exact: true }).last().click();
  await completed();

  pending = await mode(20);
  await ask('查询当前维修项目');
  await waitFor(async () => (await health()).model_calls > pending.model_calls);
  await mode(0);
  await ask('只看D楼');
  await completed();
  const final = await conversation();
  assert(final.turns.some(turn => turn.status === 'superseded'));
  assert.match(final.turns.at(-1).question, /只看D楼/);
  assert.match(final.turns.at(-1).answer, /D楼维修项目共2条/);
  assert.equal((await health()).writes, 0, 'Browser flow must not execute business writes');
  assert.deepEqual(errors, []);
  assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  assert(await page.locator('.assistant-panel').evaluate(element => element.scrollWidth <= element.clientWidth + 1));
  await page.screenshot({ path: path.join(output, 'native-stop-supplement.png') });
  console.log('[ResidentPC] production dist + real Python/Node; file access, model switch/context, portal restart/no replay, stop/continue/supplement passed');
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png') }).catch(() => {});
  console.error(JSON.stringify({ error: String(error), pageErrors: errors, posts: posts.length, conversation: await conversation().catch(() => null) }));
  throw error;
} finally {
  await context.close();
  await browser.close();
}
