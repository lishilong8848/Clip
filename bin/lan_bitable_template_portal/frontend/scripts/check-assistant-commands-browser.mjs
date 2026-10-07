import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// Serve only production assets on an ephemeral loopback port. All APIs are
// synthetic browser fixtures; no production server, model or cloud is contacted.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../dist');
const output = path.resolve(root, '../../../../output/playwright/assistant-commands');
await mkdir(output, { recursive: true });
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const filename = path.resolve(root, '.' + (url.pathname === '/' ? '/assistant.html' : decodeURIComponent(url.pathname)));
    if (!filename.startsWith(root + path.sep)) throw new Error('outside assets');
    let bytes = await readFile(filename);
    if (filename.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="command-fixture" data-user-name="测试账号"'));
    res.setHeader('Content-Type', filename.endsWith('.js') ? 'application/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [], requests = [];
const builtin = { name: 'lighthouse-repairs', id: 'lighthouse-repairs', kind: 'skill', label: '维修单与跟进', display_name: '维修单与跟进', description: '查询维修单进度和跟进记录，沿用原业务确认流程', source: 'builtin' };
const shared = { name: 'shared-example-12345678', id: 'shared-example-12345678', kind: 'skill', label: '共享示例技能', display_name: '共享示例技能', description: '所有账号共用的查询说明', source: 'shared', removable: true };
let installed = false, failedFirst = true, turns = [], appearance = { color: 'encre', shape: 'cercle', size: 56, snap_back: true };

try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.route(base + '/api/**', async route => {
    const url = new URL(route.request().url()), pathname = url.pathname, method = route.request().method();
    const fulfill = (data, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(status >= 400 ? { ok: false, error: data } : { ok: true, data }) });
    if (pathname.endsWith('/appearance')) { if (method === 'PUT') appearance = route.request().postDataJSON(); return fulfill(appearance); }
    if (pathname.endsWith('/conversation')) return fulfill({ turns, conversation_id: 'command-conversation', configured: true, enabled: true, can_manage_settings: true, model_name: '测试模型', models: [{ name: '测试模型' }], busy: false });
    if (pathname.endsWith('/commands')) {
      await new Promise(resolve => setTimeout(resolve, 180));
      const rows = url.searchParams.get('kind') === 'tools' ? [{ id: 'weather', kind: 'tool', label: '天气查询', description: '实时和历史天气', read_only: true, group: '常用工具' }, { id: 'PUT /api/repair-management/records/{record_id}', kind: 'tool', label: '维修单与跟进 · 更新维修单', description: 'PUT /api/repair-management/records/{record_id}', read_only: false, group: '维修单与跟进' }] : [builtin, ...(installed ? [shared] : [])];
      const keyword = url.searchParams.get('keyword') || '', group = url.searchParams.get('group') || '';
      const items = rows.filter(item => (!group || item.group === group) && (item.label + item.description).includes(keyword));
      return fulfill({ items, total: items.length, page: 1, groups: ['常用工具', '维修单与跟进'] });
    }
    if (pathname.endsWith('/skills/install')) { installed = true; return fulfill({ ...shared, duplicate: false, warnings: [] }); }
    if (pathname.endsWith('/skills')) return fulfill({ items: [builtin, ...(installed ? [shared] : [])] });
    if (pathname.includes('/skills/')) {
      if (method === 'DELETE') { installed = false; return fulfill({ removed: true }); }
      return fulfill({ content: '## 查询说明\n只查询当前账号有权限的业务信息。', next_offset: 0, references: [] });
    }
    if (pathname.endsWith('/messages')) {
      const payload = route.request().postDataJSON(); requests.push(payload);
      if (failedFirst) { failedFirst = false; return fulfill('测试响应失败，原消息保留', 503); }
      const commands = payload.commands.map(item => ({ ...item, label: item.id === builtin.id ? builtin.label : item.id === 'weather' ? '天气查询' : shared.label }));
      const turn = { ...payload, commands, submitted_commands: payload.commands, at: Date.now() / 1000, status: 'completed', answer: '测试查询完成，没有执行业务修改。', model_name: '测试模型' };
      turns = [turn]; return fulfill({ run_id: '', turn });
    }
    return fulfill({});
  });
  const page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
  await page.goto(base);
  const beforeOpen = await page.locator('.assistant-launcher').boundingBox();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.waitForFunction(() => { const panel = document.querySelector('.assistant-panel')?.getBoundingClientRect(), bot = document.querySelector('.assistant-launcher')?.getBoundingClientRect(); return panel && bot && panel.right <= bot.left - 8; });
  const afterOpen = await page.locator('.assistant-launcher').boundingBox();
  assert(Math.abs(beforeOpen.x - afterOpen.x) < 1 && Math.abs(beforeOpen.y - afterOpen.y) < 1, 'opening chat must not move the icon');
  const input = page.locator('#assistant-question'); await input.waitFor(); await input.fill('/维修');
  await page.getByRole('option', { name: /维修单与跟进/ }).waitFor();
  assert.equal(requests.length, 0);
  await input.press('Enter');
  assert.equal(await input.inputValue(), '');
  assert.equal(await page.locator('.command-chips').innerText(), '维修单与跟进');
  assert.equal(requests.length, 0, 'choosing a command must not execute or submit');
  await input.fill('查询D楼维修单'); await page.getByRole('button', { name: '发送问题', exact: true }).click();
  assert.equal(await input.inputValue(), '', 'send clears the input immediately');
  await page.getByRole('button', { name: '重试', exact: true }).click();
  await page.locator('.answer').getByText('测试查询完成，没有执行业务修改。').waitFor();
  assert.deepEqual(requests[0].commands, [{ kind: 'skill', id: builtin.id }]);
  assert.deepEqual(requests[1].commands, requests[0].commands, 'retry keeps exact command selections');
  assert.equal(requests[1].operation_id, requests[0].operation_id);
  await page.getByRole('button', { name: '选择技能或工具', exact: true }).click();
  await page.getByRole('tab', { name: '工具', exact: true }).click();
  await page.getByRole('option', { name: /天气查询/ }).waitFor();
  await input.press('Escape'); assert.equal(await page.locator('.assistant-panel').count(), 1, 'Escape hides menu, not the assistant');
  await page.getByRole('button', { name: '选择技能或工具', exact: true }).click(); await page.getByRole('option', { name: /天气查询/ }).waitFor();
  await input.press('ArrowDown'); await input.press('Enter');
  assert.match(await page.locator('.command-chips').innerText(), /更新维修单/);
  await page.getByRole('button', { name: /移除 维修单与跟进 · 更新维修单/ }).click();
  await page.getByRole('button', { name: '技能与工具', exact: true }).click();
  await page.getByRole('button', { name: '安装技能', exact: true }).waitFor();
  await page.locator('.skill-manager input[type=file]').setInputFiles({ name: 'SKILL.md', mimeType: 'text/markdown', buffer: Buffer.from('---\nname: example\ndescription: shared guide\n---\nUse native queries.') });
  await page.locator('.skill-list').getByRole('button', { name: /共享示例技能/ }).waitFor();
  await page.screenshot({ path: path.join(output, 'skill-manager.png') });
  await page.locator('.skill-list').getByRole('button', { name: /共享示例技能/ }).click();
  await page.getByRole('button', { name: '删除共享技能', exact: true }).click();
  const confirmation = page.getByRole('dialog', { name: '删除共享技能', exact: true });
  await confirmation.getByRole('button', { name: '取消', exact: true }).click(); assert.equal(installed, true);
  await page.getByRole('button', { name: '删除共享技能', exact: true }).click();
  await confirmation.getByRole('button', { name: '确认', exact: true }).click();
  await page.getByRole('button', { name: '返回会话', exact: true }).waitFor();
  assert.equal(installed, false); assert.equal(await page.locator('.skill-list').getByRole('button', { name: /共享示例技能/ }).count(), 0);
  await page.getByRole('button', { name: '返回会话', exact: true }).click();

  for (const viewport of [{ width: 1440, height: 1000 }, { width: 1366, height: 768 }]) {
    await page.setViewportSize(viewport); await page.getByRole('button', { name: '选择技能或工具', exact: true }).click();
    await page.getByRole('option', { name: /维修单与跟进/ }).waitFor();
    const menu = await page.locator('.command-menu').boundingBox(), panel = await page.locator('.assistant-panel').boundingBox();
    assert(menu.x >= 0 && menu.y >= 0 && menu.x + menu.width <= viewport.width + 1 && menu.y + menu.height <= viewport.height, 'menu fits desktop viewport');
    assert(await page.locator('.assistant-panel').evaluate(el => el.scrollWidth <= el.clientWidth + 1), 'panel has no horizontal overflow');
    assert(panel.height > 400, 'assistant keeps usable conversation height');
    await page.screenshot({ path: path.join(output, `commands-${viewport.width}.png`) });
    await input.press('Escape');
  }
  await page.getByRole('button', { name: '图标设置', exact: true }).click();
  const settings = page.getByRole('dialog', { name: '图标设置', exact: true });
  await settings.getByLabel('尺寸', { exact: true }).focus(); await settings.getByLabel('尺寸', { exact: true }).press('End');
  await settings.getByRole('button', { name: '保存', exact: true }).click(); await settings.waitFor({ state: 'detached' });
  await input.fill('检查大图标布局');
  await page.waitForFunction(() => { const panel = document.querySelector('.assistant-panel')?.getBoundingClientRect(), bot = document.querySelector('.assistant-launcher')?.getBoundingClientRect(); return panel && bot && panel.right <= bot.left - 8; });
  const bot = await page.locator('.assistant-launcher').boundingBox(), send = await page.getByRole('button', { name: '发送问题', exact: true }).boundingBox(), textbox = await input.boundingBox();
  assert(send.x + send.width < bot.x && textbox.x + textbox.width < bot.x, '200px bot must not obscure sending or typing');
  await page.screenshot({ path: path.join(output, 'large-bot.png') });
  await input.fill('');
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor({ state: 'detached' });
  appearance = { color: 'encre', shape: 'cercle', size: 56, snap_back: false };
  await page.evaluate(value => {
    localStorage.setItem('lighthouse_appearance:command-fixture', JSON.stringify(value));
    localStorage.setItem('lighthouse_pos:command-fixture:launcher', JSON.stringify({ x: 36, y: 260 }));
    localStorage.removeItem('lighthouse_pos:command-fixture:panel');
  }, appearance);
  await page.reload();
  await page.waitForFunction(() => Math.abs((document.querySelector('.assistant-launcher')?.getBoundingClientRect().left ?? -100) - 36) < 1);
  const leftIcon = await page.locator('.assistant-launcher').boundingBox();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.waitForFunction(() => { const panel = document.querySelector('.assistant-panel')?.getBoundingClientRect(), bot = document.querySelector('.assistant-launcher')?.getBoundingClientRect(); return panel && bot && panel.left >= bot.right + 8; });
  const rightOpenedIcon = await page.locator('.assistant-launcher').boundingBox();
  assert(Math.abs(leftIcon.x - rightOpenedIcon.x) < 1 && Math.abs(leftIcon.y - rightOpenedIcon.y) < 1, 'right opening must preserve the icon position');
  assert.equal(Number.parseFloat(await page.locator('.composer').evaluate(el => getComputedStyle(el).paddingRight)), 12, 'composer needs no icon-specific clearance');
  await page.screenshot({ path: path.join(output, 'opens-right.png') });
  await page.evaluate(() => { const modal = document.createElement('dialog'); modal.id = 'native-fixture'; modal.textContent = '业务弹窗'; document.body.append(modal); modal.showModal(); });
  await page.getByRole('button', { name: '选择技能或工具', exact: true }).click();
  await page.getByRole('option', { name: /维修单与跟进/ }).click();
  assert.equal(await input.inputValue(), '', 'command remains interactive above a native modal');
  assert.equal(errors.length, 0, errors.join('\n'));
  console.log('[CommandsBrowser] OK: slash/keyboard/search, retry, shared install/delete, left/right opening without moving the icon, desktop bounds and native modal. Synthetic APIs only.');
} finally { await browser.close(); await new Promise(resolve => server.close(resolve)); }
