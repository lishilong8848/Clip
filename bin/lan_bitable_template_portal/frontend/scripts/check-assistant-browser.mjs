import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 隔离预览服务器由 Codex 启动/维护，测试只写 instance_id === 'isolated-ai-preview' 的实例。
// fixture_user cookie 契约（由 Codex 后端按固定 open_id 白名单实现 can_manage_settings）：
//   - 默认 / fixture_user=admin ：李世龙（白名单，can_manage_settings=true，可保存模型）
//   - fixture_user=ma：马进宇（白名单，可保存模型）
//   - fixture_user=h：H楼值班，role=building，白名单（有设置且可保存）
//   - fixture_user=other-admin：role=admin 但不在设置白名单（UI 无设置入口）
//   - fixture_user=user2：普通非白名单用户（无设置按钮）
const base = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19002';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

// 防误操作真实服务：任何 DELETE/PUT 之前先校验这是隔离预览实例。
{
  const health = await context.request.get(base + '/api/health');
  assert.equal(health.ok(), true, 'isolated server health endpoint must respond 200 before any write');
  const healthBody = await health.json();
  assert.equal(healthBody.instance_id, 'isolated-ai-preview', 'refuse to write against a non-isolated server');
  assert.equal(healthBody.ok, true, 'isolated health must report ok');
}

const TEST_MODEL_NAMES = new Set(['浏览器测试模型', 'H楼值班测试模型', '马进宇测试模型', '失败保留测试模型']);
// 重跑稳定：先读取当前假服务 settings（默认身份 admin 可读），清理上次失败的测试模型，绝不删除默认模型或第二模型。
{
  const settingsResp = await context.request.get(base + '/api/assistant/settings');
  assert.equal(settingsResp.ok(), true, 'GET /api/assistant/settings must be readable by admin to reset fixture');
  const settings = (await settingsResp.json())?.data || {};
  const leftovers = (settings.models || []).filter(m => TEST_MODEL_NAMES.has(String(m.name)));
  for (const m of leftovers) {
    const del = await context.request.put(base + '/api/assistant/settings', {
      headers: { origin: base },
      data: { action: 'delete', id: m.id },
    });
    assert.equal(del.ok(), true, `leftover test model ${m.id} must be removed before the run`);
    assert.equal((await del.json())?.ok, true, `leftover test model ${m.id} delete must report ok`);
  }
}

// 初始会话 DELETE 必须携带 Origin 头并通过同源校验，且必须成功。
const reset = await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } });
assert.equal(reset.ok(), true, 'initial conversation DELETE must succeed with a matching Origin header');
const answer = '这是隔离预览数据:A楼测试维修项目正在处理中。[1]';
const errors = [];
const page = await context.newPage();
page.on('pageerror', e => errors.push(e.message));
const requests = [];
page.on('request', r => { if (r.url().includes('/api/assistant/')) requests.push(r); });

function assistantRequests(pathPart) {
  return requests.filter(r => r.url().includes(pathPart));
}
function getCount(method, pathPart) {
  return requests.filter(r => r.method() === method && r.url().includes(pathPart)).length;
}

// 保存等待：点击前安装 PUT /api/assistant/settings 响应，检查 200/ok 且响应体绝无 API Key/cipher，
// 再等待对应 .profile-form 输入框真的显示保存后的名称/接口地址（新 UI 卡片不再展示 endpoint），
// 以及保存按钮重新可用，之后才允许检查 key 空白（不用 sleep）。
async function saveAndWait(expectName, expectEndpoint) {
  const putPromise = page.waitForResponse(
    r => r.request().method() === 'PUT'
      && r.url().includes('/api/assistant/settings')
      && (r.request().postData() || '').includes('"action":"upsert"'),
  );
  await page.getByRole('button', { name: '保存', exact: true }).click();
  const put = await putPromise;
  assert.equal(put.status(), 200, 'settings PUT must return 200');
  const putJson = await put.json();
  assert.equal(putJson.ok, true, 'settings PUT must report ok before we consider save finished');
  const raw = await put.text();
  assert(!/api.?key|cipher/i.test(raw), 'settings save response must never contain API Key or cipher');
  // 保存成功后表单会自动切到已保存模型的编辑态：名称与接口地址回显，endpoint 只在选中编辑表单显示。
  await page.waitForFunction(({ name, endpoint }) => {
    const inputs = Array.from(document.querySelectorAll('.profile-form input[type=text]'));
    const nameInput = inputs[0], endpointInput = inputs[1];
    return Boolean(nameInput && endpointInput && nameInput.value === name && endpointInput.value === endpoint);
  }, { name: expectName, endpoint: expectEndpoint });
  await page.waitForFunction(() => {
    const btn = Array.from(document.querySelectorAll('.settings-actions button')).find(b => (b.textContent || '').trim() === '保存');
    return Boolean(btn && !btn.disabled);
  });
}

async function openLauncherFor(pageLike, label = '打开灯塔助手') {
  const launcher = pageLike.getByRole('button', { name: label, exact: true });
  await launcher.waitFor();
  await launcher.click();
  await pageLike.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
}

// 主题校验：暗色中性绿（无蓝色面），不依赖精确色值。解析 rgb 通道判断亮度与绿色系偏离。
function parseRgb(rgb) {
  const m = /^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/.exec(rgb || '');
  return m ? { r: Number(m[1]), g: Number(m[2]), b: Number(m[3]) } : null;
}
async function assertDarkGreenSurface(locator, label) {
  const bg = await locator.evaluate(el => getComputedStyle(el).backgroundColor);
  const c = parseRgb(bg);
  assert(c, `${label} must resolve to an rgb() color (got "${bg}")`);
  assert(c.r < 130 && c.g < 130 && c.b < 130, `${label} must be a dark surface, got ${bg}`);
  assert(c.g >= c.b, `${label} must be a neutral green (green>=blue), got ${bg}`);
}

try {
  await page.goto(base);
  const launcher = page.getByRole('button', { name: '打开灯塔助手', exact: true });
  await launcher.waitFor();
  assert.equal(requests.length, 0, 'closed assistant must not query the backend');
  {
    // 默认入口必须在视口右下角，并且只用启动器自身的小尺寸（不能拿 520x680 面板定位只有 180x50 的启动器）。
    await page.waitForTimeout(60);
    const launcherOuter = await page.locator('.lighthouse').boundingBox();
    assert(launcherOuter, 'default launcher bounding box required');
    const vw = 1440, vh = 1000, m = 24;
    assert(Math.abs((launcherOuter.x + launcherOuter.width) - (vw - m)) <= 8
      && Math.abs((launcherOuter.y + launcherOuter.height) - (vh - m)) <= 8,
      `default launcher must sit at bottom-right (${JSON.stringify(launcherOuter)})`);
    assert(launcherOuter.width < 300 && launcherOuter.height < 100,
      `default launcher must use its own small size, not the 520x680 panel (${JSON.stringify(launcherOuter)})`);
  }
  await launcher.click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  const send = page.getByRole('button', { name: '发送问题', exact: true });
  await page.getByRole('textbox', { name: '询问灯塔助手' }).fill('A楼测试维修状态');
  await send.click();
  assert(await send.isDisabled(), 'no duplicate submit while responding');
  await page.getByText(answer, { exact: true }).waitFor();
  assert.equal(await page.getByRole('textbox', { name: '询问灯塔助手' }).inputValue(), '');

  // 暗色中性绿主题：面板表面与显著 UI 面无蓝色、非亮色（不断言精确 HEX）
  await assertDarkGreenSurface(page.locator('.assistant-panel'), 'assistant panel surface');
  await assertDarkGreenSurface(page.locator('.assistant-header'), 'assistant header surface');
  await assertDarkGreenSurface(page.locator('.composer'), 'composer surface');
  assert(await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1),
    'assistant panel must not overflow horizontally at desktop');

  // 模型切换：必须真实选中另一个模型，不能静默跳过（fixture 已配置两个模型），且切换后上下文/历史保留
  const modelSelect = page.locator('#assistant-model');
  await modelSelect.waitFor();
  assert(await page.locator('#assistant-model').count() === 1, 'model picker must expose native select#assistant-model');
  const optionCount = await modelSelect.locator('option').count();
  assert.ok(optionCount >= 2, `fixture configures two models, but saw ${optionCount} option(s)`);
  const currentValue = await modelSelect.inputValue();
  let chosenValue = null;
  const optionValues = await modelSelect.locator('option').evaluateAll(ops => ops.map(o => o.value));
  for (const value of optionValues) {
    if (value !== currentValue) { chosenValue = value; break; }
  }
  assert(chosenValue !== null, 'must find a second model to actually switch to');
  const switchPromise = page.waitForResponse(
    r => r.request().method() === 'PATCH' && r.url().includes('/api/assistant/conversation'),
  );
  await modelSelect.selectOption(chosenValue);
  const switched = await switchPromise;
  assert.equal(switched.status(), 200, 'model switch PATCH must return 200');
  assert.equal((await switched.json())?.ok, true, 'model switch PATCH must report ok');
  await page.waitForFunction((value) =>
    document.querySelector('#assistant-model')?.value === value,
    chosenValue,
  );
  await page.getByText(answer, { exact: true }).waitFor(); // 切换后历史仍在

  // 自动压缩由后端 agent 在回答过程中完成，不保留任何手动压缩入口或 compress 请求
  assert.equal(await page.getByRole('button', { name: '压缩上下文', exact: true }).count(), 0, 'manual compress button must not exist');
  assert.equal(assistantRequests('/api/assistant/compress').length, 0, 'no compress request may be submitted');
  assert((await page.locator('.turn').count()) >= 1, 'history must remain after sending');
  await page.getByText(answer, { exact: true }).waitFor();

  await page.screenshot({ path: path.join(output, 'assistant-desktop.png') });

  // 已完成 turn 才显示小型横向交互按钮；仅接受本地路径；点击后应用内路由变化，面板不消失且历史保留
  const completedTurn = page.locator('.turn', { hasText: answer });
  const interactionButton = completedTurn.getByRole('button', { name: /查看维修单/ });
  assert(await interactionButton.count() >= 1, 'completed turn must show an open-related-page interaction button');
  await interactionButton.first().click();
  await page.waitForFunction(() =>
    window.location.pathname === '/repair-management'
    && new URLSearchParams(window.location.search).get('scope') === 'A',
  );
  // 交互导航后面板仍保持打开（保持会话），先验证打开态与历史保留
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  await page.getByText(answer, { exact: true }).waitFor();
  // 明确收起，验证维修页面同样保留全局收起入口（启动器只在收起时出现）
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
  // 重开验证维修页面历史仍在
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  await page.getByText(answer, { exact: true }).waitFor();
  await page.goBack();
  await page.waitForFunction(() => window.location.pathname === '/');

  // 回到首页后明确收起/重开历史保持；reload 后从收起态重开仍保留历史
  if (await page.getByRole('button', { name: '收起助手', exact: true }).count()) {
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
  }
  await launcher.click();
  await page.getByText(answer, { exact: true }).waitFor();
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.reload();
  await launcher.waitFor();
  await launcher.click();
  await page.getByText(answer, { exact: true }).waitFor();

  // 手势拖动：折叠态使用长按（约 350ms）拖动启动器；短按打开；拖动后合成的 click 不得再触发打开。
  {
    // 明确先收起，确保从收起态（launcher 可见、面板消失）进入本段
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    const launcherBtn = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    await launcherBtn.waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'launcher gesture must start from a closed panel');
    const launcherBox = await launcherBtn.boundingBox();
    assert(launcherBox, 'launcher must be measurable');
    const lx = launcherBox.x + launcherBox.width / 2, ly = launcherBox.y + launcherBox.height / 2;

    // 长按鼠标并保持不动：不得打开面板
    await page.mouse.move(lx, ly);
    await page.mouse.down();
    await page.waitForTimeout(430);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'longpress held without move must not open');
    await page.mouse.up();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'longpress release without move must not open');

    // 再次长按后拖动：启动器必须移动且面板保持收起；随后浏览器合成的 click 被抑制，不重复打开
    const beforeDragBox = await launcherBtn.boundingBox();
    assert(beforeDragBox, 'launcher must be measurable again');
    const bx = beforeDragBox.x + beforeDragBox.width / 2, by = beforeDragBox.y + beforeDragBox.height / 2;
    await page.mouse.move(bx, by);
    await page.mouse.down();
    await page.waitForTimeout(430);
    await page.mouse.move(bx - 90, by - 70, { steps: 6 });
    await page.mouse.up();
    const afterDragBox = await launcherBtn.boundingBox();
    assert(afterDragBox, 'launcher must remain measurable after longpress drag');
    assert(afterDragBox.x !== beforeDragBox.x || afterDragBox.y !== beforeDragBox.y,
      `launcher must actually move after longpress drag (${JSON.stringify(beforeDragBox)} -> ${JSON.stringify(afterDragBox)})`);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'longpress drag must not open the panel');
    assert(await launcherBtn.isVisible(), 'launcher must stay visible after longpress drag');
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'no duplicate open after launcher drag (synthetic click suppressed)');

    // 短点击打开（且只打开一次）
    await launcherBtn.click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 1, 'short click must open exactly once');
  }

  // 触屏长按拖动：dispatch touch pointer 事件驱动长按 + 拖动，拖动后的合成 click 不得打开
  {
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    const tb = page.getByRole('button', { name: '打开灯塔助手', exact: true });
    await tb.waitFor();
    const box0 = await tb.boundingBox();
    assert(box0, 'launcher must be measurable for touch drag');
    const tx = box0.x + box0.width / 2, ty = box0.y + box0.height / 2;
    await page.evaluate(({ x, y }) => {
      const el = document.elementFromPoint(x, y);
      el?.dispatchEvent(new PointerEvent('pointerdown', {
        bubbles: true, cancelable: true, pointerId: 10, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, button: 0, buttons: 1,
      }));
    }, { x: tx, y: ty });
    await page.waitForTimeout(430);
    await page.evaluate(({ x, y }) => {
      document.dispatchEvent(new PointerEvent('pointermove', {
        bubbles: true, cancelable: true, pointerId: 10, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, buttons: 1,
      }));
    }, { x: tx - 90, y: ty - 70 });
    await page.evaluate(({ x, y }) => {
      document.dispatchEvent(new PointerEvent('pointerup', {
        bubbles: true, cancelable: true, pointerId: 10, pointerType: 'touch', isPrimary: true,
        clientX: x, clientY: y, button: 0, buttons: 0,
      }));
      // 浏览器在长按后通常会补发 click：必须被抑制
      const el = document.elementFromPoint(x, y);
      el?.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, clientX: x, clientY: y }));
    }, { x: tx - 90, y: ty - 70 });
    await page.waitForTimeout(60);
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'touch longpress drag must not open the panel');
    const boxTAfter = await tb.boundingBox();
    assert(boxTAfter, 'launcher must remain measurable after touch drag');
    assert(boxTAfter.x !== box0.x || boxTAfter.y !== box0.y, 'launcher must move after touch longpress drag');
    await tb.click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 1, 'touch short tap must open once');
  }

  // 拖动展开面板：拖动 header 非交互区域；交互按钮点击不触发拖动；reload 恢复坐标；resize 仍在视口内。
  // 上一段触屏短按后面板是打开态，先明确收起，确保从同步的收起态进入本段。
  {
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    const beforeBox = await page.locator('.lighthouse').boundingBox();
    assert(beforeBox, 'assistant container must be measurable');

    // header 标题区（非交互）拖动应移动面板
    const header = page.locator('.assistant-header');
    const headerBox = await header.boundingBox();
    assert(headerBox, 'assistant header must be measurable');
    const hx = headerBox.x + 40, hy = headerBox.y + headerBox.height / 2;
    await page.mouse.move(hx, hy);
    await page.mouse.down();
    await page.waitForTimeout(430); // header 需长按约 350ms 后才进入拖动
    await page.mouse.move(hx - 140, hy - 60, { steps: 8 });
    await page.mouse.up();
    const afterBox = await page.locator('.lighthouse').boundingBox();
    assert(afterBox, 'assistant container must remain measurable');
    assert(afterBox.x !== beforeBox.x || afterBox.y !== beforeBox.y, 'dragging the panel header must move the assistant');
    const viewport = page.viewportSize();
    const panel = await page.locator('.assistant-panel').boundingBox();
    assert(panel, 'assistant panel bounding box required');
    assert(panel.x >= 0 && panel.y >= 0 && panel.x + panel.width <= viewport.width && panel.y + panel.height <= viewport.height,
      `panel must stay within viewport after drag (${JSON.stringify(panel)})`);

    // 头部交互按钮点击不得拖动面板（settings 点击后位置不变且真实打开）
    const boxBeforeSettings = await page.locator('.lighthouse').boundingBox();
    await page.getByRole('button', { name: '模型设置', exact: true }).click();
    await page.getByLabel('API Key', { exact: true }).waitFor();
    const boxAfterSettings = await page.locator('.lighthouse').boundingBox();
    assert(Math.abs(boxAfterSettings.x - boxBeforeSettings.x) <= 1 && Math.abs(boxAfterSettings.y - boxBeforeSettings.y) <= 1,
      'clicking the settings header button must not drag the panel');
    await page.getByRole('button', { name: '返回会话', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();

    // 清空会话按钮点击也不触发拖动（打开确认框、位置不变、取消）
    const clearBtn = page.getByRole('button', { name: '清空会话', exact: true });
    if (await clearBtn.count()) {
      const b0 = await page.locator('.lighthouse').boundingBox();
      await clearBtn.click();
      await page.getByRole('dialog', { name: '清空会话', exact: true }).waitFor();
      const b1 = await page.locator('.lighthouse').boundingBox();
      assert(Math.abs(b1.x - b0.x) <= 1 && Math.abs(b1.y - b0.y) <= 1, 'clicking clear must not drag the panel');
      await page.getByRole('button', { name: '取消', exact: true }).click();
    }

    const storedX = afterBox.x, storedY = afterBox.y;
    // reload 保持展开态与坐标，只读取会话，不重新提交问题。
    await page.reload();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    const restoredBox = await page.locator('.lighthouse').boundingBox();
    assert(restoredBox, 'restored assistant container bounding box required');
    assert(Math.abs(restoredBox.x - storedX) <= 2 && Math.abs(restoredBox.y - storedY) <= 2,
      `dragged position must be restored after reload (${storedX},${storedY}) vs (${restoredBox.x},${restoredBox.y})`);
    // 明确先收起再重开，进入展开态以执行 resize 视口约束断言
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await page.setViewportSize({ width: 720, height: 700 });
    const panelAfterResize = await page.locator('.assistant-panel').boundingBox();
    assert(panelAfterResize.x >= 0 && panelAfterResize.y >= 0
      && panelAfterResize.x + panelAfterResize.width <= 720
      && panelAfterResize.y + panelAfterResize.height <= 700,
      `panel must stay within viewport after resize (${JSON.stringify(panelAfterResize)})`);
    await page.setViewportSize({ width: 1440, height: 1000 });
  }

  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await launcher.click();
  await page.getByText(answer, { exact: true }).waitFor();

  await page.getByRole('textbox', { name: '询问灯塔助手' }).fill('张三的身份证号和家庭住址');
  await send.click();
  await page.getByText('不能提供人员身份证号、家庭住址或其他私密身份信息。可以查询人员姓名、工号及有权限的业务信息。', { exact: true }).waitFor();

  // 管理员模型设置：Key 不回填、可添加/编辑/删除，含站内确认
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  const key = page.getByLabel('API Key', { exact: true });
  await key.waitFor();
  assert.equal(await key.inputValue(), '', 'the API key must never come back from settings');
  await page.screenshot({ path: path.join(output, 'assistant-settings-desktop.png') });

  // 添加模型
  await page.getByRole('button', { name: '添加模型', exact: true }).click();
  await page.getByLabel('显示名称', { exact: true }).fill('浏览器测试模型');
  await page.getByLabel('接口地址', { exact: true }).fill('https://example.test/v1/chat/completions');
  await page.getByLabel('模型名称', { exact: true }).fill('gpt-browser-test');
  await key.fill('sk-test-not-replayed');
  await saveAndWait('浏览器测试模型', 'https://example.test/v1/chat/completions');
  assert.equal(await key.inputValue(), '', 'API key must never be echoed back after save');

  // 新增成功不假 dirty：保存后回到"编辑既有模型"且表单干净，切换模型不应弹出未保存确认
  const defaultCard = page.locator('.model-card', { hasText: '灯塔默认模型' });
  await defaultCard.getByRole('button', { name: '编辑', exact: true }).click();
  await page.waitForFunction((name) =>
    Array.from(document.querySelectorAll('.profile-form input[type=text]')).some(el => el.value === name),
    '灯塔默认模型',
  );
  assert.equal(await page.getByRole('dialog', { name: '切换编辑模型' }).count(), 0, 'clean add-save must not prompt dirty confirm');
  const addedCardAgain = page.locator('.model-card', { hasText: '浏览器测试模型' });
  await addedCardAgain.getByRole('button', { name: '编辑', exact: true }).click();
  await page.waitForFunction((name) =>
    Array.from(document.querySelectorAll('.profile-form input[type=text]')).some(el => el.value === name),
    '浏览器测试模型',
  );

  // 未保存切换取消内容保留：修改后切到别的模型，取消确认后内容仍在
  await page.getByLabel('模型名称', { exact: true }).fill('gpt-browser-dirty-cancel');
  await defaultCard.getByRole('button', { name: '编辑', exact: true }).click();
  await page.getByRole('dialog', { name: '切换编辑模型', exact: true }).waitFor();
  await page.getByRole('button', { name: '取消', exact: true }).click();
  assert.equal(await page.getByLabel('模型名称', { exact: true }).inputValue(), 'gpt-browser-dirty-cancel', 'cancel must preserve unsaved content');
  // 恢复为保存值，令表单回到干净状态，便于继续编辑
  await page.getByLabel('模型名称', { exact: true }).fill('gpt-browser-test');

  // 当前选中的模型已在编辑表单中（endpoint 只在此处显示），直接改 endpoint 并保存；Key 留空不回填
  assert.equal(await key.inputValue(), '', 'API key must not be backfilled when editing');
  await page.getByLabel('接口地址', { exact: true }).fill('https://example.test/v2/chat/completions');
  await saveAndWait('浏览器测试模型', 'https://example.test/v2/chat/completions');

  // 默认模型命令存在
  const modelCardForDefault = page.locator('.model-card', { hasText: '浏览器测试模型' });
  assert(await modelCardForDefault.getByRole('button', { name: '设为默认', exact: true }).count() > 0, 'set default command should exist');

  // 删除新增模型（站内确认）：当前正在编辑该模型时也可直接删除，无需先切换编辑目标
  const deletePromise = page.waitForResponse(
    r => r.request().method() === 'PUT'
      && r.url().includes('/api/assistant/settings')
      && (r.request().postData() || '').includes('"action":"delete"'),
  );
  const editedCardDelete = addedCardAgain.getByRole('button', { name: '删除', exact: true });
  assert.equal(await editedCardDelete.getAttribute('aria-label'), '删除', 'delete button must stay labelled 删除 while editing the model');
  assert.equal(await editedCardDelete.isDisabled(), false, 'currently-edited model must still be deletable without switching targets');
  await editedCardDelete.click();
  await page.getByRole('dialog', { name: '删除模型', exact: true }).waitFor();
  await page.getByRole('button', { name: '确认', exact: true }).click();
  const deleted = await deletePromise;
  assert.equal(deleted.status(), 200, 'delete settings PUT must return 200');
  assert.equal((await deleted.json())?.ok, true, 'delete settings PUT must report ok');
  await page.waitForFunction(() =>
    !Array.from(document.querySelectorAll('.model-card')).some(el => el.textContent?.includes('浏览器测试模型')),
  );

  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();

  // 返回菜单 context 不丢失：返回会话后历史仍在，再次打开设置模型列表仍在
  await page.getByText(answer, { exact: true }).waitFor();
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await key.waitFor();
  assert(await page.locator('.model-card').count() >= 1, 'returning must keep model list context');
  await page.screenshot({ path: path.join(output, 'assistant-settings-desktop-2.png') });
  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();

  // 设置保存失败：拦截下一次 settings PUT 返回失败，输入必须保留、错误可见、可取消返回
  let failNextSettingsSave = true;
  await page.route('**/api/assistant/settings', async route => {
    if (route.request().method() !== 'PUT' || !(route.request().postData() || '').includes('"action":"upsert"')) {
      await route.continue();
      return;
    }
    if (failNextSettingsSave && failNextSettingsSave === true) {
      failNextSettingsSave = false;
      await route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ ok: false, error: '模拟的保存失败' }) });
      return;
    }
    await route.continue();
  });
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await key.waitFor();
  await page.getByRole('button', { name: '添加模型', exact: true }).click();
  await page.getByLabel('显示名称', { exact: true }).fill('失败保留测试模型');
  await page.getByLabel('接口地址', { exact: true }).fill('https://example.test/fail/chat/completions');
  await page.getByLabel('模型名称', { exact: true }).fill('gpt-fail-retain');
  await key.fill('sk-fail-retain');
  await page.getByRole('button', { name: '保存', exact: true }).click();
  await page.getByText('模拟的保存失败', { exact: true }).waitFor();
  assert.equal(await page.getByLabel('显示名称', { exact: true }).inputValue(), '失败保留测试模型', 'failed save must preserve the name input');
  assert.equal(await page.getByLabel('接口地址', { exact: true }).inputValue(), 'https://example.test/fail/chat/completions', 'failed save must preserve the endpoint input');
  assert.equal(await page.getByLabel('模型名称', { exact: true }).inputValue(), 'gpt-fail-retain', 'failed save must preserve the model input');
  await page.unroute('**/api/assistant/settings');

  // dirty 返回会话取消保留：返回会话弹出确认，取消后输入仍在，再次确认后关闭设置
  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('dialog', { name: '返回会话', exact: true }).waitFor();
  await page.getByRole('button', { name: '取消', exact: true }).click();
  assert.equal(await page.getByLabel('显示名称', { exact: true }).inputValue(), '失败保留测试模型', 'cancel return must keep unsaved settings content');
  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('dialog', { name: '返回会话', exact: true }).waitFor();
  await page.getByRole('button', { name: '确认', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();

  // HTML 安全：允许回复排版，但主动标签与事件属性不会执行。
  await page.getByRole('textbox', { name: '询问灯塔助手' }).fill('长回答 HTML');
  await send.click();
  await page.locator('.assistant-rich-reply').filter({ hasText: '是普通文本,不执行。' }).last().waitFor();
  assert.equal(await page.locator('.lighthouse img').count(), 0, 'model text must not execute HTML');

  // 移动端布局不超出视口，保持 24px 边距
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const rect = document.querySelector('.assistant-panel')?.getBoundingClientRect();
    return rect && rect.left >= 0 && rect.top >= 0 && rect.right <= innerWidth && rect.bottom <= innerHeight;
  }, undefined, { timeout: 3000 });
  const panel = await page.locator('.assistant-panel').boundingBox();
  await page.screenshot({ path: path.join(output, 'assistant-mobile.png') });
  assert(panel.x >= 0 && panel.y >= 0 && panel.x + panel.width <= 390 && panel.y + panel.height <= 844,
    `assistant panel must fit mobile viewport (${JSON.stringify(panel)})`);
  assert(await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1));

  // 移动端模型设置布局截图
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await key.waitFor();
  assert(panel.x >= 0 && panel.y >= 0 && panel.x + panel.width <= 390 && panel.y + panel.height <= 844, 'mobile settings must fit the viewport');
  await page.screenshot({ path: path.join(output, 'assistant-settings-mobile.png') });
  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();

  await page.getByRole('button', { name: '清空会话', exact: true }).click();
  await page.getByRole('dialog', { name: '清空会话', exact: true }).waitFor();
  await page.getByRole('button', { name: '确认', exact: true }).click();
  await page.getByText('今天有什么需要处理的？', { exact: true }).waitFor();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).fill('长回答 1');
  await send.click();
  await page.locator('.turn').last().locator('.assistant-rich-reply').filter({ hasText: '已批准' }).waitFor();
  assert(await page.locator('.thread').evaluate(e => e.scrollHeight > e.clientHeight), 'seeded history must be scrollable');
  await page.route('**/api/assistant/agent', async route => {
    const response = await route.fetch();
    await new Promise(resolve => setTimeout(resolve, 3500));
    await route.fulfill({ response });
  });
  await page.getByRole('textbox', { name: '询问灯塔助手' }).fill('长回答 2');
  const beforeLongGets = getCount('GET', '/api/assistant/conversation');
  const secondChat = page.waitForResponse(r => r.url().includes('/api/assistant/agent') && r.request().postData()?.includes('长回答 2'));
  await send.click();
  await page.locator('.turn').last().locator('.pending').waitFor();
  // 已有可滚动历史时上滚阅读，后台轮询及当前回答到达均不能强制跳底。
  await page.locator('.thread').evaluate(e => { e.scrollTop = Math.max(0, e.scrollHeight - e.clientHeight - 160); });
  await secondChat;
  await page.locator('.turn').last().locator('.pending').waitFor({ state: 'hidden' });
  const scrollTopAfter = await page.locator('.thread').evaluate(e => e.scrollTop);
  const scrollInfo = await page.locator('.thread').evaluate(e => ({ top: e.scrollTop, max: e.scrollHeight - e.clientHeight }));
  assert(scrollInfo.max > 0, 'long answer thread must be scrollable');
  assert(scrollTopAfter <= scrollInfo.max - 40, `reading history must not be forced to bottom (scrollTop=${scrollTopAfter}, max=${scrollInfo.max})`);
  const afterLongGets = getCount('GET', '/api/assistant/conversation');
  // 下一次请求仍轮询：回答期间应持续有 conversation GET 轮询，而不是只停留在发送后的单次读取
  assert(afterLongGets > beforeLongGets, `background conversation polling must continue during a long answer (before=${beforeLongGets}, after=${afterLongGets})`);
  assert(await page.locator('.thread').evaluate(e => e.scrollHeight > e.clientHeight));
  await page.unroute('**/api/assistant/agent');
  await page.screenshot({ path: path.join(output, 'assistant-long-mobile.png') });

  // 返回桌面视口，继续全页入口检查
  await page.setViewportSize({ width: 1440, height: 1000 });

  // 恶意来源链接不可跳外站 + 未知 interaction 隐藏：注入带 external/\ 的来源，安全地降级为本地 '/' 且不渲染外链按钮
  {
    await page.route('**/api/assistant/conversation', async route => {
      if (route.request().method() === 'GET') {
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, data: {
          configured: true, enabled: true, can_manage_settings: false,
          conversation_id: 'evil-c', model_name: 'm', model_id: '1',
          model_options: [{ id: '1', name: 'm' }],
          turns: [{
            operation_id: 'evil-o1', question: 'q', answer: '恶意来源测试', status: 'completed',
            sources: [
              { number: 1, title: '外部站', url: 'https://evil.example.com/a' },
              { number: 2, title: '反斜杠', url: '/\\evil' },
              { number: 3, title: '反斜杠编码', url: '/%5cevil' },
              { number: 4, title: '合法本地', url: '/repair-management?scope=A' },
            ],
            interactions: [{ kind: 'navigate', label: '恶意跳转', url: 'https://evil.example.com/x' }],
          }],
        }})});
      } else {
        await route.continue();
      }
    });
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByText('恶意来源测试', { exact: true }).waitFor();
    const externalHrefs = await page.locator('.sources a[href^="http"]').count();
    assert.equal(externalHrefs, 0, 'malicious source links must never resolve to an external href');
    const backslashHref = await page.locator('.sources a').filter({ hasText: /反斜杠$/ }).getAttribute('href');
    assert.equal(backslashHref, '/', 'source containing a leading-slash single backslash (/\\evil) must be rejected to a safe local href');
    const encodedHref = await page.locator('.sources a').filter({ hasText: /反斜杠编码$/ }).getAttribute('href');
    assert.equal(encodedHref, '/', 'source containing /%5cevil must be rejected to a safe local href');
    const legalHref = await page.locator('.sources a', { hasText: '合法本地' }).getAttribute('href');
    assert.equal(legalHref, '/repair-management?scope=A', 'legit leading-slash local path must be preserved');
    assert.equal(await page.getByRole('button', { name: /恶意跳转/ }).count(), 0, 'unknown/malicious interaction must be hidden');
    await page.unroute('**/api/assistant/conversation');
  }

  // 账号隔离 + 权限：
  // 1) 普通非白名单用户 user2：无设置按钮、不可见对方历史
  {
    const second = await browser.newContext({ viewport: { width: 390, height: 844 } });
    await second.addCookies([{ name: 'fixture_user', value: 'user2', url: base }]);
    const otherPage = await second.newPage();
    await otherPage.goto(base);
    await otherPage.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await otherPage.getByText('今天有什么需要处理的？', { exact: true }).waitFor();
    assert.equal(await otherPage.locator('.turn').count(), 0, 'different accounts must not see history');
    assert.equal(await otherPage.getByRole('button', { name: '模型设置', exact: true }).count(), 0, 'non-whitelist non-admin must not see model settings');
    await second.close();
  }

  // 2) 非白名单 admin（other-admin）：无设置按钮
  {
    const adminOuter = await browser.newContext({ viewport: { width: 390, height: 844 } });
    await adminOuter.addCookies([{ name: 'fixture_user', value: 'other-admin', url: base }]);
    const adminPage = await adminOuter.newPage();
    await adminPage.goto(base);
    await adminPage.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await adminPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    assert.equal(await adminPage.getByRole('button', { name: '模型设置', exact: true }).count(), 0, 'admin outside settings whitelist must not see model settings');
    await adminOuter.close();
  }

  // 3) H楼值班（h，role=building 但白名单）：有设置按钮且可保存成功后返回会话
  {
    const duty = await browser.newContext({ viewport: { width: 390, height: 844 } });
    await duty.addCookies([{ name: 'fixture_user', value: 'h', url: base }]);
    const dutyPage = await duty.newPage();
    await dutyPage.goto(base);
    await dutyPage.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await dutyPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await dutyPage.getByRole('button', { name: '模型设置', exact: true }).click();
    await dutyPage.getByLabel('API Key', { exact: true }).waitFor();
    await dutyPage.getByRole('button', { name: '添加模型', exact: true }).click();
    await dutyPage.getByLabel('显示名称', { exact: true }).fill('H楼值班测试模型');
    await dutyPage.getByLabel('接口地址', { exact: true }).fill('https://example.test/h-duty/chat/completions');
    await dutyPage.getByLabel('模型名称', { exact: true }).fill('gpt-h-duty');
    await dutyPage.getByLabel('API Key', { exact: true }).fill('sk-h-duty');
    const putPromise = dutyPage.waitForResponse(
      r => r.request().method() === 'PUT'
        && r.url().includes('/api/assistant/settings')
        && (r.request().postData() || '').includes('"action":"upsert"'),
    );
    await dutyPage.getByRole('button', { name: '保存', exact: true }).click();
    const put = await putPromise;
    assert.equal(put.status(), 200, 'H-duty whitelisted non-admin settings PUT must return 200');
    assert.equal((await put.json())?.ok, true, 'H-duty settings PUT must report ok');
    await dutyPage.waitForFunction((name) =>
      Array.from(document.querySelectorAll('.profile-form input[type=text]')).some(el => el.value === name),
      'H楼值班测试模型',
    );
    await dutyPage.getByRole('button', { name: '返回会话', exact: true }).click();
    await dutyPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await duty.close();
  }

  // 4) 马进宇（ma，白名单）：有设置按钮且可保存
  {
    const ma = await browser.newContext({ viewport: { width: 390, height: 844 } });
    await ma.addCookies([{ name: 'fixture_user', value: 'ma', url: base }]);
    const maPage = await ma.newPage();
    await maPage.goto(base);
    await maPage.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await maPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await maPage.getByRole('button', { name: '模型设置', exact: true }).click();
    await maPage.getByLabel('API Key', { exact: true }).waitFor();
    await maPage.getByRole('button', { name: '添加模型', exact: true }).click();
    await maPage.getByLabel('显示名称', { exact: true }).fill('马进宇测试模型');
    await maPage.getByLabel('接口地址', { exact: true }).fill('https://example.test/ma/chat/completions');
    await maPage.getByLabel('模型名称', { exact: true }).fill('gpt-ma');
    await maPage.getByLabel('API Key', { exact: true }).fill('sk-ma');
    const putPromise = maPage.waitForResponse(
      r => r.request().method() === 'PUT'
        && r.url().includes('/api/assistant/settings')
        && (r.request().postData() || '').includes('"action":"upsert"'),
    );
    await maPage.getByRole('button', { name: '保存', exact: true }).click();
    const put = await putPromise;
    assert.equal(put.status(), 200, 'ma whitelisted settings PUT must return 200');
    assert.equal((await put.json())?.ok, true, 'ma settings PUT must report ok');
    await maPage.waitForFunction((name) =>
      Array.from(document.querySelectorAll('.profile-form input[type=text]')).some(el => el.value === name),
      '马进宇测试模型',
    );
    await maPage.getByRole('button', { name: '返回会话', exact: true }).click();
    await maPage.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await ma.close();
  }

  // 忙时重开：延迟 agent 请求时收起再打开，只重新 GET + 轮询显示原请求，绝不新增 POST
  {
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    const chatsBeforeReopen = getCount('POST', '/api/assistant/agent');
    let releaseChat = () => {};
    const chatGate = new Promise(resolve => { releaseChat = resolve; });
    await page.route('**/api/assistant/agent', async route => {
      if (route.request().method() === 'POST') {
        await chatGate;
        await route.continue();
      } else {
        await route.continue();
      }
    });
    // 模拟后端：忙时 GET 会返回原始请求仍在处理（pending），供收起后重开显示进度。
    await page.route('**/api/assistant/conversation', async route => {
      if (route.request().method() === 'GET') {
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, data: {
          configured: true, enabled: true, can_manage_settings: false,
          conversation_id: 'busy-c', model_name: 'm', model_id: '1',
          model_options: [{ id: '1', name: 'm' }],
          turns: [{ operation_id: 'busy-o', question: '忙时重开问题', status: 'pending', answer: '' }],
        } }) });
      } else {
        await route.continue();
      }
    });
    const reopenInput = page.getByRole('textbox', { name: '询问灯塔助手' });
    await reopenInput.fill('忙时重开问题');
    await page.getByRole('button', { name: '发送问题', exact: true }).click();
    await page.locator('.turn').last().locator('.pending').first().waitFor();
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await reopenInput.waitFor();
    const chatsAfterReopen = getCount('POST', '/api/assistant/agent');
    assert.equal(chatsAfterReopen, chatsBeforeReopen + 1, 'reopening while busy must not create a new chat POST');
    await page.waitForFunction(() => {
      const turns = Array.from(document.querySelectorAll('.turn'));
      return turns.some(t => (t.textContent || '').includes('忙时重开问题'));
    }, undefined, { timeout: 5000 });
    assert(await page.locator('.pending').count() >= 1, 'reopen while busy must display the original in-flight request progress');
    await page.unroute('**/api/assistant/conversation');
    releaseChat();
    // 等延迟请求真正返回后恢复稳定状态，再安全移除 agent 拦截
    await page.waitForFunction(() => document.querySelector('.pending') === null, undefined, { timeout: 8000 });
    await page.unroute('**/api/assistant/agent');
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
  }

  // 已收起时原 agent 请求完成后重开：draft 已清空、绝不重复 POST（Codex 修复：收起也接收结果并清空已提交 draft）
  {
    // 确保面板打开以便发起原问题
    if (await page.getByRole('button', { name: '收起助手', exact: true }).count() === 0) {
      await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    }
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    const chatsBeforeComplete = getCount('POST', '/api/assistant/agent');
    let releaseCompletedChat = () => {};
    const completeGate = new Promise(resolve => { releaseCompletedChat = resolve; });
    await page.route('**/api/assistant/agent', async route => {
      if (route.request().method() === 'POST') {
        await completeGate;
        await route.continue();
      } else {
        await route.continue();
      }
    });
    const completeInput = page.getByRole('textbox', { name: '询问灯塔助手' });
    await completeInput.fill('收起后完成的原问题');
    await page.getByRole('button', { name: '发送问题', exact: true }).click();
    // 收起面板，让原请求在收起状态下完成
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    assert.equal(await page.locator('.assistant-panel').count(), 0, 'panel must be collapsed while the original chat completes');
    const completeRespPromise = page.waitForResponse(
      r => r.request().method() === 'POST' && r.url().includes('/api/assistant/agent'),
    );
    releaseCompletedChat();
    const completeResp = await completeRespPromise;
    assert.equal(completeResp.status(), 200, 'agent POST must succeed while collapsed');
    // 收起状态重开：draft 必须为空，且不产生第二次 POST
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await completeInput.waitFor();
    assert.equal(await completeInput.inputValue(), '', 'completed chat draft must be cleared when reopening after collapse');
    const chatsAfterComplete = getCount('POST', '/api/assistant/agent');
    assert.equal(chatsAfterComplete, chatsBeforeComplete + 1, 'reopening after a completed collapsed chat must not create a second POST');
    await page.waitForFunction(() => {
      const turns = Array.from(document.querySelectorAll('.turn'));
      return turns.some(t => (t.textContent || '').includes('收起后完成的原问题'));
    });
    await page.unroute('**/api/assistant/agent');
    await page.getByRole('button', { name: '收起助手', exact: true }).click();
  }

  // 助手在登录后全局挂载（跨应用内路由保持）：非首页同样有收起入口
  await page.goto(base + '/learning?scope=A');
  await page.getByRole('heading', { name: '画像学练', exact: true }).waitFor();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
  assert.equal(errors.length, 0, errors.join('\n'));
  console.log('Assistant production UI checks passed: agent endpoint (POST /api/assistant/agent), native select#assistant-model model switch, context preserved, no-manual-compress, dark-green palette (no blue surface), launcher longpress (mouse+touch) + no-open-after-drag + short-click, header longpress350 drag + click-not-drag, reload/resize bounds, account isolation, whitelist settings (admin/ma/h yes, other-admin/user2 no), submit guard, privacy, model settings, settings-failure retention, dirty return retention, late-GET/read-history, busy-reopen, HTML safety, malicious-source safety, desktop/mobile, interactions/route and full-page launcher visibility.');
} catch (error) {
  console.error(await page.locator('.lighthouse').innerText().catch(() => 'assistant unavailable'));
  await page.screenshot({ path: path.join(output, 'assistant-failure.png') });
  throw error;
} finally { await browser.close(); }
