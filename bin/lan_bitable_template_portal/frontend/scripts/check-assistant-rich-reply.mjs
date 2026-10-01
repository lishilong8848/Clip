import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

// 隔离预览服务器由 Codex 启动/维护，本脚本只写 instance_id === 'isolated-ai-preview' 的实例。
// 目标：验证生产富文本答案（.assistant-rich-reply 取代 .answer 内原 <p>{{ answer }}</p>）的
//   - Markdown 渲染：标题/加粗/斜体/删除线/有序无序列表/GFM 表格/长代码围栏（内部滚动）
//   - 颜色：<span style="color:#d98b75"> 与 <font color="red"> 显示真实计算色；
//     background:url + position:fixed 的 span 只保留 color，其余危险样式被剥除
//   - 安全：script/img/svg/iframe/style/form 与事件属性、javascript/data/file/href 不执行、不外联；
//     内部 API（/api/backend/shutdown、/%61pi/backend/shutdown）无 href；
//     外链 https://example.com/reference 保持 target=_blank + rel=noopener noreferrer；
//     本地 /repair-management?scope=A 保留
//   - 注入方式：拦截 POST /api/assistant/agent，克隆先前 GET /api/assistant/conversation 的
//     data（保留全部 settings 元数据），追加同 operation_id/question 的 completed 回合，
//     全程无模型/云端调用；reload 时拦截 GET conversation 重复注入以验证持久化
//   - desktop(1440x1000) 与 mobile(390x844) 无 page/panel 横向溢出；宽表格/代码块走内部滚动；
//     composer 保持可见
const base = process.env.ASSISTANT_PREVIEW_URL || 'http://127.0.0.1:19002';
const output = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../../output/playwright/assistant-rich-reply');
await mkdir(output, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });

// 任何写入前先校验隔离实例。
{
  const health = await context.request.get(base + '/api/health');
  assert.equal(health.ok(), true, 'isolated server health endpoint must respond 200 before any write');
  const healthBody = await health.json();
  assert.equal(healthBody.instance_id, 'isolated-ai-preview', 'refuse to write against a non-isolated server');
  assert.equal(healthBody.ok, true, 'isolated health must report ok');
}

// 初始会话 DELETE 必须携带 Origin 头并通过同源校验（仅隔离实例允许）。
const reset = await context.request.delete(base + '/api/assistant/conversation', { headers: { origin: base } });
assert.equal(reset.ok(), true, 'initial conversation DELETE must succeed with a matching Origin header');

const pageErrors = [];
const dialogs = [];
const externalRequests = [];
const page = await context.newPage();
page.on('pageerror', e => pageErrors.push(e.message));
page.on('dialog', async d => { dialogs.push(d.message()); await d.dismiss().catch(() => {}); });
page.on('request', r => {
  let url;
  try { url = new URL(r.url()); } catch { return; }
  const baseUrl = new URL(base);
  // 精确同源守卫：仅对 HTTP(S) 且 origin !== base.origin 计为外联；不再豁免任何本地主机名
  // （例如 http://localhost 默认端口 80 也必须被判定为不同源）。
  if (/^https?:$/.test(url.protocol) && url.origin !== baseUrl.origin) externalRequests.push(r.url());
});

// 富文本样本：顶部为真实用户样例（含 [1] 资料编号、总体情况加粗、各模块 0 项），
// 其下另起独立标题继续覆盖高级 markdown / 颜色 / 安全链接特性。
const RICH_MARKDOWN = `### 未完成工作统计

根据接口结果[1]当前2026-10-01 07:32，**总体情况**:未完成工作总数0项，其中进行中0项、已完成0项、需关注0项，**各模块明细**

- 通告：0项
- 事件：0项
- 检修：0项
- 维护单：0项
- 水耗：0项

全部：今日资料编号[1]

---

### 高级富文本渲染样本

**总体情况**：本轮共检查 **5** 台设备，其中 <span style="color:#d98b75">需关注</span> 2 台，<font color="red">旧颜色</font> 标记已迁移，<span style="background:url(https://evil.example.com/bg.png);position:fixed;color:rgb(10,20,30)">样式受限样本</span>。

**各模块明细**：

- 模块 A：正常，*斜体说明*，含 \`code\`。
- 模块 B：~~已停用~~ 待复核。
- 模块 C：**重要** 设备，需持续观察。
- 模块 D：定期保养中，~~无需处理~~。
- 模块 E：历史遗留，本月升级。

处理步骤如下：

1. 先读取各模块状态；
2. 再核对设备台账；
3. 最后生成汇总报告。

| 模块 | 状态 | 负责人 | 优先级 | 备注 | 上次巡检 |
| --- | --- | --- | --- | --- | --- |
| A | 正常 | 张三 | 低 | 无异常 | 2026-09-01 |
| B | 需关注 | 李四 | 高 | 需复核 | 2026-09-05 |
| C | 正常 | 王五 | 中 | 例行 | 2026-09-08 |

长代码片段（内部滚动）：

\`\`\`python
def generate_long_line():
    return "这是一行非常非常非常非常非常非常非常非常非常非常非常非常非常非常非常非常非常非常非常长的代码，用于验证代码块在窄屏下能否内部横向滚动且不影响整体布局 "
\`\`\`

[外部参考链接](https://example.com/reference)
[编码引用外部链接](https://example.com/reference?q=maintenance%20plan&ratio=50%25)
[本地维修单](/repair-management?scope=A)
[内部接口 /api/backend/shutdown](/api/backend/shutdown)
[编码内部接口](/%61pi/backend/shutdown)
[协议相对链接](//evil.example)
[编码反斜杠链接](/%5cevil.example)
[编码 javascript 链接](%6a%61%76%61%73%63%72%69%70%74:alert(2))

<script>window.__rich_xss__=1</script>
<img src="https://evil.example.com/x.png" alt="图片决策依据：请查看附件说明" onerror="window.__rich_xss__=2">

**图片已安全移除**：现场影像以附件形式提供，请查阅附件 IMG_2026-1001.jpg。
<svg onload="window.__rich_xss__=3"><circle r="1"/></svg>
<iframe src="https://evil.example.com/frame"></iframe>
<form action="https://evil.example.com/post"><button name="x">提交</button></form>
<style>body{color:red}</style>
<div onclick="window.__rich_xss__=4">点击区不会执行事件</div>
[点击 javascript 链接](javascript:alert(1))
[data 链接](data:text/html,<script>1</script>)
[file 链接](file:///etc/passwd)`;

// 捕获真实 GET conversation.data 作为注入基底；注入后 reload 时复用同一份 fakeData。
let realConversationData = null;
let fakeData = null;

await page.route('**/api/assistant/conversation', async route => {
  if (route.request().method() !== 'GET') { await route.continue(); return; }
  if (fakeData) {
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, data: fakeData }) });
    return;
  }
  const response = await route.fetch();
  const body = await response.json();
  if (body && body.ok && body.data) realConversationData = body.data;
  await route.fulfill({ response });
});

// 拦截 POST /api/assistant/agent：不落到真实模型/后端，直接注入完成的富文本回合。
await page.route('**/api/assistant/agent', async route => {
  if (route.request().method() !== 'POST') { await route.continue(); return; }
  const post = JSON.parse(route.request().postData() || '{}');
  assert(realConversationData, 'must have captured a prior GET conversation.data before injecting');
  const cloned = JSON.parse(JSON.stringify(realConversationData));
  // 保留全部 settings 元数据，仅追加 scoped completed turn，并带上相同的 operation_id/question。
  cloned.conversation_id = realConversationData.conversation_id;
  cloned.enabled = realConversationData.enabled;
  cloned.configured = realConversationData.configured;
  cloned.can_manage_settings = realConversationData.can_manage_settings;
  cloned.model_id = realConversationData.model_id;
  cloned.model_name = realConversationData.model_name;
  cloned.model_options = realConversationData.model_options;
  if (realConversationData.context) cloned.context = JSON.parse(JSON.stringify(realConversationData.context));
  cloned.busy = false;
  cloned.phase = '';
  const turns = Array.isArray(cloned.turns) ? cloned.turns.slice() : [];
  turns.push({
    operation_id: post.operation_id,
    question: post.question,
    answer: RICH_MARKDOWN,
    status: 'completed',
    scopes: realConversationData.scopes || [],
    model_name: cloned.model_name || '测试模型',
    interactions: [],
  });
  cloned.turns = turns;
  fakeData = JSON.parse(JSON.stringify(cloned));
  await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, data: fakeData }) });
});

try {
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  assert(realConversationData, 'opening the assistant must have produced a real GET conversation.data');

  const input = page.getByRole('textbox', { name: '询问灯塔助手' });
  await page.evaluate(() => { window.__rich_xss__ = 0; });

  const question = '测试富文本渲染与安全';
  await input.fill(question);
  const posted = page.waitForResponse(r => r.request().method() === 'POST' && r.url().includes('/api/assistant/agent'));
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  await posted;
  await page.locator('.turn').last().locator('.assistant-rich-reply').waitFor({ timeout: 15000 });

  const lastTurn = page.locator('.turn').last();
  const answer = lastTurn.locator('.answer');
  const reply = lastTurn.locator('.assistant-rich-reply');

  // ---- .assistant-rich-reply 取代 .answer > p（raw answer）仅用于答案 ----
  assert.equal(await answer.locator(':scope > p').count(), 0, '.assistant-rich-reply must replace the raw .answer p for answers');
  const replyText = await reply.innerText();
  assert(!replyText.includes('**'), 'rendered text must not contain raw **');
  assert(replyText.includes('总体情况'), 'summary text present');

  // ---- 真实用户样例：0 计数文本与 [1] 资料编号必须原样保留；总体情况必须是 strong 而非 raw ** ----
  for (const frag of ['未完成工作总数0项', '进行中0项', '已完成0项', '需关注0项',
    '通告：0项', '事件：0项', '检修：0项', '维护单：0项', '水耗：0项', '资料编号[1]']) {
    assert(replyText.includes(frag), `user sample fragment "${frag}" must be preserved in rendered text`);
  }
  {
    const strongTotal = reply.locator('strong').filter({ hasText: '总体情况' }).first();
    await strongTotal.waitFor();
    assert((await strongTotal.innerText()).includes('总体情况'), '"总体情况" must be rendered inside a <strong>');
  }

  // ---- Markdown 特性 ----
  assert((await reply.locator('h1, h2, h3, h4, h5, h6').count()) >= 1, 'markdown headings must render');
  assert((await reply.locator('strong').count()) >= 2, 'bold must render as <strong>');
  assert((await reply.locator('strong').filter({ hasText: '总体情况' }).count()) >= 1, '"总体情况" must render as bold strong');
  assert((await reply.locator('em').count()) >= 1, 'italic must render');
  assert((await reply.locator('del, s, strike').count()) >= 1, 'strikethrough must render');
  assert((await reply.locator('ul li').count()) >= 5, 'at least 5 bullet items');
  assert((await reply.locator('ol li').count()) >= 3, 'numbered list items');
  assert((await reply.locator('table').count()) >= 1, 'GFM table must render');
  assert((await reply.locator('table thead, table tbody').count()) >= 1, 'table must have head/body');
  assert((await reply.locator('pre code').count()) >= 1, 'fenced code block must render');

  // ---- 颜色 ----
  {
    // renderer 将 inline color 归一为浏览器计算的 rgb，<font color> 属性被移除并迁移为 style.color；
    // 因此不再按 style 属性值匹配，改为按精确文本定位，并断言其计算色。
    // “需关注”同时出现在汇总段与表格中，故在按文本精确定位的候选里选计算色为目标的那个。
    const matches = await reply.getByText('需关注', { exact: true }).evaluateAll(els => els.map(el => ({
      text: el.textContent,
      color: getComputedStyle(el).color,
      styleAttr: el.getAttribute('style'),
    })));
    const target = matches.find(m => m.color === 'rgb(217, 139, 117)');
    assert(target, `span color:#d98b75 must exist with exact text "需关注": got ${JSON.stringify(matches)}`);
    assert.equal(target.text, '需关注', 'locate the #d98b75 span by its exact text');
    assert.equal(target.color, 'rgb(217, 139, 117)',
      `span color:#d98b75 must compute to rgb(217, 139, 117), got ${target.color} (style="${target.styleAttr}")`);
  }
  {
    const fontEl = reply.getByText('旧颜色', { exact: true }).first();
    await fontEl.waitFor();
    const fontInfo = await fontEl.evaluate(el => ({ text: el.textContent, color: getComputedStyle(el).color, styleAttr: el.getAttribute('style'), colorAttr: el.getAttribute('color') }));
    assert.equal(fontInfo.text, '旧颜色', '<font color="red"> text must be located by exact text');
    assert.equal(fontInfo.colorAttr, null, '<font color> attribute must be removed (migrated to style.color)');
    assert.equal(fontInfo.color, 'rgb(255, 0, 0)',
      `<font color="red"> must compute to red rgb(255, 0, 0), got ${fontInfo.color} (style="${fontInfo.styleAttr}")`);
  }
  {
    const rest = reply.getByText('样式受限样本', { exact: true }).first();
    await rest.waitFor();
    const style = await rest.evaluate(el => ({
      color: getComputedStyle(el).color,
      backgroundImage: getComputedStyle(el).backgroundImage,
      position: getComputedStyle(el).position,
    }));
    assert.equal(style.color, 'rgb(10, 20, 30)', 'restricted span must retain its color');
    assert.equal(style.backgroundImage, 'none', 'background:url must be stripped from span');
    assert.notEqual(style.position, 'fixed', 'position:fixed must be stripped from span');
  }

  // ---- 禁止标签 ----
  for (const tag of ['script', 'svg', 'iframe', 'style', 'form']) {
    assert.equal(await reply.locator(tag).count(), 0, `disallowed <${tag}> must not be retained in rich reply`);
  }

  // ---- 事件属性 ----
  {
    const hasHandler = await reply.locator('*').evaluateAll(els => els.some(el =>
      Array.from(el.attributes).some(a => a.name.toLowerCase().startsWith('on'))));
    assert.equal(hasHandler, false, 'no on* event handler attributes may survive');
  }

  // ---- 图片：raw 外链 img 被完全剥除（alt 不保留、无需保留）；其后合法正文必须保留 ----
  {
    assert.equal(await reply.locator('img').count(), 0, 'raw external img must be completely stripped (count 0)');

    // img 之后紧跟的合法正文必须保留渲染（不能因为 img 被移除而连带丢失）。
    assert(replyText.includes('图片已安全移除'), 'legitimate visible text following the stripped img must be retained');
    assert(replyText.includes('IMG_2026-1001.jpg'), 'inline-code attachment filename must be retained in rendered text');
    // 并确认没有任何外联 src 形式的 img 残留（即便 count===0 也保留防御断言）。
    const anyExternalImg = await reply.locator('img').evaluateAll(imgs => imgs.some(i => {
      const s = String(i.getAttribute('src') || '');
      return /^(https?:|data:|file:|javascript:)/i.test(s);
    }));
    assert.equal(anyExternalImg, false, 'img (if retained) must not carry a network/data/file/javascript src');
  }

  // ---- 链接 ----
  {
    // 外链安全
    const ext = reply.locator('a[href="https://example.com/reference"]');
    assert.equal(await ext.count(), 1, 'external https reference link must be retained');
    assert.equal(await ext.getAttribute('target'), '_blank', 'external link must open in _blank');
    const extRel = await ext.getAttribute('rel');
    assert(extRel && extRel.includes('noopener') && extRel.includes('noreferrer'), 'external link must carry rel noopener noreferrer');

    // 外部引用链接：编码空格（%20）与字面百分号（%25）必须被原样保留为外链（renderer 的 decoder 已修复）
    const encodedExt = reply.locator('a[href="https://example.com/reference?q=maintenance%20plan&ratio=50%25"]');
    assert.equal(await encodedExt.count(), 1, 'external citation with encoded %20 and literal %25 must be preserved exactly');
    assert.equal(await encodedExt.getAttribute('target'), '_blank', 'encoded external citation must open in _blank');
    const encodedRel = await encodedExt.getAttribute('rel');
    assert(encodedRel && encodedRel.includes('noopener') && encodedRel.includes('noreferrer'),
      'encoded external citation must carry rel noopener noreferrer');

    // 本地保留（保持不变为相对 href，不得被改写为绝对 http://…）
    assert.equal(await reply.locator('a[href="/repair-management?scope=A"]').count(), 1,
      'local /repair-management?scope=A must be retained as a relative href');
    {
      const localHrefs = await reply.locator('a').evaluateAll(as => as
        .filter(a => (a.getAttribute('href') || '').includes('repair-management'))
        .map(a => a.getAttribute('href')));
      assert(localHrefs.every(h => h && h.startsWith('/') && !/^https?:\/\//i.test(h)),
        `local /repair-management link must stay relative, got ${JSON.stringify(localHrefs)}`);
    }

    // 内部 API：/api/backend/shutdown 与 /%61pi/backend/shutdown 不得有 href
    const dangerousHrefs = await reply.locator('a').evaluateAll(as => as.filter(a => {
      const h = a.getAttribute('href') || '';
      return /backend\/shutdown/i.test(h) || /%61pi/i.test(h);
    }).map(a => a.getAttribute('href')));
    assert.equal(dangerousHrefs.length, 0, 'internal api shutdown links must have no href');
    const shutdownAnchors = reply.locator('a').filter({ hasText: /backend\/shutdown/ });
    const shutdownHrefs = await shutdownAnchors.evaluateAll(as => as.map(a => a.getAttribute('href')));
    assert(shutdownHrefs.every(h => !h), 'shutdown anchor text must not carry a clickable href');

    // 危险 scheme 链接无 href
    for (const scheme of ['javascript:', 'data:', 'file:']) {
      const bad = await reply.locator('a').evaluateAll((as, s) => as.some(a =>
        (a.getAttribute('href') || '').toLowerCase().startsWith(s)), scheme);
      assert.equal(bad, false, `no ${scheme} href may survive`);
    }

    // 协议相对（//evil.example）、编码反斜杠（/%5cevil.example）与编码 javascript 链接必须被剥除 href
    for (const text of ['协议相对链接', '编码反斜杠链接', '编码 javascript 链接']) {
      const anchor = reply.locator('a').filter({ hasText: text }).first();
      assert.equal(await anchor.count(), 1, `unsafe link "${text}" must still render as anchor text`);
      const href = await anchor.getAttribute('href');
      assert.equal(href, null, `unsafe link "${text}" must have its href stripped`);
    }
  }

  // ---- 不执行 / 不产生外联 ----
  {
    const xss = await page.evaluate(() => window.__rich_xss__);
    assert.equal(xss, 0, 'rich reply must not execute injected scripts/event handlers');
  }
  // 守卫前提：http://localhost（默认端口 80）是“本地但不同源”的 origin，精确同源守卫不得豁免它；
  // 该断言仅做前提校验，不发起任何网络请求。
  {
    const baseOrigin = new URL(base).origin;
    const localNonMatchingOrigin = new URL('http://localhost/').origin;
    assert.notEqual(localNonMatchingOrigin, baseOrigin,
      `guard premise: http://localhost origin (${localNonMatchingOrigin}) must differ from base origin (${baseOrigin})`);
  }
  assert.equal(externalRequests.length, 0, `rich reply must not trigger outbound requests: ${externalRequests.join(', ')}`);

  // ---- 桌面无溢出；宽表格/代码块内部滚动；composer 可见 ----
  {
    const panelOverflow = await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1);
    assert(panelOverflow, 'desktop assistant panel must not overflow horizontally');
    const docOverflow = await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1);
    assert(docOverflow, 'desktop document must not overflow horizontally');
    assert(await page.locator('.composer .send').isVisible(), 'desktop composer send must be visible');
    await assertContained(reply, page);
    await page.evaluate(() => { const t = document.querySelector('.thread'); if (t) t.scrollTop = 0; });
    await page.screenshot({ path: path.join(output, 'rich-reply-desktop.png') });
  }

  // ---- reload 后经拦截 GET conversation 保留富文本回复 ----
  {
    if (await page.getByRole('button', { name: '收起助手', exact: true }).count()) {
      await page.getByRole('button', { name: '收起助手', exact: true }).click();
    }
    await page.reload();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).waitFor();
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
    await page.locator('.turn').last().locator('.assistant-rich-reply').waitFor({ timeout: 15000 });
    const reloadedReply = page.locator('.turn').last().locator('.assistant-rich-reply');
    assert.match(await reloadedReply.innerText(), /总体情况/, 'reloaded rich reply must retain the rendered answer');
    await page.evaluate(() => { const t = document.querySelector('.thread'); if (t) t.scrollTop = 0; });
    await page.screenshot({ path: path.join(output, 'rich-reply-reload.png') });
  }

  // ---- mobile(390x844)：无横向溢出、宽表格/代码内部滚动、composer 可见 ----
  {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForFunction(() => {
      const panel = document.querySelector('.assistant-panel');
      if (!panel) return false;
      const r = panel.getBoundingClientRect();
      return r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight;
    }, undefined, { timeout: 5000 });
    const mobilePanel = await page.locator('.assistant-panel').boundingBox();
    assert(mobilePanel && mobilePanel.x >= 0 && mobilePanel.x + mobilePanel.width <= 390,
      `mobile assistant panel must fit viewport (${JSON.stringify(mobilePanel)})`);
    const panelOverflow = await page.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1);
    assert(panelOverflow, 'mobile assistant panel must not overflow horizontally');
    const docOverflow = await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1);
    assert(docOverflow, 'mobile document must not overflow horizontally');
    const mobileReply = page.locator('.turn').last().locator('.assistant-rich-reply');
    await mobileReply.waitFor();
    await assertContained(mobileReply, page);
    assert(await page.locator('.composer .send').isVisible(), 'mobile composer send must remain visible alongside the rich reply');
    await page.evaluate(() => { const t = document.querySelector('.thread'); if (t) t.scrollTop = 0; });
    await page.screenshot({ path: path.join(output, 'rich-reply-mobile.png') });
  }

  assert.equal(dialogs.length, 0, `no dialogs expected: ${dialogs.join('\n')}`);
  assert.equal(pageErrors.length, 0, pageErrors.join('\n'));
  console.log('Assistant rich-reply checks passed: .assistant-rich-reply replaces .answer p; markdown headings/bold/italic/strikethrough/5-bullets/numbered-list/GFM-table(6-col)/long-code-fence; #d98b75 and font red computed colors (selected by exact text); font color attribute removed; span color-only retention (background url + position:fixed stripped); no script/svg/iframe/style/form/event-handlers; no javascript/data/file/internal-api hrefs; external https link safe target_blank+noopener noreferrer; local /repair-management?scope=A stays relative; external img fully removed (count 0) with following legitimate text retained; zero dialogs/pageerrors/outbound requests (origin-exact guard); reload persistence via GET conversation; desktop(1440) & mobile(390) no page/panel/.thread h-overflow with internally-scrolling wide table/pre (right edges contained; mobile pre scrollWidth>clientWidth scrollLeft internal) and visible composer; screenshots after scrolling .thread to top.');
} catch (error) {
  await page.screenshot({ path: path.join(output, 'rich-reply-failure.png') }).catch(() => {});
  const text = await page.locator('.lighthouse').innerText().catch(() => 'assistant unavailable');
  console.error(text);
  throw error;
} finally {
  await browser.close();
}

// 宽表格/代码块允许在面板内部滚动，但绝不能撑开 page/panel/.thread 造成横向溢出；
// 这里验证：宽内容被 panel/.thread 约束（左右边界均收在容器内），thread 无水平溢出，
// 且移动端长代码块具备内部横向滚动（scrollWidth>clientWidth、scrollLeft 可调且不带动父容器/composer）。
async function assertContained(reply, pageLike) {
  const panelNoOverflow = await pageLike.locator('.assistant-panel').evaluate(e => e.scrollWidth <= e.clientWidth + 1);
  assert(panelNoOverflow, 'wide table/pre must scroll internally without horizontally overflowing the panel');
  const docNoOverflow = await pageLike.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1);
  assert(docNoOverflow, 'wide table/pre must not widen the document horizontally');

  // 主滚动容器 .thread 自身不得横向溢出。
  const threadMetrics = await pageLike.locator('.thread').evaluate(t => ({ scrollWidth: t.scrollWidth, clientWidth: t.clientWidth }));
  assert(threadMetrics.scrollWidth <= threadMetrics.clientWidth + 1,
    `thread must not overflow horizontally (scrollWidth=${threadMetrics.scrollWidth}, clientWidth=${threadMetrics.clientWidth})`);

  const panelBox = await pageLike.locator('.assistant-panel').boundingBox();
  assert(panelBox, 'assistant panel bounding box required');
  const threadBox = (await pageLike.locator('.thread').boundingBox()) || panelBox;

  // 宽内容确已渲染（表格/代码块存在）：左右边界都必须收在 panel/.thread 之内（内部滚动而非撑开布局）。
  for (const sel of ['table', 'pre']) {
    const count = await reply.locator(sel).count();
    assert(count >= 1, `wide ${sel} must be rendered for the internal-scroll check`);
    const box = await reply.locator(sel).first().boundingBox();
    assert(box, `${sel} bounding box required`);
    const leftLimit = Math.min(panelBox.x, threadBox.x);
    const rightLimit = Math.min(panelBox.x + panelBox.width, threadBox.x + threadBox.width);
    assert(box.x >= leftLimit - 1, `${sel} left edge must stay within thread/panel (internal scroll allowed, no layout overflow)`);
    assert(box.x + box.width <= rightLimit + 1,
      `${sel} right edge must stay within thread/panel (right=${(box.x + box.width).toFixed(1)}, limit=${rightLimit.toFixed(1)})`);
  }

  // 移动端长代码块：自身应有内部横向滚动能力，scrollLeft 可调且不带动父容器/composer 位置。
  const viewport = pageLike.viewportSize();
  if (viewport && viewport.width < 600) {
    const ok = await mobilePreInternalScroll(pageLike);
    assert(ok, 'mobile long pre must have scrollWidth>clientWidth and scroll via scrollLeft without moving parent/composer');
  }
}

// 检查移动端长 pre：找到具备内部横向溢出的可滚动元素，确认 scrollWidth>clientWidth、
// 且修改其 scrollLeft 不改变父容器与 composer 的位置（内部滚动不带动外层）。
async function mobilePreInternalScroll(pageLike) {
  return pageLike.evaluate(() => {
    const pre = document.querySelector('.assistant-rich-reply pre');
    if (!pre) return false;
    const scrollEl = (() => {
      let cur = pre;
      while (cur && cur !== document.body) {
        if (cur.scrollWidth > cur.clientWidth) return cur;
        cur = cur.parentElement;
      }
      return pre;
    })();
    const hasOverflow = scrollEl.scrollWidth > scrollEl.clientWidth;
    if (!hasOverflow) return false;

    const pos = el => el ? (() => { const r = el.getBoundingClientRect(); return r.left + ':' + r.top; })() : null;
    const parentBefore = pos(scrollEl.parentElement);
    const composerBefore = pos(document.querySelector('.composer'));
    const before = scrollEl.scrollLeft;
    scrollEl.scrollLeft += 40;
    const moved = scrollEl.scrollLeft !== before;
    const parentAfter = pos(scrollEl.parentElement);
    const composerAfter = pos(document.querySelector('.composer'));
    scrollEl.scrollLeft = before; // 回滚，避免影响后续截图/断言
    return moved && parentBefore === parentAfter && composerBefore === composerAfter;
  });
}
