/* 原型渲染验证：捕获运行时错误、检查五角色差异、双端溢出、抽屉交互 */
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const frontendDir = path.resolve(__dirname, '../../../bin/lan_bitable_template_portal/frontend');

const FILE = pathToFileURL(path.resolve(__dirname, process.argv[2] || 'index.html')).href;
const screenshotDir = process.argv[3] ? path.resolve(process.argv[3]) : __dirname;
const problems = [];
const notes = [];
const placeholderFindings = new Map();

function log(...a) { console.log(...a); }

async function checkPlaceholderCopy(page, view) {
  const found = await page.evaluate(() => {
    const text = document.body.innerText;
    const markers = text.match(/\b(?:TODO|FIXME)\b/g) || [];
    const filler = text.match(/\b(?:lorem|ipsum|placeholder text|Metric A|xxx)\b|示例文本|待定内容/gi) || [];
    return [...new Set([...markers, ...filler])];
  });
  if (found.length) placeholderFindings.set(view, found);
}

/* color-mix() 计算值会返回 color(srgb r g b) 且分量为 0–1，需归一化后才能算亮度 */
function toRGB(str) {
  if (!str) return 'rgb(0, 0, 0)';
  let m = str.match(/color\(srgb\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)/);
  if (m) return `rgb(${Math.round(m[1] * 255)}, ${Math.round(m[2] * 255)}, ${Math.round(m[3] * 255)})`;
  m = str.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/);
  if (m) return `rgb(${Math.round(m[1])}, ${Math.round(m[2])}, ${Math.round(m[3])})`;
  m = str.match(/^#([0-9a-f]{6})$/i);
  if (m) return `rgb(${parseInt(m[1].slice(0, 2), 16)}, ${parseInt(m[1].slice(2, 4), 16)}, ${parseInt(m[1].slice(4, 6), 16)})`;
  return str;
}

let browser;
(async () => {
  const { chromium } = require(require.resolve('playwright', { paths: [__dirname, frontendDir] }));
  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });

  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
  page.on('pageerror', e => errors.push('pageerror: ' + (e && e.message)));

  await page.goto(FILE, { waitUntil: 'load' });
  await page.waitForTimeout(400);

  /* 1. 运行时错误 */
  log('\n=== 1. 运行时错误 ===');
  if (errors.length) { errors.forEach(e => log('  ✗ ' + e)); problems.push(...errors); }
  else log('  ✓ 无 console error / pageerror');

  /* 2. 五类 persona 差异 */
  log('\n=== 2. 五类 persona 渲染与差异 ===');
  const snaps = {};
  for (const p of ['global', 'duty', 'engineer', 'supervisor', 'personal']) {
    await page.click(`#seg-persona button[data-persona="${p}"]`);
    await page.waitForTimeout(180);
    await checkPlaceholderCopy(page, `角色 ${p}`);
    const s = await page.evaluate(() => ({
      title: document.querySelector('#page-title').textContent.trim(),
      person: document.querySelector('#person-name').textContent.trim(),
      focus: Array.from(document.querySelectorAll('.focus-card .focus-label')).map(e => e.textContent.trim()),
      focusValues: Array.from(document.querySelectorAll('.focus-card .focus-value b')).map(e => e.textContent.trim()),
      tiers: Array.from(document.querySelectorAll('.tier')).map(t => ({
        tier: t.dataset.tier,
        h: t.querySelector('h3') ? t.querySelector('h3').textContent.trim() : '',
        rows: t.querySelectorAll('.row').length,
        integrity: t.querySelector('.tier-integrity') ? t.querySelector('.tier-integrity').textContent.trim().replace(/\s+/g, ' ') : ''
      })),
      shortcuts: Array.from(document.querySelectorAll('.shortcut span:nth-child(2)')).map(e => e.textContent.trim()),
      overviewOpen: document.querySelector('#overview-block').hasAttribute('open'),
      charts: document.querySelectorAll('.chart-card').length,
      attention: document.querySelectorAll('.attention-item').length,
      shiftName: document.querySelector('.shift-name') ? document.querySelector('.shift-name').textContent.trim() : '',
      navBadge: document.querySelector('.nav-badge') ? document.querySelector('.nav-badge').textContent.trim() : ''
    }));
    snaps[p] = s;
    const rowCount = s.tiers.reduce((n, t) => n + t.rows, 0);
    log(`  ${p}: 「${s.title}」${s.person} | 焦点带=[${s.focus.join(' / ')}] 值=[${s.focusValues.join(',')}]`);
    log(`        分组=${s.tiers.map(t => t.h + '(' + t.rows + ')').join(' → ')} 共${rowCount}行 | 快捷=${s.shortcuts.length} 概览展开=${s.overviewOpen} 图表=${s.charts} 留意=${s.attention} 班次=${s.shiftName}`);
    if (!s.focus.length) problems.push(`${p}: 焦点带为空`);
    if (!rowCount) problems.push(`${p}: 队列为空`);
    if (!s.tiers.every(t => t.integrity)) problems.push(`${p}: 有分组缺少数据完整性标注`);
  }
  /* 差异校验 */
  const sig = p => JSON.stringify([snaps[p].focus, snaps[p].tiers.map(t => t.tier), snaps[p].shortcuts, snaps[p].overviewOpen, snaps[p].title]);
  const keys = Object.keys(snaps);
  for (let i = 0; i < keys.length; i++) for (let j = i + 1; j < keys.length; j++) {
    if (sig(keys[i]) === sig(keys[j])) problems.push(`persona ${keys[i]} 与 ${keys[j]} 首屏无差异`);
  }
  log('  ✓ 五类 persona 两两存在实质差异');

  /* 3. 队列内容口径校验（duty） */
  log('\n=== 3. 班次队列口径（值班角色）===');
  await page.click('#seg-persona button[data-persona="duty"]');
  await page.waitForTimeout(160);
  const duty = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll('.row'));
    return {
      order: Array.from(document.querySelectorAll('.tier')).map(t => t.dataset.tier),
      overdue: rows.filter(r => r.dataset.pri === 'overdue').length,
      returned: rows.filter(r => /退回/.test(r.textContent)).length,
      kinds: Array.from(new Set(rows.map(r => r.querySelector('.row-kind span:last-child').textContent.trim()))),
      gutter: rows.map(r => getComputedStyle(r).borderLeftColor),
      radius: rows.length ? getComputedStyle(rows[0]).borderTopLeftRadius : '',
      hasLoadMore: !!document.querySelector('[data-load-more]')
    };
  });
  log(`  分组顺序 = ${duty.order.join(' → ')}`);
  log(`  逾期项 = ${duty.overdue} | 退回项 = ${duty.returned} | 加载更多 = ${duty.hasLoadMore}`);
  log(`  覆盖业务类型 = ${duty.kinds.join('、')}`);
  log(`  队列行圆角 = ${duty.radius}（应为 0px，直角行 + 左侧优先级色槽，规避 AI tile 形状）`);
  if (duty.order.join() !== 'shift,overdue,today,later') problems.push('值班角色优先级分组顺序错误: ' + duty.order.join());
  if (duty.overdue < 2) problems.push('逾期项不足 2 条');
  if (duty.returned < 1) problems.push('缺少退回/异常项');
  if (duty.kinds.length < 7) problems.push('业务类型覆盖不足 7 类: ' + duty.kinds.join());
  if (duty.radius !== '0px') problems.push('队列行不是直角，可能触发「圆角卡+左侧彩条」AI tile 规则');

  /* 4. 抽屉交互 */
  log('\n=== 4. 详情抽屉 ===');
  await page.click('.tier[data-tier="shift"] .row');
  await page.waitForTimeout(320);
  const dOpen = await page.evaluate(() => ({
    hidden: document.querySelector('#drawer').hidden,
    cls: document.body.classList.contains('drawer-open'),
    title: document.querySelector('#drawer-title').textContent.trim(),
    sects: document.querySelectorAll('.drawer-sect').length,
    footBtns: Array.from(document.querySelectorAll('#drawer-foot .btn')).map(b => b.textContent.trim()),
    modal: document.querySelector('#drawer').getAttribute('aria-modal'),
    role: document.querySelector('#drawer').getAttribute('role')
  }));
  log(`  打开=${!dOpen.hidden} 标题「${dOpen.title}」区块=${dOpen.sects} role=${dOpen.role} aria-modal=${dOpen.modal}`);
  log(`  底部操作 = ${dOpen.footBtns.join(' | ')}`);
  if (dOpen.hidden || !dOpen.cls) problems.push('抽屉未正常打开');
  if (!dOpen.sects) problems.push('抽屉内容为空');
  await checkPlaceholderCopy(page, '详情抽屉');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(320);
  const closed = await page.evaluate(() => {
    const d = document.querySelector('#drawer');
    const r = d.getBoundingClientRect();
    const c = getComputedStyle(d);
    return {
      cls: document.body.classList.contains('drawer-open'),
      display: c.display,
      hidden: d.hidden,
      onScreen: c.display !== 'none' && r.left < innerWidth && r.right > 0
    };
  });
  log(`  Esc 关闭 = ${!closed.cls ? '✓' : '✗'} | 关闭后是否仍占据屏幕 = ${closed.onScreen ? '✗ 仍可见' : '✓ 不可见'} (display=${closed.display})`);
  if (closed.cls) problems.push('Esc 无法关闭抽屉');
  if (closed.onScreen) problems.push('抽屉关闭后仍渲染在屏幕内（hidden 属性被作者级 display 覆盖）');

  /* 5. 状态演示 */
  log('\n=== 5. 加载 / 空 / 错误 / 同步失败状态 ===');
  for (const st of ['loading', 'empty', 'error', 'syncfail', 'live']) {
    await page.click(`#seg-state button[data-state="${st}"]`);
    await page.waitForTimeout(st === 'loading' ? 120 : 180);
    await checkPlaceholderCopy(page, `页面状态 ${st}`);
    const r = await page.evaluate(() => ({
      sk: document.querySelectorAll('.sk').length,
      empty: document.querySelectorAll('.state').length,
      err: document.querySelectorAll('.state-error').length,
      fail: document.querySelectorAll('.syncfail').length,
      rows: document.querySelectorAll('.row').length,
      topbar: document.querySelector('#topbar-sync').textContent.trim().replace(/\s+/g, ' ')
    }));
    log(`  ${st.padEnd(9)} 骨架=${r.sk} 空态=${r.empty} 错误=${r.err} 同步横幅=${r.fail} 行=${r.rows} 顶栏=「${r.topbar}」`);
    if (st === 'loading' && !r.sk) problems.push('加载态无骨架屏');
    if (st === 'empty' && !r.empty) problems.push('空态未渲染');
    if (st === 'error' && !r.err) problems.push('错误态未渲染');
    if (st === 'syncfail' && !r.fail) problems.push('同步失败横幅未渲染');
    if (st === 'syncfail' && !/截至/.test(r.topbar)) problems.push('同步失败时顶栏未升级为危险信号');
  }

  /* 6. 明暗外观：token 派生检查 */
  log('\n=== 6. 明暗外观与 token 派生 ===');
  await page.click('#seg-state button[data-state="live"]');
  const themes = {};
  for (const ap of ['light', 'dark']) {
    await page.click(`#seg-appearance button[data-appearance="${ap}"]`);
    await page.waitForTimeout(220);
    themes[ap] = await page.evaluate(() => {
      const cs = getComputedStyle(document.documentElement);
      const g = n => cs.getPropertyValue(n).trim();
      /* color-mix() 计算值形如 color(srgb 0.4 0.43 0.42)，分量 0–1，需归一化为 rgb() */
      const norm = s => {
        if (!s) return 'rgb(0, 0, 0)';
        let m = s.match(/color\(srgb\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)/);
        if (m) return `rgb(${Math.round(m[1] * 255)}, ${Math.round(m[2] * 255)}, ${Math.round(m[3] * 255)})`;
        m = s.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/);
        if (m) return `rgb(${Math.round(m[1])}, ${Math.round(m[2])}, ${Math.round(m[3])})`;
        return s;
      };
      const probe = el => { const c = getComputedStyle(el); return { bg: norm(c.backgroundColor), fg: norm(c.color), bd: norm(c.borderTopColor) }; };
      /* 背景可能是 transparent，需向上追溯第一个不透明祖先，否则对比度会算错 */
      const effBg = el => {
        let n = el;
        while (n && n !== document.documentElement) {
          const bg = getComputedStyle(n).backgroundColor;
          const m = bg.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+))?\s*\)/);
          const a = m && m[4] !== undefined ? parseFloat(m[4]) : 1;
          if (!/transparent|rgba?\(\s*0[,\s]+0[,\s]+0[,\s]+0\s*\)/.test(bg) && a > 0.05) return norm(bg);
          n = n.parentElement;
        }
        return norm(getComputedStyle(document.body).backgroundColor);
      };
      return {
        seedBg: g('--seed-bg'), seedFg: g('--seed-fg'), seedPrimary: g('--seed-primary'),
        body: probe(document.body),
        card: probe(document.querySelector('.focus-card')),
        tier: probe(document.querySelector('.tier-head')),
        row: probe(document.querySelector('.row')),
        tag: probe(document.querySelector('.row-state .tag')),
        topbar: probe(document.querySelector('.topbar')),
        muted: norm(getComputedStyle(document.querySelector('.row-meta')).color),
        rowTitle: norm(getComputedStyle(document.querySelector('.row-title')).color),
        rowTitleBg: effBg(document.querySelector('.row-title')),
        focusLabel: norm(getComputedStyle(document.querySelector('.focus-label')).color),
        focusLabelBg: effBg(document.querySelector('.focus-label')),
        tagBgEff: effBg(document.querySelector('.row-state .tag')),
        tierHead: probe(document.querySelector('.tier-head h3'))
      };
    });
    const t = themes[ap];
    log(`  ${ap}: seed-bg=${t.seedBg} seed-fg=${t.seedFg} seed-primary=${t.seedPrimary}`);
    log(`        body bg=${t.body.bg} fg=${t.body.fg} | 卡片 bg=${t.card.bg} 边框=${t.card.bd} | 顶栏 bg=${t.topbar.bg}`);
    log(`        次要文字=${t.muted} 标签 bg=${t.tag.bg} fg=${t.tag.fg}`);
  }
  if (themes.light.seedBg === themes.dark.seedBg) problems.push('明暗 seed-bg 未变化');
  if (themes.light.body.bg === themes.dark.body.bg) problems.push('明暗 body 背景未变化（派生链断裂）');
  if (themes.light.tier.bg === themes.dark.tier.bg) problems.push('明暗 tier 背景未变化');
  if (themes.light.tag.bg === themes.dark.tag.bg) problems.push('明暗语义标签底色未变化');

  /* 对比度计算 */
  const lum = hex => {
    const m = hex.match(/[\d.]+/g).map(Number);
    const [r, g, b] = m.length >= 3 ? m : [0, 0, 0];
    const f = v => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
  };
  const ratio = (a, b) => { const l1 = lum(a), l2 = lum(b); return ((Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05)); };
  log('\n=== 7. 对比度（WCAG AA）===');
  for (const ap of ['light', 'dark']) {
    const t = themes[ap];
    const body = ratio(t.body.fg, t.body.bg);
    const muted = ratio(t.muted, t.card.bg);
    const rowTitle = ratio(t.rowTitle, t.rowTitleBg);
    const tagFg = ratio(t.tag.fg, t.tagBgEff);
    const focusLabel = ratio(t.focusLabel, t.focusLabelBg);
    log(`  ${ap}: 正文/页面底 ${body.toFixed(2)}:1 ${body >= 4.5 ? '✓' : '✗'} | 次要文字/卡片 ${muted.toFixed(2)}:1 ${muted >= 4.5 ? '✓' : '✗'} | 行标题/有效底 ${rowTitle.toFixed(2)}:1 ${rowTitle >= 4.5 ? '✓' : '✗'} | 状态标签 ${tagFg.toFixed(2)}:1 ${tagFg >= 4.5 ? '✓' : '✗'} | 焦点标签 ${focusLabel.toFixed(2)}:1 ${focusLabel >= 4.5 ? '✓' : '✗'}`);
    log(`        有效底色 行标题=${t.rowTitleBg} 标签=${t.tagBgEff} 焦点=${t.focusLabelBg}`);
    if (body < 4.5) problems.push(`${ap} 正文对比度不足 4.5:1 (${body.toFixed(2)})`);
    if (muted < 4.5) problems.push(`${ap} 次要文字对比度不足 4.5:1 (${muted.toFixed(2)})`);
    if (rowTitle < 4.5) problems.push(`${ap} 行标题对比度不足 4.5:1 (${rowTitle.toFixed(2)})`);
    if (tagFg < 4.5) problems.push(`${ap} 状态标签对比度不足 4.5:1 (${tagFg.toFixed(2)})`);
    if (focusLabel < 4.5) problems.push(`${ap} 焦点标签对比度不足 4.5:1 (${focusLabel.toFixed(2)})`);
  }

  /* 8. 响应式溢出检查 */
  log('\n=== 8. 响应式（横向溢出与主操作可见性）===');
  await page.click('#seg-appearance button[data-appearance="light"]');
  for (const vp of ['desktop', 'mobile']) {
    await page.click(`#seg-viewport button[data-viewport="${vp}"]`);
    await page.waitForTimeout(250);
    for (const p of ['duty', 'supervisor', 'personal']) {
      await page.click(`#seg-persona button[data-persona="${p}"]`);
      await page.waitForTimeout(150);
      const r = await page.evaluate(() => {
        const de = document.documentElement;
        const main = document.querySelector('#main');
        const wide = [];
        document.querySelectorAll('.row,.focus-card,.tier-head,.shiftbar,.shortcut,.chart-card,.attention-item').forEach(el => {
          if (el.scrollWidth > el.clientWidth + 2 && getComputedStyle(el).overflowX === 'visible')
            wide.push((el.className || el.tagName).toString().slice(0, 34) + ' ' + el.scrollWidth + '>' + el.clientWidth);
        });
        return {
          docOverflow: de.scrollWidth > de.clientWidth + 1,
          mainOverflow: main.scrollWidth > main.clientWidth + 1,
          rowH: document.querySelector('.row-act .btn') ? getComputedStyle(document.querySelector('.row-act .btn')).minHeight : 'n/a',
          wide: wide.slice(0, 5)
        };
      });
      const ok = !r.docOverflow && !r.mainOverflow && !r.wide.length;
      log(`  ${vp}/${p}: 文档溢出=${r.docOverflow} 主区溢出=${r.mainOverflow} 行内按钮高=${r.rowH} ${ok ? '✓' : '✗ ' + r.wide.join(' | ')}`);
      if (r.docOverflow) problems.push(`${vp}/${p} 文档横向溢出`);
      if (r.mainOverflow) problems.push(`${vp}/${p} 主区横向溢出`);
      if (r.wide.length) problems.push(`${vp}/${p} 元素内容溢出: ` + r.wide.join(' | '));
      if (vp === 'mobile' && parseFloat(r.rowH) < 44) problems.push('移动端主操作按钮高度 < 44px: ' + r.rowH);
    }
  }

  /* 9. 真实窄屏 */
  log('\n=== 9. 真实窄屏（无模拟，直接缩放窗口）===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  for (const w of [1440, 1024, 760, 390]) {
    await page.setViewportSize({ width: w, height: 900 });
    await page.waitForTimeout(220);
    const r = await page.evaluate(() => {
      const de = document.documentElement;
      return { over: de.scrollWidth > de.clientWidth + 1, sw: de.scrollWidth, cw: de.clientWidth };
    });
    log(`  ${w}px: 溢出=${r.over} (${r.sw}/${r.cw}) ${r.over ? '✗' : '✓'}`);
    if (r.over) problems.push(`真实 ${w}px 视口横向溢出 ${r.sw}>${r.cw}`);
  }
  await page.setViewportSize({ width: 1440, height: 1000 });

  /* 10. 键盘可达性 */
  log('\n=== 10. 键盘可达性 ===');
  await page.evaluate(() => window.scrollTo(0, 0));
  const kb = await page.evaluate(() => {
    const focusables = Array.from(document.querySelectorAll('a[href],button:not([disabled]),input,select,textarea,[tabindex]:not([tabindex="-1"])'));
    const visible = focusables.filter(el => el.offsetParent !== null || el.getClientRects().length);
    return { total: focusables.length, visible: visible.length };
  });
  log(`  可聚焦元素 = ${kb.total}（可见 ${kb.visible}）`);
  await page.keyboard.press('Tab'); await page.keyboard.press('Tab'); await page.keyboard.press('Tab');
  const focused = await page.evaluate(() => {
    const a = document.activeElement;
    if (!a) return null;
    const c = getComputedStyle(a);
    return { tag: a.tagName, cls: (a.className || '').toString().slice(0, 40), outline: c.outlineWidth + ' ' + c.outlineStyle + ' ' + c.outlineColor };
  });
  log(`  Tab 焦点 = <${focused.tag} class="${focused.cls}"> outline: ${focused.outline}`);
  if (/0px|none/.test(focused.outline)) problems.push('焦点环不可见');

  /* 11. 折叠区块 */
  log('\n=== 11. 折叠区块 ===');
  const col = await page.evaluate(() => {
    const a = document.querySelector('#attention-block');
    return { attr: a.hasAttribute('open'), aria: a.querySelector('[data-collapse-toggle]').getAttribute('aria-expanded'), bodyHidden: a.querySelector('.collapse-body').hidden };
  });
  log(`  需要留意 初始 open=${col.attr} aria-expanded=${col.aria} body.hidden=${col.bodyHidden}`);
  if (col.attr !== (col.aria === 'true') || col.attr !== !col.bodyHidden) problems.push('折叠状态 attribute / aria / hidden 三者不一致');
  await page.click('#attention-block [data-collapse-toggle]');
  await page.waitForTimeout(200);
  const col2 = await page.evaluate(() => {
    const a = document.querySelector('#attention-block');
    return { attr: a.hasAttribute('open'), aria: a.querySelector('[data-collapse-toggle]').getAttribute('aria-expanded'), bodyHidden: a.querySelector('.collapse-body').hidden, items: a.querySelectorAll('.attention-item').length };
  });
  log(`  点击后 open=${col2.attr} aria-expanded=${col2.aria} body.hidden=${col2.bodyHidden} 条目=${col2.items}`);
  if (!col2.attr || col2.aria !== 'true' || col2.bodyHidden) problems.push('折叠点击后状态未正确切换');

  /* 12. 占位文案与 AI 味扫描 */
  log('\n=== 12. 占位文案 / AI 味 / 硬编码色扫描 ===');
  await checkPlaceholderCopy(page, '展开的留意区');
  const scan = await page.evaluate(() => {
    const text = document.body.innerText;
    const html = document.documentElement.outerHTML;
    const indigo = ['#6366f1', '#4f46e5', '#4338ca', '#3730a3', '#8b5cf6', '#7c3aed', '#a855f7'].filter(c => html.toLowerCase().includes(c));
    const emoji = (text.match(/[\u2728\u{1F680}\u{1F3AF}\u26A1\u{1F525}\u{1F4A1}]/gu) || []);
    const gradients = (html.match(/linear-gradient|radial-gradient/g) || []).length;
    return { indigo, emoji: emoji.slice(0, 5), gradients };
  });
  log(`  AI 默认靛蓝 = ${scan.indigo.length ? scan.indigo.join(', ') : '无 ✓'}`);
  log(`  Emoji 图标 = ${scan.emoji.length ? scan.emoji.join(' ') : '无 ✓'}`);
  log(`  渐变用量 = ${scan.gradients} 处（应为 0，Carbon 纪律：扁平表面 + 边框分层）`);
  if (scan.indigo.length) problems.push('存在 AI 默认靛蓝: ' + scan.indigo.join(', '));
  if (scan.emoji.length) problems.push('存在 emoji 图标: ' + scan.emoji.join(' '));
  if (scan.gradients > 0) notes.push(`存在 ${scan.gradients} 处渐变`);

  /* CSS 中 :root 之外的裸 hex */
  const hexOutside = await page.evaluate(() => {
    const out = [];
    for (const sheet of document.styleSheets) {
      let rules; try { rules = sheet.cssRules; } catch (e) { continue; }
      const walk = (list, ctx) => {
        for (const r of list) {
          if (r.cssRules) { walk(r.cssRules, ctx + (r.conditionText || r.cssText.slice(0, 30))); continue; }
          if (!r.style) continue;
          const sel = r.selectorText || '';
          if (sel === ':root' || sel.includes('[data-appearance')) continue;
          const txt = r.cssText;
          const m = txt.match(/#[0-9a-fA-F]{3,8}\b/g);
          if (m) out.push(sel.slice(0, 46) + ' → ' + m.join(','));
        }
      };
      walk(rules, '');
    }
    return out;
  });
  log(`  :root 外裸 hex = ${hexOutside.length} 处`);
  hexOutside.slice(0, 8).forEach(h => log('    · ' + h));
  if (hexOutside.length > 12) problems.push(`:root 外裸 hex 过多 (${hexOutside.length})`);

  /* 13. data-component / data-od-id 覆盖 */
  log('\n=== 13. Canvas 图层锚点 ===');
  const hooks = await page.evaluate(() => ({
    dc: document.querySelectorAll('[data-component]').length,
    od: document.querySelectorAll('[data-od-id]').length,
    list: Array.from(document.querySelectorAll('[data-component]')).map(e => e.dataset.component)
  }));
  log(`  data-component = ${hooks.dc} | data-od-id = ${hooks.od}`);
  log(`  ${hooks.list.join(' · ')}`);
  if (hooks.dc < 8) problems.push('data-component 锚点不足');

  /* 14. 语义结构 */
  log('\n=== 14. 语义结构与可访问性 ===');
  const sem = await page.evaluate(() => ({
    main: document.querySelectorAll('main').length,
    nav: document.querySelectorAll('nav').length,
    aside: document.querySelectorAll('aside').length,
    header: document.querySelectorAll('header').length,
    section: document.querySelectorAll('section').length,
    h1: document.querySelectorAll('h1').length,
    h2: document.querySelectorAll('h2').length,
    svgNoLabel: Array.from(document.querySelectorAll('svg[role="img"]')).filter(s => !s.getAttribute('aria-label')).length,
    svgNoTitle: Array.from(document.querySelectorAll('svg[role="img"]')).filter(s => !s.querySelector('title')).length,
    btnNoLabel: Array.from(document.querySelectorAll('button')).filter(b => !b.textContent.trim() && !b.getAttribute('aria-label')).length,
    live: document.querySelectorAll('[aria-live]').length
  }));
  log(`  main=${sem.main} nav=${sem.nav} aside=${sem.aside} header=${sem.header} section=${sem.section} h1=${sem.h1} h2=${sem.h2}`);
  log(`  role=img 的 SVG 缺 aria-label=${sem.svgNoLabel} 缺 <title>=${sem.svgNoTitle} | 无名按钮=${sem.btnNoLabel} | aria-live=${sem.live}`);
  if (sem.h1 !== 1) problems.push('h1 数量应为 1，实际 ' + sem.h1);
  if (sem.svgNoLabel) problems.push(`${sem.svgNoLabel} 个图表 SVG 缺 aria-label`);
  if (sem.svgNoTitle) problems.push(`${sem.svgNoTitle} 个图表 SVG 缺 <title>`);
  if (sem.btnNoLabel) problems.push(`${sem.btnNoLabel} 个按钮无可读名称`);

  /* 15. reduced motion —— 需在加载态下才有骨架可测 */
  log('\n=== 15. prefers-reduced-motion ===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.click('#seg-persona button[data-persona="duty"]');
  await page.click('#seg-state button[data-state="loading"]');
  await page.waitForTimeout(120);
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.click('#seg-state button[data-state="loading"]');
  await page.waitForTimeout(150);
  const rm = await page.evaluate(() => {
    const sk = document.querySelector('.sk');
    const row = document.querySelector('.btn');
    return {
      skCount: document.querySelectorAll('.sk').length,
      skAnim: sk ? getComputedStyle(sk).animationName + ' ' + getComputedStyle(sk).animationDuration : 'n/a',
      trans: row ? getComputedStyle(row).transitionDuration : 'n/a'
    };
  });
  log(`  骨架数=${rm.skCount} 动画=${rm.skAnim} 按钮过渡=${rm.trans}`);
  if (rm.skCount && !/none|0s|0\.001ms|1e-06s/.test(rm.skAnim)) problems.push('reduced-motion 下骨架动画未关闭: ' + rm.skAnim);
  if (!/0s|0\.001ms|1e-06s/.test(rm.trans)) problems.push('reduced-motion 下过渡未关闭: ' + rm.trans);
  await page.emulateMedia({ reducedMotion: 'no-preference' });
  await page.click('#seg-state button[data-state="live"]');
  await page.waitForTimeout(150);

  /* 截图 */
  log('\n=== 16. 截图 ===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  const shotPlan = [
    ['duty', 'light', 'desktop', 'duty-light-desktop'],
    ['supervisor', 'light', 'desktop', 'supervisor-light-desktop'],
    ['global', 'dark', 'desktop', 'global-dark-desktop'],
    ['personal', 'light', 'desktop', 'personal-light-desktop'],
    ['duty', 'light', 'mobile', 'duty-light-mobile'],
    ['engineer', 'dark', 'mobile', 'engineer-dark-mobile']
  ];
  for (const [p, ap, vp, name] of shotPlan) {
    await page.click(`#seg-persona button[data-persona="${p}"]`);
    await page.click(`#seg-appearance button[data-appearance="${ap}"]`);
    await page.click(`#seg-viewport button[data-viewport="${vp}"]`);
    await page.waitForTimeout(320);
    /* 概览展开态截图更能体现层级 */
    const open = await page.evaluate(() => document.querySelector('#overview-block').hasAttribute('open'));
    if (!open) { await page.click('#overview-block [data-collapse-toggle]'); await page.waitForTimeout(200); }
    await checkPlaceholderCopy(page, `角色 ${p}的展开概览`);
    await page.evaluate(() => document.querySelector('#main').scrollTop = 0);
    const f = path.join(screenshotDir, 'shot-' + name + '.png');
    await page.screenshot({ path: f, fullPage: false });
    log('  ✓ shot-' + name + '.png');
  }
  /* 抽屉截图 */
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.click('#seg-appearance button[data-appearance="light"]');
  await page.click('#seg-persona button[data-persona="duty"]');
  await page.waitForTimeout(250);
  await page.click('.tier[data-tier="overdue"] .row');
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(screenshotDir, 'shot-drawer.png') });
  log('  ✓ shot-drawer.png');

  log('\n════════════════════════════════════════');
  log(`占位文案检查：${placeholderFindings.size ? `${placeholderFindings.size} 处界面命中` : '通过'}`);
  for (const [view, markers] of placeholderFindings) {
    problems.push(`${view}存在占位文案：${markers.join('、')}`);
  }
  if (notes.length) { log('提示 (非阻塞):'); notes.forEach(n => log('  ⚠ ' + n)); }
  if (problems.length) {
    log(`✗ 发现 ${problems.length} 个问题:`);
    problems.forEach((p, i) => log(`  ${i + 1}. ${p}`));
    process.exitCode = 1;
    return;
  }
  log('✓ 全部检查通过');
})().catch(e => {
  console.error('脚本异常:', e && e.stack || e);
  process.exitCode = 2;
}).finally(() => browser?.close());
