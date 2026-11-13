const { chromium } = require('playwright');
const path = require('path');
const problems = [];
const log = (...a) => console.log(...a);
const norm = s => {
  if (!s) return 'rgb(0, 0, 0)';
  let m = s.match(/color\(srgb\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)/);
  if (m) return `rgb(${Math.round(m[1] * 255)}, ${Math.round(m[2] * 255)}, ${Math.round(m[3] * 255)})`;
  m = s.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/);
  if (m) return `rgb(${Math.round(m[1])}, ${Math.round(m[2])}, ${Math.round(m[3])})`;
  return s;
};
const lum = c => { const m = c.match(/[\d.]+/g).map(Number); const f = v => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }; return 0.2126 * f(m[0]) + 0.7152 * f(m[1]) + 0.0722 * f(m[2]); };
const ratio = (a, b) => { const l1 = lum(a), l2 = lum(b); return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05); };

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
  page.on('pageerror', e => errors.push('pageerror: ' + (e && e.message)));
  await page.goto('file:///' + path.resolve(__dirname, 'index.html').split(path.sep).join('/'));
  await page.waitForTimeout(400);

  log('=== 1. 运行时错误 ===');
  if (errors.length) { errors.forEach(e => log('  ✗ ' + e)); problems.push(...errors); } else log('  ✓ 无 console error / pageerror');

  log('\n=== 2. 六块固定信息架构与演示标记 ===');
  const ia = await page.evaluate(() => ({
    present: ['#identity', '#focus', '#urgent-block', '#ops-block', '#biz-block', '#people-block', '#shortcuts-block'].map(s => !!document.querySelector(s)),
    demo: document.body.innerText.includes('演示数据'),
    nonProd: document.body.innerText.includes('非生产权限'),
    identity: document.querySelector('#identity').textContent.replace(/\s+/g, ' ').trim().slice(0, 110)
  }));
  log('  七区存在 = ' + ia.present.join(',') + ' | 演示数据=' + ia.demo + ' 非生产=' + ia.nonProd);
  log('  身份条 = ' + ia.identity);
  if (ia.present.some(v => !v)) problems.push('六块信息架构缺失');
  if (!ia.demo || !ia.nonProd) problems.push('缺少演示数据/非生产权限标记');

  log('\n=== 3. 六类角色差异与范围隔离 ===');
  const snaps = {};
  for (const r of ['manager', 'bsup', 'beng', 'duty', 'plead', 'gadmin']) {
    await page.click(`#seg-role button[data-role="${r}"]`);
    await page.waitForTimeout(170);
    snaps[r] = await page.evaluate(() => ({
      code: document.querySelector('.id-role code').textContent.trim(),
      scope: document.querySelector('.scope-chip').textContent.trim(),
      fixed: document.querySelector('.scope-chip').classList.contains('is-fixed'),
      numbers: Array.from(document.querySelectorAll('.focus-label')).map(e => e.textContent.trim()),
      tiers: Array.from(document.querySelectorAll('.tier')).map(t => t.dataset.tier + ':' + t.querySelectorAll('.row').length),
      reasons: document.querySelectorAll('.reason').length,
      ops: document.querySelectorAll('#ops-body .ops-card').length,
      biz: document.querySelectorAll('#biz-body .ops-card').length,
      people: document.querySelectorAll('#people-body .ops-card').length,
      sc: document.querySelectorAll('#shortcuts .shortcut').length
    }));
    const s = snaps[r];
    log(`  ${r.padEnd(8)} ${s.code.padEnd(21)} 范围「${s.scope}」锁定=${s.fixed} | 待办=${s.tiers.join(',')} reason=${s.reasons} | 运行${s.ops} 业务${s.biz} 人员${s.people} 入口${s.sc}`);
    if (!s.reasons) problems.push(r + ': 待办缺少 reason');
    if (!s.tiers.length) problems.push(r + ': 紧急待办为空');
  }
  const sig = r => JSON.stringify([snaps[r].numbers, snaps[r].tiers, snaps[r].scope, snaps[r].sc]);
  const ks = Object.keys(snaps);
  for (let i = 0; i < ks.length; i++) for (let j = i + 1; j < ks.length; j++) if (sig(ks[i]) === sig(ks[j])) problems.push(`角色 ${ks[i]} 与 ${ks[j]} 首屏无差异`);
  log('  ✓ 六角色两两存在实质差异');
  if (!snaps.bsup.fixed) problems.push('A 楼主管范围未锁定');
  if (snaps.manager.fixed) problems.push('园区经理范围不应锁定');

  log('\n=== 4. 紧急待办：P 级排序 + SLA + 多来源 + 同口径 ===');
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(170);
  const q = await page.evaluate(() => ({
    tiers: Array.from(document.querySelectorAll('.tier')).map(t => t.dataset.tier),
    sla: document.querySelectorAll('.sla').length,
    multi: document.querySelectorAll('.multi-src').length,
    integrity: Array.from(document.querySelectorAll('.tier-integrity')).map(e => e.textContent.trim()),
    radius: getComputedStyle(document.querySelector('.row')).borderTopLeftRadius,
    note: document.querySelector('#queue-note').textContent.trim()
  }));
  log('  分组 = ' + q.tiers.join(' → ') + ' | SLA=' + q.sla + ' 多来源=' + q.multi + ' | 行圆角=' + q.radius);
  log('  同口径 = ' + q.integrity.join(' | '));
  log('  排序口径 = ' + q.note);
  if (q.tiers.join() !== 'P0,P1,P2,P3') problems.push('P 级顺序错误: ' + q.tiers.join());
  if (!q.sla || !q.multi) problems.push('缺少 SLA 或多来源标记');
  if (!q.integrity.every(t => /同口径/.test(t))) problems.push('缺少同口径标注');
  if (q.radius !== '0px') problems.push('待办行非直角');

  log('\n=== 5. 四个分区各自的独立来源状态（§8.5/§8.6）===');
  const SEC = { biz:['#biz-body', '#biz-src'], ops:['#ops-body', '#ops-src'], people:['#people-body', '#people-src'], urgent:['#queue', '#urgent-src'] };
  for (const sec of Object.keys(SEC)) {
    await page.click(`#seg-target button[data-target-sec="${sec}"]`);
    await page.waitForTimeout(120);
    for (const st of ['stale', 'down', 'deny', 'ok']) {
      await page.click(`#seg-src button[data-src="${st}"]`);
      await page.waitForTimeout(140);
      const r = await page.evaluate(([bodySel, chipSel]) => {
        const body = document.querySelector(bodySel);
        const others = ['#ops-body', '#biz-body', '#people-body', '#queue'].filter(s => s !== bodySel);
        return {
          chip: document.querySelector(chipSel).textContent.trim(),
          err: body.querySelectorAll('.state-error').length,
          deny: body.querySelectorAll('.denial').length,
          staleNote: body.querySelectorAll('.perm-changed').length,
          othersAlive: others.every(s => {
            const o = document.querySelector(s);
            return o.querySelectorAll('.ops-card, .row, .tier, .state-error, .denial, .state').length > 0;
          })
        };
      }, SEC[sec]);
      const okChip = /延迟|不可用|拒绝|正常/.test(r.chip);
      log(`  ${sec.padEnd(7)} ${st.padEnd(6)} 芯片=[${r.chip}] 错误=${r.err} 拒绝=${r.deny} 延迟提示=${r.staleNote} 其他分区存活=${r.othersAlive} ${okChip && r.othersAlive ? '✓' : '✗'}`);
      if (!okChip) problems.push(`${sec}/${st} 芯片状态错误`);
      if (!r.othersAlive) problems.push(`${sec}/${st} 拖垮了其他分区`);
      if (st === 'down' && !r.err) problems.push(`${sec} 不可用时无区块级错误`);
      if (st === 'deny' && !r.deny) problems.push(`${sec} 权限收回时无拒绝面板`);
      if (st === 'stale' && !r.staleNote) problems.push(`${sec} 延迟时无「可能延迟」提示`);
    }
  }
  await page.click('#seg-target button[data-target-sec="biz"]');
  await page.click('#seg-src button[data-src="ok"]');
  await page.waitForTimeout(120);

  log('\n=== 5b. 跨楼栋比较表同口径下钻（§7.2）===');
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(200);
  const cell = await page.evaluate(() => {
    const b = Array.from(document.querySelectorAll('.cmp-btn')).find(x => x.dataset.drill.startsWith('C 楼|alarm|'));
    return { label: b.textContent.trim(), drill: b.dataset.drill };
  });
  await page.click(`.cmp-btn[data-drill="${cell.drill}"]`);
  await page.waitForTimeout(300);
  const dr = await page.evaluate(() => ({
    items: document.querySelectorAll('#drawer-body .drill-item').length,
    tag: Array.from(document.querySelectorAll('#drawer-tags .tag')).map(t => t.textContent.trim()).join(' | '),
    title: document.querySelector('#drawer-title').textContent.trim()
  }));
  log(`  单元格 C 楼·关键告警 = ${cell.label} → 抽屉「${dr.title}」明细 ${dr.items} 条 | ${dr.tag}`);
  if (dr.items !== Number(cell.label)) problems.push(`下钻明细 ${dr.items} 条 ≠ 卡片 ${cell.label}`);
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);
  await page.click('.cmp-th-btn[data-drill="all|todo"]');
  await page.waitForTimeout(300);
  const all = await page.evaluate(() => ({
    rows: document.querySelectorAll('#drawer-body .drill-item').length,
    total: (document.querySelector('#drawer-tags .tag-ok') || {}).textContent || ''
  }));
  const expectTotal = await page.evaluate(() => Array.from(document.querySelectorAll('.cmp tbody tr')).reduce((n, tr) => n + Number(tr.children[2].textContent.trim()), 0));
  log(`  列头下钻：6 栋楼拆分行=${all.rows} ${all.total} | 表格列合计=${expectTotal}`);
  if (all.rows !== 6) problems.push('列头下钻未列出全部楼栋');
  if (!new RegExp(String(expectTotal)).test(all.total)) problems.push(`列头合计 ${all.total} 与表格列合计 ${expectTotal} 不一致`);
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  log('\n=== 6. 无待办空态带口径 ===');
  await page.click('[data-empty-toggle]'); await page.waitForTimeout(150);
  const em = await page.evaluate(() => (document.querySelector('#queue .state') || {}).textContent || '');
  log('  空态 = ' + em.replace(/\s+/g, ' ').trim().slice(0, 80));
  if (!/口径/.test(em)) problems.push('空态缺少口径说明');
  await page.click('[data-empty-toggle]'); await page.waitForTimeout(130);

  log('\n=== 7. 待办抽屉：统一对象 + 只读深链 ===');
  await page.click('#queue .tier[data-tier="P0"] .row'); await page.waitForTimeout(300);
  const d = await page.evaluate(() => ({
    open: !document.querySelector('#drawer').hidden,
    fields: Array.from(document.querySelectorAll('#drawer-body dt')).map(e => e.textContent.trim()).join('/'),
    reason: /为什么出现在我的工作台/.test(document.querySelector('#drawer-body').textContent),
    dedup: /dedupKey/.test(document.querySelector('#drawer-body').textContent),
    foot: document.querySelector('#drawer-foot').textContent.replace(/\s+/g, ' ')
  }));
  log('  字段 = ' + d.fields);
  log('  reason=' + d.reason + ' dedup=' + d.dedup + ' | 底栏=' + d.foot.slice(0, 60));
  if (!d.open || !d.reason || !d.dedup) problems.push('抽屉字段/reason/dedup 缺失');
  if (!/只读/.test(d.foot) || !/进入原模块/.test(d.foot)) problems.push('抽屉缺少只读+深链语义');
  await page.keyboard.press('Escape'); await page.waitForTimeout(260);

  log('\n=== 8. 权限解释与越权拒绝 ===');
  await page.click('#seg-role button[data-role="bsup"]'); await page.waitForTimeout(150);
  await page.click('[data-perm]'); await page.waitForTimeout(280);
  const pm = await page.evaluate(() => ({
    rule: /飞书用户组/.test(document.querySelector('#drawer-body').textContent),
    deny: /DENY_/.test(document.querySelector('#drawer-body').textContent),
    audit: /DEC-/.test(document.querySelector('#drawer-body').textContent)
  }));
  log(`  权限解释 角色来源=${pm.rule} 拒绝规则=${pm.deny} 审计=${pm.audit}`);
  if (!pm.rule || !pm.deny) problems.push('权限解释不完整');
  await page.click('#drawer [data-deny-demo]'); await page.waitForTimeout(280);
  const dn = await page.evaluate(() => {
    const t = document.querySelector('#drawer-body').textContent;
    return { code: /DENY_SCOPE_MISMATCH/.test(t), tpl: /当前账号以【A 楼主管】访问/.test(t), audit: (t.match(/DEC-\d+-\d+/) || [null])[0] };
  });
  log(`  拒绝演示 原因码=${dn.code} 模板=${dn.tpl} 审计=${dn.audit}`);
  if (!dn.code || !dn.tpl || !dn.audit) problems.push('拒绝演示不完整');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  log('\n=== 9. 110kV 站只读边界 ===');
  await page.click('#seg-role button[data-role="plead"]'); await page.waitForTimeout(170);
  const st110 = await page.evaluate(() => {
    const t = document.querySelector('#ops-body').textContent + document.querySelector('#drawer') .textContent;
    const body = document.querySelector('#ops-body').textContent;
    return { readonly: /只读/.test(body), noExecBtn: !Array.from(document.querySelectorAll('#ops-body button')).some(b => /分合闸|倒闸|定值/.test(b.textContent)) };
  });
  log(`  110kV 卡标注只读=${st110.readonly} 无设备控制按钮=${st110.noExecBtn}`);
  if (!st110.readonly || !st110.noExecBtn) problems.push('110kV 站未保持只读边界');

  log('\n=== 10. 响应式 ===');
  for (const vp of ['desktop', 'mobile']) {
    await page.click(`#seg-viewport button[data-viewport="${vp}"]`);
    await page.waitForTimeout(220);
    for (const r of ['manager', 'bsup', 'duty']) {
      await page.click(`#seg-role button[data-role="${r}"]`);
      await page.waitForTimeout(140);
      const res = await page.evaluate(() => {
        const de = document.documentElement;
        const wide = [];
        document.querySelectorAll('.row,.focus-card,.ops-card,.identity,.scope-chip,.shortcut').forEach(el => {
          if (el.scrollWidth > el.clientWidth + 2 && getComputedStyle(el).overflowX === 'visible') wide.push((el.className || '').toString().slice(0, 30) + ' ' + el.scrollWidth + '>' + el.clientWidth);
        });
        return { over: de.scrollWidth > de.clientWidth + 1, wide: wide.slice(0, 4), btn: document.querySelector('.row-act .btn') ? getComputedStyle(document.querySelector('.row-act .btn')).minHeight : 'n/a' };
      });
      log(`  ${vp}/${r}: 溢出=${res.over} 按钮高=${res.btn} ${res.wide.length ? '✗ ' + res.wide.join('|') : '✓'}`);
      if (res.over) problems.push(`${vp}/${r} 横向溢出`);
      if (res.wide.length) problems.push(`${vp}/${r} 元素溢出: ` + res.wide.join('|'));
      if (vp === 'mobile' && parseFloat(res.btn) < 44) problems.push('移动端主操作 <44px');
    }
  }
  await page.click('#seg-viewport button[data-viewport="desktop"]');

  log('\n=== 11. 明暗对比度 ===');
  for (const ap of ['light', 'dark']) {
    await page.click(`#seg-appearance button[data-appearance="${ap}"]`);
    await page.waitForTimeout(200);
    const t = await page.evaluate(() => {
      const effBg = el => { let n = el; while (n && n !== document.documentElement) { const bg = getComputedStyle(n).backgroundColor; const m = bg.match(/rgba?\(\s*[\d.]+[,\s]+[\d.]+[,\s]+[\d.]+(?:[,\s/]+([\d.]+))?\s*\)/); const a = m && m[1] !== undefined ? parseFloat(m[1]) : 1; if (!/rgba?\(\s*0[,\s]+0[,\s]+0[,\s]+0\s*\)/.test(bg) && a > 0.05) return bg; n = n.parentElement; } return getComputedStyle(document.body).backgroundColor; };
      const N = s => { let m = s.match(/color\(srgb\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)/); if (m) return `rgb(${Math.round(m[1] * 255)}, ${Math.round(m[2] * 255)}, ${Math.round(m[3] * 255)})`; m = s.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/); if (m) return `rgb(${Math.round(m[1])}, ${Math.round(m[2])}, ${Math.round(m[3])})`; return s; };
      const title = document.querySelector('.row-title'), reason = document.querySelector('.reason'), chip = document.querySelector('.scope-chip');
      return { title: N(getComputedStyle(title).color), titleBg: N(effBg(title)), reason: N(getComputedStyle(reason).color), reasonBg: N(effBg(reason)), chip: N(getComputedStyle(chip).color), chipBg: N(effBg(chip)), body: N(getComputedStyle(document.body).color), bodyBg: N(getComputedStyle(document.body).backgroundColor) };
    });
    const c1 = ratio(t.title, t.titleBg), c2 = ratio(t.reason, t.reasonBg), c3 = ratio(t.chip, t.chipBg), c4 = ratio(t.body, t.bodyBg);
    log(`  ${ap}: 标题 ${c1.toFixed(2)} | reason ${c2.toFixed(2)} | 范围芯片 ${c3.toFixed(2)} | 正文 ${c4.toFixed(2)} ${[c1, c2, c3, c4].every(v => v >= 4.5) ? '✓' : '✗'}`);
    if (c1 < 4.5) problems.push(ap + ' 待办标题对比度不足');
    if (c2 < 4.5) problems.push(ap + ' reason 对比度不足');
    if (c3 < 4.5) problems.push(ap + ' 范围芯片对比度不足');
    if (c4 < 4.5) problems.push(ap + ' 正文对比度不足');
  }
  await page.click('#seg-appearance button[data-appearance="light"]');

  log('\n=== 12. 扫描 ===');
  const scan = await page.evaluate(() => {
    const text = document.body.innerText, html = document.documentElement.outerHTML;
    return {
      todo: text.includes('TODO') || text.includes('FIXME'),
      lorem: /lorem|ipsum/i.test(text),
      indigo: ['#6366f1', '#4f46e5', '#8b5cf6', '#7c3aed'].filter(c => html.toLowerCase().includes(c)),
      grad: (html.match(/linear-gradient|radial-gradient/g) || []).length,
      h1: document.querySelectorAll('h1').length,
      svgBad: Array.from(document.querySelectorAll('svg[role="img"]')).filter(s => !s.getAttribute('aria-label') || !s.querySelector('title')).length,
      dc: document.querySelectorAll('[data-component]').length
    };
  });
  log(`  TODO=${scan.todo} lorem=${scan.lorem} 靛蓝=${scan.indigo.length} 渐变=${scan.grad} h1=${scan.h1} 无标签SVG=${scan.svgBad} data-component=${scan.dc}`);
  if (scan.todo || scan.lorem) problems.push('占位文案');
  if (scan.indigo.length) problems.push('AI 靛蓝');
  if (scan.grad) problems.push('渐变');
  if (scan.h1 !== 1) problems.push('h1 异常');
  if (scan.svgBad) problems.push('SVG 缺标签');

  log('\n=== 13. reduced motion ===');
  await page.click('#seg-src button[data-src="down"]'); await page.waitForTimeout(120);
  await page.emulateMedia({ reducedMotion: 'reduce' }); await page.waitForTimeout(120);
  const rm = await page.evaluate(() => getComputedStyle(document.querySelector('.btn')).transitionDuration);
  log('  按钮过渡 = ' + rm);
  if (!/0s|0\.001ms|1e-06s/.test(rm)) problems.push('reduced-motion 未生效');
  await page.emulateMedia({ reducedMotion: 'no-preference' });
  await page.click('#seg-src button[data-src="ok"]');

  log('\n=== 13b. 本轮优化项 ===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(200);
  const opt = await page.evaluate(() => ({
    stickyDesktop: getComputedStyle(document.querySelector('#scope-sticky')).display,
    trendSvg: document.querySelectorAll('#ops-body svg[role="img"], #biz-body svg[role="img"]').length,
    trendLabeled: Array.from(document.querySelectorAll('#ops-body svg[role="img"], #biz-body svg[role="img"]')).every(s => s.getAttribute('aria-label') && s.querySelector('title')),
    rowLinkIsButton: document.querySelector('.row-title').tagName === 'BUTTON',
    nestedInteractive: Array.from(document.querySelectorAll('[role="button"]')).filter(el => el.querySelector('button,a,input,select')).length,
    owners: Array.from(document.querySelectorAll('.row-meta')).slice(0, 3).map(e => e.textContent.replace(/\s+/g, ' ').trim())
  }));
  log(`  桌面 sticky 栏 display=${opt.stickyDesktop}（应 none）| 趋势 SVG=${opt.trendSvg} 全部带标签=${opt.trendLabeled}`);
  log(`  行标题为真按钮=${opt.rowLinkIsButton} | role=button 内嵌套交互=${opt.nestedInteractive}（应 0）`);
  log('  责任人/编号 = ' + opt.owners.join(' | '));
  if (opt.stickyDesktop !== 'none') problems.push('桌面端不应显示 sticky 范围栏');
  if (!opt.trendSvg || !opt.trendLabeled) problems.push('趋势图缺失或缺少无障碍标签');
  if (!opt.rowLinkIsButton) problems.push('行标题不是真按钮');
  if (opt.nestedInteractive) problems.push('role=button 容器内仍有嵌套交互元素');
  if (opt.owners.some(o => /见原模块指派/.test(o))) problems.push('仍有待办缺少责任人/编号');

  await page.click('#seg-viewport button[data-viewport="mobile"]');
  await page.waitForTimeout(250);
  const mob = await page.evaluate(() => ({
    sticky: getComputedStyle(document.querySelector('#scope-sticky')).display,
    txt: document.querySelector('#scope-sticky').textContent.replace(/\s+/g, ' ').trim().slice(0, 60),
    pos: getComputedStyle(document.querySelector('#scope-sticky')).position,
    reasonBtn: getComputedStyle(document.querySelector('.reason-btn')).display,
    reasonHidden: getComputedStyle(document.querySelector('.reason')).display
  }));
  log(`  移动 sticky display=${mob.sticky} position=${mob.pos} | 内容=「${mob.txt}」`);
  log(`  移动 reason 按钮=${mob.reasonBtn} reason 默认=${mob.reasonHidden}（应 none）`);
  if (mob.sticky === 'none' || mob.pos !== 'sticky') problems.push('移动端缺少 sticky 角色/范围栏');
  if (mob.reasonHidden !== 'none') problems.push('移动端 reason 未默认折叠');
  await page.click('.reason-btn');
  await page.waitForTimeout(120);
  const opened = await page.evaluate(() => ({ d: getComputedStyle(document.querySelector('.reason')).display, aria: document.querySelector('.reason-btn').getAttribute('aria-expanded') }));
  log(`  点按后 reason display=${opened.d} aria-expanded=${opened.aria}`);
  if (opened.d === 'none' || opened.aria !== 'true') problems.push('移动端 reason 展开失败');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.waitForTimeout(200);

  const busy = await page.evaluate(async () => {
    document.querySelector('#btn-refresh').click();
    await new Promise(r => setTimeout(r, 120));
    const during = document.querySelector('#urgent-src').textContent.trim();
    await new Promise(r => setTimeout(r, 700));
    return { during, after: document.querySelector('#urgent-src').textContent.trim() };
  });
  log(`  刷新反馈 期间=「${busy.during}」 之后=「${busy.after}」`);
  if (!/更新中/.test(busy.during)) problems.push('刷新缺少「更新中」乐观反馈');
  if (!/正常/.test(busy.after)) problems.push('刷新后未恢复来源状态');

  await page.click('#seg-src button[data-src="deny"]');
  await page.waitForTimeout(180);
  const pc = await page.evaluate(() => ({
    banner: !!document.querySelector('.perm-changed'),
    txt: (document.querySelector('.perm-changed') || {}).textContent || ''
  }));
  log(`  权限收回横幅=${pc.banner} 「${pc.txt.replace(/\s+/g, ' ').trim().slice(0, 46)}…」`);
  if (!pc.banner || !/权限已变更/.test(pc.txt)) problems.push('权限收回缺少显式提示');
  await page.click('#seg-src button[data-src="ok"]');
  await page.waitForTimeout(150);

  await page.click('#queue .tier[data-tier="P0"] .row-link');
  await page.waitForTimeout(300);
  const mg = await page.evaluate(() => ({
    merged: /mergedSourceId/.test(document.querySelector('#drawer-body').textContent),
    srcStatus: /sourceStatus 正常/.test(document.querySelector('#drawer-body').textContent)
  }));
  log(`  抽屉 mergedSourceIds=${mg.merged} sourceStatus=${mg.srcStatus}`);
  if (!mg.merged) problems.push('抽屉缺少 mergedSourceIds');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(260);

  log('\n=== 13c. 本轮优化：运行卡下钻 / F2 脱敏 / 平板双列 ===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(200);
  const od = await page.evaluate(() => {
    const b = document.querySelector('#ops-body .drill-num');
    return { label: b ? b.textContent.trim() : null, drill: b ? b.dataset.drill : null };
  });
  await page.click('#ops-body .drill-num');
  await page.waitForTimeout(300);
  const odn = await page.evaluate(() => document.querySelectorAll('#drawer-body .drill-item').length);
  log(`  园区经理 告警卡数字 ${od.label} → 下钻明细 ${odn} 条 ${Number(od.label) === odn ? '✓' : '✗'}`);
  if (Number(od.label) !== odn) problems.push('告警卡下钻明细与卡片数字不一致');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);
  for (const [role, sel, expect] of [['gadmin', '#ops-body .drill-num', 8], ['duty', '#ops-body .drill-num', 8]]) {
    await page.click(`#seg-role button[data-role="${role}"]`);
    await page.waitForTimeout(180);
    const lab = await page.evaluate(s => (document.querySelector(s) || {}).textContent, sel);
    await page.click(sel); await page.waitForTimeout(280);
    const n = await page.evaluate(() => document.querySelectorAll('#drawer-body .drill-item').length);
    log(`  ${role} 运行卡下钻：卡片=${lab} 明细=${n}（期望 ${expect}）${n === expect ? '✓' : '✗'}`);
    if (n !== expect) problems.push(`${role} 运行卡下钻明细数错误`);
    await page.keyboard.press('Escape'); await page.waitForTimeout(220);
  }
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(180);
  const mask = await page.evaluate(() => ({
    masked: document.querySelectorAll('#people-body .masked').length,
    btn: !!document.querySelector('[data-perm-fields]')
  }));
  log(`  人员区脱敏联系方式=${mask.masked} 脱敏规则按钮=${mask.btn}`);
  if (!mask.masked || !mask.btn) problems.push('人员区缺少 F2 脱敏演示');
  await page.click('[data-perm-fields]');
  await page.waitForTimeout(300);
  const ft = await page.evaluate(() => ({
    rows: document.querySelectorAll('#drawer-body .f-table tbody tr').length,
    masked: document.querySelectorAll('#drawer-body .f-table .masked').length,
    title: /字段敏感度与脱敏/.test(document.querySelector('#drawer-body').textContent)
  }));
  log(`  脱敏矩阵行=${ft.rows} 含掩码示例=${ft.masked} 标题=${ft.title}`);
  if (ft.rows !== 4 || !ft.title) problems.push('字段敏感度矩阵不完整');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  for (const w of [1024, 800]) {
    await page.setViewportSize({ width: w, height: 900 });
    await page.waitForTimeout(220);
    const t = await page.evaluate(() => ({
      opsCols: getComputedStyle(document.querySelector('#ops-body')).gridTemplateColumns.split(' ').length,
      peopleCols: getComputedStyle(document.querySelector('#people-body')).gridTemplateColumns.split(' ').length,
      focusCols: getComputedStyle(document.querySelector('#focus')).gridTemplateColumns.split(' ').length,
      over: document.documentElement.scrollWidth > document.documentElement.clientWidth + 1
    }));
    log(`  ${w}px: 运行=${t.opsCols} 列 人员=${t.peopleCols} 列 关键数字=${t.focusCols} 列 溢出=${t.over} ${t.opsCols === 2 && !t.over ? '✓' : '✗'}`);
    if (t.opsCols !== 2) problems.push(`${w}px 平板断点今日运行不是双列`);
    if (t.peopleCols !== 2) problems.push(`${w}px 平板断点人员与值班不是双列`);
    if (t.over) problems.push(`${w}px 横向溢出`);
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.waitForTimeout(200);

  log('\n=== 13d. 消息直达 / 导出授权 / 影子比对 ===');
  await page.click('#seg-role button[data-role="bsup"]');
  await page.waitForTimeout(180);
  const bell = await page.evaluate(() => ({ badge: document.querySelector('#bell-badge').textContent.trim(), aria: document.querySelector('#btn-msg').getAttribute('aria-label') }));
  log(`  铃铛未读=${bell.badge} aria=「${bell.aria}」`);
  await page.click('#btn-msg');
  await page.waitForTimeout(300);
  const msg = await page.evaluate(() => ({
    items: document.querySelectorAll('.msg-item').length,
    states: Array.from(document.querySelectorAll('.msg-item .tag')).map(t => t.textContent.trim()),
    cooldown: /冷却窗口/.test(document.querySelector('#drawer-body').textContent),
    reauth: /重新校验|重新鉴权/.test(document.querySelector('#drawer-body').textContent)
  }));
  log(`  消息 ${msg.items} 条 · 状态=[${[...new Set(msg.states)].join('/')}] · 冷却去重说明=${msg.cooldown} · 打开重鉴权=${msg.reauth}`);
  if (!msg.items) problems.push('消息抽屉为空');
  if (!new Set(msg.states).size >= 2) problems.push('消息缺少多态');
  if (!msg.cooldown || !msg.reauth) problems.push('消息缺少冷却去重或重鉴权说明');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  await page.click('[data-export]');
  await page.waitForTimeout(300);
  const ex = await page.evaluate(() => {
    const t = document.querySelector('#drawer-body').textContent;
    return { single: /单次短时授权|15 分钟/.test(t), wm: /WM-20261111-0031/.test(t), sod: /申请人 ≠ 审批人/.test(t), fail: /失败关闭/.test(t), thresh: /2,000 行/.test(t) };
  });
  log(`  导出抽屉：单次短时=${ex.single} 水印=${ex.wm} 职责分离=${ex.sod} 阈值=${ex.thresh} 失败关闭=${ex.fail}`);
  if (!ex.single || !ex.wm || !ex.sod || !ex.fail) problems.push('导出授权演示要素缺失');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  const sh0 = await page.evaluate(() => document.querySelector('#shadow-panel').hidden);
  await page.click('[data-shadow-toggle]');
  await page.waitForTimeout(200);
  const sh = await page.evaluate(() => ({
    hidden: document.querySelector('#shadow-panel').hidden,
    rows: document.querySelectorAll('#shadow-panel .shadow-table tbody tr').length,
    agree: Array.from(document.querySelectorAll('#shadow-panel .shadow-table .yes')).length,
    zero: /误放行 = 0/.test(document.querySelector('#shadow-panel').textContent),
    explain: /拒绝可解释率 100%/.test(document.querySelector('#shadow-panel').textContent)
  }));
  log(`  影子面板 初始隐藏=${sh0} 打开=${!sh.hidden} 样本=${sh.rows} 一致=${sh.agree} 误放行0=${sh.zero} 可解释100%=${sh.explain}`);
  if (sh.hidden || sh.rows !== 8 || sh.agree !== 8) problems.push('影子比对表不完整');
  if (!sh.zero) problems.push('缺少误放行=0 硬门槛说明');
  await page.click('[data-shadow-close]');
  await page.waitForTimeout(150);
  const sh2 = await page.evaluate(() => document.querySelector('#shadow-panel').hidden);
  if (!sh2) problems.push('影子面板无法收起');
  log('  收起=' + sh2 + ' ✓');

  log('\n=== 13e. 同口径派生数字 / 焦点陷阱 / 跳转链接 / 契约面板 ===');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  for (const role of ['manager', 'bsup', 'duty']) {
    await page.click(`#seg-role button[data-role="${role}"]`);
    await page.waitForTimeout(180);
    const chk = await page.evaluate(() => {
      const card = document.querySelectorAll('.focus-card')[0];
      const v0 = card.querySelector('.focus-value b').textContent.trim();
      const drill = card.dataset.drill;
      let expect = null, how = '';
      if (drill === 'block:urgent') {
        expect = Array.from(document.querySelectorAll('.tier')).filter(t => ['P0', 'P1'].includes(t.dataset.tier)).reduce((n, t) => n + t.querySelectorAll('.row').length, 0);
        how = '紧急待办 P0+P1 行数';
      } else if (drill.startsWith('ops:')) {
        const key = drill.slice(4);
        const opsCard = Array.from(document.querySelectorAll('#ops-body .drill-num')).find(b => b.dataset.drill === 'ops|' + key);
        if (opsCard) { expect = opsCard.textContent.trim(); how = '今日运行同卡数字'; }
      }
      return { v0, expect, how, drill, label: card.querySelector('.focus-label').textContent.trim() };
    });
    if (chk.expect === null) { log(`  ${role}: 关键数字[0]「${chk.label}」=${chk.v0}（drill=${chk.drill}，跳过同源断言）`); continue; }
    log(`  ${role}: 关键数字[0]「${chk.label}」=${chk.v0} vs ${chk.how}=${chk.expect} ${String(chk.v0) === String(chk.expect) ? '✓' : '✗'}`);
    if (String(chk.v0) !== String(chk.expect)) problems.push(`${role} 关键数字与同源数据不同口径 (${chk.v0} vs ${chk.expect})`);
  }
  await page.click('#seg-role button[data-role="manager"]');
  await page.waitForTimeout(180);
  const fv = await page.evaluate(() => document.querySelectorAll('.focus-card')[1].querySelector('.focus-value b').textContent.trim());
  await page.click('.focus-card[data-drill^="ops:"]');
  await page.waitForTimeout(300);
  const fn = await page.evaluate(() => document.querySelectorAll('#drawer-body .drill-item').length);
  log(`  关键数字[1] 告警=${fv} → 下钻明细=${fn} ${Number(fv) === fn ? '✓' : '✗'}`);
  if (Number(fv) !== fn) problems.push('关键数字下钻明细不一致');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);
  await page.click('.focus-card[data-drill="ref"]').catch(() => {});
  await page.waitForTimeout(200);
  const refOpen = await page.evaluate(() => !document.querySelector('#drawer').hidden);
  log(`  历史口径卡点击不打开明细抽屉 = ${!refOpen ? '✓' : '✗'}`);
  if (refOpen) problems.push('历史口径 KPI 不应打开明细抽屉');

  await page.click('#queue .tier[data-tier="P0"] .row-link');
  await page.waitForTimeout(320);
  const trap = await page.evaluate(async () => {
    const d = document.querySelector('#drawer');
    const f = () => Array.from(d.querySelectorAll('button,[href],input,select,textarea')).filter(el => !el.disabled && el.offsetParent !== null);
    const list = f();
    list[list.length - 1].focus();
    return { last: document.activeElement === list[list.length - 1], count: list.length };
  });
  await page.keyboard.press('Tab');
  const wrapped = await page.evaluate(() => {
    const d = document.querySelector('#drawer');
    return d.contains(document.activeElement);
  });
  log(`  焦点陷阱：抽屉内可聚焦 ${trap.count} 个 · 末元素 Tab 后仍在抽屉内=${wrapped ? '✓' : '✗'}`);
  if (!wrapped) problems.push('抽屉焦点陷阱失效');
  await page.keyboard.press('Escape'); await page.waitForTimeout(240);

  /* 跳转链接：重新加载后首次 Tab 必须落在 skip link（避免顺序焦点起点残留） */
  await page.reload({ waitUntil: 'load' });
  await page.waitForTimeout(350);
  await page.keyboard.press('Tab');
  const skip = await page.evaluate(() => (document.activeElement.className || '').toString().includes('skip-link'));
  const skipTarget = await page.evaluate(() => document.activeElement.getAttribute('href'));
  log(`  加载后首个 Tab 落在跳转链接=${skip ? '✓' : '✗'} href=${skipTarget}`);
  if (!skip) problems.push('缺少跳转到主内容链接');

  const ct0 = await page.evaluate(() => document.querySelector('#contract-panel').hidden);
  await page.click('[data-contract-toggle]');
  await page.waitForTimeout(200);
  const ct = await page.evaluate(() => ({
    hidden: document.querySelector('#contract-panel').hidden,
    rows: document.querySelectorAll('#contract-panel .shadow-table tbody tr').length,
    apis: Array.from(document.querySelectorAll('#contract-panel code')).slice(0, 5).map(c => c.textContent.trim())
  }));
  log(`  契约面板 初始隐藏=${ct0} 打开=${!ct.hidden} 接口行=${ct.rows} | ${ct.apis.slice(0, 3).join(' / ')}`);
  if (ct.hidden || ct.rows !== 5) problems.push('接口契约面板不完整');
  await page.click('[data-contract-close]'); await page.waitForTimeout(150);

  await page.click('#seg-viewport button[data-viewport="mobile"]');
  await page.waitForTimeout(250);
  const mref = await page.evaluate(() => getComputedStyle(document.querySelector('.row-meta .num')).display);
  log(`  移动端对象编号 display=${mref}（应 none，五要素任务卡）`);
  if (mref !== 'none') problems.push('移动端任务卡未收敛到五要素');
  await page.click('#seg-viewport button[data-viewport="desktop"]');
  await page.waitForTimeout(200);

  log('\n=== 14. 截图 ===');
  const shots = [
    ['manager', 'light', 'desktop', 'v2-manager-light'],
    ['bsup', 'light', 'desktop', 'v2-bsup-light'],
    ['duty', 'light', 'desktop', 'v2-duty-light'],
    ['gadmin', 'dark', 'desktop', 'v2-gadmin-dark'],
    ['plead', 'dark', 'desktop', 'v2-plead-dark'],
    ['bsup', 'light', 'mobile', 'v2-bsup-mobile']
  ];
  for (const [r, ap, vp, name] of shots) {
    await page.click(`#seg-role button[data-role="${r}"]`);
    await page.click(`#seg-appearance button[data-appearance="${ap}"]`);
    await page.click(`#seg-viewport button[data-viewport="${vp}"]`);
    await page.waitForTimeout(280);
    await page.evaluate(() => document.querySelector('#main').scrollTop = 0);
    await page.screenshot({ path: path.join(__dirname, 'shot-' + name + '.png') });
    log('  ✓ shot-' + name + '.png');
  }
  await browser.close();
  log('\n════════════════════════════════════════');
  if (problems.length) { log(`✗ ${problems.length} 个问题:`); problems.forEach((p, i) => log(`  ${i + 1}. ${p}`)); process.exit(1); }
  log('✓ 全部检查通过');
})().catch(e => { console.error('脚本异常:', e && e.stack || e); process.exit(2); });
