/* ==========================================================================
   灯塔 · 数据中心运维管理平台 — 视图渲染层
   作用域：主页视图（Hero / 架构图 / 中枢 / 六大功能域 / 数据底座 / 价值带）
           模块详情视图（面包屑 / 头部 / 流程图 / 场景 / 效果图 / 关联 / 翻页）
   依赖：LT.Data（data.js）、LT.Graphics（graphics.js）
   ========================================================================== */
(function (global) {
  'use strict';

  const D = global.LT.Data;
  const G = global.LT.Graphics;
  const $ = id => document.getElementById(id);

  /* ------------------------------------------------------------------
     视图切换
     ------------------------------------------------------------------ */
  function showView(v) {
    document.body.dataset.view = v;
    $('btnHome').classList.toggle('on', v === 'home');
    const el = v === 'home' ? $('viewHome') : $('viewModule');
    el.classList.remove('anim');
    void el.offsetWidth;
    el.classList.add('anim');
    if (v === 'home') history.replaceState(null, '', '#/');
    window.scrollTo(0, 0);
  }

  /* ------------------------------------------------------------------
     主页渲染（重设计版：思维导图画布 + 底座总线 + 折叠价值带 + 移动手风琴）
     ------------------------------------------------------------------ */
  function renderHome() {
    const secKeys = Object.keys(D.SECTITLE);
    const totalMods = D.MODULES.length;

    let h = `<div class="wrap">

      <!-- 精简 Hero 条 -->
      <div class="hero">
        <div class="hero-l">
          <div class="kick">南通数据中心 · 运维数字化平台</div>
          <h1>灯塔 · 数据中心运维管理平台</h1>
          <p>下方全景图即项目架构：中心为调度中枢，向六大功能域发散、再展开为 ${totalMods} 个功能模块。滚轮缩放、拖拽平移；悬浮模块看职责与指标，点击模块进详情（流程图＋场景价值＋真实界面），点击域节点可折叠。</p>
        </div>
        <div class="stats">${D.HERO_STATS.map(s => `<span>${s}</span>`).join('')}</div>
      </div>

      <!-- 交互式架构思维导图 -->
      <section class="mm">
        <div class="mm-bar">
          <h2>运维全景图 · 交互式架构思维导图</h2>
          <p class="sub">中枢 → 六大功能域 → ${totalMods} 个模块；紫色虚线为模块间关联</p>
          <div class="mm-tools">
            <button type="button" id="mmOut" title="缩小">−</button>
            <button type="button" id="mmReset" title="还原视图">还原</button>
            <button type="button" id="mmIn" title="放大">＋</button>
          </div>
        </div>
        <div class="mm-canvas" id="mmCanvas"></div>
        <div class="mm-legend">
          ${secKeys.map(k => `<span class="lg"><i style="background:${D.SECCOLOR[k]}"></i>${D.SECTITLE[k]}</span>`).join('')}
          <span class="hint">悬浮看职责 · 点击进详情 · 点域折叠 · 滚轮缩放 · 拖拽平移</span>
        </div>
      </section>
      <div class="mm-tip" id="mmTip" role="tooltip"></div>

      <!-- 移动端手风琴索引（窄屏显示） -->
      <section class="acc-wrap" aria-label="功能模块索引">
        ${secKeys.map(k => {
          const list = D.MODULES.filter(m => m.s === k);
          return `<div class="acc">
            <button type="button" class="acc-h" data-acc>
              <i style="background:${D.SECCOLOR[k]}"></i>
              <b>${D.SECTITLE[k]}</b>
              <small>${list.length} 个功能</small>
              <span class="caret" aria-hidden="true"></span>
            </button>
            <div class="acc-b">${list.map(m => `<button type="button" data-mod="${m.i}">${m.name}</button>`).join('')}</div>
          </div>`;
        }).join('')}
      </section>

      <!-- 数据底座总线 -->
      <section class="foundation">
        <h2>数据底座 · 全链路支撑</h2>
        <p class="sub">协同 × 状态 × 知识 × 外部联动，为上层 ${totalMods} 个功能模块提供统一数据与证据链</p>
        <div class="bus" aria-hidden="true"><i></i><i></i><i></i><i></i></div>
        <div class="fcols">
          ${D.FOUNDATION.map(f => `<div class="fcol">
            <div class="icon">${f.icon}</div>
            <b>${f.title}</b>
            <p>${f.desc}</p>
          </div>`).join('')}
        </div>
      </section>

      <!-- 整体价值（可折叠） -->
      <details class="band">
        <summary>整体价值 · 六维一条主线</summary>
        <div class="cols">
          ${D.PILLARS.map(p => `<div class="col"><b>${p[0]}</b><p>${p[1]}</p></div>`).join('')}
        </div>
      </details>

      <p class="foot">截图与指标快照时间 2026-11-11 · 主页 / 详情双视图，可离线演示 · 支持全屏演示与一页纸打印摘要</p>
    </div>`;

    $('viewHome').innerHTML = h;

    /* 挂载思维导图交互 */
    if (global.LT.Mindmap && global.LT.Mindmap.mount) global.LT.Mindmap.mount();
  }

  /* ------------------------------------------------------------------
     模块详情渲染
     ------------------------------------------------------------------ */
  let curMod = -1;

  function openModule(i) {
    const m = D.MODULES[i];
    const f = D.FLOWS[i];
    const mc = D.SECCOLOR[m.s];
    const pv = D.MODULES[(i - 1 + D.MODULES.length) % D.MODULES.length];
    const nx = D.MODULES[(i + 1) % D.MODULES.length];
    const secIdx = Object.keys(D.SECTITLE).indexOf(m.s) + 1;

    /* 效果图（新版 / 旧版） */
    const shot = (ver, label, color) => m.img && m.img[ver]
      ? `<div class="shotcell">
           <div class="thumb" data-i="${i}" data-ver="${ver}" role="button" tabindex="0" aria-label="放大查看 ${m.name} ${label}">
             <img src="${m.img[ver]}" alt="${m.name} ${label}界面截图" loading="lazy">
             <span class="verchip">
               <button type="button" class="${ver === 'v2' ? 'on' : ''}" data-v="v2" data-i="${i}">新版</button>
               ${m.img.v1 ? `<button type="button" data-v="v1" data-i="${i}">旧版</button>` : ''}
             </span>
           </div>
           <small><i style="background:${color}"></i>${label}界面</small>
         </div>`
      : '';

    const shots = m.img
      ? `<h3 class="h2">界面效果图（点击放大 · 悬浮预览 · 芯片切换新旧版）</h3>
         <div class="shots">${shot('v2', '新版', '#087a61')}${shot('v1', '旧版', '#64748b')}</div>`
      : `<h3 class="h2">界面效果图</h3>
         <div class="shots">
           <div class="shotcell">
             <div class="thumb" data-i="${i}" data-ver="v2">
               <span class="noimg">${m.name} · 后台/桌面能力，无独立网页界面</span>
             </div>
             <small><i style="background:#8aa0b8"></i>说明</small>
           </div>
         </div>`;

    const rel = (D.REL[i] || [])
      .map(j => `<button type="button" data-mod="${j}">${D.MODULES[j].name}</button>`)
      .join('');

    $('viewModule').innerHTML = `<div class="wrap mod" style="--mc:${mc}">

      <!-- 操作条 + 面包屑 -->
      <div class="mod-bar">
        <button type="button" class="mback" id="modBack">← 返回主页</button>
        <button type="button" class="mback" id="copyLink">复制模块链接</button>
        <span class="crumb">
          <span>主页</span><span class="sep">/</span>
          <span>${D.SECTITLE[m.s]}</span><span class="sep">/</span>
          <b>${m.name}</b>
        </span>
      </div>

      <!-- 模块头部 -->
      <header class="mhead">
        <div class="kick">${String(secIdx).padStart(2, '0')} · ${D.SECTITLE[m.s]}</div>
        <h1>${m.name}<span class="pill">${m.pill}</span></h1>
        <p>${m.tag}</p>
        <div class="chips">
          <b>数据快照</b>
          ${(D.METRICS[i] || []).map(x => `<span>${x}</span>`).join('')}
        </div>
      </header>

      <!-- 业务流程图 -->
      <h3 class="h2">功能流程图（业务逻辑）</h3>
      <div class="flowbox" id="flowBox">
        <div class="zoombar">
          <button type="button" data-zoom="out" title="缩小">−</button>
          <button type="button" data-zoom="reset" title="还原">1:1</button>
          <button type="button" data-zoom="in" title="放大">＋</button>
        </div>
        ${G.flowSVG(f, m)}
      </div>

      <!-- 业务场景与价值 -->
      <h3 class="h2">业务场景与价值</h3>
      <div class="scene">
        <div class="quote"><b>场景回放</b>${D.SCENES[i]}</div>
        <div class="cols">
          <div class="step why"><b>因为 · 痛点 / 需求</b>${m.fix}</div>
          <div class="step how"><b>所以 · 设计实现</b>${m.use}</div>
          <div class="step val"><b>带来 · 业务价值</b>${m.val}</div>
        </div>
      </div>

      ${shots}

      ${rel ? `<h3 class="h2">关联模块</h3><div class="rel">${rel}</div>` : ''}

      <!-- 翻页 -->
      <div class="pn">
        <button type="button" data-mod="${pv.i}"><small>← 上一个</small>${pv.name}</button>
        <button type="button" data-mod="${nx.i}"><small>下一个 →</small>${nx.name}</button>
      </div>

      <p class="tip">提示：键盘 ← / → 切换模块，Esc 返回主页；效果图点击放大、放大后滚轮缩放、双击还原；流程图右上角按钮可缩放查看细节。</p>
    </div>`;

    curMod = i;
    showView('module');
    history.replaceState(null, '', '#/m' + i);
  }

  function getCurMod() { return curMod; }
  function setCurMod(v) { curMod = v; }

  global.LT.Views = { showView, renderHome, openModule, getCurMod, setCurMod };
})(window);
