/* ==========================================================================
   灯塔 · 数据中心运维管理平台 — 主入口
   作用域：全局事件委托、键盘快捷键、深链路由、打印摘要、回到顶部、
           全屏切换、空闲预加载
   依赖：LT.Data / LT.Graphics / LT.Views / LT.Lightbox
   ========================================================================== */
(function (global) {
  'use strict';

  const D = global.LT.Data;
  const G = global.LT.Graphics;
  const V = global.LT.Views;
  const LB = global.LT.Lightbox;
  const $ = id => document.getElementById(id);

  /* ------------------------------------------------------------------
     全局点击事件委托
     ------------------------------------------------------------------ */
  function bindClicks() {
    document.addEventListener('click', e => {
      /* 效果图版本切换芯片 */
      const chip = e.target.closest('.verchip button');
      if (chip) {
        e.stopPropagation();
        const t = chip.closest('.thumb');
        const ver = chip.dataset.v;
        LB.setCurVer(ver);
        const m = D.MODULES[+chip.dataset.i];
        if (m && m.img && m.img[ver]) {
          const img = t.querySelector('img');
          if (img) img.src = m.img[ver];
        }
        t.querySelectorAll('.verchip button').forEach(b => b.classList.toggle('on', b.dataset.v === ver));
        return;
      }

      /* 效果图缩略图 → 打开灯箱 */
      const th = e.target.closest('.thumb');
      if (th && th.dataset.i !== undefined) {
        LB.openLb(+th.dataset.i, th.dataset.ver || 'v2');
        return;
      }

      /* 移动端手风琴展开 / 收起 */
      const acc = e.target.closest('.acc-h');
      if (acc) {
        acc.parentElement.classList.toggle('open');
        return;
      }

      /* 模块芯片 / 关联模块 / 翻页 → 打开模块详情 */
      const mod = e.target.closest('[data-mod]');
      if (mod) {
        V.openModule(+mod.dataset.mod);
        return;
      }

      /* 流程图缩放按钮 */
      const zoomBtn = e.target.closest('[data-zoom]');
      if (zoomBtn) {
        const box = zoomBtn.closest('.flowbox');
        const svg = box && box.querySelector('svg');
        if (!svg) return;
        const cur = parseFloat(svg.dataset.zoom || '1');
        let next = cur;
        if (zoomBtn.dataset.zoom === 'in') next = Math.min(2.5, cur + 0.2);
        else if (zoomBtn.dataset.zoom === 'out') next = Math.max(0.6, cur - 0.2);
        else next = 1;
        svg.dataset.zoom = String(next);
        svg.style.minWidth = (1180 * next) + 'px';
        return;
      }

      /* 返回主页 */
      if (e.target.closest('#modBack')) { V.showView('home'); return; }

      /* 复制模块链接 */
      if (e.target.closest('#copyLink')) {
        const b = e.target.closest('#copyLink');
        const done = () => {
          const t = b.textContent;
          b.textContent = '已复制 ✓';
          setTimeout(() => { b.textContent = t; }, 1200);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(location.href).then(done, done);
        } else done();
        return;
      }

      /* 顶部按钮 */
      if (e.target.closest('#btnHome')) { V.showView('home'); return; }
      if (e.target.closest('#btnPrint')) { setTimeout(() => window.print(), 30); return; }
      if (e.target.closest('#btnFull')) {
        if (!document.fullscreenElement) {
          document.documentElement.requestFullscreen && document.documentElement.requestFullscreen();
        } else {
          document.exitFullscreen && document.exitFullscreen();
        }
        return;
      }

      /* 回到顶部 */
      if (e.target.closest('#toTop')) {
        window.scrollTo({ top: 0, behavior: 'smooth' });
        return;
      }
    });

    document.addEventListener('fullscreenchange', () => {
      $('btnFull').textContent = document.fullscreenElement ? '退出全屏' : '全屏';
    });
  }

  /* ------------------------------------------------------------------
     键盘快捷键
     ------------------------------------------------------------------ */
  function bindKeys() {
    document.addEventListener('keydown', e => {
      /* Esc：先关灯箱，再回主页 */
      if (e.key === 'Escape') {
        if (LB.getLbCur() >= 0) LB.closeLb();
        else if (document.body.dataset.view === 'module') V.showView('home');
        return;
      }
      /* 灯箱内 ← / → 切换图片 */
      if (LB.getLbCur() >= 0) {
        if (e.key === 'ArrowLeft') LB.stepLb(-1);
        if (e.key === 'ArrowRight') LB.stepLb(1);
        return;
      }
      /* 模块详情内 ← / → 切换模块 */
      if (document.body.dataset.view === 'module') {
        const a = document.activeElement;
        if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA')) return;
        const cur = V.getCurMod();
        if (e.key === 'ArrowLeft') V.openModule((cur - 1 + D.MODULES.length) % D.MODULES.length);
        if (e.key === 'ArrowRight') V.openModule((cur + 1) % D.MODULES.length);
      }
    });
  }

  /* ------------------------------------------------------------------
     滚动：回到顶部按钮显隐
     ------------------------------------------------------------------ */
  function bindScroll() {
    window.addEventListener('scroll', () => {
      $('toTop').classList.toggle('show', window.scrollY > 600);
    }, { passive: true });
  }

  /* ------------------------------------------------------------------
     打印一页纸摘要
     ------------------------------------------------------------------ */
  function renderPrint() {
    const mets = [
      [1, '7 类通告、计划通告 39、飞书同步＋回执'],
      [12, '机柜 998、上电率 97.2%、台账 1245'],
      [16, '可用签名 68、目录 97 人、一次性链接'],
      [6, '每日四轮、探针 ≤5 秒、冻结归档'],
      [3, 'MOP 表格 179、本月维保 15'],
      [4, '本月事件 23、时效自动核算'],
      [13, '本月记录 22、耗水 3,806 t'],
      [14, '维修全部 114、已完成 111'],
      [15, '账号 94、已授权 90、四维权限'],
      [10, '屏蔽记录 535、屏蔽中 35'],
      [19, '业务工具 52、写入人工确认'],
      [24, '分片 14MiB/片、哈希回读校验']
    ].map(x => `<li>${D.MODULES[x[0]].name}：${x[1]}</li>`).join('');

    const secs = Object.keys(D.SECTITLE).map(k =>
      `<p><b style="color:${D.SECCOLOR[k]}">${D.SECTITLE[k]}：</b>${D.MODULES.filter(m => m.s === k).map(m => m.name).join('、')}</p>`
    ).join('');

    $('printOne').innerHTML = `<h1>灯塔 · 数据中心运维管理平台 — 功能价值汇报（摘要）</h1>
     <p>灯塔面向数据中心一线运维场景，把事件响应、通告办理、巡检检查、设备与能耗管理、人员训练与知识沉淀整合进统一平台：以飞书多维表为协作与数据中枢，桌面端保障事件秒级响应，网页工作台承载日常业务，本地智能助手提供受控的智能办理能力。</p>
     <h2>一、整体价值（六维）</h2>
     <div class="cols2">${D.PILLARS.map(p => `<p><b>${p[0]}：</b>${p[1]}</p>`).join('')}</div>
     <h2>二、六大功能分区与 ${D.MODULES.length} 个模块</h2>${secs}
     <h2>三、关键数据快照（界面指标）</h2>
     <ul style="margin:0;padding-left:16px">${mets}</ul>
     <h2>四、平台架构与数据流向</h2>${G.archSVG()}
     <p class="pfoot">快照时间 2026-11-11 · 线上地址 dt.sjhl.online · 屏幕版支持三态交互：主页宫格 / 模块详情 / 全屏与链接直达</p>`;
  }

  /* ------------------------------------------------------------------
     空闲预加载效果图
     ------------------------------------------------------------------ */
  function preloadImages() {
    const run = () => D.MODULES.forEach(m => {
      if (!m.img) return;
      [m.img.v2, m.img.v1].forEach(s => {
        if (!s) return;
        const im = new Image();
        im.src = s;
      });
    });
    if ('requestIdleCallback' in window) requestIdleCallback(run, { timeout: 4000 });
    else setTimeout(run, 1500);
  }

  /* ------------------------------------------------------------------
     启动 + 深链路由
     ------------------------------------------------------------------ */
  function boot() {
    const INIT_HASH = location.hash || '';
    V.renderHome();
    V.showView('home');
    LB.bindLightbox();
    LB.bindHoverPreview();
    bindClicks();
    bindKeys();
    bindScroll();
    renderPrint();
    preloadImages();

    /* 深链：#/m<index> 直达模块详情 */
    const mm = INIT_HASH.match(/^#\/m(\d+)$/);
    if (mm && D.MODULES[+mm[1]]) V.openModule(+mm[1]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})(window);
