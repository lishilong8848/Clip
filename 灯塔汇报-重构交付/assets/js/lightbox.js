/* ==========================================================================
   灯塔 · 数据中心运维管理平台 — 灯箱与悬浮预览
   作用域：效果图放大查看（Lightbox）、悬浮预览（Hover Preview）
   依赖：LT.Data（data.js）
   ========================================================================== */
(function (global) {
  'use strict';

  const D = global.LT.Data;
  const $ = id => document.getElementById(id);

  /* ------------------------------------------------------------------
     灯箱（Lightbox）
     ------------------------------------------------------------------ */
  let lbCur = -1;
  let lbScale = 1;
  let curVer = 'v2';

  function lbRender() {
    const m = D.MODULES[lbCur];
    if (!m || !m.img) return;
    $('lbArt').innerHTML = `<img src="${m.img[curVer]}" alt="${m.name} ${curVer === 'v2' ? '新版' : '旧版'}界面大图">`;
    $('lbTitle').textContent = m.name + '　|　' + (curVer === 'v2' ? '新版页面' : '旧版页面');
    $('lbIdx').textContent = (lbCur + 1) + ' / ' + D.MODULES.length;
    $('lbVer1').style.display = m.img.v1 ? '' : 'none';
    lbScale = 1;
  }

  function openLb(i, ver) {
    lbCur = i;
    curVer = ver || 'v2';
    lbRender();
    $('lb').classList.add('on');
    document.body.style.overflow = 'hidden';
  }

  function closeLb() {
    $('lb').classList.remove('on');
    document.body.style.overflow = '';
    lbCur = -1;
  }

  function stepLb(d) {
    if (lbCur < 0) return;
    openLb((lbCur + d + D.MODULES.length) % D.MODULES.length, curVer);
  }

  function getLbCur() { return lbCur; }
  function getCurVer() { return curVer; }
  function setCurVer(v) { curVer = v; }

  function bindLightbox() {
    $('lbClose').onclick = closeLb;
    $('lbPrev').onclick = () => stepLb(-1);
    $('lbNext').onclick = () => stepLb(1);
    $('lbVer2').onclick = () => { curVer = 'v2'; lbRender(); };
    $('lbVer1').onclick = () => { curVer = 'v1'; lbRender(); };
    $('lbMod').onclick = () => {
      if (lbCur < 0) return;
      const i = lbCur;
      closeLb();
      global.LT.Views.openModule(i);
    };
    $('lb').addEventListener('click', e => { if (e.target === $('lb')) closeLb(); });

    /* 滚轮缩放 */
    $('lbArt').addEventListener('wheel', e => {
      const img = $('lbArt').querySelector('img');
      if (!img) return;
      e.preventDefault();
      lbScale = Math.min(4, Math.max(.5, lbScale * (e.deltaY < 0 ? 1.15 : 1 / 1.15)));
      img.style.transform = 'scale(' + lbScale + ')';
    }, { passive: false });

    /* 双击还原 */
    $('lbArt').addEventListener('dblclick', () => {
      const img = $('lbArt').querySelector('img');
      if (img) { lbScale = 1; img.style.transform = 'scale(1)'; }
    });
  }

  /* ------------------------------------------------------------------
     悬浮预览（220ms 延时、边缘翻转、触屏禁用）
     ------------------------------------------------------------------ */
  function bindHoverPreview() {
    const hp = $('hoverPrev');
    const hpImg = hp.querySelector('img');
    let hpTimer = 0;

    const hpPlace = e => {
      const w = 520, h = 300;
      let x = e.clientX + 18, y = e.clientY + 18;
      if (x + w > window.innerWidth - 8) x = e.clientX - w - 18;
      if (y + h > window.innerHeight - 8) y = e.clientY - h - 18;
      hp.style.left = Math.max(8, x) + 'px';
      hp.style.top = Math.max(8, y) + 'px';
    };

    document.addEventListener('mouseover', e => {
      if ('ontouchstart' in window) return;
      const img = e.target.closest ? e.target.closest('.thumb img') : null;
      if (!img || !img.complete || !img.naturalWidth) return;
      clearTimeout(hpTimer);
      hpTimer = setTimeout(() => {
        hpImg.src = img.getAttribute('src');
        hp.style.display = 'block';
        hpPlace(e);
      }, 220);
    });

    document.addEventListener('mousemove', e => {
      if (hp.style.display === 'block') hpPlace(e);
    });

    document.addEventListener('mouseout', e => {
      const c = e.target && e.target.closest;
      const from = c ? e.target.closest('.thumb') : null;
      const r = e.relatedTarget && e.relatedTarget.closest;
      const to = r ? e.relatedTarget.closest('.thumb') : null;
      if (from && !to) {
        clearTimeout(hpTimer);
        hp.style.display = 'none';
      }
    });

    window.addEventListener('scroll', () => {
      clearTimeout(hpTimer);
      hp.style.display = 'none';
    }, { passive: true });
  }

  global.LT.Lightbox = {
    bindLightbox, bindHoverPreview,
    openLb, closeLb, stepLb,
    getLbCur, getCurVer, setCurVer
  };
})(window);
