/* =====================================================================
 * 灯塔汇报站 —— 通用交互层
 * 画布缩放拖动 / 节点悬浮说明 / 节点跳转 / 灯箱 / 缩略图悬浮预览 / 顶栏
 * 依赖：无（原生 JS，file:// 直接可用）
 * ===================================================================== */
window.DTU = (function () {
  'use strict';

  var $ = function (id) { return document.getElementById(id); };
  var $$ = function (sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); };
  function esc(s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function qs(name) {
    var m = new RegExp('[?&]' + name + '=([^&#]*)').exec(location.search);
    return m ? decodeURIComponent(m[1]) : '';
  }
  var isTouch = function () { return ('ontouchstart' in window) || navigator.maxTouchPoints > 0; };

  /* =====================================================================
   * 画布：滚轮缩放（以光标为锚点）+ 拖动平移 + 双指捏合 + 适应/复位
   * ===================================================================== */
  function canvas(host, size, opts) {
    opts = opts || {};
    var svg = host.querySelector('svg');
    if (!svg) return null;
    var k = 1, tx = 0, ty = 0;
    var minK = opts.minK || 0.25, maxK = opts.maxK || 4;
    var ptrs = new Map(), dragging = false, moved = 0, last = null, pinch = null;

    svg.style.width = size.w + 'px';
    svg.style.height = size.h + 'px';

    function apply() {
      svg.style.transform = 'translate(' + tx + 'px,' + ty + 'px) scale(' + k + ')';
      if (opts.onZoom) opts.onZoom(k);
    }
    /* 整幅适应：全部内容收进画布 */
    function fitWhole(pad) {
      var hw = host.clientWidth, hh = host.clientHeight;
      if (!hw || !hh) return;
      pad = pad == null ? 0.985 : pad;
      k = Math.min(hw / size.w, hh / size.h) * pad;
      k = Math.max(minK, Math.min(maxK, k));
      tx = (hw - size.w * k) / 2;
      ty = (hh - size.h * k) / 2;
      apply();
    }
    /* 按宽度适应：保证文字可读，纵向用平移查看 */
    function fitWidth(pad) {
      var hw = host.clientWidth, hh = host.clientHeight;
      if (!hw || !hh) return;
      pad = pad == null ? 0.985 : pad;
      k = Math.max(minK, Math.min(maxK, (hw / size.w) * pad));
      tx = (hw - size.w * k) / 2;
      var sh = size.h * k;
      ty = sh <= hh ? (hh - sh) / 2 : 6;
      apply();
    }
    function fit(mode) { return mode === 'width' ? fitWidth() : fitWhole(); }
    function zoomAt(cx, cy, factor) {
      var nk = Math.max(minK, Math.min(maxK, k * factor));
      if (nk === k) return;
      tx = cx - (cx - tx) * (nk / k);
      ty = cy - (cy - ty) * (nk / k);
      k = nk; apply();
    }
    function zoomBy(factor) { zoomAt(host.clientWidth / 2, host.clientHeight / 2, factor); }

    host.addEventListener('wheel', function (e) {
      e.preventDefault();
      var r = host.getBoundingClientRect();
      zoomAt(e.clientX - r.left, e.clientY - r.top, e.deltaY < 0 ? 1.14 : 1 / 1.14);
    }, { passive: false });

    host.addEventListener('pointerdown', function (e) {
      if (e.button != null && e.button !== 0 && e.pointerType === 'mouse') return;
      ptrs.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (ptrs.size === 1) {
        last = { x: e.clientX, y: e.clientY }; moved = 0; dragging = false;
        host.setPointerCapture(e.pointerId);
        host.classList.add('grabbing');
      } else if (ptrs.size === 2) {
        var p = Array.from(ptrs.values());
        pinch = { d: dist(p[0], p[1]), k: k, tx: tx, ty: ty };
        dragging = true; moved = 999;
      }
    });
    host.addEventListener('pointermove', function (e) {
      if (!ptrs.has(e.pointerId)) return;
      ptrs.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (ptrs.size === 2 && pinch) {
        var p = Array.from(ptrs.values()), d = dist(p[0], p[1]);
        if (pinch.d > 0) {
          var r = host.getBoundingClientRect();
          var cx = (p[0].x + p[1].x) / 2 - r.left, cy = (p[0].y + p[1].y) / 2 - r.top;
          var nk = Math.max(minK, Math.min(maxK, pinch.k * (d / pinch.d)));
          tx = cx - (cx - pinch.tx) * (nk / pinch.k);
          ty = cy - (cy - pinch.ty) * (nk / pinch.k);
          k = nk; apply();
        }
        return;
      }
      if (!last) return;
      var dx = e.clientX - last.x, dy = e.clientY - last.y;
      moved += Math.abs(dx) + Math.abs(dy);
      if (moved > 5) dragging = true;
      if (dragging) { tx += dx; ty += dy; apply(); }
      last = { x: e.clientX, y: e.clientY };
    });
    function up(e) {
      ptrs.delete(e.pointerId);
      if (ptrs.size < 2) pinch = null;
      if (ptrs.size === 0) {
        host.classList.remove('grabbing');
        last = null;
        setTimeout(function () { dragging = false; moved = 0; }, 0);
      }
    }
    host.addEventListener('pointerup', up);
    host.addEventListener('pointercancel', up);
    /* 拖动后不触发点击 */
    host.addEventListener('click', function (e) { if (dragging || moved > 5) { e.stopPropagation(); e.preventDefault(); } }, true);
    host.addEventListener('dblclick', function () { fit(defMode); });

    function dist(a, b) { return Math.hypot(a.x - b.x, a.y - b.y); }

    var defMode = opts.fitMode || 'whole';
    var api = {
      fit: function (mode) { fit(mode || defMode); },
      fitWidth: fitWidth, fitWhole: fitWhole,
      zoomIn: function () { zoomBy(1.2); }, zoomOut: function () { zoomBy(1 / 1.2); },
      scale: function () { return k; }
    };
    if (opts.fit !== false) requestAnimationFrame(function () { fit(defMode); });
    /* 画布尺寸变化（窗口缩放 / 面板从隐藏变为可见）时自动重新适配 */
    if (opts.refit !== false && typeof ResizeObserver !== 'undefined') {
      var lastW = 0, lastH = 0;
      var ro = new ResizeObserver(function (entries) {
        var r = entries[0].contentRect;
        if (Math.abs(r.width - lastW) < 1 && Math.abs(r.height - lastH) < 1) return;
        lastW = r.width; lastH = r.height;
        if (r.width > 0 && r.height > 0) fit(defMode);
      });
      ro.observe(host);
    }
    return api;
  }

  /* =====================================================================
   * 节点悬浮说明：读取 data-name / data-desc / data-pill，边缘自动翻转钳制
   * ===================================================================== */
  function nodeTip(host, sel, tipEl) {
    if (isTouch()) return;
    var timer = 0, cur = null;
    function place(e) {
      var w = tipEl.offsetWidth || 300, h = tipEl.offsetHeight || 90;
      var x = e.clientX + 16, y = e.clientY + 18;
      if (x + w > window.innerWidth - 10) x = e.clientX - w - 16;
      if (y + h > window.innerHeight - 10) y = Math.max(10, e.clientY - h - 14);
      tipEl.style.left = Math.max(8, x) + 'px';
      tipEl.style.top = Math.max(8, y) + 'px';
    }
    function hide() { clearTimeout(timer); cur = null; tipEl.classList.remove('on'); }
    host.addEventListener('pointerover', function (e) {
      var n = e.target.closest ? e.target.closest(sel) : null;
      if (!n || n === cur) return;
      cur = n;
      clearTimeout(timer);
      timer = setTimeout(function () {
        tipEl.innerHTML = '<b>' + esc(n.dataset.name || '') + '</b>' +
          (n.dataset.pill ? '<span class="tp-pill">' + esc(n.dataset.pill) + '</span>' : '') +
          (n.dataset.desc ? '<p>' + esc(n.dataset.desc) + '</p>' : '') +
          (n.dataset.href ? '<i class="tp-go">点击查看模块详情 →</i>' : '');
        tipEl.classList.add('on');
        place(e);
      }, 160);
    });
    host.addEventListener('pointermove', function (e) { if (tipEl.classList.contains('on')) place(e); });
    host.addEventListener('pointerout', function (e) {
      var from = e.target.closest ? e.target.closest(sel) : null;
      var to = e.relatedTarget && e.relatedTarget.closest ? e.relatedTarget.closest(sel) : null;
      if (from && !to) hide();
    });
    window.addEventListener('scroll', hide, { passive: true });
    return { hide: hide };
  }

  /* =====================================================================
   * 节点跳转：data-href（含键盘 Enter / Space）
   * ===================================================================== */
  function nodeNav(root, sel) {
    root.addEventListener('click', function (e) {
      var n = e.target.closest ? e.target.closest(sel) : null;
      if (!n || !n.dataset.href) return;
      location.href = n.dataset.href;
    });
    root.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var n = e.target.closest ? e.target.closest(sel) : null;
      if (!n || !n.dataset.href) return;
      e.preventDefault(); location.href = n.dataset.href;
    });
  }

  /* =====================================================================
   * 顶栏：打印 / 全屏 / 回到顶部
   * ===================================================================== */
  function topbar() {
    var bp = $('btnPrint'), bf = $('btnFull'), bt = $('toTop');
    if (bp) bp.addEventListener('click', function () { setTimeout(function () { window.print(); }, 40); });
    if (bf) bf.addEventListener('click', function () {
      if (!document.fullscreenElement) { if (document.documentElement.requestFullscreen) document.documentElement.requestFullscreen(); }
      else if (document.exitFullscreen) document.exitFullscreen();
    });
    document.addEventListener('fullscreenchange', function () {
      if (bf) bf.textContent = document.fullscreenElement ? '退出全屏' : '全屏';
    });
    if (bt) {
      window.addEventListener('scroll', function () { bt.classList.toggle('show', window.scrollY > 520); }, { passive: true });
      bt.addEventListener('click', function () { window.scrollTo({ top: 0, behavior: 'smooth' }); });
    }
  }

  /* =====================================================================
   * 灯箱：放大效果图，支持滚轮缩放、双击还原、左右切换、新旧版切换
   * ===================================================================== */
  function lightbox(getItem, count, hrefOf) {
    var lb = $('lb'); if (!lb) return null;
    var art = $('lbArt'), ttl = $('lbTitle'), idx = $('lbIdx');
    var cur = -1, ver = 'v2', scale = 1;

    function render() {
      var it = getItem(cur); if (!it) return;
      art.innerHTML = '<img src="' + esc(it.img[ver]) + '" alt="' + esc(it.name + ' ' + (ver === 'v2' ? '新版' : '旧版') + '界面大图') + '">';
      ttl.textContent = it.name + '　|　' + (ver === 'v2' ? '新版页面' : '旧版页面');
      idx.textContent = (cur + 1) + ' / ' + count;
      var v1 = $('lbVer1'); if (v1) v1.style.display = it.img.v1 ? '' : 'none';
      scale = 1;
    }
    function open(i, v) { cur = i; ver = v || 'v2'; render(); lb.classList.add('on'); document.body.style.overflow = 'hidden'; }
    function close() { lb.classList.remove('on'); document.body.style.overflow = ''; cur = -1; }
    function step(d) {
      if (cur < 0) return;
      var n = cur;
      for (var t = 0; t < count; t++) {
        n = (n + d + count) % count;
        var it = getItem(n);
        if (it && it.img && it.img[ver]) { break; }
        if (it && it.img) { ver = it.img.v2 ? 'v2' : 'v1'; break; }
      }
      open(n, ver);
    }
    $('lbClose').onclick = close;
    $('lbPrev').onclick = function () { step(-1); };
    $('lbNext').onclick = function () { step(1); };
    var b2 = $('lbVer2'), b1 = $('lbVer1');
    if (b2) b2.onclick = function () { ver = 'v2'; render(); };
    if (b1) b1.onclick = function () { ver = 'v1'; render(); };
    var bm = $('lbMod');
    if (bm) bm.onclick = function () {
      if (cur < 0) return;
      var target = hrefOf ? hrefOf(cur) : ('module.html?id=' + cur);
      close();
      if (target) location.href = target;
    };
    lb.addEventListener('click', function (e) { if (e.target === lb) close(); });
    art.addEventListener('wheel', function (e) {
      var img = art.querySelector('img'); if (!img) return;
      e.preventDefault();
      scale = Math.min(4, Math.max(0.5, scale * (e.deltaY < 0 ? 1.15 : 1 / 1.15)));
      img.style.transform = 'scale(' + scale + ')';
    }, { passive: false });
    art.addEventListener('dblclick', function () {
      var img = art.querySelector('img'); if (img) { scale = 1; img.style.transform = 'scale(1)'; }
    });
    document.addEventListener('keydown', function (e) {
      if (cur < 0) return;
      if (e.key === 'Escape') { close(); return; }
      if (e.key === 'ArrowLeft') step(-1);
      if (e.key === 'ArrowRight') step(1);
    });
    return { open: open, close: close, isOpen: function () { return cur >= 0; }, cur: function () { return cur; } };
  }

  /* =====================================================================
   * 缩略图悬浮预览：约 220ms 延时、边缘翻转钳制、触屏禁用
   * ===================================================================== */
  function hoverPreview() {
    if (isTouch()) return null;
    var hp = $('hoverPrev'); if (!hp) return null;
    var img = hp.querySelector('img'), timer = 0;
    function place(e) {
      var w = hp.offsetWidth || 520, h = hp.offsetHeight || 300;
      var x = e.clientX + 18, y = e.clientY + 18;
      if (x + w > window.innerWidth - 8) x = e.clientX - w - 18;
      if (y + h > window.innerHeight - 8) y = e.clientY - h - 18;
      hp.style.left = Math.max(8, x) + 'px';
      hp.style.top = Math.max(8, y) + 'px';
    }
    function hide() { clearTimeout(timer); hp.classList.remove('on'); }
    document.addEventListener('mouseover', function (e) {
      var t = e.target.closest ? e.target.closest('.thumb img') : null;
      if (!t || !t.complete || !t.naturalWidth) return;
      clearTimeout(timer);
      timer = setTimeout(function () { img.src = t.getAttribute('src'); hp.classList.add('on'); place(e); }, 220);
    });
    document.addEventListener('mousemove', function (e) { if (hp.classList.contains('on')) place(e); });
    document.addEventListener('mouseout', function (e) {
      var c = function (el) { return el && el.closest ? el.closest('.thumb') : null; };
      if (c(e.target) && !c(e.relatedTarget)) hide();
    });
    window.addEventListener('scroll', hide, { passive: true });
    return { hide: hide };
  }

  /* =====================================================================
   * 可折叠区块（大卡折叠 / 长文本收起）
   * ===================================================================== */
  function collapsible(root) {
    root.addEventListener('click', function (e) {
      var btn = e.target.closest ? e.target.closest('[data-fold]') : null;
      if (!btn) return;
      var box = document.querySelector(btn.dataset.fold);
      if (!box) return;
      var open = box.classList.toggle('open');
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      var ico = btn.querySelector('.fold-ico');
      if (ico) ico.textContent = open ? '−' : '+';
    });
  }

  return {
    $: $, $$: $$, esc: esc, qs: qs, isTouch: isTouch,
    canvas: canvas, nodeTip: nodeTip, nodeNav: nodeNav,
    topbar: topbar, lightbox: lightbox, hoverPreview: hoverPreview, collapsible: collapsible
  };
})();
