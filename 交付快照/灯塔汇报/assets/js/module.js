/* =====================================================================
 * module.js —— 模块详情子页面
 * 内容：模块导航条 / 头部与指标 / 业务流程图 / 关键节点拆解 /
 *       业务场景与价值 / 真实界面效果图（灯箱＋悬浮预览）/ 关联模块 / 上下页
 * 路由：module.html?id=N（兼容 #/mN 旧深链）
 * ===================================================================== */
(function () {
  'use strict';
  var D = window.DT, G = window.DTG, U = window.DTU;
  var $ = U.$, esc = U.esc;

  /* 有截图的模块清单（灯箱按其顺序翻页） */
  var SHOTS = [];
  D.MODULES.forEach(function (m) { if (m.img) SHOTS.push({ i: m.i, name: m.name, img: m.img, sec: m.s }); });
  var shotPos = {};
  SHOTS.forEach(function (s, k) { shotPos[s.i] = k; });

  function currentId() {
    var v = U.qs('id');
    if (v === '') {
      var m = (location.hash || '').match(/^#\/?m(\d+)$/);
      if (m) v = m[1];
    }
    var n = parseInt(v, 10);
    return (!isNaN(n) && n >= 0 && n < D.MODULES.length) ? n : 0;
  }

  /* ============================ 模块导航条 ============================ */
  function renderStrip(id) {
    var strip = $('modStrip'); if (!strip) return;
    strip.innerHTML = D.MODULES.map(function (m) {
      return '<a href="module.html?id=' + m.i + '" class="' + (m.i === id ? 'on' : '') + '"' +
        ' style="--c:' + D.SECCOLOR[m.s] + ';--dc:' + (m.a || D.SECCOLOR[m.s]) + '"' +
        ' title="' + esc(D.SECTITLE[m.s] + ' · ' + m.tag) + '">' +
        '<i></i>' + esc(m.name) + '</a>';
    }).join('');
    var on = strip.querySelector('a.on');
    if (on && on.scrollIntoView) on.scrollIntoView({ inline: 'center', block: 'nearest' });
  }

  /* ============================ 效果图 ============================ */
  function shotsHTML(m, id) {
    if (!m.img) {
      return '<div class="shots"><div class="shotcell"><div class="thumb">' +
        '<span class="noimg">' + esc(m.name) + ' · 后台 / 桌面端能力，无独立网页界面<br>其运行结果体现在关联模块的界面与飞书送达中</span>' +
        '</div><small><i style="background:#8aa0b8"></i>说明</small></div></div>';
    }
    function cell(ver, label, color) {
      if (!m.img[ver]) return '';
      return '<div class="shotcell"><div class="thumb" data-si="' + shotPos[id] + '" data-ver="' + ver + '"' +
        ' role="button" tabindex="0" aria-label="放大查看 ' + esc(m.name + ' ' + label) + ' 界面效果图">' +
        '<img src="' + esc(m.img[ver]) + '" alt="' + esc(m.name + ' ' + label) + '界面截图" loading="lazy">' +
        '<span class="verchip">' +
        (m.img.v2 ? '<button type="button" class="' + (ver === 'v2' ? 'on' : '') + '" data-v="v2" data-i="' + id + '">新版</button>' : '') +
        (m.img.v1 ? '<button type="button" class="' + (ver === 'v1' ? 'on' : '') + '" data-v="v1" data-i="' + id + '">旧版</button>' : '') +
        '</span></div><small><i style="background:' + color + '"></i>' + label + '界面</small></div>';
    }
    return '<div class="shots">' + cell('v2', '新版', '#087a61') + cell('v1', '旧版', '#64748b') + '</div>';
  }

  /* ============================ 关键节点 ============================ */
  function keysHTML(f) {
    return '<div class="keys">' + D.KEYNODE_LABELS.map(function (k) {
      var key = k[0], v = f[key];
      if (key === 'br') {
        return '<div class="key branch"><b>' + esc(k[1]) + '</b><div class="v">' +
          v.map(function (x) { return '<span>' + esc(x) + '</span>'; }).join('') + '</div>' +
          '<small>' + esc(k[2]) + '</small></div>';
      }
      return '<div class="key"><b>' + esc(k[1]) + '</b><div class="v">' + esc(v) + '</div>' +
        '<small>' + esc(k[2]) + '</small></div>';
    }).join('') + '</div>';
  }

  /* ============================ 主体渲染 ============================ */
  function render(id) {
    var m = D.MODULES[id], f = D.FLOWS[id], mc = D.SECCOLOR[m.s];
    var pv = D.MODULES[(id - 1 + D.MODULES.length) % D.MODULES.length];
    var nx = D.MODULES[(id + 1) % D.MODULES.length];
    var rel = (D.REL[id] || []);
    document.documentElement.style.setProperty('--pagec', mc);
    document.title = m.name + ' · ' + D.SECTITLE[m.s] + ' — 灯塔功能价值汇报';

    var main = $('modMain');
    main.style.setProperty('--mc', mc);
    main.innerHTML =
      '<div class="mod-bar">' +
      '<a class="btn" href="index.html">← 返回架构总览</a>' +
      '<a class="btn" href="index.html#/list:' + m.s + '">所属功能域清单</a>' +
      '<button type="button" class="btn" id="copyLink">复制本页链接</button>' +
      '<span class="cnt">模块 ' + String(id + 1).padStart(2, '0') + ' / ' + D.MODULES.length + '</span>' +
      '</div>' +

      '<div class="crumb"><a href="index.html">架构总览</a><s>/</s>' +
      '<a href="index.html#/list:' + m.s + '">' + esc(D.SECTITLE[m.s]) + '</a><s>/</s>' +
      '<span>' + esc(m.name) + '</span></div>' +

      '<header class="mhead"><div class="kick">' + esc(D.SECTITLE[m.s]) + '</div>' +
      '<h1>' + esc(m.name) + '<span class="pill">' + esc(m.pill) + '</span></h1>' +
      '<p>' + esc(m.tag) + '</p></header>' +

      '<div class="stats-grid">' + (D.METRICS[id] || []).map(function (x) {
        return '<div class="stat"><b>' + esc(x) + '</b></div>';
      }).join('') + '</div>' +

      '<h3 class="h2"><i></i>功能流程图<small>业务逻辑：从一线发起到闭环治理</small><span class="ln"></span></h3>' +
      '<div class="flowbox">' + G.buildFlowSVG(f, m) + '</div>' +
      '<p class="flow-hint no-print">流程图较宽，可横向滚动查看；打印 / 导出 PDF 时自动缩放为页宽。</p>' +

      '<h3 class="h2"><i></i>关键节点拆解<small>' + D.KEYNODE_LABELS.length + ' 个节点：角色 · 触发 · 分流 · 管控 · 数据落点 · 追溯</small><span class="ln"></span></h3>' +
      keysHTML(f) +

      '<h3 class="h2"><i></i>业务场景与价值<span class="ln"></span></h3>' +
      '<div class="scene"><div class="quote"><b>场景回放</b>' + esc(D.SCENES[id]) + '</div>' +
      '<div class="cols">' +
      '<div class="step p1"><b>因为 · 痛点 / 需求</b>' + esc(m.fix) + '</div>' +
      '<div class="step p2"><b>所以 · 设计实现</b>' + esc(m.use) + '</div>' +
      '<div class="step p3"><b>带来 · 业务价值</b>' + esc(m.val) + '</div>' +
      '</div></div>' +

      '<h3 class="h2"><i></i>界面效果图<small>点击放大 · 悬浮预览 · 芯片切换新旧版</small><span class="ln"></span></h3>' +
      shotsHTML(m, id) +

      (rel.length ? '<h3 class="h2"><i></i>关联模块<small>业务上下游与数据互通</small><span class="ln"></span></h3>' +
        '<div class="rel no-print">' + rel.map(function (j) {
          return '<a href="module.html?id=' + j + '" style="--dc:' + (D.MODULES[j].a || D.SECCOLOR[D.MODULES[j].s]) + '">' +
            '<i></i>' + esc(D.MODULES[j].name) + '</a>';
        }).join('') + '</div>' : '') +

      '<div class="pn no-print">' +
      '<a href="module.html?id=' + pv.i + '"><small>← 上一个模块</small>' + esc(pv.name) + '</a>' +
      '<a href="module.html?id=' + nx.i + '"><small>下一个模块 →</small>' + esc(nx.name) + '</a>' +
      '</div>' +

      '<p class="tip no-print">提示：键盘 ← / → 切换模块，Esc 返回架构总览；效果图点击放大，放大后滚轮缩放、双击还原。</p>' +

      '<div class="print-only" id="printFoot">灯塔 · 数据中心运维管理平台 — 模块汇报：' + esc(m.name) +
      '（' + esc(D.SECTITLE[m.s]) + '）　|　快照时间 ' + D.SITE.snapshot + '　|　线上地址 ' + D.SITE.online + '</div>';

    /* 缩略图交互 */
    var lb = U.lightbox(function (k) { return SHOTS[k]; }, SHOTS.length, function (k) {
      return SHOTS[k] ? 'module.html?id=' + SHOTS[k].i : null;
    });
    U.hoverPreview();

    main.addEventListener('click', function (e) {
      var chip = e.target.closest ? e.target.closest('.verchip button') : null;
      if (chip) {
        e.stopPropagation(); e.preventDefault();
        var t = chip.closest('.thumb'), mm = D.MODULES[+chip.dataset.i];
        if (!t || !mm || !mm.img) return;
        var img = t.querySelector('img');
        if (img) img.src = mm.img[chip.dataset.v];
        t.dataset.ver = chip.dataset.v;
        U.$$('.verchip button', t).forEach(function (b) { b.classList.toggle('on', b.dataset.v === chip.dataset.v); });
        return;
      }
      var th = e.target.closest ? e.target.closest('.thumb[data-si]') : null;
      if (th && lb) { lb.open(+th.dataset.si, th.dataset.ver || 'v2'); return; }
      if (e.target.closest && e.target.closest('#copyLink')) {
        var b = e.target.closest('#copyLink');
        var done = function () { var t2 = b.textContent; b.textContent = '已复制 ✓'; setTimeout(function () { b.textContent = t2; }, 1200); };
        if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(location.href).then(done, done);
        else done();
      }
    });
    main.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var th = e.target.closest ? e.target.closest('.thumb[data-si]') : null;
      if (th && lb) { e.preventDefault(); lb.open(+th.dataset.si, th.dataset.ver || 'v2'); }
    });

    document.addEventListener('keydown', function (e) {
      var a = document.activeElement;
      if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA')) return;
      if (lb && lb.isOpen()) return;
      if (e.key === 'Escape') { location.href = 'index.html'; return; }
      if (e.key === 'ArrowLeft') location.href = 'module.html?id=' + pv.i;
      if (e.key === 'ArrowRight') location.href = 'module.html?id=' + nx.i;
    });
  }

  function boot() {
    var id = currentId();
    renderStrip(id);
    render(id);
    U.topbar();
    U.nodeTip(document, '.mnode', $('nodeTip'));
    /* 空闲预加载全部效果图 */
    var preload = function () {
      D.MODULES.forEach(function (m) {
        if (!m.img) return;
        [m.img.v2, m.img.v1].forEach(function (s) { if (s) { var im = new Image(); im.src = s; } });
      });
    };
    if ('requestIdleCallback' in window) requestIdleCallback(preload, { timeout: 4000 });
    else setTimeout(preload, 1500);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
