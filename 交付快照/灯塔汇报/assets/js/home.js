/* =====================================================================
 * home.js —— 主页渲染与交互
 * 三视图：架构总览（总线式架构图）/ 思维导图（可折叠）/ 模块清单（可折叠卡片）
 * ===================================================================== */
(function () {
  'use strict';
  var D = window.DT, G = window.DTG, U = window.DTU;
  var $ = U.$, esc = U.esc;

  var archApi = null, mindApi = null;
  var collapsed = {};                       /* 思维导图折叠状态 */
  var curView = 'arch';

  /* ============================ 横幅 ============================ */
  function renderHero() {
    var el = $('hero'); if (!el) return;
    el.innerHTML =
      '<div class="kick">' + esc(D.SITE.kick) + '</div>' +
      '<h1>灯塔 · 数据中心运维管理平台</h1>' +
      '<p>本页即项目全景：顶部为四端接入与统一调度中枢，中部为六大功能域（' + D.MODULES.length +
      ' 个功能模块，点击任一节点进入详情页：业务流程图＋关键节点＋场景价值＋真实界面效果图），底部为支撑全链路的数据底座与输出。</p>' +
      '<div class="stats">' + D.HERO_STATS.map(function (s) { return '<span>' + esc(s) + '</span>'; }).join('') +
      '<span>' + D.shotCount() + ' 张真实界面截图</span></div>';
  }

  /* ============================ 视图切换 ============================ */
  function setView(v, silent) {
    if (['arch', 'mind', 'list'].indexOf(v) < 0) v = 'arch';
    curView = v;
    U.$$('#viewSeg button').forEach(function (b) {
      var on = b.dataset.view === v;
      b.classList.toggle('on', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    ['arch', 'mind', 'list'].forEach(function (k) {
      var p = $('view-' + k);
      if (p) p.classList.toggle('on', k === v);
    });
    if (!silent) {
      var h = '#/' + v;
      if (location.hash !== h) history.replaceState(null, '', h);
    }
    if (v === 'arch' && archApi) requestAnimationFrame(function () { archApi.fit(); });
    if (v === 'mind' && mindApi) requestAnimationFrame(function () { mindApi.fit(); });
  }

  /* ============================ ① 架构总览 ============================ */
  function renderArch() {
    var host = $('archHost'); if (!host) return;
    var r = G.buildArchSVG(D);
    host.innerHTML = r.svg;
    archApi = U.canvas(host, { w: r.w, h: r.h }, {
      fitMode: 'width',
      onZoom: function (k) { var z = $('archZoom'); if (z) z.textContent = Math.round(k * 100) + '%'; }
    });
    U.nodeTip(host, '.anode', $('nodeTip'));
    host.addEventListener('click', function (e) {
      var n = e.target.closest ? e.target.closest('.anode-click') : null;
      if (!n) return;
      if (n.dataset.act === 'mod') { location.href = 'module.html?id=' + n.dataset.id; return; }
      if (n.dataset.act === 'dom') { setView('list'); setTimeout(function () { focusDomain(n.dataset.key); }, 60); }
    });
    host.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var n = e.target.closest ? e.target.closest('.anode-click') : null;
      if (!n) return;
      e.preventDefault();
      if (n.dataset.act === 'mod') location.href = 'module.html?id=' + n.dataset.id;
      else if (n.dataset.act === 'dom') { setView('list'); setTimeout(function () { focusDomain(n.dataset.key); }, 60); }
    });
    var zi = $('archIn'), zo = $('archOut'), zw = $('archFitW'), za = $('archFitAll');
    if (zi) zi.onclick = function () { archApi.zoomIn(); };
    if (zo) zo.onclick = function () { archApi.zoomOut(); };
    if (zw) zw.onclick = function () { archApi.fitWidth(); };
    if (za) za.onclick = function () { archApi.fitWhole(); };
  }

  /* ============================ ② 思维导图 ============================ */
  function renderMind() {
    var host = $('mindHost'); if (!host) return;
    var r = G.buildMindSVG(D, collapsed);
    host.innerHTML = r.svg;
    mindApi = U.canvas(host, { w: r.w, h: r.h }, {
      fitMode: 'whole',
      onZoom: function (k) { var z = $('mindZoom'); if (z) z.textContent = Math.round(k * 100) + '%'; }
    });
    U.nodeTip(host, '.mnode', $('nodeTip'));
    host.addEventListener('click', function (e) {
      var f = e.target.closest ? e.target.closest('[data-act="fold"]') : null;
      if (f) { collapsed[f.dataset.key] = !collapsed[f.dataset.key]; renderMind(); return; }
      var m = e.target.closest ? e.target.closest('[data-act="mod"]') : null;
      if (m && m.dataset.href) location.href = m.dataset.href;
    });
    host.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var f = e.target.closest ? e.target.closest('[data-act="fold"]') : null;
      if (f) { e.preventDefault(); collapsed[f.dataset.key] = !collapsed[f.dataset.key]; renderMind(); return; }
      var m = e.target.closest ? e.target.closest('[data-act="mod"]') : null;
      if (m && m.dataset.href) { e.preventDefault(); location.href = m.dataset.href; }
    });
    var zi = $('mindIn'), zo = $('mindOut'), zw = $('mindFitW'), za = $('mindAll'), zc = $('mindNone'), zf = $('mindFitAll');
    if (zi) zi.onclick = function () { mindApi.zoomIn(); };
    if (zo) zo.onclick = function () { mindApi.zoomOut(); };
    if (zw) zw.onclick = function () { mindApi.fitWidth(); };
    if (zf) zf.onclick = function () { mindApi.fitWhole(); };
    if (za) za.onclick = function () { collapsed = {}; renderMind(); };
    if (zc) zc.onclick = function () {
      collapsed = {};
      D.SECKEYS.forEach(function (k) { collapsed[k] = true; });
      renderMind();
    };
    var cnt = $('mindCount');
    if (cnt) {
      cnt.textContent = D.SECKEYS.filter(function (k) { return !collapsed[k]; })
        .reduce(function (a, k) { return a + D.modulesOf(k).length; }, 0) + ' / ' + D.MODULES.length + ' 个模块展开';
    }
  }

  /* ============================ ③ 模块清单 ============================ */
  function renderList() {
    var box = $('listBody'); if (!box) return;
    box.innerHTML = '<div class="doms">' + D.SECKEYS.map(function (key) {
      var mods = D.modulesOf(key), meta = D.SECMETA[key], c = D.SECCOLOR[key];
      return '<article class="dom open" id="dom-' + key + '" style="--c:' + c + '">' +
        '<header data-dom="' + key + '" role="button" tabindex="0" aria-expanded="true" aria-controls="domb-' + key + '">' +
        '<span class="bar"></span>' +
        '<div><h3>' + esc(D.SECTITLE[key]) + '</h3>' +
        '<div class="meta">' + mods.length + ' 个功能 · ' + esc(meta.desc) + '</div></div>' +
        '<div class="kpi">' + meta.kpi.map(function (k) { return '<b>' + esc(k) + '</b>'; }).join('') + '</div>' +
        '<span class="fold-ico" aria-hidden="true">−</span>' +
        '</header>' +
        '<div class="body foldable open" id="domb-' + key + '">' +
        '<div class="tiles">' + mods.map(function (m) {
          return '<a class="tile" href="module.html?id=' + m.i + '" style="--c:' + (m.a || c) + '">' +
            '<span class="no">' + String(m.i + 1).padStart(2, '0') + '</span>' +
            '<strong>' + esc(m.name) + '</strong>' +
            '<span class="pill">' + esc(m.pill) + '</span>' +
            '<p>' + esc(m.tag) + '</p>' +
            '<span class="met">' + esc((D.METRICS[m.i] || [])[0] || '') + '</span>' +
            '<span class="go">流程图 · 关键节点 · 场景价值 · 效果图 →</span>' +
            '</a>';
        }).join('') + '</div></div></article>';
    }).join('') + '</div>';

    box.addEventListener('click', function (e) {
      var h = e.target.closest ? e.target.closest('[data-dom]') : null;
      if (h) toggleDom(h.dataset.dom);
    });
    box.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      var h = e.target.closest ? e.target.closest('[data-dom]') : null;
      if (!h) return;
      e.preventDefault(); toggleDom(h.dataset.dom);
    });
    var all = $('listOpen'), none = $('listClose');
    if (all) all.onclick = function () { D.SECKEYS.forEach(function (k) { setDom(k, true); }); };
    if (none) none.onclick = function () { D.SECKEYS.forEach(function (k) { setDom(k, false); }); };
  }
  function setDom(key, open) {
    var art = $('dom-' + key), body = $('domb-' + key);
    if (!art || !body) return;
    art.classList.toggle('open', open);
    body.classList.toggle('open', open);
    var h = art.querySelector('[data-dom]');
    if (h) h.setAttribute('aria-expanded', open ? 'true' : 'false');
    var ico = art.querySelector('.fold-ico');
    if (ico) ico.textContent = open ? '−' : '+';
  }
  function toggleDom(key) { setDom(key, !$('dom-' + key).classList.contains('open')); }
  function focusDomain(key) {
    setDom(key, true);
    var el = $('dom-' + key);
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    el.classList.add('flash');
    setTimeout(function () { el.classList.remove('flash'); }, 1400);
  }

  /* ============================ 数据底座 / 价值带 ============================ */
  function renderFoundation() {
    var el = $('foundation'); if (!el) return;
    var cols = [
      ['飞书协同中枢', '37 个固定多维表：通告、事件、签名、交接、台账等业务真相；群机器人与卡片承担送达与回执核验。', ['37 固定表', '卡片送达', '回执核验']],
      ['本地状态底座', 'SQLite 16 库 139 表：任务、身份、审计、等待队列与冻结归档；离线可用、本地可控。', ['16 库', '139 表', '离线可用']],
      ['知识与文件底座', '本地向量知识库（BGE small zh ＋ FAISS）；Excel、照片与签名归档证据链；敏感内容自动检测。', ['BGE small zh', 'FAISS', '敏感检测']],
      ['外部数据联动', '全景平台测点配置与两小时桶数值、智航屏蔽记录、培训签到接口；变更探针实时同步。', ['≤5 秒探针', '两小时桶', '补传闭环']]
    ];
    el.innerHTML = '<h2>灯塔数据底座 · 协同 × 状态 × 知识 × 外部联动 全链路支撑</h2>' +
      '<p>业务域与底座之间双向流动：向下写入并归档业务真相，向上读取配置与数值；外部系统经变更探针实时同步，避免口径滞后。</p>' +
      '<div class="fcols">' + cols.map(function (c) {
        return '<div class="fcol"><b>' + esc(c[0]) + '</b><p>' + esc(c[1]) + '</p>' +
          '<div class="num">' + c[2].map(function (x) { return '<span>' + esc(x) + '</span>'; }).join('') + '</div></div>';
      }).join('') + '</div>';
  }
  function renderBand() {
    var el = $('band'); if (!el) return;
    el.innerHTML = '<h2>整体价值 · 六维一条主线</h2>' +
      '<p>从一线表务减负，到管理层的态势与合规追问，六类价值共用同一条数据链路。</p>' +
      '<div class="cols">' + D.PILLARS.map(function (p) {
        return '<div class="col"><b>' + esc(p[0]) + '</b><p>' + esc(p[1]) + '</p></div>';
      }).join('') + '</div>';
  }
  function renderFoot() {
    var el = $('foot'); if (!el) return;
    el.textContent = '截图与指标快照时间 ' + D.SITE.snapshot + ' · 线上地址 ' + D.SITE.online +
      ' · 支持全屏演示与一页纸打印摘要 · 架构图可滚轮缩放、拖动平移';
  }

  /* ============================ 一页纸打印摘要 ============================ */
  function renderPrint() {
    var el = $('printOne'); if (!el) return;
    var mets = D.PRINT_METRICS.map(function (x) {
      return '<p><b>' + esc(D.MODULES[x[0]].name) + '：</b>' + esc(x[1]) + '</p>';
    }).join('');
    var secs = D.SECKEYS.map(function (k) {
      return '<p><b>' + esc(D.SECTITLE[k]) + '（' + D.modulesOf(k).length + '）：</b>' +
        D.modulesOf(k).map(function (m) { return esc(m.name); }).join('、') + '</p>';
    }).join('');
    el.innerHTML =
      '<h1>灯塔 · 数据中心运维管理平台 — 功能价值汇报（摘要）</h1>' +
      '<p class="lead">' + esc(D.SITE.summary) + '</p>' +
      '<h2>一、整体价值（六维）</h2><div class="cols2">' +
      D.PILLARS.map(function (p) { return '<p><b>' + esc(p[0]) + '：</b>' + esc(p[1]) + '</p>'; }).join('') +
      '</div>' +
      '<h2>二、六大功能域与 ' + D.MODULES.length + ' 个模块</h2>' + secs +
      '<h2>三、关键数据快照（界面指标）</h2><div class="cols2">' + mets + '</div>' +
      '<h2>四、平台架构与数据流向</h2><div class="archbox">' + G.buildPrintArchSVG(D) + '</div>' +
      '<p class="pfoot">快照时间 ' + D.SITE.snapshot + ' · 线上地址 ' + D.SITE.online +
      ' · 屏幕版支持三视图交互：架构总览 / 思维导图 / 模块清单，模块详情页含流程图、关键节点、场景价值与真实界面效果图</p>';
  }

  /* ============================ 启动 ============================ */
  function boot() {
    renderHero();
    renderArch();
    renderMind();
    renderList();
    renderFoundation();
    renderBand();
    renderFoot();
    renderPrint();
    U.topbar();

    var seg = $('viewSeg');
    if (seg) seg.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-view]');
      if (b) setView(b.dataset.view);
    });

    /* 深链：#/arch  #/mind  #/list  #/list:s3 */
    var h = location.hash || '';
    var m = h.match(/^#\/?(arch|mind|list)(?::(s\d))?$/);
    if (m) {
      setView(m[1], true);
      if (m[2]) setTimeout(function () { focusDomain(m[2]); }, 80);
    } else {
      setView('arch', true);
    }
    window.addEventListener('hashchange', function () {
      var mm = (location.hash || '').match(/^#\/?(arch|mind|list)(?::(s\d))?$/);
      if (mm) {
        setView(mm[1], true);
        if (mm[2]) focusDomain(mm[2]);
      }
    });

    /* 键盘：1/2/3 切视图 */
    document.addEventListener('keydown', function (e) {
      var a = document.activeElement;
      if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA')) return;
      if (e.key === '1') setView('arch');
      if (e.key === '2') setView('mind');
      if (e.key === '3') setView('list');
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
