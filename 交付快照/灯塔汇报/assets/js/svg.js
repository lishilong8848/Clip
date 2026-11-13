/* =====================================================================
 * 灯塔汇报站 —— SVG 图形生成层
 * 三张图：① 总线式平台架构图 ② 可折叠思维导图 ③ 单模块业务流程图
 * 全部为纯函数，输入数据 → 输出 SVG 字符串，不触碰 DOM。
 * ===================================================================== */
window.DTG = (function () {
  'use strict';

  var FONT = 'Microsoft YaHei,PingFang SC,sans-serif';

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }
  /* 按字符数折行（中日韩等宽近似） */
  function wrap(text, max) {
    var out = [], line = '';
    String(text || '').split('').forEach(function (ch) {
      if ((line + ch).length > max) { out.push(line); line = ch; } else { line += ch; }
    });
    if (line) out.push(line);
    return out;
  }
  function cut(text, max) {
    var t = String(text || '');
    return t.length > max ? t.slice(0, max - 1) + '…' : t;
  }
  function tx(x, y, t, size, fill, weight, anchor) {
    return '<text x="' + x + '" y="' + y + '" font-size="' + size + '" fill="' + fill +
      '" text-anchor="' + (anchor || 'middle') + '" font-weight="' + (weight || 400) +
      '" font-family="' + FONT + '">' + esc(t) + '</text>';
  }

  /* =====================================================================
   * ① 平台架构图：接入层 → 中枢层 →（调度总线）→ 六大功能域 ↔ 数据底座 → 输出层
   *    总线式收敛：中枢左侧引出纵向总线，横向支线逐一接入每个功能域
   *    每个节点都可点击（data-act="mod"），并携带 data-name / data-desc 供悬浮说明
   * ===================================================================== */
  function buildArchSVG(D) {
    var A = D.ARCH;
    var W = 1560;
    var BX = 44, BR = 1552, BW = BR - BX;          /* 层带左边界 / 右边界 / 宽度 */
    var LABX = BX + 16, LABW = 196;                 /* 左侧标签列 */
    var CX = BX + 208, CR = BR - 16;                /* 内容区 */
    var CW = CR - CX;
    var TRUNK = 22;                                 /* 纵向总线 x */

    var out = [];
    var y = 0;

    /* ---------- 通用：层带外框 + 左侧标签 ---------- */
    function band(y0, h, color, tint, label, note, extra) {
      var s = '<rect x="' + BX + '" y="' + y0 + '" width="' + BW + '" height="' + h +
        '" rx="14" fill="' + tint + '" stroke="' + color + '" stroke-opacity=".3"/>';
      s += '<rect x="' + BX + '" y="' + (y0 + 10) + '" width="5" height="' + (h - 20) +
        '" rx="2.5" fill="' + color + '"/>';
      s += tx(LABX, y0 + 27, label, 14.5, color, 700, 'start');
      if (note) {
        wrap(note, 15).slice(0, 3).forEach(function (ln, k) {
          s += tx(LABX, y0 + 47 + k * 15, ln, 10, '#7d8ea3', 400, 'start');
        });
      }
      if (extra) s += extra;
      return s;
    }

    /* ---------- 通用：可点击节点框 ---------- */
    function node(x, y0, w, h, color, title, desc, mid, fill, titleSize) {
      var click = mid != null;
      var s = '<g class="anode' + (click ? ' anode-click' : '') + '"' +
        (click ? ' data-act="mod" data-id="' + mid + '"' : '') +
        ' data-name="' + esc(title) + '" data-desc="' + esc(desc || '') + '"' +
        (click ? ' data-href="module.html?id=' + mid + '"' : '') +
        ' role="' + (click ? 'link' : 'img') + '" tabindex="' + (click ? '0' : '-1') + '"' +
        ' aria-label="' + esc(title + (desc ? '：' + desc : '')) + '">';
      s += '<rect x="' + x + '" y="' + y0 + '" width="' + w + '" height="' + h + '" rx="10" fill="' +
        (fill || '#ffffff') + '" stroke="' + color + '" stroke-opacity=".45" stroke-width="1.3"/>';
      s += tx(x + w / 2, y0 + (desc ? 22 : h / 2 + 4.5), title, titleSize || 12.5, color, 600);
      if (desc) {
        wrap(desc, Math.max(10, Math.floor((w - 20) / 10))).slice(0, 3).forEach(function (ln, k) {
          s += tx(x + w / 2, y0 + 39 + k * 14, ln, 10, '#7d8ea3');
        });
      }
      if (click) {
        s += '<circle cx="' + (x + w - 11) + '" cy="' + (y0 + 11) + '" r="3.2" fill="' + color + '" opacity=".55"/>';
      }
      return s + '</g>';
    }

    /* ============ 接入层 ============ */
    var hAccess = 90;
    var aN = A.access.nodes.length;
    var aW = (CW - (aN - 1) * 12) / aN;
    var s = band(y, hAccess, A.access.color, '#f4f8ff', A.access.label, A.access.note);
    A.access.nodes.forEach(function (n, i) {
      s += node(CX + i * (aW + 12), y + 14, aW, 62, A.access.color, n.t, n.d, n.mid, '#ffffff');
    });
    out.push(s);
    var accessBottom = y + hAccess;
    y = accessBottom + 36;

    /* ============ 中枢层 ============ */
    var hHub = 116;
    var hubMidY = y + hHub / 2;
    s = band(y, hHub, A.hub.color, '#f1f8f5', A.hub.label, '统一编排 · 治理 · 门禁');
    s += tx(CX, y + 28, A.hub.title, 18, '#0b3f36', 700, 'start');
    s += tx(CX, y + 48, A.hub.sub, 11, '#7d8ea3', 400, 'start');
    var hN = A.hub.nodes.length;
    var hW = (CW - (hN - 1) * 12) / hN;
    A.hub.nodes.forEach(function (n, i) {
      s += node(CX + i * (hW + 12), y + 60, hW, 46, '#0e7490', n.t, '', n.mid, '#ffffff', 12);
      s += tx(CX + i * (hW + 12) + hW / 2, y + 100, cut(n.d, 22), 9.2, '#8aa0b8');
    });
    out.push(s);
    var hubBottom = y + hHub;

    /* ---------- 接入层 → 中枢层：四路收敛箭头 ---------- */
    var conv = '<g class="aflow">';
    A.access.nodes.forEach(function (n, i) {
      var sx = CX + i * (aW + 12) + aW / 2, sy = accessBottom;
      var ex = CX + i * (hW + 12) + hW / 2 > CR ? CR : CX + (CW / aN) * (i + 0.5);
      ex = CX + (CW / aN) * (i + 0.5);
      conv += '<path d="M' + sx + ',' + sy + ' L' + sx + ',' + (sy + 14) + ' L' + ex + ',' + (sy + 22) +
        ' L' + ex + ',' + (hubBottom - 116 - 4) + '" fill="none" stroke="#9fb6c9" stroke-width="1.8" marker-end="url(#arw-a)"/>';
    });
    conv += '</g>';

    /* ============ 功能域（六大域 · 调度总线接入） ============ */
    y = hubBottom + 38;
    var domTops = [];
    var CHIPW = 210, CHIPH = 42, CHIPGAP = 10, KPIW = 300;
    var KPIX = CR - KPIW;
    var CHIPAREA = KPIX - 18 - CX;

    D.SECKEYS.forEach(function (key, di) {
      var mods = D.modulesOf(key);
      var meta = D.SECMETA[key], color = D.SECCOLOR[key];
      var rows = mods.length > 4 ? 2 : 1;
      var perRow = Math.ceil(mods.length / rows);
      var h = rows === 1 ? 92 : 134;
      domTops.push({ y: y, h: h, mid: y + h / 2 });

      var b = band(y, h, color, '#fcfdfe', '', '');
      /* 左侧域标签（点击跳到「模块清单」对应分组） */
      var lbase = rows === 1 ? y + 22 : y + 42;
      b += '<rect x="' + BX + '" y="' + (y + 10) + '" width="5" height="' + (h - 20) + '" rx="2.5" fill="' + color + '"/>';
      b += '<g class="anode anode-click anode-dom" data-act="dom" data-key="' + key + '" data-href="#list:' + key + '"' +
        ' data-name="' + esc(D.SECTITLE[key]) + '" data-desc="' + esc(meta.desc) + '" data-pill="' + mods.length + ' 个功能"' +
        ' role="link" tabindex="0" aria-label="' + esc(D.SECTITLE[key] + '，' + mods.length + ' 个功能，点击查看模块清单') + '">' +
        '<rect x="' + (BX + 8) + '" y="' + (y + 6) + '" width="' + LABW + '" height="' + (h - 12) + '" rx="8" fill="transparent"/>' +
        tx(LABX, lbase, D.SECTITLE[key], 14, color, 700, 'start') +
        wrap(meta.desc, 15).slice(0, 2).map(function (ln, k) {
          return tx(LABX, lbase + 20 + k * 14, ln, 10, '#93a3b6', 400, 'start');
        }).join('') +
        tx(LABX, lbase + 56, mods.length + ' 个功能 · 查看清单 →', 10, color, 600, 'start') +
        '</g>';
      /* 模块芯片 */
      var chipW = Math.min(CHIPW, (CHIPAREA - (perRow - 1) * CHIPGAP) / perRow);
      mods.forEach(function (m, k) {
        var r = rows === 1 ? 0 : Math.floor(k / perRow);
        var c = rows === 1 ? k : k % perRow;
        var cnt = rows === 1 ? mods.length : (r === 0 ? perRow : mods.length - perRow);
        var cw = Math.min(CHIPW, (CHIPAREA - (cnt - 1) * CHIPGAP) / cnt);
        var cx = CX + c * (cw + CHIPGAP);
        var cy = rows === 1 ? y + 25 : y + 20 + r * (CHIPH + 10);
        b += '<g class="anode anode-click" data-act="mod" data-id="' + m.i + '" data-href="module.html?id=' + m.i + '"' +
          ' data-name="' + esc(m.name) + '" data-desc="' + esc(m.tag) + '" data-pill="' + esc(m.pill) + '"' +
          ' role="link" tabindex="0" aria-label="' + esc(m.name + '：' + m.tag) + '">' +
          '<rect x="' + cx + '" y="' + cy + '" width="' + cw + '" height="' + CHIPH + '" rx="9" fill="#fff" stroke="' +
          color + '" stroke-opacity=".42" stroke-width="1.3"/>' +
          '<rect x="' + cx + '" y="' + cy + '" width="3.5" height="' + CHIPH + '" rx="1.75" fill="' + (m.a || color) + '"/>' +
          tx(cx + cw / 2 + 1, cy + 18, cut(m.name, 13), 12, '#243a52', 600) +
          tx(cx + cw / 2 + 1, cy + 33, m.pill, 9.4, color, 500) +
          '<circle cx="' + (cx + cw - 10) + '" cy="' + (cy + 10) + '" r="3" fill="' + color + '" opacity=".5"/>' +
          '</g>';
      });
      /* 右侧关键数据面板 */
      var ph = rows === 1 ? 76 : 100;
      var py = y + (h - ph) / 2;
      b += '<rect x="' + KPIX + '" y="' + py + '" width="' + KPIW + '" height="' + ph + '" rx="10" fill="' + color + '" fill-opacity=".055" stroke="' + color + '" stroke-opacity=".2"/>';
      b += tx(KPIX + 14, py + 20, '关键数据快照', 10, color, 700, 'start');
      meta.kpi.forEach(function (k2, k) {
        b += tx(KPIX + 14, py + 40 + k * 17, '· ' + k2, 10.6, '#4d6076', 400, 'start');
      });
      out.push(b);
      y += h + 12;
    });
    var domBottom = y - 12;

    /* ---------- 调度总线：中枢左侧引出纵向干线，横向支线接入每个功能域 ---------- */
    var bus = '<g class="abus">';
    bus += '<path d="M' + BX + ',' + hubMidY + ' H' + TRUNK + ' V' + domTops[domTops.length - 1].mid +
      '" fill="none" stroke="#087a61" stroke-width="2.6" stroke-opacity=".55"/>';
    domTops.forEach(function (d) {
      bus += '<circle cx="' + TRUNK + '" cy="' + d.mid + '" r="3.6" fill="#087a61"/>';
      bus += '<path d="M' + TRUNK + ',' + d.mid + ' H' + (BX - 4) + '" fill="none" stroke="#087a61" stroke-width="2" stroke-opacity=".55" marker-end="url(#arw-b)"/>';
    });
    bus += '<text x="' + (TRUNK - 9) + '" y="' + ((domTops[0].mid + domTops[domTops.length - 1].mid) / 2) +
      '" font-size="10.5" fill="#0e7490" font-weight="600" text-anchor="middle" font-family="' + FONT +
      '" transform="rotate(-90 ' + (TRUNK - 9) + ' ' + ((domTops[0].mid + domTops[domTops.length - 1].mid) / 2) +
      ')">统一调度总线</text>';
    bus += '</g>';

    /* ============ 数据底座 ============ */
    y = domBottom + 54;
    var hBase = 112;
    var baseTop = y;
    s = band(y, hBase, A.base.color, '#fffbf3', A.base.label, A.base.note);
    var bN = A.base.nodes.length;
    var bW = (CW - (bN - 1) * 12) / bN;
    A.base.nodes.forEach(function (n, i) {
      s += node(CX + i * (bW + 12), y + 16, bW, 80, '#96691a', n.t, n.d, n.mid, '#ffffff', 12);
    });
    out.push(s);

    /* ---------- 功能域 ↔ 数据底座：写入 / 读取 双向 ---------- */
    var midGap = baseTop - domBottom;
    var two = '<g class="aflow">';
    two += '<path d="M' + (W / 2 - 130) + ',' + domBottom + ' L' + (W / 2 - 130) + ',' + baseTop +
      '" fill="none" stroke="#b7791f" stroke-width="2.2" marker-end="url(#arw-w)"/>';
    two += tx(W / 2 - 122, domBottom + midGap / 2 + 4, '写入 / 归档', 10.5, '#b7791f', 600, 'start');
    two += '<path d="M' + (W / 2 + 130) + ',' + baseTop + ' L' + (W / 2 + 130) + ',' + domBottom +
      '" fill="none" stroke="#0e7490" stroke-width="2.2" marker-end="url(#arw-r)"/>';
    two += tx(W / 2 + 138, domBottom + midGap / 2 + 4, '读取 / 探针', 10.5, '#0e7490', 600, 'start');
    two += '</g>';

    /* ============ 输出层 ============ */
    y = baseTop + hBase + 38;
    var hOut = 90;
    s = band(y, hOut, A.output.color, '#f2faf6', A.output.label, A.output.note);
    var oN = A.output.nodes.length;
    var oW = (CW - (oN - 1) * 12) / oN;
    A.output.nodes.forEach(function (n, i) {
      s += node(CX + i * (oW + 12), y + 14, oW, 62, A.output.color, n.t, n.d, n.mid, '#ffffff', 12);
    });
    out.push(s);
    var outTop = y;
    var baseBottom = baseTop + hBase;
    var tail = '<g class="aflow"><path d="M' + (W / 2) + ',' + baseBottom + ' L' + (W / 2) + ',' + outTop +
      '" fill="none" stroke="#8aa0b8" stroke-width="2.2" marker-end="url(#arw-a)"/>' +
      tx(W / 2 + 12, baseBottom + 19, '归档取出 · 汇总送达', 10.5, '#5b6b80', 500, 'start') + '</g>';

    var H = y + hOut + 12;
    var defs = '<defs>' +
      [['arw-a', '#8aa0b8'], ['arw-b', '#087a61'], ['arw-w', '#b7791f'], ['arw-r', '#0e7490']].map(function (p) {
        return '<marker id="' + p[0] + '" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="' + p[1] + '"/></marker>';
      }).join('') + '</defs>';

    return {
      w: W, h: H,
      svg: '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="灯塔平台整体架构：接入层、中枢层、六大功能域、数据底座与输出层">' +
        defs + '<g class="avp">' + conv + out.join('') + bus + two + tail + '</g></svg>'
    };
  }

  /* =====================================================================
   * ② 思维导图：根 → 六大功能域 → 25 个模块
   *    模块按每行 4 个的网格排布，整图横向展开、高度可控，便于整幅浏览
   *    功能域节点可折叠 / 展开；模块叶片可点击进入详情
   * ===================================================================== */
  function buildMindSVG(D, collapsed) {
    var RX = 20, RW = 152, RH = 86;
    var DX = 246, DW = 202;
    var MX = 508, MW = 250, MH = 40, GX = 10, GY = 9, PERROW = 4;
    var GRIDW = PERROW * MW + (PERROW - 1) * GX;
    var DGAP = 26;
    var links = [], nodes = [];
    var y = 20, domCenters = [];

    D.SECKEYS.forEach(function (key) {
      var mods = D.modulesOf(key), color = D.SECCOLOR[key];
      var folded = !!collapsed[key];
      var rows = Math.ceil(mods.length / PERROW);
      var gridH = rows * (MH + GY) - GY;
      var blockH = folded ? 52 : Math.max(52, gridH);
      var dcy = y + blockH / 2;
      domCenters.push(dcy);

      /* 域节点（高度随子节点块自适应，形成真正的分支） */
      nodes.push('<g class="mnode mnode-dom" data-act="fold" data-key="' + key + '" role="button" tabindex="0" aria-expanded="' + (!folded) + '" aria-label="' + esc(D.SECTITLE[key] + '，' + mods.length + ' 个功能，点击折叠或展开') + '">' +
        '<rect x="' + DX + '" y="' + y + '" width="' + DW + '" height="' + blockH + '" rx="11" fill="' + color + '" fill-opacity="' + (folded ? '.15' : '.08') + '" stroke="' + color + '" stroke-opacity=".5" stroke-width="1.5"/>' +
        tx(DX + 15, dcy - (folded ? 4 : 6), D.SECTITLE[key], 13, color, 700, 'start') +
        tx(DX + 15, dcy + 13, (folded ? '已折叠 · ' : '') + mods.length + ' 个功能', 9.8, '#7d8ea3', 400, 'start') +
        '<circle cx="' + (DX + DW - 17) + '" cy="' + (y + 17) + '" r="8.5" fill="' + color + '" fill-opacity=".16"/>' +
        tx(DX + DW - 17, y + 20.6, folded ? '+' : '−', 12, color, 700) +
        '</g>');

      if (!folded) {
        mods.forEach(function (m, k) {
          var r = Math.floor(k / PERROW), c = k % PERROW;
          var cnt = (r === rows - 1) ? (mods.length - (rows - 1) * PERROW) : PERROW;
          var mx = MX + c * (MW + GX);
          var my = y + (blockH - gridH) / 2 + r * (MH + GY);
          var mcy = my + MH / 2;
          links.push(link(DX + DW, dcy, mx, mcy, color, .5));
          nodes.push('<g class="mnode mnode-mod" data-act="mod" data-id="' + m.i + '" data-href="module.html?id=' + m.i + '"' +
            ' data-name="' + esc(m.name) + '" data-desc="' + esc(m.tag) + '" role="link" tabindex="0" aria-label="' + esc(m.name + '：' + m.tag) + '">' +
            '<rect x="' + mx + '" y="' + my + '" width="' + MW + '" height="' + MH + '" rx="9" fill="#fff" stroke="' + (m.a || color) + '" stroke-opacity=".42" stroke-width="1.2"/>' +
            '<rect x="' + mx + '" y="' + my + '" width="3.5" height="' + MH + '" rx="1.75" fill="' + (m.a || color) + '"/>' +
            tx(mx + 13, my + 16.5, cut(m.name, 14), 11.6, '#243a52', 600, 'start') +
            tx(mx + 13, my + 31, cut(m.tag, 26), 9.3, '#8aa0b8', 400, 'start') +
            '</g>');
        });
      }
      y += blockH + DGAP;
    });

    var H = y - DGAP + 20;
    var rootCy = H / 2;
    var root = '<g class="mnode mnode-root">' +
      '<rect x="' + RX + '" y="' + (rootCy - RH / 2) + '" width="' + RW + '" height="' + RH + '" rx="13" fill="#063a30"/>' +
      tx(RX + RW / 2, rootCy - 14, '灯塔', 19, '#fff', 700) +
      tx(RX + RW / 2, rootCy + 6, '数据中心运维管理平台', 9.6, 'rgba(255,255,255,.8)') +
      tx(RX + RW / 2, rootCy + 24, D.MODULES.length + ' 模块 · 6 功能域', 9.3, 'rgba(255,255,255,.6)') +
      '</g>';
    var rootLinks = domCenters.map(function (cy, i) {
      return link(RX + RW, rootCy, DX, cy, D.SECCOLOR[D.SECKEYS[i]], .45);
    }).join('');

    var W = MX + GRIDW + 20;
    return {
      w: W, h: H,
      svg: '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="灯塔平台思维导图：六大功能域与 25 个模块">' +
        '<g class="avp">' + rootLinks + links.join('') + root + nodes.join('') + '</g></svg>'
    };
  }
  function link(x1, y1, x2, y2, color, op) {
    var mx = (x1 + x2) / 2;
    return '<path d="M' + x1 + ',' + y1 + ' C' + mx + ',' + y1 + ' ' + mx + ',' + y2 + ' ' + x2 + ',' + y2 +
      '" fill="none" stroke="' + color + '" stroke-opacity="' + (op || .5) + '" stroke-width="1.6"/>';
  }

  /* =====================================================================
   * ③ 单模块业务流程图（沿用原版三段式：一线发起 → 过程管控 → 数据与治理）
   * ===================================================================== */
  function buildFlowSVG(f, m) {
    var W = 1560, H = 560, T1 = '#5b7ea6', TB = '#16436e', TO = '#e0a458', TP = '#9b7fd4', TG = '#2f9e6f';
    var mk = function (c) { return c === TP ? 'p' : c === TG ? 'g' : c === TO ? 'o' : 'a'; };
    var ar = function (d, c, dash) {
      return '<path d="' + d + '" fill="none" stroke="' + c + '" stroke-width="1.8"' +
        (dash ? ' stroke-dasharray="6 4"' : '') + ' marker-end="url(#mf-' + mk(c) + ')"/>';
    };
    var s = '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="' + esc(m.name) + ' 业务流程图">' +
      '<defs>' + [['a', T1], ['o', TO], ['p', TP], ['g', TG]].map(function (p) {
        return '<marker id="mf-' + p[0] + '" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="' + p[1] + '"/></marker>';
      }).join('') + '</defs>' +
      '<rect width="' + W + '" height="' + H + '" rx="12" fill="#f8fbff"/>' +
      '<rect x="20" y="8" width="1520" height="58" rx="10" fill="' + TB + '"/>' +
      tx(W / 2, 32, m.name + '｜业务流程：从一线发起到闭环治理', 16.5, '#fff', 700) +
      tx(W / 2, 52, m.tag, 11, 'rgba(255,255,255,.85)') +
      [[30, '01 一线发起'], [450, '02 过程管控'], [1080, '03 数据与治理']].map(function (p) {
        return '<rect x="' + p[0] + '" y="80" width="150" height="30" rx="6" fill="' + TB + '"/>' + tx(p[0] + 75, 100, p[1], 12.5, '#fff', 600);
      }).join('') +
      '<rect x="20" y="120" width="410" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>' +
      '<rect x="440" y="120" width="620" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>' +
      '<rect x="1070" y="120" width="470" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>' +
      '<ellipse cx="120" cy="265" rx="78" ry="38" fill="#fff" stroke="' + T1 + '" stroke-width="1.6"/>' + tx(120, 269, f.r, 12.5, '#243447', 600) +
      ar('M198,265 L220,265', T1) +
      '<rect x="225" y="230" width="180" height="70" rx="8" fill="#fdf3e7" stroke="' + TO + '" stroke-width="1.8"/>' + tx(315, 269, f.tg, 12.5, '#7a5220', 600) +
      ar('M405,265 L424,265', T1) +
      '<polygon points="520,207 612,265 520,323 428,265" fill="#fff" stroke="' + TO + '" stroke-width="2"/>' + tx(520, 269, f.dc, 12, '#7a5220', 600) +
      f.br.map(function (b, k) {
        var yy = 170 + k * 72;
        return ar('M612,265 L630,265 L630,' + (yy + 23) + ' L646,' + (yy + 23), T1) +
          '<rect x="650" y="' + yy + '" width="150" height="46" rx="8" fill="#fff" stroke="' + T1 + '" stroke-width="1.4"/>' +
          tx(725, yy + 27, b, 11.5, '#243447') +
          '<path d="M800,' + (yy + 23) + ' L820,' + (yy + 23) + ' L820,265" fill="none" stroke="' + T1 + '" stroke-width="1.6"/>';
      }).join('') +
      ar('M820,265 L836,265', T1) +
      '<rect x="840" y="230" width="180" height="70" rx="8" fill="#fff" stroke="' + TB + '" stroke-width="2"/>' + tx(930, 269, f.ac, 12.5, '#16436e', 600) +
      '<rect x="840" y="352" width="180" height="58" rx="8" fill="#f3eefc" stroke="' + TP + '" stroke-width="1.6"/>' +
      tx(930, 377, f.co, 11.5, '#5b4a86', 600) + tx(930, 395, '提醒 · 待办 · 反馈', 10, '#9b7fd4') +
      ar('M930,352 L930,304', TP, true) +
      ar('M1020,265 L1100,265', T1) + tx(1060, 254, '写入', 10.5, '#5b6b80') +
      '<path d="M1105,238 v56 a85,14 0 0 0 170,0 v-56" fill="' + TB + '"/>' +
      '<ellipse cx="1190" cy="238" rx="85" ry="14" fill="#2a5d8f"/>' +
      tx(1190, 272, f.st, 11.5, '#fff', 600) +
      ar('M1275,265 L1318,265', T1) + tx(1296, 254, '归档', 10.5, '#5b6b80') +
      '<ellipse cx="1405" cy="265" rx="82" ry="40" fill="#eaf6f0" stroke="' + TG + '" stroke-width="2"/>' + tx(1405, 269, f.en, 12, '#1c6b48', 600) +
      '<rect x="1105" y="352" width="170" height="48" rx="8" fill="#fff" stroke="' + T1 + '" stroke-width="1.4"/>' + tx(1190, 380, f.rt, 11.5, '#243447') +
      ar('M1190,310 L1190,348', T1) +
      ar('M1275,376 L1405,376 L1405,309', TG) +
      '<rect x="20" y="470" width="1520" height="64" rx="10" fill="#e8eef5" stroke="#c9d6e2"/>' +
      tx(W / 2, 497, m.val, 13, '#33475c', 600) +
      tx(W / 2, 519, '数据落点：' + f.st + '　协同：' + f.co + '　追溯：' + f.rt, 10.5, '#6b7a8c') +
      '</svg>';
    return s;
  }

  /* ---------- 打印摘要用精简架构图（横向单页，只保留层级与主干） ---------- */
  function buildPrintArchSVG(D) {
    var A = D.ARCH, W = 1160, LX = 8, LW = 84, NX = 102, NR = 1152;
    var rows = [
      { l: '接入', c: '#2563eb', n: A.access.nodes.map(function (x) { return x.t; }) },
      { l: '中枢', c: '#065f4e', n: A.hub.nodes.map(function (x) { return x.t; }) },
      { l: '业务', c: '#0e7490', n: D.SECKEYS.map(function (k) { return D.SECTITLE[k]; }) },
      { l: '底座', c: '#b7791f', n: A.base.nodes.map(function (x) { return x.t; }) },
      { l: '输出', c: '#18805e', n: A.output.nodes.map(function (x) { return x.t; }) }
    ];
    var RH = 52, GAP = 24, H = rows.length * RH + (rows.length - 1) * GAP + 14;
    var s = '<svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="平台架构与数据流向（精简版）">' +
      '<defs><marker id="p-aa" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#8aa0b8"/></marker>' +
      '<marker id="p-aw" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#b7791f"/></marker>' +
      '<marker id="p-ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#0e7490"/></marker></defs>';
    rows.forEach(function (r, ri) {
      var y = 7 + ri * (RH + GAP);
      s += '<rect x="' + LX + '" y="' + y + '" width="' + LW + '" height="' + RH + '" rx="9" fill="' + r.c + '"/>' +
        tx(LX + LW / 2, y + RH / 2 + 5, r.l, 14, '#fff', 700);
      var n = r.n.length, cw = (NR - NX - (n - 1) * 9) / n;
      r.n.forEach(function (t, i) {
        var x = NX + i * (cw + 9);
        s += '<rect x="' + x + '" y="' + y + '" width="' + cw + '" height="' + RH + '" rx="9" fill="' + r.c + '" fill-opacity=".07" stroke="' + r.c + '" stroke-opacity=".35"/>';
        wrap(t, Math.max(6, Math.floor((cw - 12) / (ri === 3 ? 10.2 : 11.4)))).slice(0, 2).forEach(function (ln, k, arr) {
          s += tx(x + cw / 2, y + RH / 2 + 4 + (k - (arr.length - 1) / 2) * 13, ln, ri === 3 ? 10.2 : 11.4, r.c, 600);
        });
      });
      var by = y + RH;
      if (ri === 0 || ri === 1 || ri === 3) {
        s += '<path d="M' + (W / 2) + ',' + by + ' L' + (W / 2) + ',' + (by + GAP - 3) + '" fill="none" stroke="#8aa0b8" stroke-width="1.8" marker-end="url(#p-aa)"/>';
      }
      if (ri === 2) {
        s += '<path d="M' + (W / 2 - 96) + ',' + by + ' L' + (W / 2 - 96) + ',' + (by + GAP - 3) + '" fill="none" stroke="#b7791f" stroke-width="1.8" marker-end="url(#p-aw)"/>' +
          tx(W / 2 - 88, by + 15, '写入 / 归档', 10, '#b7791f', 600, 'start') +
          '<path d="M' + (W / 2 + 96) + ',' + (by + GAP - 3) + ' L' + (W / 2 + 96) + ',' + by + '" fill="none" stroke="#0e7490" stroke-width="1.8" marker-end="url(#p-ar)"/>' +
          tx(W / 2 + 104, by + 15, '读取 / 探针', 10, '#0e7490', 600, 'start');
      }
    });
    return s + '</svg>';
  }

  return {
    esc: esc, wrap: wrap, cut: cut,
    buildArchSVG: buildArchSVG,
    buildMindSVG: buildMindSVG,
    buildFlowSVG: buildFlowSVG,
    buildPrintArchSVG: buildPrintArchSVG
  };
})();
