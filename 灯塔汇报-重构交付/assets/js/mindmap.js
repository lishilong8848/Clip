/* ==========================================================================
   灯塔 · 数据中心运维管理平台 — 交互式架构思维导图（自顶向下树状）
   布局：顶部调度中枢 → 中部六大功能域横排 → 域下模块纵向成列
         （宽屏横向铺满、1:1 可读；窄屏由手风琴索引替代）
   交互：滚轮缩放 / 拖拽平移 / 按钮缩放还原 / 悬浮职责卡 / 关联虚线高亮 /
         点击叶子进详情 / 点击域节点折叠展开
   依赖：LT.Data（data.js）、LT.Views（views.js，点击叶子时调用）
   ========================================================================== */
(function (global) {
  'use strict';

  const D = global.LT.Data;

  /* ---- 画布与节点几何（自顶向下） ---- */
  const VB = { w: 1500, h: 500 };
  const HUB = { x: 640, y: 14, w: 220, h: 70 };
  const HUB_CX = HUB.x + HUB.w / 2;              /* 750 */
  const DOM_Y = 118, DOM_H = 46, DOM_W = 170;
  const LEAF_W = 232, LEAF_H = 32, PITCH = 37, LEAF_Y0 = 196;
  const KEYS = ['s1', 's2', 's3', 's4', 's5', 's6'];
  const COL_PITCH = 244, COL_X0 = 140;           /* 各域列中心 */

  let _geo = null;

  function layout() {
    const geo = { leaves: {}, domains: {} };
    KEYS.forEach((k, ci) => {
      const c = COL_X0 + ci * COL_PITCH;
      const list = D.MODULES.filter(m => m.s === k);
      geo.domains[k] = { cx: c, x: c - DOM_W / 2, y: DOM_Y, w: DOM_W, h: DOM_H };
      list.forEach((m, mi) => {
        const y = LEAF_Y0 + mi * PITCH;
        geo.leaves[m.i] = { x: c - LEAF_W / 2, y: y, w: LEAF_W, h: LEAF_H, cx: c, cy: y + LEAF_H / 2, k: k };
      });
    });
    return geo;
  }

  function buildSVG(geo) {
    const C = D.SECCOLOR;
    let defs = `<marker id="mk-rel" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#7c3aed"/></marker>`;
    KEYS.forEach(k => {
      defs += `<marker id="mk-${k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="${C[k]}"/></marker>`;
    });

    let edges = '', nodes = '';

    /* 中枢 → 各域 */
    KEYS.forEach(k => {
      const d = geo.domains[k];
      edges += `<path class="e-dom" data-k="${k}" d="M${HUB_CX},${HUB.y + HUB.h} C ${HUB_CX},${HUB.y + HUB.h + 16} ${d.cx},${DOM_Y - 16} ${d.cx},${DOM_Y}" fill="none" stroke="${C[k]}" stroke-width="2" opacity=".55" marker-end="url(#mk-${k})"/>`;
    });

    /* 域 → 模块列（链式下垂） */
    KEYS.forEach(k => {
      const d = geo.domains[k];
      const list = D.MODULES.filter(m => m.s === k);
      list.forEach((m, mi) => {
        const L = geo.leaves[m.i];
        const fromY = mi === 0 ? (d.y + d.h) : (geo.leaves[list[mi - 1].i].y + LEAF_H);
        edges += `<path class="e-leaf" data-k="${k}" d="M${d.cx},${fromY} L${d.cx},${L.y}" fill="none" stroke="${C[k]}" stroke-width="1.4" opacity=".5" marker-end="url(#mk-${k})"/>`;
      });
    });

    /* 中枢节点 */
    nodes += `<g class="hubnode">
      <rect x="${HUB.x}" y="${HUB.y}" width="${HUB.w}" height="${HUB.h}" rx="12" fill="#087a61"/>
      <rect x="${HUB.x}" y="${HUB.y}" width="${HUB.w}" height="${HUB.h}" rx="12" fill="none" stroke="#0e9b7a" stroke-width="2" opacity=".6"/>
      <text x="${HUB_CX}" y="${HUB.y + 28}" font-size="15" font-weight="700" fill="#fff" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">灯塔运维调度中枢</text>
      <text x="${HUB_CX}" y="${HUB.y + 46}" font-size="10.5" fill="rgba(255,255,255,.85)" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">25 模块 · 统一调度 · 智能助手 52 工具</text>
      <text x="${HUB_CX}" y="${HUB.y + 61}" font-size="10" fill="rgba(255,255,255,.7)" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">业务写入一律人工确认</text>
    </g>`;

    /* 域节点 */
    KEYS.forEach(k => {
      const d = geo.domains[k];
      const n = D.MODULES.filter(m => m.s === k).length;
      nodes += `<g class="dnode" data-k="${k}" role="button" tabindex="0" aria-label="${D.SECTITLE[k]}，点击折叠或展开">
        <rect x="${d.x}" y="${d.y}" width="${d.w}" height="${d.h}" rx="9" fill="${C[k]}" fill-opacity=".12" stroke="${C[k]}" stroke-width="1.6"/>
        <text class="dt" x="${d.x + 12}" y="${d.y + d.h / 2 + 4.5}" font-size="12.5" font-weight="700" fill="${C[k]}" font-family="Microsoft YaHei,sans-serif">${D.SECTITLE[k]}</text>
        <circle class="db" cx="${d.x + d.w - 16}" cy="${d.y + d.h / 2}" r="9.5" fill="${C[k]}"/>
        <text class="dbt" x="${d.x + d.w - 16}" y="${d.y + d.h / 2 + 3.5}" font-size="10.5" font-weight="700" fill="#fff" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">${n}</text>
      </g>`;
    });

    /* 模块叶子 */
    Object.keys(geo.leaves).forEach(i => {
      const L = geo.leaves[i];
      const m = D.MODULES[+i];
      nodes += `<g class="lnode" data-i="${i}" data-k="${L.k}" role="button" tabindex="0" aria-label="${m.name}，点击查看模块详情">
        <rect x="${L.x}" y="${L.y}" width="${L.w}" height="${L.h}" rx="7" fill="#fff" stroke="${C[L.k]}" stroke-width="1.3"/>
        <circle cx="${L.x + 12}" cy="${L.cy}" r="3" fill="${C[L.k]}"/>
        <text x="${L.x + 22}" y="${L.cy + 4}" font-size="12.5" fill="#243447" font-family="Microsoft YaHei,sans-serif">${m.name}</text>
      </g>`;
    });

    return `<svg id="mmSvg" viewBox="0 0 ${VB.w} ${VB.h}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="灯塔平台架构思维导图">
      <defs>${defs}</defs>
      <g id="mmRoot">
        <g id="mmEdges">${edges}</g>
        <g id="mmNodes">${nodes}</g>
        <g id="mmRel"></g>
      </g>
    </svg>`;
  }

  /* ---- 悬浮职责卡 ---- */
  function showTip(i, nodeEl) {
    const tip = document.getElementById('mmTip');
    if (!tip) return;
    const m = D.MODULES[i];
    tip.innerHTML = `<b>${m.name}</b><span class="tp">${m.pill}</span><p>${m.tag}</p>` +
      ((D.METRICS[i] || []).length ? `<p class="tm">${(D.METRICS[i] || []).slice(0, 3).join(' · ')}</p>` : '');
    tip.style.display = 'block';
    const r = nodeEl.getBoundingClientRect();
    const tw = tip.offsetWidth, th = tip.offsetHeight;
    let x = r.right + 12, y = r.top;
    if (x + tw > window.innerWidth - 8) x = r.left - tw - 12;
    if (x < 8) x = 8;
    if (y + th > window.innerHeight - 8) y = window.innerHeight - th - 8;
    if (y < 8) y = 8;
    tip.style.left = x + 'px';
    tip.style.top = y + 'px';
  }
  function hideTip() {
    const tip = document.getElementById('mmTip');
    if (tip) tip.style.display = 'none';
  }

  /* ---- 关联虚线高亮 ---- */
  function drawRel(svg, i) {
    const g = svg.querySelector('#mmRel');
    if (!g || !_geo) return;
    const a = _geo.leaves[i];
    if (!a) return;
    let h = '';
    (D.REL[i] || []).forEach(j => {
      const b = _geo.leaves[j];
      if (!b) return;
      const my = (a.cy + b.cy) / 2;
      h += `<path d="M${a.cx},${a.cy} C ${a.cx},${my} ${b.cx},${my} ${b.cx},${b.cy}" fill="none" stroke="#7c3aed" stroke-width="1.6" stroke-dasharray="5 4" opacity=".8" marker-end="url(#mk-rel)"/>`;
      const n = svg.querySelector('.lnode[data-i="' + j + '"]');
      if (n) n.classList.add('hl');
    });
    g.innerHTML = h;
    const self = svg.querySelector('.lnode[data-i="' + i + '"]');
    if (self) self.classList.add('hov');
  }
  function clearRel(svg) {
    const g = svg.querySelector('#mmRel');
    if (g) g.innerHTML = '';
    svg.querySelectorAll('.lnode.hl,.lnode.hov').forEach(n => n.classList.remove('hl', 'hov'));
  }

  /* ---- 域折叠 / 展开 ---- */
  function toggleDomain(svg, k) {
    const dn = svg.querySelector('.dnode[data-k="' + k + '"]');
    if (!dn) return;
    const collapsed = dn.classList.toggle('collapsed');
    const rect = dn.querySelector('rect');
    const dt = dn.querySelector('.dt');
    const C = D.SECCOLOR[k];
    if (collapsed) { rect.setAttribute('fill-opacity', '1'); dt.setAttribute('fill', '#fff'); }
    else { rect.setAttribute('fill-opacity', '.12'); dt.setAttribute('fill', C); }
    svg.querySelectorAll('.lnode[data-k="' + k + '"]').forEach(n => { n.style.display = collapsed ? 'none' : ''; });
    svg.querySelectorAll('.e-leaf[data-k="' + k + '"]').forEach(n => { n.style.display = collapsed ? 'none' : ''; });
    clearRel(svg); hideTip();
  }

  /* ---- 挂载 ---- */
  function mount() {
    const canvas = document.getElementById('mmCanvas');
    if (!canvas) return;
    const geo = layout();
    _geo = geo;
    canvas.innerHTML = buildSVG(geo);

    const svg = canvas.querySelector('#mmSvg');
    const root = svg.querySelector('#mmRoot');
    let s = 1, tx = 0, ty = 0;
    const apply = () => root.setAttribute('transform', 'translate(' + tx + ' ' + ty + ') scale(' + s + ')');
    const zoomAt = (px, py, f) => {
      const ns = Math.min(3, Math.max(.5, s * f));
      const wx = (px - tx) / s, wy = (py - ty) / s;
      tx = px - wx * ns; ty = py - wy * ns; s = ns; apply();
    };
    const toSvg = e => {
      const r = svg.getBoundingClientRect();
      return { x: (e.clientX - r.left) / r.width * VB.w, y: (e.clientY - r.top) / r.height * VB.h };
    };

    svg.addEventListener('wheel', e => {
      e.preventDefault();
      const p = toSvg(e);
      zoomAt(p.x, p.y, e.deltaY < 0 ? 1.15 : 1 / 1.15);
    }, { passive: false });

    let drag = null;
    svg.addEventListener('mousedown', e => {
      if (e.target.closest && e.target.closest('.lnode,.dnode,.hubnode')) return;
      drag = { x: e.clientX, y: e.clientY, tx: tx, ty: ty };
      svg.classList.add('grab');
    });
    window.addEventListener('mousemove', e => {
      if (!drag) return;
      const r = svg.getBoundingClientRect();
      const k = VB.w / r.width;
      tx = drag.tx + (e.clientX - drag.x) * k;
      ty = drag.ty + (e.clientY - drag.y) * k;
      apply();
    });
    window.addEventListener('mouseup', () => { drag = null; svg.classList.remove('grab'); });

    const bin = document.getElementById('mmIn');
    const bout = document.getElementById('mmOut');
    const brs = document.getElementById('mmReset');
    if (bin) bin.onclick = () => zoomAt(VB.w / 2, VB.h / 2, 1.2);
    if (bout) bout.onclick = () => zoomAt(VB.w / 2, VB.h / 2, 1 / 1.2);
    if (brs) brs.onclick = () => { s = 1; tx = 0; ty = 0; apply(); };

    svg.addEventListener('mouseover', e => {
      const ln = e.target.closest ? e.target.closest('.lnode') : null;
      if (!ln) return;
      showTip(+ln.dataset.i, ln);
      drawRel(svg, +ln.dataset.i);
    });
    svg.addEventListener('mouseout', e => {
      const ln = e.target.closest ? e.target.closest('.lnode') : null;
      if (!ln) return;
      const to = e.relatedTarget && e.relatedTarget.closest ? e.relatedTarget.closest('.lnode') : null;
      if (to === ln) return;
      hideTip(); clearRel(svg);
    });
    svg.addEventListener('click', e => {
      const ln = e.target.closest ? e.target.closest('.lnode') : null;
      if (ln) { global.LT.Views.openModule(+ln.dataset.i); return; }
      const dn = e.target.closest ? e.target.closest('.dnode') : null;
      if (dn) toggleDomain(svg, dn.dataset.k);
    });
    svg.addEventListener('keydown', e => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      const ln = e.target.closest ? e.target.closest('.lnode') : null;
      if (ln) { e.preventDefault(); global.LT.Views.openModule(+ln.dataset.i); return; }
      const dn = e.target.closest ? e.target.closest('.dnode') : null;
      if (dn) { e.preventDefault(); toggleDomain(svg, dn.dataset.k); }
    });
  }

  global.LT = global.LT || {};
  global.LT.Mindmap = { mount: mount };
})(window);
