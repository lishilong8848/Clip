/* ==========================================================================
   灯塔 · 数据中心运维管理平台 — 图形生成层
   作用域：主页五层架构 SVG、模块详情业务流程 SVG
   依赖：LT.Data（data.js）
   ========================================================================== */
(function (global) {
  'use strict';

  const D = global.LT.Data;

  /* ------------------------------------------------------------------
     五层架构 SVG（接入 → 治理 → 业务 → 底座 → 输出）
     ------------------------------------------------------------------ */
  function archSVG() {
    const bands = D.ARCH_BANDS;
    const W = 1160, H = 486;
    const ys = [8, 88, 168, 248, 352];
    let s = `<svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="平台整体架构与数据底座">
     <defs>
       <marker id="aa" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#8aa0b8"/></marker>
       <marker id="aw" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#b7791f"/></marker>
       <marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#0e7490"/></marker>
     </defs>`;
    bands.forEach((b, bi) => {
      const y = ys[bi], hh = bi === 3 ? 88 : 64;
      s += `<rect x="8" y="${y}" width="86" height="${hh}" rx="10" fill="${b.c}"/>
            <text x="51" y="${y + hh / 2 + 5}" font-size="15" font-weight="700" fill="#fff" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">${b.l}</text>`;
      const n = b.n.length;
      const cw = (W - 120 - (n - 1) * 10) / n;
      b.n.forEach((t, i) => {
        const x = 104 + i * (cw + 10);
        s += `<rect x="${x}" y="${y}" width="${cw}" height="${hh}" rx="10" fill="${b.c}" opacity=".08" stroke="${b.c}" stroke-opacity=".35"/>
              <text x="${x + cw / 2}" y="${y + hh / 2 + 4}" font-size="${bi === 3 ? 11.5 : 12.5}" fill="${b.c}" font-weight="600" text-anchor="middle" font-family="Microsoft YaHei,sans-serif">${t}</text>`;
      });
      if (bi === 0 || bi === 1) {
        s += `<path d="M${W / 2},${y + hh} L${W / 2},${y + hh + 16}" stroke="#8aa0b8" stroke-width="2" fill="none" marker-end="url(#aa)"/>`;
      }
      if (bi === 2) {
        s += `<path d="M${W / 2 - 90},${y + hh} L${W / 2 - 90},${y + hh + 16}" stroke="#b7791f" stroke-width="2" fill="none" marker-end="url(#aw)"/>
              <text x="${W / 2 - 82}" y="${y + hh + 13}" font-size="11" fill="#b7791f" font-family="Microsoft YaHei,sans-serif">写入 / 归档</text>
              <path d="M${W / 2 + 90},${y + hh + 16} L${W / 2 + 90},${y + hh}" stroke="#0e7490" stroke-width="2" fill="none" marker-end="url(#ar)"/>
              <text x="${W / 2 + 98}" y="${y + hh + 13}" font-size="11" fill="#0e7490" font-family="Microsoft YaHei,sans-serif">读取 / 探针</text>`;
      }
      if (bi === 3) {
        s += `<path d="M${W / 2},${y + hh} L${W / 2},${ys[4]}" stroke="#8aa0b8" stroke-width="2" fill="none" marker-end="url(#aa)"/>
              <text x="${W / 2 + 10}" y="${y + hh + 13}" font-size="11" fill="#5b6b80" font-family="Microsoft YaHei,sans-serif">归档取出 · 汇总送达</text>`;
      }
    });
    return s + `</svg>`;
  }

  /* ------------------------------------------------------------------
     模块业务流程 SVG（一线发起 → 过程管控 → 数据与治理）
     ------------------------------------------------------------------ */
  function flowSVG(f, m) {
    const W = 1560, H = 560;
    const T1 = '#5b7ea6', TB = '#16436e', TO = '#e0a458', TP = '#9b7fd4', TG = '#2f9e6f';
    const tx = (x, y, t, size, fill, weight) =>
      `<text x="${x}" y="${y}" font-size="${size}" fill="${fill}" text-anchor="middle" font-weight="${weight || 400}" font-family="Microsoft YaHei,sans-serif">${t}</text>`;
    const mk = c => c === TP ? 'p' : c === TG ? 'g' : c === TO ? 'o' : 'a';
    const ar = (d, c, dash) =>
      `<path d="${d}" fill="none" stroke="${c}" stroke-width="1.8"${dash ? ' stroke-dasharray="6 4"' : ''} marker-end="url(#mf-${mk(c)})}"/>`;

    let s = `<svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${m.name} 业务流程图">
     <defs>${[['a', T1], ['o', TO], ['p', TP], ['g', TG]].map(([k, c]) => `<marker id="mf-${k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6.5" markerHeight="6.5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="${c}"/></marker>`).join('')}</defs>
     <rect width="${W}" height="${H}" rx="12" fill="#f8fbff"/>
     <rect x="20" y="8" width="1520" height="58" rx="10" fill="${TB}"/>
     ${tx(W / 2, 32, m.name + '｜业务流程：从一线发起到闭环治理', 16.5, '#fff', 700)}
     ${tx(W / 2, 52, m.tag, 11, 'rgba(255,255,255,.85)')}
     ${[[30, '01 一线发起'], [450, '02 过程管控'], [1080, '03 数据与治理']].map(([x, t]) => `<rect x="${x}" y="80" width="150" height="30" rx="6" fill="${TB}"/>` + tx(x + 75, 100, t, 12.5, '#fff', 600)).join('')}
     <rect x="20" y="120" width="410" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>
     <rect x="440" y="120" width="620" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>
     <rect x="1070" y="120" width="470" height="330" rx="10" fill="#eef2f7" stroke="#d9e1ea"/>
     <ellipse cx="120" cy="265" rx="78" ry="38" fill="#fff" stroke="${T1}" stroke-width="1.6"/>${tx(120, 269, f.r, 12.5, '#243447', 600)}
     ${ar('M198,265 L220,265', T1)}
     <rect x="225" y="230" width="180" height="70" rx="8" fill="#fdf3e7" stroke="${TO}" stroke-width="1.8"/>${tx(315, 269, f.tg, 12.5, '#7a5220', 600)}
     ${ar('M405,265 L424,265', T1)}
     <polygon points="520,207 612,265 520,323 428,265" fill="#fff" stroke="${TO}" stroke-width="2"/>${tx(520, 269, f.dc, 12, '#7a5220', 600)}
     ${f.br.map((b, k) => {
        const y = 170 + k * 72;
        return ar(`M612,265 L630,265 L630,${y + 23} L646,${y + 23}`, T1)
          + `<rect x="650" y="${y}" width="150" height="46" rx="8" fill="#fff" stroke="${T1}" stroke-width="1.4"/>`
          + tx(725, y + 27, b, 11.5, '#243447')
          + `<path d="M800,${y + 23} L820,${y + 23} L820,265" fill="none" stroke="${T1}" stroke-width="1.6"/>`;
      }).join('')}
     ${ar('M820,265 L836,265', T1)}
     <rect x="840" y="230" width="180" height="70" rx="8" fill="#fff" stroke="${TB}" stroke-width="2"/>${tx(930, 269, f.ac, 12.5, '#16436e', 600)}
     <rect x="840" y="352" width="180" height="58" rx="8" fill="#f3eefc" stroke="${TP}" stroke-width="1.6"/>${tx(930, 377, f.co, 11.5, '#5b4a86', 600)}${tx(930, 395, '提醒 · 待办 · 反馈', 10, '#9b7fd4')}
     ${ar('M930,352 L930,304', TP, true)}
     ${ar('M1020,265 L1100,265', T1)}${tx(1060, 254, '写入', 10.5, '#5b6b80')}
     <path d="M1105,238 v56 a85,14 0 0 0 170,0 v-56" fill="${TB}"/>
     <ellipse cx="1190" cy="238" rx="85" ry="14" fill="#2a5d8f"/>
     ${tx(1190, 272, f.st, 11.5, '#fff', 600)}
     ${ar('M1275,265 L1318,265', T1)}${tx(1296, 254, '归档', 10.5, '#5b6b80')}
     <ellipse cx="1405" cy="265" rx="82" ry="40" fill="#eaf6f0" stroke="${TG}" stroke-width="2"/>${tx(1405, 269, f.en, 12, '#1c6b48', 600)}
     <rect x="1105" y="352" width="170" height="48" rx="8" fill="#fff" stroke="${T1}" stroke-width="1.4"/>${tx(1190, 380, f.rt, 11.5, '#243447')}
     ${ar('M1190,310 L1190,348', T1)}
     ${ar('M1275,376 L1405,376 L1405,309', TG)}
     <rect x="20" y="470" width="1520" height="64" rx="10" fill="#e8eef5" stroke="#c9d6e2"/>
     ${tx(W / 2, 497, m.val, 13, '#33475c', 600)}
     ${tx(W / 2, 519, '数据落点：' + f.st + '　协同：' + f.co + '　追溯：' + f.rt, 10.5, '#6b7a8c')}
     </svg>`;
    return s;
  }

  global.LT.Graphics = { archSVG, flowSVG };
})(window);
