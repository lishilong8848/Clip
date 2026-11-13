#!/usr/bin/env node
/* mock server 契约自检：启动子进程 → 断言契约规则 → 退出 */
'use strict';
const { spawn } = require('child_process');
const path = require('path');
const PORT = 8799;
const BASE = `http://127.0.0.1:${PORT}`;
const problems = [];
const ok = (cond, msg) => { console.log((cond ? '  ✓ ' : '  ✗ ') + msg); if (!cond) problems.push(msg); };

(async () => {
  const child = spawn(process.execPath, [path.join(__dirname, 'server.js'), String(PORT)], { stdio: ['ignore', 'pipe', 'pipe'] });
  let booted = false;
  child.stdout.on('data', d => { if (/mock server/.test(String(d))) booted = true; });
  child.stderr.on('data', d => console.error('  [server] ' + String(d).trim()));
  for (let i = 0; i < 40 && !booted; i++) await new Promise(r => setTimeout(r, 100));
  if (!booted) { console.error('server 未启动'); process.exit(2); }
  const get = async (p) => { const r = await fetch(BASE + p); return { status: r.status, audit: r.headers.get('x-mock-audit-id'), body: await r.json() }; };

  console.log('=== 1. bootstrap 契约 ===');
  const bs = await get('/api/workbench/bootstrap?role=bsup');
  ok(bs.status === 200, 'bootstrap 200');
  ok(bs.body.identity.scopeFixed === true, 'bsup scopeFixed=true');
  ok(JSON.stringify(bs.body.identity.scope.buildings) === '["A"]', 'bsup 范围仅 A 楼');
  ok(bs.body.identity.policyVersion === '2026.08.01-01', 'policyVersion 回显');
  ok(bs.body.keyNumbers.every(k => (k.actionable === false) ? !!k.derivationNote : !!k.drilldownKey), '可操作数字必须带 drilldownKey，历史数字必须带 derivationNote');
  ok(!!bs.audit, '响应带 x-mock-audit-id');

  console.log('=== 2. 同口径：bootstrap 数字 == sections 总数 == drilldown 总数 ===');
  const sec = await get('/api/workbench/sections/urgent?role=bsup');
  const p01 = bs.body.keyNumbers.find(k => k.key === 'urgent_p01').value;
  ok(sec.body.total === sec.body.items.length, 'sections total == items.length');
  ok(p01 === sec.body.items.filter(i => i.priority === 'P0' || i.priority === 'P1').length, `keyNumbers.urgent_p01(${p01}) == sections 中 P0+P1 条数(${sec.body.items.filter(i => i.priority === 'P0' || i.priority === 'P1').length})`);
  const dr = await get('/api/workbench/drilldown?drilldownKey=urgent?priority=P0,P1&role=bsup');
  ok(dr.body.total === p01, `drilldown total(${dr.body.total}) == 卡片数字(${p01})`);
  ok(dr.body.snapshotId === sec.body.snapshotId, 'drilldown 与 sections 同 snapshotId');

  console.log('=== 3. 行级范围过滤与拒绝 ===');
  ok(sec.body.items.every(i => !i.scope.buildings.length || i.scope.buildings.every(b => b === 'A')), 'bsup 的 urgent 全部为 A 楼或园区级');
  const bad = await get('/api/workbench/sections/urgent?role=bsup&buildings=B');
  ok(bad.status === 403, 'bsup 请求 B 楼 → 403');
  ok(bad.body.reasonCode === 'DENY_SCOPE_MISMATCH', '原因码 DENY_SCOPE_MISMATCH');
  ok(/【A 楼主管】/.test(bad.body.explanation) && /DENY_SCOPE_MISMATCH/.test(bad.body.explanation), '拒绝解释模板完整');
  ok(!!bad.body.auditId, '拒绝带审计编号');
  ok(!/B 楼.*\d+ 条/.test(bad.body.explanation), '拒绝解释不泄露其他楼栋数量');

  console.log('=== 4. snapshot 不一致 → 409 ===');
  const mm = await get('/api/workbench/sections/urgent?role=manager&snapshotId=SNAP-OLD');
  ok(mm.status === 409 && mm.body.reasonCode === 'SNAPSHOT_MISMATCH', '409 SNAPSHOT_MISMATCH');

  console.log('=== 5. sourceStatus 四态 ===');
  const down = await get('/api/workbench/sections/urgent?role=manager&sourceStatus=down');
  ok(down.status === 503 && down.body.retryAfterSec > 0 && !!down.body.lastSuccessAt, 'down → 503 + retryAfter + lastSuccessAt');
  const stale = await get('/api/workbench/sections/urgent?role=manager&sourceStatus=stale');
  ok(stale.status === 200 && stale.body.sourceStatus === 'stale' && /2026-11-10T22:14/.test(stale.body.lastSuccessAt), 'stale → 200 + 最近成功时间');
  const deny = await get('/api/workbench/sections/urgent?role=manager&sourceStatus=deny');
  ok(deny.status === 403 && !!deny.body.explanation, 'deny → 403 + 解释');

  console.log('=== 6. 排序与对象完整性（§8.3/§8.7）===');
  const mg = await get('/api/workbench/sections/urgent?role=manager');
  const po = { P0:0, P1:1, P2:2, P3:3 };
  const sorted = mg.body.items.every((it, i, a) => i === 0 || po[a[i - 1].priority] <= po[it.priority]);
  ok(sorted, '服务端已按 priority 排序（前端不再重排）');
  ok(mg.body.items.every(it => it.reason && it.dedupKey && it.sla && it.sourceStatus && it.actionUrl && it.owner), '每条待办含 reason/dedupKey/sla/sourceStatus/actionUrl/owner');
  ok(mg.body.items.every(it => ['P0', 'P1', 'P2', 'P3'].includes(it.priority) && ['pending', 'processing', 'done', 'overdue'].includes(it.status)), 'priority/status 为结构化枚举');
  const multi = mg.body.items.find(it => it.mergedSourceIds.length > 1);
  ok(!!multi, '存在多来源合并样例（mergedSourceIds）');

  console.log('=== 7. duty 巡检同口径 ===');
  const bd = await get('/api/workbench/bootstrap?role=duty');
  const pat = bd.body.keyNumbers.find(k => k.key === 'patrol_done');
  const pd = await get('/api/workbench/drilldown?drilldownKey=ops:patrol|duty&role=duty');
  const doneCount = pd.body.items.filter(i => i.status === 'done').length;
  ok(pat && pat.value === `${doneCount}/${pd.body.total}`, `巡检 ${pat && pat.value} == 明细 ${doneCount}/${pd.body.total}`);

  console.log('=== 8. 未知路由/角色 ===');
  const nr = await get('/api/workbench/nope');
  ok(nr.status === 404, '未知路由 404');
  const br = await get('/api/workbench/bootstrap?role=ghost');
  ok(br.status === 400 && br.body.reasonCode === 'DENY_ROLE_UNMAPPED', '未知角色 400 DENY_ROLE_UNMAPPED');

  console.log('\n════════════════════════════════');
  if (problems.length) { console.log(`✗ ${problems.length} 项失败`); problems.forEach(p => console.log('  - ' + p)); }
  else console.log('✓ mock server 契约自检全部通过');
  child.stdout.destroy(); child.stderr.destroy(); child.kill();
  setTimeout(() => process.exit(problems.length ? 1 : 0), 60);
})().catch(e => { console.error(e); process.exit(2); });
