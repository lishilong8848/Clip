#!/usr/bin/env node
/* ═════════════════════════════════════════════════════════════════════
   个人工作台 Mock Server · 阶段 3 前端联调用
   依据：《个人工作台 前后端接口契约 · 阶段 3 对接清单》§2.1 / §2.2 / §2.3
   零依赖（仅 node:http），固定 fixtures，可复现；不连接任何生产系统。

   运行：  node mock-server/server.js [port]     默认 8787
   自检：  node mock-server/selftest.js

   Mock-only 约定（生产不存在）：
   - 角色通过查询参数 role=<key> 或请求头 x-mock-role 指定（生产由签名会话解析 Principal V2）
   - 分区降级通过查询参数 sourceStatus=ok|stale|down|deny 模拟
   - 所有响应带 x-mock-audit-id，对应契约中的 auditId / decision_id
   ═════════════════════════════════════════════════════════════════════ */
'use strict';
const http = require('http');

const PORT = Number(process.argv[2] || process.env.PORT || 8787);
const SNAPSHOT = 'SNAP-20261111-0530';
const POLICY = '2026.08.01-01';
const SNAPSHOT_AT = '2026-11-11T05:30:00+08:00';
const LAST_SUCCESS = '2026-11-11T11:14:00+08:00';

/* ── 角色与范围（§1 Principal V2 的 mock 投影） ─────────────────────── */
const ROLES = {
  manager: { label:'园区经理', code:'PARK_MANAGER', buildings:['A','B','C','D','E','H'], professions:[], stations:[], fixed:false, caps:['today.read','purchase.review','contract.read','export.request'] },
  bsup:    { label:'A 楼主管', code:'BUILDING_SUPERVISOR', buildings:['A'], professions:[], stations:[], fixed:true, caps:['today.read','purchase.review'] },
  beng:    { label:'B 楼电气工程师', code:'BUILDING_ENGINEER', buildings:['B'], professions:['电气'], stations:[], fixed:true, caps:['today.read','construction.execute'] },
  duty:    { label:'值班员 · 早班', code:'DUTY_OPERATOR', buildings:['A'], professions:[], stations:[], fixed:true, shift:'07:00-15:00', caps:['today.read','patrol.execute'] },
  plead:   { label:'园区主管工程师 · 电气', code:'PROFESSION_LEAD', buildings:['A','B','C','D','E','H'], professions:['电气'], stations:['STATION_110'], fixed:false, caps:['today.read','change.review'] },
  gadmin:  { label:'综合管理员', code:'GENERAL_ADMIN', buildings:['A','B','C','D','E','H'], professions:[], stations:[], fixed:false, caps:['today.read','purchase.review','contract.read'] }
};

/* ── 统一待办对象 fixtures（与原型 index.html 同值，保证同口径可测） ── */
const T = (id, type, source, title, scope, p, status, dueAt, owner, reason, extra) => Object.assign({
  id, type, source, title, summary: title, scope, priority: p, status, dueAt,
  owner: { id: 'ou_' + owner, name: owner }, coOwners: [],
  actionUrl: `/module/${type}?id=${id}`, reason, sourceStatus: 'ok',
  dedupKey: `${id}+${type}+${scope.buildings.join(',') || 'campus'}`, mergedSourceIds: [id],
  sla: p === 'P0' ? { confirmBy: '2026-11-11T11:20:00+08:00', escalateAt: '2026-11-11T11:31:00+08:00' }
     : p === 'P1' ? { confirmBy: '2026-11-11T11:50:00+08:00', escalateAt: '2026-11-11T13:20:00+08:00' }
     : p === 'P2' ? { confirmBy: '2026-11-11T15:00:00+08:00', escalateAt: null }
     : { confirmBy: dueAt, escalateAt: null }
}, extra || {});
const B = (...b) => ({ campus:'Nantong', buildings:b, professions:[], stations:[], objectRelation:'owner' });

const URGENT = {
  manager: [
    T('TD-001','alarm','告警与风险','C 楼 蓄电池室 烟感预报警 未确认',B('C'),'P0','pending','2026-11-11T11:20:00+08:00','待指派','你是园区经理；该告警等级 P0 且 15 分钟未确认将自动升级；来源：消防主机直连。',{mergedSourceIds:['AL-2026-1187@消防主机','AL-2026-1187@今日航图']}),
    T('TD-002','event','告警与风险','A 楼 2F 机房 漏水告警 处置超时',B('A'),'P0','processing','2026-11-11T11:35:00+08:00','陈志远','你是园区经理；P0 事件 SLA 为 5 分钟确认、15 分钟升级，当前已 11 分钟未闭环。'),
    T('TD-003','contract','合同','南通云启 冷却托管合同 临期 14 天',B(),'P1','pending','2026-11-25T18:00:00+08:00','王敏','你是园区经理；该合同 scopeType=CAMPUS 且未分摊到楼栋，仅园区角色可见。'),
    T('TD-004','purchase','采购','B 楼 高压开关备件 采购单 超时未审',B('B'),'P1','pending','2026-11-11T13:00:00+08:00','陈志远','你是园区经理；采购审批额度内由你终审，申请人为陈志远，职责分离校验通过。'),
    T('TD-005','construction','施工','D 楼 冷塔防腐施工 许可临期 3 天',B('D'),'P2','processing','2026-11-14T18:00:00+08:00','恒信机电','你是园区经理；施工许可到期未复核将自动停工，影响 11 月节点。'),
    T('TD-006','export','报表','10 月园区运行月报 导出申请 待复核',B(),'P3','pending','2026-11-15T18:00:00+08:00','王敏','你是园区经理；导出需单次短时授权 + 范围裁剪 + 水印，本次申请范围=全园区。')
  ],
  bsup: [
    T('TD-101','alarm','告警与风险','A 楼 4F 冷塔风机 2 号 振动越限',B('A'),'P0','pending','2026-11-11T11:20:00+08:00','张伟','你是 A 楼主管；该告警对象楼栋=A 楼；P0 需 5 分钟内确认。'),
    T('TD-102','construction','施工','A 楼 1F 高压配电室 预防性试验 节点待确认',B('A'),'P1','pending','2026-11-11T12:10:00+08:00','刘小杏','你是 A 楼主管；该项目 project.buildingId=A；节点未确认将顺延停机窗口。'),
    T('TD-103','cert','证照与培训','A 楼 高压进网证 2 人 30 天内到期',B('A'),'P1','pending','2026-12-08T18:00:00+08:00','张伟','你是 A 楼主管；证照归属人员 buildingId=A；到期未复训将失去操作资质。'),
    T('TD-104','handover','值班','夜班交接 2 项未闭环（水表照片 / 风机异响）',B('A'),'P2','pending','2026-11-11T15:00:00+08:00','李静','你是 A 楼主管；交接对象班组=A 楼早班；未签收前责任仍挂在夜班。'),
    T('TD-105','stock','库存','A 楼 低库存 3 项（兆欧表电池 / 表笔 / 保险丝）',B('A'),'P3','pending','2026-11-13T18:00:00+08:00','周敏','你是 A 楼主管；库存仓位责任楼栋=A；低于安全库存阈值触发。')
  ],
  beng: [
    T('TD-201','construction','施工','B 楼 2F UPS 模块更换 阻断条件未解除',B('B'),'P0','pending','2026-11-11T12:00:00+08:00','陈志远','你是 B 楼电气工程师；该施工对象楼栋=B 且专业=电气，且你是施工责任人；阻断未解除不提供开工按钮。'),
    T('TD-202','event','事件','B 楼 2F 机房 温湿度越限 事件接单',B('B'),'P1','pending','2026-11-11T11:40:00+08:00','陈志远','你是 B 楼电气工程师；事件对象楼栋=B、专业=电气，且当前在岗当班。',{mergedSourceIds:['EV-1111-0342@事件','EV-1111-0342@告警']}),
    T('TD-203','maint','维保检修','B 楼 3F 配电柜 绝缘复测 证据回填',B('B'),'P2','processing','2026-11-11T15:00:00+08:00','陈志远','你是 B 楼电气工程师；该检修单负责人=本人；缺兆欧表读数照片不能申请闭环。'),
    T('TD-204','change','施工变更','B 楼 2F 气流遏制盲板补装 变更会签',B('B'),'P2','pending','2026-11-11T16:00:00+08:00','刘小杏','你是 B 楼电气工程师；变更涉及电气专业，RACI 中你为 C（征询），仅会签不审批。'),
    T('TD-205','patrol','巡检','B 楼 电气专业 周巡 4 点 待执行',B('B'),'P3','pending','2026-11-13T18:00:00+08:00','陈志远','你是 B 楼电气工程师；巡检计划专业=电气、楼栋=B，且本人为指定执行人。')
  ],
  duty: [
    T('TD-301','event','事件','A 楼 1F 消防泵房 压力波动 事件接单',B('A'),'P0','pending','2026-11-11T11:40:00+08:00','张伟','你是 A 楼早班值班员；事件当班区域=A 楼且已签到接班；P1 事件 SLA 30 分钟确认。'),
    T('TD-302','patrol','巡检','A 楼 4F 冷塔平台 巡检点 3 漏检',B('A'),'P1','pending','2026-11-11T12:00:00+08:00','张伟','你是 A 楼早班值班员；巡检计划班组=早班 A 组；漏检点在本班时间窗内。'),
    T('TD-303','handover','值班','A 楼 3F 生活水表 读数缺照片 交班缺项',B('A'),'P1','processing','2026-11-11T15:00:00+08:00','张伟','你是 A 楼早班值班员；该记录责任人=本人；缺照片将形成交班缺项并阻塞水耗归档。'),
    T('TD-304','patrol','巡检','A 楼 1F 配电间 红外测温 异常复测',B('A'),'P2','pending','2026-11-11T14:00:00+08:00','张伟','你是 A 楼早班值班员；巡检发现异常转复测任务，指派给本人。'),
    T('TD-305','handover','值班','交接班登记 本班 4 项处置留痕核对',B('A'),'P3','pending','2026-11-11T14:40:00+08:00','张伟','你是 A 楼早班值班员；交接班需在本班结束前完成签收，未交接进入待处理队列。')
  ],
  plead: [
    T('TD-401','station','告警与风险','110kV 站 2 号主变 油温越限 预判',B(),'P0','pending','2026-11-11T11:24:00+08:00','110kV 当班','你是电气专业主管且命中独立 STATION_110 授权包；该告警站点=STATION_110；仅研判与记录，不含设备控制。',{scope:{campus:'Nantong',buildings:[],professions:['电气'],stations:['STATION_110'],objectRelation:'owner'}}),
    T('TD-402','change','施工变更','C 楼 1F 2 号冷机检修 变更技术审查',B('C'),'P1','pending','2026-11-11T12:30:00+08:00','张伟','你是电气专业主管；变更专业=电气，RACI 中你为 A（最终技术负责），可退回补充。'),
    T('TD-403','maint','维保检修','全园区 电气专业 检修计划 冲突 2 处',B(),'P1','processing','2026-11-11T14:00:00+08:00','李世龙','你是电气专业主管；范围=全园区∩电气；两处检修窗口重叠需专业统筹。'),
    T('TD-404','risk','风险事项','B 楼 电气井 电缆桥架锈蚀 风险升级复核',B('B'),'P2','pending','2026-11-11T17:00:00+08:00','陈志远','你是电气专业主管；风险对象专业=电气；楼栋工程师申请升级至 P1，需专业复核。'),
    T('TD-405','report','报表','电气专业 月度技术监督报告 会签',B(),'P3','pending','2026-11-14T18:00:00+08:00','李世龙','你是电气专业主管；报告范围=全园区∩电气；会签不产生审批权。')
  ],
  gadmin: [
    T('TD-501','purchase','采购','办公区 打印机租赁 采购单 资料缺件',B(),'P1','pending','2026-11-11T12:00:00+08:00','王敏','你是综合管理员；采购域在授权包内；缺供应商资质附件不能进入审批。'),
    T('TD-502','contract','合同','保洁服务合同 付款节点 7 天内',B(),'P1','pending','2026-11-18T18:00:00+08:00','王敏','你是综合管理员；合同域在授权包内；付款经办人≠付款审批人，你仅经办。'),
    T('TD-503','auth','业务授权','施工队 临时进场授权 今日 18:00 到期',B(),'P1','processing','2026-11-11T18:00:00+08:00','王敏','你是综合管理员；授权包有效期至今日 18:00；到期未确认将自动回收并失效缓存。'),
    T('TD-504','stock','库存','行政仓 库存台账 2 条账实不符',B(),'P2','pending','2026-11-11T17:00:00+08:00','王敏','你是综合管理员；库存域在授权包内；盘盈盘亏审批人不能是本次登记人。'),
    T('TD-505','audit','权限与审计','10 月业务授权审计 3 条异常 说明',B(),'P3','pending','2026-11-15T18:00:00+08:00','王敏','你是综合管理员；审计域只读 + 说明；审计日志不可编辑或覆盖。')
  ]
};

/* 巡检点 fixtures（duty 的 5/8 同口径来源） */
const PATROL = [
  { id:'XJ-1', title:'1F 配电间 红外测温', done:true }, { id:'XJ-2', title:'1F 消防泵房 压力核对', done:true },
  { id:'XJ-3', title:'2F 机房 温湿度抄录', done:true }, { id:'XJ-4', title:'3F 电池室 通风检查', done:true },
  { id:'XJ-5', title:'3F 生活水表 读数', done:true, note:'缺照片（交班缺项）' }, { id:'XJ-6', title:'4F 冷塔平台 风机复听', done:false, note:'漏检' },
  { id:'XJ-7', title:'4F 冷塔平台 振动测量', done:false, note:'异常转复测' }, { id:'XJ-8', title:'5F 屋面 排水检查', done:false, note:'待巡' }
];
/* 告警 fixtures（manager 4 / bsup 2 同口径来源） */
const ALARMS = {
  manager: [
    { id:'AL-1', title:'C 楼 蓄电池室 烟感预报警', level:'P0', st:'未确认' },
    { id:'AL-2', title:'A 楼 2F 机房 漏水告警', level:'P0', st:'处置超时' },
    { id:'AL-3', title:'B 楼 4F 冷塔风机 2 号 振动越限', level:'P1', st:'处理中' },
    { id:'AL-4', title:'H 楼 1F 配电间 温度越限', level:'P2', st:'待确认' }
  ],
  bsup: [
    { id:'AL-11', title:'4F 冷塔风机 2 号 振动越限', level:'P0', st:'未确认' },
    { id:'AL-12', title:'1F 精密空调 4 号 除霜循环', level:'P1', st:'处理中' }
  ]
};

/* ── 工具 ─────────────────────────────────────────────────────────── */
let auditSeq = 4800;
const auditId = () => 'DEC-20261111-00' + (++auditSeq);
const EXPLAIN = (role, obj, action, scopeTxt, objTxt, code) =>
  `当前账号以【${role.label}】访问【${obj}】，所需动作【${action}】；命中的有效范围为【南通园区 / ${scopeTxt} / 当班有效】，对象范围为【${objTxt}】；因【${code}】拒绝。规则版本【${POLICY}】，人员/组织快照时间【${SNAPSHOT_AT}】。`;

function send(res, code, body, extraHeaders){
  const h = Object.assign({
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-headers': 'x-mock-role, content-type',
    'access-control-allow-methods': 'GET, POST, OPTIONS'
  }, extraHeaders || {});
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(code, h);
  res.end(payload);
}
function roleOf(req, q){
  const k = q.get('role') || req.headers['x-mock-role'] || 'manager';
  return ROLES[k] ? k : null;
}
/* 服务端行级过滤：mock 也严格执行「先过滤后聚合」 */
function filterByScope(items, role, requestedBuildings){
  if (role.fixed){
    if (requestedBuildings && requestedBuildings.length){
      const outside = requestedBuildings.filter(b => !role.buildings.includes(b));
      if (outside.length) return { denied: outside };
    }
    return { items: items.filter(it => !it.scope.buildings.length || it.scope.buildings.every(b => role.buildings.includes(b))) };
  }
  if (requestedBuildings && requestedBuildings.length){
    const outside = requestedBuildings.filter(b => !role.buildings.includes(b));
    if (outside.length) return { denied: outside };
    return { items: items.filter(it => !it.scope.buildings.length || it.scope.buildings.some(b => requestedBuildings.includes(b))) };
  }
  return { items };
}
function keyNumbersFor(roleKey){
  const items = URGENT[roleKey];
  const p01 = items.filter(i => i.priority === 'P0' || i.priority === 'P1').length;
  const p0 = items.filter(i => i.priority === 'P0').length;
  const nums = [
    { key:'urgent_p01', label: roleKey === 'bsup' ? 'A 楼 P0 / P1' : 'P0 / P1 待办', value:p01, unit:'项', tone:'bad',
      sub:`P0 ${p0} · P1 ${p01 - p0}`, delta:'较昨日 +1', drilldownKey:'urgent?priority=P0,P1',
      derivation:'count(urgent where scope∈principal and priority in (P0,P1))', actionable:true },
    { key:'urgent_p0', label:'P0 待确认', value:p0, unit:'项', tone:'bad', sub:'5 分钟内确认', delta:'较昨日 0',
      drilldownKey:'urgent?priority=P0', derivation:'count(urgent where priority=P0)', actionable:true }
  ];
  if (roleKey === 'duty'){
    const done = PATROL.filter(p => p.done).length;
    nums.push({ key:'patrol_done', label:'巡检完成率', value:`${done}/${PATROL.length}`, unit:'点', tone:'warn',
      sub:`漏检 ${PATROL.filter(p => !p.done && /漏检/.test(p.note || '')).length} · 异常 ${PATROL.filter(p => /异常/.test(p.note || '')).length}`,
      delta:'本班新增 1', drilldownKey:'ops:patrol|duty', derivation:'count(patrol done)/count(patrol total) where shift=current', actionable:true });
  }
  if (roleKey === 'manager' || roleKey === 'bsup'){
    const al = ALARMS[roleKey] || [];
    nums.push({ key:'alarm_key', label: roleKey === 'bsup' ? 'A 楼关键告警' : '关键告警', value:al.length, unit:'条', tone:'bad',
      sub:`未确认 ${al.filter(a => a.st === '未确认').length}`, delta:'较昨日 -1', drilldownKey:'ops:alarm|' + roleKey,
      derivation:'count(alarm where level<=P1 and scope∈principal)', actionable:true });
  }
  nums.push({ key:'closed_week', label:'本周已闭环', value:9, unit:'项', tone:'ok', sub:'历史口径', delta:'', drilldownKey:null,
    derivation:'offline warehouse snapshot', derivationNote:'数仓历史快照，无同口径明细', actionable:false });
  return nums;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const q = url.searchParams;
  if (req.method === 'OPTIONS') return send(res, 204, {});
  const aid = auditId();
  const hdr = { 'x-mock-audit-id': aid };

  if (url.pathname === '/api/workbench/_mock/help'){
    return send(res, 200, {
      note: 'mock-only endpoints; production derives identity from signed session',
      endpoints: [
        'GET /api/workbench/bootstrap?role=<manager|bsup|beng|duty|plead|gadmin>',
        'GET /api/workbench/sections/urgent?role=&snapshotId=&sourceStatus=&buildings=',
        'GET /api/workbench/drilldown?drilldownKey=&snapshotId=&role=',
        'GET /api/workbench/_mock/help'
      ],
      mockOnlyParams: { role: 'identity switch', sourceStatus: 'ok|stale|down|deny', buildings: 'request intent (narrow-only)' },
      snapshotId: SNAPSHOT, policyVersion: POLICY
    }, hdr);
  }

  const roleKey = roleOf(req, q);
  if (!roleKey) return send(res, 400, { ok:false, error:'unknown mock role', reasonCode:'DENY_ROLE_UNMAPPED' }, hdr);
  const role = ROLES[roleKey];
  const reqBuildings = q.get('buildings') ? q.get('buildings').split(',') : null;
  const snap = q.get('snapshotId');
  if (snap && snap !== SNAPSHOT) return send(res, 409, { ok:false, reasonCode:'SNAPSHOT_MISMATCH', expected:SNAPSHOT, got:snap, auditId:aid }, hdr);

  /* ── bootstrap ── */
  if (url.pathname === '/api/workbench/bootstrap'){
    return send(res, 200, {
      identity: {
        subjectId: 'ou_mock_' + roleKey, name: { manager:'周建国', bsup:'刘小杏', beng:'陈志远', duty:'张伟', plead:'李世龙', gadmin:'王敏' }[roleKey],
        roleIds: [role.code], roleLabels: [role.label],
        scope: { campus:'Nantong', buildings: role.buildings, professions: role.professions, stations: role.stations, shift: role.shift || null },
        scopeFixed: role.fixed, capabilities: role.caps,
        orgSnapshot: { groups: [role.label], post: role.label, snapshotAt: SNAPSHOT_AT },
        policyVersion: POLICY
      },
      keyNumbers: keyNumbersFor(roleKey),
      urgentTop: URGENT[roleKey].slice(0, 3),
      sectionStates: { urgent:'ok', ops:'ok', biz:'ok', people:'ok' },
      sync: { sourceStatus:'ok', lastSuccessAt: LAST_SUCCESS }
    }, hdr);
  }

  /* ── sections/urgent ── */
  if (url.pathname === '/api/workbench/sections/urgent'){
    const ss = q.get('sourceStatus') || 'ok';
    if (ss === 'down') return send(res, 503, { ok:false, reasonCode:'FAIL_CLOSED', retryAfterSec:30, lastSuccessAt:LAST_SUCCESS, auditId:aid }, hdr);
    if (ss === 'deny') return send(res, 403, { ok:false, reasonCode:'DENY_SCOPE_MISMATCH', explanation:EXPLAIN(role, '紧急待办', 'VIEW', role.fixed ? role.buildings.join('/') : '全园区', '请求范围越界', 'DENY_SCOPE_MISMATCH'), auditId:aid }, hdr);
    const f = filterByScope(URGENT[roleKey], role, reqBuildings);
    if (f.denied) return send(res, 403, { ok:false, reasonCode:'DENY_SCOPE_MISMATCH', explanation:EXPLAIN(role, '紧急待办', 'VIEW', role.buildings.join('/'), 'buildingId=' + f.denied.join('/'), 'DENY_SCOPE_MISMATCH'), auditId:aid }, hdr);
    let items = f.items;
    const pri = q.get('priority');
    if (pri) items = items.filter(i => pri.split(',').includes(i.priority));
    items = items.slice().sort((a, b) => {
      const po = { P0:0, P1:1, P2:2, P3:3 };
      const od = (x) => x.status === 'overdue' ? 0 : 1;
      return po[a.priority] - po[b.priority] || od(a) - od(b) || String(a.dueAt).localeCompare(String(b.dueAt));
    });
    return send(res, 200, {
      key:'urgent', snapshotId:SNAPSHOT,
      sourceStatus: ss === 'stale' ? 'stale' : 'ok',
      lastSuccessAt: ss === 'stale' ? '2026-11-10T22:14:00+08:00' : LAST_SUCCESS,
      total: items.length, page: { size:20, next:null }, items
    }, hdr);
  }

  /* ── drilldown ── */
  if (url.pathname === '/api/workbench/drilldown'){
    const k = q.get('drilldownKey') || '';
    let items = [], label = k;
    if (k === 'urgent?priority=P0,P1') items = URGENT[roleKey].filter(i => i.priority === 'P0' || i.priority === 'P1');
    else if (k === 'urgent?priority=P0') items = URGENT[roleKey].filter(i => i.priority === 'P0');
    else if (k === 'ops:patrol|duty') items = PATROL.map(p => ({ id:p.id, title:p.title, status:p.done ? 'done' : 'pending', note:p.note || '' }));
    else if (k.startsWith('ops:alarm|')){ const rk = k.slice(10); items = (ALARMS[rk] || []).map(a => ({ id:a.id, title:a.title, level:a.level, status:a.st })); }
    else return send(res, 404, { ok:false, reasonCode:'UNKNOWN_DRILLDOWN_KEY', auditId:aid }, hdr);
    return send(res, 200, { snapshotId:SNAPSHOT, drilldownKey:k, label, total:items.length, items }, hdr);
  }

  return send(res, 404, { ok:false, reasonCode:'UNKNOWN_ROUTE', auditId:aid }, hdr);
});

server.listen(PORT, () => {
  console.log('个人工作台 mock server · 零依赖 · 固定 fixtures');
  console.log('  http://localhost:' + PORT + '/api/workbench/bootstrap?role=bsup');
  console.log('  http://localhost:' + PORT + '/api/workbench/sections/urgent?role=bsup');
  console.log('  http://localhost:' + PORT + '/api/workbench/drilldown?drilldownKey=urgent?priority=P0,P1&role=bsup');
  console.log('  http://localhost:' + PORT + '/api/workbench/_mock/help');
  console.log('mock-only: role / sourceStatus / buildings 查询参数；snapshotId=' + SNAPSHOT);
});
