
'use strict';
/* SVG sprite 引用前缀：用常量拼接，避免被静态扫描误判为页内锚点 */
const HASH = '#';
/* ══════════════════════════════════════════════════════════════
   数据层 —— 对齐《南通全景运维平台差异化个人工作台规划方案 V0.1》
   角色取 §6.1/§8.8；待办对象取 §8.3 统一字段（含 reason 可解释）；
   排序取 §8.7（priority → 逾期 → dueAt）；来源状态取 §8.5/§8.6。
   全部为固定模拟数据，页面标注「演示数据 · 规划示意｜非生产权限」。
   ══════════════════════════════════════════════════════════════ */
const $ = s => document.querySelector(s);
const $$ = s => Array.prototype.slice.call(document.querySelectorAll(s));

const ICON = {
  today:'i-gauge', alarm:'i-alert', event:'i-activity', env:'i-droplets', duty:'i-clock',
  purchase:'i-upload', stock:'i-server', contract:'i-pen', build:'i-wrench',
  maint:'i-wrench', repair:'i-wrench', change:'i-megaphone', patrol:'i-scan',
  cert:'i-shield', train:'i-book', handover:'i-swap', leave:'i-user',
  station:'i-server', auth:'i-shield', people:'i-users'
};
const MODULE_LABEL = {
  today_map:'今日航图', alarm:'告警中心', event:'事件管理', patrol:'巡检管理', handover:'交接班',
  purchase:'采购管理', stock:'库存管理', contract:'合同管理', build:'施工管理',
  maint:'维保检修', repair:'维修项目', change:'变更通告', schedule:'月度排班',
  org:'组织与人员', cert:'证照培训', risk:'风险事项', report:'报表中心',
  auth:'业务授权', audit:'权限与审计', station110:'110kV 站日常', duty_roster:'值班排班'
};
const MODULE_ICON = {
  today_map:'i-gauge', alarm:'i-alert', event:'i-activity', patrol:'i-scan', handover:'i-swap',
  purchase:'i-upload', stock:'i-server', contract:'i-pen', build:'i-wrench',
  maint:'i-wrench', repair:'i-wrench', change:'i-megaphone', schedule:'i-calendar',
  org:'i-users', cert:'i-shield', risk:'i-alert', report:'i-layers',
  auth:'i-shield', audit:'i-clipboard', station110:'i-server', duty_roster:'i-calendar'
};

/* ── 六类角色：身份、范围、权限来源、策略版本 ── */
const ROLES = {
  manager:{
    title:'园区经理工作台', person:'周建国', avatar:'周',
    roleCode:'PARK_MANAGER', roleLabel:'园区经理',
    scope:'南通园区 · 全部楼栋 A–E / H', scopeFixed:false,
    scopeSource:'飞书用户组「园区管理组」明确绑定', policy:'2026.08.01-01',
    sub:'全园区 · 2026 年 11 月 11 日 星期三',
    identityNote:'最高业务视图与业务审批；不含平台安全配置，110kV 站仅风险总览',
    numbers:[
      {tone:'bad', icon:'i-alert', label:'P0 / P1 待办', d:{from:'urgentP01'}, unit:'项', sub:'与下方紧急待办清单同口径', drill:'block:urgent'},
      {tone:'warn', icon:'i-activity', label:'关键告警', d:{from:'opsDrill', key:'alarm|manager'}, unit:'条', sub:'未确认 2 · 已升级 1', drill:'ops:alarm|manager'},
      {tone:'ok', icon:'i-users', label:'今日在岗', d:{from:'people'}, unit:'人', sub:'缺岗 2 · 替班 1 已批', drill:'block:people'},
      {tone:'warn', icon:'i-pen', label:'合同 30 天内临期', d:{from:'biz', title:'合同'}, unit:'份', sub:'含 2 份待归属，不进楼栋 KPI', drill:'block:biz'}
    ],
    shortcuts:['today_map','purchase','contract','build','report','audit'],
    scNote:'仅显示已授权页面；入口携带范围参数，目标页仍由后端复核'
  },
  bsup:{
    title:'A 楼主管工作台', person:'刘小杏', avatar:'刘',
    roleCode:'BUILDING_SUPERVISOR', roleLabel:'A 楼主管',
    scope:'固定 A 楼', scopeFixed:true,
    scopeSource:'飞书用户组「A 楼主管」+ 组织节点 A', policy:'2026.08.01-01',
    sub:'A 楼 · 2026 年 11 月 11 日 星期三',
    identityNote:'不展示其他楼栋入口与数量；园区公共告警单独标注、不计入 A 楼统计',
    numbers:[
      {tone:'bad', icon:'i-alert', label:'A 楼 P0 / P1', d:{from:'urgentP01'}, unit:'项', sub:'与下方紧急待办清单同口径', drill:'block:urgent'},
      {tone:'warn', icon:'i-activity', label:'A 楼关键告警', d:{from:'opsDrill', key:'alarm|bsup'}, unit:'条', sub:'未确认 1', drill:'ops:alarm|bsup'},
      {tone:'ok', icon:'i-users', label:'A 楼在岗', d:{from:'people'}, unit:'人', sub:'请假 0 · 替班 0', drill:'block:people'},
      {tone:'warn', icon:'i-pen', label:'A 楼合同临期', d:{from:'biz', title:'合同'}, unit:'份', sub:'分摊口径仅计 A 楼金额', drill:'block:biz'}
    ],
    shortcuts:['today_map','purchase','stock','build','cert','handover'],
    scNote:'仅 A 楼授权页面；合同敏感字段按字段白名单返回'
  },
  beng:{
    title:'楼栋工程师工作台', person:'陈志远', avatar:'陈',
    roleCode:'BUILDING_ENGINEER', roleLabel:'B 楼电气工程师',
    scope:'B 楼 ∩ 电气专业 ∩ 本人负责或协同', scopeFixed:true,
    scopeSource:'飞书用户组「B 楼电气」+ 专业字段', policy:'2026.08.01-01',
    sub:'B 楼 · 电气 · 2026 年 11 月 11 日 星期三',
    identityNote:'跨专业仅见关联摘要；不能审批本人事项、强制开工或关闭重大事件',
    numbers:[
      {tone:'bad', icon:'i-wrench', label:'P0 / P1 待办（含阻断）', d:{from:'urgentP01'}, unit:'项', sub:'阻断条件未解除不开工', drill:'block:urgent'},
      {tone:'warn', icon:'i-activity', label:'本专业事件', d:{from:'ops', title:'本专业事件'}, unit:'起', sub:'1 起临近 SLA', drill:'block:ops'},
      {tone:'info', icon:'i-scan', label:'今日计划作业', d:{from:'ops', title:'今日计划作业'}, unit:'项', sub:'含 1 项检修票', drill:'block:ops'},
      {tone:'ok', icon:'i-check', label:'本周已闭环', value:'9', unit:'项', sub:'历史口径 · 非可操作 KPI', ref:true}
    ],
    shortcuts:['build','maint','repair','change','event','patrol'],
    scNote:'施工、事件、维保、检修、变更一体化闭环入口'
  },
  duty:{
    title:'值班员工作台', person:'张伟', avatar:'张',
    roleCode:'DUTY_OPERATOR', roleLabel:'值班员 · 早班',
    scope:'当前班次 07:00–15:00 ∩ A 楼 ∩ 本班组', scopeFixed:true,
    scopeSource:'排班底表 + 签到接班实例', policy:'2026.08.01-01',
    sub:'早班 · A 楼 · 2026 年 11 月 11 日 星期三',
    identityNote:'未签到/未接班或班次结束后关闭写操作；重大事件只能申请关闭',
    numbers:[
      {tone:'warn', icon:'i-scan', label:'巡检完成率', d:{from:'opsDrillDone', key:'patrol|duty'}, unit:'点', sub:'漏检 1 · 异常 1 · 与点检明细同口径', drill:'ops:patrol|duty'},
      {tone:'bad', icon:'i-activity', label:'事件 SLA 临近', d:{from:'urgentP0'}, unit:'起', sub:'与下方紧急待办清单同口径', drill:'block:urgent'},
      {tone:'warn', icon:'i-swap', label:'交班缺项', d:{from:'urgentKind', kind:'handover'}, unit:'项', sub:'照片 1 · 读数 1', drill:'block:urgent'},
      {tone:'ok', icon:'i-check', label:'当班已处置', value:'4', unit:'项', sub:'历史口径 · 非可操作 KPI', ref:true}
    ],
    shortcuts:['patrol','event','handover','duty_roster','today_map'],
    scNote:'巡检发现 → 事件处置 → 交接班留痕 的短链路'
  },
  plead:{
    title:'园区主管工程师工作台', person:'李世龙', avatar:'李',
    roleCode:'PROFESSION_LEAD', roleLabel:'园区主管工程师 · 电气',
    scope:'全园区 ∩ 电气专业（110kV 站另需独立授权包）', scopeFixed:false,
    scopeSource:'飞书用户组「电气专业主管」+ STATION_110 授权包', policy:'2026.08.01-01',
    sub:'全园区 · 电气 · 2026 年 11 月 11 日 星期三',
    identityNote:'站内内容仅监视、记录与审查；不提供分合闸、倒闸执行或定值修改',
    numbers:[
      {tone:'bad', icon:'i-alert', label:'专业 P0 风险', d:{from:'ops', title:'专业 P0 风险'}, unit:'项', sub:'110kV 站 1 · B 楼 1', drill:'block:ops'},
      {tone:'warn', icon:'i-clipboard', label:'待技术审查', d:{from:'ops', title:'待技术审查'}, unit:'项', sub:'施工 3 · 变更 2', drill:'block:ops'},
      {tone:'info', icon:'i-wrench', label:'专业检修统筹', d:{from:'ops', title:'专业检修统筹'}, unit:'项', sub:'跨 4 栋楼', drill:'block:ops'},
      {tone:'ok', icon:'i-shield', label:'两票三制合规', value:'100%', unit:'', sub:'历史口径 · 非可操作 KPI', ref:true}
    ],
    shortcuts:['station110','maint','change','build','risk','report'],
    scNote:'专业态势、会签、退回补充与催办；设备控制一律不入口'
  },
  gadmin:{
    title:'综合管理员工作台', person:'王敏', avatar:'王',
    roleCode:'GENERAL_ADMIN', roleLabel:'综合管理员',
    scope:'园区行政业务 ∩ 采购 / 合同 / 授权 / 库存域', scopeFixed:false,
    scopeSource:'飞书用户组「综合管理」', policy:'2026.08.01-01',
    sub:'园区 · 行政业务域 · 2026 年 11 月 11 日 星期三',
    identityNote:'申请人与审批人分离；不能自我授权，不默认查看无关技术运行数据',
    numbers:[
      {tone:'warn', icon:'i-upload', label:'待审核采购', d:{from:'opsDrill', key:'purchase|gadmin'}, unit:'单', sub:'2 单超 48 小时 · 与下钻明细同口径', drill:'ops:purchase|gadmin'},
      {tone:'bad', icon:'i-pen', label:'合同临期 / 付款节点', d:{from:'biz', title:'合同'}, unit:'份', sub:'7 天内 2 份', drill:'block:biz'},
      {tone:'warn', icon:'i-shield', label:'授权到期回收', d:{from:'ops', title:'授权与到期'}, unit:'项', sub:'临时权限 1 项今日到期', drill:'block:ops'},
      {tone:'info', icon:'i-clipboard', label:'库存台账异常', d:{from:'biz', title:'库存'}, unit:'条', sub:'账实不符 · 与业务进展同口径', drill:'block:biz'}
    ],
    shortcuts:['purchase','contract','auth','stock','audit','org'],
    scNote:'采购—合同—授权 跨流程协同与审计留痕'
  }
};

/* ── 紧急待办：统一待办对象（§8.3），含 reason 与 SLA（§8.7） ── */
const URGENT = {
  manager:[
    {p:'P0', sla:'剩 4 分钟确认', kind:'alarm', title:'C 楼 蓄电池室 烟感预报警 未确认', scope:'C 楼', source:'告警与风险', due:'11-11 11:20 前', dueTone:'bad', status:'待确认', statusTone:'bad', action:'确认并指派', reason:'你是园区经理；该告警等级 P0 且 15 分钟未确认将自动升级；来源：消防主机直连。', multi:['消防主机','今日航图']},
    {p:'P0', sla:'剩 11 分钟确认', kind:'event', title:'A 楼 2F 机房 漏水告警 处置超时', scope:'A 楼', source:'告警与风险', due:'11-11 11:35 前', dueTone:'bad', status:'处理中', statusTone:'warn', action:'查看处置', reason:'你是园区经理；P0 事件 SLA 为 5 分钟确认、15 分钟升级，当前已 11 分钟未闭环。'},
    {p:'P1', sla:'剩 26 分钟', kind:'contract', title:'南通云启 冷却托管合同 临期 14 天', scope:'园区', source:'合同', due:'11-25 到期', dueTone:'warn', status:'待发起续签', statusTone:'warn', action:'发起续签', reason:'你是园区经理；该合同 scopeType=CAMPUS 且未分摊到楼栋，仅园区角色可见。'},
    {p:'P1', sla:'剩 1 小时 40 分', kind:'purchase', title:'B 楼 高压开关备件 采购单 超时未审', scope:'B 楼', source:'采购', due:'今日 13:00 前', dueTone:'warn', status:'待审批', statusTone:'warn', action:'审批', reason:'你是园区经理；采购审批额度内由你终审，申请人为陈志远，职责分离校验通过。'},
    {p:'P2', sla:'当班内', kind:'build', title:'D 楼 冷塔防腐施工 许可临期 3 天', scope:'D 楼', source:'施工', due:'11-14 前', dueTone:'ok', status:'待复核', statusTone:'info', action:'复核许可', reason:'你是园区经理；施工许可到期未复核将自动停工，影响 11 月节点。'},
    {p:'P3', sla:'按计划', kind:'report', title:'10 月园区运行月报 导出申请 待复核', scope:'园区', source:'报表', due:'11-15 前', dueTone:'ok', status:'待复核', statusTone:'info', action:'复核导出', reason:'你是园区经理；导出需单次短时授权 + 范围裁剪 + 水印，本次申请范围=全园区。'}
  ],
  bsup:[
    {p:'P0', sla:'剩 4 分钟确认', kind:'alarm', title:'A 楼 4F 冷塔风机 2 号 振动越限', scope:'A 楼', source:'告警与风险', due:'11-11 11:20 前', dueTone:'bad', status:'待确认', statusTone:'bad', action:'确认并指派', reason:'你是 A 楼主管；该告警对象楼栋=A 楼；P0 需 5 分钟内确认。'},
    {p:'P1', sla:'剩 52 分钟', kind:'build', title:'A 楼 1F 高压配电室 预防性试验 节点待确认', scope:'A 楼', source:'施工', due:'今日 12:10 前', dueTone:'warn', status:'待确认', statusTone:'warn', action:'确认节点', reason:'你是 A 楼主管；该项目 project.buildingId=A；节点未确认将顺延停机窗口。'},
    {p:'P1', sla:'剩 2 小时', kind:'cert', title:'A 楼 高压进网证 2 人 30 天内到期', scope:'A 楼', source:'证照与培训', due:'12-08 前', dueTone:'warn', status:'待安排复训', statusTone:'warn', action:'安排复训', reason:'你是 A 楼主管；证照归属人员 buildingId=A；到期未复训将失去操作资质。'},
    {p:'P2', sla:'当班内', kind:'handover', title:'夜班交接 2 项未闭环（水表照片 / 风机异响）', scope:'A 楼', source:'值班', due:'今日 15:00 前', dueTone:'ok', status:'待签收', statusTone:'info', action:'签收交接', reason:'你是 A 楼主管；交接对象班组=A 楼早班；未签收前责任仍挂在夜班。'},
    {p:'P3', sla:'按计划', kind:'stock', title:'A 楼 低库存 3 项（兆欧表电池 / 表笔 / 保险丝）', scope:'A 楼', source:'库存', due:'11-13 前', dueTone:'ok', status:'待补货', statusTone:'info', action:'发起补货', reason:'你是 A 楼主管；库存仓位责任楼栋=A；低于安全库存阈值触发。'}
  ],
  beng:[
    {p:'P0', sla:'剩 9 分钟确认', kind:'build', title:'B 楼 2F UPS 模块更换 阻断条件未解除', scope:'B 楼 · 电气', source:'施工', due:'阻断中', dueTone:'bad', status:'阻断', statusTone:'bad', action:'查看阻断条件', reason:'你是 B 楼电气工程师；该施工对象楼栋=B 且专业=电气，且你是施工责任人；阻断未解除不提供开工按钮。'},
    {p:'P1', sla:'剩 22 分钟', kind:'event', title:'B 楼 2F 机房 温湿度越限 事件接单', scope:'B 楼 · 电气', source:'事件', due:'11-11 11:40 前', dueTone:'bad', status:'待接单', statusTone:'bad', action:'接单', reason:'你是 B 楼电气工程师；事件对象楼栋=B、专业=电气，且当前在岗当班。', multi:['事件','告警']},
    {p:'P2', sla:'当班内', kind:'maint', title:'B 楼 3F 配电柜 绝缘复测 证据回填', scope:'B 楼 · 电气', source:'维保检修', due:'今日 15:00 前', dueTone:'warn', status:'待回填', statusTone:'warn', action:'回填证据', reason:'你是 B 楼电气工程师；该检修单负责人=本人；缺兆欧表读数照片不能申请闭环。'},
    {p:'P2', sla:'当班内', kind:'change', title:'B 楼 2F 气流遏制盲板补装 变更会签', scope:'B 楼 · 电气', source:'施工变更', due:'今日 16:00 前', dueTone:'warn', status:'待会签', statusTone:'info', action:'专业会签', reason:'你是 B 楼电气工程师；变更涉及电气专业，RACI 中你为 C（征询），仅会签不审批。'},
    {p:'P3', sla:'按计划', kind:'patrol', title:'B 楼 电气专业 周巡 4 点 待执行', scope:'B 楼 · 电气', source:'巡检', due:'11-13 前', dueTone:'ok', status:'待执行', statusTone:'info', action:'开始巡检', reason:'你是 B 楼电气工程师；巡检计划专业=电气、楼栋=B，且本人为指定执行人。'}
  ],
  duty:[
    {p:'P0', sla:'剩 22 分钟', kind:'event', title:'A 楼 1F 消防泵房 压力波动 事件接单', scope:'A 楼', source:'事件', due:'11-11 11:40 前', dueTone:'bad', status:'待接单', statusTone:'bad', action:'接单', reason:'你是 A 楼早班值班员；事件当班区域=A 楼且已签到接班；P1 事件 SLA 30 分钟确认。'},
    {p:'P1', sla:'剩 48 分钟', kind:'patrol', title:'A 楼 4F 冷塔平台 巡检点 3 漏检', scope:'A 楼', source:'巡检', due:'今日 12:00 前', dueTone:'warn', status:'漏检', statusTone:'warn', action:'补巡检', reason:'你是 A 楼早班值班员；巡检计划班组=早班 A 组；漏检点在本班时间窗内。'},
    {p:'P1', sla:'剩 1 小时 20 分', kind:'handover', title:'A 楼 3F 生活水表 读数缺照片 交班缺项', scope:'A 楼', source:'值班', due:'今日 15:00 前', dueTone:'warn', status:'待补录', statusTone:'warn', action:'补录证据', reason:'你是 A 楼早班值班员；该记录责任人=本人；缺照片将形成交班缺项并阻塞水耗归档。'},
    {p:'P2', sla:'当班内', kind:'patrol', title:'A 楼 1F 配电间 红外测温 异常复测', scope:'A 楼', source:'巡检', due:'今日 14:00 前', dueTone:'ok', status:'待复测', statusTone:'info', action:'复测上传', reason:'你是 A 楼早班值班员；巡检发现异常转复测任务，指派给本人。'},
    {p:'P3', sla:'按计划', kind:'handover', title:'交接班登记 本班 4 项处置留痕核对', scope:'A 楼', source:'值班', due:'今日 14:40 前', dueTone:'ok', status:'待核对', statusTone:'info', action:'核对交班', reason:'你是 A 楼早班值班员；交接班需在本班结束前完成签收，未交接进入待处理队列。'}
  ],
  plead:[
    {p:'P0', sla:'剩 6 分钟确认', kind:'station', title:'110kV 站 2 号主变 油温越限 预判', scope:'STATION_110', source:'告警与风险', due:'11-11 11:24 前', dueTone:'bad', status:'待研判', statusTone:'bad', action:'进入研判', reason:'你是电气专业主管且命中独立 STATION_110 授权包；该告警站点=STATION_110；仅研判与记录，不含设备控制。'},
    {p:'P1', sla:'剩 1 小时 10 分', kind:'change', title:'C 楼 1F 2 号冷机检修 变更技术审查', scope:'C 楼 · 电气', source:'施工变更', due:'今日 12:30 前', dueTone:'warn', status:'待审查', statusTone:'warn', action:'技术审查', reason:'你是电气专业主管；变更专业=电气，RACI 中你为 A（最终技术负责），可退回补充。'},
    {p:'P1', sla:'剩 3 小时', kind:'maint', title:'全园区 电气专业 检修计划 冲突 2 处', scope:'全园区 · 电气', source:'维保检修', due:'今日 14:00 前', dueTone:'warn', status:'待统筹', statusTone:'info', action:'统筹调整', reason:'你是电气专业主管；范围=全园区∩电气；两处检修窗口重叠需专业统筹。'},
    {p:'P2', sla:'当班内', kind:'risk', title:'B 楼 电气井 电缆桥架锈蚀 风险升级复核', scope:'B 楼 · 电气', source:'风险事项', due:'今日 17:00 前', dueTone:'ok', status:'待复核', statusTone:'info', action:'复核升级', reason:'你是电气专业主管；风险对象专业=电气；楼栋工程师申请升级至 P1，需专业复核。'},
    {p:'P3', sla:'按计划', kind:'report', title:'电气专业 月度技术监督报告 会签', scope:'全园区 · 电气', source:'报表', due:'11-14 前', dueTone:'ok', status:'待会签', statusTone:'info', action:'会签', reason:'你是电气专业主管；报告范围=全园区∩电气；会签不产生审批权。'}
  ],
  gadmin:[
    {p:'P1', sla:'剩 40 分钟', kind:'purchase', title:'办公区 打印机租赁 采购单 资料缺件', scope:'园区', source:'采购', due:'今日 12:00 前', dueTone:'warn', status:'待补件', statusTone:'warn', action:'催办补件', reason:'你是综合管理员；采购域在授权包内；缺供应商资质附件不能进入审批。'},
    {p:'P1', sla:'剩 2 小时', kind:'contract', title:'保洁服务合同 付款节点 7 天内', scope:'园区', source:'合同', due:'11-18 付款', dueTone:'warn', status:'待发起付款', statusTone:'warn', action:'发起付款流程', reason:'你是综合管理员；合同域在授权包内；付款经办人≠付款审批人，你仅经办。'},
    {p:'P1', sla:'今日到期', kind:'auth', title:'施工队 临时进场授权 今日 18:00 到期', scope:'园区', source:'业务授权', due:'今日 18:00', dueTone:'bad', status:'待回收确认', statusTone:'warn', action:'确认回收', reason:'你是综合管理员；授权包有效期至今日 18:00；到期未确认将自动回收并失效缓存。'},
    {p:'P2', sla:'当班内', kind:'stock', title:'行政仓 库存台账 2 条账实不符', scope:'园区', source:'库存', due:'今日 17:00 前', dueTone:'ok', status:'待核查', statusTone:'info', action:'核查台账', reason:'你是综合管理员；库存域在授权包内；盘盈盘亏审批人不能是本次登记人。'},
    {p:'P3', sla:'按计划', kind:'audit', title:'10 月业务授权审计 3 条异常 说明', scope:'园区', source:'权限与审计', due:'11-15 前', dueTone:'ok', status:'待说明', statusTone:'info', action:'填写说明', reason:'你是综合管理员；审计域只读 + 说明；审计日志不可编辑或覆盖。'}
  ]
};

/* ── 今日运行：值守 / 告警 / 事件 / 设备与环境（§8.1 第 3 块） ── */
const OPS = {
  manager:{ src:'ok', cards:[
    {icon:'i-clock', title:'值守状态', line:['21/23','人在岗 · 缺岗 2'], tone:'warn', list:[{tone:'warn',text:'<b>C 楼</b> 缺岗 1 · 替班审批中'},{tone:'warn',text:'<b>H 楼</b> 缺岗 1 · 已升级主管'},{tone:'ok',text:'A / B / D / E 楼到岗率 100%'}], foot:'来源：排班 + 签到接班实例 · 3 分钟前'},
    {icon:'i-alert', title:'关键告警', line:['4','条 · 未确认 2'], tone:'bad', drill:'ops|alarm|manager', list:[{tone:'bad',text:'<b>P0</b> C 楼 烟感预报警 未确认'},{tone:'bad',text:'<b>P0</b> A 楼 漏水告警 处置超时'},{tone:'warn',text:'<b>P1</b> B 楼 风机振动越限'}], chart:{v:[3,5,2,6,4,7,4], l:['11-05','11-06','11-07','11-08','11-09','11-10','11-11'], u:' 条'}, foot:'园区公共告警按 appliesToBuildingIds 标注影响楼栋'},
    {icon:'i-activity', title:'处理中事件', line:['6','起 · 超 SLA 1'], tone:'warn', list:[{tone:'bad',text:'<b>EV-1111-0339</b> A 楼漏水 超 SLA'},{tone:'warn',text:'<b>EV-1111-0342</b> B 楼温湿度 处理中'}], foot:'点击数字进入同口径事件明细（drilldownKey）'},
    {icon:'i-droplets', title:'设备与环境摘要', line:['98.2%','设备可用率'], tone:'ok', list:[{tone:'ok',text:'冷源 N+1 冗余正常'},{tone:'warn',text:'<b>C 楼</b> 2 号冷机 检修中 降级运行'}], foot:'环境均值 23.4℃ / 45%RH · 航图口径'}
  ]},
  bsup:{ src:'ok', cards:[
    {icon:'i-clock', title:'A 楼值守', line:['6/6','人在岗'], tone:'ok', list:[{tone:'ok',text:'早班 签到接班完成 07:02'},{tone:'ok',text:'无请假 / 替班'}], foot:'仅 A 楼班组；不返回其他楼栋人数'},
    {icon:'i-alert', title:'A 楼关键告警', line:['2','条 · 未确认 1'], tone:'bad', drill:'ops|alarm|bsup', list:[{tone:'bad',text:'<b>P0</b> 4F 冷塔风机 2 号 振动越限'},{tone:'warn',text:'<b>P1</b> 1F 精密空调 4 号 除霜循环'}], chart:{v:[1,2,0,3,1,2,2], l:['11-05','11-06','11-07','11-08','11-09','11-10','11-11'], u:' 条'}, foot:'明细仅含 buildingId=A 的告警'},
    {icon:'i-activity', title:'A 楼处理中事件', line:['2','起'], tone:'warn', list:[{tone:'warn',text:'<b>EV-1111-0339</b> 2F 漏水 复盘待确认'},{tone:'ok',text:'<b>EV-1110-0311</b> 3F 门禁 已闭环'}], foot:'明细仅含 buildingId=A 的事件'},
    {icon:'i-droplets', title:'A 楼设备与环境', line:['97.6%','设备可用率'], tone:'ok', list:[{tone:'ok',text:'23.1℃ / 44%RH · 阈值内'},{tone:'warn',text:'4F 冷塔风机 2 号 异响待复听'}], foot:'来源：A 楼航图分区 · 与明细同口径'}
  ]},
  beng:{ src:'ok', cards:[
    {icon:'i-clock', title:'当班状态', line:['在岗','早班 · 已签到接班'], tone:'ok', list:[{tone:'ok',text:'签到 07:01 · 接班 07:05'},{tone:'warn',text:'资质：高压进网证 2027-03 到期'}], foot:'当班实例有效才开放 EXECUTE 动作'},
    {icon:'i-alert', title:'本专业阻断与告警', line:['2','项阻断'], tone:'bad', list:[{tone:'bad',text:'<b>UPS 模块更换</b> 停电窗口未批'},{tone:'warn',text:'<b>3F 配电柜</b> 绝缘 0.8MΩ 偏低'}], foot:'阻断条件未解除时不出现「开工」按钮'},
    {icon:'i-activity', title:'本专业事件', line:['3','起 · 1 起临近 SLA'], tone:'warn', list:[{tone:'bad',text:'<b>EV-1111-0342</b> 2F 温湿度 待接单'},{tone:'ok',text:'<b>EV-1110-0298</b> 1F 照明 已闭环'}], foot:'仅 B 楼 ∩ 电气 的事件'},
    {icon:'i-scan', title:'今日计划作业', line:['4','项 · 含 1 检修票'], tone:'info', list:[{tone:'info',text:'09:30 3F 配电柜 绝缘复测'},{tone:'info',text:'13:30 2F 盲板补装 会签'}], foot:'作业票与巡检计划来自原模块深链'}
  ]},
  duty:{ src:'ok', cards:[
    {icon:'i-clock', title:'本班状态', line:['07:00–15:00','已过 4 小时 10 分'], tone:'ok', list:[{tone:'ok',text:'签到 06:58 · 接班 07:00'},{tone:'warn',text:'交接遗留 2 项待签收'}], foot:'班次结束后写操作自动关闭'},
    {icon:'i-scan', title:'巡检进度', line:['5/8','点 · 异常 1'], tone:'warn', drill:'ops|patrol|duty', list:[{tone:'warn',text:'<b>4F 冷塔平台</b> 点 3 漏检'},{tone:'bad',text:'<b>1F 配电间</b> 红外异常待复测'},{tone:'ok',text:'其余 4 点正常留痕'}], foot:'扫码点检记录含照片与时间戳'},
    {icon:'i-activity', title:'当班事件', line:['1','起 临近 SLA'], tone:'bad', list:[{tone:'bad',text:'<b>EV-1111-0351</b> 消防泵房压力波动 待接单'}], foot:'重大事件只能申请关闭，不能直接闭环'},
    {icon:'i-droplets', title:'当班环境摘要', line:['23.2℃','/ 45%RH'], tone:'ok', list:[{tone:'ok',text:'A 楼各机房阈值内'},{tone:'warn',text:'4F 冷塔 风机 2 号 异响待复听'}], foot:'仅当班区域 A 楼'}
  ]},
  plead:{ src:'ok', cards:[
    {icon:'i-server', title:'110kV 站状态（只读）', line:['2','台主变运行'], tone:'warn', list:[{tone:'bad',text:'<b>2 号主变</b> 油温 78℃ 越限预判'},{tone:'ok',text:'进线 双回路正常'},{tone:'ok',text:'保护记录 本月 0 动作'}], foot:'仅监视/记录/审查；无分合闸、倒闸、定值入口'},
    {icon:'i-alert', title:'专业 P0 风险', line:['2','项'], tone:'bad', list:[{tone:'bad',text:'<b>110kV</b> 2 号主变 油温趋势'},{tone:'bad',text:'<b>B 楼</b> 电气井 桥架锈蚀'}], foot:'全园区 ∩ 电气 的风险对象'},
    {icon:'i-clipboard', title:'待技术审查', line:['5','项'], tone:'warn', list:[{tone:'warn',text:'施工 3 · 变更 2'},{tone:'info',text:'其中 1 项需退回补充材料'}], foot:'审查意见留痕并进入审计'},
    {icon:'i-wrench', title:'专业检修统筹', line:['7','项 · 跨 4 栋楼'], tone:'info', list:[{tone:'warn',text:'窗口冲突 2 处待统筹'},{tone:'ok',text:'两票三制 34 票无缺项'}], foot:'统筹不产生最终审批权'}
  ]},
  gadmin:{ src:'ok', cards:[
    {icon:'i-clock', title:'行政在岗', line:['4/4','人在岗'], tone:'ok', list:[{tone:'ok',text:'综合管理组全员在岗'}], foot:'不含运维班组人员明细'},
    {icon:'i-upload', title:'采购流程在途', line:['12','单 · 待审 8'], tone:'warn', drill:'ops|purchase|gadmin', list:[{tone:'warn',text:'2 单超 48 小时未审'},{tone:'ok',text:'到货异常 0'}], chart:{v:[6,9,7,11,8,12], l:['6月','7月','8月','9月','10月','11月'], u:' 单'}, foot:'仅行政与授权域采购'},
    {icon:'i-pen', title:'合同履约节点', line:['5','份 7–30 天'], tone:'warn', list:[{tone:'bad',text:'<b>保洁服务</b> 11-18 付款节点'},{tone:'warn',text:'<b>打印机租赁</b> 缺资质附件'}], foot:'金额按字段白名单返回，原件另授权'},
    {icon:'i-shield', title:'授权与到期', line:['3','项待回收'], tone:'warn', list:[{tone:'bad',text:'施工队临时进场 今日 18:00 到期'},{tone:'info',text:'2 项系统临时权限 11-13 到期'}], foot:'到期自动回收并失效缓存'}
  ]}
};

/* ── 业务进展：采购 / 库存 / 合同 / 施工（或专业域）（§8.1 第 4 块） ── */
const BIZ = {
  manager:{ note:'全园区口径；楼栋视图按各自分摊，园区按记录去重', src:'ok', cards:[
    {icon:'i-upload', title:'采购', line:['12','单在途 · 待审 8'], tone:'warn', list:[{tone:'warn',text:'超 48h 未审 2 单'},{tone:'ok',text:'到货异常 0'}], foot:'明细 12 条 = 卡片 12 单'},
    {icon:'i-server', title:'库存', line:['6','项低库存'], tone:'warn', list:[{tone:'warn',text:'跨楼调拨 2 项在途'}], foot:'调拨保留来源与去向两个维度'},
    {icon:'i-pen', title:'合同', line:['6','份 30 天内临期'], tone:'warn', list:[{tone:'bad',text:'待归属 2 份 · 仅园区可见'},{tone:'warn',text:'共享合同 1 份 不计楼栋 KPI'}], foot:'owningBuildingIds 治理中'},
    {icon:'i-wrench', title:'施工', line:['9','项目 · 风险 3'], tone:'warn', list:[{tone:'bad',text:'许可临期 1 · 材料阻塞 2'}], foot:'风险排行按业务影响排序'}
  ]},
  bsup:{ note:'仅 A 楼；园区公共事项只展示影响 A 的部分', src:'ok', cards:[
    {icon:'i-upload', title:'采购（A 楼）', line:['3','单在途'], tone:'warn', list:[{tone:'warn',text:'高压开关备件 待园区终审'}], foot:'不含其他楼栋采购数量'},
    {icon:'i-server', title:'库存（A 楼）', line:['3','项低库存'], tone:'warn', list:[{tone:'warn',text:'兆欧表电池 / 表笔 / 保险丝'}], foot:'领用异常 0'},
    {icon:'i-pen', title:'合同（A 楼）', line:['2','份临期'], tone:'warn', list:[{tone:'warn',text:'分摊金额仅计 A 楼部分'},{tone:'info',text:'敏感字段按白名单返回'}], foot:'待归属合同默认不对楼栋角色开放'},
    {icon:'i-wrench', title:'施工（A 楼）', line:['2','项目'], tone:'ok', list:[{tone:'warn',text:'预防性试验 节点待确认'}], foot:'许可 / 材料 / 验收 均限 A 楼'}
  ]},
  beng:{ note:'B 楼 ∩ 电气；跨专业仅关联摘要', src:'ok', cards:[
    {icon:'i-wrench', title:'施工（本人）', line:['2','项 · 1 阻断'], tone:'bad', list:[{tone:'bad',text:'UPS 模块更换 停电窗口未批'}], foot:'阻断未解除不出现开工按钮'},
    {icon:'i-wrench', title:'维保检修', line:['3','单'], tone:'warn', list:[{tone:'warn',text:'绝缘复测 待证据回填'}], foot:'申请闭环需证据齐全'},
    {icon:'i-megaphone', title:'变更', line:['1','项待会签'], tone:'info', list:[{tone:'info',text:'盲板补装 · RACI=C 仅会签'}], foot:'会签不产生审批权'},
    {icon:'i-upload', title:'采购（本人需求）', line:['1','单'], tone:'ok', list:[{tone:'ok',text:'表笔 已到货待领'}], foot:'仅本人发起或协办'}
  ]},
  duty:{ note:'当班范围；不含经营与合同数据', src:'ok', cards:[
    {icon:'i-scan', title:'巡检', line:['5/8','点'], tone:'warn', list:[{tone:'warn',text:'漏检 1 · 异常 1'}], foot:'补巡检在本班时间窗内'},
    {icon:'i-activity', title:'事件', line:['1','起待接单'], tone:'bad', list:[{tone:'bad',text:'消防泵房压力波动'}], foot:'接单后进入处置留痕链'},
    {icon:'i-swap', title:'交接班', line:['2','缺项'], tone:'warn', list:[{tone:'warn',text:'水表照片 / 读数各 1'}], foot:'未签收前责任挂夜班'},
    {icon:'i-clock', title:'本班任务', line:['4','项待办'], tone:'info', list:[{tone:'info',text:'复测 / 补录 / 核对 / 接单'}], foot:'任务来自统一待办聚合'}
  ]},
  plead:{ note:'全园区 ∩ 电气；110kV 站需独立授权包', src:'ok', cards:[
    {icon:'i-wrench', title:'检修统筹', line:['7','项'], tone:'info', list:[{tone:'warn',text:'窗口冲突 2 处'}], foot:'统筹意见留痕'},
    {icon:'i-megaphone', title:'变更技术审查', line:['2','项'], tone:'warn', list:[{tone:'warn',text:'C 楼冷机检修 待审查'}], foot:'可退回补充'},
    {icon:'i-server', title:'110kV 站（只读）', line:['2','主变'], tone:'warn', list:[{tone:'bad',text:'2 号主变 油温趋势关注'}], foot:'无设备控制入口'},
    {icon:'i-shield', title:'专业风险', line:['2','项 P0'], tone:'bad', list:[{tone:'bad',text:'桥架锈蚀 待升级复核'}], foot:'升级复核不代替值班处置'}
  ]},
  gadmin:{ note:'采购 / 合同 / 授权 / 库存域；不含技术运行明细', src:'ok', cards:[
    {icon:'i-upload', title:'采购', line:['8','单待审'], tone:'warn', list:[{tone:'warn',text:'缺件 1 · 超期 2'}], foot:'申请人与审批人分离'},
    {icon:'i-pen', title:'合同', line:['5','节点'], tone:'warn', list:[{tone:'bad',text:'7 天内付款 2 份'}], foot:'付款经办≠付款审批'},
    {icon:'i-shield', title:'业务授权', line:['3','到期'], tone:'warn', list:[{tone:'bad',text:'临时进场 今日 18:00'}], foot:'不能自我授权'},
    {icon:'i-server', title:'库存（行政仓）', line:['2','账实不符'], tone:'info', list:[{tone:'info',text:'待核查并留痕'}], foot:'盘亏审批人≠登记人'}
  ]}
};

/* ── 人员与值班：在岗 / 请假替班 / 交接 / 证照培训 + 我的日程（§8.1 第 5 块） ── */
const PEOPLE = {
  manager:{ src:'ok', left:{title:'今日在岗与缺岗', meter:[21,23], meterLabel:'在岗 21 / 应到 23', list:[{tone:'warn',text:'<b>C 楼</b> 缺岗 1 · 替班审批中（李静代张伟 夜班）· 联系 <span class="masked">138****4021</span>'},{tone:'warn',text:'<b>H 楼</b> 缺岗 1 · 已升级楼栋主管'},{tone:'ok',text:'请假 0 · 外包进场 3 人均有授权'}], foot:'汇总口径仅用于配置分析，不参与个人授权'}, right:{title:'证照与培训（园区）', list:[{tone:'warn',text:'高压进网证 30 天内到期 <b>4 人</b>'},{tone:'warn',text:'必修培训未完成 <b>6 人</b>'},{tone:'ok',text:'应急演练 11-14 已排 23 人'}], foot:'敏感字段（证号）不返回'}},
  bsup:{ src:'ok', left:{title:'A 楼在岗', meter:[6,6], meterLabel:'在岗 6 / 应到 6', list:[{tone:'ok',text:'早班 6 人全部签到接班'},{tone:'ok',text:'请假 0 · 替班 0'}], foot:'仅 A 楼班组；联系方式按需脱敏'}, right:{title:'A 楼证照与交接', list:[{tone:'warn',text:'高压进网证 30 天内到期 <b>2 人</b>'},{tone:'warn',text:'夜班交接 <b>2 项</b>未签收'},{tone:'ok',text:'本周培训完成 5 / 6'}], foot:'交接未签收前责任挂夜班'}},
  beng:{ src:'ok', left:{title:'本人当班与资质', meter:[1,1], meterLabel:'当班有效 · 已签到接班', list:[{tone:'ok',text:'早班 07:01 签到 · 07:05 接班'},{tone:'warn',text:'高压进网证 2027-03 到期（90 天提醒）'}], foot:'资质失效即回收 EXECUTE 动作'}, right:{title:'协同与交接', list:[{tone:'info',text:'协同人：张伟（值班）· 刘小杏（主管）'},{tone:'ok',text:'昨日交接 0 缺项'}], foot:'仅显示与本人任务相关的协同人'}},
  duty:{ src:'ok', left:{title:'本班组在岗', meter:[3,3], meterLabel:'早班 A 组 3 / 3', list:[{tone:'ok',text:'张伟 · 李强 · 赵敏 均已签到'},{tone:'ok',text:'替班 0'}], foot:'班组最小字段；不返回全园区人员'}, right:{title:'交接与证照', list:[{tone:'warn',text:'夜班遗留 <b>2 项</b>待签收'},{tone:'warn',text:'本人 消防操作证 12-01 到期'},{tone:'ok',text:'班前会 09:00 已参加'}], foot:'我的日程：10:30 风机复听 · 14:00 交接预检'}},
  plead:{ src:'ok', left:{title:'专业人员在岗', meter:[9,10], meterLabel:'电气专业 在岗 9 / 10', list:[{tone:'warn',text:'<b>B 楼</b> 1 人请假 · 替班已批 · 联系 <span class="masked">139****7788</span>'},{tone:'ok',text:'110kV 站 当班 2 人资质有效'}], foot:'专业人员仅看关联摘要'}, right:{title:'专业证照与审查', list:[{tone:'warn',text:'电气试验资质 30 天内到期 <b>1 人</b>'},{tone:'ok',text:'两票签发人 6 人全部有效'}], foot:'资质到期回收对应敏感动作'}},
  gadmin:{ src:'ok', left:{title:'行政组在岗', meter:[4,4], meterLabel:'综合管理 4 / 4', list:[{tone:'ok',text:'全员在岗 · 请假 0'}], foot:'不含运维班组明细'}, right:{title:'授权与培训', list:[{tone:'bad',text:'施工队临时进场 <b>今日 18:00</b> 到期'},{tone:'warn',text:'合同管理必修 1 人未完成'},{tone:'ok',text:'审计说明模板 已更新 V2'}], foot:'授权到期自动回收'}},
};

/* ── 权限解释（§9.2 权限说明抽屉）与拒绝演示（§8.10.11 模板） ── */
const PERM = {
  manager:['角色来源：<b>飞书用户组「园区管理组」</b>明确绑定，组织属性仅作辅助校验。','数据范围：<b>南通园区全部楼栋</b>；110kV 站仅风险总览，F3 详情需独立授权包。','动作能力：read / review / approve / export（导出为单次短时授权）；<b>不含</b> configure 与平台安全策略发布。','字段级别：合同原件、付款账号按 F2 单独授权；人员手机号默认脱敏。','策略版本：<code>2026.08.01-01</code> · 组织快照 <code>2026-11-11 05:30</code>（年龄 6 小时，未超 15 分钟只读上限的豁免场景为低敏感视图）'],
  bsup:['角色来源：<b>飞书用户组「A 楼主管」</b> + 组织节点 A；岗位文本不参与裁决。','数据范围：<b>仅 A 楼</b>。所有查询下推 buildingId=A；园区公共告警按 appliesToBuildingIds 单独标注，<b>不计入 A 楼统计</b>。','动作能力：本楼栋 read / update / review；<b>禁止</b>角色配置与高风险删除。','字段级别：跨楼栋合同仅返回字段白名单（编号、名称、A 楼责任范围、A 楼分摊金额、节点、截止、本楼责任人）。','策略版本：<code>2026.08.01-01</code> · 命中规则 <code>ALLOW_SCOPE_BUILDING</code>'],
  beng:['角色来源：<b>飞书用户组「B 楼电气」</b>；专业字段独立命名空间，不与楼栋混用。','数据范围：<b>B 楼 ∩ 电气 ∩ 本人负责/参与/专业公共</b>；跨专业仅关联摘要。','动作能力：EXECUTE（巡检、处置、上传证据）、CREATE（检修申请）、SUBMIT；<b>不含</b> APPROVE 本人事项、强制开工、关闭重大事件。','动态条件：当班实例有效才开放写操作；班次结束立即回收。','策略版本：<code>2026.08.01-01</code> · 命中规则 <code>ALLOW_BUILDING_PROFSSION_OWN</code>'],
  duty:['角色来源：<b>排班底表 + 签到接班实例</b>（动态条件优先于静态角色）。','数据范围：<b>当前班次 ∩ A 楼 ∩ 本班组 ∩ 本人或班组任务</b>。','动作能力：EXECUTE（巡检、接单、补录、交班）；<b>不含</b>改排班、改阈值、改他人记录；重大事件只能申请关闭。','动态条件：06:58 签到、07:00 接班有效；15:00 班次结束自动回收写权限。','策略版本：<code>2026.08.01-01</code> · 命中规则 <code>ALLOW_ON_DUTY_TEAM</code>'],
  plead:['角色来源：<b>飞书用户组「电气专业主管」</b> + 独立 <code>STATION_110</code> 授权包。','数据范围：<b>全园区 ∩ 电气</b>；110kV 站日常管理仅在命中站点授权包时可见。','动作能力：REVIEW（技术审查、会签、退回补充）、催办；<b>禁止</b>设备控制、倒闸执行、定值修改、越级审批。','职责分离：申请人不得审批本人申请；高风险操作需双人复核。','策略版本：<code>2026.08.01-01</code> · 命中规则 <code>ALLOW_PROFESSION_CAMPUS + STATION_110</code>'],
  gadmin:['角色来源：<b>飞书用户组「综合管理」</b>。','数据范围：<b>采购 / 合同 / 授权 / 库存域</b>；不按楼栋或专业全量继承；不默认查看技术运行明细。','动作能力：CREATE / EDIT（经办）、催办、台账维护；<b>不含</b> APPROVE 本人经办事项（职责分离）。','字段级别：合同金额 F2 白名单；付款账号、原件另授权。','策略版本：<code>2026.08.01-01</code> · 命中规则 <code>ALLOW_DOMAIN_ADMIN</code>']
};
const DENIAL = {
  role:'bsup', object:'B 楼 2F UPS 模块更换 施工详情', objectTags:'buildingId=B · profession=电气 · scopeType=BUILDING',
  action:'VIEW', code:'DENY_SCOPE_MISMATCH', decision:'DEC-20261111-004821',
  text:'当前账号以【A 楼主管】访问【B 楼 2F UPS 模块更换 施工详情】，所需动作【VIEW】；命中的有效范围为【南通园区 / A 楼 / 全专业 / 当班有效】，对象范围为【buildingId=B】；因【DENY_SCOPE_MISMATCH】拒绝。规则版本【2026.08.01-01】，人员/组织快照时间【2026-11-11 05:30】。'
};

/* ── 待办补充字段：责任人与业务对象编号（桌面/移动都直接可见，
     不再写「责任人 见详情」）；key = 角色:组内序号 ── */
const OWNERS = {
  'manager:0':['FOC 值班台 · 待指派','AL-2026-1187'], 'manager:1':['陈志远（B 楼电气）','EV-1111-0339'],
  'manager:2':['王敏（综合管理）','HT-2024-0117'], 'manager:3':['陈志远 申请 / 待你终审','PO-2026-0442'],
  'manager:4':['D 楼 维保厂商 恒信','SC-2026-0091'], 'manager:5':['王敏 申请 / 安全管理员复核','EX-2026-0033'],
  'bsup:0':['张伟（早班）','AL-2026-1191'], 'bsup:1':['刘小杏 本人 / 厂商 南电试验','SC-2026-0088'],
  'bsup:2':['张伟 / 赵敏 待复训','CZ-2026-0042'], 'bsup:3':['李静（夜班）→ 张伟','HJ-2026-1111'],
  'bsup:4':['库房 周敏','KC-2026-0207'],
  'beng:0':['本人（施工责任人）','SC-2026-0094'], 'beng:1':['本人 / FOC 派发','EV-1111-0342'],
  'beng:2':['本人','JX-2026-0311'], 'beng:3':['本人 会签 / 刘小杏 审批','BG-2026-0077'],
  'beng:4':['本人','XJ-2026-0455'],
  'duty:0':['本人（当班）','EV-1111-0351'], 'duty:1':['本人（早班 A 组）','XJ-2026-0456'],
  'duty:2':['本人（夜班遗留责任人）','SH-2026-0331'], 'duty:3':['本人','XJ-2026-0457'],
  'duty:4':['本人 → 李静（夜班）','HJ-2026-1112'],
  'plead:0':['110kV 站当班 · 仅研判','ST-110-2026-0021'], 'plead:1':['张伟 申请 / 你审查','BG-2026-0081'],
  'plead:2':['你统筹 / 4 楼执行','JX-2026-0315'], 'plead:3':['陈志远 申请升级','FX-2026-0064'],
  'plead:4':['你会签','RB-2026-0012'],
  'gadmin:0':['供应商 待补 / 你催办','PO-2026-0451'], 'gadmin:1':['你经办 / 经理审批','HT-2023-0088'],
  'gadmin:2':['你回收 / 安全管理员确认','AQ-2026-0119'], 'gadmin:3':['库房 登记 / 他人审批','KC-2026-0214'],
  'gadmin:4':['你说明 / 审计只读','AJ-2026-0007']
};
/* 关键数字的环比口径（KPI 必须带趋势上下文） */
const DELTA = {
  manager:['较昨日 +2','较昨日 -1','与昨日持平','较上月 +1'],
  bsup:['较昨日 +1','较昨日 0','与昨日持平','较上月 -1'],
  beng:['较昨日 +1','较昨日 +2','今日新增 1','较上周 +3'],
  duty:['本班新增 1','较上一班 +1','缺项较夜班 -1','本班已处置 4'],
  plead:['较昨日 +1','较昨日 +2','较上周 -2','本月 34 票'],
  gadmin:['较昨日 +3','较昨日 0','今日到期 1','较上周 +2']
};

/* ── 跨楼栋比较数据与同口径下钻明细（§7.2：汇总与明细同一派生模型） ── */
const CMP = {
  cols:[
    {k:'alarm', label:'关键告警', tone:v => v >= 3 ? 'hot' : v >= 1 ? 'warm' : ''},
    {k:'todo', label:'P0/P1 待办', tone:v => v >= 5 ? 'hot' : v >= 3 ? 'warm' : ''},
    {k:'onduty', label:'在岗', parse:v => Number(String(v).split('/')[0]), tone:v => { const [a, b] = String(v).split('/').map(Number); return a < b ? (b - a > 1 ? 'hot' : 'warm') : 'good'; }},
    {k:'contract', label:'合同临期', tone:v => v >= 2 ? 'warm' : ''},
    {k:'build', label:'施工风险', tone:v => v >= 2 ? 'warm' : ''}
  ],
  rows:[
    ['A 楼', 2, 3, '6/6', 2, 1],
    ['B 楼', 1, 4, '5/5', 1, 2],
    ['C 楼', 3, 5, '4/5', 1, 1],
    ['D 楼', 0, 1, '3/3', 1, 2],
    ['E 楼', 0, 1, '3/3', 1, 0],
    ['H 楼', 1, 1, '2/3', 0, 0]
  ]
};
const DRILL_POOL = {
  alarm:['烟感预报警 未确认','漏水告警 处置超时','风机振动越限','精密空调除霜循环','配电柜温度越限','UPS 旁路告警'],
  todo:['报废申请待批','专项排查退回重审','送检报告待核对','预防性试验节点待确认','低库存补货待审','临期证照待安排'],
  onduty:['张伟','李静','陈志远','王磊','赵敏','李强'],
  contract:['冷却托管合同','保洁服务合同','打印机租赁','消防维保合同','电梯维保合同','柴发保养合同'],
  build:['冷塔防腐施工','UPS 模块更换','气流遏制盲板补装','高压预防性试验','桥架锈蚀整改','冷机年度检修']
};
const DRILL_LABEL = { alarm:'关键告警', todo:'P0/P1 待办', onduty:'在岗', contract:'合同临期', build:'施工风险' };
function drillRows(building, k, raw){
  if (k === 'onduty'){
    const [on, total] = String(raw).split('/').map(Number);
    const rows = DRILL_POOL.onduty.slice(0, total).map((n, i) => ({ t:n, s:i < on ? '在岗 · 已签到' : '缺岗 · 已升级主管', tone:i < on ? 'ok' : 'bad' }));
    return { count:total, rows, note:`在岗 ${on} / 应到 ${total}；缺岗名单仅对本楼栋有权角色可见` };
  }
  const v = Number(raw);
  const pool = DRILL_POOL[k];
  const rows = Array.from({length:v}, (_, i) => ({ t:pool[i % pool.length], s:`${building} · 11-11 · 编号 ${k.toUpperCase()}-${building.charAt(0)}${String(i + 1).padStart(2, '0')}`, tone:k === 'alarm' && i === 0 ? 'bad' : 'warn' }));
  return { count:v, rows, note:v ? `明细 ${v} 条 = 卡片数字 ${v}（同一范围快照 SNAP-20261111-0530）` : '该楼栋本指标为 0；0 是真实值而非读取失败' };
}
/* ── 今日运行卡片的同口径下钻明细（条数与卡片数字一致） ── */
const OPS_DRILL = {
  'alarm|manager': { title:'全园区关键告警', rows:[
    {t:'C 楼 蓄电池室 烟感预报警', s:'P0 · 未确认 · 11-11 11:16 · 责任人 待指派', tone:'bad'},
    {t:'A 楼 2F 机房 漏水告警', s:'P0 · 处置超时 · EV-1111-0339 · 陈志远', tone:'bad'},
    {t:'B 楼 4F 冷塔风机 2 号 振动越限', s:'P1 · 处理中 · 张伟', tone:'warn'},
    {t:'H 楼 1F 配电间 温度越限', s:'P2 · 待确认 · 值班账号（共享账号不可处置）', tone:'warn'}
  ], note:'园区合计按告警 ID 去重；楼栋视图仅含本楼栋告警，二者可通过告警 ID 解释。' },
  'alarm|bsup': { title:'A 楼关键告警', rows:[
    {t:'4F 冷塔风机 2 号 振动越限', s:'P0 · 未确认 · 11-11 11:16 · 张伟', tone:'bad'},
    {t:'1F 精密空调 4 号 除霜循环', s:'P1 · 处理中 · 预期 15 分钟内结束', tone:'warn'}
  ], note:'仅 buildingId=A 的告警；园区公共告警中影响 A 楼的部分单独标注、不计入本卡。' },
  'purchase|gadmin': { title:'待审核采购单', rows:[
    {t:'高压开关备件（B 楼检修）', s:'PO-2026-0442 · 金额 已脱敏（F2）· 超 48h', tone:'bad'},
    {t:'办公区打印机租赁', s:'PO-2026-0451 · 缺供应商资质附件', tone:'warn'},
    {t:'兆欧表电池与表笔补货', s:'PO-2026-0455 · 低库存触发', tone:'warn'},
    {t:'冷塔防腐材料', s:'PO-2026-0458 · 待比价', tone:'warn'},
    {t:'消防感温测试枪年检服务', s:'PO-2026-0460 · 待审核', tone:'warn'},
    {t:'UPS 电池组单体', s:'PO-2026-0461 · 待审核', tone:'warn'},
    {t:'机房防静电地板备件', s:'PO-2026-0463 · 待审核', tone:'warn'},
    {t:'应急照明灯具', s:'PO-2026-0464 · 待审核', tone:'warn'}
  ], note:'仅行政与授权域采购；金额按 F2 脱敏，申请人≠审批人，你经办的单不在你的可审批集合内。' },
  'patrol|duty': { title:'本班巡检 8 点', rows:[
    {t:'1F 配电间 红外测温', s:'07:20 完成 · 照片 2 张', tone:'ok'},
    {t:'1F 消防泵房 压力核对', s:'07:42 完成 · 照片 1 张', tone:'ok'},
    {t:'2F 机房 温湿度抄录', s:'08:05 完成', tone:'ok'},
    {t:'3F 电池室 通风检查', s:'08:30 完成', tone:'ok'},
    {t:'3F 生活水表 读数', s:'09:10 完成 · 缺照片（交班缺项）', tone:'warn'},
    {t:'4F 冷塔平台 风机复听', s:'漏检 · 需 12:00 前补巡', tone:'bad'},
    {t:'4F 冷塔平台 振动测量', s:'异常 · 已转复测任务', tone:'warn'},
    {t:'5F 屋面 排水检查', s:'待巡 · 计划 13:30', tone:'warn'}
  ], note:'已完成 5 · 漏检 1 · 异常 1 · 待巡 1，合计 8 点与卡片 5/8 同口径；扫码点检含照片与时间戳。' }
};

function openDrill(spec){
  const [building, k, raw] = spec.split('|');
  if (building === 'ops'){
    const d = OPS_DRILL[k + '|' + raw];
    openDrawerHtml(
      `${icon('i-activity','ic-sm')}<span>同口径下钻</span><span aria-hidden="true">·</span><span>${esc(d.title)}</span>`,
      d.title + ' · ' + d.rows.length + ' 条',
      `<span class="tag tag-neutral">快照 SNAP-20261111-0530</span><span class="tag tag-ok">明细 ${d.rows.length} 条 = 卡片数字</span>`,
      sect('i-layers', '明细清单', `<ul class="drill-list">${d.rows.map((r, i) => `<li class="drill-item"><span class="di-n num">${i + 1}</span><span><b>${esc(r.t)}</b><span>${esc(r.s)}</span></span><span class="tag tag-${r.tone}">${r.tone === 'bad' ? '阻断/超时' : r.tone === 'ok' ? '正常' : '待处理'}</span></li>`).join('')}</ul><p class="hero-foot" style="border:0;padding-top:9px">${esc(d.note)}</p>`),
      `<span class="grow">${icon('i-info','ic-sm')} 汇总与明细使用同一派生数据模型</span><button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
    );
    announce('已下钻 ' + d.title + '，共 ' + d.rows.length + ' 条');
    return;
  }
  if (building === 'all'){
    const col = CMP.cols.find(c => c.k === k);
    const rows = CMP.rows.map(r => ({ b:r[0], raw:r[CMP.cols.indexOf(col) + 1] }));
    const total = rows.reduce((n, r) => n + (k === 'onduty' ? Number(String(r.raw).split('/')[0]) : Number(r.raw)), 0);
    openDrawerHtml(
      `${icon('i-building','ic-sm')}<span>同口径下钻</span><span aria-hidden="true">·</span><span>${esc(col.label)}</span>`,
      `全部楼栋 · ${col.label}`,
      `<span class="tag tag-neutral">快照 SNAP-20261111-0530</span><span class="tag tag-ok">合计 ${total}</span>`,
      sect('i-layers', '按楼栋拆分', `<ul class="drill-list">${rows.map((r, i) => `<li class="drill-item"><span class="di-n num">${i + 1}</span><span><b>${esc(r.b)}</b><span>${esc(col.label)} ${esc(String(r.raw))}</span></span><button type="button" class="btn btn-sm btn-secondary" data-drill="${esc(r.b)}|${k}|${esc(String(r.raw))}">明细</button></li>`).join('')}</ul>
        <p class="hero-foot" style="border:0;padding-top:9px">园区合计按记录 ID 去重；楼栋视图按各自分摊，二者可通过分摊明细解释。</p>`),
      `<span class="grow">${icon('i-info','ic-sm')} 汇总与明细使用同一派生数据模型</span><button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
    );
    announce('已下钻全部楼栋的' + col.label);
    return;
  }
  const d = drillRows(building, k, raw);
  openDrawerHtml(
    `${icon('i-building','ic-sm')}<span>同口径下钻</span><span aria-hidden="true">·</span><span>${esc(building)} · ${esc(DRILL_LABEL[k])}</span>`,
    `${building} · ${DRILL_LABEL[k]} · ${esc(String(raw))}`,
    `<span class="tag tag-neutral">快照 SNAP-20261111-0530</span><span class="tag tag-ok">明细 ${d.count} 条 = 卡片 ${esc(String(raw))}</span>`,
    d.rows.length
      ? sect('i-layers', '明细清单', `<ul class="drill-list">${d.rows.map((r, i) => `<li class="drill-item"><span class="di-n num">${i + 1}</span><span><b>${esc(r.t)}</b><span>${esc(r.s)}</span></span><span class="tag tag-${r.tone}">${r.tone === 'bad' ? '阻断/超时' : r.tone === 'ok' ? '正常' : '待处理'}</span></li>`).join('')}</ul><p class="hero-foot" style="border:0;padding-top:9px">${esc(d.note)}</p>`)
      : `<div class="state state-ok">${icon('i-check','ic-lg')}<b>该楼栋本指标为 0</b><p>${esc(d.note)}</p></div>`,
    `<span class="grow">${icon('i-info','ic-sm')} 明细基于同一范围快照与派生规则，保证「卡片 8 条，明细正好 8 条」</span><button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
  );
  announce(`已下钻 ${building} 的${DRILL_LABEL[k]}明细，共 ${d.count} 条`);
}

/* ── 消息与催办：冷却窗口去重（§8.7）+ 消息直达四态（§8.9） ── */
const MSG_STATE = { enter:['可进入','ok'], view:['仅查看','info'], scope:['范围已变化','warn'], closed:['对象已关闭','neutral'] };
const MSGS = {
  manager:[
    {t:'C 楼 蓄电池室 烟感预报警 升级通知', obj:'AL-2026-1187', state:'enter', when:'11:16', note:'同一 dedupKey 在 5 分钟冷却窗口内不重复推送；15 分钟未确认触发本次升级提醒。'},
    {t:'H 楼 缺岗升级催办', obj:'DT-2026-1111', state:'enter', when:'10:42', note:''},
    {t:'B 楼 UPS 模块更换 终审结果', obj:'PO-2026-0442', state:'view', when:'09:58', note:'终审人为平台安全管理员复核链：你仅查看结果，不可改判。'},
    {t:'D 楼 机柜上下电申请 已关闭', obj:'CAB-1111-07', state:'closed', when:'08:31', note:'对象已关闭：打开仅显示归档摘要，不保留旧权限。'}
  ],
  bsup:[
    {t:'A 楼 4F 冷塔风机 2 号 振动越限 P0 催确认', obj:'AL-2026-1191', state:'enter', when:'11:16', note:'同一 dedupKey 冷却窗口内不重复弹出；状态恶化或责任人变化才会再次提醒。'},
    {t:'A 楼 预防性试验 节点提醒', obj:'SC-2026-0088', state:'enter', when:'10:05', note:''},
    {t:'B 楼 温湿度越限事件 范围已变化', obj:'EV-1111-0342', state:'scope', when:'09:40', note:'该事件已改派 B 楼电气：你的范围不再包含，打开为仅查看摘要。'},
    {t:'A 楼 水表照片 催办（责任人变化）', obj:'SH-2026-0331', state:'enter', when:'08:12', note:'责任人由李静变更为张伟，触发重新提醒。'}
  ],
  beng:[
    {t:'B 楼 2F 温湿度越限 派单给你', obj:'EV-1111-0342', state:'enter', when:'11:02', note:'接单后进入处置留痕链；冷却窗口内不重复推送。'},
    {t:'B 楼 3F 配电柜 绝缘复测 提醒', obj:'JX-2026-0311', state:'enter', when:'09:30', note:''},
    {t:'C 楼 冷机检修 抄送你会签', obj:'BG-2026-0079', state:'view', when:'08:44', note:'你为 C（征询）：仅会签意见，不获得编辑或关闭权。'}
  ],
  duty:[
    {t:'A 楼 1F 消防泵房 压力波动 派单', obj:'EV-1111-0351', state:'enter', when:'11:18', note:'P1 事件 SLA 30 分钟确认；冷却窗口内不重复推送。'},
    {t:'4F 冷塔平台 漏检提醒', obj:'XJ-2026-0456', state:'enter', when:'10:20', note:''},
    {t:'昨夜 3F 水表照片 催办', obj:'SH-2026-0331', state:'enter', when:'08:12', note:'责任人变化（李静→本人）触发重新提醒。'}
  ],
  plead:[
    {t:'110kV 站 2 号主变 油温越限预判', obj:'ST-110-2026-0021', state:'enter', when:'11:12', note:'仅研判与记录入口；不含设备控制深链。'},
    {t:'C 楼 冷机检修 变更待你技术审查', obj:'BG-2026-0081', state:'enter', when:'10:31', note:''},
    {t:'B 楼 桥架锈蚀 升级申请', obj:'FX-2026-0064', state:'enter', when:'09:15', note:'升级复核不代替值班处置。'}
  ],
  gadmin:[
    {t:'打印机租赁 采购单 缺件催办', obj:'PO-2026-0451', state:'enter', when:'10:58', note:''},
    {t:'保洁服务合同 付款节点 7 天提醒', obj:'HT-2023-0088', state:'enter', when:'09:20', note:'付款经办≠付款审批：你仅可发起流程。'},
    {t:'施工队临时进场授权 今日到期', obj:'AQ-2026-0119', state:'enter', when:'08:00', note:'到期自动回收并失效缓存；本提醒在回收前 2 小时重发一次。'},
    {t:'你发起的 10 月授权复核 已归档', obj:'AJ-2026-0005', state:'closed', when:'昨日 17:40', note:'对象已关闭：打开仅显示归档摘要与审计链。'}
  ]
};
function openMessages(){
  const list = MSGS[state.role] || [];
  openDrawerHtml(
    `${icon('i-bell','ic-sm')}<span>消息与催办</span><span aria-hidden="true">·</span><span class="num">${list.length} 条</span>`,
    '消息与催办 · ' + R().roleLabel,
    `<span class="tag tag-neutral">冷却窗口 5 分钟</span><span class="tag tag-neutral">打开时重新鉴权</span>`,
    sect('i-bell', '按时间倒序', `<ul class="msg-list">${list.map(m => `
      <li class="msg-item">${icon(MSG_STATE[m.state][1] === 'ok' ? 'i-check' : MSG_STATE[m.state][1] === 'warn' ? 'i-clock' : MSG_STATE[m.state][1] === 'info' ? 'i-info' : 'i-x')}
        <span class="mi-body"><b>${esc(m.t)}</b><span class="mi-obj">${esc(m.obj)}</span>${m.note ? `<span class="mi-note">${esc(m.note)}</span>` : ''}</span>
        <span style="display:grid;gap:5px;justify-items:end"><time class="num">${esc(m.when)}</time><span class="tag tag-${MSG_STATE[m.state][1]}">${MSG_STATE[m.state][0]}</span></span>
      </li>`).join('')}</ul>
      <p class="hero-foot" style="border:0;padding-top:9px">通知正文只携带业务标识与最小摘要，不暴露敏感运行数据；点击后后端重新校验用户、角色、对象与楼栋范围。撤权后旧消息不能继续打开明细。</p>`),
    `<span class="grow">${icon('i-info','ic-sm')} 同一 dedupKey 冷却窗口内不重复弹出；状态恶化或责任人变化可重新提醒</span>
     <button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
  );
  announce('已打开消息与催办，共 ' + list.length + ' 条');
}

/* ── 导出：单次短时授权 + 范围裁剪 + 水印（§8.10.6 EXPORT / §8.10.8） ── */
function openExport(){
  const r = R();
  openDrawerHtml(
    `${icon('i-download','ic-sm')}<span>导出申请</span><span aria-hidden="true">·</span><span>${esc(SEC_NAME.biz)}</span>`,
    '申请导出 · ' + r.roleLabel,
    `<span class="tag tag-warn">单次短时授权</span><span class="tag tag-neutral">需安全管理员复核</span>`,
    sect('i-shield', '导出策略（服务端强制执行）', `<ul class="perm-list" style="margin:0">
      <li>${icon('i-check')}<span><b>范围裁剪</b>：仅当前授权范围（${esc(r.scope)}）；请求参数只能进一步收窄，不能扩大。</span></li>
      <li>${icon('i-check')}<span><b>字段白名单</b>：F2 敏感字段（合同金额明细、付款账号、手机号）默认剔除或脱敏；F3 一律不导出。</span></li>
      <li>${icon('i-check')}<span><b>数量阈值</b>：单次 ≤ 2,000 行；超出转人工审批并说明用途。</span></li>
      <li>${icon('i-check')}<span><b>职责分离</b>：申请人 ≠ 审批人；共享值班账号不得发起导出。</span></li>
      <li>${icon('i-alert')}<span><b>失败关闭</b>：权限源不可用或快照超龄时，导出直接拒绝，不降级执行。</span></li></ul>`)
    + sect('i-download', '模拟签发结果', `<div class="token-block">export_token: <b>EXP-20261111-0031-7f3a</b><br>valid: <b>15 分钟</b> · 单次使用 · 用后即失效<br>scope: ${esc(r.scope)}<br>watermark: <b>WM-20261111-0031</b>（含导出人、时间、规则版本、范围）<br>audit: DEC-20261111-004839 · policy 2026.08.01-01</div>
      <p class="hero-foot" style="border:0;padding-top:9px">水印编号写入审计；外发文件可据此回溯导出人与范围。</p>`),
    `<span class="grow">${icon('i-info','ic-sm')} 原型演示：不产生真实导出任务</span>
     <button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>
     <button type="button" class="btn btn-primary">提交导出申请</button>`
  );
  announce('已打开导出申请：单次短时授权 + 范围裁剪 + 水印');
}

/* ── 角色化首屏英雄区：六种身份六种设计（§8.8） ── */
function renderHero(){
  const el = $('#hero');
  const r = state.role;
  if (r === 'duty'){
    el.innerHTML = `<div class="hero-card" data-component="Shift Cockpit" data-od-id="hero-cockpit">
      <div class="hero-head"><h2>${icon('i-clock')}当班驾驶舱</h2><span class="hero-sub">早班 07:00–15:00 · 已过 4 小时 10 分 · 剩 3 小时 50 分</span>
        <span class="hero-act"><span class="tag tag-ok">已签到 06:58</span><span class="tag tag-ok">已接班 07:00</span></span></div>
      <div class="cockpit">
        <div class="cockpit-primary">
          <button type="button" class="btn btn-primary">${icon('i-scan','ic-lg')}继续巡检（唯一主操作）</button>
          <span class="cp-hint">剩余 3 个巡检点 · 扫码点检并拍照留证；本班结束前完成</span>
        </div>
        <div class="cockpit-meters">
          <div class="cm tone-warn"><span class="big num">5/8<small> 点</small></span><span>巡检完成率 62%</span><div class="bar"><i style="width:62%"></i></div></div>
          <div class="cm tone-bad"><span class="big num">22<small> 分钟</small></span><span>事件 SLA 剩余</span><div class="bar"><i style="width:26%"></i></div></div>
          <div class="cm tone-warn"><span class="big num">2<small> 项</small></span><span>交班就绪度 缺 2 项</span><div class="bar"><i style="width:66%"></i></div></div>
        </div>
      </div>
      <div class="hero-foot">巡检发现 → 事件处置 → 交接班留痕 为同一条可追溯链路；重大事件只能申请关闭，不能直接闭环。</div>
    </div>`;
    return;
  }
  if (r === 'manager'){
    el.innerHTML = `<div class="hero-card" data-component="Building Compare" data-od-id="hero-compare">
      <div class="hero-head"><h2>${icon('i-building')}跨楼栋比较</h2><span class="hero-sub">同口径 · 园区按记录去重，楼栋按各自分摊 · 点击数字下钻明细</span>
        <span class="hero-act"><button type="button" class="btn btn-secondary btn-sm" data-shortcut="today_map">进入今日航图</button></span></div>
      <div class="cmp-wrap"><table class="cmp">
        <caption class="sr-only">各楼栋关键告警、P0/P1 待办、在岗、合同临期与施工风险比较；点击单元格查看同口径明细</caption>
        <thead><tr><th scope="col">楼栋</th>${CMP.cols.map(c => `<th scope="col"><button type="button" class="cmp-th-btn" data-drill="all|${c.k}" aria-label="下钻全部楼栋的${c.label}明细">${c.label}</button></th>`).join('')}</tr></thead>
        <tbody>${CMP.rows.map(row => `<tr>
          <td>${esc(row[0])}</td>
          ${CMP.cols.map((c, ci) => {
            const raw = row[ci + 1], v = c.parse ? c.parse(raw) : raw;
            const cls = c.tone ? c.tone(raw) : '';
            return `<td class="${cls}"><button type="button" class="cmp-btn ${cls}" data-drill="${esc(row[0])}|${c.k}|${raw}" aria-label="${esc(row[0])} ${c.label} ${esc(String(raw))}，下钻明细">${esc(String(raw))}</button></td>`;
          }).join('')}
        </tr>`).join('')}</tbody>
      </table></div>
      <div class="hero-foot">C 楼与 H 楼为今日关注：C 楼烟感预报警未确认、H 楼缺岗 1 人已升级。每个数字的明细条数与卡片完全一致（同一范围快照 SNAP-20261111-0530）。</div>
    </div>`;
    return;
  }
  if (r === 'bsup'){
    el.innerHTML = `<div class="hero-card" data-component="Building Risk" data-od-id="hero-risk">
      <div class="hero-head"><h2>${icon('i-alert')}A 楼风险与责任人</h2><span class="hero-sub">仅 A 楼；园区公共告警单独标注、不计入本卡</span></div>
      <div class="risk-rows">
        <div class="risk-row is-bad"><span class="rr-body"><b>4F 冷塔风机 2 号 振动越限（P0）</b><span>处置时限 11:20 前 · 阻断：无</span></span><span class="rr-owner">责任人 <b>张伟</b></span><span class="tag tag-bad">待确认</span></div>
        <div class="risk-row"><span class="rr-body"><b>1F 高压配电室 预防性试验 节点未确认</b><span>处置时限 今日 12:10 · 顺延将影响停机窗口</span></span><span class="rr-owner">责任人 <b>刘小杏</b></span><span class="tag tag-warn">待确认</span></div>
        <div class="risk-row"><span class="rr-body"><b>3F 生活水表 读数缺照片</b><span>交班缺项 · 阻塞水耗月度归档</span></span><span class="rr-owner">责任人 <b>张伟</b></span><span class="tag tag-warn">待补录</span></div>
      </div>
      <div class="hero-foot">园区公共告警 1 条影响 A 楼：仅展示 A 楼受影响部分与本楼动作，不展示其他楼栋数量与责任人。</div>
    </div>`;
    return;
  }
  if (r === 'beng'){
    el.innerHTML = `<div class="hero-card" data-component="Closure Chain" data-od-id="hero-chain">
      <div class="hero-head"><h2>${icon('i-wrench')}专业任务闭环链路</h2><span class="hero-sub">B 楼 2F UPS 模块更换 · SC-2026-0094</span>
        <span class="hero-act"><span class="tag tag-bad">阻断中</span></span></div>
      <ol class="chain" aria-label="任务闭环链路">
        <li class="chain-step done"><span class="cs-n">${icon('i-check')}1 风险发现</span><span class="cs-t">11-09 绝缘 0.8MΩ 偏低</span></li>
        <li class="chain-step done"><span class="cs-n">${icon('i-check')}2 转计划作业</span><span class="cs-t">检修票 JX-2026-0311</span></li>
        <li class="chain-step blocked"><span class="cs-n">${icon('i-alert')}3 阻断条件</span><span class="cs-t">停电窗口未批 · 不开工</span></li>
        <li class="chain-step"><span class="cs-n">${icon('i-upload')}4 现场证据</span><span class="cs-t">读数 + 照片 待回填</span></li>
        <li class="chain-step"><span class="cs-n">${icon('i-shield')}5 复核</span><span class="cs-t">主管工程师 待复核</span></li>
        <li class="chain-step"><span class="cs-n">${icon('i-check')}6 申请闭环</span><span class="cs-t">证据齐全后方可申请</span></li>
      </ol>
      <div class="hero-foot">阻断条件未解除时不提供「强制开工」；本人事项不得自审自批。</div>
    </div>`;
    return;
  }
  if (r === 'plead'){
    el.innerHTML = `<div class="hero-card" data-component="Station Readonly" data-od-id="hero-station">
      <div class="hero-head"><h2>${icon('i-server')}110kV 站日常管理（只读）</h2><span class="hero-sub">命中独立 STATION_110 授权包 · 仅电气主管可见</span>
        <span class="hero-act"><span class="tag tag-info">只读 · 无设备控制</span></span></div>
      <dl class="ro-grid">
        <div class="ro-cell"><dt>1 号主变</dt><dd class="tone-ok">运行 · 62℃</dd></div>
        <div class="ro-cell"><dt>2 号主变</dt><dd class="tone-bad">油温 78℃ 越限预判</dd></div>
        <div class="ro-cell"><dt>进线回路</dt><dd class="tone-ok">双回路正常</dd></div>
        <div class="ro-cell"><dt>保护记录</dt><dd class="tone-ok">本月 0 动作</dd></div>
        <div class="ro-cell"><dt>两票三制</dt><dd class="tone-ok">34 票无缺项</dd></div>
        <div class="ro-cell"><dt>倒闸计划</dt><dd class="tone-warn">2 份待审阅</dd></div>
      </dl>
      <p class="ro-note">${icon('i-info')}本面板仅监视、记录与审查；分合闸、倒闸执行与定值修改不在任何入口出现，高风险操作需双人复核与完整操作票。</p>
    </div>`;
    return;
  }
  el.innerHTML = `<div class="hero-card" data-component="Approval Timeline" data-od-id="hero-approval">
    <div class="hero-head"><h2>${icon('i-clipboard')}审批队列与临期时间线</h2><span class="hero-sub">采购 — 合同 — 授权 跨流程协同</span>
      <span class="hero-act"><button type="button" class="btn btn-secondary btn-sm">进入授权中心</button></span></div>
    <div class="due-buckets">
      <div class="bucket b7"><h4>7 天内</h4><span class="bn num">2</span><ul><li><b>保洁服务合同</b> 11-18 付款节点</li><li><b>施工队临时进场</b> 今日 18:00 到期</li></ul></div>
      <div class="bucket b14"><h4>8–14 天</h4><span class="bn num">3</span><ul><li><b>打印机租赁</b> 缺资质附件</li><li><b>2 项系统临时权限</b> 11-13 到期</li></ul></div>
      <div class="bucket b30"><h4>15–30 天</h4><span class="bn num">4</span><ul><li><b>3 份合同</b> 续签窗口</li><li><b>1 项授权</b> 季度复核</li></ul></div>
    </div>
    <div class="hero-foot">申请人与审批人分离：你经办的 8 单采购不在你的可审批集合内；到期授权自动回收并失效缓存。</div>
  </div>`;
}


/* ══════════════════════════════════════════════════════════════
   渲染层
   ══════════════════════════════════════════════════════════════ */
const state = { role:'manager', appearance:'light', viewport:'desktop', sec:'biz', empty:false, scq:'' };
const icon = (id, cls) => `<svg class="ic ${cls || ''}" aria-hidden="true"><use href="${HASH}${id}"/></svg>`;
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const R = () => ROLES[state.role];

/* 迷你条形图：内联 SVG，带可见数值与 aria（用于业务进展的趋势） */
function miniBars(values, labels, unit){
  const w = 260, h = 74, padB = 16, padT = 12;
  const max = Math.max.apply(null, values) || 1;
  const bw = Math.floor((w - 8) / values.length) - 6;
  let s = `<svg class="chart-svg" viewBox="0 0 ${w} ${h}" role="img" aria-label="近 ${values.length} 期趋势：${values.map((v,i)=>labels[i]+' '+v+unit).join('、')}"><title>近 ${values.length} 期趋势</title>`;
  values.forEach((v, i) => {
    const x = 4 + i * (bw + 6), bh = Math.max(3, Math.round((h - padB - padT) * v / max)), y = h - padB - bh;
    s += `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" rx="2" fill="${i === values.length - 1 ? 'var(--seed-accent)' : 'color-mix(in srgb, var(--seed-primary) 45%, var(--seed-surface))'}"/>`;
    s += `<text x="${x + bw / 2}" y="${y - 3}" text-anchor="middle" font-size="9" font-weight="600" fill="var(--fg)">${v}</text>`;
    s += `<text x="${x + bw / 2}" y="${h - 4}" text-anchor="middle" font-size="9" fill="var(--muted)">${esc(labels[i])}</text>`;
  });
  return s + '</svg>';
}

/* 移动端持续可见的角色与范围栏 */
function renderSticky(){
  const r = R();
  $('#scope-sticky').innerHTML = `
    <span class="ss-role">${esc(r.roleLabel)}</span>
    <span class="scope-chip${r.scopeFixed ? ' is-fixed' : ''}">${icon(r.scopeFixed ? 'i-building' : 'i-grid')}${esc(r.scope)}</span>
    <span class="src src-ok" id="sticky-src">${icon('i-check')}同步 3 分钟前</span>`;
}

function renderIdentity(){
  const r = R();
  $('#identity').innerHTML = `
    <div class="id-who">
      <span class="id-avatar" aria-hidden="true">${esc(r.avatar)}</span>
      <span><span class="id-name">${esc(r.person)}</span>
      <span class="id-role">${icon('i-shield','ic-sm')}<span>${esc(r.roleLabel)}</span><code>${esc(r.roleCode)}</code></span></span>
    </div>
    <div class="id-facts">
      <span class="scope-chip${r.scopeFixed ? ' is-fixed' : ''}">${icon(r.scopeFixed ? 'i-building' : 'i-grid')}${esc(r.scope)}${r.scopeFixed ? ' · 不可切换' : ' · 可比较'}</span>
      <button type="button" class="btn btn-secondary btn-sm" data-perm>${icon('i-info','ic-sm')}为什么是这个范围</button>
      ${state.role === 'bsup' ? '<button type="button" class="btn btn-ghost btn-sm" data-deny-demo>模拟越权访问 B 楼</button>' : ''}
    </div>
    <div class="id-meta">
      <span>权限来源：${esc(r.scopeSource)}</span>
      <span>策略版本 <code>${esc(r.policy)}</code></span>
      <span>组织快照 <code>2026-11-11 05:30</code></span>
      <span id="id-sync">最近同步 3 分钟前</span>
      <span class="why muted">${esc(r.identityNote)}</span>
    </div>`;
}

/* 关键数字一律由同源数据派生（§7.2 同口径）；无法派生的历史数字显式标注为非可操作 KPI（§11.2） */
function numValue(n){
  if (!n.d) return n.value;
  const d = n.d, role = state.role;
  if (d.from === 'urgentP01') return URGENT[role].filter(x => x.p === 'P0' || x.p === 'P1').length;
  if (d.from === 'urgentP0') return URGENT[role].filter(x => x.p === 'P0').length;
  if (d.from === 'urgentKind') return URGENT[role].filter(x => x.kind === d.kind).length;
  if (d.from === 'people') return PEOPLE[role].left.meter.join('/');
  if (d.from === 'ops'){ const c = OPS[role].cards.find(x => x.title.includes(d.title)); return c ? c.line[0] : n.value; }
  if (d.from === 'biz'){ const c = BIZ[role].cards.find(x => x.title.includes(d.title)); return c ? c.line[0] : n.value; }
  if (d.from === 'opsDrill') return OPS_DRILL[d.key].rows.length;
  if (d.from === 'opsDrillDone'){ const rows = OPS_DRILL[d.key].rows; return rows.filter(x => x.tone === 'ok').length + '/' + rows.length; }
  return n.value;
}
function renderNumbers(){
  const r = R();
  $('#focus').innerHTML = r.numbers.map((n, i) => `
    <button type="button" class="focus-card tone-${n.tone}" data-drill="${esc(n.drill || 'ref')}" aria-describedby="fn-${i}">
      <span class="focus-icon">${icon(n.icon,'ic-lg')}</span>
      <span class="focus-body">
        <span class="focus-label">${esc(n.label)}${n.ref ? ' <span class="tag tag-neutral" style="height:18px;font-size:11px">历史口径</span>' : ''}</span>
        <span class="focus-value"><b class="num">${esc(String(numValue(n)))}</b><span>${esc(n.unit)}</span></span>
        <span class="focus-sub" id="fn-${i}">${esc(n.sub)} · ${esc((DELTA[state.role] || [])[i] || '')}</span>
      </span>
      <span class="focus-go">${icon('i-upright')}</span>
    </button>`).join('');
}

const P_META = {
  P0:{label:'P0 · 立即处置', rule:'5 分钟内确认 · 15 分钟未确认自动升级'},
  P1:{label:'P1 · 当班优先', rule:'30 分钟内确认 · 2 小时内更新'},
  P2:{label:'P2 · 当班内', rule:'本班时间窗内确认'},
  P3:{label:'P3 · 按计划', rule:'按计划日期处理'}
};
function urgentRows(){
  return (URGENT[state.role] || []).filter(it => !state.empty);
}
function renderUrgent(){
  const box = $('#queue'), items = urgentRows(), sv = SRC.urgent;
  $('#sla-legend').innerHTML = `<b>SLA</b> P0 5′确认/15′升级 · P1 30′/2h · P2 当班 · P3 计划`;
  srcChipFor($('#urgent-src'), 'urgent');
  if (sv === 'down'){
    $('#queue-count').textContent = '读取失败';
    box.innerHTML = secError('urgent', '统一待办聚合接口超时；已成功的来源仍保留最近一次结果。');
    return;
  }
  if (sv === 'deny'){
    $('#queue-count').textContent = '权限不足';
    box.innerHTML = secDeny('urgent');
    return;
  }
  if (state.empty){
    $('#queue-count').textContent = '0 项';
    $('#queue-note').textContent = '口径：P0–P3 且状态≠已完成';
    box.innerHTML = `<div class="tier"><div class="state state-ok">${icon('i-check','ic-lg')}<b>当前无待办</b><p>口径：优先级 P0–P3 且状态为待处理/处理中/已过期的统一待办对象共 0 条；已完成与已关闭事项不计入。七个来源均返回正常，非读取失败。</p></div></div>`;
    return;
  }
  const groups = ['P0','P1','P2','P3'].filter(p => items.some(it => it.p === p));
  $('#queue-count').textContent = `共 ${items.length} 条 · 卡片与明细同口径`;
  $('#queue-note').textContent = '排序：priority → 是否逾期 → dueAt → 业务影响';
  box.innerHTML = (sv === 'stale' ? secStaleNote() : '') + groups.map((p, gi) => {
    const list = items.filter(it => it.p === p);
    return `<section class="tier" data-tier="${p}" id="tier-${p}" aria-labelledby="tier-h-${p}">
      <header class="tier-head">
        <span class="tier-rank num" aria-hidden="true">${gi + 1}</span>
        <h3 id="tier-h-${p}">${esc(P_META[p].label)}</h3>
        <span class="tier-meta num">${list.length} 条</span>
        <span class="tier-meta">· ${esc(P_META[p].rule)}</span>
        <span class="tier-integrity"><span class="tag tag-ok" style="height:20px">${icon('i-check','ic-sm')}共 ${list.length} 条 · 明细 ${list.length} 条（同口径）</span></span>
      </header>
      <div class="rows">${list.map((it, i) => {
        const ow = OWNERS[state.role + ':' + items.indexOf(it)] || ['见原模块指派', ''];
        return `
        <div class="row" data-pri="${it.p}" data-todo="${state.role}:${it.p}:${i}">
          <span class="row-kind">${icon(ICON[it.kind] || 'i-clipboard','ic-sm')}<span>${esc(it.source)}</span>${it.multi ? `<span class="multi-src">${icon('i-layers','ic-sm')}${it.multi.length} 来源</span>` : ''}</span>
          <span class="row-main"><button type="button" class="row-link row-title" data-todo="${state.role}:${it.p}:${i}">${esc(it.title)}</button>
            <span class="row-meta"><span>${esc(it.scope)}</span><span class="sep" aria-hidden="true">·</span><span>责任人 ${esc(ow[0])}</span><span class="sep" aria-hidden="true">·</span><span class="num">${esc(ow[1])}</span></span>
            <button type="button" class="reason-btn" data-reason aria-expanded="false">${icon('i-info')}为什么在我的工作台</button>
            <span class="reason">${icon('i-info')}<span><b>为什么在我的工作台：</b>${esc(it.reason)}</span></span>
          </span>
          <span class="row-due due-${it.dueTone}"><b>${esc(it.due)}</b><span class="sla sla-${it.p.toLowerCase()}">${icon('i-clock')}${esc(it.sla)}</span></span>
          <span class="row-state"><span class="tag tag-${it.statusTone}">${esc(it.status)}</span></span>
          <span class="row-act"><button type="button" class="btn btn-sm ${it.p === 'P0' ? 'btn-primary' : 'btn-secondary'}" data-todo="${state.role}:${it.p}:${i}">${esc(it.action)}</button></span>
        </div>`; }).join('')}</div>
    </section>`;
  }).join('');
}

/* 每个分区独立的来源状态（§8.5/§8.6）：ok / stale / down / deny */
const SRC = { urgent:'ok', ops:'ok', biz:'ok', people:'ok' };
const SEC_NAME = { urgent:'紧急待办', ops:'今日运行', biz:'业务进展', people:'人员与值班' };
const DENY_CODE = { urgent:'DENY_NOT_ON_DUTY', ops:'DENY_SCOPE_MISMATCH', biz:'DENY_SCOPE_MISMATCH', people:'DENY_SCOPE_EMPTY' };
const SRC_MAP = { ok:['src-ok','i-check','来源正常'], stale:['src-stale','i-clock','数据延迟 · 最近成功 11-10 22:14'], down:['src-down','i-alert','来源不可用'], deny:['src-deny','i-shield','权限不足 · 已拒绝'] };
function srcChipFor(el, key){
  const m = SRC_MAP[SRC[key] || 'ok'];
  el.className = 'src ' + m[0];
  el.innerHTML = icon(m[1]) + m[2];
}
function skelCards(n){
  return Array.from({length:n}, () => `<article class="ops-card"><header><span class="sk" style="width:96px;height:13px"></span></header><span class="sk" style="width:64px;height:26px"></span><span class="sk" style="width:100%;height:11px"></span><span class="sk" style="width:82%;height:11px"></span></article>`).join('');
}
function secError(key, msg){
  return `<div class="state-error" style="grid-column:1/-1"><span style="flex:none">${icon('i-alert','ic-lg')}</span><span style="flex:1;min-width:180px"><b>${esc(SEC_NAME[key])}来源不可用</b><p>${esc(msg)}仅本区块降级；其他分区继续使用各自来源，不刷新整页，也不把旧数据伪装成实时数据。</p></span><button type="button" class="btn btn-secondary btn-sm" data-retry-sec="${key}">${icon('i-refresh','ic-sm')}重试本区块</button></div>`;
}
function secDeny(key){
  const r = R();
  return `<div class="perm-changed" role="alert" style="grid-column:1/-1">${icon('i-shield')}<span><b>权限已变更。</b>${esc(SEC_NAME[key])}域的授权包于 11-11 09:12 到期未续，已被回收。已立即清除相关卡片缓存与深链；旧标签页与旧消息链接再次打开时会重新鉴权并得到同样的拒绝。</span></div>
  <div class="denial" style="grid-column:1/-1">
    <h3>${icon('i-shield','ic-lg')}${esc(SEC_NAME[key])}：权限不足，已拒绝</h3>
    <p>当前账号以【${esc(r.roleLabel)}】访问【${esc(SEC_NAME[key])}】，所需动作【VIEW】；命中的有效范围为【${esc(r.scope)}】；因【${DENY_CODE[key]}】拒绝。规则版本【${esc(r.policy)}】，人员/组织快照时间【2026-11-11 05:30】。</p>
    <p>审计编号 <code>DEC-20261111-0048${key.length}7</code> · 原因码 <code>${DENY_CODE[key]}</code>。解释中不展示其他楼栋记录数量或敏感字段；前端隐藏入口不能替代后端拒绝。</p>
  </div>`;
}
function secStaleNote(){
  return `<div class="perm-changed" style="grid-column:1/-1;margin:0">${icon('i-clock')}<span><b>数据可能延迟。</b>保留最近一次有效数据并后台重试；最近成功时间 11-10 22:14，勿据此判断为零。</span></div>`;
}
function renderSection(k){ ({urgent:renderUrgent, ops:renderOps, biz:renderBiz, people:renderPeople})[k](); }

function cardHtml(c){
  const num = c.drill
    ? `<button type="button" class="drill-num" data-drill="${esc(c.drill)}" aria-label="${esc(c.title)} ${esc(c.line[0])}，下钻同口径明细">${esc(c.line[0])}</button><span class="drill-hint">下钻</span>`
    : `<b class="num">${esc(c.line[0])}</b>`;
  return `<article class="ops-card">
    <header>${icon(c.icon)}${esc(c.title)}</header>
    <div class="ops-line tone-${c.tone}">${num}<span>${esc(c.line[1])}</span></div>
    <ul class="ops-list">${c.list.map(li => `<li class="is-${li.tone}">${icon(li.tone === 'bad' ? 'i-alert' : li.tone === 'warn' ? 'i-clock' : li.tone === 'ok' ? 'i-check' : 'i-info')}<span>${li.text}</span></li>`).join('')}</ul>
    ${c.chart ? miniBars(c.chart.v, c.chart.l, c.chart.u || '') : ''}
    <div class="ops-foot">${esc(c.foot)}</div>
  </article>`;
}
function renderOps(){
  const o = OPS[state.role], sv = SRC.ops, body = $('#ops-body');
  srcChipFor($('#ops-src'), 'ops');
  if (sv === 'down'){ $('#ops-count').textContent = '读取失败'; body.innerHTML = secError('ops', '告警与事件聚合接口超时。'); return; }
  if (sv === 'deny'){ $('#ops-count').textContent = '权限不足'; body.innerHTML = secDeny('ops'); return; }
  $('#ops-count').textContent = `${o.cards.length} 个分区${sv === 'stale' ? ' · 数据延迟' : ''}`;
  body.innerHTML = (sv === 'stale' ? secStaleNote() : '') + o.cards.map(c => cardHtml(sv === 'stale' ? {...c, foot:'可能延迟 · ' + c.foot} : c)).join('');
}
function renderBiz(){
  const b = BIZ[state.role], sv = SRC.biz, body = $('#biz-body');
  srcChipFor($('#biz-src'), 'biz');
  $('#biz-note').textContent = b.note;
  if (sv === 'down'){
    $('#biz-count').textContent = '读取失败';
    body.innerHTML = secError('biz', '采购与合同聚合接口超时。');
    return;
  }
  if (sv === 'deny'){
    $('#biz-count').textContent = '权限不足';
    body.innerHTML = secDeny('biz');
    return;
  }
  $('#biz-count').textContent = `${b.cards.length} 个业务域${sv === 'stale' ? ' · 数据延迟' : ''}`;
  body.innerHTML = (sv === 'stale' ? secStaleNote() : '') + b.cards.map(c => cardHtml(sv === 'stale' ? {...c, foot:'可能延迟 · ' + c.foot} : c)).join('');
}
function renderPeople(){
  const p = PEOPLE[state.role], sv = SRC.people;
  srcChipFor($('#people-src'), 'people');
  const body = $('#people-body');
  if (sv === 'down'){ $('#people-count').textContent = '读取失败'; body.innerHTML = secError('people', '排班与证照聚合接口超时。'); return; }
  if (sv === 'deny'){ $('#people-count').textContent = '权限不足'; body.innerHTML = secDeny('people'); return; }
  $('#people-count').textContent = `2 个分区${sv === 'stale' ? ' · 数据延迟' : ''}`;
  const L = p.left, Rt = p.right;
  body.innerHTML = (sv === 'stale' ? secStaleNote() : '') + `
    <article class="ops-card">
      <header>${icon('i-users')}${esc(L.title)}</header>
      <div class="duty-meter">
        <div class="bar" role="img" aria-label="${esc(L.meterLabel)}"><i style="width:${Math.round(100 * L.meter[0] / L.meter[1])}%"></i></div>
        <span class="ops-foot" style="border:0;padding:0">${esc(L.meterLabel)}</span>
      </div>
      <ul class="ops-list">${L.list.map(li => `<li class="is-${li.tone}">${icon(li.tone === 'bad' ? 'i-alert' : li.tone === 'warn' ? 'i-clock' : 'i-check')}<span>${li.text}</span></li>`).join('')}</ul>
      <div class="ops-foot" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap"><span style="flex:1;min-width:140px">${esc(sv === 'stale' ? '可能延迟 · ' + L.foot : L.foot)}</span><button type="button" class="btn btn-ghost btn-sm" data-perm-fields>${icon('i-shield','ic-sm')}字段脱敏规则</button></div>
    </article>
    <article class="ops-card">
      <header>${icon('i-shield')}${esc(Rt.title)}</header>
      <ul class="ops-list">${Rt.list.map(li => `<li class="is-${li.tone}">${icon(li.tone === 'bad' ? 'i-alert' : li.tone === 'warn' ? 'i-clock' : 'i-check')}<span>${li.text}</span></li>`).join('')}</ul>
      <div class="ops-foot">${esc(sv === 'stale' ? '可能延迟 · ' + Rt.foot : Rt.foot)}</div>
    </article>`;
}
function renderShortcuts(){
  const r = R(), q = state.scq.trim().toLowerCase();
  const ids = r.shortcuts.filter(id => !q || (MODULE_LABEL[id] || id).toLowerCase().includes(q));
  $('#sc-note').textContent = q ? `匹配 ${ids.length} 个入口` : r.scNote;
  $('#shortcuts').innerHTML = ids.length ? ids.map(id => `
    <button type="button" class="shortcut" data-shortcut="${esc(id)}">
      <span class="sc-icon">${icon(MODULE_ICON[id] || 'i-grid')}</span>
      <span>${esc(MODULE_LABEL[id] || id)}</span>
      ${icon('i-chevright','ic-sm')}
    </button>`).join('')
    : `<p class="sc-empty">没有匹配「${esc(state.scq)}」的入口。快捷入口只列出当前角色已授权的现有页面。</p>`;
}

/* ── 抽屉：统一待办对象详情 / 权限解释 / 拒绝演示 ── */
let lastFocus = null;
function openDrawerHtml(kicker, title, tags, body, foot){
  $('#drawer-kicker').innerHTML = kicker;
  $('#drawer-title').textContent = title;
  $('#drawer-tags').innerHTML = tags;
  $('#drawer-body').innerHTML = body;
  $('#drawer-foot').innerHTML = foot;
  lastFocus = document.activeElement;
  $('#drawer').hidden = false;
  document.body.classList.add('drawer-open');
  $('#drawer-close').focus();
}
function closeDrawer(){
  document.body.classList.remove('drawer-open');
  const d = $('#drawer');
  setTimeout(() => { if (!document.body.classList.contains('drawer-open')) d.hidden = true; }, 220);
  if (lastFocus && lastFocus.focus) lastFocus.focus();
}
function announce(m){ $('#live').textContent = m; }
const sect = (ic, title, inner) => `<section class="drawer-sect"><h3>${icon(ic)}${esc(title)}</h3>${inner}</section>`;
const dl = pairs => `<dl class="dl">${pairs.filter(Boolean).map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${v}</dd></div>`).join('')}</dl>`;

function openTodo(key){
  const [role, p, idx] = key.split(':');
  const it = URGENT[role].filter(x => x.p === p)[Number(idx)];
  const id = 'TD-20261111-' + String(Number(idx) + 1).padStart(3, '0');
  openDrawerHtml(
    `${icon(ICON[it.kind] || 'i-clipboard','ic-sm')}<span>统一待办对象</span><span aria-hidden="true">·</span><span class="num">${id}</span><span aria-hidden="true">·</span><span>${esc(it.source)}</span>`,
    it.title,
    `<span class="tag tag-${it.statusTone}">${esc(it.status)}</span><span class="tag tag-${it.p === 'P0' ? 'bad' : it.p === 'P1' ? 'warn' : 'neutral'}">${it.p}</span><span class="tag tag-neutral">${esc(it.scope)}</span>${it.multi ? `<span class="tag tag-info">多来源 ${it.multi.length}</span>` : ''}`,
    sect('i-clipboard', '待办对象字段（§8.3）', dl([
      ['id / type / source', `<span class="num">${id}</span> · ${esc(it.kind)} · ${esc(it.source)}`],
      ['scope', esc(it.scope)],
      ['priority / status', `${it.p} · ${esc(it.status)}`],
      ['dueAt', `<span class="num">${esc(it.due)}</span>（${esc(it.sla)}）`],
      ['owner', '见原模块指派（工作台只读，不展示跨范围人员）'],
      ['sourceStatus', '<span class="tag tag-ok">正常</span>'],
      ['actionUrl', `<code style="font-family:ui-monospace,Menlo,monospace;font-size:var(--fs-sm)">/module/${esc(it.kind)}?id=${id}&scope=${encodeURIComponent(it.scope)}</code>`]
    ]))
    + sect('i-info', '为什么出现在我的工作台（reason）', `<p class="muted" style="margin:0;font-size:var(--fs-body);line-height:1.7">${esc(it.reason)}</p>`)
    + sect('i-layers', '来源与去重（mergedSourceIds）', (() => {
      const fi = URGENT[role].indexOf(it);
      const ref = (OWNERS[role + ':' + fi] || ['', id])[1] || id;
      const srcs = it.multi || [it.source];
      return `<ul class="perm-list" style="margin:0">${srcs.map((s, k) => `<li>${icon('i-check')}<span><b>${esc(s)}</b> · mergedSourceId <code>${esc(ref)}${srcs.length > 1 ? '@' + (k + 1) : ''}</code> · sourceStatus 正常</span></li>`).join('')}
        <li>${icon('i-info')}<span>dedupKey = 对象ID + 事项类型 + 数据范围；同一对象多来源合并为一条，<b>不因合并丢失证据</b>。冲突时主业务系统状态优先，优先级取最高、截止取最早，并标记「多来源」。</span></li></ul>`;
    })()),
    `<span class="grow">${icon('i-info','ic-sm')} 工作台只读聚合；处理动作跳回原业务模块并由后端再次鉴权</span>
     <button type="button" class="btn btn-secondary">${icon('i-upright','ic-sm')}进入原模块（深链）</button>
     <button type="button" class="btn btn-primary">${esc(it.action)}</button>`
  );
  announce('已打开待办详情：' + it.title);
}
function openPerm(showFields){
  const r = R();
  openDrawerHtml(
    `${icon('i-shield','ic-sm')}<span>权限解释</span><span aria-hidden="true">·</span><span>${esc(r.roleCode)}</span>`,
    '为什么我能看到这些内容',
    `<span class="tag tag-brand">${esc(r.roleLabel)}</span><span class="tag tag-neutral">策略 ${esc(r.policy)}</span>`,
    sect('i-shield', '裁决依据', `<ul class="perm-list" style="margin:0">${PERM[state.role].map(t => `<li>${icon('i-check')}<span>${t}</span></li>`).join('')}</ul>`)
    + (showFields ? sect('i-shield', '字段敏感度与脱敏（§8.10.8）', `<table class="f-table">
        <caption class="sr-only">字段敏感度四级与当前角色可见性</caption>
        <thead><tr><th scope="col">等级</th><th scope="col">数据示例</th><th scope="col">当前角色（${esc(r.roleLabel)}）可见性</th></tr></thead>
        <tbody>
          <tr><td class="lv lv-f0">F0 公共运营</td><td>楼栋状态、事件数量、当班状态</td><td>可见 · 仍按行级范围过滤</td></tr>
          <tr><td class="lv lv-f1">F1 内部运维</td><td>设备、巡检、工单、施工进度</td><td>可见 · 楼栋 ∩ 专业 ∩ 记录关系</td></tr>
          <tr><td class="lv lv-f2">F2 敏感业务</td><td>合同金额、付款账号、联系人 <span class="masked">138****4021</span></td><td>${['manager','gadmin'].includes(state.role) ? '白名单字段可见 · 其余脱敏' : '仅摘要或脱敏 · 原件另授权'}</td></tr>
          <tr><td class="lv lv-f3">F3 高敏安全</td><td>权限策略、110kV 详细操作信息、资质</td><td>不可见 · 需专项授权包 + 双人复核 + 全量审计</td></tr>
        </tbody></table>
        <p class="hero-foot" style="border:0;padding-top:9px">人员池中的身份证号、手机号明文、邮箱、生日、学历、照片不属于日常鉴权输入，权限服务不预加载；所有导出写入导出人、时间、规则版本、范围与水印编号。</p>`) : '')
    + sect('i-alert', '拒绝与降级规则', `<ul class="perm-list" style="margin:0">
        <li>${icon('i-alert')}<span><b>DENY_SCOPE_MISMATCH</b> —— 对象楼栋/专业不在授权范围：列表、详情、统计、导出均不返回，且不泄露存在性。</span></li>
        <li>${icon('i-clock')}<span><b>DENY_NOT_ON_DUTY</b> —— 值班执行动作无有效当班实例：允许必要历史查看，禁止处置与提交。</span></li>
        <li>${icon('i-shield')}<span><b>DENY_SOURCE_STALE</b> —— 人员源超 24 小时：冻结新增授权，审批/导出/敏感写操作失败关闭。</span></li></ul>`)
    + sect('i-clipboard', '审计', `<p class="muted" style="margin:0;font-size:var(--fs-sm);line-height:1.7">每次裁决记录 decision_id、principal、角色来源、命中策略、数据范围、动作与允许/拒绝原因；不记录令牌与敏感凭据。最近一次裁决 <code style="font-family:ui-monospace,Menlo,monospace">DEC-20261111-004817 ALLOW</code>。</p>`),
    `<span class="grow">${icon('i-info','ic-sm')} 前端隐藏入口只改善体验，安全边界在后端</span>
     ${state.role === 'bsup' ? '<button type="button" class="btn btn-danger-quiet" data-deny-demo>模拟越权访问 B 楼</button>' : ''}
     <button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
  );
  announce('已打开权限解释');
}
function openDenial(){
  openDrawerHtml(
    `${icon('i-shield','ic-sm')}<span>拒绝演示</span><span aria-hidden="true">·</span><span class="num">${esc(DENIAL.decision)}</span>`,
    '无权访问：' + DENIAL.object,
    `<span class="tag tag-bad">${esc(DENIAL.code)}</span><span class="tag tag-neutral">动作 ${esc(DENIAL.action)}</span>`,
    `<div class="denial"><h3>${icon('i-alert','ic-lg')}服务端拒绝，前端不渲染任何 B 楼数据</h3>
      <p>${esc(DENIAL.text)}</p>
      <p>对象标签：<code>${esc(DENIAL.objectTags)}</code>。响应不返回记录、数量或字段占位；审计编号 <code>${esc(DENIAL.decision)}</code> 可用于申诉与排障。</p></div>`
    + sect('i-info', '下一步', `<ul class="perm-list" style="margin:0">
        <li>${icon('i-check')}<span>如确需跨楼协作，走<b>临时授权</b>：记录授权人、审批依据、起止时间，到期自动回收。</span></li>
        <li>${icon('i-info')}<span>园区公共事项若影响 A 楼，会单独标注并只展示 A 楼受影响部分。</span></li></ul>`),
    `<span class="grow">${icon('i-shield','ic-sm')} 失败关闭：权限源异常时高风险动作一律拒绝</span>
     <button type="button" class="btn btn-secondary" data-close-drawer>关闭</button>`
  );
  announce('已打开越权拒绝演示');
}

/* ══════════════════════════════════════════════════════════════
   交互层
   ══════════════════════════════════════════════════════════════ */
function pressIn(segId, attr, value){ $$('#' + segId + ' > button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset[attr] === value))); }
function setAppearance(v){
  state.appearance = v; document.documentElement.dataset.appearance = v;
  pressIn('seg-appearance', 'appearance', v);
  $$('[data-appearance-toggle]').forEach(b => {
    b.setAttribute('aria-label', v === 'light' ? '切换深色外观' : '切换浅色外观');
    b.innerHTML = `<svg class="ic ic-lg" aria-hidden="true"><use href="${HASH}${v === 'light' ? 'i-moon' : 'i-sun'}</use></svg>`;
  });
}
function setViewport(v){ state.viewport = v; document.documentElement.dataset.viewport = v; pressIn('seg-viewport', 'viewport', v); }
function setRole(v){
  state.role = v; Object.keys(SRC).forEach(k => SRC[k] = 'ok'); state.empty = false;
  pressIn('seg-role', 'role', v); pressIn('seg-src', 'src', 'ok');
  renderAll();
  announce('已切换身份：' + R().roleLabel + '，数据范围 ' + R().scope);
}
function setSrc(v){ SRC[state.sec] = v; pressIn('seg-src', 'src', v); renderSection(state.sec); announce(SEC_NAME[state.sec] + ' 来源状态：' + SRC_MAP[v][2]); }
function setTargetSec(v){ state.sec = v; pressIn('seg-target', 'targetSec', v); announce('降级目标分区：' + SEC_NAME[v] + '，当前状态 ' + SRC_MAP[SRC[v]][2]); }
function renderAll(){
  const r = R();
  $('#page-title').textContent = r.title;
  $('#page-sub').textContent = r.sub;
  $('#person-name').textContent = r.person;
  $('#person-avatar').textContent = r.avatar;
  document.title = `灯塔 · ${r.title}（V0.1 评审稿对齐）— 高保真原型`;
  renderSticky(); renderIdentity(); renderNumbers(); renderHero(); renderUrgent(); renderOps(); renderBiz(); renderPeople(); renderShortcuts();
  const badge = $('#bell-badge');
  badge.textContent = (MSGS[state.role] || []).length;
  $('#btn-msg').setAttribute('aria-label', '消息与催办，' + badge.textContent + ' 条未读');
}

$('#seg-role').addEventListener('click', e => { const b = e.target.closest('button[data-role]'); if (b) setRole(b.dataset.role); });
$('#seg-viewport').addEventListener('click', e => { const b = e.target.closest('button[data-viewport]'); if (b) setViewport(b.dataset.viewport); });
$('#seg-appearance').addEventListener('click', e => { const b = e.target.closest('button[data-appearance]'); if (b) setAppearance(b.dataset.appearance); });
$('#seg-src').addEventListener('click', e => { const b = e.target.closest('button[data-src]'); if (b) setSrc(b.dataset.src); });
$('#seg-target').addEventListener('click', e => { const b = e.target.closest('button[data-target-sec]'); if (b) setTargetSec(b.dataset.targetSec); });
document.addEventListener('click', e => { if (e.target.closest('[data-appearance-toggle]')) setAppearance(state.appearance === 'light' ? 'dark' : 'light'); });

document.addEventListener('click', e => {
  if (e.target.closest('[data-perm-fields]')){ openPerm(true); return; }
  if (e.target.closest('[data-perm]')){ openPerm(false); return; }
  if (e.target.closest('[data-deny-demo]')){ openDenial(); return; }
  if (e.target.closest('[data-close-drawer]') || e.target.closest('#drawer-close') || e.target.closest('#scrim')){ closeDrawer(); return; }
  const rb = e.target.closest('[data-reason]');
  if (rb){
    const reason = rb.parentElement.querySelector('.reason');
    const open = reason.classList.toggle('is-open');
    rb.setAttribute('aria-expanded', String(open));
    announce(open ? '已展开原因说明' : '已收起原因说明');
    return;
  }
  const todo = e.target.closest('[data-todo]'); if (todo){ openTodo(todo.dataset.todo); return; }
  const num = e.target.closest('.focus-card[data-drill]');
  if (num){
    const d = num.dataset.drill;
    if (d === 'ref'){ announce('该数字为历史口径统计，不提供同口径明细，按规划不作为可操作 KPI'); return; }
    if (d.startsWith('ops:')){ openDrill('ops|' + d.slice(4)); return; }
    const map = { urgent:'#urgent-block', ops:'#ops-block', biz:'#biz-block', people:'#people-block' };
    const el = document.querySelector(map[d.slice(6)] || '#urgent-block');
    if (!el) return;
    el.scrollIntoView({behavior:'smooth', block:'start'});
    if (el.animate) el.animate([{boxShadow:'0 0 0 0 color-mix(in srgb, var(--seed-primary) 40%, transparent)'},{boxShadow:'0 0 0 4px color-mix(in srgb, var(--seed-primary) 34%, transparent)'},{boxShadow:'0 0 0 0 transparent'}],{duration:1100,easing:'ease-out'});
    announce('已定位到同口径明细区块');
    return;
  }
  const rs = e.target.closest('[data-retry-sec]');
  if (rs){ SRC[rs.dataset.retrySec] = 'ok'; renderSection(rs.dataset.retrySec); announce(SEC_NAME[rs.dataset.retrySec] + ' 已重新读取'); return; }
  if (e.target.closest('[data-empty-toggle]')){ state.empty = !state.empty; e.target.closest('[data-empty-toggle]').setAttribute('aria-pressed', String(state.empty)); renderUrgent(); announce(state.empty ? '演示：无待办空态' : '演示：恢复待办'); return; }
  if (e.target.closest('#btn-msg')){ openMessages(); return; }
  if (e.target.closest('[data-export]')){ openExport(); return; }
  if (e.target.closest('[data-shadow-toggle]')){
    const p = $('#shadow-panel'); p.hidden = !p.hidden;
    e.target.closest('[data-shadow-toggle]').setAttribute('aria-pressed', String(!p.hidden));
    if (!p.hidden) p.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  if (e.target.closest('[data-shadow-close]')){ $('#shadow-panel').hidden = true; const b = document.querySelector('[data-shadow-toggle]'); if (b) b.setAttribute('aria-pressed','false'); return; }
  if (e.target.closest('[data-contract-toggle]')){
    const p = $('#contract-panel'); p.hidden = !p.hidden;
    e.target.closest('[data-contract-toggle]').setAttribute('aria-pressed', String(!p.hidden));
    if (!p.hidden) p.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  if (e.target.closest('[data-contract-close]')){ $('#contract-panel').hidden = true; const b = document.querySelector('[data-contract-toggle]'); if (b) b.setAttribute('aria-pressed','false'); return; }
  const dr = e.target.closest('[data-drill]');
  if (dr){ openDrill(dr.dataset.drill); return; }
  const sc = e.target.closest('[data-shortcut]');
  if (sc){ announce('原型演示：深链进入 ' + sc.textContent.trim() + '（目标页由后端再次校验范围）'); return; }
  if (e.target.closest('#btn-refresh')){
    /* §8.6/§11.1：立即本地反馈（各分区芯片转「更新中」），已有数据保留，异步刷新后恢复 */
    $$('#urgent-src, #ops-src, #biz-src, #people-src, #sticky-src').forEach(c => {
      const key = { 'urgent-src':'urgent', 'ops-src':'ops', 'biz-src':'biz', 'people-src':'people' }[c.id];
      if (key && SRC[key] !== 'ok') return;
      c.className = 'src src-busy'; c.innerHTML = icon('i-refresh') + '更新中…';
    });
    announce('已给出本地反馈，正在异步刷新各分区');
    setTimeout(() => { renderAll(); announce('各分区已刷新完成'); }, 650);
    return;
  }
  if (e.target.closest('#btn-advanced')){
    const p = $('#advanced-panel'); p.hidden = !p.hidden;
    if (!p.hidden) p.scrollIntoView({behavior:'smooth', block:'center'});
    return;
  }
  if (e.target.closest('[data-close-advanced]')){ $('#advanced-panel').hidden = true; return; }
  if (e.target.closest('#assistant-tab')){ announce('原型演示：打开灯塔助手会话（现有常驻入口，不在本次重设计范围）'); return; }
});
document.addEventListener('input', e => { if (e.target.id === 'sc-q'){ state.scq = e.target.value; renderShortcuts(); } });
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && document.body.classList.contains('drawer-open')){ closeDrawer(); return; }
  /* 抽屉内焦点陷阱：Tab 循环不逸出模态 */
  if (e.key === 'Tab' && document.body.classList.contains('drawer-open')){
    const f = Array.from($('#drawer').querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'))
      .filter(el => !el.disabled && el.offsetParent !== null);
    if (!f.length) return;
    const first = f[0], last = f[f.length - 1];
    if (e.shiftKey && document.activeElement === first){ e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last){ e.preventDefault(); first.focus(); }
  }
});

/* 初始渲染 */
setAppearance('light'); setViewport('desktop'); setRole('manager');

