// Offline contracts copied from check-workspace-{notices,mop,repairs,water}.mjs
// and check-cabinet-details-browser.mjs. Importing this module starts no server/test.
import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const execute = promisify(execFile);
const codes = ['A', 'B', 'C', 'D', 'E', 'H', '110'];
const kinds = ['maintenance', 'change', 'repair', 'polling', 'adjust', 'power'];
const labels = ['维保通告', '变更通告', '检修通告', '设备轮巡', '设备调整', '上下电通告'];
const zone = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit' });
const day = zone.format(new Date());
const stamp = day + ' 09:30:00';
const now = Date.now() / 1000;
const clone = value => structuredClone(value);
const ok = data => ({ status: 200, json: { ok: true, data: clone(data) } });
const failure = (message, status = 404) => ({ status, json: { ok: false, error: message } });
const blank = () => ({ status: 204, contentType: 'text/plain', body: '' });
const image = () => ({ status: 200, contentType: 'image/svg+xml', body: '<svg xmlns="http://www.w3.org/2000/svg" width="800" height="600" viewBox="0 0 800 600"><rect width="800" height="600" fill="#dbe8ef"/><rect x="80" y="80" width="640" height="400" rx="8" fill="#f4f8fb" stroke="#456477"/><text x="120" y="180" font-family="sans-serif" font-size="30" fill="#243c4d">OFFLINE PHOTO FIXTURE</text><text x="120" y="240" font-family="sans-serif" font-size="22" fill="#456477">Responsive layout preview only</text></svg>' });
const textFile = name => ({ status: 200, contentType: 'text/plain; charset=utf-8', body: 'OFFLINE PREVIEW ONLY: ' + name + '\nNot a real exported workbook.' });
const concrete = value => codes.includes(value) ? value : 'A';
const inScope = (row, scope) => !scope || ['ALL', 'CAMPUS'].includes(scope) || row.scope === scope || row.building_codes?.includes(scope);
const number = (url, key, fallback) => Math.max(1, Math.floor(Number(url.searchParams.get(key)) || fallback));
function paginate(items, url, size = 30) {
  const pageSize = number(url, 'page_size', size), page = number(url, 'page', 1);
  const offset = url.searchParams.has('offset') ? Math.max(0, Number(url.searchParams.get('offset')) || 0) : (page - 1) * pageSize;
  return { records: items.slice(offset, offset + pageSize), total: items.length, offset, page, page_size: pageSize };
}
function parsedBody(value) {
  if (value == null) return {};
  if (typeof value === 'object' && !Buffer.isBuffer(value)) return value;
  try { return JSON.parse(String(value)); } catch { return {}; }
}

const user = { open_id: 'ou_responsive_fixture', name: '布局验收管理员', role: 'admin', level: 'L0', category: 'manager', category_label: '经理', login_method: 'password',
  home_scope: 'A', can_manage_accounts: true, can_delete_business: true, scopes: ['ALL', ...codes],
  modules: [...kinds, 'event', 'engineer_mop', 'repair_management', 'cabinet', 'water', 'assistant', 'critical_guard', 'risk_inspection', 'daily', 'plan_convergence', 'drills', 'learning', 'knowledge', 'links', 'life_guide', 'signatures', 'handover', 'work_handover', 'accounts'] };
const scopes = [{ value: 'ALL', label: '全部楼栋' }, ...codes.map(value => ({ value, label: value === '110' ? '110站' : value + '楼' }))];
const people = ['测试实施人', '测试审核人（较长姓名与岗位）', '待签名人员'].map((name, i) => ({ record_id: 'audit-person-' + i,
  user_id: 'audit-person-' + i, open_id: 'ou_audit_person_' + i, name, employee_no: 'TEST00' + i, building: 'A楼',
  source: 'staff', selectable: true, has_signature: i !== 2, usage_confirmed: i !== 2 }));
const layouts = new Map(), bindings = new Map(), jobs = new Map(), sopRows = new Map(), drafts = new Map();
let serial = 0;

// One promise per scope: concurrent requests share the same isolated Python render.
const noticeCache = new Map();
async function noticesFor(scope) {
  scope = concrete(scope);
  if (!noticeCache.has(scope)) noticeCache.set(scope, renderNotices(scope).catch(error => { noticeCache.delete(scope); throw error; }));
  return noticeCache.get(scope);
}
async function renderNotices(scope) {
  const temp = await mkdtemp(path.join(tmpdir(), 'lighthouse-responsive-'));
  const python = String.raw`
import copy,json,os,socket,sys
def blocked(*args,**kwargs):
    raise AssertionError('responsive fixture forbids networking')
socket.socket.connect=blocked
socket.socket.connect_ex=blocked
socket.create_connection=blocked
sys.path.insert(0,os.path.join(os.getcwd(),'bin'))
from lan_bitable_template_portal.workbench_lite import render_workbench_lite,extract_workbench_lite_fragments
from test_lighthouse_notice_command_regression import VALID_STARTS
scope,day=sys.argv[1:3]
pages={}
for kind,base in VALID_STARTS.items():
    label={'maintenance':'维保','change':'变更','repair':'设备检修','power':'上电','polling':'设备轮巡','adjust':'设备调整'}[kind]
    draft=copy.deepcopy(base)
    for key in ('title','location','content','reason','impact'):
        if isinstance(draft.get(key),str): draft[key]=draft[key].replace('A楼',scope+'楼')
    draft.update(scope=scope,building_codes=[scope],start_time=day+'T09:00',end_time=day+'T18:30')
    records=[];ongoing=[]
    for i in range(3):
        title='EA118机房'+scope+'楼'+label+['冷却水系统及相关设备联合核验（跨系统长标题与现场恢复确认）','BA系统资料核对','待补充设备事项'][i]
        record_id='audit-plan-'+scope+'-'+kind+'-'+str(i)
        active_id='audit-active-'+scope+'-'+kind+'-'+str(i)
        target_id='audit-target-'+scope+'-'+kind+'-'+str(i)
        fields={'名称':title,'维护总项':title,'楼栋':scope+'楼','专业':'' if i==2 else '暖通','计划进度':'未开始'}
        records.append({**draft,'title':title,'record_id':record_id,'source_record_id':record_id,'work_type':kind,'scope':scope,'building':scope+'楼','source_progress':'未开始','display_fields':fields})
        ongoing.append({**draft,'title':title+'（进行中）','record_id':target_id,'target_record_id':target_id,'active_item_id':active_id,'source_record_id':record_id,'work_type':kind,'scope':scope,'building':scope+'楼','status':'开始','site_photo_count':1 if i==0 else 0})
    args=dict(payload={'records':records,'ongoing':ongoing},session={'role':'admin','can_delete_business':True,'allowed_modules':list(VALID_STARTS),'user':{'name':'布局验收管理员','open_id':'ou_responsive_fixture'}},scope=scope,work_type=kind,month=str(int(day[5:7]))+'月',scope_options=[{'value':scope,'label':scope+'楼'}])
    html=render_workbench_lite(**args)
    details={}
    details['_manual']=extract_workbench_lite_fragments(render_workbench_lite(**{**args,'payload':{'records':records,'ongoing':[]}},manual=True))['detail']
    for row in records:
        details[row['record_id']]=extract_workbench_lite_fragments(render_workbench_lite(**args,record_id=row['record_id']))['detail']
    for row in ongoing:
        details[row['active_item_id']]=extract_workbench_lite_fragments(render_workbench_lite(**args,active_item_id=row['active_item_id']))['detail']
    pages[kind]={'html':html,'details':details,'fragments':extract_workbench_lite_fragments(html),'payload':{**args['payload'],'records_pagination':{'total':3,'page':1,'page_size':30},'ongoing_pagination':{'total':3,'page':1,'page_size':30}}}
print(json.dumps(pages,ensure_ascii=False))
`;
  try {
    const { stdout } = await execute(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-B', '-s', '-c', python, scope, day], {
      cwd: root, windowsHide: true, timeout: 120000, maxBuffer: 32 * 1024 * 1024,
      env: { ...process.env, CLIPFLOW_DATA_DIR: temp, PYTHONIOENCODING: 'utf-8', PYTHONDONTWRITEBYTECODE: '1' },
    });
    return JSON.parse(stdout);
  } finally {
    if (path.dirname(temp) === path.resolve(tmpdir()) && path.basename(temp).startsWith('lighthouse-responsive-')) await rm(temp, { recursive: true, force: true });
  }
}

const field = (field_name, field_type = 1, extra = {}) => ({ field_name, field_type, editable: true, ...extra });
const projectFields = [field('维修名称'), field('故障发生时间', 5), field('故障维修原因'), field('所属专业', 3, { options: ['电气', '暖通'] }),
  field('所属数据中心/楼栋-使用', 3, { options: codes.map(code => code + '楼') }), field('维修结束时间（2026）-L', 5), field('当前维修进度', 2), field('备注')];
const followupFields = [field('维修进展描述'), field('维修进度', 2), field('设备名称'), field('设备编号'), field('维修方', 3, { options: ['我方', '厂家'] }),
  field('随工人员（我方维修人员）', 11), field('跟进项（如有）'), field('后续整改措施（如有）')];
const projects = codes.flatMap(scope => ['配电设备通信异常检修（联合排查与长标题展示）', '冷却水压力传感器维修', '故障资料待补充'].map((title, i) => ({
  record_id: 'audit-project-' + scope + '-' + i, title: scope + '楼' + title, scope, building: scope + '楼', building_codes: [scope],
  specialty: i === 2 ? '' : i === 0 ? '电气' : '暖通', workflow: i === 2 ? '未开始' : '维修中', progress_percent: [35, 70, 0][i],
  followup_count: i === 2 ? 0 : 3, fault_time: i === 2 ? '' : stamp, summary_only: true, is_completed: false,
})));
const followups = [35, 55, 70].map((progress, i) => ({ record_id: 'audit-followup-' + i, title: ['完成现场核验，等待更换通信模块及备件调拨', '厂家复测正常，继续观察', '待补充现场说明'][i],
  progress: progress / 100, created_time: stamp, ledger_device_ids: ['audit-device-0'], ledger_devices: [{ record_id: 'audit-device-0', '设备编号': 'A-101', '设备名称': '配电设备', '机楼': 'A楼' }],
  raw_fields: { '维修进展描述': i === 2 ? '' : '现场检查、备件更换与运行状态验证。', '维修进度': progress / 100, '设备名称': '配电设备' },
  display_fields: { '维修进展描述': i === 2 ? '' : '现场检查、备件更换与运行状态验证。', '维修进度': progress + '%', '设备名称': '配电设备' } }));
function fullProject(row) {
  return { ...row, summary_only: false, record_version: 'audit-v1', source_event_id: 'audit-event-' + row.scope + '-0', source_repair_ids: ['audit-target-' + row.scope + '-repair-0'],
    raw_fields: {}, display_fields: { '维修名称': row.title, '故障发生时间': row.fault_time, '故障维修原因': row.fault_time ? '通信模块异常，等待备件及厂家联合核验。' : '',
      '所属专业': row.specialty, '所属数据中心/楼栋-使用': row.building, '维修结束时间（2026）-L': '', '当前维修进度': row.progress_percent + '%' } };
}

const mopTitles = ['冷却塔月度维保保养记录单（设备逐项检查与长标题签名测试）', 'BA系统资料维护记录单', '空调过滤网维护'];
const mopNotices = codes.flatMap(scope => mopTitles.map((title, i) => ({ notice_key: 'audit-mop-notice-' + scope + '-' + i,
  source_record_id: 'audit-plan-' + scope + '-maintenance-' + i, title: 'EA118机房' + scope + '楼' + title, building: scope + '楼', scope,
  specialty: i === 2 ? '' : '暖通', status: i === 1 ? '已结束' : '开始', maintenance_cycle: '每月', start_time: day + ' 09:00', end_time: i === 2 ? '' : day + ' 18:30',
  mop_uploaded: false, mop_attachment_count: 0 })));
const mopCandidates = mopTitles.map((title, i) => ({ record_id: 'audit-mop-' + i, title, file_no: 'JSNT-MOP-' + (301 + i), specialty: i === 2 ? '' : '暖通', maintenance_type: '每月',
  version: 'V1.2', file_status: '有效', attachment_count: 1, fields: { 文件编号: 'JSNT-MOP-' + (301 + i), 专业: '暖通', 维护类型: '每月' },
  attachments: [{ file_token: 'audit-mop-file-' + i, name: title + '.xlsx' }] }));
const mopSheetRows = [['设备维护记录表', '', '', '', '', '', ''], ['维护开始时间', '', '', '', '', '', ''], ['维护完成时间', '', '', '', '', '', ''], ['审核确认时间', '', '', '', '', '', ''],
  ...Array.from({ length: 6 }, () => ['', '', '', '', '', '', '']), ['序号', '设备编号', '检查项目', '检查要求', '正常/异常', '检查结果', '备注'],
  ...Array.from({ length: 24 }, (_, i) => [String(i + 1), 'A-CH-' + String(i + 1).padStart(3, '0'), ['运行状态', '水系统', '电气检查'][i % 3], '按作业指导书核查设备运行状态及保护功能', '', '', ''])];
const sheet = { name: '维护记录', is_cover: false, row_count: mopSheetRows.length, column_count: 7, rows: mopSheetRows, merged_cells: [],
  checkbox_cells: [{ row: 11, col: 4, cell_ref: 'E12' }, { row: 12, col: 4, cell_ref: 'E13' }],
  maintenance_fields: ['维护开始时间', '维护完成时间', '审核确认时间'].map((label, i) => ({ row: i + 1, label_col: 0, value_col: 1, label, value_cell_ref: 'B' + (i + 2) })) };

function eventsFor(url) {
  const month = /^\d{4}-\d{2}$/.test(url.searchParams.get('month') || '') ? url.searchParams.get('month') : day.slice(0, 7);
  return codes.flatMap(scope => [0, 1, 2].map(i => ({ record_id: 'audit-event-' + scope + '-' + i, title: scope + '楼' + ['冷却水流量过低告警排查（现场设备与控制逻辑联合验证）', '配电设备通信故障', '设备异常待补充'][i],
    building: scope + '楼', building_codes: [scope], scope, occurrence_time: month === day.slice(0, 7) ? day + ' 0' + (7 + i) + ':00:00' : month + '-' + String(i + 1).padStart(2, '0') + ' 09:00:00',
    status: i === 1 ? '已结束' : '处理中', level: i === 2 ? '' : i === 0 ? 'I2' : 'I3', under_repair: i === 0, transfer_to_overhaul: i === 0 ? '是' : '否',
    alarm_desc: i === 2 ? '' : '现场核查流量开关、阀位、控制回路及机组运行状态，持续跟进恢复结果。',
    display_fields: { '事件名称': scope + '楼设备异常', '事件来源': i === 2 ? '' : '动环监控', '当前进度': i === 2 ? '' : '现场排查中' }, timeline: [{ time: stamp, content: '发现告警，开始现场核验' }, { time: stamp, content: '隔离数据：设备状态复测' }] })));
}
const eventStats = records => ({ total: records.length, under_repair: records.filter(row => row.under_repair).length, i2_or_higher: records.filter(row => row.level === 'I2').length, i3: records.filter(row => row.level === 'I3').length });

function racksFor(scope) {
  return [0, 1, 2].map(i => ({ room: i === 2 ? '202' : '201', rack: scope + '0' + (i + 1), state: ['formal', 'test', 'off'][i], rack_type: i === 1 ? '网络机柜' : '服务器机柜', power: i === 2 ? null : 4200, positions: [scope + (i + 1)] }));
}
function cabinetOverview(scope) {
  const racks = racksFor(scope), counts = { total: 3, powered: 2, formal: 1, test: 1, off: 1, unknown: 0 };
  const rooms = ['201', '202'].map((id, i) => ({ id, name: id + '机房', sheet: id, total: i ? 1 : 2, counts: { formal: i ? 0 : 1, test: i ? 0 : 1, off: i ? 1 : 0, unknown: 0 },
    types: { '服务器机柜': 1, '网络机柜': i ? 0 : 1 } }));
  return { version: 1, updated_at: stamp, record_count: 3, inventory_only: 0, issues: [], counts, rooms, racks,
    sheet_formats: [{ sheet: '机柜上电时间统计', groups: [{}], columns: [{ column: 1, field: 'room', label: '包间', width: 100 }, { column: 2, field: 'rack', label: '机柜', width: 100 }, { column: 3, field: 'rack_type', label: '类型', width: 160 }] }] };
}
function operationsFor(scope) {
  return racksFor(scope).map((rack, i) => ({ ...rack, record_id: 'audit-operation-' + scope + '-' + i, scope, source: '机柜上电时间统计', source_row: i + 8, category: 'up', version: 'audit-v1', issues: [],
    groups: [{ id: 'audit-group-' + i, action: '上正式电', expected: day + ' 08:00', actual: i === 2 ? '' : stamp, result: i === 2 ? '' : '成功' }],
    events: [{ id: 'audit-rack-event-' + i, action: '上正式电', expected: day + ' 08:00', actual: i === 2 ? '' : stamp, result: i === 2 ? '' : '成功' }] }));
}
const batchRows = racksFor('A').map((rack, i) => ({ ...rack, row_id: 'audit-batch-row-' + i, scope: 'A', action: '上正式电', expected: stamp, actual: i === 2 ? '' : stamp,
  result: i === 2 ? '' : '成功', status: i === 0 ? 'completed' : 'ready', editable: i !== 0, confirmable: i === 1, current_power_state: i === 0 ? 'formal' : 'off', issues: i === 2 ? ['待补充实际操作时间和结果'] : [], evidence_images: [] }));
let batch = { batch_id: 'audit-batch', source: 'text', created_at: stamp, status: 'ready', version: 1, scopes: ['A'], rows: batchRows, images: [], warnings: [], can_confirm_all: true, can_download_files: true, stats: { total: 3, completed: 1, confirmable: 1 } };
const cabinetExports = [...'ABCDE'].map(scope => ({ scope, status: 'succeeded', result: { export_id: 'audit-export-' + scope, filename: scope + '楼机柜上下电记录.xlsx', created_at: stamp, cloud_upload_status: 'succeeded', file_available: true } }));

// Local-only plan-convergence contracts; importing these fixtures performs no queries.
const planDevices = codes.flatMap(scope => [0, 1].map(i => ({ scope, zone: '演示园区', building: scope + '楼',
  floor: scope + '-1F', room: scope + '-101机房', obj_name: i ? '楼宇自控' : '暖通空调',
  inst_name: scope + (i ? '-BAS-01' : '-CRAH-01'), ins_id: 'audit-plan-device-' + scope + '-' + i,
  position: scope + '楼 / ' + scope + '-101机房' })));
const planRules = planDevices.map(device => ({ ...device, alarm_name: '演示设备运行异常告警',
  alarm_config_id: 'audit-alarm-' + device.ins_id, classify_model: device.obj_name, rule_desc: '仅用于隔离页面验收的告警规则' }));
const planBlocks = codes.flatMap((scope, building) => [0, 1, 2, 3].map(i => ({
  blockId: 81001 + building * 10 + i, blockName: '演示' + scope + '楼' + ['空调检修屏蔽', '控制系统变更屏蔽', '机房设备状态核验屏蔽（较长说明）', '已完成作业屏蔽'][i],
  scope, status: i === 3 ? '0' : '1', userName: '演示操作员', startTime: stamp, creatTime: stamp,
  endTime: i === 3 ? day + ' 11:30:00' : '',
  alarmBlockDetailResultList: planDevices.filter(device => device.scope === scope).map((device, index) => ({
    blockDetailId: 'audit-detail-' + scope + '-' + i + '-' + index, classifyModel: device.obj_name,
    classifyModelId: 'audit-type-' + index, domainCode: 'audit-domain-' + scope,
    spaceModel: device.room, spaceModelId: 'audit-room-' + scope, relateConfig: '演示设备运行异常告警',
    alarmConfigId: 'audit-alarm-' + device.ins_id, instances: device.inst_name, instanceIds: device.ins_id,
  })),
})));
const planSets = [0, 1].map(i => ({ id: 9101 + i, name: ['演示空调检修规则集', '演示控制系统变更规则集'][i],
  remark: '隔离验收假数据，不连接智航或飞书', version: 'audit-ruleset-1', item_count: 2,
  items: planDevices.filter(device => device.scope === 'A').map(device => ({ scope_type: 'device',
    inst_name: device.inst_name, ins_id: device.ins_id, obj_name: device.obj_name,
    rule_type: i ? 'common' : 'normal', rule_group_no: 1, rule_label: '演示A楼设备范围' })) }));
const planNotices = Object.fromEntries(['maintenance', 'change'].map(kind => [kind, codes.flatMap(scope => [0, 1, 2].map(i => {
  const block = planBlocks.find(row => row.scope === scope && row.blockName.includes(kind === 'change' ? '变更' : '检修'));
  return { record_id: 'audit-convergence-' + kind + '-' + scope + '-' + i, scope, building: scope + '楼',
    name: '演示' + scope + '楼' + (kind === 'change' ? '控制系统变更' : '空调设备检修') + ['已匹配', '待核对屏蔽', '待自动核对'][i],
    status: i ? '更新' : '开始', device: scope + (kind === 'change' ? '-BAS-01' : '-CRAH-01'),
    rooms: [scope + '-101'], location: scope + '-101机房', fault: '演示设备状态核验', start_time: stamp,
    ...(scope === 'D' || scope === 'E' ? { check_status: 'skipped' } : i === 2 ? { auto_check_status: 'pending' }
      : { auto_check_status: 'ready', auto_checked_at: now, hits: i ? [] : [{ blockId: block.blockId, blockName: block.blockName, reason: '演示设备与机房匹配' }] }),
  };
}))]));
function planCatalog(query) {
  const selected = planDevices.filter(row => ['zone', 'building', 'floor', 'room'].every(key => !query.get(key) || row[key] === query.get(key))
    && (!query.get('inst') || row.inst_name === query.get('inst'))
    && (!(query.get('objs') || query.get('obj')) || (query.get('objs') || query.get('obj')).split(',').includes(row.obj_name))
    && (!query.get('rooms') || query.get('rooms').split(',').some(room => room.split('|').every((part, index) => !part || row[['zone', 'building', 'floor', 'room'][index]] === part))));
  const kind = query.get('kind') || 'types', fields = { types: 'obj_name', zones: 'zone', buildings: 'building', floors: 'floor', rooms: 'room' };
  let items;
  if (fields[kind]) {
    items = [...new Set(selected.map(row => row[fields[kind]]))].map(value => ({ [fields[kind]]: value, devices: selected.filter(row => row[fields[kind]] === value).length }));
  } else if (kind === 'devices') items = selected;
  else if (kind === 'rules') items = planRules.filter(row => selected.some(device => device.ins_id === row.ins_id));
  else return null;
  const keyword = (query.get('kw') || '').trim().toLowerCase();
  if (keyword) items = items.filter(row => Object.values(row).join(' ').toLowerCase().includes(keyword));
  const limit = Math.min(500, Math.max(1, Number(query.get('limit')) || 60)), offset = Math.max(0, Number(query.get('offset')) || 0);
  return { items: items.slice(offset, offset + limit), total: items.length, limit, has_more: offset + limit < items.length };
}

const waterScopes = [...'ABCDEH'];
const waterRows = waterScopes.flatMap((scope, building) => {
  // Keep the unrecorded weekly sample out of the overview's daily-only totals.
  const current = [0, 1, 2].map(i => ({ record_id: 'audit-water-' + scope + '-' + i, scope,
    statistic_date: day, statistic_date_key: day,
    meter: scope + '楼' + ['生活水表（楼栋主进水及生活配水系统）', '冷却补水表', '待记录水表'][i],
    frequency: i === 2 ? '周' : '日', shift: i === 2 ? '夜' : '白',
    meter_value: i === 2 ? null : [1200, 2500][i], corrected_usage: null,
    computed_usage: i === 2 ? null : [25 + building, 30 + building][i], photo_count: i === 0 ? 1 : 0,
    ...(i === 1 ? { large_change: { requires_confirmation: true, ratio_percent: 100 } } : {}) }));
  const history = Array.from({ length: 29 }, (_, index) => {
    const offset = index + 1, date = new Date(Date.parse(day + 'T00:00:00Z') - offset * 86400000).toISOString().slice(0, 10);
    return current.slice(0, 2).map((row, meter) => ({ ...row,
      record_id: 'audit-water-' + scope + '-history-' + offset + '-' + meter,
      statistic_date: date, statistic_date_key: date, meter_value: row.meter_value - offset * 20,
      computed_usage: 22 + meter * 7 + building + offset % 5, photo_count: 1, large_change: undefined }));
  }).flat();
  return [...current, ...history];
});
function waterRecords(query) {
  const terms = (query.get('q') || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
  return waterRows.filter(row => inScope(row, query.get('scope'))
    && (!query.get('month') || row.statistic_date_key.startsWith(query.get('month')))
    && (!query.get('start_date') || row.statistic_date_key >= query.get('start_date'))
    && (!query.get('end_date') || row.statistic_date_key <= query.get('end_date'))
    && ['meter', 'frequency', 'shift'].every(key => !query.get(key) || row[key] === query.get(key))
    && terms.every(term => [row.meter, row.statistic_date, row.frequency, row.shift].join(' ').toLowerCase().includes(term)));
}
function waterSummary(rows) {
  return { record_count: rows.length, total_usage: Number(rows.reduce((sum, row) => sum + (row.corrected_usage ?? row.computed_usage ?? 0), 0).toFixed(3)),
    photo_records: rows.filter(row => row.photo_count > 0).length,
    latest_date_ms: Math.max(0, ...rows.map(row => Date.parse(row.statistic_date_key + 'T00:00:00+08:00'))) };
}
function waterDetail(row) {
  return { ...row, version: 'audit-v1', previous_date_text: day, previous_value_text: '1175', yoy_ratio: 0.05,
    photos: row.photo_count ? [{ image_id: 'audit-water-photo', name: '水表现场照片.svg' }] : [], edit_policy: { is_admin: true, can_edit: true, remaining_edits: null } };
}
function sopList(scope, kind) {
  const key = scope + ':' + kind;
  if (!sopRows.has(key)) sopRows.set(key, [0, 1, 2].map(i => ({ sop_id: 'audit-sop-' + scope + '-' + kind + '-' + i, scope, work_type: kind, version: 1,
    name: scope + '楼' + (labels[kinds.indexOf(kind)] || '通告') + ['设备状态核验与多系统联合恢复操作标准作业流程（长标题）', '常规检查标准工单', '现场确认工单'][i], mode: 'normal',
    attachments: i === 2 ? [] : [{ attachment_id: 'audit-sop-file-' + i, file_name: '标准作业指导书与现场设备操作确认附件.txt' }],
    steps: [0, 1, 2].map(s => ({ step_id: 'step-' + s, content: ['核对设备编号、现场位置、运行方式及操作范围，记录操作前状态。', '根据作业指导书完成操作，并通知审核人员确认。', '复查运行状态，恢复现场并记录结果。'][s], operator_required: true, reviewer_required: s !== 2, photo_required: s === 1, time_limit_seconds: s ? 0 : 300, delay_reminder_minutes: 0, repeat_rules: s === 2 ? [{ rule_id: 'repeat-one', from_step_id: 'step-0', to_step_id: 'step-1', count: 4 }] : [] })) })));
  return sopRows.get(key);
}

/** A local-only fake HTTP handler. Unknown routes return null: the caller must
 * fail closed, never proxy an unmatched request to a real backend or website. */
export async function coreFixture({ url, method = 'GET', body }) {
  if (!(url instanceof URL)) throw new TypeError('coreFixture requires a URL object');
  method = method.toUpperCase();
  const p = url.pathname, q = url.searchParams, data = parsedBody(body), scope = concrete(q.get('scope') || data.scope);
  if (p === '/api/health') return { status: 200, json: { ok: true, service: 'clipflow_backend', instance_id: 'responsive-audit-static-only' } };
  if (p === '/api/auth/status' && method === 'GET') return ok({ logged_in: true, user, scope_options: scopes });
  if (p === '/api/auth/login' && method === 'GET') return failure('隔离预览不进行真实登录', 403);
  if (p.startsWith('/api/workspace-layouts/')) {
    const current = layouts.get(p) || { revision: 0, value: null };
    if (method === 'GET') return ok(current);
    if (method !== 'PUT') return null;
    if (data.revision !== current.revision) return failure('隔离预览：布局版本已变化，请重新读取。', 409);
    const next = { revision: current.revision + 1, value: data.value }; layouts.set(p, next); return ok(next);
  }
  if (p === '/api/assistant/appearance') return ok({ shape: 'weather-lighthouse-mini', color: 'encre', size: 56 });
  if (p === '/api/assistant/icon-weather') return ok({ available: false, city: '' });
  if (p === '/api/assistant/conversation') return ok({ conversation_id: 'responsive-audit', configured: false, enabled: false, busy: false, turns: [], model_options: [] });
  if (p === '/api/scope-overview') return ok({ scopes: Object.fromEntries(['ALL', ...codes].map(code => [code, {
    event_total: code === 'ALL' ? 21 : 3, event_processing: code === 'ALL' ? 14 : 2,
    ...Object.fromEntries(kinds.flatMap(kind => [[kind + '_pending', code === 'ALL' ? 21 : 3], [kind + '_ongoing', code === 'ALL' ? 21 : 3]])),
    ongoing_titles: [], ongoing_title_count: code === 'ALL' ? 126 : 18,
  }])), scope_options: scopes, source_snapshot_ready: true, last_loaded_at: stamp });
  if (p === '/api/history-summary') return ok({ days: [{ date: day, items: kinds.map((work_type, i) => ({ record_id: 'audit-history-' + i, title: scope + '楼' + labels[i] + '历史操作核验', work_type, status: i === 1 ? '已结束' : '开始', scope, time: stamp })) }] });
  if (p === '/api/bootstrap') {
    const pages = await noticesFor(scope), records = Object.values(pages).flatMap(page => page.payload.records), ongoing = Object.values(pages).flatMap(page => page.payload.ongoing);
    return ok({ scope, scope_label: scope + '楼', scope_options: scopes, default_scope: 'A', default_work_type: 'maintenance', source_snapshot_ready: true,
      last_loaded_at: stamp, payload_version: 'responsive-audit-v1', warnings: [], maintenance_status: '未开始', field_order: ['名称', '楼栋', '专业', '计划进度'],
      fields: ['名称', '楼栋', '专业', '计划进度'].map(field_name => ({ field_name, ui_type: 'text', type: 1, options: [], has_formula: false, is_primary: field_name === '名称' })),
      filters: { default_month: Number(day.slice(5, 7)) + '月', months: Array.from({ length: 12 }, (_, i) => (i + 1) + '月'), specialties: ['暖通', '电气'], buildings: codes.map(code => code + '楼') },
      records, ongoing, zhihang_change_records: [], daily_summary: { items: [] }, maintenance_options: {}, defaults: {},
      record_type_counts: Object.fromEntries(kinds.map(kind => [kind, 3])), ongoing_type_counts: Object.fromEntries(kinds.map(kind => [kind, 3])), auth: { user } });
  }
  if (p === '/api/signatures/people' || p === '/api/signatures/management/mop-list') return ok({ people, signatures: [], count: people.length });
  if (p === '/api/signatures/temporary/people') return ok({ people: [], count: 0 });
  if (p === '/api/signatures/temporary/list') return ok({ signatures: [], items: [] });
  if (/^\/api\/signatures\/(?:images|people)\//.test(p) && method === 'GET') return image();

  if (p === '/workbench-lite' || p === '/api/workbench' || ['/api/workbench/lite-detail', '/api/workbench/lite-fragment'].includes(p)) {
    const pages = await noticesFor(scope), kind = kinds.includes(q.get('work_type')) ? q.get('work_type') : 'maintenance', page = pages[kind];
    if (p === '/workbench-lite') return { status: 200, contentType: 'text/html; charset=utf-8', body: page.html };
    if (p === '/api/workbench') {
      if (q.get('work_type') !== 'all') return ok(page.payload);
      const all = Object.values(pages); return ok({ records: all.flatMap(v => v.payload.records), ongoing: all.flatMap(v => v.payload.ongoing), records_pagination: { total: 18, page: 1, page_size: 30 }, ongoing_pagination: { total: 18, page: 1, page_size: 30 } });
    }
    const id = q.get('active_item_id') || q.get('record_id');
    const detail = page.details[q.get('manual') === '1' ? '_manual' : id] || page.details['audit-plan-' + scope + '-' + kind + '-0'];
    return ok({ canonical_url: '/workbench-lite?' + q, fragments: p.endsWith('lite-detail') ? { detail } : { ...page.fragments, detail } });
  }
  if (p === '/api/workbench/draft') {
    const values = method === 'GET' ? Object.fromEntries(q) : data;
    const key = JSON.stringify(['scope', 'work_type', 'target_record_id', 'action'].map(key => values[key] || ''));
    if (method === 'GET') return ok({ draft: drafts.get(key) || null });
    if (method === 'PUT') { const draft = { ...data, version: (drafts.get(key)?.version || 0) + 1 }; drafts.set(key, draft); return ok({ draft }); }
    return null;
  }
  if (p === '/api/workbench/planned-notice-prefill') return ok({ selected: { source_record_id: q.get('record_id') }, draft: {}, recommendations: [{ work_type: q.get('work_type') || 'maintenance', source_building: scope === 'A' ? 'B' : 'A', source_title: '同类通告历史参考（隔离数据）', source_updated_at: stamp,
    suggested_values: { content: scope + '楼设备运行状态与资料核对', reason: '按周期开展核查', impact: '现场核实后填写实际影响范围', execution_party: '自维' }, needs_site_confirmation: ['impact'] }] });
  if (p === '/api/workbench/source-options') return ok({ items: [0, 1, 2].map(i => ({ source_record_id: 'audit-plan-' + scope + '-maintenance-' + i, title: scope + '楼源表事项' + (i + 1), building: scope + '楼', maintenance_cycle: i === 2 ? '' : '每月', progress: '未开始' })) });
  if (p === '/api/notice-target-candidates') return ok({ candidates: [0, 1, 2].map(i => ({ target_record_id: 'audit-target-' + scope + '-maintenance-' + i, title: scope + '楼目标通告关联核验' + (i + 1), building: scope + '楼', status: '开始' })) });
  if (p === '/api/workbench/repair-event-candidates') return ok({ records: projects.filter(row => row.scope === scope).map(row => ({ repair_management_record_id: row.record_id, event_record_id: 'audit-event-' + scope + '-0', title: row.title, building: row.building })), total: 3 });
  if (p === '/api/notice-identity/bind' || p === '/api/workbench/repair-event-bind') return ok({ ...data, active_item_id: data.active_item_id || 'audit-active-' + scope + '-maintenance-0' });
  if (p === '/api/workbench/polling-delay-status') return ok({ state: '', due_at_ts: 0, enabled: false, missing_completion: false });
  if (/^\/api\/(maintenance|change|repair)-refresh$/.test(p)) return ok({ completed: true, mock_external: true });
  if (p === '/api/notice-alert-tags') return ok({ status: 'ready', title: scope + '楼设备状态核验', action: 'update', tags: [{ label: '设备轮巡', content: '冷却系统运行状态核验' }] });
  if (p === '/api/notice-image-drafts' && method === 'GET') return ok({ items: [], images: [], uploads: [] });
  if (p === '/api/qt-active-items/stream' && method === 'GET') return blank();
  if (p === '/api/notice-attachments') return ok({ upload_id: 'audit-upload-' + (++serial), local_image_id: 'a'.repeat(32), file_name: q.get('file_name') || '现场照片.svg', preview_url: '/api/notice-images/' + 'a'.repeat(32) });
  if (p.startsWith('/api/notice-images/')) return image();
  if (p === '/api/change-confirmations') return ok({ can_confirm: true, items: [0, 1, 2].map(i => ({ target_record_id: 'audit-change-' + i, building_codes: [scope], building: scope + '楼', title: scope + '楼待确认变更事项' + i, state: ['missing_screenshot', 'awaiting_confirmation', 'confirmed'][i], local_screenshot_items: [] })) });
  if (/^\/api\/change-confirmations\/[^/]+\/(screenshot|confirm)$/.test(p)) return ok({});
  if (p === '/api/workbench-actions' && method === 'POST') { const job_id = 'audit-job-' + (++serial); jobs.set(job_id, { ...data, job_id, phase: 'success', message_sent: true, record_id: 'audit-target-saved', target_record_id: 'audit-target-saved', notice_text: '隔离预览：模拟完成，未发送真实通告' }); return ok({ job_id, phase: 'success' }); }
  if (p === '/api/notice-undo/available') return ok({ items: [0, 1, 2].map(i => ({ undo_id: 'audit-undo-' + i, title: scope + '楼通告历史操作' + (i + 1), scope, work_type: 'maintenance', undo_label: '回退上一步结束', operated_at: stamp })) });
  if (/^\/api\/notice-undo\/[^/]+\/apply$/.test(p)) { const job_id = 'audit-undo-job-' + (++serial); jobs.set(job_id, { job_id, phase: 'success', scope, work_type: 'maintenance' }); return ok({ job_id, phase: 'success' }); }
  if (p === '/api/jobs/batch') return ok({ items: (q.get('ids') || '').split(',').flatMap(id => jobs.has(id) ? [jobs.get(id)] : []), missing: [], denied: [] });
  if (/^\/api\/jobs\/[^/]+(?:\/retry)?$/.test(p)) { const job = jobs.get(p.split('/')[3]); return job ? ok(job) : failure('隔离任务不存在'); }

  if (p === '/api/polling-sops' && method === 'GET') return ok({ items: sopList(scope, kinds.includes(q.get('work_type')) ? q.get('work_type') : 'maintenance') });
  if (p === '/api/polling-sops' && method === 'POST') { const rows = sopList(concrete(data.scope), data.work_type || 'maintenance'), item = { ...data, sop_id: 'audit-sop-created-' + (++serial), version: 1, attachments: [] }; rows.push(item); return ok(item); }
  if (p.startsWith('/api/polling-sops/')) {
    const id = p.split('/')[3], rows = [...sopRows.values()].find(items => items.some(item => item.sop_id === id));
    const index = rows?.findIndex(item => item.sop_id === id); if (!rows || index < 0) return failure('隔离SOP不存在');
    if (p.includes('/attachments/') && method === 'GET') return textFile('SOP attachment');
    if (method === 'DELETE' && !p.includes('/attachments')) { rows.splice(index, 1); return ok({ deleted: true }); }
    if (method === 'PUT') Object.assign(rows[index], data, { version: rows[index].version + 1 });
    if (p.endsWith('/attachments') && method === 'POST') { rows[index].attachments.push({ attachment_id: 'audit-sop-upload-' + (++serial), file_name: '上传附件（隔离预览）.txt' }); rows[index].version++; }
    if (p.includes('/attachments/') && method === 'DELETE') { rows[index].attachments = rows[index].attachments.filter(file => file.attachment_id !== p.split('/')[5]); rows[index].version++; }
    return ok(rows[index]);
  }
  if (p === '/api/events/monthly') { const records = eventsFor(url).filter(row => inScope(row, q.get('scope'))); return ok({ records, stats: eventStats(records), snapshot_exists: true, last_refreshed_at: now, unknown_time_count: 0 }); }
  if (p === '/api/events/overview') { const records = eventsFor(url); return ok({ stats: eventStats(records), building_stats: codes.map(code => ({ scope: code, building: code + '楼', ...eventStats(records.filter(row => row.scope === code)) })), last_refreshed_at: now, snapshot_exists: true, unknown_time_count: 0 }); }
  if (p === '/api/events/transfer-repair' || p === '/api/events/refresh') return ok({ mock_external: true, completed: true });

  if (p === '/api/engineer/mop/bootstrap') return ok({ notices: mopNotices.filter(row => inScope(row, q.get('scope'))).map(row => ({ ...row, ...(bindings.get(row.notice_key) || {}) })), mop_candidates: mopCandidates, warnings: [], auth: { user } });
  if (p === '/api/engineer/mop/bind') { bindings.set(data.notice_key, data); return ok({ binding: data }); }
  if (p === '/api/engineer/mop/upload-local') return ok({ local_mop_candidate: { ...mopCandidates[0], record_id: 'audit-local-mop', source: 'local_upload', local_upload: true, upload_id: 'audit-local-upload', attachments: [{ upload_id: 'audit-local-upload', file_token: 'audit-local-file', name: mopTitles[0] + '.xlsx' }] }, file_name: mopTitles[0] + '.xlsx', warnings: [] });
  if (p === '/api/engineer/mop/preview') return ok({ local_file: { path: 'audit.xlsx', relative_path: 'engineer_mop_files/audit/preview.xlsx', file_name: mopTitles[0] + '.xlsx' }, mop_title: mopTitles[0], attachment: { name: mopTitles[0] + '.xlsx' }, sheets: [sheet, { name: '封面', is_cover: true, row_count: 1, column_count: 2, rows: [['设备维护作业', scope + '楼']] }], warnings: [] });
  if (p === '/api/engineer/mop/fill') return q.get('download') === '1' ? textFile('filled-mop.xlsx') : ok({ path: 'audit-filled.xlsx', relative_path: 'engineer_mop_files/audit/filled.xlsx', file_name: '维护记录单（隔离填写）.xlsx', filled_file: { path: 'audit-filled.xlsx', relative_path: 'engineer_mop_files/audit/filled.xlsx', file_name: '维护记录单（隔离填写）.xlsx' }, sheets: [sheet], warnings: [] });
  if (p === '/api/engineer/mop/reset') return ok({ local_file: { path: 'audit.xlsx', relative_path: 'engineer_mop_files/audit/preview.xlsx', file_name: mopTitles[0] + '.xlsx' }, sheets: [sheet], warnings: [] });
  if (p === '/api/engineer/mop/upload-signed') return ok({ uploaded_at: stamp, notice_key: data.notice_key, source_record_id: data.source_record_id });

  if (p === '/api/repair-management/stream') return blank();
  if (p === '/api/repair-management/changes') return ok({ changes: [], cursor: 0 });
  if (p === '/api/repair-management/status') { const records = projects.filter(row => inScope(row, q.get('scope'))); return ok({ stats: { total: records.length, without_followup: records.filter(row => !row.followup_count).length, in_progress: records.filter(row => row.followup_count).length, completed_total: 0 }, records: [], total: records.length }); }
  if (p === '/api/repair-management/records' && method === 'GET') return ok({ fields: projectFields, ...paginate(projects.filter(row => inScope(row, q.get('scope')) && (q.get('state') !== 'completed' || row.is_completed)), url) });
  if (/^\/api\/repair-management\/records\/[^/]+$/.test(p)) { const row = projects.find(item => item.record_id === p.split('/')[4]); if (!row) return failure('隔离维修项目不存在'); if (method === 'GET') return ok({ record: fullProject(row) }); if (method === 'PUT') return ok({ record_id: row.record_id, fields: data.fields, record_version: 'audit-v2' }); if (method === 'DELETE') return ok({ record_id: row.record_id, deleted: true }); }
  if (p === '/api/repair-management/followups') { if (method === 'GET') return ok({ fields: followupFields, records: followups, total: followups.length, shared_fields: {}, brand_model_options: {}, device_brand_model_options: {} }); if (method === 'POST') return ok({ record_id: 'audit-followup-created-' + (++serial), fields: data.fields, record_version: 'audit-v2' }); }
  if (/^\/api\/repair-management\/followups\/[^/]+$/.test(p)) return ok({ record_id: p.split('/')[4], fields: data.fields || {}, record_version: 'audit-v2', deleted: method === 'DELETE' });
  if (p === '/api/repair-management/ledger-candidates') return ok({ records: [0, 1, 2].map(i => ({ record_id: 'audit-device-' + i, '设备编号': scope + '-10' + i, '设备名称': i === 2 ? '冷却水系统压力传感器（远端机房长名称）' : '配电设备', '机楼': scope + '楼', '系统名称': '配电', '大设备类型': '配电柜', '品牌': '', '型号': '' })), total: 3, page: 1, options: { '机楼': [scope + '楼'], '系统名称': ['配电'], '大设备类型': ['配电柜'] }, cache: { ready: true, refreshing: false } });
  if (p === '/api/repair-management/people') return ok({ people, total: people.length });
  if (p === '/api/repair-management/event-candidates') return ok({ records: eventsFor(url).filter(row => row.scope === scope), total: 3 });
  if (p === '/api/repair-management/repair-candidates') return ok({ records: projects.filter(row => row.scope === scope), total: 3 });
  if (p === '/api/repair-management/prefill') return ok({ fields: fullProject(projects.find(row => row.scope === scope)).display_fields, warnings: [] });
  if (p === '/api/repair-management/health') return ok({ pending_count: 0, failed_count: 0 });
  if (p === '/api/repair-management/sync-status') return ok({ status: 'completed' });
  if (p.startsWith('/api/repair-management/operations/')) return ok({ status: 'completed', retryable: false });

  if (p === '/api/cabinet-power/bootstrap') return ok({ status: 'succeeded', ready: 5, total: 5, buildings: [...'ABCDE'].map(scope => ({ scope, status: 'succeeded' })) });
  if (p === '/api/cabinet-power/buildings') return ok({ buildings: [...'ABCDE'].map(scope => ({ scope, bootstrap_status: 'succeeded', ...cabinetOverview(scope) })) });
  if (p === '/api/cabinet-power/overview') return ok(cabinetOverview(scope));
  if (p === '/api/cabinet-power/racks') return ok({ version: 1, items: racksFor(scope) });
  if (/^\/api\/cabinet-power\/rooms\/[^/]+\/layout$/.test(p)) { const room = p.split('/')[4]; return ok({ layout: { width: 900, height: 600, cells: [...racksFor(scope).filter(row => row.room === room).map((rack, i) => ({ ...rack, ref: rack.rack, x: 100 + i * 150, y: 90, width: 110, height: 80, text: rack.rack, color: rack.state === 'formal' ? '#f00' : '#00b050', style: {} })), { ref: 'label', x: 100, y: 270, width: 300, height: 60, text: '恒湿机 / 空调设备区域', style: {} }] } }); }
  if (p === '/api/cabinet-power/operations') { const items = operationsFor(scope).filter(row => (!q.get('room') || row.room === q.get('room')) && (!q.get('rack') || row.rack === q.get('rack'))); if (method === 'GET') return ok({ items, total: items.length, page: 1, page_size: 50, rack_state: racksFor(scope).find(row => row.rack === q.get('rack')) }); return ok({ write_id: 'audit-write', status: 'succeeded', result: { record_id: 'audit-operation-saved' } }); }
  if (p === '/api/cabinet-power/writes') return ok({ items: [] });
  if (/^\/api\/cabinet-power\/(?:operations\/[^/]+|rack-power|writes\/[^/]+(?:\/(?:resume|reconcile))?)$/.test(p)) return ok({ write_id: 'audit-write', status: 'succeeded', result: { record_id: 'audit-operation-saved' } });
  if (p === '/api/cabinet-power/export-schedule') return ok({ enabled: true, day: 1, time: '08:00', server_time: stamp, next_run: day + ' 08:00', ...data });
  if (p === '/api/cabinet-power/storage') return ok({ files: 3, bytes: 1024, deleted: 0, bytes_removed: 0 });
  if (p === '/api/cabinet-power/export-history') return ok({ items: cabinetExports.map(item => ({ ...item.result, scope: item.scope })), total: 5, page: 1 });
  if (/^\/api\/cabinet-power\/export-batches(?:\/[^/]+(?:\/resume)?)?$/.test(p)) return ok({ batch_id: 'audit-export-batch', status: 'succeeded', items: cabinetExports, total: 5, completed: 5, cloud_upload_status: 'succeeded', created_at: stamp, updated_at: stamp });
  if (p === '/api/cabinet-power/exports' && method === 'POST') return ok({ job_id: 'audit-export-job', status: 'succeeded', export_id: 'audit-export-A' });
  if (p.startsWith('/api/cabinet-power/jobs/')) return ok({ job_id: 'audit-export-job', status: 'succeeded', result: cabinetExports[0].result });
  if (/^\/api\/cabinet-power\/exports\/[^/]+\/download$/.test(p)) return textFile('cabinet-export.xlsx');
  if (p === '/api/cabinet-power/batches') { if (method === 'GET') return ok({ items: [batch, { ...batch, batch_id: 'audit-batch-2', status: 'completed' }, { ...batch, batch_id: 'audit-batch-3', status: 'draft' }], total: 3, pending_count: 2, page: 1, page_size: 20 }); return ok(batch); }
  if (p === '/api/cabinet-power/batches/text-preview') return ok({ rows: (data.sources || []).map((source, i) => ({ ...batchRows[i % 3], row_id: 'audit-parsed-' + i, text_id: source.id, text_row: 1 })) });
  if (/^\/api\/cabinet-power\/batches\/[^/]+\/status$/.test(p)) return ok({ version: batch.version, status: batch.status, stats: batch.stats });
  if (/^\/api\/cabinet-power\/batches\/[^/]+$/.test(p)) { if (method === 'PATCH') { for (const patch of data.rows || []) { const row = batch.rows.find(row => row.row_id === patch.row_id); if (row) Object.assign(row, patch); } for (const row of batch.rows) if (data.row_ids?.includes(row.row_id)) Object.assign(row, data.common || {}); batch.version++; } return ok({ ...batch, batch_id: p.split('/')[4] }); }
  if (/^\/api\/cabinet-power\/batches\/[^/]+\/(?:confirm|cancel|rollback|restore-rows|clear-overlaps)$/.test(p)) return ok(batch);

  if (p === '/api/plan-convergence/bootstrap' && method === 'GET') return ok({ is_admin: true,
    catalog_ready: true, points_ready: true, zh_ready: true, counts: { devices: planDevices.length, rules: planRules.length },
    blocks: { items: planBlocks, loaded_at: now } });
  if (p === '/api/plan-convergence/blocks' && method === 'GET') return ok({ items: planBlocks.filter(row => inScope(row, q.get('scope'))), loaded_at: now });
  if (p === '/api/plan-convergence/catalog' && method === 'GET') {
    const catalog = planCatalog(q); return catalog ? ok(catalog) : failure('隔离目录不支持此查询类型', 400);
  }
  if (p === '/api/plan-convergence/rulesets' && method === 'GET') return ok(planSets);
  let planMatch = p.match(/^\/api\/plan-convergence\/blocks\/(\d+)$/);
  if (planMatch && method === 'GET') {
    const block = planBlocks.find(row => String(row.blockId) === planMatch[1]);
    return block ? ok(block) : failure('隔离屏蔽记录不存在');
  }
  planMatch = p.match(/^\/api\/plan-convergence\/(maintenance|change)\/(records|check)$/);
  if (planMatch && (planMatch[2] === 'records' && method === 'GET' || planMatch[2] === 'check' && method === 'POST')) {
    const kind = planMatch[1], records = planNotices[kind].filter(row => inScope(row, q.get('scope') || data.scope));
    if (planMatch[2] === 'records') return ok(records);
    const selected = records.filter(row => !data.record_id || row.record_id === data.record_id);
    if (data.record_id && !selected.length) return failure('隔离通告记录不存在');
    for (const row of selected) {
      if (row.check_status === 'skipped') continue;
      const block = planBlocks.find(block => block.scope === row.scope && block.blockName.includes(kind === 'change' ? '变更' : '检修'));
      Object.assign(row, { check_status: 'ready', checked_at: now, check_source: 'manual',
        hits: row.record_id.endsWith('-1') ? [] : [{ blockId: block.blockId, blockName: block.blockName, reason: '演示设备与机房匹配' }] });
    }
    return ok({ records: selected, stats: { [kind]: selected.length, blocks: planBlocks.filter(row => row.status === '1').length,
      matched_records: selected.filter(row => row.hits?.length).length,
      orphan_blocks: planBlocks.filter(block => block.status === '1' && !selected.some(row => row.hits?.some(hit => hit.blockId === block.blockId))).length } });
  }
  planMatch = p.match(/^\/api\/plan-convergence\/rulesets\/(\d+)(?:\/(expand|match))?$/);
  if (planMatch && (method === 'GET' && planMatch[2] !== 'match' || method === 'POST' && planMatch[2] === 'match')) {
    const set = planSets.find(row => String(row.id) === planMatch[1]);
    if (!set) return failure('隔离规则集不存在');
    const devices = planDevices.filter(row => set.items.some(item => item.ins_id === row.ins_id));
    if (planMatch[2] === 'expand') return ok({ devices, count: devices.length });
    if (planMatch[2] === 'match') {
      const block = planBlocks.find(row => String(row.blockId) === String(data.block_id));
      if (!block) return failure('隔离屏蔽记录不存在');
      const missing = devices.filter(row => !block.alarmBlockDetailResultList.some(detail => detail.instanceIds === row.ins_id));
      return ok({ passed: !missing.length, set_device_count: devices.length, record_device_count: block.alarmBlockDetailResultList.length,
        common: { groups: [] }, rules: { groups: [{ group_no: 1, label: '演示设备范围', matched: !missing.length,
          device_count: devices.length, missing_count: missing.length, missing }] }, not_found_devices: [] });
    }
    return ok(set);
  }
  if (p === '/api/plan-convergence/snapshots' && method === 'POST') {
    const block = planBlocks.find(row => String(row.blockId) === String(data.blockId));
    const detail = block?.alarmBlockDetailResultList.find(row => row.blockDetailId === data.blockDetailId);
    if (!detail) return failure('隔离屏蔽明细不存在');
    return ok(planDevices.filter(row => row.ins_id === detail.instanceIds).map(row => ({ insName: row.inst_name, insStandardId: row.ins_id, objName: row.obj_name })));
  }
  if (p === '/api/plan-convergence/rule-view' && method === 'POST') return ok(planRules.map(row => ({
    alarmName: row.alarm_name, ruleName: row.rule_desc, alarmConfigId: row.alarm_config_id, enable: true })));
  if (p === '/api/plan-convergence/points' && method === 'GET') return ok(planDevices.filter(row => !q.get('name') || row.inst_name === q.get('name')).map(row => ({
    inst_id: row.ins_id, inst_name: row.inst_name, location: row.position, points: 2, alarms: 1 })));
  planMatch = p.match(/^\/api\/plan-convergence\/points\/([^/]+)$/);
  if (planMatch && method === 'GET') {
    const device = planDevices.find(row => row.ins_id === decodeURIComponent(planMatch[1]));
    return device ? ok(['运行状态', '故障告警'].map((name, i) => ({ point_name: '演示' + name, point_id: device.ins_id + '-point-' + i, has_alarm: i === 1 }))) : failure('隔离设备不存在');
  }
  if (p === '/api/plan-convergence/settings' && method === 'GET') return ok({ has_token: true, expired: false,
    display_name: '演示智航账号', token_owner: '隔离验收', expires_at: now + 86400 });
  if (p === '/api/plan-convergence/settings/browser-login' && method === 'GET') return ok({ status: 'idle', message: '', job_id: '' });

  if (p === '/api/capacity/water/buildings') return ok({ month: day.slice(0, 7), scopes: waterScopes.map(value => {
    const summary = waterSummary(waterRecords(new URLSearchParams({ scope: value, month: day.slice(0, 7) })));
    return { value, label: value + '楼', month_record_count: summary.record_count, month_total_usage: summary.total_usage,
      record_count: waterRows.filter(row => row.scope === value).length, latest_date_ms: summary.latest_date_ms };
  }), snapshot: { exists: true, refreshing: false } });
  if (p === '/api/capacity/water/bootstrap') return ok({ scope, month: day.slice(0, 7),
    summary: waterSummary(waterRecords(new URLSearchParams({ scope, month: day.slice(0, 7) }))),
    options: { meters: [...new Set(waterRows.filter(row => row.scope === scope).map(row => row.meter))], frequencies: ['日', '周', '月'], shifts: ['白', '夜'] },
    snapshot: { exists: true, snapshot_version: 1, refreshing: false, refreshed_at: now }, permissions: { is_admin: true, can_create: true } });
  if (p === '/api/capacity/water/records' && method === 'GET') return ok({ ...paginate(waterRecords(q), url), snapshot: { exists: true } });
  if (p === '/api/capacity/water/uploads') return ok({ upload_id: 'audit-water-upload-' + (++serial) });
  if (/^\/api\/capacity\/water\/records\/[^/]+$/.test(p)) { const row = waterRows.find(row => row.record_id === p.split('/')[5]); if (!row) return failure('隔离水耗记录不存在'); if (method === 'GET') return ok(waterDetail(row)); if (method === 'PATCH') { Object.assign(row, data); return ok({ record_id: row.record_id, warnings: [] }); } }
  if (p === '/api/capacity/water/records' && method === 'POST') return ok({ record_id: 'audit-water-created-' + (++serial), warnings: [] });
  if (p.startsWith('/api/capacity/water/images/')) return image();
  if (p === '/api/capacity/water/refresh') return ok({ completed: true, mock_external: true });
  return null;
}
