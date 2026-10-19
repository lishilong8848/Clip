// Synthetic, process-local fixtures only. No imports, network, storage or browser setup.
// Shapes follow check-workspace-accounts, check-account-signatures,
// check-signature-management-preview, check-assistant-shared-models and check-workspace-history.
const scopes = ['110', ...'ABCDEH'];
const scopeOptions = scopes.map(value => ({value, label: value === '110' ? '110站' : value + '楼'}));
const moduleOptions = [
  ['maintenance', '维保通告'], ['change', '变更通告'], ['repair', '检修通告'], ['polling', '设备轮巡'],
  ['adjust', '设备调整'], ['power', '上下电通告'], ['event', '事件管理'], ['engineer_mop', '维护单管理'],
  ['repair_management', '维修项目'], ['cabinet', '机柜管理'], ['daily', '每日任务'], ['water', '水耗管理'],
  ['critical_guard', '重保管理'], ['risk_inspection', '风险排查'], ['drills', '演练管理'], ['learning', '画像学练'],
  ['plan_convergence', '计划收敛审查'], ['signatures', '签名管理'], ['knowledge', '共享知识库'],
  ['links', '多维表导航'], ['life_guide', '参考人生指南'], ['handover', '交接班审核'],
  ['work_handover', '人员工作交接'], ['assistant', '灯塔助手'], ['accounts', '账号与设置'],
].map(([value, label], index) => ({value, label, group: index < 6 ? '通告办理' : '业务管理'}));
const modules = moduleOptions.map(row => row.value);
const categoryOptions = [
  ['manager', '经理'], ['supervisor', '主管'], ['engineer', '工程师'], ['shift_leader', '值班长'],
  ['operator', '值班员'], ['duty', '固定值班账号'], ['other', '其他'],
].map(([value, label]) => ({value, label}));
const levelOptions = ['L1', 'L2', 'L3', 'L4'].map(value => ({value, label: value + ' ' + {
  L1: '全楼管理员', L2: '工程师编辑', L3: '本楼主管', L4: '本楼基础',
}[value]}));
const now = Math.floor(Date.now() / 1000);
const dateText = new Date(now * 1000).toISOString().slice(0, 10);
const clone = value => structuredClone(value);
const ok = (data, status = 200) => ({status, json: {ok: true, data: clone(data)}});
const fail = (error, status = 400) => ({status, json: {ok: false, error}});
const pick = (value, keys) => Object.fromEntries(keys.filter(key => value?.[key] !== undefined).map(key => [key, clone(value[key])]));
const codes = values => Array.isArray(values) ? scopes.filter(scope => values.includes(scope)) : [];
let serial = 1;
const nextId = prefix => prefix + String(serial++).padStart(4, '0');

function account(index, scope, category, level = 'L4') {
  const id = 'recAudit' + String(index).padStart(3, '0');
  const name = '演示' + (categoryOptions.find(row => row.value === category)?.label || '人员') + String(index).padStart(2, '0');
  return {id, record_id: id, open_id: 'ou_audit' + String(index).padStart(3, '0'), name,
    employee_no: 'AUDIT-' + String(index).padStart(3, '0'), kind: category === 'duty' ? 'duty' : index % 3 ? 'vnet' : 'external',
    position: categoryOptions.find(row => row.value === category)?.label || '其他', category, home_scope: scope,
    own_scopes: [scope], scopes: ['L0', 'L1'].includes(level) ? [...scopes] : [scope], level,
    role: ['L0', 'L1'].includes(level) ? 'admin' : 'building', enabled: true, authorized: true, locked: level === 'L0',
    modules: modules.filter(module => level !== 'L4' || module !== 'accounts'), blocked_reason: '',
    can_edit: level !== 'L0', can_edit_access: level !== 'L0' && category !== 'duty',
    can_set_password: index % 3 === 0 && level !== 'L0' && category !== 'duty', can_delete: level !== 'L0' && category !== 'duty',
    can_impersonate: level !== 'L0', password_set: index % 4 !== 0, revision: 'audit-revision-1',
    last_login: now - index * 3600, account_nature: index % 3 ? 'VNET' : '外部账号'};
}
const accounts = [account(0, 'H', 'manager', 'L0'), account(1, 'H', 'manager', 'L1'), account(2, 'H', 'engineer', 'L2')];
for (const scope of scopes) {
  accounts.push(account(accounts.length, scope, 'supervisor', 'L3'));
  for (const category of ['engineer', 'operator', 'shift_leader', 'duty']) {
    accounts.push(account(accounts.length, scope, category, scope === 'H' && category === 'duty' ? 'L1' : 'L4'));
  }
}
accounts[8].enabled = false;
accounts[8].can_impersonate = false;
const administrator = accounts[0];
Object.assign(administrator, {open_id: 'ou_responsive_fixture', name: '布局验收管理员',
  home_scope: 'A', own_scopes: ['A']});
let impersonation = null;
function publicUser() {
  const person = impersonation ? accounts.find(row => row.id === impersonation.record_id) : administrator;
  return {open_id: person.open_id, name: person.name, employee_no: person.employee_no, role: person.role,
    level: person.level, category: person.category, category_label: person.position,
    home_scope: person.home_scope, scopes: person.scopes, scope_rules: person.scopes,
    modules: person.modules, login_method: impersonation ? 'impersonation' : 'password',
    can_delete_business: ['L0', 'L1', 'L3'].includes(person.level),
    can_manage_accounts: !impersonation, can_view_accounts: !impersonation,
    ...(impersonation ? {impersonation: {id: impersonation.id, active: true, operator_name: administrator.name,
      target_name: person.name, expires_at: now + 1200}} : {})};
}
const audits = [{audit_id: 'audit-account-seed', action: 'access_update', status: 'success',
  actor_open_id: administrator.open_id, actor_name: administrator.name, target_record_id: accounts[3].id,
  target_name: accounts[3].name, created_at: now - 3600, updated_at: now - 3600, metadata: {fixture: true}}];
function addAudit(action, target) {
  audits.unshift({audit_id: nextId('audit'), action, status: 'success', actor_open_id: administrator.open_id,
    actor_name: administrator.name, target_record_id: target.id, target_name: target.name,
    created_at: now, updated_at: now, metadata: {fixture: true}});
}
const applications = accounts.slice(4, 7).map((person, index) => ({request_id: 'audit-request-' + index,
  open_id: person.open_id, name: person.name, status: index === 2 ? 'approved' : 'pending',
  created_at: dateText + ' 09:00', reason: '演示：申请处理本楼业务', requested_scopes: [person.home_scope],
  requested_scope_labels: [person.home_scope + '楼'],
  requested_access: {level: 'L4', category: person.category, scopes: [person.home_scope], modules: ['learning', 'signatures']}}));

// Generated line-stroke sample from check-signature-management-preview; no real handwriting.
const signaturePng = 'iVBORw0KGgoAAAANSUhEUgAAAMgAAABQCAYAAABcbTqwAAAAxUlEQVR4nO3bMQrAIAAEwfz/0wYsU2xKo5mB6wXd0usCAAAAAAAAAAAAAAB4NVYfAL5MIBAEAkEgEARyuGG2cBzOJUMQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgTCt/rtsew0mjwGCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCOSHVv9nNnuOTbk8CAKBIBAIAgEAAAAAAAAAAAAAAMoNzMn+EJC9rUEAAAAASUVORK5CYII=';
const people = accounts.filter(person => person.category !== 'duty').map((person, index) => ({
  source: 'staff', record_id: person.record_id, person_key: 'staff:' + person.record_id, open_id: person.open_id,
  name: person.name, employee_no: person.employee_no, building: person.home_scope === '110' ? '110站' : person.home_scope + '楼',
  scope_text: person.home_scope === '110' ? '110站' : person.home_scope + '楼', position: person.position,
  account_nature: person.account_nature, has_signature: index % 3 === 0, signature_count: index % 3 === 1 ? 0 : 1,
  signature_status: ['signed', 'unsigned', 'resign'][index % 3], signature_reason: index % 3 === 2 ? '演示：待重新签名' : '',
  effective_has_signature: index % 3 === 0, effective_source: 'staff', can_preview: index % 3 === 0,
  can_request: true, can_receive_message: true, is_self: person.id === administrator.id, invite_only: false,
  identity_version: 'audit-identity-' + person.id, request: null,
}));
for (const suffix of ['A', 'B']) people.push({source: 'external', record_id: 'recAuditExternal' + suffix,
  person_key: 'external:recAuditExternal' + suffix, name: '演示外协人员', employee_no: 'AUDIT-EXT-' + suffix,
  building: 'C楼', scope_text: 'C楼', position: '临时人员', has_signature: true, signature_count: 1,
  signature_status: 'signed', effective_has_signature: true, effective_source: 'external', can_preview: true,
  can_request: true, can_receive_message: false, is_self: false, identity_version: 'audit-ext-' + suffix, request: null});
const signatureRequests = new Map();
const signatureSources = {staff: {ok: true, error: '', loaded_at: now}, external: {ok: true, error: '', loaded_at: now}};
function signatureRequest(person, payload, local = false) {
  const id = String(payload.operation_id || nextId('audit-signature-'));
  if (signatureRequests.has(id)) return signatureRequests.get(id);
  const request = {request_id: id, person_key: person.person_key, name: person.name, building: person.building,
    status: local ? 'ready' : 'sent', recipient_label: local ? '本人在线签名' : '演示接收人',
    created_at: now, expires_at: now + 86400, destination: person.source === 'staff' ? '正式人员表' : '临时人员表'};
  signatureRequests.set(id, request);
  person.request = request;
  return request;
}

const models = [
  {id: 'audit-shared', name: '演示默认模型', shared: true},
  {id: 'audit-personal', name: '演示备选模型', shared: false},
].map(model => ({...model, model: 'synthetic-preview-only', endpoint: 'https://model.example/v1/chat/completions',
  configured: true, tool_calls: true, image_input: false, reasoning: true, reasoning_only: false,
  reasoning_optional: true, custom_protocol: false, reasoning_efforts: ['low', 'medium', 'high', 'xhigh'], reasoning_effort: 'xhigh'}));
let selectedModel = models[0].id;
let assistantEnabled = true;
let appearance = {shape: 'weather-lighthouse-mini', color: 'encre', size: 56, snap_back: true};
const fileId = 'a'.repeat(32), fileText = '隔离验收演示附件。此文件不含真实业务资料。\n';
const file = {id: fileId, name: '演示说明.txt', kind: 'text', mime: 'text/plain', size: Buffer.byteLength(fileText),
  status: 'ready', url: '/api/assistant/files/' + fileId};
const turns = [{operation_id: 'audit-turn-1', run_id: 'audit-run-1', question: '演示：查看本楼待办',
  answer: '以下仅为隔离验收示例。\n\n| 类型 | 未完成 |\n| --- | ---: |\n| 维护 | 3 |\n| 维修 | 2 |\n\n可进入对应模块查看明细。[1]',
  status: 'completed', at: now - 600, model_id: models[0].id, model_name: models[0].name, attachments: [],
  sources: [{number: 1, title: '演示维护列表', url: '/workbench-lite?scope=C&work_type=maintenance', scopes: ['C']}],
  process: [{label: '读取演示副本', status: 'completed'}], interactions: [], output_files: []},
  {operation_id: 'audit-turn-2', run_id: 'audit-run-2', question: '演示：生成一份说明文件',
    answer: '演示文件已放在当前会话中，可预览或下载。没有向飞书发送消息。', status: 'completed', at: now - 300,
    model_id: models[0].id, model_name: models[0].name, attachments: [], output_files: [file], sources: [], interactions: []}];
const runs = new Map(turns.map(turn => [turn.run_id, turn]));
function conversation() {
  return {conversation_id: 'responsive-audit:' + administrator.open_id, owner_open_id: administrator.open_id,
    enabled: assistantEnabled, configured: true, busy: false,
    active_run_id: '', model_id: selectedModel, model_name: models.find(model => model.id === selectedModel)?.name || '',
    model_options: models, can_manage_settings: true, can_manage_shared: true,
    context: {turns: turns.length, messages: turns.length * 2, summary: '演示会话，无真实上下文。'},
    turns, has_more: false, history_has_more: false, phase: ''};
}
const skills = [{id: 'audit-company', name: '公司知识库', title: '公司知识库', description: '演示：按明确指令查询公司资料',
  enabled: true, command: '/公司知识库', managed: true},
  {id: 'audit-files', name: '文件分析', title: '文件分析', description: '演示：读取附件并整理要点', enabled: true, managed: true}];
function stream(turn) {
  const chunks = [{type: 'start', messageId: turn.run_id}, {type: 'text-start', id: 'audit-answer'},
    {type: 'text-delta', id: 'audit-answer', delta: turn.answer}, {type: 'text-end', id: 'audit-answer'},
    {type: 'data-turn', data: turn}, {type: 'finish'}];
  return {status: 200, contentType: 'text/event-stream',
    body: chunks.map(chunk => 'data: ' + JSON.stringify(chunk) + '\n\n').join('') + 'data: [DONE]\n\n'};
}

const historyTitles = ['演示空调过滤网维护', '演示配电柜维护', '演示消防系统变更', '演示水泵检修', '演示BA资料维护'];
const historySources = Array.from({length: 30}, (_, index) => {
  const scope = 'ABCDE'[index % 5], workType = ['maintenance', 'change', 'repair'][index % 3];
  return {id: 'audit-source-' + index, title: '演示机房' + scope + '楼' + historyTitles[index % 5],
    building: scope + '楼', building_codes: [scope], work_type: workType, source_record_id: 'recAuditPlan' + index,
    memory_name: 'audit-memory-' + index, source_status: '未开始', specialty: index % 2 ? '电气' : '暖通',
    maintenance_cycle: '每月', current_fields: {content: '当前演示事项内容', location: scope + '楼演示机房'},
    memory: index === 0 ? {content: '已保存的演示记忆', location: scope + '楼演示机房', impact: '演示：不影响运行'} : {}};
});
const historyCandidates = historySources.filter((_, index) => index % 4 !== 3).map(source => ({
  id: 'audit-candidate-' + source.id, title: source.title, building: source.building, building_codes: source.building_codes,
  work_type: source.work_type, business_time: dateText + ' 09:00', fields: {content: '历史演示维护内容',
    location: source.building + '演示机房', reason: '演示计划维护', impact: '演示：不影响运行', specialty: source.specialty}}));

/** Only the isolated audit server should call this; unknown APIs deliberately return null. */
export async function accountFixture({url, method = 'GET', body} = {}) {
  let parsed;
  try { parsed = new URL(url instanceof URL ? url.href : String(url || ''), 'http://audit.invalid'); }
  catch { return null; }
  const path = parsed.pathname, query = parsed.searchParams;
  method = String(method).toUpperCase();
  const owned = path === '/api/health' || path.startsWith('/api/auth/') || path.startsWith('/api/signatures/management/')
    || path.startsWith('/api/assistant/') || path.startsWith('/api/admin/notice-memory/');
  if (!owned) return null;
  let payload = body || {};
  if (typeof payload === 'string' || Buffer.isBuffer(payload)) {
    try { payload = payload.length ? JSON.parse(String(payload)) : {}; }
    catch { return fail('演示接口：JSON 格式无效'); }
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) return fail('演示接口：需要对象参数');

  if (path === '/api/health' && method === 'GET') return {json: {ok: true, service: 'clipflow_backend', instance_id: 'isolated-responsive-audit'}};
  if (path === '/api/auth/status' && method === 'GET') return ok({logged_in: true, user: publicUser(),
    scope_options: scopeOptions, allowed_scopes: impersonation ? publicUser().scope_rules : scopes,
    default_scope: publicUser().home_scope, login_url: '/api/auth/login', configured: true});
  if (path === '/api/auth/accounts' && method === 'GET') return ok({items: accounts, scope_options: scopeOptions,
    module_options: moduleOptions, category_options: categoryOptions, level_options: levelOptions, loaded_at: now,
    policy: {...publicUser(), scope_rules: scopes, modules}});
  if (path === '/api/auth/accounts/audit' && method === 'GET') return ok({items: audits});
  if (path === '/api/auth/permissions' && method === 'GET') return ok({users: accounts, scope_options: scopeOptions});
  if (path === '/api/auth/permission-directory' && method === 'GET') return ok({items: accounts, loaded_at: now});
  if (path === '/api/auth/accounts/permissions' && method === 'POST') {
    for (const change of payload.changes || []) {
      const target = accounts.find(row => row.id === change.id);
      if (!target || !target.can_edit) return fail('演示账号不可修改', 403);
      Object.assign(target, pick(change, ['enabled', 'level', 'category', 'role']));
      if (Array.isArray(change.scopes)) target.scopes = codes(change.scopes);
      if (Array.isArray(change.modules)) target.modules = modules.filter(module => change.modules.includes(module));
      target.revision = nextId('audit-revision-');
      addAudit('access_update', target);
    }
    return ok({changed: (payload.changes || []).length});
  }
  let match = path.match(/^\/api\/auth\/accounts\/([^/]+)\/(password|impersonate)$/);
  if (match && method === 'POST') {
    const target = accounts.find(row => row.id === decodeURIComponent(match[1]));
    if (!target) return fail('演示账号不存在', 404);
    if (match[2] === 'password') {
      if (!target.can_set_password) return fail('此演示账号不使用密码登录', 403);
      target.password_set = true;
      addAudit('password_reset', target); // Never retain or echo submitted passwords.
      return ok({password_changed: true});
    }
    if (!target.can_impersonate || !target.enabled) return fail('此演示账号不可代登录', 403);
    impersonation = {id: nextId('audit-impersonation-'), record_id: target.id};
    return ok({redirect_url: '/'});
  }
  if (path === '/api/auth/impersonation/stop' && method === 'POST') { impersonation = null; return ok({redirect_url: '/?admin=accounts'}); }
  match = path.match(/^\/api\/auth\/accounts\/([^/]+)$/);
  if (match && method === 'DELETE') {
    const index = accounts.findIndex(row => row.id === decodeURIComponent(match[1]));
    if (index < 0 || !accounts[index].can_delete) return fail('演示账号不可删除', 403);
    addAudit('account_delete', accounts[index]);
    accounts.splice(index, 1);
    return ok({deleted: true});
  }
  if (path === '/api/auth/permission-requests/admin' && method === 'GET') return ok({items: applications.filter(row => !query.get('status') || query.get('status') === 'all' || query.get('status') === row.status)});
  if (path === '/api/auth/permission-requests/current' && method === 'GET') return ok({request: null});
  if (path === '/api/auth/permission-approvers' && method === 'GET') return ok({items: [], current: publicUser(), scope_options: scopeOptions, module_options: moduleOptions});
  match = path.match(/^\/api\/auth\/permission-requests\/([^/]+)\/(approve|reject)$/);
  if (match && method === 'POST') {
    const request = applications.find(row => row.request_id === match[1]);
    if (!request) return fail('演示申请不存在', 404);
    request.status = match[2] === 'approve' ? 'approved' : 'rejected';
    return ok({request, notification: {ok: true, queued: false, fixture: true}});
  }
  if (path === '/api/auth/permission-requests/bulk-approve' && method === 'POST') {
    const items = applications.filter(row => (payload.request_ids || []).includes(row.request_id));
    for (const request of items) request.status = 'approved';
    return ok({items, failed: []});
  }
  if (path === '/api/auth/certificate' && method === 'GET') return ok({state: 'healthy', domain: 'audit.invalid',
    issuer: 'Synthetic audit certificate', not_before: now - 86400, not_after: now + 87 * 86400, checked_at: now,
    remaining_days: 87, renewal: {enabled: true, last_check_ok: true, serving_current: true, next_renewal_at: now + 60 * 86400}});

  const signature = '/api/signatures/management/';
  if (path === signature + 'account' && method === 'GET') {
    const person = people.find(row => row.open_id === (query.get('open_id') || publicUser().open_id));
    return ok({person: person || null, reason: person ? '' : '演示共享账号没有个人签名。'});
  }
  if ((path === signature + 'people' && method === 'GET') || (path === signature + 'refresh' && method === 'POST')) {
    const terms = (query.get('q') || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
    const rows = people.filter(person => (!query.get('scope') || query.get('scope') === 'ALL' || person.building === query.get('scope') + '楼' || query.get('scope') === '110' && person.building === '110站')
      && (!query.get('source') || query.get('source') === 'all' || person.source === query.get('source'))
      && (!query.get('status') || query.get('status') === 'all' || person.signature_status === query.get('status'))
      && terms.every(term => [person.name, person.employee_no, person.building, person.position].join(' ').toLowerCase().includes(term)));
    const page = Math.max(1, Number(query.get('page')) || 1), pageSize = Math.max(1, Math.min(500, Number(query.get('page_size')) || 50));
    return ok({people: rows.slice((page - 1) * pageSize, page * pageSize), count: rows.length, page, page_size: pageSize,
      can_manage_directory: true, scope_options: scopeOptions, recipients: scopeOptions.map(row => ({value: row.value, label: row.label + '值班账号'})),
      sources: signatureSources, counts: Object.fromEntries(['signed', 'unsigned', 'resign'].map(status => [status, people.filter(row => row.signature_status === status).length]))});
  }
  if (path === signature + 'preview' && method === 'GET') {
    const person = people.find(row => row.source === query.get('source') && row.record_id === query.get('record_id'));
    return person?.has_signature ? {contentType: 'image/png', body: Buffer.from(signaturePng, 'base64')} : fail('演示人员尚未签名', 404);
  }
  if (path === signature + 'duplicates' && method === 'GET') return ok({groups: [{name: '演示外协人员', version: 'audit-duplicate-1', people: people.filter(row => row.source === 'external')}]});
  if ((path === signature + 'requests' || path === signature + 'self-start') && method === 'POST') {
    const local = path.endsWith('/self-start');
    const person = people.find(row => local ? row.open_id === publicUser().open_id : row.source === payload.source && row.record_id === payload.record_id);
    if (!person) return fail('演示签名人员不存在', 404);
    const request = signatureRequest(person, payload, local);
    return ok({...request, ...(local ? {link_url: '/signature?request_id=' + request.request_id + '#token=audit-only-token'} : {})});
  }
  if (path === signature + 'request' && method === 'GET') {
    const request = signatureRequests.get(query.get('request_id'));
    return request ? ok(request) : fail('演示签名请求不存在', 404);
  }
  if (path === signature + 'submit' && method === 'POST') {
    const request = signatureRequests.get(payload.request_id);
    if (!request || payload.confirmed_self !== true) return fail('演示签名需要本人确认');
    request.status = 'completed'; request.saved_at = now;
    const person = people.find(row => row.person_key === request.person_key);
    Object.assign(person, {has_signature: true, effective_has_signature: true, signature_status: 'signed', can_preview: true, signature_count: 1});
    return ok(request);
  }

  if (path === '/api/assistant/appearance' && ['GET', 'PUT'].includes(method)) {
    if (method === 'PUT') appearance = {...appearance, ...pick(payload, ['shape', 'color', 'size', 'snap_back'])};
    return ok(appearance);
  }
  if (path === '/api/assistant/icon-weather' && method === 'GET') return ok({available: true, city: '演示城市',
    weather: '多云', condition: 'cloudy', temperature: 22, temp_c: 22, is_day: true, timezone: 'Asia/Shanghai', observed_at: now, source: 'synthetic-fixture'});
  if (path === '/api/assistant/conversation' && ['GET', 'PATCH', 'DELETE'].includes(method)) {
    if (method === 'DELETE') turns.splice(0);
    if (method === 'PATCH' && models.some(model => model.id === payload.model_id)) selectedModel = payload.model_id;
    return ok(conversation());
  }
  if (path === '/api/assistant/history' && method === 'GET') return ok({turns, has_more: false, history_has_more: false});
  if (path === '/api/assistant/settings' && ['GET', 'PUT'].includes(method)) {
    if (method === 'PUT') {
      if (payload.action === 'toggle') assistantEnabled = Boolean(payload.enabled);
      if (payload.action === 'select' && models.some(model => model.id === payload.id)) selectedModel = payload.id;
      if (payload.action === 'delete') { const index = models.findIndex(model => model.id === payload.id); if (index >= 0) models.splice(index, 1); }
      if (payload.action === 'upsert') {
        const profile = pick(payload.profile, ['id', 'name', 'model', 'tool_calls', 'image_input', 'reasoning', 'reasoning_only', 'reasoning_optional', 'custom_protocol', 'reasoning_efforts', 'reasoning_effort']);
        profile.id ||= nextId('audit-model-');
        const index = models.findIndex(model => model.id === profile.id);
        const saved = {...profile, endpoint: 'https://model.example/v1/chat/completions', shared: payload.scope === 'shared', configured: true};
        if (index < 0) models.push(saved); else models[index] = saved;
      }
      if (!models.some(model => model.id === selectedModel)) selectedModel = models[0]?.id || '';
    }
    return ok({models, active_model_id: selectedModel, configured: Boolean(models.length), enabled: assistantEnabled, can_manage_shared: true});
  }
  if (path === '/api/assistant/feishu-settings' && ['GET', 'POST'].includes(method)) return ok({app_id: 'cli_auditfixtureonly',
    has_secret: true, enabled: true, revision: 'audit-feishu-1', restart_required: false});
  if (path === '/api/assistant/skills' && method === 'GET') return ok({items: skills, can_manage: true});
  if (path === '/api/assistant/extensions' && method === 'GET') return ok({revision: 1, can_manage: true, servers: [{
    id: 'audit-utilities', name: '演示内置工具', transport: 'builtin', enabled: true, removable: false, admin_only: false,
    allowed_tools: ['text_lines'], tools: [{name: 'text_lines', description: '演示文本整理'}]}],
    native_tools: [{name: 'public_search', label: '联网搜索', description: '仅演示，不进行实际搜索'}], recent_calls: []});
  if ((path === '/api/assistant/messages' || path === '/api/assistant/chat') && method === 'POST') {
    const operation = String(payload.operation_id || nextId('audit-turn-'));
    let turn = turns.find(row => row.operation_id === operation);
    if (!turn) {
      turn = {operation_id: operation, run_id: nextId('audit-run-'), question: String(payload.question || '演示附件分析'),
        answer: '这是隔离页面验收的演示回答。\n\n- 可以检查文字、表格与操作区布局。\n- 未查询真实业务资料，未调用模型或飞书。',
        status: 'completed', at: now, model_id: selectedModel, model_name: models.find(model => model.id === selectedModel)?.name,
        sources: [], process: [], attachments: [], output_files: [], interactions: []};
      turns.push(turn); runs.set(turn.run_id, turn);
    }
    return ok(path.endsWith('/chat') ? conversation() : {run_id: turn.run_id, turn, status: 'completed'}, path.endsWith('/chat') ? 200 : 202);
  }
  match = path.match(/^\/api\/assistant\/runs\/([^/]+)(?:\/(stream|cancel))?$/);
  if (match) {
    const turn = runs.get(match[1]);
    if (!turn) return fail('演示回答不存在', 404);
    if (method === 'GET' && match[2] === 'stream') return stream(turn);
    if (method === 'GET' && !match[2]) return ok({run_id: turn.run_id, status: turn.status, turn});
    if (method === 'POST' && match[2] === 'cancel') return ok({run_id: turn.run_id, status: turn.status});
  }
  if (path === '/api/assistant/files/' + fileId && method === 'GET') return query.has('preview')
    ? ok({kind: 'text', name: file.name, text: fileText, content: fileText}) : {contentType: 'text/plain; charset=utf-8', body: fileText};

  if (path === '/api/admin/notice-memory/history-scan' && method === 'POST') return ok({source_items: historySources,
    candidates: historyCandidates, matches: historyCandidates.map(candidate => ({source_id: candidate.id.replace('audit-candidate-', ''), candidate_id: candidate.id, selected: true})),
    warnings: [], counts: {source: historySources.length, candidates: historyCandidates.length, recommended: historyCandidates.length}});
  if (path === '/api/admin/notice-memory/history-save' && method === 'POST') {
    const selected = (payload.matches || []).filter(match => historySources.some(source => source.id === match.source_item?.id));
    for (const match of selected) historySources.find(source => source.id === match.source_item.id).memory = pick(match.fields,
      ['content', 'location', 'reason', 'impact', 'progress', 'specialty', 'execution_party']);
    return ok({saved_count: selected.length, overwritten_count: 0, skipped_count: (payload.matches || []).length - selected.length});
  }
  return null;
}
