// Isolated responsive-audit data only. No server, browser, filesystem or network side effects.
// The host owns authentication, layouts and static assets; unmatched requests return null.
const clone = value => structuredClone(value);
const ok = data => ({ status: 200, json: { ok: true, data: clone(data) } });
const fail = (error, status = 400) => ({ status, json: { ok: false, error } });
const missing = () => fail('隔离样例记录不存在。', 404);
const unsupported = () => fail('隔离预览未模拟此操作，不会解析文件、调用模型或发送真实消息。', 501);
const now = () => new Date().toISOString();
const day = () => new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
const scopes = ['A', 'B', 'C', 'D', 'E', 'H'];
const options = scopes.map(value => ({ value, label: value + '楼', authorized: true }));
let sequence = 100;
const id = prefix => prefix + '-' + (++sequence);
const hex = () => (++sequence).toString(16).padStart(32, '0');
const title = '机房冷却水系统及设备联动异常排查、交接与恢复确认（长标题换行验收）';
const people = ['布局验收管理员', '隔离工程师乙', '隔离主管丙'].map((name, i) => ({
  id: i === 0 ? 'ou_responsive_fixture' : 'collab-person-' + (i + 1), record_id: 'collab-person-' + (i + 1),
  open_id: i === 0 ? 'ou_responsive_fixture' : 'ou_collab_fixture_' + (i + 1), name, employee_no: 'FIX00' + (i + 1),
  scopes: i === 0 ? ['A'] : ['A', scopes[i]], home_scope: scopes[i], building: scopes[i], active: true,
  position: i === 2 ? '楼栋主管' : '暖通工程师', has_signature: false, selectable: true,
}));
function payload(body) {
  if (typeof body === 'string') { try { return JSON.parse(body); } catch { return {}; } }
  if (body && typeof body === 'object' && !ArrayBuffer.isView(body) && !(body instanceof ArrayBuffer)) return body;
  if (ArrayBuffer.isView(body)) { try { return JSON.parse(new TextDecoder().decode(body)); } catch { return {}; } }
  return {};
}
function page(items, url, extra = {}) {
  const size = Math.max(1, Math.min(100, Number(url.searchParams.get('page_size')) || 20));
  const current = Math.max(1, Number(url.searchParams.get('page')) || 1);
  return { items: items.slice((current - 1) * size, current * size), total: items.length, page: current, page_size: size, ...extra };
}
function search(items, url) {
  const q = (url.searchParams.get('q') || url.searchParams.get('search') || '').trim();
  return q ? items.filter(item => JSON.stringify(item).includes(q)) : items;
}
function scoped(items, url) {
  const scope = url.searchParams.get('scope');
  return !scope || ['ALL', 'CAMPUS'].includes(scope) ? items : items.filter(item => item.scope === scope || item.scopes?.includes(scope));
}

const questions = Array.from({ length: 15 }, (_, i) => ({
  id: 'collab-question-' + (i + 1), version: 1, bank: 'written', type: i === 2 ? 'multiple' : 'single',
  stem: `${i + 1}. ${i === 0 ? title : '设备异常时应先核对哪些现场信息，再进行下一步操作？'}`,
  options: [{ id: 'a', text: '先确认人员安全、设备状态并按既有流程报告。' }, { id: 'b', text: '未经确认立即复位全部设备。' }, { id: 'c', text: '保留现场信息与处理记录。' }],
  correct_option_ids: i === 2 ? ['a', 'c'] : ['a'], answer_text: '', analysis: '隔离样例：先核对再处置，保留操作记录。',
  hint: '优先考虑安全与可追溯性。', has_hint: true, status: 'active', attachments: [],
}));
let settings = { enabled: true, publish_time: '13:00', night_publish_time: '20:00', question_count: 15, repeat_days: 7 };
const summary = { assigned: 45, answered: 30, task_answered: 30, choice_answered: 30, correct: 24, wrong: 6, accuracy: 80,
  independent_answered: 30, independent_correct: 24, learning_days: 3, interview_total: 0, interview_ratings: {}, papers: 3, answered_people: 3, received_people: 3 };
function makePaper(person, mode = 'daily', paperId = id('collab-paper')) {
  return { id: paperId, person_id: person.id, person: clone(person), scope: person.home_scope, date: day(),
    version: 1, mode, status: 'pending', created_at: now(), shortage: {}, questions: clone(questions),
    stats: { total: questions.length, answered: 0, shortage: 0 } };
}
const papers = people.map((person, i) => makePaper(person, 'daily', 'collab-paper-' + (i + 1)));
const claims = new Map();
const issues = people.map((person, i) => ({ id: 'collab-issue-' + i, version: 1, status: 'open', title: '现场学习问题 ' + title,
  content: '请核对设备运行参数与处理步骤。', person_id: person.id, created_at: now(), attachments: [] }));
const sharing = people.map((person, i) => ({
  id: 'collab-sharing-' + (i + 1), title: '分享会：' + title, url: '/knowledge-base?document=collab-doc-1',
  status: i === 0 ? 'draft' : 'published', version: 1, created_at: now(), question_count: 8, completed: 1, total: 3,
  participants: people.map(p => ({ name: p.name, open_id: p.open_id, scope: p.home_scope, exempted: false, delivery_status: 'sent', submitted_at: '', correct: null })),
  questions: questions.slice(0, 8).map(q => ({ id: q.id, type: q.type, stem: q.stem, options: q.options.map(o => o.text),
    correct: q.correct_option_ids.map(x => q.options.findIndex(o => o.id === x)), explanation: q.analysis, evidence: '隔离样例纪要第2段' })), results: [],
}));
function learning(url, method, data, parts) {
  const [area, key, action] = parts;
  if (area === 'bootstrap' && method === 'GET') return ok({ is_admin: true, can_answer: true, can_view_buildings: true,
    self_person: people[0], self_scope: 'A', scope: url.searchParams.get('scope') || 'A', scopes: options,
    identity_issue: '', settings, sync: { status: 'ready', pending: 0 }, today: day(), question_problem_count: 0 });
  if (area === 'people' && method === 'GET') return ok(page(search(scoped(people, url), url), url, { ready: true, issues: [] }));
  if (area === 'settings') {
    if (method === 'PUT') settings = { ...settings, ...data };
    return ['GET', 'PUT'].includes(method) ? ok(settings) : null;
  }
  if (area === 'profile' && method === 'GET') return ok({ person: people.find(p => p.id === url.searchParams.get('person_id')) || people[0], published: true,
    summary, today_summary: summary, topics: [], banks: [{ label: '笔试', wrong: 6 }], distribution: [{ label: '80-99%', count: 3 }],
    trend: Array.from({ length: 7 }, (_, i) => ({ date: new Date(Date.now() - (6 - i) * 86400000).toISOString().slice(0, 10), answered: i + 4, people: 3, accuracy: 70 + i * 3 })),
    people: people.map(p => ({ person_id: p.id, name: p.name, scope: p.home_scope, employee_no: p.employee_no, summary,
      last_answered_at: now(), today: { answered: 10, total: 15 } })) });
  if (area === 'review' && method === 'GET') return ok(page(questions.slice(0, 3).map(question => ({
    paper_id: papers[0]?.id, date: day(), scope: 'A', question, mistake_count: 2, last_wrong_at: now(),
  })), url));
  if (area === 'history' && method === 'GET') return ok(page(scoped(papers, url).filter(p => !url.searchParams.get('person_id') || p.person_id === url.searchParams.get('person_id')), url));
  if (area === 'papers') {
    if (key === 'claim' && method === 'POST') {
      const claimKey = `${data.person_id || people[0].id}:${data.mode || 'daily'}:${data.operation_id || day()}`;
      let paper = claims.get(claimKey) || papers.find(p => p.person_id === (data.person_id || people[0].id) && p.mode === (data.mode || 'daily') && data.mode !== 'practice');
      if (!paper) { paper = makePaper(people.find(p => p.id === data.person_id) || people[0], data.mode || 'daily'); papers.push(paper); }
      claims.set(claimKey, paper); return ok(paper);
    }
    if (!key && method === 'GET') return ok(page(scoped(papers, url).filter(p =>
      (!url.searchParams.get('person_id') || p.person_id === url.searchParams.get('person_id')) &&
      (!url.searchParams.get('mode') || p.mode === url.searchParams.get('mode'))), url, { today: day(), published: true }));
    const paper = papers.find(p => p.id === key); if (!paper) return missing();
    if (!action && method === 'GET') return ok(paper);
    if (!action && method === 'DELETE') { papers.splice(papers.indexOf(paper), 1); return ok({ id: key, deleted: true }); }
    if (method !== 'POST') return null;
    if (action === 'start') { paper.started_at ||= now(); return ok(paper); }
    const question = paper.questions.find(q => q.id === data.question_id); if (!question) return missing();
    if (action === 'answer') {
      const correct = [...(data.option_ids || [])].sort().join() === [...question.correct_option_ids].sort().join();
      const attempt = { correct, option_ids: data.option_ids || [], answer_text: data.answer_text || '', submitted_at: now(), self_rating: data.self_rating, assisted: false };
      if (data.practice) (question.practice ||= []).push(attempt); else question.attempt = attempt;
      paper.stats.answered = paper.questions.filter(q => q.attempt).length;
      paper.status = paper.stats.answered === paper.questions.length ? 'completed' : 'started';
      paper.version++; return ok(paper);
    }
    if (action === 'notes') { Object.assign(question, { note: data.note || '', favorite: !!data.favorite }); return ok(paper); }
    if (action === 'reveal') { question.revealed = true; return ok(paper); }
    return null;
  }
  if (area === 'questions' || area === 'issues') {
    const list = area === 'questions' ? questions : issues;
    if (!key && method === 'GET') return ok(page(search(list, url), url));
    if (!key && method === 'POST') { const item = { ...clone(data), id: data.new_id || id('collab-' + area), version: 1, attachments: data.attachments || [] }; list.push(item); return ok(item); }
    const item = list.find(row => row.id === key); if (!item) return missing();
    if (!action && method === 'GET') return ok(item);
    if (!action && ['PUT', 'PATCH'].includes(method)) { Object.assign(item, clone(data), { id: key, version: Number(item.version) + 1 }); return ok(item); }
    if (action === 'status' && method === 'POST') { item.status = data.status; item.version++; return ok(item); }
    if (action === 'copy' && method === 'POST') { const copied = { ...clone(item), id: id('collab-question'), status: 'draft', version: 1 }; list.push(copied); return ok(copied); }
    return null;
  }
  if (area === 'sharing') {
    if (!key && method === 'GET') return ok(page(sharing, url));
    if (!key && method === 'POST') { const batch = { ...clone(sharing[0]), id: id('collab-sharing'), status: 'draft', version: 1, url: data.url || '', title: '隔离样例分享会：' + title }; sharing.push(batch); return ok(batch); }
    const batch = sharing.find(row => row.id === key); if (!batch) return missing();
    if (!action && method === 'GET') return ok(batch);
    if (!action && method === 'PUT') {
      if (data.questions) batch.questions = clone(data.questions);
      for (const person of batch.participants) person.exempted = (data.excluded_open_ids || []).includes(person.open_id);
      batch.question_count = batch.questions.length; batch.version++; return ok(batch);
    }
    if (method === 'POST' && ['publish', 'close', 'retry', 'retry-delivery'].includes(action)) {
      if (action === 'publish') batch.status = 'published'; if (action === 'close') batch.status = 'closed'; batch.version++; return ok(batch);
    }
    return null;
  }
  if (['attachments', 'import', 'publish', 'sync'].includes(area)) return unsupported();
  return null;
}

const drillSteps = Array.from({ length: 3 }, (_, i) => ({ row: 10 + i, location: '设备间', content: i === 0 ? title : '确认现场状态并记录处理结果。', duration_minutes: i === 2 ? 0 : 5, signature_slots: i === 2 ? 0 : 1 }));
const drills = people.map((person, i) => ({ drill_id: 'collab-drill-' + (i + 1), name: '应急演练：' + title,
  year: new Date().getFullYear(), month: new Date().getMonth() + 1, status: 'published', assigned_scopes: scopes, version: 1,
  source: { name: '隔离演练记录表.xlsx', size: 25600 }, sheets: ['演练记录表', '演练评估表'],
  configuration: { record_sheet: '演练记录表', assessment_sheet: '演练评估表', steps: clone(drillSteps),
    mapping: { scenario: 'A4', reviewer_signature: 'A20', review_time: 'B20', assessment: { evaluator_signature: 'A10', evaluation_time: 'B10' } } } }));
const executions = new Map();
const sheetModel = { column_count: 4, sheet_width_px: 960, column_widths: [160, 360, 140, 300], row_heights: [36, 48, 72],
  rows: [['演练记录表', '', '', ''], ['演练场景', title, '', ''], ['执行情况', '已确认现场安全，按步骤完成检查。', '', '']],
  merges: [{ row: 0, col: 0, rowspan: 1, colspan: 4 }], cell_styles: {}, images: [], signature_cells: [] };
function drillFixture(url, method, data, parts) {
  const [key, action] = parts;
  if (key === 'bootstrap' && method === 'GET') return ok({ drills, people, scopes: options });
  if (key === 'people' && method === 'GET') return ok({ people });
  if (!key) return method === 'GET' ? ok({ items: drills }) : method === 'POST' ? unsupported() : null;
  const drill = drills.find(row => row.drill_id === key); if (!drill) return missing();
  const scope = url.searchParams.get('scope') || data.scope || 'A', executionKey = key + ':' + scope;
  if (!executions.has(executionKey)) executions.set(executionKey, { drill_id: key, scope, version: 1, status: 'draft', drill_date: day(),
    first_start_time: '09:00', simulation_scenario: title, commander: people[0], participants: people, step_signers: {}, steps: clone(drillSteps) });
  const execution = executions.get(executionKey);
  if (action === 'execution' && method === 'GET') return ok({ drill, execution });
  if (action === 'execution' && method === 'PUT') { Object.assign(execution, clone(data), { version: execution.version + 1 }); return ok({ drill, execution }); }
  if (['preview', 'print-model'].includes(action) && method === 'GET') return ok({ model: sheetModel, drill, execution });
  if (action === 'configuration' && method === 'PUT') { drill.configuration = clone(data.configuration || data); drill.version++; return ok(drill); }
  if (action === 'publish' && method === 'POST') { drill.status = 'published'; drill.assigned_scopes = data.assigned_scopes || drill.assigned_scopes; return ok(drill); }
  if (['generate', 'retry-sync'].includes(action) && method === 'POST') return unsupported();
  return null;
}

let riskFields = [{ key: 'number', field_id: 'number', field_name: '排查任务编号', type: 1, role: 'number', is_primary: true },
  { key: 'f1', field_id: 'f1', field_name: '排查结果', type: 1, role: 'input', required: true },
  { key: 'f2', field_id: 'f2', field_name: '风险等级', type: 3, role: 'input', property: { options: [{ name: '无异常' }, { name: '有风险' }] } },
  { key: 'f3', field_id: 'f3', field_name: '现场照片', type: 17, role: 'input' },
  { key: 'f4', field_id: 'f4', field_name: '排查日期', type: 5, role: 'input' }];
const riskReleases = people.map((person, i) => ({ id: (i + 1).toString().repeat(32), name: '风险排查：' + title,
  created: Math.floor(Date.now() / 1000), deadline: Date.now() + 86400000, status: 'active', fields: clone(riskFields),
  owner: people[0].name, reviewer: people[2].name, reviewer_id: people[2].id, reviewer_version: 1,
  people: people.map((p, n) => ({ id: p.id, name: p.name, scope: p.home_scope, version: 1, revoked: false,
    items: Array.from({ length: 3 }, (_, j) => ({ id: `collab-risk-item-${n}-${j}`, title: title + ' · ' + (j + 1), status: 'pending', values: {}, files: [] })) })),
  is_manager: true, can_manage_release: true, can_delete_release: true, is_reviewer: true,
  self_id: people[0].id, can_claim: true, claim_scopes: scopes, jobs: [], audit: [] }));
const riskTemplates = Array.from({ length: 3 }, (_, i) => ({ id: (i + 4).toString().repeat(32), kind: 'base', name: '基础模板：' + title + (i + 1),
  revision: 1, version: 'collab-v1', enabled: true, created: Math.floor(Date.now() / 1000), updated: Math.floor(Date.now() / 1000),
  creator: people[0].name, scopes, legacy: false, cloud: { synced: true }, sync_status: { status: 'done' },
  fields: clone(riskFields), items: Array.from({ length: 3 }, (_, j) => ({ title: title + (j + 1), values: {} })) }));
function riskFixture(url, method, data, parts) {
  const [key, action, personId, personAction] = parts;
  if (key === 'people' && method === 'GET') return ok({ items: people.map(p => ({ ...p, can_review: true })) });
  if (key === 'template') {
    if (method === 'POST') {
      const fieldId = data.field_id || id('collab-field'), existing = riskFields.find(f => f.field_id === fieldId);
      if (data.action === 'delete') riskFields = riskFields.filter(f => f.field_id !== fieldId);
      else if (existing) Object.assign(existing, data, { key: fieldId, field_id: fieldId, property: { options: (data.options || []).map(name => ({ name })) } });
      else riskFields.push({ ...data, key: fieldId, field_id: fieldId, property: { options: (data.options || []).map(name => ({ name })) } });
    }
    return ['GET', 'POST'].includes(method) ? ok({ fields: riskFields, version: 'collab-v' + sequence }) : null;
  }
  if (['base-templates', 'templates'].includes(key)) {
    if (!action && method === 'GET') return ok(page(search(riskTemplates, url), url));
    if (!action && method === 'POST') { const template = { ...clone(riskTemplates[0]), ...data, id: hex(), kind: key === 'base-templates' ? 'base' : 'history' }; riskTemplates.push(template); return ok(template); }
    const template = riskTemplates.find(row => row.id === action); if (!template) return missing();
    if (method === 'GET') return ok(template);
    if (method === 'PUT') { Object.assign(template, clone(data), { id: action, revision: template.revision + 1 }); return ok(template); }
    return null;
  }
  if (!key && method === 'GET') return ok(page(search(riskReleases.map(r => ({ ...r,
    total: r.people.reduce((n, p) => n + p.items.length, 0), submitted: r.people.flatMap(p => p.items).filter(i => i.status !== 'pending').length,
    approved: 0, people_count: r.people.length })), url), url, { is_manager: true, can_edit_template: true }));
  if (!key && method === 'POST') { const release = { ...clone(riskReleases[0]), ...data, id: data.id || hex(), jobs: [], audit: [] };
    release.people = (data.people || people).map(p => ({ ...p, id: p.id || p.record_id, scope: p.scope || p.home_scope || p.scopes?.[0] || 'A', version: 1,
      items: (data.items || riskTemplates[0].items).map(item => ({ ...clone(item), id: id('collab-risk-item'), status: 'pending', files: [] })) }));
    riskReleases.push(release); return ok(release); }
  const release = riskReleases.find(row => row.id === key); if (!release) return missing();
  if (!action && method === 'GET') return ok(release);
  if (!action && method === 'DELETE') { riskReleases.splice(riskReleases.indexOf(release), 1); return ok({ id: key, status: 'deleted' }); }
  if (action === 'reviewer' && method === 'POST') { release.reviewer_id = data.reviewer_id; release.reviewer = people.find(p => p.id === data.reviewer_id)?.name || ''; release.reviewer_version++; return ok(release); }
  if (action === 'claim' && method === 'POST') { if (!release.people.some(p => p.id === release.self_id)) release.people.push({ id: release.self_id, name: people[0].name, scope: data.scope || 'A', version: 1, items: clone(riskReleases[0].people[0].items) }); return ok(release); }
  if (action === 'people' && personAction === 'actions' && method === 'POST') {
    const person = release.people.find(p => p.id === personId); if (!person) return missing();
    const item = person.items.find(i => i.id === data.item_id);
    if (data.action === 'revoke_person') person.revoked = true;
    else if (!item) return missing();
    else if (data.action === 'save') item.values = clone(data.values || {});
    else if (['submit', 'approve', 'return', 'revoke_item'].includes(data.action)) { if (data.values) item.values = clone(data.values); item.status = ({ submit: 'submitted', approve: 'approved', return: 'returned', revoke_item: 'revoked' })[data.action]; }
    else return null;
    person.version++; return ok(release);
  }
  if (['retry', 'files'].includes(action)) return unsupported();
  return null;
}

const guardItems = Array.from({ length: 3 }, (_, i) => ({ key: 'collab-guard-item-' + i, category: '设备运行', content: i === 0 ? title : '检查设备状态及现场环境。', name: '现场检查 ' + (i + 1), title: '现场检查 ' + (i + 1) }));
const guardSheet = { name: '检查清单', title: '重保现场检查清单', kind: 'check', has_signature: false, items: guardItems };
const guardTasks = Array.from({ length: 3 }, (_, i) => ({ task_id: 'collab-guard-' + (i + 1), name: '重保任务：' + title, complete: false, status: 'published',
  scopes, sheet_types: ['检查清单'], created_at: now(), responses: scopes.map(scope => ({ response_id: `collab-response-${i}-${scope}`, task_id: 'collab-guard-' + (i + 1),
    scope, sheet_type: '检查清单', cells: { checks: {}, template_items: clone(guardItems) }, version: 1, status: 'pending', selected_signers: [] })) }));
const guardTemplates = new Map();
function guardFixture(url, method, data, parts) {
  const [area, key, action] = parts;
  const publicTask = task => {
    const responses = url.searchParams.get('scope') ? task.responses.filter(r => r.scope === url.searchParams.get('scope')) : task.responses;
    return { ...task, responses, response_count: responses.length, pending_count: responses.filter(r => r.status === 'pending').length,
      draft_count: responses.filter(r => r.status === 'draft').length, submitted_count: responses.filter(r => r.status === 'submitted').length };
  };
  if (area === 'bootstrap' && method === 'GET') return ok({ catalog: { sheets: [guardSheet] }, scope_options: options, scopes: options.map(s => ({ ...s, pending: 3, completed: 0 })) });
  if (area === 'tasks') {
    if (!key && method === 'GET') return ok({ tasks: guardTasks.map(publicTask) });
    if (!key && method === 'POST') { const task = clone(guardTasks[0]); Object.assign(task, data, { task_id: id('collab-guard'), created_at: now() });
      task.responses = (data.scopes || scopes).map(scope => ({ ...clone(guardTasks[0].responses[0]), response_id: id('collab-response'), scope, task_id: task.task_id })); guardTasks.push(task); return ok(task); }
    const task = guardTasks.find(t => t.task_id === key); if (!task) return missing();
    if (!action && method === 'GET') return ok(publicTask(task));
    if (!action && method === 'DELETE') { guardTasks.splice(guardTasks.indexOf(task), 1); return ok({ task_id: key, deleted: true }); }
    if (action === 'archive-retry') return unsupported();
    return null;
  }
  if (area === 'responses' && method === 'PUT') {
    const response = guardTasks.flatMap(t => t.responses).find(r => r.response_id === key); if (!response) return missing();
    if (data.generate_image) return unsupported();
    Object.assign(response, { cells: clone(data.cells || {}), version: response.version + 1, selected_signers: clone(data.signatures || []) }); return ok(response);
  }
  if (area === 'scope-template') {
    const templateKey = (url.searchParams.get('scope') || data.scope || 'A') + ':' + (url.searchParams.get('sheet_type') || data.sheet_type || '检查清单');
    if (!guardTemplates.has(templateKey)) guardTemplates.set(templateKey, { items: clone(guardItems), revision: 1, customized: false });
    const template = guardTemplates.get(templateKey);
    if (method === 'GET') return ok(template);
    if (method === 'PUT' || key === 'reset' && method === 'POST') {
      template.items = clone(key === 'reset' ? guardItems : data.items || []); template.revision++; template.customized = key !== 'reset';
      const response = guardTasks.flatMap(t => t.responses).find(r => r.response_id === data.response_id);
      if (response) { response.cells = { ...clone(data.cells || response.cells), template_items: clone(template.items), template_revision: template.revision, template_customized: template.customized }; response.version++; }
      return ok({ template, response: response || null });
    }
    return null;
  }
  if (area === 'weather-status' && method === 'GET') return ok({ enabled: false, paused: true, status: 'idle', job: null, error: '', message: '隔离预览不访问天气网络。' });
  if (['weather-refresh', 'weather-pause', 'source-files'].includes(area)) return unsupported();
  return null;
}

const checkDefinition = { version: 1, items: Array.from({ length: 3 }, (_, i) => ({ id: 'collab-check-item-' + i,
  text: i === 0 ? title : '检查设备运行状态及交接事项 ' + (i + 1) })) };
const checkDefinitions = new Map(scopes.map(scope => [scope, clone(checkDefinition)]));
const checkTasks = scopes.flatMap(scope => ['白', '夜'].map((shift, i) => ({ id: `collab-check-${scope}-${i}`, scope, shift, day: day(),
  due: Math.floor(Date.now() / 1000) + (i ? 3600 : -600), deadline: Math.floor(Date.now() / 1000) + (i ? 36000 : 10800),
  files: [], handover_photo: null, can_upload_handover: true, definition_version: 1, items: clone(checkDefinition.items),
  people: people.slice(0, 2).map(p => ({ name: p.name })), submitted: '', actor_name: '', can_submit: true, answers: [], sync_status: 'pending' })));
const checkHistory = Array.from({ length: 3 }, (_, i) => ({ ...clone(checkTasks[i]), id: 'collab-history-' + i,
  submitted: now(), actor_name: people[i].name, normal_count: 2, abnormal_count: 1, can_submit: false, sync_status: 'synced',
  answers: checkDefinition.items.map((item, n) => ({ ...item, status: n === 2 ? 'abnormal' : 'normal', note: n === 2 ? '隔离样例：已安排检查异常设备，等待复核确认。' : '' })) }));
function preShift(url, method, data, parts) {
  const [area] = parts;
  const scope = url.searchParams.get('scope') || 'A', definition = checkDefinitions.get(scope) || checkDefinition;
  if (!area && method === 'GET') return ok({ server_time: now(), scope: url.searchParams.get('scope') || 'A', scopes,
    can_manage: true, can_configure_location: false, definition, roster_ready: true, roster_error: '',
    tasks: scoped(checkTasks, url), overview: { due_count: scoped(checkTasks, url).filter(t => !t.submitted && t.due <= Date.now() / 1000).length } });
  if (area === 'recipients' && method === 'GET') return ok({ items: people });
  if (area === 'history' && method === 'GET') return ok(page(scoped(checkHistory, url), url, { scope: url.searchParams.get('scope') || 'A' }));
  if (area === 'definition' && method === 'PUT') {
    const updated = { version: definition.version + 1, items: clone(data.items || []) }; checkDefinitions.set(scope, updated);
    for (const task of checkTasks.filter(t => t.scope === scope)) if (!task.submitted) { task.items = clone(updated.items); task.definition_version = updated.version; }
    return ok(updated);
  }
  if (area === 'submit' && method === 'POST') {
    const task = checkTasks.find(t => t.id === data.task_id); if (!task) return missing();
    if (task.submitted) return ok({ completed: true, already_completed: true, actor_name: task.actor_name });
    task.answers = (data.answers || []).map(answer => ({ ...task.items.find(i => i.id === answer.id), ...clone(answer) }));
    Object.assign(task, { submitted: now(), actor_name: people[0].name, can_submit: false, sync_status: 'synced' }); checkHistory.unshift(clone(task));
    return ok({ completed: true, actor_name: task.actor_name });
  }
  if (['files', 'photo-preview', 'location-settings'].includes(area)) return unsupported();
  return null;
}

const handoverSources = people.map((person, i) => ({ id: (i + 1).toString().repeat(64), title: '维修交接：' + title,
  kind: i === 1 ? 'todo' : 'ongoing', business_status: i === 1 ? '未完成' : '进行中', scope: 'A',
  module: 'repair_management', period: day().slice(0, 7), source_url: '/repair-management', source_checked_at: now(), participated: true, responsible: true }));
const handovers = people.map((person, i) => ({ id: (i + 4).toString().repeat(32), title: '工作交接：' + title, recipient_id: people[1].id, recipient_name: people[1].name,
  owner: { id: people[0].id, name: people[0].name }, status: i === 0 ? 'draft' : 'pending', version: 1, notes: '请核对未完成维修与风险排查的处理状态。',
  items: handoverSources.map(s => ({ ...s, source_id: s.id, done: false, removed: false })), created_at: now(), updated_at: now(),
  is_owner: true, is_recipient: true, files: [], audit: [], notification: { state: 'sent' }, total: 3, remaining: 3 }));
function handoverFixture(url, method, data, parts) {
  const [key, action] = parts;
  if (key === 'people' && method === 'GET') return ok({ items: people, self: { id: people[0].id, name: people[0].name, category: 'engineer', home_scope: 'A' },
    scopes, default_modules: ['repair_management', 'risk_inspection'], modules: [{ value: 'repair_management', label: '维修项目' }, { value: 'risk_inspection', label: '风险排查' }, { value: 'maintenance', label: '维保通告' }] });
  if (key === 'sources' && method === 'GET') return ok({ items: search(scoped(handoverSources, url), url), warnings: [], failed_modules: [] });
  if (!key && method === 'GET') return ok(page(handovers, url));
  if (!key && method === 'POST') { const doc = { ...clone(handovers[0]), ...data, id: data.id || hex(), status: 'draft', version: 1 }; handovers.push(doc); return ok(doc); }
  const doc = handovers.find(row => row.id === key); if (!doc) return missing();
  if (!action && method === 'GET') return ok(doc);
  if (!action && method === 'PUT') { Object.assign(doc, clone(data.data || {}), { version: doc.version + 1, updated_at: now() }); doc.total = doc.items.length; doc.remaining = doc.items.filter(i => !i.done && !i.removed).length; return ok(doc); }
  if (action === 'refresh' && method === 'POST') return ok(doc);
  if (action === 'actions' && method === 'POST') {
    if (data.action === 'delete_draft') { handovers.splice(handovers.indexOf(doc), 1); return ok({ id: key, deleted: true }); }
    if (['send', 'retry-notification'].includes(data.action)) return unsupported();
    if (data.action === 'accept') { doc.status = 'accepted'; doc.decision_by = people[1].name; doc.decision_at = now(); }
    else { const item = doc.items.find(i => i.id === data.item_id); if (!item) return missing();
      if (['complete', 'reopen'].includes(data.action)) item.done = data.action === 'complete';
      else if (['remove', 'restore'].includes(data.action)) item.removed = data.action === 'remove'; else return null; }
    doc.version++; doc.remaining = doc.items.filter(i => !i.done && !i.removed).length; return ok(doc);
  }
  return null;
}

const dailyCategories = ['maintenance', 'repair', 'risk'].map((key, i) => ({ key, label: ['维保通告', '维修项目', '风险排查'][i], count: 3,
  tasks: Array.from({ length: 3 }, (_, n) => ({ task_id: `collab-daily-${key}-${n}`, category: key, category_label: ['维保通告', '维修项目', '风险排查'][i],
    type_key: key, type_label: '现场处理', title: title + ' · ' + (n + 1), status: n === 2 ? '已完成' : '进行中', status_tone: n === 2 ? 'completed' : 'ongoing',
    time: day() + ' 09:00', sort_time: Date.now() / 1000, action_summary: '已完成现场核对，待记录处理结果。', building: scopes[i], specialty: '暖通', level: '', progress_percent: n === 2 ? 100 : 35 })) }));
function morningModel(date = day()) { return { date, generated: false, weather_condition: '多云（隔离样例）', dry_bulb_temperature: 25, wet_bulb_temperature: 21,
  rows: scopes.map(scope => ({ scope, label: scope + '楼', lines: [title, '检修项目待复核与当日值班交接事项。'] })), warnings: [] }; }
function dailyFixture(url, method, data, parts) {
  if (!parts.length && method === 'GET') {
    const scope = url.searchParams.get('scope'), categories = dailyCategories.map(c => ({ ...c, tasks: !scope || scope === 'ALL' || scope === 'H' ? c.tasks : c.tasks.filter(t => t.building === scope) })).map(c => ({ ...c, count: c.tasks.length }));
    const tasks = categories.flatMap(c => c.tasks); return ok({ date: url.searchParams.get('date') || day(), generated_at: now(), categories, warnings: [],
      stats: { total: tasks.length, ongoing: tasks.filter(t => t.status_tone === 'ongoing').length, completed: tasks.filter(t => t.status_tone === 'completed').length, attention: 0 } });
  }
  if (parts[0] === 'morning-meeting' && ['preview', 'print-model'].includes(parts[1]) && method === 'GET') return ok(morningModel(url.searchParams.get('date') || day()));
  if (parts[0] === 'send' || parts[1] === 'generate') return unsupported();
  return null;
}

const kbGroups = [{ id: 'collab-group-1', name: '运行资料与应急处理流程', parent_id: null },
  { id: 'collab-group-2', name: '暖通专业与设备操作说明（长文件夹名称）', parent_id: 'collab-group-1' },
  { id: 'collab-group-3', name: '交接与培训', parent_id: null }];
const kbDocs = Array.from({ length: 3 }, (_, i) => ({ id: 'collab-doc-' + (i + 1), name: '21V-JSNTFOC-EOP-机房局部高温故障应急处理流程与现场操作说明-' + (i + 1) + '.txt',
  owner_name: people[0].name, active_version: 1, version: 1, can_edit: true, status: 'ready', updated_at: Date.now() / 1000,
  size: 1600, chunks: 3, group_id: kbGroups[i].id, group_name: kbGroups[i].name }));
function sections(doc) { return Array.from({ length: 3 }, (_, i) => ({ ordinal: i, location: '第' + (i + 1) + '节：' + title,
  text: '隔离资料预览。先核对现场设备状态，记录异常参数，按批准的操作流程处理并持续复核。'.repeat(i + 2), document_id: doc.id, version: doc.active_version })); }
function knowledge(url, method, data, parts) {
  const [area, key, action] = parts;
  if (!area && method === 'GET') {
    let items = search(kbDocs.filter(d => (d.status === 'deleted') === (url.searchParams.get('deleted') === '1')), url);
    const group = url.searchParams.get('group') || url.searchParams.get('group_id');
    if (group) { const descendants = new Set([group]); let changed = true; while (changed) { changed = false; for (const g of kbGroups) if (descendants.has(g.parent_id) && !descendants.has(g.id)) { descendants.add(g.id); changed = true; } }
      items = items.filter(d => group === 'ungrouped' ? !d.group_id : descendants.has(d.group_id)); }
    return ok(page(items, url, { is_admin: true, revision: sequence, settings: { mode: 'local', engine_status: 'ready' } }));
  }
  if (area === 'groups') {
    if (!key && method === 'GET') return ok({ groups: kbGroups.map(g => ({ ...g, owner_name: people[0].name, created_at: Date.now() / 1000, count: kbDocs.filter(d => d.group_id === g.id && d.status !== 'deleted').length, can_manage: true })), total: kbGroups.length });
    if (!key && method === 'POST') { const group = { id: id('collab-group'), name: data.name || '新文件夹', parent_id: data.parent_id || null }; kbGroups.push(group); return ok(group); }
    if (action === 'documents' && method === 'PUT') {
      if (key !== 'ungrouped' && !kbGroups.some(g => g.id === key)) return missing();
      let updated = 0; for (const doc of kbDocs) if ((data.document_ids || []).includes(doc.id)) { doc.group_id = key === 'ungrouped' ? null : key; doc.group_name = kbGroups.find(g => g.id === key)?.name || ''; updated++; } return ok({ updated });
    }
    const group = kbGroups.find(g => g.id === key); if (!group) return missing();
    if (method === 'PATCH') { if (data.parent_id === key) return fail('不能把文件夹移到自身。'); Object.assign(group, { name: data.name || group.name, parent_id: data.parent_id || null }); return ok(group); }
    if (method === 'DELETE') { kbGroups.splice(kbGroups.indexOf(group), 1); for (const g of kbGroups) if (g.parent_id === key) g.parent_id = null; for (const d of kbDocs) if (d.group_id === key) { d.group_id = null; d.group_name = ''; } return ok({ deleted: true }); }
    return null;
  }
  if (area === 'documents') {
    const doc = kbDocs.find(d => d.id === key); if (!doc) return missing();
    if (!action && method === 'GET') return ok({ document: { ...doc, view_version: Number(url.searchParams.get('version')) || doc.active_version }, sections: sections(doc), total: 3, page: 1, page_size: 20 });
    if (action === 'file' && method === 'GET') return { status: 200, contentType: 'text/plain; charset=utf-8', body: sections(doc).map(s => s.text).join('\n\n') };
    if (!action && method === 'DELETE') { doc.status = 'deleted'; doc.version++; return ok(doc); }
    if (action === 'restore' && method === 'POST') { doc.status = 'ready'; doc.version++; return ok(doc); }
    if (action === 'retry' && method === 'POST') return unsupported();
    return null;
  }
  if (area === 'search' && ['GET', 'POST'].includes(method)) return ok({ mode: 'keyword', warning: '隔离样例检索结果，不访问模型或向量服务。',
    items: kbDocs.filter(d => d.status === 'ready').map(d => ({ ...sections(d)[0], name: d.name, url: '/knowledge-base?document=' + d.id + '&version=1&chunk=0' })) });
  if (area === 'files') return unsupported();
  return null;
}

const links = Array.from({ length: 27 }, (_, i) => ({ id: 'collab-link-' + (i + 1), name: i < 3 ? '常用操作与资料导航：' + title : '隔离导航 ' + (i + 1),
  url: ['/learning', '/work-handover', '/knowledge-base'][i % 3], purpose: '隔离页面入口，验证长文本、分类和翻页操作。', sort: i + 1, category: i % 2 ? '网页导航' : '设备管理' }));
function navigation(url, method, data, parts) {
  const [key] = parts;
  const listing = () => ({ items: search(links, url), total: links.length, can_edit: true, updated_at: Date.now() / 1000, stale: false, error: '' });
  if (!key && method === 'GET' || key === 'refresh' && method === 'POST') return ok(listing());
  if (!key && method === 'POST') { const item = { ...data, id: id('collab-link') }; links.push(item); return ok({ item }); }
  if (key === 'reorder' && method === 'POST') {
    const source = links.findIndex(x => x.id === data.record_id), target = links.find(x => x.id === data.target_id);
    if (source < 0 || !target || target.id === data.record_id) return fail('请选择不同的导航项。');
    const [item] = links.splice(source, 1); links.splice(links.indexOf(target) + (data.placement === 'after' ? 1 : 0), 0, item);
    links.forEach((x, i) => { x.sort = i + 1; }); return ok(listing());
  }
  const item = links.find(x => x.id === key); if (!item) return missing();
  if (method === 'PUT') { Object.assign(item, data, { id: key }); return ok({ item }); }
  if (method === 'DELETE') { links.splice(links.indexOf(item), 1); return ok({ deleted: true }); }
  return null;
}

/** In-memory fake API. Importing this module never starts tests or a server. */
export async function collabFixture({ url, method = 'GET', body }) {
  if (!(url instanceof URL)) throw new TypeError('collabFixture expects url to be a URL object');
  const verb = method.toUpperCase(), data = payload(body);
  if (url.pathname === '/api/handover-links' && verb === 'GET') return ok({ links: Object.fromEntries(['110', ...scopes].map(scope => [scope, 'http://127.0.0.1:14536/placeholder?scope=' + scope])) });
  const handlers = [
    ['/api/learning', learning], ['/api/drills', drillFixture], ['/api/risk-inspections', riskFixture],
    ['/api/critical-guard', guardFixture], ['/api/pre-shift-checks', preShift],
    ['/api/work-handovers', handoverFixture], ['/api/daily-tasks', dailyFixture],
    ['/api/assistant/knowledge', knowledge], ['/api/link-directory', navigation],
  ];
  for (const [base, handler] of handlers) {
    if (url.pathname !== base && !url.pathname.startsWith(base + '/')) continue;
    const parts = url.pathname.slice(base.length).split('/').filter(Boolean).map(decodeURIComponent);
    return handler(url, verb, data, parts);
  }
  return null;
}
