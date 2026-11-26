// Runs component state in memory: no browser, portal, model, or business writes.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { createRequire, Module } from 'node:module';
import { parse, compileScript, compileTemplate } from '@vue/compiler-sfc';
import { build } from 'esbuild';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
async function loadComponent(filename) {
const source = await readFile(filename, 'utf8');
const { descriptor } = parse(source);
const script = compileScript(descriptor, { id: 'state-check' });
assert.deepEqual(compileTemplate({ source: descriptor.template.content, filename, id: 'state-check', compilerOptions: { bindingMetadata: script.bindings } }).errors, []);
const { outputFiles } = await build({ stdin: { contents: script.content, resolveDir: dirname(filename), sourcefile: filename, loader: 'ts' },
  bundle: true, write: false, format: 'cjs', platform: 'node', packages: 'external', plugins: [{ name: 'isolated-components', setup(builder) {
    builder.onResolve({ filter: /\.vue$/ }, args => ({ path: args.path, namespace: 'component' }));
    builder.onLoad({ filter: /.*/, namespace: 'component' }, () => ({ contents: 'export default {};' }));
    builder.onResolve({ filter: /api\/client$/ }, () => ({ path: 'api', namespace: 'api' }));
    builder.onLoad({ filter: /.*/, namespace: 'api' }, () => ({ contents: 'export const assistantClientId = async () => "isolated-client"; export const requestJson = (...args) => globalThis.fixtureRequest(...args);' }));
  } }] });
const require = createRequire(filename);
const componentModule = new Module(filename);
componentModule.paths = require.resolve.paths('vue');
componentModule._compile(outputFiles[0].text, filename);
return componentModule.exports.default;
}
const memory = new Map();
const storage = { getItem: key => memory.get(key) ?? null, setItem: (key, value) => memory.set(key, value), removeItem: key => memory.delete(key) };
globalThis.sessionStorage = storage;
globalThis.window = { localStorage: storage, sessionStorage: storage, location: new URL('http://fixture.invalid/'),
  addEventListener() {}, removeEventListener() {}, dispatchEvent() {}, clearTimeout, clearInterval,
  setTimeout: (...args) => { const timer = setTimeout(...args); timer.unref(); return timer; },
  innerWidth: 1440, innerHeight: 1000, matchMedia: () => ({ matches: false }) };
const require = createRequire(import.meta.url);
const { createApp, effectScope, nextTick, reactive } = require('vue');
const component = await loadComponent(resolve(root, 'src/components/LighthouseAssistant.vue'));
function setup(account = 'account-A', target = component, extraProps = {}) {
  const scope = effectScope(), previous = console.warn;
  // Lifecycle hooks are deliberately not mounted in this state-only check.
  console.warn = message => { if (!String(message).includes('no active component instance')) previous(message); };
  try { return { vm: createApp({}).runWithContext(() => scope.run(() =>
    target.setup({ userId: account, userName: account, ...extraProps }, { expose() {}, emit() {} }))), stop: () => scope.stop() }; }
  finally { console.warn = previous; }
}
const plan = id => ({ id, version: 1, status: 'needs_input', title: id, operations: [], fields: [{ name: 'title', type: 'text', value: '原内容' }] });
const snapshot = () => ({ conversation_id: 'conversation-A', configured: true, enabled: true, turns: ['one', 'two'].map((id, index) => ({ operation_id: id, status: 'completed', at: index, plan: plan(id) })) });
const first = setup(), vm = first.vm;
const message = { api_id: 'POST /api/message-delivery/send', body: { text: '较早报告的完整正文', recipient_ids: ['private-id'] },
  selected_labels: { recipient_names: '测试甲 · 工号 1001', message_content: '10-05 · 先前报告' }, selected_files: [{ name: '报告.pdf' }] };
assert.deepEqual(vm.operationPreview(message).map(row => row.value), ['测试甲 · 工号 1001', '10-05 · 先前报告', '较早报告的完整正文', '报告.pdf']);
assert.deepEqual(vm.operationPreview(message, true), []);
assert.equal(vm.planFieldIsGroup({ type: 'multiselect' }), true);
assert.equal(vm.planFieldIsGroup({ type: 'object' }), true);
assert.equal(vm.planFieldIsGroup({ type: 'select', options: [{ value: 'self', label: '本人' }] }), true);
assert.equal(vm.planFieldIsGroup({ type: 'textarea' }), false);
assert.equal(vm.planFieldIsGroup({ type: 'select', options_source: 'message_recipients' }), false);
assert.notEqual(vm.planFieldId({ id: 'test' }, { name: '步骤.姓名' }), vm.planFieldId({ id: 'test' }, { name: '步骤.工号' }));
vm.replaceState(snapshot());
await nextTick();
vm.draft.value = '还没有发送的问题';
vm.planValues['two:title'] = '未提交的业务填写';
vm.saveDrafts();
const reload = setup();
reload.vm.replaceState(snapshot());
await nextTick();
assert.equal(reload.vm.draft.value, '还没有发送的问题');
assert.equal(reload.vm.planValues['two:title'], '未提交的业务填写');
const other = setup('account-B');
other.vm.replaceState(snapshot());
await nextTick();
assert.equal(other.vm.draft.value, '');
assert.equal(other.vm.planValues['two:title'], '原内容');
let finish;
globalThis.fixtureRequest = url => url.includes('/one') ? new Promise(resolve => { finish = resolve; }) : Promise.reject(new Error('第二条操作未完成'));
const pending = vm.planCall(vm.state.value.turns[0], '/confirm', 'POST', {});
assert.equal(vm.isPlanBusy(vm.state.value.turns[0].plan), true);
assert.equal(vm.isPlanBusy(vm.state.value.turns[1].plan), false);
await vm.planCall(vm.state.value.turns[1], '/confirm', 'POST', {});
assert.equal(vm.planErrors.two, '第二条操作未完成');
vm.replaceState(snapshot());
await nextTick();
assert.equal(vm.planErrors.two, '第二条操作未完成');
assert.equal(vm.planValues['two:title'], '未提交的业务填写');
finish({ ...plan('one'), status: 'completed' });
await pending;
assert.equal(vm.isPlanBusy(plan('one')), false);
assert.equal(vm.state.value.turns[0].plan.status, 'completed');
assert.equal(vm.planIsFinished(vm.state.value.turns[0].plan), true);
assert.equal(!!vm.expandedPlans.one, false);
assert.equal(vm.planIsFinished({ status: 'failed' }), false);
vm.replaceState({ ...snapshot(), conversation_id: 'new-conversation', turns: [] });
await nextTick();
vm.saveDrafts();
assert.equal(vm.draft.value, '');
assert.equal(Object.keys(vm.planValues).length, 0);
assert.equal(memory.has('lighthouse_draft:account-A'), false);
vm.replaceState(snapshot());
let finishHistory;
globalThis.fixtureRequest = () => new Promise(resolve => { finishHistory = resolve; });
const oldHistory = vm.loadHistory();
vm.replaceState({ ...snapshot(), conversation_id: 'cleared-conversation', turns: [] });
finishHistory({ turns: [{ operation_id: 'old-history', status: 'completed', at: -1 }], has_more: false });
await oldHistory;
assert.equal(vm.state.value.turns.length, 0, 'late history must not restore a cleared conversation');
assert.equal(vm.hasOlder.value, true, 'old history paging must not overwrite the new conversation');
vm.replaceState(snapshot());
globalThis.fixtureRequest = async () => ({ turns: [{ operation_id: 'older', status: 'completed', at: -1 }], has_more: false });
await vm.loadHistory();
assert.equal(vm.state.value.turns[0].operation_id, 'older', 'current conversation history must still load');
let failHistory;
globalThis.fixtureRequest = () => new Promise((_resolve, reject) => { failHistory = reject; });
const rejectedHistory = vm.loadHistory();
vm.replaceState({ ...snapshot(), conversation_id: 'another-conversation', turns: [] });
vm.error.value = '';
failHistory(new Error('Old conversation history failed'));
await rejectedHistory;
assert.equal(vm.error.value, '', 'late history errors must not leak into a new conversation');
globalThis.Element = class { closest() { return {}; } };
globalThis.document = { activeElement: new Element() };
vm.thread.value = { scrollTop: 37, scrollHeight: 100, clientHeight: 100, contains: () => true };
vm.scrollBottom();
await nextTick();
assert.equal(vm.thread.value.scrollTop, 37, 'background answers must not scroll the form being edited');
assert.equal(vm.hasNewContent.value, true);
vm.thread.value = null;
vm.state.value.turns = [{ run_id: 'run-1', status: 'pending', answer: 'same answer' }];
vm.streamChat.value = { messages: [{ role: 'assistant', metadata: { run_id: 'run-1' }, parts: [{ type: 'text', text: 'same answer' }] }] };
vm.renderStream();
assert.equal(vm.hasNewContent.value, true, 'unchanged stream fragments must not trigger scroll');
const sdkMessages = reactive({ messages: [{ role: 'assistant', metadata: { run_id: 'run-1' }, parts: [{ type: 'text', text: 'same answer' }] }] });
vm.streamChat.value = sdkMessages;
await nextTick();
sdkMessages.messages[0] = { ...sdkMessages.messages[0], parts: [{ type: 'text', text: 'same answer extended' }] };
await nextTick();
await new Promise(resolve => setTimeout(resolve, 200));
assert.equal(vm.state.value.turns[0].answer, 'same answer extended', 'SDK replacement must still render without a deep watcher');
document.hidden = true;
sdkMessages.messages[0] = { ...sdkMessages.messages[0], parts: [{ type: 'text', text: 'same answer extended while hidden' }] };
await nextTick();
await new Promise(resolve => setTimeout(resolve, 200));
assert.equal(vm.state.value.turns[0].answer, 'same answer extended', 'hidden pages should not keep rendering chunks');
document.hidden = false;
// A startup failure follows the existing attempt exclusively through conversation reads.
const recovery = setup('startup-fixture'), chat = recovery.vm;
const turn = { operation_id: 'original-question', attempt_id: 'first-attempt', run_id: 'original-run',
  question: '隔离原问题', status: 'failed', error_code: 'startup_catalog', error: '模型目录尚未就绪。' };
const conversation = (value, active = '') => ({ ...snapshot(), turns: [{ ...value }],
  active_run_id: active, busy: !!active, sync_stamp: value.status });
for (const error_code of ['startup_listener', 'startup_handshake', 'startup_catalog', 'startup_cooldown']) {
  assert.equal(chat.turnAction({ ...turn, error_code }), '重试原问题');
}
for (const receipt of [{ task_id: 'existing-task' }, { job_id: 'submitted-job' }, { plan: { status: 'submitted' } }]) {
  assert.equal(chat.turnAction({ ...turn, ...receipt }), '查看原任务');
}
assert.equal(chat.turnAction({ ...turn, client_pending: true }), '查看原进度');
assert.equal(chat.turnAction({ ...turn, error_code: 'runtime_startup' }), '查看原进度');
chat.replaceState(conversation({ ...turn, client_pending: true }));
const reads = [];
let observed = conversation({ ...turn, status: 'pending', error: '', error_code: '' }, turn.run_id);
globalThis.fixtureRequest = async (url, options) => {
  reads.push([url, options.method]);
  assert.equal(url, '/api/assistant/conversation');
  assert.equal(options.method, 'GET');
  return structuredClone(observed);
};
await chat.send(chat.state.value.turns[0]);
assert.equal(chat.state.value.turns[0].status, 'pending', 'same run and attempt resume from the server snapshot');
assert.equal(chat.state.value.turns[0].run_id, turn.run_id);
chat.replaceState(conversation({ ...turn, status: 'stopped' }));
await chat.readState(false, true);
assert.equal(chat.state.value.turns[0].status, 'stopped', 'late pending snapshot cannot undo a stop');
const legacy = { ...turn, error_code: '', error: '助手启动超时，原消息已保留，可继续。诊断编号:fixture' };
observed = conversation(legacy);
chat.replaceState(structuredClone(observed));
await chat.resumeResearch({ ...legacy, resume_available: true });
assert.equal(reads.length, 3, 'legacy timeout and research entry only reread');
chat.replaceState(conversation(turn));
let ambiguousPosts = 0;
globalThis.fixtureRequest = async (url, options) => {
  if (options.method === 'POST') { ambiguousPosts++; throw new Error('lost retry receipt'); }
  return conversation(turn); // Storage still exposes only the previous failed attempt.
};
await chat.send(chat.state.value.turns[0]);
assert.equal(ambiguousPosts, 1);
const afterReload = setup('startup-fixture');
afterReload.vm.replaceState(conversation(turn));
assert.equal(afterReload.vm.turnAction(afterReload.vm.state.value.turns[0]), '查看原进度');
await afterReload.vm.send(afterReload.vm.state.value.turns[0]);
assert.equal(ambiguousPosts, 1, 'old receipt after reload does not authorize another retry');
chat.replaceState(conversation({ ...turn, attempt_id: afterReload.vm.state.value.turns[0].attempt_id,
  run_id: 'confirmed-retry', status: 'completed' }));
afterReload.stop();

for (const message of ['HTTP 503', '请求超时，请稍后重试。']) {
  let posts = 0, gets = 0, body;
  chat.replaceState({ ...snapshot(), turns: [] });
  chat.draft.value = '只提交一次';
  globalThis.fixtureRequest = async (url, options) => {
    if (url === '/api/assistant/messages') {
      posts++; body = JSON.parse(options.body); throw new Error(message);
    }
    assert.equal(url, '/api/assistant/conversation'); gets++;
    return conversation({ ...turn, ...body, status: 'completed', answer: '原运行已完成', error: '', error_code: '' });
  };
  await chat.send();
  assert.equal(posts, 1); assert.equal(gets, 1);
  assert.equal(chat.state.value.turns.length, 1);
  assert.equal(chat.state.value.turns[0].attempt_id, body.attempt_id);
  assert.equal(chat.state.value.turns[0].status, 'completed');
  assert.equal(chat.error.value, '', 'lost POST receipt cannot leave a stale error on a confirmed answer');
}
recovery.stop();
vm.open.value = true;
vm.resumeStreamRendering();
assert.equal(vm.state.value.turns[0].answer, 'same answer extended while hidden');
for (const instance of [first, reload, other]) instance.stop();
const structured = await loadComponent(resolve(root, 'src/components/LighthouseStructuredField.vue'));
for (const [field, expected] of [
  [{ type: 'multiselect', choice_group: true, options: [{ value: 'a', label: 'A' }] }, true],
  [{ type: 'select', options: [{ value: 'a', label: 'A' }] }, true],
  [{ type: 'select', person_picker: true, options: [{ value: 'a', label: 'A' }] }, false],
  [{ type: 'textarea' }, false],
]) {
  const control = setup('account-A', structured, { id: 'field-test', field, modelValue: '', context: [] });
  assert.equal(Boolean(control.vm.isChoiceGroup.value), expected);
  control.stop();
}
const convergence = setup('account-A', await loadComponent(resolve(root, 'src/components/PlanConvergencePage.vue')));
const page = convergence.vm;
page.detail.value = { alarmBlockDetailResultList: [] };
page.selectedId.value = '123';
page.ruleSets.value = [{ id: 'one', name: '规则一' }, { id: 'two', name: '规则二' }];
page.setId.value = 'one';
await nextTick();
globalThis.fixtureRequest = () => new Promise(resolve => { finish = resolve; });
const match = page.matchRules();
page.setId.value = 'two';
await nextTick();
finish({ passed: true });
await match;
assert.equal(page.matchResult.value, null, 'late result from previous rule must not be shown');
page.sheets.value = [{ name: '场景一', rows: [{}] }, { name: '场景二', rows: [{}] }];
page.sheetName.value = '场景一';
await nextTick();
const compare = page.compareExcel();
page.sheetName.value = '场景二';
await nextTick();
finish({ passed: true });
await compare;
assert.equal(page.excelResult.value, null, 'late result from previous sheet must not be shown');
page.matchResult.value = { passed: true };
globalThis.fixtureRequest = () => Promise.reject(new Error('VPN offline'));
await page.matchRules();
assert.equal(page.matchResult.value, null, 'failed recheck must not leave old success visible');
convergence.stop();
console.log('[AssistantStateCheck] draft restore, account isolation, parallel plans, retained errors, collapse and clear OK');
console.log('[PlanReviewStateCheck] rule/sheet switching, late responses and failed recheck OK');
