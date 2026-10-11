<template>
  <section class="notice-panel" aria-label="通告待办" :aria-busy="busy || preparing">
    <header class="notice-head">
      <div class="notice-heading"><span class="notice-mark"><ListChecks :size="19" /></span><div><h3>通告待办</h3><small>{{ editionLabel }}</small></div></div>
    </header>
    <div class="notice-filters">
      <label><span>楼栋</span><span class="notice-select"><select v-model="scope" aria-label="楼栋" :disabled="busy || running" @change="changeScope"><option value="">请选择楼栋</option><option v-for="item in meta.scopes || []" :key="item.value" :value="item.value">{{ item.label }}</option></select><ChevronDown :size="13" aria-hidden="true" /></span></label>
      <label v-if="meta.edition?.slot === 'evening'"><span>时段</span><span class="notice-select"><select v-model="slot" aria-label="时段" :disabled="busy || running" @change="loadCurrent(false)"><option value="morning">今日 08:00</option><option value="evening">今日 17:00</option></select><ChevronDown :size="13" aria-hidden="true" /></span></label>
      <label class="notice-search"><Search :size="15" /><input v-model="search" aria-label="搜索通告" placeholder="搜索通告名称" @input="page = 1" /></label>
    </div>
    <div class="notice-tabs" role="tablist" aria-label="通告分类">
      <button v-for="tab in noticeTabs" :id="'notice-tab-' + tab.key" :key="tab.key" role="tab" :aria-selected="noticeView === tab.key" aria-controls="notice-tab-content" :tabindex="noticeView === tab.key ? 0 : -1" :disabled="busy || loading || preparing || !!run && !editable" @click="switchView(tab.key)" @keydown.left.prevent="switchView('ongoing', true)" @keydown.right.prevent="switchView('planned', true)">{{ tab.label }}<span aria-hidden="true">{{ tab.count }}</span></button>
    </div>
    <p v-if="error || run?.error" class="notice-error" role="alert">{{ error || run?.error }}</p>
    <button v-if="conflict" class="conflict-refresh" :disabled="busy" @click="readLatest">读取最新草稿</button>
    <p v-if="draftWarning" class="notice-warning" role="status">{{ draftWarning }}</p>
    <div v-if="run && !loading && !preparing" class="notice-count"><span class="notice-stage">{{ editable ? '待办清单' : run.state === 'confirm' ? '发送前确认' : running ? '正在办理' : '办理结果' }}</span><span><Loader2 v-if="running" class="spin" :size="13" />{{ filtered.length }} 条<span class="selection-count">已选 {{ selected.length }}</span></span></div>
    <div id="notice-tab-content" ref="contentRoot" class="notice-content" role="tabpanel" :aria-labelledby="'notice-tab-' + noticeView">
      <UiTransition name="notice-page" mode="out-in"><div :key="noticeView">
      <div v-if="loading || preparing" class="notice-empty"><Loader2 class="spin" :size="24" /><span>正在整理本地通告…</span></div>
      <div v-else-if="!scope" class="notice-empty"><Building2 :size="26" /><span>请选择要办理的楼栋</span></div>
      <div v-else-if="!meta.edition && !run" class="notice-empty"><Clock3 :size="26" /><span>今日待办将在 08:00 更新</span></div>
      <div v-else-if="run?.state === 'failed'" class="notice-empty"><AlertCircle :size="25" /><span>待办读取未完成</span><button @click="act('refresh')" :disabled="busy"><RefreshCw :size="14" />重新读取</button></div>
      <template v-else-if="run">
        <div v-if="!filtered.length" class="notice-empty"><Check :size="24" /><span>{{ search ? '没有匹配的通告' : noticeView === 'ongoing' ? '当前没有进行中的通告' : '当前没有未开始的计划' }}</span></div>
        <article v-for="item in visibleRows" :key="item.key" class="notice-row" :class="{ selected: item.selected, blocked: !!item.blocked }">
          <div class="row-heading">
            <input v-if="editable" :id="'np-' + run.id + '-' + item.key" type="checkbox" :checked="item.selected" :disabled="busy || !!item.blocked" :aria-label="'选择' + item.title" @change="selectRow(item, ($event.target as HTMLInputElement).checked)" />
            <label v-if="editable" :for="'np-' + run.id + '-' + item.key" class="notice-title">{{ item.title }}</label><strong v-else class="notice-title">{{ item.title }}</strong>
            <button v-if="editable && item.selected" class="icon" :title="expanded[item.key] ? '折叠填写区' : '展开填写区'" :aria-label="expanded[item.key] ? '折叠填写区' : '展开填写区'" :aria-expanded="!!expanded[item.key]" @click="expanded[item.key] = !expanded[item.key]"><ChevronUp v-if="expanded[item.key]" :size="16" /><ChevronDown v-else :size="16" /></button>
          </div>
          <div class="notice-details"><div class="notice-meta"><span class="notice-kind">{{ typeLabel(item) }}</span><span>{{ item.status }}</span><span class="notice-operation" :class="{ finishing: item.draft?.notice_action === '结束' }">{{ item.action === 'start' ? '待开始' : item.draft?.notice_action || '更新 / 结束' }}</span></div>
          <small v-if="item.window" class="notice-window"><Clock3 :size="12" aria-hidden="true" />{{ item.window }}</small></div>
          <p v-if="item.blocked" class="notice-warning">{{ item.blocked }}</p>
          <div v-if="editable && item.selected" class="row-mode"><span>通告内容</span><button :disabled="busy" @click="toggleFields(item)"><Pencil :size="13" />{{ item.edit_all ? '仅补充空白字段' : '编辑全部字段' }}</button></div>
          <UiTransition name="notice-collapse">
            <div v-if="editable && item.selected && expanded[item.key]" class="notice-fields-wrap"><div class="notice-fields">
              <div v-for="field in shownFields(item)" :key="field.key" class="notice-field">
                <LighthouseNoticeSop v-if="field.kind === 'notice_sop'" :id="'npf-' + item.key + '-sop'" :field="sopField(item)" :model-value="item.draft.notice_sop" :disabled="busy || !!sopDirectories[item.key]?.loading" @update:model-value="updateSop(item, $event)" @load-options="loadSops(item, $event.q)" />
                <RepairFieldControl v-else-if="!field.readonly && ['start_time', 'end_time'].includes(field.key)" :field="dateField" :input-id="'npf-' + item.key + '-' + field.key" :label="fieldLabel(field)" :model-value="repairDraftInputValue(dateField, item.draft[field.key])" :required="!!field.required" :disabled="busy" compact @update:model-value="item.draft[field.key] = $event.replace('T', ' '); changed()" />
                <template v-else><span v-if="field.readonly">{{ fieldLabel(field) }}</span><label v-else :for="'npf-' + item.key + '-' + field.key">{{ fieldLabel(field) }}<em v-if="field.required" aria-hidden="true"> *</em></label>
                <span v-if="field.readonly || String(item.draft?.[field.key] ?? '').length > 1000" class="readonly">{{ item.draft?.[field.key] || '—' }}</span>
                <select v-else-if="field.kind === 'select'" :id="'npf-' + item.key + '-' + field.key" v-model="item.draft[field.key]" :aria-required="field.required" :disabled="busy" @change="onFieldChange(item, field)"><option value="">请选择</option><option v-for="value in field.options || []" :key="value" :value="value">{{ value }}</option></select>
                <textarea v-else-if="field.kind === 'multiline'" :id="'npf-' + item.key + '-' + field.key" v-model="item.draft[field.key]" :aria-required="field.required" rows="1" maxlength="1000" :disabled="busy" @input="changed" />
                <input v-else :id="'npf-' + item.key + '-' + field.key" v-model="item.draft[field.key]" :aria-required="field.required" type="text" maxlength="1000" :disabled="busy" @input="changed" />
                </template>
              </div>
              <small v-if="!shownFields(item).length" class="notice-meta">暂无待补充字段</small>
            </div></div>
          </UiTransition>
          <p v-if="item.error" class="notice-error">{{ item.error }}</p>
          <pre v-if="run.state === 'confirm'" class="notice-preview">{{ item.preview }}</pre>
          <p v-if="running || run.state === 'done'" class="notice-result" :class="{ failed: item.phase === 'failed', success: item.phase === 'success' }"><Loader2 v-if="!['success', 'failed', 'unavailable'].includes(item.phase)" class="spin" :size="13" />{{ item.result || '等待处理' }}</p>
        </article>
      </template>
      </div></UiTransition>
    </div>
    <footer class="notice-footer">
      <div v-if="pages > 1" class="notice-pages"><button class="icon" :disabled="page <= 1" title="上一页" aria-label="上一页" @click="page--"><ChevronLeft :size="16" /></button><span>{{ page }} / {{ pages }}</span><button class="icon" :disabled="page >= pages" title="下一页" aria-label="下一页" @click="page++"><ChevronRight :size="16" /></button></div>
      <div v-if="editable" class="notice-actions"><button class="icon" :disabled="busy || !dirty" title="保存草稿" aria-label="保存草稿" @click="act('save')"><Save :size="17" /></button><button class="icon" :disabled="busy" title="重新读取待办" aria-label="重新读取待办" @click="refreshRows"><RefreshCw :size="17" /></button><button class="primary" :disabled="busy || !selected.length" @click="act('preview')"><Loader2 v-if="busy" class="spin" :size="15" /><Check v-else :size="15" />预览所选 {{ selected.length }} 条</button></div>
      <div v-else-if="run?.state === 'confirm'" class="notice-actions"><button :disabled="busy" @click="act('edit')">返回修改</button><button class="primary" :disabled="busy" @click="act('confirm')"><Loader2 v-if="busy" class="spin" :size="15" /><Send v-else :size="15" />确认发送 {{ selected.length }} 条</button></div>
      <div v-else-if="run?.state === 'done'" class="notice-actions"><button v-if="selected.some(item => item.convergence_confirmation_required)" class="primary" :disabled="busy" @click="act('confirm_unmatched')"><AlertCircle :size="15" />确认未匹配项并继续发送</button><button v-if="selected.some(item => item.phase === 'failed' && !item.convergence_confirmation_required)" :disabled="busy" @click="act('retry')"><RotateCcw :size="15" />重试失败项</button><button :disabled="busy" @click="act('refresh')"><RefreshCw :size="15" />继续办理其他通告</button></div>
      <span v-else-if="running" class="notice-meta">已提交，关闭面板后仍会继续办理</span>
      <span v-else-if="run?.state === 'expired'" class="notice-meta">此批待办已跨日，请查看今日待办</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { AlertCircle, Building2, Check, ChevronDown, ChevronLeft, ChevronRight, ChevronUp, Clock3, ListChecks, Loader2, Pencil, RefreshCw, RotateCcw, Save, Search, Send } from 'lucide-vue-next';
import { ApiError, requestJson, type Dict } from '../api/client';
import RepairFieldControl from './RepairFieldControl.vue';
import LighthouseNoticeSop from './LighthouseNoticeSop.vue';
import { repairDraftInputValue } from '../repairManagementUtils';

const props = defineProps<{ userId: string; active: boolean }>();
const emit = defineEmits<{ availability: [available: boolean]; edition: [id: string] }>();
const base = '/api/assistant/notice-panel';
const meta = ref<Dict>({ scopes: [] }), run = ref<Dict | null>(null), scope = ref(''), search = ref(''), page = ref(1);
const loading = ref(false), busy = ref(false), error = ref(''), draftWarning = ref('');
const slot = ref(''), conflict = ref(false);
const noticeView = ref('planned'), contentRoot = ref<HTMLElement | null>(null);
const expanded = reactive<Record<string, boolean>>({});
const sopDirectories = reactive<Record<string, Dict>>({});
const dateField = { ui_type: 'datetime' };
let original: Dict | null = null, disposed = false, serial = 0, timer = 0, saveTimer = 0, polling = false, editionPending = false;
let controller: AbortController | undefined;
const selected = computed<Dict[]>(() => (run.value?.items || []).filter((item: Dict) => item.selected));
const editable = computed(() => run.value?.state === 'edit');
const preparing = computed(() => run.value?.state === 'preparing');
const running = computed(() => run.value?.state === 'running');
const editionLabel = computed(() => run.value ? `${run.value.date} ${run.value.slot === 'evening' ? '17:00' : '08:00'}` : meta.value.edition?.label || '定时通告');
const noticeTabs = computed(() => [
  { key: 'ongoing', label: '进行中通告', count: (run.value?.items || []).filter((item: Dict) => item.action !== 'start').length },
  { key: 'planned', label: '未开始计划', count: (run.value?.items || []).filter((item: Dict) => item.action === 'start').length },
]);
function inCurrentView(item: Dict): boolean { return (item.action === 'start') === (noticeView.value === 'planned'); }
const filtered = computed<Dict[]>(() => {
  const rows = editable.value ? (run.value?.items || []).filter(inCurrentView) : selected.value;
  const query = search.value.trim().toLowerCase();
  return rows.filter((item: Dict) => !query || String(item.title).toLowerCase().includes(query));
});
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / 10)));
const visibleRows = computed(() => filtered.value.slice((Math.min(page.value, pages.value) - 1) * 10, Math.min(page.value, pages.value) * 10));
const dirty = computed(() => changes().length > 0);
function typeLabel(item: Dict): string { return ({ maintenance: '维保', change: '变更', repair: '检修', polling: '轮巡', adjust: '设备调整', power: '上下电' } as Dict)[item.work_type] || ''; }
function fieldLabel(field: Dict): string { return String(field.label || '').replace('（YYYY-MM-DD HH:mm）', ''); }
function briefKeys(item: Dict): string[] { return (item.fields || []).filter((field: Dict) => ['notice_action', 'notice_sop'].includes(field.key) || !field.readonly && (String(item.draft?.[field.key] ?? '').trim() === '' || field.key === 'progress' && !!(item.draft?.notice_action === '结束' && field.default_on_end || String(item.draft?.[field.key] ?? '') === String(field.default_on_end || '')))).map((field: Dict) => field.key); }
function shownFields(item: Dict): Dict[] { const keys = new Set([...(item.brief_fields || []), ...briefKeys(item)]); return (item.fields || []).filter((field: Dict) => item.edit_all || keys.has(field.key)); }
function changed(): void { error.value = ''; window.clearTimeout(saveTimer); saveTimer = window.setTimeout(saveLocal, 350); }
function onFieldChange(item: Dict, field: Dict): void {
  if (field.key === 'notice_action') {
    const progress = (item.fields || []).find((f: Dict) => f.key === 'progress');
    const endLine = String(progress?.default_on_end || '');
    if (String(item.draft?.notice_action ?? '') === '结束' && endLine) {
      if (!String(item.draft?.progress ?? '').trim()) item.draft.progress = endLine;
    } else if (String(item.draft?.notice_action ?? '') === '更新' && endLine) {
      if (String(item.draft?.progress ?? '') === endLine) item.draft.progress = '';
    }
  }
  changed();
}
function rememberView(): void {
  if (!run.value) return;
  try { window.sessionStorage.setItem(`notice-panel-view:${props.userId}`, JSON.stringify({ scope: run.value.scope, slot: run.value.slot, date: run.value.date, view: noticeView.value })); } catch { /* Optional navigation memory. */ }
}
function switchView(value: string, focus = false): void {
  if (busy.value || loading.value || preparing.value || run.value && !editable.value || noticeView.value === value) return;
  for (const item of run.value?.items || []) item.selected = false;
  noticeView.value = value; search.value = ''; page.value = 1;
  if (contentRoot.value) contentRoot.value.scrollTop = 0;
  changed(); rememberView(); saveLocal();
  if (focus) void nextTick(() => document.getElementById('notice-tab-' + value)?.focus());
}
function selectRow(item: Dict, value: boolean): void { item.selected = value; if (!item.brief_fields?.length) item.brief_fields = briefKeys(item); if (value) { for (const key in expanded) expanded[key] = false; expanded[item.key] = true; } changed(); }
function toggleFields(item: Dict): void { item.edit_all = !item.edit_all; expanded[item.key] = true; changed(); }
function sopField(item: Dict): Dict {
  const directory = sopDirectories[item.key] || {};
  return { ...directory.field, notice_scope: scope.value, work_type: item.work_type, directory_loading: !!directory.loading, directory_error: directory.error || '' };
}
function updateSop(item: Dict, value: Dict): void { item.draft.notice_sop = value; changed(); }
async function loadSops(item: Dict, query = ''): Promise<void> {
  if (!run.value || busy.value || sopDirectories[item.key]?.loading) return;
  const identity = run.value.id, entry = reactive<Dict>({ ...sopDirectories[item.key], loading: true, error: '' });
  sopDirectories[item.key] = entry;
  try {
    const result = await requestJson(base + '/' + identity + '/sop-options?' + new URLSearchParams({ item_key: item.key, q: query }), { timeoutMs: 15000 });
    if (disposed || run.value?.id !== identity || sopDirectories[item.key] !== entry) return;
    const people = new Map<string, Dict>((entry.field?.people || []).map((person: Dict) => [person.record_id, person]));
    for (const person of result.field.people || []) people.set(person.record_id, person);
    entry.field = { ...result.field, people: [...people.values()] };
  } catch (e) { if (!disposed && run.value?.id === identity && sopDirectories[item.key] === entry) entry.error = e instanceof Error ? e.message : '工单目录读取失败，请重试'; }
  finally { entry.loading = false; }
}
function changes(): Dict[] {
  if (!run.value || !original || !editable.value) return [];
  const originals = new Map<string, Dict>((original.items || []).map((item: Dict) => [item.key, item]));
  return (run.value.items || []).flatMap((item: Dict) => {
    const old = originals.get(item.key); if (!old) return [];
    const draft: Dict = {};
    for (const field of item.fields || []) {
      if (field.readonly) continue;
      const value = item.draft?.[field.key], previous = old.draft?.[field.key];
      if (field.kind === 'notice_sop') {
        if (JSON.stringify(value) !== JSON.stringify(previous)) draft[field.key] = JSON.parse(JSON.stringify(value));
      } else if (String(value ?? '') !== String(previous ?? '')) draft[field.key] = String(value ?? '');
    }
    return item.selected !== old.selected || !!item.edit_all !== !!old.edit_all || Object.keys(draft).length ? [{ key: item.key, selected: !!item.selected, edit_all: !!item.edit_all, draft }] : [];
  });
}
function localKey(id: string): string { return `notice-panel:${props.userId}:${id}`; }
function saveLocal(): void {
  if (!run.value || !props.userId || !editable.value) return;
  try { const items = changes(); if (items.length) window.sessionStorage.setItem(localKey(run.value.id), JSON.stringify({ revision: run.value.revision, changes: items })); else window.sessionStorage.removeItem(localKey(run.value.id)); }
  catch { draftWarning.value = '浏览器未能暂存，请先保存草稿再离开。'; }
}
function receive(value: Dict, restore = false): void {
  if (disposed) return;
  if (run.value?.id !== value.id) {
    for (const key in sopDirectories) delete sopDirectories[key];
    noticeView.value = value.slot === 'evening' ? 'ongoing' : 'planned';
    try {
      const saved = JSON.parse(window.sessionStorage.getItem(`notice-panel-view:${props.userId}`) || 'null');
      if (saved?.scope === value.scope && saved?.slot === value.slot && saved?.date === value.date && ['planned', 'ongoing'].includes(saved.view)) noticeView.value = saved.view;
    } catch { /* Ignore invalid navigation memory. */ }
  }
  run.value = structuredClone(value); original = structuredClone(value); page.value = Math.min(page.value, pages.value);
  slot.value = value.slot;
  rememberView();
  if (restore && value.state === 'edit') {
    try {
      const saved = JSON.parse(window.sessionStorage.getItem(localKey(value.id)) || 'null');
      if (saved?.revision === value.revision) for (const update of saved.changes || []) {
        const item = run.value!.items.find((row: Dict) => row.key === update.key); if (!item || item.blocked) continue;
        if (!item.brief_fields?.length) item.brief_fields = briefKeys(item); item.selected = !!update.selected; item.edit_all = !!update.edit_all;
        for (const field of item.fields || []) {
          const savedValue = update.draft?.[field.key];
          if (!field.readonly && (typeof savedValue === 'string' || field.kind === 'notice_sop' && savedValue && typeof savedValue === 'object' && !Array.isArray(savedValue))) item.draft[field.key] = savedValue;
        }
        if (item.selected) expanded[item.key] = true;
      }
      else if (saved) draftWarning.value = '服务器草稿已有新版本；浏览器中的旧填写仍保留，未覆盖服务器内容。';
    } catch { /* A malformed local draft must not alter the server document. */ }
    if (!Object.values(expanded).some(Boolean)) {
      const first = run.value!.items.find((item: Dict) => item.selected);
      if (first) expanded[first.key] = true;
    }
  }
  if (value.state === 'edit') for (const item of run.value!.items || []) if (!inCurrentView(item)) item.selected = false;
}
async function loadCurrent(latest = false): Promise<void> {
  if (!scope.value || !props.active || !meta.value.edition || busy.value) return;
  saveLocal(); controller?.abort(); const pending = new AbortController(); controller = pending; const epoch = ++serial;
  loading.value = true; error.value = ''; conflict.value = false; draftWarning.value = ''; search.value = ''; page.value = 1;
  if (latest || !slot.value || run.value?.date !== meta.value.edition.date && run.value) slot.value = meta.value.edition.slot;
  if (slot.value === meta.value.edition.slot) editionPending = false;
  try { const result = await requestJson(base + '/open', { method: 'POST', body: JSON.stringify({ scope: scope.value, slot: slot.value }), signal: pending.signal, timeoutMs: 15000 }); if (!disposed && serial === epoch) receive(result.run, true); }
  catch (e) { if (!pending.signal.aborted && !disposed && serial === epoch) error.value = e instanceof Error ? e.message : '待办读取失败'; }
  finally { if (!disposed && serial === epoch) { loading.value = false; schedule(); } }
}
async function changeScope(): Promise<void> { saveLocal(); run.value = null; original = null; draftWarning.value = ''; await loadCurrent(); }
async function refreshRows(): Promise<void> {
  if ((dirty.value || selected.value.length) && !window.confirm('重新读取将清除本批尚未提交的填写和选择，是否继续？')) return;
  await act('refresh');
}
async function readLatest(): Promise<void> {
  if (!run.value || busy.value || !window.confirm('读取最新草稿会替换当前尚未提交的填写，是否继续？')) return;
  busy.value = true;
  try { const result = await requestJson(base + '/' + run.value.id, { timeoutMs: 15000 }); receive(result.run); conflict.value = false; error.value = ''; draftWarning.value = ''; saveLocal(); }
  catch (e) { error.value = e instanceof Error ? e.message : '读取未完成'; }
  finally { busy.value = false; schedule(); }
}
async function act(action: string): Promise<void> {
  if (!run.value || busy.value) return;
  if(action === 'confirm_unmatched' && !window.confirm('以下通告尚未匹配计划收敛，是否仍要发送？\n\n'+selected.value.filter(item=>item.convergence_confirmation_required).map(item=>item.title).join('\n')))return;
  saveLocal(); const id = run.value.id; busy.value = true; error.value = ''; window.clearTimeout(timer);
  try {
    const body: Dict = { action, revision: run.value.revision };
    if (action === 'save' || action === 'preview') body.changes = changes();
    const result = await requestJson(base + '/' + id + '/action', { method: 'POST', body: JSON.stringify(body), timeoutMs: 15000 });
    if (disposed) return;
    receive(result.run);
    if (action === 'preview' && result.run.state === 'confirm') { search.value = ''; page.value = 1; }
    conflict.value = false; draftWarning.value = '';
    try { window.sessionStorage.removeItem(localKey(id)); } catch { /* Server draft is already saved. */ }
    for (const item of result.run.items || []) if (item.error) expanded[item.key] = true;
    if (['confirm', 'retry', 'confirm_unmatched'].includes(action)) window.dispatchEvent(new Event('clipflow-business-changed'));
  } catch (e) {
    if (!disposed) error.value = e instanceof Error ? e.message : '操作未完成，填写已保留';
    if (e instanceof ApiError && e.status === 409) conflict.value = true;
    if (['confirm', 'retry', 'confirm_unmatched'].includes(action)) {
      try { const result = await requestJson(base + '/' + id, { timeoutMs: 15000 }); if (!disposed) receive(result.run, true); } catch { /* Keep the original operation for retry. */ }
    }
  } finally { busy.value = false; if (!disposed) schedule(); }
}
async function poll(): Promise<void> {
  if (polling) return;
  if (disposed || document.hidden || busy.value || loading.value) { schedule(); return; }
  polling = true;
  try {
    const next = await requestJson(base, { timeoutMs: 15000 }); if (disposed) return;
    if (next.edition?.id && next.edition.id !== meta.value.edition?.id) editionPending = true;
    meta.value = next;
    emit('availability', !!meta.value.edition || !!run.value);
    if (meta.value.edition?.id) emit('edition', meta.value.edition.id);
    if (scope.value && !meta.value.scopes?.some((item: Dict) => item.value === scope.value)) { saveLocal(); scope.value = ''; run.value = null; original = null; }
    if (!scope.value) {
      try { const saved = JSON.parse(window.sessionStorage.getItem(`notice-panel-view:${props.userId}`) || 'null'); if (saved && meta.value.scopes?.some((item: Dict) => item.value === saved.scope)) scope.value = saved.scope; } catch { /* Ignore an invalid navigation preference. */ }
      if (!scope.value && meta.value.scopes?.length === 1) scope.value = meta.value.scopes[0].value;
    }
    if (props.active && scope.value && meta.value.edition && !running.value && (editionPending || !run.value)) { editionPending = false; await loadCurrent(true); return; }
    if (run.value && props.active && (preparing.value || running.value)) { const result = await requestJson(base + '/' + run.value.id, { timeoutMs: 15000 }); if (!disposed) { const wasRunning = running.value; receive(result.run); if (wasRunning && !running.value) window.dispatchEvent(new Event('clipflow-business-changed')); } }
    if (editionPending && props.active && scope.value && !running.value) { editionPending = false; await loadCurrent(true); }
  } catch (e) { if (!disposed) { error.value = e instanceof Error ? e.message : '待办读取失败'; if (e instanceof ApiError && [401, 403].includes(e.status)) { run.value = null; original = null; scope.value = ''; } } }
  finally { polling = false; schedule(); }
}
function schedule(): void {
  window.clearTimeout(timer);
  if (disposed || document.hidden) return;
  const boundary = Number(meta.value.next_at || 0) * 1000 - Date.now();
  const interval = props.active && (preparing.value || running.value) ? 2200 : 30000;
  timer = window.setTimeout(poll, boundary > 0 ? Math.min(interval, Math.max(250, boundary + 100)) : interval);
}
function visibility(): void { if (document.hidden) window.clearTimeout(timer); else void poll(); }
watch(() => props.active, value => { if (value) void poll(); });
onMounted(() => { document.addEventListener('visibilitychange', visibility); void poll(); });
onBeforeUnmount(() => { saveLocal(); disposed = true; ++serial; controller?.abort(); window.clearTimeout(timer); window.clearTimeout(saveTimer); document.removeEventListener('visibilitychange', visibility); });
</script>

<style scoped>
.notice-panel {
  width: var(--notice-panel-width, 360px); height: var(--notice-panel-height, 700px); max-height: calc(100dvh - 48px);
  display: flex; flex-direction: column; flex: none; min-width: 0; overflow: hidden;
  border: 1px solid var(--lh-border, #d3dce7); border-radius: var(--lh-panel-radius, 18px);
  background: var(--lh-surface, #f9fbfe); color: var(--lh-charcoal, #233347); box-shadow: var(--lh-shadow, 0 18px 48px #10182038);
}
.notice-panel * { box-sizing: border-box; }
.notice-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; min-height: 68px; padding: 12px 14px; border-bottom: 1px solid var(--lh-border); }
.notice-heading { display: flex; align-items: center; min-width: 0; gap: 10px; }
.notice-heading > div { min-width: 0; }
.notice-mark { display: grid; place-items: center; width: 34px; height: 34px; flex: none; border-radius: 10px; border: 1px solid var(--lh-border); background: var(--lh-surface-subtle); color: var(--lh-accent); }
h3 { margin: 0 0 3px; font-size: 15px; line-height: 1.35; font-weight: 650; color: var(--lh-charcoal-strong); }
small { font-size: 11px; line-height: 1.5; color: var(--lh-muted); }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 34px; padding: 7px 11px; border: 1px solid var(--lh-border); border-radius: var(--lh-control-radius, 8px); background: var(--lh-surface-subtle); color: var(--lh-charcoal); font: inherit; font-size: 12px; cursor: pointer; transition: background 160ms ease, border-color 160ms ease, color 160ms ease; }
button:hover:not(:disabled) { background: var(--lh-surface-hover); border-color: var(--lh-border-strong); }
button:disabled { opacity: .45; cursor: not-allowed; }
button.icon { width: 32px; height: 32px; min-height: 32px; padding: 0; flex: none; border-color: transparent; background: transparent; color: var(--lh-muted); }
button.icon:hover:not(:disabled) { color: var(--lh-charcoal-strong); }
button.primary { min-height: 36px; background: var(--lh-action, #1761c1); border-color: var(--lh-action, #1761c1); color: #fff; font-weight: 600; }
button.primary:hover:not(:disabled) { background: var(--lh-action-hover, #0c4fa5); border-color: var(--lh-action-hover); }
button.primary:disabled { opacity: 1; background: var(--lh-surface-hover); border-color: var(--lh-border); color: var(--lh-faint-muted); }
button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible { outline: 2px solid var(--lh-accent, #588ed0); outline-offset: 2px; }
.notice-filters { padding: 10px 14px; display: flex; flex-wrap: wrap; gap: 8px 12px; }
.notice-filters label { display: flex; align-items: center; gap: 7px; min-width: 0; font-size: 12px; color: var(--lh-muted); }
.notice-select { position: relative; display: flex; min-width: 0; }
.notice-select select { appearance: none; max-width: 148px; padding-right: 26px; }
.notice-select > svg { pointer-events: none; position: absolute; right: 8px; top: 50%; transform: translateY(-50%); }
.notice-filters .notice-search { flex: 1 1 100%; min-height: 34px; padding: 0 10px; border: 1px solid var(--lh-border); border-radius: var(--lh-control-radius, 8px); background: var(--lh-surface-subtle); transition: border-color 160ms ease; }
.notice-search > svg { flex: none; }
.notice-search:focus-within { border-color: var(--lh-accent); }
.notice-filters .notice-search input { width: 100%; min-width: 0; border: 0; background: transparent; padding: 5px 0; outline: 0; box-shadow: none; }
input:not([type=checkbox]), select, textarea { min-height: 34px; max-width: 100%; padding: 7px 9px; border: 1px solid var(--lh-input-border, #b8c7d9); border-radius: var(--lh-control-radius, 8px); background: var(--lh-surface-subtle, #fff); color: var(--lh-charcoal); font: inherit; font-size: 13px; }
input::placeholder, textarea::placeholder { color: var(--lh-faint-muted); }
input[type=checkbox] { accent-color: var(--lh-action, #1761c1); width: 17px; height: 17px; min-width: 17px; margin: 3px 0 0; cursor: pointer; }
input:disabled { cursor: not-allowed; }
.notice-count { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex: none; padding: 7px 14px; font-size: 11px; font-variant-numeric: tabular-nums; border-block: 1px solid var(--lh-border); color: var(--lh-muted); }
.notice-count > span { display: inline-flex; align-items: center; gap: 8px; }
.notice-tabs { display: flex; flex: none; padding: 0 14px; gap: 16px; }
.notice-tabs button { flex: 1; gap: 5px; border: 0; border-bottom: 2px solid transparent; border-radius: 0; padding: 8px 0; min-width: 0; background: transparent; color: var(--lh-muted); white-space: nowrap; }
.notice-tabs button[aria-selected=true] { border-bottom-color: var(--lh-accent); color: var(--lh-charcoal-strong); font-weight: 600; }
.notice-tabs button > span { font-size: 11px; font-variant-numeric: tabular-nums; color: var(--lh-muted); }
.notice-page-enter-active, .notice-page-leave-active { transition: opacity 120ms ease; }
.notice-page-enter-from, .notice-page-leave-to { opacity: 0; }
.notice-stage { font-size: 12px; font-weight: 600; color: var(--lh-charcoal); }
.selection-count { padding-left: 8px; border-left: 1px solid var(--lh-border-strong); }
.notice-content { flex: 1; overflow-y: auto; min-height: 0; padding: 0 0 8px; overscroll-behavior: contain; scrollbar-width: thin; scrollbar-color: var(--lh-border-strong) transparent; }
.notice-empty { min-height: 160px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; padding: 20px; font-size: 13px; line-height: 1.6; color: var(--lh-muted); text-align: center; }
.notice-empty > svg { color: var(--lh-accent); }
.notice-row { padding: 9px 14px; border-bottom: 1px solid var(--lh-border); overflow-wrap: anywhere; transition: background 160ms ease, box-shadow 160ms ease; }
.notice-row:not(.blocked):hover { background: color-mix(in srgb, var(--lh-surface-subtle) 65%, transparent); }
.notice-row.selected { background: var(--lh-surface-subtle); box-shadow: inset 3px 0 var(--lh-action); }
.notice-row.blocked .notice-title { color: var(--lh-muted); }
.row-heading { display: flex; align-items: flex-start; gap: 9px; }
.row-heading > .icon { width: 26px; height: 26px; min-height: 26px; }
.notice-title { flex: 1; min-width: 0; font-size: 14px; line-height: 1.5; font-weight: 650; color: var(--lh-charcoal-strong); cursor: pointer; }
.notice-details { display: flex; flex-wrap: wrap; align-items: center; gap: 3px 10px; margin: 4px 0 0 26px; }
.notice-meta { display: flex; align-items: center; flex-wrap: wrap; gap: 3px 7px; font-size: 11px; line-height: 1.5; color: var(--lh-muted); }
.notice-kind { color: var(--lh-charcoal); }
.notice-kind + span::before { content: ''; display: inline-block; width: 3px; height: 3px; margin: 0 8px 3px 0; border-radius: 50%; background: var(--lh-muted); }
.notice-operation { color: var(--lh-accent); }
.notice-operation.finishing { color: var(--lh-warn); }
.notice-window { display: flex; align-items: flex-start; gap: 4px; }
.notice-window > svg { flex: none; margin-top: 2px; }
.row-mode { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 6px; margin: 8px 0 6px; padding-top: 6px; border-top: 1px solid var(--lh-border); font-size: 12px; color: var(--lh-muted); }
.row-mode button { min-height: 28px; padding: 4px 6px; border-color: transparent; background: transparent; color: var(--lh-accent-strong); }
.notice-fields { display: grid; gap: 9px; padding: 4px 4px 5px; }
.notice-fields-wrap { display: grid; grid-template-rows: 1fr; }
.notice-fields-wrap > .notice-fields { min-height: 0; overflow: hidden; }
.notice-collapse-enter-active, .notice-collapse-leave-active { transition: grid-template-rows 180ms ease, opacity 180ms ease; }
.notice-collapse-enter-from, .notice-collapse-leave-to { grid-template-rows: 0fr; opacity: 0; }
.notice-field { display: flex; flex-direction: column; gap: 6px; min-width: 0; font-size: 12px; }
.notice-field > label, .notice-field > span:first-child { color: var(--lh-muted); }
.notice-field em { color: var(--lh-danger); font-style: normal; }
.notice-field textarea { resize: vertical; min-height: 38px; max-height: 110px; line-height: 1.6; }
.notice-field input, .notice-field textarea, .notice-field select { background: var(--lh-surface); }
.notice-field :deep(.repair-date-control input) { min-height: 34px; padding-right: 38px; background: var(--lh-surface); }
.notice-field :deep(.repair-field-label) { font-size: 12px; }
.notice-field :deep(.lhs-people-cols) { grid-template-columns: minmax(0, 1fr); }
.readonly { white-space: pre-wrap; font-size: 13px; line-height: 1.6; color: var(--lh-charcoal); }
.notice-error, .notice-warning { font-size: 12px; line-height: 1.6; margin: 8px 14px; overflow-wrap: anywhere; }
.notice-error { color: var(--lh-danger, #b32d35); }.notice-warning { color: var(--lh-warn, #916018); }
.notice-row .notice-error, .notice-row .notice-warning { margin: 8px 0; padding-left: 9px; border-left: 2px solid currentColor; }
.notice-preview { white-space: pre-wrap; overflow-wrap: anywhere; font: inherit; font-size: 13px; line-height: 1.75; margin: 12px 0 0; padding: 2px 0 2px 12px; border-left: 2px solid var(--lh-accent); }
.notice-result { display: flex; align-items: center; gap: 6px; font-size: 12px; line-height: 1.6; color: var(--lh-muted); }
.notice-result.success { color: #80d3b0; }.notice-result.failed { color: var(--lh-danger); }
.notice-footer { flex: none; padding: 7px 14px 10px; border-top: 1px solid var(--lh-border); background: var(--lh-surface); display: flex; flex-direction: column; align-items: stretch; gap: 5px; }
.notice-actions, .notice-pages { display: flex; align-items: center; gap: 8px; }
.notice-actions { justify-content: flex-end; flex-wrap: wrap; }
.notice-actions > .primary { flex: 1; }
.notice-actions > .icon { border-color: var(--lh-border); }
.notice-pages { justify-content: center; font-size: 12px; font-variant-numeric: tabular-nums; color: var(--lh-muted); }
.notice-pages > span { min-width: 50px; text-align: center; }
.notice-pages > .icon { width: 28px; height: 28px; min-height: 28px; }
.notice-footer > .notice-meta { margin: 0; justify-content: center; }
.conflict-refresh { margin: 0 14px 8px; align-self: flex-start; }
.spin { animation: notice-spin .9s linear infinite; }@keyframes notice-spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .spin { animation-duration: 1.8s; } button, .notice-row, .notice-search, .notice-collapse-enter-active, .notice-collapse-leave-active, .notice-page-enter-active, .notice-page-leave-active { transition: none; } }
</style>
