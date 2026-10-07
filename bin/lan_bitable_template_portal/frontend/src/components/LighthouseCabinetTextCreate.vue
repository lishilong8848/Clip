<template>
  <section class="lh-text-create" aria-label="新建粘贴文本机柜待办">
    <div class="paste-bar">
      <textarea :id="pasteInputId" ref="inputRef" v-model="text" rows="2" :disabled="busy || disabled" placeholder="粘贴机柜确认表格或文本…" aria-label="粘贴机柜确认文本" @paste="onPaste"></textarea>
      <button type="button" :disabled="busy || disabled || !text.trim()" @click="recognizeNew"><ScanText :size="16" />识别当前文本</button>
    </div>
    <div v-if="busy" class="feedback" role="status"><Loader2 :size="15" class="spin" />识别中，请稍候…</div>
    <div v-if="error" class="feedback error" role="alert">{{ error }}</div>

    <div class="toolbar">
      <div class="source-picker">
        <VnetSelect :input-id="id + '-source'" label="筛选粘贴来源" :options="sourceLabels" :model-value="sourceCurrent" :disabled="busy || disabled" placeholder="全部粘贴" :menu-z-index="10030" @update:model-value="pickSource" />
        <button v-if="sourceFilter" type="button" class="icon-button" :disabled="busy || disabled" title="删除所选粘贴及其全部记录" :aria-label="'删除所选粘贴 ' + sourceFilterLabel" @click="removeSource"><Trash2 :size="15" /></button>
      </div>
      <label class="search"><Search :size="15" /><input v-model="search" aria-label="搜索粘贴记录" placeholder="搜索包间、机柜或操作" :disabled="busy || disabled" @input="page = 1" /></label>
      <span class="count" role="status">当前 {{ filtered.length }} 条 / 全部 {{ rows.length }} 条</span>
      <button v-if="pendingSources.length" type="button" class="ghost" :disabled="busy || disabled" title="重新核对已粘贴文本" @click="refresh"><RefreshCw :size="14" />重新核对</button>
    </div>

    <div v-if="filtered.length" class="table-scroll">
      <table class="rows-table">
        <thead>
          <tr>
            <th>来源</th><th>楼栋</th><th>包间</th><th class="rack-head">机柜</th><th>操作类型</th>
            <th>期望完成</th><th>实际完成</th><th>结果</th>
            <th v-if="hasSupplier">运营商机柜</th><th>机柜类型</th>
            <th v-if="hasTypeDetail">类型明细</th><th>状态</th><th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in pagedRows" :key="keyOf(row)" :class="{ duplicate: duplicateRows.has(keyOf(row)) }">
            <td class="source-cell">{{ sourceIndex(row) }}</td>
            <td>{{ row.scope }}楼</td>
            <td>{{ row.room }}</td>
            <td class="rack-cell"><small>{{ row.scope }}楼 / {{ row.room }}</small><strong>{{ row.rack || '—' }}</strong></td>
            <td>
              <select :value="row.action" :disabled="busy || disabled" :aria-label="'操作类型 ' + row.rack" @change="editAction(row, $event)">
                <option value="">请选择</option>
                <option v-for="action in actions" :key="action">{{ action }}</option>
              </select>
            </td>
            <td><input :value="localDate(row.expected)" :disabled="busy || disabled" type="datetime-local" step="1" :aria-label="'期望完成时间 ' + row.rack" @input="editDate(row, 'expected', $event)" /></td>
            <td><input :value="localDate(row.actual)" :disabled="busy || disabled" type="datetime-local" step="1" :aria-label="'实际完成时间 ' + row.rack" @input="editDate(row, 'actual', $event)" /></td>
            <td class="result-cell">
              <select :value="row.result" :disabled="busy || disabled" :aria-label="'结果 ' + row.rack" @change="editResult(row, $event)">
                <option value="">待核对</option><option>成功</option><option>失败</option>
              </select>
              <input v-if="row.result === '失败'" v-model="row.failure_reason" :disabled="busy || disabled" required maxlength="1000" placeholder="失败原因" :aria-label="'失败原因 ' + row.rack" @input="markEdited(row)" />
            </td>
            <td v-if="hasSupplier">{{ row.supplier_rack || '—' }}</td>
            <td>{{ row.rack_type || '待核对' }}</td>
            <td v-if="hasTypeDetail">{{ row.type_detail || '—' }}</td>
            <td class="status-cell" :class="{ duplicate: duplicateRows.has(keyOf(row)) }">{{ rowMessage(row) }}</td>
            <td><button type="button" class="icon-button" :disabled="busy || disabled" title="移除记录" :aria-label="'移除记录 ' + row.rack" @click="removeRow(row)"><Trash2 :size="15" /></button></td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-else class="empty">{{ busy ? '正在识别粘贴内容…' : rows.length ? '没有符合筛选条件的记录' : '暂无粘贴记录' }}</p>

    <footer v-if="pages > 1" class="pagination">
      <span>每页 25 条</span>
      <button type="button" :disabled="page <= 1 || busy || disabled" aria-label="上一页" @click="page--"><ChevronLeft :size="16" /></button>
      <span>{{ page }} / {{ pages }}</span>
      <button type="button" :disabled="page >= pages || busy || disabled" aria-label="下一页" @click="page++"><ChevronRight :size="16" /></button>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ChevronLeft, ChevronRight, Loader2, RefreshCw, ScanText, Search, Trash2 } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { randomHexId } from '../browserStorage';
import VnetSelect from './VnetSelect.vue';

interface Source { id: string; text: string; $query?: Dict }
interface Row extends Record<string, any> {
  text_id: string;
  text_row: number;
  scope: string;
  room: string;
  rack: string;
  action: string;
  expected: string;
  actual: string;
  result: string;
  failure_reason: string;
  supplier_rack: string;
  rack_type: string;
  type_detail: string;
  type_resolution: string;
}

const props = defineProps<{
  field: Record<string, any>;
  modelValue?: any;
  disabled?: boolean;
  id: string;
  planId?: string;
  planVersion?: number;
}>();
const emit = defineEmits<{ 'update:modelValue': [value: Dict] }>();

const ROW_FIELDS = ['scope', 'room', 'rack', 'action', 'expected', 'actual', 'result', 'failure_reason', 'supplier_rack', 'rack_type', 'type_detail', 'type_resolution'] as const;

const pasteInputId = computed(() => `${props.id}-text-create-input`);
const actions = computed<string[]>(() => Array.isArray(props.field?.actions) ? props.field.actions.map(String) : []);

const inputRef = ref<HTMLTextAreaElement>();
const text = ref('');
const sources = ref<Source[]>([]);
const rows = ref<Row[]>([]);
const busy = ref(false);
const error = ref('');
const page = ref(1);
const search = ref('');
const sourceFilter = ref('');
let disposed = false;
let generation = 0;

const keyOf = (row: { text_id?: unknown; text_row?: unknown }) => `${row.text_id ?? ''}:${row.text_row ?? 0}`;
const localDate = (value: string) => String(value || '').replace(' ', 'T');
const inputDate = (event: Event) => (event.target as HTMLInputElement).value.replace('T', ' ');

function normalizeRow(raw: Dict): Row {
  return {
    text_id: String(raw?.text_id ?? ''),
    text_row: Number(raw?.text_row ?? 0),
    scope: String(raw?.scope ?? ''),
    room: String(raw?.room ?? ''),
    rack: String(raw?.rack ?? ''),
    action: String(raw?.action ?? ''),
    expected: String(raw?.expected ?? ''),
    actual: String(raw?.actual ?? ''),
    result: String(raw?.result ?? ''),
    failure_reason: String(raw?.failure_reason ?? ''),
    supplier_rack: String(raw?.supplier_rack ?? ''),
    rack_type: String(raw?.rack_type ?? ''),
    type_detail: String(raw?.type_detail ?? ''),
    type_resolution: String(raw?.type_resolution ?? ''),
    issues: Array.isArray(raw?.issues) ? raw.issues : [],
    _edited: false,
  };
}
function normalizeModel(value: Dict): { sources: Source[]; rows: Row[] } {
  const raw = value && typeof value === 'object' ? value : {};
  const sources = Array.isArray(raw.sources) ? raw.sources
    .map((item: Dict) => ({ ...(item?.$query ? { $query: item.$query } : {}), id: String(item?.id ?? ''), text: String(item?.text ?? '') }))
    .filter((item: Source) => Boolean(item.id)) : [];
  const rows = Array.isArray(raw.rows) ? raw.rows.filter((item: Dict) => item && typeof item === 'object').map((item: Dict) => normalizeRow(item)) : [];
  return { sources, rows };
}
function serializeRow(row: Row): Dict {
  const out: Dict = { text_id: row.text_id, text_row: row.text_row };
  for (const field of ROW_FIELDS) out[field] = row[field];
  return out;
}
function serializeValue(): Dict {
  return { sources: sources.value, rows: busy.value ? [] : rows.value.map(serializeRow) };
}

const duplicateRows = computed(() => {
  const seen = new Set<string>(), duplicates = new Set<string>();
  for (const row of rows.value) {
    const key = [row.scope, row.room, row.rack, row.action, row.actual].join('|');
    if (row.actual && seen.has(key)) duplicates.add(keyOf(row));
    seen.add(key);
  }
  return duplicates;
});
const hasSupplier = computed(() => rows.value.some(row => Boolean(row.supplier_rack)));
const hasTypeDetail = computed(() => rows.value.some(row => Boolean(row.type_detail)));
const filtered = computed(() => {
  const query = search.value.trim().toUpperCase();
  return rows.value.filter(row =>
    (!sourceFilter.value || row.text_id === sourceFilter.value) &&
    (!query || `${row.scope} ${row.room} ${row.rack} ${row.supplier_rack} ${row.action}`.toUpperCase().includes(query))
  );
});
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / 25)));
const pagedRows = computed(() => filtered.value.slice((page.value - 1) * 25, page.value * 25));
watch(pages, value => { page.value = Math.min(page.value, value); });

const sourceOptions = computed(() => [
  { id: '', label: `全部粘贴（${sources.value.length} 次）` },
  ...sources.value.map((source, index) => {
    const count = rows.value.filter(row => row.text_id === source.id).length;
    const first = rows.value.find(row => row.text_id === source.id);
    return { id: source.id, label: first ? `第 ${index + 1} 次 · ${count} 条 · ${first.scope}楼 ${first.room}` : `第 ${index + 1} 次 · 0 条` };
  }),
]);
const sourceLabels = computed(() => sourceOptions.value.map(option => option.label));
const sourceCurrent = computed(() => sourceOptions.value.find(option => option.id === sourceFilter.value)?.label || sourceOptions.value[0]?.label || '');
const sourceFilterLabel = computed(() => sourceOptions.value.find(option => option.id === sourceFilter.value)?.label || sourceFilter.value);
function sourceIndex(row: Row): number {
  const index = sources.value.findIndex(source => source.id === row.text_id);
  return index >= 0 ? index + 1 : 0;
}
function pickSource(label: string): void {
  const matched = sourceOptions.value.find(option => option.label === label);
  sourceFilter.value = matched ? matched.id : '';
  page.value = 1;
}

function rowMessage(row: Row): string {
  if (duplicateRows.value.has(keyOf(row))) return '与前面重复';
  if (row.result === '失败' && !row.failure_reason) return '需填写失败原因';
  if (row._edited) return '已更正，创建时复核';
  const messages = (row.issues || []).map((issue: Dict) => String(issue?.message || '')).filter(Boolean);
  return messages.length ? messages.join('；') : '已识别';
}

function markEdited(row: Row): void { row._edited = true; }
function editAction(row: Row, event: Event): void { row.action = (event.target as HTMLSelectElement).value; markEdited(row); }
function editDate(row: Row, key: 'expected' | 'actual', event: Event): void { row[key] = inputDate(event); markEdited(row); }
function editResult(row: Row, event: Event): void { row.result = (event.target as HTMLSelectElement).value; markEdited(row); }

function rowHasSource(sid: string): boolean {
  return rows.value.some(row => String(row.text_id) === sid);
}
const pendingSources = computed(() => sources.value.filter(source => !rowHasSource(source.id)));

function removeRow(row: Row): void {
  const sid = row.text_id;
  rows.value = rows.value.filter(item => item !== row);
  if (!rowHasSource(sid)) {
    sources.value = sources.value.filter(source => source.id !== sid);
    if (sourceFilter.value === sid) sourceFilter.value = '';
  }
  page.value = Math.min(page.value, pages.value);
}
function removeSource(): void {
  const id = sourceFilter.value;
  if (!id) return;
  rows.value = rows.value.filter(row => row.text_id !== id);
  sources.value = sources.value.filter(source => source.id !== id);
  sourceFilter.value = '';
  page.value = Math.min(page.value, pages.value);
}

/**
 * 合并后端返回的识别行：只有尚未物化（当前没有已保留记录）的来源才会被引入。
 * 已有保留记录（materialized）的来源是明确保留集，绝不重新引入其被删行，也不覆盖原编辑。
 */
function applyIncoming(incoming: Dict[]): void {
  const materialized = new Set<string>();
  for (const row of rows.value) materialized.add(String(row.text_id));
  const added = new Set<string>();
  const additions: Row[] = [];
  for (const item of incoming || []) {
    const sid = String(item?.text_id ?? '');
    if (!sid || materialized.has(sid)) continue;
    const key = keyOf(item);
    if (!key || added.has(key) || rows.value.some(existing => keyOf(existing) === key)) continue;
    added.add(key);
    additions.push(normalizeRow(item));
  }
  if (!additions.length) return;
  rows.value = rows.value.concat(additions);
  page.value = Math.min(page.value, pages.value);
}

function preview(nextSources: Source[]): Promise<Dict> {
  return requestJson(`/api/assistant/plans/${props.planId}/cabinet-text-preview`, {
    method: 'POST',
    body: JSON.stringify({ version: props.planVersion, field: props.field?.name, sources: nextSources }),
    timeoutMs: 90000,
  });
}

async function runPreview(nextSources: Source[]): Promise<boolean> {
  const gen = generation;
  busy.value = true;
  error.value = '';
  try {
    const result = await preview(nextSources);
    if (disposed || gen !== generation) return false;
    applyIncoming(result.rows || []);
    return true;
  } catch (exc: any) {
    if (disposed || gen !== generation) return false;
    error.value = exc.message || '文本识别失败，请重新粘贴';
    return false;
  } finally {
    if (!disposed && gen === generation) busy.value = false;
  }
}

function onPaste(event: ClipboardEvent): void {
  if (busy.value || props.disabled) return;
  const value = event.clipboardData?.getData('text/plain');
  if (value?.trim()) { event.preventDefault(); text.value = value; void recognizeNew(); }
}

async function recognizeNew(): Promise<void> {
  if (busy.value || props.disabled || !text.value.trim()) return;
  const source: Source = { id: 'src_' + randomHexId(), text: text.value };
  const ok = await runPreview([source]);
  if (!ok || disposed) return;
  sources.value = [...sources.value, source];
  text.value = '';
  page.value = 1;
  await nextTick();
  if (!disposed) inputRef.value?.focus();
}

async function refresh(): Promise<void> {
  if (busy.value || props.disabled || !pendingSources.value.length) return;
  await runPreview(pendingSources.value);
}

function adoptExternal(value: any): void {
  generation++;
  const normalized = normalizeModel(value);
  sources.value = normalized.sources;
  rows.value = normalized.rows;
  busy.value = false;
  search.value = '';
  sourceFilter.value = '';
  page.value = 1;
  error.value = '';
}

const initial = normalizeModel(props.modelValue);
sources.value = initial.sources;
rows.value = initial.rows;
let lastEmitted = JSON.stringify(serializeValue());
let lastBusyEmit = false;
let lastNormalized = JSON.stringify(normalizeModel(serializeValue()));

watch([sources, rows, busy], () => {
  const value = serializeValue();
  const snapshot = JSON.stringify(value);
  if (snapshot === lastEmitted && busy.value === lastBusyEmit) return;
  lastEmitted = snapshot;
  lastBusyEmit = busy.value;
  lastNormalized = JSON.stringify(normalizeModel(value));
  emit('update:modelValue', value);
}, { deep: true });

watch(() => props.modelValue, (value) => {
  const external = JSON.stringify(normalizeModel(value));
  if (external === lastNormalized) return;
  adoptExternal(value);
}, { deep: true });

onMounted(async () => {
  if (props.disabled) return;
  if (pendingSources.value.length) await runPreview(pendingSources.value);
});

onBeforeUnmount(() => { disposed = true; generation++; });
</script>

<style scoped>
.lh-text-create {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
  color: var(--lh-charcoal, #203650);
  font: 13px/1.5 "Microsoft YaHei", sans-serif;
  letter-spacing: 0;
}
.paste-bar { display: flex; align-items: stretch; gap: 8px; flex-wrap: wrap; }
.paste-bar textarea { width: 100%; min-width: 0; min-height: 54px; box-sizing: border-box; padding: 8px 10px; resize: vertical; border: 1px solid var(--lh-input-border, #ccd9e8); border-radius: 6px; background: var(--lh-surface, #fff); color: inherit; font: inherit; }
.lh-text-create button { box-sizing: border-box; display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 38px; padding: 7px 12px; border: 1px solid var(--lh-input-border, #ccd9e8); border-radius: 6px; background: var(--lh-surface, #fff); color: var(--lh-charcoal, #203650); cursor: pointer; font: inherit; }
.lh-text-create button:hover:not(:disabled) { background: var(--lh-surface-hover, #edf5ff); }
.lh-text-create button:disabled { opacity: .5; cursor: not-allowed; }
.lh-text-create .icon-button { width: 38px; flex: none; padding: 0; }
.lh-text-create .ghost { min-height: 32px; padding: 4px 10px; color: var(--lh-muted, #4a6a8c); font-size: 12px; }
.lh-text-create input, .lh-text-create select { min-height: 38px; box-sizing: border-box; padding: 6px 8px; border: 1px solid var(--lh-input-border, #ccd9e8); border-radius: 5px; background: var(--lh-surface, #fff); color: inherit; font: inherit; }
.lh-text-create :focus-visible { outline: 2px solid var(--lh-accent-ring, #2186c4); outline-offset: 2px; }
.feedback { display: flex; align-items: center; gap: 8px; overflow-wrap: anywhere; color: var(--lh-muted, #59738c); font-size: 12px; }
.feedback.error { color: var(--lh-danger, #ad3434); }
.feedback.error button { min-height: 30px; padding: 3px 10px; font-size: 12px; }
.toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.toolbar .source-picker { display: flex; align-items: center; gap: 6px; }
.toolbar .count { margin-left: auto; color: var(--lh-muted, #60758c); font-size: 12px; white-space: nowrap; }
.toolbar .search { display: inline-flex; align-items: center; gap: 6px; color: var(--lh-muted, #60758c); }
.toolbar .search input { width: min(260px, 52vw); }
.table-scroll { overflow: auto; border: 1px solid var(--lh-border, #dce6f1); border-radius: 6px; background: var(--lh-surface, #fff); }
.rows-table { width: 100%; min-width: 980px; border-collapse: collapse; white-space: nowrap; font-size: 12px; }
.rows-table th { position: sticky; top: 0; z-index: 2; padding: 9px 10px; background: var(--lh-surface-subtle, #edf3fa); color: var(--lh-muted, #425c77); text-align: left; font-weight: 600; }
.rows-table th, .rows-table td { border-bottom: 1px solid var(--lh-border, #e8eef5); }
.rows-table td { padding: 8px 10px; vertical-align: middle; }
.rows-table tbody tr:hover { background: var(--lh-surface-hover, #f8fbff); }
.rows-table tbody tr.duplicate { background: var(--lh-warn-soft, #fff9ed); }
.rows-table select, .rows-table input { min-width: 118px; }
.rows-table .source-cell, .rows-table .result-cell select { min-width: 0; }
.rows-table .rack-cell strong { color: var(--lh-charcoal, #1e354d); }
.rows-table .rack-cell, .rows-table .rack-head { position: sticky; left: 0; background: var(--lh-surface-subtle, #edf3fa); z-index: 1; }
.rows-table .rack-head { z-index: 3; }.rows-table .rack-cell small { display: block; color: var(--lh-muted); font-size: 11px; }
.rows-table .status-cell { color: var(--lh-accent, #175dbb); }
.rows-table .status-cell.duplicate, .rows-table tbody tr.duplicate .status-cell { color: var(--lh-warn, #946100); }
.rows-table .result-cell input { display: block; width: 100%; margin-top: 4px; }
.empty { padding: 24px 12px; text-align: center; color: var(--lh-muted, #7b8998); font-size: 13px; }
.pagination { display: flex; align-items: center; justify-content: flex-end; gap: 8px; color: var(--lh-muted, #60768c); font-size: 12px; }
.pagination > span:first-child { margin-right: auto; }
.pagination button { width: 38px; padding: 0; }
.spin { animation: lh-text-create-spin 1s linear infinite; }
@keyframes lh-text-create-spin { to { transform: rotate(360deg); } }
</style>
