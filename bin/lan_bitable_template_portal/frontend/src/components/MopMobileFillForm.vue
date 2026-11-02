<template>
  <section ref="formRef" class="mop-mobile-form" aria-label="维护单快捷填写" data-v2-inline-feedback>
    <div v-if="sheet.is_cover" class="mobile-cover">{{ sheet.name }}<span>只读</span></div>
    <template v-else>
      <section v-if="commonFields.length" class="quick-fields">
        <label v-for="field in commonFields" :key="`${field.row}:${field.col}`" :data-mop-mobile-cell="`${field.row}:${field.col}`">
          <span>{{ field.label }}</span>
          <textarea :value="fieldValue(field)" :rows="field.label.includes('备注') ? 2 : 1" @input="writeText(field, ($event.target as HTMLTextAreaElement).value)" />
        </label>
      </section>
      <section v-if="step === 'checks'" class="quick-checks">
        <div class="quick-check-head">
          <div><strong>检查结果（选填）</strong><span>{{ completedCount }} / {{ fields.checks.length }} 项</span></div>
          <button v-if="bulkCount" type="button" class="quick-normal" @click="emit('mark-all-normal')"><CheckCheck :size="19" />第10行后全正常</button>
        </div>
        <details v-if="fields.checks.length" class="quick-note-section" open>
          <summary>备注与数据记录</summary>
          <label class="quick-search"><Search :size="17" /><input v-model="search" type="search" aria-label="搜索检查项" placeholder="搜索设备或检查项" /></label>
          <label class="quick-check-picker">
            <span>检查项</span>
            <select v-model="selectedKey" aria-label="选择检查项">
              <option v-for="item in filteredChecks" :key="checkKey(item)" :value="checkKey(item)">{{ item.row + 1 }}行 · {{ item.title }} · {{ checkState(item) ? '已填' : '未填' }}</option>
            </select>
          </label>
          <p v-if="!filteredChecks.length" class="quick-empty">没有匹配的检查项</p>
          <template v-if="selectedCheck">
            <details v-if="selectedCheck.detail" class="quick-requirement"><summary>检查要求</summary><p>{{ selectedCheck.detail }}</p></details>
            <div class="quick-result-row" :class="{ 'mop-pending': checkPending(selectedCheck) }" :data-mop-mobile-cell="checkKey(selectedCheck)"><span>本项结果<small v-if="checkPending(selectedCheck)" class="quick-pending-text">未填写</small></span><div class="quick-result-options" role="group" aria-label="本项检查结果">
              <button v-for="option in selectedCheck.options" :key="option.value" type="button"
                :class="{ chosen: checkState(selectedCheck) === option.value, abnormal: option.value.includes('异常') }"
                :aria-pressed="checkState(selectedCheck) === option.value"
                @click="emit('edit-checkbox', selectedCheck.row, selectedCheck.col, option.value)">{{ option.label }}</button>
            </div></div>
            <label v-for="field in selectedCheck.fields" :key="`${field.row}:${field.col}`" :data-mop-mobile-cell="`${field.row}:${field.col}`">
              <span>{{ field.label }}</span>
              <textarea :value="fieldValue(field)" :aria-label="`${selectedCheck.title} ${field.label}`" rows="2" @input="writeText(field, ($event.target as HTMLTextAreaElement).value)" />
            </label>
            <div class="quick-item-navigation">
              <button type="button" :disabled="selectedIndex <= 0" @click="selectAdjacent(-1)"><ChevronLeft :size="18" />上一项</button>
              <span>{{ selectedIndex + 1 }} / {{ filteredChecks.length }}</span>
              <button type="button" :disabled="selectedIndex >= filteredChecks.length - 1" @click="selectAdjacent(1)">下一项<ChevronRight :size="18" /></button>
            </div>
          </template>
        </details>
      </section>
      <section v-if="metadata.length" class="quick-fields quick-time-fields">
        <strong v-if="step === 'details'">维护时间（选填）</strong>
        <label v-for="field in metadata" :key="`${field.row}:${field.value_col}:${field.label}`" :class="{ 'quick-inline-field': String(field.label).includes('维护完成情况'), 'mop-pending': metadataPending(field) }" :data-mop-mobile-cell="`${field.row}:${field.value_col}`">
          <span>{{ field.label }}<small v-if="metadataPending(field)" class="quick-pending-text">未填写</small></span>
          <select v-if="String(field.label).includes('维护完成情况')" :aria-label="String(field.label)" :value="completionValue(field)" @change="emit('edit-field', field, ($event.target as HTMLSelectElement).value)">
            <option value="">未填写</option><option value="已完成[√] 未完成[ ]">已完成</option><option value="已完成[ ] 未完成[√]">未完成</option>
          </select>
          <input v-else type="datetime-local" :aria-label="String(field.label)" :value="dateValue(field)" @input="emit('edit-field', field, formatMopDateTime(($event.target as HTMLInputElement).value))" />
        </label>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue';
import { CheckCheck, ChevronLeft, ChevronRight, Search } from 'lucide-vue-next';
import type { Dict } from '../api/client';
import { mopMobileFields, type MopMobileCheck, type MopMobileField } from '../mopMobileFields';
import { formatMopDateTime, parseMopComparableDate } from '../mopSheetUtils';
import { mopSelectedChoice } from '../mopFillProgress';

const props = defineProps<{
  sheet: Dict; bulkCount: number; step: 'details' | 'checks';
  pendingKeys?: string[];
  cellValue: (row: number, col: number) => string;
  editableCellAt: (row: number, col: number) => boolean;
  cellMergeSpan: (row: number, col: number) => Dict;
  checkboxStateLabel: (cell: Dict) => string;
  maintenanceFieldAt: (row: number, col: number) => Dict | null;
}>();
const emit = defineEmits<{
  'mark-all-normal': [];
  'edit-raw': [row: number, col: number, value: string];
  'edit-checkbox': [row: number, col: number, value: string];
  'edit-field': [field: Dict, value: string];
}>();
const fields = computed(() => mopMobileFields(props.sheet, props.editableCellAt, props.cellMergeSpan));
const formRef = ref<HTMLElement | null>(null);
const pending = computed(() => new Set(props.pendingKeys || []));
const commonFields = computed(() => fields.value.common.filter(field => field.label.includes('备注') === (props.step === 'checks')));
const metadata = computed<Dict[]>(() => (props.sheet.maintenance_fields || []).filter((field: Dict) => (
  props.maintenanceFieldAt(Number(field.row), Number(field.value_col)) && String(field.label).includes('维护完成情况') === (props.step === 'checks')
)));
const search = ref(''), selectedKey = ref('');
const checkKey = (item: MopMobileCheck) => `${item.row}:${item.col}`;
const filteredChecks = computed(() => {
  const query = search.value.trim().toLocaleLowerCase();
  return fields.value.checks.filter(item => !query || `${item.title} ${item.detail}`.toLocaleLowerCase().includes(query));
});
const selectedIndex = computed(() => filteredChecks.value.findIndex(item => checkKey(item) === selectedKey.value));
const selectedCheck = computed(() => filteredChecks.value[selectedIndex.value]);
watch(filteredChecks, checks => { if (!checks.some(item => checkKey(item) === selectedKey.value)) selectedKey.value = checks[0] ? checkKey(checks[0]) : ''; }, { immediate: true });
watch(() => props.sheet.name, () => { search.value = ''; selectedKey.value = fields.value.checks[0] ? checkKey(fields.value.checks[0]) : ''; });
const checkboxMap = computed(() => new Map<string, Dict>((props.sheet.checkbox_cells || []).map((cell: Dict) => [`${cell.row}:${cell.col}`, cell])));
function checkState(item: MopMobileCheck) { return props.checkboxStateLabel(checkboxMap.value.get(checkKey(item)) || item); }
function checkPending(item: MopMobileCheck) { return pending.value.has(checkKey(item)) && !checkState(item); }
const completedCount = computed(() => fields.value.checks.filter(item => !!checkState(item)).length);
function fieldValue(field: MopMobileField) {
  const value = props.cellValue(field.row, field.col);
  return field.prefix && value.startsWith(field.prefix) ? value.slice(field.prefix.length) : value;
}
function writeText(field: MopMobileField, value: string) { emit('edit-raw', field.row, field.col, field.prefix + value); }
function selectAdjacent(step: number) { const item = filteredChecks.value[selectedIndex.value + step]; if (item) selectedKey.value = checkKey(item); }
function dateValue(field: Dict) {
  const date = parseMopComparableDate(props.cellValue(Number(field.row), Number(field.value_col)));
  if (!date) return '';
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}
function completionValue(field: Dict) {
  const value = props.cellValue(Number(field.row), Number(field.value_col)).replace(/^\s*维护完成情况\s*[:：]?\s*/, '');
  const selected = mopSelectedChoice(value, ['已完成', '未完成']);
  return selected === '未完成' ? '已完成[ ] 未完成[√]' : selected === '已完成' ? '已完成[√] 未完成[ ]' : '';
}
function metadataPending(field: Dict) {
  return pending.value.has(`${field.row}:${field.value_col}`)
    && !(String(field.label).includes('维护完成情况') ? completionValue(field) : dateValue(field));
}

// The owner selects the target's details/checks step before requesting focus.
async function focusCell(row: number, col: number): Promise<boolean> {
  if (!Number.isInteger(row) || !Number.isInteger(col) || row < 0 || col < 0) return false;
  await nextTick();
  const key = `${row}:${col}`;
  const check = fields.value.checks.find(item => checkKey(item) === key || item.fields.some(field => `${field.row}:${field.col}` === key));
  const visibleCheck = props.step === 'checks' ? check : undefined;
  if (!visibleCheck && !commonFields.value.some(field => `${field.row}:${field.col}` === key)
    && !metadata.value.some(field => `${field.row}:${field.value_col}` === key)) return false;
  search.value = '';
  if (visibleCheck) selectedKey.value = checkKey(visibleCheck);
  await nextTick();
  const root = formRef.value;
  const target = root?.querySelector<HTMLElement>(`[data-mop-mobile-cell="${key}"]`);
  if (!root || !target) return false;
  for (let details = target.closest('details'); details && root.contains(details); details = details.parentElement?.closest('details') || null) {
    details.open = true;
  }
  const control = target.querySelector<HTMLElement>('textarea, input, select, button');
  if (!control) return false;
  control.focus({ preventScroll: true });
  await nextTick();
  target.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'auto' });
  // V2 的原生 select 会把焦点转交给同一标签内的可见选择器。
  return target.contains(root.ownerDocument.activeElement);
}
defineExpose({ focusCell });
</script>

<style scoped>
.mop-mobile-form { --mop-pending-duration: 2.6s; display: grid; gap: 14px; padding: 12px; color: #1e293b; font-size: 14px; }
.quick-fields, .quick-checks { display: grid; gap: 12px; min-width: 0; }
.quick-fields > strong, .quick-check-head strong { font-size: 15px; font-weight: 650; }
.quick-check-head { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px 12px; }
.quick-check-head > div { display: grid; gap: 3px; }
.quick-check-head span { color: #64748b; font-size: 12px; white-space: nowrap; }
label { display: grid; gap: 6px; min-width: 0; }
label > span { font-size: 13px; color: #475569; }
input, select, textarea { box-sizing: border-box; width: 100%; min-width: 0; max-width: 100%; min-height: 44px; border: 1px solid #cbd5e1; border-radius: 6px; padding: 9px 10px; font: inherit; font-size: 16px; color: #1e293b; background: #fff; }
textarea { line-height: 1.5; resize: vertical; }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 44px; border: 1px solid #cbd5e1; border-radius: 6px; padding: 8px 12px; font: inherit; font-weight: 550; background: #fff; color: #334155; cursor: pointer; }
button:disabled { opacity: .45; cursor: default; }
:is(input, select, textarea, button, summary):focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
.quick-normal { flex: 0 1 auto; color: #fff; border-color: #16744e; background: #16744e; }
.quick-check-picker, .quick-inline-field { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; gap: 10px; }
.quick-check-picker > span, .quick-inline-field > span { white-space: nowrap; }
.quick-inline-field select { justify-self: end; width: min(100%, 160px); }
.quick-note-section { border-block: 1px solid #e2e8f0; padding: 8px 0; }
summary { min-height: 44px; display: flex; align-items: center; gap: 8px; cursor: pointer; font-weight: 600; list-style: none; }
summary::after { content: ''; width: 7px; height: 7px; border-right: 1.5px solid currentColor; border-bottom: 1.5px solid currentColor; transform: rotate(45deg); margin-left: auto; margin-right: 4px; transition: transform 160ms ease; }
details[open] > summary::after { transform: rotate(225deg); }
details > :not(summary) { margin-top: 10px; }
.quick-search { position: relative; }
.quick-search svg { position: absolute; left: 12px; top: 14px; color: #64748b; pointer-events: none; }
.quick-search input { padding-left: 36px; }
.quick-result-row { display: flex; align-items: center; flex-wrap: wrap; gap: 8px 10px; }
.quick-result-row > span { color: #475569; font-size: 13px; white-space: nowrap; }
.quick-result-options { display: flex; flex: 1 1 180px; flex-wrap: wrap; gap: 8px; }
.quick-result-options button { flex: 1 1 64px; }
.quick-result-options button.chosen { color: #11633d; border-color: #69ae90; background: #e9f7ef; }
.quick-result-options button.chosen.abnormal { color: #b42318; border-color: #f2a79d; background: #fff1ef; }
.quick-requirement { color: #64748b; font-size: 13px; }
.quick-requirement p { white-space: pre-wrap; line-height: 1.6; }
.quick-item-navigation { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.quick-item-navigation span { color: #64748b; font-size: 12px; }
.quick-empty { color: #64748b; }
.mobile-cover { display: flex; justify-content: space-between; }
.mobile-cover span { color: #64748b; }
.quick-pending-text { margin-left: 8px; color: #946200; font-size: 12px; font-weight: 500; }
.mop-pending { position: relative; }
/* 只让未填控件外围的提示边框呼吸，文字和输入区域保持稳定。 */
.mop-pending::before { content: ''; position: absolute; inset: -4px; border: 1px solid #b88a28; border-radius: 8px; pointer-events: none; animation: mop-pending-breathe var(--mop-pending-duration) ease-in-out infinite; }
@keyframes mop-pending-breathe { 0%, 100% { opacity: .25; } 50% { opacity: .8; } }
@media (prefers-reduced-motion: reduce) { summary::after { transition: none; } .mop-pending::before { animation: none; opacity: .65; } }
</style>
