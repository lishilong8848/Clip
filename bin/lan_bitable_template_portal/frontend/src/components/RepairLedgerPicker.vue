<template>
  <RecordPickerDialog
    :open="open" title="选择台账设备" kicker="设备台账 · 多选"
    :records="records" :columns="columns" :selected-ids="selectedIds" multiple allow-empty
    :loading="loading" :query="query" :result-note="`共 ${total} 台 · 每页 50 台`"
    :status-message="statusMessage" :status-tone="error ? 'error' : 'info'"
    :server-page="page" :server-page-count="Math.max(1, Math.ceil(total / 50))"
    search-placeholder="搜索设备编号、设备名称、品牌、型号、安装位置等"
    @update:query="query = $event" @search="load(1)" @change-page="load($event)"
    @close="emit('close')" @confirm="confirm"
  >
    <template #toolbar-actions>
      <button v-if="canRefresh" class="ledger-refresh" type="button" :disabled="refreshing" @click="refresh">
        <RefreshCw :size="16" :class="{ spinning: refreshing }" aria-hidden="true" />
        {{ refreshing ? '同步中' : '刷新设备台账' }}
      </button>
    </template>
    <template #filters>
      <div class="ledger-filters">
        <label v-for="field in filterFields" :key="field">
          <span>{{ field }}</span>
          <VnetSelect :input-id="`ledger-filter-${field}`" :label="field" :model-value="filters[field] || '不限'"
            :options="['不限', ...(options[field] || [])]" :menu-z-index="2500"
            @update:model-value="setFilter(field, $event)" />
        </label>
        <button type="button" class="ledger-reset" :disabled="!hasFilters" @click="clearFilters" title="清除筛选" aria-label="清除筛选">
          <FilterX :size="18" aria-hidden="true" />
        </button>
      </div>
    </template>
  </RecordPickerDialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue';
import { FilterX, RefreshCw } from 'lucide-vue-next';
import { requestJson } from '../api/client';
import type { LooseDict } from '../types';
import RecordPickerDialog from './RecordPickerDialog.vue';
import VnetSelect from './VnetSelect.vue';

const props = defineProps<{ open: boolean; scope: string; selectedIds: string[]; selectedRecords: LooseDict[] }>();
const emit = defineEmits<{ close: []; confirm: [ids: string[], records: LooseDict[]] }>();
const filterFields = ['机楼', '系统名称', '大设备类型', '设备名称'];
const columnFields = ['设备编号', ...filterFields, '产品其它参数', '品牌', '安装位置', '型号', '设备类型标识', '容量'];
const columns = columnFields.map(key => ({ key, label: key, width: key === '产品其它参数' || key === '安装位置' ? '200px' : '135px', wrap: true }));
const filters = reactive<Record<string, string>>({});
const options = ref<Record<string, string[]>>({});
const records = ref<LooseDict[]>([]), query = ref(''), page = ref(1), total = ref(0);
const loading = ref(false), refreshing = ref(false), ready = ref(false), canRefresh = ref(false), error = ref('');
const knownRecords = new Map<string, LooseDict>();
let abort: AbortController | undefined, refreshAbort: AbortController | undefined;
let poll: ReturnType<typeof setTimeout> | undefined, generation = 0;
const hasFilters = computed(() => Object.values(filters).some(Boolean));
const statusMessage = computed(() => error.value || (refreshing.value
  ? ready.value ? '正在更新设备台账，仍可选择已缓存的设备。' : '正在初始化设备台账，完成后自动显示。'
  : !ready.value && !loading.value ? '设备台账尚未就绪，请刷新重试。' : ''));

function stop(): void {
  generation++; abort?.abort(); refreshAbort?.abort();
  clearTimeout(poll); poll = undefined; loading.value = false;
}
async function load(targetPage = page.value): Promise<void> {
  if (!props.open) return;
  abort?.abort(); clearTimeout(poll); poll = undefined;
  const controller = new AbortController(); abort = controller;
  const version = ++generation;
  loading.value = true; error.value = '';
  const params = new URLSearchParams({ scope: props.scope, q: query.value, page: String(targetPage) });
  for (const field of filterFields) if (filters[field]) params.set(field, filters[field]);
  try {
    const data = await requestJson(`/api/repair-management/ledger-candidates?${params}`, { signal: controller.signal });
    if (version !== generation || !props.open) return;
    records.value = data.records || []; options.value = data.options || {}; total.value = Number(data.total || 0); page.value = Number(data.page || targetPage);
    for (const item of records.value) knownRecords.set(String(item.record_id), item);
    ready.value = Boolean(data.cache?.ready); refreshing.value = Boolean(data.cache?.refreshing);
    canRefresh.value = Boolean(data.can_force_refresh); error.value = String(data.cache?.error || '');
    if (refreshing.value) poll = setTimeout(() => { void load(); }, 1500);
  } catch (e) {
    if (!controller.signal.aborted && version === generation) error.value = e instanceof Error ? e.message : '设备台账读取失败';
  } finally {
    if (version === generation) loading.value = false;
  }
}
function setFilter(field: string, value: string): void { filters[field] = value === '不限' ? '' : value; void load(1); }
function clearFilters(): void { for (const field of filterFields) filters[field] = ''; void load(1); }
async function refresh(): Promise<void> {
  if (refreshing.value) return;
  refreshing.value = true; error.value = '';
  const controller = new AbortController(); refreshAbort = controller;
  try {
    await requestJson('/api/repair-management/ledger-cache/refresh', { method: 'POST', body: '{}', signal: controller.signal });
    if (!controller.signal.aborted && props.open) await load(1);
  } catch (e) {
    if (!controller.signal.aborted) error.value = e instanceof Error ? e.message : '设备台账刷新失败';
    refreshing.value = false;
  }
}
function confirm(ids: string[]): void { emit('confirm', ids, ids.map(id => knownRecords.get(id) || { record_id: id })); }
watch(() => [props.open, props.scope], () => {
  stop();
  if (!props.open) return;
  knownRecords.clear(); for (const item of props.selectedRecords) knownRecords.set(String(item.record_id), item);
  records.value = []; query.value = ''; options.value = {}; total.value = 0;
  for (const field of filterFields) filters[field] = '';
  void load(1);
}, { immediate: true });
onBeforeUnmount(stop);
</script>

<style scoped>
.ledger-filters { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)) 38px; gap: 14px; align-items: end; }
.ledger-filters label { display: grid; gap: 5px; color: #506274; font-size: 13px; min-width: 0; }
.ledger-refresh, .ledger-reset { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 38px; border: 1px solid #cddaea; border-radius: 6px; background: white; color: #25537c; cursor: pointer; }
.ledger-refresh { white-space: nowrap; padding: 0 12px; }
.ledger-refresh:disabled, .ledger-reset:disabled { opacity: .5; cursor: default; }
.spinning { animation: ledger-spin 1s linear infinite; }
@keyframes ledger-spin { to { transform: rotate(360deg); } }
</style>
