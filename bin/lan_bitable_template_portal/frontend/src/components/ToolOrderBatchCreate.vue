<template>
  <section class="batch-plan" aria-label="新增本月工单">
    <header>
      <strong>{{ plan.month }} 月度计划</strong>
      <span>可创建 {{ plan.items.length }} 项</span>
    </header>
    <div v-if="plan.items.length" class="batch-filters">
      <NInput v-model:value="search" clearable :disabled="busy" placeholder="搜索工具、型号或位置"
        :input-props="{ 'aria-label': '搜索计划工具' }" />
      <select v-model="scope" :disabled="busy" aria-label="筛选计划楼栋">
        <option v-for="option in scopeOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
      </select>
      <select v-model="kind" :disabled="busy" aria-label="筛选计划类型">
        <option value="all">全部类型</option>
        <option value="self">自检</option>
        <option value="external">送检</option>
      </select>
    </div>
    <template v-if="filtered.length">
      <div class="batch-selection">
        <NCheckbox :checked="allVisibleSelected" :indeterminate="someVisibleSelected && !allVisibleSelected"
          :disabled="busy || (!allVisibleSelected && !canSelect(filtered))"
          @update:checked="checked => toggle(filtered, checked)">全选筛选结果</NCheckbox>
        <NCheckbox :checked="allPageSelected" :indeterminate="somePageSelected && !allPageSelected"
          :disabled="busy || !canSelect(pageItems)" @update:checked="checked => toggle(pageItems, checked)">选择本页</NCheckbox>
        <span v-if="!canSelect(filtered)">单次最多 {{ plan.limit }} 项，可分批选择。</span>
      </div>
      <div class="batch-items">
        <NCheckbox v-for="item in pageItems" :key="key(item)" class="batch-item"
          :checked="selected.has(key(item))" :disabled="busy || (!selected.has(key(item)) && selected.size >= plan.limit)"
          @update:checked="checked => toggle([item], checked)">
          <span class="batch-tool">
            <strong>{{ item.name }}</strong>
            <small v-if="item.model || item.location">{{ [item.model, item.location].filter(Boolean).join(' · ') }}</small>
          </span>
          <span class="batch-due">{{ item.scope }}楼 · {{ kindLabels[item.kind] }} <small>计划 {{ item.due }}</small></span>
        </NCheckbox>
      </div>
      <NPagination v-if="filtered.length > pageSize" v-model:page="page" :item-count="filtered.length"
        :page-size="pageSize" :disabled="busy" simple />
    </template>
    <p v-else class="batch-empty">{{ plan.archived ? '本月已归档。' : plan.items.length
      ? '没有符合筛选条件的计划。' : '本月暂无待创建的自检或送检计划。' }}</p>
    <footer>
      <span aria-live="polite">已选 {{ selectedItems.length }} 项</span>
      <NButton size="small" :disabled="busy || !selectedItems.length" @click="selected = new Set()">清空选择</NButton>
      <NButton size="small" :disabled="busy" @click="emit('close')">取消</NButton>
      <NButton type="primary" :loading="busy" :disabled="!selectedItems.length"
        @click="emit('create', selectedItems)">创建 {{ selectedItems.length }} 张工单</NButton>
    </footer>
  </section>
</template>

<script lang="ts">
export interface PlannedOrder {
  asset_id: string;
  scope: string;
  name: string;
  model: string | null;
  location: string | null;
  kind: 'self' | 'external';
  due: string;
}
export interface OrderCreationPlan {
  month: string;
  items: PlannedOrder[];
  archived: boolean;
  limit: number;
}
</script>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { NButton, NCheckbox, NInput, NPagination } from 'naive-ui';

const props = defineProps<{ plan: OrderCreationPlan; busy: boolean }>();
const emit = defineEmits<{ close: []; create: [orders: PlannedOrder[]] }>();
const search = ref('');
const scope = ref('all');
const kind = ref('all');
const page = ref(1);
const pageSize = 12;
const selected = ref(new Set<string>());
const kindLabels = { self: '自检', external: '送检' };
const scopeOptions = computed(() => [{ value: 'all', label: '全部楼栋' },
  ...Array.from(new Set(props.plan.items.map(item => item.scope)), value => ({ value, label: `${value}楼` }))]);
const key = (item: PlannedOrder) => `${item.asset_id}:${item.kind}`;
const filtered = computed(() => {
  const query = search.value.trim().toLocaleLowerCase();
  return props.plan.items.filter(item => (scope.value === 'all' || item.scope === scope.value)
    && (kind.value === 'all' || item.kind === kind.value)
    && (!query || [item.name, item.model, item.location].join(' ').toLocaleLowerCase().includes(query)));
});
const pageItems = computed(() => filtered.value.slice((page.value - 1) * pageSize, page.value * pageSize));
const selectedItems = computed(() => props.plan.items.filter(item => selected.value.has(key(item))));
const allVisibleSelected = computed(() => filtered.value.length > 0 && filtered.value.every(item => selected.value.has(key(item))));
const someVisibleSelected = computed(() => filtered.value.some(item => selected.value.has(key(item))));
const allPageSelected = computed(() => pageItems.value.every(item => selected.value.has(key(item))));
const somePageSelected = computed(() => pageItems.value.some(item => selected.value.has(key(item))));
const canSelect = (items: PlannedOrder[]) => selected.value.size
  + items.filter(item => !selected.value.has(key(item))).length <= props.plan.limit;

function toggle(items: PlannedOrder[], checked: boolean) {
  if (checked && !canSelect(items)) return;
  for (const item of items) {
    if (checked) selected.value.add(key(item));
    else selected.value.delete(key(item));
  }
}

watch([search, scope, kind], () => { page.value = 1; });
watch(() => props.plan, (plan, previous) => {
  selected.value = new Set(plan.month === previous.month ? selectedItems.value.map(key) : []);
  if (!scopeOptions.value.some(option => option.value === scope.value)) scope.value = 'all';
  page.value = Math.min(page.value, Math.max(1, Math.ceil(filtered.value.length / pageSize)));
});
</script>

<style scoped>
.batch-plan { display:grid; gap:10px; padding:12px 14px; border-top:1px solid var(--tool-line); }
header,footer,.batch-selection { display:flex; flex-wrap:wrap; align-items:center; gap:6px 10px; }
header span,footer span,.batch-selection span,.batch-empty,small { color:var(--tool-muted); font-size:13px; }
footer span { margin-inline-end:auto; }
.batch-filters { display:grid; grid-template-columns:minmax(0,1fr) repeat(2,minmax(0,132px)); gap:8px; }
.batch-filters select { min-width:0; min-height:34px; padding:4px 8px; border:1px solid var(--tool-line); border-radius:6px; background:var(--tool-surface); color:inherit; font:inherit; }
.batch-items { border-block:1px solid var(--tool-line); }
.batch-item { display:grid; grid-template-columns:auto minmax(0,1fr); align-items:center; padding:6px 8px; }
.batch-item :deep(.n-checkbox__label) { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:8px; align-items:center; min-width:0; }
.batch-item + .batch-item { border-top:1px solid var(--tool-line); }
.batch-item[aria-checked="true"],.batch-item:hover { background:var(--tool-tint); }
.batch-tool { overflow-wrap:anywhere; }
.batch-item small { display:block; }
.batch-due { text-align:end; }
.batch-empty { margin:4px 0; }
@container (max-width:560px) {
  .batch-plan { padding:8px 10px; }
  .batch-filters { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .batch-filters > :first-child { grid-column:1/-1; }
}
</style>
