<template>
  <section class="risk-sheet" :aria-label="schema ? '基础模板字段表' : '排查事项预填表'">
    <NDataTable :columns="columns" :data="rows" :row-key="row => row.key" :scroll-x="tableWidth" :max-height="schema?undefined:mobile?420:520" :pagination="pagination" size="small" bordered>
      <template #empty><NEmpty :description="schema ? '模板暂无字段' : '暂无排查事项'" /></template>
    </NDataTable>
  </section>
</template>

<script setup lang="ts">
import { computed, h, onBeforeUnmount, ref, toRaw, watch } from 'vue';
import { NButton, NDataTable, NEmpty, NInput, NSwitch, NTag, type DataTableColumns } from 'naive-ui';
import { Pencil, Trash2 } from 'lucide-vue-next';
import type { Dict } from '../api/client';
import RiskInspectionFields from './RiskInspectionFields.vue';
import { riskRoleName, riskTypeName } from './riskInspectionUi';

type Row = { key: string; index: number; label?: string; item?: Dict };
const props = defineProps<{ fields: Dict[]; items?: Dict[]; schema?: boolean; canEdit?: boolean; disabled?: boolean; history?: boolean }>();
const emit = defineEmits<{ 'update:items': [items: Dict[]]; edit: [field: Dict]; removeField: [field: Dict]; requiredChange: [field: Dict, required: boolean] }>();
const page = ref(1), keys = new WeakMap<object, string>();
const query = window.matchMedia('(max-width:700px)'), mobile = ref(query.matches);
const resize = (event: MediaQueryListEvent) => { mobile.value = event.matches; };
query.addEventListener('change', resize);
onBeforeUnmount(() => query.removeEventListener('change', resize));
let serial = 0;
function keyFor(item: Dict) { const raw = toRaw(item); if (!keys.has(raw)) keys.set(raw, 'row-' + ++serial); return keys.get(raw)!; }
const rows = computed<Row[]>(() => props.schema
  ? ['字段类型', '填写方式', '填写要求', '可选内容'].map((label, index) => ({ key: String(index), index, label }))
  : (props.items || []).map((item, index) => ({ key: keyFor(item), index, item })));
watch(() => [rows.value.length, rows.value[0]?.key] as const, ([length, first], [previous, oldFirst]) => {
  page.value = first !== oldFirst ? 1 : length > previous ? Math.max(1, Math.ceil(length / 10)) : Math.max(1, Math.min(page.value, Math.ceil(length / 10)));
});
const pagination = computed(() => !props.schema && rows.value.length > 10 ? { page: page.value, pageSize: 10, pageSlot: 5, onChange: (value: number) => { page.value = value; } } : false);
function update(row: Row, patch: Dict) {
  if (props.disabled || !row.item) return;
  const items = [...(props.items || [])], next = { ...row.item, ...patch };
  if (!items[row.index] || keyFor(items[row.index]!) !== row.key) return;
  keys.set(next, row.key); items[row.index] = next; emit('update:items', items);
}
function remove(row: Row) { if (!props.disabled && (props.items?.length || 0) > 1 && props.items?.[row.index] && keyFor(props.items[row.index]!) === row.key) emit('update:items', props.items.filter((_, index) => index !== row.index)); }
function schemaValue(field: Dict, index: number) {
  if (index === 0) return riskTypeName(field.type);
  if (index === 1) return riskRoleName(field.role);
  if (index === 2) return field.role !== 'input' ? '自动生成' : field.required ? '必填' : '不必填';
  return [3, 4].includes(field.type) ? (field.property?.options || []).map((option: Dict) => option.name).join(' / ') || '尚未配置选项' : '—';
}
const width = (field: Dict) => field.type === 1 ? 260 : field.type === 17 ? 200 : 180;
const firstWidth = computed(() => props.schema ? 116 : mobile.value ? 180 : 260);
const tableWidth = computed(() => firstWidth.value + props.fields.reduce((sum, field) => sum + width(field), 0));
const columns = computed<DataTableColumns<Row>>(() => [
  { key: 'item', title: props.schema ? '模板设置' : '排查事项', width: firstWidth.value, fixed: 'left', render: row => props.schema ? h('strong', row.label) : h('div', { class: 'risk-sheet-item' }, [
    h('span', { class: 'risk-sheet-number' }, String(row.index + 1).padStart(2, '0')),
    h(NInput, { value: row.item!.title, disabled: props.disabled, maxlength: 300, type: 'textarea', autosize: { minRows: 1, maxRows: 4 },
      placeholder: `事项 ${row.index + 1}：设备、部件或需要检查的内容`, inputProps: { 'aria-label': `排查事项 ${row.index + 1}` }, 'onUpdate:value': value => update(row, { title: value }) }),
    h(NButton, { text: true, disabled: props.disabled || props.items!.length <= 1, 'aria-label': `移除事项 ${row.index + 1}`, onClick: () => remove(row) }, () => h(Trash2, { size: 15 })),
  ]) },
  ...props.fields.map(field => ({ key: field.key, width: width(field), title: () => h('div', { class: 'risk-sheet-heading' }, [
    h('strong', field.field_name),
    h('div', { class: 'risk-sheet-heading-meta' }, [h('small', riskTypeName(field.type)), field.is_primary ? h(NTag, { size: 'small', bordered: false }, () => '主字段') : null,
      field.unsupported ? h(NTag, { size: 'small', type: 'warning', bordered: false }, () => '需调整类型') : field.role === 'input' ? h('small', field.required ? '必填' : '选填') : h('small', '系统填写')]),
    props.schema && props.canEdit ? h('div', { class: 'risk-sheet-heading-actions' }, [
      h(NButton, { size: 'small', disabled: props.disabled, 'aria-label': `修改字段 ${field.field_name}`, onClick: () => emit('edit', field) }, () => [h(Pencil, { size: 14 }), '修改']),
      !field.is_primary ? h(NButton, { size: 'small', quaternary: true, type: 'error', disabled: props.disabled, 'aria-label': `删除字段 ${field.field_name}`, onClick: () => emit('removeField', field) }, () => h(Trash2, { size: 14 })) : null,
    ]) : null,
  ]), render: (row: Row) => props.schema ? row.index === 2 && props.canEdit && field.role === 'input'
    ? h('div', { class: 'risk-sheet-required' }, [h(NSwitch, { value: field.required === true, disabled: props.disabled || field.unsupported, 'aria-label': field.field_name + '是否必填',
      'onUpdate:value': required => emit('requiredChange', field, required) }), h('span', field.required ? '必填' : '不必填')])
    : h('div', { class: ['risk-sheet-value', field.unsupported && 'risk-sheet-warning'] }, schemaValue(field, row.index))
    : field.role !== 'input' ? h('span', { class: 'risk-sheet-generated' }, riskRoleName(field.role))
    : field.unsupported ? h('span', { class: 'risk-sheet-warning' }, '请先调整字段类型')
    : props.history && [5, 17].includes(field.type) ? h('span', { class: 'risk-sheet-generated' }, field.type === 5 ? '本次发布重新填写' : '排查人现场上传')
    : h(RiskInspectionFields, { fields: [field], values: row.item!.values, prefill: true, cell: true, disabled: props.disabled,
      'onUpdate:values': values => update(row, { values }) }),
  })),
]);
</script>

<style scoped>
.risk-sheet :deep(.risk-sheet-required){display:inline-flex;align-items:center;gap:8px;white-space:nowrap}
.risk-sheet{min-width:0;max-width:100%;border-radius:6px;overflow:hidden}.risk-sheet :deep(.risk-fields-cell){display:block;grid-template-columns:minmax(0,1fr)}.risk-sheet :deep(.n-data-table){--n-th-padding:12px;--n-td-padding:12px}.risk-sheet :deep(.n-data-table-td){vertical-align:top}.risk-sheet :deep(.n-data-table-th){vertical-align:top;background:var(--table-header-color,var(--cf-surface-soft,#f2f5f9))}.risk-sheet :deep(.risk-sheet-heading){display:grid;gap:8px;min-width:0}.risk-sheet :deep(.risk-sheet-heading>strong){font-size:13px;line-height:1.5;overflow-wrap:anywhere}.risk-sheet :deep(.risk-sheet-heading-meta),.risk-sheet :deep(.risk-sheet-heading-actions){display:flex;align-items:center;gap:6px;flex-wrap:wrap}.risk-sheet :deep(small),.risk-sheet :deep(.risk-sheet-generated),.risk-sheet :deep(.risk-sheet-number){font-size:12px;color:var(--text-color-3,#667085)}.risk-sheet :deep(.risk-sheet-generated){display:block;line-height:1.7;padding-top:7px}.risk-sheet :deep(.risk-sheet-item){display:flex;gap:8px;align-items:flex-start}.risk-sheet :deep(.risk-sheet-item>.n-input){flex:1;min-width:0}.risk-sheet :deep(.risk-sheet-number){padding-top:7px}.risk-sheet :deep(.risk-sheet-item>.n-button){margin-top:6px}.risk-sheet :deep(.risk-sheet-value){line-height:1.7;white-space:pre-wrap;overflow-wrap:anywhere}.risk-sheet :deep(.risk-sheet-warning){color:var(--warning-color,#a56611)}
@media(max-width:700px){.risk-sheet :deep(.n-data-table){--n-th-padding:10px;--n-td-padding:10px}.risk-sheet :deep(.n-button){min-height:44px;min-width:44px}.risk-sheet :deep(.risk-sheet-item>.n-button){margin-top:0}.risk-sheet :deep(.risk-sheet-item){gap:5px}.risk-sheet :deep(.risk-sheet-number){display:none}.risk-sheet :deep(.risk-sheet-heading-actions){gap:4px}}
</style>
