<template>
  <div v-if="editable" class="document-field-control">
    <NInput v-if="kind === 'text'" :value="raw == null ? '' : Array.isArray(raw) ? raw.map(valueText).join('') : String(raw)" type="textarea" :autosize="{ minRows: 2, maxRows: 6 }" :disabled="disabled" :input-props="{ 'aria-label': field.name }" @update:value="update" />
    <NInputNumber v-else-if="kind === 'number'" :value="raw == null || raw === '' ? null : Number(raw)" :disabled="disabled" :input-props="{ 'aria-label': field.name }" clearable @update:value="update" />
    <NSelect v-else-if="kind === 'select' || kind === 'multi'" :value="raw ?? (kind === 'multi' ? [] : null)" :multiple="kind === 'multi'" :options="options" :disabled="disabled" :aria-label="field.name" filterable clearable @update:value="update" />
    <NDatePicker v-else-if="kind === 'date'" :value="dateValue" type="date" clearable :disabled="disabled" :input-props="{ 'aria-label': field.name }" @update:value="updateDate" />
    <NSwitch v-else-if="kind === 'bool'" :value="Boolean(raw)" :disabled="disabled" :aria-label="field.name" @update:value="update"><template #checked>是</template><template #unchecked>否</template></NSwitch>
    <NSelect v-else-if="kind === 'person'" :value="field.multiple === false ? personIds[0] || null : personIds" :options="peopleOptions" :multiple="field.multiple !== false" filterable :disabled="disabled" :aria-label="field.name" clearable @update:value="updatePeople" />
    <div v-else-if="kind === 'url'" class="document-url-fields">
      <NInput :value="typeof raw === 'string' ? raw : raw?.link || ''" :disabled="disabled" placeholder="https://" :input-props="{ 'aria-label': field.name + '链接' }" @update:value="value => update({ link: value, text: raw?.text || '' })" />
      <NInput :value="raw?.text || ''" :disabled="disabled" placeholder="链接名称" :input-props="{ 'aria-label': field.name + '链接名称' }" @update:value="value => update({ link: typeof raw === 'string' ? raw : raw?.link || '', text: value })" />
    </div>
  </div>
  <a v-else-if="kind === 'url' && safeLink && interactive" :href="safeLink" target="_blank" rel="noopener noreferrer" class="document-field-link">{{ raw?.text || safeLink }}<ExternalLink :size="13" aria-hidden="true" /></a>
  <time v-else-if="kind === 'date' && dateValue !== null" :datetime="new Date(dateValue).toISOString()">{{ new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai' }).format(dateValue) }}</time>
  <span v-else class="document-field-value" :class="{ 'is-empty': raw == null || raw === '' || Array.isArray(raw) && !raw.length }">{{ valueText(modelValue) }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { NDatePicker, NInput, NInputNumber, NSelect, NSwitch } from 'naive-ui';
import { ExternalLink } from 'lucide-vue-next';
import { fieldKind, fieldValue, valueText, writableField, type LibraryField, type LibraryValue } from '../api/documentLibrary';

const props = withDefaults(defineProps<{ field: LibraryField; modelValue?: LibraryValue; editing?: boolean; disabled?: boolean; interactive?: boolean }>(), { editing: false, disabled: false, interactive: true });
const emit = defineEmits<{ 'update:modelValue': [value: LibraryValue] }>();
const kind = computed(() => fieldKind(props.field));
const raw = computed(() => fieldValue(props.modelValue));
const editable = computed(() => props.editing && writableField(props.field));
const options = computed(() => (props.field.options || []).map(option => typeof option === 'string' ? { label: option, value: option } : {
  label: String(option.label || option.name || option.value || option.id || ''), value: String(option.value || (kind.value === 'person' ? option.id || option.name : option.name || option.id) || ''),
}));
const personIds = computed<string[]>(() => (Array.isArray(raw.value) ? raw.value : []).map(person => typeof person === 'string' ? person : person.id).filter(Boolean));
const peopleOptions = computed(() => {
  const result = options.value.slice();
  for (const person of Array.isArray(raw.value) ? raw.value : []) {
    const id = typeof person === 'string' ? person : person.id;
    if (id && !result.some(option => option.value === id)) result.push({ value: id, label: typeof person === 'string' ? person : person.name || id });
  }
  return result;
});
const dateValue = computed(() => {
  if (raw.value == null || raw.value === '') return null;
  const value = typeof raw.value === 'number' ? raw.value : Date.parse(String(raw.value));
  return Number.isFinite(value) ? value : null;
});
const safeLink = computed(() => {
  const link = typeof raw.value === 'string' ? raw.value : raw.value?.link;
  try { const url = new URL(link); return ['https:', 'http:'].includes(url.protocol) ? url.href : ''; } catch { return ''; }
});
function update(value: unknown) {
  const old = props.modelValue;
  emit('update:modelValue', old && typeof old === 'object' && 'type' in old && 'value' in old ? { ...old, value } : value);
}
function updateDate(value: number | null) {
  update(value === null ? null : typeof raw.value === 'string' ? new Date(value).toISOString() : value);
}
function updatePeople(value: string[]|string|null) {
  const ids=Array.isArray(value)?value:value?[value]:[];
  update(ids.map(id => ({ id, name: peopleOptions.value.find(person => person.value === id)?.label || id })));
}
</script>

<style scoped>
.document-field-control,.document-url-fields{display:grid;gap:6px;min-width:0}.document-field-control :deep(.n-input-number),.document-field-control :deep(.n-date-picker){width:100%}.document-field-value{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.6}.document-field-value.is-empty{color:var(--doc-muted,#64748b)}.document-field-link{display:inline-flex;align-items:center;gap:4px;color:var(--doc-accent,#005bb5);overflow-wrap:anywhere;max-width:100%}.document-field-link svg{flex:none}
</style>
