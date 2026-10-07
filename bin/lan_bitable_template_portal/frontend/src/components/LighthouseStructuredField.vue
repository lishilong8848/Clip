<template>
  <div ref="controlRoot" class="lh-sf" :class="{ 'cabinet-row-editor': field.native_cabinet_row_editor, 'compact-fields': field.compact_columns, 'drill-config-editor': field.native_drill_configuration }">
    <PlanConvergenceRules v-if="field.native_plan_rules" :is-admin="true" :model-value="modelValue" :disabled="disabled" @update:model-value="emitUpdate" />
    <CabinetBatchTextFill v-else-if="field.native_cabinet_text_fill" :batch-id="field.batch_id" embedded :model-value="modelValue" :disabled="disabled" :plan-id="planId" :plan-version="planVersion" :field-name="field.name" @update:model-value="emitUpdate" />
    <LighthouseCabinetTextCreate v-else-if="field.native_cabinet_text_create" :field="field" :model-value="modelValue" :disabled="disabled" :id="props.id" :plan-id="planId" :plan-version="planVersion" @update:model-value="emitUpdate" />
    <LighthouseCabinetProof v-else-if="field.native_cabinet_proof" :field="field" :model-value="modelValue" :disabled="disabled" :id="props.id" @update:model-value="emitUpdate" @load-options="emit('load-options', $event)" />
    <LighthouseNoticeSop v-else-if="field.native_notice_sop" :field="field" :model-value="modelValue" :disabled="disabled" :id="props.id" @update:model-value="emitUpdate" @load-options="emit('load-options', $event)" />
    <RepairPeoplePicker v-else-if="field.native_repair_people" :scope="field.scope || 'ALL'" :input-id="props.id" :label="field.label" :model-value="repairPeople" :disabled="disabled" @update:model-value="updateRepairPeople" />
    <fieldset v-else-if="isObject" class="lh-sf-fs" :aria-label="field.label">
      <legend class="lh-sf-g">{{ field.label || '' }}</legend>
      <label v-if="hasSparePartFields" class="lh-sf-spare"><input type="checkbox" :checked="sparePartsEnabled" :disabled="disabled" @change="toggleSpareParts" />是否涉及更换备件</label>
      <input v-if="field.searchable" class="lh-sf-input lh-sf-search" type="search" v-model="searchText" :aria-label="'查找' + field.label" maxlength="120" :placeholder="'查找' + field.label" @input="page = 1" />
      <div v-for="(child, i) in visibleChildren" :key="child.path" class="lh-sf-c" :class="{ 'lh-sf-wide': ['array', 'object'].includes(child.type) }">
        <LighthouseStructuredField :field="child" :model-value="childValue(child)" :disabled="disabled || child.read_only"
          :id="props.id + '-' + i" :context="activeContext" :step-limit="stepLimit"
          :min-step-id="child.path === 'to_step_id' ? childValue({ path: 'from_step_id' }) : minStepId"
          @update:model-value="(v: any) => onChild(child, v)" />
      </div>
      <div v-if="field.paginated && pageCount > 1" class="lh-sf-page" role="navigation" :aria-label="field.label + '分页'">
        <button type="button" class="lh-sf-b lh-sf-pg" :disabled="page <= 1" aria-label="上一页" title="上一页" @click="pagePrev"><ChevronLeft :size="14" /></button>
        <span class="lh-sf-page-txt">第 {{ page }} / {{ pageCount }} 页 · 共 {{ objectChildren.length }} 项</span>
        <button type="button" class="lh-sf-b lh-sf-pg" :disabled="page >= pageCount" aria-label="下一页" title="下一页" @click="pageNext"><ChevronRight :size="14" /></button>
      </div>
    </fieldset>

    <fieldset v-else-if="isArray" class="lh-sf-fs" :aria-label="field.label">
      <legend class="lh-sf-g lh-sf-ahead">
        <span>{{ field.label || '' }}</span>
        <button v-if="!fixedSize" type="button" class="lh-sf-b" :disabled="!canAdd" :aria-label="'添加' + (field.label || '一项')" @click="addItem"><Plus :size="14" /></button>
      </legend>
      <ol v-if="arrayValue.length" class="lh-sf-list">
        <li v-for="idx in pageIndexes" :key="arrayValue[idx]?.[field.identity_key] || arrayValue[idx]?.step_id || arrayValue[idx]?.rule_id || idx" class="lh-sf-entry">
          <span class="lh-sf-idx" aria-hidden="true">{{ idx + 1 }}</span>
          <LighthouseStructuredField :field="entryField" :model-value="arrayValue[idx]" :disabled="disabled || fixedCommander(idx)"
            :id="props.id + '-' + idx" :context="activeContext" :step-limit="field.context_source === 'sop_steps' ? idx : stepLimit"
            @update:model-value="(v: any) => onItem(idx, v)" />
          <button v-if="!fixedSize" type="button" class="lh-sf-b lh-sf-rm" :disabled="!canRemove || fixedCommander(idx)" :aria-label="'删除第 ' + (idx + 1) + ' 项'" @click="removeItem(idx)"><Trash2 :size="14" /></button>
          <button v-if="field.reorderable" type="button" class="lh-sf-b" :disabled="disabled || idx === 0" :aria-label="'上移第 ' + (idx + 1) + ' 项'" title="上移" @click="moveItem(idx, -1)"><ArrowUp :size="14" /></button>
          <button v-if="field.reorderable" type="button" class="lh-sf-b" :disabled="disabled || idx === arrayValue.length - 1" :aria-label="'下移第 ' + (idx + 1) + ' 项'" title="下移" @click="moveItem(idx, 1)"><ArrowDown :size="14" /></button>
        </li>
      </ol>
      <p v-else class="lh-sf-empty">（空，可添加）</p>
      <div v-if="pageCount > 1" class="lh-sf-page" role="navigation" :aria-label="'第 ' + field.label + ' 分页'">
        <button type="button" class="lh-sf-b lh-sf-pg" :disabled="page <= 1" :aria-label="'上一页'" title="上一页" @click="pagePrev"><ChevronLeft :size="14" /></button>
        <span class="lh-sf-page-txt">第 {{ page }} / {{ pageCount }} 页 · 共 {{ arrayValue.length }} 条</span>
        <button type="button" class="lh-sf-b lh-sf-pg" :disabled="page >= pageCount" :aria-label="'下一页'" title="下一页" @click="pageNext"><ChevronRight :size="14" /></button>
      </div>
    </fieldset>

    <div v-else class="lh-sf-scalar">
      <RepairFieldControl v-if="isRepairField" :input-id="props.id" :field="repairFieldMeta" :label="field.label"
        :required="!!field.required" :disabled="disabled" :compact="true" :percentage="isRepairPercentage"
        :placeholder="field.placeholder || ''"
        :select-options="field.select_options ?? null" :allow-custom-select="!!field.allow_custom_select" :menu-z-index="10010"
        :model-value="repairDraftValue" @update:model-value="emitUpdate" />
      <template v-else>
        <component :is="isChoiceGroup ? 'span' : 'label'" class="lh-sf-label" :for="isChoiceGroup ? undefined : props.id">{{ field.label || '' }}<span v-if="field.required" class="lh-sf-req" aria-hidden="true">*</span></component>
        <div v-if="field.toggle_zero" class="lh-sf-delay"><label><input type="checkbox" :checked="delayEnabled" :disabled="disabled" :aria-label="'开启' + field.label" @change="toggleDelay" />开启</label><input :id="props.id" class="lh-sf-input" type="number" :value="delayEnabled ? scalarBind : ''" :disabled="disabled || !delayEnabled" :required="delayEnabled" min="1" :max="field.max" step="1" :aria-label="'延时分钟数'" @input="onScalar($event, field)" /><span>分钟</span></div>
        <VnetSelect v-else-if="field.person_picker" :input-id="props.id" :label="field.label" :model-value="personLabel"
          :options="options.map(option => option.label)" :disabled="disabled" :required="!!field.required" :menu-z-index="10010" @update:model-value="selectPerson" />
        <fieldset v-else-if="field.choice_group" class="lh-sf-choices" :class="{ 'lh-sf-photo-choices': field.photo_choices }" :disabled="disabled" :aria-label="field.label">
          <label v-for="option in options" :key="option.value"><a v-if="field.photo_choices && waterPhotoUrl(option.url)" :href="waterPhotoUrl(option.url).replace('variant=thumb', 'variant=original')" target="_blank" rel="noopener" :aria-label="'查看原图：' + option.label"><img :src="waterPhotoUrl(option.url)" alt="" loading="lazy" /></a><input type="checkbox" :name="props.id" :aria-label="option.label" :checked="arrayValue.includes(option.value)" @change="toggleChoice(option.value, ($event.target as HTMLInputElement).checked)" /><span>{{ option.label }}</span></label>
          <span v-if="!options.length" class="lh-sf-empty">尚无选项</span>
        </fieldset>
        <fieldset v-else-if="isSelect && !isMultiSelect && smallSingleChoice(options)" class="lh-sf-single" :disabled="disabled" :aria-label="field.label"><label v-for="option in options" :key="String(option.value)" :class="{ selected: String(modelValue) === String(option.value) }"><input type="radio" :name="props.id" :checked="String(modelValue) === String(option.value)" @change="emitUpdate(option.value)" /><span>{{ option.label }}</span></label></fieldset>
        <VnetSelect v-else-if="isSelect && !isMultiSelect && !options.some(option => option.disabled)" :input-id="props.id" :label="field.label" :model-value="selectedLabel(options, modelValue)" :options="labelledOptions(options).map(option => option.label)" :disabled="disabled" :required="!!field.required" :menu-z-index="10010" @update:model-value="selectOptionLabel" />
        <select v-else-if="isSelect" class="lh-sf-input" :id="props.id" :disabled="disabled" :required="!!field.required"
          :aria-label="field.label" :multiple="isMultiSelect ? true : undefined"
          v-model="selectProxy">
          <option v-if="!isMultiSelect" :value="undefined" :disabled="!!field.required" label="请选择"></option>
          <option v-for="(o, oi) in options" :key="oi" :value="o.value" :disabled="o.disabled">{{ o.label != null ? o.label : String(o.value) }}</option>
        </select>
        <input v-else-if="isBool" type="checkbox" class="lh-sf-check" :id="props.id" :checked="!!modelValue"
          :disabled="disabled" :aria-label="field.label" @change="onScalar($event, field)" />
        <textarea v-else-if="jsonMode" class="lh-sf-input lh-sf-area" :id="props.id" :value="jsonText" rows="4"
          :disabled="disabled" :required="!!field.required" :aria-label="field.label" @input="jsonInput" />
        <textarea v-else-if="isTextarea" class="lh-sf-input lh-sf-area" :id="props.id" :value="scalarBind" rows="2"
          :maxlength="field.maxlength" :disabled="disabled" :required="!!field.required" :aria-label="field.label"
          @input="onScalar($event, field)" />
        <input v-else class="lh-sf-input" :id="props.id" :type="nativeType" :value="scalarBind" :min="field.min" :max="field.max"
          :step="field.step" :maxlength="field.maxlength" :disabled="disabled" :required="!!field.required"
          :list="field.suggestions?.length ? props.id + '-suggestions' : undefined"
          :aria-label="field.label" @input="onScalar($event, field)" />
        <datalist v-if="field.suggestions?.length" :id="props.id + '-suggestions'"><option v-for="value in field.suggestions" :key="value" :value="value" /></datalist>
        <span v-if="jsonMode && jsonError" class="lh-sf-error" role="alert">{{ jsonError }}</span>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight, Plus, Trash2 } from "lucide-vue-next";
import RepairFieldControl from "./RepairFieldControl.vue";
import RepairPeoplePicker from "./RepairPeoplePicker.vue";
import VnetSelect from "./VnetSelect.vue";
import { labelledOptions, selectedLabel, selectedValue, smallSingleChoice } from '../lighthouseSelect';
import PlanConvergenceRules from "./PlanConvergenceRules.vue";
import CabinetBatchTextFill from "./CabinetBatchTextFill.vue";
import LighthouseCabinetTextCreate from "./LighthouseCabinetTextCreate.vue";
import LighthouseCabinetProof from "./LighthouseCabinetProof.vue";
import LighthouseNoticeSop from "./LighthouseNoticeSop.vue";
import { repairDraftInputValue, repairFieldValueToText, resolveRepairDeviceCatalog, repairModelsForBrand, repairDeviceDependentPatch,
  REPAIR_SUPPLIER_FIELDS, REPAIR_SPARE_PART_FIELDS, repairFollowupFieldDisabled, repairFollowupFieldPlaceholder } from "../repairManagementUtils";

const props = defineProps<{ field: Record<string, any>; modelValue?: any; disabled?: boolean; id: string; context?: Record<string, any>[]; stepLimit?: number; minStepId?: string; planId?: string; planVersion?: number }>();
const emit = defineEmits<{ (e: "update:modelValue", value: any): void; (e: "load-options", value: { scope: string; q?: string }): void }>();
const controlRoot = ref<HTMLElement | null>(null);
function hasInvalidDraft(): boolean {
  const invalid = Array.from(controlRoot.value?.querySelectorAll<HTMLTextAreaElement>('textarea:invalid') || []).find(input => input.validity.customError);
  if (invalid) invalid.focus();
  return Boolean(invalid);
}

const field = computed(() => props.field);
function waterPhotoUrl(value: unknown): string {
  return typeof value === 'string' && /^\/api\/capacity\/water\/images\/[A-Za-z0-9_-]+\?scope=[ABCDEH]&variant=thumb$/.test(value) ? value : '';
}
const searchText = ref('');
const hasSparePartFields = computed(() => field.value.native_repair_followup && field.value.children?.some((child: Record<string, any>) => REPAIR_SPARE_PART_FIELDS.has(child.path)));
const hasSparePartValues = computed(() => (field.value.children || []).some((child: Record<string, any>) => REPAIR_SPARE_PART_FIELDS.has(child.path) && repairFieldValueToText(props.modelValue?.[child.path]).trim()));
const sparePartsEnabled = ref(Boolean(hasSparePartValues.value));
watch(hasSparePartValues, filled => { if (filled) sparePartsEnabled.value = true; });
function toggleSpareParts(event: Event): void {
  if (props.disabled) return;
  sparePartsEnabled.value = (event.target as HTMLInputElement).checked;
  if (sparePartsEnabled.value) return;
  const value = { ...(props.modelValue || {}) };
  for (const child of field.value.children || []) if (!child.read_only && REPAIR_SPARE_PART_FIELDS.has(child.path)) value[child.path] = '';
  emitUpdate(value);
}
const objectChildren = computed(() => (field.value.children || []).map((child: Record<string, any>) => {
  if (field.value.native_repair_followup) child = { ...child,
    read_only: child.read_only || repairFollowupFieldDisabled(child.path, props.modelValue || {}),
    placeholder: repairFollowupFieldPlaceholder(child.path, props.modelValue || {}),
    hidden: child.hidden || !sparePartsEnabled.value && REPAIR_SPARE_PART_FIELDS.has(child.path) };
  const catalog = field.value.repair_catalog;
  if (!catalog || !['设备品牌', '设备型号'].includes(child.path)) return child;
  const device = repairFieldValueToText(props.modelValue?.['设备名称']);
  const matched = resolveRepairDeviceCatalog(device, catalog.devices);
  const options = child.path === '设备型号' ? repairModelsForBrand(repairFieldValueToText(props.modelValue?.['设备品牌']), device, catalog.devices, catalog.brands)
    : matched.matched ? Object.keys(matched.brandModels) : child.repair_field?.options || [];
  return { ...child, select_options: options };
}).filter((child: Record<string, any>) => !child.hidden && (!child.when ||
  ('equals' in child.when ? childValue({ path: child.when.path }) === child.when.equals : childValue({ path: child.when.path }) !== child.when.not_equals)) &&
  (!field.value.searchable || `${child.label || ''} ${child.path || ''}`.toLocaleLowerCase().includes(searchText.value.trim().toLocaleLowerCase()))));
const visibleChildren = computed(() => field.value.paginated ? objectChildren.value.slice((page.value - 1) * pageSize.value, page.value * pageSize.value) : objectChildren.value);
const isObject = computed(() => field.value?.type === "object");
const isArray = computed(() => field.value?.type === "array");
const isBool = computed(() => field.value && (field.value.type === "boolean" || field.value.type === "checkbox"));
const isTextarea = computed(() => field.value?.type === "textarea");
const jsonMode = computed(() => !!field.value && (field.value.type === "json" || field.value.value_format === "json"));
const options = computed<Record<string, any>[]>(() => field.value.options_source === 'sop_steps'
  ? (props.context || []).slice(0, props.stepLimit == null ? undefined : props.stepLimit + 1)
      .map((step, index) => ({ value: step.step_id, label: `第${index + 1}步 · ${String(step.content || '').slice(0, 60)}` }))
      .filter((option, index) => option.value && (!props.minStepId || index >= (props.context || []).findIndex(step => step.step_id === props.minStepId)))
  : field.value.options_source === 'drill_participants'
    ? [{ value: '', label: '未选择' }, ...(props.context || []).filter(person => person.record_id).map(person => ({ value: person.record_id, label: person.name || '参演人' }))]
    : (Array.isArray(field.value?.options) ? field.value.options : []));
function toggleChoice(value: string, checked: boolean): void { emitUpdate(checked ? [...new Set([...arrayValue.value, value])] : arrayValue.value.filter(item => item !== value)); }
const personLabel = computed(() => options.value.find(option => option.value === props.modelValue?.record_id)?.label || '');
function selectPerson(label: string): void { emitUpdate(options.value.find(option => option.label === label)?.person || {}); }
const isSelect = computed(() => field.value.type === 'select' || field.value.type === 'multiselect' || options.value.length > 0);
const isMultiSelect = computed(() => options.value.length > 0 && (field.value.type === "multiselect" || field.value.multiple === true));
const isChoiceGroup = computed(() => !field.value.toggle_zero && !field.value.person_picker &&
  (field.value.choice_group || (isSelect.value && !isMultiSelect.value && smallSingleChoice(options.value))));
const nativeType = computed(() => {
  const t = field.value?.type;
  if (t === "integer" || t === "number") return "number";
  if (["date", "time", "datetime-local", "month"].includes(t)) return t;
  return "text";
});
const scalarBind = computed(() => (props.modelValue == null ? "" : String(props.modelValue)));
const isRepairField = computed(() => Boolean(field.value?.repair_field));
const repairFieldMeta = computed<Record<string, any>>(() => field.value?.repair_field || {});
const isRepairPercentage = computed(() => field.value?.percentage === true);
const repairDraftValue = computed(() => repairDraftInputValue(field.value?.repair_field || {}, props.modelValue));
const arrayValue = computed(() => (Array.isArray(props.modelValue) ? props.modelValue : []));
const repairPeople = computed(() => {
  const value = props.modelValue;
  const people = Array.isArray(value) ? value : value && typeof value === 'object'
    ? Array.isArray(value.users) ? value.users : Array.isArray(value.value) ? value.value : [value] : [];
  return people.filter((person: unknown) => person && typeof person === 'object' && !Array.isArray(person));
});
function updateRepairPeople(people: Record<string, any>[]): void {
  emitUpdate(people.map(person => ({ id: String(person.user_id || person.id || person.open_id || '').trim(),
    name: String(person.name || '已选人员'), employee_no: String(person.employee_no || '') })).filter(person => person.id));
}
const activeContext = computed(() => field.value.context_source === 'sop_steps' ? arrayValue.value
  : field.value.context_source === 'drill_execution' ? drillParticipants(props.modelValue || {}) : props.context);
function drillParticipants(execution: Record<string, any>): Record<string, any>[] {
  const people = [execution.commander || {}, ...(execution.participants || [])].filter(person => person.record_id);
  return Array.from(new Map(people.map(person => [person.record_id, person])).values());
}
const delayEnabled = computed(() => Number(props.modelValue) > 0);
const lastDelay = ref(Number(props.modelValue) || field.value.toggle_default || 5);
watch(() => props.modelValue, value => { if (Number(value) > 0) lastDelay.value = Number(value); });
function toggleDelay(event: Event): void { emitUpdate((event.target as HTMLInputElement).checked ? lastDelay.value : 0); }
const PAGE_SIZE = 10;
const pageSize = computed(() => field.value.native_cabinet_edit ? 1 : PAGE_SIZE);
const page = ref(1);
const pageCount = computed(() => Math.max(1, Math.ceil((isObject.value ? objectChildren.value.length : arrayValue.value.length) / pageSize.value)));
const pageIndexes = computed(() => {
  if (!arrayValue.value.length) return [] as number[];
  const start = (page.value - 1) * PAGE_SIZE;
  const out: number[] = [];
  for (let i = start; i < Math.min(start + PAGE_SIZE, arrayValue.value.length); i++) out.push(i);
  return out;
});
function clampPage(): void {
  page.value = Math.min(Math.max(1, page.value), pageCount.value);
}
function pagePrev(): void { if (!hasInvalidDraft() && page.value > 1) page.value -= 1; }
function pageNext(): void { if (!hasInvalidDraft() && page.value < pageCount.value) page.value += 1; }
const canAdd = computed(() => !props.disabled && (field.value.maxItems == null || arrayValue.value.length < field.value.maxItems));
const fixedSize = computed(() => field.value.minItems != null && field.value.minItems === field.value.maxItems);
const canRemove = computed(() => !props.disabled && (field.value.minItems == null || arrayValue.value.length > field.value.minItems));
function fixedCommander(index: number): boolean { return Boolean(field.value.commander_first && index === 0 && arrayValue.value[index]?.record_id && arrayValue.value[index].record_id === props.context?.[0]?.record_id); }
const entryField = computed(() => {
  const it = field.value?.item;
  return it ? { ...it } : { label: "", type: "text" };
});
const selectProxy = computed({
  get: () => props.modelValue,
  set: (v: any) => emit("update:modelValue", v),
});
function selectOptionLabel(label: string): void {
  const value = selectedValue(options.value, label);
  if (!props.disabled && value !== undefined) emitUpdate(value);
}

function getByPath(obj: any, path: string): any {
  if (!obj || typeof obj !== "object") return undefined;
  let cur = obj;
  for (const p of String(path).split(".")) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}
function setPath(root: Record<string, any>, path: string, value: any): Record<string, any> {
  const parts = String(path).split(".");
  const head = parts[0];
  if (parts.length === 1) return { ...root, [head]: value };
  const cur = root[head];
  const base = cur && typeof cur === "object" && !Array.isArray(cur) ? cur : {};
  return { ...root, [head]: setPath(base, parts.slice(1).join("."), value) };
}
function emitUpdate(v: any): void { emit("update:modelValue", v); }
function coerce(f: Record<string, any>, v: any): any {
  if (f.type === "integer") { const n = Number(v); return v == null || v === "" || Number.isNaN(n) ? "" : Math.trunc(n); }
  if (f.type === "number") { const n = Number(v); return v == null || v === "" || Number.isNaN(n) ? "" : n; }
  if (f.type === "boolean" || f.type === "checkbox") return !!v;
  return v;
}
function onScalar(ev: Event, f: Record<string, any>): void {
  const el = ev.target as HTMLInputElement | HTMLTextAreaElement;
  const isCheck = f.type === "boolean" || f.type === "checkbox";
  emitUpdate(coerce(f, isCheck ? (el as HTMLInputElement).checked : el.value));
}
function isLiteralKey(child: Record<string, any>): boolean {
  return !!(child && child.literal_key === true);
}
function childValue(child: Record<string, any>): any {
  const cur = props.modelValue && typeof props.modelValue === "object" && !Array.isArray(props.modelValue) ? props.modelValue : {};
  if (isLiteralKey(child)) return cur[child.path || ""];
  return getByPath(cur, child.path || "");
}
function onChild(child: Record<string, any>, v: any): void {
  if (props.disabled || child.read_only) return;
  const src = props.modelValue && typeof props.modelValue === "object" && !Array.isArray(props.modelValue) ? { ...props.modelValue } : {};
  const key = child.path || "";
  if (field.value.native_repair) {
    src[key] = v;
    const editable = new Set((field.value.children || []).map((item: Record<string, any>) => item.path));
    const patch = field.value.repair_catalog ? repairDeviceDependentPatch(src, key, field.value.repair_catalog.devices, field.value.repair_catalog.brands) : {};
    if (field.value.native_repair_followup && key === '维修方' && String(v || '').trim() === '我方') {
      for (const name of REPAIR_SUPPLIER_FIELDS) patch[name] = '';
    }
    emitUpdate({ ...src, ...Object.fromEntries(Object.entries(patch).filter(([name]) => editable.has(name))) });
    return;
  }
  if (isLiteralKey(child)) { src[key] = v; emitUpdate(src); return; }
  if (field.value.native_drill && ['commander', 'participants'].includes(key)) {
    const oldCommander = src.commander?.record_id;
    src[key] = v;
    if (key === 'commander') src.participants = (src.participants || []).filter((person: Record<string, any>) => ![oldCommander, v?.record_id].includes(person.record_id));
    src.participants = [...drillParticipants(src), ...(src.participants || []).filter((person: Record<string, any>) => !person.record_id)];
    const allowed = new Set(src.participants.map((person: Record<string, any>) => person.record_id));
    src.step_signers = Object.fromEntries(Object.entries(src.step_signers || {}).map(([row, people]) => [row, (people as string[]).map(person => allowed.has(person) ? person : '')]));
    emitUpdate(src);
    return;
  }
  emitUpdate(setPath(src, key, v));
}
function onItem(i: number, v: any): void {
  const src = Array.isArray(props.modelValue) ? props.modelValue.slice() : [];
  src[i] = v;
  emitUpdate(src);
}
function moveItem(index: number, delta: number): void {
  if (props.disabled || hasInvalidDraft() || !arrayValue.value[index + delta]) return;
  const values = [...arrayValue.value];
  [values[index], values[index + delta]] = [values[index + delta], values[index]];
  emitUpdate(values);
  page.value = Math.floor((index + delta) / PAGE_SIZE) + 1;
}
function initObject(m: Record<string, any>): Record<string, any> {
  const o: Record<string, any> = {};
  for (const child of Array.isArray(m.children) ? m.children : []) {
    if (child.generate === 'uuid') o[child.path] = globalThis.crypto?.randomUUID?.().replace(/-/g, '') || `step_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
    else if ('initial' in child) o[child.path] = JSON.parse(JSON.stringify(child.initial));
    else if (child.type === "boolean" || child.type === "checkbox") o[child.path] = false;
    else if (child.type === "object") {
      const sub = initObject(child);
      if (Object.keys(sub).length) o[child.path] = sub;
    }
  }
  return o;
}
function emptyFor(m: Record<string, any>): any {
  if (!m) return "";
  if (m.person_picker) return {};
  if (m.type === "object") return initObject(m);
  if (m.type === "array") return [];
  if (m.type === "boolean" || m.type === "checkbox") return false;
  return "";
}
function addItem(): void {
  if (props.disabled || hasInvalidDraft()) return;
  const max = field.value.maxItems;
  if (max != null && arrayValue.value.length >= max) return;
  const src = Array.isArray(props.modelValue) ? props.modelValue.slice() : [];
  src.push(emptyFor(field.value.item));
  emitUpdate(src);
  page.value = Math.max(1, Math.ceil(src.length / PAGE_SIZE)); // 跳转到新条目所在页
}
function removeItem(i: number): void {
  if (props.disabled || hasInvalidDraft()) return;
  const min = field.value.minItems;
  if (min != null && arrayValue.value.length <= min) return;
  const src = Array.isArray(props.modelValue) ? props.modelValue.slice() : [];
  const removedId = src[i]?.step_id;
  src.splice(i, 1);
  emitUpdate(field.value.context_source === 'sop_steps' && removedId
    ? src.map(step => ({ ...step, repeat_rules: (step.repeat_rules || []).filter((rule: Record<string, any>) => rule.from_step_id !== removedId && rule.to_step_id !== removedId) })) : src);
  clampPage(); // 删除后夹紧页码
}
const jsonText = ref("");
const jsonError = ref("");
function jsonDisplay(v: any): string {
  if (v == null || v === "") return "";
  if (typeof v === "string") return v;
  try { return JSON.stringify(v, null, 2); } catch { return String(v); }
}
function syncJson(): void {
  if (!jsonMode.value) return;
  const next = jsonDisplay(props.modelValue);
  // 成功 emit 回流时 JSON 内容已一致:不重置本地文本,避免改写用户输入/光标。
  if (!jsonContentEqual(next, jsonText.value)) jsonText.value = next;
  jsonError.value = "";
}
function jsonContentEqual(a: string, b: string): boolean {
  try { return JSON.stringify(JSON.parse(a || "null")) === JSON.stringify(JSON.parse(b || "null")); }
  catch { return a === b; }
}
watch(() => props.modelValue, syncJson);
watch(() => arrayValue.value.length, () => clampPage());
onMounted(syncJson);
function jsonInput(ev: Event): void {
  if (!jsonMode.value) return;
  const el = ev.target as HTMLTextAreaElement;
  const text = el.value;
  jsonText.value = text;
  el.setCustomValidity("");
  const t = text.trim();
  if (t === "") {
    jsonError.value = "";
    if (!field.value.required) emitUpdate(null);
    return; // 必填为空交给原生 required,不 emit
  }
  try {
    emitUpdate(JSON.parse(t));
    jsonError.value = "";
  } catch {
    jsonError.value = "格式无效";
    el.setCustomValidity("格式无效"); // 阻止 form submit,保留文本不 emit
  }
}
</script>

<style scoped>
.lh-sf-single { display: flex; flex-wrap: wrap; gap: 7px 16px; padding: 0; margin: 0; border: 0; }
.lh-sf-single label { display: inline-flex; align-items: center; gap: 6px; padding: 5px 0; cursor: pointer; }
.lh-sf-single input { width: 15px; height: 15px; margin: 0; accent-color: var(--lh-accent, #255b98); }
.lh-sf-single label.selected { color: var(--lh-accent, #255b98); font-weight: 600; }
.lh-sf { display: flex; flex-direction: column; gap: 6px; min-width: 0; max-width: 100%; }
.lh-sf :deep(.repair-people-popover) { position: static; margin-top: 6px; max-height: 240px; }
.lh-sf :deep(.repair-people-results) { max-height: 238px; }
.lh-sf-fs { border: 1px solid var(--lh-border, #d7dee8); border-radius: 6px; padding: 6px 8px 8px; margin: 0; min-width: 0; }
.lh-sf-g { padding: 0 4px; font-size: 12px; font-weight: 600; color: var(--lh-muted, #5a6a7d); max-width: 100%; box-sizing: border-box; overflow-wrap: anywhere; }
.lh-sf-search { margin-top: 6px; }
.lh-sf-spare { display: flex; align-items: center; gap: 7px; margin: 6px 0; font-size: 12px; }
.lh-sf-spare input { width: 16px; height: 16px; accent-color: var(--lh-accent); }
.lh-sf-ahead { display: flex; align-items: center; justify-content: space-between; gap: 8px; width: 100%; }
.lh-sf-c { margin-top: 6px; }
@media (min-width: 800px) { .cabinet-row-editor > .lh-sf-fs { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 12px; } }
.drill-config-editor :deep(.lh-sf-fs), .drill-config-editor > .lh-sf-fs { border: 0; border-radius: 0; padding: 4px 0; }
.drill-config-editor > .lh-sf-fs > .lh-sf-c.lh-sf-wide { border-top: 1px solid var(--lh-border); margin-top: 10px; padding-top: 10px; }
@media (min-width: 800px) {
  .compact-fields > .lh-sf-fs { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 12px; }
  .compact-fields > .lh-sf-fs > .lh-sf-wide { grid-column: 1 / -1; }
}
.lh-sf-list { list-style: none; margin: 8px 0 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.lh-sf-entry { display: flex; align-items: flex-start; gap: 6px; padding-left: 8px; border-left: 2px solid var(--lh-accent-soft, #e3ecfb); }
.lh-sf-idx { flex: 0 0 auto; min-width: 18px; text-align: right; font-size: 11px; line-height: 24px; color: var(--lh-faint-muted, #93a1b3); }
.lh-sf-entry > .lh-sf { flex: 1 1 auto; min-width: 0; }
.lh-sf-empty { margin: 6px 0 0; font-size: 12px; color: var(--lh-faint-muted, #93a1b3); }
.lh-sf-scalar { display: flex; flex-direction: column; gap: 3px; min-width: 0; }
.lh-sf-choices { display: grid; gap: 6px; margin: 0; padding: 8px; min-width: 0; border: 1px solid var(--lh-border); border-radius: 6px; max-height: 240px; overflow: auto; }
.lh-sf-choices label { display: flex; align-items: flex-start; gap: 7px; font-size: 12px; line-height: 1.5; }
.lh-sf-choices span { white-space: pre-wrap; overflow-wrap: anywhere; min-width: 0; }
.lh-sf-choices input { width: auto; flex: 0 0 auto; margin-top: 3px; }
.lh-sf-photo-choices { grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); }
.lh-sf-photo-choices label { align-items: center; }
.lh-sf-photo-choices a { flex: 0 0 48px; }
.lh-sf-photo-choices img { display: block; width: 48px; height: 48px; object-fit: contain; border-radius: 4px; }
.lh-sf :deep(.repair-field-label), .lh-sf :deep(.repair-people-label) { color: var(--lh-charcoal, #334155); }
.lh-sf-delay { display: flex; align-items: center; gap: 8px; }
.lh-sf-delay label { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
.lh-sf-delay input[type=number] { min-width: 0; width: 90px; }
.lh-sf-delay input[type=checkbox] { accent-color: var(--lh-accent); }
.lh-sf-label { font-size: 12px; font-weight: 500; color: var(--lh-charcoal, #334155); overflow-wrap: anywhere; }
.lh-sf-req { color: var(--lh-danger, #d03030); font-weight: 600; }
.lh-sf-input { width: 100%; box-sizing: border-box; font-size: 12px; line-height: 1.4; padding: 4px 6px; color: var(--lh-charcoal, #334155); background: var(--lh-surface, #fff); border: 1px solid var(--lh-input-border, #c8d2e0); border-radius: 5px; }
.lh-sf-input:focus { outline: 2px solid var(--lh-accent-ring, #5b9bf5); }
.lh-sf-input:disabled { background: var(--lh-surface-subtle, #f3f6fa); color: var(--lh-muted, #5a6a7d); cursor: not-allowed; }
.lh-sf-area { resize: vertical; }
.lh-sf-check { width: 16px; height: 16px; accent-color: var(--lh-accent, #2f6fed); }
.lh-sf-b { display: inline-flex; align-items: center; justify-content: center; padding: 2px; border: 1px solid transparent; border-radius: 5px; background: transparent; color: var(--lh-accent, #2f6fed); cursor: pointer; }
.lh-sf-b:hover:not(:disabled) { background: var(--lh-surface-hover, #eef3fc); border-color: var(--lh-border, #d7dee8); }
.lh-sf-b:disabled { color: var(--lh-faint-muted, #b6c1ce); cursor: not-allowed; }
.lh-sf-rm { color: var(--lh-warn, #b45309); }
.lh-sf-page { display: flex; align-items: center; justify-content: flex-end; gap: 8px; margin-top: 8px; min-width: 0; flex-wrap: wrap; }
.lh-sf-pg { border: 1px solid var(--lh-border, #d7dee8); border-radius: 5px; background: var(--lh-surface, #fff); }
.lh-sf-pg:hover:not(:disabled) { background: var(--lh-surface-hover, #eef3fc); }
.lh-sf-page-txt { font-size: 12px; color: var(--lh-muted, #5a6a7d); white-space: nowrap; }
.lh-sf-error { font-size: 11px; color: var(--lh-danger, #d03030); }
</style>
