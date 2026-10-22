<template>
  <div class="v2-notice-field-grid">
    <NFormItem v-for="field in fields" :key="field.key" :label="field.label" :required="field.required" :data-notice-field="field.key" :show-feedback="!!warnings[field.key]||!compact&&(invalid(field)||!!field.hint)" :validation-status="invalid(field)||warnings[field.key]?'error':undefined" :class="{'is-wide':field.type==='textarea'||field.key==='title','has-cycle-warning':!!warnings[field.key]}" class="v2-notice-field">
      <template v-if="compact" #label><span class="v2-notice-compact-label">{{ field.label }}<NPopover v-if="field.hint" trigger="click" style="max-width:360px"><template #trigger><NButton text size="tiny" data-v2-navigation :aria-label="field.label+'填写说明'"><Info :size="14" /></NButton></template>{{ field.hint }}</NPopover></span></template>
      <NDatePicker v-if="field.type==='datetime-local'" :formatted-value="String(values[field.key]||'').replace('T',' ')||null" type="datetime" value-format="yyyy-MM-dd HH:mm" format="yyyy-MM-dd HH:mm" clearable :disabled="locked||field.readonly" :input-props="inputProps(field)" @update:formatted-value="value=>values[field.key]=value||''" />
      <NSelect v-else-if="field.options.length" v-model:value="values[field.key]" :options="field.options" filterable :tag="field.type!=='select-one'" :disabled="locked||field.readonly" :aria-label="field.label" :input-props="inputProps(field)" />
      <NInputNumber v-else-if="field.type==='number'" :value="String(values[field.key]??'').trim()?Number(values[field.key]):null" :min="field.min" :max="field.max" :step="field.step" :disabled="locked||field.readonly" :input-props="inputProps(field)" @update:value="value=>values[field.key]=value===null?'':String(value)" />
      <NInput v-else v-model:value="values[field.key]" :type="field.type==='textarea'?'textarea':'text'" :autosize="compact?false:{minRows:2,maxRows:5}" :rows="compact?2:undefined" :disabled="locked||field.readonly" :input-props="inputProps(field)" />
      <span v-if="compact&&!warnings[field.key]&&(invalid(field)||field.hint)" :id="'v2-notice-help-'+field.key" class="v2-notice-field-description">{{ invalid(field)?'请填写'+field.label:field.hint }}</span>
      <template v-if="warnings[field.key]||!compact&&(invalid(field)||field.hint)" #feedback><span :id="'v2-notice-help-'+field.key" :data-cycle-warning="warnings[field.key]?field.key:undefined" role="status">{{ invalid(field)?'请填写'+field.label:warnings[field.key]||field.hint }}</span></template>
    </NFormItem>
    <slot />
  </div>
</template>
<script setup lang="ts">
import { computed } from 'vue';
import { NButton,NDatePicker,NFormItem,NInput,NInputNumber,NPopover,NSelect } from 'naive-ui';
import { Info } from 'lucide-vue-next';
import type { Dict } from './client';
import type { NoticeField } from './noticeForm';
import { noticeCycleWarnings, type MaintenanceCycleRule } from '../noticeCycleWarnings';
const props=defineProps<{fields:NoticeField[];values:Dict;locked:boolean;attempted:boolean;compact?:boolean;cycleRules?:MaintenanceCycleRule[]}>();
const warnings=computed<Record<string,string|undefined>>(()=>noticeCycleWarnings(props.values,props.cycleRules));
const invalid=(field:NoticeField)=>props.attempted&&field.required&&!String(props.values[field.key]??'').trim();
const inputProps=(field:NoticeField)=>({'aria-label':field.label,'aria-invalid':invalid(field),'aria-describedby':invalid(field)||warnings.value[field.key]||field.hint?'v2-notice-help-'+field.key:undefined});
</script>
<style>.v2-notice-field-grid .n-input-number{width:100%}.v2-notice-field{margin-bottom:16px}.v2-notice-compact-label{display:inline-flex;align-items:center;gap:6px}.v2-notice-field-description{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}@media(max-width:760px){.v2-notice-memory .n-button{min-height:44px}.v2-notice-facts-summary>div{grid-template-columns:80px minmax(0,1fr) 44px}.v2-notice-facts-summary .n-button{min-height:44px}}</style>
