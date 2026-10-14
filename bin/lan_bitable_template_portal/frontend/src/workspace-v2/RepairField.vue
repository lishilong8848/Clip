<template>
  <NFormItem :label="label || String(field.field_name || '')" :label-props="{for:percentage&&disabled?undefined:inputId}" :required="required" :validation-status="error ? 'error' : undefined" :feedback="error || undefined" :show-feedback="Boolean(error)" :class="{ 'v2-repair-field-wide': wide }">
    <div v-if="percentage" class="v2-repair-progress-input">
      <span v-if="disabled&&!filled" class="v2-repair-muted">未填写</span><NProgress v-else-if="disabled" type="line" :percentage="percent" :height="7" :show-indicator="true" :status="percent===100?'success':'default'" />
      <template v-else><NSlider :value="percent" :min="0" :max="100" :step="1" :aria-label="label || String(field.field_name)" @update:value="percentageInput" /><NInputNumber :value="filled?percent:null" placeholder="未填写" :min="0" :max="100" :precision="0" :input-props="inputProps" @update:value="percentageInput"><template #suffix>%</template></NInputNumber></template>
    </div>
    <NSelect v-else-if="selectable" :value="modelValue || null" :options="options" :disabled="disabled" :tag="allowCustomSelect" filterable clearable :placeholder="placeholder || '请选择'" :input-props="inputProps" @update:value="update($event || '')" />
    <NDatePicker v-else-if="date" v-control-label="inputProps" :formatted-value="modelValue?.replace('T',' ') || null" type="datetime" value-format="yyyy-MM-dd HH:mm" format="yyyy-MM-dd HH:mm" :disabled="disabled" :clearable="!required" @update:formatted-value="update($event?.replace(' ','T') || '')" />
    <NInputNumber v-else-if="number" :value="numericValue" :min="numberMin" :max="numberMax" :step="typeof numberStep==='number'?numberStep:1" :disabled="disabled" :input-props="inputProps" @update:value="update($event===null?'':String($event))" />
    <NInput v-else :value="modelValue || ''" :type="textarea?'textarea':'text'" :autosize="textarea ? {minRows:2,maxRows:5} : undefined" :disabled="disabled" :placeholder="placeholder || (disabled?'未填写':'请输入')" :input-props="inputProps" @update:value="update" />
  </NFormItem>
</template>
<script setup lang="ts">
import { computed, type Directive } from 'vue';
import { NDatePicker, NFormItem, NInput, NInputNumber, NProgress, NSelect, NSlider } from 'naive-ui';
import { repairFieldUsesTextarea } from '../repairManagementUtils';
import type { LooseDict } from '../types';
const props=withDefaults(defineProps<{field:LooseDict;modelValue?:string;inputId:string;label?:string;required?:boolean;error?:string;disabled?:boolean;wide?:boolean;percentage?:boolean;selectOptions?:string[]|null;allowCustomSelect?:boolean;placeholder?:string;numberMin?:number;numberMax?:number;numberStep?:number|string}>(),{modelValue:'',label:'',error:'',selectOptions:null});
const emit=defineEmits<{'update:modelValue':[value:string];edited:[]}>();
const options=computed(()=>{const source=props.selectOptions ?? props.field.options;return Array.isArray(source)?source.map((value:unknown)=>String(value??'').trim()).filter(Boolean).map(value=>({label:value,value})):[];});
const selectable=computed(()=>props.selectOptions!==null||options.value.length>0);
const date=computed(()=>Number(props.field.field_type)===5||String(props.field.ui_type||'').toLowerCase().includes('datetime'));
const number=computed(()=>Number(props.field.field_type)===2||['number','progress'].includes(String(props.field.ui_type||'').toLowerCase()));
const textarea=computed(()=>repairFieldUsesTextarea(props.field.field_name));
const numericValue=computed(()=>props.modelValue?.trim() && Number.isFinite(Number(props.modelValue))?Number(props.modelValue):null);
const filled=computed(()=>Boolean(props.modelValue?.trim()));
const percent=computed(()=>{const value=Number(String(props.modelValue ?? '').replace(/[％%]/g,''));return Number.isFinite(value)?Math.max(0,Math.min(100,Math.round(value<=1?value*100:value))):0;});
const inputProps=computed(()=>({id:props.inputId,'aria-label':props.label || String(props.field.field_name||''),'aria-invalid':Boolean(props.error)}));
// DatePicker does not expose native input attributes; keep the visible label linked.
const labelControl=(element:HTMLElement,values:Record<string,unknown>)=>{const input=element.querySelector('input');if(input)for(const [key,value] of Object.entries(values))input.setAttribute(key,String(value));};
const vControlLabel:Directive<HTMLElement,Record<string,unknown>>={mounted:(el,binding)=>labelControl(el,binding.value),updated:(el,binding)=>labelControl(el,binding.value)};
function update(value:string){emit('update:modelValue',value);emit('edited');}
function percentageInput(value:number|null){update(value===null?'':String(value/100));}
</script>
<style scoped>
.v2-repair-field-wide{grid-column:1/-1}.v2-repair-progress-input{display:flex;align-items:center;gap:18px;width:100%}.v2-repair-progress-input>.n-slider{flex:1}.v2-repair-progress-input>.n-input-number{width:106px}.n-date-picker,.n-input-number{width:100%}
</style>
