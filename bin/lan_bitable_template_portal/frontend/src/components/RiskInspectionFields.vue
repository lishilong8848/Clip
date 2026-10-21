<template>
  <div class="risk-fields" :class="{'risk-fields-cell':cell}">
    <label v-for="field in visibleFields" :key="field.key" class="risk-field" :class="{'wide':field.type===17,'risk-field-system':field.role!=='input','risk-field-invalid':invalid(field.key)}" :data-risk-field="field.key">
      <span :class="{'risk-cell-label':cell}">{{field.field_name}} <b v-if="field.role==='input'&&field.required" aria-label="必填">*</b><small v-if="field.role!=='input'"> · 系统填写</small></span>
      <div v-if="field.role!=='input'" class="risk-readonly" role="status" :aria-label="field.field_name+'（系统填写）'">{{riskSystemText(field,systemValues)}}</div>
      <template v-else-if="field.type===17">
        <div class="risk-file-zone" :tabindex="disabled||prefill?undefined:0" v-bind="inputAria(field)" @dragover.prevent @drop.prevent="drop($event,field.key)" @paste="paste($event,field.key)">
          <span v-if="!disabled&&!prefill" class="risk-upload"><Upload :size="17" /><span>选择照片或文件</span><input type="file" multiple accept=".pdf,.doc,.docx,.xls,.xlsx,.xlsm,.md,.txt,.csv,.jpg,.jpeg,.png,.webp" :aria-label="'上传'+field.field_name" @change="pick($event,field.key)" /></span>
          <small v-if="!disabled&&!prefill">照片或文件，每个不超过20MB；可拖入或粘贴图片。</small>
          <small v-if="prefill">附件由排查人现场上传</small>
          <div v-for="fid in values[field.key]||[]" :key="fid" class="risk-file">
            <a :href="'/api/risk-inspections/files/'+fid" target="_blank" rel="noopener noreferrer">{{fileName(fid)}}</a>
            <NButton v-if="!disabled" text :aria-label="'移除'+fileName(fid)" @click="set(field.key,values[field.key].filter((v:string)=>v!==fid))"><X :size="15" /></NButton>
          </div>
          <span v-if="disabled&&!(values[field.key]||[]).length">未上传</span>
        </div>
      </template>
      <div v-else-if="disabled" class="risk-readonly">{{readable(field,values[field.key])}}</div>
      <NInputNumber v-else-if="field.type===2" :value="values[field.key]??null" :disabled="disabled" :status="invalid(field.key)?'error':undefined" :input-props="inputAria(field)" @update:value="v=>set(field.key,v)" />
      <NSelect :to="true" v-else-if="[3,4].includes(field.type)" :multiple="field.type===4" filterable clearable :value="values[field.key]??(field.type===4?[]:null)" :options="(field.property?.options||[]).map((v:Dict)=>({label:v.name,value:v.name}))" :disabled="disabled" :status="invalid(field.key)?'error':undefined" :input-props="inputAria(field)" v-bind="inputAria(field)" @update:value="v=>set(field.key,v)" />
      <NDatePicker :to="true" v-else-if="field.type===5" type="date" clearable :value="values[field.key]??null" :disabled="disabled" :status="invalid(field.key)?'error':undefined" :input-props="inputAria(field)" @update:value="v=>set(field.key,v)" />
      <NCheckbox v-else-if="field.type===7" :checked="values[field.key]===true" :disabled="disabled" v-bind="inputAria(field)" @update:checked="v=>set(field.key,v)">{{field.field_name}}</NCheckbox>
      <NInput v-else :value="values[field.key]||''" :disabled="disabled" :status="invalid(field.key)?'error':undefined" :type="field.type===1?'textarea':'text'" :autosize="{minRows:1,maxRows:5}" :maxlength="10000" :input-props="inputAria(field)" @update:value="v=>set(field.key,v)" />
      <small v-if="invalid(field.key)" :id="errorId(field.key)" class="risk-field-error">请{{field.type===17?'上传':'填写'}}{{field.field_name}}</small>
    </label>
  </div>
</template>
<script setup lang="ts">
import {computed,useId} from 'vue';
import {NButton,NInput,NInputNumber,NSelect,NDatePicker,NCheckbox} from 'naive-ui';
import {Upload,X} from 'lucide-vue-next';
import type {Dict} from '../api/client';
import {riskFieldRole,riskPresentationFields,riskSystemText} from './riskInspectionUi';
const props=defineProps<{fields:Dict[];values:Dict;systemValues?:Dict;files?:Dict[];disabled?:boolean;prefill?:boolean;cell?:boolean;invalidFields?:string[]}>();
const emit=defineEmits<{(e:'update:values',value:Dict):void;(e:'files',field:string,files:File[]):void}>();
const visibleFields=computed(()=>riskPresentationFields(props.fields).filter(f=>f.role==='input'&&!f.is_primary));
const id=useId(),errorId=(key:string)=>id+'-error-'+key;
const invalid=(key:string)=>!props.disabled&&props.invalidFields?.includes(key)===true;
const inputAria=(field:Dict)=>({'aria-label':field.field_name,'aria-required':field.required?true:undefined,'aria-invalid':invalid(field.key)?true:undefined,'aria-describedby':invalid(field.key)?errorId(field.key):undefined});
function set(key:string,value:any){if(!props.disabled&&props.fields.some(field=>field.key===key&&riskFieldRole(field)==='input'))emit('update:values',{...props.values,[key]:value});}
function fileName(id:string){return props.files?.find(f=>f.id===id)?.name||'排查附件';}
function readable(f:Dict,v:any){if(v===undefined||v===null||v===''||Array.isArray(v)&&!v.length)return '未填写';if(f.type===7)return v?'是':'否';if(f.type===5){const date=new Date(v);return Number.isFinite(date.getTime())?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai'}).format(date):'日期无效';}return Array.isArray(v)?v.join('、'):String(v);}
function send(field:string,files:File[]){if(!props.disabled&&!props.prefill&&files.length)emit('files',field,files);}
function pick(e:Event,key:string){const input=e.target as HTMLInputElement;send(key,Array.from(input.files||[]));input.value='';}
function drop(e:DragEvent,key:string){send(key,Array.from(e.dataTransfer?.files||[]));}
function paste(e:ClipboardEvent,key:string){const files=Array.from(e.clipboardData?.files||[]);if(files.length){e.preventDefault();send(key,files);}}
</script>
<style scoped>
.risk-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.risk-field{display:flex;flex-direction:column;gap:6px;min-width:0}.risk-field>span{font-weight:600;font-size:13px}.risk-field b{color:#d14343}.wide{grid-column:1/-1}.risk-file-zone{border:1px dashed var(--border-color,#c9d2dd);border-radius:6px;padding:12px;display:grid;gap:8px}.risk-file{display:flex;gap:8px;align-items:center;min-width:0}.risk-file a{overflow-wrap:anywhere;color:var(--primary-color,#2463d5)}.risk-file-zone input{max-width:100%}small{color:var(--text-color-3,#6b7280)}@media(max-width:600px){.risk-fields{grid-template-columns:1fr;gap:12px}}
</style>
<style scoped>
.risk-upload{position:relative;display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:40px;border:1px solid var(--border-color,#ccd5e0);border-radius:6px;cursor:pointer;width:fit-content;padding:0 12px}.risk-upload input{position:absolute;inset:0;opacity:0;width:100%;height:100%;cursor:pointer}.risk-upload:focus-within{outline:2px solid var(--primary-color,#2563eb);outline-offset:2px}.risk-readonly{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.7;min-height:32px;padding:5px 0}.risk-field:has(.risk-readonly){border-bottom:1px solid var(--border-color,#dce2e9);padding-bottom:6px}
.risk-fields-cell{display:block}.risk-fields-cell>.risk-field{gap:0}.risk-fields-cell .risk-cell-label{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}.risk-fields-cell .risk-file-zone{border:0;padding:6px 0}.risk-fields-cell :deep(.n-checkbox){min-height:34px}.risk-file-zone:focus-visible{outline:2px solid var(--primary-color,#2563eb);outline-offset:2px}
.risk-field-invalid .risk-file-zone{border-color:var(--error-color,#d14343)}.risk-field-error{color:var(--error-color,#d14343);font-size:12px;line-height:1.5}.risk-field{scroll-margin-block:24px}
@media(max-width:600px){.risk-fields:not(.risk-fields-cell){grid-template-columns:repeat(2,minmax(0,1fr));gap:10px 14px}.risk-fields:not(.risk-fields-cell)>.risk-field:not(.risk-field-system){grid-column:1/-1}.risk-field-system small{font-size:10px}.risk-field-system .risk-readonly{font-size:12px;line-height:1.5;min-height:24px;padding:2px 0}}
</style>
