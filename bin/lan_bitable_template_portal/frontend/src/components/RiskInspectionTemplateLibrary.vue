<template>
  <section class="risk-library" aria-label="历史排查模板库">
    <NAlert v-if="error&&!confirmOpen" type="error" :bordered="false" role="alert">{{error}}</NAlert>
    <NAlert v-if="message" type="info" :bordered="false">{{message}}</NAlert>
    <template v-if="!selected">
      <form class="history-toolbar" @submit.prevent="page=1;load()"><NInput v-model:value="search" clearable placeholder="搜索历史模板名称" :input-props="{'aria-label':'搜索历史模板名称'}" /><NButton attr-type="submit" :loading="loading"><Search :size="16" />查询</NButton></form>
      <NSpin :show="loading"><div class="history-list"><article v-for="row in rows" :key="row.id"><div><strong>{{row.name}}</strong><NTag size="small" :type="row.enabled?'success':'default'">{{row.enabled?'启用':'停用'}}</NTag><span>V{{row.revision}}</span></div><p>{{row.item_count}} 项排查 · {{row.field_count}} 个字段 · {{row.scopes.map(scopeLabel).join('、')}}</p><small>{{row.creator}} · {{date(row.updated)}}{{row.legacy?' · 原预填待核对':''}}</small><footer><NButton :disabled="loading" @click="open(row.id)"><Pencil :size="16" />查看 / 编辑</NButton><NButton type="primary" :disabled="!row.enabled||loading" @click="reuse(row.id)"><CopyPlus :size="16" />复用发布</NButton></footer></article></div><NEmpty v-if="!loading&&!rows.length" description="暂无可用历史模板；排查成功发布后会自动保存" /></NSpin>
      <NPagination v-if="total>20" :page="page" :page-count="Math.ceil(total/20)" :page-slot="5" @update:page="p=>{page=p;load()}" />
    </template>
    <template v-else>
      <header class="history-toolbar"><NButton :disabled="busy" @click="back"><ArrowLeft :size="16" />模板列表</NButton><span>V{{selected.revision}} · {{selected.creator}} · {{date(selected.created)}}</span><NButton type="primary" :disabled="busy||dirty||!selected.enabled" @click="reuse(selected.id)"><CopyPlus :size="16" />复用发布</NButton></header>
      <NAlert v-if="selected.legacy" type="warning" :bordered="false">此历史任务未保存发布时的原始预填内容，目前只恢复字段与事项标题，请核对并补充预填内容。</NAlert>
      <NAlert v-if="selected.sync_status?.error" type="warning" :bordered="false">模板已保存在灯塔，飞书备份尚未完成：{{selected.sync_status.error}} <NButton text :disabled="busy" @click="retry">重试备份</NButton></NAlert>
      <div class="history-settings"><label><span>模板名称</span><NInput v-model:value="form.name" :maxlength="120" :disabled="busy" :input-props="{'aria-label':'历史模板名称'}" /></label><NSwitch v-model:value="form.enabled" :disabled="busy"><template #checked>启用</template><template #unchecked>停用</template></NSwitch></div>
      <details class="history-schema"><summary>字段设置 · {{form.fields.length}} 列</summary><RiskInspectionSheet :fields="form.fields" schema can-edit :disabled="busy" @edit="editField" @remove-field="removeField" @required-change="(field,required)=>{if(!busy)field.required=required}" /><div class="history-schema-actions"><NButton :disabled="busy||form.fields.length>=80" @click="addField"><Plus :size="16" />添加字段</NButton></div></details>
      <div class="history-toolbar"><strong>排查事项</strong><NButton :disabled="busy||form.items.length>=100" @click="form.items.push({title:'',values:{}})"><Plus :size="16" />添加事项</NButton></div>
      <RiskInspectionSheet :fields="form.fields" v-model:items="form.items" history :disabled="busy" />
      <NAlert v-if="valueIssues.length" type="warning" :bordered="false">{{valueIssues.slice(0,3).join('；')}}</NAlert>
      <footer class="history-footer"><span>{{dirty?'有未保存修改':selected.cloud?.synced?'已保存并备份至飞书':'已保存，等待飞书备份'}} · 日期、人员、现场附件及审核结果不复用</span><NButton type="primary" :disabled="busy||!dirty||!form.name.trim()||!form.items.length||valueIssues.length>0||form.items.some((i:Dict)=>!i.title.trim())" @click="confirmOpen=true">保存新版本</NButton></footer>
    </template>
    <NModal :show="confirmOpen" preset="dialog" title="保存历史模板" positive-text="确认保存" negative-text="取消" :loading="busy" :mask-closable="false" :closable="!busy" :close-on-esc="!busy" :negative-button-props="{disabled:busy}" @update:show="v=>{if(!busy)confirmOpen=v}" @positive-click="save"><NAlert v-if="error" type="error" :bordered="false" role="alert">{{error}}</NAlert><p>保存为新版本，不修改基础模板或已经发布的排查。停用后不能再次选用此模板。</p></NModal>
    <NModal :show="!!fieldDraft" preset="card" title="历史模板字段" class="risk-history-field-modal" :mask-closable="false" @update:show="show=>{if(!show)closeField()}">
      <NAlert v-if="fieldError" type="error" :bordered="false">{{fieldError}}</NAlert>
      <div v-if="fieldDraft" class="history-field-form"><label><span>字段名称</span><NInput v-model:value="fieldDraft.field_name" :maxlength="80" :input-props="{'aria-label':'历史字段名称'}" /></label><label><span>字段类型</span><NSelect v-model:value="fieldDraft.type" :disabled="fieldDraft.is_primary||fieldDraft.role!=='input'" :options="typeOptions" aria-label="历史字段类型" /></label><label><span>填写要求</span><div v-if="fieldDraft.role==='input'" class="history-required"><NSwitch v-model:value="fieldDraft.required" aria-label="历史字段是否必填" /><span>{{fieldDraft.required?'必填':'不必填'}}</span></div><small v-else>由系统自动填写，无需设置</small></label><label v-if="[3,4].includes(fieldDraft.type)" class="wide"><span>选项，每行一个</span><NInput v-model:value="fieldDraft.optionsText" type="textarea" :autosize="{minRows:3,maxRows:6}" /></label></div>
      <template #footer><div class="history-toolbar"><NButton @click="closeField">取消</NButton><NButton type="primary" @click="applyField">应用到当前草稿</NButton></div></template>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,reactive,ref,toRaw} from 'vue';
import {NAlert,NButton,NEmpty,NInput,NModal,NPagination,NSelect,NSpin,NSwitch,NTag} from 'naive-ui';
import {ArrowLeft,CopyPlus,Pencil,Plus,Search} from 'lucide-vue-next';
import {requestJson,type Dict} from '../api/client';
import RiskInspectionSheet from './RiskInspectionSheet.vue';
import {prefillIssues,riskTypeOptions} from './riskInspectionUi';
const emit=defineEmits<{(e:'reuse',template:Dict):void;(e:'dirty',value:boolean):void}>();
const base='/api/risk-inspections/templates',rows=ref<Dict[]>([]),selected=ref<Dict|null>(null),search=ref(''),page=ref(1),total=ref(0),loading=ref(false),busy=ref(false),error=ref(''),message=ref(''),confirmOpen=ref(false);
const form=reactive<Dict>({name:'',enabled:true,fields:[],items:[]});
const original=ref('');const dirty=computed(()=>!!selected.value&&JSON.stringify(form)!==original.value);
const reusableFields=computed(()=>form.fields.filter((f:Dict)=>![5,17].includes(f.type)));
const valueIssues=computed(()=>prefillIssues(reusableFields.value,form.items));
const typeOptions=riskTypeOptions,fieldDraft=ref<Dict|null>(null),fieldError=ref(''),fieldOriginal=ref('');
const fieldDirty=computed(()=>!!fieldDraft.value&&JSON.stringify(fieldDraft.value)!==fieldOriginal.value);
let abort:AbortController|undefined,disposed=false;
function scopeLabel(s:string){return s==='110'?'110站':s+'楼';}
function date(t:number){return new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(new Date(t*1000));}
function leaveAllowed(){return !busy.value&&(!(dirty.value||fieldDirty.value)||window.confirm('放弃尚未保存的历史模板修改？'));}
function unload(e:BeforeUnloadEvent){if(busy.value||dirty.value||fieldDirty.value){e.preventDefault();e.returnValue='';}}
function assign(data:Dict){selected.value=data;Object.assign(form,{name:data.name,enabled:data.enabled,fields:structuredClone(data.fields),items:structuredClone(data.items)});original.value=JSON.stringify(form);}
async function load(){if(busy.value)return;abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';try{const data=await requestJson(base+'?'+new URLSearchParams({page:String(page.value),search:search.value}),{cache:'no-store',signal:controller.signal});if(!controller.signal.aborted){rows.value=data.items;total.value=data.total;}}catch(e:any){if(!controller.signal.aborted)error.value=e.message;}finally{if(abort===controller){abort=undefined;loading.value=false;}}}
async function open(id:string){if(!leaveAllowed())return;abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';try{const data=await requestJson(base+'/'+id,{cache:'no-store',signal:controller.signal});if(!controller.signal.aborted)assign(data);}catch(e:any){if(!controller.signal.aborted)error.value=e.message;}finally{if(abort===controller){abort=undefined;loading.value=false;}}}
async function reuse(id:string){if(!leaveAllowed())return;busy.value=true;error.value='';try{const data=await requestJson(base+'/'+id,{cache:'no-store'});if(!data.enabled)throw new Error('该模板已停用，请选择其它模板。');if(!disposed)emit('reuse',data);}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
function back(){if(!leaveAllowed())return;selected.value=null;message.value='';void load();}
function clearValues(key:string){for(const item of form.items)delete item.values[key];}
function removeField(f:Dict){if(f.is_primary||!window.confirm('删除此模板字段及其预填值？历史排查结果不会改变。'))return;form.fields=form.fields.filter((v:Dict)=>v.key!==f.key);clearValues(f.key);}
function editField(field:Dict){fieldDraft.value={...structuredClone(toRaw(field)),optionsText:(field.property?.options||[]).map((option:Dict)=>option.name).join('\n')};fieldOriginal.value=JSON.stringify(fieldDraft.value);fieldError.value='';}
function addField(){if(form.fields.length>=80)return;const key='custom_'+Array.from(crypto.getRandomValues(new Uint8Array(12)),value=>value.toString(16).padStart(2,'0')).join('');fieldDraft.value={key,field_id:key,field_name:'',type:1,role:'input',required:false,is_primary:false,property:{},optionsText:''};fieldOriginal.value=JSON.stringify(fieldDraft.value);fieldError.value='';}
function closeField(){if(fieldDirty.value&&!window.confirm('放弃尚未应用的字段修改？'))return;fieldDraft.value=null;}
function applyField(){
 const field=fieldDraft.value;if(!field)return;const name=field.field_name.trim();
 if(!name||form.fields.some((item:Dict)=>item.key!==field.key&&item.field_name===name)){fieldError.value='请填写不重复的字段名称。';return;}
 const originalOptions:string[]=(field.property?.options||[]).map((option:Dict)=>option.name);
 const options=field.optionsText===originalOptions.join('\n')?originalOptions:[...new Set<string>(field.optionsText.split('\n').map((value:string)=>value.trim()).filter(Boolean))];
 if([3,4].includes(field.type)&&(!options.length||options.length>100)){fieldError.value='请配置1至100个不重复的选项。';return;}
 const previous=form.fields.find((item:Dict)=>item.key===field.key);
 if(previous&&previous.type!==field.type){if(form.items.some((item:Dict)=>item.values[field.key]!==undefined)&&!window.confirm('更换类型将清除该字段的模板预填值，是否继续？'))return;clearValues(field.key);}
 const {optionsText,...value}=field;
 const updated={...value,field_name:name,property:[3,4].includes(field.type)?{options:options.map(name=>({name}))}:previous?.type===field.type?previous.property:{}};
 form.fields=previous?form.fields.map((item:Dict)=>item.key===field.key?updated:item):[...form.fields,updated];fieldDraft.value=null;
}
async function save(){if(busy.value||!selected.value)return false;busy.value=true;error.value='';try{const data=await requestJson(base+'/'+selected.value.id,{method:'PUT',body:JSON.stringify({version:selected.value.version,...form})});assign(data);message.value='历史模板新版本已保存，已有排查不受影响。';confirmOpen.value=false;}catch(e:any){error.value=e.message;}finally{busy.value=false;}return false;}
async function retry(){if(busy.value||!selected.value)return;busy.value=true;try{await requestJson('/api/risk-inspections/'+selected.value.source_release_id+'/retry',{method:'POST',body:JSON.stringify({kind:'template_sync'})});message.value='已安排重试模板备份。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});
onBeforeUnmount(()=>{disposed=true;abort?.abort();window.removeEventListener('beforeunload',unload);});
defineExpose({leaveAllowed});
</script>
<style scoped>
.history-required{display:flex;align-items:center;gap:8px}
.risk-library{grid-template-columns:minmax(0,1fr)}.risk-library>*{min-width:0;max-width:100%}.history-settings label{min-width:0}
.history-field-form :deep(.n-switch){justify-self:start}
.risk-library{display:grid;gap:14px;min-width:0}.history-toolbar,.history-settings,.history-footer{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.history-toolbar>.n-input{flex:1;min-width:160px}.history-toolbar>span{flex:1}.history-toolbar>strong{flex:1}.history-settings label{display:grid;gap:6px;flex:1}.history-settings{padding-block:6px}.history-list{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(min(100%,320px),1fr))}.history-list article{border:1px solid var(--border-color,#dce2e9);border-radius:8px;padding:14px;background:var(--card-color,#fff)}.history-list article>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.history-list strong{flex:1;overflow-wrap:anywhere}.history-list p,.history-list small,.history-footer>span{font-size:13px;color:var(--text-color-3,#667085)}.history-list footer{display:flex;gap:8px;justify-content:flex-end;margin-top:12px}.history-schema,.history-item{border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.history-schema summary,.history-item summary{cursor:pointer;padding-bottom:12px}.history-field{display:grid;grid-template-columns:minmax(150px,1fr) 140px 70px 32px;gap:10px;align-items:center;padding-bottom:12px}.history-field small,.history-options{grid-column:1/-1}.history-options{display:grid;gap:6px}.history-item>details{margin-top:12px}.history-footer{justify-content:space-between;border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.history-footer>span{flex:1}@media(max-width:600px){.history-field{grid-template-columns:minmax(0,1fr) 120px 52px 28px;gap:6px}.risk-library :deep(.n-button){min-height:40px}.history-toolbar{gap:8px}.history-settings{align-items:end}.history-footer{position:sticky;bottom:0;background:var(--card-color,#fff);padding:12px 0}.history-footer>span{flex-basis:100%}}
.history-schema-actions{display:flex;justify-content:flex-end;padding-top:10px}.history-field-form{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.history-field-form label{display:grid;gap:6px;min-width:0}.history-field-form .wide{grid-column:1/-1}:global(.risk-history-field-modal.n-modal){width:min(580px,calc(100vw - 24px));max-height:90dvh;overflow:auto}.history-field-form{margin-top:12px}.history-settings{gap:14px}.history-settings label>span{font-weight:600;font-size:13px}@media(max-width:600px){.history-field-form{grid-template-columns:1fr}:global(.workspace-v2 .history-footer){bottom:calc(68px + env(safe-area-inset-bottom))}}
</style>
