<template>
  <section class="v2-notice-relations" aria-label="关联核验">
    <div class="v2-notice-tools"><h2>已发通告</h2><NTag :type="targetBound?'success':'default'" size="small" :bordered="false">{{ targetBound?'已绑定目标通告':'尚未绑定目标通告' }}</NTag><NButton :disabled="locked||busy" :aria-expanded="mode==='target'" :aria-label="targetBound?'重新查找目标通告':'查找目标通告'" data-v2-navigation @click="toggle('target')"><Search :size="15" />{{ targetBound?'更换':'查找' }}</NButton><NButton v-if="form.values.work_type==='repair'" :disabled="locked||busy||!hasIdentity" :aria-expanded="mode==='event'" aria-label="关联事件与维修项目" data-v2-navigation @click="toggle('event')">事件 / 维修项目</NButton></div>
    <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }}</NAlert>
    <div v-if="mode" class="v2-relation-picker">
      <label :for="'notice-relation-'+mode">{{ mode==='event'?'事件 / 维修项目':'目标通告' }}</label>
      <NSelect v-model:value="selected" :options="candidateOptions" :disabled="locked||busy" :loading="loading" filterable clearable :remote="mode==='event'" :input-props="{id:'notice-relation-'+mode}" class="v2-notice-candidates" aria-label="关联候选" :placeholder="mode==='event'?'输入故障、设备或项目名称':'输入目标通告名称'" @search="searchCandidates"><template #empty>{{ loading?'正在查找…':'未找到符合条件的记录' }}</template></NSelect>
      <NButton :disabled="!selected||locked||loading||selectedIsBound" :loading="busy" data-v2-submit @click="confirm">{{ selectedIsBound?'已关联':'核对并保存关联' }}</NButton>
      <NButton quaternary circle :disabled="busy" aria-label="收起关联选择" data-v2-navigation @click="toggle(mode)"><X :size="16" /></NButton>
    </div>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,ref,watch} from 'vue';
import {NAlert,NButton,NSelect,NTag,useDialog} from 'naive-ui';
import {Search,X} from 'lucide-vue-next';
import {requestJson,type Dict} from './client';
import type {NoticeForm} from './noticeForm';
const props=defineProps<{form:NoticeForm;locked:boolean}>(),emit=defineEmits<{bound:[result:Dict];busy:[value:boolean]}>(),dialog=useDialog();
const mode=ref(''),search=ref(''),selected=ref<string|null>(null),items=ref<Dict[]>([]),loading=ref(false),busy=ref(false),error=ref('');
const targetBound=computed(()=>!!props.form.values.target_record_id);
const selectedIsBound=computed(()=>mode.value==='target'&&!!selected.value&&selected.value===props.form.values.target_record_id);
const hasIdentity=computed(()=>props.form.values.source_record_id||props.form.values.active_item_id||props.form.values.target_record_id);
const visible=computed(()=>items.value);
const candidateId=(i:Dict)=>String(mode.value==='event'?i.repair_management_record_id:i.target_record_id||i.record_id||'');
const candidateOptions=computed(()=>visible.value.map(item=>({value:candidateId(item),label:[item.title||item.project_title||item.event_title||'未命名记录',item.building,item.status||item.event_status,item.start_time||item.occurrence_time].filter(Boolean).join(' · ')})));
watch(visible,()=>{if(selected.value&&!visible.value.some(item=>candidateId(item)===selected.value))selected.value=null;});
let controller:AbortController|undefined;
let searchTimer=0;
function toggle(next:string){if(busy.value||props.locked)return;clearTimeout(searchTimer);if(mode.value===next){controller?.abort();mode.value='';loading.value=false;error.value='';}else void lookup(next);}
function searchCandidates(value:string){search.value=value;clearTimeout(searchTimer);if(mode.value==='event')searchTimer=window.setTimeout(()=>lookup('event'),300);}
async function lookup(next:string){
  if(busy.value||props.locked)return;
  controller?.abort();const c=new AbortController();controller=c;
  if(mode.value!==next)search.value='';
  mode.value=next;selected.value=null;items.value=[];loading.value=true;error.value='';
  const v=props.form.values;
  try{
    const data=next==='event'?await requestJson('/api/workbench/repair-event-candidates?'+new URLSearchParams({scope:v.scope,q:search.value,limit:'100'}),{signal:c.signal,cache:'no-store'}):await requestJson('/api/notice-target-candidates',{method:'POST',signal:c.signal,body:JSON.stringify({scope:v.scope,lookup_context:props.form.detailMode==='ongoing'?'target_table':'planned_target_table',work_type:v.work_type,title:v.title,start_time:v.start_time,end_time:v.end_time,action:'update',content:v.content,reason:v.reason,impact:v.impact,progress:v.progress})});
    if(c.signal.aborted)return;
    items.value=(next==='event'?data.records||[]:data.candidates||[]).filter((i:Dict)=>candidateId(i));
    if(next==='target'&&items.value.some(i=>candidateId(i)===v.target_record_id))selected.value=v.target_record_id;
    if(data.warnings?.length)error.value=data.warnings.join('；');
    if(data.total>items.value.length)error.value+=' 当前仅显示前'+items.value.length+'条，结果不完整。';
  }catch(e){if(!c.signal.aborted)error.value=(e as Error).message;}
  finally{if(controller===c)loading.value=false;}
}
function confirm(){const item=items.value.find(i=>candidateId(i)===selected.value);if(!item||busy.value||props.locked)return;dialog.warning({title:'确认保存关联',content:'将关联“'+(item.title||item.project_title||item.event_title)+'”，并重新读取通告。当前未保存的填写将重置，请先核对。',positiveText:'确认关联并重新读取',negativeText:'返回',onPositiveClick:async()=>{busy.value=true;emit('busy',true);error.value='';const v=props.form.values;try{const result=mode.value==='event'?await requestJson('/api/workbench/repair-event-bind',{method:'POST',body:JSON.stringify({scope:v.scope,source_record_id:v.repair_management_record_id||v.source_record_id||'',event_record_id:item.event_record_id,candidate_project_record_id:item.repair_management_record_id,active_item_id:v.active_item_id||'',target_record_id:v.target_record_id||''})}):await requestJson('/api/notice-identity/bind',{method:'POST',body:JSON.stringify({scope:v.scope,binding_context:props.form.detailMode==='ongoing'?'ongoing':'planned',work_type:v.work_type,notice_type:v.notice_type,active_item_id:v.active_item_id||item.active_item_id||'',source_record_id:v.source_record_id||'',target_record_id:candidateId(item),record_id:candidateId(item),title:v.title||item.title,reason:v.reason,start_time:v.start_time,end_time:v.end_time,status:item.status||''})});emit('busy',false);emit('bound',result);}catch(e){error.value=(e as Error).message;return false;}finally{busy.value=false;emit('busy',false);}}});}
onBeforeUnmount(()=>{controller?.abort();clearTimeout(searchTimer);});
</script>
<style>.v2-notice-relations .v2-notice-tools h2{margin:0}.v2-relation-picker{display:grid;grid-template-columns:auto minmax(120px,1fr) auto auto;align-items:center;gap:8px;margin-top:8px}.v2-relation-picker>label{font-size:12px;color:var(--v2-muted)}.v2-relation-picker .v2-notice-candidates{min-width:0;margin:0}@media(max-width:600px){.v2-relation-picker{grid-template-columns:minmax(0,1fr) auto auto}.v2-relation-picker>label{grid-column:1/-1}.v2-relation-picker>.n-button{min-height:44px}.v2-relation-picker .n-base-selection{--n-height:44px!important}}</style>
