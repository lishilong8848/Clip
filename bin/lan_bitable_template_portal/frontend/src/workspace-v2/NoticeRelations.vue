<template>
  <section class="v2-notice-relations" aria-label="关联核验">
    <div class="v2-notice-tools"><h2>关联核验</h2><NTag :type="targetBound?'success':'default'" size="small" :bordered="false">{{ targetBound?'已绑定目标通告':'尚未绑定目标通告' }}</NTag><NButton :disabled="locked||busy" :loading="loading" data-v2-navigation @click="lookup('target')">{{ targetBound?'重新查找目标通告':'查找目标通告' }}</NButton><NButton v-if="form.values.work_type==='repair'" :disabled="locked||busy||!hasIdentity" :loading="loading" data-v2-navigation @click="lookup('event')">关联事件与维修项目</NButton></div>
    <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }}</NAlert>
    <form v-if="mode" class="v2-notice-binding" @submit.prevent="lookup(mode)"><NInput v-model:value="search" clearable :placeholder="mode==='event'?'搜索故障或设备':'筛选目标通告名称'" :input-props="{'aria-label':'搜索关联记录'}" /><NButton attr-type="submit" :disabled="busy" :loading="loading">查找</NButton></form>
    <NSelect v-if="mode" v-model:value="selected" :options="candidateOptions" :disabled="locked||busy||loading" :loading="loading" filterable clearable class="v2-notice-candidates" aria-label="关联候选" placeholder="选择要关联的记录" />
    <NEmpty v-if="mode&&!loading&&!visible.length&&!error" description="未找到符合条件的记录" />
    <div v-if="mode&&visible.length" class="v2-notice-list-footer"><span>{{ selectedIsBound?'已绑定当前选中记录':'选择后核对并保存才会更改关联' }}</span><NButton :disabled="!selected||locked||loading||selectedIsBound" :loading="busy" data-v2-submit @click="confirm">核对并保存关联</NButton></div>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,ref,watch} from 'vue';
import {NAlert,NButton,NEmpty,NInput,NSelect,NTag,useDialog} from 'naive-ui';
import {requestJson,type Dict} from './client';
import type {NoticeForm} from './noticeForm';
const props=defineProps<{form:NoticeForm;locked:boolean}>(),emit=defineEmits<{bound:[result:Dict];busy:[value:boolean]}>(),dialog=useDialog();
const mode=ref(''),search=ref(''),selected=ref<string|null>(null),items=ref<Dict[]>([]),loading=ref(false),busy=ref(false),error=ref('');
const targetBound=computed(()=>!!props.form.values.target_record_id);
const selectedIsBound=computed(()=>mode.value==='target'&&!!selected.value&&selected.value===props.form.values.target_record_id);
const hasIdentity=computed(()=>props.form.values.source_record_id||props.form.values.active_item_id||props.form.values.target_record_id);
const visible=computed(()=>mode.value==='event'?items.value:items.value.filter(i=>String(i.title||'').includes(search.value)));
const candidateId=(i:Dict)=>String(mode.value==='event'?i.repair_management_record_id:i.target_record_id||i.record_id||'');
const candidateOptions=computed(()=>visible.value.map(item=>({value:candidateId(item),label:[item.title||item.project_title||item.event_title||'未命名记录',item.building,item.status||item.event_status,item.start_time||item.occurrence_time].filter(Boolean).join(' · ')})));
watch(visible,()=>{if(selected.value&&!visible.value.some(item=>candidateId(item)===selected.value))selected.value=null;});
let controller:AbortController|undefined;
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
onBeforeUnmount(()=>controller?.abort());
</script>
<style>.v2-notice-candidates{margin:10px 0}.v2-notice-relations .v2-notice-tools h2{margin:0}.v2-notice-relations .v2-notice-binding{margin-top:10px}.v2-notice-relations .v2-notice-list-footer{font-size:12px;color:var(--v2-muted)}</style>
