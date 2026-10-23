<template>
  <NModal :show="true" preset="card" title="修改检查记录" class="check-record-editor" :mask-closable="false" :close-on-esc="false" :closable="!saving&&!uploading" :auto-focus="false" @update:show="value=>{if(!value)requestClose()}" @esc="requestClose">
    <NSkeleton v-if="!record&&loading" text :repeat="4" />
    <template v-if="record">
      <div class="check-record-meta"><strong>{{record.scope==='110'?'110站':record.scope+'楼'}} · {{record.day}} · {{record.shift}}班</strong><span>原提交：{{record.actor_name}} · {{dateLabel(record.submitted)}}</span><span v-if="record.revision>1&&record.updated_at">最近修改：{{record.updated_by_name||record.updated_by}} · {{dateLabel(record.updated_at)}}</span></div>
      <NAlert v-if="!record.can_edit" type="info" :bordered="false">当前账号不可修改该次检查。</NAlert>
      <div class="check-record-items" role="group" aria-label="修改检查项目">
        <fieldset v-for="(item,index) in record.items" :key="item.id" class="check-record-item">
          <legend>{{index+1}}. {{item.text}}</legend>
          <div class="v2-check-result"><NRadioGroup v-model:value="answers[item.id].status" :name="'edit-'+record.id+'-'+item.id" :disabled="locked" :aria-label="item.text"><NRadioButton class="v2-check-normal" value="normal">无异常</NRadioButton><NRadioButton class="v2-check-abnormal" value="abnormal">异常</NRadioButton></NRadioGroup></div>
          <NInput v-if="answers[item.id].status==='abnormal'" v-model:value="answers[item.id].note" type="textarea" :autosize="{minRows:2,maxRows:4}" :maxlength="500" :disabled="locked" :input-props="{'aria-label':item.text+'异常备注'}" placeholder="填写异常情况" />
          <CheckAttachments :task-id="record.id" :item-id="item.id" kind="item" :files="record.files.filter((file:Dict)=>file.item_id===item.id)" :revision="record.revision" :locked="locked" :allow-upload="record.can_edit" @busy="uploading=$event" @changed="attachmentChanged" />
        </fieldset>
      </div>
      <section class="check-record-handover"><h3>交接班照片</h3><CheckAttachments :task-id="record.id" kind="handover" :files="record.handover_photo?[record.handover_photo]:[]" :revision="record.revision" :locked="locked" :allow-upload="record.can_edit" @busy="uploading=$event" @changed="attachmentChanged" /></section>
    </template>
    <template #footer><div class="check-record-footer"><span :role="error?'alert':'status'" :class="{'has-error':!!error}">{{error||status}}</span><NButton attr-type="button" :disabled="saving||uploading" @click="requestClose">关闭</NButton><NButton v-if="conflict||!record" attr-type="button" secondary :loading="loading" :disabled="saving||uploading" @click="reload">重新读取</NButton><NButton attr-type="button" type="primary" :loading="saving" :disabled="locked||!record" @click="save"><template #icon><Save :size="16" /></template>保存检查结果</NButton></div></template>
  </NModal>
</template>

<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NInput,NModal,NRadioButton,NRadioGroup,NSkeleton,useDialog,useMessage} from 'naive-ui';
import {Save} from 'lucide-vue-next';
import {requestJson,type Dict} from '../client';
import {registerNavigationGuard} from '../navigation';
import CheckAttachments from './CheckAttachments.vue';

const props=defineProps<{taskId:string;registerGuard?:(guard:(target:string,proceed:()=>void)=>boolean)=>()=>void}>();
const emit=defineEmits<{close:[];changed:[]}>();
const record=ref<Dict>(),answers=ref<Record<string,{status:string;note:string}>>({});
const loading=ref(false),saving=ref(false),uploading=ref(false),conflict=ref(false),error=ref(''),status=ref('');
const dialog=useDialog(),message=useMessage();
let disposed=false,controller:AbortController|undefined,original='',confirming=false;
const dirty=computed(()=>!!record.value&&JSON.stringify(answers.value)!==original);
const locked=computed(()=>loading.value||saving.value||uploading.value||conflict.value||record.value?.can_edit!==true);
const endpoint='/api/pre-shift-checks/tasks/'+encodeURIComponent(props.taskId);
function dateLabel(value:string){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai'});}
function notify(){emit('changed');window.dispatchEvent(new Event('pre-shift-record-changed'));window.dispatchEvent(new Event('clipflow-business-changed'));}
function adopt(value:Dict,preserve=false){
  if(value.id!==props.taskId||!value.submitted||!Number.isInteger(value.revision)||!Array.isArray(value.items)||!Array.isArray(value.answers)||!Array.isArray(value.files))throw new Error('检查记录返回不完整，请重新读取。');
  if(!preserve){answers.value=Object.fromEntries(value.items.map((item:Dict)=>{const answer=value.answers.find((row:Dict)=>row.id===item.id);return [item.id,{status:answer?.status||'',note:answer?.note||''}];}));original=JSON.stringify(answers.value);}
  record.value=value;
}
async function load(preserve=false,expectedRevision?:number){
  controller?.abort();controller=new AbortController();const read=controller;loading.value=true;
  try{const value=await requestJson(endpoint,{cache:'no-store',signal:read.signal});if(disposed||read.signal.aborted)return;adopt(value,preserve);conflict.value=preserve&&expectedRevision!==value.revision;error.value=conflict.value?'记录已被其他人员修改，当前填写已保留，请重新读取后核对。':'';}
  catch(e){if(!disposed&&!read.signal.aborted){error.value=(e as Error).message;if(preserve)conflict.value=true;}}
  finally{if(controller===read)loading.value=false;}
}
async function attachmentChanged(result?:Dict){
  if(!result||disposed)return;
  status.value='附件已保存';notify();
  if(!Number.isInteger(result.revision)){conflict.value=true;error.value='附件已保存，但记录版本尚未确认，请重新读取后核对。';return;}
  await load(true,result.revision);
}
async function save(){
  if(locked.value||!record.value)return;
  const values=record.value.items.map((item:Dict)=>({id:item.id,...answers.value[item.id]}));
  if(values.some((answer:Dict)=>!['normal','abnormal'].includes(answer.status)||answer.status==='abnormal'&&!answer.note.trim())){error.value='请逐项选择检查结果，并填写异常项备注。';return;}
  saving.value=true;error.value='';
  try{const result=await requestJson(endpoint,{method:'PUT',body:JSON.stringify({revision:record.value.revision,answers:values})});if(disposed)return;adopt(result);status.value='检查结果已保存，等待飞书同步';message.success('检查结果已保存');notify();}
  catch(e){if(!disposed){error.value=(e as Error).message;const code=(e as Error&{status?:number}).status;if(code===409||code===403)conflict.value=true;}}
  finally{saving.value=false;}
}
function leave(proceed:()=>void){
  if(saving.value||uploading.value||loading.value){message.warning('正在保存或读取，请稍候。');return false;}
  if(!dirty.value){proceed();return true;}
  if(confirming)return false;confirming=true;
  dialog.warning({title:'检查结果尚未保存',content:'放弃本次检查结果修改？已经保存的附件将保留。',positiveText:'放弃修改',negativeText:'继续填写',onPositiveClick:()=>{confirming=false;original=JSON.stringify(answers.value);proceed();},onNegativeClick:()=>{confirming=false;},onClose:()=>{confirming=false;}});
  return false;
}
function requestClose(){leave(()=>emit('close'));}
function reload(){leave(()=>void load());}
const removeGuard=(props.registerGuard||registerNavigationGuard)((_target,proceed)=>{
  if(!dirty.value&&!saving.value&&!uploading.value&&!loading.value)return true;
  leave(()=>{emit('close');proceed();});return false;
});
function beforeUnload(event:BeforeUnloadEvent){if(dirty.value||saving.value||uploading.value){event.preventDefault();event.returnValue='';}}
onMounted(()=>{void load();window.addEventListener('beforeunload',beforeUnload);});
onBeforeUnmount(()=>{disposed=true;controller?.abort();removeGuard();window.removeEventListener('beforeunload',beforeUnload);});
</script>

<style>
.check-record-footer>span.has-error{color:#b42318;flex-basis:100%;overflow-wrap:anywhere;line-height:1.5}
.check-record-editor.n-card{width:min(860px,calc(100vw - 24px));max-height:calc(100dvh - 24px);display:flex;--v2-blue:#1762cf;--v2-border:#d8e3f1;--v2-muted:#647487}
.check-record-editor .n-card__content,.check-record-editor .n-card-content{min-height:0;overflow:auto;scrollbar-gutter:stable;scroll-padding-block:12px}
.check-record-meta{display:flex;flex-wrap:wrap;gap:5px 16px;padding:10px 0;font-size:12px;color:var(--v2-muted)}.check-record-meta strong{width:100%;font-size:14px;color:inherit}
.check-record-items{display:flex;flex-direction:column}.check-record-item{border:0;border-bottom:1px solid var(--v2-border);margin:0;padding:14px 0;min-width:0;display:grid;gap:10px}.check-record-item legend{float:left;padding:0;font-size:16px;font-weight:600;line-height:1.6;overflow-wrap:anywhere}.check-record-item .v2-check-result{justify-content:flex-end}
.check-record-handover{padding-top:12px}.check-record-handover h3{font-size:14px;margin:0 0 8px}.check-record-footer{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}.check-record-footer>span{margin-right:auto;font-size:12px;color:var(--v2-muted)}.check-record-footer .n-button{min-height:44px}
@media(max-width:600px){.check-record-editor.n-card{width:calc(100vw - 16px);max-height:calc(100dvh - 16px)}.check-record-editor .n-card__content,.check-record-editor .n-card-content,.check-record-editor .n-card__footer,.check-record-editor .n-card-header{padding:12px}.check-record-footer>span{flex-basis:100%}.check-record-item .n-radio-group{display:flex;flex:1}.check-record-item .n-radio-button{flex:1;justify-content:center}}
</style>
