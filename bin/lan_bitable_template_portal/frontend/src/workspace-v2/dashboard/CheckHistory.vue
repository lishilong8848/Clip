<template>
  <section class="check-history" aria-label="历史检查记录" data-v2-navigation>
    <header><span>历史检查记录<small v-if="data">共 {{data.total}} 次</small></span><NButton quaternary circle :loading="loading" aria-label="刷新历史检查记录" @click="load(page)"><RefreshCw :size="16" /></NButton></header>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{error}}<NButton text @click="load(page)">重试</NButton></NAlert>
    <NSkeleton v-if="!data&&loading" text :repeat="4" />
    <template v-else-if="data">
      <NCollapse v-if="data.items.length" :key="scope+':'+page" accordion>
        <NCollapseItem v-for="row in data.items" :key="row.id" :name="row.id">
          <template #header><div class="check-history-heading"><strong>{{row.day}} · {{row.shift}}班</strong><span>{{row.actor_name}}</span><NTag size="small" :bordered="false" :type="row.abnormal_count?'warning':'success'">{{row.abnormal_count?'存在异常':'全部正常'}}</NTag><small>正常 {{row.normal_count}} · 异常 {{row.abnormal_count}}</small></div></template>
          <div class="check-history-meta"><span>提交时间 {{dateLabel(row.submitted)}}</span><span>计划时间 {{dateLabel(row.due*1000)}}</span><span>{{row.sync_status==='synced'?'已同步飞书':'本地已保存，待同步飞书'}}</span></div>
          <div v-if="row.can_edit||row.revision>1" class="check-history-edit"><span v-if="row.revision>1&&row.updated_at">最近修改：{{row.updated_by_name||row.updated_by}} · {{dateLabel(row.updated_at)}}</span><NButton v-if="row.can_edit" size="small" secondary @click="emit('edit',row.id)"><template #icon><Pencil :size="15" /></template>修改记录</NButton></div>
          <NAlert v-if="row.sync_error" type="warning" :bordered="false">{{row.sync_error}}</NAlert>
          <ol class="check-history-answers">
            <li v-for="answer in row.answers" :key="answer.id" :class="{'is-abnormal':answer.status==='abnormal'}">
              <div><strong>{{answer.text}}</strong><NTag size="small" :bordered="false" :type="answer.status==='abnormal'?'warning':'success'">{{answer.status==='abnormal'?'异常':'正常'}}</NTag></div>
              <p v-if="answer.note">备注：{{answer.note}}</p>
              <CheckAttachments v-if="row.files.some((file:Dict)=>file.item_id===answer.id)" :task-id="row.id" :item-id="answer.id" kind="item" :files="row.files.filter((file:Dict)=>file.item_id===answer.id)" locked :allow-upload="false" />
            </li>
          </ol>
          <section v-if="row.handover_photo" class="check-history-photo"><strong>交接班照片</strong><CheckAttachments :task-id="row.id" kind="handover" :files="[row.handover_photo]" locked :allow-upload="false" /></section>
        </NCollapseItem>
      </NCollapse>
      <NEmpty v-else description="本楼暂无历史检查记录" size="small" />
      <NPagination v-if="data.total>data.page_size" :page="page" :item-count="data.total" :page-size="data.page_size" :page-slot="5" :disabled="loading" aria-label="历史检查记录分页" @update:page="load" />
    </template>
  </section>
</template>
<script setup lang="ts">
import {onBeforeUnmount,ref,watch} from 'vue';
import {NAlert,NButton,NCollapse,NCollapseItem,NEmpty,NPagination,NSkeleton,NTag} from 'naive-ui';
import {Pencil,RefreshCw} from 'lucide-vue-next';
import {requestJson,type Dict} from '../client';
import CheckAttachments from './CheckAttachments.vue';
const props=defineProps<{scope:string}>();
const emit=defineEmits<{edit:[id:string]}>();
const data=ref<Dict>(),page=ref(1),loading=ref(false),error=ref('');
let controller:AbortController|undefined,disposed=false;
function dateLabel(value:string|number){return new Date(value).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});}
async function load(next=1){
  if(disposed)return;controller?.abort();const read=new AbortController();controller=read;loading.value=true;error.value='';
  try{
    const result=await requestJson('/api/pre-shift-checks/history?'+new URLSearchParams({scope:props.scope,page:String(next),page_size:'20'}),{signal:read.signal,cache:'no-store'});
    if(disposed||read.signal.aborted)return;
    if(result.scope!==props.scope||!Number.isInteger(result.total)||result.total<0||!Number.isInteger(result.page)||result.page<1||result.page_size!==20||!Array.isArray(result.items)||result.items.some((row:Dict)=>!row.id||!row.submitted||!Array.isArray(row.answers)||!Array.isArray(row.files)))throw new Error('历史检查记录返回不完整，请重新读取。');
    data.value=result;page.value=result.page;
  }catch(e){if(!disposed&&!read.signal.aborted)error.value=e instanceof Error?e.message:'历史检查记录读取失败，请重试。';}
  finally{if(controller===read)loading.value=false;}
}
watch(()=>props.scope,()=>{data.value=undefined;page.value=1;void load();},{immediate:true});
function refresh(){if(!loading.value)void load(page.value);}
window.addEventListener('pre-shift-record-changed',refresh);
onBeforeUnmount(()=>{disposed=true;controller?.abort();window.removeEventListener('pre-shift-record-changed',refresh);});
</script>
<style scoped>
.check-history-edit{display:flex;align-items:center;justify-content:flex-end;flex-wrap:wrap;gap:8px;margin-bottom:10px;font-size:12px;color:var(--v2-muted,#647487)}
.check-history{min-width:0;display:grid;gap:12px}.check-history>header{display:flex;align-items:center;justify-content:space-between;gap:8px}.check-history>header>span{font-weight:600}.check-history small{font-size:12px;color:var(--v2-muted,#647487);font-weight:400}.check-history>header small{margin-left:10px}
.check-history-heading{display:flex;align-items:center;flex-wrap:wrap;gap:6px 12px;min-width:0;line-height:1.6}.check-history-heading strong{font-size:13px}.check-history-heading>span{font-size:13px}.check-history-meta{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12px;color:var(--v2-muted,#647487);line-height:1.6;margin-bottom:10px}
.check-history-answers{margin:0;padding-left:24px}.check-history-answers>li{padding:9px 0;border-bottom:1px solid var(--v2-border,#d8e3f1);overflow-wrap:anywhere}.check-history-answers>li>div:first-child{display:flex;align-items:flex-start;gap:10px;justify-content:space-between}.check-history-answers strong{font-size:13px;font-weight:500;line-height:1.6}.check-history-answers p{white-space:pre-wrap;margin:6px 0;font-size:13px;line-height:1.6}.check-history-answers .is-abnormal p{color:var(--warning-color,#985719)}.check-history-photo{display:grid;gap:8px;margin-top:12px;font-size:13px}.check-history>.n-pagination{justify-self:end;max-width:100%}
@media(max-width:600px){.check-history-heading{gap:5px 8px}.check-history-heading>small{flex-basis:100%}.check-history :deep(.n-collapse-item__header){min-height:44px}.check-history>header .n-button{min-width:44px;min-height:44px}}
</style>
