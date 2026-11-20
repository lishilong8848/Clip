<template>
  <NModal :show="true" preset="card" class="v2-notice-confirm task-records" aria-label="本机办理记录"
    :mask-closable="!busy" :closable="!busy" :close-on-esc="!busy" @update:show="emit('close')">
    <template #header><header class="task-bar">
      <strong>本机办理记录</strong>
      <NButton size="small" :loading="loading" :disabled="busy" @click="load">刷新状态</NButton>
    </header></template>
    <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }}</NAlert>
    <NAlert v-if="missing" type="warning" :bordered="false">{{ missing }} 条本机历史记录暂未查到原任务。</NAlert>
    <NSpin :show="loading">
      <ul v-if="visible.length" class="task-list">
        <li v-for="item in visible.slice((page-1)*10,page*10)" :key="item.job_id">
          <header class="task-bar">
            <strong>{{ item.title }}</strong>
            <span class="task-phase" :data-phase="item.phase">{{ phaseLabel(item.phase) }}</span>
            <NButton v-if="item.phase==='failed' && item.error_retryable && item.can_retry!==false" size="small" :disabled="busy" @click="retry(item)">重试原任务</NButton>
          </header>
          <small>{{ item.scope }} · {{ item.updated_at||item.created_at }}{{ item.request?.action==='end'?' · 结束通告':'' }}</small>
          <NAlert v-if="item.phase==='failed' && (item.error||item.upload_message)" type="error" :bordered="false">{{ item.error||item.upload_message }}</NAlert>
          <p v-if="item.work_order_hint">{{ item.work_order_hint }}</p>
        </li>
      </ul>
      <NEmpty v-else-if="!loading&&!error" description="本机暂无已确认的办理记录" />
    </NSpin>
    <template #footer><div class="task-bar">
      <NPagination v-model:page="page" :page-size="10" :item-count="visible.length" simple />
      <NButton :disabled="busy" @click="emit('close')">关闭</NButton>
    </div></template>
  </NModal>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NEmpty,NModal,NPagination,NSpin,useMessage} from 'naive-ui';
import {requestJson,type Dict} from './client';
import {noticeTasks} from './noticeTasks';
import {registerNavigationGuard} from './navigation';
import {trackButtonRequest} from './actionMotion';
const props=defineProps<{userId:string;scope:string;modules:string[]}>(),emit=defineEmits<{close:[];changed:[]}>(),message=useMessage();
const items=ref<Dict[]>([]),loading=ref(false),busy=ref(false),error=ref(''),missing=ref(0),page=ref(1);
const visible=computed(()=>items.value.filter(i=>props.modules.includes(i.work_type)&&(props.scope==='ALL'||props.scope===i.scope||props.scope==='CAMPUS'&&'ABCDE'.includes(i.scope))));
const phaseLabel=(phase:string)=>({event_waiting:'已排队，等待事件结束',success:'已完成',failed:'本次提交失败',cancelled:'已取消'}[phase]||'处理中');
let timer=0,disposed=false,controller:AbortController|undefined;
async function load(){clearTimeout(timer);if(disposed||document.hidden||loading.value)return;loading.value=true;error.value='';controller=new AbortController();const rows=noticeTasks(props.userId).filter(r=>props.modules.includes(r.work_type)&&(props.scope==='ALL'||props.scope===r.scope||props.scope==='CAMPUS'&&'ABCDE'.includes(r.scope)));missing.value=0;try{if(!rows.length){items.value=[];return;}const result=await requestJson('/api/jobs/batch?ids='+encodeURIComponent(rows.map(r=>r.job_id).join(',')),{signal:controller.signal,cache:'no-store'});if(disposed)return;items.value=(result.items||[]).map((item:Dict)=>({...rows.find(r=>r.job_id===item.job_id),...item}));for(const item of items.value)void trackButtonRequest(Promise.resolve(item));missing.value=(result.missing||[]).length;if(items.value.some(i=>!['success','failed','cancelled'].includes(i.phase)))timer=window.setTimeout(load,4000);}catch(e){if(!disposed)error.value=(e as Error).message;}finally{loading.value=false;}}
async function retry(item:Dict){if(busy.value)return;busy.value=true;try{await requestJson('/api/jobs/'+encodeURIComponent(item.job_id)+'/retry',{method:'POST',body:'{}'});emit('changed');void load();}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
function visibility(){if(document.hidden)clearTimeout(timer);else void load();}
const stop=registerNavigationGuard(()=>{if(!busy.value)return true;message.warning('正在提交重试，请等待结果');return false;});
onMounted(()=>{void load();document.addEventListener('visibilitychange',visibility);});onBeforeUnmount(()=>{disposed=true;clearTimeout(timer);controller?.abort();stop();document.removeEventListener('visibilitychange',visibility);});
</script>
<style scoped>
.task-bar {display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:4px 10px;min-width:0}
.task-list {list-style:none;margin:0;padding:0}
.task-list li {padding:9px 0;border-bottom:1px solid var(--soft-edge);overflow-wrap:anywhere}
.task-list .task-bar strong {flex:1 1 8em}
.task-list small {color:var(--v2-muted)}
.task-list p {margin:3px 0}
.task-phase {padding:2px 8px;background:var(--soft-field);border-radius:8px;font-size:12px}
.task-phase[data-phase=failed] {font-weight:650}
</style>
