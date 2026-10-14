<template><NModal :show="true" preset="card" class="v2-notice-confirm" title="本机办理记录" :mask-closable="!busy" :closable="!busy" :close-on-esc="!busy" @update:show="emit('close')"><NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }}</NAlert><NAlert v-if="missing" type="warning" :bordered="false">{{ missing }} 个原任务暂未查到，请核验原通告，不要重复新增。</NAlert><NSpin :show="loading"><NList><NListItem v-for="item in visible.slice((page-1)*10,page*10)" :key="item.job_id"><strong>{{ item.title }}</strong><p>{{ item.scope }} · {{ item.updated_at||item.created_at }} · {{ phaseLabel(item.phase) }}</p><NAlert v-if="item.error" type="error" :bordered="false">{{ item.error }}</NAlert><NButton v-if="item.phase==='failed'&&item.error_retryable" :disabled="busy" @click="retry(item)">重试原任务</NButton></NListItem></NList><NEmpty v-if="!loading&&!visible.length&&!error" description="本机暂无已确认的办理记录" /></NSpin><template #footer><div class="v2-dialog-actions"><NPagination v-model:page="page" :page-size="10" :item-count="visible.length" simple /><NButton :loading="loading" :disabled="busy" @click="load">刷新状态</NButton><NButton :disabled="busy" @click="emit('close')">关闭</NButton></div></template></NModal></template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref} from 'vue';
import {NAlert,NButton,NEmpty,NList,NListItem,NModal,NPagination,NSpin,useMessage} from 'naive-ui';
import {requestJson,type Dict} from './client';
import {noticeTasks} from './noticeTasks';
import {registerNavigationGuard} from './navigation';
import {trackButtonRequest} from './actionMotion';
const props=defineProps<{userId:string;scope:string;modules:string[]}>(),emit=defineEmits<{close:[];changed:[]}>(),message=useMessage();
const items=ref<Dict[]>([]),loading=ref(false),busy=ref(false),error=ref(''),missing=ref(0),page=ref(1);
const visible=computed(()=>items.value.filter(i=>props.modules.includes(i.work_type)&&(props.scope==='ALL'||props.scope===i.scope||props.scope==='CAMPUS'&&'ABCDE'.includes(i.scope))));
const phaseLabel=(phase:string)=>({success:'已完成',failed:'失败',cancelled:'已取消'}[phase]||'处理中');
let timer=0,disposed=false,controller:AbortController|undefined;
async function load(){clearTimeout(timer);if(disposed||document.hidden||loading.value)return;loading.value=true;error.value='';controller=new AbortController();const rows=noticeTasks(props.userId);try{if(!rows.length){items.value=[];return;}const result=await requestJson('/api/jobs/batch?ids='+encodeURIComponent(rows.map(r=>r.job_id).join(',')),{signal:controller.signal,cache:'no-store'});if(disposed)return;items.value=(result.items||[]).map((item:Dict)=>({...rows.find(r=>r.job_id===item.job_id),...item}));for(const item of items.value)void trackButtonRequest(Promise.resolve(item));missing.value=(result.missing||[]).length;if(items.value.some(i=>!['success','failed','cancelled'].includes(i.phase)))timer=window.setTimeout(load,4000);}catch(e){if(!disposed)error.value=(e as Error).message;}finally{loading.value=false;}}
async function retry(item:Dict){if(busy.value)return;busy.value=true;try{await requestJson('/api/jobs/'+encodeURIComponent(item.job_id)+'/retry',{method:'POST',body:'{}'});emit('changed');void load();}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
function visibility(){if(document.hidden)clearTimeout(timer);else void load();}
const stop=registerNavigationGuard(()=>{if(!busy.value)return true;message.warning('正在提交重试，请等待结果');return false;});
onMounted(()=>{void load();document.addEventListener('visibilitychange',visibility);});onBeforeUnmount(()=>{disposed=true;clearTimeout(timer);controller?.abort();stop();document.removeEventListener('visibilitychange',visibility);});
</script>
