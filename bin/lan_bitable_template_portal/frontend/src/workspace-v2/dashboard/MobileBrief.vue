<template>
  <section v-if="entries.length" class="v2-mobile-brief" aria-label="当前业务概况">
    <header><h2>当前概况</h2><span>当前未结束 · 本月待发</span></header>
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }} <NButton text @click="load">重试</NButton></NAlert>
    <div class="v2-brief-grid">
      <NButton v-for="entry in entries" :key="entry.label" data-v2-navigation quaternary @click="$emit('open',entry.url)">
        <span>{{ entry.label }}</span><NSkeleton v-if="loading&&!ready" width="42px" height="28px" /><strong v-else>{{ value(entry.keys) ?? '—' }}</strong><ChevronRight :size="14" aria-hidden="true" />
      </NButton>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NSkeleton } from 'naive-ui';
import { ChevronRight } from 'lucide-vue-next';
import { requestJson,type Dict } from '../client';
import { NOTICE_IDS,count,beijingDate } from '../modules';
const props=defineProps<{user:Dict;scope:string}>();
defineEmits<{open:[url:string]}>();
const data=ref<Dict>({}),loading=ref(false),ready=ref(false),error=ref('');
const entries=computed(()=>{
  const notices=NOTICE_IDS.filter(id=>(props.user.modules||[]).includes(id));
  const noticeUrl='/workbench-lite?'+new URLSearchParams({scope:props.scope,work_type:notices.length===1?notices[0]:'all',month:Number(beijingDate().slice(5,7))+'月'});
  return [
    ...((props.user.modules||[]).includes('event')?[{url:'/?'+new URLSearchParams({view:'tasks',kind:'event',scope:props.scope}),label:'处理中事件',keys:['event_processing']}]:[]),
    ...(notices.length?[{url:noticeUrl,label:'进行中通告',keys:notices.map(id=>id+'_ongoing')},{url:noticeUrl,label:'本月待发计划',keys:notices.map(id=>id+'_pending')}]:[]),
  ];
});
let abort:AbortController|undefined;
function value(keys:string[]){const values=keys.map(key=>count(data.value[key]));return values.some(v=>v===null)?null:values.reduce<number>((sum,v)=>sum+v!,0);}
async function load(){
  abort?.abort();error.value='';
  if(!entries.value.length||document.hidden){loading.value=false;return;}
  const controller=new AbortController();abort=controller;loading.value=true;
  try{
    const result=await requestJson('/api/scope-overview?scope='+encodeURIComponent(props.scope),{signal:controller.signal});
    if(controller.signal.aborted)return;
    if(result.source_snapshot_ready===false||!result.scopes?.[props.scope])throw new Error('概况暂未就绪，不能判断为零。');
    data.value=result.scopes[props.scope];ready.value=true;
  }catch(e){if(!controller.signal.aborted)error.value=e instanceof Error?e.message:'概况读取失败';}
  finally{if(abort===controller)loading.value=false;}
}
function visibility(){if(document.hidden)abort?.abort();else void load();}
watch(()=>JSON.stringify([props.scope,props.user.open_id,props.user.modules]),()=>{data.value={};ready.value=false;void load();});
onMounted(()=>{void load();window.addEventListener('clipflow-business-changed',load);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{abort?.abort();window.removeEventListener('clipflow-business-changed',load);document.removeEventListener('visibilitychange',visibility);});
</script>
