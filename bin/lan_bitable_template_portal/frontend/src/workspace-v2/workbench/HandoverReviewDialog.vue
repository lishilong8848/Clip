<template>
  <NModal :show="true" preset="card" title="交接班审核" role="dialog" aria-label="交接班审核"
    class="review-scope-dialog" style="width:min(510px,calc(100vw - 24px))" closable @update:show="show=>{if(!show)emit('close')}">
    <template v-if="!loading && !error" #header-extra><span class="review-count">{{ links.length }} 个可用入口</span></template>
    <NSpin v-if="loading" size="small" description="正在读取审核入口" />
    <NAlert v-else-if="error" type="error" :bordered="false">
      {{ error }} <NButton text @click="load">重新读取</NButton>
    </NAlert>
    <div v-else-if="links.length" class="handover-buildings">
      <button v-for="link in links" :key="link.scope" type="button" @click="openReview(link.url)"><span>{{ link.label }}审核</span><ArrowUpRight :size="16" aria-hidden="true"/></button>
    </div>
    <NEmpty v-else description="当前授权楼栋暂无已配置的审核链接" />
  </NModal>
</template>
<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from 'vue';
import { ArrowUpRight } from 'lucide-vue-next';
import { NAlert, NButton, NEmpty, NModal, NSpin } from 'naive-ui';
import { requestJson } from '../client';
import { runNavigationAction } from '../navigation';

const props=defineProps<{scopes:{value:string;label:string}[]}>();
const emit=defineEmits<{close:[]}>();
const links=ref<{scope:string;label:string;url:string}[]>([]);
const loading=ref(true),error=ref('');
const controller=new AbortController();
async function load(){
  loading.value=true;
  error.value='';
  try{
    const data=await requestJson('/api/handover-links',{signal:controller.signal,cache:'no-store'});
    if(controller.signal.aborted)return;
    const allowed=new Set(props.scopes.map(item=>item.value));
    const options:{value:string;label:string}[]=data.scope_options||[];
    links.value=options.flatMap(({value,label})=>{
      const url=data.links?.[value];
      if(!(allowed.has('ALL')||allowed.has('CAMPUS')||allowed.has(value)) || typeof url!=='string' || !/^https?:\/\//i.test(url))return [];
      return [{scope:value,label,url}];
    });
  }catch(e){
    if(!controller.signal.aborted)error.value=e instanceof Error?e.message:'读取审核链接失败';
  }finally{if(!controller.signal.aborted)loading.value=false;}
}
function openReview(url:string){runNavigationAction(url,()=>window.location.assign(url));}
onMounted(load);
onBeforeUnmount(()=>controller.abort());
</script>
<style scoped>
.handover-buildings{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,104px),1fr));gap:8px}
.review-count{font-size:12px;color:var(--v2-muted)}
.handover-buildings button{display:flex;align-items:center;justify-content:space-between;gap:6px;min-height:44px;padding:8px 12px;border-radius:10px;border:1px solid var(--v2-border);background:var(--soft-field);color:var(--v2-ink);font:inherit;text-align:start;cursor:pointer;overflow-wrap:anywhere}
.handover-buildings button:focus-visible{outline:2px solid var(--v2-blue);outline-offset:2px}
.handover-buildings button>span{min-width:0}
.handover-buildings svg{flex:none;color:var(--v2-blue)}
@media(pointer:coarse){.handover-buildings button{min-height:48px}}
</style>
