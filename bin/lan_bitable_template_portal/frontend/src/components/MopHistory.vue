<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { Download, RefreshCw, Search } from 'lucide-vue-next';
import { downloadFile, requestJson } from '../api/client';
import RecordViewSwitch from './RecordViewSwitch.vue';

type Attachment = {id:string;name:string};
type Row = {record_id:string;title:string;building:string;specialty:string;cycle:string;status:string;planned:string;attachments:Attachment[]};
const props = defineProps<{scope:string; scopeOptions:{label:string;value:string}[]}>();
defineEmits<{'update:scope':[scope:string]}>();
const view = ref<'cards'|'table'>('cards');
const search = ref(''), query = ref(''), specialty = ref(''), options = ref<string[]>([]);
const page = ref(1), total = ref(0), pageSize = ref(20), revision = ref(0);
const records = ref<Row[]>([]), busy = ref(false), error = ref(''), downloadError = ref(''), downloading = ref('');
const pages = computed(()=>Math.max(1,Math.ceil(total.value/pageSize.value)));
function applySearch() {
  page.value=1;
  query.value=search.value.trim();
}
watch(()=>props.scope,()=>{
  downloadRequest?.abort();
  page.value=1;
  specialty.value='';
  options.value=[];
});
const endpoint='/api/engineer/mop/history';
watch([()=>props.scope,query,specialty,page,revision],async ([scope,term,major,number,tick],old,onCleanup)=>{
  const controller=new AbortController();
  onCleanup(()=>controller.abort());
  busy.value=true;
  records.value=[];
  error.value=downloadError.value='';
  try {
    const params=new URLSearchParams({scope,search:term,specialty:major,page:String(number)});
    if(old?.length && tick!==old[4])params.set('refresh','1');
    const data=await requestJson(endpoint+'?'+params,{signal:controller.signal,timeoutMs:30000,cache:'no-store'});
    if(controller.signal.aborted)return;
    if(data.scope!==scope||!Array.isArray(data.records)||!Array.isArray(data.specialties))throw new Error('填写记录未完整返回。');
    records.value=data.records;
    options.value=data.specialties;
    total.value=data.total;
    pageSize.value=data.page_size;
    if(data.page!==number)page.value=data.page;
  } catch(cause) {
    if(!controller.signal.aborted)error.value=cause instanceof Error?cause.message:'填写记录读取失败。';
  } finally {
    if(!controller.signal.aborted)busy.value=false;
  }
},{immediate:true});
let downloadRequest:AbortController|undefined;
async function download(row:Row,file:Attachment) {
  if(downloading.value)return;
  const request=downloadRequest=new AbortController();
  downloading.value=row.record_id+file.id;
  downloadError.value='';
  try {
    await downloadFile(endpoint+'/'+encodeURIComponent(row.record_id)+'/'+encodeURIComponent(file.id)+'?'
      +new URLSearchParams({scope:props.scope}),{signal:request.signal,timeoutMs:60000});
  } catch(cause) {
    if(!request.signal.aborted)downloadError.value=cause instanceof Error?cause.message:'附件下载失败，请重试。';
  } finally {
    downloading.value='';
  }
}
onBeforeUnmount(()=>downloadRequest?.abort());
</script>

<template>
  <section class="mop-history" aria-labelledby="mop-history-heading" :aria-busy="busy">
    <header class="history-toolbar">
      <h2 id="mop-history-heading">填写记录</h2>
      <form class="history-filters" aria-label="填写记录筛选" @submit.prevent="applySearch">
        <select v-if="scopeOptions.length" data-native-select :value="scope" aria-label="填写记录楼栋"
          @change="$emit('update:scope',($event.target as HTMLSelectElement).value)">
          <option v-for="item in scopeOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
        </select>
        <select v-model="specialty" data-native-select aria-label="筛选专业" @change="page=1">
          <option value="">全部专业</option>
          <option v-for="option in options" :key="option" :value="option">{{ option }}</option>
        </select>
        <input v-model="search" type="search" aria-label="搜索填写记录" placeholder="通告、日期或附件">
        <button type="submit" aria-label="查询填写记录" title="查询"><Search :size="16" aria-hidden="true"/></button>
      </form>
      <RecordViewSwitch v-model="view"/>
      <button type="button" aria-label="刷新填写记录" title="刷新填写记录" :disabled="busy" @click="revision++"><RefreshCw :size="16" aria-hidden="true"/></button>
    </header>
    <p v-if="downloadError" role="alert" class="history-feedback">{{ downloadError }}</p>
    <p v-if="error" role="alert" class="history-feedback">{{ error }} <button type="button" @click="revision++">重试</button></p>
    <p v-else-if="busy || !records.length" role="status" class="history-feedback">{{ busy?'正在读取填写记录…':'暂无符合条件的已上传维保单' }}</p>
    <div v-else class="history-records" :data-view="view" :role="view==='table'?'table':'list'" aria-label="已上传维保单">
      <div v-if="view==='table'" class="history-columns" role="row">
        <b role="columnheader">维保通告</b><b role="columnheader">楼栋 / 专业</b><b role="columnheader">计划日期 / 状态</b><b role="columnheader">附件下载</b>
      </div>
      <div v-for="row in records" :key="row.record_id" class="history-row" :role="view==='table'?'row':'listitem'">
        <strong :role="view==='table'?'cell':undefined">{{ row.title }}</strong>
        <span :role="view==='table'?'cell':undefined">{{ [row.building,row.specialty,row.cycle].filter(Boolean).join(' · ') }}</span>
        <span :role="view==='table'?'cell':undefined">{{ [row.planned,row.status].filter(Boolean).join(' · ') || '已上传' }}</span>
        <div class="history-files" :role="view==='table'?'cell':undefined">
          <button v-for="file in row.attachments" :key="file.id" type="button" :disabled="!!downloading"
            :aria-label="'下载 '+file.name" @click="download(row,file)">
            <Download :size="15" aria-hidden="true"/>{{ downloading===row.record_id+file.id?'下载中…':file.name }}
          </button>
        </div>
      </div>
    </div>
    <footer v-if="!busy&&!error" class="history-pagination">
      <span>共 {{ total }} 条</span>
      <button type="button" :disabled="page<=1" @click="page--">上一页</button>
      <span>{{ page }} / {{ pages }}</span>
      <button type="button" :disabled="page>=pages" @click="page++">下一页</button>
    </footer>
  </section>
</template>

<style scoped>
.mop-history{display:grid;gap:12px;min-width:0;color:var(--v2-ink,#243149)}
.history-toolbar,.history-filters,.history-pagination{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.history-toolbar h2{margin:0 auto 0 0;font-size:1.1rem}
.history-filters{max-width:100%}
.history-filters input,.history-filters select,.mop-history button:not(.rvs-btn){min-height:34px;padding:5px 8px;border:1px solid var(--v2-border,#d4dbe4);
  border-radius:7px;background:var(--soft-face,#fff);color:inherit;max-width:100%;font:inherit}
.mop-history button{cursor:pointer}
.mop-history button:disabled{opacity:.64;cursor:default}
.history-records[data-view=cards]{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(285px,100%),1fr));gap:8px}
.history-row,.history-columns{min-width:0;display:grid;align-items:start;gap:6px;padding:10px 12px;overflow-wrap:anywhere}
.history-row{border:1px solid var(--v2-border,#d4dbe4);border-radius:9px;background:var(--soft-face,#fff)}
.history-row>span,.history-pagination,.history-feedback{color:var(--v2-muted,#516177);font-size:.88rem}
.history-records[data-view=table] :is(.history-row,.history-columns){grid-template-columns:minmax(0,1.3fr) minmax(0,.8fr) minmax(0,.85fr) minmax(0,1fr);gap:8px;border-radius:0}
.history-columns{background:var(--soft-field,#f2f4f7);font-size:.85rem}
.history-files{display:flex;gap:4px;flex-wrap:wrap;min-width:0}
.history-files button{display:inline-flex;align-items:center;gap:5px;text-align:left;min-width:0;overflow-wrap:anywhere}
.history-files svg{flex:none;color:var(--v2-palette-accent-text,#19704c)}
.history-pagination{justify-content:flex-end}
.history-pagination span:first-child{margin-inline-end:auto}
.history-feedback{margin:4px 0}
@media(max-width:660px){
  .history-filters{order:1;width:100%;display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr) auto}
  .history-filters input{grid-column:1/3;width:100%}
  .history-records[data-view=table] .history-columns{display:none}
  .history-records[data-view=table] .history-row{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
  .history-records[data-view=table] .history-row>strong,.history-records[data-view=table] .history-files{grid-column:1/-1}
}
@media(pointer:coarse){.history-filters input,.history-filters select,.mop-history button:not(.rvs-btn){min-height:44px}}
</style>
