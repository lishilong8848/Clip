<template>
  <section class="release-journal" aria-label="开发问题与更新记录">
    <header class="release-heading">
      <h2><NotebookPen :size="19" aria-hidden="true" />开发问题与更新记录</h2>
      <NButton size="small" :loading="loading" aria-label="刷新更新记录" @click="load(true)"><template #icon><RefreshCw :size="16" /></template>刷新</NButton>
    </header>
    <div class="release-filters">
      <NInput v-model:value="query" clearable placeholder="搜索版本、问题或解决方法" aria-label="搜索更新记录"><template #prefix><Search :size="16" /></template></NInput>
      <NSelect v-model:value="moduleFilter" :options="moduleOptions" clearable placeholder="全部模块" aria-label="筛选模块" />
      <span class="release-count">{{ total }} 个版本</span>
    </div>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }} <NButton text @click="load(true)">重试</NButton></NAlert>
    <NSpin :show="loading && !items.length">
      <div v-if="items.length" class="release-list">
        <button v-for="entry in items" :key="entry.id" type="button" class="release-entry" @click="openDetail(entry)">
          <span class="release-version"><NTag size="small" :bordered="false" type="info">补丁 {{ entry.patch }}</NTag><strong>{{ entry.version }}</strong></span>
          <span class="release-summary">{{ summary(entry) }}</span>
          <span class="release-meta"><time>{{ publishedAt(entry.published_at) }}</time><ChevronRight :size="17" /></span>
        </button>
      </div>
      <NEmpty v-else-if="!loading && !error" description="暂无已发布的更新记录" />
    </NSpin>
    <NPagination v-if="total > pageSize" v-model:page="page" :page-size="pageSize" :item-count="total" :page-slot="5" class="release-pages" aria-label="更新记录分页" />
    <NModal v-model:show="detailVisible" preset="card" class="release-detail" :title="detail?.version || '更新详情'" :style="{ width:'min(960px, calc(100vw - 24px))' }">
      <NSpin :show="detailLoading">
        <NAlert v-if="detailError" type="error" :bordered="false" role="alert">{{ detailError }}</NAlert>
        <template v-if="detail">
          <p class="release-detail-meta">补丁 {{ detail.patch }} · {{ publishedAt(detail.published_at) }}</p>
          <article v-for="(item, index) in detail.items || []" :key="index" class="release-issue">
            <h3>{{ item.module || '通用' }}</h3>
            <dl><div><dt>问题</dt><dd>{{ item.problem }}</dd></div><div><dt>解决方法</dt><dd>{{ item.solution }}</dd></div></dl>
          </article>
          <NCollapse v-if="detail.commits?.length"><NCollapseItem title="关联提交" name="commits"><ul class="release-commits"><li v-for="(commit, index) in detail.commits" :key="index"><code>{{ (commit.id || commit.hash || commit.sha || '').slice(0, 8) }}</code><span>{{ commit.subject || commit.title || commit.message }}</span></li></ul></NCollapseItem></NCollapse>
        </template>
      </NSpin>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NCollapse, NCollapseItem, NEmpty, NInput, NModal, NPagination, NSelect, NSpin, NTag } from 'naive-ui';
import { ChevronRight, NotebookPen, RefreshCw, Search } from 'lucide-vue-next';
import { ApiError, requestJson } from '../api/client';
type Issue = { problem:string; solution:string; module:string };
type Release = { id:string; version:string; patch:number; published_at:string; items:Issue[]; commits?:Array<{id?:string;hash?:string;sha?:string;subject?:string;title?:string;message?:string}> };
const query=ref(''),moduleFilter=ref<string|null>(null),page=ref(1),pageSize=20,total=ref(0),items=ref<Release[]>([]),modules=ref<string[]>([]);
const loading=ref(false),error=ref(''),detail=ref<Release|null>(null),detailVisible=ref(false),detailLoading=ref(false),detailError=ref('');
const moduleOptions=computed(()=>modules.value.map(value=>({label:value,value})));
let read:AbortController|undefined,detailRead:AbortController|undefined,timer=0;
function summary(entry:Release){return (entry.items||[]).map(item=>item.problem).filter(Boolean).slice(0,2).join('；')||'查看问题及解决方法';}
function publishedAt(value:string){const time=new Date(value);return value&&!Number.isNaN(time.getTime())?time.toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'发布时间待确认';}
async function load(fresh=false){
  read?.abort();const current=new AbortController();read=current;loading.value=true;error.value='';
  try{
    const params=new URLSearchParams({page:String(page.value),q:query.value.trim(),module:moduleFilter.value||''});
    const result=await requestJson('/api/release-journal?'+params,{signal:current.signal,fresh});
    if(current.signal.aborted)return;
    items.value=result.items||[];total.value=result.total||0;modules.value=result.modules||[];
    if(result.page&&result.page!==page.value)page.value=result.page;
  }catch(exc){
    if(current.signal.aborted)return;
    error.value=exc instanceof Error?exc.message:'更新记录读取失败';
    if(exc instanceof ApiError&&[401,403].includes(exc.status)){items.value=[];total.value=0;detail.value=null;detailVisible.value=false;}
  }finally{if(read===current)loading.value=false;}
}
async function openDetail(entry:Release){
  detailRead?.abort();const current=new AbortController();detailRead=current;detail.value=null;detailError.value='';detailVisible.value=true;detailLoading.value=true;
  try{
    const result=await requestJson('/api/release-journal/'+encodeURIComponent(entry.id),{signal:current.signal,fresh:true});
    if(typeof result.id!=='string'||typeof result.version!=='string'||!Array.isArray(result.items))throw new Error('更新详情未完整返回，请重试');
    if(!current.signal.aborted)detail.value=result as Release;
  }
  catch(exc){if(!current.signal.aborted)detailError.value=exc instanceof Error?exc.message:'详情读取失败';}
  finally{if(detailRead===current)detailLoading.value=false;}
}
watch([query,moduleFilter],()=>{window.clearTimeout(timer);timer=window.setTimeout(()=>{if(page.value!==1)page.value=1;else void load();},250);});
watch(page,()=>void load());
watch(detailVisible,value=>{if(!value)detailRead?.abort();});
onMounted(()=>void load());
onBeforeUnmount(()=>{read?.abort();detailRead?.abort();window.clearTimeout(timer);});
</script>
<style scoped>
.release-journal{display:grid;gap:14px;min-width:0;color:var(--v2-text,#263740)}
.release-heading,.release-version,.release-meta{display:flex;align-items:center;gap:10px;min-width:0}
.release-heading{justify-content:space-between}.release-heading h2{display:flex;align-items:center;gap:8px;margin:0;font-size:18px;line-height:1.4}
.release-filters{display:grid;grid-template-columns:minmax(190px,1fr) minmax(150px,220px) auto;align-items:center;gap:10px}
.release-count,.release-detail-meta{font-size:13px;color:var(--v2-muted,#64747c)}
.release-list{display:grid;gap:0;border-top:1px solid var(--v2-border,#dce4e7)}
.release-entry{display:grid;grid-template-columns:minmax(200px,1fr) minmax(180px,1.5fr) auto;align-items:center;gap:16px;width:100%;padding:16px 8px;border:0;border-bottom:1px solid var(--v2-border,#dce4e7);border-radius:0;background:transparent;color:inherit;text-align:left;cursor:pointer;font:inherit;transition:background .18s}
.release-entry:hover{background:var(--v2-hover,#f0f6fa)}.release-entry:focus-visible{outline:2px solid var(--v2-primary,#2974c8);outline-offset:-2px}
.release-version{flex-wrap:wrap}.release-version strong{font-size:14px;overflow-wrap:anywhere}.release-summary{font-size:14px;line-height:1.6;overflow-wrap:anywhere}.release-meta{font-size:12px;color:var(--v2-muted,#64747c)}.release-meta time{max-width:145px;overflow-wrap:anywhere}
.release-pages{justify-self:end}.release-issue{padding:12px 0;border-bottom:1px solid var(--v2-border,#dce4e7)}.release-issue h3{margin:0 0 10px;font-size:15px}.release-issue dl{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin:0}.release-issue dt{font-size:12px;color:var(--v2-muted,#64747c);margin-bottom:6px}.release-issue dd{margin:0;font-size:14px;line-height:1.7;white-space:pre-wrap;overflow-wrap:anywhere}.release-commits{list-style:none;padding:0;display:grid;gap:8px}.release-commits li{display:flex;gap:12px;overflow-wrap:anywhere}.release-commits code{flex:none}
:global(.release-detail .n-card__content){max-height:calc(100dvh - 180px);overflow:auto}
@media(max-width:760px){.release-filters{grid-template-columns:minmax(0,1fr) minmax(120px,.6fr)}.release-count{grid-column:1/-1}.release-entry{grid-template-columns:1fr;gap:7px;padding:12px 4px}.release-meta{justify-content:space-between}.release-meta time{max-width:none}.release-issue dl{grid-template-columns:1fr;gap:12px}.release-heading h2{font-size:16px}.release-pages{justify-self:center}}
@media(prefers-reduced-motion:reduce){.release-entry{transition:none}}
</style>
