<template>
  <section class="serenity-desk cold-desk" aria-label="冷源运行状态" data-v2-inline-feedback>
    <header class="serenity-heading">
      <div><h1>冷源运行状态</h1><p>当前授权范围内的湿球温度与运行模式</p></div>
      <div class="cold-tools"><div class="serenity-segment" role="group" aria-label="冷源视图">
      <button type="button" :aria-pressed="view==='readings'" @click="view='readings'">楼栋运行</button>
      <button type="button" :aria-pressed="view==='history'" @click="view='history'">通知记录</button>
      </div><NButton secondary :loading="loading" @click="load(page)"><RefreshCw :size="15"/>刷新</NButton></div>
    </header>
    <NAlert v-if="error" type="error" :bordered="false">{{ error }} <NButton text @click="load(page)">重试</NButton></NAlert>
    <div v-if="view==='readings'" class="cold-deck">
      <article v-for="row in items" :key="row.id" class="cold-reading">
        <header><span class="serenity-symbol"><Building2 :size="20"/></span><h2>{{ scopeLabel(row.scope) }}</h2><time>{{ row.date }}</time></header>
        <div class="cold-reading-values"><div><span>湿球温度</span><strong>{{ row.temperature ?? '—' }}<small v-if="row.temperature!=null"> °C</small></strong></div>
          <div><span>运行模式</span><b>{{ row.mode || '未记录' }}</b></div></div>
        <footer v-if="row.hint"><Info :size="14" aria-hidden="true"/>{{ row.hint }}</footer>
      </article>
    </div>
    <div v-else class="serenity-panel">
      <nav v-if="total>0" class="serenity-pager cold-pagination" aria-label="冷源通知分页"><span>共 {{ total }} 条 · 第 {{ page }} 页</span>
        <NButton size="small" :disabled="loading||page<=1" @click="load(page-1)">上一页</NButton>
        <NButton size="small" :disabled="loading||page*pageSize>=total" @click="load(page+1)">下一页</NButton>
      </nav>
      <ul v-if="jobs.length" class="cold-log">
        <li v-for="job in jobs" :key="job.id"><div><strong>{{ scopeLabel(job.row.scope) }} · {{ job.row.mode || '冷源通知' }}</strong>
          <span>{{ job.row.hint }}</span><small>{{ formatTime(job.created_at) }}</small></div>
          <div><b>{{ status(job.status) }}</b><small v-for="person in job.targets" :key="person.id">{{ person.name }} · {{ status(person.status) }}</small><small v-if="job.error">{{ job.error }}</small></div></li>
      </ul>
    </div>
    <NEmpty v-if="!error && !(view==='history'?jobs:items).length" :description="loading?'正在读取冷源数据':'当前范围暂无记录'" />
  </section>
</template>
<script setup lang="ts">
import {onMounted,onBeforeUnmount,ref} from 'vue';
import {NButton,NEmpty,NAlert} from 'naive-ui';
import {Building2,Info,RefreshCw} from 'lucide-vue-next';
import {requestJson} from './client';
type Reading={id:string;scope:string;date:string;temperature:number|null;mode:string;hint:string};
type Job={id:string;row:Reading;created_at:number;status:string;error?:string;targets:Record<string,{id:string;name:string;status:string}>};
const view=ref<'readings'|'history'>('readings'),items=ref<Reading[]>([]),jobs=ref<Job[]>([]);
const page=ref(1),total=ref(0),loading=ref(true),error=ref('');
const pageSize=10;
const scopeLabel=(scope:string)=>scope==='110'?'110站':scope+'楼';
const labels:Record<string,string>={sent:'已发送',pending:'等待发送',uncertain:'结果待核实',superseded:'数据已更新'};
const status=(key:string)=>labels[key]||key||'等待发送';
const formatTime=(value:number)=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(value*1000);
let read:AbortController|undefined;
async function load(next:number){
  read?.abort();
  const controller=new AbortController();read=controller;loading.value=true;error.value='';
  try {
    const data=await requestJson('/api/cold-source-monitor?page='+next,{signal:controller.signal,cache:'no-store'});
    if(controller.signal.aborted)return;
    items.value=data.items||[];jobs.value=data.history||[];page.value=data.history_page||1;total.value=data.history_total||0;
  }catch(e){if(!controller.signal.aborted)error.value=e instanceof Error?e.message:'冷源数据读取失败';}
  finally{if(!controller.signal.aborted)loading.value=false;}
}
onMounted(()=>load(1));onBeforeUnmount(()=>read?.abort());
</script>
<style scoped>
.cold-tools {display:flex;align-items:center;gap:6px;flex-wrap:wrap;min-width:0}
.cold-pagination{margin:0 0 6px;align-items:center;flex-wrap:wrap}
.cold-deck{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,235px),1fr));gap:12px}
.cold-reading{border-radius:16px;background:var(--soft-face);box-shadow:var(--soft-raised);padding:14px}
.cold-reading header,.cold-reading footer{display:flex;gap:8px;align-items:center}
.cold-reading h2{margin:0;font-size:16px}.cold-reading time{margin-left:auto;color:var(--v2-muted);font-size:12px}
.cold-reading-values{display:grid;grid-template-columns:1fr 1fr;align-items:start;gap:10px;margin:12px 0}
.cold-reading-values>div,.cold-log li>div{display:grid;gap:4px;min-width:0}.cold-reading-values span,.cold-log small{color:var(--v2-muted);font-size:12px}
.cold-reading-values strong{font-size:30px;line-height:1.15}.cold-reading-values small,.cold-reading-values b{font-size:15px}.cold-reading footer{font-size:13px;border-top:1px solid var(--soft-edge);padding-top:8px}
.cold-log{list-style:none;padding:0;margin:0}.cold-log li{display:flex;justify-content:space-between;gap:10px;border-bottom:1px solid var(--soft-edge);padding:9px 0}
</style>
