<template>
  <section class="water-overview" aria-label="水耗汇总" :aria-busy="loading">
    <header><div><h2>{{ scopeLabel }}水耗汇总</h2><span>{{ waterDay(-29) }} 至 {{ waterDay() }} · 近30天</span></div><NButton size="small" :loading="loading" @click="load(true)"><RefreshCw :size="15" />刷新汇总</NButton></header>
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }}{{ ready ? '；以下保留上次读取的汇总。' : '' }}<NButton text @click="load(true)">重新读取</NButton></NAlert>
    <div class="water-overview-metrics">
      <NStatistic label="近30天日统计耗水量"><NSkeleton v-if="loading&&!ready" width="120px" height="30px" /><span v-else>{{ waterNumberText(totalUsage,2) }} <small>t</small></span></NStatistic>
      <NStatistic label="近30天日记录"><NSkeleton v-if="loading&&!ready" width="80px" height="30px" /><span v-else>{{ waterNumberText(recordCount,0) }} <small>条</small></span></NStatistic>
      <NStatistic label="最近统计"><span class="water-overview-date">{{ dateText(latestDate) }}</span></NStatistic>
    </div>
    <NEmpty v-if="!available.length" description="当前账号没有可访问的水耗楼栋" />
    <NEmpty v-else-if="!loading&&!ready&&!error" description="等待首次水耗数据同步" />
    <div v-else class="water-overview-body">
      <MotionChart title="近30天每日耗水量" :caption="scopeLabel+(trend.missing?' · 部分耗水量未取得':'')" :items="trend.items" trend view="area" unit="t" :loading="loading&&!ready" :failed="!!error || trend.missing>0" @select="item=>selectedDay=item.label" />
      <section class="water-building-list" aria-label="楼栋水耗明细">
        <NButton v-if="selectedDay" text size="small" @click="selectedDay=''">{{selectedDay}} · 返回30天合计</NButton>
        <div class="water-building-head"><span>楼栋</span><span>{{selectedDay||'近30天'}}耗水量</span><span>记录</span><span aria-hidden="true"></span></div>
        <NButton v-for="row in rows" :key="row.value" data-v2-navigation quaternary class="water-building-row" :aria-label="'查看'+row.label+'水耗记录'" @click="open(row.value)"><span class="water-building-name"><Building2 :size="17" /><strong>{{ row.label }}</strong></span><span class="water-building-usage">{{ waterNumberText(ready?row.month_total_usage:null,2) }} <small>t</small></span><span class="water-building-count">{{ waterNumberText(ready?row.month_record_count:null,0) }} <small>条</small></span><ChevronRight :size="15" /></NButton>
      </section>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed,defineAsyncComponent,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NSkeleton,NStatistic } from 'naive-ui';
import { Building2,ChevronRight,RefreshCw } from 'lucide-vue-next';
import { requestJson,type Dict } from './client';
import { usePageReadRefresh } from '../api/usePageReadRefresh';
import { waterDay,waterNumber,waterNumberText,waterScopes,waterTrend } from './water';
const MotionChart=defineAsyncComponent(()=>import('./MotionChart.vue'));
const props=defineProps<{scope:string;scopeOptions:{value:string;label:string}[]}>();
const emit=defineEmits<{open:[scope:string]}>();
const data=ref<Dict>({}),loading=ref(false),error=ref(''),selectedDay=ref('');
const available=computed(()=>props.scopeOptions.filter(row=>waterScopes.has(row.value)&&(['ALL','CAMPUS'].includes(props.scope)||row.value===props.scope)));
const periodRows=computed<Dict[]>(()=>data.value.records||[]);
const trend=computed(()=>waterTrend(periodRows.value));
const rows=computed<Dict[]>(()=>available.value.map(scope=>{const records=periodRows.value.filter(row=>row.scope===scope.value&&(!selectedDay.value||String(row.statistic_date_key||row.statistic_date).slice(0,10)===selectedDay.value));const result=waterTrend(records);return {...scope,month_total_usage:result.missing?null:result.items.reduce((sum,row)=>sum+row.value,0),month_record_count:records.length,latest_date_ms:Math.max(0,...records.map(row=>Date.parse(String(row.statistic_date_key||row.statistic_date).slice(0,10)+'T00:00:00+08:00')||0))};}));
const ready=computed(()=>data.value.snapshot?.exists===true);
const scopeLabel=computed(()=>['ALL','CAMPUS'].includes(props.scope)?'授权楼栋':props.scopeOptions.find(row=>row.value===props.scope)?.label||'当前范围');
const totalUsage=computed(()=>ready.value&&!trend.value.missing?trend.value.items.reduce((sum,row)=>sum+row.value,0):null),recordCount=computed(()=>ready.value?periodRows.value.length:null);
const latestDate=computed(()=>!ready.value?null:Math.max(0,...periodRows.value.map(row=>Date.parse(String(row.statistic_date_key||row.statistic_date).slice(0,10)+'T00:00:00+08:00')||0)));
function dateText(value:number|null){return value?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(value)):'—';}
function open(scope:string){if(available.value.some(row=>row.value===scope))emit('open',scope);}
let read:AbortController|undefined,disposed=false,poll=0;
async function load(fresh=false){
  if(disposed||document.hidden||!available.value.length)return;
  read?.abort();clearTimeout(poll);const c=new AbortController();read=c;loading.value=true;error.value='';
  try{
    const results=await Promise.all(available.value.map(async building=>{
      const records:Dict[]=[];let page=1,total=1;
      while(records.length<total){
        const params=new URLSearchParams({scope:building.value,start_date:waterDay(-29),end_date:waterDay(),frequency:'日',page:String(page),page_size:'200'});
        const result=await requestJson('/api/capacity/water/records?'+params,{signal:c.signal,fresh});
        if(!Array.isArray(result.records)||!Number.isInteger(result.total)||result.total<0||result.snapshot?.exists!==true)throw new Error(building.label+'近30天数据尚未完整取得。');
        total=result.total;if(total>10000)throw new Error(building.label+'近30天记录过多，请进入记录办理缩小范围。');
        if(!result.records.length&&records.length<total)throw new Error(building.label+'分页数据不完整。');
        records.push(...result.records.map((row:Dict)=>({...row,scope:building.value})));page++;
      }
      return records;
    }));
    if(c.signal.aborted||disposed)return;
    data.value={records:results.flat(),snapshot:{exists:true}};
  }catch(e){if(!c.signal.aborted&&!disposed)error.value=e instanceof Error?e.message:'水耗汇总读取失败。';}
  finally{if(read===c)loading.value=false;}
}
function visibility(){clearTimeout(poll);if(document.hidden)read?.abort();else if(!ready.value||data.value.snapshot?.refreshing)void load(true);}
watch(()=>[props.scope,props.scopeOptions],()=>{read?.abort();data.value={};selectedDay.value='';void load();});
usePageReadRefresh(url=>url.pathname.startsWith('/api/capacity/water/'),()=>load(),()=>!loading.value);
onMounted(()=>{void load();document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;read?.abort();clearTimeout(poll);document.removeEventListener('visibilitychange',visibility);});
</script>
<style scoped>
.water-overview{min-width:0}.water-overview>header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 0}.water-overview h2{font-size:17px;margin:0 0 5px}.water-overview header span{font-size:12px;color:var(--v2-muted)}.water-overview header :deep(.n-button__content){gap:6px}.water-overview-metrics{display:grid;grid-template-columns:1.2fr 1fr 1fr;gap:20px;padding:14px 0 24px;border-bottom:1px solid var(--v2-border)}.water-overview-metrics :deep(.n-statistic-value){font-size:28px;font-variant-numeric:tabular-nums}.water-overview-metrics small{font-size:13px;color:var(--v2-muted);font-weight:400}.water-overview-date{font-size:20px}.water-overview-body{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(320px,1fr);gap:32px;padding:24px 0}.water-building-list{min-width:0}.water-building-head,.water-building-row :deep(.n-button__content){display:grid;grid-template-columns:90px minmax(0,1fr) 82px 16px;gap:12px;align-items:center;width:100%}.water-building-head{padding:8px 12px;color:var(--v2-muted);font-size:12px;border-bottom:1px solid var(--v2-border)}.water-building-head>span:nth-child(2),.water-building-head>span:nth-child(3){text-align:right}.water-building-row.n-button{width:100%;height:54px;padding:10px 12px;border-bottom:1px solid var(--v2-border);border-radius:0;text-align:left}.water-building-name{display:flex;gap:8px;align-items:center;color:var(--v2-ink)}.water-building-name svg{color:var(--v2-blue)}.water-building-usage,.water-building-count{text-align:right;font-variant-numeric:tabular-nums}.water-building-usage{font-weight:600}.water-building-row small{font-size:11px;color:var(--v2-muted);font-weight:400}.water-building-row :deep(.n-button__content)>svg{color:var(--v2-muted)}
@media(max-width:1100px){.water-overview-body{grid-template-columns:minmax(0,1fr);gap:16px}}
@media(max-width:760px){.water-overview>header{padding:12px 0}.water-overview h2{font-size:16px}.water-overview-metrics{gap:12px;padding:8px 0 16px;grid-template-columns:1fr 1fr}.water-overview-metrics>.n-statistic:last-child{grid-column:1/-1;display:flex;align-items:baseline;gap:12px}.water-overview-metrics :deep(.n-statistic-value){font-size:23px}.water-overview-date{font-size:14px}.water-overview-body{padding:16px 0}.water-building-head,.water-building-row :deep(.n-button__content){grid-template-columns:68px minmax(0,1fr) 66px 14px;gap:8px}.water-building-row.n-button{padding:10px 8px}.water-building-head{padding-inline:8px}.water-building-name{gap:5px}}
</style>
