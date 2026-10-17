<template>
  <div ref="host" class="v2-metric-widget" :class="{'is-calendar-widget':item.display==='calendar','is-compact-calendar':compactCalendar}" :style="item.display==='calendar'?{height:capacity.height+'px'}:undefined">
    <div v-if="!compactCalendar" class="v2-metric-toolbar"><span>{{ item.kind==='events'?'每日事件 · 条':item.metric==='ongoing'?'当前进行中 · 条':'本月待发计划 · 条' }}</span><NRadioGroup v-if="item.kind==='events'" v-model:value="period" size="small" aria-label="事件图表时间范围"><NRadioButton value="week">本周</NRadioButton><NRadioButton value="month">本月</NRadioButton><NRadioButton v-if="item.display==='calendar'" value="year">本年</NRadioButton></NRadioGroup><NButton text :loading="loading" aria-label="刷新图表" @click="load"><RefreshCw :size="15" /></NButton></div>
    <NAlert v-if="error" type="warning" :bordered="false">{{ error }}<NButton text @click="load">重新读取</NButton></NAlert>
    <NAlert v-if="unknownCount&&!compactCalendar" type="warning" :bordered="false">{{unknownCount}} 条事件缺少有效发生时间，未计入图表。<NButton text @click="reviewDates">核对事件</NButton></NAlert>
    <NButton v-else-if="unknownCount" class="v2-calendar-warning" text :title="unknownCount+' 条事件缺少发生时间，未计入图表'" :aria-label="unknownCount+' 条事件未计入，打开事件核对'" @click="reviewDates"><CircleAlert :size="15" /></NButton>
    <NButton v-if="item.display==='number'" text class="v2-number-widget" :disabled="unavailable||!ready" @click="drill()"><NNumberAnimation v-if="ready&&!unavailable" :from="0" :to="total" :duration="reduced?0:650" /><span v-else>—</span><small>{{ item.kind==='events'?(unknownCount?'已核验日期事件':'事件总数'):item.metric==='ongoing'?'进行中通告':'待发计划' }}</small></NButton>
    <MotionChart v-else title="" :items="items" :loading="loading||!ready&&!error" :failed="unavailable" :view="item.display" :fit="item.display==='calendar'" :details="item.display!=='calendar'" @select="drill" />
    <footer v-if="!compactCalendar" class="v2-widget-source"><span>{{ stamp || '本地业务快照' }}</span><NButton text type="primary" :disabled="!ready||unavailable" @click="drill()">查看明细 <ArrowUpRight :size="14" /></NButton></footer>
    <NModal v-model:show="showDetails" preset="card" class="v2-chart-records" :title="detailTitle" :bordered="false"><NList><NListItem v-for="row in details" :key="eventRecordId(row)"><NButton text class="v2-row-title" @click="openEvent(row)">{{ row.title||row.name||row.summary||row.overview||'事件记录' }}</NButton><div class="v2-source-note">{{ row.building||row.scope }} · {{ row.status }} · {{ row.occurrence_time }}</div></NListItem></NList><NEmpty v-if="!details.length" description="该时段没有事件记录" /></NModal>
  </div>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NList,NListItem,NModal,NNumberAnimation,NRadioButton,NRadioGroup } from 'naive-ui';
import { ArrowUpRight,CircleAlert,RefreshCw } from 'lucide-vue-next';
import { requestJson,type Dict } from '../client';
import { modules,NOTICE_IDS,count,moduleUrl } from '../modules';
import { eventCalendarRange,eventPeriodRange,eventTrend,mergeEventMonths,occurrenceTimestamp } from '../eventPeriod';
import { eventRecordId } from '../../eventManagementUtils';
import MotionChart from '../MotionChart.vue';
import type { Widget } from './layout';
import {useCapacity} from '../pageLayout';
import {eventSnapshot} from '../eventSnapshots';
const props=defineProps<{item:Widget;scope:string;user:Dict}>();
const emit=defineEmits<{open:[url:string]}>();
const host=ref<HTMLElement>(),loading=ref(false),ready=ref(false),error=ref(''),stamp=ref(''),period=ref(props.item.period);
const unknownCount=ref(0),capacity=useCapacity();
const compactCalendar=computed(()=>props.item.display==='calendar'&&(capacity.value.width<380||capacity.value.height<240));
const unavailable=computed(()=>Boolean(error.value)||unknownCount.value>0&&!records.value.length);
const items=ref<Array<{label:string;value:number;url?:string;start?:number;end?:number}>>([]),records=ref<Dict[]>([]),details=ref<Dict[]>([]),showDetails=ref(false),detailTitle=ref('事件明细');
const total=computed(()=>items.value.reduce((sum,item)=>sum+item.value,0));
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
let observer:IntersectionObserver|undefined,abort:AbortController|undefined,visible=false,disposed=false,stale=true;
async function load(){
  if(!visible||disposed||document.hidden)return;
  abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';stale=true;
  try{
    let next:typeof items.value=[],nextRecords:Dict[]=[],nextStamp='',nextUnknown=0;
    if(props.item.kind==='events'){
      const range=props.item.display==='calendar'?eventCalendarRange(period.value):eventPeriodRange(period.value==='year'?'month':period.value),payloads:Dict[]=[];
      for(const month of range.months)payloads.push(await eventSnapshot(props.scope,month,controller.signal));
      const data=mergeEventMonths(payloads,range);nextUnknown=Number(data.unknown_time_count||0);
      nextRecords=data.records;next=eventTrend(nextRecords,range,props.item.display==='calendar');nextStamp=range.caption;
    }else{
      const data=await requestJson('/api/scope-overview?scope='+encodeURIComponent(props.scope),{signal:controller.signal});
      if(data.source_snapshot_ready===false)throw new Error('本地资料尚未准备完成，不能判断为零。');
      const values=data.scopes?.[props.scope];if(!values)throw new Error('所选楼栋统计尚未返回。');
      for(const entry of modules.filter(m=>NOTICE_IDS.includes(m.id)&&(props.user.modules||[]).includes(m.id))){
        const value=count(values[entry.id+'_'+props.item.metric]);if(value===null)throw new Error('部分通告统计暂不可用，请进入业务页核对。');
        next.push({label:entry.label,value,url:moduleUrl(entry,props.scope)});
      }
      nextStamp=String(data.last_loaded_at||'当前状态');
    }
    if(controller.signal.aborted)return;items.value=next;records.value=nextRecords;unknownCount.value=nextUnknown;stamp.value=nextStamp;ready.value=true;stale=false;
  }catch(e){if(!controller.signal.aborted)error.value=e instanceof Error?e.message:'统计读取失败，不能判断为零。';}
  finally{if(abort===controller)loading.value=false;}
}
function drill(item?:typeof items.value[number]){
  if(!ready.value||unavailable.value)return;
  if(props.item.kind==='notices'){emit('open',item?.url||'/workbench-lite?'+new URLSearchParams({scope:props.scope,work_type:(props.user.modules||[]).filter((id:string)=>NOTICE_IDS.includes(id)).length===1?(props.user.modules||[]).find((id:string)=>NOTICE_IDS.includes(id)):'all'}));return;}
  details.value=item?.start!==undefined?records.value.filter(r=>{const t=occurrenceTimestamp(r.occurrence_time);return t>=item.start!&&t<item.end!;}):records.value;
  detailTitle.value=(item?.label||'全部')+' · 事件明细';showDetails.value=true;
}
function openEvent(row:Dict){emit('open','/?'+new URLSearchParams({mode:'events',detail:'1',scope:props.scope,record_id:eventRecordId(row),month:String(row.occurrence_time).slice(0,7)}));}
function reviewDates(){emit('open','/?'+new URLSearchParams({mode:'events',detail:'1',scope:props.scope}));}
watch(()=>[props.item.period,props.item.display],()=>period.value=props.item.period==='year'&&props.item.display!=='calendar'?'month':props.item.period);
watch(()=>[props.scope,props.item.kind,props.item.metric,period.value,props.item.display==='calendar'],()=>{abort?.abort();ready.value=false;items.value=[];records.value=[];unknownCount.value=0;showDetails.value=false;void load();});
function changed(){stale=true;void load();}
function visibility(){if(document.hidden)abort?.abort();else if(stale||!ready.value)void load();}
onMounted(()=>{observer=new IntersectionObserver(entries=>{visible=entries.some(e=>e.isIntersecting);if(visible&&(stale||!ready.value)&&!loading.value)void load();},{rootMargin:'80px'});if(host.value)observer.observe(host.value);window.addEventListener('clipflow-business-changed',changed);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;abort?.abort();observer?.disconnect();window.removeEventListener('clipflow-business-changed',changed);document.removeEventListener('visibilitychange',visibility);});
</script>
<style>
.v2-metric-widget.is-calendar-widget{display:flex;flex-direction:column;gap:6px;height:100%;min-height:0;position:relative;overflow:hidden}
.is-calendar-widget>.v2-motion-chart{flex:1;min-height:0}
.is-calendar-widget>.v2-widget-source{flex:none;margin:0;padding:3px 0;font-size:11px}
.is-compact-calendar .v2-calendar-scale{display:none}
.v2-calendar-warning{position:absolute;right:0;top:0;z-index:2;min-height:28px!important}
</style>
