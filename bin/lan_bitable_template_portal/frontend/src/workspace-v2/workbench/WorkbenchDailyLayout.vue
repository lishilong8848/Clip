<script setup lang="ts">
import { computed, inject, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from 'vue';
import { NModal, NPopover } from 'naive-ui';
import { ArrowRight, Bell, CalendarDays, ChevronLeft, ChevronRight, ShieldCheck, UserRound, X } from 'lucide-vue-next';
import { todoTimestamp } from '../businessTodos';
import { waitWithSignal } from '../client';
import { readWorkbenchRequest } from './useWorkbenchReads';
import type { WorkItem } from './sources';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';
import WorkbenchAgendaList from './WorkbenchAgendaList.vue';
import WorkbenchRoster, { type Handover } from './WorkbenchRoster.vue';
import WorkbenchWeather from './WorkbenchWeather.vue';
import NavThemePicker from '../NavThemePicker.vue';
import './workbench.css';

type AgendaEntry = {key:string; item:WorkItem; type:'plan'|'change'|'due'; label:string; time:string; stamp:number};
export type CalendarItem={kind:string;title:string;scopes?:string[];notes?:string[];start?:string;end?:string;source_id?:string;source?:string};
export type SealStatus='ready'|'stale'|'failed'|'unconfigured';
type CalendarDay={items:CalendarItem[];staff:{scope:string;shift:string;names:string[]}[]};
type CalendarResponse={month:string;days:Record<string,CalendarDay>;unavailable:string[];roster_dates?:string[];
  handover?:Handover;roster_pending?:boolean;seal_status?:SealStatus;seal_windows?:CalendarItem[]};
type RosterSnapshot={key:string;state:'ready'|'failed';handover?:Handover};
type CalendarWeather={warning_ready:boolean;
  alerts:{id?:string;title:string;guard_level?:string}[];guard_level?:string;guard_actions:string[];guard_note?:string};
const props = defineProps<{name:string; avatar?:string; clock:number; tasks:WorkItem[]; unread:WorkItem[]; guest?:boolean;
  impersonating?:boolean;
  calendarScope?:string; calendarIdentity?:string; queueNote?:string; userId?:string}>();
const emit = defineEmits<{open:[url:string]; unread:[]; task:[item:WorkItem]; 'guest-account':[]}>();

const avatar=computed(()=>/^https:\/\//i.test(props.avatar||'')?props.avatar:undefined);
const avatarFailed=ref(false);
watch(avatar,()=>{avatarFailed.value=false;});
const surface = ref<HTMLElement>();
const mobileNavigation=inject('workspace-mobile-navigation',ref(false));
const noticeButton = ref<HTMLButtonElement>();
const noticesOpen = ref(false);
function dismiss() {
  if (noticesOpen.value) { noticeButton.value?.focus(); noticesOpen.value=false; }
}
function escapePanel(event:KeyboardEvent) {
  if(noticesOpen.value){event.stopPropagation();dismiss();}
}
function outside(event:PointerEvent) {
  if (noticesOpen.value && !(event.target instanceof Element && surface.value?.contains(event.target) && event.target.closest('.desk-anchor'))) noticesOpen.value=false;
}

const zone = 'Asia/Shanghai';
const dayKey = (stamp:number) => new Date(stamp+8*3600000).toISOString().slice(0,10);
const dayTime = (stamp:number) => new Intl.DateTimeFormat('zh-CN',{timeZone:zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(stamp);
const today = computed(()=>dayKey(props.clock));
const greeting=computed(()=>{
  const hour=new Date(props.clock).getHours();
  return hour<6?'夜里好':hour<11?'早上好':hour<14?'中午好':hour<18?'下午好':'晚上好';
});

const selected = ref(today.value), month = ref(today.value.slice(0,7));
const monthDate = computed(()=>new Date(month.value+'-01T00:00:00Z'));
watch(today,(_next,previous)=>{
  if(selected.value===previous) goToday();
});
function goToday(){ selected.value=today.value; month.value=today.value.slice(0,7); }
function moveMonth(delta:number) {
  const date=monthDate.value;
  month.value=new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth()+delta,1)).toISOString().slice(0,7);
  selected.value=month.value===today.value.slice(0,7)?today.value:month.value+'-01';
}
const monthCells = computed(()=>{
  const begin=monthDate.value, shift=(begin.getUTCDay()+6)%7;
  const length=new Date(Date.UTC(begin.getUTCFullYear(),begin.getUTCMonth()+1,0)).getUTCDate();
  return Array.from({length:Math.ceil((shift+length)/7)*7},(_,i)=>{
    const current=new Date(+begin+(i-shift)*86400000);
    const key=current.toISOString().slice(0,10);
    return {key,number:current.getUTCDate(),inMonth:key.startsWith(month.value)};
  });
});
const monthExpanded=ref(false), agendaExpanded=ref(false);
const agendaView=ref<'cards'|'table'>('table');
const weekday=computed(()=>new Intl.DateTimeFormat('zh-CN',{weekday:'short',timeZone:zone}).format(props.clock));
const visibleDates=computed(()=>{
  if(monthExpanded.value)return monthCells.value;
  const row=Math.floor(monthCells.value.findIndex(cell=>cell.key===selected.value)/7)*7;
  return monthCells.value.slice(row,row+7);
});
function moveCalendar(delta:number){
  if(monthExpanded.value){moveMonth(delta);return;}
  const target=Date.parse(selected.value+'T00:00:00Z')+delta*7*86400000;
  selectDay(new Date(target).toISOString().slice(0,10));
}
const byDay = computed(()=>{
  const days = new Map<string,AgendaEntry[]>();
  for (const task of props.tasks) {
    const start = task.kind === 'notices' ? task.noticeWindow?.start || (!task.ongoing ? task.time : '') : task.scheduledAt;
    const startAt = todoTimestamp(start);
    const dueAt = task.dueAt ?? todoTimestamp(task.noticeWindow?.end);
    const add = (stamp:number, type:AgendaEntry['type'], label:string, raw?:string) => {
      const day=dayKey(stamp);
      const time=raw && /^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/.test(raw.trim())?'全天':dayTime(stamp);
      const entry={key:task.key+':'+type,item:task,type,label,time,stamp};
      const entries=days.get(day);
      if(entries) entries.push(entry);
      else days.set(day,[entry]);
    };
    if (startAt) add(startAt,task.detail==='变更'?'change':'plan',task.detail ? task.detail+'计划' : '计划开始',typeof start==='string'?start:undefined);
    if (dueAt) add(dueAt,'due','截止',task.noticeWindow?.end);
  }
  for (const entries of days.values()) entries.sort((a,b)=>a.stamp-b.stamp);
  return days;
});
const mergedAgenda=computed(()=>{
  const days=new Map<string,(AgendaEntry&{due?:string})[]>();
  for(const [day,entries] of byDay.value){
    const tasks=new Map<string,AgendaEntry&{due?:string}>();
    for(const entry of entries){
      const other=tasks.get(entry.item.key);
      if(!other)tasks.set(entry.item.key,{...entry});
      else if(entry.type==='due')other.due=entry.time;
      else tasks.set(entry.item.key,{...entry,due:other.time});
    }
    days.set(day,[...tasks.values()].sort((a,b)=>a.stamp-b.stamp));
  }
  return days;
});
const calendarDays=ref<Record<string,CalendarDay>>({});
const calendarSourceKeys=computed(()=>new Map(Object.entries(calendarDays.value).map(([day,record])=>[
  day,new Set(record.items.filter(item=>item.source_id).map(item=>'change:'+item.source_id)),
])));
function separateTasks(day:string){
  const represented=calendarSourceKeys.value.get(day);
  return (mergedAgenda.value.get(day)||[]).filter(entry=>!entry.item.sourceKey||!represented?.has(entry.item.sourceKey));
}
const calendarNotice=ref(''), calendarBusy=ref(false);
const calendarRevision=ref(0), calendarFailed=ref(false);
let calendarReadAt=0;
const calendarDay=computed(()=>calendarDays.value[selected.value]);
const calendarScope=computed(()=>props.calendarScope||'');
const sealStatus=ref<SealStatus>();
const seals=ref<CalendarItem[]>([]);
const pendingAgendaTask=ref<WorkItem>();
watch([selected,calendarScope,()=>props.calendarIdentity],()=>{
  pendingAgendaTask.value=undefined;
  agendaExpanded.value=false;
});
const rosterKey=computed(()=>JSON.stringify([today.value,calendarScope.value,props.calendarIdentity]));
const rosterRevision=ref(0);
const roster=ref<RosterSnapshot>();
const visibleRoster=computed(()=>roster.value?.key===rosterKey.value?roster.value:undefined);
const rosterMessage=computed(()=>{
  if(!calendarScope.value)return '暂无排班';
  if(!visibleRoster.value)return '正在读取排班…';
  return visibleRoster.value.state==='failed'?(visibleRoster.value.handover?'排班待核验':'暂未读到排班'):'暂无排班';
});
const dayCount=(day:string)=>separateTasks(day).length+(calendarDays.value[day]?.items.length||0);
function scheduleTime(value?:string){
  if(value && /^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/.test(value.trim()))return '全天';
  const stamp=todoTimestamp(value);
  return stamp===undefined?'—':dayTime(stamp);
}
async function readCalendar(month:string,scope:string,signal:AbortSignal,rosterOnly=false):Promise<CalendarResponse>{
  const query=new URLSearchParams({month,scope});
  if(rosterOnly)query.set('view','roster');
  const data=await readWorkbenchRequest('/api/personal-calendar?'+query,signal);
  if(data.month!==month||!data.days||!Array.isArray(data.unavailable))throw new Error('日程未完整返回');
  return data as CalendarResponse;
}
watch([month,calendarScope,()=>props.calendarIdentity,calendarRevision],async([activeMonth,scope,identity],old,onCleanup)=>{
  if(!old || activeMonth!==old[0] || scope!==old[1] || identity!==old[2])calendarDays.value={};
  if(!old || scope!==old[1] || identity!==old[2]){sealStatus.value=undefined;seals.value=[];}
  calendarNotice.value='';
  calendarFailed.value=false;
  calendarBusy.value=false;
  if(props.guest||!scope)return;
  const controller=new AbortController();
  calendarBusy.value=true;
  onCleanup(()=>controller.abort());
  try {
    const data=await readCalendar(activeMonth,scope,controller.signal);
    if(controller.signal.aborted)return;
    const other=data.unavailable.filter(label=>label!=='封网');
    const hasPrevious=Object.keys(calendarDays.value).length>0;
    if(!other.length||!hasPrevious)calendarDays.value=data.days;
    sealStatus.value=data.seal_status ?? (data.unavailable.includes('封网')?'failed':'ready');
    seals.value=data.seal_windows||[];
    calendarFailed.value=other.length>0;
    calendarNotice.value=calendarFailed.value?other.join('、')+(hasPrevious?'待更新 · 显示上次核验结果':'尚未读全'):'';
  } catch (error) {
    if(!controller.signal.aborted){
      const denied=[401,403].includes((error as {status?:number})?.status||0);
      if(denied){calendarDays.value={};sealStatus.value=undefined;seals.value=[];}
      calendarNotice.value=Object.keys(calendarDays.value).length?'日程待更新 · 显示上次核验结果':'日程暂未读全';
      calendarFailed.value=true;
    }
  } finally {
    if(!controller.signal.aborted){calendarBusy.value=false;calendarReadAt=Date.now();}
  }
},{immediate:true});
let rosterReadAt=0;
const rosterReading=ref(false);
watch([rosterKey,rosterRevision],async([key],_old,onCleanup)=>{
  const scope=calendarScope.value, date=today.value;
  if(props.guest||!scope)return;
  const controller=new AbortController();
  rosterReading.value=true;
  onCleanup(()=>{controller.abort();rosterReading.value=false;});
  try {
    let data=await readCalendar(date.slice(0,7),scope,controller.signal,true);
    if(data.roster_pending){
      await waitWithSignal(1500,controller.signal);
      data=await readCalendar(date.slice(0,7),scope,controller.signal,true);
    }
    if(controller.signal.aborted)return;
    const handover=data.handover;
    for(const side of ['current','incoming'] as const){
      const incoming=handover?.[side], previous=visibleRoster.value?.handover?.[side];
      if(handover&&incoming&&!incoming.verified&&previous?.verified&&incoming.date===previous.date&&incoming.shift===previous.shift)
        handover[side]=previous;
    }
    if(handover && (handover.current?.verified||handover.incoming?.verified)) roster.value={key,state:'ready',handover};
    else if(visibleRoster.value?.state!=='ready')roster.value={key,state:'failed',handover};
  } catch(error){
    if(!controller.signal.aborted&&([401,403].includes((error as {status?:number})?.status||0)||visibleRoster.value?.state!=='ready'))
      roster.value={key,state:'failed'};
  } finally {
    if(!controller.signal.aborted){rosterReading.value=false;rosterReadAt=Date.now();}
  }
},{immediate:true});
watch(()=>props.clock,()=>{
  if(!props.guest&&!document.hidden&&calendarScope.value&&!calendarBusy.value&&
    (calendarFailed.value||sealStatus.value==='stale'||sealStatus.value==='failed')&&Date.now()-calendarReadAt>=30000)
    calendarRevision.value++;
  const shiftEnd=todoTimestamp(visibleRoster.value?.handover?.current?.end);
  if(!props.guest&&!document.hidden&&!rosterReading.value&&(Date.now()-rosterReadAt>(visibleRoster.value?.state==='ready'?120000:45000)
    ||shiftEnd!==undefined&&rosterReadAt<shiftEnd&&props.clock>=shiftEnd))
    rosterRevision.value++;
});
const calendarWeather=ref<CalendarWeather>();
const guardFailed=ref(false);
const calendarWeatherBusy=ref(false);
let calendarWeatherRequest:AbortController|undefined;
let calendarWeatherAt=0;
async function loadCalendarWeather(force=false){
  if(props.guest||calendarWeatherBusy.value||!force&&Date.now()-calendarWeatherAt<60000)return;
  calendarWeatherBusy.value=true;
  const request=calendarWeatherRequest=new AbortController();
  try{
    const data=await readWorkbenchRequest('/api/personal-calendar/weather',request.signal);
    if(!request.signal.aborted){
      if(!data.guard_read_failed||!calendarWeather.value)calendarWeather.value=data as CalendarWeather;
      guardFailed.value=data.guard_read_failed===true;
    }
  } catch(error) {
    if(!request.signal.aborted){
      if([401,403].includes((error as {status?:number})?.status||0))calendarWeather.value=undefined;
      guardFailed.value=true;
    }
  } finally {
    if(calendarWeatherRequest===request){calendarWeatherBusy.value=false;calendarWeatherAt=Date.now();}
  }
}
watch(()=>props.calendarIdentity,()=>{
  calendarWeatherRequest?.abort();
  calendarWeather.value=undefined;
  calendarWeatherAt=0;
  calendarWeatherBusy.value=false;
  guardFailed.value=false;
  void loadCalendarWeather();
});
watch(()=>props.clock,()=>{if(!document.hidden)void loadCalendarWeather();});
const noticesByDay = computed(()=>{
  const days = new Map<string, {item:WorkItem; times:string[]}[]>();
  for(const [day,entries] of byDay.value){
    const notices = new Map<string, {item:WorkItem; times:string[]}>();
    for(const {item,type,time} of entries){
      if(item.kind!=='notices'&&item.kind!=='event_notices') continue;
      let notice=notices.get(item.key);
      if(!notice){notice={item,times:[]};notices.set(item.key,notice);}
      notice.times.push(type==='due'?'截止 '+time:time);
    }
    if(notices.size) days.set(day,[...notices.values()]);
  }
  return days;
});

const selectedWeekday = computed(()=>new Intl.DateTimeFormat('zh-CN',{timeZone:zone,weekday:'long'}).format(new Date(selected.value+'T12:00:00+08:00')));
onMounted(()=>{document.addEventListener('pointerdown',outside);if(!props.guest)void loadCalendarWeather();});
onDeactivated(()=>{noticesOpen.value=false;pendingAgendaTask.value=undefined;agendaExpanded.value=false;});
onBeforeUnmount(()=>{document.removeEventListener('pointerdown',outside);calendarWeatherRequest?.abort();});


const agendaPanel=ref<HTMLElement>();
function showAgenda(){
  agendaPanel.value?.scrollIntoView({block:'start'});
  agendaPanel.value?.focus({preventScroll:true});
}
function selectDay(key:string){
  selected.value=key;
  month.value=key.slice(0,7);
}
function showUnreadQueue(){
  noticesOpen.value=false;
  emit('unread');
}
function openNotification(item:WorkItem){
  dismiss();
  emit('task',item);
}
const itinerary=computed(()=>{
  const records=(calendarDay.value?.items||[]).map((record,index)=>{
    const task=record.source_id?props.tasks.find(row=>row.sourceKey==='change:'+record.source_id):undefined;
    const related=task && mergedAgenda.value.get(selected.value)?.find(entry=>entry.item.key===task.key);
    const due=related?.type==='due'?related.time:related?.due;
    return {key:'schedule:'+index,stamp:todoTimestamp(record.start),time:scheduleTime(record.start),label:record.kind,title:record.title,
      meta:[record.source,...(record.scopes||[]),...(record.notes||[]),due?'截止 '+due:''].filter(Boolean).join(' · '),task};
  });
  const entries=separateTasks(selected.value).map(entry=>({key:entry.key,stamp:entry.stamp,time:entry.time,label:entry.label,title:entry.item.title,
    meta:[entry.item.scope,entry.item.status,entry.due?'截止 '+entry.due:''].filter(Boolean).join(' · '),task:entry.item}));
  return [...records,...entries].sort((a,b)=>(a.stamp??Infinity)-(b.stamp??Infinity));
});
function openAgendaTask(item:WorkItem) {
  pendingAgendaTask.value=item;
  agendaExpanded.value=false;
}
function afterAgendaClose() {
  const item=pendingAgendaTask.value;
  pendingAgendaTask.value=undefined;
  if(item)emit('task',item);
}
</script>

<template>
  <div ref="surface" class="daily-desk" @keydown.esc="escapePanel">
    <header class="desk-head masthead">
      <div class="desk-title">
        <span v-if="guest" class="desk-avatar" aria-hidden="true">
          <img v-if="avatar && !avatarFailed" :src="avatar" alt="" @error="avatarFailed=true">
          <UserRound v-else :size="20" />
        </span>
        <div class="mast-identity">
          <div v-if="!guest" class="title-line"><h1 data-v2-page-title>个人工作台</h1>
            <time class="folio-date" :datetime="today">{{ Number(today.slice(5,7)) }}月{{ Number(today.slice(-2)) }}日 · {{ weekday }}</time>
          </div>
          <div class="identity-line">
            <button v-if="guest" class="name-control" @click="emit('guest-account')">{{ name || '访客账号' }}</button>
            <span v-else>{{ greeting }}{{ name?'，'+name:'' }}</span><slot name="context" />
            <WorkbenchWeather v-if="mobileNavigation&&!guest&&!impersonating" :clock="clock" :identity="calendarIdentity"/>
          </div>
        </div>
      </div>
      <div class="mast-actions">
      <div v-if="!guest && $slots.focus" class="command-line"><slot name="focus" /></div>
      <div v-if="!guest" class="desk-tools">
        <button class="small-key agenda-jump" aria-label="跳到日历" title="日历" @click="showAgenda"><CalendarDays :size="16" aria-hidden="true" /></button>
        <template v-if="!guest">
          <div class="desk-anchor">
            <button ref="noticeButton" class="tool-key" :aria-label="'消息通知，'+(!unread.length && queueNote?queueNote:unread.length+' 项未读')" :aria-expanded="noticesOpen" @click="noticesOpen=!noticesOpen">
              <Bell :size="18" aria-hidden="true" /><span v-if="unread.length" class="notice-count" aria-hidden="true">{{ unread.length>99?'99+':unread.length }}</span>
            </button>
            <section v-if="noticesOpen" class="floating notification" aria-label="未读消息">
              <div class="line-head">
                <h3>未读</h3><div class="notification-actions">
                  <button v-if="unread.length" class="icon-key" aria-label="打开未读清单" title="打开未读清单" @click="showUnreadQueue"><ArrowRight :size="16" aria-hidden="true" /></button>
                  <button class="icon-key" aria-label="关闭消息" @click="dismiss"><X :size="15" aria-hidden="true" /></button>
                </div>
              </div>
              <button v-for="item in unread.slice(0,5)" :key="item.key" class="notice-row" @click="openNotification(item)"><strong>{{ item.title }}</strong><span v-if="item.scope || item.status">{{ [item.scope,item.status].filter(Boolean).join(' · ') }}</span></button>
              <p v-if="!unread.length">{{ queueNote || '暂无未读' }}</p>
            </section>
          </div>
          <NavThemePicker v-if="mobileNavigation" :user-id="userId||''" palette-only />
        </template>
      </div>
      </div>
    </header>

    <div :class="{'working-grid':!guest}">
      <div class="desk-content"><slot /></div>
      <aside v-if="!guest" ref="agendaPanel" class="day-planner" tabindex="-1" aria-label="我的日程">
        <div class="planner-module rail-module">
        <header class="planner-head">
          <div class="month-tools"><button class="month-mode" :aria-expanded="monthExpanded" :aria-label="monthExpanded?'收起月历':'展开月历'" @click="monthExpanded=!monthExpanded">{{ month.replace('-', '.') }} <span>{{ monthExpanded?'月历 ▴':'周历 ▾' }}</span></button><div>
            <button class="icon-key" :aria-label="monthExpanded?'上个月':'上一周'" @click="moveCalendar(-1)"><ChevronLeft :size="16" /></button><button class="today-key" @click="goToday">今天</button><button class="icon-key" :aria-label="monthExpanded?'下个月':'下一周'" @click="moveCalendar(1)"><ChevronRight :size="16" /></button>
          </div></div>
          <div class="month-grid">
            <span v-for="weekday in ['一','二','三','四','五','六','日']" :key="weekday" class="weekday">{{ weekday }}</span>
            <NPopover v-for="cell in visibleDates" :key="cell.key" :disabled="!noticesByDay.get(cell.key)?.length" trigger="hover" placement="top" :duration="100">
              <template #trigger><button class="day-cell" :class="{adjacent:!cell.inMonth,current:cell.key===today}" :aria-pressed="cell.key===selected"
                :aria-current="cell.key===today?'date':undefined" :aria-label="cell.key+'，已载入 '+dayCount(cell.key)+' 项日程'" @click="selectDay(cell.key)">
                {{ cell.number }}<i v-if="dayCount(cell.key)" aria-hidden="true" />
              </button></template>
              <section class="day-preview" :aria-label="cell.key+' 通告预览'"><strong>{{ cell.key }} 通告预览</strong>
                <p v-for="entry in noticesByDay.get(cell.key)" :key="entry.item.key"><b>{{ entry.item.title }}</b><small>{{ entry.item.scope }} · {{ entry.times.join(' / ') }}</small></p>
              </section>
            </NPopover>
          </div>
        </header>
        <section class="itinerary" aria-label="所选日期日程">
          <div class="line-head"><h3>{{ selected.slice(5).replace('-',' / ') }} <small>{{ selectedWeekday }}</small></h3>
            <RecordViewSwitch v-if="itinerary.length" v-model="agendaView"/>
          </div>
          <WorkbenchAgendaList v-if="itinerary.length" :entries="itinerary.slice(0,2)" :view="agendaView" @task="emit('task',$event)" />
          <p v-else-if="!calendarNotice" class="muted">{{ calendarBusy?'读取中…':calendarFailed?'日程读取失败':!calendarScope?'暂无授权日程':queueNote?'暂无已读取日程':'当日无安排' }}</p>
          <button v-if="itinerary.length>2" class="text-key" aria-haspopup="dialog" @click="agendaExpanded=true">展开全部 {{ itinerary.length }} 项日程</button>
          <NModal v-model:show="agendaExpanded" preset="card" class="wb-agenda-dialog" style="width:min(92vw,560px)" :title="selected+' 日程'"
            aria-label="当天全部日程" transform-origin="center" @after-leave="afterAgendaClose">
            <div class="agenda-scroll"><WorkbenchAgendaList :entries="itinerary" :view="agendaView" @task="openAgendaTask" /></div>
          </NModal>
          <div v-if="calendarNotice" class="retry-note" role="status">{{ calendarNotice }} <button class="text-key" @click="calendarRevision++">重试日程</button></div>
          <div v-if="sealStatus==='unconfigured'" class="retry-note muted" role="status">封网表未接入</div>
          <div v-else-if="sealStatus==='stale'" class="retry-note" role="status">封网待更新 · 显示最近核验结果 <button class="text-key" @click="calendarRevision++">重试封网</button></div>
          <div v-else-if="sealStatus==='failed'" class="retry-note" role="status">封网读取失败 <button class="text-key" @click="calendarRevision++">重试封网</button></div>
          <p v-else-if="sealStatus==='ready'&&!calendarDay?.items.some(item=>item.kind==='封网')" class="retry-note muted">当日无封网</p>
        </section>
        </div>
        <section class="weather-guard rail-module" aria-label="天气预警与戒备" data-v2-inline-feedback>
          <div class="line-head"><h3><ShieldCheck :size="16" /> 今日预警与戒备</h3><button class="text-key" :disabled="calendarWeatherBusy" @click="loadCalendarWeather(true)">刷新</button></div>
          <div class="guard-summary" role="status">
            <span v-if="!calendarWeather && calendarWeatherBusy">读取中…</span>
            <span v-else-if="!calendarWeather?.warning_ready && !calendarWeather?.guard_level && !guardFailed">待更新</span>
            <template v-else>
              <span v-if="!calendarWeather?.warning_ready">预警待更新</span>
              <span v-else-if="!calendarWeather.alerts?.length">无天气预警</span>
              <b v-if="calendarWeather?.guard_level">戒备等级 {{ calendarWeather.guard_level }}</b>
              <span v-else-if="!guardFailed">{{ calendarWeather?.warning_ready?'当前无戒备':'戒备待更新' }}</span>
              <span v-if="guardFailed">{{ calendarWeather?.warning_ready?'显示上次识别结果':'戒备暂未读到' }}</span>
            </template>
          </div>
          <div v-for="alert in calendarWeather?.alerts" :key="alert.id || alert.title" class="warning"><b>{{ alert.title }}</b><span v-if="alert.guard_level">{{ alert.guard_level }}</span></div>
          <ul v-if="calendarWeather?.guard_actions?.length"><li v-for="action in calendarWeather.guard_actions" :key="action">{{ action }}</li></ul><p v-if="calendarWeather?.guard_note">{{ calendarWeather.guard_note }}</p>
        </section>
        <WorkbenchRoster class="rail-module roster-module" :handover="visibleRoster?.handover" :message="rosterMessage"
          :can-retry="visibleRoster?.state==='failed'&&!rosterReading" :impersonating="impersonating" @retry="rosterRevision++" />
      </aside>
      <div v-if="$slots.overview" class="desk-supplement">
        <slot name="overview" :seals="seals" :seal-status="sealStatus" />
      </div>
      <div v-if="!guest&&$slots.shortcuts" class="desk-shortcuts">
        <slot v-if="!guest" name="shortcuts" />
      </div>
    </div>
  </div>
</template>

<style scoped>
.daily-desk {
  --desk-paper:var(--soft-face,#f4f7f7); --desk-well:var(--soft-field,#edf2f3);
  --desk-ink:var(--v2-ink,#20373b); --desk-muted:var(--v2-muted); --desk-line:var(--soft-edge);
  --desk-green:var(--v2-palette-accent-text); --desk-green-wash:var(--v2-palette-selection);
  --desk-blue:#266288; --desk-blue-wash:#dcedf8; --desk-shadow:var(--soft-key);
  display:grid; gap:14px; width:100%; min-width:0; container:daily-desk / inline-size; color:var(--desk-ink); font:inherit;
}
:global([data-v2-theme="dark"] .daily-desk) {
  --desk-blue:#aad5ee; --desk-blue-wash:#243e4f;
}
.daily-desk *, .daily-desk *::after { box-sizing:border-box }
.daily-desk :is(h1,h2,h3,p,ul,ol) { margin:0 }
.daily-desk h1 { font-size:clamp(20px,2vw,27px); line-height:1.2; letter-spacing:-.035em }
.daily-desk h2 { font-size:18px }
.daily-desk h3 { font-size:14px }
.daily-desk :is(button,select,input) { font:inherit }
.daily-desk :where(button:not(.n-button),select,input) { color:inherit }
.daily-desk button { cursor:pointer }
.daily-desk button:disabled { cursor:wait; opacity:.65 }
.daily-desk :is(button,select,input):focus-visible,.day-planner:focus-visible { outline:2px solid var(--v2-blue,#24799f); outline-offset:2px }
.desk-head,.desk-title,.desk-tools,.identity-line,.line-head,.month-tools,.month-tools>div,.notification-actions,.weather-guard h3 { display:flex; align-items:center; gap:8px }
.desk-head,.line-head,.month-tools { justify-content:space-between }
.desk-head { flex-wrap:wrap; gap:10px 16px }
.desk-title { gap:12px; min-width:0 }
.desk-avatar { width:43px; height:43px; border-radius:15px; display:grid; place-items:center; color:var(--desk-blue); background:linear-gradient(135deg,var(--desk-blue-wash),var(--desk-green-wash)); box-shadow:var(--desk-shadow); overflow:hidden }
.desk-avatar img { width:100%; height:100%; object-fit:cover }
.identity-line { font-size:12px; color:var(--desk-muted); flex-wrap:wrap; gap:3px 8px; margin-top:3px; overflow-wrap:anywhere }
.name-control,.text-key { border:0; padding:0; background:transparent; color:var(--desk-green); font-weight:650 }
.desk-tools { position:relative; flex-wrap:nowrap; max-width:100% }
.desk-anchor { position:relative; min-width:0 }
.tool-key,.small-key,.day-planner select,.today-key { background:var(--desk-paper); border:1px solid var(--desk-line); border-radius:10px }
.tool-key,.small-key,.text-key,.icon-key { display:inline-flex; align-items:center; justify-content:center; gap:5px }
.tool-key,.small-key { min-height:40px; padding:5px 10px; font-size:13px; position:relative }
.icon-key { background:transparent; border:1px solid transparent; width:32px; height:32px; border-radius:8px }
.notice-count { position:absolute; right:0; top:0; min-width:16px; padding:1px 3px; border-radius:999px; background:var(--desk-blue); color:var(--desk-paper); font-size:10px; line-height:1.35; font-weight:700; text-align:center }
.notification-actions { gap:3px }
.floating { position:absolute; z-index:25; top:calc(100% + 6px); right:0; width:min(310px,85vw); padding:14px; border:1px solid var(--desk-line); border-radius:14px; background:var(--desk-paper); box-shadow:0 12px 30px #122e3a2b; overflow:auto; max-height:60vh; font-size:13px }
.notification { display:grid; gap:4px }
.notice-row { background:transparent; border:0; border-radius:8px; padding:9px; text-align:left }
.notice-row:hover,.icon-key:hover { background:var(--desk-blue-wash)!important }
.notice-row { display:grid; gap:2px }
.notice-row span,.muted,.itinerary h3 small { color:var(--desk-muted); font-size:12px; font-weight:normal }
.desk-head.masthead { display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:8px 16px;min-width:0;padding:4px 0 }
.mast-actions:empty { display:none }
.mast-actions { display:flex;align-items:center;justify-content:flex-end;flex-wrap:wrap;gap:5px;min-width:0;margin-inline-start:auto }
.title-line { display:flex;align-items:center;gap:4px 9px;flex-wrap:wrap }
.title-line h1 { font-size:21px;letter-spacing:-.03em;line-height:1.25 }
.folio-date { color:var(--desk-muted);white-space:nowrap;font-size:12px;font-variant-numeric:tabular-nums }
.masthead .tool-key,.masthead .small-key { width:36px;min-height:36px;padding:0;border-radius:9px;white-space:nowrap;color:var(--desk-ink) }
.masthead :is(.tool-key,.small-key):hover,.masthead :is(.tool-key,.small-key)[aria-expanded=true] { background:var(--desk-green-wash);border-color:var(--desk-green);color:var(--desk-green) }
.masthead .desk-tools { gap:5px; margin-inline-start:auto }
.command-line { display:flex;align-items:center;flex-wrap:wrap;min-width:0;max-width:100% }
.command-line :slotted(*) { max-width:100%;flex-wrap:wrap }
.month-mode { display:inline-flex;align-items:center;gap:4px;border:0;background:transparent;padding:4px 0;color:var(--desk-ink);font-weight:650!important }
.month-mode span { color:var(--desk-blue);font-size:11px }
.working-grid { display:grid;align-items:start;gap:12px;min-width:0;grid-template-columns:minmax(0,1fr);grid-template-areas:"tasks" "agenda" "supplement" "shortcuts" }
.desk-content { grid-area:tasks;min-width:0 }
.desk-supplement { grid-area:supplement;display:grid;gap:10px;min-width:0 }
.desk-shortcuts { grid-area:shortcuts;min-width:0 }
.day-planner { grid-area:agenda;display:grid;align-items:start;gap:9px;min-width:0;scroll-margin-top:72px }
.rail-module { min-width:0;padding:11px 13px;border:1px solid var(--desk-line);border-radius:15px;background:var(--desk-paper);box-shadow:var(--desk-shadow) }
.day-planner .roster-module { padding:0 }
.planner-module { background:linear-gradient(155deg,color-mix(in srgb,var(--desk-blue-wash) 55%,var(--desk-paper)),var(--desk-paper) 58%) }
.weather-guard h3 { font-size:14px;font-weight:680 }
.itinerary { margin-top:8px;padding-top:8px;border-top:1px solid var(--desk-line) }
.guard-summary { display:flex;align-items:center;flex-wrap:wrap;gap:3px 7px;color:var(--desk-muted) }
.guard-summary b { color:var(--desk-green) }
.month-tools select { min-width:0; max-width:102px; padding:3px 5px; font-size:12px }
.month-tools { flex-wrap:wrap; gap:3px 5px; font-size:13px }
.month-tools>div { gap:2px; margin-inline-start:auto }
.month-tools .icon-key { width:26px }
.today-key { min-height:30px; padding:3px 8px; font-size:12px;border-radius:7px }
.month-grid { display:grid; grid-template-columns:repeat(7,minmax(0,1fr)); gap:2px; margin-top:5px }
.weekday { text-align:center; color:var(--desk-muted); font-size:11px; padding:3px }
.day-cell { position:relative; min-width:0; min-height:31px; border:1px solid transparent; border-radius:8px; background:transparent; font-size:13px!important; font-variant-numeric:tabular-nums }
.day-cell.adjacent { color:var(--desk-muted); font-weight:normal }
.day-cell.current { border-color:var(--desk-green) }
.day-cell[aria-pressed="true"] { background:var(--v2-palette-accent); color:var(--v2-palette-on-accent); font-weight:700 }
.day-cell i { display:block; position:absolute; bottom:2px; left:calc(50% - 2px); border-radius:50%; background:currentColor; width:4px; height:4px }
.day-preview { max-width:min(280px,80vw); max-height:230px; overflow:auto; font-size:13px }
.day-preview p { display:grid; margin:6px 0; gap:2px }
.itinerary .muted { margin-top:9px }
:global(.wb-agenda-dialog) { --desk-blue:var(--v2-palette-accent-text,var(--v2-blue)); --desk-green:var(--desk-blue); --desk-muted:var(--v2-muted); --desk-line:var(--v2-border) }
.agenda-scroll { max-height:min(58dvh,480px);overflow:auto;overscroll-behavior:contain }
.retry-note,.weather-guard { font-size:12px }
.retry-note,.weather-guard>*+* { margin-top:6px }
.weather-guard ul { padding-left:18px }
.warning { display:flex; justify-content:space-between; padding:6px 9px; background:var(--desk-blue-wash); border-radius:8px }
.agenda-jump { display:inline-flex }
@container daily-desk (min-width:980px) {
  .working-grid { grid-template-columns:minmax(0,1fr) minmax(310px,25%);grid-template-areas:"tasks agenda" "supplement agenda" "shortcuts shortcuts";grid-template-rows:auto 1fr auto }
  .desk-supplement,.day-planner { align-self:stretch }
  .desk-supplement :deep(.wb-period) { display:flex;flex-direction:column }
  .day-planner { grid-template-rows:auto auto auto;align-content:start }
  .agenda-jump { display:none }
}
@container daily-desk (min-width:700px) and (max-width:979px){
  .day-planner { grid-template-columns:repeat(2,minmax(0,1fr)) }
  .planner-module { grid-row:span 2 }
}
@media(max-width:700px) {
  .daily-desk { gap:9px }
  .desk-head.masthead { grid-template-columns:minmax(0,1fr) auto;gap:6px 10px;padding:0;position:relative }
  .masthead .desk-title { grid-column:1/-1 }
  .masthead:has(.name-control) .desk-title { grid-column:auto }
  .masthead .desk-tools { flex:1 1 auto;flex-wrap:wrap;margin-inline-start:0 }
  .day-cell,.today-key,.masthead .small-key,.masthead .tool-key { min-height:40px }
  .masthead .small-key,.masthead .tool-key{width:40px}
}
@media(max-width:540px) {
  .desk-title { width:100% }
  .mast-actions { margin-inline-start:0;flex-wrap:nowrap }
  .mast-identity { display:flex;align-items:center;flex-wrap:wrap;gap:2px 10px }
  .identity-line { margin-top:0 }
  .masthead .desk-tools { flex-wrap:nowrap }
  .desk-anchor { position:static }
  .masthead .floating { left:0;width:100%;box-sizing:border-box;top:100% }
}
.month-mode,.today-key { white-space:nowrap }
</style>
