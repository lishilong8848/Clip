<!-- 独立新版页面代码：灯塔个人工作台。业务数据与办理权限由现有工作台提供。 -->
<template>
  <div ref="surface" class="daily-desk" @keydown.esc="dismiss">
    <header class="desk-bar" :class="{'desk-guest':guest}">
      <div v-if="!guest" class="desk-greeting">
        <span class="desk-date">{{ dateLabel }}</span>
        <h1>{{ greeting }}，{{ name || '当前账号' }}</h1>
      </div>
      <div class="desk-actions">
        <div v-if="!guest" class="desk-anchor">
          <button ref="weatherButton" type="button" class="desk-control" aria-label="天气详情"
            :aria-expanded="panel==='weather'" @click="toggle('weather')">
            <CloudSun :size="19" aria-hidden="true" /> <span v-if="weather?.temp!=null">{{ weather.temp }}°</span>
          </button>
          <section v-if="panel==='weather'" class="desk-popover" aria-label="南通天气预报">
            <h2>南通天气</h2>
            <p v-if="!weather" role="status">{{ weatherBusy ? '正在获取天气…' : '天气暂不可用' }}</p>
            <template v-else>
              <p>{{ weather.temp==null?'实时气温暂缺':weather.temp+'°C' }} · {{ weather.description }} · {{ weather.humidity==null?'湿度暂缺':'湿度 '+weather.humidity+'%' }}</p>
              <ul class="desk-pop-list">
                <li v-for="day in weather.days" :key="day.date"><span>{{ day.date.slice(5) }} · {{ day.description }}</span><strong>{{ day.minimum ?? '—' }}～{{ day.maximum ?? '—' }}°C</strong></li>
              </ul>
            </template>
          </section>
        </div>
        <form class="desk-anchor desk-search" role="search" @submit.prevent="openSearch(matches[searchIndex])">
          <Search :size="17" aria-hidden="true" />
          <input ref="searchInput" v-model.trim="query" type="search" aria-label="全局搜索"
            :aria-expanded="panel==='search'" aria-controls="desk-search-results" placeholder="搜索系统功能"
            @focus="panel='search'" @input="panel='search'; searchIndex=0"
            @keydown.down.prevent="moveSearch(1)" @keydown.up.prevent="moveSearch(-1)" />
          <section v-if="panel==='search'" id="desk-search-results" class="desk-popover" aria-label="搜索结果">
            <p v-if="!matches.length" role="status">未找到可访问的功能</p>
            <ul v-else class="desk-pop-list">
              <li v-for="(entry,index) in matches" :key="entry.url">
                <button type="button" :class="{highlight:index===searchIndex}" data-v2-navigation @click="openSearch(entry)">
                  <span>{{ entry.label }}</span><ChevronRight :size="15" aria-hidden="true" />
                </button>
              </li>
            </ul>
          </section>
        </form>
        <div v-if="!guest" class="desk-anchor">
          <button ref="noticeButton" type="button" class="desk-control desk-bell" :aria-label="'消息通知，'+unread.length+' 条未读'"
            :aria-expanded="panel==='notices'" @click="toggle('notices')">
            <Bell :size="19" aria-hidden="true" /><span v-if="unread.length" class="desk-badge" aria-hidden="true">{{ Math.min(unread.length,99) }}</span>
          </button>
          <section v-if="panel==='notices'" class="desk-popover desk-notifications" aria-label="新任务通知">
            <h2>新任务通知</h2>
            <p v-if="!unread.length" role="status">暂无未读任务</p>
            <ul v-else class="desk-pop-list">
              <li v-for="item in unread.slice(0,8)" :key="item.key">
                <button type="button" @click="panel=''; emit('task',item)"><span>{{ item.title }}</span><ChevronRight :size="15" aria-hidden="true" /></button>
              </li>
            </ul>
            <button v-if="unread.length" type="button" class="wb-link" @click="emit('unread'); panel=''">查看全部未读</button>
          </section>
        </div>
        <button type="button" class="desk-avatar" :title="name || '个人信息'" :aria-label="guest?'访客账号':'个人信息'" data-v2-navigation @click="guest ? emit('guest-account') : emit('open','/?view=my')">
          <img v-if="avatar && !avatarFailed" :src="avatar" alt="" referrerpolicy="no-referrer" @error="avatarFailed=true" />
          <span v-else>{{ name?.slice(0,1) || '我' }}</span>
        </button>
      </div>
    </header>

    <div v-if="!guest" class="desk-indicators" aria-label="工作台指标">
      <article v-for="(metric,index) in metrics" :key="metric.label" class="desk-metric">
        <component :is="indicatorIcons[index]" :size="19" aria-hidden="true" />
        <h2>{{ metric.label }}</h2>
        <strong>{{ metric.value ?? '—' }}<small v-if="metric.value!==null && metric.partial"> 已读取</small></strong>
        <button v-if="metric.url" type="button" class="desk-metric-link wb-link" data-v2-navigation @click="emit('open',metric.url)">打开审核入口 <ChevronRight :size="14" aria-hidden="true" /></button>
        <span v-else-if="metric.value===null" class="desk-metric-note">{{ metric.note || '暂无可核验数据' }}</span>
      </article>
    </div>
    <div v-if="!guest" class="desk-columns">
      <div class="desk-main"><slot /></div>
      <aside class="wb-panel desk-agenda" aria-labelledby="desk-agenda-title">
        <header class="desk-agenda-title"><CalendarDays :size="20" aria-hidden="true" /><h2 id="desk-agenda-title">我的日程</h2>
        </header>
        <label v-if="agendaScopes?.length && agendaScopes.length>1" class="desk-agenda-scope">
          <span>日程楼栋</span><select v-model="agendaScope"><option value="">所有已授权楼栋</option>
          <option v-for="scope in agendaScopes" :key="scope.value" :value="scope.value">{{ scope.label }}</option></select>
        </label>
        <div class="desk-weather-panel">
          <div class="desk-weather-strip" aria-label="选中日期天气">
            <div class="desk-weather-date"><b>{{ selected.slice(-2) }}</b><span>{{ selectedWeekday }}</span></div>
            <component v-if="selectedWeather" :is="weatherIcon" class="desk-weather-icon" :size="33" aria-hidden="true" />
            <div class="desk-weather-reading">
              <b v-if="selectedWeather?.temperature">{{ selectedWeather.temperature }}</b>
              <small>{{ selectedWeather?.description || (weatherBusy ? '正在读取天气…' : '暂无天气预报') }}</small>
            </div>
          </div>
          <section class="desk-hourly" aria-labelledby="desk-hourly-title">
            <h3 id="desk-hourly-title">未来 12 小时气温</h3>
            <WorkbenchWeatherTrend v-if="temperaturePoints.length" :points="temperaturePoints" />
            <p v-else class="wb-note" role="status">{{ calendarWeatherBusy ? '正在读取气温趋势…' : calendarWeather?.hourly_note || '气温趋势暂不可用' }}</p>
          </section>
        </div>
        <div class="desk-month">
          <strong>{{ monthDate.getUTCFullYear() }} 年 {{ monthDate.getUTCMonth()+1 }} 月</strong>
          <div>
            <button type="button" class="desk-small" aria-label="上个月" @click="moveMonth(-1)"><ChevronLeft :size="17" /></button>
            <button type="button" class="desk-small" @click="goToday">今天</button>
            <button type="button" class="desk-small" aria-label="下个月" @click="moveMonth(1)"><ChevronRight :size="17" /></button>
          </div>
        </div>
        <div class="desk-calendar">
          <span v-for="day in '一二三四五六日'" :key="day" class="desk-weekday">{{ day }}</span>
          <template v-for="day in monthCells" :key="day.key">
            <NPopover v-if="day.inMonth" trigger="hover" placement="top" scrollable
              :disabled="!noticesByDay.has(day.key)&&!calendarDays[day.key]?.items.length" :delay="180">
              <template #trigger>
                <button type="button" class="desk-day" :class="{scheduled:dayCount(day.key)>0, conflict:calendarDays[day.key]?.items.some(item=>item.kind==='冲突')}"
                  :aria-label="day.key+(dayCount(day.key)?'，'+dayCount(day.key)+' 项日程':'')"
                  :aria-current="day.key===today?'date':undefined" :aria-pressed="day.key===selected" @click="selected=day.key">
                  {{ day.number }} <i v-if="dayCount(day.key)" aria-hidden="true"></i>
                </button>
              </template>
              <section class="desk-day-preview" :aria-label="day.key+' 通告预览'">
                <strong>{{ day.key.slice(5).replace('-',' 月 ') }} 日日程</strong>
                <ul>
                  <li v-for="{item,times} in noticesByDay.get(day.key)" :key="item.key">
                    <small>{{ item.detail || '通告' }} · {{ item.scope.replace(/^([A-EH])$/,'$1楼') }} · {{ times.join(' · ') }}</small>
                    <span>{{ item.title }}</span>
                  </li>
                  <li v-for="(item,index) in calendarDays[day.key]?.items" :key="'calendar-'+index">
                    <small>{{ item.kind }}<template v-if="item.scopes?.length"> · {{ item.scopes.join('、') }}</template></small><span>{{ item.title }}</span>
                  </li>
                </ul>
              </section>
            </NPopover>
            <span v-else class="desk-day desk-other" aria-hidden="true">{{ day.number }}</span>
          </template>
        </div>
        <p v-if="calendarNotice" class="desk-calendar-note" role="status">{{ calendarNotice }}</p>
        <div class="desk-schedule-head"><h3>{{ selected.slice(5).replace('-',' 月 ') }} 日安排</h3><span>{{ dayCount(selected) }} 项</span></div>
        <ul v-if="dayCount(selected)" class="desk-schedule">
          <li v-for="entry in agenda" :key="entry.key">
            <span class="desk-event-time">{{ entry.time }}</span>
            <span class="desk-event-dot" :class="entry.type" aria-hidden="true"></span>
            <div><small>{{ entry.label }}<template v-if="agendaScopes?.length && !agendaScope"> · {{ entry.item.scope.replace(/^([A-EH])$/,'$1楼') }}</template></small>
              <button v-if="entry.item.url && !entry.item.readOnly" class="desk-agenda-link" type="button" data-v2-navigation @click="emit('task',entry.item)">{{ entry.item.title }}</button>
              <strong v-else>{{ entry.item.title }}</strong>
            </div>
          </li>
          <li v-for="(item,index) in calendarDay?.items" :key="'calendar-'+index">
            <span class="desk-event-time">{{ scheduleTime(item.start) }}</span>
            <span :class="['desk-event-dot',item.kind==='冲突'?'conflict':item.kind==='封网'?'due':'change']" aria-hidden="true"></span>
            <div><small>{{ item.kind }}<template v-if="item.scopes?.length"> · {{ item.scopes.join('、') }}</template></small>
              <strong>{{ item.title }}</strong><small v-if="item.notes?.length">{{ item.notes.join('；') }}</small></div>
          </li>
        </ul>
        <p v-else class="wb-note">此日暂无已读取的日程</p>
        <section class="desk-schedule-extra" aria-labelledby="desk-staff-title">
          <h3 id="desk-staff-title">排班人员 <span v-if="calendarDay?.staff.length">{{ calendarDay.staff.length }} 个班次</span></h3>
          <dl v-if="calendarDay?.staff.length" class="desk-staff">
            <template v-for="(row,index) in calendarDay.staff" :key="index">
              <dt>{{ row.scope === '110' ? '110站' : row.scope+'楼' }} · {{ row.shift || '班次待核对' }}</dt><dd>{{ row.names.join('、') || '人员待核对' }}</dd>
            </template>
          </dl>
        <p v-else class="wb-note">{{ rosterDates.includes(selected) ? '该日暂无本楼排班人员' : '该日暂无排班快照' }}</p>
        </section>
        <section class="desk-schedule-extra" aria-labelledby="desk-alerts-title">
          <h3 id="desk-alerts-title">预警与戒备</h3>
          <p v-if="calendarWeatherBusy && !calendarWeather" class="wb-note" role="status">正在读取预警和戒备状态…</p>
          <template v-else>
            <div v-if="calendarWeather?.warning_ready" class="desk-alerts">
              <strong v-if="calendarWeather.guard_level">戒备等级 {{ calendarWeather.guard_level }}</strong>
              <ul v-if="calendarWeather.alerts.length">
                <li v-for="(alert,index) in calendarWeather.alerts" :key="alert.id||index">
                  {{ alert.title }}<small v-if="alert.guard_level && alert.guard_level!==calendarWeather.guard_level"> · {{ alert.guard_level }}</small>
                </li>
              </ul>
              <p v-else-if="!calendarWeather.guard_level && !calendarWeather.guard_note" class="wb-note">当前暂无戒备预警</p>
              <ul v-if="calendarWeather.guard_actions.length"><li v-for="action in calendarWeather.guard_actions" :key="action">{{ action }}</li></ul>
              <p v-if="calendarWeather.guard_note" class="wb-note">{{ calendarWeather.guard_note }}</p>
            </div>
            <p v-else class="wb-note">{{ calendarWeather?.guard_note || '预警及戒备状态待核对' }}</p>
          </template>
        </section>
      </aside>
    </div>
    <div v-if="!guest" class="desk-overview"><slot name="overview" /></div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NPopover } from 'naive-ui';
import { Bell, CalendarDays, ChevronLeft, ChevronRight, ClipboardList, CloudRain, CloudSnow, CloudSun, Radio, Search, ShieldCheck, Sun } from 'lucide-vue-next';
import { todoTimestamp } from '../businessTodos';
import { requestJson } from '../client';
import type { WorkItem } from './sources';
import { inDeskScope } from './deskPolicy';
import WorkbenchWeatherTrend from './WorkbenchWeatherTrend.vue';
import './workbench.css';

type Link = {label:string; url:string};
type DayWeather = {date:string; description:string; minimum:number|null; maximum:number|null};
type AgendaEntry = {key:string; item:WorkItem; type:'plan'|'change'|'due'; label:string; time:string; stamp:number};
type CalendarItem={kind:string;title:string;scopes?:string[];notes?:string[];start?:string;source_id?:string};
type CalendarDay={items:CalendarItem[];staff:{scope:string;shift:string;names:string[]}[]};
type CalendarWeather={hourly_note?:string;warning_ready:boolean;hours:{time:string;temp?:number|string|null}[];
  alerts:{id?:string;title:string;guard_level?:string}[];guard_level?:string;guard_actions:string[];guard_note?:string};
const props = defineProps<{name:string; avatar?:string; clock:number; links:Link[]; tasks:WorkItem[]; unread:WorkItem[]; guest?:boolean;
  metrics?:{label:string;value:number|null;partial?:boolean;note?:string;url?:string}[]; agendaScopes?:{label:string;value:string}[];
  calendarScope?:string; calendarIdentity?:string}>();
const emit = defineEmits<{open:[url:string]; unread:[]; task:[item:WorkItem]; 'guest-account':[]}>();
const indicatorIcons=[ClipboardList,Radio,CalendarDays,ShieldCheck];
const avatar=computed(()=>/^https:\/\//i.test(props.avatar||'')?props.avatar:undefined);
const avatarFailed=ref(false);
watch(avatar,()=>{avatarFailed.value=false;});
const surface = ref<HTMLElement>();
const weatherButton = ref<HTMLButtonElement>(), noticeButton = ref<HTMLButtonElement>(), searchInput = ref<HTMLInputElement>();
const panel = ref<''|'weather'|'search'|'notices'>('');
const query = ref(''), searchIndex = ref(0);
function toggle(next:'weather'|'notices') {
  panel.value = panel.value === next ? '' : next;
  if (panel.value === 'weather' && !weather.value) void readWeather();
}
function dismiss() {
  const trigger = panel.value === 'weather' ? weatherButton : panel.value === 'notices' ? noticeButton : searchInput;
  if (panel.value) { trigger.value?.focus(); panel.value = ''; }
}
function outside(event:PointerEvent) {
  if (panel.value && !(event.target instanceof Element && surface.value?.contains(event.target) && event.target.closest('.desk-anchor'))) panel.value = '';
}
function rank(label:string, term:string) {
  if (label.includes(term)) return label === term ? 0 : 1;
  let index = 0;
  for (const character of label) if (character === term[index]) index++;
  return index === term.length ? 2 : Infinity;
}
const matches = computed(() => {
  const terms = query.value.toLowerCase().split(/\s+/).filter(Boolean);
  return props.links.map(entry=>({entry,score:terms.reduce((sum,term)=>sum+rank(entry.label.toLowerCase(),term),0)}))
    .filter(row=>Number.isFinite(row.score)).sort((a,b)=>a.score-b.score).slice(0,8).map(row=>row.entry);
});
watch(matches,()=>{searchIndex.value=0;});
function moveSearch(delta:number) {
  panel.value = 'search';
  if (matches.value.length) searchIndex.value = (searchIndex.value+delta+matches.value.length)%matches.value.length;
}
function openSearch(entry?:Link) {
  if (!entry) return;
  panel.value = '';
  query.value = '';
  emit('open',entry.url);
}

const zone = 'Asia/Shanghai';
const dayKey = (stamp:number) => new Date(stamp+8*3600000).toISOString().slice(0,10);
const dayTime = (stamp:number) => new Intl.DateTimeFormat('zh-CN',{timeZone:zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(stamp);
const today = computed(()=>dayKey(props.clock));
const dateLabel = computed(()=>new Intl.DateTimeFormat('zh-CN',{timeZone:zone,month:'long',day:'numeric',weekday:'long'}).format(props.clock));
const greeting = computed(()=>{
  const hour = new Date(props.clock+8*3600000).getUTCHours();
  return hour<6?'夜里好':hour<11?'早上好':hour<14?'中午好':hour<18?'下午好':'晚上好';
});
const selected = ref(today.value), month = ref(today.value.slice(0,7));
const agendaScope=ref('');
watch(()=>props.agendaScopes,scopes=>{if(!scopes?.some(scope=>scope.value===agendaScope.value))agendaScope.value='';});
const monthDate = computed(()=>new Date(month.value+'-01T00:00:00Z'));
watch(today,(_next,previous)=>{
  if(selected.value===previous) goToday();
  if(!props.guest){weather.value=undefined;weatherAbort?.abort();void readWeather();}
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
const byDay = computed(()=>{
  const days = new Map<string,AgendaEntry[]>();
  for (const task of props.tasks) {
    if(agendaScope.value && !inDeskScope(task,agendaScope.value)) continue;
    const start = task.kind === 'notices' ? task.noticeWindow?.start || (!task.ongoing ? task.time : '') : task.scheduledAt;
    const startAt = todoTimestamp(start);
    const dueAt = task.dueAt ?? todoTimestamp(task.noticeWindow?.end);
    const add = (stamp:number, type:AgendaEntry['type'], label:string, raw?:string) => {
      const day=dayKey(stamp);
      if(type!=='due'&&task.sourceKey&&calendarDays.value[day]?.items.some(item=>item.source_id&&'change:'+item.source_id===task.sourceKey))return;
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
const agenda = computed(()=>byDay.value.get(selected.value) || []);
const calendarDays=ref<Record<string,CalendarDay>>({});
const rosterDates=ref<string[]>([]);
const calendarNotice=ref('');
const calendarDay=computed(()=>calendarDays.value[selected.value]);
const dayCount=(day:string)=>(byDay.value.get(day)?.length||0)+(calendarDays.value[day]?.items.length||0);
function scheduleTime(value?:string){
  const stamp=todoTimestamp(value);
  return stamp===undefined?'—':dayTime(stamp);
}
watch([month,agendaScope,()=>props.calendarScope,()=>props.calendarIdentity],async(_value,_old,onCleanup)=>{
  calendarDays.value={};
  rosterDates.value=[];
  calendarNotice.value='';
  if(props.guest||!props.calendarScope)return;
  const controller=new AbortController();
  onCleanup(()=>controller.abort());
  calendarNotice.value='正在读取日程…';
  try {
    const query=new URLSearchParams({month:month.value,scope:agendaScope.value||props.calendarScope});
    const data=await requestJson('/api/personal-calendar?'+query,{signal:controller.signal,timeoutMs:16000});
    if(controller.signal.aborted)return;
    if(data.month!==month.value||!data.days||!Array.isArray(data.unavailable))throw new Error('日程未完整返回');
    calendarDays.value=data.days;
    rosterDates.value=Array.isArray(data.roster_dates)?data.roster_dates:[];
    calendarNotice.value=data.unavailable.length?data.unavailable.join('、')+'来源暂不可用':'';
  } catch {
    if(!controller.signal.aborted)calendarNotice.value='日程暂不可用';
  }
},{immediate:true});
const calendarWeather=ref<CalendarWeather>();
const temperaturePoints=computed(()=>{
  const points=new Map<number,number>();
  for(const hour of calendarWeather.value?.hours||[]){
    const time=Date.parse(hour.time);
    const raw=hour.temp;
    const temperature=typeof raw==='number'?raw:typeof raw==='string'&&raw.trim()?Number(raw):NaN;
    if(Number.isFinite(time)&&Number.isFinite(temperature))points.set(time,temperature);
  }
  return [...points].sort(([a],[b])=>a-b);
});
const calendarWeatherBusy=ref(false);
let calendarWeatherRequest:AbortController|undefined;
let calendarWeatherAt=0;
async function loadCalendarWeather(){
  if(props.guest||calendarWeatherBusy.value||Date.now()-calendarWeatherAt<240000)return;
  calendarWeatherBusy.value=true;
  const request=calendarWeatherRequest=new AbortController();
  try{
    const data=await requestJson('/api/personal-calendar/weather',{signal:request.signal,timeoutMs:16000});
    if(!request.signal.aborted){calendarWeather.value=data as CalendarWeather;calendarWeatherAt=Date.now();}
  } catch {
    if(!request.signal.aborted)calendarWeather.value=undefined;
  } finally {
    if(calendarWeatherRequest===request)calendarWeatherBusy.value=false;
  }
}
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

const weather = ref<{temp:number|null;humidity:number|null;description:string;days:DayWeather[]}>();
const weatherBusy = ref(false);
const selectedWeekday = computed(()=>new Intl.DateTimeFormat('zh-CN',{timeZone:zone,weekday:'long'}).format(new Date(selected.value+'T12:00:00+08:00')));
const selectedWeather = computed(()=>{
  const forecast=weather.value?.days.find(day=>day.date===selected.value);
  const current=selected.value===today.value?weather.value:undefined;
  if (!current&&!forecast) return undefined;
  const temp=current?.temp;
  const low=forecast?.minimum, high=forecast?.maximum;
  return {
    description:current?.description||forecast?.description||'',
    temperature:temp!=null?temp+'°C':low!=null&&high!=null?low+'～'+high+'°C':high!=null?high+'°C':low!=null?low+'°C':'',
  };
});
const weatherIcon = computed(()=>{
  const description=selectedWeather.value?.description||'';
  return description.includes('雪')?CloudSnow:/雨|雷/.test(description)?CloudRain:description==='晴'?Sun:CloudSun;
});
let weatherAbort:AbortController|undefined;
async function readWeather() {
  if (weatherBusy.value&&!weatherAbort?.signal.aborted) return;
  weatherBusy.value=true;
  const controller=weatherAbort=new AbortController();
  try {
    const data=await requestJson('/api/assistant/icon-weather?detail=1',{signal:controller.signal,timeoutMs:14000});
    if(controller.signal.aborted||data.available!==true||data.date!==today.value||typeof data.description!=='string') return;
    const temperature=(value:unknown)=>typeof value==='number'&&Number.isFinite(value)?Math.round(value):null;
    const days:DayWeather[]=[];
    if(Array.isArray(data.forecast)) data.forecast.slice(0,5).forEach(day=>{
      if(typeof day?.date==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(day.date)&&typeof day.description==='string')
        days.push({date:day.date,description:day.description,minimum:temperature(day.minimum),maximum:temperature(day.maximum)});
    });
    weather.value={temp:temperature(data.temperature),humidity:temperature(data.humidity),description:data.current_description||data.description,days};
  } catch {
    // The dashboard remains usable while weather is unreachable.
  } finally {
    if(weatherAbort===controller)weatherBusy.value=false;
  }
}
onMounted(()=>{document.addEventListener('pointerdown',outside);if(!props.guest){void readWeather();void loadCalendarWeather();}});
onBeforeUnmount(()=>{document.removeEventListener('pointerdown',outside);weatherAbort?.abort();calendarWeatherRequest?.abort();});
</script>

<style scoped>
.daily-desk { min-width:0; }
.desk-bar,.desk-agenda { color:var(--wb-ink); background:var(--wb-surface); border:1px solid var(--wb-border); border-radius:12px; }
.desk-bar { display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 20px;margin-bottom:14px; }
.desk-guest { justify-content:flex-end; }
.desk-greeting { min-width:0;overflow-wrap:anywhere; }
.desk-greeting .desk-date { display:block;margin-bottom:5px;font-size:12px;color:var(--wb-muted); }
.desk-actions { display:flex;align-items:center;gap:9px;flex:none; }
.desk-anchor { position:relative; }
.desk-control,.desk-avatar,.desk-small { display:inline-flex;justify-content:center;align-items:center;gap:6px;background:var(--wb-surface);color:var(--wb-ink);border:1px solid var(--wb-border);border-radius:8px;min-width:36px;min-height:36px;padding:4px 8px; }
.desk-control[aria-expanded=true],.desk-control:hover,.desk-small:hover { color:var(--wb-primary);background:var(--desk-tint); }
.desk-avatar { width:36px;height:36px;padding:0;overflow:hidden;border-radius:50%;color:var(--wb-primary);background:var(--desk-tint);font-weight:700; }
.desk-avatar img { width:100%;height:100%;object-fit:cover; }
.daily-desk { --desk-tint:color-mix(in srgb,var(--wb-primary) 9%,var(--wb-surface)); }
.desk-bell { position:relative; }
.desk-badge { position:absolute;top:-6px;right:-5px;border:2px solid var(--wb-surface);border-radius:12px;background:var(--wb-accent);color:var(--wb-on-primary);min-width:17px;padding:0 3px;font:600 10px/15px system-ui,sans-serif; }
.desk-search { display:flex;align-items:center;gap:7px;padding:0 9px;min-height:36px;width:clamp(150px,18vw,265px);border:1px solid var(--wb-border);border-radius:8px;color:var(--wb-muted); }
.desk-search:focus-within { border-color:var(--wb-primary); }
.desk-search input { font:inherit;color:var(--wb-ink);background:transparent;border:0;outline:0;min-width:0;width:100%; }
.desk-popover { position:absolute;right:0;top:calc(100% + 7px);z-index:12;min-width:235px;max-width:min(340px,calc(100vw - 32px));padding:12px;background:var(--wb-surface);border:1px solid var(--wb-border);border-radius:10px;box-shadow:0 8px 24px #17274824;color:var(--wb-ink); }
.desk-notifications { width:300px; }
.desk-popover > p { margin-top:8px;color:var(--wb-muted); }
.desk-pop-list,.desk-schedule { list-style:none;margin:8px 0 0;padding:0; }
.desk-pop-list li { display:flex;align-items:center;justify-content:space-between;gap:8px;padding:5px 0; }
.desk-pop-list li button { display:flex;justify-content:space-between;align-items:center;gap:8px;width:100%;padding:6px;border:0;text-align:left;border-radius:6px;color:inherit;background:none; }
.desk-pop-list li button:hover,.desk-pop-list li button.highlight { background:var(--desk-tint); }
.desk-pop-list li button span { overflow-wrap:anywhere; }
.desk-pop-list svg { flex-shrink:0; }
.desk-columns { display:grid;grid-template-columns:minmax(0,2fr) minmax(268px,1fr);gap:14px;align-items:start; }
.desk-indicators { display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:14px; }
.desk-metric { display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:4px 8px;min-height:92px;padding:12px 15px;border:1px solid var(--wb-border);border-radius:10px;background:var(--wb-surface); }
.desk-metric svg { grid-area:1/2;color:var(--wb-primary);box-sizing:content-box;padding:6px;border-radius:8px;background:var(--desk-tint); }
.desk-metric h2 { grid-area:1/1;color:var(--wb-muted);font-weight:600;font-size:13px; }
.desk-metric strong { font-size:29px;line-height:1.05;font-variant-numeric:tabular-nums;grid-column:1/-1; }
.desk-metric strong small,.desk-metric-note { font-size:11px;color:var(--wb-muted);font-weight:400; }
.desk-metric-note { grid-column:1/-1; }
.desk-metric .desk-metric-link { grid-column:1/-1;display:flex;align-items:center;gap:4px;min-height:24px;padding:0;font-size:12px; }
.desk-main { min-width:0; }
.desk-main :deep(.wb-module-grid) { grid-template-columns:minmax(0,1fr); }
.desk-main :deep(.desk-delete) { flex:none; }
.desk-overview { margin-top:14px; }
.desk-overview :deep(.wb-period) { grid-column:1/-1; }
.desk-agenda { padding:17px; }
.desk-agenda-title,.desk-month,.desk-schedule-head { display:flex;align-items:center;gap:8px; }
.desk-agenda-title svg { color:var(--wb-primary); }
.desk-agenda-title h2 { font-size:18px; }
.desk-agenda-scope { display:flex;align-items:center;gap:8px;margin-top:12px;color:var(--wb-muted); }
.desk-agenda-scope select { min-width:0;flex:1;color:var(--wb-ink);background:var(--wb-surface);border:1px solid var(--wb-border);border-radius:6px;padding:5px; }
.desk-month,.desk-schedule-head { justify-content:space-between;margin:15px 0 8px; }
.desk-month > div { display:flex;gap:3px; }
.desk-small { min-height:28px;min-width:28px;padding:2px 4px;font-size:12px;border-color:transparent; }
.desk-calendar { display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:3px;text-align:center; }
.desk-weekday { color:var(--wb-muted);padding:4px 0;font-size:12px; }
.desk-day { position:relative;display:grid;place-items:center;min-height:34px;border:1px solid transparent;border-radius:7px;background:none;color:inherit;font-variant-numeric:tabular-nums; }
.desk-day[aria-current=date] { border-color:var(--wb-primary); }
.desk-day.scheduled { background:var(--desk-tint);color:var(--wb-primary);font-weight:700; }
.desk-day.conflict:not([aria-pressed=true]) { color:var(--v2-danger,#a42b35);background:color-mix(in srgb,currentColor 10%,var(--wb-surface)); }
.desk-day:hover { border-color:var(--wb-primary); }
.desk-day[aria-pressed=true] { background:var(--wb-accent);color:var(--wb-on-primary); }
.desk-day i { position:absolute;bottom:2px;width:4px;height:4px;border-radius:50%;background:currentColor; }
.desk-day-preview { width:min(300px,calc(100vw - 48px));max-height:45dvh;overflow:auto;overflow-wrap:anywhere; }
.desk-day-preview ul { list-style:none;margin:6px 0 0;padding:0; }
.desk-day-preview li { display:grid;gap:3px;padding:6px 0;border-top:1px solid var(--v2-border,#dbe2ed); }
.desk-day-preview small { color:var(--v2-muted,#526073); }
.desk-other { color:var(--wb-muted);opacity:.55; }
.desk-weather-panel { margin-top:12px;border-radius:8px;background:linear-gradient(110deg,var(--desk-tint),var(--wb-surface)); }
.desk-weather-strip { display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:60px;padding:8px 12px; }
.desk-weather-date { display:flex;align-items:baseline;gap:6px;white-space:nowrap; }
.desk-weather-date b { font-size:28px;color:var(--wb-primary);font-variant-numeric:tabular-nums; }
.desk-weather-icon { flex:none;color:var(--wb-primary); }
.desk-weather-reading { min-width:0;text-align:right; }
.desk-weather-reading b { display:block;font-size:17px; }
.desk-weather-reading small { display:block;color:var(--wb-muted); }
.desk-schedule-head span,.desk-event-time,.desk-schedule small { color:var(--wb-muted);font-size:12px; }
.desk-schedule { max-height:240px;overflow:auto; }
.desk-schedule li { display:grid;grid-template-columns:39px 7px 1fr;gap:7px;align-items:start;padding:8px 0;border-top:1px solid var(--wb-border); }
.desk-schedule strong,.desk-schedule small { display:block;overflow-wrap:anywhere; }
.desk-agenda-link { display:block;padding:0;border:0;background:none;text-align:left;color:var(--wb-ink);font-weight:650;overflow-wrap:anywhere; }
.desk-agenda-link:hover { color:var(--wb-primary);text-decoration:underline; }
.desk-event-dot { width:7px;height:7px;border-radius:50%;margin-top:6px;background:var(--wb-primary); }
.desk-event-dot.change { background:#a072d6; }
.desk-event-dot.due { background:#dc8a27; }
.desk-event-dot.conflict { background:var(--v2-danger,#a42b35); }
.desk-calendar-note { padding:7px 0;color:var(--wb-muted);font-size:12px; }
.desk-schedule-extra { border-top:1px solid var(--wb-border);margin-top:10px;padding-top:8px; }
.desk-schedule-extra h3 { font-weight:650;line-height:1.6; }
.desk-schedule-extra h3 span { margin-left:5px;font-weight:400;color:var(--wb-muted);font-size:12px; }
.desk-schedule-extra > .wb-note { margin-top:6px; }
.desk-staff { display:grid;grid-template-columns:auto 1fr;gap:5px 9px;margin:7px 0 0; }
.desk-staff dt { color:var(--wb-muted); }
.desk-staff dd { margin:0;overflow-wrap:anywhere; }
.desk-hourly { margin:0 10px;padding:6px 0 1px;border-top:1px solid color-mix(in srgb,var(--wb-border) 65%,transparent); }
.desk-agenda .desk-hourly h3 { font-size:12px;font-weight:550;color:var(--wb-muted); }
.desk-hourly p { padding:7px 0 9px;font-size:12px; }
.desk-alerts small { font-size:11px;color:var(--wb-muted); }
.desk-alerts ul { padding-left:18px;margin:5px 0; }
.desk-main :deep(.desk-queue) { display:block;padding:0;border-radius:12px; }
.desk-main :deep(.wb-todo-toolbar) { border:0;border-radius:12px;padding:12px 14px; }
.desk-main :deep(.wb-task-module-grid) { display:block; }
.desk-main :deep(.wb-task-list > li) { padding:0 14px; }
.desk-main :deep(.wb-task-row) { min-height:60px; }
.desk-main :deep(.wb-task-action) { max-width:36%;text-align:right;white-space:normal;overflow-wrap:anywhere; }
.desk-main :deep(.wb-task-action > span:first-child) { white-space:normal;color:var(--wb-muted); }
.desk-main :deep(.wb-task-group),.desk-main :deep(.wb-queue-status) { margin:0;padding:5px 14px; }
.desk-main :deep(.desk-task-tabs) { display:flex;gap:16px;border-bottom:1px solid var(--wb-border);padding:0 14px; }
.desk-main :deep(.desk-task-tabs button) { padding:8px 3px;border:0;border-bottom:2px solid transparent;background:none;color:var(--wb-muted); }
.desk-main :deep(.desk-task-tabs button[aria-pressed=true]) { border-color:var(--wb-primary);color:var(--wb-primary);font-weight:650; }
.desk-main :deep(.desk-module-pill) { background:var(--desk-tint);color:var(--wb-primary);border-radius:20px;width:88px;max-width:30%;flex:none;text-align:center;padding:4px 3px;font-size:12px; }
.desk-main :deep(.desk-unread) { display:inline-block;width:6px;height:6px;margin-right:5px;vertical-align:2px;border-radius:50%;background:var(--wb-primary); }
.desk-overview :deep(.wb-dashboard) { display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px; }
@container workbench (max-width:790px) {
  .desk-columns { grid-template-columns:minmax(0,1fr); }
}
@container workbench (max-width:530px) {
  .desk-indicators,.desk-overview :deep(.wb-dashboard) { grid-template-columns:repeat(2,minmax(0,1fr));gap:8px; }
  .desk-bar { align-items:flex-start;flex-direction:column;padding:12px; }
  .desk-actions { width:100%; }
  .desk-search { flex:1;min-width:0;width:auto; }
  .desk-actions > .desk-anchor:first-child .desk-popover { left:0;right:auto; }
  .desk-actions { position:relative; }
  .desk-actions > .desk-anchor { position:static; }
  .desk-actions .desk-popover { min-width:0;max-width:100%;width:min(300px,100%); }
  .desk-main :deep(.wb-task-list > li) { padding:0 8px; }
  .desk-main :deep(.desk-module-pill) { width:auto;max-width:27%; }
}
</style>
