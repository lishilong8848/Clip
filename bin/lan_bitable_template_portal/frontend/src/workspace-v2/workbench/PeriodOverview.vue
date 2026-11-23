<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { ArrowUpRight, BookOpenCheck, Building2, ChevronRight, ClipboardCheck, ListChecks, MapPinned, ShieldCheck } from 'lucide-vue-next';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';
import type { Dict } from '../client';
import { todoTimestamp } from '../businessTodos';
import { beijingDate, eventPeriodRange } from '../eventPeriod';
import { readWorkbenchRequest } from './useWorkbenchReads';
import type { Role, ScopeOption } from './recipes';
import type { WorkItem } from './sources';
import type { CalendarItem, SealStatus } from './WorkbenchDailyLayout.vue';

const props=defineProps<{user:Dict;scope:string;scopeLabel:string;scopes:ScopeOption[];role:Role;clock:number;tasks:WorkItem[];
  planState?:'loading'|'ok'|'stale'|'down'|'deny';sealStatus?:SealStatus;seals:CalendarItem[];partial:boolean;
  checkStatus:string;inspectionState?:'loading'|'ok'|'stale'|'down'|'deny';action:(item:WorkItem)=>string}>();
defineEmits<{open:[url:string];task:[item:WorkItem];retry:[];check:[];retryInspection:[]}>();
const period=ref<'day'|'week'|'month'>('week');
const view=ref<'cards'|'table'>('cards');
const periods=[{id:'day',label:'今日'},{id:'week',label:'本周'},{id:'month',label:'本月'}] as const;
const filters=[
  {id:'all',label:'全部'},{id:'late',label:'逾期'},{id:'soon',label:'今明截止'},
  {id:'running',label:'进行中'},{id:'planned',label:'未开始'},{id:'seal',label:'封网'},
] as const;
type Filter = typeof filters[number]['id'];
const filter=ref<Filter>('all');
const today=computed(()=>beijingDate(props.clock));
const canSeeCampus=computed(()=>props.role==='manager'||props.user.category==='manager');
const canSeeBuilding=computed(()=>!['guest','temporary'].includes(props.user.role)&&!!props.scope);
const canSeePersonal=computed(()=>!props.user.impersonation&&!['guest','temporary'].includes(props.user.role));
const canReadLearning=computed(()=>!props.user.impersonation&&props.user.modules?.includes('learning'));
const canReadTeam=computed(()=>canReadLearning.value&&(canSeeCampus.value||props.role==='supervisor')
  &&/^(?:ALL|CAMPUS|[A-EH])$/.test(props.scope));
const range=computed(()=>{
  const at=Date.parse(today.value+'T12:00:00+08:00');
  return period.value==='day'?eventPeriodRange('custom','',at,[today.value,today.value]):eventPeriodRange(period.value,'',at);
});
const labels:Partial<Record<WorkItem['kind'],string>>={
  notices:'通告',events:'事件',event_notices:'事件',repair_management:'维修跟进',engineer_mop:'维保单',critical_guard:'重保',
  risk_inspection:'专项排查',water:'水耗',drills:'演练',inspection:'巡检',cabinet:'上下电',learning:'画像学练',accounts:'权限审批',tool_management:'工具',
};
const noticeLabel=(item:WorkItem)=>item.detail==='维保'?'维保通告':item.detail==='变更'?'变更通告':item.detail||'通告';
const label=(item:WorkItem)=>item.kind==='notices'?noticeLabel(item):labels[item.kind]||'待办';
const scopeName=(scope:string)=>/^[A-EH]$/.test(scope)?scope+'楼':scope==='110'?'110站':['ALL','CAMPUS'].includes(scope)?'园区':scope;
const dateFormat=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'numeric',day:'numeric'});
const timeFormat=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const timeText=(value:number)=>timeFormat.format(value);
const dateOnly=(value?:string|number)=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value);
const startAt=(item:WorkItem)=>todoTimestamp(item.noticeWindow?.start||item.scheduledAt||item.time);
const deadline=(item:WorkItem)=>item.dueAt??todoTimestamp(item.noticeWindow?.end);
const running=(item:WorkItem)=>item.kind==='notices'?item.ongoing===true:item.kind==='events'||item.ongoing||/进行中|处理中|维修中/.test(item.status);
const planned=(item:WorkItem)=>item.kind==='notices'&&!running(item);
const tomorrow=computed(()=>beijingDate(props.clock+86400000));
function urgency(item:WorkItem){
  const due=deadline(item);
  if(due===undefined)return 4;
  return due<=props.clock?0:beijingDate(due)===today.value?1:beijingDate(due)===tomorrow.value?2:3;
}
function matchesPeriod(item:WorkItem){
  const from=startAt(item), end=deadline(item);
  if(planned(item)){
    if(item.detail==='检修')return true;
    if(from===undefined&&end===undefined)return period.value==='month'&&item.planMonth===Number(today.value.slice(5,7))+'月';
    return (from??end!)<range.value.end&&(end??from!)>=range.value.start;
  }
  if (end!==undefined&&end<=props.clock) return true;
  if (running(item)||item.kind==='repair_management') return from===undefined||from<range.value.end;
  if (from===undefined&&end===undefined)return true;
  return (from??end!)<range.value.end&&(end??from!)>=range.value.start;
}
function sortRows(a:WorkItem,b:WorkItem){
  return urgency(a)-urgency(b)||(deadline(a)??Infinity)-(deadline(b)??Infinity)
    ||Number(!!b.urgentEvent)-Number(!!a.urgentEvent)||a.title.localeCompare(b.title,'zh-CN');
}
function timing(item:WorkItem){
  const due=deadline(item);
  if(due!==undefined){
    const when=dateOnly(item.noticeWindow?.end)?dateFormat.format(due):timeText(due);
    return ['已逾期','今日截止','明日截止','截止'][urgency(item)]+' '+when;
  }
  const raw=item.noticeWindow?.start||item.scheduledAt||item.time, start=startAt(item);
  return start===undefined?'':dateOnly(raw)?dateFormat.format(start):timeText(start);
}
const highLevel=new Set(['events','event_notices','critical_guard','risk_inspection']);
const personalKinds=new Set(['learning','signatures','inspection']);
const inspectionTask=computed(()=>props.tasks.find(row=>row.kind==='inspection'));
const active=computed(()=>props.tasks.filter(matchesPeriod).sort(sortRows));
const campusFocus=(row:WorkItem)=>highLevel.has(row.kind);
const campusRows=computed(()=>active.value.filter(campusFocus));
const buildingRows=computed(()=>active.value.filter(row=>!personalKinds.has(row.kind)&&(!canSeeCampus.value||!campusFocus(row))));
const cards=computed(()=>[
  ...(canSeeCampus.value?[{id:'campus',title:'园区',icon:MapPinned,rows:campusRows.value}]:[]),
  ...(canSeeBuilding.value?[{id:'building',title:'楼栋',icon:Building2,rows:buildingRows.value}]:[]),
  ...(canSeePersonal.value?[{id:'personal',title:'个人',icon:BookOpenCheck,
    rows:active.value.filter(row=>personalKinds.has(row.kind)&&row.kind!=='inspection'&&!(row.kind==='learning'&&canReadLearning.value))}]:[]),
]);
const sealRows=computed(()=>props.seals.filter(item=>{
  const start=todoTimestamp(item.start),end=todoTimestamp(item.end);
  return start!==undefined&&end!==undefined&&start<range.value.end&&end>=range.value.start&&end>=props.clock;
}).sort((a,b)=>(todoTimestamp(a.start)??0)-(todoTimestamp(b.start)??0)));
const matchesFilter=(item:WorkItem,value:Filter)=>{
  switch(value){
    case 'late':return urgency(item)===0;
    case 'soon':return urgency(item)===1||urgency(item)===2;
    case 'running':return running(item);
    case 'planned':return planned(item);
    case 'seal':return false;
    default:return true;
  }
};
const statusFilters=computed(()=>{
  const rows=cards.value.flatMap(card=>card.rows);
  const seals=sealRows.value.length;
  return filters.map(option=>({...option,count:option.id==='seal'?seals:
    option.id==='all'?rows.length+seals:rows.filter(item=>matchesFilter(item,option.id)).length}))
    .filter(option=>option.id!=='seal'||option.count>0||filter.value==='seal');
});
const sealCard=computed(()=>canSeeCampus.value?'campus':'building');
const visibleCards=computed(()=>cards.value.map(card=>({...card,
  rows:filter.value==='all'?card.rows:card.rows.filter(item=>matchesFilter(item,filter.value)),
  seals:card.id===sealCard.value&&(filter.value==='all'||filter.value==='seal')?sealRows.value:[],
})).filter(card=>filter.value==='all'||card.rows.length||card.seals.length));
function cardGroups(card:typeof visibleCards.value[number]){
  if(card.id!=='building')return [{id:card.id,label:'',rows:card.rows}];
  return [
    {id:'building-running',label:'进行中通告',rows:card.rows.filter(row=>row.kind==='notices'&&running(row))},
    {id:'building-planned',label:'未开始通告',rows:card.rows.filter(planned)},
    {id:'building-other',label:'其他待办',rows:card.rows.filter(row=>row.kind!=='notices')},
  ].filter(group=>group.rows.length);
}
const incomplete=computed(()=>props.partial||['loading','stale','down'].includes(props.planState||''));
const rowMeta=(item:WorkItem)=>[
  (item.referencedScopes?.length?[...new Set(item.referencedScopes)].sort().map(scopeName).join('、'):scopeName(item.scope)),
  item.urgentEvent,
  item.status,
].filter(Boolean).join(' · ');
const expanded=ref<Partial<Record<string,boolean>>>({});
watch([period,filter],()=>{expanded.value={};});
watch(()=>props.scope,()=>{filter.value='all';expanded.value={};});
const issuerScopes=computed(()=>new Set(props.scopes.map(item=>item.value)));
type Count = {done:number;total:number;pending:number;late:number;checking:number;upcoming:number};
const team=ref<Count>(), personal=ref<{done?:number;total?:number;message?:string}>();
const teamError=ref(''), personalError=ref(''), revision=ref(0);
function count(value:unknown){
  if(typeof value!=='number'||!Number.isFinite(value)||value<0)throw new Error('完成数据待核验');
  return value;
}
watch([()=>props.scope,period,today,canReadTeam,canReadLearning,canSeePersonal,revision],async(_current,_previous,onCleanup)=>{
  const request=new AbortController();
  onCleanup(()=>request.abort());
  team.value=undefined;personal.value=undefined;teamError.value=personalError.value='';
  const from=beijingDate(range.value.start),to=today.value,scope=props.scope,signal=request.signal;
  const read=(path:string,params:Record<string,string>)=>readWorkbenchRequest(path+'?'+new URLSearchParams(params),signal);
  const requests:Promise<unknown>[]=[];
  if(canReadTeam.value)requests.push(read('/api/learning/workbench-summary',{scope,from,to}).then(data=>{
    if(signal.aborted)return;
    if(!Array.isArray(data.scopes)||!data.scopes.length||data.scopes.some((building:string)=>
      ['ALL','CAMPUS'].includes(scope)?issuerScopes.value.size>0&&!issuerScopes.value.has(building):scope!==building))
      throw new Error('学练汇总楼栋未核验');
    const done=count(data.summary?.['已完成']),late=count(data.summary?.['逾期']);
    const pending=count(data.summary?.['待答'])+count(data.summary?.['答题中'])+late;
    team.value={done,total:done+pending,pending,late,checking:count(data.summary?.['待核对']),upcoming:count(data.summary?.['未到时'])};
  }).catch(cause=>{if(!signal.aborted)teamError.value=[401,403].includes(cause?.status)?'当前账号无权查看学练汇总':'学练汇总读取失败';}));
  if(canReadLearning.value&&canSeePersonal.value)requests.push(read('/api/learning/bootstrap',{}).then(async boot=>{
    if(signal.aborted)return;
    if(!boot.self_person?.id){personal.value={message:'账号未关联学练人员'};return;}
    if(!['ALL','CAMPUS'].includes(scope)&&boot.self_scope&&boot.self_scope!==scope){
      personal.value={message:'本楼暂无个人题单'};return;
    }
    if(period.value==='day'&&boot.daily_exemption?.status==='exempt'){personal.value={message:'入职首月免答'};return;}
    const data=await read('/api/learning/profile',{scope:boot.self_scope||'',person_id:boot.self_person.id,from,to});
    if(signal.aborted)return;
    const summary=period.value==='day'?data.today_summary:data.summary;
    const done=count(summary?.task_answered),total=count(summary?.assigned);
    personal.value={done,total,message:total?undefined:'本周期暂无当班题单'};
  }).catch(cause=>{if(!signal.aborted)personalError.value=[401,403].includes(cause?.status)?'账号未关联学练人员':'学练进度读取失败';}));
  await Promise.all(requests);
},{immediate:true});
</script>

<template>
  <section v-if="cards.length" class="wb-period" aria-label="工作状态">
    <header class="period-tools">
      <h2>工作状态</h2>
      <div class="period-switch" role="group" aria-label="统计周期">
        <button v-for="option in periods" :key="option.id" type="button" :aria-pressed="period===option.id" @click="period=option.id">{{ option.label }}</button>
      </div>
      <RecordViewSwitch v-model="view"/>
    </header>
    <div class="status-filters" role="group" aria-label="工作状态筛选">
      <button v-for="option in statusFilters" :key="option.id" type="button" :data-filter="option.id" :data-has-items="option.count>0"
        :aria-pressed="filter===option.id" @click="filter=option.id">{{ option.label }} <b>{{ option.count }}</b></button>
    </div>
    <p v-if="planState==='down'||planState==='stale'" class="period-muted" role="status">计划通告待更新 <button type="button" @click="$emit('retry')">重试</button></p>
    <p v-else-if="incomplete" class="period-muted" role="status">{{ planState==='loading'?'正在读取计划通告…':'部分数据待更新' }}</p>
    <p v-if="!visibleCards.length&&!incomplete" class="period-empty" role="status">
      {{ filter==='seal'&&sealStatus!=='ready'?'封网信息待更新':'本周期暂无此类事项' }}
    </p>
    <div class="period-grid" :data-view="view">
      <article v-for="card in visibleCards" :key="card.id" class="period-card" :data-card="card.id" :aria-label="card.title+'卡片'">
        <header class="card-head">
          <component :is="card.icon" :size="17" aria-hidden="true"/><h3>{{ card.title }}</h3>
          <small v-if="card.id!=='personal'">{{ scopeLabel }}</small>
          <span v-if="card.rows.length||card.seals.length" class="row-count"><b>{{ card.rows.length+card.seals.length }}</b> 项</span>
        </header>

        <section v-for="group in cardGroups(card)" v-show="group.rows.length" :key="group.id" class="period-group"
          :data-group="group.id" :aria-label="group.label||undefined">
          <h4 v-if="group.label" class="period-group-title">{{ group.label }} <small>{{ group.rows.length }}</small></h4>
          <ul class="period-entries" :class="{expanded:expanded[group.id]}">
            <li v-for="item in (expanded[group.id]?group.rows:group.rows.slice(0,4))" :key="item.key">
              <button type="button" class="period-entry" :data-tone="urgency(item)===0?'late':urgency(item)<3?'soon':undefined" @click="$emit('task',item)">
                <span class="period-kind">{{ label(item) }}</span><strong class="period-title">{{ item.title }}</strong>
                <span class="period-action">{{ action(item) }} <ChevronRight :size="13" aria-hidden="true"/></span>
                <small class="period-meta">{{ rowMeta(item) }}</small>
                <small v-if="timing(item)" class="period-time">{{ timing(item) }}</small>
              </button>
            </li>
          </ul>
          <button v-if="group.rows.length>4" type="button" class="period-expand" :aria-expanded="!!expanded[group.id]" @click="expanded[group.id]=!expanded[group.id]">
            {{ expanded[group.id]?'收起':`展开其余 ${group.rows.length-4} 项` }}
          </button>
        </section>

        <div v-if="card.seals.length" class="seal-list" aria-label="周期内封网">
          <div v-for="item in card.seals" :key="item.title+'-'+item.start" class="seal-row">
            <ShieldCheck :size="15" aria-hidden="true"/>
            <div><strong>{{ item.title }}</strong><small>{{ timeText(todoTimestamp(item.start)!) }} 至 {{ timeText(todoTimestamp(item.end)!) }}</small></div>
            <small class="seal-state">{{ todoTimestamp(item.start)!<=clock?'封网中':'待生效' }}</small>
          </div>
        </div>

        <div v-if="filter==='all'&&canReadTeam&&(card.id==='campus'||!canSeeCampus&&card.id==='building')" class="learning-summary">
          <h4>画像学练情况汇总</h4>
          <template v-if="team">
            <div class="learning-kpis"><span>已完成 <b>{{ team.done }} / {{ team.total }}</b> 人天</span>
              <span :data-warn="team.pending>0">未完成 <b>{{ team.pending }}</b></span><span v-if="team.late" data-warn="true">其中逾期 <b>{{ team.late }}</b></span></div>
            <meter :value="team.done" :max="team.total||1" aria-label="学练人天完成进度"/>
            <small v-if="team.checking||team.upcoming">{{ [team.checking?'待核对 '+team.checking:'',team.upcoming?'未到时 '+team.upcoming:''].filter(Boolean).join(' · ') }}</small>
            <small v-else-if="!team.total">暂无已到作答时段的人员</small>
          </template>
          <small v-else role="status">{{ teamError || '正在读取学练汇总…' }}
            <button v-if="teamError==='学练汇总读取失败'" type="button" @click="revision++">重试</button>
          </small>
        </div>

        <div v-if="filter==='all'&&card.id==='personal'&&canReadLearning" class="learning-summary">
          <div class="learning-title"><strong>我的画像学练</strong>
            <button type="button" aria-label="打开画像学练" @click="$emit('open','/learning?view=practice')">去学练 <ArrowUpRight :size="14" aria-hidden="true"/></button></div>
          <template v-if="personal">
            <div v-if="personal.total" class="learning-kpis"><span>已答 <b>{{ personal.done }} / {{ personal.total }}</b> 题</span>
              <span>{{ personal.total>Number(personal.done)?'待完成':'已完成' }}</span></div>
            <meter v-if="personal.total" :value="Math.min(personal.done||0,personal.total)" :max="personal.total" aria-label="当班题单进度"/>
            <small v-if="personal.message">{{ personal.message }}</small>
          </template>
          <small v-else role="status">{{ personalError || '正在读取个人进度…' }}
            <button v-if="personalError==='学练进度读取失败'" type="button" @click="revision++">重试</button>
          </small>
        </div>

        <div v-if="filter==='all'&&card.id==='personal'&&(checkStatus||inspectionState&&inspectionState!=='deny')" class="personal-duty">
          <button v-if="checkStatus" type="button" class="personal-duty-row" @click="$emit('check')">
            <ClipboardCheck :size="16" aria-hidden="true"/><strong>班前检查</strong><small>{{ checkStatus }}</small><ChevronRight :size="13" aria-hidden="true"/>
          </button>
          <button v-if="inspectionTask" type="button" class="personal-duty-row" @click="$emit('task',inspectionTask)">
            <ListChecks :size="16" aria-hidden="true"/><strong>{{ inspectionTask.title }}</strong><small>{{ inspectionTask.status }}</small><ChevronRight :size="13" aria-hidden="true"/>
          </button>
          <div v-else-if="inspectionState&&inspectionState!=='deny'" class="personal-duty-row" role="status">
            <ListChecks :size="16" aria-hidden="true"/><strong>日常巡检</strong>
            <small>{{ inspectionState==='loading'?'读取中…':inspectionState==='ok'?'暂无待巡检':'待核对' }}</small>
            <button v-if="inspectionState==='stale'||inspectionState==='down'" type="button" @click="$emit('retryInspection')">重试</button>
          </div>
        </div>

        <p v-if="!card.rows.length&&!card.seals.length&&!incomplete
          &&!(canReadTeam&&(card.id==='campus'||!canSeeCampus&&card.id==='building'))
          &&!(card.id==='personal'&&(canReadLearning||checkStatus||inspectionState&&inspectionState!=='deny'))" class="period-muted">本周期暂无待处理事项</p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.wb-period{display:grid;gap:8px;min-width:0;container:wb-period / inline-size}
.period-tools,.period-switch,.card-head,.learning-title,.learning-kpis{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.period-tools h2{margin-inline-end:auto}
.period-switch button,.status-filters button,.period-expand,.learning-title button,.period-muted button,.learning-summary small button{
  border:1px solid var(--desk-line);background:var(--desk-paper);color:var(--desk-green);
  border-radius:7px;min-height:32px;padding:4px 8px;cursor:pointer;font-size:.78rem}
.period-switch button[aria-pressed=true],.status-filters button[aria-pressed=true]{
  background:var(--desk-green-wash);border-color:var(--desk-green);font-weight:700}
.status-filters{display:flex;gap:5px;flex-wrap:wrap}
.status-filters button,.row-count{font-variant-numeric:tabular-nums;white-space:nowrap}
.status-filters button b{margin-inline-start:3px;font-size:.82rem}
.status-filters button[data-filter=late][data-has-items=true]{color:var(--cf-danger,#a43c31)}
.status-filters button[data-filter=soon][data-has-items=true]{color:var(--cf-warning,#8a4d11)}
.period-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,255px),1fr));gap:8px;align-items:start}
.period-grid[data-view=table]{grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr))}
.period-card{min-width:0;display:grid;gap:7px;padding:9px;border:1px solid var(--desk-line);border-radius:12px;background:var(--desk-paper);overflow-wrap:anywhere;container:period-card / inline-size}
.card-head{min-height:24px}
.card-head svg{color:var(--desk-green)}
.card-head h3{margin:0}.card-head small,.period-muted,.learning-summary small{color:var(--desk-muted);font-size:.76rem}
.row-count{margin-inline-start:auto;color:var(--desk-muted);font-size:.77rem}
.row-count b{color:var(--desk-ink)}
.learning-summary{min-width:0;display:grid;gap:4px;padding:6px;border-radius:7px;background:var(--desk-well);font-size:.83rem}
.learning-title{justify-content:space-between}
.learning-title button{display:inline-flex;align-items:center;gap:3px}
.learning-summary meter{width:100%;height:9px;accent-color:var(--desk-green)}
.learning-summary meter::-webkit-meter-optimum-value{background:var(--desk-green)}
.learning-kpis{color:var(--desk-muted);font-size:.76rem}
.learning-kpis b{font-variant-numeric:tabular-nums;color:var(--desk-green)}
.learning-kpis [data-warn=true],.learning-kpis [data-warn=true] b{color:var(--cf-warning,#8a4d11)}
.personal-duty{display:grid;gap:4px}
.personal-duty-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto;align-items:center;gap:5px;text-align:start;
  min-width:0;min-height:34px;padding:5px 7px;border:1px solid var(--desk-line);border-radius:7px;background:var(--desk-well);color:var(--desk-ink);font:inherit;font-size:.79rem}
button.personal-duty-row{cursor:pointer}
.personal-duty-row :is(svg,small){color:var(--desk-green)}
.personal-duty-row button{color:var(--desk-green)}
.period-entries{display:grid;gap:4px;list-style:none;margin:0;padding:0}
.period-group{display:grid;gap:4px;min-width:0}
.period-group-title{display:flex;align-items:center;gap:6px;margin:2px 0 0;color:var(--desk-muted);font-size:.78rem}
.period-group-title small{padding:0 .4em;border-radius:999px;background:var(--desk-well);font-variant-numeric:tabular-nums}
.period-group[data-group=building-running] .period-group-title{color:var(--desk-green)}
.period-group[data-group=building-planned] .period-group-title{color:var(--v2-amber,#99441d)}
.period-entries.expanded,.seal-list{max-height:21rem;overflow:auto;overscroll-behavior:contain;padding:2px}
.period-entry,.seal-row{width:100%;min-width:0;display:grid;gap:3px 7px;align-items:center;text-align:left;
  border:1px solid var(--desk-line);border-radius:8px;background:var(--desk-well);padding:6px 7px;color:var(--desk-ink);font:inherit;font-size:.8rem}
.period-entry{grid-template-columns:minmax(0,1fr) auto;grid-template-areas:"kind action" "title title" "meta time"}
.period-entry:hover,.period-entry:focus-visible{border-color:var(--desk-green)}
.period-kind{grid-area:kind;justify-self:start;color:var(--desk-green);font-size:.74rem;font-weight:650}
.period-title{grid-area:title;overflow-wrap:anywhere;line-height:1.32}
.period-action{grid-area:action;justify-self:end;display:flex;align-items:center;gap:1px;color:var(--desk-green);font-size:.75rem;white-space:nowrap}
.period-meta,.period-time,.seal-row small{font-size:.74rem;color:var(--desk-muted);line-height:1.35}
.period-meta{grid-area:meta}.period-time{grid-area:time;justify-self:end;text-align:end}
.period-entry[data-tone=late]{border-inline-start:3px solid var(--cf-danger,#a43c31)}
.period-entry[data-tone=soon]{border-inline-start:3px solid var(--cf-warning,#8a4d11)}
.period-entry[data-tone=late] .period-time{color:var(--cf-danger,#a43c31);font-weight:700}
.period-entry[data-tone=soon] .period-time{color:var(--cf-warning,#8a4d11);font-weight:700}
.period-expand{justify-self:end}
.seal-list{display:grid;gap:4px}
.seal-row{grid-template-columns:auto minmax(0,1fr) auto}
.seal-row>div{display:grid;gap:2px}
.seal-row :is(svg,.seal-state){color:var(--desk-green)}
.period-empty{padding:10px 12px;border:1px solid var(--desk-line);border-radius:8px;color:var(--desk-muted);font-size:.8rem}
@container wb-period (min-width:815px){
  .period-grid[data-view=cards]:has(article:nth-child(3)){grid-template-columns:minmax(0,1fr) minmax(0,1fr) minmax(190px,.64fr)}
}
@container period-card (min-width:525px){
  .period-grid[data-view=cards] .period-entries{grid-template-columns:repeat(2,minmax(0,1fr))}
  .period-grid[data-view=table] .period-entry{
    grid-template-columns:75px minmax(0,1fr) minmax(0,.55fr) minmax(0,.68fr) auto;
    grid-template-areas:"kind title meta time action";min-height:38px
  }
}
.period-grid[data-view=table] .period-group{max-inline-size:56rem}
@media(pointer:coarse){.period-tools button,.status-filters button,.period-entry,.period-expand,.learning-title button,.personal-duty-row{min-height:44px}}
</style>
