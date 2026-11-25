<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { NModal } from 'naive-ui';
import { BookOpenCheck, ChevronDown, ChevronRight, ListChecks, Pencil } from 'lucide-vue-next';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';
import type { Dict } from '../client';
import { todoTimestamp } from '../businessTodos';
import { beijingDate, eventPeriodRange } from '../eventPeriod';
import { readWorkbenchRequest } from './useWorkbenchReads';
import { requestLearning } from '../../api/learning';
import WorkbenchQuestionReview from './WorkbenchQuestionReview.vue';
import type { Role, ScopeOption } from './recipes';
import type { WorkItem } from './sources';
import type { CalendarItem, SealStatus } from './WorkbenchDailyLayout.vue';
import { noticeTileLabel, taskTileKey, taskTilePriority, taskTileStyle } from './taskTiles';

const props=defineProps<{user:Dict;scope:string;scopeLabel:string;scopes:ScopeOption[];role:Role;clock:number;tasks:WorkItem[];
  planState?:'loading'|'ok'|'stale'|'down'|'deny';sealStatus?:SealStatus;seals:CalendarItem[];partial:boolean;learningRevision:number;
  inspectionState?:'loading'|'ok'|'stale'|'down'|'deny';action:(item:WorkItem)=>string}>();
defineEmits<{task:[item:WorkItem];retry:[];retryInspection:[]}>();
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
const canSeePersonal=computed(()=>!['guest','temporary'].includes(props.user.role));
const canReadLearning=computed(()=>canSeePersonal.value&&props.user.modules?.includes('learning'));
const canReadPersonalLearning=computed(()=>canReadLearning.value&&props.user.category!=='duty');
const hasPersonalDuty=computed(()=>!!inspectionTask.value||['stale','down'].includes(props.inspectionState||''));
const canReadTeam=computed(()=>canReadLearning.value&&!props.user.impersonation&&(canSeeCampus.value||props.role==='supervisor')
  &&/^(?:ALL|CAMPUS|[A-EH])$/.test(props.scope));
const learningIdentity=computed(()=>JSON.stringify([props.user.open_id,props.user.impersonation?.id,props.user.modules,props.scopes]));
const range=computed(()=>{
  const at=Date.parse(today.value+'T12:00:00+08:00');
  return period.value==='day'?eventPeriodRange('custom','',at,[today.value,today.value]):eventPeriodRange(period.value,'',at);
});
const labels:Partial<Record<WorkItem['kind'],string>>={
  notices:'通告',events:'事件',event_notices:'事件',repair_management:'维修跟进',engineer_mop:'维保单',critical_guard:'重保',
  risk_inspection:'专项排查',water:'水耗',drills:'演练',inspection:'巡检',cabinet:'上下电',learning:'画像学练',accounts:'权限审批',tool_management:'工具自检',tool_external:'工具送检',pre_shift:'班前检查',
};
const label=(item:WorkItem)=>item.kind==='notices'?noticeTileLabel(item):labels[item.kind]||'待办';
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
    const due=urgency(item);
    return (due===1||due===2)&&end!==undefined&&end>=range.value.start&&end<range.value.end;
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
    const when=item.kind==='tool_external'||dateOnly(item.noticeWindow?.end)?dateFormat.format(due):timeText(due);
    return ['已逾期','今日截止','明日截止','截止'][urgency(item)]+' '+when;
  }
  const raw=item.noticeWindow?.start||item.scheduledAt||item.time, start=startAt(item);
  return start===undefined?'':dateOnly(raw)?dateFormat.format(start):timeText(start);
}
const highLevel=new Set(['events','event_notices','critical_guard','risk_inspection']);
const personalKinds=new Set(['learning','signatures','inspection','pre_shift']);
const isPersonal=(row:WorkItem)=>personalKinds.has(row.kind)||[props.user.open_id,props.user.union_id]
  .some(id=>typeof id==='string'&&!!id&&row.actorIds?.includes(id));
const inspectionTask=computed(()=>props.tasks.find(row=>row.kind==='inspection'));
const active=computed(()=>props.tasks.filter(matchesPeriod).sort(sortRows));
const campusFocus=(row:WorkItem)=>highLevel.has(row.kind)&&!isPersonal(row);
const campusRows=computed(()=>active.value.filter(campusFocus));
const buildingRows=computed(()=>active.value.filter(row=>(!canSeePersonal.value||!isPersonal(row))&&(!canSeeCampus.value||!campusFocus(row))));
const personalRows=computed(()=>active.value.filter(row=>isPersonal(row)&&row.kind!=='inspection'
  &&!(row.kind==='learning'&&(props.user.category==='duty'||hasPersonalLearning.value))));
const hasPersonalLearning=computed(()=>Boolean(personal.value?.total));
const hasQuestionReviews=computed(()=>reviewQueue.value.total>0);
const hasTeamSummary=computed(()=>Boolean(team.value?.total||team.value?.checking||team.value?.upcoming||teamError.value));
const cards=computed(()=>[
  ...(canSeeCampus.value?[{id:'campus',title:'园区',rows:campusRows.value}]:[]),
  ...(canSeeBuilding.value?[{id:'building',title:'楼栋',rows:buildingRows.value}]:[]),
  ...(canSeePersonal.value?[{id:'personal',title:'个人',rows:personalRows.value}]:[]),
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
  teamSummary:filter.value==='all'&&canReadTeam.value&&card.id===sealCard.value&&hasTeamSummary.value,
})).filter(card=>card.rows.length||card.seals.length||card.teamSummary||
  (filter.value==='all'&&card.id==='personal'&&(hasPersonalLearning.value||hasQuestionReviews.value||hasPersonalDuty.value))));
const businessPriority:Record<string,number>={
  seal:5,repair_management:6,
  pre_shift:7,inspection:8,cabinet:9,engineer_mop:10,water:11,drills:12,
  question:13,learning:14,'my-learning':14,team:15,planned:90,
};
const periodModules=computed(()=>visibleCards.value.flatMap(card=>{
  const groups=new Map<string,WorkItem[]>();
  for(const item of card.rows){
    const kind=planned(item)?'planned':taskTileKey(item);
    if(!groups.has(kind))groups.set(kind,[]);
    groups.get(kind)!.push(item);
  }
  const group=(kind:string,title:string,rows:WorkItem[]=[],seals:CalendarItem[]=[])=>{
    const notice=kind.startsWith('notices:');
    const [icon,hue]=taskTileStyle(['team','my-learning','question'].includes(kind)?'learning':kind);
    return {key:card.id+'-'+kind,scope:card.id,scopeTitle:card.title,kind,title,rows,seals,notice,icon,hue,
      priority:taskTilePriority[kind]??businessPriority[kind]??(notice?4:20)};
  };
  const modules=[...groups].map(([kind,rows])=>group(kind,kind==='planned'?'未开始通告':label(rows[0]),rows));
  if(card.seals.length)modules.push(group('seal','封网',[],card.seals));
  if(card.teamSummary)modules.push(group('team','画像学练情况汇总'));
  if(filter.value==='all'&&card.id==='personal'){
    if(hasQuestionReviews.value)modules.push(group('question','待处理题目'));
    if(learningEntry.value)modules.push(group('my-learning','我的画像学练'));
    if(hasPersonalDuty.value)modules.push(group('inspection','日常巡检'));
  }
  return modules;
}).sort((a,b)=>a.priority-b.priority));
const incomplete=computed(()=>props.partial||['loading','stale','down'].includes(props.planState||''));
const rowMeta=(item:WorkItem)=>[
  (item.referencedScopes?.length?[...new Set(item.referencedScopes)].sort().map(scopeName).join('、'):scopeName(item.scope)),
  item.urgentEvent,
  item.status,
].filter(Boolean).join(' · ');
const expanded=ref<Partial<Record<string,boolean>>>({});
const plannedOpen=ref(false);
watch([period,filter],()=>{expanded.value={};plannedOpen.value=filter.value==='planned';});
watch(()=>props.scope,()=>{filter.value='all';expanded.value={};plannedOpen.value=false;});
const issuerScopes=computed(()=>new Set(props.scopes.map(item=>item.value)));
type Count = {done:number;total:number;pending:number;late:number;checking:number;upcoming:number};
const team=ref<Count>(), personal=ref<{done:number;total:number;scope:string;canAnswer:boolean}>();
const learningEntry=computed<WorkItem|undefined>(()=>{
  const progress=personal.value;
  if(!progress?.total)return;
  return {key:'learning:personal',kind:'learning',module:'learning',title:'我的画像学练',
    scope:progress.scope,status:`已答 ${progress.done} / ${progress.total} 题`,time:today.value,
    readOnly:!progress.canAnswer,url:'/learning?'+new URLSearchParams({view:'practice',tab:'today',scope:progress.scope})};
});
const reviewQueue=ref<Dict>({}), reviewOpen=ref(false), reviewPage=ref(1), reviewRefresh=ref(0);
const reviewDayPart=computed(()=>Math.floor(props.clock/90000));
const canReviewQuestions=computed(()=>canReadPersonalLearning.value&&!props.user.impersonation);
watch([canReviewQuestions,reviewPage,reviewRefresh,reviewDayPart,learningIdentity],async([permitted,page],_old,onCleanup)=>{
  if(!permitted){reviewQueue.value={};reviewOpen.value=false;return;}
  const request=new AbortController();
  onCleanup(()=>request.abort());
  try {
    const data=await requestLearning({signal:request.signal},'/issues/workbench-questions?page='+page);
    if(request.signal.aborted)return;
    if(!Array.isArray(data.items)||!Number.isInteger(data.total)||data.total<0)return;
    if(page>1&&!data.items.length&&data.total>0){reviewPage.value=page-1;return;}
    reviewQueue.value=data;
  } catch(error) {
    if(!request.signal.aborted&&[401,403].includes((error as {status?:number})?.status||0)){
      reviewQueue.value={};reviewOpen.value=false;
    }
  }
},{immediate:true});
type LearningProgress={scope:string;name:string;shift:string;status:string;answered:number;assigned:number};
const teamOpen=ref(false), teamDetail=ref<LearningProgress[]>([]), detailBuildings=ref<string[]>([]);
const detailScope=ref(''), detailLoading=ref(false), detailError=ref(''), detailRevision=ref(0);
const detailDate=ref(''),detailFilter=ref('all');
const dateStart=computed(()=>beijingDate(range.value.start));
const unfinished=(row:LearningProgress)=>['待答','答题中','逾期'].includes(row.status);
function openTeam(){
  detailDate.value=today.value;detailScope.value='';detailFilter.value='all';teamOpen.value=true;
}
watch([teamOpen,detailDate,detailRevision,learningIdentity],async([open,date],_old,onCleanup)=>{
  if(!open || !canReadTeam.value || !date){detailLoading.value=false;detailError.value='';teamDetail.value=[];return;}
  const request=new AbortController();
  onCleanup(()=>request.abort());
  detailLoading.value=true;detailError.value='';teamDetail.value=[];
  try {
    const query=new URLSearchParams({scope:'ALL',date,people:'1'});
    const data=await readWorkbenchRequest('/api/learning/workbench-summary?'+query,request.signal);
    if(request.signal.aborted)return;
    if(data.from!==date||data.to!==date||!validTeamScopes(data.scopes)||!Array.isArray(data.people)||
      data.people.some((row:LearningProgress)=>!row||!data.scopes.includes(row.scope)||typeof row.name!=='string'||
        typeof row.status!=='string'||!Number.isSafeInteger(row.answered)||!Number.isSafeInteger(row.assigned)||row.answered<0||row.assigned<0))
      throw new Error('学练明细楼栋未核验');
    detailBuildings.value=data.scopes;
    if(!data.scopes.includes(detailScope.value))detailScope.value='';
    teamDetail.value=data.people;
  } catch(error) {
    if(!request.signal.aborted)detailError.value=[401,403].includes((error as {status?:number})?.status||0)
      ?'当前账号无权查看学练明细':'学练明细暂未读到';
  } finally {if(!request.signal.aborted)detailLoading.value=false;}
});
watch([()=>props.scope,canReadTeam,period,today,learningIdentity],()=>{teamOpen.value=false;teamDetail.value=[];detailBuildings.value=[];},{flush:'sync'});
const detailRows=computed(()=>teamDetail.value.filter(row=>(!detailScope.value||row.scope===detailScope.value)
  && (detailFilter.value==='all'||unfinished(row)&&(detailFilter.value!=='unanswered'||row.answered===0)))
  .sort((a,b)=>a.scope.localeCompare(b.scope)||Number(unfinished(b))-Number(unfinished(a))||a.answered-b.answered||a.name.localeCompare(b.name,'zh-CN')));
const teamError=ref(''), revision=ref(0);
function validTeamScopes(value:unknown):value is string[]{
  return Array.isArray(value)&&value.length>0&&value.every(scope=>issuerScopes.value.has(scope));
}
function count(value:unknown){
  if(typeof value!=='number'||!Number.isFinite(value)||value<0)throw new Error('完成数据待核验');
  return value;
}
watch([()=>props.scope,period,today,canReadTeam,canReadPersonalLearning,canSeePersonal,revision,learningIdentity,()=>props.learningRevision],async(_current,_previous,onCleanup)=>{
  const request=new AbortController();
  onCleanup(()=>request.abort());
  team.value=undefined;personal.value=undefined;teamError.value='';
  const from=beijingDate(range.value.start),to=today.value,scope=props.scope,signal=request.signal;
  const read=(path:string,params:Record<string,string>)=>readWorkbenchRequest(path+'?'+new URLSearchParams(params),signal);
  const requests:Promise<unknown>[]=[];
  if(canReadTeam.value)requests.push(read('/api/learning/workbench-summary',{scope:'ALL',from,to}).then(data=>{
    if(signal.aborted)return;
    if(!validTeamScopes(data.scopes))throw new Error('学练汇总楼栋未核验');
    const done=count(data.summary?.['已完成']),late=count(data.summary?.['逾期']);
    const pending=count(data.summary?.['待答'])+count(data.summary?.['答题中'])+late;
    team.value={done,total:done+pending,pending,late,checking:count(data.summary?.['待核对']),upcoming:count(data.summary?.['未到时'])};
  }).catch(cause=>{if(!signal.aborted)teamError.value=[401,403].includes(cause?.status)?'当前账号无权查看学练汇总':'学练汇总读取失败';}));
  if(canReadPersonalLearning.value&&canSeePersonal.value)requests.push(read('/api/learning/bootstrap',{}).then(async boot=>{
    if(signal.aborted)return;
    if(!boot.self_person?.id||!issuerScopes.value.has(boot.self_scope)||!['ALL','CAMPUS',boot.self_scope].includes(scope))return;
    if(period.value==='day'&&boot.daily_exemption?.status==='exempt')return;
    const data=await read('/api/learning/profile',{scope:boot.self_scope||'',person_id:boot.self_person.id,from,to});
    if(signal.aborted)return;
    const summary=period.value==='day'?data.today_summary:data.summary;
    const done=count(summary?.task_answered),total=count(summary?.assigned);
    personal.value={done,total,scope:boot.self_scope,canAnswer:boot.can_answer===true&&!props.user.impersonation};
  }));
  await Promise.allSettled(requests);
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
    <p v-if="!periodModules.length&&!incomplete" class="period-empty" role="status">
      {{ filter==='seal'&&sealStatus!=='ready'?'封网信息待更新':'本周期暂无此类事项' }}
    </p>
    <div v-if="periodModules.length" class="period-grid" :data-view="view">
      <article v-for="block in periodModules" :key="block.key" class="period-card workbench-module" :data-card="block.scope"
        :data-module="block.kind" :data-state="block.notice?'ongoing':undefined"
        :style="{'--tile-hue':block.hue}" :aria-label="block.scopeTitle+' · '+block.title">
        <header v-if="block.rows.length||block.seals.length" class="card-head">
          <component :is="block.icon" :size="21" aria-hidden="true"/>
          <h3 v-if="block.kind!=='planned'" :title="block.title">{{ block.title }}</h3>
          <button v-else type="button" class="period-fold" :aria-expanded="plannedOpen" :aria-controls="'period-'+block.key" @click="plannedOpen=!plannedOpen">
            {{ block.title }} <ChevronDown :size="14" aria-hidden="true"/>
          </button>
          <small class="scope-tag">{{ block.scopeTitle }}<template v-if="block.notice"> · 进行中</template></small>
          <span class="row-count">{{ block.rows.length+block.seals.length }}</span>
        </header>

        <section v-if="block.rows.length" v-show="block.kind!=='planned'||plannedOpen" :id="'period-'+block.key" class="period-group">
          <ul class="period-entries">
            <li v-for="item in (expanded[block.key]?block.rows:block.rows.slice(0,2))" :key="item.key">
              <button type="button" class="period-entry" :data-tone="urgency(item)===0?'late':urgency(item)<3?'soon':undefined"
                :aria-label="action(item)+' '+item.title" :title="[item.title, rowMeta(item), timing(item)].filter(Boolean).join('\n')" @click="$emit('task',item)">
                <strong class="period-title">{{ item.title }}</strong>
                <small class="period-meta">{{ rowMeta(item) }}</small>
                <small v-if="timing(item)" class="period-time">{{ timing(item) }}</small><ChevronRight :size="13" aria-hidden="true"/>
              </button>
            </li>
          </ul>
          <button v-if="block.rows.length>2" type="button" class="period-expand" :aria-expanded="!!expanded[block.key]" @click="expanded[block.key]=!expanded[block.key]">
            {{ expanded[block.key]?'收起':`展开其余 ${block.rows.length-2} 项` }}
          </button>
        </section>

        <div v-else-if="block.seals.length" class="seal-list" aria-label="周期内封网">
          <details v-for="item in block.seals" :key="item.title+'-'+item.start">
          <summary class="seal-row">
            <strong class="period-title" :title="item.title">{{ item.title }}</strong>
            <small class="seal-state">{{ todoTimestamp(item.start)!<=clock?'封网中':'待生效' }}</small>
          </summary>
          <small class="seal-details">{{ timeText(todoTimestamp(item.start)!) }} 至 {{ timeText(todoTimestamp(item.end)!) }}</small>
          <small class="seal-details">{{ [item.source,...(item.scopes||[]).map(scopeName),...(item.notes||[])].filter(Boolean).join(' · ') || '适用范围 '+scopeLabel }}</small>
          </details>
        </div>

        <div v-else-if="block.kind==='team'" class="learning-summary">
          <button type="button" class="learning-open" aria-haspopup="dialog" @click="openTeam"><BookOpenCheck :size="16" aria-hidden="true"/><span>{{ block.title }}</span><small class="scope-tag" aria-hidden="true">{{ block.scopeTitle }}</small><ChevronRight :size="15" aria-hidden="true"/></button>
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

        <div v-else-if="block.kind==='question'" class="learning-summary">
          <button type="button" class="learning-open review-open" aria-haspopup="dialog" @click="reviewOpen=true">
            <Pencil :size="15" aria-hidden="true"/><span>待处理题目</span><small>{{ reviewQueue.total }} 题</small><ChevronRight :size="15" aria-hidden="true"/>
          </button>
        </div>
        <div v-else-if="block.kind==='my-learning'&&personal&&learningEntry" class="learning-summary">
          <button type="button" class="learning-open" aria-label="打开画像学练" aria-haspopup="dialog" @click="$emit('task',learningEntry)">
            <BookOpenCheck :size="16" aria-hidden="true"/><span>{{ block.title }}</span><small class="scope-tag" aria-hidden="true">{{ block.scopeTitle }}</small><ChevronRight :size="15" aria-hidden="true"/>
          </button>
          <div class="learning-kpis"><span>已答 <b>{{ personal.done }} / {{ personal.total }}</b> 题</span>
            <span>{{ personal.total>Number(personal.done)?'待完成':'已完成' }}</span></div>
          <meter :value="Math.min(personal.done||0,personal.total)" :max="personal.total" aria-label="当班题单进度"/>
        </div>

        <div v-else-if="block.kind==='inspection'" class="personal-duty">
          <button v-if="inspectionTask" type="button" class="personal-duty-row" @click="$emit('task',inspectionTask)">
            <ListChecks :size="16" aria-hidden="true"/><strong class="period-title" :title="inspectionTask.title">{{ inspectionTask.title }}</strong><small class="scope-tag" aria-hidden="true">个人</small><ChevronRight :size="13" aria-hidden="true"/>
          </button>
          <div v-else-if="inspectionState&&inspectionState!=='deny'" class="personal-duty-row" role="status">
            <ListChecks :size="16" aria-hidden="true"/><strong>日常巡检</strong>
            <small>{{ inspectionState==='loading'?'读取中…':inspectionState==='ok'?'暂无待巡检':'待核对' }}</small>
            <button v-if="inspectionState==='stale'||inspectionState==='down'" type="button" @click="$emit('retryInspection')">重试</button>
          </div>
        </div>

      </article>
    </div>
    <NModal v-model:show="teamOpen" preset="card" title="画像学练情况汇总" style="width:min(700px,calc(100vw - 20px))">
      <div class="learning-detail" :aria-busy="detailLoading">
        <header>
          <input v-model="detailDate" type="date" :min="dateStart" :max="today" aria-label="学练明细日期" />
          <select v-model="detailScope" data-native-select aria-label="学练明细楼栋">
            <option value="">全部授权楼栋</option>
            <option v-for="building in detailBuildings" :key="building" :value="building">{{ scopeName(building) }}</option>
          </select>
          <select v-model="detailFilter" data-native-select aria-label="学练明细状态">
            <option value="all">全部状态</option><option value="unfinished">未完成</option><option value="unanswered">未作答</option>
          </select>
        </header>
        <p v-if="detailLoading" role="status">正在读取学练明细…</p>
        <p v-else-if="detailError" role="alert">{{ detailError }} <button type="button" @click="detailRevision++">重试</button></p>
        <div v-else-if="detailRows.length" class="learning-roster-table">
          <table aria-label="逐人答题明细">
            <thead><tr><th>楼栋</th><th>姓名</th><th>班次</th><th>已答 / 应答</th><th>状态</th></tr></thead>
            <tbody><tr v-for="(row,index) in detailRows" :key="`${row.scope}:${row.name}:${index}`">
              <td>{{ scopeName(row.scope) }}</td><td>{{ row.name }}</td><td>{{ row.shift||'—' }}</td>
              <td>{{ row.answered }} / {{ row.assigned }}</td>
              <td><span class="learning-row-status" :data-state="row.status" :data-unanswered="unfinished(row)&&row.answered===0">
                {{ unfinished(row)&&row.answered===0&&row.status!=='逾期'?'未作答':row.status }}
              </span></td>
            </tr></tbody>
          </table>
        </div>
        <p v-else>{{ detailDate?'当天暂无符合条件的记录':'请选择日期' }}</p>
      </div>
    </NModal>
  </section>
  <WorkbenchQuestionReview v-if="reviewOpen" :show="reviewOpen" :queue="reviewQueue" @close="reviewOpen=false"
    @refresh="page=>reviewPage=page" @saved="reviewRefresh++" />
</template>

<style scoped>
.wb-period{display:grid;gap:8px;min-width:0}
.period-tools,.period-switch,.card-head,.learning-kpis{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.period-tools h2{margin-inline-end:auto}
.period-switch button,.status-filters button,.period-expand,.period-muted button,.learning-summary small button,.learning-detail button{
  border:1px solid var(--desk-line);background:var(--desk-paper);color:var(--desk-ink);
  border-radius:8px;min-height:34px;padding:4px 10px;cursor:pointer;font:inherit;font-size:.78rem}
.period-switch button:hover,.status-filters button:hover,.period-expand:hover,.period-fold:hover{border-color:var(--desk-green)}
.period-switch button[aria-pressed=true],.status-filters button[aria-pressed=true]{
  background:var(--desk-green-wash);border-color:var(--desk-green);color:var(--desk-green);font-weight:700}
.status-filters{display:flex;gap:5px;flex-wrap:wrap}
.status-filters button,.row-count{font-variant-numeric:tabular-nums;white-space:nowrap}
.status-filters button b{margin-inline-start:3px;font-size:.82rem}
.status-filters button[data-filter=late][data-has-items=true]{color:var(--cf-danger,#a43c31)}
.status-filters button[data-filter=soon][data-has-items=true]{color:var(--cf-warning,#8a4d11)}
.period-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,248px),1fr));gap:8px;align-items:start}
.period-grid[data-view=table]{grid-template-columns:minmax(0,1fr)}
.period-card{min-width:0;display:grid;gap:6px;padding:9px;border:1px solid color-mix(in srgb,var(--tile-accent) 26%,var(--desk-line));border-radius:10px;
  background:var(--tile-tint);box-shadow:var(--soft-raised);container:period-card / inline-size}
.period-fold{display:flex;align-items:center;gap:5px;min-height:24px;padding:0;border:0;
  background:transparent;color:inherit;text-align:start;cursor:pointer;font:inherit;font-weight:650}
.period-fold[aria-expanded=false] svg{transform:rotate(-90deg)}
.period-tools h2{font-size:var(--cf-font-section)}
.card-head{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto;gap:6px;min-height:30px;font-size:var(--cf-font-section);color:var(--tile-accent)}
.card-head h3{margin:0;font-size:inherit;font-weight:650}
.period-muted,.learning-summary small{color:var(--desk-muted);font-size:var(--cf-font-meta)}
.scope-tag{display:inline-flex;align-items:center;padding:2px 5px;border-radius:4px;background:var(--desk-paper);
  color:var(--desk-muted);font-size:var(--cf-font-meta);line-height:1.35;white-space:nowrap}
.row-count{min-width:1em;text-align:end;font-size:20px;font-weight:700}
.learning-summary{min-width:0;display:grid;gap:4px;font-size:var(--cf-font-body)}
.learning-summary meter{width:100%;height:9px;accent-color:var(--desk-green)}
.learning-summary meter::-webkit-meter-optimum-value{background:var(--desk-green)}
.learning-kpis{color:var(--desk-muted);font-size:.76rem}
.learning-kpis b{font-variant-numeric:tabular-nums;color:var(--desk-green)}
.learning-kpis [data-warn=true],.learning-kpis [data-warn=true] b{color:var(--cf-warning,#8a4d11)}
.learning-summary .learning-open{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto;align-items:center;gap:6px;width:100%;min-height:27px;
  border:0;border-radius:6px;background:transparent;color:var(--tile-accent);padding:0;text-align:start;font:inherit;font-weight:650}
.learning-summary .learning-open:hover{background:var(--desk-paper)}
.seal-row{cursor:pointer}
.seal-details{display:block;padding:2px 8px 8px;overflow-wrap:anywhere}
.learning-detail{font-size:13px;min-width:0}
.learning-detail header{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.learning-detail :is(select,input){min-height:36px;min-width:0;max-width:100%;background:var(--soft-field);color:inherit;
  border:1px solid var(--soft-edge);border-radius:7px;padding:4px 7px;font:inherit}
.learning-roster-table{max-height:55dvh;overflow:auto;overscroll-behavior:contain}
.learning-detail table{width:100%;border-collapse:collapse;text-align:center;font-variant-numeric:tabular-nums}
.learning-detail :is(th,td){padding:7px 3px;border-bottom:1px solid var(--soft-edge);overflow-wrap:anywhere}
.learning-detail :is(th,td):nth-child(2){text-align:start}
.learning-detail th{position:sticky;top:0;background:var(--soft-face,#fff);color:var(--v2-ink)}
.learning-row-status[data-state="已完成"]{color:var(--v2-green,var(--v2-palette-accent-text))}
.learning-row-status:is([data-unanswered=true],[data-state="逾期"]){color:var(--cf-danger,#a43c31);font-weight:650}
@media(pointer:coarse){.learning-detail :is(select,input){min-height:44px;font-size:16px}}
@media(max-width:600px){.learning-summary .learning-open{min-height:44px}}
.personal-duty{display:grid;gap:4px}
.personal-duty-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto;align-items:center;gap:6px;text-align:start;
  min-width:0;min-height:27px;padding:0;border:0;border-radius:6px;background:transparent;color:var(--desk-ink);font:inherit;font-size:var(--cf-font-body)}
button.personal-duty-row{cursor:pointer}
.personal-duty-row :is(svg,small){color:var(--desk-green)}
.personal-duty-row button{color:var(--desk-green)}
.period-entries,.seal-list{display:grid;gap:2px;list-style:none;margin:0;padding:0;min-width:0;
  max-block-size:14rem;overflow:auto;overscroll-behavior-y:contain;scroll-padding:4px}
.period-group{display:grid;gap:4px;min-width:0}
.period-entry,.seal-row{width:100%;min-width:0;display:grid;gap:3px 7px;align-items:center;text-align:left;
  border:0;border-radius:6px;background:var(--desk-paper);padding:5px 6px;min-height:32px;color:var(--desk-ink);font:inherit;font-size:var(--cf-font-body)}
.period-entry{grid-template-columns:minmax(0,1fr) auto auto;cursor:pointer}
.period-entry:hover,.period-entry:focus-visible{background:var(--desk-green-wash)}
.period-entry:focus-visible{outline-offset:-2px}
.period-title,.card-head h3,.learning-open span,.period-meta,.period-time{
  min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.period-title{font-weight:550}
.period-meta,.period-time,.seal-row small{font-size:var(--cf-font-meta);color:var(--desk-muted);line-height:1.35}
.period-meta{max-width:6.5em}
.period-time{display:none}
.period-entry svg{grid-column:-2;color:var(--tile-accent)}
.period-entry[data-tone=late]{border-inline-start:3px solid var(--cf-danger,#a43c31)}
.period-entry[data-tone=soon]{border-inline-start:3px solid var(--cf-warning,#8a4d11)}
.period-entry[data-tone=late] .period-time{color:var(--cf-danger,#a43c31);font-weight:700}
.period-entry[data-tone=soon] .period-time{color:var(--cf-warning,#8a4d11);font-weight:700}
.period-expand{justify-self:end;min-height:27px;padding:2px 6px;border:0;color:var(--v2-blue)}
.seal-row{grid-template-columns:minmax(0,1fr) auto}
.seal-row .seal-state{color:var(--v2-blue);white-space:nowrap}
.period-empty{padding:10px 12px;border:1px solid var(--desk-line);border-radius:8px;color:var(--desk-muted);font-size:.8rem}
@container period-card (min-width:400px){
  .period-entry{grid-template-columns:minmax(0,1fr) auto auto auto}
  .period-time{display:block;max-width:10em}
}
@container period-card (min-width:560px){
  .period-grid[data-view=table] .period-entry{grid-template-columns:minmax(0,1fr) minmax(0,.38fr) minmax(0,.3fr) auto}
  .period-grid[data-view=table] .period-meta{display:block}
}
@media(pointer:coarse){.period-tools button,.status-filters button,.period-entry,.period-expand,.period-fold,.personal-duty-row{min-height:44px}}
</style>
