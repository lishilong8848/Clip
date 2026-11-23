<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onActivated, onBeforeUnmount, onDeactivated, onMounted, ref, watch, type VNode } from 'vue';
import { NModal, NPopover } from 'naive-ui';
import { Activity, ArrowLeftRight, Bell, BookOpen, ChevronRight, CircleAlert, ClipboardList, Droplets, FileCheck2, KeyRound, Layers3, ListChecks, Package, PenLine, Route, ScanLine, Search, ShieldCheck, Siren, SlidersHorizontal, Wrench, X, Zap } from 'lucide-vue-next';
import type { Dict } from '../client';
import { canReviewHandover, moduleForRoute, moduleIcons, moduleUrl, NOTICE_IDS } from '../modules';
import { widget, type Widget } from '../dashboard/layout';
import { preShiftHomeAccount, preShiftHomeVisible, preShiftSummary } from '../../preShiftUtils';
import { requestEmbeddedChange, runNavigationAction } from '../navigation';
import PreShiftCheck from '../dashboard/PreShiftCheck.vue';
import PeriodOverview from './PeriodOverview.vue';
import WorkbenchDailyLayout from './WorkbenchDailyLayout.vue';
import WorkbenchTaskRows from './WorkbenchTaskRows.vue';
import RecordViewSwitch from '../../components/RecordViewSwitch.vue';
import AsyncPageState from '../../components/AsyncPageState.vue';
import { recipes, workbenchModules, workbenchRole, workbenchScopes, type ScopeOption } from './recipes';
import { scopedUrl, workbenchPlanSource, workbenchSources, type SourceId, type WorkItem } from './sources';
import { readWorkbenchRequest, useReadOnlyLayout, useWorkbenchReads } from './useWorkbenchReads';
import { deskActionOrder, deskRole, deskSources, deskTaskVisible, inDeskScope } from './deskPolicy';
import { todoTimestamp } from '../businessTodos';
import { eventLevelTone } from '../../eventManagementUtils';
import { beijingDate, eventPeriodRange } from '../eventPeriod';

const props=withDefaults(defineProps<{user:Dict;scopes:ScopeOption[];scope:string;explicitScope?:boolean;searchLinks?:{label:string;url:string}[]}>(),{explicitScope:true});
const emit=defineEmits<{open:[url:string]}>();
const content=ref<HTMLElement>();
const clock=ref(Date.now());
const authorizedScopes=computed(()=>workbenchScopes(props.scopes));
const accessKey=computed(()=>JSON.stringify([props.user.open_id,props.user.union_id,props.user.level,props.user.role,
  props.user.category,props.user.specialties,props.user.home_scope,props.user.modules,props.user.scope_rules,props.user.impersonation,
  props.user.tool_management_enabled,props.user.tool_management_full_access,props.user.can_manage_accounts,props.user.enabled,props.scopes]));
const concreteScopes=computed(()=>authorizedScopes.value.filter(option=>/^[A-EH]$|^110$/.test(option.value)));
const ownScope=computed(()=>concreteScopes.value.find(option=>option.value===props.user.home_scope)?.value
  || (concreteScopes.value.length===1?concreteScopes.value[0]?.value:'' )||'');
const checkScope=computed(()=>preShiftHomeAccount(props.user)?ownScope.value:'');
const checkUser=computed(()=>props.user.home_scope===checkScope.value?props.user:{...props.user,home_scope:checkScope.value});
const shift=ref('');
const checkSnapshot=ref<Dict>();
const checkPanel=ref<InstanceType<typeof PreShiftCheck>>();
const accountLevel=computed(()=>deskRole(props.user));
const globalDesk=computed(()=>accountLevel.value==='L0'||accountLevel.value==='L1');
const role=computed(()=>globalDesk.value?'manager':accountLevel.value==='L2'?'engineer':accountLevel.value==='L3'?'supervisor':
  accountLevel.value==='duty'?'duty':workbenchRole(props.user,authorizedScopes.value,Boolean(shift.value)));
const recipe=computed(()=>recipes[role.value]);
const roleLabel=computed(()=>accountLevel.value
  ? {L0:'超级管理员',L1:'全楼管理员',L2:'工程师编辑',L3:'本楼主管',L4:'本楼基础账号',duty:'固定值班账号'}[accountLevel.value]
  : recipe.value.label);
const composite=(scope:string)=>['ALL','CAMPUS'].includes(scope);
const scopeOptions=computed(()=>{
  if (accountLevel.value==='L2') return concreteScopes.value;
  if (['L3','L4','duty'].includes(accountLevel.value||'')) return concreteScopes.value.filter(option=>option.value===ownScope.value);
  if (['supervisor','engineer'].includes(role.value)&&ownScope.value) return concreteScopes.value.filter(option=>option.value===ownScope.value);
  if (['manager','specialist'].includes(role.value)&&concreteScopes.value.length>1&&!authorizedScopes.value.some(option=>composite(option.value)))
    return [{value:'ALL',label:'授权楼栋汇总'},...authorizedScopes.value];
  return authorizedScopes.value;
});
const selectedScope=computed(()=>{
  const aggregate=scopeOptions.value.find(option=>composite(option.value));
  if (!props.explicitScope&&aggregate&&['manager','specialist'].includes(role.value)) return aggregate.value;
  if (!props.explicitScope&&accountLevel.value==='L2'&&ownScope.value) return ownScope.value;
  return (scopeOptions.value.find(option=>option.value===props.scope)||scopeOptions.value[0])?.value||'';
});
const dataScope=selectedScope;
const dutyOverview=computed(()=>!props.user.impersonation&&!!checkScope.value
  &&(dataScope.value===checkScope.value||composite(dataScope.value))&&preShiftHomeVisible(checkUser.value,checkSnapshot.value));
const checkStatus=computed(()=>{
  if(!dutyOverview.value)return '';
  if(!checkSnapshot.value)return '待核对';
  try{
    const {pending,completed}=preShiftSummary(checkSnapshot.value);
    return pending?pending+' 项待检查':completed?'已提交':'暂无待检查';
  }catch{return '待核对';}
});
function focusPreShift(){
  const section=checkPanel.value?.$el;
  if(section instanceof HTMLElement){
    section.scrollIntoView({block:'start'});
    section.focus({preventScroll:true});
  }
}
const readScopes=computed(()=>composite(dataScope.value)&&concreteScopes.value.length>0
  &&(dataScope.value==='CAMPUS'||!authorizedScopes.value.some(option=>option.value==='ALL'))
  ?concreteScopes.value.map(option=>option.value):undefined);
const context=computed(()=>JSON.stringify([accessKey.value,role.value,dataScope.value]));
const lazy=(loader:()=>Promise<any>)=>defineAsyncComponent({loader,loadingComponent:AsyncPageState,errorComponent:AsyncPageState,delay:80,timeout:30000});
const NoticeWorkspace=lazy(()=>import('../NoticeWorkspace.vue'));
const EventNoticeBoard=lazy(()=>import('../../components/EventNoticeBoard.vue'));
const NoticeChangeConfirmations=lazy(()=>import('../NoticeChangeConfirmations.vue'));
const ModuleWidget=lazy(()=>import('../ModuleWidget.vue'));
type OpenCase={item:WorkItem;mode:'read'|'module'|'notice'|'event'|'change';scope:string;url:string;focusId?:string;widget?:Widget};
const activeCase=ref<OpenCase>();
const casePanel=ref<HTMLElement>();
const caseBody=ref<HTMLElement>();
const caseEditor=ref<{guardNavigation:(proceed:()=>void)=>boolean}>();
const caseListOpen=ref(false),caseSearch=ref('');

const {layout}=useReadOnlyLayout(()=>props.user,accessKey);
const widgets=computed<Dict[]>(()=>{
  const value=layout.value as Dict|null;
  return value?.version===1&&Array.isArray(value.widgets)
    ?value.widgets.slice(0,16).filter(row=>row&&typeof row==='object').sort((a,b)=>(Number(a.y)||0)-(Number(b.y)||0)||(Number(a.x)||0)-(Number(b.x)||0)):[];
});
const preferredTasks=computed(()=>widgets.value.find(item=>item.kind==='tasks')?.taskKinds);
const preferredLinks=computed(()=>widgets.value.flatMap(item=>{
  if (item.kind==='shortcuts'&&Array.isArray(item.links)) return item.links;
  if (['module','repair_actions','notice_actions'].includes(item.kind)) return [item.module];
  return ({notices:['notices'],events:['event'],learning:['learning'],water:['water']} as Record<string,string[]>)[item.kind]||[];
}).map(id=>NOTICE_IDS.includes(id)?'notices':id));
const modules=computed(()=>workbenchModules(props.user,scopeOptions.value).filter(entry=>
  entry.id!=='handover' && (!recipe.value.pending||recipe.value.actions.includes(entry.id))
  && !(entry.id==='learning'&&props.user.category==='duty')
  && !(entry.id==='daily_inspection'&&!['ALL','CAMPUS','D','E'].includes(selectedScope.value))
  && !(['cabinet','critical_guard','drills'].includes(entry.id)&&['H','110'].includes(selectedScope.value))));
const shortcuts=computed(()=>{
  const entries=new Map(modules.value.map(entry=>[entry.id,entry]));
  return [...new Set([...preferredLinks.value,...recipe.value.actions,...entries.keys()])].flatMap(id=>{
    const entry=entries.get(id);
    if (!entry) return [];
    const scope=entry.id==='daily_inspection'&&selectedScope.value==='CAMPUS'
      ?concreteScopes.value.find(option=>['D','E'].includes(option.value))?.value||''
      :composite(selectedScope.value)?readScopes.value?.[0]||selectedScope.value:selectedScope.value;
    return [{...entry,url:moduleUrl(entry,scope)}];
  });
});
function switchScope(event:Event){
  const select=event.target as HTMLSelectElement, scope=select.value;
  select.value=selectedScope.value;
  if (scope!==selectedScope.value&&scopeOptions.value.some(option=>option.value===scope))
    emit('open',composite(scope)&&!authorizedScopes.value.some(option=>option.value===scope)?'/':scopedUrl('/',{scope}));
}

const pendingSources=computed(()=>accountLevel.value
  ?deskSources[accountLevel.value==='L4'&&role.value==='duty'?'duty':accountLevel.value]:recipe.value.pending);
const sourceContext=computed(()=>({user:props.user,scope:dataScope.value,scopes:authorizedScopes.value,
  selectedScopes:readScopes.value,role:role.value,pending:pendingSources.value,accessLevel:accountLevel.value,
  inspectionOnDuty:dutyOverview.value&&!!shift.value}));
const planMonths=computed(()=>[...new Set([beijingDate(clock.value).slice(0,7),
  ...eventPeriodRange('week','',clock.value).months,beijingDate(clock.value+86400000).slice(0,7)])]
  .map(month=>Number(month.slice(5))+'月'));
const sources=computed(()=>[
  ...workbenchSources(sourceContext.value,readWorkbenchRequest),
  ...workbenchPlanSource(sourceContext.value,planMonths.value,readWorkbenchRequest),
]);
const {states,load,refresh,refreshExpired}=useWorkbenchReads(context,sources);
watch(()=>planMonths.value.join(','),()=>refresh(['notice_plans']));
const allShortcuts=ref(false);
const sourceOrder=computed(()=>[...new Set<SourceId>([
  ...(pendingSources.value||[]),...(Array.isArray(preferredTasks.value)?preferredTasks.value:[]),...sources.value.map(source=>source.id),
])].sort((a,b)=>Number(a==='drills')-Number(b==='drills')));
const orderedSources=computed(()=>sourceOrder.value.flatMap(id=>sources.value.filter(source=>source.id===id)));
const sourceLabels=computed(()=>new Map(sources.value.map(source=>[source.id,source.label])));
watch([context,orderedSources],()=>{for(const source of orderedSources.value) void load(source.id);},{immediate:true,flush:'post'});
type TaskView='all'|'today'|'previous-repairs'|'unread';
const taskSearch=ref(''),taskView=ref<TaskView>('all');
const allTasks=computed(()=>orderedSources.value
  .filter(source=>source.id!=='notice_plans')
  .flatMap(source=>(states.value[source.id]?.result?.rows||[]).map(item=>source.readOnly?{...item,readOnly:true}:item))
  .filter(item=>(accountLevel.value!=='L2'||inDeskScope(item,dataScope.value))
    &&deskTaskVisible(accountLevel.value,props.user,item,ownScope.value)));
const missingOwnership=computed(()=>['L2','L3','L4'].includes(accountLevel.value||'')&&orderedSources.value.some(source=>
  source.id==='engineer_mop' && states.value[source.id]?.result?.rows.some(item=>
    accountLevel.value==='L4' ? !item.actorIds?.length : (accountLevel.value==='L3'||!inDeskScope(item,ownScope.value))&&!item.ownerLevel)));
const readStorageKey=computed(()=>{
  const id=props.user.open_id||props.user.union_id;
  return id&&!props.user.impersonation?'clipflow:workbench-read:'+id+':'+dataScope.value:'';
});
const readKeys=ref(new Set<string>());
const readIdentity=(item:WorkItem)=>JSON.stringify([item.kind,item.scope,item.key,item.status,item.time,item.dueAt,item.noticeWindow?.start,item.noticeWindow?.end]);
const isRead=(item:WorkItem)=>readKeys.value.has(readIdentity(item));
const inQueueScope=(row:WorkItem)=>composite(selectedScope.value)||inDeskScope(row,selectedScope.value);
const queueTasks=computed(()=>allTasks.value.filter(inQueueScope));
const periodTasks=computed(()=>{
  const current=new Set(queueTasks.value.filter(item=>item.ongoing&&item.sourceKey).map(item=>item.sourceKey));
  return [...queueTasks.value,...(states.value.notice_plans?.result?.rows||[]).filter(item=>
    !current.has(item.sourceKey)&&inQueueScope(item)&&deskTaskVisible(accountLevel.value,props.user,item,ownScope.value))];
});
const unreadTasks=computed(()=>queueTasks.value.filter(item=>!isRead(item)));
watch(context,()=>{
  let keys:unknown;
  try { if(readStorageKey.value) keys=JSON.parse(localStorage.getItem(readStorageKey.value)||'[]'); } catch { /* Storage may be disabled. */ }
  readKeys.value=new Set(Array.isArray(keys)?keys.filter(key=>typeof key==='string'):[]);
},{immediate:true,flush:'sync'});
function markRead(item:WorkItem) {
  if(isRead(item))return;
  const keys=[...readKeys.value,readIdentity(item)].slice(-500);
  readKeys.value=new Set(keys);
  try { if(readStorageKey.value) localStorage.setItem(readStorageKey.value,JSON.stringify(keys)); } catch { /* Keep session state. */ }
}
const activeGroup=ref('');
const queueView=ref<'cards'|'table'>('cards');
const popoverView=ref<'cards'|'table'>('table');
const priorityView=ref<'cards'|'table'>('cards');
const groupPlacement=ref<'top-start'|'bottom-start'>('bottom-start');
const groupMaxHeight=ref<number>();
let tileTrigger: HTMLButtonElement | null=null;
const queueSources=computed(()=>orderedSources.value.filter(source=>source.id!=='notice_plans'&&states.value[source.id]?.status!=='deny'));
const sourcesForQueue=computed(()=>queueSources.value.filter(source=>
  taskView.value!=='previous-repairs'||source.id==='repair_management'));
const isOverdue=(row:WorkItem)=>!!row.dueAt&&row.dueAt<=clock.value;
const priorityExpanded=ref(false);
watch([context,selectedScope],()=>{priorityExpanded.value=false;});
const priorities=computed(()=>queueTasks.value.flatMap(item=>{
  if(item.urgentEvent)return [{item,rank:eventLevelTone(item.urgentEvent)==='critical'?-1:0,label:item.urgentEvent+'事件'}];
  if(isOverdue(item))return [{item,rank:1,label:'已逾期'}];
  if(item.dueAt && item.dueAt-clock.value<=2*3600000)return [{item,rank:2,label:'两小时内截止'}];
  return [];
}).sort((a,b)=>a.rank-b.rank||(a.item.dueAt??Infinity)-(b.item.dueAt??Infinity)));
const dayStart=computed(()=>{
  const date=new Date(clock.value+8*3600000).toISOString().slice(0,10);
  return Date.parse(date+'T00:00:00+08:00');
});
const startsAt=(row:WorkItem)=>todoTimestamp(row.noticeWindow?.start||row.scheduledAt||row.time);
const onDeskDay=(time:number|undefined)=>time!==undefined&&time>=dayStart.value&&time<dayStart.value+86400000;
function isTodayTask(row:WorkItem) {
  if (row.kind==='notices'&&row.ongoing) {
    const actualStart=todoTimestamp(row.time)||todoTimestamp(row.noticeWindow?.start);
    return actualStart===undefined||actualStart<dayStart.value+86400000;
  }
  const start=startsAt(row), end=todoTimestamp(row.noticeWindow?.end);
  return onDeskDay(start)||onDeskDay(row.dueAt)||(start!==undefined&&start<dayStart.value&&end!==undefined&&end>dayStart.value);
}
function isPreviousRepair(row:WorkItem) {
  const start=startsAt(row);
  return row.kind==='repair_management'&&!isTodayTask(row)&&start!==undefined&&start<dayStart.value;
}
const hasPreviousRepairs=computed(()=>queueTasks.value.some(isPreviousRepair)||states.value.repair_management?.result?.more);
const taskScopeLabel=(scope:string)=>/^[A-EH]$/.test(scope)?scope+'楼':scope==='110'?'110站':scope==='ALL'||scope==='CAMPUS'?'园区':scope;
const taskModuleLabels:Partial<Record<SourceId,string>>={
  notices:'通告',events:'事件',event_notices:'事件',repair_management:'维修',critical_guard:'重保',risk_inspection:'专项排查',
  water:'水耗',engineer_mop:'维保单',cabinet:'机柜',drills:'演练',accounts:'权限',ali_confirmation:'阿里确认',
  tool_management:'工具',inspection:'巡检',learning:'学练',signatures:'签名',
};
const taskModuleLabel=(kind:SourceId)=>taskModuleLabels[kind]||sourceLabels.value.get(kind);
const taskLabel=(item:WorkItem)=>item.kind==='notices'
  ? item.detail==='维保'?'维保通告':item.detail||'通告'
  :taskModuleLabel(item.kind)||'待办';
const matchedTasks=computed(()=>{
  const terms=taskSearch.value.toLowerCase().split(/\s+/).filter(Boolean);
  return queueTasks.value
    .filter(row=>(taskView.value!=='today'||isTodayTask(row))&&(taskView.value!=='previous-repairs'||isPreviousRepair(row))
      &&(taskView.value!=='unread'||!isRead(row))&&terms.every(term=>
      [row.title,taskScopeLabel(row.scope),row.status,taskLabel(row),sourceLabels.value.get(row.kind)].join(' ').toLowerCase().includes(term)));
});
const groupPriority:Record<string,number>={events:0,critical_guard:1,'notices:变更':2,risk_inspection:3};
const tileStyles:Record<string,[typeof Layers3,number]>={
  events:[Siren,352],critical_guard:[ShieldCheck,32],'notices:变更':[ArrowLeftRight,218],risk_inspection:[ScanLine,278],
  'notices:维保':[Wrench,165],'notices:检修':[FileCheck2,186],'notices:轮巡':[Route,240],
  'notices:设备调整':[SlidersHorizontal,295],'notices:上下电':[Zap,46],
  engineer_mop:[ClipboardList,220],repair_management:[Wrench,172],cabinet:[Layers3,252],drills:[Activity,205],
  water:[Droplets,202],accounts:[KeyRound,38],tool_management:[Package,26],learning:[BookOpen,270],
  inspection:[ListChecks,157],signatures:[PenLine,219],ali_confirmation:[FileCheck2,219],
};
function rowPriority(item:WorkItem){
  if(item.urgentEvent)return eventLevelTone(item.urgentEvent)==='critical'?0:1;
  if(isOverdue(item))return 2;
  if(item.dueAt && item.dueAt-clock.value<=2*3600000)return 3;
  return isTodayTask(item)?4:5;
}
function compareRows(a:WorkItem,b:WorkItem){
  return rowPriority(a)-rowPriority(b)||Array.from(a.title).length-Array.from(b.title).length
    ||(a.dueAt??Infinity)-(b.dueAt??Infinity)||a.title.localeCompare(b.title,'zh-CN')||a.key.localeCompare(b.key);
}
function byBuilding(items:WorkItem[]){
  const groups=new Map<string,WorkItem[]>();
  for(const item of items){
    const codes=item.referencedScopes?.length?[...new Set(item.referencedScopes)].sort():[];
    const label=!composite(selectedScope.value)&&inDeskScope(item,selectedScope.value)
      ?taskScopeLabel(selectedScope.value):codes.length?codes.map(taskScopeLabel).join('、'):taskScopeLabel(item.scope)||'楼栋待核对';
    const rows=groups.get(label)||[];
    rows.push(item);
    groups.set(label,rows);
  }
  return [...groups].sort(([a],[b])=>a.localeCompare(b,'zh-CN',{numeric:true})).map(([building,rows])=>({building,rows}));
}
const switchableCases=computed(()=>{
  const query=caseSearch.value.trim().toLowerCase();
  return queueTasks.value.filter(item=>item.key!==activeCase.value?.item.key
    && [item.title,taskScopeLabel(item.scope),taskModuleLabel(item.kind),item.detail].join(' ').toLowerCase().includes(query)).sort(compareRows);
});
const taskGroups=computed(()=>{
  const groups=new Map<string,{key:string;label:string;source:SourceId;icon:typeof Layers3;hue:number;rows:WorkItem[]}>();
  function groupFor(key:string,label:string,source:SourceId) {
    const existing=groups.get(key);
    if(existing)return existing;
    const [icon,hue]=tileStyles[key]||[ClipboardList,205];
    const group={key,label,source,icon,hue,rows:[] as WorkItem[]};
    groups.set(key,group);
    return group;
  }
  for(const item of matchedTasks.value){
    const source=item.kind==='event_notices'?'events':item.kind;
    const label=taskLabel(item);
    const key=source==='notices'?'notices:'+(item.detail||label):source;
    groupFor(key,label,source).rows.push(item);
  }
  const represented=new Set([...groups.values()].map(group=>group.source));
  for(const source of sourcesForQueue.value){
    if(!represented.has(source.id)&&states.value[source.id]?.result?.more)
      groupFor(source.id,taskModuleLabel(source.id)||source.label,source.id);
  }
  const ordered=[...groups.values()];
  for(const group of ordered)group.rows.sort(compareRows);
  return ordered.sort((a,b)=>{
    const rankA=groupPriority[a.key]??4, rankB=groupPriority[b.key]??4;
    if(rankA!==rankB)return rankA-rankB;
    if(rankA===4){
      const roleOrder=a.rows[0]&&b.rows[0]?deskActionOrder(accountLevel.value,a.rows[0])-deskActionOrder(accountLevel.value,b.rows[0]):0;
      const sourceRank=sourceOrder.value.indexOf(a.source)-sourceOrder.value.indexOf(b.source);
      if(roleOrder||sourceRank)return roleOrder||sourceRank;
    }
    return Array.from(a.label).length-Array.from(b.label).length||a.label.localeCompare(b.label,'zh-CN');
  });
});
function setGroup(key:string,show:boolean){
  if(show)activeGroup.value=key;
  else if(activeGroup.value===key)activeGroup.value='';
}
function anchorGroup(event:MouseEvent){
  tileTrigger=event.currentTarget as HTMLButtonElement;
  const nav=tileTrigger.closest('.v2-shell')?.querySelector('.v2-mobile-nav');
  const tile=tileTrigger.getBoundingClientRect();
  const bottom=nav?.getClientRects().length?nav.getBoundingClientRect().top:window.innerHeight;
  const below=bottom-tile.bottom-12;
  const above=tile.top-12;
  const flip=below<window.innerHeight*.48&&above>below;
  groupPlacement.value=flip?'top-start':'bottom-start';
  groupMaxHeight.value=Math.max(0,flip?above:below);
}
function closeGroup(){
  activeGroup.value='';
  if(tileTrigger?.isConnected)tileTrigger.focus({preventScroll:true});
  else content.value?.focus({preventScroll:true});
}
function openGroupItem(item:WorkItem){
  closeGroup();
  openItem(item);
}
watch(taskGroups,groups=>{
  if(activeGroup.value&&!groups.some(group=>group.key===activeGroup.value))activeGroup.value='';
});
const allLoading=computed(()=>sourcesForQueue.value.some(({id})=>!states.value[id]||
  (states.value[id]?.status==='loading'&&!states.value[id]?.result)));
const queueIssues=computed(()=>sourcesForQueue.value.filter(({id})=>['stale','down'].includes(states.value[id]?.status||'')));
const queueStatus=computed(()=>{
  if (taskGroups.value.length) return '';
  if (allLoading.value) return '正在读取…';
  if (queueIssues.value.length) return '';
  if (missingOwnership.value) return '待办归属待核对';
  if (taskSearch.value.trim()) return '未找到待办';
  if (taskView.value==='today') return '今日暂无待办';
  if (taskView.value==='previous-repairs') return '暂无未完维修';
  if (taskView.value==='unread') return '暂无未读待办';
  return '暂无待办';
});
watch([taskSearch,taskView],()=>{activeGroup.value='';});
watch(queueSources,list=>{
  if(taskView.value==='previous-repairs'&&!list.some(source=>source.id==='repair_management'))taskView.value='all';
});
function sourceStatus(id:SourceId) {
  const state=states.value[id];
  if (state?.status==='down') return '读取失败';
  return state?.result?.rows.length?'部分数据待读取':'源数据未就绪';
}
const deadline=(time:number)=>new Date(time).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
function taskWhen(item:WorkItem){
  if(item.dueAt)return '截止 '+deadline(item.dueAt);
  const raw=item.time||item.noticeWindow?.start;
  const stamp=todoTimestamp(raw);
  return stamp&&raw&&!/^\d{4}-\d{2}-\d{2}$/.test(raw)?deadline(stamp):raw||'';
}
let trigger:HTMLElement|null=null;
const readOnly=(item:WorkItem)=>!!item.readOnly||sources.value.some(source=>source.id===item.kind&&source.readOnly);
function actionLabel(item:WorkItem) {
  if (readOnly(item)) return '查看';
  if (item.kind==='accounts'&&globalDesk.value) return '授权审核';
  if (item.kind==='ali_confirmation') return '确认截图';
  if (role.value!=='duty'||item.kind!=='notices'||!item.ongoing) return '办理';
  return '更新／结束';
}
function canOpenNoticeDeletion(item:WorkItem){
  return !props.user.impersonation&&props.user.can_delete_business===true&&['L0','L3'].includes(accountLevel.value||'')
    && item.kind==='notices'&&item.ongoing===true&&!readOnly(item);
}
function openItem(item:WorkItem){
  if(activeCase.value?.item.key===item.key){caseListOpen.value=false;focusCasePanel();return;}
  if (readOnly(item)) {
    changeCase({item,mode:'read',url:item.url,scope:item.scope});
    return;
  }
  const url=new URL(item.url,location.origin);
  const explicitScope=url.searchParams.get('scope');
  if(url.origin!==location.origin || explicitScope&&!authorizedScopes.value.some(option=>option.value===explicitScope))return;
  const module=moduleForRoute(url), work=url.searchParams.get('work_type')||'';
  if(!module)return;
  const scope=explicitScope||authorizedScopes.value.find(option=>option.value===selectedScope.value)?.value||authorizedScopes.value[0]?.value||'';
  const path=url.pathname+url.search;
  if(item.kind==='ali_confirmation')changeCase({item,scope,url:path,mode:'change',focusId:url.searchParams.get('change_confirmation')||''});
  else if(work==='event'&&(item.kind==='event_notices'||item.kind==='notices'))changeCase({item,scope,url:path,mode:'event',focusId:url.searchParams.get('event_id')||''});
  else if(item.kind==='notices')changeCase({item,scope,url:path,mode:'notice'});
  else {
    url.searchParams.set('workbench_return',selectedScope.value);
    changeCase({item,scope,url:url.pathname+url.search,mode:'module',
      widget:widget('module',props.user,{id:'workbench-inline-task',module,title:item.title})});
  }
}
function refreshClosedCase(current:OpenCase){
  if(current.mode==='read')return;
  if(current.mode==='event')refresh(['event_notices','events']);
  else if(current.mode==='notice')refresh(['notices','notice_plans','engineer_mop','repair_management']);
  else if(current.mode==='change')refresh(['ali_confirmation','notices','notice_plans']);
  else refresh(current.item.kind==='repair_management'?['repair_management','notices']:[current.item.kind]);
}
function changeCase(next?:OpenCase){
  const current=activeCase.value,identity=context.value;
  const proceed=()=>{
    if(activeCase.value!==current||context.value!==identity)return;
    if(!current&&next)trigger=document.activeElement as HTMLElement;
    if(current)refreshClosedCase(current);
    caseListOpen.value=false;
    caseSearch.value='';
    activeCase.value=next;
    if(next){markRead(next.item);focusCasePanel();}
    else restoreFocus();
  };
  if(current?.widget){requestEmbeddedChange(current.widget.id,proceed);return;}
  if(current&&caseEditor.value?.guardNavigation(proceed)===false)return;
  if(!current&&next&&next.mode!=='read')runNavigationAction(next.url,proceed);
  else proceed();
}
function focusCasePanel(){
  void nextTick(()=>{
    casePanel.value?.focus({preventScroll:true});
    if(caseBody.value)caseBody.value.scrollTop=0;
  });
}
function closeCase(){changeCase();}
function openCaseList(){
  caseListOpen.value=!caseListOpen.value;
  if(caseListOpen.value)void nextTick(()=>casePanel.value?.querySelector<HTMLInputElement>('.case-search input')?.focus({preventScroll:true}));
}
function escapeCaseList(event:KeyboardEvent){
  if(!caseListOpen.value)return;
  event.stopPropagation();
  event.preventDefault();
  caseListOpen.value=false;
  casePanel.value?.querySelector<HTMLElement>('.case-switch')?.focus({preventScroll:true});
}
function restoreFocus(){
  const last=trigger;trigger=null;
  void nextTick(()=>{(last?.isConnected?last:content.value)?.focus({preventScroll:true});});
}
watch([context,selectedScope],()=>{
  activeCase.value=undefined;caseListOpen.value=false;caseSearch.value='';trigger=null;
  taskSearch.value=''; taskView.value='all'; activeGroup.value='';tileTrigger=null; allShortcuts.value=false;
},{flush:'sync'});

function readDuty(data:Dict){
  checkSnapshot.value=data.scope===checkScope.value?data:undefined;
  const assigned=data.viewer_active_shift;
  shift.value=data.scope===checkScope.value&&data.viewer_on_roster===true&&['白','夜'].includes(assigned)?assigned:'';
}
let dutyTimer:ReturnType<typeof setInterval>|undefined;
watch([accessKey,checkScope],()=>{shift.value='';checkSnapshot.value=undefined;},{flush:'sync'});
function updateClock(){
  if(!document.hidden){clock.value=Date.now();refreshExpired();}
}
onMounted(()=>{dutyTimer=setInterval(updateClock,60000);});
onActivated(()=>{
  updateClock();
  if(!dutyTimer)dutyTimer=setInterval(updateClock,60000);
  for(const source of orderedSources.value)void load(source.id);
});
function pause(){
  clearInterval(dutyTimer);
  dutyTimer=undefined;
}
onDeactivated(()=>{
  pause();
  activeCase.value=undefined;caseListOpen.value=false;caseSearch.value='';
  activeGroup.value='';trigger=null;tileTrigger=null;
});
onBeforeUnmount(pause);


const currentScopeLabel=computed(()=>scopeOptions.value.find(option=>option.value===selectedScope.value)?.label || '');
const accountName=computed(()=>String(props.user.name||props.user.display_name||'').trim());
const deskLinks=computed(()=>{
  const links=props.searchLinks?.length?props.searchLinks:shortcuts.value.map(({label,url})=>({label,url}));
  return canReviewHandover(props.user)?[...links,{label:'交接班审核',url:'/?view=handover'}]:links;
});
const visibleShortcuts=computed(()=>allShortcuts.value?shortcuts.value:shortcuts.value.slice(0,6));
const showQueueCount=computed(()=>matchedTasks.value.length>0 && (
  taskView.value!=='all'||!!taskSearch.value.trim()||selectedScope.value!==dataScope.value
  ||accountLevel.value==='L3'||accountLevel.value==='duty'
));
const queueNote=computed(()=>{
  if(!queueSources.value.length)return '暂无待办';
  if(queueSources.value.every(source=>!states.value[source.id]?.result))
    return queueSources.value.some(source=>!states.value[source.id]||states.value[source.id]?.status==='loading')?'正在读取…':'待办暂不可用';
  if(missingOwnership.value)return '部分归属待核对';
  if(queueSources.value.some(source=>states.value[source.id]?.status!=='ok'||states.value[source.id]?.result?.more))return '部分待办待更新';
  return '';
});
function showQueue(view:TaskView){
  taskView.value=view;
  taskSearch.value='';
  content.value?.scrollIntoView({block:'start'});
  content.value?.focus({preventScroll:true});
}
const viewButtons=computed<{view:TaskView;label:string;count?:number}[]>(()=>[
  {view:'all',label:'全部'},
  {view:'today',label:'今日',count:queueTasks.value.filter(isTodayTask).length},
  {view:'unread',label:'未读',count:unreadTasks.value.length},
  ...(hasPreviousRepairs.value||taskView.value==='previous-repairs'?[{view:'previous-repairs' as const,label:'历史未完维修',count:queueTasks.value.filter(isPreviousRepair).length}]:[]),
]);
function shortcutIcon(icon:string){return moduleIcons[icon as keyof typeof moduleIcons]||Layers3;}
</script>

<template>
  <div class="wb-home task-desk">
    <WorkbenchDailyLayout :name="accountName" :avatar="user.avatar_url || user.avatar" :clock="clock" :links="deskLinks"
      :tasks="periodTasks" :unread="unreadTasks" :calendar-scope="dataScope" :impersonating="!!user.impersonation"
      :calendar-identity="accessKey" :user-id="String(user.open_id||'')" :queue-note="queueNote" @open="emit('open',$event)" @task="openItem" @unread="showQueue('unread')">
      <template #context><span class="wb-identity role-context">{{ [roleLabel,scopeOptions.length<=1?currentScopeLabel:'',shift?shift+'班':''].filter(Boolean).join(' · ') }}</span></template>
      <template v-if="scopeOptions.length>1" #focus>
        <label class="wb-scope scope-choice"><select data-native-select :value="selectedScope" aria-label="工作台楼栋" @change="switchScope"><option v-for="opt in scopeOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option></select></label>
      </template>
      <template #shortcuts>
        <nav v-if="shortcuts.length" class="wb-shortcuts business-strip" aria-label="常用业务">
          <strong>常用业务</strong>
          <button v-for="entry in visibleShortcuts" :key="entry.id" class="wb-shortcut business-key" @click="emit('open',entry.url)"><component :is="shortcutIcon(entry.icon)" :size="15" />{{ entry.label }}</button>
          <button v-if="shortcuts.length>6" class="more-key" :aria-expanded="allShortcuts" @click="allShortcuts=!allShortcuts">{{ allShortcuts?'收起入口':'全部入口 ('+shortcuts.length+')' }}</button>
        </nav>
      </template>

      <section v-if="priorities.length" class="priority-board" aria-label="紧急事项">
        <header><h2><CircleAlert :size="17" aria-hidden="true" />紧急事项 <span>{{ priorities.length }}</span></h2>
          <RecordViewSwitch v-model="priorityView"/>
          <button v-if="priorities.length>2" class="more-key" :class="{'mobile-priority-toggle':priorities.length===3}" :aria-expanded="priorityExpanded" @click="priorityExpanded=!priorityExpanded">{{ priorityExpanded?'收起':'查看全部' }}</button></header>
        <div class="priority-grid" :data-collapsed="!priorityExpanded" :data-view="priorityView">
          <button v-for="{item,rank,label} in (priorityExpanded?priorities:priorities.slice(0,3))" :key="item.key" class="priority-capsule"
            :class="{'due-soon':rank===2}" :aria-label="label+'，'+actionLabel(item)+' '+item.title" :title="item.title" @click="openItem(item)">
            <span class="priority-status">{{ label }}</span>
            <strong>{{ item.title }}</strong>
            <small>{{ [taskScopeLabel(item.scope),rank<=0?'':sourceLabels.get(item.kind),taskWhen(item)].filter(Boolean).join(' · ') }}</small>
            <ChevronRight :size="15" aria-hidden="true" />
          </button>
        </div>
      </section>
      <section v-if="queueSources.length" ref="content" class="wb-todo-panel queue-panel" tabindex="-1" aria-labelledby="workbench-queue-title" :aria-busy="allLoading">
        <header class="queue-toolbar">
          <div class="queue-title"><h2 id="workbench-queue-title">待办办理</h2>
            <span v-if="showQueueCount" class="wb-task-count" :title="queueNote" :aria-label="'已读取 '+matchedTasks.length+' 项'">{{ matchedTasks.length }}</span>
          </div>
          <div class="queue-switches" role="group" aria-label="待办筛选">
            <button v-for="view in viewButtons" :key="view.view" :aria-pressed="taskView===view.view" @click="taskView=view.view">
              {{ view.label }}<b v-if="view.count" class="queue-filter-count" aria-hidden="true">{{ view.count }}</b>
            </button>
          </div>
          <div class="queue-filters">
            <label><Search :size="15" aria-hidden="true" /><input v-model="taskSearch" type="search" aria-label="搜索待办" placeholder="搜索待办"></label>
            <RecordViewSwitch v-model="queueView" @update:model-value="activeGroup=''"/>
          </div>
        </header>
        <details v-if="queueIssues.length" class="desk-queue-health queue-health"><summary>{{ queueIssues.length }} 类待办待更新</summary>
          <div v-for="source in queueIssues" :key="source.id"><span>{{ source.label }} · {{ sourceStatus(source.id) }}</span><button :aria-label="'重试'+source.label" @click="load(source.id,true)">重试</button></div>
        </details>
        <p v-if="queueStatus" class="quiet-message" role="status">{{ queueStatus }}</p>
        <WorkbenchTaskRows v-if="queueView==='table'&&matchedTasks.length" :groups="byBuilding([...matchedTasks].sort(compareRows))"
          view="table" :label="taskLabel" :action="actionLabel" :when="taskWhen" :seen="isRead" :late="isOverdue" :read-only="readOnly"
          :can-delete="canOpenNoticeDeletion" @open="openItem"/>
        <ul v-else-if="taskGroups.length" class="task-tiles" aria-label="待办模块">
          <li v-for="group in taskGroups" :key="group.key" :style="{'--tile-hue':group.hue}">
            <NPopover trigger="click" :placement="groupPlacement" raw :show-arrow="false"
              :animated="false" :show="activeGroup===group.key" @update:show="show=>setGroup(group.key,show)">
              <template #trigger>
                <button type="button" class="wb-task-tile" :data-source="group.source" :data-group="group.key" :data-count="group.rows.length"
                  aria-haspopup="dialog" :aria-expanded="activeGroup===group.key" :aria-label="group.label+'，'+(group.rows.length?group.rows.length+' 项':'有待核对页')"
                  @click="anchorGroup">
                  <component :is="group.icon" class="tile-icon" :size="20" aria-hidden="true" />
                  <b class="tile-count">{{ group.rows.length || '…' }}</b>
                  <strong class="tile-label">{{ group.label }}</strong>
                </button>
              </template>
              <section class="tile-popover" role="dialog" aria-label="模块待办条目" tabindex="-1"
                :style="{'--tile-hue':group.hue,'--tile-available':groupMaxHeight===undefined?undefined:groupMaxHeight+'px'}"
                @vue:mounted="(node:VNode)=>node.el?.focus({preventScroll:true})" @keydown.esc.stop.prevent="closeGroup">
                <header><component :is="group.icon" :size="17" aria-hidden="true" /><h3>{{ group.label }}</h3>
                  <span>{{ group.rows.length ? (group.source==='notices'?'进行中 ':'')+group.rows.length+' 项' : '暂无已读明细' }}</span>
                  <RecordViewSwitch v-if="group.rows.length" v-model="popoverView"/>
                  <button v-if="states[group.source]?.result?.more" type="button" class="tile-more" :disabled="states[group.source]?.status==='loading'" @click="load(group.source,true,true)">继续读取</button>
                  <button type="button" class="tile-close" aria-label="关闭待办条目" @click="closeGroup"><X :size="16" /></button>
                </header>
                <WorkbenchTaskRows :groups="byBuilding(group.rows)" :view="popoverView" :action="actionLabel" :when="taskWhen"
                  :seen="isRead" :late="isOverdue" :read-only="readOnly" :can-delete="canOpenNoticeDeletion" @open="openGroupItem"/>
              </section>
            </NPopover>
          </li>
        </ul>
      </section>
      <PreShiftCheck v-if="checkScope" ref="checkPanel" :key="accessKey+checkScope" class="wb-check-panel shift-check" tabindex="-1"
        :user="checkUser" :fixed-scope="checkScope" workbench @snapshot="readDuty" />
      <template #overview="{seals,sealStatus}"><PeriodOverview :key="accessKey" :user="user" :scope="dataScope" :scope-label="currentScopeLabel"
        :scopes="concreteScopes" :role="role" :clock="clock" :tasks="periodTasks" :plan-state="states.notice_plans?.status"
        :seal-status="sealStatus" :seals="seals" :partial="!!queueNote && queueNote!=='暂无待办'" :action="actionLabel"
        :check-status="checkStatus" :inspection-state="orderedSources.some(source=>source.id==='inspection')?states.inspection?.status||'loading':undefined"
        @open="emit('open',$event)" @task="openItem" @retry="load('notice_plans',true)" @check="focusPreShift"
        @retry-inspection="load('inspection',true)" /></template>
    </WorkbenchDailyLayout>

    <NModal :show="!!activeCase" :mask-closable="false" :close-on-esc="!caseListOpen" :auto-focus="false" @update:show="visible=>!visible&&closeCase()">
      <div v-if="activeCase" class="task-dialog" role="dialog" aria-modal="true" aria-label="待办办理窗口">
        <section ref="casePanel" class="case-panel" tabindex="-1" :aria-label="(activeCase.mode==='read'?'查看 ':'办理 ')+activeCase.item.title" @keydown.esc="escapeCaseList">
          <header class="case-header">
            <div class="case-title"><small>{{ [taskModuleLabel(activeCase.item.kind),taskScopeLabel(activeCase.item.scope||activeCase.scope)].filter(Boolean).join(' · ') }}</small><h2 :title="activeCase.item.title">{{ activeCase.item.title }}</h2></div>
            <div id="workbench-task-header-tools" class="case-tools" />
            <button type="button" class="close-key case-switch" :aria-expanded="caseListOpen" aria-controls="workbench-case-list" @click="openCaseList"><ListChecks :size="17" aria-hidden="true" />切换待办</button>
            <button type="button" class="close-key" :aria-label="activeCase.mode==='read'?'关闭只读详情':'关闭待办办理'" @click="closeCase"><X :size="18" /></button>
          </header>
          <section v-if="caseListOpen" id="workbench-case-list" class="case-list" aria-label="选择另一条待办">
            <label class="case-search"><Search :size="16" aria-hidden="true" /><input v-model="caseSearch" type="search" aria-label="搜索可切换待办" placeholder="搜索名称、楼栋或模块"></label>
            <ul><li v-for="item in switchableCases" :key="item.key"><button type="button" @click="caseListOpen=false;openItem(item)">
              <small>{{ [taskModuleLabel(item.kind),taskScopeLabel(item.scope),item.status].filter(Boolean).join(' · ') }}</small><strong>{{ item.title }}</strong><ChevronRight :size="15" aria-hidden="true" />
            </button></li></ul>
            <p v-if="!switchableCases.length" role="status">{{ caseSearch?'没有匹配的待办':'当前楼栋暂无其他待办' }}</p>
          </section>
          <div v-show="!caseListOpen" ref="caseBody" class="case-body" :key="activeCase.item.key">
            <ModuleWidget v-if="activeCase.widget" :key="activeCase.url" :item="activeCase.widget" :user="user" :scope="activeCase.scope" :scopes="authorizedScopes"
              fixed-scope record-only :initial-url="activeCase.url" header-tools-id="workbench-task-header-tools" @close="closeCase" />
            <NoticeWorkspace v-else-if="activeCase.mode==='notice'" ref="caseEditor" :url="activeCase.url" :scope="activeCase.scope" :scopes="authorizedScopes"
              :user="user" :active="true" record-only @record-close="closeCase" />
            <EventNoticeBoard v-else-if="activeCase.mode==='event'" ref="caseEditor" :scope="activeCase.scope" :focus-event-id="activeCase.focusId" :active="true" />
            <NoticeChangeConfirmations v-else-if="activeCase.mode==='change'" ref="caseEditor" :scope="activeCase.scope" :focus-id="activeCase.focusId" embedded @close="closeCase" @changed="refresh(['ali_confirmation','notices'])" />
            <dl v-else><div v-for="[label,value] in [['业务',sourceLabels.get(activeCase.item.kind)],['楼栋',taskScopeLabel(activeCase.scope)],['类型',activeCase.item.detail],['状态',activeCase.item.status],['开始时间',activeCase.item.noticeWindow?.start || activeCase.item.time],['结束时间',activeCase.item.noticeWindow?.end || (activeCase.item.dueAt && deadline(activeCase.item.dueAt))]]" :key="label"><dt>{{ label }}</dt><dd>{{ value || '暂未提供' }}</dd></div></dl>
          </div>
        </section>
      </div>
    </NModal>
  </div>
</template>

<style scoped>
.task-desk.wb-home { display:block; width:100%; min-width:0; max-width:none; margin:0; padding:0 }
.task-desk :is(button,select,input) { font:inherit }
.task-desk button,.task-dialog button { cursor:pointer }
.task-desk button:disabled { cursor:default }
.business-strip,.business-key,.queue-title,.queue-switches,.queue-filters label,.expand-key { display:flex; align-items:center; gap:7px }
.scope-choice { min-width:0 }
.scope-choice select { max-width:100%;min-height:34px;padding:4px 8px;border:1px solid var(--desk-line);border-radius:999px;background:var(--desk-paper);color:var(--desk-ink);font-size:12px }
.quiet-message { color:var(--desk-muted); font-size:12px }
.task-desk button:focus-visible,.task-dialog button:focus-visible { outline:2px solid var(--v2-blue,#24799f); outline-offset:2px }
.task-desk .role-context { font-size:12px; color:var(--desk-muted); background:transparent; padding:0; margin:0 }
.task-desk .business-strip { flex-wrap:wrap; gap:5px 7px; padding:0; background:transparent }
.business-strip>strong { color:var(--desk-muted); font-size:12px; margin-right:4px }
.task-desk .business-key,.more-key,.expand-key,.queue-health button { width:auto; min-height:32px; justify-content:center; padding:4px 9px; border:1px solid var(--desk-line); border-radius:9px; background:var(--desk-paper); color:var(--desk-ink); font-size:12px }
.business-key svg { color:var(--desk-blue) }
.task-desk .queue-panel,.shift-check { display:block; color:var(--desk-ink); overflow-wrap:anywhere; scroll-margin-top:70px }
.queue-panel { min-width:0; padding:10px 12px; border:1px solid var(--desk-line); border-radius:15px; background:var(--desk-paper); box-shadow:var(--desk-shadow) }
.shift-check { border:1px solid var(--desk-line);border-radius:13px;background:var(--desk-paper) }
.task-desk .shift-check { margin-bottom:12px; padding:12px; min-width:0 }
.queue-toolbar { display:flex;flex-wrap:wrap;align-items:center;gap:7px 12px }
.queue-title { gap:7px }
.queue-title h2 { margin:0;font-size:18px;line-height:1.25 }
.queue-title .wb-task-count { margin:0; padding:1px 6px; border-radius:999px; color:var(--desk-blue); background:var(--desk-blue-wash); font-variant-numeric:tabular-nums }
.queue-switches { flex-wrap:wrap; gap:4px }
.task-desk .queue-switches button { min-height:32px;padding:3px 9px;border:1px solid transparent;border-radius:999px;background:transparent;color:var(--desk-muted);font-size:12px;white-space:nowrap }
.task-desk .queue-switches button[aria-pressed="true"] { background:var(--v2-palette-selection); color:var(--v2-palette-accent-text); font-weight:650 }
.queue-filters { display:flex;align-items:center;gap:5px;flex:1 1 170px;max-width:272px;margin-inline-start:auto }
.queue-filters label { flex:1;min-width:0; min-height:35px; padding:0 7px; background:var(--desk-well); border:1px solid var(--desk-line); border-radius:8px; color:var(--desk-muted) }
.queue-filters svg { flex:none }
.task-desk .queue-filters input { min-width:0; width:100%; min-height:32px; border:0; color:var(--desk-ink); background:transparent; box-shadow:none; padding:0; font-size:13px }
.task-tiles {
  display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,106px),1fr));gap:7px;
  margin:9px 0 0;padding:0;list-style:none
}
.task-tiles>li { min-width:0 }
.wb-task-tile,.tile-popover { --tile-accent:hsl(var(--tile-hue) 57% 35%);--tile-tint:color-mix(in srgb,var(--tile-accent) 10%,var(--desk-paper)) }
.task-desk .wb-task-tile {
  display:grid;grid-template-columns:1fr auto;align-items:center;align-content:space-between;gap:7px 4px;
  width:100%;min-height:72px;padding:9px 11px;text-align:left;border-radius:13px;
  border:1px solid color-mix(in srgb,var(--tile-accent) 26%,var(--desk-line));background:var(--tile-tint);color:var(--tile-accent)
}
.wb-task-tile:hover,.wb-task-tile[aria-expanded=true] { outline:2px solid var(--tile-accent);outline-offset:-2px }
.tile-count { font-size:18px;line-height:1;font-variant-numeric:tabular-nums }
.tile-label { grid-column:1/-1;font-size:13px;line-height:1.25;overflow-wrap:anywhere }
.tile-popover {
  --desk-paper:var(--soft-face,#f4f7f7);--desk-well:var(--soft-field,#edf2f3);--desk-line:var(--soft-edge,#c5d2d0);
  --desk-ink:var(--v2-ink,#20373b);--desk-muted:var(--v2-muted,#496064);--desk-blue:var(--v2-blue,#24799f);--desk-green:var(--v2-palette-accent-text,#175e52);
  display:flex;flex-direction:column;box-sizing:border-box;width:min(420px,calc(100vw - 32px));
  /* The shared mobile dialog limit also matches this anchored popover. */
  max-height:min(420px,48dvh,var(--tile-available,100dvh))!important;
  background:var(--desk-paper);color:var(--desk-ink);border:1px solid var(--desk-line);
  border-radius:14px;box-shadow:0 10px 32px #112c3a32;overflow:hidden;font-size:13px
}
.tile-popover>header { display:flex;flex:none;flex-wrap:wrap;align-items:center;gap:6px;padding:5px 8px 5px 12px;background:var(--tile-tint);color:var(--tile-accent);border-bottom:1px solid var(--desk-line) }
.tile-popover h3 { margin:0;font-size:14px }.tile-popover>header>span { flex:1;font-size:12px }
.tile-popover button { color:inherit;cursor:pointer }
.tile-more,.tile-close { border:0;background:transparent;min-height:32px;border-radius:8px }
.tile-close { width:34px;display:grid;place-items:center }
.tile-popover .queue-list { min-height:0;list-style:none;padding:0 8px;margin:0;overflow:auto;overscroll-behavior:contain }
:global(.v-binder-follower-container:has(.tile-popover)) { position:fixed }
:global([data-v2-theme="dark"] :is(.wb-task-tile,.tile-popover)) { --tile-accent:hsl(var(--tile-hue) 65% 73%) }
@media(max-width:600px){
  .task-tiles { grid-template-columns:repeat(3,minmax(0,1fr));gap:6px }
  .task-desk .wb-task-tile { min-height:66px;padding:8px 10px }
  .tile-popover :is(.tile-close,.tile-more) { min-height:44px }
}
.quiet-message,.queue-health { margin:7px 0 0; line-height:1.6 }
.quiet-message { padding-block:5px }
.queue-health { border-radius:9px; padding:7px 9px; color:var(--desk-muted); background:var(--desk-well); font-size:12px }
.queue-health summary { cursor:pointer }
.queue-health>div { display:flex; align-items:center; justify-content:space-between; margin-top:6px }
.task-dialog { width:min(1240px,calc(100vw - 36px)); border:1px solid var(--soft-edge,#aebdc1); border-radius:16px; background:var(--soft-face,#f4f7f7); box-shadow:0 18px 65px #10252c3d; color:var(--v2-ink,#20373b); box-sizing:border-box; overflow-wrap:anywhere }
.close-key { display:grid; place-items:center; min-width:38px; min-height:36px; padding:5px 9px; color:inherit; background:var(--soft-field,#edf2f3); border:1px solid #879b9c66; border-radius:8px }
@media(max-width:760px) {
  .scope-choice select { min-height:44px }
  .task-desk :is(.business-key,.more-key,.expand-key,.queue-switches button),.queue-filters label,.close-key { min-height:44px }
  .queue-switches { gap:2px }
  .task-dialog { width:calc(100vw - 12px) }
}
@media(max-width:440px) {
  .task-desk .business-strip { gap:4px }
  .queue-panel { padding:8px }
}
.priority-board { margin:0 0 12px }
.queue-filter-count { margin-inline-start:4px;font-size:11px;font-variant-numeric:tabular-nums }
.priority-board>header,.priority-board h2 { display:flex;align-items:center;gap:6px }
.priority-board>header { justify-content:space-between;margin-bottom:6px }
.priority-board h2 { font-size:17px;color:var(--priority-red,#a82c3e);margin-inline-end:auto }
.priority-board h2 span { font-size:12px;font-variant-numeric:tabular-nums }
.priority-grid { display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px }
.priority-grid:has(> :only-child) { grid-template-columns:minmax(0,min(100%,460px)) }
.priority-grid:has(> :nth-child(2):last-child) { grid-template-columns:repeat(2,minmax(0,1fr)) }
.priority-board .priority-grid[data-view=table]{grid-template-columns:repeat(auto-fit,minmax(min(100%,19rem),1fr));max-inline-size:58rem}
.task-desk .priority-capsule {
  --attention:var(--priority-red,#a82c3e);
  display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:3px 5px;min-width:0;
  padding:7px 10px;border:1px solid color-mix(in srgb,var(--attention) 30%,var(--desk-line));border-inline-start:3px solid var(--attention);
  border-radius:18px;background:color-mix(in srgb,var(--attention) 7%,var(--desk-paper));color:var(--desk-ink);text-align:left
}
.priority-capsule.due-soon { --attention:var(--priority-amber,#865313) }
.priority-capsule :is(strong,small) { min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap }
.priority-capsule strong { font-size:13px;font-weight:700 }
.priority-capsule small { font-size:11px;color:var(--desk-muted) }
.priority-status { justify-self:start;padding:1px 6px;border-radius:999px;background:color-mix(in srgb,var(--attention) 14%,var(--desk-paper));color:var(--attention);font-size:11px;font-weight:700 }
.priority-capsule>svg { color:var(--attention);grid-area:1/2/span 3 }
.priority-grid[data-view=table] .priority-capsule{border-radius:7px}
.priority-grid[data-view=table] .priority-capsule strong{white-space:normal;overflow-wrap:anywhere}
:global([data-v2-theme="dark"] .task-desk .priority-board) { --priority-red:#ffaeb2;--priority-amber:#fed491 }
@media(max-width:700px){
  .priority-board .priority-grid { grid-template-columns:minmax(0,1fr) }
  .priority-grid[data-collapsed=true] > :nth-child(n+3) { display:none }
  .priority-capsule strong { display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;white-space:normal;overflow-wrap:anywhere }
}
@media(min-width:701px){.task-desk .mobile-priority-toggle {display:none}}
.case-panel { display:flex;flex-direction:column;min-width:0;max-height:min(860px,calc(100dvh - 48px));border-radius:inherit;overflow:hidden }
.case-header { display:flex;align-items:center;gap:8px;padding:10px 14px;flex:none;background:var(--soft-field,#e4eeeb);border-bottom:1px solid var(--soft-edge,#bacac8) }
.case-title { flex:1;min-width:0 }
.case-title small { color:var(--v2-muted,#496064);font-size:12px }
.case-title h2 { margin:0;font-size:17px;font-weight:700;color:var(--v2-ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap }
.case-tools,.case-panel .close-key { flex:none }
.case-tools:empty { display:none }
.case-panel .case-switch { display:flex;align-items:center;gap:6px;color:var(--v2-blue,#175e52);font-weight:650 }
.case-switch[aria-expanded=true] { background:var(--soft-sky,#dcecf2);border-color:currentColor }
.case-list { flex:none;display:grid;grid-template-rows:auto minmax(0,1fr);max-height:min(40dvh,360px);padding:9px 14px;gap:6px;border-bottom:1px solid var(--soft-edge,#bacac8) }
.case-search { display:flex;align-items:center;gap:8px;border:1px solid var(--soft-edge,#aebdc1);border-radius:8px;padding-inline:9px;background:var(--soft-field,#edf2f3) }
.case-search input { width:100%;min-width:0;min-height:37px;font:inherit;color:inherit;background:transparent;border:0 }
.case-list ul { list-style:none;margin:0;padding:0;overflow:auto;overscroll-behavior:contain }
.case-list li + li { border-top:1px solid var(--soft-edge,#bacac8) }
.case-list li button { display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:2px 8px;width:100%;padding:6px 8px;text-align:start;border:0;border-radius:6px;background:transparent;color:inherit;font:inherit }
.case-list li button:hover,.case-list li button:focus-visible { background:var(--soft-field,#edf2f3) }
.case-list li small { color:var(--v2-muted,#496064);font-size:11px }
.case-list li strong { grid-column:1;min-width:0;overflow-wrap:anywhere;font-size:13px;font-weight:650 }
.case-list li svg { grid-column:2;grid-row:1/span 2 }
.case-list p { margin:0;padding:8px;font-size:13px;color:var(--v2-muted,#496064) }
.case-body { flex:0 1 auto;min-height:0;min-width:0;overflow:auto;overscroll-behavior:contain;scroll-padding-block:12px;padding:12px }
.case-panel dl { display:grid;grid-template-columns:repeat(auto-fit,minmax(min(170px,100%),1fr));gap:7px 14px;margin:10px 12px }
.case-panel dt { color:var(--v2-muted,#496064);font-size:12px }
.case-panel dd { margin:0 }
@media(max-width:640px){
  .case-panel { max-height:calc(100dvh - 20px - env(safe-area-inset-bottom)) }
  .case-header { display:grid;grid-template-columns:minmax(0,1fr) auto auto;gap:4px 7px;padding:8px 10px }
  .case-title { grid-column:1/-1 }
  .case-panel .close-key { min-height:44px }
  .case-tools:empty { display:block }
  .case-search input { min-height:44px;font-size:16px }
  .case-list li button { min-height:44px }
  .case-body { padding:8px }
}
.task-desk .shift-check { margin:12px 0 0 }
.shift-check :deep(.v2-check-task:only-child:has(> .n-empty)) { grid-column:1 / -1;display:block }
@media(max-width:640px) {
  .queue-toolbar { display:grid; grid-template-columns:minmax(0,1fr) minmax(130px,49%); gap:4px 6px }
  .queue-filters { grid-area:1/2; width:100% }
  .queue-switches { grid-column:1/-1 }
}
</style>
