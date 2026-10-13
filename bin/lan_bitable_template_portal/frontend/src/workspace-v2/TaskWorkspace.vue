<template>
  <div class="v2-home" :class="{'v2-task-embedded':embedded}">
    <template v-if="view !== 'functions'">
      <section v-if="!embedded" class="v2-welcome"><div><span class="v2-eyebrow">{{ today }} · {{ greeting }}</span><h1>{{ view === 'tasks' ? '待处理事项' : personaTitles[persona] }}</h1><p>{{ scopeLabel }} · {{ user.name }}</p></div><NButton secondary :loading="taskLoading" :disabled="!taskTabs.length" @click="refresh"><template #icon><RefreshCw :size="16" /></template>刷新</NButton></section>
      <nav v-if="!embedded && view === 'home' && persona !== 'duty' && commonActions.length" class="v2-common-actions" aria-label="常用操作">
        <NButton v-for="entry in commonActions" :key="entry.id" secondary @click="open(moduleUrl(entry,scope))"><template #icon><component :is="moduleIcons[entry.icon as keyof typeof moduleIcons]" :size="18" /></template>{{ entry.label }}</NButton>
      </nav>
      <div class="v2-work-grid tasks-only">
        <section class="v2-task-section">
          <div class="v2-section-title"><h2 v-if="!embedded">{{ persona === 'personal' && view === 'home' ? '我的待办' : '未开始与进行中' }}</h2><NSelect v-if="taskTabs.length > 1" v-model:value="taskKind" class="v2-task-filter" :options="taskTabs" value-field="id" aria-label="待办业务分类" filterable /><NButton v-if="embedded" text :loading="taskLoading" aria-label="刷新待办" @click="refresh"><RefreshCw :size="15" /></NButton></div>
          <OverviewPanel v-if="panelKinds.includes(taskKind)" :key="taskKind" :kind="taskKind" :user="user" :scope="scope" :scopes="scopes" tasks-only :brief="brief" @open="open" />
          <template v-else>
          <NAlert v-if="taskError" type="warning" :bordered="false">{{ taskError }} <NButton text @click="loadTasks">重试</NButton></NAlert>
          <NSpin v-if="(compact || embedded) && taskTabs.length" :show="taskLoading"><NList class="v2-mobile-tasks"><NListItem v-for="row in visibleRows" :key="row.key">
            <NButton data-v2-navigation quaternary class="v2-task-row" @click="open(row.url)">
              <span class="v2-task-copy"><span class="v2-task-title">{{ row.title }}</span><span class="v2-mobile-task-meta"><span>{{ row.scope }}</span><span v-if="row.time">{{ row.time }}</span><NTag v-if="dueLabel(row)" type="warning" size="small" :bordered="false">{{ dueLabel(row) }}</NTag></span></span>
              <span class="v2-task-state"><NTag size="small" :bordered="false" :type="row.status==='未开始'?'default':'info'">{{ row.status }}</NTag><small v-if="row.progress!==null">进度 {{ row.progress }}%</small></span>
              <span class="v2-task-action">{{ brief?'查看':taskKind==='learning'?(row.status==='未开始'?'开始学练':'继续学练'):'办理' }}<ChevronRight :size="16" class="v2-task-arrow" aria-hidden="true" /></span>
            </NButton>
          </NListItem></NList><NEmpty v-if="!visibleRows.length && !taskLoading && !taskError" :description="brief&&rows.length?'已读取事项中暂无到期或异常提醒':emptyText" /></NSpin>
          <NDataTable v-else-if="taskTabs.length" :columns="columns" :data="rows" :loading="taskLoading" :row-key="row=>row.key" :bordered="false" :single-line="false" :pagination="false" size="small"><template #empty><NEmpty :description="taskError ? '资料暂未读取成功' : emptyText" /></template></NDataTable>
          <NEmpty v-else description="当前没有获授待办模块" />
          <div v-if="taskTabs.length" class="v2-section-foot"><span>{{ brief?'基于已读取事项':taskNote }}</span><NButton data-v2-navigation text type="primary" :disabled="!taskUrl" @click="open(footerTarget)">{{ brief?'查看待办':taskKind === 'learning' ? '进入我的学练' : '更多待办' }} <ArrowRight :size="15" /></NButton></div>
          </template>
          <p v-if="persona === 'personal' && view === 'home'" class="v2-source-note">本人工单请从分配给你的工单链接进入。</p>
        </section>
      </div>
    </template>
    <section v-if="view === 'functions'" class="v2-functions functions-page"><div class="v2-section-title"><h1>全部功能</h1></div><div class="v2-function-groups"><section v-for="group in featureGroups" :key="group.name"><h2>{{ group.name }}</h2><div class="v2-feature-grid"><NButton v-for="entry in group.items" :key="entry.id" class="v2-feature-entry" :bordered="false" quaternary @click="open(moduleUrl(entry,scope))"><span class="v2-feature-icon"><component :is="moduleIcons[entry.icon as keyof typeof moduleIcons]" :size="20" /></span><span>{{ entry.label }}</span><ChevronRight :size="16" /></NButton></div></section></div><NEmpty v-if="!featureGroups.length" description="暂无已授权功能" /></section>
  </div>
</template>
<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NDataTable, NEmpty, NList, NListItem, NSelect, NSpin, NTag, type DataTableColumns } from 'naive-ui';
import { ArrowRight, ChevronRight, RefreshCw } from 'lucide-vue-next';
import { requestJson, type Dict } from './client';
import { modules, moduleIcons, navigationModules, NOTICE_IDS, beijingDate, count, moduleUrl } from './modules';
import { eventStatusTone } from '../eventManagementUtils';
import { personaFor, personaTitles } from './persona';
import OverviewPanel from './OverviewPanel.vue';
const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];scope:string;view:string;embedded?:boolean;initialKind?:string;brief?:boolean}>();
const emit=defineEmits<{open:[url:string]}>();
const open=(url:string)=>emit('open',url);
const today=beijingDate(), greeting=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',weekday:'long'}).format(new Date());
const permitted=computed<string[]>(()=>props.user.modules || []);
const persona=computed(()=>personaFor(props.user));
const commonActions=computed(()=>{
  const order={global:['notices','event','repair_management','daily'],engineer:['event','repair_management','plan_convergence'],supervisor:['event','notices','cabinet','plan_convergence'],duty:[],personal:['learning','knowledge','signatures']}[persona.value];
  const entries=navigationModules(permitted.value);
  return order.flatMap(id=>entries.filter(entry=>entry.id===id));
});
const scopeLabel=computed(()=>props.scopes.find(s=>s.value===props.scope)?.label || props.scope || '授权范围');
const compact=ref(window.innerWidth<800);
const taskKind=ref(''), taskLoading=ref(false), taskError=ref(''), rows=ref<Dict[]>([]), taskNote=ref('');
const emptyText=ref('当前分类暂无待处理事项');
const visibleRows=computed(()=>props.brief?rows.value.filter(row=>taskKind.value==='learning'||dueLabel(row)||/异常|失败|待核对/.test(row.status)).slice(0,3):rows.value);
let taskAbort:AbortController|undefined, disposed=false;
const cache=new Map<string,{at:number;value:Dict}>();
const noticeTypes=computed(()=>modules.filter(m=>NOTICE_IDS.includes(m.id)&&permitted.value.includes(m.id)));
const panelKinds=['plan_convergence','cabinet','engineer_mop','drills','critical_guard'];
const taskTabs=computed(()=>persona.value==='personal' && props.view==='home' ? (permitted.value.includes('learning')?[{id:'learning',label:'今日学练'}]:[]) : [
  ...(permitted.value.includes('daily')?[{id:'daily',label:'每日任务'}]:[]),
  ...noticeTypes.value.map(m=>({id:m.id,label:m.label})),
  ...(permitted.value.includes('repair_management')?[{id:'repair_management',label:'维修项目'}]:[]),
  ...(permitted.value.includes('event')?[{id:'event',label:'事件'}]:[]),
  ...(permitted.value.includes('learning')?[{id:'learning',label:'我的学练'}]:[]),
  ...panelKinds.filter(id=>permitted.value.includes(id)).map(id=>({id,label:modules.find(m=>m.id===id)!.label})),
].sort((a,b)=>{const order=persona.value==='engineer'?['event','repair_management']:persona.value==='supervisor'?['event','daily']:NOTICE_IDS;return (order.indexOf(a.id)<0?99:order.indexOf(a.id))-(order.indexOf(b.id)<0?99:order.indexOf(b.id));}));
const taskUrl=computed(()=>{if(taskKind.value==='learning')return '/learning?view=practice';const m=modules.find(m=>m.id===taskKind.value);return m?moduleUrl(m,props.scope):'';});
const footerTarget=computed(()=>props.brief?'/?'+new URLSearchParams({view:'tasks',kind:taskKind.value,scope:props.scope}):taskUrl.value);
const columns=computed<DataTableColumns<Dict>>(()=>[
  {title:'事项',key:'title',minWidth:260,render:row=>h(NButton,{text:true,type:'primary',class:'v2-row-title',onClick:()=>open(row.url)},()=>row.title)},
  {title:'状态',key:'status',width:106,render:row=>h(NTag,{size:'small',bordered:false,type:row.status.includes('完成')?'success':'info'},()=>row.status)},
  {title:'楼栋',key:'scope',width:74},
  {title:'时间 / 进度',key:'time',width:180,render:row=>h('span',{class:'v2-table-meta'},[row.time||'—',row.progress!=null?h('small',{},`${row.progress}%`):null,dueLabel(row)?h(NTag,{type:'warning',size:'small',bordered:false},()=>dueLabel(row)):null])},
  {title:'操作',key:'action',width:88,render:row=>h(NButton,{text:true,type:'primary','data-v2-navigation':'',onClick:()=>open(row.url)},()=>taskKind.value==='learning'?'继续学练':'办理')},
]);
function dueTime(value:string){const normalized=String(value).replace(' ','T');return Date.parse(/(?:Z|[+-]\d{2}:\d{2})$/.test(normalized)?normalized:normalized+'+08:00');}
function dueLabel(row:Dict){if(!row.dueAt||!Number.isFinite(dueTime(row.dueAt)))return '';return dueTime(row.dueAt)<Date.now()?'已过期':String(row.dueAt).slice(0,10)===today?'今日到期':'';}
const featureGroups=computed(()=>{const entries=navigationModules(permitted.value);return [...new Set(entries.map(m=>m.group))].map(name=>({name,items:entries.filter(m=>m.group===name)}));});
function text(value:any):string {return Array.isArray(value)?value.map(text).join('、'):value&&typeof value==='object'?String(value.text||value.name||''):String(value??'');}
function items(value:unknown):Dict[]{if(!Array.isArray(value))throw new Error('待办资料未完整返回，请重试或进入业务列表核对。');return value;}
function toRows(items:Dict[],kind:string):Dict[]{
  return items.map((item,i)=>{
    const fields=item.display_fields||item.fields||{},id=item.active_item_id||item.record_id||item.id;
    const url=new URL(moduleUrl(modules.find(m=>m.id===kind)!,props.scope),location.origin);
    if(id)url.searchParams.set(item.active_item_id?'active_item_id':'record_id',id);
    return{
      key:kind+':'+String(id||i),
      title:text(item.title||item.name||item.notice_title||fields['维护总项']||fields['名称（标题）']||fields['名称']||fields['标题']||fields['维修名称']||fields['事件概述'])||'未命名事项',
      status:text(item.status_label||item.workflow||item.source_progress||item.source_status||item.status||fields['流程']||fields['维护实施状态'])||'待处理',
      scope:text(item.building_codes||item.building||item.scope||fields['楼栋'])||scopeLabel.value,
      time:text(item.time||item.start_time||item.updated_at||item.created_at||fields['开始时间']||fields['故障发生时间']),
      progress:count(item.progress_percent??item.progress),dueAt:item.end_time||item.deadline||'',url:url.pathname+url.search,
    };
  });
}
async function loadTasks(){
  taskAbort?.abort();
  const kind=taskKind.value;if(!kind||panelKinds.includes(kind)||props.view==='functions'||disposed||document.hidden){rows.value=[];taskLoading.value=false;return;}
  const abort=new AbortController();taskAbort=abort;taskLoading.value=true;taskError.value='';rows.value=[];taskNote.value='';emptyText.value='当前分类暂无待处理事项';
  const key=props.scope+':'+kind;
  try{
    const existing=cache.get('tasks:'+key);let data:Dict;
    if(existing&&Date.now()-existing.at<30000)data=existing.value;
    else if(NOTICE_IDS.includes(kind))data=await requestJson('/api/workbench?'+new URLSearchParams({scope:props.scope,work_type:kind,month:Number(today.slice(5,7))+'月',sections:'records,ongoing,stats',records_page:'1',records_page_size:'12',ongoing_page:'1',ongoing_page_size:'12'}),{signal:abort.signal,timeoutMs:15000});
    else if(kind==='repair_management')data=await requestJson('/api/repair-management/status?'+new URLSearchParams({scope:props.scope,state:'all',period:'all',limit:'12',offset:'0'}),{signal:abort.signal,timeoutMs:15000});
    else if(kind==='event')data=await requestJson('/api/events/monthly?'+new URLSearchParams({scope:props.scope,month:today.slice(0,7)}),{signal:abort.signal,timeoutMs:15000});
    else if(kind==='learning'){
      const boot=await requestJson('/api/learning/bootstrap',{signal:abort.signal,timeoutMs:15000});
      data=boot.self_person?.id?{...(await requestJson('/api/learning/profile?'+new URLSearchParams({scope:boot.self_scope,person_id:boot.self_person.id,period:'7'}),{signal:abort.signal,timeoutMs:15000})),self_scope:boot.self_scope}: {unlinked:true};
    }else data=await requestJson('/api/daily-tasks?'+new URLSearchParams({scope:props.scope,date:today}),{signal:abort.signal,timeoutMs:15000});
    if(abort.signal.aborted)return;
    if(data.source_snapshot_ready===false)throw new Error('本地资料尚未准备完成，请稍后重试。');
    if(NOTICE_IDS.includes(kind)){
      const active=items(data.ongoing);const linked=new Set(active.flatMap((r:Dict)=>[r.source_record_id,r.record_id,r.active_item_id].filter(Boolean)));
      const pending=items(data.records).filter((r:Dict)=>{const status=text(r.source_progress||r.source_status||r.work_summary?.status||r.status||r.display_fields?.['维护实施状态']||r.display_fields?.['变更进度']);return !linked.has(r.record_id)&&!(/已结束|正常结束|延期结束|闭环|已完成|作废|删除/.test(status)&&!status.includes('未结束'));});
      rows.value=[...toRows(active,kind),...toRows(pending,kind)].slice(0,12);taskNote.value=`当前显示 ${rows.value.length} 条`;
    } else if(kind==='learning'){
      const total=count(data.today_summary?.assigned),done=count(data.today_summary?.task_answered);
      if(total && done!==null && done<total)rows.value=[{key:'self-learning',title:'今日学练',status:done?'答题中':'未开始',scope:data.self_scope,time:`已答 ${done} / ${total} 题`,progress:null,url:'/learning?view=practice'}];
      emptyText.value=data.unlinked?'当前账号尚未关联个人学练身份':total&&done===total?'今日学练已完成':total===0?'今日暂无已领取题单': '今日学练进度暂未返回';
      taskNote.value='仅当前登录人员的题单';
    } else if(kind==='event'){rows.value=toRows(items(data.records).filter((r:Dict)=>eventStatusTone(r.status)==='processing'),kind).slice(0,12);taskNote.value=`本月处理中 · 显示 ${rows.value.length} 条`;}
    else if(kind==='daily'){const tasks=items(data.categories).flatMap((c:Dict)=>items(c.tasks)).filter((r:Dict)=>r.status_tone!=='completed');rows.value=toRows(tasks,kind).slice(0,12);taskNote.value=`今日任务 · 显示 ${rows.value.length} 条`;}
    else{rows.value=toRows(items(data.records),kind);taskNote.value=`未开始与维修中 · 显示 ${rows.value.length} 条`;}
    cache.set('tasks:'+key,{at:Date.now(),value:data});
  }catch(e:any){if(!abort.signal.aborted)taskError.value=e.message||'此分类读取失败，不能判断为零条。';}
  finally{if(taskAbort===abort)taskLoading.value=false;}
}
function invalidate(){cache.clear();void loadTasks();}
function refresh(){window.dispatchEvent(new Event('clipflow-business-changed'));}
function visibility(){if(document.hidden){taskAbort?.abort();}else{void loadTasks();}}
function resize(){compact.value=window.innerWidth<800;}
watch(taskTabs,tabs=>{if(!tabs.some(t=>t.id===taskKind.value))taskKind.value=tabs.find(t=>t.id===props.initialKind)?.id||tabs[0]?.id||'';},{immediate:true});
watch(()=>props.initialKind,kind=>{if(taskTabs.value.some(t=>t.id===kind))taskKind.value=kind!;});
watch(()=>[taskKind.value,props.scope,props.view],()=>void loadTasks(),{flush:'post'});
onMounted(()=>{void loadTasks();window.addEventListener('clipflow-business-changed',invalidate);window.addEventListener('resize',resize);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;taskAbort?.abort();window.removeEventListener('clipflow-business-changed',invalidate);window.removeEventListener('resize',resize);document.removeEventListener('visibilitychange',visibility);});
</script>
