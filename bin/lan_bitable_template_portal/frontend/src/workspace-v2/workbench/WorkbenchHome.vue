<template>
  <div class="wb-home">
    <a class="wb-skip" href="#wb-content" @click.prevent="focusContent">跳到工作台内容</a>
    <WorkbenchDailyLayout :name="String(user.name || '')" :avatar="user.avatar_url" :clock="clock" :links="searchLinks || shortcuts" :tasks="allTasks"
      :unread="unreadTasks" :metrics="metrics" :agenda-scopes="agendaScopes" :calendar-scope="dataScope"
      :calendar-identity="accessKey" @open="emit('open',$event)" @unread="showUnread" @task="openItem">
    <header class="wb-identity">
      <div class="wb-identity-copy">
        <span>{{ user.name || '当前账号' }}<template v-if="role!=='personal' || accountLevel"> · {{ roleLabel }}</template><template v-if="shift"> · {{ shift }}班</template></span>
      </div>
      <label v-if="scopeOptions.length>1" class="wb-scope">
        <span>工作范围</span>
        <select :value="selectedScope" @change="switchScope">
          <option v-for="option in scopeOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
        </select>
      </label>
      <span v-else>{{ scopeOptions[0]?.label }}</span>
      <nav v-if="quickActions.length" class="desk-quick-actions" aria-label="常用业务">
        <button v-for="entry in quickActions" :key="entry.id" type="button" class="wb-link" data-v2-navigation @click="openShortcut(entry)">{{ entry.label }}</button>
      </nav>
      <button v-if="mainAction" type="button" class="wb-primary" data-v2-navigation @click="act(mainAction)">
        {{ mainAction.label }} <ArrowRight :size="16" aria-hidden="true" />
      </button>
    </header>

    <div id="wb-content" ref="content" class="wb-grid wb-module-grid" tabindex="-1">
      <section class="wb-todo-panel desk-queue wb-panel" aria-labelledby="wb-todo-title">
        <header class="wb-todo-toolbar">
          <h2 id="wb-todo-title">待办阅览 <span v-if="matchedTasks.length" class="wb-task-count">{{ incomplete ? '已读取 ' : '' }}{{ matchedTasks.length }} 项</span></h2>
          <button type="button" class="wb-link" :disabled="allLoading" @click="refreshHome"><RefreshCw :size="15" aria-hidden="true" />刷新</button>
          <div class="wb-task-filters">
            <label class="desk-task-search"><Search :size="15" aria-hidden="true" /><span class="wb-sr-only">搜索待办</span><input v-model.trim="taskSearch" type="search" placeholder="搜索名称、楼栋或状态" /></label>
            <label><span class="wb-sr-only">业务分类</span><select v-model="taskKind">
              <option value="">全部业务</option><option v-for="source in queueSources" :key="source.id" :value="source.id">{{ source.label }}</option>
            </select></label>
          </div>
        </header>
        <div class="desk-task-tabs" aria-label="待办显示范围">
          <button type="button" :aria-pressed="!unreadOnly && !overdueOnly" @click="setTaskView('all')">全部待办</button>
          <button type="button" :aria-pressed="unreadOnly" @click="setTaskView('unread')">未读 <span>{{ unreadTasks.length }}</span></button>
          <button type="button" :aria-pressed="overdueOnly" @click="setTaskView('overdue')">逾期 <span>{{ overdueCount }}</span></button>
        </div>
        <div v-if="taskModules.length" class="wb-task-module-grid">
        <article v-for="module in taskModules" :key="module.id" class="wb-task-module" :class="{'desk-drill-queue':module.id==='drills'}" :data-source="module.id" :aria-label="module.label">
        <h3 v-if="module.id==='drills' && module.rows.length" class="desk-drill-heading">待完成演练 <span class="wb-task-count">{{ module.rows.length }} 项</span></h3>
        <template v-for="group in module.groups" :key="group.id">
        <h4 v-if="group.label" class="wb-task-group">{{ group.label }} <small>显示 {{ group.rows.length }} / {{ group.total }} 项</small></h4>
        <ul class="wb-rows wb-task-list" :aria-label="group.label || module.label">
          <li v-for="item in group.rows" :key="item.key">
            <button type="button" class="wb-task-row" :aria-label="actionLabel(item)+' '+item.title" @click="openItem(item)">
              <span class="desk-module-pill">{{ item.detail && item.kind==='notices' ? item.detail : sourceLabels.get(item.kind) }}</span>
              <span class="wb-row-copy">
                <strong><i v-if="!isRead(item)" class="desk-unread" role="img" aria-label="未读"></i>{{ item.title }}</strong>
                <small>{{ taskScopeLabel(item.scope) }}<template v-if="item.detail && item.kind!=='notices'"> · {{ item.detail }}</template> · <span :class="{'wb-overdue':isOverdue(item)}">{{ isOverdue(item) ? '已逾期 · ' : '' }}{{ item.status }}</span></small>
              </span>
              <span class="wb-task-action"><span v-if="taskWhen(item)">{{ taskWhen(item) }}</span><span>{{ actionLabel(item) }} <ChevronRight :size="15" aria-hidden="true" /></span></span>
            </button>
            <button v-if="canOpenNoticeDeletion(item)" class="wb-link desk-delete" type="button" @click="openItem(item)">删除单据</button>
          </li>
        </ul>
        </template>
        <div v-if="module.hidden || module.more" class="wb-list-footer">
          <button type="button" class="wb-link" :disabled="module.loading && !module.hidden" @click="loadMore(module.id)">显示更多{{ module.label }}</button>
        </div>
        </article>
        </div>
        <p v-if="queueStatus" class="wb-note wb-queue-status" role="status">{{ queueStatus }}</p>
        <div v-if="hiddenTasks || canLoadMore" class="wb-list-footer">
          <button type="button" class="wb-link" :disabled="!taskModules.some(module=>module.hidden || module.more && !module.loading)" @click="loadMore()">显示更多待办</button>
        </div>
        <details v-if="queueIssues.length || missingOwnership" class="desk-queue-health">
          <summary>读取状态 <span v-if="queueIssues.length">{{ queueIssues.length }} 类待重试</span></summary>
          <ul v-if="queueIssues.length">
            <li v-for="source in queueIssues" :key="source.id"><span>{{ source.label }}<small>{{ states[source.id]?.result?.rows.length ? '上次已读取的记录仍可查看' : '暂未读取到明细' }}</small></span>
              <button type="button" class="wb-link" :aria-label="'重试'+source.label" :disabled="states[source.id]?.retrying" @click="load(source.id,true)">{{ states[source.id]?.retrying ? '读取中…' : '重试' }}</button></li>
          </ul>
          <p v-if="missingOwnership" class="wb-note">部分记录的归属待核对。</p>
        </details>
      </section>
    </div>
    <template #overview>
      <div class="wb-dashboard">
      <PeriodOverview v-if="!accountLevel" :role="role" :user="user" :scope="selectedScope" :scopes="scopeOptions" :read-scopes="readScopes" @open="emit('open',$event)" />
      <section v-if="!accountLevel && focusSources.length" class="wb-panel wb-focus" aria-labelledby="wb-focus-title">
        <header class="wb-section-heading"><h2 id="wb-focus-title">{{ recipe.focusTitle }}</h2><span class="wb-note">业务概览</span></header>
        <div class="wb-focus-grid">
          <button v-for="source in focusSources" :key="source.id" type="button" class="wb-focus-card" :disabled="states[source.id]?.status==='deny'" @click="openSource(source)">
            <span>{{ source.label }}</span>
            <strong>{{ total(source.id) ?? '—' }}</strong>
            <small>{{ sourceStatus(source.id) }}<template v-if="source.readOnly"> · 只读</template></small>
          </button>
        </div>
      </section>
        <section v-if="checkScope" ref="checkPanel" class="wb-panel wb-check-panel" tabindex="-1" aria-labelledby="wb-check-title">
          <h2 id="wb-check-title">班前检查</h2>
          <PreShiftCheck :key="checkScope" :user="checkUser" />
        </section>
      <section v-if="!accountLevel" class="wb-panel wb-shortcut-panel" aria-labelledby="wb-shortcut-title">
          <header class="wb-section-heading">
            <h2 id="wb-shortcut-title">快捷入口</h2>
            <button v-if="shortcuts.length>6" type="button" class="wb-link" :aria-expanded="allShortcuts" @click="allShortcuts=!allShortcuts">{{ allShortcuts ? '收起' : '查看全部' }}</button>
          </header>
          <p v-if="layoutState==='stale'" class="wb-note">使用本机偏好或岗位默认设置。<button type="button" class="wb-link" @click="readLayout">重新读取偏好</button></p>
          <p v-if="!shortcuts.length" class="wb-note">此范围暂无可用入口。</p>
          <nav v-else class="wb-shortcuts" aria-label="快捷入口">
            <button v-for="entry in (allShortcuts?shortcuts:shortcuts.slice(0,6))" :key="entry.id" type="button" class="wb-shortcut" data-v2-navigation @click="openShortcut(entry)">
              <component :is="moduleIcons[entry.icon as keyof typeof moduleIcons]" :size="17" aria-hidden="true" />{{ entry.label }}
              <ChevronRight :size="15" aria-hidden="true" />
            </button>
          </nav>
        </section>
      </div>
    </template>
    </WorkbenchDailyLayout>

    <NDrawer :show="!!selectedSource" :width="drawerWidth" :auto-focus="true" :trap-focus="true" block-scroll @update:show="value=>{if(!value)closeDrawer()}">
      <NDrawerContent :title="selectedSource?.label" closable>
        <template v-if="selectedSource">
          <p role="status">{{ sourceStatus(selectedSource.id) }}</p>
          <p v-if="states[selectedSource.id]?.result?.note">{{ states[selectedSource.id]?.result?.note }}</p>
          <NButton v-if="['stale','down'].includes(states[selectedSource.id]?.status || '')" size="small" @click="load(selectedSource.id,true)">重试</NButton>
          <ul class="wb-rows">
            <li v-for="item in states[selectedSource.id]?.result?.rows || []" :key="item.key">
              <span class="wb-row-copy"><strong>{{ item.title }}</strong><small>{{ item.scope }} · {{ item.status }}</small></span>
              <button type="button" class="wb-link" :aria-label="actionLabel(item)+' '+item.title" @click="openItem(item)">{{ actionLabel(item) }}</button>
            </li>
          </ul>
          <NButton v-if="states[selectedSource.id]?.result?.more" size="small" :disabled="states[selectedSource.id]?.status==='loading'" @click="load(selectedSource.id,true,true)">读取更多明细</NButton>
          <NButton v-if="!selectedSource.readOnly && states[selectedSource.id]?.status!=='deny'" data-v2-navigation @click="emit('open',selectedSource.url)">打开原模块</NButton>
        </template>
      </NDrawerContent>
    </NDrawer>
    <NModal v-if="readOnlyTask" :show="true" preset="card" class="wb-readonly-dialog" :title="readOnlyTask.title"
      :mask-closable="true" @update:show="value=>{if(!value)closeReadOnlyTask()}">
      <dl class="wb-detail">
        <dt>业务</dt><dd>{{ readOnlyTask.detail || sourceLabels.get(readOnlyTask.kind) }}</dd>
        <dt>楼栋</dt><dd>{{ taskScopeLabel(readOnlyTask.scope) }}</dd>
        <dt>状态</dt><dd>{{ readOnlyTask.status }}</dd>
        <template v-if="readOnlyTask.noticeWindow?.start || readOnlyTask.time"><dt>开始时间</dt><dd>{{ readOnlyTask.noticeWindow?.start || readOnlyTask.time }}</dd></template>
        <template v-if="readOnlyTask.noticeWindow?.end"><dt>结束时间</dt><dd>{{ readOnlyTask.noticeWindow.end }}</dd></template>
      </dl>
    </NModal>
    <NoticeWorkspace v-if="inlineNotice && inlineNotice.work!=='event'" :key="inlineNotice.url" :url="inlineNotice.url" :scope="inlineNotice.scope"
      :scopes="authorizedScopes" :user="user" active record-only @record-close="closeInlineNotice" />
    <NModal v-if="inlineNotice?.work==='event'" :show="true" preset="card" title="办理事件通告" class="wb-inline-event"
      :mask-closable="false" @update:show="value=>{if(!value)closeInlineNotice()}">
      <EventNoticeBoard :scope="inlineNotice.scope" :focus-event-id="inlineNotice.eventId" active />
    </NModal>
    <NoticeChangeConfirmations v-if="aliFocus" scope="ALL" :focus-id="aliFocus" @close="aliFocus=''; refresh(['ali_confirmation'])" @changed="refresh(['ali_confirmation'])" />
    <NModal v-if="inlineTask" :show="inlineTaskShown" preset="card" :title="inlineTask.item.title" class="wb-inline-task" :closable="false"
      :mask-closable="false" :close-on-esc="false" @update:show="value=>{if(!value)closeInlineTask()}" @after-leave="finishInlineTask">
      <template #header-extra><NButton text aria-label="关闭待办办理" @click="closeInlineTask"><X :size="20" /></NButton></template>
      <ModuleWidget :key="inlineTask.item.key" :item="inlineTask.widget" :initial-url="inlineTask.url"
        :scope="inlineTask.scope" :scopes="authorizedScopes" :user="user" fixed-scope @close="closeInlineTask" />
    </NModal>
  </div>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NButton, NDrawer, NDrawerContent, NModal } from 'naive-ui';
import { ArrowRight, ChevronRight, RefreshCw, Search, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../client';
import { moduleForRoute, moduleIcons, moduleUrl, NOTICE_IDS } from '../modules';
import { mandatoryCheck, widget, type Widget } from '../dashboard/layout';
import { requestEmbeddedChange, runNavigationAction } from '../navigation';
import PreShiftCheck from '../dashboard/PreShiftCheck.vue';
import PeriodOverview from './PeriodOverview.vue';
import WorkbenchDailyLayout from './WorkbenchDailyLayout.vue';
import AsyncPageState from '../../components/AsyncPageState.vue';
import { recipes, workbenchModules, workbenchRole, workbenchScopes, type ScopeOption } from './recipes';
import { scopedUrl, workbenchSources, type Source, type SourceId, type WorkItem } from './sources';
import { useReadOnlyLayout, useWorkbenchReads } from './useWorkbenchReads';
import { deskActionOrder, deskRole, deskSources, deskTaskVisible, inDeskScope } from './deskPolicy';
import { todoTimestamp } from '../businessTodos';
import { eventPeriodRange } from '../eventPeriod';

const props=withDefaults(defineProps<{user:Dict;scopes:ScopeOption[];scope:string;explicitScope?:boolean;searchLinks?:{label:string;url:string}[]}>(),{explicitScope:true});
const emit=defineEmits<{open:[url:string]}>();
const content=ref<HTMLElement>(), checkPanel=ref<HTMLElement>();
const clock=ref(Date.now());
const authorizedScopes=computed(()=>workbenchScopes(props.scopes));
const accessKey=computed(()=>JSON.stringify([props.user.open_id,props.user.union_id,props.user.level,props.user.role,
  props.user.category,props.user.home_scope,props.user.modules,props.user.scope_rules,props.user.impersonation,
  props.user.tool_management_enabled,props.user.tool_management_full_access,props.user.can_manage_accounts,props.user.enabled,props.scopes]));
const concreteScopes=computed(()=>authorizedScopes.value.filter(option=>/^[A-EH]$|^110$/.test(option.value)));
const ownScope=computed(()=>concreteScopes.value.find(option=>option.value===props.user.home_scope)?.value
  || (concreteScopes.value.length===1?concreteScopes.value[0]?.value:'' )||'');
const checkScope=computed(()=>mandatoryCheck(props.user)?ownScope.value||concreteScopes.value[0]?.value||'':'');
const checkUser=computed(()=>props.user.home_scope===checkScope.value?props.user:{...props.user,home_scope:checkScope.value});
const shift=ref('');
const accountLevel=computed(()=>deskRole(props.user));
const globalDesk=computed(()=>accountLevel.value==='L0'||accountLevel.value==='L1');
const role=computed(()=>globalDesk.value?'manager':accountLevel.value==='L2'?'engineer':accountLevel.value==='L3'?'supervisor':
  accountLevel.value==='L4'?'personal':accountLevel.value==='duty'?'duty':workbenchRole(props.user,authorizedScopes.value,Boolean(shift.value)));
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
const dataScope=computed(()=>globalDesk.value
  ?scopeOptions.value.find(option=>composite(option.value))?.value||selectedScope.value:selectedScope.value);
const readScopes=computed(()=>composite(dataScope.value)&&!authorizedScopes.value.some(option=>option.value===dataScope.value)
  ?concreteScopes.value.map(option=>option.value):undefined);
const agendaScopes=computed(()=>globalDesk.value&&composite(dataScope.value)?concreteScopes.value:[]);
const context=computed(()=>JSON.stringify([accessKey.value,role.value,dataScope.value]));
const lazy=(loader:()=>Promise<any>)=>defineAsyncComponent({loader,loadingComponent:AsyncPageState,errorComponent:AsyncPageState,delay:80,timeout:30000});
const NoticeWorkspace=lazy(()=>import('../NoticeWorkspace.vue'));
const EventNoticeBoard=lazy(()=>import('../../components/EventNoticeBoard.vue'));
const NoticeChangeConfirmations=lazy(()=>import('../NoticeChangeConfirmations.vue'));
const ModuleWidget=lazy(()=>import('../ModuleWidget.vue'));
const inlineNotice=ref<{url:string;scope:string;work:string;eventId:string}>();
const inlineTask=ref<{item:WorkItem;widget:Widget;url:string;scope:string}>();
const inlineTaskShown=ref(false);
const readOnlyTask=ref<WorkItem>();
const aliFocus=ref('');

const {layout,state:layoutState,load:readLayout}=useReadOnlyLayout(()=>props.user,accessKey);
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
  (!recipe.value.pending||recipe.value.actions.includes(entry.id))
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
type Action = {id?:string;label:string;url?:string};
const mainAction=computed<Action|undefined>(()=>{
  for (const id of recipe.value.actions) {
    if (id==='pre_shift'&&checkScope.value) return {label:'打开班前检查'};
    if (id==='station'&&scopeOptions.value.some(option=>option.value==='110')&&props.user.modules?.includes('event'))
      return {label:'查看 110kV 站事件',url:scopedUrl('/',{mode:'events',detail:'1',scope:'110'})};
    const entry=shortcuts.value.find(item=>item.id===id);
    if (entry) return {id:entry.id,label:(role.value==='duty'&&entry.id==='notices'?'查看':'打开')+entry.label,url:entry.url};
  }
  return shortcuts.value[0];
});
const quickActions=computed(()=>accountLevel.value?shortcuts.value.filter(entry=>entry.id!==mainAction.value?.id).slice(0,2):[]);
function act(action:Action) {
  if (action.url) openShortcut({id:action.id||'',url:action.url});
  else {
    checkPanel.value?.scrollIntoView({block:'start'});
    checkPanel.value?.focus({preventScroll:true});
  }
}
function focusContent(){content.value?.focus();}
function switchScope(event:Event){
  const select=event.target as HTMLSelectElement, scope=select.value;
  select.value=selectedScope.value;
  if (scope!==selectedScope.value&&scopeOptions.value.some(option=>option.value===scope))
    emit('open',composite(scope)&&!authorizedScopes.value.some(option=>option.value===scope)?'/':scopedUrl('/',{scope}));
}

const pendingSources=computed(()=>accountLevel.value?deskSources[accountLevel.value]:recipe.value.pending);
const sources=computed(()=>workbenchSources({user:props.user,scope:dataScope.value,scopes:authorizedScopes.value,
  selectedScopes:readScopes.value,role:role.value,pending:pendingSources.value,accessLevel:accountLevel.value},async(path,signal)=>{
  const data=await requestJson(path,{signal,cache:'no-store',timeoutMs:12000});
  if (data.source_snapshot_ready===false || data.snapshot?.exists===false) throw new Error('workbench-source-delayed');
  return data;
}));
const {states,load,refresh}=useWorkbenchReads(context,sources);
const allShortcuts=ref(false);
// Completed sources collapse; unavailable or partial reads must stay distinguishable from zero.
const focusSources=computed(()=>orderedSources.value.filter(source=>{
  const state=states.value[source.id];
  return !!state?.result?.rows.length;
}));
const sourceOrder=computed(()=>[...new Set<SourceId>([
  ...(pendingSources.value||[]),...(Array.isArray(preferredTasks.value)?preferredTasks.value:[]),...sources.value.map(source=>source.id),
])].sort((a,b)=>Number(a==='drills')-Number(b==='drills')));
const orderedSources=computed(()=>sourceOrder.value.flatMap(id=>sources.value.filter(source=>source.id===id)));
const sourceLabels=computed(()=>new Map(sources.value.map(source=>[source.id,source.label])));
watch([context,orderedSources],()=>{for(const source of orderedSources.value) void load(source.id);},{immediate:true,flush:'post'});
const taskKind=ref<SourceId|''>(''),taskSearch=ref(''),overdueOnly=ref(false),unreadOnly=ref(false);
const allTasks=computed(()=>orderedSources.value
  .flatMap(source=>(states.value[source.id]?.result?.rows||[]).map(item=>source.readOnly?{...item,readOnly:true}:item))
  .filter(item=>(accountLevel.value!=='L2'||inDeskScope(item,dataScope.value))
    &&deskTaskVisible(accountLevel.value,props.user,item,ownScope.value)));
const missingOwnership=computed(()=>['L2','L3','L4'].includes(accountLevel.value||'')&&orderedSources.value.some(source=>
  ['notices','engineer_mop','repair_management'].includes(source.id) && states.value[source.id]?.result?.rows.some(item=>
    accountLevel.value==='L4' ? !item.actorIds?.length : (accountLevel.value==='L3'||!inDeskScope(item,ownScope.value))&&!item.ownerLevel)));
const week=computed(()=>eventPeriodRange('week','',clock.value));
const inWeek=(stamp:number|undefined)=>stamp!==undefined&&stamp>=week.value.start&&stamp<week.value.end;
const weeklyNotices=ref<number|null>(null);
const weeklyRevision=ref(0);
function refreshHome(){ refresh(); weeklyRevision.value++; }
const refreshWeekly=()=>{weeklyRevision.value++;};
watch([context,()=>week.value.start,weeklyRevision],async(_value,_old,onCleanup)=>{
  weeklyNotices.value=null;
  if(!['L0','L1','L2'].includes(accountLevel.value||'')||!dataScope.value) return;
  const types=['maintenance','change'].filter(type=>props.user.modules?.includes(type));
  if(!types.length)return;
  const controller=new AbortController();
  onCleanup(()=>controller.abort());
  const currentWeek=week.value;
  await nextTick();
  if(controller.signal.aborted)return;
  try {
    const batches=await Promise.all((readScopes.value||[dataScope.value]).flatMap(scope=>currentWeek.months.flatMap(month=>types.map(async work_type=>{
      const batch=await requestJson(scopedUrl('/api/history-summary',{scope,month,work_type}),{signal:controller.signal,timeoutMs:12000});
      if(batch.scope!==scope||batch.month!==month||!Array.isArray(batch.days)||batch.config_missing||batch.warnings?.length) throw new Error('通告统计暂不可用');
      return batch.days.reduce((sum:number,day:Dict)=>{
        if(!inWeek(todoTimestamp(day.date))) return sum;
        const started=day.stats?.started;
        if(!Number.isSafeInteger(started)||started<0) throw new Error('通告统计未核验');
        return sum+started;
      },0) as number;
    }))));
    if(!controller.signal.aborted) weeklyNotices.value=batches.reduce((sum,value)=>sum+value,0);
  } catch { /* The metric remains unavailable when any applicable scope cannot be verified. */ }
},{immediate:true});
const metrics=computed(()=>{
  const metric=(label:string,kinds:SourceId[],predicate:(item:WorkItem)=>boolean=()=>true)=>{
    const available=orderedSources.value.filter(source=>kinds.includes(source.id));
    const loaded=available.some(source=>!!states.value[source.id]?.result);
    const value=allTasks.value.filter(item=>kinds.includes(item.kind)&&predicate(item)).length;
    const unknown=missingOwnership.value&&kinds.some(kind=>['notices','engineer_mop','repair_management'].includes(kind));
    return {label,value:loaded&&(value||!unknown)?value:null,
      partial:unknown||available.some(source=>states.value[source.id]?.status!=='ok'||states.value[source.id]?.result?.more)};
  };
  const all=orderedSources.value.map(source=>source.id);
  const weekly={label:globalDesk.value?'本周变更／维保通告':'本周本楼维保／变更通告',value:accountLevel.value==='L2'&&selectedScope.value!==ownScope.value?null:weeklyNotices.value};
  const handover={label:accountLevel.value==='L3'?'待交接班审核':'交接班审核待办',value:null,note:'暂无可核验的审核数量',
    url:props.user.modules?.includes('handover')?'/?view=handover':undefined};
  switch(accountLevel.value){
    case 'L0': return [{...metric('全域待办总数',all),partial:true},metric('全域未闭环事件',['events']),weekly,metric('账号权限待审批',['accounts'])];
    case 'L1': return [{...metric('全域待办总数',all),partial:true},metric('全域未闭环事件',['events']),weekly,handover];
    case 'L2': return [metric('本楼待办总数',all),metric('本楼未闭环事件',['events']),weekly,metric('在途维修单',['repair_management'])];
    case 'L3': return [metric('本楼待办审核数',all,item=>/待审核|待审批/.test(item.status)),metric('本楼未闭环事件',['events']),
      metric('本楼重保／专项排查进行中',['critical_guard','risk_inspection']),handover];
    case 'L4': return [metric('我的待办任务',all),metric('我参与的在途单据',all,item=>item.ongoing===true||item.kind==='repair_management'),
      metric('我本周轮巡计划',['notices'],item=>item.detail==='轮巡'&&!item.ongoing&&inWeek(todoTimestamp(item.noticeWindow?.start||item.time))),
      metric('我的截止临近事项',all,item=>!!item.dueAt&&item.dueAt>=clock.value&&item.dueAt<=clock.value+3*86400000)];
    case 'duty': return [metric('当班待办确认项',all,item=>item.kind==='ali_confirmation'||/待确认|待核验/.test(item.status)),
      metric('本楼未闭环事件',['events']),metric('当班上下电／检修通告',['notices'],item=>['上下电','检修'].includes(item.detail||'')&&onShift(item)),metric('当班巡检任务',['inspection'])];
    default: return [metric('待办任务',all),metric('未读任务',all,item=>!isRead(item)),metric('未闭环事件',['events']),metric('待跟进维修',['repair_management'])];
  }
});
function onShift(item:WorkItem) {
  const at=new Date(clock.value+8*3600000);
  const hour=at.getUTCHours();
  if(hour<9)at.setUTCDate(at.getUTCDate()-1);
  const day=at.toISOString().slice(0,10), dayShift=hour>=9&&hour<18;
  const start=Date.parse(day+(dayShift?'T09:00:00+08:00':'T18:00:00+08:00'));
  const end=start+(dayShift?9:15)*3600000;
  const time=todoTimestamp(item.noticeWindow?.start||item.time);
  const until=todoTimestamp(item.noticeWindow?.end);
  return time!==undefined&&time<end&&(time>=start||until!==undefined&&until>start);
}
const readStorageKey=computed(()=>{
  const id=props.user.open_id||props.user.union_id;
  return id&&!props.user.impersonation?'clipflow:workbench-read:'+id+':'+dataScope.value:'';
});
const readKeys=ref(new Set<string>());
const readIdentity=(item:WorkItem)=>JSON.stringify([item.kind,item.scope,item.key,item.status,item.time,item.dueAt,item.noticeWindow?.start,item.noticeWindow?.end]);
const isRead=(item:WorkItem)=>readKeys.value.has(readIdentity(item));
const inQueueScope=(row:WorkItem)=>dataScope.value===selectedScope.value||inDeskScope(row,selectedScope.value);
const unreadTasks=computed(()=>allTasks.value.filter(item=>inQueueScope(item)&&!isRead(item)));
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
function showUnread(){
  setTaskView('unread'); taskKind.value=''; taskSearch.value='';
  content.value?.scrollIntoView({block:'start'}); focusContent();
}
function setTaskView(view:'all'|'unread'|'overdue'){
  unreadOnly.value=view==='unread';
  overdueOnly.value=view==='overdue';
}
const modulePageSize=4;
const visibleLimits=ref<Partial<Record<SourceId,number>>>({});
const queueSources=computed(()=>orderedSources.value.filter(source=>states.value[source.id]?.status!=='deny'));
const sourcesForQueue=computed(()=>queueSources.value.filter(source=>!taskKind.value||source.id===taskKind.value));
const isOverdue=(row:WorkItem)=>!!row.dueAt&&row.dueAt<=clock.value;
const overdueCount=computed(()=>allTasks.value.filter(row=>inQueueScope(row)&&isOverdue(row)).length);
const taskScopeLabel=(scope:string)=>/^[A-EH]$/.test(scope)?scope+'楼':scope==='110'?'110站':scope==='ALL'||scope==='CAMPUS'?'园区':scope;
const matchedTasks=computed(()=>{
  const terms=taskSearch.value.toLowerCase().split(/\s+/).filter(Boolean);
  return allTasks.value
    .filter(row=>inQueueScope(row)
      &&(!taskKind.value||row.kind===taskKind.value)&&(!unreadOnly.value||!isRead(row))&&(!overdueOnly.value||isOverdue(row))&&terms.every(term=>
      [row.title,taskScopeLabel(row.scope),row.status,row.detail,sourceLabels.value.get(row.kind)].join(' ').toLowerCase().includes(term)))
    .sort((a,b)=>deskActionOrder(accountLevel.value,a)-deskActionOrder(accountLevel.value,b)
      ||Number(isOverdue(b))-Number(isOverdue(a))||(a.dueAt??Infinity)-(b.dueAt??Infinity)
      ||sourceOrder.value.indexOf(a.kind)-sourceOrder.value.indexOf(b.kind));
});
const taskModules=computed(()=>{
  const groups=new Map<SourceId,Source[]>();
  for (const source of sourcesForQueue.value) {
    const id=role.value==='duty' && ['notices','event_notices'].includes(source.id) ? 'notices' : source.id;
    groups.set(id,[...(groups.get(id)||[]),source]);
  }
  return [...groups].flatMap(([id,sources])=>{
    const rows=matchedTasks.value.filter(row=>sources.some(source=>source.id===row.kind));
    const more=sources.some(source=>states.value[source.id]?.result?.more);
    const loading=sources.some(source=>!states.value[source.id] || states.value[source.id]?.status==='loading');
    if (!rows.length && !more) return [];
    // A combined notice module shares one limit across plan and ongoing subgroups.
    const limit=visibleLimits.value[id]??modulePageSize;
    const notices=role.value==='duty' && id==='notices';
    const subgroups=notices ? [
      {id:'plans',label:'计划通告',rows:rows.filter(row=>!row.ongoing)},
      {id:'ongoing',label:'进行中通告',rows:rows.filter(row=>row.ongoing)},
    ] : [{id,label:'',rows}];
    const populated=subgroups.filter(group=>group.rows.length);
    let remaining=limit;
    const groups=populated.map((group,index)=>{
      // Reserve a row for each later subgroup so both notice states stay discoverable.
      const take=Math.min(group.rows.length,Math.max(0,remaining-(populated.length-index-1)));
      remaining-=take;
      return {...group,total:group.rows.length,rows:group.rows.slice(0,take)};
    });
    const visible=groups.flatMap(group=>group.rows);
    return [{id,label:notices?'通告待办':sources[0]!.label,sources,rows,limit,more,loading,
      hidden:rows.length>visible.length,groups}];
  });
});
const hiddenTasks=computed(()=>taskModules.value.some(module=>module.hidden));
const allLoading=computed(()=>sourcesForQueue.value.some(source=>!states.value[source.id]||states.value[source.id]?.status==='loading'));
const canLoadMore=computed(()=>sourcesForQueue.value.some(source=>states.value[source.id]?.result?.more));
const incomplete=computed(()=>canLoadMore.value||sourcesForQueue.value.some(source=>states.value[source.id]?.status!=='ok'));
const queueIssues=computed(()=>sourcesForQueue.value.filter(source=>{
  const state=states.value[source.id];
  return state?.retrying||state?.status==='stale'||state?.status==='down';
}));
const queueStatus=computed(()=>{
  if (matchedTasks.value.length) return '';
  if (allLoading.value) return '正在读取待办…';
  if (canLoadMore.value) return '当前页没有匹配项，可继续查看。';
  if (queueIssues.value.length) return '部分待办暂未读取，可在读取状态中重试。';
  if (missingOwnership.value) return '暂无归属已确认的待办。';
  if (unreadOnly.value) return '暂无匹配的未读待办。';
  if (taskSearch.value || overdueOnly.value || taskKind.value) return '暂无匹配的待办。';
  return '当前没有未完成的待办。';
});
function loadMore(id?:SourceId){
  for (const module of taskModules.value.filter(module=>!id || module.id===id)) {
    if (!module.hidden) for (const source of module.sources) {
      if (states.value[source.id]?.result?.more) void load(source.id,true,true);
    }
    visibleLimits.value={...visibleLimits.value,[module.id]:module.limit+modulePageSize};
  }
}
watch([taskKind,taskSearch,overdueOnly,unreadOnly],()=>{visibleLimits.value={};});
watch(queueSources,list=>{if(taskKind.value&&!list.some(source=>source.id===taskKind.value))taskKind.value='';});
function total(id:SourceId) {
  const state=states.value[id];
  return state?.status==='ok'&&state.result&&!state.result.more ? state.result.rows.length : null;
}
function sourceStatus(id:SourceId) {
  const state=states.value[id];
  if (!state) return '待加载';
  if (state.status==='loading') return '正在读取…';
  if (state.status==='deny') return '当前无权查看';
  if (state.status==='down') return '读取失败';
  if (state.status==='stale') return state.result?.rows.length?'数据待更新':'等待同步';
  return state.result?.more?'仅显示已读取明细':state.result?.rows.length?'待处理':'暂无待处理';
}
const deadline=(time:number)=>new Date(time).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
function taskWhen(item:WorkItem){
  if(item.dueAt)return '截止 '+deadline(item.dueAt);
  const raw=item.time||item.noticeWindow?.start;
  const stamp=todoTimestamp(raw);
  return stamp&&raw&&!/^\d{4}-\d{2}-\d{2}$/.test(raw)?deadline(stamp):raw;
}
const drawerSource=ref<SourceId>(), drawerWidth=ref(Math.min(window.innerWidth,480));
const selectedSource=computed(()=>sources.value.find(source=>source.id===drawerSource.value));
let trigger:HTMLElement|null=null;
function openSource(source:Source){trigger=document.activeElement as HTMLElement;drawerSource.value=source.id;void load(source.id);}
const readOnly=(item:WorkItem)=>sources.value.some(source=>source.id===item.kind&&source.readOnly);
function actionLabel(item:WorkItem) {
  if (readOnly(item)) return '查看';
  if (item.kind==='accounts'&&globalDesk.value) return '授权审核';
  if (item.kind==='ali_confirmation') return '确认截图';
  if (role.value!=='duty'||item.kind!=='notices') return '办理';
  return item.ongoing?'更新／结束':item.status==='待核验'?'核验':'开始';
}
function canOpenNoticeDeletion(item:WorkItem){
  return !props.user.impersonation&&props.user.can_delete_business===true&&['L0','L3'].includes(accountLevel.value||'')
    && item.kind==='notices'&&item.ongoing===true&&!readOnly(item);
}
function openItem(item:WorkItem){
  if (readOnly(item)) {
    markRead(item);
    if(!drawerSource.value)trigger=document.activeElement as HTMLElement;
    drawerSource.value=undefined;
    readOnlyTask.value=item;
    return;
  }
  const url=new URL(item.url,location.origin);
  const explicitScope=url.searchParams.get('scope');
  if(url.origin!==location.origin || explicitScope&&!authorizedScopes.value.some(option=>option.value===explicitScope))return;
  const module=moduleForRoute(url), work=url.searchParams.get('work_type')||'';
  if(!module)return;
  const scope=explicitScope||authorizedScopes.value.find(option=>option.value===selectedScope.value)?.value||authorizedScopes.value[0]?.value||'';
  runNavigationAction(url.href,()=>{
    markRead(item);
    if (!drawerSource.value) trigger=document.activeElement as HTMLElement;
    drawerSource.value=undefined;
    if(item.kind==='ali_confirmation'){
      aliFocus.value=url.searchParams.get('change_confirmation')||'';
    } else if (item.kind==='notices'||item.kind==='event_notices') {
      inlineNotice.value={url:url.pathname+url.search,scope,work,eventId:url.searchParams.get('event_id')||''};
    } else {
      url.searchParams.set('workbench_return',selectedScope.value);
      inlineTask.value={item,scope,url:url.pathname+url.search,
        widget:widget('module',props.user,{id:'workbench-inline-task',module,title:item.title})};
      inlineTaskShown.value=true;
    }
  });
}
function closeReadOnlyTask(){readOnlyTask.value=undefined;restoreFocus();}
function closeInlineNotice(){
  const events=inlineNotice.value?.work==='event';
  inlineNotice.value=undefined;
  refresh(events?['event_notices','events']:['notices','engineer_mop','repair_management']);
  restoreFocus();
}
function closeInlineTask(){
  const current=inlineTask.value;
  if(current)requestEmbeddedChange(current.widget.id,()=>{
    if(inlineTask.value===current)inlineTaskShown.value=false;
  });
}
async function finishInlineTask(){
  const current=inlineTask.value;
  await nextTick();
  if(!current||inlineTask.value!==current||inlineTaskShown.value)return;
  inlineTask.value=undefined;
  refresh(current.item.kind==='repair_management'?['repair_management','notices']:[current.item.kind]);
  restoreFocus();
}
function openShortcut(entry:{id:string;url:string}){
  if (role.value==='duty'&&entry.id==='notices') {
    content.value?.scrollIntoView({block:'start'});
    content.value?.focus({preventScroll:true});
    return;
  }
  emit('open',entry.url);
}
function restoreFocus(){
  const last=trigger;trigger=null;
  if (last?.isConnected) void nextTick(()=>last.focus({preventScroll:true}));
}
function closeDrawer(){if(!drawerSource.value)return;drawerSource.value=undefined;restoreFocus();}
watch([context,selectedScope],()=>{
  inlineNotice.value=undefined;inlineTaskShown.value=false;inlineTask.value=undefined;readOnlyTask.value=undefined;aliFocus.value='';closeDrawer(); taskKind.value=''; taskSearch.value=''; overdueOnly.value=false; unreadOnly.value=false;
  visibleLimits.value={}; allShortcuts.value=false;
},{flush:'sync'});

let dutyRead:AbortController|undefined;
async function readDuty(){
  dutyRead?.abort();
  const category=props.user.category;
  const canRead=mandatoryCheck(props.user)||['manager','supervisor'].includes(category)||['L0','L1'].includes(props.user.level);
  if (!ownScope.value || !props.user.open_id || ['guest','temporary'].includes(props.user.role) || category==='duty'||!canRead) return;
  const controller=dutyRead=new AbortController(), key=accessKey.value;
  try {
    const data=await requestJson(scopedUrl('/api/pre-shift-checks',{scope:ownScope.value}),{cache:'no-store',signal:controller.signal,timeoutMs:8000});
    if (controller.signal.aborted||key!==accessKey.value) return;
    if (data.scope!==ownScope.value||!data.roster_ready||!Array.isArray(data.tasks)||!Array.isArray(data.definition?.items)) {
      shift.value='';
      return;
    }
    const rosterItems=new Set(data.definition.items.filter((item:Dict)=>item.frequency?.kind==='shift'&&!item.fixed_recipients?.length).map((item:Dict)=>item.id));
    const now=Date.parse(data.server_time);
    const latest=data.tasks.filter((task:Dict)=>{
      const start=Date.parse(task.day+(task.shift==='夜'?'T17:50:00':'T08:00:00')+'+08:00');
      return ['白','夜'].includes(task.shift)&&start<=now&&now<task.deadline*1000
        && task.definition_version===data.definition.version
        && Array.isArray(task.people)&&task.people.some((person:Dict)=>person.open_id===props.user.open_id)
        && Array.isArray(task.items)&&task.items.some((item:Dict)=>rosterItems.has(item.id)
          && (!Array.isArray(item.recipient_ids)||item.recipient_ids.includes(props.user.open_id)));
    }).sort((a:Dict,b:Dict)=>b.due-a.due)[0];
    shift.value=latest?.shift||'';
  } catch {
    if (!controller.signal.aborted&&key===accessKey.value) shift.value='';
  }
}
const resize=()=>{drawerWidth.value=Math.min(window.innerWidth,480);};
let dutyTimer:ReturnType<typeof setInterval>|undefined;
watch(accessKey,()=>{shift.value='';void readDuty();},{immediate:true,flush:'post'});
onMounted(()=>{
  window.addEventListener('resize',resize);
  window.addEventListener('clipflow-business-changed',refreshWeekly);
  dutyTimer=setInterval(()=>{if(!document.hidden){clock.value=Date.now();void readDuty();}},60000);
});
onBeforeUnmount(()=>{
  dutyRead?.abort(); clearInterval(dutyTimer);
  window.removeEventListener('resize',resize);
  window.removeEventListener('clipflow-business-changed',refreshWeekly);
});
</script>
