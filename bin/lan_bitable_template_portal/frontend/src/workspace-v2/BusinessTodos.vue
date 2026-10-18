<template>
  <section class="v2-business-todos" aria-label="业务待办" data-v2-inline-feedback>
    <header class="v2-todo-toolbar" data-v2-navigation>
      <TodoKindPicker v-model:value="selected" :user="user" :disabled="preferencesBusy" @commit="commitSelection" />
      <NButton quaternary circle aria-label="刷新业务待办" :loading="loading" :disabled="loading||!selected.length" @click="refresh"><RefreshCw :size="17" /></NButton>
    </header>
    <NEmpty v-if="!selected.length" size="small" description="请选择要显示的待办业务" />
    <section v-for="(kind,index) in selected" :key="kind" class="v2-todo-group" :aria-label="label(kind)+'待办'">
      <header><h3><span class="v2-todo-order">{{index+1}}</span>{{label(kind)}}</h3><span v-if="groupRows(kind).length||states[kind]?.ready&&!groupErrors(kind)">{{groupRows(kind).length}} 条<template v-if="hasMore(kind)||groupErrors(kind)">已读取</template></span><span v-else>{{groupErrors(kind)?'待核验':'正在读取'}}</span><NSpin v-if="states[kind]?.loading" :size="13" /></header>
      <NAlert v-if="groupErrors(kind)" type="warning" :bordered="false" role="alert" class="v2-todo-error">{{groupErrors(kind)}}<NButton text :disabled="states[kind]?.loading" @click="loadGroup(kind)">重新读取</NButton></NAlert>
      <NSkeleton v-if="states[kind]?.loading&&!states[kind]?.ready" :repeat="3" height="48px" class="v2-todo-skeleton" />
      <ul v-else class="v2-todo-list">
        <li v-for="row in groupRows(kind)" :key="row.key">
          <button type="button" data-v2-navigation class="v2-todo-row" @click="openRow(row)">
            <span class="v2-todo-row-copy"><strong>{{row.title}}</strong><span><span>{{scopeText(row.scope)}}</span><span v-if="row.detail">{{row.detail}}</span><time v-if="row.time">{{row.time}}</time><time v-if="row.dueAt">截止 {{dateText(row.dueAt)}}</time></span></span>
            <span class="v2-todo-row-state"><NTag size="small" :bordered="false" :type="row.dueAt&&row.dueAt<Date.now()?'warning':'default'">{{row.status}}</NTag><span>办理<ChevronRight :size="15" /></span></span>
          </button>
        </li>
      </ul>
      <p v-if="states[kind]?.ready&&!groupRows(kind).length&&!groupErrors(kind)" class="v2-todo-empty">{{hasMore(kind)?'已读取记录中暂无待办，可继续读取。':states[kind]?.sources.length?'当前没有未完成事项':'当前楼栋未接入此业务'}}</p>
      <footer v-if="hasMore(kind)||groupNotes(kind)"><span>{{groupNotes(kind)}}</span><NButton v-if="hasMore(kind)" size="small" secondary :loading="states[kind]?.loading" @click="loadGroup(kind)">继续读取待办</NButton></footer>
    </section>
    <NModal v-if="active" :show="true" :mask-closable="false" :close-on-esc="false" :auto-focus="false" @update:show="shown=>{if(!shown)closeHandler()}">
      <section class="v2-todo-handler" role="dialog" aria-modal="true" :aria-label="'办理：'+active.title">
        <header><div><small>{{label(active.kind)}} · {{scopeText(active.scope)}}</small><h2>{{active.title}}</h2></div><NButton data-v2-navigation quaternary circle aria-label="关闭待办办理" @click="closeHandler"><X :size="22" /></NButton></header>
        <div class="v2-todo-handler-body"><ModuleWidget :key="active.key" :item="handlerWidget!" :user="user" :scopes="scopes" :scope="scope" :initial-url="active.url" /></div>
      </section>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import { computed,defineAsyncComponent,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NEmpty,NModal,NSkeleton,NSpin,NTag } from 'naive-ui';
import { ChevronRight,RefreshCw,X } from 'lucide-vue-next';
import { requestJson,type Dict } from './client';
import { requestEmbeddedChange } from './navigation';
import { widget } from './dashboard/layout';
import { todoCatalog,todoSelection,todoSources,sortTodos,type TodoKind,type TodoRow,type TodoSource } from './businessTodos';
import TodoKindPicker from './TodoKindPicker.vue';
const ModuleWidget=defineAsyncComponent(()=>import('./ModuleWidget.vue'));
const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];scope:string;selectedScopes?:string[];selection?:TodoKind[];widgetId:string;preferencesBusy?:boolean}>();
const emit=defineEmits<{selection:[value:TodoKind[]]}>();
type SourceState=TodoSource & {rows:TodoRow[];error:string;note:string};
type Group={sources:SourceState[];loading:boolean;ready:boolean};
const selected=ref(todoSelection(props.selection,props.user)),states=ref<Partial<Record<TodoKind,Group>>>({}),active=ref<TodoRow|null>(null);
const controllers=new Map<TodoKind,AbortController>();
const loading=computed(()=>Object.values(states.value).some(group=>group.loading));
const handlerWidget=computed(()=>active.value?widget('module',props.user,{id:props.widgetId+'-action',module:active.value.module,title:active.value.title}):null);
const label=(kind:TodoKind)=>todoCatalog.find(item=>item.value===kind)!.label;
const scopeText=(value:string)=>['ALL','CAMPUS',''].includes(value)?'授权范围':/^[ABCDEH]$/.test(value)?value+'楼':value==='110'?'110站':value;
const dateText=(value:number)=>new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false}).format(value);
const groupRows=(kind:TodoKind)=>sortTodos((states.value[kind]?.sources||[]).flatMap(source=>source.rows));
const groupErrors=(kind:TodoKind)=>[...new Set((states.value[kind]?.sources||[]).map(source=>source.error).filter(Boolean))].join('；');
const groupNotes=(kind:TodoKind)=>[...new Set((states.value[kind]?.sources||[]).map(source=>source.note).filter(Boolean))].join('；');
const hasMore=(kind:TodoKind)=>states.value[kind]?.sources.some(source=>source.more)===true;
let disposed=false,timer=0;
function commitSelection(){if(JSON.stringify(selected.value)!==JSON.stringify(todoSelection(props.selection,props.user)))emit('selection',[...selected.value]);}
function makeGroup(kind:TodoKind):Group {return {sources:todoSources(kind,props,(path,signal)=>requestJson(path,{signal,cache:'no-store'})).map(source=>({...source,rows:[],error:'',note:''})),loading:false,ready:false};}
async function loadGroup(kind:TodoKind,reset=false){
  if(disposed||document.hidden||!selected.value.includes(kind))return;
  if(states.value[kind]?.loading&&!reset)return;
  controllers.get(kind)?.abort();const controller=new AbortController();controllers.set(kind,controller);
  const previous=states.value[kind];if(reset||!previous){const next=makeGroup(kind);next.ready=previous?.ready||false;for(const source of next.sources)source.rows=previous?.sources.find(old=>old.id===source.id)?.rows||[];states.value[kind]=next;}
  const group=states.value[kind]!;group.loading=true;
  await Promise.all(group.sources.filter(source=>source.more||source.error).map(async source=>{
    try{
      const through=reset?Math.max(1,previous?.sources.find(old=>old.id===source.id)?.page||0):source.page+1;
      let page=source.page,more=true,collected=page?source.rows:[],note=source.note;
      while(more&&page<through){const result=await source.load(page+1,controller.signal);if(disposed||controller.signal.aborted)return;collected=sortTodos([...collected,...result.rows]);page++;more=result.more;note=result.note||'';}
      source.rows=collected;source.page=page;source.more=more;source.error='';source.note=note;
    }catch(error){if(!disposed&&!controller.signal.aborted)source.error=error instanceof Error?error.message:'待办读取失败';}
  }));
  if(!disposed&&!controller.signal.aborted){group.ready=true;group.loading=false;}
}
function refresh(){for(const kind of selected.value)void loadGroup(kind,true);}
function openRow(row:TodoRow){active.value=row;}
function closeHandler(){if(!active.value)return;const kind=active.value.kind;requestEmbeddedChange(props.widgetId+'-action',()=>{active.value=null;void loadGroup(kind,true);});}
function visibility(){if(!document.hidden)refresh();else for(const controller of controllers.values())controller.abort();}
watch(()=>props.selection,value=>{selected.value=todoSelection(value,props.user);},{deep:true});
watch(()=>JSON.stringify([props.scope,props.user.open_id,props.user.modules,props.scopes,props.selectedScopes]),()=>{
  for(const controller of controllers.values())controller.abort();controllers.clear();states.value={};active.value=null;selected.value=todoSelection(props.selection,props.user);refresh();
});
watch(selected,kinds=>{for(const [kind,controller] of controllers)if(!kinds.includes(kind)){controller.abort();controllers.delete(kind);delete states.value[kind];}for(const kind of kinds)if(!states.value[kind])void loadGroup(kind);},{immediate:true,deep:true});
onMounted(()=>{window.addEventListener('clipflow-business-changed',refresh);document.addEventListener('visibilitychange',visibility);timer=window.setInterval(()=>{if(active.value&&!document.hidden)void loadGroup(active.value.kind,true);},15000);});
onBeforeUnmount(()=>{disposed=true;clearInterval(timer);for(const controller of controllers.values())controller.abort();window.removeEventListener('clipflow-business-changed',refresh);document.removeEventListener('visibilitychange',visibility);});
</script>
<style>
/* Keep ordered tasks as full-width rows even inside a multi-column dashboard. */
.workspace-v2 .v2-business-todos ul.v2-todo-list{display:block}.workspace-v2 .v2-business-todos ul.v2-todo-list>li{border:0;border-radius:0;overflow:visible}.workspace-v2 .v2-business-todos ul.v2-todo-list>li+li{border-top:1px solid var(--v2-border)}.workspace-v2 .v2-business-todos ul.v2-todo-list button.v2-todo-row{height:auto;padding:12px 2px!important}
.v2-business-todos{min-width:0;color:var(--v2-ink)}.v2-todo-toolbar{display:flex;align-items:center;gap:8px;padding:4px 0 12px}.v2-todo-toolbar>.n-select{flex:1;min-width:0;max-width:460px}.v2-todo-toolbar>.n-button{margin-left:auto;flex:none}.v2-todo-group>header{display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--v2-border);padding:10px 2px;font-size:12px;color:var(--v2-muted)}.v2-todo-group h3{display:flex;align-items:center;gap:8px;font-size:13px;margin:0;font-weight:600;color:var(--v2-ink)}.v2-todo-group>header>span{margin-left:auto}.v2-todo-order{display:grid;place-items:center;width:20px;height:20px;background:var(--v2-bg);border:1px solid var(--v2-border);border-radius:5px;font-size:11px}.v2-todo-list{list-style:none;margin:0;padding:0}.v2-todo-list>li+li{border-top:1px solid var(--v2-border)}.v2-todo-row{display:flex;align-items:center;gap:12px;width:100%;padding:12px 2px!important;min-height:60px;border:0!important;background:transparent!important;text-align:left;cursor:pointer;color:inherit}.v2-todo-row:hover{background:color-mix(in srgb,var(--v2-blue) 5%,transparent)!important}.v2-todo-row:focus-visible{outline:2px solid var(--v2-blue);outline-offset:-2px}.v2-todo-row-copy{flex:1;min-width:0;display:grid;gap:6px}.v2-todo-row-copy>strong{font-size:14px;font-weight:550;line-height:1.6;overflow-wrap:anywhere}.v2-todo-row-copy>span{display:flex;flex-wrap:wrap;gap:4px 10px;font-size:12px;color:var(--v2-muted)}.v2-todo-row-state{display:flex;align-items:center;flex:none;gap:12px}.v2-todo-row-state>span:last-child{display:flex;align-items:center;font-size:12px;color:var(--v2-blue)}.v2-todo-group>footer{display:flex;gap:8px;align-items:center;justify-content:space-between;padding:8px 0 12px;font-size:12px;color:var(--v2-muted)}.v2-todo-empty{font-size:12px;color:var(--v2-muted);padding:10px 2px;margin:0}.v2-todo-error{margin:8px 0}.v2-todo-skeleton{margin:8px 0}.v2-todo-handler{display:flex;flex-direction:column;width:min(1340px,calc(100vw - 32px));height:calc(100dvh - 40px);background:var(--v2-bg);color:var(--v2-ink);border:1px solid var(--v2-border);border-radius:8px;overflow:hidden}.v2-todo-handler>header{display:flex;align-items:center;gap:12px;padding:12px 18px;border-bottom:1px solid var(--v2-border);flex:none}.v2-todo-handler>header>div{flex:1;min-width:0}.v2-todo-handler h2{font-size:16px;line-height:1.5;margin:3px 0 0;overflow-wrap:anywhere}.v2-todo-handler small{font-size:12px;color:var(--v2-muted)}.v2-todo-handler-body{flex:1;min-height:0;overflow:auto;padding:0 16px 16px;overscroll-behavior:contain}.v2-todo-handler-body>.v2-full-module{min-height:100%}
@media(max-width:760px){.v2-todo-row{gap:8px}.v2-todo-row-state{flex-direction:column;align-items:flex-end;gap:7px}.v2-todo-handler{width:100vw;height:100dvh;max-height:100dvh;border-radius:0}.v2-todo-handler>header{padding:10px 12px}.v2-todo-handler h2{font-size:14px}.v2-todo-handler-body{padding:0 8px calc(10px + env(safe-area-inset-bottom))}.v2-todo-toolbar .n-button,.v2-todo-handler>header>.n-button{min-width:44px;min-height:44px}.v2-todo-group>footer{flex-wrap:wrap}}
.workspace-v2 .v2-todo-handler .signature-page.link-mode{position:relative;inset:auto;transform:none;width:100%;height:auto;min-height:0;padding:0;margin:0;overflow:visible;touch-action:auto}.workspace-v2 .v2-todo-handler .signature-page.link-mode .signature-grid,.workspace-v2 .v2-todo-handler .signature-page.link-mode .sign-panel{width:100%;height:auto;min-height:0;box-sizing:border-box}.workspace-v2 .v2-todo-handler .signature-page.link-mode .sign-panel{grid-template-rows:auto minmax(220px,1fr) auto;padding:12px;border-radius:8px}.workspace-v2 .v2-todo-handler .signature-page.link-mode .canvas-wrap{width:100%;height:clamp(220px,40dvh,450px);min-height:220px;max-height:450px;aspect-ratio:auto;touch-action:none}.workspace-v2 .v2-todo-handler .signature-page.link-mode canvas{height:100%;min-height:0}
</style>
