<template>
  <TaskWorkspace v-if="view!=='home'" v-bind="props" :initial-kind="initialKind" @open="$emit('open',$event)" />
  <div v-else class="v2-dashboard">
    <header class="v2-dashboard-heading">
      <div class="v2-dashboard-title"><h1>{{ personaTitles[personaFor(user)] }}</h1><time :datetime="today">{{ today }}</time></div>
      <div class="v2-dashboard-actions">
        <template v-if="editing"><NButton data-v2-navigation @click="cancel">取消</NButton><NButton data-v2-navigation @click="restore"><template #icon><RotateCcw :size="16" /></template>恢复默认</NButton><NButton type="primary" @click="save"><template #icon><Check :size="16" /></template>保存布局</NButton></template>
        <template v-else><NButton secondary aria-label="刷新工作台" @click="refresh"><RefreshCw :size="17" /></NButton><NButton data-v2-navigation size="small" class="v2-customize-button" aria-label="自定义工作台" title="自定义工作台" secondary type="primary" @click="edit"><template #icon><SlidersHorizontal :size="14" /></template>自定义</NButton></template>
      </div>
    </header>
    <MobileBrief v-if="mobile && !editing" :user="user" :scope="scope" @open="open" />
    <NAlert v-if="error" type="warning" :bordered="false" class="v2-layout-alert">{{ error }} <NButton text @click="reload">重新读取布局</NButton></NAlert>
    <Transition name="v2-editor-bar"><div v-if="editing" class="v2-edit-banner"><span><GripVertical :size="17" />个人布局 · 仅影响你的工作台</span><NButton data-v2-navigation secondary type="primary" @click="library=true"><template #icon><Plus :size="16" /></template>添加组件</NButton></div></Transition>
    <WidgetGrid :items="layout.widgets" :editing="editing" @layout="setPositions">
      <template #title="{item}">{{ mobile && !editing && item.title==='待处理事项' ? '需要留意' : item.title }}</template>
      <template #tools="{item}"><span v-if="!editing" class="v2-widget-scope" :class="{'is-specific-scope':item.scope!=='current'}">{{ item.kind==='pre_shift'?'班组协作':item.kind==='learning'?'本人':item.scope==='current'?scopeLabel:scopes.find(s=>s.value===item.scope)?.label }}</span><div v-else class="v2-widget-tools" data-v2-navigation><NButton quaternary circle :aria-label="'上移'+item.title" :disabled="isFirst(item)" @click="move(item,-1)"><ArrowUp :size="15" /></NButton><NButton quaternary circle :aria-label="'下移'+item.title" :disabled="isLast(item)" @click="move(item,1)"><ArrowDown :size="15" /></NButton><NButton quaternary circle :aria-label="'编辑'+item.title" @click="selected=item"><Settings2 :size="16" /></NButton><NButton v-if="item.kind!=='pre_shift'||!mandatoryCheck(user)" quaternary circle :aria-label="'移除'+item.title" @click="remove(item)"><X :size="16" /></NButton></div></template>
      <template #default="{item}"><WidgetView :item="item" :user="user" :scopes="scopes" :scope="item.scope==='current'?scope:item.scope" :brief="mobile && !editing" @open="open" /></template>
    </WidgetGrid>
    <div v-if="!layout.widgets.length" class="v2-dashboard-empty"><LayoutGrid :size="36" /><h2>把常用业务放在这里</h2><NButton data-v2-navigation type="primary" @click="edit();library=true"><template #icon><Plus :size="17" /></template>添加第一个组件</NButton></div>
    <NModal v-model:show="library" preset="card" title="添加组件" class="v2-widget-library" :bordered="false">
      <NInput v-model:value="librarySearch" clearable placeholder="搜索业务或组件样式" :input-props="{'aria-label':'搜索组件'}" class="v2-library-search" />
      <div class="v2-library-grid" data-v2-navigation><NButton v-for="entry in available" :key="entry.kind" class="v2-library-item" @click="add(entry.kind)"><component :is="icons[entry.icon]" :size="26" /><span><strong>{{ entry.title }}</strong><small>{{ entry.description }}</small></span><Plus :size="17" /></NButton></div>
      <NEmpty v-if="!available.length&&!availableTemplates.length&&!availableCharts.length" :description="librarySearch?'未找到匹配组件':'暂无已授权组件'" />
      <template v-if="availableTemplates.length"><h3 class="v2-library-subtitle">更多展示方式</h3><div class="v2-library-grid" data-v2-navigation><NButton v-for="entry in availableTemplates" :key="entry.id" class="v2-library-item v2-motion-template" @click="add(entry.kind,{title:entry.title,presentation:entry.presentation})"><component :is="icons[entry.icon]" :size="26" /><span><strong>{{ entry.title }}</strong><small>{{ entry.description }}</small></span><Plus :size="17" /></NButton></div></template>
      <template v-if="availableCharts.length"><h3 class="v2-library-subtitle">图表模板</h3><div class="v2-chart-template-grid" data-v2-navigation><NButton v-for="entry in availableCharts" :key="entry.id" class="v2-chart-template" @click="add(entry.kind,{title:entry.title,presentation:entry.presentation,display:entry.display,h:6})"><TemplatePreview :kind="entry.preview" /><span class="v2-template-copy"><strong>{{ entry.title }}</strong><Plus :size="16" /><small>{{ entry.description }}</small></span></NButton></div></template>
    </NModal>
    <WidgetEditor :value="selected" :user="user" :scopes="scopes" @close="selected=null" @apply="apply" />
  </div>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,ref } from 'vue';
import { NAlert,NButton,NEmpty,NInput,NModal,useDialog,useMessage } from 'naive-ui';
import { ArrowDown,ArrowUp,BookOpen,ChartColumn,ChartNoAxesCombined,Check,CircleCheck,ClipboardList,Droplets,GripVertical,LayoutGrid,ListTree,PanelsTopLeft,Plus,RefreshCw,RotateCcw,Settings2,SlidersHorizontal,X } from 'lucide-vue-next';
import type { Dict } from './client';
import { beijingDate } from './modules';
import { personaFor,personaTitles } from './persona';
import { registerNavigationGuard } from './navigation';
import TaskWorkspace from './TaskWorkspace.vue';
import WidgetGrid from './dashboard/WidgetGrid.vue';
import WidgetView from './dashboard/WidgetView.vue';
import WidgetEditor from './dashboard/WidgetEditor.vue';
import MobileBrief from './dashboard/MobileBrief.vue';
import TemplatePreview from './dashboard/TemplatePreview.vue';
import { allowedKind,catalog,chartTemplates,motionTemplates,defaults,readLayout,saveLayout,widget,mandatoryCheck,type Layout,type Widget,type WidgetKind } from './dashboard/layout';
const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];scope:string;view:string;initialKind?:string}>();
const emit=defineEmits<{open:[url:string]}>();
const dialog=useDialog(),message=useMessage();
const media=window.matchMedia('(max-width:760px)'),mobile=ref(media.matches);
function resize(){mobile.value=media.matches;}
media.addEventListener('change',resize);
const initial=readLayout(props.user,props.scopes.map(s=>s.value));
let saved=JSON.stringify(initial.layout),raw=initial.raw;
let removeGuard:(()=>void)|undefined;
const layout=ref<Layout>(initial.layout),editing=ref(false),library=ref(false),selected=ref<Widget|null>(null),error=ref(initial.error);
const today=beijingDate(),icons={ClipboardList,LayoutGrid,ChartNoAxesCombined,ChartColumn,BookOpen,Droplets,ListTree,CircleCheck,PanelsTopLeft};
const scopeLabel=computed(()=>props.scopes.find(s=>s.value===props.scope)?.label||props.scope);
const librarySearch=ref('');
const matches=(entry:{title:string;description:string})=>(entry.title+entry.description).toLowerCase().includes(librarySearch.value.trim().toLowerCase());
const available=computed(()=>catalog.filter(c=>allowedKind(c.kind,props.user)&&matches(c)&&(c.kind!=='pre_shift'||!layout.value.widgets.some(w=>w.kind==='pre_shift'))));
const availableTemplates=computed(()=>motionTemplates.filter(c=>allowedKind(c.kind,props.user)&&matches(c)));
const availableCharts=computed(()=>chartTemplates.filter(c=>allowedKind(c.kind,props.user)&&matches(c)));
const ordered=()=>[...layout.value.widgets].sort((a,b)=>a.y-b.y||a.x-b.x);
const isFirst=(item:Widget)=>ordered()[0]?.id===item.id;
const isLast=(item:Widget)=>{const items=ordered();return items[items.length-1]?.id===item.id;};
function releaseGuard(){removeGuard?.();removeGuard=undefined;}
function edit(){editing.value=true;releaseGuard();removeGuard=registerNavigationGuard((_target,proceed)=>{if(JSON.stringify(layout.value)===saved)return true;dialog.warning({title:'布局尚未保存',content:'离开将放弃本次组件调整。',positiveText:'放弃并离开',negativeText:'继续编辑',onPositiveClick:()=>{cancel();proceed();}});return false;});}
function cancel(){layout.value=JSON.parse(saved);editing.value=false;selected.value=null;library.value=false;error.value='';releaseGuard();}
function reload(){const next=readLayout(props.user,props.scopes.map(s=>s.value));layout.value=next.layout;saved=JSON.stringify(next.layout);raw=next.raw;error.value=next.error;editing.value=false;releaseGuard();}
function save(){try{raw=saveLayout(props.user,props.scopes.map(s=>s.value),layout.value,raw);saved=JSON.stringify(layout.value);editing.value=false;error.value='';releaseGuard();message.success('个人工作台已保存');}catch(e){error.value=e instanceof Error?e.message:'无法保存到当前浏览器，请检查存储空间。';}}
function restore(){dialog.warning({title:'恢复角色默认布局',content:'仅重置工作台组件，不会删除业务记录。保存布局后生效。',positiveText:'恢复默认',negativeText:'取消',onPositiveClick:()=>{layout.value=defaults(props.user);}});}
function add(kind:WidgetKind,style:Partial<Widget>={}){const bottom=Math.max(0,...layout.value.widgets.map(w=>w.y+w.h));const next=widget(kind,props.user,{y:bottom,...style});layout.value.widgets.push(next);library.value=false;selected.value=next;}
function remove(item:Widget){if(item.kind==='pre_shift'&&mandatoryCheck(props.user))return;layout.value.widgets=layout.value.widgets.filter(w=>w.id!==item.id);message.info('组件已移除，取消编辑可恢复');}
function apply(item:Widget){layout.value.widgets=layout.value.widgets.map(w=>w.id===item.id?item:w);selected.value=null;}
function move(item:Widget,direction:number){const items=ordered(),index=items.findIndex(w=>w.id===item.id),other=items[index+direction];if(!other)return;layout.value.widgets=layout.value.widgets.map(w=>w.id===item.id?{...w,x:Math.min(other.x,12-w.w),y:other.y}:w.id===other.id?{...w,x:Math.min(item.x,12-w.w),y:item.y}:w);}
function setPositions(items:Widget[]){if(editing.value)layout.value.widgets=items;}
function refresh(){window.dispatchEvent(new Event('clipflow-business-changed'));}
function open(url:string){emit('open',url);}
function beforeUnload(event:BeforeUnloadEvent){if(editing.value&&JSON.stringify(layout.value)!==saved){event.preventDefault();event.returnValue='';}}
window.addEventListener('beforeunload',beforeUnload);
onBeforeUnmount(()=>{releaseGuard();media.removeEventListener('change',resize);window.removeEventListener('beforeunload',beforeUnload);});
</script>
<style>
.v2-widget-library .v2-library-search{margin-bottom:16px}.v2-chart-template-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.v2-chart-template-grid .v2-chart-template{height:auto;min-height:164px;padding:12px;border-radius:8px;text-align:left;white-space:normal}.v2-chart-template .n-button__content{display:flex;flex-direction:column;align-items:stretch;width:100%;gap:12px;min-width:0;white-space:normal}.v2-template-copy{display:grid;grid-template-columns:minmax(0,1fr) 16px;gap:6px;align-items:center}.v2-template-copy strong{font-size:14px;color:#294237}.v2-template-copy small{grid-column:1/-1;font-size:12px;line-height:1.6;color:#61756a;overflow-wrap:anywhere}.v2-chart-template:hover .v2-template-preview{background:#eaf3ee}@media(max-width:480px){.v2-chart-template-grid{grid-template-columns:1fr}.v2-chart-template-grid .v2-chart-template{min-height:150px}}
</style>
