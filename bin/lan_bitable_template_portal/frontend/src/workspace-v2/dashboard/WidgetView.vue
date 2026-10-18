<template>
  <div ref="host" class="v2-widget-content">
  <template v-if="active">
  <template v-if="item.kind==='module'">
    <section v-for="entry in moduleViews" :key="item.module==='notices'?item.id:entry.scope" class="v2-widget-building-section">
      <h3 v-if="moduleViews.length>1">{{entry.label}}</h3>
      <ModuleWidget :item="moduleViews.length>1?{...item,id:item.id+'-'+entry.scope}:item" :user="user" :scopes="scopes" :scope="entry.scope" :fixed-scope="!!selectedScopes&&item.module!=='notices'" :selected-scopes="item.module==='notices'?selectedScopes:undefined" :header-tools-id="moduleViews.length===1?headerToolsId:undefined" />
    </section>
  </template>
  <nav v-else-if="item.kind==='shortcuts'" class="v2-shortcut-list" aria-label="常用操作">
    <NButton v-for="entry in entries" :key="entry.id" quaternary @click="$emit('open',moduleUrl(entry,scope))"><span class="v2-shortcut-icon"><component :is="moduleIcons[entry.icon as keyof typeof moduleIcons]" :size="20" /></span><span>{{ entry.label }}</span><ArrowUpRight :size="16" /></NButton>
    <NEmpty v-if="!entries.length" description="尚未添加常用功能" size="small" />
  </nav>
  <BusinessTodos v-else-if="item.kind==='tasks'&&orderedTodos" :widget-id="item.id" :selection="item.taskKinds" :selected-scopes="selectedScopes" :user="user" :scopes="scopes" :scope="scope" :preferences-busy="preferencesBusy" @selection="$emit('task-kinds',$event)" />
  <TaskWorkspace v-else-if="['tasks','notice_actions','repair_actions'].includes(item.kind)" :user="user" :scopes="scopes" :selected-scopes="selectedScopes" :scope="scope" view="tasks" embedded :action-mode="item.kind!=='tasks'" :presentation="item.presentation" :initial-kind="item.module" @open="$emit('open',$event)" />
  <PreShiftCheck v-else-if="item.kind==='pre_shift'" :user="user" />
  <MetricWidget v-else-if="item.kind==='notices'||item.kind==='events'" :item="item" :user="user" :scope="scope" :selected-scopes="selectedScopes" @open="$emit('open',$event)" />
  <OverviewPanel v-else :kind="item.kind" :scope="scope" :scopes="scopes" :selected-scopes="selectedScopes" :user="user" :personal="item.kind==='learning'" :presentation="item.presentation" @open="$emit('open',$event)" />
  </template>
  <NSkeleton v-else :repeat="3" text />
  </div>
</template>
<script setup lang="ts">
import { computed,defineAsyncComponent,onMounted,onBeforeUnmount,ref } from 'vue';
import { NButton,NEmpty,NSkeleton } from 'naive-ui';
import { ArrowUpRight } from 'lucide-vue-next';
import type { Dict } from '../client';
import { moduleIcons,moduleUrl,navigationModules } from '../modules';
import TaskWorkspace from '../TaskWorkspace.vue';
import BusinessTodos from '../BusinessTodos.vue';
import type {TodoKind} from '../businessTodos';
import OverviewPanel from '../OverviewPanel.vue';
import MetricWidget from './MetricWidget.vue';
import PreShiftCheck from './PreShiftCheck.vue';
import type { Widget } from './layout';
const ModuleWidget=defineAsyncComponent(()=>import('../ModuleWidget.vue'));
const props=defineProps<{item:Widget;user:Dict;scope:string;scopes:{value:string;label:string}[];selectedScopes?:string[];headerToolsId?:string;brief?:boolean;preferencesBusy?:boolean;orderedTodos?:boolean}>();
defineEmits<{open:[url:string];'task-kinds':[value:TodoKind[]]}>();
const entries=computed(()=>props.item.links.flatMap(id=>navigationModules(props.user.modules||[]).filter(entry=>entry.id===id)));
const moduleViews=computed(()=>props.selectedScopes&&props.selectedScopes.length>1&&props.item.module!=='notices'
  ?props.selectedScopes.map(scope=>({scope,label:props.scopes.find(option=>option.value===scope)?.label||scope}))
  :[{scope:props.scope,label:''}]);
const host=ref<HTMLElement>(),active=ref(false);let observer:IntersectionObserver|undefined;
onMounted(()=>{observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){active.value=true;observer?.disconnect();}},{rootMargin:'80px'});if(host.value)observer.observe(host.value);});
onBeforeUnmount(()=>observer?.disconnect());
</script>
<style>
.v2-widget-building-section{min-width:0}.v2-widget-building-section+section{margin-top:18px;padding-top:12px;border-top:1px solid var(--v2-border)}.v2-widget-building-section>h3{font-size:14px;margin:0 0 8px}
</style>
