<template>
  <div ref="host" class="v2-widget-content">
  <template v-if="active">
  <ModuleWidget v-if="item.kind==='module'" :item="item" :user="user" :scopes="scopes" :scope="scope" />
  <nav v-else-if="item.kind==='shortcuts'" class="v2-shortcut-list" aria-label="常用操作">
    <NButton v-for="entry in entries" :key="entry.id" quaternary @click="$emit('open',moduleUrl(entry,scope))"><span class="v2-shortcut-icon"><component :is="moduleIcons[entry.icon as keyof typeof moduleIcons]" :size="20" /></span><span>{{ entry.label }}</span><ArrowUpRight :size="16" /></NButton>
    <NEmpty v-if="!entries.length" description="尚未添加常用功能" size="small" />
  </nav>
  <TaskWorkspace v-else-if="['tasks','notice_actions','repair_actions'].includes(item.kind)" :user="user" :scopes="scopes" :scope="scope" view="tasks" embedded :action-mode="item.kind!=='tasks'" :brief="item.kind==='tasks'&&brief" :presentation="item.presentation" :initial-kind="item.module" @open="$emit('open',$event)" />
  <PreShiftCheck v-else-if="item.kind==='pre_shift'" :user="user" />
  <MetricWidget v-else-if="item.kind==='notices'||item.kind==='events'" :item="item" :user="user" :scope="scope" @open="$emit('open',$event)" />
  <OverviewPanel v-else :kind="item.kind" :scope="scope" :scopes="scopes" :user="user" :personal="item.kind==='learning'" :presentation="item.presentation" @open="$emit('open',$event)" />
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
import OverviewPanel from '../OverviewPanel.vue';
import MetricWidget from './MetricWidget.vue';
import PreShiftCheck from './PreShiftCheck.vue';
import type { Widget } from './layout';
const ModuleWidget=defineAsyncComponent(()=>import('../ModuleWidget.vue'));
const props=defineProps<{item:Widget;user:Dict;scope:string;scopes:{value:string;label:string}[];brief?:boolean}>();
defineEmits<{open:[url:string]}>();
const entries=computed(()=>props.item.links.flatMap(id=>navigationModules(props.user.modules||[]).filter(entry=>entry.id===id)));
const host=ref<HTMLElement>(),active=ref(false);let observer:IntersectionObserver|undefined;
onMounted(()=>{observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){active.value=true;observer?.disconnect();}},{rootMargin:'80px'});if(host.value)observer.observe(host.value);});
onBeforeUnmount(()=>observer?.disconnect());
</script>
