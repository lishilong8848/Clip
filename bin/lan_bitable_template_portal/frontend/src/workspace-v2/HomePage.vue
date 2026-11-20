<template>
  <section v-if="view==='tasks'" class="v2-todo-standalone"><NAlert v-if="error" type="warning" :bordered="false">{{error}}<NButton v-if="todoPending" text :disabled="saving" @click="saveTodoSelection">重试保存选择</NButton><NButton text :disabled="saving" @click="reload">重新读取布局</NButton></NAlert><BusinessTodos :widget-id="primaryTodo?.id||'workspace-tasks'" :selection="primaryTodo?.taskKinds" :user="user" :scopes="scopes" :scope="scope" :preferences-busy="!ready||loading||saving" @selection="chooseTodos(primaryTodo?.id||'', $event)" /></section>
  <FeatureDirectory v-else-if="view==='functions'" :groups="menuGroups" @select="$emit('selectMenu',$event)" />
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from 'vue';
import { NAlert, NButton, useDialog, useMessage } from 'naive-ui';
import type { Dict } from './client';
import type { SidebarGroup } from './sidebarSections';
import { registerNavigationGuard, requestEmbeddedChange } from './navigation';
import FeatureDirectory from './FeatureDirectory.vue';
import BusinessTodos from './BusinessTodos.vue';
import { todoSelection, type TodoKind } from './businessTodos';
import { defaults, sanitize, layoutKey, widget, type Layout } from './dashboard/layout';
import { useLayoutSync } from './useLayoutSync';

const props=defineProps<{user:Dict;scopes:{value:string;label:string}[];scope:string;view:string;initialKind?:string;menuGroups:SidebarGroup[]}>();
defineEmits<{selectMenu:[key:string]}>();

const dialog=useDialog(), message=useMessage();
const layout=ref<Layout>(defaults(props.user));
const todoPending=ref(false);
const primaryTodo=computed(()=>layout.value.widgets.find(item=>item.kind==='tasks'));
const sync=useLayoutSync({
  user:()=>props.user, page:()=> 'workspace', legacyKey:()=>layoutKey(props.user),
  enabled:()=>props.view==='tasks',
  locked:()=>todoPending.value||Boolean(document.querySelector('.v2-todo-handler')),
  normalize:value=>sanitize(value,props.user,props.scopes.map(s=>s.value)),
  apply:value=>{layout.value=value;todoPending.value=false;},
});
const {loading,saving,error,ready}=sync;
function reload(){
  if(saving.value)return;
  const proceed=()=>requestEmbeddedChange('*',()=>void sync.reload());
  if(todoPending.value)dialog.warning({title:'重新读取账号布局',content:'本次未保存的待办选择将被放弃。',positiveText:'重新读取',negativeText:'继续调整',onPositiveClick:proceed});
  else proceed();
}
async function chooseTodos(id:string,kinds:TodoKind[]){
  if(!ready.value||loading.value||saving.value)return;
  const next=todoSelection(kinds,props.user),item=layout.value.widgets.find(row=>row.id===id&&row.kind==='tasks');
  if(item)item.taskKinds=next;
  else{
    if(layout.value.widgets.length>=16){message.warning('原工作台布局已满，无法添加业务待办组件。');return;}
    layout.value.widgets.push(widget('tasks',props.user,{taskKinds:next}));
  }
  todoPending.value=true;
  await saveTodoSelection();
}
async function saveTodoSelection(){
  try{if(await sync.save(layout.value))todoPending.value=false;}
  catch(e){error.value=(e instanceof Error?e.message:'组件选择保存失败')+'；当前选择已保留，尚未同步到账号。';}
}
const releaseTodoGuard=registerNavigationGuard((_target,proceed)=>{
  if(!todoPending.value)return true;
  if(saving.value){message.warning('正在保存待办选择，请稍候。');return false;}
  dialog.warning({title:'待办选择尚未同步',content:'离开后本次选择可能丢失。',positiveText:'放弃并离开',negativeText:'继续保存',onPositiveClick:()=>{todoPending.value=false;proceed();}});
  return false;
});
function beforeUnload(event:BeforeUnloadEvent){if(saving.value||todoPending.value){event.preventDefault();event.returnValue='';}}
window.addEventListener('beforeunload',beforeUnload);
onBeforeUnmount(()=>{releaseTodoGuard();window.removeEventListener('beforeunload',beforeUnload);});
</script>
