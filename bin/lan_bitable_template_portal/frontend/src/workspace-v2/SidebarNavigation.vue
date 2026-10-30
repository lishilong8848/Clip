<template>
  <aside ref="sidebar" class="v2-sidebar v2-rail" :data-collapsed="collapsed" role="navigation" aria-label="功能导航" @mouseenter="enter" @mouseleave="leave" @focusin="focusIn" @focusout="focusOut" @keydown.esc="emit('update:collapsed',true)">
    <div class="v2-rail-brand">
      <button type="button" :aria-label="collapsed?'展开导航':'灯塔首页'" @click="collapsed?emit('update:collapsed',false):choose('home')"><img src="/assets/vnet-logo.png" alt="VNET" /><strong class="v2-rail-label">灯塔</strong></button>
    </div>
    <div ref="menuScroll" class="v2-rail-scroll">
      <NMenu :options="options" :value="selected" :indent="20" :theme-overrides="{itemHeight:'42px'}" :expanded-keys="collapsed?[]:expandedKeys" :node-props="menuNodeProps" @update:expanded-keys="expandedKeys=$event" @update:value="choose" />
    </div>
    <footer class="v2-rail-footer"><slot name="footer" /></footer>
  </aside>
</template>
<script setup lang="ts">
import {nextTick,onMounted,onBeforeUnmount,ref,watch} from 'vue';
import {NMenu,type MenuOption} from 'naive-ui';
const props=defineProps<{collapsed:boolean;options:MenuOption[];selected:string;holdOpen?:boolean}>();
const emit=defineEmits<{'update:collapsed':[value:boolean];select:[value:string];'dismiss-popovers':[]}>();
const sidebar=ref<HTMLElement>(),menuScroll=ref<HTMLElement>(),expandedKeys=ref<string[]>([]);
const desktop=()=>matchMedia('(min-width:761px) and (hover:hover)').matches;
function enter(){if(desktop())emit('update:collapsed',false);}
function leave(){if(desktop()&&!props.holdOpen)emit('update:collapsed',true);}
let keyboardNavigation=false;
function keyMode(event:KeyboardEvent){if(event.key==='Tab'||event.key.startsWith('Arrow'))keyboardNavigation=true;}
function pointerMode(event:PointerEvent){keyboardNavigation=false;if(!props.holdOpen&&event.target instanceof Node&&!sidebar.value?.contains(event.target))emit('update:collapsed',true);}
onMounted(()=>{window.addEventListener('keydown',keyMode,true);window.addEventListener('pointerdown',pointerMode,true);});
onBeforeUnmount(()=>{window.removeEventListener('keydown',keyMode,true);window.removeEventListener('pointerdown',pointerMode,true);});
function focusIn(){if(keyboardNavigation&&matchMedia('(min-width:761px)').matches)emit('update:collapsed',false);}
function focusOut(){void nextTick(()=>{if(!sidebar.value?.contains(document.activeElement)&&!sidebar.value?.matches(':hover'))leave();});}
watch(()=>props.holdOpen,value=>{if(!value&&!sidebar.value?.matches(':hover'))leave();});
function choose(value:string){emit('select',value);if(!desktop())emit('update:collapsed',true);}
watch(()=>props.collapsed,value=>{if(value)emit('dismiss-popovers');});
function menuNodeProps(option:MenuOption){return {'aria-label':typeof option.label==='string'?option.label:undefined};}
watch(()=>[props.selected,props.collapsed],async()=>{
 if(props.selected.startsWith('settings:')&&!expandedKeys.value.includes('settings'))expandedKeys.value.push('settings');
 if(props.collapsed)return;
 await nextTick();const root=menuScroll.value,active=root?.querySelector<HTMLElement>('.n-menu-item-content--selected');
 if(!root||!active)return;const rect=root.getBoundingClientRect(),item=active.getBoundingClientRect();
 if(item.top<rect.top||item.bottom>rect.bottom)root.scrollTo({top:root.scrollTop+item.top-rect.top-12,behavior:'auto'});
},{immediate:true});
</script>
