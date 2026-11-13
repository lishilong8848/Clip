<template>
  <aside ref="sidebar" class="v2-sidebar v2-rail" :data-collapsed="collapsed" role="navigation" aria-label="功能导航" @mouseenter="enter" @mouseleave="leave" @focusin="focusIn" @focusout="focusOut" @keydown.esc="collapseRail">
    <div class="v2-rail-brand">
      <button type="button" :aria-label="collapsed?'展开导航':'灯塔首页'" @click="collapsed?emit('update:collapsed',false):choose('home')"><img src="/assets/vnet-logo.png" alt="VNET" /><strong class="v2-rail-label">灯塔</strong></button>
      <button v-if="!collapsed" type="button" class="v2-rail-collapse" aria-label="收起导航" @click="collapseRail"><PanelLeftClose :size="17" aria-hidden="true" /></button>
    </div>
    <div ref="menuScroll" class="v2-rail-scroll">
      <div v-for="group in groups" :key="group.key" class="v2-rail-section">
        <button type="button" class="v2-rail-group" :data-group="group.key" :data-current="currentGroup?.key === group.key"
          :aria-label="group.label" :title="collapsed ? group.label : undefined"
          :aria-current="!group.branches && selected === group.key ? 'page' : undefined"
          :aria-expanded="group.branches ? !collapsed && openGroups.has(group.key) : undefined"
          :aria-controls="group.branches ? 'v2-rail-submenu-' + group.key : undefined"
          @click="openSection(group, $event)">
          <component :is="group.icon" class="v2-rail-group-icon" :size="19" aria-hidden="true" />
          <span class="v2-rail-label">{{ group.label }}</span>
          <ChevronDown v-if="group.branches" class="v2-rail-label v2-rail-chevron" :size="15" aria-hidden="true" />
        </button>
        <div v-if="group.branches" v-show="!collapsed && openGroups.has(group.key)"
          :id="'v2-rail-submenu-' + group.key" class="v2-rail-submenu">
          <div v-for="branch in group.branches" :key="branch.key" class="v2-rail-branch">
            <button v-if="branch.label" type="button" class="v2-rail-branch-toggle" :aria-expanded="openBranches.has(branchKey(group.key, branch.key))"
              :aria-controls="'v2-rail-branch-' + group.key + '-' + branch.key" @click="toggleBranch(group.key, branch.key, $event)">
              {{ branch.label }}<ChevronDown :size="14" class="v2-rail-chevron" aria-hidden="true" />
            </button>
            <div :id="'v2-rail-branch-' + group.key + '-' + branch.key" v-show="!branch.label || openBranches.has(branchKey(group.key, branch.key))">
              <button v-for="entry in branch.entries" :key="entry.key" type="button" class="v2-rail-entry"
                :aria-current="selected === entry.key ? 'page' : undefined" @click="choose(entry.key)">
                <component :is="entry.icon" v-if="entry.icon" :size="16" aria-hidden="true" />
                <span>{{ entry.label }}</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
    <footer class="v2-rail-footer"><slot name="footer" /></footer>
  </aside>
</template>
<script setup lang="ts">
import {computed,nextTick,onMounted,onBeforeUnmount,reactive,ref,watch} from 'vue';
import {ChevronDown,PanelLeftClose} from 'lucide-vue-next';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{collapsed:boolean;groups:SidebarGroup[];selected:string;holdOpen?:boolean}>();
const emit=defineEmits<{'update:collapsed':[value:boolean];select:[value:string];'dismiss-popovers':[]}>();
const sidebar=ref<HTMLElement>(),menuScroll=ref<HTMLElement>();
const openGroups=reactive(new Set<string>()),openBranches=reactive(new Set<string>());
const currentGroup=computed(()=>props.groups.find(group=>group.key===props.selected ||
  group.branches?.some(branch=>branch.entries.some(entry=>entry.key===props.selected))));
const currentBranch=computed(()=>currentGroup.value?.branches?.find(branch=>branch.entries.some(entry=>entry.key===props.selected)));
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
async function openSection(group:SidebarGroup,event:MouseEvent){
  if(!group.branches){choose(group.key);return;}
  const button=event.currentTarget as HTMLButtonElement;
  if(!props.collapsed && openGroups.has(group.key)){
    openGroups.delete(group.key);
    await nextTick();
    keepRowVisible(button);
    return;
  }
  openGroups.add(group.key);
  emit('update:collapsed',false);
  await nextTick();
  const firstItem=button.nextElementSibling?.querySelector<HTMLElement>('button');
  if(event.detail===0)firstItem?.focus({preventScroll:true});
  keepRowVisible(firstItem || button);
}
function branchKey(group:string,branch:string){return group+':'+branch;}
async function toggleBranch(group:string,branch:string,event:MouseEvent){
  const key=branchKey(group,branch),button=event.currentTarget as HTMLButtonElement;
  if(openBranches.delete(key))return;
  openBranches.add(key);
  await nextTick();
  keepRowVisible(button.nextElementSibling?.querySelector<HTMLElement>('button') || button);
}
function collapseRail(){
  const submenu=document.activeElement?.closest('.v2-rail-submenu');
  if(submenu){
    (submenu.previousElementSibling as HTMLElement | null)?.focus({preventScroll:true});
  } else if(document.activeElement?.classList.contains('v2-rail-collapse')){
    sidebar.value?.querySelector<HTMLElement>('.v2-rail-brand button')?.focus();
  }
  emit('update:collapsed',true);
}
function keepRowVisible(row?:HTMLElement | null){
  const scroll=menuScroll.value;
  if(!scroll||!row?.getClientRects().length)return;
  const viewport=scroll.getBoundingClientRect(),bounds=row.getBoundingClientRect();
  if(bounds.top<viewport.top)scroll.scrollTop+=bounds.top-viewport.top;
  else if(bounds.bottom>viewport.bottom)scroll.scrollTop+=bounds.bottom-viewport.bottom;
}
function revealCurrentSection(){
  const group=currentGroup.value,branch=currentBranch.value;
  if(group?.branches)openGroups.add(group.key);
  if(group && branch?.label)openBranches.add(branchKey(group.key,branch.key));
}
watch([()=>props.selected,()=>currentGroup.value?.key,()=>currentBranch.value?.key],revealCurrentSection,{immediate:true});
watch(()=>props.collapsed,value=>{
  if(!value)return;
  emit('dismiss-popovers');
});
watch([()=>props.collapsed,()=>props.selected],async()=>{
  if(props.collapsed)return;
  await nextTick();
  keepRowVisible(menuScroll.value?.querySelector<HTMLElement>('[aria-current="page"]'));
},{immediate:true});
</script>
