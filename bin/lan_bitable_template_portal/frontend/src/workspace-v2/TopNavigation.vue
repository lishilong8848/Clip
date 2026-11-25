<template>
  <nav ref="nav" class="v2-topnav" aria-label="功能导航" @keydown.esc="escape" @focusout="onFocusOut">
    <div class="topnav-start">
      <button type="button" class="topnav-brand" aria-label="南通灯塔运维平台 · 返回个人工作台" @click="choose('home')">
        <img :src="vnetLogo" alt="世纪互联 VNET"/>
        <span class="topnav-brand-copy"><strong>南通灯塔运维平台</strong><small>园区运维协作工作台</small></span>
      </button>
      <button v-if="hasHome" type="button" class="topnav-home" data-nav-entry="home" data-group="home"
        :aria-current="selected==='home'?'page':undefined" @click="choose('home')">
        <LayoutDashboard :size="17" aria-hidden="true" />个人工作台
      </button>
    </div>
    <div ref="tabs" class="topnav-tabs">
      <button v-for="(group,index) in sections" :key="group.key" type="button" class="topnav-tab" :data-group="group.key" :data-nav-entry="group.direct?.key" :aria-label="tabLabel(group)"
        :data-overflow="index>=visibleCount?true:undefined" :aria-hidden="index>=visibleCount?true:undefined" :tabindex="index>=visibleCount?-1:undefined"
        :title="group.direct?.label || group.label" :aria-description="group.direct?.label"
        :aria-current="group.direct?.key===selected?'page':undefined"
        :data-current="currentGroup===group.key" :aria-expanded="group.branches?opened===group.key:undefined"
        :aria-controls="group.branches?'nav-panel-'+group.key:undefined" @click="toggle(group,$event.detail===0)" @keydown.down="focusPanel(group,$event)"
        @keydown.tab.exact="opened===group.key&&focusPanel(group,$event)">
        <component :is="group.icon" class="topnav-tab-icon" :size="16" aria-hidden="true" />{{ tabLabel(group) }}<ChevronDown v-if="group.branches" class="topnav-chevron" :size="12" aria-hidden="true" />
      </button>
    </div>
    <section v-for="group in sectionPanels" v-show="opened===group.key" :key="group.key" :id="'nav-panel-'+group.key" class="topnav-directory"
      :aria-label="group.label" :style="{'--menu-anchor':anchor+'px','--menu-columns':group.branches?.length}">
      <div v-for="branch in group.branches" :key="branch.key" class="topnav-branch">
        <p v-if="branch.label && (group.branches?.length||0)>1" class="topnav-subtitle">{{ branch.label }}</p>
        <div class="topnav-links">
          <button v-for="entry in branch.entries" :key="entry.key" type="button" :data-nav-entry="entry.key"
            :aria-current="entry.key===selected?'page':undefined" @click="choose(entry.key)">
            <component :is="entry.icon" v-if="entry.icon" :size="15" aria-hidden="true" /><span>{{ entry.label }}</span>
          </button>
        </div>
      </div>
    </section>
    <div class="topnav-account-tools"><slot name="account" /></div>
  </nav>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {ChevronDown, LayoutDashboard} from 'lucide-vue-next';
import vnetLogo from '../assets/vnet-logo-transparent.png?inline';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{groups:SidebarGroup[];selected:string}>();
const emit=defineEmits<{select:[key:string]}>();
const nav=ref<HTMLElement>(), tabs=ref<HTMLElement>(), opened=ref(''), anchor=ref(0),visibleCount=ref(Infinity);
let observer:ResizeObserver|undefined;
const sections=computed(()=>props.groups.filter(group=>group.key!=='home'));
const sectionPanels=computed(()=>sections.value.filter(group=>group.branches));
const hasHome=computed(()=>props.groups.some(group=>group.key==='home'));
const tabLabel=(group:SidebarGroup)=>group.direct?.key==='tool_management'?group.direct.label:group.label;
const currentGroup=computed(()=>sections.value.find(group=>group.direct?.key===props.selected ||
  group.branches?.some(branch=>branch.entries.some(entry=>entry.key===props.selected)))?.key);
function groupButton(key:string){
  return nav.value?.querySelector<HTMLButtonElement>('[data-group="'+key+'"]');
}
function currentPanel(){
  return nav.value?.querySelector<HTMLElement>('#nav-panel-'+opened.value);
}
function alignPanel(){
  const button=groupButton(opened.value);
  if(nav.value && button)anchor.value=button.getBoundingClientRect().left-nav.value.getBoundingClientRect().left;
}
function fitTabs(){
  if(!tabs.value)return;
  const gap=parseFloat(getComputedStyle(tabs.value).columnGap)||0;
  const available=tabs.value.clientWidth;
  let width=0,count=0;
  for(const button of tabs.value.querySelectorAll<HTMLElement>('.topnav-tab')){
    width+=button.getBoundingClientRect().width+(count?gap:0);
    if(width>available)break;
    count++;
  }
  visibleCount.value=count;
  if(opened.value && !sections.value.slice(0,count).some(group=>group.key===opened.value))closePanel();
  alignPanel();
}
function openPanel(group:SidebarGroup){
  opened.value=group.key;
  alignPanel();
}
function toggle(group:SidebarGroup,keyboard=false){
  if(group.direct){choose(group.direct.key);return;}
  if(opened.value===group.key){closePanel();return;}
  if(keyboard)void focusPanel(group);
  else openPanel(group);
}
async function focusPanel(group:SidebarGroup,event?:KeyboardEvent){
  if(!group.branches)return;
  event?.preventDefault();
  openPanel(group);await nextTick();
  if(opened.value!==group.key)return;
  const panel=currentPanel();
  const first=panel?.querySelector<HTMLButtonElement>('button[aria-current=page]') || panel?.querySelector<HTMLButtonElement>('button');
  first?.focus();
}
function closePanel(){opened.value='';}
function choose(key:string){
  if(currentPanel()?.contains(document.activeElement))groupButton(opened.value)?.focus();
  closePanel();
  emit('select',key);
}
function escape(event:KeyboardEvent){
  if(!opened.value)return;
  event.stopPropagation();
  const button=groupButton(opened.value);
  closePanel();
  button?.focus();
}
function outside(event:PointerEvent){if(!nav.value?.contains(event.target as Node))closePanel();}
function onFocusOut(event:FocusEvent){
  const next=event.relatedTarget;
  if(opened.value && next instanceof Node && next!==groupButton(opened.value) && !currentPanel()?.contains(next))closePanel();
}
watch(()=>props.selected,closePanel);
watch(sections,groups=>{if(!groups.some(group=>group.key===opened.value&&group.branches))closePanel();fitTabs();},{flush:'post'});
onMounted(()=>{
  document.addEventListener('pointerdown',outside);
  window.addEventListener('resize',fitTabs);
  if(tabs.value&&typeof ResizeObserver!=='undefined'){
    observer=new ResizeObserver(fitTabs);
    observer.observe(tabs.value);
  }
  fitTabs();
});
onBeforeUnmount(()=>{
  document.removeEventListener('pointerdown',outside);
  window.removeEventListener('resize',fitTabs);
  observer?.disconnect();
});
</script>
