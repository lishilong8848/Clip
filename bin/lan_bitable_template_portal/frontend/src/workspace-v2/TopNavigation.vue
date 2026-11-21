<template>
  <nav ref="nav" class="v2-topnav" aria-label="功能导航" @keydown.esc="escape" @focusout="onFocusOut">
    <div class="topnav-start">
      <span class="topnav-brand">
        <span class="topnav-mark" aria-hidden="true"><svg width="30" height="30" viewBox="10 9 44 52" fill="currentColor">
          <path d="M20 18h24l-3 9H23l-3-9ZM27 28h10l5 22H22l5-22ZM16 51h32v5H16z"/>
          <circle cx="32" cy="22.5" r="3.5" fill="#dffbed"/>
        </svg></span><strong>灯塔</strong>
      </span>
      <button v-if="hasHome" type="button" class="topnav-home" data-nav-entry="home" data-group="home"
        :aria-current="selected==='home'?'page':undefined" @click="choose('home')">
        <LayoutDashboard :size="17" aria-hidden="true" />个人工作台
      </button>
    </div>
    <div class="topnav-tabs">
      <template v-for="group in sections" :key="group.key">
        <button type="button" class="topnav-tab" :data-group="group.key" :data-nav-entry="group.direct?.key" :aria-label="group.label"
          :title="group.direct?.label || group.label" :aria-description="group.direct?.label"
          :aria-current="group.direct?.key===selected?'page':undefined"
          :data-current="currentGroup===group.key" :aria-expanded="group.branches?opened===group.key:undefined"
          :aria-controls="group.branches?'nav-panel-'+group.key:undefined" @click="toggle(group,$event.detail===0)" @keydown.down="focusPanel(group,$event)">
          <component :is="group.icon" class="topnav-tab-icon" :size="16" aria-hidden="true" />{{ group.label }}<ChevronDown v-if="group.branches" class="topnav-chevron" :size="12" aria-hidden="true" />
        </button>
        <section v-if="group.branches" v-show="opened===group.key" :id="'nav-panel-'+group.key" class="topnav-directory"
          :aria-label="group.label" :style="{'--menu-anchor':anchor+'px','--menu-columns':group.branches.length}">
          <div v-for="branch in group.branches" :key="branch.key" class="topnav-branch">
            <p v-if="branch.label && group.branches.length>1" class="topnav-subtitle">{{ branch.label }}</p>
            <div class="topnav-links">
              <button v-for="entry in branch.entries" :key="entry.key" type="button" :data-nav-entry="entry.key"
                :aria-current="entry.key===selected?'page':undefined" @click="choose(entry.key)">
                <component :is="entry.icon" v-if="entry.icon" :size="15" aria-hidden="true" /><span>{{ entry.label }}</span>
              </button>
            </div>
          </div>
        </section>
      </template>
    </div>
    <div class="topnav-account-tools"><slot name="account" /></div>
  </nav>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {ChevronDown, LayoutDashboard} from 'lucide-vue-next';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{groups:SidebarGroup[];selected:string}>();
const emit=defineEmits<{select:[key:string]}>();
const nav=ref<HTMLElement>(), opened=ref(''), anchor=ref(0);
const sections=computed(()=>props.groups.filter(group=>group.key!=='home'));
const hasHome=computed(()=>props.groups.some(group=>group.key==='home'));
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
watch(sections,groups=>{if(!groups.some(group=>group.key===opened.value&&group.branches))closePanel();});
onMounted(()=>{
  document.addEventListener('pointerdown',outside);
  window.addEventListener('resize',alignPanel);
});
onBeforeUnmount(()=>{
  document.removeEventListener('pointerdown',outside);
  window.removeEventListener('resize',alignPanel);
});
</script>
