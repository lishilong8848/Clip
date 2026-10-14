<template>
  <aside ref="sidebar" class="v2-sidebar v2-sidebar-improved" :data-collapsed="collapsed" aria-label="功能导航">
    <div class="v2-nav-head">
      <div class="v2-nav-heading"><span v-if="!collapsed">工作导航 <small>{{ itemCount }}</small></span><NButton data-v2-navigation quaternary circle :aria-label="collapsed?'展开导航':'收起导航'" :title="collapsed?'展开导航':'收起导航'" :aria-expanded="!collapsed" @click="$emit('update:collapsed',!collapsed)"><PanelLeftOpen v-if="collapsed" :size="18" /><PanelLeftClose v-else :size="18" /></NButton></div>
      <NButton v-if="collapsed" data-v2-navigation quaternary circle class="v2-nav-search-toggle" aria-label="搜索功能" title="搜索功能" @click="expandSearch"><Search :size="18" /></NButton>
      <NInput v-else ref="input" v-model:value="query" class="v2-nav-search" :theme-overrides="inputTheme" clearable size="small" placeholder="搜索功能" :input-props="{'aria-label':'搜索功能'}" @keydown.esc="query=''" @keydown.enter="chooseOnly"><template #prefix><Search :size="15" /></template></NInput>
    </div>
    <NMenu :collapsed="collapsed" :collapsed-width="64" :collapsed-icon-size="20" :options="filtered" :value="selected" :indent="18" @update:value="choose" />
    <div v-if="query&&!filtered.length" class="v2-nav-empty" role="status"><SearchX :size="22" /><span>未找到相关功能</span><NButton text @click="query=''">清除搜索</NButton></div>

  </aside>
</template>
<script setup lang="ts">
import { computed,nextTick,ref,watch } from 'vue';
import { NButton,NInput,NMenu,type InputInst,type MenuOption } from 'naive-ui';
import { PanelLeftClose,PanelLeftOpen,Search,SearchX } from 'lucide-vue-next';
const props=defineProps<{collapsed:boolean;options:MenuOption[];selected:string}>();
const emit=defineEmits<{'update:collapsed':[value:boolean];select:[value:string]}>();
const query=ref(''),input=ref<InputInst>(),sidebar=ref<HTMLElement>();
const inputTheme={color:'#34413c',colorFocus:'#34413c',border:'1px solid #48584f',borderHover:'1px solid #8db5a1',borderFocus:'1px solid #8db5a1',textColor:'#edf6f0',placeholderColor:'#b1c0b6',caretColor:'#edf6f0',borderRadius:'6px'};
const itemCount=computed(()=>props.options.reduce((n,item)=>n+(item.type==='group'?(item.children?.length||0):1),0));
const filtered=computed<MenuOption[]>(()=>{
  const word=query.value.trim().toLocaleLowerCase();if(!word)return props.options;
  return props.options.flatMap(item=>{
    if(item.type==='group'){
      const groupMatch=String(item.label||'').toLocaleLowerCase().includes(word);
      const children=item.children?.filter(child=>groupMatch||String(child.label||'').toLocaleLowerCase().includes(word))||[];
      return children.length?[{...item,children}]:[];
    }
    return String(item.label||'').toLocaleLowerCase().includes(word)?[item]:[];
  });
});
async function expandSearch(){emit('update:collapsed',false);await nextTick();input.value?.focus();}
function choose(value:string){emit('select',value);}
function chooseOnly(){const choices=filtered.value.flatMap(item=>item.type==='group'?item.children||[]:[item]);if(choices.length===1&&choices[0]?.key)choose(String(choices[0].key));}
watch(()=>props.selected,async()=>{
  query.value='';await nextTick();
  const root=sidebar.value,active=root?.querySelector<HTMLElement>('.n-menu-item-content--selected');
  if(!root||!active)return;
  const rect=root.getBoundingClientRect(),item=active.getBoundingClientRect(),head=root.querySelector('.v2-nav-head')?.clientHeight||0;
  if(item.top<rect.top+head||item.bottom>rect.bottom-12)root.scrollTo({top:root.scrollTop+item.top-rect.top-head-12,behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth'});
});
</script>
<style>
@media(min-width:761px){.workspace-v2 .v2-shell{--nav-width:232px}.workspace-v2 .v2-shell.is-collapsed{--nav-width:64px}.workspace-v2 .v2-sidebar-improved{transition:width 200ms ease-out;background:#26332e;scrollbar-color:#526f5f transparent}.workspace-v2 .v2-main{transition:margin-left 200ms ease-out}}
.v2-nav-head{position:sticky;top:0;z-index:3;background:#26332e;padding:10px 12px 14px;border-bottom:1px solid #ffffff0c;flex:none}.v2-nav-heading{display:flex;align-items:center;justify-content:space-between;min-height:32px;margin-bottom:8px}.v2-nav-heading>span{display:flex;align-items:center;gap:9px;font-size:12px;color:#b8c9bd}.v2-nav-heading small{font-size:11px;color:#b8c9bd;background:#ffffff0b;border-radius:4px;padding:1px 5px;font-variant-numeric:tabular-nums}.v2-nav-heading .n-button,.v2-nav-search-toggle{width:32px;height:32px;flex:none}.v2-nav-search .n-input__prefix{color:#adbdaf}.v2-nav-search .n-base-clear{color:#d1ded5}.v2-nav-empty{display:grid;gap:10px;justify-items:center;color:#b8c9bd;font-size:12px;padding:30px 10px}
.workspace-v2 .v2-sidebar-improved .n-menu{margin-top:8px;margin-bottom:12px}.workspace-v2 .v2-sidebar-improved .n-menu-item{height:38px;margin-top:3px}.workspace-v2 .v2-sidebar-improved .n-menu-item-content{border-radius:6px;font-size:13px}.workspace-v2 .v2-sidebar-improved .n-menu-item-content__icon{margin-right:11px!important}.workspace-v2 .v2-sidebar-improved .n-menu-item-group-title{font-size:11px;height:31px;line-height:31px;padding-left:20px!important;margin-top:9px;color:#afc1b4!important}.workspace-v2 .v2-sidebar-improved .n-menu-item-content--selected::after{left:0;top:8px;bottom:8px;width:3px;background:#a6e1bd}.workspace-v2 .v2-sidebar-improved .n-menu-item-content--selected::before{background:#385c4a!important}.workspace-v2 .v2-sidebar-improved .n-menu-item-content--selected .n-menu-item-content-header{font-weight:600}
.workspace-v2 .v2-sidebar-improved[data-collapsed="true"] .v2-nav-head{padding:10px 16px}
@media(min-width:761px) and (max-width:1150px){.workspace-v2 .v2-sidebar-improved[data-collapsed="false"] .n-menu-item-content-header{display:inline}.workspace-v2 .v2-sidebar-improved[data-collapsed="false"] .n-menu-item-group-title{display:block}.workspace-v2 .v2-sidebar-improved[data-collapsed="false"] .n-menu-item-content{padding-left:18px!important}}
@media(prefers-reduced-motion:reduce){.workspace-v2 .v2-sidebar-improved,.workspace-v2 .v2-main{transition:none}}

</style>
