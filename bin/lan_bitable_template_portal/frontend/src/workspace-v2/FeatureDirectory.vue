<template>
  <section class="serenity-desk v2-functions" aria-label="功能目录">
    <header class="serenity-heading"><h1>全部功能</h1>
      <label class="serenity-search"><Search :size="16" aria-hidden="true"/><input v-model="search" type="search" aria-label="搜索已授权功能" placeholder="搜索功能" @input="category=''"></label>
    </header>
    <div class="serenity-segment directory-tabs" role="group" aria-label="功能分类">
      <button type="button" :aria-pressed="category===''" @click="category=''">全部</button>
      <button v-for="group in sections" :key="group.key" type="button" :aria-pressed="category===group.key" @click="category=group.key">{{ group.label }}</button>
    </div>
    <div class="directory-results">
      <section v-for="group in resultGroups" :key="group.key" class="directory-section">
        <header><span class="category-symbol"><component :is="group.icon" :size="20" aria-hidden="true"/></span>
          <h2>{{ group.label }}</h2></header>
        <div class="directory-tray">
          <button v-for="item in group.entries" :key="item.key" type="button" :data-nav-entry="item.key" :aria-label="item.label"
            class="directory-link" @click="emit('select',item.key)">
            <component :is="item.icon || group.icon" :size="17" aria-hidden="true"/>
            <span>{{ item.label }}</span><ChevronRight :size="13" aria-hidden="true"/>
          </button>
        </div>
      </section>
    </div>
    <NEmpty v-if="!visible.length" :description="search?'没有匹配的功能':'当前没有已授权功能'"/>
  </section>
</template>
<script setup lang="ts">
import {computed,ref,watch} from 'vue';
import {NEmpty} from 'naive-ui';
import {ChevronRight,Search} from 'lucide-vue-next';
import type {SidebarGroup} from './sidebarSections';
const props=defineProps<{groups:SidebarGroup[]}>();
const emit=defineEmits<{select:[key:string]}>();
const category=ref(''),search=ref('');
const sections=computed(()=>props.groups.filter(group=>group.key!=='home'));
const entries=computed(()=>sections.value.flatMap(group=>{
  const branches=group.direct?[{label:'',entries:[group.direct]}]:group.branches||[];
  return branches.flatMap(branch=>branch.entries.map(entry=>({...entry,group:group.key,
    keywords:[group.label,branch.label,entry.label].join(' ').toLocaleLowerCase()})));
}));
const visible=computed(()=>entries.value.filter(entry=>(!category.value||entry.group===category.value)&&
  entry.keywords.includes(search.value.trim().toLocaleLowerCase())));
const resultGroups=computed(()=>sections.value.map(group=>({...group,entries:visible.value.filter(item=>item.group===group.key)})).filter(group=>group.entries.length));
watch(sections,list=>{if(category.value&&!list.some(group=>group.key===category.value))category.value='';});
</script>
<style scoped>
.directory-results{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:13px;align-items:start}
.directory-section{min-width:0;padding:12px 15px 14px;border-radius:16px;background:var(--soft-face);box-shadow:var(--soft-raised)}
.directory-section header{display:flex;align-items:center;gap:9px;margin:0 0 8px}
.category-symbol{width:35px;height:35px;display:grid;place-items:center;flex:none;background:var(--soft-mint);color:var(--v2-blue);border-radius:12px;box-shadow:var(--soft-key)}
.directory-section:nth-child(even) .category-symbol{background:var(--soft-sky)}
.directory-section h2{margin:0;font:700 18px/1.3 var(--soft-title);color:var(--v2-ink)}
.directory-tray{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,160px),1fr));gap:3px 8px}
.directory-link{min-width:0;display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:7px;align-items:center;text-align:start;padding:7px 5px;border:0;border-bottom:1px solid var(--soft-edge);font:inherit;color:var(--v2-ink);background:transparent;overflow-wrap:anywhere;cursor:pointer}
.directory-link:hover{color:var(--v2-blue);background:var(--soft-field)}.directory-link>svg{color:var(--v2-muted)}
@media(min-width:1500px){.directory-results{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:720px){.directory-results{grid-template-columns:1fr}.directory-section{padding:10px}.directory-tray{grid-template-columns:repeat(2,minmax(0,1fr))}.directory-link{min-height:44px}.directory-tabs{flex-wrap:nowrap;overflow-x:auto}}
</style>
