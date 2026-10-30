<template>
  <nav v-if="selectedNotice" ref="navigation" class="v2-mop-mobile-navigation" aria-label="维护单办理导航" tabindex="-1">
    <div class="v2-mop-mobile-command" data-v2-navigation>
      <button v-if="workOpen" type="button" :disabled="busy" @click="workOpen=false"><ChevronLeft :size="18" />返回列表 / 切换通告</button>
      <button v-else type="button" :disabled="busy" @click="workOpen=true">继续办理当前通告<ChevronRight :size="18" /></button>
      <span v-if="workOpen">{{selectedNotice.status || '状态待核对'}}</span>
    </div>
    <strong :title="String(selectedNotice.title || '')">{{selectedNotice.title || '当前通告'}}</strong>
    <details v-if="workOpen && (selectedNotice.building || selectedNotice.start_time || selectedNotice.end_time)" class="v2-mop-mobile-notice-details">
      <summary>{{selectedNotice.building || '通告信息'}} · 查看时间</summary>
      <p>{{selectedNotice.start_time || '未填开始'}} ~ {{selectedNotice.end_time || '未填结束'}}</p>
    </details>
  </nav>
</template>

<script setup lang="ts">
import { inject, nextTick, onMounted, ref, watch } from 'vue';
import { ChevronLeft, ChevronRight } from 'lucide-vue-next';
import type { Dict } from '../api/client';

const props=defineProps<{selectedNotice:Dict|null;busy:boolean}>();
const workOpen=inject('workspace-v2:mop-work-open',ref(false));
const navigation=ref<HTMLElement>();

async function focusNavigation(){
  if(!matchMedia('(max-width:900px)').matches)return;
  await nextTick();
  navigation.value?.focus({preventScroll:true});
  navigation.value?.scrollIntoView({block:'start',behavior:'auto'});
}
watch(workOpen,focusNavigation);
watch(()=>props.selectedNotice?.notice_key,key=>{if(!key)workOpen.value=false;});
onMounted(()=>{if(workOpen.value)void focusNavigation();});
</script>

<style scoped>
.v2-mop-mobile-navigation{display:none}
@media(max-width:900px){
  .v2-mop-mobile-navigation{display:grid;gap:6px;min-width:0;padding:0 0 10px;border-bottom:1px solid var(--v2-border,#dbe3df);color:var(--v2-ink,#24312e);scroll-margin-top:calc(var(--head-height,68px) + 12px)}
  .v2-mop-mobile-command{display:flex;align-items:center;justify-content:space-between;gap:8px}
  .v2-mop-mobile-command button{display:inline-flex;align-items:center;justify-content:center;gap:5px;min-height:44px;padding:8px 10px;border:1px solid var(--v2-border,#dbe3df);border-radius:6px;background:var(--cf-surface,#fff);color:var(--v2-blue,#0071e3);font:inherit;font-size:14px;cursor:pointer}
  .v2-mop-mobile-command button:disabled{cursor:not-allowed;opacity:.5}
  .v2-mop-mobile-command span{font-size:12px;color:var(--v2-muted,#63736d)}
  .v2-mop-mobile-navigation>strong{font-size:15px;line-height:1.5;overflow-wrap:anywhere}
  .v2-mop-mobile-notice-details{font-size:12px;color:var(--v2-muted,#63736d)}
  .v2-mop-mobile-notice-details summary{min-height:44px;align-content:center;cursor:pointer;width:fit-content}
  .v2-mop-mobile-notice-details p{margin:0 0 6px;line-height:1.5;overflow-wrap:anywhere}
  .v2-mop-mobile-navigation :is(button,summary):focus-visible{outline:2px solid var(--v2-blue,#0071e3);outline-offset:2px}
}
</style>
