<template>
  <nav ref="navigation" class="v2-mop-mobile-navigation" aria-label="维护单办理步骤" tabindex="-1">
    <div class="v2-mop-mobile-command" data-v2-navigation>
      <button type="button" :disabled="busy" :aria-pressed="!workOpen" @click="workOpen=false"><ChevronLeft v-if="workOpen" :size="18" />1 选择通告</button>
      <button type="button" :disabled="busy || !selectedNotice" :aria-pressed="workOpen" @click="workOpen=true">2 填写维护单<ChevronRight :size="18" /></button>
    </div>
    <strong v-if="selectedNotice" :title="String(selectedNotice.title || '')">{{selectedNotice.title || '当前通告'}}</strong>
    <span v-else class="v2-mop-mobile-hint">选择通告后继续选择表格、填写与上传。</span>
    <details v-if="workOpen && selectedNotice" class="v2-mop-mobile-notice-details">
      <summary>{{selectedNotice.building || '通告信息'}} · 查看时间</summary>
      <p>{{selectedNotice.status || '状态待核对'}}</p>
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
  .v2-mop-mobile-command button{flex:1}
  .v2-mop-mobile-command button[aria-pressed="true"]{background:var(--v2-blue,#0071e3);border-color:var(--v2-blue,#0071e3);color:#fff}
  .v2-mop-mobile-hint{font-size:12px;color:var(--v2-muted,#63736d);line-height:1.5}
  .v2-mop-mobile-command span{font-size:12px;color:var(--v2-muted,#63736d)}
  .v2-mop-mobile-navigation>strong{font-size:15px;line-height:1.5;overflow-wrap:anywhere}
  .v2-mop-mobile-notice-details{font-size:12px;color:var(--v2-muted,#63736d)}
  .v2-mop-mobile-notice-details summary{min-height:44px;align-content:center;cursor:pointer;width:fit-content}
  .v2-mop-mobile-notice-details p{margin:0 0 6px;line-height:1.5;overflow-wrap:anywhere}
  .v2-mop-mobile-navigation :is(button,summary):focus-visible{outline:2px solid var(--v2-blue,#0071e3);outline-offset:2px}
}
</style>
