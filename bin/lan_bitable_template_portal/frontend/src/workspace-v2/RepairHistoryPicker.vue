<template>
  <NButton data-v2-navigation size="small" secondary :disabled="r.saving||r.binding" aria-haspopup="dialog" :aria-expanded="open" @click="open=true"><History :size="15" />历史跟进 · {{r.v2Ready?r.total:'—'}} 条</NButton>
  <NModal v-model:show="open" preset="card" title="历史跟进" :bordered="false" :style="{width:'min(520px, calc(100vw - 32px))',maxHeight:'calc(100dvh - 32px)'}">
    <section class="v2-repair-history-picker" aria-label="选择历史跟进">
      <NInput :value="r.followupQuery" clearable placeholder="搜索历史跟进" :input-props="{'aria-label':'搜索历史跟进'}" @update:value="r.followupQuery=$event"><template #prefix><Search :size="15" /></template></NInput>
      <NSkeleton v-if="r.loading&&!r.records.length" height="48px" :repeat="2" />
      <NEmpty v-else-if="!r.timelineRecords.length" description="暂无匹配的跟进记录" />
      <div v-else class="v2-repair-history" aria-label="历史跟进记录">
        <button v-for="item in r.timelineRecords" :key="item.record_id" type="button" data-v2-navigation :disabled="r.saving||r.binding" :class="{active:r.editingRecordId===item.record_id}" @click="select(item)">
          <span><strong>{{item.title||'未命名跟进'}}</strong><small>{{r.repairDisplayTime(item.created_time)||'时间未填写'}}<template v-if="r.isOutsideFilterRecord(item)"> · 当前编辑，筛选外</template></small></span><b>{{r.progressLabel(item.progress)}}</b><ChevronRight :size="16" />
        </button>
      </div>
      <NPagination v-if="r.pageCount>1" :page="r.page" :page-count="r.pageCount" :disabled="r.loading||r.saving||r.binding" :page-slot="4" @update:page="value=>r.requestChangePage(value-r.page)" />
    </section>
  </NModal>
</template>
<script setup lang="ts">
import {ref} from 'vue';
import {NButton,NEmpty,NInput,NModal,NPagination,NSkeleton} from 'naive-ui';
import {ChevronRight,History,Search} from 'lucide-vue-next';
const props=defineProps<{r:Record<string,any>}>();
const open=ref(false);
function select(item:any){open.value=false;props.r.requestSelectRecord(item);}
</script>
<style scoped>
.v2-repair-history-picker{--repair-line:var(--v2-border);display:grid;gap:10px;min-width:0;color:var(--v2-ink)}
.v2-repair-history-picker .v2-repair-history{max-height:min(380px,50dvh);overscroll-behavior:contain}
.v2-repair-history-picker .v2-repair-history>button{min-height:48px;background:transparent;color:inherit}
.v2-repair-history-picker .v2-repair-history>button.active,.v2-repair-history-picker .v2-repair-history>button:hover{background:color-mix(in srgb,var(--v2-blue) 10%,transparent)}
.v2-repair-history-picker .v2-repair-history small{color:var(--v2-muted)}
</style>
