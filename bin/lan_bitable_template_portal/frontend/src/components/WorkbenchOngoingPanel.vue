<template>
  <aside class="panel ongoing-panel">
    <div class="panel-head">
      <div>
        <h2><span class="step-badge">3</span>已开始未结束</h2>
        <small>更新、结束、删除和回退都在这里</small>
      </div>
      <div class="panel-head-actions">
        <div class="ongoing-type-filter" aria-label="进行中通告显示范围">
          <button
            type="button"
            :class="{ active: ongoingTypeFilter === 'all' }"
            title="显示当前楼栋所有进行中通告"
            @click="emit('update:ongoingTypeFilter', 'all')"
          >
            全部通告
          </button>
          <button
            type="button"
            :class="{ active: ongoingTypeFilter === 'current' }"
            title="只显示当前通告类型的进行中通告"
            @click="emit('update:ongoingTypeFilter', 'current')"
          >
            当前类型
          </button>
        </div>
        <div class="panel-head-status">
          <b>{{ ongoingCountLabel }}</b>
          <em>{{ ongoingStepText }}</em>
        </div>
      </div>
    </div>
    <div v-if="filteredOngoing.length === 0" class="empty-block">
      <strong>没有进行中的通告</strong>
      <p>{{ ongoingEmptyText }}</p>
    </div>
    <template v-else>
      <div v-if="attentionItems.length" class="ongoing-attention-strip" aria-label="进行中通告注意事项">
        <span v-for="item in attentionItems" :key="item.key" :class="item.tone">
          <b>{{ item.count }}</b>
          <strong>{{ item.label }}</strong>
          <em>{{ item.hint }}</em>
        </span>
      </div>
      <div class="ongoing-list">
        <OngoingNoticeCard
          v-for="item in filteredOngoing"
          :key="ongoingLineKey(item)"
          :item="item"
          :draft="ongoingDraft(item)"
          :title="ongoingTitle(item)"
          :meta="ongoingMeta(item)"
          :compact-summary="ongoingCompactSummary(item)"
          :line-key="ongoingLineKey(item)"
          :undo-line-key="undoLineKey(item)"
          :expanded="isOngoingExpanded(item)"
          :busy="isLineBusy(ongoingLineKey(item))"
          :undo-busy="isLineBusy(undoLineKey(item))"
          :needs-binding="ongoingNeedsBinding(item)"
          :photo-count="ongoingPhotoCount(item)"
          :site-photo-required="ongoingEndRequiresSitePhoto(item)"
          :maintenance-cycle-options="maintenanceCycleOptions"
          :zhihang-records="zhihangRecords"
          :sync-maintenance-visible="sourceWorkTypeForRecord(item) === 'maintenance' && String(item.work_type || '') === 'change'"
          :job-text="jobText"
          :job-class="jobClass"
          :copy-text="jobCopyText(ongoingLineKey(item), ongoingNoticePreviewText(item))"
          :local-remove-allowed="localRemoveAllowed"
          @expand="emit('expand', item)"
          @toggle="emit('toggle', item)"
          @set-edit="(key, value) => emit('set-edit', item, key, value)"
          @bind-zhihang="(recordId) => emit('bind-zhihang', item, recordId)"
          @photo-input="(event) => emit('photo-input', item, event)"
          @photo-paste="(event) => emit('photo-paste', item, event)"
          @remove-photo="(index) => emit('remove-photo', item, index)"
          @send="(action) => emit('send', item, action)"
          @copy-notice="emit('copy-notice', item)"
          @delete="emit('delete', item)"
          @remove-local="emit('remove-local', item)"
          @bind-target="emit('bind-target', item)"
          @apply-undo="emit('apply-undo', item)"
        />
      </div>
    </template>
    <div v-if="recentUndoItems.length || closedSummaryItems.length" class="secondary-panels">
      <button
        v-if="recentUndoItems.length"
        class="secondary-toggle undo"
        :class="{ active: showUndoSection }"
        type="button"
        @click="showUndoSection = !showUndoSection"
      >
        <span>近三天可回退</span>
        <b>{{ recentUndoItems.length }}</b>
      </button>
      <button
        v-if="closedSummaryItems.length"
        class="secondary-toggle closed"
        :class="{ active: showClosedSection }"
        type="button"
        @click="showClosedSection = !showClosedSection"
      >
        <span>今日结束</span>
        <b>{{ closedSummaryItems.length }}</b>
      </button>
    </div>
    <RecentUndoPanel
      v-if="showUndoSection"
      :model-value="undoFilter"
      :items="recentUndoItems"
      :job-text="jobText"
      :job-class="jobClass"
      :is-line-busy="isLineBusy"
      @update:model-value="emit('update:undoFilter', $event)"
      @apply="emit('apply-undo', $event)"
    />
    <ClosedTodayPanel
      v-if="showClosedSection"
      :items="closedSummaryItems"
      :job-text="jobText"
      :job-class="jobClass"
      :is-line-busy="isLineBusy"
      :closed-line-key="closedLineKey"
      :undo-line-key="undoLineKey"
      @apply-undo="emit('apply-undo', $event)"
    />
  </aside>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import ClosedTodayPanel from "./ClosedTodayPanel.vue";
import OngoingNoticeCard from "./OngoingNoticeCard.vue";
import RecentUndoPanel from "./RecentUndoPanel.vue";

type Dict = Record<string, any>;

const props = defineProps<{
  filteredOngoing: Dict[];
  ongoingCountLabel: string;
  ongoingEmptyText: string;
  ongoingTypeFilter: string;
  recentUndoItems: Dict[];
  undoFilter: string;
  closedSummaryItems: Dict[];
  maintenanceCycleOptions: string[];
  zhihangRecords: Dict[];
  ongoingLineKey: (item: Dict) => string;
  undoLineKey: (item: Dict) => string;
  closedLineKey: (item: Dict) => string;
  ongoingDraft: (item: Dict) => Dict;
  ongoingTitle: (item: Dict) => string;
  ongoingMeta: (item: Dict) => string;
  ongoingCompactSummary: (item: Dict) => string;
  isOngoingExpanded: (item: Dict) => boolean;
  isLineBusy: (key: string) => boolean;
  ongoingNeedsBinding: (item: Dict) => boolean;
  ongoingPhotoCount: (item: Dict) => number;
  ongoingEndRequiresSitePhoto: (item: Dict) => boolean;
  sourceWorkTypeForRecord: (item: Dict) => string;
  ongoingNoticePreviewText: (item: Dict) => string;
  jobCopyText: (key: string, text: string) => string;
  jobText: (key: string) => string;
  jobClass: (key: string) => string;
  localRemoveAllowed: boolean;
}>();

const emit = defineEmits<{
  "update:ongoingTypeFilter": [value: string];
  "update:undoFilter": [value: string];
  expand: [item: Dict];
  toggle: [item: Dict];
  "set-edit": [item: Dict, key: string, value: any];
  "bind-zhihang": [item: Dict, recordId: string];
  "photo-input": [item: Dict, event: Event];
  "photo-paste": [item: Dict, event: ClipboardEvent];
  "remove-photo": [item: Dict, index: number];
  send: [item: Dict, action: string];
  "copy-notice": [item: Dict];
  delete: [item: Dict];
  "remove-local": [item: Dict];
  "bind-target": [item: Dict];
  "apply-undo": [item: Dict];
}>();

const showUndoSection = ref(true);
const showClosedSection = ref(false);
const busyCount = computed(() => (
  props.filteredOngoing.filter((item) => (
    props.isLineBusy(props.ongoingLineKey(item)) || props.isLineBusy(props.undoLineKey(item))
  )).length
));
const bindingCount = computed(() => (
  props.filteredOngoing.filter((item) => props.ongoingNeedsBinding(item)).length
));
const photoMissingCount = computed(() => (
  props.filteredOngoing.filter((item) => (
    props.ongoingEndRequiresSitePhoto(item) && props.ongoingPhotoCount(item) === 0
  )).length
));
const ongoingStepText = computed(() => {
  if (!props.filteredOngoing.length) return "无进行中";
  if (busyCount.value) return "后台处理中";
  if (bindingCount.value) return "先关联记录";
  if (photoMissingCount.value) return "结束前补照片";
  return "展开处理";
});
const attentionItems = computed(() => [
  {
    key: "busy",
    label: "处理中",
    hint: "等待后台完成",
    count: busyCount.value,
    tone: "blue",
  },
  {
    key: "binding",
    label: "待关联",
    hint: "先选择对应通告",
    count: bindingCount.value,
    tone: "amber",
  },
  {
    key: "photo",
    label: "缺现场照片",
    hint: "结束前补照片",
    count: photoMissingCount.value,
    tone: "rose",
  },
].filter((item) => item.count > 0));

watch(
  () => props.recentUndoItems.length,
  (count) => {
    showUndoSection.value = count > 0;
  },
  { immediate: true },
);

watch(
  () => props.closedSummaryItems.length,
  (count) => {
    if (!count) showClosedSection.value = false;
  },
);
</script>

<style scoped>
.ongoing-panel {
  position: relative;
  overflow: hidden;
  display: grid;
  align-content: start;
  gap: 14px;
  min-height: 0;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  padding: 14px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
  margin: -14px -14px 0;
  border-radius: 22px 22px 0 0;
  border-bottom: 1px solid #e7f0fb;
  padding: 14px 16px 10px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.98)),
    linear-gradient(90deg, rgba(48, 128, 255, 0.06), transparent 42%);
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.04);
}

.panel-head h2 {
  margin: 0;
  color: #09204a;
  font-size: 16px;
  font-weight: 900;
  letter-spacing: 0;
}

.panel-head small {
  display: block;
  margin-top: 4px;
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.35;
}

.step-badge {
  display: inline-grid;
  place-items: center;
  width: 22px;
  min-width: 22px;
  height: 22px;
  margin-right: 8px;
  border-radius: 8px;
  color: #ffffff;
  font-size: 12px;
  font-weight: 950;
  vertical-align: 1px;
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
  box-shadow: 0 6px 14px rgba(22, 120, 255, 0.18);
}

.panel-head-status {
  flex: 0 0 auto;
  display: grid;
  justify-items: end;
  gap: 4px;
  min-width: 0;
}

.panel-head-status b,
.panel-head-status em {
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 3px 8px;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
  font-size: 12px;
  font-weight: 900;
  font-style: normal;
  line-height: 1.2;
  white-space: nowrap;
}

.panel-head-status em {
  border-color: #d8e5f7;
  background: #ffffff;
  color: #64748b;
  font-size: 11px;
  font-weight: 850;
}

.panel-head-actions {
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
  min-width: 0;
}

.ongoing-type-filter {
  display: inline-flex;
  gap: 3px;
  padding: 3px;
  border: 1px solid #dbe7f5;
  border-radius: 999px;
  background: #f7fbff;
}

.ongoing-type-filter button {
  min-height: 24px;
  border: 0;
  border-radius: 999px;
  padding: 3px 8px;
  background: transparent;
  color: #60728d;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
  cursor: pointer;
}

.ongoing-type-filter button.active {
  color: #fff;
  background: linear-gradient(135deg, #1678ff, #005bd8);
  box-shadow: 0 8px 16px rgba(22, 120, 255, 0.2);
}

.ongoing-list {
  overflow: auto;
  display: grid;
  align-content: start;
  gap: 10px;
  padding-right: 2px;
  scroll-behavior: smooth;
  scrollbar-color: #9cc7ff #eef6ff;
  scrollbar-width: thin;
}

.ongoing-list::-webkit-scrollbar {
  width: 8px;
}

.ongoing-list::-webkit-scrollbar-track {
  border-radius: 999px;
  background: #eef6ff;
}

.ongoing-list::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: linear-gradient(180deg, #9cc7ff, #1678ff);
}

.ongoing-attention-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 7px;
}

.ongoing-attention-strip span {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  grid-template-areas:
    "count label"
    "count hint";
  align-items: center;
  column-gap: 8px;
  min-width: 0;
  min-height: 44px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 7px 9px;
  background: #f8fbff;
  color: #48627f;
}

.ongoing-attention-strip b {
  grid-area: count;
  display: grid;
  place-items: center;
  min-width: 28px;
  height: 28px;
  border-radius: 10px;
  background: #eaf3ff;
  color: #0757d7;
  font-size: 13px;
  font-weight: 950;
}

.ongoing-attention-strip strong,
.ongoing-attention-strip em {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ongoing-attention-strip strong {
  grid-area: label;
  color: #0f2f6a;
  font-size: 12px;
  font-weight: 950;
}

.ongoing-attention-strip em {
  grid-area: hint;
  color: #64748b;
  font-size: 11px;
  font-style: normal;
  font-weight: 850;
}

.ongoing-attention-strip .blue {
  border-color: #bfdbfe;
  background: #eff6ff;
}

.ongoing-attention-strip .amber {
  border-color: #fed7aa;
  background: #fff7ed;
}

.ongoing-attention-strip .amber b {
  background: #ffedd5;
  color: #c2410c;
}

.ongoing-attention-strip .rose {
  border-color: #fecaca;
  background: #fff1f2;
}

.ongoing-attention-strip .rose b {
  background: #ffe4e6;
  color: #be123c;
}

.secondary-panels {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 2px 0 0;
}

.secondary-toggle {
  flex: 1 1 132px;
  min-height: 38px;
  display: inline-flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  border: 1px solid #dbe7f5;
  border-radius: 16px;
  padding: 7px 10px;
  background: rgba(248, 251, 255, 0.92);
  color: #52657f;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
  box-shadow: 0 8px 18px rgba(37, 99, 235, 0.05);
  transition:
    transform 0.16s ease,
    border-color 0.16s ease,
    background 0.16s ease,
    color 0.16s ease;
}

.secondary-toggle:hover {
  border-color: #bdd2f4;
  background: #ffffff;
  transform: translateY(-1px);
}

.secondary-toggle.active {
  border-color: #8dbbfb;
  background: #eff6ff;
  color: #0757d7;
}

.secondary-toggle.undo.active {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #9a3412;
}

.secondary-toggle.closed.active {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.secondary-toggle b {
  flex: 0 0 auto;
  min-width: 24px;
  border-radius: 999px;
  padding: 3px 7px;
  background: rgba(255, 255, 255, 0.78);
  color: currentColor;
  font-size: 11px;
  font-weight: 950;
  text-align: center;
}

.empty-block {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 6px;
  min-height: 140px;
  border-radius: 22px;
  padding: 18px;
  background: rgba(248, 251, 255, 0.94);
  color: #64748b;
  text-align: center;
  line-height: 1.7;
}

.empty-block strong {
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.empty-block p {
  max-width: 300px;
  margin: 0;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.6;
}

@media (max-width: 760px) {
  .ongoing-attention-strip {
    grid-template-columns: 1fr;
  }

  .panel-head,
  .panel-head-actions {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
