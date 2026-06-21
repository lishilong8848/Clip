<template>
  <section class="panel binding-panel">
    <div class="panel-head">
      <div>
        <h2>选择 MOP 表格</h2>
        <p>推荐表格已置顶，选中后直接打开填写。</p>
      </div>
      <span>{{ mopCandidates.length }}</span>
    </div>

    <div v-if="!selectedNotice" class="empty-box">请选择一条维保通告。</div>
    <template v-else>
      <div class="mop-flow-steps" aria-label="MOP填写流程">
        <span class="done"><b>1</b>选通告</span>
        <span :class="{ done: selectedMop }"><b>2</b>选MOP</span>
        <span :class="{ active: selectedMop && !canPreview, done: canPreview }"><b>3</b>选附件</span>
        <span :class="{ active: canPreview }"><b>4</b>打开填写</span>
      </div>

      <article class="selected-notice">
        <div class="selected-notice__top">
          <span>当前通告</span>
          <em>{{ selectedNotice.status || "进行中" }}</em>
        </div>
        <strong>{{ selectedNotice.title }}</strong>
        <p>
          {{ selectedNotice.building || "-" }}
          <template v-if="selectedNotice.start_time || selectedNotice.end_time">
            · {{ selectedNotice.start_time || "未填开始" }} ~ {{ selectedNotice.end_time || "未填结束" }}
          </template>
        </p>
      </article>

      <article class="binding-next-step" :class="nextStepTone">
        <span>下一步</span>
        <div>
          <strong>{{ nextStepTitle }}</strong>
          <small>{{ nextStepText }}</small>
        </div>
      </article>

      <MopSelectedFileCard
        v-if="selectedMop"
        v-model:selected-attachment-token="selectedAttachmentModel"
        :selected-mop="selectedMop"
        :attachments="selectedMopAttachments"
        :selected-attachment="selectedAttachment"
        :binding-status="bindingStatus"
        :binding-error="bindingError"
        :can-preview="canPreview"
        :busy="busy"
        :disabled-reason="disabledReason"
        :button-text="buttonText"
        @open="$emit('open')"
      />

      <label class="field search-field">
        <span>搜索 MOP 表格</span>
        <input v-model="mopSearchModel" placeholder="搜索 MOP 名称、文件编号、专业" />
      </label>
      <div class="candidate-summary">
        <span>{{ mopCandidates.length }} 个可选 MOP</span>
        <strong v-if="recommendedCount">推荐 {{ recommendedCount }} 个已置顶</strong>
        <strong v-else>可搜索名称、编号、专业</strong>
      </div>
      <div v-if="!mopCandidates.length" class="empty-box">
        当前关键词下没有可选 MOP 表格。可清空搜索或刷新 MOP 数据后再试。
      </div>
      <div v-else class="mop-candidate-list">
        <button
          v-for="mop in mopCandidates"
          :key="mop.record_id"
          type="button"
          class="mop-row"
          :class="{ active: mop.record_id === selectedMopRecordId, recommended: recommended(mop) }"
          @click="$emit('select-mop', String(mop.record_id || ''))"
        >
          <span class="mop-row-title">
            <strong>{{ mop.title || "未命名 MOP" }}</strong>
            <span class="mop-row-badges">
              <em v-if="mop.record_id === selectedMopRecordId" class="mop-selected-mark">已选</em>
              <em v-if="recommended(mop)" class="mop-recommend">推荐</em>
            </span>
          </span>
          <small>
            <template v-if="mop.file_no">{{ mop.file_no }} · </template>
            <template v-if="mop.specialty">{{ mop.specialty }} · </template>
            <template v-if="mop.maintenance_type">{{ mop.maintenance_type }} · </template>
            <template v-if="mop.version">{{ mop.version }} · </template>
            <template v-if="mop.file_status">{{ mop.file_status }} · </template>
            附件 {{ mop.attachment_count || 0 }} 个
          </small>
          <small v-if="recommended(mop)" class="mop-recommend-reason">
            {{ recommendationReason(mop) }}
          </small>
        </button>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { Dict } from "../api/client";
import MopSelectedFileCard from "./MopSelectedFileCard.vue";

const props = defineProps<{
  selectedNotice: Dict | null;
  selectedMop: Dict | null;
  selectedMopAttachments: Dict[];
  selectedAttachmentToken: string;
  selectedAttachment: Dict | null;
  bindingStatus: string;
  bindingError: string;
  canPreview: boolean;
  busy: boolean;
  disabledReason: string;
  buttonText: string;
  mopCandidates: Dict[];
  selectedMopRecordId: string;
  mopSearch: string;
  isRecommendedMop: (mop: Dict) => boolean;
}>();

const emit = defineEmits<{
  "update:selectedAttachmentToken": [value: string];
  "update:mopSearch": [value: string];
  open: [];
  "select-mop": [recordId: string];
}>();

const selectedAttachmentModel = computed({
  get: () => props.selectedAttachmentToken,
  set: (value: string) => emit("update:selectedAttachmentToken", value),
});

const mopSearchModel = computed({
  get: () => props.mopSearch,
  set: (value: string) => emit("update:mopSearch", value),
});

const recommendedCount = computed(() => props.mopCandidates.filter((item) => recommended(item)).length);

const nextStepTitle = computed(() => {
  if (!props.selectedMop) return "选择一个 MOP 表格";
  if (!props.selectedAttachment) return "选择可预览的表格附件";
  if (props.canPreview) return "已准备好，可以打开填写";
  return "核对 MOP 附件状态";
});

const nextStepText = computed(() => {
  if (!props.selectedMop) return "推荐项会自动排在最上方；选中后再打开填写。";
  if (!props.selectedAttachment) return "当前 MOP 没有可用 xlsx/csv 附件，需更换表格或刷新数据。";
  if (props.bindingError) return props.bindingError;
  if (props.bindingStatus) return props.bindingStatus;
  if (props.canPreview) return "点击已选表格卡片里的“打开填写”，系统会先自动绑定。";
  return props.disabledReason || "请核对当前 MOP 是否有可预览附件。";
});

const nextStepTone = computed(() => {
  if (props.bindingError || (props.selectedMop && !props.selectedAttachment)) return "warning";
  if (props.canPreview) return "ready";
  return "active";
});

function recommended(mop: Dict): boolean {
  return props.isRecommendedMop(mop);
}

function recommendationReason(mop: Dict): string {
  if (props.selectedNotice?.mop_binding?.inherited) return "推荐原因：继承上次同类通告绑定";
  if (String(mop.record_id || "") === String(props.selectedNotice?.mop_binding?.mop_record_id || "")) return "推荐原因：该通告已绑定过这个 MOP";
  return "推荐原因：与当前维保通告匹配";
}
</script>

<style scoped>
.panel {
  padding: 16px;
  display: grid;
  gap: 12px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 16px 36px rgba(0, 47, 135, 0.08);
}

.binding-panel {
  min-height: min(720px, calc(100vh - 180px));
  height: auto;
  max-height: none;
  overflow: visible;
  grid-template-rows: auto auto auto auto minmax(120px, 1fr);
  align-content: stretch;
}

.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}

.panel-head h2 {
  margin: 0;
  color: #0f172a;
  font-size: 17px;
  font-weight: 950;
}

.panel-head p {
  margin: 6px 0 0;
  color: #64748b;
  font-size: 12px;
}

.panel-head > span {
  padding: 5px 10px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  color: #005bff;
  background: #eff6ff;
  font-weight: 800;
}

.selected-notice {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 12px 14px;
  text-align: left;
  color: #0f172a;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.92)),
    #ffffff;
}

.selected-notice strong {
  display: block;
  margin-top: 7px;
  line-height: 1.45;
  font-size: 14px;
  font-weight: 950;
}

.selected-notice__top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.selected-notice span,
.selected-notice em {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 4px 9px;
  border-radius: 999px;
  font-size: 12px;
  font-style: normal;
  font-weight: 900;
}

.selected-notice span {
  color: #0757d7;
  background: #eff6ff;
}

.selected-notice em {
  color: #047857;
  background: #ecfdf5;
}

.selected-notice p {
  margin: 7px 0 0;
  color: #64748b;
  font-size: 12px;
}

.binding-next-step {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 10px;
  border: 1px solid #bfdbfe;
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.9)),
    #ffffff;
  padding: 10px 12px;
  box-shadow: 0 10px 24px rgba(30, 99, 255, 0.08);
}

.binding-next-step > span {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 28px;
  border-radius: 999px;
  background: #1e63ff;
  color: #ffffff;
  padding: 0 10px;
  font-size: 12px;
  font-weight: 950;
  white-space: nowrap;
}

.binding-next-step strong,
.binding-next-step small {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
}

.binding-next-step strong {
  color: #0f2f6a;
  font-size: 14px;
  font-weight: 950;
}

.binding-next-step small {
  margin-top: 3px;
  color: #475569;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.4;
  display: -webkit-box;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.binding-next-step.ready {
  border-color: #bbf7d0;
  background:
    linear-gradient(135deg, rgba(236, 253, 245, 0.96), rgba(255, 255, 255, 0.9)),
    #ffffff;
}

.binding-next-step.ready > span {
  background: #059669;
}

.binding-next-step.warning {
  border-color: #fed7aa;
  background:
    linear-gradient(135deg, rgba(255, 247, 237, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.binding-next-step.warning > span {
  background: #ea580c;
}

.mop-flow-steps {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  padding: 7px;
  border: 1px solid rgba(207, 224, 255, 0.9);
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.86), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.mop-flow-steps span {
  min-width: 0;
  min-height: 31px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
  line-height: 1.2;
  white-space: nowrap;
}

.mop-flow-steps b {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3156c9;
  font-size: 11px;
}

.mop-flow-steps span.done {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.mop-flow-steps span.done b {
  background: #059669;
  color: #ffffff;
}

.mop-flow-steps span.active {
  border-color: #bfdbfe;
  background: #eff6ff;
  color: #0757d7;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.12);
}

.mop-flow-steps span.active b {
  background: #1e63ff;
  color: #ffffff;
}

.field {
  display: grid;
  gap: 6px;
  color: #475569;
  font-size: 13px;
  font-weight: 750;
}

.field input {
  min-height: 36px;
  border: 1px solid #c8dcf3;
  border-radius: 14px;
  background: #fbfdff;
  padding: 0 12px;
  color: #0f172a;
}

.candidate-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-height: 28px;
  border: 1px solid rgba(216, 229, 247, 0.82);
  border-radius: 999px;
  background: rgba(248, 251, 255, 0.9);
  padding: 4px 10px;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
}

.candidate-summary span,
.candidate-summary strong {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.candidate-summary strong {
  color: #0757d7;
  font-weight: 950;
}

.mop-candidate-list {
  max-height: clamp(260px, 46vh, 520px);
  min-height: 220px;
  overflow: auto;
  display: grid;
  gap: 10px;
  align-content: start;
  padding-right: 4px;
}

.mop-row {
  position: relative;
  width: 100%;
  display: grid;
  gap: 6px;
  padding: 9px 12px 9px 15px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  color: #0f172a;
  background: #ffffff;
  line-height: 1.45;
  text-align: left;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.mop-row::before {
  content: "";
  position: absolute;
  top: 12px;
  bottom: 12px;
  left: 7px;
  width: 3px;
  border-radius: 999px;
  background: #cfe0ff;
}

.mop-row-title {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: start;
  gap: 8px;
  min-width: 0;
}

.mop-row:hover,
.mop-row.active {
  border-color: #1e63ff;
  box-shadow: 0 10px 24px rgba(30, 99, 255, 0.13);
  transform: translateY(-1px);
}

.mop-row.recommended {
  border-color: #bfdbfe;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.92), rgba(255, 255, 255, 0.96)),
    #ffffff;
  box-shadow: inset 0 0 0 1px rgba(30, 99, 255, 0.08);
}

.mop-row.recommended::before {
  background: #60a5fa;
}

.mop-row.recommended.active {
  border-color: #1e63ff;
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.16);
}

.mop-row.active::before {
  background: #1e63ff;
}

.mop-row strong {
  display: -webkit-box;
  margin-top: 0;
  overflow: hidden;
  line-height: 1.45;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.mop-row small {
  display: block;
  margin-top: 0;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
  line-height: 1.5;
  white-space: normal;
  word-break: break-word;
}

.mop-recommend-reason {
  width: fit-content;
  max-width: 100%;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: #eff6ff;
  padding: 3px 8px;
  color: #0757d7 !important;
  font-weight: 900;
  -webkit-line-clamp: 1 !important;
}

.mop-row-badges {
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 5px;
  min-width: 0;
}

.mop-recommend,
.mop-selected-mark {
  width: fit-content;
  border: 1px solid #bbf7d0;
  border-radius: 999px;
  background: #ecfdf5;
  padding: 3px 8px;
  color: #047857;
  font-size: 12px;
  font-style: normal;
  font-weight: 900;
  white-space: nowrap;
}

.mop-selected-mark {
  border-color: #bfdbfe;
  background: #eff6ff;
  color: #0757d7;
}

.empty-box {
  padding: 18px;
  border: 1px dashed #cfe0ff;
  border-radius: 16px;
  color: #475569;
  background: #f8fbff;
}

@media (max-width: 960px) {
  .binding-panel {
    min-height: 0;
  }

  .mop-flow-steps {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
