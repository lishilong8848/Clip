<template>
  <section class="paste-panel">
    <div class="paste-panel-head">
      <div>
        <strong>解析粘贴通告</strong>
        <small>只生成待发起草稿，确认发送后才会通知和上传。</small>
      </div>
      <span>{{ pasteText.trim() ? "已粘贴内容" : "等待粘贴" }}</span>
    </div>
    <textarea
      :value="pasteText"
      placeholder="粘贴完整维保、变更、检修、上电、轮巡或调整通告文本"
      @input="emit('update:pasteText', ($event.target as HTMLTextAreaElement).value)"
    ></textarea>
    <div class="card-actions">
      <span class="job-line" :class="{ failed: pasteParseStatus === 'failed', success: pasteParseStatus === 'success' }">
        {{ pasteParseLine }}
      </span>
      <button class="btn blue" :disabled="parseDisabled" :title="parseDisabledReason" @click="emit('parse')">
        {{ pasteParseBusy ? "解析中" : "解析到待发起通告" }}
      </button>
    </div>
    <DisabledReason
      v-if="parseDisabledReason && !pasteParseBusy"
      :text="parseDisabledReason"
      tone="warning"
    />
    <PastedTargetSelectionPanel
      v-if="pendingChangeTargetSelection"
      :target-search-text="changeTargetSearchText"
      :source-search-text="changeSourceSearchText"
      :selected-source-id="selectedChangeSourceId"
      :selection="pendingChangeTargetSelection"
      :target-candidates="filteredChangeTargetCandidates"
      :source-candidates="changeSourceCandidates"
      :filtered-source-candidates="filteredChangeSourceCandidates"
      :active-target-candidate="visibleActiveChangeTargetCandidate"
      :selected-target-id="selectedChangeTargetId"
      :confirming="changeTargetConfirming"
      :selected-target-visible="selectedTargetVisible"
      :selected-source-visible="selectedSourceVisible"
      :work-type-label="workTypeLabel"
      :target-candidate-id="targetCandidateId"
      :source-candidate-id="sourceCandidateId"
      :detail-rows-for="detailRowsFor"
      @update:target-search-text="emit('update:changeTargetSearchText', $event)"
      @update:source-search-text="emit('update:changeSourceSearchText', $event)"
      @update:selected-source-id="emit('update:selectedChangeSourceId', $event)"
      @preview-target="emit('preview-target', $event)"
      @select-target="emit('select-target', $event)"
      @confirm="emit('confirm')"
    />
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import DisabledReason from "./DisabledReason.vue";
import PastedTargetSelectionPanel from "./PastedTargetSelectionPanel.vue";
import type { Dict } from "../api/client";

const props = defineProps<{
  pasteText: string;
  pasteParseStatus: string;
  pasteParseLine: string;
  pasteParseBusy: boolean;
  pendingChangeTargetSelection: Dict | null;
  changeTargetSearchText: string;
  changeSourceSearchText: string;
  selectedChangeSourceId: string;
  filteredChangeTargetCandidates: Dict[];
  changeSourceCandidates: Dict[];
  filteredChangeSourceCandidates: Dict[];
  visibleActiveChangeTargetCandidate: Dict | null;
  selectedChangeTargetId: string;
  changeTargetConfirming: boolean;
  selectedTargetVisible: boolean;
  selectedSourceVisible: boolean;
  workTypeLabel: (value: string) => string;
  targetCandidateId: (item: Dict) => string;
  sourceCandidateId: (item: Dict) => string;
  detailRowsFor: (item: Dict | null) => Array<{ label: string; value: string }>;
}>();

const emit = defineEmits<{
  "update:pasteText": [value: string];
  "update:changeTargetSearchText": [value: string];
  "update:changeSourceSearchText": [value: string];
  "update:selectedChangeSourceId": [value: string];
  parse: [];
  "preview-target": [item: Dict];
  "select-target": [item: Dict];
  confirm: [];
}>();

const parseDisabledReason = computed(() => {
  if (props.pasteParseBusy) return "正在解析，请稍后。";
  if (!props.pasteText.trim()) return "请先粘贴完整通告文本。";
  return "";
});

const parseDisabled = computed(() => Boolean(parseDisabledReason.value));
</script>

<style scoped>
.paste-panel {
  display: grid;
  gap: 10px;
  margin-bottom: 16px;
  padding: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.paste-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  min-width: 0;
}

.paste-panel-head div {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.paste-panel-head strong {
  color: #071a39;
  font-size: 14px;
  font-weight: 950;
}

.paste-panel-head small {
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.4;
}

.paste-panel-head span {
  flex: 0 0 auto;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #f8fbff;
  padding: 5px 10px;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 900;
}

.paste-panel textarea {
  min-height: 100px;
  width: 100%;
  resize: vertical;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.94);
  color: #0f172a;
  font: inherit;
  line-height: 1.55;
  outline: none;
}

.paste-panel textarea:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.card-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
}

.job-line {
  flex: 1 1 auto;
  color: #64748b;
  font-size: 13px;
  font-weight: 800;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}

.btn {
  min-height: 40px;
  border: 1px solid #cfe0ff;
  border-radius: 16px;
  padding: 0 14px;
  background: #fff;
  color: #1d4ed8;
  font-weight: 900;
  cursor: pointer;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

@media (max-width: 760px) {
  .paste-panel-head {
    align-items: stretch;
    flex-direction: column;
  }

  .paste-panel-head span {
    width: fit-content;
  }

  .card-actions {
    align-items: stretch;
    flex-direction: column;
  }

  .btn {
    width: 100%;
  }
}
</style>
