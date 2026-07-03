<template>
  <MopSummaryStrip
    :notices="noticesCount"
    :pending="pending"
    :bound="bound"
    :uploaded="uploaded"
    :mop-files="mopFiles"
  />

  <section class="mop-layout">
    <MopNoticeList
      :notice-search="noticeSearch"
      :notice-status-filter="noticeStatusFilter"
      :items="items"
      :selected-notice-key="selectedNoticeKey"
      @update:notice-search="$emit('update:noticeSearch', $event)"
      @update:notice-status-filter="$emit('update:noticeStatusFilter', $event)"
      @select="$emit('select-notice', $event)"
    />

    <MopBindingPanel
      :selected-attachment-token="selectedAttachmentToken"
      :mop-search="mopSearch"
      :selected-notice="selectedNotice"
      :selected-mop="selectedMop"
      :selected-mop-attachments="selectedMopAttachments"
      :selected-attachment="selectedAttachment"
      :binding-status="bindingStatus"
      :binding-error="bindingError"
      :can-preview="canPreview"
      :busy="busy"
      :disabled-reason="disabledReason"
      :button-text="buttonText"
      :mop-candidates="mopCandidates"
      :selected-mop-record-id="selectedMopRecordId"
      :is-recommended-mop="isRecommendedMop"
      :local-upload-busy="localUploadBusy"
      :local-upload-status="localUploadStatus"
      :local-upload-message="localUploadMessage"
      @update:selected-attachment-token="$emit('update:selectedAttachmentToken', $event)"
      @update:mop-search="$emit('update:mopSearch', $event)"
      @open="$emit('open')"
      @select-mop="$emit('select-mop', $event)"
      @upload-local="$emit('upload-local', $event)"
      @upload-local-invalid="$emit('upload-local-invalid', $event)"
    />
  </section>
</template>

<script setup lang="ts">
import type { Dict } from "../api/client";
import MopBindingPanel from "./MopBindingPanel.vue";
import MopNoticeList from "./MopNoticeList.vue";
import MopSummaryStrip from "./MopSummaryStrip.vue";

defineProps<{
  noticesCount: number;
  pending: number;
  bound: number;
  uploaded: number;
  mopFiles: number;
  noticeSearch: string;
  noticeStatusFilter: string;
  items: Dict[];
  selectedNoticeKey: string;
  selectedAttachmentToken: string;
  mopSearch: string;
  selectedNotice: Dict | null;
  selectedMop: Dict | null;
  selectedMopAttachments: Dict[];
  selectedAttachment: Dict | null;
  bindingStatus: string;
  bindingError: string;
  canPreview: boolean;
  busy: boolean;
  disabledReason: string;
  buttonText: string;
  mopCandidates: Dict[];
  selectedMopRecordId: string;
  isRecommendedMop: (item: Dict) => boolean;
  localUploadBusy: boolean;
  localUploadStatus: string;
  localUploadMessage: string;
}>();

defineEmits<{
  "update:noticeSearch": [value: string];
  "update:noticeStatusFilter": [value: string];
  "update:selectedAttachmentToken": [value: string];
  "update:mopSearch": [value: string];
  "select-notice": [noticeKey: string];
  open: [];
  "select-mop": [recordId: string];
  "upload-local": [file: File];
  "upload-local-invalid": [message: string];
}>();
</script>

<style scoped>
.mop-layout {
  display: grid;
  grid-template-columns: minmax(320px, 0.82fr) minmax(520px, 1.18fr);
  gap: 18px;
  align-items: start;
}

@media (max-width: 1180px) {
  .mop-layout {
    grid-template-columns: 1fr;
  }
}
</style>
