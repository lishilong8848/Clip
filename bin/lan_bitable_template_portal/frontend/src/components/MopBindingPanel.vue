<template>
  <section class="panel binding-panel">
    <div class="panel-head">
      <div>
        <h2>选择 MOP 表格</h2>
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
        <strong v-if="recommendedCount">推荐 {{ recommendedCount }}</strong>
        <strong v-else>候选 MOP</strong>
      </div>
      <div v-if="!mopCandidates.length" class="empty-box">
        暂无可选 MOP 表格
      </div>
      <div v-else class="mop-candidate-list">
        <button type="button"
          v-for="mop in mopCandidates"
          :key="mop.record_id"
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

      <section
        class="local-mop-upload"
        :class="{ dragging: localDragActive, busy: localUploadBusy, success: localUploadStatus === 'success', failed: localUploadStatus === 'failed' }"
        tabindex="0"
        role="button"
        aria-label="上传本地 MOP 文件"
        @click="triggerLocalFileInput"
        @keydown.enter.prevent="triggerLocalFileInput"
        @keydown.space.prevent="triggerLocalFileInput"
        @dragenter.prevent="localDragActive = true"
        @dragover.prevent="localDragActive = true"
        @dragleave.prevent="localDragActive = false"
        @drop.prevent="handleLocalDrop"
        @paste="handleLocalPaste"
      >
        <input
          ref="localFileInput"
          class="local-mop-file-input"
          type="file"
          accept=".xlsx,.xlsm,.xls,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel.sheet.macroEnabled.12,application/vnd.ms-excel"
          :disabled="localUploadBusy"
          @click.stop
          @change="handleLocalInput"
        />
        <div class="local-upload-icon" aria-hidden="true">
          <span v-if="localUploadBusy">···</span>
          <span v-else>↑</span>
        </div>
        <div class="local-upload-copy">
          <strong>{{ localUploadTitle }}</strong>
          <small>{{ localUploadHint }}</small>
          <em v-if="localUploadMessage" :class="{ failed: localUploadStatus === 'failed', success: localUploadStatus === 'success' }">
            {{ localUploadMessage }}
          </em>
        </div>
        <button type="button"
          class="local-upload-button"
          :disabled="localUploadBusy"
          @click.stop="triggerLocalFileInput"
        >
          选择文件
        </button>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
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
  localUploadBusy: boolean;
  localUploadStatus: string;
  localUploadMessage: string;
}>();

const emit = defineEmits<{
  "update:selectedAttachmentToken": [value: string];
  "update:mopSearch": [value: string];
  open: [];
  "select-mop": [recordId: string];
  "upload-local": [file: File];
  "upload-local-invalid": [message: string];
}>();

const localFileInput = ref<HTMLInputElement | null>(null);
const localDragActive = ref(false);

const selectedAttachmentModel = computed({
  get: () => props.selectedAttachmentToken,
  set: (value: string) => emit("update:selectedAttachmentToken", value),
});

const mopSearchModel = computed({
  get: () => props.mopSearch,
  set: (value: string) => emit("update:mopSearch", value),
});

const recommendedCount = computed(() => props.mopCandidates.filter((item) => recommended(item)).length);

const localUploadTitle = computed(() => {
  if (props.localUploadBusy) return "上传中 / 识别中";
  if (props.localUploadStatus === "success") return "已选本地 MOP";
  if (props.localUploadStatus === "failed") return "本地 MOP 上传失败";
  return "没有合适的 MOP？上传本地文件";
});

const localUploadHint = computed(() => {
  if (props.localUploadBusy) return "识别中";
  if (props.localUploadStatus === "success") return "已选中";
  if (props.localUploadStatus === "failed") return "上传失败";
  return ".xlsx / .xlsm / .xls";
});

function recommended(mop: Dict): boolean {
  return props.isRecommendedMop(mop);
}

function recommendationReason(mop: Dict): string {
  if (props.selectedNotice?.mop_binding?.inherited) return "继承绑定";
  if (String(mop.record_id || "") === String(props.selectedNotice?.mop_binding?.mop_record_id || "")) return "已绑定";
  return "匹配";
}

function triggerLocalFileInput(): void {
  if (props.localUploadBusy) return;
  localFileInput.value?.click();
}

function firstValidExcelFile(files: FileList | File[] | null | undefined): File | null {
  const items = Array.from(files || []);
  return items.find((file) => /\.(xlsx|xlsm|xls)$/i.test(file.name || "")) || null;
}

function submitLocalFile(file: File | null): void {
  if (!file || props.localUploadBusy) return;
  emit("upload-local", file);
}

function handleLocalInput(event: Event): void {
  const input = event.target as HTMLInputElement;
  const hasFiles = Boolean(input.files?.length);
  const file = firstValidExcelFile(input.files);
  if (!file && hasFiles) {
    emit("upload-local-invalid", "请选择 xlsx、xlsm 或 xls 格式的 Excel 文件。");
  }
  submitLocalFile(file);
  input.value = "";
}

function handleLocalDrop(event: DragEvent): void {
  localDragActive.value = false;
  const file = firstValidExcelFile(event.dataTransfer?.files);
  if (!file) {
    emit("upload-local-invalid", "请拖入 xlsx、xlsm 或 xls 格式的 Excel 文件。");
    return;
  }
  submitLocalFile(file);
}

function handleLocalPaste(event: ClipboardEvent): void {
  const file = firstValidExcelFile(event.clipboardData?.files);
  if (!file) {
    emit("upload-local-invalid", "请粘贴 Excel 文件，普通文本或图片不能作为 MOP 表格。");
    return;
  }
  event.preventDefault();
  submitLocalFile(file);
}
</script>

<style scoped>
.panel {
  padding: 14px;
  display: grid;
  gap: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 16px 36px rgba(0, 47, 135, 0.08);
}

.binding-panel {
  min-height: min(680px, calc(100vh - 190px));
  height: auto;
  max-height: none;
  overflow: visible;
  grid-template-rows: auto auto auto minmax(120px, 1fr);
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
  border-radius: 14px;
  padding: 10px 12px;
  text-align: left;
  color: #0f172a;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.92)),
    #ffffff;
}

.selected-notice strong {
  display: block;
  margin-top: 5px;
  line-height: 1.35;
  font-size: 13px;
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
  min-height: 22px;
  padding: 3px 8px;
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
  margin: 5px 0 0;
  color: #64748b;
  font-size: 12px;
}

.mop-flow-steps {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 6px;
  padding: 6px;
  border: 1px solid rgba(207, 224, 255, 0.9);
  border-radius: 16px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.86), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.mop-flow-steps span {
  min-width: 0;
  min-height: 29px;
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
  min-height: 180px;
  overflow: auto;
  display: grid;
  gap: 7px;
  align-content: start;
  padding-right: 4px;
}

.local-mop-upload {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-height: 74px;
  border: 1.5px dashed #9fc5ff;
  border-radius: 16px;
  padding: 10px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.95), rgba(255, 255, 255, 0.88)),
    #ffffff;
  color: #0f172a;
  cursor: pointer;
  transition: border-color 0.16s ease, background 0.16s ease, box-shadow 0.16s ease;
}

.local-mop-upload:hover,
.local-mop-upload:focus-visible,
.local-mop-upload.dragging {
  border-color: #1e63ff;
  background:
    linear-gradient(135deg, rgba(219, 234, 254, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: 0 14px 28px rgba(30, 99, 255, 0.12);
  outline: none;
}

.local-mop-upload.busy {
  cursor: wait;
  opacity: 0.86;
}

.local-mop-upload.success {
  border-color: #86efac;
  background: linear-gradient(135deg, rgba(236, 253, 245, 0.96), rgba(255, 255, 255, 0.9));
}

.local-mop-upload.failed {
  border-color: #fecaca;
  background: linear-gradient(135deg, rgba(254, 242, 242, 0.96), rgba(255, 255, 255, 0.9));
}

.local-mop-file-input {
  display: none;
}

.local-upload-icon {
  width: 40px;
  height: 40px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 14px;
  background: linear-gradient(135deg, #1e63ff, #005bd8);
  color: #ffffff;
  font-size: 21px;
  font-weight: 950;
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.22);
}

.local-mop-upload.success .local-upload-icon {
  background: linear-gradient(135deg, #10b981, #059669);
  box-shadow: 0 12px 26px rgba(16, 185, 129, 0.2);
}

.local-mop-upload.failed .local-upload-icon {
  background: linear-gradient(135deg, #ef4444, #dc2626);
  box-shadow: 0 12px 26px rgba(239, 68, 68, 0.16);
}

.local-upload-copy {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.local-upload-copy strong {
  color: #0f172a;
  font-size: 14px;
  font-weight: 950;
}

.local-upload-copy small {
  color: #48627f;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.45;
}

.local-upload-copy em {
  display: -webkit-box;
  overflow: hidden;
  width: fit-content;
  max-width: 100%;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  padding: 4px 8px;
  background: #eff6ff;
  color: #0757d7;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
  text-overflow: ellipsis;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.local-upload-copy em.success {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.local-upload-copy em.failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #b91c1c;
}

.local-upload-button {
  min-height: 32px;
  border: 1px solid #bdd2f4;
  border-radius: 999px;
  padding: 0 13px;
  background: rgba(255, 255, 255, 0.94);
  color: #0757d7;
  font: inherit;
  font-size: 12px;
  font-weight: 950;
  cursor: pointer;
  white-space: nowrap;
}

.local-upload-button:disabled {
  cursor: not-allowed;
  opacity: 0.56;
}

.mop-row {
  position: relative;
  width: 100%;
  display: grid;
  gap: 5px;
  padding: 8px 11px 8px 14px;
  border: 1px solid #d8e5f7;
  border-radius: 13px;
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

  .local-mop-upload {
    grid-template-columns: auto minmax(0, 1fr);
  }

  .local-upload-button {
    grid-column: 1 / -1;
    width: 100%;
  }
}
</style>
