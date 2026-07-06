<template>
  <section class="engineer-mop" :class="{ 'preview-open': previewMode }">
    <div v-if="!previewMode" class="page-back-row">
      <VnetBackButton to="/" />
    </div>
    <MopPageHeader
      v-if="!previewMode"
      v-model:scope="scope"
      :scope-options="scopeOptions"
      :loading="loading"
      :preview-loading="previewLoading"
      @refresh="loadPage"
    />

    <div v-if="checking" class="notice-box" role="status" aria-live="polite">正在检查登录状态...</div>
    <div v-else-if="!loggedIn" class="notice-box" role="alert">
      请先登录飞书后再使用工程师 MOP 页面。
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </div>

    <template v-else>
      <MessageBanner
        v-if="message"
        :tone="messageType === 'failed' ? 'failed' : messageType === 'success' ? 'success' : 'info'"
        :text="message"
      />
      <MessageBanner
        v-if="warnings.length"
        tone="warning"
        title="需要注意"
        :items="warnings"
      />

      <MopFlowSteps v-if="!previewMode" :steps="mopFlowSteps" />

      <section v-if="previewMode && preview" class="mop-preview-page">
        <MopPreviewHeader
          :title="previewTitle"
          :notice-title="selectedNotice?.title || ''"
          :sheet-name="activeSheet?.name || ''"
          :row-count="Number(activeSheet?.row_count || 0)"
          :completion-items="mopCompletionItems"
          @back="backToBinding"
        />
        <MopSheetStatusPanel
          :local-file="preview.local_file || null"
          :active-sheet="activeSheet || null"
          :checkbox-count="activeSheetCheckboxCells.length"
          :maintenance-field-count="activeSheetMaintenanceFields.length"
          :filled-count="mopFilledCount"
        />
        <div
          v-if="signatureManagerOpen"
          class="signature-manager-backdrop"
          @click.self="closeSignatureManager"
        ></div>
        <section
          v-if="activeSheet && !activeSheet.is_cover"
          class="mop-sign-panel"
          :class="{
            'manager-open': signatureManagerOpen,
            'signature-drawer-open': selectedSignatureDrawerOpen || temporarySignatureDrawerOpen
          }"
        >
          <div class="sign-panel-head">
            <div>
              <strong>签名任务中心</strong>
              <p v-if="signatureManagerOpen">
                {{ signatureTaskSummaryText }}
              </p>
            </div>
            <button
              v-if="signatureManagerOpen"
              type="button"
              class="sign-close-inline"
              @click="closeSignatureManager"
            >
              关闭
            </button>
          </div>
          <MopSignatureRoleSummary
            :current-role="signatureRole"
            :items="signatureRoleSummaryItems"
            @select="openSignatureManager"
          />
          <MopSignatureGuideStrip
            v-if="signatureManagerOpen"
            :items="signatureGuideItems"
            @select-role="openSignatureManager"
          />
          <div class="sign-workspace">
            <MopCompanySignaturePicker
              v-model:search="signatureSearch"
              :loading="signatureLoading"
              :status-text="signatureSearchStatus"
              :people="signaturePeople"
              :selected-ids="signatureSelectedRecords[signatureRole] || []"
              :active-record-id="activeSignatureRecordId"
              compact
              @refresh="loadSignaturePeople()"
              @select="selectSignaturePerson"
            />
            <div class="sign-canvas-card signature-zone">
              <div class="signature-zone-head">
                <strong>{{ signatureRole === 'implementer' ? '维护实施人签名' : '维护审核人签名' }}</strong>
                <small>{{ selectedRoleSignatureStatusText }}</small>
              </div>
              <MopCompanySelectedSignatures
                :role="signatureRole"
                :people="selectedFormalSignaturePeople(signatureRole)"
                :active-record-id="activeSignatureRecordId"
                :unsigned-count="selectedFormalSignatureNotReadyCount(signatureRole)"
                :unsigned-signature-count="selectedFormalSignatureMissingSignatureCount(signatureRole)"
                :drawer-open="selectedSignatureDrawerOpen"
                :link-sending-by-id="signatureLinkSendingById"
                :link-sent-at-by-id="signatureLinkSentAtById"
                :link-error-by-id="signatureLinkErrorById"
                :has-usable-signature="personReadyForMop"
                :person-key="signaturePersonKey"
                :display-name="signaturePersonDisplayName"
                :link-title="personSignatureLinkTitle"
                :web-sign-disabled-reason="staffSignatureWebSignDisabledReason"
                :bulk-link-sending="signatureBulkLinkSending"
                :confirm-sending="signatureUsageConfirmSending"
                :confirmable-count="signatureUsageConfirmationCount(signatureRole)"
                @activate="activateSelectedSignaturePerson"
                @toggle-drawer="toggleSelectedSignatureDrawer"
                @close-drawer="setSelectedSignatureDrawerOpen(false)"
                @image-error="handleSelectedSignatureImageError"
                @web-sign="openSignaturePadForPerson"
                @send-link="sendSignatureLinkForPerson"
                @send-unsigned-links="sendUnsignedSignatureLinksForRole(signatureRole)"
                @send-confirmations="sendSignatureUsageConfirmationsForRole(signatureRole)"
                @remove="removeSignaturePerson(signatureRole, $event)"
              />
              <MopOtherSignatureManager
                :drawer-open="temporarySignatureDrawerOpen"
                v-model:external-search="externalSignatureSearch"
                :role="signatureRole"
                :add-disabled-reason="addOtherSignatureDisabledReason"
                :display-rows="currentRoleOtherSignatureDisplayRows"
                :preview-row="currentRoleOtherSignaturePreviewRow"
                :unsigned-count="currentRoleOtherSignatureUnsignedCount"
                :temporary-link-sending-by-id="temporarySignatureLinkSendingById"
                :temporary-link-sent-at-by-id="temporarySignatureLinkSentAtById"
                :temporary-link-error-by-id="temporarySignatureLinkErrorById"
                :draft-sending-by-id="temporarySignatureSendingByDraft"
                :external-loading="externalSignatureLoading"
                :external-status-text="externalSignatureSearchStatus"
                :external-people="externalSignaturePeople"
                :person-status-text="otherSignaturePersonStatusText"
                :person-web-sign-disabled-reason="otherSignatureWebSignDisabledReason"
                :draft-status-text="otherSignatureDraftStatusText"
                :draft-disabled-reason="temporarySignatureRowDisabledReason"
                @update:drawer-open="setTemporarySignatureDrawerOpen"
                @add-other="addOtherSignatureDraft"
                @image-error="handleSelectedSignatureImageError"
                @web-sign-person="openSignaturePadForPerson"
                @send-temp-person="sendTemporarySignatureLinkForPerson"
                @remove-person="removeSignaturePerson(signatureRole, $event)"
                @update-draft-name="updateOtherSignatureDraftName"
                @ensure-draft-name="ensureOtherSignatureDraftName"
                @web-sign-draft="openSignaturePadForDraft"
                @send-draft-link="sendTemporarySignatureLinkForDraft"
                @remove-draft="removeOtherSignatureDraft"
                @refresh-external="loadExternalSignaturePeople()"
                @add-external="addExternalSignaturePerson"
              />
              <MopFileActions
                :message="signatureMessage"
                :message-type="signatureMessageType"
                :role-hint="signatureRoleHint"
                :fill-saving="mopFillSaving"
                :upload-saving="mopUploadSaving"
                :reset-saving="mopResetting"
                :fill-disabled-reason="fillMopDisabledReason"
                :filled-mop-available="Boolean(filledMopResult)"
                @fill="fillMopSignatures"
                @reset="resetMopSigning"
              />
            </div>
          </div>
        </section>
        <MopSignaturePadModal
          :open="signaturePadOpen"
          :title="activeSignaturePerson?.name || '手写签名'"
          :role-label="signatureRole === 'auditor' ? '维护审核人' : '维护实施人'"
          :saving="signatureSaving"
          :message="signatureMessage"
          :message-type="signatureMessageType"
          :save-disabled-reason="saveSignatureDisabledReason"
          @close="closeSignaturePad"
          @clear="clearSignatureCanvas"
          @save="saveMopSignature"
        >
            <div class="mop-sign-canvas signature-pad-canvas" :class="{ disabled: !activeSignaturePerson }">
              <button
                class="sign-clear-inline"
                type="button"
                :disabled="signatureSaving"
                @click="clearSignatureCanvas"
              >
                清空
              </button>
              <img
                v-if="personHasUsableSignature(activeSignaturePerson) && !signatureHasInk"
                class="mop-sign-preview-img"
                :src="activeSignaturePerson?.signature_preview_url"
                alt="已有手写签名"
                @error="handleSignatureImageError(activeSignaturePerson?.record_id)"
              />
              <canvas
                ref="signatureCanvasRef"
                aria-label="MOP手写签名区域"
                @pointerdown="startSignatureDraw"
                @pointermove="moveSignatureDraw"
                @pointerup="endSignatureDraw"
                @pointercancel="endSignatureDraw"
                @pointerleave="endSignatureDraw"
              ></canvas>
              <div v-if="!signatureHasInk && !personHasUsableSignature(activeSignaturePerson)" class="sign-placeholder">在此处手写签名</div>
            </div>
        </MopSignaturePadModal>
        <MopSheetTabs
          v-model="activeSheetName"
          :sheets="preview.sheets || []"
          :active-sheet="activeSheet || null"
        />
        <div
          ref="sheetScrollRef"
          class="sheet-scroll preview-scroll"
          tabindex="0"
          @keydown="handleMopTableKeydown"
          @mouseup="finishMopCellSelection"
          @mouseleave="finishMopCellSelection"
        >
          <MopSheetPreviewTable
            :active-sheet="activeSheet || null"
            :column-indexes="activeSheetColumnIndexes"
            :bulk-count="bulkFillCheckboxCells.length"
            :filled-count="mopFilledCount"
            :signature-manager-open="signatureManagerOpen"
            :active-mop-cell-position="activeMopCellPosition"
            :active-mop-cell-key="activeMopCellKey"
            :date-time="mopFillDateTime"
            :popover-mode="mopCellPopoverMode"
            :overlay-style="activeMopCellOverlayStyle"
            :popover-label="mopCellPopoverLabel"
            :checkbox-options="mopCellPopoverOptions"
            :checkbox-value="mopCellPopoverCheckboxValue"
            :raw-value="activeMopCellEditableValue"
            :selected-count="selectedMopCellKeys.length"
            :column-label="columnLabel"
            :cell-merge-span="cellMergeSpan"
            :mop-cell-key="mopCellKey"
            :checkbox-cell-at="checkboxCellAt"
            :maintenance-field-at="maintenanceFieldAt"
            :editable-cell-at="editableCellAt"
            :mop-cell-has-override="mopCellHasOverride"
            :is-mop-cell-selected="isMopCellSelected"
            :signature-role-at-cell="signatureRoleAtCell"
            :cell-signatures="cellSignatures"
            :signature-cell-style="signatureCellStyle"
            :signature-image-style="signatureImageStyle"
            :signature-more-style="signatureMoreStyle"
            :checkbox-state-label="checkboxStateLabel"
            :cell-override-value="cellOverrideValue"
            @mark-all-normal="markAllCheckboxes('normal')"
            @cell-mousedown="startMopCellSelection"
            @cell-enter="extendMopCellSelection"
            @select-checkbox="setActiveCheckboxState"
            @update:date-time="mopFillDateTime = $event"
            @update:raw-value="setActiveEditableCellValue"
            @fill-date="fillActiveMaintenanceDate"
            @fill-completion="fillActiveMaintenanceCompletion"
            @copy="copyActiveMopSelection"
            @paste="pasteActiveMopSelection"
            @restore="restoreActiveMopSelection"
            @cancel="clearMopCellSelection"
          />
        </div>
        <MopUploadFooter
          :items="mopUploadFooterItems"
          :uploaded-at-text="signedMopUploadedAtText"
          :saving="mopUploadSaving"
          :disabled="mopUploadSaving || mopFillSaving || mopResetting || Boolean(uploadSignedMopDisabledReason)"
          :disabled-reason="uploadSignedMopDisabledReason"
          @upload="uploadSignedMop"
        />
      </section>

      <template v-else>
        <MopBindingWorkspace
          v-model:notice-search="noticeSearch"
          v-model:notice-status-filter="noticeStatusFilter"
          v-model:selected-attachment-token="selectedAttachmentToken"
          v-model:mop-search="mopSearch"
          :notices-count="notices.length"
          :pending="pendingMopNoticeCount"
          :bound="boundNoticeCount"
          :uploaded="uploadedMopNoticeCount"
          :mop-files="mopCandidates.length"
          :items="filteredNotices"
          :selected-notice-key="selectedNoticeKey"
          :selected-notice="selectedNotice"
          :selected-mop="selectedMop"
          :selected-mop-attachments="selectedMopAttachments"
          :selected-attachment="selectedAttachment"
          :binding-status="mopBindingStatus"
          :binding-error="mopBindingError"
          :can-preview="canPreview"
          :busy="openMopBusy"
          :disabled-reason="openMopDisabledReason"
          :button-text="openMopButtonText"
          :mop-candidates="filteredMopCandidates"
          :selected-mop-record-id="selectedMopRecordId"
          :is-recommended-mop="isRecommendedMop"
          :local-upload-busy="localMopUploadBusy"
          :local-upload-status="localMopUploadStatus"
          :local-upload-message="localMopUploadMessage"
          @select-notice="selectNotice"
          @open="startMopPreview"
          @select-mop="selectMop"
          @upload-local="uploadLocalMopFile"
          @upload-local-invalid="handleLocalMopUploadInvalid"
        />
      </template>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from "vue";
import type { Dict } from "../api/client";
import {
  bindEngineerMop,
  fetchEngineerMopBootstrap,
  fillEngineerMop,
  previewEngineerMop,
  resetEngineerMop,
  uploadLocalEngineerMop,
  uploadSignedEngineerMop,
} from "../mopFileApi";
import {
  defaultMopDateTimeLocal,
  formatMopDateTime,
  formatMopUploadTime,
  makeMopCellKey,
  makeMopCheckboxKey,
  makeMopMaintenanceKey,
  makeRawMopCellKey,
  isMopMatrixClipboardText,
  normalizeMopRequiredTimeText,
  parseMopComparableDate,
  parseMopClipboardMatrix,
  parsePeopleCount,
  roleForMopMaintenanceLabel,
  selectedMopCellBounds as calculateSelectedMopCellBounds,
} from "../mopSheetUtils";
import {
  buildMopCellEditPayload as buildMopCellEditPayloadFromEdits,
  buildMopCheckboxPayload as buildMopCheckboxPayloadFromCells,
  buildMopFieldPayload as buildMopFieldPayloadFromFields,
  mopMemoryFieldKeys,
  mopPayloadList,
} from "../mopPayloadUtils";
import {
  buildMopSignaturePayload as buildMopSignaturePayloadFromPeople,
  mopPersonHasUsableSignature,
  otherSignatureDraftPriority,
  otherSignatureDraftStatusText,
  otherSignaturePersonPriority,
  otherSignaturePersonStatusText,
  signaturePersonDisplayName,
  signaturePersonKey,
  temporarySignatureDisplayName,
  temporarySignatureDisplayNumber,
} from "../mopSignatureUtils";
import {
  compactSearchText,
  mopCandidateMatchesSpecialty,
  mopAttachmentKey,
  mopNoticeNeedsAction,
  mopSpecialtyText,
  normalizeMopScope,
  noticeIsEnded,
  noticeMopUploaded,
  sortMopNoticesForAction,
  sortRecommendedMopFirst as sortRecommendedMopCandidates,
} from "../mopSelectionUtils";
import {
  buildMopCompletionItems,
  buildMopRequirementItems,
  buildMopUploadFooterItems,
  canUploadSignedMop as resolveCanUploadSignedMop,
  fillMopDisabledReason as resolveFillMopDisabledReason,
  maintenanceTimeValidationMessage,
  requiredImplementerSignatureCount as resolveRequiredImplementerSignatureCount,
  signatureDisplayCount,
  uploadSignedMopDisabledReason as resolveUploadSignedMopDisabledReason,
} from "../mopUploadReadiness";
import {
  createTemporarySignatureSession,
  fetchExternalSignaturePeople,
  fetchSignaturePeople,
  fetchTemporarySignatures,
  saveExternalSignature,
  saveStaffSignature,
  saveTemporarySignature,
  sendSignatureUsageConfirmations,
  sendStaffSignatureLink,
  sendTemporarySignatureLink,
} from "../mopSignatureApi";
import { useMopEditSessionLock } from "../useMopEditSessionLock";
import { useMopCellSelection } from "../useMopCellSelection";
import { useMopSheetEditing } from "../useMopSheetEditing";
import { useMopSignatureSelection } from "../useMopSignatureSelection";
import { useGuardedPolling } from "../useGuardedPolling";
import { useLocalMopUpload } from "../useLocalMopUpload";
import { useMopSignatureCanvas } from "../useMopSignatureCanvas";
import MopBindingWorkspace from "./MopBindingWorkspace.vue";
import MopCompanySelectedSignatures from "./MopCompanySelectedSignatures.vue";
import type { MopCellPopoverMode } from "./MopCellPopover.vue";
import MopCompanySignaturePicker from "./MopCompanySignaturePicker.vue";
import MopFileActions from "./MopFileActions.vue";
import MopFlowSteps from "./MopFlowSteps.vue";
import MessageBanner from "./MessageBanner.vue";
import MopOtherSignatureManager from "./MopOtherSignatureManager.vue";
import MopPageHeader from "./MopPageHeader.vue";
import MopPreviewHeader from "./MopPreviewHeader.vue";
import MopSheetPreviewTable from "./MopSheetPreviewTable.vue";
import MopSheetStatusPanel from "./MopSheetStatusPanel.vue";
import MopSheetTabs from "./MopSheetTabs.vue";
import MopSignaturePadModal from "./MopSignaturePadModal.vue";
import MopSignatureGuideStrip from "./MopSignatureGuideStrip.vue";
import MopSignatureRoleSummary, {
  type MopSignatureRole,
  type MopSignatureRoleSummaryItem,
} from "./MopSignatureRoleSummary.vue";
import MopUploadFooter from "./MopUploadFooter.vue";
import VnetBackButton from "./VnetBackButton.vue";

type ScopeOption = { value: string; label: string };

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: ScopeOption[];
}>();

const routeParams = new URLSearchParams(window.location.search);
const initialSourceRecordId = String(routeParams.get("source_record_id") || "").trim();
const initialNoticeKey = String(routeParams.get("notice_key") || "").trim();
const scope = ref(normalizeMopScope(routeParams.get("scope") || props.scopeOptions[0]?.value || "ALL"));
const loading = ref(false);
const saving = ref(false);
const previewLoading = ref(false);
const message = ref("");
const messageType = ref("");
const mopBindingStatus = ref("");
const mopBindingError = ref("");
const currentAuthUser = ref<Dict>({});
const localMopUpload = useLocalMopUpload();
const localMopUploadBusy = localMopUpload.busy;
const localMopUploadStatus = localMopUpload.status;
const localMopUploadMessage = localMopUpload.message;
const warnings = ref<string[]>([]);
const notices = ref<Dict[]>([]);
const mopCandidates = shallowRef<Dict[]>([]);
const selectedNoticeKey = ref("");
const routeNoticePreselectApplied = ref(false);
const selectedMopRecordId = ref("");
const selectedAttachmentToken = ref("");
const noticeSearch = ref("");
const noticeStatusFilter = ref("");
const mopSearch = ref("");
const preview = ref<Dict | null>(null);
const previewMode = ref(false);
const activeSheetName = ref("");
const signatureRole = ref<MopSignatureRole>("implementer");
const signatureSearch = ref("");
const signaturePeople = shallowRef<Dict[]>([]);
const {
  signaturePeopleById,
  temporarySignatures,
  otherSignatureDrafts,
  hiddenOtherSignatureKeys,
  signatureSelectedRecords,
  rememberSignaturePeople,
  updateRememberedSignaturePerson,
  markSignatureUnavailable,
  markOtherSignatureUnavailable,
  selectSignaturePerson: selectSignaturePersonByRole,
  removeSignaturePerson: removeSignaturePersonByRole,
  unhideSignatureKey,
  unhideSignaturePerson,
  selectedFormalSignaturePeople,
  selectedTemporarySignaturePeople,
  selectedSignaturePeople,
  selectedTemporarySignatureUnsignedCount,
  nextOtherSignatureDisplayName,
  ensureOtherSignatureDraftName: ensureOtherSignatureDraftNameByRole,
  updateOtherSignatureDraftName,
  resetOtherSignatures,
  resetSignatureSelection,
} = useMopSignatureSelection(signaturePeople, {
  onChanged: () => updateFormalSignaturePolling(),
});
const selectedSignatureDrawerOpen = ref(false);
const externalSignatureSearch = ref("");
const externalSignaturePeople = shallowRef<Dict[]>([]);
const externalSignaturePeopleTotal = ref(0);
const externalSignatureLoading = ref(false);
const signaturePeopleTotal = ref(0);
const signatureLoading = ref(false);
const signatureSaving = ref(false);
const signatureLinkSendingById = ref<Record<string, boolean>>({});
const signatureLinkSentAtById = ref<Record<string, string>>({});
const signatureLinkErrorById = ref<Record<string, string>>({});
const signatureBulkLinkSending = ref(false);
const signatureUsageConfirmSending = ref(false);
const temporarySignatureLinkSendingById = ref<Record<string, boolean>>({});
const temporarySignatureLinkSentAtById = ref<Record<string, string>>({});
const temporarySignatureLinkErrorById = ref<Record<string, string>>({});
const signatureManagerOpen = ref(false);
const signaturePadOpen = ref(false);
const signaturePadTarget = ref<Dict | null>(null);
const temporarySignatureDrawerOpen = ref(false);
const temporarySignatureSendingByDraft = ref<Record<string, boolean>>({});
const mopFillSaving = ref(false);
const mopUploadSaving = ref(false);
const mopResetting = ref(false);
const mopEditing = useMopSheetEditing();
const signedMopUploadedAt = mopEditing.uploadedAt;
const filledMopResult = mopEditing.filledResult;
const mopCheckboxStates = mopEditing.checkboxStates;
const mopMaintenanceValues = mopEditing.maintenanceValues;
const mopCellEdits = mopEditing.cellEdits;
const mopClipboardCellText = mopEditing.clipboardCellText;
const mopFillDateTime = ref(defaultMopDateTimeLocal());
const mopCellSelection = useMopCellSelection();
const activeMopCellKey = mopCellSelection.activeKey;
const selectedMopCellKeys = mopCellSelection.selectedKeys;
const activeMopCellOverlayStyle = mopCellSelection.overlayStyle;
const sheetScrollRef = mopCellSelection.sheetScrollRef;
const signatureMessage = ref("");
const signatureMessageType = ref("");
const signatureCanvas = useMopSignatureCanvas();
const signatureCanvasRef = signatureCanvas.canvasRef;
const signatureHasInk = signatureCanvas.hasInk;
let signatureSearchTimer: ReturnType<typeof setTimeout> | null = null;
let externalSignatureSearchTimer: ReturnType<typeof setTimeout> | null = null;
let signatureSearchRequestSeq = 0;
let externalSignatureSearchRequestSeq = 0;
const temporarySignaturePolling = useGuardedPolling(() => loadTemporarySignatures({ silent: true }), 5000);
const formalSignaturePolling = useGuardedPolling(() => refreshSelectedFormalSignatures(), 8000);

const scopeLabel = computed(() => {
  const found = props.scopeOptions.find((item) => normalizeMopScope(item.value) === scope.value);
  return found?.label || scope.value || "全部";
});

const boundNoticeCount = computed(() => notices.value.filter((item) => item.mop_binding).length);
const uploadedMopNoticeCount = computed(() => notices.value.filter((item) => noticeMopUploaded(item)).length);
const pendingMopNoticeCount = computed(() => notices.value.filter((item) => mopNoticeNeedsAction(item)).length);

const filteredNotices = computed(() => {
  const query = compactSearchText(noticeSearch.value);
  const items = notices.value.filter((item) => {
    const ended = noticeIsEnded(item);
    if (noticeStatusFilter.value === "ongoing" && ended) return false;
    if (noticeStatusFilter.value === "closed" && !ended) return false;
    if (noticeStatusFilter.value === "pending" && !mopNoticeNeedsAction(item)) return false;
    if (noticeStatusFilter.value === "bound" && !item.mop_binding) return false;
    if (noticeStatusFilter.value === "unbound" && item.mop_binding) return false;
    if (noticeStatusFilter.value === "uploaded" && !noticeMopUploaded(item)) return false;
    if (!query) return true;
    return compactSearchText([
      item.title,
      item.building,
      item.specialty,
      item.maintenance_cycle,
      item.location,
      item.content,
      item.reason,
    ].join(" ")).includes(query);
  });
  return sortMopNoticesForAction(items, {
    needsAction: mopNoticeNeedsAction,
    isEnded: noticeIsEnded,
  });
});

const selectedNotice = computed(() => notices.value.find((item) => item.notice_key === selectedNoticeKey.value) || null);
const selectedNoticeSourceRecordId = computed(() => String(
  selectedNotice.value?.source_record_id
  || selectedNotice.value?.record_id
  || "",
).trim());
const selectedNoticeSpecialty = computed(() => String(
  mopSpecialtyText(selectedNotice.value)
).trim());
const selectedMop = computed(() => mopCandidates.value.find((item) => item.record_id === selectedMopRecordId.value) || null);
const recommendedMopRecordId = computed(() => String(selectedNotice.value?.mop_binding?.mop_record_id || "").trim());
const selectedMopAttachments = computed(() => {
  const items = selectedMop.value?.attachments;
  return Array.isArray(items) ? items : [];
});
const selectedAttachment = computed(() => {
  const key = selectedAttachmentToken.value;
  return selectedMopAttachments.value.find((item) => mopAttachmentKey(item) === key) || selectedMopAttachments.value[0] || null;
});
const signatureUsageNoticeKey = computed(() => {
  const noticeKey = String(selectedNotice.value?.notice_key || selectedNoticeKey.value || "").trim();
  if (!noticeKey) return "";
  const mopRecordId = String(
    selectedMop.value?.record_id
    || selectedMopRecordId.value
    || preview.value?.mop_record_id
    || "",
  ).trim();
  const attachment = selectedAttachment.value || {};
  const attachmentKey = String(
    mopAttachmentKey(attachment)
    || selectedAttachmentToken.value
    || selectedMop.value?.upload_id
    || attachment.upload_id
    || preview.value?.local_file?.upload_id
    || preview.value?.mop_file_name
    || "",
  ).trim();
  return [
    noticeKey,
    `mop:${mopRecordId || "none"}`,
    `attachment:${attachmentKey || "none"}`,
  ].join("|");
});
const mopEditSessionLock = useMopEditSessionLock({
  scope,
  selectedNotice,
  selectedMop,
  selectedAttachment,
  selectedNoticeSourceRecordId,
  selectedNoticeKey,
  selectedMopRecordId,
  selectedAttachmentToken,
  warnings,
});
const claimMopEditSession = mopEditSessionLock.claim;
const releaseMopEditSession = mopEditSessionLock.release;
const filteredMopCandidates = computed(() => {
  const query = compactSearchText(mopSearch.value);
  const specialty = selectedNoticeSpecialty.value;
  const keepRecordIds = new Set(
    [
      selectedMopRecordId.value,
      recommendedMopRecordId.value,
    ].map((item) => String(item || "").trim()).filter(Boolean),
  );
  const specialtyMatched = mopCandidates.value.filter((item) => {
    const recordId = String(item.record_id || "").trim();
    if (recordId && keepRecordIds.has(recordId)) return true;
    if (item.local_upload || item.source === "local_upload") return true;
    return mopCandidateMatchesSpecialty(item, specialty);
  });
  const items = query ? specialtyMatched.filter((item) => compactSearchText([
    item.title,
    ...Object.values(item.fields || {}),
  ].join(" ")).includes(query)) : specialtyMatched;
  return sortRecommendedMopCandidates(items, recommendedMopRecordId.value);
});
const orderedMopCandidates = computed(() => sortRecommendedMopCandidates(filteredMopCandidates.value, recommendedMopRecordId.value));
const canBind = computed(() => Boolean(selectedNotice.value && selectedMop.value));
const canPreview = computed(() => Boolean(selectedNotice.value && selectedMop.value && selectedAttachment.value));
const openMopDisabledReason = computed(() => {
  if (openMopBusy.value) return "";
  if (!selectedNotice.value) return "请先选择左侧通告";
  if (!selectedMop.value) return "请先选择 MOP 表格";
  if (!selectedAttachment.value) return "当前 MOP 没有可打开的表格附件";
  return "";
});
const openMopBusy = computed(() => saving.value || previewLoading.value);
const openMopButtonText = computed(() => {
  if (saving.value) return "自动绑定中";
  if (previewLoading.value) return "加载表格中";
  return "打开填写";
});
const mopFlowSteps = computed(() => {
  const noticeReady = Boolean(selectedNotice.value);
  const mopReady = Boolean(selectedMop.value && selectedAttachment.value);
  const previewReady = Boolean(previewMode.value && preview.value?.local_file?.path && activeSheet.value);
  const signaturesReady = Boolean(hasImplementerSignature.value && hasAuditorSignature.value && allSelectedSignaturesReady.value);
  const uploadReady = Boolean(canUploadSignedMop.value);
  const uploaded = Boolean(signedMopUploadedAt.value);
  return [
    {
      key: "notice",
      index: "1",
      label: "选维保通告",
      text: noticeReady ? "已选择" : "待选择",
      state: noticeReady ? "done" : "active",
    },
    {
      key: "mop",
      index: "2",
      label: "选 MOP 表格",
      text: mopReady ? "已选择" : "待选择",
      state: mopReady ? "done" : noticeReady ? "active" : "pending",
    },
    {
      key: "fill",
      index: "3",
      label: "填写并签名",
      text: signaturesReady ? "已完成" : previewReady ? "待签名" : "待填写",
      state: signaturesReady ? "done" : previewReady ? "active" : "pending",
    },
    {
      key: "upload",
      index: "4",
      label: "上传归档",
      text: uploaded ? signedMopUploadedAtText.value : uploadReady ? "可上传" : "待完成",
      state: uploaded ? "done" : uploadReady ? "active" : "pending",
    },
  ];
});
const signatureSearchStatus = computed(() => {
  if (signatureLoading.value) return "搜索中";
  const count = signaturePeople.value.length;
  if (count > 0) {
    return signaturePeopleTotal.value > count ? `已找到 ${count} / ${signaturePeopleTotal.value} 人` : `已找到 ${count} 人`;
  }
  return signatureSearch.value.trim() ? "暂未找到人员" : "";
});
const externalSignatureSearchStatus = computed(() => {
  if (externalSignatureLoading.value) return "搜索中";
  const count = externalSignaturePeople.value.length;
  if (count > 0) {
    return externalSignaturePeopleTotal.value > count ? `已找到 ${count} / ${externalSignaturePeopleTotal.value} 个` : `已找到 ${count} 个`;
  }
  return externalSignatureSearch.value.trim() ? "暂未找到其他人员签名" : "";
});
const previewTitle = computed(() => {
  if (!preview.value) return "MOP表格";
  return `${preview.value.mop_title || selectedMop.value?.title || "MOP表格"} · ${preview.value.attachment?.name || ""}`;
});
const activeSignatureRecordId = computed(() => {
  const records = signatureSelectedRecords.value[signatureRole.value] || [];
  return records[records.length - 1] || "";
});
const selectedRoleSignaturePeople = computed(() => selectedSignaturePeople(signatureRole.value));
const selectedRoleUnsignedCount = computed(() => (
  selectedRoleSignaturePeople.value.filter((person) => !personReadyForMop(person)).length
));
const selectedRoleSignatureStatusText = computed(() => {
  const total = selectedRoleSignaturePeople.value.length;
  if (!total) return "未选择人员";
  const signed = Math.max(0, total - selectedRoleUnsignedCount.value);
  return selectedRoleUnsignedCount.value ? `${signed}/${total} 已签` : "签名齐全";
});
const signatureRoleSummaryItems = computed<MopSignatureRoleSummaryItem[]>(() => (
  ([
    ["implementer", "维护实施人"],
    ["auditor", "维护审核人"],
  ] as Array<[MopSignatureRole, string]>).map(([role, label]) => ({
    role,
    label,
    totalCount: selectedSignaturePeople(role).length,
    companyCount: selectedFormalSignaturePeople(role).length,
    companyUnsigned: selectedFormalSignatureNotReadyCount(role),
    temporaryCount: selectedTemporarySignaturePeople(role).length,
    temporaryUnsigned: selectedTemporarySignatureUnsignedCount(role),
  }))
));
const signatureGuideItems = computed(() => [
  {
    key: "time",
    label: "时间",
    value: mopMaintenanceTimeValidationMessage.value ? "待补" : "已完成",
    text: mopMaintenanceTimeValidationMessage.value || "已填写",
    ready: !mopMaintenanceTimeValidationMessage.value,
    tone: "time",
    role: "" as MopSignatureRole | "",
  },
  {
    key: "implementer",
    label: "维护实施人",
    value: `${implementerSignatureDisplayCount.value}/${requiredImplementerSignatureCount.value || 1}`,
    text: implementerSignatureReady.value ? "已签名" : "待签名",
    ready: implementerSignatureReady.value,
    tone: "implementer",
    role: "implementer" as MopSignatureRole,
  },
  {
    key: "auditor",
    label: "维护审核人",
    value: `${auditorSignatureDisplayCount.value}/${requiredAuditorSignatureCount.value}`,
    text: auditorSignatureReady.value ? "已签名" : "待签名",
    ready: auditorSignatureReady.value,
    tone: "auditor",
    role: "auditor" as MopSignatureRole,
  },
  {
    key: "upload",
    label: "上传",
    value: canUploadSignedMop.value ? "可上传" : "待完成",
    text: canUploadSignedMop.value ? "可上传" : uploadSignedMopDisabledReason.value,
    ready: canUploadSignedMop.value,
    tone: "upload",
    role: "" as MopSignatureRole | "",
  },
]);
const signatureTaskSummaryText = computed(() => {
  const items: string[] = [];
  if (mopMaintenanceTimeValidationMessage.value) items.push("时间待补");
  if (!implementerSignatureReady.value) {
    items.push(`实施人 ${implementerSignatureDisplayCount.value}/${requiredImplementerSignatureCount.value || 1}`);
  }
  if (!auditorSignatureReady.value) {
    items.push(`审核人 ${auditorSignatureDisplayCount.value}/${requiredAuditorSignatureCount.value}`);
  }
  if (unsignedSelectedSignatureCount.value > 0) items.push(`${unsignedSelectedSignatureCount.value} 人待处理`);
  return items.length ? items.join(" · ") : "签名已齐全，可写入 MOP";
});
const activeSignaturePerson = computed(() => (
  signaturePadTarget.value
  || selectedFormalSignaturePeople(signatureRole.value).find((item) => item.record_id === activeSignatureRecordId.value)
  || null
));
const currentUserOpenId = computed(() => String(currentAuthUser.value?.open_id || "").trim());
function isCurrentUserSignaturePerson(person: Dict | null | undefined): boolean {
  const personOpenId = String(person?.open_id || "").trim();
  return Boolean(personOpenId && currentUserOpenId.value && personOpenId === currentUserOpenId.value);
}
function staffSignatureWebSignDisabledReason(person: Dict | null | undefined): string {
  if (!String(person?.record_id || "").trim()) return "该人员资料不完整，无法网页手写";
  if (!isCurrentUserSignaturePerson(person)) return "网页手写只能保存当前登录用户本人的签名，请发送签名链接给对方";
  return "";
}
const currentRoleOtherSignatureDrafts = computed(() => (
  otherSignatureDrafts.value.filter((item) => String(item.role || "") === signatureRole.value)
));
const currentRoleOtherSignaturePeople = computed(() => (
  selectedTemporarySignaturePeople(signatureRole.value)
));
const currentRoleOtherSignatureDisplayRows = computed(() => {
  const peopleRows = currentRoleOtherSignaturePeople.value.map((person, index) => ({
    kind: "person",
    row_key: `person:${signaturePersonKey(person) || index}`,
    person,
    draft: {} as Dict,
    signed: personHasUsableSignature(person),
    display_name: temporarySignatureDisplayName(person, index),
    priority: otherSignaturePersonPriority(person),
    original_index: index,
  }));
  const draftRows = currentRoleOtherSignatureDrafts.value.map((draft, index) => ({
    kind: "draft",
    row_key: `draft:${String(draft.draft_id || index)}`,
    person: {} as Dict,
    draft,
    signed: false,
    display_name: temporarySignatureDisplayName(draft, index),
    priority: otherSignatureDraftPriority(draft),
    original_index: currentRoleOtherSignaturePeople.value.length + index,
  }));
  return [...peopleRows, ...draftRows].sort((left, right) => {
    if (left.priority !== right.priority) return left.priority - right.priority;
    const leftNo = temporarySignatureDisplayNumber(left.display_name);
    const rightNo = temporarySignatureDisplayNumber(right.display_name);
    if (leftNo !== rightNo) return leftNo - rightNo;
    const nameCompare = left.display_name.localeCompare(right.display_name, "zh-Hans-CN");
    if (nameCompare !== 0) return nameCompare;
    return left.original_index - right.original_index;
  });
});
const currentRoleOtherSignaturePreviewRow = computed(() => currentRoleOtherSignatureDisplayRows.value[0] || null);
const currentRoleOtherSignatureUnsignedCount = computed(() => (
  currentRoleOtherSignatureDisplayRows.value.filter((row) => !row.signed).length
));
const signatureRoleHint = computed(() => {
  const label = signatureRole.value === "implementer" ? "维护实施人" : "维护审核人";
  return activeSignaturePerson.value ? `${label}可网页手写或发送链接。` : `请选择${label}。`;
});
const addOtherSignatureDisabledReason = computed(() => {
  if (!selectedNotice.value) return "请先选择左侧维保通告";
  if (!selectedNotice.value.notice_key) return "当前通告缺少记忆键，无法创建临时签名";
  return "";
});
const allSelectedSignaturePeople = computed(() => [
  ...selectedSignaturePeople("implementer"),
  ...selectedSignaturePeople("auditor"),
]);
const allSelectedSignaturesReady = computed(() => allSelectedSignaturePeople.value.every((person) => personReadyForMop(person)));
const openSignaturePadDisabledReason = computed(() => {
  if (signatureSaving.value) return "";
  if (!activeSignaturePerson.value) return "请先选择签名人员";
  const source = String(activeSignaturePerson.value.source || "");
  if (source === "temporary" || activeSignaturePerson.value.temp_id) {
    if (!activeSignaturePerson.value.temp_id) return "该临时人员签名会话不完整，无法手写签名";
    return "";
  }
  if (!activeSignaturePerson.value.record_id) return "该人员资料不完整，无法手写签名";
  const staffReason = staffSignatureWebSignDisabledReason(activeSignaturePerson.value);
  if (staffReason) return staffReason;
  return "";
});
const saveSignatureDisabledReason = computed(() => {
  if (!activeSignaturePerson.value) return "请先选择签名人员";
  if (!signatureHasInk.value) return "请先在签名区域手写签名";
  return "";
});
const canFillMopSignatures = computed(() => {
  if (!preview.value?.local_file?.path || !activeSheet.value) return false;
  return allSelectedSignaturesReady.value;
});
const hasImplementerSignature = computed(() => selectedSignaturePeople("implementer").some((person) => personReadyForMop(person)));
const hasAuditorSignature = computed(() => selectedSignaturePeople("auditor").some((person) => personReadyForMop(person)));
const signedImplementerCount = computed(() => selectedSignaturePeople("implementer").filter((person) => personReadyForMop(person)).length);
const signedAuditorCount = computed(() => selectedSignaturePeople("auditor").filter((person) => personReadyForMop(person)).length);
const unsignedSelectedSignatureCount = computed(() => allSelectedSignaturePeople.value.filter((person) => !personReadyForMop(person)).length);
const involvedPeopleRequirement = computed(() => detectInvolvedPeopleRequirement());
const mopMaintenanceStartTimeText = computed(() => maintenanceFieldValueByLabel("维护开始时间"));
const mopMaintenanceFinishTimeText = computed(() => maintenanceFieldValueByLabel("维护完成时间"));
const mopAuditConfirmTimeText = computed(() => maintenanceFieldValueByLabel("审核确认时间"));
const mopMaintenanceStartDate = computed(() => parseMopComparableDate(mopMaintenanceStartTimeText.value));
const mopMaintenanceFinishDate = computed(() => parseMopComparableDate(mopMaintenanceFinishTimeText.value));
const mopMaintenanceTimeOrderInvalid = computed(() => Boolean(
  mopMaintenanceStartDate.value
  && mopMaintenanceFinishDate.value
  && mopMaintenanceStartDate.value.getTime() > mopMaintenanceFinishDate.value.getTime()
));
const mopMaintenanceStartTimeReady = computed(() => Boolean(mopMaintenanceStartTimeText.value) && !mopMaintenanceTimeOrderInvalid.value);
const mopMaintenanceFinishTimeReady = computed(() => Boolean(mopMaintenanceFinishTimeText.value) && !mopMaintenanceTimeOrderInvalid.value);
const mopAuditConfirmTimeReady = computed(() => Boolean(mopAuditConfirmTimeText.value));
const mopMaintenanceTimeValidationMessage = computed(() => maintenanceTimeValidationMessage({
  startText: mopMaintenanceStartTimeText.value,
  finishText: mopMaintenanceFinishTimeText.value,
  auditText: mopAuditConfirmTimeText.value,
  timeOrderInvalid: mopMaintenanceTimeOrderInvalid.value,
}));
const requiredImplementerSignatureCount = computed(() => (
  resolveRequiredImplementerSignatureCount({
    involvedPeopleCount: involvedPeopleRequirement.value.count,
    selectedImplementerCount: selectedSignaturePeople("implementer").length,
  })
));
const requiredAuditorSignatureCount = computed(() => 1);
const implementerSignatureReady = computed(() => (
  requiredImplementerSignatureCount.value > 0
  && signedImplementerCount.value >= requiredImplementerSignatureCount.value
));
const auditorSignatureReady = computed(() => signedAuditorCount.value >= requiredAuditorSignatureCount.value);
const implementerSignatureDisplayCount = computed(() => (
  signatureDisplayCount({
    signedCount: signedImplementerCount.value,
    requiredCount: requiredImplementerSignatureCount.value,
  })
));
const auditorSignatureDisplayCount = computed(() => (
  signatureDisplayCount({
    signedCount: signedAuditorCount.value,
    requiredCount: requiredAuditorSignatureCount.value,
  })
));
const fillMopDisabledReason = computed(() => resolveFillMopDisabledReason({
  hasLocalFile: Boolean(preview.value?.local_file?.path),
  hasActiveSheet: Boolean(activeSheet.value),
  timeValidationMessage: mopMaintenanceTimeValidationMessage.value,
  selectedPeopleCount: allSelectedSignaturePeople.value.length,
  unsignedPeopleCount: unsignedSelectedSignatureCount.value,
}));
const canUploadSignedMop = computed(() => Boolean(
  resolveCanUploadSignedMop({
    hasLocalFile: Boolean(preview.value?.local_file?.path),
    hasActiveSheet: Boolean(activeSheet.value),
    hasSelectedNotice: Boolean(selectedNotice.value),
    hasSourceRecordId: Boolean(selectedNoticeSourceRecordId.value),
    hasImplementerSignature: hasImplementerSignature.value,
    hasAuditorSignature: hasAuditorSignature.value,
    allSelectedSignaturesReady: allSelectedSignaturesReady.value,
    timeValidationMessage: mopMaintenanceTimeValidationMessage.value,
    involvedPeopleRequirement: involvedPeopleRequirement.value,
    signedImplementerCount: signedImplementerCount.value,
  })
));
const signedMopUploadedAtText = computed(() => (
  signedMopUploadedAt.value ? `${formatMopUploadTime(signedMopUploadedAt.value)}已上传` : ""
));
const uploadSignedMopDisabledReason = computed(() => resolveUploadSignedMopDisabledReason({
  hasLocalFile: Boolean(preview.value?.local_file?.path),
  hasActiveSheet: Boolean(activeSheet.value),
  hasSelectedNotice: Boolean(selectedNotice.value),
  hasSourceRecordId: Boolean(selectedNoticeSourceRecordId.value),
  timeValidationMessage: mopMaintenanceTimeValidationMessage.value,
  hasImplementerSignature: hasImplementerSignature.value,
  hasAuditorSignature: hasAuditorSignature.value,
  unsignedPeopleCount: unsignedSelectedSignatureCount.value,
  involvedPeopleRequirement: involvedPeopleRequirement.value,
  signedImplementerCount: signedImplementerCount.value,
}));
const activeSheet = computed(() => {
  const sheets = Array.isArray(preview.value?.sheets) ? preview.value?.sheets : [];
  return sheets.find((item: Dict) => item.name === activeSheetName.value) || sheets[0] || null;
});
const activeSheetCheckboxCells = computed(() => {
  const items = activeSheet.value?.checkbox_cells;
  return Array.isArray(items) ? items : [];
});
const activeSheetCheckboxCellMap = computed(() => {
  const map = new Map<string, Dict>();
  for (const cell of activeSheetCheckboxCells.value) {
    map.set(`${Number(cell.row)}:${Number(cell.col)}`, cell);
  }
  return map;
});
const bulkFillCheckboxCells = computed(() => (
  activeSheetCheckboxCells.value.filter((cell) => Number(cell.row) >= 9)
));
const activeSheetMaintenanceFields = computed(() => {
  const items = activeSheet.value?.maintenance_fields;
  return Array.isArray(items) ? items : [];
});
const activeSheetMaintenanceValueCellMap = computed(() => {
  const map = new Map<string, Dict>();
  for (const field of activeSheetMaintenanceFields.value) {
    map.set(`${Number(field.row)}:${Number(field.value_col)}`, field);
  }
  return map;
});
const activeSheetFillableMaintenanceFieldMap = computed(() => {
  const map = new Map<string, Dict>();
  for (const field of activeSheetMaintenanceFields.value) {
    if (
      !roleForMaintenanceLabel(field.label)
      && (maintenanceFieldIsTime(field) || maintenanceFieldIsCompletion(field))
    ) {
      map.set(`${Number(field.row)}:${Number(field.value_col)}`, field);
    }
  }
  return map;
});
const activeSheetMaintenanceProtectedCellSet = computed(() => {
  const set = new Set<string>();
  for (const field of activeSheetMaintenanceFields.value) {
    set.add(`${Number(field.row)}:${Number(field.value_col)}`);
    set.add(`${Number(field.row)}:${Number(field.label_col)}`);
  }
  return set;
});
const mopFilledCount = computed(() => {
  const sheetName = String(activeSheet.value?.name || "");
  if (!sheetName) return 0;
  const checkboxCount = activeSheetCheckboxCells.value.filter((cell) => Boolean(mopCheckboxStates.value[checkboxKey(cell)])).length;
  const fieldCount = activeSheetMaintenanceFields.value.filter((field) => Boolean(mopMaintenanceValues.value[maintenanceKey(field)])).length;
  const editCount = Object.keys(mopCellEdits.value).filter((key) => key.startsWith(`${sheetName}:`)).length;
  return checkboxCount + fieldCount + editCount;
});
const mopCompletionItems = computed(() => {
  const fillTotal = activeSheetCheckboxCells.value.length + activeSheetMaintenanceFields.value.length;
  return buildMopCompletionItems({
    hasLocalFile: Boolean(preview.value?.local_file?.path),
    implementerCount: signedImplementerCount.value,
    auditorCount: signedAuditorCount.value,
    fillTotal,
    filledCount: mopFilledCount.value,
  });
});
const mopRequirementItems = computed(() => buildMopRequirementItems({
  hasNotice: Boolean(selectedNotice.value && selectedNoticeSourceRecordId.value),
  hasOpenedFile: Boolean(preview.value?.local_file?.path && activeSheet.value),
  hasImplementerSignature: hasImplementerSignature.value,
  hasAuditorSignature: hasAuditorSignature.value,
  uploadDisabledReason: uploadSignedMopDisabledReason.value,
  canUpload: canUploadSignedMop.value,
}));

const mopUploadFooterItems = computed(() => buildMopUploadFooterItems({
  startText: mopMaintenanceStartTimeText.value,
  finishText: mopMaintenanceFinishTimeText.value,
  auditText: mopAuditConfirmTimeText.value,
  startReady: mopMaintenanceStartTimeReady.value,
  finishReady: mopMaintenanceFinishTimeReady.value,
  auditReady: mopAuditConfirmTimeReady.value,
  implementerText: `${implementerSignatureDisplayCount.value}/${requiredImplementerSignatureCount.value}`,
  implementerReady: implementerSignatureReady.value,
  auditorText: `${auditorSignatureDisplayCount.value}/${requiredAuditorSignatureCount.value}`,
  auditorReady: auditorSignatureReady.value,
}));

const activeSheetColumnIndexes = computed(() => {
  const count = Math.max(0, Number(activeSheet.value?.column_count || 0));
  return Array.from({ length: count }, (_value, index) => index);
});
const signedSelectedPeopleByRole = computed(() => ({
  implementer: selectedSignaturePeople("implementer").filter((person) => personReadyForMop(person)),
  auditor: selectedSignaturePeople("auditor").filter((person) => personReadyForMop(person)),
}));
const activeSheetSignatureCellMap = computed(() => {
  const map = new Map<string, Dict[]>();
  for (const field of activeSheetMaintenanceFields.value) {
    const role = roleForMaintenanceLabel(field?.label);
    if (!role) continue;
    const row = Number(field.row);
    const valueCol = Number(field.value_col);
    const labelCol = Number(field.label_col);
    const baseCol = Number.isFinite(valueCol)
      ? valueCol
      : (Number.isFinite(labelCol) ? labelCol + 1 : 0);
    map.set(`${row}:${baseCol}`, signedSelectedPeopleByRole.value[role]);
  }
  return map;
});
const activeSheetSignatureRoleCellMap = computed(() => {
  const map = new Map<string, "implementer" | "auditor">();
  for (const field of activeSheetMaintenanceFields.value) {
    const role = roleForMaintenanceLabel(field?.label);
    if (!role) continue;
    const row = Number(field.row);
    const valueCol = Number(field.value_col);
    const labelCol = Number(field.label_col);
    const baseCol = Number.isFinite(valueCol)
      ? valueCol
      : (Number.isFinite(labelCol) ? labelCol + 1 : 0);
    map.set(`${row}:${baseCol}`, role);
  }
  return map;
});
const activeMopCellPosition = computed(() => mopCellSelection.activePosition());
const selectedMopCellPositions = computed(() => mopCellSelection.selectedPositions());
const singleMopCellSelected = computed(() => selectedMopCellKeys.value.length <= 1);
const activeMopCellCheckbox = computed(() => {
  const position = activeMopCellPosition.value;
  return position ? checkboxCellAt(position.row, position.col) : null;
});
const activeMopCellMaintenanceField = computed(() => {
  const position = activeMopCellPosition.value;
  return position ? maintenanceFieldAt(position.row, position.col) : null;
});
const activeMopCellEditable = computed(() => {
  const position = activeMopCellPosition.value;
  return Boolean(position && editableCellAt(position.row, position.col));
});
const activeMopCellEditableValue = computed(() => {
  const position = activeMopCellPosition.value;
  return position ? editableCellValue(position.row, position.col) : "";
});
const activeMopCellInfo = computed(() => {
  const position = activeMopCellPosition.value;
  if (!position) return null;
  const cellRef = `${columnLabel(position.col)}${position.row + 1}`;
  if (activeMopCellCheckbox.value) {
    return { label: `${activeMopCellCheckbox.value.cell_ref || cellRef} 选择项` };
  }
  if (activeMopCellMaintenanceField.value) {
    return { label: `${activeMopCellMaintenanceField.value.label || cellRef}` };
  }
  if (activeMopCellEditable.value) {
    return { label: `${cellRef} 普通单元格` };
  }
  return null;
});
const mopCellPopoverMode = computed<MopCellPopoverMode>(() => {
  if (!activeMopCellPosition.value) return "none";
  if (selectedMopCellKeys.value.length > 1) return "selection";
  if (activeMopCellCheckbox.value && singleMopCellSelected.value) return "checkbox";
  if (activeMopCellMaintenanceField.value && singleMopCellSelected.value) {
    if (maintenanceFieldIsTime(activeMopCellMaintenanceField.value)) return "field-time";
    if (maintenanceFieldIsCompletion(activeMopCellMaintenanceField.value)) return "field-completion";
  }
  if (activeMopCellEditable.value && singleMopCellSelected.value) return "raw";
  return "none";
});
const mopCellPopoverLabel = computed(() => {
  const position = activeMopCellPosition.value;
  if (!position) return "";
  if (mopCellPopoverMode.value === "selection") return `已选 ${selectedMopCellKeys.value.length} 个单元格`;
  if (activeMopCellCheckbox.value) return String(activeMopCellCheckbox.value.cell_ref || `${columnLabel(position.col)}${position.row + 1}`);
  if (activeMopCellMaintenanceField.value) return String(activeMopCellMaintenanceField.value.label || "");
  return `${columnLabel(position.col)}${position.row + 1} 普通单元格`;
});
const mopCellPopoverOptions = computed(() => {
  const cell = activeMopCellCheckbox.value;
  if (!cell) return [];
  return checkboxOptions(cell).map((option) => ({
    label: String(option.label || option.key || ""),
    value: checkboxOptionValue(option),
  })).filter((option) => option.label || option.value);
});
const mopCellPopoverCheckboxValue = computed(() => (
  activeMopCellCheckbox.value ? checkboxState(activeMopCellCheckbox.value) : ""
));
const activeSheetMergeMap = computed(() => {
  const map = new Map<string, { hidden: boolean; rowspan: number; colspan: number }>();
  const merges = Array.isArray(activeSheet.value?.merges) ? activeSheet.value?.merges : [];
  for (const merge of merges) {
    const row = Number(merge?.row || 0);
    const col = Number(merge?.col || 0);
    const rowspan = Math.max(1, Number(merge?.rowspan || 1));
    const colspan = Math.max(1, Number(merge?.colspan || 1));
    map.set(`${row}:${col}`, { hidden: false, rowspan, colspan });
    for (let rowOffset = 0; rowOffset < rowspan; rowOffset += 1) {
      for (let colOffset = 0; colOffset < colspan; colOffset += 1) {
        if (rowOffset === 0 && colOffset === 0) continue;
        map.set(`${row + rowOffset}:${col + colOffset}`, { hidden: true, rowspan: 1, colspan: 1 });
      }
    }
  }
  return map;
});

function columnLabel(index: number): string {
  const columns = Array.isArray(activeSheet.value?.columns) ? activeSheet.value?.columns : [];
  const existing = String(columns[index] || "").trim();
  if (existing) return existing;
  let value = Math.max(1, index + 1);
  let label = "";
  while (value > 0) {
    const remainder = (value - 1) % 26;
    label = String.fromCharCode(65 + remainder) + label;
    value = Math.floor((value - 1) / 26);
  }
  return label || String(index + 1);
}

function cellMergeSpan(rowIndex: number, colIndex: number): { hidden: boolean; rowspan: number; colspan: number } {
  return activeSheetMergeMap.value.get(`${rowIndex}:${colIndex}`) || { hidden: false, rowspan: 1, colspan: 1 };
}

function escapeRegExp(value: string): string {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isRecommendedMop(mop: Dict): boolean {
  return Boolean(recommendedMopRecordId.value && String(mop?.record_id || "") === recommendedMopRecordId.value);
}

function maintenanceFieldValueByLabel(labelText: string): string {
  const fields = activeSheetMaintenanceFields.value.filter((item) => String(item.label || "").includes(labelText));
  for (const field of fields) {
    const row = Number(field.row);
    const valueCol = Number(field.value_col ?? field.label_col ?? -1);
    const rowValues = Array.isArray(activeSheet.value?.rows?.[row]) ? activeSheet.value?.rows?.[row] : [];
    const candidates = [
      mopMaintenanceValues.value[maintenanceKey(field)],
      field.fill_value,
      Object.prototype.hasOwnProperty.call(mopCellEdits.value, rawCellKey(row, valueCol))
        ? mopCellEdits.value[rawCellKey(row, valueCol)]
        : "",
      field.value,
      rowValues?.[valueCol],
    ];
    for (const candidate of candidates) {
      const normalized = normalizeMopRequiredTimeText(candidate);
      if (normalized) return normalized;
    }
  }
  return "";
}

function sheetCellDisplayText(rowIndex: number, colIndex: number): string {
  return String(cellOverrideValue(rowIndex, colIndex) || activeSheet.value?.rows?.[rowIndex]?.[colIndex] || "");
}

function detectInvolvedPeopleRequirement(): { count: number; cell_ref: string } {
  const rows = Array.isArray(activeSheet.value?.rows) ? activeSheet.value?.rows : [];
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    for (const colIndex of activeSheetColumnIndexes.value) {
      const text = sheetCellDisplayText(rowIndex, colIndex);
      if (!String(text || "").includes("涉及人数")) continue;
      const inlineCount = parsePeopleCount(text);
      if (inlineCount > 0) {
        return { count: inlineCount, cell_ref: `${columnLabel(colIndex)}${rowIndex + 1}` };
      }
      for (const offset of [1, 2, 3]) {
        const nextCol = colIndex + offset;
        if (!activeSheetColumnIndexes.value.includes(nextCol)) continue;
        const count = parsePeopleCount(sheetCellDisplayText(rowIndex, nextCol));
        if (count > 0) {
          return { count, cell_ref: `${columnLabel(nextCol)}${rowIndex + 1}` };
        }
      }
    }
  }
  return { count: 0, cell_ref: "" };
}

function checkboxKey(cell: Dict): string {
  return makeMopCheckboxKey(String(activeSheet.value?.name || ""), cell);
}

function maintenanceKey(field: Dict): string {
  return makeMopMaintenanceKey(String(activeSheet.value?.name || ""), field);
}

function checkboxState(cell: Dict): string {
  return mopCheckboxStates.value[checkboxKey(cell)] || "";
}

function checkboxOptions(cell: Dict): Dict[] {
  const items = Array.isArray(cell?.options) ? cell.options : [];
  if (items.length) return items;
  return [
    { key: "normal", label: "正常" },
    { key: "abnormal", label: "异常" },
  ];
}

function checkboxOptionValue(option: Dict): string {
  return String(option?.label || option?.key || "").trim();
}

function checkboxStateLabel(cell: Dict): string {
  const state = checkboxState(cell);
  if (!state) return "";
  return checkboxOptions(cell).find((option) => checkboxOptionValue(option) === state)?.label || state;
}

function mopCellKey(rowIndex: number, colIndex: number): string {
  return makeMopCellKey(String(activeSheetName.value || activeSheet.value?.name || ""), rowIndex, colIndex);
}

function focusSheetWithoutScroll(): void {
  const sheet = sheetScrollRef.value;
  if (!sheet) return;
  try {
    sheet.focus({ preventScroll: true });
  } catch {
    sheet.focus();
  }
}

function updateActiveMopCellOverlayPosition(): void {
  mopCellSelection.updateOverlay(mopCellPopoverMode.value);
}

function isMopCellSelected(rowIndex: number, colIndex: number): boolean {
  return mopCellSelection.isSelected(mopCellKey(rowIndex, colIndex));
}

function mopCellIsVisible(rowIndex: number, colIndex: number): boolean {
  return Boolean(activeSheet.value) && !cellMergeSpan(rowIndex, colIndex).hidden;
}

function startMopCellSelection(rowIndex: number, colIndex: number, event: MouseEvent): void {
  if (!mopCellIsVisible(rowIndex, colIndex)) return;
  const key = mopCellKey(rowIndex, colIndex);
  mopCellSelection.start(rowIndex, colIndex, event, key);
  const signatureRoleForCell = signatureRoleAtCell(rowIndex, colIndex);
  if (signatureRoleForCell) {
    openSignatureManager(signatureRoleForCell);
  }
  nextTick(() => {
    focusSheetWithoutScroll();
    updateActiveMopCellOverlayPosition();
  });
}

function extendMopCellSelection(rowIndex: number, colIndex: number, event: MouseEvent): void {
  if (!activeSheet.value) return;
  mopCellSelection.extend({
    row: rowIndex,
    col: colIndex,
    event,
    sheetName: String(activeSheetName.value || activeSheet.value?.name || ""),
    columns: activeSheetColumnIndexes.value,
    visible: mopCellIsVisible,
  });
}

function finishMopCellSelection(): void {
  mopCellSelection.finish();
}

function clearMopCellSelection(): void {
  mopCellSelection.clear();
}

function activateMopCell(rowIndex: number, colIndex: number): void {
  const signatureRoleForCell = signatureRoleAtCell(rowIndex, colIndex);
  if (signatureRoleForCell) {
    mopCellSelection.setSingle(mopCellKey(rowIndex, colIndex), rowIndex, colIndex);
    openSignatureManager(signatureRoleForCell);
    nextTick(() => {
      focusSheetWithoutScroll();
      updateActiveMopCellOverlayPosition();
    });
    return;
  }
  if (!checkboxCellAt(rowIndex, colIndex) && !maintenanceFieldAt(rowIndex, colIndex) && !editableCellAt(rowIndex, colIndex)) return;
  mopCellSelection.setSingle(mopCellKey(rowIndex, colIndex), rowIndex, colIndex);
  nextTick(() => {
    focusSheetWithoutScroll();
    updateActiveMopCellOverlayPosition();
  });
}

function setCheckboxState(cell: Dict, state: string): void {
  mopEditing.setCheckbox(checkboxKey(cell), state);
}

function checkboxCellAt(rowIndex: number, colIndex: number): Dict | null {
  return activeSheetCheckboxCellMap.value.get(`${rowIndex}:${colIndex}`) || null;
}

function maintenanceFieldAt(rowIndex: number, colIndex: number): Dict | null {
  return activeSheetFillableMaintenanceFieldMap.value.get(`${rowIndex}:${colIndex}`) || null;
}

function rawCellKey(rowIndex: number, colIndex: number): string {
  return makeRawMopCellKey(String(activeSheet.value?.name || ""), rowIndex, colIndex);
}

function protectedMopCell(rowIndex: number, colIndex: number): boolean {
  if (!activeSheet.value || activeSheet.value.is_cover) return true;
  if (cellMergeSpan(rowIndex, colIndex).hidden) return true;
  if (checkboxCellAt(rowIndex, colIndex)) return true;
  if (activeSheetMaintenanceProtectedCellSet.value.has(`${rowIndex}:${colIndex}`)) return true;
  if (signatureRoleAtCell(rowIndex, colIndex)) return true;
  return false;
}

function editableCellAt(rowIndex: number, colIndex: number): boolean {
  return !protectedMopCell(rowIndex, colIndex);
}

function editableCellValue(rowIndex: number, colIndex: number): string {
  const key = rawCellKey(rowIndex, colIndex);
  if (Object.prototype.hasOwnProperty.call(mopCellEdits.value, key)) {
    return mopCellEdits.value[key];
  }
  const row = Array.isArray(activeSheet.value?.rows?.[rowIndex]) ? activeSheet.value?.rows?.[rowIndex] : [];
  return String(row?.[colIndex] || "");
}

function setEditableCellValue(rowIndex: number, colIndex: number, value: string): void {
  if (!editableCellAt(rowIndex, colIndex)) return;
  mopEditing.setCellEdit(rawCellKey(rowIndex, colIndex), String(value || ""));
}

function clearEditableCell(rowIndex: number, colIndex: number): void {
  mopEditing.clearCellEdit(rawCellKey(rowIndex, colIndex));
}

function clearSelectedMopCells(): void {
  const editNext = { ...mopCellEdits.value };
  const checkboxNext = { ...mopCheckboxStates.value };
  const maintenanceNext = { ...mopMaintenanceValues.value };
  let count = 0;
  for (const position of selectedMopCellPositions.value) {
    const rawKey = rawCellKey(position.row, position.col);
    if (Object.prototype.hasOwnProperty.call(editNext, rawKey)) {
      delete editNext[rawKey];
      count += 1;
    }
    const checkbox = checkboxCellAt(position.row, position.col);
    if (checkbox) {
      const key = checkboxKey(checkbox);
      if (Object.prototype.hasOwnProperty.call(checkboxNext, key)) {
        delete checkboxNext[key];
        count += 1;
      }
    }
    const field = maintenanceFieldAt(position.row, position.col);
    if (field) {
      const key = maintenanceKey(field);
      if (Object.prototype.hasOwnProperty.call(maintenanceNext, key)) {
        delete maintenanceNext[key];
        count += 1;
      }
    }
  }
  if (count) {
    mopEditing.replaceSheetValues({
      cellEdits: editNext,
      checkboxStates: checkboxNext,
      maintenanceValues: maintenanceNext,
    });
    signatureMessage.value = `已还原 ${count} 个填写项。`;
    signatureMessageType.value = "success";
  } else {
    signatureMessage.value = "选中区域没有可还原的填写项。";
    signatureMessageType.value = "failed";
  }
}

function setActiveEditableCellValue(value: string): void {
  const position = activeMopCellPosition.value;
  if (!position) return;
  setEditableCellValue(position.row, position.col, value);
}

function setActiveCheckboxState(value: string): void {
  const cell = activeMopCellCheckbox.value;
  if (!cell) return;
  setCheckboxState(cell, value);
}

function fillActiveMaintenanceDate(): void {
  const field = activeMopCellMaintenanceField.value;
  if (!field) return;
  fillMaintenanceField(field, formatMopDateTime(mopFillDateTime.value));
}

function fillActiveMaintenanceCompletion(value: string): void {
  const field = activeMopCellMaintenanceField.value;
  if (!field) return;
  fillMaintenanceField(field, value);
}

function clearActiveEditableCell(): void {
  const position = activeMopCellPosition.value;
  if (!position) return;
  clearEditableCell(position.row, position.col);
}

async function copyActiveMopSelection(): Promise<void> {
  const position = activeMopCellPosition.value;
  if (!position) return;
  if (selectedMopCellKeys.value.length > 1) await copySelectedMopCells();
  else await copyMopCell(position.row, position.col);
}

async function pasteActiveMopSelection(): Promise<void> {
  const position = activeMopCellPosition.value;
  if (!position) return;
  await pasteMopClipboardAt(position.row, position.col);
}

function restoreActiveMopSelection(): void {
  if (selectedMopCellKeys.value.length > 1) clearSelectedMopCells();
  else clearActiveEditableCell();
}

function mopCellCurrentText(rowIndex: number, colIndex: number): string {
  return String(cellOverrideValue(rowIndex, colIndex) || activeSheet.value?.rows?.[rowIndex]?.[colIndex] || "");
}

async function writeTextClipboard(text: string): Promise<void> {
  try {
    await navigator.clipboard?.writeText(text);
  } catch {
    // Browser clipboard can be blocked by permissions; keep the in-page clipboard.
  }
}

async function readTextClipboard(): Promise<string> {
  try {
    const text = await navigator.clipboard?.readText();
    if (typeof text === "string") return text;
  } catch {
    // Fall back to the in-page clipboard.
  }
  return mopClipboardCellText.value;
}

async function copyMopCell(rowIndex: number, colIndex: number): Promise<void> {
  const text = mopCellCurrentText(rowIndex, colIndex);
  mopEditing.setClipboardText(text);
  await writeTextClipboard(text);
  signatureMessage.value = `${columnLabel(colIndex)}${rowIndex + 1} 已复制。`;
  signatureMessageType.value = "success";
}

function selectedMopCellBounds(): { minRow: number; maxRow: number; minCol: number; maxCol: number } | null {
  return calculateSelectedMopCellBounds(selectedMopCellPositions.value);
}

async function copySelectedMopCells(): Promise<void> {
  const bounds = selectedMopCellBounds();
  if (!bounds) return;
  const lines: string[] = [];
  const selected = new Set(selectedMopCellKeys.value);
  for (let row = bounds.minRow; row <= bounds.maxRow; row += 1) {
    const values: string[] = [];
    for (const col of activeSheetColumnIndexes.value) {
      if (col < bounds.minCol || col > bounds.maxCol) continue;
      values.push(selected.has(mopCellKey(row, col)) ? mopCellCurrentText(row, col) : "");
    }
    lines.push(values.join("\t"));
  }
  const text = lines.join("\n");
  mopEditing.setClipboardText(text);
  await writeTextClipboard(text);
  signatureMessage.value = selectedMopCellKeys.value.length > 1
    ? `已复制 ${selectedMopCellKeys.value.length} 个单元格。`
    : "已复制 1 个单元格。";
  signatureMessageType.value = "success";
}

function pasteTextToMopCells(startRow: number, startCol: number, text: string): number {
  if (!editableCellAt(startRow, startCol)) return 0;
  const rows = parseMopClipboardMatrix(text);
  let count = 0;
  const next = { ...mopCellEdits.value };
  rows.forEach((rowValues, rowOffset) => {
    rowValues.forEach((value, colOffset) => {
      const rowIndex = startRow + rowOffset;
      const colIndex = startCol + colOffset;
      if (rowIndex < 0 || rowIndex >= (activeSheet.value?.rows || []).length) return;
      if (!activeSheetColumnIndexes.value.includes(colIndex)) return;
      if (!editableCellAt(rowIndex, colIndex)) return;
      next[rawCellKey(rowIndex, colIndex)] = String(value || "");
      count += 1;
    });
  });
  if (count) {
    mopEditing.replaceCellEdits(next);
  }
  return count;
}

function pasteSingleTextToSelectedMopCells(text: string): number {
  if (selectedMopCellKeys.value.length <= 1) return 0;
  const next = { ...mopCellEdits.value };
  let count = 0;
  for (const position of selectedMopCellPositions.value) {
    if (!editableCellAt(position.row, position.col)) continue;
    next[rawCellKey(position.row, position.col)] = String(text || "");
    count += 1;
  }
  if (count) {
    mopEditing.replaceCellEdits(next);
  }
  return count;
}

async function pasteMopClipboardAt(rowIndex: number, colIndex: number): Promise<void> {
  const text = await readTextClipboard();
  if (!text && !mopClipboardCellText.value) {
    signatureMessage.value = "当前没有可粘贴的单元格内容。";
    signatureMessageType.value = "failed";
    return;
  }
  const content = text || mopClipboardCellText.value;
  const isMatrixPaste = isMopMatrixClipboardText(content);
  const count = !isMatrixPaste && selectedMopCellKeys.value.length > 1
    ? pasteSingleTextToSelectedMopCells(content)
    : pasteTextToMopCells(rowIndex, colIndex, content);
  if (!count) {
    signatureMessage.value = "当前选中的单元格不可粘贴。";
    signatureMessageType.value = "failed";
    return;
  }
  signatureMessage.value = count > 1 ? `已粘贴 ${count} 个单元格。` : `${columnLabel(colIndex)}${rowIndex + 1} 已粘贴。`;
  signatureMessageType.value = "success";
}

function eventTargetIsTextInput(event: KeyboardEvent): boolean {
  const target = event.target as HTMLElement | null;
  if (!target) return false;
  const tag = target.tagName.toLowerCase();
  return tag === "input" || tag === "textarea" || target.isContentEditable;
}

async function handleMopTableKeydown(event: KeyboardEvent): Promise<void> {
  if (!activeMopCellPosition.value || eventTargetIsTextInput(event)) return;
  if (event.key === "Escape") {
    event.preventDefault();
    clearMopCellSelection();
    signatureMessage.value = "已取消单元格选择。";
    signatureMessageType.value = "success";
    return;
  }
  if (!(event.ctrlKey || event.metaKey)) return;
  const key = event.key.toLowerCase();
  if (key !== "c" && key !== "v") return;
  event.preventDefault();
  const { row, col } = activeMopCellPosition.value;
  if (key === "c") {
    if (selectedMopCellKeys.value.length > 1) {
      await copySelectedMopCells();
    } else {
      await copyMopCell(row, col);
    }
  } else {
    await pasteMopClipboardAt(row, col);
  }
}

function maintenanceFieldIsTime(field: Dict): boolean {
  const label = String(field?.label || "");
  return field?.kind === "datetime_placeholder"
    || label.includes("维护开始时间")
    || label.includes("维护完成时间")
    || label.includes("审核确认时间")
    || label.includes("日期")
    || label.includes("时间");
}

function maintenanceFieldIsCompletion(field: Dict): boolean {
  return String(field?.label || "").includes("维护完成情况");
}

function maintenanceFieldWritesValueOnly(field: Dict): boolean {
  const label = String(field?.label || "");
  return label.includes("维护完成时间") || label.includes("审核确认时间");
}

function fillMaintenanceField(field: Dict, value: string): void {
  const text = String(value || "").trim();
  if (!text) return;
  mopEditing.setMaintenanceValue(maintenanceKey(field), text);
}

function clearMopOutputState(): void {
  mopEditing.clearOutputState();
}

function clearMopFillState(options: { clearSignatures?: boolean } = {}): void {
  closeMopTransientUi();
  mopEditing.resetSheetValues();
  if (options.clearSignatures) {
    resetSignatureSelection();
    updateTemporarySignaturePolling();
    updateFormalSignaturePolling();
    signatureRole.value = "implementer";
    clearSignatureCanvas();
  }
}

function applyMopFillMemory(memory: Dict | null | undefined): number {
  const payload = memory?.payload && typeof memory.payload === "object" ? memory.payload as Dict : null;
  if (!payload || !activeSheet.value) return 0;
  let applied = 0;

  const checkboxByPosition = new Map<string, Dict>();
  for (const cell of activeSheetCheckboxCells.value) {
    checkboxByPosition.set(`${Number(cell.row)}:${Number(cell.col)}`, cell);
    if (cell.cell_ref) checkboxByPosition.set(`ref:${String(cell.cell_ref)}`, cell);
  }
  const nextCheckboxes = { ...mopCheckboxStates.value };
  for (const item of mopPayloadList(payload.checkboxes)) {
    const state = String(
      item.selection
      || item.selected_label
      || item.selected_key
      || item.state
      || ""
    ).trim();
    if (!state) continue;
    const cell = checkboxByPosition.get(`${Number(item.row)}:${Number(item.col)}`)
      || checkboxByPosition.get(`ref:${String(item.cell_ref || "")}`);
    if (!cell) continue;
    nextCheckboxes[checkboxKey(cell)] = state;
    applied += 1;
  }

  const fieldByKey = new Map<string, Dict>();
  for (const field of activeSheetMaintenanceFields.value) {
    for (const key of mopMemoryFieldKeys(field)) {
      if (!fieldByKey.has(key)) fieldByKey.set(key, field);
    }
  }
  const nextFields = { ...mopMaintenanceValues.value };
  for (const item of mopPayloadList(payload.fields)) {
    const fillValue = String(item.fill_value || "").trim();
    if (!fillValue) continue;
    const field = mopMemoryFieldKeys(item).map((key) => fieldByKey.get(key)).find(Boolean);
    if (!field) continue;
    nextFields[maintenanceKey(field)] = fillValue;
    applied += 1;
  }

  const nextCellEdits = { ...mopCellEdits.value };
  const currentSheet = String(activeSheetName.value || activeSheet.value?.name || "");
  for (const item of mopPayloadList(payload.cell_edits)) {
    const sheet = String(item.sheet || currentSheet);
    if (sheet && sheet !== currentSheet) continue;
    const row = Number(item.row);
    const col = Number(item.col);
    if (!Number.isFinite(row) || !Number.isFinite(col)) continue;
    if (!editableCellAt(row, col)) continue;
    nextCellEdits[rawCellKey(row, col)] = String(item.value ?? "");
    applied += 1;
  }

  if (applied) {
    mopEditing.replaceSheetValues({
      checkboxStates: nextCheckboxes,
      maintenanceValues: nextFields,
      cellEdits: nextCellEdits,
    });
  }
  return applied;
}

function markAllCheckboxes(state: string): void {
  const next = { ...mopCheckboxStates.value };
  for (const cell of bulkFillCheckboxCells.value) {
    const options = checkboxOptions(cell);
    const preferred = options.find((option) => String(option.label || "").includes("正常"))
      || options.find((option) => String(option.label || "").includes("开启"))
      || options.find((option) => String(option.label || "").includes("已完成"))
      || options[0];
    next[checkboxKey(cell)] = checkboxOptionValue(preferred) || state;
  }
  mopEditing.replaceCheckboxStates(next);
}

function checkboxDisplayValue(cell: Dict): string {
  const state = checkboxState(cell);
  const original = String(cell.value || "");
  if (!state) return original;
  const labels = checkboxOptions(cell).map((option) => String(option.label || option.key || "").trim()).filter(Boolean);
  let text = original || labels.map((label) => `□${label}`).join(" ");
  for (const label of labels) {
    const mark = label === state ? "☑" : "□";
    const prefixPattern = new RegExp(`[□☐■☑√✔✓]\\s*${escapeRegExp(label)}`, "g");
    const bracketPattern = new RegExp(`${escapeRegExp(label)}\\[[^\\]]*\\]`, "g");
    if (prefixPattern.test(text)) {
      text = text.replace(prefixPattern, `${mark}${label}`);
    } else if (bracketPattern.test(text)) {
      text = text.replace(bracketPattern, `${label}[${label === state ? "√" : " "}]`);
    }
  }
  return text;
}

function mopCellHasOverride(rowIndex: number, colIndex: number): boolean {
  const checkbox = checkboxCellAt(rowIndex, colIndex);
  if (checkbox && checkboxState(checkbox)) return true;
  const field = activeSheetMaintenanceValueCellMap.value.get(`${rowIndex}:${colIndex}`);
  if (field && mopMaintenanceValues.value[maintenanceKey(field)]) return true;
  return Object.prototype.hasOwnProperty.call(mopCellEdits.value, rawCellKey(rowIndex, colIndex));
}

function cellOverrideValue(rowIndex: number, colIndex: number): string {
  const checkbox = checkboxCellAt(rowIndex, colIndex);
  if (checkbox) return checkboxDisplayValue(checkbox);
  const field = activeSheetMaintenanceValueCellMap.value.get(`${rowIndex}:${colIndex}`);
  if (field) {
    const value = mopMaintenanceValues.value[maintenanceKey(field)] || "";
    if (value) {
      const labelCol = Number(field.label_col);
      if (maintenanceFieldWritesValueOnly(field)) return value;
      return labelCol === colIndex ? `${field.label || ""}：${value}` : value;
    }
  }
  const editKey = rawCellKey(rowIndex, colIndex);
  if (Object.prototype.hasOwnProperty.call(mopCellEdits.value, editKey)) {
    return mopCellEdits.value[editKey];
  }
  return "";
}

function buildMopCheckboxPayload(): Dict[] {
  return buildMopCheckboxPayloadFromCells(activeSheetCheckboxCells.value, checkboxState);
}

function buildMopFieldPayload(): Dict[] {
  return buildMopFieldPayloadFromFields(
    activeSheetMaintenanceFields.value,
    (field) => mopMaintenanceValues.value[maintenanceKey(field)] || "",
  );
}

function buildMopCellEditPayload(): Dict[] {
  return buildMopCellEditPayloadFromEdits(mopCellEdits.value);
}

function buildMopSignaturePayload(): Dict[] {
  return buildMopSignaturePayloadFromPeople({
    formalImplementers: selectedFormalSignaturePeople("implementer").filter((person) => personReadyForMop(person)),
    formalAuditors: selectedFormalSignaturePeople("auditor").filter((person) => personReadyForMop(person)),
    otherImplementers: selectedTemporarySignaturePeople("implementer").filter((person) => personReadyForMop(person)),
    otherAuditors: selectedTemporarySignaturePeople("auditor").filter((person) => personReadyForMop(person)),
  });
}

function buildMopRequestPayload(extra: Dict = {}): Dict {
  return {
    scope: scope.value,
    local_file_path: preview.value?.local_file?.path || "",
    mop_record_id: preview.value?.mop_record_id || selectedMop.value?.record_id || "",
    mop_title: preview.value?.mop_title || selectedMop.value?.title || "",
    mop_file_name: preview.value?.mop_file_name || selectedAttachment.value?.name || preview.value?.local_file?.file_name || "",
    notice_key: selectedNotice.value?.notice_key || selectedNoticeKey.value || "",
    sheet_name: activeSheet.value?.name || "",
    fields: buildMopFieldPayload(),
    checkboxes: buildMopCheckboxPayload(),
    cell_edits: buildMopCellEditPayload(),
    signatures: buildMopSignaturePayload(),
    ...extra,
  };
}

function personHasUsableSignature(person: Dict | null | undefined): boolean {
  return mopPersonHasUsableSignature(person);
}

function personReadyForMop(person: Dict | null | undefined): boolean {
  if (!personHasUsableSignature(person)) return false;
  const source = String(person?.source || "");
  if (source === "temporary" || source === "external" || person?.temp_id) return true;
  if (person?.usage_rejected) return false;
  return isCurrentUserSignaturePerson(person) || Boolean(person?.usage_confirmed);
}

function selectedFormalSignatureNotReadyCount(role: MopSignatureRole): number {
  return selectedFormalSignaturePeople(role).filter((person) => !personReadyForMop(person)).length;
}

function selectedFormalSignatureMissingSignatureCount(role: MopSignatureRole): number {
  return selectedFormalSignaturePeople(role).filter((person) => !personHasUsableSignature(person)).length;
}

function handleSignatureImageError(recordId: unknown): void {
  markSignatureUnavailable(recordId);
  signatureMessage.value = "该人员签名附件不可用，请重新手写保存。";
  signatureMessageType.value = "failed";
}

function handleSelectedSignatureImageError(person: Dict): void {
  const source = String(person?.source || "");
  if (source === "temporary" || source === "external" || person?.temp_id) {
    markOtherSignatureUnavailable(person);
  } else {
    markSignatureUnavailable(person?.record_id);
  }
  signatureMessage.value = "该签名附件不可用，请重新签名保存。";
  signatureMessageType.value = "failed";
}

function resizeSignatureCanvas(): void {
  signatureCanvas.resize();
}

function ensureSignatureCanvasObserver(): void {
  signatureCanvas.observe();
}

function disconnectSignatureCanvasObserver(): void {
  signatureCanvas.disconnect();
}

async function openSignaturePad(): Promise<void> {
  if (openSignaturePadDisabledReason.value) return;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  signatureCanvas.resetInk();
  closeSignatureDrawers();
  signaturePadOpen.value = true;
  await nextTick();
  disconnectSignatureCanvasObserver();
  ensureSignatureCanvasObserver();
  resizeSignatureCanvas();
}

async function openSignaturePadForPerson(person: Dict): Promise<void> {
  const source = String(person?.source || "");
  const tempId = String(person?.temp_id || "").trim();
  const recordId = String(person?.record_id || "").trim();
  if (source === "external") {
    if (!recordId) return;
    signaturePadTarget.value = person;
  } else if (source === "temporary" || tempId) {
    if (!tempId) return;
    signaturePadTarget.value = person;
  } else {
    if (!recordId) return;
    const disabledReason = staffSignatureWebSignDisabledReason(person);
    if (disabledReason) {
      signatureMessage.value = disabledReason;
      signatureMessageType.value = "failed";
      return;
    }
    signaturePadTarget.value = null;
    setActiveSignaturePerson(recordId);
  }
  await nextTick();
  await openSignaturePad();
}

function otherSignatureWebSignDisabledReason(person: Dict): string {
  const source = String(person?.source || "");
  if (source === "external") {
    return String(person?.record_id || "").trim() ? "" : "该外部人员资料不完整，无法网页手写";
  }
  return String(person?.temp_id || "").trim() ? "" : "该临时人员签名会话不完整，无法网页手写";
}

async function openSignaturePadForDraft(draft: Dict): Promise<void> {
  const draftId = String(draft?.draft_id || "").trim();
  if (!draftId) return;
  const role = String(draft.role || signatureRole.value) === "auditor" ? "auditor" : "implementer";
  const displayName = String(draft.display_name || "").trim() || nextOtherSignatureDisplayName(role);
  temporarySignatureSendingByDraft.value = {
    ...temporarySignatureSendingByDraft.value,
    [draftId]: true,
  };
  otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
    String(item.draft_id || "") === draftId
      ? { ...item, display_name: displayName, status: "sending", error: "" }
      : item
  ));
  try {
    const data = await createTemporarySignatureSession({
      scope: scope.value,
      noticeKey: selectedNotice.value?.notice_key || "",
      noticeTitle: selectedNotice.value?.title || "",
      specialty: selectedNoticeSpecialty.value,
      role,
      displayName,
    });
    if (data) {
      mergeTemporarySignatures([data]);
      unhideSignaturePerson(data);
      otherSignatureDrafts.value = otherSignatureDrafts.value.filter((item) => String(item.draft_id || "") !== draftId);
      signaturePadTarget.value = data;
      updateTemporarySignaturePolling();
      await nextTick();
      await openSignaturePad();
    }
  } catch (error) {
    otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
      String(item.draft_id || "") === draftId
        ? { ...item, status: "failed", error: error instanceof Error ? error.message : "创建临时签名失败" }
        : item
    ));
    signatureMessage.value = error instanceof Error ? error.message : "创建临时签名失败";
    signatureMessageType.value = "failed";
  } finally {
    const next = { ...temporarySignatureSendingByDraft.value };
    delete next[draftId];
    temporarySignatureSendingByDraft.value = next;
  }
}

function closeSignaturePad(): void {
  signatureCanvas.stopDrawing();
  signaturePadOpen.value = false;
  signatureCanvas.resetInk();
  signaturePadTarget.value = null;
  disconnectSignatureCanvasObserver();
}

function closeMopTransientUi(): void {
  mopCellSelection.clear();
  closeSignatureManager();
  if (signaturePadOpen.value) closeSignaturePad();
}

function startSignatureDraw(event: PointerEvent): void {
  signatureCanvas.startDraw(event, Boolean(activeSignaturePerson.value && !signatureSaving.value));
}

function moveSignatureDraw(event: PointerEvent): void {
  signatureCanvas.moveDraw(event);
}

function endSignatureDraw(event: PointerEvent): void {
  signatureCanvas.endDraw(event);
}

function clearSignatureCanvas(): void {
  signatureCanvas.clear();
}

function selectSignaturePerson(recordId: string): void {
  selectSignaturePersonByRole(signatureRole.value, recordId);
  clearMopOutputState();
  signatureMessage.value = "";
  signatureMessageType.value = "";
  clearSignatureCanvas();
  updateFormalSignaturePolling();
}

function setActiveSignaturePerson(recordId: string): void {
  selectSignaturePerson(recordId);
}

function removeSignaturePerson(role: "implementer" | "auditor", recordId: string): void {
  removeSignaturePersonByRole(role, recordId);
  clearMopOutputState();
  clearSignatureCanvas();
}

function closeSignatureDrawers(): void {
  selectedSignatureDrawerOpen.value = false;
  temporarySignatureDrawerOpen.value = false;
}

function setSelectedSignatureDrawerOpen(value: boolean): void {
  selectedSignatureDrawerOpen.value = value;
  if (value) temporarySignatureDrawerOpen.value = false;
}

function setTemporarySignatureDrawerOpen(value: boolean): void {
  temporarySignatureDrawerOpen.value = value;
  if (value) selectedSignatureDrawerOpen.value = false;
}

function toggleSelectedSignatureDrawer(): void {
  setSelectedSignatureDrawerOpen(!selectedSignatureDrawerOpen.value);
}

function toggleTemporarySignatureDrawer(): void {
  setTemporarySignatureDrawerOpen(!temporarySignatureDrawerOpen.value);
}

function ensureOtherSignatureDraftName(draft: Dict): void {
  ensureOtherSignatureDraftNameByRole(draft, signatureRole.value);
}

function activateSelectedSignaturePerson(person: Dict): void {
  if (String(person?.source || "") === "temporary" || String(person?.source || "") === "external" || person?.temp_id) return;
  setActiveSignaturePerson(String(person.record_id || ""));
}

function roleForMaintenanceLabel(label: unknown): "implementer" | "auditor" | "" {
  return roleForMopMaintenanceLabel(label);
}

function cellSignatures(rowIndex: number, colIndex: number): Dict[] {
  return activeSheetSignatureCellMap.value.get(`${rowIndex}:${colIndex}`) || [];
}

function signatureRoleAtCell(rowIndex: number, colIndex: number): "implementer" | "auditor" | "" {
  return activeSheetSignatureRoleCellMap.value.get(`${rowIndex}:${colIndex}`) || "";
}

function activeSheetRowHeightPx(rowIndex: number): number {
  const rowHeights = activeSheet.value?.row_heights;
  const explicit = rowHeights && typeof rowHeights === "object"
    ? Number((rowHeights as Dict)[String(rowIndex)] ?? (rowHeights as Dict)[rowIndex])
    : 0;
  const fallback = Number(activeSheet.value?.default_row_height_px || 20);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  return Number.isFinite(fallback) && fallback > 0 ? fallback : 20;
}

function signatureMaxHeightPx(rowIndex: number): number {
  return Math.max(1, Math.round(activeSheetRowHeightPx(rowIndex) * 1.5));
}

function signatureCellStyle(rowIndex: number): Record<string, string> {
  return {
    minHeight: `${activeSheetRowHeightPx(rowIndex)}px`,
    "--mop-signature-max-height": `${signatureMaxHeightPx(rowIndex)}px`,
  };
}

function signatureImageStyle(rowIndex: number): Record<string, string> {
  return {
    maxHeight: `${signatureMaxHeightPx(rowIndex)}px`,
    maxWidth: "150px",
  };
}

function signatureMoreStyle(rowIndex: number): Record<string, string> {
  const height = Math.max(18, Math.min(28, signatureMaxHeightPx(rowIndex)));
  return {
    height: `${height}px`,
  };
}

function openSignatureManager(role: "implementer" | "auditor"): void {
  if (signatureRole.value !== role) {
    closeSignatureDrawers();
  }
  signatureRole.value = role;
  signatureManagerOpen.value = true;
}

function closeSignatureManager(): void {
  signatureManagerOpen.value = false;
  closeSignatureDrawers();
}

function scheduleSignaturePeopleSearch(): void {
  if (signatureSearchTimer) {
    clearTimeout(signatureSearchTimer);
  }
  signatureSearchTimer = setTimeout(() => {
    signatureSearchTimer = null;
    void loadSignaturePeople({ silent: true });
  }, 300);
}

async function loadSignaturePeople(options: { silent?: boolean } = {}): Promise<void> {
  const requestSeq = ++signatureSearchRequestSeq;
  signatureLoading.value = true;
  if (!options.silent) {
    signatureMessage.value = "";
    signatureMessageType.value = "";
  }
  try {
    const data = await fetchSignaturePeople({
      scope: scope.value,
      q: signatureSearch.value,
      noticeKey: signatureUsageNoticeKey.value,
      refresh: !options.silent,
      limit: 60,
    });
    if (requestSeq !== signatureSearchRequestSeq) return;
    signaturePeople.value = Array.isArray(data.people) ? data.people : [];
    signaturePeopleTotal.value = Number(data.count || data.total || signaturePeople.value.length || 0);
    rememberSignaturePeople(signaturePeople.value);
    const current = activeSignatureRecordId.value;
    if (!current && signaturePeople.value.length === 1) {
      selectSignaturePerson(String(signaturePeople.value[0].record_id || ""));
    }
  } catch (error) {
    if (requestSeq !== signatureSearchRequestSeq) return;
    signatureMessage.value = error instanceof Error ? error.message : "读取签名人员失败";
    signatureMessageType.value = "failed";
  } finally {
    if (requestSeq === signatureSearchRequestSeq) {
      signatureLoading.value = false;
      await nextTick();
      resizeSignatureCanvas();
    }
  }
}

async function saveMopSignature(): Promise<void> {
  if (!activeSignaturePerson.value || !signatureHasInk.value) return;
  signatureSaving.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const target = activeSignaturePerson.value;
    const source = String(target.source || "");
    const tempId = String(target.temp_id || "").trim();
    const signatureImage = signatureCanvas.dataUrl();
    if (source === "temporary" || tempId) {
      const data = await saveTemporarySignature(tempId, signatureImage);
      if (data) mergeTemporarySignatures([data]);
      unhideSignaturePerson(data || target);
      signatureMessage.value = `${data?.display_name || data?.name || target.name || "临时人员"} 已保存签名。`;
      updateTemporarySignaturePolling();
    } else if (source === "external") {
      const data = await saveExternalSignature(
        String(target.record_id || ""),
        String(target.name || target.display_name || ""),
        signatureImage,
      );
      const merged = { ...target, ...data, source: "external", role: target.role || signatureRole.value };
      mergeTemporarySignatures([merged]);
      unhideSignaturePerson(merged);
      externalSignaturePeople.value = externalSignaturePeople.value.map((item) => (
        String(item.record_id || "") === String(target.record_id || "") ? { ...item, ...data, source: "external" } : item
      ));
      signatureMessage.value = `${data.name || data.display_name || target.name || "其他人员"} 已保存签名。`;
    } else {
      const data = await saveStaffSignature(
        String(target.record_id || ""),
        String(target.name || ""),
        signatureImage,
      );
      signatureMessage.value = `${data.name || target.name || "签名"} 已保存到签名库。`;
      updateRememberedSignaturePerson(target.record_id, {
        has_signature: true,
        signature_count: 1,
        signature_preview_url: data.signature_preview_url || target.signature_preview_url || "",
        signature_version: data.signature_version || target.signature_version || "",
      });
      updateFormalSignaturePolling();
      const notificationWarning = String(data.notification_warning || "").trim();
      if (notificationWarning) {
        warnings.value = [...new Set([...warnings.value, notificationWarning])];
      }
    }
    clearMopOutputState();
    signatureMessageType.value = "success";
    clearSignatureCanvas();
    closeSignaturePad();
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "保存签名失败";
    signatureMessageType.value = "failed";
  } finally {
    signatureSaving.value = false;
  }
}

async function fillMopSignatures(): Promise<void> {
  if (!canFillMopSignatures.value || !preview.value?.local_file?.path) return;
  mopFillSaving.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await fillEngineerMop(buildMopRequestPayload());
    filledMopResult.value = data;
    signedMopUploadedAt.value = "";
    signatureMessage.value = `已写入本地 MOP：${data.relative_path || data.file_name || "本地文件"}`;
    signatureMessageType.value = "success";
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "签名写入 MOP 失败";
    signatureMessageType.value = "failed";
  } finally {
    mopFillSaving.value = false;
  }
}

async function uploadSignedMop(): Promise<void> {
  if (uploadSignedMopDisabledReason.value || !preview.value?.local_file?.path || !selectedNotice.value) return;
  mopUploadSaving.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await uploadSignedEngineerMop(buildMopRequestPayload({
        source_record_id: selectedNoticeSourceRecordId.value,
        notice_title: selectedNotice.value.title || "",
        notice_key: selectedNotice.value.notice_key || "",
      }));
    filledMopResult.value = data.filled_file || filledMopResult.value;
    signedMopUploadedAt.value = String(data.uploaded_at || new Date().toISOString());
    const sourceRecordId = String(data.source_record_id || selectedNoticeSourceRecordId.value || "").trim();
    const noticeKey = String(data.notice_key || selectedNotice.value.notice_key || "").trim();
    const notice = notices.value.find((item) => (
      (noticeKey && String(item.notice_key || "") === noticeKey)
      || (sourceRecordId && String(item.source_record_id || item.record_id || "") === sourceRecordId)
    ));
    if (notice) {
      notice.mop_uploaded = true;
      notice.mop_attachment_count = Math.max(1, Number(notice.mop_attachment_count || 0));
      notice.mop_engineer_confirmed = true;
      notice.mop_supervisor_confirmed = true;
    }
    const warning = String(data.notification_warning || "").trim();
    const memoryWarning = String(data.memory_warning || "").trim();
    const warningsText = [warning, memoryWarning].filter(Boolean).join("；");
    signatureMessage.value = warningsText
      ? `已上传已签名 MOP，并已勾选确认项；${warningsText}`
      : "已上传已签名 MOP，并已勾选工程师确认、主管确认；下次同名 MOP 会自动带出本次填写内容。";
    signatureMessageType.value = "success";
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "上传已签名 MOP 失败";
    signatureMessageType.value = "failed";
  } finally {
    mopUploadSaving.value = false;
  }
}

async function resetMopSigning(): Promise<void> {
  if (!filledMopResult.value || !preview.value || !selectedMop.value) return;
  mopResetting.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const data = await resetEngineerMop({
      scope: scope.value,
      filledFilePath: String(filledMopResult.value.path || ""),
      mopRecordId: String(preview.value.mop_record_id || selectedMop.value.record_id || ""),
      fileToken: mopAttachmentKey(attachment),
      fileName: String(attachment.name || ""),
    });
    clearMopFillState({ clearSignatures: true });
    preview.value = data;
    const sheets = Array.isArray(data.sheets) ? data.sheets : [];
    activeSheetName.value = String(sheets[0]?.name || activeSheetName.value || "");
    signatureMessage.value = "已删除旧签名文件，并重新下载干净 MOP。";
    signatureMessageType.value = "success";
    await nextTick();
    ensureSignatureCanvasObserver();
    resizeSignatureCanvas();
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "重新下载干净 MOP 失败";
    signatureMessageType.value = "failed";
  } finally {
    mopResetting.value = false;
  }
}

function personSignatureLinkTitle(person: Dict): string {
  if (!String(person?.record_id || "").trim()) return "该人员资料不完整，无法发送链接";
  return personHasUsableSignature(person)
    ? "重新发送签名链接；新签名保存前，原签名仍可继续使用"
    : "发送签名链接";
}

function shortClockText(date = new Date()): string {
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  return `${hour}:${minute}`;
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function unsignedStaffSignaturePeople(role: MopSignatureRole): Dict[] {
  return selectedFormalSignaturePeople(role)
    .filter((person) => !personHasUsableSignature(person))
    .filter((person) => String(person.record_id || "").trim());
}

function staffSignaturesRequiringConfirmation(role: MopSignatureRole): Dict[] {
  return selectedFormalSignaturePeople(role)
    .filter((person) => personHasUsableSignature(person))
    .filter((person) => String(person.record_id || "").trim())
    .filter((person) => !personReadyForMop(person))
    .filter((person) => {
      const openId = String(person.open_id || "").trim();
      return Boolean(openId && currentUserOpenId.value && openId !== currentUserOpenId.value);
    });
}

function signatureUsageConfirmationCount(role: MopSignatureRole): number {
  return staffSignaturesRequiringConfirmation(role).length;
}

async function sendSignatureLinkForPerson(person: Dict, forceResign = false): Promise<boolean> {
  const recordId = String(person?.record_id || "").trim();
  if (!recordId) return false;
  signatureLinkSendingById.value = {
    ...signatureLinkSendingById.value,
    [recordId]: true,
  };
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await sendStaffSignatureLink(recordId, String(person.name || ""), scope.value);
    const resultPerson = data.person || person;
    signatureLinkSentAtById.value = {
      ...signatureLinkSentAtById.value,
      [recordId]: shortClockText(),
    };
    const errors = { ...signatureLinkErrorById.value };
    delete errors[recordId];
    signatureLinkErrorById.value = errors;
    signatureMessage.value = forceResign
      ? `已给 ${resultPerson.name || person.name || "该人员"} 发送重新签名链接；新签名前原签名仍有效。`
      : `签名链接已发送给 ${resultPerson.name || person.name || "该人员"}。`;
    signatureMessageType.value = "success";
    updateFormalSignaturePolling();
    return true;
  } catch (error) {
    const messageText = errorMessage(error, "发送签名链接失败");
    signatureLinkErrorById.value = {
      ...signatureLinkErrorById.value,
      [recordId]: messageText,
    };
    signatureMessage.value = messageText;
    signatureMessageType.value = "failed";
    return false;
  } finally {
    const next = { ...signatureLinkSendingById.value };
    delete next[recordId];
    signatureLinkSendingById.value = next;
  }
}

async function sendUnsignedSignatureLinksForRole(role: MopSignatureRole): Promise<void> {
  const people = unsignedStaffSignaturePeople(role);
  if (!people.length || signatureBulkLinkSending.value) return;
  signatureBulkLinkSending.value = true;
  let okCount = 0;
  try {
    for (const person of people) {
      if (await sendSignatureLinkForPerson(person, false)) okCount += 1;
    }
    signatureMessage.value = okCount === people.length
      ? `已发送 ${okCount} 个待签名链接。`
      : `已发送 ${okCount}/${people.length} 个待签名链接，请查看失败项。`;
    signatureMessageType.value = okCount ? "success" : "failed";
  } finally {
    signatureBulkLinkSending.value = false;
  }
}

async function sendSignatureUsageConfirmationsForRole(role: MopSignatureRole): Promise<void> {
  const people = staffSignaturesRequiringConfirmation(role);
  if (!people.length || signatureUsageConfirmSending.value) return;
  signatureUsageConfirmSending.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await sendSignatureUsageConfirmations({
      scope: scope.value,
      noticeKey: signatureUsageNoticeKey.value,
      noticeTitle: selectedNotice.value?.title || "",
      mopAttachmentName: String(selectedAttachment.value?.name || selectedMop.value?.title || ""),
      signatures: people.map((person) => ({
        source: "staff",
        role,
        record_id: String(person.record_id || ""),
      })),
    });
    const sent = Number(data.sent_count || data.sent || 0);
    const failed = Number(data.failed_count || 0);
    signatureMessage.value = failed
      ? `已发送 ${sent} 个签名使用确认，${failed} 个失败。`
      : `已发送 ${sent} 个签名使用确认。`;
    signatureMessageType.value = failed ? "failed" : "success";
    updateFormalSignaturePolling();
  } catch (error) {
    signatureMessage.value = errorMessage(error, "发送签名使用确认失败");
    signatureMessageType.value = "failed";
  } finally {
    signatureUsageConfirmSending.value = false;
  }
}

async function sendTemporarySignatureLinkForPerson(person: Dict): Promise<boolean> {
  const tempId = String(person?.temp_id || "").trim();
  if (!tempId) return false;
  temporarySignatureLinkSendingById.value = {
    ...temporarySignatureLinkSendingById.value,
    [tempId]: true,
  };
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await sendTemporarySignatureLink({
      temporaryId: tempId,
      scope: scope.value,
    });
    if (data.signature) {
      mergeTemporarySignatures([data.signature]);
      unhideSignaturePerson(data.signature);
      clearMopOutputState();
    }
    temporarySignatureLinkSentAtById.value = {
      ...temporarySignatureLinkSentAtById.value,
      [tempId]: shortClockText(),
    };
    const errors = { ...temporarySignatureLinkErrorById.value };
    delete errors[tempId];
    temporarySignatureLinkErrorById.value = errors;
    updateTemporarySignaturePolling();
    signatureMessage.value = `${temporarySignatureDisplayName(person)} 签名链接已发送。`;
    signatureMessageType.value = "success";
    return true;
  } catch (error) {
    const messageText = errorMessage(error, "发送临时人员签名链接失败");
    temporarySignatureLinkErrorById.value = {
      ...temporarySignatureLinkErrorById.value,
      [tempId]: messageText,
    };
    signatureMessage.value = messageText;
    signatureMessageType.value = "failed";
    return false;
  } finally {
    const next = { ...temporarySignatureLinkSendingById.value };
    delete next[tempId];
    temporarySignatureLinkSendingById.value = next;
  }
}

function mergeTemporarySignatures(items: Dict[]): void {
  const next = new Map<string, Dict>();
  for (const item of temporarySignatures.value) {
    const id = signaturePersonKey(item);
    if (id) next.set(id, item);
  }
  for (const item of items) {
    const source = String(item?.source || "") === "external" ? "external" : "temporary";
    const id = signaturePersonKey({ ...item, source });
    if (id) next.set(id, { ...(next.get(id) || {}), ...item, source });
  }
  temporarySignatures.value = Array.from(next.values());
}

async function loadTemporarySignatures(options: { silent?: boolean } = {}): Promise<void> {
  const noticeKey = String(selectedNotice.value?.notice_key || "").trim();
  if (!noticeKey) {
    temporarySignatures.value = [];
    return;
  }
  try {
    const data = await fetchTemporarySignatures(scope.value, noticeKey);
    const items = Array.isArray(data.items) ? data.items : [];
    mergeTemporarySignatures(items);
    updateTemporarySignaturePolling();
  } catch (error) {
    if (!options.silent) {
      signatureMessage.value = error instanceof Error ? error.message : "读取临时签名失败";
      signatureMessageType.value = "failed";
    }
  }
}

function temporarySignaturePollNeeded(): boolean {
  return temporarySignatures.value.some((item) => (
    String(item.source || "") !== "external"
    && !hiddenOtherSignatureKeys.value.includes(signaturePersonKey(item))
    && String(item.status || "") !== "failed"
    && String(item.status || "") !== "signed"
  ));
}

function scheduleExternalSignaturePeopleSearch(): void {
  if (externalSignatureSearchTimer) {
    clearTimeout(externalSignatureSearchTimer);
  }
  externalSignatureSearchTimer = setTimeout(() => {
    externalSignatureSearchTimer = null;
    void loadExternalSignaturePeople({ silent: true });
  }, 300);
}

async function loadExternalSignaturePeople(options: { silent?: boolean } = {}): Promise<void> {
  const requestSeq = ++externalSignatureSearchRequestSeq;
  externalSignatureLoading.value = true;
  if (!options.silent) {
    signatureMessage.value = "";
    signatureMessageType.value = "";
  }
  try {
    const data = await fetchExternalSignaturePeople({
      scope: scope.value,
      q: externalSignatureSearch.value,
      refresh: !options.silent,
      limit: 60,
    });
    if (requestSeq !== externalSignatureSearchRequestSeq) return;
    externalSignaturePeople.value = Array.isArray(data.people) ? data.people : [];
    externalSignaturePeopleTotal.value = Number(data.count || data.total || externalSignaturePeople.value.length || 0);
  } catch (error) {
    if (requestSeq !== externalSignatureSearchRequestSeq) return;
    signatureMessage.value = error instanceof Error ? error.message : "读取其他人员签名失败";
    signatureMessageType.value = "failed";
  } finally {
    if (requestSeq === externalSignatureSearchRequestSeq) {
      externalSignatureLoading.value = false;
    }
  }
}

function addExternalSignaturePerson(person: Dict): void {
  const recordId = String(person?.record_id || "").trim();
  if (!recordId) return;
  mergeTemporarySignatures([
    {
      ...person,
      source: "external",
      role: signatureRole.value,
      status: "signed",
      has_signature: true,
    },
  ]);
  unhideSignatureKey(`external:${recordId}`);
  clearMopOutputState();
  signatureMessage.value = `${person.name || person.display_name || "其他人员"} 已加入${signatureRole.value === "auditor" ? "维护审核人" : "维护实施人"}。`;
  signatureMessageType.value = "success";
}

function addOtherSignatureDraft(): void {
  if (addOtherSignatureDisabledReason.value) return;
  const draftId = `other-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const role = signatureRole.value;
  otherSignatureDrafts.value = [
    ...otherSignatureDrafts.value,
    {
      draft_id: draftId,
      role,
      display_name: nextOtherSignatureDisplayName(role),
      status: "draft",
      error: "",
    },
  ];
  clearMopOutputState();
  setTemporarySignatureDrawerOpen(true);
}

function removeOtherSignatureDraft(draftId: string): void {
  otherSignatureDrafts.value = otherSignatureDrafts.value.filter((item) => String(item.draft_id || "") !== draftId);
  clearMopOutputState();
}

function temporarySignatureRowDisabledReason(draft: Dict): string {
  if (temporarySignatureSendingByDraft.value[String(draft.draft_id || "")]) return "";
  if (!selectedNotice.value) return "请先选择左侧维保通告";
  if (!selectedNotice.value.notice_key) return "当前通告缺少记忆键，无法创建临时签名";
  const recipients = selectedFormalSignaturePeople("implementer").filter((person) => String(person.open_id || "").trim());
  if (!recipients.length) return "请先选择带飞书身份的维护实施人";
  if (String(draft.status || "") === "sent") return "签名链接已发送";
  return "";
}

function selectedPendingFormalSignaturePeople(): Dict[] {
  return [
    ...selectedFormalSignaturePeople("implementer"),
    ...selectedFormalSignaturePeople("auditor"),
  ].filter((person, index, items) => (
    !personReadyForMop(person)
    && String(person.record_id || "").trim()
    && items.findIndex((item) => String(item.record_id || "") === String(person.record_id || "")) === index
  ));
}

function formalSignaturePollNeeded(): boolean {
  return selectedPendingFormalSignaturePeople().length > 0;
}

async function refreshSelectedFormalSignatures(): Promise<void> {
  const targets = selectedPendingFormalSignaturePeople();
  if (!targets.length) {
    updateFormalSignaturePolling();
    return;
  }
  for (const person of targets) {
    const recordId = String(person.record_id || "").trim();
    if (!recordId) continue;
    try {
      const data = await fetchSignaturePeople({
        scope: scope.value,
        recordId,
        noticeKey: signatureUsageNoticeKey.value,
        refresh: true,
        limit: 1,
      });
      const found = Array.isArray(data.people) ? data.people[0] : null;
      if (found && String(found.record_id || "") === recordId) {
        updateRememberedSignaturePerson(recordId, found);
      }
    } catch {
      // Polling should not interrupt the user's current MOP editing flow.
    }
  }
  updateFormalSignaturePolling();
}

function updateFormalSignaturePolling(): void {
  formalSignaturePolling.update(formalSignaturePollNeeded());
}

function updateTemporarySignaturePolling(): void {
  temporarySignaturePolling.update(temporarySignaturePollNeeded());
}

async function sendTemporarySignatureLinkForDraft(draft: Dict): Promise<void> {
  const draftId = String(draft?.draft_id || "").trim();
  if (!draftId || temporarySignatureRowDisabledReason(draft)) return;
  const role = String(draft.role || signatureRole.value) === "auditor" ? "auditor" : "implementer";
  const displayName = String(draft.display_name || "").trim() || nextOtherSignatureDisplayName(role);
  const recipients = [
    ...new Set(
      selectedFormalSignaturePeople("implementer")
        .map((person) => String(person.open_id || "").trim())
        .filter(Boolean),
    ),
  ];
  temporarySignatureSendingByDraft.value = {
    ...temporarySignatureSendingByDraft.value,
    [draftId]: true,
  };
  otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
    String(item.draft_id || "") === draftId
      ? { ...item, display_name: displayName, status: "sending", error: "" }
      : item
  ));
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await sendTemporarySignatureLink({
      scope: scope.value,
      noticeKey: selectedNotice.value?.notice_key || "",
      noticeTitle: selectedNotice.value?.title || "",
      specialty: selectedNoticeSpecialty.value,
      role,
      displayName,
      recipientOpenIds: recipients,
    });
    if (data.signature) {
      mergeTemporarySignatures([data.signature]);
      unhideSignaturePerson(data.signature);
      const tempId = String(data.signature.temp_id || "").trim();
      if (tempId) {
        temporarySignatureLinkSentAtById.value = {
          ...temporarySignatureLinkSentAtById.value,
          [tempId]: shortClockText(),
        };
        const errors = { ...temporarySignatureLinkErrorById.value };
        delete errors[tempId];
        temporarySignatureLinkErrorById.value = errors;
      }
    }
    otherSignatureDrafts.value = otherSignatureDrafts.value.filter((item) => String(item.draft_id || "") !== draftId);
    updateTemporarySignaturePolling();
    signatureMessage.value = `${role === "auditor" ? "维护审核人" : "维护实施人"}${displayName}签名链接已发送。`;
    signatureMessageType.value = "success";
  } catch (error) {
    const messageText = errorMessage(error, "发送失败");
    otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
      String(item.draft_id || "") === draftId
        ? { ...item, status: "failed", error: messageText }
        : item
    ));
    signatureMessage.value = errorMessage(error, "发送其他人员签名链接失败");
    signatureMessageType.value = "failed";
  } finally {
    const next = { ...temporarySignatureSendingByDraft.value };
    delete next[draftId];
    temporarySignatureSendingByDraft.value = next;
  }
}

function applyNoticeBinding(notice: Dict): boolean {
  const binding = notice?.mop_binding;
  if (!binding || !binding.mop_record_id) {
    selectedMopRecordId.value = "";
    selectedAttachmentToken.value = "";
    activeSheetName.value = "";
    return false;
  }
  selectedMopRecordId.value = String(binding.mop_record_id || "");
  selectedAttachmentToken.value = String(binding.mop_attachment_token || "");
  activeSheetName.value = String(binding.selected_sheet || "");
  return true;
}

function selectDefaultMopCandidate(): void {
  const first = orderedMopCandidates.value[0];
  if (!first?.record_id) {
    selectedMopRecordId.value = "";
    selectedAttachmentToken.value = "";
    return;
  }
  selectMop(String(first.record_id || ""));
}

function selectNotice(noticeKey: string): void {
  closeMopTransientUi();
  releaseMopEditSession();
  selectedNoticeKey.value = noticeKey;
  preview.value = null;
  clearMopOutputState();
  previewMode.value = false;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  localMopUpload.reset();
  resetOtherSignatures();
  updateTemporarySignaturePolling();
  const notice = selectedNotice.value;
  const applied = notice ? applyNoticeBinding(notice) : false;
  if (!applied || !selectedMop.value) selectDefaultMopCandidate();
  void loadSignaturePeople({ silent: true });
  void loadTemporarySignatures({ silent: true });
}

function selectMop(recordId: string): void {
  closeMopTransientUi();
  releaseMopEditSession();
  selectedMopRecordId.value = recordId;
  const nextMop = mopCandidates.value.find((item) => String(item.record_id || "") === String(recordId || ""));
  if (nextMop && !(nextMop.local_upload || nextMop.source === "local_upload") && localMopUploadStatus.value === "success") {
    localMopUpload.reset();
  }
  preview.value = null;
  clearMopOutputState();
  previewMode.value = false;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  const first = selectedMopAttachments.value[0];
  selectedAttachmentToken.value = first ? mopAttachmentKey(first) : "";
}

function backToBinding(): void {
  closeMopTransientUi();
  releaseMopEditSession();
  previewMode.value = false;
}

function noticeMatchesRouteTarget(item: Dict): boolean {
  if (initialNoticeKey && String(item.notice_key || "") === initialNoticeKey) return true;
  if (!initialSourceRecordId) return false;
  return String(item.source_record_id || item.record_id || "") === initialSourceRecordId;
}

function preferredNoticeKeyFromRoute(): string {
  const routeMatched = notices.value.find((item) => noticeMatchesRouteTarget(item));
  if (routeMatched?.notice_key) return String(routeMatched.notice_key);
  return "";
}

async function loadPage(): Promise<void> {
  if (!props.loggedIn) return;
  loading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const data = await fetchEngineerMopBootstrap(scope.value);
    currentAuthUser.value = (data.auth?.user || data.user || {}) as Dict;
    notices.value = Array.isArray(data.notices) ? data.notices : [];
    mopCandidates.value = Array.isArray(data.mop_candidates) ? data.mop_candidates : [];
    warnings.value = Array.isArray(data.warnings) ? data.warnings.map((item: unknown) => String(item)) : [];
    const routeNoticeKey = routeNoticePreselectApplied.value ? "" : preferredNoticeKeyFromRoute();
    if (routeNoticeKey) {
      selectedNoticeKey.value = routeNoticeKey;
      routeNoticePreselectApplied.value = true;
    }
    if (!selectedNoticeKey.value || !notices.value.some((item) => item.notice_key === selectedNoticeKey.value)) {
      selectedNoticeKey.value = filteredNotices.value[0]?.notice_key || notices.value[0]?.notice_key || "";
    }
    const applied = selectedNotice.value ? applyNoticeBinding(selectedNotice.value) : false;
    if (!applied || !selectedMop.value) {
      selectDefaultMopCandidate();
    }
    void loadSignaturePeople({ silent: true });
    void loadTemporarySignatures({ silent: true });
  } catch (error) {
    message.value = error instanceof Error ? error.message : "加载工程师 MOP 数据失败";
    messageType.value = "failed";
  } finally {
    loading.value = false;
  }
}

async function saveBinding(options: { silent?: boolean } = {}): Promise<Dict | null> {
  if (!canBind.value || !selectedNotice.value || !selectedMop.value) return null;
  saving.value = true;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  if (!options.silent) {
    message.value = "";
    messageType.value = "";
  }
  try {
    const attachment = selectedAttachment.value || {};
    const payload = {
      scope: scope.value,
      notice_key: selectedNotice.value.notice_key,
      template_key: selectedNotice.value.mop_template_key || "",
      mop_template_key: selectedNotice.value.mop_template_key || "",
      notice_title: selectedNotice.value.title,
      notice_status: selectedNotice.value.status,
      building: selectedNotice.value.building,
      maintenance_total: selectedNotice.value.maintenance_total,
      maintenance_cycle: selectedNotice.value.maintenance_cycle,
      source_record_id: selectedNotice.value.source_record_id,
      target_record_id: selectedNotice.value.target_record_id,
      active_item_id: selectedNotice.value.active_item_id,
      mop_source: selectedMop.value.source === "local_upload" || selectedMop.value.local_upload ? "local_upload" : "bitable",
      upload_id: selectedMop.value.upload_id || attachment.upload_id || "",
      mop_app_token: selectedMop.value.app_token,
      mop_table_id: selectedMop.value.table_id,
      mop_record_id: selectedMop.value.record_id,
      mop_title: selectedMop.value.title,
      mop_attachment_token: attachment.file_token || attachment.url || "",
      mop_attachment_name: attachment.name || "",
      selected_sheet: activeSheetName.value,
    };
    const data = await bindEngineerMop(payload);
    const binding = data.binding || {};
    const notice = selectedNotice.value;
    if (notice) notice.mop_binding = binding;
    mopBindingStatus.value = "已自动绑定，下次同类通告会自动带出";
    if (!options.silent) {
      message.value = "已自动绑定。";
      messageType.value = "success";
    }
    return binding;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "自动绑定 MOP 失败";
    mopBindingError.value = errorMessage;
    if (!options.silent) {
      message.value = errorMessage;
      messageType.value = "failed";
    }
    return null;
  } finally {
    saving.value = false;
  }
}

function handleLocalMopUploadInvalid(messageText: string): void {
  localMopUpload.fail(messageText);
}

function upsertMopCandidate(candidate: Dict): void {
  const recordId = String(candidate?.record_id || "").trim();
  if (!recordId) return;
  const next = mopCandidates.value.filter((item) => String(item.record_id || "") !== recordId);
  mopCandidates.value = [candidate, ...next];
}

async function uploadLocalMopFile(file: File): Promise<void> {
  if (!selectedNotice.value) {
    handleLocalMopUploadInvalid("请先选择一条维保通告。");
    return;
  }
  if (!selectedNoticeSourceRecordId.value) {
    handleLocalMopUploadInvalid("当前维保通告缺少源记录，无法绑定本地 MOP。");
    return;
  }
  const fileError = localMopUpload.validateFile(file);
  if (fileError) {
    handleLocalMopUploadInvalid(fileError);
    return;
  }
  localMopUpload.begin();
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  try {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("scope", scope.value);
    formData.append("source_record_id", selectedNoticeSourceRecordId.value);
    formData.append("notice_key", String(selectedNotice.value.notice_key || ""));
    formData.append("notice_title", String(selectedNotice.value.title || ""));
    const data = await uploadLocalEngineerMop(formData);
    const candidate = data.local_mop_candidate || {};
    const recordId = String(candidate.record_id || "");
    if (!recordId) throw new Error("本地 MOP 上传成功但未返回可选记录。");
    upsertMopCandidate(candidate);
    selectMop(recordId);
    const attachment = Array.isArray(candidate.attachments) ? candidate.attachments[0] : null;
    selectedAttachmentToken.value = attachment ? mopAttachmentKey(attachment) : "";
    localMopUpload.succeed(String(data.file_name || file.name), Array.isArray(data.warnings) ? data.warnings : []);
    message.value = "本地 MOP 已上传并选中，可打开填写。";
    messageType.value = "success";
    if (Array.isArray(data.warnings)) {
      warnings.value = [...new Set([...warnings.value, ...data.warnings.map((item: unknown) => String(item))])];
    }
  } catch (error) {
    const text = localMopUpload.failFromError(error);
    message.value = text;
    messageType.value = "failed";
  } finally {
    localMopUpload.finish();
  }
}

async function startMopPreview(): Promise<void> {
  if (!canPreview.value || !selectedMop.value) return;
  const binding = await saveBinding({ silent: true });
  if (!binding) return;
  previewLoading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const data = await previewEngineerMop({
      scope: scope.value,
      mopRecordId: String(selectedMop.value.record_id || ""),
      fileToken: mopAttachmentKey(attachment),
      fileName: String(attachment.name || ""),
      uploadId: String(selectedMop.value.upload_id || attachment.upload_id || ""),
    });
    clearMopFillState({ clearSignatures: true });
    preview.value = data;
    const sheets = Array.isArray(data.sheets) ? data.sheets : [];
    const memorySheet = String(data.fill_memory?.sheet_name || data.fill_memory?.payload?.sheet_name || "");
    if (memorySheet && sheets.some((sheet: Dict) => sheet.name === memorySheet)) {
      activeSheetName.value = memorySheet;
    } else if (!sheets.some((sheet: Dict) => sheet.name === activeSheetName.value)) {
      activeSheetName.value = String(sheets[0]?.name || "");
    }
    const appliedMemoryCount = applyMopFillMemory(data.fill_memory);
    previewMode.value = true;
    claimMopEditSession();
    if (appliedMemoryCount) {
      signatureMessage.value = `已自动带出上次填写内容（${appliedMemoryCount} 项）。`;
      signatureMessageType.value = "success";
    }
    if (Array.isArray(data.warnings)) {
      warnings.value = [...new Set([...warnings.value, ...data.warnings.map((item: unknown) => String(item))])];
    }
    await nextTick();
    ensureSignatureCanvasObserver();
    resizeSignatureCanvas();
    if (!signaturePeople.value.length) {
      void loadSignaturePeople();
    }
    if (!externalSignaturePeople.value.length) {
      void loadExternalSignaturePeople({ silent: true });
    }
    void loadTemporarySignatures({ silent: true });
  } catch (error) {
    message.value = error instanceof Error ? error.message : "读取 MOP 表格失败";
    messageType.value = "failed";
  } finally {
    previewLoading.value = false;
  }
}

watch(scope, () => {
  closeMopTransientUi();
  releaseMopEditSession();
  const url = new URL(window.location.href);
  url.searchParams.set("scope", scope.value);
  window.history.replaceState({}, "", url);
  selectedNoticeKey.value = "";
  selectedMopRecordId.value = "";
  selectedAttachmentToken.value = "";
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  preview.value = null;
  previewMode.value = false;
  void loadPage();
});

watch(selectedMopAttachments, (items) => {
  if (!items.length) {
    selectedAttachmentToken.value = "";
    return;
  }
  if (!items.some((item) => mopAttachmentKey(item) === selectedAttachmentToken.value)) {
    selectedAttachmentToken.value = mopAttachmentKey(items[0]);
  }
});

watch(activeSheetName, () => {
  closeMopTransientUi();
});

onMounted(() => {
  ensureSignatureCanvasObserver();
  window.addEventListener("scroll", updateActiveMopCellOverlayPosition, true);
  window.addEventListener("resize", updateActiveMopCellOverlayPosition);
  if (props.loggedIn) void loadPage();
});

onBeforeUnmount(() => {
  if (signatureSearchTimer) {
    clearTimeout(signatureSearchTimer);
    signatureSearchTimer = null;
  }
  if (externalSignatureSearchTimer) {
    clearTimeout(externalSignatureSearchTimer);
    externalSignatureSearchTimer = null;
  }
  temporarySignaturePolling.stop();
  formalSignaturePolling.stop();
  window.removeEventListener("scroll", updateActiveMopCellOverlayPosition, true);
  window.removeEventListener("resize", updateActiveMopCellOverlayPosition);
  disconnectSignatureCanvasObserver();
  releaseMopEditSession();
});

watch(signatureRole, () => {
  signatureMessage.value = "";
  signatureMessageType.value = "";
  closeSignatureDrawers();
  clearSignatureCanvas();
  void nextTick(() => resizeSignatureCanvas());
});

watch(signatureSelectedRecords, () => {
  updateFormalSignaturePolling();
}, { deep: true });

watch(signatureUsageNoticeKey, () => {
  if (!props.loggedIn) return;
  void loadSignaturePeople({ silent: true });
  updateFormalSignaturePolling();
});

watch(signatureSearch, () => {
  scheduleSignaturePeopleSearch();
});

watch(externalSignatureSearch, () => {
  scheduleExternalSignaturePeopleSearch();
});

watch(() => props.loggedIn, (value) => {
  if (value) void loadPage();
});

watch(() => props.scopeOptions, (items) => {
  if (!items.length) return;
  const allowed = items.map((item) => normalizeMopScope(item.value));
  if (!allowed.includes(scope.value)) {
    scope.value = allowed[0] || "ALL";
  }
}, { immediate: true, deep: true });
</script>

<style scoped>
.engineer-mop {
  width: min(1800px, 100%);
  margin: 0 auto;
  padding: 28px 32px 42px;
  display: grid;
  gap: 20px;
}

.engineer-mop.preview-open {
  width: min(1920px, 100%);
  padding: 14px 20px 28px;
  gap: 12px;
}

.page-back-row {
  display: flex;
  align-items: center;
  justify-content: flex-start;
}

.page-back-btn {
  min-height: 36px;
  padding: 0 13px;
  border-radius: 999px;
}

.page-back-btn span {
  font-size: 19px;
  line-height: 1;
}

.notice-box,
.message,
.warning-list {
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.88);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.btn {
  min-height: 44px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 14px;
  color: #0f172a;
  font-weight: 750;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease, background 0.16s ease;
}

.btn:hover:not(:disabled) {
  border-color: #9cc7ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.08);
  transform: translateY(-1px);
}

.btn:focus-visible {
  outline: 3px solid rgba(75, 153, 255, 0.38);
  outline-offset: 2px;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
  box-shadow: none;
  transform: none;
}

.btn.blue {
  border-color: transparent;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn.ghost {
  color: #1d4ed8;
  background: #f8fbff;
}

.btn[aria-busy="true"] {
  position: relative;
  color: transparent;
}

.btn[aria-busy="true"]::after {
  content: "";
  width: 16px;
  height: 16px;
  position: absolute;
  inset: 50% auto auto 50%;
  margin: -8px 0 0 -8px;
  border-radius: 999px;
  border: 2px solid rgba(29, 78, 216, 0.25);
  border-top-color: #1d4ed8;
  animation: mopSpin 0.8s linear infinite;
}

@keyframes mopSpin {
  to {
    transform: rotate(360deg);
  }
}

.field {
  display: grid;
  gap: 7px;
  color: #475569;
  font-size: 13px;
  font-weight: 750;
}

.empty-box,
.notice-box,
.message,
.warning-list {
  padding: 18px;
  color: #475569;
}

.empty-box {
  border: 1px dashed #cfe0ff;
  border-radius: 16px;
  background: #f8fbff;
}

.empty-box.warning,
.message.failed {
  color: #b45309;
  background: #fffbeb;
  border-color: #fde68a;
}

.message.success {
  color: #047857;
  background: #ecfdf5;
  border-color: #a7f3d0;
}

.warning-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.warning-list span {
  padding: 7px 10px;
  border-radius: 999px;
  color: #92400e;
  background: #fffbeb;
}

.mop-sign-panel {
  display: grid;
  position: relative;
  z-index: 20;
  gap: 12px;
  padding: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.mop-preview-page .mop-sign-panel {
  grid-column: 1;
  grid-row: auto;
  position: relative;
  z-index: 1;
  gap: 8px;
  padding: 10px;
  border-radius: 16px;
  border-color: rgba(191, 219, 254, 0.92);
}

.signature-manager-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-modal-backdrop, 800);
  background: rgba(7, 20, 48, 0.22);
  backdrop-filter: blur(3px);
}

.mop-preview-page .mop-sign-panel.manager-open {
  position: fixed;
  z-index: var(--cf-z-modal, 840);
  top: 18px;
  right: 18px;
  bottom: 18px;
  left: auto;
  width: min(980px, calc(100vw - 36px));
  max-height: none;
  display: grid;
  grid-template-rows: auto auto auto minmax(0, 1fr);
  align-content: stretch;
  gap: 8px;
  padding: 0 7px 7px;
  overflow-x: hidden;
  overflow-y: hidden;
  transform: none;
  border-radius: 22px;
  border-color: rgba(191, 219, 254, 0.96);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.96)),
    #ffffff;
  box-shadow: 0 26px 76px rgba(12, 46, 108, 0.28);
  overscroll-behavior: contain;
  scrollbar-gutter: stable;
}

.mop-preview-page .mop-sign-panel.manager-open.signature-drawer-open {
  overflow: hidden;
}

.mop-preview-page .mop-sign-panel.manager-open.signature-drawer-open .sign-workspace {
  overflow: auto;
}

.mop-preview-page .mop-sign-panel.manager-open .sign-panel-head {
  position: relative;
  z-index: 4;
  margin: 0 -7px;
  min-height: 40px;
  padding: 8px 12px;
  border-bottom: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 22px 22px 0 0;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.99), rgba(255, 255, 255, 0.96)),
    #ffffff;
  backdrop-filter: blur(10px);
}

.mop-preview-page .mop-sign-panel:not(.manager-open) .sign-workspace,
.mop-preview-page .mop-sign-panel:not(.manager-open) .other-signature-panel,
.mop-preview-page .mop-sign-panel:not(.manager-open) .mop-completion-panel,
.mop-preview-page .mop-sign-panel:not(.manager-open) .mop-upload-readiness {
  display: none;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) {
  grid-template-columns: minmax(78px, auto) minmax(0, 1fr);
  align-items: center;
  padding: 5px 7px 5px 9px;
  border-radius: 14px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.97), rgba(248, 251, 255, 0.92)),
    #ffffff;
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.06);
}

.mop-preview-page .mop-sign-panel:not(.manager-open)::before {
  content: "";
  position: absolute;
  left: 0;
  top: 8px;
  bottom: 8px;
  width: 3px;
  border-radius: 999px;
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.signature-role-summary) {
  gap: 5px;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.signature-role-summary button) {
  min-height: 30px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  border-radius: 12px;
  padding: 4px 7px;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.role-title-row) {
  flex: 1 1 auto;
  min-width: 0;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.role-title-row b) {
  padding: 2px 6px;
  font-size: 10px;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.role-chip-row) {
  display: none;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) :deep(.role-state-line) {
  flex: 0 0 auto;
  padding: 3px 6px;
  font-size: 10px;
}

.mop-preview-page .sign-panel-head {
  min-height: 32px;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) .sign-panel-head {
  min-height: 0;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) .sign-panel-head p {
  display: none;
}

.mop-preview-page .mop-sign-panel.manager-open .sign-panel-head p {
  display: block;
  margin: 2px 0 0;
  color: #64748b;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.35;
}

.mop-preview-page .sign-workspace {
  grid-template-columns: 1fr;
  gap: 10px;
  min-height: 0;
}

.mop-preview-page .mop-sign-panel.manager-open .sign-workspace {
  grid-template-columns: minmax(244px, 300px) minmax(0, 1fr);
  align-items: start;
  gap: 7px;
  min-height: 0;
  overflow: auto;
  padding: 0 1px 3px 0;
  scrollbar-width: thin;
  scrollbar-gutter: stable;
}

.mop-preview-page .mop-sign-panel.manager-open .sign-workspace::-webkit-scrollbar {
  width: 8px;
}

.mop-preview-page .mop-sign-panel.manager-open .sign-workspace::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(148, 163, 184, 0.42);
}

.mop-preview-page .mop-sign-panel.manager-open :deep(.company-signature-picker.compact .sign-person-list) {
  max-height: min(34vh, 256px);
}

.mop-preview-page .mop-sign-panel.manager-open :deep(.company-signature-picker.compact .sign-person) {
  min-height: 40px;
  padding: 6px 7px;
}

.mop-preview-page .mop-sign-panel.manager-open :deep(.external-signature-reuse-body) {
  max-height: min(24vh, 188px);
}

.mop-preview-page .mop-sign-panel.manager-open :deep(.external-signature-results) {
  max-height: min(16vh, 128px);
}

.mop-preview-page .mop-sign-canvas {
  min-height: 96px;
}

.mop-preview-page .mop-sign-canvas canvas {
  height: 96px;
}

.mop-preview-page .mop-completion-panel {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.sign-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.sign-panel-head strong {
  display: block;
  color: #0f172a;
  font-size: 16px;
  font-weight: 950;
}

.mop-preview-page .mop-sign-panel:not(.manager-open) .sign-panel-head strong {
  font-size: 12px;
  font-weight: 950;
}

.sign-panel-head p {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.55;
}

.sign-close-inline {
  flex: 0 0 auto;
  min-height: 28px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  background: #ffffff;
  color: #3156c9;
  padding: 5px 12px;
  font-size: 11px;
  font-weight: 900;
  cursor: pointer;
}

.sign-close-inline:hover {
  border-color: #8dbbfb;
  background: #eff6ff;
}

.sign-workspace {
  display: grid;
  grid-template-columns: minmax(260px, 360px) minmax(0, 1fr);
  gap: 12px;
}

.signature-zone {
  display: grid;
  align-content: start;
  gap: 7px;
  min-width: 0;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.98));
  padding: 8px;
}

.mop-preview-page .signature-zone {
  min-height: 0;
  border-color: rgba(191, 219, 254, 0.98);
  border-radius: 18px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.96)),
    #ffffff;
  box-shadow: 0 14px 30px rgba(30, 99, 255, 0.07);
}

.signature-zone-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  min-width: 0;
  border-bottom: 1px solid rgba(216, 229, 247, 0.78);
  padding-bottom: 6px;
}

.signature-zone-head strong {
  color: #0f172a;
  font-size: 15px;
  font-weight: 950;
}

.signature-zone-head small {
  min-width: 0;
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  line-height: 1.4;
  text-align: right;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-sign-canvas {
  position: relative;
  min-height: 180px;
  border: 1px dashed #93c5fd;
  border-radius: 16px;
  background:
    linear-gradient(90deg, rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    linear-gradient(rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    #ffffff;
  background-size: 22px 22px;
  overflow: hidden;
  user-select: none;
  -webkit-user-select: none;
  touch-action: none;
}

.mop-sign-canvas.disabled {
  opacity: 0.65;
}

.mop-sign-canvas canvas {
  position: relative;
  z-index: 2;
  display: block;
  width: 100%;
  height: 180px;
  cursor: crosshair;
  touch-action: none;
  user-select: none;
  -webkit-user-select: none;
}

.sign-clear-inline {
  position: absolute;
  z-index: 5;
  top: 10px;
  right: 10px;
  border: 1px solid rgba(148, 163, 184, 0.38);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.92);
  color: #3156c9;
  padding: 5px 10px;
  font-size: 12px;
  font-weight: 850;
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
  cursor: pointer;
}

.sign-clear-inline:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.mop-sign-preview-img {
  position: absolute;
  z-index: 1;
  inset: 12%;
  width: 76%;
  height: 76%;
  object-fit: contain;
  pointer-events: none;
}

.sign-placeholder {
  position: absolute;
  z-index: 3;
  inset: 0;
  display: grid;
  place-items: center;
  pointer-events: none;
  color: #94a3b8;
  font-weight: 850;
}

.signature-pad-canvas,
.mop-preview-page .signature-pad-canvas {
  min-height: min(56vh, 520px);
  border-radius: 20px;
}

.signature-pad-canvas canvas,
.mop-preview-page .signature-pad-canvas canvas {
  height: min(56vh, 520px);
  min-height: 360px;
}

.sign-status {
  min-width: 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.6;
}

.sign-status.success {
  color: #047857;
}

.sign-status.failed {
  color: #b45309;
}

.empty-box.compact {
  padding: 12px;
  font-size: 13px;
}

.mop-completion-panel {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.mop-completion-panel span {
  display: grid;
  gap: 3px;
  min-width: 0;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: #ffffff;
  padding: 9px 10px;
}

.mop-completion-panel span.done {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.mop-completion-panel span.pending {
  border-color: #fde68a;
  background: #fffbeb;
}

.mop-completion-panel strong {
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
}

.mop-completion-panel em {
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-upload-readiness {
  display: grid;
  gap: 7px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: linear-gradient(135deg, #f8fbff, #ffffff);
  padding: 10px;
}

.mop-upload-readiness > strong {
  color: #0f172a;
  font-size: 13px;
  font-weight: 900;
}

.mop-upload-readiness span {
  display: grid;
  grid-template-columns: minmax(86px, auto) minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  border: 1px solid #fde68a;
  border-radius: 12px;
  background: #fffbeb;
  padding: 7px 8px;
  color: #92400e;
  font-size: 12px;
  font-weight: 850;
}

.mop-upload-readiness span.done {
  border-color: #bbf7d0;
  background: #ecfdf5;
  color: #047857;
}

.mop-upload-readiness em {
  min-width: 0;
  overflow: hidden;
  color: inherit;
  font-style: normal;
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sheet-scroll {
  min-height: 0;
  max-height: none;
  overflow: visible;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: #ffffff;
  box-shadow:
    0 14px 36px rgba(15, 73, 153, 0.08),
    0 0 0 1px rgba(255, 255, 255, 0.78) inset;
}

.mop-preview-page {
  --mop-upload-footer-safe-space: clamp(124px, 15vh, 176px);
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  grid-template-rows: auto auto auto auto auto;
  align-items: start;
  column-gap: 14px;
  gap: 10px;
  width: min(1800px, 100%);
  margin: 0 auto;
  min-height: calc(100vh - 46px);
  padding-bottom: var(--mop-upload-footer-safe-space);
  scroll-padding-bottom: var(--mop-upload-footer-safe-space);
  overflow: visible;
}

.preview-scroll {
  grid-column: 1;
  position: relative;
  min-height: 0;
  height: auto;
  max-height: none;
  overflow: visible;
  border-radius: 18px;
  padding-bottom: var(--mop-upload-footer-safe-space);
  scroll-padding-bottom: var(--mop-upload-footer-safe-space);
}

@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    animation: none !important;
    transition: none !important;
    scroll-behavior: auto !important;
  }
}

@media (max-width: 1180px) {
  .sign-panel-head {
    align-items: stretch;
    flex-direction: column;
  }

  .mop-completion-panel {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .sign-workspace {
    grid-template-columns: 1fr;
  }

  .mop-preview-page .mop-sign-panel.manager-open .sign-workspace {
    grid-template-columns: 1fr;
  }

  .sign-status {
    margin-right: 0;
  }

  .engineer-mop.preview-open {
    padding: 14px;
  }

  .mop-preview-page {
    grid-template-columns: 1fr;
    --mop-upload-footer-safe-space: clamp(150px, 22vh, 240px);
  }

  .mop-preview-page .mop-sign-panel,
  .preview-scroll {
    grid-column: 1;
  }

  .mop-preview-page .mop-sign-panel {
    position: static;
    max-height: none;
    grid-row: auto;
  }

  .mop-preview-page .mop-sign-panel.manager-open {
    inset: 12px;
    width: auto;
  }

  .preview-scroll {
    max-height: none;
  }
}

@media (max-width: 720px) {
  .mop-preview-page .mop-sign-panel.manager-open {
    inset: 8px;
    border-radius: 18px;
  }
}
</style>
