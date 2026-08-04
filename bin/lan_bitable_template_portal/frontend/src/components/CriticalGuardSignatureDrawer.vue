<template>
  <Teleport to="body">
    <div v-if="open" class="guard-signature-backdrop" @click.self="requestClose">
      <aside class="guard-signature-drawer" role="dialog" aria-modal="true" aria-label="检查人签名管理">
        <header class="drawer-header">
          <div>
            <span>检查人签名</span>
            <strong>{{ taskTitle }} · 当前楼栋全部检查表</strong>
          </div>
          <button type="button" class="icon-close" aria-label="关闭签名管理" @click="requestClose">
            <X :size="20" />
          </button>
        </header>

        <div class="signature-summary" :class="{ ready: totalSignerCount > 0 && totalPendingCount === 0 }">
          <span><UsersRound :size="17" /> 已选 {{ totalSignerCount }} 人</span>
          <strong>{{ totalPendingCount ? `待处理 ${totalPendingCount} 人` : totalSignerCount ? "全部可用" : "请选择检查人" }}</strong>
        </div>
        <p class="shared-signature-note">本任务当前楼栋的所有检查表共用本组签名，只需确认一次。</p>

        <nav class="source-tabs" aria-label="签名人员类型">
          <button type="button" :class="{ active: activeTab === 'company' }" @click.stop="switchSourceTab('company')">
            公司人员 <span>{{ selectedCompany.length }}</span>
          </button>
          <button type="button" :class="{ active: activeTab === 'other' }" @click.stop="switchSourceTab('other')">
            临时/外部人员 <span>{{ selectedOther.length + drafts.length }}</span>
          </button>
        </nav>

        <div class="drawer-body">
          <section v-show="activeTab === 'company'" class="company-layout">
            <MopCompanySignaturePicker
              v-model:search="companySearch"
              :loading="companyLoading"
              :status-text="companyStatusText"
              :people="companyPeople"
              :selected-ids="selectedCompanyIds"
              :temporary-mapped-ids="temporaryMappedCompanyIds"
              :active-record-id="activePersonKey"
              @refresh="loadCompanyPeople(true)"
              @select="toggleCompanyPerson"
            />
            <MopCompanySelectedSignatures
              role="inspector"
              ready-target-label="检查表"
              :people="selectedCompany"
              :active-record-id="activePersonKey"
              :unsigned-count="companyPendingCount"
              :unsigned-signature-count="companyUnsignedCount"
              :link-sending-by-id="companyLinkSending"
              :link-sent-at-by-id="companyLinkSentAt"
              :link-error-by-id="companyLinkErrors"
              :has-usable-signature="personReady"
              :person-key="personKey"
              :display-name="personName"
              :link-title="companyLinkTitle"
              :web-sign-disabled-reason="companyWebSignDisabledReason"
              :bulk-link-sending="bulkLinkSending"
              :confirm-sending="confirmationSending"
              :confirmable-count="companyConfirmableCount"
              :show-signature-preview="false"
              @activate="activatePerson"
              @image-error="markImageUnavailable"
              @web-sign="openWebSignature"
              @send-link="sendCompanyLink"
              @send-unsigned-links="sendAllUnsignedCompanyLinks"
              @send-confirmations="sendAllUsageConfirmations"
              @remove="removeCompanyPerson"
            />
          </section>

          <MopOtherSignatureManager
            v-show="activeTab === 'other'"
            role="inspector"
            :active="activeTab === 'other'"
            :add-disabled-reason="addTemporaryDisabledReason"
            :display-rows="otherDisplayRows"
            :unsigned-count="otherPendingCount"
            :temporary-link-sending-by-id="temporaryLinkSending"
            :temporary-link-sent-at-by-id="temporaryLinkSentAt"
            :temporary-link-error-by-id="temporaryLinkErrors"
            :draft-sending-by-id="draftSending"
            :external-search="externalSearch"
            :external-loading="externalLoading"
            :external-status-text="externalStatusText"
            :external-people="externalPeople"
            :person-status-text="otherPersonStatusText"
            :person-web-sign-disabled-reason="otherWebSignDisabledReason"
            :draft-status-text="draftStatusText"
            :draft-disabled-reason="draftDisabledReason"
            :show-signature-preview="false"
            @add-other="addTemporaryDraft"
            @image-error="markImageUnavailable"
            @web-sign-person="openWebSignature"
            @send-temp-person="sendExistingTemporaryLink"
            @remove-person="removeOtherPerson"
            @update-draft-name="updateDraftName"
            @ensure-draft-name="ensureDraftName"
            @web-sign-draft="openDraftWebSignature"
            @send-draft-link="sendDraftTemporaryLink"
            @remove-draft="removeDraft"
            @update:external-search="externalSearch = $event"
            @refresh-external="loadExternalPeople(true)"
            @add-external="addExternalPerson"
          />
        </div>

        <footer class="drawer-footer">
          <span v-if="statusMessage" :class="statusTone">{{ statusMessage }}</span>
          <span v-else>{{ totalSignerCount ? `可用 ${readyCount}/${totalSignerCount}` : "尚未选择签名人员" }}</span>
          <button type="button" class="done-button" @click="requestClose">完成</button>
        </footer>
      </aside>

      <MopSignaturePadModal
        :open="Boolean(activePadPerson)"
        :title="`${personName(activePadPerson || {})} · 网页手写签名`"
        role-label="检查人"
        :saving="padSaving"
        :message="padMessage"
        :message-type="padMessageType"
        :save-disabled-reason="padSaveDisabledReason"
        @close="closeSignaturePad"
        @clear="signatureCanvas.clear"
        @save="saveWebSignature"
      >
        <div class="signature-canvas-shell">
          <canvas
            ref="signatureCanvasRef"
            aria-label="检查人手写签名区域"
            @pointerdown="signatureCanvas.startDraw($event, !padSaving)"
            @pointermove="signatureCanvas.moveDraw"
            @pointerup="signatureCanvas.endDraw"
            @pointercancel="signatureCanvas.endDraw"
            @pointerleave="signatureCanvas.endDraw"
          ></canvas>
          <span v-if="!signatureHasInk">请在此处手写签名</span>
        </div>
      </MopSignaturePadModal>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import { UsersRound, X } from "lucide-vue-next";
import type { Dict } from "../api/client";
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
import { useMopSignatureCanvas } from "../useMopSignatureCanvas";
import MopCompanySelectedSignatures from "./MopCompanySelectedSignatures.vue";
import MopCompanySignaturePicker from "./MopCompanySignaturePicker.vue";
import MopOtherSignatureManager from "./MopOtherSignatureManager.vue";
import MopSignaturePadModal from "./MopSignaturePadModal.vue";

const props = defineProps<{
  open: boolean;
  scope: string;
  contextKey: string;
  taskTitle: string;
  currentUserOpenId: string;
  initialSigners: Dict[];
}>();

const emit = defineEmits<{
  close: [];
  change: [people: Dict[]];
  refresh: [people: Dict[]];
  status: [text: string, tone: string];
}>();

const activeTab = ref<"company" | "other">("company");
const companySearch = ref("");
const companyPeople = ref<Dict[]>([]);
const companyLoading = ref(false);
const companyTotal = ref(0);
const selectedCompany = ref<Dict[]>([]);
const selectedOther = ref<Dict[]>([]);
const temporaryPeople = ref<Dict[]>([]);
const drafts = ref<Dict[]>([]);
const externalSearch = ref("");
const externalPeople = ref<Dict[]>([]);
const externalLoading = ref(false);
const externalTotal = ref(0);
const companyLinkSending = ref<Record<string, boolean>>({});
const companyLinkSentAt = ref<Record<string, string>>({});
const companyLinkErrors = ref<Record<string, string>>({});
const temporaryLinkSending = ref<Record<string, boolean>>({});
const temporaryLinkSentAt = ref<Record<string, string>>({});
const temporaryLinkErrors = ref<Record<string, string>>({});
const draftSending = ref<Record<string, boolean>>({});
const bulkLinkSending = ref(false);
const confirmationSending = ref(false);
const activePersonKey = ref("");
const activePadPerson = ref<Dict | null>(null);
const padSaving = ref(false);
const padMessage = ref("");
const padMessageType = ref("info");
const statusMessage = ref("");
const statusTone = ref("info");
const signatureCanvas = useMopSignatureCanvas();
const signatureCanvasRef = signatureCanvas.canvasRef;
const signatureHasInk = signatureCanvas.hasInk;
let companySearchTimer: ReturnType<typeof setTimeout> | null = null;
let externalSearchTimer: ReturnType<typeof setTimeout> | null = null;
let pollingTimer: ReturnType<typeof setInterval> | null = null;
let loadGeneration = 0;

const currentOpenId = computed(() => String(props.currentUserOpenId || "").trim());

type CompanyLinkOutcome = "sent" | "converted" | "converted_failed" | "failed";
const temporaryMappedCompanyIds = computed(() => companyPeople.value
  .filter((item) => findMatchingPerson(item, selectedOther.value) || findMatchingDraft(item))
  .map((item) => String(item.record_id || "").trim())
  .filter(Boolean));
const selectedCompanyIds = computed(() => {
  const selectedIds = new Set(
    selectedCompany.value
      .map((item) => String(item.record_id || "").trim())
      .filter(Boolean),
  );
  temporaryMappedCompanyIds.value.forEach((recordId) => selectedIds.add(recordId));
  return [...selectedIds];
});
const selectedPeople = computed(() => [...selectedCompany.value, ...selectedOther.value]);
const selectedCount = computed(() => selectedPeople.value.length);
const readyCount = computed(() => selectedPeople.value.filter(personReady).length);
const pendingCount = computed(() => Math.max(0, selectedCount.value - readyCount.value));
const totalSignerCount = computed(() => selectedCount.value + drafts.value.length);
const totalPendingCount = computed(() => pendingCount.value + drafts.value.length);
const companyUnsignedCount = computed(() => selectedCompany.value.filter((item) => !personHasStoredSignature(item)).length);
const companyPendingCount = computed(() => selectedCompany.value.filter((item) => !personReady(item)).length);
const companyConfirmableCount = computed(() => selectedCompany.value.filter((item) => (
  personHasStoredSignature(item)
  && !personReady(item)
  && String(item.open_id || "").trim()
  && String(item.open_id || "").trim() !== currentOpenId.value
)).length);
const otherPendingCount = computed(() => selectedOther.value.filter((item) => !personReady(item)).length + drafts.value.length);
const companyStatusText = computed(() => {
  if (companyLoading.value) return "搜索中";
  if (!companyPeople.value.length) return companySearch.value.trim() ? "暂未找到人员" : "暂无人员";
  return companyPeople.value.length === companyTotal.value
    ? `已找到 ${companyTotal.value} 人`
    : `已显示 ${companyPeople.value.length}/${companyTotal.value} 人`;
});
const externalStatusText = computed(() => {
  if (externalLoading.value) return "搜索中";
  if (!externalPeople.value.length) return externalSearch.value.trim() ? "暂未找到人员" : "暂无已保存签名";
  return externalPeople.value.length === externalTotal.value
    ? `已找到 ${externalTotal.value} 人`
    : `已显示 ${externalPeople.value.length}/${externalTotal.value} 人`;
});
const addTemporaryDisabledReason = computed(() => props.contextKey ? "" : "请先打开检查表。" );
const padSaveDisabledReason = computed(() => {
  if (padSaving.value) return "签名保存中";
  if (!activePadPerson.value) return "未选择签名人员";
  if (!signatureHasInk.value) return "请先手写签名";
  return "";
});
const otherDisplayRows = computed(() => [
  ...selectedOther.value.map((person) => ({
    kind: "person",
    row_key: personKey(person),
    person,
    draft: {},
    signed: personReady(person),
    display_name: personName(person),
  })),
  ...drafts.value.map((draft) => ({
    kind: "draft",
    row_key: `draft:${String(draft.draft_id || "")}`,
    person: {},
    draft,
    signed: false,
    display_name: String(draft.display_name || ""),
  })),
]);

function protectedSigner(item: Dict): Dict {
  const result = { ...item };
  delete result.signature_preview_url;
  delete result.signature_file_token;
  return result;
}

function clonePeople(value: Dict[] | undefined): Dict[] {
  return Array.isArray(value) ? value.map(protectedSigner) : [];
}

function personKey(person: Dict | null | undefined): string {
  const source = String(person?.source || "staff");
  if (source === "temporary" || person?.temp_id) return `temporary:${String(person?.temp_id || "")}`;
  if (source === "external") return `external:${String(person?.record_id || "")}`;
  return String(person?.record_id || "");
}

function personName(person: Dict): string {
  return String(person?.name || person?.display_name || "未命名人员").trim() || "未命名人员";
}

function personOriginMatches(companyPerson: Dict, otherPerson: Dict): boolean {
  const companyRecordId = String(companyPerson.record_id || "").trim();
  const companyOpenId = String(companyPerson.open_id || "").trim();
  const originRecordId = String(otherPerson.origin_staff_record_id || "").trim();
  const originOpenId = String(otherPerson.origin_staff_open_id || "").trim();
  return Boolean(
    (companyRecordId && originRecordId && companyRecordId === originRecordId)
    || (companyOpenId && originOpenId && companyOpenId === originOpenId),
  );
}

function findMatchingPerson(companyPerson: Dict, candidates: Dict[]): Dict | undefined {
  return candidates.find((item) => personOriginMatches(companyPerson, item));
}

function findMatchingDraft(companyPerson: Dict): Dict | undefined {
  return drafts.value.find((item) => personOriginMatches(companyPerson, item));
}

function withCompanyOrigin(person: Dict, companyPerson: Dict): Dict {
  return {
    ...person,
    origin_staff_record_id: String(companyPerson.record_id || person.origin_staff_record_id || "").trim(),
    origin_staff_open_id: String(companyPerson.open_id || person.origin_staff_open_id || "").trim(),
  };
}

function reconcileDuplicateSelections(): boolean {
  let changed = false;
  const retainedCompany: Dict[] = [];
  for (const companyPerson of selectedCompany.value) {
    const matchedOther = findMatchingPerson(companyPerson, selectedOther.value)
      || findMatchingPerson(companyPerson, temporaryPeople.value)
      || findMatchingPerson(companyPerson, externalPeople.value);
    const matchedDraft = findMatchingDraft(companyPerson);
    if (!matchedOther && !matchedDraft) {
      retainedCompany.push(companyPerson);
      continue;
    }
    if (matchedOther) replaceOrAddOther(withCompanyOrigin(matchedOther, companyPerson));
    if (matchedDraft) Object.assign(matchedDraft, withCompanyOrigin(matchedDraft, companyPerson));
    changed = true;
  }
  if (changed) selectedCompany.value = retainedCompany;
  return changed;
}

function personHasStoredSignature(person: Dict | null | undefined): boolean {
  return Boolean(person?.has_signature);
}

function personReady(person: Dict | null | undefined): boolean {
  if (!personHasStoredSignature(person)) return false;
  const source = String(person?.source || "staff");
  if (source === "temporary" || source === "external" || person?.temp_id) return true;
  return Boolean(person?.usage_confirmed || person?.is_current_user || String(person?.open_id || "") === currentOpenId.value);
}

function emitSelectionChanged(): void {
  emit("change", clonePeople(selectedPeople.value));
}

function emitStatusRefresh(): void {
  emit("refresh", clonePeople(selectedPeople.value));
}

function setStatus(text: string, tone = "info"): void {
  statusMessage.value = text;
  statusTone.value = tone;
  emit("status", text, tone);
}

function initializeSelection(): void {
  const initial = clonePeople(props.initialSigners);
  selectedCompany.value = initial.filter((item) => String(item.source || "staff") === "staff");
  selectedOther.value = initial.filter((item) => ["temporary", "external"].includes(String(item.source || "")) || item.temp_id);
  drafts.value = [];
  activePersonKey.value = "";
}

async function loadCompanyPeople(refresh = false, silent = false): Promise<void> {
  const generation = loadGeneration;
  companyLoading.value = true;
  try {
    const data = await fetchSignaturePeople({
      scope: props.scope,
      q: companySearch.value,
      noticeKey: props.contextKey,
      refresh,
      limit: 100,
    });
    if (generation !== loadGeneration || !props.open) return;
    const items = Array.isArray(data.people) ? data.people.map(protectedSigner) : [];
    companyPeople.value = items;
    companyTotal.value = Number(data.count || items.length);
    const byId = new Map(items.map((item: Dict) => [String(item.record_id || ""), item]));
    selectedCompany.value = selectedCompany.value.map((item) => ({
      ...item,
      ...(byId.get(String(item.record_id || "")) || {}),
      source: "staff",
      role: "inspector",
    }));
    if (reconcileDuplicateSelections()) emitStatusRefresh();
    if (silent) emitStatusRefresh();
  } catch (error: any) {
    if (!silent) setStatus(error?.message || "公司人员读取失败。", "error");
  } finally {
    if (generation === loadGeneration) companyLoading.value = false;
  }
}

async function loadTemporaryStatuses(silent = false): Promise<void> {
  const generation = loadGeneration;
  try {
    const data = await fetchTemporarySignatures(props.scope, props.contextKey);
    if (generation !== loadGeneration || !props.open) return;
    const items = Array.isArray(data.items) ? data.items.map(protectedSigner) : [];
    temporaryPeople.value = items;
    const byId = new Map(items.map((item: Dict) => [String(item.temp_id || ""), item]));
    selectedOther.value = selectedOther.value.map((item) => {
      if (String(item.source || "") !== "temporary" && !item.temp_id) return item;
      return {
        ...item,
        ...(byId.get(String(item.temp_id || "")) || {}),
        source: "temporary",
        role: "inspector",
      };
    });
    if (reconcileDuplicateSelections()) emitStatusRefresh();
    if (silent) emitStatusRefresh();
  } catch (error: any) {
    if (!silent) setStatus(error?.message || "临时签名状态读取失败。", "error");
  }
}

async function loadExternalPeople(refresh = false): Promise<void> {
  externalLoading.value = true;
  const generation = loadGeneration;
  try {
    const data = await fetchExternalSignaturePeople({
      scope: props.scope,
      q: externalSearch.value,
      noticeKey: props.contextKey,
      refresh,
      limit: 100,
    });
    if (generation !== loadGeneration || !props.open) return;
    externalPeople.value = Array.isArray(data.people) ? data.people.map(protectedSigner) : [];
    externalTotal.value = Number(data.count || externalPeople.value.length);
    if (reconcileDuplicateSelections()) emitStatusRefresh();
  } catch (error: any) {
    setStatus(error?.message || "外部签名读取失败。", "error");
  } finally {
    if (generation === loadGeneration) externalLoading.value = false;
  }
}

async function refreshPendingStatuses(): Promise<void> {
  if (!props.open || document.hidden || !pendingCount.value) return;
  await Promise.all([
    loadCompanyPeople(false, true),
    loadTemporaryStatuses(true),
  ]);
}

function startPolling(): void {
  stopPolling();
  pollingTimer = setInterval(() => void refreshPendingStatuses(), 5000);
}

function stopPolling(): void {
  if (pollingTimer) clearInterval(pollingTimer);
  pollingTimer = null;
}

function toggleCompanyPerson(recordId: string): void {
  const index = selectedCompany.value.findIndex((item) => String(item.record_id || "") === recordId);
  if (index >= 0) {
    selectedCompany.value.splice(index, 1);
    emitSelectionChanged();
    return;
  }

  const person = companyPeople.value.find((item) => String(item.record_id || "") === recordId);
  if (!person) return;

  const selectedOtherMatch = findMatchingPerson(person, selectedOther.value);
  if (selectedOtherMatch) {
    replaceOrAddOther(withCompanyOrigin(selectedOtherMatch, person));
    activeTab.value = "other";
    emitSelectionChanged();
    setStatus(`${personName(person)} 已对应临时/外部人员，请在临时/外部人员中操作。`, "info");
    return;
  }

  const draftMatch = findMatchingDraft(person);
  if (draftMatch) {
    Object.assign(draftMatch, withCompanyOrigin(draftMatch, person));
    activeTab.value = "other";
    setStatus(`${personName(person)} 已对应待处理的临时人员，请在临时/外部人员中完成签名。`, "info");
    return;
  }

  const knownTemporaryMatch = findMatchingPerson(person, temporaryPeople.value);
  if (knownTemporaryMatch) {
    if (selectedCount.value >= 50) return setStatus("每个任务每栋楼最多选择 50 名检查人。", "error");
    replaceOrAddOther(withCompanyOrigin(knownTemporaryMatch, person));
    activeTab.value = "other";
    emitSelectionChanged();
    setStatus(`${personName(person)} 已恢复为此前创建的临时人员。`, "success");
    return;
  }

  const externalMatch = findMatchingPerson(person, externalPeople.value);
  if (externalMatch) {
    if (selectedCount.value >= 50) return setStatus("每个任务每栋楼最多选择 50 名检查人。", "error");
    replaceOrAddOther(withCompanyOrigin({ ...externalMatch, source: "external", role: "inspector", ready: true }, person));
    activeTab.value = "other";
    emitSelectionChanged();
    setStatus(`${personName(person)} 已使用此前关联的外部人员签名。`, "success");
    return;
  }

  if (selectedCount.value >= 50) return setStatus("每个任务每栋楼最多选择 50 名检查人。", "error");
  selectedCompany.value.push({ ...person, source: "staff", role: "inspector" });
  emitSelectionChanged();
}

function removeCompanyPerson(key: string): void {
  selectedCompany.value = selectedCompany.value.filter((item) => personKey(item) !== key);
  emitSelectionChanged();
}

function activatePerson(person: Dict): void {
  activePersonKey.value = personKey(person);
}

function companyWebSignDisabledReason(person: Dict | null | undefined): string {
  if (!person?.record_id) return "人员记录不完整";
  if (!currentOpenId.value) return "当前登录账号缺少 openid";
  if (String(person.open_id || "") !== currentOpenId.value) return "网页手写只能签当前登录用户本人";
  return "";
}

function companyLinkTitle(person: Dict): string {
  if (!String(person.open_id || "").trim()) return "该人员缺少 openid，无法发送链接";
  return personHasStoredSignature(person) ? "发送重新签名链接" : "发送签名链接";
}

function switchSourceTab(tab: "company" | "other"): void {
  activeTab.value = tab;
}

function isBotUnavailableFailure(value: any): boolean {
  const payload = value?.payload && typeof value.payload === "object" ? value.payload : {};
  const data = payload?.data && typeof payload.data === "object" ? payload.data : {};
  const results = Array.isArray(data.results)
    ? data.results
    : Array.isArray(value?.results)
      ? value.results
      : [];
  const failureKinds = [
    value?.failure_kind,
    data.failure_kind,
    ...results.map((item: Dict) => item?.failure_kind),
  ].map((item) => String(item || "").trim().toLowerCase());
  if (failureKinds.includes("bot_unavailable")) return true;
  const text = [
    value?.message,
    value?.error,
    payload?.error,
    data?.message,
    ...results.map((item: Dict) => item?.message),
  ].map((item) => String(item || "")).join(" ");
  return /bot\s+has\s+no\s+availability|no\s+availability\s+to\s+this\s+user|机器人对该用户不可用/i.test(text);
}

async function convertThirdPartyCompanyPerson(person: Dict): Promise<CompanyLinkOutcome> {
  const recordId = String(person.record_id || "").trim();
  const displayName = personName(person);
  let temporaryPerson: Dict;
  try {
    temporaryPerson = {
      ...(await createTemporarySignatureSession({
        scope: props.scope,
        noticeKey: props.contextKey,
        noticeTitle: props.taskTitle,
        specialty: "",
        role: "inspector",
        displayName,
        contextType: "critical_guard",
        originStaffRecordId: recordId,
        originStaffOpenId: String(person.open_id || "").trim(),
      })),
      source: "temporary",
      role: "inspector",
    };
  } catch (error: any) {
    companyLinkErrors.value = {
      ...companyLinkErrors.value,
      [recordId]: error?.message || "转为临时人员失败",
    };
    setStatus(`${displayName} 无法接收机器人消息，转为临时人员失败：${error?.message || "请稍后重试"}`, "error");
    return "failed";
  }

  selectedCompany.value = selectedCompany.value.filter((item) => personKey(item) !== personKey(person));
  replaceOrAddOther(temporaryPerson);
  if (activePersonKey.value === personKey(person)) activePersonKey.value = "";
  const companyErrors = { ...companyLinkErrors.value };
  delete companyErrors[recordId];
  companyLinkErrors.value = companyErrors;
  activeTab.value = "other";
  emitSelectionChanged();

  const sent = await sendExistingTemporaryLink(temporaryPerson, true);
  if (sent) {
    setStatus(
      `${displayName} 无法接收机器人消息，已转为临时人员；签名链接已自动发送给当前登录人，请在当前登录人的手机上让该人员完成签字。`,
      "warning",
    );
    return "converted";
  }
  setStatus(
    `${displayName} 已转为临时人员，但签名链接发送给当前登录人失败，请确认当前登录账号可以接收机器人消息后重试。`,
    "error",
  );
  return "converted_failed";
}

async function sendCompanyLink(person: Dict): Promise<CompanyLinkOutcome> {
  const recordId = String(person.record_id || "");
  if (!recordId || companyLinkSending.value[recordId]) return "failed";
  companyLinkSending.value = { ...companyLinkSending.value, [recordId]: true };
  try {
    await sendStaffSignatureLink(recordId, personName(person), props.scope, "critical_guard", props.taskTitle);
    companyLinkSentAt.value = { ...companyLinkSentAt.value, [recordId]: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" }) };
    const errors = { ...companyLinkErrors.value };
    delete errors[recordId];
    companyLinkErrors.value = errors;
    setStatus(`${personName(person)} 的签名链接已发送。`, "success");
    return "sent";
  } catch (error: any) {
    if (isBotUnavailableFailure(error)) {
      return await convertThirdPartyCompanyPerson(person);
    }
    companyLinkErrors.value = { ...companyLinkErrors.value, [recordId]: error?.message || "发送失败" };
    setStatus(error?.message || `${personName(person)} 的签名链接发送失败。`, "error");
    return "failed";
  } finally {
    const next = { ...companyLinkSending.value };
    delete next[recordId];
    companyLinkSending.value = next;
  }
}

async function sendAllUnsignedCompanyLinks(): Promise<void> {
  if (bulkLinkSending.value) return;
  const people = selectedCompany.value.filter((item) => !personHasStoredSignature(item));
  if (!people.length) return;
  bulkLinkSending.value = true;
  let success = 0;
  let converted = 0;
  let convertedFailed = 0;
  for (const person of people) {
    const outcome = await sendCompanyLink(person);
    if (outcome === "sent") success += 1;
    else if (outcome === "converted") converted += 1;
    else if (outcome === "converted_failed") convertedFailed += 1;
  }
  bulkLinkSending.value = false;
  if (converted || convertedFailed) {
    const parts = [`公司人员链接已发送 ${success} 人`];
    if (converted) parts.push(`${converted} 名第三方人员已转为临时人员并自动把链接发给当前登录人`);
    if (convertedFailed) parts.push(`${convertedFailed} 名第三方人员已转为临时人员但链接发送失败`);
    setStatus(`${parts.join("；")}。请在当前登录人的手机上完成临时人员签字。`, convertedFailed ? "error" : "warning");
    return;
  }
  setStatus(`签名链接已发送 ${success}/${people.length} 人。`, success === people.length ? "success" : "warning");
}

async function sendAllUsageConfirmations(): Promise<void> {
  if (confirmationSending.value) return;
  const targets = selectedCompany.value.filter((item) => (
    personHasStoredSignature(item)
    && !personReady(item)
    && String(item.open_id || "").trim()
    && String(item.open_id || "").trim() !== currentOpenId.value
  ));
  if (!targets.length) return;
  confirmationSending.value = true;
  try {
    const data = await sendSignatureUsageConfirmations({
      scope: props.scope,
      noticeKey: props.contextKey,
      noticeTitle: props.taskTitle,
      mopAttachmentName: "本任务全部检查表",
      contextType: "critical_guard",
      signatures: targets.map((item) => ({ source: "staff", role: "inspector", record_id: item.record_id })),
    });
    const results = Array.isArray(data.results) ? data.results : [];
    const unavailableResults = results.filter((item: Dict) => !item.ok && isBotUnavailableFailure(item));
    let converted = 0;
    let convertedFailed = 0;
    for (const result of unavailableResults) {
      const person = selectedCompany.value.find((item) => (
        String(item.record_id || "") === String(result.record_id || "")
        || String(item.open_id || "") === String(result.open_id || "")
      ));
      if (!person) continue;
      const outcome = await convertThirdPartyCompanyPerson(person);
      if (outcome === "converted") converted += 1;
      else if (outcome === "converted_failed") convertedFailed += 1;
    }
    const otherFailed = Math.max(0, Number(data.failed_count || 0) - converted - convertedFailed);
    if (converted || convertedFailed) {
      const parts = [`确认请求已发送 ${Number(data.sent_count || 0)} 人`];
      if (converted) parts.push(`${converted} 名第三方人员已转为临时人员，链接已自动发送给当前登录人`);
      if (convertedFailed) parts.push(`${convertedFailed} 名第三方人员临时链接发送失败`);
      if (otherFailed) parts.push(`${otherFailed} 人发送失败`);
      setStatus(`${parts.join("；")}。请在当前登录人的手机上完成临时人员签字。`, convertedFailed || otherFailed ? "error" : "warning");
    } else {
      setStatus(`确认请求已发送 ${Number(data.sent_count || 0)} 人。`, Number(data.failed_count || 0) ? "warning" : "success");
    }
    await loadCompanyPeople(false, true);
  } catch (error: any) {
    setStatus(error?.message || "确认请求发送失败。", "error");
  } finally {
    confirmationSending.value = false;
  }
}

function addTemporaryDraft(): void {
  if (selectedCount.value + drafts.value.length >= 50) return setStatus("每个任务每栋楼最多选择 50 名检查人。", "error");
  const nextNumber = selectedOther.value.filter((item) => String(item.source || "") === "temporary").length + drafts.value.length + 1;
  drafts.value.push({ draft_id: `draft_${Date.now()}_${Math.random().toString(16).slice(2)}`, display_name: `临时人员${nextNumber}`, status: "draft" });
}

function updateDraftName(draftId: string, value: string): void {
  const draft = drafts.value.find((item) => String(item.draft_id || "") === draftId);
  if (draft) draft.display_name = value;
}

function ensureDraftName(draft: Dict): void {
  if (String(draft.display_name || "").trim()) return;
  const index = drafts.value.findIndex((item) => item === draft);
  draft.display_name = `临时人员${Math.max(1, index + 1)}`;
}

function removeDraft(draftId: string): void {
  drafts.value = drafts.value.filter((item) => String(item.draft_id || "") !== draftId);
}

function draftStatusText(draft: Dict): string {
  return draftSending.value[String(draft.draft_id || "")] ? "发送中" : "待发送";
}

function draftDisabledReason(draft: Dict): string {
  if (draftSending.value[String(draft.draft_id || "")]) return "正在发送";
  if (!currentOpenId.value) return "当前登录账号缺少 openid";
  return "";
}

function otherPersonStatusText(person: Dict): string {
  if (String(person.source || "") === "external") return "已有外部签名，可直接使用";
  return personReady(person) ? "临时人员已签名" : "等待现场签名";
}

function otherWebSignDisabledReason(person: Dict): string {
  if (String(person.source || "") === "temporary" && !person.temp_id) return "临时签名会话不完整";
  if (String(person.source || "") === "external" && !person.record_id) return "外部签名记录不完整";
  return "";
}

function replaceOrAddOther(person: Dict): void {
  const normalized = protectedSigner({ ...person, role: "inspector" });
  const key = personKey(normalized);
  const index = selectedOther.value.findIndex((item) => personKey(item) === key);
  if (index >= 0) selectedOther.value[index] = { ...selectedOther.value[index], ...normalized };
  else selectedOther.value.push(normalized);
}

function removeOtherPerson(key: string): void {
  selectedOther.value = selectedOther.value.filter((item) => personKey(item) !== key);
  emitSelectionChanged();
}

function addExternalPerson(person: Dict): void {
  if (selectedCount.value >= 50) return setStatus("每个任务每栋楼最多选择 50 名检查人。", "error");
  replaceOrAddOther({ ...person, source: "external", role: "inspector", ready: true });
  emitSelectionChanged();
}

async function createTemporaryFromDraft(draft: Dict): Promise<Dict> {
  const signature = await createTemporarySignatureSession({
    scope: props.scope,
    noticeKey: props.contextKey,
    noticeTitle: props.taskTitle,
    specialty: "",
    role: "inspector",
    displayName: String(draft.display_name || "").trim(),
    contextType: "critical_guard",
    originStaffRecordId: String(draft.origin_staff_record_id || "").trim(),
    originStaffOpenId: String(draft.origin_staff_open_id || "").trim(),
  });
  return { ...signature, source: "temporary", role: "inspector" };
}

async function openDraftWebSignature(draft: Dict): Promise<void> {
  const draftId = String(draft.draft_id || "");
  if (draftSending.value[draftId]) return;
  draftSending.value = { ...draftSending.value, [draftId]: true };
  try {
    const person = await createTemporaryFromDraft(draft);
    replaceOrAddOther(person);
    removeDraft(draftId);
    emitSelectionChanged();
    openWebSignature(person);
  } catch (error: any) {
    setStatus(error?.message || "临时签名创建失败。", "error");
  } finally {
    const next = { ...draftSending.value };
    delete next[draftId];
    draftSending.value = next;
  }
}

async function sendDraftTemporaryLink(draft: Dict): Promise<void> {
  const draftId = String(draft.draft_id || "");
  if (draftDisabledReason(draft)) return;
  draftSending.value = { ...draftSending.value, [draftId]: true };
  try {
    const data = await sendTemporarySignatureLink({
      scope: props.scope,
      noticeKey: props.contextKey,
      noticeTitle: props.taskTitle,
      role: "inspector",
      displayName: String(draft.display_name || "").trim(),
      recipientOpenIds: [currentOpenId.value],
      contextType: "critical_guard",
      originStaffRecordId: String(draft.origin_staff_record_id || "").trim(),
      originStaffOpenId: String(draft.origin_staff_open_id || "").trim(),
    });
    const person = { ...(data.signature || {}), source: "temporary", role: "inspector" };
    replaceOrAddOther(person);
    removeDraft(draftId);
    emitSelectionChanged();
    setStatus(`${personName(person)} 的签名链接已发送给当前登录人，请在当前登录人的手机上完成现场签字。`, "success");
  } catch (error: any) {
    setStatus(error?.message || "临时签名链接发送失败。", "error");
  } finally {
    const next = { ...draftSending.value };
    delete next[draftId];
    draftSending.value = next;
  }
}

async function sendExistingTemporaryLink(person: Dict, silentStatus = false): Promise<boolean> {
  const tempId = String(person.temp_id || "");
  if (!tempId || temporaryLinkSending.value[tempId]) return false;
  if (!currentOpenId.value) {
    temporaryLinkErrors.value = { ...temporaryLinkErrors.value, [tempId]: "当前登录账号缺少 openid" };
    if (!silentStatus) setStatus("当前登录账号缺少 openid，无法接收临时签名链接。", "error");
    return false;
  }
  temporaryLinkSending.value = { ...temporaryLinkSending.value, [tempId]: true };
  try {
    const data = await sendTemporarySignatureLink({
      temporaryId: tempId,
      scope: props.scope,
      recipientOpenIds: [currentOpenId.value],
      contextType: "critical_guard",
    });
    replaceOrAddOther({ ...person, ...(data.signature || {}), source: "temporary", role: "inspector" });
    temporaryLinkSentAt.value = { ...temporaryLinkSentAt.value, [tempId]: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" }) };
    const errors = { ...temporaryLinkErrors.value };
    delete errors[tempId];
    temporaryLinkErrors.value = errors;
    emitStatusRefresh();
    if (!silentStatus) {
      setStatus(`${personName(person)} 的签名链接已发送给当前登录人，请在当前登录人的手机上完成现场签字。`, "success");
    }
    return true;
  } catch (error: any) {
    temporaryLinkErrors.value = { ...temporaryLinkErrors.value, [tempId]: error?.message || "发送失败" };
    if (!silentStatus) setStatus(error?.message || "临时签名链接发送失败。", "error");
    return false;
  } finally {
    const next = { ...temporaryLinkSending.value };
    delete next[tempId];
    temporaryLinkSending.value = next;
  }
}

function openWebSignature(person: Dict): void {
  const source = String(person.source || "staff");
  if (source === "staff") {
    const reason = companyWebSignDisabledReason(person);
    if (reason) return setStatus(reason, "warning");
  }
  activePadPerson.value = person;
  padMessage.value = "";
  padMessageType.value = "info";
  signatureCanvas.clear();
  signatureCanvas.resetInk();
  void nextTick(() => {
    signatureCanvas.resize();
    signatureCanvas.observe();
  });
}

function closeSignaturePad(): void {
  if (padSaving.value) return;
  activePadPerson.value = null;
  signatureCanvas.disconnect();
  signatureCanvas.clear();
}

async function saveWebSignature(): Promise<void> {
  const person = activePadPerson.value;
  if (!person || padSaveDisabledReason.value) return;
  const png = signatureCanvas.dataUrl();
  padSaving.value = true;
  try {
    const source = String(person.source || "staff");
    let saved: Dict;
    if (source === "temporary" || person.temp_id) {
      saved = await saveTemporarySignature(String(person.temp_id || ""), png);
      replaceOrAddOther({ ...person, ...saved, source: "temporary", role: "inspector", ready: true });
    } else if (source === "external") {
      saved = await saveExternalSignature(String(person.record_id || ""), personName(person), png);
      replaceOrAddOther({ ...person, ...saved, source: "external", role: "inspector", usage_confirmed: true, ready: true });
    } else {
      saved = await saveStaffSignature(String(person.record_id || ""), personName(person), png);
      const index = selectedCompany.value.findIndex((item) => String(item.record_id || "") === String(person.record_id || ""));
      if (index >= 0) selectedCompany.value[index] = protectedSigner({ ...selectedCompany.value[index], ...saved, source: "staff", role: "inspector", usage_confirmed: true, is_current_user: true, ready: true });
    }
    emitSelectionChanged();
    padMessage.value = "签名已保存。";
    padMessageType.value = "success";
    setStatus(`${personName(person)} 的签名已保存。`, "success");
    activePadPerson.value = null;
    signatureCanvas.disconnect();
    signatureCanvas.clear();
  } catch (error: any) {
    padMessage.value = error?.message || "签名保存失败。";
    padMessageType.value = "failed";
  } finally {
    padSaving.value = false;
  }
}

function markImageUnavailable(person: Dict): void {
  person.has_signature = false;
  person.signature_preview_url = "";
  person.ready = false;
  emitStatusRefresh();
}

function requestClose(): void {
  if (padSaving.value) return;
  if (drafts.value.length) {
    activeTab.value = "other";
    setStatus("请先为新增临时人员选择网页签名或发送链接，也可以移除该人员。", "warning");
    return;
  }
  emit("close");
}

function handleKeydown(event: KeyboardEvent): void {
  if (props.open && event.key === "Escape" && !activePadPerson.value) requestClose();
}

watch(() => [props.open, props.contextKey] as const, ([open]) => {
  loadGeneration += 1;
  if (!open) {
    stopPolling();
    return;
  }
  initializeSelection();
  activeTab.value = "company";
  statusMessage.value = "";
  void loadCompanyPeople();
  void loadTemporaryStatuses();
  void loadExternalPeople();
  startPolling();
}, { immediate: true });

watch(companySearch, () => {
  if (companySearchTimer) clearTimeout(companySearchTimer);
  companySearchTimer = setTimeout(() => void loadCompanyPeople(), 300);
});

watch(externalSearch, () => {
  if (externalSearchTimer) clearTimeout(externalSearchTimer);
  externalSearchTimer = setTimeout(() => void loadExternalPeople(), 300);
});

watch(() => props.open, (open) => {
  if (open) window.addEventListener("keydown", handleKeydown);
  else window.removeEventListener("keydown", handleKeydown);
});

onBeforeUnmount(() => {
  loadGeneration += 1;
  if (companySearchTimer) clearTimeout(companySearchTimer);
  if (externalSearchTimer) clearTimeout(externalSearchTimer);
  stopPolling();
  signatureCanvas.disconnect();
  window.removeEventListener("keydown", handleKeydown);
});
</script>

<style scoped>
.guard-signature-backdrop {
  position: fixed;
  inset: 0;
  z-index: 780;
  display: flex;
  justify-content: flex-end;
  background: rgba(7, 25, 54, 0.42);
  backdrop-filter: blur(5px);
}

.guard-signature-drawer {
  width: min(1120px, calc(100vw - 56px));
  height: 100vh;
  display: grid;
  grid-template-rows: auto auto auto auto minmax(0, 1fr) auto;
  overflow: hidden;
  border-left: 1px solid #c9dcf4;
  background: #f8fbff;
  box-shadow: -24px 0 64px rgba(11, 54, 116, 0.2);
  isolation: isolate;
}

.drawer-header,
.drawer-footer,
.signature-summary,
.source-tabs {
  margin: 0 16px;
}

.drawer-header {
  min-height: 70px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  border-bottom: 1px solid #d8e5f5;
}

.drawer-header div { min-width: 0; }
.drawer-header span,
.drawer-header strong { display: block; }
.drawer-header span { color: #5f7391; font-size: 12px; font-weight: 850; }
.drawer-header strong { margin-top: 3px; overflow: hidden; color: #102a52; font-size: 16px; text-overflow: ellipsis; white-space: nowrap; }

.icon-close {
  flex: 0 0 auto;
  width: 40px;
  height: 40px;
  display: grid;
  place-items: center;
  border: 1px solid #cdddf1;
  border-radius: 12px;
  background: #fff;
  color: #245b9f;
  cursor: pointer;
}

.signature-summary {
  min-height: 44px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  border: 1px solid #fed7aa;
  border-radius: 10px;
  padding: 0 12px;
  background: #fff8ed;
  color: #9a3412;
  font-size: 12px;
  font-weight: 900;
}
.signature-summary.ready { border-color: #a7e8c2; background: #effaf5; color: #047857; }
.signature-summary span { display: inline-flex; align-items: center; gap: 7px; }
.shared-signature-note { margin: 8px 14px 0; border-radius: 8px; padding: 7px 10px; background: #eef5ff; color: #285d9f; font-size: 12px; font-weight: 800; }

.source-tabs {
  display: flex;
  gap: 5px;
  padding: 6px 0;
  position: relative;
  z-index: 10;
  flex: none;
  background: #f8fbff;
  pointer-events: auto;
}
.source-tabs button {
  min-height: 30px;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  padding: 0 10px;
  background: #fff;
  color: #536b8b;
  font: inherit;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
  position: relative;
  z-index: 1;
  pointer-events: auto;
}
.source-tabs button.active { border-color: #3b82f6; background: #eaf3ff; color: #165dc7; }
.source-tabs span { min-width: 18px; border-radius: 999px; padding: 1px 5px; background: #edf2f8; font-size: 10px; line-height: 16px; text-align: center; }

.drawer-body {
  min-height: 0;
  overflow: auto;
  overscroll-behavior: contain;
  padding: 0 16px 14px;
  position: relative;
  z-index: 1;
}

.company-layout {
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(300px, 0.75fr) minmax(520px, 1.25fr);
  gap: 10px;
  align-items: start;
}

.drawer-footer {
  min-height: 62px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  border-top: 1px solid #d8e5f5;
  background: rgba(248, 251, 255, 0.98);
}
.drawer-footer > span { min-width: 0; overflow: hidden; color: #64748b; font-size: 12px; font-weight: 850; text-overflow: ellipsis; white-space: nowrap; }
.drawer-footer > span.success { color: #047857; }
.drawer-footer > span.error { color: #b42318; }
.drawer-footer > span.warning { color: #b45309; }
.done-button {
  min-width: 100px;
  min-height: 38px;
  border: 1px solid #1760dc;
  border-radius: 10px;
  background: #1764e8;
  color: #fff;
  font: inherit;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
}

.signature-canvas-shell {
  position: relative;
  min-height: 360px;
  margin: 14px;
  overflow: hidden;
  border: 2px dashed #8ab6ed;
  border-radius: 14px;
  background: #fff;
  touch-action: none;
}
.signature-canvas-shell canvas { position: absolute; inset: 0; width: 100%; height: 100%; cursor: crosshair; touch-action: none; }
.signature-canvas-shell > span { position: absolute; inset: 0; display: grid; place-items: center; color: #94a3b8; font-size: 15px; font-weight: 850; pointer-events: none; }

@media (max-width: 900px) {
  .guard-signature-drawer { width: 100vw; }
  .company-layout { grid-template-columns: 1fr; }
}

@media (max-width: 620px) {
  .drawer-header,
  .drawer-footer,
  .signature-summary,
  .source-tabs { margin-inline: 10px; }
  .drawer-body { padding-inline: 10px; }
  .source-tabs button { flex: 1; min-height: 44px; justify-content: center; }
  .done-button { min-height: 44px; }
  .signature-canvas-shell { min-height: 300px; margin: 10px; }
}
</style>
