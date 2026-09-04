<template>
  <Teleport to="body">
    <div v-if="open" class="guard-signature-backdrop" @click.self="requestClose">
      <aside class="guard-signature-drawer" role="dialog" aria-modal="true" aria-label="选择检查人签名">
        <header class="drawer-header">
          <div>
            <span>检查人签名</span>
            <strong>{{ taskTitle }} · 当前楼栋全部检查表</strong>
          </div>
          <button type="button" class="icon-close" aria-label="关闭检查人选择" @click="requestClose">
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
            临时/外部人员 <span>{{ selectedOther.length }}</span>
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
              @refresh="refreshAllSignatures"
              @select="toggleCompanyPerson"
            />
            <MopCompanySelectedSignatures
              role="inspector"
              ready-target-label="检查表"
              :people="selectedCompany"
              :active-record-id="activePersonKey"
              :unsigned-count="companyPendingCount"
              :unsigned-signature-count="companyUnsignedCount"
              :has-usable-signature="personReady"
              :person-key="personKey"
              :display-name="personName"
              :confirm-sending="confirmationSending"
              :confirmable-count="companyConfirmableCount"
              @activate="activatePerson"
              @send-confirmations="sendAllUsageConfirmations"
              @remove="removeCompanyPerson"
            />
          </section>

          <MopOtherSignatureManager
            v-show="activeTab === 'other'"
            role="inspector"
            :active="activeTab === 'other'"
            :display-rows="otherDisplayRows"
            :unsigned-count="otherPendingCount"
            :external-search="externalSearch"
            :external-loading="externalLoading"
            :external-status-text="externalStatusText"
            :external-people="externalPeople"
            :person-status-text="otherPersonStatusText"
            @remove-person="removeOtherPerson"
            @update:external-search="externalSearch = $event"
            @refresh-external="refreshAllSignatures"
            @add-external="addExternalPerson"
          />
        </div>

        <footer class="drawer-footer">
          <span v-if="statusMessage" :class="statusTone">{{ statusMessage }}</span>
          <span v-else>{{ totalSignerCount ? `可用 ${readyCount}/${totalSignerCount}` : "尚未选择签名人员" }}</span>
          <button type="button" class="done-button" @click="requestClose">完成</button>
        </footer>
      </aside>

    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { UsersRound, X } from "lucide-vue-next";
import type { Dict } from "../api/client";
import {
  refreshSignatureDirectory,
  refreshedSignaturePerson,
  fetchExternalSignaturePeople,
  fetchSignaturePeople,
  fetchTemporarySignatures,
  sendSignatureUsageConfirmations,
} from "../mopSignatureApi";
import MopCompanySelectedSignatures from "./MopCompanySelectedSignatures.vue";
import MopCompanySignaturePicker from "./MopCompanySignaturePicker.vue";
import MopOtherSignatureManager from "./MopOtherSignatureManager.vue";

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
const externalSearch = ref("");
const externalPeople = ref<Dict[]>([]);
const externalLoading = ref(false);
const externalTotal = ref(0);
const confirmationSending = ref(false);
const activePersonKey = ref("");
const statusMessage = ref("");
const statusTone = ref("info");
let companySearchTimer: ReturnType<typeof setTimeout> | null = null;
let externalSearchTimer: ReturnType<typeof setTimeout> | null = null;
let pollingTimer: ReturnType<typeof setInterval> | null = null;
let loadGeneration = 0;
let companyLoadSequence = 0;
let temporaryLoadSequence = 0;
let externalLoadSequence = 0;
let pendingStatusRefreshInFlight = false;

const currentOpenId = computed(() => String(props.currentUserOpenId || "").trim());

const temporaryMappedCompanyIds = computed(() => companyPeople.value
  .filter((item) => findMatchingPerson(item, selectedOther.value))
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
const totalSignerCount = computed(() => selectedCount.value);
const totalPendingCount = computed(() => pendingCount.value);
const companyUnsignedCount = computed(() => selectedCompany.value.filter((item) => !personHasStoredSignature(item)).length);
const companyPendingCount = computed(() => selectedCompany.value.filter((item) => !personReady(item)).length);
const companyConfirmableCount = computed(() => selectedCompany.value.filter((item) => (
  personHasStoredSignature(item)
  && !personReady(item)
  && String(item.open_id || "").trim()
  && String(item.open_id || "").trim() !== currentOpenId.value
)).length);
const otherPendingCount = computed(() => selectedOther.value.filter((item) => !personReady(item)).length);
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
const otherDisplayRows = computed(() => [
  ...selectedOther.value.map((person) => ({
    kind: "person",
    row_key: personKey(person),
    person,
    draft: {},
    signed: personReady(person),
    display_name: personName(person),
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
    if (companyPerson.has_signature) {
      retainedCompany.push(companyPerson);
      continue;
    }
    const matchedOther = findMatchingPerson(companyPerson, selectedOther.value)
      || findMatchingPerson(companyPerson, temporaryPeople.value)
      || findMatchingPerson(companyPerson, externalPeople.value);
    if (!matchedOther) {
      retainedCompany.push(companyPerson);
      continue;
    }
    if (matchedOther) replaceOrAddOther(withCompanyOrigin(matchedOther, companyPerson));
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
  activePersonKey.value = "";
}

function loadContextIsCurrent(generation: number, scope: string, contextKey: string): boolean {
  return generation === loadGeneration
    && props.open
    && props.scope === scope
    && props.contextKey === contextKey;
}

async function refreshAllSignatures(): Promise<void> {
  if (companyLoading.value || externalLoading.value) return;
  const generation = loadGeneration, requestScope = props.scope, context = props.contextKey;
  ++companyLoadSequence; ++externalLoadSequence;
  companyLoading.value = true; externalLoading.value = true;
  try {
    const data = await refreshSignatureDirectory();
    if (!loadContextIsCurrent(generation, requestScope, context)) return;
    const selected = [...selectedCompany.value, ...selectedOther.value].map((p) => refreshedSignaturePerson(p, data));
    const unique = [...new Map(selected.map((p) => [personKey(p), p])).values()];
    selectedCompany.value = unique.filter((p) => !p.source || p.source === "staff");
    selectedOther.value = unique.filter((p) => p.source === "external" || p.source === "temporary");
    companyPeople.value = (data.people || []).filter((p: Dict) => p.source === "staff");
    externalPeople.value = (data.people || []).filter((p: Dict) => p.source === "external");
    companyTotal.value = companyPeople.value.length;
    externalTotal.value = externalPeople.value.length;
    emitStatusRefresh();
    setStatus(Object.values(data.sources || {}).some((s: any) => !s.ok) ? "部分人员表刷新失败，保留上次数据。" : "两张人员表签名已刷新，已保留检查人选择。", "success");
  } catch (error: any) { setStatus(error.message || "刷新失败，已保留原选择。", "error"); }
  finally { companyLoading.value = false; externalLoading.value = false; }
}

async function loadCompanyPeople(refresh = false, silent = false): Promise<void> {
  if (!props.open) return;
  const sequence = ++companyLoadSequence;
  const generation = loadGeneration;
  const requestScope = props.scope;
  const requestContextKey = props.contextKey;
  const requestQuery = companySearch.value;
  companyLoading.value = true;
  try {
    const data = await fetchSignaturePeople({
      scope: requestScope,
      q: requestQuery,
      noticeKey: requestContextKey,
      refresh,
      limit: 100,
    });
    if (
      sequence !== companyLoadSequence
      || !loadContextIsCurrent(generation, requestScope, requestContextKey)
      || companySearch.value !== requestQuery
    ) return;
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
    if (
      sequence === companyLoadSequence
      && loadContextIsCurrent(generation, requestScope, requestContextKey)
      && companySearch.value === requestQuery
      && !silent
    ) setStatus(error?.message || "公司人员读取失败。", "error");
  } finally {
    if (sequence === companyLoadSequence && generation === loadGeneration) companyLoading.value = false;
  }
}

async function loadTemporaryStatuses(silent = false): Promise<void> {
  if (!props.open) return;
  const sequence = ++temporaryLoadSequence;
  const generation = loadGeneration;
  const requestScope = props.scope;
  const requestContextKey = props.contextKey;
  try {
    const data = await fetchTemporarySignatures(requestScope, requestContextKey);
    if (
      sequence !== temporaryLoadSequence
      || !loadContextIsCurrent(generation, requestScope, requestContextKey)
    ) return;
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
    if (
      sequence === temporaryLoadSequence
      && loadContextIsCurrent(generation, requestScope, requestContextKey)
      && !silent
    ) setStatus(error?.message || "临时签名状态读取失败。", "error");
  }
}

async function loadExternalPeople(refresh = false): Promise<void> {
  if (!props.open) return;
  const sequence = ++externalLoadSequence;
  externalLoading.value = true;
  const generation = loadGeneration;
  const requestScope = props.scope;
  const requestContextKey = props.contextKey;
  const requestQuery = externalSearch.value;
  try {
    const data = await fetchExternalSignaturePeople({
      scope: requestScope,
      q: requestQuery,
      noticeKey: requestContextKey,
      refresh,
      limit: 100,
    });
    if (
      sequence !== externalLoadSequence
      || !loadContextIsCurrent(generation, requestScope, requestContextKey)
      || externalSearch.value !== requestQuery
    ) return;
    externalPeople.value = Array.isArray(data.people) ? data.people.map(protectedSigner) : [];
    externalTotal.value = Number(data.count || externalPeople.value.length);
    if (reconcileDuplicateSelections()) emitStatusRefresh();
  } catch (error: any) {
    if (
      sequence === externalLoadSequence
      && loadContextIsCurrent(generation, requestScope, requestContextKey)
      && externalSearch.value === requestQuery
    ) setStatus(error?.message || "外部签名读取失败。", "error");
  } finally {
    if (sequence === externalLoadSequence && generation === loadGeneration) externalLoading.value = false;
  }
}

async function refreshPendingStatuses(): Promise<void> {
  if (!props.open || document.hidden || !pendingCount.value || pendingStatusRefreshInFlight) return;
  pendingStatusRefreshInFlight = true;
  try {
    await Promise.all([
      loadCompanyPeople(false, true),
      loadTemporaryStatuses(true),
    ]);
  } finally {
    pendingStatusRefreshInFlight = false;
  }
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

function switchSourceTab(tab: "company" | "other"): void {
  activeTab.value = tab;
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
    const failed = Number(data.failed_count || 0);
    setStatus(
      failed
        ? `确认请求已发送 ${Number(data.sent_count || 0)} 人，${failed} 人发送失败；签名新增或重签请使用首页指纹入口。`
        : `确认请求已发送 ${Number(data.sent_count || 0)} 人。`,
      failed ? "warning" : "success",
    );
    await loadCompanyPeople(false, true);
  } catch (error: any) {
    setStatus(error?.message || "确认请求发送失败。", "error");
  } finally {
    confirmationSending.value = false;
  }
}

function otherPersonStatusText(person: Dict): string {
  if (String(person.source || "") === "external") return "已有外部签名，可直接使用";
  return personReady(person) ? "临时人员已签名" : "等待现场签名";
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

function requestClose(): void {
  emit("close");
}

function handleKeydown(event: KeyboardEvent): void {
  if (props.open && event.key === "Escape") requestClose();
}

watch(() => [props.open, props.scope, props.contextKey] as const, ([open]) => {
  loadGeneration += 1;
  if (!open) {
    stopPolling();
    companyLoading.value = false;
    externalLoading.value = false;
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
  companySearchTimer = setTimeout(() => {
    if (props.open) void loadCompanyPeople();
  }, 300);
});

watch(externalSearch, () => {
  if (externalSearchTimer) clearTimeout(externalSearchTimer);
  externalSearchTimer = setTimeout(() => {
    if (props.open) void loadExternalPeople();
  }, 300);
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
