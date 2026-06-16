<template>
  <section class="engineer-mop">
    <header class="mop-head">
      <div>
        <strong>工程师 MOP 填写</strong>
        <p>选择当天维保通告，绑定已有 MOP 表格后在右侧预览 Sheet 内容。</p>
      </div>
      <div class="head-actions">
        <label v-if="scopeOptions.length" class="scope-select">
          楼栋
          <select v-model="scope" :disabled="loading || previewLoading">
            <option v-for="item in scopeOptions" :key="item.value" :value="normalizeScope(item.value)">
              {{ item.label }}
            </option>
          </select>
        </label>
        <button class="btn ghost" type="button" :disabled="loading" @click="loadPage">刷新</button>
        <a class="btn ghost" href="/">功能选择</a>
      </div>
    </header>

    <div v-if="checking" class="notice-box">正在检查登录状态...</div>
    <div v-else-if="!loggedIn" class="notice-box">
      请先登录飞书后再使用工程师 MOP 页面。
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </div>

    <template v-else>
      <div v-if="message" class="message" :class="{ failed: messageType === 'failed', success: messageType === 'success' }">
        {{ message }}
      </div>
      <div v-if="warnings.length" class="warning-list">
        <span v-for="warning in warnings" :key="warning">{{ warning }}</span>
      </div>

      <section class="mop-summary">
        <article>
          <span>维保通告</span>
          <strong>{{ notices.length }}</strong>
        </article>
        <article>
          <span>已绑定</span>
          <strong>{{ boundNoticeCount }}</strong>
        </article>
        <article>
          <span>MOP 表格</span>
          <strong>{{ mopCandidates.length }}</strong>
        </article>
        <article>
          <span>当前范围</span>
          <strong>{{ scopeLabel }}</strong>
        </article>
      </section>

      <section class="mop-layout">
        <aside class="panel notice-panel">
          <div class="panel-head">
            <div>
              <h2>当天维保通告</h2>
              <p>包含今日进行中与已闭环维保通告。</p>
            </div>
            <span>{{ filteredNotices.length }}</span>
          </div>
          <div class="filters">
            <input v-model="noticeSearch" placeholder="搜索通告、楼栋、专业" />
            <select v-model="noticeStatusFilter">
              <option value="">全部状态</option>
              <option value="ongoing">未完成</option>
              <option value="closed">已完成</option>
              <option value="bound">已绑定</option>
              <option value="unbound">未绑定</option>
            </select>
          </div>
          <div class="list notice-list">
            <button
              v-for="notice in filteredNotices"
              :key="notice.notice_key"
              type="button"
              class="notice-row"
              :class="{ active: notice.notice_key === selectedNoticeKey, closed: notice.status === '已结束' }"
              @click="selectNotice(notice.notice_key)"
            >
              <span class="row-status" :class="{ closed: notice.status === '已结束' }">{{ notice.status || "进行中" }}</span>
              <strong>{{ notice.title || "未命名维保通告" }}</strong>
              <small>
                {{ notice.building || "未识别楼栋" }}
                <template v-if="notice.specialty"> · {{ notice.specialty }}</template>
                <template v-if="notice.maintenance_cycle"> · {{ notice.maintenance_cycle }}</template>
              </small>
              <em v-if="notice.mop_binding">已绑定：{{ notice.mop_binding.mop_title || notice.mop_binding.mop_record_id }}</em>
              <em v-else>未绑定 MOP 表格</em>
            </button>
          </div>
        </aside>

        <section class="panel binding-panel">
          <div class="panel-head">
            <div>
              <h2>MOP 对应关系</h2>
              <p>先选左侧通告，再选择现有 MOP 表格。</p>
            </div>
          </div>

          <div v-if="!selectedNotice" class="empty-box">请选择一条维保通告。</div>
          <template v-else>
            <article class="selected-notice">
              <span>{{ selectedNotice.status || "进行中" }}</span>
              <strong>{{ selectedNotice.title }}</strong>
              <p>
                {{ selectedNotice.building || "-" }}
                <template v-if="selectedNotice.start_time || selectedNotice.end_time">
                  · {{ selectedNotice.start_time || "未填开始" }} ~ {{ selectedNotice.end_time || "未填结束" }}
                </template>
              </p>
            </article>

            <label class="field">
              <span>选择 MOP 表格</span>
              <input v-model="mopSearch" placeholder="搜索 MOP 名称或字段内容" />
            </label>
            <div class="mop-candidate-list">
              <button
                v-for="mop in filteredMopCandidates"
                :key="mop.record_id"
                type="button"
                class="mop-row"
                :class="{ active: mop.record_id === selectedMopRecordId }"
                @click="selectMop(mop.record_id)"
              >
                <strong>{{ mop.title || "未命名 MOP" }}</strong>
                <small>附件 {{ mop.attachment_count || 0 }} 个</small>
              </button>
            </div>

            <label v-if="selectedMopAttachments.length" class="field">
              <span>表格附件</span>
              <select v-model="selectedAttachmentToken">
                <option
                  v-for="attachment in selectedMopAttachments"
                  :key="attachmentKey(attachment)"
                  :value="attachment.file_token || attachment.url || attachment.name"
                >
                  {{ attachment.name || "MOP表格" }}
                </option>
              </select>
            </label>
            <div v-else-if="selectedMop" class="empty-box warning">
              该 MOP 记录暂未识别到 xlsx/csv 附件。
            </div>

            <div class="actions">
              <button class="btn ghost" type="button" :disabled="!canBind || saving" @click="saveBinding">
                {{ saving ? "保存中" : "保存对应关系" }}
              </button>
              <button class="btn blue" type="button" :disabled="!canPreview || previewLoading" @click="startMopPreview">
                {{ previewLoading ? "加载表格中" : "开始填写 MOP 表格" }}
              </button>
            </div>
          </template>
        </section>

        <section class="panel sheet-panel">
          <div class="panel-head">
            <div>
              <h2>表格预览</h2>
              <p>{{ previewTitle }}</p>
            </div>
            <span v-if="activeSheet">{{ activeSheet.row_count || 0 }} 行</span>
          </div>
          <div v-if="previewLoading" class="empty-box">正在读取表格附件...</div>
          <div v-else-if="!preview" class="empty-box">选择 MOP 后点击“开始填写 MOP 表格”。</div>
          <template v-else>
            <div class="sheet-tabs">
              <button
                v-for="sheet in preview.sheets || []"
                :key="sheet.name"
                type="button"
                :class="{ active: sheet.name === activeSheetName }"
                @click="activeSheetName = sheet.name"
              >
                {{ sheet.name }}
              </button>
            </div>
            <div v-if="activeSheet?.truncated" class="sheet-note">
              表格较大，当前预览已限制显示前 {{ activeSheet.row_count }} 行 / {{ activeSheet.column_count }} 列。
            </div>
            <div class="sheet-scroll">
              <table v-if="activeSheet">
                <tbody>
                  <tr v-for="(row, rowIndex) in activeSheet.rows || []" :key="rowIndex">
                    <th>{{ rowIndex + 1 }}</th>
                    <td
                      v-for="colIndex in activeSheetColumnIndexes"
                      :key="`${rowIndex}:${colIndex}`"
                    >
                      {{ row[colIndex] || "" }}
                    </td>
                  </tr>
                </tbody>
              </table>
              <div v-else class="empty-box">该附件没有可显示的 Sheet。</div>
            </div>
          </template>
        </section>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { requestJson, type Dict } from "../api/client";

type ScopeOption = { value: string; label: string };

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: ScopeOption[];
}>();

const scope = ref(normalizeScope(new URLSearchParams(window.location.search).get("scope") || props.scopeOptions[0]?.value || "ALL"));
const loading = ref(false);
const saving = ref(false);
const previewLoading = ref(false);
const message = ref("");
const messageType = ref("");
const warnings = ref<string[]>([]);
const notices = ref<Dict[]>([]);
const mopCandidates = ref<Dict[]>([]);
const selectedNoticeKey = ref("");
const selectedMopRecordId = ref("");
const selectedAttachmentToken = ref("");
const noticeSearch = ref("");
const noticeStatusFilter = ref("");
const mopSearch = ref("");
const preview = ref<Dict | null>(null);
const activeSheetName = ref("");

const scopeLabel = computed(() => {
  const found = props.scopeOptions.find((item) => normalizeScope(item.value) === scope.value);
  return found?.label || scope.value || "全部";
});

const boundNoticeCount = computed(() => notices.value.filter((item) => item.mop_binding).length);

const filteredNotices = computed(() => {
  const query = compactText(noticeSearch.value);
  return notices.value.filter((item) => {
    const status = String(item.status || "");
    if (noticeStatusFilter.value === "ongoing" && status === "已结束") return false;
    if (noticeStatusFilter.value === "closed" && status !== "已结束") return false;
    if (noticeStatusFilter.value === "bound" && !item.mop_binding) return false;
    if (noticeStatusFilter.value === "unbound" && item.mop_binding) return false;
    if (!query) return true;
    return compactText([
      item.title,
      item.building,
      item.specialty,
      item.maintenance_cycle,
      item.location,
      item.content,
      item.reason,
    ].join(" ")).includes(query);
  });
});

const selectedNotice = computed(() => notices.value.find((item) => item.notice_key === selectedNoticeKey.value) || null);
const selectedMop = computed(() => mopCandidates.value.find((item) => item.record_id === selectedMopRecordId.value) || null);
const selectedMopAttachments = computed(() => {
  const items = selectedMop.value?.attachments;
  return Array.isArray(items) ? items : [];
});
const selectedAttachment = computed(() => {
  const key = selectedAttachmentToken.value;
  return selectedMopAttachments.value.find((item) => attachmentKey(item) === key) || selectedMopAttachments.value[0] || null;
});
const filteredMopCandidates = computed(() => {
  const query = compactText(mopSearch.value);
  if (!query) return mopCandidates.value;
  return mopCandidates.value.filter((item) => compactText([
    item.title,
    ...Object.values(item.fields || {}),
  ].join(" ")).includes(query));
});
const canBind = computed(() => Boolean(selectedNotice.value && selectedMop.value));
const canPreview = computed(() => Boolean(selectedNotice.value && selectedMop.value && selectedAttachment.value));
const previewTitle = computed(() => {
  if (!preview.value) return "显示选中的 xlsx/csv 表格内容，支持 Sheet 切换。";
  return `${preview.value.mop_title || selectedMop.value?.title || "MOP表格"} · ${preview.value.attachment?.name || ""}`;
});
const activeSheet = computed(() => {
  const sheets = Array.isArray(preview.value?.sheets) ? preview.value?.sheets : [];
  return sheets.find((item: Dict) => item.name === activeSheetName.value) || sheets[0] || null;
});
const activeSheetColumnIndexes = computed(() => {
  const count = Math.max(0, Number(activeSheet.value?.column_count || 0));
  return Array.from({ length: count }, (_value, index) => index);
});

function normalizeScope(value: string | null | undefined, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function compactText(value: unknown): string {
  return String(value || "").replace(/\s+/g, "").toLowerCase();
}

function attachmentKey(attachment: Dict): string {
  return String(attachment?.file_token || attachment?.url || attachment?.name || "").trim();
}

function applyNoticeBinding(notice: Dict): void {
  const binding = notice?.mop_binding;
  if (!binding) return;
  selectedMopRecordId.value = String(binding.mop_record_id || "");
  selectedAttachmentToken.value = String(binding.mop_attachment_token || "");
  activeSheetName.value = String(binding.selected_sheet || "");
}

function selectNotice(noticeKey: string): void {
  selectedNoticeKey.value = noticeKey;
  preview.value = null;
  const notice = selectedNotice.value;
  if (notice) applyNoticeBinding(notice);
}

function selectMop(recordId: string): void {
  selectedMopRecordId.value = recordId;
  preview.value = null;
  const first = selectedMopAttachments.value[0];
  selectedAttachmentToken.value = first ? attachmentKey(first) : "";
}

async function loadPage(): Promise<void> {
  if (!props.loggedIn) return;
  loading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const data = await requestJson(`/api/engineer/mop/bootstrap?scope=${encodeURIComponent(scope.value)}`);
    notices.value = Array.isArray(data.notices) ? data.notices : [];
    mopCandidates.value = Array.isArray(data.mop_candidates) ? data.mop_candidates : [];
    warnings.value = Array.isArray(data.warnings) ? data.warnings.map((item: unknown) => String(item)) : [];
    if (!selectedNoticeKey.value || !notices.value.some((item) => item.notice_key === selectedNoticeKey.value)) {
      selectedNoticeKey.value = notices.value[0]?.notice_key || "";
    }
    if (selectedNotice.value) applyNoticeBinding(selectedNotice.value);
    if (!selectedMopRecordId.value && mopCandidates.value.length) {
      selectMop(mopCandidates.value[0].record_id);
    }
  } catch (error) {
    message.value = error instanceof Error ? error.message : "加载工程师 MOP 数据失败";
    messageType.value = "failed";
  } finally {
    loading.value = false;
  }
}

async function saveBinding(): Promise<Dict | null> {
  if (!selectedNotice.value || !selectedMop.value) return null;
  saving.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const payload = {
      scope: scope.value,
      notice_key: selectedNotice.value.notice_key,
      notice_title: selectedNotice.value.title,
      notice_status: selectedNotice.value.status,
      source_record_id: selectedNotice.value.source_record_id,
      target_record_id: selectedNotice.value.target_record_id,
      active_item_id: selectedNotice.value.active_item_id,
      mop_app_token: selectedMop.value.app_token,
      mop_table_id: selectedMop.value.table_id,
      mop_record_id: selectedMop.value.record_id,
      mop_title: selectedMop.value.title,
      mop_attachment_token: attachment.file_token || attachment.url || "",
      mop_attachment_name: attachment.name || "",
      selected_sheet: activeSheetName.value,
    };
    const data = await requestJson("/api/engineer/mop/bind", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const binding = data.binding || {};
    const notice = selectedNotice.value;
    if (notice) notice.mop_binding = binding;
    message.value = "MOP 对应关系已保存。";
    messageType.value = "success";
    return binding;
  } catch (error) {
    message.value = error instanceof Error ? error.message : "保存 MOP 对应关系失败";
    messageType.value = "failed";
    return null;
  } finally {
    saving.value = false;
  }
}

async function startMopPreview(): Promise<void> {
  if (!canPreview.value || !selectedMop.value) return;
  const binding = await saveBinding();
  if (!binding) return;
  previewLoading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const params = new URLSearchParams({
      scope: scope.value,
      mop_record_id: String(selectedMop.value.record_id || ""),
      file_token: attachmentKey(attachment),
      file_name: String(attachment.name || ""),
    });
    const data = await requestJson(`/api/engineer/mop/preview?${params.toString()}`);
    preview.value = data;
    const sheets = Array.isArray(data.sheets) ? data.sheets : [];
    activeSheetName.value = activeSheetName.value || String(sheets[0]?.name || "");
    if (Array.isArray(data.warnings)) {
      warnings.value = [...new Set([...warnings.value, ...data.warnings.map((item: unknown) => String(item))])];
    }
  } catch (error) {
    message.value = error instanceof Error ? error.message : "读取 MOP 表格失败";
    messageType.value = "failed";
  } finally {
    previewLoading.value = false;
  }
}

watch(scope, () => {
  const url = new URL(window.location.href);
  url.searchParams.set("scope", scope.value);
  window.history.replaceState({}, "", url);
  selectedNoticeKey.value = "";
  selectedMopRecordId.value = "";
  selectedAttachmentToken.value = "";
  preview.value = null;
  void loadPage();
});

watch(selectedMopAttachments, (items) => {
  if (!items.length) {
    selectedAttachmentToken.value = "";
    return;
  }
  if (!items.some((item) => attachmentKey(item) === selectedAttachmentToken.value)) {
    selectedAttachmentToken.value = attachmentKey(items[0]);
  }
});

onMounted(() => {
  if (props.loggedIn) void loadPage();
});

watch(() => props.loggedIn, (value) => {
  if (value) void loadPage();
});

watch(() => props.scopeOptions, (items) => {
  if (!items.length) return;
  const allowed = items.map((item) => normalizeScope(item.value));
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

.mop-head,
.mop-summary article,
.panel,
.notice-box,
.message,
.warning-list {
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.88);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.mop-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 18px;
  padding: 22px 24px;
}

.mop-head strong {
  display: block;
  color: #0f172a;
  font-size: 24px;
  font-weight: 700;
}

.mop-head p,
.panel-head p,
.selected-notice p {
  margin: 6px 0 0;
  color: #64748b;
}

.head-actions,
.actions,
.filters {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.scope-select {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #475569;
  font-size: 13px;
  font-weight: 700;
}

select,
input {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 11px;
  color: #0f172a;
  background: #ffffff;
  outline: none;
}

input:focus,
select:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.btn {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 14px;
  color: #0f172a;
  font-weight: 750;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.mop-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
}

.mop-summary article {
  padding: 18px 20px;
}

.mop-summary span {
  color: #64748b;
  font-size: 13px;
}

.mop-summary strong {
  display: block;
  margin-top: 8px;
  color: #005bff;
  font-size: 26px;
  font-weight: 800;
}

.mop-layout {
  display: grid;
  grid-template-columns: minmax(270px, 0.9fr) minmax(310px, 1fr) minmax(520px, 1.7fr);
  gap: 18px;
  align-items: start;
}

.panel {
  min-height: 620px;
  padding: 18px;
  display: grid;
  grid-template-rows: auto auto 1fr;
  gap: 14px;
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
  font-size: 18px;
}

.panel-head > span {
  padding: 5px 10px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  color: #005bff;
  background: #eff6ff;
  font-weight: 800;
}

.list,
.mop-candidate-list {
  min-height: 0;
  overflow: auto;
  display: grid;
  gap: 10px;
  align-content: start;
  padding-right: 4px;
}

.notice-row,
.mop-row,
.selected-notice {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 13px 14px;
  text-align: left;
  color: #0f172a;
  background: #ffffff;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.notice-row,
.mop-row {
  cursor: pointer;
}

.notice-row:hover,
.mop-row:hover,
.notice-row.active,
.mop-row.active {
  border-color: #1e63ff;
  box-shadow: 0 10px 24px rgba(30, 99, 255, 0.13);
  transform: translateY(-1px);
}

.notice-row strong,
.mop-row strong,
.selected-notice strong {
  display: block;
  margin-top: 8px;
  line-height: 1.45;
}

.notice-row small,
.mop-row small,
.notice-row em {
  display: block;
  margin-top: 7px;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
}

.notice-row em {
  color: #2563eb;
}

.row-status,
.selected-notice span {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 4px 9px;
  border-radius: 999px;
  color: #047857;
  background: #ecfdf5;
  font-size: 12px;
  font-weight: 800;
}

.row-status.closed,
.notice-row.closed .row-status {
  color: #475569;
  background: #f1f5f9;
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

.sheet-panel {
  min-width: 0;
}

.sheet-tabs {
  display: flex;
  gap: 8px;
  overflow-x: auto;
  padding-bottom: 2px;
}

.sheet-tabs button {
  flex: 0 0 auto;
  min-height: 34px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 7px 12px;
  background: #ffffff;
  color: #475569;
  font-weight: 750;
  cursor: pointer;
}

.sheet-tabs button.active {
  border-color: #1e63ff;
  color: #ffffff;
  background: #1e63ff;
}

.sheet-note {
  padding: 10px 12px;
  border: 1px solid #fde68a;
  border-radius: 12px;
  color: #92400e;
  background: #fffbeb;
  font-size: 13px;
}

.sheet-scroll {
  min-height: 470px;
  max-height: 68vh;
  overflow: auto;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: #ffffff;
}

table {
  border-collapse: collapse;
  min-width: 100%;
  font-size: 13px;
}

th,
td {
  min-width: 96px;
  max-width: 360px;
  border: 1px solid #e2e8f0;
  padding: 7px 9px;
  vertical-align: top;
  white-space: pre-wrap;
  word-break: break-word;
}

th {
  position: sticky;
  left: 0;
  z-index: 1;
  min-width: 44px;
  width: 44px;
  color: #64748b;
  background: #f8fafc;
  text-align: center;
}

@media (max-width: 1180px) {
  .mop-summary,
  .mop-layout {
    grid-template-columns: 1fr;
  }

  .panel {
    min-height: auto;
  }
}
</style>
