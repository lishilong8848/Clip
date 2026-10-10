<template>
  <section class="kb-page">
    <header class="kb-head">
      <div class="kb-title">
        <Database :size="22" aria-hidden="true" />
        <h1>共享知识库</h1>
        <span v-if="isAdmin" class="kb-badge" :class="settingsMeta?.configured ? 'ok' : 'warn'">
          {{ settingsMeta?.configured ? '已配置' : '未配置' }}
        </span>
      </div>
      <div class="kb-actions">
        <button v-if="isAdmin" type="button" class="icon-button" title="知识库设置" aria-label="知识库设置" :disabled="loadingList" @click="openSettings">
          <Settings :size="18" />
        </button>
        <button type="button" class="primary" :disabled="uploadBusy" @click="openUpload">
          <Upload :size="16" />上传文件
        </button>
      </div>
    </header>

    <div class="kb-toolbar">
      <label class="kb-search">
        <Search :size="16" aria-hidden="true" />
        <input v-model="search" type="search" placeholder="搜索文件名" aria-label="搜索知识库" spellcheck="false" />
        <Loader2 v-if="loadingList" :size="15" class="spin" aria-hidden="true" />
      </label>
      <div class="kb-toggle" role="group" aria-label="列表范围">
        <button :class="{ active: !recycle }" :aria-pressed="!recycle" @click="switchBin(false)"><FileText :size="15" />可用文档</button>
        <button :class="{ active: recycle }" :aria-pressed="recycle" @click="switchBin(true)"><Trash2 :size="15" />回收站</button>
      </div>
      <button type="button" class="icon-button" title="刷新" aria-label="刷新列表" :disabled="loadingList" @click="refresh">
        <RefreshCw :size="17" :class="{ spin: loadingList }" />
      </button>
    </div>

    <div v-if="error" class="alert error" role="alert">
      <AlertCircle :size="18" aria-hidden="true" /><span>{{ error }}</span>
      <button type="button" class="icon-button" aria-label="关闭错误提示" @click="error = ''"><X :size="16" /></button>
    </div>
    <div v-if="notice" class="alert success" role="status">
      <CheckCircle2 :size="18" aria-hidden="true" /><span>{{ notice }}</span>
    </div>

    <div v-if="uploadPanel" class="kb-upload">
      <div class="kb-upload-head">
        <strong>{{ uploadTarget ? '替换文档' : '新增共享文档' }}</strong>
        <button type="button" class="icon-button" aria-label="关闭上传" :disabled="uploadBusy" @click="uploadPanel = false"><X :size="16" /></button>
      </div>
      <label class="kb-upload-pick">
        <Upload :size="15" aria-hidden="true" />选择文件
        <input type="file" :multiple="!uploadTarget" @change="onUploadPick" />
      </label>
      <ul v-if="pendingFiles.length" class="kb-upload-list">
        <li v-for="(file, index) in pendingFiles" :key="index">
          <FileText :size="14" aria-hidden="true" /><span>{{ file.name }}</span><em>{{ sizeText(file.size) }}</em>
        </li>
      </ul>
      <p v-if="uploadError" class="alert error" role="alert"><AlertCircle :size="15" aria-hidden="true" /><span>{{ uploadError }}</span></p>
      <p v-if="uploadTarget" class="kb-upload-replace">上传后将为「{{ uploadTarget.name }}」建立新索引版本，原共享版本继续保留。</p>
      <div class="kb-upload-actions">
        <span class="muted">最多 10 个 · 单个 ≤20MiB · 合计 ≤100MiB</span>
        <button type="button" class="primary" :disabled="!pendingFiles.length || uploadBusy" @click="requestUpload">
          <Loader2 v-if="uploadBusy" :size="15" class="spin" aria-hidden="true" />{{ uploadBusy ? '上传中…' : '确认上传' }}
        </button>
      </div>
    </div>

    <div class="kb-layout" :class="{ 'has-detail': detail }">
      <div class="kb-pane">
        <div v-if="loadingList && !quietLoading" class="empty"><Loader2 :size="24" class="spin" aria-hidden="true" />正在读取文档…</div>
        <div v-else-if="!items.length" class="empty">
          {{ recycle ? '回收站是空的' : search.trim() ? '没有匹配的文档' : '尚未上传文档' }}
        </div>
        <ul v-else class="kb-list" aria-label="文档列表">
          <li v-for="item in items" :key="item.id" class="kb-row" :class="{ selected: detail?.id === item.id }">
            <button type="button" class="kb-row-main" @click="openDetail(item)">
              <span class="kb-file-icon"><FileText :size="17" aria-hidden="true" /></span>
              <span class="kb-row-text">
                <strong>{{ item.name }}</strong>
                <small>{{ item.owner_name || '—' }} · {{ sizeText(item.size) }} · {{ item.chunks || 0 }} 片段 · {{ timeText(item.updated_at) }}</small>
              </span>
              <em class="kb-status" :class="statusClass(item.status)">{{ statusText(item.status) }}</em>
            </button>
            <div v-if="!recycle" class="kb-row-actions">
              <button v-if="item.status === 'ready'" type="button" class="icon-button" title="下载原文件" aria-label="下载原文件" :disabled="actionBusy" @click="downloadActive(item)"><Download :size="16" /></button>
              <button v-if="item.can_edit" type="button" class="icon-button" title="替换文件" aria-label="替换文件" :disabled="actionBusy" @click="pickReplace(item)"><RefreshCw :size="16" /></button>
              <button v-if="item.status === 'failed' && item.can_edit" type="button" class="icon-button" title="重新索引" aria-label="重新索引" :disabled="actionBusy" @click="retryTarget = item"><RotateCcw :size="16" /></button>
              <button v-if="item.can_edit" type="button" class="icon-button danger" title="移入回收站" aria-label="移入回收站" :disabled="actionBusy" @click="deleteTarget = item"><Trash2 :size="16" /></button>
            </div>
            <div v-else class="kb-row-actions">
              <button v-if="item.can_edit" type="button" class="icon-button" title="恢复" aria-label="恢复" :disabled="actionBusy" @click="restoreTarget = item"><RotateCcw :size="16" /></button>
            </div>
            <p v-if="item.error && !recycle" class="kb-row-error" role="alert">{{ item.error }}</p>
          </li>
        </ul>
        <footer v-if="pages > 1" class="kb-pager">
          <span>共 {{ total }} 项</span>
          <button class="icon-button" aria-label="上一页" :disabled="page <= 1 || loadingList" @click="changePage(page - 1)"><ChevronLeft :size="16" /></button>
          <span>{{ page }} / {{ pages }}</span>
          <button class="icon-button" aria-label="下一页" :disabled="page >= pages || loadingList" @click="changePage(page + 1)"><ChevronRight :size="16" /></button>
        </footer>
      </div>

      <aside v-if="detail" class="kb-detail">
        <header class="kb-detail-head">
          <div>
            <strong>{{ detail.name }}</strong>
            <small>{{ detail.owner_name || '—' }} · 预览版本 {{ detail.view_version || detail.active_version || detail.version }}{{ detail.historical ? '（历史版本）' : '' }}</small>
          </div>
          <button type="button" class="icon-button" aria-label="关闭预览" @click="closeDetail"><X :size="17" /></button>
        </header>
        <div v-if="detailLoading" class="empty"><Loader2 :size="20" class="spin" aria-hidden="true" />正在读取原文…</div>
        <template v-else>
          <div class="kb-detail-actions">
            <button type="button" class="primary" @click="downloadActive(detail)"><Download :size="15" />下载原文件</button>
            <span class="muted">共享版本 {{ detail.active_version || '—' }} · 状态 {{ statusText(detail.status) }}</span>
          </div>
          <p v-if="detail.status !== 'ready' && sections.length" class="kb-stale" role="status"><AlertCircle :size="14" aria-hidden="true" /><span>正在预览当前版本{{ detail.view_version ? `（${detail.view_version}）` : '' }}，新版本{{ detail.status === 'failed' ? '索引失败，可重试' : '仍在索引中' }}。</span></p>
          <div class="kb-sections">
            <article v-for="section in sections" :key="section.ordinal" class="kb-section" :class="{ current: focusedOrdinal === section.ordinal }">
              <div class="kb-section-meta"><span>片段 {{ section.ordinal }}</span><em>{{ section.location || '—' }}</em></div>
              <p>{{ section.text }}</p>
            </article>
            <div v-if="!sections.length" class="empty">{{ previewEmptyText }}</div>
          </div>
          <footer v-if="detailPages > 1" class="kb-pager">
            <button class="icon-button" aria-label="上一页片段" :disabled="detailPage <= 1 || detailLoading" @click="detailPage--; reloadDetail()"><ChevronLeft :size="16" /></button>
            <span>{{ detailPage }} / {{ detailPages }}</span>
            <button class="icon-button" aria-label="下一页片段" :disabled="detailPage >= detailPages || detailLoading" @click="detailPage++; reloadDetail()"><ChevronRight :size="16" /></button>
          </footer>
        </template>
      </aside>
    </div>

    <UiTransition name="ui-overlay" appear>
      <div v-if="settingsOpen" class="kb-backdrop" @click.self="settingsOpen = false">
        <form class="kb-settings" role="dialog" aria-modal="true" aria-labelledby="kb-settings-title" @submit.prevent="saveSettings">
          <header>
            <div><span>管理员设置</span><strong id="kb-settings-title">嵌入配置</strong></div>
            <button type="button" class="icon-button" aria-label="关闭设置" :disabled="settingsSaving" @click="settingsOpen = false"><X :size="18" /></button>
          </header>
          <div v-if="settingsError" class="alert error" role="alert"><AlertCircle :size="16" aria-hidden="true" /><span>{{ settingsError }}</span></div>
          <div class="kb-settings-state">
            <span :class="settingsDraft.configured ? 'ok' : 'warn'">{{ settingsDraft.configured ? '已配置' : '未配置（未建立向量索引，暂无可用文档）' }}</span>
            <span v-if="settingsDraft.dimensions">{{ settingsDraft.dimensions }} 维</span>
          </div>
          <label>嵌入服务端点（Embeddings）
            <input v-model="settingsDraft.endpoint" type="url" placeholder="https://…/embeddings" autocomplete="off" spellcheck="false" :disabled="settingsSaving" />
          </label>
          <label>模型
            <input v-model="settingsDraft.model" type="text" placeholder="模型名称" autocomplete="off" spellcheck="false" :disabled="settingsSaving" />
          </label>
          <label>API Key
            <input v-model="settingsDraft.api_key" type="password" autocomplete="new-password" placeholder="留空保持不变" spellcheck="false" :disabled="settingsSaving" @copy.prevent @cut.prevent @contextmenu.prevent />
          </label>
          <label>允许接收文档片段的目标端（每行一个 https 来源）
            <textarea v-model="settingsDraft.originsText" rows="5" placeholder="https://chat.example.com" spellcheck="false" :disabled="settingsSaving" />
          </label>
          <p class="kb-settings-tip">将文档片段作为上下文发送给上述白名单端点；配置保存并完成索引后，员工才可检索这些文档。</p>
          <footer>
            <span class="muted">{{ settingsSaving ? '正在保存…' : '' }}</span>
            <button type="button" :disabled="settingsSaving" @click="settingsOpen = false">取消</button>
            <button type="submit" :disabled="settingsSaving"><Loader2 v-if="settingsSaving" :size="15" class="spin" aria-hidden="true" />保存</button>
          </footer>
        </form>
      </div>
    </UiTransition>

    <input ref="replaceInput" type="file" hidden @change="onReplacePick" />
    <ConfirmDialog :open="!!uploadConfirm" title="确认上传" :message="uploadConfirmMessage" tone="warning" @resolve="resolveUpload" />
    <ConfirmDialog :open="!!deleteTarget" title="移入回收站" :message="deleteTarget ? '将「' + deleteTarget.name + '」移入回收站？历史共享版本保留，之后可恢复。' : ''" tone="danger" @resolve="resolveDelete" />
    <ConfirmDialog :open="!!restoreTarget" title="恢复文档" :message="restoreTarget ? '恢复「' + restoreTarget.name + '」？恢复后需要重新索引才会再次共享。' : ''" tone="primary" @resolve="resolveRestore" />
    <ConfirmDialog :open="!!retryTarget" title="重新索引" :message="retryTarget ? '对「' + retryTarget.name + '」重新执行索引？' : ''" tone="primary" @resolve="resolveRetry" />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import {
  AlertCircle, CheckCircle2, ChevronLeft, ChevronRight, Database, Download, FileText,
  Loader2, RefreshCw, RotateCcw, Search, Settings, Trash2, Upload, X,
} from "lucide-vue-next";
import { requestJson, downloadFile, type Dict } from "../api/client";
import ConfirmDialog from "./ConfirmDialog.vue";
import UiTransition from "./UiTransition.vue";

const BASE = "/api/assistant/knowledge";

interface DocumentItem {
  id: string;
  name: string;
  owner_name: string;
  can_edit: boolean;
  version: string | number;
  active_version?: string | number | null;
  view_version?: string | number | null;
  historical?: boolean;
  status: string;
  updated_at: unknown;
  size: number;
  chunks: number;
  error?: string;
}

interface Section {
  text: string;
  location: string;
  ordinal: number;
}

const search = ref("");
const page = ref(1);
const total = ref(0);
const recycle = ref(false);
const items = ref<DocumentItem[]>([]);
const isAdmin = ref(false);
const settingsMeta = ref<Dict | null>(null);
const loadingList = ref(false);
const quietLoading = ref(false);
const error = ref("");
const notice = ref("");

const detail = ref<DocumentItem | null>(null);
const sections = ref<Section[]>([]);
const detailLoading = ref(false);
const detailPage = ref(1);
const detailTotal = ref(0);
const detailPageSize = ref(20);
const focusedOrdinal = ref<number | null>(null);
let detailRequest = 0;
const detailViewVersion = ref<string | number>("");

const previewEmptyText = computed(() => {
  const current = detail.value;
  if (!current) return "";
  if (current.status !== "ready") {
    return current.error || "文档尚未完成索引，暂无片段可预览。";
  }
  return current.historical ? "该版本尚无片段可预览。" : "没有可预览的片段";
});

const uploadPanel = ref(false);
const uploadTarget = ref<DocumentItem | null>(null);
const pendingFiles = ref<File[]>([]);
const uploadBusy = ref(false);
const uploadError = ref("");
const uploadConfirm = ref(false);

const deleteTarget = ref<DocumentItem | null>(null);
const restoreTarget = ref<DocumentItem | null>(null);
const retryTarget = ref<DocumentItem | null>(null);
const actionBusy = ref(false);

const settingsOpen = ref(false);
const settingsLoading = ref(false);
const settingsSaving = ref(false);
const settingsError = ref("");
const settingsDraft = reactive({
  endpoint: "",
  model: "",
  api_key: "",
  originsText: "",
  configured: false,
  dimensions: 0,
});
let settingsController: AbortController | undefined;

const replaceInput = ref<HTMLInputElement | null>(null);
const pages = computed(() => Math.max(1, Math.ceil(total.value / 20)));
const detailPages = computed(() => Math.max(1, Math.ceil(detailTotal.value / (detailPageSize.value || 20))));
const uploadConfirmMessage = computed(() => {
  if (uploadTarget.value) return "上传后将替换当前文档并建立新索引版本，原共享版本保留。确认继续？";
  const count = pendingFiles.value.length;
  const names = pendingFiles.value.slice(0, 3).map(f => f.name).join("、");
  return `将上传 ${count} 个文件（${names}${count > 3 ? "…" : ""}）至共享知识库。上传并完成索引后所有员工均可检索，请确认这些文件不包含私密聊天内容。`;
});

let searchTimer = 0;
let listController: AbortController | undefined;
let pollTimer: number | undefined;

function statusText(status: string): string {
  const map: Record<string, string> = {
    queued: "排队中", indexing: "索引中", ready: "就绪", failed: "失败", blocked: "已屏蔽", deleted: "已删除",
  };
  return map[status] || status || "—";
}
function statusClass(status: string): string {
  const key = String(status || "").toLowerCase();
  if (["queued", "indexing"].includes(key)) return key;
  if (key === "ready") return "ready";
  if (key === "failed") return "failed";
  if (key === "blocked") return "blocked";
  return "deleted";
}
function sizeText(value: unknown): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / 1024 / 1024).toFixed(1) + " MB";
}
function timeText(value: unknown): string {
  if (!value) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  const ms = n < 1e12 ? n * 1000 : n;
  return new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(ms));
}

function clearNotice(): void { notice.value = ""; }

function updatePolling(): void {
  const hasPending = items.value.some(item => ["queued", "indexing"].includes(item.status));
  if (hasPending && pollTimer === undefined) {
    pollTimer = window.setInterval(() => {
      if (document.hidden) return;
      void loadList(false, false);
    }, 8000);
  } else if (!hasPending && pollTimer !== undefined) {
    window.clearInterval(pollTimer);
    pollTimer = undefined;
  }
}

async function loadList(spinner = true, clearError = true): Promise<void> {
  listController?.abort();
  const controller = new AbortController();
  listController = controller;
  if (spinner) { loadingList.value = true; quietLoading.value = false; }
  else { quietLoading.value = true; }
  if (clearError) error.value = "";
  try {
    const params = new URLSearchParams({
      page: String(page.value),
      deleted: recycle.value ? "1" : "0",
    });
    if (search.value.trim()) params.set("q", search.value.trim());
    const data = await requestJson(`${BASE}?${params}`, { signal: controller.signal, cache: "no-store" });
    if (listController !== controller || controller.signal.aborted) return;
    items.value = data.items || [];
    total.value = Number(data.total) || 0;
    page.value = Number(data.page) || page.value;
    isAdmin.value = Boolean(data.is_admin);
    settingsMeta.value = data.settings || null;
    updatePolling();
  } catch (cause) {
    if (listController === controller && !controller.signal.aborted) {
      if (clearError) error.value = cause instanceof Error ? cause.message : "文档读取失败";
    }
  } finally {
    if (listController === controller) { loadingList.value = false; quietLoading.value = false; }
  }
}

function onSearchChange(): void {
  window.clearTimeout(searchTimer);
  listController?.abort();
  searchTimer = window.setTimeout(() => {
    if (page.value !== 1) page.value = 1;
    void loadList();
  }, 450);
}
watch(search, onSearchChange);

function switchBin(next: boolean): void {
  if (recycle.value === next) return;
  recycle.value = next;
  page.value = 1;
  void loadList();
}
function refresh(): void { void loadList(true, true); }
function changePage(next: number): void {
  page.value = next;
  void loadList(true, true);
}

function openDetail(item: DocumentItem): void {
  if (detailLoading.value) return;
  focusedOrdinal.value = null;
  // Active preview always shows the current shared (active) version, even while a replacement indexes.
  detailViewVersion.value = item.active_version != null && item.active_version !== "" ? item.active_version : "";
  void loadDetail(item, undefined, 1, "", detailViewVersion.value);
}

async function loadDetail(item: DocumentItem, chunk: number | undefined, targetPage = 1, preserveError = "", viewVersion: string | number = ""): Promise<void> {
  const id = String(item.id);
  detailRequest += 1;
  const requestSeq = detailRequest;
  detailLoading.value = true;
  if (preserveError) error.value = preserveError;
  try {
    const params = new URLSearchParams({ page: String(targetPage) });
    // Revision (item.version) is only valid for mutations. For preview, use the version being viewed.
    if (viewVersion != null && viewVersion !== "") params.set("version", String(viewVersion));
    // chunk is only sent on an explicit citation jump; plain pagination must not send chunk.
    if (chunk != null) params.set("chunk", String(chunk));
    const data = await requestJson(`${BASE}/documents/${encodeURIComponent(id)}?${params}`, { cache: "no-store" });
    if (requestSeq !== detailRequest) return;
    detail.value = (data.document || { ...item }) as DocumentItem;
    sections.value = data.sections || [];
    detailTotal.value = Number(data.total) || sections.value.length;
    detailPage.value = Number(data.page) || targetPage;
    detailPageSize.value = Number(data.page_size) || 20;
    focusedOrdinal.value = chunk ?? focusedOrdinal.value;
    if (data.document && (data.document.view_version != null)) {
      detailViewVersion.value = data.document.view_version;
    }
  } catch (cause) {
    if (requestSeq === detailRequest) error.value = cause instanceof Error ? cause.message : "原文读取失败";
  } finally {
    if (requestSeq === detailRequest) detailLoading.value = false;
  }
}

async function reloadDetail(): Promise<void> {
  if (!detail.value) return;
  // Pagination keeps the same viewed version; chunk is intentionally omitted (no citation jump).
  await loadDetail(detail.value, undefined, detailPage.value, error.value, detailViewVersion.value);
}

function closeDetail(): void {
  detailRequest += 1;
  detail.value = null;
  sections.value = [];
  detailViewVersion.value = "";
  focusedOrdinal.value = null;
  detailPage.value = 1;
}

async function downloadActive(item: DocumentItem): Promise<void> {
  if (actionBusy.value) return;
  // Download matches the version being previewed (active for list actions, view_version for citation preview).
  const version = item.view_version || item.active_version || item.version;
  actionBusy.value = true;
  clearNotice();
  try {
    await downloadFile(`${BASE}/documents/${encodeURIComponent(item.id)}/file?version=${encodeURIComponent(String(version))}`);
    notice.value = "已开始下载原文件。";
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "文件下载失败";
  } finally {
    actionBusy.value = false;
  }
}

function openUpload(): void {
  uploadTarget.value = null;
  pendingFiles.value = [];
  uploadError.value = "";
  uploadPanel.value = true;
}

const MAX_SINGLE = 20 * 1024 * 1024;
const MAX_TOTAL = 100 * 1024 * 1024;

function onUploadPick(event: Event): void {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = "";
  if (!files.length) return;
  const all = uploadTarget.value ? [files[0]] : files;
  if (all.some(file => !file.size || file.size > MAX_SINGLE)) { uploadError.value = "单个文件须非空且不超过20MiB"; return; }
  if (all.reduce((sum, file) => sum + file.size, 0) > MAX_TOTAL) { uploadError.value = "一次上传合计不能超过100MiB"; return; }
  if (!uploadTarget.value && all.length > 10) { uploadError.value = "一次最多上传10个文件"; return; }
  pendingFiles.value = all;
  uploadError.value = "";
}

function pickReplace(item: DocumentItem): void {
  uploadTarget.value = item;
  pendingFiles.value = [];
  uploadError.value = "";
  replaceInput.value?.click();
}

function onReplacePick(event: Event): void {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  input.value = "";
  if (!file) return;
  if (!file.size || file.size > MAX_SINGLE) { uploadError.value = "文件须非空且不超过20MiB"; if (uploadPanel.value) uploadPanel.value = false; return; }
  uploadTarget.value = uploadTarget.value || null;
  pendingFiles.value = [file];
  uploadError.value = "";
  uploadPanel.value = true;
}

function requestUpload(): void {
  if (!pendingFiles.value.length || uploadBusy.value) return;
  uploadConfirm.value = true;
}

async function doUpload(): Promise<void> {
  if (!pendingFiles.value.length || uploadBusy.value) return;
  const target = uploadTarget.value;
  const submitted = [...pendingFiles.value];
  uploadBusy.value = true;
  clearNotice();
  try {
    const form = new FormData();
    for (const file of submitted) form.append("files", file);
    let url = `${BASE}/files`;
    if (target) {
      const params = new URLSearchParams({ document_id: target.id, version: String(target.version) });
      url += `?${params}`;
    }
    const data = await requestJson(url, { method: "POST", body: form, timeoutMs: 180000 });
    // Partial upload responses carry {items, errors}. Preserve any failed files for retry.
    const errors = Array.isArray(data?.errors) ? data.errors : [];
    const failedNames = new Set<string>();
    let mappedErrors = false;
    for (const err of errors) {
      if (err && typeof err === "object") {
        const name = String(err.name || err.filename || err.file || "");
        if (name) { failedNames.add(name); mappedErrors = true; }
      }
    }
    let keptFiles = submitted.filter(file => failedNames.has(file.name));
    // If errors exist but carry no machine-readable file name, preserve all submitted files so none are lost.
    if (errors.length && !mappedErrors) keptFiles = [...submitted];
    pendingFiles.value = keptFiles;
    const succeeded = submitted.length - keptFiles.length;
    if (errors.length) {
      const messages = errors.map(err => err?.error || err?.message || "上传失败").filter(Boolean);
      uploadError.value = messages.length ? messages.join("；") : "部分文件上传失败，请检查后重试。";
    } else {
      uploadError.value = "";
    }
    if (succeeded > 0) {
      if (keptFiles.length === 0) {
        uploadPanel.value = false;
        uploadTarget.value = null;
      }
      notice.value = target
        ? "文件已上传，将建立新索引版本。"
        : `已上传 ${succeeded} 个文件，完成索引后将进入共享知识库。`;
      await loadList(true, true);
    } else {
      // All files failed: keep the upload panel open with the file list preserved for retry.
      uploadPanel.value = true;
    }
  } catch (cause) {
    uploadError.value = cause instanceof Error ? cause.message : "上传失败，请重试。";
  } finally {
    uploadBusy.value = false;
    uploadConfirm.value = false;
  }
}

async function resolveUpload(accepted: boolean): Promise<void> {
  uploadConfirm.value = false;
  if (!accepted) return;
  await doUpload();
}

async function resolveDelete(accepted: boolean): Promise<void> {
  const item = deleteTarget.value;
  deleteTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}`, { method: "DELETE", body: JSON.stringify({ version: item.version }) });
    if (detail.value?.id === item.id) closeDetail();
    notice.value = "已移入回收站。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "删除失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

async function resolveRestore(accepted: boolean): Promise<void> {
  const item = restoreTarget.value;
  restoreTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}/restore`, { method: "POST", body: JSON.stringify({ version: item.version }) });
    notice.value = "已恢复文档。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "恢复失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

async function resolveRetry(accepted: boolean): Promise<void> {
  const item = retryTarget.value;
  retryTarget.value = null;
  if (!accepted || !item || actionBusy.value) return;
  actionBusy.value = true;
  clearNotice();
  try {
    await requestJson(`${BASE}/documents/${encodeURIComponent(item.id)}/retry`, { method: "POST", body: JSON.stringify({ version: item.version }) });
    notice.value = "已重新提交索引。";
    await loadList(true, false);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : "操作失败，请重试。";
  } finally {
    actionBusy.value = false;
  }
}

async function openSettings(): Promise<void> {
  settingsOpen.value = true;
  settingsError.value = "";
  settingsLoading.value = true;
  settingsController?.abort();
  const controller = new AbortController();
  settingsController = controller;
  try {
    const data = await requestJson(`${BASE}/settings`, { signal: controller.signal, cache: "no-store" });
    if (settingsController !== controller || !settingsOpen.value) return;
    settingsDraft.endpoint = data.endpoint || "";
    settingsDraft.model = data.model || "";
    settingsDraft.api_key = "";
    settingsDraft.originsText = Array.isArray(data.approved_origins) ? data.approved_origins.join("\n") : "";
    settingsDraft.configured = Boolean(data.configured);
    settingsDraft.dimensions = Number(data.dimensions) || 0;
  } catch (cause) {
    if (settingsController === controller) settingsError.value = cause instanceof Error ? cause.message : "设置读取失败";
  } finally {
    if (settingsController === controller) settingsLoading.value = false;
  }
}

async function saveSettings(): Promise<void> {
  if (settingsSaving.value || settingsLoading.value) return;
  settingsSaving.value = true;
  settingsError.value = "";
  const safeEndpoint = settingsDraft.endpoint.trim();
  if (safeEndpoint && !/^https:\/\/\S+$/i.test(safeEndpoint)) {
    settingsError.value = "端点需为完整的 HTTPS 地址";
    settingsSaving.value = false;
    return;
  }
  const origins = settingsDraft.originsText.split("\n").map(line => line.trim()).filter(Boolean);
  const invalid = origins.find(origin => !/^https:\/\/[^\s/]+$/i.test(origin));
  if (invalid) { settingsError.value = `来源须为 https 地址：${invalid}`; settingsSaving.value = false; return; }
  try {
    const payload = {
      endpoint: safeEndpoint,
      model: settingsDraft.model.trim(),
      api_key: settingsDraft.api_key,
      approved_origins: origins,
    };
    await requestJson(`${BASE}/settings`, { method: "PUT", body: JSON.stringify(payload), timeoutMs: 30000 });
    settingsOpen.value = false;
    notice.value = "知识库设置已保存。";
    await loadList(true, true);
  } catch (cause) {
    settingsError.value = cause instanceof Error ? cause.message : "保存失败，请重试。";
  } finally {
    settingsSaving.value = false;
    settingsController = undefined;
  }
}

onMounted(() => {
  void loadList(true, true);
  const params = new URLSearchParams(window.location.search);
  const docId = params.get("document");
  const version = params.get("version");
  const chunk = params.get("chunk");
  if (docId) {
    const item: DocumentItem = {
      id: docId,
      name: params.get("name") || "文档",
      owner_name: "",
      can_edit: false,
      version: version || "",
      status: "ready",
      updated_at: null,
      size: 0,
      chunks: 0,
    };
    focusedOrdinal.value = chunk !== null && Number.isInteger(Number(chunk)) ? Math.max(0, Number(chunk)) : null;
    const citationChunk = focusedOrdinal.value ?? undefined;
    detailViewVersion.value = version || "";
    void loadDetail(item, citationChunk, 1, "", version || "");
  }
});

onBeforeUnmount(() => {
  window.clearTimeout(searchTimer);
  window.clearInterval(pollTimer);
  pollTimer = undefined;
  listController?.abort();
  settingsController?.abort();
  detailRequest += 1;
});
</script>

<style scoped>
.kb-page {
  display: grid;
  gap: 12px;
  width: min(1200px, 100%);
  margin: 0 auto;
  padding: 20px 22px 40px;
  box-sizing: border-box;
  color: var(--cf-text, #0f172a);
}

.kb-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.kb-title { display: flex; align-items: center; gap: 10px; min-width: 0; }
.kb-title h1 { margin: 0; font-size: 22px; line-height: 1.2; color: #0c2d63; }
.kb-title > svg { color: var(--cf-brand-blue, #1e63ff); }
.kb-badge { display: inline-flex; align-items: center; min-height: 22px; padding: 0 9px; border-radius: 999px; font-size: 12px; font-weight: 850; }
.kb-badge.ok { color: #047857; background: #ecfdf5; border: 1px solid #a7f3d0; }
.kb-badge.warn { color: #92400e; background: #fffbeb; border: 1px solid #fde68a; }
.kb-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }

.kb-toolbar {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px;
  border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-panel, 22px);
  background: rgba(255, 255, 255, 0.9);
  box-shadow: var(--cf-shadow-card, 0 12px 30px rgba(0, 47, 135, 0.08));
}
.kb-search {
  flex: 1 1 240px;
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 8px;
  border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-control, 16px);
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.96);
}
.kb-search > svg { flex: 0 0 auto; color: var(--cf-muted, #64748b); }
.kb-search input { flex: 1 1 auto; min-width: 0; border: 0; outline: 0; background: transparent; color: var(--cf-text, #0f172a); font: inherit; }
.kb-search .spin { flex: 0 0 auto; color: var(--cf-brand-blue, #1e63ff); }
.kb-toggle { display: flex; gap: 4px; padding: 4px; border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px; background: #f4f8ff; }
.kb-toggle button {
  display: inline-flex; align-items: center; gap: 5px; min-height: 30px; padding: 0 12px;
  border: 0; border-radius: 999px; background: transparent; color: #48627f; font: inherit; font-size: 13px; font-weight: 800; cursor: pointer;
}
.kb-toggle button.active { background: #fff; color: var(--cf-brand-blue, #1e63ff); box-shadow: 0 3px 10px rgba(30, 99, 255, 0.14); }

.alert {
  display: flex; align-items: center; gap: 8px; min-width: 0; padding: 9px 12px;
  border-radius: 14px; font-size: 13px; line-height: 1.4;
}
.alert > span { min-width: 0; overflow-wrap: anywhere; flex: 1 1 auto; }
.alert.error { border: 1px solid #fecaca; background: #fef2f2; color: var(--cf-danger, #991b1b); }
.alert.success { border: 1px solid #a7f3d0; background: #ecfdf5; color: var(--cf-success, #047857); }
.alert .icon-button { flex: 0 0 auto; }

.kb-upload {
  display: grid; gap: 10px; padding: 14px; border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-card, 18px); background: rgba(248, 251, 255, 0.86);
}
.kb-upload-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.kb-upload-head strong { color: #0c2d63; font-size: 15px; }
.kb-upload-pick {
  display: inline-flex; align-items: center; gap: 7px; width: fit-content; min-height: 34px;
  padding: 7px 12px; border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px;
  background: #fff; color: var(--cf-brand-blue, #1e63ff); font-size: 13px; font-weight: 800; cursor: pointer;
}
.kb-upload-pick input { display: none; }
.kb-upload-list { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
.kb-upload-list li { display: flex; align-items: center; gap: 8px; min-width: 0; font-size: 13px; }
.kb-upload-list li span { flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.kb-upload-list li em { flex: 0 0 auto; color: var(--cf-muted, #64748b); font-style: normal; }
.kb-upload-replace { margin: 0; color: var(--cf-warning, #92400e); font-size: 12px; overflow-wrap: anywhere; }
.kb-upload-actions { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }

.kb-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 14px;
  min-width: 0;
}
@media (min-width: 920px) {
  .kb-layout.has-detail { grid-template-columns: minmax(0, 1.05fr) minmax(0, 1fr); align-items: start; }
}
.kb-pane {
  min-width: 0; padding: 10px 0; border-top: 1px solid var(--cf-border, #d8e5f7);
}
.kb-list { display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }
.kb-row {
  display: grid; gap: 4px; padding: 8px; border: 1px solid transparent; border-radius: 8px;
}
.kb-row.selected { border-color: var(--cf-border-strong, #cfe0ff); background: #f2f7ff; }
.kb-row-main {
  display: flex; align-items: center; gap: 10px; width: 100%; min-width: 0; text-align: left;
  border: 0; background: transparent; color: inherit; font: inherit; cursor: pointer; padding: 4px 2px;
}
.kb-file-icon { flex: 0 0 auto; display: inline-grid; place-items: center; width: 34px; height: 34px; border-radius: 12px; background: #eef4ff; color: var(--cf-brand-blue, #1e63ff); }
.kb-row-text { flex: 1 1 auto; min-width: 0; }
.kb-row-text strong { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 14px; }
.kb-row-text small { display: block; margin-top: 2px; color: var(--cf-muted, #64748b); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.kb-status { flex: 0 0 auto; display: inline-flex; align-items: center; min-height: 20px; padding: 0 8px; border-radius: 999px; font-size: 11px; font-style: normal; font-weight: 850; }
.kb-status.ready { color: #047857; background: #ecfdf5; }
.kb-status.queued, .kb-status.indexing { color: #1e40af; background: #e0ecff; }
.kb-status.failed { color: #991b1b; background: #fef2f2; }
.kb-status.blocked { color: #92400e; background: #fffbeb; }
.kb-status.deleted { color: #64748b; background: #f1f5f9; }
.kb-row-actions { display: flex; align-items: center; gap: 4px; padding-left: 44px; flex-wrap: wrap; }
.kb-row-error { margin: 0; padding-left: 44px; color: var(--cf-danger, #991b1b); font-size: 12px; overflow-wrap: anywhere; }

.icon-button {
  display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; flex: 0 0 auto;
  border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px; background: #fff; color: #33526f; cursor: pointer; padding: 0;
}
.icon-button:hover:not(:disabled) { border-color: var(--cf-border-strong, #cfe0ff); background: #f4f8ff; }
.icon-button.danger { color: var(--cf-danger, #991b1b); }
.icon-button:disabled { opacity: 0.5; cursor: not-allowed; }

.primary {
  display: inline-flex; align-items: center; gap: 6px; min-height: 36px; padding: 0 14px;
  border: 0; border-radius: var(--cf-radius-control, 16px); background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff; font: inherit; font-size: 13px; font-weight: 900; cursor: pointer;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.24);
}
.primary:hover:not(:disabled) { filter: brightness(1.04); }
.primary:disabled { opacity: 0.55; cursor: not-allowed; }

.kb-pager { display: flex; align-items: center; justify-content: flex-end; gap: 6px; padding: 10px 4px 2px; color: var(--cf-muted, #64748b); font-size: 12px; }

.kb-detail {
  min-width: 0; padding: 12px; border: 1px solid var(--cf-border, #d8e5f7);
  border-radius: var(--cf-radius-panel, 22px); background: rgba(248, 251, 255, 0.9);
  box-shadow: var(--cf-shadow-card, 0 12px 30px rgba(0, 47, 135, 0.08));
  position: sticky; top: 14px;
}
.kb-detail-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; padding: 2px 2px 10px; border-bottom: 1px solid #e5edf8; }
.kb-detail-head > div { min-width: 0; }
.kb-detail-head strong { display: block; overflow-wrap: anywhere; color: #0c2d63; font-size: 15px; }
.kb-detail-head small { display: block; margin-top: 3px; color: var(--cf-muted, #64748b); font-size: 12px; overflow-wrap: anywhere; }
.kb-detail-actions { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 10px 2px; flex-wrap: wrap; }
.kb-stale { display: flex; align-items: flex-start; gap: 6px; margin: 0 0 8px; padding: 8px 10px; border: 1px solid #fde68a; border-radius: 10px; background: #fffbeb; color: var(--cf-warning, #92400e); font-size: 12px; line-height: 1.5; overflow-wrap: anywhere; }
.kb-stale svg { flex: 0 0 auto; margin-top: 1px; }
.kb-sections { display: grid; gap: 8px; max-height: 60vh; overflow: auto; padding-right: 2px; }
.kb-section { padding: 10px; border-bottom: 1px solid var(--cf-border, #d8e5f7); background: #fff; }
.kb-section.current { border-color: #a8c7ff; box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.1); }
.kb-section-meta { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 6px; font-size: 12px; }
.kb-section-meta span { color: var(--cf-brand-blue, #1e63ff); font-weight: 850; }
.kb-section-meta em { color: var(--cf-muted, #64748b); font-style: normal; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 60%; }
.kb-section p { margin: 0; color: #334155; font-size: 13px; line-height: 1.6; white-space: pre-wrap; overflow-wrap: anywhere; }

.empty { display: grid; place-items: center; gap: 8px; min-height: 120px; padding: 20px; color: var(--cf-muted, #64748b); font-size: 13px; text-align: center; }

.kb-backdrop { position: fixed; inset: 0; z-index: var(--cf-z-modal-backdrop, 800); display: grid; place-items: center; padding: 20px; background: rgba(9, 32, 74, 0.42); backdrop-filter: blur(4px); }
.kb-settings {
  display: flex; flex-direction: column; gap: 12px; width: min(560px, 100%); max-height: calc(100dvh - 40px);
  border: 1px solid #d8e5f7; border-radius: 24px; padding: 18px; overflow: auto; box-sizing: border-box;
  background: linear-gradient(135deg, rgba(248, 251, 255, 0.98), rgba(255, 255, 255, 0.98)), #fff;
  box-shadow: 0 28px 80px rgba(0, 47, 135, 0.24); color: var(--cf-text, #0f172a);
}
.kb-settings header { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
.kb-settings header > div { min-width: 0; }
.kb-settings header span { display: block; color: var(--cf-brand-blue, #1e63ff); font-size: 12px; font-weight: 900; }
.kb-settings header strong { display: block; margin-top: 2px; color: #0c2d63; font-size: 18px; }
.kb-settings-state { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; font-size: 13px; font-weight: 800; }
.kb-settings-state .ok { color: #047857; }
.kb-settings-state .warn { color: #92400e; }
.kb-settings label { display: grid; gap: 5px; color: #475569; font-size: 13px; font-weight: 800; }
.kb-settings input, .kb-settings textarea {
  width: 100%; box-sizing: border-box; border: 1px solid var(--cf-border, #d8e5f7); border-radius: 14px;
  padding: 9px 11px; background: rgba(255, 255, 255, 0.96); color: var(--cf-text, #0f172a); font: inherit;
}
.kb-settings textarea { resize: vertical; min-height: 100px; line-height: 1.5; }
.kb-settings input:focus, .kb-settings textarea:focus { border-color: #155dfc; outline: none; box-shadow: 0 0 0 3px rgba(21, 93, 252, 0.14); }
.kb-settings-tip { margin: 0; color: var(--cf-muted, #64748b); font-size: 12px; line-height: 1.5; overflow-wrap: anywhere; }
.kb-settings footer { display: flex; align-items: center; justify-content: flex-end; gap: 8px; padding-top: 12px; border-top: 1px solid #e5edf8; }
.kb-settings footer button {
  min-height: 34px; padding: 0 13px; border: 1px solid var(--cf-border, #d8e5f7); border-radius: 999px;
  background: #fff; color: #33526f; font: inherit; font-size: 13px; font-weight: 850; cursor: pointer;
}
.kb-settings footer button[type="submit"] {
  border-color: transparent; background: linear-gradient(135deg, #1e63ff, #1554df); color: #fff;
}
.kb-settings footer button:disabled { opacity: 0.55; cursor: not-allowed; }

.muted { color: var(--cf-muted, #64748b); font-size: 12px; }
.spin { animation: kb-spin 1s linear infinite; }
@keyframes kb-spin { to { transform: rotate(360deg); } }

@media (max-width: 720px) {
  .kb-page { padding: 14px 12px 32px; }
  .kb-detail { position: static; }
}
</style>
