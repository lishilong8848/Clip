<template>
  <section ref="pageElement" class="link-directory-page" :aria-busy="loading" @pointermove="pointerMove" @pointerup="pointerDrop" @pointercancel="endDrag" @lostpointercapture="endDrag">
    <VnetBackButton to="/" title="返回首页" />

    <header class="page-head">
      <div class="heading">
        <h1>多维表导航</h1>
        <span v-if="!loading" class="count">共 {{ items.length }} 条</span>
      </div>
      <div class="actions">
        <button
          type="button"
          class="icon-button"
          :disabled="loading || saving || !!error"
          title="刷新链接列表"
          aria-label="刷新链接列表"
          @click="reload"
        >
          <RefreshCw :size="17" :class="{ spinning: loading }" aria-hidden="true" />
        </button>
      </div>
    </header>

    <div class="toolbar">
      <label class="search-field">
        <Search :size="16" aria-hidden="true" />
        <input v-model.trim="search" type="search" placeholder="搜索名称、用途或分类" aria-label="按名称、用途或分类搜索" />
      </label>
      <label class="category-field">
        <span class="sr-only">按分类筛选</span>
        <select v-model="categoryFilter" aria-label="按分类筛选">
          <option value="">全部分类</option>
          <option v-for="category in categories" :key="category" :value="category">{{ category }}</option>
        </select>
      </label>
      <div class="segmented" role="group" aria-label="按类型筛选">
        <button
          type="button"
          :class="{ active: kindFilter === '' }"
          :aria-pressed="kindFilter === ''"
          @click="kindFilter = ''"
        >全部</button>
        <button
          type="button"
          :class="{ active: kindFilter === 'bitable' }"
          :aria-pressed="kindFilter === 'bitable'"
          @click="kindFilter = 'bitable'"
        >多维表</button>
        <button
          type="button"
          :class="{ active: kindFilter === 'webpage' }"
          :aria-pressed="kindFilter === 'webpage'"
          @click="kindFilter = 'webpage'"
        >网页导航</button>
      </div>
      <button
        v-if="canEdit"
        type="button"
        class="primary add-button"
        :disabled="loading || saving"
        @click="openCreate"
      >
        <Plus :size="16" aria-hidden="true" />
        新增链接
      </button>
    </div>

    <div v-if="error" class="state-block error" role="alert">
      <AlertCircle :size="18" aria-hidden="true" />
      <span>{{ error }}</span>
      <button type="button" :disabled="loading" @click="reload">
        <RefreshCw :size="15" aria-hidden="true" />重试
      </button>
    </div>
    <div v-else-if="loading && !items.length" class="state-block" role="status">
      <LoadingIndicator>正在读取链接列表...</LoadingIndicator>
    </div>
    <div v-if="staleWarning" class="state-block warn" role="status">
      <AlertCircle :size="18" aria-hidden="true" />
      <span>{{ staleWarning }}</span>
    </div>
    <div v-if="orderMessage || reordering" class="order-status" :class="{ failed: orderFailed }" role="status">
      <LoadingIndicator v-if="reordering">正在保存排序...</LoadingIndicator>
      <span v-else>{{ orderMessage }}</span>
    </div>
    <div v-if="!items.length && !error && !loading" class="state-block">
      {{ canEdit ? '暂无链接，可点击「新增链接」开始维护导航目录。' : '暂无链接。' }}
    </div>
    <div v-else-if="items.length && !filteredItems.length" class="state-block">
      没有匹配的链接
    </div>

    <div v-if="filteredItems.length" class="table-wrap">
      <table class="link-table">
        <thead>
          <tr>
            <th>名称</th>
            <th>分类</th>
            <th>用途</th>
            <th class="sort-col">排序</th>
            <th v-if="canEdit" class="ops-col"><span class="sr-only">操作</span></th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in pageItems"
            :key="String(row.id)"
            :data-link-id="row.id"
            :class="{ untrusted: !isTrustedUrl(row.url), dragging: draggedId === String(row.id), moved: movedId === String(row.id), 'drop-before': dropTarget === String(row.id) && dropPlacement === 'before', 'drop-after': dropTarget === String(row.id) && dropPlacement === 'after' }"
          >
            <td class="name-cell">
              <a
                v-if="isTrustedUrl(row.url)"
                :href="row.url"
                target="_blank"
                rel="noopener noreferrer"
                :title="row.url"
                class="link-name"
              >
                <span class="kind-icon-wrap" :title="linkKindLabel(row.url)">
                  <Table2 v-if="linkKind(row.url) === 'bitable'" :size="14" class="kind-icon bitable-icon" aria-hidden="true" />
                  <Globe v-else :size="14" class="kind-icon globe-icon" aria-hidden="true" />
                </span>
                <span>{{ row.name || '未命名链接' }}</span>
              </a>
              <span v-else :title="row.url || '缺少可信链接地址'">{{ row.name || '未命名链接' }}</span>
            </td>
            <td class="category-cell">{{ row.category || '—' }}</td>
            <td class="purpose-cell">{{ row.purpose || '—' }}</td>
            <td class="sort-cell">{{ Number(row.sort ?? 0) }}</td>
            <td v-if="canEdit" class="ops-cell">
              <div class="row-actions">
                <button
                  type="button"
                  class="icon-button"
                  :disabled="saving || loading"
                  aria-label="编辑链接"
                  title="编辑链接"
                  @click="openEdit(row)"
                >
                  <Pencil :size="16" aria-hidden="true" />
                </button>
                <button
                  type="button"
                  class="icon-button danger"
                  :disabled="saving || loading"
                  aria-label="删除链接"
                  title="删除链接"
                  @click="askDelete(row)"
                >
                  <Trash2 :size="16" aria-hidden="true" />
                </button>
                <button type="button" class="icon-button drag-handle"
                  :disabled="saving || loading"
                  :aria-label="'调整顺序：' + row.name" title="拖动调整顺序；悬停分页按钮可跨页；也可按上下方向键"
                  @pointerdown="startDrag($event, row)" @dragstart.prevent
                  @keydown.up.prevent="moveByKeyboard(row, -1)" @keydown.down.prevent="moveByKeyboard(row, 1)">
                  <GripVertical :size="17" aria-hidden="true" />
                </button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer v-if="filteredItems.length" class="table-footer" :class="{ 'drag-pagination': !!draggedId }">
      <span>每页 {{ PAGE_SIZE }} 条 · 共 {{ filteredItems.length }} 条</span>
      <nav aria-label="链接列表分页">
        <button type="button" data-page-direction="-1" :disabled="page <= 1 || saving" :class="{ 'page-hover': hoverDirection === -1 }" title="拖动时悬停翻到上一页" @click="page -= 1">上一页</button>
        <b>{{ page }} / {{ pageCount }}</b>
        <button type="button" data-page-direction="1" :disabled="page >= pageCount || saving" :class="{ 'page-hover': hoverDirection === 1 }" title="拖动时悬停翻到下一页" @click="page += 1">下一页</button>
      </nav>
    </footer>

    <UiTransition name="ui-overlay" appear>
      <div v-if="modalOpen" class="link-backdrop" @click.self="closeModal">
        <section
          ref="modalElement"
          class="link-modal"
          role="dialog"
          aria-modal="true"
          :aria-labelledby="modalTitleId"
          tabindex="-1"
        >
          <header>
            <h2 :id="modalTitleId">{{ editor.id ? '编辑链接' : '新增链接' }}</h2>
            <button type="button" class="icon-button" aria-label="关闭窗口" title="关闭窗口" :disabled="saving" @click="closeModal">
              <X :size="20" aria-hidden="true" />
            </button>
          </header>
          <div class="modal-scroll" :inert="saving || undefined">
            <div v-if="modalError" class="modal-alert" role="alert">
              <AlertCircle :size="17" aria-hidden="true" />{{ modalError }}
            </div>
            <form id="link-editor-form" class="editor-form" @submit.prevent="saveEntry">
              <label class="field">
                <span>名称 <em>*</em></span>
                <input id="link-name" v-model="editor.name" type="text" maxlength="120" required placeholder="例如：机房巡检台账" />
              </label>
              <label class="field">
                <span>链接 URL <em>*</em></span>
                <input
                  id="link-url"
                  v-model="editor.url"
                  type="text"
                  maxlength="2048"
                  required
                  placeholder="贴入 http/https 网页地址，或 vnet.feishu.cn base/wiki 多维表/知识库链接"
                />
                <small class="field-hint">支持普通 http/https 网页地址；飞书多维表需为 vnet.feishu.cn base/wiki 链接且包含 table 参数。</small>
              </label>
              <div class="form-row">
                <label class="field">
                  <span>分类</span>
                  <input id="link-category" v-model="editor.category" type="text" maxlength="80" placeholder="例如：巡检" />
                </label>
                <label class="field sort-field">
                  <span>排序</span>
                  <input id="link-sort" v-model.number="editor.sort" type="number" min="0" max="1000000" step="1" placeholder="0" />
                </label>
              </div>
              <label class="field">
                <span>用途</span>
                <textarea
                  id="link-purpose"
                  v-model="editor.purpose"
                  rows="4"
                  maxlength="2000"
                  placeholder="选填，说明该链接或多维表的用途"
                ></textarea>
              </label>
            </form>
          </div>
          <footer>
            <span v-if="saving" class="muted"><LoadingIndicator>正在保存...</LoadingIndicator></span>
            <span v-else class="muted">{{ modalDirty ? '有未保存内容' : '' }}</span>
            <div class="actions">
              <button type="button" :disabled="saving" @click="closeModal">取消</button>
              <button
                type="submit"
                class="primary"
                form="link-editor-form"
                :disabled="saving"
                @click.prevent="saveEntry"
              >
                <Save :size="16" aria-hidden="true" v-if="!saving" />
                {{ saving ? '保存中...' : '保存' }}
              </button>
            </div>
          </footer>
        </section>
      </div>
    </UiTransition>

    <Teleport to="body"><div v-if="draggedId" class="link-drag-preview" :style="dragPosition"><GripVertical :size="16" /><span>{{ dragName }}</span></div></Teleport>
    <ConfirmDialog
      :open="confirmOpen"
      tone="danger"
      title="删除导航入口"
      :message="confirmMessage"
      confirm-label="删除入口"
      cancel-label="取消"
      @resolve="resolveDelete"
    />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import {
  AlertCircle,
  Globe,
  GripVertical,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Search,
  Table2,
  Trash2,
  X,
} from "lucide-vue-next";
import { requestJson } from "../api/client";
import { acquireModal } from "../modalState";
import VnetBackButton from "./VnetBackButton.vue";
import LoadingIndicator from "./LoadingIndicator.vue";
import UiTransition from "./UiTransition.vue";
import ConfirmDialog from "./ConfirmDialog.vue";

type LinkDirectoryItem = {
  id: string | number;
  name: string;
  url: string;
  category: string;
  purpose: string;
  sort: number;
};

const PAGE_SIZE = 25;
const WRITE_TIMEOUT = 60_000;
const REFRESH_TIMEOUT = 120_000;
const UUID_V4_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const items = ref<LinkDirectoryItem[]>([]);
const canEdit = ref(false);
const loading = ref(true);
const error = ref("");
const staleWarning = ref("");
const search = ref("");
const categoryFilter = ref("");
const kindFilter = ref<"" | "bitable" | "webpage">("");
const page = ref(1);
const saving = ref(false);
const pageElement=ref<HTMLElement|null>(null),dragName=ref('');
const pointer=reactive({x:0,y:0});
const dragPosition=computed(()=>({left:Math.max(8,Math.min(pointer.x+12,window.innerWidth-320))+'px',top:Math.min(pointer.y+12,window.innerHeight-45)+'px'}));
let dragStart: {id:string;pointerId:number;x:number;y:number}|null=null;
let scrollFrame=0;
const draggedId = ref(""), dropTarget = ref(""), dropPlacement = ref<"before" | "after">("before");
const reordering = ref(false), movedId = ref(""), orderMessage = ref(""), orderFailed = ref(false);
const hoverDirection = ref(0);
let pageHoverTimer: ReturnType<typeof setTimeout> | undefined;
let disposed = false;

let readController: AbortController | undefined;
let listGeneration = 0;

const categories = computed(() => {
  const seen = new Set<string>();
  for (const item of items.value) {
    const value = String(item.category || "").trim();
    if (value) seen.add(value);
  }
  return Array.from(seen).sort((a, b) => a.localeCompare(b, "zh-CN"));
});

const filteredItems = computed(() => {
  const normalized = search.value.trim().toLowerCase();
  const category = categoryFilter.value;
  const kind = kindFilter.value;
  return items.value.filter((item) => {
    if (category && String(item.category || "") !== category) return false;
    if (kind && linkKind(item.url) !== kind) return false;
    if (!normalized) return true;
    return [item.name, item.purpose, item.category]
      .join(" ")
      .toLowerCase()
      .includes(normalized);
  });
});

const sortedFiltered = computed(() => {
  return filteredItems.value.slice().sort((a, b) => {
    const sa = Number(a.sort ?? 0);
    const sb = Number(b.sort ?? 0);
    if (sa !== sb) return sa - sb;
    if (a.name !== b.name) return a.name < b.name ? -1 : 1;
    return String(a.id) < String(b.id) ? -1 : String(a.id) > String(b.id) ? 1 : 0;
  });
});

const pageCount = computed(() => Math.max(1, Math.ceil(sortedFiltered.value.length / PAGE_SIZE)));
const pageItems = computed(() => {
  const start = (page.value - 1) * PAGE_SIZE;
  return sortedFiltered.value.slice(start, start + PAGE_SIZE);
});

watch([search, categoryFilter, kindFilter, () => items.value.length], () => {
  page.value = 1;
});
watch(pageCount, (count) => {
  if (page.value > count) page.value = count;
});

function clearPageHover(): void {
  clearTimeout(pageHoverTimer);pageHoverTimer=undefined;hoverDirection.value=0;
}
function endDrag(): void {
  const active=dragStart;dragStart=null;
  clearPageHover();cancelAnimationFrame(scrollFrame);draggedId.value="";dropTarget.value="";
  window.removeEventListener("keydown", dragKeydown, true);
  if(active&&pageElement.value?.hasPointerCapture(active.pointerId))pageElement.value.releasePointerCapture(active.pointerId);
}
function dragKeydown(event:KeyboardEvent):void {
  if(event.key==='Escape'&&dragStart){event.preventDefault();event.stopImmediatePropagation();endDrag();}
}
function startDrag(event: PointerEvent, row: LinkDirectoryItem): void {
  if(!canEdit.value||saving.value||loading.value||event.button!==0||!event.isPrimary)return;
  event.preventDefault();
  dragStart={id:String(row.id),pointerId:event.pointerId,x:event.clientX,y:event.clientY};
  dragName.value=row.name;pointer.x=event.clientX;pointer.y=event.clientY;
  // Capture on the stable page, so changing pagination cannot lose the dragged row.
  pageElement.value?.setPointerCapture(event.pointerId);
  window.addEventListener('keydown',dragKeydown,true);
}
function pointerMove(event:PointerEvent):void {
  if(!dragStart||dragStart.pointerId!==event.pointerId)return;
  pointer.x=event.clientX;pointer.y=event.clientY;
  if(!draggedId.value&&Math.hypot(pointer.x-dragStart.x,pointer.y-dragStart.y)<5)return;
  if(!draggedId.value){draggedId.value=dragStart.id;movedId.value='';scrollFrame=requestAnimationFrame(scrollDrag);}
  updateDragTarget();
}
function updateDragTarget():void {
  const target=document.elementFromPoint(pointer.x,pointer.y);
  if(!target||!pageElement.value?.contains(target)){clearPageHover();dropTarget.value='';return;}
  const pager=target.closest<HTMLButtonElement>('button[data-page-direction]');
  if(pager&&!pager.disabled){
    const direction=Number(pager.dataset.pageDirection);dropTarget.value='';
    if(pageHoverTimer&&hoverDirection.value===direction)return;
    clearPageHover();hoverDirection.value=direction;
    pageHoverTimer=setTimeout(async()=>{
      pageHoverTimer=undefined;
      if(!draggedId.value)return;
      page.value=Math.max(1,Math.min(pageCount.value,page.value+direction));
      await nextTick();if(draggedId.value)updateDragTarget();
    },650);
    return;
  }
  clearPageHover();
  const row=target.closest<HTMLElement>('tr[data-link-id]');dropTarget.value=row?.dataset.linkId||'';
  if(row){const bounds=row.getBoundingClientRect();dropPlacement.value=pointer.y<bounds.top+bounds.height/2?'before':'after';}
}
function scrollDrag():void {
  if(!draggedId.value)return;
  if(!hoverDirection.value){
    const delta=pointer.y<65?-10:pointer.y>window.innerHeight-65?10:0;
    if(delta){window.scrollBy(0,delta);updateDragTarget();}
  }
  scrollFrame=requestAnimationFrame(scrollDrag);
}
function pointerDrop(event:PointerEvent):void {
  if(!dragStart||dragStart.pointerId!==event.pointerId)return;
  if(draggedId.value)updateDragTarget();
  const id=draggedId.value,target=dropTarget.value,placement=dropPlacement.value;endDrag();
  if(id&&target)void moveEntry(id,target,placement);
}
function moveByKeyboard(row: LinkDirectoryItem, direction: number): void {
  const index=sortedFiltered.value.findIndex(item=>String(item.id)===String(row.id));
  const target=sortedFiltered.value[index+direction];
  if(target)void moveEntry(String(row.id),String(target.id),direction<0?"before":"after");
}
async function moveEntry(id: string, target: string, placement: "before" | "after"): Promise<void> {
  if(!canEdit.value||saving.value||loading.value||id===target)return;
  listGeneration++;readController?.abort();saving.value=true;reordering.value=true;
  orderMessage.value="";orderFailed.value=false;
  try {
    const data=await requestJson("/api/link-directory/reorder",{
      method:"POST",body:JSON.stringify({record_id:id,target_id:target,placement}),timeoutMs:WRITE_TIMEOUT,
    });
    if(disposed)return;
    if(!Array.isArray(data.items))throw new Error("排序结果未确认，请刷新目录核对。");
    applyData(data);staleWarning.value="";movedId.value=id;
    await nextTick();
    const index=sortedFiltered.value.findIndex(row=>String(row.id)===id);
    if(index>=0)page.value=Math.floor(index/PAGE_SIZE)+1;
    orderMessage.value="顺序已保存";
    saving.value=false;reordering.value=false;
    await nextTick();
    const handle=document.querySelector<HTMLElement>(".link-table tr.moved .drag-handle");
    handle?.focus({preventScroll:true});
    handle?.closest("tr")?.scrollIntoView({behavior:"smooth",block:"nearest"});
  } catch(e) {
    if(!disposed){orderFailed.value=true;orderMessage.value=e instanceof Error?e.message:"排序未完成，请刷新核对后重试。";}
  } finally {saving.value=false;reordering.value=false;}
}

const HAS_UNSAFE_CHARS = /[\u0000-\u0020\u007f]/;

function hasNoUnsafeChars(raw: string): boolean {
  return !HAS_UNSAFE_CHARS.test(raw);
}

function parseUrl(raw: string): URL | null {
  try {
    return new URL(raw);
  } catch {
    return null;
  }
}

function isSafeWebUrl(url: unknown): boolean {
  const raw = String(url || "");
  if (!raw.trim() || !hasNoUnsafeChars(raw)) return false;
  const parsed = parseUrl(raw.trim());
  if (!parsed) return false;
  const protocol = parsed.protocol.toLowerCase();
  if (protocol !== "http:" && protocol !== "https:") return false;
  if (!parsed.hostname) return false;
  if (parsed.username || parsed.password) return false;
  return true;
}

function isBitableUrl(url: unknown): boolean {
  const raw = String(url || "");
  if (!raw.trim() || !hasNoUnsafeChars(raw)) return false;
  const parsed = parseUrl(raw.trim());
  if (!parsed) return false;
  if (parsed.protocol !== "https:") return false;
  if (parsed.hostname !== "vnet.feishu.cn") return false;
  if (parsed.username || parsed.password) return false;
  const port = parsed.port;
  if (port && port !== "443") return false;
  if (!/^\/(?:base|wiki)\/[A-Za-z0-9]+\/?$/i.test(parsed.pathname)) return false;
  // Exact vnet base/wiki path + table query -> treat as bitable.
  const tableValues = parsed.searchParams.getAll("table");
  if (tableValues.length !== 1 || !/^tbl[A-Za-z0-9]+$/.test(tableValues[0] || "")) return false;
  const viewValues = parsed.searchParams.getAll("view");
  if (viewValues.length > 1 || (viewValues.length === 1 && !/^vew[A-Za-z0-9]+$/.test(viewValues[0] || ""))) return false;
  return true;
}

function linkKind(url: unknown): "bitable" | "webpage" {
  return isBitableUrl(url) ? "bitable" : "webpage";
}

function linkKindLabel(url: unknown): string {
  return linkKind(url) === "bitable" ? "多维表" : "网页导航";
}

function isTrustedUrl(url: unknown): boolean {
  if (!isSafeWebUrl(url)) return false;
  const parsed = parseUrl(String(url).trim());
  if (parsed?.port === '0') return false;
  return parsed?.hostname === 'vnet.feishu.cn' && /^\/(?:base|wiki)\//.test(parsed.pathname)
    ? isBitableUrl(url) : true;
}

function applyData(data: Record<string, any>): void {
  if (Array.isArray(data?.items)) {
    items.value = data.items;
  }
  if (typeof data?.can_edit === "boolean") canEdit.value = data.can_edit;
  if (data?.error) {
    if (items.value.length) staleWarning.value = String(data.error);
    else if (!error.value) error.value = String(data.error);
  }
}

function onRevalidated(data: Record<string, any>): void {
  if (modalOpen.value || saving.value || draggedId.value) return;
  applyData(data);
}

async function loadData(forceFresh: boolean): Promise<void> {
  readController?.abort();
  const controller = new AbortController();
  readController = controller;
  const loadGeneration = listGeneration;
  loading.value = true;
  error.value = "";
  staleWarning.value = "";
  try {
    let data: Record<string, any>;
    if (forceFresh) {
      // Refresh is a read-only POST that bypasses the 5-minute GET cache.
      data = await requestJson("/api/link-directory/refresh", {
        method: "POST",
        body: "{}",
        timeoutMs: REFRESH_TIMEOUT,
        signal: controller.signal,
      });
    } else {
      data = await requestJson("/api/link-directory", { signal: controller.signal }, { onRevalidated });
    }
    if (controller.signal.aborted || loadGeneration !== listGeneration) return;
    applyData(data);
  } catch (e) {
    if (controller.signal.aborted || loadGeneration !== listGeneration) return;
    const message = e instanceof Error ? e.message : "链接列表读取失败，请重试。";
    error.value = message;
  } finally {
    if (readController === controller) loading.value = false;
  }
}

function reload(): void {
  void loadData(true);
}

// ---- editor modal ----
const modalOpen = ref(false);
const modalElement = ref<HTMLElement | null>(null);
const modalTitleId = `link-modal-${Math.random().toString(36).slice(2)}`;
const editor = reactive<LinkDirectoryItem>({
  id: "",
  name: "",
  url: "",
  category: "",
  purpose: "",
  sort: 0,
});
const editorSnapshot = ref("");
const modalError = ref("");
const requestId = ref("");
let modalOwner: ReturnType<typeof acquireModal> | undefined;
let returnFocus: HTMLElement | null = null;

const modalDirty = computed(() => (
  JSON.stringify({ ...editor, id: String(editor.id || "") }) !== editorSnapshot.value
));
const confirmOpen = ref(false);
const confirmMessage = ref("");
let pendingDelete: LinkDirectoryItem | null = null;

function v4FromBytes(bytes: Uint8Array): string {
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function uid(): string {
  let candidate = "";
  if (typeof globalThis.crypto?.randomUUID === "function") {
    candidate = globalThis.crypto.randomUUID();
  } else {
    const bytes = new Uint8Array(16);
    if (globalThis.crypto?.getRandomValues) {
      globalThis.crypto.getRandomValues(bytes);
    } else {
      for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
    }
    candidate = v4FromBytes(bytes);
  }
  if (!UUID_V4_RE.test(candidate)) {
    // Defensive re-derivation; RFC4122 v4 is required by the backend.
    const bytes = new Uint8Array(16);
    for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
    candidate = v4FromBytes(bytes);
  }
  return candidate;
}

function openCreate(): void {
  Object.assign(editor, { id: "", name: "", url: "", category: "", purpose: "", sort: 0 });
  modalError.value = "";
  requestId.value = uid();
  editorSnapshot.value = JSON.stringify({ ...editor, id: "" });
  modalOpen.value = true;
}

function openEdit(item: LinkDirectoryItem): void {
  Object.assign(editor, {
    id: item.id,
    name: item.name || "",
    url: item.url || "",
    category: item.category || "",
    purpose: item.purpose || "",
    sort: Number(item.sort ?? 0),
  });
  modalError.value = "";
  editorSnapshot.value = JSON.stringify({ ...editor, id: String(editor.id || "") });
  modalOpen.value = true;
}

function closeModal(): void {
  if (saving.value) return;
  modalOpen.value = false;
  modalError.value = "";
}

function validateEditor(): string {
  const name = String(editor.name || "").trim();
  const url = String(editor.url || "").trim();
  if (!name) return "请填写名称。";
  if (!url) return "请填写链接 URL。";
  if (!isTrustedUrl(url)) return "链接需为 http/https 网页地址；飞书多维表需 vnet.feishu.cn base/wiki 链接并含 table 参数。";
  const sort = Number(editor.sort);
  if (!Number.isInteger(sort) || sort < 0 || sort > 1_000_000) return "排序需为 0 到 1000000 之间的整数。";
  if (String(editor.purpose || "").length > 2000) return "用途最多 2000 字。";
  return "";
}

async function saveEntry(): Promise<void> {
  if (saving.value) return;
  const problem = validateEditor();
  if (problem) {
    modalError.value = problem;
    return;
  }
  saving.value = true;
  modalError.value = "";
  const payload = {
    name: String(editor.name || "").trim(),
    url: String(editor.url || "").trim(),
    category: String(editor.category || "").trim(),
    purpose: String(editor.purpose || "").trim(),
    sort: Number(editor.sort),
  };
  try {
    const id = String(editor.id || "");
    if (id) {
      const data = await requestJson("/api/link-directory/" + encodeURIComponent(id), {
        method: "PUT",
        body: JSON.stringify(payload),
        timeoutMs: WRITE_TIMEOUT,
      });
      const item = data.item as LinkDirectoryItem;
      if (item) upsertItem(item);
    } else {
      const data = await requestJson("/api/link-directory", {
        method: "POST",
        body: JSON.stringify({ ...payload, request_id: requestId.value }),
        timeoutMs: WRITE_TIMEOUT,
      });
      const item = data.item as LinkDirectoryItem;
      if (item) upsertItem(item);
      requestId.value = uid();
    }
    modalOpen.value = false;
    modalError.value = "";
  } catch (e) {
    if (!modalOpen.value) return;
    modalError.value = e instanceof Error ? e.message : "保存失败，请重试。";
  } finally {
    saving.value = false;
  }
}

function upsertItem(item: LinkDirectoryItem): void {
  listGeneration += 1;
  const index = items.value.findIndex((existing) => String(existing.id) === String(item.id));
  if (index >= 0) {
    items.value.splice(index, 1, item);
  } else {
    items.value.push(item);
  }
}

function askDelete(item: LinkDirectoryItem): void {
  pendingDelete = item;
  confirmMessage.value = `仅删除「${item.name || "此链接"}」这个导航入口，不会删除原始网页、多维表或知识库文档。`;
  confirmOpen.value = true;
}

async function resolveDelete(confirmed: boolean): Promise<void> {
  confirmOpen.value = false;
  const target = pendingDelete;
  pendingDelete = null;
  if (!confirmed || !target) return;
  saving.value = true;
  try {
    await requestJson("/api/link-directory/" + encodeURIComponent(String(target.id)), {
      method: "DELETE",
      body: "{}",
      timeoutMs: WRITE_TIMEOUT,
    });
    listGeneration += 1;
    items.value = items.value.filter((existing) => String(existing.id) !== String(target.id));
  } catch (e) {
    if (!error.value) error.value = e instanceof Error ? e.message : "删除失败，请重试。";
  } finally {
    saving.value = false;
  }
}

function modalKeydown(event: KeyboardEvent): void {
  if (!modalOwner?.isTop(event, modalElement.value) || !modalElement.value || confirmOpen.value) return;
  if (event.key === "Escape") {
    event.preventDefault();
    event.stopImmediatePropagation();
    closeModal();
  } else if (event.key === "Tab") {
    const nodes = Array.from(modalElement.value.querySelectorAll<HTMLElement>(
      'button:not(:disabled),input:not(:disabled),textarea:not(:disabled),select:not(:disabled),[tabindex="0"]',
    )).filter((node) => node.getClientRects().length);
    const first = nodes[0], last = nodes[nodes.length - 1];
    if (!modalElement.value.contains(document.activeElement) || (event.shiftKey ? document.activeElement === first : document.activeElement === last)) {
      event.preventDefault();
      (event.shiftKey ? last : first)?.focus();
    }
  }
}

watch(modalOpen, async (open, previous) => {
  if (open && !previous) {
    returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    modalOwner = acquireModal();
    window.addEventListener("keydown", modalKeydown, true);
  }
  if (!open) {
    modalOwner?.release();
    modalOwner = undefined;
    window.removeEventListener("keydown", modalKeydown, true);
    await nextTick();
    returnFocus?.focus?.();
    returnFocus = null;
  } else {
    await nextTick();
    modalElement.value?.focus();
  }
});

onBeforeUnmount(() => {
  disposed=true;endDrag();
  readController?.abort();
  modalOwner?.release();
  modalOwner = undefined;
  window.removeEventListener("keydown", modalKeydown, true);
  saving.value = false;
  confirmOpen.value = false;
});

void loadData(false);
</script>

<style scoped>
.link-directory-page {
  display: grid;
  gap: 12px;
  padding: 14px 18px 22px;
  color: #20344e;
  font-size: 14px;
}

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.page-head,
.heading,
.actions,
.toolbar,
.table-footer,
.table-footer nav {
  display: flex;
  align-items: center;
  gap: 10px;
}

.page-head {
  justify-content: space-between;
}

.page-head h1 {
  margin: 0;
  color: #0b2547;
  font-size: 18px;
  font-weight: 850;
}

.count {
  color: #71839a;
  font-size: 12px;
  font-weight: 700;
}

.icon-button {
  display: inline-grid;
  place-items: center;
  min-width: 34px;
  height: 34px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  background: #f8fbff;
  color: #1d63d6;
  cursor: pointer;
}

.icon-button.danger {
  color: #c2414a;
}

.icon-button:disabled,
button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.toolbar {
  flex-wrap: wrap;
  justify-content: space-between;
}

.search-field {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 260px;
  flex: 1 1 280px;
  max-width: 480px;
  height: 40px;
  padding: 0 12px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  color: #64809f;
  background: #fff;
}

.search-field:focus-within {
  border-color: #4c91f2;
  box-shadow: 0 0 0 3px rgba(37, 117, 232, 0.1);
}

.search-field input {
  width: 100%;
  min-width: 0;
  border: 0;
  outline: 0;
  background: transparent;
  color: #10233f;
  font: inherit;
  font-size: 13px;
}

.category-field select {
  min-height: 40px;
  min-width: 160px;
}

.segmented {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  padding: 3px;
  border: 1px solid #d5e4f7;
  border-radius: 8px;
  background: #f2f7fe;
}

.segmented button {
  min-height: 32px;
  padding: 0 12px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #5a7192;
  font: inherit;
  font-size: 12px;
  font-weight: 750;
  cursor: pointer;
}

.segmented button:hover {
  color: #1d63d6;
}

.segmented button.active {
  background: #fff;
  color: #1554df;
  box-shadow: 0 1px 3px rgba(21, 84, 223, 0.18);
}

.primary.add-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  min-height: 42px;
  padding: 0 16px;
  border: 0;
  border-radius: 8px;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
  font: inherit;
  font-size: 13px;
  font-weight: 800;
  cursor: pointer;
}

.state-block {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 26px 18px;
  border: 1px solid #e0ebf7;
  border-radius: 8px;
  background: #f7faff;
  color: #5d718a;
  font-size: 13px;
  text-align: center;
}

.state-block.error {
  border-color: #f6cdcd;
  background: #fff2f2;
  color: #b33a3a;
  justify-content: flex-start;
  flex-wrap: wrap;
}

.state-block.warn {
  border-color: #f0e2b8;
  background: #fffaf0;
  color: #92600d;
  justify-content: flex-start;
  flex-wrap: wrap;
}

.state-block.error button {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 32px;
  padding: 0 12px;
  border: 1px solid #eccaca;
  border-radius: 8px;
  background: #fff;
  color: #a03636;
  font: inherit;
  font-size: 12px;
  font-weight: 750;
  cursor: pointer;
}

.table-wrap {
  overflow-x: auto;
  border: 1px solid #e0ebf7;
  border-radius: 8px;
  background: #fff;
}

.link-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}

.link-table th,
.link-table td {
  padding: 8px 10px;
  border-bottom: 1px solid #edf3fa;
  text-align: left;
  vertical-align: middle;
  font-size: 13px;
}

.link-table th {
  background: #f6f9fd;
  color: #5c718b;
  font-size: 12px;
  font-weight: 750;
}

.link-table tr:last-child td {
  border-bottom: 0;
}

.link-table th:first-child { width: 26%; }
.link-table th:nth-child(2) { width: 14%; }
.link-table th:nth-child(3) { width: auto; }
.link-table th:nth-child(4) { width: 72px; }
.link-table th:nth-child(5) { width: 132px; }

.drag-handle { cursor: grab; color: #627990; touch-action: none; }
.drag-handle:active { cursor: grabbing; }
.link-table tr.dragging { opacity: .45; }
.link-drag-preview { position:fixed; z-index:2600; pointer-events:none; display:flex; align-items:center; gap:6px; width:max-content; max-width:300px; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; padding:8px 10px; border:1px solid #b9d5f2; border-radius:6px; background:#fff; color:#244f79; box-shadow:0 4px 16px #294e7826; font-size:13px; }
.link-drag-preview svg { flex:none; }.link-drag-preview span { overflow:hidden; text-overflow:ellipsis; }
.link-table tr.moved { background: #edf7ff; }
.link-table tr.drop-before td { box-shadow: inset 0 3px #2776ce; }
.link-table tr.drop-after td { box-shadow: inset 0 -3px #2776ce; }
.order-status { min-height: 22px; color: #237553; font-size: 13px; }
.order-status.failed { color: #b33a3a; }
.table-footer.drag-pagination { position: sticky; bottom: 12px; z-index: 5; padding: 10px 64px 10px 12px; background: #fff; border: 1px solid #b9d5f2; border-radius: 8px; box-shadow: 0 4px 18px #294e7820; }
.table-footer button.page-hover { background: #e5f1ff; border-color: #2776ce; }

.name-cell a {
  color: #1554df;
  font-weight: 750;
  overflow-wrap: anywhere;
  text-decoration: none;
}

.name-cell a.link-name {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  max-width: 100%;
  vertical-align: middle;
}

.kind-icon-wrap {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex: 0 0 auto;
  width: 18px;
  height: 18px;
  border-radius: 5px;
}

.kind-icon-wrap .kind-icon {
  stroke-width: 2;
}

.kind-icon-wrap .bitable-icon {
  color: #1554df;
}

.kind-icon-wrap .globe-icon {
  color: #0d9488;
}

.name-cell a:hover {
  text-decoration: underline;
}

.name-cell > span {
  overflow-wrap: anywhere;
}

.category-cell,
.purpose-cell {
  color: #334d6b;
  overflow-wrap: anywhere;
}

.sort-cell {
  color: #526c88;
  font-variant-numeric: tabular-nums;
}

.row-actions {
  display: flex;
  align-items: center;
  gap: 6px;
}

.table-footer {
  justify-content: space-between;
  color: #71839a;
  font-size: 12px;
}

.table-footer nav {
  gap: 8px;
}

.table-footer button {
  min-height: 32px;
  padding: 0 12px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  background: #f8fbff;
  color: #1d63d6;
  font: inherit;
  font-size: 12px;
  font-weight: 750;
  cursor: pointer;
}

.table-footer b {
  color: #334d6b;
  font-size: 12px;
}

.spinning {
  animation: link-spin 900ms linear infinite;
}

@keyframes link-spin {
  to { transform: rotate(360deg); }
}

/* modal */
.link-backdrop {
  position: fixed;
  inset: 0;
  z-index: 800;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(15, 35, 65, 0.4);
}

.link-modal {
  display: flex;
  flex-direction: column;
  width: min(560px, 100%);
  max-height: calc(100dvh - 48px);
  background: #fff;
  border: 1px solid #d8e5f7;
  border-radius: 8px;
  box-shadow: 0 22px 80px rgba(18, 53, 98, 0.22);
  font: inherit;
  color: #20344e;
  outline: none;
}

.link-modal > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 16px 20px;
  border-bottom: 1px solid #d8e5f7;
  flex: 0 0 auto;
}

.link-modal > header h2 {
  margin: 0;
  font-size: 16px;
  font-weight: 850;
  color: #0b2547;
}

.modal-scroll {
  padding: 18px 20px;
  overflow-y: auto;
  min-height: 0;
  overscroll-behavior: contain;
}

.modal-alert {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: 14px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #fff1f1;
  color: #b33a3a;
  font-size: 13px;
  line-height: 1.5;
}

.editor-form {
  display: grid;
  gap: 16px;
}

.form-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 150px;
  gap: 14px;
}

.field {
  display: grid;
  gap: 6px;
}

.field > span {
  color: #334d6b;
  font-size: 13px;
  font-weight: 750;
}

.field em {
  color: #d6454d;
  font-style: normal;
}

.field input,
.field textarea {
  width: 100%;
  box-sizing: border-box;
  min-width: 0;
  max-width: 100%;
  min-height: 40px;
  padding: 8px 10px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  background: #fff;
  font: inherit;
  font-size: 13px;
  color: #10233f;
  resize: vertical;
}

.field input:focus,
.field textarea:focus {
  border-color: #155dfc;
  box-shadow: 0 0 0 3px rgba(21, 93, 252, 0.14);
  outline: none;
}

.field-hint {
  color: #8a98ab;
  font-size: 11px;
  line-height: 1.4;
}

.sort-field input {
  min-width: 0;
}

.link-modal > footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 20px;
  border-top: 1px solid #d8e5f7;
  background: #f8fbff;
  flex: 0 0 auto;
  flex-wrap: wrap;
}

.link-modal > footer .actions {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-left: auto;
}

.link-modal > footer .actions button {
  min-height: 40px;
  padding: 0 16px;
  border: 1px solid #cfe0f4;
  border-radius: 8px;
  background: #fff;
  color: #33526f;
  font: inherit;
  font-size: 13px;
  font-weight: 750;
  cursor: pointer;
}

.link-modal > footer .actions button.primary {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 0;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
}

.muted {
  color: #71839a;
  font-size: 12px;
}

@media (max-width: 700px) {
  .link-directory-page {
    padding: 10px 12px 18px;
  }

  .link-table {
    min-width: 640px;
  }

  .form-row {
    grid-template-columns: minmax(0, 1fr);
  }

  .link-backdrop {
    padding: 10px;
  }

  .link-modal {
    max-height: calc(100dvh - 20px);
  }
}
</style>
