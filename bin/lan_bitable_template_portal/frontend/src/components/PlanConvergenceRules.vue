<template>
  <section ref="rulesElement" class="pc-rules" :class="{ 'draft-only': draftOnly }">
    <fieldset class="editor-fields" :disabled="disabled">
    <!-- ===== 紧凑工具栏 ===== -->
    <header v-if="!draftOnly" class="toolbar">
      <span v-if="!isAdmin" class="badge muted">只读</span>
      <span v-else class="badge">管理员</span>
      <span v-if="currentSetMeta" class="muted set-name-inline" :title="currentSetMeta">{{ currentSetMeta }}</span>
      <div class="actions">
        <button type="button" v-if="currentSetId && isAdmin" class="icon-button danger" title="删除当前规则集及其全部条目（不可恢复）" aria-label="删除当前规则集" :disabled="busy || saving" @click="deleteSet"><Trash2 :size="15" /></button>
        <button type="button" v-if="currentSetId" class="command-button" title="查看规则集覆盖设备" :disabled="busy || saving || expanding" @click="previewExpand"><Eye :size="15" />查看覆盖</button>
        <button type="button" v-if="isAdmin && currentSetId" class="primary" :disabled="busy || saving || !dirty" @click="saveSet"><Save :size="15" />保存规则集</button>
        <button type="button" v-if="currentSetId && !draftOnly" :disabled="busy || saving" @click="reloadSet"><RefreshCw :size="15" />读取最新规则</button>
      </div>
    </header>

    <div v-if="toast.msg" class="toast" :class="toast.type" :role="toast.type === 'error' ? 'alert' : 'status'"><span>{{ toast.msg }}</span><button type="button" class="icon-button" aria-label="关闭提示" @click="toast.msg = ''"><X :size="15" /></button></div>

    <!-- ===== 工作区（无边框分离列） ===== -->
    <div class="workspace">
      <!-- 左：规则集列表 -->
      <aside v-if="!draftOnly" class="set-panel panel">
        <div class="panel-title"><h3>规则集</h3><button type="button" v-if="isAdmin" class="icon-button" title="刷新" aria-label="刷新规则集" :disabled="setsLoading || busy || saving" @click="loadSets"><RefreshCw :size="15" /></button></div>
        <form v-if="isAdmin" class="new-set-form" @submit.prevent="createSet">
          <input v-model="newSetName" placeholder="新规则集名称（如：南区公共部分）" maxlength="60" :disabled="busy || saving || setsLoading" />
          <button type="submit" class="primary" title="新建规则集" aria-label="新建规则集" :disabled="busy || saving || setsLoading || !newSetName.trim()"><Plus :size="15" /></button>
        </form>
        <p v-if="setsLoading" class="muted empty"><Loader2 :size="16" class="spin" />加载中…</p>
        <p v-else-if="setsError" class="alert error" role="alert"><AlertCircle :size="16" /><span>{{ setsError }}</span></p>
        <p v-else-if="!sets.length" class="muted empty">暂无规则集，{{ isAdmin ? "请在左侧新建。" : "请等待管理员创建。" }}</p>
        <ul v-else class="set-list">
          <li v-for="s in sets" :key="s.id" :class="{ active: currentSetId === s.id }">
            <button type="button" class="set-row" :disabled="busy || saving" @click="selectSet(s.id)">
              <span class="set-name">{{ s.name || "未命名规则集" }}</span>
              <span class="set-meta"><span class="badge">{{ s.item_count ?? 0 }} 项</span><small class="muted">{{ s.remark ? s.remark.slice(0, 18) + (s.remark.length > 18 ? "…" : "") : "无备注" }}</small></span>
            </button>
            <button type="button" v-if="isAdmin" class="icon-button danger" title="删除该规则集及其全部条目（不可恢复）" aria-label="删除该规则集" :disabled="busy || saving" @click.stop="deleteSetById(s.id, s.name || '未命名规则集')"><Trash2 :size="14" /></button>
          </li>
        </ul>
      </aside>

      <!-- 中：四列选择器 -->
      <main class="picker-panel panel">
        <div class="panel-title">
          <h3>选择屏蔽范围</h3>
          <div class="actions">
            <button type="button" class="primary" :disabled="!isAdmin || busy || saving || !hasCurrentSet" @click="addEntry"><Plus :size="16" />添加屏蔽范围</button>
          </div>
        </div>

        <template v-if="hasCurrentSet">
          <!-- 当前规则集 名称 / 备注 -->
          <div class="set-fields">
            <label>规则集名称
              <input v-model="name" :disabled="!isAdmin || busy || saving" maxlength="60" placeholder="规则集名称" :required="draftOnly" />
            </label>
            <label>备注
              <textarea v-model="remark" :disabled="!isAdmin || busy || saving" rows="2" maxlength="300" placeholder="备注（可选）"></textarea>
            </label>
          </div>

          <!-- 4 列选择器 -->
          <div class="picker-grid">
            <!-- 列1 设备类型 -->
            <div class="picker-col">
              <div class="col-head">
                <label class="col-all" title="全选当前筛选出的候选（已加载部分）；结果较多时仅为子集，不代表全量数据"><input type="checkbox" :checked="objAllChecked" :disabled="!isAdmin || saving || objsFiltered.length === 0" @change="toggleColAll('obj')" /><strong>设备类型</strong></label>
                <span class="muted count" title="已勾选 / 当前筛选候选（已加载）">{{ objSelCount }} / {{ objsFiltered.length }}<span v-if="objHasMore">+</span></span>
              </div>
              <input v-model="objKw" class="col-search" placeholder="搜索类型…" :disabled="!isAdmin || saving" />
              <p v-if="objLoading" class="muted empty"><Loader2 :size="15" class="spin" />加载中…</p>
              <p v-else-if="objError" class="alert error"><AlertCircle :size="15" /><span>{{ objError }}</span></p>
              <p v-else-if="!objsFiltered.length" class="muted empty">{{ objs.length ? "无匹配类型" : "暂无设备类型" }}</p>
              <ul v-else class="cand-list">
                <li v-for="o in objPageItems" :key="o.obj_name">
                  <label class="cand"><input type="checkbox" :checked="o._checked" :disabled="!isAdmin || saving" @change="onObjCheck(o, $event)" /><span class="cand-name" :title="o.obj_name">{{ o.obj_name }}</span><span class="cand-count">{{ o.devices }}</span></label>
                </li>
              </ul>
              <p v-if="objHasMore" class="hint">结果较多，当前仅已加载候选中的部分，请缩小搜索范围</p>
              <div v-if="objPagesCount > 1" class="pagination">
                <button type="button" class="icon-button" :disabled="objPage <= 1 || saving" @click="objPage--"><ChevronLeft :size="15" /></button>
                <span>{{ objPage }} / {{ objPagesCount }}</span>
                <button type="button" class="icon-button" :disabled="objPage >= objPagesCount || saving" @click="objPage++"><ChevronRight :size="15" /></button>
              </div>
            </div>

            <!-- 列2 空间 -->
            <div class="picker-col">
              <div class="col-head">
                <label class="col-all" title="全选当前筛选出的候选空间（已加载部分）；结果较多时仅为子集，不代表全量空间"><input type="checkbox" :checked="roomAllChecked" :disabled="!isAdmin || saving || roomFiltered.length === 0" @change="toggleColAll('room')" /><strong>空间</strong></label>
                <span class="muted count" title="已勾选 / 当前筛选候选（已加载）">{{ roomSelCount }} / {{ roomFiltered.length }}<span v-if="roomHasMore">+</span></span>
              </div>
              <div class="breadcrumb">
                <button type="button" :disabled="saving" @click="crumbTo('root')" :title="drill.zone || drill.building || drill.floor ? '返回区列表' : '当前在区列表'">区</button>
                <template v-if="drill.zone">
                  <span class="sep">›</span>
                  <button type="button" :disabled="saving" @click="crumbTo('zone')" title="返回该区下的楼栋列表" :class="{ 'crumb-cur': !drill.building }">{{ drill.zone }}</button>
                  <template v-if="drill.building">
                    <span class="sep">›</span>
                    <button type="button" :disabled="saving" @click="crumbTo('building')" title="返回该楼下的楼层列表" :class="{ 'crumb-cur': !drill.floor }">{{ drill.building }}</button>
                    <template v-if="drill.floor">
                      <span class="sep">›</span><span class="crumb-cur">{{ drill.floor }}</span>
                    </template>
                  </template>
                </template>
              </div>
              <input v-model="roomKw" class="col-search" placeholder="搜索空间…" :disabled="!isAdmin || saving" />
              <p v-if="roomLoading" class="muted empty"><Loader2 :size="15" class="spin" />加载中…</p>
              <p v-else-if="roomError" class="alert error"><AlertCircle :size="15" /><span>{{ roomError }}</span></p>
              <p v-else-if="!roomFiltered.length" class="muted empty">{{ rooms.length ? "无匹配空间" : "暂无空间" }}</p>
              <ul v-else class="cand-list">
                <li v-for="r in roomPageItems" :key="roomNodeKey(r)">
                  <label class="cand"><input type="checkbox" :checked="r._checked" :disabled="!isAdmin || saving" @change="onRoomCheck(r, $event)" /><span class="cand-name" :title="r.name">{{ r.name }}</span><span class="cand-count">{{ r.devices }}</span></label>
                  <button type="button" v-if="r.level !== 'room'" class="icon-button drill" title="下钻" aria-label="下钻" :disabled="!isAdmin || busy || saving" @click="drillDown(r)"><ChevronRight :size="15" /></button>
                </li>
              </ul>
              <p v-if="roomHasMore" class="hint">结果较多，当前仅已加载候选中的部分，请缩小搜索范围</p>
              <div v-if="roomPagesCount > 1" class="pagination">
                <button type="button" class="icon-button" :disabled="roomPage <= 1 || saving" @click="roomPage--"><ChevronLeft :size="15" /></button>
                <span>{{ roomPage }} / {{ roomPagesCount }}</span>
                <button type="button" class="icon-button" :disabled="roomPage >= roomPagesCount || saving" @click="roomPage++"><ChevronRight :size="15" /></button>
              </div>
            </div>

            <!-- 列3 设备 -->
            <div class="picker-col">
              <div class="col-head">
                <label class="col-all" title="全选当前筛选出的设备候选（已加载部分）；结果较多时仅为子集，不代表全量设备"><input type="checkbox" :checked="devAllChecked" :disabled="!isAdmin || saving || devFiltered.length === 0" @change="toggleColAll('dev')" /><strong>设备</strong></label>
                <span class="muted count" title="已勾选 / 当前筛选候选（已加载）">{{ devSelCount }} / {{ devFiltered.length }}<span v-if="devHasMore">+</span></span>
              </div>
              <input v-model="devKw" class="col-search" placeholder="搜索设备…" :disabled="!isAdmin || saving" />
              <p v-if="devLoading" class="muted empty"><Loader2 :size="15" class="spin" />加载中…</p>
              <p v-else-if="devError" class="alert error"><AlertCircle :size="15" /><span>{{ devError }}</span></p>
              <p v-else-if="!hasObjCtx && !hasRoomCtx" class="muted empty">先在类型 / 空间中选择以加载设备</p>
              <p v-else-if="!devFiltered.length" class="muted empty">暂无匹配设备</p>
              <ul v-else class="cand-list">
                <li v-for="d in devPageItems" :key="d.inst_name + ':' + (d.ins_id || '')">
                  <label class="cand"><input type="checkbox" :checked="d._checked" :disabled="!isAdmin || saving" @change="onDevCheck(d, $event)" /><span class="cand-name" :title="d.inst_name">{{ d.inst_name }}</span><button type="button" class="link-button" title="查看详情" @click.prevent="openDevDetail(d)">详情</button></label>
                </li>
              </ul>
              <p v-if="devHasMore" class="hint">结果较多，当前仅已加载候选中的部分，请缩小搜索范围</p>
              <div v-if="devPagesCount > 1" class="pagination">
                <button type="button" class="icon-button" :disabled="devPage <= 1 || saving" @click="devPage--"><ChevronLeft :size="15" /></button>
                <span>{{ devPage }} / {{ devPagesCount }}</span>
                <button type="button" class="icon-button" :disabled="devPage >= devPagesCount || saving" @click="devPage++"><ChevronRight :size="15" /></button>
              </div>
            </div>

            <!-- 列4 告警规则 -->
            <div class="picker-col">
              <div class="col-head">
                <label class="col-all" title="全选当前筛选出的规则候选（已加载部分）；结果较多时仅为子集，不代表全量规则"><input type="checkbox" :checked="ptAllChecked" :disabled="!isAdmin || saving || ptFiltered.length === 0" @change="toggleColAll('pt')" /><strong>告警规则</strong></label>
                <span class="muted count" title="已勾选 / 当前筛选候选（已加载）">{{ ptSelCount }} / {{ ptFiltered.length }}<span v-if="ptHasMore">+</span></span>
              </div>
              <input v-model="ptKw" class="col-search" placeholder="搜索规则…" :disabled="!isAdmin || saving" />
              <p v-if="ptLoading" class="muted empty"><Loader2 :size="15" class="spin" />加载中…</p>
              <p v-else-if="ptError" class="alert error"><AlertCircle :size="15" /><span>{{ ptError }}</span></p>
              <p v-else-if="!hasObjCtx && !hasDevCtx && !hasRoomCtx" class="muted empty">先在类型 / 空间 / 设备中选择以加载规则</p>
              <p v-else-if="!ptFiltered.length" class="muted empty">暂无匹配规则</p>
              <ul v-else class="cand-list">
                <li v-for="p in ptPageItems" :key="p.alarm_config_id || p.alarm_name">
                  <label class="cand"><input type="checkbox" :checked="p._checked" :disabled="!isAdmin || saving" @change="onPtCheck(p, $event)" /><span class="cand-name" :title="p.alarm_name">{{ p.alarm_name }}</span><span class="cand-count" v-if="p.classify_model">{{ p.classify_model }}</span><button type="button" class="link-button" title="查看详情" @click.prevent="openPtDetail(p)">详情</button></label>
                </li>
              </ul>
              <p v-if="ptHasMore" class="hint">结果较多，当前仅已加载候选中的部分，请缩小搜索范围</p>
              <div v-if="ptPagesCount > 1" class="pagination">
                <button type="button" class="icon-button" :disabled="ptPage <= 1 || saving" @click="ptPage--"><ChevronLeft :size="15" /></button>
                <span>{{ ptPage }} / {{ ptPagesCount }}</span>
                <button type="button" class="icon-button" :disabled="ptPage >= ptPagesCount || saving" @click="ptPage++"><ChevronRight :size="15" /></button>
              </div>
            </div>
          </div>
        </template>
        <p v-else class="hint center">请先选择或新建一个规则集。</p>
      </main>

      <!-- 右：草稿组 -->
      <aside class="draft-panel panel">
        <div class="panel-title"><h3>屏蔽条目（{{ drafts.length }}）</h3><div class="actions"><button type="button" v-if="isAdmin" class="danger-text command-button text-btn" :disabled="busy || saving || !drafts.length" @click="clearAll"><Trash2 :size="15" />清空</button><button type="button" v-if="isAdmin" :disabled="busy || saving || mergeSel.length < 2" @click="mergeSelected"><Merge :size="15" />合并({{ mergeSel.length }})</button></div></div>
        <p class="muted hint">「公共」条目须全部被包含；此外至少需匹配一个「普通」条目。没有普通条目时，只需满足全部公共条目。</p>
        <p v-if="!drafts.length" class="muted empty">暂无屏蔽条目。</p>
        <ul v-else class="draft-list">
          <li v-for="(d, i) in drafts" :key="i" :class="{ selected: mergeSel.includes(i), common: d.rule_type === 'common' }">
            <div class="draft-head">
              <button type="button" v-if="isAdmin && d.rule_type === 'normal'" class="icon-button merge-check" :class="{ on: mergeSel.includes(i) }" title="选择参与合并" aria-label="选择参与合并" :disabled="busy || saving" @click="toggleMergeSel(i)"><Square :size="14" /></button>
              <span class="draft-idx">#{{ i + 1 }}</span>
              <input v-if="isAdmin" v-model="d.label" class="draft-label" :disabled="busy || saving" :aria-label="'第' + (i + 1) + '组名称'" />
              <span v-else class="draft-label">{{ d.label }}</span>
              <button type="button" :class="['badge', d.rule_type === 'common' ? 'common-badge' : '']" :title="d.rule_type === 'common' ? '公共条目：须全部包含；且（如有）至少匹配一个普通条目' : '普通条目：至少匹配一个（若有公共条目，须先全部匹配）'" :disabled="!isAdmin || busy || saving" @click="toggleCommon(i)">{{ d.rule_type === 'common' ? '公共' : '普通' }}</button>
              <button type="button" class="icon-button" title="查看条目" aria-label="查看条目" :disabled="busy || saving" @click="openGroupDetail(i)"><Eye :size="15" /></button>
              <button type="button" v-if="isAdmin" class="icon-button danger" title="删除条目" aria-label="删除条目" :disabled="busy || saving" @click="removeGroup(i)"><Trash2 :size="15" /></button>
            </div>
            <p class="draft-summary">{{ d.items.length }} 项</p>
          </li>
        </ul>
      </aside>
    </div>
    </fieldset>

    <!-- ===== 预览 / 详情 弹窗 ===== -->
    <Teleport to="body">
      <UiTransition name="ui-overlay" appear>
      <div v-if="preview.open" class="pc-modal-overlay" :style="draftOnly ? { ...previewTheme, zIndex: 10010 } : undefined" @click.self="closePreview">
        <section ref="modalElement" class="pc-modal" role="dialog" aria-modal="true" tabindex="-1">
          <header><h2>{{ preview.title }}</h2><button type="button" class="icon-button" aria-label="关闭" title="关闭" @click="closePreview"><X :size="18" /></button></header>
          <div class="pc-modal-scroll">
            <p v-if="previewKindNote" class="muted hint">{{ previewKindNote }}</p>
            <table v-if="preview.kind === 'table' && previewRows.length" class="table-wrap">
              <thead><tr><th v-for="(h, n) in preview.headers" :key="n">{{ h }}</th></tr></thead>
              <tbody><tr v-for="(row, n) in previewRows" :key="n"><td v-for="(cell, m) in row" :key="m">{{ cell }}</td></tr></tbody>
            </table>
            <p v-else-if="preview.kind === 'table' && !previewRows.length" class="muted empty">暂无数据</p>
            <dl v-else-if="preview.kind === 'kv'" class="kv-list">
              <template v-for="(kv, n) in preview.kv" :key="n"><dt>{{ kv.label }}</dt><dd>{{ kv.value }}</dd></template>
            </dl>
          </div>
          <footer>
            <template v-if="preview.kind === 'table' && (preview.total ?? 0) > 0">
              <span class="muted">每页 {{ PREVIEW_PAGE }} 条 · 共 {{ preview.total }} 条</span>
              <div class="actions">
                <button type="button" class="icon-button" :disabled="previewPage <= 1" @click="previewPage--"><ChevronLeft :size="17" /></button>
                <span>{{ previewPage }} / {{ previewPages }}</span>
                <button type="button" class="icon-button" :disabled="previewPage >= previewPages" @click="previewPage++"><ChevronRight :size="17" /></button>
              </div>
            </template>
            <span v-else></span>
            <button type="button" class="primary" @click="closePreview">关闭</button>
          </footer>
        </section>
      </div>
      </UiTransition>
    </Teleport>

    <ConfirmDialog
      :open="confirm.open"
      :style="draftOnly ? { zIndex: 10020 } : undefined"
      :title="confirm.title"
      :message="confirm.message"
      :tone="confirm.tone"
      :details="confirm.details"
      :confirm-label="confirm.confirmLabel"
      :cancel-label="confirm.cancelLabel"
      @resolve="onConfirmResolve"
    />
  </section>
</template>

<script lang="ts" setup>
import {
  ref, reactive, computed, watch, onMounted, onBeforeUnmount, nextTick,
} from "vue";
import {
  Plus, X, Trash2, Save, Eye, Loader2, RefreshCw, AlertCircle,
  ChevronLeft, ChevronRight, Square, Merge,
} from "lucide-vue-next";
import { requestJson, type Dict } from "../api/client";
import ConfirmDialog from "./ConfirmDialog.vue";
import { acquireModal } from "../modalState";
import { inheritedControlTheme } from "../controlTheme";
import {
  itemKey, itemDesc, entryLabel, scopeTypeLabel, entryKeyInfo, selectionItemCount,
  allDraftItems, submittedKey, draftsSubmitKey, roomToSelParam, roomNodeKey, restoreDrafts,
  type Draft, type RoomNode, type Drill,
} from "../planConvergenceRules";

const props = defineProps<{ isAdmin: boolean; modelValue?: Dict; disabled?: boolean }>();
const emit = defineEmits<{ (e: "update:modelValue", value: Dict): void }>();
const isAdmin = computed(() => props.isAdmin);
const draftOnly = computed(() => props.modelValue !== undefined);
const rulesElement = ref<HTMLElement | null>(null);
const previewTheme = ref<Record<string, string>>({});

/* 供父级路由守卫/离开保护查询未保存状态。 */
defineExpose({ hasUnsavedChanges: () => dirty.value });

const PREVIEW_PAGE = 50;
const PICKER_PAGE = 60;

/* ========== 通用提示 ========== */
const toast = reactive({ msg: "", type: "" as "" | "success" | "error" });
let toastTimer: ReturnType<typeof setTimeout> | undefined;
function toastMsg(m: string, type: "" | "success" | "error" = "") {
  toast.msg = m; toast.type = type;
  window.clearTimeout(toastTimer);
  if (type !== 'error') toastTimer = window.setTimeout(() => { toast.msg = ""; toast.type = ""; }, 2600);
}
const successMsg = (m: string) => toastMsg(m, "success");
const errorMsg = (m: string) => toastMsg(m, "error");
function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/* ========== 确认框 ========== */
const confirm = reactive({
  open: false, title: "", message: "", tone: "warning" as "danger" | "warning" | "primary",
  details: [] as string[], confirmLabel: "确认", cancelLabel: "取消",
});
let confirmResolve: ((v: boolean) => void) | null = null;
function askConfirm(opts: { title: string; message: string; tone?: "danger" | "warning" | "primary"; details?: string[]; confirmLabel?: string; cancelLabel?: string }): Promise<boolean> {
  Object.assign(confirm, {
    open: true, title: opts.title, message: opts.message,
    tone: opts.tone ?? "warning", details: opts.details ?? [],
    confirmLabel: opts.confirmLabel ?? "确认", cancelLabel: opts.cancelLabel ?? "取消",
  });
  return new Promise<boolean>((resolve) => { confirmResolve = resolve; });
}
function onConfirmResolve(v: boolean): void {
  confirm.open = false; confirmResolve?.(v); confirmResolve = null;
}

/* ========== 数据节点类型 ========== */
type ObjNode = { obj_name: string; devices: number; _checked: boolean };
type DevNode = { inst_name: string; ins_id: string; obj_name: string; position: string; _checked: boolean };
type PtNode = { alarm_name: string; alarm_config_id: string; classify_model: string; rule_desc: string; _checked: boolean };

/* ========== API 封装 ========== */
async function getJson(path: string): Promise<Dict> {
  return requestJson(path, { cache: "no-store" });
}
async function postJson(path: string, body: Dict): Promise<Dict> {
  if (draftOnly.value) throw new Error("会话草稿只能通过助手确认后保存");
  return requestJson(path, { method: "POST", body: JSON.stringify(body) });
}
async function putJson(path: string, body: Dict): Promise<Dict> {
  if (draftOnly.value) throw new Error("会话草稿只能通过助手确认后保存");
  return requestJson(path, { method: "PUT", body: JSON.stringify(body) });
}
async function deleteJson(path: string): Promise<Dict> {
  if (draftOnly.value) throw new Error("会话草稿只能通过助手确认后保存");
  return requestJson(path, { method: "DELETE" });
}
async function fetchCatalog(params: Dict): Promise<{ items: Dict[]; has_more: boolean; limit: number }> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && String(v) !== "") qs.set(k, String(v));
  const data = await getJson(`/api/plan-convergence/catalog?${qs.toString()}`);
  return {
    items: Array.isArray(data?.items) ? data.items : [],
    has_more: Boolean(data?.has_more),
    limit: Number(data?.limit || 0),
  };
}

/* ========== 规则集状态 ========== */
const sets = ref<Array<{ id: number; name: string; remark: string; item_count?: number }>>([]);
const setsLoading = ref(false);
const setsError = ref("");
const currentSetId = ref<number | null>(null);
const loadedVersion = ref('');
const hasCurrentSet = computed(() => draftOnly.value || Boolean(currentSetId.value));
const currentSetMeta = computed(() => {
  const s = sets.value.find((x) => x.id === currentSetId.value);
  return s ? `当前：${s.name || "未命名"}` : "";
});
const newSetName = ref("");
const name = ref("");
const remark = ref("");
const loadedName = ref("");
const loadedRemark = ref("");
const loadedItemsKey = ref("");
const drafts = ref<Draft[]>([]);
const mergeSel = ref<number[]>([]);
const busy = ref(false);
const saving = ref(false);
const expanding = ref(false);
const dirty = computed(() => {
  if (!currentSetId.value) return false;
  return name.value !== loadedName.value
    || remark.value !== loadedRemark.value
    || draftsSubmitKey(drafts.value) !== loadedItemsKey.value;
});
let applyingDraft = false;
watch(() => props.modelValue, value => {
  if (!value) return;
  if (String(value.name || "") === name.value && String(value.remark || "") === remark.value && submittedKey(value.items || []) === draftsSubmitKey(drafts.value)) return;
  applyingDraft = true;
  name.value = String(value.name || ""); remark.value = String(value.remark || "");
  drafts.value = restoreDrafts(value.items || []);
  mergeSel.value = [];
  applyingDraft = false;
}, { immediate: true });
watch([name, remark, () => draftsSubmitKey(drafts.value)], () => {
  if (draftOnly.value && !applyingDraft && !props.disabled) emit("update:modelValue", { ...props.modelValue, name: name.value, remark: remark.value, items: allDraftItems(drafts.value) });
}, { flush: "sync" });

async function loadSets(): Promise<void> {
  if (disposed) return;
  setsLoading.value = true; setsError.value = "";
  try {
    const data = await getJson("/api/plan-convergence/rulesets");
    if (disposed) return;
    const list = Array.isArray(data) ? data : (data?.items || []);
    sets.value = (list as Array<{ id: number; name: string; remark: string; item_count?: number }>) || [];
  } catch (e) { if (!disposed) setsError.value = errText(e); }
  finally { if (!disposed) setsLoading.value = false; }
}

async function loadSetDetail(id: number): Promise<void> {
  currentSetId.value = id; busy.value = true; setsError.value = "";
  try {
    const data = await getJson(`/api/plan-convergence/rulesets/${id}`);
    if (disposed) return;
    drafts.value = restoreDrafts((data.items || []) as Dict[]);
    loadedVersion.value = String(data.version || '');
    mergeSel.value = [];
    name.value = String(data.name || ""); remark.value = String(data.remark || "");
    loadedName.value = name.value; loadedRemark.value = remark.value;
    loadedItemsKey.value = draftsSubmitKey(drafts.value);
    await resetPicker();
  } catch (e) { if (!disposed) { setsError.value = errText(e); currentSetId.value = null; } }
  finally { if (!disposed) busy.value = false; }
}

async function selectSet(id: number): Promise<void> {
  if (saving.value || busy.value) return;
  if (id === currentSetId.value) return;
  if (dirty.value) {
    const ok = await askConfirm({
      tone: "warning", title: "有未保存的修改",
      message: "切换到其他规则集会丢弃当前未保存的修改，是否继续？",
    });
    if (!ok) return;
    if (saving.value || disposed) return;
  }
  await loadSetDetail(id);
  await loadSets();
}

async function reloadSet(): Promise<void> {
  if (!currentSetId.value || busy.value || saving.value) return;
  if (dirty.value && !await askConfirm({ tone: 'warning', title: '读取最新规则', message: '当前未保存修改将被替换，请先核对或备份需要保留的内容。是否继续？' })) return;
  if (!disposed && currentSetId.value) await loadSetDetail(currentSetId.value);
}

async function createSet(): Promise<void> {
  const n = newSetName.value.trim();
  if (!n) { errorMsg("请输入规则集名称"); return; }
  if (!isAdmin.value) { errorMsg("仅管理员可新建规则集"); return; }
  if (saving.value || busy.value) return;
  if (dirty.value) {
    const ok = await askConfirm({
      tone: "warning", title: "有未保存的修改",
      message: "新建规则集会丢弃当前未保存的修改，是否继续？",
    });
    if (!ok) return;
    if (saving.value || disposed) return;
  }
  busy.value = true;
  try {
    const r = await postJson("/api/plan-convergence/rulesets", { name: n, remark: "" });
    if (disposed) return;
    newSetName.value = "";
    await loadSets();
    await loadSetDetail(Number(r.id));
    successMsg("已创建规则集");
  } catch (e) { if (!disposed) errorMsg(errText(e)); }
  finally { if (!disposed) busy.value = false; }
}

/* 冻结取数：在发起保存瞬间锁定 payload，之后读取的基线不要受异步期间编辑影响。 */
async function saveSet(): Promise<void> {
  if (!currentSetId.value) { errorMsg("请先选择规则集"); return; }
  if (!isAdmin.value) { errorMsg("仅管理员可保存"); return; }
  if (saving.value || busy.value) return;
  if (!name.value.trim()) { errorMsg("规则集名称不能为空"); return; }

  const payloadName = name.value.trim();
  const payloadRemark = remark.value;
  const payloadItems = allDraftItems(drafts.value);
  // 允许清空后保存空规则集（clearAll 已确认；此处再次确认避免误清空）。
  if (!payloadItems.length) {
    const ok = await askConfirm({
      tone: "warning", title: "保存空规则集",
      message: "当前没有屏蔽条目，将把该规则集保存为空（清空全部条目）并写入服务器。是否继续？",
      confirmLabel: "保存空规则集",
    });
    if (!ok) return;
    if (saving.value || disposed) return;
  }

  saving.value = true;
  try {
    const saved = await putJson(`/api/plan-convergence/rulesets/${currentSetId.value}`, {
      name: payloadName, remark: payloadRemark, items: payloadItems, expected_version: loadedVersion.value,
    });
    if (disposed) return;
    loadedVersion.value = String(saved.version || '');
    loadedName.value = payloadName; loadedRemark.value = payloadRemark;
    loadedItemsKey.value = submittedKey(payloadItems);
    successMsg(payloadItems.length ? "已保存规则集" : "已保存空规则集");
    await loadSets();
  } catch (e) { if (!disposed) errorMsg(errText(e)); }
  finally { if (!disposed) saving.value = false; }
}

async function deleteSetById(id: number, setName: string): Promise<void> {
  if (!isAdmin.value || saving.value || busy.value) return;
  const ok = await askConfirm({
    tone: "danger", title: "删除规则集",
    message: `确定删除「${setName || "未命名规则集"}」以及其中全部条目吗？此操作不可恢复。`,
    details: [`规则集 ID：${id}`], confirmLabel: "删除",
  });
  if (!ok) return;
  if (saving.value || disposed) return;
  busy.value = true;
  try {
    await deleteJson(`/api/plan-convergence/rulesets/${id}`);
    if (disposed) return;
    if (currentSetId.value === id) {
      currentSetId.value = null; drafts.value = []; mergeSel.value = [];
      name.value = ""; remark.value = "";
      loadedName.value = ""; loadedRemark.value = ""; loadedItemsKey.value = "";
      newSetName.value = "";
      await resetPicker();
    }
    await loadSets();
    successMsg("已删除");
  } catch (e) { if (!disposed) errorMsg(errText(e)); }
  finally { if (!disposed) busy.value = false; }
}
async function deleteSet(): Promise<void> {
  if (!currentSetId.value) return;
  await deleteSetById(currentSetId.value, name.value || "未命名规则集");
}

/* ========== 选择器状态 ========== */
const objs = ref<ObjNode[]>([]);
const rooms = ref<RoomNode[]>([]);
const devs = ref<DevNode[]>([]);
const pts = ref<PtNode[]>([]);
/** 跨搜索重载保留的设备/规则选中（keyed by 唯一标识），避免搜索静默丢失选中。 */
const devSel = ref<DevNode[]>([]);
const ptSel = ref<PtNode[]>([]);
const drill = ref<Drill>({ zone: "", building: "", floor: "" });
const allObj = ref(false);
const allRoom = ref(false);
const allDev = ref(false);
const allPt = ref(false);

const objKw = ref(""); const roomKw = ref(""); const devKw = ref(""); const ptKw = ref("");
const objLoading = ref(false); const roomLoading = ref(false); const devLoading = ref(false); const ptLoading = ref(false);
const objError = ref(""); const roomError = ref(""); const devError = ref(""); const ptError = ref("");
const objHasMore = ref(false); const roomHasMore = ref(false); const devHasMore = ref(false); const ptHasMore = ref(false);

const objPage = ref(1); const roomPage = ref(1); const devPage = ref(1); const ptPage = ref(1);

/* 各列独立纪元：避免一列的请求使无关列响应作废、把另一列 spinner 卡住。 */
let objEpoch = 0, roomEpoch = 0, devEpoch = 0, ptEpoch = 0;
function nextColumnEpoch(col: "obj" | "room" | "dev" | "pt"): number {
  if (col === "obj") return ++objEpoch;
  if (col === "room") return ++roomEpoch;
  if (col === "dev") return ++devEpoch;
  return ++ptEpoch;
}

/* 生命周期后卸载标记：卸载后不再改任何状态。 */
let disposed = false;
const loadedDevSig = ref("");
const loadedPtSig = ref("");

watch(objKw, () => { objPage.value = 1; });
watch(roomKw, () => { roomPage.value = 1; });
watch(devKw, () => { devPage.value = 1; scheduleRemoteSearch("dev"); });
watch(ptKw, () => { ptPage.value = 1; scheduleRemoteSearch("pt"); });

/* 上下文检测 */
function selObjs(): string[] { return objs.value.filter((o) => o._checked).map((o) => o.obj_name); }
function selRooms(): RoomNode[] { return rooms.value.filter((r) => r._checked); }
function selDevInsts(): string[] { return devs.value.filter((d) => d._checked).map((d) => d.inst_name); }
function selPts(): PtNode[] { return pts.value.filter((p) => p._checked); }
const hasObjCtx = computed(() => allObj.value || selObjs().length > 0);
const hasRoomCtx = computed(() => allRoom.value || selRooms().length > 0);
const hasDevCtx = computed(() => allDev.value || selDevInsts().length > 0);
function objsParam(): string {
  const os = selObjs();
  return os.length ? os.join(",") : "all";
}
function pickerSig(): string {
  const rs = selRooms().map(roomNodeKey).join(",") || "*";
  return JSON.stringify({
    o: objsParam(), r: rs,
    z: drill.value.zone, b: drill.value.building, f: drill.value.floor,
    dk: devKw.value.trim(), pk: ptKw.value.trim(),
  });
}

/* 设备/规则 去重与保留选中合并 */
function devKey(d: DevNode): string { return d.ins_id || [d.inst_name, d.obj_name, d.position].join('|'); }
function ptKey(p: PtNode): string { return p.alarm_config_id || p.alarm_name || ""; }
function mergeDevSel(fresh: DevNode[]): void {
  const selByKey = new Map(devSel.value.map((d) => [devKey(d), d]));
  const out = fresh.map((n) => ({ ...n, _checked: selByKey.has(devKey(n)) }));
  const newKeys = new Set(out.map(devKey));
  devSel.value.forEach((d) => { if (!newKeys.has(devKey(d))) out.push(d); });
  devs.value = out;
  devSel.value = out.filter((d) => d._checked);
}
function mergePtSel(fresh: PtNode[]): void {
  const selByKey = new Map(ptSel.value.map((p) => [ptKey(p), p]));
  const out = fresh.map((n) => ({ ...n, _checked: selByKey.has(ptKey(n)) }));
  const newKeys = new Set(out.map(ptKey));
  ptSel.value.forEach((p) => { if (!newKeys.has(ptKey(p))) out.push(p); });
  pts.value = out;
  ptSel.value = out.filter((p) => p._checked);
}

/* catalog 请求 */
async function loadTypes(): Promise<void> {
  const myEpoch = nextColumnEpoch("obj");
  objLoading.value = true; objError.value = "";
  try {
    const res = await fetchCatalog({ kind: "types" });
    if (disposed || myEpoch !== objEpoch) return;
    objs.value = res.items.map((x) => ({ obj_name: String(x.obj_name || ""), devices: Number(x.devices || 0), _checked: false }));
    objHasMore.value = res.has_more;
  } catch (e) { if (!disposed && myEpoch === objEpoch) objError.value = errText(e); }
  finally { if (!disposed && myEpoch === objEpoch) objLoading.value = false; }
}

async function loadRoomLevel(carryChecked: RoomNode[] = []): Promise<void> {
  const myEpoch = nextColumnEpoch("room");
  roomLoading.value = true; roomError.value = ""; roomHasMore.value = false;
  try {
    const d = drill.value;
    let kind = "zones"; const params: Dict = {};
    if (!d.zone) kind = "zones";
    else if (!d.building) { kind = "buildings"; params.zone = d.zone; }
    else if (!d.floor) { kind = "floors"; params.zone = d.zone; params.building = d.building; }
    else { kind = "rooms"; params.zone = d.zone; params.building = d.building; params.floor = d.floor; }
    const res = await fetchCatalog({ kind, ...params });
    if (disposed || myEpoch !== roomEpoch) return;
    const level = kind === "zones" ? "zone" : kind === "buildings" ? "building" : kind === "floors" ? "floor" : "room";
    const mapped: RoomNode[] = res.items.map((it) => {
      const key = kind === "zones" ? String(it.zone || "") : kind === "buildings" ? String(it.building || "") : kind === "floors" ? String(it.floor || "") : String(it.room || "");
      return {
        level: level as RoomNode["level"], key, name: key, devices: Number(it.devices || 0), _checked: false,
        // 加载时的完整祖先空间，选中后长期保留、不随钻取位漂移
        zone: level === "building" ? d.zone : level === "floor" || level === "room" ? d.zone : "",
        building: level === "floor" || level === "room" ? d.building : "",
        floor: level === "room" ? d.floor : "",
      };
    });
    /* 跨级保留已勾选节点：先按完整空间键合并回新列表，再追加新列表未覆盖的旧选中节点（非静默丢弃）。 */
    const carryByKey = new Map(carryChecked.map((c) => [roomNodeKey(c), c]));
    mapped.forEach((n) => { const c = carryByKey.get(roomNodeKey(n)); if (c) n._checked = c._checked; });
    const newKeys = new Set(mapped.map(roomNodeKey));
    carryChecked.forEach((c) => { if (!newKeys.has(roomNodeKey(c))) mapped.push(c); });
    rooms.value = mapped;
    allRoom.value = false;
    roomHasMore.value = res.has_more;
    roomLoading.value = false;
    await cascadeFrom("room");
  } catch (e) { if (!disposed && myEpoch === roomEpoch) { roomError.value = errText(e); roomLoading.value = false; } }
}

async function loadDevicesForSig(sig: string, preserveSel: boolean): Promise<void> {
  if (disposed) return;
  if (!hasObjCtx.value && !hasRoomCtx.value) {
    devs.value = []; allDev.value = false; devHasMore.value = false; devSel.value = [];
    loadedDevSig.value = ""; return;
  }
  const myEpoch = nextColumnEpoch("dev");
  devLoading.value = true; devError.value = "";
  try {
    const params: Dict = { kind: "devices", objs: objsParam(), kw: devKw.value.trim() };
    const rs = selRooms();
    if (rs.length) {
      params.rooms = rs.map((r) => { const p = roomToSelParam(r); return [p.zone, p.building, p.floor, p.room].join("|"); }).join(",");
    } else {
      const d = drill.value;
      if (d.zone) params.zone = d.zone;
      if (d.building) params.building = d.building;
      if (d.floor) params.floor = d.floor;
    }
    const res = await fetchCatalog(params);
    if (disposed || myEpoch !== devEpoch) return;
    const fresh = res.items.map((x) => ({
      inst_name: String(x.inst_name || ""), ins_id: String(x.ins_id || ""),
      obj_name: String(x.obj_name || ""), position: String(x.position || ""), _checked: false,
    }));
    if (preserveSel) mergeDevSel(fresh);
    else { devSel.value = []; devs.value = fresh; }
    allDev.value = false; devHasMore.value = res.has_more; devLoading.value = false;
    loadedDevSig.value = sig;
  } catch (e) { if (!disposed && myEpoch === devEpoch) { devError.value = errText(e); devLoading.value = false; } }
}

async function loadRulesForSig(sig: string, preserveSel: boolean): Promise<void> {
  if (disposed) return;
  if (!hasObjCtx.value && !hasDevCtx.value && !hasRoomCtx.value) {
    pts.value = []; allPt.value = false; ptHasMore.value = false; ptSel.value = [];
    loadedPtSig.value = ""; return;
  }
  const myEpoch = nextColumnEpoch("pt");
  ptLoading.value = true; ptError.value = "";
  try {
    const params: Dict = { kind: "rules", kw: ptKw.value.trim() };
    const insts = selDevInsts();
    const objs = selObjs();
    const rs = selRooms();
    if (insts.length === 1) params.inst = insts[0];
    else if (objs.length === 1) params.obj = objs[0];
    else if (objs.length > 1) params.objs = objs.join(",");
    else if (rs.length) params.rooms = rs.map((r) => { const p = roomToSelParam(r); return [p.zone, p.building, p.floor, p.room].join("|"); }).join(",");
    const res = await fetchCatalog(params);
    if (disposed || myEpoch !== ptEpoch) return;
    const fresh = res.items.map((x) => ({
      alarm_name: String(x.alarm_name || ""), alarm_config_id: String(x.alarm_config_id || ""),
      classify_model: String(x.classify_model || ""), rule_desc: String(x.rule_desc || ""), _checked: false,
    }));
    if (preserveSel) mergePtSel(fresh);
    else { ptSel.value = []; pts.value = fresh; }
    allPt.value = false; ptHasMore.value = res.has_more; ptLoading.value = false;
    loadedPtSig.value = sig;
  } catch (e) { if (!disposed && myEpoch === ptEpoch) { ptError.value = errText(e); ptLoading.value = false; } }
}

async function cascadeFrom(source: "obj" | "room" | "dev"): Promise<void> {
  if (disposed) return;
  const sig = pickerSig();
  const clearContext = source === "obj" || source === "room";
  if (clearContext) {
    if (!hasObjCtx.value && !hasRoomCtx.value) {
      devs.value = []; pts.value = []; allDev.value = false; allPt.value = false;
      devSel.value = []; ptSel.value = []; devHasMore.value = false; ptHasMore.value = false;
      loadedDevSig.value = ""; loadedPtSig.value = "";
      return;
    }
    devSel.value = []; ptSel.value = [];
  }
  const needDev = hasObjCtx.value || hasRoomCtx.value;
  const needPt = hasObjCtx.value || hasDevCtx.value || hasRoomCtx.value;
  if (needDev && (clearContext || loadedDevSig.value !== sig) && source !== "dev") {
    await loadDevicesForSig(sig, false);
  }
  if (needPt && (source === "dev" || clearContext || loadedPtSig.value !== sig)) {
    await loadRulesForSig(sig, source === "dev" || clearContext ? false : false);
  }
}

let devTimer: ReturnType<typeof setTimeout> | undefined;
let ptTimer: ReturnType<typeof setTimeout> | undefined;
function scheduleRemoteSearch(col: "dev" | "pt"): void {
  if (col === "dev") { window.clearTimeout(devTimer); devTimer = window.setTimeout(() => { void reloadRemote(col); }, 350); }
  else { window.clearTimeout(ptTimer); ptTimer = window.setTimeout(() => { void reloadRemote(col); }, 350); }
}
/* 搜索重载：保留当前选中（不静默丢失）；仅重载本列，不影响其他列。 */
async function reloadRemote(col: "dev" | "pt"): Promise<void> {
  if (disposed) return;
  if (col === "dev") {
    loadedDevSig.value = "";
    if (hasObjCtx.value || hasRoomCtx.value) await loadDevicesForSig(pickerSig(), true);
    else { devs.value = []; devSel.value = []; }
  } else {
    loadedPtSig.value = "";
    if (hasObjCtx.value || hasDevCtx.value || hasRoomCtx.value) await loadRulesForSig(pickerSig(), true);
    else { pts.value = []; ptSel.value = []; }
  }
}

async function resetPicker(): Promise<void> {
  allObj.value = false; allRoom.value = false; allDev.value = false; allPt.value = false;
  objs.value.forEach((o) => (o._checked = false));
  rooms.value = []; devs.value = []; pts.value = [];
  devSel.value = []; ptSel.value = [];
  drill.value = { zone: "", building: "", floor: "" };
  objKw.value = ""; roomKw.value = ""; devKw.value = ""; ptKw.value = "";
  loadedDevSig.value = ""; loadedPtSig.value = "";
  devHasMore.value = false; ptHasMore.value = false; objHasMore.value = false; roomHasMore.value = false;
  objPage.value = roomPage.value = devPage.value = ptPage.value = 1;
  await loadRoomLevel([]);
}

async function drillDown(node: RoomNode): Promise<void> {
  if (saving.value || busy.value) return;
  /* 依据节点自身层级与不可变存储路径推导弹入目标，避免把保留的父级节点名误当当前层级键。 */
  if (node.level === "zone") {
    drill.value.zone = node.key; drill.value.building = ""; drill.value.floor = "";
  } else if (node.level === "building") {
    drill.value.zone = node.zone; drill.value.building = node.key; drill.value.floor = "";
  } else if (node.level === "floor") {
    drill.value.zone = node.zone; drill.value.building = node.building; drill.value.floor = node.key;
  } else {
    return; // room 不可再下钻（按钮未渲染此分支）
  }
  const carry = rooms.value.filter((r) => r._checked);
  roomKw.value = ""; rooms.value = []; devs.value = []; pts.value = [];
  devSel.value = []; ptSel.value = [];
  loadedDevSig.value = ""; loadedPtSig.value = ""; allDev.value = false; allPt.value = false;
  await loadRoomLevel(carry);
}

/* 精确面包屑：root / zone / building，恢复对应层列表并保留跨级已选空间。 */
async function crumbTo(kind: "root" | "zone" | "building"): Promise<void> {
  if (saving.value || busy.value) return;
  const d = drill.value;
  if (kind === "root") { d.zone = ""; d.building = ""; d.floor = ""; }
  else if (kind === "zone") { d.building = ""; d.floor = ""; }
  else if (kind === "building") { d.floor = ""; }
  const carry = rooms.value.filter((r) => r._checked);
  roomKw.value = ""; rooms.value = []; devs.value = []; pts.value = [];
  devSel.value = []; ptSel.value = [];
  loadedDevSig.value = ""; loadedPtSig.value = ""; allDev.value = false; allPt.value = false;
  await loadRoomLevel(carry);
}

/* 勾选事件：显式把浏览器勾选状态写入节点 _checked，再做级联。 */
function onObjCheck(o: ObjNode, e: Event): void {
  if (saving.value) return;
  o._checked = (e.target as HTMLInputElement).checked;
  allObj.value = false;
  void cascadeFrom("obj");
}
function onRoomCheck(r: RoomNode, e: Event): void {
  if (saving.value) return;
  r._checked = (e.target as HTMLInputElement).checked;
  allRoom.value = false;
  void cascadeFrom("room");
}
function onDevCheck(d: DevNode, e: Event): void {
  if (saving.value) return;
  d._checked = (e.target as HTMLInputElement).checked;
  allDev.value = false;
  devSel.value = devs.value.filter((x) => x._checked);
  void cascadeFrom("dev");
}
function onPtCheck(p: PtNode, e: Event): void {
  if (saving.value) return;
  p._checked = (e.target as HTMLInputElement).checked;
  allPt.value = false;
  ptSel.value = pts.value.filter((x) => x._checked);
}

/* 列筛选 / 分页 / 全选 */
const objsFiltered = computed(() => {
  const kw = objKw.value.trim().toLowerCase();
  return kw ? objs.value.filter((o) => o.obj_name.toLowerCase().includes(kw)) : objs.value;
});
const roomFiltered = computed(() => {
  const kw = roomKw.value.trim().toLowerCase();
  return kw ? rooms.value.filter((r) => r.name.toLowerCase().includes(kw)) : rooms.value;
});
const devFiltered = computed(() => {
  const kw = devKw.value.trim().toLowerCase();
  if (!kw) return devs.value;
  return devs.value.filter((d) => d.inst_name.toLowerCase().includes(kw) || d.obj_name.toLowerCase().includes(kw) || d.position.toLowerCase().includes(kw));
});
const ptFiltered = computed(() => {
  const kw = ptKw.value.trim().toLowerCase();
  if (!kw) return pts.value;
  return pts.value.filter((p) => p.alarm_name.toLowerCase().includes(kw) || p.classify_model.toLowerCase().includes(kw) || p.alarm_config_id.toLowerCase().includes(kw));
});

const objPagesCount = computed(() => Math.max(1, Math.ceil(objsFiltered.value.length / PICKER_PAGE)));
const roomPagesCount = computed(() => Math.max(1, Math.ceil(roomFiltered.value.length / PICKER_PAGE)));
const devPagesCount = computed(() => Math.max(1, Math.ceil(devFiltered.value.length / PICKER_PAGE)));
const ptPagesCount = computed(() => Math.max(1, Math.ceil(ptFiltered.value.length / PICKER_PAGE)));

const objPageItems = computed(() => objsFiltered.value.slice((objPage.value - 1) * PICKER_PAGE, objPage.value * PICKER_PAGE));
const roomPageItems = computed(() => roomFiltered.value.slice((roomPage.value - 1) * PICKER_PAGE, roomPage.value * PICKER_PAGE));
const devPageItems = computed(() => devFiltered.value.slice((devPage.value - 1) * PICKER_PAGE, devPage.value * PICKER_PAGE));
const ptPageItems = computed(() => ptFiltered.value.slice((ptPage.value - 1) * PICKER_PAGE, ptPage.value * PICKER_PAGE));

const objSelCount = computed(() => objs.value.filter((o) => o._checked).length);
const roomSelCount = computed(() => rooms.value.filter((r) => r._checked).length);
const devSelCount = computed(() => devs.value.filter((d) => d._checked).length);
const ptSelCount = computed(() => pts.value.filter((p) => p._checked).length);

const objAllChecked = computed(() => objsFiltered.value.length > 0 && objsFiltered.value.every((o) => o._checked));
const roomAllChecked = computed(() => roomFiltered.value.length > 0 && roomFiltered.value.every((r) => r._checked));
const devAllChecked = computed(() => devFiltered.value.length > 0 && devFiltered.value.every((d) => d._checked));
const ptAllChecked = computed(() => ptFiltered.value.length > 0 && ptFiltered.value.every((p) => p._checked));

/* 「全部」勾选操作在被筛选候选（已加载）范围内，不代表全量数据（has_more 时为子集）。 */
function toggleColAll(which: "obj" | "room" | "dev" | "pt"): void {
  if (!isAdmin.value || saving.value) return;
  if (which === "obj") { const checked = !objAllChecked.value; objsFiltered.value.forEach((o) => (o._checked = checked)); void cascadeFrom("obj"); }
  else if (which === "room") { const checked = !roomAllChecked.value; roomFiltered.value.forEach((r) => (r._checked = checked)); void cascadeFrom("room"); }
  else if (which === "dev") {
    const checked = !devAllChecked.value; devFiltered.value.forEach((d) => (d._checked = checked));
    devSel.value = devs.value.filter((d) => d._checked);
    void cascadeFrom("dev");
  } else {
    const checked = !ptAllChecked.value; ptFiltered.value.forEach((p) => (p._checked = checked));
    ptSel.value = pts.value.filter((p) => p._checked);
  }
}

/* ========== 添加屏蔽范围（与 legacy addEntry 语义一致） ========== */
async function addEntry(): Promise<void> {
  if (saving.value || busy.value) return;
  if (!hasCurrentSet.value) { errorMsg("请先选择或新建一个规则集"); return; }
  if (!isAdmin.value) { errorMsg("仅管理员可修改规则"); return; }
  if (allDev.value) { errorMsg("“全部设备”无法表示为屏蔽条目，请勾选具体设备"); return; }
  if (allPt.value) { errorMsg("“全部规则”无法表示为屏蔽条目，请勾选具体规则"); return; }
  const objs = selObjs();
  const rooms = selRooms();
  const insts = selDevInsts();
  const selectedDevices = devs.value.filter(d => d._checked);
  const pts = selPts();

  if (!objs.length && !rooms.length && !insts.length && !pts.length) { errorMsg("请先勾选内容"); return; }
  const count = selectionItemCount(objs.length, rooms.length, selectedDevices.length, pts.length);
  if (count > 500) {
    errorMsg(`当前选择将展开为 ${count.toLocaleString()} 项，单个规则集最多500项。请缩小设备或告警规则范围；当前选择和已有草稿已保留。`);
    return;
  }

  if (pts.length) {
    let items: Dict[] = [];
    if (insts.length) {
      /* 具体设备 × 规则：逐台生成，规则不挂错设备。 */
      selectedDevices.forEach((device) => pts.forEach((p) => items.push({
        scope_type: "point", inst_name: device.inst_name, ins_id: device.ins_id, obj_name: device.obj_name || "",
        point_name: p.alarm_name || p.alarm_config_id || "", rule_name: p.alarm_name || "",
        alarm_config_id: p.alarm_config_id || "",
      })));
    } else if (objs.length && rooms.length) {
      /* 类型 × 空间 × 规则：组合可表示为「单一类型上下文」才生成；多类型需交互拆分以避免范围意外扩大。 */
      if (objs.length > 1) {
        errorMsg("同时选择多个设备类型、空间与告警规则无法成组，避免范围意外扩大。建议：仅保留一个设备类型，或只选空间与规则，或只选设备与规则。");
        return;
      }
      const type = objs[0];
      rooms.forEach((r) => { const p0 = roomToSelParam(r); pts.forEach((p) => items.push({
        scope_type: "point", zone: p0.zone, building: p0.building, floor: p0.floor, room: p0.room, inst_name: "",
        obj_name: type, point_name: p.alarm_name || p.alarm_config_id || "", rule_name: p.alarm_name || "",
        alarm_config_id: p.alarm_config_id || "",
      })); });
    } else if (objs.length) {
      /* 类型 × 规则：每条规则按其 classify_model 映射到所选类型内的对应类型，保留上下文、防止范围意外扩大。 */
      const typeSet = new Set(objs);
      const skipped: string[] = [];
      pts.forEach((p) => {
        const model = (p.classify_model || "").trim();
        if (model && typeSet.has(model)) {
          items.push({
            scope_type: "point", obj_name: model, inst_name: "",
            point_name: p.alarm_name || p.alarm_config_id || "", rule_name: p.alarm_name || "",
            alarm_config_id: p.alarm_config_id || "",
          });
        } else if (!model) {
          skipped.push((p.alarm_name || p.alarm_config_id || "未命名规则") + "（缺少类型信息，无法安全映射）");
        } else {
          skipped.push((p.alarm_name || p.alarm_config_id || "未命名规则") + "（类型 " + model + " 未选）");
        }
      });
      if (!items.length) {
        errorMsg("所选告警规则均未匹配到已选设备类型（每条规则只映射到其 classify_model 对应类型的已选类型）。请勾选对应设备类型或具体设备后重试。");
        return;
      }
      if (skipped.length) errorMsg("已跳过 " + skipped.length + " 条未匹配的规则：" + skipped.slice(0, 3).join("；") + (skipped.length > 3 ? "…" : ""));
    } else if (rooms.length) {
      rooms.forEach((r) => { const p0 = roomToSelParam(r); pts.forEach((p) => items.push({
        scope_type: "point", zone: p0.zone, building: p0.building, floor: p0.floor, room: p0.room, inst_name: "",
        point_name: p.alarm_name || p.alarm_config_id || "", rule_name: p.alarm_name || "",
        alarm_config_id: p.alarm_config_id || "",
      })); });
    } else {
      pts.forEach((p) => items.push({
        scope_type: "point", inst_name: "",
        point_name: p.alarm_name || p.alarm_config_id || "", rule_name: p.alarm_name || "",
        alarm_config_id: p.alarm_config_id || "",
      }));
    }
    if (!items.length) return;
    await pushEntry(items);
    return;
  }

  if (insts.length) {
    const items = selectedDevices.map((device) => ({ scope_type: "device", inst_name: device.inst_name, ins_id: device.ins_id, obj_name: device.obj_name || "" }));
    await pushEntry(items);
    return;
  }

  if (rooms.length) {
    const items = rooms.flatMap((r) => {
      const p0 = roomToSelParam(r);
      if (objs.length) {
        /* 多类型 × 空间 → 生成 objtype_room（类型 × 空间）组合，而非丢弃类型上下文。 */
        return objs.map((n) => ({ scope_type: "objtype_room", obj_name: n, zone: p0.zone, building: p0.building, floor: p0.floor, room: p0.room }));
      }
      const st: string = r.level === "zone" ? "zone" : r.level === "building" ? "building" : r.level === "floor" ? "floor" : "room";
      return [{ scope_type: st, zone: p0.zone, building: p0.building, floor: p0.floor, room: p0.room }];
    });
    await pushEntry(items as Dict[]);
    return;
  }

  if (objs.length) {
    const items = objs.map((n) => ({ scope_type: "objtype", obj_name: n }));
    await pushEntry(items as Dict[]);
    return;
  }

  errorMsg("请先勾选内容");
}

async function pushEntry(items: Dict[]): Promise<void> {
  if (saving.value || busy.value) return;
  const existing = new Set(allDraftItems(drafts.value).map(itemKey));
  const fresh = items.filter((it) => { const key = itemKey(it); if (existing.has(key)) return false; existing.add(key); return true; });
  if (!fresh.length) { errorMsg("该范围已存在，请调整勾选"); return; }
  if (allDraftItems(drafts.value).length + fresh.length > 500) { errorMsg("单个规则集最多支持500项，请缩小选择范围；当前选择和已有草稿已保留"); return; }
  const label = entryLabel(fresh);
  drafts.value.push({ label, rule_type: "normal", items: fresh });
  await resetPicker();
  successMsg(`已添加 ${fresh.length} 条屏蔽范围（共 ${drafts.value.length} 条）`);
}

/* ========== 草稿组操作 ========== */
function toggleCommon(i: number): void {
  if (!isAdmin.value || saving.value) return;
  const d = drafts.value[i];
  if (!d) return;
  d.rule_type = d.rule_type === "common" ? "normal" : "common";
  if (d.rule_type === "common") mergeSel.value = mergeSel.value.filter((m) => m !== i);
  successMsg(d.rule_type === "common"
    ? "已设为公共：全部公共条目必须被匹配；且（如有）至少匹配一个普通条目。"
    : "已设为普通：至少匹配一个普通条目（若有公共条目，须先全部匹配）。");
}
function removeGroup(i: number): void {
  if (!isAdmin.value || saving.value) return;
  drafts.value.splice(i, 1);
  mergeSel.value = mergeSel.value.filter((m) => m !== i).map((m) => (m > i ? m - 1 : m));
}
function toggleMergeSel(i: number): void {
  if (saving.value) return;
  if (drafts.value[i]?.rule_type !== "normal") return;
  const idx = mergeSel.value.indexOf(i);
  if (idx >= 0) mergeSel.value.splice(idx, 1);
  else mergeSel.value.push(i);
}
function mergeSelected(): void {
  if (!isAdmin.value || saving.value) return;
  const sels = mergeSel.value.slice().sort((a, b) => a - b);
  const normals = sels.filter((i) => drafts.value[i]?.rule_type === "normal");
  if (normals.length < 2) return;
  const groups = normals.map((i) => drafts.value[i]);
  const seen = new Set<string>();
  const mergedItems: Dict[] = [];
  groups.forEach((g) => g.items.forEach((it) => { const k = itemKey(it); if (!seen.has(k)) { seen.add(k); mergedItems.push(it); } }));
  const label = groups.map((g) => g.label || "未命名").join(" + ");
  const target = normals[0];
  drafts.value[target] = { label, rule_type: "normal", items: mergedItems };
  normals.slice(1).sort((a, b) => b - a).forEach((i) => { drafts.value.splice(i, 1); });
  mergeSel.value = [];
  successMsg(`已合并 ${groups.length} 个普通条目`);
}
async function clearAll(): Promise<void> {
  if (!isAdmin.value || saving.value || busy.value) return;
  const ok = await askConfirm({
    tone: "danger", title: "清除全部内容",
    message: draftOnly.value ? "将清空会话草稿中的条目，确认助手操作清单后才会保存。是否继续？" : "将清除当前规则集的所有屏蔽条目与当前勾选（需点击保存后生效）。是否继续？",
    confirmLabel: "清除",
  });
  if (!ok) return;
  if (saving.value || disposed) return;
  drafts.value = []; mergeSel.value = [];
  await resetPicker();
  successMsg(draftOnly.value ? "草稿已清空，确认助手操作后生效" : "已清空，点击「保存规则集」后生效");
}

/* ========== 预览 / 详情 弹窗 ========== */
type PreviewState = {
  open: boolean;
  kind: "table" | "kv";
  title: string;
  headers?: string[];
  rows?: string[][];
  kv?: Array<{ label: string; value: string }>;
  total?: number;
  note?: string;
};
const preview = reactive<PreviewState>({ open: false, kind: "table", title: "" });
const previewPage = ref(1);
const previewRows = computed(() => {
  if (preview.kind !== "table" || !preview.rows) return [];
  const start = (previewPage.value - 1) * PREVIEW_PAGE;
  return preview.rows.slice(start, start + PREVIEW_PAGE);
});
const previewPages = computed(() => Math.max(1, Math.ceil((preview.total ?? (preview.rows?.length || 0)) / PREVIEW_PAGE)));
const previewKindNote = computed(() => preview.note || "");

let modalOwner: ReturnType<typeof acquireModal> | undefined;
let returnFocus: HTMLElement | null = null;
const modalElement = ref<HTMLElement | null>(null);

function openPreview(state: Omit<PreviewState, "open">): void {
  if (preview.open) return;
  previewTheme.value = inheritedControlTheme(rulesElement.value);
  previewPage.value = 1;
  Object.assign(preview, { ...state, open: true });
  returnFocus = document.activeElement as HTMLElement;
  modalOwner = acquireModal();
  window.addEventListener("keydown", modalKeydown, true);
  void nextTick(() => modalElement.value?.focus());
}
function closePreview(): void {
  if (!preview.open) return;
  preview.open = false;
  modalOwner?.release(); modalOwner = undefined;
  window.removeEventListener("keydown", modalKeydown, true);
  returnFocus?.focus();
}
function modalKeydown(event: KeyboardEvent): void {
  if (!modalOwner?.isTop(event, modalElement.value) || !modalElement.value) return;
  if (event.key === "Escape") { event.preventDefault(); event.stopImmediatePropagation(); closePreview(); }
  if (event.key === "Tab") {
    const nodes = Array.from(modalElement.value.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled),textarea:not(:disabled),a[href],[tabindex="0"]')).filter((n) => n.getClientRects().length);
    const first = nodes[0], last = nodes[nodes.length - 1];
    if (!modalElement.value.contains(document.activeElement) || (event.shiftKey ? document.activeElement === first : document.activeElement === last)) { event.preventDefault(); (event.shiftKey ? last : first)?.focus(); }
  }
}

function openGroupDetail(i: number): void {
  const d = drafts.value[i];
  if (!d) return;
  const label = d.label || `条目 ${i + 1}`;
  const headers = ["类型", "设备类型", "空间", "关联设备", "关联告警规则"];
  const rows = d.items.map((it) => {
    const sp = [it.zone, it.building, it.floor, it.room].filter(Boolean).join(" / ");
    const st = String(it.scope_type || "");
    const devType = it.obj_name || (st === "objtype" || st === "objtype_room" ? (entryKeyInfo(it) || "全部") : "全部类型");
    const device = it.inst_name ? String(it.inst_name) : sp ? "该空间下全部设备" : (st === "objtype" || st === "objtype_room" ? "该类型下全部设备" : "全部设备");
    const rule = (it.rule_name || it.point_name || "") + (it.alarm_config_id ? (" · ID " + it.alarm_config_id) : "");
    return [
      scopeTypeLabel(st) + (d.rule_type === "common" ? "（公共）" : ""),
      devType,
      sp || "—",
      device,
      rule || (st === "point" ? "规则名未标注（旧数据）" : "不限定（屏蔽全部规则）"),
    ];
  });
  openPreview({ kind: "table", title: `屏蔽条目 #${i + 1} · ${label}（${d.items.length} 项）`, headers, rows, total: rows.length });
}
function openDevDetail(d: DevNode): void {
  openPreview({
    kind: "kv", title: "设备详情",
    kv: [
      { label: "设备名称", value: d.inst_name },
      { label: "设备类型", value: d.obj_name },
      { label: "位置", value: d.position },
      { label: "唯一 ID", value: d.ins_id },
    ].filter((x) => String(x.value || "").trim() !== ""),
  });
}
function openPtDetail(p: PtNode): void {
  openPreview({
    kind: "kv", title: "告警规则详情",
    kv: [
      { label: "告警名称", value: p.alarm_name },
      { label: "告警配置 ID", value: p.alarm_config_id },
      { label: "类型", value: p.classify_model },
      { label: "规则说明", value: p.rule_desc },
    ].filter((x) => String(x.value || "").trim() !== ""),
  });
}
async function previewExpand(): Promise<void> {
  if (!currentSetId.value || saving.value) return;
  expanding.value = true;
  try {
    const data = await getJson(`/api/plan-convergence/rulesets/${currentSetId.value}/expand`);
    if (disposed) return;
    const devs2 = (data.devices || []) as Dict[];
    const count = Number(data.count || devs2.length || 0);
    openPreview({
      kind: "table", title: `规则集覆盖设备（共 ${count} 台）`,
      headers: ["设备", "唯一 ID", "类型", "位置"],
      rows: devs2.map((d) => [String(d.inst_name || ""), String(d.ins_id || ""), String(d.obj_name || ""), String(d.position || "")]),
      total: devs2.length,
      note: devs2.length < count ? "以下为展开接口返回的本页明细，总数以接口 count 为准。" : "",
    });
  } catch (e) { if (!disposed) errorMsg(errText(e)); }
  finally { if (!disposed) expanding.value = false; }
}

/* ========== 生命周期 ========== */
onMounted(async () => {
  if (!draftOnly.value) await loadSets();
  await loadTypes();
  await loadRoomLevel([]);
});
onBeforeUnmount(() => {
  disposed = true;
  window.clearTimeout(toastTimer);
  window.clearTimeout(devTimer);
  window.clearTimeout(ptTimer);
  modalOwner?.release(); modalOwner = undefined;
  window.removeEventListener("keydown", modalKeydown, true);
  confirm.open = false;
  confirmResolve?.(false); confirmResolve = null;
});
</script>

<style scoped>
.pc-rules {
  display: flex;
  flex-direction: column;
  gap: 12px;
  font-size: 14px;
}
.editor-fields { border: 0; margin: 0; padding: 0; min-width: 0; display: flex; flex-direction: column; gap: 12px; }
/* 紧凑工具栏（不再重复页面大标题） */
.toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.badge {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 1px 8px; border-radius: 999px;
  background: var(--cf-surface-2, #eef2f7); color: var(--cf-text-2, #52606d);
  font-size: 12px; line-height: 20px; border: 1px solid transparent;
  white-space: nowrap;
}
.badge.common-badge { background: #fff7e6; color: #ad6800; border-color: #ffd591; cursor: pointer; }
.toast {
  position: fixed; top: 16px; right: 16px; z-index: 2000;
  padding: 9px 14px; border-radius: 8px; font-size: 13px;
  display: flex; align-items: flex-start; gap: 10px; max-width: min(520px, calc(100% - 32px)); overflow-wrap: anywhere;
  box-shadow: 0 6px 18px rgba(0,0,0,.14);
}
.toast.success { background: #e6fffb; color: #00675b; border: 1px solid #8ce9de; }
.toast.error { background: #fff1f0; color: #a4150d; border: 1px solid #ffa39e; }
/* 无边框分离列，避免卡片套卡片 */
.workspace {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr) 340px;
  gap: 12px;
  align-items: start;
}
.panel { min-width: 0; }
.panel-title {
  display: flex; justify-content: space-between; align-items: center;
  gap: 8px; margin-bottom: 8px;
}
.panel-title h3 { margin: 0; font-size: 14px; }
.actions { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.muted { color: var(--cf-text-2, #68737f); }
.alert {
  display: flex; gap: 6px; align-items: flex-start;
  padding: 7px 9px; border-radius: 7px; font-size: 12.5px; margin: 6px 0;
}
.alert.error { background: #fff1f0; color: #a4150d; border: 1px solid #ffa39e; }
.empty { padding: 14px 4px; text-align: center; font-size: 13px; }
.hint { color: var(--cf-text-2, #68737f); font-size: 12.5px; padding: 4px 2px; }
.hint.center { text-align: center; padding: 20px 4px; }
.spin { animation: pcSpin 1s linear infinite; }
@keyframes pcSpin { to { transform: rotate(360deg); } }

/* ===== 规则集列表 ===== */
.set-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.set-list li { display: flex; align-items: center; gap: 4px; }
.set-list li.active .set-row { border-color: var(--cf-accent, #2f6fed); background: #f5f8ff; }
.set-row {
  flex: 1; min-width: 0; text-align: left; display: flex; flex-direction: column; gap: 3px;
  border: 1px solid var(--cf-border, #e3e8ef); border-radius: 8px;
  padding: 7px 9px; background: transparent; cursor: pointer;
}
.set-row:disabled { opacity: .7; cursor: default; }
.set-name { font-weight: 600; font-size: 13.5px; word-break: break-all; }
.set-meta { display: flex; align-items: center; gap: 6px; }
.set-meta small { font-size: 12px; }
.new-set-form { display: flex; gap: 6px; margin-bottom: 8px; }
.new-set-form input { flex: 1; }

/* ===== 选择器 ===== */
.set-name-inline { display: inline-block; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.set-fields {
  display: grid; grid-template-columns: 1fr 1.4fr; gap: 8px;
  margin-bottom: 10px;
}
.set-fields label { display: flex; flex-direction: column; gap: 4px; font-size: 12.5px; color: var(--cf-text-2, #68737f); }
input, textarea {
  width: 100%; box-sizing: border-box;
  border: 1px solid var(--cf-border, #d4dae2); border-radius: 7px;
  padding: 7px 9px; font-size: 13px; background: var(--cf-surface-1, #fff);
  font-family: inherit;
}
input:disabled, textarea:disabled { background: var(--cf-surface-2, #f2f4f7); color: var(--cf-text-2, #68737f); }
button { font-family: inherit; }
.picker-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.picker-col {
  border-radius: 8px; padding: 6px; display: flex; flex-direction: column; gap: 6px;
  min-width: 0;
}
.col-head { display: flex; justify-content: space-between; align-items: center; gap: 6px; }
.col-all { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; cursor: pointer; }
.col-all input { width: auto; }
.col-search { padding: 5px 8px; font-size: 12.5px; }
.cand-list { list-style: none; margin: 0; padding: 0; max-height: 340px; overflow: auto; display: flex; flex-direction: column; border-block: 1px solid var(--cf-border, #eef1f5); }
.cand-list li { display: flex; align-items: center; gap: 4px; border-bottom: 1px solid var(--cf-border, #eef1f5); }
.cand {
  display: flex; align-items: center; gap: 6px; flex: 1; min-width: 0;
  padding: 4px 2px; cursor: pointer;
}
.cand input { width: auto; flex: none; }
.cand-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12.8px; }
.cand-count { color: var(--cf-text-2, #68737f); font-size: 11.5px; flex: none; }
.link-button { border: none; background: none; color: var(--cf-accent, #2f6fed); font-size: 12px; cursor: pointer; padding: 2px; flex: none; }
.icon-button, .command-button {
  border: 1px solid var(--cf-border, #d4dae2); background: var(--cf-surface-1, #fff);
  border-radius: 7px; padding: 4px 8px; cursor: pointer; line-height: 1.25; white-space: nowrap; min-height: 28px; flex-shrink: 0;
  display: inline-flex; align-items: center; justify-content: center; gap: 4px;
}
.icon-button:disabled, .command-button:disabled { opacity: .5; cursor: default; }
.icon-button.danger { color: #c0392b; }
button.danger-text { color: #c0392b; }
.text-btn { border-color: transparent; background: transparent; }
button.primary {
  background: var(--cf-accent, #2f6fed); color: #fff; border: none; border-radius: 7px;
  padding: 7px 10px; cursor: pointer; display: inline-flex; align-items: center; gap: 5px; font-size: 13px;
}
button.primary:disabled { opacity: .5; cursor: default; }
.drill { flex: none; }
.breadcrumb { display: flex; flex-wrap: wrap; align-items: center; gap: 2px; font-size: 12.5px; }
.breadcrumb button { border: none; background: none; color: var(--cf-accent, #2f6fed); cursor: pointer; padding: 1px 3px; font-size: 12.5px; }
.breadcrumb button:disabled { opacity: .6; cursor: default; }
.crumb-cur { color: var(--cf-text-2, #68737f); padding: 1px 3px; cursor: default; }
.sep { color: var(--cf-border-strong, #b9c2cd); }
.pagination { display: flex; align-items: center; justify-content: flex-end; gap: 6px; font-size: 12px; color: var(--cf-text-2, #68737f); }

/* ===== 草稿组 ===== */
.draft-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 6px; max-height: 520px; overflow: auto; }
.draft-list li {
  border: 1px solid var(--cf-border, #e3e8ef); border-radius: 8px; padding: 6px 8px;
  display: flex; flex-direction: column; gap: 4px;
}
.draft-list li.selected { border-color: var(--cf-accent, #2f6fed); background: #f5f8ff; }
.draft-list li.common { border-left: 3px solid #faad14; }
.draft-head { display: flex; align-items: center; gap: 5px; }
.merge-check.on { background: var(--cf-accent, #2f6fed); color: #fff; }
.draft-idx { font-weight: 700; color: var(--cf-text-2, #68737f); font-size: 12.5px; }
.draft-label { flex: 1; min-width: 0; font-size: 13px; padding: 3px 4px; }
.draft-summary { margin: 0; font-size: 12px; color: var(--cf-text-2, #68737f); }

/* ===== 弹窗 ===== */
.pc-modal-overlay {
  position: fixed; inset: 0; background: rgba(15, 23, 42, 0.45);
  display: flex; align-items: center; justify-content: center; z-index: 1200; padding: 16px;
}
.pc-modal {
  background: var(--cf-surface-1, #fff); color: var(--lh-charcoal, inherit); border-radius: 12px; width: min(760px, 100%);
  max-height: 82vh; display: flex; flex-direction: column; overflow: hidden;
  box-shadow: 0 18px 50px rgba(0,0,0,.22); outline: none;
}
.pc-modal header {
  display: flex; justify-content: space-between; align-items: center; gap: 8px;
  padding: 10px 12px; border-bottom: 1px solid var(--cf-border, #eef1f5);
}
.pc-modal header h2 { margin: 0; font-size: 14.5px; word-break: break-all; }
.pc-modal-scroll { padding: 10px 12px; overflow: auto; min-height: 80px; }
.pc-modal footer {
  display: flex; justify-content: space-between; align-items: center; gap: 8px;
  padding: 9px 12px; border-top: 1px solid var(--cf-border, #eef1f5);
}
.table-wrap {
  width: 100%; border-collapse: collapse; font-size: 12.5px; margin: 4px 0;
}
.table-wrap th, .table-wrap td {
  border: 1px solid var(--cf-border, #eef1f5); padding: 5px 7px; text-align: left; vertical-align: top;
  word-break: break-word;
}
.table-wrap th { background: var(--cf-surface-2, #f2f4f7); font-weight: 600; }
.kv-list { display: grid; grid-template-columns: 120px 1fr; gap: 6px 10px; margin: 6px 0 2px; font-size: 12.8px; }
.kv-list dt { color: var(--cf-text-2, #68737f); }
.kv-list dd { margin: 0; word-break: break-word; }
.draft-only { --cf-surface-1: var(--lh-surface); --cf-surface-2: var(--lh-surface-subtle); --cf-border: var(--lh-input-border); --cf-text-2: var(--lh-muted); --cf-accent: var(--lh-accent); }
.draft-only .workspace { grid-template-columns: minmax(0, 1fr); }
.draft-only .picker-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.draft-only .toast { position: static; box-shadow: none; max-width: 100%; }
.draft-only .cand-list { max-height: 190px; }
.draft-only .draft-list { max-height: 320px; }
.draft-only input, .draft-only textarea { color: inherit; }
.draft-only button.primary, .draft-only .merge-check.on { color: #162519; }
.draft-only .draft-list li.selected { background: var(--lh-accent-soft); }
.draft-only .badge.common-badge { background: var(--lh-warn-soft); color: var(--lh-warn); border-color: var(--lh-warn); }
.draft-only .toast.success { background: var(--lh-accent-soft); color: var(--lh-accent-strong); border-color: var(--lh-accent); }
.draft-only .toast.error, .draft-only .alert.error { background: var(--lh-danger-soft); color: var(--lh-danger); border-color: var(--lh-danger); }
.draft-only .icon-button.danger, .draft-only .danger-text { color: var(--lh-danger); }
.pc-modal-overlay .primary { color: var(--lh-surface, #fff); }

@media (max-width: 1280px) {
  .workspace { grid-template-columns: 240px minmax(0, 1fr); }
  .draft-panel { grid-column: 1 / -1; }
  .picker-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 820px) {
  .workspace { grid-template-columns: minmax(0, 1fr); }
  .set-panel, .draft-panel { grid-column: 1 / -1; }
  .set-fields { grid-template-columns: 1fr; }
}
@media (max-width: 640px) {
  .picker-grid { grid-template-columns: minmax(0, 1fr); }
  .draft-only .picker-grid { grid-template-columns: minmax(0, 1fr); }
}
</style>
