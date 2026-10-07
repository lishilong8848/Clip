<template>
  <div class="lhp-proof" :class="{ disabled }">
    <template v-if="completionMode">
      <section class="lhp-select" aria-label="楼栋">
        <label class="lhp-label" :for="props.id + '-scope'">楼栋</label>
        <VnetSelect
          :input-id="props.id + '-scope'"
          :model-value="currentScopeLabel"
          :options="scopeLabels"
          :label="'楼栋'"
          :placeholder="'请选择楼栋'"
          :disabled="scopeDisabled"
          :menu-z-index="10010"
          @update:model-value="selectScope"
        />
      </section>
      <div class="lhp-dir-row">
        <button
          type="button"
          class="lhp-input lhp-dir-btn"
          :aria-label="'读取机柜目录'"
          :disabled="directoryBtnDisabled"
          @click="emitLoadOptions"
        >读取机柜目录</button>
      </div>
    </template>

    <section class="lhp-select" aria-label="证明图片">
      <label class="lhp-label" :for="props.id + '-image'">证明图片</label>
      <div class="lhp-image-row">
        <VnetSelect
          :input-id="props.id + '-image'"
          :model-value="currentImageLabel"
          :options="imageLabels"
          :label="'证明图片'"
          :placeholder="'请选择证明图片'"
          :disabled="disabled"
          :menu-z-index="10010"
          @update:model-value="selectImage"
        />
        <span v-if="currentImage && imageThumb" class="lhp-thumb"><img :src="imageThumb" alt="" loading="lazy" /></span>
        <button
          v-if="previewable"
          type="button"
          class="lhp-icon-btn"
          :aria-label="'查看原图'"
          title="查看原图"
          :disabled="disabled"
          @click="openPreview"
        >
          <ZoomIn :size="18" aria-hidden="true" />
        </button>
      </div>
    </section>

    <section class="lhp-select" aria-label="识别候选">
      <label class="lhp-label" :for="props.id + '-candidate'">识别候选</label>
      <VnetSelect
        :input-id="props.id + '-candidate'"
        :model-value="currentCandidateLabel"
        :options="candidateLabels"
        :label="'识别候选'"
        :placeholder="'请选择识别候选'"
        :disabled="disabled"
        :menu-z-index="10010"
        @update:model-value="selectCandidate"
      />
    </section>

    <section class="lhp-select" aria-label="目标机柜">
      <label class="lhp-label" :for="props.id + '-rack'">目标机柜</label>
      <VnetSelect
        :input-id="props.id + '-rack'"
        :model-value="currentRowLabel"
        :options="rowLabels"
        :label="'目标机柜'"
        :placeholder="'请选择机柜'"
        :disabled="rowSelectDisabled"
        :menu-z-index="10010"
        @update:model-value="selectRow"
      />
    </section>

    <template v-if="!completionMode">
      <p v-if="local.attach && mismatchNotice" class="lhp-notice lhp-mismatch" role="status">
        <ShieldAlert :size="14" aria-hidden="true" /><span>识别机柜与所选机柜不一致，仅关联证明，不采用识别内容</span>
      </p>
      <p v-else-if="local.attach && candidateReviewOnly" class="lhp-notice" role="status">
        <Link2Off :size="14" aria-hidden="true" /><span>仅关联证明，不采用识别内容</span>
      </p>
      <p v-if="!local.attach && local.rowId" class="lhp-notice" role="status">
        <Link2Off :size="14" aria-hidden="true" /><span>仅移除本柜截图关联，不改操作内容</span>
      </p>
    </template>

    <section class="lhp-fields" aria-label="机柜字段">
      <label class="lhp-label" :for="props.id + '-action'">操作类型</label>
      <select :id="props.id + '-action'" class="lhp-input" :value="local.fields.action || ''" :disabled="fieldDisabled" :aria-label="'操作类型'" @change="onField('action', $event)">
        <option value="">请选择</option>
        <option v-for="action in actions" :key="action" :value="action">{{ action }}</option>
      </select>

      <label class="lhp-label" :for="props.id + '-expected'">期望完成时间</label>
      <input :id="props.id + '-expected'" class="lhp-input" type="datetime-local" step="1" :value="localDateTime(local.fields.expected)" :disabled="fieldDisabled" :aria-label="'期望完成时间'" @input="onField('expected', $event)" />

      <label class="lhp-label" :for="props.id + '-actual'">实际完成时间</label>
      <input :id="props.id + '-actual'" class="lhp-input" type="datetime-local" step="1" :value="localDateTime(local.fields.actual)" :disabled="fieldDisabled" :aria-label="'实际完成时间'" @input="onField('actual', $event)" />

      <label class="lhp-label" :for="props.id + '-supplier'">供应商机柜号</label>
      <input :id="props.id + '-supplier'" class="lhp-input" type="text" :value="local.fields.supplier_rack || ''" :disabled="fieldDisabled" maxlength="200" :aria-label="'供应商机柜号'" @input="onField('supplier_rack', $event)" />

      <template v-if="completionMode">
        <label class="lhp-label" :for="props.id + '-rack-type'">机柜类型</label>
        <input :id="props.id + '-rack-type'" class="lhp-input" type="text" :value="currentRow?.rack_type || ''" readonly :disabled="fieldDisabled" :aria-label="'机柜类型'" />
      </template>

      <label class="lhp-label" :for="props.id + '-result'">结果</label>
      <select :id="props.id + '-result'" class="lhp-input" :value="local.fields.result || ''" :disabled="fieldDisabled" :aria-label="'结果'" @change="onField('result', $event)">
        <option value="">请选择</option>
        <option>成功</option>
        <option>失败</option>
      </select>

      <template v-if="local.fields.result === '失败'">
        <label class="lhp-label" :for="props.id + '-failure'">失败原因</label>
        <input :id="props.id + '-failure'" class="lhp-input" type="text" :value="local.fields.failure_reason || ''" :disabled="fieldDisabled" maxlength="1000" required :placeholder="'失败原因'" :aria-label="'失败原因'" @input="onField('failure_reason', $event)" />
      </template>

      <template v-if="completionMode">
        <label class="lhp-label" :for="props.id + '-type-detail'">类型明细</label>
        <input :id="props.id + '-type-detail'" class="lhp-input" type="text" :value="local.fields.type_detail || ''" :disabled="fieldDisabled" maxlength="500" :placeholder="'类型明细'" :aria-label="'类型明细'" @input="onField('type_detail', $event)" />
      </template>
    </section>

    <section v-if="!completionMode" class="lhp-options" aria-label="关联与核对选项">
      <label class="lhp-check"><input type="checkbox" :checked="local.attach" :disabled="disabled" :aria-label="'关联证明'" @change="onAttach" /><span>关联证明</span></label>
      <label class="lhp-check"><input type="checkbox" :checked="local.reviewTimes" :disabled="reviewDisabled" :aria-label="'核对时间'" @change="onReviewTimes" /><span>核对时间</span></label>
      <label class="lhp-check"><input type="checkbox" :checked="local.reviewBusiness" :disabled="reviewDisabled" :aria-label="'核对操作与结果'" @change="onReviewBusiness" /><span>核对操作与结果</span></label>
    </section>

    <Teleport to="body">
    <dialog ref="previewDialog" class="lhp-preview" aria-label="证明原图预览" @click.self="closePreview" @keydown.esc.stop="closePreview">
      <div class="lhp-preview-head">
        <span class="lhp-preview-title">证明原图</span>
        <button type="button" class="lhp-icon-btn" :aria-label="'关闭预览'" title="关闭预览" @click="closePreview"><X :size="20" aria-hidden="true" /></button>
      </div>
      <div class="lhp-preview-body">
        <img v-if="previewUrl" :src="previewUrl" alt="证明原图" />
      </div>
    </dialog>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { Link2Off, ShieldAlert, X, ZoomIn } from "lucide-vue-next";
import VnetSelect from "./VnetSelect.vue";

const props = defineProps<{ field: any; modelValue?: any; disabled?: boolean; id: string }>();
const emit = defineEmits<{
  (e: "update:modelValue", value: any): void;
  (e: "load-options", payload: { scope: string }): void;
}>();

const fieldImages = computed(() => (Array.isArray(props.field?.images) ? props.field.images : []));
const fieldRows = computed(() => (Array.isArray(props.field?.rows) ? props.field.rows : []));
const directoryRows = computed(() => {
  if (!completionMode.value) return fieldRows.value;
  return fieldRows.value.filter((r: any) => String(r.scope ?? "") === local.scope);
});
const actions = computed(() => (Array.isArray(props.field?.actions) ? props.field.actions : []));
const fieldScopes = computed(() => (Array.isArray(props.field?.scopes) ? props.field.scopes : []));
const directoryScope = computed(() => String(props.field?.directory_scope ?? ""));

const completionMode = computed(() => props.field?.native_cabinet_correct === true);

const local = reactive({
  scope: "",
  imageId: "",
  candidateIndex: -1 as number,
  rowId: "",
  fields: {} as Record<string, any>,
  attach: true,
  reviewTimes: false,
  reviewBusiness: false,
});

const directoryAligned = computed(
  () => !completionMode.value || (Boolean(local.scope) && directoryScope.value === local.scope)
);

function tripleKey(scope: any, room: any, rack: any): string {
  return [String(scope ?? ""), String(room ?? ""), String(rack ?? "")].join("/");
}
function tripleMatches(a: any, b: any): boolean {
  return tripleKey(a?.scope, a?.room, a?.rack) === tripleKey(b?.scope, b?.room, b?.rack);
}
function findRowByTriple(scope: any, room: any, rack: any): any | null {
  const key = tripleKey(scope, room, rack);
  const matches = directoryRows.value.filter((r: any) => tripleKey(r.scope, r.room, r.rack) === key);
  return matches.length === 1 ? matches[0] : null;
}
let lastEmitted: any = null;

function cloneValue(v: any): any {
  return v == null ? null : JSON.parse(JSON.stringify(v));
}

function syncFromModel(): void {
  const v = props.modelValue || {};
  if (completionMode.value) {
    local.scope = String(v.scope ?? "");
    local.imageId = String(v.image_id ?? "");
    const ci = v.candidate_index;
    local.candidateIndex = ci == null || ci === "" ? -1 : Number(ci);
    local.rowId = String(v.row_id ?? "");
    local.fields = cloneValue(v.fields) || {};
    local.attach = true;
    local.reviewTimes = false;
    local.reviewBusiness = false;
    return;
  }
  local.imageId = String(v.image_id ?? "");
  const ci = v.candidate_index;
  local.candidateIndex = ci == null || ci === "" ? -1 : Number(ci);
  local.rowId = String(v.row_id ?? "");
  local.fields = cloneValue(v.fields) || {};
  local.attach = typeof v.attach === "boolean" ? v.attach : true;
  local.reviewTimes = v.review_times === true;
  local.reviewBusiness = v.review_business === true;
}

watch(
  () => props.modelValue,
  (v) => {
    if (v !== lastEmitted) syncFromModel();
  },
  { immediate: true }
);

watch(fieldRows, () => {
  reconcileDirectory();
});

const currentImage = computed(() => fieldImages.value.find((i: any) => i.image_id === local.imageId) || null);
const currentCandidate = computed(() => {
  const img = currentImage.value;
  if (!img || local.candidateIndex < 0) return null;
  return (img.candidates || []).find((c: any) => c.index === local.candidateIndex) || null;
});
const currentRow = computed(() => directoryRows.value.find((r: any) => r.row_id === local.rowId) || null);

function uniqueLabels(list: any[], getLabel: (x: any) => string): Array<{ label: string; raw: any }> {
  const used = new Set<string>();
  const out: Array<{ label: string; raw: any }> = [];
  for (const item of list) {
    const base = String(getLabel(item) || "").trim() || "未命名";
    let label = base;
    let n = 1;
    while (used.has(label)) {
      n += 1;
      label = `${base} #${n}`;
    }
    used.add(label);
    out.push({ label, raw: item });
  }
  return out;
}

const imageOptions = computed(() =>
  fieldImages.value.map((img: any, i: number) => ({
    label: `${i + 1}. ${String(img.name || "").trim() || "未命名"}`,
    value: String(img.image_id),
  }))
);
const imageLabels = computed(() => imageOptions.value.map((o: any) => o.label));
const currentImageLabel = computed(() => imageOptions.value.find((o: any) => o.value === local.imageId)?.label || "");

const scopeOptions = computed(() => fieldScopes.value.map((s: any) => String(s).trim()).filter(Boolean));
const scopeLabels = computed(() => scopeOptions.value.map((o: string) => o));
const currentScopeLabel = computed(() => scopeOptions.value.includes(local.scope) ? local.scope : "");

const candidateOptions = computed(() => {
  const list: any[] = [{ index: -1, label: completionMode.value ? "手工补全" : "仅关联证明" }];
  const img = currentImage.value;
  if (img) {
    for (const c of img.candidates || []) {
      list.push({ index: c.index, label: c.label || `${c.scope || ""} ${c.room || ""}/${c.rack || ""}` });
    }
  }
  return uniqueLabels(list, (c) => c.label).map((o: any) => ({ label: o.label, value: Number(o.raw.index) }));
});
const candidateLabels = computed(() => candidateOptions.value.map((o) => o.label));
const currentCandidateLabel = computed(() => candidateOptions.value.find((o) => o.value === local.candidateIndex)?.label || "");

const rowOptions = computed(() =>
  uniqueLabels(directoryRows.value, (r: any) => r.label || `${r.scope || ""} ${r.room || ""}/${r.rack || ""}`).map((o: any) => ({
    label: o.label,
    value: String(o.raw.row_id),
  }))
);
const rowLabels = computed(() => rowOptions.value.map((o) => o.label));
const currentRowLabel = computed(() => rowOptions.value.find((o) => o.value === local.rowId)?.label || "");

const candidateReviewOnly = computed(() => local.candidateIndex === -1);
const mismatchNotice = computed(() => {
  const row = currentRow.value;
  const cand = currentCandidate.value;
  if (!row || !cand || local.candidateIndex < 0) return "";
  const same =
    [String(cand.scope), String(cand.room), String(cand.rack)].join("/") ===
    [String(row.scope), String(row.room), String(row.rack)].join("/");
  return same ? "" : "mismatch";
});

const fieldDisabled = computed(() => {
  if (props.disabled) return true;
  if (!completionMode.value) return !local.attach || !local.rowId;
  return !directoryAligned.value || !local.rowId || !currentRow.value;
});
const reviewDisabled = computed(() => props.disabled || !local.attach || !local.rowId);

const scopeDisabled = computed(() => props.disabled);
const rowSelectDisabled = computed(
  () => props.disabled || (completionMode.value && !directoryAligned.value)
);
const directoryBtnDisabled = computed(
  () => props.disabled || !local.scope || !scopeOptions.value.includes(local.scope)
);

function emitLoadOptions(): void {
  if (props.disabled) return;
  const scope = currentScopeLabel.value;
  if (!scope) return;
  emit("load-options", { scope });
}

const imageThumb = computed(() => safeUrl(String(currentImage.value?.thumbnail_url || "")));
const previewable = computed(() => Boolean(safeUrl(String(currentImage.value?.url || ""))));

function resetReview(): void {
  local.reviewTimes = false;
  local.reviewBusiness = false;
}

function selectImage(label: string): void {
  const opt = imageOptions.value.find((o: any) => o.label === label);
  if (!opt) return;
  local.imageId = opt.value;
  local.candidateIndex = -1;
  local.rowId = "";
  resetReview();
  rebuildFields();
  emitUpdate();
}

function selectScope(label: string): void {
  if (completionMode.value) {
    const scope = scopeOptions.value.find((o: string) => o === label);
    if (!scope) return;
    local.scope = scope;
    local.rowId = "";
    local.fields = {};
    resetReview();
    emitUpdate();
    emitLoadOptions();
    return;
  }
  // 非补全模式无楼栋选择，忽略
}

function selectCandidate(label: string): void {
  const opt = candidateOptions.value.find((o) => o.label === label);
  if (!opt) return;
  local.candidateIndex = Number(opt.value);
  if (local.candidateIndex >= 0 && completionMode.value) {
    const c = currentImage.value?.candidates?.find((x: any) => x.index === local.candidateIndex);
    if (directoryAligned.value && c) {
      let row = directoryRows.value.find((r: any) => r.row_id === c.row_id) || null;
      if (!row) row = findRowByTriple(c.scope, c.room, c.rack);
      local.rowId = row ? String(row.row_id) : "";
    } else {
      local.rowId = "";
    }
  } else if (local.candidateIndex >= 0) {
    const c = currentImage.value?.candidates?.find((x: any) => x.index === local.candidateIndex);
    const rowExists = c && fieldRows.value.some((r: any) => r.row_id === c.row_id);
    local.rowId = rowExists ? String(c.row_id) : "";
  }
  resetReview();
  rebuildFields();
  emitUpdate();
}

function selectRow(label: string): void {
  const opt = rowOptions.value.find((o) => o.label === label);
  if (!opt) return;
  local.rowId = String(opt.value);
  resetReview();
  rebuildFields();
  emitUpdate();
}

function rebuildCompletionFields(): void {
  const row = currentRow.value;
  const base: Record<string, any> = row?.fields ? cloneValue(row.fields) : {};
  const cand = currentCandidate.value;
  if (row && cand && local.candidateIndex >= 0 && tripleMatches(cand, row)) {
    const c = cand.fields || {};
    for (const key of ["expected", "actual", "action", "supplier_rack", "type_detail"]) {
      const value = String(c[key] ?? "").trim();
      if (value) base[key] = value;
    }
    const result = String(c.result ?? "").trim();
    if (result === "成功" || result === "失败") base.result = result;
  }
  local.fields = base;
}

function rebuildFields(): void {
  if (completionMode.value) {
    rebuildCompletionFields();
    return;
  }
  const row = currentRow.value;
  const base: Record<string, any> = row?.fields ? cloneValue(row.fields) : {};
  const cand = currentCandidate.value;
  if (local.attach && row && cand && local.candidateIndex >= 0) {
    const same =
      [String(cand.scope), String(cand.room), String(cand.rack)].join("/") ===
      [String(row.scope), String(row.room), String(row.rack)].join("/");
    if (same) {
      for (const key of ["expected", "actual"]) {
        const value = String(cand.fields?.[key] || "").trim();
        if (value) base[key] = value;
      }
      for (const key of ["action", "supplier_rack"]) {
        const value = String(cand.fields?.[key] || "").trim();
        if (value && !String(row.fields?.[key] || "").trim()) base[key] = value;
      }
    }
  }
  local.fields = base;
}

function reconcileDirectory(): void {
  if (!completionMode.value || !directoryAligned.value) return;
  // 用户已选行仍在本楼目录 → 保留人工填写
  if (local.rowId && directoryRows.value.some((r: any) => r.row_id === local.rowId)) {
    return;
  }
  const cand = currentCandidate.value;
  if (cand && local.candidateIndex >= 0) {
    const matched = findRowByTriple(cand.scope, cand.room, cand.rack);
    if (matched) {
      local.rowId = String(matched.row_id);
      rebuildCompletionFields();
    } else {
      local.rowId = "";
      local.fields = {};
    }
    emitUpdate();
    return;
  }
  // 手工候选（-1）且已选行已不在新目录 → 清空，避免提交按钮保持可点
  if (local.rowId && !directoryRows.value.some((r: any) => r.row_id === local.rowId)) {
    local.rowId = "";
    local.fields = {};
    emitUpdate();
  }
}

function onField(key: string, event: Event): void {
  const el = event.target as HTMLInputElement | HTMLSelectElement;
  let value = el.value;
  if (key === "expected" || key === "actual") value = value.replace("T", " ");
  local.fields = { ...local.fields, [key]: value };
  emitUpdate();
}

function onAttach(event: Event): void {
  if (completionMode.value) return;
  const checked = (event.target as HTMLInputElement).checked;
  local.attach = checked;
  if (!checked) {
    local.reviewTimes = false;
    local.reviewBusiness = false;
    const row = currentRow.value;
    local.fields = row?.fields ? cloneValue(row.fields) : {};
  } else {
    resetReview();
    rebuildFields();
  }
  emitUpdate();
}

function onReviewTimes(event: Event): void {
  local.reviewTimes = (event.target as HTMLInputElement).checked;
  emitUpdate();
}
function onReviewBusiness(event: Event): void {
  local.reviewBusiness = (event.target as HTMLInputElement).checked;
  emitUpdate();
}

function emitUpdate(): void {
  const next: any = completionMode.value
    ? {
        image_id: local.imageId || "",
        candidate_index: local.candidateIndex,
        row_id: local.rowId || "",
        scope: local.scope || "",
        fields: cloneValue(local.fields) || {},
      }
    : {
        image_id: local.imageId || "",
        candidate_index: local.candidateIndex,
        row_id: local.rowId || "",
        fields: cloneValue(local.fields) || {},
        attach: local.attach === true,
        review_times: local.reviewTimes === true,
        review_business: local.reviewBusiness === true,
      };
  lastEmitted = next;
  emit("update:modelValue", next);
}

function localDateTime(value: any): string {
  return String(value || "").replace(" ", "T");
}

function safeUrl(raw: string): string {
  if (!raw) return "";
  let parsed: URL;
  try {
    parsed = new URL(String(raw), window.location.origin);
  } catch {
    return "";
  }
  if (parsed.origin !== window.location.origin || parsed.protocol !== window.location.protocol) return "";
  if (parsed.username || parsed.password || parsed.hash) return "";
  if (!/^\/api\/cabinet-power\/batches\/[^/]+\/images\/[^/]+$/.test(parsed.pathname)) return "";
  const q = parsed.search;
  if (q !== "" && q !== "?thumbnail=1") return "";
  return parsed.origin + parsed.pathname + q;
}

const previewDialog = ref<HTMLDialogElement | null>(null);
const previewUrl = ref("");

function openPreview(): void {
  const src = safeUrl(String(currentImage.value?.url || ""));
  if (!src) return;
  previewUrl.value = src;
  void nextTick(() => {
    previewDialog.value?.showModal?.();
  });
}
function closePreview(): void {
  previewDialog.value?.close?.();
}

onMounted(() => {
  if (completionMode.value && currentScopeLabel.value && directoryScope.value !== local.scope) {
    emitLoadOptions();
  }
});
</script>

<style scoped>
.lhp-proof { display: flex; flex-direction: column; gap: 8px; min-width: 0; max-width: 100%; font-size: 13px; color: var(--lh-charcoal, #183353); }
.lhp-proof.disabled { opacity: 0.85; }
.lhp-select { display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.lhp-proof :deep(.vnet-select-trigger), .lhp-proof :deep(.vnet-combobox-control) { min-height: 40px; }
.lhp-label { font-size: 12px; font-weight: 600; color: var(--lh-muted, #48586e); line-height: 1.3; overflow-wrap: anywhere; }
.lhp-image-row { display: flex; align-items: center; gap: 6px; min-width: 0; }
.lhp-image-row > :deep(.vnet-select) { flex: 1 1 auto; min-width: 0; }
.lhp-dir-row { display: flex; align-items: center; gap: 8px; min-width: 0; }
.lhp-dir-btn { flex: 0 0 auto; cursor: pointer; font-weight: 600; }
.lhp-dir-btn:disabled { cursor: not-allowed; }
.lhp-thumb { flex: 0 0 auto; width: 30px; height: 30px; border-radius: 6px; overflow: hidden; border: 1px solid var(--lh-border, #dbe5f1); background: var(--lh-surface-subtle, #f3f6fa); display: inline-flex; align-items: center; justify-content: center; }
.lhp-thumb img { width: 100%; height: 100%; object-fit: cover; display: block; }
.lhp-icon-btn { flex: 0 0 40px; width: 40px; height: 40px; min-width: 40px; min-height: 40px; display: inline-flex; align-items: center; justify-content: center; border: 1px solid var(--lh-border, #cbd8e8); border-radius: 8px; background: var(--lh-surface-hover, #f7faff); color: var(--lh-accent, #47709e); cursor: pointer; }
.lhp-icon-btn:hover:not(:disabled) { background: var(--lh-surface-hover, #edf4ff); }
.lhp-icon-btn:disabled { color: var(--lh-faint-muted, #b6c1ce); cursor: not-allowed; }
.lhp-notice { display: flex; align-items: flex-start; gap: 6px; margin: 0; padding: 7px 9px; border-radius: 7px; font-size: 12px; line-height: 1.4; color: var(--lh-muted, #5a6a7d); background: var(--lh-accent-soft, #f2f6fc); min-width: 0; }
.lhp-notice span { min-width: 0; overflow-wrap: anywhere; }
.lhp-mismatch { color: var(--lh-warn, #9a4c10); background: var(--lh-warn-soft, #fff7ed); }
.lhp-notice svg { flex: 0 0 auto; margin-top: 1px; }
.lhp-fields { display: grid; grid-template-columns: minmax(0, 1fr); gap: 6px 10px; min-width: 0; }
@media (min-width: 800px) { .lhp-fields { grid-template-columns: 120px minmax(0, 1fr); align-items: center; } }
.lhp-input { min-width: 0; max-width: 100%; min-height: 40px; box-sizing: border-box; font-size: 13px; line-height: 1.4; padding: 6px 8px; color: var(--lh-charcoal, #142b49); background: var(--lh-surface, #fff); border: 1px solid var(--lh-input-border, #c8d2e0); border-radius: 6px; }
.lhp-input:focus { outline: 2px solid var(--lh-accent-ring, rgba(30, 99, 255, 0.4)); outline-offset: 0; }
.lhp-input:disabled { background: var(--lh-surface-subtle, #f3f6fa); color: var(--lh-muted, #8a99ac); cursor: not-allowed; }
.lhp-options { display: flex; flex-wrap: wrap; gap: 8px 14px; min-width: 0; }
.lhp-check { display: inline-flex; align-items: center; gap: 6px; min-height: 40px; font-size: 13px; color: var(--lh-charcoal, #334155); cursor: pointer; }
.lhp-check:has(input:disabled) { cursor: not-allowed; color: var(--lh-faint-muted, #8a99ac); }
.lhp-check input { width: 16px; height: 16px; accent-color: var(--lh-accent, #2f6fed); }
.lhp-preview { border: 0; border-radius: 12px; padding: 0; max-width: min(92vw, 1000px); max-height: min(90vh, 900px); overflow: hidden; background: var(--lh-surface, #fff); box-shadow: 0 24px 60px rgba(22, 58, 105, 0.35); }
.lhp-preview::backdrop { background: rgba(15, 23, 42, 0.5); }
.lhp-preview[open] { display: flex; flex-direction: column; }
.lhp-preview-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 8px 10px; border-bottom: 1px solid var(--lh-border, #dbe5f1); background: var(--lh-surface-hover, #f7faff); }
.lhp-preview-title { font-size: 13px; font-weight: 700; color: var(--lh-charcoal, #334155); overflow-wrap: anywhere; }
.lhp-preview-body { min-width: 0; max-width: 100%; max-height: calc(min(90vh, 900px) - 57px); overflow: auto; background: var(--lh-surface-subtle, #f3f6fa); }
.lhp-preview-body img { display: block; max-width: 100%; height: auto; }
@media (max-width: 440px) {
  .lhp-preview { max-width: calc(100vw - 16px); max-height: calc(100dvh - 16px); }
  .lhp-preview-body { max-height: calc(100dvh - 73px); }
}
</style>
