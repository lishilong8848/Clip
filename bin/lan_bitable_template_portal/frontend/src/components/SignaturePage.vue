<template>
  <section class="signature-page" :class="{ 'link-mode': linkMode }">
    <header v-if="!linkMode" class="signature-hero">
      <div>
        <span>线上手写签名</span>
        <h2>选择人员后在手机上签名保存</h2>
      </div>
    </header>

    <section class="signature-grid">
      <aside v-if="!linkMode" class="person-panel">
        <div class="panel-title">
          <strong>签名人员</strong>
          <small>{{ peopleCountText }}</small>
        </div>
        <label class="search-box">
          <span>搜索</span>
          <div class="search-control">
            <input
              v-model="query"
              enterkeyhint="search"
              placeholder="输入姓名、工号、楼栋"
              @keyup.enter="loadPeople()"
            />
            <button
              class="btn ghost refresh-mini"
              type="button"
              :disabled="loading"
              title="重新读取签名人员"
              aria-label="重新读取签名人员"
              @click="loadPeople()"
            >
              {{ loading ? "读取" : "刷新" }}
            </button>
          </div>
          <small class="search-inline-status">{{ searchStatusText }}</small>
        </label>
        <div class="person-list">
          <button
            v-for="person in people"
            :key="personKey(person)"
            class="person-card"
            :class="{ active: personKey(person) === selectedRecordId }"
            type="button"
            @click="selectPerson(person)"
          >
            <span class="avatar">{{ personInitial(person) }}</span>
            <span class="person-main">
              <strong>{{ person.name || "未命名人员" }}</strong>
              <small>
                <template v-if="person.employee_no">{{ person.employee_no }} · </template>
                <template v-if="person.building">{{ person.building }} · </template>
                {{ person.position || person.team || "未填写岗位" }}
              </small>
            </span>
            <em :class="{ ok: personHasUsableSignature(person) }">
              {{ personHasUsableSignature(person) ? "已有签名" : "待签名" }}
            </em>
          </button>
          <div v-if="!loading && !people.length" class="empty-state">
            暂未找到人员，请更换关键词后重试。
          </div>
        </div>
      </aside>

      <section class="sign-panel">
        <div class="panel-title">
          <strong>{{ selectedPerson?.name || (temporaryMode ? "正在读取临时签名" : linkMode ? "正在读取签名人员" : "请选择签名人员") }}</strong>
          <small v-if="selectedPerson">
            <template v-if="selectedPerson.employee_no">{{ selectedPerson.employee_no }} · </template>
            <template v-if="selectedPerson.building">{{ selectedPerson.building }} · </template>
            {{ selectedPerson.position || selectedPerson.team || selectedPerson.role_label || "签名记录" }}
          </small>
          <small v-else>{{ linkMode ? "读取中" : "未选择" }}</small>
        </div>

        <div class="canvas-wrap" :class="{ disabled: !selectedPerson }">
          <button
            v-if="selectedPerson"
            class="signature-clear-inline"
            type="button"
            :disabled="saving"
            @click="clearCanvas"
          >
            清空
          </button>
          <img
            v-if="signaturePreviewUrl && !hasInk"
            class="signature-preview-img"
            :src="signaturePreviewUrl"
            alt="已有手写签名"
            @error="markSelectedSignatureUnavailable"
          />
          <canvas
            ref="canvasRef"
            aria-label="手写签名区域"
            @pointerdown="startDraw"
            @pointermove="moveDraw"
            @pointerup="endDraw"
            @pointercancel="endDraw"
            @pointerleave="endDraw"
          ></canvas>
          <div v-if="!hasInk && !signaturePreviewUrl" class="canvas-placeholder">
            请在此处手写签名
          </div>
          <div v-if="message" class="signature-toast" :class="messageType">
            {{ message }}
          </div>
        </div>

        <div class="signature-actions">
          <button class="btn blue" type="button" :disabled="Boolean(saveDisabledReason)" :title="saveDisabledReason" @click="saveSignature">
            {{ saving ? "保存中" : (temporaryMode ? "保存临时签名" : "保存到签名表") }}
          </button>
          <span v-if="saveDisabledReason && !signaturePreviewUrl" class="signature-action-hint">{{ saveDisabledReason }}</span>
          <span v-if="signaturePreviewUrl && !hasInk" class="saved-inline">
            {{ linkMode ? "已保存" : temporaryMode ? "已保存" : "已有签名" }}
          </span>
        </div>
      </section>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { requestJson, type Dict } from "../api/client";

const props = defineProps<{
  defaultScope?: string;
}>();

const params = new URLSearchParams(window.location.search);
const query = ref(params.get("q") || params.get("name") || "");
const requestedRecordId = ref(params.get("record_id") || "");
const requestedTemporaryId = ref(params.get("temporary_id") || "");
const linkToken = ref(params.get("token") || "");
const temporaryMode = computed(() => Boolean(requestedTemporaryId.value));
const linkMode = computed(() => Boolean(requestedRecordId.value || requestedTemporaryId.value));
const people = ref<Dict[]>([]);
const totalCount = ref(0);
const selectedRecordId = ref("");
const loading = ref(false);
const saving = ref(false);
const message = ref("");
const messageType = ref<"success" | "failed" | "info">("info");
const canvasRef = ref<HTMLCanvasElement | null>(null);
const hasInk = ref(false);
let drawing = false;
let resizeObserver: ResizeObserver | null = null;
let searchTimer: ReturnType<typeof setTimeout> | null = null;
let peopleRequestSeq = 0;

const selectedPerson = computed(() => people.value.find((item) => personKey(item) === selectedRecordId.value) || null);
const signaturePreviewUrl = computed(() => (
  personHasUsableSignature(selectedPerson.value) ? String(selectedPerson.value?.signature_preview_url || "") : ""
));
const peopleCountText = computed(() => {
  if (loading.value) return "读取中";
  if (!totalCount.value) return "0 人";
  if (totalCount.value === people.value.length) return `${totalCount.value} 人`;
  return `${people.value.length} / ${totalCount.value} 人`;
});
const searchStatusText = computed(() => {
  if (loading.value) return "搜索中";
  if (people.value.length) {
    return totalCount.value > people.value.length
      ? `已显示 ${people.value.length} / ${totalCount.value} 人`
      : `已找到 ${people.value.length} 人`;
  }
  return query.value.trim() ? "暂未找到人员" : "输入姓名、工号或楼栋自动搜索";
});
const saveDisabledReason = computed(() => {
  if (saving.value) return "正在保存签名，请稍候。";
  if (!selectedPerson.value) return "请先选择签名人员。";
  if (!hasInk.value) return "请先在签名区手写签名。";
  return "";
});

function setMessage(text: string, type: "success" | "failed" | "info" = "info"): void {
  message.value = text;
  messageType.value = type;
}

function personInitial(person: Dict): string {
  const name = String(person.name || person.employee_no || "?").trim();
  return name.slice(0, 1).toUpperCase() || "?";
}

function personKey(person: Dict | null | undefined): string {
  return String(person?.record_id || person?.temp_id || "").trim();
}

function personHasUsableSignature(person: Dict | null | undefined): boolean {
  return Boolean(person?.has_signature && String(person?.signature_preview_url || "").trim());
}

function markSelectedSignatureUnavailable(): void {
  if (!selectedPerson.value) return;
  selectedPerson.value.has_signature = false;
  selectedPerson.value.signature_count = 0;
  selectedPerson.value.signature_preview_url = "";
  selectedPerson.value.signature_version = "";
  setMessage("该人员签名附件不可用，请重新手写保存。", "failed");
}

function selectPerson(person: Dict): void {
  selectedRecordId.value = personKey(person);
  clearCanvas();
  setMessage(`${person.name || "该人员"} 已选中，请手写签名。`, "info");
}

function schedulePeopleSearch(): void {
  if (searchTimer) {
    clearTimeout(searchTimer);
  }
  searchTimer = setTimeout(() => {
    searchTimer = null;
    requestedRecordId.value = "";
    void loadPeople({ silent: true });
  }, 300);
}

async function loadPeople(options: { silent?: boolean } = {}): Promise<void> {
  const requestSeq = ++peopleRequestSeq;
  loading.value = true;
  try {
    if (temporaryMode.value) {
      const url = new URL("/api/signatures/temporary/session", window.location.origin);
      url.searchParams.set("temporary_id", requestedTemporaryId.value);
      url.searchParams.set("token", linkToken.value);
      const data = await requestJson(`${url.pathname}${url.search}`);
      if (requestSeq !== peopleRequestSeq) return;
      const roleLabel = data.role === "auditor" ? "维护审核人" : "维护实施人";
      people.value = [{ ...data, role_label: roleLabel }];
      totalCount.value = 1;
      selectedRecordId.value = personKey(people.value[0]);
      if (!options.silent) setMessage("临时签名页面已打开。", "info");
      return;
    }
    const url = new URL("/api/signatures/people", window.location.origin);
    if (props.defaultScope) url.searchParams.set("scope", props.defaultScope);
    if (query.value.trim()) url.searchParams.set("q", query.value.trim());
    if (requestedRecordId.value) url.searchParams.set("record_id", requestedRecordId.value);
    if (linkToken.value) url.searchParams.set("token", linkToken.value);
    url.searchParams.set("limit", "120");
    const data = await requestJson(`${url.pathname}${url.search}`);
    if (requestSeq !== peopleRequestSeq) return;
    people.value = Array.isArray(data.people) ? data.people : [];
    totalCount.value = Number(data.count || people.value.length || 0);
    if (requestedRecordId.value && people.value.length) {
      selectedRecordId.value = personKey(people.value[0]);
    } else if (!selectedRecordId.value && people.value.length === 1) {
      selectedRecordId.value = personKey(people.value[0]);
    }
    if (!people.value.length) {
      setMessage("暂未找到匹配人员。", "failed");
    } else if (!options.silent) {
      setMessage("人员列表已更新。", "success");
    }
  } catch (error: unknown) {
    if (requestSeq !== peopleRequestSeq) return;
    const text = error instanceof Error ? error.message : "读取签名人员失败。";
    setMessage(text, "failed");
  } finally {
    if (requestSeq === peopleRequestSeq) {
      loading.value = false;
      await nextTick();
      resizeCanvas();
    }
  }
}

function canvasContext(): CanvasRenderingContext2D | null {
  const canvas = canvasRef.value;
  if (!canvas) return null;
  const ctx = canvas.getContext("2d");
  if (!ctx) return null;
  ctx.globalAlpha = 1;
  ctx.globalCompositeOperation = "source-over";
  ctx.imageSmoothingEnabled = true;
  ctx.lineWidth = 5.5;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = "#000000";
  return ctx;
}

function resizeCanvas(): void {
  const canvas = canvasRef.value;
  if (!canvas) return;
  const ratio = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
  const rect = canvas.getBoundingClientRect();
  const cssWidth = canvas.clientWidth || rect.width;
  const cssHeight = canvas.clientHeight || rect.height;
  const width = Math.max(320, Math.floor(cssWidth * ratio));
  const height = Math.max(180, Math.floor(cssHeight * ratio));
  if (canvas.width === width && canvas.height === height) return;
  const previous = document.createElement("canvas");
  const previousHasInk = hasInk.value && canvas.width > 0 && canvas.height > 0;
  if (previousHasInk) {
    previous.width = canvas.width;
    previous.height = canvas.height;
    previous.getContext("2d")?.drawImage(canvas, 0, 0);
  }
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    hasInk.value = false;
    return;
  }
  if (previousHasInk) {
    ctx.drawImage(previous, 0, 0, previous.width, previous.height, 0, 0, width, height);
  }
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.globalAlpha = 1;
  ctx.globalCompositeOperation = "source-over";
  ctx.imageSmoothingEnabled = true;
  ctx.lineWidth = 5.5;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = "#000000";
  hasInk.value = Boolean(previousHasInk);
}

function pointFromEvent(event: PointerEvent): { x: number; y: number } {
  const canvas = canvasRef.value;
  const rect = canvas?.getBoundingClientRect();
  if (canvas && rect && linkMode.value && window.matchMedia("(max-width: 760px) and (orientation: portrait)").matches) {
    const cssWidth = canvas.clientWidth || rect.height || 1;
    const cssHeight = canvas.clientHeight || rect.width || 1;
    const x = (event.clientY - rect.top) * (cssWidth / Math.max(1, rect.height));
    const y = (rect.right - event.clientX) * (cssHeight / Math.max(1, rect.width));
    return {
      x: Math.max(0, Math.min(cssWidth, x)),
      y: Math.max(0, Math.min(cssHeight, y)),
    };
  }
  return {
    x: event.clientX - (rect?.left || 0),
    y: event.clientY - (rect?.top || 0),
  };
}

function startDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!selectedPerson.value || saving.value) return;
  const canvas = canvasRef.value;
  const ctx = canvasContext();
  if (!canvas || !ctx) return;
  canvas.setPointerCapture?.(event.pointerId);
  drawing = true;
  hasInk.value = true;
  const point = pointFromEvent(event);
  ctx.beginPath();
  ctx.moveTo(point.x, point.y);
}

function moveDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!drawing) return;
  const ctx = canvasContext();
  if (!ctx) return;
  const point = pointFromEvent(event);
  ctx.lineTo(point.x, point.y);
  ctx.stroke();
  hasInk.value = true;
}

function endDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!drawing) return;
  drawing = false;
  canvasRef.value?.releasePointerCapture?.(event.pointerId);
}

function clearCanvas(): void {
  const canvas = canvasRef.value;
  const ctx = canvasContext();
  if (canvas && ctx) {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
  }
  hasInk.value = false;
}

async function saveSignature(): Promise<void> {
  if (!selectedPerson.value || !canvasRef.value || !hasInk.value) return;
  saving.value = true;
  try {
    const signaturePng = canvasRef.value.toDataURL("image/png");
    const data = temporaryMode.value
      ? await requestJson("/api/signatures/temporary/save", {
        method: "POST",
        body: JSON.stringify({
          temporary_id: requestedTemporaryId.value,
          token: linkToken.value,
          signature_png: signaturePng,
        }),
      })
      : await requestJson("/api/signatures/save", {
        method: "POST",
        body: JSON.stringify({
          record_id: selectedPerson.value.record_id,
          token: linkToken.value,
          signer_name: selectedPerson.value.name || "",
          signature_png: signaturePng,
        }),
      });
    setMessage(temporaryMode.value ? "签名已保存。" : `${data.name || selectedPerson.value.name || "签名"} 已保存。`, "success");
    selectedPerson.value.has_signature = true;
    selectedPerson.value.signature_count = 1;
    selectedPerson.value.signature_preview_url = data.signature_preview_url || selectedPerson.value.signature_preview_url || "";
    selectedPerson.value.signature_version = data.signature_version || selectedPerson.value.signature_version || "";
    selectedPerson.value.status = data.status || "signed";
    selectedPerson.value.record_id = data.record_id || selectedPerson.value.record_id || "";
    clearCanvas();
  } catch (error: unknown) {
    const text = error instanceof Error ? error.message : "保存签名失败。";
    setMessage(text, "failed");
  } finally {
    saving.value = false;
  }
}

onMounted(async () => {
  if (linkMode.value) {
    document.body.classList.add("signature-link-active");
  }
  await nextTick();
  resizeCanvas();
  if (canvasRef.value && "ResizeObserver" in window) {
    resizeObserver = new ResizeObserver(() => resizeCanvas());
    resizeObserver.observe(canvasRef.value);
  }
  await loadPeople({ silent: linkMode.value });
});

onBeforeUnmount(() => {
  document.body.classList.remove("signature-link-active");
  if (searchTimer) {
    clearTimeout(searchTimer);
    searchTimer = null;
  }
  resizeObserver?.disconnect();
  resizeObserver = null;
});

watch(query, () => {
  schedulePeopleSearch();
});
</script>

<style scoped>
:global(body.signature-link-active) {
  overflow: hidden;
  user-select: none;
  -webkit-user-select: none;
  overscroll-behavior: none;
  touch-action: none;
}

.signature-page {
  display: grid;
  gap: 18px;
  width: min(1120px, calc(100vw - 32px));
  margin: 0 auto 48px;
}

.signature-page.link-mode {
  width: min(100%, 1180px);
  min-height: 100dvh;
  margin: 0 auto;
  padding: 10px;
  box-sizing: border-box;
  gap: 10px;
  user-select: none;
  -webkit-user-select: none;
  overscroll-behavior: none;
}

.signature-page.link-mode .signature-grid {
  grid-template-columns: 1fr;
  min-height: calc(100dvh - 20px);
}

.signature-page.link-mode .sign-panel {
  grid-template-rows: auto minmax(220px, 1fr) auto;
  min-height: calc(100dvh - 20px);
  padding: 14px;
}

.signature-page.link-mode .canvas-wrap {
  min-height: 220px;
  height: min(62dvh, calc((100vw - 28px) * 0.46));
  max-height: calc(100dvh - 130px);
  aspect-ratio: 16 / 7;
}

.signature-page.link-mode .canvas-wrap canvas {
  height: 100%;
  min-height: 0;
}

.signature-page.link-mode .signature-actions {
  display: grid;
  grid-template-columns: 1fr 1fr;
}

.signature-page.link-mode .signature-actions .btn {
  min-height: 46px;
  font-size: 15px;
}

.signature-page.link-mode .panel-title strong {
  font-size: 20px;
}

.signature-hero,
.person-panel,
.sign-panel {
  border: 1px solid rgba(148, 163, 184, 0.28);
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 20px 55px rgba(15, 23, 42, 0.08);
}

.signature-hero {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 24px;
}

.signature-hero span {
  display: inline-flex;
  margin-bottom: 8px;
  color: #2563eb;
  font-size: 13px;
  font-weight: 800;
}

.signature-hero h2 {
  margin: 0;
  color: #0f172a;
  font-size: 28px;
  line-height: 1.2;
}

.signature-hero p {
  max-width: 640px;
  margin: 8px 0 0;
  color: #64748b;
  line-height: 1.7;
}

.signature-grid {
  display: grid;
  grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
  gap: 18px;
}

.person-panel,
.sign-panel {
  display: grid;
  align-content: start;
  gap: 16px;
  min-width: 0;
  padding: 18px;
}

.panel-title {
  display: grid;
  gap: 4px;
}

.panel-title strong {
  color: #0f172a;
  font-size: 18px;
}

.panel-title small {
  color: #64748b;
  line-height: 1.5;
}

.search-box {
  display: grid;
  gap: 8px;
}

.search-box span {
  color: #475569;
  font-size: 13px;
  font-weight: 700;
}

.search-control {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
}

.search-box input {
  width: 100%;
  box-sizing: border-box;
  border: 1px solid #dbe3ee;
  border-radius: 14px;
  padding: 12px 14px;
  color: #0f172a;
  font: inherit;
  outline: none;
}

.search-box input:focus {
  border-color: #2563eb;
  box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12);
}

.refresh-mini {
  min-height: 44px;
  padding-inline: 12px;
  white-space: nowrap;
}

.search-inline-status,
.saved-inline {
  color: #64748b;
  font-size: 12px;
  line-height: 1.5;
}

.person-list {
  display: grid;
  gap: 10px;
  max-height: min(58vh, 620px);
  overflow: auto;
  padding-right: 4px;
}

.person-card {
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  width: 100%;
  border: 1px solid #e2e8f0;
  border-radius: 16px;
  background: #ffffff;
  padding: 12px;
  text-align: left;
  cursor: pointer;
}

.person-card:hover,
.person-card.active {
  border-color: #2563eb;
  box-shadow: 0 10px 24px rgba(37, 99, 235, 0.12);
}

.avatar {
  display: grid;
  width: 42px;
  height: 42px;
  place-items: center;
  border-radius: 14px;
  background: linear-gradient(135deg, #2563eb, #22c1dc);
  color: #ffffff;
  font-weight: 900;
}

.person-main {
  display: grid;
  min-width: 0;
  gap: 4px;
}

.person-main strong,
.person-main small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.person-main strong {
  color: #0f172a;
  font-size: 15px;
}

.person-main small {
  color: #64748b;
  font-size: 12px;
}

.person-card em {
  border-radius: 999px;
  background: #eef2ff;
  padding: 5px 8px;
  color: #3156c9;
  font-size: 12px;
  font-style: normal;
  font-weight: 800;
  white-space: nowrap;
}

.person-card em.ok {
  background: #dcfce7;
  color: #15803d;
}

.canvas-wrap {
  position: relative;
  min-height: 260px;
  border: 1px dashed #93c5fd;
  border-radius: 20px;
  background:
    linear-gradient(90deg, rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    linear-gradient(rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    #ffffff;
  background-size: 24px 24px;
  overflow: hidden;
  user-select: none;
  -webkit-user-select: none;
  touch-action: none;
}

.signature-clear-inline {
  position: absolute;
  top: 10px;
  right: 10px;
  z-index: 5;
  min-height: 34px;
  border: 1px solid #bfdbfe;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.94);
  padding: 6px 12px;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(37, 99, 235, 0.12);
}

.signature-clear-inline:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.canvas-wrap.disabled {
  opacity: 0.62;
}

.canvas-wrap canvas {
  position: relative;
  z-index: 2;
  display: block;
  width: 100%;
  height: 260px;
  touch-action: none;
  cursor: crosshair;
  user-select: none;
  -webkit-user-select: none;
}

.signature-preview-img {
  position: absolute;
  z-index: 1;
  inset: 12%;
  width: 76%;
  height: 76%;
  object-fit: contain;
  pointer-events: none;
}

.canvas-placeholder {
  position: absolute;
  z-index: 3;
  inset: 0;
  display: grid;
  place-items: center;
  pointer-events: none;
  color: #94a3b8;
  font-size: 18px;
  font-weight: 800;
}

.signature-toast {
  position: absolute;
  z-index: 4;
  top: 10px;
  right: 10px;
  max-width: min(72%, 360px);
  border-radius: 999px;
  background: rgba(239, 246, 255, 0.96);
  padding: 7px 11px;
  color: #1d4ed8;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.35;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12);
}

.signature-toast.success {
  background: rgba(220, 252, 231, 0.96);
  color: #15803d;
}

.signature-toast.failed {
  background: rgba(254, 226, 226, 0.96);
  color: #b91c1c;
}

.signature-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  align-items: center;
  gap: 10px;
}

.signature-actions .saved-inline {
  flex: 1 1 220px;
  text-align: right;
}

.signature-action-hint {
  flex: 1 1 220px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #f8fbff;
  padding: 7px 10px;
  color: #48627f;
  font-size: 12px;
  font-weight: 850;
  line-height: 1.35;
  text-align: right;
}

.signature-note {
  border-radius: 14px;
  background: #f8fafc;
  padding: 12px 14px;
  color: #64748b;
  font-size: 13px;
  line-height: 1.6;
}

.signature-message,
.empty-state {
  border-radius: 16px;
  padding: 12px 14px;
  background: #eff6ff;
  color: #1d4ed8;
  line-height: 1.55;
}

.signature-message.success {
  background: #dcfce7;
  color: #15803d;
}

.signature-message.failed {
  background: #fee2e2;
  color: #b91c1c;
}

.empty-state {
  background: #f8fafc;
  color: #64748b;
}

@media (max-width: 760px) {
  .signature-page {
    width: min(100% - 20px, 620px);
    gap: 12px;
    margin-bottom: 28px;
  }

  .signature-page.link-mode {
    width: 100vw;
    min-height: 100dvh;
    margin: 0;
    padding: 8px;
  }

  .signature-page.link-mode .sign-panel {
    min-height: calc(100dvh - 16px);
    border-radius: 18px;
  }

  .signature-hero {
    display: grid;
    padding: 18px;
  }

  .signature-hero h2 {
    font-size: 23px;
  }

  .signature-grid {
    grid-template-columns: 1fr;
  }

  .person-list {
    max-height: 300px;
  }

  .canvas-wrap,
  .canvas-wrap canvas {
    min-height: 220px;
    height: 220px;
  }

  .signature-page.link-mode .canvas-wrap,
  .signature-page.link-mode .canvas-wrap canvas {
    height: min(60dvh, calc((100vw - 20px) * 0.58));
    min-height: 220px;
  }

  .signature-actions {
    display: grid;
    grid-template-columns: 1fr;
  }

  .signature-page.link-mode .signature-actions {
    grid-template-columns: 1fr 1fr;
  }
}

@media (max-width: 760px) and (orientation: portrait) {
  .signature-page.link-mode {
    position: fixed;
    top: 0;
    left: 0;
    width: 100dvh;
    max-width: none;
    height: 100dvw;
    min-height: 0;
    margin: 0;
    padding: 8px;
    overflow: hidden;
    transform: rotate(90deg) translateY(-100%);
    transform-origin: top left;
    touch-action: none;
    background: #f8fafc;
  }

  .signature-page.link-mode .signature-grid {
    width: calc(100dvh - 16px);
    height: calc(100dvw - 16px);
    min-height: 0;
  }

  .signature-page.link-mode .sign-panel {
    width: 100%;
    height: 100%;
    min-height: 0;
    box-sizing: border-box;
    grid-template-rows: auto minmax(0, 1fr) auto;
    padding: 12px;
  }

  .signature-page.link-mode .panel-title {
    grid-template-columns: minmax(0, 1fr);
    gap: 2px;
  }

  .signature-page.link-mode .panel-title strong {
    overflow: hidden;
    font-size: 18px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .signature-page.link-mode .panel-title small {
    overflow: hidden;
    font-size: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .signature-page.link-mode .canvas-wrap,
  .signature-page.link-mode .canvas-wrap canvas {
    width: 100%;
    height: 100%;
    min-height: 0;
    max-height: none;
  }

  .signature-page.link-mode .canvas-wrap {
    aspect-ratio: auto;
    border-radius: 16px;
  }

  .signature-page.link-mode .signature-actions {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
