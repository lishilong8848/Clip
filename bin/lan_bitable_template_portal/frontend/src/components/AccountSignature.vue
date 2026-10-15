<template>
  <section class="account-signature">
    <header class="as-head">
      <Fingerprint :size="17" class="as-head-icon" />
      <div class="as-head-copy">
        <strong>个人签名</strong>
        <span>{{ openId ? '查看该账号签名状态' : '当前登录账号签名状态' }}</span>
      </div>
      <button v-if="!loading && !error" type="button" class="as-refresh" :disabled="busy" @click="load()">
        <RefreshCw :size="14" :class="{ spin: loading }" />刷新
      </button>
    </header>

    <div v-if="loading" class="as-state" role="status"><RefreshCw :size="16" class="spin" />正在读取签名信息…</div>

    <div v-else-if="error" class="as-state error" role="alert">
      <CircleAlert :size="17" /><span>{{ error }}</span>
      <button type="button" class="as-link" @click="load()"><RefreshCw :size="14" />重试</button>
    </div>

    <template v-else-if="person">
      <dl class="as-grid">
        <div><dt>姓名</dt><dd>{{ person.name || '—' }}</dd></div>
        <div v-if="person.employee_no"><dt>工号</dt><dd>{{ person.employee_no }}</dd></div>
        <div v-if="person.building"><dt>楼栋</dt><dd>{{ buildingLabel(person.building) }}</dd></div>
        <div><dt>签名状态</dt><dd><span :class="['as-badge', statusTone(person.signature_status)]">{{ signatureStatusLabel(person.signature_status) }}</span></dd></div>
      </dl>

      <p v-if="person.signature_reason" class="as-note">{{ person.signature_reason }}</p>

      <details v-if="person.request" class="as-request" open>
        <summary>签名请求</summary>
        <dl>
          <div v-if="person.request.status"><dt>状态</dt><dd>{{ requestStatusLabel(person.request.status) }}</dd></div>
          <div v-if="person.request.created_at"><dt>发起时间</dt><dd>{{ formatTime(person.request.created_at) }}</dd></div>
          <div v-if="person.request.saved_at"><dt>签名保存时间</dt><dd>{{ formatTime(person.request.saved_at) }}</dd></div>
          <div v-if="person.request.expires_at"><dt>有效期至</dt><dd>{{ formatTime(person.request.expires_at) }}</dd></div>
        </dl>
        <p v-if="person.request.error" class="as-note err" role="alert">{{ person.request.error }}</p>
      </details>

      <div class="as-actions">
        <button v-if="canViewSignature" type="button" class="as-btn" :disabled="busy" @click="togglePreview">
          <Eye :size="15" />{{ showPreview ? '收起签名' : '查看签名' }}
        </button>
        <button v-if="canSelfResign" type="button" class="as-btn primary" :disabled="busy" @click="startSelf">
          <RefreshCw :size="15" :class="{ spin: selfBusy }" />{{ selfButtonLabel }}
        </button>
        <button v-else-if="canRequestOther" type="button" class="as-btn primary" :disabled="busy" @click="openConfirm">
          <Send :size="15" />发起签名
        </button>
        <a v-if="canManageRelay" href="/signature-management" class="as-link">
          <Send :size="15" />签名管理中心
        </a>
      </div>

      <p v-if="actionError" class="as-note err" role="alert">{{ actionError }}</p>

      <div v-if="showPreview" class="as-preview" aria-live="polite">
        <img v-if="previewSrc" :key="previewSrc" :src="previewSrc" :alt="`${person.name}的签名`"
          class="as-preview-img" :class="{ hidden: previewLoading || previewError }"
          @load="previewLoaded" @error="previewFailed" @dragstart.prevent />
        <span v-if="previewLoading" class="as-preview-state" role="status"><RefreshCw :size="16" class="spin" />正在加载签名…</span>
        <div v-else-if="previewError" class="as-preview-state error" role="alert">
          <CircleAlert :size="16" /><span>{{ previewError }}</span>
          <button type="button" class="as-link" @click="loadPreview"><RefreshCw :size="14" />重试</button>
        </div>
      </div>

      <div v-if="confirming" class="as-confirm" role="group" :aria-labelledby="confirmLabelId">
        <p :id="confirmLabelId" class="as-confirm-copy">确认向「{{ person.name }}」发送签名请求？该人员需在收到的链接中完成签名。</p>
        <div class="as-confirm-actions">
          <button type="button" class="as-btn" :disabled="requestBusy" @click="confirming = false">取消</button>
          <button type="button" class="as-btn primary" :disabled="requestBusy" @click="sendRequest">
            <Send :size="15" :class="{ spin: requestBusy }" />确认发送
          </button>
        </div>
        <p v-if="requestError" class="as-note err" role="alert">{{ requestError }}</p>
      </div>

      <p v-if="notice" class="as-note ok" role="status">{{ notice }}</p>
    </template>

    <div v-else class="as-state info" role="status"><Info :size="16" /><span>{{ reason || '该账号暂无个人签名。' }}</span></div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, useId, watch } from 'vue';
import { CircleAlert, Eye, Fingerprint, Info, RefreshCw, Send } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';

const props = defineProps<{ openId?: string }>();

const person = ref<Dict | null>(null);
const reason = ref('');
const loading = ref(false);
const error = ref('');
const showPreview = ref(false);
const previewSrc = ref('');
const previewLoading = ref(false);
const previewError = ref('');
const confirming = ref(false);
const requestError = ref('');
const actionError = ref('');
const selfBusy = ref(false);
const requestBusy = ref(false);
const notice = ref('');
const operationId = ref(newOperationId());
const recognizedFailure = ref(false);
const confirmLabelId = useId();

let seq = 0;
let opSeq = 0;
let disposed = false;
let active: AbortController | null = null;
let opActive: AbortController | null = null;

const busy = computed(() => loading.value || selfBusy.value || requestBusy.value);
const canViewSignature = computed(() => !!person.value?.can_preview && !!person.value?.has_signature);
const canSelfResign = computed(() => person.value?.is_self === true && person.value?.can_request === true);
const canRequestOther = computed(() => person.value?.is_self !== true && person.value?.can_request === true);
const canManageRelay = computed(() => canRequestOther.value);
const selfButtonLabel = computed(() =>
  person.value?.is_self === true && person.value?.signature_status === 'signed' ? '重新签名' : '开始签名'
);

function cancelInFlight() {
  if (active) { active.abort(); active = null; }
  if (opActive) { opActive.abort(); opActive = null; }
  seq++;
  opSeq++;
  selfBusy.value = false;
  requestBusy.value = false;
}

function newOperationId(): string {
  return crypto.randomUUID?.() || Array.from(crypto.getRandomValues(new Uint32Array(4)), n => n.toString(16)).join('');
}

function buildingLabel(value: string) {
  const raw = String(value || '').trim();
  if (!raw) return raw;
  if (raw === '110' || raw === '110站') return '110站';
  // Preserve already-preformatted labels (e.g. "E楼", "H楼"); map only raw codes.
  return /(楼|站)$/.test(raw) ? raw : `${raw}楼`;
}

const STATUS_LABELS: Record<string, string> = {
  signed: '已有可用签名', unsigned: '未签名', resign: '需重新签名',
};
function signatureStatusLabel(value?: string) { return STATUS_LABELS[String(value || '')] || value || '未知'; }
function statusTone(value?: string) {
  if (value === 'signed') return 'ok';
  if (value === 'resign') return 'warn';
  return 'neutral';
}

const REQUEST_LABELS: Record<string, string> = {
  ready: '待本人在线签名', sent: '待本人签名', sending: '消息发送中', saving: '保存待核验',
  completed: '签名已保存', send_failed: '发送失败', send_unknown: '发送结果待核验',
  revoked: '链接已撤销', expired: '链接已过期',
};
function requestStatusLabel(value?: string) { return REQUEST_LABELS[String(value || '')] || value || '—'; }

function formatTime(value: unknown) {
  if (value == null || value === '') return '—';
  let date: Date;
  const raw = String(value);
  if (typeof value === 'number') date = new Date(value * 1000);
  else if (/^\d+$/.test(raw)) date = new Date(Number(raw) * 1000);
  else date = new Date(raw);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('zh-CN', { hour12: false });
}

function resetPreview() { showPreview.value = false; previewSrc.value = ''; previewLoading.value = false; previewError.value = ''; }

async function load(options: { preserveNotice?: boolean } = {}) {
  const preserveNotice = options.preserveNotice === true;
  active?.abort();
  const controller = new AbortController();
  active = controller;
  const n = ++seq;
  resetPreview();
  confirming.value = false; requestError.value = ''; actionError.value = ''; reason.value = ''; person.value = null;
  if (!preserveNotice) notice.value = '';
  loading.value = true; error.value = '';
  try {
    const qs = props.openId ? `?open_id=${encodeURIComponent(props.openId)}` : '';
    const data = await requestJson(`/api/signatures/management/account${qs}`, { signal: controller.signal, cache: 'no-store' });
    if (disposed || controller.signal.aborted || n !== seq) return;
    person.value = data.person || null;
    reason.value = String(data.reason || '');
  } catch (e: any) {
    if (disposed || controller.signal.aborted || n !== seq) return;
    error.value = e.message || '签名信息读取失败，请重试。';
  } finally {
    if (active === controller) {
      active = null;
      if (!disposed && n === seq) loading.value = false;
    }
  }
}

function togglePreview() {
  if (!canViewSignature.value || busy.value) return;
  if (showPreview.value) { resetPreview(); return; }
  showPreview.value = true;
  loadPreview();
}

function loadPreview() {
  const p = person.value;
  if (!p || !canViewSignature.value) return;
  previewSrc.value = '';
  previewLoading.value = true;
  previewError.value = '';
  const params = new URLSearchParams({
    source: String(p.source || ''),
    record_id: String(p.record_id || ''),
    _rt: Date.now().toString(36) + Math.random().toString(36).slice(2, 8),
  });
  previewSrc.value = '/api/signatures/management/preview?' + params.toString();
}

function previewLoaded(event: Event) {
  const img = event.currentTarget as HTMLImageElement;
  if (!img || img.src !== new URL(previewSrc.value, document.baseURI).href) return;
  previewLoading.value = false; previewError.value = '';
}
function previewFailed(event: Event) {
  const img = event.currentTarget as HTMLImageElement;
  if (!img || img.src !== new URL(previewSrc.value, document.baseURI).href) return;
  previewLoading.value = false; previewError.value = '签名图片读取失败，请重试。';
}

function openConfirm() {
  if (canRequestOther.value && !busy.value) {
    // Generate the operation id once when the confirmation is opened; the same id
    // is reused for network/unknown retries so the sender never duplicates.
    operationId.value = newOperationId();
    recognizedFailure.value = false;
    confirming.value = true; requestError.value = '';
  }
}

function isSignaturePath(url: string): boolean {
  if (!url.startsWith('/signature?')) return false;
  try {
    const parsed = new URL(url, window.location.origin);
    if (parsed.origin !== window.location.origin) return false;
    if (parsed.pathname !== '/signature') return false;
    const token = new URLSearchParams(parsed.hash.replace(/^#/, '')).get('token');
    return Boolean(parsed.searchParams.get('request_id') && token);
  } catch { return false; }
}

async function startSelf() {
  if (!canSelfResign.value || selfBusy.value || requestBusy.value) return;
  opActive?.abort();
  const controller = new AbortController();
  opActive = controller;
  const n = ++opSeq;
  selfBusy.value = true; actionError.value = '';
  try {
    const data = await requestJson('/api/signatures/management/self-start', { method: 'POST', body: '{}', signal: controller.signal, cache: 'no-store' });
    if (disposed || controller.signal.aborted || n !== opSeq) return;
    const linkUrl = String(data.link_url || '');
    if (!isSignaturePath(linkUrl)) { actionError.value = '签名链接无效，请稍后重试。'; return; }
    window.location.assign(linkUrl);
  } catch (e: any) {
    if (disposed || controller.signal.aborted || n !== opSeq) return;
    actionError.value = e.message || '发起重新签名失败，请重试。';
  } finally {
    if (opActive === controller) {
      opActive = null;
      if (!disposed && n === opSeq) selfBusy.value = false;
    }
  }
}

async function sendRequest() {
  const p = person.value;
  if (!p || !canRequestOther.value || requestBusy.value || selfBusy.value) return;
  // Recognized failures (send_failed/recipient_required) may start a fresh
  // operation on an EXPLICIT retry; network/unknown retries keep the SAME id so
  // the sender never creates a duplicate.
  if (recognizedFailure.value) {
    operationId.value = newOperationId();
    recognizedFailure.value = false;
  }
  opActive?.abort();
  const controller = new AbortController();
  opActive = controller;
  const n = ++opSeq;
  requestBusy.value = true; requestError.value = ''; notice.value = '';
  try {
    const data = await requestJson('/api/signatures/management/requests', {
      method: 'POST',
      body: JSON.stringify({
        source: p.source,
        record_id: p.record_id,
        recipient: '',
        operation_id: operationId.value,
      }),
      signal: controller.signal,
      cache: 'no-store',
    });
    if (disposed || controller.signal.aborted || n !== opSeq) return;
    const status = String(data.status || '');
    if (status === 'sent') {
      confirming.value = false;
      requestBusy.value = false;
      notice.value = '签名请求已发送，请该人员通过链接完成签名。';
      await load({ preserveNotice: true });
    } else {
      requestError.value = data.error || requestStatusLabel(status);
      if (status === 'send_failed') {
        recognizedFailure.value = true;
        requestError.value += ' 可再次确认发送。';
      } else if (status === 'recipient_required') {
        recognizedFailure.value = true;
        requestError.value += ' 请前往签名管理中心为该人员选择VNET人员或值班账号代收后重试。';
      } else if (status === 'send_unknown') {
        // Ambiguous send result: keep this operation id (idempotent on backend),
        // and ask the sender to confirm receipt rather than silently resend.
        requestError.value += ' 发送结果待核验，请先确认对方是否已收到；不要重复发送。';
      } else {
        recognizedFailure.value = false;
      }
    }
  } catch (e: any) {
    if (disposed || controller.signal.aborted || n !== opSeq) return;
    // Network/timeout ambiguity — keep the SAME operation id on retry so the
    // sender never duplicates, and ask to confirm receipt first.
    requestError.value = (e.message || '发送失败，请重试。') + ' 如不确定对方是否收到，请先确认后再重试，避免重复发送。';
  } finally {
    if (opActive === controller) {
      opActive = null;
      if (!disposed && n === opSeq) requestBusy.value = false;
    }
  }
}

watch(() => props.openId, () => { cancelInFlight(); void load(); }, { immediate: true });
onBeforeUnmount(() => { disposed = true; cancelInFlight(); });
</script>

<style scoped>
.account-signature {
  display: grid;
  gap: 10px;
  padding: 14px 16px;
  border: 1px solid #dce6f0;
  border-radius: 8px;
  background: #fff;
  color: #203650;
  font-size: 13px;
}
.as-head { display: flex; align-items: center; gap: 9px; }
.as-head-icon { color: #1f62de; flex: none; }
.as-head-copy { display: grid; gap: 1px; min-width: 0; }
.as-head-copy strong { font-size: 14px; }
.as-head-copy span { color: #63778e; font-size: 12px; }
.as-refresh {
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-height: 44px;
  padding: 6px 11px;
  border: 1px solid #ccd8e6;
  border-radius: 7px;
  background: #f8fafc;
  color: #294662;
  font: inherit;
  font-size: 12px;
  cursor: pointer;
}
.as-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 14px; margin: 0; }
.as-grid > div { display: grid; gap: 2px; }
.as-grid dt { color: #63778e; font-size: 12px; }
.as-grid dd { margin: 0; overflow-wrap: anywhere; font-weight: 600; }
.as-badge { display: inline-block; width: fit-content; padding: 2px 7px; border-radius: 999px; font-size: 12px; font-weight: 700; }
.as-badge.ok { background: #e6f5ed; color: #186742; }
.as-badge.warn { background: #fff2df; color: #8b4c03; }
.as-badge.neutral { background: #f0f4f9; color: #506884; }
.as-note { margin: 0; padding: 8px 10px; border-radius: 6px; background: #f4f7fb; color: #40566f; font-size: 12px; line-height: 1.6; }
.as-note.err { background: #fff0f0; color: #a92b2b; }
.as-note.ok { background: #eaf7ef; color: #206a44; }
.as-request { border-top: 1px solid #eceff5; padding-top: 8px; }
.as-request summary { cursor: pointer; color: #294662; font-weight: 600; font-size: 12px; }
.as-request dl { display: grid; gap: 6px; margin: 8px 0 0; }
.as-request dl > div { display: grid; grid-template-columns: 72px minmax(0,1fr); gap: 8px; font-size: 12px; }
.as-request dt { color: #63778e; }
.as-request dd { margin: 0; overflow-wrap: anywhere; }
.as-actions { display: flex; flex-wrap: wrap; gap: 8px; }
.as-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  min-height: 44px;
  padding: 7px 13px;
  border: 1px solid #ccd8e6;
  border-radius: 8px;
  background: #fff;
  color: #294662;
  font: inherit;
  font-size: 13px;
  cursor: pointer;
}
.as-btn.primary { border-color: #1f62de; background: #1f62de; color: #fff; }
.as-btn:hover:not(:disabled) { border-color: #9cbef0; background: #eef5ff; }
.as-btn.primary:hover:not(:disabled) { background: #154cad; color: #fff; border-color: #154cad; }
.as-btn:disabled { opacity: .5; cursor: not-allowed; }
.as-preview { display: grid; place-items: center; min-height: 90px; border: 1px dashed #ccd8e6; border-radius: 8px; background: #fafbfd; padding: 10px; }
.as-preview-img { max-width: 100%; max-height: 180px; object-fit: contain; -webkit-user-drag: none; user-select: none; }
.as-preview-img.hidden { visibility: hidden; }
.as-preview-state { display: inline-flex; align-items: center; gap: 7px; color: #60748e; font-size: 12px; flex-wrap: wrap; }
.as-preview-state.error { color: #a92b2b; }
.as-state { display: flex; align-items: center; gap: 8px; min-height: 40px; color: #63778e; font-size: 13px; flex-wrap: wrap; }
.as-state.error { color: #a92b2b; }
.as-state.info { color: #506884; }
.as-link {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-height: 44px;
  padding: 6px 11px;
  border: 1px solid #ccd8e6;
  border-radius: 7px;
  background: #fff;
  color: #1d60c6;
  font: inherit;
  font-size: 12px;
  cursor: pointer;
}
.as-confirm { border: 1px solid #dce6f0; border-radius: 8px; background: #f8fafc; padding: 10px 12px; display: grid; gap: 10px; }
.as-confirm-copy { margin: 0; font-size: 13px; line-height: 1.6; }
.as-confirm-actions { display: flex; gap: 8px; justify-content: flex-end; flex-wrap: wrap; }
.spin { animation: as-spin .9s linear infinite; }
@keyframes as-spin { to { transform: rotate(360deg); } }
button:focus-visible, summary:focus-visible, img:focus-visible { outline: 2px solid #2563eb; outline-offset: 2px; }
@media (min-width: 561px) {
  .account-signature { padding: 16px 18px; }
  .as-btn, .as-refresh, .as-link { min-height: 36px; }
  .as-refresh, .as-link { padding: 4px 9px; }
}
@media (prefers-reduced-motion: reduce) { .spin { animation: none; } }
</style>
