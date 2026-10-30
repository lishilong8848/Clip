<template>
  <dialog ref="dialog" class="notice-history" :aria-labelledby="titleId" @cancel.prevent="requestClose" @close="nativeClosed">
    <header class="nh-header">
      <div><h2 :id="titleId" ref="heading" tabindex="-1">{{ target ? '通告详情' : '历史通告' }}</h2><p>{{ target ? '查看全部字段 · 保存仅修改表格，不发送消息' : '按实际开始月份查看所有已开始通告，包含进行中 · 北京时间' }}</p></div>
      <button type="button" class="nh-close" :disabled="saving" @click="requestClose">关闭</button>
    </header>

    <div ref="body" class="nh-body" :aria-busy="loading || detailLoading || saving">
      <template v-if="!target">
        <form class="nh-filters" @submit.prevent="applyFilters">
          <label>实际开始月份<input v-model="filters.month" type="month" required /></label>
          <label>楼栋范围<select v-model="filters.scope"><option v-for="option in scopeOptions" :key="option.value" :value="option.value">{{ option.label }}</option></select></label>
          <label>通告类型<select v-model="filters.workType"><option v-for="option in typeOptions" :key="option.value" :value="option.value">{{ option.label }}</option></select></label>
          <label class="nh-search">搜索名称<input v-model="filters.q" type="search" placeholder="输入通告名称" /></label>
          <button type="submit" class="nh-primary">{{ loading ? '重新筛选' : '筛选' }}</button>
        </form>
        <p v-if="filtersDirty" class="nh-muted">筛选条件已修改，点击“筛选”后更新。</p>
        <div v-if="listError" class="nh-feedback nh-error" role="alert">{{ listError }}<button type="button" @click="loadList">重新读取</button></div>
        <ul v-if="warnings.length" class="nh-feedback"><li v-for="(warning, index) in warnings" :key="index">{{ warning }}</li></ul>
        <p class="nh-list-caption" role="status">{{ applied.month }} · {{ scopeLabel(applied.scope) }} · {{ loading ? '正在读取…' : `共 ${total} 条` }}</p>
        <div class="nh-list" :aria-busy="loading">
          <div class="nh-list-head" aria-hidden="true"><span>通告名称</span><span>实际开始时间</span><span>实际结束时间</span><span>状态</span></div>
          <button v-for="row in items" :key="row.work_type + ':' + row.record_id" type="button" class="nh-row" :disabled="loading" @click="openRecord(row)">
            <span class="nh-row-title"><strong>{{ row.title || '未命名通告' }}</strong><small>{{ typeLabel(row.work_type) }} · {{ row.building || scopeLabel(applied.scope) }}</small></span>
            <span class="nh-time"><small>开始</small>{{ formatTime(row.start_time) }}</span>
            <span class="nh-time"><small>结束</small>{{ endTime(row.end_time) }}</span>
            <span class="nh-status">{{ row.status || (hasTime(row.end_time) ? '已结束' : '进行中') }}</span>
          </button>
          <p v-if="!items.length" class="nh-empty">{{ loading ? '正在读取历史通告…' : listError ? '读取失败，请重试。' : '所选月份筛选范围内暂无已开始通告。' }}</p>
        </div>
      </template>

      <template v-else>
        <div class="nh-detail-tools"><button type="button" :disabled="saving" @click="backToList">返回列表</button><button type="button" :disabled="detailLoading || saving || unresolved" @click="refreshDetail">{{ detailLoading ? '正在读取…' : '刷新详情，保留填写' }}</button><span v-if="contextChanged" class="nh-muted">外层范围已变化；当前详情仍属于 {{ scopeLabel(target.scope) }}，返回列表后应用新范围。</span></div>
        <div v-if="detailError" class="nh-feedback nh-error" role="alert">{{ detailError }}</div>
        <div v-if="notice" class="nh-feedback" role="status">{{ notice }}</div>
        <p v-if="detailLoading && !detail" class="nh-empty" role="status">正在读取全部字段…</p>
        <template v-if="detail">
          <ul v-if="detail.warnings?.length" class="nh-feedback" aria-label="详情警告"><li v-for="(warning, index) in detail.warnings" :key="index">{{ warning }}</li></ul>
          <section class="nh-summary" aria-label="通告信息">
            <h3>{{ detail.record.title || '未命名通告' }}</h3>
            <dl><div><dt>工作类型</dt><dd>{{ typeLabel(detail.record.work_type) }}</dd></div><div><dt>通告类型</dt><dd>{{ detail.record.notice_type || '—' }}</dd></div><div><dt>楼栋</dt><dd>{{ detail.record.building || scopeLabel(target.scope) }}</dd></div><div><dt>状态</dt><dd>{{ detail.record.status || (hasTime(detail.record.end_time) ? '已结束' : '进行中') }}</dd></div><div><dt>实际开始时间</dt><dd>{{ formatTime(detail.record.start_time) }}</dd></div><div><dt>实际结束时间</dt><dd>{{ endTime(detail.record.end_time) }}</dd></div><div><dt>记录编号</dt><dd>{{ detail.record.record_id }}</dd></div></dl>
          </section>
          <p v-if="!detail.editable" class="nh-feedback">当前记录只可查看。可修改范围由服务端权限决定。</p>
          <section v-if="attempt" class="nh-operation nh-feedback" aria-label="保存结果" aria-live="polite">
            <strong>{{ saving ? (attempt.action === 'abandon' ? '正在停止剩余同步' : '正在核验并保存') : operationTitle }}</strong><p>{{ attempt.rejected ? '请求在持久化前被拒绝，未创建本次保存操作。填写已保留，可刷新版本后重新核对。' : attempt.result?.message || '保存结果尚未确认。填写和原操作编号已保留，请重试原保存。' }}</p>
            <small>操作编号：{{ attempt.id }}</small>
            <ul v-if="attempt.result?.steps?.length"><li v-for="(step, index) in attempt.result.steps" :key="index"><strong>{{ step.label }}</strong> · {{ stepLabel(step.status) }}<span v-if="step.error"> · {{ step.error }}</span></li></ul>
            <button v-if="unresolved && canManageOperation" type="button" class="nh-primary" :disabled="saving" @click="retryOperation">{{ attempt.action === 'abandon' ? '重试停止剩余同步' : operationPersisted ? '重试原操作' : '重试原保存' }}</button>
            <button v-if="unresolved && operationPersisted && canManageOperation && attempt.action !== 'abandon'" type="button" :disabled="saving" @click="confirmAbandon">停止剩余同步</button>
            <button v-if="attempt.rejected" type="button" :disabled="saving" @click="editRejected">返回修改</button>
          </section>
          <section v-if="latest" class="nh-feedback nh-compare" aria-label="核对最新版本">
            <h3>最新详情已读取，手填内容仍保留</h3><p>{{ unresolved ? '原操作尚未解决，请先重试原操作或停止剩余同步，再核对填写。' : '请核对下列修改与表格中的最新值。确认后使用最新版本保存。' }}</p>
            <ul v-if="latest.warnings?.length"><li v-for="(warning, index) in latest.warnings" :key="index">{{ warning }}</li></ul>
            <p v-if="!latest.editable">最新记录只可查看，不能继续写入。请核对权限或其他人员尚未完成的操作。</p>
            <div v-for="edit in dirtyEntries" :key="edit.group + ':' + edit.field.id" class="nh-comparison">
              <strong>{{ edit.groupLabel }} / {{ edit.field.name }}</strong>
              <dl><div><dt>原值</dt><dd>{{ displayDraft(edit.field, baseline[edit.group]?.[edit.field.id]) }}</dd></div><div><dt>你的填写</dt><dd>{{ displayDraft(edit.field, edit.value) }}</dd></div><div><dt>表格最新值</dt><dd>{{ latestField(edit.group, edit.field.id) ? fieldText(latestField(edit.group, edit.field.id)!) : '该字段已不存在' }}</dd></div></dl>
              <p v-if="!canKeepEdit(edit)" class="nh-error-text">此项目前不可写入，填写已保留。撤销此项后才能采用最新版本。</p>
              <button type="button" :disabled="saving || unresolved" @click="revertEdit(edit)">撤销此项修改</button>
            </div>
            <button type="button" class="nh-primary" :disabled="saving || detailLoading || unresolved || dirtyEntries.some(edit => !canKeepEdit(edit))" @click="adoptLatest">已核对，采用最新版本并保留填写</button>
          </section>

          <form :id="formId" ref="editForm" class="nh-edit-form" @submit.prevent="save">
            <section v-for="group in detail.groups" :key="group.key" class="nh-group">
              <header><h3>{{ group.label }}</h3><small>记录：{{ group.record_id || '—' }}</small></header>
              <div class="nh-fields">
                <div v-for="field in group.fields" :key="field.id" class="nh-field" :class="{'nh-wide': isWide(field), 'nh-dirty': fieldDirty(group.key, field)}">
                  <label :for="fieldId(group.key, field.id)">{{ field.name }}<small v-if="fieldDirty(group.key, field)">已修改</small><small v-else-if="!canEdit(field)">只读</small></label>
                  <template v-if="canEdit(field)">
                    <textarea v-if="field.type === 1" :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id]" :disabled="locked" rows="2" />
                    <input v-else-if="field.type === 2" :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id]" type="number" step="any" :disabled="locked" />
                    <select v-else-if="field.type === 3" :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id]" :disabled="locked"><option :value="null">未选择</option><option v-for="(option, index) in fieldOptions(field)" :key="index" :value="option.value">{{ option.label }}</option></select>
                    <fieldset v-else-if="field.type === 4" :id="fieldId(group.key, field.id)" class="nh-multiple" :disabled="locked"><legend>{{ field.name }}</legend><label v-for="(option, index) in fieldOptions(field)" :key="index"><input v-model="drafts[group.key]![field.id]" type="checkbox" :value="option.value" />{{ option.label }}</label><span v-if="!fieldOptions(field).length" class="nh-muted">暂无可选项</span></fieldset>
                    <div v-else-if="field.type === 5"><input :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id]" type="datetime-local" step="0.001" :disabled="locked" /><small class="nh-muted">北京时间</small></div>
                    <label v-else-if="field.type === 7" class="nh-checkbox"><input :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id]" type="checkbox" :disabled="locked" />{{ drafts[group.key]![field.id] ? '是' : '否' }}</label>
                    <div v-else-if="field.type === 15" class="nh-url"><input :id="fieldId(group.key, field.id)" v-model="drafts[group.key]![field.id].link" type="url" :aria-label="field.name + '链接地址'" placeholder="https://…" :disabled="locked" /><input v-model="drafts[group.key]![field.id].text" type="text" :aria-label="field.name + '显示文字'" placeholder="显示文字" :disabled="locked" /></div>
                  </template>
                  <div v-else :id="fieldId(group.key, field.id)" class="nh-readonly">{{ fieldText(field) }}</div>
                  <small v-if="field.reason" class="nh-muted">{{ field.reason }}</small>
                  <small v-else-if="field.editable && !supportsEditor(field)" class="nh-muted">此字段类型或值格式暂不支持编辑。</small>
                  <ul v-if="field.attachments?.length" class="nh-attachments"><li v-for="(attachment, index) in field.attachments" :key="index"><a v-if="safeAttachment(attachment.url)" :href="safeAttachment(attachment.url)" target="_blank" rel="noopener noreferrer" download>{{ attachment.name || '下载附件' }}</a><span v-else>{{ attachment.name || '附件暂不可下载' }}</span></li></ul>
                </div>
              </div>
              <p v-if="!group.fields.length" class="nh-muted">此记录暂无字段。</p>
            </section>
            <label v-if="detail.editable" class="nh-reason">修改原因（选填）<textarea v-model="reason" rows="2" :disabled="locked" placeholder="填写本次更正原因" /></label>
          </form>
        </template>
      </template>
    </div>

    <footer class="nh-footer">
      <template v-if="!target"><span>每页 {{ pageSize }} 条</span><nav aria-label="历史通告分页"><button type="button" :disabled="loading || page <= 1" @click="changePage(page - 1)">上一页</button><span>{{ page }} / {{ pages }}</span><button type="button" :disabled="loading || page >= pages" @click="changePage(page + 1)">下一页</button></nav></template>
      <template v-else><span role="status">{{ saving ? '正在保存，请等待结果' : `已修改 ${dirtyEntries.length} 项 · 只改表，不发消息` }}</span><button v-if="detail?.editable" type="submit" :form="formId" class="nh-primary" :disabled="locked || !dirtyEntries.length || !!latest || needsRefresh || attempt?.result?.status === 'conflict'">保存修改</button></template>
    </footer>
  </dialog>

  <dialog ref="confirmation" class="notice-history nh-confirm" :aria-labelledby="confirmId" @cancel.prevent="cancelConfirmation">
    <h2 :id="confirmId">{{ confirmTitle }}</h2><p>{{ confirmMessage }}</p><div class="nh-confirm-actions"><button ref="keepButton" type="button" class="nh-primary" @click="cancelConfirmation">{{ confirmCancelLabel }}</button><button type="button" @click="acceptConfirmation">{{ confirmActionLabel }}</button></div>
  </dialog>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from 'vue';
import { ApiError, AUTH_EXPIRED_EVENT, requestJson } from '../api/client';
import { readCacheKey } from '../api/readCache';
// The shared requestJson checks {ok, data} and returns payload.data, not the envelope.

type Option = { value: string; label: string };
type NoticeRecord = { record_id: string; work_type: string; notice_type: string; title: string; building: string; start_time: string | number | null; end_time: string | number | null; status: string };
type Field = { id: string; name: string; type: number; value: any; display?: unknown; editable: boolean; options?: { label: string; value: any }[]; reason?: string; attachments?: { name: string; url: string }[] };
type Group = { key: string; label: string; record_id: string; fields: Field[] };
type Detail = { record: NoticeRecord; revision: string | number; groups: Group[]; editable: boolean; warnings?: string[]; pending_operation?: Operation | string | null };
type Operation = { operation_id: string; status: 'completed' | 'pending' | 'conflict' | 'abandoned'; message: string; steps: { label: string; status: string; error?: string }[]; detail?: Detail };
type ListData = { month: string; items: NoticeRecord[]; total: number; page: number; page_size: number; warnings: string[]; scope_options: Option[]; types: Option[] };
type Values = Record<string, Record<string, any>>;
type DirtyEntry = { group: string; groupLabel: string; field: Field; value: any };
type Target = { scope: string; work_type: string; record_id: string };
type SaveBody = Target & { operation_id: string; expected_revision: string | number; changes: Values; reason?: string };
type OperationAction = 'save' | 'retry' | 'abandon';
type Attempt = { id: string; body: SaveBody | null; result: Operation | null; rejected?: boolean; action?: OperationAction };

const props = withDefaults(defineProps<{ scope: string; workType?: string; show?: boolean }>(), { workType: 'all', show: true });
const emit = defineEmits<{ close: []; changed: [] }>();
const uid = useId(), formId = `notice-history-form-${uid}`, titleId = `notice-history-title-${uid}`, confirmId = `notice-history-confirm-${uid}`;
const dialog = ref<HTMLDialogElement | null>(null), confirmation = ref<HTMLDialogElement | null>(null);
const heading = ref<HTMLElement | null>(null), body = ref<HTMLElement | null>(null), keepButton = ref<HTMLButtonElement | null>(null), editForm = ref<HTMLFormElement | null>(null);
const visible = ref(false), loading = ref(false), detailLoading = ref(false), saving = ref(false);
const filters = ref({ month: beijingMonth(), scope: props.scope, workType: props.workType, q: '' });
const applied = ref({ ...filters.value }), items = ref<NoticeRecord[]>([]), total = ref(0), page = ref(1), pageSize = ref(20);
const warnings = ref<string[]>([]), serverScopes = ref<Option[]>([]), serverTypes = ref<Option[]>([]), listError = ref('');
const target = ref<Target | null>(null), detail = ref<Detail | null>(null), latest = ref<Detail | null>(null);
const drafts = ref<Values>({}), baseline = ref<Values>({}), reason = ref(''), detailError = ref(''), notice = ref('');
const attempt = ref<Attempt | null>(null), needsRefresh = ref(false);
const confirmTitle = ref(''), confirmMessage = ref(''), confirmActionLabel = ref(''), confirmCancelLabel = ref('继续编辑');
let confirmAction: (() => void) | null = null, confirmFocus: HTMLElement | null = null, returnFocus: HTMLElement | null = null;
let listAbort: AbortController | undefined, detailAbort: AbortController | undefined, saveAbort: AbortController | undefined;
let listSequence = 0, detailSequence = 0, saveSequence = 0, disposed = false;
const outerContext = ref(JSON.stringify([props.scope, props.workType]));
const identity = () => readCacheKey('/api/scope-overview', window.location.origin);
let currentIdentity = identity();
const scopeOptions = computed(() => withCurrent(serverScopes.value, filters.value.scope));
const typeOptions = computed(() => withCurrent([{ value: 'all', label: '全部类型' }, ...serverTypes.value.filter(option => option.value !== 'all')], filters.value.workType));
const filtersDirty = computed(() => JSON.stringify(filters.value) !== JSON.stringify(applied.value));
const pages = computed(() => Math.max(1, Math.ceil(total.value / pageSize.value)));
const contextChanged = computed(() => JSON.stringify([props.scope, props.workType]) !== outerContext.value);
const unresolved = computed(() => !!attempt.value && !attempt.value.rejected && (!attempt.value.result || ['pending', 'conflict'].includes(attempt.value.result.status)));
const operationPersisted = computed(() => !!attempt.value && (!attempt.value.body || ['pending', 'conflict'].includes(attempt.value.result?.status || '')));
const canManageOperation = computed(() => !!detail.value?.editable && latest.value?.editable !== false);
const locked = computed(() => saving.value || detailLoading.value || unresolved.value || latest.value?.editable === false);
const dirtyEntries = computed<DirtyEntry[]>(() => (detail.value?.groups || []).flatMap(group => group.fields.filter(field => fieldDirty(group.key, field)).map(field => ({ group: group.key, groupLabel: group.label, field, value: drafts.value[group.key]?.[field.id] }))));
const hasUnsaved = computed(() => dirtyEntries.value.length > 0 || !!reason.value.trim() || unresolved.value);
const operationTitle = computed(() => attempt.value?.rejected ? '保存请求被拒绝' : attempt.value?.result ? ({ completed: '表格已保存，未发送消息', pending: '部分步骤尚未完成', conflict: '原操作存在冲突，请重试或停止剩余同步', abandoned: '已停止剩余同步，已保存字段不回退' })[attempt.value.result.status] : '保存结果待确认');

function clone<T>(value: T): T { return JSON.parse(JSON.stringify(value)); }
function beijingMonth() { return new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().slice(0, 7); }
function hasTime(value: unknown) { return value !== null && value !== undefined && value !== '' && value !== 0 && value !== '0'; }
function formatTime(value: NoticeRecord['start_time']) {
  if (!hasTime(value)) return '—';
  if (typeof value === 'string' && !/^\d+$/.test(value) && !/(?:Z|[+-]\d{2}:?\d{2})$/i.test(value)) return value.replace('T', ' ');
  const date = new Date(typeof value === 'number' || /^\d+$/.test(String(value)) ? Number(value) : String(value));
  return Number.isNaN(date.getTime()) ? String(value) : new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(date);
}
function endTime(value: NoticeRecord['end_time']) { return hasTime(value) ? formatTime(value) : '未结束'; }
function withCurrent(options: Option[], value: string) { return options.some(option => option.value === value) ? options : [{ value, label: value === 'ALL' ? '全部楼栋' : value }, ...options]; }
function scopeLabel(value: string) { return serverScopes.value.find(option => option.value === value)?.label || (value === 'ALL' ? '全部楼栋' : value); }
function typeLabel(value: string) { return serverTypes.value.find(option => option.value === value)?.label || ({ maintenance: '维保', change: '变更', repair: '维修', polling: '巡检', adjust: '设备调整', all: '全部类型' } as Record<string, string>)[value] || value; }
function text(value: unknown): string { return value === null || value === undefined || value === '' ? '—' : typeof value === 'string' ? value : typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value); }
function fieldText(field: Field) { return text(field.display ?? field.value); }
function fieldId(group: string, field: string) { return `nh-${uid}-${encodeURIComponent(group)}-${encodeURIComponent(field)}`; }
function isWide(field: Field) { return fieldText(field).length > 160 || field.type === 4; }
function safeAttachment(value: string) { try { const url = new URL(value, location.origin); return value && ['http:', 'https:'].includes(url.protocol) ? url.href : undefined; } catch { return undefined; } }
function stepLabel(status: string) { return ({ completed: '已完成', success: '已完成', pending: '待完成', running: '进行中', writing: '正在核验', failed: '失败', conflict: '冲突', abandoned: '已停止', skipped: '已跳过' } as Record<string, string>)[status] || status; }
function supportsEditor(field: Field) {
  const value = field.value;
  if (![1, 2, 3, 4, 5, 7, 15].includes(field.type)) return false;
  if (value === null || value === undefined) return true;
  if (field.type === 1) return typeof value === 'string';
  if (field.type === 2) return value === '' || (typeof value === 'number' && Number.isFinite(value));
  if (field.type === 5) return value === '' || (typeof value === 'number' && Number.isFinite(new Date(value + 8 * 60 * 60 * 1000).getTime()));
  if (field.type === 3) return typeof value === 'string' || typeof value === 'number';
  if (field.type === 4) return Array.isArray(value) && value.every(item => typeof item === 'string' || typeof item === 'number');
  if (field.type === 7) return typeof value === 'boolean';
  return typeof value === 'object' && !Array.isArray(value) && (value.link == null || typeof value.link === 'string') && (value.text == null || typeof value.text === 'string');
}
function canEdit(field: Field) { return !!detail.value?.editable && field.editable && supportsEditor(field); }
function inputValue(field: Field) {
  if (!supportsEditor(field)) return field.value ?? null;
  if (field.type === 5) return field.value == null || field.value === '' ? '' : new Date(Number(field.value) + 8 * 60 * 60 * 1000).toISOString().slice(0, 23);
  if (field.type === 15) return { link: field.value?.link ?? '', text: field.value?.text ?? '' };
  if (field.type === 4) return clone(field.value ?? []);
  if (field.type === 7) return field.value ?? false;
  if (field.type === 3) return field.value === '' ? null : field.value ?? null;
  return field.value ?? '';
}
function comparable(field: Field, value: any) {
  if (field.type === 4 && Array.isArray(value)) return JSON.stringify(value.map(item => JSON.stringify(item)).sort());
  if (field.type === 2) return JSON.stringify(value === '' || value == null ? null : Number(value));
  if (field.type === 5) return JSON.stringify(value ? Date.parse(value + '+08:00') : null);
  return JSON.stringify(value);
}
function fieldDirty(group: string, field: Field) { return canEdit(field) && comparable(field, drafts.value[group]?.[field.id]) !== comparable(field, baseline.value[group]?.[field.id]); }
function fieldOptions(field: Field) {
  const options = [...(field.options || [])];
  const values = field.type === 4 ? (field.value || []) : [field.value];
  for (const value of values) if (value != null && value !== '' && !options.some(option => option.value === value)) options.push({ label: `${text(value)}（当前值）`, value });
  return options;
}
function displayDraft(field: Field, value: any) {
  if (field.type === 3) return fieldOptions(field).find(option => option.value === value)?.label || text(value);
  if (field.type === 4 && Array.isArray(value)) return value.map(item => fieldOptions(field).find(option => option.value === item)?.label || text(item)).join('、') || '—';
  if (field.type === 7) return value ? '是' : '否';
  if (field.type === 5) return value ? value.replace('T', ' ') + '（北京时间）' : '—';
  return text(value);
}
function errorText(error: unknown) { return error instanceof Error ? error.message : '读取失败，请稍后重试。'; }
function validDetail(value: Detail, expected: Target) {
  if (!value?.record || value.record.record_id !== expected.record_id || value.record.work_type !== expected.work_type || !Array.isArray(value.groups) || value.revision == null) throw new Error('详情与当前记录不一致，原填写已保留，请重新读取。');
  return value;
}
function installDetail(value: Detail, keep: DirtyEntry[] = []) {
  const initial = Object.fromEntries(value.groups.map(group => [group.key, Object.fromEntries(group.fields.map(field => [field.id, inputValue(field)]))]));
  detail.value = value;
  baseline.value = initial;
  drafts.value = clone(baseline.value);
  for (const entry of keep) drafts.value[entry.group]![entry.field.id] = clone(entry.value);
  needsRefresh.value = false;
}
function pendingAttempt(value: Detail) {
  const pending = value.pending_operation;
  if (!pending) return;
  const id = typeof pending === 'string' ? pending : pending.operation_id;
  if (!id || (typeof pending !== 'string' && !['pending', 'conflict'].includes(pending.status))) return;
  attempt.value = { id, body: null, result: typeof pending === 'string' ? { operation_id: id, status: 'pending', message: '此记录有尚未完成的保存操作，请重试原操作。', steps: [] } : pending };
}

async function loadList() {
  if (!visible.value || target.value) return;
  listAbort?.abort(); const controller = new AbortController(); listAbort = controller;
  const sequence = ++listSequence, who = identity(); loading.value = true; listError.value = '';
  const query = new URLSearchParams({ month: applied.value.month, scope: applied.value.scope, work_type: applied.value.workType, q: applied.value.q, page: String(page.value), page_size: String(pageSize.value) });
  try {
    const data = await requestJson('/api/notice-history?' + query, { signal: controller.signal, cache: 'no-store', fresh: true, timeoutMs: 120000 }) as ListData;
    if (disposed || sequence !== listSequence || controller.signal.aborted || who !== identity()) return;
    if (!data || !Array.isArray(data.items) || !Number.isFinite(data.total) || !(data.page >= 1) || !(data.page_size >= 1)) throw new Error('历史列表格式异常，请重新读取。');
    items.value = data.items; total.value = data.total; page.value = data.page; pageSize.value = data.page_size;
    warnings.value = data.warnings || []; serverScopes.value = data.scope_options || []; serverTypes.value = data.types || [];
  } catch (error) { if (!disposed && sequence === listSequence && !controller.signal.aborted && who === identity()) { listError.value = errorText(error); items.value = []; total.value = 0; } }
  finally { if (sequence === listSequence) loading.value = false; }
}
function applyFilters() {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(filters.value.month)) { listError.value = '请选择有效的实际开始月份。'; return; }
  filters.value.q = filters.value.q.trim(); applied.value = { ...filters.value }; page.value = 1; items.value = []; warnings.value = []; void loadList();
}
function changePage(value: number) { page.value = value; items.value = []; void loadList(); body.value?.scrollTo({ top: 0 }); }
async function openRecord(row: NoticeRecord) {
  if (loading.value) return;
  returnFocus = document.activeElement as HTMLElement;
  target.value = { scope: applied.value.scope, work_type: row.work_type, record_id: row.record_id };
  clearEditor(); await nextTick(); heading.value?.focus(); body.value?.scrollTo({ top: 0 }); await refreshDetail();
}
async function refreshDetail() {
  if (!target.value || saving.value || unresolved.value) return;
  detailAbort?.abort(); const controller = new AbortController(); detailAbort = controller;
  const sequence = ++detailSequence, expected = { ...target.value }, who = identity(); detailLoading.value = true; detailError.value = '';
  try {
    const value = validDetail(await requestJson('/api/notice-history/detail?' + new URLSearchParams(expected), { signal: controller.signal, cache: 'no-store', fresh: true, timeoutMs: 120000 }) as Detail, expected);
    if (disposed || sequence !== detailSequence || controller.signal.aborted || who !== identity()) return;
    if (dirtyEntries.value.length) { latest.value = value; notice.value = '最新详情已读取。请核对后采用最新版本，当前填写尚未替换。'; }
    else { installDetail(value); latest.value = null; attempt.value = null; pendingAttempt(value); }
  } catch (error) { if (!disposed && sequence === detailSequence && !controller.signal.aborted && who === identity()) detailError.value = errorText(error); }
  finally { if (sequence === detailSequence) detailLoading.value = false; }
}
function latestField(group: string, id: string) { return latest.value?.groups.find(item => item.key === group)?.fields.find(field => field.id === id); }
function canKeepEdit(edit: DirtyEntry) {
  const group = latest.value?.groups.find(item => item.key === edit.group), original = detail.value?.groups.find(item => item.key === edit.group);
  const field = latestField(edit.group, edit.field.id);
  return !!latest.value?.editable && group?.record_id === original?.record_id && !!field?.editable && field.type === edit.field.type && supportsEditor(field);
}
function revertEdit(edit: DirtyEntry) { if (saving.value || unresolved.value) return; ask('撤销此项修改？', `“${edit.field.name}”将恢复为本次打开时的值。`, '撤销此项', () => { drafts.value[edit.group]![edit.field.id] = clone(baseline.value[edit.group]![edit.field.id]); }); }
function adoptLatest() {
  if (!latest.value || saving.value || unresolved.value || dirtyEntries.value.some(edit => !canKeepEdit(edit))) return;
  const kept = clone(dirtyEntries.value), value = latest.value; installDetail(value, kept); latest.value = null; attempt.value = null; pendingAttempt(value);
  notice.value = '已采用最新版本并保留填写，请核对后保存。'; detailError.value = '';
}
function wireValue(field: Field, value: any) {
  if (field.type === 2) { if (value === '' || value == null) return null; const number = Number(value); if (!Number.isFinite(number)) throw new Error(`${field.name}必须是有效数字。`); return number; }
  if (field.type === 5) { if (!value) return null; const time = Date.parse(value + '+08:00'); if (!Number.isFinite(time)) throw new Error(`${field.name}必须是有效时间。`); return time; }
  return clone(value);
}
function operationId() { return 'notice-history-' + (globalThis.crypto?.randomUUID?.() || Date.now().toString(36) + '-' + Array.from(globalThis.crypto.getRandomValues(new Uint8Array(16)), byte => byte.toString(16).padStart(2, '0')).join('')); }
async function save() {
  if (locked.value || !detail.value || !target.value || !dirtyEntries.value.length || latest.value || needsRefresh.value || attempt.value?.result?.status === 'conflict') return;
  if (!editForm.value?.reportValidity()) return;
  detailError.value = ''; notice.value = '';
  try {
    const changes: Values = {};
    for (const entry of dirtyEntries.value) (changes[entry.group] ||= {})[entry.field.id] = wireValue(entry.field, entry.value);
    const id = operationId(), command: SaveBody = { ...target.value, operation_id: id, expected_revision: detail.value.revision, changes, ...(reason.value.trim() ? { reason: reason.value.trim() } : {}) };
    attempt.value = { id, body: clone(command), result: null }; await submitAttempt('save');
  } catch (error) { detailError.value = errorText(error); }
}
async function retryOperation() {
  if (!saving.value && unresolved.value && attempt.value && canManageOperation.value) await submitAttempt(attempt.value.action === 'abandon' ? 'abandon' : operationPersisted.value ? 'retry' : 'save');
}
function confirmAbandon() {
  if (saving.value || !unresolved.value || !operationPersisted.value || !canManageOperation.value) return;
  ask('停止剩余同步，已保存字段不回退', '只停止原操作尚未执行的写入。已保存字段保留，当前手填内容会在刷新详情后继续保留供核对。', '确认停止剩余同步', () => { void submitAttempt('abandon'); }, '保留原操作');
}
function editRejected() { if (attempt.value?.rejected && !saving.value) { attempt.value = null; detailError.value = ''; notice.value = '原请求已被服务端拒绝。请更正填写后重新保存。'; } }
async function submitAttempt(action: OperationAction) {
  if (!attempt.value || !target.value || saving.value) return;
  const sent = attempt.value, expected = { ...target.value }, who = identity(), sequence = ++saveSequence;
  const controller = new AbortController(); saveAbort = controller; saving.value = true; detailError.value = ''; sent.rejected = false; sent.action = action;
  try {
    let result: Operation;
    try {
      result = await requestJson(action === 'save' ? '/api/notice-history/save' : '/api/notice-history/operations/' + encodeURIComponent(sent.id) + '/' + action, { method: 'POST', body: JSON.stringify(action === 'save' ? sent.body : {}), signal: controller.signal, timeoutMs: 120000 }) as Operation;
    } catch (error) {
      // The service may use HTTP 409 for the same structured conflict result.
      const conflict = error instanceof ApiError ? error.payload?.data : null;
      if (conflict?.status === 'conflict') result = conflict as Operation; else throw error;
    }
    if (disposed || sequence !== saveSequence || who !== identity() || controller.signal.aborted) return;
    if (!result || result.operation_id !== sent.id || !['completed', 'pending', 'conflict', 'abandoned'].includes(result.status)) throw new Error('保存结果与原操作不一致，请重试原操作核验。');
    sent.result = result;
    if (result.status === 'completed') {
      const retainLocalEdits = !sent.body && dirtyEntries.value.length > 0;
      if (!retainLocalEdits) { baseline.value = clone(drafts.value); reason.value = ''; latest.value = null; }
      needsRefresh.value = true;
      notice.value = result.message || '修改已保存到表格，未发送消息。'; emit('changed');
      if (result.detail) {
        const value = validDetail(result.detail, expected);
        if (retainLocalEdits) { latest.value = value; notice.value += ' 另有本地填写尚未保存，请核对最新版本。'; }
        else { installDetail(value); pendingAttempt(value); }
      }
    } else if (result.status === 'abandoned') {
      latest.value = null; needsRefresh.value = true;
      notice.value = '已停止剩余同步，已保存字段不回退。当前填写已保留，请核对最新详情。'; emit('changed');
    } else if (result.status === 'conflict') {
      notice.value = '原操作发生冲突，填写已保留。请重试原操作或停止剩余同步，不能绕过原操作创建新保存。';
      if (result.detail) latest.value = validDetail(result.detail, expected);
    }
  } catch (error) {
    if (!disposed && sequence === saveSequence && who === identity() && !controller.signal.aborted) {
      detailError.value = errorText(error) + (sent.result?.status === 'completed' ? ' 修改已保存，请刷新详情后继续。' : ' 原填写和操作编号已保留。');
      sent.rejected = action === 'save' && !sent.result && error instanceof ApiError && [400, 409, 422].includes(error.status) && !error.payload?.data;
      if (sent.rejected) { detailError.value = errorText(error) + ' 本次操作未创建，填写已保留。'; needsRefresh.value = true; }
    }
  } finally {
    if (sequence === saveSequence) {
      saving.value = false;
      if (!disposed && who === identity() && ['completed', 'abandoned'].includes(sent.result?.status || '') && needsRefresh.value) await refreshDetail();
    }
  }
}

function ask(title: string, message: string, label: string, action: () => void, cancelLabel = '继续编辑') {
  confirmTitle.value = title; confirmMessage.value = message; confirmActionLabel.value = label; confirmCancelLabel.value = cancelLabel; confirmAction = action; confirmFocus = document.activeElement as HTMLElement;
  if (!confirmation.value?.open) confirmation.value?.showModal(); void nextTick(() => keepButton.value?.focus());
}
function cancelConfirmation() { confirmAction = null; confirmation.value?.close(); confirmFocus?.focus(); }
function acceptConfirmation() { const action = confirmAction; confirmAction = null; confirmation.value?.close(); action?.(); }
function guardedLeave(action: () => void, label: string) {
  if (saving.value) return;
  if (hasUnsaved.value) ask(unresolved.value ? '离开未完成的保存？' : '放弃未保存的修改？', unresolved.value ? '服务端操作可能仍在进行。离开会清除本地填写；重新打开此记录可核验待完成操作。' : '当前填写尚未保存，离开后将丢弃这些修改。', label, action);
  else action();
}
function clearEditor() { detail.value = null; latest.value = null; drafts.value = {}; baseline.value = {}; reason.value = ''; attempt.value = null; notice.value = ''; detailError.value = ''; needsRefresh.value = false; }
function applyOuterContext() { outerContext.value = JSON.stringify([props.scope, props.workType]); filters.value.scope = props.scope; filters.value.workType = props.workType; applied.value = { ...filters.value }; page.value = 1; }
function backToList() {
  guardedLeave(() => { detailAbort?.abort(); ++detailSequence; detailLoading.value = false; target.value = null; clearEditor(); if (contextChanged.value) applyOuterContext(); void loadList(); void nextTick(() => { if (returnFocus?.isConnected) returnFocus.focus(); else heading.value?.focus(); }); }, '放弃填写并返回');
}
function abortRequests() { listAbort?.abort(); detailAbort?.abort(); saveAbort?.abort(); ++listSequence; ++detailSequence; ++saveSequence; loading.value = false; detailLoading.value = false; saving.value = false; }
function closeNow(notify = true) { visible.value = false; abortRequests(); target.value = null; clearEditor(); dialog.value?.close(); if (notify) emit('close'); }
function guardNavigation(proceed: () => void): boolean {
  if (!visible.value) return true;
  if (saving.value) { notice.value = '正在处理原操作，请等待结果后再离开。'; return false; }
  if (hasUnsaved.value) { guardedLeave(() => { closeNow(false); proceed(); }, '放弃填写并离开'); return false; }
  return true;
}
defineExpose({ guardNavigation });
function requestClose() { guardedLeave(closeNow, '放弃填写并关闭'); }
function nativeClosed() { if (!visible.value || dialog.value?.open) return; if (hasUnsaved.value || saving.value) { dialog.value?.showModal(); requestClose(); } else closeNow(); }
async function open() {
  if (visible.value) return;
  visible.value = true; currentIdentity = identity(); applyOuterContext(); items.value = []; total.value = 0; warnings.value = [];
  await nextTick(); if (disposed || !visible.value) return; dialog.value?.showModal(); await loadList();
}
function identityChanged() {
  if (identity() === currentIdentity) return;
  currentIdentity = identity(); cancelConfirmation(); abortRequests(); items.value = []; total.value = 0; serverScopes.value = []; serverTypes.value = []; warnings.value = []; listError.value = '';
  if (visible.value) closeNow(); else { target.value = null; clearEditor(); }
}
function authExpired() { cancelConfirmation(); closeNow(); }
function beforeUnload(event: BeforeUnloadEvent) { if (visible.value && (hasUnsaved.value || saving.value)) { event.preventDefault(); event.returnValue = ''; } }
watch(() => props.show, value => { if (value) void open(); else if (visible.value) requestClose(); });
watch(() => [props.scope, props.workType], () => {
  listAbort?.abort(); ++listSequence; loading.value = false;
  // An open editor owns a captured scope. Never retarget its edits to another building.
  if (target.value) return;
  applyOuterContext(); items.value = []; if (visible.value) void loadList();
});
onMounted(() => { window.addEventListener('clipflow-read-cache-reset', identityChanged); window.addEventListener(AUTH_EXPIRED_EVENT, authExpired); window.addEventListener('beforeunload', beforeUnload); if (props.show) void open(); });
onBeforeUnmount(() => { disposed = true; abortRequests(); confirmation.value?.close(); dialog.value?.close(); window.removeEventListener('clipflow-read-cache-reset', identityChanged); window.removeEventListener(AUTH_EXPIRED_EVENT, authExpired); window.removeEventListener('beforeunload', beforeUnload); });
</script>

<style scoped>
.notice-history{--nh-bg:var(--v2-surface,var(--cf-surface,#fff));--nh-text:var(--v2-ink,var(--cf-text,#20252d));--nh-muted:var(--v2-muted,var(--cf-muted,#596473));--nh-border:var(--v2-border,var(--cf-border,#d5dce5));--nh-accent:var(--v2-blue,var(--cf-brand-blue,#2457e6));--nh-soft:var(--v2-bg,#f3f5f7);box-sizing:border-box;color:var(--nh-text);background:var(--nh-bg);border:1px solid var(--nh-border);border-radius:10px;padding:0;width:min(1320px,calc(100vw - 32px));max-width:calc(100vw - 32px);height:min(920px,calc(100dvh - 32px));max-height:calc(100dvh - 32px);margin:auto;box-shadow:0 20px 80px #0004;font-family:inherit;font-size:13px;line-height:1.5;overflow:hidden;color-scheme:inherit}
.notice-history[open]{display:flex;flex-direction:column}.notice-history::backdrop{background:#10182785}.notice-history *{box-sizing:border-box;min-width:0}.notice-history h2,.notice-history h3,.notice-history p{margin:0}.notice-history h2{font-size:18px;line-height:1.4}.notice-history h3{font-size:14px}.notice-history button,.notice-history input,.notice-history select,.notice-history textarea{font:inherit;line-height:1.5;color:var(--nh-text);border:1px solid var(--nh-border);background:var(--nh-bg);border-radius:5px;max-width:100%;box-shadow:none;margin:0;letter-spacing:normal}.notice-history button{display:inline-flex;justify-content:center;align-items:center;min-height:36px;padding:6px 12px;cursor:pointer;white-space:normal;overflow-wrap:anywhere;flex-shrink:0}.notice-history button:hover:not(:disabled){border-color:var(--nh-accent);background:var(--nh-soft)}.notice-history button:disabled{opacity:.55;cursor:not-allowed}.notice-history :is(button,input,select,textarea,a):focus-visible{outline:2px solid var(--nh-accent);outline-offset:2px}.notice-history input:not([type=checkbox]),.notice-history select,.notice-history textarea{display:block;width:100%;padding:6px 9px;min-height:36px}.notice-history input[type=checkbox]{width:17px;height:17px;flex:none;accent-color:var(--nh-accent)}.notice-history textarea{resize:vertical;white-space:pre-wrap}.notice-history .nh-primary{background:var(--nh-accent);border-color:transparent;color:var(--v2-on-accent,#fff);font-weight:600}.notice-history .nh-primary:hover:not(:disabled){background:var(--nh-accent);filter:brightness(.94)}
.nh-header,.nh-footer{display:flex;align-items:center;justify-content:space-between;gap:12px;flex:none;padding:14px 18px}.nh-header{border-bottom:1px solid var(--nh-border)}.nh-header>div{flex:1}.nh-header p{color:var(--nh-muted);font-size:12px;margin-top:4px;overflow-wrap:anywhere}.nh-close{font-weight:600}.nh-body{overflow:auto;overscroll-behavior:contain;min-height:0;flex:1;padding:14px 18px;scrollbar-gutter:stable;overflow-wrap:anywhere}.nh-footer{border-top:1px solid var(--nh-border);font-size:12px;flex-wrap:wrap;background:var(--nh-bg);padding-bottom:max(12px,env(safe-area-inset-bottom))}.nh-footer nav,.nh-detail-tools{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.nh-footer nav span{min-width:50px;text-align:center}.nh-filters{display:grid;grid-template-columns:160px minmax(100px,.8fr) minmax(110px,.8fr) minmax(160px,1.5fr) auto;gap:10px;align-items:end}.nh-filters label,.nh-reason{display:grid;gap:5px;font-size:12px}.nh-muted{color:var(--nh-muted);font-size:12px}.nh-list-caption{display:block;color:var(--nh-muted);padding:14px 0 8px}.nh-feedback{margin:12px 0!important;border:1px solid var(--nh-border);border-left:3px solid var(--nh-accent);background:var(--nh-soft);padding:10px 12px;border-radius:5px;overflow-wrap:anywhere}.nh-feedback button{margin:8px 8px 0 0}.nh-feedback p{margin:5px 0}.nh-feedback ul{padding-left:18px}.nh-error{border-left-color:#b42318}.nh-error-text{color:var(--cf-danger,#b42318)}.nh-empty{padding:36px 12px;text-align:center;color:var(--nh-muted)}.nh-list{border:1px solid var(--nh-border);border-radius:6px;overflow:hidden}.nh-list-head,.notice-history .nh-row{display:grid;grid-template-columns:minmax(180px,1.8fr) minmax(150px,1fr) minmax(150px,1fr) minmax(65px,.5fr);gap:12px;align-items:center}.nh-list-head{background:var(--nh-soft);padding:8px 12px;font-size:12px;color:var(--nh-muted)}.notice-history .nh-row{width:100%;border:0;border-top:1px solid var(--nh-border);border-radius:0;padding:12px;text-align:left;font-weight:400;justify-content:normal}.nh-row-title{display:grid;gap:4px;overflow-wrap:anywhere}.nh-row-title strong{font-size:14px}.nh-row-title small{font-size:12px;color:var(--nh-muted)}.nh-time{font-size:12px;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}.nh-time small{display:none}.nh-status{font-size:12px;justify-self:start;background:var(--nh-soft);padding:2px 6px;border-radius:4px;overflow-wrap:anywhere}.nh-detail-tools{margin-bottom:12px}.nh-detail-tools>span{flex-basis:100%}.nh-summary{border:1px solid var(--nh-border);border-radius:6px;padding:12px}.nh-summary h3{font-size:16px;white-space:pre-wrap;overflow-wrap:anywhere}.nh-summary dl{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:12px 0 0}.nh-summary dt,.nh-comparison dt{font-size:12px;color:var(--nh-muted)}.nh-summary dd,.nh-comparison dd{margin:3px 0 0;white-space:pre-wrap;overflow-wrap:anywhere}.nh-operation small{display:block;overflow-wrap:anywhere}.nh-group{margin-top:16px;border-top:1px solid var(--nh-border);padding-top:12px}.nh-group>header{display:flex;align-items:baseline;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:10px}.nh-group>header small{font-size:11px;color:var(--nh-muted);overflow-wrap:anywhere}.nh-fields{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px 16px;align-items:start}.nh-field>label{display:flex;gap:6px;align-items:center;flex-wrap:wrap;font-size:12px;margin-bottom:4px}.nh-field>label small{font-size:11px;color:var(--nh-muted)}.nh-field.nh-dirty>label small{color:var(--nh-accent);font-weight:600}.nh-field>small{display:block;margin-top:4px}.nh-wide{grid-column:span 2}.nh-readonly{padding:7px 9px;min-height:36px;border:1px solid var(--nh-border);background:var(--nh-soft);border-radius:5px;white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px}.nh-multiple{border:1px solid var(--nh-border);border-radius:5px;padding:6px 9px;margin:0;display:flex;gap:4px 12px;flex-wrap:wrap}.nh-multiple legend{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)}.nh-multiple label,.nh-checkbox{display:flex;align-items:center;gap:8px;min-height:30px;overflow-wrap:anywhere}.nh-checkbox{padding:3px}.nh-url{display:grid;gap:6px}.nh-attachments{list-style:none;padding:0;margin:6px 0 0}.nh-attachments a{display:inline-block;padding:5px 0;color:var(--nh-accent);overflow-wrap:anywhere;text-decoration:underline}.nh-reason{margin-top:16px;border-top:1px solid var(--nh-border);padding-top:12px}.nh-comparison{padding:10px 0;border-top:1px solid var(--nh-border)}.nh-comparison dl{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:6px 0}.nh-confirm{width:min(440px,calc(100vw - 32px));height:auto;padding:20px;gap:14px;overflow:auto}.nh-confirm p{white-space:pre-wrap;overflow-wrap:anywhere}.nh-confirm-actions{display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap}
@media(min-width:1200px){.nh-fields{grid-template-columns:repeat(4,minmax(0,1fr))}}
@media(max-width:900px){.nh-filters{grid-template-columns:repeat(3,minmax(0,1fr))}.nh-search{grid-column:span 2}.nh-fields{grid-template-columns:repeat(2,minmax(0,1fr))}.nh-summary dl{grid-template-columns:repeat(3,minmax(0,1fr))}.nh-list-head,.notice-history .nh-row{grid-template-columns:minmax(120px,1.3fr) minmax(110px,1fr) minmax(110px,1fr) 62px;gap:8px}}
@media(max-width:600px){.notice-history{width:calc(100vw - 12px);max-width:calc(100vw - 12px);height:calc(100dvh - 12px);max-height:calc(100dvh - 12px);border-radius:8px}.nh-header,.nh-footer{padding:10px 12px;gap:8px}.nh-header h2{font-size:17px}.nh-body{padding:10px 12px;scrollbar-gutter:auto}.notice-history button{min-height:44px}.notice-history input:not([type=checkbox]),.notice-history select,.notice-history textarea{min-height:44px;font-size:16px;width:100%;max-width:100%}.nh-filters{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.nh-filters .nh-search{grid-column:1/-1;grid-row:2}.nh-filters>button{grid-column:2;grid-row:3}.nh-filters>label:nth-child(3){grid-row:3}.nh-list-head{display:none}.notice-history .nh-row{grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:12px}.nh-row:first-of-type{border-top:0}.nh-row-title{grid-column:1/-1}.nh-time{grid-column:1;display:flex;flex-wrap:wrap;gap:6px}.nh-time small{display:inline;color:var(--nh-muted);font-size:12px}.nh-status{grid-column:2;grid-row:2/span 2}.nh-fields{grid-template-columns:minmax(0,1fr);gap:12px}.nh-wide{grid-column:auto}.nh-summary dl{grid-template-columns:repeat(2,minmax(0,1fr))}.nh-comparison dl{grid-template-columns:minmax(0,1fr);gap:8px}.nh-footer>span{flex:1 1 100%}.nh-footer nav{justify-content:space-between;width:100%}.nh-footer>.nh-primary{width:100%}.nh-multiple label,.nh-checkbox{min-height:44px}.nh-confirm{width:calc(100vw - 32px);max-width:440px;height:auto;padding:18px}.nh-confirm-actions button{flex:1}.nh-attachments a{padding:12px 0}.nh-detail-tools button{flex:1}.nh-header .nh-close{align-self:flex-start}}
</style>
