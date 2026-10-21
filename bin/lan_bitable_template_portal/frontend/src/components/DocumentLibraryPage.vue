<template>
  <NConfigProvider :locale="zhCN" :date-locale="dateZhCN">
    <section class="document-library" :style="styles" aria-label="南通文件" data-v2-inline-feedback>
      <VnetBackButton to="/?entry=tools" :disabled="busy" />
      <header class="document-toolbar" data-v2-navigation>
        <div class="document-heading"><FolderOpen :size="21" aria-hidden="true" /><h1>南通文件</h1></div>
        <div class="document-actions">
          <NButton v-if="canConfigure" :disabled="busy || tablesLoading || !!tablesError" title="接入数据表" aria-label="接入数据表" @click="openSettings(true)"><Plus :size="16" /><span>接入表</span></NButton>
          <NButton v-if="canConfigure && activeTable" :disabled="busy || tablesLoading || !!tablesError" title="数据表配置" aria-label="数据表配置" @click="openSettings(false)"><Settings2 :size="16" /></NButton>
          <NButton :loading="tablesLoading || listLoading" :disabled="busy" title="刷新文件列表" aria-label="刷新文件列表" @click="reload"><RefreshCw :size="16" /></NButton>
          <NButton v-if="activeTable?.capabilities?.create" type="primary" :disabled="busy || !schema || !!tablesError" @click="createRecord"><Plus :size="16" />新建资料</NButton>
        </div>
      </header>
      <NAlert v-if="tablesError" type="error" :bordered="false" role="alert">{{ tablesError }} <NButton text :disabled="busy" @click="loadTables()">重新读取数据表</NButton></NAlert>
      <NAlert v-if="notice" type="success" :bordered="false" role="status">{{ notice }}</NAlert>
      <form v-if="tables.items.length" class="document-filters" @submit.prevent="searchRecords">
        <NSelect :value="activeKey" :options="tables.items.map(table => ({ value: table.key, label: table.name }))" :disabled="busy || tablesLoading" aria-label="文件数据表" @update:value="selectTable" />
        <NInput v-model:value="query" clearable :disabled="busy" placeholder="搜索资料" :input-props="{ 'aria-label': '搜索资料', type: 'search' }"><template #prefix><Search :size="16" /></template></NInput>
        <NSelect v-if="scopeOptions.length" :value="scope" :options="[{ value: '', label: '全部授权楼栋' }, ...scopeOptions]" :disabled="busy" aria-label="资料楼栋" @update:value="changeScope" />
        <NButton attr-type="submit" :disabled="busy || listLoading"><Search :size="16" />查询</NButton>
      </form>
      <NAlert v-if="listError" type="error" :bordered="false" role="alert">{{ listError }} <NButton text :disabled="busy || listLoading" @click="readRecords(currentCursor, cursorTrail)">重试读取列表</NButton></NAlert>
      <div v-if="tablesLoading && !tables.items.length || listLoading && !records.length" class="document-loading" aria-label="正在读取资料" role="status"><NSkeleton v-for="index in 5" :key="index" height="48px" /></div>
      <NEmpty v-else-if="!tablesLoading && !tablesError && !tables.items.length" description="尚无可访问的数据表" />
      <template v-else-if="schema">
        <div v-if="records.length" class="document-results" :aria-busy="listLoading">
          <div class="document-table-wrap">
            <table class="document-table">
              <thead><tr><th v-for="field in displayFields" :key="field.id" scope="col">{{ field.name }}</th><th scope="col">附件 / 索引</th><th scope="col"><span class="document-sr-only">操作</span></th></tr></thead>
              <tbody><tr v-for="record in records" :key="record.record_id">
                <td v-for="(field, index) in displayFields" :key="field.id">
                  <button v-if="index === 0" class="document-record-title" type="button" :disabled="busy" :aria-label="'查看资料：' + title(record)" @click="openRecord(record.record_id)"><FileText :size="17" /><DocumentLibraryField :field="field" :model-value="record.fields[field.id]" :interactive="false" /></button>
                  <DocumentLibraryField v-else :field="field" :model-value="record.fields[field.id]" />
                </td>
                <td><div class="document-record-meta"><span><Paperclip :size="14" />{{ record.attachments?.length || 0 }}</span><NTag :type="indexFailed(record) ? 'warning' : 'default'" :bordered="false" size="small">{{ indexLabel(record) }}</NTag></div></td>
                <td><NButton quaternary :disabled="busy" :aria-label="'打开资料：' + title(record)" title="查看详情" @click="openRecord(record.record_id)"><ChevronRight :size="17" /></NButton></td>
              </tr></tbody>
            </table>
          </div>
          <div class="document-mobile-list"><article v-for="record in records" :key="record.record_id"><button class="document-mobile-title" type="button" :disabled="busy" @click="openRecord(record.record_id)"><FileText :size="20" /><strong>{{ title(record) }}</strong><ChevronRight :size="17" /></button><dl><template v-for="field in displayFields.slice(1, 4)" :key="field.id"><dt>{{ field.name }}</dt><dd><DocumentLibraryField :field="field" :model-value="record.fields[field.id]" /></dd></template></dl><div class="document-record-meta"><span><Paperclip :size="14" />{{ record.attachments?.length || 0 }} 个附件</span><NTag :type="indexFailed(record) ? 'warning' : 'default'" size="small" :bordered="false">{{ indexLabel(record) }}</NTag></div></article></div>
        </div>
        <NEmpty v-else-if="listReady && !listLoading && !listError" description="没有符合条件的资料" />
        <footer v-if="records.length" class="document-pagination"><span>{{ records.length }} 条{{ listLoading ? ' · 正在读取' : '' }}</span><div><NButton :disabled="busy || listLoading || !cursorTrail.length" aria-label="上一页资料" title="上一页" @click="previousPage"><ChevronLeft :size="16" /></NButton><NButton :disabled="busy || listLoading || !hasMore" aria-label="下一页资料" title="下一页" @click="nextPage"><ChevronRight :size="16" /></NButton></div></footer>
      </template>

      <NDrawer :show="detailOpen" width="min(800px, 100vw)" :mask-closable="!busy" :close-on-esc="!busy" @update:show="value => { if (!value) closeDetail() }">
        <NDrawerContent :title="creating ? '新建资料' : detail ? title(detail) : '资料详情'" :closable="!busy" :style="styles">
          <div class="document-detail" data-v2-inline-feedback>
            <NAlert v-if="detailError" type="error" :bordered="false" role="alert">{{ detailError }} <NButton v-if="!detail && targetId" text :disabled="busy" @click="openRecord(targetId)">重试读取详情</NButton></NAlert>
            <NAlert v-if="pendingWrite || pendingUpload" type="warning" :bordered="false" role="alert">操作回执尚未确认，输入及原操作编号已保留。请核验后按原操作重试。</NAlert>
            <NAlert v-if="conflict" type="warning" :bordered="false" role="alert">资料已被更新，尚未保存的输入已保留。<NButton text :disabled="busy" @click="readLatest">读取最新版本，保留输入</NButton></NAlert>
            <NAlert v-if="detail && indexFailed(detail)" type="warning" :bordered="false" role="status">资料已保存，知识索引待重试。</NAlert>
            <NAlert v-else-if="detail && indexState(detail) === 'unsupported'" type="warning" :bordered="false" role="status">资料已保存，部分附件格式暂不支持正文解析。</NAlert>
            <NAlert v-else-if="notice" type="success" :bordered="false" role="status">{{ notice }}</NAlert>
            <details v-if="indexIssues.length" class="document-index-issues"><summary>索引详情 · {{ indexIssues.length }} 项需关注</summary><ul><li v-for="item in indexIssues" :key="item.kind + ':' + (item.attachment_id || item.name)"><strong>{{ item.kind === 'record' ? '记录字段' : item.name }}</strong><span>{{ item.error || '索引暂未完成，系统将自动重试。' }}</span></li></ul></details>
            <div v-if="detailLoading" class="document-loading" role="status" aria-label="正在读取详情"><NSkeleton v-for="index in 4" :key="index" height="44px" /></div>
            <template v-else-if="detail || creating">
              <div class="document-detail-bar"><NTag v-if="detail" :bordered="false" size="small" :title="detail.revision">修订 {{ detail.revision.slice(0, 10) }}</NTag><NButton v-if="!editing && detail?.capabilities.edit" :disabled="busy || !!pendingUpload" @click="editing = true"><Pencil :size="15" />编辑资料</NButton><NButton v-if="detail && !editing" :disabled="busy || !!pendingUpload" title="重新读取资料" aria-label="重新读取资料" @click="readLatest"><RefreshCw :size="15" /></NButton></div>
              <NTabs :value="detailTab" type="line" @update:value="selectDetailTab"><NTab name="fields">资料详情</NTab><NTab v-if="detail" name="versions" :disabled="editing || busy">附件版本</NTab></NTabs>
              <form v-if="detailTab === 'fields'" id="document-record-form" class="document-field-grid" @submit.prevent="saveRecord">
                <label v-for="field in schema?.fields" :key="field.id" class="document-detail-field" :class="{ 'document-wide': ['text', 'files', 'readonly'].includes(fieldKind(field)) }"><span>{{ field.name }}<span v-if="editing && field.required && canEditField(field)" class="document-required"> *</span><LockKeyhole v-if="editing && !canEditField(field)" :size="12" aria-label="只读字段" /></span><DocumentLibraryField :field="field" :model-value="editing ? draft[field.id] : detail?.fields[field.id]" :editing="editing && canEditField(field)" :disabled="busy || !!pendingWrite || conflict" @update:model-value="value => draft[field.id] = value" /><small v-if="field.read_only_reason" class="document-muted">{{ field.read_only_reason }}</small><small v-if="fieldErrors[field.id]" class="document-error">{{ fieldErrors[field.id] }}</small></label>
                <label v-if="editing" class="document-detail-field document-wide"><span>变更原因 *</span><NInput v-model:value="reason" type="textarea" :autosize="{ minRows: 2, maxRows: 4 }" :disabled="busy || !!pendingWrite" :maxlength="500" :input-props="{ 'aria-label': '变更原因' }" /></label>
              </form>
              <section v-if="detailTab === 'fields' && detail" class="document-attachments" aria-label="当前附件"><h2>当前附件 <span>{{ detail.attachments.length }}</span></h2><div v-for="file in detail.attachments" :key="file.id" class="document-attachment"><img v-if="file.mime_type?.startsWith('image/')" :src="fileUrl(file)" :alt="file.name" width="40" height="40" loading="lazy" /><FileText v-else :size="23" aria-hidden="true" /><div><strong>{{ file.name }}</strong><small>{{ schema?.fields.find(field => field.id === file.field_id)?.name }} · {{ sizeText(file.size) }}{{ file.version ? ' · V' + file.version : '' }}</small></div><a :href="fileUrl(file)" target="_blank" rel="noopener noreferrer" :aria-label="'打开附件：' + file.name" title="打开附件"><ExternalLink :size="16" /></a></div><p v-if="!detail.attachments.length" class="document-muted">暂无附件</p>
                <div v-if="detail.capabilities.upload && !editing" class="document-upload"><div class="document-upload-controls"><NSelect v-model:value="uploadField" :options="attachmentFields.map(field => ({ value: field.id, label: field.name }))" :disabled="busy || !!pendingUpload" aria-label="上传附件字段" /><NSelect v-model:value="replaceId" :options="[{ value: '', label: '新增附件版本' }, ...detail.attachments.filter(file => file.field_id === uploadField).map(file => ({ value: file.id, label: '替换：' + file.name }))]" :disabled="busy || !!pendingUpload" aria-label="附件替换目标" /></div><label class="document-file-picker"><Upload :size="18" /><span>{{ pickedFile?.name || '选择附件' }}</span><input type="file" :disabled="busy || !!pendingUpload" aria-label="选择资料附件" @change="pickFile" /></label><label class="document-detail-field"><span>附件变更原因 *</span><NInput v-model:value="uploadReason" :disabled="busy || !!pendingUpload" :input-props="{ 'aria-label': '附件变更原因' }" /></label><NProgress v-if="uploading" type="line" :percentage="Math.min(progress, 99)" :show-indicator="true" /><NButton type="primary" :loading="uploading" :disabled="busy || !pickedFile || !uploadField || !uploadReason.trim() || conflict" @click="uploadAttachment"><Upload :size="16" />{{ pendingUpload ? '按原操作重试上传' : '上传附件' }}</NButton></div>
              </section>
              <section v-if="detailTab === 'versions'" class="document-versions" aria-label="附件版本记录">
                <NAlert v-if="versionsError" type="error" :bordered="false" role="alert">{{ versionsError }} <NButton text @click="loadVersions">重试读取版本</NButton></NAlert>
                <NSpin :show="versionsLoading">
                  <ol v-if="versions.length"><li v-for="file in versions" :key="file.id + ':' + file.version">
                    <History :size="17" /><div><strong>{{ file.name }}</strong><small>V{{ file.version || '—' }} · {{ file.created_at || '时间未提供' }}{{ file.created_by ? ' · ' + file.created_by : '' }}</small></div>
                    <NTag v-if="file.current" size="small" :bordered="false">当前</NTag><a :href="fileUrl(file)" target="_blank" rel="noopener noreferrer" :aria-label="'打开历史附件：' + file.name" title="打开历史附件"><ExternalLink :size="16" /></a>
                  </li></ol>
                  <NEmpty v-else-if="!versionsLoading && !versionsError && versionsReady" description="暂无附件版本记录" />
                </NSpin>
              </section>
            </template>
          </div>
          <template #footer><div class="document-detail-footer"><NButton :disabled="busy" @click="closeDetail">{{ editing ? '取消' : '关闭' }}</NButton><NButton v-if="editing" type="primary" attr-type="submit" form="document-record-form" :loading="saving" :disabled="busy || conflict || !reason.trim() || !creating && !dirty">{{ pendingWrite ? '按原操作重试保存' : '保存资料' }}</NButton></div></template>
        </NDrawerContent>
      </NDrawer>
      <DocumentLibraryTableSettings :show="settingsOpen" :initial="settingsNew ? undefined : activeTable" :schema="settingsNew ? undefined : schema || undefined" :scopes="configurationScopes" @close="closeSettings" @saved="configurationSaved" />
      <NModal v-model:show="discardOpen" preset="dialog" title="离开未保存的资料？" :content="pendingWrite || pendingUpload ? '操作回执尚未确认，离开后请先核验资料，避免重复新增。' : '未保存的输入将被放弃。'" positive-text="离开" negative-text="继续填写" @positive-click="confirmDiscard" />
    </section>
  </NConfigProvider>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NConfigProvider, NDrawer, NDrawerContent, NEmpty, NInput, NModal, NProgress, NSelect, NSkeleton, NSpin, NTab, NTabs, NTag, dateZhCN, zhCN, useThemeVars } from 'naive-ui';
import { ChevronLeft, ChevronRight, ExternalLink, FileText, FolderOpen, History, LockKeyhole, Paperclip, Pencil, Plus, RefreshCw, Search, Settings2, Upload } from 'lucide-vue-next';
import { registerNavigationGuard } from '../navigation';
import { ApiError } from '../api/client';
import { cloneLibraryValue as clone, documentLibraryApi as api, fieldKind, fieldValue, newLibraryOperationId, valueText, writableField,
  type LibraryAttachment, type LibraryField, type LibraryFields, type LibraryMutation, type LibraryRecord, type LibrarySchema, type LibraryTable, type LibraryTables } from '../api/documentLibrary';
import DocumentLibraryField from './DocumentLibraryField.vue';
import DocumentLibraryTableSettings from './DocumentLibraryTableSettings.vue';
import VnetBackButton from './VnetBackButton.vue';

const props = defineProps<{ scope?: string }>();
const theme = useThemeVars();
const styles = computed(() => ({ '--doc-ink': theme.value.textColor1, '--doc-muted': theme.value.textColor3,
  '--doc-line': theme.value.borderColor, '--doc-surface': theme.value.cardColor, '--doc-soft': theme.value.tableHeaderColor,
  '--doc-accent': theme.value.primaryColor, '--doc-error': theme.value.errorColor }));
const tables = ref<LibraryTables>({ items: [] }), activeKey = ref(''), schema = ref<LibrarySchema | null>(null);
const records = ref<LibraryRecord[]>([]), tablesLoading = ref(false), listLoading = ref(false), listReady = ref(false);
const tablesError = ref(''), listError = ref(''), notice = ref(''), query = ref(''), appliedQuery = ref(''), scope = ref(props.scope || '');
const currentCursor = ref(''), cursorTrail = ref<string[]>([]), nextCursor = ref(''), hasMore = ref(false), actorScopes = ref<Array<{ value: string; label: string }>>([]);
const activeTable = computed(() => tables.value.items.find(table => table.key === activeKey.value));
const canConfigure = computed(() => typeof tables.value.can_configure === 'boolean' ? tables.value.can_configure
  : tables.value.capabilities?.configure_tables === true || tables.value.actor?.can_configure === true);
const configurationScopes = computed(() => tables.value.scopes || actorScopes.value);
const scopeOptions = computed(() => configurationScopes.value.filter(scope => !activeTable.value?.read_scopes || activeTable.value.read_scopes.includes(scope.value)));
const displayFields = computed(() => (schema.value?.display_fields || []).map(id => schema.value!.fields.find(field => field.id === id)).filter((field): field is LibraryField => !!field));
const attachmentFields = computed(() => (schema.value?.attachment_fields || []).map(id => schema.value!.fields.find(field => field.id === id)).filter((field): field is LibraryField => !!field));
const detailOpen = ref(false), detail = ref<LibraryRecord | null>(null), targetId = ref(''), detailLoading = ref(false), detailError = ref('');
const editing = ref(false), creating = ref(false), detailTab = ref('fields'), draft = ref<LibraryFields>({}), baseline = ref<LibraryFields>({}), reason = ref('');
const fieldErrors = ref<Record<string, string>>({}), conflict = ref(false), saving = ref(false), uploading = ref(false), progress = ref(0);
const pendingWrite = ref<LibraryMutation | null>(null);
type UploadIntent = { file: File; field_id: string; expected_revision: string; operation_id: string; reason: string; replace_attachment_id: string };
const pendingUpload = ref<UploadIntent | null>(null), pickedFile = ref<File | null>(null), uploadField = ref(''), replaceId = ref(''), uploadReason = ref('');
const versions = ref<LibraryAttachment[]>([]), versionsLoading = ref(false), versionsError = ref(''), versionsReady = ref(false);
const settingsOpen = ref(false), settingsNew = ref(false), discardOpen = ref(false);
const busy = computed(() => saving.value || uploading.value);
const dirty = computed(() => JSON.stringify(draft.value) !== JSON.stringify(baseline.value));
let tablesRead: AbortController | undefined, listRead: AbortController | undefined, detailRead: AbortController | undefined, versionRead: AbortController | undefined;
let leaveAction: (() => void) | undefined, unregisterNavigation: (() => void) | undefined, disposed = false, contextReady = false;
function errorText(error: unknown) { return error instanceof Error ? error.message : '请求未完成，请重试。'; }
function title(record: LibraryRecord) { return displayFields.value.length ? valueText(record.fields[displayFields.value[0].id]) : record.record_id; }
function indexState(record: LibraryRecord) { return typeof record.knowledge_status === 'string' ? record.knowledge_status : record.knowledge_status?.state || ''; }
function indexFailed(record: LibraryRecord) { return ['failed', 'error', 'pending_retry', 'retrying', 'partial', 'blocked'].includes(indexState(record)); }
function indexLabel(record: LibraryRecord) { const state = indexState(record); return indexFailed(record) ? '待索引重试' : state === 'unsupported' ? '部分格式不支持' : ['indexed', 'ready', 'succeeded', 'synced'].includes(state) ? '已索引' : state === 'none' ? '等待索引同步' : state ? '待索引' : '未提供索引状态'; }
const indexIssues = computed(() => (detail.value?.knowledge_items || []).filter(item => ['failed', 'blocked', 'retrying', 'unsupported'].includes(item.status)));
function sizeText(size?: number) { return size === undefined ? '大小未提供' : size < 1024 ? size + ' B' : size < 1048576 ? (size / 1024).toFixed(1) + ' KB' : (size / 1048576).toFixed(1) + ' MB'; }
function fileUrl(file: LibraryAttachment) { return api.fileUrl(activeKey.value, detail.value!.record_id, file); }
function hasUnsaved() { return settingsOpen.value || detailOpen.value && (editing.value && dirty.value || !!pickedFile.value || !!pendingWrite.value || !!pendingUpload.value); }
function attemptLeave(action: () => void): boolean {
  if (busy.value) return false;
  if (hasUnsaved()) { leaveAction = action; discardOpen.value = true; return false; }
  action(); return true;
}
function confirmDiscard() { const action = leaveAction; leaveAction = undefined; discardOpen.value = false; action?.(); }
function beforeUnload(event: BeforeUnloadEvent) { if (busy.value || hasUnsaved()) { event.preventDefault(); event.returnValue = ''; } }
async function loadTables(preferred?: string) {
  tablesRead?.abort(); const read = tablesRead = new AbortController(); tablesLoading.value = true; tablesError.value = '';
  try {
    const result = await api.tables(read.signal);
    if (!contextReady && !result.scopes) {
      const context = await api.context(read.signal);
      if (read.signal.aborted || disposed) return;
      actorScopes.value = context.scopes; contextReady = true;
    }
    if (read.signal.aborted || disposed) return;
    tables.value = result;
    activeKey.value = result.items.some(table => table.key === (preferred || activeKey.value)) ? preferred || activeKey.value : result.items[0]?.key || '';
    await loadTable();
  } catch (error) { if (!read.signal.aborted && !disposed) tablesError.value = errorText(error); }
  finally { if (tablesRead === read) tablesLoading.value = false; }
}
async function loadTable() {
  listRead?.abort(); detailRead?.abort(); versionRead?.abort(); detailOpen.value = false; detail.value = null;
  records.value = []; schema.value = null; listReady.value = false; listError.value = ''; currentCursor.value = ''; cursorTrail.value = [];
  if (!activeKey.value) return;
  const read = listRead = new AbortController(); const key = activeKey.value; listLoading.value = true;
  try {
    const result = await api.schema(key, read.signal);
    if (read.signal.aborted || disposed) return;
    schema.value = result; await readRecords('', []);
  } catch (error) { if (!read.signal.aborted && !disposed) listError.value = errorText(error); }
  finally { if (listRead === read) listLoading.value = false; }
}
async function readRecords(cursor = '', trail: string[] = []) {
  if (!schema.value) { await loadTable(); return; }
  listRead?.abort(); const read = listRead = new AbortController(); listLoading.value = true; listError.value = '';
  try {
    const result = await api.records(activeKey.value, { scope: scope.value, q: appliedQuery.value, cursor, page_size: 30 }, read.signal);
    if (read.signal.aborted || disposed) return;
    records.value = result.items; hasMore.value = result.has_more; nextCursor.value = result.cursor;
    currentCursor.value = cursor; cursorTrail.value = trail.slice(); listReady.value = true;
  } catch (error) { if (!read.signal.aborted && !disposed) listError.value = errorText(error); }
  finally { if (listRead === read) listLoading.value = false; }
}
function reload() { attemptLeave(() => { detailOpen.value = false; void loadTables(); }); }
function selectTable(key: string) { if (key !== activeKey.value) attemptLeave(() => { activeKey.value = key; appliedQuery.value = ''; query.value = ''; void loadTable(); }); }
function searchRecords() { if (!busy.value) { appliedQuery.value = query.value.trim(); void readRecords('', []); } }
function changeScope(value: string) { if (!busy.value) { scope.value = value; void readRecords('', []); } }
function nextPage() { if (hasMore.value && nextCursor.value) void readRecords(nextCursor.value, [...cursorTrail.value, currentCursor.value]); }
function previousPage() { const trail = cursorTrail.value.slice(); const cursor = trail.pop(); if (cursor !== undefined) void readRecords(cursor, trail); }
function resetDetail() {
  detailError.value = ''; notice.value = ''; reason.value = ''; editing.value = false; creating.value = false; fieldErrors.value = {}; conflict.value = false;
  pendingWrite.value = null; pendingUpload.value = null; pickedFile.value = null; replaceId.value = ''; uploadReason.value = '';
  detailTab.value = 'fields'; versions.value = []; versionsError.value = ''; versionsReady.value = false;
}
function applyDetail(record: LibraryRecord) {
  detail.value = record; draft.value = clone(record.fields); baseline.value = clone(record.fields);
  if (!attachmentFields.value.some(field => field.id === uploadField.value)) uploadField.value = attachmentFields.value[0]?.id || '';
}
function openRecord(id: string) {
  attemptLeave(() => {
    detailRead?.abort(); versionRead?.abort(); resetDetail(); detail.value = null; targetId.value = id; detailOpen.value = true;
    const read = detailRead = new AbortController(); detailLoading.value = true;
    void api.record(activeKey.value, id, read.signal).then(record => { if (!read.signal.aborted && !disposed) applyDetail(record); })
      .catch(error => { if (!read.signal.aborted && !disposed) detailError.value = errorText(error); })
      .finally(() => { if (detailRead === read) detailLoading.value = false; });
  });
}
function createRecord() { attemptLeave(() => {
  detailRead?.abort(); versionRead?.abort(); resetDetail(); detail.value = null; targetId.value = ''; draft.value = {}; baseline.value = {};
  const allowed = (activeTable.value?.write_scopes || []).filter(scope => configurationScopes.value.some(option => option.value === scope));
  const field = schema.value?.fields.find(field => field.id === activeTable.value?.scope_field);
  if (field && writableField(field) && allowed.length === 1) draft.value[field.id] = fieldKind(field) === 'multi' ? allowed : allowed[0];
  detailLoading.value = false; creating.value = true; editing.value = true; detailOpen.value = true;
}); }
function closeDetail() { attemptLeave(() => { detailRead?.abort(); versionRead?.abort(); detailOpen.value = false; }); }
function canEditField(field: LibraryField) { return writableField(field); }
function changes(): LibraryFields {
  return Object.fromEntries((schema.value?.fields || []).filter(field => canEditField(field) && JSON.stringify(draft.value[field.id]) !== JSON.stringify(baseline.value[field.id])).map(field => [field.id, draft.value[field.id]]));
}
function validate() {
  const errors: Record<string, string> = {};
  for (const field of schema.value?.fields || []) {
    if (!canEditField(field)) continue;
    const value = fieldValue(draft.value[field.id]);
    if ((field.required || creating.value && field.id === activeTable.value?.scope_field) && (value === undefined || value === null || value === '' || Array.isArray(value) && !value.length)) errors[field.id] = '请填写' + field.name;
    if (fieldKind(field) === 'number' && value != null && value !== '' && !Number.isFinite(Number(value))) errors[field.id] = '请输入有效数字';
    if (fieldKind(field) === 'url' && value) { try { if (!['http:', 'https:'].includes(new URL(typeof value === 'string' ? value : value.link).protocol)) errors[field.id] = '请输入有效的 HTTP 或 HTTPS 链接'; } catch { errors[field.id] = '请输入有效链接'; } }
  }
  fieldErrors.value = errors; return !Object.keys(errors).length;
}
function uncertain(error: unknown) { return !(error instanceof ApiError) || !error.status || error.status >= 500; }
function saved(record: LibraryRecord) {
  applyDetail(record); targetId.value = record.record_id; creating.value = false; editing.value = false; pendingWrite.value = null; pendingUpload.value = null; conflict.value = false;
  const index = records.value.findIndex(item => item.record_id === record.record_id); if (index >= 0) records.value[index] = record;
  notice.value = indexFailed(record) ? '资料保存成功，知识索引待重试。' : '资料保存成功。';
  void readRecords(currentCursor.value, cursorTrail.value);
}
async function saveRecord() {
  if (busy.value || conflict.value || !reason.value.trim() || !validate()) return;
  if (!pendingWrite.value && !Object.keys(changes()).length) { detailError.value = '请先填写或修改资料字段。'; return; }
  if (!pendingWrite.value) pendingWrite.value = { fields: clone(changes()), expected_revision: detail.value?.revision, reason: reason.value.trim(), operation_id: newLibraryOperationId() };
  saving.value = true; detailError.value = ''; notice.value = '';
  try { saved(await api.save(activeKey.value, creating.value ? undefined : detail.value!.record_id, clone(pendingWrite.value))); }
  catch (error) { detailError.value = errorText(error); if (!uncertain(error)) pendingWrite.value = null; if (error instanceof ApiError && error.status === 409) conflict.value = true; }
  finally { saving.value = false; }
}
async function readLatest() {
  if (!detail.value || busy.value || pendingWrite.value || pendingUpload.value) return;
  detailRead?.abort(); const read = detailRead = new AbortController(); detailLoading.value = true; detailError.value = '';
  const unsaved = clone(changes());
  try {
    const record = await api.record(activeKey.value, detail.value.record_id, read.signal);
    if (read.signal.aborted || disposed) return;
    applyDetail(record); if (editing.value) draft.value = { ...draft.value, ...unsaved };
    conflict.value = false;
  } catch (error) { if (!read.signal.aborted && !disposed) detailError.value = errorText(error); }
  finally { if (detailRead === read) detailLoading.value = false; }
}
function pickFile(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  const limit = detail.value?.capabilities.max_upload_bytes;
  if (file && limit && file.size > limit) { detailError.value = '附件超过此表允许的文件大小：' + sizeText(limit); pickedFile.value = null; return; }
  pickedFile.value = file;
}
async function uploadAttachment() {
  if (busy.value || !detail.value || !pickedFile.value || !uploadField.value || !uploadReason.value.trim() || conflict.value) return;
  if (!pendingUpload.value) pendingUpload.value = { file: pickedFile.value, field_id: uploadField.value, expected_revision: detail.value.revision,
    reason: uploadReason.value.trim(), operation_id: newLibraryOperationId(), replace_attachment_id: replaceId.value };
  const intent = pendingUpload.value; const form = new FormData(); form.append('file', intent.file);
  for (const key of ['field_id', 'expected_revision', 'operation_id', 'reason', 'replace_attachment_id'] as const) if (intent[key]) form.append(key, intent[key]);
  uploading.value = true; detailError.value = ''; progress.value = 0; notice.value = '';
  try { saved(await api.upload(activeKey.value, detail.value.record_id, form, percent => progress.value = percent)); pickedFile.value = null; uploadReason.value = ''; versionsReady.value = false; }
  catch (error) { detailError.value = errorText(error); if (!uncertain(error)) pendingUpload.value = null; if (error instanceof ApiError && error.status === 409) conflict.value = true; }
  finally { uploading.value = false; }
}
function selectDetailTab(value: string) { detailTab.value = value; if (value === 'versions' && !versionsReady.value) void loadVersions(); }
async function loadVersions() {
  if (!detail.value) return;
  versionRead?.abort(); const read = versionRead = new AbortController(); versionsLoading.value = true; versionsError.value = '';
  try { const result = await api.versions(activeKey.value, detail.value.record_id, read.signal); if (!read.signal.aborted && !disposed) { versions.value = result; versionsReady.value = true; } }
  catch (error) { if (!read.signal.aborted && !disposed) versionsError.value = errorText(error); }
  finally { if (versionRead === read) versionsLoading.value = false; }
}
function openSettings(isNew: boolean) { attemptLeave(() => { detailOpen.value = false; settingsNew.value = isNew; settingsOpen.value = true; }); }
function closeSettings(verify = false) { settingsOpen.value = false; if (verify) void loadTables(); }
function configurationSaved(table: LibraryTable) { settingsOpen.value = false; notice.value = settingsNew.value ? '数据表已接入，请核对显示字段及编辑范围。' : '数据表配置已保存。'; void loadTables(table.key); }
watch(() => props.scope, value => { if (value !== undefined && value !== scope.value && !busy.value) changeScope(value); });
watch(uploadField, () => replaceId.value = '');
onMounted(() => { unregisterNavigation = registerNavigationGuard((_target, proceed) => { if (busy.value) return false; if (!hasUnsaved()) return true; leaveAction = proceed; discardOpen.value = true; return false; }); window.addEventListener('beforeunload', beforeUnload); void loadTables(); });
onBeforeUnmount(() => { disposed = true; tablesRead?.abort(); listRead?.abort(); detailRead?.abort(); versionRead?.abort(); unregisterNavigation?.(); window.removeEventListener('beforeunload', beforeUnload); });
</script>

<style scoped>
.document-library{width:100%;max-width:1600px;min-width:0;box-sizing:border-box;margin:auto;padding:16px 20px;color:var(--doc-ink);font-size:14px;letter-spacing:0;display:grid;gap:14px}.document-toolbar,.document-heading,.document-actions,.document-pagination,.document-pagination>div,.document-detail-bar,.document-detail-footer{display:flex;align-items:center;gap:8px}.document-toolbar{flex-wrap:wrap;padding-bottom:14px;border-bottom:1px solid var(--doc-line)}.document-heading{min-width:0}.document-heading h1{margin:0;font-size:19px;font-weight:650;white-space:nowrap}.document-heading>svg{color:var(--doc-accent)}.document-actions{margin-left:auto;flex-wrap:wrap}.document-library :deep(.n-button__content),.document-detail :deep(.n-button__content){gap:5px}.document-filters{display:grid;grid-template-columns:minmax(180px,260px) minmax(150px,1fr) minmax(140px,190px) auto;gap:10px;margin:0}.document-filters>.n-input{grid-column:auto}.document-loading{display:grid;gap:10px;padding:8px 0}.document-results{min-width:0}.document-table-wrap{overflow:auto;max-width:100%}.document-table{width:100%;border-collapse:collapse;text-align:left;table-layout:fixed}.document-table th{font-size:12px;color:var(--doc-muted);background:var(--doc-soft);font-weight:500}.document-table th,.document-table td{padding:12px 10px;border-bottom:1px solid var(--doc-line);overflow-wrap:anywhere;vertical-align:middle}.document-table th:last-child{width:44px}.document-table th:nth-last-child(2){width:160px}.document-table td{font-size:13px;line-height:1.6}.document-table tbody tr:hover{background:color-mix(in srgb,var(--doc-accent) 4%,transparent)}.document-record-title{display:flex;align-items:flex-start;gap:8px;text-align:left;font:inherit;font-weight:550;border:0;background:transparent;color:var(--doc-ink);cursor:pointer;padding:3px 0;max-width:100%;min-width:0}.document-record-title>svg{flex:none;margin-top:3px;color:var(--doc-accent)}.document-record-meta{display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-size:12px;color:var(--doc-muted)}.document-record-meta>span{display:flex;align-items:center;gap:4px}.document-pagination{justify-content:space-between;font-size:12px;color:var(--doc-muted)}.document-mobile-list{display:none}.document-detail{display:grid;gap:14px;min-width:0;color:var(--doc-ink);font-size:14px}.document-detail-bar{justify-content:flex-end}.document-detail-bar>.n-tag{margin-right:auto}.document-field-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px 20px}.document-detail-field{display:grid;align-content:start;gap:6px;min-width:0}.document-detail-field>span{display:flex;align-items:center;gap:5px;color:var(--doc-muted);font-size:12px}.document-wide{grid-column:1/-1}.document-required,.document-error{color:var(--doc-error)}.document-error{font-size:12px}.document-attachments{border-top:1px solid var(--doc-line);padding-top:14px;min-width:0}.document-attachments h2{display:flex;align-items:center;gap:7px;font-size:14px;margin:0 0 10px}.document-attachments h2>span{color:var(--doc-muted);font-size:12px;font-weight:400}.document-attachment{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--doc-line);min-width:0}.document-attachment>svg{flex:none;color:var(--doc-muted)}.document-attachment>img{object-fit:cover;border:1px solid var(--doc-line);border-radius:4px;flex:none}.document-attachment>div{min-width:0;flex:1}.document-attachment strong,.document-versions strong{display:block;font-size:13px;font-weight:500;overflow-wrap:anywhere}.document-attachment small,.document-versions small{display:block;color:var(--doc-muted);font-size:12px;margin-top:4px}.document-attachment>a,.document-versions a{display:flex;align-items:center;justify-content:center;color:var(--doc-accent);min-width:36px;min-height:36px}.document-upload{display:grid;gap:12px;margin-top:16px}.document-upload>.n-button{justify-self:end}.document-upload-controls{display:grid;grid-template-columns:1fr 1fr;gap:10px}.document-file-picker{position:relative;display:flex;align-items:center;gap:10px;min-height:54px;padding:10px 12px;box-sizing:border-box;border:1px dashed var(--doc-line);border-radius:6px;overflow-wrap:anywhere;cursor:pointer}.document-file-picker>span{min-width:0}.document-file-picker input{position:absolute;inset:0;opacity:0;width:100%;cursor:pointer}.document-versions ol{list-style:none;margin:0;padding:0}.document-versions li{display:flex;align-items:flex-start;gap:10px;padding:14px 0;border-bottom:1px solid var(--doc-line)}.document-versions li>div{flex:1;min-width:0}.document-versions li>svg{margin-top:3px;flex:none;color:var(--doc-muted)}.document-detail-footer{justify-content:flex-end;flex-wrap:wrap}.document-muted{color:var(--doc-muted);font-size:13px}.document-sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)}.document-library :focus-visible,.document-detail :focus-visible{outline:2px solid var(--doc-accent);outline-offset:3px}.document-record-title,.document-mobile-title{transition:color 160ms ease-out}.document-record-title:hover,.document-mobile-title:hover{color:var(--doc-accent)}@media(max-width:760px){.document-library{padding:12px;gap:12px}.document-toolbar{gap:10px}.document-toolbar>.n-button{order:0}.document-heading h1{font-size:17px}.document-actions{margin-left:auto;gap:6px}.document-filters{grid-template-columns:minmax(0,1fr) auto;gap:8px}.document-filters>.n-base-selection,.document-filters>.n-select{grid-column:1/-1}.document-filters>.n-input{grid-column:1}.document-filters>.n-button{grid-column:2;grid-row:2}.document-table-wrap{display:none}.document-mobile-list{display:grid}.document-mobile-list article{padding:12px 0;border-bottom:1px solid var(--doc-line)}.document-mobile-title{display:flex;align-items:center;gap:8px;width:100%;min-height:44px;padding:0;text-align:left;border:0;background:transparent;font:inherit;color:var(--doc-ink);cursor:pointer}.document-mobile-title>svg{flex:none;color:var(--doc-accent)}.document-mobile-title>strong{min-width:0;flex:1;overflow-wrap:anywhere;font-weight:550}.document-mobile-list dl{display:grid;grid-template-columns:88px minmax(0,1fr);gap:6px 10px;margin:8px 0 12px;font-size:13px}.document-mobile-list dt{color:var(--doc-muted);overflow-wrap:anywhere}.document-mobile-list dd{margin:0;min-width:0}.document-detail .n-button,.document-library :deep(.n-button),.document-detail-footer :deep(.n-button){min-height:44px}.document-field-grid{grid-template-columns:minmax(0,1fr);gap:14px}.document-upload-controls{grid-template-columns:minmax(0,1fr)}.document-attachment>a,.document-versions a{min-width:44px;min-height:44px}.document-actions>.n-button{min-width:40px}.document-pagination{padding-top:4px}}@media(max-width:400px){.document-actions{width:100%;justify-content:flex-end}.document-toolbar{row-gap:8px}}@media(prefers-reduced-motion:reduce){.document-library *,.document-detail *{transition:none!important;animation:none!important}}
</style>

<style scoped>
.document-detail-field :deep(.document-field-value),
.document-detail-field > time {
  color: var(--doc-ink);
  font-size: 14px;
}
.document-detail-field > span:first-child {
  overflow-wrap: anywhere;
}
.document-detail-bar {
  flex-wrap: wrap;
}
.document-detail {
  padding-bottom: env(safe-area-inset-bottom, 0px);
}
.document-index-issues { font-size: 13px; overflow-wrap: anywhere; }
.document-index-issues summary { cursor: pointer; padding: 8px 0; }
.document-index-issues ul { margin: 0; padding-left: 20px; }
.document-index-issues li { padding: 6px 0; }
.document-index-issues strong, .document-index-issues span { display: block; }
.document-index-issues span { color: var(--doc-muted); margin-top: 4px; }
</style>
