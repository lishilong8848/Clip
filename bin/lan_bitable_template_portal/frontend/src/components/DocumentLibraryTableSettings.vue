<template>
  <NDrawer :show="show" width="min(680px, 100vw)" :mask-closable="!busy && !uncertain" :close-on-esc="!busy && !uncertain" @update:show="value => { if (!value) close() }">
    <NDrawerContent :title="initial ? '数据表配置' : '接入数据表'" :closable="!busy && !uncertain" :style="styles">
      <form id="document-table-settings" class="document-settings" data-v2-inline-feedback @submit.prevent="save">
        <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
        <NAlert v-if="uncertain" type="warning" :bordered="false">配置结果尚未确认。请关闭后重新读取配置核验，不要重复接入同一张表。</NAlert>
        <div class="document-source-row">
          <label><span>飞书 Base 或 Wiki 链接 *</span><NInput v-model:value="link" :disabled="busy || uncertain" placeholder="粘贴飞书多维表或知识库链接" :input-props="{ 'aria-label': '飞书数据表链接' }" @keydown.enter.prevent="readSource" /></label>
          <NButton :loading="reading" :disabled="busy || uncertain || reading || !link.trim()" @click="readSource"><Search :size="16" />读取数据表</NButton>
        </div>
        <label v-if="tableOptions.length"><span>选择数据表 *</span><NSelect v-model:value="selectedTable" :options="tableOptions" :disabled="busy || uncertain" aria-label="选择数据表" @update:value="selectTable" /></label>
        <p v-if="sourceReady && fields" class="document-source-status" role="status">已读取 {{ fields.length }} 个字段</p>
        <div v-if="sourceReady" class="document-settings-grid">
          <label><span>显示名称 *</span><NInput v-model:value="draft.name" :disabled="busy || uncertain" :input-props="{ 'aria-label': '数据表显示名称' }" /></label>
          <label><span>楼栋归属方式 *</span><NRadioGroup v-model:value="ownership" :disabled="busy || uncertain" aria-label="楼栋归属方式"><NRadioButton value="field">按字段</NRadioButton><NRadioButton value="fixed">固定楼栋</NRadioButton></NRadioGroup></label>
          <label v-if="ownership === 'field'" class="full"><span>楼栋字段 *</span><NSelect v-model:value="draft.scope_field" :options="fieldOptions" clearable :disabled="busy || uncertain || !fields" aria-label="楼栋字段" /><small v-if="draft.fixed_scopes?.length" class="document-source-status">记录归属同时限定：{{ draft.fixed_scopes.map(value => scopes.find(scope => scope.value === value)?.label || value).join('、') }}</small></label>
          <label v-else class="full"><span>固定归属楼栋 *</span><NSelect v-model:value="draft.fixed_scopes" :options="scopes" multiple :disabled="busy || uncertain" aria-label="固定归属楼栋" /></label>
          <label class="full"><span>允许查看的楼栋 *</span><NSelect v-model:value="draft.read_scopes" :options="scopes" multiple :disabled="busy || uncertain" aria-label="允许查看的楼栋" /></label>
          <label class="full"><span>允许编辑的楼栋</span><NSelect v-model:value="draft.write_scopes" :options="scopes.filter(scope => draft.read_scopes?.includes(scope.value))" multiple :disabled="busy || uncertain" aria-label="允许编辑的楼栋" /></label>
          <label class="full"><span>附件字段</span><NSelect v-model:value="draft.attachment_fields" :options="attachmentOptions" multiple :disabled="busy || uncertain || !fields" aria-label="附件字段" /></label>
          <label class="full"><span>列表显示字段</span><NSelect v-model:value="draft.display_fields" :options="fieldOptions" multiple filterable :disabled="busy || uncertain || !fields" aria-label="列表显示字段" /></label>
        </div>
        <ol v-if="sourceReady && draft.display_fields?.length" class="document-field-order" aria-label="显示字段顺序">
          <li v-for="(id, index) in draft.display_fields" :key="id"><span>{{ fieldName(id) }}</span><NButton quaternary :disabled="busy || uncertain || index === 0" :aria-label="'上移' + fieldName(id)" title="上移" @click="move(index, -1)"><ArrowUp :size="15" /></NButton><NButton quaternary :disabled="busy || uncertain || index === draft.display_fields.length - 1" :aria-label="'下移' + fieldName(id)" title="下移" @click="move(index, 1)"><ArrowDown :size="15" /></NButton></li>
        </ol>
        <label v-if="sourceReady" class="document-table-enabled"><span>启用此表</span><NSwitch v-model:value="draft.enabled" :disabled="busy || uncertain" aria-label="启用此表" /></label>
      </form>
      <template #footer><div class="document-settings-footer"><NButton :disabled="busy" @click="close">{{ uncertain ? '关闭并核验' : '取消' }}</NButton><NButton type="primary" attr-type="submit" form="document-table-settings" :loading="busy" :disabled="busy || uncertain || !valid">{{ initial ? '保存配置' : '接入数据表' }}</NButton></div></template>
    </NDrawerContent>
  </NDrawer>
  <NModal v-model:show="discard" preset="dialog" title="放弃未保存的表配置？" positive-text="放弃修改" negative-text="继续编辑" @positive-click="finishClose()" />
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { NAlert, NButton, NDrawer, NDrawerContent, NInput, NModal, NRadioButton, NRadioGroup, NSelect, NSwitch, useThemeVars } from 'naive-ui';
import { ArrowDown, ArrowUp, Search } from 'lucide-vue-next';
import { ApiError } from '../api/client';
import { cloneLibraryValue, documentLibraryApi, fieldKind, type LibraryField, type LibrarySchema, type LibraryTable } from '../api/documentLibrary';

const props = defineProps<{ show: boolean; initial?: LibraryTable; schema?: LibrarySchema; scopes: Array<{ value: string; label: string }> }>();
const emit = defineEmits<{ close: [verify?: boolean]; saved: [table: LibraryTable] }>();
const theme = useThemeVars();
const styles = computed(() => ({ '--doc-line': theme.value.borderColor, '--doc-muted': theme.value.textColor3 }));
const draft = ref<LibraryTable>({ key: '', name: '' });
const busy = ref(false), uncertain = ref(false), error = ref(''), original = ref(''), discard = ref(false);
const link = ref(''), reading = ref(false), sourceReady = ref(false), previewFields = ref<LibraryField[] | null>(null);
const selectedTable = ref<string | null>(null), tableOptions = ref<Array<{ value: string; label: string }>>([]);
const ownership = ref<'field' | 'fixed'>('field');
let readController: AbortController | undefined, readSequence = 0, resetting = false;
const fields = computed(() => sourceReady.value ? previewFields.value || props.schema?.fields : undefined);
const fieldOptions = computed(() => (fields.value || []).map(field => ({ value: field.id, label: field.name })));
const attachmentOptions = computed(() => (fields.value || []).filter(field => fieldKind(field) === 'files').map(field => ({ value: field.id, label: field.name })));
const valid = computed(() => sourceReady.value && !reading.value && /^[A-Za-z0-9_-]{1,64}$/.test(draft.value.key) && !!draft.value.name.trim()
  && /^[A-Za-z0-9]{8,200}$/.test(draft.value.app_token || '') && /^tbl[A-Za-z0-9]+$/.test(draft.value.table_id || '')
  && (ownership.value === 'field' ? !!draft.value.scope_field : !!draft.value.fixed_scopes?.length) && !!draft.value.read_scopes?.length
  && (!fields.value || (draft.value.display_fields || []).every(id => fields.value!.some(field => field.id === id))
    && (draft.value.attachment_fields || []).every(id => attachmentOptions.value.some(field => field.value === id))
    && (ownership.value !== 'field' || fields.value.some(field => field.id === draft.value.scope_field)))
  && (draft.value.write_scopes || []).every(scope => draft.value.read_scopes?.includes(scope)) && (!props.initial || !!draft.value.revision));
function cancelRead() { readSequence++; readController?.abort(); readController = undefined; reading.value = false; }
watch(() => [props.show, props.initial?.key] as const, ([show]) => {
  cancelRead();
  if (!show) return;
  resetting = true;
  draft.value = cloneLibraryValue(props.initial || { key: '', name: '', app_token: '', table_id: '', url: '',
    enabled: true, display_fields: [], attachment_fields: [], scope_field: '', fixed_scopes: [],
    read_scopes: props.scopes.map(scope => scope.value), write_scopes: [] });
  link.value = draft.value.url || ''; ownership.value = draft.value.scope_field ? 'field' : props.initial ? 'fixed' : 'field';
  sourceReady.value = !!props.initial; previewFields.value = null; selectedTable.value = null; tableOptions.value = [];
  original.value = JSON.stringify(draft.value); error.value = ''; uncertain.value = false; discard.value = false;
  resetting = false;
}, { immediate: true });
watch(link, () => {
  if (resetting) return;
  cancelRead(); sourceReady.value = false; previewFields.value = null; tableOptions.value = []; selectedTable.value = null; error.value = '';
}, { flush: 'sync' });
watch(ownership, mode => {
  if (resetting) return;
  if (mode === 'field') draft.value.fixed_scopes = [];
  else draft.value.scope_field = '';
}, { flush: 'sync' });
function selectTable(tableId: string | null) {
  if (busy.value || uncertain.value || !tableId || !tableOptions.value.some(option => option.value === tableId)) return;
  try {
    const url = new URL(link.value.trim());
    url.searchParams.set('table', tableId);
    // The table picker deliberately changes the source; do not clear itself via the URL watcher.
    resetting = true; link.value = url.toString(); resetting = false;
    selectedTable.value = tableId;
    void readSource();
  } catch {
    cancelRead(); sourceReady.value = false; error.value = '请输入有效的飞书 Base 或 Wiki 链接。';
  }
}
async function readSource() {
  if (busy.value || uncertain.value || !link.value.trim()) return;
  cancelRead(); const sequence = readSequence, controller = new AbortController(); readController = controller;
  reading.value = true; sourceReady.value = false; error.value = '';
  try {
    const result = await documentLibraryApi.preview(link.value.trim(), selectedTable.value || undefined, controller.signal);
    if (sequence !== readSequence || !props.show) return;
    tableOptions.value = result.tables.map(table => ({ value: table.table_id, label: table.name }));
    if (!result.source) { selectedTable.value = null; return; }
    const source = result.source, sameSource = source.app_token === draft.value.app_token && source.table_id === draft.value.table_id;
    if (!sameSource) {
      const displayFields = result.fields.filter(field => fieldKind(field) !== 'files').slice(0, 4).map(field => field.id);
      draft.value = { ...draft.value, key: props.initial?.key || source.key, name: props.initial ? draft.value.name : source.name,
        app_token: source.app_token, table_id: source.table_id, scope_field: '', fixed_scopes: [],
        read_scopes: props.scopes.map(scope => scope.value), write_scopes: [],
        display_fields: displayFields.length ? displayFields : [result.fields[0].id], attachment_fields: [] };
      ownership.value = 'field';
    }
    const ids = new Set(result.fields.map(field => field.id));
    draft.value.display_fields = (draft.value.display_fields || []).filter(id => ids.has(id));
    draft.value.attachment_fields = (draft.value.attachment_fields || []).filter(id => result.fields.some(field => field.id === id && fieldKind(field) === 'files'));
    if (draft.value.scope_field && !ids.has(draft.value.scope_field)) draft.value.scope_field = '';
    resetting = true; link.value = result.url; resetting = false;
    draft.value.url = result.url; selectedTable.value = source.table_id!; previewFields.value = result.fields; sourceReady.value = true;
  } catch (exception) {
    if (sequence === readSequence && !controller.signal.aborted && props.show) error.value = exception instanceof Error ? exception.message : '数据表读取失败，请重试。';
  } finally { if (sequence === readSequence) reading.value = false; }
}
onBeforeUnmount(cancelRead);
function fieldName(id: string) { return fields.value?.find(field => field.id === id)?.name || id; }
function move(index: number, direction: number) {
  const fields = draft.value.display_fields!;
  [fields[index], fields[index + direction]] = [fields[index + direction], fields[index]];
}
function close() {
  if (busy.value) return;
  if (!uncertain.value && (JSON.stringify(draft.value) !== original.value || link.value !== (draft.value.url || ''))) discard.value = true;
  else finishClose(uncertain.value);
}
function finishClose(verify = false) { cancelRead(); emit('close', verify); }
async function save() {
  if (busy.value || uncertain.value || !valid.value) return;
  busy.value = true; error.value = '';
  try {
    const result = await documentLibraryApi.configure(cloneLibraryValue(draft.value), !!props.initial);
    emit('saved', result);
  } catch (exception) {
    error.value = exception instanceof Error ? exception.message : '表配置未保存。';
    uncertain.value = !(exception instanceof ApiError) || !exception.status || exception.status >= 500;
  } finally { busy.value = false; }
}
</script>

<style scoped>
.document-source-row{display:flex;align-items:flex-end;gap:10px}.document-source-row>label{flex:1;min-width:0}.document-source-row>.n-button{flex:none}.document-source-status{margin:0;color:var(--doc-muted);font-size:13px}
.document-settings{display:grid;gap:16px;font-size:14px}.document-settings-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.document-settings-grid>label,.document-settings>label,.document-source-row>label{display:grid;gap:6px;min-width:0}.document-settings-grid>label>span,.document-settings>label>span,.document-source-row>label>span{font-size:13px;color:var(--doc-muted)}.document-settings-grid .full{grid-column:1/-1}.document-field-order{margin:0;padding:0 0 0 24px}.document-field-order li{display:flex;gap:6px;align-items:center;border-bottom:1px solid var(--doc-line);padding:5px 0}.document-field-order span{flex:1;min-width:0;overflow-wrap:anywhere}.document-table-enabled{display:flex!important;align-items:center;justify-content:space-between;border-top:1px solid var(--doc-line);padding-top:12px}.document-settings-footer{display:flex;gap:10px;justify-content:flex-end}.document-settings :deep(.n-button){min-height:36px}@media(max-width:560px){.document-settings-grid{grid-template-columns:minmax(0,1fr)}.document-settings :deep(.n-button),.document-settings-footer :deep(.n-button){min-height:44px}}
</style>
