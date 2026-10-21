<template>
  <NDrawer :show="show" width="min(680px, 100vw)" :mask-closable="!busy && !uncertain" :close-on-esc="!busy && !uncertain" @update:show="value => { if (!value) close() }">
    <NDrawerContent :title="initial ? '数据表配置' : '接入数据表'" :closable="!busy && !uncertain" :style="styles">
      <form id="document-table-settings" class="document-settings" @submit.prevent="save">
        <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
        <NAlert v-if="uncertain" type="warning" :bordered="false">配置结果尚未确认。请关闭后重新读取配置核验，不要重复接入同一张表。</NAlert>
        <div class="document-settings-grid">
          <label><span>配置标识 *</span><NInput v-model:value="draft.key" :disabled="!!initial || busy || uncertain" :input-props="{ 'aria-label': '配置标识' }" /></label>
          <label><span>显示名称 *</span><NInput v-model:value="draft.name" :disabled="busy || uncertain" :input-props="{ 'aria-label': '数据表显示名称' }" /></label>
          <label class="full"><span>多维表 Base Token *</span><NInput v-model:value="draft.app_token" :disabled="busy || uncertain" :input-props="{ 'aria-label': '多维表 Base Token' }" /></label>
          <label><span>数据表 ID *</span><NInput v-model:value="draft.table_id" :disabled="busy || uncertain" :input-props="{ 'aria-label': '数据表 ID' }" /></label>
          <label><span>飞书数据表链接</span><NInput v-model:value="draft.url" :disabled="busy || uncertain" :input-props="{ 'aria-label': '飞书数据表链接' }" /></label>
          <label><span>楼栋字段</span><NSelect v-model:value="draft.scope_field" :options="fieldOptions" clearable :disabled="busy || uncertain || !schema" aria-label="楼栋字段" /></label>
          <label><span>固定归属楼栋</span><NSelect v-model:value="draft.fixed_scopes" :options="scopes" multiple :disabled="busy || uncertain" aria-label="固定归属楼栋" /></label>
          <label class="full"><span>允许查看的楼栋 *</span><NSelect v-model:value="draft.read_scopes" :options="scopes" multiple :disabled="busy || uncertain" aria-label="允许查看的楼栋" /></label>
          <label class="full"><span>允许编辑的楼栋</span><NSelect v-model:value="draft.write_scopes" :options="scopes.filter(scope => draft.read_scopes?.includes(scope.value))" multiple :disabled="busy || uncertain" aria-label="允许编辑的楼栋" /></label>
          <label class="full"><span>附件字段</span><NSelect v-model:value="draft.attachment_fields" :options="attachmentOptions" multiple :disabled="busy || uncertain || !schema" aria-label="附件字段" /></label>
          <label class="full"><span>列表显示字段</span><NSelect v-model:value="draft.display_fields" :options="fieldOptions" multiple filterable :disabled="busy || uncertain || !schema" aria-label="列表显示字段" /></label>
        </div>
        <ol v-if="draft.display_fields?.length" class="document-field-order" aria-label="显示字段顺序">
          <li v-for="(id, index) in draft.display_fields" :key="id"><span>{{ schema?.fields.find(field => field.id === id)?.name || id }}</span><NButton quaternary :disabled="busy || uncertain || index === 0" :aria-label="'上移' + fieldName(id)" title="上移" @click="move(index, -1)"><ArrowUp :size="15" /></NButton><NButton quaternary :disabled="busy || uncertain || index === draft.display_fields.length - 1" :aria-label="'下移' + fieldName(id)" title="下移" @click="move(index, 1)"><ArrowDown :size="15" /></NButton></li>
        </ol>
        <label class="document-table-enabled"><span>启用此表</span><NSwitch v-model:value="draft.enabled" :disabled="busy || uncertain" aria-label="启用此表" /></label>
      </form>
      <template #footer><div class="document-settings-footer"><NButton :disabled="busy" @click="close">{{ uncertain ? '关闭并核验' : '取消' }}</NButton><NButton type="primary" attr-type="submit" form="document-table-settings" :loading="busy" :disabled="busy || uncertain || !valid">{{ initial ? '保存配置' : '接入数据表' }}</NButton></div></template>
    </NDrawerContent>
  </NDrawer>
  <NModal v-model:show="discard" preset="dialog" title="放弃未保存的表配置？" positive-text="放弃修改" negative-text="继续编辑" @positive-click="emit('close')" />
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { NAlert, NButton, NDrawer, NDrawerContent, NInput, NModal, NSelect, NSwitch, useThemeVars } from 'naive-ui';
import { ArrowDown, ArrowUp } from 'lucide-vue-next';
import { ApiError } from '../api/client';
import { cloneLibraryValue, documentLibraryApi, fieldKind, type LibrarySchema, type LibraryTable } from '../api/documentLibrary';

const props = defineProps<{ show: boolean; initial?: LibraryTable; schema?: LibrarySchema; scopes: Array<{ value: string; label: string }> }>();
const emit = defineEmits<{ close: [verify?: boolean]; saved: [table: LibraryTable] }>();
const theme = useThemeVars();
const styles = computed(() => ({ '--doc-line': theme.value.borderColor, '--doc-muted': theme.value.textColor3 }));
const draft = ref<LibraryTable>({ key: '', name: '' });
const busy = ref(false), uncertain = ref(false), error = ref(''), original = ref(''), discard = ref(false);
const fieldOptions = computed(() => (props.schema?.fields || []).map(field => ({ value: field.id, label: field.name })));
const attachmentOptions = computed(() => (props.schema?.fields || []).filter(field => fieldKind(field) === 'files').map(field => ({ value: field.id, label: field.name })));
const valid = computed(() => /^[A-Za-z0-9_-]{1,64}$/.test(draft.value.key) && !!draft.value.name.trim()
  && /^[A-Za-z0-9]{8,200}$/.test(draft.value.app_token || '') && /^tbl[A-Za-z0-9]+$/.test(draft.value.table_id || '')
  && (!!draft.value.scope_field || !!draft.value.fixed_scopes?.length) && !!draft.value.read_scopes?.length
  && (draft.value.write_scopes || []).every(scope => draft.value.read_scopes?.includes(scope)) && (!props.initial || !!draft.value.revision));
watch(() => props.show, show => {
  if (!show) return;
  draft.value = cloneLibraryValue(props.initial || { key: '', name: '', app_token: '', table_id: '', url: '',
    enabled: true, display_fields: [], attachment_fields: [], scope_field: '', fixed_scopes: props.scopes.slice(0, 1).map(scope => scope.value),
    read_scopes: props.scopes.map(scope => scope.value), write_scopes: [] });
  original.value = JSON.stringify(draft.value); error.value = ''; uncertain.value = false;
});
function fieldName(id: string) { return props.schema?.fields.find(field => field.id === id)?.name || id; }
function move(index: number, direction: number) {
  const fields = draft.value.display_fields!;
  [fields[index], fields[index + direction]] = [fields[index + direction], fields[index]];
}
function close() {
  if (busy.value) return;
  if (!uncertain.value && JSON.stringify(draft.value) !== original.value) discard.value = true;
  else emit('close', uncertain.value);
}
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
.document-settings{display:grid;gap:16px;font-size:14px}.document-settings-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.document-settings label{display:grid;gap:6px;min-width:0}.document-settings label>span{font-size:13px;color:var(--doc-muted)}.document-settings-grid .full{grid-column:1/-1}.document-field-order{margin:0;padding:0 0 0 24px}.document-field-order li{display:flex;gap:6px;align-items:center;border-bottom:1px solid var(--doc-line);padding:5px 0}.document-field-order span{flex:1;min-width:0;overflow-wrap:anywhere}.document-table-enabled{display:flex!important;align-items:center;justify-content:space-between;border-top:1px solid var(--doc-line);padding-top:12px}.document-settings-footer{display:flex;gap:10px;justify-content:flex-end}.document-settings :deep(.n-button){min-height:36px}@media(max-width:560px){.document-settings-grid{grid-template-columns:minmax(0,1fr)}.document-settings :deep(.n-button),.document-settings-footer :deep(.n-button){min-height:44px}}
</style>
