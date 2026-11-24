<template>
  <section class="tool-settings" aria-label="工具配置">
    <div class="settings-line settings-heading">
      <div><span v-if="data">{{ scopedAssets.length }} 条工具记录</span></div>
      <NButton size="small" :loading="loading" :disabled="saving" @click="load(true)">刷新配置</NButton>
    </div>
    <NAlert v-if="error" type="error" :bordered="false">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false">{{ message }}</NAlert>
    <div class="settings-picker">
      <label>工具记录 <NSelect v-model:value="selection" :options="assetOptions" filterable placeholder="搜索楼栋、工具名称或型号" :disabled="saving" /></label>
    </div>
    <template v-if="asset && form">
      <div class="settings-line settings-info"><strong>{{ asset.scope }}楼 · {{ asset.name }}</strong><span>{{ asset.profession || '专业未填写' }}</span><span>{{ asset.location }}</span><span>{{ asset.quantity }} 件</span></div>
      <div class="settings-line settings-info">
        <span>维护周期 {{ asset.source_self_cycle || '—' }}</span><span>送检周期 {{ asset.source_external_cycle || '—' }}</span>
        <small>周期从飞书工具底表读取。</small>
      </div>
      <div class="settings-line">
        <label>工具负责人 <NSelect v-model:value="form.responsible_id" aria-label="工具负责人" :options="responsibleOptions" filterable clearable :disabled="saving || loading" placeholder="选择有本楼权限的人员" /></label>
        <label v-if="asset.source_external_cycle && asset.source_external_cycle !== '停用'">送检专业主管 <NSelect v-model:value="form.supervisor" :options="supervisorOptions" filterable clearable :disabled="saving || loading" placeholder="选择对应专业主管" /></label>
        <NButton type="primary" :loading="saving" :disabled="loading || !dirty" @click="saveTool">保存配置</NButton>
      </div>
    </template>
    <p v-if="data?.scrap_approvers?.length" class="settings-line settings-approval">
      报废审批人 {{ data.scrap_approvers.join('、') }}（任一位同意后生效）
    </p>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NSelect } from 'naive-ui';
import { requestJson, requestPeople } from '../api/client';

const props = defineProps<{ scope?: string }>();
const emit = defineEmits<{ changed: [] }>();
interface Asset {
  id: string; name: string; model: string; location: string; scope: string; quantity: number;
  revision: number; supervisor?: string; profession?: string; responsible_id?: string; responsible_name?: string;
  source_self_cycle: string; source_external_cycle: string;
}
interface Inventory { items: Asset[]; scrap_approvers: string[] }
interface Person { id: string; name: string; category: string; position?: string; final_scopes: string[] }
const data = ref<Inventory>();
const people = ref<Person[]>([]);
const selection = ref<string | null>(null);
const form = ref<{ supervisor: string | null; responsible_id: string | null }>();
const loading = ref(false);
const saving = ref(false);
const message = ref('');
const error = ref('');
let read: AbortController | undefined;
const scopedAssets = computed(() => (data.value?.items || []).filter(row =>
  !props.scope || ['ALL', 'CAMPUS'].includes(props.scope) || row.scope === props.scope));
const asset = computed(() => scopedAssets.value.find(row => row.id === selection.value));
const assetOptions = computed(() => scopedAssets.value.map(row => ({ value: row.id,
  label: [row.scope + '楼', row.name, row.profession, row.model, row.location].filter(Boolean).join(' · ') })));
const eligiblePeople = computed(() => people.value.filter(person => asset.value && person.final_scopes.includes(asset.value.scope)));
const supervisorOptions = computed(() => eligiblePeople.value.filter(person =>
  person.category === 'supervisor' || person.position?.includes('主管')).map(person => ({value: person.id, label: person.name})));
const responsibleOptions = computed(() => {
  const options = eligiblePeople.value.map(person => ({ value: person.id, label: person.name }));
  const current = asset.value;
  if (current?.responsible_id && !options.some(option => option.value === current.responsible_id))
    return [...options, { value: current.responsible_id, label: (current.responsible_name || '原负责人') + '（待核对）', disabled: true }];
  return options;
});
const changes = computed(() => {
  const fields: Record<string, string> = {};
  if (!asset.value || !form.value) return fields;
  for (const key of ['responsible_id', 'supervisor'] as const) {
    if ((form.value[key] || '') !== (asset.value[key] || '')) fields[key] = form.value[key] || '';
  }
  return fields;
});
const dirty = computed(() => Object.keys(changes.value).length > 0);
const showError = (value: unknown) => { error.value = value instanceof Error ? value.message : '配置操作未完成，请重试。'; };
watch(asset, value => {
  form.value = value ? { supervisor: value.supervisor || null, responsible_id: value.responsible_id || null } : undefined;
}, { immediate: true });

async function load(force = false) {
  read?.abort();
  const controller = read = new AbortController();
  const options = { signal: controller.signal, cache: 'no-store' as const };
  loading.value = true;
  error.value = '';
  try {
    const [inventory, directory] = await Promise.all([
      requestJson('/api/tool-self-check/inventory', options),
      requestPeople('/api/tool-self-check/inventory/people', { ...options, fresh: force }),
    ]);
    if (controller.signal.aborted) return;
    data.value = inventory as Inventory;
    people.value = directory.items as Person[];
  } catch (value) { if (!controller.signal.aborted) showError(value); }
  finally { if (read === controller) loading.value = false; }
}
async function save(id: string, body: object) {
  saving.value = true;
  error.value = message.value = '';
  try {
    await requestJson(`/api/tool-self-check/inventory/${encodeURIComponent(id)}`, { method: 'POST', body: JSON.stringify(body) });
    message.value = '配置已保存。';
    emit('changed');
    await load();
  } catch (value) { showError(value); }
  finally { saving.value = false; }
}
function saveTool() {
  if (asset.value && dirty.value && !saving.value && !loading.value) void save(asset.value.id, {
    ...changes.value, revision: asset.value.revision });
}
onMounted(load);
onBeforeUnmount(() => read?.abort());
</script>

<style scoped>
.tool-settings { display:grid; gap:12px; min-width:0; padding:12px 14px; border:1px solid var(--tool-line); border-radius:10px; background:var(--tool-surface); }
.settings-line { display:flex; flex-wrap:wrap; align-items:end; gap:8px 12px; min-width:0; }
.settings-heading { justify-content:space-between; align-items:center; }
.settings-heading h3 { font-size:15px; margin:0; }
.settings-heading span,.settings-line small { color:var(--tool-muted); font-size:12px; }
.settings-line>label { flex:1 1 200px; max-width:420px; }
.settings-picker label,.settings-line label { display:grid; min-width:0; gap:4px; font-size:13px; }
.settings-info { font-size:13px; overflow-wrap:anywhere; }
.settings-approval { padding-top:10px; border-top:1px solid var(--tool-line); }
</style>
