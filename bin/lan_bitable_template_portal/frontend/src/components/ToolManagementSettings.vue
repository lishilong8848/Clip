<template>
  <section class="tool-settings" aria-label="工具配置">
    <div class="settings-line settings-heading">
      <div><h3>工具配置</h3><span v-if="data">{{ data.items.length }} 条授权工具记录</span></div>
      <NButton size="small" :loading="loading" :disabled="saving" @click="load">刷新配置</NButton>
    </div>
    <NAlert v-if="error" type="error" :bordered="false">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false">{{ message }}</NAlert>
    <div class="settings-picker">
      <label>工具记录 <NSelect v-model:value="selection" :options="assetOptions" filterable placeholder="搜索楼栋、工具名称或型号" :disabled="saving" /></label>
    </div>
    <template v-if="asset && form">
      <div class="settings-line settings-info"><strong>{{ asset.scope }}楼 · {{ asset.name }}</strong><span>{{ asset.location }}</span><span>{{ asset.quantity }} 件</span></div>
      <div class="settings-line settings-info">
        <span>维护周期 {{ asset.source_self_cycle || '—' }}</span><span>送检周期 {{ asset.source_external_cycle || '—' }}</span>
        <small>周期从飞书工具底表读取。</small>
      </div>
      <div class="settings-line" v-if="asset.source_external_cycle && asset.source_external_cycle !== '停用'">
        <label>送检专业主管 <NSelect v-model:value="form.supervisor" :options="supervisorOptions" filterable clearable :disabled="saving" placeholder="选择对应专业主管" /></label>
        <NButton type="primary" :loading="saving" @click="saveTool">保存专业主管</NButton>
      </div>
    </template>
    <p v-if="data?.scrap_approvers?.length" class="settings-line settings-approval">
      报废审批人 {{ data.scrap_approvers.join('、') }}（均通过后生效）
    </p>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NSelect } from 'naive-ui';
import { requestJson } from '../api/client';

interface Asset {
  id: string; name: string; model: string; location: string; scope: string; quantity: number;
  revision: number; supervisor?: string;
  source_self_cycle: string; source_external_cycle: string;
}
interface Inventory { items: Asset[]; scrap_approvers: string[] }
interface Person { id: string; name: string; category: string; position?: string; final_scopes: string[] }
const data = ref<Inventory>();
const people = ref<Person[]>([]);
const selection = ref<string | null>(null);
const form = ref<{ supervisor: string | null }>();
const loading = ref(false);
const saving = ref(false);
const message = ref('');
const error = ref('');
let read: AbortController | undefined;
const asset = computed(() => data.value?.items.find(row => row.id === selection.value));
const assetOptions = computed(() => (data.value?.items || []).map(row => ({ value: row.id,
  label: [row.scope + '楼', row.name, row.model, row.location].filter(Boolean).join(' · ') })));
const supervisorOptions = computed(() => people.value.filter(person => asset.value && person.final_scopes.includes(asset.value.scope) &&
  (person.category === 'supervisor' || person.position?.includes('主管'))).map(person => ({value: person.id, label: person.name})));
const showError = (value: unknown) => { error.value = value instanceof Error ? value.message : '配置操作未完成，请重试。'; };
watch(asset, value => {
  form.value = value ? { supervisor: value.supervisor || null } : undefined;
}, { immediate: true });

async function load() {
  read?.abort();
  const controller = read = new AbortController();
  const options = { signal: controller.signal, cache: 'no-store' as const };
  loading.value = true;
  error.value = '';
  try {
    const [inventory, directory] = await Promise.all([
      requestJson('/api/tool-self-check/inventory', options),
      requestJson('/api/risk-inspections/people', options),
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
    await load();
  } catch (value) { showError(value); }
  finally { saving.value = false; }
}
function saveTool() {
  if (asset.value && form.value) void save(asset.value.id, {
    supervisor: form.value.supervisor || '', revision: asset.value.revision });
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
