<template>
  <section class="tool-settings" aria-label="工具排期配置">
    <div class="settings-line settings-heading">
      <div><h3>工具排期</h3><span v-if="data">{{ data.items.length }} 条授权工具记录</span></div>
      <NButton size="small" :loading="loading" :disabled="saving" @click="load">刷新配置</NButton>
    </div>
    <NAlert v-if="error" type="error" :bordered="false">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false">{{ message }}</NAlert>
    <div class="settings-picker">
      <label>工具记录 <NSelect v-model:value="selection" :options="assetOptions" filterable placeholder="搜索楼栋、工具名称或型号" :disabled="saving" /></label>
    </div>
    <template v-if="asset && form">
      <div class="settings-line settings-info"><strong>{{ asset.scope }}楼 · {{ asset.name }}</strong><span>{{ asset.location }}</span><span>{{ asset.quantity }} 件</span></div>
      <div class="settings-cycles">
        <fieldset>
          <legend>自检维护</legend>
          <label>维护周期 <NSelect v-model:value="form.self_cycle" :options="cycles(asset.source_self_cycle)" :disabled="saving" /></label>
          <label>首次维护日期 <input v-model="form.self_first" type="date" :disabled="saving" /></label>
        </fieldset>
        <fieldset>
          <legend>委外送检</legend>
          <label>送检周期 <NSelect v-model:value="form.external_cycle" :options="cycles(asset.source_external_cycle)" :disabled="saving" /></label>
          <label>首次送检日期 <input v-model="form.external_first" type="date" :disabled="saving" /></label>
          <label class="settings-supervisor">送检专业主管 <NSelect v-model:value="form.supervisor" :options="supervisorOptions" filterable clearable :disabled="saving" placeholder="选择对应专业主管" /></label>
        </fieldset>
      </div>
      <div class="settings-line">
        <NButton type="primary" :loading="saving" @click="saveTool">保存工具排期</NButton>
        <small>优先按上次检查日期推算。没有历史日期时，按首次日期或本月起排。</small>
      </div>
    </template>
    <div v-if="data?.can_set_manager" class="settings-line settings-approval">
      <label>园区报废审批人 <NSelect v-model:value="managerId" :options="managerOptions" filterable placeholder="选择园区经理" :disabled="saving" /></label>
      <NButton :disabled="saving || !managerId || managerId === data.manager_id" @click="saveManager">保存审批人</NButton>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NSelect } from 'naive-ui';
import { requestJson } from '../api/client';

interface Asset {
  id: string; name: string; model: string; location: string; scope: string; quantity: number;
  revision: number; self_cycle: string; external_cycle: string; self_first?: string; external_first?: string; supervisor?: string;
  source_self_cycle: string; source_external_cycle: string; override_self_cycle: string; override_external_cycle: string;
}
interface Inventory { items: Asset[]; manager_id: string; can_set_manager: boolean }
interface Person { id: string; name: string; category: string; position?: string; final_scopes: string[] }
const data = ref<Inventory>();
const people = ref<Person[]>([]);
const selection = ref<string | null>(null);
const managerId = ref<string | null>(null);
const form = ref<{ self_cycle: string; external_cycle: string; self_first: string; external_first: string; supervisor: string | null }>();
const loading = ref(false);
const saving = ref(false);
const message = ref('');
const error = ref('');
let read: AbortController | undefined;
const asset = computed(() => data.value?.items.find(row => row.id === selection.value));
const assetOptions = computed(() => (data.value?.items || []).map(row => ({ value: row.id,
  label: [row.scope + '楼', row.name, row.model, row.location].filter(Boolean).join(' · ') })));
const managerOptions = computed(() => people.value.filter(person => person.category === 'manager' &&
  ['A', 'B', 'C', 'D', 'E', 'H'].every(scope => person.final_scopes.includes(scope))).map(person => ({value: person.id, label: person.name})));
const supervisorOptions = computed(() => people.value.filter(person => asset.value && person.final_scopes.includes(asset.value.scope) &&
  (person.category === 'supervisor' || person.position?.includes('主管'))).map(person => ({value: person.id, label: person.name})));
const cycles = (source: string) => [{ value: '', label: source ? `随底表（${source}）` : '随底表（未配置）' },
  ...['停用','每月','季度','半年','一年','两年','三年','四年','五年'].map(value => ({value, label:value}))];
const showError = (value: unknown) => { error.value = value instanceof Error ? value.message : '配置操作未完成，请重试。'; };
watch(asset, value => {
  form.value = value ? { self_cycle: value.override_self_cycle, external_cycle: value.override_external_cycle,
    self_first: value.self_first || '', external_first: value.external_first || '', supervisor: value.supervisor || null } : undefined;
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
    managerId.value = data.value.manager_id || null;
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
function saveManager() { if (managerId.value) void save('park-manager', { manager_id: managerId.value }); }
function saveTool() {
  if (asset.value && form.value) void save(asset.value.id, { ...form.value,
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
.settings-picker label,.settings-line label,.settings-cycles label { display:grid; min-width:0; gap:4px; font-size:13px; }
.settings-cycles { display:grid; gap:8px; grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr)); }
.settings-cycles fieldset {
  margin:0; padding:8px; min-width:0; border:1px solid var(--tool-line); border-radius:8px;
  display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px;
}
.settings-cycles legend { font-weight:650; padding:0 5px; }
.settings-cycles input { box-sizing:border-box; padding:7px 9px; border:1px solid var(--tool-line); border-radius:6px; color:inherit; background:var(--tool-surface); font:inherit; min-width:0; max-width:100%; }
.settings-supervisor { grid-column:1/-1; }
.settings-info { font-size:13px; overflow-wrap:anywhere; }
.settings-approval { padding-top:10px; border-top:1px solid var(--tool-line); }
</style>
