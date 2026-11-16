<template>
  <fieldset v-if="editable" class="report-check" :disabled="disabled">
    <legend>核对报告</legend>
    <div class="report-fields">
      <label>报告编号 <input v-model.trim="model.report_no" maxlength="120" /></label>
      <label>检测机构 <input v-model.trim="model.institution" maxlength="160" /></label>
      <label>检测结论
        <select v-model="model.result" data-native-select aria-label="检测结论" @change="model.expires_on = ''">
          <option value="">请选择</option><option value="qualified">合格</option><option value="unqualified">不合格</option>
        </select>
      </label>
      <label v-if="model.result === 'qualified'">有效至（选填）
        <input v-model="model.expires_on" type="date" :min="actual || undefined" />
        <small>留空按送检周期计算</small>
      </label>
    </div>
    <details v-if="assets && assets.length > 1" class="report-overrides">
      <summary>逐台检测结论（{{ Object.keys(model.tools || {}).length }} 台单独设置）</summary>
      <div v-for="asset in assets" :key="asset.id" class="report-fields report-tool">
        <strong>{{ asset.name }}<small v-if="asset.model"> · {{ asset.model }}</small></strong>
        <label>检测结论
          <select data-native-select :aria-label="asset.name + '检测结论'" :value="model.tools?.[asset.id]?.result || ''"
            @change="changeToolResult(asset.id, ($event.target as HTMLSelectElement).value)">
            <option value="">沿用本批结论</option><option value="qualified">合格</option><option value="unqualified">不合格</option>
          </select>
        </label>
        <label v-if="model.tools?.[asset.id]?.result === 'qualified'">有效至（选填）
          <input type="date" :min="actual || undefined" :aria-label="asset.name + '有效至'"
            :value="model.tools[asset.id].expires_on" @input="model.tools[asset.id].expires_on = ($event.target as HTMLInputElement).value" />
          <small>留空按本工具送检周期计算</small>
        </label>
      </div>
    </details>
    <label class="report-confirm"><input type="checkbox" :checked="confirmed" :disabled="!assetIds.length || disabled"
      @change="model.confirmed_asset_ids = ($event.target as HTMLInputElement).checked ? [...assetIds] : []" />
      已核对报告与本次提交的 {{ assetIds.length }} 台工具一致
    </label>
  </fieldset>
  <dl v-else-if="model.result" class="report-fields report-readonly">
    <div><dt>报告编号</dt><dd>{{ model.report_no }}</dd></div>
    <div><dt>检测机构</dt><dd>{{ model.institution }}</dd></div>
    <template v-if="assets?.length && model.tools">
      <div v-for="asset in assets" :key="asset.id"><dt>{{ asset.name }}</dt>
        <dd>{{ model.tools[asset.id]?.result === 'qualified' ? '合格' : model.tools[asset.id]?.result === 'unqualified' ? '不合格' : '待核对' }}
          <span v-if="model.tools[asset.id]?.expires_on"> · 有效至 {{ model.tools[asset.id].expires_on }}</span></dd>
      </div>
    </template>
    <template v-else>
      <div><dt>检测结论</dt><dd>{{ model.result === 'qualified' ? '合格' : '不合格' }}</dd></div>
      <div v-if="expiresOn || model.expires_on"><dt>有效至</dt><dd>{{ expiresOn || model.expires_on }}</dd></div>
    </template>
  </dl>
</template>

<script lang="ts">
export interface InspectionReport {
  report_no: string;
  institution: string;
  result: '' | 'qualified' | 'unqualified';
  expires_on: string;
  confirmed_asset_ids: string[];
  tools?: Record<string, { result: InspectionReport['result']; expires_on: string }>;
}
export function emptyInspectionReport(value?: Partial<InspectionReport>): InspectionReport {
  return { report_no: '', institution: '', result: '', expires_on: '', ...value, confirmed_asset_ids: [...(value?.confirmed_asset_ids || [])],
    tools: value?.tools ? Object.fromEntries(Object.entries(value.tools).map(([id, item]) => [id, { ...item }])) : undefined };
}
function confirmsAssets(value: InspectionReport, ids: string[]) {
  return ids.length > 0 && ids.length === value.confirmed_asset_ids.length && ids.every(id => value.confirmed_asset_ids.includes(id));
}
export function inspectionReady(value: InspectionReport, ids: string[], actual: string) {
  return !!(actual && value.report_no.trim() && value.institution.trim() && confirmsAssets(value, ids) &&
    ids.every(id => {
      const entry = value.tools?.[id] || value;
      return entry.result && (!entry.expires_on || entry.result === 'qualified' && entry.expires_on >= actual);
    }));
}
</script>

<script setup lang="ts">
import { computed } from 'vue';

const props = defineProps<{ model: InspectionReport; assetIds: string[]; assets?: { id: string; name: string; model?: string }[];
  actual?: string; expiresOn?: string; editable?: boolean; disabled?: boolean }>();
const confirmed = computed(() => confirmsAssets(props.model, props.assetIds));
function changeToolResult(id: string, result: string) {
  const tools = { ...props.model.tools };
  if (result === 'qualified' || result === 'unqualified') tools[id] = { result, expires_on: '' };
  else delete tools[id];
  props.model.tools = tools;
}
</script>

<style scoped>
.report-check { min-width:0; margin:8px 0 0; border:1px solid var(--tool-line, var(--line)); border-radius:6px; padding:8px 10px; }
.report-check legend { font-weight:600; padding-inline:4px; }
.report-fields { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,155px),1fr)); align-items:start; gap:8px; }
.report-fields label { display:grid; gap:4px; min-width:0; }
.report-fields :is(input,select) { box-sizing:border-box; width:100%; min-width:0; min-height:32px; padding:4px 6px; border:1px solid var(--tool-line, var(--line)); border-radius:4px; background:var(--tool-surface, var(--surface)); color:inherit; font:inherit; }
.report-fields :is(small,dt) { color:var(--tool-muted, inherit); }
.report-readonly dd { margin:0; overflow-wrap:anywhere; }
.report-overrides { margin-top:10px; }
.report-overrides summary { cursor:pointer; }
.report-tool { padding-block:6px; border-top:1px solid var(--tool-line, var(--line)); }
.report-tool strong { min-width:0; overflow-wrap:anywhere; }
.report-confirm { display:flex; align-items:center; gap:6px; margin-top:10px; }
</style>
