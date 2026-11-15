<template>
  <fieldset v-if="editable" class="report-check" :disabled="disabled">
    <legend>核对报告</legend>
    <div class="report-fields">
      <label>报告编号 <input v-model.trim="model.report_no" maxlength="120" /></label>
      <label>检测机构 <input v-model.trim="model.institution" maxlength="160" /></label>
      <label>检测结论
        <select v-model="model.result" @change="model.expires_on = ''">
          <option value="">请选择</option><option value="qualified">合格</option><option value="unqualified">不合格</option>
        </select>
      </label>
      <label v-if="model.result === 'qualified'">有效至（选填）
        <input v-model="model.expires_on" type="date" :min="actual || undefined" />
        <small>留空按送检周期计算</small>
      </label>
    </div>
    <label class="report-confirm"><input type="checkbox" :checked="confirmed" :disabled="!assetIds.length || disabled"
      @change="model.confirmed_asset_ids = ($event.target as HTMLInputElement).checked ? [...assetIds] : []" />
      已核对报告与本次提交的 {{ assetIds.length }} 台工具一致
    </label>
  </fieldset>
  <dl v-else-if="model.result" class="report-fields report-readonly">
    <div><dt>报告编号</dt><dd>{{ model.report_no }}</dd></div>
    <div><dt>检测机构</dt><dd>{{ model.institution }}</dd></div>
    <div><dt>检测结论</dt><dd>{{ model.result === 'qualified' ? '合格' : '不合格' }}</dd></div>
    <div v-if="expiresOn || model.expires_on"><dt>有效至</dt><dd>{{ expiresOn || model.expires_on }}</dd></div>
  </dl>
</template>

<script lang="ts">
export interface InspectionReport {
  report_no: string;
  institution: string;
  result: '' | 'qualified' | 'unqualified';
  expires_on: string;
  confirmed_asset_ids: string[];
}
export function emptyInspectionReport(value?: Partial<InspectionReport>): InspectionReport {
  return { report_no: '', institution: '', result: '', expires_on: '', ...value, confirmed_asset_ids: [...(value?.confirmed_asset_ids || [])] };
}
function confirmsAssets(value: InspectionReport, ids: string[]) {
  return ids.length > 0 && ids.length === value.confirmed_asset_ids.length && ids.every(id => value.confirmed_asset_ids.includes(id));
}
export function inspectionReady(value: InspectionReport, ids: string[], actual: string) {
  return !!(actual && value.report_no.trim() && value.institution.trim() && value.result &&
    (!value.expires_on || value.expires_on >= actual) && confirmsAssets(value, ids));
}
</script>

<script setup lang="ts">
import { computed } from 'vue';

const props = defineProps<{ model: InspectionReport; assetIds: string[]; actual?: string; expiresOn?: string; editable?: boolean; disabled?: boolean }>();
const confirmed = computed(() => confirmsAssets(props.model, props.assetIds));
</script>

<style scoped>
.report-check { min-width:0; margin:8px 0 0; border:1px solid var(--tool-line, var(--line)); border-radius:6px; padding:8px 10px; }
.report-check legend { font-weight:600; padding-inline:4px; }
.report-fields { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,155px),1fr)); align-items:start; gap:8px; }
.report-fields label { display:grid; gap:4px; min-width:0; }
.report-fields :is(input,select) { box-sizing:border-box; width:100%; min-width:0; min-height:32px; padding:4px 6px; border:1px solid var(--tool-line, var(--line)); border-radius:4px; background:var(--tool-surface, var(--surface)); color:inherit; font:inherit; }
.report-fields :is(small,dt) { color:var(--tool-muted, inherit); }
.report-readonly dd { margin:0; overflow-wrap:anywhere; }
.report-confirm { display:flex; align-items:center; gap:6px; margin-top:10px; }
</style>
