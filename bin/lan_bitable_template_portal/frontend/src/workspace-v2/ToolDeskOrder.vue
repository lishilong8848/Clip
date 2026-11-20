<script setup lang="ts">
import { computed, ref } from 'vue';
import ToolInspectionReportFields, {inspectionReady} from '../components/ToolInspectionReportFields.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
const props=defineProps<{r:Record<string,any>}>();
const r=props.r;
const confirmation=ref<{title:string; detail:string; run:()=>void}|null>(null);
const ready=computed(()=>r.order.kind==='self' || (r.order.report_name && inspectionReady(r.reportCheck,r.reportAssets,r.actual)));
function ask(title:string,detail:string,run:()=>void) { confirmation.value={title,detail,run}; }
function confirm() { const next=confirmation.value; confirmation.value=null; next?.run(); }
function openBatch() {
  r.historyAsset=r.order.asset_id || '';
  r.openHistoryWorkspace(r.order.workspace_id);
}
async function release() { await r.release(r.order); if (!r.error) r.closeOrder(); }
</script>
<template>
  <section class="td-panel">
    <div class="td-heading"><h2>{{ r.kindLabel(r.order) }}工单</h2>
    
    
    <div class="td-actions"><span class="td-chip">{{ r.status(r.order) }}</span>
      <button v-if="r.order.workspace_id" :disabled="r.busy" @click="openBatch">查看原办理批次</button>
      <button v-if="r.order.can_claim" :disabled="r.busy" @click="ask('领取并填写？',r.order.tool_name,()=>r.openOrder(r.order,true))">领取工单</button>
      <button v-if="r.order.can_release" :disabled="r.busy" @click="r.leave(()=>ask('释放工单？','释放后可重新领取。',release))">释放工单</button>
      <button v-if="r.order.status==='submitted'" :disabled="r.busy" @click="r.download(r.orderPath(r.order)+'/download')">下载工单归档</button>
    </div></div>
    <p>{{ [r.order.scope+'楼',r.order.location,r.order.due,r.order.result_label,r.order.tool_status_label].filter(Boolean).join(' · ') }}</p>
    <p v-if="r.order.result_notice" class="td-notice">{{ r.order.result_notice }}</p>
  </section>
  <section v-if="r.order.kind==='self'" class="td-panel"><div class="td-heading"><h2>自检表单</h2><small>已确认 {{ r.answered }} / {{ r.checkCount }} 项</small>
    <div v-if="r.order.can_edit" class="td-actions"><select data-native-select v-model="r.templateId" aria-label="追加填报模板"><option :value="null">选择填报模板</option><option v-for="t in r.templateOptions" :key="t.value" :value="t.value">{{ t.label }}</option></select>
      <button :disabled="r.busy || !r.templateId || r.records.length>=20" @click="r.addRecord()">追加表单</button></div></div>
    <div v-if="r.records.length" class="td-row"><label class="td-field">正在查看<select data-native-select v-model="r.selected"><option v-for="(e,i) in r.records" :key="e.entry_id" :value="e.entry_id">{{ Number(i)+1 }} · {{ e.template.name }} · v{{ e.version }}</option></select></label>
      <button v-if="r.active && r.order.can_edit" :disabled="r.busy" @click="ask('移除本份填报？',r.active.template.name,()=>r.removeRecord(r.selected))">移除本份表单</button></div>
    <template v-if="r.active">
      <p class="td-muted">{{ [r.active.template.schema.specialty,r.active.template.schema.applicable,r.active.template.schema.precautions].filter(Boolean).join(' · ') }}</p>
      <fieldset class="td-stack" :disabled="r.busy || !r.order.can_edit"><div class="td-grid">
        <label class="td-field">检查位置<input v-model="r.active.location"/></label><label class="td-field">检查人<input v-model="r.active.inspector"/></label>
        <label class="td-field">检查开始时间<input v-model="r.active.checked_at" type="datetime-local"/></label>
        <label class="td-field">审核人<input v-model="r.active.reviewer"/></label><label class="td-field">审核时间<input v-model="r.active.reviewed_at" type="datetime-local"/></label>
      </div>
      <div v-if="r.active.template_values" class="td-grid"><label v-for="(label,address) in r.active.template.schema.fields" :key="address" class="td-field">{{ label }} · {{ address }}<input v-model="r.active.template_values[address]"/></label></div>
      <div v-for="group in r.active.template.schema.calibration" :key="group.row" class="td-grid"><label v-for="(label,address) in group.fields" :key="address" class="td-field">{{ group.label }} · {{ label }} · {{ address }}<input v-model="r.active.template_values[address]"/></label></div>
      <div class="td-list"><label v-for="(c,i) in r.active.template.schema.checks" :key="c.row" class="td-row"><span><strong>{{ c.name }}</strong><br/><small>{{ c.method }} · {{ c.standard }}</small></span>
        <select data-native-select v-model="r.active.checks[i]"><option value="">待确认</option><option>是</option><option>否</option></select></label></div>
      <label class="td-field">检查备注<textarea v-model="r.active.notes" maxlength="1500"/></label></fieldset>
    </template><p v-else class="td-empty">暂无填报表单</p>
  </section>
  <section v-else class="td-panel"><h2>送检报告</h2><label class="td-field">实际检测日期<input v-model="r.actual" type="date" :disabled="r.busy || !r.order.can_edit"/></label>
    <label v-if="r.order.can_edit" class="td-field">上传报告（最多 12 MB）<input type="file" accept=".pdf,.jpg,.jpeg,.png,.xlsx,.docx" :disabled="r.busy" @change="r.uploadReport"/></label>
    <a v-if="r.order.report_name" class="td-button" :href="r.api+r.orderPath(r.order)+'/report-file'" target="_blank" rel="noopener">{{ r.order.report_name }}</a>
    <ToolInspectionReportFields :model="r.reportCheck" :asset-ids="r.reportAssets" :actual="r.actual" :expires-on="r.order.inspection_expires_on" :editable="r.order.can_edit" :disabled="r.busy"/>
  </section>
  <section class="td-panel"><div class="td-heading"><h2>处置与审批</h2><span v-if="r.order.outcome" class="td-chip">{{ r.outcomeLabels[r.order.outcome] || r.order.outcome }}</span>
      <div v-if="r.order.can_repair || r.order.can_approve" class="td-actions"><button v-for="s in r.transitions" :key="s.action" :class="{'td-primary':s.type==='primary'}" :disabled="r.busy || ((r.order.can_repair || s.action==='reject') && !r.reason.trim())" @click="ask(s.label+'？',r.order.tool_name+' · '+r.reason,()=>r.advance(s.action))">{{ s.label }}</button></div></div>
    <p v-if="r.order.scrap_qty > 0" class="td-muted">申请报废 {{ r.order.scrap_qty }} 件</p>
    <p v-if="r.order.review_note" class="td-notice">审核意见 · {{ r.order.review_note }}</p>
    <p v-if="r.order.reason || r.order.resolution">{{ [r.order.reason,r.order.resolution].filter(Boolean).join(' · ') }}</p>
    <fieldset :disabled="r.busy" class="td-stack">
      <label v-if="r.order.can_edit && r.order.kind==='self'" class="td-field">本台检查结果<select data-native-select v-model="r.outcome"><option value="">请选择</option><option v-for="o in r.outcomeOptions" :key="o.value" :value="o.value">{{ o.label }}</option></select></label>
      <label v-if="r.order.can_repair || r.order.can_approve || (r.order.can_edit && r.order.kind==='self')" class="td-field">处理说明<textarea v-model="r.reason" maxlength="1000"/></label>
      <div v-if="r.order.can_repair || (r.order.can_edit && r.outcome==='scrap')" class="td-grid"><label class="td-field">申请报废数量<input v-model.number="r.quantity" type="number" min="1" step="1" :max="r.order.quantity"/></label>
      <label v-if="r.order.can_repair" class="td-field">检修后申请报废的现场照片（最多 3 张，每张 1 MB）<input type="file" accept="image/jpeg,image/png" multiple @change="r.selectScrapPhotos"/></label></div>
      <div v-if="r.order.can_repair && r.repairPhotos.length" class="td-actions"><img v-for="(p,i) in r.repairPhotos" :key="i" :src="p" width="80" alt="报废现场"/><button @click="r.repairPhotos=[]">清除照片</button></div>
      <template v-if="r.order.can_review"><label class="td-field">自检审核意见<textarea v-model="r.reviewReason" maxlength="1000"/></label>
        <div class="td-actions"><button class="td-primary" @click="ask('通过自检审核？',r.order.tool_name,()=>r.advance('review-approve'))">通过审核</button>
          <button :disabled="!r.reviewReason.trim()" @click="ask('退回重填？',r.reviewReason,()=>r.advance('review-reject'))">退回重填</button></div></template>
      <div v-if="r.order.can_edit" class="td-actions"><button v-if="r.order.kind==='self'" @click="r.save('draft')">保存草稿</button>
        <button class="td-primary" :disabled="!ready" @click="ask('提交工单？',r.order.tool_name,()=>r.save('submit'))">提交工单</button></div>
    </fieldset>
    <p v-for="(v,i) in r.order.scrap_approvals" :key="i">{{ v.name }} · {{ r.voteLabel(v.decision) }} {{ v.at }} {{ v.note }}</p>
  </section>
  <section v-if="r.order.history?.length || r.order.sync?.length" class="td-panel"><h2>办理轨迹</h2>
    <p v-for="(s,i) in r.order.sync" :key="'s'+i">{{ s.label }} · {{ s.state }} {{ s.error }}</p>
    <div class="td-list"><p v-for="(e,i) in r.order.history" :key="i">{{ r.displayTime(e.at) }} · {{ e.name }} · {{ e.action }} {{ e.detail }}</p></div></section>
  <ToolDeskDialog v-if="confirmation" :title="confirmation.title" @close="confirmation=null"><div class="td-row"><p>{{ confirmation.detail }}</p><div class="td-actions"><button @click="confirmation=null">返回</button><button class="td-primary" @click="confirm()">确认</button></div></div></ToolDeskDialog>
</template>