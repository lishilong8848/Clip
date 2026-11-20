<script setup lang="ts">
import { computed } from 'vue';
import ToolDeskSheet from './ToolDeskSheet.vue';
import ToolDeskReview from './ToolDeskReview.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
import ToolInspectionReportFields, { inspectionReady } from '../components/ToolInspectionReportFields.vue';
const props=defineProps<{r:Record<string,any>}>();
const tools=computed(() => props.r.session.items.map((entry:{asset:{id:string;name:string;model?:string}})=>entry.asset));
const writable=computed(()=>props.r.editable && !!props.r.item?.can_edit);
const external=computed(()=>props.r.session.kind==='external');
const ready=computed(()=>!external.value || (!!props.r.session.report_name && !props.r.report && inspectionReady(props.r.reportCheck,props.r.reportAssets,props.r.actualDate)));
const submitLabel=computed(()=>external.value?'提交送检报告':props.r.resubmitAsset?'重新提交本台审核':'提交自检审核');
const workbookControls=computed(()=>{
  const sheet=props.r.sheets.find((entry:{name:string})=>entry.name===props.r.item?.template?.schema?.sheet);
  return new Set(sheet?.rows.flatMap((row:{cells:{address:string}[]})=>
    row.cells.map(cell=>props.r.reviewCell(cell,sheet)))||[]);
});
</script>
<template>
  <section class="td-panel">
    <div class="td-heading"><h2 id="tool-editor-title" tabindex="-1">{{ external?'送检报告':'维护自检' }} · {{ r.session.items.length }} 台</h2><div class="td-actions"><span class="td-chip">{{ r.sessionStatus(r.session) }}</span><button class="td-link" @click="r.returnToSelection()">← 返回{{ r.recordsOpen?'办理记录':'工具选择' }}</button></div></div>
    <div class="td-row">
      <label class="td-field">当前工具<select data-native-select :value="r.itemId" @change="r.selectItem(($event.target as HTMLSelectElement).value)">
        <option v-for="entry in r.session.items" :key="entry.id" :value="entry.id">{{ entry.asset.name }} · {{ r.scopeLabel(entry.asset.scope) }} · {{ r.archiveStatus(entry.archive_status) }}</option></select></label>
      <div class="td-actions"><button v-if="r.item" @click="r.emit('asset-history',r.item.asset_id)">工具履历</button>
        <button v-if="r.item?.can_withdraw" @click="r.confirmItemAction('withdraw')">撤回本台审核</button>
      <button v-if="r.item?.can_reassign" @click="r.startReassign()">改派审核人</button><button v-if="r.item?.can_release" @click="r.confirmItemAction('release')">取消本台办理</button>
        <button v-if="r.reviewable.length" @click="r.openBatchReview()">批量审核（{{ r.reviewable.length }}）</button>
        <a v-if="['pending','archived'].includes(r.session.status) && r.session.report_available!==false" class="td-button" :href="r.sessionUrl+'/download'" download>下载批次归档</a>
      </div>
    </div>
    <p v-if="r.session.binding_pending" class="td-notice">正在核对工具与模板的关联。</p>
    <div v-if="r.item?.binding_error && r.session.status==='opening'" class="td-stack">
      <p class="td-notice">{{ r.item.binding_error }}</p><label class="td-field">更换模板<select data-native-select v-model="r.replacementTemplate"><option :value="null">选择可用模板</option><option v-for="t in r.templates" :value="t.id" :key="t.id">{{ r.templateName(t.id) }}</option></select></label>
      <div class="td-actions"><button :disabled="!r.replacementTemplate" @click="r.resolveBinding(false)">重新关联</button><button @click="r.retryBindings()">重试核验</button><button @click="r.resolveBinding(true)">移出本批</button></div>
    </div>
    <p v-if="r.item?.review_note || r.item?.result_notice || r.item?.archive_error" class="td-notice">{{ [r.item.review_note,r.item.result_notice,r.item.archive_error].filter(Boolean).join(' · ') }}</p>
    
  </section>
  <section v-if="external" class="td-panel">
    <div class="td-heading"><h2>报告与覆盖工具</h2>
      <a v-if="r.session.report_available!==false && r.session.report_name" class="td-button td-report-link"
        :href="r.sessionUrl+'/report-file'" target="_blank" rel="noopener">{{ r.session.report_name }}</a></div>
    <p v-if="r.session.report_available===false" class="td-notice">共用报告包含其他权限范围的工具。</p>
    <template v-else>
      <div class="td-report-inputs">
        <label class="td-field">实际检测日期<input v-model="r.actualDate" :disabled="!r.editable" type="date"/></label>
        <div v-if="r.editable" class="td-report-inputs td-report-attachment">
          <label class="td-field">送检报告附件（最多 12 MB）<input type="file" accept=".pdf,.jpg,.jpeg,.png,.xlsx,.docx" @change="r.pickReport"/></label>
          <button :disabled="!r.report" :title="r.report?.name" @click="r.uploadReport()">上传报告</button>
        </div>
      </div>
      <ToolInspectionReportFields :model="r.reportCheck" :asset-ids="r.reportAssets" :assets="tools" :actual="r.actualDate" :expires-on="r.session.inspection_expires_on" :editable="r.editable" :disabled="!!r.busy"/>
    </template>
  </section>
  <div v-else-if="r.item" class="td-workbook-editor">
    <ToolDeskSheet :r="r"/>
    <section class="td-panel td-editor-outcome"><div class="td-heading"><h2>处理结论</h2></div>
      <fieldset class="td-stack" :disabled="!writable">
        <label v-if="!workbookControls.has('result')" class="td-field">完成情况<select data-native-select :value="r.item.completion || ''" @change="r.setCompletion"><option value="">按检查项判断</option><option value="completed">已完成</option><option value="incomplete">未完成</option></select></label>
        <label class="td-field">检查结果<select data-native-select v-model="r.item.outcome"><option value="">请选择</option><option value="normal">正常</option><option value="repair">转检修</option><option value="scrap">申请报废</option></select></label>
        <label v-if="!workbookControls.has('reviewer')" class="td-field">审核人<select data-native-select :value="r.item.reviewer_id || ''" @focus="r.loadReviewers()" @change="r.setReviewer">
          <option value="">本楼有权限的审核人</option><option v-if="r.item.reviewer_id && !r.reviewerOptions.some((p:any)=>p.id===r.item.reviewer_id)" :value="r.item.reviewer_id">{{ r.item.reviewer_name || r.item.reviewer }}</option>
          <option v-for="p in r.reviewerOptions" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
      </fieldset>
      <label v-if="r.item.outcome==='repair' || r.item.outcome==='scrap' || r.item.reason" class="td-field">处理原因<textarea v-model="r.item.reason" maxlength="1000" :disabled="!writable"/></label>
      <template v-if="r.item.outcome==='scrap'"><label class="td-field">报废数量<input v-model.number="r.item.quantity" :disabled="!writable" type="number" min="1" step="1"/></label>
        <label v-if="writable" class="td-field">报废现场照片（最多 3 张，每张 1 MB）<input type="file" accept="image/jpeg,image/png" multiple :disabled="!r.canAddScrapPhoto" @change="r.pickScrapPhotos(r.item,$event)"/></label>
        <div class="td-actions"><span v-for="(photo,i) in r.item.scrap_photos" :key="i"><button @click="r.photoPreview=photo"><img :src="photo" width="72" height="52" alt="报废现场照片"/></button><button v-if="writable" @click="r.removeScrapPhoto(r.item,i)">移除</button></span></div>
      </template>
      <div v-if="writable" class="td-actions">
        <button @click="r.saveDraft()">保存草稿</button>
        <button class="td-primary" @click="r.ask(submitLabel+'？',r.resubmitAsset?r.item.asset.name:'请核对本批工具及填写结果。',r.submit)">{{ submitLabel }}</button>
      </div>
    </section>
  </div>
  <section v-if="r.editable && external" class="td-panel"><div class="td-row">
    <p>确认报告、结论与覆盖工具后提交。</p>
    <button class="td-primary" :disabled="!ready" @click="r.ask(submitLabel+'？','请核对本批工具及填写结果。',r.submit)">{{ submitLabel }}</button>
  </div></section>
  <ToolDeskReview :r="r"/>
  <ToolDeskDialog v-if="r.photoPreview" title="报废现场照片" @close="r.photoPreview=''"><img :src="r.photoPreview" alt="报废现场照片大图"/></ToolDeskDialog>
</template>

<style scoped>
.td-workbook-editor{display:grid;grid-template-columns:minmax(0,1fr) minmax(235px,19rem);gap:.7rem;align-items:start;min-width:0}
.tool-desk .td-editor-outcome{padding:.75rem;gap:.65rem}
@container(max-width:1050px){.td-workbook-editor{grid-template-columns:minmax(0,1fr)}}
.td-report-inputs {display:flex;align-items:end;flex-wrap:wrap;gap:.45rem .8rem;min-width:0;max-width:100%}
.td-report-inputs .td-field,.td-report-inputs input {min-width:0;max-width:100%}
.td-report-attachment {flex-wrap:nowrap}
.td-report-attachment button {flex:none}
.td-report-link {min-width:0;max-width:100%;overflow-wrap:anywhere}
</style>
