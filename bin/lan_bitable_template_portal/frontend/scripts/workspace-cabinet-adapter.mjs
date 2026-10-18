// V2 presentation reuses the shared text-first input mode and business handlers.
export function adaptCabinetBatchPage(source) {
  let code = source.replaceAll('\r\n', '\n');
  const replace = (before, after) => {
    if (code.split(before).length !== 2) throw new Error(`Cabinet V2 contract changed: ${before.slice(0, 80)}`);
    code = code.replace(before, after);
  };
  replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NButton, NDropdown } from "naive-ui";\nimport DataBars from "../workspace-v2/MotionChart.vue";');
  if(!code.includes("const createMode = ref('text')"))throw new Error('Cabinet text-first input contract changed');
  replace(`<nav class="tabs" aria-label="批量登记方式"><button :class="{ active: createMode === 'pdf' }" @click="createMode = 'pdf'">PDF识别</button><button :class="{ active: createMode === 'image' }" @click="createMode = 'image'">图片识别</button><button :class="{ active: createMode === 'text' }" @click="createMode = 'text'">粘贴文本识别</button><button :class="{ active: createMode === 'manual' }" @click="createMode = 'manual'; loadDirectory()">手工批量</button></nav>`,
    `<nav class="v2-batch-methods" aria-label="批量登记方式">
      <NButton :type="createMode === 'text' ? 'primary' : 'default'" :disabled="uploading" :aria-pressed="createMode === 'text'" @click="createMode = 'text'"><template #icon><ClipboardPaste :size="17" /></template>粘贴文本识别</NButton>
      <NDropdown trigger="click" :options="[{key:'pdf',label:'PDF识别'},{key:'image',label:'图片识别'},{key:'manual',label:'手工批量'}]" @select="value => { createMode=String(value); if(value==='manual')loadDirectory(); }">
        <NButton :disabled="uploading" quaternary><template #icon><Files :size="16" /></template>{{ createMode==='text' ? '其他录入方式' : createMode==='pdf' ? 'PDF识别' : createMode==='image' ? '图片识别' : '手工批量' }}</NButton>
      </NDropdown>
    </nav>`);
  replace('<div class="text-input-bar">', '<div class="v2-paste-heading"><ClipboardPaste :size="21" aria-hidden="true" /><h2>待识别文本</h2><span>{{ textRows.length }} 条机柜记录</span></div>\n        <div class="text-input-bar">');
  replace('v-model="textInput" rows="2" aria-label="粘贴机柜确认文本"', 'v-model="textInput" rows="5" aria-label="粘贴机柜确认文本"');
  replace('<div class="text-toolbar">', '<div class="v2-paste-result-heading"><h2>识别结果</h2><span :class="{warning:textIssueCount}">{{ textIssueCount ? `${textIssueCount} 条待核对` : textRows.length ? \'待确认创建\' : \'等待粘贴\' }}</span></div>\n        <div class="text-toolbar">');
  replace('        <div class="v2-paste-result-heading">', `        <DataBars v-if="textRows.length" class="v2-paste-distribution" title="识别结果分布" caption="当前批次 · 条" :items="[
          {label:'已识别',value:textRows.filter(row=>!textRowMessage(row)).length,color:'#168879'},
          {label:'待核对',value:textRows.filter(row=>!textDuplicateIds.has(row.row_id) && textRowMessage(row)).length,color:'#a77319'},
          {label:'重复记录',value:textDuplicateIds.size,color:'#bb5260'}]" />
        <div class="v2-paste-result-heading">`);
  replace('title="识别结果分布" caption="当前批次 · 条"', `title="识别结果分布" caption="当前批次 · 条" @select="(_item,index)=>{textSource='';textSearch='';textReview=['ready','review_only','duplicates'][index] || '';textPage=1;textTable?.scrollIntoView({block:'center',behavior:'smooth'});}"`);
  replace('(textReview.value===\'duplicates\'?textDuplicateIds.value.has(row.row_id):Boolean(textRowMessage(row)))', "(textReview.value==='ready'?!textRowMessage(row):textReview.value==='review_only'?!textDuplicateIds.value.has(row.row_id)&&Boolean(textRowMessage(row)):textReview.value==='duplicates'?textDuplicateIds.value.has(row.row_id):Boolean(textRowMessage(row)))");
  replace('<option value="issues">待核对（{{ textIssueCount }}）</option>', '<option value="ready">已识别</option><option value="review_only">待核对（不含重复）</option><option value="issues">全部待核对（{{ textIssueCount }}）</option>');
  replace("'识别并创建待办' }}<span v-if=\"!uploading && textRows.length\">", "'确认并创建待办' }}<span v-if=\"!uploading && textRows.length\">");
  return code;
}

export function adaptCabinetPage(source) {
  let code=source.replaceAll('\r\n','\n');
  const archiveLoading='<LoadingIndicator v-if="allExportBusy">各楼正在导出</LoadingIndicator>';
  const archiveButton='<button v-if="!scope && !readOnly" class="primary" :disabled="!allExportReady || allExportBusy || [\'running\',\'checking\',\'interrupted\',\'failed\'].includes(allExportStatus)" @click="startAllExports">';
  const archiveResume='<button v-if="[\'failed\',\'interrupted\',\'checking\'].includes(allExportStatus)" :disabled="allExportBusy" @click="resumeAllExports">{{ allExportStatus === \'checking\' ? \'核验状态\' : \'继续原批次\' }}</button>';
  const archiveLabel=code.split('\n').find(line=>line.startsWith('const allExportStatusLabel = computed('));
  const archiveRetry='  allExportRetryAt.value = Number(batch.retry_at || 0);';
  if(code.split(archiveLoading).length!==2 || code.split(archiveButton).length!==2 || code.split(archiveResume).length!==2 || !archiveLabel || code.split(archiveRetry).length!==2)throw new Error('Cabinet archive progress contract changed');
  code=code.replace(archiveButton,'<button v-if="!scope && !readOnly" class="primary" data-v2-navigation :aria-label="v2ArchiveActionLabel" :disabled="v2ArchiveChecking || ((!allExportBatchId || allExportStatus === \'succeeded\') && !allExportReady) || (allExportBusy && !allExportRetryAt)" @click="v2ArchiveAction">');
  code=code.replace(archiveResume,'');
  code=code.replace('<template v-else>一键导出/上传所有楼栋</template>','<template v-else>{{ v2ArchiveActionLabel }}</template>');
  code=code.replace(archiveLoading,'<LoadingIndicator v-if="allExportBusy">{{ v2ArchiveButtonLabel }}</LoadingIndicator>');
  code=code.replace(archiveLabel,`const v2ArchiveProgress = ref({uploaded:0,error:''});
const v2ArchiveChecking = ref(false);
async function v2ArchiveAction(): Promise<void> {
  if(v2ArchiveChecking.value)return;
  v2ArchiveChecking.value=true;
  try {
    if(allExportBusy.value) {
      const batch=await read('export-batches/'+allExportBatchId.value,{},12000);
      applyAllExportBatch(batch);saveAllExportState();
    } else if(['running','failed','interrupted','checking'].includes(allExportStatus.value)) {
      if(allExportStatus.value!=='checking')applyAllExportBatch(await write('export-batches/'+allExportBatchId.value+'/resume',{}));
      void pollAllExports();
    } else { v2ArchiveChecking.value=false;void startAllExports(); }
  } catch(exc) { fail(exc); }
  finally { v2ArchiveChecking.value=false; }
}
const v2ArchiveButtonLabel = computed(() => allExportRetryAt.value ? (v2ArchiveChecking.value ? '正在核验原批次' : '核验归档进度') : ({uploading:'上传附件 '+v2ArchiveProgress.value.uploaded+'/5',archiving:'合并写入归档',linking:'补齐归档链接',verifying:'核验归档结果'} as Dict)[allExportPhase.value] || '正在生成五楼表格');
const v2ArchiveActionLabel = computed(() => allExportBusy.value ? v2ArchiveButtonLabel.value : v2ArchiveChecking.value ? '正在核验原批次' : ['running','failed','interrupted','checking'].includes(allExportStatus.value) ? '继续原批次' : '一键导出/上传所有楼栋');
const allExportStatusLabel = computed(() => {
  if(allExportStatus.value==='succeeded')return '五楼已归档';
  if(allExportStatus.value==='checking')return '状态待核验';
  if(allExportStatus.value==='interrupted')return '任务已中断，将继续原批次';
  if(allExportStatus.value==='failed')return allExportPhase.value==='linking' ? '附件已写入，链接待补齐' : '批次未完成，请继续原批次';
  const stage=({uploading:'已上传 '+v2ArchiveProgress.value.uploaded+'/5 份附件',archiving:'五份附件合并写入同一条记录',linking:'附件已写入，正在补齐归档链接',verifying:'正在回读核验归档',preparing:'正在生成五楼表格'} as Dict)[allExportPhase.value] || '正在处理原批次';
  return stage+(allExportRetryAt.value ? '，飞书处理中，稍后自动继续原批次' : '');
});`);
  code=code.replace(archiveRetry,archiveRetry+"\n  v2ArchiveProgress.value = {uploaded:Number(batch.uploaded_files || 0),error:String(batch.error || '')};");
  code=code.replace('<div class="all-export-items">','<p v-if="allExportRetryAt && v2ArchiveProgress.error" class="all-export-retry" role="status">{{ v2ArchiveProgress.error }}</p>\n      <div class="all-export-items">');
  code=code.replace('<script setup lang="ts">','<script setup lang="ts">\nimport CabinetBuildingSwitcher from "../workspace-v2/CabinetBuildingSwitcher.vue";\nimport {cabinetPalette as v2CabinetPalette} from "../workspace-v2/cabinetPalette";\nimport {nightMode as v2CabinetNight} from "../workspace-v2/theme";');
  const props='scope: string; isAdmin: boolean;';
  const heading='<div class="heading-title"><h1>{{ scope ? scope + \'楼机柜上下电\' : \'机柜上下电\' }}</h1><p>机柜台账 <span v-if="overview.updated_at">· 更新于 {{ overview.updated_at }}</span></p></div>';
  if(code.split(props).length!==2 || code.split(heading).length!==2)throw new Error('Cabinet building switcher contract changed');
  code=code.replace(props,'scope: string; scopeOptions?: {value:string;label:string}[]; isAdmin: boolean;');
  code=code.replace(heading,'<CabinetBuildingSwitcher :scope="scope" :scope-options="scopeOptions" />');
  const buildingTotal='<strong>{{ building.counts.total }}<small>机柜</small></strong>';
  if(code.split(buildingTotal).length!==2)throw new Error('Cabinet overview total contract changed');
  code=code.replace(buildingTotal,buildingTotal+'<span class="v2-cabinet-building-rate">上电率 {{ v2CabinetRateText(building.counts) }}</span>');
  // V2-only compact status summary: original counts/state actions and rack palette stay untouched.
  const rateSummary =
    '\n// Compact cabinet status summary: powered = formal + test (or the backend powered field).\n' +
    'const v2CabinetPowered = computed(() => { const c = overview.value.counts || {}; return Number(c.powered ?? Number(c.formal || 0) + Number(c.test || 0)); });\n' +
    'const v2CabinetTotal = computed(() => Number((overview.value.counts || {}).total || 0));\n' +
    'function v2CabinetRateText(counts: Dict): string { const c = counts || {}; const powered = Number(c.powered ?? Number(c.formal || 0) + Number(c.test || 0)); const total = Number(c.total || 0); return total > 0 ? (powered / total * 100).toFixed(1) + \'%\' : \'—\'; }\n' +
    'const v2CabinetRate = computed(() => v2CabinetRateText(overview.value.counts));\n';
  code=code.replace('import {nightMode as v2CabinetNight} from "../workspace-v2/theme";', 'import {nightMode as v2CabinetNight} from "../workspace-v2/theme";' + rateSummary);
  const palette="background: cell.color || s.fill || 'transparent', color: cell.rack && cell.state === 'formal' ? '#fff' : s.font_color || '#1e293b',";
  if(!code.includes(palette))throw new Error('Cabinet map palette contract changed');
  code=code.replace(palette,'...v2CabinetPalette(cell,v2CabinetNight.value),');
  const selected=':class="{ found: cell.rack && mapSearch.toUpperCase() === cell.rack }"';
  if(code.split(selected).length!==2)throw new Error('Cabinet map selection contract changed');
  code=code.replace(selected,':class="{ found: cell.rack && mapSearch.toUpperCase() === cell.rack, \'v2-rack-selected\': cell.rack && historyOpen && historyRoom === currentRoom && historyRack === cell.rack }" :aria-pressed="cell.rack ? historyOpen && historyRoom === currentRoom && historyRack === cell.rack : undefined"');
  const metricsBefore =
    '      <section class="metrics">\n' +
    '        <div v-for="item in metricItems" :key="item.key" :class="item.key"><button class="metric-link" :aria-label="\'查看\' + item.label + \'机柜\'" @click="showStateRacks(item.key)"><span>{{ item.label }}</span><strong>{{ overview.counts[item.key] }}</strong></button></div>\n' +
    '      </section>';
  if(code.split(metricsBefore).length!==2)throw new Error('Cabinet status summary contract changed');
  code=code.replace(metricsBefore,
    '      <section class="metrics v2-cabinet-status" aria-label="机柜状态汇总">\n' +
    '        <div class="v2-cabinet-rate" role="group" :aria-label="\'上电率 \' + v2CabinetRate + \'（已上电 \' + v2CabinetPowered + \' / 总数 \' + v2CabinetTotal + \'）\'" :title="\'已上电 \' + v2CabinetPowered + \' / 总数 \' + v2CabinetTotal + \'（\' + v2CabinetRate + \'）\'">\n' +
    '          <span class="v2-cabinet-rate-label">上电率</span>\n' +
    '          <strong class="v2-cabinet-rate-value" aria-hidden="true">{{ v2CabinetRate }}</strong>\n' +
    '        </div>\n' +
    '        <div class="v2-cabinet-counts" role="group" aria-label="各状态机柜数量">\n' +
    '          <button v-for="item in metricItems" :key="item.key" class="v2-cabinet-count" :class="\'v2-count-\' + item.key" :aria-label="\'查看\' + item.label + \'机柜\'" @click="showStateRacks(item.key)"><span class="v2-cabinet-count-label">{{ item.label }}</span><strong class="v2-cabinet-count-value">{{ overview.counts[item.key] }}</strong></button>\n' +
    '        </div>\n' +
    '      </section>');
  code += '\n<style scoped src="../workspace-v2/cabinetStatus.css"></style>\n';
  return code;
}
