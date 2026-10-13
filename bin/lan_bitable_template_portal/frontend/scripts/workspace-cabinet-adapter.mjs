// Only presentation and the initial input mode differ in V2; all handlers stay original.
export function adaptCabinetBatchPage(source) {
  let code = source.replaceAll('\r\n', '\n');
  const replace = (before, after) => {
    if (code.split(before).length !== 2) throw new Error(`Cabinet V2 contract changed: ${before.slice(0, 80)}`);
    code = code.replace(before, after);
  };
  replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NButton, NDropdown } from "naive-ui";\nimport DataBars from "../workspace-v2/MotionChart.vue";');
  replace("const createMode = ref('pdf')", "const createMode = ref('text')");
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
