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
