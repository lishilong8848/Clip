<script setup lang="ts">
import { computed } from 'vue';
import ToolDeskSheetPreview from './ToolDeskSheetPreview.vue';

const props = defineProps<{r:Record<string,any>}>();
const canWrite = computed(() => props.r.editable && props.r.item?.can_edit && !props.r.busy);
const hasChecks = computed(() => props.r.activeSheet?.rows.some((row:{cells:{check?:boolean}[]})=>row.cells.some(cell=>cell.check)));
const hasCompletion = computed(() => props.r.activeSheet?.rows.some((row:{cells:{address:string}[]})=>
  row.cells.some(cell=>props.r.reviewCell(cell)==='result')));
</script>

<template>
  <section v-if="r.activeSheet" class="td-panel td-sheet">
    <div class="td-heading">
      <div>
        <h2>表格预览</h2>
        <small v-if="canWrite">点击单元格填写</small>
      </div>
      <div class="td-actions">
        <select data-native-select aria-label="工作表" :value="r.activeSheet.name" @change="r.sheetName=($event.target as HTMLSelectElement).value">
          <option v-for="sheet in r.sheets" :key="sheet.name" :value="sheet.name">{{ sheet.name }}</option>
        </select>
        <button v-if="canWrite && hasCompletion && r.item.completion" class="td-link" @click="r.item.completion=''">按检查项判断</button>
        <button v-if="canWrite && hasChecks" @click="r.ask('本表检查项全部正常？','将本工作表的检查项填为「是」，并设置完成情况和本台结果。',r.normalFill)">本表全部正常</button>
      </div>
    </div>
    <ToolDeskSheetPreview :r="r"/>
  </section>
  <p v-else class="td-empty">该记录暂无可显示的工作表。</p>
</template>

<style scoped>
.tool-desk .td-sheet{padding:.65rem;gap:.65rem}
.td-sheet .td-heading {padding:.15rem .15rem 0}
</style>
