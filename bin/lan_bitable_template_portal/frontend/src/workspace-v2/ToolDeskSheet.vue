<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import ToolDeskSheetPreview from './ToolDeskSheetPreview.vue';
import ToolWorkbookInput from '../components/ToolWorkbookInput.vue';
const preview=ref(false);
const props = defineProps<{r:Record<string,any>}>();
interface Cell { address:string; value:unknown; check?:boolean; editable?:boolean; text_editable?:boolean }
interface Row { index:number; cells:Cell[] }
const full = ref(true);
const editing = ref('');
const canWrite = computed(() => props.r.editable && props.r.item?.can_edit && !props.r.busy);
watch(() => [props.r.itemId,props.r.activeSheet?.name], () => { editing.value=''; });
function caption(cell:Cell) {
  return props.r.item.values[props.r.cellKey(cell)] ?? cell.value;
}
const rows = computed(() => ((props.r.activeSheet?.rows || []) as Row[]).map(row => ({
  index:row.index,
  cells:row.cells.filter(cell => full.value || cell.editable || props.r.reviewCell(cell) || String(caption(cell) ?? '').trim()),
})).filter(row => row.cells.length));
function label(cell:Cell,cells:Cell[]) {
  const prior = cells.slice(0,cells.indexOf(cell)).reverse().find(c => !c.editable && !props.r.reviewCell(c) && caption(c));
  return String(prior ? caption(prior) : '') || cell.address;
}
</script>
<template>
  <section v-if="r.activeSheet" class="td-panel">
    <div class="td-heading"><h2>表单确认行</h2><div class="td-actions"><div class="td-tabs" aria-label="表单视图"><button :aria-pressed="!preview" @click="preview=false">字段填写</button><button :aria-pressed="preview" @click="preview=true">表格预览</button></div><select data-native-select aria-label="工作表" :value="r.activeSheet.name" @change="r.sheetName=($event.target as HTMLSelectElement).value">
      <option v-for="s in r.sheets" :key="s.name" :value="s.name">{{ s.name }}</option></select><label v-if="!preview" class="td-check"><input v-model="full" type="checkbox"/>显示空白单元格</label>
      <button v-if="canWrite && rows.some(row=>row.cells.some(cell=>cell.check))" @click="r.ask('本表检查项全部正常？','将本工作表的检查项填为「是」，并设置完成情况和本台结果。',r.normalFill)">本表全部正常</button></div></div>
    <ToolDeskSheetPreview v-if="preview" :r="r"/><template v-else>
    <label v-for="t in r.activeSheet.titles || []" :key="t.key" class="td-field">表内标题
      <input v-if="canWrite" :value="r.item.values[r.cellKey({address:t.key})] ?? t.text" @input="r.setCell({address:t.key},$event)"/>
      <span v-else>{{ r.item.values[r.cellKey({address:t.key})] ?? t.text }}</span></label>
    <div class="td-list"><section v-for="row in rows" :key="row.index" class="sheet-row">
      <small class="line-no">第 {{ row.index }} 行</small><div class="sheet-fields">
        <template v-for="cell in row.cells" :key="cell.address">
          <label v-if="cell.check" class="td-field check-field">检查确认 · {{ cell.address }}
            <select data-native-select :disabled="!canWrite" :value="r.item.checks[r.cellKey(cell)] || ''" @change="r.setCheck(cell,$event)">
              <option value="">待确认</option><option>是</option><option>否</option>
            </select>
          </label>
          <div v-else-if="r.reviewCell(cell)" class="sheet-text"><small>{{ cell.address }}</small>
            <span>{{ r.reviewText(cell) || '待确认' }}</span>
            <img v-if="r.isSignatureCell(cell) && r.isPng(r.item.signature)" :src="r.item.signature" alt="审核签名"/>
          </div>
          <div v-else-if="canWrite && r.specialCell(cell)" class="td-field"><span>{{ cell.address }}</span>
            <ToolWorkbookInput :key="r.cellKey(cell)" :kind="r.cellDateKind(cell)" :value="String(r.inputCellValue(cell))" :original="String(cell.value ?? '')"
              :label="'填写 '+r.cellKey(cell)" @change="value=>r.setCellValue(cell,value)"/>
          </div>
          <label v-else-if="cell.editable" class="td-field">{{ label(cell,row.cells) }} · {{ cell.address }}
            <input type="text" :disabled="!canWrite" :value="canWrite?r.inputCellValue(cell):r.sheetText(cell)" @input="r.setCell(cell,$event)"/>
          </label>
          <label v-else-if="editing===cell.address && canWrite" class="td-field">修改 {{ cell.address }}
            <textarea :value="caption(cell) as string" @input="r.setCell(cell,$event)" @keydown.esc="editing=''" />
            <button @click="editing=''">收起编辑</button></label>
          <div v-else class="sheet-text"><small>{{ cell.address }}</small>
            <button v-if="cell.text_editable && canWrite" class="td-link" :aria-label="'编辑 '+cell.address+' '+(caption(cell) ?? '')" @click="editing=cell.address">{{ caption(cell) === '' || caption(cell) == null ? '空白' : caption(cell) }}</button>
            <span v-else>{{ r.sheetText(cell) }}</span></div>
        </template>
      </div>
    </section></div>
    <details v-if="r.sheetImages.length"><summary>表内图片（{{ r.sheetImages.length }}）</summary>
      <img v-for="im in r.sheetImages" :key="im.key" :src="im.src" :alt="'表内图片 '+im.key"/></details></template>
  </section>
</template>
<style scoped>
.sheet-row{display:grid;grid-template-columns:4.7rem minmax(0,1fr);gap:.7rem}.line-no{padding-top:.3rem;font-variant-numeric:tabular-nums}
.sheet-fields{display:flex;align-items:start;flex-wrap:wrap;gap:.7rem 1.2rem}.sheet-fields>*{flex:1 1 130px;min-width:0}
.sheet-text{display:grid;align-content:start;white-space:pre-wrap}.sheet-text small{font-size:.72rem;color:var(--td-muted)}.sheet-text .td-link{display:block;text-align:left;padding:.2rem 0;min-height:30px;white-space:pre-wrap}.sheet-text img{max-height:50px}
@container(max-width:530px){.sheet-row{grid-template-columns:1fr;gap:.2rem}.sheet-fields>*{flex-basis:105px}}
</style>
