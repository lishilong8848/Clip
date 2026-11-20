<script setup lang="ts">
import { computed,ref,useId,watch } from 'vue';
import ToolWorkbookInput from '../components/ToolWorkbookInput.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
const props=defineProps<{r:Record<string,any>}>();
type Cell={address:string;rowspan:number;colspan:number;check?:boolean;editable?:boolean;text_editable?:boolean;value:unknown;style?:Record<string,string>};
type Row={index:number;height:number;cells:Cell[]};
const group=useId();
const selected=ref<{address:string;value:unknown;check?:boolean}>();
const writable=computed(()=>!!props.r.editable && !!props.r.item?.can_edit && !props.r.busy);
watch(()=>[props.r.item?.id,props.r.activeSheet?.name,writable.value],()=>{selected.value=undefined;});
const sheet=computed(()=>props.r.activeSheet as {name:string;columns:number[];rows:Row[];titles?:{key:string;text:string;x:number;y:number;width:number;height:number;size:number;font?:string;left?:number;top?:number;color?:string;background?:string}[]});
const tracks=computed(()=>({
  width:props.r.sheetWidth+'px',
  gridTemplateColumns:sheet.value.columns.map(w=>w+'px').join(' '),
  gridTemplateRows:sheet.value.rows.map(row=>row.height+'px').join(' '),
}));
const choices=[{value:'completed',label:'已完成'},{value:'incomplete',label:'未完成'}];
const signature=computed(()=>props.r.reviewSignatures[props.r.item.id] || props.r.item.signature);
function position(cell:Cell,row:number){
  const letters=cell.address.match(/^[A-Z]+/)![0];
  const column=[...letters].reduce((n,c)=>n*26+c.charCodeAt(0)-64,0);
  const align=cell.style?.verticalAlign;
  return {...cell.style,gridColumn:column+' / span '+cell.colspan,gridRow:row+' / span '+cell.rowspan,
    justifyContent:align==='middle'?'center':align==='top'?'start':'end'};
}
function content(cell:Cell) {
  const r=props.r,key=r.cellKey(cell);
  if(cell.check){const v=r.item.checks[key];return v==='是'?'是[√]　否[ ]':v==='否'?'是[ ]　否[√]':cell.value;}
  const review=r.reviewCell(cell);
  if(review==='time' && r.reviewDates[r.item.id])return r.reviewDates[r.item.id].replace('T',' ');
  return review?r.reviewText(cell):r.sheetText(cell);
}
function checked(cell:Cell,value:string){
  if(props.r.item.completion)return props.r.item.completion===value;
  return String(props.r.reviewText(cell)).includes(value==='completed'?'已完成[√]':'未完成[√]');
}
</script>
<template>
  <div class="paper-scroll" role="region" aria-label="表格预览" tabindex="0">
    <div class="paper" :style="tracks" role="table" :aria-label="sheet.name">
      <div v-for="(row,i) in sheet.rows" :key="row.index" role="row" class="paper-row" :aria-rowindex="row.index">
        <div v-for="c in row.cells" :key="c.address" role="cell" :data-address="c.address" :title="sheet.name+'!'+c.address"
          :class="{'paper-signed':r.isSignatureCell(c) && r.isPng(signature || ''),'paper-short':row.height<42 && c.rowspan===1}" :aria-rowspan="c.rowspan" :aria-colspan="c.colspan" :style="position(c,i+1)" class="paper-cell">
          <div v-if="r.reviewCell(c)==='result' && r.editable && r.item.can_edit" class="paper-choices" role="radiogroup" aria-label="原表完成情况">
            <label v-for="choice in choices" :key="choice.value"><input type="radio" :name="group" :value="choice.value" :checked="checked(c,choice.value)" :disabled="!!r.busy" @click="r.setCompletion($event)"/>{{ choice.label }}</label>
          </div>
          <button v-else-if="writable && !r.reviewCell(c) && (c.check || c.editable || c.text_editable)" type="button"
            class="paper-edit" :aria-label="'编辑 '+r.cellKey(c)" @click="selected=c">{{ content(c) }}</button>
          <template v-else><span>{{ content(c) }}</span><img v-if="r.isSignatureCell(c) && r.isPng(signature || '')" class="paper-signature" :src="signature" alt="审核签名"/></template>
        </div>
      </div>
      <img v-for="image in r.sheetImages" :key="image.key" class="paper-image" :src="image.src" :style="image.style" :alt="'表内图片 '+image.key"/>
      <div v-for="t in sheet.titles" :key="t.key" class="paper-title" :style="{left:t.x+'px',top:t.y+'px',width:t.width+'px',height:t.height+'px',fontSize:t.size+'px',fontFamily:t.font,paddingInline:(t.left||0)+'px',paddingBlock:(t.top||0)+'px',color:t.color,backgroundColor:t.background}">
        <button v-if="writable" class="paper-edit" type="button" :aria-label="'编辑表格标题 '+t.key" @click="selected={address:t.key,value:t.text}">{{ r.item.values[r.cellKey({address:t.key})] ?? t.text }}</button>
        <span v-else>{{ r.item.values[r.cellKey({address:t.key})] ?? t.text }}</span></div>
    </div>
  </div>
  <ToolDeskDialog v-if="selected && writable" :title="'填写 '+r.cellKey(selected)" @close="selected=undefined">
    <select v-if="selected.check" data-native-select :aria-label="'检查 '+r.cellKey(selected)" :value="r.item.checks[r.cellKey(selected)] || ''" @change="r.setCheck(selected,$event)">
      <option value="">待确认</option><option>是</option><option>否</option>
    </select>
    <ToolWorkbookInput v-else :kind="r.cellDateKind(selected)" :label="'填写 '+r.cellKey(selected)" :original="String(selected.value ?? '')"
      :value="String(r.inputCellValue(selected))" @change="value=>selected && r.setCellValue(selected,value)"/>
  </ToolDeskDialog>
</template>
<style scoped>
.tool-desk button.paper-edit{display:block;pointer-events:auto;width:100%;height:100%;min-height:0;padding:0;border:0;border-radius:0;background:transparent;color:inherit;box-shadow:none;line-height:inherit;font:inherit;text-align:inherit;white-space:inherit;overflow:hidden;overflow-wrap:anywhere}
.tool-desk button.paper-edit:focus-visible{outline:2px solid #166f50;outline-offset:-2px}
.paper-scroll{width:fit-content;max-width:100%;min-width:0;max-height:68vh;overflow:auto;overscroll-behavior:contain;background:var(--td-field);border:1px solid var(--td-line)}
.paper{position:relative;display:grid;background:white;color:#151515;font:13px/1.15 "Microsoft YaHei",sans-serif}
.paper-row{display:contents}.paper-cell{min-height:0;min-width:0;overflow:hidden;display:flex;flex-direction:column;padding:1px 3px;box-sizing:border-box}
.paper-cell>span{overflow-wrap:anywhere;flex-shrink:1;min-height:0}.paper-cell .paper-signature{align-self:center;min-height:0;max-height:68%;max-width:95%;object-fit:contain}
.paper-cell.paper-signed.paper-short{flex-direction:row;align-items:center;justify-content:flex-start!important;gap:3px}.paper-signed.paper-short .paper-signature{max-width:60%;max-height:100%;min-width:0}.paper-image,.paper-title{position:absolute;pointer-events:none;max-width:none}.paper-title{box-sizing:border-box;display:grid;align-content:center;text-align:center;white-space:pre-wrap;overflow:hidden;font-weight:700}
.paper-choices,.paper-choices label{display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:.24em}.paper-choices{gap:.8em}.tool-desk .paper-choices input[type=radio]{accent-color:#136648;margin:0;width:1em;height:1em;min-height:0;box-shadow:none}
</style>
