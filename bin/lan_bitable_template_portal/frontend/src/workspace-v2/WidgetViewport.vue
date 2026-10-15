<template>
  <div ref="host" class="v2-widget-body v2-responsive-body" :inert="editing" :data-capacity="size.compact?'compact':'wide'" :style="{'--widget-width':size.width+'px','--widget-height':size.height+'px','--widget-rows':size.rows}">
    <slot />
    <div v-if="counts.total>counts.shown" class="v2-visible-count" aria-live="polite">本页显示 {{counts.shown}} / {{counts.total}} 条 <button type="button" data-v2-navigation @click="next">下一组</button><button v-if="offset" type="button" data-v2-navigation @click="first">回到首组</button></div>
  </div>
</template>
<script setup lang="ts">
import { computed,nextTick,onBeforeUnmount,onMounted,provide,reactive,ref,watch } from 'vue';
import { capacity,capacityContext } from './pageLayout';
const props=defineProps<{editing?:boolean;adaptive?:boolean}>();
const host=ref<HTMLElement>(),size=ref(capacity(800,480)),counts=reactive({shown:0,total:0});
provide(capacityContext,computed(()=>size.value));
let resize:ResizeObserver|undefined,mutation:MutationObserver|undefined,frame=0,offset=0,pageStep=3;
const hiddenRows=new Set<HTMLElement>(),hiddenCells=new Set<HTMLElement>();
function reset(){for(const el of hiddenRows)el.removeAttribute('data-v2-capacity-hidden');for(const el of hiddenCells)el.removeAttribute('data-v2-capacity-column');hiddenRows.clear();hiddenCells.clear();}
function showRows(rows:HTMLElement[]){
  if(!rows.length)return;
  const row=rows[0]!,height=Math.max(32,row.getBoundingClientRect().height||48),top=Math.max(0,row.getBoundingClientRect().top-host.value!.getBoundingClientRect().top+host.value!.scrollTop);
  const count=Math.max(3,Math.min(60,Math.floor((size.value.height-top-76)/height))),start=Math.min(offset,Math.max(0,rows.length-count));pageStep=Math.min(pageStep,count);
  for(let i=0;i<rows.length;i++)if((i<start||i>=start+count)&&!rows[i]!.contains(document.activeElement)){rows[i]!.setAttribute('data-v2-capacity-hidden','');hiddenRows.add(rows[i]!);}
  counts.total+=rows.length;counts.shown+=rows.filter(row=>!hiddenRows.has(row)).length;
}
function adapt(){
  const root=host.value;if(!root)return;reset();counts.total=counts.shown=0;pageStep=60;if(!props.adaptive)return;
  for(const table of root.querySelectorAll<HTMLTableElement>('table')){
    if(table.closest('[role="dialog"],.n-modal,.modal,.editor-body')||table.querySelector('input:not([type="checkbox"]),textarea,select,[contenteditable="true"]')||table.querySelector('[rowspan]:not([rowspan="1"]),[colspan]:not([colspan="1"])'))continue;
    const heads=[...table.querySelectorAll<HTMLTableCellElement>('thead tr:last-child th')];
    const budget=size.value.columns,keep=new Set<number>();
    const priority=heads.map((cell,i)=>({i,score:i===0?100:cell.querySelector('input')?100:/操作|查看/.test(cell.textContent||'')?95:/状态|进度/.test(cell.textContent||'')?90:/姓名|名称|标题|事项/.test(cell.textContent||'')?85:/日期|时间|总数/.test(cell.textContent||'')?70:20-i})).sort((a,b)=>b.score-a.score);
    for(const item of priority.slice(0,budget))keep.add(item.i);
    const focused=(document.activeElement as HTMLElement|null)?.closest('td,th') as HTMLTableCellElement|null;if(focused&&table.contains(focused))keep.add(focused.cellIndex);
    if(heads.length>budget)for(const row of table.rows)for(let i=0;i<row.cells.length;i++)if(!keep.has(i)){row.cells[i]!.setAttribute('data-v2-capacity-column','');hiddenCells.add(row.cells[i]!);}
    if(heads.length>budget)[...table.querySelectorAll<HTMLElement>('colgroup > col')].forEach((col,i)=>{if(!keep.has(i)){col.setAttribute('data-v2-capacity-column','');hiddenCells.add(col);}});
    const rows=[...table.querySelectorAll<HTMLTableRowElement>('tbody > tr')].filter(row=>row.cells.length>1&&!/^\s*(总计|合计)/.test(row.textContent||''));
    showRows(rows);
  }
  for(const list of root.querySelectorAll<HTMLElement>('.v2-notice-list,.record-page-list,.kb-files,.link-list,.pc-list-items,.drill-list,.task-list,.notice-list,.mop-candidate-list')){
    if(list.closest('[role="dialog"],.n-modal'))continue;
    const rows=[...list.children].filter((child):child is HTMLElement=>child instanceof HTMLElement&&!child.matches('.empty,.n-empty,[role="status"],header,footer'));
    if(rows.every(row=>row.matches('button,article,.n-list-item,.notice-row,.record-row,.mop-row')))showRows(rows);
  }
}
function schedule(){cancelAnimationFrame(frame);frame=requestAnimationFrame(adapt);}
function next(){offset+=pageStep;if(offset>=counts.total)offset=0;adapt();}
function first(){offset=0;adapt();}
watch(()=>props.adaptive,()=>{offset=0;schedule();});
onMounted(()=>{if(!host.value)return;resize=new ResizeObserver(([entry])=>{if(!entry)return;const next=capacity(Math.round(entry.contentRect.width),Math.round(entry.contentRect.height));if(next.width!==size.value.width||next.height!==size.value.height){size.value=next;offset=0;schedule();}});resize.observe(host.value);mutation=new MutationObserver(records=>{if(records.some(record=>!(record.target as Element).closest?.('.v2-visible-count')))schedule();});mutation.observe(host.value,{childList:true,subtree:true});void nextTick(schedule);});
onBeforeUnmount(()=>{resize?.disconnect();mutation?.disconnect();cancelAnimationFrame(frame);reset();});
</script>
