<template>
  <div class="v2-learning-trend">
    <div ref="host" role="img" :aria-label="label" tabindex="0" @pointerdown="start" @pointermove="move" @pointerup="release" @pointercancel="release" @pointerleave="release" @keydown="key" />
    <NSkeleton v-if="loading" class="v2-learning-trend-state" height="260px" />
    <NEmpty v-else-if="failed || !items.some(row=>row.value!==null)" class="v2-learning-trend-state" :description="failed?'图表暂不可用，可查看数据明细':'暂无作答数据'" />
    <details class="v2-learning-trend-values"><summary>数据明细</summary><div><NButton v-for="item in items" :key="item.label" data-v2-navigation text :disabled="loading || item.value===null" @click="emit('select',item)"><span>{{item.label}}</span><strong>{{item.value===null?'无成绩':item.value+' '+unit}}</strong><ChevronRight :size="13" /></NButton></div></details>
  </div>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {NButton,NEmpty,NSkeleton} from 'naive-ui';
import {ChevronRight} from 'lucide-vue-next';
import type {EChartsType} from 'echarts/core';
import {chartOption} from './vipboard';
import {nightMode} from './theme';
type Point={label:string;value:number|null};
const props=defineProps<{items:Point[];unit:string;percentage?:boolean;loading?:boolean}>();
const emit=defineEmits<{select:[item:Point]}>();
const host=ref<HTMLElement>(),failed=ref(false),label=computed(()=>props.items.map(row=>row.label+'：'+(row.value===null?'无成绩':row.value+' '+props.unit)).join('，'));
let chart:EChartsType|undefined,resize:ResizeObserver|undefined,visible:IntersectionObserver|undefined,disposed=false,starting=false,pressed=false,ready=false,frame=0,selected=0,down:[number,number]=[0,0],dragged=false;
const reduced=matchMedia('(prefers-reduced-motion:reduce)');
function update(){
  ready=false;pressed=false;
  if(!chart||chart.isDisposed())return;
  chart.dispatchAction({type:'hideTip'});
  if(props.loading||!props.items.some(row=>row.value!==null)){chart.clear();return;}
  selected=Math.max(0,Math.min(selected,props.items.length-1));
  const base=chartOption(props.items.map(row=>({...row,value:row.value??0})),'line',props.unit,(host.value?.clientWidth||0)<440,!reduced.matches&&!document.hidden,nightMode.value) as any;
  // Null grades are gaps, never invented zero-percent results.
  base.series[0].data=props.items.map(row=>row.value);base.series[0].connectNulls=false;
  base.series[0].showSymbol=true;base.series[0].animationDuration=1000;
  if(props.percentage){base.yAxis.min=0;base.yAxis.max=100;base.yAxis.axisLabel.formatter='{value}%';}
  base.tooltip.formatter=(rows:any)=>{const index=(Array.isArray(rows)?rows[0]:rows)?.dataIndex,row=props.items[index];return row?row.label+'\n'+(row.value===null?'无成绩':row.value+' '+props.unit):'';};
  try{chart.setOption(base,{replaceMerge:['series','xAxis','yAxis','dataZoom'],lazyUpdate:false});ready=true;failed.value=false;}catch{failed.value=true;}
}
async function initialize(){
  if(chart||disposed||starting||!host.value)return;starting=true;
  try{const engine=await import('./chartEngine');if(disposed||!host.value)return;chart=engine.init(host.value,undefined,{renderer:'svg'});resize=new ResizeObserver(()=>{cancelAnimationFrame(frame);frame=requestAnimationFrame(()=>{if(!disposed){chart?.resize();update();}});});resize.observe(host.value);update();}catch{failed.value=true;}finally{starting=false;}
}
function move(event:PointerEvent){
  if(!pressed||!chart||!ready||!host.value||props.loading)return;
  const bounds=host.value.getBoundingClientRect(),point=chart.convertFromPixel({seriesIndex:0},[event.clientX-bounds.left,event.clientY-bounds.top]) as number[];
  if(Math.hypot(event.clientX-down[0],event.clientY-down[1])>8)dragged=true;
  if(!Array.isArray(point)||!Number.isFinite(point[0]))return;
  selected=Math.max(0,Math.min(props.items.length-1,Math.round(point[0]!)));chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:selected});
}
function start(event:PointerEvent){if(props.loading||!ready||!chart||!host.value)return;const box=host.value.getBoundingClientRect();if(!chart.containPixel({gridIndex:0},[event.clientX-box.left,event.clientY-box.top]))return;down=[event.clientX,event.clientY];dragged=false;pressed=true;host.value.setPointerCapture(event.pointerId);move(event);}
function release(event?:PointerEvent){const row=props.items[selected];if(pressed&&!dragged&&event?.type==='pointerup'&&row?.value!==null&&row&&!props.loading)emit('select',row);pressed=false;chart?.dispatchAction({type:'hideTip'});}
function key(event:KeyboardEvent){if(!chart||!ready||props.loading||!props.items.length)return;if(['ArrowLeft','ArrowRight'].includes(event.key)){event.preventDefault();selected=Math.max(0,Math.min(props.items.length-1,selected+(event.key==='ArrowLeft'?-1:1)));chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:selected});}else if(event.key==='Enter'){const row=props.items[selected];if(row&&row.value!==null)emit('select',row);}else if(event.key==='Escape')release();}
watch(()=>[props.items,props.unit,props.percentage,props.loading,nightMode.value],update,{deep:true});
onMounted(()=>{visible=new IntersectionObserver(entries=>{if(entries.some(entry=>entry.isIntersecting))void initialize();});if(host.value)visible.observe(host.value);reduced.addEventListener('change',update);});
onBeforeUnmount(()=>{disposed=true;visible?.disconnect();resize?.disconnect();cancelAnimationFrame(frame);reduced.removeEventListener('change',update);chart?.dispose();});
</script>
