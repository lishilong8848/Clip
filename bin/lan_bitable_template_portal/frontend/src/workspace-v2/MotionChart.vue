<template>
  <section class="v2-motion-chart" :aria-label="title || '业务图表'" :aria-busy="loading">
    <header v-if="title || caption || trend || $slots.actions"><h3 v-if="title">{{ title }}</h3><span>{{ caption }}</span><slot name="actions" /><NRadioGroup v-if="trend" v-model:value="kind" size="small" aria-label="图表类型"><NRadioButton value="bar">柱状图</NRadioButton><NRadioButton value="line">折线图</NRadioButton><NRadioButton value="area">面积图</NRadioButton></NRadioGroup></header>
    <div class="v2-chart-surface">
      <div ref="host" class="v2-chart-canvas" :class="{muted:unavailable}" role="img" :aria-hidden="unavailable" :aria-label="accessibleLabel" @pointerdown="scrub" @pointermove="move" @pointerup="release" @pointercancel="release" @pointerleave="release" />
      <span v-if="visible && isTrend(kind) && endPoint && !pressed && !unavailable && !engineFailed" class="v2-chart-end" :style="{left:endPoint[0]+'px',top:endPoint[1]+'px'}" aria-hidden="true" />
      <div v-if="!unavailable && !engineFailed && kind==='donut'" class="v2-chart-total" aria-hidden="true"><strong>{{ total.toLocaleString('zh-CN') }}</strong><span>合计 · {{ unit }}</span></div>
      <div v-if="loading" class="v2-chart-overlay"><NSkeleton height="100%" :sharp="false" /></div>
      <NEmpty v-else-if="failed || invalid || engineFailed || !items.length" class="v2-chart-overlay" :description="failed || invalid ? '数据暂不可用' : engineFailed ? '图表暂不可用，请查看数据明细' : '暂无数据'" size="small" />
      <p v-else-if="isCircular(kind) && total===0" class="v2-chart-overlay v2-chart-zero">当前合计 0 {{ unit }}</p>
    </div>
    <details v-if="!loading&&!failed&&!invalid&&items.length" class="v2-chart-data" :open="engineFailed || narrow && isCircular(kind)">
      <summary>数据明细 <span>{{ items.length }} 项</span></summary>
      <div class="v2-chart-details" aria-label="图表明细"><NButton v-for="(item,index) in items" :key="String(item.key??item.label)+index" data-v2-navigation text @click="$emit('select',item,index)"><i :style="{background:chartPalette[index%chartPalette.length]}" aria-hidden="true" /><span>{{ item.label }}</span><strong>{{ item.value.toLocaleString('zh-CN') }} {{ unit }}</strong><ChevronRight :size="14" /></NButton></div>
    </details>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NButton,NEmpty,NSkeleton,NRadioGroup,NRadioButton } from 'naive-ui';
import { ChevronRight } from 'lucide-vue-next';
import type { EChartsType } from 'echarts/core';
import { chartOption,chartPalette,isCircular,isTrend,type ChartItem,type ChartView } from './vipboard';
import { nightMode } from './theme';
const props=withDefaults(defineProps<{title:string;caption?:string;items:ChartItem[];loading?:boolean;failed?:boolean;trend?:boolean;view?:ChartView;unit?:string}>(),{unit:'条'});
const emit=defineEmits<{select:[item:ChartItem,index:number]}>();
const host=ref<HTMLDivElement>(),kind=ref<ChartView>(props.view||'bar'),pressed=ref(false),endPoint=ref<number[]|null>(null);
const engineFailed=ref(false),visible=ref(false),narrow=ref(false);
const invalid=computed(()=>props.items.some(item=>!Number.isFinite(item.value)||item.value<0));
const unavailable=computed(()=>Boolean(props.loading||props.failed||invalid.value||!props.items.length));
const total=computed(()=>invalid.value?0:props.items.reduce((sum,item)=>sum+item.value,0));
const accessibleLabel=computed(()=>props.items.map(item=>`${item.label} ${item.value} ${props.unit}`).join('，'));
let chart:EChartsType|undefined,resize:ResizeObserver|undefined,observer:IntersectionObserver|undefined,disposed=false,initializing=false,frame=0,inViewport=false;
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
function update(){
  endPoint.value=null;
  if(!chart||unavailable.value)return;
  try{chart.setOption(chartOption(props.items,kind.value,props.unit,narrow.value,!reduced.matches&&visible.value,nightMode.value),{replaceMerge:['series','xAxis','yAxis','dataZoom'],lazyUpdate:true});engineFailed.value=false;}
  catch{engineFailed.value=true;}
}
function updateEnd(){
  if(!chart||!props.items.length||!isTrend(kind.value)||unavailable.value)return;
  const i=props.items.length-1,point=chart.convertToPixel({seriesIndex:0},[i,props.items[i]!.value]) as number[];
  endPoint.value=Array.isArray(point)&&point.every(Number.isFinite)?point:null;
}
async function initialize(){
  if(initializing||chart||disposed)return;initializing=true;
  try{
    const {init}=await import('./chartEngine');
    if(disposed||!host.value)return;
    narrow.value=host.value.clientWidth<440;
    chart=init(host.value,undefined,{renderer:'svg'});chart.on('finished',updateEnd);
    chart.on('mousedown',(params:any)=>{const shape=params.event?.target;if(reduced.matches||!['bar','horizontal'].includes(kind.value)||shape?.type!=='rect')return;const bounds=shape.getBoundingRect();shape.originY=bounds.y+bounds.height;shape.animateTo({scaleY:.96},{duration:90,done:()=>{if(!disposed)shape.animateTo({scaleY:1},{duration:220,easing:'cubicOut'});}});});
    chart.on('click',(params:any)=>{if(unavailable.value)return;const item=props.items[params.dataIndex];if(item)emit('select',item,params.dataIndex);});
    resize=new ResizeObserver(()=>{cancelAnimationFrame(frame);frame=requestAnimationFrame(()=>{if(disposed)return;chart?.resize();const small=(host.value?.clientWidth||0)<440;if(narrow.value!==small){narrow.value=small;update();}updateEnd();});});
    resize.observe(host.value);update();
  }catch{engineFailed.value=true;}finally{initializing=false;}
}
function scrub(event:PointerEvent){if(!isTrend(kind.value)||!chart||unavailable.value)return;pressed.value=true;host.value?.setPointerCapture?.(event.pointerId);move(event);}
function move(event:PointerEvent){
  if(!pressed.value||!chart||!host.value||unavailable.value)return;
  const box=host.value.getBoundingClientRect(),point=chart.convertFromPixel({seriesIndex:0},[event.clientX-box.left,event.clientY-box.top]) as number[];
  if(!Array.isArray(point)||!Number.isFinite(point[0]))return;
  chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:Math.max(0,Math.min(props.items.length-1,Math.round(point[0]!)))});
}
function release(){if(!pressed.value)return;pressed.value=false;chart?.dispatchAction({type:'hideTip'});}
function visibility(){visible.value=inViewport&&!document.hidden;update();}
watch(()=>[props.items,props.loading,props.failed,props.unit,kind.value,nightMode.value],update,{deep:true});
watch(()=>props.view,value=>{if(value)kind.value=value;});
onMounted(()=>{observer=new IntersectionObserver(entries=>{inViewport=entries.some(e=>e.isIntersecting);visibility();if(inViewport)void initialize();},{rootMargin:'80px'});if(host.value)observer.observe(host.value);reduced.addEventListener('change',update);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;observer?.disconnect();resize?.disconnect();cancelAnimationFrame(frame);reduced.removeEventListener('change',update);document.removeEventListener('visibilitychange',visibility);chart?.dispose();});
</script>
<style scoped>
.v2-motion-chart{min-width:0;color:#294237}.v2-motion-chart header{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:10px}.v2-motion-chart h3{font-size:15px;font-weight:600;margin:0}.v2-motion-chart header>span{font-size:12px;color:#63746b;margin-right:auto}
.v2-chart-surface{position:relative;height:230px;min-width:0}.v2-chart-canvas{height:100%;width:100%;touch-action:pan-y;transition:opacity 180ms}.v2-chart-canvas.muted{opacity:0;pointer-events:none}.v2-chart-overlay{position:absolute;inset:12px;display:flex;align-items:center;justify-content:center}.v2-chart-overlay>.n-skeleton{width:100%}.v2-chart-zero{background:#ffffffd9;color:#63746b;font-size:13px;pointer-events:none}
.v2-chart-total{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);display:grid;text-align:center;pointer-events:none;max-width:35%}.v2-chart-total strong{font-size:24px;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}.v2-chart-total span{font-size:11px;color:#63746b;margin-top:2px}
.v2-chart-data{border-top:1px solid #e7eee9;font-size:12px;margin-top:8px}.v2-chart-data summary{cursor:pointer;padding:10px 2px;color:#526c5d;min-height:38px;box-sizing:border-box}.v2-chart-data summary span{float:right;color:#718277}.v2-chart-data summary:focus-visible{outline:2px solid #197f70;outline-offset:2px}.v2-chart-details{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:2px 14px}.v2-chart-details .n-button{width:100%;height:auto;min-height:38px;text-align:left;white-space:normal}.v2-chart-details :deep(.n-button__content){display:flex;width:100%;gap:7px;min-width:0;white-space:normal}.v2-chart-details span{flex:1;overflow-wrap:anywhere}.v2-chart-details strong{font-size:12px;white-space:nowrap;font-variant-numeric:tabular-nums}.v2-chart-details i{width:7px;height:7px;border-radius:2px;flex:none}
.v2-chart-end{position:absolute;width:7px;height:7px;background:#197f70;border-radius:50%;transform:translate(-50%,-50%);pointer-events:none;animation:v2-end-pulse 1.8s ease-in-out infinite}@keyframes v2-end-pulse{50%{box-shadow:0 0 0 5px #197f7018}}@media(max-width:760px){.v2-chart-surface{height:210px}.v2-chart-details .n-button,.v2-chart-data summary{min-height:44px}}@media(prefers-reduced-motion:reduce){.v2-chart-end{animation:none}.v2-chart-canvas{transition:none}}
</style>
