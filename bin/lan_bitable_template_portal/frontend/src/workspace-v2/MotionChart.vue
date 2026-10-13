<template>
  <section class="v2-motion-chart" :aria-label="title" :aria-busy="loading">
    <header><h3>{{ title }}</h3><span>{{ caption }}</span><slot name="actions" /><NRadioGroup v-if="trend" v-model:value="kind" size="small" aria-label="图表类型"><NRadioButton value="bar">柱状图</NRadioButton><NRadioButton value="line">折线图</NRadioButton></NRadioGroup></header>
    <div class="v2-chart-surface">
      <div ref="host" class="v2-chart-canvas" :class="{muted:loading||failed||!total}" role="img" :aria-hidden="loading||failed||!total" :aria-label="items.map(item=>`${item.label} ${item.value} 条`).join('，')"
        @pointerdown="scrub" @pointermove="move" @pointerup="release" @pointercancel="release" @pointerleave="release" />
      <span v-if="visible && kind==='line' && endPoint && !pressed && !loading && !failed && total" class="v2-chart-end" :style="{left:endPoint[0]+'px',top:endPoint[1]+'px'}" aria-hidden="true" />
      <div v-if="loading" class="v2-chart-overlay"><NSkeleton height="100%" :sharp="false" /></div>
      <NEmpty v-else-if="failed || engineFailed || !total" class="v2-chart-overlay" :description="failed ? '数据暂不可用' : engineFailed ? '图表暂不可用，可查看下方明细' : '暂无数据'" size="small" />
    </div>
    <div v-if="!loading&&!failed&&total" class="v2-chart-details" :class="{'show-legend':kind==='donut'&&narrow}" aria-label="图表明细">
      <NButton v-for="(item,index) in items" v-show="kind!=='donut'||item.value>0" :key="item.label" text size="small" @click="$emit('select',item,index)"><i v-if="kind==='donut'&&narrow" :style="{background:palette[index%palette.length]}" aria-hidden="true" />{{ item.label }} · {{ item.value }}</NButton>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NButton, NEmpty, NSkeleton, NRadioGroup, NRadioButton } from 'naive-ui';
import type { EChartsType } from 'echarts/core';
type Item={label:string;value:number;color?:string;[key:string]:unknown};
const props=defineProps<{title:string;caption?:string;items:Item[];loading?:boolean;failed?:boolean;trend?:boolean;view?:'bar'|'line'|'donut'}>();
const emit=defineEmits<{select:[item:Item,index:number]}>();
const host=ref<HTMLDivElement>(),kind=ref<'bar'|'line'|'donut'>(props.view||'bar'),pressed=ref(false),endPoint=ref<number[]|null>(null);
const engineFailed=ref(false),visible=ref(!document.hidden);
const narrow=ref(false),palette=['#168575','#548fa5','#bd8c49','#a1adb6','#667d80','#748d64'];
const total=computed(()=>props.items.reduce((sum,item)=>sum+item.value,0));
let chart:EChartsType|undefined,resize:ResizeObserver|undefined,observer:IntersectionObserver|undefined,disposed=false,frame=0;
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
const spring=(t:number)=>1+2.28*Math.pow(t-1,3)+1.28*Math.pow(t-1,2);
function update(){
  if(!chart||props.loading)return;
  if(kind.value==='donut'){
    chart.setOption({animation:!reduced.matches&&!document.hidden,animationDuration:650,animationDurationUpdate:500,tooltip:{trigger:'item',renderMode:'richText',confine:true},xAxis:{show:false},yAxis:{show:false},series:[{id:'records',type:'pie',radius:['48%','73%'],center:['50%','50%'],selectedMode:'single',selectedOffset:8,padAngle:3,itemStyle:{borderRadius:5},label:{formatter:'{b}\n{c}',fontSize:11,color:'#53605d'},labelLayout:{hideOverlap:true},emphasis:{scale:true,scaleSize:6},animationType:'expansion',animationEasing:'cubicOut',animationEasingUpdate:spring,universalTransition:true,data:props.items.map((item,i)=>({name:item.label,value:item.value,label:{show:!narrow.value&&item.value>0},itemStyle:{color:palette[i%palette.length]}}))}]},{replaceMerge:['series']});return;
  }
  const values=props.items.map(i=>i.value),maximum=Math.max(1,...values),line=kind.value==='line';
  chart.setOption({animation:!reduced.matches&&!document.hidden,animationDurationUpdate:500,animationEasingUpdate:spring,
    grid:{left:36,right:18,top:28,bottom:36},
    tooltip:{trigger:'axis',renderMode:'richText',confine:true,axisPointer:{type:line?'line':'shadow',snap:true},formatter:(input:any)=>{const p=Array.isArray(input)?input[0]:input;const item=props.items[p?.dataIndex];return item?`${item.label}\n${item.value} 条`:'';}},
    xAxis:{show:true,type:'category',data:props.items.map(i=>i.label),axisLine:{lineStyle:{color:'#d1d1d6'}},axisTick:{show:false},axisLabel:{color:'#636366',fontSize:11,hideOverlap:true}},
    yAxis:{show:true,type:'value',minInterval:1,splitLine:{lineStyle:{color:'#e5e5ea',type:'dashed'}},axisLabel:{color:'#636366',fontSize:11}},
    series:[{id:'records',type:line?'line':'bar',data:props.items.map((item,index)=>({id:'slot-'+index,name:'slot-'+index,value:item.value,itemStyle:{color:item.value===maximum?'#0071e3':item.color&&/^#[0-9a-f]{6}$/i.test(item.color)?item.color+'88':'#a9bacd'}})),
      barMaxWidth:40,barMinHeight:0,itemStyle:{borderRadius:[5,5,0,0]},
      label:{show:!line,position:'top',color:'#48484a',valueAnimation:true,formatter:'{c}'},
      animationDuration:line?1200:500,animationEasing:line?'cubicOut':spring,animationDelay:line?0:(i:number)=>Math.min(i,7)*80+100,
      animationDurationUpdate:500,animationEasingUpdate:spring,universalTransition:true,
      smooth:.25,showSymbol:false,symbolSize:8,lineStyle:{color:'#0071e3',width:2.5},
      areaStyle:line?{color:{type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{offset:0,color:'#0071e335'},{offset:1,color:'#0071e300'}]}}:undefined,
      emphasis:{focus:'series',itemStyle:{color:'#005bb5'},scale:true}}]}, {lazyUpdate:true,replaceMerge:['series']});
}
function updateEnd(){if(!chart||!props.items.length||kind.value!=='line')return;const i=props.items.length-1;endPoint.value=chart.convertToPixel({seriesIndex:0},[i,props.items[i]!.value]) as number[];}
async function initialize(){
  const {init}=await import('./chartEngine');
  if(disposed||!host.value)return;
  narrow.value=host.value.clientWidth<440;
  chart=init(host.value,undefined,{renderer:'svg'});chart.on('finished',updateEnd);
  chart.on('mousedown',(params:any)=>{const target=params.event?.target;if(reduced.matches||kind.value!=='bar'||target?.type!=='rect')return;const bounds=target.getBoundingRect();target.originY=bounds.y+bounds.height;target.animateTo({scaleY:.96},{duration:90,done:()=>{if(!disposed)target.animateTo({scaleY:1},{duration:220,easing:spring});}});});
  chart.on('click',(params:any)=>{if(props.loading||props.failed)return;const item=props.items[params.dataIndex];if(item)emit('select',item,params.dataIndex);});
  resize=new ResizeObserver(()=>{cancelAnimationFrame(frame);frame=requestAnimationFrame(()=>{chart?.resize();const small=(host.value?.clientWidth||0)<440;if(narrow.value!==small){narrow.value=small;update();}updateEnd();});});resize.observe(host.value);update();
}
function scrub(event:PointerEvent){if(kind.value!=='line'||!chart||props.loading||props.failed)return;pressed.value=true;host.value?.setPointerCapture?.(event.pointerId);move(event);}
function move(event:PointerEvent){if(!pressed.value||!chart||!host.value)return;const box=host.value.getBoundingClientRect();const point=chart.convertFromPixel({seriesIndex:0},[event.clientX-box.left,event.clientY-box.top]) as number[];if(!Array.isArray(point))return;const i=Math.max(0,Math.min(props.items.length-1,Math.round(point[0]||0)));chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:i});}
function release(){if(!pressed.value)return;pressed.value=false;chart?.dispatchAction({type:'hideTip'});}
function visibility(){visible.value=!document.hidden;update();}
watch(()=>[props.items,props.loading,props.failed,kind.value],update,{deep:true});
watch(()=>props.view,value=>{if(value)kind.value=value;});
onMounted(()=>{observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){observer?.disconnect();void initialize().catch(()=>{engineFailed.value=true;});}},{rootMargin:'150px'});if(host.value)observer.observe(host.value);reduced.addEventListener('change',update);document.addEventListener('visibilitychange',visibility);});
onBeforeUnmount(()=>{disposed=true;observer?.disconnect();resize?.disconnect();cancelAnimationFrame(frame);reduced.removeEventListener('change',update);document.removeEventListener('visibilitychange',visibility);chart?.dispose();});
</script>
<style scoped>
.v2-motion-chart{min-width:0}.v2-motion-chart header{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:10px}.v2-motion-chart h3{font-size:15px;font-weight:600;margin:0;color:#1d1d1f}.v2-motion-chart header>span{font-size:12px;color:#636366;margin-right:auto}.v2-chart-surface{position:relative;height:230px;min-width:0}.v2-chart-canvas{height:100%;width:100%;touch-action:pan-y;transition:opacity 150ms}.v2-chart-canvas.muted{opacity:0;pointer-events:none}.v2-chart-overlay{position:absolute;inset:12px;display:flex;align-items:center;justify-content:center}.v2-chart-overlay>.n-skeleton{width:100%}.v2-chart-details{display:flex;flex-wrap:wrap;gap:7px 15px}.v2-chart-details .n-button{font-size:12px}.v2-chart-end{position:absolute;width:8px;height:8px;background:#0071e3;border-radius:50%;transform:translate(-50%,-50%);pointer-events:none;animation:v2-end-pulse 1.8s ease-in-out infinite}@keyframes v2-end-pulse{50%{box-shadow:0 0 0 5px #0071e318}}@media(max-width:760px){.v2-chart-surface{height:210px}.v2-chart-details .n-button{min-height:32px}}@media(prefers-reduced-motion:reduce){.v2-chart-end{animation:none}.v2-chart-canvas{transition:none}}
</style>
<style scoped>
.v2-chart-details{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)}
.v2-chart-details:focus-within{position:static;width:auto;height:auto;overflow:visible;clip-path:none}
.v2-chart-details.show-legend{position:static;width:auto;height:auto;overflow:visible;clip-path:none;gap:4px 14px}.v2-chart-details.show-legend i{width:8px;height:8px;border-radius:2px;margin-right:6px}
@media(max-width:760px){.v2-chart-details .n-button{min-height:44px}}
</style>
