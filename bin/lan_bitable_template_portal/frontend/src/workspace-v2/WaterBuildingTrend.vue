<template>
  <section class="water-building-trend" aria-label="本月楼栋耗水量" :aria-busy="loading">
    <header><h3>本月楼栋耗水量</h3><span>按天 · t{{ data.missing ? ' · 部分耗水量未取得' : '' }}</span></header>
    <NCheckboxGroup :value="visible" class="water-trend-legend" aria-label="显示楼栋曲线" @update:value="visible=$event.map(String)">
      <NCheckbox v-for="(row,index) in data.series" :key="row.scope" :value="row.scope"><i :style="{borderColor:colors[index%colors.length],borderTopStyle:styles[index%styles.length]}" aria-hidden="true" />{{ row.label }}</NCheckbox>
    </NCheckboxGroup>
    <div class="water-trend-surface">
      <div ref="host" class="v2-chart-canvas" role="img" :aria-label="shown.map(row=>row.label).join('、')+'每日耗水量曲线'" :style="{visibility:loading||failed||engineFailed?'hidden':'visible'}" />
      <NSkeleton v-if="loading" height="100%" class="water-trend-overlay" />
      <NEmpty v-else-if="failed||engineFailed||!hasValues" class="water-trend-overlay" :description="failed?'数据暂不可用':engineFailed?'图表暂不可用，请查看每日明细':!shown.length?'请选择楼栋':'本月暂无日统计数据'" size="small" />
    </div>
    <details v-if="!loading&&!failed&&shown.length" class="water-trend-details" :open="engineFailed">
      <summary>每日明细 <span>{{ data.days.length }} 天</span></summary>
      <div class="water-trend-table"><table><caption class="water-trend-sr">每日楼栋耗水量，单位吨；横线表示数据缺失</caption><thead><tr><th scope="col">日期</th><th v-for="row in shown" :key="row.scope" scope="col">{{ row.label }}</th></tr></thead><tbody><tr v-for="(day,index) in data.days" :key="day"><th scope="row"><NButton data-v2-navigation text size="small" @click="$emit('select',day)">{{day.slice(5)}}</NButton></th><td v-for="row in shown" :key="row.scope">{{waterNumberText(row.values[index],2)}}</td></tr></tbody></table></div>
    </details>
  </section>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NButton,NCheckbox,NCheckboxGroup,NEmpty,NSkeleton } from 'naive-ui';
import type { EChartsType } from 'echarts/core';
import type { Dict } from './client';
import { chartOption,chartPalette } from './vipboard';
import { nightMode } from './theme';
import { waterNumberText,type WaterBuildingTrend } from './water';
const props=defineProps<{data:WaterBuildingTrend;loading:boolean;failed:boolean}>();
const emit=defineEmits<{select:[day:string]}>();
const host=ref<HTMLDivElement>(),visible=ref<string[]>([]),engineFailed=ref(false);
const styles=['solid','dashed','dotted'] as const;
const colors=computed(()=>nightMode.value?['#65cdb3','#8fc3e4','#efbd70','#c3acee','#a5cf9b','#efacb7']:chartPalette);
const shown=computed(()=>props.data.series.filter(row=>visible.value.includes(row.scope)));
const hasValues=computed(()=>shown.value.some(row=>row.values.some(value=>value!==null)));
const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
let chart:EChartsType|undefined,resize:ResizeObserver|undefined,disposed=false,frame=0;
watch(()=>props.data.series.map(row=>row.scope).join(','),()=>{visible.value=props.data.series.map(row=>row.scope);},{immediate:true});
function update(){
  if(!chart||chart.isDisposed())return;
  try{
    chart.dispatchAction({type:'hideTip'});
    if(props.loading||props.failed||!hasValues.value){chart.clear();return;}
    const option=chartOption(props.data.days.map(label=>({label,value:0})),'line','t',(host.value?.clientWidth||0)<440,!reduced.matches,nightMode.value) as Dict;
    const base=option.series[0];
    option.yAxis={...option.yAxis,minInterval:undefined};
    option.xAxis={...option.xAxis,axisLabel:{...option.xAxis.axisLabel,formatter:(day:string)=>day.slice(5)}};
    option.series=shown.value.map(row=>{const index=props.data.series.findIndex(series=>series.scope===row.scope),color=colors.value[index%colors.value.length];return {...base,id:row.scope,name:row.label,data:row.values,connectNulls:false,showSymbol:row.values.filter(value=>value!==null).length<2,symbolSize:6,itemStyle:{color},lineStyle:{width:2.5,color,type:styles[index%styles.length]}};});
    option.tooltip={...option.tooltip,trigger:'axis',formatter:(params:any[])=>{const index=params[0]?.dataIndex;return index===undefined?'':[props.data.days[index],...shown.value.map(row=>row.label+'：'+waterNumberText(row.values[index],2)+' t')].join('\n');}};
    chart.setOption(option,{replaceMerge:['series','xAxis','yAxis','dataZoom'],lazyUpdate:false});engineFailed.value=false;
  }catch{engineFailed.value=true;}
}
watch(()=>[props.data,props.loading,props.failed,visible.value,nightMode.value],update,{deep:true});
onMounted(async()=>{
  reduced.addEventListener('change',update);
  try{const {init}=await import('./chartEngine');if(disposed||!host.value)return;chart=init(host.value,undefined,{renderer:'svg'});chart.on('click',(point:any)=>{const day=props.data.days[point.dataIndex];if(day&&!props.loading&&!props.failed)emit('select',day);});resize=new ResizeObserver(()=>{cancelAnimationFrame(frame);frame=requestAnimationFrame(()=>{if(disposed)return;chart?.resize();update();});});resize.observe(host.value);update();}catch{engineFailed.value=true;}
});
onBeforeUnmount(()=>{disposed=true;resize?.disconnect();cancelAnimationFrame(frame);reduced.removeEventListener('change',update);chart?.dispose();});
</script>
<style scoped>
.water-building-trend{min-width:0}.water-building-trend header{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.water-building-trend h3{font-size:15px;margin:0}.water-building-trend header>span{font-size:12px;color:var(--v2-muted)}.water-trend-legend{display:flex;gap:4px 14px;flex-wrap:wrap;margin:8px 0}.water-trend-legend .n-checkbox{min-height:36px;align-items:center}.water-trend-legend i{display:inline-block;width:16px;border-top:3px solid;vertical-align:middle;margin-right:5px}.water-trend-surface{height:280px;position:relative;min-width:0}.water-trend-surface .v2-chart-canvas{height:100%;width:100%;touch-action:pan-y}.water-trend-overlay{position:absolute;inset:12px;display:flex;align-items:center;justify-content:center}.water-trend-details{border-top:1px solid var(--v2-border);font-size:12px;margin-top:8px}.water-trend-details summary{padding:12px 0;cursor:pointer}.water-trend-details summary>span{float:right;color:var(--v2-muted)}.water-trend-table{overflow-x:auto}.water-trend-table table{border-collapse:collapse;min-width:100%;font-variant-numeric:tabular-nums}.water-trend-table th,.water-trend-table td{text-align:right;padding:8px;border-bottom:1px solid var(--v2-border);white-space:nowrap}.water-trend-table th:first-child{text-align:left}.water-trend-sr{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}@media(max-width:760px){.water-trend-surface{height:260px}.water-trend-legend .n-checkbox{min-height:44px}.water-trend-table th,.water-trend-table td{padding:8px 5px}}
</style>
