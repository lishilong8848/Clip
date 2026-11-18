<template>
  <div ref="host" v-show="!failed" class="weather-trend" role="img" :aria-label="description" />
  <p v-if="failed" class="weather-trend-fallback">{{ description }}</p>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import type { EChartsType } from 'echarts/core';
import { activeColors } from '../theme';

const props=defineProps<{points:ReadonlyArray<readonly [time:number, temperature:number]>}>();
const host=ref<HTMLElement>();
const failed=ref(false);
const clockFormat=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const dateFormat=new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
const description=computed(()=>props.points.map(([time,temp])=>dateFormat.format(time)+' '+temp+'°C').join('，'));
let chart:EChartsType|undefined;
let observer:ResizeObserver|undefined;
let disposed=false;

function draw(){
  if(!chart)return;
  if(!props.points.length){chart.clear();return;}
  const {accentText,muted,border,surface,text}=activeColors.value;
  const readings=props.points.map(([,temperature])=>temperature);
  const start=props.points[0]![0], end=props.points[props.points.length-1]![0];
  chart.setOption({
    animation:false,
    grid:{left:30,right:18,top:9,bottom:24},
    tooltip:{trigger:'axis',renderMode:'richText',confine:true,backgroundColor:surface,borderColor:border,
      textStyle:{color:text,fontSize:12},
      formatter:(entries:unknown)=>{
        const entry=Array.isArray(entries)?entries[0]:entries;
        const point=entry?.data;
        return Array.isArray(point)?dateFormat.format(point[0])+'  '+point[1]+'°C':'';
      }},
    xAxis:{type:'time',min:start===end?start-1800000:start,max:start===end?end+1800000:end,splitNumber:3,
      minInterval:3600000,axisLine:{show:false},axisTick:{show:false},splitLine:{show:false},
      axisLabel:{color:muted,fontSize:10,hideOverlap:true,formatter:(time:number)=>clockFormat.format(time)}},
    yAxis:{type:'value',min:Math.floor(Math.min(...readings))-1,max:Math.ceil(Math.max(...readings))+1,splitNumber:2,
      axisLabel:{color:muted,fontSize:10,formatter:'{value}°'},splitLine:{lineStyle:{color:border,opacity:0.55}}},
    series:[{type:'line',data:props.points.map(([time,temp])=>[time,temp]),symbol:'circle',symbolSize:5,showSymbol:true,
      lineStyle:{color:accentText,width:1.8},itemStyle:{color:accentText},clip:false}],
  },{notMerge:true});
}

watch([()=>props.points,activeColors],draw);
onMounted(async()=>{
  try {
    const {init}=await import('../chartEngine');
    if(disposed||!host.value)return;
    chart=init(host.value,undefined,{renderer:'svg'});
    draw();
    observer=new ResizeObserver(()=>chart?.resize());
    observer.observe(host.value);
  } catch {
    chart?.dispose();
    chart=undefined;
    failed.value=true;
  }
});
onBeforeUnmount(()=>{
  disposed=true;
  observer?.disconnect();
  chart?.dispose();
});
</script>

<style scoped>
.weather-trend { width:100%;height:90px;min-width:0;overflow:hidden; }
.weather-trend-fallback { color:var(--wb-muted);font-size:12px;padding:7px 0; }
</style>
