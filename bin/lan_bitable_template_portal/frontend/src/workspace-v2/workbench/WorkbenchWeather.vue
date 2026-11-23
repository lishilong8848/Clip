<script setup lang="ts">
import { computed, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from 'vue';
import { NPopover } from 'naive-ui';
import { Cloud, CloudRain, CloudSnow, CloudSun, Sun, X } from 'lucide-vue-next';
import { requestJson } from '../client';
import { todoTimestamp } from '../businessTodos';

const props=defineProps<{clock?:number; identity?:string}>();
const open=ref(false);
const forecast=ref<HTMLElement>();
const now=ref(Date.now());
const clock=computed(()=>props.clock??now.value);
const zone='Asia/Shanghai';
const dayKey=(stamp:number)=>new Date(stamp+8*3600000).toISOString().slice(0,10);
const dayTime=(stamp:number)=>new Intl.DateTimeFormat('zh-CN',{timeZone:zone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(stamp);
const today=computed(()=>dayKey(clock.value));
const weather = ref<{temp:number|null;humidity:number|null;description:string;minimum:number|null;maximum:number|null;
  observedAt?:number;hourly:{stamp:number;temp:number}[]}>();
const weatherBusy = ref(false);
const condition=(description:string)=>description.includes('雪')?'snow':/雨|雷/.test(description)?'rain':
  /多云|晴间/.test(description)?'partly':/晴/.test(description)?'sun':'cloud';
const icons={snow:CloudSnow,rain:CloudRain,partly:CloudSun,sun:Sun,cloud:Cloud};
const currentCondition=computed(()=>condition(weather.value?.description||''));
const weatherHour=3600000;
const firstForecastHour=computed(()=>Math.floor(clock.value/weatherHour)*weatherHour+weatherHour);
const weatherTime=(stamp:number)=> (dayKey(stamp)===today.value?'':'次日 ')+dayTime(stamp);
const hourForecast=computed(()=>weather.value?.hourly.find(point=>point.stamp>=firstForecastHour.value-weatherHour&&point.stamp<=firstForecastHour.value));
const weatherTemperature=computed(()=>{
  const current=weather.value;
  const temp=current?.temp??hourForecast.value?.temp;
  if(temp!=null)return temp+'°C';
  return current?.minimum!=null&&current.maximum!=null?current.minimum+'–'+current.maximum+'°C':'';
});
const weatherCaption=computed(()=>{
  const current=weather.value;
  if(!current)return '';
  const time=current.temp!=null&&current.observedAt?dayTime(current.observedAt)+' 实况'
    :current.temp==null&&hourForecast.value?weatherTime(hourForecast.value.stamp)+' 预报'
    :current.temp==null&&weatherTemperature.value?'今日预报':'';
  return [time,current.description,current.humidity!=null?'湿度 '+current.humidity+'%':''].filter(Boolean).join(' · ');
});
const weatherTrend=computed(()=>{
  if(!weather.value)return;
  const start=firstForecastHour.value, end=start+9*weatherHour;
  const samples=weather.value.hourly.filter(point=>point.stamp>=start&&point.stamp<=end&&point.stamp%weatherHour===0);
  if(!samples.length)return;
  const min=Math.min(...samples.map(point=>point.temp)),max=Math.max(...samples.map(point=>point.temp));
  const readings=samples.map(({stamp,temp})=>({stamp,temp,
    x:3+138*(stamp-start)/(end-start),y:25-20*(temp-min+1)/(max-min+2)}));
  const runs:(typeof readings)[]=[];
  for(const point of readings){
    const run=runs[runs.length-1];
    if(run&&point.stamp-run[run.length-1].stamp===weatherHour)run.push(point);
    else runs.push([point]);
  }
  const segments=runs.filter(run=>run.length>1).map(run=>{
    const points=run.map(({x,y})=>`${x},${y}`).join(' ');
    return {points,fill:`${run[0].x},28 ${points} ${run[run.length-1].x},28`};
  });
  return {readings,segments,start:weatherTime(start),end:dayKey(start)===dayKey(end)?dayTime(end):weatherTime(end)};
});
const weatherStamp=ref<number>();
const weatherReading=computed(()=>weatherTrend.value?.readings.find(point=>point.stamp===weatherStamp.value));
function showWeatherPoint(event:PointerEvent) {
  const rect=(event.currentTarget as HTMLElement).getBoundingClientRect();
  const x=144*(event.clientX-rect.left)/rect.width;
  const readings=weatherTrend.value?.readings;
  if(readings?.length)weatherStamp.value=readings.reduce((near,point)=>Math.abs(point.x-x)<Math.abs(near.x-x)?point:near).stamp;
}
function stepWeatherPoint(step:number) {
  const readings=weatherTrend.value?.readings;
  if(!readings?.length)return;
  const index=readings.findIndex(point=>point.stamp===weatherStamp.value);
  weatherStamp.value=readings[Math.min(readings.length-1,Math.max(0,index+step))].stamp;
}
let weatherAbort:AbortController|undefined;
let weatherAt=0;
async function readWeather() {
  if (weatherBusy.value&&!weatherAbort?.signal.aborted) return;
  weatherBusy.value=true;
  const controller=weatherAbort=new AbortController();
  try {
    const data=await requestJson('/api/assistant/icon-weather?detail=1',{signal:controller.signal,timeoutMs:14000});
    if(controller.signal.aborted)return;
    if(data.available!==true||data.date!==today.value||typeof data.description!=='string')return;
    const temperature=(value:unknown)=>typeof value==='number'&&Number.isFinite(value)?Math.round(value*10)/10:null;
    const hourly=new Map<number,number>();
    if(Array.isArray(data.hourly)) for(const point of data.hourly){
      const stamp=todoTimestamp(point?.time),temp=temperature(point?.temp);
      if(stamp!==undefined&&temp!==null)hourly.set(stamp,temp);
    }
    const temp=temperature(data.temperature);
    const description=data.current_description||data.description||weather.value?.description||'';
    const forecast=hourly.size?[...hourly].sort(([a],[b])=>a-b).map(([stamp,temp])=>({stamp,temp})):weather.value?.hourly||[];
    weather.value={temp,humidity:temperature(data.humidity),description,
      minimum:temperature(data.minimum),maximum:temperature(data.maximum),observedAt:todoTimestamp(data.observed_at),hourly:forecast};
  } catch {
    // Keep verified readings until the date changes.
  } finally {
    if(weatherAbort===controller){weatherAt=Date.now();weatherBusy.value=false;}
  }
}
watch(clock,()=>{if(!document.hidden&&Date.now()-weatherAt>(weatherTrend.value?.readings.length===10?600000:60000))void readWeather();});
watch(open,(shown,_previous,cleanup)=>{
  if(!shown)return;
  now.value=Date.now();
  let frame=0;
  const keepInView=()=>{
    cancelAnimationFrame(frame);
    frame=requestAnimationFrame(()=>{
      const panel=forecast.value?.closest<HTMLElement>('.v-binder-follower-content');
      if(!panel)return;
      panel.style.translate='';
      const {left,right}=panel.getBoundingClientRect();
      const width=document.documentElement.clientWidth;
      const offset=Math.max(8-left,0)-Math.max(right-width+8,0);
      if(offset)panel.style.translate=`${offset}px`;
    });
  };
  keepInView();
  window.addEventListener('resize',keepInView);
  cleanup(()=>{cancelAnimationFrame(frame);window.removeEventListener('resize',keepInView);});
},{flush:'post'});
watch([today,()=>props.identity],()=>{open.value=false;weatherAbort?.abort();weather.value=undefined;weatherAt=0;void readWeather();});
let timer:ReturnType<typeof setInterval>|undefined;
onMounted(()=>{if(props.clock===undefined)timer=setInterval(()=>{now.value=Date.now();},60000);void readWeather();});
onDeactivated(()=>{open.value=false;});
onBeforeUnmount(()=>{clearInterval(timer);weatherAbort?.abort();});
</script>

<template>
  <NPopover v-model:show="open" trigger="click" placement="bottom-end" :animated="false" :show-arrow="false" raw>
    <template #trigger>
      <button class="weather-key" type="button" aria-label="查看今日天气和未来10小时气温" :aria-expanded="open" aria-haspopup="dialog">
        <component :is="icons[currentCondition]" :size="18" aria-hidden="true"/>
        <span>{{ [weather?.description,weatherTemperature].filter(Boolean).join(' ') || (weatherBusy?'天气读取中…':'今日天气') }}</span>
      </button>
    </template>
    <section ref="forecast" class="workbench-forecast" role="dialog" aria-label="今日天气和未来10小时气温" @keydown.esc.stop="open=false">
      <header class="forecast-head"><div><strong>{{ weatherTemperature || '今日天气' }}</strong>
        <p v-if="weatherCaption">{{ weatherCaption }}</p></div>
        <button type="button" aria-label="关闭天气" @click="open=false"><X :size="16" aria-hidden="true"/></button>
      </header>
        <div v-if="weatherTrend" class="weather-chart">
          <div class="weather-plot" role="group" tabindex="0" aria-label="未来 10 小时气温趋势，使用左右方向键查看预报"
            @pointermove="$event.pointerType!=='touch' && showWeatherPoint($event)" @pointerdown="showWeatherPoint"
            @pointerleave="$event.pointerType!=='touch' && (weatherStamp=undefined)" @blur="weatherStamp=undefined"
            @focus="weatherStamp===undefined && stepWeatherPoint(1)" @keydown.left.prevent="stepWeatherPoint(-1)"
            @keydown.right.prevent="stepWeatherPoint(1)" @keydown.esc="weatherStamp=undefined">
            <svg viewBox="0 0 144 28" preserveAspectRatio="none" aria-hidden="true">
              <g v-for="segment in weatherTrend.segments" :key="segment.points">
                <polygon :points="segment.fill" fill="currentColor" opacity=".13"/>
                <polyline :points="segment.points" fill="none" stroke="currentColor" stroke-width="2" vector-effect="non-scaling-stroke" stroke-linejoin="round" stroke-linecap="round"/>
              </g>
            </svg>
            <span v-for="point in weatherTrend.readings" :key="point.stamp" class="weather-point" aria-hidden="true"
              :class="{active:point.stamp===weatherStamp}" :style="{left:point.x/144*100+'%',top:point.y/28*100+'%'}"></span>
            <output v-if="weatherReading" class="weather-reading" :style="{'--sample-x':weatherReading.x/144*100+'%'}">{{ weatherTime(weatherReading.stamp) }} · {{ weatherReading.temp }}°C</output>
          </div>
          <div class="weather-axis"><span>{{ weatherTrend.start }}</span><span>未来 10 小时</span><span>{{ weatherTrend.end }}</span></div>
        </div>
        <p v-else class="forecast-note">{{ weatherBusy?"正在读取小时预报…":"暂无小时预报" }} <button v-if="!weatherBusy" type="button" @click="readWeather">重试</button></p>
    </section>
  </NPopover>
</template>

<style scoped>
.weather-key,.forecast-head button,.forecast-note button{display:inline-flex;align-items:center;justify-content:center;gap:5px;
  min-height:36px;padding:4px 8px;border:1px solid var(--v2-border);border-radius:8px;background:var(--soft-face);
  color:var(--v2-palette-accent-text);font:inherit;cursor:pointer}
.weather-key{font-size:.83rem;white-space:nowrap;max-width:148px}
.weather-key span{overflow:hidden;text-overflow:ellipsis}.weather-key svg{flex:none}
.workbench-forecast{width:min(340px,calc(100vw - 24px));padding:12px;border:1px solid var(--v2-border);
  border-radius:12px;background:var(--soft-face,#fff);color:var(--v2-ink);box-shadow:var(--soft-key);font:inherit}
.forecast-head,.weather-axis{display:flex;justify-content:space-between;gap:8px;align-items:center}
.forecast-head p,.forecast-note{margin:4px 0;color:var(--v2-muted);font-size:.8rem}
.forecast-head button{min-height:32px}
.weather-chart{padding:10px 4px 0;color:var(--v2-palette-accent-text);min-width:0}
.weather-plot{position:relative;height:62px;cursor:crosshair;touch-action:pan-y}
.weather-plot svg{display:block;width:100%;height:100%}
.weather-point{position:absolute;transform:translate(-50%,-50%);width:6px;height:6px;border:1px solid currentColor;border-radius:50%;background:var(--soft-face,#fff)}
.weather-point.active{width:9px;height:9px;background:currentColor}
.weather-axis{color:var(--v2-muted);font-size:.74rem;font-variant-numeric:tabular-nums;margin-top:6px}
.weather-reading{position:absolute;left:clamp(58px,var(--sample-x),calc(100% - 58px));top:0;transform:translate(-50%,-75%);
  background:var(--soft-face,#fff);box-shadow:var(--soft-key);border:1px solid var(--v2-border);border-radius:5px;padding:2px 5px;font-size:.75rem;white-space:nowrap;pointer-events:none}
@media(pointer:coarse){.weather-key,.forecast-head button,.forecast-note button{min-height:44px}}
</style>
