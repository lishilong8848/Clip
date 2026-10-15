<template>
  <div class="v2-event-charts">
    <MotionChart title="事件分布" :caption="range.label + ' · 件'" unit="件" :items="dimension==='date'?trend:levels" :loading="loading" :failed="failed" :trend="dimension==='date'" :view="dimension==='date'?'bar':'donut'"
      @select="item=>dimension==='date'?$emit('date',Number(item.start),Number(item.end),item.label):$emit('level',item.label)">
      <template #actions><NRadioGroup data-v2-filter v-model:value="dimension" size="small" aria-label="事件统计维度">
        <NRadioButton value="date">{{ (range.end-range.start)/86400000>62?'按时段':'按天' }}</NRadioButton><NRadioButton value="level">按等级</NRadioButton>
      </NRadioGroup></template>
    </MotionChart>
  </div>
</template>
<script setup lang="ts">
import { computed, ref } from 'vue';
import { NRadioGroup, NRadioButton } from 'naive-ui';
import MotionChart from './MotionChart.vue';
import { eventTrend, type EventRange } from './eventPeriod';
import type { LooseDict } from '../types';
const props=defineProps<{records:LooseDict[];range:EventRange;loading:boolean;failed:boolean}>();
defineEmits<{date:[start:number,end:number,label:string];level:[value:string]}>();
const dimension=ref<'date'|'level'>('date');
const trend=computed(()=>eventTrend(props.records,props.range));
const levels=computed(()=>{
  const groups=new Map<string,number>();
  for(const item of props.records){const label=String(item.level||'未填写等级').trim()||'未填写等级';groups.set(label,(groups.get(label)||0)+1);}
  const colors=['#2476aa','#168879','#a77319','#bb5260','#7c8797'];
  return [...groups].sort(([a],[b])=>a.localeCompare(b)).map(([label,value],i)=>({label,value,color:colors[i%colors.length]}));
});
</script>
<style scoped>
.v2-event-charts { display:grid; gap:12px; padding:20px 0; border-bottom:1px solid var(--v2-border); }
</style>
