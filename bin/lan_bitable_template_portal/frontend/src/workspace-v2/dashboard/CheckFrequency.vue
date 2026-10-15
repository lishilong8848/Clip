<template>
  <div class="v2-check-frequency" data-v2-navigation>
    <label>执行频率<NSelect :value="modelValue.kind" :options="kinds" :disabled="disabled" aria-label="执行频率" @update:value="changeKind" /></label>
    <label>生效日期<NDatePicker :formatted-value="modelValue.start_date" value-format="yyyy-MM-dd" type="date" :disabled="disabled" :clearable="false" aria-label="生效日期" @update:formatted-value="value=>patch({start_date:value})" /></label>
    <template v-if="modelValue.kind==='temporary'">
      <label>临时重复<NSelect :value="modelValue.cadence" :options="[{value:'daily',label:'每天一次'},{value:'shift',label:'每天每班'}]" :disabled="disabled" @update:value="changeCadence" /></label>
      <label>结束条件<NSelect :value="modelValue.limit_type" :options="[{value:'occurrences',label:'执行指定次数'},{value:'days',label:'持续指定天数'}]" :disabled="disabled" @update:value="value=>patch({limit_type:value})" /></label>
      <label>{{modelValue.limit_type==='days'?'持续天数':'执行次数'}}<NInputNumber :value="modelValue.limit" :min="1" :max="366" :precision="0" :disabled="disabled" @update:value="value=>patch({limit:value})" /></label>
    </template>
    <label v-if="modelValue.kind==='weekly'">每周<NSelect :value="modelValue.weekday" :options="weekdays" :disabled="disabled" @update:value="value=>patch({weekday:value})" /></label>
    <label v-if="modelValue.kind==='interval'">间隔天数<NInputNumber :value="modelValue.every_days" :min="1" :max="3660" :precision="0" :disabled="disabled" @update:value="value=>patch({every_days:value})" /></label>
    <template v-if="isShift">
      <label>白班时间<NTimePicker :formatted-value="modelValue.day_time" format="HH:mm" value-format="HH:mm" :disabled="disabled" :clearable="false" @update:formatted-value="value=>patch({day_time:value})" /></label>
      <label>夜班时间<NTimePicker :formatted-value="modelValue.night_time" format="HH:mm" value-format="HH:mm" :disabled="disabled" :clearable="false" @update:formatted-value="value=>patch({night_time:value})" /></label>
    </template>
    <label v-else>执行时间<NTimePicker :formatted-value="modelValue.time" format="HH:mm" value-format="HH:mm" :disabled="disabled" :clearable="false" @update:formatted-value="value=>patch({time:value})" /></label>
  </div>
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {NSelect,NDatePicker,NTimePicker,NInputNumber} from 'naive-ui';
import type {Dict} from '../client';
const props=defineProps<{modelValue:Dict;disabled:boolean}>();
const emit=defineEmits<{'update:modelValue':[value:Dict]}>();
const kinds=[{value:'shift',label:'每日每班组'},{value:'daily',label:'每日'},{value:'weekly',label:'每周'},{value:'month_start',label:'每月月初'},{value:'month_end',label:'每月月底'},{value:'interval',label:'每隔 X 天'},{value:'temporary',label:'一次性 / 临时'}];
const weekdays=['星期一','星期二','星期三','星期四','星期五','星期六','星期日'].map((label,value)=>({label,value}));
const isShift=computed(()=>props.modelValue.kind==='shift'||props.modelValue.kind==='temporary'&&props.modelValue.cadence==='shift');
function patch(values:Dict){emit('update:modelValue',{...props.modelValue,...values});}
function changeKind(kind:string){const value:Dict={kind,start_date:props.modelValue.start_date};if(kind==='shift')Object.assign(value,{day_time:'08:50',night_time:'17:50'});else value.time='08:50';if(kind==='weekly')value.weekday=0;if(kind==='interval')value.every_days=2;if(kind==='temporary')Object.assign(value,{cadence:'daily',limit_type:'occurrences',limit:1});emit('update:modelValue',value);}
function changeCadence(cadence:string){const value:Dict={...props.modelValue,cadence};delete value.time;delete value.day_time;delete value.night_time;Object.assign(value,cadence==='shift'?{day_time:'08:50',night_time:'17:50'}:{time:'08:50'});emit('update:modelValue',value);}
</script>
<style>
.v2-check-frequency{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:10px 0 0 28px}.v2-check-frequency>label{display:grid;gap:4px;font-size:12px;color:var(--v2-muted);min-width:0}.v2-check-frequency .n-date-picker,.v2-check-frequency .n-time-picker{width:100%}@media(max-width:600px){.v2-check-frequency{grid-template-columns:repeat(2,minmax(0,1fr));margin-left:0}}
</style>
