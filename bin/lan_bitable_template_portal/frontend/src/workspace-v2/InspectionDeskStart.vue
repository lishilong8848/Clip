<script setup lang="ts">
import {ref,watch} from 'vue';
import InspectionDeskDialog from './InspectionDeskDialog.vue';
const p=defineProps<{label:string;duty:string;round:number;options:string[];modelValue:string[];disabled:boolean;target?:HTMLElement}>();
const emit=defineEmits<{'update:modelValue':[string[]];start:[]}>();
const show=ref(false);
watch(()=>[p.duty,p.round,p.disabled],()=>{show.value=false});
function choose(name:string,checked:boolean){
  emit('update:modelValue',checked?[...new Set([...p.modelValue,name])]:p.modelValue.filter(p=>p!==name));
}
</script>
<template><button class="ins-primary" :disabled="disabled" @click="show=true">{{ label }}</button>
  <InspectionDeskDialog :show="show" title="开始巡检" :target="target" @close="show=false">
    <div class="ins-head"><strong>{{ duty }} · 第 {{ round }} 轮</strong></div>
    <fieldset class="ins-options"><legend>本轮巡检专业</legend><label v-for="name in options" :key="name"><input type="checkbox" :checked="modelValue.includes(name)" @change="choose(name,($event.target as HTMLInputElement).checked)"/>{{ name }}</label></fieldset>
    <div class="ins-head"><p>开始后锁定本轮采集数据。</p><button class="ins-primary" :disabled="disabled || !modelValue.length" @click="show=false;emit('start')">确认开始</button></div>
  </InspectionDeskDialog></template>