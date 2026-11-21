<template>
  <nav class="v2-mobile-nav" aria-label="手机导航" data-v2-navigation :style="{gridTemplateColumns:`repeat(${tabs.length},minmax(0,1fr))`}">
    <button v-for="tab in tabs" :key="tab.value" type="button" :aria-current="active===tab.value?'page':undefined" @click="select(tab.value)">
      <span class="mobile-nav-icon"><component :is="tab.icon" :size="19" aria-hidden="true" /></span>
      <span>{{ tab.label }}</span>
    </button>
  </nav>
</template>
<script setup lang="ts">
import type { Component } from 'vue';
const props=defineProps<{tabs:{value:string;label:string;icon:Component}[];active:string}>();
const emit=defineEmits<{select:[value:string];direction:[value:number]}>();
function select(value:string){
  if(value!==props.active && !matchMedia('(prefers-reduced-motion: reduce)').matches){
    const index=(key:string)=>props.tabs.findIndex(tab=>tab.value===key);
    emit('direction',index(value)>index(props.active)?1:-1);
  }
  emit('select',value);
}
</script>
