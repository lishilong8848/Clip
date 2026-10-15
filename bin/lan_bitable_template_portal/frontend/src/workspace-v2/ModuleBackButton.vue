<template>
  <NButton v-if="active&&visible" data-v2-navigation quaternary :disabled="disabled" :title="title||'返回模块上一层'" aria-label="返回" @click="back"><ArrowLeft :size="16" /><slot>返回</slot></NButton>
</template>
<script setup lang="ts">
import {computed,onActivated,onDeactivated,onMounted,onBeforeUnmount,ref} from 'vue';
import {NButton} from 'naive-ui';
import {ArrowLeft} from 'lucide-vue-next';
import {logicalUrl} from './version';
import {moduleKey,moduleBackTarget,navigateBack} from './navigation';
const props=defineProps<{to?:string;hard?:boolean;disabled?:boolean;title?:string;withinModule?:boolean}>();
const emit=defineEmits<{click:[]}>();
const active=ref(true),revision=ref(0);
onActivated(()=>active.value=true);onDeactivated(()=>active.value=false);
function update(){revision.value++;}onMounted(()=>window.addEventListener('popstate',update));onBeforeUnmount(()=>window.removeEventListener('popstate',update));
const visible=computed(()=>{void revision.value;if(!props.to)return props.withinModule===true;const current=logicalUrl(),target=logicalUrl(new URL(props.to,current.origin).href);return target.origin===current.origin&&moduleKey(target)===moduleKey(current)&&!!moduleBackTarget(props.to,current);});
function back(){if(props.disabled)return;if(props.to)navigateBack(props.to,props.hard);else emit('click');}
</script>
