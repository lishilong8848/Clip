<template><LegacyBot v-bind="$attrs" :appearance="shown" :position="placed" :viewport-top="shown.shape==='weather-lighthouse-mini'?12-shown.size*.2:12" @moved="$emit('moved',$event)" /></template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import LegacyBot from '../components/LighthouseBot.vue';
import type { BotAppearance } from '../botAppearance';
const props=defineProps<{appearance:BotAppearance;position?:{x:number;y:number}|null}>();
defineEmits<{moved:[position:{x:number;y:number}]}>();
const width=ref(window.innerWidth),height=ref(window.innerHeight);
const shown=computed(()=>width.value<=760?{...props.appearance,size:Math.min(props.appearance.size,64)}:props.appearance);
const placed=computed(()=>width.value<=760?{x:Math.min(props.position?.x??width.value-shown.value.size-12,width.value-shown.value.size-12),y:Math.min(props.position?.y??height.value-shown.value.size-80,height.value-shown.value.size-80)}:props.position);
function resize(){width.value=window.innerWidth;height.value=window.innerHeight;}
onMounted(()=>window.addEventListener('resize',resize));onBeforeUnmount(()=>window.removeEventListener('resize',resize));
</script>
