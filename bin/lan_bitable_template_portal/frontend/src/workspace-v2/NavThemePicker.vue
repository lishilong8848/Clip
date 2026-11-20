<template>
  <NDropdown placement="bottom-end" trigger="click" :options="options" @select="changePalette">
    <NButton quaternary class="v2-mode-button" aria-label="主题配色" title="主题配色"><Palette :size="18" aria-hidden="true" /></NButton>
  </NDropdown>
  <NButton v-if="!paletteOnly" quaternary class="v2-mode-button" :aria-pressed="nightMode" aria-label="夜间模式"
    :title="nightMode?'切换浅色外观':'切换深色外观'" @click="toggle">
    <component :is="nightMode ? Sun : Moon" :size="18" aria-hidden="true" />
    <span v-if="label" class="v2-rail-label">{{ nightMode ? '浅色外观' : '深色外观' }}</span>
  </NButton>
</template>
<script setup lang="ts">
import {computed,h} from 'vue';
import {NButton,NDropdown,useMessage} from 'naive-ui';
import {Moon,Palette,Sun} from 'lucide-vue-next';
import {nightMode,paletteId,palettes,saveNightMode,savePalette,type PaletteId} from './theme';
const props=defineProps<{userId:string;label?:boolean;paletteOnly?:boolean}>();
const message=useMessage();
const labels={mineral:'松绿',ocean:'海蓝',violet:'暮紫'};
const options=computed(()=>Object.entries(labels).map(([key,label])=>({
  key,label:paletteId.value===key?label+' · 已选':label,
  icon:()=>h('span',{style:{width:'13px',height:'13px',borderRadius:'50%',background:palettes[key as PaletteId][nightMode.value?'dark':'light'].accent}}),
  props:paletteId.value===key?{'aria-current':true}:undefined,
})));
function changePalette(key:PaletteId){
  if(!savePalette(props.userId,key))message.warning('当前配色已切换，浏览器未能保存偏好。');
}
function toggle(){
  if(!saveNightMode(props.userId,!nightMode.value))message.warning('当前外观已切换，浏览器未能保存偏好。');
}
</script>
