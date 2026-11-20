<template>
  <NButton quaternary class="v2-mode-button" :aria-pressed="nightMode" aria-label="夜间模式"
    :title="nightMode?'切换浅色外观':'切换深色外观'" @click="toggle">
    <component :is="nightMode ? Sun : Moon" :size="18" aria-hidden="true" />
    <span v-if="label" class="v2-rail-label">{{ nightMode ? '浅色外观' : '深色外观' }}</span>
  </NButton>
</template>
<script setup lang="ts">
import {NButton,useMessage} from 'naive-ui';
import {Moon,Sun} from 'lucide-vue-next';
import {nightMode,saveNightMode} from './theme';
const props=defineProps<{userId:string;label?:boolean}>();
const message=useMessage();
function toggle(){
  if(!saveNightMode(props.userId,!nightMode.value))message.warning('当前外观已切换，浏览器未能保存偏好。');
}
</script>
