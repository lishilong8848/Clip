<template>
  <span class="v2-motion-progress" :class="{ 'is-complete': percent === 100, 'is-indeterminate': percent === null }" role="progressbar" aria-label="处理进度" :aria-valuemin="0" :aria-valuemax="100" :aria-valuenow="percent ?? undefined" :aria-valuetext="percent === null ? '进度待返回' : percent + '%'">
    <span class="v2-progress-track" aria-hidden="true"><span :style="{width: percent === null ? '30%' : percent + '%'}"></span></span>
    <output class="v2-progress-value" aria-hidden="true">{{percent === null ? '—' : percent + '%'}}</output>
  </span>
</template>
<script setup lang="ts">
import {computed} from 'vue';
const props=withDefaults(defineProps<{value?:number|string|null;max?:number|string}>(),{max:1});
const percent=computed(()=>{
  const max=Number(props.max),value=Number(props.value);
  if(props.value===undefined||props.value===null||props.value===''||!Number.isFinite(value)||!Number.isFinite(max)||max<=0)return null;
  return value>=max?100:Math.min(99,Math.round(Math.max(0,value/max)*100));
});
</script>
