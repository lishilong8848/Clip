<template>
  <div ref="host" class="v2-widget-body v2-responsive-body" :inert="editing" :data-capacity="size.compact?'compact':'wide'" :style="{'--widget-width':size.width+'px','--widget-height':size.height+'px','--widget-rows':size.rows}">
    <slot />
  </div>
</template>
<script setup lang="ts">
import { computed,onBeforeUnmount,onMounted,provide,ref } from 'vue';
import { capacity,capacityContext } from './pageLayout';
defineProps<{editing?:boolean}>();
const host=ref<HTMLElement>(),size=ref(capacity(800,480));
provide(capacityContext,computed(()=>size.value));
let resize:ResizeObserver|undefined;
// Business components own their rows, columns and pagination; the viewport only measures layout.
onMounted(()=>{
  if(!host.value)return;
  resize=new ResizeObserver(([entry])=>{
    if(!entry)return;
    const next=capacity(Math.round(entry.contentRect.width),Math.round(entry.contentRect.height));
    if(next.width!==size.value.width||next.height!==size.value.height)size.value=next;
  });
  resize.observe(host.value);
});
onBeforeUnmount(()=>resize?.disconnect());
</script>
