<template>
  <div class="repair-text-preview">
    <NPopover trigger="hover" :disabled="!overflow || expanded" placement="top-start">
      <template #trigger>
        <button :id="triggerId" type="button" class="repair-preview-text" :class="{expanded}"
          :tabindex="overflow || expanded ? 0 : -1" :aria-expanded="overflow || expanded ? expanded : undefined"
          :aria-disabled="!overflow && !expanded"
          @click="toggle"><span ref="text">{{ value || '未填写' }}</span></button>
      </template>
      <div class="repair-preview-popover">{{ value }}</div>
    </NPopover>
  </div>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NPopover } from 'naive-ui';

const props=defineProps<{value?:string;triggerId?:string}>();
const text=ref<HTMLElement>();
const expanded=ref(false),overflow=ref(false);
function measure(){
  if(text.value&&!expanded.value)overflow.value=text.value.scrollHeight>text.value.clientHeight+1;
}
function toggle(){
  if(expanded.value||overflow.value)expanded.value=!expanded.value;
}
let observer:ResizeObserver|undefined;
onMounted(()=>{
  if(text.value){observer=new ResizeObserver(measure);observer.observe(text.value);}
  measure();
});
watch(()=>props.value,async()=>{expanded.value=false;await nextTick();measure();});
onBeforeUnmount(()=>observer?.disconnect());
</script>

<style scoped>
.repair-text-preview{width:100%;min-width:0}
.repair-preview-text{display:block;width:100%;min-width:0;border:0;border-radius:6px;padding:6px 8px;text-align:start;
  font:inherit;font-size:var(--cf-font-body);line-height:1.5;color:inherit;background:var(--repair-readonly-bg,var(--cf-surface-soft))}
.repair-preview-text[aria-expanded]{cursor:pointer}
.repair-preview-text span,.repair-preview-popover{white-space:pre-wrap;overflow-wrap:anywhere}
.repair-preview-text:not(.expanded) span{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden}
.repair-preview-popover{max-inline-size:min(34rem,85vw);max-block-size:45dvh;overflow:auto;overscroll-behavior:contain;font-size:var(--cf-font-body)}
</style>
