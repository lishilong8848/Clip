<template>
  <span class="v2-template-preview" :class="'preview-'+kind" aria-hidden="true">
    <span v-if="kind==='donut'||kind==='pie'" class="preview-ring" />
    <span v-else class="preview-bars"><i v-for="(size,i) in [36,58,46,78,64,92]" :key="i" :style="{'--size':size+'%','--i':i}" /></span>
  </span>
</template>
<script setup lang="ts">
defineProps<{kind:string}>();
</script>
<style scoped>
.v2-template-preview{height:76px;width:100%;display:flex;align-items:center;justify-content:center;background:#f4f8f6;border-radius:6px;overflow:hidden;padding:10px 18px;box-sizing:border-box;flex:none}.preview-ring{height:56px;width:56px;border-radius:50%;background:conic-gradient(#197f70 0 48%,#548eb3 48% 76%,#bf985c 76% 100%)}.preview-donut .preview-ring{mask:radial-gradient(circle,transparent 0 48%,#000 50%)}.preview-bars{display:flex;gap:7px;width:100%;height:100%;align-items:flex-end}.preview-bars i{display:block;flex:1;background:#9bb8b4;height:var(--size);border-radius:3px 3px 0 0;transform-origin:bottom;animation:preview-in 400ms both;animation-delay:calc(var(--i)*30ms)}.preview-bars i:nth-child(4){background:#197f70}.preview-horizontal .preview-bars{flex-direction:column;gap:3px;align-items:flex-start}.preview-horizontal .preview-bars i{width:var(--size);height:auto;border-radius:0 3px 3px 0;transform-origin:left}.preview-area .preview-bars,.preview-line .preview-bars{clip-path:polygon(0 70%,18% 35%,34% 55%,51% 16%,69% 42%,100% 0,100% 100%,0 100%);background:#c7e1d7;gap:0}.preview-area .preview-bars i,.preview-line .preview-bars i{height:100%;background:#8cbfb0}.preview-line .preview-bars{height:42px;max-width:180px}.preview-line .preview-bars i{background:#197f70}@keyframes preview-in{from{opacity:.3;transform:scaleY(.4)}to{opacity:1;transform:scaleY(1)}}@media(prefers-reduced-motion:reduce){.preview-bars i{animation:none}}
</style>
