import {parse,compileScript} from '@vue/compiler-sfc';

// V2 replaces presentation only; scan, matching and save stay in the legacy controller.
export function adaptHistoryMemory(source, view) {
  const {descriptor}=parse(source);
  if(!descriptor.scriptSetup)throw new Error('History memory controller requires script setup');
  const bindings=compileScript(descriptor,{id:'history-memory-v2'}).bindings;
  const names=[...new Set([...view.matchAll(/\br\.([A-Za-z_$][\w$]*)/g)].map(match=>match[1]))].sort();
  for(const name of names)if(!(name in bindings))throw new Error('History memory binding changed: '+name);
  const oldDrawerKey='onMounted(() => document.addEventListener("keydown", handleReviewKeydown));';
  if(!descriptor.scriptSetup.content.includes(oldDrawerKey))throw new Error('History drawer keyboard contract changed');
  return `<template><HistoryMemoryWorkspace v-bind="$props" :r="v2History" /></template>
<script setup lang="ts">
import {proxyRefs} from 'vue';
import HistoryMemoryWorkspace from '../workspace-v2/HistoryMemoryWorkspace.vue';
${descriptor.scriptSetup.content.replace(oldDrawerKey,'')}
const v2History=proxyRefs({${names.join(',')}});
</script>`;
}
