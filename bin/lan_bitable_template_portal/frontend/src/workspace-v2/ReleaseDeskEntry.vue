<script setup lang="ts">
import { ChevronRight } from 'lucide-vue-next';
defineProps<{ version: string; patch: number; published: string; issueCount: number; summary: string; modules: string[] }>();
defineEmits<{ open: [] }>();
</script>
<template>
  <button type="button" class="release-journal-entry" @click="$emit('open')" :aria-label="'查看版本 '+version+' 的发布详情'">
    <span class="rj-version"><strong>{{ version }}</strong><small v-if="patch!=null">补丁 {{ patch }}</small></span>
    <span class="rj-copy"><b>{{ summary }}</b><small>{{ modules.join('、') || '模块未注明' }}</small></span>
    <span class="rj-dates"><time>{{ published }}</time><small>{{ issueCount }} 条变更</small></span>
    <ChevronRight :size="16" aria-hidden="true"/>
  </button>
</template>
<style src="./releaseJournalDesk.css"></style>
