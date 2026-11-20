<script setup lang="ts">
import { ref } from 'vue';
import InspectionDeskDialog from './InspectionDeskDialog.vue';

type Duty = { scope: string; date: string; shift: string };
type Archive = { id: string; duty: Duty; round: number; actor: string; completed: number; url?: string };
defineProps<{
  records: Archive[];
  dutyLabel: (duty: Duty) => string;
  time: (timestamp: number) => string;
  compact?: boolean;
  target?: HTMLElement;
}>();
const open = ref(false);
</script>
<template>
  <button v-if="compact" type="button" @click="open=true">归档记录（{{ records.length }}）</button>
  <component :is="compact ? InspectionDeskDialog : 'div'"
    v-bind="compact ? { show: open, title: '巡检归档记录', target } : {}" @close="open=false">
    <div class="ins-grid">
      <article v-for="record in records" :key="record.id" class="ins-head ins-record">
        <div><strong>{{ dutyLabel(record.duty) }} · 第 {{ record.round }} 轮</strong>
          <small>{{ record.actor }} · {{ time(record.completed) }}</small></div>
        <a v-if="record.url" :href="record.url" target="_blank" rel="noopener">查看归档</a>
      </article>
    </div>
  </component>
</template>
