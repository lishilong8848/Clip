<script setup lang="ts">
import { NModal } from 'naive-ui';
import { X } from 'lucide-vue-next';
defineProps<{ title: string; locked?: boolean }>();
defineEmits<{ close: [] }>();
</script>
<template>
  <NModal :show="true" :mask-closable="!locked" :close-on-esc="!locked" @update:show="!$event && $emit('close')">
    <section class="tool-desk td-dialog" role="dialog" aria-modal="true" :aria-label="title">
      <header class="td-heading"><h2>{{ title }}</h2><div class="td-actions"><slot name="actions"/><button type="button" class="td-icon" aria-label="关闭弹窗" :disabled="locked" @click="$emit('close')"><X :size="18"/></button></div></header>
      <slot/>
    </section>
  </NModal>
</template>