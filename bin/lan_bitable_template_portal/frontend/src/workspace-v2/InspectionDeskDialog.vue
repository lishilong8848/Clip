<script setup lang="ts">
import { NModal } from 'naive-ui';
defineProps<{show:boolean; title:string; target?:HTMLElement; locked?:boolean}>();
defineEmits<{close:[]; afterLeave:[]}>();
</script>
<template><NModal :show="show" :to="target" :mask-closable="!locked" :close-on-esc="!locked" @update:show="!$event && $emit('close')" @after-leave="$emit('afterLeave')">
  <section class="ins-modal ins-desk" role="dialog" aria-modal="true" :aria-label="title"><header class="ins-head"><h2>{{ title }}</h2><div class="ins-actions"><slot name="actions"/><button :disabled="locked" @click="$emit('close')">关闭</button></div></header><slot/></section>
</NModal></template>