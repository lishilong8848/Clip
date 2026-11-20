<script setup lang="ts">
import { NModal } from 'naive-ui';
defineProps<{title:string; locked:boolean; alert?:boolean}>();
defineEmits<{close:[]}>();
</script>
<template>
  <NModal :show="true" :mask-closable="!locked" :close-on-esc="!locked" @update:show="value=>{if(!value&&!locked)$emit('close')}">
    <section class="evt-desk evt-dialog" data-v2-inline-feedback :role="alert?'alertdialog':'dialog'" :aria-label="title" aria-modal="true">
      <header><h2>{{title}}</h2><div class="evt-actions"><slot name="actions" /></div></header>
      <div class="evt-dialog-body"><slot /></div>
    </section>
  </NModal>
</template>
