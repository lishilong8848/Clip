<template>
  <form class="assistant-question-form" aria-label="补充回答" @submit.prevent="submit">
    <LighthouseStructuredField v-if="options.length" v-model="selected" :id="id+'-choices'" :field="choiceField" :disabled="disabled" />
    <LighthouseStructuredField v-model="answer" :id="id+'-answer'" :field="answerField" :disabled="disabled" />
    <footer class="plan-form-footer"><button type="submit" class="primary" :disabled="disabled||!ready"><Check :size="15" />补充并继续</button></footer>
  </form>
</template>
<script setup lang="ts">
import {computed,defineAsyncComponent,ref} from 'vue';
import {Check} from 'lucide-vue-next';
import type {Dict} from '../api/client';
const LighthouseStructuredField=defineAsyncComponent(()=>import('./LighthouseStructuredField.vue'));
const props=defineProps<{id:string;question:Dict;disabled:boolean}>();
const emit=defineEmits<{reply:[answer:string]}>();
const selected=ref<string|string[]>(''),answer=ref<string|number|null>('');
const options=computed<string[]>(()=>props.question.options||[]);
const choiceField=computed(()=>({type:props.question.multiple?'multiselect':'select',choice_group:!!props.question.multiple,
  label:'选择回答',options:options.value.map(value=>({label:value,value}))}));
const answerField=computed(()=>({type:props.question.input_type||'textarea',label:options.value.length?'补充说明（可选）':'补充回答',maxlength:1000}));
const choices=computed(()=>{const values=Array.isArray(selected.value)?selected.value:[selected.value];return options.value.filter(value=>values.includes(value));});
const ready=computed(()=>choices.value.length>0||String(answer.value??'').trim().length>0);
function submit(){if(!props.disabled&&ready.value)emit('reply',[choices.value.join('、'),String(answer.value??'').trim()].filter(Boolean).join('\n'));}
</script>
<style scoped>
.assistant-question-form{display:grid;gap:12px;margin-top:12px;padding-top:12px;border-top:1px solid var(--lh-border,#dbe2ea);min-width:0}
.assistant-question-form :deep(.lh-sf-single){flex-wrap:wrap}
.assistant-question-form footer{margin:0;justify-content:flex-end}
</style>
