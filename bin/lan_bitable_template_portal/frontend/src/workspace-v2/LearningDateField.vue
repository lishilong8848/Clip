<template><component ref="picker" :is="type==='time'?NTimePicker:NDatePicker" :type="type==='time'?undefined:'date'" :formatted-value="modelValue || null" :value-format="type==='time'?'HH:mm':'yyyy-MM-dd'" :format="type==='time'?'HH:mm':undefined" :disabled="disabled" clearable @update:formatted-value="change" /></template>
<script setup lang="ts">
import {ref,watchEffect} from 'vue';
import {NDatePicker,NTimePicker} from 'naive-ui';
const props=defineProps<{modelValue?:string;type?:'date'|'time';disabled?:boolean;ariaLabel?:string;id?:string}>();
const picker=ref<{$el:HTMLElement}>();
watchEffect(()=>{
  const input=picker.value?.$el.querySelector('input');
  if(!input)return;
  input.setAttribute('aria-label',props.ariaLabel || (props.type==='time'?'时间':'日期'));
  if(props.id)input.id=props.id;
},{flush:'post'});
const emit=defineEmits<{'update:modelValue':[value:string];change:[value:string]}>();
function change(value:string|null){emit('update:modelValue',value||'');emit('change',value||'');}
</script>
