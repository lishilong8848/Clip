<template><NSelect :value="modelValue || null" :options="choices" :disabled="disabled" :placeholder="placeholder || label || '请选择'" :aria-label="label || '学练选项'" filterable @update:value="value => emit('update:modelValue',value || '')" /></template>
<script setup lang="ts">
import {computed} from 'vue';
import {NSelect} from 'naive-ui';
const props=defineProps<{modelValue?:string;options:Array<string|{label:string;value:string}>;disabled?:boolean;placeholder?:string;label?:string;inputId?:string}>();
const emit=defineEmits<{'update:modelValue':[value:string]}>();
const choices=computed(()=>props.options.map(item=>typeof item==='string'?{label:item,value:item}:item));
</script>
