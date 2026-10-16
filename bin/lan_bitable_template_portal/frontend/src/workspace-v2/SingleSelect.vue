<template>
  <LegacySelect v-if="allowCustom" v-bind="$props" @update:model-value="$emit('update:modelValue',$event)" @change="$emit('change',$event)" />
  <NativeSingleSelect v-else class="v2-string-select">
    <select :id="inputId" :value="modelValue" :disabled="disabled" :required="required" :aria-label="label||placeholder||'请选择'" :aria-invalid="!!error" @change="changed">
      <option value="">{{placeholder||'请选择'}}</option>
      <option v-if="modelValue&&!options.includes(modelValue)" :value="modelValue" disabled>{{modelValue}}</option>
      <option v-for="option in options" :key="option" :value="option">{{option}}</option>
    </select>
  </NativeSingleSelect>
</template>
<script setup lang="ts">
import NativeSingleSelect from './NativeSingleSelect.vue';
import LegacySelect from '../components/VnetSelect.vue';
withDefaults(defineProps<{modelValue?:string;options:string[];inputId:string;label?:string;placeholder?:string;disabled?:boolean;required?:boolean;error?:string;allowCustom?:boolean;clearable?:boolean;menuZIndex?:number}>(),{modelValue:'',label:'',placeholder:'请选择',disabled:false,required:false,error:'',allowCustom:false,clearable:false});
const emit=defineEmits<{'update:modelValue':[value:string];change:[value:string]}>();
function changed(event:Event){const value=(event.target as HTMLSelectElement).value;emit('update:modelValue',value);emit('change',value);}
</script>
