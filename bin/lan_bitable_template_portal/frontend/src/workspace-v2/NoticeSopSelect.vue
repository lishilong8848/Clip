<template>
  <NSelect ref="select" size="small" v-bind="$attrs" :value="selectedValue" :options="optionList()" :disabled="disabled" :to="true" :menu-props="{style:{zIndex:menuZIndex||10010}}" filterable :placeholder="placeholder||'请选择'" :status="error?'error':undefined" :input-props="{id:inputId||id,'aria-label':label||String($attrs['aria-label']||''),'aria-required':required?'true':undefined}" @update:value="change" />
</template>
<script setup lang="ts">
import {computed,ref,useSlots,type VNode} from 'vue';
import {NSelect,type SelectInst} from 'naive-ui';
defineOptions({inheritAttrs:false});
const props=defineProps<{value?:string|number;modelValue?:string;options?:string[];disabled?:boolean;id?:string;inputId?:string;label?:string;placeholder?:string;required?:boolean;menuZIndex?:number}>();
const emit=defineEmits<{'update:modelValue':[value:string];change:[event:Event]}>(),slots=useSlots(),select=ref<SelectInst|null>(null),error=ref('');
// NSelect treats an empty string as an actual selected value and hides the hint.
const selectedValue=computed(()=>{const value=props.modelValue??props.value;return value===''||value==null?null:value;});
function optionList(){
  if(props.options)return props.options.map(value=>({value,label:value}));
  const result:{value:string|number;label:string;disabled:boolean}[]=[];
  const visit=(nodes:VNode[])=>{for(const node of nodes){if(node.type==='option'&&node.props?.value!=='')result.push({value:node.props?.value,label:String(node.children??''),disabled:!!node.props?.disabled});else if(Array.isArray(node.children))visit(node.children as VNode[]);}};
  visit(slots.default?.()||[]);return result;
}
function change(value:string|number){emit('update:modelValue',String(value));emit('change',{target:{value:String(value)}} as unknown as Event);}
defineExpose({focus:()=>select.value?.focus(),setCustomValidity:(message:string)=>{error.value=message;}});
</script>
