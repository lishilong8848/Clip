<template>
  <NSelect class="v2-widget-building-picker" multiple size="small" :show="open" :value="open?draft:value" :options="choices" :max-tag-count="1" :ellipsis-tag-popover-props="{disabled:true}" :disabled="disabled" :aria-label="label" :consistent-menu-width="false" :menu-props="menuProps" @pointerenter="cancelClose" @pointerleave="leave" @update:value="select" @update:show="show" />
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,ref,watch} from 'vue';
import {NSelect} from 'naive-ui';
import type {ScopeOption} from './widgetScopes';
const props=defineProps<{value:string[];options:ScopeOption[];label:string;disabled?:boolean}>();
const emit=defineEmits<{change:[value:string[]]}>();
const choices=computed(()=>[{value:'ALL',label:'全部授权楼栋'},...props.options]);
const open=ref(false),draft=ref<string[]>([]);
let closeTimer:ReturnType<typeof setTimeout>|undefined;
function cancelClose(){if(closeTimer)clearTimeout(closeTimer);closeTimer=undefined;}
function leave(event:PointerEvent){if(event.pointerType==='mouse'){cancelClose();closeTimer=setTimeout(()=>show(false),180);}}
const menuProps={class:'v2-widget-building-menu',onPointerenter:cancelClose,onPointerleave:leave};
function show(value:boolean){
  cancelClose();if(value===open.value)return;
  if(value){draft.value=[...props.value];open.value=true;}
  else{open.value=false;if(!props.disabled&&JSON.stringify(draft.value)!==JSON.stringify(props.value))emit('change',[...draft.value]);}
}
function select(values:string[]){
  const previous=open.value?draft.value:props.value;
  const next=values.filter(value=>value!=='ALL'&&props.options.some(option=>option.value===value));
  const value=values.includes('ALL')&&!previous.includes('ALL')?['ALL']:next.length?next:['ALL'];
  if(open.value)draft.value=value;else emit('change',value);
}
watch(()=>props.disabled,value=>{if(value){cancelClose();open.value=false;}});
onBeforeUnmount(cancelClose);
</script>
<style>
.v2-widget-building-picker{width:164px;max-width:100%;flex:none;min-width:0}.v2-widget-building-picker .n-base-selection-tags{flex-wrap:nowrap}.v2-widget-building-picker .n-base-selection-tag-wrapper{min-width:0}.v2-widget-building-picker .n-tag__content{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
@media(max-width:760px){.v2-widget-building-picker{width:142px}.v2-widget-building-picker .n-base-selection{min-height:40px}}
</style>
