<template>
  <NSelect class="v2-widget-building-picker" multiple size="small" :value="value" :options="choices" :max-tag-count="1" :disabled="disabled" :aria-label="label" :consistent-menu-width="false" @update:value="select" />
</template>
<script setup lang="ts">
import {computed} from 'vue';
import {NSelect} from 'naive-ui';
import type {ScopeOption} from './widgetScopes';
const props=defineProps<{value:string[];options:ScopeOption[];label:string;disabled?:boolean}>();
const emit=defineEmits<{change:[value:string[]]}>();
const choices=computed(()=>[{value:'ALL',label:'全部授权楼栋'},...props.options]);
function select(values:string[]){
  if(values.includes('ALL')&&!props.value.includes('ALL')){emit('change',['ALL']);return;}
  const next=values.filter(value=>value!=='ALL'&&props.options.some(option=>option.value===value));
  emit('change',next.length?next:['ALL']);
}
</script>
<style>
.v2-widget-building-picker{width:164px;max-width:100%;flex:none;min-width:0}.v2-widget-building-picker .n-base-selection-tags{flex-wrap:nowrap}.v2-widget-building-picker .n-base-selection-tag-wrapper{min-width:0}.v2-widget-building-picker .n-tag__content{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
@media(max-width:760px){.v2-widget-building-picker{width:142px}.v2-widget-building-picker .n-base-selection{min-height:40px}}
</style>
