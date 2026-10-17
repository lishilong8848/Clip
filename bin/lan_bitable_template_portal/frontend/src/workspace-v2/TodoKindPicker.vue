<template>
  <NSelect :value="value" :options="options" multiple filterable clearable :disabled="disabled" :max-tag-count="2" :ellipsis-tag-popover-props="{disabled:true}" :render-label="renderLabel" :to="true" aria-label="选择待办业务及排列顺序" placeholder="选择要显示的业务" @update:value="change" @update:show="shown=>{open=shown;if(!shown)$emit('commit')}" />
</template>
<script setup lang="ts">
import { computed,h,ref } from 'vue';
import { NSelect,type SelectOption } from 'naive-ui';
import type { Dict } from './client';
import { todoOptions,todoSelection,type TodoKind } from './businessTodos';
const props=defineProps<{value:TodoKind[];user:Dict;disabled?:boolean}>();
const emit=defineEmits<{'update:value':[value:TodoKind[]];commit:[]}>();
const options=computed(()=>todoOptions(props.user));
const open=ref(false);
function change(value:string[]){const next=todoSelection(value,props.user);emit('update:value',[...props.value.filter(id=>next.includes(id)),...next.filter(id=>!props.value.includes(id))]);if(!open.value)emit('commit');}
function renderLabel(option:SelectOption){const order=props.value.indexOf(option.value as TodoKind);return h('span',{class:'v2-todo-option'},[h('span',String(option.label)),order>=0?h('b',{'aria-label':'排列第'+(order+1)+'项'},String(order+1)):null]);}
</script>
<style>
.v2-todo-option{display:inline-flex;align-items:center;justify-content:space-between;gap:10px;width:100%;min-width:0}.v2-todo-option b{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;font-size:11px;border-radius:4px;background:var(--v2-blue,#1764dd);color:#fff}
</style>
