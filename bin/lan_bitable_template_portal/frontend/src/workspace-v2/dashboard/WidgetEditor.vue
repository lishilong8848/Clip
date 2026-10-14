<template>
  <NModal :show="Boolean(value)" preset="card" class="v2-widget-editor" title="组件设置" :bordered="false" :mask-closable="false" @close="$emit('close')" @update:show="shown=>{if(!shown)$emit('close')}">
    <NForm v-if="draft" label-placement="top" @submit.prevent="submit">
      <NFormItem label="组件名称"><NInput v-model:value="draft.title" :maxlength="40" placeholder="请输入组件名称" /></NFormItem>
      <div class="v2-widget-form-grid">
        <NFormItem v-if="presentationOptions(draft.kind).length>1" label="组件样式"><NSelect v-model:value="draft.presentation" :options="presentationOptions(draft.kind)" /></NFormItem>
        <NFormItem v-if="draft.kind==='tasks'" label="待办业务"><NSelect v-model:value="draft.module" :options="taskOptions(user)" filterable /></NFormItem>
        <NFormItem v-if="!['shortcuts','learning','pre_shift'].includes(draft.kind)" label="楼栋范围"><NSelect v-model:value="draft.scope" :options="[{value:'current',label:'跟随工作台楼栋'},...scopes]" /></NFormItem>
        <NFormItem label="组件宽度"><NSelect v-model:value="draft.w" :options="[{label:'紧凑 · 1/3',value:4},{label:'标准 · 1/2',value:6},{label:'宽幅 · 2/3',value:8},{label:'整行',value:12}]" /></NFormItem>
        <NFormItem label="组件高度"><NSelect v-model:value="draft.h" :options="[{label:'紧凑',value:3},{label:'标准',value:5},{label:'加高',value:7},{label:'超高',value:9}]" /></NFormItem>
        <NFormItem v-if="draft.kind==='notices'" label="统计口径"><NSelect v-model:value="draft.metric" :options="[{label:'当前进行中',value:'ongoing'},{label:'本月待发计划',value:'pending'}]" /></NFormItem>
        <NFormItem v-if="draft.kind==='events'" label="时间范围"><NSelect v-model:value="draft.period" :options="[{label:'本周',value:'week'},{label:'本月',value:'month'}]" /></NFormItem>
        <NFormItem v-if="['events','notices'].includes(draft.kind)" label="展示方式"><NSelect v-model:value="draft.display" :options="displayOptions(draft.kind)" /></NFormItem>
      </div>
      <NFormItem v-if="draft.kind==='shortcuts'" label="常用功能"><NSelect v-model:value="draft.links" multiple :options="navigationModules(user.modules||[]).map(m=>({label:m.label,value:m.id}))" filterable /></NFormItem>
      <NAlert v-if="error" type="error" :bordered="false">{{ error }}</NAlert>
      <div class="v2-editor-footer"><NButton @click="$emit('close')">取消</NButton><NButton type="primary" attr-type="submit">应用设置</NButton></div>
    </NForm>
  </NModal>
</template>
<script setup lang="ts">
import { ref,watch } from 'vue';
import { NAlert,NButton,NForm,NFormItem,NInput,NModal,NSelect } from 'naive-ui';
import type { Dict } from '../client';
import { navigationModules } from '../modules';
import { displayOptions,presentationOptions,taskOptions,type Widget } from './layout';
const props=defineProps<{value:Widget|null;user:Dict;scopes:{value:string;label:string}[]}>();
const emit=defineEmits<{close:[];apply:[value:Widget]}>();
const draft=ref<Widget|null>(null),error=ref('');
watch(()=>props.value,value=>{draft.value=value?JSON.parse(JSON.stringify(value)):null;error.value='';},{immediate:true});
function submit(){if(!draft.value)return;if(!draft.value.title.trim()){error.value='请填写组件名称。';return;}emit('apply',{...draft.value,x:Math.min(draft.value.x,12-draft.value.w)});}
</script>
