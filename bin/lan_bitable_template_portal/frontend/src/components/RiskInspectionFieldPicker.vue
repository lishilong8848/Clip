<template>
  <div class="risk-field-picker">
    <section class="risk-field-pane" aria-label="可选字段库">
      <header><div><h3>字段库</h3><span>{{ library?.fields.length || 0 }} 个字段</span></div><NButton v-if="canEdit" size="small" :disabled="disabled||loading||!library||library.fields.length>=80" @click="$emit('add')"><template #icon><Plus :size="16" /></template>添加字段</NButton></header>
      <NInput v-model:value="search" clearable placeholder="搜索字段名称" :input-props="{'aria-label':'搜索字段名称'}" />
      <NAlert v-if="error" type="error" :bordered="false"><span>{{ error }}</span><NButton text :disabled="disabled||loading" @click="$emit('reload')">重新读取字段库</NButton></NAlert>
      <NSpin :show="loading"><div class="risk-field-list">
        <div v-for="field in visibleFields" :key="field.key" class="risk-field-row" :class="{'is-chosen':selectedKeys.has(field.key)}">
          <button class="risk-field-pick" data-v2-navigation type="button" :disabled="disabled||loading||field.unsupported||selectedKeys.has(field.key)||fields.length>=80||field.is_primary&&fields.some(f=>f.is_primary)" :aria-label="'添加字段 '+field.field_name" @click="$emit('select',field)">
            <Check v-if="selectedKeys.has(field.key)" :size="17" /><Plus v-else :size="17" />
            <span><strong>{{ field.field_name }}</strong><small>{{ riskTypeName(field.type) }} · {{ field.is_primary?'主字段':riskRoleName(field.role) }}{{ field.unsupported?' · 暂不支持':'' }}</small></span>
          </button>
          <div v-if="canEdit" class="risk-field-tools"><NButton quaternary circle :disabled="disabled||loading" :title="'修改字段 '+field.field_name" :aria-label="'修改字段 '+field.field_name" @click="$emit('edit',field)"><Pencil :size="15" /></NButton><NButton quaternary circle type="error" :disabled="disabled||loading||field.is_primary" :title="field.is_primary?'主字段不可删除':'删除库字段 '+field.field_name" :aria-label="'删除库字段 '+field.field_name" @click="$emit('delete',field)"><Trash2 :size="15" /></NButton></div>
        </div>
        <NEmpty v-if="!loading&&!visibleFields.length" size="small" :description="library?'没有匹配的字段':'字段库尚未读取'" />
      </div></NSpin>
    </section>
    <section class="risk-field-pane risk-selected-pane" aria-label="当前模板字段">
      <header><div><h3>已选字段</h3><span>{{ fields.length }} / 80</span></div></header>
      <slot name="settings" />
      <ol class="risk-field-list risk-selected-fields">
        <li v-for="(field,index) in fields" :key="field.key" class="risk-field-row">
          <span class="risk-field-order">{{ index+1 }}</span><div class="risk-selected-name"><strong>{{ field.field_name }}</strong><small>{{ riskTypeName(field.type) }} · {{ field.is_primary?'主字段':riskRoleName(field.role) }}</small></div>
          <label v-if="field.role==='input'" class="risk-field-required"><NSwitch :value="!!field.required" :disabled="disabled" :aria-label="field.field_name+'是否必填'" @update:value="value=>$emit('required',field,value)" /><span>必填</span></label>
          <LockKeyhole v-if="field.is_primary" :size="16" aria-label="主字段不可移除" />
          <NButton v-else data-v2-navigation quaternary circle :disabled="disabled" :title="'移除已选字段 '+field.field_name" :aria-label="'移除已选字段 '+field.field_name" @click="$emit('remove',field)"><X :size="17" /></NButton>
        </li>
      </ol>
      <slot name="prefill" />
      <footer class="risk-field-generate"><slot name="footer" /></footer>
    </section>
  </div>
</template>
<script setup lang="ts">
import {computed,ref} from 'vue';
import {NAlert,NButton,NEmpty,NInput,NSpin,NSwitch} from 'naive-ui';
import {Check,LockKeyhole,Pencil,Plus,Trash2,X} from 'lucide-vue-next';
import type {Dict} from '../api/client';
import {riskRoleName,riskTypeName} from './riskInspectionUi';
const props=defineProps<{library:Dict|null;fields:Dict[];loading:boolean;disabled:boolean;canEdit:boolean;error:string}>();
defineEmits<{(e:'add'|'reload'):void;(e:'select'|'edit'|'delete'|'remove',field:Dict):void;(e:'required',field:Dict,value:boolean):void}>();
const search=ref('');
const selectedKeys=computed(()=>new Set(props.fields.map(field=>field.key)));
const visibleFields=computed<Dict[]>(()=>props.library?.fields.filter((field:Dict)=>field.field_name.toLocaleLowerCase().includes(search.value.trim().toLocaleLowerCase()))||[]);
</script>
<style scoped>
.risk-field-picker{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:24px;align-items:start}.risk-field-pane{display:grid;gap:12px;min-width:0}.risk-selected-pane{padding-left:24px;border-left:1px solid var(--border-color,#dce2e9)}.risk-field-pane>header,.risk-field-pane>header>div{display:flex;align-items:center;gap:10px}.risk-field-pane>header{justify-content:space-between;min-height:34px}.risk-field-pane h3{margin:0;font-size:16px}.risk-field-pane>header span{color:var(--text-color-3,#667085);font-size:12px}.risk-field-list{display:grid;align-content:start;gap:4px;max-height:52dvh;overflow:auto;overscroll-behavior-y:auto;padding:2px;margin:0;list-style:none}.risk-field-row{display:flex;align-items:center;gap:8px;min-width:0;min-height:54px;border-bottom:1px solid var(--border-color,#dce2e9);border-radius:4px}.risk-field-row.is-chosen{background:color-mix(in srgb,var(--primary-color,#168753) 7%,transparent)}.risk-field-pick{display:flex;align-items:center;gap:10px;flex:1;min-width:0;align-self:stretch;padding:10px 8px;border:0;background:transparent;color:inherit;text-align:left;cursor:pointer}.risk-field-pick:focus-visible{outline:2px solid var(--primary-color,#168753);outline-offset:-2px}.risk-field-pick:disabled{cursor:default}.risk-field-pick:not(:disabled):hover{background:color-mix(in srgb,var(--primary-color,#168753) 9%,transparent)}.risk-field-pick>svg{flex-shrink:0;color:var(--primary-color,#168753)}.risk-field-pick>span,.risk-selected-name{display:grid;gap:3px;min-width:0}.risk-field-row strong{font-size:14px;font-weight:500;overflow-wrap:anywhere}.risk-field-row small{font-size:12px;color:var(--text-color-3,#667085);overflow-wrap:anywhere}.risk-field-tools{display:flex;gap:4px;flex-shrink:0;padding-right:4px}.risk-selected-fields .risk-field-row{padding:8px 4px}.risk-selected-name{flex:1}.risk-field-order{min-width:20px;text-align:center;font-size:12px;color:var(--text-color-3,#667085)}.risk-field-required{display:flex;align-items:center;gap:6px;flex-shrink:0;font-size:12px}.risk-field-generate{display:flex;align-items:center;justify-content:space-between;gap:12px;border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.risk-field-generate :slotted(span){font-size:12px;color:var(--text-color-3,#667085)}
@media(max-width:760px){.risk-field-picker{grid-template-columns:minmax(0,1fr);gap:20px}.risk-selected-pane{padding:16px 0 0;border-left:0;border-top:1px solid var(--border-color,#dce2e9)}.risk-field-list{max-height:32dvh}.risk-field-tools :deep(.n-button),.risk-selected-fields :deep(.n-button){min-width:40px;min-height:40px}.risk-field-generate{flex-wrap:wrap}.risk-field-generate :slotted(.n-button){margin-left:auto;min-height:44px}}
</style>
