<script setup lang="ts">
import {computed} from 'vue';
import { NButton,NInput,NRadioButton,NRadioGroup } from 'naive-ui';
import { Paperclip } from 'lucide-vue-next';
import type {Dict} from '../client';
import CheckAttachments from './CheckAttachments.vue';
const p=defineProps<{
  task:Dict; locked:boolean; uploading:boolean; recordEditor?:boolean;
  answerFor:(id:string)=>{status:string;note:string}; expanded?:Record<string,boolean>;
}>();
const emit=defineEmits<{
  status:[id:string,value:string]; note:[id:string,value:string]; toggle:[id:string,event:MouseEvent];
  busy:[value:boolean]; changed:[value?:Dict];
}>();
const resultLocked=computed(()=>p.locked || (p.recordEditor?!p.task.can_edit:!p.task.can_submit));
function files(id:string){return (p.task.files || []).filter((f:Dict)=>f.item_id===id);}
</script>
<template>
  <div :class="[recordEditor?'check-record-items':'v2-check-items','ps-form-list']" role="group" :aria-label="recordEditor?'修改检查项目':'本次检查项目'" tabindex="0">
    <fieldset v-for="(item,index) in task.items" :key="item.id" :class="[recordEditor?'check-record-item':'v2-check-item','ps-form-row']" :data-result="answerFor(item.id).status">
      <legend><span>{{ index+1 }}</span>{{ item.text }}</legend>
      <div class="v2-check-result">
        <NRadioGroup :value="answerFor(item.id).status" :name="(recordEditor?'edit-':'')+task.id+'-'+item.id" :disabled="resultLocked" :aria-label="item.text" @update:value="v=>emit('status',item.id,String(v))">
          <NRadioButton class="v2-check-normal" value="normal">无异常</NRadioButton><NRadioButton class="v2-check-abnormal" value="abnormal">异常</NRadioButton>
        </NRadioGroup>
        <NButton v-if="!recordEditor" class="v2-check-attachment-toggle" attr-type="button" quaternary :aria-label="item.text+'附件'" :aria-expanded="!!expanded?.[task.id+':'+item.id]"
          :disabled="locked || uploading || (!task.can_submit && !files(item.id).length)" @click="emit('toggle',item.id,$event)">
          <Paperclip :size="15"/><span v-if="files(item.id).length">{{ files(item.id).length }}</span>
        </NButton>
      </div>
      <div v-if="answerFor(item.id).status==='abnormal'" class="v2-check-note ps-note">
        <label :for="(recordEditor?'record-note-':'check-note-')+task.id+'-'+item.id">异常备注</label>
        <NInput :value="answerFor(item.id).note" type="textarea" :autosize="{minRows:1,maxRows:4}" :maxlength="500" :disabled="resultLocked"
          :input-props="{id:(recordEditor?'record-note-':'check-note-')+task.id+'-'+item.id,'aria-label':item.text+'异常备注'}" placeholder="填写异常情况" @update:value="v=>emit('note',item.id,v)"/>
      </div>
      <CheckAttachments v-if="recordEditor || expanded?.[task.id+':'+item.id]" :format-hint="!recordEditor" :task-id="task.id" :item-id="item.id" kind="item" :files="files(item.id)"
        :revision="recordEditor?task.revision:undefined" :locked="resultLocked||uploading" :allow-upload="recordEditor?task.can_edit:task.can_submit"
        @busy="v=>emit('busy',v)" @changed="v=>emit('changed',v)"/>
    </fieldset>
  </div>
</template>
<style scoped>
.ps-form-list{display:grid;align-content:start;min-width:0}
.ps-form-list fieldset.ps-form-row{display:flow-root;box-sizing:border-box;min-inline-size:0;margin:0;padding:.58rem .7rem;border:0;border-bottom:1px solid var(--v2-palette-border,#d2dfda);border-inline-start:3px solid transparent;border-radius:0;background:var(--soft-face,#fff)}
.ps-form-list fieldset[data-result=normal]{border-inline-start-color:#589e83}.ps-form-list fieldset[data-result=abnormal]{border-inline-start-color:#bf7647;background:color-mix(in srgb,var(--soft-face,#fff) 95%,#de9668)}
.ps-form-row legend{float:left;max-width:calc(100% - 185px);margin:0;padding:.4rem 0;line-height:1.45;overflow-wrap:anywhere;font-weight:600}.ps-form-row legend span{font-variant-numeric:tabular-nums;margin-right:.55rem;color:var(--v2-blue,#43738b)}
.ps-form-row>.v2-check-result{display:flex;justify-content:flex-end;align-items:center;gap:.4rem}.ps-form-row :is(.ps-note,.v2-check-attachments){clear:both}.ps-form-row .ps-note{padding-block-start:.45rem}.ps-form-row .v2-check-attachments{padding-block-start:.45rem}
@container shift-check (max-width:560px){.ps-form-row legend{float:none;max-width:100%}.ps-form-row>.v2-check-result{justify-content:flex-start}}
@media(max-width:600px){.ps-form-row legend{float:none;max-width:100%}.ps-form-row>.v2-check-result{justify-content:flex-start}}
.ps-form-row :deep(.n-radio-button--checked.n-radio-button--disabled){background-color:var(--n-button-color-active);color:#fff;transition:none}
</style>