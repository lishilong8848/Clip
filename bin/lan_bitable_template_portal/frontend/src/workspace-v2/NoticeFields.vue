<template>
  <div class="v2-notice-field-grid">
    <NFormItem v-for="field in fields" :key="field.key" :label="field.label" :required="field.required" :data-notice-field="field.key" :validation-status="attempted&&field.required&&!String(values[field.key]??'').trim()?'error':undefined" :class="{'is-wide':field.type==='textarea'||field.key==='title'}">
      <NDatePicker v-if="field.type==='datetime-local'" :formatted-value="String(values[field.key]||'').replace('T',' ')||null" type="datetime" value-format="yyyy-MM-dd HH:mm" format="yyyy-MM-dd HH:mm" clearable :disabled="locked||field.readonly" :input-props="{'aria-label':field.label}" @update:formatted-value="value=>values[field.key]=value||''" />
      <NSelect v-else-if="field.options.length" v-model:value="values[field.key]" :options="field.options" filterable :tag="field.type!=='select-one'" :disabled="locked||field.readonly" :aria-label="field.label" />
      <NInput v-else v-model:value="values[field.key]" :type="field.type==='textarea'?'textarea':'text'" :autosize="{minRows:2,maxRows:5}" :disabled="locked||field.readonly" :input-props="{'aria-label':field.label}" />
      <template v-if="field.hint" #feedback>{{ field.hint }}</template>
    </NFormItem>
    <slot />
  </div>
</template>
<script setup lang="ts">
import { NDatePicker,NFormItem,NInput,NSelect } from 'naive-ui';
import type { Dict } from './client';
import type { NoticeField } from './noticeForm';
defineProps<{fields:NoticeField[];values:Dict;locked:boolean;attempted:boolean}>();
</script>
