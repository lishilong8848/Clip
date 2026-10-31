<template>
  <div class="v2-event-period">
    <NTabs :value="modelValue" type="segment" animated :tabs-padding="4" role="tablist" aria-label="事件时间范围" @update:value="select">
      <NTab v-for="option in options" :key="option.key" :name="option.key" :disabled="disabled" role="tab"
        :aria-selected="modelValue === option.key" :aria-disabled="disabled" aria-controls="v2-event-list"
        :tabindex="modelValue === option.key ? 0 : -1" @keydown="onKey($event, option.key)">
        <component :is="option.icon" :size="16" aria-hidden="true" />{{ option.label }}
      </NTab>
    </NTabs>
    <div class="v2-event-period-meta">
      <NDatePicker v-if="modelValue === 'history'" :formatted-value="month" type="month" value-format="yyyy-MM" format="yyyy年MM月"
        :disabled="disabled" :input-readonly="true" :clearable="false" aria-label="事件历史月份" @update:formatted-value="updateMonth" />
      <div v-if="modelValue === 'custom'" class="v2-event-custom-dates">
        <label><span>开始日期</span><NDatePicker :formatted-value="dates[0]" type="date" value-format="yyyy-MM-dd" :disabled="disabled" :input-readonly="true" :clearable="false" aria-label="事件开始日期" @update:formatted-value="value=>updateDate(0,value)" /></label>
        <span>至</span>
        <label><span>结束日期</span><NDatePicker :formatted-value="dates[1]" type="date" value-format="yyyy-MM-dd" :disabled="disabled" :input-readonly="true" :clearable="false" aria-label="事件结束日期" @update:formatted-value="value=>updateDate(1,value)" /></label>
      </div>
      <span class="v2-event-range" aria-live="polite"><CalendarDays :size="14" aria-hidden="true" />{{ range.caption }}</span>
      <span class="v2-event-period-note">按事件发生时间 · 北京时间</span>
    </div>
    <slot />
  </div>
</template>

<script setup lang="ts">
import { NTabs, NTab, NDatePicker } from 'naive-ui';
import { CalendarDays, CalendarRange, History } from 'lucide-vue-next';
import type { EventPeriod, EventRange } from './eventPeriod';
const props = defineProps<{ modelValue: EventPeriod; month: string; dates: [string, string]; range: EventRange; disabled?: boolean }>();
const emit = defineEmits<{ 'update:modelValue': [value: EventPeriod]; 'update:month': [value: string]; 'update:dates': [value: [string, string]] }>();
const options = [{key:'week',label:'本周事件',icon:CalendarDays},{key:'month',label:'本月事件',icon:CalendarRange},{key:'history',label:'历史月份',icon:History},{key:'custom',label:'自定义',icon:CalendarRange}];
function select(value: string | number) { if (!props.disabled && options.some(option=>option.key===String(value))) emit('update:modelValue', value as EventPeriod); }
function updateMonth(value: string | null) { if (value) emit('update:month', value); }
function updateDate(index: number, value: string | null) { if (props.disabled || !value) return; const dates: [string, string] = [...props.dates]; dates[index] = value; emit('update:dates', dates); }
function onKey(event: KeyboardEvent, key: string) {
  if (props.disabled || !['ArrowLeft','ArrowRight','Home','End','Enter',' '].includes(event.key)) return;
  event.preventDefault();
  const index=options.findIndex(option=>option.key===key);
  const count=options.length;
  const next=event.key==='Home'?0:event.key==='End'?count-1:event.key==='ArrowLeft'?(index+count-1)%count:event.key==='ArrowRight'?(index+1)%count:index;
  const value=options[next]!.key;
  select(value);
  (event.currentTarget as HTMLElement).closest('[role=tablist]')?.querySelector<HTMLElement>(`[data-name="${value}"]`)?.focus();
}
</script>

<style scoped>
.v2-event-period { display:flex; align-items:center; flex-wrap:wrap; gap:14px 24px; padding:16px 0; border-block:1px solid var(--v2-border,#e1e6e5); }
.v2-event-period :deep(.n-tabs) { width:460px; max-width:100%; flex-shrink:0; }
.v2-event-period :deep(.n-tabs-tab__label) { display:flex; align-items:center; gap:7px; }
.v2-event-period :deep(.n-tabs-capsule) { transition-duration:350ms; transition-timing-function:cubic-bezier(.2,.8,.25,1.04); }
.v2-event-period :deep(.n-tabs-tab) { min-height:38px; border-radius:6px; }
.v2-event-period :deep(.n-tabs-tab:focus-visible) { outline:2px solid #0071e3; outline-offset:-2px; }
.v2-event-period-meta { display:flex; align-items:center; flex-wrap:wrap; gap:8px 14px; min-height:36px; }
.v2-event-period-meta :deep(.n-date-picker) { width:160px; }
.v2-event-custom-dates {display:flex;align-items:end;gap:8px;max-width:100%}
.v2-event-custom-dates>label {display:grid;gap:4px;min-width:0;color:#636366;font-size:12px}
.v2-event-custom-dates>span {padding-bottom:8px;color:#636366}
.v2-event-range { display:flex; align-items:center; gap:6px; color:#3a3a3c; font-variant-numeric:tabular-nums; font-size:13px; }
.v2-event-period-note { color:#636366; font-size:12px; }
@media(max-width:700px) {
  .v2-event-period { gap:10px; padding:12px 0; }
  .v2-event-period :deep(.n-tabs) { width:100%; }
  .v2-event-period :deep(.n-tabs-tab) { min-height:44px; }
  .v2-event-period-meta { gap:6px 10px; }
  .v2-event-period :deep(.n-tabs-tab__label) {gap:0}
  .v2-event-period :deep(.n-tabs-tab__label>svg) {display:none}
  .v2-event-custom-dates {width:100%}
  .v2-event-custom-dates>label {flex:1}
  .v2-event-custom-dates :deep(.n-date-picker) {width:100%}
}
@media(prefers-reduced-motion:reduce) { .v2-event-period :deep(.n-tabs-capsule) { transition-duration:100ms; } }
</style>
