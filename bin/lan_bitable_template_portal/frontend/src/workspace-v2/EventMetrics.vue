<template>
  <div class="v2-event-metrics" :aria-busy="loading">
    <component :is="card.clickable ? 'button' : 'div'" v-for="card in cards" :key="card.key" :type="card.clickable ? 'button' : undefined"
      class="v2-event-metric" :class="card.tone" :disabled="card.clickable && (loading || failed)" :data-metric="card.key"
      :aria-label="card.clickable ? `查看${card.label}记录` : undefined" @click="card.clickable && !loading && !failed && $emit('select', card.key)">
      <component :is="icons[card.key]" :size="18" class="v2-event-metric-icon" aria-hidden="true" />
      <span>{{ card.label }}</span>
      <div class="v2-event-metric-value">
        <NSkeleton v-if="loading" width="64px" height="34px" :sharp="false" />
        <strong v-else-if="failed" title="数据未取得">—</strong>
        <NStatistic v-else><NNumberAnimation :from="previous[card.key] || 0" :to="Number(card.value)" :duration="reduced.matches ? 0 : 500" /><template #suffix>{{ card.unit }}</template></NStatistic>
      </div>
      <small>{{ card.badge }}</small>
      <ChevronRight v-if="card.clickable" :size="15" class="v2-event-metric-arrow" aria-hidden="true" />
    </component>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';
import { NSkeleton, NStatistic, NNumberAnimation } from 'naive-ui';
import { Activity, Wrench, ShieldAlert, CircleAlert, ChevronRight } from 'lucide-vue-next';
const props=defineProps<{ cards: Array<{key:string; label:string; value:string|number; unit:string; badge:string; tone:string; clickable?:boolean}>; loading:boolean; failed:boolean }>();
const previous=ref<Record<string,number>>({}),reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
watch(()=>props.loading,value=>{if(value&&!props.failed)previous.value=Object.fromEntries(props.cards.map(card=>[card.key,Number(card.value)]));});
defineEmits<{select:[key:string]}>();
const icons: Record<string, typeof Activity> = {total:Activity,repairing:Wrench,i2plus:ShieldAlert,i3:CircleAlert};
</script>

<style scoped>
.v2-event-metrics { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; }
.v2-event-metric { position:relative; display:grid; grid-template-columns:22px 1fr; gap:6px 8px; min-width:0; padding:16px; border:1px solid #e5e5ea; border-radius:8px; background:#fff; color:#3a3a3c; text-align:left; font:inherit; transition:background 180ms ease-out,border-color 180ms ease-out,transform 180ms ease-out; }
.v2-event-metric>span { font-size:13px; }
button.v2-event-metric:not(:disabled) { cursor:pointer; }
button.v2-event-metric:not(:disabled):hover { border-color:#a8c6f0; background:#f5f8ff; transform:translateY(-1px); }
button.v2-event-metric:not(:disabled):active { transform:scale(.98); }
button.v2-event-metric:focus-visible { outline:2px solid #0071e3; outline-offset:2px; }
.v2-event-metric:disabled { opacity:1; cursor:default; }
.v2-event-metric-icon { color:#2879ab; }.emerald .v2-event-metric-icon{color:#0071e3}.amber .v2-event-metric-icon{color:#a66b16}.rose .v2-event-metric-icon{color:#c24c58}
.v2-event-metric-value { grid-column:1/-1; min-height:40px; display:flex; align-items:center; }
.v2-event-metric-value :deep(.n-statistic-value__content),.v2-event-metric-value>strong { font-size:30px; font-weight:650; font-variant-numeric:tabular-nums; line-height:1.3; }
.v2-event-metric-value :deep(.n-statistic-value__suffix) { font-size:12px; color:#636366; }
.v2-event-metric small { grid-column:1/-1; font-size:12px; color:#636366; padding-right:14px; }
.v2-event-metric-arrow { position:absolute; right:14px; bottom:17px; color:#636366; }
.v2-event-metrics[aria-busy=true] :deep(.n-skeleton) { opacity:0; animation:metric-wait 150ms ease-out 300ms forwards; }
@keyframes metric-wait {to{opacity:1}}
@media(max-width:850px) { .v2-event-metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.v2-event-metric{padding:12px} }
@media(max-width:760px) { .v2-event-metric{padding:10px;gap:3px 6px}.v2-event-metric small{display:none}.v2-event-metric-value{min-height:30px}.v2-event-metric-value :deep(.n-statistic-value__content),.v2-event-metric-value>strong{font-size:25px} }
@media(prefers-reduced-motion:reduce) { .v2-event-metric{transition:none}.v2-event-metrics :deep(.n-skeleton){animation:none;opacity:1} }
</style>
