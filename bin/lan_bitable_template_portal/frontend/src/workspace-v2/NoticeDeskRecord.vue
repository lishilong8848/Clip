<template>
  <article class="desk-record" :class="{ 'is-ongoing': ongoing }"
    @click="activateCard($event) && emit('open')">
    <div v-if="!ongoing" class="schedule-date">
      <template v-if="startDate"><small>{{ startDate.month }}月</small><b>{{ startDate.day }}</b></template>
      <template v-else><CalendarDays :size="18" /><small>{{ planWindow.start ? '计划' : '待定' }}</small></template>
    </div>
    <header class="record-heading">
      <span class="record-location"><MapPin :size="14" />{{ building }}</span>
      <span class="record-status" :data-tone="status.tone"><i aria-hidden="true"></i>{{ status.label }}</span>
      <div v-if="ongoing" class="record-actions">
          <button type="button" v-if="!source.bound" class="bind-action" data-v2-navigation @click="emit('bind')"><Link2 :size="14" />绑定源表</button>
          <button type="button" data-v2-navigation @click="emit('open', 'end')">结束</button>
          <button type="button" class="primary-action" data-v2-navigation @click="emit('open', 'update')">更新<ArrowUpRight :size="14" /></button>
      </div>
    </header>
    <h3><button type="button" class="record-link" data-v2-navigation @click="emit('open')">{{ title }}</button></h3>
    <div v-if="!ongoing" class="record-actions plan-action">
      <button type="button" :class="{ 'primary-action': !ended && !unknown }" data-v2-navigation
        :disabled="unknown" @click="emit('open', started ? '' : 'start')">
        {{ unknown ? '请重新读取' : ended ? '查看' : started ? '核验' : '开始' }}<ArrowRight v-if="!unknown" :size="14" />
      </button>
    </div>
    <div class="record-meta">
      <span>{{ type }}</span>
      <span v-if="specialty">{{ specialty }}</span>
      <span v-if="cycle">{{ cycle }}</span>
      <span v-if="ongoing">{{ source.label }}</span>
    </div>
    <div v-if="!ongoing || planWindow.start || planWindow.end" class="record-window">
      <CalendarDays :size="15" aria-hidden="true" />
      <span>计划时段</span>
      <span class="time-range"><span>{{ planWindow.start || '开始时间未填' }}</span><span aria-label="至"> → </span><span>{{ planWindow.end || '结束时间未填' }}</span></span>
    </div>
    <footer v-if="(ongoing && photoCount!==null) || attention">
      <small v-if="ongoing && photoCount !== null" class="record-photos"><Camera :size="14" />现场照片 {{ photoCount }} 张</small>
      <p v-if="attention" class="record-note"><CircleAlert :size="14" />{{ attention }}</p>

    </footer>
  </article>
</template>
<script setup lang="ts">
import { computed } from 'vue';
import { ArrowRight, ArrowUpRight, CalendarDays, Camera, CircleAlert, Link2, MapPin } from 'lucide-vue-next';
import type { Dict } from './client';
import { activateCard } from './cardActivation';
import { noticeListCycle, noticeListSpecialty, noticeListStatus, noticePlanWindow, noticeSourceMode } from './noticeListMeta';

const props = defineProps<{
  row: Dict; lane: string; title: string; type: string; building: string;
  attention: string; ended: boolean; started: boolean; unknown: boolean;
}>();
const emit = defineEmits<{ open: [action?: string]; bind: [] }>();
const ongoing = computed(() => props.lane === 'ongoing');
const status = computed(() => noticeListStatus(props.row, props.lane));
const planWindow = computed(() => noticePlanWindow(props.row));
const startDate = computed(() => {
  const match = /^(?:\d{4})-(\d{2})-(\d{2})(?:\s|$)/.exec(planWindow.value.start);
  return match ? { month: Number(match[1]), day: match[2] } : null;
});
const photoCount = computed(() => {
  const value = props.row.site_photo_count;
  if (!['maintenance','change','repair'].includes(props.row.work_type) || value == null || value === '') return null;
  const count = Number(value);
  return Number.isInteger(count) && count >= 0 ? count : null;
});
const specialty = computed(() => noticeListSpecialty(props.row));
const cycle = computed(() => noticeListCycle(props.row));
const source = computed(() => noticeSourceMode(props.row));
</script>
<style scoped>
.desk-record {
  box-sizing:border-box; min-width:0; height:100%; display:flex; flex-direction:column; gap:8px;
  padding:14px 16px; background:var(--soft-face); border:1px solid var(--soft-edge);
  border-radius:13px; color:var(--v2-ink); cursor:pointer;
  transition:border-color .16s ease, box-shadow .16s ease;
}
.desk-record:hover,.desk-record:focus-within {border-color:var(--v2-blue);box-shadow:var(--soft-key)}
.desk-record:not(.is-ongoing) {
  display:grid;grid-template-columns:40px minmax(0,1fr) auto;align-content:start;gap:3px 9px;
  border:0;border-radius:0;border-bottom:1px solid var(--soft-edge);padding:9px 3px;
  background:transparent;
}
.desk-record:not(.is-ongoing) h3 {grid-area:1 / 2}
.desk-record:not(.is-ongoing) .record-heading {grid-area:2 / 2;justify-content:flex-start}
.plan-action {grid-area:1 / 3 / span 2;align-self:start}
.desk-record:not(.is-ongoing) > :is(.record-meta,.record-window,footer) {grid-column:2 / -1}
.desk-record:not(.is-ongoing):hover,.desk-record:not(.is-ongoing):focus-within {background:var(--soft-field);box-shadow:none}
.schedule-date {grid-row:1 / span 4;align-self:start;display:grid;justify-items:center;gap:2px;padding:8px 3px;border-radius:10px;
  color:var(--v2-ink);background:var(--desk-sky);font-variant-numeric:tabular-nums}
.schedule-date small {font-size:11px;font-weight:600}.schedule-date b {font-size:22px;line-height:1.1}
.record-photos {display:inline-flex;align-items:center;gap:4px;color:var(--v2-muted);font-size:12px}
.desk-record:not(.is-ongoing) .record-window {padding:0;background:none}
.record-heading,footer,.record-actions,.record-meta,.record-window,.record-location,.record-status,.record-note {
  display:flex; align-items:center; flex-wrap:wrap; min-width:0;
}
.record-heading {justify-content:space-between;gap:6px}
.record-location,.record-meta,.record-window,.record-note {font-size:12px;line-height:1.5;color:var(--v2-muted);overflow-wrap:anywhere}
.record-location,.record-note,.record-window {gap:5px}
.record-status {gap:6px;font-size:12px;line-height:1.4;font-weight:600}
.record-status i {width:7px;height:7px;border-radius:50%;background:var(--v2-blue)}
.record-status[data-tone="info"] i {background:var(--desk-green)}
.record-status[data-tone="warning"] i {background:#bd7c25}
.record-status[data-tone="error"] i {background:#be4c45}
h3 {margin:0;line-height:1.45}
.record-link {font:inherit;font-size:15px;font-weight:650;text-align:start;background:transparent;color:inherit;border:0;padding:0;cursor:pointer;overflow-wrap:anywhere}
.record-link:hover {text-decoration:underline;text-underline-offset:3px}
.record-meta {gap:4px 0}
.record-meta span+span::before {content:'·';margin-inline:7px;color:var(--v2-muted)}
.record-window {padding:7px 9px;border-radius:7px;background:var(--soft-field);font-variant-numeric:tabular-nums}
.time-range {display:inline-flex;flex-wrap:wrap;gap:2px 5px;color:var(--v2-ink)}
footer {margin-top:auto;gap:6px;justify-content:space-between}
.record-actions {gap:6px;margin-inline-start:auto}
.record-note {margin:0;color:var(--v2-ink)}
.record-note svg {color:#b07a25;flex:none}
.record-actions button {min-height:32px;padding:4px 10px;border-radius:8px;border:1px solid var(--soft-edge);color:var(--v2-ink);background:var(--soft-face);font:inherit;font-size:12px;font-weight:550;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:4px}
.record-actions .primary-action {background:var(--desk-green);color:white;border-color:transparent}
.record-actions .bind-action {background:transparent;border-color:transparent;color:var(--v2-blue)}
.record-actions button:hover:enabled {box-shadow:var(--soft-key)}
.record-actions button:disabled {cursor:default;opacity:.65}
.desk-record button:focus-visible {outline:2px solid var(--v2-blue);outline-offset:2px}
.is-ongoing {background:linear-gradient(120deg,var(--desk-mint),var(--soft-face) 52%)}
@media(pointer:coarse),(max-width:760px){.record-actions button {min-height:44px}.record-link {min-height:32px}}
@media(prefers-reduced-motion:reduce){.desk-record{transition:none}}
</style>
