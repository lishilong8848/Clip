<script setup lang="ts">
import { computed, defineAsyncComponent, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ArrowUpRight, Building2, CalendarDays, RefreshCw } from 'lucide-vue-next';
import { usePageReadRefresh } from '../api/usePageReadRefresh';
import { requestJson, type Dict } from './client';
import { validWaterDate, waterBuildingTrend, waterDay, waterNumberText, waterScopes, waterTrend } from './water';

const WaterBuildingTrend = defineAsyncComponent(() => import('./WaterBuildingTrend.vue'));
const props = defineProps<{ scope: string; scopeOptions: { value: string; label: string }[]; initialDay: string }>();
const emit = defineEmits<{ open: [context: { scope: string; startDate: string; endDate: string; frequency: '日'; selectedDay: string }] }>();
const available = computed(() => props.scopeOptions.filter(item => waterScopes.has(item.value)));
const end = ref(waterDay());
const start = computed(() => end.value.slice(0, 7) + '-01');
const initial = props.initialDay;
const day = ref(validWaterDate(initial) && initial >= start.value && initial <= end.value ? initial : '');
const period = ref<Dict[] | null>(null);
const loading = ref(false);
const error = ref('');
let read: AbortController | undefined;
let disposed = false;

async function load(fresh = false) {
  read?.abort();
  if (disposed || document.hidden || !available.value.length) return;
  const today = waterDay();
  if (today.slice(0, 7) !== end.value.slice(0, 7)) { period.value = null; day.value = ''; }
  end.value = today;
  const current = new AbortController();
  read = current;
  error.value = '';
  loading.value = true;
  try {
    const groups = await Promise.all(available.value.map(async building => {
      const rows: Dict[] = [];
      for (let page = 1; ; page++) {
        const query = new URLSearchParams({ scope: building.value, start_date: start.value, end_date: end.value, frequency: '日', page: String(page), page_size: '200' });
        const data = await requestJson('/api/capacity/water/records?' + query, { signal: current.signal, fresh });
        if (!Array.isArray(data.records) || !Number.isInteger(data.total) || data.total < 0 || data.snapshot?.exists !== true) throw new Error(building.label + '本月记录尚未完整取得。');
        if (data.total > 10000) throw new Error(building.label + '记录过多，请进入记录办理缩小日期范围。');
        rows.push(...data.records.map((record: Dict) => ({ ...record, scope: building.value })));
        if (rows.length >= data.total) return rows;
        if (!data.records.length) throw new Error(building.label + '分页数据不完整。');
      }
    }));
    if (!current.signal.aborted) period.value = groups.flat();
  } catch (cause) {
    if (!current.signal.aborted) error.value = cause instanceof Error ? cause.message : '水耗汇总读取失败。';
  } finally {
    if (read === current) loading.value = false;
  }
}
const trend = computed(() => waterBuildingTrend(period.value || [], available.value, start.value, end.value));
function dateOf(record: Dict) { return String(record.statistic_date_key || record.statistic_date || '').slice(0, 10); }
function usage(records: Dict[]) {
  const daily = waterTrend(records);
  return !records.length || daily.missing ? null : daily.items.reduce((sum, item) => sum + item.value, 0);
}
const monthUsage = computed(() => usage(period.value || []));
function latestDate(records: Dict[]) {
  return records.reduce((last, record) => {
    const date = dateOf(record);
    return validWaterDate(date) && date > last ? date : last;
  }, '');
}
const latest = computed(() => latestDate(period.value || []));
const buildings = computed(() => available.value.map(building => {
  const records = (period.value || []).filter(record => record.scope === building.value && (!day.value || dateOf(record) === day.value));
  return { ...building, count: period.value ? records.length : null, usage: usage(records), latest: latestDate(records) };
}));
function selectDay(value: string) {
  if (!value || validWaterDate(value) && value >= start.value && value <= end.value) day.value = value;
}
function open(scope: string) {
  if (available.value.some(item => item.value === scope)) {
    emit('open', { scope, startDate: day.value || start.value, endDate: day.value || end.value, frequency: '日', selectedDay: day.value });
  }
}
function visibility() {
  if (document.hidden) read?.abort();
  else if (!period.value || end.value !== waterDay()) void load(true);
}
watch(() => [props.scope, ...available.value.map(item => item.value)], () => {
  period.value = null;
  day.value = '';
  void load();
});
usePageReadRefresh(url => url.pathname.startsWith('/api/capacity/water/'), () => load(), () => !loading.value);
onMounted(() => { void load(); document.addEventListener('visibilitychange', visibility); });
onBeforeUnmount(() => { disposed = true; read?.abort(); document.removeEventListener('visibilitychange', visibility); });
</script>

<template>
  <div class="desk-overview water-overview">
    <div class="overview-topline">
      <span><CalendarDays :size="16" aria-hidden="true"/>{{ start }} 至 {{ end }}</span>
      <button class="water-tool" type="button" :disabled="loading || !available.length" @click="load(true)">
        <RefreshCw :size="15" aria-hidden="true"/>{{ loading ? '正在读取' : '刷新总览' }}
      </button>
    </div>
    <p v-if="error" role="alert" class="overview-error">汇总暂不可用 · {{ error }}<span v-if="period">（显示上次读取结果）</span></p>
    <section class="overview-grid">
      <div class="overview-analysis">
        <dl class="water-overview-metrics overview-numbers" aria-label="本月日统计汇总">
          <div><dt>本月日统计耗水量</dt><dd>{{ waterNumberText(monthUsage) }} <small>t</small></dd></div>
          <div><dt>日统计记录</dt><dd>{{ waterNumberText(period?.length) }} <small>条</small></dd></div>
          <div><dt>最近记录日期</dt><dd class="last-date">{{ latest || '—' }}</dd></div>
        </dl>
        <section class="overview-chart water-trend-surface">
          <WaterBuildingTrend :data="trend" :loading="loading && !period" :failed="!!error && !period" @select="selectDay"/>
        </section>
      </div>
      <section class="building-book" aria-label="按楼栋办理">
        <header><Building2 :size="19" aria-hidden="true"/><h2>按楼栋办理</h2>
        <label class="day-field">记录范围
          <select :value="day" @change="selectDay(($event.target as HTMLSelectElement).value)">
            <option value="">本月全部日期</option>
            <option v-for="date in [...trend.days].reverse()" :key="date" :value="date">{{ date }}</option>
          </select>
        </label></header>
        <ul class="building-list">
          <li v-for="building in buildings" :key="building.value">
            <button type="button" class="building-entry water-building-row" data-v2-navigation
              :aria-label="'查看' + building.label + '水耗记录'" @click="open(building.value)">
              <span class="entry-name"><strong>{{ building.label }}</strong><small>{{ waterNumberText(building.count) }} 条日记录</small><small v-if="building.latest">最近 {{ building.latest }}</small></span>
              <span class="entry-usage">{{ waterNumberText(building.usage) }} <small>t</small></span>
              <ArrowUpRight :size="17" aria-hidden="true"/>
            </button>
          </li>
        </ul>
        <p v-if="!buildings.length" role="status">当前账号暂无授权楼栋。</p>
      </section>
    </section>
  </div>
</template>

<style scoped>
.desk-overview { display: grid; gap: 12px; min-width: 0; }
.overview-topline,.overview-topline span,.water-tool,.building-book header { display: flex; align-items: center; gap: 8px; }
.overview-topline { justify-content: space-between; color: var(--water-muted); font-size: .85rem; }
.water-tool,.day-field select { color: inherit; font: inherit; background: var(--water-face); border: 1px solid var(--water-line); border-radius: 9px; padding: 8px 12px; }
.water-tool,.building-entry,.day-field select { cursor: pointer; }
.water-tool:disabled { opacity: .55; cursor: wait; }
.overview-grid { display: grid; grid-template-columns: minmax(0, 2fr) minmax(235px, .83fr); gap: 16px; align-items: start; }
.overview-analysis,.building-book { min-width: 0; }
.overview-analysis,.building-book { background: var(--water-face); border: 1px solid var(--water-line); border-radius: 18px; box-shadow: var(--soft-raised, 3px 4px 12px #273f5710); }
.overview-numbers { margin: 0; padding: 20px; display: grid; grid-template-columns: 1.35fr .8fr 1fr; align-items: end; gap: 12px; border-radius: 18px 18px 0 0; background: var(--water-wash); }
.overview-numbers dt,.entry-name small,.day-field,.building-book p { font-size: .82rem; color: var(--water-muted); }
.overview-numbers dd { margin: 6px 0 0; font-size: clamp(1.25rem, 2.7vw, 2.1rem); font-weight: 650; line-height: 1.2; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.overview-numbers dd small,.entry-usage small { font-size: .8rem; font-weight: 500; }
.overview-numbers .last-date { font-size: clamp(.95rem, 1.6vw, 1.28rem); }
.overview-chart { padding: 14px; min-width: 0; }
.overview-chart :deep(.v2-chart-canvas) { min-width: 0; width: 100%; height: clamp(210px, 28vh, 310px); }
.building-book { padding: 16px; }
.building-book h2 { margin: 0; font-size: 1.05rem; }
.building-book header { flex-wrap: wrap; margin-bottom: 8px; }
.day-field { display: flex; align-items: center; gap: 5px; margin-inline-start: auto; font-size: .82rem; white-space: nowrap; }
.day-field select { min-width: 0; width: auto; color: var(--water-ink); background: var(--water-field); }
.building-list { list-style: none; margin: 0; padding: 0; }
.building-list li + li { border-top: 1px solid var(--water-line); }
.building-entry { display: grid; grid-template-columns: minmax(0,1fr) auto auto; align-items: center; width: 100%; padding: 12px 4px; gap: 8px; border: 0; background: transparent; color: inherit; text-align: left; font: inherit; }
.building-entry:hover,.building-entry:focus-visible { background: var(--water-field); }
.entry-name { display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 8px; overflow-wrap: anywhere; }
.entry-usage { font-variant-numeric: tabular-nums; font-weight: 650; }
.building-entry svg { box-sizing: content-box; padding: 5px; border-radius: 50%; background: #137351; color: white; }
.overview-error { padding: 10px 12px; margin: 0; border-inline-start: 3px solid #b36926; }
@media (max-width: 980px) { .overview-grid { grid-template-columns: 1fr; } }
@media (max-width: 600px) {
  .overview-topline { flex-wrap: wrap; }
  .water-tool,.day-field select { min-height: 44px; }
  .overview-numbers { padding: 14px; grid-template-columns: 1fr 1fr; }
  .overview-numbers > :first-child { grid-column: 1 / -1; }
  .overview-chart { padding: 8px; }
}
</style>
