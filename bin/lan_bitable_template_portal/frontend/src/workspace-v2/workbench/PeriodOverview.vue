<template>
  <section v-if="shownSources.length" class="wb-period" aria-label="工作状态">
    <header class="wb-period-header">
      <h2 :title="dateLabel+' · '+scopeLabel">工作状态</h2>
      <div class="wb-period-select" role="group" aria-label="统计周期">
        <button v-for="option in periods" :key="option.id" type="button" :aria-pressed="period === option.id"
          @click="period = option.id">{{ option.label }}</button>
      </div>
    </header>
    <div class="wb-period-grid" aria-live="polite">
      <article v-for="source in shownSources" :key="source.id" class="wb-period-card">
        <header>
          <div class="wb-period-topic"><component :is="icons[source.id]" :size="16" aria-hidden="true" /><h3>{{ source.label }}</h3></div>
          <div v-if="results[source.id]?.metric" class="wb-period-value"><strong>{{ results[source.id]?.metric?.value }}</strong><span>{{ results[source.id]?.metric?.unit }}</span></div>
          <button v-if="source.url" type="button" :aria-label="`打开${source.label}`"
            @click="$emit('open', source.url)"><ArrowUpRight :size="17" /></button>
        </header>
        <template v-if="results[source.id]?.metric">
          <div v-if="results[source.id]?.metric?.bars.length" class="wb-period-stats">
            <div v-for="bar in results[source.id]?.metric?.bars" :key="bar.label" class="wb-period-bar" :class="bar.tone">
              <span>{{ bar.label }}</span><b>{{ bar.value }}</b>
              <meter :value="bar.value" :max="bar.max || 1" :aria-label="bar.label" />
            </div>
          </div>
          <p v-if="results[source.id]?.metric?.note">{{ results[source.id]?.metric?.note }}</p>
        </template>
        <p v-if="results[source.id]?.error" class="wb-period-message">{{ results[source.id]?.error }}
          <button type="button" @click="load(source)">重试</button>
        </p>
        <p v-else-if="!results[source.id]" class="wb-period-message" role="status">正在读取…</p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onActivated, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from 'vue';
import { ArrowUpRight, BookOpenCheck, ClipboardList, Radio, Siren } from 'lucide-vue-next';
import { requestJson, type Dict } from '../client';
import { beijingDate, eventCalendarRange, eventPeriodRange, mergeEventMonths } from '../eventPeriod';
import { eventStatusTone } from '../../eventManagementUtils';
import type { Role, ScopeOption } from './recipes';
import { readWorkbenchRequest } from './useWorkbenchReads';

const props = defineProps<{ user: Dict; scope: string; scopes: ScopeOption[]; role: Role; readScopes?:string[] }>();
defineEmits<{ open: [url: string] }>();
const periods = [{ id: 'day', label: '今日' }, { id: 'week', label: '本周' }, { id: 'month', label: '本月' }] as const;
const icons={daily:ClipboardList, events:Siren, notices:Radio, learning:BookOpenCheck};
type Period = typeof periods[number]['id'];
type Bar = { label: string; value: number; max: number; tone?: 'ok' | 'warn' };
type Metric = { value: number | string; unit?: string; bars: Bar[]; note?: string };
type Source = { id: keyof typeof icons; label: string; url?: string; read: (signal: AbortSignal) => Promise<Metric> };
const period = ref<Period>('day');
const today = ref(beijingDate());
const range = computed(() => {
  const at = Date.parse(today.value + 'T12:00:00+08:00');
  return period.value === 'day' ? eventPeriodRange('custom', '', at, [today.value, today.value]) : eventCalendarRange(period.value, at);
});
const dateLabel = computed(() => period.value === 'day' ? today.value : range.value.caption);
const scopeLabel = computed(() => props.scopes.find(option => option.value === props.scope)?.label || '本人');
const results = ref<Record<string, { metric?: Metric; error?: string; denied?: boolean }>>({});
const requests = new Map<string, AbortController>();
const count = (value: unknown): number => {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) throw new Error('数据待核验');
  return value;
};
const read = (path: string, query: Record<string, string>, signal: AbortSignal) =>
  readWorkbenchRequest(path + '?' + new URLSearchParams(query), signal);

const sources = computed<Source[]>(() => {
  const grants: string[] = Array.isArray(props.user.modules) ? props.user.modules : [];
  if (['temporary', 'guest'].includes(props.user.role)) return [];
  const scope = props.scope, date = today.value, activeRange = range.value, selectedPeriod = period.value;
  const scoped = props.scopes.some(option => option.value === scope);
  const personal = props.role==='personal';
  const team = ['manager','supervisor'].includes(props.role);
  const eventScopes=props.readScopes||[scope];
  const rows: Source[] = [];
  if (personal && scoped && grants.includes('daily') && selectedPeriod === 'day') rows.push({
    id: 'daily', label: '每日任务', url: '/daily-tasks?' + new URLSearchParams({ scope, date }),
    async read(signal) {
      const data = await read('/api/daily-tasks', { scope, date }, signal);
      if (data.date !== date || data.scope !== scope || data.warnings?.length) throw new Error('任务数据待同步');
      const total = count(data.stats?.total);
      return { value: total, unit: '项任务', bars: [
        { label: '已完成', value: count(data.stats.completed), max: total, tone: 'ok' },
        { label: '进行中', value: count(data.stats.ongoing), max: total },
        { label: '需关注', value: count(data.stats.attention), max: total, tone: 'warn' },
      ] };
    },
  });
  if ((personal||team||props.role==='specialist') && scoped && grants.includes('event')) rows.push({
    id: 'events', label: '事件', url: props.readScopes?undefined:'/?' + new URLSearchParams({ scope, mode: 'events', detail: '1' }),
    async read(signal) {
      const batches = await Promise.all(eventScopes.flatMap(building=>activeRange.months.map(async month => {
        const batch=await read('/api/events/monthly', {scope:building,month,date_field:'occurrence_time'},signal);
        if (batch.scope!==building) throw new Error('事件数据待同步');
        return batch;
      })));
      if (batches.some(batch => batch.snapshot_exists !== true || batch.config_missing || Object.keys(batch.last_failed || {}).length)) {
        throw new Error('事件数据待同步');
      }
      const events = mergeEventMonths(batches, activeRange);
      const total = events.records.length;
      return { value: total, unit: '起事件', bars: [
        { label: '处理中', value: events.records.filter((row: Dict) => eventStatusTone(row.status) === 'processing').length, max: total, tone: 'warn' },
        { label: '已恢复', value: events.records.filter((row: Dict) => eventStatusTone(row.status) === 'recovered').length, max: total, tone: 'ok' },
        { label: 'I2 及以上', value: events.stats.i2_or_higher, max: total },
      ], note: events.unknown_time_count ? '部分时间待核对' : undefined };
    },
  });
  const noticeKinds = ['maintenance', 'change', 'repair'].filter(kind => grants.includes(kind));
  if (personal && scoped && noticeKinds.length) rows.push({
    id: 'notices', label: '通告办理记录', url: '/workbench-lite?' + new URLSearchParams({ scope, work_type: noticeKinds.length === 1 ? noticeKinds[0]! : 'all' }),
    async read(signal) {
      const workTypes = noticeKinds.length === 3 ? ['all'] : noticeKinds;
      const requests = activeRange.months.flatMap(month => workTypes.map(async work_type => ({
        month, data: await read('/api/history-summary', { scope, month, work_type }, signal),
      })));
      const values = { started: 0, updated: 0, ended: 0 };
      for (const { month, data } of await Promise.all(requests)) {
        if (data.scope !== scope || data.month !== month || !Array.isArray(data.days)) throw new Error('通告记录待核验');
        for (const day of data.days) {
          if (day.date < beijingDate(activeRange.start) || day.date > date) continue;
          for (const key of ['started', 'updated', 'ended'] as const) values[key] += count(day.stats?.[key]);
        }
      }
      const total = values.started + values.updated + values.ended;
      return { value: total, unit: '项状态记录', note: '同一通告的开始、更新、结束分别统计', bars: [
        { label: '开始', value: values.started, max: total }, { label: '更新', value: values.updated, max: total },
        { label: '结束', value: values.ended, max: total, tone: 'ok' },
      ] };
    },
  });
  if (team && scoped && /^(?:[A-EH]|ALL|CAMPUS)$/.test(scope) && grants.includes('learning') && !props.user.impersonation) rows.push({
    id:'learning',label:'画像学练情况汇总',
    async read(signal) {
      const data=await read('/api/learning/workbench-summary',{scope,from:beijingDate(activeRange.start),to:date},signal);
      const authorized=new Set(props.scopes.filter(option=>/^[A-EH]$/.test(option.value)).map(option=>option.value));
      if (!Array.isArray(data.scopes)||!data.scopes.length||data.scopes.some((building:string)=>
        ['ALL','CAMPUS'].includes(scope)
          ? (!authorized.has(building)&&!props.scopes.some(option=>option.value==='ALL'&&!props.readScopes))
          : building!==scope)) throw new Error('数据待核验');
      const values=['已完成','待答','答题中','逾期','待核对','未到时'].map(label=>({label,value:count(data.summary?.[label])}));
      const total=values.reduce((sum,item)=>sum+item.value,0);
      return {value:total,unit:'人天',bars:values.map(item=>({...item,max:total,tone:item.label==='已完成'?'ok':item.label==='逾期'||item.label==='待核对'?'warn':undefined})),
        note:selectedPeriod==='day'?undefined:'同一人不同排班日分别统计'};
    },
  });
  if ((personal||props.role==='duty'&&props.user.category!=='duty') && grants.includes('learning')) rows.push({
    id: 'learning', label: '个人学练', url: '/learning?view=practice',
    async read(signal) {
      const boot = await requestJson('/api/learning/bootstrap', { signal, timeoutMs: 12_000 });
      if (!boot.self_person?.id) return { value: '未关联', bars: [], note: '当前账号暂无个人学练身份' };
      if (selectedPeriod === 'day' && boot.daily_exemption?.status === 'exempt') return {
        value: '首月免做', bars: [], note: `${boot.daily_exemption.resume_date} 起按排班安排`,
      };
      const data = await read('/api/learning/profile', { scope: boot.self_scope || '', person_id: boot.self_person.id,
        from: beijingDate(activeRange.start), to: date }, signal);
      const summary = selectedPeriod === 'day' ? data.today_summary : data.summary;
      const done = count(summary?.task_answered), total = count(summary?.assigned);
      return { value: `${done} / ${total}`, unit: '题已答', bars: [{ label: '答题进度', value: done, max: Math.max(total, done), tone: 'ok' }],
        note: summary.task_label || (total ? undefined : '暂无已安排题目') };
    },
  });
  const order = team ? ['events', 'learning']
    : ['engineer', 'specialist'].includes(props.role) ? ['notices', 'events', 'daily', 'learning']
    : props.role === 'duty' ? ['daily', 'notices', 'events', 'learning'] : ['learning', 'daily', 'notices', 'events'];
  return rows.sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));
});
const shownSources=computed(()=>sources.value.filter(source=>!results.value[source.id]?.denied));
let activeView=true, refreshPending=false, refreshedAt=0;

async function load(source: Source) {
  if(!activeView||requests.has(source.id))return;
  const controller = new AbortController();
  requests.set(source.id, controller);
  const previous=results.value[source.id]?.metric;
  try {
    const metric = await source.read(controller.signal);
    if (!controller.signal.aborted) results.value[source.id] = { metric };
  } catch (error) {
    if (!controller.signal.aborted) {
      const denied = [401,403].includes((error as { status?: number })?.status || 0);
      const label = error instanceof Error && /待(同步|核验)/.test(error.message) ? error.message : '读取失败';
      results.value[source.id] = {denied, metric:denied?undefined:previous, error:previous?'汇总待更新':label};
    }
  } finally {
    if (requests.get(source.id) === controller) {
      requests.delete(source.id);
      if(!requests.size&&refreshPending)refresh();
    }
  }
}
function refresh(){
  if(!activeView||document.hidden||requests.size){refreshPending=true;return;}
  refreshPending=false;
  refreshedAt=Date.now();
  shownSources.value.forEach(source=>void load(source));
}
watch(()=>JSON.stringify([range.value.start,range.value.end,props.scope,props.readScopes,props.role,
  props.scopes,props.user.open_id,props.user.modules,props.user.level,props.user.category,props.user.impersonation]),()=>{
  requests.forEach(request=>request.abort());
  requests.clear();
  results.value={};
  refresh();
},{immediate:true});
function resume(){
  if(!activeView||document.hidden)return;
  const next=beijingDate();
  if(next!==today.value){today.value=next;return;}
  if(refreshPending||Date.now()-refreshedAt>=120000)refresh();
}
const dateTimer = window.setInterval(resume,60000);
onActivated(()=>{activeView=true;resume();});
onDeactivated(()=>{activeView=false;});
onMounted(()=>{
  window.addEventListener('clipflow-business-changed',refresh);
  document.addEventListener('visibilitychange',resume);
});
onBeforeUnmount(() => {
  requests.forEach(request => request.abort());
  window.clearInterval(dateTimer);
  window.removeEventListener('clipflow-business-changed', refresh);
  document.removeEventListener('visibilitychange',resume);
});
</script>

<style scoped>
.wb-period { display:grid;gap:9px;min-width:0;padding:11px;border:1px solid var(--soft-edge);border-radius:14px;background:var(--soft-face) }
.wb-period-header { display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:4px 9px }
.wb-period h2, .wb-period h3, .wb-period p { margin: 0; }
.wb-period h2 { font-size: 16px; }
.wb-period p { color: var(--v2-muted); font-size: 13px; line-height: 1.55; }
.wb-period-select { display:grid;grid-template-columns:repeat(3,auto);border:1px solid var(--v2-border);background:var(--soft-face);border-radius:999px }
.wb-period button { border: 0; color: inherit; background: none; cursor: pointer; }
.wb-period button:focus-visible { outline: 2px solid var(--wb-primary,var(--v2-blue)); outline-offset: -2px; }
.wb-period-select button { min-height:30px;padding:0 9px;border-radius:999px;font-size:12px;font-weight:600 }
.wb-period-select button[aria-pressed='true'] { color: var(--wb-primary,var(--v2-blue)); background: color-mix(in srgb,var(--wb-primary,var(--v2-blue)) 12%,transparent); }
.wb-period-grid { display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,235px),1fr));gap:8px;align-items:start }
.wb-period-card { display:grid;align-content:start;gap:8px;min-width:0;padding:9px 11px;background:var(--soft-field);border:1px solid var(--soft-edge);border-radius:10px }
.wb-period-card header { display:flex;align-items:center;gap:7px;min-height:28px }
.wb-period-topic { display:flex;align-items:center;gap:6px;min-width:0;margin-inline-end:auto }
.wb-period-topic svg { color:var(--v2-palette-accent-text);flex:none }
.wb-period-card h3 { font-size: 13px; }
.wb-period-card header button { display:grid;place-items:center;min-width:28px;min-height:28px;color:var(--wb-primary,var(--v2-blue)) }
.wb-period-value { display:flex;align-items:baseline;gap:4px;white-space:nowrap;line-height:1.12 }
.wb-period-value strong { font-size:clamp(16px,1.6vw,21px);font-variant-numeric:tabular-nums }
.wb-period-value span { color: var(--v2-muted); font-size: 12px; }
.wb-period-stats { display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,68px),1fr));gap:6px 9px }
.wb-period-bar { --bar-color:var(--wb-primary,var(--v2-blue));display:grid;grid-template-columns:minmax(0,1fr) auto;
  align-items:center;gap:2px 4px;font-size:12px }
.wb-period-bar.ok { --bar-color: var(--v2-green,#18835a); }
.wb-period-bar.warn { --bar-color: var(--v2-amber,#b66b24); }
.wb-period-bar b { min-width: 2ch; text-align: right; font-variant-numeric: tabular-nums; }
.wb-period-bar meter { grid-column:1/-1;width:100%;height:5px;appearance:none }
.wb-period-bar meter::-webkit-meter-bar { border: 0; background: var(--v2-border); }
.wb-period-bar meter::-webkit-meter-optimum-value { background: var(--bar-color); }
.wb-period-bar meter::-moz-meter-bar { background: var(--bar-color); }
.wb-period-message button { color: var(--wb-primary,var(--v2-blue)); min-height: 34px; text-decoration: underline; }
@media(max-width:560px) {
  .wb-period-select button,.wb-period-card header button,.wb-period-message button { min-height: 44px; }
  .wb-period-card { padding-inline: 10px; }
}
</style>
