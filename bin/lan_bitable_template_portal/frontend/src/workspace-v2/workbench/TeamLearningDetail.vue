<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { NModal } from 'naive-ui';
import { CalendarDays, ChevronLeft, ChevronRight } from 'lucide-vue-next';
import { readWorkbenchRequest } from './useWorkbenchReads';
import type { Dict } from '../client';

const props = defineProps<{ from: string; to: string; allowed: Set<string> }>();
const emit = defineEmits<{ close: []; summary: [value: Dict] }>();
type Row = { date: string; name: string; scopes: string[]; shift: string; status: string; answered: number; assigned: number };
type Filter = 'due' | 'pending' | 'done' | 'upcoming' | 'checking';
const bucket = (row: Row): Exclude<Filter, 'due'> => {
  if (row.status === '已完成') return 'done';
  if (row.status === '未到时') return 'upcoming';
  if (row.status === '待核对') return 'checking';
  return 'pending';
};
const validStates = ['已完成', '未到时', '待核对', '答题中', '待答', '逾期'];
const scope = ref(''), date = ref(''), filter = ref<Filter>('due'), unanswered = ref(false);
const rows = ref<Row[]>([]), buildings = ref<string[]>([]), loading = ref(true), error = ref(''), revision = ref(0);
const page = ref(1), size = 15;
const dayList = computed(() => {
  const days: string[] = [];
  for (let d = Date.parse(props.to); d >= Date.parse(props.from); d -= 86400000) {
    days.push(new Date(d).toISOString().slice(0, 10));
  }
  return days;
});
watch([scope, revision, () => props.from, () => props.to], async (_, __, cleanup) => {
  const request = new AbortController();
  cleanup(() => request.abort());
  rows.value = []; error.value = ''; loading.value = true;
  try {
    const query = new URLSearchParams({ scope: scope.value || 'ALL', from: props.from, to: props.to, people: '1' });
    const data = await readWorkbenchRequest('/api/learning/workbench-summary?' + query, request.signal);
    if (request.signal.aborted) return;
    if (data.from !== props.from || data.to !== props.to || !Array.isArray(data.scopes) || !data.scopes.length ||
        !data.scopes.every((b: string) => props.allowed.has(b) && (!scope.value || b === scope.value)) ||
        !Array.isArray(data.people) || !data.people.every((r: Row) => r && dayList.value.includes(r.date) &&
          typeof r.name === 'string' && Array.isArray(r.scopes) && r.scopes.every(b => data.scopes.includes(b)) &&
          validStates.includes(r.status) && [r.answered, r.assigned].every(n => Number.isSafeInteger(n) && n >= 0))) {
      throw new Error('名单范围或进度待核对');
    }
    if (!scope.value) {
      buildings.value = data.scopes;
      emit('summary', data.summary);
    }
    rows.value = data.people;
  } catch (cause) {
    if (!request.signal.aborted) error.value = [401,403].includes((cause as {status?:number}).status || 0)
      ? '当前账号无权查看学练明细' : '人员进度读取失败';
  } finally {
    if (!request.signal.aborted) loading.value = false;
  }
}, { immediate: true });
const byDate = computed(() => rows.value.filter(row => !date.value || row.date === date.value));
const counts = computed(() => {
  const n = { due:0, pending:0, done:0, upcoming:0, checking:0 };
  for (const row of byDate.value) n[bucket(row)]++;
  n.due = n.done + n.pending;
  return n;
});
const filters: { id: Filter; label: string }[] = [
  {id:'due', label:'全部应答'}, {id:'pending', label:'未完成'}, {id:'done', label:'已完成'},
  {id:'upcoming', label:'未到时'}, {id:'checking', label:'待核对'},
];
const visibleFilters = computed(() => filters.filter(f => !['upcoming','checking'].includes(f.id) || counts.value[f.id] || f.id === filter.value));
const priority = (row: Row) => row.status === '逾期' ? 0 : bucket(row) === 'pending' ? 1 : 2;
const filtered = computed(() => byDate.value.filter(row => {
  const group = bucket(row);
  if (filter.value === 'due') return group === 'pending' || group === 'done';
  return filter.value === group && !(group === 'pending' && unanswered.value && row.answered > 0);
}).sort((a,b) => priority(a)-priority(b) || b.date.localeCompare(a.date) || a.name.localeCompare(b.name,'zh-CN')));
const pages = computed(() => Math.max(1, Math.ceil(filtered.value.length / size)));
const visible = computed(() => filtered.value.slice((page.value-1)*size,page.value*size));
watch([date,scope,filter,unanswered,rows], () => { page.value = 1; });
const buildingName = (s: string) => s==='110'?'110站':s+'楼';
</script>

<template>
  <NModal :show="true" preset="card" title="画像学练情况汇总" class="team-learning-dialog"
    :bordered="false" @update:show="value => { if (!value) emit('close') }">
    <div class="learning-detail">
      <div class="detail-tools">
        <span class="period-label"><CalendarDays :size="15" aria-hidden="true" />{{ from }}<template v-if="from!==to"> 至 {{ to.slice(5) }}</template></span>
        <select v-model="scope" aria-label="学练明细楼栋" data-native-select>
          <option value="">全部授权楼栋</option><option v-for="b in buildings" :key="b" :value="b">{{ buildingName(b) }}</option>
        </select>
        <select v-if="from!==to" v-model="date" aria-label="学练明细日期" data-native-select>
          <option value="">全部日期</option><option v-for="d in dayList" :key="d" :value="d">{{ d.slice(5) }}</option>
        </select>
      </div>
      <div v-if="loading" class="detail-message" role="status">正在读取人员进度…</div>
      <div v-else-if="error" class="detail-message" role="alert">{{ error }} <button @click="revision++">重试</button></div>
      <template v-else>
        <div class="detail-filters" role="group" aria-label="学练明细状态">
          <button v-for="f in visibleFilters" :key="f.id" :aria-pressed="filter===f.id" :data-filter="f.id" @click="filter=f.id">
            <span>{{ f.label }}</span> <b>{{ counts[f.id] }}</b>
          </button>
        </div>
        <label v-if="filter==='pending'" class="only-unanswered"><input v-model="unanswered" type="checkbox">只看未作答</label>
        <div v-if="visible.length" class="detail-list">
          <table aria-label="逐人答题明细"><thead><tr><th>人员 / 班次</th><th>日期</th><th>已答 / 应答</th><th>状态</th></tr></thead>
            <tbody><tr v-for="(r,i) in visible" :key="i">
              <td><strong>{{ r.name || '姓名待核对' }}</strong><small>{{ [...r.scopes.map(buildingName), r.shift].filter(Boolean).join(' · ') }}</small></td>
              <td>{{ r.date.slice(5) }}</td>
              <td class="detail-number"><b>{{ r.answered }}</b> / {{ r.assigned }}</td>
              <td><span class="detail-status" :data-group="bucket(r)" :data-late="r.status==='逾期'">{{ r.status==='待答' && !r.answered ? '未作答' : r.status }}</span></td>
            </tr></tbody>
          </table>
        </div>
        <p v-else class="detail-message">{{ filter==='pending' ? '暂无符合条件的未完成人员' : '暂无符合条件的人员记录' }}</p>
        <footer><span>共 {{ filtered.length }} 人天</span>
          <div v-if="pages>1"><button aria-label="上一页人员" :disabled="page<=1" @click="page--"><ChevronLeft :size="16"/></button>
            <span>{{ page }} / {{ pages }}</span>
            <button aria-label="下一页人员" :disabled="page>=pages" @click="page++"><ChevronRight :size="16"/></button></div>
        </footer>
      </template>
    </div>
  </NModal>
</template>

<style scoped>
:global(.team-learning-dialog.n-card){width:min(660px,calc(100vw - 24px));max-height:calc(100dvh - 24px);overflow:hidden}
:global(.team-learning-dialog > .n-card__content),:global(.team-learning-dialog > .n-card-content){min-height:0;overflow:auto}
.learning-detail{color:var(--v2-ink);font-size:13px}
.detail-tools,.detail-filters,footer,footer div,.period-label{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.detail-tools{padding-top:10px}
.period-label{margin-right:auto;color:var(--v2-muted);font-variant-numeric:tabular-nums}
.detail-tools select,.learning-detail button{font:inherit;color:inherit;border:1px solid var(--soft-edge);border-radius:7px;background:var(--soft-face);padding:5px 9px;min-height:32px}
.learning-detail button{cursor:pointer}
.detail-filters{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(0,1fr);gap:5px;margin:12px 0}
.detail-filters button{display:flex;align-items:center;justify-content:center;gap:6px;min-width:0;background:var(--soft-field);border-color:transparent;font-size:12px}
.detail-filters button[aria-pressed=true]{background:var(--v2-palette-selection);color:var(--v2-palette-accent-text);border-color:var(--v2-palette-accent)}
.detail-filters b{font-size:16px;font-variant-numeric:tabular-nums}
.detail-filters [data-filter=pending] b{color:var(--cf-warning,#925510)}
.only-unanswered{display:inline-flex;align-items:center;margin-bottom:8px;gap:4px}
.detail-list{max-height:51dvh;overflow:auto;overscroll-behavior:contain;border-block:1px solid var(--soft-edge)}
table{width:100%;border-collapse:separate;border-spacing:0;text-align:left;font-variant-numeric:tabular-nums}
th,td{padding:8px 6px}th{font-size:12px;font-weight:550;color:var(--v2-muted);position:sticky;top:0;background:var(--soft-field);z-index:1}
td{border-top:1px solid var(--soft-edge)}tbody tr:first-child td{border-top:0}td strong,td small{display:block}td small{color:var(--v2-muted);font-size:11px}
th:nth-last-child(-n+2),td:nth-last-child(-n+2){text-align:right;white-space:nowrap}
.detail-number{color:var(--v2-muted)}.detail-number b{color:var(--v2-ink)}
.detail-status{display:inline-block;padding:2px 7px;border-radius:5px;background:var(--soft-field);color:var(--v2-muted);font-size:12px}
.detail-status[data-group=done]{color:var(--cf-success,#18704c);background:color-mix(in srgb,currentColor 10%,var(--soft-face))}
.detail-status[data-group=pending]{color:var(--cf-warning,#925510);background:color-mix(in srgb,currentColor 10%,var(--soft-face))}
.detail-status[data-late=true]{color:var(--cf-danger,#b33532)}
footer{justify-content:space-between;margin-top:8px;color:var(--v2-muted);font-size:12px}
footer button{display:grid;place-items:center}footer button:disabled{opacity:.4;cursor:default}
.detail-message{padding:18px 0;color:var(--v2-muted);text-align:center}
@media(max-width:540px){.period-label{flex-basis:100%}.detail-tools select,.detail-filters button,footer button{min-height:40px}.detail-tools{gap:5px}td,th{padding:7px 3px}.detail-list{max-height:46dvh}}
@media(max-width:540px){.detail-filters button{flex-direction:column;gap:2px;padding:5px 1px}.detail-filters span{white-space:nowrap}}
</style>
