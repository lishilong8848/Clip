<template>
  <section class="completion" aria-label="每日完成情况" :aria-busy="loading">
    <h2>每日完成</h2>
    <form class="completion-filters" @submit.prevent="reload()">
      <label>开始日期<input v-model="filters.from" type="date" required :max="filters.to" @input="followToday = false" /></label>
      <label>结束日期<input v-model="filters.to" type="date" required :min="filters.from" @input="followToday = false" /></label>
      <label for="completion-state">完成状态<VnetSelect v-model="filters.status" input-id="completion-state" :options="stateOptions" /></label>
      <label for="completion-shift">班次<VnetSelect v-model="filters.shift" input-id="completion-shift" :options="shiftOptions" /></label>
      <label v-if="canViewBuildings" class="search">姓名<input v-model="filters.q" type="search" maxlength="80" /></label>
      <button type="submit" :disabled="loading"><Search :size="16" />查询</button>
      <button type="button" class="icon-button" title="刷新完成情况" aria-label="刷新完成情况" :disabled="loading" @click="load()"><RefreshCw :size="17" :class="{ spin: loading }" /></button>
    </form>
    <div v-if="dateError || error" class="completion-error" role="alert"><AlertCircle :size="17" />{{ dateError || error }}</div>
    <div v-if="hasResult" class="completion-summary" aria-label="完成状态汇总"><span v-for="state in states" :key="state">{{ state }}<strong>{{ result.summary?.[state] || 0 }}</strong></span></div>
    <div v-if="loading" class="completion-loading" role="status"><Loader2 :size="17" class="spin" />正在读取完成情况</div>
    <div class="completion-table" tabindex="0" aria-label="每日完成记录">
      <table>
        <thead><tr><th>日期 / 姓名</th><th>楼栋 / 班次</th><th>完成状态</th><th>应答 / 已答 / 正确</th><th>首次客观正确率</th><th>看答案后数</th><th>主动练习</th><th>复习次数</th><th>发送状态</th><th>更新时间</th></tr></thead>
        <tbody><tr v-for="row in result.items || []" :key="row.id">
          <td><time>{{ row.date }}</time><strong>{{ row.person.name || '未填写' }}</strong><details v-if="row.source_papers?.length" class="completion-records"><summary>题单与作答记录</summary><ul><li v-for="paperId in row.source_papers" :key="paperId"><button type="button" @click="emit('open-paper', { paperId, person: row.person, date: row.date })"><BookOpen :size="14" />{{ paperTitle(row, paperId) }}</button></li></ul></details></td>
          <td>{{ row.scopes.map((s: string) => `${s}楼`).join('、') || '未关联' }}<small>{{ shifts(row) }}</small></td>
          <td><span class="completion-state" :class="tone(row.status)">{{ row.status }}</span><details v-if="row.tasks.length"><summary>任务 {{ row.tasks.length }}</summary><ul><li v-for="task in row.tasks" :key="task.task_id"><span>{{ task.source === 'sharing' ? '分享会' : '每日题单' }} {{ task.shift ? `${task.shift}班` : '' }}</span><span>{{ task.status }} · {{ task.answered }}/{{ task.assigned ?? '待核对' }}</span></li></ul></details></td>
          <td class="numbers">{{ row.assigned }} / {{ row.answered }} / {{ row.correct }}</td>
          <td class="numbers">{{ row.accuracy == null ? '暂无客观作答' : `${row.accuracy.toFixed(1)}%` }}</td>
          <td class="numbers">{{ row.answer_viewed }}<small v-if="row.answer_view_unknown">{{ row.answer_view_unknown }} 题证据待核对</small></td>
          <td class="numbers">{{ row.practice_questions }} 题<small>{{ row.practice_attempts }} 次作答</small></td>
          <td class="numbers">{{ row.review_count }}</td>
          <td><span :class="{ failure: row.delivery_failed }">{{ delivery(row) }}</span></td>
          <td><time>{{ timeLabel(row.updated_at) }}</time></td>
        </tr></tbody>
      </table>
      <div v-if="!loading && !dateError && !error && !(result.items || []).length" class="completion-empty">暂无符合条件的记录</div>
    </div>
    <footer v-if="hasResult" class="completion-pagination"><span>共 {{ result.total || 0 }} 条</span><div><button type="button" class="icon-button" title="上一页" aria-label="上一页" :disabled="loading || page <= 1" @click="turn(-1)"><ChevronLeft :size="18" /></button><span>{{ page }} / {{ pageCount }}</span><button type="button" class="icon-button" title="下一页" aria-label="下一页" :disabled="loading || page >= pageCount" @click="turn(1)"><ChevronRight :size="18" /></button></div></footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { AlertCircle, BookOpen, ChevronLeft, ChevronRight, Loader2, RefreshCw, Search } from 'lucide-vue-next';
import { getLearningCompletion } from '../api/learning';
import { ApiError, type Dict } from '../api/client';
import { completionDateError } from '../learningDates';
import { beijingDate } from '../workspace-v2/eventPeriod';
import VnetSelect from './VnetSelect.vue';

const props = defineProps<{ scope?: string; personId?: string; canViewBuildings?: boolean; refreshKey?: number }>();
const emit=defineEmits<{ 'open-paper': [value:{paperId:string;person:Dict;date:string}] }>();
const states = ['未安排', '未到时', '待答', '答题中', '已完成', '逾期', '待核对'];
const stateOptions = ['全部状态', ...states];
const shiftOptions = ['全部班次', '白班', '夜班'];
let today = beijingDate(), followToday = true;
const filters = reactive({ from: today, to: today, status: '全部状态', shift: '全部班次', q: '' });
const dateError = computed(() => completionDateError(filters.from, filters.to));
const result = ref<Dict>({ items: [], summary: {}, total: 0 });
const hasResult=ref(false);
const page = ref(1), loading = ref(false), error = ref('');
const pageCount = computed(() => Math.max(1, Math.ceil((result.value.total || 0) / 30)));
let request: AbortController | undefined;
let todayTimer: number | undefined;
function shifts(row: Dict): string { return [...new Set(row.tasks.map((t: Dict) => t.shift).filter(Boolean))].map(s => `${s}班`).join('、') || '未安排班次'; }
function delivery(row: Dict): string { return [...new Set(row.tasks.map((t: Dict) => t.delivery_status).filter(Boolean))].join('、') || '未记录'; }
function tone(state: string): string { return state === '已完成' ? 'success' : ['逾期', '待核对'].includes(state) ? 'warning' : ''; }
function paperTitle(row:Dict,id:string):string{const task=row.tasks.find((task:Dict)=>task.source==='paper'&&task.task_id===id);return task?`${task.shift?task.shift+'班 ':''}每日题单 ${task.answered}/${task.assigned ?? '待核对'}`:'练习与复习记录';}
function timeLabel(value: string): string { return new Date(value).toLocaleString('zh-CN', { hour12: false }); }
async function load(): Promise<void> {
  const currentDay = beijingDate();
  if (followToday && currentDay !== today) {
    today = currentDay;
    filters.from = today; filters.to = today; page.value = 1;
  }
  request?.abort();
  if (dateError.value) { request = undefined; loading.value = false; return; }
  const controller = new AbortController(); request = controller;
  loading.value = true; error.value = '';
  try {
    const response = await getLearningCompletion({ ...filters, status: filters.status === '全部状态' ? '' : filters.status,
      shift: filters.shift === '全部班次' ? '' : filters.shift[0], scope: props.scope, person_id: props.personId,
      page: page.value, page_size: 30 }, controller.signal);
    if (request === controller && !controller.signal.aborted) { result.value = response; hasResult.value=true; }
  } catch (e) {
    if (!controller.signal.aborted) {
      if(e instanceof ApiError && [401,403].includes(e.status)){result.value={items:[],summary:{},total:0};hasResult.value=false;}
      error.value = e instanceof Error ? e.message : '完成情况读取失败';
    }
  } finally { if (request === controller) loading.value = false; }
}
function reload(): void { page.value = 1; void load(); }
function turn(delta: number): void { page.value += delta; void load(); }
function checkToday(): void {
  if (!document.hidden && followToday && beijingDate() !== today) void load();
}
watch(() => [filters.from, filters.to], () => { error.value = ''; });
watch(() => [props.scope, props.personId, props.refreshKey], (value, previous) => {
  if (!previous || value[0] !== previous[0] || value[1] !== previous[1]) {
    result.value = { items: [], summary: {}, total: 0 }; hasResult.value = false; page.value = 1;
  }
  void load();
}, { immediate: true });
onMounted(() => {
  document.addEventListener('visibilitychange', checkToday);
  todayTimer = window.setInterval(checkToday, 60_000);
});
onBeforeUnmount(() => {
  request?.abort();
  window.clearInterval(todayTimer);
  document.removeEventListener('visibilitychange', checkToday);
});
</script>

<style scoped>
.completion { min-width: 0; color: var(--text-color, #27364b); }
.completion h2 { margin: 0; padding-top: 4px; font-size: 16px; font-weight: 600; }
.completion-filters { display: flex; flex-wrap: wrap; align-items: end; gap: 10px; padding: 12px 0; }
.completion-filters label { display: grid; gap: 5px; min-width: 130px; font-size: 12px; font-weight: 600; }
.completion-filters .search { flex: 1; max-width: 220px; }
.completion input { box-sizing: border-box; min-width: 0; width: 100%; height: 36px; padding: 6px 9px; border: 1px solid var(--border-color, #d4dde7); border-radius: 5px; color: inherit; background: var(--card-color, #fff); font: inherit; }
.completion button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; min-height: 36px; border: 1px solid var(--border-color, #d4dde7); border-radius: 5px; padding: 6px 12px; color: inherit; background: var(--card-color, #fff); cursor: pointer; }
.completion button:disabled { opacity: .5; cursor: default; }.completion .icon-button { width: 36px; padding: 0; flex-shrink: 0; }
.completion-summary { display: flex; flex-wrap: wrap; gap: 12px 20px; padding: 12px 0; border-block: 1px solid var(--border-color, #e0e6ec); font-size: 13px; }.completion-summary span { display: flex; gap: 9px; }.completion-summary strong { font-variant-numeric: tabular-nums; }
.completion-table { max-width: 100%; overflow-x: auto; }.completion table { width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; }.completion th { font-weight: 600; background: var(--table-header-color, #f5f7fa); white-space: nowrap; }.completion th, .completion td { padding: 12px 10px; border-bottom: 1px solid var(--border-color, #e0e6ec); vertical-align: top; }.completion td { min-width: 90px; }.completion td strong, .completion small { display: block; margin-top: 5px; }.completion small, .completion time { color: var(--text-color-3, #687587); }.completion .numbers { font-variant-numeric: tabular-nums; white-space: nowrap; }
.completion-state { white-space: nowrap; font-weight: 600; }.completion .success { color: #227755; }.completion .warning, .completion .failure { color: #ae443b; }.completion details { margin-top: 8px; font-size: 12px; }.completion summary { cursor: pointer; }.completion ul { list-style: none; padding: 0; min-width: 170px; }.completion li { display: grid; gap: 4px; padding: 6px 0; border-bottom: 1px solid var(--border-color, #e0e6ec); }
.completion-empty { padding: 36px; text-align: center; color: var(--text-color-3, #687587); }.completion-pagination { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 12px 0; font-size: 13px; }.completion-pagination div { display: flex; align-items: center; gap: 12px; }.completion-error, .completion-loading { display: flex; align-items: center; gap: 8px; padding: 12px 0; }.completion-error { color: #ae443b; }.spin { animation: completion-spin 1s linear infinite; }@keyframes completion-spin { to { transform: rotate(360deg); } }
@media (max-width: 600px) { .completion-filters { gap: 8px; }.completion-filters label { flex: 1 1 calc(50% - 8px); min-width: 0; }.completion-filters .search { flex: 1 1 calc(100% - 140px); max-width: none; }.completion input, .completion button { min-height: 44px; }.completion .icon-button { width: 44px; }.completion-summary { gap: 10px 16px; }.completion th, .completion td { padding: 10px 8px; } }
@media (prefers-reduced-motion: reduce) { .spin { animation: none; } }
</style>
