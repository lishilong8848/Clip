<template>
  <section class="learning-personal-home" aria-label="个人学习首页" :aria-busy="loading">
    <button v-if="dailyDuty" type="button" class="daily-tile" :aria-label="actionLabel" :aria-describedby="progressId" :disabled="loading || !person" @click="failed ? emit('refresh') : emit('open', 'today')">
      <span class="daily-tile-main">
        <span class="daily-title"><CalendarDays :size="22" aria-hidden="true" />当班题单</span>
        <span :id="progressId" class="daily-stats">
          <span>{{ dailyDuty.date }} · {{ dailyDuty.shift }}班</span>
          <template v-if="loading">正在读取当班进度…</template>
          <template v-else-if="failed">进度暂未读取</template>
          <template v-else-if="total">
            <span class="daily-figures"><span><strong>{{ answered }} / {{ total }}</strong> 题<small>已答题数</small></span><span>{{ uncertain ? '当班进度待核对' : completed ? '已完成' : `待作答 ${total - answered} 题` }}</span></span>
            <progress class="daily-progress" :max="total" :value="answered" aria-label="当班题单进度"></progress>
          </template>
          <template v-else>{{ uncertain ? '当班进度待核对' : '暂无已领取题目' }}</template>
        </span>
      </span>
      <span class="daily-action">{{ actionText }} <ArrowRight :size="23" aria-hidden="true" /></span>
    </button>

    <nav v-if="!workspaceV2" class="personal-shortcuts" aria-label="学习入口">
      <button type="button" :disabled="loading" @click="emit('open', 'practice')"><Pencil :size="19" />自主练习</button>
      <button type="button" :disabled="loading" @click="emit('open', 'sharing')"><Layers :size="19" />分享会练习</button>
      <button type="button" :disabled="loading" @click="emit('open', 'review')"><RotateCcw :size="19" />复习中心</button>
    </nav>

    <div class="personal-summary">
      <section aria-label="个人学习画像">
        <header><h2 v-if="workspaceV2">个人学习画像</h2><button v-else type="button" class="summary-heading" :disabled="loading" @click="emit('open', 'profile')">个人学习画像 <ChevronRight :size="16" /></button><small>近7天</small></header>
        <dl class="personal-metrics">
          <div><dt>首次答题</dt><dd>{{ metric(data.summary?.answered) }}</dd></div>
          <div><dt>选择题正确率</dt><dd>{{ metric(data.summary?.accuracy, '%') }}</dd></div>
          <div><dt>选择题错题</dt><dd>{{ metric(data.summary?.wrong) }}</dd></div>
        </dl>
        <MotionChart title="答题趋势" view="bar" unit="题" :items="trend" :details="false" :loading="loading" :failed="failed" />
      </section>
      <section aria-label="学习记录">
        <header><h2>学习记录</h2><button v-if="!workspaceV2" type="button" :disabled="loading" @click="emit('open', 'history')">查看全部</button></header>
        <p v-if="recordLoading" role="status">正在读取学习记录…</p>
        <p v-else-if="recordError">学习记录暂不可用</p>
        <p v-else-if="!records.length">暂无学习记录</p>
        <ul v-else class="personal-records">
          <li v-for="row in records" :key="row.id">
            <component :is="row.mode === 'practice' ? Pencil : CalendarDays" :size="17" />
            <span>{{ row.mode === 'practice' ? '自主练习' : '历史题单' }}</span>
            <span>{{ metric(row.stats?.answered) }} / {{ metric(row.stats?.total) }}</span><time :datetime="row.date">{{ row.date }}</time>
          </li>
        </ul>
        <footer v-if="!workspaceV2"><button type="button" :disabled="loading" @click="emit('open', 'completion')">查看每日完成</button><button type="button" :disabled="loading" @click="emit('open', 'issues')">题目反馈</button></footer>
      </section>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, useId, watch } from 'vue';
import { ArrowRight, CalendarDays, ChevronRight, Layers, Pencil, RotateCcw } from 'lucide-vue-next';
import type { Dict } from '../api/client';
import { requestLearning } from '../api/learning';
import MotionChart from '../workspace-v2/MotionChart.vue';

const props = defineProps<{ data: Dict; person?: Dict | null; loading: boolean; failed?: boolean; dailyDuty?: Dict | null }>();
const emit = defineEmits<{ open: [value: string]; refresh: [] }>();
const workspaceV2 = document.documentElement.classList.contains('workspace-v2');
const progressId = useId();
const task = computed(() => {
  const duty = props.dailyDuty;
  return duty && (props.data.daily_tasks || []).find((row: Dict) =>
    row.date === duty.date && row.shift === duty.shift && row.scope === duty.scope);
});
const total = computed(() => Number(task.value?.assigned) || 0);
const answered = computed(() => Math.min(total.value, Number(task.value?.answered) || 0));
const uncertain = computed(() => task.value?.status === '待核对');
const completed = computed(() => total.value > 0 && answered.value >= total.value && !uncertain.value);
const actionText = computed(() => props.loading ? '载入中' : props.failed ? '重试' : completed.value ? '回顾题单' : answered.value ? '继续答题' : '打开当班题单');
const actionLabel = computed(() => props.failed ? '刷新学习进度' : actionText.value);
const metric = (value: unknown, suffix = '') => typeof value === 'number' && Number.isFinite(value) ? value + suffix : '—';
const trend = computed(() => (props.data.trend || []).filter((row: Dict) => typeof row.attempt_count === 'number' || typeof row.answered === 'number')
  .map((row: Dict) => ({ label: String(row.date).slice(5).replace('-', '/'), value: row.attempt_count ?? row.answered })));
const records = ref<Dict[]>([]), recordLoading = ref(false), recordError = ref(false);
watch([() => props.person?.id, () => props.loading], async ([personId, loading], _previous, onCleanup) => {
  if (!personId || loading) return;
  const controller = new AbortController();
  onCleanup(() => controller.abort());
  recordLoading.value = true; recordError.value = false; records.value = [];
  try {
    const result = await requestLearning({ signal: controller.signal }, `/history?person_id=${encodeURIComponent(personId)}&page_size=3`);
    if (!controller.signal.aborted) records.value = (result.items || []).slice(0, 3);
  } catch {
    if (!controller.signal.aborted) recordError.value = true;
  } finally {
    if (!controller.signal.aborted) recordLoading.value = false;
  }
}, { immediate: true });
</script>

<style scoped>
.learning-personal-home{--learning-action:#2156c1;--learning-action-hover:#1949a9;--learning-progress:#90e9ec;--learning-progress-track:#ffffff45;display:grid;gap:14px;min-width:0;color:var(--v2-ink,var(--cf-text,#172638));letter-spacing:0}
.learning-personal-home button{font:inherit;cursor:pointer;letter-spacing:0}.learning-personal-home button:disabled{cursor:default}.learning-personal-home button:focus-visible{outline:2px solid #3277e6;outline-offset:3px}
.learning-personal-home .daily-tile{display:grid;grid-template-columns:minmax(0,2.3fr) minmax(144px,1fr);align-items:center;gap:25px;text-align:left;min-height:186px;width:100%;padding:20px 28px;border-radius:8px;border:1px solid transparent;background:var(--learning-action);color:var(--learning-on-action,#fff);box-shadow:none}
.daily-tile:hover:not(:disabled){background:var(--learning-action-hover)}.daily-tile:disabled{opacity:.72}.daily-tile-main,.daily-stats{display:grid;gap:8px;min-width:0}.daily-title,.daily-action,.daily-figures{display:flex;align-items:center;gap:10px}.daily-title{font-weight:700;font-size:19px}.daily-figures{justify-content:space-between;align-items:end;flex-wrap:wrap}.daily-figures strong{font-size:40px;line-height:1.13;font-variant-numeric:tabular-nums}.daily-figures small{display:block;font-size:13px}.daily-figures span:nth-child(2){font-size:13px}.daily-action{border-inline-start:1px solid #ffffff65;justify-content:center;min-height:90px;white-space:nowrap;font-size:20px;font-weight:700}
.daily-tile progress{appearance:none;width:100%;height:8px;border:0;border-radius:8px;overflow:hidden;background:var(--learning-progress-track);color:var(--learning-progress)}.daily-tile progress::-webkit-progress-bar{background:transparent}.daily-tile progress::-webkit-progress-value{background:var(--learning-progress);border-radius:8px}.daily-tile progress::-moz-progress-bar{background:var(--learning-progress);border-radius:8px}
.daily-progress :deep(.v2-progress-track){background:var(--learning-progress-track)}.daily-progress :deep(.v2-progress-track>span){background:var(--learning-progress)}.daily-progress :deep(.v2-progress-value){display:none}
.personal-shortcuts{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));padding:4px 0 12px;border-bottom:1px solid var(--cf-border,#d4deeb)}
.learning-personal-home :is(.personal-shortcuts, .personal-summary) button{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:42px;padding:0 6px;border:0;border-radius:0;background:none;color:inherit;box-shadow:none}.learning-personal-home :is(.personal-shortcuts, .personal-summary) button:hover:not(:disabled){text-decoration:underline}
.personal-shortcuts button+button{border-inline-start:1px solid var(--cf-border,#d4deeb)}.personal-shortcuts svg,.personal-records svg{color:#16887f;flex:none}
.personal-summary{display:grid;grid-template-columns:minmax(0,1.13fr) minmax(0,1fr);gap:0}.personal-summary>section{min-width:0;padding:0 22px 8px 0}.personal-summary>section+section{padding:0 0 8px 22px;border-inline-start:1px solid var(--cf-border,#d4deeb)}
:global(.workspace-v2 .personal-summary){align-items:start}
.personal-summary header,.personal-summary footer{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}.personal-summary header{min-height:42px;margin:0}.personal-summary footer{justify-content:flex-start;margin-top:8px}.personal-summary h2,.learning-personal-home .personal-summary .summary-heading{padding:0;margin:0;font-size:19px;font-weight:700}.personal-summary small,.personal-summary p{font-size:13px;color:var(--cf-muted,#52647c)}
.personal-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin:8px 0 14px}.personal-metrics>div+div{border-inline-start:1px solid var(--cf-border,#d4deeb);padding-inline-start:12px}.personal-metrics dt{font-size:12px;overflow-wrap:anywhere}.personal-metrics dd{margin:0;font-size:25px;line-height:1.3;font-weight:700;font-variant-numeric:tabular-nums}
.personal-records{list-style:none;margin:8px 0 0;padding:0}.personal-records li{display:grid;grid-template-columns:19px minmax(0,1fr) auto auto;gap:10px;align-items:center;padding:11px 0;border-bottom:1px solid var(--cf-border,#d4deeb);font-variant-numeric:tabular-nums}.personal-records time{color:var(--cf-muted,#52647c)}
.learning-personal-home :deep(.v2-chart-surface){height:164px}
@media(max-width:820px){.personal-summary{grid-template-columns:minmax(0,1fr)}.personal-summary>section,.personal-summary>section+section{padding:0 0 8px;border-inline-start:0}.personal-summary>section+section{border-top:1px solid var(--cf-border,#d4deeb);padding-top:10px}}
@media(max-width:540px){.learning-personal-home .daily-tile{grid-template-columns:minmax(0,1fr);gap:10px;padding:16px;min-height:166px}.daily-action{justify-content:flex-end;min-height:36px;border-inline-start:0;font-size:16px}.daily-figures strong{font-size:31px}.personal-shortcuts button{font-size:13px;gap:4px}.personal-shortcuts svg{width:16px}.personal-records li{gap:7px;font-size:13px}}
</style>
