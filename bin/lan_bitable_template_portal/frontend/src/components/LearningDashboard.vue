<template>
  <div class="learning-dashboard">
    <div class="metrics" aria-label="学习统计">
      <div v-for="(item, n) in metrics" :key="item.label" :class="{ 'metric-warning': n === (person ? 1 : 2), 'metric-success': n === 3 }"><span>{{ item.label }}</span><strong>{{ item.value }}</strong><component :is="[person ? BookOpen : UsersRound, person ? CircleAlert : BookOpen, person ? Target : CircleAlert, Target][n]" :size="21" class="metric-icon" aria-hidden="true" /></div>
    </div>
    <div v-if="person" class="today-strip"><span>今日进度 <strong>{{ data.today_summary?.task_answered ?? 0 }} / {{ data.today_summary?.assigned ?? 0 }}</strong></span><span>学习 {{ data.summary?.learning_days ?? 0 }} 天</span><span>复习 {{ data.summary?.practice_count ?? 0 }} 次</span><span>问答 {{ data.summary?.interview_total ?? 0 }} 题</span><button :disabled="disabled || !data.published" @click="$emit('continue')"><BookOpen :size="16" />{{ data.today_summary?.papers ? '继续今日学练' : '领取今日题单' }}</button><small v-if="!data.published">今日尚未发布</small></div>
    <div v-else class="today-strip"><span>今日已答 <strong>{{ data.today_summary?.answered_people ?? 0 }}</strong> 人</span><span>已完成 {{ data.today_summary?.completed_people ?? 0 }} 人</span><span>已领取未开始 {{ data.today_summary?.not_started_people ?? 0 }} 人</span><span>选择题正确率 {{ percent(data.today_summary?.accuracy) }}</span><small>{{ data.published ? '今日已发布' : '今日尚未发布' }}</small></div>
    <div class="dashboard-panels" aria-label="学练仪表盘">
      <section class="completion-panel" aria-label="题目完成进度">
        <header><h2>题目完成进度</h2><div class="segments" role="group" aria-label="完成进度时段"><button :class="{ active: progressPeriod === 'period' }" :aria-pressed="progressPeriod === 'period'" @click="progressPeriod = 'period'">本期</button><button :class="{ active: progressPeriod === 'today' }" :aria-pressed="progressPeriod === 'today'" @click="progressPeriod = 'today'">今日</button></div></header>
        <div class="completion-body">
          <div class="completion-ring" role="img" :aria-label="progress.total ? `已完成${progress.done}题，待完成${progress.total - progress.done}题，完成率${percent(progress.rate)}` : '尚无已领取题目'">
            <svg viewBox="0 0 160 160" aria-hidden="true"><circle cx="80" cy="80" r="62" class="ring-track" /><circle v-if="progress.rate != null" :key="`${progressPeriod}:${progress.rate}`" cx="80" cy="80" r="62" class="ring-fill" pathLength="100" stroke-dasharray="100" :style="{ strokeDashoffset: 100 - progress.rate }" transform="rotate(-90 80 80)" /></svg>
            <div><strong>{{ progress.rate == null ? '—' : percent(progress.rate) }}</strong><span>{{ progressPeriod === 'today' ? '今日完成率' : '本期完成率' }}</span></div>
          </div>
          <dl class="completion-legend"><div><dt><i class="dot complete" />已完成</dt><dd>{{ progress.done }}<small>题</small></dd></div><div><dt><i class="dot pending" />待完成</dt><dd>{{ progress.total - progress.done }}<small>题</small></dd></div><div class="legend-total"><dt>已领取题量</dt><dd>{{ progress.total }}<small>题</small></dd></div></dl>
        </div>
        <small class="panel-note">{{ progress.total ? '已剔除无效题；未领取备用题不计入' : '尚无已领取题目' }}</small>
      </section>
      <section class="answer-method-panel" aria-label="选择题作答方式对比">
        <header><h2>选择题作答方式</h2><span class="panel-unit">首次正确率</span></header>
        <div class="method-bars"><div v-for="item in answerMethods" :key="item.label" class="method-row">
          <div class="method-heading"><span>{{ item.label }}</span><strong>{{ percent(item.rate) }}</strong></div>
          <div class="method-track" role="img" :aria-label="`${item.label}：${item.total == null ? '数据不足' : `${item.total}题，首次正确${item.correct}题，正确率${percent(item.rate)}`}`"><i :class="item.kind" :style="{ transform: `scaleX(${(item.rate ?? 0) / 100})` }" /></div>
          <small>{{ item.total == null ? '暂无统计数据' : item.total ? `首次正确 ${item.correct} / ${item.total} 题` : '尚无作答' }}</small>
        </div></div>
        <small class="panel-note">仅统计有效选择题；问答自评不计入正确率</small>
      </section>
      <section class="interview-panel" aria-label="问答当前自评分布">
        <header><h2>问答当前自评</h2><span class="panel-unit">{{ ratingTotal }} 题</span></header>
        <div v-if="ratingTotal" class="rating-bars"><div v-for="item in ratingBars" :key="item.label" class="rating-row">
          <div class="method-heading"><span><i class="dot" :style="{ background: item.color }" />{{ item.label }}</span><strong>{{ item.count }}<small>题</small></strong></div>
          <div class="method-track" role="img" :aria-label="`${item.label}：${item.count}题，占${percent(item.count * 100 / ratingTotal)}`"><i :style="{ transform: `scaleX(${item.count / ratingTotal})`, background: item.color }" /></div>
        </div></div>
        <p v-else class="panel-empty">暂无问答自评</p>
        <small class="panel-note">{{ ratingTotal ? '按最近一次自评展示，不作为对错判分' : '统计期内暂无自评记录' }}</small>
      </section>
    </div>
    <div class="charts" :class="{ 'building-charts': !person }">
      <section class="trend"><header><h2>{{ person ? '个人答题趋势' : '每日参与趋势' }}</h2><div class="segments" role="group" aria-label="趋势指标"><button :class="{ active: metric === 'answered' }" :aria-pressed="metric === 'answered'" @click="metric = 'answered'">{{ person ? '答题数' : '参与人数' }}</button><button :class="{ active: metric === 'accuracy' }" :aria-pressed="metric === 'accuracy'" @click="metric = 'accuracy'">正确率</button></div></header>
        <p v-if="!data.summary?.answered" class="empty">暂无作答数据</p>
        <svg v-else class="trend-plot" :viewBox="`0 0 ${plotWidth} 190`" role="img" :aria-label="metric === 'accuracy' ? '首次选择题正确率趋势' : '作答趋势'">
          <g v-for="tick in [0, 1, 2]" :key="tick"><line x1="38" :x2="plotWidth - 16" :y1="22 + tick * 66" :y2="22 + tick * 66" stroke="#e1e6ed" /><text x="30" :y="26 + tick * 66" text-anchor="end">{{ Math.round(max * (2 - tick) / 2) }}{{ metric === 'accuracy' ? '%' : '' }}</text></g>
          <polyline v-for="(line, n) in paths" :key="`${metric}:${n}:${line}`" class="trend-line" :points="line" pathLength="1000" fill="none" stroke="#266ad1" stroke-width="2.5" stroke-linejoin="round" />
          <g v-for="point in points" :key="point.date"><circle v-if="point.value != null" class="trend-point" tabindex="0" :aria-label="`${point.date}：${point.value}${metric === 'accuracy' ? '%' : person ? '题' : '人'}`" :cx="point.x" :cy="point.y" r="4" fill="#266ad1"><title>{{ point.date }}：{{ point.value }}{{ metric === 'accuracy' ? '%' : '' }}</title></circle><text v-if="point.label" :x="point.x" y="179" text-anchor="middle">{{ point.date.slice(5) }}</text></g>
        </svg>
      </section>
      <section><header><h2>{{ person ? '错题分类对比' : '选择题正确率分布' }}</h2><select v-if="person" v-model="classification" aria-label="错题分类"><option value="banks">按题库</option><option value="topics">按专业 / 知识点</option></select></header><div class="bars"><div v-for="item in bars" :key="item.label" class="bar-row"><span :title="item.label">{{ item.label }}</span><div><i :class="{ 'wrong-bar': person }" :style="{ transform: `scaleX(${item.value / barMax})` }" /></div><strong>{{ item.value }}</strong></div><p v-if="!bars.length" class="empty">暂无统计数据</p></div><small v-if="!person">无选择题作答 {{ data.without_choice_answers ?? 0 }} 人，未按 0% 计入</small></section>
      <section v-if="!person"><header><h2>错题分类对比</h2><select v-model="classification" aria-label="错题分类"><option value="banks">按题库</option><option value="topics">按专业 / 知识点</option></select></header><div class="bars"><div v-for="item in wrongBars" :key="item.label" class="bar-row"><span :title="item.label">{{ item.label }}</span><div><i class="wrong-bar" :style="{ transform: `scaleX(${item.value / wrongMax})` }" /></div><strong>{{ item.value }}</strong></div><p v-if="!wrongBars.length" class="empty">暂无统计数据</p></div></section>
    </div>
    <template v-if="!person">
      <header class="table-head"><h2>人员学习汇总</h2><div><label><Search :size="16" /><input v-model="search" type="search" placeholder="姓名 / 工号" aria-label="搜索已答人员" @input="page = 1" /></label><label><input v-model="includeUnanswered" type="checkbox" @change="page = 1" />包括已领取未作答</label><select v-model="sort" aria-label="人员汇总排序" @change="page = 1"><option value="recent">最近作答</option><option value="name">姓名</option><option value="answered">答题数</option></select></div></header>
      <div class="table-wrap"><table><thead><tr><th>姓名 / 工号</th><th>答题数</th><th>选择题错题</th><th>选择题正确率</th><th>今日进度</th><th>最近作答</th><th></th></tr></thead><tbody><tr v-for="row in visible" :key="row.person_id"><td>{{ row.name }}<small>{{ row.employee_no || '未填写工号' }}</small></td><td>{{ row.summary.answered }}</td><td>{{ row.summary.wrong }}</td><td>{{ percent(row.summary.accuracy) }}</td><td>{{ row.today ? `${row.today.answered} / ${row.today.total}` : '未领取' }}</td><td>{{ row.last_answered_at?.replace('T', ' ').slice(0, 16) || '尚未作答' }}</td><td><button :disabled="disabled" @click="$emit('select', { id: row.person_id, name: row.name, employee_no: row.employee_no })"><ChartNoAxesCombined :size="16" />个人画像</button></td></tr></tbody></table><p v-if="!filtered.length" class="empty">统计期内暂无匹配人员</p></div>
      <footer><span>共 {{ filtered.length }} 人 · 每页 20 人</span><div><button aria-label="人员上一页" :disabled="page <= 1" @click="page--"><ChevronLeft :size="17" /></button><span>{{ page }} / {{ Math.max(1, Math.ceil(filtered.length / 20)) }}</span><button aria-label="人员下一页" :disabled="page * 20 >= filtered.length" @click="page++"><ChevronRight :size="17" /></button></div></footer>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { BookOpen, ChartNoAxesCombined, Search, ChevronLeft, ChevronRight, UsersRound, Target, CircleAlert } from 'lucide-vue-next';
import type { Dict } from '../api/client';
const props = defineProps<{ data: Dict; person?: Dict | null; disabled?: boolean }>();
defineEmits<{ continue: []; select: [person: Dict] }>();
const metric = ref('answered'), classification = ref('banks'), search = ref(''), sort = ref('recent'), page = ref(1), includeUnanswered = ref(false);
const progressPeriod = ref('period');
const percent = (value: unknown) => value == null ? '数据不足' : `${Number(value).toFixed(1)}%`;
const count = (value: unknown): number | null => typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null;
const progress = computed(() => {
  const s = (progressPeriod.value === 'today' ? props.data.today_summary : props.data.summary) || {};
  const total = count(s.assigned) ?? 0, done = Math.min(total, count(s.task_answered) ?? 0);
  return { total, done, rate: total ? done * 100 / total : null };
});
const answerMethods = computed(() => {
  const s = props.data.summary || {};
  const total = count(s.choice_answered), correct = count(s.correct), independent = count(s.independent_answered), independentCorrect = count(s.independent_correct);
  const row = (label: string, kind: string, n: number | null, right: number | null) => {
    const valid = n != null && right != null && right <= n;
    return { label, kind, total: valid ? n : null, correct: valid ? right : null, rate: valid && n ? right * 100 / n : null };
  };
  const valid = total != null && correct != null && independent != null && independentCorrect != null && correct <= total && independent <= total && independentCorrect <= independent && independentCorrect <= correct && correct - independentCorrect <= total - independent;
  return [row('独立作答', 'independent', independent, independentCorrect), row('使用提示后作答', 'assisted', valid ? total! - independent! : null, valid ? correct! - independentCorrect! : null)];
});
const ratingBars = computed(() => Object.entries(props.data.summary?.interview_ratings || {}).filter(([, value]) => (count(value) || 0) > 0).map(([label, value]) => ({ label: label || '未填写自评', count: Number(value), color: label === '需复习' ? '#ba626c' : label === '部分掌握' ? '#b48835' : '#77889d' })).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label, 'zh-CN')));
const ratingTotal = computed(() => ratingBars.value.reduce((total, row) => total + row.count, 0));
const metrics = computed(() => {
  const s = props.data.summary || {};
  return props.person ? [{ label: '答题数', value: s.answered ?? 0 }, { label: '选择题错题数', value: s.wrong ?? 0 }, { label: '选择题首次正确率', value: percent(s.accuracy) }, { label: '独立作答正确率', value: percent(s.independent_accuracy) }]
    : [{ label: '已答人数', value: s.answered_people ?? 0 }, { label: '答题总数', value: s.answered ?? 0 }, { label: '选择题错题数', value: s.wrong ?? 0 }, { label: '选择题首次正确率', value: percent(s.accuracy) }];
});
const values = computed(() => (props.data.trend || []).map((row: Dict) => ({ date: row.date, value: row[metric.value === 'accuracy' ? 'accuracy' : props.person ? 'answered' : 'people'] as number | null })));
const max = computed(() => metric.value === 'accuracy' ? 100 : Math.max(2, ...values.value.map((r: Dict) => r.value || 0)));
const plotWidth = computed(() => props.person ? 680 : 420);
const points = computed(() => values.value.map((row: Dict, i: number) => ({ ...row, x: values.value.length === 1 ? (plotWidth.value + 18) / 2 : 44 + i * (plotWidth.value - 70) / (values.value.length - 1), y: 154 - (row.value || 0) * 132 / max.value, label: i === 0 || i === values.value.length - 1 || i % Math.max(1, Math.ceil(values.value.length / 7)) === 0 })));
const paths = computed(() => { const groups: string[] = []; let pointsInLine: string[] = []; for (const p of points.value) { if (p.value == null) { if (pointsInLine.length) groups.push(pointsInLine.join(' ')); pointsInLine = []; } else pointsInLine.push(`${p.x},${p.y}`); } if (pointsInLine.length) groups.push(pointsInLine.join(' ')); return groups; });
const bars = computed(() => (props.person ? props.data[classification.value] || [] : props.data.distribution || []).map((r: Dict) => ({ label: r.label || r.topic, value: props.person ? r.wrong : r.count })));
const barMax = computed(() => Math.max(1, ...bars.value.map((r: Dict) => r.value)));
const wrongBars = computed(() => (props.data[classification.value] || []).map((r: Dict) => ({ label: r.label || r.topic, value: r.wrong })));
const wrongMax = computed(() => Math.max(1, ...wrongBars.value.map((r: Dict) => r.value)));
const filtered = computed<Dict[]>(() => [...(props.data.people || [])].filter((r: Dict) => (includeUnanswered.value || r.summary.answered > 0) && `${r.name} ${r.employee_no}`.includes(search.value.trim())).sort((a, b) => sort.value === 'name' ? a.name.localeCompare(b.name, 'zh-CN') : sort.value === 'answered' ? b.summary.answered - a.summary.answered || a.person_id.localeCompare(b.person_id) : b.last_answered_at.localeCompare(a.last_answered_at) || a.person_id.localeCompare(b.person_id)));
const visible = computed(() => filtered.value.slice((page.value - 1) * 20, page.value * 20));
watch(() => props.data, () => { page.value = 1; });
</script>

<style scoped>
.learning-dashboard { color: #24364b; font-variant-numeric: tabular-nums; }
.learning-dashboard * { box-sizing: border-box; }
.dashboard-panels { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 26px; padding: 22px 0; border-top: 1px solid #e0e7ef; }
.dashboard-panels > section { min-width: 0; display: flex; flex-direction: column; }
.dashboard-panels > section + section { border-left: 1px solid #e0e7ef; padding-left: 26px; }
.dashboard-panels header { min-height: 34px; flex-wrap: wrap; }
.panel-unit { font-size: 12px; color: #64758a; }.panel-note { margin-top: auto; padding-top: 14px; line-height: 1.6; }.panel-empty { margin: auto; padding: 44px 0; color: #6a7b90; font-size: 13px; }
.completion-body { display: flex; align-items: center; gap: 14px; flex: 1; padding-top: 12px; }
.completion-ring { position: relative; flex: 0 0 156px; width: 156px; aspect-ratio: 1; }
.completion-ring svg { display: block; width: 100%; height: 100%; fill: none; stroke-width: 13px; }.ring-track { stroke: #e1e7ef; }.ring-fill { stroke: #278a7c; animation: learning-ring .65s cubic-bezier(.22, 1, .36, 1) both; }
.completion-ring > div { position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; }.completion-ring strong { font-size: 23px; font-weight: 600; }.completion-ring span { font-size: 12px; color: #64758a; }
.completion-legend { margin: 0; flex: 1; min-width: 0; display: grid; gap: 12px; }.completion-legend > div { display: flex; gap: 8px; justify-content: space-between; align-items: center; flex-wrap: wrap; }.completion-legend dt { font-size: 12px; display: inline-flex; align-items: center; gap: 7px; }.completion-legend dd { margin: 0; font-size: 17px; font-variant-numeric: tabular-nums; }.completion-legend small, .rating-row strong small { display: inline; margin-left: 4px; font-weight: 400; }.legend-total { border-top: 1px solid #e4eaf2; padding-top: 10px; }
.dot { display: inline-block; flex: 0 0 8px; width: 8px; height: 8px; border-radius: 50%; }.dot.complete { background: #278a7c; }.dot.pending { background: #aebdce; }
.method-bars, .rating-bars { display: grid; gap: 22px; padding-top: 25px; }.method-heading { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 9px; font-size: 13px; }.method-heading > span { display: flex; align-items: center; gap: 8px; min-width: 0; overflow-wrap: anywhere; }.method-heading strong { font-size: 15px; font-weight: 600; white-space: nowrap; font-variant-numeric: tabular-nums; }
.method-track { height: 10px; background: #e9eef4; border-radius: 3px; overflow: hidden; }.method-track > i { display: block; height: 100%; }.method-track .independent { background: #278a7c; }.method-track .assisted { background: #b48835; }.method-row > small { margin-top: 7px; }
@media (min-width: 701px) and (max-width: 1180px) { .dashboard-panels { gap: 16px; }.dashboard-panels > section + section { padding-left: 16px; }.completion-ring { flex-basis: 132px; width: 132px; }.completion-body { gap: 8px; }.completion-ring strong { font-size: 21px; } }
.charts.building-charts { grid-template-columns: minmax(0, 1.4fr) repeat(2, minmax(0, 1fr)); }
.bar-row i.wrong-bar { background: #c26967; }
.metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border-block: 1px solid #dce5ef; background: white; }
.metrics > div { position: relative; padding: 20px 56px 20px 20px; border-right: 1px solid #e4ebf2; }.metrics > div:last-child { border: 0; }.metrics span, small { display: block; color: #64758a; font-size: 12px; }.metrics strong { display: block; margin-top: 8px; font-size: 26px; font-weight: 600; overflow-wrap: anywhere; }.metric-icon { position: absolute; right: 20px; top: 23px; color: #507ab5; }.metric-warning .metric-icon { color: #b15e6b; }.metric-success .metric-icon { color: #278a7c; }
.today-strip { display: flex; gap: 22px; align-items: center; flex-wrap: wrap; padding: 15px 0; }.today-strip button { margin-left: auto; color: white; background: #2167d3; border-color: #2167d3; }
.charts { display: grid; grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr); gap: 30px; padding: 22px 0; border-block: 1px solid #e0e7ef; }.charts section { min-width: 0; }.charts header { flex-wrap: wrap; min-height: 36px; }header, footer, footer > div, .table-head > div { display: flex; align-items: center; justify-content: space-between; gap: 12px; }h2 { margin: 0; font-size: 16px; font-weight: 600; }
button, select, input[type=search] { font: inherit; border: 1px solid #d4dfeb; background: white; border-radius: 6px; padding: 8px 10px; color: #28568c; }button { display: inline-flex; align-items: center; gap: 6px; cursor: pointer; }button:disabled { opacity: .5; cursor: not-allowed; }button:focus-visible, select:focus-visible, input:focus-visible { outline: 2px solid #2167d3; outline-offset: 2px; }.segments { display: flex; gap: 4px; }.segments button { font-size: 12px; padding: 6px 9px; }.segments .active { background: #e9f1ff; border-color: #8db4ec; }
.trend-plot { display: block; width: 100%; height: 210px; }svg text { font-size: 10px; fill: #62758b; }.bars { display: grid; gap: 14px; max-height: 210px; overflow: auto; padding: 18px 0; }.bar-row { display: grid; grid-template-columns: minmax(80px, 130px) minmax(0, 1fr) 28px; gap: 12px; align-items: center; }.bar-row > span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }.bar-row > div { background: #edf1f5; height: 10px; border-radius: 3px; overflow: hidden; }.bar-row i { background: #309787; height: 100%; display: block; }.bar-row strong { font-size: 13px; font-weight: 500; }
.table-head { margin: 24px 0 14px; }.table-head label { display: inline-flex; align-items: center; gap: 6px; }.table-wrap { overflow: auto; }table { width: 100%; border-collapse: collapse; background: white; }th, td { text-align: left; padding: 13px 14px; border-bottom: 1px solid #e4eaf2; }th { background: #f4f7fb; font-size: 12px; color: #5c7088; }td small { margin-top: 4px; }.empty { padding: 54px 20px; color: #6a7b90; text-align: center; }footer { padding: 14px 0; color: #65758a; font-size: 13px; }
button, select, input { transition: background-color .18s ease, border-color .18s ease, box-shadow .18s ease; }
button:hover:not(:disabled) { background: #edf4ff; border-color: #8aafe1; }.today-strip button:hover:not(:disabled) { background: #1958bb; }
.segments { padding: 3px; gap: 2px; background: #eaf0f7; border-radius: 7px; }.segments button { border-color: transparent; background: transparent; color: #51677f; min-height: 30px; }.segments .active { background: #fff; color: #205db2; border-color: #d8e3f1; box-shadow: 0 1px 3px #1f477010; }
.method-track > i, .bar-row i { width: 100%; transform-origin: left center; transition: transform .45s cubic-bezier(.22, 1, .36, 1); animation: learning-bar .6s cubic-bezier(.22, 1, .36, 1) both; }
.trend-line { stroke-dasharray: 1000; stroke-dashoffset: 0; animation: learning-line .7s ease-out both; }
.trend-point { stroke: #fff; stroke-width: 2px; transition: stroke-width .18s ease; }.trend-point:hover, .trend-point:focus { stroke: #a6c6f4; stroke-width: 4px; outline: none; }
tbody tr:hover { background: #f7faff; }.table-head, .table-head > div { flex-wrap: wrap; }
@keyframes learning-ring { from { stroke-dashoffset: 100; } }
@keyframes learning-bar { from { transform: scaleX(0); } }
@keyframes learning-line { from { stroke-dashoffset: 1000; } }
@media (prefers-reduced-motion: reduce) { .ring-fill, .trend-line, .method-track > i, .bar-row i { animation: none; transition: none; }button, select, input, .trend-point { transition: none; } }
</style>
