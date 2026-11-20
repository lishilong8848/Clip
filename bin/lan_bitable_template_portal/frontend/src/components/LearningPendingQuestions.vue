<template>
  <section class="learning-pending-questions" aria-label="待处理题目" :aria-busy="loading || detailLoading">
    <aside class="pending-list">
      <h2>待处理题目</h2>
      <p v-if="error" role="alert">{{ error }} <button type="button" class="text-action" :disabled="loading" @click="emit('page', queue.page || 1)">重试</button></p>
      <p v-else-if="loading && !queue.items?.length" role="status">正在读取待处理题目…</p>
      <p v-else-if="!queue.items?.length">暂无待处理题目</p>
      <ul v-else>
        <li v-for="item in queue.items" :key="item.question_id">
          <button type="button" class="pending-question-row" :aria-current="item.question_id === selection ? 'true' : undefined" :disabled="busy || loading" @click="selection = item.question_id">
            <strong>{{ item.stem }}</strong>
            <small>{{ item.type_label || labels[item.type] }} · {{ item.feedback_count }} 条反馈</small>
          </button>
        </li>
      </ul>
      <nav v-if="queue.total > queue.page_size" aria-label="待处理题目分页">
        <button type="button" :disabled="queue.page <= 1 || busy || loading" aria-label="上一页" title="上一页" @click="emit('page', queue.page - 1)"><ChevronLeft :size="16" /></button>
        <span>{{ queue.page }} / {{ Math.ceil(queue.total / queue.page_size) }}</span>
        <button type="button" :disabled="queue.page * queue.page_size >= queue.total || busy || loading" aria-label="下一页" title="下一页" @click="emit('page', queue.page + 1)"><ChevronRight :size="16" /></button>
      </nav>
    </aside>
    <article class="pending-detail" aria-label="原题">
      <p v-if="detailError" role="alert">{{ detailError }} <button type="button" class="text-action" @click="retry++">重试</button></p>
      <p v-else-if="detailLoading" role="status">正在读取原题…</p>
      <template v-else-if="question">
        <header class="pending-detail-header">
          <div><h2>原题</h2><small>{{ bankNames[question.bank] }} · {{ question.type_label || labels[question.type] }}</small></div>
          <button type="button" class="pending-edit" :disabled="busy || loading || !detail?.editable" @click="emit('edit', selectedIssue)"><Pencil :size="16" />编辑本题</button>
        </header>
        <h3>{{ question.stem }}</h3>
        <ol v-if="question.options?.length" class="pending-options">
          <li v-for="(option, index) in question.options" :key="option.id" :class="{ correct: question.correct_option_ids?.includes(option.id) }">
            <b>{{ String.fromCharCode(65 + Number(index)) }}</b><span>{{ option.text }}</span>
            <small v-if="question.correct_option_ids?.includes(option.id)">参考答案</small>
          </li>
        </ol>
        <p v-if="question.answer_text" class="pending-answer"><strong>参考答案</strong> {{ question.answer_text }}</p>
        <p v-if="question.analysis" class="pending-answer"><strong>解析</strong> {{ question.analysis }}</p>
        <h3 class="feedback-heading">题目反馈 · {{ detail?.feedback?.length || 0 }} 条</h3>
        <p v-if="!detail?.editable">这道题已处理。</p>
        <ul v-else class="pending-reports">
          <li v-for="item in detail.feedback || []" :key="item.id">
            <div>{{ item.description }}<small v-if="item.suggestion">{{ item.suggestion }}</small></div>
            <small>{{ item.scope ? item.scope + '楼 · ' : '' }}{{ item.created_at?.slice(0,10) }}</small>
          </li>
        </ul>
      </template>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { ChevronLeft, ChevronRight, Pencil } from 'lucide-vue-next';
import type { Dict } from '../api/client';
import { requestLearning } from '../api/learning';

const props = defineProps<{ queue: Dict; labels: Dict; loading: boolean; busy: boolean; error: string }>();
const emit = defineEmits<{ page: [value: number]; edit: [issueId: string] }>();
const bankNames: Dict = { written: '笔试题库', duty: '值班面试', professional: '专业面试', supplemental: '专项题库' };
const selection = ref(''), retry = ref(0), detail = ref<Dict>();
const detailLoading = ref(false), detailError = ref('');
const selectedIssue = computed<string>(() => props.queue.items?.find((item: Dict) => item.question_id === selection.value)?.issue_id || '');
const question = computed(() => detail.value?.question);

watch(() => props.queue, queue => {
  if (!queue.items?.some((item: Dict) => item.question_id === selection.value)) selection.value = queue.items?.[0]?.question_id || '';
}, { immediate: true });
watch([selectedIssue, () => props.queue, retry], async ([issueId], _previous, onCleanup) => {
  detail.value = undefined; detailError.value = ''; detailLoading.value = Boolean(issueId);
  if (!issueId) return;
  const controller = new AbortController();
  onCleanup(() => controller.abort());
  try {
    const result = await requestLearning({ signal: controller.signal }, `/issues/${encodeURIComponent(issueId)}/question?group=1`);
    if (!controller.signal.aborted) detail.value = result;
  } catch (error) {
    if (!controller.signal.aborted) detailError.value = (error as Error).message;
  } finally {
    if (!controller.signal.aborted) detailLoading.value = false;
  }
}, { immediate: true });
</script>

<style scoped>
.learning-pending-questions{--learning-action:#2156c1;display:grid;grid-template-columns:minmax(230px,30%) minmax(0,1fr);gap:26px;min-width:0;padding:20px 0;color:var(--v2-ink,var(--cf-text,#172638));letter-spacing:0}
.learning-pending-questions :is(aside,article,header,li){min-width:0}
.learning-pending-questions :is(ul,ol){list-style:none;margin:0;padding:0}
.learning-pending-questions :is(h2,h3){margin:0;line-height:1.4;white-space:pre-wrap;overflow-wrap:anywhere}
.learning-pending-questions h2{font-size:20px}.learning-pending-questions h3{font-size:18px}
.pending-list{border-inline-end:1px solid var(--cf-border,#d4deeb);padding-inline-end:20px}
.pending-list h2{margin-bottom:14px}.pending-list li,.pending-reports li,.pending-options li{border-bottom:1px solid var(--cf-border,#d4deeb)}
.pending-list nav,.pending-detail-header,.pending-detail-header>div,.pending-reports li{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.pending-list nav{justify-content:center;margin-top:10px}.pending-detail-header,.pending-reports li{justify-content:space-between}
.learning-pending-questions button{font:inherit;cursor:pointer;letter-spacing:0}.learning-pending-questions button:disabled{cursor:default;opacity:.55}
.learning-pending-questions button:focus-visible{outline:2px solid #3277e6;outline-offset:2px}
.learning-pending-questions .pending-question-row{display:grid;gap:4px;width:100%;padding:12px 14px;min-height:58px;text-align:left;border:0;border-inline-start:3px solid transparent;border-radius:6px;background:transparent;color:inherit;overflow-wrap:anywhere}
.learning-pending-questions .pending-question-row:hover,.learning-pending-questions .pending-question-row[aria-current]{background:var(--v2-selection,#e6efff);border-inline-start-color:#3277e6}
.learning-pending-questions small{color:var(--cf-muted,#52647c);font-size:12px}.pending-detail-header{padding-bottom:12px;margin-bottom:18px;border-bottom:1px solid var(--cf-border,#d4deeb)}
.learning-pending-questions .pending-edit{display:inline-flex;align-items:center;gap:7px;padding:8px 14px;min-height:38px;border:1px solid transparent;border-radius:6px;background:var(--learning-action);color:#fff;font-weight:650}
.pending-options{margin-top:16px!important}.pending-options li{display:grid;grid-template-columns:24px minmax(0,1fr) auto;gap:10px;align-items:start;padding:10px 12px;overflow-wrap:anywhere}
.pending-options li.correct{border-radius:6px;background:#e8f7f4;color:#11544b}.pending-options li.correct small{color:inherit}
.learning-pending-questions .feedback-heading{margin-top:22px;margin-bottom:6px}.pending-reports li{align-items:baseline;padding:9px 0}.pending-reports li>div,.pending-answer{overflow-wrap:anywhere;white-space:pre-wrap}.pending-reports li>div{flex:1 1 220px}.pending-reports li small{display:block}
.learning-pending-questions .text-action{min-height:32px;padding:2px 6px;border:0;background:none;color:var(--cf-link,#2156c1)}
@media(max-width:760px){.learning-pending-questions{grid-template-columns:minmax(0,1fr);gap:14px;padding:8px 0}.pending-list{border:0;border-bottom:1px solid var(--cf-border,#d4deeb);padding:0 0 12px}.pending-detail-header{margin-bottom:12px}.learning-pending-questions .pending-edit{min-height:44px}}
@media(max-width:760px){.pending-list ul{max-height:220px;overflow:auto;overscroll-behavior:contain}}
</style>
