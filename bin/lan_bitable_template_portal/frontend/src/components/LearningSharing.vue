<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ArrowLeft, CheckCircle2, ChevronLeft, ChevronRight, FileText, Link, Loader2, RefreshCw, Save, Send, Square, Users } from 'lucide-vue-next';
import { requestLearning } from '../api/learning';
import type { Dict } from '../api/client';
import { randomHexId } from '../browserStorage';
import ConfirmDialog from './ConfirmDialog.vue';

const props = defineProps<{ isAdmin: boolean; userId: string }>();
const emit = defineEmits<{ dirty: [value: boolean]; busy: [value: boolean] }>();
const rows = ref<Dict[]>([]), detail = ref<Dict | null>(null), url = ref('');
const page = ref(1), total = ref(0), questionPage = ref(1), personSearch = ref('');
const selectedPerson = ref('');
const resultPaper = computed(() => (detail.value?.results || []).find((p: Dict) => p.open_id === selectedPerson.value));
const resultQuestions = computed(() => (resultPaper.value?.question_ids || []).map((id: string) => detail.value?.questions.find((q: Dict) => q.id === id)).filter(Boolean));
const busy = ref(false), loading = ref(false), error = ref(''), message = ref(''), dirty = ref(false);
watch(busy, value => emit('busy', value));
const labels: Record<string, string> = { queued: '等待生成', generating: '正在生成', draft: '待核对发布', published: '答题中', closed: '已关闭', failed: '生成失败' };
const confirm = ref<{ open: boolean; action: string; title: string; message: string }>({ open: false, action: '', title: '', message: '' });
let request: AbortController | undefined, epoch = 0, timer: ReturnType<typeof setTimeout> | undefined, disposed = false;
let createId = randomHexId();
const questions = computed(() => (detail.value?.questions || []).slice((questionPage.value - 1) * 5, questionPage.value * 5));
const participants = computed(() => (detail.value?.participants || []).filter((p: Dict) => `${p.name} ${p.scope}`.includes(personSearch.value.trim())));
const percent = computed(() => detail.value?.total ? Math.round(detail.value.completed * 100 / detail.value.total) : 0);
function changed() { dirty.value = true; emit('dirty', true); }
function clean() { dirty.value = false; emit('dirty', false); }
function answer(q: Dict) { return (q.correct || []).map((i: number) => String.fromCharCode(65 + i)).join('、'); }
function choose(q: Dict, i: number, checked: boolean) {
  q.correct = checked ? [...new Set([...(q.correct || []), i])].sort() : q.correct.filter((v: number) => v !== i);
  changed();
}
async function load(id = detail.value?.id, silent = false) {
  const ticket = ++epoch; request?.abort(); request = new AbortController();
  if (!silent) loading.value = true;
  error.value = '';
  try {
    const result = await requestLearning({ signal: request.signal }, id ? `/sharing/${id}` : `/sharing?page=${page.value}`);
    if (disposed || ticket !== epoch) return;
    if (id) { if (!dirty.value) detail.value = result; }
    else { rows.value = result.items || []; total.value = result.total; }
  } catch (e) { if (!disposed && ticket === epoch) error.value = (e as Error).message; }
  finally { if (ticket === epoch) loading.value = false; }
}
async function open(row: Dict) { if (busy.value) return; clean(); questionPage.value = 1; personSearch.value = ''; selectedPerson.value = ''; await load(row.id); }
async function mutate(action: string) {
  if (busy.value) return;
  busy.value = true; error.value = ''; message.value = '';
  try {
    let result: Dict;
    if (action === 'create') {
      result = await requestLearning({}, '/sharing', 'POST', { url: url.value.trim(), operation_id: createId });
      createId = randomHexId(); url.value = '';
    } else {
      const id = detail.value!.id;
      result = await requestLearning({}, `/sharing/${id}${action === 'save' ? '' : '/' + action}`, action === 'save' ? 'PUT' : 'POST',
        { version: detail.value!.version, ...(action === 'save' ? { questions: detail.value!.questions } : {}) });
      if (action === 'save') clean();
    }
    await load(result.id);
    message.value = ({ create: '纪要已进入生成队列，生成完成后可核对题目。', save: '题目已保存。', publish: '已固定每人5道题，正在后台逐人发送卡片。', retry: '已重新排队生成。', close: '已关闭本批答题及未答提醒，历史成绩保留。', 'retry-delivery': '已继续原发送任务，不会重新抽题。' } as Record<string, string>)[action];
  } catch (e) { error.value = (e as Error).message; }
  finally { busy.value = false; }
}
function ask(action: string) {
  confirm.value = { open: true, action,
    title: action === 'discard' ? '放弃未保存修改' : action === 'publish' ? '发布分享会练习' : action === 'close' ? '关闭此批次' : '重试未确认发送',
    message: action === 'publish' ? `确认已核对全部${detail.value!.question_count}道题及纪要依据？将向${detail.value!.total}位参会人真实发送每人5题的飞书答题卡，发布后不能修改题目。`
      : action === 'discard' ? '放弃本页尚未保存的题目修改，读取服务器最新版本。'
      : action === 'close' ? '关闭后不能继续提交，停止未答提醒；已有答题及正确答案保留。'
      : '将继续失败的原任务，不重新分配题目。若上次网络中断但对方已经收到，超出飞书去重时限后可能再次收到相同卡片，请先核对。' };
}
function resolved(ok: boolean) { const action = confirm.value.action; confirm.value.open = false; if (!ok) return; if (action === 'discard') { clean(); void load(); } else void mutate(action); }
async function back() { if (busy.value) return; if (dirty.value) { error.value = '请先保存题目修改，再返回列表。'; return; } detail.value = null; await load(); }
function schedule() {
  timer = setTimeout(async () => {
    if (!disposed && !document.hidden && !busy.value && !dirty.value && detail.value && ['queued', 'generating', 'published'].includes(detail.value.status)) await load(detail.value.id, true);
    if (!disposed) schedule();
  }, 8000);
}
onMounted(() => { void load(); schedule(); });
onBeforeUnmount(() => { disposed = true; epoch++; request?.abort(); clearTimeout(timer); });
</script>

<template>
  <section class="sharing-exercises" :aria-busy="busy || loading">
    <div v-if="error" role="alert" class="sharing-alert error">{{ error }}</div>
    <div v-if="message" role="status" class="sharing-alert success">{{ message }}</div>
    <header class="sharing-toolbar">
      <div class="sharing-title"><button v-if="detail" type="button" :disabled="busy" @click="back"><ArrowLeft :size="16" />批次列表</button><h2>{{ detail ? detail.title : '分享会练习' }}</h2></div>
      <button type="button" :disabled="busy || loading || dirty" title="刷新分享会练习" @click="load()"><RefreshCw :size="16" :class="{ spin: loading }" />刷新</button>
    </header>
    <template v-if="!detail">
      <form v-if="isAdmin" class="sharing-create" @submit.prevent="mutate('create')">
        <label for="sharing-minutes-url"><Link :size="16" />飞书纪要链接</label>
        <div><input id="sharing-minutes-url" v-model="url" type="url" required maxlength="2048" placeholder="https://vnet.feishu.cn/docx/…" :disabled="busy" /><button type="submit" class="primary" :disabled="busy || !url.trim()"><Loader2 v-if="busy" :size="16" class="spin" /><FileText v-else :size="16" />读取纪要并生成题目</button></div>
      </form>
      <div v-if="loading && !rows.length" class="sharing-empty" role="status"><Loader2 :size="24" class="spin" />正在读取批次…</div>
      <div v-else-if="!rows.length" class="sharing-empty"><FileText :size="30" /><span>{{ isAdmin ? '暂无分享会练习批次' : '暂无与你相关的分享会练习' }}</span></div>
      <div v-else class="sharing-batches">
        <button v-for="row in rows" :key="row.id" type="button" class="sharing-batch" @click="open(row)"><div><strong>{{ row.title }}</strong><small>{{ row.created_at?.slice(0, 16).replace('T', ' ') }}</small></div><span class="sharing-status" :class="row.status">{{ labels[row.status] }}</span><span>{{ row.question_count }}题</span><span>{{ row.completed }}/{{ row.total }}人完成</span><ChevronRight :size="18" /></button>
      </div>
      <footer class="sharing-pagination"><span>共 {{ total }} 批</span><div><button type="button" aria-label="上一页" :disabled="busy || loading || page <= 1" @click="page--; load()"><ChevronLeft :size="16" /></button><span>{{ page }} / {{ Math.max(1, Math.ceil(total / 15)) }}</span><button type="button" aria-label="下一页" :disabled="busy || loading || page * 15 >= total" @click="page++; load()"><ChevronRight :size="16" /></button></div></footer>
    </template>
    <template v-else>
      <div class="sharing-meta"><span class="sharing-status" :class="detail.status">{{ labels[detail.status] }}</span><a :href="detail.url" target="_blank" rel="noopener noreferrer">查看原纪要</a><span v-if="detail.published_at">发布于 {{ detail.published_at.slice(0, 16).replace('T', ' ') }}</span></div>
      <button v-if="dirty" type="button" :disabled="busy" @click="ask('discard')"><RefreshCw :size="16" />放弃修改，读取最新</button>
      <div v-if="['queued', 'generating'].includes(detail.status)" class="sharing-empty" role="status"><Loader2 :size="28" class="spin" /><span>{{ detail.progress || '等待后台生成，不影响其它功能使用' }}</span></div>
      <div v-if="detail.error" class="sharing-alert error">{{ detail.error }}</div>
      <div class="sharing-toolbar" v-if="isAdmin">
        <div class="sharing-buttons"><button v-if="detail.status === 'failed'" type="button" :disabled="busy" @click="mutate('retry')"><RefreshCw :size="16" />重新生成</button><button v-if="detail.status === 'draft'" type="button" :disabled="busy || !dirty" @click="mutate('save')"><Save :size="16" />保存题目</button><button v-if="detail.status === 'draft'" type="button" class="primary" :disabled="busy || dirty" @click="ask('publish')"><Send :size="16" />核对后发布给参会人</button><button v-if="detail.status === 'published'" type="button" :disabled="busy" @click="ask('retry-delivery')"><RefreshCw :size="16" />重试失败发送</button><button v-if="detail.status === 'published'" type="button" :disabled="busy" @click="ask('close')"><Square :size="15" />关闭本批</button><span v-if="dirty">题目有未保存修改</span><span v-if="busy" role="status"><Loader2 :size="16" class="spin" />正在保存…</span></div>
      </div>
      <div v-if="detail.total" class="sharing-metrics"><div><Users :size="18" /><strong>{{ detail.total }}</strong><span>参会人数</span></div><div><CheckCircle2 :size="18" /><strong>{{ detail.completed }}</strong><span>已完成</span></div><div><strong>{{ detail.total - detail.completed }}</strong><span>未完成</span></div><div><strong>{{ percent }}%</strong><span>完成率</span><progress :value="percent" max="100" aria-label="分享会练习完成率" /></div></div>
      <section v-if="detail.participants.length" class="sharing-section"><header><h3>参会人答题情况</h3><input v-model="personSearch" type="search" aria-label="搜索参会人" placeholder="搜索姓名 / 楼栋" /></header><div class="sharing-table"><table><thead><tr><th>姓名</th><th>楼栋</th><th>提交状态</th><th>正确题数</th><th>正确率</th><th>通知</th><th v-if="detail.results?.length">答卷</th></tr></thead><tbody><tr v-for="p in participants" :key="p.open_id"><td>{{ p.name }}</td><td>{{ p.scope || '未归楼' }}</td><td>{{ p.submitted_at ? '已提交' : '未提交' }}</td><td>{{ p.correct === null || p.correct === undefined ? '—' : p.correct + '/5' }}</td><td>{{ p.correct === null || p.correct === undefined ? '—' : p.correct * 20 + '%' }}</td><td><span v-if="p.delivery_error" class="error-text">{{ p.delivery_error }}</span><span v-else>{{ p.delivery_status === 'sent' ? '已发送' : detail.status === 'draft' ? '待发布' : '待发送' }}</span></td><td v-if="detail.results?.length"><button type="button" @click="selectedPerson = selectedPerson === p.open_id ? '' : p.open_id"><FileText :size="15" />{{ selectedPerson === p.open_id ? '收起答卷' : '查看答卷' }}</button></td></tr></tbody></table></div></section>
      <section v-if="resultPaper" class="sharing-section"><header><h3>{{ resultPaper.name }} · 本次5道题</h3><span>{{ resultPaper.submitted_at?.slice(0, 16).replace('T', ' ') || '尚未提交' }}</span></header><article v-for="(q, i) in resultQuestions" :key="q.id" class="sharing-question"><strong>{{ i + 1 }}. {{ q.stem }}</strong><p v-for="(option, n) in q.options" :key="n">{{ String.fromCharCode(65 + Number(n)) }}. {{ option }}</p><p>作答：{{ resultPaper.answers?.[q.id]?.map((n: number) => String.fromCharCode(65 + n)).join('、') || '未答' }} · 正确答案：{{ answer(q) }}</p></article></section>
      <section v-if="isAdmin && detail.weekly_reports?.length" class="sharing-section"><header><h3>最近群汇总</h3><button v-if="detail.status === 'closed'" type="button" :disabled="busy" @click="ask('retry-delivery')"><RefreshCw :size="16" />重试失败发送</button></header><div class="sharing-table"><table><thead><tr><th>统计截止</th><th>分组</th><th>发送状态</th><th>说明</th></tr></thead><tbody><tr v-for="r in detail.weekly_reports" :key="`${r.date}-${r.part}`"><td>{{ r.end }}</td><td>{{ r.part }} / {{ r.parts }}</td><td>{{ ({ sent: '已发送', pending: '待发送', failed: '发送失败', skipped: '已停止' } as Record<string, string>)[r.status] || r.status }}</td><td>{{ r.error || r.sent_at?.slice(0,16).replace('T',' ') || '等待后台发送' }}</td></tr></tbody></table></div></section>
      <section v-if="detail.questions?.length" class="sharing-section"><header><h3>{{ isAdmin ? '生成题目与答案' : '我的5道题' }}</h3><span>{{ detail.questions.length }}题</span></header>
        <article v-for="(q, i) in questions" :key="q.id" class="sharing-question"><header><strong>第 {{ (questionPage - 1) * 5 + i + 1 }} 题</strong><span>{{ (q.correct?.length || (q.type === 'single' ? 1 : 2)) === 1 ? '单选题' : '多选题' }}</span></header>
          <template v-if="isAdmin && detail.status === 'draft'"><label :for="`sharing-${q.id}`">题干<textarea :id="`sharing-${q.id}`" v-model="q.stem" rows="2" maxlength="500" :disabled="busy" @input="changed" /></label><div v-for="(option, n) in q.options" :key="n" class="sharing-option"><input :id="`sharing-${q.id}-${n}`" type="checkbox" :checked="q.correct.includes(n)" :aria-label="`第${(questionPage - 1) * 5 + i + 1}题选项${String.fromCharCode(65 + Number(n))}为正确答案`" :disabled="busy" @change="choose(q, Number(n), ($event.target as HTMLInputElement).checked)" /><label :for="`sharing-option-${q.id}-${n}`">{{ String.fromCharCode(65 + Number(n)) }}</label><textarea :id="`sharing-option-${q.id}-${n}`" v-model="q.options[n]" rows="1" maxlength="300" :disabled="busy" @input="changed" /></div><label>答案解析<textarea v-model="q.explanation" rows="2" maxlength="600" :disabled="busy" @input="changed" /></label><label>纪要原文依据<textarea v-model="q.evidence" rows="2" maxlength="800" :disabled="busy" @input="changed" /></label></template>
          <template v-else><p>{{ q.stem }}</p><p v-for="(option, n) in q.options" :key="n">{{ String.fromCharCode(65 + Number(n)) }}. {{ option }}</p><p v-if="q.selected">我的选择：{{ q.selected.map((n: number) => String.fromCharCode(65 + n)).join('、') }}</p><div v-if="q.correct" class="sharing-answer"><strong>正确答案：{{ answer(q) }}</strong><p>{{ q.explanation }}</p><blockquote>{{ q.evidence }}</blockquote></div></template>
        </article>
        <footer class="sharing-pagination"><span>每页5题</span><div><button type="button" aria-label="题目上一页" :disabled="questionPage <= 1" @click="questionPage--"><ChevronLeft :size="16" /></button><span>{{ questionPage }} / {{ Math.ceil(detail.questions.length / 5) }}</span><button type="button" aria-label="题目下一页" :disabled="questionPage * 5 >= detail.questions.length" @click="questionPage++"><ChevronRight :size="16" /></button></div></footer>
      </section>
    </template>
    <ConfirmDialog :open="confirm.open" :title="confirm.title" :message="confirm.message" @resolve="resolved" />
  </section>
</template>

<style scoped>
.sharing-exercises { color: #23354b; min-width: 0; }
.sharing-toolbar,.sharing-title,.sharing-buttons,.sharing-meta,.sharing-pagination,.sharing-pagination>div,.sharing-section>header,.sharing-question>header { display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
.sharing-toolbar,.sharing-section>header,.sharing-pagination { justify-content:space-between; margin:16px 0; }
.sharing-title { flex:1; min-width:0; }.sharing-title h2 { font-size:18px; line-height:1.5; overflow-wrap:anywhere; margin:0; }
h3 { font-size:15px; margin:0; }button { display:inline-flex; gap:6px; align-items:center; justify-content:center; border:1px solid #cddaea; border-radius:6px; background:white; color:#28559a; padding:8px 12px; min-height:38px; font:inherit; cursor:pointer; transition:background .18s; }button:hover:not(:disabled){background:#edf4ff;}button:disabled{opacity:.5;cursor:not-allowed;}button.primary{background:#2463df;color:white;border-color:#2463df;}button.primary:hover:not(:disabled){background:#174caf;}
button:focus-visible,a:focus-visible,input:focus-visible,textarea:focus-visible { outline:2px solid #2766df;outline-offset:3px; }input,textarea{font:inherit;border:1px solid #cad7e5;border-radius:6px;padding:9px 11px;color:inherit;background:#fff;min-width:0;line-height:1.6;}textarea{resize:vertical;width:100%;box-sizing:border-box;}a{color:#235abc;text-decoration:underline;}small,.sharing-meta,.sharing-pagination {color:#52667d;font-size:13px;}
.sharing-create{border-block:1px solid #dce5ee;padding:20px 0;margin-bottom:20px;}.sharing-create>label{display:flex;gap:7px;align-items:center;margin-bottom:9px;font-weight:600;}.sharing-create>div{display:flex;gap:10px;}.sharing-create input{flex:1;}.sharing-alert{padding:12px 15px;margin:12px 0;border-radius:6px;line-height:1.6;overflow-wrap:anywhere;}.error{background:#fff1f2;color:#a82342;}.success{background:#ecf9f2;color:#146844;}.error-text{color:#a82342;}
.sharing-empty{display:flex;min-height:160px;align-items:center;justify-content:center;flex-direction:column;gap:14px;color:#58708a;}.sharing-batches{border-top:1px solid #dce5ee;}.sharing-batch{display:grid;width:100%;grid-template-columns:minmax(0,1fr) 90px 65px 125px 20px;gap:14px;text-align:left;border:0;border-bottom:1px solid #dce5ee;border-radius:0;padding:16px 12px;color:inherit;}.sharing-batch strong{display:block;overflow-wrap:anywhere;line-height:1.65;}.sharing-batch small{display:block;margin-top:5px;}
.sharing-status{background:#eef3fa;color:#405e85;border-radius:4px;padding:4px 8px;font-size:12px;white-space:nowrap;}.sharing-status.published{background:#e4f6ef;color:#11704f;}.sharing-status.failed{background:#fff0f0;color:#ab2948;}.sharing-status.generating,.sharing-status.queued{background:#fff4dc;color:#805a17;}
.sharing-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;padding:22px 0;border-block:1px solid #dce5ee;margin:18px 0;}.sharing-metrics>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}.sharing-metrics strong{font-size:25px;font-weight:650;}.sharing-metrics span{color:#52667d;}.sharing-metrics progress{width:100%;height:6px;accent-color:#198763;}.sharing-table{overflow-x:auto;max-width:100%;}table{border-collapse:collapse;width:100%;min-width:630px;font-size:14px;}th,td{padding:12px 14px;border-bottom:1px solid #e1e8f0;text-align:left;vertical-align:top;}th{background:#f2f6fb;color:#465e7b;font-weight:600;}td:last-child{max-width:350px;overflow-wrap:anywhere;}
.sharing-question{padding:20px 0;border-top:1px solid #dce5ee;}.sharing-question>header{margin-bottom:14px;}.sharing-question>header span{font-size:12px;color:#5e738a;}.sharing-question label{display:grid;gap:6px;margin:10px 0;}.sharing-question p{white-space:pre-wrap;line-height:1.8;overflow-wrap:anywhere;margin:8px 0;}.sharing-option{display:grid;grid-template-columns:20px 20px minmax(0,1fr);gap:10px;align-items:center;margin:10px 0;}.sharing-option input{width:17px;height:17px;accent-color:#2766df;}.sharing-option label{display:block;margin:0;}.sharing-answer{background:#f0f8f4;border-left:3px solid #31a67c;padding:12px 16px;margin-top:14px;}.sharing-answer blockquote{margin:10px 0 0;color:#4d665b;font-size:13px;line-height:1.7;}.spin{animation:sharing-spin 1s linear infinite;}@keyframes sharing-spin{to{transform:rotate(360deg)}}
@media(max-width:700px){.sharing-create>div{flex-direction:column;}.sharing-metrics{grid-template-columns:repeat(2,minmax(0,1fr));}.sharing-batch{grid-template-columns:1fr auto;}.sharing-batch>div{grid-column:1/-1;}.sharing-batch>svg{display:none;}.sharing-toolbar{align-items:flex-start;}.sharing-section>header input{width:100%;box-sizing:border-box;}.sharing-title h2{font-size:16px;}.sharing-question{padding:16px 0;}button{min-height:42px;}}
@media(prefers-reduced-motion:reduce){.spin{animation-duration:2s;}button{transition:none;}}
</style>
