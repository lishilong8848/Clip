<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ArrowLeft, CheckCircle2, ChevronLeft, ChevronRight, FileText, Link, Loader2, Plus, RefreshCw, Save, Send, Square, Trash2, Upload, Users, X } from 'lucide-vue-next';
import { requestLearning } from '../api/learning';
import type { Dict } from '../api/client';
import { randomHexId, resilientStorage } from '../browserStorage';
import { NModal, NPagination, NRadioButton, NRadioGroup, NTab, NTabs } from 'naive-ui';
import ConfirmDialog from './ConfirmDialog.vue';
import LearningQuestionImages from './LearningQuestionImages.vue';
import { logicalUrl } from '../workspace-v2/version';

const props = defineProps<{ isAdmin: boolean; userId: string }>();
const emit = defineEmits<{ dirty: [value: boolean]; busy: [value: boolean] }>();

const TYPES = ['single', 'multiple', 'interview'] as const;
const typeLabels: Record<string, string> = { single: '单选题', multiple: '多选题', interview: '问答题' };
const originLabels: Record<string, string> = { ai: 'AI生成', manual: '手动录入' };
const AI_MAX = 40, TOTAL_MAX = 100, PAGE_SIZE = 5, CHOICE_MIN = 2, CHOICE_MAX = 6;
const letters = (n: number) => String.fromCharCode(65 + n);

const rows = ref<Dict[]>([]), detail = ref<Dict | null>(null), url = ref(''), materialUrl = ref('');
const title = ref('');
const questionTypes = ref<string[]>(['single']);
const autoGenerate = ref(true), includeMinutes = ref(true);
const creationMode = ref<'materials' | 'manual' | 'import'>('materials');
const importing = computed(() => creationMode.value === 'import');
const manualQuestions = ref<Dict[]>([]);
const filledManualQuestions = computed(() => manualQuestions.value.filter(q => q.stem?.trim() || q.explanation?.trim() || q.reference_answer?.trim() || q.options?.some((o: string) => o.trim())));
const manualOnly = computed(() => creationMode.value === 'manual');
const manualEditing = computed(() => manualOnly.value || (!importing.value && !autoGenerate.value));
watch(manualEditing, value => { if (value && !manualQuestions.value.length) manualQuestions.value = [newQuestion('single')]; });
const materialUrls = ref<string[]>([]), materialIds = ref<string[]>([]);
const uploadedNames = ref<Record<string, string>>({});
const materialFiles = ref<File[]>([]), uploading = ref(false);
const people = ref<Dict[]>([]), participants = ref<Dict[]>([]), peopleSearch = ref(''), peopleLoading = ref(false);
const page = ref(1), total = ref(0), questionPage = ref(1), personSearch = ref('');
const selectedPerson = ref('');
const busy = ref(false), loading = ref(false), error = ref(''), message = ref(''), dirty = ref(false);
watch(() => busy.value || uploading.value, value => emit('busy', value));
const creating = ref(false), sectionTab = ref('questions'), personPage = ref(1), storageWarning = ref('');
watch(() => [creating.value, creationMode.value], () => { if (creating.value) questionPage.value = 1; });
const additionIds = ref<string[]>([]), evidenceQuestion = ref<Dict | null>(null);
const creationDirty = computed(() => !!(filledManualQuestions.value.length || title.value || url.value || materialUrl.value || materialUrls.value.length || materialIds.value.length || materialFiles.value.length || participants.value.length));
const storage = resilientStorage('localStorage', () => { storageWarning.value = '浏览器暂不能保存草稿，请保持页面打开并先保存。'; });
const creationKey = () => 'clipflow:sharing:create:v1:' + props.userId;
const labels: Record<string, string> = { queued: '等待生成', generating: '正在生成', draft: '待核对发布', published: '答题中', closed: '已关闭', failed: '生成失败' };
const confirm = ref<{ open: boolean; action: string; title: string; message: string }>({ open: false, action: '', title: '', message: '' });
let request: AbortController | undefined, epoch = 0, timer: ReturnType<typeof setTimeout> | undefined, disposed = false;
let createId = randomHexId();

/* ---------- draft editor / detail views ---------- */
const questions = computed(() => (detail.value?.questions || []).slice((questionPage.value - 1) * PAGE_SIZE, questionPage.value * PAGE_SIZE));
const participantsView = computed(() => (detail.value?.participants || []).filter((p: Dict) => `${p.name} ${p.scope}`.includes(personSearch.value.trim())));
const pagedParticipants = computed(() => participantsView.value.slice((personPage.value - 1) * 12, personPage.value * 12));
const assignedCount = computed(() => (detail.value?.participants || []).filter((p: Dict) => !p.exempted).length);
const exemptedCount = computed(() => (detail.value?.participants || []).length - assignedCount.value);
const percent = computed(() => detail.value?.total ? Math.round(detail.value.completed * 100 / detail.value.total) : 0);
const isDraft = computed(() => ['draft', 'failed'].includes(detail.value?.status || ''));
const editable = computed(() => props.isAdmin && isDraft.value && !busy.value);
const canEditQuestions = computed(() => editable.value || (props.isAdmin && creating.value && manualEditing.value && !detail.value && !busy.value));
const counts = computed(() => {
  const list = detail.value?.questions || [];
  const by = (origin: string) => list.filter((q: Dict) => (q.origin || 'ai') === origin).length;
  const manual = by('manual'), ai = by('ai');
  const types = Object.fromEntries(TYPES.map(kind => [kind, list.filter((q: Dict) => q.type === kind).length]));
  const start = (questionPage.value - 1) * PAGE_SIZE;
  const pageAi = list.slice(start, start + PAGE_SIZE).filter((q: Dict) => (q.origin || 'ai') === 'ai').length;
  return { manual, ai, total: list.length, types, pageAi, aiLeft: AI_MAX - ai, manualSized: manual >= PAGE_SIZE };
});
const publishIssue = computed(() => {
  const c = counts.value;
  if (c.total < 5) return '至少需要5道题才能发布。';
  if (c.ai > AI_MAX) return `AI生成题超过 ${AI_MAX} 道，请删除或改为手动录入。`;
  if (c.total > TOTAL_MAX) return `手动与AI合计超过 ${TOTAL_MAX} 道。`;
  if (!assignedCount.value) return '请至少选择1位参加答题的参会人。';
  const unconfirmed = (detail.value?.questions || []).findIndex((q: Dict) => needsImageConfirmation(q) && q.image_confirmed !== true);
  if (unconfirmed >= 0) return `第${unconfirmed + 1}题需勾选“已对照原图核对题目与答案”后再发布。`;
  const incomplete = (detail.value?.questions || []).map((q: Dict, i: number) => questionProblem(q, i, c.ai)).find(Boolean);
  if (incomplete) return incomplete;
  return '';
});
const paperSizeHint = computed(() => counts.value.manualSized
  ? `已手动录入 ${counts.value.manual} 道，每人完整作答这 ${counts.value.manual} 道（不额外补AI题）。`
  : counts.value.ai === 0 ? `已手动录入 ${counts.value.manual} 道，还需补充 ${PAGE_SIZE - counts.value.manual} 道题后发布。`
  : `手动录入 ${counts.value.manual} 道（全部必答），不足5道时由AI生成题补足到5道。`);
const visiblePeople = computed(() => {
  const keyword = peopleSearch.value.trim();
  return keyword ? people.value.filter(p => `${p.name} ${p.scope}`.includes(keyword)) : people.value;
});
const participantOpenIds = computed(() => new Set(participants.value.map(p => p.open_id)));

/* ---------- participant answering views ---------- */
const paper = computed<Dict | null>(() => detail.value?.paper || null);
const myQuestions = computed(() => {
  const paperValue = paper.value;
  if (!paperValue) return detail.value?.questions || [];
  const byId = new Map<string, Dict>((detail.value?.my_questions || detail.value?.questions || []).map((q: Dict) => [q.id, q]));
  const ids: string[] = paperValue.question_ids || [];
  const ordered = ids.map(id => byId.get(id)).filter(Boolean);
  return ordered.length ? ordered : detail.value?.questions || [];
});
const submitted = computed(() => !!paper.value?.submitted_at);
const canAnswer = computed(() => !!paper.value && !submitted.value && detail.value?.status === 'published' && paper.value.open_id === props.userId);
const answerPages = computed(() => Math.max(1, Math.ceil(myQuestions.value.length / PAGE_SIZE)));
const answerPage = ref(1), answers = ref<Dict>({}), answerVersion = ref(0), answerError = ref('');
const answerBaseline = ref<Dict>({}), answerConflicts = ref<{ id: string; mine: unknown; latest: unknown }[]>([]);
let answerOperation: { fingerprint: string; id: string } | undefined;
const answerPageQuestions = computed(() => myQuestions.value.slice((answerPage.value - 1) * PAGE_SIZE, answerPage.value * PAGE_SIZE));
const answeredCount = computed(() => myQuestions.value.filter((q: Dict) => hasAnswer(q)).length);
const lastPageComplete = computed(() => answerPageQuestions.value.every((q: Dict) => hasAnswer(q)));
const resultDetail = ref<Dict | null>(null), resultLoading = ref(false), resultError = ref('');
const resultPaper = computed(() => resultDetail.value?.open_id === selectedPerson.value ? resultDetail.value : (detail.value?.results || []).find((p: Dict) => p.open_id === selectedPerson.value));
const resultPage = ref(1);
const resultPages = computed(() => Math.max(1, Math.ceil((resultPaper.value?.question_ids || []).length / PAGE_SIZE)));
const resultQuestions = computed(() => {
  const list = resultPaper.value?.question_ids || [], start = (resultPage.value - 1) * PAGE_SIZE;
  return list.slice(start, start + PAGE_SIZE).map((id: string) => detail.value?.questions.find((q: Dict) => q.id === id)).filter(Boolean);
});

/* ---------- helpers ---------- */
function changed() { dirty.value = true; emit('dirty', true); }
function questionChanged(q: Dict) { q.image_confirmed = false; if (detail.value) changed(); }
function imageIds(q: Dict): string[] { return Array.isArray(q.image_ids) ? q.image_ids : (q.images || []).map((image: Dict) => image.id); }
function questionImages(q: Dict): Dict[] {
  if (!props.isAdmin || !isDraft.value) return q.images || [];
  const images = new Map<string, Dict>([...(q.images || []), ...(detail.value?.source_images || [])].map((image: Dict) => [image.id, image]));
  return imageIds(q).map(id => images.get(id)).filter((image): image is Dict => !!image);
}
function needsImageConfirmation(q: Dict) {
  return questionImages(q).some(image => image.mode === 'vision');
}
function selectQuestionImages(q: Dict, ids: string[]) {
  if (!canEditQuestions.value || ids.length > 3 || new Set(ids).size !== ids.length
    || ids.some(id => !(detail.value?.source_images || []).some((image: Dict) => image.id === id))) return;
  q.image_ids = ids;
  questionChanged(q);
}
function clean() { dirty.value = false; emit('dirty', false); }
function showAnswer(value: unknown, q?: Dict) {
  if (q?.type === 'interview') return typeof value === 'string' ? value.trim() : '';
  return typeof value === 'string' ? value : Array.isArray(value) ? value.map((i: number) => letters(Number(i))).join('、') : '';
}
function correctAnswer(q: Dict) {
  return (q.correct || []).map((i: number) => `${letters(Number(i))}. ${q.options?.[i] ?? ''}`).join('；');
}
function hasAnswer(q: Dict) {
  const value = answers.value[q.id];
  return q.type === 'interview' ? typeof value === 'string' && !!value.trim() : Array.isArray(value) && value.length > 0;
}
function questionProblem(q: Dict, index: number, pageAi: number) {
  const base = `第${index + 1}题`;
  if (!String(q.stem || '').trim()) return `${base}缺少题干。`;
  if (String(q.stem).trim().length > 500) return `${base}题干超过500字。`;
  if (!['single', 'multiple', 'interview'].includes(q.type)) return `${base}题型无效。`;
  if ((q.origin || 'ai') === 'ai' && pageAi > 0 && !String(q.evidence || '').trim()) return `${base}为AI生成题，需保留纪要/图片依据。`;
  if (['single', 'multiple'].includes(q.type)) {
    const options = (q.options || []).map((o: string) => String(o || '').trim());
    if (options.length < CHOICE_MIN || options.length > CHOICE_MAX) return `${base}需设置${CHOICE_MIN}-${CHOICE_MAX}个选项。`;
    if (options.some((o: string) => !o)) return `${base}存在空白选项。`;
    const correct: number[] = Array.from(new Set<number>(((q.correct || []) as number[]).map(Number))).sort((a: number, b: number) => a - b);
    if (!correct.length) return `${base}尚未设置正确答案。`;
    if (correct.some((i: number) => i < 0 || i >= options.length)) return `${base}正确选项超出范围。`;
    if (q.type === 'single' && correct.length !== 1) return `${base}为单选题，只能设置1个正确答案。`;
  } else if (!String(q.reference_answer || '').trim()) return `${base}为问答题，需填写参考答案。`;
  if (String(q.explanation || '').trim().length > 600) return `${base}答案解析超过600字。`;
  if (String(q.evidence || '').trim().length > 800) return `${base}纪要/图片依据超过800字。`;
  if (!String(q.explanation || '').trim()) return `${base}需填写答案解析。`;
  if (q.type === 'multiple' && (q.correct || []).length < 2) return `${base}为多选题，需至少2个正确选项。`;
  return '';
}
const questionIssues = computed(() => {
  const start = (questionPage.value - 1) * PAGE_SIZE;
  return questions.value.map((q: Dict, i: number) => questionProblem(q, start + i, counts.value.pageAi));
});

/* ---------- creation ---------- */
function toggleMaterialUrl(value: string, checked: boolean) {
  materialUrls.value = checked ? [...new Set([...materialUrls.value, value])] : materialUrls.value.filter(item => item !== value);
}
function addMaterialUrl() {
  let value = materialUrl.value.trim();
  if (!value) return;
  try {
    const url=new URL(value);
    if(url.protocol!=='https:'||url.hostname!=='vnet.feishu.cn'||url.port||url.username||url.password||!/^\/(docx|wiki|file)\/[A-Za-z0-9]+\/?$/.test(url.pathname))throw new Error();
    value=url.origin+url.pathname.replace(/\/$/,'');
  } catch { error.value = '资料链接支持企业飞书文档、Wiki和文件链接；其它文件请上传。'; return; }
  if (materialUrls.value.length + materialIds.value.length >= 10) { error.value = '每批最多10份材料。'; return; }
  if (!materialUrls.value.includes(value)) materialUrls.value = [...materialUrls.value, value];
  materialUrl.value = '';
  error.value = '';
}
function pickFiles(event: Event) {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  const accepted = files.filter(file => file.size > 0 && /\.(pdf|docx|xlsx|xlsm|pptx)$/i.test(file.name));
  if (accepted.length !== files.length) error.value = '请选择非空PDF、DOCX、XLSX、XLSM或PPTX。';
  if (materialUrls.value.length + materialIds.value.length + materialFiles.value.length + accepted.length > 10) { error.value = '每批最多10份材料。'; return; }
  materialFiles.value = [...materialFiles.value, ...accepted.filter(file => !materialFiles.value.some(old => old.name === file.name && old.size === file.size))];
  input.value = '';
}
function removeFile(file: File) { materialFiles.value = materialFiles.value.filter(item => item !== file); }
async function uploadMaterials() {
  if (busy.value || uploading.value || !props.isAdmin || !materialFiles.value.length) return;
  uploading.value = true; error.value = ''; message.value = '';
  try {
    const files = materialFiles.value.slice();
    for (let index = 0; index < files.length; index++) {
      const file = files[index], body = new FormData(); body.append('files', file);
      message.value = `正在上传 ${index + 1}/${files.length}：${file.name}`;
      const result = await requestLearning({}, '/sharing/materials', 'POST', body);
      if (disposed) return;
      const items: Dict[] = Array.isArray(result?.items) ? result.items : [];
      if (items.length !== 1 || !items[0].id) throw new Error('材料回执未确认，已成功上传的材料保留，请重试当前文件。');
      for (const item of items) { if (!materialIds.value.includes(item.id)) materialIds.value.push(item.id); uploadedNames.value[item.id] = item.name; }
      materialFiles.value = materialFiles.value.filter(value => value !== file);
    }
    message.value = `材料上传完成，共 ${materialIds.value.length} 份。`;
  } catch (e) { error.value = (e as Error).message; }
  finally { uploading.value = false; }
}
const canCreate = computed(() => !busy.value && !uploading.value
  && (manualOnly.value ? participants.value.length > 0 : (
  (!autoGenerate.value && !importing.value || !!(includeMinutes.value && url.value.trim()) || !!materialUrl.value.trim() || materialUrls.value.length > 0 || materialIds.value.length > 0 || importing.value && materialFiles.value.length > 0)
  && (!!(includeMinutes.value && url.value.trim()) || participants.value.length > 0)
  && (importing.value || !autoGenerate.value || questionTypes.value.length > 0) && (importing.value || !materialFiles.value.length))));
function toggleType(kind: string, checked: boolean) {
  questionTypes.value = checked ? TYPES.filter(item => item === kind || questionTypes.value.includes(item)) : questionTypes.value.filter(item => item !== kind);
}

/* ---------- participant picker ---------- */
async function loadPeople(force = false) {
  if (peopleLoading.value || (people.value.length && !force)) return;
  peopleLoading.value = true;
  try {
    const result = await requestLearning({}, '/sharing/people');
    people.value = Array.isArray(result?.items) ? result.items : [];
  } catch (e) { if (!force) error.value = (e as Error).message; }
  finally { peopleLoading.value = false; }
}
function knownParticipant(person: Dict): Dict {
  const found = (detail.value?.participants || []).find((p: Dict) => p.open_id === person.open_id || (person.id && p.person_id === person.id));
  return found || { ...person, exempted: false };
}
function addParticipant(person: Dict) {
  if (busy.value) return;
  if (participantOpenIds.value.has(person.open_id)) { message.value = `${person.name} 已在答题名单中。`; return; }
  participants.value = [...participants.value, knownParticipant(person)];
}
function addDraftParticipant(person: Dict) {
  if (!editable.value || !person.person_id || detail.value!.participants.some((p: Dict) => p.open_id === person.open_id)) return;
  detail.value!.participants.push({ ...person, exempted: false }); additionIds.value.push(person.person_id); changed();
}
function removeParticipant(person: Dict) {
  if (busy.value) return;
  participants.value = participants.value.filter(p => p.open_id !== person.open_id);
}
function setParticipation(person: Dict, checked: boolean) {
  if (!props.isAdmin || !isDraft.value || busy.value) return;
  person.exempted = !checked;
  changed();
}

/* ---------- manual question editor ---------- */
function newQuestion(type: string): Dict {
  return { id: `new-${randomHexId()}`, type, origin: 'manual', stem: '', options: type === 'interview' ? [] : ['', '', '', ''], correct: [], reference_answer: '', explanation: '', evidence: '', image_ids: [], image_confirmed: false };
}
function countOf(origin: string) { return (detail.value?.questions || []).filter((q: Dict) => (q.origin || 'ai') === origin).length; }
function addQuestion(type: string) {
  if (!canEditQuestions.value) return;
  if (countOf('ai') > AI_MAX) { error.value = `AI生成题已超过 ${AI_MAX} 道，请先删除多余的AI题。`; return; }
  const target = detail.value?.questions || manualQuestions.value;
  if (target.length >= TOTAL_MAX) { error.value = `手动与AI合计最多 ${TOTAL_MAX} 道。`; return; }
  const next = [...target, newQuestion(type)];
  if (detail.value) { detail.value.questions = next; changed(); }
  else manualQuestions.value = next;
  questionPage.value = Math.ceil(next.length / PAGE_SIZE);
}
function removeQuestion(q: Dict) {
  if (!canEditQuestions.value) return;
  const next = (detail.value?.questions || manualQuestions.value).filter((item: Dict) => item.id !== q.id);
  if (detail.value) { detail.value.questions = next; changed(); }
  else manualQuestions.value = next;
  questionPage.value = Math.min(questionPage.value, Math.max(1, Math.ceil(next.length / PAGE_SIZE)));
}
function switchType(q: Dict, next: string) {
  if (!canEditQuestions.value || q.type === next) return;
  if (next === 'interview') { q.options = []; q.correct = []; if (typeof q.reference_answer !== 'string') q.reference_answer = ''; }
  else {
    const options = Array.isArray(q.options) && q.options.length ? q.options : ['', '', '', ''];
    q.options = options; q.correct = next === 'single' ? (q.correct || []).slice(0, 1) : (q.correct || []);
  }
  q.type = next;
  questionChanged(q);
}
function addOption(q: Dict) {
  if (!canEditQuestions.value || !Array.isArray(q.options) || q.options.length >= CHOICE_MAX) return;
  q.options.push(''); questionChanged(q);
}
function removeOption(q: Dict, index: number) {
  if (!canEditQuestions.value || !Array.isArray(q.options) || q.options.length <= CHOICE_MIN) return;
  q.options.splice(index, 1);
  q.correct = (q.correct || []).map((i: number) => (i === index ? -1 : i > index ? i - 1 : i)).filter((i: number) => i >= 0);
  questionChanged(q);
}
function choose(q: Dict, index: number, checked: boolean) {
  if (!canEditQuestions.value) return;
  const current: number[] = q.correct || [];
  q.correct = q.type === 'single' ? (checked ? [index] : []) : checked ? [...new Set([...current, index])].sort((a, b) => a - b) : current.filter((i: number) => i !== index);
  questionChanged(q);
}
/* ---------- requests ---------- */
async function load(id = detail.value?.id, silent = false) {
  const ticket = ++epoch; request?.abort(); request = new AbortController();
  if (!silent) loading.value = true;
  error.value = '';
  try {
    let result = await requestLearning({ signal: request.signal }, id ? `/sharing/${id}${silent ? '?progress=1' : '?compact=1'}` : `/sharing?page=${page.value}`);
    if (disposed || ticket !== epoch) return;
    if (id) {
      if (silent && detail.value && (result.version !== detail.value.version || result.status !== detail.value.status || result.paper?.submitted_at !== detail.value.paper?.submitted_at)) {
        result = await requestLearning({ signal: request.signal }, `/sharing/${id}?compact=1`);
        if (disposed || ticket !== epoch) return;
      } else if (silent) result = { ...detail.value, ...result };
      if (!dirty.value) detail.value = result;
      if (!editingAnswers.value) resetAnswers(true);
    } else { rows.value = result.items || []; total.value = result.total; }
  } catch (e) { if (!disposed && ticket === epoch) error.value = (e as Error).message; }
  finally { if (ticket === epoch) loading.value = false; }
}
function resetAnswers(force = false) {
  const fromServer: Dict = submitted.value ? Object.fromEntries(myQuestions.value.map((q: Dict) => [q.id, q.selected])) : paper.value?.draft_answers || {};
  if (!force && editingAnswers.value) return;
  answers.value = Object.fromEntries(myQuestions.value.map((q: Dict) => {
    const saved = fromServer[q.id];
    return [q.id, q.type === 'interview' ? (typeof saved === 'string' ? saved : '') : (Array.isArray(saved) ? saved.map(Number).filter((i: number) => Number.isInteger(i) && i >= 0 && i < (q.options || []).length) : [])];
  }));
  answerVersion.value = Number(paper.value?.progress_version ?? 0);
  answerBaseline.value = JSON.parse(JSON.stringify(answers.value)); answerConflicts.value = [];
  answerPage.value = Math.min(answerPage.value, answerPages.value);
  editingAnswers.value = false;
  answerOperation = undefined;
  emit('dirty', dirty.value);
  answerError.value = '';
}
const editingAnswers = ref(false);
async function openBatch(row: Dict) {
  if (busy.value || uploading.value) return;
  if (creationDirty.value) { error.value = '新建草稿已保留，请先完成或放弃后打开其他批次。'; return; }
  clean(); questionPage.value = 1; personSearch.value = ''; selectedPerson.value = ''; resultPage.value = 1;
  answerPage.value = 1; editingAnswers.value = false;
  await load(row.id);
  if (canAnswer.value) resetAnswers(true);
}
async function mutate(action: string) {
  if (busy.value) return;
  if (action === 'create' && importing.value && materialFiles.value.length) { await uploadMaterials(); if (materialFiles.value.length) return; }
  if (action === 'publish' && (dirty.value || publishIssue.value)) { error.value = dirty.value ? '请先保存当前修改。' : publishIssue.value; return; }
  if (action === 'create' && !canCreate.value) { error.value = manualOnly.value ? '请先选择答题人员。' : '请核对出题资料和答题名单，或选择纯手工出题。'; return; }
  if (action === 'create' && !manualOnly.value && materialUrl.value.trim()) { addMaterialUrl(); if (materialUrl.value.trim()) return; }
  busy.value = true; error.value = ''; message.value = '';
  const generating = !manualOnly.value && (importing.value || autoGenerate.value);
  try {
    let result: Dict;
    if (action === 'create') {
      const minutes = !manualOnly.value && includeMinutes.value && url.value.trim() ? url.value.trim() : '';
      result = await requestLearning({}, '/sharing', 'POST', {
        url: minutes, title: title.value.trim(), question_types: questionTypes.value.length ? questionTypes.value : ['single'],
        operation_id: createId, material_urls: manualOnly.value ? [] : materialUrls.value, material_ids: manualOnly.value ? [] : materialIds.value,
        participant_ids: participants.value.map(p => p.person_id).filter(Boolean),
        auto_generate: generating, import_questions: importing.value,
        ...(manualEditing.value ? { initial_questions: filledManualQuestions.value } : {}),
      });
      createId = randomHexId(); url.value = ''; materialUrl.value = ''; title.value = ''; materialUrls.value = []; materialIds.value = []; uploadedNames.value = {};
      participants.value = []; materialFiles.value = []; manualQuestions.value = [];
      autoGenerate.value = true; includeMinutes.value = true; creationMode.value = 'materials';
      storage.removeItem(creationKey()); creating.value = false;
    } else {
      const id = detail.value!.id;
      const body: Dict = { version: detail.value!.version };
      if (action === 'save') {
        body.questions = detail.value!.questions.map((q: Dict) => {
          const { images, ...question } = q;
          return { ...question, image_ids: imageIds(q), image_confirmed: q.image_confirmed === true };
        });
        body.excluded_open_ids = detail.value!.participants.filter((p: Dict) => p.exempted).map((p: Dict) => p.open_id);
        if (additionIds.value.length) body.participant_ids = additionIds.value;
      }
      result = await requestLearning({}, `/sharing/${id}${action === 'save' ? '' : '/' + action}`, action === 'save' ? 'PUT' : 'POST', body);
      if (action === 'save') { clean(); additionIds.value = []; }
    }
    await load(result.id);
    const doneMessages: Record<string, string> = { save: '题目与答题名单已保存。', publish: '已发布答题，正在后台发送答题卡并同步题目到题库。', retry: '已重新排队生成。', close: '已关闭本批答题及未答提醒，历史成绩保留。', 'retry-delivery': '已继续失败的原任务，不重新抽题，也不重复发送已成功的卡片。' };
    message.value = action === 'create'
      ? generating ? '已进入生成队列，生成完成后可核对题目。' : '已创建草稿，可手动录入题目，不使用AI生成。'
      : doneMessages[action] || '已完成。';
  } catch (e) { error.value = (e as Error).message; }
  finally { busy.value = false; }
}
function ask(action: string) {
  const size = counts.value.manualSized ? counts.value.manual : 5;
  confirm.value = { open: true, action,
    title: action === 'discard' ? '放弃未保存修改' : action === 'publish' ? '发布分享会练习' : action === 'close' ? '关闭此批次' : action === 'remove-participant' ? '移出答题名单' : '重试未确认发送',
    message: action === 'publish' ? `${publishIssue.value ? publishIssue.value + ' ' : ''}确认已核对全部${counts.value.total}道题（手动${counts.value.manual}道、AI${counts.value.ai}道）？将向${assignedCount.value}位参会人真实发送每人${size}题的飞书答题卡，${exemptedCount.value}人本次不做；全部题目同步至纪要题库。发布后不能修改题目和答题名单。`
      : action === 'discard' ? '放弃本页尚未保存的题目和答题名单修改，读取服务器最新版本。'
      : action === 'close' ? '关闭后不能继续提交，停止未答提醒；已有答题及正确答案保留。'
      : action === 'remove-participant' ? '移出后该参会人不再收到答题卡，草稿发布前可随时重新加入。'
      : '将继续失败的原任务，不重新分配题目。若上次网络中断但对方已经收到，超出飞书去重时限后可能再次收到相同卡片，请先核对。' };
}
function resolved(ok: boolean) { const action = confirm.value.action; confirm.value = { ...confirm.value, open: false }; if (!ok) return; if (action === 'discard-create') { url.value = ''; title.value = ''; materialUrl.value = ''; materialUrls.value = []; materialIds.value = []; participants.value = []; materialFiles.value = []; manualQuestions.value = []; storage.removeItem(creationKey()); creating.value = false; } else if (action === 'delete') void deleteBatch(); else if (action === 'discard') { clean(); additionIds.value = []; editingAnswers.value = false; void load(); } else void mutate(action); }
function closeCreate() { if (uploading.value || busy.value) return; if (creationDirty.value) confirm.value = { open: true, action: 'discard-create', title: '放弃新建草稿？', message: '将清空本次未创建的标题、材料和人员选择，已经发布的批次不受影响。' }; else creating.value = false; }
function askDelete() { confirm.value = { open: true, action: 'delete', title: '删除分享会批次？', message: '删除后停止本批答题、提醒和后续同步；已上传到多维题库的题目不会删除。' }; }
async function deleteBatch() {
  if (!props.isAdmin || !detail.value || busy.value) return;
  busy.value = true; error.value = '';
  try { await requestLearning({}, `/sharing/${detail.value.id}/delete`, 'POST', { version: detail.value.version }); clean(); editingAnswers.value = false; detail.value = null; selectedPerson.value = ''; message.value = '批次已删除，已上传题目保留。'; await load(); if (!rows.value.length && page.value > 1) { page.value--; await load(); } }
  catch (e) { error.value = (e as Error).message; } finally { busy.value = false; }
}
async function back() { if (busy.value) return; if (dirty.value || editingAnswers.value) { error.value = '请先保存当前修改或答题进度，再返回列表。'; return; } detail.value = null; selectedPerson.value = ''; await load(); }

/* ---------- answering ---------- */
function pick(q: Dict, index: number, checked: boolean) {
  if (!canAnswer.value || busy.value) return;
  const current: number[] = Array.isArray(answers.value[q.id]) ? answers.value[q.id] : [];
  answers.value = { ...answers.value, [q.id]: q.type === 'single' ? (checked ? [index] : []) : checked ? [...new Set([...current, index])].sort((a, b) => a - b) : current.filter(i => i !== index) };
  editingAnswers.value = true; emit('dirty', true);
  answerError.value = '';
}
function writeAnswer(q: Dict, value: string) { if (!canAnswer.value || busy.value) return; answers.value = { ...answers.value, [q.id]: value }; editingAnswers.value = true; emit('dirty', true); answerError.value = ''; }
function answerPayload() {
  return Object.fromEntries(answerPageQuestions.value.map((q: Dict) => [q.id, q.type === 'interview' ? (answers.value[q.id] || '') : (answers.value[q.id] || []).map(String)]));
}
async function sendAnswers(submit: boolean, target = answerPage.value) {
  if (busy.value || !canAnswer.value || !detail.value || !paper.value || answerConflicts.value.length) return;
  const round = answerPages.value;
  if (submit && answerPage.value !== round) { answerError.value = '只有最后一页可以提交，请先逐页保存进度。'; return; }
  if (submit && answeredCount.value !== myQuestions.value.length) { answerError.value = `还有 ${myQuestions.value.length - answeredCount.value} 题未作答，全部作答后才能提交。`; return; }
  const payload = { page: answerPage.value - 1, answers: answerPayload(), version: answerVersion.value, submit, target_page: target - 1 };
  const fingerprint = JSON.stringify(payload);
  if (!answerOperation || answerOperation.fingerprint !== fingerprint) answerOperation = { fingerprint, id: randomHexId() };
  busy.value = true; answerError.value = ''; error.value = ''; message.value = '';
  try {
    await requestLearning({}, `/sharing/${detail.value.id}/answers`, 'POST', { ...payload, operation_id: answerOperation.id });
    const loaded = await requestLearning({}, `/sharing/${detail.value.id}`);
    if (!disposed) {
      detail.value = loaded;
      resetAnswers(true);
    }
    if (submit) {
      answerPage.value = 1; editingAnswers.value = false;
      message.value = '已提交本批全部题目，可查看答案与解析。';
    } else {
      answerPage.value = target;
      message.value = '答题进度已保存，尚未提交整卷。';
    }
  } catch (e) {
    answerError.value = (e as Error).message;
    if ((e as { status?: number }).status === 409) await reconcileAnswers();
  }
  finally { busy.value = false; }
}
async function reconcileAnswers() {
  const mine = JSON.parse(JSON.stringify(answers.value)), baseline = answerBaseline.value;
  try {
    const latest = await requestLearning({}, `/sharing/${detail.value!.id}`);
    if (disposed) return;
    detail.value = latest; resetAnswers(true);
    if (submitted.value) { answerError.value = '另一端已提交，保留首次成绩。当前显示已提交答卷，未覆盖。'; return; }
    if (latest.status !== 'published') { answers.value = mine; editingAnswers.value = true; answerError.value = '此批次已关闭，本页未提交输入仍保留，不能继续保存或提交。'; return; }
    for (const q of myQuestions.value) {
      if (JSON.stringify(mine[q.id]) === JSON.stringify(baseline[q.id])) continue;
      if (JSON.stringify(answers.value[q.id]) !== JSON.stringify(baseline[q.id]) && JSON.stringify(answers.value[q.id]) !== JSON.stringify(mine[q.id]))
        answerConflicts.value.push({ id: q.id, mine: mine[q.id], latest: answers.value[q.id] });
      answers.value[q.id] = mine[q.id];
    }
    editingAnswers.value = true;
    answerError.value = answerConflicts.value.length ? '其他页面已修改同一题，请逐题选择要保留的答案；本页输入未丢失。' : '已合并另一端进度并保留本页输入，请确认后保存。';
  } catch { answerError.value = '最新答题进度暂不可用，本页输入已保留。请稍后重新保存。'; }
}
function resolveAnswer(id: string, useMine: boolean) { const value = answerConflicts.value.find(item => item.id === id); if (value) answers.value[id] = useMine ? value.mine : value.latest; answerConflicts.value = answerConflicts.value.filter(item => item.id !== id); }
const deliveryLabel = (value: string) => (({ sent: '已发送', pending: '等待发送', failed: '发送失败', skipped: '已停止' } as Record<string, string>)[value] || '暂无');
function showEvidence(q: Dict) { evidenceQuestion.value = q; }
const evidenceLocations = computed(() => detail.value?.evidence_locations?.[evidenceQuestion.value?.id] || []);
async function selectResult(openId: string) {
  selectedPerson.value = selectedPerson.value === openId ? '' : openId; resultDetail.value = null; resultPage.value = 1; resultError.value = '';
  if (!selectedPerson.value || !detail.value) return;
  resultLoading.value = true; const batch = detail.value.id;
  try { const value = await requestLearning({}, `/sharing/${batch}?person=${encodeURIComponent(openId)}`); if (!disposed && detail.value?.id === batch && selectedPerson.value === openId) resultDetail.value = value; }
  catch (e) { if (!disposed && selectedPerson.value === openId) resultError.value = (e as Error).message; }
  finally { if (selectedPerson.value === openId) resultLoading.value = false; }
}

/* ---------- polling ---------- */
function schedule() {
  timer = setTimeout(async () => {
    const fingerprint = () => `${detail.value?.progress_version ?? ''}|${detail.value?.status ?? ''}`;
    const before = fingerprint();
    if (!disposed && !document.hidden && !busy.value && !dirty.value && !editingAnswers.value && detail.value
      && (['queued', 'generating', 'published'].includes(detail.value.status) || (detail.value.bank_sync?.total > detail.value.bank_sync?.synced + detail.value.bank_sync?.failed))) await load(detail.value.id, true);
    else if (!disposed && !document.hidden && !busy.value && !uploading.value && !detail.value && !creating.value) await load(undefined, true);
    if (!disposed && fingerprint() !== before && paper.value && !canAnswer.value) resetAnswers(true);
    if (!disposed) schedule();
  }, 8000);
}
watch(() => detail.value?.id, () => { answerPage.value = 1; editingAnswers.value = false; });
watch(personSearch, () => { selectedPerson.value = ''; });
watch(personSearch, () => { personPage.value = 1; });
watch(() => detail.value?.id, () => { sectionTab.value = paper.value ? 'mine' : ['draft', 'failed', 'queued', 'generating'].includes(detail.value?.status) ? 'questions' : 'people'; personPage.value = 1; additionIds.value = []; });
watch(() => [dirty.value, editingAnswers.value, creationDirty.value], () => emit('dirty', dirty.value || editingAnswers.value || creationDirty.value), { flush: 'sync' });
watch(() => [title.value, url.value, materialUrl.value, materialUrls.value, materialIds.value, participants.value, autoGenerate.value, includeMinutes.value, questionTypes.value, creationMode.value, manualQuestions.value], () => {
  if (!props.isAdmin || !props.userId) return;
  if (!creationDirty.value) { storage.removeItem(creationKey()); return; }
  storage.setItem(creationKey(), JSON.stringify({ schema: 1, userId: props.userId, createId, title: title.value, url: url.value, materialUrl: materialUrl.value, materialUrls: materialUrls.value, materialIds: materialIds.value, uploadedNames: uploadedNames.value, participants: participants.value, autoGenerate: autoGenerate.value, includeMinutes: includeMinutes.value, questionTypes: questionTypes.value, creationMode: creationMode.value, manualQuestions: manualQuestions.value }));
}, { deep: true });
onMounted(() => {
  try {
    const saved = JSON.parse(storage.getItem(creationKey()) || 'null');
    if (props.isAdmin && saved?.schema === 1 && saved.userId === props.userId) {
      title.value = String(saved.title || ''); url.value = String(saved.url || ''); materialUrl.value = String(saved.materialUrl || '');
      materialUrls.value = Array.isArray(saved.materialUrls) ? saved.materialUrls : []; materialIds.value = Array.isArray(saved.materialIds) ? saved.materialIds : [];
      participants.value = Array.isArray(saved.participants) ? saved.participants : []; uploadedNames.value = saved.uploadedNames || {};
      autoGenerate.value = saved.autoGenerate !== false; includeMinutes.value = saved.includeMinutes !== false; questionTypes.value = Array.isArray(saved.questionTypes) ? saved.questionTypes : ['single'];
      creationMode.value = saved.creationMode === 'import' ? 'import' : saved.creationMode === 'manual' ? 'manual' : 'materials';
      manualQuestions.value = Array.isArray(saved.manualQuestions) ? saved.manualQuestions.slice(0, TOTAL_MAX) : [];
      if (manualEditing.value && !manualQuestions.value.length) manualQuestions.value = [newQuestion('single')];
      if (/^[a-f0-9]{32}$/.test(saved.createId)) createId = saved.createId;
      creating.value = creationDirty.value; message.value = '已恢复本账号的新建草稿；尚未上传的文件需重新选择。';
    }
  } catch { storageWarning.value = '上次新建草稿无法读取，请重新核对填写。'; }
  const batchId = logicalUrl().searchParams.get('batch_id');
  if (batchId && !/^[a-f0-9]{32}$/.test(batchId)) error.value = '分享会批次编号无效。';
  else void load(batchId || undefined);
  schedule();
});
onBeforeUnmount(() => { disposed = true; epoch++; request?.abort(); clearTimeout(timer); });
</script>

<template>
  <section class="sharing-exercises" :aria-busy="busy || loading">
    <div v-if="error" role="alert" data-v2-inline-feedback class="sharing-alert error">{{ error }}</div>
    <div v-if="message" role="status" data-v2-inline-feedback class="sharing-alert success">{{ message }}</div>
    <div v-if="storageWarning" role="alert" data-v2-inline-feedback class="sharing-alert error">{{ storageWarning }}</div>
    <header class="sharing-toolbar" :class="{ 'sharing-creating-toolbar': creating && !detail }">
      <div class="sharing-title"><button v-if="detail" type="button" :disabled="busy" @click="back"><ArrowLeft :size="16" />批次列表</button><button v-else-if="creating" type="button" :disabled="busy || uploading" @click="closeCreate"><ArrowLeft :size="16" />返回批次列表</button><h2>{{ detail ? detail.title : '分享会练习' }}</h2></div>
      <button type="button" :disabled="busy || loading || dirty || editingAnswers" title="刷新分享会练习" @click="load()"><RefreshCw :size="16" :class="{ spin: loading }" />刷新</button>
      <button v-if="isAdmin && !detail && !creating" type="button" class="primary" @click="creationMode = 'materials'; autoGenerate = true; creating = true"><Plus :size="16" />AI出题</button>
      <button v-if="isAdmin && !detail && !creating" type="button" @click="creationMode = 'manual'; creating = true"><FileText :size="16" />人工出题</button>
      <button v-if="isAdmin && !detail && !creating" type="button" @click="creationMode = 'import'; creating = true"><Upload :size="16" />导入表格/文档</button>
      <button v-if="isAdmin && detail" type="button" class="sharing-danger" :disabled="busy || uploading" @click="askDelete"><Trash2 :size="16" />删除批次</button>
    </header>
    <template v-if="!detail">
      <form v-if="isAdmin && creating" class="sharing-create" aria-label="新建分享会练习" @submit.prevent="mutate('create')">
        <div class="sharing-create-fields" :class="{ 'sharing-import-fields': importing }">
          <fieldset class="sharing-mode" :disabled="busy || uploading">
            <legend>出题方式</legend>
            <NRadioGroup v-model:value="creationMode" name="sharing-creation-mode" aria-label="出题方式" size="large" :disabled="busy || uploading">
              <NRadioButton value="materials">资料出题</NRadioButton><NRadioButton value="manual">纯手工出题</NRadioButton><NRadioButton v-if="importing" value="import">文件导入</NRadioButton>
            </NRadioGroup>
          </fieldset>
          <label for="sharing-title" class="sharing-create-title"><span>练习标题（可选）</span><input id="sharing-title" v-model="title" type="text" maxlength="120" :placeholder="manualOnly ? '请输入练习标题' : '留空则使用纪要或资料名称'" :disabled="busy" /></label>
        </div>
        <section v-if="manualEditing" class="sharing-manual-create" aria-label="人工题目填写">
          <h3>填写题目与答案</h3><p class="sharing-hint">直接手工填写，不调用AI；可以先保存未填完的草稿，发布前须补齐正确答案和解析。</p>
          <div class="sharing-editor-bar"><button v-for="(label, kind) in typeLabels" :key="kind" type="button" :disabled="busy || manualQuestions.length >= TOTAL_MAX" @click="addQuestion(kind)"><Plus :size="15" />新增{{ label }}</button></div>
          <div class="sharing-question-grid is-editing">
          <article v-for="(q, i) in manualQuestions.slice((questionPage - 1) * PAGE_SIZE, questionPage * PAGE_SIZE)" :key="q.id" class="sharing-question">
            <header><strong>第 {{ (questionPage - 1) * PAGE_SIZE + i + 1 }} 题</strong><label>题型<select :value="q.type" :disabled="busy" @change="switchType(q, ($event.target as HTMLSelectElement).value)"><option v-for="(label, kind) in typeLabels" :key="kind" :value="kind">{{ label }}</option></select></label><button type="button" :disabled="busy" @click="removeQuestion(q)">删除此题</button></header>
            <label :for="`sharing-${q.id}`">题干<textarea :id="`sharing-${q.id}`" v-model="q.stem" rows="2" maxlength="500" :disabled="busy" @input="questionChanged(q)" /></label>
            <template v-if="q.type === 'interview'">
              <label>参考答案（问答题不自动判分）<textarea v-model="q.reference_answer" rows="3" maxlength="1000" :disabled="busy" @input="questionChanged(q)" /></label>
            </template>
            <template v-else>
              <p class="sharing-option-hint">正确选项（{{ q.type === 'single' ? '单选' : '多选' }}），选项需 {{ CHOICE_MIN }}-{{ CHOICE_MAX }} 个。</p>
              <div class="sharing-option-grid">
              <div v-for="(option, n) in q.options" :key="n" class="sharing-option"><input :type="q.type === 'single' ? 'radio' : 'checkbox'" :name="`sharing-answer-${q.id}`" :checked="q.correct.includes(n)" :aria-label="`第${(questionPage - 1) * PAGE_SIZE + i + 1}题选项${letters(n)}为正确答案`" :disabled="busy" @change="choose(q, Number(n), ($event.target as HTMLInputElement).checked)" /><label :for="`sharing-option-${q.id}-${n}`">{{ letters(Number(n)) }}</label><textarea :id="`sharing-option-${q.id}-${n}`" v-model="q.options[n]" rows="1" maxlength="300" :disabled="busy" @input="questionChanged(q)" /><button type="button" class="sharing-chip-x" :aria-label="`删除第${(questionPage - 1) * PAGE_SIZE + i + 1}题选项${letters(Number(n))}`" :disabled="busy || !canEditQuestions || q.options.length <= CHOICE_MIN" @click="removeOption(q, Number(n))"><X :size="14" /></button></div>
              </div>
              <button type="button" :disabled="!canEditQuestions || q.options.length >= CHOICE_MAX" @click="addOption(q)"><Plus :size="15" />增加选项</button>
            </template>
            <label>答案解析<textarea v-model="q.explanation" rows="2" maxlength="600" :disabled="busy" @input="questionChanged(q)" /></label>
          </article>
          </div>
          <div class="sharing-pagination"><button type="button" :disabled="questionPage <= 1" @click="questionPage--">上一页</button><span>第 {{ questionPage }} / {{ Math.max(1, Math.ceil(manualQuestions.length / PAGE_SIZE)) }} 页 · 共 {{ manualQuestions.length }} 题</span><button type="button" :disabled="questionPage * PAGE_SIZE >= manualQuestions.length" @click="questionPage++">下一页</button></div>
        </section>
        <fieldset v-if="!manualOnly" class="sharing-sources" :disabled="busy">
          <legend>{{ importing ? '导入含有已有题目的表格或文档' : '出题来源' }}</legend>
          <p v-if="importing" class="sharing-hint">自动提取题干、选项、原答案和解析，不生成新题。缺项留空待补齐，导入后可编辑核对；支持XLSX、XLSM、DOCX、PDF和PPTX。</p>
          <p class="sharing-hint">支持飞书文档、Wiki和上传材料。文档没有参会人名单时，请在下方选择答题人员。</p>
          <div class="sharing-source"><label class="sharing-check sharing-minutes-toggle"><input v-model="includeMinutes" type="checkbox" :disabled="busy" /><Link :size="16" />使用飞书纪要</label>
            <input v-model="url" type="url" aria-label="飞书纪要链接" maxlength="2048" placeholder="https://vnet.feishu.cn/docx/…" :disabled="busy || !includeMinutes" /></div>
          <div class="sharing-source"><span class="sharing-check"><FileText :size="16" />资料文档 / 知识库链接</span>
            <div class="sharing-inline"><input v-model="materialUrl" type="url" aria-label="资料链接" maxlength="2048" placeholder="https://vnet.feishu.cn/wiki/…" :disabled="busy" /><button type="button" :disabled="busy || !materialUrl.trim()" @click="addMaterialUrl"><Plus :size="15" />添加链接</button></div>
            <ul v-if="materialUrls.length" class="sharing-chips sharing-source-links" aria-label="已添加的资料链接"><li v-for="item in materialUrls" :key="item"><span :title="item">{{ item }}</span><button type="button" class="sharing-chip-x" :aria-label="`移除资料链接 ${item}`" :disabled="busy" @click="toggleMaterialUrl(item, false)"><X :size="14" /></button></li></ul>
          </div>
          <div class="sharing-source"><span class="sharing-check"><Upload :size="16" />上传资料文件</span>
            <div class="sharing-inline"><input type="file" multiple accept=".pdf,.docx,.xlsx,.xlsm,.pptx" aria-label="选择资料文件" :disabled="busy || uploading" @change="pickFiles" /><button type="button" :disabled="busy || uploading || !materialFiles.length" @click="uploadMaterials"><Loader2 v-if="uploading" :size="15" class="spin" /><Upload v-else :size="15" />上传 {{ materialFiles.length || '' }} 份</button></div>
            <ul v-if="materialFiles.length" class="sharing-chips" aria-label="待上传资料"><li v-for="file in materialFiles" :key="file.name + file.size"><span>{{ file.name }}</span><button type="button" class="sharing-chip-x" :aria-label="`取消上传 ${file.name}`" :disabled="busy || uploading" @click="removeFile(file)"><X :size="14" /></button></li></ul>
            <ul v-if="materialIds.length" class="sharing-chips" aria-label="已上传资料"><li v-for="id in materialIds" :key="id"><span>{{ uploadedNames[id] || '已上传材料' }}</span><button type="button" class="sharing-chip-x" :aria-label="`移除已上传资料 ${uploadedNames[id] || id}`" :disabled="busy" @click="materialIds = materialIds.filter((item: string) => item !== id)"><X :size="14" /></button></li></ul>
          </div>
        </fieldset>
        <div v-if="!manualOnly && !importing" class="sharing-generation-options">
          <label class="sharing-check sharing-generate"><input v-model="autoGenerate" type="checkbox" :disabled="busy" />AI生成题目（最多40题）<small>关闭后可直接手工填写题目，资料可选。</small></label>
          <fieldset class="sharing-types" :disabled="busy || !autoGenerate">
            <legend>AI生成题型（可多选，手动录入不受限制）</legend>
            <label v-for="(label, kind) in typeLabels" :key="kind"><input type="checkbox" :checked="questionTypes.includes(kind)" :disabled="busy || !autoGenerate" @change="toggleType(kind, ($event.target as HTMLInputElement).checked)" />{{ label }}</label>
          </fieldset>
        </div>
        <fieldset class="sharing-roster" :disabled="busy || uploading">
          <legend>答题名单{{ !manualOnly && includeMinutes && url.trim() ? '（未选则沿用纪要参会人）' : '（必选）' }} · {{ participants.length }}人</legend>
          <div class="sharing-people-tools"><input v-model="peopleSearch" type="search" aria-label="搜索可添加人员" placeholder="搜索姓名 / 楼栋" @focus="loadPeople()" /><button type="button" :disabled="peopleLoading" @click="loadPeople(true)"><RefreshCw :size="15" />读取人员目录</button></div>
          <div class="sharing-people-list"><button v-for="p in visiblePeople" :key="p.open_id" type="button" :disabled="participantOpenIds.has(p.open_id)" :aria-label="`添加参会人：${p.name}`" @click="addParticipant(p)"><Plus :size="14" /><span>{{ p.name }}<small>{{ p.scope || '未归楼' }}</small></span></button></div>
        </fieldset>
        <div class="sharing-create-actions">
          <ul class="sharing-chips" aria-label="已选答题名单"><li v-for="p in participants" :key="p.open_id"><span>{{ p.name }}</span><button type="button" :disabled="busy || uploading" :aria-label="`移除参会人：${p.name}`" @click="removeParticipant(p)"><X :size="14" /></button></li></ul>
          <button type="submit" class="primary" :disabled="!canCreate"><Loader2 v-if="busy" :size="16" class="spin" /><FileText v-else :size="16" />{{ importing ? '导入并提取题目' : manualEditing ? '保存人工题草稿' : '读取资料并生成题目' }}</button>
        </div>
      </form>
      <template v-if="!creating">
      <div v-if="loading && !rows.length" class="sharing-empty" role="status"><Loader2 :size="24" class="spin" />正在读取批次…</div>
      <div v-else-if="!rows.length" class="sharing-empty"><FileText :size="30" /><span>{{ isAdmin ? '暂无分享会练习批次' : '暂无与你相关的分享会练习' }}</span></div>
      <div v-else class="sharing-batches">
        <button v-for="row in rows" :key="row.id" type="button" class="sharing-batch" @click="openBatch(row)"><span class="sharing-batch-title"><strong>{{ row.title }}</strong><small>{{ row.created_at?.slice(0, 16).replace('T', ' ') }}</small></span><span class="sharing-batch-meta"><span class="sharing-status" :class="row.status">{{ labels[row.status] }}</span><span>{{ row.question_count }}题</span><span>{{ row.completed }}/{{ row.total }}人完成</span><ChevronRight :size="18" /></span></button>
      </div>
      <footer class="sharing-pagination"><span>共 {{ total }} 批</span><div><button type="button" aria-label="上一页" :disabled="busy || loading || page <= 1" @click="page--; load()"><ChevronLeft :size="16" /></button><span>{{ page }} / {{ Math.max(1, Math.ceil(total / 15)) }}</span><button type="button" aria-label="下一页" :disabled="busy || loading || page * 15 >= total" @click="page++; load()"><ChevronRight :size="16" /></button></div></footer>
      </template>
    </template>
    <template v-else>
      <div class="sharing-meta"><span class="sharing-status" :class="detail.status">{{ labels[detail.status] }}</span><a v-if="detail.url" :href="detail.url" target="_blank" rel="noopener noreferrer">查看原纪要</a><span v-if="detail.published_at">发布于 {{ detail.published_at.slice(0, 16).replace('T', ' ') }}</span><span class="sharing-counts" role="status">手动 {{ counts.manual }} 题 · AI {{ counts.ai }} 题 · 合计 {{ counts.total }} 题（AI上限 {{ AI_MAX }}，合计上限 {{ TOTAL_MAX }}）</span></div>
      <button v-if="dirty || editingAnswers" type="button" :disabled="busy" @click="ask('discard')"><RefreshCw :size="16" />放弃修改，读取最新</button>
      <div v-if="['queued', 'generating'].includes(detail.status)" class="sharing-empty" role="status"><Loader2 :size="28" class="spin" /><span>{{ detail.progress || '等待后台生成，不影响其它功能使用' }}</span></div>
      <div v-if="detail.error" class="sharing-alert error">{{ detail.error }}</div>
      <ul v-if="isAdmin && detail.material_status?.length" class="sharing-material-status" aria-label="材料读取进度"><li v-for="item in detail.material_status" :key="item.id"><strong>{{ item.title }}</strong><span>{{ ({ ready: '已读取', reading: '读取中', failed: '读取失败' } as Record<string,string>)[item.status] }}</span><small v-if="item.error">{{ item.error }}</small></li></ul>
      <p v-if="isAdmin && detail.generation_note" class="sharing-alert sharing-generation-note" role="status">{{ detail.generation_note }}</p>
      <ul v-if="isAdmin && detail.sources?.length" class="sharing-chips" aria-label="出题资料快照">
        <li v-for="(source, i) in detail.sources" :key="source.id || source.url || i"><a v-if="source.url" :href="source.url" target="_blank" rel="noopener noreferrer">{{ source.title }}</a><span v-else>{{ source.title }}</span><small v-if="source.low_confidence_count">OCR低置信文字 {{ source.low_confidence_count }} 处</small></li>
      </ul>
      <div class="sharing-toolbar" v-if="isAdmin">
        <div class="sharing-buttons"><button v-if="isDraft" type="button" :disabled="!canEditQuestions" @click="sectionTab = 'questions'; addQuestion('single')"><Plus :size="16" />人工出题</button><button v-if="detail.status === 'failed'" type="button" :disabled="busy || dirty" @click="mutate('retry')"><RefreshCw :size="16" />重新生成</button><button v-if="isDraft" type="button" :disabled="busy || (!dirty && detail.status !== 'failed')" @click="mutate('save')"><Save :size="16" />保存题目与名单</button><button v-if="isDraft" type="button" class="primary" :disabled="busy || dirty || detail.status === 'failed' || !!publishIssue" :title="detail.status === 'failed' ? '请先保存题目与名单为草稿' : publishIssue" @click="ask('publish')"><Send :size="16" />核对后发布给参会人</button><button v-if="['published', 'closed'].includes(detail.status)" type="button" :disabled="busy" @click="ask('retry-delivery')"><RefreshCw :size="16" />重试未完成任务</button><button v-if="detail.status === 'published'" type="button" :disabled="busy" @click="ask('close')"><Square :size="15" />关闭本批</button><span v-if="dirty">题目或答题名单有未保存修改</span><span v-if="busy" role="status"><Loader2 :size="16" class="spin" />正在保存…</span></div>
      </div>
      <p v-if="isAdmin && isDraft && publishIssue" class="sharing-hint" role="status">{{ publishIssue }}</p>
      <div v-if="detail.participants.length" class="sharing-metrics"><div><Users :size="18" /><strong>{{ assignedCount }}</strong><span>需答题人数</span></div><div><CheckCircle2 :size="18" /><strong>{{ detail.completed }}</strong><span>已完成</span></div><div><strong>{{ assignedCount - detail.completed }}</strong><span>未完成</span></div><div><strong>{{ percent }}%</strong><span>完成率</span><progress :value="percent" max="100" aria-label="分享会练习完成率" /></div></div>

      <NTabs v-model:value="sectionTab" type="line" class="v2-sharing-tabs" aria-label="分享会内容">
        <NTab v-if="paper" name="mine">我的答题</NTab><NTab v-if="isAdmin || (!paper && detail.questions?.length)" name="questions">{{ isAdmin ? '题目核对' : '我的题目' }}</NTab><NTab name="people">参会人 · {{ detail.participants.length }}</NTab><NTab v-if="isAdmin" name="delivery">通知与同步</NTab>
      </NTabs>
      <div v-if="isAdmin && sectionTab === 'delivery' && detail.bank_sync?.total" class="sharing-bank" role="status"><strong>题库同步 {{ detail.bank_sync.synced }} / {{ detail.bank_sync.total }} 题</strong><span v-if="detail.bank_sync.total > detail.bank_sync.synced + detail.bank_sync.failed"><Loader2 :size="15" class="spin" />后台同步中</span><span v-if="detail.bank_sync.failed" class="error-text">{{ detail.bank_sync.failed }} 题待重试</span><a href="https://vnet.feishu.cn/wiki/P8R2w5FXRiNpBjkJ3WVcegJ0nzh?table=tblQevBJ9knFaBNh&view=vewhuXAYog" target="_blank" rel="noopener noreferrer">查看纪要题库</a><p v-if="detail.bank_sync.error" class="error-text">{{ detail.bank_sync.error }}</p></div>
      <section v-if="paper && sectionTab === 'mine'" class="sharing-answer-pane" aria-label="我的答题">
        <header><h3>{{ submitted ? '我的答卷' : canAnswer ? `我的答题 · 第${answerPage}页 / 共${answerPages}页` : '我的答题记录' }}</h3><span v-if="submitted">已于 {{ paper!.submitted_at.slice(0, 16).replace('T', ' ') }} 提交</span><span v-else>已答 {{ answeredCount }} / {{ myQuestions.length }} 题 · 每页5题</span></header>
        <div v-if="answerError" role="alert" class="sharing-alert error">{{ answerError }}</div>
        <section v-if="answerConflicts.length" class="sharing-conflicts" aria-label="答题冲突核对"><article v-for="item in answerConflicts" :key="item.id"><strong>{{ myQuestions.find((q: Dict) => q.id === item.id)?.stem }}</strong><p>本页：{{ showAnswer(item.mine, myQuestions.find((q: Dict) => q.id === item.id)) || '未答' }}</p><p>其他页面：{{ showAnswer(item.latest, myQuestions.find((q: Dict) => q.id === item.id)) || '未答' }}</p><button type="button" @click="resolveAnswer(item.id, true)">保留本页答案</button><button type="button" @click="resolveAnswer(item.id, false)">使用其他页面答案</button></article></section>
        <article v-for="(q, i) in answerPageQuestions" :key="q.id" class="sharing-question" :data-type="q.type">
          <header><strong>第 {{ (answerPage - 1) * PAGE_SIZE + i + 1 }} 题</strong><span>{{ typeLabels[q.type] }}</span></header>
          <p class="sharing-stem">{{ q.stem }}</p>
          <LearningQuestionImages :batch-id="detail.id" :images="q.images || []" />
          <template v-if="q.type === 'interview'">
            <label :for="`sharing-answer-${q.id}`">我的作答<textarea :id="`sharing-answer-${q.id}`" :value="answers[q.id] || ''" rows="4" maxlength="1000" :disabled="busy || !canAnswer" @input="writeAnswer(q, ($event.target as HTMLTextAreaElement).value)" /></label>
            <div v-if="submitted" class="sharing-answer"><strong>我的作答：{{ showAnswer(q.selected ?? answers[q.id], q) || '未答' }}</strong><p v-if="q.reference_answer">参考答案（不自动判分）：{{ q.reference_answer }}</p><p>{{ q.explanation }}</p><blockquote v-if="q.evidence && (isAdmin || !q.images?.length)">{{ q.evidence }}</blockquote></div>
          </template>
          <template v-else>
            <fieldset class="sharing-choices" :disabled="busy || !canAnswer"><legend class="sharing-visually-hidden">第{{ i + 1 }}题选项</legend>
              <label v-for="(option, n) in q.options" :key="n" :class="{ checked: (answers[q.id] || []).includes(n) }"><input :type="q.type === 'single' ? 'radio' : 'checkbox'" :name="`sharing-answer-${q.id}`" :checked="(answers[q.id] || []).includes(n)" :aria-label="`第${i + 1}题选项${letters(n)}`" @change="pick(q, n, ($event.target as HTMLInputElement).checked)" /><span>{{ letters(n) }}. {{ option }}</span></label>
            </fieldset>
            <div v-if="submitted" class="sharing-answer"><strong>我的作答：{{ showAnswer(q.selected ?? answers[q.id]) || '未答' }}</strong><p v-if="q.correct">正确答案：{{ correctAnswer(q) }}</p><p v-if="q.explanation">{{ q.explanation }}</p><blockquote v-if="q.evidence && (isAdmin || !q.images?.length)">{{ q.evidence }}</blockquote></div>
          </template>
        </article>
        <fieldset class="sharing-answer-controls" :disabled="busy || !!answerConflicts.length"><footer class="sharing-pagination v2-sharing-answerbar">
          <button type="button" aria-label="答题上一页" :disabled="busy || answerPage <= 1" @click="canAnswer ? sendAnswers(false, answerPage - 1) : answerPage--"><ChevronLeft :size="16" />上一页</button>
          <span>第 {{ answerPage }} / {{ answerPages }} 页</span>
          <button v-if="canAnswer" type="button" :disabled="busy" @click="sendAnswers(false)"><Save :size="16" />保存本页进度</button>
          <button v-if="answerPage < answerPages" type="button" aria-label="答题下一页" :disabled="busy" @click="canAnswer ? sendAnswers(false, answerPage + 1) : answerPage++"><ChevronRight :size="16" />{{ canAnswer ? '保存并下一页' : '下一页' }}</button>
          <button v-if="canAnswer && answerPage >= answerPages" type="button" class="primary" aria-label="提交全部答题" :disabled="busy || !lastPageComplete || answeredCount < myQuestions.length" @click="sendAnswers(true)"><Loader2 v-if="busy" :size="16" class="spin" /><Send v-else :size="16" />提交全部答题</button>
        </footer></fieldset>
      </section>

      <section v-if="sectionTab === 'people'" class="sharing-section">
        <header><h3>参会人答题情况</h3><input v-model="personSearch" type="search" aria-label="搜索参会人" placeholder="搜索姓名 / 楼栋" /></header>
        <p class="sharing-selection-summary" role="status">参会 {{ detail.participants.length }} 人 · 参加答题 {{ assignedCount }} 人 · 本次不做 {{ exemptedCount }} 人</p>
        <p v-if="isAdmin && isDraft" class="sharing-selection-hint">取消勾选无需答题的参会人，保存后再发布。免做人员不发送题目或催答提醒。</p>
        <details v-if="isAdmin && isDraft" class="sharing-add-people"><summary @click="loadPeople()">补充参会人</summary><input v-model="peopleSearch" type="search" aria-label="搜索补充参会人" placeholder="姓名或楼栋" /><div class="sharing-people-list"><button v-for="person in visiblePeople" :key="person.open_id" type="button" :disabled="busy || detail.participants.some((p: Dict) => p.open_id === person.open_id)" @click="addDraftParticipant(person)"><Plus :size="15" />{{ person.name }} {{ person.scope }}</button></div></details>
        <div class="sharing-table"><table><thead><tr><th v-if="isAdmin && isDraft">参加答题</th><th>姓名</th><th>楼栋</th><th>提交状态</th><th>选择题正确数</th><th>选择题正确率</th><th>通知</th><th v-if="detail.results?.length">答卷</th></tr></thead><tbody>
          <tr v-for="p in pagedParticipants" :key="p.open_id" :class="{ 'sharing-exempted': p.exempted }">
            <td v-if="isAdmin && isDraft"><label class="sharing-participation"><input type="checkbox" :checked="!p.exempted" :disabled="busy" :aria-label="'参加答题：' + p.name" @change="setParticipation(p, ($event.target as HTMLInputElement).checked)" /><span>{{ p.exempted ? '本次不做' : '参加' }}</span></label></td>
            <td>{{ p.name }}</td><td>{{ p.scope || '未归楼' }}</td><td>{{ p.exempted ? '无需作答' : p.submitted_at ? '已提交' : '未提交' }}</td><td>{{ !p.submitted_at || !p.choice_count ? '—' : p.correct + '/' + p.choice_count }}</td><td>{{ !p.submitted_at || !p.choice_count ? '—' : Math.round(p.correct * 100 / p.choice_count) + '%' }}</td><td><span v-if="p.exempted">免做，不推送</span><span v-else-if="p.delivery_error" class="error-text">{{ p.delivery_error }}</span><span v-else>{{ p.delivery_status === 'sent' ? '已发送' : isDraft ? '待发布' : '待发送' }}</span></td><td v-if="detail.results?.length"><button v-if="!p.exempted" type="button" @click="selectResult(p.open_id)"><FileText :size="15" />{{ selectedPerson === p.open_id ? '收起答卷' : '查看答卷' }}</button><span v-else>—</span></td>
          </tr>
        </tbody></table></div>
        <NPagination v-model:page="personPage" :item-count="participantsView.length" :page-size="12" :page-slot="5" aria-label="参会人分页" />
      </section>
      <NModal :show="!!resultPaper" preset="card" class="v2-sharing-paper" :title="resultPaper?.name + ' · 本次答卷'" :style="{width:'min(760px,calc(100vw - 24px))'}" @update:show="(value: boolean) => { if (!value) selectedPerson = ''; }"><p v-if="resultLoading" role="status">正在读取答卷…</p><p v-if="resultError" role="alert">{{ resultError }}<button type="button" @click="selectedPerson && selectResult(selectedPerson)">关闭</button></p><section v-if="resultPaper && !resultLoading && !resultError" class="sharing-section"><header><span>共 {{ (resultPaper.question_ids || []).length }} 题 · 每页5题 · {{ resultPaper.submitted_at?.slice(0, 16).replace('T', ' ') || '尚未提交' }}</span></header><article v-for="(q, i) in resultQuestions" :key="q.id" class="sharing-question"><strong>{{ (resultPage - 1) * PAGE_SIZE + i + 1 }}. {{ q.stem }}</strong><LearningQuestionImages :batch-id="detail.id" :images="q.images || []" /><p v-for="(option, n) in q.options" :key="n">{{ letters(n) }}. {{ option }}</p><p>作答：{{ showAnswer(resultPaper.answers?.[q.id], q) || '未答' }}</p><p>{{ q.type === 'interview' ? '参考答案（不自动判分）' : '正确答案' }}：{{ q.type === 'interview' ? (q.reference_answer || '—') : (correctAnswer(q) || '—') }}</p><p v-if="q.explanation">解析：{{ q.explanation }}</p></article><footer class="sharing-pagination"><span>共 {{ resultPages }} 页</span><div><button type="button" aria-label="答卷上一页" :disabled="resultPage <= 1" @click="resultPage--"><ChevronLeft :size="16" /></button><span>{{ resultPage }} / {{ resultPages }}</span><button type="button" aria-label="答卷下一页" :disabled="resultPage >= resultPages" @click="resultPage++"><ChevronRight :size="16" /></button></div></footer></section></NModal>
      <section v-if="isAdmin && sectionTab === 'delivery'" class="sharing-section"><header><h3>通知与同步</h3><input v-model="personSearch" aria-label="搜索通知人员" placeholder="姓名或楼栋" /></header><div class="sharing-table"><table><thead><tr><th>人员</th><th>题单发送</th><th>最近催答</th><th>成绩通知</th></tr></thead><tbody><tr v-for="p in pagedParticipants" :key="p.open_id"><td>{{ p.name }} {{ p.scope }}</td><td v-for="prefix in ['delivery', 'reminder_delivery', 'result_delivery']" :key="prefix">{{ p.exempted ? '无需发送' : deliveryLabel(p[prefix + '_status']) }}<small v-if="p[prefix + '_error']" class="error-text">{{ p[prefix + '_error'] }}</small><small v-else>{{ p[prefix + '_at']?.slice(0, 16).replace('T', ' ') }}</small></td></tr></tbody></table></div><NPagination v-model:page="personPage" :item-count="participantsView.length" :page-size="12" :page-slot="5" aria-label="通知人员分页" /></section>
      <section v-if="isAdmin && sectionTab === 'delivery' && detail.weekly_reports?.length" class="sharing-section"><header><h3>最近群汇总</h3></header><div class="sharing-table"><table><thead><tr><th>统计截止</th><th>分组</th><th>发送状态</th><th>说明</th></tr></thead><tbody><tr v-for="r in detail.weekly_reports" :key="`${r.date}-${r.part}`"><td>{{ r.end }}</td><td>{{ r.part }} / {{ r.parts }}</td><td>{{ deliveryLabel(r.status) }}</td><td>{{ r.error || r.sent_at?.slice(0, 16).replace('T', ' ') || '等待后台发送' }}</td></tr></tbody></table></div></section>
      <section v-if="sectionTab === 'questions' && (isAdmin || (!paper && detail.questions?.length))" class="sharing-section v2-sharing-questions">
        <header><h3>{{ isAdmin ? '题目与答案' : `我的${detail.questions.length}道题` }}</h3><span>{{ detail.questions.length }}题 · 手动{{ counts.manual }} / AI{{ counts.ai }}</span></header>
        <div v-if="isAdmin && isDraft" class="sharing-editor-bar">
          <button type="button" aria-label="新增单选题" title="新增单选题" :disabled="!canEditQuestions" @click="addQuestion('single')"><Plus :size="15" />单选题</button>
          <button type="button" aria-label="新增多选题" title="新增多选题" :disabled="!canEditQuestions" @click="addQuestion('multiple')"><Plus :size="15" />多选题</button>
          <button type="button" aria-label="新增问答题" title="新增问答题" :disabled="!canEditQuestions" @click="addQuestion('interview')"><Plus :size="15" />问答题</button>
          <span>手动 {{ counts.manual }} · AI {{ counts.ai }} · 合计 {{ counts.total }} / {{ TOTAL_MAX }} · AI剩余 {{ counts.aiLeft }}</span>
          <span class="sharing-paper-size">{{ paperSizeHint }}</span>
        </div>
        <div class="sharing-question-grid" :class="{'is-editing': isAdmin && isDraft}">
        <article v-for="(q, i) in questions" :key="q.id" class="sharing-question" :data-origin="q.origin || 'ai'" :data-kind="q.type">
          <header><strong>第 {{ (questionPage - 1) * PAGE_SIZE + i + 1 }} 题</strong><span>{{ typeLabels[q.type] }}</span><span class="sharing-origin" :class="q.origin || 'ai'">{{ originLabels[q.origin || 'ai'] }}</span></header>
          <template v-if="isAdmin && isDraft">
            <div class="sharing-question-tools">
              <label :for="`sharing-type-${q.id}`">题型<select :id="`sharing-type-${q.id}`" :value="q.type" :disabled="!canEditQuestions" @change="switchType(q, ($event.target as HTMLSelectElement).value)"><option v-for="(label, kind) in typeLabels" :key="kind" :value="kind">{{ label }}</option></select></label>
              <button type="button" :disabled="!canEditQuestions" :aria-label="`删除第${(questionPage - 1) * PAGE_SIZE + i + 1}题`" @click="removeQuestion(q)"><Trash2 :size="15" />删除此题</button>
            </div>
            <label :for="`sharing-${q.id}`">题干<textarea :id="`sharing-${q.id}`" v-model="q.stem" rows="2" maxlength="500" :disabled="busy" @input="questionChanged(q)" /></label>
            <template v-if="q.type === 'interview'">
              <label>参考答案（问答题不自动判分）<textarea v-model="q.reference_answer" rows="3" maxlength="1000" :disabled="busy" @input="questionChanged(q)" /></label>
            </template>
            <template v-else>
              <p class="sharing-option-hint">正确选项（{{ q.type === 'single' ? '单选' : '多选' }}），选项需 {{ CHOICE_MIN }}-{{ CHOICE_MAX }} 个。</p>
              <div class="sharing-option-grid">
              <div v-for="(option, n) in q.options" :key="n" class="sharing-option"><input :type="q.type === 'single' ? 'radio' : 'checkbox'" :name="`sharing-answer-${q.id}`" :checked="q.correct.includes(n)" :aria-label="`第${(questionPage - 1) * PAGE_SIZE + i + 1}题选项${letters(n)}为正确答案`" :disabled="busy" @change="choose(q, Number(n), ($event.target as HTMLInputElement).checked)" /><label :for="`sharing-option-${q.id}-${n}`">{{ letters(Number(n)) }}</label><textarea :id="`sharing-option-${q.id}-${n}`" v-model="q.options[n]" rows="1" maxlength="300" :disabled="busy" @input="questionChanged(q)" /><button type="button" class="sharing-chip-x" :aria-label="`删除第${(questionPage - 1) * PAGE_SIZE + i + 1}题选项${letters(Number(n))}`" :disabled="busy || !canEditQuestions || q.options.length <= CHOICE_MIN" @click="removeOption(q, Number(n))"><X :size="14" /></button></div>
              </div>
              <button type="button" :disabled="!canEditQuestions || q.options.length >= CHOICE_MAX" @click="addOption(q)"><Plus :size="15" />增加选项</button>
            </template>
            <label>答案解析<textarea v-model="q.explanation" rows="2" maxlength="600" :disabled="busy" @input="questionChanged(q)" /></label>
            <label>纪要/图片依据{{ (q.origin || 'ai') === 'manual' ? '（手动题可选）' : '（AI题必填，需能在纪要/资料或所选图片中核对）' }}<textarea v-model="q.evidence" rows="2" maxlength="800" :disabled="busy" @input="questionChanged(q)" /></label>
            <LearningQuestionImages :batch-id="detail.id" :images="questionImages(q)" :source-images="detail.source_images || []" :image-ids="imageIds(q)" editable :disabled="!canEditQuestions" @update:image-ids="selectQuestionImages(q, $event)" />
            <label v-if="needsImageConfirmation(q)" class="sharing-image-confirm"><input v-model="q.image_confirmed" type="checkbox" :disabled="!canEditQuestions" @change="changed" /><span>已对照原图核对题目与答案</span></label>
            <p v-if="questionIssues[i]" role="alert" class="sharing-question-error">{{ questionIssues[i] }}</p>
            <button v-if="q.evidence" type="button" :disabled="dirty" @click="showEvidence(q)"><FileText :size="15" />定位题目依据</button>
          </template>
          <template v-else><p>{{ q.stem }}</p><LearningQuestionImages :batch-id="detail.id" :images="q.images || []" /><p v-for="(option, n) in q.options" :key="n">{{ letters(Number(n)) }}. {{ option }}</p><div v-if="(isAdmin || submitted || !q.images?.length) && ((q.correct && q.correct.length) || q.reference_answer || q.explanation || q.evidence)" class="sharing-answer"><strong>{{ q.type === 'interview' ? '参考答案（不自动判分）：' + (q.reference_answer || '—') : '正确答案：' + (correctAnswer(q) || '—') }}</strong><p v-if="q.explanation">{{ q.explanation }}</p><blockquote v-if="q.evidence && (isAdmin || !q.images?.length)">{{ q.evidence }}</blockquote></div></template>
        </article>
        </div>
        <footer class="sharing-pagination"><span>每页5题<span v-if="isAdmin && isDraft"> · 第{{ questionPage }}页手动{{ (detail.questions || []).slice((questionPage - 1) * PAGE_SIZE, questionPage * PAGE_SIZE).filter((q: Dict) => (q.origin || 'ai') === 'manual').length }}题</span></span><div><button type="button" aria-label="题目上一页" :disabled="questionPage <= 1" @click="questionPage--"><ChevronLeft :size="16" /></button><span>{{ questionPage }} / {{ Math.max(1, Math.ceil(detail.questions.length / PAGE_SIZE)) }}</span><button type="button" aria-label="题目下一页" :disabled="questionPage * PAGE_SIZE >= detail.questions.length" @click="questionPage++"><ChevronRight :size="16" /></button></div></footer>
      </section>
    </template>
    <ConfirmDialog :open="confirm.open" :title="confirm.title" :message="confirm.message" @resolve="resolved" />
    <NModal :show="!!evidenceQuestion" preset="card" title="题目依据" class="sharing-evidence" :style="{width:'min(760px,calc(100vw - 24px))'}" @update:show="(value: boolean) => { if (!value) evidenceQuestion = null; }"><p>{{ evidenceQuestion?.stem }}</p><blockquote>{{ evidenceQuestion?.evidence }}</blockquote><article v-for="(location, index) in evidenceLocations" :key="index"><h3>{{ location.title }}</h3><p v-for="(ref, number) in location.references" :key="number">{{ ref.kind?.includes('slide') ? '幻灯片' : ref.page ? '页码' : '位置' }} {{ ref.page || ref.slide || ref.sheet || ref.paragraph || '' }}</p><pre>{{ location.excerpt }}</pre><a v-if="/^https:\/\/vnet\.feishu\.cn\//.test(location.url || '')" :href="location.url" target="_blank" rel="noopener noreferrer">打开原材料</a><LearningQuestionImages v-if="location.image_id && detail" :batch-id="detail.id" :images="(detail.source_images || []).filter((image: Dict) => image.id === location.image_id)" /></article><p v-if="!evidenceLocations.length">旧题未保存分材料位置，可依据以上引文核对原纪要。</p></NModal>
  </section>
</template>

<style scoped>
.sharing-exercises { color: #23354b; min-width: 0; }
.sharing-toolbar,.sharing-title,.sharing-buttons,.sharing-meta,.sharing-pagination,.sharing-pagination>div,.sharing-section>header,.sharing-question>header,.sharing-editor-bar,.sharing-question-tools,.sharing-inline,.sharing-people-tools,.sharing-bulk { display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
.sharing-toolbar,.sharing-section>header,.sharing-pagination { justify-content:space-between; margin:16px 0; }
.sharing-title { flex:1; min-width:0; }.sharing-title h2 { font-size:18px; line-height:1.5; overflow-wrap:anywhere; margin:0; }
h3 { font-size:15px; margin:0; }button { display:inline-flex; gap:6px; align-items:center; justify-content:center; border:1px solid #cddaea; border-radius:6px; background:white; color:#28559a; padding:8px 12px; min-height:38px; font:inherit; cursor:pointer; transition:background .18s; }button:hover:not(:disabled){background:#edf4ff;}button:disabled{opacity:.5;cursor:not-allowed;}button.primary{background:#2463df;color:white;border-color:#2463df;}button.primary:hover:not(:disabled){background:#174caf;}
button:focus-visible,a:focus-visible,input:focus-visible,textarea:focus-visible,select:focus-visible { outline:2px solid #2766df;outline-offset:3px; }input,textarea,select{font:inherit;border:1px solid #cad7e5;border-radius:6px;padding:9px 11px;color:inherit;background:#fff;min-width:0;line-height:1.6;}textarea{resize:vertical;width:100%;box-sizing:border-box;}select{cursor:pointer;}a{color:#235abc;text-decoration:underline;}
small,.sharing-meta,.sharing-pagination,.sharing-hint,.sharing-counts {color:#52667d;font-size:13px;}
.sharing-create{border-block:1px solid #dce5ee;padding:12px 0;margin-bottom:12px;min-width:0;}.sharing-create>label{display:flex;gap:7px;align-items:center;margin-bottom:9px;font-weight:600;}.sharing-create>div{display:flex;gap:10px;min-width:0;}.sharing-create input{flex:1;min-width:0;}
.sharing-alert{padding:12px 15px;margin:12px 0;border-radius:6px;line-height:1.6;overflow-wrap:anywhere;}.error{background:#fff1f2;color:#a82342;}.success{background:#ecf9f2;color:#146844;}.error-text{color:#a82342;}
.sharing-generation-note{background:color-mix(in srgb,var(--cf-warning,#735515) 8%,var(--cf-surface,#fff8e7));color:var(--cf-warning,#735515);white-space:pre-wrap;}
.sharing-question .sharing-image-confirm{display:flex;align-items:flex-start;gap:8px;min-height:44px;padding:10px 0;line-height:1.6;overflow-wrap:anywhere;}.sharing-image-confirm input{flex:none;width:18px;height:18px;margin:3px 0 0;accent-color:#2463df;}
.sharing-sources{border:0;margin:0 0 14px;padding:0;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,270px),1fr));gap:10px;min-width:0;align-items:start;}.sharing-sources legend,.sharing-types legend{font-size:14px;font-weight:600;margin-bottom:6px;}.sharing-sources>.sharing-hint{grid-column:1/-1;margin:0}.sharing-source{display:grid;gap:8px;min-width:0;}.sharing-source>input[type=url]{width:100%;box-sizing:border-box;}.sharing-source .sharing-inline{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:stretch;}.sharing-source .sharing-inline>input{width:100%;max-width:100%;box-sizing:border-box;}.sharing-source-links li{min-width:0;}.sharing-source-links li>span{white-space:nowrap;text-overflow:ellipsis;overflow:hidden;}
.sharing-check{display:inline-flex;gap:8px;align-items:center;font-weight:600;min-height:32px;min-width:0;overflow-wrap:anywhere;}.sharing-check input{flex:none;width:18px;height:18px;margin:0;accent-color:#2463df;}.sharing-check small{font-weight:400;flex-basis:100%;}
.sharing-generate{display:grid;grid-template-columns:20px minmax(0,1fr);gap:4px 8px;align-items:center;margin:0 0 14px;font-weight:600;}.sharing-generate input{width:18px;height:18px;margin:0;accent-color:#2463df;}.sharing-generate small{font-weight:400;grid-column:2;}
.sharing-inline{min-width:0;}.sharing-inline>input{flex:1 1 220px;min-width:0;}.sharing-inline button{flex:none;}
.sharing-chips{list-style:none;display:flex;flex-wrap:wrap;gap:6px;margin:0;padding:0;min-width:0;}.sharing-chips li{display:inline-flex;align-items:center;gap:4px;max-width:100%;background:#f2f6fb;border:1px solid #dbe5f0;border-radius:999px;padding:3px 4px 3px 10px;font-size:12px;color:#405e85;}.sharing-chips li>span{overflow-wrap:anywhere;min-width:0;}.sharing-chip-x{min-height:26px;height:26px;width:26px;padding:0;border-radius:50%;border-color:transparent;color:#58708a;flex:none;}
.sharing-create>.sharing-create-fields{display:grid;grid-template-columns:max-content minmax(0,1fr);align-items:end;gap:10px;margin-bottom:10px;}.sharing-create-title{display:grid;gap:4px;max-width:520px;min-width:0;font-weight:600;}.sharing-create-title input{width:100%;box-sizing:border-box;font-weight:400;}.sharing-mode{border:0;margin:0;padding:0;min-width:0;}.sharing-mode legend{font-size:14px;font-weight:600;margin-bottom:6px;padding:0;}
.sharing-exercises .sharing-creating-toolbar .sharing-title{flex-basis:auto;}.sharing-exercises .sharing-create .sharing-people-tools{display:grid;grid-template-columns:minmax(0,320px) auto;gap:8px;width:fit-content;max-width:100%;}.sharing-create>.sharing-roster{border:0;margin:0 0 12px;padding:0;}
.sharing-create>.sharing-generation-options{align-items:center;flex-wrap:wrap;gap:8px 20px;margin-bottom:14px;}.sharing-generation-options>.sharing-generate,.sharing-generation-options>.sharing-types{margin:0;}
.sharing-create>.sharing-create-actions{align-items:center;justify-content:space-between;flex-wrap:wrap;}.sharing-create-actions>.sharing-chips{flex:1 1 140px;}.sharing-create-actions>button{flex:none;}
@media(max-width:360px){.sharing-create>.sharing-create-fields{grid-template-columns:minmax(0,1fr);}}
.sharing-types{display:flex;align-items:center;flex-wrap:wrap;gap:8px 20px;border:0;margin:0 0 14px;padding:0;}.sharing-types label{display:flex;gap:8px;align-items:center;min-height:40px;cursor:pointer;}.sharing-types input{flex:none;width:18px;height:18px;margin:0;accent-color:#2463df;}.sharing-types small{flex-basis:100%;line-height:1.6;}
.sharing-empty{display:flex;min-height:160px;align-items:center;justify-content:center;flex-direction:column;gap:14px;color:#58708a;text-align:center;}.sharing-batches{border-top:1px solid #dce5ee;display:grid;min-width:0;}.sharing-batch{display:grid;width:100%;box-sizing:border-box;grid-template-columns:minmax(0,1fr) auto;gap:12px;text-align:left;border:0;border-bottom:1px solid #dce5ee;border-radius:0;padding:12px;color:inherit;height:auto;white-space:normal;}.sharing-batch-title{display:block;min-width:0;text-align:left;}.sharing-batch strong{display:block;overflow-wrap:anywhere;line-height:1.6;}.sharing-batch small{display:block;margin-top:4px;}.sharing-batch-meta{display:flex;align-items:center;gap:10px;flex-wrap:wrap;min-width:0;white-space:nowrap;font-size:13px;}
.sharing-status{background:#eef3fa;color:#405e85;border-radius:4px;padding:4px 8px;font-size:12px;white-space:nowrap;}.sharing-status.published{background:#e4f6ef;color:#11704f;}.sharing-status.failed{background:#fff0f0;color:#ab2948;}.sharing-status.generating,.sharing-status.queued{background:#fff4dc;color:#805a17;}
.sharing-counts{overflow-wrap:anywhere;}
.sharing-selection-summary{font-size:14px;font-weight:600;margin:0 0 8px;line-height:1.6}.sharing-selection-hint,.sharing-hint{font-size:13px;color:#52667d;margin:0 0 12px;line-height:1.6}.sharing-participation{display:inline-flex;gap:8px;align-items:center;min-height:44px;cursor:pointer;white-space:nowrap;min-width:0}.sharing-participation input{width:18px;height:18px;margin:0;accent-color:#2463df;flex:none}.sharing-exempted{background:#f7f8fa;color:#627083}
.sharing-bank{display:flex;gap:10px 16px;flex-wrap:wrap;align-items:center;padding:14px 0;border-block:1px solid #dce5ee;font-size:14px}.sharing-bank span{display:inline-flex;gap:6px;align-items:center}.sharing-bank p{flex-basis:100%;margin:0;overflow-wrap:anywhere}.sharing-answer strong{white-space:pre-wrap;overflow-wrap:anywhere}
.sharing-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;padding:12px 0;border-block:1px solid #dce5ee;margin:10px 0;}.sharing-metrics>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}.sharing-metrics strong{font-size:20px;font-weight:650;}.sharing-metrics span{color:#52667d;}.sharing-metrics progress{width:100%;height:6px;accent-color:#198763;}.sharing-table{overflow-x:auto;max-width:100%;}table{border-collapse:collapse;width:100%;min-width:630px;font-size:14px;}th,td{padding:12px 14px;border-bottom:1px solid #e1e8f0;text-align:left;vertical-align:top;}th{background:#f2f6fb;color:#465e7b;font-weight:600;}td:last-child{max-width:350px;overflow-wrap:anywhere;}
.sharing-editor-bar{gap:8px 10px;padding:10px 0 4px;font-size:13px;color:#52667d;}.sharing-editor-bar>span{line-height:1.6;}.sharing-editor-bar .sharing-paper-size{color:#28559a;font-weight:600;}
.sharing-question{padding:20px 0;border-top:1px solid #dce5ee;min-width:0;}.sharing-question>header{margin-bottom:14px;}.sharing-question>header span{font-size:12px;color:#5e738a;}.sharing-question label{display:grid;gap:6px;margin:10px 0;}.sharing-question p{white-space:pre-wrap;line-height:1.8;overflow-wrap:anywhere;margin:8px 0;}.sharing-question-tools{gap:8px 10px;margin-bottom:6px;}.sharing-question-tools label{display:inline-flex;align-items:center;gap:8px;margin:0;}.sharing-option-hint{font-size:13px;color:#52667d;margin:4px 0 0;}.sharing-question-error{color:#a82342;font-size:13px;margin:8px 0 0;}
@media(max-width:600px){.sharing-import-fields>.sharing-mode,.sharing-import-fields>.sharing-create-title{grid-column:1/-1}}.sharing-manual-create{margin:8px 0 18px;min-width:0}.sharing-manual-create .sharing-question{padding:12px 0}.sharing-manual-create h3{margin:8px 0}.sharing-origin{border-radius:4px;padding:3px 8px;font-size:12px;background:#eef3fa;color:#405e85;}.sharing-origin.ai{background:#f1ecff;color:#5b3fb0;}.sharing-origin.manual{background:#e9f7ef;color:#146844;}
.sharing-option{display:grid;grid-template-columns:20px 20px minmax(0,1fr) 28px;gap:10px;align-items:center;margin:10px 0;min-width:0;}.sharing-option input{width:17px;height:17px;accent-color:#2766df;}.sharing-option label{display:block;margin:0;}.sharing-option textarea{min-width:0;}
.sharing-choices{border:0;margin:10px 0;padding:0;display:grid;gap:8px;min-width:0;}.sharing-choices label{display:flex;gap:10px;align-items:flex-start;margin:0;padding:10px 12px;border:1px solid #d5e0ec;border-radius:8px;cursor:pointer;line-height:1.6;min-width:0;}.sharing-choices label.checked{border-color:#5e91d6;background:#edf4ff;}.sharing-choices input{width:18px;height:18px;margin:2px 0 0;accent-color:#2766df;flex:none;}.sharing-choices span{overflow-wrap:anywhere;min-width:0;}
.sharing-answer{background:#f0f8f4;border-left:3px solid #31a67c;padding:12px 16px;margin-top:14px;min-width:0;}.sharing-answer blockquote{margin:10px 0 0;color:#4d665b;font-size:13px;line-height:1.7;overflow-wrap:anywhere;}
.sharing-answer-pane>header,.sharing-roster>header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;justify-content:space-between;margin:16px 0;}
.sharing-roster{display:grid;gap:12px;min-width:0;}
.sharing-roster-list{display:flex;flex-wrap:wrap;gap:8px;min-width:0;}.sharing-roster-item{display:inline-flex;align-items:center;gap:4px;border:1px solid #dbe5f0;border-radius:8px;padding:2px 6px 2px 10px;background:#f8fafc;max-width:100%;}.sharing-roster-item .sharing-participation{min-height:36px;white-space:normal;}.sharing-roster-item small{display:block;color:#52667d;font-weight:400;}
.sharing-people{display:grid;gap:10px;min-width:0;}.sharing-people-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,150px),1fr));gap:8px;min-width:0;max-height:320px;overflow:auto;}.sharing-people-list button{justify-content:flex-start;text-align:left;font-size:13px;min-width:0;}.sharing-people-list button span{display:grid;min-width:0;overflow-wrap:anywhere;}.sharing-people-list button small{color:#52667d;}
.sharing-visually-hidden{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap;}
.spin{animation:sharing-spin 1s linear infinite;}@keyframes sharing-spin{to{transform:rotate(360deg)}}
@media(max-width:700px){.sharing-metrics{grid-template-columns:repeat(2,minmax(0,1fr));}.sharing-batch{grid-template-columns:minmax(0,1fr);gap:6px;}.sharing-batch-meta>svg{margin-left:auto;}.sharing-toolbar{align-items:flex-start;}.sharing-section>header input{width:100%;box-sizing:border-box;}.sharing-title h2{font-size:16px;}.sharing-question{padding:16px 0;}button{min-height:42px;}.sharing-people-list{grid-template-columns:minmax(0,1fr);max-height:none;}.sharing-question-tools button{flex:1 1 auto;}.sharing-pagination>*{justify-content:center;}.sharing-option{grid-template-columns:20px 20px minmax(0,1fr) 28px;}}
@media(prefers-reduced-motion:reduce){.spin{animation-duration:2s;}button{transition:none;}}
.sharing-question-tools>label{display:flex;align-items:center;gap:8px;white-space:nowrap;min-width:0}.sharing-question-tools>label>select{flex:1;min-width:0}.sharing-question>button{justify-self:start;max-width:100%}
.sharing-danger{color:#b42332}.sharing-answer-controls{padding:0;border:0;min-width:0}.sharing-conflicts article{border-bottom:1px solid #dce5ee;padding:12px 0}.sharing-conflicts button{margin:4px 8px 4px 0}.sharing-table small{display:block;max-width:260px;overflow-wrap:anywhere}.sharing-material-status{padding:0;list-style:none;display:grid;gap:6px}.sharing-material-status li{display:flex;gap:8px;flex-wrap:wrap;font-size:13px}.sharing-material-status strong{font-weight:500;overflow-wrap:anywhere}.sharing-material-status small{flex-basis:100%;color:#a82342}.sharing-add-people summary{min-height:44px;align-content:center;cursor:pointer}.sharing-evidence,.v2-sharing-paper{max-height:calc(100dvh - 32px);overflow:auto}.sharing-evidence pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit;line-height:1.7}.sharing-section .n-pagination{margin-top:12px}.sharing-section .sharing-table{max-height:480px;overflow:auto}.sharing-section th{position:sticky;top:0;background:var(--cf-surface,#fff)}
@media(max-width:760px){.sharing-exercises .sharing-people-list{max-height:280px;overflow:auto}.sharing-exercises .v2-sharing-answerbar{display:flex;flex-wrap:wrap;gap:8px}.v2-sharing-answerbar>button{flex:1 1 130px;min-height:44px}.v2-sharing-answerbar>span{flex-basis:100%;text-align:center}.sharing-exercises .sharing-buttons{display:flex;flex-wrap:wrap}.sharing-buttons button{flex:1 1 140px}.sharing-exercises .sharing-people-tools{display:flex;flex-wrap:wrap}.sharing-people-tools input{flex:1 1 150px;min-width:0}.sharing-people-tools button{flex:0 1 auto}}

.sharing-question-grid.is-editing{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:14px;align-items:start;margin-top:12px}
.sharing-question-grid.is-editing>.sharing-question{border:1px solid #dce5ee;border-radius:8px;padding:12px;min-width:0}
.sharing-question-grid.is-editing>.sharing-question>header{gap:8px;margin:0 0 10px;align-items:center}
.sharing-question-grid.is-editing>.sharing-question>header>label{display:flex;align-items:center;gap:6px;margin:0}
.sharing-question-grid.is-editing>.sharing-question>header>button{margin-left:auto}
.sharing-question-grid.is-editing>.sharing-question>header select{padding:5px 8px;min-height:36px}
.sharing-option-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr));gap:8px;min-width:0}
.sharing-option-grid .sharing-option{grid-template-columns:20px 14px minmax(0,1fr) 28px;gap:6px;margin:0;min-height:44px}
.sharing-option-grid .sharing-option textarea{padding:7px 8px;min-height:44px}
</style>
