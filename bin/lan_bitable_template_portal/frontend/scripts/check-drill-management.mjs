import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { computed, ref } from "vue";
import ts from "typescript";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const page = readFileSync(resolve(root, "src/components/DrillManagementPage.vue"), "utf8");
const app = readFileSync(resolve(root, "src/App.vue"), "utf8");
const home = readFileSync(resolve(root, "src/scopeHomeUtils.ts"), "utf8");
const monthSource = page.slice(page.indexOf('function normalizeMonth('), page.indexOf('function deepClone'));
const normalizeMonth = new Function('currentMonth', `${ts.transpile(monthSource)}; return normalizeMonth;`)('2026-10');
for (let month = 1; month <= 12; month++) {
  const expected = `2026-${String(month).padStart(2, '0')}`;
  assert.equal(normalizeMonth(expected), expected);
  assert.equal(normalizeMonth(`2026年${month}月`), expected);
}
for (const invalid of ['', '2026-13', '2026-100', 'bad']) assert.equal(normalizeMonth(invalid), '2026-10');
assert(page.includes("timeZone: 'Asia/Shanghai'"));

for (const marker of [
  "refreshSignaturePeople",
  "force: true, refresh: true",
  "statusPollKey",
  "preserveDraft",
  "requestDiscardChanges",
  "refreshSignatureDirectory",
  "stepSignerOptionLabels",
  "commanderPersonOptionLabels",
  'v-model="uploadForm.assigned_scopes"',
  'form.append("assigned_scopes", JSON.stringify(uploadForm.assigned_scopes))',
  '演练已发布至 ${assignedScopeLabel',
  "printSheetStyle",
  "configBaseline",
  "beforeunload",
  "applyTemplateImages",
  "cell_styles",
  "images_truncated",
  "return /^[ABCDE]$/.test(text) ? text : \"\"",
]) {
  if (!page.includes(marker)) throw new Error(`DrillManagementPage 缺少稳定性保护: ${marker}`);
}
if (page.includes("window.confirm")) throw new Error("演练页面仍使用原生确认框");
if (page.includes("MopSignaturePadModal") || page.includes("sendMissingSignatureLink")) throw new Error("演练页面仍含分散签名采集入口");
if (/signature\.image_data_url|signer\.image_url|signer\.signature_url/.test(page)) throw new Error("演练预览仍尝试渲染签名图片");
if (page.includes('<VnetBackButton :disabled="busy"')) throw new Error("演练后台同步仍会锁住返回按钮");
if (/type="file"[^>]*\brequired\b/.test(page)) throw new Error("演练拖放文件输入仍被 required 拦截");
if (!app.includes('routePath.value === "/drill-management/print"')) throw new Error("App 缺少演练打印路由");
if (!home.includes('primaryAction: { key: "drill"')) throw new Error("首页演练入口未启用");
if (page.includes("personAllowedForScope")) throw new Error("演练人员仍按楼栋过滤");
if (!page.includes('query.set("refresh_people", "1")')) throw new Error("签名刷新未读取完整人员目录");
if (!app.includes(':key="`${routePath}:${drillScope}:${routeParams.get(\'mode\') || \'\'}`"')) throw new Error("演练不同楼栋和管理入口仍共用旧页面状态");

const drills = ref([
  { drill_id: "a-only", status: "published", assigned_scopes: ["A"] },
  { drill_id: "a-c", status: "published", assigned_scopes: ["A", "C"] },
  { drill_id: "legacy", status: "published" },
  { drill_id: "draft", status: "draft", assigned_scopes: ["A"] },
  { drill_id: "cached-execution", status: "published", assigned_scopes: ["A"], execution: {} },
]);
const activeScope = ref("A"), viewMode = ref("admin"), selectedDrillId = ref("a-only");
const listSelectors = page.slice(page.indexOf("const buildingDrills ="), page.indexOf("const sheetNames ="));
const { buildingDrills, selectedDrill } = new Function("computed", "drills", "activeScope", "viewMode", "selectedDrillId",
  `${ts.transpile(listSelectors)}\nreturn { buildingDrills, selectedDrill };`,
)(computed, drills, activeScope, viewMode, selectedDrillId);
assert.equal(selectedDrill.value.drill_id, "a-only", "管理员仍可查看全部演练");
viewMode.value = "building";
assert.deepEqual(buildingDrills.value.map(item => item.drill_id), ["a-only", "a-c", "legacy", "cached-execution"]);
activeScope.value = "B";
assert.deepEqual(buildingDrills.value.map(item => item.drill_id), ["legacy"], "接口返回前也不能闪现未分配的缓存演练");
assert.equal(selectedDrill.value, null, "未分配演练详情也不可显示");
activeScope.value = "C";
assert.deepEqual(buildingDrills.value.map(item => item.drill_id), ["a-c", "legacy"], "多楼栋分配与旧演练保持兼容");
viewMode.value = "admin";
assert.equal(selectedDrill.value.drill_id, "a-only", "返回管理员入口仍可查看原演练");

const directory = Array.from({ length: 602 }, (_, index) => ({
  record_id: `person-${index}`, name: `人员${index}`, building: index < 2 ? "E" : "A", employee_no: `job-${index}`,
}));
const people = ref(directory);
const peopleSearch = ref("");
assert(page.includes('const peopleExpanded = ref(true);'), '默认开放全部签名人员');
const peopleExpanded = ref(true);
const context = {
  computed, people, peopleSearch, peopleExpanded, activeScope: ref("E"), commanderId: ref(""),
  personBelongsToScope: (person, scope) => person.building === scope,
  personName: (person) => person.name,
  personMeta: (person) => `${person.building} ${person.employee_no}`,
  personId: (person) => person.record_id,
  personById: (id) => people.value.find((person) => person.record_id === id),
  personOptionLabelFromPerson: (person) => person.record_id,
  uniquePeople: (items) => [...new Map(items.map((person) => [person.record_id, person])).values()],
};
const selectors = page.slice(page.indexOf("const currentBuildingPeople ="), page.indexOf("const missingSignaturePeople ="));
const evaluate = new Function(...Object.keys(context), `${ts.transpile(selectors)}\nreturn { filteredPeople, commanderPeople };`);
const { filteredPeople, commanderPeople } = evaluate(...Object.values(context));
assert.equal(filteredPeople.value.length, 602, "默认展示全部人员，本楼人员优先");
assert.equal(filteredPeople.value[0].building, 'E');
assert.equal(commanderPeople.value.length, 602, "指挥人下拉搜索必须覆盖全部人员");
peopleSearch.value = "job-601";
assert.equal(filteredPeople.value[0]?.record_id, "person-601", "可搜索跨楼栋且位于第500条之后的人员");
assert.equal(commanderPeople.value.length, 602, "参演人员搜索不能缩小指挥人候选");
peopleSearch.value = "";
peopleExpanded.value = false;
assert.equal(filteredPeople.value.length, 2, "仍可手动仅看本楼人员");
peopleExpanded.value = true;
assert.equal(filteredPeople.value.length, 602, "展开后不得截断其他人员");
people.value = [...directory, { record_id: "external:temporary-1", name: "临时参演人", building: "A", employee_no: "temporary-job" }];
peopleSearch.value = "临时参演人";
assert.deepEqual(filteredPeople.value.map(person => person.record_id), ["external:temporary-1"], "跨楼栋临时签名人员可搜索");
assert(commanderPeople.value.some(person => person.record_id === "external:temporary-1"), "临时签名人员也可选为指挥人");

const signingExecution = ref({participants:[directory[0]],step_signers:{14:['person-0','']}});
const signerNotices=[];
let dirtyCount=0;
const signingContext={
  ...context,commanderPeople,execution:signingExecution,
  participantIds:computed(()=>signingExecution.value.participants.map(p=>p.record_id)),
  selectedParticipants:computed(()=>signingExecution.value.participants),
  blankStepSignerOption:'留空（事后签名）',arrayFrom:value=>Array.isArray(value)?value:[],
  personFromOptionLabel:label=>people.value.find(person=>person.record_id===label),
  compactPerson:person=>({record_id:person.record_id,name:person.name}),
  markDirty:()=>dirtyCount++,setNotice:text=>signerNotices.push(text),
  isEccStep:()=>false,
};
const signerFunctions=page.slice(page.indexOf('const stepSignerOptions ='),page.indexOf('function isEccStep('));
const signing=new Function(...Object.keys(signingContext),`${ts.transpile(signerFunctions)};return {stepSignerOptionLabels,updateStepSigner};`)(...Object.values(signingContext));
assert(signing.stepSignerOptionLabels(14,1).includes('person-601'),'步骤签名可直接选择尚未参演的跨楼人员');
assert(signing.stepSignerOptionLabels(14,1).includes('external:temporary-1'),'步骤签名覆盖外部签名人员');
assert(!signing.stepSignerOptionLabels(14,1).includes('person-0'),'同一步的其他签名位不可重复选人');
signing.updateStepSigner({row:14},1,'external:temporary-1');
assert.equal(signingExecution.value.step_signers[14][1],'external:temporary-1');
assert.equal(signingExecution.value.participants[1].record_id,'external:temporary-1','自动补入参演名单供原后端生成签名');
signing.updateStepSigner({row:14},1,'external:temporary-1');
assert.equal(signingExecution.value.participants.length,2,'重复选择不重复添加参演人');
signing.updateStepSigner({row:14},1,'person-0');
assert.match(signerNotices.pop(),/不能重复/);
assert.equal(signingExecution.value.step_signers[14][1],'external:temporary-1');
signingExecution.value.participants=[...directory.slice(0,9),people.value.at(-1)];
const before=JSON.stringify(signingExecution.value),beforeDirty=dirtyCount;
signing.updateStepSigner({row:14},1,'person-601');
assert.match(signerNotices.pop(),/已满 10 人/);
assert.equal(JSON.stringify(signingExecution.value),before,'名额已满时不得半更新签名或截掉原参演人员');
assert.equal(dirtyCount,beforeDirty);
signing.updateStepSigner({row:14},1,'person-8');
assert.equal(signingExecution.value.step_signers[14][1],'person-8','名额已满仍可选已有参演人');
signing.updateStepSigner({row:14},1,'留空（事后签名）');
assert.equal(signingExecution.value.step_signers[14][1],'');
assert.equal(signingExecution.value.participants.length,10,'清空步骤签名不擅自删除参演名单');

const execution = ref({drill_date:'2026-09-29',first_start_time:'09:00',evaluator:{},signature_time:'',evaluation_time:''});
const validationContext={execution,commanderId:ref('p1'),hasReviewMapping:ref(true),evaluatorId:ref(''),participantIds:ref(['p1']),executionSteps:ref([]),missingSignaturePeople:ref([])};
const validateExecution=new Function(...Object.keys(validationContext),`${ts.transpile(page.slice(page.indexOf('function validateExecution()'),page.indexOf('function buildExecutionPayload()')))};return validateExecution;`)(...Object.values(validationContext));
assert.deepEqual(validateExecution(),[],'the three review fields are optional');
execution.value.signature_time='invalid';assert(validateExecution().includes('请选择有效的演练审核人签名时间'));
execution.value.signature_time='';execution.value.evaluation_time='invalid';assert(validateExecution().includes('请选择有效的演练评估人评估时间'));
execution.value.evaluation_time='';validationContext.commanderId.value='';assert(validateExecution().includes('请选择指挥人'),'commander remains mandatory');
const chooseEvaluator=new Function('execution','personFromOptionLabel','compactPerson','markDirty',`${ts.transpile(page.slice(page.indexOf('function selectEvaluatorByLabel('),page.indexOf('\nfunction ',page.indexOf('function selectEvaluatorByLabel(')+1)))};return selectEvaluatorByLabel;`)(execution,()=>({record_id:'p2',name:'评估人'}),person=>person,()=>{});
chooseEvaluator('评估人');assert.equal(execution.value.signature_time,'');assert.equal(execution.value.evaluation_time,'','choosing a person must not fabricate timestamps');
execution.value.signature_time='2026-09-29T10:15';chooseEvaluator('不填写');assert.deepEqual(execution.value.evaluator,{});assert.equal(execution.value.signature_time,'2026-09-29T10:15','clearing the person preserves independently entered times');
const reviewTemplate=page.slice(page.indexOf('<span>演练审核人 / 评估人'),page.indexOf('<div v-if="previewLoading" class="inline-state"><Loader2 :size="20" class="spinning" /> 正在读取评估表'));
assert(!reviewTemplate.includes('<b>*</b>')&&!/\srequired[\s>]/.test(reviewTemplate),'optional review controls have no required markers');
console.log("drill management checks passed, including optional review fields, clearing and supplied-time validation");
