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
const peopleExpanded = ref(false);
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
assert.equal(filteredPeople.value.length, 2, "默认只展示本楼人员");
assert.equal(commanderPeople.value.length, 602, "指挥人下拉搜索必须覆盖全部人员");
peopleSearch.value = "job-601";
assert.equal(filteredPeople.value[0]?.record_id, "person-601", "可搜索跨楼栋且位于第500条之后的人员");
assert.equal(commanderPeople.value.length, 602, "参演人员搜索不能缩小指挥人候选");
peopleSearch.value = "";
peopleExpanded.value = true;
assert.equal(filteredPeople.value.length, 602, "展开后不得截断其他人员");
people.value = [...directory, { record_id: "external:temporary-1", name: "临时参演人", building: "A", employee_no: "temporary-job" }];
peopleSearch.value = "临时参演人";
assert.deepEqual(filteredPeople.value.map(person => person.record_id), ["external:temporary-1"], "跨楼栋临时签名人员可搜索");
assert(commanderPeople.value.some(person => person.record_id === "external:temporary-1"), "临时签名人员也可选为指挥人");

console.log("drill management static check passed");
