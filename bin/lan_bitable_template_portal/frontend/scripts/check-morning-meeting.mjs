import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { ref } from "vue";
import ts from "typescript";

const page = readFileSync(new URL("../src/components/DailyTaskChecklistPage.vue", import.meta.url), "utf8");
const functions = page.slice(page.indexOf("async function openMorningMeeting"), page.indexOf("function downloadMorningMeeting"));
const context = {
  scopeCode: ref("H"), selectedDate: ref("2026-09-03"), today: "2026-09-03",
  morningLoading: ref(false), morningBusy: ref(false), morningDialogOpen: ref(false),
  morningError: ref(""), morningSuccess: ref(""), morningModel: ref(null),
  morningDownloadUrl: ref(""), morningPrintUrl: ref(""), morningGeneratedAt: ref(""), morningOperationId: ref(""),
  morningEnvironmentLoading: ref(false), morningEnvironmentWarnings: ref([]), morningEnvironmentEdited: new Set(),
  newOperationId: () => "test-generation",
};
const pending = [];
let preview = { date: context.today, generated: false, weather_condition: null, rows: [{ scope: "A", lines: ["值班巡检"] }] };
let submitted;
context.requestJson = async (url, options) => {
  if (url.includes("temperature_only=1")) return new Promise((resolve, reject) => pending.push({ resolve, reject, signal: options.signal }));
  if (url.endsWith("/generate")) {
    submitted = JSON.parse(options.body);
    return { model: { ...submitted }, download_url: "/download", print_url: "/print" };
  }
  return structuredClone(preview);
};
const { openMorningMeeting, closeMorningMeeting, generateMorningMeeting } = new Function(...Object.keys(context),
  `let morningRequestController = null;\n${ts.transpile(functions)}\nreturn { openMorningMeeting, closeMorningMeeting, generateMorningMeeting };`,
)(...Object.values(context));
const flush = () => new Promise(resolve => setTimeout(resolve, 0));
const temperatures = { weather_condition: "小雨", dry_bulb_temperature: 29.89, wet_bulb_temperature: 26.46, warnings: ["环境接口班次数据"] };

await openMorningMeeting();
assert.equal(context.morningLoading.value, false, "通告预览不能等待温度");
assert.equal(context.morningEnvironmentLoading.value, true);
assert.equal(context.morningModel.value.rows[0].lines[0], "值班巡检");
context.morningModel.value.dry_bulb_temperature = 27.1;
context.morningEnvironmentEdited.add("dry_bulb_temperature");
pending.at(-1).resolve(temperatures);
await flush();
assert.equal(context.morningModel.value.dry_bulb_temperature, 27.1, "不能覆盖用户手填值");
assert.equal(context.morningModel.value.wet_bulb_temperature, 26.46, "未编辑字段自动回填");
assert.equal(context.morningModel.value.weather_condition, "小雨", "天气也由环境接口自动回填");
assert.equal(context.morningEnvironmentLoading.value, false);

await openMorningMeeting();
const old = pending.at(-1);
closeMorningMeeting();
assert.equal(old.signal.aborted, true);
await openMorningMeeting();
old.resolve(temperatures);
await flush();
assert.equal(context.morningModel.value.wet_bulb_temperature, undefined, "旧弹窗请求不可回填新弹窗");
context.morningModel.value.dry_bulb_temperature = "";
context.morningEnvironmentEdited.add("dry_bulb_temperature");
context.morningModel.value.weather_condition = "晴";
context.morningEnvironmentEdited.add("weather_condition");
pending.at(-1).resolve(temperatures);
await flush();
assert.equal(context.morningModel.value.dry_bulb_temperature, "", "用户主动清空的温度也不可覆盖");
assert.equal(context.morningModel.value.weather_condition, "晴", "用户手动填写的天气不可覆盖");

await openMorningMeeting();
pending.at(-1).resolve({ weather_condition: null, dry_bulb_temperature: 0, wet_bulb_temperature: null, warnings: ["当班天气暂无数据"] });
await flush();
assert.equal(context.morningModel.value.weather_condition, null, "缺失天气不复用上次结果");
assert.equal(context.morningModel.value.dry_bulb_temperature, 0, "有效零值不能丢失");
assert.equal(context.morningModel.value.wet_bulb_temperature, undefined, "空温度不能补成零");

await openMorningMeeting();
const duringGeneration = pending.at(-1);
context.morningModel.value.dry_bulb_temperature = 25;
context.morningModel.value.wet_bulb_temperature = 24;
await generateMorningMeeting();
assert.equal(duringGeneration.signal.aborted, true);
assert.equal(submitted.dry_bulb_temperature, 25);
duringGeneration.resolve(temperatures);
await flush();
assert.equal(context.morningModel.value.dry_bulb_temperature, 25, "生成后预览必须与提交值保持一致");
assert.equal(context.morningEnvironmentLoading.value, false);

await openMorningMeeting();
pending.at(-1).reject(new Error("模拟温度请求失败"));
await flush();
assert.equal(context.morningError.value, "", "温度失败不应阻止通告预览和生成");
assert.match(context.morningEnvironmentWarnings.value[0], /模拟温度请求失败/);
preview = { ...preview, generated: true, dry_bulb_temperature: 25, wet_bulb_temperature: 24 };
const count = pending.length;
await openMorningMeeting();
assert.equal(pending.length, count, "已生成快照不重新读取温度");
assert.equal(context.morningModel.value.dry_bulb_temperature, 25);
closeMorningMeeting();
console.log("morning meeting background environment checks passed");
