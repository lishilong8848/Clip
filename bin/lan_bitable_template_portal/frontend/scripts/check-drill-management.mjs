import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

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
  "MopSignaturePadModal",
  "stepSignerOptionLabels",
  "commanderPersonOptionLabels",
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
if (/type="file"[^>]*\brequired\b/.test(page)) throw new Error("演练拖放文件输入仍被 required 拦截");
if (!app.includes('routePath.value === "/drill-management/print"')) throw new Error("App 缺少演练打印路由");
if (!home.includes('primaryAction: { key: "drill"')) throw new Error("首页演练入口未启用");

console.log("drill management static check passed");
