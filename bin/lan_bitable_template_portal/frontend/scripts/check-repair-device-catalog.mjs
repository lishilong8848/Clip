/**
 * Node-only verification of the pure device catalog helpers extracted into
 * src/repairManagementUtils.ts:
 *
 *   - splitDeviceNames
 *   - normalizedCatalogKey
 *   - resolveRepairDeviceCatalog
 *   - repairModelsForBrand
 *
 * The utils file is TypeScript. We transpile it with the already-installed
 * esbuild (type-only imports erased) and load the result from a data: URL so
 * this check runs without a build step and without installing anything.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as esbuild from "esbuild";

const utilsUrl = new URL("../src/repairManagementUtils.ts", import.meta.url);
const source = readFileSync(utilsUrl, "utf8");
const { code } = esbuild.transformSync(source, {
  loader: "ts",
  format: "esm",
  target: "node18",
});
const utils = await import(
  `data:text/javascript;base64,${Buffer.from(code, "utf8").toString("base64")}`,
);

const {
  splitDeviceNames,
  normalizedCatalogKey,
  resolveRepairDeviceCatalog,
  repairModelsForBrand,
  repairDeviceDependentPatch,
  repairFollowupFieldDisabled,
  repairFollowupFieldPlaceholder,
} = utils;

assert.equal(repairFollowupFieldDisabled('供应商名称', { '维修方': '我方' }), true);
assert.equal(repairFollowupFieldDisabled('供应商维修人员', { '维修方': '我方' }), true);
assert.equal(repairFollowupFieldDisabled('供应商名称', { '维修方': '供应商' }), false);
assert.equal(repairFollowupFieldDisabled('设备型号', { '设备品牌': '' }), true);
assert.equal(repairFollowupFieldDisabled('设备型号', { '设备品牌': '双登' }), false);
assert.equal(repairFollowupFieldDisabled('维修进度', { '维修方': '我方' }), false);
assert.equal(repairFollowupFieldPlaceholder('供应商名称', { '维修方': '我方' }), '维修方为我方，无需填写');
assert.equal(repairFollowupFieldPlaceholder('设备型号', {}), '请先选择设备品牌');
assert.equal(repairFollowupFieldPlaceholder('设备型号', { '设备品牌': '双登' }), '选择或输入设备型号');

// ---- Test fixtures ---------------------------------------------------------
const deviceCatalog = {
  "设备A": { "品牌X": ["M1", "M2"], "品牌Y": ["N1"] },
  "设备B": { "品牌X": ["M2", "M3"], "品牌Z": ["P1"] },
  "spaceDevice": { "品牌X": ["S1"] },
  "CaseDevice": { "品牌X": ["C1"] },
  "设备C": { "品牌X": [] },
};
const brandCatalog = {
  "品牌X": ["FB1", "FB2"],
  "品牌Y": ["FY1"],
  "品牌Z": ["FZ1"],
};

const draft = { "设备名称": "设备A", "设备品牌": "品牌X", "设备型号": "M1", "设备编号": "原编号" };
assert.deepEqual(repairDeviceDependentPatch(draft, "设备名称", deviceCatalog, brandCatalog), {});
assert.deepEqual(repairDeviceDependentPatch({ ...draft, "设备名称": "设备B" }, "设备名称", deviceCatalog, brandCatalog), { "设备型号": "" });
assert.deepEqual(repairDeviceDependentPatch({ ...draft, "设备品牌": "品牌Z" }, "设备名称", deviceCatalog, brandCatalog), { "设备品牌": "", "设备型号": "" });
assert.deepEqual(repairDeviceDependentPatch({ ...draft, "设备品牌": "品牌Y" }, "设备品牌", deviceCatalog, brandCatalog), { "设备型号": "" });
assert.deepEqual(repairDeviceDependentPatch({ ...draft, "设备名称": "未知设备", "设备编号": "" }, "设备名称", deviceCatalog, brandCatalog), { "设备编号": "未知设备" });
assert.deepEqual(repairDeviceDependentPatch({ ...draft, "设备名称": "", "设备编号": "" }, "设备名称", deviceCatalog, brandCatalog), { "设备编号": "" });
assert.deepEqual(repairDeviceDependentPatch(draft, "维修进展描述", deviceCatalog, brandCatalog), {});
assert.deepEqual(draft, { "设备名称": "设备A", "设备品牌": "品牌X", "设备型号": "M1", "设备编号": "原编号" });

// ---- splitDeviceNames: punctuation + dedupe --------------------------------
assert.deepEqual(splitDeviceNames("A、B,C；D,E"), ["A", "B", "C", "D", "E"]);
assert.deepEqual(splitDeviceNames("A、A、B"), ["A", "B"]);
assert.deepEqual(splitDeviceNames("A,，B；;\nC\r\nD"), ["A", "B", "C", "D"]);
assert.deepEqual(splitDeviceNames(""), []);
assert.deepEqual(splitDeviceNames(null), []);

// ---- normalizedCatalogKey: space strip + lowercase --------------------------
assert.equal(normalizedCatalogKey("  ABC  D "), "abcd");
assert.equal(normalizedCatalogKey("设备 A"), "设备a");
assert.equal(normalizedCatalogKey("Case Device"), "casedevice");

// ---- Empty / missing --------------------------------------------------------
assert.deepEqual(resolveRepairDeviceCatalog("", deviceCatalog), { matched: false, brandModels: {} });
assert.deepEqual(resolveRepairDeviceCatalog(null, deviceCatalog), { matched: false, brandModels: {} });
assert.deepEqual(resolveRepairDeviceCatalog(undefined, deviceCatalog), { matched: false, brandModels: {} });
assert.deepEqual(resolveRepairDeviceCatalog("   \n", deviceCatalog), { matched: false, brandModels: {} });
assert.equal(repairModelsForBrand("", "设备A", deviceCatalog, brandCatalog).length, 0);
assert.equal(repairModelsForBrand(undefined, "设备A", deviceCatalog, brandCatalog).length, 0);
assert.equal(repairModelsForBrand("设备A", "", deviceCatalog, brandCatalog).length, 0);

// ---- Direct + normalized name matching --------------------------------------
assert.deepEqual(
  resolveRepairDeviceCatalog("设备A", deviceCatalog),
  { matched: true, brandModels: { "品牌X": ["M1", "M2"], "品牌Y": ["N1"] } },
);
// Space-normalized lookup: catalog key has no space, value does.
assert.deepEqual(
  resolveRepairDeviceCatalog("space Device", deviceCatalog),
  { matched: true, brandModels: { "品牌X": ["S1"] } },
);
// Lowercase-normalized lookup: catalog key has uppercase letters.
assert.deepEqual(
  resolveRepairDeviceCatalog("casedevice", deviceCatalog),
  { matched: true, brandModels: { "品牌X": ["C1"] } },
);

// ---- Comma / semicolon multi-name merging -----------------------------------
assert.deepEqual(
  resolveRepairDeviceCatalog("设备A、设备B", deviceCatalog),
  { matched: true, brandModels: { "品牌X": ["M1", "M2", "M3"], "品牌Y": ["N1"], "品牌Z": ["P1"] } },
);
assert.deepEqual(
  resolveRepairDeviceCatalog("设备A,设备B;spaceDevice", deviceCatalog),
  { matched: true, brandModels: { "品牌X": ["M1", "M2", "M3", "S1"], "品牌Y": ["N1"], "品牌Z": ["P1"] } },
);

// ---- Duplicate model removal -------------------------------------------------
const mergedModels = resolveRepairDeviceCatalog("设备A、设备B", deviceCatalog).brandModels["品牌X"];
assert.deepEqual(mergedModels, ["M1", "M2", "M3"]);

// ---- Empty model arrays inside a matched device ------------------------------
// A matched device whose brand holds an empty model list still reports
// matched=true but contributes no brands (same as original behavior).
assert.deepEqual(
  resolveRepairDeviceCatalog("设备C", deviceCatalog),
  { matched: true, brandModels: {} },
);

// ---- Matched-empty vs fallback ------------------------------------------------
// Matched device has no "品牌Z" -> returns [] (never falls back to brandCatalog).
assert.deepEqual(repairModelsForBrand("品牌Z", "设备A", deviceCatalog, brandCatalog), []);
// Non-matched device -> falls back to brandCatalog.
assert.deepEqual(
  repairModelsForBrand("品牌X", "未知设备", deviceCatalog, brandCatalog),
  ["FB1", "FB2"],
);

// ---- Brand trim ---------------------------------------------------------------
assert.deepEqual(repairModelsForBrand("  品牌X  ", "设备A", deviceCatalog, brandCatalog), ["M1", "M2"]);
assert.deepEqual(repairModelsForBrand("\t品牌Y\n", "设备A", deviceCatalog, brandCatalog), ["N1"]);

// ---- Device catalog precedence -------------------------------------------------
// Matched device must win over the brandCatalog fallback values for the same brand.
assert.deepEqual(repairModelsForBrand("品牌X", "设备A", deviceCatalog, brandCatalog), ["M1", "M2"]);
assert.deepEqual(repairModelsForBrand("品牌Z", "设备B", deviceCatalog, brandCatalog), ["P1"]);

console.log("[CheckRepairDeviceCatalog] OK");
