import assert from "node:assert/strict";
import { build } from "esbuild";
import { fileURLToPath } from "node:url";

// Bundle the TS module to a plain in-memory JS string, then import it via data URL.
const entry = fileURLToPath(new URL("../src/planConvergenceFocus.ts", import.meta.url));
const result = await build({
  entryPoints: [entry],
  bundle: true,
  format: "esm",
  platform: "node",
  write: false,
});
const code = result.outputFiles[0].text;
const mod = await import("data:text/javascript;base64," + Buffer.from(code).toString("base64"));
const { focusedDetailIndexes } = mod;

// Neutral default row: no device match, room far away from B-124 and not a real building.
const row = (over) => ({
  blockDetailId: "d-1",
  classifyModel: "楼宇自控",
  spaceModel: "Z-999机房",
  relateConfig: "Z-999告警",
  instances: "X-0",
  instanceIds: "ins-x0",
  ...over,
});
const notice = (over) => ({
  record_id: "r-1",
  name: "B楼空调检修",
  status: "开始",
  device: "B-124-BAS-101/B-124-BAS-103/B-150-BAS-102",
  location: "B-124/150控制柜",
  building: "B楼",
  fault: "控制柜指示灯微亮或不亮",
  rooms: ["B-124", "B-150"],
  ...over,
});

// ---- single / empty ----
assert.deepEqual(focusedDetailIndexes([], notice()), [], "empty rows -> []");
assert.deepEqual(focusedDetailIndexes([row()], notice()), [0], "single row -> [0]");

// ---- second page index 26 (device match on row 26) ----
{
  const many = Array.from({ length: 30 }, (_, i) =>
    row({
      blockDetailId: "d-" + (i + 1),
      instances: i === 26 ? "BAS-101" : "OTHER-" + i,
      instanceIds: i === 26 ? "ins-target" : "ins-" + (i + 1),
    })
  );
  assert.deepEqual(focusedDetailIndexes(many, notice()), [26], "device match on index 26");
}

// ---- Chinese suffix on device names: extract code before comparing ----
{
  const rows = [row({ instances: "BAS-101控制柜" }), row({ instances: "OTHER" })];
  assert.deepEqual(
    focusedDetailIndexes(rows, notice({ device: "B-124-BAS-101" })),
    [0],
    "extract code from Chinese-suffixed instance name"
  );
  const fullRows = [
    row({ instances: "A-100-BAS-1010" }),
    row({ instances: "B-124-BAS-101控制柜" }),
  ];
  assert.deepEqual(
    focusedDetailIndexes(fullRows, notice({ device: "B-124-BAS-101" })),
    [1],
    "full Chinese-suffixed device name matches, prefix-collision excluded"
  );
}

// ---- building conflict excludes matching device in a clearly wrong building ----
{
  const rows3 = [
    row({ spaceModel: "A-100机房", instances: "BAS-101" }),
    row({ spaceModel: "C-300机房", instances: "BAS-103" }),
    row({ spaceModel: "D-400机房", instances: "BAS-999" }),
  ];
  assert.deepEqual(focusedDetailIndexes(rows3, notice()), [], "wrong-building device matches excluded");

  // Positive: short BAS-101 kept when the row room building matches notice B楼, A-500 excluded.
  const shortPos = [
    row({ spaceModel: "B-124机房", instances: "BAS-101" }),
    row({ spaceModel: "A-500机房", instances: "BAS-101" }),
  ];
  assert.deepEqual(focusedDetailIndexes(shortPos, notice()), [0], "short BAS-101 kept with matching B-124 room");
}

// ---- bare short BAS101 with B-124 room is extracted and building-filtered ----
{
  const bareShort = [
    row({ spaceModel: "B-124机房", instances: "BAS101" }),
    row({ spaceModel: "A-200机房", instances: "BAS101" }),
  ];
  assert.deepEqual(
    focusedDetailIndexes(bareShort, notice({ device: "BAS101", building: "B楼" })),
    [0],
    "bare BAS101 kept with matching B-124 room, A-200 excluded"
  );
}

// ---- names as arrays/objects, no [object Object] coercion ----
{
  const rowsArr = [row({}), row({ instances: ["BAS-101"] })];
  assert.deepEqual(focusedDetailIndexes(rowsArr, notice()), [1], "array-of-string instances matches only row with device");

  const rowsObj = [row({ instances: [{ name: "BAS-101" }] }), row({})];
  assert.deepEqual(focusedDetailIndexes(rowsObj, notice()), [0], "array-of-object name matches");

  const rowsObj2 = [row({ instances: [{ instanceName: "BAS-103" }] }), row({})];
  assert.deepEqual(focusedDetailIndexes(rowsObj2, notice()), [0], "array-of-object instanceName matches");

  const rowsObject = [row({ instances: { name: "BAS-101" } }), row({})];
  assert.deepEqual(focusedDetailIndexes(rowsObject, notice()), [0], "single object instances matches");

  const rowsNonCoerced = [row({ instances: { foo: "bar" } }), row({ instances: ["BAS-101"] })];
  assert.deepEqual(focusedDetailIndexes(rowsNonCoerced, notice()), [1], "no [object Object] coercion");
}

// ---- instanceNames field ----
{
  const rows = [row({}), row({ instances: "", instanceNames: "BAS-101" })];
  assert.deepEqual(focusedDetailIndexes(rows, notice()), [1], "instanceNames field used");
}

// ---- IDs: only when notice explicitly provides the same opaque IDs ----
{
  const idRows = [row({ instanceIds: "ins-x101" }), row({})];
  // no explicit IDs in notice -> no match from opaque ids
  assert.deepEqual(focusedDetailIndexes(idRows, notice()), [],
    "opaque ids ignored when notice does not provide them");
  // explicit IDs in notice -> exact match on row.instanceIds
  const idNotice = notice({ device: "", name: "B楼设备", instanceIds: "ins-x101" });
  assert.deepEqual(focusedDetailIndexes(idRows, idNotice), [0], "explicit notice ids match exact opaque ids");
  // still no partial/name matching for ids
  assert.deepEqual(
    focusedDetailIndexes(
      [row({ instanceIds: "ins-x101" }), row({ instanceIds: "ins-x101-extra" })],
      idNotice
    ),
    [0],
    "exact id only"
  );
}

// ---- room fallback (no device evidence) ----
{
  const roomNotice = notice({ device: "", name: "B楼某房间检修" });
  const roomRows = [
    row({ instances: "X-1", spaceModel: "A-100机房" }),
    row({ instances: "X-2", spaceModel: "B-124机房" }),
  ];
  assert.deepEqual(focusedDetailIndexes(roomRows, roomNotice), [1], "room fallback to unique row");
  // hyphen/case normalization: notice B-124 vs row b124
  const normRows = [row({ spaceModel: "b124机房" }), row({ spaceModel: "C-300机房" })];
  assert.deepEqual(focusedDetailIndexes(normRows, roomNotice), [0], "room normalization hyphen/case");
  // row.spaceModelName used too
  const nameRows = [row({ spaceModel: "", spaceModelName: "B124机房" }), row({})];
  assert.deepEqual(focusedDetailIndexes(nameRows, roomNotice), [0], "room fallback via spaceModelName");
}

// ---- room fallback dedup: same room in spaceModel AND spaceModelName counts the row once ----
{
  const roomNotice = notice({ device: "", name: "B楼某房间", rooms: ["B-124"] });
  const dedupRows = [
    row({ spaceModel: "B-124机房", spaceModelName: "B-124机房" }),
    row({ spaceModel: "Z-999机房", spaceModelName: "" }),
  ];
  assert.deepEqual(focusedDetailIndexes(dedupRows, roomNotice), [0], "same room in both fields is not ambiguous");

  const dedupAmb = [
    row({ spaceModel: "B-124机房", spaceModelName: "B-124机房" }),
    row({ spaceModel: "B-124机房", spaceModelName: "B-124机房" }),
  ];
  assert.deepEqual(focusedDetailIndexes(dedupAmb, roomNotice), [], "two distinct same-room rows still ambiguous");
}

// ---- mismatched building: room match rejected ----
{
  const cm = notice({ device: "", name: "C楼某房间检修", building: "C楼", rooms: ["C-200"] });
  const rows = [row({ spaceModel: "B-124机房" }), row({ spaceModel: "C-200机房" })];
  assert.deepEqual(focusedDetailIndexes(rows, cm), [1], "declared building C keeps only C room");
}

// ---- EA118 false-positive: A118 never extracted from EA118 ----
{
  const ea = notice({ device: "", name: "EA118_C01机房B楼设备检修", rooms: ["B-124"] });
  const rows = [row({ spaceModel: "A-118机房" }), row({ spaceModel: "B-124机房" })];
  assert.deepEqual(focusedDetailIndexes(rows, ea), [1], "EA118 does not match A118");
}

// ---- device regex: room-only B-124 is not a device id; EA118_C01 is excluded ----
{
  const roomOnlyNotice = notice({ device: "B-124", name: "" });
  const roomOnlyRows = [
    row({ instances: "B-124", spaceModel: "Z-999机房" }),
    row({ instances: "X-1", spaceModel: "B-124机房" }),
  ];
  assert.deepEqual(focusedDetailIndexes(roomOnlyRows, roomOnlyNotice), [1], "B-124 treated as room, not device");

  const eaDevNotice = notice({ device: "EA118_C01", name: "" });
  const eaDevRows = [
    row({ instances: "EA118_C01", spaceModel: "Z-999机房" }),
    row({ instances: "BAS-101", spaceModel: "Z-999机房" }),
  ];
  assert.deepEqual(focusedDetailIndexes(eaDevRows, eaDevNotice), [], "EA118_C01 is not treated as a device id");
}

// ---- prefix collisions: BAS-101 must not match BAS-1010 ----
{
  const pre1 = notice({ device: "BAS-101", name: "" });
  const rows1 = [row({ instances: "BAS-1010" }), row({ instances: "BAS-101" })];
  assert.deepEqual(focusedDetailIndexes(rows1, pre1), [1], "BAS-101 not BAS-1010");

  const pre2 = notice({ device: "B-124-BAS-101", name: "" });
  const rows2 = [row({ instances: "BAS-1010" }), row({ instances: "BAS-101" })];
  assert.deepEqual(focusedDetailIndexes(rows2, pre2), [1], "full id BAS-101 not BAS-1010");

  const pre3 = notice({ device: "BAS-1010", name: "" });
  const rows3 = [row({ instances: "BAS-101" }), row({ instances: "BAS-1010" })];
  assert.deepEqual(focusedDetailIndexes(rows3, pre3), [1], "longer BAS-1010 kept, BAS-101 excluded");
}

// ---- no clues / name-only / ambiguous room -> [] ----
{
  assert.deepEqual(
    focusedDetailIndexes([row({}), row({})], notice({ device: "", rooms: [] })),
    [],
    "no clues -> []"
  );
  assert.deepEqual(
    focusedDetailIndexes(
      [row({ spaceModel: "Z-100机房", instances: "AAA" }), row({ spaceModel: "Z-200机房", instances: "BBB" })],
      notice({ name: "纯名称匹配", device: "", rooms: [] })
    ),
    [],
    "name-only -> []"
  );
  const ambRows = [row({ spaceModel: "B-124机房" }), row({ spaceModel: "B-124机房" }), row({})];
  assert.deepEqual(focusedDetailIndexes(ambRows, notice({ device: "", rooms: ["B-124"] })), [],
    "ambiguous multiple room matches -> []");
}

// ---- block title never used ----
{
  const rows = [row({}), row({})];
  const n = notice({ name: "B-124控制柜检修屏蔽", device: "", rooms: [] });
  assert.deepEqual(focusedDetailIndexes(rows, n), [], "block title not invented as evidence");
}

console.log("[PlanConvergenceFocusCheck] OK");