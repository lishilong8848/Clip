/**
 * Conservative, deterministic UI focus targeting for block-detail rows after a
 * maintenance/change notice hit. Pure UI logic; never invents matches from block
 * titles or fuzzy substrings. We only highlight rows backed by real equipment
 * identifiers or unambiguous room tokens, and we drop rows whose room clearly
 * contradicts the notice's declared building.
 */

type Row = Record<string, any>;
type Notice = Record<string, any>;

const DEVICE_KEYS = ["instances", "instanceNames", "instName", "insNames"];
const EXPLICIT_KEYS = ["instanceIds", "instance_ids", "insIds", "ins_ids", "deviceIds", "device_ids", "instIds", "inst_ids"];

/** Flatten scalar/array/object to a searchable string without "[object Object]". */
function text(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
  if (Array.isArray(v)) return v.map(text).join(" ");
  if (typeof v === "object") {
    const picks = ["name", "text", "value", "label", "title", "insName", "instanceName", "device", "location"];
    return picks.map(k => text((v as Record<string, unknown>)[k])).filter(Boolean).join(" ");
  }
  return "";
}

/** Scalar tokens separated by common delimiters (used for ids/codes/names). */
function tokens(v: unknown): string[] {
  return text(v).split(/[\s,，、;；/]+/).map(s => s.trim()).filter(Boolean);
}

/** Normalized unique room codes (B-124 -> B124). EA118 never yields A118; A1180 never truncates to A118. */
const roomRe = /(?<![A-Za-z0-9])[ABCDEH]-?\d{3}(?![0-9])/gi;
function rooms(v: unknown): string[] {
  const out = new Set<string>();
  for (const m of text(v).matchAll(roomRe)) out.add(m[0].toUpperCase().replace(/-/g, ""));
  return [...out];
}

/** Declared building letters ("B楼" or bare "B"). */
function buildings(v: unknown): Set<string> {
  const set = new Set<string>();
  const s = text(v);
  for (const m of s.matchAll(/([ABCDEH])\s*楼/gi)) set.add(m[1].toUpperCase());
  for (const m of s.matchAll(/(?<![A-Za-z0-9])([ABCDEH])(?![A-Za-z0-9])/g)) set.add(m[1].toUpperCase());
  return set;
}

const multiRe = /[A-Za-z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)+/gi; // BAS-101, B-124-BAS-101, EA118_C01
const bareRe = /[A-Za-z]{3,}\d+[A-Za-z0-9]*/g;                 // BAS101 (no room/building prefix)

/**
 * Equipment identifiers found in a value. Rejects room-only codes (B-124, A100)
 * and room-location codes (EA118_C01); requires an equipment segment beyond the room.
 */
function equipmentCodes(v: unknown): Set<string> {
  const out = new Set<string>();
  const s = text(v);
  for (const m of s.matchAll(multiRe)) {
    const cand = m[0].toUpperCase();
    const roomOnly = /^[ABCDEH]-?\d{3}$/.test(cand);
    const roomLoc = /^[A-Za-z]{2}\d{3}[-_]/.test(cand);
    if (!roomOnly && !roomLoc) out.add(cand);
  }
  for (const m of s.matchAll(bareRe)) out.add(m[0].toUpperCase());
  return out;
}

/** Segment-exact suffix: BAS-101 matches B-124-BAS-101 but never BAS-1010. */
function sameDevice(a: string, b: string): boolean {
  if (a === b) return true;
  const long = a.length >= b.length ? a : b;
  const short = a.length >= b.length ? b : a;
  if (!long.endsWith(short)) return false;
  const before = long[long.length - short.length - 1];
  return before === undefined || before === "-" || before === "_" || before === "/" || before === " ";
}

/** True only when the row's known rooms never match any declared building. */
function conflicts(row: Row, declared: Set<string>): boolean {
  if (declared.size === 0) return false;
  const present = new Set<string>();
  for (const r of [...rooms(row.spaceModel), ...rooms(row.spaceModelName)]) present.add(r[0]);
  if (present.size === 0) return false;
  return [...present].every(b => !declared.has(b));
}

/** Zero-based detail row indices to highlight for a matched maintenance/change notice. */
export function focusedDetailIndexes(rows: Row[], notice: Notice): number[] {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  if (rows.length === 1) return [0];

  const declared = buildings(notice.building);

  // --- Step 1: exact equipment / explicit-id evidence ---
  const ids = new Set<string>([...equipmentCodes(notice.device), ...equipmentCodes(notice.name)]);
  const explicit = new Set<string>();
  for (const key of EXPLICIT_KEYS) for (const t of tokens(notice[key])) explicit.add(t);

  const deviceMatches: number[] = [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || {};
    const rowCodes = new Set<string>();
    for (const key of DEVICE_KEYS) for (const c of equipmentCodes(row[key])) rowCodes.add(c);
    let hit = false;
    outer: for (const id of ids) {
      for (const c of rowCodes) {
        if (sameDevice(id, c)) { hit = true; break outer; }
      }
    }
    if (!hit && explicit.size > 0) {
      for (const t of tokens(row.instanceIds)) {
        if (explicit.has(t)) { hit = true; break; }
      }
    }
    if (hit && !conflicts(row, declared)) deviceMatches.push(i);
  }
  if (deviceMatches.length > 0) return deviceMatches;

  // --- Step 2: unambiguous room-token fallback (each row counted once) ---
  const noticeRooms = new Set<string>();
  for (const src of [notice.rooms, notice.location, notice.name, notice.device]) {
    for (const r of rooms(src)) noticeRooms.add(r);
  }
  if (noticeRooms.size === 0) return [];

  let found = -1;
  let ambiguous = false;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] || {};
    const rowRooms = new Set<string>([...rooms(row.spaceModel), ...rooms(row.spaceModelName)]);
    let matches = false;
    for (const rr of rowRooms) {
      if (noticeRooms.has(rr) && (declared.size === 0 || declared.has(rr[0]))) { matches = true; break; }
    }
    if (matches) {
      if (found === -1) found = i;
      else { ambiguous = true; break; }
    }
  }
  return ambiguous || found === -1 ? [] : [found];
}