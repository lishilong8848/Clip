import type { Dict } from "./api/client";

export type ScopeItem = Dict;

export type Draft = {
  label: string;
  rule_type: "normal" | "common";
  items: ScopeItem[];
};

export type RoomNode = {
  level: "zone" | "building" | "floor" | "room";
  key: string;
  name: string;
  devices: number;
  _checked: boolean;
  /** 加载该节点时的完整祖先空间（zone/building/floor），选中后长期保留、不随钻取位漂移。 */
  zone: string;
  building: string;
  floor: string;
};

export type Drill = { zone: string; building: string; floor: string };

/**
 * 与 legacy ruleset.html 完全一致的 itemKey，用于跨条目去重。
 */
export function itemKey(it: Dict): string {
  return [
    it.scope_type,
    it.obj_name || "",
    it.zone || "",
    it.building || "",
    it.floor || "",
    it.room || "",
    it.inst_name || "",
    it.point_name || "",
    it.rule_name || "",
    it.alarm_config_id || "",
  ].join("|");
}

/** scope 条目人读描述（与 legacy itemDesc 一致）。 */
export function itemDesc(it: Dict): string {
  const sp =
    [it.zone, it.building, it.floor, it.room].filter(Boolean).join("-") ||
    (it.building || it.floor || it.room
      ? [it.building, it.floor, it.room].filter(Boolean).join("-")
      : "");
  switch (it.scope_type) {
    case "all":
      return "全部（历史遗留，建议拆分重选）";
    case "objtype":
      return (it.obj_name || "(未分类)") + " · 整类全部设备";
    case "objtype_room":
      return (it.obj_name || "(未分类)") + " @ " + (sp || "(未指定空间)");
    case "zone":
      return (it.zone || "?") + " 区全部";
    case "building":
      return (it.building || "?") + " 楼全部";
    case "floor":
      return (it.building || "") + "-" + (it.floor || "?") + " 层全部";
    case "room":
      return sp || "(未指定空间)";
    case "device":
      return (it.inst_name || "?") + " · 整台设备";
    case "point":
      return (it.inst_name || "?") + " · " + (it.rule_name || it.point_name || "?");
    default:
      return String(it.scope_type || "");
  }
}

/** 组合条目摘要名（与 legacy entryLabel 一致）。 */
export function entryLabel(items: ScopeItem[]): string {
  const uniq = [...new Set(items.map(itemDesc))];
  if (uniq.length === 1) return uniq[0];
  return uniq[0] + " 等 " + uniq.length + " 类";
}

/** scope_type 中文标签（与 legacy 一致）。 */
export function scopeTypeLabel(st: string): string {
  const map: Dict = {
    all: "全部",
    objtype: "设备类型",
    objtype_room: "类型×空间",
    zone: "园区",
    building: "楼栋",
    floor: "楼层",
    room: "房间",
    device: "整台设备",
    point: "规则绑定",
    exclude_device: "排除设备",
    exclude_point: "排除设备(兼容)",
  };
  return map[st] ?? st ?? "";
}

export function entryKeyInfo(it: Dict): string {
  const st = it.scope_type;
  if (st === "room") return it.room || "";
  if (st === "building") return it.building || "";
  if (st === "floor") return it.floor || "";
  if (st === "zone") return it.zone || "";
  if (st === "device") return it.inst_name || "";
  if (st === "exclude_device") return "排除：" + (it.inst_name || "");
  if (st === "point") {
    const r = it.rule_name || it.point_name || "";
    return r + (it.alarm_config_id ? " · 配置ID " + it.alarm_config_id : "");
  }
  return it.obj_name || it.inst_name || it.point_name || "";
}

/** 扁平化为提交 items：按草稿顺序重编组号，带公共/普通标记与组名（与 legacy allDraftItems 一致）。 */
export function allDraftItems(drafts: Draft[]): ScopeItem[] {
  const out: ScopeItem[] = [];
  drafts.forEach((d, idx) => {
    d.items.forEach((it) => {
      out.push({
        ...it,
        rule_group_no: idx + 1,
        rule_type: d.rule_type || "normal",
        rule_label: d.label || "",
      });
    });
  });
  return out;
}

/**
 * 归一化摘要键：把「将要提交」的 items（含 rule_type/rule_label/rule_group_no 等元数据）
 * 序列化成与草稿排列无关、可稳定比较的字符串，用于脏检测（dirty）。
 * - 每个 item 先剔除 _checked 等仅存在于 UI 的字段，仅保留提交载荷字段；
 * - 组号按 rule_group_no 参与排序，因此组顺序调整会反映为脏；
 * - rule_type / rule_label 一并纳入，切换公共/普通或改组名都会使脏。
 */
export function submittedKey(items: ScopeItem[]): string {
  const pairs = items.map((it) => {
    const { _checked, ...rest } = (it || {}) as Dict;
    void _checked;
    return JSON.stringify(rest);
  });
  return pairs.sort().join("\u0001");
}

/** 由草稿还原「完整提交 items」并计算其归一化脏键。 */
export function draftsSubmitKey(drafts: Draft[]): string {
  return submittedKey(allDraftItems(drafts));
}

/** 列2 节点转空间参数（与 legacy roomToSelParam 一致，但使用节点加载时的上下文，不再随钻取位漂移）。 */
export function roomToSelParam(r: RoomNode): Dict {
  if (r.level === "zone") return { zone: r.key, building: "", floor: "", room: "" };
  if (r.level === "building") return { zone: r.zone, building: r.key, floor: "", room: "" };
  if (r.level === "floor") return { zone: r.zone, building: r.building, floor: r.key, room: "" };
  return { zone: r.zone, building: r.building, floor: r.floor, room: r.key };
}

/**
 * 空间节点唯一键：由完整不可变祖先路径（zone/building/floor）与层级、key 组成。
 * 楼栋/房间等信息仅凭 key 或 level:key 无法区分不同建筑/区的同名空间，
 * 排序不受选择或钻取位影响，可安全用作模板 key、跨级保留 carry map 与选择签名。
 */
export function roomNodeKey(r: RoomNode): string {
  return [r.level, r.zone || "", r.building || "", r.floor || "", r.key].join("|");
}

/** 已保存 items 按 rule_group_no 还原草稿组（与 legacy selectSet 还原一致）。 */
export function restoreDrafts(items: ScopeItem[]): Draft[] {
  const byGroup = new Map<number, Draft>();
  items.forEach((it) => {
    const gno = Number(it.rule_group_no ?? 1);
    let g = byGroup.get(gno);
    if (!g) {
      g = {
        rule_type: it.rule_type === "normal" ? "normal" : "common",
        label: it.rule_label || itemDesc(it),
        items: [],
      };
      byGroup.set(gno, g);
    }
    g.items.push(it);
  });
  return [...byGroup.values()];
}

