export type NoticeSections = Record<string, string>;

export function toDatetimeLocal(value: string): string {
  const text = String(value || "").trim();
  const m = text.match(/(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})日?\D+(\d{1,2})[：:点](\d{1,2})?/);
  if (!m) return text.includes("T") ? text.slice(0, 16) : "";
  return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}T${m[4].padStart(2, "0")}:${(m[5] || "00").padStart(2, "0")}`;
}

export function normalizeNoticeLabel(label: string): string {
  return String(label || "").replace(/\s+/g, "").replace(/[：:]/g, "");
}

export function parseSections(text: string): NoticeSections {
  const sections: NoticeSections = {};
  const re = /【([^】]+)】([\s\S]*?)(?=(?:\n\s*)*【[^】]+】|$)/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(text))) sections[normalizeNoticeLabel(match[1])] = String(match[2] || "").trim();
  return sections;
}

export function sectionValue(sections: NoticeSections, names: string[], fallback = ""): string {
  for (const name of names) {
    const value = sections[normalizeNoticeLabel(name)];
    if (String(value || "").trim()) return String(value).trim();
  }
  return fallback;
}

export function pastedNoticeStatus(text: string): string {
  const match = String(text || "").match(/状态\s*[：:]\s*(开始|更新|结束)/);
  return match?.[1] || "开始";
}

export function inferBuildingText(...values: string[]): string {
  const text = values.filter(Boolean).join("\n").toUpperCase();
  const patterns: Array<[RegExp, string]> = [
    [/110\s*(?:站|KV)?|110站/i, "110站"],
    [/(?:A楼|A栋|\bA\b)/i, "A楼"],
    [/(?:B楼|B栋|\bB\b)/i, "B楼"],
    [/(?:C楼|C栋|\bC\b)/i, "C楼"],
    [/(?:D楼|D栋|\bD\b)/i, "D楼"],
    [/(?:E楼|E栋|\bE\b)/i, "E楼"],
    [/(?:H楼|H栋|\bH\b)/i, "H楼"],
    [/园区|ABC|A\/B\/C|A、B、C/i, "园区"],
  ];
  for (const [pattern, label] of patterns) if (pattern.test(text)) return label;
  return "";
}

export function splitNoticeTimeRange(value: string): { start: string; end: string } {
  const text = String(value || "").trim();
  if (!text) return { start: "", end: "" };
  const sameDayRange = text.match(
    /(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)\s*(\d{1,2}[：:点]\d{1,2})\s*(?:-|至|~|～|—|--)\s*(\d{1,2}[：:点]\d{1,2})/
  );
  if (sameDayRange) {
    const datePrefix = sameDayRange[1];
    return {
      start: toDatetimeLocal(`${datePrefix} ${sameDayRange[2]}`),
      end: toDatetimeLocal(`${datePrefix} ${sameDayRange[3]}`),
    };
  }
  const parts = text.split(/\s*(?:至|~|～|—|--)\s*/).filter(Boolean);
  if (parts.length >= 2) {
    const startRaw = parts[0];
    const endRaw = parts.slice(1).join(" ");
    const start = toDatetimeLocal(startRaw);
    let end = toDatetimeLocal(endRaw);
    if (!end && start) {
      const datePrefix = startRaw.match(/(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/)?.[1] || "";
      const endClock = endRaw.match(/(\d{1,2})[：:点](\d{1,2})?/)?.[0] || "";
      if (datePrefix && endClock) end = toDatetimeLocal(`${datePrefix} ${endClock}`);
    }
    return { start, end };
  }
  const matches = [...text.matchAll(/(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?\s*\d{1,2}[：:点]\d{0,2})/g)].map((item) => item[1]);
  if (matches.length >= 2) return { start: toDatetimeLocal(matches[0]), end: toDatetimeLocal(matches[1]) };
  return { start: toDatetimeLocal(text), end: "" };
}

export function parsedActionFromStatus(status: string): string {
  if (status === "更新") return "update";
  if (status === "结束") return "end";
  return "start";
}

export function parsedActionLabel(action: string): string {
  if (action === "update") return "更新";
  if (action === "end") return "结束";
  return "开始";
}

export function pastedNoticeWorkType(text: string, sections: NoticeSections = {}): string {
  const titleText = [
    sectionValue(sections, ["名称", "标题", "通告名称", "维修名称"]),
    sectionValue(sections, ["内容"]),
  ].filter(Boolean).join("\n");
  const raw = String(text || "");
  const headText = `${raw.match(/^【[^】]+】/)?.[0] || ""}\n${titleText}`;
  const rawHead = raw.split(/\n/).slice(0, 5).join("\n");
  if (/设备检修|检修通告/.test(headText)) return "repair";
  if (/上电通告|上下电通告|下电通告/.test(headText)) return "power";
  if (/设备轮巡|轮巡通告/.test(headText)) return "polling";
  if (/设备调整|调整通告/.test(headText)) return "adjust";
  if (/设备变更|变更通告/.test(headText)) return "change";
  if (/设备检修|检修通告/.test(rawHead)) return "repair";
  if (/上电通告|上下电通告|下电通告/.test(rawHead)) return "power";
  if (/设备轮巡|轮巡通告/.test(rawHead)) return "polling";
  if (/设备调整|调整通告/.test(rawHead)) return "adjust";
  if (/设备变更|变更通告/.test(rawHead)) return "change";
  return "maintenance";
}
