import type { Dict } from "./api/client";

export function normalizeMopScope(value: string | null | undefined, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

export function compactSearchText(value: unknown): string {
  return String(value || "").replace(/\s+/g, "").toLowerCase();
}

export function mopAttachmentKey(attachment: Dict): string {
  return String(attachment?.file_token || attachment?.url || attachment?.name || "").trim();
}

export function noticeMopUploaded(notice: Dict): boolean {
  return Boolean(notice?.mop_uploaded || Number(notice?.mop_attachment_count || 0) > 0);
}

export function mopNoticeNeedsAction(notice: Dict): boolean {
  return !notice?.mop_binding || !noticeMopUploaded(notice);
}

export function noticeIsEnded(notice: Dict): boolean {
  const status = String(notice?.status || "").trim();
  if (!status || /未(结束|完成|闭环)/.test(status)) return false;
  return /(已结束|正常结束|延迟结束|延期结束|维修完成|已完成|闭环)/.test(status);
}

export function sortRecommendedMopFirst(items: Dict[], recommendedId: string): Dict[] {
  if (!recommendedId || items.length < 2) return items;
  return [...items].sort((left, right) => {
    const leftRecommended = String(left?.record_id || "") === recommendedId;
    const rightRecommended = String(right?.record_id || "") === recommendedId;
    if (leftRecommended === rightRecommended) return 0;
    return leftRecommended ? -1 : 1;
  });
}

export function sortMopNoticesForAction(
  items: Dict[],
  options: {
    needsAction: (notice: Dict) => boolean;
    isEnded: (notice: Dict) => boolean;
  },
): Dict[] {
  const score = (notice: Dict): number => {
    let value = 0;
    if (options.needsAction(notice)) value += 80;
    if (options.isEnded(notice)) value += 30;
    if (!notice?.mop_binding) value += 20;
    if (!notice?.mop_uploaded && Number(notice?.mop_attachment_count || 0) <= 0) value += 20;
    return value;
  };
  return [...items].sort((left, right) => {
    const scoreDiff = score(right) - score(left);
    if (scoreDiff) return scoreDiff;
    const leftTime = String(left?.end_time || left?.start_time || left?.updated_at || "").trim();
    const rightTime = String(right?.end_time || right?.start_time || right?.updated_at || "").trim();
    if (leftTime !== rightTime) return rightTime.localeCompare(leftTime);
    return String(left?.title || "").localeCompare(String(right?.title || ""), "zh-Hans-CN");
  });
}
