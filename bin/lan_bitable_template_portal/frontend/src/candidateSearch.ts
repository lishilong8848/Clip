export type CandidateRecord = Record<string, any>;

export function candidateSearchBlob(item: CandidateRecord): string {
  const parts = [
    item?.title,
    item?.building,
    item?.status,
    item?.start_time,
    item?.end_time,
    item?.match_reason,
  ];
  const fieldItems = Array.isArray(item?.field_items) ? item.field_items : [];
  for (const field of fieldItems.slice(0, 30)) {
    if (!field || typeof field !== "object") continue;
    parts.push(field.label, field.value);
  }
  const fields = item?.fields && typeof item.fields === "object" ? item.fields : {};
  for (const [label, value] of Object.entries(fields).slice(0, 30)) {
    parts.push(label, value);
  }
  return parts.map((part) => String(part || "").toLowerCase()).join("\n");
}

export function filterCandidatesBySearch<T extends CandidateRecord>(
  items: T[],
  queryText: string,
): T[] {
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return items;
  return items.filter((item) => candidateSearchBlob(item).includes(query));
}
