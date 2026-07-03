import type { LooseDict } from "./types";

type MopSignatureRole = "implementer" | "auditor";

function roleLabel(role: MopSignatureRole): string {
  return role === "auditor" ? "维护审核人" : "维护实施人";
}

export function mopPersonHasUsableSignature(person: LooseDict | null | undefined): boolean {
  return Boolean(person?.has_signature && String(person?.signature_preview_url || "").trim());
}

export function temporarySignatureDisplayNumber(name: string): number {
  const match = /^临时人员(\d+)$/.exec(String(name || "").trim());
  return match ? Number(match[1] || 0) : Number.MAX_SAFE_INTEGER;
}

export function otherSignaturePersonPriority(person: LooseDict): number {
  const source = String(person?.source || "");
  const status = String(person?.status || "");
  if (!mopPersonHasUsableSignature(person)) {
    if (status === "failed") return 0;
    if (status === "draft") return 1;
    if (status === "sending") return 2;
    return 3;
  }
  if (source === "temporary" || String(person?.temp_id || "")) return 6;
  if (source === "external") return 7;
  return 8;
}

export function otherSignatureDraftPriority(draft: LooseDict): number {
  const status = String(draft?.status || "draft");
  if (status === "failed") return 0;
  if (status === "draft") return 1;
  if (status === "sending") return 2;
  if (status === "sent") return 3;
  return 4;
}

export function otherSignaturePersonStatusText(person: LooseDict): string {
  if (String(person?.source || "") === "external") return "外部已保存签名";
  if (mopPersonHasUsableSignature(person)) return "临时人员 · 已签名";
  const status = String(person?.status || "");
  if (status === "sending") return "临时人员 · 发送中";
  if (status === "failed") return "临时人员 · 发送失败";
  if (status === "sent" || status === "pending") return "临时人员 · 待签名";
  return "临时人员 · 待发送";
}

export function otherSignatureDraftStatusText(draft: LooseDict): string {
  const status = String(draft.status || "draft");
  if (status === "sending") return "发送中";
  if (status === "sent") return "待签名";
  if (status === "failed") return "发送失败";
  return "待发送";
}

export function signaturePersonKey(person: LooseDict): string {
  const tempId = String(person?.temp_id || "").trim();
  if (String(person?.source || "") === "temporary" || tempId) {
    return `temp:${tempId}`;
  }
  if (String(person?.source || "") === "external") {
    return `external:${String(person?.record_id || "").trim()}`;
  }
  return String(person?.record_id || "").trim();
}

export function signaturePersonDisplayName(person: LooseDict): string {
  const name = String(person?.name || person?.display_name || "未命名").trim() || "未命名";
  const source = String(person?.source || "");
  const status = String(person?.status || "");
  if (source === "temporary") {
    return `${name} · ${mopPersonHasUsableSignature(person) || status === "signed" ? "已签名" : "待签名"}`;
  }
  if (source === "external") {
    return `${name} · 已保存`;
  }
  return name;
}

export function temporarySignatureDisplayName(person: LooseDict, index = 0): string {
  const explicitName = String(person?.name || person?.display_name || "").trim();
  if (explicitName) return explicitName;
  const fallbackIndex = Math.max(1, Number(index || 0) + 1);
  return `临时人员${fallbackIndex}`;
}

export function buildMopSignaturePayload(input: {
  formalImplementers: LooseDict[];
  formalAuditors: LooseDict[];
  otherImplementers: LooseDict[];
  otherAuditors: LooseDict[];
}): LooseDict[] {
  const staffPayload = (role: MopSignatureRole, people: LooseDict[]) => people
    .filter((person) => mopPersonHasUsableSignature(person))
    .map((person) => ({
      source: "staff",
      role,
      label: roleLabel(role),
      record_id: person.record_id,
    }));

  const otherPayload = (role: MopSignatureRole, people: LooseDict[]) => people
    .filter((person) => mopPersonHasUsableSignature(person))
    .map((person) => {
      const source = String(person.source || "") === "external" ? "external" : "temporary";
      return {
        source,
        role,
        label: roleLabel(role),
        temp_id: source === "temporary" ? person.temp_id : "",
        record_id: person.record_id || "",
      };
    });

  return [
    ...staffPayload("implementer", input.formalImplementers),
    ...staffPayload("auditor", input.formalAuditors),
    ...otherPayload("implementer", input.otherImplementers),
    ...otherPayload("auditor", input.otherAuditors),
  ];
}
