import { requestJson, type Dict } from "./api/client";

export function refreshSignatureDirectory(): Promise<Dict> {
  return requestJson("/api/signatures/management/refresh", { method: "POST", body: "{}" });
}

export function refreshedSignaturePerson(person: Dict, snapshot: Dict): Dict {
  const source = person.source === "external" || person.source === "temporary" ? "external" : "staff";
  if (!person.record_id || !snapshot.sources?.[source]?.ok) return person;
  const effective = snapshot.resolved?.[source + ":" + person.record_id];
  if (!effective) return { ...person, has_signature: false, signature_reason: "人员记录已不存在，请核对。" };
  return { ...person, ...effective, role: person.role,
    ...(effective.source !== person.source ? { temp_id: "", usage_confirmed: false, usage_status: "" } : {}),
  };
}

export type SignaturePeopleQuery = {
  scope: string;
  q?: string;
  recordId?: string;
  noticeKey?: string;
  refresh?: boolean;
  limit?: number;
};

function appendCommonSignatureQuery(url: URLSearchParams, query: SignaturePeopleQuery): void {
  url.set("scope", query.scope);
  if (query.q?.trim()) url.set("q", query.q.trim());
  if (query.recordId?.trim()) url.set("record_id", query.recordId.trim());
  if (query.noticeKey?.trim()) url.set("notice_key", query.noticeKey.trim());
  if (query.refresh) url.set("refresh", "1");
  url.set("limit", String(query.limit || 60));
}

export function fetchSignaturePeople(query: SignaturePeopleQuery): Promise<Dict> {
  const params = new URLSearchParams();
  appendCommonSignatureQuery(params, query);
  return requestJson(`/api/signatures/people?${params.toString()}`);
}

export function fetchExternalSignaturePeople(query: SignaturePeopleQuery): Promise<Dict> {
  const params = new URLSearchParams();
  appendCommonSignatureQuery(params, query);
  return requestJson(`/api/signatures/temporary/people?${params.toString()}`);
}

export function sendSignatureUsageConfirmations(payload: {
  scope: string;
  noticeKey: string;
  noticeTitle: string;
  mopAttachmentName?: string;
  contextType?: string;
  signatures: Dict[];
}): Promise<Dict> {
  return requestJson("/api/signatures/usage-confirmations/send", {
    method: "POST",
    body: JSON.stringify({
      scope: payload.scope,
      notice_key: payload.noticeKey,
      notice_title: payload.noticeTitle,
      mop_attachment_name: payload.mopAttachmentName || "",
      context_type: payload.contextType || "mop",
      signatures: payload.signatures,
    }),
  });
}

export function fetchTemporarySignatures(scope: string, noticeKey: string): Promise<Dict> {
  const params = new URLSearchParams({
    scope,
    notice_key: noticeKey,
  });
  return requestJson(`/api/signatures/temporary/list?${params.toString()}`);
}
