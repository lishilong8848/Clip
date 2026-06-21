import { requestJson, type Dict } from "./api/client";

export type SignaturePeopleQuery = {
  scope: string;
  q?: string;
  recordId?: string;
  refresh?: boolean;
  limit?: number;
};

function appendCommonSignatureQuery(url: URLSearchParams, query: SignaturePeopleQuery): void {
  url.set("scope", query.scope);
  if (query.q?.trim()) url.set("q", query.q.trim());
  if (query.recordId?.trim()) url.set("record_id", query.recordId.trim());
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

export function saveTemporarySignature(temporaryId: string, signaturePng: string): Promise<Dict> {
  return requestJson("/api/signatures/temporary/save", {
    method: "POST",
    body: JSON.stringify({
      temporary_id: temporaryId,
      signature_png: signaturePng,
    }),
  });
}

export function saveExternalSignature(recordId: string, signerName: string, signaturePng: string): Promise<Dict> {
  return requestJson("/api/signatures/external/save", {
    method: "POST",
    body: JSON.stringify({
      record_id: recordId,
      signer_name: signerName,
      signature_png: signaturePng,
    }),
  });
}

export function saveStaffSignature(recordId: string, signerName: string, signaturePng: string): Promise<Dict> {
  return requestJson("/api/signatures/save", {
    method: "POST",
    body: JSON.stringify({
      record_id: recordId,
      signer_name: signerName,
      signature_png: signaturePng,
    }),
  });
}

export function sendStaffSignatureLink(recordId: string, signerName: string, scope: string): Promise<Dict> {
  return requestJson("/api/signatures/send-link", {
    method: "POST",
    body: JSON.stringify({
      record_id: recordId,
      signer_name: signerName,
      scope,
    }),
  });
}

export function createTemporarySignatureSession(payload: {
  scope: string;
  noticeKey: string;
  noticeTitle: string;
  specialty: string;
  role: string;
  displayName: string;
}): Promise<Dict> {
  return requestJson("/api/signatures/temporary/create", {
    method: "POST",
    body: JSON.stringify({
      scope: payload.scope,
      notice_key: payload.noticeKey,
      notice_title: payload.noticeTitle,
      specialty: payload.specialty,
      role: payload.role,
      display_name: payload.displayName,
    }),
  });
}

export function sendTemporarySignatureLink(payload: {
  temporaryId?: string;
  scope: string;
  noticeKey?: string;
  noticeTitle?: string;
  specialty?: string;
  role?: string;
  displayName?: string;
  recipientOpenIds?: string[];
}): Promise<Dict> {
  const body: Dict = {
    scope: payload.scope,
  };
  if (payload.temporaryId) body.temporary_id = payload.temporaryId;
  if (payload.noticeKey !== undefined) body.notice_key = payload.noticeKey;
  if (payload.noticeTitle !== undefined) body.notice_title = payload.noticeTitle;
  if (payload.specialty !== undefined) body.specialty = payload.specialty;
  if (payload.role !== undefined) body.role = payload.role;
  if (payload.displayName !== undefined) body.display_name = payload.displayName;
  if (payload.recipientOpenIds) body.recipient_open_ids = payload.recipientOpenIds;
  return requestJson("/api/signatures/temporary/send-link", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function fetchTemporarySignatures(scope: string, noticeKey: string): Promise<Dict> {
  const params = new URLSearchParams({
    scope,
    notice_key: noticeKey,
  });
  return requestJson(`/api/signatures/temporary/list?${params.toString()}`);
}
