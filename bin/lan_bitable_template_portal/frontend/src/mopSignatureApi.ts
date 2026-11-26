import { requestJson, requestPeople, type Dict } from "./api/client";

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
  allPeople?: boolean;
};

function appendCommonSignatureQuery(url: URLSearchParams, query: SignaturePeopleQuery): void {
  url.set("scope", query.scope);
  if (query.q?.trim()) url.set("q", query.q.trim());
  if (query.recordId?.trim()) url.set("record_id", query.recordId.trim());
  if (query.noticeKey?.trim()) url.set("notice_key", query.noticeKey.trim());
  if (query.refresh) url.set("refresh", "1");
  if (query.allPeople) url.set("all_people", "1");
  url.set("limit", String(query.limit || 60));
}

export function fetchSignaturePeople(query: SignaturePeopleQuery): Promise<Dict> {
  const params = new URLSearchParams();
  appendCommonSignatureQuery(params, query);
  const read = query.recordId || query.noticeKey ? requestJson : requestPeople;
  return read(`/api/signatures/people?${params.toString()}`, { fresh: query.refresh });
}

export function fetchExternalSignaturePeople(query: SignaturePeopleQuery): Promise<Dict> {
  const params = new URLSearchParams();
  appendCommonSignatureQuery(params, query);
  const read = query.recordId || query.noticeKey ? requestJson : requestPeople;
  return read(`/api/signatures/temporary/people?${params.toString()}`, { fresh: query.refresh });
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

export function signatureUsageDeliveryPending(data: Dict): boolean {
  return data.status === "queued" || data.status === "running";
}

export function signatureUsageDeliveryMessage(data: Dict): string {
  const sent = Number(data.sent_count || 0);
  if (signatureUsageDeliveryPending(data)) {
    return sent ? `后台正在发送，已送达 ${sent} 人。再次点击可查看原请求。`
      : "请求已受理，正在后台发送。再次点击可查看原请求。";
  }
  const failed = Number(data.failed_count || 0);
  const uncertain = Number(data.uncertain_count || 0);
  const skipped = Number(data.skipped?.length || 0);
  const count = [`已发送 ${sent} 人`, failed && `${failed} 人发送失败`,
    uncertain && `${uncertain} 人结果待核对`, skipped && `${skipped} 人按规则跳过`]
    .filter(Boolean).join("，");
  return `${count}。${data.error || ""}`;
}

export async function readSignatureUsageDelivery(initial: Dict, signal?: AbortSignal): Promise<Dict> {
  if (!initial.delivery_id) return initial;
  let current = initial;
  const deadline = Date.now() + 12_000;
  while (signatureUsageDeliveryPending(current) && !signal?.aborted && Date.now() < deadline) {
    await new Promise(resolve => window.setTimeout(resolve, 1200));
    try {
      current = await requestJson(`/api/signatures/usage-confirmations/${encodeURIComponent(initial.delivery_id)}`, {
        cache: "no-store", timeoutMs: 4500, signal,
      });
    } catch {
      // The original request was accepted. A status lookup error is not a delivery failure.
      break;
    }
  }
  return current;
}

export async function readManagedSignatureDelivery(
  initial: Dict, onPending?: () => void, signal?: AbortSignal,
): Promise<Dict> {
  if (!initial.delivery_id) return initial;
  if (signatureUsageDeliveryPending(initial)) onPending?.();
  const delivery = await readSignatureUsageDelivery(initial, signal);
  return delivery.status === "completed" && delivery.result ? delivery.result
    : signatureUsageDeliveryPending(delivery)
      ? { delivery_id: initial.delivery_id, status: "sending", error: "请求已受理，后台发送中，可核对原请求。" }
      : delivery;
}

export function fetchTemporarySignatures(scope: string, noticeKey: string): Promise<Dict> {
  const params = new URLSearchParams({
    scope,
    notice_key: noticeKey,
  });
  return requestJson(`/api/signatures/temporary/list?${params.toString()}`);
}
