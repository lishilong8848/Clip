import { ref, type Ref, shallowRef } from "vue";
import {
  mopPersonHasUsableSignature,
  signaturePersonKey,
} from "./mopSignatureUtils";
import type { LooseDict } from "./types";

type MopSignatureRole = "implementer" | "auditor";

function isOtherSignaturePerson(item: LooseDict): boolean {
  const source = String(item?.source || "").trim();
  if (source === "temporary" || source === "external") return true;
  return Boolean(String(item?.temp_id || item?.temporary_id || "").trim());
}

export function useMopSignatureSelection(
  signaturePeople: Ref<LooseDict[]>,
  options: { onChanged?: () => void } = {},
) {
  const signaturePeopleById = ref<Record<string, LooseDict>>({});
  const temporarySignatures = shallowRef<LooseDict[]>([]);
  const otherSignatureDrafts = ref<LooseDict[]>([]);
  const hiddenOtherSignatureKeys = ref<string[]>([]);
  const signatureSelectedRecords = ref<Record<string, string[]>>({ implementer: [], auditor: [] });

  function emitChanged(): void {
    options.onChanged?.();
  }

  function rememberSignaturePeople(items: LooseDict[]): void {
    if (!items.length) return;
    const next = { ...signaturePeopleById.value };
    for (const item of items) {
      const recordId = String(item?.record_id || "");
      if (!recordId) continue;
      next[recordId] = { ...(next[recordId] || {}), ...item };
    }
    signaturePeopleById.value = next;
  }

  function updateRememberedSignaturePerson(recordId: string, patch: LooseDict): void {
    const recordText = String(recordId || "");
    if (!recordText) return;
    const next = {
      ...signaturePeopleById.value,
      [recordText]: { ...(signaturePeopleById.value[recordText] || {}), ...patch },
    };
    signaturePeopleById.value = next;
    signaturePeople.value = signaturePeople.value.map((item) => (
      item.record_id === recordText ? { ...item, ...patch } : item
    ));
    emitChanged();
  }

  function markSignatureUnavailable(recordId: unknown): void {
    const id = String(recordId || "").trim();
    if (!id) return;
    updateRememberedSignaturePerson(id, {
      has_signature: false,
      signature_count: 0,
      signature_preview_url: "",
      signature_version: "",
    });
  }

  function markOtherSignatureUnavailable(person: LooseDict): void {
    const personKey = signaturePersonKey(person);
    temporarySignatures.value = temporarySignatures.value.map((item) => (
      signaturePersonKey(item) === personKey
        ? { ...item, has_signature: false, signature_preview_url: "", status: "pending" }
        : item
    ));
  }

  function selectSignaturePerson(role: MopSignatureRole, recordId: string): void {
    const recordText = String(recordId || "");
    if (!recordText) return;
    const person = signaturePeople.value.find((item) => item.record_id === recordText)
      || signaturePeopleById.value[recordText];
    if (person) rememberSignaturePeople([person]);
    const existing = signatureSelectedRecords.value[role] || [];
    const next = existing.includes(recordText)
      ? [...existing.filter((item) => item !== recordText), recordText]
      : [...existing, recordText];
    signatureSelectedRecords.value = {
      ...signatureSelectedRecords.value,
      [role]: [...new Set(next)],
    };
    emitChanged();
  }

  function removeSignaturePerson(role: MopSignatureRole, recordId: string): void {
    const recordText = String(recordId || "");
    if (recordText.startsWith("temp:") || recordText.startsWith("external:")) {
      hiddenOtherSignatureKeys.value = [...new Set([...hiddenOtherSignatureKeys.value, recordText])];
      emitChanged();
      return;
    }
    signatureSelectedRecords.value = {
      ...signatureSelectedRecords.value,
      [role]: (signatureSelectedRecords.value[role] || []).filter((item) => item !== recordText),
    };
    emitChanged();
  }

  function unhideSignatureKey(key: unknown): void {
    const keyText = String(key || "").trim();
    if (!keyText) return;
    hiddenOtherSignatureKeys.value = hiddenOtherSignatureKeys.value.filter((item) => item !== keyText);
  }

  function unhideSignaturePerson(person: LooseDict): void {
    unhideSignatureKey(signaturePersonKey(person));
  }

  function selectedFormalSignaturePeople(role: MopSignatureRole): LooseDict[] {
    const ids = signatureSelectedRecords.value[role] || [];
    return ids
      .map((id) => signaturePeopleById.value[id] || signaturePeople.value.find((item) => item.record_id === id))
      .filter((item): item is LooseDict => Boolean(item));
  }

  function selectedTemporarySignaturePeople(role: MopSignatureRole): LooseDict[] {
    const hidden = new Set(hiddenOtherSignatureKeys.value);
    return temporarySignatures.value
      .filter((item) => String(item.role || "") === role)
      .filter((item) => isOtherSignaturePerson(item))
      .filter((item) => String(item.status || "") !== "failed")
      .filter((item) => !hidden.has(signaturePersonKey(item)));
  }

  function selectedSignaturePeople(role: MopSignatureRole): LooseDict[] {
    return [
      ...selectedFormalSignaturePeople(role),
      ...selectedTemporarySignaturePeople(role),
    ];
  }

  function selectedSignatureUnsignedCount(role: MopSignatureRole): number {
    return selectedSignaturePeople(role).filter((person) => !mopPersonHasUsableSignature(person)).length;
  }

  function selectedFormalSignatureUnsignedCount(role: MopSignatureRole): number {
    return selectedFormalSignaturePeople(role).filter((person) => !mopPersonHasUsableSignature(person)).length;
  }

  function selectedTemporarySignatureUnsignedCount(role: MopSignatureRole): number {
    return selectedTemporarySignaturePeople(role).filter((person) => !mopPersonHasUsableSignature(person)).length;
  }

  function nextOtherSignatureDisplayName(role: MopSignatureRole): string {
    const usedNumbers = [
      ...selectedTemporarySignaturePeople(role).map((item) => String(item.name || item.display_name || "")),
      ...otherSignatureDrafts.value
        .filter((item) => String(item.role || "") === role)
        .map((item) => String(item.display_name || "")),
    ]
      .map((name) => /^临时人员(\d+)$/.exec(name.trim())?.[1] || "")
      .map((value) => Number(value || 0))
      .filter((value) => Number.isFinite(value) && value > 0);
    return `临时人员${(usedNumbers.length ? Math.max(...usedNumbers) : 0) + 1}`;
  }

  function ensureOtherSignatureDraftName(draft: LooseDict, fallbackRole: MopSignatureRole): void {
    const draftId = String(draft?.draft_id || "");
    if (!draftId || String(draft.display_name || "").trim()) return;
    const role = String(draft.role || fallbackRole) === "auditor" ? "auditor" : "implementer";
    const displayName = nextOtherSignatureDisplayName(role);
    otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
      String(item.draft_id || "") === draftId ? { ...item, display_name: displayName } : item
    ));
  }

  function updateOtherSignatureDraftName(draftId: string, value: string): void {
    const idText = String(draftId || "");
    if (!idText) return;
    otherSignatureDrafts.value = otherSignatureDrafts.value.map((item) => (
      String(item.draft_id || "") === idText ? { ...item, display_name: value } : item
    ));
  }

  function resetSignatureSelection(): void {
    signatureSelectedRecords.value = { implementer: [], auditor: [] };
    temporarySignatures.value = [];
    otherSignatureDrafts.value = [];
    hiddenOtherSignatureKeys.value = [];
  }

  function resetOtherSignatures(): void {
    temporarySignatures.value = [];
    otherSignatureDrafts.value = [];
    hiddenOtherSignatureKeys.value = [];
  }

  return {
    signaturePeopleById,
    temporarySignatures,
    otherSignatureDrafts,
    hiddenOtherSignatureKeys,
    signatureSelectedRecords,
    rememberSignaturePeople,
    updateRememberedSignaturePerson,
    markSignatureUnavailable,
    markOtherSignatureUnavailable,
    selectSignaturePerson,
    removeSignaturePerson,
    unhideSignatureKey,
    unhideSignaturePerson,
    selectedFormalSignaturePeople,
    selectedTemporarySignaturePeople,
    selectedSignaturePeople,
    selectedSignatureUnsignedCount,
    selectedFormalSignatureUnsignedCount,
    selectedTemporarySignatureUnsignedCount,
    nextOtherSignatureDisplayName,
    ensureOtherSignatureDraftName,
    updateOtherSignatureDraftName,
    resetOtherSignatures,
    resetSignatureSelection,
  };
}
