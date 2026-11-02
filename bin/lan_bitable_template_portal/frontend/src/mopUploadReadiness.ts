import { mopPersonHasUsableSignature } from "./mopSignatureUtils";
import type { LooseDict } from "./types";

export type MopStatusItem = {
  key: string;
  label: string;
  text: string;
  done: boolean;
};

export type MopUploadFooterItem = {
  key: string;
  label: string;
  text: string;
  ready: boolean;
};

export function signedPeopleCount(people: LooseDict[]): number {
  return people.filter((person) => mopPersonHasUsableSignature(person)).length;
}

export function unsignedPeopleCount(people: LooseDict[]): number {
  return people.filter((person) => !mopPersonHasUsableSignature(person)).length;
}

export function selectedSignaturesReady(people: LooseDict[]): boolean {
  return people.length > 0 && people.every((person) => mopPersonHasUsableSignature(person));
}

export function signatureDisplayCount(input: {
  signedCount: number;
  requiredCount: number;
}): number {
  return input.requiredCount > 0
    ? Math.min(input.signedCount, input.requiredCount)
    : input.signedCount;
}

export function fillMopDisabledReason(input: {
  hasLocalFile: boolean;
  hasActiveSheet: boolean;
}): string {
  if (!input.hasLocalFile) return "请先打开 MOP 表格";
  if (!input.hasActiveSheet) return "请先选择需要填写的 Sheet";
  return "";
}

export function uploadSignedMopDisabledReason(input: {
  hasLocalFile: boolean;
  hasActiveSheet: boolean;
  hasSelectedNotice: boolean;
  hasSourceRecordId: boolean;
  hasImplementerSignature: boolean;
  hasAuditorSignature: boolean;
}): string {
  if (!input.hasLocalFile) return "请先打开 MOP 表格";
  if (!input.hasActiveSheet) return "请先选择包含签名位置的维护单填写页，封面为只读页";
  if (!input.hasSelectedNotice) return "请先选择左侧通告";
  if (!input.hasSourceRecordId) return "当前通告未关联维保计划事项，无法确定维护单的归档位置。请在通告办理中绑定对应维保计划，再刷新本页上传；若已经绑定，请重新读取通告。";
  if (!input.hasImplementerSignature) return "请至少选择一个维护实施人签名";
  if (!input.hasAuditorSignature) return "请至少选择一个维护审核人签名";
  return "";
}

export function canUploadSignedMop(input: {
  hasLocalFile: boolean;
  hasActiveSheet: boolean;
  hasSelectedNotice: boolean;
  hasSourceRecordId: boolean;
  hasImplementerSignature: boolean;
  hasAuditorSignature: boolean;
}): boolean {
  return Boolean(
    input.hasLocalFile
    && input.hasActiveSheet
    && input.hasSelectedNotice
    && input.hasSourceRecordId
    && input.hasImplementerSignature
    && input.hasAuditorSignature
  );
}

export function buildMopCompletionItems(input: {
  hasLocalFile: boolean;
  implementerCount: number;
  auditorCount: number;
  fillTotal: number;
  filledCount: number;
}): MopStatusItem[] {
  return [
    {
      key: "file",
      label: "表格文件",
      text: input.hasLocalFile ? "已打开" : "未打开",
      done: input.hasLocalFile,
    },
    {
      key: "implementer",
      label: "实施人签名",
      text: input.implementerCount ? `已选 ${input.implementerCount} 人` : "未选",
      done: input.implementerCount > 0,
    },
    {
      key: "auditor",
      label: "审核人签名",
      text: input.auditorCount ? `已选 ${input.auditorCount} 人` : "未选",
      done: input.auditorCount > 0,
    },
    {
      key: "fields",
      label: "表格选填",
      text: input.fillTotal ? `${input.filledCount}/${input.fillTotal}` : "无待填项",
      done: true,
    },
  ];
}

export function buildMopRequirementItems(input: {
  hasNotice: boolean;
  hasOpenedFile: boolean;
  hasImplementerSignature: boolean;
  hasAuditorSignature: boolean;
  uploadDisabledReason: string;
  canUpload: boolean;
}): MopStatusItem[] {
  return [
    {
      key: "notice",
      label: "维保通告",
      text: "请选择左侧通告",
      done: input.hasNotice,
    },
    {
      key: "file",
      label: "MOP文件",
      text: "请先打开表格",
      done: input.hasOpenedFile,
    },
    {
      key: "implementer",
      label: "实施人签名",
      text: "至少 1 个可用签名",
      done: input.hasImplementerSignature,
    },
    {
      key: "auditor",
      label: "审核人签名",
      text: "至少 1 个可用签名",
      done: input.hasAuditorSignature,
    },
    {
      key: "upload",
      label: "维保单写入",
      text: input.uploadDisabledReason || "可上传",
      done: input.canUpload,
    },
  ];
}

export function buildMopUploadFooterItems(input: {
  implementerText: string;
  implementerReady: boolean;
  auditorText: string;
  auditorReady: boolean;
}): MopUploadFooterItem[] {
  return [
    {
      key: "implementer",
      label: "实施人",
      text: input.implementerText,
      ready: input.implementerReady,
    },
    {
      key: "auditor",
      label: "审核人",
      text: input.auditorText,
      ready: input.auditorReady,
    },
  ];
}
