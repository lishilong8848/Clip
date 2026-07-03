import { mopPersonHasUsableSignature } from "./mopSignatureUtils";
import type { LooseDict } from "./types";

export type MopInvolvedPeopleRequirement = {
  count: number;
  cell_ref: string;
};

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

export function maintenanceTimeValidationMessage(input: {
  startText: string;
  finishText: string;
  auditText: string;
  timeOrderInvalid: boolean;
}): string {
  if (!input.startText) return "请填写维护开始时间";
  if (!input.finishText) return "请填写维护完成时间";
  if (!input.auditText) return "请填写审核确认时间";
  if (input.timeOrderInvalid) return "维护开始时间不能晚于维护完成时间";
  return "";
}

export function requiredImplementerSignatureCount(input: {
  involvedPeopleCount: number;
  selectedImplementerCount: number;
}): number {
  return input.involvedPeopleCount > 0
    ? input.involvedPeopleCount
    : input.selectedImplementerCount;
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
  timeValidationMessage: string;
  selectedPeopleCount: number;
  unsignedPeopleCount: number;
}): string {
  if (!input.hasLocalFile) return "请先打开 MOP 表格";
  if (!input.hasActiveSheet) return "请先选择需要填写的 Sheet";
  if (input.timeValidationMessage) return input.timeValidationMessage;
  if (!input.selectedPeopleCount) return "请至少选择一个签名人员";
  if (input.unsignedPeopleCount > 0) return `还有 ${input.unsignedPeopleCount} 个已选人员未签名`;
  return "";
}

export function uploadSignedMopDisabledReason(input: {
  hasLocalFile: boolean;
  hasActiveSheet: boolean;
  hasSelectedNotice: boolean;
  hasSourceRecordId: boolean;
  timeValidationMessage: string;
  hasImplementerSignature: boolean;
  hasAuditorSignature: boolean;
  unsignedPeopleCount: number;
  involvedPeopleRequirement: MopInvolvedPeopleRequirement;
  signedImplementerCount: number;
}): string {
  if (!input.hasLocalFile) return "请先打开 MOP 表格";
  if (!input.hasActiveSheet) return "请先选择需要填写的 Sheet";
  if (!input.hasSelectedNotice) return "请先选择左侧通告";
  if (!input.hasSourceRecordId) return "当前通告缺少可上传的维保事项，无法上传";
  if (input.timeValidationMessage) return input.timeValidationMessage;
  if (!input.hasImplementerSignature) return "请至少选择一个维护实施人签名";
  if (!input.hasAuditorSignature) return "请至少选择一个维护审核人签名";
  if (input.unsignedPeopleCount > 0) return `还有 ${input.unsignedPeopleCount} 个已选人员未签名`;
  if (
    input.involvedPeopleRequirement.count > 0
    && input.signedImplementerCount < input.involvedPeopleRequirement.count
  ) {
    return `${input.involvedPeopleRequirement.cell_ref || "涉及人数"}为 ${input.involvedPeopleRequirement.count} 人，维护实施人至少需要 ${input.involvedPeopleRequirement.count} 个已签名人员`;
  }
  return "";
}

export function canUploadSignedMop(input: {
  hasLocalFile: boolean;
  hasActiveSheet: boolean;
  hasSelectedNotice: boolean;
  hasSourceRecordId: boolean;
  hasImplementerSignature: boolean;
  hasAuditorSignature: boolean;
  allSelectedSignaturesReady: boolean;
  timeValidationMessage: string;
  involvedPeopleRequirement: MopInvolvedPeopleRequirement;
  signedImplementerCount: number;
}): boolean {
  return Boolean(
    input.hasLocalFile
    && input.hasActiveSheet
    && input.hasSelectedNotice
    && input.hasSourceRecordId
    && input.hasImplementerSignature
    && input.hasAuditorSignature
    && input.allSelectedSignaturesReady
    && !input.timeValidationMessage
    && (
      input.involvedPeopleRequirement.count <= 0
      || input.signedImplementerCount >= input.involvedPeopleRequirement.count
    )
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
      label: "表格填写",
      text: input.fillTotal ? `${input.filledCount}/${input.fillTotal}` : "无待填项",
      done: input.fillTotal === 0 || input.filledCount >= input.fillTotal,
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
  startText: string;
  finishText: string;
  auditText: string;
  startReady: boolean;
  finishReady: boolean;
  auditReady: boolean;
  implementerText: string;
  implementerReady: boolean;
  auditorText: string;
  auditorReady: boolean;
}): MopUploadFooterItem[] {
  return [
    {
      key: "start_time",
      label: "开始时间",
      text: input.startText || "未填",
      ready: input.startReady,
    },
    {
      key: "finish_time",
      label: "完成时间",
      text: input.finishText || "未填",
      ready: input.finishReady,
    },
    {
      key: "audit_time",
      label: "审核时间",
      text: input.auditText || "未填",
      ready: input.auditReady,
    },
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
