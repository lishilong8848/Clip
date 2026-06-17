type Dict = Record<string, any>;

export function terminalPhase(phase: string): boolean {
  return ["success", "failed"].includes(String(phase || ""));
}

export function friendlyFailureText(error: unknown, fallback = "失败"): string {
  const source = error as { message?: unknown; error?: unknown } | null | undefined;
  const text = String(source?.message || source?.error || error || fallback || "").trim();
  if (!text) return fallback;
  if (/缺少|必填|请选择|不能为空/.test(text)) return `缺字段：${text}`;
  if (/无权|权限|未登录|登录/.test(text)) return `无权限：${text}`;
  if (/飞书|openid|机器人|群消息|消息/.test(text)) return `飞书失败：${text}`;
  if (/多维|bitable|record|Record|1254|字段|表格|记录不存在/.test(text)) return `多维失败：${text}`;
  if (/关联|绑定|找不到|未找到/.test(text)) return `记录绑定失败：${text}`;
  if (/超时|timeout|网络|HTTP|SSL/.test(text)) return `网络异常：${text}`;
  return text;
}

export function backendJobStatusPatch(job: Dict): { phase: string; status: string; text: string } {
  const phase = String(job.phase || job.status || "");
  const text = String(job.message_warning || job.message || job.upload_message || phase || "处理中");
  return {
    phase,
    status: phase === "success" ? "success" : phase === "failed" ? "failed" : "busy",
    text: phase === "success" ? text || "成功" : phase === "failed" ? friendlyFailureText(job.error || text, "失败") : text,
  };
}
