/** Display observed server milestones, never generated reasoning or draft text. */
export function assistantProgressText(turn: Record<string, any>): string {
  const timing = turn.runtime || {};
  const labels: Record<string, string> = {
    queued: '正在等待处理', context: '正在整理上下文', model: '正在生成回答',
    tool: '正在查询资料', checking: '正在核对结果', finalizing: '正在整理答复',
  };
  const parts = [labels[timing.phase] || '正在处理'];
  const count = timing.task?.source_count;
  if (Number.isInteger(count) && count > 0 && count <= 1000000) parts.push(`已读取 ${count} 个来源`);
  const elapsed = timing.elapsed_ms;
  if (Number.isFinite(elapsed) && elapsed >= 15000 && elapsed <= 86400000)
    parts.push(`已用时 ${Math.floor(elapsed / 15000) * 15} 秒`);
  return parts.join(' · ');
}
