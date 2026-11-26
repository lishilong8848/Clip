const preStartCodes = new Set(['startup_listener', 'startup_handshake', 'startup_catalog', 'startup_cooldown']);

export function startupFailure(turn: Record<string, any>): boolean {
  return turn.status === 'failed' && (
    preStartCodes.has(turn.error_code) || ['runtime_startup', 'service_startup_timeout'].includes(turn.error_code)
    || /^助手启动超时，原消息已保留，可继续。/.test(turn.error || '')
  );
}

export function recoveryAction(turn: Record<string, any>, activeRun: string): string {
  if (turn.plan || turn.task_id || turn.job_id) return '查看原任务';
  if (turn.client_pending || turn.status === 'pending' || (activeRun && activeRun === turn.run_id)) return '查看原进度';
  if (!startupFailure(turn)) return '';
  return preStartCodes.has(turn.error_code) && turn.operation_id && turn.attempt_id && turn.run_id
    ? '重试原问题' : '查看原进度';
}

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
