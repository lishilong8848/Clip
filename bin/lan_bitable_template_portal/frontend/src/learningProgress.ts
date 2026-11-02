import type { Dict } from './api/client';

export function hasLearningActivity(row: Dict): boolean {
  return Number(row.summary?.attempt_count ?? row.summary?.answered) > 0 ||
    Number(row.today?.activity_answered) > 0 || Number(row.today?.review_count) > 0 || Number(row.today?.sharing_answered) > 0;
}

/** Learning activity is not equivalent to claiming a daily paper. */
export function learningProgressLabel(progress: Dict | null | undefined): string {
  if (!progress) return '暂无今日学习记录';
  const tasks: Dict[] = progress.tasks || [];
  if (tasks.length) {
    const prefix = progress.date && tasks.every(task => task.date && task.date < progress.date) ? '昨夜夜班 · ' : '';
    const uncertain = tasks.some(task => task.status === '待核对');
    const completed = tasks.every(task => task.status === '已完成');
    const state = uncertain ? '待核对' : completed ? '已完成' : tasks.some(task => task.status === '逾期') ? '逾期' : tasks.some(task => task.status === '答题中') || Number(progress.answered) > 0 ? '答题中' : tasks.every(task => task.status === '未到时') ? '未到时' : '待答';
    return `${prefix}${state} · ${progress.answered} / ${uncertain ? '待核对' : progress.total}`;
  }
  if (progress.practice_total) return `自主练习${progress.practice_answered >= progress.practice_total ? '已完成' : ''} · ${progress.practice_answered} / ${progress.practice_total}`;
  if (progress.sharing_uncertain) return '资料练习待核对';
  if (progress.sharing_total) return `资料练习${progress.sharing_answered >= progress.sharing_total ? '已完成' : ''} · ${progress.sharing_answered} / ${progress.sharing_total}`;
  if (progress.review_count) return `今日复习 ${progress.review_count} 次`;
  if (progress.activity_answered) return `今日已答 ${progress.activity_answered} 题`;
  if (progress.total > 0) return `每日题单 · ${progress.answered} / ${progress.total}`;
  return '暂无今日学习记录';
}

export function learningProgressDetail(progress: Dict | null | undefined): string {
  if (!progress) return '';
  const parts: string[] = [];
  if (progress.tasks?.length && progress.practice_total) parts.push(`自主练习 ${progress.practice_answered} / ${progress.practice_total}`);
  if ((progress.tasks?.length || progress.practice_total) && (progress.sharing_total || progress.sharing_uncertain)) parts.push(progress.sharing_uncertain ? '资料练习待核对' : `资料练习 ${progress.sharing_answered} / ${progress.sharing_total}`);
  if (progress.review_count && (progress.tasks?.length || progress.practice_total || progress.sharing_total)) parts.push(`复习 ${progress.review_count} 次`);
  return parts.join(' · ');
}
