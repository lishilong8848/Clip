export function monthlyArchivePreview(day: number, clock: string, now: number, hasBatch: boolean) {
  if (!Number.isInteger(day) || day < 1 || day > 31 || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(clock) || !Number.isFinite(now)) return null;
  const offset = 8 * 60 * 60 * 1000, local = new Date(now + offset);
  const [hour, minute] = clock.split(':').map(Number);
  const due = (step: number) => {
    const first = new Date(Date.UTC(local.getUTCFullYear(), local.getUTCMonth() + step, 1));
    const last = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 0)).getUTCDate();
    return Date.UTC(first.getUTCFullYear(), first.getUTCMonth(), Math.min(day, last), hour, minute) - offset;
  };
  const current = due(0), format = (value: number) => new Date(value + offset).toISOString().slice(0, 16).replace('T', ' ');
  return { next_run_at: format(!hasBatch && current > now ? current : due(1)), pending_run_at: !hasBatch && current <= now ? format(current) : '' };
}
