const DAY = 86400;
let calendar: Intl.DateTimeFormat | null | undefined;
let savedDate: number | undefined;
let saved: { day: number; days: number } | null = null;

export function lunarMoon(now: number): { day: number; phase: number } | null {
  if (!Number.isFinite(now)) return null;
  const date = Math.floor((now + 8 * 3600) / DAY) * DAY - 8 * 3600;
  if (savedDate !== date) {
    savedDate = date; saved = null;
    try {
      if (calendar === undefined) {
        calendar = new Intl.DateTimeFormat('en-u-ca-chinese', { timeZone: 'Asia/Shanghai', day: 'numeric' });
        if (calendar.resolvedOptions().calendar !== 'chinese') calendar = null;
      }
      if (calendar) {
        const dayAt = (stamp: number) => Number(calendar!.formatToParts(new Date(stamp * 1000)).find(part => part.type === 'day')?.value);
        const day = dayAt(date);
        if (Number.isInteger(day) && day >= 1 && day <= 30) {
          const last = dayAt(date + (30 - day) * DAY);
          if (last === 30 || last === 1) saved = { day, days: last === 30 ? 30 : 29 };
        }
      }
    } catch { /* Keep an unavailable phase unknown instead of using the Gregorian day. */ }
  }
  // A calendar-based appearance, smoothly advancing within each lunar day.
  return saved ? { day: saved.day, phase: (saved.day - 1 + (now - date) / DAY) / saved.days } : null;
}
