import { requestJson } from './api/client';
import { lunarMoon } from './lunarMoon';

export type IconWeather = {
  available: boolean; city: string; description?: string; date?: string; minimum?: number | null; maximum?: number | null;
  sunrise?: number; sunset?: number; fetched_at?: number; server_time?: number; client_clock?: number;
};
export type WeatherScene = { weather: string; phase: string; x: number; y: number; moonX?: number; moonY?: number; rotation?: number; solarCycle?: number; lunarDay?: number; lunarPhase?: number; title: string };

export function weatherClock(value: IconWeather): number {
  return Number.isFinite(value.server_time) && Number.isFinite(value.client_clock)
    ? value.server_time! + performance.now() / 1000 - value.client_clock! : Date.now() / 1000;
}

const cache = new Map<string, { value: IconWeather; expires: number }>();
const pending = new Map<string, Promise<IconWeather>>();
export async function readIconWeather(city: string): Promise<IconWeather> {
  const saved = cache.get(city);
  const clock = saved ? weatherClock(saved.value) : Date.now() / 1000;
  const today = new Date((clock + 8 * 3600) * 1000).toISOString().slice(0, 10);
  if (saved && saved.expires > performance.now() && (!saved.value.available || saved.value.date === today)) return saved.value;
  if (pending.has(city)) return pending.get(city)!;
  const request = requestJson('/api/assistant/icon-weather?city=' + encodeURIComponent(city), { timeoutMs: 14000, fresh: true })
    .then(value => value as IconWeather).catch(() => ({ available: false, city }))
    .then(value => {
      value = { ...value, client_clock: performance.now() / 1000 };
      cache.set(city, { value, expires: performance.now() + (value.available ? 600000 : 60000) });
      if (cache.size > 16) cache.delete(cache.keys().next().value!);
      pending.delete(city); return value;
    });
  pending.set(city, request); return request;
}

export function weatherScene(value: IconWeather, now = Date.now() / 1000): WeatherScene {
  const today = new Date((now + 8 * 3600) * 1000).toISOString().slice(0, 10);
  const fresh = value.available && value.date === today && Number.isFinite(value.fetched_at) && now - value.fetched_at! >= -300 && now - value.fetched_at! <= 1800;
  const dayStart = Math.floor((now + 8 * 3600) / 86400) * 86400 - 8 * 3600;
  const hasSolar = Number.isFinite(value.sunrise) && Number.isFinite(value.sunset) && value.sunrise! < value.sunset!
    && value.sunrise! >= dayStart && value.sunset! < dayStart + 86400;
  // All configured cities are in China; fallback is a clock effect, not a weather claim.
  const rise = hasSolar ? value.sunrise! : dayStart + 6 * 3600, set = hasSolar ? value.sunset! : dayStart + 18 * 3600;
  const phase = now < rise - 1800 || now > set + 1800 ? 'night' : now < rise + 1800 ? 'dawn' : now > set - 1800 ? 'dusk' : 'day';
  const dayProgress = Math.max(0, Math.min(1, (now - rise) / (set - rise)));
  const nightProgress = Math.max(0, Math.min(1, ((now - set + 86400) % 86400) / (rise + 86400 - set)));
  const angle = Math.PI * dayProgress, moonAngle = Math.PI * nightProgress;
  const descriptions = (fresh ? value.description || '' : '').split('转');
  const description = phase === 'night' || phase === 'dusk' ? descriptions[descriptions.length - 1]! : descriptions[0]!;
  const weather = /雷/.test(description) ? 'storm' : /雪/.test(description) ? 'snow' : /雨/.test(description) ? 'rain'
    : /雾|霾|沙尘/.test(description) ? 'fog' : /大风|强风|疾风/.test(description) ? 'wind' : /云|阴/.test(description) ? 'cloudy' : /晴/.test(description) ? 'clear' : 'unknown';
  const label: Record<string, string> = { clear: '晴', cloudy: '多云', fog: '雾', storm: '雷雨', snow: '雪', rain: '雨', wind: '大风', unknown: '天气暂不可用' };
  const lunar = lunarMoon(now);
  return { weather, phase, x: 380 - Math.cos(angle) * 158, y: 258 - Math.sin(angle) * 258,
    lunarDay: lunar?.day, lunarPhase: lunar?.phase,
    solarCycle: now >= rise && now <= set ? dayProgress : 1 + nightProgress,
    moonX: 380 - Math.cos(moonAngle) * 158, moonY: 258 - Math.sin(moonAngle) * 258, rotation: nightProgress * 180,
    title: `${value.city} · ${fresh ? '今日预报：' + (value.description || label[weather]) : label.unknown}${fresh && Number.isFinite(value.minimum) && Number.isFinite(value.maximum) ? ' · ' + value.minimum + '～' + value.maximum + '℃' : ''} · ${phase === 'night' ? '夜间' : phase === 'dawn' ? '清晨' : phase === 'dusk' ? '傍晚' : '白天'}` };
}
