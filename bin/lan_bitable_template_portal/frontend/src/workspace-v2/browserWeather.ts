import { readIconWeather, type IconWeather } from '../botWeather';
import weatherCities from './weatherCities.json';

// Browser-location weather integration for the V2-compiled LighthouseBot.
//
// Coordinates are resolved against the bundled prefecture/province-level city
// centers (src/workspace-v2/weatherCities.json, generated offline from the
// official QWeather LocationList CSV — see weatherCities.NOTICE.md). Resolution
// uses only the stored static dataset and never sends coordinates anywhere.
// The nearest prefecture center is an APPROXIMATE locality, not an exact
// administrative boundary, and a position more than MAX_DISTANCE_KM away from
// every known center is rejected in favour of the configured/default city.

export const DEFAULT_CITY = '南通';
export const MAX_DISTANCE_KM = 100;
// Accept a cached location for a session, then refresh on the next animation
// cycle after it becomes stale — never re-requesting geolocation per frame.
export const STALE_MS = 12 * 60 * 60 * 1000;
// After a denied/unavailable result, keep using the fallback without re-prompting
// the user repeatedly, but allow a periodic retry instead of a permanent lockout.
export const RETRY_MS = 5 * 60 * 1000;
export const EARTH_RADIUS_KM = 6371;

export type WeatherGeoMode = 'configured' | 'fallback' | 'auto';
export type WeatherGeoState = {
  city: string;
  mode: WeatherGeoMode;
  geolocation: 'granted' | 'denied' | 'unavailable' | 'unsupported' | 'skipped' | 'pending';
  explicit: boolean;
  approximate: boolean;
  distanceKm: number | null;
  resolved: boolean;
  note: string;
  at: number;
};

export type BrowserWeatherResult = IconWeather & { _geo?: WeatherGeoState };

type CityCenter = { province: string; city: string; lat: number; lon: number };

const validChineseCity = (value: string) => /^[\u4e00-\u9fff]{2,16}$/.test(value.trim());

// Prefecture/province-level city centers as [province, resolvableCityName, lat, lon].
// "city" is the uniquely-resolvable name for the existing Chinaweather endpoint:
// bare prefecture name where unique, otherwise "province+city" to disambiguate
// districts with the same name in other provinces.
const CENTERS: CityCenter[] = (weatherCities.list as unknown as ReadonlyArray<[string, string, number, number]>).map(
  ([province, city, lat, lon]) => ({ province, city, lat, lon })
);

let geoPromise: Promise<GeolocationPosition> | null = null;
let geoState: WeatherGeoState | null = null;

function browserGeo(): Promise<GeolocationPosition> | null {
  if (typeof navigator === 'undefined' || !navigator.geolocation) return null;
  if (!geoPromise) {
    // Coalesce every concurrent consumer behind a single in-flight read so the
    // user is never re-prompted repeatedly within one animation/update loop.
    geoPromise = new Promise<GeolocationPosition>((resolve, reject) => {
      navigator.geolocation.getCurrentPosition(resolve, reject, { enableHighAccuracy: false, timeout: 8000, maximumAge: 600000 });
    }).catch((error: GeolocationPositionError) => {
      throw error && typeof error.code === 'number' ? error : new Error('unavailable');
    });
  }
  return geoPromise;
}

function buildGeoState(city: string, explicit: boolean, override: Partial<WeatherGeoState>): WeatherGeoState {
  return {
    city, mode: explicit ? 'configured' : 'fallback', geolocation: 'skipped', explicit,
    approximate: false, distanceKm: null, resolved: false, note: '', at: Date.now(),
    ...override,
  };
}

function isValidCoords(lat: number, lon: number): boolean {
  return Number.isFinite(lat) && Number.isFinite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}

function distanceKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
}

function nearestCenter(lat: number, lon: number): { center: CityCenter; km: number } | null {
  let best: { center: CityCenter; km: number } | null = null;
  for (const center of CENTERS) {
    const km = distanceKm(lat, lon, center.lat, center.lon);
    if (!best || km < best.km) best = { center, km };
  }
  return best && best.km <= MAX_DISTANCE_KM ? best : null;
}

function nearestCenterKm(lat: number, lon: number): number {
  let best = Infinity;
  for (const center of CENTERS) {
    const km = distanceKm(lat, lon, center.lat, center.lon);
    if (km < best) best = km;
  }
  return best;
}

async function resolveGeoState(city: string, explicit: boolean, fallback: string): Promise<WeatherGeoState> {
  if (explicit) return buildGeoState(city, true, { mode: 'configured', geolocation: 'skipped' });
  const base = validChineseCity(city) ? city : fallback;
  const now = Date.now();
  // Reuse a recent position/city without re-querying geolocation every update.
  if (geoState) {
    if (geoState.resolved && geoState.geolocation === 'granted' && now - geoState.at < STALE_MS) {
      return geoState;
    }
    if (!geoState.resolved && now - geoState.at < RETRY_MS) {
      return geoState;
    }
    // Stale resolution or a retry window passed: allow a fresh attempt.
    geoState = null;
    geoPromise = null;
  }
  const geo = browserGeo();
  if (!geo) return buildGeoState(base, false, { geolocation: 'unsupported', note: '当前浏览器不支持定位，已按默认城市显示天气。' });
  let position: GeolocationPosition;
  try {
    position = await geo;
  } catch (error) {
    const code = error && typeof error === 'object' && 'code' in error ? (error as GeolocationPositionError).code : undefined;
    const denied = code === 1;
    return buildGeoState(base, false, {
      geolocation: denied ? 'denied' : 'unavailable',
      note: denied ? '定位权限被拒绝，已按默认城市显示天气。' : '定位暂不可用，已按默认城市显示天气。',
    });
  }
  const lat = position.coords.latitude;
  const lon = position.coords.longitude;
  if (!isValidCoords(lat, lon)) {
    return buildGeoState(base, false, {
      geolocation: 'granted',
      note: '获得的位置坐标无效，已按默认城市显示天气。',
    });
  }
  const near = nearestCenter(lat, lon);
  if (!near) {
    return buildGeoState(base, false, {
      geolocation: 'granted',
      distanceKm: Math.round(nearestCenterKm(lat, lon) * 10) / 10,
      note: `当前位置距最近的城市中心超过 ${MAX_DISTANCE_KM} 公里，无法可靠自动定位，已按默认城市显示天气。`,
    });
  }
  return buildGeoState(near.center.city, false, {
    mode: 'auto',
    geolocation: 'granted',
    approximate: true,
    distanceKm: Math.round(near.km * 10) / 10,
    resolved: true,
    note: `按最近的地区中心城市「${near.center.province}${near.center.city}」近似估算（约 ${near.km.toFixed(1)} 公里），非精确行政边界。`,
  });
}

// Coalesced, cached replacement for the compiled V2 LighthouseBot's readIconWeather
// call. It keeps the same IconWeather contract and delegates to the existing backend
// weather facility, attaching geolocation transparency metadata. Explicitly configured
// cities are preserved exactly; the preexisting default city does not block automatic
// browser-locale resolution.
export async function readBrowserWeather(city = DEFAULT_CITY): Promise<BrowserWeatherResult> {
  const trimmed = city.trim();
  const explicit = validChineseCity(trimmed) && trimmed !== DEFAULT_CITY;
  const fallback = trimmed && validChineseCity(trimmed) ? trimmed : DEFAULT_CITY;
  const state = await resolveGeoState(trimmed, explicit, fallback);
  if (!state.explicit) geoState = state;
  const value = await readIconWeather(state.city);
  return { ...value, _geo: state };
}

export function weatherGeoState(): WeatherGeoState | null { return geoState; }
export { validChineseCity };

// Test-only hooks; not used by the production weather path.
export function __resetWeatherGeo(): void { geoState = null; geoPromise = null; }
export function __setGeoStateAt(ms: number): void { if (geoState) geoState = { ...geoState, at: ms }; }
