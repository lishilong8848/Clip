// V2-build-only presentational adapter: rewires the compiled LighthouseBot (and any
// related weather settings) to resolve weather through the browser-geolocation aware
// helper (src/workspace-v2/browserWeather.ts) while keeping the existing
// readIconWeather / weatherScene contracts intact.
//
// This adapter ONLY affects the V2 workspace bundle; the legacy LighthouseBot and the
// shared botWeather.ts source remain untouched.

const BOT_IMPORT_SOURCE = "import { readIconWeather, weatherScene, weatherClock, type IconWeather } from '../botWeather';";
const BOT_IMPORT_TARGET =
  "import { weatherScene, weatherClock, type IconWeather } from '../botWeather';\n" +
  "import { readBrowserWeather as readIconWeather } from '../workspace-v2/browserWeather';";

const DRAW_TITLE_OLD = 'weatherTitle.value = scene.title;';
const DRAW_TITLE_NEW =
  'const __v2Geo = (weather as any)?._geo; weatherTitle.value = __v2Geo?.note ? scene.title + \' · \' + __v2Geo.note : scene.title;';

// Rewrite the compiled LighthouseBot so its readIconWeather calls resolve through the
// browser-location helper (which preserves configured cities, coalesces geolocation,
// and clearly reports fallback / missing reverse-geocoding API).
export function adaptBrowserWeather(source, filename) {
  if (typeof source !== 'string') return null;
  const file = filename.replaceAll('\\', '/').split('?')[0];
  if (!file.endsWith('/LighthouseBot.vue') && !file.endsWith('/LighthouseBotSettings.vue')) return null;
  let result = source;
  let changed = false;
  if (result.includes("readIconWeather, weatherScene, weatherClock, type IconWeather")) {
    result = result.split(BOT_IMPORT_SOURCE).join(BOT_IMPORT_TARGET);
    changed = true;
  }
  if (result.includes(DRAW_TITLE_OLD) && !result.includes('__v2Geo')) {
    result = result.split(DRAW_TITLE_OLD).join(DRAW_TITLE_NEW);
    changed = true;
  }
  return changed ? result : null;
}