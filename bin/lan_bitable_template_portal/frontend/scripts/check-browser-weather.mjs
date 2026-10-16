import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { adaptBrowserWeather } from './workspace-weather-adapter.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// ---- Adapter unit checks (V2 compiled LighthouseBot rewrite) ----
const botSample = `import { readIconWeather, weatherScene, weatherClock, type IconWeather } from '../botWeather';
function drawWeather(){ const scene = weatherScene(weather, weatherClock(weather)); bot?.setWeather(scene); weatherTitle.value = scene.title; }
async function refreshWeather(g){ const value = await readIconWeather(props.appearance.weather_city || '南通'); weather = value; }`;
const adapted = adaptBrowserWeather(botSample, root + '/src/components/LighthouseBot.vue');
assert(adapted, 'LighthouseBot.vue is adapted');
assert.match(adapted, /import \{\s*weatherScene, weatherClock, type IconWeather \} from '\.\.\/botWeather'/);
assert.match(adapted, /import \{\s*readBrowserWeather as readIconWeather \} from '\.\.\/workspace-v2\/browserWeather'/);
assert.match(adapted, /__v2Geo\?\.note/);
assert(!adapted.includes('readIconWeather, weatherScene, weatherClock'), 'weather import was split');
// Contract preserved: readIconWeather name still calls the browser helper.
assert.match(adapted, /const value = await readIconWeather\(props\.appearance\.weather_city/);
// Only the intended files are rewritten.
assert.equal(adaptBrowserWeather(botSample, root + '/src/components/OtherPage.vue'), null);
assert.equal(adaptBrowserWeather('<div>无关内容</div>', root + '/src/components/LighthouseBotSettings.vue'), null, 'settings with no weather logic stays untouched');

// ---- Browser runtime checks for the bundled prefecture-center resolution ----
const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div>
<script type="module">
import { readBrowserWeather, DEFAULT_CITY, MAX_DISTANCE_KM, STALE_MS, RETRY_MS, __resetWeatherGeo, __setGeoStateAt } from '/src/workspace-v2/browserWeather.ts';
window.__read = readBrowserWeather;
window.__resetWeatherGeo = __resetWeatherGeo;
window.__setGeoStateAt = __setGeoStateAt;
window.__default = () => DEFAULT_CITY === '南通';
window.__maxKm = () => MAX_DISTANCE_KM === 100;
window.__retryMs = () => RETRY_MS;
window.__staleMs = () => STALE_MS;
</script>`;

const server = await createServer({ root, configFile: false, plugins: [vue()], optimizeDeps: { include: ['vue'] }, logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false } });
await server.listen();
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({ headless: true });

function installGeo(page, mode, coords) {
  return page.addInitScript(({ mode, coords }) => {
    window.__geoCalls = 0;
    Object.defineProperty(navigator, 'geolocation', {
      value: {
        getCurrentPosition(success, error) {
          window.__geoCalls++;
          if (mode === 'denied') return setTimeout(() => error({ code: 1, message: 'denied' }), 0);
          if (mode === 'unavailable') return setTimeout(() => error({ code: 2, message: 'unavailable' }), 0);
          return setTimeout(() => success({ coords: coords || { latitude: 31.23, longitude: 121.47, accuracy: 50 }, timestamp: Date.now() }), 0);
        },
      },
    });
  }, { mode, coords });
}

async function openPage(geoMode, coords) {
  const context = await browser.newContext();
  const page = await context.newPage();
  installGeo(page, geoMode, coords);
  await page.route(origin + '/api/**', async route => {
    const url = new URL(route.request().url());
    assert.equal(url.pathname, '/api/assistant/icon-weather');
    const city = url.searchParams.get('city');
    const now = Date.now() / 1000;
    const dayStart = Math.floor((now + 8 * 3600) / 86400) * 86400 - 8 * 3600;
    await route.fulfill({ json: { ok: true, data: { available: true, city, date: new Date((now + 8 * 3600) * 1000).toISOString().slice(0, 10), description: '晴', minimum: 10, maximum: 20, fetched_at: now, server_time: now, sunrise: dayStart + 6 * 3600, sunset: dayStart + 18 * 3600 } } });
  });
  await page.route('**/*', route => { const u = new URL(route.request().url()); if (u.pathname === '/__weather-check') return route.fulfill({ contentType: 'text/html', body: html }); return route.continue(); });
  await page.goto(origin + '/__weather-check');
  await page.waitForFunction(() => window.__read && window.__default && window.__maxKm && window.__resetWeatherGeo);
  await page.evaluate(() => window.__resetWeatherGeo());
  return { context, page };
}

async function calls(page) { return page.evaluate(() => window.__geoCalls || 0); }

try {
  // 1) Configured city is preserved exactly and never triggers a geolocation prompt.
  {
    const { context, page } = await openPage('granted');
    const a = await page.evaluate(async () => await window.__read('上海'));
    assert.equal(a.city, '上海');
    assert.equal(a._geo.mode, 'configured');
    assert.equal(a._geo.explicit, true);
    assert.equal(a._geo.geolocation, 'skipped');
    assert.equal(a._geo.resolved, false);
    assert.equal(await calls(page), 0, 'configured city does not prompt geolocation');
    await context.close();
  }
  // 2) Default city does not block automatic browser locale: grants near 南通 resolve to 南通.
  {
    const { context, page } = await openPage('granted', { latitude: 32.0, longitude: 120.8, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await page.evaluate(() => window.__default()), true);
    assert.equal(a.city, '南通');
    assert.equal(a._geo.mode, 'auto');
    assert.equal(a._geo.explicit, false);
    assert.equal(a._geo.geolocation, 'granted');
    assert.equal(a._geo.resolved, true);
    assert.equal(a._geo.approximate, true);
    assert(a._geo.note.includes('近似'), 'auto resolution is clearly approximate');
    // Cache: a second read reuses the resolved city without a new geolocation call.
    const again = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(again.city, '南通');
    assert.equal(await calls(page), 1, 'cached auto city reuses single location read');
    await context.close();
  }
  // 3) Auto resolution away from home city: browser in 北京 -> 北京 (default 南通 does not block).
  {
    const { context, page } = await openPage('granted', { latitude: 39.9, longitude: 116.4, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a.city, '北京');
    assert.equal(a._geo.mode, 'auto');
    assert.equal(a._geo.resolved, true);
    assert.equal(a._geo.approximate, true);
    await context.close();
  }
  // 4) Ambiguous prefecture name gets province+city so a district with the same name is never chosen.
  {
    // Liaoning Chaoyang prefecture center 41.5768/120.4512; grant a nearby coords.
    const { context, page } = await openPage('granted', { latitude: 41.58, longitude: 120.45, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a.city, '辽宁省朝阳', 'ambiguous prefecture must include province prefix');
    assert.equal(a._geo.resolved, true);
    await context.close();
  }
  // 5) Consent-denied falls back with a clear message and is cached without hammering.
  {
    const { context, page } = await openPage('denied');
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a._geo.geolocation, 'denied');
    assert.equal(a._geo.mode, 'fallback');
    assert.equal(a._geo.resolved, false);
    assert(a._geo.note.includes('拒绝'), 'denied is clearly reported');
    assert.equal(a.city, '南通');
    const second = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(second._geo.geolocation, 'denied');
    assert.equal(await calls(page), 1, 'denied result is coalesced without re-prompting');
    await context.close();
  }
  // 6) Retry: within RETRY_MS reuse, after the window elapses allow a fresh attempt.
  {
    const { context, page } = await openPage('unavailable');
    await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await calls(page), 1);
    // Within retry window -> no new call.
    await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await calls(page), 1, 'unavailable stays cached within retry window');
    // Force retry window to elapse -> next read re-attempts geolocation.
    await page.evaluate(() => window.__setGeoStateAt(Date.now() - (window.__retryMs() + 50)));
    await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await calls(page), 2, 'retry triggers a fresh location attempt after RETRY_MS');
    await context.close();
  }
  // 7) Distance beyond MAX_DISTANCE_KM refuses and falls back.
  {
    const { context, page } = await openPage('granted', { latitude: 20.0, longitude: 150.0, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a._geo.geolocation, 'granted');
    assert.equal(a._geo.resolved, false);
    assert.equal(a._geo.mode, 'fallback');
    assert(a._geo.distanceKm !== null && a._geo.distanceKm > 100, 'distance refusal carries distance');
    assert(a._geo.note.includes('超过'), 'distance refusal is explicit');
    assert.equal(a.city, '南通');
    await context.close();
  }
  // 8) Invalid coordinates (NaN / out of range) fall back, never pick a false city.
  {
    const { context, page } = await openPage('granted', { latitude: NaN, longitude: NaN, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a._geo.geolocation, 'granted');
    assert.equal(a._geo.resolved, false);
    assert(a._geo.note.includes('无效'), 'invalid coords are clearly reported');
    assert.equal(a.city, '南通');
    await context.close();
  }
  // 9) Stale cached location triggers a refresh, not a repeated per-frame fetch.
  {
    const { context, page } = await openPage('granted', { latitude: 32.0, longitude: 120.8, accuracy: 50 });
    const a = await page.evaluate(async () => await window.__read('南通'));
    assert.equal(a._geo.resolved, true);
    assert.equal(await calls(page), 1);
    // Many warm updates reuse the cached city.
    for (let i = 0; i < 5; i++) await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await calls(page), 1, 'multiple animation updates reuse cache');
    // Force the cached resolution stale -> next read refreshes once.
    await page.evaluate(() => window.__setGeoStateAt(Date.now() - (window.__staleMs() + 50)));
    await page.evaluate(async () => await window.__read('南通'));
    assert.equal(await calls(page), 2, 'stale resolution refreshes the location once');
    assert.equal((await page.evaluate(async () => await window.__read('南通')))._geo.resolved, true);
    await context.close();
  }
  console.log(JSON.stringify({ ok: true, checks: 'configured preserved; default city does not block auto browser locale (auto cities, ambiguous province-prefixed cities); single cached location read across animation updates; consent-denied/retry/distance/stale/invalid all handled; V2 LighthouseBot adapter keeps readIconWeather/weatherScene contracts' }));
} finally {
  await browser.close();
  await server.close();
}