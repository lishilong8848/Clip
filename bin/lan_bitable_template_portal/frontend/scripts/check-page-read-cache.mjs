import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import ts from 'typescript';

const target = new EventTarget();
globalThis.window = Object.assign(target, {
  location: { origin: 'http://fixture', pathname: '/', search: '', assign() {} },
  setTimeout, clearTimeout,
});
const dataModule = source => 'data:text/javascript;base64,' + Buffer.from(ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
}).outputText).toString('base64');
const cacheURL = dataModule(await fs.readFile(new URL('../src/api/readCache.ts', import.meta.url), 'utf8'));
const cache = await import(cacheURL);
const client = await import(dataModule((await fs.readFile(new URL('../src/api/client.ts', import.meta.url), 'utf8'))
  .replace("from './readCache'", `from '${cacheURL}'`)));
const response = data => new Response(JSON.stringify({ ok: true, data }), { headers: { 'Content-Type': 'application/json' } });
let calls = 0;
globalThis.fetch = async () => { calls++; return response({ value: calls, rows: [{ title: 'Original' }] }); };
client.setReadCacheIdentity('owner-A');
const first = await client.requestJson('/api/scope-overview?scope=A');
first.rows[0].title = 'Edited';
assert.equal((await client.requestJson('/api/scope-overview?scope=A')).rows[0].title, 'Original');
assert.equal(calls, 1, 'return visits reuse independent snapshots');
client.setReadCacheIdentity('owner-B');
await client.requestJson('/api/scope-overview?scope=A');
assert.equal(calls, 2, 'account changes cannot reuse the previous account snapshot');
await client.requestJson('/api/workbench-actions', { method: 'POST', body: '{}' });
await client.requestJson('/api/scope-overview?scope=A');
assert.equal(calls, 4, 'writes invalidate list reads');
assert.equal(cache.readCacheKey('/api/workbench-actions/jobs/id', window.location.origin), '');
assert.equal(cache.readCacheKey('/api/signatures', window.location.origin), '');
assert.equal(cache.readCacheKey('http://another/api/scope-overview', window.location.origin), '');
assert.equal(cache.readCacheKey('/api/scope-overview?force=1', window.location.origin), '');

let release;
client.invalidateReadCache();
globalThis.fetch = async () => { calls++; return await new Promise(resolve => { release = resolve; }); };
const pending1 = client.requestJson('/api/cabinet-power/buildings');
const pending2 = client.requestJson('/api/cabinet-power/buildings');
assert.equal(calls, 5, 'identical cold reads share one request');
release(response({ version: 1 }));
assert.deepEqual(await pending1, await pending2);

client.invalidateReadCache();
const beforeRefresh = client.requestJson('/api/cabinet-power/buildings');
const releaseOld = release;
globalThis.fetch = async () => response({ version: 3 });
assert.equal((await client.requestJson('/api/cabinet-power/buildings', { fresh: true })).version, 3);
releaseOld(response({ version: 2 }));
await beforeRefresh;
assert.equal((await client.requestJson('/api/cabinet-power/buildings')).version, 3, 'an old in-flight read cannot undo explicit refresh');

client.invalidateReadCache();
globalThis.fetch = async () => { calls++; return await new Promise(resolve => { release = resolve; }); };
const oldIdentity = client.requestJson('/api/cabinet-power/buildings');
const releaseIdentity = release;
client.setReadCacheIdentity('owner-C');
globalThis.fetch = async () => response({ version: 4 });
await client.requestJson('/api/cabinet-power/buildings');
releaseIdentity(response({ version: 0 }));
await oldIdentity;
assert.equal((await client.requestJson('/api/cabinet-power/buildings')).version, 4);

const oldNow = Date.now;
let now = oldNow();
Date.now = () => now;
const key = cache.readCacheKey('/api/scope-overview?scope=E', window.location.origin);
cache.saveRead(key, { value: 7 }, cache.readCacheGeneration());
now += 4000;
globalThis.fetch = async () => { calls++; return await new Promise(resolve => { release = resolve; }); };
let changed = 0;
window.addEventListener(client.READ_CACHE_UPDATED, () => { changed++; });
assert.equal((await client.requestJson('/api/scope-overview?scope=E')).value, 7);
release(response({ value: 8 }));
await new Promise(resolve => setTimeout(resolve, 0));
assert.equal(cache.cachedRead(key).data.value, 8);
assert.equal(changed, 1, 'background refresh announces completion without blocking the visible page');
const aborted = new AbortController(); aborted.abort();
await assert.rejects(client.requestJson('/api/scope-overview?scope=E', { signal: aborted.signal }), /取消/);
now += 61_000;
assert.equal(cache.cachedRead(key), null, 'expired snapshots do not stand in for a current read');
cache.saveRead(key, { source_snapshot_ready: false }, cache.readCacheGeneration());
assert.equal(cache.cachedRead(key), null, 'not-ready data must not be cached as an empty source');
Date.now = oldNow;
console.log('[PageReadCache] OK');
