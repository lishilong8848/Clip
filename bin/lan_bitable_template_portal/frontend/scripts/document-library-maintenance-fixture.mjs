import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { documentFixture, startDocumentLibraryPreview } from './document-library-preview.mjs';

export const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const output = path.resolve(frontend, '../../../output/playwright/document-library-maintenance');
const copy = value => structuredClone(value);
const wait = delay => new Promise(resolve => setTimeout(resolve, delay));
export function maintenanceFixture() {
  const original = documentFixture();
  const records = Array.from({ length: 103 }, (_, index) => {
    const record = copy(original.records[0]);
    record.record_id = `recMaintenance${index + 1}`;
    record.fields.fldTitle = `维护资料${index + 1}`;
    record.fields.fldScope = index < 65 ? 'A' : 'E';
    record.fields.fldCount = index + 1;
    record.capabilities.reindex = true;
    record.knowledge_status = 'ready';
    record.knowledge_items.forEach(item => { item.status = 'ready'; item.vectorized = true; });
    record.knowledge_summary = { total: 2, ready: 2, cached: 2, vectorized: true };
    if (index === 1 || index === 2) {
      record.knowledge_status = index === 1 ? 'partial' : 'unsupported';
      record.knowledge_items[1].status = index === 1 ? 'text_only' : 'unsupported';
      record.knowledge_items[1].vectorized = false;
      record.knowledge_summary.ready = 1; record.knowledge_summary.vectorized = false;
    }
    return record;
  });
  return {
    tables: original.tables.map(table => ({ ...table, capabilities: { ...table.capabilities, reindex: true } })), records,
    canConfigure: true, sourceReindex: true, recordReindex: true, legacy: false,
    actor: 'ou_fixtureDoc', calls: [], mutations: [], receipts: new Map(), applied: 0,
    cache: { complete: true, updated_at: 1792500000, refreshing: false, error: '' },
    cleanup_pending: 0, cleanup_error: '', cleanupStatus: 'pending',
    listDelay: 0, mutationDelay: 0, listFailure: false, listStatus: 0, detailStatus: 0, loseReceipt: false, malformedReceipt: false,
    conflict: false, duplicateCursor: false, empty: false,
  };
}

function matchesFields(record, filters = []) {
  return filters.every(filter => {
    const value = record.fields[filter.field], empty = value === undefined || value === null || value === '';
    if (filter.op === 'empty') return empty;
    if (filter.op === 'not_empty') return !empty;
    if (empty) return false;
    if (filter.op === 'gte') return Number(value) >= Number(filter.value);
    if (filter.op === 'lte') return Number(value) <= Number(filter.value);
    return filter.op === 'eq' ? String(value) === String(filter.value) : String(value).includes(filter.value);
  });
}

export async function startMaintenancePreview({ port = 0 } = {}) {
  const base = await startDocumentLibraryPreview();
  const fixture = maintenanceFixture();
  const server = createServer(async (request, response) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    const json = (data, status = 200) => {
      response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      response.end(JSON.stringify(status < 400 ? { ok: true, data } : { ok: false, error: data }));
    };
    const present = record => {
      const result = copy(record); result.capabilities.reindex = fixture.recordReindex;
      if (fixture.legacy) {
        delete result.capabilities.reindex; delete result.knowledge_summary;
        result.knowledge_items.forEach(item => delete item.vectorized);
      }
      return result;
    };
    try {
      if (url.pathname === '/api/auth/status') return json({ logged_in: true,
        user: { open_id: fixture.actor, name: '隔离维护验收', role: fixture.canConfigure ? 'admin' : 'building', level: fixture.canConfigure ? 'L1' : 'L4', home_scope: 'A', modules: ['knowledge'] },
        scope_options: [{ value: 'A', label: 'A楼' }, { value: 'E', label: 'E楼' }] });
      if (url.pathname.startsWith('/api/document-library/')) {
        const relative = url.pathname.slice('/api/document-library'.length);
        const match = /^\/tables\/([^/]+)(.*)$/.exec(relative);
        const key = match ? decodeURIComponent(match[1]) : '', resource = match?.[2] || '';
        const call = { path: relative, method: request.method, query: Object.fromEntries(url.searchParams) };
        fixture.calls.push(call);
        if (relative === '/tables' && request.method === 'GET') return json({
          items: fixture.tables.map(table => { const result = copy(table); result.capabilities.reindex = fixture.sourceReindex; if (fixture.legacy) delete result.capabilities.reindex; return result; }),
          can_configure: fixture.canConfigure, scopes: [{ value: 'A', label: 'A楼' }, { value: 'E', label: 'E楼' }],
          ...(fixture.legacy ? {} : { cleanup_pending: fixture.cleanup_pending, cleanup_error: fixture.cleanup_error }),
        });
        const table = fixture.tables.find(table => table.id === key);
        if (table && resource === '/sync' && request.method === 'POST') {
          fixture.cache.error = ''; fixture.cache.refreshing = true;
          return json({ job_id: 'synthetic-sync', status: 'queued' });
        }
        if (request.method === 'POST' && resource === '/reindex' || request.method === 'DELETE' && match && !resource) {
          const chunks = []; for await (const chunk of request) chunks.push(chunk);
          const body = JSON.parse(Buffer.concat(chunks).toString()); call.body = body;
          fixture.mutations.push(copy(call));
          await wait(fixture.mutationDelay);
          assert.match(body.operation_id, /^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/i);
          if (fixture.receipts.has(body.operation_id)) return json(fixture.receipts.get(body.operation_id));
          if (!table) return json('资料表已移除', 404);
          if (fixture.conflict || body.expected_revision !== table.revision) return json('配置版本已变化', 409);
          if (request.method === 'DELETE' ? !fixture.canConfigure : !fixture.sourceReindex || !fixture.recordReindex) return json('没有本地维护权限', 403);
          let receipt;
          if (request.method === 'DELETE') {
            assert.deepEqual(Object.keys(body).sort(), ['expected_revision', 'operation_id']);
            fixture.tables = fixture.tables.filter(item => item.id !== key);
            fixture.cleanup_pending = fixture.cleanupStatus === 'pending' ? 1 : 0;
            receipt = { operation_id: body.operation_id, removed: true, cleanup_status: fixture.cleanupStatus };
          } else {
            let selected;
            if (body.all) {
              assert(!('record_ids' in body)); assert.equal(typeof body.q, 'string'); assert.equal(typeof body.scope, 'string');
              selected = fixture.records.filter(record => (!body.q || record.fields.fldTitle.includes(body.q)) && (!body.scope || record.fields.fldScope === body.scope) && matchesFields(record, body.filters));
            } else {
              assert(body.record_ids.length > 0); assert.equal(new Set(body.record_ids).size, body.record_ids.length);
              selected = fixture.records.filter(record => body.record_ids.includes(record.record_id));
            }
            receipt = { operation_id: body.operation_id, status: 'queued', records: selected.length, items: selected.length * 2 };
          }
          fixture.applied++; fixture.receipts.set(body.operation_id, receipt);
          if (fixture.loseReceipt) { fixture.loseReceipt = false; return json('已提交，但回执传输失败', 503); }
          if (fixture.malformedReceipt) { fixture.malformedReceipt = false; return json({ operation_id: 'wrong' }); }
          return json(receipt);
        }
        if (!table) return json('没有此表访问权限', 403);
        if (resource === '/records' && request.method === 'GET') {
          await wait(fixture.listDelay);
          if (fixture.listStatus) return json('列表读取权限已失效', fixture.listStatus);
          if (fixture.listFailure) return json('缓存列表暂时不可用', 503);
          const filters = JSON.parse(url.searchParams.get('filters') || '[]');
          const visible = fixture.empty ? [] : fixture.records.filter(record => (!url.searchParams.get('q') || record.fields.fldTitle.includes(url.searchParams.get('q'))) && (!url.searchParams.get('scope') || record.fields.fldScope === url.searchParams.get('scope')) && matchesFields(record, filters));
          const start = Number(url.searchParams.get('cursor') || 0), size = Number(url.searchParams.get('page_size') || 30), more = start + size < visible.length;
          return json({ items: visible.slice(start, start + size).map(present), has_more: more,
            cursor: more ? String(start + size) : '',
            ...(fixture.legacy ? {} : { total: visible.length, cache: fixture.cache }),
          });
        }
        if (/^\/records\/[^/]+$/.test(resource) && request.method === 'GET') {
          if (fixture.detailStatus) return json('详情读取权限已失效', fixture.detailStatus);
          const record = fixture.records.find(record => record.record_id === resource.split('/')[2]);
          return record ? json(present(record)) : json('资料不存在', 404);
        }
        if (request.method !== 'GET') return json('验收禁止飞书写入及其他变更', 403);
        if (resource === '/schema') {
          const upstream = await fetch(base.origin + request.url);
          const body = await upstream.json(); body.data.cache = fixture.legacy ? undefined : fixture.cache;
          return json(body.data);
        }
      }
      if (!['GET', 'HEAD'].includes(request.method)) return json('验收禁止非本地维护写入', 403);
      // Only the existing local synthetic server receives fall-through requests.
      const upstream = await fetch(base.origin + request.url, { redirect: 'manual' });
      response.writeHead(upstream.status, Object.fromEntries([...upstream.headers].filter(([name]) => !['transfer-encoding', 'content-encoding', 'content-length'].includes(name))));
      response.end(Buffer.from(await upstream.arrayBuffer()));
    } catch (error) { json(error.message, 500); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  return { fixture, origin: `http://127.0.0.1:${server.address().port}`, close: async () => {
    await new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }); await base.close();
  } };
}

export async function checkMaintenanceSource() {
  const [{ readFile }, { compileScript, parse }, ts, vue, { runInNewContext }, { randomUUID, webcrypto }] = await Promise.all([
    import('node:fs/promises'), import('@vue/compiler-sfc'), import('typescript'), import('vue'), import('node:vm'), import('node:crypto'),
  ]);
  const source = await readFile(path.join(frontend, 'src/components/DocumentLibraryPage.vue'), 'utf8');
  const compiled = ts.transpileModule(compileScript(parse(source).descriptor, { id: 'maintenance-source-check' }).content,
    { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  class ApiError extends Error { constructor(message, options = {}) { super(message); this.status = options.status; this.payload = options.payload || {}; } }
  const pending = () => new ApiError('正在同步', { status: 503, payload: { code: 'document_library_sync_pending' } });
  const clone = value => JSON.parse(JSON.stringify(value));
  const flush = () => new Promise(resolve => setImmediate(resolve));
  const checks = [];
  const apiExports = {}, apiRequests = [];
  let reply;
  const crypto = { getRandomValues: values => webcrypto.getRandomValues(values) };
  const apiCode = ts.transpileModule(await readFile(path.join(frontend, 'src/api/documentLibrary.ts'), 'utf8'),
    { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  runInNewContext(apiCode, { exports: apiExports, window: { crypto }, URLSearchParams,
    require: () => ({ ApiError, requestJson: async (url, options) => { apiRequests.push({ url, options }); return reply; } }),
  });
  const ids = Array.from({ length: 32 }, () => apiExports.newLibraryOperationId());
  ids.forEach(id => assert.match(id, /^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/));
  assert.equal(new Set(ids).size, ids.length);
  crypto.getRandomValues = values => values.fill(255);
  assert.equal(apiExports.newLibraryOperationId(), 'ffffffff-ffff-4fff-bfff-ffffffffffff');
  crypto.getRandomValues = values => values.fill(0);
  assert.equal(apiExports.newLibraryOperationId(), '00000000-0000-4000-8000-000000000000');
  crypto.randomUUID = () => ids[0]; assert.equal(apiExports.newLibraryOperationId(), ids[0]);
  reply = { fields: [{ id: 'title', name: 'Title' }], display_fields: ['title'], attachment_fields: [], cache: { complete: false } };
  await apiExports.documentLibraryApi.schema('table'); assert(!apiRequests.at(-1).url.includes('refresh='));
  await apiExports.documentLibraryApi.schema('table', undefined, true); assert(apiRequests.at(-1).url.endsWith('?refresh=1'));
  reply = maintenanceFixture().records[0];
  await apiExports.documentLibraryApi.record('table', reply.record_id); assert(!apiRequests.at(-1).url.includes('refresh='));
  await apiExports.documentLibraryApi.record('table', reply.record_id, undefined, true); assert(apiRequests.at(-1).url.endsWith('?refresh=1'));
  reply = { operation_id: 'incorrect' };
  await assert.rejects(apiExports.documentLibraryApi.reindex('table', { operation_id: ids[0], expected_revision: 'r1', all: true, q: '', scope: '' }), /回执不完整/);
  checks.push('LAN HTTP UUIDv4 fallback with version/variant bits; real API cache/refresh and receipt validation');
  function make() {
    const fixture = maintenanceFixture(), calls = [], timers = new Map(), unmount = [], events = new EventTarget();
    let timerId = 0, identity = 'actor-a';
    const rows = fixture.records.map(record => ({ ...record, attachments: record.attachments.map(file => ({ ...file, id: file.attachment_id })) }));
    const api = {
      tables: async () => ({ items: fixture.tables.map(table => ({ ...table, key: table.id })), can_configure: true, scopes: [{ value: 'A', label: 'A楼' }] }),
      schema: async (key, signal, refresh) => { calls.push({ schema: key, refresh }); return { fields: [{ id: 'fldTitle', name: '资料名称', type: 1 }, { id: 'fldCount', name: '文件页数', type: 2 }], display_fields: ['fldTitle'], attachment_fields: [] }; },
      records: async (key, params) => {
        calls.push({ key, params }); const start = Number(params.cursor || 0);
        return { items: rows.slice(start, start + params.page_size), has_more: start + params.page_size < rows.length, cursor: start + params.page_size < rows.length ? String(start + params.page_size) : '', total: rows.length, cache: fixture.cache };
      },
      record: async (key, id, signal, refresh) => { calls.push({ key, id, refresh }); return rows.find(record => record.record_id === id); },
      reindex: async (key, payload) => { calls.push({ key, payload }); return { operation_id: payload.operation_id, status: 'queued', records: 1, items: 2 }; },
      remove: async (key, payload) => ({ operation_id: payload.operation_id, removed: true, cleanup_status: 'pending' }),
    };
    const window = { location: { origin: 'http://127.0.0.1' }, crypto: { randomUUID },
      setTimeout(callback, delay) { timers.set(++timerId, { callback, delay }); return timerId; }, clearTimeout(id) { timers.delete(id); },
      addEventListener: events.addEventListener.bind(events), removeEventListener: events.removeEventListener.bind(events) };
    const document = { hidden: false, addEventListener() {}, removeEventListener() {} };
    const exports = {};
    runInNewContext(compiled, { exports, Error, AbortController, DOMException, URL, window, document,
      require(name) {
        if (name === 'vue') return { ...vue, onMounted() {}, onBeforeUnmount(fn) { unmount.push(fn); } };
        if (name === 'naive-ui') return { useThemeVars: () => vue.ref({}) };
        if (name.endsWith('/api/documentLibrary')) return { documentLibraryApi: api, cloneLibraryValue: clone, newLibraryOperationId: randomUUID,
          fieldKind: () => 'text', fieldValue: value => value, valueText: value => String(value), writableField: () => true };
        if (name.endsWith('/api/readCache')) return { readCacheKey: () => identity };
        if (name.endsWith('/api/client')) return { ApiError, AUTH_EXPIRED_EVENT: 'auth-expired', requestJson: async () => { calls.push({ sync: true }); } };
        if (name.endsWith('/navigation')) return { registerNavigationGuard: () => () => {} };
        if (name === 'lucide-vue-next' || name.endsWith('.vue')) return {};
        throw new Error('Unexpected component import: ' + name);
      },
    });
    const page = exports.default.setup({}, { expose() {} });
    return { page, api, rows, calls, fixture, timers, document, identity: value => { identity = value; }, dispose: () => unmount.forEach(fn => fn()) };
  }
  const contexts = [];
  async function open() { const ctx = make(); contexts.push(ctx); await ctx.page.loadTables('electrical'); return ctx; }
  try {
    const configured = await open();
    configured.page.settingsOpen.value = true; configured.page.settingsNew.value = true;
    await configured.page.configurationSaved({ key: 'mechanical' });
    assert.equal(configured.page.activeKey.value, 'mechanical');
    assert.equal(configured.page.notice.value, '数据表已接入，请核对显示字段及编辑范围。');
    await configured.page.loadTables();
    assert.equal(configured.page.notice.value, '数据表已接入，请核对显示字段及编辑范围。');
    checks.push('confirmed table connection survives source switching and background refresh');
    const ctx = await open(), p = ctx.page;
    assert.equal(p.vectorized(ctx.rows[0]), true);
    assert.equal(p.vectorized(ctx.rows[1]), false); assert.equal(p.vectorProgress(ctx.rows[1]), '已完成 1/2 项');
    assert.equal(p.vectorized(ctx.rows[2]), false);
    const legacy = clone(ctx.rows[0]); delete legacy.knowledge_summary; legacy.knowledge_items.forEach(item => delete item.vectorized);
    assert.equal(p.vectorized(legacy), false);
    const missing = clone(ctx.rows[0]); missing.knowledge_items = [missing.knowledge_items[1]];
    assert.equal(p.vectorized(missing), false, 'Missing record text cannot be green');
    checks.push('actual vectors, partial/text-only/unsupported/legacy states');

    const originalRead = ctx.api.records;
    let finish;
    ctx.api.records = () => new Promise(resolve => { finish = resolve; });
    const refreshing = p.loadTables(); await flush();
    assert.equal(p.records.value.length, 30, 'Same-table refresh retains cached rows');
    finish({ items: ctx.rows.slice(0, 2), has_more: true, cursor: '2', total: 7, cache: { ...ctx.fixture.cache, complete: false, refreshing: true } });
    await refreshing; assert.equal(p.listPending.value, true); assert.equal(p.records.value.length, 2);
    ctx.api.records = async () => { throw pending(); };
    await p.readRecords(); assert.equal(p.listPending.value, true); assert.equal(p.records.value.length, 2); assert.equal(p.listError.value, '');
    assert.equal(ctx.timers.size, 0, 'Pending must not start an aggressive retry loop');
    ctx.api.records = originalRead;
    ctx.fixture.cache.error = '部分缓存同步失败'; ctx.fixture.cache.complete = false;
    await p.readRecords(); assert.equal(p.syncFailed.value, true); assert.equal(p.records.value.length, 30);
    ctx.fixture.cache.error = ''; await p.retryRecords(); assert.equal(ctx.calls.filter(call => call.sync).length, 1);
    assert.equal(p.syncFailed.value, false);
    p.schedulePoll(); assert.equal([...ctx.timers.values()][0].delay, 30_000);
    p.listPending.value = false; p.schedulePoll(); assert.equal([...ctx.timers.values()][0].delay, 120_000);
    ctx.document.hidden = true; p.visibilityChanged(); assert.equal(ctx.timers.size, 0);
    const reads = ctx.calls.length; await p.poll(); assert.equal(ctx.calls.length, reads);
    ctx.document.hidden = false; p.schedulePoll(); ctx.dispose(); assert.equal(ctx.timers.size, 0);
    checks.push('first-sync partial/pending keeps rows; cache.error retries /sync; 30s pending / 120s settled visibility polling and teardown');

    const background = await open(), b = background.page;
    const schemaReads = background.calls.filter(call => call.schema).length;
    let backgroundResolve;
    const backgroundRead = background.api.records;
    background.api.records = () => new Promise(resolve => { backgroundResolve = resolve; });
    const runningPoll = b.poll(); await flush();
    assert.equal(b.listLoading.value, false); assert.equal(b.tablesLoading.value, false);
    assert.equal(b.listRefreshing.value, true); assert.equal(b.busy.value, false);
    assert.equal(b.records.value.length, 30); b.selectPage(); assert.equal(b.selectedIds.value.length, 30);
    await b.poll(); assert.equal(background.calls.filter(call => call.schema).length, schemaReads, 'Silent refresh does not reload unchanged schema');
    backgroundResolve({ items: background.rows.slice(0, 30), has_more: true, cursor: '30', total: 103, cache: background.fixture.cache });
    const sameRows = b.records.value; await runningPoll;
    assert.equal(b.records.value, sameRows, 'Unchanged rows retain identity');
    assert.equal(b.selectedIds.value.length, 30); assert.equal(b.listRefreshing.value, false);
    background.api.records = backgroundRead;
    b.goPage(3); await flush(); assert.equal(b.currentCursor.value, '60');
    assert.deepEqual(Array.from(b.pageNumbers.value), [1, 2, 3, 4]);
    b.total.value = 600; b.currentCursor.value = '270';
    assert.deepEqual(Array.from(b.pageNumbers.value), [1, null, 9, 10, 11, null, 20]);
    b.filters.value = [{ field: 'fldCount', op: 'gte', value: null }];
    b.searchRecords(); assert(b.filterError.value); assert.equal(b.currentCursor.value, '270');
    b.filters.value[0].value = 10; b.searchRecords(); await flush();
    assert.equal(b.currentCursor.value, ''); assert.equal(b.hasSelection.value, false);
    assert.deepEqual(clone(background.calls.filter(call => call.params).at(-1).params.filters), [{ field: 'fldCount', op: 'gte', value: 10 }]);
    b.selectAll(); b.filters.value[0].value = 99; b.confirmReindex();
    assert.deepEqual(clone(b.maintenanceIntent.value.payload.filters), [{ field: 'fldCount', op: 'gte', value: 10 }], 'All matching uses applied filters, not unsubmitted edits');
    checks.push('silent refresh keeps controls enabled and selection; unchanged row identity; numbered pages; typed applied field filters shared by cross-page selection');

    const stale = await open();
    let oldResolve;
    stale.api.records = () => new Promise(resolve => { oldResolve = resolve; });
    const old = stale.page.readRecords(); stale.page.activeKey.value = 'mechanical';
    oldResolve({ items: stale.rows, has_more: false, cursor: '' }); await old;
    assert.equal(stale.page.records.value.length, 30, 'Old table result ignored');
    let cancelledReject;
    stale.api.records = () => new Promise((_, reject) => { cancelledReject = reject; });
    const cancelling = stale.page.readRecords(); stale.dispose(); cancelledReject(pending()); await cancelling;
    assert.equal(stale.page.listError.value, '');
    const switching = await open(); let resolveFirst;
    switching.api.records = key => key === 'electrical' ? new Promise(resolve => { resolveFirst = resolve; }) : Promise.resolve({ items: [switching.rows[6]], has_more: false, cursor: '' });
    const first = switching.page.readRecords(); switching.page.activeKey.value = 'mechanical'; await switching.page.readRecords();
    resolveFirst({ items: [switching.rows[0]], has_more: false, cursor: '' }); await first;
    assert.equal(switching.page.records.value[0].record_id, 'recMaintenance7');
    switching.api.records = async () => { throw new ApiError('同步失败', { payload: { code: 'document_library_sync_failed' } }); };
    await switching.page.readRecords(); assert.equal(switching.page.syncFailed.value, true);
    switching.api.records = async () => ({ items: [], has_more: false, cursor: '' }); await switching.page.retryRecords();
    assert.equal(switching.calls.filter(call => call.sync).length, 1);
    checks.push('first-sync cancelled reads, stale table responses, replacement reads and explicit retry');

    const selected = await open(), s = selected.page;
    s.selectPage(); await s.readRecords('30', []); s.toggleRecord(selected.rows[30], true);
    assert.equal(s.selectedIds.value.length, 31); assert.equal(s.selectedIds.value.at(-1), 'recMaintenance31');
    const beforeLast = selected.calls.filter(call => call.params).length;
    s.lastPage(); await flush(); assert.equal(s.currentCursor.value, '90'); assert.equal(s.records.value.length, 13);
    assert.equal(selected.calls.filter(call => call.params).length - beforeLast, 1, '103 records: last page is exactly one request');
    s.previousPage(); await flush(); assert.equal(s.currentCursor.value, '60');
    s.firstPage(); await flush(); assert.equal(s.currentCursor.value, ''); assert.equal(s.selectedIds.value.length, 31);
    s.appliedQuery.value = '维护'; s.query.value = '未应用输入'; s.scope.value = 'A'; s.selectAll(); s.confirmReindex();
    assert.equal(s.maintenanceIntent.value.payload.all, true); assert.equal(s.maintenanceIntent.value.payload.q, '维护'); assert.equal(s.maintenanceIntent.value.payload.scope, 'A');
    assert.equal('record_ids' in s.maintenanceIntent.value.payload, false);
    let resolveMutation; const operations = [];
    selected.api.reindex = (key, payload) => { operations.push(payload.operation_id); return new Promise(resolve => { resolveMutation = resolve; }); };
    const one = s.runMaintenance(); await s.runMaintenance(); assert.equal(operations.length, 1);
    resolveMutation({ records: 5, items: 10 }); await one;
    s.confirmReindex(selected.rows[0]); const operation = s.maintenanceIntent.value.payload.operation_id;
    selected.api.reindex = async (key, payload) => { operations.push(payload.operation_id); throw new ApiError('receipt lost', { status: 503 }); };
    await s.runMaintenance(); await s.runMaintenance(); assert.equal(s.maintenanceUncertain.value, true);
    assert.deepEqual(operations.slice(-2), [operation, operation]);
    s.query.value = '新过滤'; s.searchRecords(); await flush();
    assert.equal(s.hasSelection.value, false); assert.equal(s.maintenanceOpen.value, false);
    checks.push('page/explicit/all selection, 103 rows last page in one request, previous/home, applied filters, deduplication and stable retry UUID');

    const editing = await open(), e = editing.page;
    e.openRecord(editing.rows[0].record_id); await flush(); assert.equal(editing.calls.at(-1).refresh, undefined);
    e.editing.value = true; e.draft.value.fldTitle = '未保存输入'; e.reason.value = '保留原因';
    const beforePoll = editing.calls.length; await e.poll(); assert.equal(editing.calls.length, beforePoll);
    e.reload(); await flush(); assert.equal(e.draft.value.fldTitle, '未保存输入'); assert.equal(e.detailOpen.value, true);
    assert.equal(editing.calls.filter(call => call.schema).at(-1).refresh, true);
    await e.readLatest(); assert.equal(editing.calls.at(-1).refresh, true); assert.equal(e.draft.value.fldTitle, '未保存输入');
    e.editing.value = false; e.selectAll(); e.confirmReindex();
    editing.identity('actor-b'); e.identityChanged(); await flush();
    assert.equal(e.hasSelection.value, false); assert.equal(e.maintenanceOpen.value, false); assert.equal(e.detailOpen.value, false);
    assert.deepEqual(Object.keys(e.draft.value), []);
    checks.push('editing survives manual refresh; explicit refresh=1; identity clears selections and old drawers');

    for (const status of [401, 403, 404, 503]) {
      for (const stage of ['list', 'schema', 'latest', 'poll-detail']) {
        const access = await open(), a = access.page;
        a.openRecord(access.rows[0].record_id); await flush(); a.selectPage();
        if (stage !== 'poll-detail') { a.editing.value = true; a.draft.value.fldTitle = '尚未保存'; a.reason.value = '本地编辑'; }
        let rejected = 0;
        const deny = async () => { rejected++; throw new ApiError(`Read status ${status}`, { status }); };
        if (stage === 'list') { access.api.records = deny; await a.readRecords(); }
        else if (stage === 'schema') { access.api.schema = deny; await a.loadTable(); }
        else { access.api.record = deny; if (stage === 'latest') await a.readLatest(); else await a.poll(); }
        assert.equal(rejected, 1, `${stage}/${status}: actual read must execute`);
        if (status === 503) {
          assert.equal(a.records.value.length, 30, `${stage}: transient failure retains rows`);
          assert.equal(a.detailOpen.value, true); assert.equal(a.detail.value.record_id, access.rows[0].record_id);
          assert.equal(a.selectedIds.value.length, 30);
          if (stage !== 'poll-detail') assert.equal(a.draft.value.fldTitle, '尚未保存');
        } else {
          assert.equal(a.records.value.length, 0, `${stage}/${status}: unauthorized cached rows removed`);
          assert.equal(a.detailOpen.value, false); assert.equal(a.detail.value, null);
          assert.equal(a.schema.value, null); assert.equal(a.targetId.value, '');
          assert.equal(a.hasSelection.value, false); assert.equal(a.maintenanceOpen.value, false);
          assert.deepEqual(Object.keys(a.draft.value), []); assert.deepEqual(Object.keys(a.baseline.value), []);
          assert.equal(a.listError.value, `Read status ${status}`);
        }
      }
    }
    const revoked = await open(), r = revoked.page;
    r.openRecord(revoked.rows[0].record_id); await flush();
    let delayedDetail;
    revoked.api.record = () => new Promise(resolve => { delayedDetail = resolve; });
    const late = r.readLatest();
    revoked.api.records = async () => { throw new ApiError('No longer authorized', { status: 403 }); };
    await r.readRecords(); delayedDetail(revoked.rows[0]); await late;
    assert.equal(r.records.value.length, 0); assert.equal(r.detail.value, null); assert.equal(r.detailOpen.value, false);
    checks.push('401/403/404 list/schema/latest/poll-detail revoke cached rows, detail and selection; 503 keeps edits; late detail cannot restore revoked data');
  } finally { contexts.forEach(ctx => ctx.dispose()); }
  return checks;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const server = await startMaintenancePreview({ port: Number(process.env.DOCUMENT_LIBRARY_MAINTENANCE_PORT || 0) });
  console.log(`Synthetic maintenance preview: ${server.origin}/?version=v2`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await server.close(); process.exit(0); });
}
