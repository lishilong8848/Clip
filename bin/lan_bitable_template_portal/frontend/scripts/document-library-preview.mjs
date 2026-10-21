import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'vite';
import vue from '@vitejs/plugin-vue';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const output = path.resolve(root, '../../../output/playwright/document-library');
const copy = value => JSON.parse(JSON.stringify(value));
const schema = {
  fields: [
    { id: 'fldTitle', name: '资料名称', type: 1, read_only: false, required: true },
    { id: 'fldKind', name: '资料分类', type: 3, options: ['运维规程', '图纸资料'], read_only: false },
    { id: 'fldScope', name: '所属楼栋', type: 3, options: ['A', 'E'], read_only: false },
    { id: 'fldCount', name: '文件页数', type: 2, read_only: false },
    { id: 'fldTags', name: '专业标签', type: 4, options: ['电气', '暖通'], read_only: false },
    { id: 'fldDate', name: '生效日期', type: 5, read_only: false },
    { id: 'fldOwner', name: '资料负责人', type: 11, options: [{ id: 'ou_fixtureDoc', name: '资料管理员' }, { id: 'ou_fixturePeer', name: '值班资料员' }], read_only: false },
    { id: 'fldLink', name: '关联链接', type: 15, read_only: false },
    { id: 'fldValid', name: '当前有效', type: 7, read_only: false },
    { id: 'fldFiles', name: '资料附件', type: 17, read_only: false },
    { id: 'fldFormula', name: '系统汇总', type: 20, read_only: true },
    { id: 'fldUnknown', name: '原始系统字段', type: 999, read_only: false },
  ],
  display_fields: ['fldTitle', 'fldKind', 'fldScope', 'fldOwner'], attachment_fields: ['fldFiles'],
};
const scopes = [{ value: 'A', label: 'A楼' }, { value: 'E', label: 'E楼' }];
function table(key, name, writable = true) {
  return { id: key, name, base_token: 'baseSyntheticDocument', table_id: 'tblSynthetic' + key, url: '', revision: 'config1',
    enabled: true, fixed_scopes: ['A'], scope_field: 'fldScope', read_scopes: ['A', 'E'], write_scopes: writable ? ['A'] : [],
    display_fields: schema.display_fields.slice(), attachment_fields: ['fldFiles'],
    capabilities: { create: writable, configure_tables: true, max_upload_bytes: 104857600 } };
}
function makeRecord(id, title, scope = 'A') {
  return { record_id: id, revision: 'a'.repeat(64), fields: { fldTitle: title, fldKind: '运维规程', fldScope: scope,
    fldCount: 16, fldTags: ['电气'], fldDate: 1785456000000, fldOwner: [{ id: 'ou_fixtureDoc', name: '资料管理员' }],
    fldLink: { link: 'https://example.invalid/reference', text: '资料关联' }, fldValid: true,
    fldFiles: [{ name: '南通标识.png' }], fldFormula: '自动汇总 · 16页', fldUnknown: { name: '系统原始值' } },
    attachments: [{ attachment_id: 'file1', field_id: 'fldFiles', name: '南通标识.png', size: 2048, type: 'image/png' }],
    capabilities: { edit: true, upload: true }, knowledge_status: { state: 'indexed' } };
}
export function documentFixture() {
  return { tables: [table('electrical', '电气技术资料'), table('mechanical', '暖通技术资料', false)],
    records: [makeRecord('recDoc1', 'A楼高压配电操作规程'), makeRecord('recDoc2', 'A楼电气系统竣工图'), makeRecord('recDoc3', 'E楼运行资料', 'E')],
    calls: [], writes: [], applied: 0, receipts: new Map(), flags: { tablesFailure: false, schemaFailure: false, listFailure: false,
      detailFailure: false, versionsFailure: false, conflict: false, loseReceipt: false, indexFailure: false, denyWrite: false },
    canConfigure: true, personMultiple: false, listDelay: 0, version: 1, authReads: 0 };
}
function fixturePlugin() {
  return {
    name: 'isolated-document-library-preview',
    enforce: 'post',
    resolveId(id) { if (id === 'virtual:document-library-preview') return '\0document-library-preview'; },
    load(id) {
      if (id !== '\0document-library-preview') return;
      return `import { createApp, h } from 'vue';
        import { NConfigProvider, darkTheme } from 'naive-ui';
        import Page from '/src/components/DocumentLibraryPage.vue';
        import { theme, nightTheme } from '/src/workspace-v2/theme';
        import '/src/global.css'; import '/src/workspace-v2/night.css';
        const params = new URLSearchParams(location.search), v2 = params.get('version') === 'v2', dark = params.has('dark');
        if (v2) { document.documentElement.classList.add('workspace-v2'); document.documentElement.dataset.v2Theme = dark ? 'dark' : 'light'; }
        document.body.style.margin = '0'; document.body.style.background = dark ? '#16191d' : '#ffffff';
        createApp({ render: () => h(NConfigProvider, { theme: dark ? darkTheme : null, themeOverrides: v2 ? dark ? nightTheme : theme : undefined }, {default: () => h(Page)}) }).mount('#app');`;
    },
    generateBundle(_options, bundle) {
      const entry = Object.values(bundle).find(item => item.type === 'chunk' && item.isEntry);
      const css = Object.values(bundle).filter(item => item.fileName.endsWith('.css'));
      this.emitFile({ type: 'asset', fileName: 'index.html', source: `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>南通文件 · 隔离验收</title>${css.map(item => `<link rel="stylesheet" href="/${item.fileName}">`).join('')}</head><body><div id="app"></div><script type="module" src="/${entry.fileName}"></script></body></html>` });
    },
  };
}
export async function startDocumentLibraryPreview({ port = 0 } = {}) {
  const outDir = path.join(output, 'site'); await mkdir(output, { recursive: true });
  await build({ root, configFile: false, publicDir: false, logLevel: 'error', plugins: [fixturePlugin(), vue()],
    build: { outDir, emptyOutDir: true, cssCodeSplit: false, rollupOptions: { input: 'virtual:document-library-preview' } } });
  const fixture = documentFixture();
  const server = createServer(async (request, response) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    const json = (data, status = 200) => { response.statusCode = status; response.setHeader('Content-Type', 'application/json'); response.end(JSON.stringify(status < 400 ? { ok: true, data } : { ok: false, error: data })); };
    try {
      if (url.pathname === '/api/auth/status' && request.method === 'GET') {
        fixture.authReads++;
        return json({ logged_in: true, user: { open_id: 'ou_fixtureDoc', modules: ['knowledge'] }, scope_options: scopes });
      }
      if (url.pathname.startsWith('/api/') && !url.pathname.startsWith('/api/document-library')) return json('隔离验收禁止访问其他接口', 403);
      if (!url.pathname.startsWith('/api/')) {
        const file = url.pathname.startsWith('/assets/') ? path.resolve(outDir, '.' + url.pathname) : path.join(outDir, 'index.html');
        assert(file.startsWith(outDir + path.sep));
        const content = await readFile(file); response.setHeader('Content-Type', file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8'); response.end(content); return;
      }
      const relative = url.pathname.slice('/api/document-library'.length);
      const match = /^\/tables\/([^/]+)(.*)$/.exec(relative);
      const key = match ? decodeURIComponent(match[1]) : '', resource = match?.[2] || '';
      fixture.calls.push({ path: relative, method: request.method, table: key, query: Object.fromEntries(url.searchParams) });
      const selected = fixture.tables.find(item => item.id === key);
      const capabilities = record => ({ edit: selected.write_scopes.includes(record.fields.fldScope), upload: selected.write_scopes.includes(record.fields.fldScope), max_upload_bytes: 104857600 });
      const fail = message => json(message, 503);
      if (request.method === 'GET') {
        if (relative === '/tables') return fixture.flags.tablesFailure ? fail('数据表列表暂时不可用') : json({ items: fixture.tables, can_configure: fixture.canConfigure });
        if (!selected) return json('没有此数据表的权限', 403);
        if (resource === '/schema') return fixture.flags.schemaFailure ? fail('字段读取暂时失败') : json({ table: selected,
          fields: schema.fields.map(field => ({ field_id: field.id, field_name: field.name, type: field.type, property: { options: field.options || [], ...(field.type===11?{multiple:fixture.personMultiple}:{}) }, read_only: field.read_only, required: field.required })),
          display_fields: selected.display_fields, attachment_fields: selected.attachment_fields });
        if (resource === '/records') {
          if (fixture.listDelay) await new Promise(resolve => setTimeout(resolve, fixture.listDelay));
          if (fixture.flags.listFailure) return fail('资料列表暂时读取失败');
          const visible = fixture.records.filter(record => (!url.searchParams.get('q') || record.fields.fldTitle.includes(url.searchParams.get('q'))) && (!url.searchParams.get('scope') || record.fields.fldScope === url.searchParams.get('scope')));
          const start = url.searchParams.get('cursor') === 'page2' ? 2 : 0;
          return json({ items: visible.slice(start, start + 2).map(record => ({ ...record, capabilities: capabilities(record) })), has_more: visible.length > start + 2, cursor: visible.length > start + 2 ? 'page2' : '' });
        }
        if (resource.endsWith('/versions')) return fixture.flags.versionsFailure ? fail('版本记录暂时读取失败') : json({ revision: fixture.records[0].revision, items: [
          { ...fixture.records[0], number: 2, created_at: 1792376400, reason: '更新资料', created_by: '资料管理员' },
          { ...fixture.records[0], attachments: [fixture.records[0].attachments[0]], revision: 'historical1', number: 1, created_at: 1790802000, reason: '首次归档', created_by: '资料管理员' },
        ] });
        if (resource.endsWith('/file')) {
          const image = await readFile(path.join(root, 'public/assets/vnet-logo.png')); response.setHeader('Content-Type', 'image/png'); response.end(image); return;
        }
        const record = fixture.records.find(item => item.record_id === resource.split('/')[2]);
        return record ? fixture.flags.detailFailure ? fail('资料详情暂时读取失败') : json({ ...record, capabilities: capabilities(record) }) : json('资料不存在', 404);
      }
      const chunks = []; for await (const chunk of request) chunks.push(chunk); const body = Buffer.concat(chunks);
      if (relative === '/tables' || match && !resource) {
        if (!fixture.canConfigure) return json('没有配置权限', 403);
        const payload = JSON.parse(body.toString()); fixture.writes.push({ path: relative, body: payload });
        const allowed = new Set(['id','name','base_token','table_id','url','display_fields','attachment_fields','scope_field','fixed_scopes','read_scopes','write_scopes','enabled','expected_revision']);
        assert(Object.keys(payload).every(key => allowed.has(key)), 'Configuration must not serialize client-side capabilities');
        assert(/^[A-Za-z0-9]{8,200}$/.test(payload.base_token)); assert(/^tbl[A-Za-z0-9]+$/.test(payload.table_id));
        assert(payload.fixed_scopes?.length || payload.scope_field); assert(payload.write_scopes.every(scope => payload.read_scopes.includes(scope)));
        const existing = fixture.tables.findIndex(item => item.id === payload.id);
        if (request.method === 'POST' && existing >= 0) return json('配置标识已存在', 409);
        if (request.method === 'PUT' && payload.expected_revision !== fixture.tables[existing]?.revision) return json('表配置版本冲突', 409);
        const result = { ...payload, revision: 'config' + (++fixture.version), capabilities: { create: !!payload.write_scopes.length, configure_tables: true } };
        if (existing >= 0) fixture.tables[existing] = result; else fixture.tables.push(result);
        return json(result);
      }
      if (!selected || fixture.flags.denyWrite || !selected.write_scopes.length) return json('没有编辑此数据表的权限', 403);
      let payload, file;
      if (resource.endsWith('/attachments')) {
        const form = await new Request('http://127.0.0.1/isolated', { method: 'POST', headers: request.headers, body }).formData();
        payload = Object.fromEntries(form); file = form.get('file'); payload.file = { name: file.name, size: file.size };
      } else payload = JSON.parse(body.toString());
      fixture.writes.push({ path: relative, body: copy(payload) });
      assert(payload.operation_id, 'Writes carry one stable operation id'); assert(payload.reason?.trim(), 'Writes carry a real reason');
      if (fixture.receipts.has(payload.operation_id)) return json(fixture.receipts.get(payload.operation_id));
      const id = resource.split('/')[2]; let record = fixture.records.find(item => item.record_id === id);
      if (record && fixture.flags.conflict) { fixture.flags.conflict = false; record.revision = 'r' + (++fixture.version); record.fields.fldCount = 18; return json('资料版本已更新，请读取最新版本', 409); }
      if (record && record.revision !== payload.expected_revision) return json('资料版本冲突', 409);
      if (!record) { record = makeRecord('recCreated' + fixture.applied, '', 'A'); record.fields = {}; record.attachments = []; fixture.records.unshift(record); }
      if (file) record.attachments.push({ attachment_id: 'fileUpload' + fixture.applied, field_id: payload.field_id, name: file.name, size: file.size, type: file.type });
      else { assert(!('fldFormula' in payload.fields)); assert(!('fldUnknown' in payload.fields)); Object.assign(record.fields, payload.fields); }
      record.revision = 'r' + (++fixture.version); record.knowledge_status = { state: fixture.flags.indexFailure ? 'failed' : 'indexed' };
      record.capabilities = capabilities(record); fixture.applied++; fixture.receipts.set(payload.operation_id, copy(record));
      if (fixture.flags.loseReceipt) { fixture.flags.loseReceipt = false; return json('原写入已提交，回执传输未完成', 503); }
      return json(record);
    } catch (error) { json(error instanceof Error ? error.message : '隔离验收失败', 500); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  return { fixture, origin: 'http://127.0.0.1:' + server.address().port, close: () => new Promise((resolve, reject) => { server.close(error => error ? reject(error) : resolve()); server.closeAllConnections(); }) };
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const preview = await startDocumentLibraryPreview({ port: Number(process.env.DOCUMENT_LIBRARY_PREVIEW_PORT || 0) });
  console.log('Synthetic document-library preview: ' + preview.origin);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await preview.close(); process.exit(0); });
}
