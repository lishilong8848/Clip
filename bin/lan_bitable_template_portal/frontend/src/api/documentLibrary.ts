import { ApiError, requestJson, uploadJson } from './client';

export type LibraryValue = unknown;
export type LibraryFields = Record<string, LibraryValue>;
export type LibraryOption = { id?: string; value?: string; name?: string; label?: string } | string;
export type LibraryField = {
  id: string; name: string; type: string | number; options?: LibraryOption[];
  read_only: boolean; required?: boolean; multiple?: boolean; read_only_reason?: string;
};
export type LibraryCapabilities = {
  create?: boolean; edit?: boolean; upload?: boolean; configure_tables?: boolean; reindex?: boolean;
  max_upload_bytes?: number;
};
export type LibraryTable = {
  key: string; name: string; app_token?: string; table_id?: string; url?: string; revision?: string;
  display_fields?: string[]; attachment_fields?: string[];
  scope_field?: string; fixed_scopes?: string[]; read_scopes?: string[]; write_scopes?: string[];
  scopes?: string[]; enabled?: boolean; capabilities?: LibraryCapabilities;
};
export type LibraryAttachment = {
  id: string; field_id: string; name: string; size?: number; mime_type?: string;
  version?: number; created_at?: string; created_by?: string; current?: boolean;
};
export type LibraryRecord = {
  record_id: string; fields: LibraryFields; revision: string;
  attachments: LibraryAttachment[]; capabilities: LibraryCapabilities;
  knowledge_status?: string | { state: string; error?: string };
  knowledge_summary?: { total: number; ready: number; cached: number; vectorized: boolean };
  knowledge_items?: Array<{ kind: string; name: string; status: string; error?: string; field_id?: string; attachment_id?: string;
    cached?: boolean; vectorized?: boolean; stage?: 'prepare' | 'index'; error_code?: string }>;
};
export type LibraryCache = { complete: boolean; updated_at: number; refreshing: boolean; error: string };
export type LibrarySchema = {
  fields: LibraryField[]; display_fields: string[]; attachment_fields: string[];
  cache?: LibraryCache;
};
export type LibraryPreview = {
  url: string; source: LibraryTable | null;
  tables: Array<{ table_id: string; name: string }>; fields: LibraryField[];
};
export type LibraryTables = {
  items: LibraryTable[]; capabilities?: LibraryCapabilities; can_configure?: boolean;
  actor?: { can_configure?: boolean }; scopes?: Array<{ value: string; label: string }>;
  cleanup_pending?: number; cleanup_error?: string;
};
export type LibraryList = { items: LibraryRecord[]; has_more: boolean; cursor: string; total?: number; cache?: LibraryCache };
export type LibraryMutation = { fields: LibraryFields; operation_id: string; reason: string; expected_revision?: string };
export type LibraryLocalOperation = { operation_id: string; expected_revision: string };
export type LibraryFilter = { field: string; op: string; value?: string | number | boolean | null };
export type LibraryReindex = LibraryLocalOperation & ({ record_ids: string[] } | { all: true; q: string; scope: string; filters?: LibraryFilter[] });

const root = '/api/document-library';
function endpoint(path: string, tableKey?: string, params: Record<string, string | number | undefined> = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) if (value !== undefined && value !== '') query.set(key, String(value));
  return root + (tableKey ? '/tables/' + encodeURIComponent(tableKey) : '') + path + (query.size ? '?' + query.toString() : '');
}
function arrayResult<T>(data: Record<string, unknown>, key: string): T[] {
  if (!Array.isArray(data[key])) throw new ApiError('服务返回的数据不完整，请重新读取。', { status: 502 });
  return data[key] as T[];
}
function attachmentResult(file: Record<string, any>, version?: Record<string, any>): LibraryAttachment {
  return { id: String(file.attachment_id || file.id || ''), field_id: file.field_id, name: file.name,
    size: file.size, mime_type: file.type || file.mime_type, version: version?.number ?? file.version,
    created_at: typeof version?.created_at === 'number' ? new Date(version.created_at * 1000).toLocaleString('zh-CN') : version?.created_at || file.created_at,
    created_by: version?.created_by || version?.actor || file.created_by, current: file.current };
}
function tableResult(source: Record<string, any>): LibraryTable {
  if (!(source.id || source.key) || !source.name) throw new ApiError('数据表配置不完整，请重新读取。', { status: 502 });
  return { ...source, key: String(source.id || source.key), name: String(source.name), app_token: source.base_token || source.app_token,
    scopes: source.read_scopes || source.scopes || [] };
}
function fieldResults(data: Record<string, any>): LibraryField[] {
  const fields = arrayResult<Record<string, any>>(data, 'fields').map(field => ({ ...field,
    id: String(field.id || field.field_id || ''), name: String(field.name || field.field_name || ''),
    options: field.options || field.property?.options || [], read_only: field.read_only === true || field.is_computed === true,
    multiple: field.property?.multiple ?? field.multiple,
  } as LibraryField));
  if (fields.some(field => !field.id || !field.name)) throw new ApiError('字段结构不完整，请重新读取。', { status: 502 });
  return fields;
}
function recordResult(data: Record<string, any>): LibraryRecord {
  const record = data.record || data;
  if (typeof record.record_id !== 'string' || !record.record_id || !record.fields || typeof record.fields !== 'object'
      || Array.isArray(record.fields) || typeof record.revision !== 'string' || !record.revision
      || record.attachments !== undefined && !Array.isArray(record.attachments))
    throw new ApiError('保存或读取回执不完整，请核验原操作，当前输入已保留。', { status: 502 });
  return { ...record, attachments: (record.attachments || []).map((file: Record<string, any>) => attachmentResult(file)), capabilities: record.capabilities || {},
    knowledge_status: record.knowledge_status ?? data.knowledge_status };
}

export const documentLibraryApi = {
  async preview(url: string, tableId?: string, signal?: AbortSignal): Promise<LibraryPreview> {
    const data = await requestJson(endpoint('/preview'), {
      method: 'POST', body: JSON.stringify({ url, ...(tableId ? { table_id: tableId } : {}) }), signal,
    });
    const tables = arrayResult<{ table_id: string; name: string }>(data, 'tables');
    if (typeof data.url !== 'string' || !data.url || tables.some(table => !table.table_id || !table.name)
        || data.source !== null && (!data.source?.base_token || !data.source?.table_id))
      throw new ApiError('数据表读取结果不完整，请重新读取。', { status: 502 });
    const fields = fieldResults(data);
    if (data.source === null ? !tables.length : !fields.length || !tables.some(table => table.table_id === data.source.table_id))
      throw new ApiError('数据表读取结果不完整，请重新读取。', { status: 502 });
    return { url: data.url, source: data.source === null ? null : tableResult(data.source), tables, fields };
  },
  async context(signal?: AbortSignal): Promise<{ scopes: Array<{ value: string; label: string }> }> {
    const data = await requestJson('/api/auth/status', { signal, fresh: true });
    const scopes = Array.isArray(data.scope_options) ? data.scope_options.filter((option: Record<string, unknown>) => ['A', 'B', 'C', 'D', 'E', 'H', '110'].includes(String(option.value))) : [];
    return { scopes };
  },
  async tables(signal?: AbortSignal): Promise<LibraryTables> {
    const data = await requestJson(endpoint('/tables'), { signal, fresh: true });
    return { ...data, items: arrayResult<Record<string, any>>(data, 'items').map(tableResult) };
  },
  async schema(key: string, signal?: AbortSignal, refresh = false): Promise<LibrarySchema> {
    const data = await requestJson(endpoint('/schema', key, { refresh: refresh ? 1 : undefined }), { signal, fresh: true });
    return { fields: fieldResults(data), cache: data.cache,
      display_fields: arrayResult<string>(data, 'display_fields'), attachment_fields: arrayResult<string>(data, 'attachment_fields') };
  },
  async records(key: string, params: { scope?: string; q?: string; cursor?: string; page_size?: number; filters?: LibraryFilter[] }, signal?: AbortSignal): Promise<LibraryList> {
    const { filters, ...query } = params;
    const data = await requestJson(endpoint('/records', key, { ...query, filters: filters?.length ? JSON.stringify(filters) : undefined }), { signal, fresh: true });
    if (typeof data.has_more !== 'boolean') throw new ApiError('记录分页状态不完整，请重新读取。', { status: 502 });
    if (data.has_more && !data.cursor) throw new ApiError('下一页游标不完整，请重新读取。', { status: 502 });
    return { items: arrayResult<Record<string, any>>(data, 'items').map(recordResult), has_more: data.has_more, cursor: String(data.cursor || ''),
      total: typeof data.total === 'number' ? data.total : undefined, cache: data.cache };
  },
  async record(key: string, id: string, signal?: AbortSignal, refresh = false): Promise<LibraryRecord> {
    return recordResult(await requestJson(endpoint('/records/' + encodeURIComponent(id), key, { refresh: refresh ? 1 : undefined }), { signal, fresh: true }));
  },
  async reindex(key: string, payload: LibraryReindex, signal?: AbortSignal): Promise<{ operation_id: string; status: 'queued'; records: number; items: number }> {
    const data = await requestJson(endpoint('/reindex', key), { method: 'POST', body: JSON.stringify(payload), signal });
    if (data.operation_id !== payload.operation_id || data.status !== 'queued' || !Number.isInteger(data.records) || data.records < 0 || !Number.isInteger(data.items) || data.items < 0)
      throw new ApiError('重新向量化回执不完整，请按原操作重试。', { status: 502 });
    return data as { operation_id: string; status: 'queued'; records: number; items: number };
  },
  async remove(key: string, payload: LibraryLocalOperation, signal?: AbortSignal): Promise<{ operation_id: string; removed: true; cleanup_status: 'pending' | 'completed' }> {
    const data = await requestJson(endpoint('', key), { method: 'DELETE', body: JSON.stringify(payload), signal });
    if (data.operation_id !== payload.operation_id || data.removed !== true || !['pending', 'completed'].includes(data.cleanup_status))
      throw new ApiError('移除接入回执不完整，请按原操作重试。', { status: 502 });
    return data as { operation_id: string; removed: true; cleanup_status: 'pending' | 'completed' };
  },
  async save(key: string, id: string | undefined, payload: LibraryMutation): Promise<LibraryRecord> {
    const data = await requestJson(endpoint('/records' + (id ? '/' + encodeURIComponent(id) : ''), key), {
      method: id ? 'PATCH' : 'POST', body: JSON.stringify(payload), timeoutMs: 90_000,
    });
    const result = recordResult(data);
    if (id && result.record_id !== id || data.operation_id && data.operation_id !== payload.operation_id)
      throw new ApiError('操作回执与当前资料不一致，请核验原操作。', { status: 502 });
    return result;
  },
  async upload(key: string, id: string, data: FormData, onProgress: (percent: number) => void): Promise<LibraryRecord> {
    const receipt = await uploadJson(endpoint('/records/' + encodeURIComponent(id) + '/attachments', key), data, {
      onProgress: event => onProgress(event.total ? Math.round(event.loaded / event.total * 100) : 0),
    });
    const result = recordResult(receipt);
    if (result.record_id !== id || receipt.operation_id && receipt.operation_id !== data.get('operation_id'))
      throw new ApiError('附件操作回执不一致，请核验原操作。', { status: 502 });
    return result;
  },
  async versions(key: string, id: string, signal?: AbortSignal): Promise<LibraryAttachment[]> {
    const data = await requestJson(endpoint('/records/' + encodeURIComponent(id) + '/versions', key), { signal, fresh: true });
    return arrayResult<Record<string, any>>(data, 'items').flatMap(version => Array.isArray(version.attachments)
      ? version.attachments.map((file: Record<string, any>) => ({ ...attachmentResult(file, version), current: version.revision === data.revision }))
      : [attachmentResult(version)]);
  },
  fileUrl(key: string, id: string, attachment: LibraryAttachment) {
    return endpoint('/records/' + encodeURIComponent(id) + '/attachments/' + encodeURIComponent(attachment.id) + '/file', key,
      { version: attachment.version });
  },
  async configure(table: LibraryTable, existing: boolean): Promise<LibraryTable> {
    const payload = { id: table.key, name: table.name, base_token: table.app_token, table_id: table.table_id, url: table.url || '',
      display_fields: table.display_fields || [], attachment_fields: table.attachment_fields || [], scope_field: table.scope_field || '',
      fixed_scopes: table.fixed_scopes || [], read_scopes: table.read_scopes || [], write_scopes: table.write_scopes || [], enabled: table.enabled !== false,
      ...(existing ? { expected_revision: table.revision } : {}),
    };
    const data = await requestJson(endpoint('/tables' + (existing ? '/' + encodeURIComponent(table.key) : '')), {
      method: existing ? 'PUT' : 'POST', body: JSON.stringify(payload),
    });
    const result = data.table || data;
    if (typeof result.revision !== 'string' || !result.revision)
      throw new ApiError('配置回执缺少修订信息，请重新读取后核验。', { status: 502 });
    return tableResult(result);
  },
};

export function fieldKind(field: LibraryField): string {
  if (typeof field.type === 'number') return ({ 1: 'text', 2: 'number', 3: 'select', 4: 'multi', 5: 'date', 7: 'bool', 11: 'person', 15: 'url', 17: 'files' } as Record<number, string>)[field.type] || 'readonly';
  return ({ multiline: 'text', single_select: 'select', multi_select: 'multi', boolean: 'bool', attachment: 'files', checkbox: 'bool' } as Record<string, string>)[field.type] || field.type;
}
export function fieldValue(value: LibraryValue): any {
  return value && typeof value === 'object' && 'type' in value && 'value' in value ? value.value : value;
}
export function writableField(field: LibraryField): boolean {
  return !field.read_only && ['text', 'number', 'select', 'multi', 'date', 'person', 'url', 'bool'].includes(fieldKind(field));
}
export function valueText(value: LibraryValue): string {
  const raw = fieldValue(value);
  if (raw === null || raw === undefined || raw === '') return '未填写';
  if (typeof raw === 'boolean') return raw ? '是' : '否';
  if (Array.isArray(raw)) return raw.length ? raw.map(valueText).join('、') : '未填写';
  if (typeof raw === 'object') return String(raw.name || raw.text || raw.label || raw.link || raw.id || JSON.stringify(raw));
  return String(raw);
}
export function newLibraryOperationId(): string {
  if (typeof window.crypto.randomUUID === 'function') return window.crypto.randomUUID();
  const bytes = window.crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export function cloneLibraryValue<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}
