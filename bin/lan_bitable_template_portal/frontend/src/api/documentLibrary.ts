import { ApiError, requestJson, uploadJson } from './client';

export type LibraryValue = unknown;
export type LibraryFields = Record<string, LibraryValue>;
export type LibraryOption = { id?: string; value?: string; name?: string; label?: string } | string;
export type LibraryField = {
  id: string; name: string; type: string | number; options?: LibraryOption[];
  read_only: boolean; required?: boolean; multiple?: boolean; read_only_reason?: string;
};
export type LibraryCapabilities = {
  create?: boolean; edit?: boolean; upload?: boolean; configure_tables?: boolean;
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
};
export type LibrarySchema = {
  fields: LibraryField[]; display_fields: string[]; attachment_fields: string[];
};
export type LibraryTables = {
  items: LibraryTable[]; capabilities?: LibraryCapabilities; can_configure?: boolean;
  actor?: { can_configure?: boolean }; scopes?: Array<{ value: string; label: string }>;
};
export type LibraryList = { items: LibraryRecord[]; has_more: boolean; cursor: string };
export type LibraryMutation = { fields: LibraryFields; operation_id: string; reason: string; expected_revision?: string };

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
  async context(signal?: AbortSignal): Promise<{ scopes: Array<{ value: string; label: string }> }> {
    const data = await requestJson('/api/auth/status', { signal, fresh: true });
    const scopes = Array.isArray(data.scope_options) ? data.scope_options.filter((option: Record<string, unknown>) => ['A', 'B', 'C', 'D', 'E', 'H', '110'].includes(String(option.value))) : [];
    return { scopes };
  },
  async tables(signal?: AbortSignal): Promise<LibraryTables> {
    const data = await requestJson(endpoint('/tables'), { signal, fresh: true });
    return { ...data, items: arrayResult<Record<string, any>>(data, 'items').map(tableResult) };
  },
  async schema(key: string, signal?: AbortSignal): Promise<LibrarySchema> {
    const data = await requestJson(endpoint('/schema', key), { signal, fresh: true });
    const fields = arrayResult<Record<string, any>>(data, 'fields').map(field => ({ ...field,
      id: String(field.id || field.field_id || ''), name: String(field.name || field.field_name || ''),
      options: field.options || field.property?.options || [], read_only: field.read_only === true || field.is_computed === true,
      multiple: field.property?.multiple ?? field.multiple,
    } as LibraryField));
    if (fields.some(field => !field.id || !field.name)) throw new ApiError('字段结构不完整，请重新读取。', { status: 502 });
    return { fields,
      display_fields: arrayResult<string>(data, 'display_fields'), attachment_fields: arrayResult<string>(data, 'attachment_fields') };
  },
  async records(key: string, params: { scope?: string; q?: string; cursor?: string; page_size?: number }, signal?: AbortSignal): Promise<LibraryList> {
    const data = await requestJson(endpoint('/records', key, params), { signal, fresh: true });
    if (typeof data.has_more !== 'boolean') throw new ApiError('记录分页状态不完整，请重新读取。', { status: 502 });
    if (data.has_more && !data.cursor) throw new ApiError('下一页游标不完整，请重新读取。', { status: 502 });
    return { items: arrayResult<Record<string, any>>(data, 'items').map(recordResult), has_more: data.has_more, cursor: String(data.cursor || '') };
  },
  async record(key: string, id: string, signal?: AbortSignal): Promise<LibraryRecord> {
    return recordResult(await requestJson(endpoint('/records/' + encodeURIComponent(id), key), { signal, fresh: true }));
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
  return window.crypto.randomUUID();
}
export function cloneLibraryValue<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}
