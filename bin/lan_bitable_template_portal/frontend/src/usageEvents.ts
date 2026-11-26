import catalog from '../../usage_pages.json';

export type UsageClient = 'web_v2' | 'web_legacy' | 'web_standalone';
type Receipt = {event_id: string; client: UsageClient; module: string; page: string; occurred_at: number};
const pages: Record<string, {pages: Record<string,string>; public_pages?: string[]}> = catalog;
const own = (object: object, key: string) => Object.prototype.hasOwnProperty.call(object, key);
const prefix = 'clipflow:usage:v1:';
const memory = new Map<string, Receipt>();
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const clients = new Set(['web_v2', 'web_legacy', 'web_standalone']);

export function knownPage(module: string, page: string) {
  return own(pages, module) && own(pages[module].pages, page);
}

// Validate disk data as strictly as fresh events. Never forward extra properties.
function valid(event: Receipt | null): event is Receipt {
  return !!event && clients.has(event.client) && typeof event.module === 'string' &&
    typeof event.page === 'string' && knownPage(event.module, event.page) &&
    typeof event.event_id === 'string' && uuid.test(event.event_id) && Number.isSafeInteger(event.occurred_at) &&
    Object.keys(event).length === 5;
}

function newId() {
  // randomUUID is unavailable on HTTP intranets. getRandomValues works there.
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, b => b.toString(16).padStart(2,'0')).join('');
  return [hex.slice(0,8),hex.slice(8,12),hex.slice(12,16),hex.slice(16,20),hex.slice(20)].join('-');
}

let database: Promise<IDBDatabase> | undefined;
function openDatabase() {
  if (!database) {
    database = new Promise<IDBDatabase>((resolve,reject) => {
      const request = indexedDB.open('clipflow-page-usage', 1);
      request.onupgradeneeded = () => request.result.createObjectStore('pending', {keyPath:'event_id'});
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    }).catch(error => { database = undefined; throw error; });
  }
  return database;
}
async function disk<T>(mode: IDBTransactionMode, action: (store: IDBObjectStore) => IDBRequest<T>) {
  const db = await openDatabase();
  return new Promise<T>((resolve,reject) => {
    const tx = db.transaction('pending', mode);
    const result = action(tx.objectStore('pending'));
    tx.oncomplete = () => resolve(result.result);
    tx.onabort = tx.onerror = () => reject(tx.error);
  });
}

let timer: ReturnType<typeof setTimeout> | undefined;
let running = false;
let requested = false;
let access: 'public' | 'guest' | 'member' = 'public';
let started = false;
let retryDelay = 5000;
function schedule(delay: number) {
  clearTimeout(timer);
  timer = setTimeout(() => { void flush(); }, delay);
}

export function setUsageAccess(next: typeof access) {
  if (next === access) return;
  access = next;
  schedule(0);
}

export function startUsage() {
  if (started) return;
  started = true;
  window.addEventListener('online', () => schedule(0));
  window.addEventListener('storage', event => {
    if (event.key?.startsWith(prefix) && event.newValue) schedule(100);
  });
  window.addEventListener('pageshow', () => schedule(0));
  schedule(0);
}

export function recordPage(client: UsageClient, module: string, page: string) {
  if (!clients.has(client) || !knownPage(module,page)) return;
  try {
    const event = {event_id:newId(), client, module, page, occurred_at:Date.now()};
    try {
      // One key per UUID prevents an acknowledgement in another tab from overwriting new arrivals.
      localStorage.setItem(prefix + event.event_id, JSON.stringify(event));
    } catch {
      memory.set(event.event_id,event);
      void disk('readwrite', store => store.put(event)).then(() => memory.delete(event.event_id)).catch(() => {});
    }
    startUsage();
    schedule(50);
  } catch { /* Metrics must not interrupt the business page. */ }
}

async function flush() {
  if (running) { requested = true; return; }
  requested = false;
  running = true;
  let failed = false;
  try {
    const pending = new Map(memory);
    const localIds = new Set<string>();
    const diskIds = new Set<string>();
    try {
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (!key?.startsWith(prefix)) continue;
        try {
          const event = JSON.parse(localStorage.getItem(key) || 'null');
          if (valid(event) && key === prefix + event.event_id) {
            pending.set(event.event_id,event);
            localIds.add(event.event_id);
          }
        } catch { /* Ignore a single corrupt key. */ }
      }
    } catch { /* IndexedDB also works with some restricted WebViews. */ }
    try {
      for (const event of await disk('readonly', store => store.getAll())) {
        if (valid(event)) { pending.set(event.event_id,event); diskIds.add(event.event_id); }
      }
    } catch { /* localStorage and in-memory receipts can still be delivered. */ }

    // A private receipt left by a previous session must not block anonymous login/signature visits.
    for (const tier of ['public','guest','member']) {
      const group = [...pending.values()].filter(event => {
        if (pages[event.module].public_pages?.includes(event.page)) return tier === 'public';
        return event.module === 'cabinet' ? tier === 'guest' : tier === 'member';
      });
      for (let i = 0; i < group.length; i += 50) {
        if (tier === 'member' && access !== 'member' || tier === 'guest' && access === 'public') break;
        const events = group.slice(i,i+50);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        try {
          const response = await fetch('/api/usage/events', {
            method:'POST', credentials:'same-origin', mode:'same-origin', redirect:'error', referrerPolicy:'origin',
            headers:{'Content-Type':'application/json'}, body:JSON.stringify({events}), signal:controller.signal,
          });
          const body = response.ok && await response.json();
          if (body?.ok !== true || body.data?.accepted !== events.length) { failed = true; break; }
          for (const {event_id} of events) {
            if (localIds.has(event_id)) localStorage.removeItem(prefix + event_id);
            if (diskIds.has(event_id)) await disk('readwrite', store => store.delete(event_id));
            memory.delete(event_id);
          }
        } catch { failed = true; break; }
        finally { clearTimeout(timeout); }
      }
    }
    // A visit may have arrived while this batch was in flight.
    if (requested && !failed) schedule(50);
  } catch { failed = true; }
  finally {
    running = false;
    if (failed) { schedule(retryDelay); retryDelay = Math.min(300000, retryDelay * 2); }
    else retryDelay = 5000;
  }
}
