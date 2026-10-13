import { requestJson as originalRequest, invalidateReadCache, type ApiRequestOptions, type ApiClientHooks, type Dict } from '../api/client';
import { trackButtonRequest } from './actionMotion';
export * from '../api/client';
type Read = { start: () => void; signal?: AbortSignal | null; reject: (error: unknown) => void };
const queue: Read[] = [];
let running = 0;
let lastDenied=0;
function checkedRequest(path:string,options:ApiRequestOptions,hooks:ApiClientHooks){
  return originalRequest(path,options,hooks).catch(error=>{
    if(error.status===403){invalidateReadCache();if(Date.now()-lastDenied>5000){lastDenied=Date.now();window.dispatchEvent(new Event('workspace-permissions-stale'));}}
    throw error;
  });
}
function pump() {
  while (running < 3 && queue.length) {
    const item = queue.shift()!;
    if (item.signal?.aborted) { item.reject(new DOMException('Aborted', 'AbortError')); continue; }
    running++; item.start();
  }
}
export function requestJson(path: string, options: ApiRequestOptions = {}, hooks: ApiClientHooks = {}): Promise<Dict> {
  if (options.method && !['GET', 'HEAD'].includes(options.method.toUpperCase()) || path.startsWith('/api/assistant/')) return trackButtonRequest(checkedRequest(path, options, hooks));
  return trackButtonRequest(new Promise<Dict>((resolve, reject) => {
    queue.push({ signal: options.signal, reject, start: () => {
      checkedRequest(path, options, hooks).then(resolve, reject).finally(() => { running--; pump(); });
    } });
    pump();
  }));
}
