import { requestJson as originalRequest, invalidateReadCache, type ApiRequestOptions, type ApiClientHooks, type Dict } from '../api/client';
import { trackButtonRequest } from './actionMotion';
import { cachedRead,saveRead,readCacheGeneration } from '../api/readCache';
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
function queued(path:string,options:ApiRequestOptions,hooks:ApiClientHooks):Promise<Dict>{
  return new Promise<Dict>((resolve,reject)=>{
    const cancel=()=>{const index=queue.indexOf(item);if(index>=0){queue.splice(index,1);reject(new DOMException('请求已取消。','AbortError'));}};
    const item:Read={signal:options.signal,reject,start:()=>{
      options.signal?.removeEventListener('abort',cancel);
      checkedRequest(path, options, hooks).then(resolve, reject).finally(() => { running--; pump(); });
    }};
    if(options.signal?.aborted){reject(new DOMException('请求已取消。','AbortError'));return;}
    options.signal?.addEventListener('abort',cancel,{once:true});queue.push(item);
    pump();
  });
}
type Flight={promise:Promise<Dict>;controller:AbortController;users:number};
const flights=new Map<string,Flight>();
const quickReads=new Set(['/api/workbench','/api/repair-management/status','/api/learning/profile','/api/plan-convergence/maintenance/records','/api/plan-convergence/change/records']);
function consume(flight:Flight,signal?:AbortSignal|null):Promise<Dict>{
  flight.users++;
  return new Promise((resolve,reject)=>{
    let done=false;
    function finish(error:unknown,data?:Dict){if(done)return;done=true;signal?.removeEventListener('abort',cancel);flight.users--;if(!flight.users)flight.controller.abort();if(error)reject(error);else resolve(structuredClone(data!));}
    const cancel=()=>finish(new DOMException('请求已取消。','AbortError'));
    if(signal?.aborted){cancel();return;}
    signal?.addEventListener('abort',cancel,{once:true});flight.promise.then(value=>finish(null,value),error=>finish(error));
  });
}
export function requestJson(path: string, options: ApiRequestOptions = {}, hooks: ApiClientHooks = {}): Promise<Dict> {
  if (options.method && !['GET', 'HEAD'].includes(options.method.toUpperCase()) || path.startsWith('/api/assistant/') || path.startsWith('/api/auth/')) return trackButtonRequest(checkedRequest(path, options, hooks));
  if(options.signal?.aborted)return Promise.reject(new DOMException('请求已取消。','AbortError'));
  const url=new URL(path,location.origin);url.searchParams.sort();
  if(quickReads.has(url.pathname)&&(options.fresh||options.cache==='reload'))invalidateReadCache();
  const stamp=readCacheGeneration(),key='workspace-read:'+stamp+':'+url.pathname+url.search;
  if(url.origin!==location.origin||!quickReads.has(url.pathname)||options.method&&options.method.toUpperCase()!=='GET'||options.fresh||['refresh','force','prefetch'].some(key=>url.searchParams.has(key))||['reload','no-store'].includes(options.cache||'')||options.credentials==='omit'||options.headers||Object.keys(hooks).length)return trackButtonRequest(queued(path,options,hooks));
  const hit=cachedRead(key);
  if(hit&&Date.now()-hit.at<30000)return trackButtonRequest(Promise.resolve(hit.data.value));
  let flight=flights.get(key);
  if(!flight||flight.controller.signal.aborted){
    const controller=new AbortController();
    flight={controller,users:0,promise:queued(path,{...options,signal:controller.signal},hooks).then(value=>{
      if(!controller.signal.aborted&&value?.source_snapshot_ready!==false)saveRead(key,{value},stamp);
      return value;
    }).finally(()=>{if(flights.get(key)?.controller===controller)flights.delete(key);})};
    flights.set(key,flight);
  }
  return trackButtonRequest(consume(flight,options.signal));
}
