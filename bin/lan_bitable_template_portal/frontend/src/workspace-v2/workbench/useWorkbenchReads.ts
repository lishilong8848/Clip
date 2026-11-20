import { onActivated, onBeforeUnmount, onDeactivated, ref, shallowRef, watch, type Ref } from 'vue';
import { JSON_REQUEST_TIMEOUT_MS, requestJson, type Dict } from '../client';
import { layoutKey } from '../dashboard/layout';
import type { Source, SourceId, SourceResult } from './sources';

type State = { status:'loading'|'ok'|'stale'|'down'|'deny'; result?:SourceResult; pages?:number };
const statusOf = (error: unknown) => (error as {status?:number})?.status;

export async function readWorkbenchRequest(path: string, signal: AbortSignal) {
  const read=()=>requestJson(path,{signal,cache:'no-store',timeoutMs:18000,queueTimeoutMs:JSON_REQUEST_TIMEOUT_MS,background:true});
  try {
    return await read();
  } catch (error) {
    if (signal.aborted||![0,408,429,502,503,504].includes(statusOf(error)??-1)) throw error;
    await new Promise(resolve=>setTimeout(resolve,700));
    signal.throwIfAborted();
    return read();
  }
}

export function useWorkbenchReads(context: Ref<string>, sources: Ref<Source[]>) {
  const states = shallowRef<Partial<Record<SourceId,State>>>({});
  const active = new Map<SourceId,AbortController>();
  const wanted = new Set<SourceId>();
  const checkedAt = new Map<SourceId,number>();
  const dirty = new Set<SourceId>();
  let visible=true;
  function reset() {
    for (const controller of active.values()) controller.abort();
    active.clear();
    wanted.clear();
    checkedAt.clear();
    dirty.clear();
    states.value = {};
  }
  function update(id: SourceId,state: State) { states.value = {...states.value,[id]:state}; }
  async function load(id: SourceId, retry=false, more=false) {
    const source=sources.value.find(item=>item.id===id);
    if (!source || !visible || (!retry&&states.value[id]) || active.has(id)) return;
    wanted.add(id);
    dirty.delete(id);
    const controller=new AbortController(), key=context.value, old=states.value[id], previous=old?.result;
    const pages=(old?.pages ?? 1) + (more ? 1 : 0);
    active.set(id,controller);
    update(id,{status:'loading',result:previous,pages:old?.pages});
    const current=()=>!controller.signal.aborted&&active.get(id)===controller&&key===context.value;
    try {
      const result=await source.load(controller.signal,pages,partial=>{
        if (current()&&((partial.rows.length&&!previous?.rows.length)||partial.restricted)) {
          update(id,{status:'loading',result:partial,pages:old?.pages});
        }
      });
      if (current()) {
        const keepPrevious = result.delayed && !result.restricted && !result.rows.length && !!previous?.rows.length;
        update(id,{status:result.delayed?'stale':'ok',result:keepPrevious?previous:result,pages:keepPrevious?old?.pages:pages});
      }
    } catch (error) {
      if (!current()) return;
      if ([401,403].includes(statusOf(error)||0)) update(id,{status:'deny'});
      else update(id,previous?{status:'stale',result:previous,pages:old?.pages}:{status:'down'});
    } finally {
      if (active.get(id)===controller) {
        active.delete(id);
        checkedAt.set(id,Date.now());
        if (dirty.has(id)&&visible&&!document.hidden) void load(id,true);
      }
    }
  }
  function refresh(ids?:SourceId[]) {
    for (const id of ids||wanted) if (wanted.has(id)) {
      dirty.add(id);
      if (visible&&!document.hidden) void load(id,true);
    }
  }
  const refreshAll=()=>refresh();
  function refreshExpired() {
    if (!visible||document.hidden) return;
    for (const id of wanted) {
      const status=states.value[id]?.status;
      const age=Date.now()-(checkedAt.get(id)||0);
      if (dirty.has(id)||(status!=='deny'&&age>=(status==='ok'?120000:30000))) void load(id,true);
    }
  }
  watch(context,reset,{flush:'sync'});
  onActivated(()=>{visible=true;refreshExpired();});
  onDeactivated(()=>{visible=false;});
  window.addEventListener('clipflow-business-changed',refreshAll);
  document.addEventListener('visibilitychange',refreshExpired);
  onBeforeUnmount(()=>{
    reset();
    window.removeEventListener('clipflow-business-changed',refreshAll);
    document.removeEventListener('visibilitychange',refreshExpired);
  });
  return {states,load,refresh,refreshExpired};
}

export function useReadOnlyLayout(user: () => Dict, context: Ref<string>) {
  const query=window.matchMedia('(max-width:760px)');
  const device=ref(query.matches?'mobile':'desktop');
  const layout=shallowRef<unknown>(null), state=ref<'loading'|'ok'|'stale'|'deny'>('loading');
  let active: AbortController|undefined;
  async function load() {
    active?.abort();
    const actor=user(), owner=String(actor.open_id||''), key=context.value, size=device.value;
    layout.value=null;
    if (!owner || ['guest','temporary'].includes(actor.role) || actor.impersonation) {
      state.value='ok';
      return;
    }
    const controller=active=new AbortController();
    const current=()=>!controller.signal.aborted&&key===context.value&&size===device.value;
    let fallback: unknown=null;
    try {
      const cache=localStorage.getItem('clipflow:account-layout:v1:'+owner+':'+size+':workspace');
      if (cache) fallback=JSON.parse(cache)?.value;
      else if (size==='desktop') fallback=JSON.parse(localStorage.getItem(layoutKey(actor))||'null');
    } catch { /* Layout preferences are optional. */ }
    state.value='loading';
    try {
      const result=await requestJson('/api/workspace-layouts/'+size+'/workspace',{cache:'no-store',signal:controller.signal,timeoutMs:10000,background:true});
      if (!current()) return;
      if (!Number.isSafeInteger(result.revision) || result.revision<0 || (result.revision===0?result.value!==null:!Array.isArray(result.value?.widgets))) {
        throw new Error('账号布局响应不完整');
      }
      layout.value=result.revision===0?fallback:result.value;
      state.value='ok';
    } catch (error) {
      if (!current()) return;
      const denied=[401,403].includes(statusOf(error)||0);
      if (!denied) layout.value=fallback;
      state.value=denied?'deny':'stale';
    }
  }
  const resize=()=>{device.value=query.matches?'mobile':'desktop';};
  query.addEventListener('change',resize);
  watch([context,device],()=>void load(),{immediate:true,flush:'sync'});
  onBeforeUnmount(()=>{active?.abort();query.removeEventListener('change',resize);});
  return {layout,state,load};
}
