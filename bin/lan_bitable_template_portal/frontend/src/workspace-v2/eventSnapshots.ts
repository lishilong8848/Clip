import {requestJson,refreshRemoteSourceAndWait,type Dict} from './client';

// The existing backend refreshes one event month at a time, shared by all widgets.
let refreshQueue:Promise<unknown>=Promise.resolve();
const refreshing=new Map<string,Promise<unknown>>();
export async function eventSnapshot(scope:string,month:string,signal?:AbortSignal):Promise<Dict>{
  const params=new URLSearchParams({scope,month,date_field:'occurrence_time'});
  let data=await requestJson('/api/events/monthly?'+params,{signal,cache:'no-store'});
  if(data.snapshot_exists===false&&!data.config_missing){
    let pending=refreshing.get(month);
    if(!pending){
      pending=refreshQueue.catch(()=>undefined).then(()=>{
        // Closing one widget must not cancel a month refresh shared with another.
        return refreshRemoteSourceAndWait('event','/api/events/refresh?'+params,{scope,month,maxWaitMs:60000});
      }).finally(()=>{if(refreshing.get(month)===pending)refreshing.delete(month);});
      refreshing.set(month,pending);refreshQueue=pending;
    }
    await pending;
    if(signal?.aborted)throw new DOMException('请求已取消。','AbortError');
    data=await requestJson('/api/events/monthly?'+params,{signal,cache:'no-store'});
    if(data.snapshot_exists===false)throw new Error('事件数据同步后仍未准备完成，请重新读取；不会按零条统计。');
  }
  return data;
}
