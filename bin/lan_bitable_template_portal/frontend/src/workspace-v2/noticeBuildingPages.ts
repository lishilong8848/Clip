import type {Dict} from './client';

const lanes=['records','ongoing'] as const;
export async function readNoticeBuildings(query:URLSearchParams,scopes:string[],read:(query:URLSearchParams)=>Promise<Dict>):Promise<Dict>{
  const end=Object.fromEntries(lanes.map(lane=>[lane,Math.max(1,Number(query.get(lane+'_page'))||1)*20]));
  const sources=await Promise.all(scopes.map(async scope=>{
    const rows:Record<string,Dict[]>={records:[],ongoing:[]},more={records:true,ongoing:true};
    for(let page=1;lanes.some(lane=>more[lane]&&rows[lane]!.length<=end[lane]!);page++){
      const active=lanes.filter(lane=>more[lane]&&rows[lane]!.length<=end[lane]!);
      const q=new URLSearchParams(query);q.set('scope',scope);q.set('sections',active.join(','));
      for(const lane of lanes){q.set(lane+'_page',String(page));q.set(lane+'_page_size',String(Math.min(200,end[lane]!+1)));}
      const data=await read(q);
      if(data.source_snapshot_ready===false)throw new Error(scope+'楼通告资料尚未完整读取。');
      for(const lane of active){
        const list=data[lane],total=data[lane+'_pagination']?.total;
        if(!Array.isArray(list)||!Number.isSafeInteger(total)||total<0)throw new Error(scope+'楼通告分页资料未完整返回。');
        if(!list.length&&rows[lane]!.length<total)throw new Error(scope+'楼通告分页读取不完整，请重试。');
        rows[lane]!.push(...list.map(row=>({...row,_request_scope:scope})));
        more[lane]=rows[lane]!.length<total;
      }
    }
    return {rows,more};
  }));
  const result:Dict={source_snapshot_ready:true};
  for(const lane of lanes){
    const unique=new Map<string,Dict>();
    // Interleave sources so a selected building is visible without exhausting another building first.
    for(let index=0;index<Math.max(0,...sources.map(source=>source.rows[lane]!.length));index++)for(const source of sources){
      const row=source.rows[lane]![index];if(!row)continue;
      const id=row.active_item_id||row.record_id||row.id;if(!id)throw new Error('通告记录标识未返回，请重新读取。');
      const key=String(row.work_type||query.get('work_type'))+':'+id;if(!unique.has(key))unique.set(key,row);
    }
    const rows=[...unique.values()],partial=sources.some(source=>source.more[lane]);
    result[lane]=rows.slice(end[lane]!-20,end[lane]);
    result[lane+'_pagination']={total:rows.length,partial};
  }
  return result;
}
