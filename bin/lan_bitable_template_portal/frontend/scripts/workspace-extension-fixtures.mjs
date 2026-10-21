// Isolated development data; never forwards a request to Feishu or production.
const entries=Array.from({length:23},(_,index)=>({
  id:'fixture-release-'+index,version:'ClipFlow_V2_fixture_'+(23-index),patch:500-index,
  published_at:'2026-10-20T10:30:00+00:00',
  items:[{module:index%2?'学练':'更新检查',problem:index%2?'每日答题缺少完成状态':'短暂断网后更新检查无法自动恢复',solution:index%2?'按排班和实际派发任务展示完成情况':'只重试只读查询，并保留脱敏诊断信息'}],
  commits:[{id:'0123456789abcdef',subject:'隔离验收提交，非真实发布记录'}],
}));
export function workspaceExtensionFixture({url,method='GET'}){
  const ok=data=>({json:{ok:true,data}});
  if(method!=='GET')return null;
  if(url.pathname==='/api/document-library/tables')return ok({items:[],can_configure:true});
  if(url.pathname==='/api/release-journal'){
    const q=url.searchParams.get('q')||'',module=url.searchParams.get('module')||'',page=Number(url.searchParams.get('page')||1);
    const filtered=entries.filter(entry=>(!q||JSON.stringify(entry).includes(q))&&(!module||entry.items.some(item=>item.module===module)));
    return ok({items:filtered.slice((page-1)*20,page*20),total:filtered.length,page,page_size:20,modules:['更新检查','学练']});
  }
  if(url.pathname.startsWith('/api/release-journal/')){
    const entry=entries.find(item=>item.id===url.pathname.split('/').at(-1));
    return entry?ok(entry):{status:404,json:{ok:false,error:'记录不存在'}};
  }
  return null;
}
