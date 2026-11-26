// Isolated development data; never forwards a request to Feishu or production.
// Read-only GETs return {json:{ok:true,data}} shapes that mirror each page's real client.
// Every other path or method returns null so the page's own error state shows instead of a
// fabricated success.
const STAMP='2026-11-03T01:30:00+00:00'; // 北京时间 09:30
const SCOPE='D',SCOPE_LABEL='D楼';
const paged=(all,page,size=20)=>all.slice((page-1)*size,page*size);

// ---------------------------------------------------------------------------
// 发布记录（原有）
const entries=Array.from({length:23},(_,index)=>({
  id:'fixture-release-'+index,version:'ClipFlow_V2_fixture_'+(23-index),patch:500-index,
  published_at:'2026-10-20T10:30:00+00:00',
  items:[{module:index%2?'学练':'更新检查',problem:index%2?'每日答题缺少完成状态':'短暂断网后更新检查无法自动恢复',solution:index%2?'按排班和实际派发任务展示完成情况':'只重试只读查询，并保留脱敏诊断信息'}],
  commits:[{id:'0123456789abcdef',subject:'隔离验收提交，非真实发布记录'}],
}));

// ---------------------------------------------------------------------------
// 每日巡检：状态 / 模板 / 源数据 / 日期工作表（check-inspection-navigation.mjs）
const inspectionTemplate={name:SCOPE+'.xlsx',scope:SCOPE,profession:'电气',
  sheets:[{name:'配电间巡检表',rows:[['项目','读数'],['电压',null]],merges:[]}]};
const inspectionReading={template:inspectionTemplate.name,sheet:'配电间巡检表',cell:'B2',value:'210'};
const inspectionState={duty:{scope:SCOPE,date:'2026-11-03',shift:'白'},can_edit:true,view_scope:SCOPE,
  history_scopes:[SCOPE],history:[],template_version:'v1',completed_rounds:1,round_limit:2,scheduled:false,active:null};
const inspectionDaySheet={scope:SCOPE,date:inspectionState.duty.date,draft_key:'fixture-date',template_version:'v1',
  can_edit_history:true,original_templates:[inspectionTemplate],
  rounds:[{id:'recFixture',round_id:'old-round',shift:'白',round:1,status:'已归档',version:'v1',original_cells:[inspectionReading]}]};

// ---------------------------------------------------------------------------
// 冷源运行（ColdSourceDesk.vue → /api/cold-source-monitor?page=N）
const coldItems=[
  {id:'cold-110',scope:'110',date:'2026-11-03',temperature:19.4,mode:'自动',hint:'湿球温度低于 20 °C，保持常规冷源。'},
  {id:'cold-D',scope:SCOPE,date:'2026-11-03',temperature:null,mode:'',hint:'未采集到湿球温度，请核对采集点。'},
];
const coldHistory=[
  {id:'cold-job-1',row:coldItems[0],created_at:1793679000,status:'sent',targets:[{id:'p1',name:'值班主管',status:'sent'}]},
  {id:'cold-job-2',row:coldItems[1],created_at:1793675400,status:'uncertain',error:'通知回执缺失',
    targets:[{id:'p2',name:'运行值班',status:'pending'}]},
];
const coldPage=(page,size=10)=>({items:coldItems,history:paged(coldHistory,page,size),history_page:page,history_total:coldHistory.length});

// ---------------------------------------------------------------------------
// EOP 场景（check-eop-step-cards-browser.mjs → /api/eop-scenarios[/:id[/image/:token]]）
const eopDetail={id:'s1',revision:'1',editable:false,readonly_fields:[],options:{},
  fields:{'场景名称':'冷却泵切换','场景编号':'EOP-001','专业':'暖通','系统/设备':'冷冻水','原优先级':'P0'},
  steps:[
    {key:'a',text:'确认冷冻泵运行状态与出口压力。',images:['t1','t2']},
    {key:'b',text:'切换备用泵并观察电流回落。',images:['t3']},
    {key:'c',text:'恢复阀门开度并记录读数。',images:[]},
    {key:'d',text:'通知值班主管确认切换结果。',images:[]}],
  images:[
    {token:'t1',name:'泵组全景',url:'/api/eop-scenarios/s1/image/t1'},
    {token:'t2',name:'压力表',url:'/api/eop-scenarios/s1/image/t2'},
    {token:'t3',name:'控制柜',url:'/api/eop-scenarios/s1/image/t3'}],
};
const eopList={updated_at:1,items:[{id:eopDetail.id,fields:eopDetail.fields,has_steps:true,has_images:true}]};
const eopSvg=label=>'<svg xmlns="http://www.w3.org/2000/svg" width="320" height="240">'
  +'<rect width="320" height="240" fill="#dfe8f5"/><text x="16" y="128" font-size="24" fill="#22384f">'
  +label+'</text></svg>';

// ---------------------------------------------------------------------------
// 智航数据拉取概览与执行记录（check-zhihang-data-browser.mjs → /api/zhihang-data[/runs/:id]）
const zhihangAuth={has_token:false,expired:false};
const zhihangTasks=[
  ...['risk 风险排查','change 变更','drill 演练','event 事件'].map(pair=>{
    const [kind,label]=pair.split(' ');
    return {kind,label,notification_owner:['risk','drill'].includes(kind)?'panorama':'lighthouse',
      times:[],enabled:false,confirmed:false,targets:[{label:label+'原表'}],latest:null};
  }),
  {kind:'inspection',label:'巡检',times:['07:00','11:00'],enabled:false,confirmed:false,
    targets:[{label:'原巡检表',url:'https://vnet.feishu.cn/base/sample'}],notification_targets:['原巡检群'],
    latest:null,next_at:null},
];
const zhihangHistory=[{id:'fixture-failed',kind:'inspection',mode:'preview',status:'failed',created_at:STAMP,
  error:'智航 inspection_list：分页返回重复记录，无法确认是否漏页。'}];
for(let index=1;index<=11;index++)zhihangHistory.push({id:'history-'+index,kind:'risk',mode:'scheduled',status:'success',
  created_at:`2026-11-12T08:${String(59-index).padStart(2,'0')}:00+08:00`});
const zhihangDetail=id=>{
  const item=zhihangHistory.find(row=>row.id===id);
  if(!item)return null;
  if(id==='fixture-failed')return {...item,stage:'巡检换页重拉',
    checks:{巡检换页重拉:{每页:50,源端总数:120,已读取:50,当前页:2}},rows:[],total:0,page:1,page_size:50};
  return {...item,counts:{create:2,update:1,delete:0},
    rows:[{楼栋:SCOPE_LABEL,巡检项目:'隔离巡检数据 1'}],total:51,page:1,page_size:50};
};

// ---------------------------------------------------------------------------
// 文档库列表与详情（document-library-maintenance-fixture.mjs → /api/document-library/...）
const documentTable={id:'electrical',name:'电气资料',revision:'r1',fields:[],capabilities:{reindex:true}};
const documentRecord=(index,title,extra={})=>({
  record_id:'recFixture'+index,revision:'r1',
  fields:{fldTitle:title,fldScope:index<SCOPE.length?SCOPE:'E',fldCount:index},
  capabilities:{reindex:true},
  knowledge_status:'ready',knowledge_summary:{total:2,ready:2,cached:2,vectorized:true},
  knowledge_items:[{status:'ready',vectorized:true},{status:'ready',vectorized:true}],
  attachments:[{attachment_id:'att-'+index,name:title+'.pdf',size:1024}],...extra});
const documentRecords=[
  documentRecord(1,'隔离资料 · 配电间巡检记录'),
  documentRecord(2,'隔离资料 · 冷站参数表',{knowledge_status:'partial',
    knowledge_summary:{total:2,ready:1,cached:2,vectorized:false},
    knowledge_items:[{status:'ready',vectorized:true},{status:'text_only',vectorized:false}]}),
  documentRecord(3,'隔离资料 · 应急预案',{knowledge_status:'unsupported',
    knowledge_summary:{total:2,ready:1,cached:2,vectorized:false},
    knowledge_items:[{status:'ready',vectorized:true},{status:'unsupported',vectorized:false}]}),
];
const documentCache={complete:true,updated_at:1792500000,refreshing:false,error:''};

// ---------------------------------------------------------------------------
export function workspaceExtensionFixture({url,method='GET'}){
  const ok=data=>({json:{ok:true,data}});
  const path=url.pathname,search=url.searchParams,
    page=(key='page')=>Math.max(1,Number(search.get(key)||1));
  if(path==='/api/daily-inspection/day-sheet'&&method==='POST')return ok(inspectionDaySheet);
  if(method!=='GET')return null;
  if(path==='/api/workbench-events/options')return ok({user_id:'ou_responsive_fixture',scopes:['A'],levels:['I3'],
    sources:['巡检发现'],specialties:['电气'],defaults:{robot_ids:[],revision:0},robots:[],can_delete:false});
  if(path==='/api/workbench-events')return ok({items:[],total:0});
  if(path==='/api/backend/stats')return ok({message_queue_size:0,qt_queue_size:0,upload_wait_size:0,
    sse_connections:{connections:1},qt_bridge:{connected:true,age_seconds:1}});
  if(path==='/api/backend/perf')return ok({requests:[],slow_requests:[]});
  if(path==='/api/backend/queues')return ok({items:[]});
  if(path==='/api/backend/consistency')return ok({ok:true,counts:{qt_active:1,web_ongoing:1},checked_at:1793679000});
  if(['/api/backend/operation-audits','/api/jobs/recent'].includes(path))return ok({items:[],total:0});
  if(path==='/api/admin/mop-settings')return ok({mop_title_field:'文件名',mop_attachment_field:'文件'});
  if(path==='/api/dingtalk-notices')return ok({revision:1,types:[],scopes:[],robots:[],contacts:[],deliveries:[]});
  if(path==='/api/assistant/voice')return ok({capabilities:{transcription:{available:false},synthesis:{available:false}}});
  if(path==='/api/assistant/notice-panel')return ok({edition:null,scopes:[]});
  if(path==='/api/assistant/intelligence-capabilities')return ok({research:false});
  if(['/api/learning/issues/workbench-questions','/api/learning/issues/pending-questions'].includes(path)){
    return ok({items:[{issue_id:'fixture-issue',question_id:'fixture-question',stem:'隔离验收题目',feedback_count:1}],total:1,page:1,page_size:20});
  }

  // 发布记录
  if(path==='/api/release-journal'){
    const q=search.get('q')||'',module=search.get('module')||'',current=page();
    const filtered=entries.filter(entry=>(!q||JSON.stringify(entry).includes(q))&&(!module||entry.items.some(item=>item.module===module)));
    return ok({items:filtered.slice((current-1)*20,current*20),total:filtered.length,page:current,page_size:20,modules:['更新检查','学练']});
  }
  if(path.startsWith('/api/release-journal/')){
    const entry=entries.find(item=>item.id===path.split('/').at(-1));
    return entry?ok(entry):{status:404,json:{ok:false,error:'记录不存在'}};
  }

  // 每日巡检
  if(path==='/api/daily-inspection'||path==='/api/daily-inspection/status')return ok(inspectionState);
  if(path==='/api/daily-inspection/templates')
    return ok({scope:SCOPE,version:inspectionState.template_version,templates:[inspectionTemplate]});
  if(path==='/api/daily-inspection/source')return ok({scope:SCOPE,devices:[],batch:1});

  // 冷源运行
  if(path==='/api/cold-source-monitor')return ok(coldPage(page()));

  // EOP 场景
  if(path==='/api/eop-scenarios')return ok(eopList);
  const eopImage=/^\/api\/eop-scenarios\/([^/]+)\/image\/([^/]+)$/.exec(path);
  if(eopImage){
    const image=eopDetail.images.find(item=>item.token===decodeURIComponent(eopImage[2]));
    return eopImage[1]===eopDetail.id&&image?{contentType:'image/svg+xml',body:eopSvg(image.name)}:null;
  }
  const eopDetailPath=/^\/api\/eop-scenarios\/([^/]+)$/.exec(path);
  if(eopDetailPath)return decodeURIComponent(eopDetailPath[1])===eopDetail.id?ok(eopDetail):null;

  // 智航数据拉取
  if(path==='/api/zhihang-data/login')return ok({status:'idle'});
  if(path==='/api/zhihang-data'){
    const size=10,current=Math.min(page('history_page'),Math.max(1,Math.ceil(zhihangHistory.length/size)));
    return ok({auth:zhihangAuth,tasks:zhihangTasks,history:paged(zhihangHistory,current,size),
      notification_digests:[{kind:'daily',label:'智航数据同步',times:['08:00'],prepare_at:'00:10',
        modules:['inspection','change','event'],enabled_modules:[],target:{label:'汇总通知群'},latest:{}}],
      history_pagination:{page:current,page_size:size,total:zhihangHistory.length}});
  }
  const zhihangRun=/^\/api\/zhihang-data\/runs\/([^/]+)$/.exec(path);
  if(zhihangRun){
    const detail=zhihangDetail(decodeURIComponent(zhihangRun[1]));
    return detail?ok({...detail,page:page()}):null;
  }

  // 文档库：列表（含分页游标）与单条详情
  if(path==='/api/document-library/tables')
    return ok({items:[documentTable],can_configure:true,scopes:[{value:SCOPE,label:SCOPE_LABEL}],cleanup_pending:0,cleanup_error:''});
  const recordsPath=/^\/api\/document-library\/tables\/([^/]+)\/records$/.exec(path);
  if(recordsPath&&decodeURIComponent(recordsPath[1])===documentTable.id){
    const cursor=Number(search.get('cursor')||0),size=Number(search.get('page_size')||30);
    const all=documentRecords.filter(record=>search.get('scope')?record.fields.fldScope===search.get('scope'):true)
      .filter(record=>!search.get('q')||record.fields.fldTitle.includes(search.get('q')));
    const more=cursor+size<all.length;
    return ok({items:all.slice(cursor,cursor+size),has_more:more,cursor:more?String(cursor+size):'',total:all.length,cache:documentCache});
  }
  const recordPath=/^\/api\/document-library\/tables\/([^/]+)\/records\/([^/]+)$/.exec(path);
  if(recordPath&&decodeURIComponent(recordPath[1])===documentTable.id){
    const record=documentRecords.find(item=>item.record_id===decodeURIComponent(recordPath[2]));
    return record?ok(record):null;
  }

  // 未知接口交由页面自行报错，不伪造成功。
  return null;
}
