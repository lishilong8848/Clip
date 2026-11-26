import {collabFixture} from '../responsive-audit-collab-fixtures.mjs';

const ok = data => ({status:200,json:{ok:true,data}});
const day = () => new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
const sample = {id:'audit-image',name:'冷却水泵巡查示意.svg',kind:'question',mime:'image/svg+xml',url:'/api/learning/attachments/audit-image'};
const imageBody = '<svg xmlns="http://www.w3.org/2000/svg" width="460" height="180"><rect width="100%" height="100%" fill="#e4f3ef"/><path d="M20 110h140V65h160v45h120" stroke="#3a836c" fill="none" stroke-width="9"/><text x="22" y="34" font-family="sans-serif" font-size="19">隔离示意 · 冷却水泵</text></svg>';

export function operationsFixture(){
  const user={open_id:'ou_responsive_fixture',name:'隔离验收员',role:'admin',level:'L0',category:'manager',login_method:'password',home_scope:'A',modules:['event','daily','learning','repair_management','accounts']};
  const scopes=['ALL','A','H'].map(value=>({value,label:value==='ALL'?'全部楼栋':value+'楼'}));
  const event={record_id:'evt-audit',title:'H楼冷却水泵运行告警',status:'处理中',level:'I2',building:'H楼',building_codes:['H'],occurrence_time:day()+' 08:10:00',response_time:day()+' 08:16:00',source:'值班巡查',specialty:'暖通',display_fields:{告警位置:'H楼冷却水泵房',现场情况:'备用泵已投入，正在核对轴承温度。'},transfer_to_overhaul:'False',under_repair:false};
  const tasks=[['risk','风险排查'],['change','变更'],['drill','演练'],['event','事件'],['inspection','巡检']].map(([kind,label])=>({kind,label,pull_scope:'近三日授权楼栋记录',times:['07:00','17:00'],confirmed:true,enabled:true,notify:true,targets:[{label:label+'原表'}],notification_targets:['运行值班群']}));
  const inspection=tasks.at(-1);
  const makeRun=(id,mode,status)=>({id,kind:'inspection',mode,status,created_at:day()+'T08:00:00+08:00',counts:{total:2,create:1,update:1,delete:0}});
  const failed={...makeRun('audit-failure','preview','failed'),error:'源端分页记录重复，已停止本次拉取。',stage:'分页核验',checks:{分页核验:{已读取:50,当前页:2}}};
  let history=[failed],morning=null,printModel=null,syncReads=0;
  const writes=[];
  async function handle(url,method,body){
    const p=url.pathname;
    if(method!=='GET')writes.push({method,path:p});
    if(p==='/api/health')return {json:{ok:true,service:'clipflow_backend',instance_id:'isolated-dialogs'}};
    if(p==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes,default_scope:'A'});
    if(p==='/api/assistant/appearance')return ok({enabled:false});
    if(p.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
    if(p==='/api/events/monthly')return ok({snapshot_exists:true,records:event.occurrence_time.startsWith(url.searchParams.get('month'))?[event]:[],last_refreshed_at:Date.now()/1000});
    if(p==='/api/events/transfer-repair'){event.transfer_to_overhaul='True';event.under_repair=true;return ok({});}
    if(p==='/api/cold-source-monitor'){
      const row={id:'wet-h',scope:'H',date:day(),temperature:21.5,mode:'冷机供冷',hint:'当班湿球温度已核对'};
      const jobs=Array.from({length:11},(_,i)=>({id:'notify-'+i,row,created_at:Date.now()/1000-i*3600,status:i===0?'uncertain':'sent',targets:{shift:{name:'当班工程师',id:'shift',status:'sent'}},error:i===0?'备用通知通道回执待核实':''}));
      const page=Number(url.searchParams.get('page')||1);
      return ok({items:[row,{...row,id:'wet-a',scope:'A',temperature:18.7,mode:'自然冷却'}],history:jobs.slice((page-1)*10,page*10),history_page:page,history_total:jobs.length});
    }
    if(p.startsWith('/api/zhihang-data')){
      const sub=p.slice('/api/zhihang-data'.length);
      if(!sub)return ok({auth:{has_token:true,display_name:'本地验收'},tasks,history,history_pagination:{total:history.length,page:1,page_size:10}});
      if(sub==='/login')return ok({status:'idle'});
      if(sub.endsWith('/settings')){Object.assign(tasks.find(t=>sub.startsWith('/'+t.kind+'/')),body);return ok(body);}
      if(sub==='/inspection/preview'){const run=makeRun('audit-preview','preview','success');inspection.last_preview=inspection.latest=run;history=[run,...history];return ok(run);}
      if(sub==='/inspection/sync'){const run=makeRun('audit-sync','sync','pending_verify');inspection.latest=run;history=[run,...history];syncReads=0;return ok(run);}
      if(sub.startsWith('/runs/')){const run=history.find(r=>r.id===sub.slice(6));if(run.id==='audit-sync'&&++syncReads>=2){run.status='success';run.notification={status:'sent',targets:['运行值班群']};inspection.last_sync=run;}
        return ok({...run,rows:run===failed?[]:[{楼栋:'H楼',巡检项目:'冷却水泵轴承温度'},{楼栋:'A楼',巡检项目:'机房通风检查'}],page:1,page_size:50,total:run===failed?0:2});}
    }
    if(p==='/api/repair-management/people')return ok({people:[{id:'audit-recipient',user_id:'audit-recipient',name:'隔离接收人',employee_no:'TEST',building:'H',selectable:true}],total:1});
    if(p==='/api/daily-tasks/send')return ok({sent_count:body.recipient_open_ids.length,failed_count:0});
    if(p.endsWith('/morning-meeting/generate')){printModel={...morning,...body,generated:true};return ok({model:printModel,generated_at:day()+' 09:00',download_url:'/fixture-sheet',print_url:'/assets/workspace-v2/index.html?__page=/daily-tasks/morning-meeting/print&date='+day()});}
    if(p.endsWith('/morning-meeting/print-model')&&printModel)return ok(printModel);
    if(p==='/api/learning/attachments/audit-image')return {body:imageBody,contentType:'image/svg+xml'};
    if(p==='/api/learning/import')return ok(body.preview?{errors:[]}:{errors:[],count:1});
    if(p==='/api/learning/completion')return ok({items:[{id:'done1',date:day(),person_id:'ou_responsive_fixture',person:{id:'ou_responsive_fixture',name:'隔离验收员'},scopes:['A'],status:'已完成',assigned:2,answered:2,correct:1,accuracy:50,source_papers:['collab-paper-1'],tasks:[{source:'paper',task_id:'collab-paper-1',shift:'白',delivery_status:'已发送'}]}],summary:{已完成:1},total:1});
    const result=await collabFixture({url,method,body});
    if(result?.json?.data){const data=result.json.data;
      if(p==='/api/daily-tasks')data.scope=url.searchParams.get('scope');
      if(p.endsWith('/morning-meeting/preview')&&!url.searchParams.has('temperature_only'))morning=data;
      if(p.endsWith('/learning/bootstrap')){data.daily_duty={date:data.today,scope:'A',shift:'白',end_at:new Date(Date.now()+3600000).toISOString()};data.silent_manual_publish=true;data.settings.enabled=false;}
      if(p.endsWith('/learning/settings'))Object.assign(data,{enabled:false,publish_time:'13:00',night_publish_time:'20:00',reminder_time:'17:00'});
      if(/^\/api\/learning\/papers(?:\/|$)/.test(p))for(const paper of (data.items||[data]))if(paper.questions?.length)paper.questions[0].attachments=[sample];
      if(p.endsWith('/learning/issues'))for(const issue of (data.items||[data]))Object.assign(issue,{can_update:true,question_id:'collab-question-1',category:'题干',status:'pending',description:'设备型号需要核对',suggestion:'补充水泵铭牌信息',attachments:[sample]});
      if(/^\/api\/learning\/questions\//.test(p)&&method==='GET')data.attachments=[sample];
      return result;
    }
    if(result)return result;
    if(method==='GET')return ok({items:[],records:[],links:{},options:[],settings:{}});
    return {status:405,json:{ok:false,error:'隔离夹具未实现此操作'}};
  }
  return {handle,writes};
}
