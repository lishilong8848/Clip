import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';

export const eventId='web_'+'a'.repeat(32), secondId='web_'+'b'.repeat(32);
const robot1='1'.repeat(32),robot2='2'.repeat(32);
const stamp=new Date(Date.now()+8*3600000).toISOString().slice(0,10);
const date=(time)=>stamp+' '+time;
const raw=(title,action,progress)=>['【事件通告】状态：'+action,'【标题】'+title,'【发生时间】'+date('09:15'),'【进展】'+progress].join('\n');
const fields=['summary','building','specialty','level','event_source','occurrence_time'];
const targets=()=>[
  {channel:'feishu',name:'园区 I3 值班群',state:'sent'},
  {channel:'dingtalk',name:'A楼运行群',state:'failed',reason:'模拟群机器人连接超时'},
  {channel:'dingtalk',name:'设施协同群',state:'uncertain',reason:'模拟回执未返回，群内需核对'},
];
function row(id,title,specialty,state='active'){
  return {event_id:id,text:raw(title,state==='ended'?'结束':'更新','已完成现场复核，值班人员跟踪运行状态。'),created_at:Date.now()/1000,created_by:'offline-duty',
    state,metadata:{summary:title,building:'A',specialty,level:'I3',event_source:'巡检发现',occurrence_time:date('09:15'),recover_time:state==='ended'?date('10:00'):'',transfer_to_overhaul:false},
    metadata_revision:2,sync_state:'synced',missing:[],delivery:{state:'sent',action:'update',operation_id:'fixture-'+id,targets:targets().map(t=>({...t,state:'sent',reason:''}))}};
}
export async function fixture(assets,frontend,canDelete=false){
  const main=row(eventId,'冷冻水回水温度偏高','暖通'),second=row(secondId,'配电监控测点读数中断','电气');
  main.delivery.state='failed';main.delivery.targets=targets();
  second.delivery.state='failed';second.delivery.targets=[{channel:'dingtalk',name:'A楼运行群',state:'failed',reason:'模拟发送失败'}];
  const ended=row('web_'+'c'.repeat(32),'机房压差恢复','暖通','ended');
  ended.metadata.event_source='';ended.missing=['事件来源'];ended.sync_state='waiting';
  const rows=new Map([main,second,ended].map(r=>[r.event_id,r]));
  const options={can_delete:canDelete,user_id:canDelete?'offline-admin':'offline-duty',scopes:['A'],levels:['I2','I3','I4'],sources:['巡检发现','监控告警'],specialties:['电气','暖通','弱电'],defaults:{robot_ids:[robot1,robot2],revision:3},
    robots:[{id:robot1,name:'A楼运行群',remark:'当班处置'},{id:robot2,name:'设施协同群',remark:'专业协同'}]};
  const writes=[],accepted=new Map();
  let failCreate=true, failDelete=true;
  function publicRows(){return [...rows.values()].filter(r=>r.state!=='draft');}
  function newRow(id){return {event_id:id,state:'draft',created_at:Date.now()/1000,created_by:options.user_id,text:'',metadata:{},metadata_revision:0,sync_state:'waiting',missing:[]};}
  function updateMeta(r,patch){Object.assign(r.metadata,patch);r.missing=fields.filter(k=>!r.metadata[k]);r.metadata_revision++;r.sync_state=r.missing.length?'waiting':'synced';}
  const server=createServer(async(req,res)=>{
    const u=new URL(req.url,'http://localhost'),p=u.pathname;
    const json=(status,body)=>{res.writeHead(status,{'Content-Type':'application/json'});res.end(JSON.stringify(body));};
    const ok=data=>json(200,{ok:true,data});
    const fail=(status,error)=>json(status,{ok:false,error});
    try{
      if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:options.user_id,name:'隔离验收',role:canDelete?'admin':'building',level:canDelete?'L0':'L4',category:canDelete?'operator':'duty',home_scope:'A',can_delete_business:canDelete,modules:['event']},scope_options:[{value:'A',label:'A楼'}]});
      if(p==='/api/pre-shift-checks')return ok({scope:'A',roster_ready:true,server_time:new Date().toISOString(),tasks:[],definition:{items:[]},scopes:['A']});
      if(p==='/api/health')return json(200,{ok:true,service:'clipflow_backend',instance_id:'event-offline'});
      if(p==='/api/workbench-events/options')return ok(options);
      if(p==='/api/workbench-events')return ok({...options,items:publicRows()});
      if(p.startsWith('/api/workbench-events/') && req.method==='POST'){
        const chunks=[];for await(const c of req)chunks.push(c);
        const b=JSON.parse(Buffer.concat(chunks).toString());writes.push({p,body:structuredClone(b)});
        const r=rows.get(b.event_id);
        switch(p.split('/').pop()){
          case 'defaults':
            if(b.revision!==options.defaults.revision)return fail(409,'模拟版本冲突');
            options.defaults={robot_ids:b.robot_ids,revision:b.revision+1};return ok(options.defaults);
          case 'parse':return ok({summary:b.text.match(/【标题】([^\n]+)/)?.[1]||'',building:'A',level:'I3',specialty:'暖通',event_source:'巡检发现',occurrence_time:date('09:15')});
          case 'metadata':{const item=r||newRow(b.event_id);updateMeta(item,b.patch);rows.set(item.event_id,item);return ok(item);}
          case 'send':{
            if(accepted.has(b.operation_id))return ok(accepted.get(b.operation_id));
            if(b.action==='start'&&failCreate){failCreate=false;return fail(503,'模拟受理响应中断，请核验原发送');}
            const item=r||newRow(b.event_id);item.state=b.action==='end'?'ended':'active';item.text=b.text;updateMeta(item,b.metadata_patch);
            item.delivery={action:b.action,operation_id:b.operation_id,state:'sent',targets:[{channel:'feishu',name:'园区 I3 值班群',state:'sent'},...options.defaults.robot_ids.map(id=>({channel:'dingtalk',state:'sent',name:options.robots.find(robot=>robot.id===id).name}))]};
            rows.set(item.event_id,item);accepted.set(b.operation_id,structuredClone(item));return ok(item);}
          case 'retry':{
            if(!r||b.operation_id!==r.delivery.operation_id)return fail(409,'模拟任务编号不匹配');
            if(!b.confirm_uncertain&&r.delivery.targets.some(t=>['posting','uncertain'].includes(t.state)))return fail(409,'需要先核对群消息');
            for(const target of r.delivery.targets){if(target.state!=='sent'){target.state='sent';target.reason='';}}r.delivery.state='sent';return ok(r);}
          case 'delete':
            if(!options.can_delete)return fail(403,'当前账号无删除权限');
            if(failDelete){failDelete=false;return fail(503,'模拟删除未完成，记录仍保留');}
            rows.delete(b.event_id);return ok({deleted:true});
          default:return fail(404,'未知模拟接口');
        }
      }
      if(p.startsWith('/api/')){
        if(req.method!=='GET')return fail(405,'未配置此模拟写入');
        if(p.startsWith('/api/workspace-layouts/'))return ok({value:null,revision:0});
        return ok({records:[],items:[],settings:{},fields:[],summary:{},available:false});
      }
      if(p==='/assets/connection-guard.js'){res.writeHead(200,{'Content-Type':'text/javascript'});return res.end();}
      const m=/^\/assets\/(workspace-v2\/)?([\w.-]+\.(?:html|js|css|json|svg|png|webp))$/.exec(p);
      if(!m){res.writeHead(404);return res.end();}
      const file=await readFile(path.join(m[1]?assets:path.join(frontend,'public/assets'),m[2]));
      const type={'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json','.svg':'image/svg+xml','.png':'image/png','.webp':'image/webp'};
      res.writeHead(200,{'Content-Type':type[path.extname(m[2])],'Content-Security-Policy':"default-src 'self' data: blob:; connect-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"});res.end(file);
    }catch(e){if(!res.headersSent)fail(404,'模拟资源不可用');else res.end();}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  return {origin:'http://127.0.0.1:'+server.address().port,writes,options,rows,close:()=>new Promise(resolve=>server.close(resolve))};
}

