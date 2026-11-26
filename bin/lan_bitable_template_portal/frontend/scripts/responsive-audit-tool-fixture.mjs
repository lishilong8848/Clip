// Only used by the loopback preview server. Sessions and readings live in memory.
const base='/api/tool-self-check';
const tools=[
  ['钳形电流表','FLUKE 362','A','低压配电间','self'],['数字万用表','FLUKE 17B+','A','电气工具柜','self'],
  ['温湿度记录仪','174H','E','暖通值班室','self'],['接触式温度计','52 II','E','冷站工具柜','self'],
  ['绝缘电阻表','1507','D','电气工具柜','self'],['接地电阻测试仪','4105A','D','配电间','external'],
];
const assets=tools.map(([name,model,scope,location,kind],i)=>({id:'demo-tool-'+i,name,model,scope,location,brand:i===5?'KYORITSU':'FLUKE',eligible_kinds:[kind],
  profession:scope==='E'?'暖通':'电气',
  ...(kind==='self'?{self_cycle:'每月',template_name:scope==='E'?'暖通仪表检查记录':'电气仪表检查记录'}:{external_cycle:'一年'})}));
const templates=['电气仪表检查记录','暖通仪表检查记录'].map((name,i)=>({id:'demo-template-'+i,name}));
const sessions=new Map();
function layout(template){
  const name=template.name;
  const cell=(address,value,extra={})=>({address,value,rowspan:1,colspan:1,text_editable:true,...extra});
  return {schema:{sheet:name,result_row:4,review_row:5},sheets:[{name,columns:[180,200],rows:[
    {index:1,height:32,cells:[cell('A1',name,{colspan:2})]},
    {index:2,height:28,cells:[cell('A2','外观及功能检查'),cell('B2','',{editable:true,check:true})]},
    {index:3,height:28,cells:[cell('A3','实测读数'),cell('B3','',{editable:true})]},
  ]}]};
}
export function toolFixture({url,method,body}){
  const path=url.pathname, ws=base+'/workspace';
  if(path!==base && !path.startsWith(ws))return;
  const ok=data=>({json:{ok:true,data}}),issue=error=>({status:409,json:{ok:false,error}});
  if(path===base && method==='GET')return ok({month:new Date().toISOString().slice(0,7),followups:[],templates,can_manage_templates:true});
  if(path===ws+'/history' && method==='GET')return ok({sessions:[...sessions.values()].map(s=>({...s,history_items:s.items.map(i=>({id:i.asset_id,scope:i.asset.scope,asset_name:i.asset.name,order_status:'pending'}))})),history_cursor:null});
  if((method==='GET'&&path===ws)||(method==='POST'&&path===ws+'/refresh'))return ok({assets,templates,sessions:[...sessions.values()]});
  if(method==='POST'&&path===ws+'/open'&&body.kind==='self'){
    const ids=body.asset_ids || [], selected=ids.map(id=>assets.find(a=>a.id===id));
    if(!ids.length||selected.some(a=>!a || !a.eligible_kinds.includes('self') || !templates.some(t=>t.name===a.template_name)))return issue('工具台账缺少模板文件');
    const id='demo-session-'+(sessions.size+1), session={id,kind:'self',status:'draft',revision:1,can_edit:true,items:selected.map(asset=>({
      id:asset.id,asset_id:asset.id,asset,can_edit:true,order_revision:1,values:{},checks:{},outcome:'',reviewer:'',signature:'',reviewed_at:'',qualified:null,
      template:layout(templates.find(t=>t.name===asset.template_name)),
    }))};
    sessions.set(id,session);return ok(session);
  }
  const [,id,action]=path.slice(ws.length).split('/');
  const session=sessions.get(id);if(!session)return;
  if(method==='GET'&&action==='people')return ok({A:[],D:[],E:[]});
  if(method==='GET'&&!action)return ok(session);
  if(method==='POST'&&action==='save'){
    if(session.revision!==body.revision)return issue('草稿已更新，请重新打开');
    for(const item of session.items){
      const update=body.items.find(p=>p.id===item.id);
      if(update)for(const field of ['values','checks','outcome','reviewer','reviewed_at','qualified'])if(field in update)item[field]=update[field];
    }
    session.revision++;return ok(session);
  }
}
