import {preview} from 'vite';
import {fileURLToPath} from 'node:url';
import {appendFile,mkdir} from 'node:fs/promises';
import path from 'node:path';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';
import {collabFixture} from './responsive-audit-collab-fixtures.mjs';
import {accountFixture} from './responsive-audit-account-fixtures.mjs';

// Development-only preview: every API is local fixture data, never a production proxy.
const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/responsive-audit');
await mkdir(output,{recursive:true});
const modules=['event','maintenance','change','repair','polling','adjust','power','engineer_mop','repair_management','critical_guard','risk_inspection','daily','cabinet','water','plan_convergence','drills','learning','knowledge','links','life_guide','signatures','handover','work_handover','assistant','accounts'];
const scopes=[{value:'ALL',label:'全部授权楼栋'},...['A','B','C','D','E','H','110'].map(value=>({value,label:value==='110'?'110站':value+'楼'}))];
const user={open_id:'ou_responsive_fixture',name:'布局验收管理员',level:'L0',category:'manager',category_label:'经理',role:'admin',can_manage_accounts:true,can_delete_business:true,modules,home_scope:'A',scopes:scopes.map(s=>s.value),login_method:'password'};
const layouts=new Map();
let server;
const api={name:'isolated-responsive-fixtures',configurePreviewServer(host){host.middlewares.use(async(req,res,next)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  const ref=new URL(req.headers.referer||'http://127.0.0.1');
  const reply=payload=>{res.statusCode=payload.status||200;res.setHeader('Cache-Control','no-store');res.setHeader('Content-Type',payload.json?'application/json; charset=utf-8':payload.contentType||'text/plain; charset=utf-8');res.end(payload.json?JSON.stringify(payload.json):payload.body||'');};
  const ok=data=>reply({json:{ok:true,data}});
  if(url.pathname==='/__audit/stop'){ok({stopped:true});setTimeout(()=>server.httpServer.close(()=>process.exit()),20);return;}
  if(!url.pathname.startsWith('/api/')&&url.pathname!=='/workbench-lite'&&!url.pathname.startsWith('/polling-work-order'))return next();
  try{
    const chunks=[];for await(const chunk of req){chunks.push(chunk);if(chunks.reduce((n,c)=>n+c.length,0)>1024*1024)throw new Error('Fixture request exceeds 1 MiB');}
    const text=Buffer.concat(chunks).toString();
    const body=text&&req.headers['content-type']?.includes('application/json')?JSON.parse(text):{};
    if(url.pathname==='/api/health')return reply({json:{ok:true,service:'clipflow_backend',instance_id:'responsive-isolated-preview'}});
    if(url.pathname==='/api/auth/status'){
      const login=ref.searchParams.get('__audit')==='login',guest=ref.searchParams.get('__audit')==='guest';
      return ok({logged_in:!login,user:login?{}:guest?{...user,name:'访客',role:'guest',modules:['cabinet']}:user,scope_options:login?[]:scopes,allowed_scopes:login?[]:scopes.map(s=>s.value),default_scope:'A',login_url:'/api/auth/login'});
    }
    if(url.pathname.startsWith('/api/workspace-layouts/')){
      const saved=layouts.get(url.pathname)||{revision:0,value:null};
      if(req.method==='GET')return ok(saved);
      const following={revision:saved.revision+1,value:body.value};layouts.set(url.pathname,following);return ok(following);
    }
    for(const fixture of [accountFixture,coreFixture,collabFixture]){
      const result=await fixture({url,method:req.method,body});if(result)return reply(result);
    }
    await appendFile(path.join(output,'unhandled.jsonl'),JSON.stringify({path:url.pathname,query:url.search,method:req.method,page:ref.pathname+ref.search})+'\n');
    return reply({status:503,json:{ok:false,error:'隔离预览尚未配置此接口：'+url.pathname}});
  }catch(error){console.error(url.pathname,error);return reply({status:500,json:{ok:false,error:'隔离预览数据错误：'+error.message}});}
});}};
server=await preview({root,configFile:false,plugins:[api],logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:Number(process.env.RESPONSIVE_PORT||14536),strictPort:true}});
console.log('ISOLATED_PREVIEW http://127.0.0.1:'+server.httpServer.address().port);
process.on('SIGINT',()=>server.httpServer.close(()=>process.exit()));
