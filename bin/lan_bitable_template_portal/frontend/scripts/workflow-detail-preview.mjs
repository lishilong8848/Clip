// Source-only, loopback preview for the workflow dialog acceptance suite.
import {createServer} from 'vite';
import {readFile, mkdtemp} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';
import {collabFixture} from './responsive-audit-collab-fixtures.mjs';
import {workspaceExtensionFixture} from './workspace-extension-fixtures.mjs';

const root=fileURLToPath(new URL('../',import.meta.url));
const base='/assets/workspace-v2/';
const manifest=JSON.parse(await readFile(path.join(root,'package.json'),'utf8'));
const cacheDir=await mkdtemp(path.join(tmpdir(),'workflow-details-vite-'));
const scopes=['A','B','C','D','E','H','110'].map(value=>({value,label:value==='110'?'110站':value+'楼'}));
const layouts=new Map();
const ok=data=>({json:{ok:true,data}});
function localApi(url,method,body){
  if(url.pathname==='/api/health')return {json:{ok:true,service:'clipflow_backend',instance_id:'workflow-detail-isolated'}};
  if(url.pathname==='/api/handover-links')return ok({scope_options:scopes,links:Object.fromEntries(scopes.map(s=>[s.value,url.origin+'/review-fixture/'+s.value]))});
  if(url.pathname.startsWith('/api/workspace-layouts/')){
    let row=layouts.get(url.pathname)||{revision:0,value:null};
    if(method!=='GET'){row={revision:row.revision+1,value:body.value};layouts.set(url.pathname,row);}
    return ok(row);
  }
}

const dev=await createServer({root,base,configFile:path.join(root,'vite.workspace-v2.config.ts'),cacheDir,
  optimizeDeps:{noDiscovery:true,entries:[],include:Object.keys(manifest.dependencies)},
  server:{host:'127.0.0.1',port:0,hmr:false,proxy:{},watch:{ignored:['**/dist/**','**/output/**']}},
  plugins:[{name:'isolated-workflow-detail-preview',enforce:'pre',configureServer(server){
    server.middlewares.use(async(req,res,next)=>{
      const url=new URL(req.url,'http://'+req.headers.host);
      const send=result=>{
        res.statusCode=result.status||200;
        res.setHeader('Cache-Control','no-store');
        res.setHeader('Content-Type',result.json?'application/json; charset=utf-8':result.contentType||'text/plain; charset=utf-8');
        res.end(result.json?JSON.stringify(result.json):result.body||'');
      };
      try{
        if(url.pathname===base+'index.html'){
          res.setHeader('Content-Security-Policy',"default-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; connect-src 'self'; form-action 'self'; frame-src 'self' blob:");
          return send({contentType:'text/html; charset=utf-8',body:'<!doctype html><html class="workspace-v2" lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="'+base+'src/workspace-v2/main.ts"></script></html>'});
        }
        if(url.pathname.startsWith('/review-fixture/'))return send({body:'隔离楼栋审核入口 '+url.pathname.split('/').pop()});
        if(/^\/assets\/[\w.-]+\.(png|svg|webp|css)$/.test(url.pathname)){
          const ext=path.extname(url.pathname);
          return send({contentType:({'.png':'image/png','.webp':'image/webp','.svg':'image/svg+xml','.css':'text/css'})[ext],body:await readFile(path.join(root,'public',url.pathname))});
        }
        if(url.pathname.startsWith('/api/')||url.pathname==='/workbench-lite'){
          const chunks=[];for await(const chunk of req)chunks.push(chunk);
          const raw=Buffer.concat(chunks);
          const body=raw.length&&req.headers['content-type']?.includes('application/json')?JSON.parse(raw.toString()):{};
          const input={url,method:req.method,body};
          const result=localApi(url,req.method,body)||await workspaceExtensionFixture(input)||await coreFixture(input)||await collabFixture(input);
          return send(result||{status:503,json:{ok:false,error:'隔离样本缺少 '+url.pathname}});
        }
        return next();
      }catch(error){return send({status:500,json:{ok:false,error:error.message}});}
    });
  }}]});
await dev.listen();
console.log('WORKFLOW_DETAIL_ORIGIN=http://127.0.0.1:'+dev.httpServer.address().port);

