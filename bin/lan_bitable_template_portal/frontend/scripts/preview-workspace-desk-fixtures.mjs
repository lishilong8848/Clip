// Serve an isolated V2 build with in-memory fixtures. No proxy or upstream backend.
import {createServer} from 'node:http';
import {createReadStream} from 'node:fs';
import {access, stat, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {accountFixture} from './responsive-audit-account-fixtures.mjs';
import {workspaceExtensionFixture} from './workspace-extension-fixtures.mjs';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';

if (!process.env.DESK_AUDIT) throw new Error('Set DESK_AUDIT to the isolated build directory');
const directory = path.resolve(process.env.DESK_AUDIT);
const bundle = path.join(directory,'bundle');
await access(path.join(bundle,'entry.json'));
const publicDir = fileURLToPath(new URL('../public/',import.meta.url));
const scopes = ['ALL','A','B','C','D','E'].map(value => ({value,label:value==='ALL'?'全部楼栋':value+'楼'}));
const user = {open_id:'audit-admin',name:'隔离验收',level:'L0',role:'admin',category:'manager',home_scope:'A',
  scopes:scopes.map(scope=>scope.value),modules:['assistant','cabinet','water','accounts'],can_manage_accounts:true,login_method:'password'};
const mime = {'.json':'application/json','.js':'application/javascript','.css':'text/css','.html':'text/html; charset=utf-8',
  '.svg':'image/svg+xml','.png':'image/png','.ico':'image/x-icon','.woff2':'font/woff2','.jpg':'image/jpeg'};
const base = '/assets/workspace-v2/';
const server = createServer(async (req,res) => {
  const url = new URL(req.url,'http://127.0.0.1');
  const p = url.pathname;
  res.setHeader('Cache-Control','no-store');
  res.setHeader('Content-Security-Policy',"default-src 'self' blob: data:; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' blob:; img-src 'self' blob: data:;");
  const json = (value,status=200) => {
    res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});
    res.end(JSON.stringify(value));
  };
  const ok = data => json({ok:true,data});
  try {
    if (p==='/__stop') {res.end('closed');server.close();return;}
    if (p.startsWith('/api/')) {
      if (p==='/api/health') return json({ok:true,service:'clipflow_backend',instance_id:'desk-isolated'});
      if (p==='/api/auth/status') return ok({logged_in:true,user,scope_options:scopes});
      if (p==='/api/auth/certificate') return ok({state:'healthy',domain:'fixture.invalid',remaining_days:60});
      if (p.startsWith('/api/workspace-layouts/')) return ok({revision:0,value:null});
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = req.headers['content-type']?.includes('application/json') ? JSON.parse(Buffer.concat(chunks).toString()||'{}') : {};
      for (const fixture of [accountFixture,workspaceExtensionFixture,coreFixture]) {
        const result = await fixture({url,method:req.method,body});
        if (!result) continue;
        if (result.json) return json(result.json,result.status);
        res.writeHead(result.status||200,{'Content-Type':result.contentType||'text/plain'});
        return res.end(result.body);
      }
      return json({ok:false,error:'隔离预览未配置 '+p},404);
    }
    let file;
    if (!path.extname(p) || p===base+'index.html') file=path.join(bundle,'index.html');
    else if (p.startsWith(base) && /^[\w.-]+$/.test(p.slice(base.length))) file=path.join(bundle,p.slice(base.length));
    else if (p.startsWith('/assets/')) {
      const asset=path.resolve(publicDir,'.'+p);
      if (asset.startsWith(path.resolve(publicDir)+path.sep)) file=asset;
    }
    if (!file || !(await stat(file).catch(()=>null))?.isFile()) return res.writeHead(404).end();
    res.setHeader('Content-Type',mime[path.extname(file)]||'application/octet-stream');
    createReadStream(file).on('error',()=>res.destroy()).pipe(res);
  } catch (error) {
    if (!res.headersSent) json({ok:false,error:error.message},500);
    else res.destroy();
  }
});
server.listen(0,'127.0.0.1',async () => {
  const origin='http://127.0.0.1:'+server.address().port;
  await writeFile(path.join(directory,'origin.txt'),origin);
  console.log(origin);
});