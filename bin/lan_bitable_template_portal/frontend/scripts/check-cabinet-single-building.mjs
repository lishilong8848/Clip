import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {createServer} from 'node:http';
import {readFile,mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const frontend=fileURLToPath(new URL('../',import.meta.url));
const dist=process.env.CABINET_TEST_DIST || path.join(frontend,'dist');
const shots=path.join(frontend,'../../../output/playwright/cabinet-single-building');
await mkdir(shots,{recursive:true});
const server=createServer(async(req,res)=>{
  const url=new URL(req.url,'http://localhost');
  const rel=url.pathname.startsWith('/assets/')?url.pathname.slice(1):'index.html';
  try {
    const file=path.resolve(dist,rel);
    assert(file.startsWith(path.resolve(dist)+path.sep));
    const body=await readFile(file);
    res.writeHead(200,{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json'})[path.extname(file)]||'application/octet-stream'});res.end(body);
  } catch {res.writeHead(404);res.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch();
try {
 for(const [entry,width,admin] of [['/cabinet-power?frontend=legacy&scope=A',1440,true],['/assets/workspace-v2/index.html?__page=/cabinet-power&scope=A',1280,true],['/assets/workspace-v2/index.html?__page=/cabinet-power&scope=A',390,false]]){
  const context=await browser.newContext({viewport:{width,height:900},acceptDownloads:true});
  const page=await context.newPage(), problems=[], attempts=[], downloads=[];
  page.on('pageerror',error=>problems.push(error.message));
  let releasePoll, blockPoll=false, jobId='', fileId='', failFile=false;
  await page.route('**/*',async route=>{
   const req=route.request(),url=new URL(req.url()),p=url.pathname;
   if(url.origin!==origin)return route.abort();
   if(!p.startsWith('/api/'))return route.continue();
   const ok=data=>route.fulfill({json:{ok:true,data}});
   if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture',name:'隔离验收',role:admin?'admin':'user',level:admin?'L0':'L3',modules:['cabinet'],scope_rules:['A']},allowed_scopes:['A'],scope_options:[{value:'A',label:'A楼'}]});
   if(p==='/api/cabinet-power/overview')return ok({version:1,rooms:[{id:'201',name:'201',total:1,counts:{formal:1,test:0,off:0,unknown:0},types:{}}],counts:{formal:1,test:0,off:0,unknown:0},issues:[],sheet_formats:[]});
   if(p==='/api/cabinet-power/bootstrap')return ok({status:'succeeded',ready:1,total:1,buildings:[{scope:'A',status:'succeeded'}]});
   if(p==='/api/cabinet-power/exports'&&req.method()==='POST'){
    const body=req.postDataJSON();attempts.push(body);jobId=('0'.repeat(31)+attempts.length);fileId='export'+attempts.length;
    return ok({job_id:jobId,kind:'export',status:'pending'});
   }
   if(p==='/api/cabinet-power/jobs/'+jobId){
    if(blockPoll) await new Promise(resolve=>{releasePoll=resolve;});
    return ok({job_id:jobId,kind:'export',status:'succeeded',result:{export_id:fileId,filename:'A楼.xlsm'}});
   }
   if(p.startsWith('/api/cabinet-power/exports/')&&p.endsWith('/download')){
    downloads.push(p);
    if(failFile)return route.fulfill({status:503,json:{ok:false,error:'隔离下载失败'}});
    return route.fulfill({headers:{'Content-Type':'application/octet-stream','Content-Disposition':"attachment; filename*=UTF-8''A%E6%A5%BC.xlsm"},body:Buffer.from('verified fixture')});
   }
   assert.equal(req.method(),'GET',p);
   return ok({items:[],total:0});
  });
  await page.goto(origin+entry);
  const heading=page.locator('.cabinet-page .heading'),exp=heading.getByRole('button',{name:'导出',exact:true});
  await exp.waitFor();
  for(const name of ['批量登记','上下电待办','导出历史'])await heading.getByRole('button',{name:new RegExp(name)}).waitFor();
  assert.equal(await heading.getByText('更多工具',{exact:true}).count(),0);
  assert.equal(await heading.getByRole('button',{name:'设置',exact:true}).count(),admin?1:0);
  await heading.screenshot({path:path.join(shots,(entry.includes('legacy')?'legacy':'v2')+'-'+width+'.png')});
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  blockPoll=true;
  const started=page.waitForRequest(r=>r.method()==='POST'&&new URL(r.url()).pathname==='/api/cabinet-power/exports');
  await exp.evaluate(el=>{el.click();el.click();});await started;
  const completed=page.waitForEvent('download',{timeout:20000});
  for(let i=0;i<200&&!releasePoll;i++)await new Promise(r=>setTimeout(r,10));
  assert(releasePoll);assert.equal(downloads.length,0);blockPoll=false;releasePoll();
  assert.equal((await completed).suggestedFilename(),'A楼.xlsm');
  assert.equal(attempts.length,1);assert.equal(attempts[0].scope,'A');assert.deepEqual(downloads,['/api/cabinet-power/exports/export1/download']);
  failFile=true;await exp.click();
  await page.getByText('文件已生成，但下载未完成，请点击文件旁的「下载」重试。',{exact:false}).first().waitFor();
  assert.deepEqual(downloads,['/api/cabinet-power/exports/export1/download','/api/cabinet-power/exports/export2/download']);
  failFile=false;
  const retry=page.waitForEvent('download');
  await page.getByRole('link',{name:'下载',exact:true}).first().click();
  assert.equal((await retry).suggestedFilename(),'A楼.xlsm');
  assert.equal(downloads.at(-1),'/api/cabinet-power/exports/export2/download');
  assert.equal(attempts.length,2);assert.deepEqual(problems,[]);
  await context.close();
  console.log('PASS',entry,width,'inline permissions, single export click/download, original file retry');
 }
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
