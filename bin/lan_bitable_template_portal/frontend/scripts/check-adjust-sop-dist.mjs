import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,readdir,writeFile,rm} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),project=path.resolve(root,'../../..');
const output=path.join(project,'output/playwright/adjust-sop-dist'),scratch=await mkdtemp(path.join(os.tmpdir(),'adjust-sop-dist-'));
const python=path.join(project,'bin/.venv/Scripts/python.exe');
const bootstrap=`import json,sys,socket
from pathlib import Path
sys.path.insert(0,'bin')
def denied(*args,**kwargs): raise AssertionError('isolated SOP fixture forbids network')
socket.socket.connect=denied
from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
from lan_bitable_template_portal.state_store import LanPortalStateStore
from clipflow_backend.api_models import PollingSopRequest
root=Path(sys.argv[1])
service=PollingWorkOrderService(LanPortalStateStore(root/'sops.sqlite3'))
service.sop_root=root/'sops'
service.work_order_root=root/'orders'
data=json.loads(sys.stdin.read())
action=data['action']
if action=='init':
    rows=[]
    for ui in ('legacy','modern'):
        for mode in ('normal','cooling'):
            row=service.save_sop({'name':ui+'-'+mode,'scope':'A','work_type':'adjust','steps':[{'content':'Initial fixture operation','operator_required':True,'reviewer_required':True,'photo_required':False}]})
            rows.append(service.add_sop_attachment(row['sop_id'],file_name='fixture.txt',content=b'isolated fixture',expected_version=row['version']))
    result=rows
elif action=='list': result={'items':service.list_sops(data['scope'],data['work_type'])}
elif action=='save': result=service.save_sop(PollingSopRequest(**data['body']).to_payload())
elif action=='get': result=service.get_sop(data['sop_id'])
elif action=='html':
    from lan_bitable_template_portal.workbench_lite import render_workbench_lite
    result=render_workbench_lite(payload={'records':[],'ongoing':[],'stats':{}},session={'role':'admin','allowed_modules':['maintenance','polling','adjust'],'user':{'name':'Isolated SOP tester'}},scope='A',work_type='adjust',scope_options=[{'value':'A','label':'A楼'}])
else: raise AssertionError(action)
print(json.dumps(result,ensure_ascii=False))
`;
function backend(data){
  const result=spawnSync(python,['-B','-c',bootstrap,scratch],{cwd:project,input:JSON.stringify(data),encoding:'utf8',timeout:30000,
    env:{...process.env,CLIPFLOW_DATA_DIR:scratch,PYTHONNOUSERSITE:'1',PYTHONDONTWRITEBYTECODE:'1',PYTHONIOENCODING:'utf-8'}});
  assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout);
}
async function hashes(directory){
  const result={};
  async function visit(folder){for(const item of await readdir(folder,{withFileTypes:true})){
    const filename=path.join(folder,item.name);if(item.isDirectory())await visit(filename);
    else if(item.isFile())result[path.relative(directory,filename).replaceAll('\\','/')]=createHash('sha256').update(await readFile(filename)).digest('hex');
  }}
  await visit(directory);return result;
}
let server,browser,page;
const results=[],writes=[],errors=[],unexpected=[],assets={legacy:new Set(),modern:new Set()};
await mkdir(output,{recursive:true});
const distBefore=await hashes(path.join(root,'dist'));
try{
  const initial=backend({action:'init'}),legacyHtml=backend({action:'html'});
  server=await preview({root,configFile:false,logLevel:'error',build:{outDir:'dist'},preview:{host:'127.0.0.1',port:0}});
  const origin='http://127.0.0.1:'+server.httpServer.address().port;
  browser=await chromium.launch({headless:true});
  for(const ui of ['legacy','modern']){
    const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce',serviceWorkers:'block'});
    await context.addInitScript(()=>{if(location.pathname!=='/assets/workspace-v2/index.html')localStorage.setItem('clipflow:frontend-version:ou_sop_dist_fixture','legacy');});
    await context.route('**/*',async route=>{
      const req=route.request(),url=new URL(req.url());
      if(url.origin!==origin){unexpected.push('external:'+url.origin);return route.abort();}
      if(url.pathname.startsWith('/src/')||url.pathname.startsWith('/@vite/')){unexpected.push(url.pathname);return route.abort();}
      if(url.pathname==='/workbench-lite'&&url.searchParams.get('_assistant_frame')==='1')return route.fulfill({contentType:'text/html',body:legacyHtml});
      if(!url.pathname.startsWith('/api/')){
        if(url.pathname.startsWith('/assets/')&&url.pathname.endsWith('.js'))assets[ui].add(url.pathname);
        return route.continue();
      }
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_sop_dist_fixture',name:'隔离SOP验证',role:'admin',level:'L1',category:'supervisor',can_delete_business:true,home_scope:'A',scope_rules:['A'],modules:['maintenance','polling','adjust']},scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
      if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'sop-dist-isolated'}});
      if(url.pathname==='/api/polling-sops'&&req.method()==='GET')return ok(backend({action:'list',scope:url.searchParams.get('scope'),work_type:url.searchParams.get('work_type')}));
      if(url.pathname.startsWith('/api/polling-sops/')&&req.method()==='PUT'){
        const body=req.postDataJSON();assert.equal(body.sop_id,url.pathname.split('/').at(-1));
        const saved=backend({action:'save',body});writes.push({ui,body,saved});return ok(saved);
      }
      if(req.method()!=='GET'){unexpected.push(req.method()+' '+url.pathname);return route.abort();}
      if(url.pathname.startsWith('/api/workspace-layouts/'))return ok({revision:0,value:null});
      if(url.pathname==='/api/workbench')return ok({records:[],ongoing:[],stats:{},records_pagination:{total:0},ongoing_pagination:{total:0}});
      if(url.pathname==='/api/workbench/draft')return ok({draft:null});
      return ok({items:[],records:[],available:false,stats:{},summary:{}});
    });
    page=await context.newPage();page.setDefaultTimeout(20000);page.on('pageerror',error=>errors.push({ui,message:error.message}));
    const url=ui==='legacy'?origin+'/workbench-lite?frontend=legacy&scope=A&work_type=adjust':origin+'/assets/workspace-v2/index.html?__page=/&view=sops&scope=A&work_type=adjust';
    async function open(name){
      await page.goto(url,{waitUntil:'domcontentloaded'});
      if(ui==='legacy'){
        const frame=page.frameLocator('iframe[title="通告管理"]');
        await frame.locator('#lite-polling-sop-open').click();
        await frame.locator('#lite-polling-sop-list').getByRole('button',{name,exact:false}).click();
        const editor=frame.locator('#lite-polling-sop-editor');
        const type=editor.locator('select').first();
        assert.deepEqual(await type.locator('option').evaluateAll(options=>options.map(option=>option.value)),['normal','cooling']);
        return {editor,type,content:editor.locator('textarea').first(),
          save:()=>editor.getByRole('button',{name:'保存 SOP',exact:true}).click(),frame};
      }
      await page.locator('.v2-sop-manager').getByRole('button',{name,exact:false}).click();
      return {editor:page.locator('.v2-sop-manager'),type:page.locator('[aria-label="调整SOP类型"]').first(),
        content:page.getByRole('textbox',{name:'步骤1内容',exact:true}),save:()=>page.getByRole('button',{name:'保存SOP',exact:true}).click()};
    }
    for(const mode of ['normal','cooling']){
      const name=ui+'-'+mode,content=mode==='cooling'?'核对所选制冷单元状态，按现场步骤完成模式切换。':'核对普通设备状态并完成调整。';
      let controls=await open(name);
      if(ui==='modern')await page.getByRole('button',{name:'修改',exact:true}).click();
      if(mode==='cooling'){
        if(ui==='legacy')await controls.type.selectOption('cooling');
        else{await controls.type.click();await page.locator('.n-base-select-option').filter({hasText:'制冷单元模式切换 SOP'}).click();}
      }
      await controls.content.fill(content);
      const before=writes.length;
      await Promise.all([page.waitForResponse(response=>response.url().includes('/api/polling-sops')&&response.request().method()==='GET'),controls.save()]);
      assert.equal(writes.length,before+1);assert.equal(writes.at(-1).body.adjust_sop_type,mode);
      controls=await open(name);
      if(ui==='legacy')assert.equal(await controls.type.inputValue(),mode);
      else assert.match(await controls.type.innerText(),mode==='cooling'?/制冷单元模式切换/:/普通设备调整/);
      assert.equal(await controls.content.inputValue(),content);
      const original=initial.find(row=>row.name===name),stored=backend({action:'get',sop_id:original.sop_id});
      assert.equal(stored.adjust_sop_type,mode);assert.equal(stored.version,original.version+1);
      assert.equal(stored.steps[0].content,content);assert.equal(stored.attachments.length,1);
      assert.equal(stored.steps[0].operator_required,true);assert.equal(stored.steps[0].reviewer_required,true);
      await page.screenshot({path:path.join(output,name+'-reopened.png'),fullPage:true});
      results.push({ui,mode,saved:true,reopened:true,version:stored.version,contentUnchanged:true,attachmentPreserved:true});
      console.log(ui,mode,'saved and reopened from isolated SQLite');
    }
    await context.close();
  }
  assert([...assets.legacy].some(url=>/^\/assets\/index-/.test(url)),'old dist entry loaded');
  assert([...assets.modern].some(url=>/^\/assets\/workspace-v2\/app-v2-/.test(url)),'new dist entry loaded');
  assert.deepEqual(unexpected,[]);assert.deepEqual(errors,[]);
  assert.deepEqual(await hashes(path.join(root,'dist')),distBefore,'built dist remains byte-identical');
  await writeFile(path.join(output,'result.json'),JSON.stringify({passed:true,results,writes:writes.map(({ui,body})=>({ui,method:'PUT',type:body.adjust_sop_type})),
    loadedAssets:Object.fromEntries(Object.entries(assets).map(([ui,paths])=>[ui,[...paths]])),distUnchanged:true,externalNetwork:false,
    legacyContent:'unchanged backend-rendered iframe hosted by actual legacy dist',errors},null,2));
  console.log('PASS: both actual dist entries, four save/reopen cases, real isolated SOP service, no cloud or production writes.');
}catch(error){
  if(page&&!page.isClosed()){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true}).catch(()=>{});console.error((await page.locator('body').innerText().catch(()=>'' )).slice(0,4000));}
  throw error;
}finally{await browser?.close();await server?.httpServer.close();await rm(scratch,{recursive:true,force:true});}
