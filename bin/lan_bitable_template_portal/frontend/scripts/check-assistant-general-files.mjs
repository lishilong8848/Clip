import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url)),project=path.resolve(root,'../../..');
const generated=spawnSync(path.join(project,'bin/.venv/Scripts/python.exe'),['-c',`
import sys,tempfile,json,base64
from pathlib import Path
sys.path.insert(0,'bin')
from test_lighthouse_stream import Store,ACTOR
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant.lighthouse_artifacts import document_bytes,preview_file
with tempfile.TemporaryDirectory() as directory:
    files=LighthouseFiles(Store(Path(directory)/'state.sqlite3'))
    examples=[('example.py',b'print("example")'),
        ('report.docx',document_bytes('report.docx',{'title':'测试报告','paragraphs':['来自隔离测试数据。']})),
        ('report.xlsx',document_bytes('report.xlsx',{'sheets':[{'name':'汇总','headers':['项目','数量'],'rows':[['测试',2]]}]})),
        ('report.pdf',document_bytes('report.pdf',{'title':'测试报告','paragraphs':['来自隔离测试数据。']})),
        ('report.pptx',document_bytes('report.pptx',{'slides':[{'title':'测试报告','bullets':['来自隔离测试数据。']}]}))]
    output=[]
    for name,content in examples:
        item=files.upload(ACTOR,name,content,extract=False)
        output.append({'file':item,'preview':preview_file(files,ACTOR,item['id']),'content':base64.b64encode(content).decode()})
    print(json.dumps(output,ensure_ascii=False))
`],{cwd:project,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'}});
assert.equal(generated.status,0,generated.stderr);
const artifacts=JSON.parse(generated.stdout),files=artifacts.map(item=>item.file);
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0},plugins:[{
  name:'isolated-generated-file-downloads',
  configurePreviewServer(server){
    // Chromium downloads can bypass route interception; serve only these synthetic files.
    server.middlewares.use((req,res,next)=>{
      const url=new URL(req.url,'http://127.0.0.1'),artifact=artifacts.find(item=>item.file.url===url.pathname);
      if(!artifact)return next();
      res.setHeader('Content-Type',artifact.file.mime);
      res.setHeader('Content-Disposition','attachment; filename="'+artifact.file.name+'"');
      res.end(Buffer.from(artifact.content,'base64'));
    });
  },
}]});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
try{
  for(const width of [1440,390]){
    const context=await browser.newContext({viewport:{width,height:900},acceptDownloads:true});
    const writes=[],errors=[],reads=[];
    await context.route('**/*',route=>{
      const request=route.request(),url=new URL(request.url());
      reads.push(url.pathname+url.search);
      if(url.origin!==origin)return route.abort();
      if(url.pathname==='/assets/connection-guard.js')return route.fulfill({contentType:'application/javascript',body:''});
      if(!url.pathname.startsWith('/api/'))return route.continue();
      if(request.method()!=='GET'){writes.push(url.pathname);return route.abort();}
      const ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'file-fixture',name:'文件验证',role:'admin',level:'L1',modules:['assistant']},scope_options:[{value:'D',label:'D楼'}]});
      if(url.pathname==='/api/assistant/appearance')return ok({shape:'weather-lighthouse-mini',color:'encre',size:56});
      if(url.pathname==='/api/assistant/icon-weather')return ok({available:false,city:'南通'});
      if(url.pathname==='/api/assistant/conversation')return ok({conversation_id:'files',configured:true,enabled:true,busy:false,model_name:'隔离模型',model_options:[],turns:[{operation_id:'files',question:'生成文件发给我',answer:'文件已生成，请在当前会话查看或下载。',status:'completed',output_files:files}]});
      const artifact=artifacts.find(item=>item.file.url===url.pathname);
      if(artifact){
        if(url.searchParams.get('preview'))return ok(artifact.preview);
        return route.fulfill({contentType:artifact.file.mime,headers:{'content-disposition':'attachment; filename="'+artifact.file.name+'"'},body:Buffer.from(artifact.content,'base64')});
      }
      return ok({items:[],records:[],scopes:[],settings:{}});
    });
    const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
    await page.goto(origin+'/assets/workspace-v2/index.html?view=my');
    await page.getByRole('button',{name:'打开灯塔助手',exact:true}).click();
    await page.locator('.output-file').first().waitFor();
    assert.equal(await page.locator('.output-file').count(),5);
    for(const artifact of artifacts){
      const download=page.waitForEvent('download');
      await page.locator('.output-file').getByRole('link',{name:'下载 '+artifact.file.name,exact:true}).click();
      const result=await download,stream=await result.createReadStream(),chunks=[];
      for await(const chunk of stream)chunks.push(chunk);
      assert.equal(result.suggestedFilename(),artifact.file.name,JSON.stringify({href:await page.locator('.output-file').getByRole('link',{name:'下载 '+artifact.file.name,exact:true}).getAttribute('href'),reads:reads.slice(-8)}));
      assert.deepEqual(Buffer.concat(chunks),Buffer.from(artifact.content,'base64'));
      if(artifact.preview.kind==='pdf')continue;
      await page.getByRole('button',{name:'预览 '+artifact.file.name,exact:true}).click();
      await page.locator('.artifact-preview .preview-body').waitFor();
      assert((await page.locator('.artifact-preview .preview-body').innerText()).trim());
      await page.getByRole('button',{name:'关闭文件预览',exact:true}).click();
    }
    assert.equal(await page.getByText('收件人（姓名 · 工号）',{exact:true}).count(),0);
    assert.deepEqual(writes,[],'file delivery never starts another Feishu send');
    assert.deepEqual(errors,[]);
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    await context.close();
  }
  console.log('PASS: actual generated code/Word/Excel/PDF/PPT bytes download in desktop/mobile conversation; private preview; no send/write requests.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
