import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdir,readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {preview} from 'vite';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),project=path.resolve(root,'../../..');
const python=path.join(project,'bin/.venv',process.platform==='win32'?'Scripts/python.exe':'bin/python');
const fixture=JSON.parse(execFileSync(python,['-c',`
import json,sys,tempfile
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,'bin')
from lan_bitable_template_portal import drill_management as d
from lan_bitable_template_portal.state_store import LanPortalStateStore
from test_drill_untimed_steps import _drill_fixture
source=Path('D:/下载/21V-JSNTFOC-EOP-KT-308 机房局部高温故障应急处理流程V1.5.xlsx')
with tempfile.TemporaryDirectory() as folder:
 service=d.DrillManagementService(LanPortalStateStore(Path(folder)/'state.sqlite3'),data_root=Path(folder)/'drills')
 with patch.object(d,'_step_duration_minutes',side_effect=d.parse_duration_minutes):
  failed=service.create_definition(name='机房局部高温演练',year=2026,month=10,file_name='template.xlsx',source=source.read_bytes() if source.is_file() else _drill_fixture(14,predicted='7分'))
 recovered=service.list_definitions('2026-10')[0]
 for item in [failed,recovered]: item['source'].pop('path',None)
 print(json.dumps({'failed':failed,'recovered':recovered},ensure_ascii=False))
`],{cwd:project,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'},maxBuffer:4*1024*1024}));
const expected=fixture.recovered.configuration.steps.length;
assert(expected>0);
const shots=path.join(project,'output/playwright/drill-recognition');
await mkdir(shots,{recursive:true});
const server=await preview({root,logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const browser=await chromium.launch({headless:true});
let page;
try {
 for(const version of ['legacy','v2'])for(const width of [1440,375]){
  const context=await browser.newContext({viewport:{width,height:width<700?667:960},reducedMotion:'reduce'});
  page=await context.newPage();page.setDefaultTimeout(20000);
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  let listing=[structuredClone(fixture.failed)],reads=0,writes=0,uploadMode='failed';
  await context.route('**/*',async route=>{
   const request=route.request(),url=new URL(request.url()),p=url.pathname;
   if(url.origin!==origin)return route.abort();
   if(p==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
   if(!p.startsWith('/api/'))return route.continue();
   const ok=data=>route.fulfill({json:{ok:true,data}});
   const scopes=['A','B','C','D','E'].map(value=>({value,label:value+'楼'}));
   if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'drill-recovery-fixture',name:'隔离验收人员',role:'admin',level:'L0',modules:['drills']},scope_options:scopes});
   if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'drill-recovery-fixture'}});
   if(p==='/api/drills/bootstrap')return ok({drills:listing,scopes,is_admin:true,people:[]});
   if(p==='/api/drills'&&request.method()==='GET'){reads++;return ok({items:listing});}
   if(p==='/api/drills'&&request.method()==='POST'){
    writes++;
    const failed={...structuredClone(fixture.failed),drill_id:'uploaded-'+writes,name:'上传识别回归'+writes};
    if(uploadMode==='failed')failed.detection_error='工作表未识别到完整的演练步骤表头。';
    const current=uploadMode==='recovered'?{...structuredClone(fixture.recovered),drill_id:failed.drill_id,name:failed.name}:failed;
    listing=[current];return ok({drill:failed});
   }
   if(p.endsWith('/configuration')){
    const body=request.postDataJSON();assert.equal(body.expected_version,listing[0].version,'save uses the recovered version, not the old upload response');
    listing[0]={...listing[0],version:listing[0].version+1,configuration:body.configuration};return ok({drill:listing[0]});
   }
   if(request.method()!=='GET'){errors.push('unexpected write '+p);return route.fulfill({status:400,json:{ok:false,error:'unexpected fixture write'}});}
   return ok({items:[],records:[],stats:{},summary:{},total:0});
  });
  const target=version==='v2'?'/assets/workspace-v2/index.html?__page=/drill-management&mode=admin&month=2026-10':'/drill-management?mode=admin&month=2026-10';
  await page.goto(origin+target);
  const panel=page.locator('.configuration-panel');
  await panel.getByText('未识别到步骤，请检查步骤区域映射。',{exact:true}).waitFor();
  await panel.getByText(fixture.failed.detection_error,{exact:true}).waitFor();
  await page.waitForFunction(()=>!document.querySelector('.drill-page button[aria-label="刷新演练数据"]')?.disabled);
  listing=[structuredClone(fixture.recovered)];
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('clipflow-read-cache-updated',{detail:{path:'/api/drills?month=2026-10'}})));
  const slots=panel.getByRole('spinbutton',{name:'步骤签名人数',exact:true});
  await slots.first().waitFor();assert.equal(await slots.count(),expected);
  assert.equal(await panel.getByText('步骤识别未完成',{exact:true}).count(),0);
  const original=await slots.first().inputValue();await slots.first().fill(original==='1'?'2':'1');
  const manual=await slots.first().inputValue(),before=reads;
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('clipflow-read-cache-updated',{detail:{path:'/api/drills?month=2026-10'}})));
  await page.waitForTimeout(150);
  assert.equal(reads,before,'background refresh preserves unsaved configuration');
  assert.equal(await slots.first().inputValue(),manual);
  await page.reload();await slots.first().waitFor();assert.equal(await slots.first().inputValue(),original);
  async function upload(){
   await page.getByRole('button',{name:'上传演练文件',exact:true}).click();
   await page.locator('.upload-panel input[type=file]').setInputFiles({name:'识别回归.xlsx',mimeType:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',buffer:Buffer.from('isolated upload')});
   await page.getByRole('button',{name:'上传并识别',exact:true}).click();
   await page.locator('.upload-panel').waitFor({state:'hidden'});
  }
  await upload();
  await page.locator('.drill-page').getByText('文件已上传，但步骤识别未完成，请查看模板中的具体原因。',{exact:true}).waitFor();
  await panel.getByText('工作表未识别到完整的演练步骤表头。',{exact:true}).waitFor();
  assert.equal(await page.getByText('文件已上传并完成初步识别，请核对配置后发布。',{exact:true}).count(),0);
  uploadMode='recovered';await upload();await slots.first().waitFor();
  assert.equal(await slots.count(),expected,'fresh list recovery must not be overwritten by the older POST response');
  await page.getByRole('button',{name:'保存配置',exact:true}).click();await page.locator('.drill-page').getByText('模板配置已保存',{exact:true}).waitFor();
  await panel.locator('details').filter({hasText:'步骤与签名人数'}).scrollIntoViewIfNeeded();
  await page.screenshot({path:path.join(shots,`${version}-${width}.png`),fullPage:true});
  assert.deepEqual(errors,[]);assert.equal(writes,2);
  await context.close();
 }
 console.log('PASS: legacy/V2 PC/mobile show recovered persisted steps, refresh clean drafts, preserve manual edits, report upload detection failures and keep the latest version. All HTTP writes mocked.');
}catch(error){if(page&&!page.isClosed()){await page.screenshot({path:path.join(shots,'failure.png'),fullPage:true});console.error(await page.locator('body').innerText());}throw error;}
finally{await browser.close();await server.close();}
