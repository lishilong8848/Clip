import assert from 'node:assert/strict';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {createServer} from 'node:http';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const temp=await mkdtemp(path.join(os.tmpdir(),'clipflow-tool-import-'));
const dist=path.resolve(process.env.INSPECTION_LEGACY_DIR || path.join(root,'dist'));
const server=createServer(async(request,response)=>{
  const url=new URL(request.url,'http://localhost');
  const name=url.pathname.startsWith('/assets/')?url.pathname.slice(1):'index.html';
  const file=path.resolve(dist,name);
  try{
    if(!file.startsWith(dist+path.sep))throw new Error('Invalid asset');
    response.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css'})[path.extname(file)]||'application/octet-stream');
    response.end(await readFile(file));
  }catch{response.writeHead(404);response.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser=await chromium.launch();
try{
  const page=await browser.newPage();
  page.setDefaultTimeout(12000);
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  const month='2026-11', base='/api/tool-self-check', uploads=[], submissions=[];
  let ready=false;
  const planItems=[
    {asset_id:'a',scope:'A',name:'电导率仪',kind:'self',due:'2026-11-10'},
    {asset_id:'a',scope:'A',name:'电导率仪',kind:'external',due:'2026-11-12'},
    {asset_id:'e',scope:'E',name:'风速仪',kind:'self',due:'2026-11-13'},
  ];
  await page.route('**/api/**',async route=>{
    const request=route.request(),url=new URL(request.url());
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-tools',name:'测试人员',role:'admin',level:'L0',category:'engineer',tool_management_enabled:true,modules:['risk_inspection']},
      allowed_scopes:['A'],scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'tool-fixture'}});
    if(url.pathname===base)return ok({month,orders:[],followups:[],templates:[],can_manage_templates:true,
      can_create_orders:true,can_download_month:true,archived_at:'2026-11-06',actor_name:'测试人员'});
    if(url.pathname===base+'/orders/batch-create'){
      if(request.method()==='GET')return ok({month,items:ready?planItems:[],archived:true,limit:200,
        inventory_at:ready?'2026-11-11':'',tool_count:2});
      submissions.push(request.postDataJSON());
      return ok({month,created_count:2,existing_count:0});
    }
    if(url.pathname===base+'/templates'){
      const name=url.searchParams.get('name');
      uploads.push(name);
      if(name==='重复')return route.fulfill({status:409,json:{ok:false,error:'相同模板已导入。'}});
      if(name==='损坏')return route.fulfill({status:400,json:{ok:false,error:'XLSX 模板无法读取。'}});
      return ok({id:'imported'});
    }
    return ok({items:[]});
  });
  await page.goto('http://127.0.0.1:'+server.address().port+'/risk-inspections?frontend=legacy&view=tool-self-check');
  try{await page.getByRole('button',{name:'新增本月工单'}).click();}
  catch(error){console.error(await page.locator('body').innerText(),errors);throw error;}
  await page.getByText('工具底表尚未完成首次同步，稍后可重读计划。').waitFor();
  assert.equal(await page.getByText('可创建 0 项').count(),0);
  ready=true;
  await page.getByRole('button',{name:'重读计划'}).click();
  await page.getByText('可创建 3 项').waitFor();
  await page.getByLabel('筛选计划楼栋').selectOption('A');
  await page.getByRole('checkbox',{name:'全选筛选结果'}).click();
  await page.getByRole('button',{name:'创建 2 张工单'}).click();
  await page.getByText('已创建 2 张工单。').waitFor();
  assert.deepEqual(submissions,[{month,orders:[{asset_id:'a',kind:'self'},{asset_id:'a',kind:'external'}]}]);

  await page.getByRole('button',{name:'工具与模板'}).click();
  await page.getByRole('button',{name:'导入文件夹'}).waitFor();
  const folder=await mkdtemp(path.join(temp,'uploads-'));
  for(const file of ['有效.xlsx','重复.xlsx','损坏.xlsx','~$临时.xlsx','说明.txt'])
    await writeFile(path.join(folder,file),'browser upload fixture');
  await page.locator('input[webkitdirectory]').setInputFiles(folder);
  await page.getByText('已导入 1 份，跳过重复 1 份，失败 1 份。').waitFor();
  assert.deepEqual(uploads.sort(),['有效','重复','损坏'].sort());
  assert.match(await page.getByRole('listitem').filter({hasText:'损坏.xlsx'}).innerText(),/XLSX 模板无法读取/);
  assert.deepEqual(errors,[]);
  console.log('工具月度批量筛选创建、空状态和文件夹导入反馈通过。');
}finally{
  await browser.close();
  await new Promise(resolve=>server.close(resolve));
  if(path.dirname(path.resolve(temp))===path.resolve(os.tmpdir()))await rm(temp,{recursive:true,force:true});
}
