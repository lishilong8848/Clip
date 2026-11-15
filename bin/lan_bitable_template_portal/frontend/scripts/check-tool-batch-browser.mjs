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
  const month='2026-11', base='/api/tool-self-check', uploads=[], submissions=[], clears=[];
  let ready=false, recovered=false;
  const report = { id:'old-report',asset_id:'report-asset', month:'2026-10', scope:'A', kind:'external',
    revision:3, status:'pending', due:'2026-10-01', tool_name:'跨月送检表', model:'X-1', location:'工具间',
    owner_name:'测试人员', can_edit:true, report_name:'待核对.pdf', actual:'', outcome:'', records:[], sync:[], history:[] };
  const removed = { ...report, id:'removed', asset_id:'removed-asset', month, status:'cancelled', tool_name:'本月已取消工单',
    revision:4, can_edit:false, can_restore:true };
  const planItems=[
    {asset_id:'a',scope:'A',name:'电导率仪',kind:'self',due:'2026-11-10'},
    {asset_id:'b',scope:'A',name:'低压接地线',kind:'external',due:'2026-11-12'},
    {asset_id:'e',scope:'E',name:'风速仪',kind:'self',due:'2026-11-13'},
  ];
  await page.route('**/api/**',async route=>{
    const request=route.request(),url=new URL(request.url());
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-tools',name:'测试人员',role:'admin',level:'L0',category:'engineer',tool_management_enabled:true,modules:['risk_inspection']},
      allowed_scopes:['A'],scope_options:[{value:'A',label:'A楼'}],default_scope:'A'});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'tool-fixture'}});
    if(url.pathname===base)return ok({month,orders:[recovered ? {...removed,status:'pending',can_restore:false} : removed],
      carryovers:[report],followups:[],templates:[],can_manage_templates:true,can_create_orders:true,
      can_download_month:true,archived_at:'2026-11-06',archive_status:'snapshot',actor_name:'测试人员'});
    if(url.pathname===base+'/orders/old-report') return ok(report);
    if(url.pathname===base+'/orders/removed/restore'){
      assert.deepEqual(request.postDataJSON(),{revision:4});
      recovered=true;
      return ok({ ...removed,status:'pending',can_restore:false });
    }
    if(url.pathname===base+'/inventory/report-asset/history'){
      return ok({asset:{name:report.tool_name,scope:'A',model:'X-1'},orders:[],next_external_due:'2026-12-01'});
    }
    if(url.pathname===base+'/orders/clear-month'){
      if(request.method()==='GET')return ok({month,fingerprint:'fixture',pending_count:1,deleted_count:2});
      clears.push(request.postDataJSON());
      ready=true;
      return ok({cleared_count:3});
    }
    if(url.pathname===base+'/orders/batch-create'){
      if(request.method()==='GET')return ok({month,items:ready?planItems:[],archived:true,limit:200,
        inventory_at:ready?'2026-11-11':'',tool_count:2});
      submissions.push(request.postDataJSON());
      return ok({month,created_count:2,existing_count:0});
    }
    if(url.pathname===base+'/templates'){
      if(request.method()==='GET')return ok({templates:[],legacy_templates:[]});
      const name=url.searchParams.get('name');
      uploads.push(name);
      if(name==='重复')return route.fulfill({status:409,json:{ok:false,error:'相同模板已导入。'}});
      if(name==='损坏')return route.fulfill({status:400,json:{ok:false,error:'XLSX 模板无法读取。'}});
      return ok({id:'imported'});
    }
    return ok({items:[]});
  });
  await page.goto('http://127.0.0.1:'+server.address().port+'/risk-inspections?frontend=legacy&view=tool-self-check&tool_view=orders&scope=A');
  try{await page.getByRole('button',{name:'新增本月工单'}).click();}
  catch(error){console.error(await page.locator('body').innerText(),errors);throw error;}
  await page.getByText('工具底表尚未完成首次同步，稍后可重读计划。').waitFor();
  assert.equal(await page.getByText('可创建 0 项').count(),0);
  await page.getByRole('button',{name:'清理本月工单'}).click();
  await page.getByText(/1 张未提交工单/).waitFor();
  await page.getByRole('button',{name:'关闭',exact:true}).click();
  assert.equal(clears.length,0);
  await page.getByRole('button',{name:'清理本月工单'}).click();
  await page.getByRole('button',{name:'确认清理'}).click();
  assert.deepEqual(clears,[{month,fingerprint:'fixture'}]);
  await page.getByText('可创建 3 项').waitFor();
  await page.getByLabel('筛选计划楼栋').selectOption('A');
  await page.getByRole('checkbox',{name:'全选筛选结果'}).click();
  await page.getByRole('button',{name:'创建 2 张工单'}).click();
  await page.getByText('已创建 2 张工单。').waitFor();
  assert.deepEqual(submissions,[{month,orders:[{asset_id:'a',kind:'self'},{asset_id:'b',kind:'external'}]}]);

  await page.getByRole('button',{name:'跨月待办 1'}).click();
  const carryover=page.getByRole('row').filter({hasText:'跨月送检表'});
  await carryover.getByRole('button',{name:'履历'}).click();
  await page.getByText('下次送检 2026-12-01').waitFor();
  await page.keyboard.press('Escape');
  await carryover.getByRole('button',{name:'继续填写'}).click();
  await page.getByRole('link',{name:'查看报告'}).waitFor();
  assert.equal(await page.getByRole('button',{name:'提交工单'}).isDisabled(),true);
  await page.getByLabel('实际检测日期').fill('2026-10-31');
  await page.getByLabel('报告编号').fill('REC-12');
  await page.getByLabel('检测机构').fill('测试计量院');
  await page.getByLabel('检测结论').selectOption('qualified');
  await page.getByRole('checkbox',{name:/已核对报告/}).check();
  assert.equal(await page.getByRole('button',{name:'提交工单'}).isEnabled(),true);
  await page.getByRole('button',{name:'返回工单列表'}).click();
  await page.getByRole('button',{name:'放弃填写'}).click();
  await page.getByLabel('工单状态').click();
  await page.getByText('已取消',{exact:true}).last().click();
  await page.getByRole('button',{name:'恢复原单'}).click();
  await page.getByRole('button',{name:'确认恢复'}).click();
  await page.getByText('已恢复原工单，可继续填写。').waitFor();

  await page.getByRole('button',{name:'工具与模板'}).click();
  await page.getByRole('button',{name:'导入文件夹'}).waitFor();
  const folder=await mkdtemp(path.join(temp,'uploads-'));
  for(const file of ['有效.xlsx','重复.xlsx','损坏.xlsx','~$临时.xlsx','说明.txt'])
    await writeFile(path.join(folder,file),'browser upload fixture');
  await page.locator('input[webkitdirectory]').setInputFiles(folder);
  await page.getByText('已导入 1 份，跳过重复 1 份，失败 1 份。').waitFor();
  assert.deepEqual(uploads.sort(),['有效','重复','损坏'].sort());
  assert.match(await page.getByRole('listitem').filter({hasText:'损坏.xlsx'}).innerText(),/XLSX 模板无法读取/);

  const v2 = new URL('/assets/workspace-v2/index.html', 'http://127.0.0.1:'+server.address().port);
  v2.searchParams.set('__page','/risk-inspections');
  v2.searchParams.set('view','tool-self-check');
  v2.searchParams.set('tool_view','orders');
  v2.searchParams.set('scope','A');
  await page.setViewportSize({width:390,height:844});
  await page.goto(v2.href);
  await page.getByRole('button',{name:'清理本月工单'}).click();
  await page.getByRole('button',{name:'确认清理'}).click();
  await page.getByText('可创建 3 项').waitFor();
  assert.equal(clears.length,2);
  await page.getByRole('button',{name:'跨月待办 1'}).click();
  await page.getByRole('row').filter({hasText:'跨月送检表'}).getByRole('button',{name:'继续填写'}).click();
  await page.getByRole('link',{name:'查看报告'}).waitFor();
  assert(await page.locator('.tool-report-file').evaluate(file => file.scrollWidth <= file.clientWidth + 1));
  assert.deepEqual(errors,[]);
  console.log('新旧版月度计划、跨月续办、送检核对、工具履历、单张恢复和文件夹导入浏览器检查通过。');
}finally{
  await browser.close();
  await new Promise(resolve=>server.close(resolve));
  if(path.dirname(path.resolve(temp))===path.resolve(os.tmpdir()))await rm(temp,{recursive:true,force:true});
}
