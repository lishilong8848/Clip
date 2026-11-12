import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const legacy = process.env.INSPECTION_LEGACY_DIR || path.join(frontend, 'dist');
const workspace = process.env.INSPECTION_WORKSPACE_DIR || path.join(legacy, 'assets/workspace-v2');
const eopOnly = process.argv.includes('--eop-only');
const contentTypes = {'.html':'text/html', '.js':'text/javascript', '.css':'text/css', '.svg':'image/svg+xml'};
const server = createServer(async (request, response) => {
  const url = new URL(request.url, 'http://127.0.0.1');
  const isWorkspace = url.pathname.startsWith('/assets/workspace-v2/');
  const root = path.resolve(isWorkspace ? workspace : legacy);
  const name = isWorkspace ? url.pathname.slice('/assets/workspace-v2/'.length)
    : url.pathname.startsWith('/assets/') ? url.pathname.slice(1) : 'index.html';
  const file = path.resolve(root, name);
  try {
    if (!file.startsWith(root + path.sep)) throw new Error('invalid file');
    response.writeHead(200, {'content-type':contentTypes[path.extname(file)] || 'application/octet-stream'});
    response.end(await readFile(file));
  } catch {
    response.writeHead(404);
    response.end();
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const origin = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({headless:true});
const errors = [];
const scenario = {id:'recEop',revision:'v1',editable:true,
  fields:{'场景名称':'隔离测试场景','场景编号':'EOP-001','专业':'电气','系统/设备':'变压器','原优先级':'P0-高',
    '处置正文（原文）':'1、首轮核对\n（1）现场确认\n2、通报\n1、复核 2、记录','原通告模板':'测试模板','原演练场景':'','原EOP精炼（参考）':'测试精炼'},
  options:{'专业':['电气'],'系统/设备':['变压器'],'原优先级':['P0-高']},
  steps:[{text:'检查设备',images:['photo1']},{text:'记录结果',images:[]}],
  images:[{token:'photo1',name:'已关联图片',url:'/api/eop-scenarios/recEop/image/fixture'},
    {token:'photo2',name:'待分配图片',url:'/api/eop-scenarios/recEop/image/fixture'}]};

function entry(route, legacyMode) {
  return legacyMode ? origin + route + '?frontend=legacy'
    : origin + '/assets/workspace-v2/index.html?__page=' + encodeURIComponent(route) + '&scope=D';
}
async function fixture(page) {
  const duty={scope:'D',date:'2026-11-03',shift:'白'};
  const template={name:'D.xlsx',scope:'D',profession:'电气',sheets:[{name:'配电间巡检表',rows:[['项目','读数'],['电压',null]],merges:[]}]};
  const state={duty,can_edit:true,view_scope:'D',history_scopes:['D'],history:[],template_version:'v1',
    completed_rounds:1,round_limit:2,scheduled:false,active:null};
  const reading={template:template.name,sheet:template.sheets[0].name,cell:'B2',value:'210'};
  let drafts=0;
  page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/api/**',async route=>{
    const request=route.request(),url=new URL(request.url());
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'inspection-fixture'}});
    if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'fixture-user',name:'隔离测试',
      role:'admin',level:'L0',category:'engineer',modules:['daily','drills']},allowed_scopes:['D'],
      scope_options:[{value:'D',label:'D楼'}],default_scope:'D'});
    if(url.pathname==='/api/daily-inspection'||url.pathname==='/api/daily-inspection/status')return ok(state);
    if(url.pathname==='/api/daily-inspection/templates')return ok({scope:'D',version:'v1',templates:[template]});
    if(url.pathname==='/api/daily-inspection/source')return ok({scope:'D',devices:[],batch:1});
    if(url.pathname==='/api/daily-inspection/day-sheet')return ok({scope:'D',date:duty.date,draft_key:'fixture-date',
      template_version:'v1',can_edit_history:true,original_templates:[template],
      rounds:[{id:'recFixture',round_id:'old-round',shift:'白',round:1,status:'已归档',version:'v1',original_cells:[reading]}]});
    if(url.pathname==='/api/daily-inspection/history-edit'){
      const body=request.postDataJSON();
      assert.equal(body.id,'recFixture');
      assert.equal(body.cells[0].value,'211');
      drafts++;
      reading.value='211';
      return ok({id:'recFixture',scope:'D',version:'v2'});
    }
    if(url.pathname==='/api/eop-scenarios')return ok({items:[{id:scenario.id,fields:scenario.fields}]});
    if(url.pathname==='/api/eop-scenarios/recEop')return ok(scenario);
    return ok({items:[],revision:0,value:null});
  });
  return ()=>drafts;
}

try {
  if(!eopOnly)for(const legacyMode of [false,true]){
    const page=await browser.newPage({viewport:{width:1440,height:900}});
    const writes=await fixture(page);
    await page.goto(entry('/daily-inspection',legacyMode));
    const book=page.locator('.inspection-workbook');
    await book.getByRole('cell',{name:'B2 210'}).click();
    const editor=book.getByRole('textbox',{name:'编辑 B2'});
    await editor.fill('211');
    const saved=page.waitForResponse(response=>response.url().includes('/daily-inspection/history-edit')&&response.ok());
    await editor.press('Enter');
    await saved;
    await book.getByRole('cell',{name:'B2 211'}).waitFor();
    assert.equal(writes(),1);
    assert.equal(await book.getByRole('cell',{name:'B2 211'}).count(),1);
    assert.equal(await page.locator('.inspection-heading').getByRole('button',{name:'返回'}).count(),Number(legacyMode));
    if(legacyMode){
      await page.locator('.inspection-heading').getByRole('button',{name:'返回'}).click();
      await page.waitForURL(url=>url.pathname==='/');
    }
    await page.close();
  }
  if(eopOnly) {
  for (const legacyMode of [false,true]) for (const width of [1440,402,440]) {
    const page=await browser.newPage({viewport:{width,height:900}});
    await fixture(page);
    await page.goto(entry('/eop-scenarios',legacyMode));
    const root=page.locator('.eop-page');
    await root.getByRole('button',{name:/隔离测试场景/}).click();
    const refined=root.getByRole('region',{name:'原EOP精炼（参考）'});
    const template=root.getByRole('region',{name:'原通告模板'});
    await refined.getByText('测试精炼').waitFor();
    const left=await refined.boundingBox();
    const right=await template.boundingBox();
    if(width>760) assert(left.x<right.x && Math.abs(left.width-right.width)<2 && left.y===right.y);
    else assert(left.y<right.y && Math.abs(left.width-right.width)<2);
    assert.equal(await root.locator('.disposition-group').count(),2);
    assert.equal(await root.locator('.disposition-group').last().locator('li').count(),2);
    assert.match(await root.getByText('现场确认',{exact:true}).locator('..').getAttribute('style'),/--indent: 1/);
    const gallery=root.locator('.eop-step-media').first();
    assert.equal(await gallery.locator('figure').first().isVisible(),false);
    await gallery.locator('summary').click();
    await gallery.getByRole('button',{name:'预览 已关联图片'}).waitFor();
    await gallery.locator('summary').click();
    if (width>760) {
      const work=await root.locator('.eop-work').boundingBox();
      const pool=await root.locator('.eop-pool').boundingBox();
      assert(pool.x>work.x+work.width-2,'desktop places unassigned photos beside the steps');
    }
    await root.getByRole('button',{name:'编辑',exact:true}).click();
    const code=root.getByRole('textbox',{name:'场景编号'});
    assert.equal(await code.isVisible(),false);
    await root.locator('.eop-meta summary').click();
    assert.equal(await code.inputValue(),'EOP-001');
    await root.getByRole('textbox',{name:'场景名称'}).fill('隔离测试场景 · 已编辑');
    await code.fill('EOP-002');
    let submitted;
    await page.route('**/api/eop-scenarios/recEop', async route => {
      if (route.request().method()!=='PUT') return route.fallback();
      submitted=route.request().postDataJSON();
      await route.fulfill({json:{saved:true,record:{...scenario,revision:'v2',fields:submitted.fields,steps:submitted.steps}}});
    });
    await root.getByRole('button',{name:'保存记录'}).click();
    await root.getByText('记录已保存到飞书。').waitFor();
    assert.equal(submitted.fields['场景编号'],'EOP-002');
    assert.equal(submitted.fields['处置正文（原文）'],scenario.fields['处置正文（原文）']);
    assert.deepEqual(submitted.steps[0].images,['photo1']);
    await root.getByRole('textbox',{name:'第 1 步内容'}).fill('未保存的步骤');
    await root.getByRole('button',{name:'返回场景列表'}).click();
    await page.getByRole('dialog').getByText('有未保存的更改').waitFor();
    await page.getByRole('button',{name:'继续编辑'}).click();
    assert.equal(await root.getByRole('textbox',{name:'第 1 步内容'}).inputValue(),'未保存的步骤');
    await root.getByRole('button',{name:'返回场景列表'}).click();
    await page.getByRole('button',{name:'放弃更改'}).click();
    await root.getByRole('button',{name:/隔离测试场景/}).waitFor();
    const back=root.getByRole('button',{name:'返回',exact:true});
    if (legacyMode) {
      await back.click();
      await page.waitForURL(url => url.pathname==='/' && url.searchParams.get('entry')==='special');
    } else {
      assert.equal(await back.count(),0,'新版场景列表使用工作台导航');
    }
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    await page.close();
    console.log('EOP 返回、未保存保护和布局通过',legacyMode?'旧版':'新版',width);
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:900}});
    await fixture(page);
    let releaseList, listRequested;
    const pending=new Promise(resolve=>{releaseList=resolve;});
    const requested=new Promise(resolve=>{listRequested=resolve;});
    await page.route(url=>url.pathname==='/api/eop-scenarios',async route=>{
      listRequested();
      await pending;
      try { await route.abort(); } catch { /* Navigation can cancel the request first. */ }
    });
    await page.goto(entry('/eop-scenarios',true));
    await requested;
    await page.locator('.eop-page').getByRole('button',{name:'返回',exact:true}).click();
    await page.waitForURL(url=>url.pathname==='/' && url.searchParams.get('entry')==='special');
    releaseList();
    await page.close();
    console.log('EOP 列表读取期间可返回');
  }
  }
  assert.deepEqual(errors,[]);
} catch (error) {
  if (process.env.INSPECTION_SCREENSHOT_DIR) {
    for (const context of browser.contexts()) for (const page of context.pages()) {
      await page.screenshot({path:path.join(process.env.INSPECTION_SCREENSHOT_DIR,'inspection-error.png')}).catch(()=>{});
      console.log('错误时页面滚动状态',await page.evaluate(()=>({scrollY:window.scrollY,
        back:document.querySelector('.inspection-heading button')?.getBoundingClientRect().toJSON(),
        topbar:document.querySelector('.app-topbar')?.getBoundingClientRect().toJSON(),
        under:document.elementFromPoint(30,25)?.outerHTML.slice(0,200)})).catch(()=>null));
    }
  }
  throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}

