import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const frontend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const legacy = process.env.INSPECTION_LEGACY_DIR || path.join(frontend, 'dist');
const workspace = process.env.INSPECTION_WORKSPACE_DIR || path.join(legacy, 'assets/workspace-v2');
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
const duty = {scope:'D',date:'2026-11-03',shift:'白'};
const device = {id:'D:1',name:'隔离测试柜',profession:'电气',
  sections:[{location:'一层',form:'巡检',integrity:'完整',missing:'',note:''}],points:[],
  parameters:[{id:'current',name:'电流',reference:10,section:0}]};
const review = {values:{current:'10'},result:'正常',note:'',revision:1};
const archive = {id:'recFixture',scope:'D',date:'2026-11-03',shift:'白班',group:'A班',round:1,
  completed:1793673000000,actor:'测试人员',confirmed:1,devices:1,url:'#',editor:'',modified:null};
const detail = {...archive,version:'v1',can_edit:true,devices_data:[{id:'D:1',order:1,name:'隔离测试柜',
  profession:'电气',result:'正常',note:'',rows:[{id:'current',name:'电流',reference:10,value:'10',
    location:'一层',source:'',read_only:false}]}]};
const scenario = {id:'recEop',revision:'v1',editable:true,
  fields:{'场景名称':'隔离测试场景','专业':'电气','系统/设备':'变压器','原优先级':'高',
    '处置正文（原文）':'测试正文','原通告模板':'','原演练场景':'','原EOP精炼（参考）':''},
  options:{'专业':['电气'],'系统/设备':['变压器'],'原优先级':['高']},
  steps:[{text:'检查设备',images:[]},{text:'记录结果',images:[]}],
  images:[{token:'photo1',name:'测试图片',url:'/api/eop-scenarios/recEop/image/fixture'}]};

function entry(route, legacyMode) {
  return legacyMode ? origin + route + '?frontend=legacy'
    : origin + '/assets/workspace-v2/index.html?__page=' + encodeURIComponent(route) + '&scope=D';
}
async function fixture(page,layout=null) {
  const state = {duty,completed_rounds:0,round_limit:2,history_scopes:['D'],history:[],active:{
    id:'round-fixture',duty,round:1,pending:false,draft_key:'fixture-user',reviews:{'D:1':review},
    review_version:1,batch:{batch:1793671200000,count:1,devices:[device]}}};
  let releaseDraft;
  const gate = new Promise(resolve => {releaseDraft=resolve;});
  let draftRequested;
  const waiting = new Promise(resolve => {draftRequested=resolve;});
  let releaseHistorySave, historySaveRequested;
  const historyGate = new Promise(resolve => {releaseHistorySave=resolve;});
  const historyWaiting = new Promise(resolve => {historySaveRequested=resolve;});
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/**', async route => {
    const request = route.request(), url = new URL(request.url());
    const ok = data => route.fulfill({json:{ok:true,data}});
    if (url.pathname==='/api/health') return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'inspection-fixture'}});
    if (url.pathname==='/api/auth/status') return ok({logged_in:true,user:{open_id:'fixture-user',
      name:'隔离测试',role:'admin',level:'L0',category:'engineer',modules:['daily','drills']},
      allowed_scopes:['D'],scope_options:[{value:'D',label:'D楼'}],default_scope:'D'});
    if (url.pathname.startsWith('/api/workspace-layouts/')) return ok(layout?{revision:1,value:layout}:{revision:0,value:null});
    if (url.pathname==='/api/daily-inspection') return ok(state);
    if (url.pathname==='/api/daily-inspection/status') return ok({unchanged:true,history_scopes:['D']});
    if (url.pathname==='/api/daily-inspection/history') return ok({history:[archive]});
    if (url.pathname==='/api/daily-inspection/history-record') return ok(detail);
    if (url.pathname==='/api/daily-inspection/history-edit') {
      historySaveRequested();
      await historyGate;
      return ok(detail);
    }
    if (url.pathname==='/api/daily-inspection/draft') {
      draftRequested();
      await gate;
      const submitted=request.postDataJSON();
      state.active.reviews={...state.active.reviews,...submitted.reviews};
      return ok(state);
    }
    if (url.pathname==='/api/eop-scenarios') return ok({items:[{id:scenario.id,fields:scenario.fields}]});
    if (url.pathname==='/api/eop-scenarios/recEop') return ok(scenario);
    return ok({items:[],revision:0,value:null});
  });
  return {waiting,releaseDraft,historyWaiting,releaseHistorySave};
}
async function historyEdit(page) {
  await page.getByRole('button',{name:'历史巡检'}).click();
  const history=page.locator('.inspection-history-page');
  await history.getByRole('button',{name:/D楼.*白班/}).click();
  await history.getByRole('button',{name:'修改记录'}).click();
  await history.getByRole('textbox',{name:/电流.*现场读数/}).fill('12');
  return history;
}
async function discardDialog(page,accept) {
  const dialog=page.getByRole('dialog',{name:'有未保存的历史巡检修改'});
  await dialog.waitFor();
  await dialog.getByRole('button',{name:accept?'放弃修改':'继续编辑'}).click();
}

try {
  for (const legacyMode of [false,true]) {
    for (const width of [402,440,1440]) {
      const page=await browser.newPage({viewport:{width,height:900}});
      const mock=await fixture(page);
      await page.goto(entry('/daily-inspection',legacyMode));
      const root=page.locator('.inspection-page');
      const reading=root.getByRole('textbox',{name:'电流现场读数'});
      await reading.fill('11');
      await root.getByRole('button',{name:'保存本轮草稿'}).click();
      await mock.waiting;
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await root.getByRole('alert').getByText('巡检操作正在处理，请稍后再离开。').waitFor();
      mock.releaseDraft();
      await root.getByText('本轮草稿已共享给本楼当班人员。').waitFor();

      const history=await historyEdit(page);
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await discardDialog(page,false);
      assert.equal(await history.getByRole('textbox',{name:/电流.*现场读数/}).inputValue(),'12');
      await history.getByRole('button',{name:'保存到原记录'}).click();
      await mock.historyWaiting;
      await root.getByRole('button',{name:'返回本班'}).click();
      await history.getByRole('alert').getByText('当前操作正在处理，请稍后再离开。').waitFor();
      mock.releaseHistorySave();
      await history.getByText('已更新原飞书巡检记录。').waitFor();
      await root.getByRole('button',{name:'返回本班'}).click();
      assert.equal(await reading.inputValue(),'11');

      await historyEdit(page);
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await discardDialog(page,true);
      await history.getByRole('button',{name:/D楼.*白班/}).waitFor();
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await reading.waitFor();
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await page.waitForURL(url => legacyMode ? url.pathname==='/' && url.searchParams.get('entry')==='tools'
        : url.searchParams.get('__page')==='/' && url.searchParams.get('view')==='functions');
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
      await page.close();
      console.log('巡检返回、历史编辑和提交保护通过',legacyMode?'旧版':'新版',width);
    }
  }
  for (const [legacyMode,width] of [[false,1440],[false,402],[false,440],[true,402],[true,440]]) {
    const page=await browser.newPage({viewport:{width,height:900}});
    await fixture(page);
    await page.goto(entry('/eop-scenarios',legacyMode));
    const root=page.locator('.eop-page');
    await root.getByRole('button',{name:/隔离测试场景/}).click();
    if (width>760) {
      const work=await root.locator('.eop-work').boundingBox();
      const pool=await root.locator('.eop-pool').boundingBox();
      assert(pool.x>work.x+work.width-2,'desktop places unassigned photos beside the steps');
    }
    await root.getByRole('button',{name:'编辑',exact:true}).click();
    await root.getByRole('textbox',{name:'第 1 步内容'}).fill('未保存的步骤');
    await root.getByRole('button',{name:'返回场景列表'}).click();
    await page.getByRole('dialog').getByText('有未保存的更改').waitFor();
    await page.getByRole('button',{name:'继续编辑'}).click();
    assert.equal(await root.getByRole('textbox',{name:'第 1 步内容'}).inputValue(),'未保存的步骤');
    await root.getByRole('button',{name:'返回场景列表'}).click();
    await page.getByRole('button',{name:'放弃更改'}).click();
    await root.getByRole('button',{name:/隔离测试场景/}).waitFor();
    await root.getByRole('button',{name:'返回',exact:true}).click();
    await page.waitForURL(url => legacyMode ? url.pathname==='/' && url.searchParams.get('entry')==='special'
      : url.searchParams.get('__page')==='/' && url.searchParams.get('view')==='functions');
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
    await page.route('**/api/eop-scenarios',async route=>{
      listRequested();
      await pending;
      try { await route.abort(); } catch { /* Navigation can cancel the request first. */ }
    });
    await page.goto(entry('/eop-scenarios',false));
    await requested;
    await page.locator('.eop-page').getByRole('button',{name:'返回',exact:true}).click();
    await page.waitForURL(url=>url.searchParams.get('__page')==='/' && url.searchParams.get('view')==='functions');
    releaseList();
    await page.close();
    console.log('EOP 列表读取期间可返回');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:900}});
    await fixture(page);
    let releaseHistory, historyRequested;
    const pending=new Promise(resolve=>{releaseHistory=resolve;});
    const requested=new Promise(resolve=>{historyRequested=resolve;});
    await page.route('**/api/daily-inspection/history**',async route=>{
      historyRequested();
      await pending;
      try { await route.abort(); } catch { /* Navigation can cancel the request first. */ }
    });
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    await root.getByRole('button',{name:'历史巡检'}).click();
    await requested;
    await root.getByRole('button',{name:'返回',exact:true}).click();
    await root.getByRole('textbox',{name:'电流现场读数'}).waitFor();
    releaseHistory();
    await page.close();
    console.log('历史巡检读取期间可返回');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:900}});
    await fixture(page);
    await page.goto(entry('/daily-inspection',false));
    const history=await historyEdit(page);
    const home=page.getByRole('navigation',{name:'手机导航'}).getByRole('button',{name:'首页'});
    await home.click();
    await discardDialog(page,false);
    assert.equal(await history.getByRole('textbox',{name:/电流.*现场读数/}).inputValue(),'12');
    await home.click();
    await discardDialog(page,true);
    await page.waitForURL(url=>url.searchParams.get('__page')==='/' && url.searchParams.get('view')==='home');
    await page.close();
    console.log('切换手机导航时的历史编辑保护通过');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:900}});
    await page.addInitScript(()=>{
      const original=Storage.prototype.setItem;
      Storage.prototype.setItem=function(key,value) {
        if (key.startsWith('clipflow:daily-inspection:')) throw new Error('storage unavailable');
        return original.call(this,key,value);
      };
    });
    await fixture(page);
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    const reading=root.getByRole('textbox',{name:'电流现场读数'});
    await reading.fill('12');
    assert(await page.evaluate(()=>{
      const event=new Event('beforeunload',{cancelable:true});
      window.dispatchEvent(event);
      return event.defaultPrevented;
    }), 'closing immediately checks whether the new draft can be stored');
    await root.getByText('浏览器暂存不可用，离开前可保存本轮草稿。').waitFor();
    await root.getByRole('button',{name:'返回',exact:true}).click();
    const dialog=page.getByRole('dialog',{name:'本机填写尚未保存'});
    await dialog.getByRole('button',{name:'继续填写'}).click();
    assert.equal(await reading.inputValue(),'12');
    await root.getByRole('button',{name:'返回',exact:true}).click();
    await dialog.getByRole('button',{name:'仍要离开'}).click();
    await page.waitForURL(url=>url.searchParams.get('__page')==='/' && url.searchParams.get('view')==='functions');
    await page.close();
    console.log('本机暂存失败时的离开保护通过');
  }
  {
    const page=await browser.newPage({viewport:{width:1440,height:900}});
    const widget=(id,module,y)=>({id,kind:'module',title:id,module,scope:'current',display:'bar',
      period:'week',metric:'ongoing',presentation:'default',links:[],x:0,y,w:12,h:10});
    await fixture(page,{version:1,widgets:[widget('inspection-widget','daily_inspection',0),widget('eop-widget','eop_scenarios',10)]});
    await page.goto(entry('/',false));
    const inspection=page.locator('.v2-full-module .inspection-page');
    await inspection.getByRole('textbox',{name:'电流现场读数'}).waitFor();
    assert.equal(await inspection.getByRole('button',{name:'返回',exact:true}).count(),0);
    await inspection.getByRole('button',{name:'历史巡检'}).click();
    await inspection.getByRole('button',{name:'返回',exact:true}).click();
    await inspection.getByRole('textbox',{name:'电流现场读数'}).waitFor();
    await page.getByText('eop-widget',{exact:true}).scrollIntoViewIfNeeded();
    const eop=page.locator('.v2-full-module .eop-page');
    await eop.getByRole('button',{name:/隔离测试场景/}).waitFor();
    assert.equal(await eop.getByRole('button',{name:'返回',exact:true}).count(),0);
    await eop.getByRole('button',{name:/隔离测试场景/}).click();
    await eop.getByRole('button',{name:'返回场景列表'}).click();
    await eop.getByRole('button',{name:/隔离测试场景/}).waitFor();
    await page.close();
    console.log('工作台内嵌模块的返回入口通过');
  }
  assert.deepEqual(errors,[]);
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}

