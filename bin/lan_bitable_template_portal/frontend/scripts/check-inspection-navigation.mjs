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
const duty = {scope:'D',date:'2026-11-03',shift:'白'};
const device = {id:'D:1',name:'隔离测试柜',profession:'电气',
  sections:[{location:'一层',form:'巡检',integrity:'完整',missing:'',note:''}],points:[],
  parameters:[{id:'current',name:'电流',reference:10,section:0}]};
const review = {values:{current:'10'},result:'正常',note:'',revision:1};
const archive = {id:'recFixture',scope:'D',date:'2026-11-03',shift:'白班',group:'A班',round:1,
  completed:1793673000000,actor:'测试人员',confirmed:1,devices:1,url:'#',editor:'',modified:null};
const detail = {...archive,version:'v1',draft_key:'fixture-history-user',can_edit:true,devices_data:[{id:'D:1',order:1,name:'隔离测试柜',
  profession:'电气',result:'正常',note:'',rows:[{id:'current',name:'电流',reference:10,value:'10',
    location:'一层',source:'',read_only:false}]}]};
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
async function fixture(page,layout=null,{unreviewed=false,loseFirst=false,conflictOnce=false,historyLoseFirst=false,historyConflictOnce=false,manyDevices=false,manyReadings=false,professionalFixture=false,startEmpty=false}={}) {
  const sourceDevices=professionalFixture ? [device,{...device,id:'D:2',name:'二号配电柜'},
    {...device,id:'D:3',name:'一号空调',profession:'暖通'},{...device,id:'D:4',name:'二号空调',profession:'暖通'}]
    : manyDevices ? [device,{...device,id:'D:2',name:'二号配电柜'}, {...device,id:'D:3',name:'三号配电柜'}] : [device];
  const devices=manyReadings ? sourceDevices.map(item=>({...item,parameters:[...item.parameters,
    {id:'voltage',name:'电压(V)',reference:220,section:0},
    {id:'power',name:'功率(kW)',reference:8.8,section:0},
    {id:'frequency',name:'频率(Hz)',reference:50,section:0}]})) : sourceDevices;
  const state = {duty,completed_rounds:0,round_limit:2,history_scopes:['D'],history:[],active:{
    id:'round-fixture',duty,round:1,pending:false,draft_key:'fixture-user',reviews:unreviewed?{}:{'D:1':review},
    review_version:unreviewed?0:1,batch:{batch:1793671200000,count:devices.length,devices}}};
  if (professionalFixture) Object.assign(state.active,{professions:['电气','暖通'],profession_confirmations:{},
    profession_versions:{'电气':'version-electric','暖通':'version-hvac'},orders:{ALL:devices.map(item=>item.id)}});
  if (startEmpty) state.active=null;
  const reportBodies=[],finishBodies=[],draftBodies=[],historyBodies=[],orderBodies=[],startBodies=[],confirmationBodies=[];
  let currentDetail=structuredClone(detail);
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
    if (url.pathname==='/api/daily-inspection/source') return ok({scope:'D',batch:1793671200000,count:devices.length,devices,
      orders:professionalFixture ? {ALL:['D:1','D:2','D:3','D:4'],电气:['D:2','D:1'],暖通:['D:4','D:3']} : {}});
    if (url.pathname==='/api/daily-inspection/status') return ok(state.active ? {unchanged:true,history_scopes:['D']} : state);
    if (url.pathname==='/api/daily-inspection/start') {
      const body=request.postDataJSON();startBodies.push(body);
      state.active={id:'round-fixture',duty,round:1,pending:false,draft_key:'fixture-user',reviews:{},review_version:0,
        professions:body.professions,profession_confirmations:{},profession_versions:{'电气':'version-electric','暖通':'version-hvac'},
        orders:{ALL:devices.map(item=>item.id)},batch:{batch:1793671200000,count:devices.length,
          devices:devices.filter(item=>body.professions.includes(item.profession))}};
      return ok(state);
    }
    if (url.pathname==='/api/daily-inspection/order') {
      const body=request.postDataJSON();orderBodies.push(body);
      state.active.orders ||= {};
      state.active.orders[body.view]=body.order;
      return ok(state);
    }
    if (url.pathname==='/api/daily-inspection/confirm-profession') {
      const body=request.postDataJSON();confirmationBodies.push(body);
      state.active.profession_confirmations[body.profession]={actor:'隔离测试',confirmed:1793673000000,version:body.profession_version};
      state.active.review_version++;
      if (state.active.professions.every(name=>state.active.profession_confirmations[name])) {
        state.active=null;state.completed_rounds=1;
      }
      return ok(state);
    }
    if (url.pathname==='/api/daily-inspection/history') return ok({history:[archive]});
    if (url.pathname==='/api/daily-inspection/day-sheet') {
      const body=request.postDataJSON();
      reportBodies.push(body);
      const value=body.drafts?.['D:1']?.values?.current ?? '';
      return ok({scope:body.scope,date:body.date,rounds:[{round_id:'round-fixture',shift:'白',round:1,
        status:'进行中',local_draft:!!value,sheets:[{name:'现场核对',rows:[
          ['楼栋','顺序','设备标识','设备','专业','位置','来源','参数','参考值','现场读数','参数状态','核对结论','现场备注','填写人'],
          ['D楼',1,'D:1','隔离测试柜','电气','一层','巡检','电流',10,value,'','','',''],
          ['D楼',2,'D:2','另一设备','电气','一层','巡检','电压',220,null,'','','','']]}]}]});
    }
    if (url.pathname==='/api/daily-inspection/day-export') {
      reportBodies.push(request.postDataJSON());
      return route.fulfill({status:200,contentType:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers:{'Content-Disposition':"attachment; filename*=UTF-8''inspection.xlsx"},body:'isolated fixture'});
    }
    if (url.pathname==='/api/daily-inspection/finish') {
      finishBodies.push(request.postDataJSON());
      state.active=null;
      state.completed_rounds=1;
      return ok(state);
    }
    if (url.pathname==='/api/daily-inspection/history-record') return ok(currentDetail);
    if (url.pathname==='/api/daily-inspection/history-edit') {
      historySaveRequested();
      await historyGate;
      const submitted=request.postDataJSON();
      historyBodies.push(submitted);
      if (historyConflictOnce) {
        historyConflictOnce=false;
        currentDetail.devices_data[0].rows[0].value='99';
        currentDetail.version='v2';
        return route.fulfill({status:409,json:{ok:false,error:'这条巡检已被修改，请重新打开后核对最新数据。'}});
      }
      for (const [id,change] of Object.entries(submitted.changes)) {
        const target=currentDetail.devices_data.find(item=>item.id===id);
        for (const row of target.rows) if (Object.hasOwn(change.values,row.id)) row.value=change.values[row.id];
        target.result=change.result;target.note=change.note;
      }
      currentDetail.version='v2';
      if (historyLoseFirst) {historyLoseFirst=false;return route.abort('failed');}
      return ok(currentDetail);
    }
    if (url.pathname==='/api/daily-inspection/draft') {
      draftRequested();
      await gate;
      const submitted=request.postDataJSON();
      draftBodies.push(submitted);
      if (conflictOnce) {
        conflictOnce=false;
        state.active.reviews['D:1']={...review,values:{current:'99'},revision:2};
        state.active.review_version++;
        return route.fulfill({status:409,json:{ok:false,error:'设备已被其他页面更新，请刷新并核对。'}});
      }
      for (const [id,value] of Object.entries(submitted.reviews)) {
        const previous=state.active.reviews[id] || {values:{},result:'',note:'',revision:0};
        const same=JSON.stringify([value.values,value.result,value.note])===JSON.stringify([previous.values,previous.result,previous.note]);
        if (value.revision!==previous.revision && !same)
          return route.fulfill({status:409,json:{ok:false,error:'设备已被其他页面更新，请刷新并核对。'}});
        if (!same) state.active.reviews[id]={...value,revision:previous.revision+1};
      }
      state.active.review_version++;
      if (loseFirst) {loseFirst=false;return route.abort('failed');}
      return ok(state);
    }
    if (url.pathname==='/api/eop-scenarios') return ok({items:[{id:scenario.id,fields:scenario.fields}]});
    if (url.pathname==='/api/eop-scenarios/recEop') return ok(scenario);
    return ok({items:[],revision:0,value:null});
  });
  const forceExternalArchive=()=>{
    state.history=[{id:'round-fixture',duty,round:1,actor:'其他人员',completed:1793673000000,url:'#',draft_key:'fixture-user'}];
    state.active=null;state.completed_rounds=1;
  };
  return {waiting,releaseDraft,historyWaiting,releaseHistorySave,reportBodies,finishBodies,draftBodies,historyBodies,
    orderBodies,startBodies,confirmationBodies,forceExternalArchive};
}
async function historyEdit(page,value='12') {
  await page.getByRole('button',{name:'历史巡检'}).click();
  const history=page.locator('.inspection-history-page');
  await history.getByRole('button',{name:/D楼.*白班/}).click();
  await history.getByRole('button',{name:'修改记录'}).click();
  await history.getByRole('textbox',{name:/电流.*巡检数据/}).fill(value);
  return history;
}
async function discardDialog(page,accept) {
  const dialog=page.getByRole('dialog',{name:'有未保存的历史巡检修改'});
  await dialog.waitFor();
  await dialog.getByRole('button',{name:accept?'放弃本机修改':'继续编辑'}).click();
}

try {
  for (const legacyMode of eopOnly ? [] : process.env.INSPECTION_ONLY_LEGACY ? [true] : [false,true]) {
    for (const width of [402,440,1440]) {
      const page=await browser.newPage({viewport:{width,height:900}});
      if (process.env.INSPECTION_ONLY_LEGACY) page.setDefaultTimeout(6000);
      const mock=await fixture(page);
      await page.goto(entry('/daily-inspection',legacyMode));
      const root=page.locator('.inspection-page');
      const reading=root.getByRole('textbox',{name:'电流巡检数据'});
      await reading.fill('11');
      await mock.waiting;
      assert.equal(await reading.isEnabled(),true,'自动保存期间仍可继续填写');
      await reading.fill('12');
      mock.releaseDraft();
      await root.getByText('已自动保存').waitFor();
      assert.equal(mock.draftBodies.length,2);
      assert.equal(mock.draftBodies[1].reviews['D:1'].values.current,'12');

      const history=await historyEdit(page);
      await root.getByRole('button',{name:'返回',exact:true}).click();
      await discardDialog(page,false);
      assert.equal(await history.getByRole('textbox',{name:/电流.*巡检数据/}).inputValue(),'12');
      await mock.historyWaiting;
      await root.getByRole('button',{name:'返回本班'}).click();
      await history.getByRole('alert').getByText('当前操作正在处理，请稍后再离开。').waitFor();
      mock.releaseHistorySave();
      await history.getByText('已自动保存').waitFor();
      await root.getByRole('button',{name:'返回本班'}).click();
      assert.equal(await reading.inputValue(),'12');

      await historyEdit(page,'13');
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
  if (!eopOnly) {
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
    await root.getByRole('textbox',{name:'电流巡检数据'}).waitFor();
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
    assert.equal(await history.getByRole('textbox',{name:/电流.*巡检数据/}).inputValue(),'12');
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
    const reading=root.getByRole('textbox',{name:'电流巡检数据'});
    await reading.fill('12');
    assert(await page.evaluate(()=>{
      const event=new Event('beforeunload',{cancelable:true});
      window.dispatchEvent(event);
      return event.defaultPrevented;
    }), 'closing immediately checks whether the new draft can be stored');
    await root.getByText('浏览器暂存不可用，请保持页面打开，等待自动保存完成。').waitFor();
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
    await inspection.getByRole('textbox',{name:'电流巡检数据'}).waitFor();
    assert.equal(await inspection.getByRole('button',{name:'返回',exact:true}).count(),0);
    await inspection.getByRole('button',{name:'历史巡检'}).click();
    await inspection.getByRole('button',{name:'返回',exact:true}).click();
    await inspection.getByRole('textbox',{name:'电流巡检数据'}).waitFor();
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
  for (const legacyMode of [false,true]) {
    for (const width of [402,440,1440]) {
      const page=await browser.newPage({viewport:{width,height:850},acceptDownloads:true});
      const mock=await fixture(page);
      await page.goto(entry('/daily-inspection',legacyMode));
      const root=page.locator('.inspection-page');
      await root.getByRole('textbox',{name:'电流巡检数据'}).fill('12');
      await root.getByText('原值 10').waitFor();
      await root.getByRole('button',{name:'巡检表与导出'}).click();
      const sheet=root.locator('.day-sheet');
      await sheet.getByText('尚未共享的填写',{exact:false}).waitFor();
      await sheet.getByRole('cell',{name:'12',exact:true}).waitFor();
      await sheet.getByRole('cell',{name:'数据缺失'}).waitFor();
      assert.deepEqual((await sheet.locator('thead th').allTextContents()).slice(0,4),['设备','参数','参考值','巡检数据']);
      await sheet.getByLabel('文字大小').selectOption('18');
      assert.equal(await sheet.locator('table').evaluate(el=>getComputedStyle(el).fontSize),'18px');
      const download=page.waitForEvent('download');
      await sheet.getByRole('button',{name:'导出白夜班表格'}).click();
      assert.equal((await download).suggestedFilename(),'inspection.xlsx');
      assert.equal(mock.reportBodies.at(-1).drafts['D:1'].values.current,'12');
      if (process.env.INSPECTION_SCREENSHOT_DIR) await page.screenshot({path:path.join(process.env.INSPECTION_SCREENSHOT_DIR,`${legacyMode?'legacy':'new'}-${width}.png`)});
      await page.close();
    }
  }
  console.log('新旧版巡检表查看、放大和导出通过');
  for (const legacyMode of [false,true]) {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{unreviewed:true,manyDevices:true});
    await page.goto(entry('/daily-inspection',legacyMode));
    const root=page.locator('.inspection-page');
    assert.equal(await root.getByRole('textbox',{name:'电流巡检数据'}).first().inputValue(),'10');
    assert.equal(await root.locator('.inspection-device').count(),3);
    if (process.env.INSPECTION_SCREENSHOT_DIR) await page.screenshot({path:path.join(process.env.INSPECTION_SCREENSHOT_DIR,`${legacyMode?'legacy':'new'}-device-402.png`)});
    await root.getByRole('button',{name:'确认本轮并归档'}).click();
    await page.getByRole('dialog',{name:'确认整轮巡检？'}).getByRole('button',{name:'确认并归档'}).click();
    await root.getByText(/已完成 1/).waitFor();
    assert.deepEqual(mock.finishBodies[0].reviews,{});
    await page.close();
  }
  console.log('无逐项填写时统一确认通过');
  for (const legacyMode of [false,true]) for (const width of [360,375,402,440,1440]) {
    const page=await browser.newPage({viewport:{width,height:850}});
    await fixture(page,null,{unreviewed:true,manyDevices:true,manyReadings:true});
    await page.goto(entry('/daily-inspection',legacyMode));
    const root=page.locator('.inspection-page');
    const first=root.locator('.inspection-device').first();
    await first.getByRole('textbox',{name:'电流巡检数据'}).waitFor();
    const readings=first.locator('.inspection-reading');
    assert.equal(await readings.count(),4);
    const a=await readings.nth(0).boundingBox(), b=await readings.nth(1).boundingBox();
    assert(a && b && Math.abs(a.y-b.y)<2 && b.x>=a.x+a.width-1,'同一设备的多个参数应同行显示且互不遮挡');
    assert.equal(await first.getByRole('textbox',{name:'电压(V)巡检数据'}).inputValue(),'220');
    assert.equal(await root.getByText('来源明细').count(),0);
    assert.equal(await root.getByText('异常与备注').count(),0);
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
    if (width===402 && process.env.INSPECTION_SCREENSHOT_DIR) await page.screenshot({path:path.join(process.env.INSPECTION_SCREENSHOT_DIR,`${legacyMode?'legacy':'new'}-dense-402.png`)});
    await page.close();
  }
  console.log('新旧版多参数同排展示和小屏无横向溢出通过');
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{professionalFixture:true,startEmpty:true});
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    await root.getByText('暖通 · 2 台').waitFor();
    assert.equal(await root.getByText('一号空调').count(),1,'未开始巡检时也应显示所选专业的设备数据');
    assert.equal(await root.getByText('最新参考数据',{exact:false}).count(),1);
    if (process.env.INSPECTION_SCREENSHOT_DIR) await page.screenshot({path:path.join(process.env.INSPECTION_SCREENSHOT_DIR,'new-preview-402.png')});
    await root.getByLabel('暖通 · 2 台').uncheck();
    assert.equal(await root.getByText('一号空调').count(),0,'专业选择应同步筛选参考数据');
    assert.deepEqual((await root.locator('.inspection-device h2').allTextContents()).map(value=>value.trim()),
      ['二号配电柜','隔离测试柜'],'专业预览应使用该专业的个人顺序');
    await root.getByRole('button',{name:'开始第 1 次巡检'}).click();
    await root.getByRole('button',{name:'电气确认'}).waitFor();
    assert.deepEqual(mock.startBodies[0].professions,['电气']);
    assert.equal(await root.getByText('一号空调').count(),0);
    await page.close();
    console.log('开始巡检可选择单个专业');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{professionalFixture:true,unreviewed:true});
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    await root.getByRole('button',{name:'电气确认'}).click();
    await page.getByRole('dialog',{name:'确认电气专业？'}).getByRole('button',{name:'确认该专业'}).click();
    assert.equal(await root.getByRole('button',{name:'电气已确认'}).isDisabled(),true);
    await root.getByRole('button',{name:'暖通确认'}).click();
    await page.getByRole('dialog',{name:'确认暖通专业？'}).getByRole('button',{name:'确认该专业'}).click();
    await root.getByText(/已完成 1/).waitFor();
    assert.deepEqual(mock.confirmationBodies.map(item=>item.profession),['电气','暖通']);
    await page.close();
    console.log('多个专业分别确认后归档');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850},acceptDownloads:true});
    const mock=await fixture(page,null,{professionalFixture:true,unreviewed:true});
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    await root.getByRole('button',{name:'排序',exact:true}).click();
    let modal=page.locator('.inspection-order');
    await modal.locator('.order-list label').last().locator('input').check();
    await modal.getByRole('button',{name:'移动',exact:true}).click();
    await modal.getByRole('button',{name:'保存顺序'}).click();
    assert.equal(mock.orderBodies[0].view,'ALL');
    await root.getByLabel('专业筛选').selectOption('电气');
    await root.getByRole('button',{name:'排序',exact:true}).click();
    modal=page.locator('.inspection-order');
    assert.equal(await modal.locator('.order-list label').count(),2);
    const exported=page.waitForEvent('download');
    await modal.getByRole('button',{name:'导出顺序'}).click();
    const json=JSON.parse(await readFile(await (await exported).path(),'utf8'));
    assert.equal(json.view,'电气');
    await modal.locator('.order-list label').last().locator('input').check();
    await modal.getByRole('button',{name:'移动',exact:true}).click();
    await modal.getByRole('button',{name:'保存顺序'}).click();
    assert.equal(mock.orderBodies[1].view,'电气');
    await root.getByRole('button',{name:'排序',exact:true}).click();
    modal=page.locator('.inspection-order');
    await modal.locator('input[type=file]').setInputFiles({name:'shared.json',mimeType:'application/json',buffer:Buffer.from(JSON.stringify(json))});
    await modal.getByText(/已预览 2 台/).waitFor();
    await modal.getByRole('button',{name:'保存顺序'}).click();
    assert.deepEqual(mock.orderBodies[2].order,json.order);
    await root.getByLabel('专业筛选').selectOption('');
    await root.getByRole('button',{name:'排序',exact:true}).click();
    assert.deepEqual((await page.locator('.inspection-order .order-list label').allTextContents()).map(value=>value.trim()),
      ['1. 二号空调 暖通','2. 隔离测试柜 电气','3. 二号配电柜 电气','4. 一号空调 暖通']);
    await page.close();
    console.log('全部与专业排序独立保存、导出和导入通过');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page);
    mock.releaseHistorySave();
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    let history=await historyEdit(page);
    await root.getByRole('button',{name:'返回',exact:true}).click();
    await page.getByRole('dialog',{name:'有未保存的历史巡检修改'}).getByRole('button',{name:'保留并离开'}).click();
    await history.getByRole('button',{name:/D楼.*白班/}).click();
    history=page.locator('.inspection-history-page');
    assert.equal(await history.getByRole('textbox',{name:/电流.*巡检数据/}).inputValue(),'12');
    await history.getByText('已自动保存').waitFor();
    await page.close();
    console.log('历史修改可保留离开并在返回后自动保存');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{unreviewed:true});
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page');
    await root.getByRole('textbox',{name:'电流巡检数据'}).fill('13');
    mock.forceExternalArchive();
    await page.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));
    await root.getByText('有 1 轮本机修改需要核对').waitFor();
    await root.getByText(/电流：13/).waitFor();
    await page.reload();
    await root.getByText('有 1 轮本机修改需要核对').waitFor();
    await root.getByText(/电流：13/).waitFor();
    await page.close();
    console.log('他人先归档时本机修改保留并可在刷新后核对');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{loseFirst:true});
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page'), reading=root.getByRole('textbox',{name:'电流巡检数据'});
    await reading.fill('11');
    await mock.waiting;
    await reading.fill('12');
    await page.waitForTimeout(350);
    mock.releaseDraft();
    await root.getByRole('button',{name:'重试保存'}).waitFor();
    await page.reload();
    await root.getByText('已自动保存').waitFor();
    assert.equal(await reading.inputValue(),'12');
    assert.equal(mock.draftBodies.length,2);
    assert.equal(mock.draftBodies[1].reviews['D:1'].revision,2);
    await page.close();
    console.log('响应丢失和刷新后的本机修改自动恢复通过');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{conflictOnce:true});
    mock.releaseDraft();
    await page.goto(entry('/daily-inspection',false));
    const root=page.locator('.inspection-page'), reading=root.getByRole('textbox',{name:'电流巡检数据'});
    await reading.fill('12');
    await root.getByText('待核对共享修改').waitFor();
    assert.equal(mock.draftBodies.length,1,'冲突时不能自动覆盖他人数据');
    assert.equal(await reading.inputValue(),'12');
    await root.getByRole('button',{name:'合并我的改动'}).click();
    await root.getByText('已自动保存').waitFor();
    assert.equal(mock.draftBodies[1].reviews['D:1'].revision,2);
    await page.close();
    console.log('多人修改冲突提示与确认后恢复自动保存通过');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{historyLoseFirst:true});
    mock.releaseHistorySave();
    await page.goto(entry('/daily-inspection',false));
    let history=await historyEdit(page);
    await history.getByRole('button',{name:'重试保存'}).waitFor();
    await page.reload();
    await page.locator('.inspection-page').getByRole('button',{name:'历史巡检'}).click();
    history=page.locator('.inspection-history-page');
    await history.getByRole('button',{name:/D楼.*白班/}).click();
    await history.getByText('已自动保存').waitFor();
    assert.equal(await history.getByRole('textbox',{name:/电流.*巡检数据/}).inputValue(),'12');
    assert.equal(mock.historyBodies.length,2);
    assert.deepEqual(mock.historyBodies[0],mock.historyBodies[1],'响应丢失时复用同一份修改');
    await page.close();
    console.log('历史巡检保存响应丢失后的恢复通过');
  }
  {
    const page=await browser.newPage({viewport:{width:402,height:850}});
    const mock=await fixture(page,null,{historyConflictOnce:true});
    mock.releaseHistorySave();
    await page.goto(entry('/daily-inspection',false));
    const history=await historyEdit(page);
    await history.getByText('原记录已变化，本机修改待核对').waitFor();
    assert.equal(await history.getByRole('textbox',{name:/电流.*巡检数据/}).inputValue(),'12');
    assert.equal(mock.historyBodies.length,1,'历史版本冲突时不能自动覆盖');
    await page.close();
    console.log('历史巡检多人修改冲突保护通过');
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

