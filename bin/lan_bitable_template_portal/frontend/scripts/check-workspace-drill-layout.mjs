import assert from 'node:assert/strict';
import {mkdir,readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {parse,compileScript} from '@vue/compiler-sfc';
import {adaptDrillPresentation} from './workspace-drill-adapter.mjs';
import {adaptPageLayouts} from './workspace-page-layout-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const baseline=process.argv.includes('--baseline');
for(const file of ['DrillManagementPage.vue','LifeGuidePage.vue']){
  const source=await readFile(path.join(root,'src/components',file),'utf8');
  const adapted=adaptDrillPresentation(source,file),original=parse(source).descriptor;
  const {descriptor,errors}=parse(adapted);assert.deepEqual(errors,[]);
  compileScript(descriptor,{id:file,inlineTemplate:true});
  const stableScript=text=>text.replace('\nimport { NSelect as V2DrillSelect } from "naive-ui";','').replace(/const stepSignerOptions[\s\S]*?(?=function stepSignerIds)/,'').replace(/function updateStepSigner[\s\S]*?(?=function isEccStep)/,'');
  assert.equal(stableScript(descriptor.scriptSetup.content),stableScript(original.scriptSetup.content),'only step candidate selection changes; save controllers stay unchanged');
  assert.equal(adaptPageLayouts(adapted,file),adapted,'no PageCanvas wrapping');
  if(file==='DrillManagementPage.vue')assert.equal(descriptor.template.ast.children[0].loc.source,original.template.ast.children[0].loc.source,'print view unchanged');
}
const output=path.resolve(root,'../../../output/playwright/drill-layout');
await mkdir(output,{recursive:true});
const server=await preview({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const browser=await chromium.launch({headless:true});
const scopes=['A','B','C','D','E'].map(value=>({value,label:value+'楼'}));
const people=Array.from({length:7},(_,i)=>({record_id:`p${i}`,name:`演练人员${'甲乙丙丁戊己庚'[i]}`,employee_no:`1000${i}`,building:'A',has_signature:i!==6}));
people.push({record_id:'external:cross-building',name:'外部跨楼签名人员',source:'external',employee_no:'20001',building:'B',has_signature:true});
const steps=[['设备间','确认故障设备位置、现场环境及安全条件。'],['值班室','汇报现场情况，核对人员到位情况并记录处理过程。'],['设备间','恢复设备，确认运行参数正常并完成记录。']].map(([location,content],i)=>({row:10+i,location,content,duration_minutes:5,signature_slots:2}));
const definitions=['冷却水系统故障应急演练','高压配电系统停电应急演练','制冷单元故障处置演练','消防系统联动应急演练'].map((name,i)=>({
  drill_id:`fixture-${i}`,name,year:2026,month:10,status:'published',assigned_scopes:['A','B'],version:1,
  source:{name:name+'.xlsx',size:25600},sheets:['演练记录表','演练评估表'],
  configuration:{record_sheet:'演练记录表',assessment_sheet:'演练评估表',steps,mapping:{scenario:'A4',reviewer_signature:'A20',review_time:'B20',assessment:{evaluator_signature:'A10',evaluation_time:'B10'}}},
}));
let role='admin',failSave=true,saved,generated=0;
const executions=new Map(definitions.map(item=>[item.drill_id,{drill_id:item.drill_id,scope:'A',version:1,status:'draft',drill_date:'2026-10-15',first_start_time:'09:00',simulation_scenario:'模拟冷却水供水异常，组织人员进行排查和恢复。',commander:people[0],participants:people.slice(0,3),step_signers:{10:['p0','p1'],11:['p1','p2'],12:['p0','p2']},steps}]));
try{
  const context=await browser.newContext({viewport:{width:1440,height:960},reducedMotion:'reduce'});
  const errors=[];
  await context.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url()),p=url.pathname;
    if(url.origin!==origin)return route.abort();
    if(p==='/assets/vnet-logo.png')return route.fulfill({contentType:'image/png',body:await readFile(path.join(root,'public/assets/vnet-logo.png'))});
    if(!p.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'drill-layout-fixture'}});
    if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'drill-layout-'+role,name:'隔离验收人员',role,level:role==='admin'?'L0':'L4',modules:['drills','life_guide']},scope_options:scopes});
    if(p==='/api/drills/bootstrap')return ok({drills:definitions,people,scopes});
    if(p==='/api/drills')return ok({items:definitions});
    if(p==='/api/drills/people')return ok({people});
    const id=p.split('/')[3],execution=executions.get(id);
    if(p.endsWith('/execution')){
      if(request.method()==='PUT'){
        if(failSave){failSave=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试：保存失败'}});}
        saved=request.postDataJSON();assert.equal(saved.expected_version,execution.version);
        Object.assign(execution,saved,{version:execution.version+1});
      }
      return ok({drill:definitions.find(item=>item.drill_id===id),execution});
    }
    if(p.endsWith('/preview'))return ok({model:{column_count:3,rows:[[{text:'演练表格预览'},{text:'现场确认'},{text:'操作记录'}]],sheet_width_px:720}});
    if(p.endsWith('/generate')){generated++;execution.status='completed';definitions.find(item=>item.drill_id===id).execution_status='completed';return ok({execution});}
    assert.equal(request.method(),'GET','Unexpected write: '+p);
    return ok({items:[],records:[],settings:{},summary:{}});
  });
  const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
  const go=route=>page.goto(origin+'/assets/workspace-v2/index.html?__page='+route);
  const shot=name=>page.screenshot({path:path.join(output,(baseline?'before-':'after-')+name+'.png'),animations:'disabled'});
  await go('/drill-management&scope=A');await page.locator('#drill-commander').waitFor();
  await shot('desktop');
  if(!baseline){
    assert.equal(await page.locator('.drill-page .v2-page-canvas').count(),0);
    assert.equal(await page.locator('.drill-page').getByText('自定义布局',{exact:true}).count(),0);
    assert.equal(await page.locator('.drill-list__item').count(),4);
    assert.equal(await page.locator('.v2-drill-preview').getAttribute('open'),null);
    const scenario=page.locator('.scenario-field textarea');await scenario.fill('修改后的模拟场景');
    await page.locator('.people-options').getByText('外部跨楼签名人员',{exact:true}).waitFor();
    await page.locator('.v2-single-select:has(#drill-step-10-2) .n-select').click();assert.equal(await page.locator('.n-base-select-option').filter({hasText:'外部跨楼签名人员'}).count(),0,'unselected people are not step candidates');await page.keyboard.press('Escape');
    await page.locator('.people-options label').filter({hasText:'外部跨楼签名人员'}).locator('input').check();
    await page.locator('.v2-single-select:has(#drill-step-10-2) .n-select').click();
    await shot('step-picker');
    await page.locator('.n-base-select-option').filter({hasText:'外部跨楼签名人员'}).click();
    await page.getByRole('button',{name:'演练评估表',exact:true}).click();await page.locator('#drill-evaluator').waitFor();
    assert.deepEqual(await page.locator('.assessment-fields input[type=datetime-local]').evaluateAll(items=>items.map(item=>item.value)),['','']);
    await page.getByRole('button',{name:'演练记录表',exact:true}).click();assert.equal(await scenario.inputValue(),'修改后的模拟场景');
    await page.locator('.drill-list__item').nth(1).click();
    await page.getByRole('button',{name:'继续编辑',exact:true}).click();assert.equal(await scenario.inputValue(),'修改后的模拟场景');
    await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('隔离测试：保存失败',{exact:true}).waitFor();
    assert.equal(await scenario.inputValue(),'修改后的模拟场景');
    await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('演练草稿已保存',{exact:true}).waitFor();
    assert.equal(saved.simulation_scenario,'修改后的模拟场景');assert.deepEqual(saved.evaluator,{});
    assert.equal(saved.step_signers['10'][1],'external:cross-building');
    assert(saved.participants.some(person=>person.record_id==='external:cross-building'),'only selected participants can be assigned steps');
    await page.locator('.v2-drill-preview summary').click();await page.locator('.v2-drill-preview .sheet-table-wrap').waitFor();
    await page.getByRole('button',{name:'生成并归档',exact:true}).click();await page.getByText('已提交生成任务，完成后会自动同步飞书。',{exact:true}).waitFor();assert.equal(generated,1);
  }
  for(const width of baseline?[390]:[1366,1920,768,390,360]){
    await page.setViewportSize({width,height:width<761?844:960});
    await go('/drill-management&scope=A');await page.locator('#drill-commander').waitFor();await shot('record-'+width);
    if(!baseline){
      assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),'viewport overflow '+width);
      assert(!await page.locator('.v2-main').evaluate(el=>el.scrollWidth>el.clientWidth+1),'workspace overflow '+width);
      if(width<961){
        await page.locator('.v2-drill-mobile-picker .n-select').click();
        await page.locator('.n-base-select-option').filter({hasText:definitions[1].name}).click();
        await page.locator('.execution-header h2').filter({hasText:definitions[1].name}).waitFor();
      }
      await page.getByRole('button',{name:'演练评估表',exact:true}).click();await page.locator('#drill-evaluator').waitFor();await shot('assessment-'+width);
      await page.getByRole('button',{name:'管理员入口',exact:true}).click();await page.locator('.configuration-panel').waitFor();await shot('admin-'+width);
      assert(!await page.locator('.v2-main').evaluate(el=>el.scrollWidth>el.clientWidth+1),'admin overflow '+width);
      await page.getByRole('button',{name:'上传演练文件',exact:true}).click();await page.locator('.upload-panel').waitFor();await shot('upload-'+width);
    }
  }
  await page.setViewportSize({width:1440,height:960});await go('/life-guide');await page.locator('.life-guide-page iframe').waitFor();await shot('guide');
  if(!baseline){
    assert.equal(await page.getByText('指南阅读',{exact:true}).count(),0);
    assert.equal(await page.getByText('自定义布局',{exact:true}).count(),0);
    const frame=await page.locator('.life-guide-page iframe').boundingBox(),main=await page.locator('.v2-main').boundingBox();
    assert(frame.width>main.width-2&&frame.height>main.height-2,'guide fills workspace: '+JSON.stringify({frame,main}));
    assert.equal(await page.locator('.life-guide-page iframe').getAttribute('sandbox'),'allow-scripts allow-popups allow-downloads');
    role='member';await go('/drill-management&scope=A&mode=admin');await page.locator('#drill-commander').waitFor();assert.equal(await page.getByRole('button',{name:'管理员入口',exact:true}).count(),0);
    assert.deepEqual(errors,[]);
  }
  console.log(baseline?'Captured existing drill and guide layouts.':'PASS: drill layouts, responsive execution/admin, unsaved guard, optional review, save failure/retry, generation and full-area guide. All APIs mocked.');
}finally{await browser.close();await new Promise(resolve=>server.httpServer.close(resolve));}
