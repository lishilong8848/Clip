import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url)),project=path.resolve(root,'../../..');
const python=path.join(project,'bin/.venv',process.platform==='win32'?'Scripts/python.exe':'bin/python');
const fixture=JSON.parse(execFileSync(python,['-c',`import sys,json,tempfile
from pathlib import Path
sys.path.insert(0,'bin')
from test_drill_management import _fixture_xlsx
from lan_bitable_template_portal.drill_management import DrillManagementService
from lan_bitable_template_portal.state_store import LanPortalStateStore
with tempfile.TemporaryDirectory() as tmp:
 service=DrillManagementService(LanPortalStateStore(Path(tmp)/'state.sqlite3'),data_root=Path(tmp)/'drills')
 definition=service.create_definition(name='可选评估验收',year=2026,month=10,file_name='fixture.xlsx',source=_fixture_xlsx(with_evaluator=True))
 definition=service.publish(definition['drill_id'],expected_version=definition['version'])
 people=[{'record_id':'p1','name':'指挥人','has_signature':True,'building':'A'},{'record_id':'p2','name':'评估人','has_signature':True,'building':'H'},{'record_id':'external:p3','name':'跨楼签名人','source':'external','has_signature':True,'building':'B'},{'record_id':'p4','name':'未签名人员','has_signature':False,'building':'C'}]
 execution=service.save_execution(definition['drill_id'],'A',{'drill_date':'2026-10-13','first_start_time':'09:00','commander':people[0],'participants':[people[0]],'step_signers':{str(s['row']):['p1'] for s in definition['configuration']['steps']}},expected_version=0)
 print(json.dumps({'definition':definition,'execution':execution,'people':people,'preview':service.preview_model(definition['drill_id'],'A','assessment')},ensure_ascii=False))
`],{cwd:project,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8'}}));
const server=await createServer({root,configFile:false,plugins:[vue()],logLevel:'error',server:{host:'127.0.0.1',port:0}});
await server.listen();const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
const browser=await chromium.launch({headless:true});
try{
  for(const width of [1440,390]){
    const context=await browser.newContext({viewport:{width,height:950}}),page=await context.newPage(),errors=[];
    let execution=structuredClone(fixture.execution),generated=0;
    page.on('pageerror',error=>errors.push(error.message));
    await context.route('**/drill-fixture?**',route=>route.fulfill({contentType:'text/html',body:'<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><div id="app"></div><script type="module" src="/scripts/fixtures/drill-optional.ts"></script>'}));
    await context.route(origin+'/api/**',route=>{
      const url=new URL(route.request().url()),ok=data=>route.fulfill({json:{ok:true,data}});
      if(url.pathname==='/api/drills/bootstrap')return ok({drills:[fixture.definition],people:fixture.people,scopes:[{value:'A',label:'A楼'}],is_admin:false});
      if(url.pathname==='/api/drills')return ok({items:[fixture.definition]});
      if(url.pathname.endsWith('/execution')){
        if(route.request().method()!=='GET')execution={...execution,...route.request().postDataJSON(),version:execution.version+1};
        return ok({drill:fixture.definition,execution});
      }
      if(url.pathname.endsWith('/preview'))return ok({model:fixture.preview});
      if(url.pathname.endsWith('/generate')){generated++;return ok({execution});}
      return ok({});
    });
    await page.goto(origin+'/drill-fixture?scope=A&sheet=assessment&drill_id='+fixture.definition.drill_id);
    try{await page.locator('#drill-evaluator').waitFor();}catch(error){console.log(errors,await page.locator('body').innerText());throw error;}
    const fields=page.locator('.assessment-fields'),times=fields.locator('input[type=datetime-local]');
    assert.equal(await fields.locator('b').count(),0);assert.equal(await page.locator('#drill-evaluator').getAttribute('aria-required'),'false');
    assert.deepEqual(await times.evaluateAll(inputs=>inputs.map(el=>el.value)),['','']);
    await page.getByRole('button',{name:'生成并归档',exact:true}).click();await page.waitForFunction(()=>document.body.textContent.includes('已提交生成任务'));assert.equal(generated,1);
    await page.locator('#drill-evaluator').click();await page.getByRole('option').filter({hasText:'评估人'}).click();
    assert.deepEqual(await times.evaluateAll(inputs=>inputs.map(el=>el.value)),['','']);
    await times.first().fill('2026-10-13T10:15');
    await page.locator('#drill-evaluator').click();await page.getByRole('option',{name:'不填写',exact:true}).click();
    await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('演练草稿已保存',{exact:true}).waitFor();
    assert.deepEqual(execution.evaluator,{});assert.equal(execution.signature_time,'2026-10-13T10:15');assert.equal(execution.evaluation_time,'');
    await page.reload();await page.locator('#drill-evaluator').waitFor();assert.equal(await times.first().inputValue(),'2026-10-13T10:15');assert.equal(await times.nth(1).inputValue(),'');
    await times.first().fill('');await page.getByRole('button',{name:'生成并归档',exact:true}).click();
    await page.getByText('已提交生成任务，完成后会自动同步飞书。',{exact:true}).waitFor();assert.equal(generated,2);assert.equal(execution.signature_time,'');
    await page.getByRole('button',{name:'演练记录表',exact:true}).click();
    await page.locator('.people-options').getByText('跨楼签名人',{exact:true}).waitFor();
    const row=fixture.definition.configuration.steps[1].row;
    await page.locator(`#drill-step-${row}-1`).click();
    await page.getByRole('option').filter({hasText:'跨楼签名人'}).click();
    await page.locator('.selected-people').getByText('跨楼签名人',{exact:false}).first().waitFor();
    await page.getByRole('button',{name:'保存草稿',exact:true}).click();await page.getByText('演练草稿已保存',{exact:true}).waitFor();
    assert.equal(execution.step_signers[String(row)][0],'external:p3');
    assert(execution.participants.some(person=>person.record_id==='external:p3'));
    await page.reload();await page.getByRole('button',{name:'演练记录表',exact:true}).click();await page.locator(`#drill-step-${row}-1`).waitFor();
    assert.match(await page.locator(`#drill-step-${row}-1`).innerText(),/跨楼签名人/,'跨楼签名保存后刷新仍保留');
    await page.locator(`#drill-step-${row}-1`).click();await page.getByRole('option').filter({hasText:'未签名人员'}).click();
    assert(await page.getByRole('button',{name:'生成并归档',exact:true}).isDisabled(),'缺签名仍禁止生成');
    assert.deepEqual(errors,[]);
    await context.close();
  }
  console.log('PASS: desktop/mobile optional review fields; all-building and external step signatures auto-enroll, persist and reload; missing signature still blocks generation. Isolated APIs only.');
}finally{await browser.close();await server.close();}
