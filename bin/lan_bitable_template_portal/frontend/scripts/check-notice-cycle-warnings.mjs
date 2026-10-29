import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFile,mkdtemp,mkdir,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { parse,compileScript } from '@vue/compiler-sfc';
import { createServer } from 'vite';
import { listenTestServer } from './test-server.mjs';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import ts from 'typescript';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(root,'../../..');
const temporary=await mkdtemp(path.join(tmpdir(),'notice-cycle-check-'));
const generated=spawnSync(path.join(repo,'bin/.venv/Scripts/python.exe'),['-c',`
import copy,json,sys
from unittest.mock import patch
sys.path.insert(0,'bin')
with patch('socket.socket.connect', side_effect=AssertionError('No network in descriptor checks')):
 from upload_event_module.services.notice_cycle_warnings import maintenance_cycle_rules
 from openclaw_service.assistant.lighthouse_api import _notice_frontend_fields
 from lan_bitable_template_portal.workbench_lite import MAINTENANCE_CYCLE_OPTIONS
 from lan_bitable_template_portal.notice_panel_data import fields_for
 from lan_bitable_template_portal.notice_panel import NoticePanel
 kinds=['maintenance','change','repair','polling','adjust','power']
 manager=object.__new__(NoticePanel)
 manager._owner_modules=lambda doc:set(kinds)
 panel={}
 for kind in kinds:
  draft={'work_type':kind,'notice_type':'维保通告' if kind=='maintenance' else '变更通告','maintenance_cycle':'每月'}
  fields=fields_for(kind,draft,'update')
  item={'key':kind+':fixture','action':'update','draft':draft,'fields':fields}
  public=manager.public_items({'items':[copy.deepcopy(item)]})[0]
  legacy=copy.deepcopy(item)
  for field in legacy['fields']:field.pop('cycle_rules',None)
  restored=manager.public_items({'items':[legacy]})[0]
  panel[kind]={'new':public,'restored':restored}
 print(json.dumps({'rules':maintenance_cycle_rules(),'descriptors':{k:_notice_frontend_fields(k,['A']) for k in kinds},'options':list(MAINTENANCE_CYCLE_OPTIONS),'panel':panel},ensure_ascii=False))
`],{cwd:repo,encoding:'utf8',env:{...process.env,PYTHONIOENCODING:'utf-8',CLIPFLOW_DATA_DIR:temporary},maxBuffer:5*1024*1024});
assert.equal(path.dirname(temporary),path.resolve(tmpdir()));assert(path.basename(temporary).startsWith('notice-cycle-check-'));
await rm(temporary,{recursive:true,force:true});
assert.equal(generated.status,0,generated.stderr);
const fixture=JSON.parse(generated.stdout),rules=fixture.rules;
const compiled=await build({entryPoints:[path.join(root,'src/noticeCycleWarnings.ts')],bundle:true,write:false,format:'esm'});
const {noticeCycleWarnings,maintenanceCycleRules}=await import('data:text/javascript;base64,'+Buffer.from(compiled.outputFiles[0].text).toString('base64'));
const values={work_type:'maintenance',notice_type:'维保通告',maintenance_cycle:'每月',content:'季度检验与半年度保养',reason:'按季度计划维保',title:'隔离周期核对',building_codes:['A'],specialty:'电气',execution_party:'自维',location:'A楼',impact:'无影响',progress:'已准备',start_time:'2026-10-22T09:00',end_time:'2026-10-22T18:00'};
fixture.values=values;
const warn=(patch={},input=rules)=>noticeCycleWarnings({...values,...patch},input);
assert.deepEqual(Object.keys(warn()),['content','reason']);
assert.match(warn().content,/季度、半年度/);
assert.match(warn().reason,/按季/);
for(const cycle of ['每月','月度','按月','月检']) assert.deepEqual(warn({maintenance_cycle:cycle,content:'每月例行检查',reason:'按月度计划'}),{});
for(const [cycle,content] of [['半年','半年度与每六个月保养'],['每年','每年度保养年检'],['每两年','每两年或双年度保养'],['每三年','3年一次'],['每五年','五年一次']]) assert.deepEqual(warn({maintenance_cycle:cycle,content,reason:''}),{});
assert.match(warn({maintenance_cycle:'半年',content:'年度保养',reason:''}).content,/年度/);
assert.match(warn({maintenance_cycle:'每年',content:'半年度保养',reason:''}).content,/半年度/);
assert.match(warn({maintenance_cycle:'每年',content:'每2年保养',reason:''}).content,/每2年/);
for(const work_type of ['change','repair','polling','adjust','power','event','']) assert.deepEqual(warn({work_type}),{});
for(const notice_type of ['变更通告','设备检修通告','事件通告','维保']) assert.deepEqual(warn({notice_type}),{});
assert(Object.keys(warn({notice_type:undefined})).length,'known maintenance work with absent notice_type');
for(const maintenance_cycle of ['未知周期','每月/每季','月',null,{},'']) assert.deepEqual(warn({maintenance_cycle}),{});
for(const [cycle,content] of [['每季','三个月、每三个月、3个月和一个季度检查'],['半年','六个月和0.5年保养'],['每年','十二个月检查'],['每18个月','十八个月保养'],['每周','七天检查']]) assert.deepEqual(warn({maintenance_cycle:cycle,content,reason:''}),{});
for(const [cycle,content] of [['每季','每两个月维护'],['每年','18个月保养'],['每周','两周巡检']]) assert(warn({maintenance_cycle:cycle,content,reason:''}).content);
assert.deepEqual(warn({content:'2026年10月21日检查3台设备',reason:'12个过滤器'}),{});
assert.deepEqual(warn({maintenance_cycle:'每季',content:'计划在3月检修，一月维护完成',reason:''}),{});
const savedRules=rules.map(({interval,...rule})=>rule);
assert.deepEqual(warn({},savedRules),warn());
assert.deepEqual(warn({maintenance_cycle:'每季',content:'三个月维护',reason:'按季度计划'},savedRules),{});
for(const raw of [null,undefined,{},'invalid',[],[{value:'每月',aliases:null}]]) assert.deepEqual(noticeCycleWarnings(values,raw),{});
assert.deepEqual(maintenanceCycleRules(JSON.stringify(rules)),rules);
assert.deepEqual(warn({maintenance_cycle:'冲突'},[{value:'每月',aliases:['冲突']},{value:'每季',aliases:['冲突']}]),{});
assert.match(warn({maintenance_cycle:'a+b',content:'c(d)',reason:''},[{value:'a+b',aliases:['a+b']},{value:'c(d)',aliases:['c(d)']}]).content,/c\(d\)/);
const original=JSON.stringify(values);noticeCycleWarnings(values,rules);assert.equal(JSON.stringify(values),original);
assert.deepEqual(fixture.descriptors.maintenance.children.find(c=>c.path==='maintenance_cycle').cycle_rules,rules);
assert.deepEqual(fixture.options,['/','每月','每季','每年','半年','每两年','每三年','每五年','冬季保温每日','非计划性']);
assert.deepEqual(fixture.descriptors.maintenance.children.find(c=>c.path==='maintenance_cycle').suggestions,fixture.options);
for(const [kind,descriptor] of Object.entries(fixture.descriptors)) if(kind!=='maintenance') assert(!descriptor.children.some(c=>c.cycle_rules),'other notices have no cycle metadata');
for(const [kind,variants] of Object.entries(fixture.panel)) for(const [variant,item] of Object.entries(variants)){
 const cycle=item.fields.find(field=>field.key==='maintenance_cycle');
 if(kind==='maintenance') assert.deepEqual(cycle?.cycle_rules,rules,`Real notice_panel ${variant} payload must carry cycle_rules; descriptor metadata alone does not reach this panel`);
 else assert(!item.fields.some(field=>field.cycle_rules),`No cycle rules for ${kind} ${variant} panel`);
}
for(const name of ['workspace-v2/NoticeFields.vue','workspace-v2/NoticeWorkspace.vue','components/LighthouseStructuredField.vue','components/LighthouseNoticePanel.vue','components/LighthouseAssistant.vue']){
 const source=await readFile(path.join(root,'src',name),'utf8');const {descriptor,errors}=parse(source,{filename:name});assert.deepEqual(errors,[]);compileScript(descriptor,{id:name});
}
const formSource=await readFile(path.join(root,'src/workspace-v2/noticeForm.ts'),'utf8');
assert(formSource.split('export function validateNotice')[1].includes('noticeCycleWarnings'),'all actions must validate cycle conflicts');
assert(!formSource.split('export function noticeCommand')[1].split('export function ')[0].includes('cycleRules'),'cycle metadata stays out of submitted data');
const assistantSource=parse(await readFile(path.join(root,'src/components/LighthouseAssistant.vue'),'utf8')).descriptor.scriptSetup.content;
const ast=ts.createSourceFile('assistant.ts',assistantSource,ts.ScriptTarget.Latest,true);
const controlNode=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='planControl');
const extracted=ts.transpileModule('export '+controlNode.getText(ast),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText;
const {planControl}=await import('data:text/javascript;base64,'+Buffer.from(extracted).toString('base64'));
for(const body of [{work_type:'change'},{work_type:'maintenance',notice_type:'变更通告'},{work_type:'maintenance',patch:{notice_type:'变更通告'}},{work_type:'maintenance',patch:{work_type:'change'}}]){
 const control=planControl({operations:[{body}]},fixture.descriptors.maintenance);
 assert.deepEqual(noticeCycleWarnings({...values,work_type:control.work_type,notice_type:control.notice_type},rules),{});
}
const unknownControl=planControl({operations:[{body:{}}]},{native_notice:true,children:fixture.descriptors.maintenance.children});
assert.deepEqual(noticeCycleWarnings({...values,work_type:unknownControl.work_type},rules),{});
console.log('PASS source contracts: aliases, longest match, exact normalization, all notice types, no text mutation, descriptor and options isolation, SFC compilation.');
if(process.argv.includes('--source-only')) process.exit(0);

const escape=value=>String(value).replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;');
const detail=(kind,action)=>`<form id="lite-notice-form" data-action="${action}" data-detail-mode="${action==='start'?'planned':'ongoing'}" data-maintenance-cycle-rules="${escape(JSON.stringify(rules))}">
${Object.entries({scope:'A',work_type:kind,notice_type:kind==='maintenance'?'维保通告':'变更通告',source_record_id:'recFixture',...(action==='start'?{}:{target_record_id:'recTarget',active_item_id:'activeFixture'}),manual:'0'}).map(([key,value])=>`<input type="hidden" name="${key}" value="${escape(value)}">`).join('')}
<input type="checkbox" name="building_codes" value="A" checked>
${Object.entries({title:'名称',maintenance_cycle:'维保周期',content:'内容',reason:'原因'}).map(([key,label])=>`<label><span>${label}</span>${['content','reason'].includes(key)?`<textarea name="${key}" required>${escape(values[key])}</textarea>`:`<input name="${key}" value="${escape(values[key])}" required>`}</label>`).join('')}
${['start','update','end'].map(action=>`<button name="submit_action" value="${action}"></button>`).join('')}</form>`;
const html=`<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><div id="app"></div><script type="module">
import {createApp} from 'vue';import Fixture from '/scripts/notice-cycle-warnings-fixture.vue';import UiTransition from '/src/components/UiTransition.vue';
import '/src/global.css';import '/src/workspace-v2/workspace.css';import '/src/workspace-v2/night.css';
window.cycleFixture=${JSON.stringify(fixture).replaceAll('<','\\u003c')};createApp(Fixture).component('UiTransition',UiTransition).mount('#app');
</script></body></html>`;
const plugin={name:'isolated-cycle-fixture',configureServer(server){server.middlewares.use(async(req,res,next)=>{
 if(!req.url.startsWith('/__cycle'))return next();res.setHeader('Content-Type','text/html;charset=utf-8');res.end(await server.transformIndexHtml(req.url,html));
});}};
const server=await createServer({root,configFile:false,plugins:[vue(),plugin],logLevel:'error',server:{host:'127.0.0.1',port:0}});
await listenTestServer(server);const origin='http://127.0.0.1:'+server.httpServer.address().port;
if(process.argv.includes('--serve')){console.log(origin+'/__cycle?mode=workspace');await new Promise(()=>{});}
const browser=await chromium.launch({headless:true});
const helper=await build({entryPoints:[path.join(root,'src/workspace-v2/noticeForm.ts')],bundle:true,write:false,format:'iife',globalName:'noticeHelpers'});
const output=path.join(repo,'output/playwright/notice-cycle-warnings');await mkdir(output,{recursive:true});
let groups=0;
try{
 for(const viewport of [{width:402,height:874},{width:440,height:956},{width:1440,height:900}]){
  for(const action of ['start','update','end']){
   const context=await browser.newContext({viewport,reducedMotion:'reduce'}),page=await context.newPage(),errors=[],submissions=[];
   page.on('pageerror',error=>errors.push(error.message));
   await context.route('**/*',route=>{
    const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/workbench')return ok({records:[{record_id:'recFixture',title:'隔离周期核对',work_type:'maintenance',source_progress:'未开始'}],ongoing:[],records_pagination:{total:1},ongoing_pagination:{total:0}});
    if(url.pathname==='/api/workbench/lite-detail')return ok({fragments:{detail:detail('maintenance',action)}});
    if(url.pathname==='/api/workbench/draft')return ok({draft:request.method()==='GET'?null:{...request.postDataJSON(),version:1}});
    if(url.pathname==='/api/workbench-actions'){submissions.push(request.postDataJSON());return ok({job_id:'isolated-job',phase:'queued'});}
    if(url.pathname==='/api/jobs/isolated-job')return ok({job_id:'isolated-job',phase:'success'});
    if(request.method()!=='GET')throw new Error('Unexpected write: '+url.pathname);
    return ok({items:[],settings:{},records:[],fields:[]});
   });
   await page.goto(origin+'/__cycle?mode=workspace');
   await page.getByRole('button',{name:'隔离周期核对',exact:true}).click();
   const editor=page.locator('.v2-notice-editor'),warnings=editor.locator('[data-cycle-warning]');
   await warnings.first().waitFor();assert.equal(await warnings.count(),2);
   const content=editor.locator('[data-notice-field=content] textarea'),reason=editor.locator('[data-notice-field=reason] textarea'),cycle=editor.locator('[data-notice-field=maintenance_cycle] input');
   assert(await content.evaluate(el=>el.checkValidity()),'warning must not set custom validity');
   assert(await content.getAttribute('aria-describedby'));
   await content.fill('每月例行检查');await reason.fill('月度计划');await page.waitForFunction(()=>!document.querySelector('.v2-notice-editor [data-cycle-warning]'));
   await cycle.fill('每季');await warnings.first().waitFor();assert.equal(await warnings.count(),2);
   await cycle.fill('自定义周期');await page.waitForFunction(()=>!document.querySelector('.v2-notice-editor [data-cycle-warning]'));
   await cycle.fill('每月');await content.fill(values.content);await reason.fill(values.reason);await warnings.first().waitFor();
   await page.screenshot({path:path.join(output,`workspace-${viewport.width}-${action}.png`),fullPage:true});
   const label={start:'开始',update:'更新',end:'结束'}[action];
   assert(await editor.getByRole('button',{name:'预览并确认'+label,exact:true}).isEnabled());
   await editor.getByRole('button',{name:'预览并确认'+label,exact:true}).click();
   assert.equal(submissions.length,0,'conflicts must not submit any action');
   assert.equal(await editor.getByRole('button',{name:'确认发送'+label,exact:true}).count(),0);
   assert((await editor.innerText()).includes('修改一致后再发送'));
   assert.equal(await content.inputValue(),values.content,'invalid draft is retained');
   await content.fill('每月例行检查');await reason.fill('按月度计划维护');
   await editor.getByRole('button',{name:'预览并确认'+label,exact:true}).click();
   assert(!(await editor.getByLabel('待发送通告全文').innerText()).includes('周期核对提醒'),'preview text unchanged');
   await editor.getByRole('button',{name:'确认发送'+label,exact:true}).click();
   await editor.getByRole('heading',{name:'通告办理完成',exact:true}).waitFor().catch(async error=>{console.error({action,viewport,submissions,errors,text:await editor.innerText()});throw error;});
   assert.equal(submissions.length,1);assert.equal(submissions[0].action,action);assert.equal(submissions[0].patch.content,'每月例行检查');assert.equal(submissions[0].patch.reason,'按月度计划维护');
   assert(!JSON.stringify(submissions[0]).includes('cycleRules'));assert(!JSON.stringify(submissions[0]).includes('cycle_rules'));assert(!JSON.stringify(submissions[0]).includes('不阻止发送'));
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
   await page.addScriptTag({content:helper.outputFiles[0].text});
   const parsed=await page.evaluate(html=>{const h=window.noticeHelpers,f=h.parseNoticeForm(html);return {rules:f.cycleRules,error:h.validateNotice(f,'start',{},[]),command:h.noticeCommand(f,'start','test-operation',0,{},[],[])};},detail('maintenance','start'));
   assert.deepEqual(parsed.rules,rules);assert.match(parsed.error,/修改一致后再发送/);assert.equal(parsed.command.patch.content,values.content);
   for(const kind of ['change','repair','polling','power','adjust'])assert.deepEqual(await page.evaluate(html=>window.noticeHelpers.parseNoticeForm(html).cycleRules,detail(kind,'start')),[]);
   assert.deepEqual(errors,[]);await context.close();groups++;
  }
  for(const dark of [false,true]){
   const page=await browser.newPage({viewport,reducedMotion:'reduce'}),errors=[];page.on('pageerror',error=>errors.push(error.message));
   await page.route('**/*',route=>new URL(route.request().url()).origin===origin&&!new URL(route.request().url()).pathname.startsWith('/api/')?route.continue():route.abort());
   await page.goto(origin+'/__cycle?mode=structured&dark='+(dark?'1':'0'));
   const form=page.locator('.assistant-fixture'),warnings=form.locator('[data-cycle-warning]');
   await warnings.first().waitFor();assert.equal(await warnings.count(),2);
   await form.getByRole('button',{name:'补充并继续'}).click();assert.equal(await page.evaluate(()=>window.cycleControls.submitted.length),1);
   assert.equal(await page.evaluate(()=>window.cycleControls.submitted[0].content),values.content);
   await form.getByLabel('内容',{exact:true}).fill('每月保养');await form.getByLabel('原因',{exact:true}).fill('月度计划');await page.waitForFunction(()=>!document.querySelector('[data-cycle-warning]'));
   await form.getByLabel('维保周期',{exact:true}).fill('每季');await warnings.first().waitFor();
   await page.screenshot({path:path.join(output,`assistant-${viewport.width}-${dark?'dark':'light'}.png`),fullPage:true});
   for(const patch of [{notice_type:'变更通告'},{notice_type:'维保通告',work_type:'change'}]){
    await page.evaluate(patch=>window.cycleControls.setValues(patch),patch);await page.waitForFunction(()=>!document.querySelector('[data-cycle-warning]'));
   }
   await page.evaluate(()=>{window.cycleControls.setValues({work_type:'maintenance',notice_type:'维保通告'});window.cycleControls.setControl({notice_type:'变更通告'});});
   assert.equal(await warnings.count(),0);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);assert.deepEqual(errors,[]);
   await page.close();groups++;
  }
  const panelContext=await browser.newContext({viewport,reducedMotion:'reduce'}),panelPage=await panelContext.newPage(),panelWrites=[];
  const panelRun={id:'isolated-panel',revision:1,scope:'A',date:'2026-10-22',slot:'morning',state:'edit',items:[{
   key:'maintenance:fixture',title:'隔离周期待办',work_type:'maintenance',notice_type:'维保通告',action:'start',selected:false,brief_fields:['progress'],draft:{...values},
   fields:fixture.panel.maintenance.restored.fields.filter(field=>['maintenance_cycle','content','reason','progress'].includes(field.key)),
  }]};
  await panelContext.route('**/*',route=>{
   const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
   const ok=data=>route.fulfill({json:{ok:true,data}});
   if(url.pathname==='/api/assistant/notice-panel')return ok({scopes:[{value:'A',label:'A楼'}],edition:{id:'edition-fixture',date:'2026-10-22',slot:'morning',label:'08:00'}});
   if(url.pathname==='/api/assistant/notice-panel/open')return ok({run:panelRun});
   if(url.pathname==='/api/assistant/notice-panel/isolated-panel/action'){
    const payload=request.postDataJSON();panelWrites.push(payload);
    for(const change of payload.changes||[])Object.assign(panelRun.items[0],{...change,draft:{...panelRun.items[0].draft,...change.draft}});
    if(payload.action==='preview')panelRun.state='confirm';
    else if(payload.action==='edit')panelRun.state='edit';
    else if(payload.action==='confirm'){panelRun.state='done';panelRun.items[0].phase='success';panelRun.items[0].result='隔离发送成功';}
    return ok({run:panelRun});
   }
   if(url.pathname==='/api/assistant/notice-panel/isolated-panel')return ok({run:panelRun});
   return ok({});
  });
  await panelPage.goto(origin+'/__cycle?mode=panel');
  const panel=panelPage.locator('.notice-panel');await panel.getByText('隔离周期待办',{exact:true}).waitFor();
  await panel.locator('.row-heading input[type=checkbox]').check();await panel.locator('[data-cycle-warning]').first().waitFor();
   const panelContent=panel.locator('textarea[id$="-content"]');await panelContent.fill('每月保养');
  assert.equal(await panel.locator('[data-cycle-warning=content]').count(),0);assert.equal(await panelContent.inputValue(),'每月保养','focused field must remain mounted after repair');
  await panel.getByRole('button',{name:'编辑全部字段',exact:true}).click();await panelContent.fill(values.content);
  assert.equal(await panel.locator('[data-cycle-warning]').count(),2);
  await panel.getByRole('button',{name:'预览所选 1 条',exact:true}).click();await panel.locator('[data-cycle-warning-summary]').first().waitFor();
  await panel.getByRole('button',{name:'确认发送 1 条',exact:true}).click();
  assert.equal(panelWrites.filter(value=>value.action==='confirm').length,0,'panel conflicts must not reach submission');
  assert((await panel.innerText()).includes('修改一致后再发送'));
  await panel.getByRole('button',{name:'返回修改',exact:true}).click();
  const expandFields=panel.getByRole('button',{name:'编辑全部字段',exact:true});if(await expandFields.count())await expandFields.click();
  await panel.locator('textarea[id$="-content"]').fill('每月检查');await panel.locator('textarea[id$="-reason"]').fill('按月度计划维护');
  await panel.getByRole('button',{name:'预览所选 1 条',exact:true}).click();
  await panel.getByRole('button',{name:'确认发送 1 条',exact:true}).click();await panel.getByText('隔离发送成功',{exact:true}).waitFor();
  assert.equal(panelWrites.filter(value=>value.action==='confirm').length,1);
  assert.equal(panelRun.items[0].draft.content,'每月检查');assert.equal(panelRun.items[0].draft.reason,'按月度计划维护');
  assert.equal(await panelPage.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await panelContext.close();groups++;
 }
 console.log(`PASS: ${groups} source-browser viewport/action/theme groups; blocked start/update/end, corrected submissions, equivalent durations, draft retention and type isolation. Screenshots: ${output}`);
}finally{await browser.close();await server.close();}
