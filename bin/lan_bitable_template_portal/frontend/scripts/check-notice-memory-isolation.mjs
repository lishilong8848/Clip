import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {build} from 'esbuild';
import {runInNewContext} from 'node:vm';
import {preview} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const compiled=await build({entryPoints:[path.join(root,'src/workspace-v2/noticeForm.ts')],bundle:true,write:false,format:'cjs'});
const module={exports:{}};runInNewContext(compiled.outputFiles[0].text,{module,exports:module.exports,Set,Date,JSON});
const {restoreNoticeEdits}=module.exports;
const fresh=()=>({contextVersion:'C:maintenance:filter:monthly',values:{content:'新历史',reason:'新原因',execution_party:'自维'},fields:['content','reason','execution_party'].map(key=>({key,readonly:false}))});
const cached={schema:2,context:'C:maintenance:filter:monthly',draftVersion:0,baseValues:{content:'旧历史',reason:'旧原因',execution_party:'厂维'},snapshot:{values:{content:'用户手填',reason:'',execution_party:'厂维'}}};
let form=fresh();assert(restoreNoticeEdits(form,cached,0));
assert.equal(form.values.content,'用户手填');assert.equal(form.values.reason,'');assert.equal(form.values.execution_party,'自维','old automatic values never overwrite fresh defaults');
for(const context of ['A:maintenance:filter:monthly','C:maintenance:diesel:monthly','C:maintenance:filter:quarterly','C:change:filter:monthly']){
  form=fresh();form.contextVersion=context;assert(!restoreNoticeEdits(form,cached,0));assert.equal(form.values.content,'新历史');
}
assert(!restoreNoticeEdits(fresh(),{...cached,schema:1},0));
assert(!restoreNoticeEdits(fresh(),cached,1));

const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
let locationText='C楼冷站',cycle='每月',user='memory-user';
const names={filter:'冷站Y型过滤器检查',diesel:'柴发空载测试'};
const escape=value=>String(value).replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;');
const detail=id=>`<form id="lite-notice-form" data-action="start" data-detail-mode="planned" data-draft-context="C:maintenance:${id}:${cycle}">
${Object.entries({scope:'C',work_type:'maintenance',notice_type:'维保通告',source_record_id:id,source_month:'10月',manual:'0',maintenance_cycle:cycle}).map(([k,v])=>`<input type="hidden" name="${k}" value="${escape(v)}">`).join('')}
<input type="checkbox" name="building_codes" checked value="C">
${Object.entries({title:names[id],content:id==='filter'?'过滤器检查':'柴发空载测试',reason:'例行检查',location:id==='filter'?locationText:'柴发机房',execution_party:'自维'}).map(([k,v])=>`<label><span>${({title:'名称',content:'内容',reason:'原因',location:'位置',execution_party:'执行方'})[k]}</span><input name="${k}" value="${escape(v)}"></label>`).join('')}
<select name="source_record_id"><option value="${id}">${names[id]}</option><option value="${id==='filter'?'diesel':'filter'}">更换计划</option></select><button name="submit_action" value="start"></button></form>`;
try{
 const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'}),page=await context.newPage(),errors=[],writes=[];
 page.on('pageerror',error=>errors.push(error.message));
 await context.route('**/*',route=>{
  const request=route.request(),url=new URL(request.url());if(url.origin!==origin)return route.abort();if(!url.pathname.startsWith('/api/'))return route.continue();
  if(request.method()!=='GET')writes.push(url.pathname);
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:user,name:'隔离检查',modules:['maintenance']},scope_options:[{value:'C',label:'C楼'}]});
  if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'memory-test'}});
  if(url.pathname==='/api/workbench')return ok({records:Object.entries(names).map(([record_id,title])=>({record_id,title,work_type:'maintenance',building:'C楼',source_progress:'未开始'})),ongoing:[],records_pagination:{total:2},ongoing_pagination:{total:0}});
  if(url.pathname==='/api/workbench/lite-detail')return ok({fragments:{detail:detail(url.searchParams.get('record_id')||'filter')}});
  if(url.pathname==='/api/workbench/planned-notice-prefill')return ok({selected:{source_record_id:url.searchParams.get('source_record_id')},draft:{title:names[url.searchParams.get('source_record_id')]},recommendations:[]});
  if(url.pathname==='/api/workbench/source-options')return ok({items:Object.entries(names).map(([source_record_id,title])=>({source_record_id,title,building:'C楼',maintenance_cycle:cycle}))});
  return ok({draft:null,items:[],settings:{},records:[]});
 });
 const editor=page.locator('.v2-notice-editor');
 const input=label=>editor.locator('.n-form-item').filter({has:page.locator('.n-form-item-label',{hasText:new RegExp('^'+label+'$')})}).locator('input').first();
 const open=async id=>{await page.locator('[data-list=planned]').getByRole('button',{name:names[id],exact:true}).click();await input('内容').waitFor();};
 const close=async()=>{await editor.getByRole('button',{name:'关闭',exact:true}).click();const leave=page.getByRole('button',{name:'保留副本并离开',exact:true});if(await leave.isVisible())await leave.click();await editor.waitFor({state:'hidden'});};
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/workbench-lite&scope=C&work_type=maintenance&month=10月');
 await open('filter');await input('内容').fill('用户手填过滤器内容');await input('原因').fill('');await close();
 locationText='C楼新冷站位置';await open('filter');assert.equal(await input('内容').inputValue(),'用户手填过滤器内容');assert.equal(await input('原因').inputValue(),'');assert.equal(await input('位置').inputValue(),locationText);await close();
 cycle='每季';await open('filter');assert.equal(await input('内容').inputValue(),'过滤器检查','a new cycle cannot restore monthly edits');
 const binding=page.locator('[data-notice-section=binding]');await binding.getByRole('button',{name:'更换关联',exact:true}).click();await binding.locator('.n-select').click();await page.locator('.n-base-select-option').getByText('柴发空载测试 · 每季 · C楼',{exact:true}).click();await binding.getByRole('button',{name:'确认关联',exact:true}).click();
 await input('内容').filter({visible:true}).waitFor();await page.waitForFunction(()=>document.querySelector('.v2-notice-editor input')&&[...document.querySelectorAll('.v2-notice-editor input')].some(el=>el.value==='柴发空载测试'));
 assert.equal(await input('内容').inputValue(),'柴发空载测试');assert.equal(await input('位置').inputValue(),'柴发机房');await close();
 user='another-memory-user';await page.reload();await open('filter');assert.equal(await input('内容').inputValue(),'过滤器检查');await close();
 for(const width of [360,390]){await page.setViewportSize({width,height:844});await open('filter');assert.equal(await input('内容').inputValue(),'过滤器检查');await close();assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);}
 assert.deepEqual(writes,[]);assert.deepEqual(errors,[]);
 console.log('PASS: actual PC/mobile editor, user-edit-only restoration, fresh automatic values, account/cycle isolation and source replacement. No business writes.');
}finally{await browser.close();await server.close();}
