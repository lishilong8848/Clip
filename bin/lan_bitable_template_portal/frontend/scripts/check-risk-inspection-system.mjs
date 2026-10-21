import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/risk-system');
await mkdir(output,{recursive:true});
const roles=['number','status','scope','person','reviewer','review','reviewed_at','sync','inspected_at'];
const fixed=roles.map((role,index)=>({key:'fixed_'+index,field_id:'fixed_'+index,field_name:['任务编号','排查进度','楼栋','排查人','复核人','复核结果','复核日期','最终推送','排查日期'][index],role:role==='inspected_at'?'input':role,type:['reviewed_at','inspected_at'].includes(role)?5:1,is_primary:role==='number',required:true}));
const answer={key:'answer',field_id:'answer',field_name:'现场情况',role:'input',type:1,required:true};
const fields=[...fixed,answer];
const directory=[
  {id:'admin',name:'发起人',employee_no:'001',home_scope:'A'},
  {id:'alice',name:'排查人员甲',employee_no:'002',home_scope:'A'},
  {id:'reviewer',name:'受限审核人',employee_no:'003',home_scope:'C'},
  {id:'managerA',name:'甲楼经理',can_receive_final:true,final_scopes:['A']},
  {id:'managerB',name:'乙楼经理',can_receive_final:true,final_scopes:['B']},
  {id:'managerC',name:'无楼栋经理',can_receive_final:true,final_scopes:[]},
].map(person=>({...person,scopes:['A','B'],can_review:true,active:true}));
const template={id:'base-a',kind:'base',name:'隔离风险模板',enabled:true,version:'v1',revision:1,creator:'发起人',created:1792152000,updated:1792152000,scopes:['A','B'],fields,items:[{title:'设备检查',values:{answer:'保留的预填内容'}}]};
const csv=Buffer.from('排查事项,现场情况\n设备检查,导入预填\n');
let actor,people,reviewerIds,templates,publication,writes,requests,failFile,failBitable,holdImport,assigned,legacyResponse;
function reset(){
  actor='alice';reviewerIds=['reviewer','alice'];templates=[structuredClone(template)];publication=null;writes=[];requests=[];failFile=false;failBitable=false;holdImport=false;assigned=true;legacyResponse=false;
  const item=(id,scope,status='pending')=>({id,scope,title:'设备检查',status,values:status==='submitted'?{answer:'已核对'}:{},files:[],system_values:{fixed_0:'自动编号',fixed_2:scope+'楼',fixed_4:'受限审核人',fixed_8:null}});
  people=[{id:'alice',name:'排查人员甲',scope:'A',scopes:['A','B'],version:1,revoked:false,items:[item('a-A','A'),item('a-B','B')]},{id:'bob',name:'排查人员乙',scope:'B',scopes:['B'],version:1,revoked:false,items:[item('b-B','B','submitted')]}];
}
function detail(){
  const data={id:'release',name:'隔离风险排查',owner:'发起人',reviewers:reviewerIds.map(id=>({id,name:directory.find(person=>person.id===id).name})),reviewer_ids:reviewerIds,reviewer:'旧单值复核人',reviewer_id:reviewerIds[0],reviewer_version:1,deadline:Date.now()+86400000,status:'active',fields,scopes:['A','B'],self_id:actor,is_manager:actor==='admin',is_reviewer:reviewerIds.includes(actor),can_manage_release:actor==='admin',can_delete_release:false,can_claim:false,claim_scopes:[],people:structuredClone(people),jobs:[],audit:[],final_managers:[{id:'managerA',name:'甲楼经理'},{id:'managerB',name:'乙楼经理'}]};
  if(legacyResponse){delete data.reviewers;delete data.reviewer_ids;}return data;
}
function imported(kind){return {name:kind==='file'?'文件导入草稿':'多维表导入草稿',fields,items:[{title:'导入设备检查',values:{answer:'导入预填'}}],source:kind==='file'?{kind:'file',filename:'检查.csv'}:{kind:'bitable',url:'https://example.feishu.cn/base/Offline?table=table1'},warnings:['离线预览提醒'],needs_selection:false};}
function harness(version){
  const entry='/src/__risk_system_test.ts';
  return {name:'risk-system-offline',resolveId(id){if(id===entry)return path.join(root,entry.slice(1));},load(id){
    if(id.replaceAll('\\','/')!==path.join(root,entry.slice(1)).replaceAll('\\','/'))return;
    return `import {createApp,h} from 'vue';import {NConfigProvider,zhCN,dateZhCN} from 'naive-ui';
import RiskInspectionPage from '/src/components/RiskInspectionPage.vue';
import {theme} from '/src/workspace-v2/theme.ts';import '/src/global.css';
${version==='v2'?['workspace','interactions','motionBlocks','vipboard','feedback','feedbackToasts','pageFilters','pageLayout','compactWorkspaces','header','scrollbars','night','scrollLayout'].map(name=>`import '/src/workspace-v2/${name}.css';`).join('\n'):''}
const query=new URLSearchParams(location.search);
let app;if(query.has('shell')){app=createApp((await import('${version==='v2'?'/src/workspace-v2/Workspace.vue':'/src/App.vue'}')).default);${version==='legacy'?"app.component('UiTransition',(await import('/src/components/UiTransition.vue')).default);app.component('LoadingIndicator',(await import('/src/components/LoadingIndicator.vue')).default);":''}}
else app=createApp({render:()=>h(NConfigProvider,{themeOverrides:theme,locale:zhCN,dateLocale:dateZhCN},()=>h(RiskInspectionPage,{scope:'A',releaseId:query.get('id')||''}))});app.mount('#app');`;
  },configureServer(server){server.middlewares.use(async(req,res,next)=>{
    try{
      const url=new URL(req.url,'http://127.0.0.1');
      if(url.pathname.endsWith('/__risk-system.html')||url.pathname==='/assets/workspace-v2/index.html'||['/risk-inspections','/water-management'].includes(url.pathname)){
        res.setHeader('Content-Type','text/html; charset=utf-8');res.end(await server.transformIndexHtml(req.url,`<!doctype html><html lang="zh-CN" ${version==='v2'?'class="workspace-v2"':''}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>风险排查隔离验收</title></head><body><div id="app"></div><script type="module" src="${entry}"></script></body></html>`));return;
      }
      if(!url.pathname.startsWith('/api/'))return next();
      const endpoint=url.pathname.slice('/api/risk-inspections'.length),method=req.method;
      const chunks=[];if(method!=='GET')for await(const chunk of req)chunks.push(chunk);
      const raw=Buffer.concat(chunks),body=endpoint==='/base-templates/import-file'?null:raw.length?JSON.parse(raw.toString()):{};
      requests.push({endpoint,method,query:url.search});if(method!=='GET')writes.push({endpoint,method,body});let data;
      if(url.pathname==='/api/auth/status')data={logged_in:true,user:{open_id:'offline-'+actor,name:actor,level:actor==='admin'?'L0':'L4',role:actor==='admin'?'admin':'building',home_scope:'A',scope_rules:[],modules:['risk_inspection'],risk_inspection_assigned:assigned},scope_options:actor==='admin'?[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]:[],default_scope:''};
      else if(url.pathname==='/api/health'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,service:'clipflow_backend',instance_id:'risk-fixture'}));return;}
      else if(!url.pathname.startsWith('/api/risk-inspections')){assert.equal(method,'GET','No unrelated writes');data={items:[],records:[],total:0,summary:{},scopes:{}};}
      else if(!endpoint&&method==='GET'){
        if(actor!=='admin'||url.searchParams.get('view')==='mine')assert.equal(url.searchParams.has('scope'),false,'Home building cannot filter assigned tasks');
        data={items:[{id:'release',name:'隔离风险排查',reviewer:'受限审核人、排查人员甲',deadline:Date.now()+86400000,status:'active',people_count:2,total:3,submitted:1,approved:0}],total:1,is_manager:actor==='admin',can_edit_template:actor==='admin'};
      }else if(!endpoint&&method==='POST'){assert(Array.isArray(body.reviewer_ids));assert(body.people.every(p=>Array.isArray(p.scopes)&&!('scope' in p)));publication=body;reviewerIds=body.reviewer_ids;data=detail();}
      else if(endpoint==='/people')data={self_id:actor,items:directory,scopes:['A','B']};
      else if(endpoint==='/template')data={fields,version:'library-v1'};
      else if(endpoint==='/base-templates/import-file'||endpoint==='/base-templates/import-bitable'){
        const file=endpoint.endsWith('import-file');
        if(file){assert(url.searchParams.get('name')?.match(/\.(csv|xlsx)$/));assert.deepEqual(raw,csv,'Raw upload bytes survive unchanged');}else assert.deepEqual(Object.keys(body).sort(),body.title_field?['title_field','url']:['url']);
        if(holdImport){await new Promise(resolve=>setTimeout(resolve,600));data=imported(file?'file':'bitable');}
        else if(file&&failFile||!file&&failBitable){if(file)failFile=false;else failBitable=false;res.statusCode=422;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:false,error:'离线模拟：导入失败'}));return;}
        else if(file&&url.searchParams.get('name').endsWith('.xlsx')&&!url.searchParams.has('sheet'))data={name:'选择工作表',fields:[],items:[],source:{},warnings:[],needs_selection:true,sheets:['机房','配电']};
        else if(file&&url.searchParams.get('name').endsWith('.xlsx')&&!url.searchParams.has('title_field'))data={name:'选择事项列',fields:[],items:[],source:{},warnings:[],needs_selection:true,title_fields:['设备','部件']};
        else if(!file&&!body.title_field)data={name:'选择事项列',fields:[],items:[],source:{},warnings:[],needs_selection:true,title_fields:['设备','部件']};
        else data=imported(file?'file':'bitable');
      }else if(endpoint==='/base-templates'||endpoint==='/templates'){
        if(method==='POST'){assert.equal(endpoint,'/base-templates');assert.deepEqual(Object.keys(body).sort(),['enabled','fields','id','items','name']);assert(!templates.some(t=>t.id===body.id),'Import always creates a new template');const saved={...body,kind:'base',version:'saved-v1',revision:1,creator:'发起人',created:1792152000,updated:1792152000,scopes:['A','B']};templates.push(saved);data=saved;}
        else data={items:templates.map(t=>({...t,item_count:t.items.length,field_count:t.fields.length})),total:templates.length};
      }else if(endpoint==='/base-templates/base-a'||endpoint==='/templates/base-a')data={...template,kind:endpoint.startsWith('/templates/')?'history':'base'};
      else if(endpoint==='/release/reviewer'){assert(Array.isArray(body.reviewer_ids)&&body.reviewer_ids.length);reviewerIds=body.reviewer_ids;data=detail();}
      else if(endpoint.endsWith('/actions')){
        const person=people.find(p=>p.id===endpoint.split('/')[3]),item=person?.items.find(i=>i.id===body.item_id);assert(person&&item);
        if(['save','submit'].includes(body.action)){assert.equal(actor,person.id);assert.deepEqual(Object.keys(body.values),['answer'],'System values are never copied into user answers');item.values=body.values;if(body.action==='submit'){item.status='submitted';item.system_values.fixed_8=1792152000000;}}
        else {assert(detail().is_reviewer);assert.equal(item.status,'submitted');item.status=body.action==='approve'?'approved':'returned';item.reviewed_by=actor;item.system_values.fixed_5=item.status;}
        person.version++;data=detail();
      }else if(endpoint==='/release')data=detail();
      else throw new Error('Unexpected offline endpoint: '+method+' '+url.pathname);
      res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:true,data}));
    }catch(error){res.statusCode=500;res.setHeader('Content-Type','application/json');res.end(JSON.stringify({ok:false,error:error.message}));console.error(error);}
  });}};
}
async function choose(page,label,text){const select=page.locator('.n-select[aria-label="'+label+'"]'),multiple=await select.locator('.n-base-selection').evaluate(el=>el.classList.contains('n-base-selection--multiple'));await select.locator('.n-base-suffix').click();const menu=page.locator('.n-base-select-menu:visible');await menu.waitFor();assert.equal(await menu.evaluate(el=>!!el.closest('.n-modal,.n-drawer')),false,label+': list must escape modal and drawer clipping');await menu.locator('.n-base-select-option').filter({hasText:text,visible:true}).click();if(multiple)await page.keyboard.press('Escape');await page.locator('.n-base-select-option:visible').first().waitFor({state:'hidden'});}
async function noSystemFields(page){for(const field of fixed)assert.equal(await page.locator('[data-risk-field="'+field.key+'"],.risk-selected-fields strong,.risk-field-pick strong,.risk-sheet th,.risk-feedback-summary dt').filter({hasText:field.field_name}).count(),0,'System field not rendered: '+field.field_name);assert.equal(await page.locator('.risk-field-system').count(),0);}
async function capture(page,tag){await page.screenshot({path:path.join(output,tag+'.png'),fullPage:true,animations:'disabled'});await page.screenshot({path:path.join(output,tag+'-viewport.png'),animations:'disabled'});assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'No page overflow: '+tag);}
async function checkInputs(page,tag,root='.risk-publish',minimum=5){
  const checks=[];
  for(const control of await page.locator(root).locator('input:not([type="hidden"]),textarea,.n-base-selection,.risk-readonly').all()){
    if(!await control.isVisible())continue;
    const small=await control.evaluate(el=>{const r=el.getBoundingClientRect();return r.width<20||r.height<20;});if(small)continue;
    await control.scrollIntoViewIfNeeded();const result=await control.evaluate(el=>{const r=el.getBoundingClientRect(),x=r.x+r.width/2,y=r.y+r.height/2,owns=point=>{const hit=document.elementFromPoint(point,y);return !!hit&&(el.contains(hit)||hit.contains(el)||!!el.closest('.n-input')?.contains(hit));};return {label:el.getAttribute('aria-label')||el.closest('[aria-label]')?.getAttribute('aria-label')||el.tagName,rect:{x:r.x,y:r.y,width:r.width,height:r.height},hit:owns(x),edges:owns(r.x+8)&&owns(r.right-8),within:x>=0&&x<innerWidth&&y>=0&&y<innerHeight};});checks.push(result);
  }
  assert(checks.length>=minimum);await writeFile(path.join(output,tag+'-inputs.json'),JSON.stringify(checks,null,2));await page.screenshot({path:path.join(output,tag+'-inputs.png'),animations:'disabled'});assert(checks.every(result=>result.hit&&result.within&&result.edges),'Input center or edge is obscured: '+JSON.stringify(checks.filter(result=>!result.hit||!result.within||!result.edges)));
}
async function confirm(page){await page.locator('.risk-confirm').getByRole('button',{name:'确认',exact:true}).click();await page.locator('.risk-confirm').waitFor({state:'hidden'});}
async function testFeedback(page,url,tag){
  await page.goto(url);await page.locator('.risk-task-name:visible').click();await choose(page,'反馈楼栋','B楼');
  assert.equal(await page.locator('.risk-feedback-table').getByText('A楼',{exact:true}).count(),0,'Filter uses each item scope');
  await page.getByRole('button',{name:'处理 排查人员甲 · 设备检查',exact:true}).click();const drawer=page.locator('.risk-feedback-drawer');await drawer.waitFor();await noSystemFields(page);
  assert.match(await drawer.innerText(),/排查人员甲 · B楼/);await drawer.getByRole('textbox',{name:'现场情况',exact:true}).fill('第二楼独立反馈');
  await drawer.getByRole('button',{name:'保存草稿',exact:true}).click();await drawer.getByText('草稿已保存。',{exact:true}).waitFor();
  assert.deepEqual(people[0].items[0].values,{},'Saving B must not change A');assert.deepEqual(people[0].items[1].values,{answer:'第二楼独立反馈'});
  await drawer.getByRole('button',{name:'提交审核',exact:true}).click();await confirm(page);await noSystemFields(page);assert.equal(people[0].items[1].system_values.fixed_8,1792152000000);
  assert.equal(await drawer.getByRole('button',{name:'审核通过',exact:true}).count(),0,'Filling never mixes review actions');
  await page.keyboard.press('Escape');await drawer.waitFor({state:'hidden'});await page.getByRole('button',{name:'进入审核任务',exact:true}).click();await choose(page,'反馈楼栋','B楼');await page.getByRole('button',{name:'处理 排查人员甲 · 设备检查',exact:true}).click();
  assert.equal(await drawer.getByRole('button',{name:'保存草稿',exact:true}).count(),0,'Review is a separate read-only page');
  await drawer.getByRole('button',{name:'审核通过',exact:true}).click();await confirm(page);assert.equal(people[0].items[1].status,'approved','Designated reviewers can self-review');await capture(page,tag+'-self-review');await page.keyboard.press('Escape');await drawer.waitFor({state:'hidden'});
  actor='reviewer';await page.goto(url+'?view=review');await page.locator('.risk-task-name:visible').click();await page.getByRole('button',{name:'处理 排查人员乙 · 设备检查',exact:true}).click();await drawer.getByRole('button',{name:'审核通过',exact:true}).click();await confirm(page);assert.equal(people[1].items[0].status,'approved','Reviewer with no building permissions can review assigned tasks');await noSystemFields(page);await capture(page,tag+'-assigned-reviewer');
  await page.keyboard.press('Escape');actor='alice';legacyResponse=true;delete people[0].items[0].scope;
  await page.goto(url);await page.locator('.risk-task-name:visible').click();await page.getByRole('button',{name:'处理 排查人员甲 · 设备检查',exact:true}).first().click();assert.match(await drawer.innerText(),/A楼/,'Legacy items fall back to person.scope');
}
async function testPublish(page,url,tag){
  actor='admin';await page.goto(url);await page.getByRole('button',{name:'发布排查',exact:true}).click();await page.getByRole('button',{name:'选用并填写',exact:true}).click();
  assert.match(await page.locator('.n-select[aria-label="审核人"]').innerText(),/发起人/);await choose(page,'审核人','受限审核人');await choose(page,'接收人员','排查人员甲');await choose(page,'接收人员','发起人');
  await page.locator('.risk-recipients summary').click();await choose(page,'排查人员甲的接收楼栋','B楼');const scope=page.locator('.n-select[aria-label="排查人员甲的接收楼栋"]');assert.match(await scope.innerText(),/A楼/);assert.match(await scope.innerText(),/B楼/);assert(!await scope.locator('.n-base-selection').evaluate(el=>el.classList.contains('n-base-selection--disabled')));
  await choose(page,'接收经理','甲楼经理');await page.getByText(/接收经理需覆盖全部接收楼栋/).waitFor();await page.locator('.n-select[aria-label="接收经理"] .n-base-selection').click();assert.equal(await page.locator('.n-base-select-option').filter({hasText:'无楼栋经理',visible:true}).count(),0,'Use final_scopes');await page.keyboard.press('Escape');await choose(page,'接收经理','乙楼经理');
  await page.locator('.risk-publish .n-date-picker input').fill('2099-01-01 12:00:00');await page.keyboard.press('Enter');await page.keyboard.press('Tab');await noSystemFields(page);await capture(page,tag+'-publish');await checkInputs(page,tag+'-publish');await page.getByRole('button',{name:'确认发布',exact:true}).click();await confirm(page);
  assert.deepEqual(publication.reviewer_ids,['admin','reviewer']);assert.deepEqual(publication.people,[{id:'alice',scopes:['A','B']},{id:'admin',scopes:['A']}]);assert.deepEqual(publication.items,template.items);
  await page.getByRole('button',{name:'更换审核人',exact:true}).click();await choose(page,'新审核人','排查人员甲');await confirm(page);assert(reviewerIds.includes('alice'));
}
async function testImports(page,url,tag){
  actor='admin';await page.goto(url);await page.getByRole('button',{name:'基础模板',exact:true}).click();await page.locator('.history-list article').waitFor();assert.equal(await page.locator('.risk-field-picker').count(),0);assert.equal(requests.filter(r=>r.endpoint==='/template').length,0,'List entry does not start creation');
  await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.locator('.risk-field-picker').waitFor();await noSystemFields(page);assert.equal(await page.locator('.risk-selected-fields li').count(),0);await page.getByRole('button',{name:'模板列表',exact:true}).click();
  await page.getByRole('button',{name:'查看 / 编辑',exact:true}).click();await page.locator('.risk-selected-fields li').waitFor();await noSystemFields(page);await page.getByRole('button',{name:'模板列表',exact:true}).click();
  const modal=page.locator('.risk-history-import-modal'),open=async kind=>{await page.getByRole('button',{name:kind==='file'?'导入表格':'导入多维表',exact:true}).click();await modal.waitFor();},pick=async name=>modal.getByLabel('选择导入表格').setInputFiles({name,mimeType:'application/octet-stream',buffer:csv});
  const preview=async()=>{const response=page.waitForResponse(response=>response.url().includes('/base-templates/import-'));await modal.getByRole('button',{name:/^(预览导入|重新预览)$/}).click();await response;await page.waitForFunction(()=>[...document.querySelectorAll('.risk-history-import-modal button')].some(el=>/^(预览导入|重新预览)$/.test(el.textContent.trim())&&!el.disabled));},close=async()=>{await modal.getByRole('button',{name:'取消',exact:true}).click();await modal.waitFor({state:'hidden'});};
  const before=writes.length;await open('file');await pick('旧格式.xls');await modal.getByText(/请选择20MB以内/).waitFor();assert.equal(writes.length,before);await pick('检查.csv');failFile=true;await preview();await modal.getByText('离线模拟：导入失败',{exact:true}).waitFor();assert.match(await modal.innerText(),/检查.csv/);await close();assert.equal(templates.length,1);
  await open('file');await pick('多工作表.xlsx');await preview();assert(await modal.getByRole('button',{name:'确认带入新草稿',exact:true}).isDisabled());await choose(page,'导入工作表','配电');await preview();await choose(page,'导入事项名称列','设备');await preview();
  await choose(page,'导入工作表','机房');assert.equal(await modal.locator('.n-select[aria-label="导入事项名称列"]').count(),0);await preview();assert(!new URLSearchParams(requests.at(-1).query).has('title_field'),'Changing sheet clears the old title selection');assert(await modal.getByRole('button',{name:'确认带入新草稿',exact:true}).isDisabled());await choose(page,'导入事项名称列','部件');await preview();await noSystemFields(page);await capture(page,tag+'-import-preview');await checkInputs(page,tag+'-import-preview','.history-import-preview',2);await close();assert.equal(templates.length,1);
  await open('file');await pick('检查.csv');holdImport=true;await modal.getByRole('button',{name:'预览导入',exact:true}).click();await close();await page.waitForTimeout(650);holdImport=false;assert.equal(await page.locator('.risk-field-picker').count(),0,'Late cancelled responses cannot replace the library');
  await open('file');await pick('检查.csv');await preview();await modal.getByRole('button',{name:'确认带入新草稿',exact:true}).click();await modal.waitFor({state:'hidden'});await page.locator('.risk-field-picker').waitFor();await noSystemFields(page);await page.getByText('离线预览提醒',{exact:true}).waitFor();assert.equal(templates.length,1);
  await page.locator('.history-base-prefill summary').click();assert.equal(await page.getByRole('textbox',{name:'现场情况',exact:true}).inputValue(),'导入预填');await capture(page,tag+'-import-draft');await page.getByRole('button',{name:'生成基础模板',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});
  assert.equal(templates.length,2);assert.deepEqual(templates[1].fields,fields);assert.deepEqual(templates[1].items,imported('file').items);assert.deepEqual(templates[0],template);
  await page.getByRole('button',{name:'模板列表',exact:true}).click();await open('bitable');await modal.getByRole('textbox',{name:'多维表链接',exact:true}).fill('https://example.feishu.cn/base/Offline?table=table1');failBitable=true;await preview();await modal.getByText('离线模拟：导入失败',{exact:true}).waitFor();assert.match(await modal.getByRole('textbox',{name:'多维表链接',exact:true}).inputValue(),/Offline/);await close();assert.equal(templates.length,2);
  await open('bitable');await modal.getByRole('textbox',{name:'多维表链接',exact:true}).fill('https://example.feishu.cn/base/Offline?table=table1');await preview();await choose(page,'导入事项名称列','设备');await preview();
  await modal.getByRole('textbox',{name:'多维表链接',exact:true}).fill('https://example.feishu.cn/base/Offline?table=table2');assert.equal(await modal.locator('.n-select[aria-label="导入事项名称列"]').count(),0);await preview();assert.deepEqual(Object.keys(writes.at(-1).body),['url'],'Changing Bitable source clears the old title selection');await choose(page,'导入事项名称列','部件');await preview();
  await modal.getByRole('button',{name:'确认带入新草稿',exact:true}).click();await modal.waitFor({state:'hidden'});assert.equal(templates.length,2);await page.getByRole('button',{name:'生成基础模板',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});assert.equal(templates.length,3);assert.deepEqual(templates[2].fields,fields);assert(!writes.some(w=>w.endpoint===''||w.method==='PUT'));
}
async function testShell(page,origin,version){
  actor='reviewer';assigned=true;
  const shellUrl=route=>origin+(version==='v2'?'/assets/workspace-v2/index.html?__page='+route+'&':route+'?')+'shell=1&id=release&scope=B';
  await page.goto(shellUrl('/risk-inspections'));await page.locator('.risk-feedback-table').waitFor();assert.equal(await page.locator('.module-denied,.n-result--403-type').count(),0);
  await page.goto(shellUrl('/water-management'));await page.getByText('没有该功能权限',{exact:true}).waitFor();assert.equal(await page.locator('.risk-page').count(),0,'No access expansion to other modules');
  assigned=false;await page.goto(shellUrl('/risk-inspections'));await page.getByText(/申请.*权限/).first().waitFor();assert.equal(await page.locator('.risk-page').count(),0,'Module alone cannot bypass no-scope guard');
}
const browser=await chromium.launch({headless:true}),reports=[];
try{
  for(const version of process.argv.includes('--preview-only')?['v2']:['legacy','v2']){
    reset();const server=await createServer({root,configFile:version==='v2'?path.join(root,'vite.workspace-v2.config.ts'):false,cacheDir:path.join(output,'vite-cache-'+version),publicDir:false,plugins:[harness(version),...version==='legacy'?[vue()]:[]],server:{host:'127.0.0.1',port:0,hmr:false,proxy:{}},optimizeDeps:{noDiscovery:true,include:['vue','naive-ui','lucide-vue-next']},logLevel:'error'});
    try{
      await server.listen();const origin='http://127.0.0.1:'+server.httpServer.address().port,url=origin+(version==='v2'?'/assets/workspace-v2':'')+'/__risk-system.html';
      for(const scenario of process.argv.includes('--preview-only')?[]:[{width:1440,height:900},{width:402,height:874},{width:440,height:956}]){
        reset();const context=await browser.newContext({viewport:scenario,reducedMotion:'reduce',serviceWorkers:'block'}),errors=[];await context.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));const tag=version+'-'+scenario.width;
        try{if(process.argv.includes('--visual-only')){reset();await testPublish(page,url,tag);assert.deepEqual(errors,[]);reports.push({tag,checks:'publish screenshots and unobscured input hit checks'});continue;}await testFeedback(page,url,tag);console.log('PASS '+tag+' feedback');reset();await testPublish(page,url,tag);console.log('PASS '+tag+' publish');reset();await testImports(page,url,tag);console.log('PASS '+tag+' imports');await testShell(page,origin,version);assert.deepEqual(errors,[]);reports.push({tag,checks:'real page and app shell; restricted/self-review; per-item scope; multi-reviewers and replacement; list-first; imports fail/cancel/select/confirm/save; hidden schema and answer retention; no global access expansion'});console.log('PASS '+tag);}
        catch(error){await page.screenshot({path:path.join(output,tag+'-failure.png'),fullPage:true});await writeFile(path.join(output,tag+'-failure.txt'),await page.locator('body').innerText());throw error;}
        finally{await context.close();}
      }
      if(version==='v2'&&process.argv.includes('--responsively')){reset();try{const result=await promisify(execFile)(process.execPath,['C:/Users/l1773/.codex/scripts/responsively-preview.mjs',url+'?id=release',path.join(output,'responsively')],{timeout:120000,maxBuffer:1024*1024});console.log(result.stdout);}catch(error){console.log('Responsively unavailable: '+error.message);reports.push({responsively:false,error:error.message});}}
    }finally{await server.close();}
  }
}finally{await browser.close();await writeFile(path.join(output,process.argv.includes('--visual-only')?'report-visual.json':process.argv.includes('--preview-only')?'report-responsively.json':'report.json'),JSON.stringify(reports,null,2));}
console.log('PASS source-page isolation. APIs use only local fixtures; no dist build. Evidence: '+output);
