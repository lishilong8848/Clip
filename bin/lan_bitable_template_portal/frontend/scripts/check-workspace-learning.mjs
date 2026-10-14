import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdir,readFile,readdir,writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {build} from 'vite';
import {parse,compileScript} from '@vue/compiler-sfc';
import {chromium} from 'playwright';
import {adaptLearningPage} from './workspace-learning-adapter.mjs';

const root=fileURLToPath(new URL('..',import.meta.url)),out=path.resolve(root,'../../../output/playwright/workspace-learning');
const oldPath=path.join(root,'src/components/LearningPage.vue'),oldPage=await readFile(oldPath,'utf8');
const adapted=adaptLearningPage(oldPage),parsed=parse(adapted);assert.deepEqual(parsed.errors,[]);
assert.equal(parsed.descriptor.script.content,parse(oldPage).descriptor.script.content,'module-level question/import helpers remain unchanged');
compileScript(parsed.descriptor,{id:'learning-v2',inlineTemplate:true});
assert(adapted.includes('v2Resume(\'daily\')')&&adapted.includes('v2NewPractice'));
const buildCache=path.join(out,'build');await mkdir(buildCache,{recursive:true});
const assets=new Map();
if(process.argv.includes('--reuse-build')){
  for(const name of await readdir(buildCache))assets.set('/assets/workspace-v2/'+name,await readFile(path.join(buildCache,name)));
}else{
  console.log('Building isolated V2 learning fixture');
  const built=await build({root,configFile:path.join(root,'vite.workspace-v2.config.ts'),logLevel:'error',build:{write:false}});
  for(const item of built.output){const value=item.type==='chunk'?item.code:item.source;assert.equal(path.basename(item.fileName),item.fileName);assets.set('/assets/workspace-v2/'+item.fileName,value);await writeFile(path.join(buildCache,item.fileName),value);}
}
const types={'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json','.png':'image/png','.svg':'image/svg+xml'};
const server=createServer(async(req,res)=>{
  const url=new URL(req.url,'http://fixture');let value=assets.get(url.pathname);
  if(value===undefined&&url.pathname.startsWith('/assets/')&&!url.pathname.includes('..'))try{value=await readFile(path.join(root,'public',url.pathname));}catch{}
  if(value===undefined){res.writeHead(404);res.end();return;}res.setHeader('content-type',types[path.extname(url.pathname)]||'application/octet-stream');res.end(value);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port,entry='/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true}),errors=[],requests=[],writes=[];
const day='2026-10-14',person={id:'self-learning',name:'学练验收本人',employee_no:'fixture',scopes:['A'],active:true};
let role='admin',claimed=false,practiceCount=0,gate,releaseGate,auditPage;
const papers=new Map();
function makePaper(id,mode='daily',pid=person.id){return {id,mode,date:day,scope:'A',person_id:pid,person:pid===person.id?person:{id:pid,name:'A楼其他人员',employee_no:'other'},version:0,status:'pending',shortage:{},questions:Array.from({length:15},(_,n)=>({id:id+'-q'+n,question_id:id+'-q'+n,version:'v1',stem:'第'+(n+1)+'题：设备维护前需要核对的安全条件是什么？',bank:'written',type:n===1?'multiple':n===2?'interview':'single',options:(n===1?'ABCD':'AB').split('').map(letter=>({id:letter,text:letter==='A'?'核对现场安全条件':letter==='B'?'直接开始操作':'检查条件'+letter})),has_hint:true,attachments:[],...((mode==='daily'&&n===0)?{attempt:{option_ids:['B'],correct:false,submitted_at:day+'T09:00:00',operation_id:'first-wrong'}}:{})}))};}
papers.set('daily-self',makePaper('daily-self'));
papers.set('daily-other',makePaper('daily-other','daily','other-learning'));
const summary={assigned:15,task_answered:3,answered:3,choice_answered:2,wrong:1,correct:1,accuracy:50,independent_answered:2,independent_correct:1,learning_days:2,practice_count:1,attempt_count:4,interview_total:1,interview_ratings:{'需复习':1},answered_people:24};
const people=Array.from({length:25},(_,n)=>({person_id:'person-'+n,name:'画像人员'+n,employee_no:'fixture-'+n,scope:'A',summary:{...summary,accuracy:n%2?100:50},today:n%3?{answered:3,total:15}:null,last_answered_at:day+'T09:00:00'}));
const profile=pid=>({person:pid?pid===person.id?person:{id:pid,name:'A楼其他人员'}:null,summary:{...summary},today_summary:{...summary,papers:1,answered_people:12,completed_people:4,not_started_people:3},people,trend:[{date:'2026-10-12',attempt_count:2,people:2,accuracy:50},{date:'2026-10-13',attempt_count:0,people:0,accuracy:null},{date:day,attempt_count:4,people:3,accuracy:100}],banks:[{bank:'written',label:'笔试题库',wrong:1}],topics:[{topic:'电气安全',wrong:1}],distribution:[{label:'0-59%',count:13},{label:'60-79%',count:0},{label:'80-99%',count:0},{label:'100%',count:12}],published:true});
const publicPaper=p=>({...structuredClone(p),stats:{total:15,answered:p.questions.filter(q=>q.attempt).length,accuracy:50}});
function reveal(p,q){q.answer={correct_option_ids:q.type==='multiple'?['A','B','C','D']:q.type==='interview'?[]:['A'],answer_text:q.type==='interview'?'先确认安全条件再操作':'',analysis:'按原题参考答案核对',hint:'先核实现场安全条件',attachments:[]};}
try{
  await mkdir(out,{recursive:true});
  const context=await browser.newContext({viewport:{width:1440,height:1000},serviceWorkers:'block'});
  await context.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),p=url.pathname;
    if(url.origin!==origin)return route.abort();
    if(p==='/assets/connection-guard.js')return route.fulfill({contentType:'text/javascript',body:''});
    if(!p.startsWith('/api/'))return route.continue();
    const body=req.method()==='GET'?null:req.postDataJSON();requests.push({p,method:req.method(),body,query:Object.fromEntries(url.searchParams)});
    const ok=data=>route.fulfill({json:{ok:true,data}}),isAdmin=role==='admin',duty=role==='duty';
    if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'learning-fixture-'+role,name:person.name,role:isAdmin?'admin':'member',level:isAdmin?'L0':'L4',modules:['learning']},scope_options:[{value:'A',label:'A楼'}]});
    if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'learning-isolated'}});
    if(p==='/api/learning/bootstrap')return ok({is_admin:isAdmin,can_answer:!duty,can_view_buildings:isAdmin||duty,self_person:duty?null:person,self_scope:duty?'':'A',scope:'A',scopes:isAdmin?['A','B','C','D','E','H'].map(value=>({value,label:value+'楼'})):[{value:'A',label:'A楼'}],today:day,settings:{enabled:true,publish_time:'13:00',night_publish_time:'20:00'},sync:{status:'ready',pending:0},question_problem_count:0});
    if(p==='/api/learning/profile')return ok(profile(url.searchParams.get('person_id')));
    if(p==='/api/learning/people')return ok({items:[{id:'other-learning',name:'A楼其他人员',employee_no:'other',scopes:['A']}],total:1,ready:true,issues:[]});
    if(p==='/api/learning/history')return ok({items:[...papers.values()].filter(row=>row.person_id===url.searchParams.get('person_id')).map(publicPaper),total:[...papers.values()].filter(row=>row.person_id===url.searchParams.get('person_id')).length});
    if(p==='/api/learning/review')return ok({items:[{paper_id:'daily-self',date:day,scope:'A',mistake_count:1,question:publicPaper(papers.get('daily-self')).questions[0]}],total:1});
    if(p==='/api/learning/settings')return ok({enabled:true,publish_time:'13:00',night_publish_time:'20:00',reminder_time:'17:00',timeout_reminder_enabled:true,weekly_report_enabled:true});
    if(p==='/api/learning/papers'){
      const mode=url.searchParams.get('mode')||'daily';
      return ok({today:day,published:true,items:mode==='practice'?[...papers.values()].filter(row=>row.mode==='practice').slice(-1).map(publicPaper):claimed?[publicPaper(papers.get('daily-self'))]:[]});
    }
    if(p==='/api/learning/papers/claim'){
      assert(!duty&&body.person_id===person.id,'only actual login person can claim');writes.push({p,body});
      if(body.mode==='practice'){const paper=makePaper('practice-'+(++practiceCount),'practice');papers.set(paper.id,paper);return ok(publicPaper(paper));}
      claimed=true;return ok(publicPaper(papers.get('daily-self')));
    }
    const match=p.match(/^\/api\/learning\/papers\/([^/]+)(?:\/(answer|reveal|start|notes))?$/);
    if(match){
      const paper=papers.get(match[1]);assert(paper,'known isolated paper');
      if(req.method()==='GET')return ok(publicPaper(paper));
      assert(!duty&&body.person_id===person.id,'other people cannot answer');writes.push({p,body});
      if(match[2]==='start'){paper.started_at=day+'T09:01:00';return ok({started_at:paper.started_at});}
      const q=paper.questions.find(row=>row.id===body.question_id);assert(q,'known question');
      if(match[2]==='answer'){
        if(gate)await gate;
        q.attempt={option_ids:body.option_ids,answer_text:body.answer_text,self_rating:body.self_rating,correct:q.type==='interview'?null:[...(body.option_ids||[])].sort().join('')===(q.type==='multiple'?'ABCD':'A'),submitted_at:day+'T09:02:00',operation_id:body.operation_id};paper.version++;paper.status=paper.questions.every(row=>row.attempt)?'completed':'pending';reveal(paper,q);
      }else if(match[2]==='reveal')reveal(paper,q);
      return ok(publicPaper(paper));
    }
    assert.equal(req.method(),'GET','unexpected write '+p);return ok({items:[],records:[],options:[],links:{}});
  });
  const page=await context.newPage();auditPage=page;page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));page.on('console',event=>{if(event.type()==='error'||event.type()==='warning')console.log('Browser:',event.text());});
  async function go(search){await page.goto(origin+entry+'?__page=/learning&'+search);await page.locator('.learning-page').waitFor();await page.waitForFunction(()=>document.querySelector('.learning-page')?.getAttribute('aria-busy')==='false');}
  async function noOverflow(){assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'page has no horizontal overflow');}
  await go('view=practice');await page.getByRole('button',{name:'继续今日答题',exact:true}).waitFor();
  assert.equal(writes.filter(row=>row.body.mode==='practice').length,0);
  assert.equal(await page.locator('.v2-learning-tabs button').count(),4);
  assert.equal(await page.getByRole('button',{name:'发布设置',exact:true}).count(),0,'management is not a primary tab');
  await page.getByRole('button',{name:'继续今日答题',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.stem')?.textContent.includes('第2题'));
  assert.equal(writes.filter(row=>row.p.endsWith('/claim')).length,1,'continue never generates an extra paper');
  await page.waitForTimeout(300);await page.screenshot({path:path.join(out,'pc-daily.png'),fullPage:true});
  await page.locator('.v2-learning-studybar .n-radio-button').filter({hasText:'自主练习'}).click();await page.getByRole('button',{name:'新练习15题',exact:true}).click();
  await page.locator('.options label').filter({hasText:'直接开始操作'}).click();await page.getByRole('button',{name:'确认作答',exact:true}).click();
  await page.getByText('首次作答：回答错误',{exact:false}).waitFor();
  assert.equal(await page.locator('.answer-actions .result').isVisible(),true,'wrong result remains beside the question');
  await page.getByRole('button',{name:'下一题',exact:true}).click();
  for(const letter of ['D','C','B','A'])await page.locator('.options label').filter({has:page.locator('input[value="'+letter+'"]')}).click();
  gate=new Promise(resolve=>releaseGate=resolve);await page.getByRole('button',{name:'确认作答',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.learning-page').getAttribute('aria-busy')==='true');
  assert.equal(await page.getByRole('button',{name:'下一题',exact:true}).isDisabled(),true);releaseGate();gate=null;
  await page.getByText('首次作答：回答正确',{exact:false}).waitFor();
  assert.deepEqual(writes.findLast(row=>row.p.endsWith('/answer')).body.option_ids,['D','C','B','A']);
  await page.getByRole('button',{name:'新练习15题',exact:true}).click();await page.getByRole('dialog').filter({hasText:'开始新练习'}).getByRole('button',{name:'取消',exact:true}).click();assert.equal(practiceCount,1,'cancel keeps the unfinished practice');
  await page.locator('.v2-learning-tabs').getByRole('button',{name:'个人画像',exact:true}).click();
  await page.locator('.v2-learning-metrics').waitFor();await page.locator('.v2-learning-trend>div:first-child svg').waitFor();
  const selectedTab=page.locator('.v2-learning-tabs .n-button--primary-type');await selectedTab.hover();assert.equal(await selectedTab.evaluate(node=>getComputedStyle(node).backgroundColor),'rgb(233, 242, 255)','selected tab remains readable on hover');
  assert.equal(await page.locator('.profile-toolbar .n-date-picker').count(),0,'preset period does not show redundant date inputs');
  await page.waitForTimeout(1100);await page.screenshot({path:path.join(out,'pc-personal-portrait.png'),fullPage:true});
  await page.locator('.v2-learning-chart-head .n-select').click();await page.locator('.n-base-select-option').filter({hasText:'错题分类对比'}).click();
  await page.locator('.v2-chart-data summary').click();await page.locator('.v2-chart-details').getByRole('button',{name:/笔试题库/}).click();
  await page.waitForFunction(()=>document.querySelector('.learning-page').dataset.v2LearningTab==='review');
  assert(requests.some(row=>row.p==='/api/learning/review'&&row.query.bank==='written'));
  await page.locator('.v2-learning-tabs').getByRole('button',{name:'个人画像',exact:true}).click();
  await page.locator('.v2-learning-trend').waitFor();await page.locator('.v2-learning-trend-values summary').click();
  await page.getByRole('button',{name:'2026-10-12 2 次',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.learning-page').dataset.v2LearningTab==='history');
  assert(requests.some(row=>row.p==='/api/learning/history'&&row.query.from==='2026-10-12'&&row.query.to==='2026-10-12'));
  await page.setViewportSize({width:390,height:844});await go('view=practice');await noOverflow();
  assert.equal(await page.locator('.v2-learning-rail').getAttribute('open'),null,'mobile answer card is collapsed');
  const bar=await page.locator('.v2-learning-answerbar').boundingBox();assert(bar.y>600&&bar.y+bar.height<810,'answer controls remain above mobile navigation');
  assert.equal(await page.locator('.v2-learning-answerbar .n-button--primary-type').evaluate(node=>getComputedStyle(node).color),'rgb(255, 255, 255)','mobile submit has a distinct primary treatment');
  await page.locator('.v2-learning-rail summary').click();await page.locator('.question-numbers button').first().waitFor();
  const target=await page.locator('.question-numbers button').first().boundingBox();assert(target.width>=44&&target.height>=44);
  await page.locator('.v2-learning-rail summary').click();await page.waitForFunction(()=>document.querySelector('.v2-learning-rail').getBoundingClientRect().height<65);await page.waitForTimeout(400);await page.screenshot({path:path.join(out,'mobile-answer.png'),fullPage:true});
  const current=papers.get('practice-'+practiceCount);for(const q of current.questions){q.attempt||={correct:q.type==='interview'?null:true,option_ids:['A'],self_rating:'需复习',submitted_at:day+'T09:10:00'};reveal(current,q);}current.status='completed';
  await go('view=practice');await page.locator('.v2-learning-studybar .n-radio-button').filter({hasText:'自主练习'}).click();await page.locator('.v2-learning-completion').waitFor();
  await page.screenshot({path:path.join(out,'mobile-completion.png'),fullPage:true});
  await go('view=overview&scope=A');await page.locator('.v2-learning-person-list article').first().waitFor();await noOverflow();
  assert.equal(await page.locator('.v2-learning-people .n-data-table').count(),0,'mobile does not render the wide table');
  await page.screenshot({path:path.join(out,'mobile-building-portrait.png'),fullPage:true});
  for(const width of [360,430]){await page.setViewportSize({width,height:844});await noOverflow();}
  await page.locator('.profile-toolbar .n-radio-button').filter({hasText:'自定义'}).click();await page.getByLabel('统计开始日期',{exact:true}).click();await page.locator('.n-date-panel').waitFor();
  const calendar=await page.locator('.n-date-panel').boundingBox();assert(calendar.x>=-1&&calendar.x+calendar.width<=430+1,'mobile calendar fits its screen');await page.keyboard.press('Escape');
  await page.setViewportSize({width:1366,height:900});await go('view=overview&scope=A');await noOverflow();
  await page.setViewportSize({width:1920,height:1080});await noOverflow();
  await page.getByRole('button',{name:'学练管理',exact:true}).click();await page.locator('.n-dropdown-option').filter({hasText:'发布设置'}).click();await page.locator('.settings-form .n-time-picker').first().waitFor();assert.equal(await page.locator('.settings-form .n-time-picker').count(),2,'day and night publication use the same mature picker');
  const writesBefore=writes.length;await go('view=overview&scope=A&person_id=other-learning');
  await page.locator('.v2-learning-tabs').getByRole('button',{name:'错题',exact:true}).click();
  assert.equal(writes.length,writesBefore,'viewing another person is read-only');
  role='personal';await go('view=practice');assert.equal(await page.getByRole('radio',{name:'楼栋画像',exact:true}).count(),0);
  role='duty';await go('view=overview&scope=A');assert.equal(await page.getByRole('radio',{name:'我的答题',exact:true}).count(),0);assert.equal(await page.locator('.v2-learning-studybar').count(),0);
  await noOverflow();assert.deepEqual(errors,[]);assert.equal(await readFile(oldPath,'utf8'),oldPage,'legacy source is unchanged');
  console.log('PASS: V2 in-memory production build; daily/practice actions, saved progress, busy guard, unordered multi-select, inline grading, chart/date drill-down, compact mobile answer card/footer, completion, mobile roster, read-only person view and role gates. No real business writes.');
}catch(error){
  console.error('Fixture page errors:',errors);console.error('Learning reads:',requests.filter(row=>row.p.startsWith('/api/learning')));if(auditPage){console.error((await auditPage.locator('.learning-page').evaluateAll(nodes=>nodes.map(node=>node.outerHTML))).join('').slice(0,5000));console.error((await auditPage.locator('body').innerText()).slice(0,1500));await auditPage.screenshot({path:path.join(out,'failed.png'),fullPage:true});}throw error;
}finally{releaseGate?.();await browser.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
