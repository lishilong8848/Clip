import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {auditUrl} from './responsive-audit-pages.mjs';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/responsive-audit');
const version=process.env.COLLAB_VERSION||'legacy';
assert.ok(['legacy','v2'].includes(version),'COLLAB_VERSION must be legacy or v2');
assert.notEqual(process.env.COLLAB_CSS_PREVIEW,'1','Final dist acceptance forbids CSS injection');
const phase=process.env.COLLAB_PHASE||'final-built';
const folder=path.join(output,'continued-collab',phase,version);
const reportPath=path.join(output,`continued-collab-${phase}-${version}.json`);
const origin='http://127.0.0.1:14536';
const selected=process.argv.slice(2);
const sizes=[{width:402,height:874},{width:440,height:956},{width:1440,height:900}].filter(s=>!process.env.COLLAB_WIDTH||s.width===Number(process.env.COLLAB_WIDTH));
assert.ok(sizes.length,'COLLAB_WIDTH must be 402, 440 or 1440');
const hash=value=>createHash('sha256').update(value).digest('hex');
async function buildStamp(){
  const [entry,manifest]=await Promise.all([readFile(path.join(root,'dist/index.html')),readFile(path.join(root,'dist/assets/workspace-v2/entry.json'))]);
  return {legacyEntrySha256:hash(entry),workspaceManifestSha256:hash(manifest),buildSha256:hash(Buffer.concat([entry,manifest]))};
}
function logoAssertions(g){
  const overlaps=(a,b)=>Math.min(a.right,b.right)-Math.max(a.left,b.left)>1&&Math.min(a.bottom,b.bottom)-Math.max(a.top,b.top)>1;
  return [
    {label:'Logo image loaded',pass:g.complete&&g.naturalWidth>0&&g.naturalHeight>0},
    {label:'Logo height is 7mm',pass:Math.abs(g.logo.height-7*96/25.4*g.scale)<1},
    {label:'Logo top offset is 2mm',pass:Math.abs(g.logo.top-g.sheet.top-2*96/25.4*g.scale)<1},
    {label:'Logo stays inside title row',pass:g.logo.top>=g.title.top-1&&g.logo.bottom<=g.title.bottom+1&&g.logo.left>=g.title.left-1&&g.logo.right<=g.title.right+1},
    {label:'Logo does not overlap date row',pass:!overlaps(g.logo,g.weather)},
    {label:'Logo does not overlap title text',pass:!overlaps(g.logo,g.titleText)},
  ];
}
if(selected.includes('--self-test')){
  const logical='/daily-tasks?scope=H';
  assert.equal(new URL(auditUrl(origin,'legacy',logical)).searchParams.get('frontend'),'legacy');
  const v2=new URL(auditUrl(origin,'v2',logical));
  assert.equal(v2.pathname,'/assets/workspace-v2/index.html');assert.equal(v2.searchParams.get('__page'),'/daily-tasks');assert.equal(v2.searchParams.get('scope'),'H');
  const good={complete:true,naturalWidth:200,naturalHeight:80,scale:1,sheet:{top:0},logo:{top:2*96/25.4,bottom:9*96/25.4,left:20,right:269,height:7*96/25.4},title:{top:0,bottom:11*96/25.4,left:0,right:1047},weather:{top:11*96/25.4,bottom:80,left:0,right:1047},titleText:{top:10,bottom:30,left:450,right:650}};
  assert.ok(logoAssertions(good).every(c=>c.pass));
  assert.ok(logoAssertions({...good,logo:{...good.logo,bottom:60,height:52}}).some(c=>!c.pass));
  assert.ok(!logoAssertions({...good,complete:false})[0].pass);
  console.log('PASS legacy/v2 URL selection and logo geometry positive/negative cases; no browser or service started.');
  process.exit(0);
}
const health=await (await fetch(origin+'/api/health')).json();
assert.equal(health.instance_id,'responsive-isolated-preview');
await mkdir(folder,{recursive:true});
const stamp=await buildStamp();
const entryPath=version==='v2'?'assets/workspace-v2/index.html':'index.html';
const entry=await readFile(path.join(root,'dist',entryPath));
const servedEntry=await fetch(origin+'/'+entryPath);assert.equal(servedEntry.status,200);
assert.equal(hash(Buffer.from(await servedEntry.arrayBuffer())),hash(entry),'served HTML must match the selected actual dist file');
const report={phase,tool:'project Playwright Chromium',origin,version,viewports:sizes,
  distEntrySha256:hash(entry),buildBefore:stamp,servedEntryMatchesDist:true,startedAt:new Date().toISOString(),
  constraints:{buildStarted:false,responsivelyControlled:false,productionAccess:false,messagesSent:false,cssInjected:false},
  evidence:[],errors:[],blockedNetwork:[],writes:[],unverified:[],findings:[]};
report.cssPreview=false;
const browser=await chromium.launch({headless:true});
let page,current,step;
const button=(name,scope=page)=>scope.getByRole('button',{name,exact:true}).filter({visible:true}).first();
const click=async locator=>{await locator.click({timeout:4500});await page.waitForTimeout(230);};
const open=async url=>{
  await page.goto(auditUrl(origin,version,url));await page.waitForTimeout(400);
  assert.equal(await page.locator('html.workspace-v2').count(),version==='v2'?1:0,'selected frontend theme must match COLLAB_VERSION');
};
async function check(locator,label){
  const item={label};
  try{
    await locator.scrollIntoViewIfNeeded({timeout:3000});
    const geometry=await locator.evaluate(el=>{
      const r=el.getBoundingClientRect(),s=getComputedStyle(el);
      const hit=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
      return {rect:{x:r.x,y:r.y,width:r.width,height:r.height},inViewport:r.x>=-1&&r.right<=innerWidth+1&&r.y>=-1&&r.bottom<=innerHeight+1,
        hit:!!hit&&(el.contains(hit)||hit.contains(el)),occludedBy:hit?.className||hit?.tagName,disabled:!!el.disabled,display:s.display};
    });
    Object.assign(item,geometry);
    if(!geometry.disabled){await locator.click({trial:true,timeout:2500});item.clickable=true;}
    else item.clickable='disabled by existing business condition';
    item.pass=geometry.inViewport&&(geometry.disabled||geometry.hit&&item.clickable===true);
  }catch(error){item.pass=false;item.error=error.message.slice(0,1000);}
  return item;
}
async function capture(state,controls=[],scope='body'){
  step=state;
  const checks=[];
  for(const [label,locator] of controls)checks.push(await check(locator,label));
  await page.waitForTimeout(250);
  const screenshot=path.join(folder,`${current.id}-${current.width}-${state}.png`);
  await page.screenshot({path:screenshot});
  const metrics=await page.locator(scope).last().evaluate(el=>{
    const rect=e=>{const r=e.getBoundingClientRect();return {x:Math.round(r.x),y:Math.round(r.y),width:Math.round(r.width),height:Math.round(r.height)};};
    const visible=e=>{const s=getComputedStyle(e),r=e.getBoundingClientRect();return s.visibility!=='hidden'&&s.display!=='none'&&r.width&&r.height;};
    const scrollers=[el,...el.querySelectorAll('*')].filter(e=>visible(e)&&e.scrollWidth>e.clientWidth+2&&/auto|scroll/.test(getComputedStyle(e).overflowX));
    const overflow=[el,...el.querySelectorAll('button,input,select,textarea,h1,h2,h3,strong,.n-tag,[role=combobox]')].filter(e=>{
      if(!visible(e))return false;
      const r=e.getBoundingClientRect();if(r.bottom<0||r.top>innerHeight||r.left>=-2&&r.right<=innerWidth+2)return false;
      let a=e.parentElement;while(a&&a!==document.body){if(scrollers.includes(a))return false;a=a.parentElement;}return true;
    });
    return {viewport:{width:innerWidth,height:innerHeight},documentWidth:document.documentElement.scrollWidth,scope:rect(el),
      overflow:overflow.map(e=>({tag:e.tagName,class:e.className,text:(e.innerText||e.getAttribute('aria-label')||'').slice(0,120),...rect(e)})),
      horizontalScrollers:scrollers.map(e=>({class:e.className,clientWidth:e.clientWidth,scrollWidth:e.scrollWidth,...rect(e)})),
      layers:[...document.querySelectorAll('.template-editor-overlay,.confirm-backdrop,.n-modal,.n-drawer')].map(e=>({class:e.className,zIndex:getComputedStyle(e).zIndex,...rect(e)})),
      text:el.innerText.replaceAll(/内部资料 · 布局验收管理员 · [^\n]+\n?/g,'').slice(0,14000)};
  });
  const row={...current,state,url:page.url(),screenshot:path.relative(output,screenshot).replaceAll('\\','/'),checks,metrics,
    status:checks.some(c=>!c.pass)||metrics.overflow.length?'needs-review':'checked'};
  report.evidence.push(row);
  await writeReport();
  console.log(JSON.stringify({page:current.id,width:current.width,state,status:row.status,failed:checks.filter(c=>!c.pass).map(c=>c.label),overflow:metrics.overflow.map(e=>e.text)}));
}
async function writeReport(){await writeFile(reportPath,JSON.stringify(report,null,2));}
async function modalCancel(){await click(button('取消',page.locator('.n-modal').last()));}
const scenarios={
  risk:async()=>{
    await open('/risk-inspections?scope=A');
    await click(button('查看反馈'));
    await click(page.locator('.risk-feedback-table button').filter({hasText:'填写'}).filter({visible:true}).first());
    await capture('feedback-drawer',[['提交审核',button('提交审核')],['保存草稿',button('保存草稿')]],'.n-drawer');
    await click(button('提交审核'));
    await capture('required-validation',[['必填字段定位',button('排查结果')]],'.n-drawer');
    await page.getByRole('textbox',{name:'排查结果',exact:true}).fill('仅浏览器布局验收，不提交。');
    await click(button('提交审核'));
    await capture('feedback-submit-confirm',[['取消',button('取消',page.locator('.n-modal'))],['确认',button('确认',page.locator('.n-modal'))]],'.n-modal');
    await modalCancel();
    await open('/risk-inspections?scope=A');
    await click(button('查看反馈'));
    await click(button('更换审核人'));
    await capture('reviewer-confirm',[['取消',button('取消')],['审核人',page.locator('.n-modal .n-select')]],'.n-modal');
    await modalCancel();
    await open('/risk-inspections?scope=A');
    await click(button('基础模板'));
    if(version==='v2')await click(button('新建基础模板'));
    await click(button('添加字段'));
    await capture('field-editor',[['字段名称',page.getByRole('textbox',{name:'字段库名称',exact:true})],['保存字段',button('保存字段')],['取消',button('取消')]],'.n-modal');
    await modalCancel();
    await capture('base-template-bottom',[['生成基础模板',button('生成基础模板')]]);
    await open('/risk-inspections?scope=A');
    await click(button('历史模板库'));
    await click(button('查看 / 编辑'));
    await capture('history-editor',[['保存新版本',button('保存新版本')]]);
    await open('/risk-inspections?scope=A');
    await click(button('发布排查'));
    await click(button('选用并填写'));
    await capture('publish-form',[['接收人员',page.locator('.risk-publish [aria-label="接收人员"]')]]);
    await capture('publish-footer',[['确认发布',button('确认发布')]]);
  },
  guard:async()=>{
    await open('/critical-guard?scope=A');
    await capture('fill-bottom',[['保存',button('保存')],['生成图片',button('生成图片')]]);
    const wrap=page.locator('.check-table-wrap');
    if(await wrap.count())await wrap.evaluate(el=>{el.scrollLeft=el.scrollWidth;});
    await capture('table-right',[['末行备注',page.locator('.check-table input').last()]]);
    await click(button('编辑模板'));
    await capture('template-editor',[['添加一行',button('添加一行')],['取消',button('取消')]],'.template-editor-dialog');
    await click(button('添加一行'));
    await page.locator('.template-editor-row').last().locator('textarea').first().fill('布局验收新增行');
    await click(button('取消'));
    await capture('template-discard',[['取消',button('取消',page.locator('.confirm-modal'))],['放弃修改',button('放弃修改')]],'.confirm-modal');
    await click(button('放弃修改'));
  },
  daily:async()=>{
    await open('/daily-tasks?scope=A');
    await click(button('发送今日工作至飞书'));
    await click(page.locator('#daily-report-recipients'));
    await capture('send-people-menu',[['人员选项',page.getByRole('option').first()]],'.send-dialog');
    await click(page.getByRole('option').first());
    await page.keyboard.press('Escape');
    assert.ok(await button('确认发送').isEnabled(),'selected recipient enables confirmation');
    await capture('send-footer',[['确认发送',button('确认发送')],['取消',button('取消')]],'.send-dialog');
    await click(button('取消'));
    await open('/daily-tasks?scope=H');
    await click(button('生成晨会表格'));
    await capture('morning-preview',[['生成表格',button('生成表格')],['关闭',button('关闭')]],'.morning-dialog');
    const logoGeometry=await page.locator('.morning-sheet').evaluate(sheet=>{
      const rect=e=>{const r=e.getBoundingClientRect();return {top:r.top,bottom:r.bottom,left:r.left,right:r.right,width:r.width,height:r.height};};
      const logo=sheet.querySelector('.morning-logo'),title=sheet.querySelector('.title-row'),weather=sheet.querySelector('.weather-row');
      const range=document.createRange();range.selectNodeContents(title.querySelector('th'));
      const titleText=range.getBoundingClientRect();
      return {sheet:rect(sheet),scale:sheet.getBoundingClientRect().width/sheet.offsetWidth,logo:rect(logo),title:rect(title),weather:rect(weather),titleText:{top:titleText.top,bottom:titleText.bottom,left:titleText.left,right:titleText.right},complete:logo.complete,naturalWidth:logo.naturalWidth,naturalHeight:logo.naturalHeight};
    });
    const assertions=logoAssertions(logoGeometry),last=report.evidence.at(-1);
    Object.assign(last,{logoGeometry,assertions});
    if(assertions.some(c=>!c.pass)){last.status='needs-review';report.findings.push({...current,state:'morning-preview',problem:'Morning logo geometry failed',assertions});}
    await writeReport();assert.ok(assertions.every(c=>c.pass),JSON.stringify(assertions));
    await click(button('关闭'));
  },
  drills:async()=>{
    await open('/drill-management?scope=A');
    await capture('record-people',[['指挥人',page.locator('#drill-commander')],['展开人员',page.locator('.people-filter-actions button')]]);
    const badge=await page.locator('.execution-header>.status-chip').evaluate(el=>{
      const range=document.createRange();range.selectNodeContents(el);const rects=[...range.getClientRects()];
      return {text:el.textContent,lines:new Set(rects.map(r=>Math.round(r.top))).size,whiteSpace:getComputedStyle(el).whiteSpace,flexShrink:getComputedStyle(el).flexShrink};
    });
    report.evidence.at(-1).statusBadge=badge;
    if(badge.lines!==1){report.evidence.at(-1).status='needs-review';report.findings.push({...current,state:'record-people',problem:'演练执行状态标签竖排',badge});}
    await click(page.locator('.people-filter-actions button'));
    await capture('record-steps',[['步骤执行人',page.locator('#drill-step-10-1')]]);
    await capture('record-footer',[['保存草稿',button('保存草稿')],['生成并归档',button('生成并归档')]]);
    await click(button('演练评估表'));
    await capture('assessment',[['评估人',page.locator('#drill-evaluator')],['评估时间',page.locator('input[type=datetime-local]').last()]]);
    await capture('assessment-footer',[['下载文件',button('下载文件')],['保存草稿',button('保存草稿')]]);
  },
  learning:async()=>{
    await open('/learning?scope=A');
    await click(button('题目有疑问'));
    await capture('question-issue',[['关闭窗口',button('关闭窗口')]],'.learning-modal');
    await click(button('关闭窗口'));
    await click(button('下一题'));
    await capture('next-question',[['上一题',button('上一题')],['确认作答',button('确认作答')]]);
    await click(button('错题与复习'));
    await capture('review',[['学习历史',button('学习历史')]]);
    await click(button('个人画像'));
    await capture('profile',[]);
    await click(button('分享会练习'));
    await capture('sharing-list',[]);
    await click(page.locator('.sharing-batch').first());
    await capture('sharing-detail',[['核对后发布给参会人',button('核对后发布给参会人')]]);
    await click(button('核对后发布给参会人'));
    await capture('sharing-publish-confirm',[['取消',button('取消',page.locator('.confirm-modal'))]],'.confirm-modal');
    await click(button('取消',page.locator('.confirm-modal')));
    await click(button('批次列表'));
    await click(button('题库管理'));
    await click(button('新增题目'));
    await capture('question-editor',[['关闭窗口',button('关闭窗口')],['保存题目',button('保存题目')]],'.learning-modal');
  },
  knowledge:async()=>{
    await open('/knowledge-base');
    await click(page.locator('.kb-row-main').first());
    await capture('document-detail',[['关闭预览',button('关闭预览')]],'.kb-detail');
    await click(button('关闭预览'));
    await click(page.getByRole('checkbox',{name:'全选本页',exact:true}));
    await capture('move-toolbar',[]);
    await click(page.locator('.kb-group-picker-trigger'));
    await click(page.locator('.kb-group-picker-dialog').getByText('交接与培训',{exact:true}));
    await capture('group-picker',[['选择此分组',button('选择此分组')],['取消',button('取消')]],'.kb-group-picker-dialog');
    await click(button('选择此分组'));
    await capture('move-ready',[['应用分组',button('应用分组')]],'.kb-batch-bar');
    await click(button('上传文件'));
    await click(button('粘贴文本'));
    await page.getByRole('textbox',{name:'文档名称',exact:true}).fill('仅浏览器本地验收');
    await page.getByRole('textbox',{name:'文档内容',exact:true}).fill('布局验证文本，不提交上传。');
    await capture('text-upload',[['加入待上传',button('加入待上传')],['确认上传',button('确认上传')]],'.kb-upload');
    await click(button('加入待上传'));
    await click(button('确认上传'));
    await capture('upload-confirm',[['取消',button('取消',page.locator('.confirm-modal'))],['确认',button('确认',page.locator('.confirm-modal'))]],'.confirm-modal');
    await click(button('取消',page.locator('.confirm-modal')));
    await click(button('关闭上传'));
  },
  links:async()=>{
    await open('/link-directory');
    await click(button('下一页'));
    await capture('page-two',[['编辑链接',page.getByTitle('编辑链接').first()]]);
    await click(page.getByTitle('编辑链接').first());
    await capture('edit-modal',[['取消',button('取消')]],'[role=dialog]');
    await click(button('取消'));
    await click(button('新增链接'));
    await capture('new-modal',[['取消',button('取消')]],'[role=dialog]');
    await click(button('取消'));
    await capture('sort-control',[['排序控件',page.getByTitle('拖动调整顺序；悬停分页按钮可跨页；也可按上下方向键').first()]]);
  },
  signatures:async()=>{
    await open('/signature-management');
    await click(button('查看签名'));
    await capture('preview',[['关闭',page.locator('dialog footer').getByRole('button',{name:'关闭',exact:true})]],'dialog[open]');
    await click(button('重新签名',page.locator('dialog')));
    await capture('invite-relay',[['代收搜索',page.locator('dialog input[type=search]')],['取消',button('取消',page.locator('dialog'))]],'dialog[open]');
    await click(button('取消',page.locator('dialog')));
    const sessionResponse=await page.request.post(origin+'/api/signatures/management/self-start',{data:{operation_id:`collab-layout-${phase}-${version}-${current.width}`}});
    assert.equal(sessionResponse.status(),200);
    const session=await sessionResponse.json();assert.ok(session.data.link_url.startsWith('/signature?request_id='));
    report.writes.push({...current,method:'POST',path:'/api/signatures/management/self-start',purpose:'existing in-memory fixture only; creates no message'});
    await open(session.data.link_url);
    await capture('handwriting',[['画布',page.locator('canvas[aria-label="手写签名区域"]')],['清空',button('清空')]]);
    const canvas=page.locator('canvas[aria-label="手写签名区域"]');
    await canvas.scrollIntoViewIfNeeded();
    const box=await canvas.boundingBox();
    await page.mouse.move(box.x+40,box.y+80);await page.mouse.down();
    await page.mouse.move(box.x+120,box.y+110,{steps:8});await page.mouse.up();
    const ink=await canvas.evaluate(el=>{const data=el.getContext('2d').getImageData(0,0,el.width,el.height).data;let n=0;for(let i=3;i<data.length;i+=4)if(data[i])n++;return n;});
    assert.ok(ink>20,'handwriting canvas receives pointer strokes');
    await page.locator('.signature-action-hint input[type=checkbox]').check();
    assert.ok(await button('保存到签名表').isEnabled(),'ink plus local fixture consent enables save');
    await capture('handwriting-ink',[['清空',button('清空')],['保存到签名表',button('保存到签名表')]]);
    report.evidence.at(-1).canvasInkPixels=ink;
    await click(button('清空'));
    await open('/signature-management');
    await click(button('新增临时人员'));
    await capture('create-person',[['取消',button('取消',page.locator('dialog'))]],'dialog[open]');
    await click(button('取消',page.locator('dialog')));
    await click(button('重复人员核对'));
    await capture('duplicates',[['收起',button('收起')]]);
    for(const check of await page.locator('.duplicates input[type=checkbox]').all())await check.check();
    await click(button('备份并合并清理'));
    await capture('merge-confirm',[['取消',button('取消',page.locator('.confirm-modal'))],['确认合并',button('确认合并')]],'.confirm-modal');
    await click(button('取消',page.locator('.confirm-modal')));
    await click(button('关联正式人员'));
    await capture('associate',[['取消',button('取消',page.locator('dialog'))]],'dialog[open]');
    await click(button('取消',page.locator('dialog')));
  },
  handover:async()=>{
    await open('/?entry=handover');
    await capture('building-links',[]);
    report.unverified.push({...current,state:'审核详情',reason:'入口跳转外部交接表，禁止将外链/占位页面当作审核详情通过'});
  },
  'work-handover':async()=>{
    await open('/work-handover?scope=A');
    await click(page.locator('.handover-list-row').first());
    await capture('draft-footer',[['保存草稿',button('保存草稿')],['发送交接',button('发送交接')]]);
    await click(button('添加事项'));
    await capture('item-editor',[['加入清单',button('加入清单')]],'.handover-modal');
    await click(page.locator('.handover-modal .n-base-close'));
    await click(button('从业务添加'));
    await capture('source-picker',[['读取业务',button('读取业务')]],'.handover-source-modal');
    await click(button('读取业务'));
    await capture('source-results',[['添加已选',page.locator('.handover-source-modal .n-card__footer button')]],'.handover-source-modal');
    await click(page.locator('.handover-source-modal .n-base-close'));
    await click(button('发送交接'));
    await capture('send-confirm',[['取消',button('取消')]],'.n-modal');
    await modalCancel();
    if(await button('交接列表').isVisible())await click(button('交接列表'));
    await click(page.locator('.handover-list-row').nth(1));
    await click(button('退回补充'));
    await capture('return-confirm',[['取消',button('取消')]],'.n-modal');
  },
};
try{
  for(const size of sizes){
    const context=await browser.newContext({viewport:size,reducedMotion:'reduce'});
    await context.route('**/*',async route=>{
      const req=route.request(),url=new URL(req.url());
      if(url.origin!==origin){report.blockedNetwork.push({url:req.url(),reason:'outside isolated origin'});return route.abort();}
      if(!['GET','HEAD'].includes(req.method())){
        const row={page:current?.id,width:size.width,method:req.method(),path:url.pathname};
        report.writes.push(row);
        if(/\/send$|\/invites|\/request$|\/publish$/.test(url.pathname)){
          report.blockedNetwork.push({...row,reason:'messages/publication excluded'});
          return route.fulfill({status:409,json:{ok:false,error:'本次布局验收不发送或发布。'}});
        }
      }
      return route.continue();
    });
    page=await context.newPage();page.setDefaultTimeout(5000);
    page.on('pageerror',e=>report.errors.push({...current,state:step,kind:'pageerror',error:e.message}));
    page.on('response',res=>{if(res.status()>=400&&res.url().includes('/api/'))report.errors.push({...current,state:step,kind:'api',status:res.status(),url:res.url()});});
    for(const [id,run] of Object.entries(scenarios)){
      if(selected.length&&!selected.includes(id))continue;
      if(version==='v2'&&!selected.length&&!['risk','guard','drills','daily'].includes(id))continue;
      current={id,version,...size};step='entry';
      try{await run();}catch(error){
        report.errors.push({...current,state:step,kind:'flow',error:error.message.slice(0,1800)});
        await capture('flow-error').catch(()=>{});
      }
    }
    await context.close();
  }
}finally{
  await browser.close();report.finishedAt=new Date().toISOString();
  report.distEntrySha256After=hash(await readFile(path.join(root,'dist',entryPath)));
  report.buildAfter=await buildStamp();
  report.summary={states:report.evidence.length,checks:report.evidence.reduce((n,e)=>n+e.checks.length,0),
    failedChecks:report.evidence.flatMap(e=>e.checks).filter(c=>!c.pass).length,
    apiErrors:report.errors.filter(e=>e.kind==='api').length,flowErrors:report.errors.filter(e=>e.kind==='flow').length,
    geometryAssertions:report.evidence.flatMap(e=>e.assertions||[]).length,
    failedGeometryAssertions:report.evidence.flatMap(e=>e.assertions||[]).filter(c=>!c.pass).length,
    sourceCssOnlyPreview:false,distEntryStable:report.distEntrySha256===report.distEntrySha256After,
    buildStable:report.buildBefore.buildSha256===report.buildAfter.buildSha256};
  await writeReport();
}
if(report.errors.length||report.summary.failedChecks||report.summary.failedGeometryAssertions||report.findings.length||!report.summary.buildStable)process.exitCode=1;
console.log('COLLAB_REPORT',reportPath);
