import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

// Serve the real risk components through the V2 source adapters, without building dist.
const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright',process.env.CLIPFLOW_RISK_FIELDS_OUTPUT||'risk-fields');
await mkdir(output,{recursive:true});
const reports=[],browser=await chromium.launch({headless:true});
const built=process.argv.includes('--dist');
const fixed=[
  {key:'primary',field_id:'primary',field_name:'任务编号',role:'number',type:1,is_primary:true},
  ...['status','scope','person','reviewer','review','reviewed_at','sync','inspected_at'].map((role,index)=>({key:role,field_id:role,field_name:['排查进度','楼栋','排查人','复核人','复核结果','复核日期','最终推送','排查日期'][index],role,type:['reviewed_at','inspected_at'].includes(role)?5:1})),
];
const inputs=[
  {key:'result',field_id:'result',field_name:'排查结果',role:'input',type:1,required:true,property:{}},
  {key:'level',field_id:'level',field_name:'风险等级',role:'input',type:3,required:false,property:{options:[{name:'正常'},{name:'异常'}]}},
  ...Array.from({length:30},(_,i)=>({key:'field_'+i,field_id:'field_'+i,field_name:'设备检查 '+(i+1),role:'input',type:1,property:{}})),
];
async function inspectViewport(page,tag){
  await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  await page.screenshot({path:path.join(output,tag+'-viewport.png'),animations:'disabled'});
  const evidence=await page.locator('.risk-v2-field-modal').evaluate(el=>{
    const rect=node=>{const r=node.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height,bottom:r.bottom,right:r.right};};
    const scrolling=node=>/auto|scroll/.test(getComputedStyle(node).overflowY)&&node.scrollHeight>node.clientHeight+1;
    const content=el.querySelector(':scope>.n-card-content,:scope>.n-card__content');
    const save=[...el.querySelectorAll('button')].find(node=>node.textContent.includes('保存字段'));
    const button=rect(save),v=window.visualViewport;
    const parents=[];for(let node=el.parentElement;node;node=node.parentElement)if(scrolling(node))parents.push(node.className);
    return {innerHeight,viewport:{height:v.height,width:v.width,top:v.offsetTop,left:v.offsetLeft,scale:v.scale},modal:{...rect(el),css:{position:getComputedStyle(el).position,maxHeight:getComputedStyle(el).maxHeight,top:getComputedStyle(el).top,translate:getComputedStyle(el).translate,style:el.getAttribute('style')}},content:{className:content?.className,overflow:content&&getComputedStyle(content).overflowY,clientHeight:content?.clientHeight,scrollHeight:content?.scrollHeight},scrollers:[el,...el.querySelectorAll('*')].filter(scrolling).map(node=>node.className),scrollingParents:parents,button,buttonVisible:button.y>=v.offsetTop&&button.bottom<=v.offsetTop+v.height&&button.x>=v.offsetLeft&&button.right<=v.offsetLeft+v.width,buttonHit:document.elementFromPoint(button.x+button.width/2,button.y+button.height/2)?.closest('button')===save};
  });
  await writeFile(path.join(output,tag+'-dom.json'),JSON.stringify(evidence,null,2));return evidence;
}
function verifyViewport(evidence){
  assert.equal(evidence.content.className,'n-card-content','Use the installed Naive UI DOM class, not an assumed selector');
  assert.equal(evidence.content.overflow,'auto');
  assert.deepEqual(evidence.scrollers.filter(name=>name!=='n-input__textarea-el'),['n-card-content'],'Only one modal body scrolls; native textarea scrolling is allowed');
  assert.deepEqual(evidence.scrollingParents,[],'The Naive modal wrapper must not add a second scrollbar');
  assert(evidence.buttonVisible&&evidence.buttonHit,'Save is fully inside the visible viewport and can receive a click');
}
async function verifyInlineFieldError(page,modal,text){
  await modal.getByText(text,{exact:true}).waitFor();
  assert(await modal.evaluate(el=>el.hasAttribute('data-v2-inline-feedback')),'Teleported risk modal retains its inline-feedback boundary');
  // Let the real feedback observer complete its 50ms scan before testing pointer access.
  await page.waitForTimeout(120);
  assert.equal(await modal.locator('.n-alert[popover],.n-alert[data-v2-feedback]').count(),0,'Field errors must not be promoted into top-layer toasts');
  assert(await modal.getByText(text,{exact:true}).evaluate(el=>!!el.closest('.n-card-content')),'The error stays in the actual modal body');
}
function harness(version){
  const entry='/src/__risk_fields_test.ts';
  return {name:'risk-offline-harness',
    resolveId(id){if(id===entry)return path.join(root,entry.slice(1));},
    load(id){if(id.replaceAll('\\','/')!==path.join(root,entry.slice(1)).replaceAll('\\','/'))return;
      return `import {createApp,h} from 'vue';
import {NConfigProvider,darkTheme,zhCN,dateZhCN} from 'naive-ui';
import RiskInspectionPage from '/src/components/RiskInspectionPage.vue';
import {theme,nightTheme} from '/src/workspace-v2/theme.ts';
${version==='v2'?"import {installFeedbackMotion} from '/src/workspace-v2/feedbackMotion.ts';\ninstallFeedbackMotion();":''}
import '/src/global.css';
${version==='v2'?['workspace','interactions','motionBlocks','vipboard','feedback','feedbackToasts','pageFilters','pageLayout','compactWorkspaces','header','scrollbars','night','scrollLayout'].map(name=>`import '/src/workspace-v2/${name}.css';`).join('\n'):''}
const dark=new URLSearchParams(location.search).has('dark');
document.documentElement.dataset.v2Theme=dark?'dark':'light';
createApp({render:()=>h(NConfigProvider,{theme:dark?darkTheme:null,themeOverrides:dark?nightTheme:theme,locale:zhCN,dateLocale:dateZhCN},()=>h(RiskInspectionPage))}).mount('#app');`;},
    configureServer(server){server.middlewares.use(async(req,res,next)=>{
      if(!req.url?.split('?')[0].endsWith('/__risk-fields.html'))return next();
      const html=`<!doctype html><html lang="zh-CN" ${version==='v2'?'class="workspace-v2"':''}><head><meta name="viewport" content="width=device-width,initial-scale=1"/><title>风险排查离线验收</title></head><body><div id="app"></div><script type="module" src="${entry}"></script></body></html>`;
      res.setHeader('Content-Type','text/html');res.end(await server.transformIndexHtml(req.url,html));
    });},
  };
}
async function run(version){
  const server=built?await preview({root,configFile:false,logLevel:'error',build:{outDir:process.env.CLIPFLOW_RISK_BUILD_DIR||'dist'},preview:{host:'127.0.0.1',port:0}}):await createServer({root,configFile:version==='v2'?path.join(root,'vite.workspace-v2.config.ts'):false,
    cacheDir:path.join(output,'vite-cache-'+version),publicDir:false,
    plugins:[harness(version),...version==='legacy'?[vue()]:[]],
    server:{host:'127.0.0.1',port:0,strictPort:false,hmr:false,proxy:{}},
    optimizeDeps:{noDiscovery:true,include:['vue','naive-ui','lucide-vue-next']},logLevel:'error'});
  try{
    if(!built)await server.listen();const origin='http://127.0.0.1:'+server.httpServer.address().port;
    const cases=version==='v2'?[{width:1440,height:900},{width:402,height:874},{width:440,height:956},{width:390,height:667,dark:true,reduced:true}]:[{width:1440,height:900}];
    for(const scenario of process.env.CLIPFLOW_RISK_VIEWPORT_AUDIT?cases.filter(c=>version==='v2'&&c.width===390&&!c.dark):cases){
      const context=await browser.newContext({viewport:{width:scenario.width,height:scenario.height},reducedMotion:scenario.reduced?'reduce':'no-preference',serviceWorkers:'block'});
      const page=await context.newPage(),errors=[],requests=[],writes=[],viewports=[],layouts=layoutApiFixture();
      await context.addInitScript(dark=>localStorage.setItem('clipflow:workspace-night-mode:ou_risk-test',dark?'dark':'light'),!!scenario.dark);
      let fields=structuredClone([...fixed,...inputs]),templates=[{id:'base-a',name:'电气设备基础模板',enabled:true,kind:'base',version:'v1',revision:1,creator:'测试管理员',created:1792300000,updated:1792300000,scopes:['A','B'],fields:structuredClone([...fixed,...inputs.slice(0,2)]),items:[{title:'配电设备状态检查',values:{result:'保留的预填内容'}}]}];
      let fieldFailures=1,canEditLibrary=true;
      page.on('pageerror',error=>errors.push(error.message));
      await context.route('**/*',async route=>{
        const req=route.request(),url=new URL(req.url());
        if(url.origin!==origin)return route.abort('blockedbyclient');
        if(url.pathname==='/assets/connection-guard.js')return route.fulfill({body:'',contentType:'application/javascript'});
        if(!url.pathname.startsWith('/api/'))return route.continue();
        if(built&&await layouts.handle(route,'risk-test'))return;
        if(url.pathname==='/api/auth/status')return route.fulfill({json:{ok:true,data:{logged_in:true,user:{open_id:'ou_risk-test',name:'测试管理员',level:'L0',role:'admin',home_scope:'A',scope_rules:['A','B'],modules:['risk_inspection']},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}],default_scope:'A'}}});
        if(url.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'risk-fixture'}});
        if(!url.pathname.startsWith('/api/risk-inspections')){assert.equal(req.method(),'GET','No unrelated writes');return route.fulfill({json:{ok:true,data:{items:[],records:[],scopes:{},summary:{},total:0}}});}
        const method=req.method(),body=req.postDataJSON()||{},endpoint=url.pathname;
        requests.push({method,endpoint});if(method!=='GET')writes.push({endpoint,body});
        let data,status=200;
        if(endpoint==='/api/risk-inspections'&&method==='GET')data={items:[],total:0,is_manager:true,can_edit_template:canEditLibrary};
        else if(endpoint==='/api/risk-inspections/template'){
          if(method==='POST'){
            if(fieldFailures-- >0)return route.fulfill({status:503,json:{ok:false,error:'离线模拟：字段保存暂不可用'}});
            assert.equal(body.confirmed,true);fields.push({key:'new-field',field_id:'new-field',field_name:body.field_name,type:body.type,role:body.role,required:body.required,property:{options:body.options.map(name=>({name}))}});
          }
          data={fields,version:'library-'+writes.length};
        }else if(endpoint==='/api/risk-inspections/base-templates'){
          if(method==='POST'){const saved={...body,kind:'base',version:'v1',revision:1,creator:'测试管理员',created:1792300000,updated:1792300000,scopes:[]};templates.push(saved);data=saved;}
          else data={items:templates.map(t=>({...t,item_count:t.items.length,field_count:t.fields.length})),total:templates.length};
        }else if(endpoint.startsWith('/api/risk-inspections/base-templates/')){
          const index=templates.findIndex(t=>t.id===endpoint.split('/').pop());assert(index>=0,'Only fixture templates can be accessed');
          if(method==='PUT')templates[index]={...templates[index],...body,version:'v2',revision:2};data=templates[index];
        }else {status=404;errors.push('Unexpected fixture API: '+method+' '+endpoint);data={error:'未配置的离线接口'};}
        await route.fulfill({status,json:status===200?{ok:true,data}:data});
      });
      const tag=`${version}-${scenario.width}${scenario.dark?'-dark':''}`;
      try{
        const url=built?origin+(version==='v2'?'/assets/workspace-v2/index.html?__page=/risk-inspections':'/risk-inspections'):origin+(version==='v2'?'/assets/workspace-v2':'')+'/__risk-fields.html'+(scenario.dark?'?dark':'');
        await page.goto(url);
        await page.getByRole('button',{name:'基础模板',exact:true}).click();
        if(version==='legacy'){
          await page.locator('.history-list article').waitFor();
          assert.equal(await page.locator('.risk-field-picker').count(),0,'Legacy also opens the list first');
          await page.getByRole('button',{name:'新建基础模板',exact:true}).click();
          await page.locator('.risk-field-picker').waitFor();
          assert.equal(await page.locator('.risk-selected-fields>li').count(),0,'Legacy hides all fixed fields in a new draft');
          for(const field of fixed){assert.equal(await page.getByRole('button',{name:'修改字段 '+field.field_name,exact:true}).count(),0);assert.equal(await page.getByRole('button',{name:'移除已选字段 '+field.field_name,exact:true}).count(),0);}
          assert.equal(await page.getByRole('button',{name:'添加字段 楼栋',exact:true}).count(),0,'Legacy fixed fields are absent from the library');
          assert.equal(await page.locator('.risk-v2-field-modal').count(),0);
          assert.deepEqual(errors,[]);
          await page.screenshot({path:path.join(output,tag+'-legacy.png'),animations:'disabled',fullPage:true});
          reports.push({tag,checks:'legacy list-first and hidden fixed fields'});continue;
        }
        await page.locator('.history-list article').waitFor();
        assert.equal(await page.locator('.risk-field-picker').count(),0,'Base entry opens the library, not creation');
        assert.equal(requests.filter(r=>r.endpoint.endsWith('/template')).length,0,'Browsing the library cannot start creation');
        await page.screenshot({path:path.join(output,tag+'-library.png'),animations:'disabled',fullPage:true});
        await page.getByRole('button',{name:'查看 / 编辑',exact:true}).click();
        await page.locator('.risk-field-picker .n-spin-body').waitFor({state:'hidden'});
        const selected=page.locator('.risk-selected-fields'),library=page.locator('[aria-label="可选字段库"]');
        assert.deepEqual(await selected.locator('.risk-selected-name strong').allTextContents(),['排查结果','风险等级']);
        for(const field of fixed){assert.equal(await library.getByRole('button',{name:'添加字段 '+field.field_name,exact:true}).count(),0);assert.equal(await selected.getByText(field.field_name,{exact:true}).count(),0);}
        await page.getByRole('textbox',{name:'搜索字段名称',exact:true}).fill('楼栋');
        await library.getByText('没有匹配的字段',{exact:true}).waitFor();
        await page.getByRole('textbox',{name:'搜索字段名称',exact:true}).fill('');
        await page.getByRole('switch',{name:'排查结果是否必填',exact:true}).click();
        await page.getByRole('button',{name:'保存新版本',exact:true}).click();
        await page.getByRole('button',{name:'确认保存',exact:true}).click();
        await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});
        const saved=writes.find(r=>r.endpoint.endsWith('/base-a')).body;
        assert.deepEqual(saved.fields.filter(f=>fixed.some(s=>s.key===f.key)),fixed,'Invisible fixed fields survive saves byte-for-byte');
        assert.deepEqual(saved.items,[{title:'配电设备状态检查',values:{result:'保留的预填内容'}}],'Prefills survive the presentation change');
        await page.screenshot({path:path.join(output,tag+'-selected.png'),animations:'disabled',fullPage:true});
        await page.getByRole('button',{name:'模板列表',exact:true}).click();
        await page.getByRole('button',{name:'新建基础模板',exact:true}).click();
        await page.locator('.risk-field-picker').waitFor();
        assert.equal(await selected.locator('li').count(),0,'A new template does not expose its mandatory primary field');
        await page.getByText('尚未选择填写字段',{exact:true}).waitFor();
        await page.getByRole('button',{name:'添加字段 排查结果',exact:true}).click();
        await page.getByRole('textbox',{name:'基础模板名称',exact:true}).fill('新增基础模板');
        await page.getByRole('button',{name:'添加字段',exact:true}).click();
        const modal=page.locator('.risk-v2-field-modal'),select=modal.locator('.n-select');
        await modal.waitFor();await page.screenshot({path:path.join(output,tag+'-field-form.png'),animations:'disabled'});await select.locator('.n-base-selection').click();
        const menu=page.locator('.risk-v2-field-menu');await menu.waitFor();
        await menu.getByText('附件 / 照片',{exact:true}).scrollIntoViewIfNeeded();
        const geometry=await menu.evaluate(el=>{
          const option=[...el.querySelectorAll('.n-base-select-option')].find(n=>n.textContent.includes('附件 / 照片')),r=option.getBoundingClientRect();
          const scrollers=[el,...el.querySelectorAll('*')].filter(n=>/auto|scroll/.test(getComputedStyle(n).overflowY)&&n.scrollHeight>n.clientHeight+1);
          return {insideModal:!!el.closest('.n-modal'),scrollers:scrollers.length,visible:r.top>=0&&r.bottom<=innerHeight,hit:!!document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('.risk-v2-field-menu'),scopeRoot:el.closest('.workspace-v2')?.tagName,inBody:document.body.contains(el),optionMinHeight:getComputedStyle(option).minHeight};
        });
        assert.deepEqual(geometry,{insideModal:false,scrollers:1,visible:true,hit:true,scopeRoot:'HTML',inBody:true,optionMinHeight:scenario.width<600?'44px':'40px'},'Body portal retains V2 styles and one unclipped scrolling list');
        assert.equal(await modal.evaluate(el=>[el,...el.querySelectorAll('*')].filter(n=>/auto|scroll/.test(getComputedStyle(n).overflowY)&&n.scrollHeight>n.clientHeight+1).length),0,'The compact field form has no nested scrolling layers');
        await page.screenshot({path:path.join(output,tag+'-field-menu.png'),animations:'disabled',fullPage:false});
        await menu.getByText('附件 / 照片',{exact:true}).click();await menu.waitFor({state:'hidden'});
        assert.match(await select.innerText(),/附件 \/ 照片/);
        await select.locator('.n-base-selection').focus();await page.keyboard.press('ArrowDown');await menu.waitFor();
        await page.keyboard.press('Escape');await menu.waitFor({state:'hidden'});assert(await modal.isVisible(),'Escape closes the dropdown before the enclosing dialog');
        await modal.getByRole('button',{name:'取消',exact:true}).click();
        await page.getByRole('button',{name:'继续编辑',exact:true}).click();assert(await modal.isVisible(),'Cancelling discard retains the field draft');
        await select.locator('.n-base-selection').click();await menu.getByText('单选',{exact:true}).click();
        if(scenario.width<700){
          await modal.locator('textarea').fill(Array.from({length:30},(_,i)=>'选项 '+i).join('\n'));
          await page.getByRole('textbox',{name:'字段库名称',exact:true}).focus();
          await page.setViewportSize({width:scenario.width,height:340});
          const short=await inspectViewport(page,tag+'-short');verifyViewport(short);viewports.push({kind:'short-content-viewport',...short});
          await modal.locator(':scope>.n-card-content').evaluate(node=>node.scrollTop=node.scrollHeight);
          assert(await modal.locator(':scope>.n-card-content').evaluate(node=>node.scrollTop>0),'The single content scrollbar actually moves');
          const prior=writes.length;await modal.getByRole('button',{name:'保存字段',exact:true}).click();
          await verifyInlineFieldError(page,modal,'请填写不重复的字段名称。');assert.equal(writes.length,prior,'The visible save button executes local validation without sending an incomplete field');
          await page.screenshot({path:path.join(output,tag+'-short-field-error.png'),animations:'disabled'});
          await page.setViewportSize({width:scenario.width,height:scenario.height});
          const cdp=await context.newCDPSession(page);await cdp.send('Emulation.setPageScaleFactor',{pageScaleFactor:2});
          const visual=await inspectViewport(page,tag+'-visual');assert(visual.viewport.height<visual.innerHeight,'Real browser visual viewport shrinks without resizing the layout viewport');verifyViewport(visual);viewports.push({kind:'visual-only-viewport',...visual});
          await modal.getByRole('button',{name:'保存字段',exact:true}).click();assert.equal(writes.length,prior,'Save is clickable while only the visual viewport is constrained');
          await cdp.send('Emulation.setPageScaleFactor',{pageScaleFactor:1});await cdp.detach();
          await modal.locator('textarea').fill('');
        }
        const before=writes.length;await modal.getByRole('button',{name:'保存字段',exact:true}).click();
        await verifyInlineFieldError(page,modal,'请填写不重复的字段名称。');assert.equal(writes.length,before,'Validation failures do not save a field');
        await page.getByRole('textbox',{name:'字段库名称',exact:true}).fill('现场检查结论');
        await modal.locator('textarea').fill('无异常\n需跟进');
        await modal.getByRole('button',{name:'保存字段',exact:true}).click();
        await verifyInlineFieldError(page,modal,'离线模拟：字段保存暂不可用');
        await page.screenshot({path:path.join(output,tag+'-field-error.png'),animations:'disabled'});
        assert.equal(await page.getByRole('textbox',{name:'字段库名称',exact:true}).inputValue(),'现场检查结论','Failed save retains the field draft');
        await modal.getByRole('button',{name:'保存字段',exact:true}).click();await modal.waitFor({state:'hidden'});
        await page.getByRole('button',{name:'添加字段 现场检查结论',exact:true}).click();
        await page.getByRole('button',{name:'生成基础模板',exact:true}).click();
        await page.getByRole('button',{name:'确认保存',exact:true}).click();await page.getByRole('button',{name:'确认保存',exact:true}).waitFor({state:'hidden'});
        assert.deepEqual(templates[1].fields.map(f=>f.key),[...fixed.map(field=>field.key),'result','new-field'],'New template retains every fixed field plus selected inputs');
        assert.equal(writes.filter(r=>r.endpoint==='/api/risk-inspections').length,0,'Editing templates cannot publish inspections');
        canEditLibrary=false;await page.goto(url);await page.getByRole('button',{name:'基础模板',exact:true}).click();
        await page.getByRole('button',{name:'新建基础模板',exact:true}).click();await page.locator('.risk-field-picker').waitFor();
        assert.equal(await page.getByRole('button',{name:'添加字段',exact:true}).count(),0,'Managers without library-edit permission cannot add shared fields');
        assert.equal(await page.locator('.risk-field-tools').count(),0,'Library-edit permission is not expanded');
        await page.getByRole('button',{name:'添加字段 排查结果',exact:true}).click();assert.equal(await selected.locator('li').count(),1,'Permitted template selection remains available');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'No horizontal page overflow');
        assert.deepEqual(errors,[],'No browser errors or unmatched APIs');
        reports.push({tag,built,checks:'library-first, hidden system fields, payload retention, single scrolling overlay, selection, validation, failed save recovery, no publishing',writes:writes.length,portal:geometry,viewports});
        console.log('PASS '+tag);
      }catch(error){await page.screenshot({path:path.join(output,tag+'-failure.png'),fullPage:true});throw error;}
      finally{await context.close();}
    }
  }finally{if(built)await new Promise(resolve=>server.httpServer.close(resolve));else await server.close();}
}
try{await run('v2');await run('legacy');await writeFile(path.join(output,'results.json'),JSON.stringify(reports,null,2));console.log(JSON.stringify({ok:true,output,scenarios:reports.map(({tag,built,checks,writes,viewports})=>({tag,built,checks,writes,viewportChecks:viewports?.length||0}))},null,2));}
finally{await browser.close();}
