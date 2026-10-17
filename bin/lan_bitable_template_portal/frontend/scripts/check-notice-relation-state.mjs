import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {build,preview} from 'vite';
import vue from '@vitejs/plugin-vue';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const outDir=path.resolve(root,'../../../output/notice-relation-state-build');
const screenshots=path.resolve(root,'../../../output/playwright/notice-relation-state');
await mkdir(screenshots,{recursive:true});
await build({root,configFile:false,publicDir:false,logLevel:'error',plugins:[{
  name:'notice-relation-state-fixture',enforce:'post',
  resolveId(id){if(id==='virtual:relation-state')return '\0relation-state';},
  load(id){if(id!=='\0relation-state')return;return `
    import {createApp,h,ref} from 'vue';
    import {NConfigProvider,NDialogProvider} from 'naive-ui';
    import Relations from '/src/workspace-v2/NoticeRelations.vue';
    import PlanBinding from '/src/workspace-v2/NoticePlanBinding.vue';
    import {parseNoticeForm,noticeCommand,normalizePastedNotice} from '/src/workspace-v2/noticeForm.ts';
    const fixtures={
      linked:{event:'recEventA',name:'控制柜指示灯故障',project:'recProjectA'},
      missing:{event:'',name:'未选择',project:'recProjectA'},
      titleOnly:{event:'',name:'只有描述，尚未保存关联',project:'recProjectA'},
      unbound:{event:'',name:'未选择',project:''},
      maintenance:{event:'',name:'未选择',project:'recProjectA',kind:'maintenance'},
      saved:{event:'recEventB',name:'新故障来源',project:'recProjectA'}
    };
    const html=f=>'<form id="lite-notice-form"><input name="work_type" type="hidden" value="'+(f.kind||'repair')+'"><input name="scope" type="hidden" value="A"><input name="target_record_id" type="hidden" value="recTarget"><input name="repair_management_record_id" type="hidden" value="'+f.project+'"><select name="source_record_id"><option value="'+f.project+'" selected>控制柜指示灯维修项目</option></select><section data-repair-event-link><input name="related_event_record_id" type="hidden" value="'+f.event+'"><span id="lite-repair-event-link-status">'+f.name+'</span></section><button name="submit_action" value="update"></button></form>';
    const forms=Object.fromEntries(Object.entries(fixtures).map(([key,f])=>[key,parseNoticeForm(html(f))]));
    if(forms.linked.repairRelation.eventId!=='recEventA'||forms.linked.repairRelation.projectTitle!=='控制柜指示灯维修项目')throw new Error('Existing relation metadata was not read');
    const command=form=>noticeCommand(form,'update','fixture-operation',0,{},[],[]);
    if(JSON.stringify(command(forms.linked))!==JSON.stringify(command(forms.missing)))throw new Error('Read-only metadata changed the notice command');
    if('related_event_record_id' in command(forms.linked).patch||'repairRelation' in command(forms.linked).patch)throw new Error('Display metadata leaked into business payload');
    if(normalizePastedNotice('\\r\\n 标题 \\r\\n\\t \\r\\n  内容\\r说明\\n\\n')!==' 标题 \\n  内容\\n说明'||normalizePastedNotice(' \\n\\t\\r\\n')!=='')throw new Error('Pasted text lost content or kept blank lines');
    const selected=ref('linked');
    const plan=ref({...forms.maintenance,values:{scope:'A',work_type:'maintenance',building_codes:['A'],title:'EA118机房A楼BA系统资料维护',maintenance_cycle:'半年',manual_binding_required:true}});
    const planOptions=ref([]),choice=ref(null),expanded=ref(true),locked=ref(false),loading=ref(false);let confirms=0;
    window.planFixture={set(options,values={}){planOptions.value=options;Object.assign(plan.value.values,{source_record_id:'',manual_binding_choice:''},values);choice.value=null;expanded.value=true;locked.value=false;loading.value=false;selected.value='plan';},state:()=>({choice:choice.value,source:plan.value.values.source_record_id,mode:plan.value.values.manual_binding_choice,confirms}),busy(value){loading.value=value;},lock(value){locked.value=value;}};
    createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>[
      h('nav',{'aria-label':'隔离测试场景'},Object.keys(fixtures).map(key=>h('button',{onClick:()=>selected.value=key},'fixture-'+key))),
      selected.value==='plan'?h(PlanBinding,{form:plan.value,choice:choice.value,expanded:expanded.value,options:planOptions.value,locked:locked.value,loading:loading.value,onToggle:()=>{expanded.value=!expanded.value;},onMode:value=>{plan.value.values.manual_binding_choice=value;choice.value=null;},'onUpdate:choice':value=>{choice.value=value;},onConfirm:()=>{confirms++;plan.value.values.source_record_id=choice.value;}})
      :h(Relations,{key:selected.value,form:forms[selected.value],locked:false,onBound:()=>{selected.value='saved';}})
    ]))}).mount('#app');`;
  },
  generateBundle(_options,bundle){
    const entry=Object.values(bundle).find(item=>item.type==='chunk'&&item.isEntry);
    const css=Object.values(bundle).filter(item=>item.fileName.endsWith('.css'));
    this.emitFile({type:'asset',fileName:'index.html',source:`<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(item=>`<link rel="stylesheet" href="/${item.fileName}">`).join('')}<style>*{box-sizing:border-box}body{margin:0;padding:16px;font-family:system-ui,sans-serif;color:#203040;background:#f4f7fb}nav{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px}#app>section{padding:12px;background:white}.v2-notice-tools{display:flex;align-items:center;gap:8px}.v2-notice-tools h2{font-size:14px}button{cursor:pointer}</style></head><body><div id="app"></div><script type="module" src="/${entry.fileName}"></script></body></html>`});
  }
},vue()],build:{outDir,emptyOutDir:true,cssCodeSplit:false,rollupOptions:{input:'virtual:relation-state'}}});
const server=await preview({root,configFile:false,logLevel:'error',build:{outDir},preview:{host:'127.0.0.1',port:0}});
const origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true});
const errors=[],bindings=[];let reads=0,page;
try{
  const context=await browser.newContext({viewport:{width:1366,height:768},reducedMotion:'reduce'});
  await context.route('**/*',async route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.origin!==origin)return route.abort();
    if(!url.pathname.startsWith('/api/'))return route.continue();
    const ok=data=>route.fulfill({json:{ok:true,data}});
    if(url.pathname==='/api/workbench/repair-event-candidates'){
      reads++;assert.equal(url.searchParams.get('scope'),'A');
      return ok({records:[{repair_management_record_id:'recProjectA',event_record_id:'recEventA',title:'控制柜指示灯故障',building:'A楼'},
        {repair_management_record_id:'recProjectB',event_record_id:'recEventB',title:'新故障来源',building:'A楼'}],total:2});
    }
    if(url.pathname==='/api/workbench/repair-event-bind'){
      const body=request.postDataJSON();bindings.push(body);
      assert.deepEqual(body,{scope:'A',source_record_id:'recProjectA',event_record_id:'recEventB',candidate_project_record_id:'recProjectB',active_item_id:'',target_record_id:'recTarget'});
      return ok({saved:true});
    }
    if(url.pathname==='/api/notice-target-candidates')return ok({candidates:[{target_record_id:'recTarget',title:'原目标通告'}]});
    throw new Error('Unexpected business request: '+request.method()+' '+url.pathname);
  });
  page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin);
  const relation=page.getByRole('group',{name:'故障关联',exact:true});
  await relation.getByRole('status').filter({hasText:'控制柜指示灯故障 · 控制柜指示灯维修项目'}).waitFor();
  assert.equal(reads,0,'already-associated plans do not fetch candidates automatically');
  assert.equal(await relation.getByRole('button',{name:'补充故障关联'}).count(),0);
  for(const width of [1366,390,360]){
    await page.setViewportSize({width,height:width===1366?768:640});
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    if(width<600)assert((await relation.getByRole('button',{name:'更换故障关联'}).boundingBox()).height>=44);
    await page.screenshot({path:path.join(screenshots,'linked-'+width+'.png'),fullPage:true});
  }
  await page.setViewportSize({width:1366,height:768});
  await relation.getByRole('button',{name:'更换故障关联'}).click();
  await page.getByRole('button',{name:'已关联',exact:true}).waitFor();
  assert(await page.getByRole('button',{name:'已关联',exact:true}).isDisabled());
  assert.equal(bindings.length,0,'opening an existing relation never saves it again');
  await page.getByLabel('关联候选',{exact:true}).click();
  await page.locator('.n-base-select-option').filter({hasText:'新故障来源'}).click();
  await page.getByRole('button',{name:'核对并保存关联',exact:true}).click();
  assert.equal(bindings.length,0,'selecting another event still needs confirmation');
  await page.getByRole('button',{name:'返回',exact:true}).click();
  assert.equal(bindings.length,0,'cancelling never changes the relationship');
  await page.getByRole('button',{name:'核对并保存关联',exact:true}).click();
  await page.getByRole('button',{name:'确认关联并重新读取',exact:true}).click();
  await relation.getByRole('status').filter({hasText:'新故障来源'}).waitFor();
  assert.equal(bindings.length,1);
  for(const scenario of ['missing','titleOnly','unbound']){
    await page.getByRole('button',{name:'fixture-'+scenario,exact:true}).click();
    await relation.getByRole('button',{name:'补充故障关联',exact:true}).waitFor();
    assert.equal(await relation.getByRole('button',{name:'更换故障关联'}).count(),0,'names alone are not proof of a saved association');
    if(scenario==='missing')await relation.getByRole('status').filter({hasText:'已关联维修项目，尚未关联事件'}).waitFor();
  }
  await page.getByRole('button',{name:'fixture-maintenance',exact:true}).click();
  assert.equal(await relation.count(),0,'non-repair notices have no fault-association entry');
  await page.getByRole('button',{name:'重新查找目标通告',exact:true}).click();
  await page.getByRole('button',{name:'已关联',exact:true}).waitFor();
  assert(await page.getByRole('button',{name:'已关联',exact:true}).isDisabled(),'target association keeps its original behavior');
  const planOption={value:'recPlanA',title:'EA118机房A楼BA系统资料维护',label:'EA118机房A楼BA系统资料维护 · 半年 · A楼 · 未开始',building:'A楼',cycle:'半年'};
  await page.evaluate(option=>window.planFixture.set([option]),planOption);
  const binding=page.locator('[data-notice-section="binding"]'),planSelect=binding.getByLabel('源表计划事项',{exact:true});
  await binding.getByRole('alert').filter({hasText:'独立通告 · 未关联计划'}).waitFor();
  assert.equal(await planSelect.count(),0,'no plan selector until a handling mode is chosen');
  await binding.getByText('独立通告',{exact:true}).click();
  assert.equal(await binding.getByRole('alert').count(),0);
  assert.equal(await planSelect.count(),0);
  assert.equal(await binding.getByRole('button',{name:'确认关联',exact:true}).count(),0,'independent notices have no plan-binding controls');
  await binding.getByText('关联计划',{exact:true}).click();
  await planSelect.waitFor();
  assert.equal(await binding.getByRole('alert').count(),0);
  assert.equal((await page.evaluate(()=>window.planFixture.state())).choice,null,'a recommendation is not automatically applied');
  assert(await binding.getByRole('button',{name:'确认关联',exact:true}).isDisabled());
  const apply=binding.getByRole('button',{name:'应用推荐计划',exact:true});
  for(const width of [1366,390,360]){
    await page.setViewportSize({width,height:width===1366?768:640});
    assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));
    if(width===1366){const selectBox=await planSelect.boundingBox(),recBox=await binding.locator('.v2-plan-recommendation').boundingBox();assert(recBox.x>selectBox.x+selectBox.width,'recommendation is beside the selector on desktop');}
    else assert((await apply.boundingBox()).height>=44);
    await page.screenshot({path:path.join(screenshots,'plan-'+width+'.png'),fullPage:true});
  }
  await page.evaluate(()=>window.planFixture.busy(true));
  assert(await apply.isDisabled());
  await page.evaluate(()=>window.planFixture.busy(false));
  await apply.click();
  assert.deepEqual(await page.evaluate(()=>window.planFixture.state()),{choice:'recPlanA',source:'',mode:'bind',confirms:0},'Apply selects only; binding remains unconfirmed');
  await page.evaluate(()=>window.planFixture.lock(true));
  assert(await binding.getByRole('button',{name:'确认关联',exact:true}).isDisabled());
  await page.evaluate(()=>window.planFixture.lock(false));
  await binding.getByRole('button',{name:'确认关联',exact:true}).click();
  assert.equal((await page.evaluate(()=>window.planFixture.state())).source,'recPlanA');
  await binding.getByText('已关联计划',{exact:true}).waitFor();
  for(const options of [
    [planOption,{...planOption,value:'recDuplicate'}],
    [{...planOption,building:'B楼'}],
    [{...planOption,cycle:'每月'}],
    [{...planOption,title:'EA118机房A楼BA系统软件维护'}],
    [{...planOption,title:'EA118机房A楼BA系统资料维护（第二阶段）'}],
    []
  ]){
    await page.evaluate(options=>window.planFixture.set(options,{manual_binding_choice:'bind'}),options);
    assert.equal(await apply.count(),0,'ambiguous or conflicting plans are never recommended');
  }
  await page.evaluate(option=>window.planFixture.set([{...option,title:'A楼BA系统资料维护通告'}],{manual_binding_choice:'bind'}),planOption);
  await apply.waitFor();
  assert.equal(bindings.length,1,'plan previews and Apply never write business data');
  assert.deepEqual(errors,[]);
  console.log('PASS: saved/partial fault associations, unchanged commands, cancel/confirm, target lookup, explicit independent/plan choice, recommendation requires Apply and confirm, ambiguity/building/cycle guards, blank-line cleanup, PC/390/360px. All business calls mocked; production assets unchanged.');
}catch(error){if(page)await page.screenshot({path:path.join(screenshots,'failure.png'),fullPage:true});throw error;}
finally{await browser.close();await server.close();}
