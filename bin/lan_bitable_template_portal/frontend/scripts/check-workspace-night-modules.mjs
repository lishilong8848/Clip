import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';

assert(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR,'Use an isolated build, not the production service.');
const only=process.argv.find(value=>value.startsWith('--only='))?.slice(7);
const audit=process.argv.includes('--audit'),out=path.resolve('../../../output/playwright/night-modules'+(audit?'-before':'')+(only?'-'+only.replace(/[^a-z-]/g,''):''));
await mkdir(out,{recursive:true});
const server=await preview({configFile:'vite.workspace-v2.config.ts',logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,entry=origin+'/assets/workspace-v2/index.html';
const browser=await chromium.launch({headless:true}),context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
const errors=[],report=[],scopes=[{value:'A',label:'A楼'},{value:'B',label:'B楼'}];
const modules=['event','maintenance','change','repair','polling','adjust','power','engineer_mop','repair_management','critical_guard','daily','cabinet','water','plan_convergence','drills','learning','knowledge','links','signatures','accounts'];
const user={open_id:'night-audit',name:'隔离主题验收',role:'admin',level:'L0',category:'manager',modules,login_method:'password'};
const date=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai'}).format(new Date());
const field=(field_name,field_type=1,extra={})=>({field_name,field_type,editable:true,...extra});
const project={record_id:'p1',title:'配电设备故障检修',workflow:'维修中',building:'A楼',specialty:'电气',progress_percent:35,followup_count:1,is_completed:false,source_event_id:'e1',source_repair_ids:['r1'],record_version:'v1',display_fields:{'维修名称':'配电设备故障检修','故障发生时间':date+' 09:00:00','备注':'待复核'},raw_fields:{}};
const notice={record_id:'n1',source_record_id:'s1',notice_key:'n1',title:'A楼冷机维护',building:'A楼',scope:'A',status:'开始',specialty:'暖通',source_progress:'未开始'};
const doc={id:'doc1',name:'主题验收.txt',owner_name:'示例人员',can_edit:true,version:1,status:'ready',size:128,chunks:2,updated_at:1792044000};
const summary={answered:15,task_answered:15,assigned:30,choice_answered:12,correct:9,wrong:3,accuracy:75,learning_days:3,practice_count:1,attempt_count:15,answered_people:1,interview_ratings:{},interview_total:3};
await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url()),p=url.pathname,ok=data=>route.fulfill({json:{ok:true,data}});
  if(url.origin!==origin)return route.abort();
  if(p==='/assets/vnet-logo.png')return route.fulfill({path:path.resolve('public/assets/vnet-logo.png')});
  if(!p.startsWith('/api/'))return route.continue();
  if(p==='/api/engineer/mop/bind')return ok({binding:request.postDataJSON()});
  if(p==='/api/engineer/mop/preview')return ok({local_file:{path:'fixture.xlsx',file_name:'维护记录表.xlsx'},mop_title:'冷机维护记录',sheets:[{name:'维护记录',row_count:2,column_count:3,is_cover:false,rows:[['检查项目','结果','备注'],['设备检查','','']],merged_cells:[],checkbox_cells:[],maintenance_fields:[]}],warnings:[]});
  if(p==='/api/repair-management/prefill')return ok({fields:{},warnings:[]});
  if(request.method()!=='GET'){errors.push('Unexpected request: '+request.method()+' '+p);return route.abort();}
  if(p==='/api/auth/status')return ok({logged_in:true,user,scope_options:scopes});
  if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'night-audit'}});
  if(p==='/api/auth/accounts')return ok({items:[{...user,id:'person1',record_id:'person1',name:'示例主管',level:'L3',category:'supervisor',home_scope:'A',own_scopes:['A'],scopes:['A'],enabled:true,kind:'vnet'}],category_options:[],scope_options:scopes,level_options:[],module_options:[],policy:{level:'L0'}});
  if(p==='/api/engineer/mop/bootstrap')return ok({notices:[notice],mop_candidates:[{record_id:'m1',title:'冷机维护标准作业指导书',specialty:'暖通',maintenance_type:'月度',version:'V1.2',file_no:'MOP-001',file_status:'有效',fields:{专业:'暖通',维护类型:'月度'},attachment_count:1,attachments:[{file_token:'f1',name:'维护记录表.xlsx'}]}],warnings:[],auth:{user}});
  if(p==='/api/workbench')return ok({records:[notice],ongoing:[{...notice,active_item_id:'a1',target_record_id:'t1'}],source_snapshot_ready:true,records_pagination:{total:1},ongoing_pagination:{total:1}});
  if(p==='/api/repair-management/records')return ok({fields:[field('维修名称'),field('备注'),field('故障发生时间',5)],records:[project],total:1,offset:0});
  if(p==='/api/repair-management/records/p1')return ok({record:project});
  if(p==='/api/repair-management/status')return ok({stats:{total:1,in_progress:1,completed_total:0},records:[project]});
  if(p==='/api/repair-management/followups')return ok({fields:[field('维修进展描述'),field('维修进度',2),field('维修方',3,{options:['我方','厂家']})],records:[],total:0,shared_fields:{}});
  if(p==='/api/repair-management/stream')return route.fulfill({status:204,body:''});
  if(p==='/api/repair-management/sync-status')return ok({status:'completed'});
  if(p==='/api/capacity/water/bootstrap')return ok({scope:'A',options:{meters:['水表1'],frequencies:['日'],shifts:['白','夜']},permissions:{can_create:true},summary:{record_count:1,total_usage:20,photo_records:1},snapshot:{exists:true}});
  if(p==='/api/capacity/water/records')return ok({records:[{record_id:'w1',meter:'水表1',frequency:'日',shift:'白',statistic_date:date,meter_value:120,computed_usage:20,photo_count:1}],total:1,snapshot:{exists:true}});
  if(p==='/api/events/monthly')return ok({records:[{record_id:'e1',title:'设备通信故障',occurrence_time:date+' 09:00:00',status:'处理中',building:'A楼',building_codes:['A'],level:'I3',transfer_to_overhaul:true}],snapshot_exists:true});
  if(p==='/api/assistant/knowledge')return ok({items:[doc],total:1,is_admin:true,settings:{mode:'local',engine_status:'ready'}});
  if(p==='/api/assistant/knowledge/documents/doc1')return ok({document:doc,sections:[{text:'隔离测试资料',ordinal:0}],total:1});
  if(p==='/api/link-directory')return ok({can_edit:true,items:[{id:'link1',name:'示例业务入口',category:'业务',purpose:'隔离展示',url:'https://example.com',sort_order:1}]});
  if(p==='/api/learning/bootstrap')return ok({is_admin:true,can_answer:true,can_view_buildings:true,self_person:{id:'self1',name:user.name,employee_no:'TEST',scopes:['A']},self_scope:'A',scope:'A',scopes,today:date,settings:{enabled:true,publish_time:'13:00'},sync:{status:'ready'}});
  if(p==='/api/learning/profile')return ok({summary,today_summary:{...summary,papers:1,answered_people:1,completed_people:0,not_started_people:0},people:[{person_id:'self1',name:user.name,scope:'A',summary,today:{answered:15,total:30}}],trend:[{date,answered:15,attempt_count:15,people:1,accuracy:75}],banks:[{label:'笔试',wrong:3}],topics:[],distribution:[{label:'60–79%',count:1}],published:true});
  if(p==='/api/plan-convergence/maintenance/records'||p==='/api/plan-convergence/change/records')return ok([]);
  return ok({items:[],records:[],users:[],fields:[],people:[],tasks:[],categories:[],rows:[],groups:[],scopes,scope_options:scopes,settings:{},summary:{},stats:{},permissions:{},total:0,snapshot:{exists:true}});
});
const page=await context.newPage();page.setDefaultTimeout(10000);page.on('pageerror',error=>errors.push(error.message));
async function theme(mode){
  if(!await page.getByRole('switch',{name:'夜间模式'}).isVisible())await page.getByRole('button',{name:'导航配色',exact:true}).click();
  const toggle=page.getByRole('switch',{name:'夜间模式'});
  await toggle.waitFor({state:'visible'});
  if(await toggle.getAttribute('aria-checked')!==String(mode==='dark'))await toggle.click();
  await page.waitForFunction(mode=>document.documentElement.dataset.v2Theme===mode,mode);
  await page.getByRole('button',{name:'导航配色',exact:true}).click();await toggle.waitFor({state:'hidden'});await page.waitForTimeout(650);
}
async function capture(name,selector){
  const root=page.locator(selector).first();await root.waitFor();await root.scrollIntoViewIfNeeded();
  const samples=await root.evaluate(root=>{
    const rgb=value=>{const m=value.match(/^rgba?\(([^)]+)\)/);return m?m[1].split(/[\s,\/]+/).map(Number):null;};
    const lum=c=>c.slice(0,3).map(n=>n/255).map(n=>n<=.04045?n/12.92:((n+.055)/1.055)**2.4).reduce((sum,n,i)=>sum+n*[.2126,.7152,.0722][i],0);
    const over=(fg,bg)=>fg.slice(0,3).map((c,i)=>c*(fg[3]??1)+bg[i]*(1-(fg[3]??1)));
    const samples=[];
    for(const el of root.querySelectorAll('*')){
      const rect=el.getBoundingClientRect(),style=getComputedStyle(el),text=[...el.childNodes].filter(n=>n.nodeType===3).map(n=>n.textContent.trim()).join('').trim();
      if(!text||!rect.width||!rect.height||el.closest('[aria-hidden="true"],:disabled,[disabled],.n-button--disabled,.v2-watermark,svg,canvas,script,style'))continue;
      const chain=[];let opacity=1,gradient=false;
      for(let parent=el;parent;parent=parent.parentElement){const s=getComputedStyle(parent);opacity*=Number(s.opacity);if(s.visibility==='hidden')opacity=0;if(s.backgroundImage!=='none')gradient=true;chain.unshift(s.backgroundColor);}
      if(opacity<.8||gradient)continue;
      let bg=[255,255,255];for(const c of chain){const parsed=rgb(c);if(parsed)bg=over(parsed,bg);}
      const color=rgb(style.color);if(!color)continue;const fg=over(color,bg),contrast=(Math.max(lum(fg),lum(bg))+.05)/(Math.min(lum(fg),lum(bg))+.05);
      const min=parseFloat(style.fontSize)>=24||parseFloat(style.fontSize)>=18.66&&Number(style.fontWeight)>=700?3:4.5;
      samples.push({text:text.slice(0,55),tag:el.tagName,cls:String(el.className).slice(0,120),fg:style.color,bg:bg.map(Math.round),contrast:Math.round(contrast*100)/100,min});
    }
    return samples;
  });
  const mode=await page.locator('html').getAttribute('data-v2-theme'),width=page.viewportSize().width;
  report.push({name,mode,width,samples:samples.length,failures:samples.filter(s=>s.contrast<s.min)});
  await page.screenshot({path:path.join(out,`${name}-${mode}-${width}.png`),animations:'disabled'});
}
const cases=[
  ['mop','/engineer/mop?scope=A','.v2-mop-workspace'],['repair','/repair-management?scope=A','.v2-repair-workspace'],
  ['notices','/workbench-lite?scope=A&work_type=maintenance','.v2-notices'],['water','/water-management?scope=A&water_view=records','.v2-water'],
  ['events','/?mode=events&detail=1&scope=A','.event-page'],['learning','/learning?view=overview&scope=A','.learning-page'],
  ['knowledge','/knowledge-base','.kb-page'],['links','/link-directory','.link-directory-page'],['accounts','/admin/settings','.accounts'],
  ['daily','/daily-tasks?scope=A','.daily-page'],['guard','/critical-guard?scope=A','.guard-page'],['drills','/drill-management?scope=A','.drill-page'],
  ['plan','/plan-convergence','.pc-page'],['signatures','/signature-management','.signature-management'],['history','/admin/history-memory','.v2-history'],
  ['sops','/?view=sops&scope=A','.v2-sop-manager'],['profile','/?view=my','.v2-my'],['home','/?view=home','.v2-dashboard'],
  ['cabinet','/cabinet-power?scope=A','.cabinet-page'],['cabinet-batch','/cabinet-power/batches?scope=A&mode=new','.batch-page'],
];
try{
  for(const width of [1440,390]){
    await page.setViewportSize({width,height:width===390?844:1000});
    for(const [name,url,selector] of cases.filter(item=>!only||item[0]===only)){
      const target=new URL(url,origin),params=new URLSearchParams(target.search);params.set('__page',target.pathname);
      await page.goto(entry+'?'+params);await page.locator(selector).first().waitFor();await page.waitForTimeout(300);
      for(const mode of ['light','dark']){await theme(mode);await capture(name,selector);}
      if(name==='mop'){
        const select=page.locator('.v2-mop-workspace .v2-single-select .n-select').first();
        for(const mode of ['dark','light']){
          await theme(mode);await select.click();await page.locator('.v2-single-select-menu').waitFor();await page.waitForTimeout(300);
          await capture('mop-select','.v2-single-select-menu');await page.keyboard.press('Escape');await page.locator('.v2-single-select-menu').waitFor({state:'hidden'});
        }
      }
      if(name==='repair'){
        await page.locator('.record-row').first().click();await page.locator('.v2-repair-followups').waitFor();
        for(const mode of ['dark','light']){await theme(mode);await capture('repair-form','.v2-repair-detail');}
      }
      if(name==='mop'&&width>=761){
        await page.getByRole('button',{name:'打开填写',exact:true}).click();await page.locator('.preview-scroll table').waitFor();
        for(const mode of ['light','dark']){await theme(mode);await capture('mop-sheet','.mop-preview-page');}
      }
    }
  }
  console.log(JSON.stringify({views:report.length,textChecks:report.reduce((n,r)=>n+r.samples,0),failures:report.filter(r=>r.failures.length)},null,2));
  assert.deepEqual(errors,[]);
  if(!audit&&!process.argv.includes('--report-only'))assert.equal(report.reduce((n,r)=>n+r.failures.length,0),0,'contrast failures: see report.json');
}finally{await writeFile(path.join(out,'report.json'),JSON.stringify({errors,report},null,2));await browser.close();await server.close();}
