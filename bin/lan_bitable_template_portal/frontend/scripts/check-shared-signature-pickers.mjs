import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('..',import.meta.url));
const staff=Array.from({length:602},(_,i)=>({source:'staff',record_id:'person-'+i,name:'签名人员'+i,employee_no:'EMP'+i,
  building:i?'B楼':'A楼',open_id:'ou-'+i,has_signature:true,usage_confirmed:i===0,usage_confirmation_required:i!==0}));
const external=[{source:'external',record_id:'external-cross',name:'跨楼外部人员',building:'H楼',has_signature:true,portable_signature:true}];
const html=`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><div id="page-back-slot"></div><div id="app"></div>
<script type="module">
import {createApp,h,ref} from 'vue';
import {NConfigProvider,NDialogProvider,NMessageProvider} from 'naive-ui';
import Mop from '/src/components/EngineerMopPage.vue';
import Guard from '/src/components/CriticalGuardSignatureDrawer.vue';
import UiTransition from '/src/components/UiTransition.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import {pageContext} from '/src/workspace-v2/pageLayout';
import '/src/global.css';
const selected=ref([]);window.signers=[];
const kind=new URLSearchParams(location.search).get('module');
createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(NMessageProvider,{},()=>
  kind==='mop'
  ?h(Mop,{checking:false,loggedIn:true,loginUrl:'',scopeOptions:[{value:'A',label:'A楼'}]})
  :h(Guard,{open:true,scope:'A',contextKey:'critical_guard:fixture:A',taskTitle:'隔离重保检查',currentUserOpenId:'ou-0',initialSigners:selected.value,onChange:value=>{selected.value=value;window.signers=value}})
)))}).provide(pageContext,{user:ref({open_id:'fixture-user',role:'user',modules:['engineer_mop','critical_guard']}),scope:ref('A'),scopes:ref(['A']),module:ref('engineer_mop')}).component('UiTransition',UiTransition).component('LoadingIndicator',LoadingIndicator).mount('#app');
</script>`;
const browser=await chromium.launch({headless:true});
try{
  for(const version of ['legacy','v2']){
    const server=await createServer({root,configFile:path.join(root,version==='v2'?'vite.workspace-v2.config.ts':'vite.config.ts'),base:'/',logLevel:'error',
      cacheDir:path.resolve(root,'../../../output/signature-picker-cache',version),optimizeDeps:{entries:[],include:['vue','naive-ui','lucide-vue-next']},
      server:{host:'127.0.0.1',port:0,hmr:false},plugins:[{name:'signature-fixture',configureServer(dev){
        dev.middlewares.use('/__signature-check',async(_req,res)=>{res.setHeader('Content-Type','text/html');res.end(await dev.transformIndexHtml('/__signature-check',html));});
      }}]});
    try{
      await server.listen();const origin=`http://127.0.0.1:${server.httpServer.address().port}`;
      for(const width of [1440,390]){
        const context=await browser.newContext({viewport:{width,height:900},reducedMotion:'reduce'}),errors=[],queries=[];
        await context.route('**/*',async route=>{
          try {
          const request=route.request(),url=new URL(request.url()),p=url.pathname;
          if(url.origin!==origin)return route.abort();
          if(!p.startsWith('/api/'))return route.continue();
          const ok=data=>route.fulfill({json:{ok:true,data}});
          if(['/api/signatures/people','/api/signatures/temporary/people'].includes(p)){
            assert.equal(url.searchParams.get('all_people'),'1','all candidates explicitly requested');
            assert.equal(url.searchParams.get('scope'),'A','business scope must not become ALL');
            queries.push(url);
            const q=url.searchParams.get('q')||'';
            const people=(p.includes('/temporary/')?external:staff).filter(person=>JSON.stringify(person).includes(q));
            return ok({people,count:people.length,returned:people.length,scope:'A'});
          }
          if(p==='/api/engineer/mop/bootstrap')return ok({auth:{user:{open_id:'ou-0',name:'当前人员'}},notices:[{notice_key:'notice-a',source_record_id:'source-a',title:'隔离A楼维护',scope:'A',building:'A楼',status:'开始'}],mop_candidates:[{record_id:'mop-a',title:'隔离维护记录表',attachments:[{file_token:'file-a',name:'维护记录表.xlsx'}]}]});
          if(p==='/api/engineer/mop/bind')return ok({binding:request.postDataJSON()});
          if(p==='/api/engineer/mop/preview')return ok({local_file:{path:'fixture.xlsx',file_name:'维护记录表.xlsx'},mop_title:'隔离维护记录表',attachment:{name:'维护记录表.xlsx'},sheets:[{name:'维护记录',row_count:2,column_count:2,rows:[['检查项','结果'],['设备状态','正常']],checkbox_cells:[],maintenance_fields:[]}],warnings:[]});
          assert.equal(request.method(),'GET','Unexpected real business action '+p);
          return ok({people:[],items:[],signatures:[]});
          } catch(error) { errors.push(error.message); await route.abort(); }
        });
        const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.stack||error.message));
        page.on('console',msg=>{if(msg.type()==='warning'||msg.type()==='error')errors.push(msg.text().slice(0,1000));});
        try{
          for(const module of ['mop','guard']){
            await page.goto(origin+'/__signature-check?module='+module+'&scope=A');
            if(module==='mop'){
              await page.getByRole('button',{name:'打开填写',exact:true}).click();
              await page.locator('.signature-role-summary button').first().click();
            }
            const panel=page.getByRole('dialog',{name:module==='mop'?'维护人员签名':'选择检查人签名',exact:true});
            await panel.waitFor();
            await panel.locator('.sign-person').filter({hasText:'签名人员601'}).click();
            assert.equal(await panel.locator('.sign-person').count(),602,'all staff appear without an initial 60/100/500 cutoff');
            assert(await panel.locator('.sign-person').filter({hasText:'签名人员601'}).evaluate(el=>el.classList.contains('active')));
            if(module==='guard'){
              const selected=await page.evaluate(()=>window.signers);
              assert.equal(selected[0].record_id,'person-601');assert.equal(selected[0].usage_confirmed,false,'selecting another signer does not approve consent');
            }
            await panel.getByRole(module==='mop'?'tab':'button',{name:/临时.*外部人员/}).click();
            const expand=panel.getByRole('button',{name:/选择已有外部签名/});
            if(await expand.count())await expand.click();
            await panel.locator('.external-signature-results button').filter({hasText:'跨楼外部人员'}).click();
            await panel.locator('.other-signature-task-list').getByText('跨楼外部人员',{exact:true}).waitFor();
            if(module==='guard')assert((await page.evaluate(()=>window.signers)).some(person=>person.record_id==='external-cross'));
          }
          assert(queries.some(url=>url.pathname.includes('/temporary/people')));
          assert(queries.some(url=>url.searchParams.get('notice_key')?.startsWith('critical_guard:fixture:A')));
          assert(queries.some(url=>url.searchParams.get('notice_key')?.includes('notice-a')));
          assert.deepEqual(errors,[]);
        }catch(error){console.error(version,width,errors,await page.locator('body').innerText());throw error;}
        finally{await context.close();}
      }
    }finally{await server.close();}
  }
  console.log('PASS: legacy/V2 and desktop/mobile MOP/guard pickers select staff beyond 500 and cross-building external signatures, preserving business scope and pending consent. APIs mocked.');
}finally{await browser.close();}
