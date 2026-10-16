import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createServer} from 'vite';
import {chromium} from 'playwright';
import {mkdir} from 'node:fs/promises';

const root=fileURLToPath(new URL('..',import.meta.url));
const output=path.resolve(root,'../../../output/playwright/mop-signature-invitations');await mkdir(output,{recursive:true});
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
        const context=await browser.newContext({viewport:{width,height:900},reducedMotion:'reduce'}),errors=[],queries=[],invitations=[],invitationWrites=[];
        let nextSendStatus='sent',loseNextReply=false;
        await context.route('**/*',async route=>{
          try {
          const request=route.request(),url=new URL(request.url()),p=url.pathname;
          if(url.origin!==origin)return route.abort();
          if(!p.startsWith('/api/'))return route.continue();
          const ok=data=>route.fulfill({json:{ok:true,data}});
          if(p==='/api/signatures/management/mop-people')return ok({scope_options:[{value:'A',label:'A楼'}],recipients:[{value:'duty:A',label:'A楼值班账号'},{value:'staff:person-0',label:'签名人员0 · EMP0 · A楼'}]});
          if(p==='/api/signatures/management/mop-list')return ok({items:invitations});
          if(p==='/api/signatures/management/mop-invite'){
            const payload=request.postDataJSON();invitationWrites.push(payload);
            assert.equal(payload.scope,'A');assert.equal(payload.role,'implementer');assert.equal(payload.notice_key,'notice-a');assert.equal(payload.recipient,'duty:A');
            assert(payload.operation_id&&payload.send_operation_id);
            const index=invitations.findIndex(person=>person.operation_id===payload.operation_id),previous=invitations[index];
            assert.equal(payload.expected_request_id,previous?.request_id||'','retry must name the current request');
            const request_id='mopsend_'+payload.send_operation_id;
            const person={source:'external',mop_invitation:true,record_id:previous?.record_id||'recVisitor'+invitations.length,
              role:payload.role,name:payload.name,building:'A楼',scope:'A',employee_no:payload.employee_no,specialty:payload.specialty,
              status:'pending',has_signature:false,operation_id:payload.operation_id,send_operation_id:payload.send_operation_id,
              request_id,recipient:payload.recipient,request_status:nextSendStatus,
              signature_reason:nextSendStatus==='send_failed'?'隔离测试：链接发送失败。':''};
            if(index<0)invitations.push(person);else invitations[index]=person;
            if(loseNextReply){loseNextReply=false;return ok({});}
            return ok({person,request:{status:nextSendStatus,request_id}});
          }
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
        async function openMopSignatures(){
          await page.getByRole('button',{name:'打开填写',exact:true}).click();
          const trigger=page.locator('.signature-role-summary button').first();
          if(version==='v2' && width<=900)await page.getByRole('button',{name:/继续.*检查与备注/}).click();
          await trigger.click();
        }
        try{
          for(const module of ['mop','guard']){
            await page.goto(origin+'/__signature-check?module='+module+'&scope=A');
            if(module==='mop'){
              await openMopSignatures();
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
            if(module==='mop'){
              await panel.getByRole('button',{name:'添加人员',exact:true}).click();
              const form=panel.getByRole('form',{name:'添加维护单签名人员',exact:true});
              await form.getByLabel('签名人员姓名 *',{exact:true}).fill('临时测试人员');
              assert(await form.getByRole('button',{name:'添加并发送签名链接',exact:true}).isDisabled(),'a recipient is required');
              await form.getByLabel('签名链接接收人',{exact:true}).click();
              await page.locator('.n-base-select-option').getByText('A楼值班账号',{exact:true}).click();
              assert(await form.evaluate(el=>el.scrollWidth<=el.clientWidth+1),'inline form does not overflow');
              await page.screenshot({path:path.join(output,version+'-'+width+'-form.png')});
              await form.getByRole('button',{name:'添加并发送签名链接',exact:true}).click();await form.waitFor({state:'detached'});
              const added=panel.locator('.other-signature-task-list article').filter({hasText:'临时测试人员'});
              await added.waitFor();assert(await added.evaluate(el=>el.classList.contains('pending')),'an invitation is not a saved signature');
              assert.doesNotMatch(await added.innerText(),/已保存/,'an unsigned external person must not be labelled saved');
              invitations[0]={...invitations[0],status:'signed',has_signature:true};
              await page.waitForFunction(()=>[...document.querySelectorAll('.other-signature-task-list article.ready')].some(el=>el.textContent.includes('临时测试人员')));
              assert.equal(invitationWrites.length,1,'automatic signature polling never sends another invitation');
              await page.screenshot({path:path.join(output,version+'-'+width+'.png')});
              await page.reload();await openMopSignatures();
              await page.getByRole('tab',{name:/临时.*外部人员/}).click();
              await page.locator('.other-signature-task-list article.ready').filter({hasText:'临时测试人员'}).waitFor();
              assert.equal(invitationWrites.length,1,'reopening restores the signature without creating or sending again');

              async function reopen(){
                await page.reload();await openMopSignatures();
                await page.getByRole('tab',{name:/临时.*外部人员/}).click();
              }
              async function addVisitor(name,status='sent'){
                nextSendStatus=status;
                await panel.getByRole('button',{name:'添加人员',exact:true}).click();
                await form.getByLabel('签名人员姓名 *',{exact:true}).fill(name);
                await form.getByLabel('签名链接接收人',{exact:true}).click();
                await page.locator('.n-base-select-option').getByText('A楼值班账号',{exact:true}).click();
                await form.getByRole('button',{name:'添加并发送签名链接',exact:true}).click();
              }
              await addVisitor('恢复测试人员','send_failed');
              await form.getByRole('alert').getByText('隔离测试：链接发送失败。',{exact:true}).waitFor();
              const firstAttempt=invitationWrites.at(-1);
              await reopen();
              const recoveryRow=panel.locator('.other-signature-task-list article').filter({hasText:'恢复测试人员'});
              await recoveryRow.getByRole('button',{name:'重试发送链接',exact:true}).click();
              const resumeForm=panel.getByRole('form',{name:'继续维护单签名请求',exact:true});
              assert(await resumeForm.getByLabel('签名人员姓名 *',{exact:true}).isDisabled());
              assert.equal(await resumeForm.getByLabel('签名人员姓名 *',{exact:true}).inputValue(),'恢复测试人员');
              nextSendStatus='sent';
              await resumeForm.getByRole('button',{name:'重试发送链接',exact:true}).click();await resumeForm.waitFor({state:'detached'});
              assert.equal(invitationWrites.at(-1).operation_id,firstAttempt.operation_id,'resume reuses the same person creation');
              assert.notEqual(invitationWrites.at(-1).send_operation_id,firstAttempt.send_operation_id,'explicit retry gets one fresh send id');
              assert.equal(invitations.length,2,'failed delivery recovery never creates another person');

              let current=invitations.find(person=>person.name==='恢复测试人员');
              Object.assign(current,{request_status:'expired',signature_reason:'签名链接已过期，请重新发送链接。'});
              await reopen();await recoveryRow.getByText('签名链接已过期，请重新发送链接。',{exact:true}).waitFor();
              await recoveryRow.getByRole('button',{name:'重新发送链接',exact:true}).click();
              const beforeRenew=invitationWrites.length;
              await resumeForm.getByRole('button',{name:'重新发送链接',exact:true}).click();await resumeForm.waitFor({state:'detached'});
              assert.equal(invitationWrites.length,beforeRenew+1);assert.equal(invitations.length,2);
              assert.equal(invitationWrites.at(-1).operation_id,firstAttempt.operation_id);

              current=invitations.find(person=>person.name==='恢复测试人员');
              Object.assign(current,{recipient:'',request_status:'send_unknown',signature_reason:'消息发送结果未确认，请先核对原请求。'});
              await reopen();await recoveryRow.getByRole('button',{name:'核对发送结果',exact:true}).click();
              const beforeCheck=invitationWrites.length;
              await resumeForm.getByRole('button',{name:'核对发送结果',exact:true}).click();
              await resumeForm.getByRole('alert').waitFor();
              assert.equal(invitationWrites.length,beforeCheck,'unknown results are checked read-only, including legacy requests without a saved recipient');
              Object.assign(current,{request_status:'expired',signature_reason:'签名链接已过期，请重新发送链接。'});
              await resumeForm.getByRole('button',{name:'核对发送结果',exact:true}).click();
              assert.equal(invitationWrites.length,beforeCheck,'checking an expired result does not automatically resend');
              await resumeForm.getByLabel('签名链接接收人',{exact:true}).click();
              await page.locator('.n-base-select-option').getByText('A楼值班账号',{exact:true}).click();
              await resumeForm.getByRole('button',{name:'重新发送链接',exact:true}).click();await resumeForm.waitFor({state:'detached'});
              assert.equal(invitationWrites.length,beforeCheck+1);assert.equal(invitations.length,2);

              loseNextReply=true;await addVisitor('回执恢复人员');
              await form.getByRole('button',{name:'核对发送结果',exact:true}).waitFor();
              const beforeLostCheck=invitationWrites.length;
              await form.getByRole('button',{name:'核对发送结果',exact:true}).click();await form.waitFor({state:'detached'});
              assert.equal(invitationWrites.length,beforeLostCheck,'lost responses recover by reading the original request, never by sending again');
              assert.equal(invitations.length,3);
              await page.screenshot({path:path.join(output,version+'-'+width+'-recovered.png')});
            }
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
  console.log('PASS: legacy/V2 and desktop/mobile MOP/guard pickers; inline invitations; failed delivery recovery after reload; expired-link renewal; unknown/lost reply read-only checks; stable creation IDs and no implicit resends. APIs mocked.');
}finally{await browser.close();}
