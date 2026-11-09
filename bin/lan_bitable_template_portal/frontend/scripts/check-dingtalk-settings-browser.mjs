import assert from 'node:assert/strict';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir} from 'node:fs/promises';
import {createServer} from 'vite';
import {chromium} from 'playwright';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../build_output/dingtalk-browser');
await mkdir(output,{recursive:true});
const server=await createServer({root,server:{host:'127.0.0.1',port:0},plugins:[{name:'ding-fixture',configureServer(s){
  s.middlewares.use('/__ding_fixture',async(_req,res)=>{res.setHeader('Content-Type','text/html');res.end(await s.transformIndexHtml('/__ding_fixture',`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><div id="app"></div><script type="module">
  import {createApp,ref,h} from 'vue'; import Settings from '/src/components/DingTalkNoticeSettings.vue'; import Picker from '/src/components/NoticeDingTalkPicker.vue';
  createApp({setup(){const selected=ref([]),names=ref([]);return ()=>h('main',[h(Settings),h(Picker,{modelValue:selected.value,workType:'change',noticeType:'变更通告',buildings:['D'],'onUpdate:modelValue':v=>selected.value=v,onSelectedNames:v=>names.value=v}),h('p',{id:'selection'},names.value.join('、'))]);}}).mount('#app');
  </script></body></html>`));});}}]});
await server.listen();
const browser=await chromium.launch({headless:true});
try{
  for(const [width,height] of [[390,844],[1440,900]]){
    const page=await browser.newPage({viewport:{width,height}});let writes=[];
    page.on('pageerror',error=>console.error('fixture error:',error.message));
    page.on('console',message=>{if(message.type()==='error')console.error('fixture console:',message.text());});
    let state={revision:1,types:['变更通告'],scopes:['D','E'],deliveries:[],contacts:[],robots:[...['甲群','乙群','丙群'].map((name,index)=>({id:String(index+1).repeat(32),name,remark:'',configured:true,enabled:true,scopes:[],types:[],mention_mode:'none',mention_user_ids:[],mention_contact_ids:[],mention_building_contact_ids:{}}))]};
    await page.route(url=>url.pathname.startsWith('/api/'),async route=>{
      const request=route.request(),url=new URL(request.url());let data;
      if(url.pathname==='/api/dingtalk-notices'){
        if(request.method()==='POST'){const body=request.postDataJSON();writes.push(body);state={...state,revision:state.revision+1,contacts:body.contacts,robots:body.robots.map(r=>({...r,configured:true}))};}
        data=state;
      }else if(url.pathname==='/api/workbench/dingtalk-options')data={robots:state.robots};
      else throw new Error('unexpected API '+url.pathname);
      await route.fulfill({json:{ok:true,data}});
    });
    await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/__ding_fixture`, {waitUntil:'domcontentloaded',timeout:90000});
    try{await page.locator('.robot').first().waitFor({timeout:10000});}catch(e){console.error('fixture body:',(await page.locator('body').innerText()).slice(0,2000));console.error('fixture html:',(await page.content()).slice(0,2000));throw e;}
    const directory = page.locator('.contact-directory');
    await directory.locator('summary').click();
    for (const [remark, mobile] of [['通用主管', '13800138000'], ['现场负责人', '13800138001']]) {
      await directory.getByRole('button', {name:'添加艾特人员'}).click();
      const row = directory.locator('.contact-row').last();
      await row.getByLabel('人员备注').fill(remark);
      await row.getByLabel('手机号').fill(mobile);
    }
    const robot = page.locator('.robot').first();
    const otherRobot = page.locator('.robot').nth(1);
    async function selectPerson(panel, scope, remark) {
      const picker = panel.locator(`[data-mention-scope="${scope}"]`);
      await picker.locator('.n-base-selection').click();
      const visibleOptions = page.locator('.n-base-select-option__content:visible');
      await visibleOptions.filter({hasText:remark}).click();
      await page.mouse.click(1,1);
      await visibleOptions.first().waitFor({state:'hidden'});
      assert.match(await picker.innerText(), new RegExp(remark));
      assert.doesNotMatch(await picker.innerText(), /1380013800[01]/);
    }
    await robot.locator('.mentions summary').click();
    await robot.getByLabel('默认艾特方式').selectOption('people');
    await robot.getByLabel('钉钉用户 ID').fill('person_a\nperson_b');
    await selectPerson(robot, 'common', '通用主管');
    await selectPerson(robot, 'D', '现场负责人');
    await selectPerson(robot, 'E', '现场负责人');
    await otherRobot.locator('.mentions summary').click();
    await otherRobot.getByLabel('默认艾特方式').selectOption('people');
    await selectPerson(otherRobot, 'D', '通用主管');
    assert(await directory.locator('.contact-row').last().getByRole('button', {name:'移除人员'}).isDisabled());
    await directory.locator('summary').click();
    await page.getByRole('button',{name:'保存配置'}).click();
    await page.getByRole('status').filter({hasText:'配置已保存'}).waitFor();
    const [common, floorPerson] = writes[0].contacts;
    assert.deepEqual(writes[0].robots[0].mention_user_ids,['person_a','person_b']);
    assert.deepEqual(writes[0].robots[0].mention_contact_ids,[common.id]);
    assert.deepEqual(writes[0].robots[0].mention_building_contact_ids,{D:[floorPerson.id],E:[floorPerson.id]});
    assert.deepEqual(writes[0].robots[1].mention_building_contact_ids,{D:[common.id]});
    assert.doesNotMatch(JSON.stringify(writes[0].robots), /1380013800[01]/);
    await page.getByRole('button',{name:'刷新',exact:true}).click();
    await page.waitForFunction(() => !document.querySelector('.ding-settings fieldset')?.disabled);
    assert.match(await robot.locator('[data-mention-scope="D"]').innerText(), /现场负责人/);
    assert.match(await otherRobot.locator('[data-mention-scope="D"]').innerText(), /通用主管/);
    await page.locator('.notice-dingtalk label').filter({hasText:'甲群'}).locator('input').check();
    await page.locator('.notice-dingtalk label').filter({hasText:'乙群'}).locator('input').check();
    assert.equal(await page.locator('#selection').textContent(),'甲群、乙群');
    assert.equal(await page.locator('.notice-dingtalk input:checked').count(),2);
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
    await page.screenshot({path:path.join(output,`settings-${width}.png`),fullPage:true});
    console.log(`钉钉配置 ${width}x${height}：通过`);await page.close();
  }
}finally{await browser.close();await server.close();}
