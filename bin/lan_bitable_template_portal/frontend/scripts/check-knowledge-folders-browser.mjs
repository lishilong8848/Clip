import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {preview} from 'vite';
import {chromium} from 'playwright';
const output=path.resolve('../../../output/playwright/knowledge-folders');await mkdir(output,{recursive:true});
const server=await preview({logLevel:'error',preview:{host:'127.0.0.1',port:0,proxy:{}}});
const origin='http://127.0.0.1:'+server.httpServer.address().port,browser=await chromium.launch({headless:true});
const groups=[{id:'root',name:'运维',parent_id:null,count:0,can_manage:true},{id:'child',name:'电气',parent_id:'root',count:2,can_manage:true},{id:'other',name:'制度',parent_id:null,count:0,can_manage:true}];
const docs=['one','two'].map((id,index)=>({id,name:'操作规程'+(index+1)+'.txt',active_version:1,version:1,status:'ready',size:100,chunks:1,can_edit:true,group_id:'child'}));
const writes=[],errors=[];let page;
try{
 page=await browser.newPage({viewport:{width:1440,height:1000},reducedMotion:'reduce'});page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url()),p=url.pathname;if(url.origin!==origin)return route.abort();if(!p.startsWith('/api/'))return route.continue();const ok=data=>route.fulfill({json:{ok:true,data}});
  if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'folder-fixture',name:'隔离验收',role:'admin',level:'L0',modules:['knowledge']},scope_options:[{value:'A',label:'A楼'}]});
  if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'folder-fixture'}});
  if(req.method()!=='GET'){const body=req.postDataJSON();writes.push({p,method:req.method(),body});
   if(p==='/api/assistant/knowledge/groups'&&req.method()==='POST'){const group={id:'new'+writes.length,name:body.name,parent_id:body.parent_id||null,can_manage:true,count:0};groups.push(group);return ok(group);}
   const id=p.split('/')[5];if(req.method()==='PATCH'){Object.assign(groups.find(g=>g.id===id),body);return ok(groups.find(g=>g.id===id));}
   if(p.endsWith('/documents')&&req.method()==='PUT'){for(const doc of docs.filter(d=>body.document_ids.includes(d.id)))doc.group_id=id;return ok({updated:body.document_ids.length});}
   throw new Error('Unexpected isolated write '+p);
  }
  if(p==='/api/assistant/knowledge/groups')return ok({groups,total:groups.length});
  if(p==='/api/assistant/knowledge'){const filter=url.searchParams.get('group_id');const items=filter&&filter!=='all'?docs.filter(d=>d.group_id===filter):docs;return ok({items,total:items.length,page:1,page_size:20,is_admin:true});}
  return ok({items:[],settings:{},engine:{},stats:{}});
 });
 await page.goto(origin+'/assets/workspace-v2/index.html?__page=/knowledge-base');
 const tree=page.locator('.kb-tree-pane');await tree.getByRole('button',{name:'运维',exact:true}).waitFor();
 await tree.getByRole('button',{name:'在「运维」下新建子文件夹',exact:true}).click();
 const dialog=page.getByRole('dialog',{name:'新建子文件夹',exact:true});await dialog.waitFor();assert.match(await dialog.innerText(),/在「运维」下/);
 await dialog.getByRole('textbox',{name:'子文件夹名称'}).fill('暖通');await dialog.getByRole('button',{name:'创建',exact:true}).click();
 await tree.getByRole('button',{name:'暖通',exact:true}).waitFor();assert.equal(writes[0].body.parent_id,'root');
 await tree.getByRole('button',{name:'移动「暖通」',exact:true}).click();
 const move=page.getByRole('dialog',{name:'移动文件夹',exact:true});await move.locator('.n-base-selection').click();
 await page.locator('.n-base-select-option').getByText('运维 / 电气',{exact:true}).click();await move.getByRole('button',{name:'移动',exact:true}).click();
 await page.waitForFunction(()=>!document.querySelector('.n-dialog'));assert.equal(writes.at(-1).body.parent_id,'child');
 await tree.getByRole('button',{name:'移动「暖通」',exact:true}).click();await move.locator('.n-base-selection').click();await page.locator('.n-base-select-option').getByText('根目录',{exact:true}).click();await move.getByRole('button',{name:'移动',exact:true}).click();
 await page.waitForFunction(()=>!document.querySelector('.n-dialog'));assert.equal(writes.at(-1).body.parent_id,null);
 await tree.getByRole('button',{name:'重命名「暖通」',exact:true}).click();await page.keyboard.press('Escape');await page.getByRole('dialog',{name:'重命名文件夹',exact:true}).waitFor({state:'hidden'});
 await tree.getByRole('button',{name:'电气',exact:false}).first().click();await page.locator('.kb-row-main').first().waitFor();
 const boxes=page.locator('.kb-row').getByRole('checkbox');await boxes.nth(0).click();await boxes.nth(1).click();
 await page.getByRole('combobox',{name:'目标分组',exact:true}).click();await page.locator('.n-base-select-option').getByText('制度',{exact:true}).click();await page.getByRole('button',{name:'应用分组',exact:true}).click();
 await page.waitForFunction(()=>document.body.innerText.includes('已将'));assert.equal(writes.at(-1).method,'PUT');assert.deepEqual([...writes.at(-1).body.document_ids].sort(),['one','two']);
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:1000});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(output,'folders-'+width+'.png'),fullPage:true});}
 assert.deepEqual(errors,[]);console.log('PASS: visible nested folders, child create, nested/root moves, Escape close, bulk document moves, PC/mobile, no real business writes.');
}catch(error){if(page)await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors);throw error;}finally{await browser.close();await server.close();}
