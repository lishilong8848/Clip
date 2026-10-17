import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {layoutApiFixture} from './layout-api-fixture.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(root,'../../../output/playwright/knowledge-selection');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
try{
 for(const variant of ['legacy','v2']){
  const dir=path.resolve(root,process.env.CLIPFLOW_KNOWLEDGE_BUILD_DIR||(variant==='v2'?'../../../output/knowledge-folders-review':'../../../output/knowledge-folders-legacy'));
  const server=await preview({root,logLevel:'error',build:{outDir:dir},preview:{host:'127.0.0.1',port:0,proxy:{}}});
  const origin='http://127.0.0.1:'+server.httpServer.address().port;
  try{
   for(const width of [1440,390]){
    const context=await browser.newContext({viewport:{width,height:width<700?844:1000},reducedMotion:'reduce'});
    const layouts=layoutApiFixture(),errors=[],writes=[];
    const groups=[{id:'a',name:'A楼',parent_id:null},{id:'a-child',name:'电气',parent_id:'a'},{id:'b',name:'B楼',parent_id:null},{id:'b-child',name:'电气',parent_id:'b'}].map(g=>({...g,count:0,owner_name:'验收',created_at:1,can_manage:true}));
    const longName='变压器故障应急处理与现场操作规程及交接班核对资料';
    groups.push({id:'a-long',name:longName,parent_id:'a-child',count:0,owner_name:'验收',created_at:1,can_manage:true});
    const docs=Array.from({length:46},(_,i)=>({id:'d'+i,name:i===45?'其它制度.txt':'操作规程'+i+'.txt',version:1,active_version:1,status:'ready',size:100,chunks:1,can_edit:i>1,owner_name:'验收',group_id:'a-child',updated_at:1}));
    let stale=false,listDelay=0;
    await context.route('**/*',async route=>{
     const req=route.request(),url=new URL(req.url()),p=url.pathname;
     if(url.origin!==origin)return route.abort();
     if(p==='/assets/vnet-logo.png')return route.fulfill({path:path.join(root,'public/assets/vnet-logo.png')});
     if(!p.startsWith('/api/'))return route.continue();
     if(await layouts.handle(route,'knowledge-selection'))return;
     const ok=data=>route.fulfill({json:{ok:true,data}});
     if(p==='/api/auth/status')return ok({logged_in:true,user:{open_id:'knowledge-selection',name:'隔离验收',role:'admin',level:'L0',modules:['knowledge']},scope_options:[{value:'A',label:'A楼'}]});
     if(p==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'knowledge-selection'}});
     if(p==='/api/assistant/knowledge/groups'&&req.method()==='POST'){
      const body=req.postDataJSON();assert(!groups.some(g=>g.name===body.name&&g.parent_id===body.parent_id));
      const group={id:'new'+groups.length,...body,count:0,can_manage:true,created_at:1,owner_name:'验收'};groups.push(group);return ok(group);
     }
     if(p==='/api/assistant/knowledge/groups')return ok({groups,total:groups.length});
     if(p==='/api/assistant/knowledge'){
      if(listDelay)await new Promise(resolve=>setTimeout(resolve,listDelay));
      const q=url.searchParams.get('q')||'',group=url.searchParams.get('group')||'';
      const result=docs.filter(d=>d.name.includes(q)&&(!group||group==='all'||d.group_id===group));
      const page=Math.min(Number(url.searchParams.get('page')||1),Math.max(1,Math.ceil(result.length/20)));
      return ok({items:result.slice((page-1)*20,page*20),total:result.length,page,page_size:20,is_admin:true,
       ...(url.searchParams.has('select_all')?{selection:{q,group,fingerprint:'f'.repeat(64),count:result.filter(d=>d.can_edit).length}}:{})});
     }
     if(p.endsWith('/documents')&&req.method()==='PUT'){
      const body=req.postDataJSON();
      if(stale)return route.fulfill({status:409,json:{ok:false,error:'搜索结果或文件权限已变化，未移动任何文件，请重新全选。'}});
      const selected=body.selection?docs.filter(d=>d.can_edit&&d.name.includes(body.selection.q)&&(!body.selection.group||d.group_id===body.selection.group)&&!body.selection.excluded_ids.includes(d.id)):docs.filter(d=>body.document_ids.includes(d.id));
      assert(selected.every(d=>d.can_edit));writes.push({body,ids:selected.map(d=>d.id)});
      for(const doc of selected)doc.group_id=decodeURIComponent(p.split('/')[5]);
      return ok({updated:selected.length});
     }
     if(req.method()!=='GET')return route.fulfill({status:400,json:{ok:false,error:'unexpected isolated write'}});
     return ok({items:[],settings:{},engine:{},stats:{}});
    });
    const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
    try{
     await page.goto(origin+(variant==='v2'?'/assets/workspace-v2/index.html?__page=/knowledge-base':'/knowledge-base?frontend=legacy'));
     const tree=page.locator('.kb-tree-pane');await tree.locator('button.v2-folder-name[title="A楼 / 电气"]').waitFor();
     assert.equal(await tree.locator('button.v2-folder-name[title="B楼 / 电气"]').count(),1);
     const rootButton=tree.locator('button.v2-folder-name[title="A楼"]'),childButton=tree.locator('button.v2-folder-name[title="A楼 / 电气"]');
     assert((await childButton.boundingBox()).x>(await rootButton.boundingBox()).x,'children must visibly indent in both versions');
     await tree.getByRole('button',{name:'收起全部',exact:true}).click();await childButton.waitFor({state:'hidden'});
     await tree.getByRole('button',{name:'展开全部',exact:true}).click();await childButton.waitFor();
     await childButton.click();await childButton.getAttribute('aria-pressed').then(value=>assert.equal(value,'true'));
     if(variant==='v2'){
      const bulk=page.locator('.kb-select-all'),list=page.locator('.kb-list');await list.waitFor();
      assert((await bulk.boundingBox()).y<(await list.boundingBox()).y,'selection controls stay above files');
      await tree.getByRole('button',{name:'收起全部',exact:true}).click();await rootButton.click();await childButton.waitFor();
      assert.equal(await page.locator('.v2-kb-location').getByText('A楼',{exact:true}).count(),1,'folder selection is immediately visible');
      await childButton.click();await page.locator('.kb-row-main').first().waitFor();
      const longButton=tree.locator('button.v2-folder-name').filter({hasText:longName});await longButton.waitFor();await page.waitForTimeout(250);
      assert(await longButton.locator('span').first().evaluate(el=>getComputedStyle(el).whiteSpace!=='nowrap'&&el.scrollWidth<=el.clientWidth+1),'deep long folder names wrap instead of clipping');
     }
     await tree.screenshot({path:path.join(output,'tree-'+variant+'-'+width+'.png')});
     await tree.getByRole('button',{name:'全部',exact:true}).click();
     for(const building of ['A楼','B楼']){
      await tree.getByRole('button',{name:`在「${building}」下新建子文件夹`,exact:true}).click();
      const modal=page.getByRole('dialog',{name:'新建子文件夹',exact:true});await modal.getByRole('textbox',{name:'子文件夹名称',exact:true}).fill('资料');
      await modal.getByRole('button',{name:'创建',exact:true}).click();await tree.locator(`button.v2-folder-name[title="${building} / 资料"]`).waitFor();
     }
     const search=page.getByRole('searchbox',{name:'搜索知识库',exact:true});
     await search.fill('操作规程');await page.getByRole('button',{name:'全选全部',exact:true}).waitFor();
     await page.getByRole('checkbox',{name:'全选本页',exact:true}).click();await page.getByText('已选 18 个文件',{exact:true}).waitFor();
     await page.getByRole('button',{name:'下一页',exact:true}).click();await page.getByRole('checkbox',{name:'选择操作规程20.txt',exact:true}).click();
     await page.getByText('已选 19 个文件',{exact:true}).waitFor();
     async function target(value,label){
      const select=page.getByRole('combobox',{name:'目标分组',exact:true});
      if(await select.evaluate(el=>el.tagName==='SELECT'))await select.selectOption(value);
      else{await select.click();const picker=page.locator('.kb-group-picker-dialog');if(await picker.count()){await picker.getByTitle(label,{exact:true}).click();await picker.getByRole('button',{name:'选择此分组',exact:true}).click();}else await page.locator('.n-base-select-option').getByText(label,{exact:true}).click();}
     }
     await target('b-child','B楼 / 电气');await page.getByRole('button',{name:'应用分组',exact:true}).click();
     await page.getByText('已将 19 个文件分入「B楼 / 电气」。',{exact:true}).waitFor();assert.equal(writes[0].ids.length,19);
     await page.getByRole('button',{name:'全选全部',exact:true}).click();await page.getByText('已选 43 个文件（全部搜索结果）',{exact:true}).waitFor();
     await page.getByRole('checkbox',{name:'取消选择操作规程20.txt',exact:true}).click();await page.getByText('已选 42 个文件（全部搜索结果）',{exact:true}).waitFor();
     await page.getByRole('button',{name:'末页',exact:true}).click();await page.getByRole('checkbox',{name:'取消选择操作规程44.txt',exact:true}).waitFor();
     await target('a-child','A楼 / 电气');
     assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
     await page.screenshot({path:path.join(output,variant+'-'+width+'.png'),fullPage:true});
     await page.getByRole('button',{name:'应用分组',exact:true}).click();await page.getByText('已将 42 个文件分入「A楼 / 电气」。',{exact:true}).waitFor();
     assert.equal(writes[1].ids.length,42);assert(!writes[1].ids.includes('d20'));assert(!writes[1].ids.includes('d45'));
     stale=true;await page.getByRole('button',{name:'全选全部',exact:true}).click();await target('b-child','B楼 / 电气');
     await page.getByRole('button',{name:'应用分组',exact:true}).click();await page.getByRole('alert').filter({hasText:'请重新全选'}).waitFor();
     assert.equal(writes.length,2);
     await page.getByRole('button',{name:'关闭错误提示',exact:true}).click();
     listDelay=250;
     await Promise.all([page.waitForRequest(req=>new URL(req.url()).pathname==='/api/assistant/knowledge'&&new URL(req.url()).searchParams.get('page')==='1'),page.locator('.kb-pager-nav').getByRole('button',{name:'首页',exact:true}).click()]);
     await search.fill('不存在的文件');await page.waitForTimeout(80);
     assert(await page.getByRole('button',{name:'全选全部',exact:true}).isDisabled(),'aborted page reads must not re-enable stale search selections');
     await page.getByText('没有匹配的文档',{exact:true}).waitFor();
     assert.equal(await page.locator('.kb-batch-bar').count(),0);assert.deepEqual(errors,[]);
     console.log('PASS: '+variant+' '+width+' folder hierarchy, duplicate leaf names, page/all selection, exclusions, atomic stale-result handling.');
    }catch(error){await page.screenshot({path:path.join(output,'failure-'+variant+'-'+width+'.png'),fullPage:true});console.error(errors);throw error;}
    finally{await context.close();}
   }
  }finally{await server.close();}
 }
}finally{await browser.close();}
