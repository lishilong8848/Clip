import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { compileScript, parse } from '@vue/compiler-sfc';

// Install before the workspace-business-imports and Vue plugins, in the V2 config only.
// Original module IDs retain the host's API, navigation and lazy-loading resolution.
const screens = {
  ToolSelfCheckPage: ['ToolDeskPage', 'ToolDeskLibrary', 'ToolDeskOrder'],
  ToolWorkspace: ['ToolDeskWorkspace', 'ToolDeskSelection', 'ToolDeskSession', 'ToolDeskSheet', 'ToolDeskSheetPreview', 'ToolDeskReview'],
  ToolManagementSettings: ['ToolDeskSettings'],
  ToolAssetHistory: ['ToolDeskHistory'],
  ToolInspectionReportFields: ['ToolDeskInspection'],
};
const views = new URL('../src/workspace-v2/', import.meta.url);

export function adaptToolDesk(source, id, watchFile = () => {}) {
  const name = id.replaceAll('\\', '/').match(/\/src\/components\/(\w+)\.vue$/)?.[1];
  const files = screens[name];
  if (!files) return null;
  const { descriptor } = parse(source, { filename: id });
  if (!descriptor.scriptSetup) throw new Error('Missing tool controller: ' + name);
  const bindings = compileScript(descriptor, { id: name }).bindings;
  const exposed = new Set();
  for (const file of files) {
    const path = fileURLToPath(new URL(file + '.vue', views));
    watchFile(path);
    const view = readFileSync(path, 'utf8');
    for (const [, key] of view.matchAll(/\br\.([A-Za-z_$][\w$]*)/g)) {
      if (!(key in bindings)) throw new Error(file + ': missing controller binding ' + key);
      exposed.add(key);
    }
  }
  // Preserve named JS exports, including inspectionReady, as well as the public component contract.
  const ordinary = descriptor.script ? '<script lang="ts">' + descriptor.script.content + '</script>' : '';
  return `<template><DeskView :r="deskController" /></template>
${ordinary}
<script setup lang="ts">
import { proxyRefs } from 'vue';
import DeskView from '../workspace-v2/${files[0]}.vue';
${descriptor.scriptSetup.content}
const deskController = proxyRefs({ ${[...exposed].sort().join(', ')} });
</script>`;
}

export default function workspaceToolDesign() {
  return {
    name: 'workspace-tool-design',
    enforce: 'pre',
    transform(code, id) {
      const result = adaptToolDesk(code, id, path => this.addWatchFile(path));
      return result === null ? null : { code: result, map: null };
    },
  };
}


// From the frontend directory: node scripts/workspace-tool-design.mjs --check-pairing
// Everything, including the Vite output, stays in memory or a fresh temporary capture directory.
async function checkPairing() {
  const { build } = await import('vite');
  const { default: vue } = await import('@vitejs/plugin-vue');
  const { chromium } = await import('playwright');
  const { createServer } = await import('node:http');
  const { mkdtemp, writeFile } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const { default: assert } = await import('node:assert/strict');
  const root = fileURLToPath(new URL('../', import.meta.url));
  const output = await mkdtemp(join(tmpdir(), 'tool-desk-pairing-'));
  const entry = 'virtual:tool-desk-pairing';
  const bundle = await build({
    root, configFile: false, publicDir: false, logLevel: 'error', cacheDir: join(output,'cache'),
    plugins: [workspaceToolDesign(), vue(), {
      name: 'isolated-pairing-entry',
      resolveId(id) { if (id === entry) return '\0' + entry; },
      load(id) {
        if (id === '\0' + entry) return `import {createApp,h} from 'vue';
          import Page from '/src/components/ToolSelfCheckPage.vue';
          createApp({render:()=>h(Page,{embedded:true,scope:'ALL'})}).mount('#app');`;
      },
    }],
    build: {write:false, cssCodeSplit:false, rollupOptions:{input:entry}},
  });
  const files = new Map(bundle.output.map(f => ['/'+f.fileName,f]));
  const js = bundle.output.find(f => f.type==='chunk' && f.isEntry).fileName;
  const css = bundle.output.filter(f => f.fileName.endsWith('.css'))
    .map(f => '<link rel="stylesheet" href="/'+f.fileName+'">').join('');
  const templates = [{id:'electrical',name:'电气仪表维护确认单'}, {id:'hvac',name:'暖通仪表维护确认单'}];
  const assets = [
    {id:'recClamp1',name:'交流钳形表',scope:'D',model:'C-01',location:'配电间'},
    {id:'recClamp2',name:'数字万用表',scope:'D',model:'M-02',location:'电气工具柜'},
    {id:'recMeter1',name:'温湿度记录仪',scope:'E',model:'H-03',location:'暖通工具柜'},
    {id:'recMeter2',name:'接触式温度计',scope:'E',model:'T-04',location:'冷站'},
  ].map(a => ({...a,self_cycle:'每月',eligible_kinds:['self']}));
  const expected = Object.fromEntries(assets.map((a,i)=>[a.id, templates[Math.floor(i/2)].id]));
  function layout(template) {
    const sheet = template.name;
    const cell = (address,value,extra={}) => ({address,value,rowspan:1,colspan:1,text_editable:true,...extra});
    const cover={name:'封面',columns:[140,120,80],images:[],rows:[
      {index:1,height:32,cells:[cell('A1','自检封面',{colspan:2}),cell('C1','')]},
      {index:2,height:32,cells:[cell('A2','附件'),cell('B2','[][]'),cell('C2','')]},
    ]};
    const stamp='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jh98AAAAASUVORK5CYII=';
    return {schema:{sheet,review_row:4,result_row:3},sheets:[cover,{
      name:sheet,columns:[100,116,72,68,132,96,128],
      review:{reviewer_address:'E4',reviewed_at_address:'G4',signature_address:'E4'},
      titles:[{key:'@title0',text:sheet,x:8,y:160,width:340,height:26,size:18}],
      images:[{address:'C2',src:stamp,offsetX:4,offsetY:3,width:18,height:12}],
      rows:[
        {index:1,height:30,cells:[cell('A1','实测读数'),cell('B1','',{editable:true}),cell('C1','仪表自检表头',{colspan:3,style:{fontWeight:'bold',backgroundColor:'#eee'}})]},
        {index:2,height:34,cells:[cell('A2','外观及功能'),cell('B2','是[ ]　否[ ]',{editable:true,check:true}),
          cell('C2','附件[][]［ ］☐'),cell('F2','年 月 日 时',{input_kind:'datetime-local'}),cell('G2','年 月 日 时 分',{input_kind:'datetime-local'})]},
        {index:3,height:40,cells:[cell('A3','完成情况',{rowspan:2}),cell('E3','已完成[ ]　未完成[ ]',{colspan:3})]},
        {index:4,height:48,cells:[cell('C4','审核人'),cell('E4','',{colspan:2}),cell('G4','')]},
        {index:5,height:40,cells:[cell('A5','',{colspan:7})]},
      ],
    }]};
  }
  const api = '/api/tool-self-check', workspace = api+'/workspace';
  let session, opening, saved, fixtureSignature;
  const failures = [];
  const server = createServer(async (req,res) => {
    const pathname = new URL(req.url,'http://localhost').pathname;
    const ok = data => { res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({ok:true,data})); };
    try {
      if (pathname==='/') {
        res.writeHead(200,{'Content-Type':'text/html;charset=utf-8','Content-Security-Policy':"connect-src 'self'; object-src 'none'"});
        return res.end('<!doctype html><html lang="zh"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>工具配对填写</title>'+css+
          '<style>body{margin:0;background:#f3f6f5;font:15px/1.5 "Microsoft YaHei",sans-serif}</style><div id="app"></div><script type="module" src="/'+js+'"></script></html>');
      }
      const asset = files.get(pathname);
      if (asset) {res.writeHead(200,{'Content-Type':asset.type==='chunk'?'text/javascript':'text/css'});return res.end(asset.type==='chunk'?asset.code:asset.source);}
      if (pathname==='/favicon.ico') {res.writeHead(204);return res.end();}
      if (req.method==='GET') {
        if (pathname===api) return ok({month:'2026-11',followups:[],templates:[],can_manage_templates:true,inventory_error:''});
        if (pathname===workspace) return ok({assets,templates,sessions:session?[{id:'signed',kind:'self',status:'archived',item_count:1}]:[]});
        if (pathname===workspace+'/signed' && session) {
          const item=session.items[0];
          return ok({...session,id:'signed',status:'archived',can_edit:false,items:[{...item,can_edit:false,reviewer:'隔离审核员',
            reviewed_at:'2026-11-19T08:30',signature:fixtureSignature}]});
        }
        if (pathname===workspace+'/pairing/people') return ok({D:[],E:[]});
        if (pathname===workspace+'/pairing') return ok(session);
      }
      const chunks=[];for await(const chunk of req)chunks.push(chunk);
      const body=chunks.length?JSON.parse(Buffer.concat(chunks)):null;
      if (req.method==='POST' && pathname===workspace+'/open') {
        opening=body;
        assert.deepEqual(body.template_ids,expected);
        assert.equal(body.new_record,true);
        session={id:'pairing',kind:'self',status:'draft',revision:1,can_edit:true,items:assets.map(a=>({
          id:a.id,asset_id:a.id,asset:a,can_edit:true,order_revision:1,values:{},checks:{},
          outcome:'',reviewer:'',signature:'',reviewed_at:'',qualified:null,
          template:layout(templates.find(t=>t.id===expected[a.id])),
        }))};
        return ok(session);
      }
      if(req.method==='POST' && pathname===workspace+'/pairing/save') {
        assert.equal(body.revision,session.revision);
        saved=body.items;
        for(const patch of saved) Object.assign(session.items.find(item=>item.id===patch.id),patch);
        session.revision++;
        return ok(session);
      }
      throw Error('Unexpected fixture request '+req.method+' '+pathname);
    } catch(err) {failures.push(String(err));res.writeHead(500);res.end(JSON.stringify({ok:false,error:String(err)}));}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  let browser;
  try {
    const origin='http://127.0.0.1:'+server.address().port;
    browser=await chromium.launch();
    const page=await browser.newPage({viewport:{width:1440,height:880}});
    page.on('pageerror',e=>failures.push(e.message));
    await page.route('**/*',route=>{
      if(new URL(route.request().url()).origin===origin)return route.continue();
      failures.push('Non-local request');return route.abort();
    });
    const button=name=>page.getByRole('button',{name,exact:true});
    await page.goto(origin);
    fixtureSignature=await page.evaluate(()=>{
      const pad=document.createElement('canvas');
      pad.width=180;pad.height=44;
      const pen=pad.getContext('2d');
      pen.strokeStyle='#263943';pen.lineWidth=3;pen.lineCap='round';pen.beginPath();
      pen.moveTo(6,32);pen.bezierCurveTo(42,6,18,4,25,32);pen.bezierCurveTo(70,-6,43,47,72,20);
      pen.quadraticCurveTo(95,34,144,18);pen.stroke();
      return pad.toDataURL();
    });
    for(const t of templates) {
      const group=assets.filter(a=>expected[a.id]===t.id);
      for(const a of group)await page.getByRole('checkbox',{name:'选择'+a.name,exact:true}).check();
      await button('配对至'+t.name).click();
      const basket=page.locator('[data-group="'+t.id+'"]');
      for(const a of group)await basket.getByRole('button',{name:'移除'+a.name,exact:true}).waitFor();
      for(const a of assets.filter(a=>expected[a.id]!==t.id))assert(!(await basket.innerText()).includes(a.name));
    }
    assert.equal(await page.locator('.pair-group').count(),2);
    const refresh=await button('刷新').boundingBox(), title=await page.getByRole('heading',{level:1}).boundingBox();
    assert(refresh.y<title.y+title.height && title.y<refresh.y+refresh.height,'Desktop title and actions share a line');
    const palette=await page.locator('.pair-templates').boundingBox(), batch=await page.locator('.pair-batch').boundingBox();
    assert(batch.x>=palette.x+palette.width-1 && Math.abs(batch.y-palette.y)<2,'Pairing and batch are side by side');
    await page.locator('.td-page').screenshot({path:join(output,'两组工具配对.png')});
    for (const [width,height,label] of [[820,1180,'平板'],[393,852,'手机']]) {
      await page.setViewportSize({width,height});
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Pairing must fit '+label);
      await page.locator('.td-page').screenshot({path:join(output,label+'两组工具配对.png')});
    }
    await page.setViewportSize({width:1440,height:880});
    await button('开始填写自检').click();
    await page.getByRole('heading',{name:'表单确认行'}).waitFor();
    assert.equal(opening.asset_ids.length,4);
    for(const [i,a] of assets.entries()){
      await page.getByLabel('当前工具').selectOption(a.id);
      const sheetName=templates.find(t=>t.id===expected[a.id]).name;
      assert.equal(await page.getByLabel('工作表',{exact:true}).inputValue(),sheetName);
      const reading=page.locator('.sheet-fields input[type=text]').first();
      assert.equal(await reading.inputValue(),'','Values must start independent even within one template');
      await reading.fill(String(101+i));
      await page.getByLabel('检查确认 · B2').selectOption('是');
      await page.getByLabel('检查结果').selectOption('normal');
      await page.getByLabel('表内标题',{exact:true}).fill(sheetName+' · 核对');
      const result = i%2?'incomplete':'completed';
      const fieldCompletion = page.locator('label').filter({hasText:/^完成情况/}).locator('select');
      await fieldCompletion.selectOption(result);
      await button('表格预览').click();
      const paper=page.getByRole('region',{name:'表格预览',exact:true});
      const at=address=>paper.locator('[data-address="'+address+'"]');
      assert.equal(await at('B1').innerText(),String(101+i));
      assert.equal(await at('B2').innerText(),'是[√]　否[ ]');
      if (i===0) {
        const edit=async(address,act)=>{
          await button('编辑 '+sheetName+'!'+address).click();
          const modal=page.getByRole('dialog',{name:'填写 '+sheetName+'!'+address,exact:true});
          await act(modal);await modal.getByRole('button',{name:'关闭弹窗'}).click();
        };
        await edit('C1', modal=>modal.getByRole('textbox').fill('合并表头'));
        await edit('A5', modal=>modal.getByRole('textbox').fill('空白锚点'));
        await edit('C2', async modal=>{
          const boxes=modal.getByRole('checkbox');
          assert.equal(await boxes.count(),4);
          await boxes.nth(1).check();assert(!(await boxes.nth(0).isChecked()));
          await boxes.nth(0).check();assert(await boxes.nth(1).isChecked());
          await boxes.nth(2).check();await boxes.nth(3).check();
          await boxes.nth(1).uncheck();
          assert(await boxes.nth(0).isChecked() && await boxes.nth(2).isChecked());
        });
        for(const addr of ['F2','G2']) await edit(addr, modal=>modal.locator('input[type=datetime-local]').fill('2026-11-20T14:37'));
        await edit('F2',async modal=>assert.equal(await modal.locator('input').inputValue(),'2026-11-20T14:37'));
        assert.equal(await at('F2').innerText(),'2026年11月20日14时37分');
        assert.equal(await at('G2').innerText(),'2026年11月20日14时37分');
        await button('编辑表格标题 @title0').click();
        const titleEditor=page.getByRole('dialog');await titleEditor.getByRole('textbox').fill(sheetName+' · 核对');
        await titleEditor.getByRole('button',{name:'关闭弹窗'}).click();
        const sheetPicker=page.getByLabel('工作表',{exact:true});
        await sheetPicker.selectOption('封面');
        await button('编辑 封面!A1').click();
        const coverEditor=page.getByRole('dialog');
        await coverEditor.getByRole('textbox').fill('本批校验封面');
        await coverEditor.getByRole('button',{name:'关闭弹窗'}).click();
        await button('编辑 封面!B2').click();
        await coverEditor.getByRole('checkbox').nth(1).check();
        await coverEditor.getByRole('button',{name:'关闭弹窗'}).click();
        assert.equal(await at('B2').innerText(),'[][√]');
        await sheetPicker.selectOption(sheetName);
      }
      if(i===0){
        await page.setViewportSize({width:393,height:852});
        assert(await paper.evaluate(el=>el.scrollWidth>el.clientWidth),'Wide paper scrolls within its own pane');
        assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
        await at('E3').getByRole('radio',{name:'未完成',exact:true}).check();
        await at('E3').getByRole('radio',{name:'已完成',exact:true}).check();
        await page.setViewportSize({width:1440,height:880});
      }
      assert.equal(saved,undefined,'Switching views never submits or persists a separate draft');
      assert.equal(await at('E3').getByRole('radio',{name:i%2?'未完成':'已完成',exact:true}).isChecked(),true);
      // Both original completion choices must update the item, including an explicit override of the computed result.
      await at('E3').getByRole('radio',{name:'未完成',exact:true}).check();
      await at('E3').getByRole('radio',{name:'已完成',exact:true}).check();
      if(i%2)await at('E3').getByRole('radio',{name:'未完成',exact:true}).check();
      const header=await at('C1').boundingBox(), origin=await at('A1').boundingBox(), merged=await at('A3').boundingBox();
      assert.equal(header.x-origin.x,216);assert.equal(header.width,272);assert.equal(merged.height,88);
      const picture=await paper.locator('.paper-image').boundingBox(),title=await paper.locator('.paper-title').boundingBox();
      assert.equal(picture.x-origin.x,220);assert.equal(picture.y-origin.y,33);
      assert.equal(title.y-origin.y,160);assert.equal(await paper.locator('.paper-title').innerText(),sheetName+' · 核对');
      if(i===0)await paper.screenshot({path:join(output,'原表填写预览.png')});
      await button('字段填写').click();
      assert.equal(await fieldCompletion.inputValue(),result);
      assert.equal(await reading.inputValue(),String(101+i));
      assert.equal(await page.getByLabel('检查确认 · B2').inputValue(),'是');

    }
    await Promise.all([page.waitForResponse(resp=>resp.url().includes('/pairing/save')),button('保存草稿').click()]);
    assert.equal(saved.length,4);
    for(const [i,a] of assets.entries()){
      const sheetName=templates.find(t=>t.id===expected[a.id]).name;
      const item=saved.find(entry=>entry.id===a.id);
      const extra=i===0?{'封面!A1':'本批校验封面','封面!B2':'[][√]',[sheetName+'!C1']:'合并表头',[sheetName+'!A5']:'空白锚点',[sheetName+'!C2']:'附件[√][]［√］☑',
        [sheetName+'!F2']:'2026-11-20T14:37',[sheetName+'!G2']:'2026-11-20T14:37'}:{};
      assert.deepEqual(item.values,{[sheetName+'!B1']:String(101+i),[sheetName+'!@title0']:sheetName+' · 核对',...extra});
      assert.deepEqual(item.checks,{[sheetName+'!B2']:'是'});
      assert.equal(item.outcome,'normal');assert.equal(item.completion,i%2?'incomplete':'completed');
      await page.getByLabel('当前工具').selectOption(a.id);
      assert.equal(await page.locator('.sheet-fields input[type=text]').first().inputValue(),String(101+i));
      if(i===0 || i===2) await page.locator('.td-page').screenshot({path:join(output,sheetName+'填写.png')});
    }
    await button('← 返回工具选择').click();
    await page.getByLabel('继续已有批次').selectOption('signed');
    await button('表格预览').click();
    const signCell=page.locator('.paper-cell[data-address="E4"]');
    assert.match(await signCell.innerText(),/隔离审核员/);
    assert(await signCell.getByRole('img',{name:'审核签名'}).isVisible());
    const signatureImage=signCell.getByRole('img',{name:'审核签名'});
    await signatureImage.evaluate(image=>image.decode());
    assert.equal(await signatureImage.evaluate(image=>image.naturalWidth),180);
    assert.equal(await signatureImage.getAttribute('src'),fixtureSignature);
    await page.getByRole('region',{name:'表格预览',exact:true}).screenshot({path:join(output,'签名原表核对.png')});
    assert.equal(await page.locator('.paper-cell[data-address="G4"]').innerText(),'2026-11-19 08:30');
    assert.equal(await page.locator('.paper-cell[data-address="E3"] input').count(),0);
    assert.equal(await page.locator('.paper-edit').count(),0);
    await page.getByLabel('工作表',{exact:true}).selectOption('封面');
    assert.equal(await page.locator('.paper-cell[data-address="A1"]').innerText(),'本批校验封面');
    assert.equal(await page.locator('.paper-cell[data-address="B2"]').innerText(),'[][√]');
    assert.equal(await page.locator('.paper-edit').count(),0);
    await page.getByRole('region',{name:'表格预览',exact:true}).screenshot({path:join(output,'归档封面勾选.png')});
    assert.deepEqual(failures,[]);
    await writeFile(join(output,'配对核验.json'),JSON.stringify({mapping:opening.template_ids,saved},null,2));
    console.log('PASS 两组工具逐台填写、原表坐标与合并、标题和图片、完成情况双向切换及只读签名核对。',output);
  } finally {try{await browser?.close();}finally{await new Promise(resolve=>server.close(resolve));}}
}

if (process.argv[2]==='--check-pairing' && process.argv[1]===fileURLToPath(import.meta.url)) {
  await checkPairing();
}
