import assert from 'node:assert/strict';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { parse, compileTemplate } from '@vue/compiler-sfc';
import { markFeedbackAlerts } from './workspace-feedback-adapter.mjs';

const sample = `<template><NAlert type="error">失败</NAlert><NAlert :type="ok ? 'success' : 'warning'">状态</NAlert></template>`;
const marked = markFeedbackAlerts(sample);
assert.equal(markFeedbackAlerts(marked),marked,'metadata adaptation is idempotent');
assert(marked.includes(':data-v2-feedback-kind="&quot;error&quot;"'));
assert(marked.includes(':data-v2-feedback-kind="ok ? \'success\' : \'warning\'"'));
assert.deepEqual(compileTemplate({source:parse(marked).descriptor.template.content,filename:'fixture.vue',id:'fixture'}).errors,[]);

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/workspace-v2-feedback');
const modulePath = path.join(root, 'src/workspace-v2/feedbackMotion.ts').replaceAll('\\', '/');
const bundle = await build({
  stdin: { contents: `
    import {createApp,h,reactive} from 'vue';
    import {NMessageProvider,NModal,NAlert,NButton,useMessage} from 'naive-ui';
    import {installFeedbackMotion} from ${JSON.stringify(modulePath)};
    const state=reactive({items:[],modal:false,hidden:false,retries:0});
    const Child={setup(){
      const message=useMessage();
      window.drive={state,message,stop:installFeedbackMotion()};
      const notices=()=>state.items.map(item=>h('p',{'data-test':item.id,class:item.tone,key:item.id,style:{display:item.hidden?'none':''}},
        item.action?[item.text,h('button',{onClick:()=>state.retries++},'重试')]:item.text));
      return ()=>h('main',[
        h('input',{id:'password',type:'password',value:'fixture-only'}),
        h('section',{class:'source',style:{display:state.hidden?'none':''}},notices()),
        h('p',{class:'field-hint'},'请填写本次进度'),
        h('div',{class:'answer-feedback'},[h('p',{class:'error'},'这道题答错了')]),
        h('div',{class:'save-conflicts'},[h('p',{class:'warning'},'请选择要保留的版本')]),
        h(NModal,{show:state.modal,preset:'card',title:'编辑通告',style:{width:'480px'},'onUpdate:show':v=>state.modal=v},{default:()=>[
          h('input',{id:'modal-input'}),
          h(NAlert,{type:'error','data-v2-feedback-kind':'error','data-test':'modal-alert'},{default:()=>['附件读取失败',h(NButton,{onClick:()=>state.retries++},()=> '重新读取')]}),
        ]}),
      ]);
    }};
    createApp({render:()=>h(NMessageProvider,{placement:'bottom-right',duration:0,closable:true},()=>h(Child))}).mount('#app');
  `, resolveDir: root, sourcefile: 'feedback-fixture.ts', loader: 'ts' },
  bundle: true, write: false, format: 'iife', platform: 'browser',
  define: { 'process.env.NODE_ENV': '"production"', __VUE_OPTIONS_API__: 'true', __VUE_PROD_DEVTOOLS__: 'false', __VUE_PROD_HYDRATION_MISMATCH_DETAILS__: 'false' },
  logLevel: 'silent',
});
const css = await readFile(path.join(root, 'src/workspace-v2/feedbackToasts.css'), 'utf8');
const browser = await chromium.launch({ headless: true });
const errors = [];
try {
  await mkdir(output, { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route('**/*', route => route.abort());
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  page.setDefaultTimeout(8000);
  async function mount(unsupported = false) {
    await page.goto('about:blank');
    if (unsupported) await page.evaluate(() => { HTMLElement.prototype.showPopover = undefined; });
    await page.setContent('<html class="workspace-v2"><body><div id="app"></div></body></html>');
    await page.addStyleTag({ content: css + '\n.source{transform:translateX(40px);overflow:hidden;width:350px} main{padding:30px;font:14px sans-serif}' });
    await page.addScriptTag({ content: bundle.outputFiles[0].text });
  }
  const set = items => page.evaluate(items => { window.drive.state.items = items; }, items);
  const visible = () => page.locator('[data-v2-feedback-visible="true"]');
  async function assertStack(count) {
    await page.waitForFunction(n => document.querySelectorAll('[data-v2-feedback-visible="true"]').length === n, count);
    await page.waitForTimeout(420);
    const boxes = await visible().evaluateAll(nodes => nodes.map(n => { const b = n.getBoundingClientRect(); return { x:b.x, y:b.y, right:b.right, bottom:b.bottom, width:b.width, height:b.height }; }));
    const size = page.viewportSize();
    for (const b of boxes) {
      assert(b.x >= 0 && b.y >= 0 && b.right <= size.width && b.bottom <= size.height, 'toast stays in viewport');
      assert(b.x > size.width - 410, 'toast stays at the right, outside its clipped source');
    }
    const sorted = boxes.sort((a,b) => a.y - b.y);
    for (let i=1; i<sorted.length; i++) assert(sorted[i].y >= sorted[i-1].bottom, 'toasts never overlap');
  }
  await mount();
  await page.locator('#password').focus();
  await set([{id:'success',tone:'success',text:'保存成功'},{id:'error',tone:'error',text:'保存失败，原填写内容已保留',action:true}]);
  await assertStack(2);
  assert.equal(await page.locator('#password').evaluate(el=>el===document.activeElement),true,'toast does not steal input focus');
  await page.getByRole('button',{name:'重试',exact:true}).click();
  assert.equal(await page.evaluate(()=>window.drive.state.retries),1,'original handler is still connected');
  await page.mouse.move(1,1);
  await page.waitForFunction(()=>document.querySelector('[data-test="success"]').dataset.v2FeedbackVisible==='false',null,{timeout:6500});
  assert.equal(await page.locator('[data-test=error]').isVisible(),true,'error never expires automatically');
  assert.equal(await page.locator('.answer-feedback .error').getAttribute('data-v2-feedback'),null,'answer data remains in place');
  assert.equal(await page.locator('.save-conflicts .warning').getAttribute('data-v2-feedback'),null,'conflict form remains in place');
  await set([{id:'error',tone:'error',text:'新的错误内容'}]);
  await assertStack(1);
  assert.equal(await page.locator('[data-test=error] [data-v2-feedback-icon]').count(),1);
  assert.equal(await page.locator('[data-test=error] [data-v2-feedback-close]').count(),1);
  await page.locator('[data-test=error] [data-v2-feedback-close]').click();
  await page.getByRole('button',{name:'查看已收起的 1 条提示',exact:true}).waitFor();
  assert.equal(await visible().count(),0);
  await page.getByRole('button',{name:'查看已收起的 1 条提示',exact:true}).click();
  await assertStack(1);
  await page.locator('[data-test=error] [data-v2-feedback-close]').click();
  await set([{id:'error',tone:'error',text:'关闭动画中到达的新结果'}]);
  await assertStack(1);
  await set([{id:'error',tone:'error',text:'关闭动画中到达的新结果',hidden:true}]);
  await page.waitForFunction(()=>document.querySelector('[data-test=error]').dataset.v2FeedbackVisible==='false');
  await set(Array.from({length:5},(_,i)=>({id:'n'+i,tone:'warning',text:'待处理提示 '+i})).concat({id:'duplicate',tone:'warning',text:'待处理提示 4'}));
  await assertStack(3);
  await page.getByRole('button',{name:'查看其它 2 条提示',exact:true}).click();
  await assertStack(2);
  await page.evaluate(()=>window.drive.state.hidden=true);
  await page.waitForFunction(()=>document.querySelectorAll('[data-v2-feedback-visible=true]').length===0);
  await page.evaluate(()=>window.drive.state.hidden=false);
  await assertStack(3);
  await set([]);
  await page.waitForFunction(()=>document.querySelectorAll('[data-v2-feedback-visible=true]').length===0);
  await page.evaluate(()=>window.drive.state.modal=true);
  await page.locator('#modal-input').focus();
  await page.locator('[data-test=modal-alert][data-v2-feedback-visible=true]').waitFor();
  assert.equal(await page.locator('[data-test=modal-alert]').getAttribute('data-v2-feedback-tone'),'error');
  await page.getByRole('button',{name:'重新读取',exact:true}).click();
  assert.equal(await page.evaluate(()=>window.drive.state.retries),2,'modal action works in top layer');
  await page.getByRole('button',{name:'重新读取',exact:true}).focus();
  assert.equal(await page.getByRole('button',{name:'重新读取',exact:true}).evaluate(el=>el===document.activeElement),true,'modal focus trap retains original ancestry');
  await page.screenshot({path:path.join(output,'desktop-modal-toast.png')});
  await page.evaluate(()=>window.drive.state.modal=false);
  await page.evaluate(()=>window.drive.message.success('原生消息组件保存成功'));
  await page.locator('.n-message[data-v2-feedback-visible=true]').waitFor();
  await page.mouse.move(1,1);
  await page.waitForFunction(()=>!document.querySelector('.n-message'),null,{timeout:6500});
  await page.emulateMedia({reducedMotion:'reduce'});
  await page.setViewportSize({width:390,height:844});
  await set([{id:'mobile',tone:'error',text:'无法读取通告，请重试。',action:true},{id:'mobile-long',tone:'warning',text:'请核对现场信息。'.repeat(60)}]);
  await assertStack(1);
  assert(await visible().evaluateAll(nodes=>nodes.every(n=>n.getBoundingClientRect().height<=140&&n.getBoundingClientRect().width<=320)),'mobile toast stays compact');
  await page.getByRole('button',{name:'查看其它 1 条提示',exact:true}).click();await assertStack(1);
  assert.equal(await visible().evaluateAll(nodes=>nodes.some(n=>n.getAnimations().length>0)),false,'reduced motion skips movement');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.screenshot({path:path.join(output,'mobile-toasts.png')});
  await set([{id:'mop-warning',tone:'warning',text:'请核对维护单填写内容'}]);
  await page.evaluate(()=>{
    const page=document.createElement('section');page.className='v2-mop-workspace preview-open';
    const footer=document.createElement('div');footer.className='floating-upload-box';footer.style.cssText='position:fixed;bottom:82px;left:12px;right:12px;height:140px';
    page.append(footer);document.body.append(page);
  });
  const aboveFooter=()=>page.waitForFunction(()=>{
    const footer=document.querySelector('.floating-upload-box').getBoundingClientRect();
    return document.querySelector('[data-test="mop-warning"]').getBoundingClientRect().bottom<footer.top;
  });
  await aboveFooter();
  await page.locator('.floating-upload-box').evaluate(el=>el.style.height='220px');
  await aboveFooter();
  await page.locator('.v2-mop-workspace').evaluate(el=>el.remove());
  await page.waitForFunction(()=>parseFloat(document.querySelector('[data-test="mop-warning"]').style.getPropertyValue('--v2-feedback-bottom'))===82);
  await page.evaluate(()=>window.drive.stop());
  assert.equal(await page.locator('[data-v2-feedback]').count(),0,'uninstall removes adaptation');
  await mount(true);
  await set([{id:'fallback',tone:'error',text:'浏览器兼容模式'}]);
  assert.equal(await page.getByText('浏览器兼容模式',{exact:true}).isVisible(),true,'old browsers retain inline feedback');
  assert.equal(await page.locator('[data-test=fallback]').getAttribute('popover'),null);
  assert.deepEqual(errors,[]);
  console.log('PASS: bottom-right placement, clipped ancestors, stacking, dedup, expiry, persistence, recall, Vue updates, focus, modal actions, provider cleanup, mobile, reduced motion, fallback; no business/network requests.');
} finally { await browser.close(); }
