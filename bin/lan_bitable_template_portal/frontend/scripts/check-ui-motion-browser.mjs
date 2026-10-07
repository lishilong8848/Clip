import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../..', 'output/playwright/ui-motion');
await mkdir(output, { recursive: true });
const targets = {
  AdminTools: ['admin-shell'], CabinetPowerBatchPage: ['row-editor-overlay', 'image-preview'],
  CabinetPowerPage: ['scrim', 'evidence-preview'], ConfirmDialog: ['confirm-backdrop'],
  CriticalGuardPage: ['image-viewer', 'template-editor-overlay'], CriticalGuardSignatureDrawer: ['guard-signature-backdrop'],
  DailyTaskChecklistPage: ['send-dialog-backdrop'], EngineerMopPage: ['signature-manager-backdrop'],
  EventManagementPage: ['event-metric-backdrop', 'event-drawer-backdrop'], HistoryMemoryPage: ['history-review-backdrop'],
  LearningPage: ['learning-overlay'], PlanConvergencePage: ['pc-overlay'], PlanConvergenceRules: ['pc-modal-overlay'],
  LighthouseBotSettings: ['bot-settings-backdrop'],
  RecordPickerDialog: ['record-picker-backdrop'], RefreshDataMenu: ['refresh-menu-panel'],
  RepairManagementPage: ['repair-project-overlay'], RepairPeoplePicker: ['repair-people-popover'],
  RepairTaskCenter: ['repair-task-overlay'], VnetSelect: ['vnet-select-menu'],
  WaterManagementPage: ['drawer-backdrop', 'lightbox', 'abnormal-note-backdrop'],
};
const { parse: parseSFC } = await import('@vue/compiler-sfc');
const { parse: parseTemplate } = await import('@vue/compiler-dom');
for (const [file, classes] of Object.entries(targets)) {
  const source = await readFile(path.join(root, 'src/components', file + '.vue'), 'utf8');
  const template = parseSFC(source).descriptor.template;
  const found = new Set();
  function visit(node, animated = false) {
    animated ||= node.tag === 'UiTransition';
    const cls = node.props?.find(prop => prop.type === 6 && prop.name === 'class')?.value?.content || '';
    for (const name of classes) if (cls.split(/\s+/).includes(name)) {
      assert(animated, `${file}: ${name} lacks an open/close transition`); found.add(name);
    }
    for (const child of node.children || []) visit(child, animated);
  }
  visit(parseTemplate(template.content));
  assert.equal(found.size, classes.length);
}

const legacySource = await readFile(path.resolve(root, '../workbench_lite.py'), 'utf8');
const legacyStart = legacySource.indexOf('    .end-check-backdrop {{');
const legacyEnd = legacySource.indexOf('    .end-check-dialog {{', legacyStart);
assert(legacyStart > 0 && legacyEnd > legacyStart);
const legacyStyles = legacySource.slice(legacyStart, legacyEnd).replaceAll('{{','{').replaceAll('}}','}');
const html = `<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>*{box-sizing:border-box}body{margin:0;padding:24px;font:14px 'Microsoft YaHei',sans-serif}button{min-height:36px;margin:3px}.page-content{position:relative} .sample-page{min-height:100px;background:#edf5fa;padding:20px}.guard-signature-drawer{max-width:90vw}.model-settings{min-width:0}dialog{border:1px solid #ccc;padding:24px}dialog::backdrop{background:#2346}</style></head>
<body><style>${legacyStyles}@media(prefers-reduced-motion:reduce){.end-check-backdrop,.end-check-backdrop>section{transition:none!important}}</style><button id="legacy-trigger" onclick="document.getElementById('legacy-dialog').hidden=false">通告弹窗</button><div id="legacy-dialog" class="end-check-backdrop" hidden><section style="background:white;padding:24px"><button id="legacy-close" onclick="document.getElementById('legacy-dialog').hidden=true">关闭通告弹窗</button></section></div><div id="app"></div><script type="module">
import {createApp,ref,h,nextTick} from 'vue';
import UiTransition from '/src/components/UiTransition.vue';
import LoadingIndicator from '/src/components/LoadingIndicator.vue';
import ConfirmDialog from '/src/components/ConfirmDialog.vue';
import Drawer from '/src/components/CriticalGuardSignatureDrawer.vue';
import Picker from '/src/components/RecordPickerDialog.vue';
import VnetSelect from '/src/components/VnetSelect.vue';
import TextFill from '/src/components/CabinetBatchTextFill.vue';
import {isLoadingText} from '/src/loadingText';
import '/src/global.css';
for(const text of ['正在进入维修管理','正在读取平面图…','保存中','正在准备导出数据…'])if(!isLoadingText(text))throw Error('Missing activity state');
for(const text of ['保存失败','正在检修','加载超时','已保存'])if(isLoadingText(text))throw Error('False activity state');
const modal=ref(false),drawer=ref(false),picker=ref(false),fill=ref(false),fillMounted=ref(false),view=ref('A');
window.resolveCount=0;window.openModal=()=>{modal.value=true};window.closeModal=()=>{modal.value=false};
window.openFill=()=>{fillMounted.value=true;fill.value=true};window.closeFill=()=>{fill.value=false};
const app=createApp({render(){return h('div',{class:'app-shell'},[
 h('button',{id:'dialog-trigger',onClick:()=>modal.value=true},'弹窗'),
 h('button',{id:'drawer-trigger',onClick:()=>drawer.value=true},'抽屉'),
 h('button',{id:'picker-trigger',onClick:()=>picker.value=true},'记录选择'),
 h('button',{id:'native-trigger',onClick:()=>document.querySelector('dialog').showModal()},'原生弹窗'),
 h('button',{id:'text-trigger',onClick:window.openFill},'文本识别'),
 h('button',{id:'page-trigger',onClick:()=>view.value=view.value==='A'?'B':'A'},'切换页面'),
 h(VnetSelect,{inputId:'sample-menu',modelValue:'A',options:['A','B','C'],label:'选项'}),
 h(LoadingIndicator,{},()=> '正在加载'),
 h('div',{class:'page-content'},h(UiTransition,{name:'ui-page'},{default:()=>h('section',{key:view.value,class:'sample-page'},'页面 '+view.value)})),
 h(ConfirmDialog,{open:modal.value,title:'确认操作',message:'测试内容',onResolve:()=>{window.resolveCount++;modal.value=false}}),
 h(Drawer,{open:drawer.value,scope:'E',contextKey:'fixture',currentUserOpenId:'fixture',taskTitle:'隔离测试',initialSigners:[],onClose:()=>drawer.value=false}),
 h(Picker,{open:picker.value,title:'选择记录',records:[],columns:[],loading:true,onClose:()=>picker.value=false}),
 fillMounted.value?h(TextFill,{batchId:'fixture',open:fill.value,onClose:window.closeFill,onClosed:()=>{if(!fill.value)fillMounted.value=false}}):null,
 h('dialog',{},[h('strong',{},'原生弹窗'),h('button',{id:'native-close',onClick:()=>document.querySelector('dialog').close()},'关闭')]),
 ])}}).component('UiTransition',UiTransition).component('LoadingIndicator',LoadingIndicator);
app.mount('#app');window.ready=true;window.unmountApp=()=>app.unmount();
</script></body></html>`;
const server = await createServer({ root, logLevel: 'error', server: { host: '127.0.0.1', port: 0, hmr: false },
  plugins: [{ name: 'motion-check', configureServer(dev) {
    dev.middlewares.use('/__motion-check', async (_req, res) => {
      res.setHeader('Content-Type', 'text/html'); res.end(await dev.transformIndexHtml('/__motion-check', html));
    });
  } }],
});
let browser;
try {
  await server.listen();
  const base = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
  for (const width of [1440, 390]) for (const reducedMotion of ['no-preference', 'reduce']) {
    const context = await browser.newContext({ viewport: { width, height: 900 }, reducedMotion });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error' || message.type() === 'warning' && /Transition|non-element root/.test(message.text())) errors.push(message.text()); });
    page.on('requestfailed', request => errors.push(request.url() + ': ' + request.failure()?.errorText));
    await page.route('**/api/**', route => new URL(route.request().url()).pathname.startsWith('/api/')
      ? route.fulfill({ json: { ok: true, data: { people: [], records: [] } } }) : route.fallback());
    await page.goto(base + '/__motion-check');
    await page.waitForFunction(() => window.ready, null, { timeout: 10000 }).catch(error => { throw new Error(JSON.stringify(errors) + '\n' + error.message); });
    const spinner = page.locator('.loading-spinner').first();
    const first = await spinner.evaluate(el => getComputedStyle(el).transform);
    await page.waitForTimeout(100);
    assert.notEqual(await spinner.evaluate(el => getComputedStyle(el).transform), first);
    for (const [trigger, selector, closeLabel] of [
      ['#dialog-trigger','.confirm-backdrop','取消'],
      ['#drawer-trigger','.guard-signature-backdrop','关闭检查人选择'],
      ['#picker-trigger','.record-picker-backdrop','关闭'],
      ['#text-trigger','.text-fill-backdrop','关闭文本识别'],
    ]) {
      await page.locator(trigger).click();
      const overlay = page.locator(selector);
      await overlay.waitFor();
      if (reducedMotion === 'no-preference') {
        const animated = await overlay.evaluate(el => getComputedStyle(el).transitionDuration);
        assert(animated.includes('0.18s'), selector + ': entrance must have motion');
      }
      await page.waitForTimeout(220);
      await overlay.getByRole('button',{name:closeLabel,exact:true}).click();
      if (reducedMotion === 'no-preference') {
        const state = await overlay.evaluate(el => ({ connected: el.isConnected, inert: el.inert, pointerEvents: getComputedStyle(el).pointerEvents }));
        assert.equal(state.connected, true, selector + ': exit must not remove instantly');
        assert.equal(state.inert, true);
        assert.equal(state.pointerEvents, 'none');
      }
      await overlay.waitFor({ state:'detached' });
      assert.equal(await page.evaluate(()=>document.body.style.overflow), '', 'modal scroll lock released');
    }
    await page.locator('#sample-menu').click();
    const menu=page.locator('.vnet-select-menu'); await menu.waitFor();
    await page.keyboard.press('Escape');
    await menu.waitFor({state:'detached'});
    await page.evaluate(()=>{window.openModal();});
    await page.waitForTimeout(240);
    await page.evaluate(()=>{window.closeModal();setTimeout(window.openModal,40);});
    await page.waitForTimeout(280);
    assert.equal(await page.locator('.confirm-backdrop').count(),1);
    assert.equal(await page.locator('.confirm-backdrop').evaluate(el=>el.inert),false,'reopened dialog is interactive');
    await page.locator('.confirm-backdrop').getByRole('button',{name:'取消',exact:true}).click();
    await page.locator('.confirm-backdrop').waitFor({state:'detached'});
    await page.evaluate(()=>window.openFill());
    await page.waitForTimeout(240);
    await page.evaluate(()=>{window.closeFill();setTimeout(window.openFill,40);});
    await page.waitForTimeout(280);
    assert.equal(await page.locator('.text-fill-backdrop').evaluate(el=>el.inert),false);
    assert.equal(await page.evaluate(()=>document.body.style.overflow),'hidden','reopened text dialog restores scroll lock');
    await page.locator('.text-fill-backdrop').getByRole('button',{name:'关闭文本识别',exact:true}).click();
    await page.locator('.text-fill-backdrop').waitFor({state:'detached'});
    await page.locator('#native-trigger').click();
    const native=page.locator('dialog'); await native.waitFor();
    await page.waitForTimeout(240);
    await page.locator('#native-close').click();
    await page.waitForTimeout(240);
    assert.equal(await native.evaluate(el=>el.open),false);
    await page.locator('#legacy-trigger').click();
    const legacy=page.locator('#legacy-dialog'); await legacy.waitFor();
    await page.waitForTimeout(240);
    assert.equal(await legacy.evaluate(el=>getComputedStyle(el).opacity),'1');
    await page.locator('#legacy-close').click();
    if(reducedMotion==='no-preference') {
      const state=await legacy.evaluate(el=>({display:getComputedStyle(el).display,pointerEvents:getComputedStyle(el).pointerEvents,hidden:el.hidden}));
      assert(state.hidden && state.display!=='none' && state.pointerEvents==='none','legacy closing is visible but not interactive');
    }
    await legacy.waitFor({state:'hidden'});
    await page.locator('#page-trigger').click();
    await page.waitForTimeout(240);
    assert.equal(await page.locator('.sample-page').count(),1,'no duplicate page retained');
    assert.equal(await page.locator('.sample-page').innerText(),'页面 B');
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth), 'no horizontal overflow');
    await page.screenshot({path:path.join(output,`motion-${width}-${reducedMotion}.png`),fullPage:true});
    await page.evaluate(()=>window.unmountApp());
    assert.equal(await page.locator('.confirm-backdrop,.guard-signature-backdrop,.record-picker-backdrop,.text-fill-backdrop,.vnet-select-menu').count(),0);
    assert.deepEqual(errors,[]);
    console.log(`UI motion/loading PASS width=${width} preference=${reducedMotion}`);
    await context.close();
  }
} finally { await browser?.close(); await server.close(); }
