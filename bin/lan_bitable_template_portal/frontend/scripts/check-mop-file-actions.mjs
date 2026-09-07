import assert from "node:assert/strict";
import { mkdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { createServer } from "vite";
import { chromium } from "playwright";

const root = fileURLToPath(new URL("..", import.meta.url));
const output = resolve(root, "../../..", "output/playwright/mop-file-actions");
await mkdir(output, { recursive: true });
const html = `<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>*{box-sizing:border-box}.app-shell{font-family:"Microsoft YaHei",sans-serif}</style></head>
<body class="app-shell"><h1 tabindex="0">MOP 文件操作检查</h1><div id="app"></div><script type="module">
import { createApp, h, reactive, ref, nextTick } from 'vue';
import Panel from '/src/components/MopBindingPanel.vue';
import Footer from '/src/components/MopUploadFooter.vue';
import { downloadFilledEngineerMop } from '/src/mopFileApi.ts';
import { useLocalMopUpload } from '/src/useLocalMopUpload.ts';
import '/src/global.css';
window.uploads=[];window.errors=[];window.visible=ref(true);window.downloadError='';
window.props=reactive({selectedNotice:{title:'A楼测试维保'},selectedMop:null,selectedMopAttachments:[],selectedAttachmentToken:'',selectedAttachment:null,bindingStatus:'',bindingError:'',canPreview:false,busy:false,disabledReason:'',buttonText:'打开填写',mopCandidates:[],selectedMopRecordId:'',mopSearch:'',isRecommendedMop:()=>false,localUploadBusy:false,localUploadStatus:'',localUploadMessage:''});
window.payload={scope:'A',cell_edits:[{row:1,col:2,value:'当前填写内容'}]};
window.validateFile=useLocalMopUpload().validateFile;window.flush=nextTick;
createApp({render:()=>h('div',{},[
window.visible.value?h(Panel,{...window.props,onUploadLocal:f=>{window.uploads.push(f.name);window.props.localUploadBusy=true},onUploadLocalInvalid:m=>window.errors.push(m),'onUpdate:mopSearch':v=>window.props.mopSearch=v}):null,
h(Footer,{items:[],downloadDisabled:false,onDownload:async()=>{try{await downloadFilledEngineerMop(window.payload)}catch(e){window.downloadError=e.message}}})])}).mount('#app');
</script></body></html>`;
const server = await createServer({ root, server: { host: "127.0.0.1", port: 0 }, logLevel: "error", plugins: [{
  name: "mop-check-page",
  configureServer(dev) {
    dev.middlewares.use("/__mop-check", async (_req, res) => {
      res.setHeader("Content-Type", "text/html");
      res.end(await dev.transformIndexHtml("/__mop-check", html));
    });
  },
}] });
let browser;
try {
  await server.listen();
  browser = await chromium.launch({ headless: true, channel: process.platform === "win32" ? "msedge" : undefined });
  const page = await browser.newPage({ viewport: { width: 1360, height: 1000 } });
  const runtimeErrors=[];
  page.on("pageerror", e=>runtimeErrors.push(e.message));
  await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/__mop-check`);
  await page.getByText("没有合适的 MOP？上传本地文件").waitFor();
  await page.locator("h1").focus();
  const transfer = async (event, names) => page.evaluate(({event,names})=>{
    const data=new DataTransfer();
    for(const name of names)data.items.add(new File(['sample'],name));
    const e=event==='paste'?new ClipboardEvent('paste',{clipboardData:data,bubbles:true,cancelable:true}):new DragEvent(event,{dataTransfer:data,bubbles:true,cancelable:true});
    document.body.dispatchEvent(e);return e.defaultPrevented;
  },{event,names});
  assert.equal(await transfer("paste", ["粘贴.xlsx"]),true);
  assert.deepEqual(await page.evaluate(()=>window.uploads),["粘贴.xlsx"]);
  await transfer("drop", ["忙碌时.xlsm"]);
  assert.equal(await page.evaluate(()=>window.uploads.length),1);
  await page.evaluate(async()=>{window.props.localUploadBusy=false;await window.flush()});
  assert.equal(await transfer("dragover", ["拖入.xlsm"]),true);
  assert.equal(await transfer("drop", ["拖入.xlsm"]),true);
  assert.deepEqual(await page.evaluate(()=>window.uploads),["粘贴.xlsx","拖入.xlsm"]);
  await page.evaluate(async()=>{window.props.localUploadBusy=false;await window.flush()});
  await transfer("drop",["一.xlsx","二.xlsx"]);
  await transfer("paste",["图片.png"]);
  assert.equal(await page.evaluate(()=>window.errors.length),2);
  assert.equal(await page.evaluate(()=>{
    const data=new DataTransfer();data.setData('text/plain','普通文字');
    const event=new ClipboardEvent('paste',{clipboardData:data,bubbles:true,cancelable:true});
    document.querySelector('.search-field input').dispatchEvent(event);return event.defaultPrevented;
  }),false);
  await page.locator(".local-mop-file-input").setInputFiles({name:"选择文件.xls",mimeType:"application/vnd.ms-excel",buffer:Buffer.from("file")});
  assert.equal(await page.evaluate(()=>window.uploads.at(-1)),"选择文件.xls");
  assert.match(await page.evaluate(()=>window.validateFile(new File([new ArrayBuffer(20*1024*1024+1)],'large.xlsx'))),/20MB/);
  await page.screenshot({path:resolve(output,"desktop.png"),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await page.screenshot({path:resolve(output,"mobile.png"),fullPage:true});
  const button=await page.getByRole("button",{name:"下载文件",exact:true}).boundingBox();
  assert.ok(button && button.x>=0 && button.x+button.width<=390);
  let submitted;
  await page.route("**/api/engineer/mop/fill?download=1",async route=>{
    submitted=route.request().postDataJSON();
    await route.fulfill({status:200,contentType:"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",headers:{'Content-Disposition':"attachment; filename*=UTF-8''"+encodeURIComponent('已填写维护单.xlsx')},body:Buffer.from("fresh-workbook")});
  });
  const downloaded=page.waitForEvent("download");
  await page.getByRole("button",{name:"下载文件",exact:true}).click();
  const file=await downloaded;
  assert.equal(file.suggestedFilename(),"已填写维护单.xlsx");
  assert.equal((await readFile(await file.path())).toString(),"fresh-workbook");
  assert.equal(submitted.cell_edits[0].value,"当前填写内容");
  await page.route("**/api/engineer/mop/fill?download=1",route=>route.fulfill({status:503,json:{error:"生成暂时失败"}}));
  await page.getByRole("button",{name:"下载文件",exact:true}).click();
  await page.waitForFunction(()=>window.downloadError==='生成暂时失败');
  await page.evaluate(async()=>{window.visible.value=false;await window.flush()});
  assert.equal(await transfer("paste",["不应上传.xlsx"]),false);
  assert.equal(await page.evaluate(()=>window.uploads.length),3);
  assert.deepEqual(runtimeErrors,[]);
  console.log("MOP 文件选择、拖放、粘贴、忙碌保护、文本粘贴、卸载清理、下载内容与失败提示检查通过");
} finally {
  await browser?.close();
  await server.close();
}
