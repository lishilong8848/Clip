import assert from 'node:assert/strict';
import {readFile, writeFile, mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';

const root=fileURLToPath(new URL('../../../../',import.meta.url));
const out=root+'output/responsive-audit/';
const report=JSON.parse(await readFile(out+'continued-operations-review.json','utf8'));
const baseline=JSON.parse(await readFile(out+'continued-operations-repair-before.json','utf8'));
const source=async(file,needle)=>{
  const text=await readFile(root+file,'utf8'),index=text.indexOf(needle);
  assert(index>=0,needle);
  return {file,line:text.slice(0,index).split('\n').length};
};
const components='bin/lan_bitable_template_portal/frontend/src/components/';
const workspace='bin/lan_bitable_template_portal/frontend/src/workspace-v2/';
const references={
  mop:await source(components+'EngineerMopPage.vue','<MopUploadFooter'),
  'local-workbook-upload':await source(components+'MopBindingPanel.vue','class="local-mop-upload"'),
  'maintenance-time':await source(components+'MopMobileFillForm.vue','class="quick-fields quick-time-fields"'),
  'select-signatures':await source(components+'MopCompanySignaturePicker.vue','class="sign-person"'),
  'write-signatures-to-workbook':await source(components+'EngineerMopPage.vue','async function fillMopSignatures'),
  'signature-invitation-form':await source(components+'MopSignatureInvitation.vue','async function load()'),
  'checks-and-remarks':await source(components+'MopMobileFillForm.vue','class="quick-checks"'),
  'download-filled-maintenance':await source(components+'MopUploadFooter.vue','class="btn mop-download"'),
  'upload-signed-maintenance':await source(components+'MopUploadFooter.vue','class="btn blue floating-upload-signed-mop"'),
  'edit-existing-followup':await source(components+'RepairFollowupPanel.vue','class="followup-workspace"'),
  'repair-device-picker':await source(components+'RepairLedgerPicker.vue','<template>'),
  'sop-attachment-upload':await source(workspace+'NoticeSopManager.vue','async function upload'),
  'sop-edit-step':await source(workspace+'NoticeSopManager.vue','class="v2-sop-step"'),
  'legacy-sop-step':await source('bin/lan_bitable_template_portal/workbench_lite.py','function pollingStepEditor'),
  'legacy-sop-attachment':await source('bin/lan_bitable_template_portal/workbench_lite.py','function pollingAttachmentRows'),
};
const oldRepair=await source(components+'RepairFollowupPanel.vue','grid-template-columns: minmax(0, 1fr);');
const oldTitle=await source(components+'RepairFollowupPanel.vue','.followup-title strong {');
const newRepair=await source(workspace+'repair.css','.v2-repair-detail-body>.n-radio-group{display:flex;width:100%;min-height:44px}');
report.scope={versions:['legacy','v2'],viewports:report.viewports,legacyQuery:'frontend=legacy',realApplicationRoutes:true,componentDemos:false,responsively:'Main thread owns restoration and capture; this script never connects to 9224/9225',buildPerformed:false,productionWrites:false,externalMessages:false,compactWorkspacesChanged:false};
report.findings=[
  {id:'legacy-followup-clipping',page:'旧版维修项目 > 跟进记录 > 选择历史跟进',controls:['跟进标题','搜索跟进内容','维修进展描述','维修进度','更新跟进记录'],position:'维修项目抽屉正文',cause:'followup-panel 的隐式 Grid 列按长标题最小内容宽度扩张；抽屉 overflow-x:hidden 在控件进入视区时被横向卷动，左侧文字被裁切。',fix:'显式 minmax(0,1fr) 单列；strong 改为 block，使现有省略规则生效。',sources:[oldRepair,oldTitle],phone:{},pc:{}},
  {id:'v2-followup-tab-overlap',page:'新版维修项目 > 跟进记录 > 编辑这条',controls:['办理流程/跟进记录/维修单信息/完成核对页签','历史跟进标题'],position:'跟进表单上方页签与标题交界',cause:'手机页签按钮为44px，容器仅34px，下方标题从按钮结束前2px开始。',fix:'仅在现有手机断点补容器 min-height:44px。',sources:[newRepair],phone:{},pc:{}},
];
let geometryAssertions=0;
for(const row of report.results) {
  row.sources=[references[row.name]||references[row.name==='repair'?'edit-existing-followup':row.version==='v2'?'sop-edit-step':'legacy-sop-step']];
  row.device=row.width===1440?'PC':row.width===402?'402x874':'440x956';
  row.browserFlowStatus=row.failure?'blocked':row.checks.some(c=>c.status==='blocked')?'partial':'observed';
  row.businessAcceptance='未进行真实业务提交；隔离接口回执只证明界面与请求链路。';
  for(const check of row.checks)check.source=references[check.control];
  for(const state of row.states) {
    if(row.version==='legacy')assert.equal(new URL(state.url).searchParams.get('frontend'),'legacy');
    assert.equal(state.width,row.width);assert.equal(state.height,row.height);
    if(row.name==='repair'&&state.state==='editing') {
      const before=baseline.results.find(r=>r.version===row.version&&r.width===row.width).states.find(s=>s.state==='editing');
      const panel=state.containers.find(c=>c.class.startsWith('followup-panel'));
      const prior=before.containers.find(c=>c.class.startsWith('followup-panel'));
      assert(panel.rect.left>=0&&panel.rect.right<=row.width+1);geometryAssertions++;
      assert(panel.scrollWidth<=panel.clientWidth+2);geometryAssertions++;
      if(row.version==='legacy') {
        const data={before:{left:prior.rect.left,width:prior.rect.width,scrollWidth:prior.scrollWidth},after:{left:panel.rect.left,width:panel.rect.width,scrollWidth:panel.scrollWidth},beforeScreenshot:before.screenshot,afterScreenshot:state.screenshot,status:'source-css-verified'};
        report.findings[0][row.width===1440?'pc':'phone'][row.device]=data;
      } else {
        const tabs=state.containers.filter(c=>c.class.startsWith('n-radio-button'));
        const heading=state.containers.find(c=>c.class==='v2-repair-followup-heading');
        const priorTabs=before.containers.filter(c=>c.class.startsWith('n-radio-button'));
        const priorHeading=before.containers.find(c=>c.class==='v2-repair-followup-heading');
        const gap=heading.rect.top-Math.max(...tabs.map(c=>c.rect.bottom));
        assert(gap>=0);geometryAssertions++;
        report.findings[1][row.width===1440?'pc':'phone'][row.device]={beforeGap:priorHeading.rect.top-Math.max(...priorTabs.map(c=>c.rect.bottom)),afterGap:gap,beforeScreenshot:before.screenshot,afterScreenshot:state.screenshot,status:row.width===1440?'desktop-unchanged':'source-css-verified'};
      }
    }
  }
}
report.fixtureGaps=[
  {page:'新旧版维护单 > 临时/外部人员 > 添加人员',endpoint:'GET /api/signatures/management/mop-people',status:'blocked-fixture-503',required:'scope_options 与 recipients；不要模拟邀请已发送或本人已签名。',source:references['signature-invitation-form']},
  {page:'旧版 SOP 附件',status:'partial-fixture-contract',required:'core-fixtures 的 attachments 需补 name、size、download_url；SOP需提供匹配服务合同的ready等字段。当前 undefined · 0KB 和 href=# 不能算附件下载通过。',source:references['legacy-sop-attachment']},
  {page:'新旧版维护单下载/签名写入/上传',status:'transport-only',required:'当前fill?download=1 返回文本，上传本地Excel也直接替换成固定预览。只接受控件和隔离请求结果，不验证真实Excel、签名图片、飞书附件或持久化。',source:references.mop},
];
report.verification={geometryAssertions,deepFlows:report.results.length,screenshots:report.results.reduce((sum,row)=>sum+row.states.length,0),sourceOnlyCss:true,realWorkbookExport:false,realBitableUpload:false,realSignatureInvitation:false,physicalSafariTouchKeyboard:false};
const sheetFolder=out+'continued-operations/contact-sheets/';
await mkdir(sheetFolder,{recursive:true});
const browser=await chromium.launch({headless:true});
const sheets=[];
try {
  const page=await browser.newPage({viewport:{width:920,height:2100}});
  for(const row of report.results) {
    for(let offset=0;offset<row.states.length;offset+=4) {
      const states=row.states.slice(offset,offset+4);
      const items=await Promise.all(states.map(async s=>({name:s.screenshot.split('/').at(-1),data:(await readFile(out+s.screenshot)).toString('base64')})));
      await page.setContent('<style>*{box-sizing:border-box}body{margin:0;padding:8px;background:#dfe5ea;font:13px Arial}main{display:grid;grid-template-columns:440px 440px;gap:12px}figure{margin:0;align-self:start}figcaption{height:26px;padding:5px;background:#fff}img{width:440px;height:auto;display:block}</style><main>'+items.map(item=>'<figure><figcaption>'+item.name+'</figcaption><img src="data:image/png;base64,'+item.data+'"></figure>').join('')+'</main>');
      await page.locator('img').evaluateAll(images=>Promise.all(images.map(img=>img.decode())));
      const name=`${row.version}-${row.name}-${row.width}-${offset/4+1}.png`;
      await page.locator('main').screenshot({path:sheetFolder+name});
      sheets.push({file:'continued-operations/contact-sheets/'+name,screenshots:states.map(s=>s.screenshot)});
    }
  }
} finally {await browser.close();}
report.contactSheets=sheets;
report.visualReview={status:'pending-human-inspection',contactSheets:sheets.map(s=>s.file)};
await writeFile(out+'continued-operations-review.json',JSON.stringify(report,null,2));
console.log(JSON.stringify({geometryAssertions,flows:report.results.length,screenshots:report.verification.screenshots,contactSheets:sheets.length,blocked:report.results.flatMap(row=>row.checks.filter(c=>c.status==='blocked').map(c=>({version:row.version,width:row.width,control:c.control,reason:c.reason||c.result?.reason})))}));
