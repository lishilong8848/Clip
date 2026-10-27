import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {preview} from 'vite';
import {layoutApiFixture} from './layout-api-fixture.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),output=path.resolve(root,'../../../output/playwright/pre-shift');
await mkdir(output,{recursive:true});
const project=path.resolve(root,'../../..'),python=path.join(project,'bin/.venv',process.platform==='win32'?'Scripts/python.exe':'bin/python');
const photoFixtures=JSON.parse(execFileSync(python,['-c',`
import datetime as dt,sys,tempfile,base64,json,io
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'bin'))
from PIL import Image,ImageDraw
from lan_bitable_template_portal.pre_shift_photo import render_handover,TZ
with tempfile.TemporaryDirectory() as folder:
    raw,stamped=Path(folder)/'source.png',Path(folder)/'stamp.jpg'
    image=Image.new('RGB',(1600,1200),'#708090')
    ImageDraw.Draw(image).rectangle((900,100,1400,1000),fill='#a5b5c5')
    image.save(raw)
    current=dt.datetime(2026,10,14,9,tzinfo=TZ)
    render_handover(raw,stamped,'A',current,capture={'captured_at':current.timestamp()*1000})
    logo=io.BytesIO();Image.new('RGBA',(128,64),'#20d050').save(logo,format='PNG')
    print(json.dumps({'stamped':base64.b64encode(stamped.read_bytes()).decode(),'source':base64.b64encode(raw.read_bytes()).decode(),'logo':base64.b64encode(logo.getvalue()).decode()}))
`],{cwd:project,maxBuffer:12*1024*1024,encoding:'utf8'}));
const stampedPhoto=Buffer.from(photoFixtures.stamped,'base64');
await writeFile(path.join(output,'watermarked-photo.jpg'),stampedPhoto);
const server=await preview({root,...(process.env.CLIPFLOW_WORKSPACE_BUILD_DIR?{configFile:path.join(root,'vite.workspace-v2.config.ts')}:{}),logLevel:'error',preview:{host:'127.0.0.1',port:0}}),origin='http://127.0.0.1:'+server.httpServer.address().port;
const browser=await chromium.launch({headless:true}),errors=[];
const layouts=layoutApiFixture();
let writes=0,definitionWrites=0,submitted=false,fail=false,mode='operator',page;
let fileWrites=0,files=[],handover=null,photoSaves=[];
let previewCount=0,cancelCount=0,previewSources=new Map(),photoPreferences={version:0,style:{}};
let locationSettings={configured:false,revision:'fixture-location-0'},locationWrites=0;
let releaseLocationSave;const locationRelease=new Promise(resolve=>{releaseLocationSave=resolve;});
let shift='白',day='2026-10-14';
let overviewOverride;
const items=[{id:'x',text:'核对测试清单一'},{id:'y',text:'核对测试清单二：较长的检查说明应在手机屏幕自动换行，不挤压检查结果与异常备注'}];
let definition={version:1,items:structuredClone(items)};
try{
 const context=await browser.newContext({viewport:{width:1440,height:960},reducedMotion:'reduce'});
 await context.route('**/*',async route=>{
  const req=route.request(),u=new URL(req.url());if(u.origin!==origin)return route.abort();if(!u.pathname.startsWith('/api/'))return route.continue();
  if(await layouts.handle(route,mode))return;
  const ok=data=>route.fulfill({json:{ok:true,data}});
  if(u.pathname==='/api/auth/status')return ok({logged_in:true,user:{open_id:'ou_'+mode,name:'隔离验收人员',role:mode==='manager'?'admin':'building',level:mode==='manager'?'L1':'L4',category:mode,home_scope:'A',modules:mode==='restricted'?['change']:['maintenance','polling','adjust'],can_delete_business:mode==='manager'},scope_options:[{value:'A',label:'A楼'},{value:'B',label:'B楼'}]});
  if(u.pathname==='/api/health')return route.fulfill({json:{ok:true,service:'clipflow_backend',instance_id:'pre-shift-fixture'}});
  if(u.pathname==='/api/pre-shift-checks')return ok({server_time:new Date().toISOString(),scope:'A',scopes:mode==='manager'?['A','B']:['A'],can_manage:mode==='manager',can_configure_location:mode==='manager',overview:overviewOverride,definition,roster_ready:true,roster_error:'',tasks:!submitted&&!definition.items.length?[]:[{id:'fixture-'+day+shift,day,scope:'A',shift,due:Date.now()/1000-60,files,handover_photo:handover,can_upload_handover:!handover,definition_version:submitted?1:definition.version,items:submitted?items:definition.items,people:[{name:'值班员一'},{name:'值班长二'}],submitted:submitted?'2026-10-14T09:05:00+08:00':'',actor_name:'隔离验收人员',can_submit:!submitted,deadline:Date.now()/1000+10000,answers:submitted?[{...items[0],status:'normal',note:''},{...items[1],status:'abnormal',note:'测试异常说明'}]:[],sync_status:submitted?'synced':'pending'}]});
  if(u.pathname==='/api/pre-shift-checks/location-settings'){
   assert.equal(mode,'manager','only privileged accounts access global map settings');
   if(req.method()==='PUT'){assert.deepEqual(req.postDataJSON(),{key:'0'.repeat(32),revision:locationSettings.revision});locationWrites++;await locationRelease;locationSettings={configured:true,revision:'fixture-location-'+locationWrites};}
   return ok(locationSettings);
  }
  if(u.pathname==='/api/pre-shift-checks/recipients')return ok({items:[{open_id:'ou_fixed',name:'固定通知人',employee_no:'001'},{open_id:'ou_other',name:'备用通知人',employee_no:'002'}]});
  if(u.pathname==='/api/pre-shift-checks/history')return ok({scope:u.searchParams.get('scope'),items:[],total:0,page:1,page_size:20});
  if(u.pathname==='/api/pre-shift-checks/submit'){writes++;if(fail){fail=false;return route.fulfill({status:503,json:{ok:false,error:'隔离测试：暂未确认，请重试'}});}assert.deepEqual(req.postDataJSON().answers,[{id:'x',status:'normal',note:''},{id:'y',status:'abnormal',note:'测试异常说明'}]);submitted=true;return ok({completed:true});}
  if(u.pathname==='/api/pre-shift-checks/definition'){definitionWrites++;definition={...req.postDataJSON(),version:definition.version+1};return ok(definition);}
  if(u.pathname==='/api/pre-shift-checks/photo-preview'){
   if(req.method()==='DELETE'){cancelCount++;previewSources.delete(req.postDataJSON().preview_id);return ok({cancelled:true});}
   assert.equal(u.searchParams.get('retain'),'1','new photo editor retains the uploaded source');
   const existing=u.searchParams.get('name')==='粘贴截图.png',captured=u.searchParams.get('capture')==='1';
   const key='preview-'+(++previewCount),time=u.searchParams.get('name')==='取消.png'?'':'2026-10-14 09:00';previewSources.set(key,{params:new URLSearchParams(u.search),size:req.postDataBuffer().length});
   return ok({preview_id:key,preferences:structuredClone(photoPreferences),original_time:time,time_origin:captured?'camera':'exif',image:existing?'data:image/jpeg;base64,'+photoFixtures.stamped:'data:image/png;base64,'+photoFixtures.source,width:1600,height:1200,existing_watermark:existing,recognition:existing?'identified':'not_detected',time_source:captured?'本次拍照':'照片拍摄信息',options:{enabled:captured&&!existing,existing,scale:.4,title:'EA118南通机房A楼班前检查',area:'A楼',time,weather:'',location:''}});
  }
  if(u.pathname==='/api/pre-shift-checks/files'){
   const body=req.headers()['content-type']?.startsWith('application/json')?req.postDataJSON():null;
   const source=body?previewSources.get(body.preview_id):{params:u.searchParams,size:req.postDataBuffer().length};assert(source,'confirmed photo uses its existing staged source');
   const params=source.params,isHandover=params.get('kind')==='handover',options=body?.watermark,stamped=options?.enabled&&!options.existing,isImage=/\.(png|jpe?g|webp)$/i.test(params.get('name'));
   if(isImage){assert(body,'images are not uploaded a second time');assert.deepEqual(Object.keys(body).sort(),['preview_id','remember_style','watermark']);}
   fileWrites++;
   if(isImage)photoSaves.push(options);
   if(stamped&&body.remember_style)photoPreferences={version:photoPreferences.version+1,style:{scale:options.scale,logo:options.logo||'',position:options.position||{x:0,y:1}}};
   const file={id:'file-'+fileWrites,item_id:params.get('item_id'),name:stamped?(isHandover?'交接班_交接班水印.jpg':'现场_班前检查水印.jpg'):params.get('name'),size:source.size,is_image:isImage,can_delete:true,...(isImage?{watermarked:stamped||options.existing,delivery_status:'pending',watermark:{audit:{original_time:'2026-10-14 09:00',display_time:options.time,modified:options.time!=='2026-10-14 09:00',source:'本次拍照'}}}:{})};if(isHandover)handover=file;else files.push(file);return ok(file);}
  if(u.pathname.startsWith('/api/pre-shift-checks/files/')){const file=[...files,handover].find(f=>f&&u.pathname.endsWith('/'+f.id));return route.fulfill(file?.is_image?{body:stampedPhoto,contentType:'image/jpeg',headers:{'Content-Disposition':"inline; filename*=UTF-8''"+encodeURIComponent(file.name)}}:{body:'isolated file'});}
  if(u.pathname==='/api/scope-overview')return ok({scopes:{A:{maintenance_pending:0,maintenance_ongoing:0}},source_snapshot_ready:true});
  if(u.pathname==='/api/workbench')return ok({records:[],ongoing:[],records_pagination:{total:0},ongoing_pagination:{total:0}});
  return ok({items:[],records:[],stats:{},summary:{}});
 });
 page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
 async function confirmPhoto(){const editor=page.locator('.v2-photo-editor');await editor.waitFor();await editor.getByRole('button',{name:/确认水印并上传|保留原图并上传/}).click();await editor.waitFor({state:'hidden'});const viewer=page.locator('.v2-check-photo-preview');await viewer.waitFor();await viewer.getByRole('button',{name:'完成',exact:true}).click();}
 await page.goto(origin+'/assets/workspace-v2/index.html');
 const check=page.getByRole('region',{name:'班前检查',exact:true}).last();
 await page.getByText('本次待检查',{exact:true}).waitFor();
 assert(await check.evaluate(el=>el.classList.contains('has-pending-checks')));
 assert.equal(await check.evaluate(el=>getComputedStyle(el,'::before').animationName),'none','reduced motion uses a static red outline');
 const card=page.locator('.grid-stack-item[data-widget-kind="pre_shift"]>.v2-widget');
 assert.equal(await card.evaluate(el=>getComputedStyle(el,'::after').animationName),'none','reduced motion stops the whole-card pulse');
 assert.notEqual(await card.evaluate(el=>getComputedStyle(el,'::after').backgroundColor),'rgba(0, 0, 0, 0)','attention tints the whole card, not only its edge');
 const refresh=card.locator('.v2-widget-head').getByRole('button',{name:'刷新班前检查',exact:true});
 const custom=page.getByRole('button',{name:'自定义工作台',exact:true}),buttonBox=await custom.boundingBox(),refreshBox=await refresh.boundingBox(),headerBox=await card.locator('.v2-widget-head').boundingBox();
 assert(buttonBox&&refreshBox&&headerBox&&buttonBox.height<=32&&buttonBox.x+buttonBox.width<=refreshBox.x&&Math.abs(buttonBox.y+buttonBox.height/2-refreshBox.y-refreshBox.height/2)<2&&Math.abs(refreshBox.x+refreshBox.width-headerBox.x-headerBox.width)<=24,'customization and refresh share the top-right header row');
 await page.emulateMedia({reducedMotion:'no-preference'});
 assert.equal(await card.evaluate(el=>getComputedStyle(el,'::after').animationName),'preShiftPulse');
 await page.emulateMedia({reducedMotion:'reduce'});
 assert.equal(await check.getByRole('radio',{checked:true}).count(),0,'no item is pre-checked');
 await page.getByRole('button',{name:'自定义工作台',exact:true}).click();assert.equal(await page.getByRole('button',{name:'移除班前检查',exact:true}).count(),0);await page.locator('.v2-dashboard-actions').getByRole('button',{name:'取消',exact:true}).click();
 await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('请逐项选择检查结果，并填写异常项备注。',{exact:true}).waitFor();assert.equal(writes,0);
 const rows=check.locator('fieldset');
 assert((await rows.nth(0).locator('.n-radio-button').first().boundingBox()).height>=44,'check results have a full touch target on PC as well as phone');
 await check.locator('.v2-check-choice').first().click({position:{x:5,y:5}});assert.equal(writes,0,'selecting a shift card does not submit the check');
 await rows.nth(0).locator('.n-radio-button').filter({hasText:'无异常'}).click();await rows.nth(1).locator('.n-radio-button').filter({hasText:'异常'}).last().click();
 for(const theme of ['light','dark']){
  await page.evaluate(value=>document.documentElement.dataset.v2Theme=value,theme);
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('.v2-check-normal.n-radio-button--checked')).backgroundColor==='rgb(21, 128, 61)'&&getComputedStyle(document.querySelector('.v2-check-abnormal.n-radio-button--checked')).backgroundColor==='rgb(180, 35, 24)');
  for(const selected of [rows.nth(0).locator('.v2-check-normal'),rows.nth(1).locator('.v2-check-abnormal')])assert.equal(await selected.evaluate(el=>getComputedStyle(el).color),'rgb(255, 255, 255)','normal and abnormal selected labels keep contrast in both themes');
 }
 await page.evaluate(()=>document.documentElement.dataset.v2Theme='light');
 await check.getByRole('button',{name:'提交检查',exact:true}).click();assert.equal(writes,0,'abnormal requires a note');await rows.nth(1).getByRole('textbox').fill('测试异常说明');
 await rows.nth(0).getByRole('button',{name:items[0].text+'附件',exact:true}).click();
 const album=rows.nth(0).getByLabel('上传检查照片',{exact:true});
 assert((await album.getAttribute('accept')).includes('.heic'),'album picker accepts iPhone originals');
 await album.setInputFiles({name:'iPhone.HEIC',mimeType:'image/heic',buffer:Buffer.from('isolated codec fixture')});
 await page.locator('.v2-photo-editor').waitFor();
 await page.locator('.v2-photo-editor').getByRole('button',{name:'取消',exact:true}).click();
 await page.locator('.v2-photo-editor').waitFor({state:'hidden'});assert.equal(fileWrites,0,'canceling HEIC preview does not upload');
 await rows.nth(0).getByLabel('上传检查附件',{exact:true}).setInputFiles({name:'检查记录.txt',mimeType:'text/plain',buffer:Buffer.from('test')});await check.getByRole('link',{name:'检查记录.txt',exact:true}).waitFor();assert.equal(fileWrites,1);assert.equal(writes,0);assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'测试异常说明');
 const uploadZone=rows.nth(0).getByRole('group',{name:'检查附件上传区',exact:true});
 await uploadZone.evaluate(el=>{const data=new DataTransfer();data.items.add(new File(['paste'],'粘贴截图.png',{type:'image/png'}));el.dispatchEvent(new ClipboardEvent('paste',{bubbles:true,cancelable:true,clipboardData:data}));});await page.locator('.v2-photo-editor').getByText('已识别到水印，当前保留原图。',{exact:true}).waitFor();assert.equal(fileWrites,1,'inspection does not save files');await confirmPhoto();await check.getByRole('link',{name:'粘贴截图.png',exact:true}).waitFor();assert.equal(fileWrites,2);assert.equal(photoSaves.at(-1).enabled,false);
 await uploadZone.evaluate(el=>{const data=new DataTransfer();data.items.add(new File(['drop'],'拖拽照片.jpg',{type:'image/jpeg'}));el.dispatchEvent(new DragEvent('dragenter',{bubbles:true,cancelable:true,dataTransfer:data}));el.dispatchEvent(new DragEvent('drop',{bubbles:true,cancelable:true,dataTransfer:data}));});await confirmPhoto();await check.getByRole('link',{name:'拖拽照片.jpg',exact:true}).waitFor();assert.equal(fileWrites,3);assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'测试异常说明');
 await uploadZone.evaluate(el=>{const data=new DataTransfer();data.items.add(new File(['no'],'不可上传.exe'));el.dispatchEvent(new DragEvent('drop',{bubbles:true,cancelable:true,dataTransfer:data}));});await uploadZone.getByText('不支持此文件类型。',{exact:true}).waitFor();assert.equal(fileWrites,3);
 const photoInput=check.getByLabel('上传交接班照片',{exact:true});assert.equal(await photoInput.getAttribute('capture'),null,'photo picker opens album, not a forced camera');await photoInput.setInputFiles({name:'交接班.png',mimeType:'image/png',buffer:Buffer.from('mock')});await confirmPhoto();await check.getByText('本班已上传',{exact:true}).waitFor();assert.equal(fileWrites,4);assert.equal(await check.locator('.v2-check-handover').getByRole('button',{name:'上传照片',exact:true}).count(),0);
for(const width of [1440,390,360,375]){await page.setViewportSize({width,height:width===360?640:width===375?667:width<700?844:960});await check.scrollIntoViewIfNeeded();if(width===390){const choosing=page.waitForEvent('filechooser');await rows.nth(0).getByRole('button',{name:'上传照片',exact:true}).click();const chooser=await choosing;assert.equal(await chooser.element().getAttribute('capture'),null);await chooser.setFiles({name:'手机相册.png',mimeType:'image/png',buffer:Buffer.from('album')});await confirmPhoto();await rows.nth(0).getByRole('link',{name:'手机相册.png',exact:true}).waitFor();assert.equal(fileWrites,5);}assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));const photo=check.locator('.is-handover'),image=photo.locator('.v2-check-photo-thumb img');await image.waitFor();await photo.getByText('照片已保存，正在发送给当前登录账号',{exact:true}).waitFor();await image.scrollIntoViewIfNeeded();assert(await image.evaluate(el=>el.complete&&el.naturalWidth>0&&getComputedStyle(el).objectFit==='contain'),'show the actual photo without cropping');const box=await image.boundingBox();assert(box&&box.height>120&&box.width<=width);await photo.screenshot({path:path.join(output,'handover-'+width+'.png')});await page.screenshot({path:path.join(output,'check-'+width+'.png'),fullPage:true});}
 handover.delivery_status='sent';await refresh.click();await check.getByText('照片已发送给当前登录账号',{exact:true}).waitFor();assert.equal(fileWrites,5,'checking delivery status never reuploads');
 await rows.nth(0).getByLabel('上传检查照片',{exact:true}).setInputFiles({name:'取消.png',mimeType:'image/png',buffer:Buffer.from('cancel')});const missingTime=page.locator('.v2-photo-editor');await missingTime.getByRole('switch',{name:'添加水印',exact:true}).click();await missingTime.getByRole('button',{name:'确认水印并上传',exact:true}).click();await missingTime.getByText('请选择照片的拍照时间。',{exact:true}).waitFor();assert.equal(fileWrites,5,'missing capture time cannot silently use upload time');
 for(const width of [1440,360,375]){
  await page.setViewportSize({width,height:width<700?640:960});
  const captureInput=missingTime.locator('.n-date-picker input').first();await captureInput.fill('');await captureInput.press('Enter');await captureInput.click();
  const calendar=page.locator('.n-date-panel:visible');await calendar.waitFor();
  await page.waitForFunction(()=>{let el=document.querySelector('.n-date-panel');if(!el)return false;for(;el;el=el.parentElement)if(Number(getComputedStyle(el).opacity)<.99)return false;return true;});
  await page.screenshot({path:path.join(output,'photo-time-picker-'+width+'.png')});
  await calendar.locator('[data-n-date]:not(.n-date-panel-date--excluded):not(.n-date-panel-date--disabled)').filter({hasText:/^14$/}).click();
  await calendar.getByRole('button',{name:'确认',exact:true}).click();await calendar.waitFor({state:'hidden'});
  assert.match(await captureInput.inputValue(),/^20\d{2}-\d{2}-14 \d{2}:\d{2}$/,'empty capture time can be selected from the calendar');
  await missingTime.getByText('手动填写 · 照片未提供可核验的拍摄时间',{exact:true}).waitFor();
  assert.equal(fileWrites,5,'choosing a capture time only updates the preview');
 }
 await missingTime.getByRole('button',{name:'取消',exact:true}).click();await missingTime.waitFor({state:'hidden'});assert.equal(fileWrites,5,'canceling photo editing never uploads a record');assert.equal(cancelCount,2,'both canceled previews remove their staged source');
 await page.setViewportSize({width:375,height:667});
 await page.evaluate(()=>{window.__sharedPhoto=null;Object.defineProperty(navigator,'canShare',{configurable:true,value:()=>true});Object.defineProperty(navigator,'share',{configurable:true,value:async data=>{window.__sharedPhoto={name:data.files[0].name,type:data.files[0].type,size:data.files[0].size};}});});
 await rows.nth(0).getByLabel('拍摄检查照片',{exact:true}).setInputFiles({name:'拍照.png',mimeType:'image/png',buffer:Buffer.from('camera')});
 const photoEditor=page.locator('.v2-photo-editor');await photoEditor.waitFor();
 const timeInput=photoEditor.locator('.n-date-picker input').first();await timeInput.fill('2026-10-14 09:15');await timeInput.press('Enter');await photoEditor.getByText('手动修改 · 原始 2026-10-14 09:00（本次拍照）',{exact:true}).waitFor();
 await photoEditor.getByRole('textbox',{name:'水印标题',exact:true}).fill('修改后的标题');
 await photoEditor.getByRole('textbox',{name:'施工区域',exact:true}).fill('A楼值班室');
 await photoEditor.getByRole('textbox',{name:'水印天气',exact:true}).fill('多云 25℃');
 await photoEditor.getByRole('textbox',{name:'水印地点',exact:true}).fill('已确认地点');
 const sizeSlider=photoEditor.getByRole('group',{name:'水印大小',exact:true}).getByRole('slider');await sizeSlider.focus();await sizeSlider.press('ArrowRight');
 await page.setViewportSize({width:1440,height:960});
 await photoEditor.locator('.n-radio-button').filter({hasText:'右上角'}).click();
 const handle=photoEditor.getByRole('button',{name:'移动水印',exact:true});await handle.scrollIntoViewIfNeeded();
 let stampBounds=await handle.boundingBox(),canvasBounds=await photoEditor.locator('canvas').boundingBox();assert(stampBounds&&canvasBounds&&stampBounds.x>canvasBounds.x+canvasBounds.width/2&&stampBounds.y<canvasBounds.y+30,'corner placement is reflected in the actual photo');
 await page.mouse.move(stampBounds.x+stampBounds.width/2,stampBounds.y+stampBounds.height/2);await page.mouse.down();await page.mouse.move(stampBounds.x+stampBounds.width/2-100,stampBounds.y+stampBounds.height/2+80,{steps:12});await page.mouse.up();
 let movedBounds=await handle.boundingBox();assert(movedBounds.x<stampBounds.x-80&&movedBounds.y>stampBounds.y+60,'drag moves the watermark in image coordinates');
 await handle.focus();await handle.press('ArrowDown');assert((await handle.boundingBox()).y>movedBounds.y,'keyboard can move the same watermark');
 await photoEditor.locator('.n-radio-button').filter({hasText:'右下角'}).click();
 stampBounds=await handle.boundingBox();canvasBounds=await photoEditor.locator('canvas').boundingBox();assert(stampBounds.x+stampBounds.width<=canvasBounds.x+canvasBounds.width+1&&stampBounds.y+stampBounds.height<=canvasBounds.y+canvasBounds.height+1,'watermark remains fully inside photo');
 await page.setViewportSize({width:1440,height:960});await photoEditor.getByText('VNET',{exact:true}).click();await page.waitForFunction(()=>document.querySelector('.v2-photo-editor input[value="vnet"]')?.disabled===false);await page.waitForFunction(()=>{const image=document.querySelector('.v2-photo-editor canvas');return image&&Array.from(image.getContext('2d').getImageData(0,0,image.width,image.height).data).filter((v,i,a)=>i%4===0&&v>245&&a[i+1]>245&&a[i+2]>245).length>200;});await photoEditor.locator('.n-card-content').evaluate(el=>el.scrollTop=0);await photoEditor.screenshot({path:path.join(output,'photo-editor-vnet.png')});
 await photoEditor.getByText('自定义',{exact:true}).click();await photoEditor.getByRole('button',{name:'确认水印并上传',exact:true}).click();await photoEditor.getByText('请选择可读取的Logo图片。',{exact:true}).waitFor();assert.equal(fileWrites,5,'custom logo must be selected before saving');
 await photoEditor.getByLabel('上传自定义Logo',{exact:true}).setInputFiles({name:'不支持.svg',mimeType:'image/svg+xml',buffer:Buffer.from('<svg/>')});await photoEditor.getByText('请选择2MB以内的PNG、JPG或WebP格式Logo。',{exact:true}).waitFor();assert.equal(fileWrites,5);
 await photoEditor.getByLabel('上传自定义Logo',{exact:true}).setInputFiles({name:'自定义Logo.png',mimeType:'image/png',buffer:Buffer.from(photoFixtures.logo,'base64')});await photoEditor.getByRole('img',{name:'自定义Logo',exact:true}).waitFor();await page.waitForFunction(()=>{const image=document.querySelector('.v2-photo-editor canvas');return image&&Array.from(image.getContext('2d').getImageData(0,0,image.width,image.height).data).some((v,i,a)=>i%4===0&&v<90&&a[i+1]>160&&a[i+2]<120);});
 await page.setViewportSize({width:360,height:640});await photoEditor.locator('.n-card-content').evaluate(el=>el.scrollTop=0);
 const touchClient=await context.newCDPSession(page);await touchClient.send('Emulation.setTouchEmulationEnabled',{enabled:true,maxTouchPoints:1});
 const beforeTouch=await handle.boundingBox(),tx=beforeTouch.x+beforeTouch.width/2,ty=beforeTouch.y+beforeTouch.height/2;
 await touchClient.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:tx,y:ty}]});
 await touchClient.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:tx-40,y:ty-30}]});
 await touchClient.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
 assert((await handle.boundingBox()).x<beforeTouch.x-25,'mobile touch drags the same watermark without scrolling the form');
 await touchClient.send('Emulation.setTouchEmulationEnabled',{enabled:false});await touchClient.detach();
 await photoEditor.locator('.n-radio-button').filter({hasText:'右下角'}).click();
 for(const width of [1440,360,375]){await page.setViewportSize({width,height:width<700?640:960});await photoEditor.locator('.n-card-content').evaluate(el=>el.scrollTop=0);await photoEditor.screenshot({path:path.join(output,'photo-editor-'+width+'.png')});assert(!await photoEditor.evaluate(el=>el.scrollWidth>el.clientWidth+1));const group=photoEditor.locator('.v2-photo-logo-field'),radioBounds=await group.locator('.n-radio-group').boundingBox(),uploadBounds=await group.locator('.v2-photo-logo-upload').boundingBox();assert(radioBounds&&uploadBounds&&uploadBounds.y>=radioBounds.y+radioBounds.height+4,'logo options and upload row must not overlap');if(width<700)assert((await group.locator('.n-radio-button').first().boundingBox()).height>=44,'logo modes have full mobile touch targets');const save=await photoEditor.getByRole('button',{name:'确认水印并上传',exact:true}).boundingBox();assert(save&&save.y+save.height<=await page.evaluate(()=>innerHeight),'editor actions stay visible on small phones');}
 const downloadEvent=page.waitForEvent('download');await photoEditor.getByRole('button',{name:'确认水印并上传',exact:true}).click();
 const previewPhoto=page.locator('.v2-check-photo-preview');await previewPhoto.locator('img').waitFor();
 const download=await downloadEvent;assert.equal(download.suggestedFilename(),'现场_班前检查水印.jpg');
 assert.equal(fileWrites,6);assert(files.at(-1).watermarked);assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'测试异常说明');
 assert.equal(photoSaves.at(-1).title,'修改后的标题');assert.equal(photoSaves.at(-1).time,'2026-10-14 09:15');assert.equal(photoSaves.at(-1).scale,.41,'slider changes final uploaded watermark size');assert.match(photoSaves.at(-1).logo,/^data:image\/(webp|png);base64,/,'custom logo reaches the existing upload API');assert.deepEqual(photoSaves.at(-1).position,{x:1,y:1});assert.deepEqual(Object.keys(photoPreferences.style).sort(),['logo','position','scale'],'no time, weather or location in account preferences');await previewPhoto.getByText('拍照时间：2026-10-14 09:15 · 手动修改；原始 2026-10-14 09:00',{exact:true}).waitFor();
 await previewPhoto.getByRole('button',{name:'打开系统保存',exact:true}).click();
 assert.deepEqual(await page.evaluate(()=>window.__sharedPhoto),{name:'现场_班前检查水印.jpg',type:'image/jpeg',size:stampedPhoto.length});
 await previewPhoto.screenshot({path:path.join(output,'camera-save-375.png')});
 await previewPhoto.getByRole('button',{name:'完成',exact:true}).click();
 const tabs=context.pages().length;await rows.nth(0).getByRole('link',{name:'现场_班前检查水印.jpg',exact:true}).click();await previewPhoto.waitFor();assert.equal(context.pages().length,tabs,'photo filename opens in-page');await previewPhoto.getByRole('button',{name:'完成',exact:true}).click();
 await rows.nth(0).getByLabel('上传检查照片',{exact:true}).setInputFiles({name:'验证样式.jpg',mimeType:'image/jpeg',buffer:Buffer.from('style')});await photoEditor.waitFor();await photoEditor.getByRole('switch',{name:'添加水印',exact:true}).click();assert.equal(await photoEditor.getByRole('radio',{name:'右下角',exact:true}).isChecked(),true);assert.equal(await photoEditor.getByRole('slider').getAttribute('aria-valuenow'),'41');assert.equal(await photoEditor.getByRole('textbox',{name:'水印地点',exact:true}).inputValue(),'','previous photo location is not remembered');await photoEditor.getByRole('textbox',{name:'水印标题',exact:true}).fill('第二张照片');await photoEditor.getByRole('button',{name:'取消',exact:true}).click();await photoEditor.waitFor({state:'hidden'});assert.equal(fileWrites,6,'restoring and canceling styles does not save a file');assert.equal(previewCount,8,'one source upload per image selection including HEIC');
 await page.setViewportSize({width:1440,height:960});fail=true;await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('隔离测试：暂未确认，请重试',{exact:true}).waitFor();assert.equal(await rows.nth(1).getByRole('textbox').inputValue(),'测试异常说明');
 await check.getByRole('button',{name:'提交检查',exact:true}).click();await check.getByText('本次已完成',{exact:true}).waitFor();assert.equal(writes,2);await check.getByText('已同步飞书',{exact:true}).waitFor();assert.equal(await check.getByRole('button',{name:'提交检查',exact:true}).count(),0);
 assert(await rows.nth(1).getByRole('radio',{name:'异常',exact:true}).isDisabled());
 assert.equal(await rows.nth(1).locator('.v2-check-abnormal').evaluate(el=>getComputedStyle(el).backgroundColor),'rgb(180, 35, 24)','completed snapshots retain abnormal red');
 await page.reload();await page.getByText('本次已完成',{exact:true}).waitFor();assert.equal(writes,2);
 assert(!await check.evaluate(el=>el.classList.contains('has-pending-checks')));
 assert.equal(await card.evaluate(el=>getComputedStyle(el,'::after').content),'none','completion removes the whole-card tint');
 shift='夜';submitted=false;await refresh.click();await check.getByText('本次待检查',{exact:true}).waitFor();assert(await check.evaluate(el=>el.classList.contains('has-pending-checks')),'next shift alerts independently');
 submitted=true;await refresh.click();await check.getByText('本次已完成',{exact:true}).waitFor();assert(!await check.evaluate(el=>el.classList.contains('has-pending-checks')),'peer or duty completion clears every viewer');
 mode='manager';await page.reload();await page.getByRole('button',{name:'管理清单',exact:true}).click();const editor=page.locator('.v2-check-editor');
 await editor.getByText('拍照地点解析设置',{exact:true}).click();await editor.getByText('尚未配置地点解析',{exact:true}).waitFor();
 const mapKey=editor.getByLabel('高德 Web 服务 Key',{exact:true});await mapKey.fill('wrong');await editor.getByRole('button',{name:'保存地点解析',exact:true}).click();await editor.getByText('请填写高德 Web 服务类型的32位 Key。',{exact:true}).waitFor();assert.equal(locationWrites,0);
 await mapKey.fill('0'.repeat(32));const locationRequest=page.waitForRequest(request=>request.url().endsWith('/api/pre-shift-checks/location-settings')&&request.method()==='PUT');await editor.getByRole('button',{name:'保存地点解析',exact:true}).click();await locationRequest;
 assert(await editor.getByRole('button',{name:'取消',exact:true}).isDisabled(),'cannot close while the key save is unconfirmed');assert(await editor.getByRole('button',{name:'保存清单',exact:true}).isDisabled());await page.keyboard.press('Escape');assert(await editor.isVisible());releaseLocationSave();
 await editor.getByText('已保存，下次照片定位时生效',{exact:true}).waitFor();assert.equal(await mapKey.inputValue(),'');assert.equal(locationWrites,1);
 for(const width of [1440,360]){await page.setViewportSize({width,height:width<700?640:960});await mapKey.scrollIntoViewIfNeeded();assert(!await editor.evaluate(el=>el.scrollWidth>el.clientWidth+1));await page.screenshot({path:path.join(output,'location-settings-'+width+'.png')});}
 await page.setViewportSize({width:1440,height:960});await editor.getByText('拍照地点解析设置',{exact:true}).click();
 await editor.getByRole('button',{name:'添加检查项',exact:true}).click();await editor.getByRole('textbox',{name:'检查项3',exact:true}).fill('新增测试检查项');await editor.getByLabel('执行频率',{exact:true}).nth(2).click();await page.locator('.n-base-select-option').getByText('每周',{exact:true}).click();
 const fixedRecipients=editor.locator('.v2-check-recipients').nth(2);await fixedRecipients.locator('.n-base-selection').click();await page.locator('.n-base-select-option').getByText('固定通知人 · 001',{exact:true}).click();await page.locator('.n-base-select-option').getByText('备用通知人 · 002',{exact:true}).click();await editor.getByRole('textbox',{name:'检查项3',exact:true}).click();
 for(const width of [1440,360]){await page.setViewportSize({width,height:width<700?640:960});await fixedRecipients.scrollIntoViewIfNeeded();assert(!await editor.evaluate(el=>el.scrollWidth>el.clientWidth+1));await editor.screenshot({path:path.join(output,'fixed-recipients-'+width+'.png')});}
 await page.setViewportSize({width:1440,height:960});await editor.getByRole('button',{name:'保存清单',exact:true}).click();await editor.waitFor({state:'hidden'});assert.equal(definitionWrites,1);assert.equal(definition.items.length,3);assert.equal(definition.items[2].frequency.kind,'weekly');assert.deepEqual(definition.items[2].fixed_recipients,['ou_fixed','ou_other']);assert.deepEqual(definition.items[0].fixed_recipients,[]);
 assert.equal(await check.locator('fieldset').count(),2,'completed checks keep their original snapshot');
 submitted=false;await page.reload();await check.locator('fieldset').nth(2).waitFor();
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:960});assert(await check.getByRole('button',{name:'提交检查',exact:true}).isEnabled(),'manager can submit in PC/mobile view');assert(await check.locator('.n-radio-input').first().isEnabled(),'manager checklist fields are editable');}
 await check.locator('fieldset').first().locator('.n-radio-button').filter({hasText:'无异常'}).click();
 for(const width of [1440,390]){
  await page.setViewportSize({width,height:width<700?844:960});
  const history=check.getByRole('button',{name:'历史检查记录',exact:true});assert(await history.isEnabled(),'unsent answers do not disable history');await history.click();
  await check.getByRole('region',{name:'历史检查记录',exact:true}).waitFor();await check.getByRole('button',{name:'返回检查清单',exact:true}).click();
  assert(await check.locator('fieldset').first().getByRole('radio',{name:'无异常',exact:true}).isChecked(),'viewing history preserves unsent answers');
  const manage=check.getByRole('button',{name:'管理清单',exact:true});assert(await manage.isEnabled(),'unsent answers do not disable management');await manage.click();await editor.waitFor();await editor.getByRole('button',{name:'取消',exact:true}).click();await editor.waitFor({state:'hidden'});
  assert(await check.locator('fieldset').first().getByRole('radio',{name:'无异常',exact:true}).isChecked(),'canceling management preserves current answers');
 }
 await page.getByRole('button',{name:'管理清单',exact:true}).click();for(const name of ['删除检查项3','删除检查项2','删除检查项1'])await editor.getByRole('button',{name,exact:true}).click();await editor.getByRole('button',{name:'保存清单',exact:true}).click();await editor.waitFor({state:'hidden'});await check.getByText('尚未配置班前检查清单',{exact:true}).waitFor();assert.equal(await check.locator('fieldset').count(),0,'clearing removes the open checklist without page reload');
 await page.getByRole('button',{name:'管理清单',exact:true}).click();await editor.getByRole('button',{name:'添加检查项',exact:true}).click();await editor.getByRole('textbox',{name:'检查项1',exact:true}).fill('保存后立即出现的检查项');await editor.getByRole('button',{name:'保存清单',exact:true}).click();await editor.waitFor({state:'hidden'});await check.locator('fieldset').filter({hasText:'保存后立即出现的检查项'}).waitFor();assert.equal(await check.locator('fieldset').count(),1,'adding renders immediately without page reload');
 await page.setViewportSize({width:1440,height:700});const main=page.locator('.v2-main'),body=check.locator('xpath=ancestor::div[contains(@class,"v2-widget-body")]');await main.evaluate(el=>el.scrollTop=0);await body.evaluate(el=>el.scrollTop=0);const overflowing=await body.evaluate(el=>el.scrollHeight>el.clientHeight+2);await body.hover();await page.mouse.wheel(0,240);await page.waitForTimeout(250);assert(await (overflowing?body:main).evaluate(el=>el.scrollTop)>0,'wheel scrolls the widget when overflowing, otherwise the page');
 definition={version:definition.version+1,items:Array.from({length:25},(_,i)=>({id:'long-'+i,text:'核对本楼设备运行状态、交接记录与现场安全措施，逐项确认检查内容 '+(i+1)}))};await refresh.click();await check.locator('fieldset').nth(24).waitFor();
 const itemList=check.getByRole('group',{name:'本次检查项目',exact:true});
 for(const viewport of [{width:1440,height:900},{width:402,height:874},{width:440,height:956},{width:360,height:640}]){
  await page.setViewportSize(viewport);await itemList.scrollIntoViewIfNeeded();
  assert(await itemList.evaluate(el=>el.scrollHeight>el.clientHeight&&el.clientHeight<=481),'long checklists have a bounded scroll area');
  assert(await check.locator('legend').first().evaluate(el=>parseFloat(getComputedStyle(el).fontSize)>=16),'check descriptions stay readable');
  await check.locator('fieldset').first().locator('.v2-check-normal').click();
  await check.locator('fieldset').last().locator('.v2-check-abnormal').click();
  await check.locator('fieldset').last().getByRole('textbox').fill('末项异常备注');
  assert(await itemList.evaluate(el=>el.scrollTop>0),'the last check and its note are reachable within the list');
  assert(await check.locator('fieldset').first().getByRole('radio',{name:'无异常',exact:true}).isChecked(),'scrolling retains earlier answers');
  assert(!await check.evaluate(el=>el.scrollWidth>el.clientWidth+1),'check text and controls never overflow the widget');
  await check.getByRole('button',{name:'提交检查',exact:true}).scrollIntoViewIfNeeded();
  assert.equal(await check.locator('.v2-check-form>footer').evaluate(el=>el.previousElementSibling.classList.contains('v2-check-items')),true,'submit remains outside the scroll list');
  await itemList.evaluate(el=>el.scrollTop=0);await check.scrollIntoViewIfNeeded();await page.screenshot({path:path.join(output,'long-check-'+viewport.width+'.png'),fullPage:true});
 }
 await page.setViewportSize({width:1440,height:700});await main.evaluate(el=>el.scrollTop=0);await body.evaluate(el=>el.scrollTo({top:el.scrollHeight,behavior:'instant'}));await itemList.evaluate(el=>el.scrollTop=el.scrollHeight);await page.waitForTimeout(500);const bounds=await body.boundingBox();assert(bounds);await page.mouse.move(bounds.x+20,Math.min(600,bounds.y+60));await page.mouse.wheel(0,240);await page.waitForTimeout(300);assert(await main.evaluate(el=>el.scrollTop)>0,'widget boundary chains scrolling to the page');await page.setViewportSize({width:1440,height:960});
 await page.getByRole('menuitem',{name:'SOP 步骤编辑',exact:true}).click();await page.getByRole('button',{name:'放弃并离开',exact:true}).click();
 await page.locator('.v2-sop-manager.is-page').waitFor();assert.equal(await page.locator('.n-modal:visible').count(),0,'SOP editor is a page, not a notice modal');await page.getByRole('button',{name:'新增',exact:true}).click();await page.getByRole('textbox',{name:'SOP名称',exact:true}).fill('未保存模板');
 await page.getByRole('menuitem',{name:'个人工作台',exact:true}).click();await page.getByText('放弃未保存的SOP修改？',{exact:true}).waitFor();await page.getByRole('button',{name:'继续编辑',exact:true}).click();
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:960});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1));await page.screenshot({path:path.join(output,'sop-page-'+width+'.png'),fullPage:true});}
 assert.equal(await page.getByRole('button',{name:'返回SOP列表',exact:true}).count(),0);await page.getByRole('button',{name:'取消修改',exact:true}).click();await page.getByRole('button',{name:'放弃修改',exact:true}).click();await page.locator('.v2-sop-content').waitFor({state:'detached'});assert.match(page.url(),/view=sops/);await page.locator('.n-dialog:visible').waitFor({state:'hidden'});assert.equal(await page.locator('.n-dialog:visible').count(),0,'leaving SOP confirms once');
 submitted=true;overviewOverride={pending:1,rows:[{id:'A-day',scope:'A',day,shift:'白',status:'已完成',total:2,completed:2},{id:'B-day',scope:'B',day,shift:'白',status:'已完成',total:2,completed:2}]};
 await page.goto(origin+'/assets/workspace-v2/index.html');await check.locator('.v2-check-overview').waitFor();
 for(const width of [1440,390]){await page.setViewportSize({width,height:width<700?844:960});assert(await check.evaluate(el=>el.classList.contains('has-pending-checks')),'global pulse retains an older overdue period even when current shifts are complete');}
 overviewOverride.pending=0;await page.evaluate(()=>window.dispatchEvent(new Event('clipflow-business-changed')));await page.waitForFunction(()=>!document.querySelector('.v2-shift-check')?.classList.contains('has-pending-checks'));
 overviewOverride.pending=1;overviewOverride.rows[1].status='待完成';await refresh.click();await check.locator('.v2-check-alert').waitFor();
 await check.getByRole('button',{name:'查看A楼检查',exact:true}).click();await check.locator('.v2-check-workspace').waitFor();assert(!await check.evaluate(el=>el.classList.contains('has-pending-checks')),'B pending work does not pulse inside completed A building view');
 await check.getByRole('button',{name:'返回楼栋列表',exact:true}).click();assert(await check.evaluate(el=>el.classList.contains('has-pending-checks')),'global view still signals B');
 delete overviewOverride.pending;await refresh.click();assert(await check.evaluate(el=>el.classList.contains('has-pending-checks')),'older server responses retain row-based fallback');
 mode='restricted';await page.goto(origin+'/assets/workspace-v2/index.html?view=sops');await page.getByText('没有该功能权限',{exact:true}).waitFor();assert.equal(await page.getByRole('menuitem',{name:'SOP 步骤编辑',exact:true}).count(),0,'change-only accounts cannot open SOP editor');
 assert.deepEqual(errors,[]);console.log('PASS: checklist lifecycle and permissions; photo single-upload/metadata confirmation/cancel; mouse, touch and keyboard watermark placement; Logo, capture-time audit and account styles; fixed small-phone actions; SOP navigation/dirty guard. PC/mobile, no real writes.');
}catch(e){if(page){await page.screenshot({path:path.join(output,'failure.png'),fullPage:true});console.error(errors,await page.locator('body').innerText());}throw e;}finally{await browser.close();await server.close();}
