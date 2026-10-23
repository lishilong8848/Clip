<template>
  <div class="v2-check-attachments" :class="{'is-dragging':dragging,'is-handover':kind==='handover'}" data-v2-navigation role="group" :aria-label="kind==='handover'?'交接班照片上传区':'检查附件上传区'" :tabindex="locked||allowUpload===false?-1:0" @paste="paste" @dragenter.prevent="dragEnter" @dragover.prevent @dragleave="dragLeave" @drop.prevent.stop="drop" @keydown.enter.self.prevent="photos?.click()" @keydown.space.self.prevent="photos?.click()">
    <div class="v2-check-file-list"><div v-for="file in shownFiles" :key="file.id" class="v2-check-file">
      <button v-if="file.is_image" type="button" class="v2-check-photo-thumb" :aria-label="'查看'+file.name" @click="showPhoto(file)"><img :src="url(file)" :alt="file.name" /></button>
      <FileText v-else :size="18" /><a :href="url(file)" :target="file.is_image?undefined:'_blank'" rel="noopener" @click="openFile($event,file)">{{file.name}}</a>
      <NButton v-if="file.is_image" size="small" quaternary :disabled="working" @click="showPhoto(file)"><Download :size="14" />保存照片</NButton>
      <NButton v-if="file.can_delete" quaternary circle :disabled="locked||working" :aria-label="'移除'+file.name" @click="remove(file)"><X :size="14" /></NButton>
      <span v-if="file.delivery_status" class="v2-check-photo-delivery" :class="{'has-error':!!file.delivery_error}" role="status">{{file.delivery_status==='sent'?'照片已发送给当前登录账号':file.delivery_error||'照片已保存，正在发送给当前登录账号'}}</span>
    </div></div>
    <div v-if="!locked&&allowUpload!==false" class="v2-check-file-actions">
      <NButton attr-type="button" size="small" secondary :disabled="working" @click="photos?.click()"><ImagePlus :size="15" />{{replacingHandover?'替换照片':'上传照片'}}</NButton>
      <NButton v-if="kind!=='handover'" attr-type="button" size="small" quaternary :disabled="working" @click="picker?.click()"><Paperclip :size="15" />添加文件</NButton>
      <NButton attr-type="button" size="small" secondary :disabled="working" @click="camera?.click()"><Camera :size="15" />拍照</NButton>
    </div>
    <div v-if="working" class="v2-check-file-actions" role="status"><NSpin :size="14" />{{editor?'正在编辑照片':'正在处理'}} {{uploadName}}</div>
    <input ref="photos" class="v2-check-file-input" type="file" accept="image/jpeg,image/png,image/webp" :multiple="kind!=='handover'" :aria-label="kind==='handover'?'上传交接班照片':'上传检查照片'" :disabled="locked||working||allowUpload===false" @change="pick" />
    <input ref="camera" class="v2-check-file-input" type="file" accept="image/jpeg,image/png,image/webp" capture="environment" :aria-label="kind==='handover'?'拍摄交接班照片':'拍摄检查照片'" :disabled="locked||working||allowUpload===false" @change="pick" />
    <input v-if="kind!=='handover'" ref="picker" class="v2-check-file-input" type="file" accept=".jpg,.jpeg,.png,.webp,.pdf,.doc,.docx,.xls,.xlsx,.md,.txt,.csv" multiple aria-label="上传检查附件" :disabled="locked||working||allowUpload===false" @change="pick" />
    <p v-if="error" class="v2-check-file-error" role="alert">{{error}}</p>
  </div>
  <NModal :show="!!previewPhoto" preset="card" title="照片预览与保存" class="v2-check-photo-preview" :auto-focus="false" @update:show="value=>{if(!value)closePhoto()}">
    <template v-if="previewPhoto">
      <img :src="previewPhoto.url" :alt="previewPhoto.file.name" />
      <p role="status">{{saveStatus||'照片已保存到灯塔。手机可打开系统保存，或长按照片保存到相册。'}}</p>
      <p v-if="previewPhoto.meta.preference_warning" role="status">{{previewPhoto.meta.preference_warning}}</p>
      <p v-if="photoAudit" class="v2-check-photo-audit">{{photoAudit}}</p>
      <div class="v2-check-file-actions">
        <NButton secondary @click="downloadPhoto"><Download :size="16" />下载到本机</NButton>
        <NButton v-if="canSharePhoto" type="primary" @click="sharePhoto"><ImagePlus :size="16" />打开系统保存</NButton>
        <NButton quaternary @click="closePhoto">完成</NButton>
      </div>
    </template>
  </NModal>
  <PreShiftPhotoEditor v-if="editor" :key="editor.file.name" :info="editor.info" :busy="editorSaving" :error="editorError" @save="saveEditedPhoto" @cancel="cancelEditor" />
</template>
<script setup lang="ts">
import {ref,computed,watch,onBeforeUnmount} from 'vue';
import {NButton,NSpin,NModal,useDialog} from 'naive-ui';
import {Camera,Paperclip,ImagePlus,FileText,X,Download} from 'lucide-vue-next';
import {requestBinaryJson,requestJson,type Dict} from '../client';
import {requestFile} from '../../api/client';
import PreShiftPhotoEditor from './PreShiftPhotoEditor.vue';
const props=withDefaults(defineProps<{taskId:string;itemId?:string;kind:'item'|'handover';files:Dict[];locked:boolean;allowUpload?:boolean;revision?:number}>(),{allowUpload:true});
const emit=defineEmits<{changed:[result?:Dict];busy:[value:boolean]}>();
const camera=ref<HTMLInputElement>(),photos=ref<HTMLInputElement>(),picker=ref<HTMLInputElement>(),working=ref(false),error=ref(''),uploadName=ref(''),dragging=ref(false);
const dialog=useDialog();let disposed=false,dragDepth=0;
const uploaded=ref<Dict[]>([]),previewPhoto=ref<{url:string;file:File;meta:Dict}>(),saveStatus=ref('');
const removed=ref<string[]>([]),mutationRevision=ref(props.revision);
const editor=ref<{file:File;info:Dict;taskId:string}>(),editorSaving=ref(false),editorError=ref('');
let resolveEditor:((file:Dict|null)=>void)|undefined,uploadController:AbortController|undefined;
const shownFiles=computed(()=>(props.kind==='handover'&&uploaded.value.length?uploaded.value.slice(-1):[...props.files,...uploaded.value.filter(file=>!props.files.some(item=>item.id===file.id))]).filter(file=>!removed.value.includes(file.id)));
const replacingHandover=computed(()=>props.kind==='handover'&&props.revision!==undefined&&shownFiles.value.length===1);
const canSharePhoto=computed(()=>Boolean(previewPhoto.value&&navigator.canShare?.({files:[previewPhoto.value.file]})));
const photoAudit=computed(()=>{const audit=previewPhoto.value?.meta.watermark?.audit;if(!audit?.display_time)return '';return '拍照时间：'+audit.display_time+' · '+(audit.modified?(audit.original_time?'手动修改；原始 '+audit.original_time:'手动填写'):audit.source);});
function url(file:Dict){return '/api/pre-shift-checks/files/'+encodeURIComponent(file.id);}
function openFile(event:MouseEvent,file:Dict){if(file.is_image){event.preventDefault();void showPhoto(file);}}
function pick(event:Event){const input=event.target as HTMLInputElement,files=Array.from(input.files||[]);input.value='';void upload(files,input===camera.value);}
function paste(event:ClipboardEvent){const files=Array.from(event.clipboardData?.files||[]);if(!files.length)return;event.preventDefault();event.stopPropagation();void upload(files);}
function dragEnter(event:DragEvent){if(!event.dataTransfer?.types.includes('Files'))return;dragDepth++;dragging.value=!props.locked&&props.allowUpload!==false&&!working.value;}
function dragLeave(){if(--dragDepth<=0){dragDepth=0;dragging.value=false;}}
function drop(event:DragEvent){dragDepth=0;dragging.value=false;void upload(Array.from(event.dataTransfer?.files||[]));}
function currentPosition():Promise<Dict>{return new Promise(resolve=>{if(!navigator.geolocation){resolve({});return;}const timer=setTimeout(()=>resolve({}),3000);try{navigator.geolocation.getCurrentPosition(value=>{clearTimeout(timer);resolve({position:{latitude:value.coords.latitude,longitude:value.coords.longitude,accuracy:value.coords.accuracy,timestamp:value.timestamp}});},()=>{clearTimeout(timer);resolve({});},{enableHighAccuracy:true,maximumAge:0,timeout:3000});}catch{clearTimeout(timer);resolve({});}});}
function photoRequest(file:File,context:Dict,captured:boolean,preview=false){
  const metadata=JSON.stringify(context),header=encodeURIComponent(metadata);
  if(metadata.length>4096||header.length>7000)throw new Error('水印文字和Logo信息过长，请缩短文字后重试。');
  uploadController=new AbortController();
  return requestBinaryJson((preview?'/api/pre-shift-checks/photo-preview?':'/api/pre-shift-checks/files?')+new URLSearchParams({task_id:props.taskId,item_id:props.itemId||'',kind:props.kind,name:file.name,capture:captured?'1':'0',retain:preview?'1':'0',...(mutationRevision.value!==undefined?{revision:String(mutationRevision.value)}:{}),...(replacingHandover.value?{replace_file_id:shownFiles.value[0].id}:{})}),file,
    {signal:uploadController.signal,headers:{'Content-Type':file.type||'application/octet-stream','X-Photo-Context':header}});
}
async function saveEditedPhoto(options:Dict,remember:boolean){
  const pending=editor.value;if(!pending||editorSaving.value)return;editorSaving.value=true;editorError.value='';
  uploadController=new AbortController();
  try{const saved=await requestJson('/api/pre-shift-checks/files',{method:'POST',body:JSON.stringify({preview_id:pending.info.preview_id,watermark:options,remember_style:remember}),signal:uploadController.signal});if(disposed)return;editor.value=undefined;const resolve=resolveEditor;resolveEditor=undefined;resolve?.(saved);}
  catch(e){if(!disposed)editorError.value=(e as Error).message;}finally{editorSaving.value=false;}
}
function cancelEditor(){if(editorSaving.value)return;const pending=editor.value;editor.value=undefined;const resolve=resolveEditor;resolveEditor=undefined;resolve?.(null);if(pending?.info.preview_id)void requestJson('/api/pre-shift-checks/photo-preview',{method:'DELETE',body:JSON.stringify({preview_id:pending.info.preview_id})}).catch(e=>{if(!disposed&&pending.taskId===props.taskId)error.value=(e as Error).message;}).finally(()=>{if(!disposed)emit('changed');});}
async function upload(files:File[],captured=false){
  if(props.locked||props.allowUpload===false||working.value||!files.length)return;
  working.value=true;emit('busy',true);error.value='';const capturedAt=Date.now(),taskId=props.taskId;
  let lastPhoto:Dict|undefined;
  try{
    if(files.length+(replacingHandover.value?0:shownFiles.value.length)>(props.kind==='handover'?1:10))throw new Error(props.kind==='handover'?'本班交接班照片只需上传一次。':'每项最多10个附件。');
    for(const file of files){if(!file.size||file.size>20*1024*1024)throw new Error('请选择20MB以内的非空文件。');if(!(props.kind==='handover'?/\.(jpe?g|png|webp)$/i:/\.(jpe?g|png|webp|pdf|docx?|xlsx?|md|txt|csv)$/i).test(file.name))throw new Error('不支持此文件类型。');}
    const context=files.some(file=>/\.(jpe?g|png|webp)$/i.test(file.name))?await currentPosition():{};
    if(captured)context.captured_at=capturedAt;
    for(const file of files){
      if(disposed||taskId!==props.taskId)break;uploadName.value=file.name;
      let saved:Dict|null;
      if(/\.(jpe?g|png|webp)$/i.test(file.name)){
        const info=await photoRequest(file,context,captured,true);
        if(disposed||taskId!==props.taskId)break;
        if(!info.image||!info.options||!info.preview_id)throw new Error('照片暂存未完整返回，请重新选择。');
        editorError.value='';
        saved=await new Promise(resolve=>{resolveEditor=resolve;editor.value={file,info,taskId};});
      }else saved=await photoRequest(file,context,captured);
      if(disposed||taskId!==props.taskId)break;
      if(saved){uploaded.value.push(saved);if(Number.isInteger(saved.revision))mutationRevision.value=saved.revision;emit('changed',saved);if(saved.is_image)lastPhoto=saved;}
    }
    if(lastPhoto&&!disposed)await showPhoto(lastPhoto,captured);
  }catch(e){if(!disposed)error.value=(e as Error).message;emit('changed');}
  finally{working.value=false;emit('busy',false);}
}
async function showPhoto(meta:Dict,autoDownload=false){const taskId=props.taskId;try{const file=await requestFile(url(meta),{cache:'no-store'});if(disposed||taskId!==props.taskId)return;if(!file.type.startsWith('image/'))throw new Error('返回内容不是图片。');closePhoto();previewPhoto.value={file,url:URL.createObjectURL(file),meta};if(autoDownload&&matchMedia('(pointer:fine)').matches)downloadPhoto();}catch(e){error.value='照片已保存到灯塔，但预览读取失败：'+(e as Error).message;}}
function downloadPhoto(){const photo=previewPhoto.value;if(!photo)return;const link=document.createElement('a');link.href=photo.url;link.download=photo.file.name;document.body.appendChild(link);link.click();link.remove();saveStatus.value='已请求下载，请查看本机下载记录；手机如未保存，可打开系统保存或长按照片。';}
async function sharePhoto(){if(!previewPhoto.value)return;try{await navigator.share({files:[previewPhoto.value.file]});saveStatus.value='请在系统菜单中选择保存图片；相册是否保存成功以手机提示为准。';}catch(e){if((e as Error).name!=='AbortError')saveStatus.value='当前浏览器不能打开系统保存，请长按照片保存到相册。';}}
function closePhoto(){if(previewPhoto.value){const old=previewPhoto.value.url;setTimeout(()=>URL.revokeObjectURL(old),60000);}previewPhoto.value=undefined;saveStatus.value='';}
function remove(file:Dict){if(props.locked||working.value)return;const taskId=props.taskId;dialog.warning({title:'移除此附件？',content:file.name,positiveText:'移除',negativeText:'取消',onPositiveClick:async()=>{if(taskId!==props.taskId||props.locked||working.value)return false;working.value=true;emit('busy',true);try{const suffix=mutationRevision.value!==undefined?'?'+new URLSearchParams({task_id:taskId,revision:String(mutationRevision.value)}):'';const result=await requestJson(url(file)+suffix,{method:'DELETE'});uploaded.value=uploaded.value.filter(item=>item.id!==file.id);removed.value.push(file.id);if(Number.isInteger(result.revision))mutationRevision.value=result.revision;emit('changed',result);}catch(e){error.value=(e as Error).message;return false;}finally{working.value=false;emit('busy',false);}}});}
watch(()=>[props.files,props.revision] as const,([files,revision])=>{
  // Once a server snapshot acknowledges an upload, local receipts must not resurrect removed files.
  uploaded.value=revision!==undefined&&revision>=(mutationRevision.value??0)?[]:uploaded.value.filter(file=>!files.some(item=>item.id===file.id));
  removed.value=removed.value.filter(id=>files.some(file=>file.id===id));
  if(revision!==undefined)mutationRevision.value=Math.max(revision,mutationRevision.value??0);
});
watch(()=>[props.taskId,props.itemId],()=>{uploadController?.abort();editorSaving.value=false;cancelEditor();uploaded.value=[];removed.value=[];mutationRevision.value=props.revision;closePhoto();error.value='';});
onBeforeUnmount(()=>{disposed=true;uploadController?.abort();editorSaving.value=false;cancelEditor();closePhoto();});
</script>
<style>
/* Keep hidden pickers inside their upload area, not the modal's outer scroll frame. */
.v2-check-attachments{position:relative}.v2-check-file-input{top:0;left:0}
.v2-check-file-actions{scroll-margin-block:12px}.v2-check-file-actions .n-button{min-height:44px}
.v2-check-photo-preview.n-card{width:min(800px,calc(100vw - 24px));max-height:92dvh;overflow:auto}.v2-check-photo-preview img{display:block;width:100%;max-height:65dvh;object-fit:contain;touch-action:manipulation}.v2-check-photo-preview p{font-size:12px;line-height:1.5;color:var(--v2-muted)}
.v2-check-photo-thumb{display:block;border:0;padding:0;background:transparent;cursor:pointer;border-radius:4px;overflow:hidden;min-width:44px;min-height:44px}.v2-check-photo-thumb img{display:block;width:56px;height:44px;object-fit:contain}.is-handover .v2-check-photo-thumb{grid-column:1/-1;width:100%}.is-handover .v2-check-photo-thumb img{width:auto;max-width:100%;height:auto;max-height:320px;margin:auto}@media(max-width:760px){.is-handover .v2-check-photo-thumb img{max-height:240px}}
.v2-check-attachments{outline:2px solid transparent;outline-offset:3px;transition:outline-color .18s,background-color .18s;border-radius:6px}.v2-check-attachments:focus-visible,.v2-check-attachments.is-dragging{outline-color:var(--v2-blue)}.v2-check-attachments.is-dragging{background:color-mix(in srgb,var(--v2-blue) 10%,transparent)}
.v2-check-attachments.is-handover .v2-check-file-list{display:block}.v2-check-attachments.is-handover .v2-check-file{display:grid;grid-template-columns:minmax(0,1fr) auto;width:100%;box-sizing:border-box;padding:8px;gap:8px}.v2-check-attachments.is-handover .n-image{grid-column:1/-1;width:100%;justify-content:center}.v2-check-attachments.is-handover .n-image img{max-width:100%;width:auto;max-height:320px;object-fit:contain}.v2-check-photo-delivery{grid-column:1/-1;color:var(--v2-muted);line-height:1.5}.v2-check-photo-delivery.has-error{color:#a35c10}@media(max-width:760px){.v2-check-attachments.is-handover .n-image img{max-height:240px}}
.v2-check-attachments{grid-column:1/-1;min-width:0}.v2-check-file-list{display:flex;gap:6px;flex-wrap:wrap}.v2-check-file{display:flex;align-items:center;gap:6px;max-width:100%;padding:4px 6px;border:1px solid var(--v2-border);border-radius:6px;font-size:12px}.v2-check-file a{overflow-wrap:anywhere;min-width:0;color:var(--v2-blue)}.v2-check-file-actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:8px}.v2-check-file-actions>span{display:flex;gap:6px;font-size:12px;overflow-wrap:anywhere}.v2-check-file-input{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}.v2-check-file-error{margin:6px 0;color:#b42318;font-size:12px}.v2-check-handover{padding:12px 0;border-top:1px solid var(--v2-border)}.v2-check-handover h3{font-size:14px;margin:0 0 8px}
</style>
