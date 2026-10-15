<template>
  <div class="v2-check-attachments" :class="{'is-dragging':dragging}" data-v2-navigation role="group" :aria-label="kind==='handover'?'交接班照片上传区':'检查附件上传区'" :tabindex="locked||allowUpload===false?-1:0" @paste="paste" @dragenter.prevent="dragEnter" @dragover.prevent @dragleave="dragLeave" @drop.prevent.stop="drop" @keydown.enter.self.prevent="photos?.click()" @keydown.space.self.prevent="photos?.click()">
    <div class="v2-check-file-list"><div v-for="file in files" :key="file.id" class="v2-check-file">
      <NImage v-if="file.is_image" :src="url(file)" :alt="file.name" width="56" height="44" object-fit="cover" />
      <FileText v-else :size="18" /><a :href="url(file)" target="_blank" rel="noopener">{{file.name}}</a>
      <NButton v-if="file.can_delete" quaternary circle :disabled="locked||working" :aria-label="'移除'+file.name" @click="remove(file)"><X :size="14" /></NButton>
    </div></div>
    <div v-if="!locked&&allowUpload!==false" class="v2-check-file-actions">
      <NButton size="small" secondary :disabled="working" @click="photos?.click()"><ImagePlus :size="15" />上传照片</NButton>
      <NButton v-if="kind!=='handover'" size="small" quaternary :disabled="working" @click="picker?.click()"><Paperclip :size="15" />添加文件</NButton>
      <NButton size="small" secondary :disabled="working" @click="camera?.click()"><Camera :size="15" />拍照</NButton>
    </div>
    <div v-if="working" class="v2-check-file-actions" role="status"><NSpin :size="14" />正在保存 {{uploadName}}</div>
    <input ref="photos" class="v2-check-file-input" type="file" accept="image/jpeg,image/png,image/webp" :multiple="kind!=='handover'" :aria-label="kind==='handover'?'上传交接班照片':'上传检查照片'" :disabled="locked||working||allowUpload===false" @change="pick" />
    <input ref="camera" class="v2-check-file-input" type="file" accept="image/jpeg,image/png,image/webp" capture="environment" :aria-label="kind==='handover'?'拍摄交接班照片':'拍摄检查照片'" :disabled="locked||working||allowUpload===false" @change="pick" />
    <input v-if="kind!=='handover'" ref="picker" class="v2-check-file-input" type="file" accept=".jpg,.jpeg,.png,.webp,.pdf,.doc,.docx,.xls,.xlsx,.md,.txt,.csv" multiple aria-label="上传检查附件" :disabled="locked||working||allowUpload===false" @change="pick" />
    <p v-if="error" class="v2-check-file-error" role="alert">{{error}}</p>
  </div>
</template>
<script setup lang="ts">
import {ref,onBeforeUnmount} from 'vue';
import {NButton,NImage,NSpin,useDialog} from 'naive-ui';
import {Camera,Paperclip,ImagePlus,FileText,X} from 'lucide-vue-next';
import {requestBinaryJson,requestJson,type Dict} from '../client';
const props=withDefaults(defineProps<{taskId:string;itemId?:string;kind:'item'|'handover';files:Dict[];locked:boolean;allowUpload?:boolean}>(),{allowUpload:true});
const emit=defineEmits<{changed:[];busy:[value:boolean]}>();
const camera=ref<HTMLInputElement>(),photos=ref<HTMLInputElement>(),picker=ref<HTMLInputElement>(),working=ref(false),error=ref(''),uploadName=ref(''),dragging=ref(false);
const dialog=useDialog();let disposed=false,dragDepth=0;
function url(file:Dict){return '/api/pre-shift-checks/files/'+encodeURIComponent(file.id);}
function pick(event:Event){const input=event.target as HTMLInputElement,files=Array.from(input.files||[]);input.value='';void upload(files);}
function paste(event:ClipboardEvent){const files=Array.from(event.clipboardData?.files||[]);if(!files.length)return;event.preventDefault();event.stopPropagation();void upload(files);}
function dragEnter(event:DragEvent){if(!event.dataTransfer?.types.includes('Files'))return;dragDepth++;dragging.value=!props.locked&&props.allowUpload!==false&&!working.value;}
function dragLeave(){if(--dragDepth<=0){dragDepth=0;dragging.value=false;}}
function drop(event:DragEvent){dragDepth=0;dragging.value=false;void upload(Array.from(event.dataTransfer?.files||[]));}
async function upload(files:File[]){if(props.locked||props.allowUpload===false||working.value||!files.length)return;working.value=true;emit('busy',true);error.value='';
  try{if(files.length+props.files.length>(props.kind==='handover'?1:10))throw new Error(props.kind==='handover'?'本班交接班照片只需上传一次。':'每项最多10个附件。');for(const file of files){if(!file.size||file.size>20*1024*1024)throw new Error('请选择20MB以内的非空文件。');if(!(props.kind==='handover'?/\.(jpe?g|png|webp)$/i:/\.(jpe?g|png|webp|pdf|docx?|xlsx?|md|txt|csv)$/i).test(file.name))throw new Error(props.kind==='handover'?'交接班照片仅支持 JPG、PNG 或 WebP。':'不支持此文件类型。');}for(const file of files){if(disposed)break;uploadName.value=file.name;await requestBinaryJson('/api/pre-shift-checks/files?'+new URLSearchParams({task_id:props.taskId,item_id:props.itemId||'',kind:props.kind,name:file.name}),file,{headers:{'Content-Type':file.type||'application/octet-stream'}});emit('changed');}}
  catch(e){error.value=(e as Error).message;emit('changed');}finally{working.value=false;emit('busy',false);}}
function remove(file:Dict){dialog.warning({title:'移除此附件？',content:file.name,positiveText:'移除',negativeText:'取消',onPositiveClick:async()=>{working.value=true;emit('busy',true);try{await requestJson(url(file),{method:'DELETE'});emit('changed');}catch(e){error.value=(e as Error).message;return false;}finally{working.value=false;emit('busy',false);}}});}
onBeforeUnmount(()=>{disposed=true;});
</script>
<style>
.v2-check-attachments{outline:2px solid transparent;outline-offset:3px;transition:outline-color .18s,background-color .18s;border-radius:6px}.v2-check-attachments:focus-visible,.v2-check-attachments.is-dragging{outline-color:var(--v2-blue)}.v2-check-attachments.is-dragging{background:color-mix(in srgb,var(--v2-blue) 10%,transparent)}
.v2-check-attachments{grid-column:1/-1;min-width:0}.v2-check-file-list{display:flex;gap:6px;flex-wrap:wrap}.v2-check-file{display:flex;align-items:center;gap:6px;max-width:100%;padding:4px 6px;border:1px solid var(--v2-border);border-radius:6px;font-size:12px}.v2-check-file a{overflow-wrap:anywhere;min-width:0;color:var(--v2-blue)}.v2-check-file-actions{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-top:8px}.v2-check-file-actions>span{display:flex;gap:6px;font-size:12px;overflow-wrap:anywhere}.v2-check-file-input{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}.v2-check-file-error{margin:6px 0;color:#b42318;font-size:12px}.v2-check-handover{padding:12px 0;border-top:1px solid var(--v2-border)}.v2-check-handover h3{font-size:14px;margin:0 0 8px}
</style>
