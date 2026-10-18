<template>
  <div class="v2-notice-photo-uploader" :data-notice-upload-kind="ali?'ali':'site'">
    <div class="v2-notice-photo-strip"><NImageGroup><div class="v2-notice-photos"><slot /><figure v-for="photo in modelValue" :key="photo.upload_id"><NImage v-if="preview(photo)" :src="preview(photo)" :alt="photo.file_name||'新增图片'" width="108" /><span>{{ photo.file_name }}</span><small>本次新增 · 已就绪</small><NButton quaternary circle data-v2-navigation :disabled="locked" aria-label="移除本次图片" @click="removeReady(photo)"><X :size="15" /></NButton></figure></div></NImageGroup></div>
    <ul v-if="queue.length" class="v2-photo-queue" aria-live="polite"><li v-for="entry in queue" :key="entry.id"><NSpin v-if="entry.state==='uploading'" size="small" /><CircleAlert v-else-if="entry.state==='failed'" :size="16" /><ImagePlus v-else :size="16" /><span><strong>{{ entry.file.name }}</strong><small>{{ entry.state==='uploading'?'正在上传':entry.state==='queued'?'等待上传':entry.error }}</small></span><NButton v-if="entry.state==='failed'" size="small" data-v2-navigation :disabled="locked" @click="retry(entry)">重试此张</NButton><NButton v-if="entry.state!=='uploading'" quaternary circle data-v2-navigation :disabled="working" aria-label="移除未上传图片" @click="queue=queue.filter(v=>v.id!==entry.id)"><X :size="15" /></NButton></li></ul>
    <label class="v2-notice-upload" @dragover.prevent @drop.prevent="add($event.dataTransfer?.files)" @paste.stop="paste"><ImagePlus :size="20" /><span>{{ working?'正在上传所选图片…':ali?'选择确认截图（最多一张）':'添加现场照片，也可拖入或粘贴' }}</span><input type="file" accept="image/*" :multiple="!ali" :disabled="locked||working" :aria-label="ali?'添加阿里确认截图':'添加现场照片'" @change="input" /></label>
    <p v-if="error" class="v2-photo-error" role="alert">{{ error }}</p>
  </div>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,ref} from 'vue';
import {NButton,NImage,NImageGroup,NSpin} from 'naive-ui';
import {CircleAlert,ImagePlus,X} from 'lucide-vue-next';
import {requestBinaryJson,type Dict} from './client';
import type {NoticeForm} from './noticeForm';
type Entry={id:number;file:File;state:'queued'|'uploading'|'failed';error:string};
const props=defineProps<{form:NoticeForm;modelValue:Dict[];locked:boolean;ali?:boolean}>();
const emit=defineEmits<{'update:modelValue':[value:Dict[]];busy:[value:boolean]}>();
const queue=ref<Entry[]>([]),working=ref(false),error=ref('');let serial=0,disposed=false;
const pending=computed(()=>queue.value.length);
const preview=(p:Dict)=>/^\/api\/notice-images\/[a-f0-9]{32}$/.test(p.preview_url||'')?p.preview_url:'';
function removeReady(photo:Dict){if(!props.locked)emit('update:modelValue',props.modelValue.filter(p=>p.upload_id!==photo.upload_id));}
function add(files:FileList|File[]|undefined|null){
  if(!files?.length||props.locked||working.value)return;error.value='';
  const capacity=(props.ali?1:20)-props.modelValue.length-queue.value.length;
  if(files.length>capacity){error.value=props.ali?'请先移除本次截图，再选择另一张。':'本次最多添加20张图片，请减少选择后重试。';return;}
  for(const file of Array.from(files))queue.value.push({id:++serial,file,state:'queued',error:''});
  void run();
}
async function run(){
  if(working.value||disposed)return;working.value=true;emit('busy',true);
  try{for(const entry of queue.value.filter(v=>v.state==='queued')){
    if(disposed)break;entry.state='uploading';
    try{
      const file=entry.file;if(!file.type.startsWith('image/')||file.size>8*1024*1024)throw new Error('请选择8MB以内的图片');
      const v=props.form.values,result=await requestBinaryJson('/api/notice-attachments?'+new URLSearchParams({scope:v.scope,identity:v.active_item_id||v.manual_id||v.source_record_id||v.record_id,kind:props.ali?'ali':'site',file_name:file.name,...(props.ali&&v.target_record_id?{target_record_id:v.target_record_id}:{})}),file,{headers:{'Content-Type':file.type}});
      if(disposed)break;if(!result.upload_id)throw new Error('未收到图片标识，请重试此张');
      emit('update:modelValue',props.ali?[result]:[...props.modelValue,result]);queue.value=queue.value.filter(v=>v.id!==entry.id);
      // Allow the parent model to receive this successful file before appending the next one.
      await nextTick();
    }catch(e){entry.state='failed';entry.error=(e as Error).message;}
  }}finally{working.value=false;if(!disposed)emit('busy',false);}
}
function retry(entry:Entry){if(props.locked||working.value)return;entry.state='queued';entry.error='';void run();}
function input(event:Event){const el=event.target as HTMLInputElement;add(el.files);el.value='';}
function paste(event:ClipboardEvent){if(event.clipboardData?.files.length){event.preventDefault();add(event.clipboardData.files);}}
onBeforeUnmount(()=>{disposed=true;});
defineExpose({add,pending});
</script>
<style>
.v2-notice-upload:focus-within{outline:2px solid var(--v2-blue);outline-offset:2px}
.v2-notice-photo-uploader{display:grid;grid-template-columns:minmax(0,1fr);gap:6px;align-items:start;min-width:0}.v2-notice-photo-uploader:has(figure){grid-template-columns:minmax(0,1fr) minmax(112px,32%)}.v2-notice-photo-strip:not(:has(figure)){display:none}.v2-notice-photo-strip{min-width:0;overflow-x:auto;scrollbar-width:thin}.v2-notice-photo-strip .v2-notice-photos{flex-wrap:nowrap;gap:6px}.v2-notice-photo-strip .v2-notice-photos figure{flex:none;width:94px;box-sizing:border-box;margin:0;padding:4px}.v2-notice-photo-strip .v2-notice-photos img{height:58px;max-width:100%}.v2-notice-photo-uploader>.v2-notice-upload{grid-column:-2/-1;grid-row:1;box-sizing:border-box;min-height:68px;padding:8px;font-size:12px;line-height:1.5}.v2-notice-photo-uploader>.v2-photo-queue,.v2-notice-photo-uploader>.v2-photo-error{grid-column:1/-1}.v2-notice-photo-uploader>.v2-photo-queue{grid-row:2;margin:0}.v2-notice-photo-uploader>.v2-photo-error{margin:0}.v2-notice-photo-uploader:has(figure)>.v2-notice-upload{min-height:96px}.v2-notice-photo-uploader .v2-notice-upload>svg{flex:none}
.v2-photo-queue{list-style:none;padding:0;margin:0 0 12px}.v2-photo-queue li{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--v2-border)}.v2-photo-queue li>span{flex:1;min-width:0}.v2-photo-queue strong{display:block;font-size:13px;font-weight:500;overflow-wrap:anywhere}.v2-photo-queue small,.v2-notice-photos small{display:block;color:var(--v2-muted);font-size:12px;margin-top:3px}.v2-photo-error{font-size:12px;color:#b42318}
.v2-notice-photo-uploader:has(figure){grid-template-columns:minmax(0,max-content) minmax(112px,180px);justify-content:start}.v2-notice-photo-strip .v2-notice-photos figure{width:116px}.v2-notice-photo-strip .v2-notice-photos small{font-size:11px;white-space:nowrap}.v2-notice-photo-strip .n-image{max-width:100%}
</style>
