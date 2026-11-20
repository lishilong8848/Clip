<template>
  <NModal :show="true" preset="card" class="change-inbox" title="变更确认" aria-label="变更确认" data-v2-inline-feedback :mask-closable="false" :closable="!busy" :close-on-esc="!busy && !imageOpen" @update:show="close">
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert><NAlert v-if="warning" type="warning">{{ warning }}</NAlert>
    <div class="change-controls"><div class="serenity-segment" role="group" aria-label="变更确认状态">
      <button v-for="mode in filters" :key="mode.value" type="button" :aria-pressed="mode.value===filter" @click="filter=mode.value">{{ mode.label }}</button></div>
      <NInput v-model:value="search" size="small" clearable placeholder="名称或楼栋" :input-props="{'aria-label':'搜索变更确认'}"/><NButton size="small" :disabled="busy" :loading="loading" @click="load">刷新</NButton>
    </div>
    <NSpin :show="loading"><div v-if="selected" class="change-spread">
      <section class="change-queue" aria-label="待核对变更"><button v-for="item in pageItems" :key="item.target_record_id" type="button" :aria-pressed="selected.target_record_id===item.target_record_id" @click="selectedId=item.target_record_id">
        <small>{{ item.building }} · {{ stateLabel(item.state) }}</small><strong>{{ item.title }}</strong><time v-if="item.started_at">{{ item.started_at }}</time></button>
      </section>
      <article class="change-proof" :key="selected.target_record_id">
        <header><span>{{ selected.building }} · {{ selected.target_status }}</span><NTag :type="selected.state==='confirmed'?'success':'warning'" size="small">{{ stateLabel(selected.state) }}</NTag></header>
        <h2>{{ selected.title }}</h2><p v-if="selected.started_at">变更开始 · {{ selected.started_at }}</p>
        <NAlert v-if="selected.last_error" type="warning">{{ selected.last_error }}</NAlert>
        <NImageGroup v-model:show="imageOpen"><div class="change-photos"><figure v-for="photo in photos" :key="photo.local_image_id || photo.file_token">
          <NImage :src="photo.preview_url" :alt="photo.file_name || '确认截图'" width="145"/><figcaption>{{ photo.file_name || '确认截图' }}</figcaption>
          <NButton v-if="photo.target_written" quaternary circle size="small" :disabled="busy" aria-label="删除确认截图" @click="remove(selected,photo)"><Trash2 :size="15"/></NButton>
        </figure></div></NImageGroup>
        <p v-if="!photos.length" role="status">{{ selected.screenshot_count?'已关联截图，暂时无法预览':'尚未上传确认截图' }}</p>
        <footer><label class="change-file"><ImagePlus :size="17"/>{{ pending[selected.target_record_id]?.file_name||'选择确认截图' }}<input type="file" accept="image/*" :disabled="busy" aria-label="选择确认截图" @change="stage(selected,$event)"/></label>
          <NButton v-if="pending[selected.target_record_id]" :loading="busy" @click="upload(selected)">上传截图</NButton>
          <NButton v-if="canConfirm" type="primary" :disabled="busy||selected.state!=='awaiting_confirmation'" @click="confirm(selected)">确认变更</NButton></footer>
      </article></div><NEmpty v-else-if="!loading && !error" description="当前没有符合条件的变更"/></NSpin>
    <template #footer><div class="change-tools"><span>{{ visible.length }} 项</span><NPagination v-if="visible.length>10" v-model:page="page" :item-count="visible.length" :page-size="10" simple/>
      <NButton :disabled="busy" data-v2-navigation @click="close">关闭</NButton></div></template>
  </NModal>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,ref,watch} from 'vue';
import {NAlert,NButton,NEmpty,NImage,NImageGroup,NInput,NModal,NPagination,NSpin,NTag,useDialog,useMessage} from 'naive-ui';
import {ImagePlus,Trash2} from 'lucide-vue-next';
import {requestJson,requestBinaryJson,type Dict} from './client';
import {registerNavigationGuard} from './navigation';
import {noticeInScope,noticeBuildings} from './noticeExperience';
const props=defineProps<{scope:string;focusId?:string}>(),emit=defineEmits<{close:[];changed:[]}>(),dialog=useDialog(),message=useMessage();
const items=ref<Dict[]>([]),pending=ref<Record<string,Dict>>({}),busy=ref(false),loading=ref(false),error=ref(''),warning=ref(''),canConfirm=ref(false),page=ref(1),filter=ref(props.focusId?'all':'pending'),search=ref('');
const filters=[{value:'pending',label:'待处理'},{value:'missing_screenshot',label:'待上传截图'},{value:'awaiting_confirmation',label:'待确认'},{value:'all',label:'全部未结束'}];
const visible=computed(()=>items.value.filter(i=>noticeInScope(i,props.scope)&&(filter.value==='all'||filter.value==='pending'&&i.state!=='confirmed'||filter.value===i.state)&&(!search.value||String(i.title+' '+i.building).includes(search.value))));
const stateLabel=(v:string)=>({missing_screenshot:'待上传截图',awaiting_confirmation:'待确认',confirmed:'已确认'}[v]||v);
const safeImage=(url:unknown)=>/^\/api\/notice-images\/[a-f0-9]{32}$/.test(String(url||''));
const base=(i:Dict)=>'/api/change-confirmations/'+encodeURIComponent(i.target_record_id);
const selectedId=ref(props.focusId || ''), imageOpen=ref(false);
const pageItems=computed(()=>visible.value.slice((page.value-1)*10,page.value*10));
const selected=computed(()=>pageItems.value.find(item=>item.target_record_id===selectedId.value) || pageItems.value[0]);
watch(visible,rows=>{page.value=Math.max(1,Math.min(page.value,Math.ceil(rows.length/10)));});
const photos=computed(()=>{
  const item=selected.value;
  if(!item)return [];
  const local:Dict[]=(item.local_screenshot_items || []).filter((photo:Dict)=>safeImage(photo.preview_url));
  const tokens=new Set(local.map(photo=>photo.file_token).filter(Boolean));
  const result=[...local];
  for(const photo of item.screenshot_items || []){
    const token=photo?.file_token||photo?.token;
    if(typeof token!=='string' || !token || tokens.has(token))continue;
    tokens.add(token);
    result.push({file_token:token,file_name:photo.name || '确认截图',target_written:true,
      preview_url:base(item)+'/screenshot/preview?'+new URLSearchParams({file_token:token})});
  }
  return result;
});
let controller:AbortController|undefined;
async function load(){controller?.abort();controller=new AbortController();const c=controller;loading.value=true;error.value='';try{const value=await requestJson('/api/change-confirmations',{signal:c.signal,cache:'no-store'});if(c.signal.aborted)return;items.value=value.items||[];warning.value=value.warning||'';canConfirm.value=value.can_confirm===true;for(const item of items.value)if((item.local_screenshot_items||[]).some((p:Dict)=>p.target_written&&p.local_image_id===pending.value[item.target_record_id]?.local_image_id))delete pending.value[item.target_record_id];if(props.focusId){const index=visible.value.findIndex(i=>i.target_record_id===props.focusId);if(index>=0)page.value=Math.floor(index/10)+1;}}catch(e){if(!c.signal.aborted)error.value=(e as Error).message;}finally{if(controller===c)loading.value=false;}}
async function stage(item:Dict,event:Event){const input=event.target as HTMLInputElement,file=input.files?.[0];input.value='';if(!file||busy.value)return;busy.value=true;error.value='';try{if(!file.type.startsWith('image/')||file.size>8*1024*1024)throw new Error('请选择8MB以内的图片。');pending.value[item.target_record_id]=await requestBinaryJson('/api/notice-attachments?'+new URLSearchParams({scope:noticeBuildings(item).includes(props.scope)?props.scope:noticeBuildings(item)[0]||props.scope,identity:'change:'+item.target_record_id,target_record_id:item.target_record_id,kind:'ali',file_name:file.name}),file,{headers:{'Content-Type':file.type}});}catch(e){error.value=(e as Error).message;}finally{busy.value=false;}}
async function upload(item:Dict){if(busy.value)return;const file=pending.value[item.target_record_id];if(!file?.upload_id)return;busy.value=true;error.value='';try{await requestJson(base(item)+'/screenshot',{method:'POST',body:JSON.stringify({upload_id:file.upload_id,local_image_id:file.local_image_id||''})});delete pending.value[item.target_record_id];emit('changed');await load();}catch(e){error.value=(e as Error).message+'；已保留原图片，请先刷新核验上传结果。';}finally{busy.value=false;}}
function confirm(item:Dict){dialog.warning({title:'确认此变更？',content:item.title,positiveText:'确认变更',negativeText:'返回核对',onPositiveClick:async()=>{busy.value=true;try{await requestJson(base(item)+'/confirm',{method:'POST',body:'{}'});emit('changed');await load();}catch(e){error.value=(e as Error).message;return false;}finally{busy.value=false;}}});}
function remove(item:Dict,photo:Dict){dialog.warning({title:'删除此确认截图？',positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{busy.value=true;try{await requestJson(base(item)+'/screenshot?'+new URLSearchParams({local_image_id:photo.local_image_id||'',file_token:photo.file_token||''}),{method:'DELETE'});emit('changed');await load();}catch(e){error.value=(e as Error).message;return false;}finally{busy.value=false;}}});}
function close(){if(busy.value)return;if(Object.keys(pending.value).length)dialog.warning({title:'截图尚未提交',content:'关闭后，本次选择不会上传到变更记录。',positiveText:'关闭',negativeText:'继续处理',onPositiveClick:()=>emit('close')});else emit('close');}
const stop=registerNavigationGuard((_target,proceed)=>{if(busy.value){message.warning('正在处理变更确认，请等待结果');return false;}if(Object.keys(pending.value).length){dialog.warning({title:'截图尚未提交',positiveText:'离开',negativeText:'继续处理',onPositiveClick:proceed});return false;}return true;});
function unload(event:BeforeUnloadEvent){if(busy.value||Object.keys(pending.value).length){event.preventDefault();event.returnValue='';}}
watch([filter,search],()=>page.value=1);onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});onBeforeUnmount(()=>{controller?.abort();stop();window.removeEventListener('beforeunload',unload);});
</script>
<style>
.change-inbox.n-card{width:min(980px,95vw);max-height:92dvh;overflow:auto}.change-controls,.change-tools,.change-proof header,.change-proof footer,.change-file,.change-photos{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.change-controls{margin-bottom:13px}.change-controls .n-input{flex:1;min-width:135px}.change-tools{justify-content:flex-end}.change-tools>span{margin-right:auto}.change-spread{display:grid;grid-template-columns:minmax(215px,1fr) minmax(0,1.65fr);gap:16px;align-items:start}
.change-queue{display:grid;gap:3px;max-height:54dvh;overflow:auto;border-radius:12px;padding:6px;background:var(--soft-field);box-shadow:var(--soft-inset)}
.change-queue button{display:grid;gap:3px;border:0;border-inline-start:3px solid transparent;background:transparent;color:var(--v2-ink);text-align:start;cursor:pointer;font:inherit;padding:8px}
.change-queue button[aria-pressed=true]{background:var(--soft-face);border-color:var(--v2-blue);box-shadow:var(--soft-key);border-radius:6px}.change-queue small,.change-queue time,.change-proof p,.change-proof header>span,.change-photos figcaption{font-size:12px;color:var(--v2-muted)}
.change-proof{padding:12px 15px;border-radius:14px;box-shadow:var(--soft-raised);overflow-wrap:anywhere}.change-proof header{justify-content:space-between}.change-proof h2{margin:9px 0;font-size:20px}.change-photos figure{position:relative;margin:4px 0}.change-photos figure>.n-button{position:absolute;top:2px;right:2px;background:var(--soft-face)}.change-proof footer{margin-top:10px}.change-file{position:relative;padding:6px 9px;border-radius:8px;background:var(--soft-field);color:var(--v2-blue)}.change-file:focus-within{outline:2px solid currentColor}.change-file input{position:absolute;inset:0;width:100%;opacity:0;cursor:pointer}
@media(max-width:650px){.change-spread{grid-template-columns:minmax(0,1fr)}.change-queue{max-height:18dvh}.change-controls .serenity-segment{flex-wrap:wrap}.change-proof{min-width:0}.change-inbox.n-card{width:96vw}}
</style>
