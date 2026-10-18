<template>
  <NModal show preset="card" title="照片与水印" class="v2-photo-editor" :mask-closable="false" :close-on-esc="!busy" @close="!busy&&emit('cancel')" @update:show="value=>{if(!value&&!busy)emit('cancel')}">
    <div class="v2-photo-editor-grid">
      <div class="v2-photo-editor-picture"><div class="v2-photo-canvas-wrap" :style="{'--photo-ratio':info.width/info.height}">
        <canvas ref="canvas" aria-label="照片水印预览" />
        <button v-if="form.enabled&&!form.existing&&bounds" type="button" class="v2-photo-stamp-handle" :class="{'is-dragging':dragging}" :style="handleStyle" aria-label="移动水印" title="移动水印" :disabled="busy" data-v2-navigation @pointerdown="startDrag" @pointermove="moveStamp" @pointerup="stopDrag" @pointercancel="stopDrag" @lostpointercapture="stopDrag" @keydown.left.prevent="nudge(-.02,0)" @keydown.right.prevent="nudge(.02,0)" @keydown.up.prevent="nudge(0,-.02)" @keydown.down.prevent="nudge(0,.02)"><Move :size="16" aria-hidden="true" /></button>
      </div></div>
      <div class="v2-photo-editor-fields">
        <div class="v2-photo-editor-toggle"><strong>添加水印</strong><NSwitch :value="form.enabled&&!form.existing" :disabled="busy||form.existing||info.can_watermark===false" aria-label="添加水印" @update:value="form.enabled=$event" /></div>
        <p v-if="info.can_watermark===false" class="v2-photo-editor-hint">照片过窄，当前保留原图。</p>
        <p v-if="info.preferences?.warning" class="v2-photo-editor-hint">{{info.preferences.warning}}</p>
        <NCheckbox v-model:checked="form.existing" :disabled="busy||info.existing_locked" @update:checked="value=>{if(value)form.enabled=false}">照片已有水印</NCheckbox>
        <p v-if="form.existing" class="v2-photo-editor-hint">已识别到水印，当前保留原图。</p>
        <p v-else-if="info.recognition==='unavailable'" class="v2-photo-editor-hint">请核对照片是否已有水印，再选择是否添加。</p>
        <fieldset :disabled="busy||!form.enabled||form.existing">
          <label role="group" aria-label="水印大小">水印大小 <output>{{Math.round(form.scale*100)}}%</output><NSlider v-model:value="percent" :min="18" :max="75" :step="1" :disabled="busy||!form.enabled||form.existing" /></label>
          <div class="v2-photo-position-field" role="group" aria-label="水印位置"><span>位置</span><NRadioGroup :value="corner" :disabled="fieldsLocked" aria-label="水印位置选择" size="small" @update:value="chooseCorner">
            <NRadioButton value="top-left" title="左上角"><ArrowUpLeft :size="17" aria-hidden="true" /><span class="v2-photo-position-label">左上角</span></NRadioButton>
            <NRadioButton value="top-right" title="右上角"><ArrowUpRight :size="17" aria-hidden="true" /><span class="v2-photo-position-label">右上角</span></NRadioButton>
            <NRadioButton value="bottom-left" title="左下角"><ArrowDownLeft :size="17" aria-hidden="true" /><span class="v2-photo-position-label">左下角</span></NRadioButton>
            <NRadioButton value="bottom-right" title="右下角"><ArrowDownRight :size="17" aria-hidden="true" /><span class="v2-photo-position-label">右下角</span></NRadioButton>
          </NRadioGroup></div>
          <div class="v2-photo-logo-field" role="group" aria-label="水印Logo">
            <span>Logo</span>
            <NRadioGroup :value="logoMode" :disabled="fieldsLocked||logoBusy" size="small" aria-label="水印Logo选择" @update:value="chooseLogo">
              <NRadioButton value="none">不添加</NRadioButton><NRadioButton value="vnet">VNET</NRadioButton><NRadioButton value="custom">自定义</NRadioButton>
            </NRadioGroup>
            <div v-if="logoMode==='custom'" class="v2-photo-logo-upload"><NButton size="small" secondary :disabled="fieldsLocked||logoBusy" :loading="logoBusy" @click="logoPicker?.click()"><ImagePlus :size="15" />{{customLogo?'更换Logo':'上传Logo'}}</NButton><img v-if="customLogo" :src="customLogo" alt="自定义Logo" /></div>
            <input ref="logoPicker" class="v2-photo-logo-input" type="file" accept="image/png,image/jpeg,image/webp" aria-label="上传自定义Logo" :disabled="fieldsLocked||logoBusy" @change="pickLogo" />
          </div>
          <label>标题<NInput v-model:value="form.title" :maxlength="100" :disabled="fieldsLocked" :input-props="{'aria-label':'水印标题'}" /></label>
          <label>施工区域<NInput v-model:value="form.area" :maxlength="80" :disabled="fieldsLocked" :input-props="{'aria-label':'施工区域'}" /></label>
          <label class="v2-photo-editor-time">拍照时间<NDatePicker v-model:formatted-value="form.time" type="datetime" value-format="yyyy-MM-dd HH:mm" format="yyyy-MM-dd HH:mm" clearable :disabled="fieldsLocked" aria-label="拍照时间" /></label>
          <small class="v2-photo-editor-hint" :class="{'is-edited':timeModified}">{{timeHint}}</small>
          <label>天气<NInput v-model:value="form.weather" :maxlength="100" :disabled="fieldsLocked" placeholder="留空则不显示" :input-props="{'aria-label':'水印天气'}" /></label>
          <label>地点<NInput v-model:value="form.location" :maxlength="200" :disabled="fieldsLocked" placeholder="留空则不显示" :input-props="{'aria-label':'水印地点'}" /></label>
        </fieldset>
      </div>
    </div>
    <p v-if="issue||error" class="v2-photo-editor-error" role="alert">{{issue||error}}</p>
    <template #footer><div class="v2-photo-editor-footer"><NCheckbox v-model:checked="rememberStyle" :disabled="busy||fieldsLocked">记住样式</NCheckbox><NButton :disabled="busy" @click="emit('cancel')">取消</NButton><NButton type="primary" :loading="busy" @click="save">{{form.enabled&&!form.existing?'确认水印并上传':'保留原图并上传'}}</NButton></div></template>
  </NModal>
</template>
<script setup lang="ts">
import {ref,computed,onMounted,watch,onBeforeUnmount} from 'vue';
import {NModal,NInput,NDatePicker,NSlider,NSwitch,NCheckbox,NButton,NRadioGroup,NRadioButton} from 'naive-ui';
import {ImagePlus,Move,ArrowUpLeft,ArrowUpRight,ArrowDownLeft,ArrowDownRight} from 'lucide-vue-next';
import type {Dict} from '../client';
const props=defineProps<{info:Dict;busy:boolean;error:string}>();
const emit=defineEmits<{save:[options:Dict,remember:boolean];cancel:[]}>();
const style=props.info.preferences?.style||{},initial=props.info.options;
const form=ref<Dict>({...initial,enabled:initial.enabled&&props.info.can_watermark!==false,scale:style.scale??initial.scale,logo:style.logo??initial.logo??'',position:{...(style.position||initial.position||{x:0,y:1})}}),issue=ref(''),canvas=ref<HTMLCanvasElement>();
const logoMode=ref(form.value.logo==='vnet'?'vnet':form.value.logo?'custom':'none'),customLogo=ref(form.value.logo&&form.value.logo!=='vnet'?form.value.logo:''),logoBusy=ref(false),logoPicker=ref<HTMLInputElement>(),rememberStyle=ref(true);
const bounds=ref<{left:number;top:number;width:number;height:number;travelX:number;travelY:number}>(),dragging=ref(false);
const originalTime=props.info.original_time??initial.time??'';
const timeModified=computed(()=>Boolean(form.value.time&&form.value.time!==originalTime));
const timeHint=computed(()=>timeModified.value?(originalTime?'手动修改 · 原始 '+originalTime+'（'+props.info.time_source+'）':'手动填写 · 照片未提供可核验的拍摄时间'):props.info.time_source);
const corners:Record<string,{x:number;y:number}>={'top-left':{x:0,y:0},'top-right':{x:1,y:0},'bottom-left':{x:0,y:1},'bottom-right':{x:1,y:1}};
const corner=computed(()=>Object.keys(corners).find(key=>corners[key].x===form.value.position.x&&corners[key].y===form.value.position.y)||'');
const handleStyle=computed(()=>{const b=bounds.value;return b?{left:b.left/props.info.width*100+'%',top:b.top/props.info.height*100+'%',width:b.width/props.info.width*100+'%',height:b.height/props.info.height*100+'%'}:{};});
const percent=computed({get:()=>Math.round(form.value.scale*100),set:(value:number)=>{form.value.scale=value/100;}});
const fieldsLocked=computed(()=>props.busy||!form.value.enabled||form.value.existing);
let picture:HTMLImageElement|undefined,logoPicture:HTMLCanvasElement|undefined,disposed=false;
let drag:{id:number;x:number;y:number;position:{x:number;y:number};ratioX:number;ratioY:number}|undefined;
const clamp=(value:number)=>Math.round(Math.max(0,Math.min(1,value))*10000)/10000;
function chooseCorner(value:string){if(corners[value])form.value.position={...corners[value]};}
function nudge(x:number,y:number){if(!fieldsLocked.value)form.value.position={x:clamp(form.value.position.x+x),y:clamp(form.value.position.y+y)};}
function startDrag(event:PointerEvent){
  const b=bounds.value,rect=canvas.value?.getBoundingClientRect();if(fieldsLocked.value||!b||!rect)return;
  event.preventDefault();drag={id:event.pointerId,x:event.clientX,y:event.clientY,position:{...form.value.position},ratioX:b.travelX?props.info.width/rect.width/b.travelX:0,ratioY:b.travelY?props.info.height/rect.height/b.travelY:0};dragging.value=true;(event.currentTarget as HTMLElement).setPointerCapture(event.pointerId);
}
function moveStamp(event:PointerEvent){if(!drag||event.pointerId!==drag.id)return;form.value.position={x:clamp(drag.position.x+(event.clientX-drag.x)*drag.ratioX),y:clamp(drag.position.y+(event.clientY-drag.y)*drag.ratioY)};}
function stopDrag(){drag=undefined;dragging.value=false;}
function loadImage(source:string):Promise<HTMLImageElement>{return new Promise((resolve,reject)=>{const image=new Image();image.onload=()=>resolve(image);image.onerror=()=>reject(new Error('Logo图片无法读取，请重新选择。'));image.src=source;});}
function trimLogo(image:HTMLImageElement):HTMLCanvasElement{
  const target=document.createElement('canvas');target.width=image.naturalWidth;target.height=image.naturalHeight;
  const ctx=target.getContext('2d');if(!ctx)throw new Error('当前浏览器不支持图片编辑。');ctx.drawImage(image,0,0);
  const data=ctx.getImageData(0,0,target.width,target.height).data;let left=target.width,top=target.height,right=-1,bottom=-1;
  for(let y=0;y<target.height;y++)for(let x=0;x<target.width;x++)if(data[(y*target.width+x)*4+3]){left=Math.min(left,x);right=Math.max(right,x);top=Math.min(top,y);bottom=Math.max(bottom,y);}
  if(right<left)throw new Error('Logo图片为空，请重新选择。');
  const cropped=document.createElement('canvas');cropped.width=right-left+1;cropped.height=bottom-top+1;cropped.getContext('2d')!.drawImage(target,left,top,cropped.width,cropped.height,0,0,cropped.width,cropped.height);return cropped;
}
async function chooseLogo(value:string){
  logoMode.value=value;form.value.logo=value==='vnet'?'vnet':value==='custom'?customLogo.value:'';logoPicture=undefined;issue.value='';draw();
  if(!form.value.logo)return;logoBusy.value=true;
  try{const image=await loadImage(form.value.logo==='vnet'?'/assets/vnet-logo.png':form.value.logo);if(!disposed){logoPicture=trimLogo(image);draw();}}catch(e){if(!disposed)issue.value=(e as Error).message;}finally{logoBusy.value=false;}
}
async function pickLogo(event:Event){
  const input=event.target as HTMLInputElement,file=input.files?.[0];input.value='';if(!file)return;
  issue.value='';logoBusy.value=true;let url='';
  try{
    if(!/\.(png|jpe?g|webp)$/i.test(file.name)||!file.size||file.size>2*1024*1024)throw new Error('请选择2MB以内的PNG、JPG或WebP格式Logo。');
    url=URL.createObjectURL(file);const image=await loadImage(url);
    if(image.naturalWidth*image.naturalHeight>4_000_000)throw new Error('Logo像素过大，请选择较小图片。');
    const cropped=trimLogo(image),small=document.createElement('canvas');let selected='';
    for(const maxWidth of [256,192,128,96,64]){
      const scale=Math.min(1,maxWidth/cropped.width,96/cropped.height);small.width=Math.max(1,Math.round(cropped.width*scale));small.height=Math.max(1,Math.round(cropped.height*scale));small.getContext('2d')!.drawImage(cropped,0,0,small.width,small.height);
      for(const quality of [.8,.6,.4]){const value=small.toDataURL('image/webp',quality);if(value.split(',')[1].length<=2048){selected=value;break;}}
      if(selected)break;
    }
    if(!selected)throw new Error('Logo细节过多，请选择更简洁的图片。');
    const normalized=await loadImage(selected);if(disposed)return;customLogo.value=selected;form.value.logo=selected;logoPicture=trimLogo(normalized);draw();
  }catch(e){if(!disposed)issue.value=(e as Error).message;}finally{if(url)URL.revokeObjectURL(url);logoBusy.value=false;}
}
function draw(){
  const target=canvas.value;if(!target||!picture)return;
  const w=props.info.width,h=props.info.height;target.width=w;target.height=h;
  const context=target.getContext('2d');if(!context)return;const ctx:CanvasRenderingContext2D=context;ctx.drawImage(picture,0,0,w,h);
  const value=form.value;if(!value.enabled||value.existing){bounds.value=undefined;return;}
  const label=timeModified.value?(originalTime?'拍照时间（手动修改）':'拍照时间（手动填写）'):'拍照时间';
  const lines=['施工区域：'+value.area,label+'：'+(value.time||'请选择拍照时间')];
  if(value.weather)lines.push('天　　气：'+value.weather);if(value.location)lines.push('地　　点：'+value.location);
  const margin=Math.max(8,Math.round(Math.min(w,h)*.02));
  const width=Math.min(w-margin*2,w*value.scale,(h-margin*2)*18/(1.95+1.5*lines.length+.35));
  const size=Math.max(1,Math.round(width/18)),line=Math.round(size*1.5),header=Math.round(size*1.95);
  const height=header+line*lines.length+Math.round(size*.35),travelX=Math.max(0,w-margin*2-width),travelY=Math.max(0,h-margin*2-height),left=margin+Math.round(travelX*value.position.x),top=margin+Math.round(travelY*value.position.y),radius=Math.max(5,Math.round(size*.45));
  bounds.value={left,top,width,height,travelX,travelY};
  ctx.fillStyle='rgba(255,255,255,.733)';ctx.beginPath();ctx.roundRect(left,top,width,height,radius);ctx.fill();
  ctx.fillStyle='rgba(52,120,224,.922)';ctx.beginPath();ctx.roundRect(left,top,width,header,radius);ctx.fill();ctx.fillRect(left,top+header/2,width,header/2);
  let titleX=left+size*1.7;
  if(logoPicture&&value.logo){
    const boxHeight=Math.max(4,Math.round(header*.72)),boxWidth=Math.max(4,Math.min(Math.round(boxHeight*logoPicture.width/logoPicture.height),Math.round(width*.22)));
    const boxLeft=left+Math.round(size*.4),boxTop=top+Math.floor((header-boxHeight)/2),padding=Math.max(1,Math.round(size*.1));
    ctx.fillStyle='white';ctx.beginPath();ctx.roundRect(boxLeft,boxTop,boxWidth,boxHeight,Math.max(2,Math.round(size*.15)));ctx.fill();
    const scale=Math.min(1,(boxWidth-padding*2)/logoPicture.width,(boxHeight-padding*2)/logoPicture.height),lw=Math.max(1,Math.round(logoPicture.width*scale)),lh=Math.max(1,Math.round(logoPicture.height*scale));
    ctx.drawImage(logoPicture,boxLeft+Math.floor((boxWidth-lw)/2),boxTop+Math.floor((boxHeight-lh)/2),lw,lh);titleX=boxLeft+boxWidth+size*.4;
  }else{ctx.fillStyle='#ffc63b';ctx.beginPath();ctx.ellipse(left+size*.725,top+header/2,size*.225,size*.2,0,0,Math.PI*2);ctx.fill();}
  function text(content:string,x:number,y:number,available:number,bold=false,color='#0c1116'){
    let font=size;ctx.font=`${bold?'bold ':''}${font}px "Microsoft YaHei",sans-serif`;
    while(font>8&&ctx.measureText(content).width>available){font--;ctx.font=`${bold?'bold ':''}${font}px "Microsoft YaHei",sans-serif`;}
    ctx.fillStyle=color;ctx.textBaseline='middle';ctx.fillText(content,x,y,available);
  }
  text(value.title,titleX,top+header/2,left+width-titleX-size*.3,true,'white');
  lines.forEach((value,index)=>text(value,left+size*.5,top+header+line*(index+.5),width-size));
}
function save(){issue.value='';if(form.value.enabled&&!form.value.existing){if(logoBusy.value){issue.value='Logo正在处理，请稍候。';return;}if(logoMode.value!=='none'&&!logoPicture){issue.value='请选择可读取的Logo图片。';return;}if(!form.value.title.trim()||!form.value.area.trim()){issue.value='请填写标题和施工区域。';return;}if(!form.value.time){issue.value='请选择照片的拍照时间。';return;}}emit('save',{...form.value,time:form.value.time||''},rememberStyle.value);}
onMounted(()=>{picture=new Image();picture.onload=draw;picture.src=props.info.image;if(form.value.logo)void chooseLogo(logoMode.value);});
watch(form,()=>{issue.value='';draw();},{deep:true});
onBeforeUnmount(()=>{disposed=true;});
</script>
<style>
.v2-photo-editor.n-card{width:min(1080px,calc(100vw - 24px));max-height:94dvh;display:flex}.v2-photo-editor .n-card-content{overflow:auto;min-height:0}.v2-photo-editor-grid{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(260px,1fr);gap:20px;align-items:start}.v2-photo-editor-picture{position:sticky;top:0;min-width:0;background:#f0f2f5;border-radius:6px;overflow:hidden;display:flex;justify-content:center}.v2-photo-editor canvas{display:block;width:100%;height:100%}.v2-photo-editor-fields{display:flex;flex-direction:column;gap:10px;min-width:0}.v2-photo-editor-toggle{display:flex;align-items:center;justify-content:space-between}.v2-photo-editor fieldset{border:0;padding:0;margin:0;display:flex;flex-direction:column;gap:10px;min-width:0}.v2-photo-editor fieldset>label{display:flex;flex-direction:column;gap:5px;font-size:13px}.v2-photo-editor output{float:right}.v2-photo-editor-hint{font-size:12px;line-height:1.5;color:var(--v2-muted,#637184);margin:0}.v2-photo-editor-error{color:#b42318;font-size:13px}.v2-photo-editor-footer{display:flex;justify-content:flex-end;gap:10px}.v2-photo-editor-footer>.n-checkbox{margin-right:auto;flex-shrink:0;align-self:center}.v2-photo-editor-footer .n-button{min-height:40px}.v2-photo-editor .n-date-picker{width:100%}
.v2-photo-canvas-wrap{position:relative;min-width:0;width:min(100%,calc(65dvh * var(--photo-ratio)));aspect-ratio:var(--photo-ratio)}.v2-photo-stamp-handle{position:absolute;box-sizing:border-box;min-width:44px;min-height:44px;border:1px dashed transparent;background:transparent;color:white;cursor:grab;touch-action:none;user-select:none;padding:0;display:flex;align-items:flex-start;justify-content:flex-end;transition:border-color .15s}.v2-photo-stamp-handle svg{padding:2px;background:#1d5da8;border-radius:3px;opacity:0}.v2-photo-stamp-handle:hover,.v2-photo-stamp-handle:focus-visible,.v2-photo-stamp-handle.is-dragging{border-color:#3b82f6;outline:0}.v2-photo-stamp-handle:hover svg,.v2-photo-stamp-handle:focus-visible svg,.v2-photo-stamp-handle.is-dragging svg{opacity:1}.v2-photo-stamp-handle.is-dragging{cursor:grabbing}.v2-photo-position-field{display:flex;flex-direction:column;gap:6px;font-size:13px}.v2-photo-position-field .n-radio-group{display:flex;align-self:flex-start}.v2-photo-position-field .n-radio__label{display:flex;align-items:center}.v2-photo-position-label{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}.v2-photo-editor-hint.is-edited{color:#9a5b11}@media(pointer:coarse){.v2-photo-stamp-handle svg{opacity:.85}}
.v2-photo-logo-field{display:flex;flex-direction:column;gap:6px;font-size:13px}.v2-photo-logo-upload{display:flex;align-items:center;gap:8px}.v2-photo-logo-upload img{max-width:120px;height:32px;object-fit:contain;background:white;border:1px solid var(--v2-border,#dce2ea);border-radius:4px}.v2-photo-logo-input{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}
.v2-photo-logo-field .n-radio-group{display:flex;align-self:flex-start}
@media(max-width:700px){.v2-photo-editor-grid{grid-template-columns:minmax(0,1fr);gap:12px}.v2-photo-editor-picture{position:relative}.v2-photo-canvas-wrap{width:min(100%,calc(38dvh * var(--photo-ratio)))}.v2-photo-editor-footer .n-button,.v2-photo-logo-upload .n-button{min-height:44px}.v2-photo-editor .n-card-header,.v2-photo-editor .n-card-content,.v2-photo-editor .n-card__footer{padding:12px}.v2-photo-editor-fields fieldset{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.v2-photo-editor-fields fieldset>label:first-child,.v2-photo-logo-field,.v2-photo-position-field,.v2-photo-editor-time,.v2-photo-editor-fields fieldset>small,.v2-photo-editor-fields fieldset>label:last-child{grid-column:1/-1}.v2-photo-logo-field .n-radio-group,.v2-photo-position-field .n-radio-group{--n-height:44px!important}}
</style>
