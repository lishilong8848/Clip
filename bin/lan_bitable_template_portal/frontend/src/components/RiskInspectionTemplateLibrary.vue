<template>
  <section class="risk-library" :aria-label="baseMode?'基础排查模板库':'历史排查模板库'">
    <NAlert v-if="error&&!confirmOpen" type="error" :bordered="false" role="alert">{{error}}</NAlert>
    <NAlert v-if="message" type="info" :bordered="false">{{message}}</NAlert>
    <div v-if="baseMode" class="history-toolbar history-import-toolbar"><NButton :disabled="busy||loading" @click="openImport('file')"><FileSpreadsheet :size="16" />导入表格</NButton><NButton :disabled="busy||loading" @click="openImport('bitable')"><Table2 :size="16" />导入多维表</NButton></div>
    <template v-if="!selected">
      <form class="history-toolbar" @submit.prevent="page=1;load()"><NInput v-model:value="search" clearable placeholder="搜索模板名称" :input-props="{'aria-label':'搜索模板名称'}" /><NButton attr-type="submit" :loading="loading"><Search :size="16" />查询</NButton><NButton v-if="baseMode" type="primary" :disabled="busy||loading" @click="create"><Plus :size="16" />新建基础模板</NButton></form>
      <NSpin :show="loading"><div class="history-list"><article v-for="row in rows" :key="row.id"><div><strong>{{row.name}}</strong><NTag size="small" :type="row.enabled?'success':'default'">{{row.enabled?'启用':'停用'}}</NTag><span>V{{row.revision}}</span></div><p>{{row.item_count}} 项排查 · {{row.field_count}} 个字段 · {{row.scopes.map(scopeLabel).join('、')}}</p><small>{{row.creator}} · {{date(row.updated)}}{{row.legacy?' · 原预填待核对':''}}</small><footer><NButton v-if="!selecting" :disabled="loading" @click="open(row.id)"><Pencil :size="16" />查看 / 编辑</NButton><NButton type="primary" :disabled="!row.enabled||loading" @click="reuse(row.id)"><CopyPlus :size="16" />{{selecting?'选用并填写':'复用发布'}}</NButton></footer></article></div><NEmpty v-if="!loading&&!rows.length" :description="baseMode?'暂无可用基础模板，请先创建或启用模板':'暂无可用历史模板；排查成功发布后会自动保存'" /></NSpin>
      <NPagination v-if="total>20" :page="page" :page-count="Math.ceil(total/20)" :page-slot="5" @update:page="p=>{page=p;load()}" />
    </template>
    <template v-else>
      <header class="history-toolbar"><NButton :disabled="busy" @click="back"><ArrowLeft :size="16" />模板列表</NButton><span>{{selected.version?'V'+selected.revision:'新建模板'}} · {{selected.creator}} · {{date(selected.created)}}</span><NButton v-if="selected.version" type="primary" :disabled="busy||dirty||!selected.enabled" @click="reuse(selected.id)"><CopyPlus :size="16" />{{selecting?'选用并填写':'复用发布'}}</NButton></header>
      <NAlert v-if="selected.legacy" type="warning" :bordered="false">此历史任务未保存发布时的原始预填内容，目前只恢复字段与事项标题，请核对并补充预填内容。</NAlert>
      <NAlert v-for="(warning,index) in selected.warnings||[]" :key="index" type="warning" :bordered="false">{{warning}}</NAlert>
      <p v-if="selected.source" class="history-import-source">导入来源：{{sourceLabel(selected.source)}}</p>
      <NAlert v-if="selected.sync_status?.error" type="warning" :bordered="false">模板已保存在灯塔，飞书备份尚未完成：{{selected.sync_status.error}} <NButton text :disabled="busy" @click="retry">重试备份</NButton></NAlert>
      <RiskInspectionFieldPicker v-if="baseMode" :workspace-v2="workspaceV2" :library="fieldLibrary" :fields="form.fields" :loading="libraryLoading" :disabled="busy" :can-edit="canEditLibrary" :error="libraryError" @select="selectField" @remove="removeField" @required="(field,required)=>{if(!busy)field.required=required}" @add="editLibraryField()" @edit="editLibraryField" @delete="deleteLibraryField" @reload="loadFieldLibrary">
        <template #settings><div class="history-settings"><label><span>模板名称</span><NInput v-model:value="form.name" :maxlength="120" :disabled="busy" :input-props="{'aria-label':'基础模板名称'}" placeholder="输入基础模板名称" /></label><NSwitch v-model:value="form.enabled" :disabled="busy" aria-label="启用基础模板"><template #checked>启用</template><template #unchecked>停用</template></NSwitch></div></template>
        <template #prefill><details class="history-base-prefill"><summary>排查事项预填 · {{form.items.length}} 项</summary><div class="history-toolbar"><NButton size="small" :disabled="busy||form.items.length>=100" @click="form.items.push({title:'',values:{}})"><Plus :size="16" />添加事项</NButton></div><RiskInspectionSheet :fields="form.fields" v-model:items="form.items" history :disabled="busy" /></details><NAlert v-if="valueIssues.length" type="warning" :bordered="false">{{valueIssues.slice(0,3).join('；')}}</NAlert></template>
        <template #footer><span>{{!selected.version?'尚未生成模板':dirty?'有未保存修改':'模板已保存'}}</span><NButton type="primary" :disabled="saveDisabled" @click="confirmOpen=true">{{selected.version?'保存新版本':'生成基础模板'}}</NButton></template>
      </RiskInspectionFieldPicker>
      <template v-else>
      <div class="history-settings"><label><span>模板名称</span><NInput v-model:value="form.name" :maxlength="120" :disabled="busy" :input-props="{'aria-label':'历史模板名称'}" /></label><NSwitch v-model:value="form.enabled" :disabled="busy"><template #checked>启用</template><template #unchecked>停用</template></NSwitch></div>
      <details class="history-schema"><summary>字段设置 · {{inputFields.length}} 个填写字段</summary><RiskInspectionSheet :fields="form.fields" schema can-edit :disabled="busy" @edit="editField" @remove-field="removeField" @required-change="(field,required)=>{if(!busy)field.required=required}" /><div class="history-schema-actions"><NButton :disabled="busy||form.fields.length>=80" @click="addField"><Plus :size="16" />添加字段</NButton></div></details>
      <div class="history-toolbar"><strong>排查事项</strong><NButton :disabled="busy||form.items.length>=100" @click="form.items.push({title:'',values:{}})"><Plus :size="16" />添加事项</NButton></div>
      <RiskInspectionSheet :fields="form.fields" v-model:items="form.items" history :disabled="busy" />
      <NAlert v-if="valueIssues.length" type="warning" :bordered="false">{{valueIssues.slice(0,3).join('；')}}</NAlert>
      <footer class="history-footer"><span>{{!selected.version?'新建模板，尚未保存':dirty?'有未保存修改':selected.cloud?.synced?'已保存并备份至飞书':'已保存，等待飞书备份'}} · 日期、人员、现场附件及审核结果不复用</span><NButton type="primary" :disabled="busy||!dirty||!form.name.trim()||!baseMode&&!form.items.length||valueIssues.length>0||form.items.some((i:Dict)=>!i.title.trim())" @click="confirmOpen=true">{{selected.version?'保存新版本':'保存基础模板'}}</NButton></footer>
      </template>
    </template>
    <NModal :show="!!importKind" preset="card" :title="importKind==='file'?'导入表格':'导入多维表'" class="risk-history-import-modal" :mask-closable="false" @update:show="show=>{if(!show)closeImport()}">
      <NAlert v-if="importError" type="error" :bordered="false" role="alert">{{importError}}</NAlert>
      <div class="history-import-form">
        <label v-if="importKind==='file'" class="history-import-file"><FileSpreadsheet :size="18" /><span>{{importFile?.name||'选择表格文件'}}</span><input type="file" accept=".xlsx,.xlsm,.csv" :disabled="importBusy" aria-label="选择导入表格" @change="pickImportFile" /></label>
        <label v-else><span>多维表链接</span><NInput v-model:value="importOptions.url" :disabled="importBusy" placeholder="https://..." :input-props="{'aria-label':'多维表链接'}" @update:value="changeImportSource" /></label>
        <label v-if="sheetOptions.length"><span>工作表</span><NSelect v-model:value="importOptions.sheet" :options="sheetOptions" :disabled="importBusy" aria-label="导入工作表" @update:value="changeImportSheet" /></label>
        <label v-if="titleOptions.length"><span>事项名称列</span><NSelect v-model:value="importOptions.title_field" :options="titleOptions" :disabled="importBusy" aria-label="导入事项名称列" @update:value="importStale=true" /></label>
        <NButton :loading="importBusy" :disabled="importBusy||(importKind==='file'?!importFile:!importOptions.url.trim())" @click="previewImport"><Search :size="16" />{{importPreview?'重新预览':'预览导入'}}</NButton>
      </div>
      <section v-if="importPreview" class="history-import-preview" aria-label="导入预览">
        <NAlert v-if="importPreview.needs_selection||importStale" type="info" :bordered="false">请确认工作表和事项名称列后重新预览。</NAlert>
        <NAlert v-for="(warning,index) in importPreview.warnings||[]" :key="index" type="warning" :bordered="false">{{warning}}</NAlert>
        <div class="history-toolbar"><strong>{{importPreview.name||'待确认导入'}}</strong><span>{{importPreview.items?.length||0}} 项 · {{importPreview.fields?.filter((field:Dict)=>riskFieldRole(field)==='input'&&!field.is_primary).length||0}} 个填写字段</span></div>
        <RiskInspectionSheet v-if="importPreview.items?.length" :fields="importPreview.fields||[]" :items="importPreview.items" disabled />
      </section>
      <template #footer><div class="history-toolbar"><NButton @click="closeImport">取消</NButton><NButton type="primary" :disabled="importBusy||!importPreview||!!importPreview.needs_selection||importStale" @click="useImport">确认带入新草稿</NButton></div></template>
    </NModal>
    <NModal :show="confirmOpen" preset="dialog" :title="baseMode?'保存基础模板':'保存历史模板'" positive-text="确认保存" negative-text="取消" :loading="busy" :mask-closable="false" :closable="!busy" :close-on-esc="!busy" :negative-button-props="{disabled:busy}" @update:show="v=>{if(!busy)confirmOpen=v}" @positive-click="save"><NAlert v-if="error" type="error" :bordered="false" role="alert">{{error}}</NAlert><p>仅保存当前模板，不修改其它模板或已经发布的排查。停用后不能再次选用此模板。</p></NModal>
    <NModal :show="!!discard" preset="dialog" :title="discard?.title||'确认放弃修改'" :content="discard?.message" type="warning" :positive-text="discard?.positive||'放弃修改'" negative-text="继续编辑" :mask-closable="false" @positive-click="discardChanges" @negative-click="discard=null" @update:show="show=>{if(!show)discard=null}" />
    <NModal :show="!!fieldDraft" preset="card" :title="fieldTarget==='library'?'字段库字段':'历史模板字段'" class="risk-history-field-modal" :class="{'risk-v2-field-modal':workspaceV2}" :style="workspaceV2?fieldViewport:undefined" :mask-closable="false" :closable="!busy" :close-on-esc="!busy" @update:show="show=>{if(!show)closeField()}">
      <NAlert v-if="fieldError" type="error" :bordered="false">{{fieldError}}</NAlert>
      <div v-if="fieldDraft" class="history-field-form"><label><span>字段名称</span><NInput v-model:value="fieldDraft.field_name" :maxlength="80" :disabled="busy" :input-props="{'aria-label':fieldTarget==='library'?'字段库名称':'历史字段名称'}" /></label><label><span>字段类型</span><NSelect v-model:value="fieldDraft.type" :to="workspaceV2?'body':undefined" :virtual-scroll="!workspaceV2" :menu-props="workspaceV2?{class:'risk-v2-field-menu'}:undefined" :disabled="busy||fieldDraft.is_primary||fieldDraft.role!=='input'" :options="typeOptions" :aria-label="fieldTarget==='library'?'字段库类型':'历史字段类型'" /></label><label><span>填写要求</span><div v-if="fieldDraft.role==='input'" class="history-required"><NSwitch v-model:value="fieldDraft.required" :disabled="busy" :aria-label="fieldTarget==='library'?'字段库是否必填':'历史字段是否必填'" /><span>{{fieldDraft.required?'必填':'不必填'}}</span></div><small v-else>由系统自动填写，无需设置</small></label><label v-if="[3,4].includes(fieldDraft.type)" class="wide"><span>选项，每行一个</span><NInput v-model:value="fieldDraft.optionsText" :disabled="busy" type="textarea" :autosize="{minRows:3,maxRows:6}" /></label></div>
      <template #footer><div class="history-toolbar"><NButton :disabled="busy" @click="closeField">取消</NButton><NButton type="primary" :loading="busy" :disabled="busy" @click="applyField">{{fieldTarget==='library'?'保存字段':'应用到当前草稿'}}</NButton></div></template>
    </NModal>
  </section>
</template>
<script setup lang="ts">
import {computed,onBeforeUnmount,onMounted,reactive,ref,toRaw,watch} from 'vue';
import {NAlert,NButton,NEmpty,NInput,NModal,NPagination,NSelect,NSpin,NSwitch,NTag} from 'naive-ui';
import {ArrowLeft,CopyPlus,FileSpreadsheet,Pencil,Plus,Search,Table2} from 'lucide-vue-next';
import {requestJson,type Dict} from '../api/client';
import RiskInspectionSheet from './RiskInspectionSheet.vue';
import RiskInspectionFieldPicker from './RiskInspectionFieldPicker.vue';
import {prefillIssues,riskFieldRole,riskSystemField,riskTypeOptions} from './riskInspectionUi';
const emit=defineEmits<{(e:'reuse',template:Dict):void;(e:'dirty',value:boolean):void}>();
const props=withDefaults(defineProps<{kind?:'base'|'history';selecting?:boolean;canEditLibrary?:boolean;workspaceV2?:boolean}>(),{kind:'history',selecting:false,canEditLibrary:false,workspaceV2:false});
const baseMode=computed(()=>props.kind==='base');
const base='/api/risk-inspections/'+(props.kind==='base'?'base-templates':'templates'),rows=ref<Dict[]>([]),selected=ref<Dict|null>(null),search=ref(''),page=ref(1),total=ref(0),loading=ref(false),busy=ref(false),error=ref(''),message=ref(''),confirmOpen=ref(false);
const form=reactive<Dict>({name:'',enabled:true,fields:[],items:[]});
const inputFields=computed(()=>form.fields.filter((field:Dict)=>riskFieldRole(field)==='input'&&!field.is_primary));
const importKind=ref<'file'|'bitable'|''>(''),importFile=ref<File|null>(null),importPreview=ref<Dict|null>(null),importBusy=ref(false),importError=ref(''),importStale=ref(false);
const importOptions=reactive({url:'',sheet:null as string|null,title_field:null as string|null});
function importChoices(choices:any[]=[]){return choices.map(choice=>typeof choice==='string'?{value:choice,label:choice}:{value:String(choice.value??choice.key??choice.field_id??choice.name??choice.field_name),label:String(choice.label??choice.field_name??choice.name??choice.value??choice.key??choice.field_id)});}
const sheetOptions=computed(()=>importChoices(importPreview.value?.sheets));
const titleOptions=computed(()=>importChoices(importPreview.value?.title_fields));
const original=ref('');const dirty=computed(()=>!!selected.value&&JSON.stringify(form)!==original.value);
const reusableFields=computed(()=>form.fields.filter((f:Dict)=>![5,17].includes(f.type)));
const valueIssues=computed(()=>prefillIssues(reusableFields.value,form.items));
const saveDisabled=computed(()=>busy.value||!dirty.value||!form.name.trim()||!form.fields.length||form.fields.filter((f:Dict)=>f.is_primary).length!==1||form.fields.some((f:Dict)=>f.unsupported)||!baseMode.value&&!form.items.length||valueIssues.value.length>0||form.items.some((i:Dict)=>!i.title.trim()));
const typeOptions=riskTypeOptions,fieldDraft=ref<Dict|null>(null),fieldError=ref(''),fieldOriginal=ref('');
const fieldViewport=ref<Record<string,string>>({});
// Keyboard/zoom can shrink only the visual viewport, leaving vh and the modal wrapper unchanged.
watch(()=>props.workspaceV2&&!!fieldDraft.value,(open,_,cleanup)=>{
 if(!open){fieldViewport.value={};return;}
 const viewport=window.visualViewport;let frame=0;
 const update=()=>{frame=0;fieldViewport.value={'--risk-field-height':(viewport?.height??window.innerHeight)+'px','--risk-field-width':(viewport?.width??window.innerWidth)+'px','--risk-field-top':(viewport?.offsetTop??0)+'px','--risk-field-left':(viewport?.offsetLeft??0)+'px'};};
 const schedule=()=>{if(!frame)frame=requestAnimationFrame(update);};
 update();viewport?.addEventListener('resize',schedule);viewport?.addEventListener('scroll',schedule);window.addEventListener('resize',schedule);
 cleanup(()=>{cancelAnimationFrame(frame);viewport?.removeEventListener('resize',schedule);viewport?.removeEventListener('scroll',schedule);window.removeEventListener('resize',schedule);});
},{flush:'post'});
const fieldLibrary=ref<Dict|null>(null),libraryLoading=ref(false),libraryError=ref(''),fieldTarget=ref<'library'|'template'>('template');
const fieldDirty=computed(()=>!!fieldDraft.value&&JSON.stringify(fieldDraft.value)!==fieldOriginal.value);
let abort:AbortController|undefined,libraryAbort:AbortController|undefined,importAbort:AbortController|undefined,disposed=false;
function scopeLabel(s:string){return s==='110'?'110站':s+'楼';}
function date(t:number){return new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'short'}).format(new Date(t*1000));}
const discard=ref<{message:string;proceed:()=>void;title?:string;positive?:string}|null>(null);
function askDiscard(message:string,proceed:()=>void,title?:string,positive?:string){if(!discard.value)discard.value={message,proceed,title,positive};}
function discardChanges(){const next=discard.value?.proceed;discard.value=null;next?.();return false;}
function leaveAllowed(proceed?:()=>void){if(busy.value||importBusy.value)return false;if(!(dirty.value||fieldDirty.value||importPreview.value))return true;if(proceed)askDiscard('离开将放弃当前模板尚未保存的修改。',()=>{original.value=JSON.stringify(form);fieldOriginal.value=JSON.stringify(fieldDraft.value);closeImport();proceed();});return false;}
function unload(e:BeforeUnloadEvent){if(busy.value||importBusy.value||dirty.value||fieldDirty.value||importPreview.value){e.preventDefault();e.returnValue='';}}
function openImport(kind:'file'|'bitable'){if(busy.value||loading.value)return;closeImport();importKind.value=kind;}
function sourceLabel(source:any){return typeof source==='string'?source:[source.filename||source.name||source.url||source.kind||source.type,source.sheet].filter(Boolean).join(' · ');}
function closeImport(){importAbort?.abort();importAbort=undefined;importKind.value='';importBusy.value=false;importFile.value=null;importPreview.value=null;importError.value='';importStale.value=false;Object.assign(importOptions,{url:'',sheet:null,title_field:null});}
function changeImportSource(){importOptions.sheet=null;importOptions.title_field=null;importPreview.value=null;importError.value='';importStale.value=true;}
function changeImportSheet(){importOptions.title_field=null;importError.value='';importStale.value=true;if(importPreview.value)importPreview.value={...importPreview.value,fields:[],items:[],title_fields:[],needs_selection:true};}
function pickImportFile(event:Event){const input=event.target as HTMLInputElement,file=input.files?.[0];input.value='';if(!file)return;importPreview.value=null;importError.value='';if(!file.size||file.size>20*1024*1024||! /\.(xlsx|xlsm|csv)$/i.test(file.name)){importFile.value=null;importError.value='请选择20MB以内的非空 xlsx、xlsm 或 csv 文件。';return;}importFile.value=file;importOptions.sheet=null;importOptions.title_field=null;importStale.value=true;}
async function previewImport(){
 if(importBusy.value||!importKind.value)return;const kind=importKind.value;
 if(kind==='file'&&!importFile.value)return;
 if(kind==='bitable'&&!/^https?:\/\/[^\s]+$/.test(importOptions.url.trim())){importError.value='请输入有效的多维表链接。';return;}
 importAbort?.abort();const controller=new AbortController();importAbort=controller;importBusy.value=true;importError.value='';importStale.value=true;
 try{const query=new URLSearchParams();if(importFile.value)query.set('name',importFile.value.name);if(importOptions.sheet)query.set('sheet',importOptions.sheet);if(importOptions.title_field)query.set('title_field',importOptions.title_field);
  const data=await requestJson(base+'/import-'+kind+(kind==='file'?'?'+query:''),{method:'POST',body:kind==='file'?importFile.value:JSON.stringify({url:importOptions.url.trim(),...(importOptions.title_field?{title_field:importOptions.title_field}:{})}),headers:kind==='file'?{'Content-Type':'application/octet-stream'}:undefined,signal:controller.signal,timeoutMs:60000});
  if(disposed||controller.signal.aborted)return;
  if(!data||!Array.isArray(data.warnings)||!data.needs_selection&&(typeof data.name!=='string'||!data.name.trim()||!Array.isArray(data.fields)||!data.fields.length||data.fields.filter((field:Dict)=>field.is_primary).length!==1||!Array.isArray(data.items)||!data.items.length||data.items.some((item:Dict)=>typeof item.title!=='string'||!item.title.trim()||!item.values||typeof item.values!=='object'||Array.isArray(item.values))))throw new Error('导入预览不完整，请重新读取。');
  importPreview.value={...data,sheets:data.sheets||importPreview.value?.sheets,title_fields:data.title_fields||importPreview.value?.title_fields};importStale.value=false;
 }catch(e:any){if(!controller.signal.aborted)importError.value=e.message;}finally{if(importAbort===controller){importAbort=undefined;importBusy.value=false;}}
}
function useImport(){
 const data=importPreview.value;if(importBusy.value||!data||data.needs_selection||importStale.value)return;
 const apply=()=>{
  importError.value='';
  try{
   // Match manual creation: getRandomValues also works on ordinary LAN HTTP origins.
   assign({id:Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join(''),name:data.name,enabled:true,fields:toRaw(data.fields),items:toRaw(data.items),source:data.source,warnings:data.warnings,version:'',revision:0,creator:'本人',created:Date.now()/1000});
   original.value='';closeImport();message.value='导入内容已带入新草稿，请核对后生成基础模板。';void loadFieldLibrary();
  }catch(e:any){importError.value='带入新草稿失败：'+(e?.message||'请重试。');}
 };
 if(dirty.value)askDiscard('带入导入内容将放弃当前未保存的修改，并新建一份模板草稿。已保存模板保留。',apply,'带入新草稿','确认带入');else apply();
}
function assign(data:Dict){const next={name:data.name,enabled:data.enabled,fields:structuredClone(data.fields),items:structuredClone(data.items)},snapshot=JSON.stringify(next);selected.value=data;Object.assign(form,next);original.value=snapshot;}
async function load(){if(busy.value)return;abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';try{const data=await requestJson(base+'?'+new URLSearchParams({page:String(page.value),search:search.value}),{cache:'no-store',signal:controller.signal});if(!controller.signal.aborted){rows.value=data.items;total.value=data.total;}}catch(e:any){if(!controller.signal.aborted)error.value=e.message;}finally{if(abort===controller){abort=undefined;loading.value=false;}}}
async function open(id:string){if(!leaveAllowed(()=>void open(id)))return;abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';try{const data=await requestJson(base+'/'+id,{cache:'no-store',signal:controller.signal});if(!controller.signal.aborted){assign(data);if(baseMode.value)void loadFieldLibrary();}}catch(e:any){if(!controller.signal.aborted)error.value=e.message;}finally{if(abort===controller){abort=undefined;loading.value=false;}}}
async function reuse(id:string){if(!leaveAllowed(()=>void reuse(id)))return;busy.value=true;error.value='';try{const data=await requestJson(base+'/'+id,{cache:'no-store'});if(!data.enabled)throw new Error('该模板已停用，请选择其它模板。');if(!disposed)emit('reuse',data);}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
function back(){if(!leaveAllowed(back))return;selected.value=null;fieldDraft.value=null;message.value='';void load();}
function validLibrary(data:Dict){if(!Array.isArray(data.fields)||!data.fields.length||data.fields.some((f:Dict)=>!f||typeof f.key!=='string'||typeof f.field_name!=='string')||data.fields.filter((f:Dict)=>f.is_primary&&f.type===1).length!==1||typeof data.version!=='string'||!data.version)throw new Error('基础字段未完整读取，请重试。');return data;}
async function loadFieldLibrary(){if(busy.value)return;libraryAbort?.abort();const controller=new AbortController();libraryAbort=controller;libraryLoading.value=true;libraryError.value='';try{const data=validLibrary(await requestJson('/api/risk-inspections/template',{cache:'no-store',timeoutMs:60000,signal:controller.signal}));if(!disposed&&!controller.signal.aborted)fieldLibrary.value=data;}catch(e:any){if(!disposed&&!controller.signal.aborted)libraryError.value=e.message;}finally{if(libraryAbort===controller)libraryLoading.value=false;}}
async function create(){if(!leaveAllowed(()=>void create()))return;abort?.abort();const controller=new AbortController();abort=controller;loading.value=true;error.value='';try{const data=validLibrary(await requestJson('/api/risk-inspections/template',{cache:'no-store',timeoutMs:60000,signal:controller.signal}));if(disposed||controller.signal.aborted)return;fieldLibrary.value=data;libraryError.value='';assign({id:Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join(''),name:'',enabled:true,fields:data.fields.filter((f:Dict)=>f.is_primary||riskSystemField(f)),items:[],version:'',revision:0,creator:'本人',created:Date.now()/1000});}catch(e:any){if(!disposed&&!controller.signal.aborted)error.value=e.message;}finally{if(abort===controller){abort=undefined;loading.value=false;}}}
function selectField(field:Dict){if(busy.value||field.unsupported||form.fields.length>=80||form.fields.some((f:Dict)=>f.key===field.key))return;if(form.fields.some((f:Dict)=>f.field_name===field.field_name||field.is_primary&&f.is_primary)){error.value='当前模板已包含同名字段或主字段，请先核对已选字段。';return;}form.fields.push(structuredClone(toRaw(field)));error.value='';}
function clearValues(key:string){for(const item of form.items)delete item.values[key];}
function removeField(f:Dict){if(busy.value||f.is_primary||riskSystemField(f))return;const remove=()=>{form.fields=form.fields.filter((v:Dict)=>v.key!==f.key);clearValues(f.key);};if(baseMode.value&&!form.items.some((i:Dict)=>i.values[f.key]!==undefined))remove();else askDiscard('移除当前模板的该字段及其预填值？字段库和历史排查结果保留。',remove,'移除已选字段','确认移除');}
function editField(field:Dict){if(riskSystemField(field))return;fieldTarget.value='template';fieldDraft.value={...structuredClone(toRaw(field)),optionsText:(field.property?.options||[]).map((option:Dict)=>option.name).join('\n')};fieldOriginal.value=JSON.stringify(fieldDraft.value);fieldError.value='';}
function addField(){if(form.fields.length>=80)return;fieldTarget.value='template';const key='custom_'+Array.from(crypto.getRandomValues(new Uint8Array(12)),value=>value.toString(16).padStart(2,'0')).join('');fieldDraft.value={key,field_id:key,field_name:'',type:1,role:'input',required:false,is_primary:false,property:{},optionsText:''};fieldOriginal.value=JSON.stringify(fieldDraft.value);fieldError.value='';}
function editLibraryField(field?:Dict){if(busy.value||libraryLoading.value||!props.canEditLibrary||!fieldLibrary.value||field&&riskSystemField(field))return;fieldTarget.value='library';fieldDraft.value=field?{...structuredClone(toRaw(field)),optionsText:(field.property?.options||[]).map((option:Dict)=>option.name).join('\n')}:{field_name:'',type:1,role:'input',required:false,is_primary:false,property:{},optionsText:''};fieldOriginal.value=JSON.stringify(fieldDraft.value);fieldError.value='';}
function deleteLibraryField(field:Dict){if(busy.value||libraryLoading.value||!props.canEditLibrary||field.is_primary||riskSystemField(field))return;askDiscard('删除字段库中的“'+field.field_name+'”及对应列数据？当前草稿中的该字段也会移除，已生成模板和已发布任务保持原样。',()=>void persistLibraryField('delete',field),'删除库字段','确认删除');}
async function persistLibraryField(action:string,field:Dict){
 if(busy.value||!props.canEditLibrary||!fieldLibrary.value)return;libraryAbort?.abort();libraryLoading.value=false;busy.value=true;libraryError.value='';fieldError.value='';
 try{const previous=form.fields.find((f:Dict)=>f.key===field.key);const data=validLibrary(await requestJson('/api/risk-inspections/template',{method:'POST',body:JSON.stringify({action,version:fieldLibrary.value.version,field_id:field.key,field_name:field.field_name,type:field.type,role:field.role,required:field.required,options:(field.property?.options||[]).map((o:Dict)=>o.name),confirmed:true}),timeoutMs:60000}));if(disposed)return;fieldLibrary.value=data;
  if(action==='delete'){form.fields=form.fields.filter((f:Dict)=>f.key!==field.key);clearValues(field.key);}
  else if(previous){const saved=data.fields.find((f:Dict)=>f.key===field.key);if(saved){if(previous.type!==saved.type)clearValues(field.key);form.fields=form.fields.map((f:Dict)=>f.key===field.key?structuredClone(saved):f);}}
  fieldDraft.value=null;message.value='字段库已保存。';
 }catch(e:any){if(!disposed){if(fieldDraft.value)fieldError.value=e.message;else libraryError.value=e.message;}}finally{busy.value=false;}
}
function closeField(){if(busy.value)return;if(fieldDirty.value){askDiscard('放弃尚未保存的字段修改？',()=>{fieldDraft.value=null;});return;}fieldDraft.value=null;}
function applyField(){
 const field=fieldDraft.value;if(!field||busy.value)return;const name=field.field_name.trim();const source=fieldTarget.value==='library'?fieldLibrary.value?.fields||[]:form.fields;
 if(!name||source.some((item:Dict)=>item.key!==field.key&&item.field_name===name)){fieldError.value='请填写不重复的字段名称。';return;}
 const originalOptions:string[]=(field.property?.options||[]).map((option:Dict)=>option.name);
 const options=field.optionsText===originalOptions.join('\n')?originalOptions:[...new Set<string>(field.optionsText.split('\n').map((value:string)=>value.trim()).filter(Boolean))];
 if([3,4].includes(field.type)&&(!options.length||options.length>100)){fieldError.value='请配置1至100个不重复的选项。';return;}
 const previous=source.find((item:Dict)=>item.key===field.key);
 const {optionsText,...value}=field;
 const updated={...value,field_name:name,property:[3,4].includes(field.type)?{options:options.map(name=>({name}))}:previous?.type===field.type?previous.property:{}};
 if(fieldTarget.value==='library'){const apply=()=>void persistLibraryField(previous?'update':'add',updated);if(previous&&previous.type!==field.type)askDiscard('更改类型可能转换或清空字段库中的对应列数据，并清除当前草稿该字段的预填值。',apply,'修改字段类型','确认修改');else apply();return;}
 const apply=()=>{if(previous&&previous.type!==field.type)clearValues(field.key);form.fields=previous?form.fields.map((item:Dict)=>item.key===field.key?updated:item):[...form.fields,updated];fieldDraft.value=null;};
 if(previous&&previous.type!==field.type&&form.items.some((item:Dict)=>item.values[field.key]!==undefined)){askDiscard('更换类型将清除该字段的模板预填值，是否继续？',apply);return;}apply();
}
async function save(){if(busy.value||!selected.value)return false;busy.value=true;error.value='';try{const existing=!!selected.value.version;const data=await requestJson(base+(existing?'/'+selected.value.id:''),{method:existing?'PUT':'POST',body:JSON.stringify({...(existing?{version:selected.value.version}:{id:selected.value.id}),...form})});assign(data);message.value='模板已保存，已有排查不受影响。';confirmOpen.value=false;}catch(e:any){error.value=e.message;}finally{busy.value=false;}return false;}
async function retry(){if(busy.value||!selected.value)return;busy.value=true;try{await requestJson(baseMode.value?base+'/'+selected.value.id+'/retry':'/api/risk-inspections/'+selected.value.source_release_id+'/retry',{method:'POST',body:JSON.stringify(baseMode.value?{}:{kind:'template_sync'})});message.value='已安排重试模板备份。';}catch(e:any){error.value=e.message;}finally{busy.value=false;}}
onMounted(()=>{void load();window.addEventListener('beforeunload',unload);});
onBeforeUnmount(()=>{disposed=true;abort?.abort();libraryAbort?.abort();importAbort?.abort();window.removeEventListener('beforeunload',unload);});
defineExpose({leaveAllowed});
</script>
<style scoped>
.history-base-prefill{min-width:0;border-top:1px solid var(--border-color,#dce2e9);padding-top:10px}.history-base-prefill summary{cursor:pointer;font-size:13px;padding:4px 0}.history-base-prefill .history-toolbar{justify-content:flex-end;padding:8px 0}
.history-import-toolbar{justify-content:flex-end}.history-import-form{display:grid;gap:12px;margin-top:12px}.history-import-form>label{display:grid;gap:6px;min-width:0}.history-import-form>.history-import-file{position:relative;display:flex;gap:8px;align-items:center;min-height:44px;border:1px dashed var(--border-color,#c9d2dd);border-radius:6px;padding:10px;overflow-wrap:anywhere}.history-import-file span{min-width:0}.history-import-file input{position:absolute;inset:0;opacity:0;width:100%;height:100%;cursor:pointer}.history-import-file:focus-within{outline:2px solid var(--primary-color,#2563eb);outline-offset:2px}.history-import-preview{display:grid;gap:10px;min-width:0;margin-top:16px}:global(.risk-history-import-modal.n-modal){width:min(920px,calc(100vw - 24px));max-height:90dvh;overflow:auto}.history-import-form>.n-button{justify-self:end}
.history-required{display:flex;align-items:center;gap:8px}
.risk-library{grid-template-columns:minmax(0,1fr)}.risk-library>*{min-width:0;max-width:100%}.history-settings label{min-width:0}
.history-field-form :deep(.n-switch){justify-self:start}
.risk-library{display:grid;gap:14px;min-width:0}.history-toolbar,.history-settings,.history-footer{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.history-toolbar>.n-input{flex:1;min-width:160px}.history-toolbar>span{flex:1}.history-toolbar>strong{flex:1}.history-settings label{display:grid;gap:6px;flex:1}.history-settings{padding-block:6px}.history-list{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(min(100%,320px),1fr))}.history-list article{border:1px solid var(--border-color,#dce2e9);border-radius:8px;padding:14px;background:var(--card-color,#fff)}.history-list article>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.history-list strong{flex:1;overflow-wrap:anywhere}.history-list p,.history-list small,.history-footer>span{font-size:13px;color:var(--text-color-3,#667085)}.history-list footer{display:flex;gap:8px;justify-content:flex-end;margin-top:12px}.history-schema,.history-item{border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.history-schema summary,.history-item summary{cursor:pointer;padding-bottom:12px}.history-field{display:grid;grid-template-columns:minmax(150px,1fr) 140px 70px 32px;gap:10px;align-items:center;padding-bottom:12px}.history-field small,.history-options{grid-column:1/-1}.history-options{display:grid;gap:6px}.history-item>details{margin-top:12px}.history-footer{justify-content:space-between;border-top:1px solid var(--border-color,#dce2e9);padding-top:12px}.history-footer>span{flex:1}@media(max-width:600px){.history-field{grid-template-columns:minmax(0,1fr) 120px 52px 28px;gap:6px}.risk-library :deep(.n-button){min-height:40px}.history-toolbar{gap:8px}.history-settings{align-items:end}.history-footer{position:sticky;bottom:0;background:var(--card-color,#fff);padding:12px 0}.history-footer>span{flex-basis:100%}}
.history-schema-actions{display:flex;justify-content:flex-end;padding-top:10px}.history-field-form{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.history-field-form label{display:grid;gap:6px;min-width:0}.history-field-form .wide{grid-column:1/-1}:global(.risk-history-field-modal.n-modal){width:min(580px,calc(100vw - 24px));max-height:90dvh;overflow:auto}.history-field-form{margin-top:12px}.history-settings{gap:14px}.history-settings label>span{font-weight:600;font-size:13px}@media(max-width:600px){.history-field-form{grid-template-columns:1fr}:global(.workspace-v2 .history-footer){bottom:calc(68px + env(safe-area-inset-bottom))}}
</style>
<style>
.workspace-v2 .risk-v2-field-modal.n-modal.n-card{overflow:hidden;display:flex;flex-direction:column;position:fixed;margin:0;box-sizing:border-box;top:calc(var(--risk-field-top,0px) + var(--risk-field-height,100dvh)*.5);left:calc(var(--risk-field-left,0px) + var(--risk-field-width,100vw)*.5);translate:-50% -50%;width:min(580px,calc(var(--risk-field-width,100vw) - 24px));max-width:calc(var(--risk-field-width,100vw) - 24px);max-height:calc(var(--risk-field-height,100dvh) - 24px)!important}
.workspace-v2 .risk-v2-field-modal>.n-card-content{min-height:0;overflow:auto;overscroll-behavior:contain}
.workspace-v2 .risk-v2-field-modal .history-field-form{margin-top:0}
.workspace-v2 .risk-v2-field-modal .history-required{flex-wrap:wrap}
.workspace-v2 .risk-v2-field-modal .n-card-header,.workspace-v2 .risk-v2-field-modal .n-card__footer{flex:none}
.workspace-v2 .risk-v2-field-menu .n-base-select-option{min-height:40px}
@media(max-width:600px){
  .workspace-v2 .risk-v2-field-modal .history-field-form{grid-template-columns:repeat(2,minmax(0,1fr))}
  .workspace-v2 .risk-v2-field-modal .history-field-form>label:first-child{grid-column:1/-1}
  .workspace-v2 .risk-v2-field-modal .n-base-selection,.workspace-v2 .risk-v2-field-modal .n-input,.workspace-v2 .risk-v2-field-menu .n-base-select-option{min-height:44px}
}
</style>
