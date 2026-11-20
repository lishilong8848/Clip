<script setup lang="ts">
import { computed, defineAsyncComponent, h, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { NButton, NDataTable, NDrawer, NModal, NPagination, NTag, useDialog, type DataTableColumns } from 'naive-ui';
import { Activity, ArrowLeft, ArrowRight, ChartNoAxesCombined, CheckCircle2, ChevronRight, CircleAlert, ClipboardList, Download, Droplets, Plus, RefreshCw, Search, SlidersHorizontal, X } from 'lucide-vue-next';
import { ApiError, requestBinaryJson, requestJson, type Dict } from './client';
import { usePageReadRefresh } from '../api/usePageReadRefresh';
import { navigate, registerNavigationGuard } from './navigation';
import { isAssistantEvent } from '../modalState';
import { trackButtonRequest } from './actionMotion';
import WaterRecordEditor from './WaterRecordEditor.vue';
import WaterDeskOverview from './WaterDeskOverview.vue';
import WaterDeskAbnormal from './WaterDeskAbnormal.vue';
import { logicalUrl } from './version';
import { currentEmbedded as waterEmbedded } from './embeddedContext';
import { validWaterDate, waterDay, waterDefaultFilters, waterImageMime, waterNumber, waterNumberText, waterPhotoCount, waterQuery, waterRecordState, waterScopes, waterTrend, type WaterDraft, type WaterFilters, type WaterPhoto } from './water';
const MotionChart=defineAsyncComponent(()=>import('./MotionChart.vue'));
const props=defineProps<{scope:string;scopeOptions:{value:string;label:string}[];initialView?:string}>();
const emit=defineEmits<{'switch-scope':[scope:string];status:[message:string]}>();
const dialog=useDialog();
const availableScopes=computed(()=>props.scopeOptions.filter(item=>waterScopes.has(item.value)));
const scope=computed(()=>availableScopes.value.some(item=>item.value===props.scope)?props.scope:'');
const showOverview=computed(()=>props.initialView!=='records'||!scope.value);
const bootstrap=ref<Dict>({}),bootstrapLoading=ref(false),bootstrapError=ref(''),refreshing=ref(false);
const canCreate=computed(()=>bootstrap.value.permissions?.can_create===true);
const records=ref<Dict[]>([]),recordsReady=ref(false),recordsLoading=ref(false),recordsError=ref(''),filterError=ref(''),total=ref(0),page=ref(1),loadedQuery=ref(''),loadedQueryLabel=ref(''),queryDay=ref(waterDay());
const ranges=['近30天','近3天','本月','全部时间','自定义日期'];
const filters=reactive<WaterFilters>(waterDefaultFilters());
type OverviewContext={scope:string;startDate:string;endDate:string;frequency:'日';selectedDay:string};
function overviewRouteFilters(){
  const url=logicalUrl(),params=url.searchParams,startDate=params.get('water_start')||'',endDate=params.get('water_end')||'';
  return url.pathname==='/water-management'&&params.get('water_view')==='records'&&params.get('scope')===props.scope&&availableScopes.value.some(item=>item.value===props.scope)&&validWaterDate(startDate)&&validWaterDate(endDate)&&startDate<=endDate&&params.get('water_frequency')==='日'?{range:'自定义日期',startDate,endDate,frequency:'日',meter:'',shift:'',query:''}:null;
}
const overviewOrigin=ref(overviewRouteFilters());
if(overviewOrigin.value)Object.assign(filters,overviewOrigin.value);
const overviewSource=computed(()=>!!overviewOrigin.value&&Object.entries(overviewOrigin.value).every(([key,value])=>filters[key as keyof WaterFilters]===value));
const embedded=waterEmbedded();
let restoredRoute=logicalUrl().href;
function restoreRouteFilters(){
  const url=logicalUrl();if(url.href===restoredRoute)return;restoredRoute=url.href;
  overviewOrigin.value=overviewRouteFilters();
  Object.assign(filters,overviewOrigin.value||waterDefaultFilters());page.value=1;attention.value='';
}
watch(()=>embedded?.route.value.href,restoreRouteFilters);
const advancedOpen=ref(false),trendOpen=ref(false),attention=ref('');
const attentionOptions=[{label:'全部',value:''},{label:'待填写',value:'未记录'},{label:'需核查',value:'变化较大'},{label:'缺照片',value:'待补照片'}];
const advancedCount=computed(()=>[filters.meter,filters.frequency,filters.shift].filter(Boolean).length);
const filterFields = [
  {key: 'meter', source: 'meters', label: '水表'},
  {key: 'frequency', source: 'frequencies', label: '频次'},
  {key: 'shift', source: 'shifts', label: '班次'}
] as const;
const totalPages=computed(()=>Math.max(1,Math.ceil(total.value/50)));
const querySignature=computed(()=>{try{return waterQuery(scope.value,filters,page.value,queryDay.value).toString();}catch{return '';}});
const recordsStale=computed(()=>loadedQuery.value!==querySignature.value);
const visibleRecords=computed(()=>attention.value?records.value.filter(row=>waterRecordState(row).label===attention.value):records.value);
const trend=computed(()=>waterTrend(records.value));
const emptyText=computed(()=>attention.value?'本页没有符合该状态的记录':'当前条件下暂无水耗记录');
const notice=reactive({text:'',tone:'success'});
const media=matchMedia('(max-width:760px)'),mobile=ref(media.matches);
const editorOpen=ref(false),recordId=ref(''),detail=ref<Dict>({}),editorLoading=ref(false),saving=ref(false),uploading=ref(false),uncertain=ref(false),editorError=ref('');
const form=reactive<WaterDraft>({meter:'',frequency:'',shift:'',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});
const retained=ref<Dict[]>([]),photos=ref<WaterPhoto[]>([]),errors=reactive<Record<string,string>>({}),baseline=ref('');
const editor=ref<InstanceType<typeof WaterRecordEditor>>(),mobileEditor=ref<InstanceType<typeof WaterRecordEditor>>();
const admin=computed(()=>bootstrap.value.permissions?.is_admin===true||detail.value.edit_policy?.is_admin===true);
const locked=computed(()=>!!recordId.value&&!admin.value&&detail.value.edit_policy?.can_edit!==true);
const draftStamp=()=>JSON.stringify([form,retained.value.map(photo=>photo.image_id),photos.value.map(photo=>[photo.localId,photo.uploadId])],(_key,value)=>typeof value==='number'&&!Number.isFinite(value)?'invalid-number':value);
const dirty=computed(()=>editorOpen.value&&baseline.value!==draftStamp());
const busy=computed(()=>saving.value||uploading.value);
const editorProps=computed(()=>({scope:scope.value,recordId:recordId.value,form,detail:detail.value,options:bootstrap.value.options||{},retained:retained.value,photos:photos.value,errors,error:editorError.value,loading:editorLoading.value,saving:saving.value,uploading:uploading.value,locked:locked.value,admin:admin.value,dirty:dirty.value,uncertain:uncertain.value}));
const abnormalOpen=ref(false),abnormalNote=ref(''),abnormalError=ref(''),abnormalDetails=ref<Dict>({}),abnormalMessage=ref('');
const lightbox=reactive({url:'',name:''});
const buildingLabel = computed(() => availableScopes.value.find(item => item.value === scope.value)?.label || scope.value);
const summary = computed(() => bootstrap.value.snapshot?.exists ? bootstrap.value.summary || {} : {});
const attentionCounts = computed(() => {
  const counts: Record<string, number> = { '': records.value.length };
  for (const row of records.value) {
    const state = waterRecordState(row).label;
    counts[state] = (counts[state] || 0) + 1;
  }
  return counts;
});
const editorPosition = computed(() => visibleRecords.value.findIndex(row => row.record_id === recordId.value));
function stepRecord(offset: number) {
  const row = visibleRecords.value[editorPosition.value + offset];
  if (row && !recordsStale.value && !busy.value) openRecord(row.record_id);
}
function resetFilters() {
  Object.assign(filters, waterDefaultFilters());
  attention.value = '';
}
const editorEvents = {
  close: () => leaveEditing(closeEditor), reload: reloadEditor, save: () => saveRecord(),
  retrySave, change: fieldChanged, upload: uploadFiles, retryPhoto, removePhoto,
  removeRetained, preview: previewPhoto
};
const snapshotText=computed(()=>{const s=bootstrap.value.snapshot||{};if(refreshing.value)return '正在同步数据';if(bootstrapError.value)return '同步状态暂不可用';const at=waterNumber(s.refreshed_at);return at?'更新于 '+new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}).format(new Date(at*1000)):'等待首次同步';});
const ratio=(v:unknown)=>waterNumber(v)===null?'—':waterNumberText(Number(v)*100,1)+'%';
function filterOptions(key:string,label:string){return [{label,value:''},...(bootstrap.value.options?.[key]||[]).map((value:string)=>({label:value,value}))];}
const recordUsage=(row:Dict)=>waterNumber(row.corrected_usage) ?? (waterNumber(row.meter_value)===null?null:row.computed_usage);
const meterLabel=(row:Dict)=>String(row.meter || '').trim() || '水表未填写';
const columns:DataTableColumns<Dict>=[
  {title:'日期',key:'statistic_date',width:'13%',className:'water-date-cell',render:row=>row.statistic_date||'—'},
  {title:'水表',key:'meter',width:'26%',className:'water-meter-cell',render:row=>{const label=meterLabel(row);return h(NButton,{text:true,'data-v2-navigation':'',class:'water-meter-link',title:label,disabled:busy.value||recordsStale.value,'aria-label':'填写'+label+row.statistic_date,onClick:()=>openRecord(row.record_id)},{default:()=>label});}},
  {title:'频次 / 班次',key:'frequency',width:'12%',align:'center',className:'water-frequency-cell',render:row=>`${row.frequency||'—'} / ${row.shift||'—'}`},
  {title:'水表读数',key:'meter_value',width:'15%',align:'right',className:'water-number-cell',render:row=>waterNumberText(row.meter_value)},
  {title:'耗水量 / t',key:'usage',width:'14%',align:'right',className:'water-number-cell',render:row=>waterNumberText(recordUsage(row))},
  {title:'状态',key:'status',width:'12%',align:'center',className:'water-state-cell',render:row=>h(NTag,{size:'small',bordered:false,type:waterRecordState(row).type,title:waterRecordState(row).title},{default:()=>waterRecordState(row).label})},
  {title:'照片',key:'photo_count',width:'8%',align:'center',className:'water-photo-cell',render:row=>waterPhotoCount(row)===null?'—':waterPhotoCount(row)+' 张'},
];
function recordClass(row:Dict){return ['water-row-'+waterRecordState(row).type,row.record_id===recordId.value?'water-row-selected':''].join(' ');}
function rowProps(row:Dict){return {class:recordClass(row),onClick:(event:MouseEvent)=>{if(!(event.target as Element)?.closest('button,a')&&!busy.value&&!recordsStale.value)void openRecord(row.record_id);}};}
const rowKey=(row:Dict)=>row.record_id;
function drawerVisibility(show:boolean){if(!show)leaveEditing(closeEditor);}
function abnormalVisibility(show:boolean){if(!saving.value)abnormalOpen.value=show;}
function lightboxVisibility(show:boolean){if(!show)lightbox.url='';}
let disposed=false,bootstrapRead:AbortController|undefined,listRead:AbortController|undefined,detailRead:AbortController|undefined;
let listGeneration=0,detailGeneration=0,bootstrapGeneration=0,filterTimer=0,pollTimer=0;
let operation='',pendingSave:{path:string;method:string;payload:Dict}|null=null;
const lifetime=new AbortController();
function signalError(error:unknown){return error instanceof Error?error.message:'数据读取失败，请重试。';}
function queryLabel(){return [filters.range==='本月'?filters.month:filters.range==='自定义日期'?`${filters.startDate} 至 ${filters.endDate}`:filters.range,filters.meter||'全部水表',filters.frequency,filters.shift&&filters.shift+'班',filters.query].filter(Boolean).join(' · ');}
async function loadBootstrap(fresh=false){
  if(showOverview.value||!scope.value||disposed)return;bootstrapRead?.abort();const c=new AbortController();bootstrapRead=c;const generation=++bootstrapGeneration;bootstrapLoading.value=true;
  const previousVersion=bootstrap.value.snapshot?.snapshot_version;
  const params=new URLSearchParams({scope:scope.value});if(fresh)params.set('force','1');
  try{const data=await requestJson('/api/capacity/water/bootstrap?'+params,{signal:c.signal});if(c.signal.aborted||disposed||generation!==bootstrapGeneration)return;
    bootstrap.value=data;bootstrapError.value='';refreshing.value=data.snapshot?.refreshing===true;
    if(data.snapshot?.status==='failed'){refreshing.value=false;bootstrapError.value=data.snapshot.error||'同步失败，保留上次数据。';}
    if(data.snapshot?.exists===true&&(!recordsReady.value&&!recordsLoading.value||previousVersion!==undefined&&previousVersion!==data.snapshot.snapshot_version))void loadRecords(true);
    schedulePoll();
  }catch(error){if(!c.signal.aborted&&!disposed){bootstrapError.value=signalError(error);refreshing.value=false;clearTimeout(pollTimer);}}
  finally{if(generation===bootstrapGeneration)bootstrapLoading.value=false;}
}
async function loadRecords(fresh=false){
  listRead?.abort();const generation=++listGeneration;if(showOverview.value||!scope.value||disposed)return;queryDay.value=waterDay();
  let query:URLSearchParams;try{query=waterQuery(scope.value,filters,page.value,queryDay.value);filterError.value='';}catch(error){filterError.value=signalError(error);recordsLoading.value=false;return;}
  const signature=query.toString(),label=queryLabel(),c=new AbortController();listRead=c;recordsLoading.value=true;recordsError.value='';
  const requestParams=new URLSearchParams(query);if(fresh)requestParams.set('force','1');
  try{const data=await requestJson('/api/capacity/water/records?'+requestParams,{signal:c.signal});if(c.signal.aborted||disposed||generation!==listGeneration)return;
    if(!Array.isArray(data.records)||waterNumber(data.total)===null)throw new Error('记录返回不完整，请重试。');
    if(data.snapshot?.exists===false){recordsReady.value=false;return;}
    const amount=Number(data.total);if(amount<0)throw new Error('记录数量暂不可用。');
    if(page.value>Math.max(1,Math.ceil(amount/50))){page.value=Math.max(1,Math.ceil(amount/50));void loadRecords(fresh);return;}
    records.value=data.records;total.value=amount;recordsReady.value=true;loadedQuery.value=signature;loadedQueryLabel.value=label;
  }catch(error){if(!c.signal.aborted&&!disposed&&generation===listGeneration)recordsError.value=signalError(error);}
  finally{if(generation===listGeneration)recordsLoading.value=false;}
}
function schedulePoll(){clearTimeout(pollTimer);if(disposed||document.hidden||!refreshing.value)return;pollTimer=window.setTimeout(()=>{void loadBootstrap(true);},3000);}
async function refreshData(){if(refreshing.value||busy.value)return;refreshing.value=true;
  try{await requestJson('/api/capacity/water/refresh?'+new URLSearchParams({scope:scope.value}),{method:'POST',body:'{}'});if(!disposed)await loadBootstrap(true);}
  catch(error){if(!disposed){refreshing.value=false;bootstrapError.value=signalError(error);notice.text=bootstrapError.value;notice.tone='warning';}}
}
function leaveEditing(proceed:()=>void){
  if(busy.value){notice.text='正在保存或上传照片，请等待结果。';notice.tone='warning';return;}
  if(uncertain.value){dialog.warning({title:'保存结果尚未确认',content:'离开后请先核验原记录，不要重新新增相同记录。',positiveText:'确认离开',negativeText:'继续原任务',onPositiveClick:proceed});return;}
  if(dirty.value){dialog.warning({title:'放弃未保存的水耗填写？',content:'本次读数和未保存照片将被丢弃。',positiveText:'放弃并离开',negativeText:'继续填写',onPositiveClick:proceed});}else proceed();
}
const removeGuard=registerNavigationGuard((_url,proceed)=>{if(busy.value||dirty.value||uncertain.value){leaveEditing(proceed);return false;}return true;});
function setView(view:'overview'|'records',building=scope.value||availableScopes.value[0]?.value){
  if(view==='overview'&&showOverview.value||view==='records'&&!showOverview.value&&building===scope.value)return;
  if(view==='records'&&!availableScopes.value.some(item=>item.value===building))return;
  leaveEditing(()=>{
    closeEditor();notice.text='';
    const url=new URL(logicalUrl());url.searchParams.set('water_view',view);
    for(const key of ['record_id','water_start','water_end','water_frequency'])url.searchParams.delete(key);
    if(view==='records'&&building)url.searchParams.set('scope',building);
    navigate(url);
  });
}
function chooseBuilding(event: Event) {
  const select = event.target as HTMLSelectElement;
  const next = select.value;
  select.value = scope.value;
  if (next === scope.value || !availableScopes.value.some(item => item.value === next)) return;
  leaveEditing(() => {
    closeEditor();
    notice.text = '';
    const url = new URL(logicalUrl());
    if (url.searchParams.has('record_id')) {
      url.searchParams.delete('record_id');
      navigate(url, {replace: true});
    }
    emit('switch-scope', next);
  });
}
function openOverviewRecords(context:OverviewContext){
  if(!availableScopes.value.some(item=>item.value===context.scope)||!validWaterDate(context.startDate)||!validWaterDate(context.endDate)||context.startDate>context.endDate)return;
  leaveEditing(()=>{
    closeEditor();notice.text='';page.value=1;attention.value='';
    const origin=new URL(logicalUrl());origin.searchParams.set('water_overview_day',context.selectedDay);
    // Save the chart selection in the history entry that Back will return to.
    navigate(origin,{replace:true});
    overviewOrigin.value={range:'自定义日期',startDate:context.startDate,endDate:context.endDate,frequency:'日',meter:'',shift:'',query:''};
    Object.assign(filters,overviewOrigin.value);
    const url=new URL(logicalUrl());url.searchParams.delete('record_id');
    for(const [key,value] of Object.entries({water_view:'records',scope:context.scope,water_start:context.startDate,water_end:context.endDate,water_frequency:'日',water_overview_day:context.selectedDay}))url.searchParams.set(key,value);
    navigate(url);
  });
}
function clearPhotos(){for(const photo of photos.value)URL.revokeObjectURL(photo.previewUrl);photos.value=[];}
function resetEditor(){clearPhotos();retained.value=[];detail.value={};editorError.value='';for(const key of Object.keys(errors))delete errors[key];operation='';pendingSave=null;uncertain.value=false;abnormalOpen.value=false;abnormalNote.value='';Object.assign(form,{meter:'',frequency:'',shift:'',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});baseline.value=draftStamp();}
function closeEditor(){detailRead?.abort();detailGeneration++;editorOpen.value=false;editorLoading.value=false;recordId.value='';resetEditor();}
function openCreate(){if(!canCreate.value){notice.text='当前账号无新增权限。';notice.tone='warning';return;}closeEditor();notice.text='';Object.assign(form,{meter:'',frequency:bootstrap.value.options?.frequencies?.[0]||'日',shift:bootstrap.value.options?.shifts?.[0]||'白',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});editorOpen.value=true;baseline.value=draftStamp();}
async function readRecord(id:string){
  detailRead?.abort();const c=new AbortController();detailRead=c;const generation=++detailGeneration;
  recordId.value=id;resetEditor();notice.text='';editorOpen.value=true;editorLoading.value=true;
  try{const data=await requestJson('/api/capacity/water/records/'+encodeURIComponent(id)+'?'+new URLSearchParams({scope:scope.value}),{signal:c.signal,cache:'no-store'});if(disposed||c.signal.aborted||generation!==detailGeneration)return;
    // The existing API returns an empty version when Feishu has no modification timestamp.
    if(data.record_id!==id||!['string','number'].includes(typeof data.version)||!Array.isArray(data.photos))throw new Error('记录或照片信息未完整返回，已阻止填写，请重新读取。');detail.value=data;
    Object.assign(form,{meter:String(data.meter||''),frequency:String(data.frequency||''),shift:String(data.shift||''),statisticDate:String(data.statistic_date||data.statistic_date_key||''),meterValue:waterNumber(data.meter_value),correctedUsage:waterNumber(data.corrected_usage),expectedVersion:String(data.version||'')});
    retained.value=Array.isArray(data.photos)?data.photos.map((photo:Dict)=>({...photo})):[];baseline.value=draftStamp();
  }catch(error){if(!c.signal.aborted&&!disposed)editorError.value=signalError(error);}
  finally{if(generation===detailGeneration)editorLoading.value=false;}
}
function openRecord(id:string){if(editorOpen.value&&recordId.value===id)return;leaveEditing(()=>{void readRecord(id);});}
function reloadEditor(){const id=recordId.value;if(id)leaveEditing(()=>{void readRecord(id);});}
function fieldChanged(field:string){
  delete errors[field];
  if((field==='meterValue'||field==='correctedUsage')&&form[field]!==null&&!Number.isFinite(form[field]))errors[field]='请填写有效数字，例如 1234.56。';
  if(!uncertain.value){operation='';pendingSave=null;}editorError.value='';
}
function validate(){
  for(const key of Object.keys(errors))delete errors[key];
  if(!form.meter)errors.meter='请选择水表。';if(!form.frequency)errors.frequency='请选择统计频次。';if(!form.shift)errors.shift='请选择班次。';if(!validWaterDate(form.statisticDate))errors.statisticDate='请选择有效的统计日期。';
  if(form.meterValue===null||!Number.isFinite(form.meterValue)||form.meterValue<0)errors.meterValue='请填写大于或等于零的水表读数。';
  if(form.correctedUsage!==null&&!Number.isFinite(form.correctedUsage))errors.correctedUsage='请填写有效的耗水量。';
  if(photos.value.some(photo=>photo.error))errors.photos='请重试失败照片，或移除后重新上传。';else if(!retained.value.length&&!photos.value.some(photo=>photo.uploadId))errors.photos='请至少上传一张水表照片。';
  const field=Object.keys(errors)[0];if(field){void (mobile.value?mobileEditor.value:editor.value)?.focus(field);return false;}return true;
}
function saveRecord(confirmed=false,note=''){
  if(busy.value||locked.value||uncertain.value||(!recordId.value&&!canCreate.value)||!validate())return;
  if(!operation)operation=crypto.randomUUID?.()||'water_'+Date.now()+'_'+Math.random().toString(36).slice(2);
  pendingSave={path:'/api/capacity/water/records'+(recordId.value?'/'+encodeURIComponent(recordId.value):''),method:recordId.value?'PATCH':'POST',payload:{operation_id:operation,expected_version:form.expectedVersion,scope:scope.value,meter:form.meter,frequency:form.frequency,shift:form.shift,statistic_date:form.statisticDate,meter_value:form.meterValue,corrected_usage:form.correctedUsage,upload_ids:photos.value.map(photo=>photo.uploadId).filter(Boolean),retained_image_ids:retained.value.map(photo=>photo.image_id).filter(Boolean),large_change_confirmed:confirmed,abnormal_note:note}};
  return trackButtonRequest(executeSave());
}
async function executeSave(){
  if(!pendingSave||saving.value)return {ok:false};saving.value=true;editorError.value='';const task=pendingSave;
  try{const result=await requestJson(task.path,{method:task.method,body:JSON.stringify(task.payload)});if(disposed)return result;
    saving.value=false;closeEditor();const warnings=Array.isArray(result.warnings)?result.warnings.filter(Boolean):[];notice.text='水耗记录已保存。'+warnings.join('；');notice.tone=warnings.length?'warning':'success';
    void Promise.all([loadBootstrap(),loadRecords()]);return result;
  }catch(error){if(disposed)return {ok:false};const payload=error instanceof ApiError?error.payload:{};
    if(payload.error_code==='confirmation_required'&&payload.details?.kind==='water_large_change'){abnormalDetails.value=payload.details;abnormalMessage.value=signalError(error);abnormalError.value='';abnormalOpen.value=true;}
    else{editorError.value=signalError(error);uncertain.value=!(error instanceof ApiError)||error.status===0||error.status>=500;if(uncertain.value){abnormalOpen.value=false;editorError.value+=' 保存结果尚未确认，请继续原保存任务核验。';}else abnormalError.value=editorError.value;}
    return {ok:false};
  }finally{saving.value=false;}
}
function retrySave(){if(uncertain.value&&pendingSave&&!busy.value)return trackButtonRequest(executeSave());}
function saveWithNote(){if(!abnormalNote.value.trim()){abnormalError.value='请填写异常原因或现场核查情况。';return;}return saveRecord(true,abnormalNote.value.trim());}
async function uploadFiles(files:File[]){
  if(busy.value||locked.value||uncertain.value||!editorOpen.value)return;const accepted:WaterPhoto[]=[],rejected:string[]=[];
  for(const file of files){if(!waterImageMime(file)){rejected.push(file.name+' 不是支持的图片。');continue;}if(file.size>8*1024*1024){rejected.push(file.name+' 超过8MB，请压缩后上传。');continue;}
    accepted.push(reactive({localId:crypto.randomUUID?.()||String(Date.now()+Math.random()),uploadId:'',name:file.name,file,previewUrl:URL.createObjectURL(file),uploading:false,error:''}));}
  if(rejected.length){errors.photos=rejected.join('；');void (mobile.value?mobileEditor.value:editor.value)?.focus('photos');}
  if(!accepted.length)return;photos.value.push(...accepted);fieldChanged('photos');if(rejected.length)errors.photos=rejected.join('；');uploading.value=true;
  try{for(const photo of accepted){if(disposed)break;await uploadPhoto(photo);}}finally{uploading.value=false;}
}
async function uploadPhoto(photo:WaterPhoto){
  photo.uploading=true;photo.error='';
  try{const result=await requestBinaryJson('/api/capacity/water/uploads?'+new URLSearchParams({scope:scope.value,file_name:photo.name}),photo.file,{headers:{'Content-Type':waterImageMime(photo.file)},signal:lifetime.signal});if(disposed)return;if(!result.upload_id)throw new Error('未取得照片上传结果，请重试。');photo.uploadId=String(result.upload_id);}
  catch(error){if(!disposed){photo.error=signalError(error);photo.uploadId='';}}
  finally{photo.uploading=false;}
}
async function retryPhoto(photo:WaterPhoto){if(busy.value||locked.value||uncertain.value||photo.uploadId||!photo.error)return;fieldChanged('photos');uploading.value=true;try{await uploadPhoto(photo);}finally{uploading.value=false;}}
function removePhoto(photo:WaterPhoto){if(busy.value||locked.value||uncertain.value)return;if(lightbox.url===photo.previewUrl)lightbox.url='';URL.revokeObjectURL(photo.previewUrl);photos.value=photos.value.filter(item=>item.localId!==photo.localId);fieldChanged('photos');}
function removeRetained(photo:Dict){if(busy.value||locked.value||uncertain.value)return;dialog.warning({title:'移除水表照片？',content:'保存记录后，这张照片不再保留。',positiveText:'移除照片',negativeText:'保留',onPositiveClick:()=>{retained.value=retained.value.filter(item=>item.image_id!==photo.image_id);fieldChanged('photos');}});}
function previewPhoto(url:string,name:string){lightbox.url=url;lightbox.name=name;}
function changePage(value:number){page.value=value;void loadRecords();}
function drillDay(item:{label:string}){Object.assign(filters,{range:'自定义日期',startDate:item.label,endDate:item.label});attention.value='';}
watch(()=>[filters.range,filters.month,filters.startDate,filters.endDate,filters.meter,filters.frequency,filters.shift,filters.query],()=>{
  // Cancel before validation/debounce so an old response cannot overwrite invalid filters.
  listRead?.abort();listGeneration++;recordsLoading.value=false;clearTimeout(filterTimer);page.value=1;queryDay.value=waterDay();
  try{waterQuery(scope.value,filters);filterError.value='';}catch(error){filterError.value=signalError(error);return;}
  filterTimer=window.setTimeout(()=>{void loadRecords();},250);
},{flush:'sync'});
usePageReadRefresh(url=>url.pathname.startsWith('/api/capacity/water/')&&url.searchParams.get('scope')===scope.value,async()=>{await Promise.all([loadBootstrap(),loadRecords()]);},()=>!showOverview.value&&!busy.value&&!dirty.value&&!editorLoading.value&&!bootstrapLoading.value&&!recordsLoading.value&&!uncertain.value);
watch([scope,showOverview], ([building,overview],[previousBuilding]) => {
  if (building !== previousBuilding) {
    closeEditor(); clearTimeout(filterTimer); clearTimeout(pollTimer);
    bootstrap.value = {}; bootstrapError.value = ''; refreshing.value = false;
    records.value = []; recordsReady.value = false; recordsError.value = ''; loadedQuery.value = '';
    total.value = 0; page.value = 1; attention.value = ''; notice.text = '';
  }
  restoreRouteFilters();
  if (overview) { bootstrapRead?.abort(); listRead?.abort(); clearTimeout(pollTimer); }
  else void Promise.all([loadBootstrap(),loadRecords()]);
});
onMounted(()=>window.addEventListener('popstate',restoreRouteFilters));
onBeforeUnmount(()=>window.removeEventListener('popstate',restoreRouteFilters));
function viewport(){mobile.value=media.matches;}
function visibility(){clearTimeout(pollTimer);if(!document.hidden){if(queryDay.value!==waterDay())void loadRecords();if(refreshing.value)void loadBootstrap(true);else if(!bootstrap.value.snapshot?.exists&&!bootstrapLoading.value)void loadBootstrap();}}
function unload(event:BeforeUnloadEvent){if(busy.value||dirty.value||uncertain.value){event.preventDefault();event.returnValue='';}}
function escape(event:KeyboardEvent){if(event.key!=='Escape'||event.defaultPrevented||isAssistantEvent(event)||lightbox.url||abnormalOpen.value||(event.target as Element)?.closest('.n-dialog,.water-abnormal-sheet,.water-photo-dialog'))return;if(editorOpen.value){event.preventDefault();leaveEditing(closeEditor);}}
onMounted(()=>{void Promise.all([loadBootstrap(),loadRecords()]);const target=logicalUrl().searchParams.get('record_id');if(target&&!showOverview.value)void readRecord(target);media.addEventListener('change',viewport);window.addEventListener('beforeunload',unload);document.addEventListener('visibilitychange',visibility);window.addEventListener('keydown',escape);});
onBeforeUnmount(()=>{disposed=true;bootstrapRead?.abort();listRead?.abort();detailRead?.abort();lifetime.abort();clearTimeout(filterTimer);clearTimeout(pollTimer);clearPhotos();removeGuard();media.removeEventListener('change',viewport);window.removeEventListener('beforeunload',unload);document.removeEventListener('visibilitychange',visibility);window.removeEventListener('keydown',escape);});
</script>

<template>
  <div class="water-desk v2-water">
    <header class="desk-header">
      <div class="desk-identity">
        <span class="desk-symbol"><Droplets :size="24" aria-hidden="true"/></span>
        <div><h1>水耗管理</h1><p>{{ showOverview ? '授权楼栋 · 本月日统计' : buildingLabel + ' · 水表读数与现场照片' }}</p></div>
      </div>
      <div class="desk-views" role="group" aria-label="水耗工作视图">
        <button type="button" data-v2-navigation :aria-pressed="showOverview" @click="setView('overview')">
          <Activity :size="16" aria-hidden="true"/>水耗总览
        </button>
        <button type="button" data-v2-navigation :aria-pressed="!showOverview" :disabled="!availableScopes.length" @click="setView('records')">
          <ClipboardList :size="16" aria-hidden="true"/>记录办理
        </button>
      </div>
    </header>

    <div v-if="notice.text" class="water-notice desk-notice" :class="{ caution: notice.tone==='warning' }" role="status">
      <component :is="notice.tone==='warning' ? CircleAlert : CheckCircle2" :size="17" aria-hidden="true"/>
      <span>{{ notice.text }}</span>
      <button type="button" class="icon-button" aria-label="关闭提示" @click="notice.text=''"><X :size="16"/></button>
    </div>

    <WaterDeskOverview v-if="showOverview" :scope="scope" :scope-options="availableScopes"
      :initial-day="logicalUrl().searchParams.get('water_overview_day') || ''" @open="openOverviewRecords"/>
    <template v-else>
      <section class="record-context water-query-summary" aria-label="楼栋记录概况">
        <div class="context-building">
          <label>办理楼栋
            <select aria-label="水耗楼栋" :value="scope" :disabled="busy || availableScopes.length < 2" @change="chooseBuilding">
              <option v-for="item in availableScopes" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </label>
          <small>{{ snapshotText }}</small>
        </div>
        <dl class="context-numbers" :aria-label="(bootstrap.month || '本月')+'水耗汇总'">
          <div><dt>本月耗水量</dt><dd>{{ waterNumberText(summary.total_usage) }} <small>t</small></dd></div>
          <div><dt>本月记录</dt><dd>{{ waterNumberText(summary.record_count) }} <small>条</small></dd></div>
          <div><dt>含照片记录</dt><dd>{{ waterNumberText(summary.photo_records) }} <small>条</small></dd></div>
        </dl>
        <div class="context-actions">
          <button class="tool-button" type="button" :disabled="refreshing || busy" @click="refreshData"><RefreshCw :size="15" aria-hidden="true"/>{{ refreshing ? '同步中' : '同步数据' }}</button>
          <button v-if="canCreate" class="primary-button" type="button" :disabled="busy" @click="leaveEditing(openCreate)"><Plus :size="16" aria-hidden="true"/>新增录入</button>
        </div>
        <p v-if="bootstrapError" class="inline-error context-error" role="alert">
          基础数据读取失败 · {{ bootstrapError }}
          <button class="text-button" type="button" :disabled="bootstrapLoading" @click="loadBootstrap(true)">重读基础数据</button>
        </p>
      </section>

      <section v-if="!editorOpen || mobile" class="record-book" aria-label="水表记录办理">
        <div class="book-filters">
          <div class="filter-tools">
            <h2>水表记录</h2>
            <label class="water-search">
              <Search :size="16" aria-hidden="true"/>
              <input v-model="filters.query" type="search" aria-label="搜索水耗记录" placeholder="搜索水耗记录">
            </label>
            <button class="tool-button" type="button" :aria-expanded="advancedOpen" @click="advancedOpen=!advancedOpen">
              <SlidersHorizontal :size="15" aria-hidden="true"/>筛选<span v-if="advancedCount">({{ advancedCount }})</span>
            </button>
            <button class="tool-button" type="button" :aria-expanded="trendOpen" @click="trendOpen=!trendOpen">
              <ChartNoAxesCombined :size="16" aria-hidden="true"/>趋势
            </button>
          </div>
          <div class="range-picker" role="group" aria-label="记录日期范围">
            <button v-for="range in ranges" :key="range" type="button" :aria-pressed="filters.range === range" @click="filters.range=range">{{ range }}</button>
          </div>
          <div v-if="filters.range==='自定义日期' || filters.range==='本月'" class="field-row">
            <label v-if="filters.range==='本月'">统计月份<input v-model="filters.month" type="month" aria-label="筛选月份"></label>
            <template v-else>
              <label>开始日期<input v-model="filters.startDate" type="date" aria-label="筛选开始日期"></label>
              <label>结束日期<input v-model="filters.endDate" type="date" aria-label="筛选结束日期"></label>
            </template>
          </div>
          <div v-if="advancedOpen" class="field-row filter-details">
            <label v-for="item in filterFields" :key="item.key">{{ item.label }}
              <select v-model="filters[item.key]">
                <option v-for="option in filterOptions(item.source, '全部'+item.label)" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </label>
            <button class="text-button" type="button" @click="resetFilters">重置筛选</button>
          </div>
          <p v-if="filterError" role="alert" class="inline-error">{{ filterError }}</p>
          <p v-if="overviewSource" class="origin-note">沿用总览的楼栋、日期及日统计条件。</p>
        </div>

        <div v-if="trendOpen" class="book-trend">
          <MotionChart title="本页日统计耗水趋势" caption="选择日期可查看当日记录" :items="recordsStale ? [] : trend.items"
            :loading="recordsLoading" :failed="!!recordsError || !!filterError" trend view="line" unit="t" @select="drillDay"/>
          <p v-if="!recordsStale && trend.missing" class="origin-note">{{ trend.missing }} 条日统计数据不完整，对应日期未纳入趋势。</p>
        </div>

        <div class="queue-heading">
          <span class="queue-label">本页状态</span>
          <div class="queue-options" role="group" aria-label="本页记录状态">
            <button v-for="item in attentionOptions" :key="item.value" type="button" :aria-pressed="attention===item.value"
              @click="attention=item.value">{{ item.label }} <b>{{ recordsReady && !recordsStale ? (attentionCounts[item.value] || 0) : '—' }}</b></button>
          </div>
        </div>
        <div v-if="recordsError" class="water-read-error book-message" role="alert">
          <strong>水耗记录暂不可用</strong><span>{{ recordsError }}</span>
          <button type="button" class="tool-button" @click="loadRecords(true)">重试读取</button>
        </div>
        <p v-if="recordsReady && recordsStale" class="water-stale book-message" role="status">当前显示上次查询的记录（{{ loadedQueryLabel }}），新条件生效后可办理。</p>
        <div v-if="recordsReady" class="book-results" :aria-busy="recordsLoading">
          <div v-if="visibleRecords.length && mobile" class="mobile-list">
            <button v-for="row in visibleRecords" :key="row.record_id" type="button" data-v2-navigation
              class="water-mobile-record" :class="recordClass(row)" :disabled="busy || recordsStale"
              :aria-label="'填写'+meterLabel(row)+row.statistic_date" @click="openRecord(row.record_id)">
              <span class="mobile-title"><strong>{{ meterLabel(row) }}</strong><ChevronRight :size="16" aria-hidden="true"/></span>
              <span class="mobile-meta">{{ row.statistic_date || '—' }} · {{ row.frequency || '—' }} / {{ row.shift || '—' }}</span>
              <span class="mobile-numbers"><span>读数 <b>{{ waterNumberText(row.meter_value) }}</b></span><span>耗水量 <b>{{ waterNumberText(recordUsage(row)) }} t</b></span></span>
              <span class="mobile-meta"><strong :title="waterRecordState(row).title">{{ waterRecordState(row).label }}</strong> · 照片 {{ waterNumberText(waterPhotoCount(row)) }} 张</span>
            </button>
          </div>
          <NDataTable v-else-if="visibleRecords.length" class="water-record-table" size="small" :bordered="false" max-height="min(60vh, 720px)" :scroll-x="660"
            :columns="columns" :data="visibleRecords" :row-key="rowKey" :row-props="rowProps" :single-line="true"/>
          <p v-else-if="!recordsError && !recordsStale" class="book-message">{{ emptyText }}</p>
          <footer class="book-footer">
            <span>{{ loadedQueryLabel }} · 共 {{ total }} 条</span>
            <NPagination :page="page" :page-count="totalPages" :page-slot="mobile ? 3 : 5" :disabled="recordsStale && !recordsError" @update:page="changePage"/>
          </footer>
        </div>
        <div v-else-if="!recordsError" class="book-message" role="status">
          <strong>{{ bootstrap.snapshot?.exists===false ? '等待首次数据同步' : filterError ? '请检查筛选条件' : '正在读取水耗记录' }}</strong>
          <button v-if="bootstrap.snapshot?.exists===false && !refreshing" class="tool-button" type="button" @click="refreshData">同步数据</button>
        </div>
      </section>

      <section v-if="editorOpen && !mobile" class="reading-stage" aria-label="水表读数办理">
        <div class="reading-toolbar">
          <button type="button" class="tool-button" data-v2-navigation @click="leaveEditing(closeEditor)"><ArrowLeft :size="15" aria-hidden="true"/>返回记录</button>
          <span>{{ buildingLabel }} · {{ recordId ? '记录详情' : '新增读数' }}</span>
          <div v-if="editorPosition >= 0" class="reading-pager">
            <button class="icon-button" type="button" aria-label="上一条记录" :disabled="editorPosition===0 || busy || recordsStale" @click="stepRecord(-1)"><ArrowLeft :size="16"/></button>
            <small>{{ editorPosition+1 }} / {{ visibleRecords.length }}</small>
            <button class="icon-button" type="button" aria-label="下一条记录" :disabled="editorPosition+1 >= visibleRecords.length || busy || recordsStale" @click="stepRecord(1)"><ArrowRight :size="16"/></button>
          </div>
        </div>
        <div class="reading-body">
          <div class="reading-form"><WaterRecordEditor ref="editor" v-bind="editorProps" v-on="editorEvents"/></div>
          <section v-if="visibleRecords.length && !recordsStale" class="reading-queue" aria-label="切换本页记录">
            <h3>同页记录 <small>{{ visibleRecords.length }} 条</small></h3>
            <div class="reading-items">
              <button v-for="row in visibleRecords" :key="row.record_id" type="button" data-v2-navigation
                :aria-current="row.record_id===recordId ? 'true' : undefined" :disabled="busy" @click="openRecord(row.record_id)">
                <strong>{{ meterLabel(row) }}</strong>
                <span>{{ row.statistic_date || '—' }} · {{ row.frequency || '—' }} / {{ row.shift || '—' }}</span>
                <span>{{ waterRecordState(row).label }}</span>
              </button>
            </div>
          </section>
        </div>
      </section>
    </template>

    <NDrawer class="water-mobile-drawer" :show="mobile && editorOpen" placement="bottom" height="100dvh" :close-on-esc="false" :mask-closable="false"
      :content-style="{padding: '0', height:'100%', overflow:'hidden'}" @update:show="drawerVisibility">
      <div v-if="mobile && editorOpen" class="water-phone-stage"><WaterRecordEditor ref="mobileEditor" v-bind="editorProps" v-on="editorEvents"/></div>
    </NDrawer>
    <WaterDeskAbnormal :show="abnormalOpen" v-model:note="abnormalNote" :message="abnormalMessage" :error="abnormalError"
      :saving="saving" :previous-date="abnormalDetails.previous_date || '—'" :old-reading="waterNumberText(abnormalDetails.old_value)"
      :new-reading="waterNumberText(abnormalDetails.new_value)" :change="abnormalDetails.baseline_zero ? '上期读数为零' : ratio(abnormalDetails.ratio)"
      @cancel="abnormalVisibility(false)" @confirm="saveWithNote"/>
    <NModal :show="!!lightbox.url" @update:show="lightboxVisibility">
      <section class="water-photo-dialog" role="dialog" aria-modal="true" aria-label="水表照片预览">
        <header><strong>{{ lightbox.name || '水表照片' }}</strong>
          <a :href="lightbox.url" :download="lightbox.name || '水表照片'" aria-label="下载原图"><Download :size="18"/>下载</a>
          <button class="icon-button" type="button" aria-label="关闭照片预览" @click="lightboxVisibility(false)"><X :size="18"/></button>
        </header>
        <a v-if="lightbox.url" :href="lightbox.url" target="_blank" rel="noopener" aria-label="打开原图"><img :src="lightbox.url" :alt="lightbox.name || '现场水表照片'"></a>
      </section>
    </NModal>
  </div>
</template>

<style scoped>
.water-desk {
  --water-face: var(--soft-face, #f5f7f8);
  --water-field: var(--soft-field, #eaf0f2);
  --water-ink: var(--v2-ink, #203b46);
  --water-muted: color-mix(in srgb, var(--water-ink) 76%, var(--water-face));
  --water-line: color-mix(in srgb, var(--water-ink) 15%, var(--water-face));
  --water-wash: linear-gradient(106deg, color-mix(in srgb, #b6dbf0 35%, var(--water-face)), color-mix(in srgb, #a9e0c7 32%, var(--water-face)));
  display: grid;
  min-width: 0;
  align-content: start;
  gap: 14px;
  color: var(--water-ink);
  line-height: 1.45;
}
.water-desk :is(button, input, select),.water-photo-dialog button { font-family: inherit; }
.water-desk button:focus-visible,.water-desk select:focus-visible,.water-desk input:focus-visible,.water-photo-dialog :focus-visible { outline: 2px solid var(--v2-blue,#397fa3); outline-offset: 2px; }
.water-desk button:disabled { cursor: not-allowed; opacity: .58; }
.desk-header,.desk-identity,.desk-views,.context-actions,.filter-tools,.range-picker,.field-row,.queue-heading,.queue-options,.book-footer,.reading-toolbar,.reading-pager,.desk-notice,.water-photo-dialog header { display: flex; align-items: center; gap: 8px; }
.desk-header { justify-content: space-between; flex-wrap: wrap; gap: 14px; padding: 4px 0; }
.desk-identity { gap: 12px; min-width: 0; }
.desk-symbol { display: grid; place-items: center; width: 44px; height: 44px; border: 1px solid var(--water-line); border-radius: 14px; background: var(--water-wash); color: var(--v2-blue,#296d91); box-shadow: var(--soft-raised, 2px 3px 7px #27485913); }
.desk-header h1,.filter-tools h2 { color: inherit; margin: 0; font-weight: 680; line-height: 1.25; }
.desk-header h1 { font-size: clamp(1.3rem, 2.1vw, 1.7rem); }
.desk-header p,.context-building small,.origin-note,.queue-label,.book-footer,.mobile-meta,.reading-toolbar > span { color: var(--water-muted); font-size: .81rem; }
.desk-header p { margin: 3px 0 0; }
.desk-views { gap: 3px; padding: 4px; border-radius: 13px; background: var(--water-field); box-shadow: var(--soft-inset, inset 1px 2px 5px #36485319); }
.desk-views button,.range-picker button,.queue-options button,.tool-button,.primary-button,.icon-button,.text-button { display: inline-flex; justify-content: center; align-items: center; gap: 6px; min-height: 34px; padding: 6px 11px; font: inherit; font-size: .86rem; line-height: 1.25; cursor: pointer; color: inherit; background: transparent; border: 1px solid transparent; border-radius: 9px; }
.desk-views button[aria-pressed="true"] { background: var(--water-face); box-shadow: 1px 2px 5px #263e4b22; font-weight: 650; }
.tool-button,.icon-button { background: var(--water-face,#f5f7f8); border-color: var(--water-line,#ced9d9); }
.tool-button:not(:disabled):hover,.icon-button:not(:disabled):hover,.range-picker button:not([aria-pressed="true"]):hover,.queue-options button:not([aria-pressed="true"]):hover { background: var(--water-field,#eaf0f2); }
.icon-button { padding: 6px; min-width: 34px; }
.text-button { color: var(--v2-blue,#28688f); text-decoration: underline; text-underline-offset: 3px; }
.primary-button { color: #fff; background: #137351; border-color: #126346; font-weight: 650; }
.primary-button:hover { background: #105f43; }
.desk-notice,.book-message,.context-error { overflow-wrap: anywhere; }
.desk-notice { border-inline-start: 3px solid #358b69; background: var(--water-field); border-radius: 8px; padding: 7px 10px; }
.desk-notice.caution { border-color: #bb793a; }
.desk-notice .icon-button { margin-inline-start: auto; }
.record-context,.reading-stage,.record-book { min-width: 0; border: 1px solid var(--water-line); border-radius: 16px; background: var(--water-face); box-shadow: var(--soft-raised, 3px 4px 12px #273f5710); }
.record-context { display: flex; gap: 14px 24px; align-items: center; flex-wrap: wrap; padding: 14px 18px; background: var(--water-wash); }
.context-building { display: grid; gap: 4px; }
.context-building label { display: flex; align-items: center; gap: 8px; font-size: .84rem; font-weight: 600; }
.context-building select,.field-row :is(input,select) { min-height: 36px; max-width: 100%; min-width: 0; border: 1px solid var(--water-line); border-radius: 8px; font: inherit; color: var(--water-ink); background: var(--water-face); padding: 5px 8px; box-sizing: border-box; }
.context-numbers { display: flex; align-items: center; gap: clamp(14px, 2.5vw, 42px); margin: 0; }
.context-numbers dt { color: var(--water-muted); font-size: .78rem; }
.context-numbers dd { margin: 2px 0 0; font-weight: 650; font-size: 1.27rem; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.context-numbers small { font-size: .72rem; }
.context-actions { margin-inline-start: auto; flex-wrap: wrap; }
.context-error { flex-basis: 100%; }
.book-filters { padding: 16px 18px 12px; display: grid; gap: 12px; }
.filter-tools { flex-wrap: wrap; }
.filter-tools h2 { font-size: 1.09rem; margin-inline-end: auto; }
.water-search { display: flex; gap: 6px; align-items: center; height: 36px; width: min(100%, 245px); padding: 0 9px; border: 1px solid var(--water-line); border-radius: 6px; background: var(--water-field); box-sizing: border-box; }
.water-search:focus-within { outline: 2px solid var(--v2-blue,#397fa3); }
.water-search input { flex: 1; min-width: 0; width: 100%; border: 0; background: transparent; color: inherit; font: inherit; font-size: .85rem; }
.water-search input:focus-visible { outline: none; }
.range-picker,.queue-options,.queue-heading { flex-wrap: wrap; }
.range-picker { gap: 5px; }
.range-picker button { border-color: var(--water-line); }
.range-picker button[aria-pressed="true"],.queue-options button[aria-pressed="true"],.tool-button[aria-expanded="true"] { background: var(--v2-palette-selection,var(--water-field)); border-color: var(--v2-blue,#4781a2); font-weight: 650; }
.field-row { align-items: end; flex-wrap: wrap; }
.field-row label { display: grid; gap: 4px; flex: 0 1 175px; max-width: 100%; min-width: 0; font-size: .82rem; }
.field-row select,.field-row input { width: 100%; }
.origin-note,.inline-error { margin: 0; }
.inline-error { color: var(--water-ink); padding-inline-start: 8px; border-inline-start: 3px solid #c17938; }
.queue-heading { padding: 10px 18px; border-top: 1px solid var(--water-line); gap: 8px 14px; }
.queue-options { gap: 4px; }
.queue-options b { font-variant-numeric: tabular-nums; }
.book-results,.book-trend { min-width: 0; }
.book-trend { padding: 0 16px 12px; }
.book-message { display: flex; flex-wrap: wrap; align-items: center; justify-content: center; gap: 10px; margin: 0; padding: 16px; }
.water-stale,.water-read-error { background: color-mix(in srgb,#e0b276 15%,var(--water-face)); }
.book-footer { justify-content: space-between; flex-wrap: wrap; padding: 12px 18px; }
.water-record-table :deep(.water-meter-link) { max-width: 100%; height: auto; white-space: normal; text-align: left; line-height: 1.35; }
.water-record-table :deep(.water-meter-link .n-button__content) { white-space: normal; overflow-wrap: anywhere; display: block; }
.water-record-table :deep(.n-data-table-td) { font-variant-numeric: tabular-nums; padding: 9px 8px; }
.water-record-table :deep(.n-data-table-tr) { cursor: pointer; }
.water-record-table :deep(.water-row-info td),.water-mobile-record.water-row-info { background-color: color-mix(in srgb, #a3cbe2 18%, var(--water-face)); }
.water-record-table :deep(.water-row-error td),.water-mobile-record.water-row-error { background-color: color-mix(in srgb, #de9a92 19%, var(--water-face)); }
.water-record-table :deep(.water-row-warning td),.water-mobile-record.water-row-warning { background-color: color-mix(in srgb, #deb879 15%, var(--water-face)); }
.reading-toolbar { padding: 10px 14px; background: var(--water-wash); border-radius: 16px 16px 0 0; }
.reading-pager { margin-inline-start: auto; }
.reading-body { display: grid; grid-template-columns: minmax(0, 1fr) auto; padding: 12px; gap: 12px; }
.reading-form { min-width: 0; }
.reading-form :deep(.water-editor) { width: 100%; max-width: 100%; min-height: 0; box-sizing: border-box; }
.reading-queue { width: clamp(174px, 16vw, 245px); box-sizing: border-box; min-width: 0; align-self: start; padding: 12px; background: var(--water-field); border-radius: 12px; }
.reading-queue h3 { display: flex; align-items: baseline; justify-content: space-between; gap: 6px; margin: 0 0 8px; font-size: .94rem; }
.reading-queue small,.reading-items span { color: var(--water-muted); font-size: .78rem; font-weight: 400; }
.reading-items { display: grid; gap: 4px; max-height: 55vh; overflow: auto; }
.reading-items button { display: grid; gap: 3px; min-width: 0; padding: 8px; border-radius: 8px; border: 1px solid transparent; background: transparent; color: inherit; text-align: left; font: inherit; cursor: pointer; overflow-wrap: anywhere; }
.reading-items button[aria-current="true"],.reading-items button:hover { background: var(--water-face); border-color: var(--water-line); }
.reading-stage { container-type: inline-size; }
@container (max-width: 820px) {
  .reading-body { grid-template-columns: minmax(0,1fr); }
  .reading-queue { width: 100%; }
  .reading-items { max-height: 180px; grid-template-columns: repeat(auto-fit,minmax(min(180px,100%),1fr)); }
}
:global(body .water-mobile-drawer[role="dialog"]) { max-width: 100vw !important; max-height: 100dvh !important; }
.water-phone-stage { width: 100%; height: 100%; min-height: 0; overflow: hidden; }
.water-phone-stage > :deep(.water-editor) { width: 100%; max-width: none; height: 100%; max-height: 100%; box-sizing: border-box; margin: 0; border-radius: 0; }
.water-photo-dialog { width: min(840px, calc(100vw - 24px)); max-height: 93dvh; overflow: auto; padding: 12px; border-radius: 14px; color: var(--v2-ink,#203b46); background: var(--soft-face,#f5f7f8); box-sizing: border-box; }
.water-photo-dialog header { margin-bottom: 10px; }
.water-photo-dialog header strong { min-width: 0; margin-inline-end: auto; overflow-wrap: anywhere; }
.water-photo-dialog header a { display: inline-flex; align-items: center; gap: 4px; flex-shrink: 0; color: inherit; }
.water-photo-dialog img { display: block; max-width: 100%; max-height: 76dvh; margin: auto; object-fit: contain; }
.mobile-list { display: grid; gap: 8px; padding: 0 10px; }
.water-mobile-record { display: grid; gap: 4px; padding: 11px 13px; border: 1px solid var(--water-line); border-radius: 11px; background: var(--water-field); color: inherit; font: inherit; text-align: left; cursor: pointer; }
.mobile-title,.mobile-numbers { display: flex; align-items: center; gap: 6px 16px; }
.mobile-title { justify-content: space-between; overflow-wrap: anywhere; }
.mobile-title svg { flex-shrink: 0; }
.mobile-numbers { flex-wrap: wrap; font-size: .86rem; font-variant-numeric: tabular-nums; }
@media (max-width: 760px) {
  .water-desk { gap: 10px; }
  .desk-views button,.range-picker button,.queue-options button,.tool-button,.primary-button,.icon-button,.field-row :is(input,select),.context-building select { min-height: 44px; }
  .desk-views,.range-picker,.queue-options { gap: 8px; }
  .desk-header { align-items: start; }
  .context-building,.context-numbers { width: 100%; }
  .context-actions { margin: 0; }
  .record-context,.book-filters { padding: 12px; }
  .context-numbers { justify-content: space-between; gap: 8px; }
  .filter-tools h2 { width: 100%; }
  .water-search { flex: 1 1 130px; width: auto; }
  .field-row label { flex: 1 1 130px; }
  .queue-heading { padding: 10px 12px; }
  .range-picker button,.queue-options button { padding-inline: 8px; }
  .book-footer { padding: 12px; }
}
@media (max-width: 699px) { .water-search { height: 44px; } }
@media (max-width: 450px) {
  .desk-header,.record-context { gap: 10px; }
  .desk-views,.context-actions,.filter-tools .water-search { width: 100%; }
  .desk-views button { flex: 1; }
  .queue-heading { display: grid; }
  .range-picker,.queue-options { justify-content: flex-start; }
}
</style>
