<template>
  <section class="v2-water" aria-label="水耗管理">
    <header class="water-toolbar">
      <div class="water-title"><NButton data-v2-navigation quaternary circle aria-label="返回工作台" @click="navigate('/')"><ArrowLeft :size="19" /></NButton><h1>水耗管理</h1><NSelect :value="scope" :options="availableScopes" :disabled="busy" class="water-scope" aria-label="水耗楼栋" @update:value="switchScope" /></div>
      <div class="water-toolbar-actions"><span class="water-updated">{{ snapshotText }}</span><NButton :loading="refreshing" :disabled="refreshing || busy" @click="refreshData"><RefreshCw :size="15" />刷新数据</NButton><NButton v-if="canCreate" data-v2-navigation type="primary" :disabled="busy" @click="leaveEditing(openCreate)"><Plus :size="16" />新增录入</NButton></div>
    </header>
    <div class="water-month-summary" :aria-busy="bootstrapLoading"><span>{{ bootstrap.month || waterDay().slice(0,7) }} · 本月</span><span>记录 <strong>{{ waterNumberText(summary.record_count,0) }}</strong></span><span>耗水量 <strong>{{ waterNumberText(summary.total_usage,2) }}</strong> t</span><span>有照片 <strong>{{ waterNumberText(summary.photo_records,0) }}</strong></span><NButton v-if="bootstrapError" data-v2-navigation text type="error" @click="loadBootstrap(true)">基础数据读取失败，重试</NButton></div>
    <div v-if="notice.text" class="water-notice" :class="'water-tone-'+notice.tone" aria-live="polite"><CheckCircle2 v-if="notice.tone==='success'" :size="17" /><CircleAlert v-else :size="17" /><span>{{ notice.text }}</span><NButton data-v2-navigation quaternary circle aria-label="收起水耗提示" @click="notice.text=''"><X :size="14" /></NButton></div>
    <div class="water-work-area" :class="{'has-editor':editorOpen&&!mobile}">
      <section class="water-records-area" aria-label="水耗记录">
        <div class="water-filter-main">
          <NRadioGroup v-model:value="filters.range" size="small" aria-label="水耗日期范围"><NRadioButton v-for="range in ranges" :key="range" :value="range">{{ range }}</NRadioButton></NRadioGroup>
          <NButton data-v2-navigation secondary size="small" :type="advancedOpen || advancedCount ? 'primary' : 'default'" :aria-expanded="advancedOpen" @click="advancedOpen=!advancedOpen"><SlidersHorizontal :size="15" />筛选{{ advancedCount ? ` · ${advancedCount}` : '' }}</NButton>
          <NButton data-v2-navigation quaternary size="small" :aria-expanded="trendOpen" @click="trendOpen=!trendOpen"><ChartNoAxesCombined :size="16" />趋势</NButton>
        </div>
        <div v-if="filters.range==='本月' || filters.range==='自定义日期'" class="water-date-filter">
          <label v-if="filters.range==='本月'" class="water-date-label"><span class="water-sr-only">筛选月份</span><NDatePicker v-model:formatted-value="filters.month" type="month" value-format="yyyy-MM" clearable /></label>
          <template v-else><label class="water-date-label"><span class="water-sr-only">筛选开始日期</span><NDatePicker v-model:formatted-value="filters.startDate" type="date" value-format="yyyy-MM-dd" clearable /></label><span>至</span><label class="water-date-label"><span class="water-sr-only">筛选结束日期</span><NDatePicker v-model:formatted-value="filters.endDate" type="date" value-format="yyyy-MM-dd" clearable /></label></template>
        </div>
        <NInput v-model:value="filters.query" clearable class="water-search" placeholder="搜索水表、日期或描述" :input-props="{'aria-label':'搜索水耗记录'}"><template #prefix><Search :size="16" /></template></NInput>
        <Transition name="water-filter">
          <div v-if="advancedOpen" class="water-filter-extra">
            <label><span>水表</span><NSelect v-model:value="filters.meter" :options="filterOptions('meters','全部水表')" filterable aria-label="筛选水表" /></label>
            <label><span>统计频次</span><NSelect v-model:value="filters.frequency" :options="filterOptions('frequencies','全部频次')" aria-label="筛选统计频次" /></label>
            <label><span>班次</span><NSelect v-model:value="filters.shift" :options="filterOptions('shifts','全部班次')" aria-label="筛选班次" /></label>
            <NButton data-v2-navigation text @click="Object.assign(filters,{meter:'',frequency:'',shift:'',query:''})">清除筛选</NButton>
          </div>
        </Transition>
        <p v-if="filterError" class="water-read-error" aria-live="polite">{{ filterError }}</p>
        <div v-if="recordsError" class="water-read-error" aria-live="polite"><span>{{ recordsError }}{{ recordsReady?'；以下保留上次读取的数据。':'' }}</span><NButton @click="loadRecords(true)">重试读取</NButton></div>
        <p v-if="recordsStale && recordsReady" class="water-stale">显示上次查询：{{ loadedQueryLabel }}。当前筛选尚未取得新结果。</p>
        <div class="water-list-head"><span>{{ !recordsReady ? '记录列表' : `共 ${total} 条` }}<template v-if="recordsReady"> · 第 {{ page }} / {{ totalPages }} 页</template></span><NSelect v-model:value="attention" :options="attentionOptions" size="small" class="water-attention" aria-label="本页记录状态" /></div>
        <div v-if="trendOpen" class="water-trend"><MotionChart title="本页日统计水耗" :caption="`${loadedQueryLabel} · ${records.length} 条记录${trend.missing?' · 部分耗水量未取得':''}`" :items="trend.items" :loading="recordsLoading" :failed="recordsStale || !!recordsError || trend.missing>0" trend view="bar" unit="t" @select="drillDay" /><p>按本页日统计记录汇总；修正值优先，不包含周、月统计。</p></div>
        <div class="water-list-content" :aria-busy="recordsLoading">
          <div v-if="!recordsReady && recordsLoading" class="water-initial"><NSkeleton v-for="i in 7" :key="i" height="40px" /></div>
          <div v-else-if="!recordsReady" class="water-initial water-empty"><CircleAlert :size="30" /><strong>{{ !scope ? '当前范围未接入水耗管理' : recordsError ? '水耗记录暂不可用' : filterError ? '请完善日期筛选' : '等待首次数据同步' }}</strong><span>{{ !scope ? '请选择已授权的 A、B、C、D、E、H 楼。' : '尚未取得完整数据。' }}</span></div>
          <template v-else>
            <div v-if="recordsLoading" class="water-list-updating"><NSpin :size="13" />正在更新记录</div>
            <NDataTable v-if="!mobile" :columns="columns" :data="visibleRecords" :row-key="rowKey" :row-props="rowProps" :scroll-x="660" :bordered="false" :single-line="false" size="small" :max-height="tableHeight" />
            <div v-else class="water-mobile-records"><NEmpty v-if="!visibleRecords.length" :description="emptyText" /><button v-for="row in visibleRecords" :key="row.record_id" type="button" data-v2-navigation class="water-mobile-record" :class="{selected:row.record_id===recordId}" :disabled="busy || recordsStale" @click="openRecord(row.record_id)"><div><strong>{{ row.meter || '水表未填写' }}</strong><NTag :type="waterRecordState(row).type" size="small" :bordered="false">{{ waterRecordState(row).label }}</NTag></div><span>{{ row.statistic_date || '日期未填写' }} · {{ row.frequency || '—' }} · {{ row.shift || '—' }}班</span><dl><div><dt>读数</dt><dd>{{ waterNumberText(row.meter_value) }}</dd></div><div><dt>耗水量</dt><dd>{{ waterNumberText(row.corrected_usage ?? row.computed_usage) }} <small>t</small></dd></div><div><dt>照片</dt><dd>{{ waterPhotoCount(row) ?? '—' }} <small>张</small></dd></div></dl></button></div>
          </template>
        </div>
        <footer class="water-list-footer"><span>{{ attention ? `本页 ${visibleRecords.length} 条符合条件` : recordsReady ? `${visibleRecords.length} 条记录` : '尚未取得记录数量' }}</span><NPagination v-if="recordsReady" :page="page" :page-count="totalPages" :page-slot="mobile?3:5" :disabled="recordsLoading || !!filterError" size="small" show-quick-jumper @update:page="changePage" /></footer>
      </section>
      <WaterRecordEditor v-if="editorOpen&&!mobile" ref="editor" v-bind="editorProps" @close="leaveEditing(closeEditor)" @reload="reloadEditor" @save="saveRecord()" @retry-save="retrySave" @change="fieldChanged" @upload="uploadFiles" @retry-photo="retryPhoto" @remove-photo="removePhoto" @remove-retained="removeRetained" @preview="previewPhoto" />
    </div>
    <NDrawer class="water-mobile-drawer" :show="editorOpen&&mobile" placement="right" :width="'100%'" :mask-closable="false" :close-on-esc="false" @update:show="drawerVisibility"><NDrawerContent :body-content-style="{padding:0}"><WaterRecordEditor ref="mobileEditor" v-bind="editorProps" @close="leaveEditing(closeEditor)" @reload="reloadEditor" @save="saveRecord()" @retry-save="retrySave" @change="fieldChanged" @upload="uploadFiles" @retry-photo="retryPhoto" @remove-photo="removePhoto" @remove-retained="removeRetained" @preview="previewPhoto" /></NDrawerContent></NDrawer>
    <NModal :show="abnormalOpen" :mask-closable="!saving" :close-on-esc="!saving" @update:show="abnormalVisibility"><NCard class="water-abnormal" title="填写异常备注" :bordered="false" role="dialog" aria-modal="true"><p>{{ abnormalMessage }}</p><dl class="water-abnormal-values"><div><dt>上期日期</dt><dd>{{ abnormalDetails.previous_date || '—' }}</dd></div><div><dt>上期读数</dt><dd>{{ waterNumberText(abnormalDetails.old_value) }}</dd></div><div><dt>本次读数</dt><dd>{{ waterNumberText(abnormalDetails.new_value) }}</dd></div><div><dt>变化率</dt><dd>{{ abnormalDetails.baseline_zero ? '上期为零，需核查' : ratio(abnormalDetails.ratio) }}</dd></div></dl><NFormItem label="异常原因或现场核查情况" required :validation-status="abnormalError?'error':undefined" :feedback="abnormalError"><NInput v-model:value="abnormalNote" type="textarea" autofocus :autosize="{minRows:3,maxRows:6}" :maxlength="1000" show-count :disabled="saving" :input-props="{'aria-label':'异常备注'}" @update:value="abnormalError=''" /></NFormItem><template #footer><div class="water-modal-actions"><NButton data-v2-navigation :disabled="saving" @click="abnormalOpen=false">继续核对</NButton><NButton type="primary" :loading="saving" :disabled="saving" @click="saveWithNote">确认并保存</NButton></div></template></NCard></NModal>
    <NModal :show="!!lightbox.url" :close-on-esc="true" @update:show="lightboxVisibility"><section class="water-lightbox" role="dialog" aria-modal="true" aria-label="水表照片预览"><header><strong>{{ lightbox.name }}</strong><NButton data-v2-navigation quaternary circle aria-label="关闭照片预览" @click="lightbox.url=''"><X :size="20" /></NButton></header><img :src="lightbox.url" :alt="lightbox.name" /><footer><NButton tag="a" data-v2-navigation :href="lightbox.url" :download="lightbox.name"><Download :size="16" />下载照片</NButton></footer></section></NModal>
  </section>
</template>
<script setup lang="ts">
import { computed, defineAsyncComponent, h, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { NButton, NCard, NDataTable, NDatePicker, NDrawer, NDrawerContent, NEmpty, NFormItem, NInput, NModal, NPagination, NRadioButton, NRadioGroup, NSelect, NSkeleton, NSpin, NTag, useDialog, type DataTableColumns } from 'naive-ui';
import { ArrowLeft, ChartNoAxesCombined, CheckCircle2, CircleAlert, Download, Plus, RefreshCw, Search, SlidersHorizontal, X } from 'lucide-vue-next';
import { ApiError, requestBinaryJson, requestJson, type Dict } from './client';
import { usePageReadRefresh } from '../api/usePageReadRefresh';
import { navigate, registerNavigationGuard } from './navigation';
import { isAssistantEvent } from '../modalState';
import { trackButtonRequest } from './actionMotion';
import WaterRecordEditor from './WaterRecordEditor.vue';
import { validWaterDate, waterDay, waterImageMime, waterNumber, waterNumberText, waterPhotoCount, waterQuery, waterRecordState, waterScopes, waterTrend, type WaterDraft, type WaterFilters, type WaterPhoto } from './water';
const MotionChart=defineAsyncComponent(()=>import('./MotionChart.vue'));
const props=defineProps<{scope:string;scopeOptions:{value:string;label:string}[]}>();
const emit=defineEmits<{'switch-scope':[scope:string];status:[message:string]}>();
const dialog=useDialog();
const scope=computed(()=>waterScopes.has(props.scope)?props.scope:'');
const availableScopes=computed(()=>props.scopeOptions.filter(item=>waterScopes.has(item.value)));
const bootstrap=ref<Dict>({}),bootstrapLoading=ref(false),bootstrapError=ref(''),refreshing=ref(false);
const summary=computed(()=>bootstrap.value.snapshot?.exists===false?{}:bootstrap.value.summary||{}),canCreate=computed(()=>bootstrap.value.permissions?.can_create===true);
const records=ref<Dict[]>([]),recordsReady=ref(false),recordsLoading=ref(false),recordsError=ref(''),filterError=ref(''),total=ref(0),page=ref(1),loadedQuery=ref(''),loadedQueryLabel=ref(''),queryDay=ref(waterDay());
const ranges=['近3天','本月','全部时间','自定义日期'];
const filters=reactive<WaterFilters>({range:'近3天',month:waterDay().slice(0,7),startDate:waterDay(-2),endDate:waterDay(),meter:'',frequency:'',shift:'',query:''});
const advancedOpen=ref(false),trendOpen=ref(false),attention=ref('');
const attentionOptions=[{label:'本页全部记录',value:''},{label:'本页待补读数',value:'待补读数'},{label:'本页待补照片',value:'待补照片'}];
const advancedCount=computed(()=>[filters.meter,filters.frequency,filters.shift].filter(Boolean).length);
const totalPages=computed(()=>Math.max(1,Math.ceil(total.value/50)));
const querySignature=computed(()=>{try{return waterQuery(scope.value,filters,page.value,queryDay.value).toString();}catch{return '';}});
const recordsStale=computed(()=>loadedQuery.value!==querySignature.value);
const visibleRecords=computed(()=>attention.value?records.value.filter(row=>waterRecordState(row).label===attention.value):records.value);
const trend=computed(()=>waterTrend(records.value));
const emptyText=computed(()=>attention.value?'本页没有符合该状态的记录':'当前条件下暂无水耗记录');
const notice=reactive({text:'',tone:'success'});
const media=matchMedia('(max-width:760px)'),mobile=ref(media.matches),height=ref(innerHeight);
const tableHeight=computed(()=>Math.max(260,height.value-(advancedOpen.value?365:285)-(trendOpen.value?285:0)));
const editorOpen=ref(false),recordId=ref(''),detail=ref<Dict>({}),editorLoading=ref(false),saving=ref(false),uploading=ref(false),uncertain=ref(false),editorError=ref('');
const form=reactive<WaterDraft>({meter:'',frequency:'',shift:'',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});
const retained=ref<Dict[]>([]),photos=ref<WaterPhoto[]>([]),errors=reactive<Record<string,string>>({}),baseline=ref('');
const editor=ref<InstanceType<typeof WaterRecordEditor>>(),mobileEditor=ref<InstanceType<typeof WaterRecordEditor>>();
const admin=computed(()=>bootstrap.value.permissions?.is_admin===true||detail.value.edit_policy?.is_admin===true);
const locked=computed(()=>!!recordId.value&&!admin.value&&detail.value.edit_policy?.can_edit!==true);
const draftStamp=()=>JSON.stringify([form,retained.value.map(photo=>photo.image_id),photos.value.map(photo=>[photo.localId,photo.uploadId])]);
const dirty=computed(()=>editorOpen.value&&baseline.value!==draftStamp());
const busy=computed(()=>saving.value||uploading.value);
const editorProps=computed(()=>({scope:scope.value,recordId:recordId.value,form,detail:detail.value,options:bootstrap.value.options||{},retained:retained.value,photos:photos.value,errors,error:editorError.value,loading:editorLoading.value,saving:saving.value,uploading:uploading.value,locked:locked.value,admin:admin.value,dirty:dirty.value,uncertain:uncertain.value}));
const abnormalOpen=ref(false),abnormalNote=ref(''),abnormalError=ref(''),abnormalDetails=ref<Dict>({}),abnormalMessage=ref('');
const lightbox=reactive({url:'',name:''});
const snapshotText=computed(()=>{const s=bootstrap.value.snapshot||{};if(refreshing.value)return '正在同步数据';if(bootstrapError.value)return '同步状态暂不可用';const at=waterNumber(s.refreshed_at);return at?'更新于 '+new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}).format(new Date(at*1000)):'等待首次同步';});
const ratio=(v:unknown)=>waterNumber(v)===null?'—':waterNumberText(Number(v)*100,1)+'%';
function filterOptions(key:string,label:string){return [{label,value:''},...(bootstrap.value.options?.[key]||[]).map((value:string)=>({label:value,value}))];}
const columns:DataTableColumns<Dict>=[
  {title:'日期',key:'statistic_date',width:100,render:row=>row.statistic_date||'—'},
  {title:'水表',key:'meter',minWidth:130,render:row=>h(NButton,{text:true,'data-v2-navigation':'',disabled:busy.value||recordsStale.value,'aria-label':'填写'+(row.meter||'水表')+row.statistic_date,onClick:()=>openRecord(row.record_id)},{default:()=>row.meter||'未填写'})},
  {title:'频次 / 班次',key:'frequency',width:90,render:row=>`${row.frequency||'—'} / ${row.shift||'—'}`},
  {title:'水表读数',key:'meter_value',width:100,align:'right',render:row=>waterNumberText(row.meter_value)},
  {title:'耗水量 / t',key:'usage',width:95,align:'right',render:row=>waterNumberText(row.corrected_usage??row.computed_usage)},
  {title:'状态',key:'status',width:100,render:row=>h(NTag,{size:'small',bordered:false,type:waterRecordState(row).type},{default:()=>waterRecordState(row).label})},
  {title:'照片',key:'photo_count',width:60,align:'right',render:row=>waterPhotoCount(row)===null?'—':waterPhotoCount(row)+'张'},
];
function rowProps(row:Dict){return {class:row.record_id===recordId.value?'water-row-selected':'',onClick:(event:MouseEvent)=>{if(!(event.target as Element)?.closest('button,a')&&!busy.value&&!recordsStale.value)void openRecord(row.record_id);}};}
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
  if(!scope.value||disposed)return;bootstrapRead?.abort();const c=new AbortController();bootstrapRead=c;const generation=++bootstrapGeneration;bootstrapLoading.value=true;
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
  listRead?.abort();const generation=++listGeneration;if(!scope.value||disposed)return;queryDay.value=waterDay();
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
function switchScope(value:string){if(waterScopes.has(value)&&value!==scope.value)leaveEditing(()=>{closeEditor();emit('switch-scope',value);});}
function clearPhotos(){for(const photo of photos.value)URL.revokeObjectURL(photo.previewUrl);photos.value=[];}
function resetEditor(){clearPhotos();retained.value=[];detail.value={};editorError.value='';for(const key of Object.keys(errors))delete errors[key];operation='';pendingSave=null;uncertain.value=false;abnormalOpen.value=false;abnormalNote.value='';Object.assign(form,{meter:'',frequency:'',shift:'',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});baseline.value=draftStamp();}
function closeEditor(){detailRead?.abort();detailGeneration++;editorOpen.value=false;editorLoading.value=false;recordId.value='';resetEditor();}
function openCreate(){if(!canCreate.value){notice.text='当前账号无新增权限。';notice.tone='warning';return;}closeEditor();notice.text='';Object.assign(form,{meter:'',frequency:bootstrap.value.options?.frequencies?.[0]||'日',shift:bootstrap.value.options?.shifts?.[0]||'白',statisticDate:waterDay(),meterValue:null,correctedUsage:null,expectedVersion:''});editorOpen.value=true;baseline.value=draftStamp();}
async function readRecord(id:string){
  detailRead?.abort();const c=new AbortController();detailRead=c;const generation=++detailGeneration;
  recordId.value=id;resetEditor();notice.text='';editorOpen.value=true;editorLoading.value=true;
  try{const data=await requestJson('/api/capacity/water/records/'+encodeURIComponent(id)+'?'+new URLSearchParams({scope:scope.value}),{signal:c.signal,cache:'no-store'});if(disposed||c.signal.aborted||generation!==detailGeneration)return;
    if(data.record_id!==id||!String(data.version??'').trim()||!Array.isArray(data.photos))throw new Error('记录或照片信息未完整返回，已阻止填写，请重新读取。');detail.value=data;
    Object.assign(form,{meter:String(data.meter||''),frequency:String(data.frequency||''),shift:String(data.shift||''),statisticDate:String(data.statistic_date||data.statistic_date_key||''),meterValue:waterNumber(data.meter_value),correctedUsage:waterNumber(data.corrected_usage),expectedVersion:String(data.version||'')});
    retained.value=Array.isArray(data.photos)?data.photos.map((photo:Dict)=>({...photo})):[];baseline.value=draftStamp();
  }catch(error){if(!c.signal.aborted&&!disposed)editorError.value=signalError(error);}
  finally{if(generation===detailGeneration)editorLoading.value=false;}
}
function openRecord(id:string){if(editorOpen.value&&recordId.value===id)return;leaveEditing(()=>{void readRecord(id);});}
function reloadEditor(){const id=recordId.value;if(id)leaveEditing(()=>{void readRecord(id);});}
function fieldChanged(field:string){delete errors[field];if(!uncertain.value){operation='';pendingSave=null;}editorError.value='';}
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
usePageReadRefresh(url=>url.pathname.startsWith('/api/capacity/water/')&&url.searchParams.get('scope')===scope.value,async()=>{await Promise.all([loadBootstrap(),loadRecords()]);},()=>!busy.value&&!dirty.value&&!editorLoading.value&&!bootstrapLoading.value&&!recordsLoading.value&&!uncertain.value);
function viewport(){mobile.value=media.matches;height.value=innerHeight;}
function visibility(){clearTimeout(pollTimer);if(!document.hidden){if(queryDay.value!==waterDay())void loadRecords();if(refreshing.value)void loadBootstrap(true);else if(!bootstrap.value.snapshot?.exists&&!bootstrapLoading.value)void loadBootstrap();}}
function unload(event:BeforeUnloadEvent){if(busy.value||dirty.value||uncertain.value){event.preventDefault();event.returnValue='';}}
function escape(event:KeyboardEvent){if(event.key!=='Escape'||event.defaultPrevented||isAssistantEvent(event)||lightbox.url||abnormalOpen.value||(event.target as Element)?.closest('.n-dialog'))return;if(editorOpen.value){event.preventDefault();leaveEditing(closeEditor);}}
onMounted(()=>{void Promise.all([loadBootstrap(),loadRecords()]);media.addEventListener('change',viewport);window.addEventListener('resize',viewport);window.addEventListener('beforeunload',unload);document.addEventListener('visibilitychange',visibility);window.addEventListener('keydown',escape);});
onBeforeUnmount(()=>{disposed=true;bootstrapRead?.abort();listRead?.abort();detailRead?.abort();lifetime.abort();clearTimeout(filterTimer);clearTimeout(pollTimer);clearPhotos();removeGuard();media.removeEventListener('change',viewport);window.removeEventListener('resize',viewport);window.removeEventListener('beforeunload',unload);document.removeEventListener('visibilitychange',visibility);window.removeEventListener('keydown',escape);});
</script>
<style scoped>
.v2-water{padding:14px 0 0;color:var(--v2-ink);min-width:0}.water-toolbar,.water-title,.water-toolbar-actions{display:flex;align-items:center;gap:10px}.water-toolbar{justify-content:space-between;flex-wrap:wrap;margin-bottom:12px}.water-title h1{font-size:20px!important;line-height:1.3;white-space:nowrap;margin:0}.water-scope{width:100px}.water-updated{font-size:11px;color:var(--v2-muted)}.water-month-summary{display:flex;gap:18px;align-items:center;flex-wrap:wrap;color:var(--v2-muted);font-size:12px;padding:8px 0 15px}.water-month-summary strong{color:var(--v2-ink);font-weight:600;font-variant-numeric:tabular-nums;margin-left:4px}.water-notice{display:flex;align-items:center;gap:8px;min-height:38px;padding:6px 10px;margin-bottom:12px;background:#edf8f3;color:#277457;border-radius:6px;font-size:12px}.water-notice.warning{background:#fff7e8;color:#996720}.water-notice>span{flex:1;line-height:1.6}.water-work-area{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:16px;align-items:stretch;min-width:0}.water-work-area.has-editor{grid-template-columns:minmax(0,1fr) 390px}.water-records-area{min-width:0;background:#fff;border:1px solid var(--v2-border);border-radius:8px;overflow:hidden}.water-filter-main{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:14px 14px 10px}.water-filter-main>.n-radio-group{margin-right:auto}.water-date-filter{display:flex;gap:8px;align-items:center;padding:0 14px 10px}.water-date-filter .n-date-picker{width:160px}.water-date-filter>span{font-size:12px;color:var(--v2-muted)}.water-search{margin:0 14px 12px;width:calc(100% - 28px)}.water-filter-extra{display:grid;grid-template-columns:minmax(130px,1.6fr) minmax(100px,1fr) minmax(90px,1fr) auto;gap:10px;align-items:end;padding:2px 14px 12px}.water-filter-extra>label{min-width:0}.water-filter-extra>label>span{display:block;font-size:11px;color:var(--v2-muted);margin-bottom:5px}.water-read-error{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:0;padding:10px 14px;color:#a93b46;background:#fff3f3;font-size:12px;line-height:1.6}.water-stale{margin:0;padding:8px 14px;color:#976b20;background:#fff8e9;font-size:11px;line-height:1.6}.water-list-head{display:flex;justify-content:space-between;align-items:center;padding:8px 14px;border-block:1px solid var(--v2-border);gap:12px;background:#f9fafb}.water-list-head>span{font-size:11px;color:var(--v2-muted)}.water-attention{width:152px}.water-list-content{position:relative;min-height:300px}.water-list-content :deep(.n-button__content){white-space:normal;overflow-wrap:anywhere;text-align:left}.water-list-content :deep(.n-data-table-td){font-variant-numeric:tabular-nums}.water-list-content :deep(.water-row-selected td){background:#eef5ff!important}.water-list-content :deep(.n-data-table-tr){cursor:pointer}.water-list-updating{position:absolute;right:14px;top:7px;z-index:3;display:flex;align-items:center;gap:7px;padding:5px 9px;border-radius:5px;background:#eff5ff;font-size:11px;color:#376eac}.water-list-footer{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;padding:10px 14px;border-top:1px solid var(--v2-border);font-size:11px;color:var(--v2-muted)}.water-initial{display:grid;gap:8px;padding:14px}.water-empty{align-content:center;justify-items:center;min-height:300px;gap:12px}.water-empty strong{font-size:14px;color:var(--v2-ink)}.water-empty>span{font-size:12px;color:var(--v2-muted)}.water-idle{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:30px;border-left:1px solid var(--v2-border);color:var(--v2-muted);text-align:center}.water-idle h2{font-size:16px;color:var(--v2-ink);margin:14px 0 0}.water-idle p{font-size:12px;line-height:1.8;max-width:220px}.water-work-area>:deep(.water-editor){height:calc(100dvh - 178px);min-height:520px;max-height:850px}.water-trend{padding:14px;border-bottom:1px solid var(--v2-border)}.water-trend p{font-size:11px;color:var(--v2-muted);margin:8px 0 0}.water-abnormal{width:min(520px,calc(100vw - 24px));max-height:calc(100dvh - 40px);overflow:auto}.water-abnormal>p{font-size:13px;line-height:1.6}.water-abnormal-values{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin:16px 0}.water-abnormal-values dt{font-size:11px;color:var(--v2-muted)}.water-abnormal-values dd{margin:5px 0 0;font-size:14px;font-variant-numeric:tabular-nums}.water-modal-actions{display:flex;justify-content:flex-end;gap:8px}.water-lightbox{width:min(960px,calc(100vw - 24px));max-height:90dvh;display:grid;grid-template-rows:auto minmax(0,1fr) auto;background:#fff;border-radius:8px;padding:12px;gap:12px}.water-lightbox header{display:flex;align-items:center;justify-content:space-between;gap:16px}.water-lightbox header strong{font-size:13px;overflow-wrap:anywhere}.water-lightbox img{max-height:calc(90dvh - 120px);max-width:100%;object-fit:contain;justify-self:center}.water-lightbox footer{display:flex;justify-content:flex-end}.water-filter-enter-active,.water-filter-leave-active{transition:opacity 180ms,transform 180ms}.water-filter-enter-from,.water-filter-leave-to{opacity:0;transform:translateY(-6px)}
.water-work-area:not(.has-editor){display:block}
.water-records-area{align-self:start}.water-list-content{min-height:180px}
.water-notice.water-tone-warning{background:#fff7e8;color:#996720}
.water-date-label{display:block;min-width:0}.water-sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
:global(.workspace-v2 .n-drawer.water-mobile-drawer),:global(.workspace-v2 .water-mobile-drawer .n-drawer){max-width:100vw!important;max-height:100dvh!important}
@media(min-width:761px) and (max-width:1280px){.water-work-area.has-editor{grid-template-columns:minmax(0,1fr) 360px}.water-updated{display:none}.water-filter-extra{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:760px){.v2-water{padding:8px 0 0}.water-toolbar{margin-bottom:8px;gap:8px}.water-title{gap:6px}.water-title h1{font-size:17px!important}.water-title>.n-button{width:38px;height:38px}.water-scope{width:86px}.water-toolbar-actions{gap:6px;margin-left:auto}.water-toolbar-actions>.n-button{height:36px;font-size:12px}.water-toolbar-actions>.n-button svg{display:none}.water-updated{display:none}.water-month-summary{gap:8px 12px;padding:4px 0 12px;font-size:11px}.water-month-summary>span:first-child{flex-basis:100%}.water-work-area,.water-work-area.has-editor{display:block}.water-filter-main{padding:10px 10px 8px;gap:6px}.water-filter-main>.n-radio-group{flex-basis:100%;display:flex;margin:0}.water-filter-main :deep(.n-radio-button){flex:1;padding:0 8px;text-align:center;font-size:12px}.water-filter-main>.n-button{height:36px}.water-search{margin:0 10px 10px;width:calc(100% - 20px)}.water-date-filter{padding:0 10px 10px;gap:6px}.water-date-filter .n-date-picker{flex:1;min-width:0;width:auto}.water-filter-extra{grid-template-columns:repeat(2,minmax(0,1fr));padding:0 10px 10px}.water-filter-extra>label:first-child{grid-column:1/-1}.water-filter-extra>.n-button{justify-self:start;height:36px}.water-list-head{padding:8px 10px;gap:8px}.water-list-footer{padding:10px;gap:10px}.water-mobile-record{display:block;width:100%;padding:12px 10px;background:#fff;border:0;border-bottom:1px solid var(--v2-border);cursor:pointer;text-align:left;color:var(--v2-ink)}.water-mobile-record.selected{background:#eef5ff}.water-mobile-record:disabled{cursor:default}.water-mobile-record>div{display:flex;align-items:center;justify-content:space-between;gap:8px}.water-mobile-record strong{font-size:13px;overflow-wrap:anywhere;line-height:1.5}.water-mobile-record>span{display:block;font-size:11px;color:var(--v2-muted);margin-top:6px}.water-mobile-record dl{display:grid;grid-template-columns:1.2fr 1.2fr 1fr;gap:10px;margin:10px 0 0}.water-mobile-record dt{font-size:10px;color:var(--v2-muted)}.water-mobile-record dd{margin:4px 0 0;font-size:15px;font-variant-numeric:tabular-nums}.water-mobile-record small{font-size:10px;color:var(--v2-muted)}.water-mobile-record :deep(.n-tag){flex:none}.water-trend{padding:10px}.water-mobile-records>.n-empty{padding:40px 12px}}
@media(prefers-reduced-motion:reduce){.water-filter-enter-active,.water-filter-leave-active{transition:none}}
@media(max-width:760px){.water-date-filter>.water-date-label{flex:1;min-width:0}.water-date-filter .water-date-label>.n-date-picker{width:100%}}
</style>
