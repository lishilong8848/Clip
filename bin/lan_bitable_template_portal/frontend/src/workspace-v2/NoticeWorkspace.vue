<template>
  <section class="v2-notices" aria-label="通告办理">
    <header class="v2-notice-heading"><div><h1>通告办理</h1><p>{{ scopeLabel }} · {{ month }}计划与当前进行中通告</p></div><div class="v2-notice-tools"><NDropdown :options="moreOptions" trigger="click" @select="more"><NButton data-v2-navigation><Ellipsis :size="17" />更多办理</NButton></NDropdown><NDropdown :options="refreshOptions" trigger="click" @select="refreshSource"><NButton :loading="loading||syncing" data-v2-navigation><RefreshCw :size="16" />刷新</NButton></NDropdown><NDropdown :options="newOptions" trigger="click" @select="newNotice"><NButton data-v2-navigation type="primary"><Plus :size="17" />新建通告</NButton></NDropdown></div></header>
    <nav class="v2-notice-types" aria-label="通告类型"><NButton v-for="entry in types" :key="entry.id" data-v2-navigation :type="kind===entry.id?'primary':'default'" :secondary="kind===entry.id" :aria-pressed="kind===entry.id" @click="setKind(entry.id)">{{ entry.label }}</NButton></nav>
    <form class="v2-notice-filters" :class="{'has-scopes':scopes.length>1}" @submit.prevent="filter"><NInput v-model:value="search" clearable placeholder="搜索名称、楼栋或专业" :input-props="{'aria-label':'搜索通告'}"><template #prefix><Search :size="16" /></template></NInput><NSelect v-if="scopes.length>1" :value="scope" :options="scopes" aria-label="通告楼栋" @update:value="changeScope" /><NSelect v-model:value="month" :options="months" aria-label="计划月份" /><NSelect v-model:value="specialty" clearable :options="specialties" placeholder="全部专业" aria-label="专业筛选" /><NButton attr-type="submit" :loading="loading">筛选</NButton></form>
    <NAlert v-if="error" role="alert" type="error" :bordered="false" class="v2-notice-feedback">{{ error }}<NButton text @click="load(true)">重新读取</NButton></NAlert>
    <NAlert v-if="syncing" type="info" :bordered="false"><NSpin size="small" />正在同步源表，当前仍可查看已加载的通告。</NAlert>
    <NTabs v-model:value="list" class="v2-notice-mobile-tabs" type="line" animated><NTab name="planned" role="tab" :aria-selected="list==='planned'">计划通告<span class="v2-notice-count">{{ total('planned') }}</span></NTab><NTab name="ongoing" role="tab" :aria-selected="list==='ongoing'">进行中<span class="v2-notice-count">{{ total('ongoing') }}</span></NTab></NTabs>
    <div class="v2-notice-columns">
      <section v-for="lane in lanes" :key="lane.id" class="v2-notice-section" :class="{'is-mobile-hidden':list!==lane.id}" :data-list="lane.id" :aria-label="lane.label">
        <header><h2>{{ lane.label }}<span class="v2-notice-count">{{ total(lane.id) }}</span></h2></header>
        <NSkeleton v-if="loading&&!lane.rows.length" :repeat="6" height="72px" class="v2-notice-skeleton" />
        <NList v-else class="v2-notice-list" :aria-busy="loading">
          <NListItem v-for="row in lane.rows" :key="rowKey(row)"><article class="v2-notice-entry"><div class="v2-notice-entry-copy"><button type="button" class="v2-notice-open" data-v2-navigation @click="openRecord(row)">{{ rowTitle(row) }}</button><span><NTag size="small" :bordered="false">{{ typeLabel(row.work_type) }}</NTag><span>{{ row.building || row.scope || scopeLabel }}</span><span>{{ row.specialty || row.display_fields?.['专业'] || '' }}</span><span>{{ row.start_time || row.updated_at || '' }}</span></span><span v-if="rowAttention(row,lane.id)" class="v2-notice-attention"><CircleAlert :size="13" />{{ rowAttention(row,lane.id) }}</span></div><div class="v2-notice-entry-state"><NTag :type="lane.id==='ongoing'?'info':'default'" size="small" :bordered="false">{{ row.source_progress || row.status_label || row.status || (lane.id==='ongoing'?'进行中':'未开始') }}</NTag><div class="v2-notice-row-actions"><template v-if="lane.id==='ongoing'"><NButton size="small" data-v2-navigation type="primary" secondary @click="openRecord(row,false,'update')">更新</NButton><NButton size="small" data-v2-navigation @click="openRecord(row,false,'end')">结束</NButton></template><NButton v-else size="small" data-v2-navigation :type="rowEnded(row)?'default':'primary'" :secondary="!rowEnded(row)" @click="openRecord(row,false,rowStarted(row)?'':'start')">{{ rowEnded(row)?'查看':rowStarted(row)?'核验':'开始' }}<ChevronRight :size="14" /></NButton></div></div></article></NListItem>
          <NEmpty v-if="!lane.rows.length&&!loading" :description="error?'通告读取失败，不能判断为零条':lane.id==='ongoing'?'当前没有进行中通告':'当前筛选下没有计划通告'" />
        </NList>
        <footer class="v2-notice-list-footer"><span>本页 {{ lane.rows.length }} 条</span><NPagination :page="pages[lane.id]" :page-size="20" :item-count="lane.total" :page-slot="5" :disabled="loading" @update:page="value=>changePage(lane.id,value)" /></footer>
      </section>
    </div>

    <NModal :show="editing" preset="card" class="v2-notice-editor v2-notice-handling" :title="form?.values.title || '填写通告'" :bordered="false" :mask-closable="false" :close-on-esc="!busy&&!uploading&&!saving" :closable="!busy&&!uploading&&!saving" @update:show="value=>{if(!value)closeEditor()}">
      <NSpin v-if="detailLoading" class="v2-notice-loading" description="正在读取通告填写内容" />
      <NAlert v-if="detailError" type="error" role="alert" :bordered="false">{{ detailError }}</NAlert>
      <template v-if="form&&!detailLoading">
        <div class="v2-notice-editor-context"><NTag :bordered="false">{{ typeLabel(form.values.work_type) }}</NTag><span>{{ selectedBuildingLabel }}</span><NRadioGroup v-model:value="action" :disabled="locked||confirming" size="small" aria-label="本次操作" @update:value="changeAction"><NRadioButton v-for="value in form.actions" :key="value" :value="value">{{ actionLabel(value) }}</NRadioButton></NRadioGroup></div>
        <NAlert v-if="!form.actions.length" type="warning" :bordered="false">此通告当前只可查看，不可重新发送。</NAlert>
        <NAlert v-if="verifyBeforeStart" type="warning" :bordered="false">计划显示进行中，但尚未定位到已发目标通告。请先在下方“关联核验”中查找原通告；此时不能重复发送开始。</NAlert>
        <NAlert v-if="job" :type="job.phase==='success'?'success':job.phase==='failed'?'error':'info'" role="status" :bordered="false"><NSpin v-if="!terminal" size="small" />{{ job.error || job.upload_message || job.message || (terminal?'任务处理完成':'已受理，正在按原业务流程处理') }}<NButton v-if="job.phase==='failed'&&job.error_retryable" text :disabled="busy" @click="retryJob">重试原任务</NButton><NButton v-else-if="job.phase==='failed'&&job.error_category==='field_or_payload'" text :disabled="busy" @click="resetRejected">返回填写</NButton><NButton v-else-if="!terminal" text @click="pollJob">查询进度</NButton></NAlert>
        <NAlert v-if="ambiguous" type="warning" :bordered="false">提交响应未确认。已保留原操作标识，请核验原任务或原样重试，不要重新创建。<NButton text :loading="busy" @click="send">重试原提交</NButton></NAlert>
        <div v-if="confirming" class="v2-notice-review" aria-label="发送前确认">
          <header><h2>发送前确认</h2><NTag :bordered="false" type="info">{{ actionLabel(action) }} · {{ form.values.building_codes?.join('、') }}</NTag></header>
          <NAlert v-if="form.values.sync_maintenance_target==='1'||form.values.sync_maintenance_target===true" type="warning" :bordered="false">本次变更同时写入关联的维保目标记录。</NAlert>
          <div class="v2-notice-review-layout"><pre class="v2-notice-fulltext" tabindex="0" aria-label="待发送通告全文">{{ noticePreview(form,action) }}</pre><aside><h3>本次修改</h3><dl><template v-for="field in changedFields" :key="field.key"><dt>{{ field.label }}</dt><dd>{{ form.values[field.key] || '—' }}</dd></template></dl><p v-if="!changedFields.length">没有修改已加载字段</p><p v-if="form.photoRequired">现场照片：已有 {{ Number(form.values.site_photo_count||0) }} 张，新增 {{ photos.length }} 张</p><p v-if="form.values.work_type==='change'">阿里确认截图：新增 {{ aliPhotos.length }} 张</p><p v-if="form.sop&&action==='start'">本次工单：{{ sop.exempt?'不使用工单':selectedSopLabel }}</p><p v-if="form.sop&&action==='start'&&!sop.exempt">操作人：{{ sopPersonName(sop.operator_record_id) }}<br />审核人：{{ sopPersonName(sop.reviewer_record_id) }}</p><p>消息接收范围：{{ selectedBuildingLabel }}，其余接收规则沿用原业务流程。</p></aside></div>
        </div>
        <template v-else>
        <NCollapse v-if="form.values.manual_binding_required||form.sourceOptions.length" :default-expanded-names="form.values.manual_binding_required&&!form.values.target_record_id?['binding']:[]"><NCollapseItem title="计划关联" name="binding"><NRadioGroup v-if="form.values.manual_binding_required&&!form.values.target_record_id" v-model:value="form.values.manual_binding_choice" :disabled="locked"><NRadio value="bind">关联计划</NRadio><NRadio value="unbound">独立通告</NRadio></NRadioGroup><div class="v2-notice-binding"><NSelect v-model:value="sourceChoice" filterable clearable remote :options="sourceOptions" placeholder="输入名称查找对应计划" :disabled="locked" @search="searchSources" /><NButton :loading="sourceLoading" :disabled="locked" @click="loadSources()">读取计划</NButton><NButton :disabled="locked||!sourceChoice" @click="bindSource">确认关联</NButton></div></NCollapseItem></NCollapse>
        <NForm label-placement="top" :disabled="locked" @submit.prevent="preview" @paste="pastePhotos">
          <section class="v2-notice-field-section"><h2>{{ action==='start'?'开始通告':action==='end'?'完成情况':'本次更新' }}<span v-if="missingFields.length">待补充 {{ missingFields.length }} 项</span></h2><NoticeFields :fields="primaryFields" :values="form.values" :locked="locked" :attempted="attempted"><NFormItem v-if="action==='start'" label="所属楼栋" required><NCheckboxGroup v-model:value="form.values.building_codes" :disabled="locked"><NCheckbox v-for="s in buildingOptions" :key="s.value" :value="s.value">{{ s.label }}</NCheckbox></NCheckboxGroup></NFormItem></NoticeFields></section>
          <NCollapse v-if="secondaryFields.length" v-model:expanded-names="expandedFields"><NCollapseItem title="通告原有信息与其它字段" name="facts"><NoticeFields :fields="secondaryFields" :values="form.values" :locked="locked" :attempted="attempted" /><NFormItem v-if="action!=='start'" label="所属楼栋" required><NCheckboxGroup v-model:value="form.values.building_codes" :disabled="locked"><NCheckbox v-for="s in buildingOptions" :key="s.value" :value="s.value">{{ s.label }}</NCheckbox></NCheckboxGroup></NFormItem></NCollapseItem></NCollapse>
          <section v-if="form.sop&&action==='start'" class="v2-notice-field-section"><h2>本次工单</h2><LighthouseNoticeSop id="v2-notice-sop" :field="sopField" v-model="sop" :disabled="locked" @load-options="loadSops($event.q)" /></section>
          <section v-if="form.groupId" class="v2-notice-field-section"><h2>已关联工单</h2><NAlert v-if="sopStatus" :type="sopStatus.state==='completed'?'success':'info'" :bordered="false">{{ sopStatus.error || (sopStatus.state==='completed'?'工单已完成':sopStatus.state==='upload_pending'?'步骤已全部完成，工单表格正在上传':'工单执行中') }}<span v-if="sopStatus.enabled&&!sopStatus.ready"> · 延时核验尚未满足，剩余 {{ sopStatus.remaining_seconds }} 秒</span></NAlert><div class="v2-notice-tools"><NButton v-for="link in form.sopLinks" :key="link.url" tag="a" :href="workspaceUrl(link.url)" target="_blank" rel="noopener noreferrer">{{ link.label }}</NButton><NButton :loading="busy" :disabled="locked" @click="refreshSopStatus">刷新工单状态</NButton><NButton :loading="busy" :disabled="locked" @click="resendSopLinks">重发操作人及审核人链接</NButton><NButton v-if="sopStatus?.state==='upload_pending'" :loading="busy" :disabled="locked" @click="retrySopUpload">重试上传工单表格</NButton></div></section>
          <section v-if="form.photoRequired" class="v2-notice-field-section"><h2>现场照片<span>{{ Number(form.values.site_photo_count||0) }} 张已上传</span></h2><NImageGroup><div class="v2-notice-photos"><figure v-for="photo in form.photos" :key="photo.local_image_id||photo.file_token"><NImage v-if="safePhoto(photo)" :src="safePhoto(photo)" :alt="photo.file_name||'已上传现场照片'" width="108" /><span>{{ photo.file_name || '已上传' }}</span></figure><figure v-for="(photo,index) in photos" :key="photo.upload_id"><NImage v-if="safePhoto(photo)" :src="safePhoto(photo)" :alt="photo.file_name||'现场照片'" width="108" /><span>{{ photo.file_name }}</span><NButton quaternary circle aria-label="移除待上传照片" :disabled="locked" @click="photos.splice(index,1)"><X :size="16" /></NButton></figure></div></NImageGroup><label class="v2-notice-upload" @dragover.prevent @drop.prevent="uploadFiles($event.dataTransfer?.files, false)" @paste.stop="pastePhotos"><ImagePlus :size="20" /><span>{{ uploading?'正在上传…':'添加现场照片，也可拖入或粘贴' }}</span><input type="file" accept="image/*" multiple :disabled="locked" aria-label="添加现场照片" @change="photoInput($event,false)" /></label></section>
          <section v-if="form.values.work_type==='change'" class="v2-notice-field-section"><h2>阿里确认截图</h2><NImageGroup><div class="v2-notice-photos"><figure v-for="photo in form.aliPhotos" :key="photo.local_image_id||photo.file_token"><NImage v-if="safePhoto(photo)" :src="safePhoto(photo)" width="108" :alt="photo.file_name||'已上传确认截图'" /><span>{{ photo.file_name || '已上传' }}</span><NButton quaternary circle :disabled="locked" aria-label="删除已上传确认截图" @click="removeAliPhoto(photo)"><Trash2 :size="15" /></NButton></figure></div></NImageGroup><p v-for="photo in aliPhotos" :key="photo.upload_id">{{ photo.file_name }}<NButton text :disabled="locked" @click="aliPhotos=[]">移除</NButton></p><label class="v2-notice-upload"><ImagePlus :size="18" /><span>选择确认截图（最多一张）</span><input type="file" accept="image/*" :disabled="locked" aria-label="添加阿里确认截图" @change="photoInput($event,true)" /></label><NButton v-if="form.values.target_record_id&&aliPhotos.length" :disabled="locked" :loading="busy" @click="uploadAliNow">仅上传确认截图</NButton></section>
          <NoticeAlertTags v-if="form.values.target_record_id" :record-id="form.values.target_record_id" :work-type="form.values.work_type" />
        </NForm>
        <NoticeRelations :key="formKey" :form="form" :locked="locked" :needs-verification="verifyBeforeStart" @busy="busy=$event" @bound="associationBound" />
        </template>
      </template>
      <template #footer><div class="v2-notice-editor-footer"><NButton v-if="form?.canDelete&&user.can_delete_business&&!confirming" type="error" text :disabled="locked" @click="deleteNotice"><Trash2 :size="16" />删除通告</NButton><span></span><NButton v-if="confirming" data-v2-navigation :disabled="busy" @click="confirming=false">返回修改</NButton><NButton v-else data-v2-navigation :disabled="busy||uploading||saving" @click="closeEditor">关闭</NButton><NButton v-if="form?.values.target_record_id&&!confirming" :loading="saving" :disabled="locked||!dirty" @click="saveWithFeedback">保存草稿</NButton><NButton v-if="confirming" type="primary" :loading="busy" @click="send">确认发送{{ actionLabel(action) }}</NButton><NButton v-else type="primary" :disabled="locked||!form?.actions.length" :loading="busy" @click="preview"><Send :size="16" />预览并确认{{ actionLabel(action) }}</NButton></div></template>
    </NModal>
    <NModal :show="pasteOpen" preset="card" class="v2-notice-confirm" title="粘贴通告原文" :mask-closable="false" :closable="!parsing" :close-on-esc="!parsing" @update:show="value=>pasteOpen=value"><NAlert v-if="pasteError" role="alert" type="error" :bordered="false">{{ pasteError }}</NAlert><NInput v-model:value="pasteText" type="textarea" :autosize="{minRows:8,maxRows:16}" maxlength="30000" :input-props="{'aria-label':'通告原文'}" :disabled="parsing" placeholder="粘贴完整通告，解析后核对字段再发送" /><template #footer><div class="v2-dialog-actions"><NButton :disabled="parsing" @click="pasteOpen=false">取消</NButton><NButton type="primary" :loading="parsing" :disabled="!pasteText.trim()" @click="parsePasted">解析并填写</NButton></div></template></NModal>
    <NoticeChangeConfirmations v-if="changeOpen" :scope="scope" :focus-id="params.get('change_confirmation')||''" @close="changeOpen=false" @changed="load(true)" />
    <NoticeTaskRecords v-if="recordsOpen" :user-id="user.open_id" :scope="scope" :modules="user.modules||[]" @close="recordsOpen=false" @changed="load(true)" />
    <NModal v-model:show="undoOpen" preset="card" class="v2-notice-confirm" title="近三天可回退" :bordered="false" :closable="!busy" :mask-closable="!busy" :close-on-esc="!busy"><NSpin :show="undoLoading"><NAlert v-if="undoError" role="alert" type="error" :bordered="false">{{ undoError }}</NAlert><NAlert v-if="undoJob" role="status" :type="undoJob.phase==='failed'?'error':undoJob.phase==='success'?'success':'info'" :bordered="false"><NSpin v-if="undoPending" size="small" />{{ undoJob.error || (undoJob.phase==='success'?'回退已完成':'回退任务已受理，正在处理') }}<NButton v-if="undoPending" text @click="pollUndo">查询进度</NButton></NAlert><NList><NListItem v-for="item in undos" :key="item.undo_id"><strong>{{ item.title }}</strong><p>{{ item.undo_label }} · {{ item.undo_created_at }}</p><NButton :disabled="busy||undoPending" @click="undo(item)">回退上一步</NButton></NListItem></NList><NEmpty v-if="!undoLoading&&!undos.length" description="暂无可回退记录" /></NSpin></NModal>
  </section>
</template>
<script setup lang="ts">
import { computed,nextTick,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import { NAlert,NButton,NCheckbox,NCheckboxGroup,NCollapse,NCollapseItem,NDropdown,NEmpty,NForm,NFormItem,NImage,NImageGroup,NInput,NList,NListItem,NModal,NPagination,NRadio,NRadioButton,NRadioGroup,NSelect,NSkeleton,NSpin,NTab,NTabs,NTag,useDialog,useMessage } from 'naive-ui';
import { ChevronRight,CircleAlert,Ellipsis,ImagePlus,Plus,RefreshCw,Search,Send,Trash2,X } from 'lucide-vue-next';
import { ApiError,requestJson,requestBinaryJson,refreshRemoteSourceAndWait,type Dict } from './client';
import { modules,NOTICE_IDS,beijingDate } from './modules';
import { navigate,registerNavigationGuard } from './navigation';
import { workspaceUrl } from './version';
import { trackButtonRequest } from './actionMotion';
import { parseNoticeForm,noticeCommand,noticePreview,validateNotice,endProgress,type NoticeForm } from './noticeForm';
import NoticeFields from './NoticeFields.vue';
import NoticeRelations from './NoticeRelations.vue';
import NoticeChangeConfirmations from './NoticeChangeConfirmations.vue';
import NoticeTaskRecords from './NoticeTaskRecords.vue';
import {rememberNoticeTask} from './noticeTasks';
import LighthouseNoticeSop from '../components/LighthouseNoticeSop.vue';
import NoticeAlertTags from '../components/NoticeAlertTags.vue';
import { createRepairOperationId } from '../repairManagementUtils';
import { noticeIsEnded } from '../mopSelectionUtils';
const props=defineProps<{url:string;scope:string;scopes:{value:string;label:string}[];user:Dict;active:boolean}>();
const dialog=useDialog(),message=useMessage();
const params=computed(()=>new URL(props.url,location.origin).searchParams);
const types=computed(()=>[{id:'all',label:'全部'},...modules.filter(m=>NOTICE_IDS.includes(m.id)&&(props.user.modules||[]).includes(m.id))]);
const kind=ref(params.value.get('work_type')||'all'),list=ref('planned'),month=ref(params.value.get('month')||Number(beijingDate().slice(5,7))+'月'),search=ref(params.value.get('search')||''),specialty=ref(params.value.get('specialty')||null);
const months=Array.from({length:12},(_,i)=>({label:(i+1)+'月',value:(i+1)+'月'})),specialties=['电气','暖通','弱电','消防'].map(value=>({label:value,value}));
const scopeLabel=computed(()=>props.scopes.find(s=>s.value===props.scope)?.label||props.scope);
const buildingOptions=computed(()=>props.scopes.filter(s=>['110',...'ABCDEH'].includes(s.value)));
const sopTypes=computed(()=>types.value.filter(t=>['maintenance','polling','adjust'].includes(t.id)).map(t=>({value:t.id,label:t.label})));
const newOptions=computed(()=>[...types.value.filter(t=>t.id!=='all').map(t=>({key:t.id,label:'手填'+t.label})),{type:'divider',key:'divider'},{key:'paste',label:'粘贴原文解析'}]);
const moreOptions=computed(()=>[{key:'records',label:'本机办理记录'},{key:'undo',label:'近三天回退'},...(sopTypes.value.length?[{key:'sop',label:'SOP与工单模板'}]:[]),...((props.user.role==='admin'||props.user.is_admin===true)&&types.value.some(t=>t.id==='change')?[{key:'change',label:'变更确认'}]:[]),...((props.user.modules||[]).includes('engineer_mop')?[{key:'mop',label:'维护单管理'}]:[]),...((props.user.modules||[]).includes('accounts')?[{key:'memory',label:'历史记忆管理'}]:[])]);
const refreshOptions=computed(()=>[{key:'page',label:'刷新本页'},...types.value.filter(t=>['maintenance','change','repair'].includes(t.id)).map(t=>({key:t.id,label:'同步'+t.label+'源表'}))]);
const changeOpen=ref(false),pasteOpen=ref(false),pasteText=ref(''),pasteError=ref(''),parsing=ref(false),syncing=ref(false);
const recordsOpen=ref(false);
function openRecords(){recordsOpen.value=true;}
let syncAbort:AbortController|undefined;
function more(key:string){if(key==='undo')void openUndo();else if(key==='sop')navigate('/?'+new URLSearchParams({view:'sops',scope:props.scope,work_type:kind.value}));else if(key==='change')changeOpen.value=true;else if(key==='records')void openRecords();else navigate((key==='mop'?'/engineer/mop':'/admin/history-memory')+'?scope='+encodeURIComponent(props.scope));}
function newNotice(key:string){if(key==='paste'){pasteError.value='';pasteOpen.value=true;}else void openRecord(null,false,'start','',key);}
async function refreshSource(key:string){if(key==='page'){void load(true);return;}if(syncing.value||!refreshOptions.value.some(t=>t.key===key))return;syncing.value=true;error.value='';syncAbort=new AbortController();try{await refreshRemoteSourceAndWait(key as 'maintenance'|'change'|'repair','/api/'+key+'-refresh?scope='+encodeURIComponent(props.scope),{scope:props.scope,signal:syncAbort.signal});await load(true);}catch(e){if(!syncAbort.signal.aborted)error.value=(e as Error).message;}finally{syncing.value=false;}}
async function parsePasted(){if(parsing.value||!pasteText.value.trim())return;parsing.value=true;pasteError.value='';try{const body=new FormData();body.append('scope',props.scope);body.append('month',month.value);body.append('work_type',kind.value==='all'?types.value[1]?.id||'maintenance':kind.value);body.append('paste_text',pasteText.value);const result=await requestJson('/workbench-lite/parse',{method:'POST',body});parseNoticeForm(result.fragments?.detail||'');pasteOpen.value=false;await openRecord(null,false,'',result.fragments.detail);}catch(e){pasteError.value=(e as Error).message;}finally{parsing.value=false;}}
const pageNumber=(value:string|null)=>Math.max(1,Math.trunc(Number(value)||1));
const pages=ref<Record<string,number>>({planned:pageNumber(params.value.get('records_page')||params.value.get('pending_page')),ongoing:pageNumber(params.value.get('ongoing_page'))}),data=ref<Dict>({}),loading=ref(false),error=ref('');
const lanes=computed(()=>[{id:'planned',label:'计划通告',rows:(data.value.records||[]) as Dict[],total:Number(data.value.records_pagination?.total??data.value.records?.length??0)}, {id:'ongoing',label:'进行中',rows:(data.value.ongoing||[]) as Dict[],total:Number(data.value.ongoing_pagination?.total??data.value.ongoing?.length??0)}]);
const total=(mode:string)=>data.value[mode==='ongoing'?'ongoing_pagination':'records_pagination']?.total??'—';
const typeLabel=(value:string)=>modules.find(m=>m.id===value)?.label||'通告';
const rowKey=(row:Dict)=>String(row.active_item_id||row.record_id||row.id);
const rowTitle=(row:Dict)=>String(row.title||row.name||row.display_fields?.['维护总项']||row.display_fields?.['名称']||'未命名通告');
const rowEnded=(row:Dict)=>noticeIsEnded({status:row.source_progress||row.status_label||row.status});
const rowStarted=(row:Dict)=>!rowEnded(row)&&/进行中|已开始|^开始$|^更新$/.test(String(row.source_progress||row.status_label||row.status||''));
function rowAttention(row:Dict,lane:string){if(lane==='planned'&&rowStarted(row))return '先核验已发通告';if(lane==='ongoing'&&['maintenance','change','repair'].includes(row.work_type)&&row.site_photo_count!==undefined&&Number(row.site_photo_count)===0)return '结束前需现场照片';return '';}
const actionLabel=(value:string)=>({start:'开始',update:'更新',end:'结束'}[value]||'');
let listAbort:AbortController|undefined,detailAbort:AbortController|undefined,disposed=false,timer=0;
const editing=ref(false),detailLoading=ref(false),form=ref<NoticeForm|null>(null),detailError=ref(''),busy=ref(false),saving=ref(false),uploading=ref(false),confirming=ref(false),ambiguous=ref(false);
const attempted=ref(false);
const verifyBeforeStart=ref(false);
const action=ref('start'),sop=ref<Dict>({}),sopField=ref<Dict>({}),photos=ref<Dict[]>([]),aliPhotos=ref<Dict[]>([]),job=ref<Dict|null>(null);
const sourceChoice=ref<string|null>(null),sourceOptions=ref<{label:string;value:string}[]>([]),sourceLoading=ref(false);
const sopStatus=ref<Dict|null>(null);
const sopScope=computed(()=>form.value?.values.building_codes?.length===1?String(form.value.values.building_codes[0]):'');
const selectedBuildingLabel=computed(()=>(form.value?.values.building_codes||[]).map((v:string)=>v==='110'?'110站':v+'楼').join('、')||'未选择楼栋');
const selectedSopLabel=computed(()=>sopField.value.sops?.find((s:Dict)=>s.sop_id===sop.value.sop_id)?.name||'已选择工单');
const sopPersonName=(id:string)=>sopField.value.people?.find((p:Dict)=>p.record_id===id)?.name||'请在工单选择区核对';
watch(sopScope,(value,previous)=>{sopField.value={notice_scope:value,work_type:form.value?.values.work_type};if(previous&&previous!==value)sop.value={sop_version:0};});
const expandedFields=ref<string[]>([]),attentionKeys=ref<string[]>([]),originalFields=ref<Dict>({});
const missingFields=computed(()=>form.value?.fields.filter(f=>f.required&&!String(form.value!.values[f.key]??'').trim())||[]);
const primaryFields=computed(()=>form.value?.fields.filter(f=>action.value==='start'||['title','progress','actual_action_time',...attentionKeys.value].includes(f.key))||[]);
const secondaryFields=computed(()=>form.value?.fields.filter(f=>!primaryFields.value.includes(f))||[]);
const changedFields=computed(()=>form.value?.fields.filter(f=>String(form.value!.values[f.key]??'')!==String(originalFields.value[f.key]??''))||[]);
const terminal=computed(()=>!!job.value&&['success','succeeded','completed','failed','cancelled'].includes(job.value.phase));
const locked=computed(()=>busy.value||saving.value||uploading.value||detailLoading.value||ambiguous.value||!!job.value||!form.value?.actions.length);
const saved=ref('');
let draftVersion=0,operationId='',frozen:Dict|null=null,formKey='',originalProgress='',progressBeforeEnd='';
const snapshot=()=>JSON.stringify({values:form.value?.values,sop:sop.value,photos:photos.value,aliPhotos:aliPhotos.value,action:action.value});
const dirty=computed(()=>!!form.value&&snapshot()!==saved.value);
function query(){return new URLSearchParams({scope:props.scope,work_type:kind.value,month:month.value,search:search.value,specialty:specialty.value||'',sections:'records,ongoing',records_page:String(pages.value.planned),records_page_size:'20',ongoing_page:String(pages.value.ongoing),ongoing_page_size:'20'});}
async function load(fresh=false){
  if(!props.active||disposed||document.hidden)return;
  listAbort?.abort();const c=new AbortController();listAbort=c;loading.value=true;error.value='';
  try{const value=await requestJson('/api/workbench?'+query(),{signal:c.signal,...(fresh?{fresh:true}:{})});if(c.signal.aborted)return;if(value.source_snapshot_ready===false||!Array.isArray(value.records)||!Array.isArray(value.ongoing))throw new Error('通告资料尚未完整读取，请稍后重试。');data.value=value;}
  catch(e){if(!c.signal.aborted)error.value=e instanceof Error?e.message:'读取失败，原页面保留';}
  finally{if(listAbort===c)loading.value=false;}
}
function setKind(value:string){const q=query();q.set('work_type',value);q.set('records_page','1');q.set('ongoing_page','1');q.delete('sections');navigate('/workbench-lite?'+q);}
function changeScope(value:string){if(!props.scopes.some(s=>s.value===value))return;const q=query();q.set('scope',value);q.set('records_page','1');q.set('ongoing_page','1');q.delete('sections');navigate('/workbench-lite?'+q);}
function changePage(lane:string,page:number){pages.value[lane]=page;const q=query();q.delete('sections');const before=location.href;navigate('/workbench-lite?'+q,{replace:true});if(before===location.href)void load();}
function filter(){pages.value={planned:1,ongoing:1};const q=query();q.delete('sections');const before=location.href;navigate('/workbench-lite?'+q,{replace:true});if(location.href===before)void load(true);}
function cacheKey(values:Dict){return 'v2-notice:'+props.user.open_id+':'+values.scope+':'+values.work_type+':'+(values.target_record_id||values.source_record_id||'manual');}
function persist(){if(!form.value||!formKey||job.value?.phase==='success')return;try{sessionStorage.setItem(formKey,JSON.stringify({snapshot:JSON.parse(snapshot()),draftVersion,job:job.value,operationId,frozen,at:Date.now()}));}catch{detailError.value='浏览器暂时不能保存恢复信息，请保持页面打开并使用保存草稿。';}}
function resetRejected(){if(job.value?.phase!=='failed'||job.value.error_category!=='field_or_payload'||busy.value)return;job.value=null;frozen=null;operationId='';ambiguous.value=false;persist();}
async function openRecord(row:Dict|null,initial=false,desiredAction='',parsedHtml='',manualType=''){
  if(busy.value||uploading.value||saving.value)return;
  if(editing.value&&dirty.value){askLeave(()=>openRecord(row,initial,desiredAction,parsedHtml,manualType));return;}
  attempted.value=false;confirming.value=false;verifyBeforeStart.value=false;expandedFields.value=[];attentionKeys.value=[];
  detailAbort?.abort();const c=new AbortController();detailAbort=c;editing.value=true;detailLoading.value=true;detailError.value='';form.value=null;job.value=null;ambiguous.value=false;frozen=null;operationId='';draftVersion=0;photos.value=[];aliPhotos.value=[];sop.value={};sopField.value={};formKey='';
  const work=row?.work_type||manualType||kind.value;
  const q=new URLSearchParams({scope:props.scope,work_type:work,month:month.value});
  if(row)q.set(row.active_item_id?'active_item_id':'record_id',rowKey(row));else if(initial){for(const key of ['active_item_id','record_id','prefill_context_id','prefill_source_record_id','prefill_target_record_id','prefill_action'])if(params.value.has(key))q.set(key,params.value.get(key)!);}else q.set('manual','1');
  try{
    const result=parsedHtml?{fragments:{detail:parsedHtml}}:await requestJson('/api/workbench/lite-detail?'+q,{signal:c.signal,cache:'no-store'});if(c.signal.aborted)return;
    const next=parseNoticeForm(result.fragments?.detail||'');
    if(!NOTICE_IDS.includes(next.values.work_type)||!(props.user.modules||[]).includes(next.values.work_type))throw new Error('无权办理此类通告。');
    form.value=next;action.value=next.actions.includes(next.initialAction)?next.initialAction:next.actions[0]||'start';formKey=cacheKey(next.values);sourceChoice.value=next.values.source_record_id||null;sourceOptions.value=next.sourceOptions;
    verifyBeforeStart.value=!!row&&rowStarted(row)&&!row.active_item_id&&!next.values.target_record_id&&next.actions.includes('start');
    originalFields.value={...next.values};attentionKeys.value=next.fields.filter(f=>f.required&&!String(next.values[f.key]??'').trim()).map(f=>f.key);
    sop.value={sop_version:0,exempt:next.values.polling_work_order_exempt===true};sopField.value={notice_scope:sopScope.value,work_type:next.values.work_type};
    if(next.values.target_record_id){const draft=await requestJson('/api/workbench/draft?'+new URLSearchParams({scope:props.scope,work_type:next.values.work_type,target_record_id:next.values.target_record_id,action:action.value}),{signal:c.signal,cache:'no-store'});if(c.signal.aborted)return;if(draft.draft){draftVersion=Number(draft.draft.version)||0;for(const field of next.fields)if(draft.draft.fields?.[field.key]!==undefined)next.values[field.key]=draft.draft.fields[field.key];}}
    originalProgress=String(next.values.progress||'');progressBeforeEnd=originalProgress;saved.value=snapshot();
    try{
      const cached=JSON.parse(sessionStorage.getItem(formKey)||'null');
      if(cached&&Date.now()-cached.at<7*86400000){
        if(cached.draftVersion===draftVersion&&(!parsedHtml||cached.job||cached.frozen)){
          for(const field of next.fields)if(!field.readonly&&cached.snapshot.values[field.key]!==undefined)next.values[field.key]=cached.snapshot.values[field.key];
          next.values.building_codes=(cached.snapshot.values.building_codes||[]).filter((s:string)=>buildingOptions.value.some(b=>b.value===s));
          sop.value={sop_version:0,...cached.snapshot.sop};photos.value=cached.snapshot.photos||[];aliPhotos.value=cached.snapshot.aliPhotos||[];
          action.value=next.actions.includes(cached.snapshot.action)?cached.snapshot.action:action.value;
        }else if(!parsedHtml)detailError.value='已读取较新的服务端草稿，本机旧副本未覆盖新内容。';
        // An unresolved write must still be recoverable even when another window saved a newer draft.
        job.value=cached.job||null;operationId=cached.operationId||'';frozen=cached.frozen||null;ambiguous.value=!!frozen&&!job.value;
        if(parsedHtml&&(job.value||frozen))detailError.value='同类手填通告有未核验的原任务，请先核验原任务；本次解析尚未提交。';
      }
    }catch{/* An invalid local draft never replaces the server values. */}
    const localManual=(()=>{try{const cached=JSON.parse(sessionStorage.getItem(formKey)||'null');return cached&&Date.now()-cached.at<7*86400000?cached.snapshot?.values:null;}catch{return null;}})();
    if(!parsedHtml&&next.values.manual==='1'&&!next.values.source_record_id&&!next.values.target_record_id&&localManual){
      const prefix='manual:lite:'+next.values.scope+':'+next.values.work_type+':';
      if(String(localManual.manual_id||'').startsWith(prefix))next.values.manual_id=localManual.manual_id;
      if(localManual.manual_binding_choice==='unbound')next.values.manual_binding_choice='unbound';
    }
    sopStatus.value=null;
    if(next.groupId){void requestJson('/api/workbench/polling-delay-status?group_id='+encodeURIComponent(next.groupId),{signal:c.signal,cache:'no-store'}).then(value=>{if(!c.signal.aborted)sopStatus.value=value;}).catch(e=>{if(!c.signal.aborted)sopStatus.value={error:'工单状态暂不可用：'+e.message};});}
    if(desiredAction&&!job.value&&!ambiguous.value){
      if(next.actions.includes(desiredAction)){action.value=desiredAction;changeAction();}
      else if(next.actions.length){action.value='';detailError.value='该记录当前不能'+actionLabel(desiredAction)+'，请核对首条上传状态，并重新选择本次操作。';}
    }
    if(job.value&&!terminal.value)void pollJob();
  }catch(e){if(!c.signal.aborted)detailError.value=e instanceof Error?e.message:'通告读取失败';}
  finally{if(detailAbort===c)detailLoading.value=false;}
}
function askLeave(proceed:()=>void){dialog.warning({title:'当前填写尚未保存',content:'继续将关闭当前填写区。本机恢复副本仍保留，未提交的内容不会发送。',positiveText:'保留副本并离开',negativeText:'继续填写',onPositiveClick:()=>{persist();saved.value=snapshot();editing.value=false;proceed();}});}
function closeEditor(){if(busy.value||uploading.value||saving.value)return;if(dirty.value&&!job.value){askLeave(()=>closeEditor());return;}persist();editing.value=false;detailAbort?.abort();window.clearTimeout(timer);}
function changeAction(){
  const values=form.value?.values;
  if(values&&values.work_type!=='adjust'){
    if(action.value==='end'&&(!String(values.progress||'').trim()||values.progress===originalProgress)){progressBeforeEnd=String(values.progress||'');values.progress=endProgress;}
    else if(action.value!=='end'&&values.progress===endProgress)values.progress=progressBeforeEnd;
  }
  confirming.value=false;
}
async function saveDraft(){
  if(!form.value||saving.value)return false;
  persist();
  if(!form.value.values.target_record_id){saved.value=snapshot();return true;}
  saving.value=true;detailError.value='';
  try{
    const v=form.value.values;
    const current=await requestJson('/api/workbench/draft?'+new URLSearchParams({scope:v.scope,work_type:v.work_type,target_record_id:v.target_record_id,action:action.value}),{cache:'no-store'});
    const conflict='草稿已被其他窗口修改，请保留本次填写并重新核对，未覆盖对方内容。';
    if(Number(current.draft?.version||0)!==draftVersion)throw new Error(conflict);
    const fields=Object.fromEntries(form.value.fields.map(f=>[f.key,String(v[f.key]??'').trim()]));
    fields.building_codes=v.building_codes.join(',');
    const revision=Number(current.draft?.client_revision||0)+1;
    const result=await requestJson('/api/workbench/draft',{method:'PUT',body:JSON.stringify({scope:v.scope,work_type:v.work_type,target_record_id:v.target_record_id,action:action.value,fields,client_revision:revision})});
    if(Number(result.draft?.client_revision)!==revision||Object.entries(fields).some(([key,value])=>String(result.draft?.fields?.[key]??'')!==value))throw new Error(conflict);
    draftVersion=Number(result.draft.version);saved.value=snapshot();persist();message.success('草稿已保存');return true;
  }catch(e){detailError.value=e instanceof Error?e.message:'草稿保存失败';return false;}
  finally{saving.value=false;}
}
function preview(){
  if(!form.value||locked.value)return;
  attempted.value=true;detailError.value=validateNotice(form.value,action.value,sop.value,photos.value);
  if(verifyBeforeStart.value&&action.value==='start')detailError.value='请先核验原目标通告，不能重复发送开始。';
  if(!detailError.value&&action.value==='end'&&form.value.groupId){
    const state=sopStatus.value;
    if(!state||state.error)detailError.value='工单状态尚未核验，请刷新工单状态后重试。';
    else if(!['upload_pending','completed'].includes(state.state))detailError.value='关联工单步骤尚未全部完成。';
    else if(state.missing_completion||Number(state.due_at_ts||0)>Date.now()/1000)detailError.value='工单延时提醒条件尚未满足，请等待后刷新工单状态。';
  }
  if(detailError.value){
    expandedFields.value=['facts'];
    const missing=form.value.fields.find(f=>f.required&&!String(form.value!.values[f.key]??'').trim());
    void nextTick(()=>{const field=missing?document.querySelector<HTMLElement>('.v2-notice-editor [data-notice-field="'+CSS.escape(missing.key)+'"] input,.v2-notice-editor [data-notice-field="'+CSS.escape(missing.key)+'"] textarea'):null;if(field){field.focus();field.scrollIntoView({block:'center'});}});
    return;
  }
  confirming.value=true;void nextTick(()=>document.querySelector('.v2-notice-editor>.n-card-content')?.scrollTo({top:0}));
}
async function submit(){
  if(!form.value||busy.value)return {ok:false};
  if(!frozen&&verifyBeforeStart.value&&action.value==='start'){detailError.value='请先核验原目标通告，不能重复发送开始。';return {ok:false};}
  busy.value=true;detailError.value='';
  try{
    if(!frozen){const issue=validateNotice(form.value,action.value,sop.value,photos.value);if(issue)throw new Error(issue);if(!await saveDraft()){confirming.value=false;return {ok:false};}operationId=createRepairOperationId('v2-notice');frozen=noticeCommand(form.value,action.value,operationId,draftVersion,sop.value,photos.value,aliPhotos.value);persist();}
    const response=await requestJson('/api/workbench-actions',{method:'POST',body:JSON.stringify(frozen)});
    if(!response.job_id)throw new Error('未收到任务编号，保留原提交等待核验。');
    job.value={...response,phase:response.phase||'queued'};ambiguous.value=false;confirming.value=false;saved.value=snapshot();persist();void pollJob();
    rememberNoticeTask(props.user.open_id,response,form.value.values);
    return response;
  }catch(e){
    const payload=e instanceof ApiError?e.payload:{};
    if(payload.error_code==='confirmation_required'&&payload.details?.kind==='plan_convergence_unmatched'){
      confirming.value=false;dialog.warning({title:'计划收敛尚未匹配',content:String(payload.error||'未匹配到计划收敛，是否仍确认发送？'),positiveText:'确认继续发送',negativeText:'返回核对',maskClosable:false,closeOnEsc:false,closable:false,onPositiveClick:()=>{if(frozen)frozen.plan_convergence_confirmation=payload.details.confirmation;void send();},onNegativeClick:()=>{frozen=null;operationId='';persist();}});
    }else{confirming.value=false;detailError.value=e instanceof Error?e.message:'提交失败，填写已保留';ambiguous.value=!!frozen&&(!(e instanceof ApiError)||!e.status||e.status>=500);if(!ambiguous.value)frozen=null;persist();}
    return {ok:false};
  }finally{busy.value=false;}
}
function send(){return trackButtonRequest(submit());}
function saveWithFeedback(){return trackButtonRequest(saveDraft().then(ok=>({ok})));}
async function pollJob(){
  window.clearTimeout(timer);
  if(!job.value?.job_id||!editing.value||!props.active||document.hidden||disposed)return;
  const id=job.value.job_id;
  try{
    const value=await requestJson('/api/jobs/'+encodeURIComponent(id),{cache:'no-store'});
    if(job.value?.job_id!==id||disposed||!props.active||!editing.value)return;
    job.value=value;persist();
    if(terminal.value){
      if(value.phase==='success'){
        frozen=null;saved.value=snapshot();
        try{sessionStorage.removeItem(formKey);}catch{/* Submission success does not depend on browser storage. */}
        window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);
      }
      return;
    }
  }catch{if(job.value?.job_id===id)detailError.value='任务进度暂时不可用，原任务仍保留，请勿重复新增。';}
  if(!disposed&&props.active&&editing.value)timer=window.setTimeout(pollJob,4000);
}
async function retryJob(){if(!job.value||busy.value)return;busy.value=true;try{job.value=await requestJson('/api/jobs/'+encodeURIComponent(job.value.job_id)+'/retry',{method:'POST',body:'{}'});void pollJob();}catch(e){detailError.value=String((e as Error).message);}finally{busy.value=false;}}
async function loadSops(q=''){
  if(!form.value)return;
  const current=form.value,s=sopScope.value;
  if(!s)return;
  sopField.value={...sopField.value,directory_loading:true,notice_scope:s,work_type:current.values.work_type};
  try{
    const sops=await requestJson('/api/polling-sops?'+new URLSearchParams({scope:s,work_type:current.values.work_type}));
    const people=await requestJson('/api/signatures/people?'+new URLSearchParams({limit:'200',q}));
    if(form.value!==current||sopScope.value!==s)return;
    const combined=new Map<string,Dict>((sopField.value.people||[]).map((p:Dict)=>[p.record_id,p]));
    for(const p of people.people||[])combined.set(p.record_id,p);
    combined.set('h_duty_account',{record_id:'h_duty_account',name:'H楼值班账号'});
    sopField.value={notice_scope:s,directory_scope:s,work_type:current.values.work_type,sops:sops.items||[],people:[...combined.values()]};
  }catch(e){if(form.value===current)sopField.value.directory_error=(e as Error).message;}
  finally{if(form.value===current)sopField.value.directory_loading=false;}
}
const safePhoto=(photo:Dict)=>/^\/api\/(?:notice-images\/[a-f0-9]{32}|change-confirmations\/[\w-]+\/screenshot\/preview(?:\?[^#]*)?)$/.test(String(photo.preview_url||''))?photo.preview_url:'';
async function uploadFiles(files:FileList|null|undefined,ali:boolean){if(!files||locked.value||!form.value)return;const current=form.value;uploading.value=true;detailError.value='';try{for(const file of [...files].slice(0,ali?1:20-photos.value.length)){if(!file.type.startsWith('image/')||file.size>8*1024*1024)throw new Error('请选择8MB以内的图片。');const value=await requestBinaryJson('/api/notice-attachments?'+new URLSearchParams({scope:current.values.scope,identity:current.values.active_item_id||current.values.manual_id||current.values.source_record_id,kind:ali?'ali':'site',file_name:file.name,...(ali&&current.values.target_record_id?{target_record_id:current.values.target_record_id}:{})}),file,{headers:{'Content-Type':file.type}});if(form.value!==current)return;if(!value.upload_id)throw new Error('图片上传未返回有效标识。');if(ali)aliPhotos.value=[value];else photos.value.push(value);}persist();}catch(e){detailError.value=(e as Error).message;}finally{uploading.value=false;}}
async function refreshAliPhotos(current:NoticeForm){try{const v=current.values,q=new URLSearchParams({scope:v.scope,work_type:'change',month:month.value,active_item_id:v.active_item_id||v.target_record_id});const result=await requestJson('/api/workbench/lite-detail?'+q,{cache:'no-store'});if(form.value===current)current.aliPhotos=parseNoticeForm(result.fragments?.detail||'').aliPhotos;}catch{detailError.value='截图操作已完成，预览暂未刷新；其它填写已保留，请稍后重新打开核验。';}}
async function uploadAliNow(){const current=form.value,photo=aliPhotos.value[0];if(!current?.values.target_record_id||!photo||locked.value)return;busy.value=true;detailError.value='';try{await requestJson('/api/change-confirmations/'+encodeURIComponent(current.values.target_record_id)+'/screenshot',{method:'POST',body:JSON.stringify({upload_id:photo.upload_id,local_image_id:photo.local_image_id||''})});aliPhotos.value=[];persist();message.success('确认截图已上传，未发送通告');await refreshAliPhotos(current);}catch(e){detailError.value=(e as Error).message+'；已保留原图片，请先核验结果。';}finally{busy.value=false;}}
function removeAliPhoto(photo:Dict){const current=form.value;if(!current?.values.target_record_id||locked.value)return;dialog.warning({title:'删除此确认截图？',positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{busy.value=true;try{await requestJson('/api/change-confirmations/'+encodeURIComponent(current.values.target_record_id)+'/screenshot?'+new URLSearchParams({file_token:photo.file_token||'',local_image_id:photo.local_image_id||''}),{method:'DELETE'});await refreshAliPhotos(current);}catch(e){detailError.value=(e as Error).message;return false;}finally{busy.value=false;}}});}
function photoInput(event:Event,ali:boolean){const input=event.target as HTMLInputElement;void uploadFiles(input.files,ali);input.value='';}
function pastePhotos(event:ClipboardEvent){if(event.clipboardData?.files.length){event.preventDefault();void uploadFiles(event.clipboardData.files,false);}}
let sourceTimer=0,sourceSequence=0;
function searchSources(q:string){clearTimeout(sourceTimer);sourceTimer=window.setTimeout(()=>loadSources(q),300);}
async function loadSources(q=''){if(!form.value)return;const current=form.value,sequence=++sourceSequence;sourceLoading.value=true;try{const value=await requestJson('/api/workbench/source-options?'+new URLSearchParams({scope:current.values.scope,work_type:current.values.work_type,month:month.value,q}));if(form.value===current&&sequence===sourceSequence)sourceOptions.value=(value.items||[]).map((r:Dict)=>({label:r.title,value:r.source_record_id}));}catch(e){if(form.value===current&&sequence===sourceSequence)detailError.value=(e as Error).message;}finally{if(form.value===current&&sequence===sourceSequence)sourceLoading.value=false;}}
async function associationBound(result:Dict){const v=form.value?.values;if(!v)return;try{sessionStorage.removeItem(formKey);}catch{/* Server identity is authoritative. */}saved.value=snapshot();editing.value=false;busy.value=false;window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);await openRecord({work_type:v.work_type,active_item_id:result.active_item_id||v.active_item_id,record_id:result.source_record_id||result.repair_management_record_id||v.source_record_id||result.target_record_id||v.target_record_id});}
async function bindSource(){if(!form.value||!sourceChoice.value||locked.value)return;const v=form.value.values;busy.value=true;try{if(v.target_record_id)await requestJson('/api/notice-identity/bind',{method:'POST',body:JSON.stringify({scope:v.scope,binding_context:'ongoing',work_type:v.work_type,notice_type:v.notice_type,active_item_id:v.active_item_id,source_record_id:sourceChoice.value,source_month:v.source_month,source_binding_only:true,target_record_id:v.target_record_id,title:v.title,reason:v.reason,start_time:v.start_time,end_time:v.end_time})});v.source_record_id=sourceChoice.value;v.manual_binding_choice='bind';persist();message.success('计划关联已更新');}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
async function resendSopLinks(){if(!form.value?.groupId)return;busy.value=true;try{await requestJson('/api/polling-work-orders/'+encodeURIComponent(form.value.groupId)+'/resend-links',{method:'POST',body:'{}'});message.success('工单链接已提交发送');}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
async function refreshSopStatus(){const current=form.value;if(!current?.groupId||busy.value)return;busy.value=true;try{const result=await requestJson('/api/workbench/polling-delay-status?group_id='+encodeURIComponent(current.groupId),{cache:'no-store'});if(form.value===current)sopStatus.value=result;}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
async function retrySopUpload(){if(!form.value?.groupId||busy.value)return;busy.value=true;try{await requestJson('/api/polling-work-orders/'+encodeURIComponent(form.value.groupId)+'/retry-upload',{method:'POST',body:'{}'});busy.value=false;await refreshSopStatus();}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
function deleteNotice(){
  if(!form.value||locked.value||!props.user.can_delete_business)return;
  const v={...form.value.values},operation=createRepairOperationId('v2-delete');
  dialog.error({title:'删除整条通告',content:'将删除当前通告及对应目标记录：'+v.title,positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{
    busy.value=true;
    try{
      await requestJson('/api/ongoing-items/delete',{method:'POST',body:JSON.stringify({scope:v.scope,work_type:v.work_type,notice_type:v.notice_type,active_item_id:v.active_item_id,source_record_id:v.source_record_id,target_record_id:v.target_record_id,record_id:v.target_record_id||v.record_id,title:v.title,operation_id:operation})});
      try{sessionStorage.removeItem(formKey);}catch{/* A denied storage write must not undo a successful business result. */}
      saved.value=snapshot();editing.value=false;form.value=null;
      window.dispatchEvent(new Event('clipflow-business-changed'));await load(true);
    }catch(e){message.error((e as Error).message);return false;}
    finally{busy.value=false;}
  }});
}
const undoOpen=ref(false),undos=ref<Dict[]>([]),undoLoading=ref(false),undoError=ref(''),undoJob=ref<Dict|null>(null);
const undoPending=computed(()=>!!undoJob.value&&!['failed','success','cancelled'].includes(undoJob.value.phase));
let undoTimer=0;
async function openUndo(){undoOpen.value=true;undoLoading.value=true;undoError.value='';try{const value=await requestJson('/api/notice-undo/available?'+new URLSearchParams({scope:props.scope,work_type:kind.value==='all'?'':kind.value}));undos.value=value.items||[];}catch(e){undoError.value=(e as Error).message;}finally{undoLoading.value=false;}if(undoPending.value)void pollUndo();}
async function pollUndo(){
  window.clearTimeout(undoTimer);
  if(!undoJob.value?.job_id||!undoOpen.value||document.hidden||disposed)return;
  const id=undoJob.value.job_id;
  try{
    const result=await requestJson('/api/jobs/'+encodeURIComponent(id),{cache:'no-store'});
    if(disposed||undoJob.value?.job_id!==id)return;
    undoJob.value=result;
    if(!undoPending.value){if(result.phase==='success'){window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);if(undoOpen.value)void openUndo();}return;}
  }catch{undoError.value='回退进度暂未取得，原任务仍保留，请勿重复回退。';}
  if(!disposed&&undoOpen.value&&!document.hidden)undoTimer=window.setTimeout(pollUndo,4000);
}
function undo(item:Dict){
  if(busy.value||undoPending.value)return;
  dialog.warning({title:'回退上一步',content:rowTitle(item)+' · '+(item.undo_label||''),positiveText:'确认回退',negativeText:'取消',onPositiveClick:async()=>{
    busy.value=true;
    try{
      const result=await requestJson('/api/notice-undo/'+encodeURIComponent(item.undo_id)+'/apply',{method:'POST',body:JSON.stringify({scope:props.scope,operation_id:'undo:'+item.undo_id})});
      if(!result.job_id)throw new Error('未收到回退任务编号，结果尚未确认，请核验原记录。');
      undoJob.value={...result,phase:result.phase||'queued'};void pollUndo();
      rememberNoticeTask(props.user.open_id,result,{...item,scope:props.scope,work_type:item.work_type||kind.value});
    }catch(e){undoError.value=(e as Error).message;return false;}finally{busy.value=false;}
  }});
}
const stopGuard=registerNavigationGuard((_target,proceed)=>{if(!props.active)return true;if(busy.value||uploading.value||saving.value||parsing.value){message.warning('正在提交或上传，请等待结果。');return false;}if(editing.value&&dirty.value&&!job.value){askLeave(proceed);return false;}persist();return true;});
function visibility(){if(document.hidden){listAbort?.abort();window.clearTimeout(timer);window.clearTimeout(undoTimer);}else if(props.active){void load();void pollJob();void pollUndo();}}
function beforeUnload(event:BeforeUnloadEvent){persist();if(busy.value||uploading.value||dirty.value){event.preventDefault();event.returnValue='';}}
let listContext=props.scope+':'+kind.value;
watch(()=>[props.url,props.active,props.scope],()=>{
  persist();detailAbort?.abort();editing.value=false;confirming.value=false;undoOpen.value=false;changeOpen.value=false;recordsOpen.value=false;pasteOpen.value=false;window.clearTimeout(timer);syncAbort?.abort();
  if(!props.active){listAbort?.abort();return;}
  kind.value=params.value.get('work_type')||'all';month.value=params.value.get('month')||month.value;
  if(params.value.has('search'))search.value=params.value.get('search')||'';
  if(params.value.has('specialty'))specialty.value=params.value.get('specialty')||null;
  const nextContext=props.scope+':'+kind.value;
  if(nextContext!==listContext){pages.value={planned:1,ongoing:1};data.value={};listContext=nextContext;}
  if(params.value.has('records_page')||params.value.has('pending_page'))pages.value.planned=pageNumber(params.value.get('records_page')||params.value.get('pending_page'));
  if(params.value.has('ongoing_page'))pages.value.ongoing=pageNumber(params.value.get('ongoing_page'));
  void load();
  if(params.value.has('active_item_id')||params.value.has('record_id'))void openRecord(null,true);
});
onMounted(()=>{void load();if(params.value.has('active_item_id')||params.value.has('record_id'))void openRecord(null,true);if(params.value.has('change_confirmation')&&moreOptions.value.some(o=>o.key==='change'))changeOpen.value=true;document.addEventListener('visibilitychange',visibility);window.addEventListener('beforeunload',beforeUnload);});
watch(undoOpen,value=>{if(!value)window.clearTimeout(undoTimer);});
onBeforeUnmount(()=>{persist();disposed=true;stopGuard();listAbort?.abort();detailAbort?.abort();syncAbort?.abort();window.clearTimeout(timer);window.clearTimeout(sourceTimer);window.clearTimeout(undoTimer);document.removeEventListener('visibilitychange',visibility);window.removeEventListener('beforeunload',beforeUnload);});
</script>
<style>
.v2-notice-review header{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}.v2-notice-review h2{font-size:17px;margin:0}.v2-notice-review-layout{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(220px,1fr);gap:24px}.v2-notice-fulltext{white-space:pre-wrap;overflow-wrap:anywhere;margin:0;background:#f5f7fa;border:1px solid var(--v2-border);padding:20px;border-radius:6px;font-family:inherit;font-size:14px;line-height:1.9}.v2-notice-review-layout h3{font-size:13px;margin-top:0}.v2-notice-review-layout dl{margin:0;display:grid;gap:5px;font-size:13px}.v2-notice-review-layout dt{color:var(--v2-muted)}.v2-notice-review-layout dd{margin:0 0 8px;white-space:pre-wrap;overflow-wrap:anywhere}.v2-notice-review-layout p{font-size:12px;color:var(--v2-muted);line-height:1.6}.v2-notice-entry-copy .v2-notice-attention{gap:5px;color:#956008;font-size:12px}.v2-notice-editor .n-collapse{margin:12px 0}.v2-notice-review{animation:v2-notice-enter 180ms ease-out}.v2-notice-editor>.n-card-header{border-bottom:1px solid var(--v2-border)}.v2-notice-editor>.n-card__footer{border-top:1px solid var(--v2-border)}
@keyframes v2-notice-enter{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
@media(max-width:760px){.v2-notice-review-layout{grid-template-columns:1fr;gap:16px}.v2-notice-fulltext{padding:12px}.v2-notice-editor .n-collapse-item__header{min-height:44px}}
@media(prefers-reduced-motion:reduce){.v2-notice-review{animation:none}}
.v2-notice-columns{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:24px;align-items:start}
.v2-notice-handling{height:min(820px,calc(100dvh - 40px))}
@media(max-width:760px){.v2-notice-handling{height:calc(100dvh - 16px)}}
.v2-notice-section{min-width:0}
.v2-notice-section>header{border-bottom:1px solid var(--v2-border);padding:4px 0 12px}
.v2-notice-section h2{display:flex;align-items:center;font-size:15px;font-weight:600;margin:0}
.v2-notice-section .n-list-item:not(:last-child){border-bottom:1px solid var(--v2-border)}
.v2-notice-mobile-tabs{display:none}
.v2-notice-open{border:0;background:transparent;padding:0;text-align:left;color:inherit;font-family:inherit;font-size:14px;line-height:1.6;font-weight:600;cursor:pointer;overflow-wrap:anywhere}
.v2-notice-open:hover{color:var(--v2-blue)}
.v2-notice-row-actions{display:flex;gap:6px}
.v2-notice-row-actions .n-button{padding:0 9px}
.v2-notice-filters.has-scopes{grid-template-columns:minmax(140px,1fr) 140px 100px 120px auto;max-width:none}
@media(max-width:760px){.v2-notice-filters.has-scopes{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:760px){.v2-notice-columns{display:block}.v2-notice-section.is-mobile-hidden{display:none}.v2-notice-section>header{display:none}.v2-notice-mobile-tabs{display:block}}
.v2-notices{padding-top:20px}.v2-notice-heading{display:flex;gap:14px;align-items:center;justify-content:space-between;margin-bottom:18px}.v2-notice-heading h1{margin:0;font-size:23px;font-weight:650}.v2-notice-heading p{margin:5px 0 0;font-size:12px;color:var(--v2-muted)}.v2-notice-tools,.v2-notice-types{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.v2-notice-types{border-bottom:1px solid var(--v2-border);padding-bottom:12px;margin-bottom:12px}.v2-notice-filters{display:grid;grid-template-columns:minmax(140px,1fr) 110px 140px auto;gap:8px;max-width:860px;margin-bottom:16px}.v2-notice-count{padding:2px 7px;font-size:12px;margin-left:7px;background:#eef2f6;border-radius:5px;font-variant-numeric:tabular-nums}.v2-notice-list{background:transparent}.v2-notice-list .n-list-item{padding:0}.v2-notice-entry{display:flex;align-items:center;gap:16px;width:100%;text-align:left;border:0;background:transparent;padding:12px 10px;color:var(--v2-ink);border-radius:6px;transition:background-color 150ms}.v2-notice-entry:hover{background:#eaf3ff}.v2-notice-entry-copy{flex:1;min-width:0;display:grid;gap:8px}.v2-notice-entry-copy strong{font-weight:600;font-size:15px;line-height:1.6;overflow-wrap:anywhere}.v2-notice-entry-copy>span{display:flex;gap:10px;align-items:center;flex-wrap:wrap;font-size:12px;color:var(--v2-muted)}.v2-notice-entry-state{display:grid;gap:10px;justify-items:end;flex:none}.v2-notice-list-footer{display:flex;justify-content:space-between;align-items:center;padding:16px 0;font-size:12px;color:var(--v2-muted)}.v2-notice-feedback{margin-bottom:12px}.v2-notice-tools .n-button__content,.v2-notice-editor-footer .n-button__content{gap:6px}.v2-notice-skeleton{margin:12px 0}.v2-notice-editor{width:min(1040px,calc(100vw - 40px));max-height:calc(100dvh - 40px)}.v2-notice-editor>.n-card-content{overflow:auto;min-height:0;scrollbar-gutter:stable}.v2-notice-editor-context{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px;font-size:13px}.v2-notice-editor-context .n-radio-group{margin-left:auto}.v2-notice-field-section{border-top:1px solid var(--v2-border);padding:16px 0}.v2-notice-field-section h2{display:flex;gap:12px;font-size:14px;font-weight:600;margin:0 0 14px}.v2-notice-field-section h2 span{font-size:12px;font-weight:400;color:var(--v2-muted)}.v2-notice-field-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 18px}.v2-notice-field-grid .is-wide{grid-column:1/-1}.v2-notice-field-grid .n-date-picker{width:100%}.v2-notice-editor-footer,.v2-dialog-actions{display:flex;justify-content:flex-end;gap:10px;align-items:center;flex-wrap:wrap}.v2-notice-editor-footer>span{flex:1}.v2-notice-confirm{width:min(700px,calc(100vw - 32px));max-height:calc(100dvh - 40px)}.v2-notice-confirm>.n-card-content{overflow:auto;min-height:0}.v2-notice-confirm dl{display:grid;grid-template-columns:110px minmax(0,1fr);font-size:14px;line-height:1.8;gap:10px}.v2-notice-confirm dt{color:var(--v2-muted)}.v2-notice-confirm dd{margin:0;white-space:pre-wrap;overflow-wrap:anywhere}.v2-notice-loading{display:flex;min-height:240px;justify-content:center;align-items:center}.v2-notice-upload{display:flex;align-items:center;gap:10px;min-height:66px;padding:12px;border:1px dashed #b9cce1;border-radius:8px;background:#f6f9fd;position:relative;color:#315b83;cursor:pointer}.v2-notice-upload input{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%}.v2-notice-photos{display:flex;flex-wrap:wrap;gap:10px}.v2-notice-photos figure{position:relative;margin:0 0 12px;border:1px solid var(--v2-border);border-radius:6px;padding:6px;width:120px;overflow:hidden}.v2-notice-photos img{width:100%;height:80px;object-fit:contain}.v2-notice-photos span{display:block;overflow:hidden;text-overflow:ellipsis;font-size:11px;white-space:nowrap}.v2-notice-photos button{position:absolute;right:4px;top:4px;background:white}.v2-notice-binding{display:flex;gap:8px;margin-top:12px}.v2-notice-binding .n-select{flex:1}.v2-notice-editor .n-alert{margin-bottom:12px}
@media(max-width:760px){.v2-notices{padding-top:12px}.v2-notice-heading{align-items:flex-start;gap:10px;flex-wrap:wrap;margin-bottom:12px}.v2-notice-heading h1{font-size:20px}.v2-notice-tools{width:100%}.v2-notice-tools .n-button{flex:1}.v2-notice-types{flex-wrap:nowrap;overflow-x:auto;margin-inline:-2px;padding:0 2px 10px}.v2-notice-types .n-button{flex:none;min-height:44px}.v2-notice-filters{grid-template-columns:90px minmax(0,1fr) 60px;gap:6px;margin-bottom:10px}.v2-notice-filters>.n-input{grid-column:1/-1}.v2-notice-entry{padding:13px 0;gap:10px}.v2-notice-entry-copy strong{font-size:14px}.v2-notice-entry-copy>span{gap:6px}.v2-notice-list-footer{flex-wrap:wrap;gap:12px}.v2-notice-editor{width:calc(100vw - 16px);max-height:calc(100dvh - 16px)}.v2-notice-editor>.n-card-content{padding:12px}.v2-notice-editor>.n-card-header{padding:16px 48px 12px 12px}.v2-notice-field-grid{grid-template-columns:minmax(0,1fr)}.v2-notice-editor-context .n-radio-group{margin-left:0}.v2-notice-editor-footer>.n-button{min-height:44px}.v2-notice-confirm dl{grid-template-columns:86px minmax(0,1fr);font-size:13px}.v2-notice-binding{flex-wrap:wrap}.v2-notice-binding .n-select{flex-basis:100%}.v2-notice-heading .n-button__content{font-size:12px}}
@media(prefers-reduced-motion:reduce){.v2-notice-entry{transition:none}}
</style>
