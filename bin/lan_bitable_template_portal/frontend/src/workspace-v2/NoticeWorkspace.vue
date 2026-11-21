<template>
  <section class="notice-desk" aria-label="通告办理" data-v2-inline-feedback>
    <template v-if="!recordOnly">
      <header v-if="kind!=='event'" class="notice-desk-head">
        <div class="notice-desk-identity">
          <span class="desk-mark" aria-hidden="true"><RadioTower :size="24" /></span>
          <div><h1>通告办理</h1><small>{{ scopeLabel }}</small></div>
        </div>
        <div class="desk-commands">
          <NButton data-v2-navigation aria-label="查看历史通告" @click="historyOpen=true"><History :size="16" />历史通告</NButton>
          <NDropdown :options="moreOptions" trigger="click" @select="more"><NButton data-v2-navigation aria-label="更多办理"><Ellipsis :size="16" />更多办理</NButton></NDropdown>
          <NDropdown :options="refreshOptions" trigger="click" @select="refreshSource"><NButton :loading="loading||syncing" data-v2-navigation aria-label="刷新通告"><RefreshCw :size="16" />刷新</NButton></NDropdown>
          <NDropdown :options="newOptions" trigger="click" @select="newNotice"><NButton type="primary" class="desk-primary" data-v2-navigation aria-label="新建通告"><Plus :size="17" />新建通告</NButton></NDropdown>
        </div>
      </header>
      <div v-if="types.length>1 || kind!=='event'" class="desk-band">
        <nav v-if="types.length>1" ref="typeNavigation" class="desk-types" aria-label="通告类型">
          <button v-for="entry in types" :key="entry.id" type="button" data-v2-navigation
            :aria-pressed="kind===entry.id" @click="setKind(entry.id)">{{ entry.label }}</button>
        </nav>
        <div v-if="kind!=='event'" class="desk-scope">
          <WidgetScopePicker v-if="!selectedScopes&&buildingOptions.length>1"
            :value="selectedBuildings||(['ALL','CAMPUS'].includes(scope)?['ALL']:[scope])" :options="buildingOptions" label="通告楼栋" @change="changeBuildings" />
          <span data-v2-layout-tools="notices"></span>
        </div>
      </div>
      <NAlert v-if="error" role="alert" type="error" :bordered="false">{{ error }} <NButton text @click="load(true)">重新读取</NButton></NAlert>
      <p v-else-if="data.partial_buildings && loading" class="notice-progress" role="status">已显示部分楼栋 · 其余读取中…</p>
      <NAlert v-if="syncing" type="info" :bordered="false"><NSpin size="small" /> 正在同步源表，当前仍可查看已加载的通告。</NAlert>
      <EventNoticeBoard v-if="kind==='event'" :scope="scope" :active="active" />
      <template v-else>
        <div class="desk-switch" role="group" aria-label="当前列表">
          <button v-for="lane in lanes" :key="lane.id" type="button" :aria-pressed="list===lane.id" @click="list=lane.id">
            {{ lane.label }} <b>{{ total(lane.id) }}</b>
          </button>
        </div>
        <PageCanvas page="notices" label="通告办理" :active="active"
          :blocks="lanes.map(lane=>({id:lane.id,title:lane.label,w:6,h:11,minW:4,minH:5}))">
          <template v-for="lane in lanes" :key="lane.id" #[lane.id]>
            <section class="desk-lane" :class="{'desk-hidden':list!==lane.id}" :data-list="lane.id" :aria-label="lane.label">
              <header class="lane-head">
                <div class="lane-caption">
                  <span class="lane-icon" aria-hidden="true"><CalendarDays v-if="lane.id==='planned'" :size="21" /><Radio v-else :size="21" /></span>
                  <div><h2>{{ lane.label }}</h2><small>{{ lane.id==='planned' ? applied.month + (applied.specialty?' · '+applied.specialty:' · 全部专业') : scopeLabel }}</small></div>
                </div>
                <span class="lane-count"><NSpin v-if="loading" :size="14" /><b>{{ total(lane.id) }}</b> 条</span>
              </header>
              <form v-if="lane.id==='planned'" class="desk-filters" aria-label="仅筛选计划通告" @submit.prevent="filter">
                <NInput v-model:value="search" clearable placeholder="搜索计划名称" :input-props="{'aria-label':'搜索计划通告'}" @clear="search='';filter()"><template #prefix><Search :size="16" /></template></NInput>
                <NSelect v-model:value="month" :options="months" aria-label="计划月份" />
                <NSelect v-model:value="specialty" clearable :options="specialties" placeholder="全部专业" aria-label="计划专业" />
                <NButton attr-type="submit" :loading="loading" aria-label="筛选计划通告">筛选</NButton>
                <small v-if="filtersDirty" role="status">点击“筛选”应用修改</small>
              </form>
              <div v-if="loading&&!lane.rows.length" class="desk-loading" role="status" aria-label="正在读取通告"><NSkeleton v-for="n in 3" :key="n" height="76px" /></div>
              <ul v-else-if="lane.rows.length" class="desk-records" :aria-busy="loading">
                <li v-for="row in lane.rows" :key="row.work_type+':'+rowKey(row)">
                  <NoticeDeskRecord :row="row" :lane="lane.id" :title="rowTitle(row)" :type="typeLabel(row.work_type)"
                    :building="rowBuildingLabel(row)" :attention="rowAttention(row,lane.id)"
                    :started="rowStarted(row)" :ended="rowEnded(row)" :unknown="rowUnknown(row)"
                    @open="value=>openRecord(row,false,value)" @bind="openRecord(row,false,'','','',true)" />
                </li>
              </ul>
              <div v-else class="desk-empty" role="status"><Inbox :size="24" /><p>{{ error?'通告读取失败，不能判断为零条':lane.id==='ongoing'?'当前没有进行中通告':'当前筛选下没有计划通告' }}</p></div>
              <footer v-if="lane.total>20 || pages[lane.id]>1" class="lane-pagination"><small>本页 {{ lane.rows.length }} 条</small><NPagination :page="pages[lane.id]" :page-size="20"
                :item-count="lane.total" :page-slot="3" :disabled="loading || !!data.partial_buildings" @update:page="value=>changePage(lane.id,value)" /></footer>
            </section>
          </template>
        </PageCanvas>
      </template>
    </template>
    <component v-if="!recordOnly||editing" :is="recordOnly?NCard:NModal" v-bind="recordOnly?{}:{show:editing,preset:'card',maskClosable:false,closeOnEsc:!busy&&!uploading&&!saving,closable:!busy&&!uploading&&!saving}"
      class="v2-notice-editor desk-editor" :class="{'is-inline-record':recordOnly}" data-v2-inline-feedback :aria-label="form?.values.title || '填写通告'" :bordered="false" @update:show="value=>{if(!value)closeEditor()}">
      <template v-if="!recordOnly" #header>
        <div class="notice-editor-title">
          <small>{{ form ? typeLabel(form.values.work_type)+' · '+selectedBuildingLabel : '通告办理' }}</small>
          <strong>{{ form?.values.title || '填写通告' }}</strong>
        </div>
      </template>
      <NSpin v-if="detailLoading" class="v2-notice-loading" description="正在读取通告填写内容" />
      <NAlert v-if="detailError" type="error" role="alert" :bordered="false">{{ detailError }}<NButton v-if="issueTarget&&!job&&!ambiguous" text data-v2-navigation @click="focusIssue">{{ issueTarget.label }}</NButton></NAlert>
      <NAlert v-if="draftWarning" type="warning" :bordered="false">{{ draftWarning }}</NAlert>
      <template v-if="form&&!detailLoading">
        <NAlert v-if="bindingFinished" type="success" role="status" :bordered="false">已绑定原通告，原通告已结束。</NAlert>
        <NAlert v-else-if="verifyBeforeStart" type="warning" :bordered="false">计划已在进行中，请核对并关联已发通告。</NAlert>
        <NAlert v-else-if="!form.actions.length" type="warning" :bordered="false">此通告当前只可查看。</NAlert>
        <NAlert v-if="(confirming||job||ambiguous)&&cycleWarnings.length" type="warning" :bordered="false" data-cycle-warning-summary><p v-for="warning in cycleWarnings" :key="warning">{{ warning }}</p></NAlert>
        <NoticeDeskReceipt v-if="job||ambiguous" :job="job" :title="taskTitle" :ambiguous="ambiguous" :succeeded="succeeded"
          :terminal="terminal" :event-waiting="eventWaiting" :delivery-label="deliveryLabel">
          <div class="v2-notice-tools"><NButton v-if="job?.phase==='failed'&&job.error_retryable&&job.can_retry!==false" type="primary" :disabled="!form.values.dingtalk_robot_ids?.length" :loading="busy" data-v2-submit @click="retryJob">重试原任务</NButton><NButton v-else-if="job?.phase==='failed'&&job.error_category==='field_or_payload'" type="primary" :disabled="busy" @click="resetRejected">返回填写</NButton><NButton v-if="ambiguous" :disabled="!form.values.dingtalk_robot_ids?.length" :loading="busy" data-v2-submit @click="send">核验并重试原提交</NButton><NButton v-if="job&&!terminal" data-v2-navigation @click="pollJob">查询进度</NButton><NButton v-if="job" data-v2-navigation @click="showTaskRecords">查看办理记录</NButton>
            <NButton class="v2-notice-close" secondary data-v2-navigation :disabled="busy||uploading||saving" @click="closeEditor">{{job&&!terminal?'后台处理，返回列表':'返回列表'}}</NButton>
          <NButton v-if="succeeded&&nextRecord" type="primary" data-v2-navigation @click="openRecord(nextRecord)">办理下一条</NButton>
          <NButton v-if="form.canDelete&&!confirming" type="error" text :loading="busy" :disabled="locked" @click="deleteNotice"><Trash2 :size="16" />删除通告</NButton>
          </div>
        </NoticeDeskReceipt>
        <section v-if="confirming" class="desk-review" aria-label="发送前确认">
          <header class="editor-section-title"><Send :size="18" /><h2>发送前确认</h2><span>{{ actionLabel(action) }}</span></header>
          <NAlert v-if="form.values.sync_maintenance_target==='1'||form.values.sync_maintenance_target===true" type="warning" :bordered="false">本次变更同时写入关联的维保目标记录。</NAlert>
          <div class="review-columns">
            <section class="review-paper"><h3>通告全文</h3><pre tabindex="0" aria-label="待发送通告全文">{{ noticePreview(form,action) }}</pre></section>
            <aside class="review-summary">
              <h3>发送范围</h3>
              <dl><dt>楼栋</dt><dd>{{ selectedBuildingLabel }}</dd><dt>钉钉群</dt><dd>{{ selectedDingNames.join('、') || '未选择（必选）' }}</dd></dl>
              <h3>本次修改</h3>
              <dl v-if="changedFields.length"><template v-for="field in changedFields" :key="field.key"><dt>{{ field.label }}</dt><dd>{{ form.values[field.key] ?? '—' }}</dd></template></dl>
              <p v-else>没有修改已加载字段</p>
              <p v-if="form.photoRequired">现场照片 · 已有 {{ Number(form.values.site_photo_count||0) }} 张，新增 {{ photos.length }} 张</p>
              <p v-if="form.values.work_type==='change'">阿里确认截图 · 新增 {{ aliPhotos.length }} 张</p>
              <p v-if="form.sop&&action==='start'">本次工单 · {{ sop.exempt?'不使用工单':selectedSopLabel }}</p>
              <dl v-if="form.sop&&action==='start'&&!sop.exempt"><dt>操作人</dt><dd>{{ (sop.operator_record_ids??[sop.operator_record_id]).map(sopPersonName).join('、') }}</dd><dt>审核人</dt><dd>{{ sopPersonName(sop.reviewer_record_id) }}</dd></dl>
            </aside>
          </div>
        </section>
        <div v-if="!job&&!ambiguous" v-show="!confirming" class="editor-sheet">
          <aside class="editor-routing">
            <header class="editor-section-title"><Link2 :size="18" /><h2>关联与接收</h2></header>
            <div data-notice-section="relations"><NoticeRelations ref="relations" :key="formKey" :form="form" :action="action" :locked="locked" :verify-plan="verifyBeforeStart" :finished="bindingFinished" @busy="busy=$event" @bound="associationBound" /></div>
        <NoticePlanBinding v-if="form.detailMode==='ongoing'||form.values.manual_binding_required||form.sourceOptions.length" :form="form" :expanded="sourceExpanded" v-model:choice="sourceChoice" :options="sourceOptions" :loading="sourceLoading" :locked="locked" @toggle="toggleSource" @mode="chooseSourceMode" @search="searchSources" @confirm="bindSource" />
          <NoticeDingTalkPicker v-if="!verifyBeforeStart&&!bindingFinished" v-model="form.values.dingtalk_robot_ids" :work-type="form.values.work_type" :notice-type="form.values.notice_type" :target-record-id="form.values.target_record_id" :buildings="form.values.building_codes" :disabled="locked" @selected-names="selectedDingNames=$event" />
            <div v-if="form.values.target_record_id" role="region" aria-label="通告推荐标签"><NoticeAlertTags :record-id="form.values.target_record_id" :work-type="form.values.work_type" /></div>
          </aside>
        <NForm label-placement="top" class="v2-notice-pane-form" :disabled="locked" @submit.prevent="preview" @paste="pastePhotos">
            <header class="editor-section-title"><ClipboardList :size="18" /><h2>{{ bindingFinished?'当前填写':'办理内容' }}</h2>
              <small v-if="!verifyBeforeStart&&!bindingFinished" role="status">{{ cycleWarnings.length?'维保周期不一致':missingFields.length?'待补充 '+missingFields.length+' 项':'必填字段已齐全' }}</small>
            </header>
            <NoticeFields :fields="editingFields" :values="form.values" :cycle-rules="form.cycleRules" :locked="locked" :attempted="attempted" compact><NFormItem v-if="action==='start'" label="所属楼栋" required data-notice-section="buildings" :show-feedback="false"><NCheckboxGroup v-model:value="form.values.building_codes" :disabled="locked"><NCheckbox v-for="s in buildingOptions" :key="s.value" :value="s.value">{{ s.label }}</NCheckbox></NCheckboxGroup></NFormItem></NoticeFields>
          <template v-if="action==='start'&&!verifyBeforeStart&&!bindingFinished">
            <section v-if="form.values.source_record_id&&!form.values.target_record_id" class="v2-notice-field-section" data-notice-section="memory"><NoticeMemorySuggestions :key="formKey" :form="form" :month="applied.month" :action="action" :locked="locked" @applied="persist" /></section>
            <section v-if="form.sop" class="v2-notice-field-section" data-notice-section="sop"><h2>本次工单</h2><LighthouseNoticeSop id="v2-notice-sop" :field="sopField" v-model="sop" :disabled="locked" @load-options="loadSops($event.q)" /></section>
          </template>
          <section v-if="form.groupId" class="v2-notice-field-section" data-notice-section="workorder"><h2>已关联工单</h2><NAlert v-if="sopStatus" :type="sopStatus.state==='completed'?'success':'info'" :bordered="false">{{ sopStatus.error || (sopStatus.state==='completed'?'工单已完成':sopStatus.state==='upload_pending'?'步骤已全部完成，工单表格正在上传':'工单执行中') }}<span v-if="sopStatus.enabled&&!sopStatus.ready"> · 延时核验尚未满足，剩余 {{ sopStatus.remaining_seconds }} 秒</span></NAlert><div class="v2-notice-tools"><NButton v-for="link in form.sopLinks" :key="link.url" tag="a" :href="workspaceUrl(link.url)" target="_blank" rel="noopener noreferrer">{{ link.label }}</NButton><NButton :loading="busy" :disabled="locked" @click="refreshSopStatus">刷新工单状态</NButton><NButton :loading="busy" :disabled="locked" @click="resendSopLinks">重发操作人及审核人链接</NButton><NButton v-if="sopStatus?.state==='upload_pending'" :loading="busy" :disabled="locked" @click="retrySopUpload">重试上传工单表格</NButton></div></section>
          <div v-if="form.photoRequired||form.values.work_type==='change'" class="v2-notice-photo-columns">
          <section v-if="form.photoRequired" class="v2-notice-field-section" data-notice-section="photos"><h2>现场照片<span>{{ Number(form.values.site_photo_count||0) }} 张已上传</span></h2><NoticePhotoUpload ref="siteUpload" :form="form" v-model="photos" :locked="locked" @busy="uploadBusy" @update:model-value="persistAfterUpload"><figure v-for="photo in form.photos" :key="photo.local_image_id||photo.file_token"><NImage v-if="safePhoto(photo)" :src="safePhoto(photo)" :alt="photo.file_name||'已上传现场照片'" width="108" /><span>{{ photo.file_name || '已有现场照片' }}</span><small>已保存到通告</small></figure></NoticePhotoUpload></section>
          <section v-if="form.values.work_type==='change'" class="v2-notice-field-section" data-notice-section="ali"><h2>阿里确认截图</h2><NoticePhotoUpload ref="aliUpload" :form="form" v-model="aliPhotos" :locked="locked" ali @busy="uploadBusy" @update:model-value="persistAfterUpload"><figure v-for="photo in form.aliPhotos" :key="photo.local_image_id||photo.file_token"><NImage v-if="safePhoto(photo)" :src="safePhoto(photo)" width="108" :alt="photo.file_name||'已上传确认截图'" /><span>{{ photo.file_name || '已上传' }}</span><small>已保存到通告</small><NButton quaternary circle :disabled="locked" aria-label="删除已上传确认截图" @click="removeAliPhoto(photo)"><Trash2 :size="15" /></NButton></figure></NoticePhotoUpload><NButton v-if="form.values.target_record_id&&aliPhotos.length" :disabled="locked" :loading="busy" @click="uploadAliNow">仅上传确认截图</NButton></section>
          </div>
        </NForm>
        </div>
      </template>
      <template v-if="!job&&!ambiguous" #footer>
        <div class="v2-notice-editor-footer" :class="{'is-review':confirming}">
          <NButton v-if="form?.canDelete&&!confirming" type="error" text :loading="busy" :disabled="locked" @click="deleteNotice"><Trash2 :size="16" />删除通告</NButton>
          <span></span>
          <NButton v-if="confirming" data-v2-navigation :disabled="busy" @click="confirming=false">返回修改</NButton>
          <NButton v-else-if="!recordOnly" class="v2-notice-close" secondary data-v2-navigation :disabled="busy||uploading||saving" @click="closeEditor"><X :size="18" />关闭</NButton>

          <div v-if="form&&!job&&!ambiguous&&!bindingFinished" class="v2-notice-submit-group" data-notice-section="action">
            <NRadioGroup v-if="!confirming&&!verifyBeforeStart" v-model:value="action" :disabled="locked" size="small" role="radiogroup" aria-label="本次操作" @update:value="changeAction"><NRadioButton v-for="value in form.actions" :key="value" :value="value">{{ actionLabel(value) }}</NRadioButton></NRadioGroup>
            <NButton v-if="confirming" type="primary" :disabled="!form.values.dingtalk_robot_ids?.length" :loading="busy" data-v2-submit @click="send">确认发送{{ actionLabel(action) }}</NButton>
            <NButton v-else-if="verifyBeforeStart||needsOriginalNotice(form,action)" type="primary" :disabled="locked" @click="locateOriginal"><Search :size="16" />查找原通告</NButton>
            <NButton v-else type="primary" :disabled="locked||!form.actions.length||!form.values.dingtalk_robot_ids?.length" :loading="busy" @click="preview"><Send :size="16" />预览并确认{{ actionLabel(action) }}</NButton>
          </div>
        </div>
      </template>
    </component>
    <NModal :show="pasteOpen" preset="card" class="v2-notice-confirm desk-dialog" title="粘贴通告原文" :mask-closable="false" :closable="!parsing" :close-on-esc="!parsing" @update:show="value=>pasteOpen=value"><NAlert v-if="pasteError" role="alert" type="error" :bordered="false">{{ pasteError }}</NAlert><NInput v-model:value="pasteText" type="textarea" :autosize="{minRows:8,maxRows:16}" maxlength="30000" :input-props="{'aria-label':'通告原文'}" :disabled="parsing" placeholder="粘贴完整通告，解析后核对字段再发送" @paste="pasteNoticeText" /><template #footer><div class="v2-dialog-actions"><NButton :disabled="parsing" @click="pasteOpen=false">取消</NButton><NButton type="primary" :loading="parsing" :disabled="!pasteText.trim()" @click="parsePasted">解析并填写</NButton></div></template></NModal>
    <NoticeChangeConfirmations v-if="changeOpen" :scope="scope" :focus-id="params.get('change_confirmation')||''" @close="changeOpen=false" @changed="load(true)" />
    <NoticeHistory workspace ref="historyDialog" :scope="scope" :work-type="kind" :show="historyOpen" @close="historyOpen=false" @changed="load(true)" />
    <NoticeTaskRecords v-if="recordsOpen" :user-id="user.open_id" :scope="scope" :modules="user.modules||[]" @close="recordsOpen=false" @changed="load(true)" />
    <NModal v-model:show="undoOpen" preset="card" class="v2-notice-confirm desk-dialog" title="近三天可回退" :bordered="false" :closable="!busy" :mask-closable="!busy" :close-on-esc="!busy"><NSpin :show="undoLoading"><NAlert v-if="undoError" role="alert" type="error" :bordered="false">{{ undoError }}</NAlert><NAlert v-if="undoJob" role="status" :type="undoJob.phase==='failed'?'error':undoJob.phase==='success'?'success':'info'" :bordered="false"><NSpin v-if="undoPending" size="small" />{{ undoJob.error || (undoJob.phase==='success'?'回退已完成':'回退任务已受理，正在处理') }}<NButton v-if="undoPending" text @click="pollUndo">查询进度</NButton></NAlert><ul v-if="undos.length" class="undo-list"><li v-for="item in undos" :key="item.undo_id"><History :size="18" /><div><strong>{{ item.title }}</strong><small>{{ item.undo_label }} · {{ item.undo_created_at }}</small></div><NButton :disabled="busy||undoPending" @click="undo(item)">回退上一步</NButton></li></ul><NEmpty v-if="!undoLoading&&!undos.length" description="暂无可回退记录" /></NSpin></NModal>
  </section>
</template>
<script setup lang="ts">
// v2-compact-adapted: this page supplies its own command area and PageCanvas toolbar host.
import NoticeDeskRecord from './NoticeDeskRecord.vue';
import NoticeDeskReceipt from './NoticeDeskReceipt.vue';
import { computed,h,nextTick,onBeforeUnmount,onMounted,ref,watch } from 'vue';
import PageCanvas from './PageCanvas.vue';
import { NAlert,NButton,NCard,NCheckbox,NCheckboxGroup,NDropdown,NEmpty,NForm,NFormItem,NImage,NInput,NModal,NPagination,NRadioButton,NRadioGroup,NSelect,NSkeleton,NSpin,useDialog,useMessage } from 'naive-ui';
import { CalendarDays,ClipboardList,Inbox,Link2,Radio,RadioTower,Ellipsis,History,Plus,RefreshCw,Search,Send,Trash2,X } from 'lucide-vue-next';
import { ApiError,requestJson,refreshRemoteSourceAndWait,type Dict } from './client';
import { modules,NOTICE_IDS,beijingDate } from './modules';
import { navigate,registerNavigationGuard } from './navigation';
import { workspaceUrl } from './version';
import { trackButtonRequest } from './actionMotion';
import { parseNoticeForm,noticeCommand,noticePreview,validateNotice,needsOriginalNotice,endProgress,normalizePastedNotice,noticeDraftContext,restoreNoticeEdits,noticeCacheKey,type NoticeForm } from './noticeForm';
import { noticeCycleWarnings } from '../noticeCycleWarnings';
import NoticePlanBinding from './NoticePlanBinding.vue';
import NoticeFields from './NoticeFields.vue';
import NoticeMemorySuggestions from './NoticeMemorySuggestions.vue';
import { noticeListStatus } from './noticeListMeta';
import NoticePhotoUpload from './NoticePhotoUpload.vue';
import { noticeBuildings,noticeIssueTarget,type NoticeIssue } from './noticeExperience';
import NoticeRelations from './NoticeRelations.vue';
import NoticeChangeConfirmations from './NoticeChangeConfirmations.vue';
import NoticeTaskRecords from './NoticeTaskRecords.vue';
import NoticeHistory from '../components/NoticeHistory.vue';
import EventNoticeBoard from '../components/EventNoticeBoard.vue';
import NoticeDingTalkPicker from '../components/NoticeDingTalkPicker.vue';
import {rememberNoticeTask} from './noticeTasks';
import LighthouseNoticeSop from '../components/LighthouseNoticeSop.vue';
import NoticeAlertTags from '../components/NoticeAlertTags.vue';
import { createRepairOperationId } from '../repairManagementUtils';
import { noticeIsEnded } from '../mopSelectionUtils';
import {readNoticeBuildings} from './noticeBuildingPages';
import WidgetScopePicker from './dashboard/WidgetScopePicker.vue';
const props=defineProps<{url:string;scope:string;scopes:{value:string;label:string}[];selectedScopes?:string[];user:Dict;active:boolean;recordOnly?:boolean}>();
const emit=defineEmits<{recordClose:[]}>();
const dialog=useDialog(),message=useMessage();
const params=computed(()=>new URL(props.url,location.origin).searchParams);
const types=computed(()=>{
  const granted = props.user.modules || [];
  const allowedNotices = modules.filter(module => NOTICE_IDS.includes(module.id) && granted.includes(module.id));
  return [
    ...(granted.includes('event') ? [{id:'event',label:'事件通告'}] : []),
    ...(allowedNotices.length ? [{id:'all',label:'全部'}, ...allowedNotices] : []),
  ];
});
const kind=ref(params.value.get('work_type')||'all'),list=ref('planned'),month=ref(params.value.get('month')||Number(beijingDate().slice(5,7))+'月'),search=ref(params.value.get('search')||''),specialty=ref(params.value.get('specialty')||null);
const typeNavigation=ref<HTMLElement|null>(null);
function revealActiveType(){
  const nav=typeNavigation.value,button=nav?.querySelector<HTMLElement>('[aria-pressed="true"]');
  if(!nav||!button||nav.scrollWidth<=nav.clientWidth)return;
  const bounds=nav.getBoundingClientRect(),selected=button.getBoundingClientRect();
  const delta=selected.left<bounds.left?selected.left-bounds.left:selected.right>bounds.right?selected.right-bounds.right:0;
  if(delta)nav.scrollLeft+=delta;
}
watch([kind,typeNavigation,()=>props.active],()=>{void nextTick(revealActiveType);},{flush:'post'});
const applied=ref({month:month.value,search:search.value,specialty:specialty.value});
const filtersDirty=computed(()=>month.value!==applied.value.month||search.value!==applied.value.search||specialty.value!==applied.value.specialty);
const months=Array.from({length:12},(_,i)=>({label:(i+1)+'月',value:(i+1)+'月'})),specialties=['电气','暖通','弱电','消防'].map(value=>({label:value,value}));
const scopeOptions=computed(()=>props.scopes.map(s=>({...s,label:s.value==='ALL'?'全部楼栋':s.label})));
const selectedBuildings=computed(()=>{const requested=props.selectedScopes||params.value.get('buildings')?.split(',');return requested?[...new Set(requested.filter(value=>buildingOptions.value.some(option=>option.value===value)))]:null;});
const scopeLabel=computed(()=>selectedBuildings.value?selectedBuildings.value.map(value=>scopeOptions.value.find(option=>option.value===value)?.label||value).join('、'):scopeOptions.value.find(s=>s.value===props.scope)?.label||props.scope);
const buildingOptions=computed(()=>props.scopes.filter(s=>['110',...'ABCDEH'].includes(s.value)));
const sopTypes=computed(()=>types.value.filter(t=>['maintenance','polling','adjust'].includes(t.id)).map(t=>({value:t.id,label:t.label})));
const newOptions=computed(()=>[...types.value.filter(t=>t.id!=='all'&&(kind.value==='all'||t.id===kind.value)).map(t=>({key:t.id,label:'手动填写'+t.label})),{type:'divider',key:'divider'},{key:'paste',label:'粘贴原文解析'}]);
const moreOptions=computed(()=>[{key:'records',label:'本机办理记录'},{key:'undo',label:'近三天回退'},...(sopTypes.value.length?[{key:'sop',label:'SOP与工单模板'}]:[]),...((props.user.role==='admin'||props.user.is_admin===true)&&types.value.some(t=>t.id==='change')?[{key:'change',label:'变更确认'}]:[]),...((props.user.modules||[]).includes('engineer_mop')?[{key:'mop',label:'维护单管理'}]:[]),...((props.user.modules||[]).includes('accounts')?[{key:'memory',label:'历史记忆管理'}]:[])]);
const refreshOptions=computed(()=>[{key:'page',label:'刷新本页'},...types.value.filter(t=>['maintenance','change','repair'].includes(t.id)).map(t=>({key:t.id,label:'同步'+t.label+'源表'}))]);
const changeOpen=ref(false),pasteOpen=ref(false),pasteText=ref(''),pasteError=ref(''),parsing=ref(false),syncing=ref(false);
const recordsOpen=ref(false),historyOpen=ref(false);
const historyDialog=ref<InstanceType<typeof NoticeHistory>|null>(null);
function openRecords(){recordsOpen.value=true;}
let syncAbort:AbortController|undefined;
function more(key:string){if(key==='paste')newNotice('paste');else if(key==='undo')void openUndo();else if(key==='sop')navigate('/?'+new URLSearchParams({view:'sops',scope:props.scope,work_type:kind.value}));else if(key==='change')changeOpen.value=true;else if(key==='records')void openRecords();else navigate((key==='mop'?'/engineer/mop':'/admin/history-memory')+'?scope='+encodeURIComponent(props.scope));}
function newNotice(key:string){if(key==='event'){setKind('event');return;}if(key==='paste'){pasteError.value='';pasteOpen.value=true;}else void openRecord(null,false,'start','',key);}
async function refreshSource(key:string){if(key==='page'){void load(true);return;}if(syncing.value||!refreshOptions.value.some(t=>t.key===key))return;syncing.value=true;error.value='';syncAbort=new AbortController();try{await refreshRemoteSourceAndWait(key as 'maintenance'|'change'|'repair','/api/'+key+'-refresh?scope='+encodeURIComponent(props.scope),{scope:props.scope,signal:syncAbort.signal});await load(true);}catch(e){if(!syncAbort.signal.aborted)error.value=(e as Error).message;}finally{syncing.value=false;}}
function pasteNoticeText(event:ClipboardEvent){
  const text=event.clipboardData?.getData('text/plain'),input=event.target;
  if(!text||!(input instanceof HTMLTextAreaElement)||parsing.value)return;
  event.preventDefault();
  const before=input.value.slice(0,input.selectionStart),after=input.value.slice(input.selectionEnd);
  pasteText.value=normalizePastedNotice(before+text+after).slice(0,30000);
  const cursor=Math.min(normalizePastedNotice(before+text).length,pasteText.value.length);
  void nextTick(()=>{if(document.activeElement===input)input.setSelectionRange(cursor,cursor);});
}
async function parsePasted(){if(parsing.value)return;pasteText.value=normalizePastedNotice(pasteText.value);if(!pasteText.value.trim())return;parsing.value=true;pasteError.value='';try{const body=new FormData();body.append('scope',props.scope);body.append('month',applied.value.month);body.append('work_type',kind.value==='all'?types.value.find(t=>NOTICE_IDS.includes(t.id))?.id||'maintenance':kind.value);body.append('paste_text',pasteText.value);const result=await requestJson('/workbench-lite/parse',{method:'POST',body});parseNoticeForm(result.fragments?.detail||'');pasteOpen.value=false;await openRecord(null,false,'',result.fragments.detail);}catch(e){pasteError.value=(e as Error).message;}finally{parsing.value=false;}}
const pageNumber=(value:string|null)=>Math.max(1,Math.trunc(Number(value)||1));
const pages=ref<Record<string,number>>({planned:pageNumber(params.value.get('records_page')||params.value.get('pending_page')),ongoing:pageNumber(params.value.get('ongoing_page'))}),data=ref<Dict>({}),loading=ref(false),error=ref('');
const lanes=computed(()=>[{id:'planned',label:'计划通告',rows:(data.value.records||[]) as Dict[],total:Number(data.value.records_pagination?.total??data.value.records?.length??0)}, {id:'ongoing',label:'进行中',rows:(data.value.ongoing||[]) as Dict[],total:Number(data.value.ongoing_pagination?.total??data.value.ongoing?.length??0)}]);
function total(mode:string) {
  const pagination = data.value[mode==='ongoing'?'ongoing_pagination':'records_pagination'];
  if (pagination?.total != null) return (pagination.partial?'已读取 ':'')+String(pagination.total);
  const rows = data.value[mode==='ongoing'?'ongoing':'records'];
  return Array.isArray(rows) ? '本页 '+rows.length : '—';
}
const typeLabel=(value:string)=>modules.find(m=>m.id===value)?.label||'通告';
function rowBuildingLabel(row:Dict) {
  const buildings = noticeBuildings(row);
  return buildings.length ? buildings.map(code => scopeOptions.value.find(option => option.value === code)?.label || (code === '110' ? '110站' : code+'楼')).join('、') : String(row.building || row.scope || scopeLabel.value);
}
const rowKey=(row:Dict)=>String(row.active_item_id||row.record_id||row.id);
const rowTitle=(row:Dict)=>String(row.title||row.name||row.display_fields?.['维护总项']||row.display_fields?.['名称']||'未命名通告');
const rowEnded=(row:Dict)=>noticeIsEnded({status:lanes.value[0].rows.includes(row)?noticeListStatus(row,'planned').label:row.status_label||row.status});
const rowUnknown=(row:Dict)=>noticeListStatus(row,'planned').label==='状态未取得';
const rowStarted=(row:Dict)=>!rowEnded(row)&&/进行中|已开始|^开始$|^更新$/.test(noticeListStatus(row,'planned').label);
function rowAttention(row:Dict,lane:string){if(lane==='planned'&&rowUnknown(row))return '源表状态未取得，请重新读取';if(lane==='planned'&&rowStarted(row))return '先核验已发通告';if(lane==='ongoing'&&['maintenance','change','repair'].includes(row.work_type)&&row.site_photo_count!==undefined&&Number(row.site_photo_count)===0)return '结束前需现场照片';return '';}
const actionLabel=(value:string)=>({start:'开始',update:'更新',end:'结束'}[value]||'');
let listAbort:AbortController|undefined,detailAbort:AbortController|undefined,disposed=false,timer=0;
const editing=ref(false),detailLoading=ref(false),form=ref<NoticeForm|null>(null),detailError=ref(''),busy=ref(false),saving=ref(false),uploading=ref(false),confirming=ref(false),ambiguous=ref(false);
const draftWarning=ref('');
const selectedDingNames=ref<string[]>([]);
function measureViewport(){void nextTick(revealActiveType);}
const attempted=ref(false),issueTarget=ref<Omit<NoticeIssue,'message'>|null>(null);
const siteUpload=ref<InstanceType<typeof NoticePhotoUpload>|null>(null),aliUpload=ref<InstanceType<typeof NoticePhotoUpload>|null>(null);
const pendingPhotos=computed(()=>Number(siteUpload.value?.pending||0)+Number(aliUpload.value?.pending||0));
function uploadBusy(value:boolean){uploading.value=value;}
function persistAfterUpload(){void nextTick(persist);}
const verifyBeforeStart=ref(false);
const bindingFinished=ref(false);
const relations=ref<InstanceType<typeof NoticeRelations>>();
const action=ref('start'),sop=ref<Dict>({}),sopField=ref<Dict>({}),photos=ref<Dict[]>([]),aliPhotos=ref<Dict[]>([]),job=ref<Dict|null>(null);
const sourceChoice=ref<string|null>(null),sourceOptions=ref<{label:string;value:string;title?:string;building?:string;cycle?:string}[]>([]),sourceLoading=ref(false),sourceExpanded=ref(false);
function toggleSource(){sourceExpanded.value=!sourceExpanded.value;if(sourceExpanded.value)void loadSources();}
function chooseSourceMode(value:string){if(!form.value||locked.value||!['bind','unbound'].includes(value))return;form.value.values.manual_binding_choice=value;sourceChoice.value=null;if(value==='bind')void loadSources();persist();}
const sopStatus=ref<Dict|null>(null);
const sopScope=computed(()=>form.value?.values.building_codes?.length===1?String(form.value.values.building_codes[0]):'');
const selectedBuildingLabel=computed(()=>(form.value?.values.building_codes||[]).map((v:string)=>v==='110'?'110站':v+'楼').join('、')||'未选择楼栋');
const selectedSopLabel=computed(()=>sopField.value.sops?.find((s:Dict)=>s.sop_id===sop.value.sop_id)?.name||'已选择工单');
const sopPersonName=(id:string)=>sopField.value.people?.find((p:Dict)=>p.record_id===id)?.name||'请在工单选择区核对';
watch(sopScope,(value,previous)=>{sopField.value={notice_scope:value,work_type:form.value?.values.work_type};if(previous&&previous!==value)sop.value={sop_version:0};});
const attentionKeys=ref<string[]>([]),originalFields=ref<Dict>({});
const missingFields=computed(()=>form.value?.fields.filter(f=>f.required&&!String(form.value!.values[f.key]??'').trim())||[]);
const cycleWarnings=computed(()=>form.value?Object.values(noticeCycleWarnings(form.value.values,form.value.cycleRules)):[]);
const primaryFields=computed(()=>{
  const fields=form.value?.fields||[],keys=new Set(['title','progress','actual_action_time',...(action.value==='start'?['start_time','end_time']:[]),...attentionKeys.value]);
  return fields.filter(f=>keys.has(f.key)).sort((a,b)=>Number(attentionKeys.value.includes(b.key))-Number(attentionKeys.value.includes(a.key)));
});
const secondaryFields=computed(()=>form.value?.fields.filter(f=>!primaryFields.value.includes(f))||[]);
const editingFields=computed(()=>[...primaryFields.value,...secondaryFields.value]);
const changedFields=computed(()=>form.value?.fields.filter(f=>String(form.value!.values[f.key]??'')!==String(originalFields.value[f.key]??''))||[]);
const terminal=computed(()=>!!job.value&&['success','succeeded','completed','failed','cancelled'].includes(job.value.phase));
const succeeded=computed(()=>!!job.value&&['success','succeeded','completed'].includes(job.value.phase));
const eventWaiting=computed(()=>job.value?.phase==='event_waiting');
const messagesDelivered=computed(()=>job.value?.message_first && [job.value.feishu_group_state,job.value.message_delivery_state,job.value.dingtalk_state].every(state=>['sent','done','skipped'].includes(state)));
const taskTitle=computed(()=>{
  if(eventWaiting.value)return '已排队，等待事件结束';
  if(ambiguous.value)return '提交结果待核验';
  if(succeeded.value)return '通告办理完成';
  if(messagesDelivered.value)return job.value?.phase==='failed'?'消息已送达，多维同步待处理':'消息已送达，多维后台同步中';
  if(job.value?.phase==='failed')return '本次未完成';
  if(job.value?.phase==='cancelled')return '任务已取消';
  return job.value?.message_first?'通告已受理，消息投递与多维同步中':'后台处理中';
});
function deliveryLabel(state:string){return ({sent:'已送达',done:'已送达',skipped:'无需发送',failed:'失败，请核验',blocked:'权限待核验',verification_required:'回执待核验',uncertain:'回执待核验'} as Record<string,string>)[state]||'正在投递';}
const shownEventWaits=new Set<string>();
function showEventWait(value:Dict){
  if(value.phase!=='event_waiting'||shownEventWaits.has(value.job_id))return;
  shownEventWaits.add(value.job_id);
  dialog.info({title:'事件优先：通告已排队',content:()=>h('div',{style:'white-space:pre-line;max-height:60vh;overflow:auto'},String(value.event_wait?.message||'事件结束或删除后自动发送。')),positiveText:'知道了，保留排队'});
}
const nextRecord=computed(()=>{const v=form.value?.values||{};return (action.value==='start'?lanes.value[0].rows:lanes.value[1].rows).find(r=>![v.active_item_id,v.source_record_id,v.target_record_id].includes(rowKey(r))&&!rowEnded(r)&&!(action.value==='start'&&(rowStarted(r)||rowUnknown(r))));});
function showTaskRecords(){if(!props.recordOnly)closeEditor();recordsOpen.value=true;}
const locked=computed(()=>busy.value||saving.value||uploading.value||detailLoading.value||ambiguous.value||!!job.value||bindingFinished.value||!form.value?.actions.length);
const saved=ref('');
let draftVersion=0,operationId='',frozen:Dict|null=null,formKey='',originalProgress='',progressBeforeEnd='';
const snapshot=()=>JSON.stringify({values:form.value?.values,sop:sop.value,photos:photos.value,aliPhotos:aliPhotos.value,action:action.value});
const dirty=computed(()=>!!form.value&&(snapshot()!==saved.value||pendingPhotos.value>0));
function query(){return new URLSearchParams({scope:props.scope,work_type:kind.value,month:applied.value.month,search:applied.value.search,specialty:applied.value.specialty||'',sections:'records,ongoing',records_page:String(pages.value.planned),records_page_size:'20',ongoing_page:String(pages.value.ongoing),ongoing_page_size:'20',...(selectedBuildings.value?{buildings:selectedBuildings.value.join(',')}:{})});}
async function load(fresh=false){
  if(props.recordOnly)return;
  if(kind.value==='event'){listAbort?.abort();loading.value=false;return;}
  if(!props.active||disposed||document.hidden)return;
  listAbort?.abort();const c=new AbortController();listAbort=c;loading.value=true;error.value='';
  try{
    const read=(q:URLSearchParams)=>requestJson('/api/workbench?'+q,{signal:c.signal,timeoutMs:16000,...(fresh?{fresh:true}:{})});
    const q=query();q.delete('buildings');const buildings=selectedBuildings.value;
    if(buildings?.length===0)throw new Error('当前未选择可访问楼栋。');
    if(buildings?.length===1)q.set('scope',buildings[0]!);
    const aggregate=props.scope==='ALL'&&buildings?.length===buildingOptions.value.length;
    const partial=!Array.isArray(data.value.records)?(snapshot:Dict)=>{
      if(!c.signal.aborted)data.value={...snapshot,partial_buildings:true};
    }:undefined;
    const value=buildings&&buildings.length>1&&!aggregate?await readNoticeBuildings(q,buildings,read,partial):await read(q);
    if(c.signal.aborted)return;
    if(value.source_snapshot_ready===false||!Array.isArray(value.records)||!Array.isArray(value.ongoing))throw new Error('通告资料尚未完整读取，请稍后重试。');data.value=value;
  }
  catch(e){if(!c.signal.aborted)error.value=e instanceof Error?e.message:'读取失败，原页面保留';}
  finally{if(listAbort===c)loading.value=false;}
}
function setKind(value:string){const q=query();q.set('work_type',value);q.set('records_page','1');q.set('ongoing_page','1');q.delete('sections');navigate('/workbench-lite?'+q);}
function changeBuildings(values:string[]){
  const selected=values.includes('ALL')?buildingOptions.value.map(option=>option.value):values.filter(value=>buildingOptions.value.some(option=>option.value===value));if(!selected.length)return;
  const q=query();q.delete('buildings');q.set('scope',selected.length>1&&props.scopes.some(option=>option.value==='ALL')?'ALL':selected[0]!);
  if(selected.length>1)q.set('buildings',selected.join(','));
  q.set('records_page','1');q.set('ongoing_page','1');q.delete('sections');navigate('/workbench-lite?'+q);
}
function changePage(lane:string,page:number){pages.value[lane]=page;const q=query();q.delete('sections');const before=location.href;navigate('/workbench-lite?'+q,{replace:true});if(before===location.href)void load();}
function filter(){pages.value={planned:1,ongoing:1};const q=query();q.set('month',month.value);q.set('search',search.value);q.set('specialty',specialty.value||'');q.delete('sections');const before=location.href;navigate('/workbench-lite?'+q,{replace:true});if(location.href===before)void load(true);}
function cacheKey(values:Dict){return noticeCacheKey(values,props.user.open_id);}
function persist():boolean{if(!form.value||!formKey||job.value?.phase==='success')return true;try{sessionStorage.setItem(formKey,JSON.stringify({schema:2,context:noticeDraftContext({...form.value,values:originalFields.value}),baseValues:originalFields.value,snapshot:JSON.parse(snapshot()),draftVersion,job:job.value,operationId,frozen,at:Date.now()}));return true;}catch{detailError.value='浏览器暂时不能保存恢复信息，请保持页面打开，核对后提交。';return false;}}
function resetRejected(){if(job.value?.phase!=='failed'||job.value.error_category!=='field_or_payload'||busy.value)return;job.value=null;frozen=null;operationId='';ambiguous.value=false;persist();}
async function openRecord(row:Dict|null,initial=false,desiredAction='',parsedHtml='',manualType='',focusBinding=false){
  if(row&&lanes.value[0].rows.includes(row)&&rowUnknown(row)){message.warning('源表状态未取得，请重新读取后办理。');return;}
  if(busy.value||uploading.value||saving.value)return;
  if(editing.value&&dirty.value&&!job.value){askLeave(()=>openRecord(row,initial,desiredAction,parsedHtml,manualType,focusBinding));return;}
  attempted.value=false;issueTarget.value=null;confirming.value=false;verifyBeforeStart.value=false;bindingFinished.value=false;attentionKeys.value=[];
  draftWarning.value='';
  detailAbort?.abort();const c=new AbortController();detailAbort=c;editing.value=true;detailLoading.value=true;detailError.value='';form.value=null;job.value=null;ambiguous.value=false;frozen=null;operationId='';draftVersion=0;photos.value=[];aliPhotos.value=[];sop.value={};sopField.value={};formKey='';
  const work=row?.work_type||manualType||kind.value;
  const q=new URLSearchParams({scope:row?._request_scope||props.scope,work_type:work,month:applied.value.month});
  if(row){
    const activeId=row.active_item_id||row.target_record_id;
    q.set(activeId?'active_item_id':'record_id',activeId||rowKey(row));
  }else if(initial){for(const key of ['active_item_id','record_id','prefill_context_id','prefill_source_record_id','prefill_target_record_id','prefill_action'])if(params.value.has(key))q.set(key,params.value.get(key)!);}else q.set('manual','1');
  try{
    const result=parsedHtml?{fragments:{detail:parsedHtml}}:await requestJson('/api/workbench/lite-detail?'+q,{signal:c.signal,cache:'no-store'});if(c.signal.aborted)return;
    const next=parseNoticeForm(result.fragments?.detail||'');
    next.values.dingtalk_robot_ids=[];
    if(!NOTICE_IDS.includes(next.values.work_type)||!(props.user.modules||[]).includes(next.values.work_type))throw new Error('无权办理此类通告。');
    const preferredAction=desiredAction||next.initialAction;
    form.value=next;action.value=next.actions.includes(preferredAction)?preferredAction:desiredAction?'':next.actions[0]||'start';formKey=cacheKey(next.values);sourceChoice.value=next.values.source_record_id||null;sourceOptions.value=next.sourceOptions;sourceExpanded.value=focusBinding||!!(next.values.manual_binding_required&&!next.values.source_record_id&&!next.values.target_record_id);
    if(sourceChoice.value&&!sourceOptions.value.some(o=>o.value===sourceChoice.value))sourceOptions.value.unshift({label:'已绑定源表事项',value:sourceChoice.value});
    verifyBeforeStart.value=Boolean((row?lanes.value[0].rows.includes(row)&&rowStarted(row):initial&&params.value.get('notice_action')==='verify')
      &&!next.values.target_record_id&&next.actions.includes('start'));
    originalFields.value={...next.values};attentionKeys.value=next.fields.filter(f=>f.required&&!String(next.values[f.key]??'').trim()).map(f=>f.key);
    sop.value={sop_version:0,scope:sopScope.value,exempt:next.values.polling_work_order_exempt===true};sopField.value={notice_scope:sopScope.value,work_type:next.values.work_type};
    if(next.values.target_record_id){
      const draft=await requestJson('/api/workbench/draft?'+new URLSearchParams({scope:next.values.scope||q.get('scope')||props.scope,work_type:next.values.work_type,target_record_id:next.values.target_record_id,action:action.value}),{signal:c.signal,cache:'no-store'});
      if(c.signal.aborted)return;
      draftWarning.value=draft.warning||'';
      draftVersion=Number((draft.draft||draft.draft_base)?.version)||0;
      for(const field of next.fields){
        if(draft.draft?.fields?.[field.key]!==undefined)next.values[field.key]=draft.draft.fields[field.key];
      }
    }
    originalFields.value=JSON.parse(JSON.stringify(next.values));originalProgress=String(next.values.progress||'');progressBeforeEnd=originalProgress;saved.value=snapshot();
    try{
      let cached=JSON.parse(sessionStorage.getItem(formKey)||'null');
      // Older versions used one key for every manual notice. Recover it only
      // when the frozen notice identity really matches the pasted notice.
      if(!cached&&parsedHtml&&next.values.manual==='1'&&!next.values.target_record_id&&!next.values.source_record_id){
        const legacyKey='v2-notice:'+props.user.open_id+':'+next.values.scope+':'+next.values.work_type+':manual';
        const legacy=JSON.parse(sessionStorage.getItem(legacyKey)||'null');
        const savedValues=legacy?.frozen||legacy?.snapshot?.values||{};
        const same=['notice_type','title','start_time','end_time'].every(key=>String(savedValues[key]||'')===String(next.values[key]||'')) &&
          JSON.stringify(savedValues.building_codes||[])===JSON.stringify(next.values.building_codes||[]);
        if(legacy&&same&&legacy.context===noticeDraftContext({...next,values:originalFields.value}))cached=legacy;
      }
      if(cached&&Date.now()-cached.at<7*86400000){
        if((!parsedHtml||cached.job||cached.frozen)&&restoreNoticeEdits(next,cached,draftVersion)){
          if(JSON.stringify(cached.snapshot.values.building_codes)!==JSON.stringify(cached.baseValues.building_codes))next.values.building_codes=(cached.snapshot.values.building_codes||[]).filter((s:string)=>buildingOptions.value.some(b=>b.value===s));
          sop.value={sop_version:0,...cached.snapshot.sop};photos.value=cached.snapshot.photos||[];aliPhotos.value=cached.snapshot.aliPhotos||[];
          action.value=next.actions.includes(cached.snapshot.action)?cached.snapshot.action:action.value;
        }else if(!parsedHtml)detailError.value='已重新读取当前通告；旧副本缺少来源校验或楼栋、周期、版本已变化，未自动覆盖。';
        // An unresolved write must still be recoverable even when another window saved a newer draft.
        const sameContext=cached.context===noticeDraftContext({...next,values:originalFields.value});
        if(parsedHtml&&!sameContext&&(cached.job||cached.frozen)){
          const previous=String(cached.job?.job_id||cached.operationId||cached.at||'unknown');
          sessionStorage.setItem(formKey+':prior:'+encodeURIComponent(previous),JSON.stringify(cached));
        }
        if(!parsedHtml||sameContext){job.value=cached.job||null;operationId=cached.operationId||'';frozen=cached.frozen||null;ambiguous.value=!!frozen&&!job.value;}
        if(parsedHtml&&sameContext&&(job.value||frozen))detailError.value='本条通告有未核验的原任务，请先核验原任务；本次解析尚未提交。';
      }
    }catch{/* An invalid local draft never replaces the server values. */}
    const localManual=(()=>{try{const cached=JSON.parse(sessionStorage.getItem(formKey)||'null');return cached?.schema===2&&cached.context===noticeDraftContext({...next,values:originalFields.value})&&Date.now()-cached.at<7*86400000?cached.snapshot?.values:null;}catch{return null;}})();
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
    attentionKeys.value=[...new Set([...attentionKeys.value,...next.fields.filter(f=>f.required&&!String(next.values[f.key]??'').trim()).map(f=>f.key)])];
    if(job.value&&!terminal.value)void pollJob();
    if(focusBinding&&!job.value&&!ambiguous.value){
      detailLoading.value=false;await nextTick();
      document.querySelector<HTMLElement>('[data-notice-section="binding"]')?.scrollIntoView({block:'nearest'});
      void loadSources();
    }
  }catch(e){if(!c.signal.aborted)detailError.value=e instanceof Error?e.message:'通告读取失败';}
  finally{if(detailAbort===c)detailLoading.value=false;}
}
function askLeave(proceed:()=>void){dialog.warning({title:'当前填写尚未保存',content:'继续将关闭当前填写区。本机恢复副本仍保留，未提交的内容不会发送。'+(pendingPhotos.value?'未上传成功的图片需要重新选择。':''),positiveText:'保留副本并离开',negativeText:'继续填写',onPositiveClick:()=>{if(!persist()){message.error(detailError.value);return false;}saved.value=snapshot();editing.value=false;proceed();}});}
function closeEditor(){if(busy.value||uploading.value||saving.value)return;const close=()=>{editing.value=false;detailAbort?.abort();window.clearTimeout(timer);if(props.recordOnly)emit('recordClose');};if(dirty.value&&!job.value){askLeave(close);return;}if(persist()||!dirty.value&&!job.value&&!ambiguous.value)close();}
function changeAction(){
  const values=form.value?.values;
  if(values)values.dingtalk_robot_ids=[];
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
    const base=current.draft||current.draft_base;
    if(Number(base?.version||0)!==draftVersion)throw new Error(conflict);
    const fields=Object.fromEntries(form.value.fields.map(f=>[f.key,String(v[f.key]??'').trim()]));
    fields.building_codes=v.building_codes.join(',');
    const revision=Number(base?.client_revision||0)+1;
    const result=await requestJson('/api/workbench/draft',{method:'PUT',body:JSON.stringify({scope:v.scope,work_type:v.work_type,target_record_id:v.target_record_id,action:action.value,fields,client_revision:revision})});
    if(Number(result.draft?.client_revision)!==revision||Object.entries(fields).some(([key,value])=>String(result.draft?.fields?.[key]??'')!==value))throw new Error(conflict);
    draftVersion=Number(result.draft.version);draftWarning.value='';saved.value=snapshot();persist();message.success('草稿已保存');return true;
  }catch(e){detailError.value=e instanceof Error?e.message:'草稿保存失败';return false;}
  finally{saving.value=false;}
}
function preview(){
  if(!form.value||locked.value)return;
  attempted.value=true;issueTarget.value=null;detailError.value=validateNotice(form.value,action.value,sop.value,photos.value);
  if(verifyBeforeStart.value||needsOriginalNotice(form.value,action.value)){locateOriginal();return;}
  if(pendingPhotos.value){detailError.value='还有图片未上传成功，请重试或移除后再预览。';issueTarget.value={target:siteUpload.value?.pending?'photos':'ali',label:'处理未上传图片'};}
  if(!detailError.value&&action.value==='end'&&form.value.groupId){
    const state=sopStatus.value;issueTarget.value={target:'workorder',label:'核验工单状态'};
    if(!state||state.error)detailError.value='工单状态尚未核验，请刷新工单状态后重试。';
    else if(!['upload_pending','completed'].includes(state.state))detailError.value='关联工单步骤尚未全部完成。';
    else if(state.missing_completion||Number(state.due_at_ts||0)>Date.now()/1000)detailError.value='工单延时提醒条件尚未满足，请等待后刷新工单状态。';
  }
  if(detailError.value){
    if(!issueTarget.value)issueTarget.value=noticeIssueTarget(form.value,action.value,sop.value,photos.value);
    focusIssue();
    return;
  }
  issueTarget.value=null;confirming.value=true;void nextTick(()=>document.querySelector('.v2-notice-editor>.n-card-content')?.scrollTo({top:0}));
}
function locateOriginal(){
  detailError.value=verifyBeforeStart.value?'':'请核对原通告的楼栋、名称和时间后选择记录。若首条刚发送，请在办理记录中核对同步状态。';
  issueTarget.value={target:'relations',label:'核验关联'};
  focusIssue();
  void relations.value?.openTarget();
}
function focusIssue(){
  if(!issueTarget.value)return;const target=issueTarget.value.target;
  void nextTick(()=>{
    const selector=target.startsWith('field:')?'[data-notice-field="'+CSS.escape(target.slice(6))+'"]':'[data-notice-section="'+CSS.escape(target)+'"]';
    const section=document.querySelector<HTMLElement>('.v2-notice-editor '+selector);if(!section)return;
    section.scrollIntoView({block:'center',behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});
    const input=section.querySelector<HTMLElement>('input:not([disabled]),textarea:not([disabled]),[role=combobox],button:not([disabled])');input?.focus({preventScroll:true});
  });
}
async function submit(){
  if(!form.value||busy.value)return {ok:false};
  if(bindingFinished.value||!frozen&&verifyBeforeStart.value){detailError.value='请核对原通告的关联状态。';return {ok:false};}
  busy.value=true;detailError.value='';
  try{
    if(!frozen){const issue=validateNotice(form.value,action.value,sop.value,photos.value);if(issue)throw new Error(issue);if(!await saveDraft()){confirming.value=false;return {ok:false};}operationId=createRepairOperationId('v2-notice');frozen=noticeCommand(form.value,action.value,operationId,draftVersion,sop.value,photos.value,aliPhotos.value);persist();}
    let response:Dict;
    for(;;){
      try{response=await requestJson('/api/workbench-actions',{method:'POST',body:JSON.stringify(frozen)});break;}
      catch(e){
        const payload=e instanceof ApiError?e.payload:{};
        if(payload.error_code!=='confirmation_required'||payload.details?.kind!=='plan_convergence_unmatched')throw e;
        if(!payload.details.confirmation)throw new Error('发送确认暂不可用，当前尚未发送，请重试。');
        const submission:Dict|null=frozen;
        const approved=await new Promise<boolean>(resolve=>dialog.warning({
          title:'计划收敛核对待确认',
          content:()=>h('div',{style:{overflowWrap:'anywhere'}},[
            h('p',String(payload.details.reason||payload.error||'尚未取得有效匹配结果。')),
            h('p','通告：'+String(payload.details.notice_name||form.value?.values.title||'当前通告')),
            h('p','当前尚未发送。确认后将继续发送'+actionLabel(action.value)+'通告；返回核对会保留本次填写。')]),
          positiveText:'仍然发送'+actionLabel(action.value),negativeText:'返回核对',maskClosable:false,closeOnEsc:false,closable:false,
          onPositiveClick:()=>{resolve(true);},onNegativeClick:()=>{resolve(false);},onAfterLeave:()=>resolve(false)
        }));
        if(!approved||disposed||!editing.value||frozen!==submission){frozen=null;operationId='';confirming.value=false;persist();return {ok:false};}
        frozen!.plan_convergence_confirmation=payload.details.confirmation;
      }
    }
    if(!response.job_id)throw new Error('未收到任务编号，保留原提交等待核验。');
    job.value={...response,phase:response.phase||response.initial_phase||'queued'};ambiguous.value=false;confirming.value=false;saved.value=snapshot();persist();showEventWait(job.value);void pollJob();
    rememberNoticeTask(props.user.open_id,response,form.value.values);
    return response;
  }catch(e){
    confirming.value=false;detailError.value=e instanceof Error?e.message:'提交失败，填写已保留';ambiguous.value=!!frozen&&(!(e instanceof ApiError)||!e.status||e.status>=500);if(!ambiguous.value)frozen=null;persist();
    return {ok:false};
  }finally{busy.value=false;}
}
function send(){return trackButtonRequest(submit());}
async function pollJob(){
  window.clearTimeout(timer);
  if(!job.value?.job_id||!editing.value||!props.active||document.hidden||disposed)return;
  const id=job.value.job_id;
  try{
    const value=await requestJson('/api/jobs/'+encodeURIComponent(id),{cache:'no-store'});
    if(job.value?.job_id!==id||disposed||!props.active||!editing.value)return;
    job.value=value;persist();showEventWait(value);
    if(terminal.value){
      if(value.phase==='success'){
        frozen=null;saved.value=snapshot();
        try{sessionStorage.removeItem(formKey);}catch{/* Submission success does not depend on browser storage. */}
        window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);
      }
      return;
    }
  }catch{if(job.value?.job_id===id)detailError.value='任务进度暂时不可用，原任务仍保留，请勿重复新增。';}
  if(!disposed&&props.active&&editing.value)timer=window.setTimeout(pollJob,eventWaiting.value?15000:4000);
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

async function refreshAliPhotos(current:NoticeForm){try{const v=current.values,q=new URLSearchParams({scope:v.scope,work_type:'change',month:applied.value.month,active_item_id:v.active_item_id||v.target_record_id});const result=await requestJson('/api/workbench/lite-detail?'+q,{cache:'no-store'});if(form.value===current)current.aliPhotos=parseNoticeForm(result.fragments?.detail||'').aliPhotos;}catch{detailError.value='截图操作已完成，预览暂未刷新；其它填写已保留，请稍后重新打开核验。';}}
async function uploadAliNow(){const current=form.value,photo=aliPhotos.value[0];if(!current?.values.target_record_id||!photo||locked.value)return;busy.value=true;detailError.value='';try{await requestJson('/api/change-confirmations/'+encodeURIComponent(current.values.target_record_id)+'/screenshot',{method:'POST',body:JSON.stringify({upload_id:photo.upload_id,local_image_id:photo.local_image_id||''})});aliPhotos.value=[];persist();message.success('确认截图已上传，未发送通告');await refreshAliPhotos(current);}catch(e){detailError.value=(e as Error).message+'；已保留原图片，请先核验结果。';}finally{busy.value=false;}}
function removeAliPhoto(photo:Dict){const current=form.value;if(!current?.values.target_record_id||locked.value)return;dialog.warning({title:'删除此确认截图？',positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{busy.value=true;try{await requestJson('/api/change-confirmations/'+encodeURIComponent(current.values.target_record_id)+'/screenshot?'+new URLSearchParams({file_token:photo.file_token||'',local_image_id:photo.local_image_id||''}),{method:'DELETE'});await refreshAliPhotos(current);}catch(e){detailError.value=(e as Error).message;return false;}finally{busy.value=false;}}});}

function pastePhotos(event:ClipboardEvent){if(event.clipboardData?.files.length&&form.value?.photoRequired){event.preventDefault();siteUpload.value?.add(event.clipboardData.files);}}
let sourceTimer=0,sourceSequence=0;
function searchSources(q:string){clearTimeout(sourceTimer);sourceTimer=window.setTimeout(()=>loadSources(q),300);}
async function loadSources(q=''){if(!form.value)return;const current=form.value,sequence=++sourceSequence;sourceLoading.value=true;try{const value=await requestJson('/api/workbench/source-options?'+new URLSearchParams({scope:current.values.scope,work_type:current.values.work_type,month:applied.value.month,q}));if(form.value===current&&sequence===sourceSequence){sourceOptions.value=(value.items||[]).map((r:Dict)=>({label:[r.title,r.maintenance_cycle,r.building,r.progress].filter(Boolean).join(' · '),value:r.source_record_id,title:r.title,building:r.building,cycle:r.maintenance_cycle}));if(current.values.source_record_id&&!sourceOptions.value.some(o=>o.value===current.values.source_record_id))sourceOptions.value.unshift({label:'已绑定源表事项',value:current.values.source_record_id});}}catch(e){if(form.value===current&&sequence===sourceSequence)detailError.value=(e as Error).message;}finally{if(form.value===current&&sequence===sourceSequence)sourceLoading.value=false;}}
async function associationBound(result:Dict){
  const current=form.value;if(!current)return;
  const v=current.values,selectedAction=['update','end'].includes(action.value)?action.value:'update';
  const target=result.target_record_id||v.target_record_id;
  if(result.target_finished===true){
    if(result.source_record_id!==v.source_record_id||!target||job.value||ambiguous.value)return;
    bindingFinished.value=true;verifyBeforeStart.value=false;confirming.value=false;
    detailError.value='';issueTarget.value=null;
    window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);
    return;
  }
  const restoring=needsOriginalNotice(current,action.value);
  const keepEdits=restoring||verifyBeforeStart.value;
  const carried=keepEdits?Object.fromEntries((restoring?current.fields:changedFields.value).filter(field=>
    !field.readonly&&!['title','name','start_time','end_time','fault_time','expected_time'].includes(field.key)
      &&(!restoring||String(v[field.key]??'').trim())
  ).map(field=>[field.key,v[field.key]])):{};
  const previousKey=formKey;
  if(keepEdits&&!persist())return;
  if(!keepEdits)try{sessionStorage.removeItem(previousKey);}catch{/* Read the canonical association again. */}
  saved.value=snapshot();editing.value=false;busy.value=false;
  window.dispatchEvent(new Event('clipflow-business-changed'));void load(true);
  await openRecord({work_type:v.work_type,active_item_id:result.active_item_id||v.active_item_id,target_record_id:target,_request_scope:v.scope},false,selectedAction);
  const reopened=form.value;
  if(!reopened||reopened===current||reopened.values.target_record_id!==target||action.value!==selectedAction)return;
  if(keepEdits&&!job.value&&!ambiguous.value){
    for(const field of reopened.fields)if(!field.readonly&&field.key in carried)reopened.values[field.key]=carried[field.key];
    if(!persist())return;
  }
  if(previousKey!==formKey)try{sessionStorage.removeItem(previousKey);}catch{/* Only remove the superseded local copy. */}
  if(result.target_record_id)message.success('已绑定原通告');
}
function bindSource(){
  if(!form.value||!sourceChoice.value||locked.value)return;
  const current=form.value,selected=sourceChoice.value;
  if(!current.values.target_record_id){
    if(current.values.manual!=='1'&&current.detailMode!=='manual'&&current.detailMode!=='ongoing'){
      const open=()=>{saved.value=snapshot();editing.value=false;void openRecord({record_id:selected,work_type:current.values.work_type,_request_scope:current.values.scope},false,'start');};
      if(dirty.value)askLeave(open);else open();
    }else{current.values.source_record_id=selected;current.values.manual_binding_choice='bind';if(current.values.work_type==='repair')current.values.repair_management_record_id=selected;formKey=cacheKey(current.values);originalFields.value={...originalFields.value,source_record_id:selected,manual_binding_choice:'bind'};persist();}
    return;
  }
  dialog.warning({title:'确认绑定源表事项',content:'将绑定“'+(sourceOptions.value.find(o=>o.value===selected)?.label||'所选计划')+'”。只更新关联，不重复发送通告；保存后重新读取，未保存的填写将重置。',positiveText:'确认绑定',negativeText:'返回核对',onPositiveClick:async()=>{
    if(form.value!==current||locked.value)return false;
    const v=current.values;busy.value=true;detailError.value='';
    try{
      const result=await requestJson('/api/notice-identity/bind',{method:'POST',body:JSON.stringify({scope:v.scope,binding_context:'ongoing',work_type:v.work_type,notice_type:v.notice_type,active_item_id:v.active_item_id,source_record_id:selected,source_month:v.source_month||applied.value.month,source_binding_only:true,target_record_id:v.target_record_id,title:v.title,reason:v.reason,start_time:v.start_time,end_time:v.end_time})});
      if(result.binding_pending){
        if(form.value===current)detailError.value=String(result.message||'关联状态待核验，后台将继续核对。');
        return;
      }
      if(form.value!==current)return;
      await associationBound(result);
      message.success('源表事项已绑定');
    }
    catch(e){detailError.value=(e as Error).message;return false;}finally{busy.value=false;}
  }});
}
async function resendSopLinks(){if(!form.value?.groupId)return;busy.value=true;try{await requestJson('/api/polling-work-orders/'+encodeURIComponent(form.value.groupId)+'/resend-links',{method:'POST',body:'{}'});message.success('工单链接已提交发送');}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
async function refreshSopStatus(){const current=form.value;if(!current?.groupId||busy.value)return;busy.value=true;try{const result=await requestJson('/api/workbench/polling-delay-status?group_id='+encodeURIComponent(current.groupId),{cache:'no-store'});if(form.value===current)sopStatus.value=result;}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
async function retrySopUpload(){if(!form.value?.groupId||busy.value)return;busy.value=true;try{await requestJson('/api/polling-work-orders/'+encodeURIComponent(form.value.groupId)+'/retry-upload',{method:'POST',body:'{}'});busy.value=false;await refreshSopStatus();}catch(e){detailError.value=(e as Error).message;}finally{busy.value=false;}}
function deleteNotice(){
  if(!form.value?.canDelete||locked.value)return;
  const v={...form.value.values},operation=createRepairOperationId('v2-delete');
  const deletingDialog=dialog.error({title:'删除整条通告',content:'将删除当前通告及对应目标记录：'+v.title,positiveText:'确认删除',negativeText:'取消',onPositiveClick:async()=>{
    if(busy.value)return false;
    deletingDialog.loading=true;deletingDialog.closable=false;deletingDialog.maskClosable=false;
    busy.value=true;
    try{
      await requestJson('/api/ongoing-items/delete',{method:'POST',body:JSON.stringify({scope:v.scope,work_type:v.work_type,notice_type:v.notice_type,active_item_id:v.active_item_id,source_record_id:v.source_record_id,target_record_id:v.target_record_id,record_id:v.target_record_id||v.record_id,title:v.title,operation_id:operation})});
      try{sessionStorage.removeItem(formKey);}catch{/* A denied storage write must not undo a successful business result. */}
      saved.value=snapshot();editing.value=false;form.value=null;
      window.dispatchEvent(new Event('clipflow-business-changed'));await load(true);
    }catch(e){message.error((e as Error).message);return false;}
    finally{busy.value=false;deletingDialog.loading=false;deletingDialog.closable=true;deletingDialog.maskClosable=true;}
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
function guardNoticeNavigation(proceed:()=>void){if(busy.value||uploading.value||saving.value||parsing.value){message.warning('正在提交或上传，请等待结果。');return false;}if(editing.value&&dirty.value&&!job.value){askLeave(proceed);return false;}return !editing.value||persist();}
function guardNavigation(proceed:()=>void){
  if(!props.active)return true;
  if(historyOpen.value&&historyDialog.value){
    const resume=()=>{historyOpen.value=false;if(guardNoticeNavigation(proceed))proceed();};
    if(!historyDialog.value.guardNavigation(resume))return false;
    historyOpen.value=false;
  }
  return guardNoticeNavigation(proceed);
}
defineExpose({guardNavigation});
const stopGuard=registerNavigationGuard((_target,proceed)=>guardNavigation(proceed));
function visibility(){if(document.hidden){listAbort?.abort();window.clearTimeout(timer);window.clearTimeout(undoTimer);}else if(props.active){void load();void pollJob();void pollUndo();}}
function beforeUnload(event:BeforeUnloadEvent){persist();if(busy.value||uploading.value||dirty.value){event.preventDefault();event.returnValue='';}}
const contextKey=()=>JSON.stringify([props.scope,selectedBuildings.value,kind.value,applied.value]);
let listContext=contextKey();
watch(()=>JSON.stringify([props.url,props.active,props.scope,props.selectedScopes]),()=>{
  persist();detailAbort?.abort();editing.value=false;confirming.value=false;undoOpen.value=false;changeOpen.value=false;recordsOpen.value=false;pasteOpen.value=false;window.clearTimeout(timer);syncAbort?.abort();
  if(!props.active){listAbort?.abort();return;}
  kind.value=params.value.get('work_type')||'all';month.value=params.value.get('month')||month.value;
  search.value=params.value.get('search')||'';
  specialty.value=params.value.get('specialty')||null;
  applied.value={month:month.value,search:search.value,specialty:specialty.value};
  const nextContext=contextKey();
  if(nextContext!==listContext){pages.value={planned:1,ongoing:1};data.value={};listContext=nextContext;}
  if(params.value.has('records_page')||params.value.has('pending_page'))pages.value.planned=pageNumber(params.value.get('records_page')||params.value.get('pending_page'));
  if(params.value.has('ongoing_page'))pages.value.ongoing=pageNumber(params.value.get('ongoing_page'));
  void load();
  if(params.value.has('active_item_id')||params.value.has('record_id'))void openRecord(null,true,requestedAction());
});
function requestedAction(){const value=params.value.get('notice_action')||'';return ['start','update','end'].includes(value)?value:'';}
onMounted(()=>{void load();if(params.value.has('active_item_id')||params.value.has('record_id'))void openRecord(null,true,requestedAction());if(params.value.has('change_confirmation')&&moreOptions.value.some(o=>o.key==='change'))changeOpen.value=true;document.addEventListener('visibilitychange',visibility);window.addEventListener('beforeunload',beforeUnload);window.addEventListener('resize',measureViewport);});
watch(undoOpen,value=>{if(!value)window.clearTimeout(undoTimer);});
onBeforeUnmount(()=>{persist();disposed=true;stopGuard();listAbort?.abort();detailAbort?.abort();syncAbort?.abort();window.clearTimeout(timer);window.clearTimeout(sourceTimer);window.clearTimeout(undoTimer);document.removeEventListener('visibilitychange',visibility);window.removeEventListener('beforeunload',beforeUnload);window.removeEventListener('resize',measureViewport);});
</script>
<style>
.notice-desk,.desk-editor,.desk-dialog {
  --desk-green:#247456;
  --desk-mint:color-mix(in srgb,#84d6ae 15%,var(--soft-face));
  --desk-sky:color-mix(in srgb,#88cfea 27%,var(--soft-face));
  color:var(--v2-ink);
}
.notice-desk {min-width:0;display:grid;gap:12px;container:notice-desk / inline-size}
.notice-desk .notice-desk-head,.notice-desk .desk-band,.notice-desk-identity,.desk-commands,.desk-scope,.lane-caption,.lane-count,.lane-pagination {
  display:flex;align-items:center;gap:9px;min-width:0;
}
.notice-desk .notice-desk-head {
  justify-content:space-between;flex-wrap:wrap;padding:14px 18px;
  border-radius:15px;background:linear-gradient(112deg,var(--desk-sky),var(--desk-mint) 80%);box-shadow:var(--soft-key);
}
.desk-mark,.lane-icon {display:grid;place-items:center;flex:none;width:42px;height:42px;border-radius:13px;background:var(--soft-face);box-shadow:var(--soft-key);color:var(--v2-blue)}
.notice-desk .notice-desk-identity h1 {display:block;margin:0;font-size:22px;line-height:1.25}
.notice-desk small,.desk-editor .notice-editor-title small {color:var(--v2-muted);font-size:12px}
.desk-commands {gap:6px;flex-wrap:wrap}
.desk-commands .n-button__content,.v2-notice-editor-footer .n-button__content,.v2-notice-tools .n-button__content {gap:5px}
.notice-desk .desk-primary:not(:disabled),.desk-editor .n-button--primary-type:not(:disabled),.desk-dialog .n-button--primary-type:not(:disabled) {color:var(--v2-palette-on-accent);background:var(--v2-palette-accent)}
.notice-desk .desk-band {justify-content:space-between;flex-wrap:wrap;gap:6px 14px}
.notice-desk .notice-progress{margin:4px 0;color:var(--v2-muted);font-size:12px}
.desk-types {display:flex;gap:3px;max-width:100%;min-width:0;overflow:auto;padding:4px;box-sizing:border-box;border-radius:12px;background:var(--soft-field);box-shadow:var(--soft-inset);scrollbar-width:thin}
.desk-types button,.desk-switch button {flex:none;min-height:34px;border:1px solid transparent;border-radius:8px;padding:5px 12px;font:inherit;font-size:13px;color:var(--v2-ink);background:transparent;cursor:pointer}
.desk-types button:hover,.desk-switch button:hover {background:var(--v2-palette-selection)}
.desk-types button[aria-pressed=true],.desk-switch button[aria-pressed=true] {font-weight:650;background:var(--soft-face);box-shadow:var(--soft-key);border-color:var(--soft-edge)}
.desk-types button:focus-visible,.desk-switch button:focus-visible {outline:2px solid var(--v2-blue);outline-offset:-2px}
.desk-scope {flex-wrap:wrap;margin-inline-start:auto}
.desk-switch {display:none}
.notice-desk [data-page-layout=notices]:not(.is-customizing)>.grid-stack>.grid-stack-item[data-widget-id^="block_"]>.v2-widget>.v2-widget-head {display:none}
.desk-lane {min-width:0;container:desk-lane / inline-size}
.lane-head {display:flex;justify-content:space-between;align-items:center;gap:8px;padding:10px 12px;border-radius:12px;background:var(--desk-sky);margin-block-end:10px}
[data-list=ongoing] .lane-head {background:var(--desk-mint)}
[data-list=ongoing] .lane-icon {color:var(--desk-green)}
.lane-caption h2,.editor-section-title h2 {margin:0;font-size:17px;font-weight:650;line-height:1.35;font-family:var(--soft-title,serif)}
.lane-count {font-size:12px;color:var(--v2-muted);gap:4px}
.lane-count b {font-size:21px;line-height:1.2;color:var(--v2-ink);font-variant-numeric:tabular-nums}
.desk-filters {display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.3fr) auto;gap:6px;align-items:center;margin-bottom:10px}
.desk-filters>.n-input,.desk-filters>small {grid-column:1/-1}
.desk-filters>.n-select {min-width:0}
.desk-records,.desk-loading {list-style:none;padding:0;margin:0;display:grid;gap:8px;grid-template-columns:minmax(0,1fr)}
.desk-records>li {min-width:0}
[data-list=planned]>.desk-records {row-gap:0;column-gap:14px}
.desk-empty {display:flex;align-items:center;gap:8px;padding:12px;text-align:start;color:var(--v2-muted);background:var(--soft-field);border-radius:10px}
.desk-empty svg {flex:none;width:18px}
.desk-empty p {margin:0}
.lane-pagination {justify-content:space-between;flex-wrap:wrap;margin-top:12px;padding:4px 0}
@container desk-lane (min-width:550px){.desk-filters{grid-template-columns:minmax(140px,1fr) 90px 110px auto}.desk-filters>.n-input{grid-column:auto}}
@container desk-lane (min-width:700px){.desk-records{grid-template-columns:repeat(2,minmax(0,1fr))}}
@container desk-lane (min-width:1060px){.desk-records{grid-template-columns:repeat(3,minmax(0,1fr))}}
@container notice-desk (max-width:760px){
  .notice-desk {gap:8px}
  .notice-desk .notice-desk-head {padding:8px 10px;gap:6px}
  .notice-desk .notice-desk-identity h1 {font-size:18px}
  .desk-mark {width:34px;height:34px;border-radius:9px}
  .desk-mark svg {width:19px}
  .desk-commands .n-button {min-height:40px;padding-inline:9px}
  .desk-types button,.desk-switch button {min-height:44px}
  .desk-band>.desk-types {flex:1 1 100%}
  .desk-switch {display:grid;grid-template-columns:1fr 1fr;padding:3px;border-radius:11px;background:var(--soft-field);box-shadow:var(--soft-inset)}
  .desk-switch button {min-width:0}.desk-switch b {font-variant-numeric:tabular-nums}
  .notice-desk .v2-page-canvas:not(.is-customizing) .lane-head {display:none}
  .desk-lane.desk-hidden,.notice-desk .v2-embedded-block:has(>.desk-hidden),.notice-desk .grid-stack-item:has(.desk-hidden) {display:none}
  .notice-desk .grid-stack-item:has(.desk-lane),.notice-desk .v2-embedded-block:has(>.desk-lane) {grid-column:1/-1}
  .desk-filters .n-button {min-height:40px}
}
@container notice-desk (max-width:430px){
  .desk-commands {width:100%;display:grid;grid-template-columns:repeat(4,minmax(0,1fr))}
  .desk-commands .n-button {padding-inline:3px;font-size:12px}
  .desk-types button {padding-inline:9px}
}

/* Size contracts shared by the notice record and its auxiliary dialogs. */
.v2-notice-editor {width:min(1040px,calc(100vw - 28px));max-height:calc(100dvh - 24px)}
.v2-notice-confirm {width:min(700px,calc(100vw - 28px));max-height:calc(100dvh - 24px)}
.v2-notice-confirm>.n-card-content,.v2-notice-editor>.n-card-content {overflow:auto;min-height:0}
.v2-notice-tools,.v2-notice-editor-footer,.v2-dialog-actions,.v2-notice-submit-group {display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.v2-notice-editor-footer,.v2-dialog-actions {justify-content:flex-end}
.v2-notice-editor-footer>span {flex:1}
.v2-notice-photos {display:flex;gap:8px;flex-wrap:wrap}
.v2-notice-photos figure {position:relative;margin:0;padding:5px;border-radius:8px;border:1px solid var(--soft-edge);max-width:120px;overflow:hidden}
.v2-notice-photos img {max-width:100%;height:70px;object-fit:contain}
.v2-notice-photos figure>span {display:block;font-size:12px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.v2-notice-photos figure button {position:absolute;right:2px;top:2px;background:var(--soft-face)}
.v2-notice-upload {display:inline-flex;align-items:center;position:relative;cursor:pointer}
.v2-notice-upload input {position:absolute;inset:0;width:100%;opacity:0;cursor:pointer}
.desk-editor.v2-notice-editor {width:min(1560px,calc(100vw - 32px));max-height:calc(100dvh - 32px);border-radius:18px;overflow:hidden;background:var(--soft-face)}
.desk-editor>.n-card-header,.desk-dialog>.n-card-header {padding:14px 20px;background:linear-gradient(110deg,var(--desk-sky),var(--desk-mint));align-items:center}
.desk-editor .n-card-header__main {min-width:0}
.desk-editor>.n-card-header .n-card-header__close {min-width:44px;min-height:44px}
.notice-editor-title {display:grid;gap:2px;min-width:0}
.notice-editor-title strong {font-size:18px;line-height:1.4;white-space:normal;overflow-wrap:anywhere;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.desk-editor>.n-card-content {padding:14px 16px;scroll-padding:14px;scrollbar-gutter:stable}
.desk-editor>.n-card__footer {padding:12px 16px;border-top:1px solid var(--soft-edge)}
.desk-editor .n-alert,.desk-dialog .n-alert {margin-bottom:8px}
.desk-editor .v2-notice-loading {display:grid;place-content:center;min-height:200px}
.desk-editor.v2-notice-editor.is-inline-record {width:100%;height:auto;max-height:none;border-radius:0;overflow:visible;background:transparent}
.is-inline-record>.n-card__content {padding:0;overflow:visible}
.is-inline-record .v2-notice-loading {min-height:108px}
.editor-sheet {display:grid;grid-template-columns:minmax(0,1fr) minmax(260px,30%);grid-template-areas:"fields routing";gap:16px;align-items:start}
.editor-routing {grid-area:routing;min-width:0;padding:12px;background:var(--soft-field);border-radius:14px;box-shadow:var(--soft-inset);overflow-wrap:anywhere}
.editor-routing>div,.editor-routing>.v2-plan-binding,.editor-routing>.notice-dingtalk {margin-top:10px}
.editor-routing .v2-notice-tools h2 {font-size:13px;margin:0}
.editor-routing .v2-plan-picker,.editor-routing .v2-plan-picker.has-recommendation {grid-template-columns:minmax(0,1fr) auto;width:100%}
.editor-routing .v2-plan-recommendation {grid-column:1/-1}
.editor-routing .notice-dingtalk {border-color:var(--soft-edge);background:var(--soft-face)}
.editor-routing .v2-plan-options {margin-top:8px}
.editor-section-title {display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:10px;color:var(--v2-ink)}
.editor-section-title>svg {color:var(--v2-blue)}
.editor-section-title>small,.editor-section-title>span {font-size:12px;color:var(--v2-muted);margin-left:auto}
.v2-notice-pane-form {grid-area:fields;min-width:0;container:desk-fields / inline-size}
.desk-editor .v2-notice-field-grid {display:grid;grid-template-columns:repeat(3,minmax(0,1fr));column-gap:12px}
.desk-editor .v2-notice-field-grid .is-wide {grid-column:span 2}
.desk-editor .v2-notice-field-grid .n-form-item {min-width:0;margin:0 0 8px}
.desk-editor .v2-notice-field-grid .n-form-item-label {font-size:12px;padding-bottom:4px}
.desk-editor .n-date-picker {width:100%}
.desk-editor .v2-notice-field-section h2 {margin:0 0 6px;font-size:14px}
.desk-editor .v2-notice-field-section h2>span {font-size:12px;font-weight:400;margin-left:8px;color:var(--v2-muted)}
.desk-editor .v2-notice-pane-form>.v2-notice-field-section,.desk-editor .v2-notice-photo-columns {padding-top:10px;margin-top:8px;border-top:1px solid var(--soft-edge)}
.desk-editor .v2-notice-photo-columns {display:grid;grid-template-columns:repeat(auto-fit,minmax(min(240px,100%),1fr));gap:10px}
.review-columns {display:grid;grid-template-columns:minmax(0,1.5fr) minmax(240px,1fr);gap:14px}
.review-paper,.review-summary {padding:18px;border-radius:13px;min-width:0}
.review-paper {background:var(--soft-field);box-shadow:var(--soft-inset)}
.review-summary {border:1px solid var(--soft-edge)}
.desk-review h3 {margin:0 0 6px;font-size:14px}
.review-paper pre,.review-summary dd {white-space:pre-wrap;overflow-wrap:anywhere;margin:0}
.review-paper pre {font:inherit;line-height:1.85}
.review-summary dl {display:grid;grid-template-columns:auto 1fr;gap:6px 14px;margin:0 0 16px}
.review-summary dt,.review-summary p {color:var(--v2-muted)}
.desk-dialog {border-radius:15px;overflow:hidden}
.desk-dialog>.n-card-content {padding-top:14px}
.undo-list {list-style:none;margin:0;padding:0}
.undo-list li {display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--soft-edge)}
.undo-list li>div {min-width:0;flex:1;overflow-wrap:anywhere}
.undo-list small {display:block;color:var(--v2-muted)}
@container desk-fields (min-width:880px){.desk-editor .v2-notice-field-grid{grid-template-columns:repeat(4,minmax(0,1fr))}}
@container desk-fields (max-width:600px){.desk-editor .v2-notice-field-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@container desk-fields (max-width:420px){.desk-editor .v2-notice-field-grid{grid-template-columns:minmax(0,1fr)}.desk-editor .v2-notice-field-grid .is-wide{grid-column:auto}}
@media(max-width:900px){.editor-sheet {grid-template-columns:minmax(0,1fr);grid-template-areas:"routing" "fields"}}
@media(max-width:760px){
  .desk-editor.v2-notice-editor {width:calc(100vw - 10px);height:calc(100dvh - 10px);max-height:calc(100dvh - 10px);border-radius:12px}
  .desk-editor>.n-card-header,.desk-editor>.n-card-content,.desk-editor>.n-card__footer {padding:10px 12px}
  .notice-editor-title strong {font-size:16px}.desk-editor>.n-card-content {scrollbar-gutter:auto}
  .v2-notice-editor-footer .n-button,.v2-notice-submit-group .n-radio-button {min-height:44px}
  .v2-notice-editor-footer:not(.is-review) .v2-notice-submit-group {justify-content:space-between;flex:1 1 100%}
  .review-columns {grid-template-columns:minmax(0,1fr)}.review-paper,.review-summary {padding:12px}
}
</style>
