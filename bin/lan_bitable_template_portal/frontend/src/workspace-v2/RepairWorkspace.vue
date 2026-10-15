<template>
  <RepairController v-bind="props" v-slot="{controller:r}">
    <section class="repair-management-page v2-repair-workspace" data-v2-inline-feedback :class="{'has-detail':r.projectDrawerOpen,'repair-status-page':statusEntry}">
      <header class="v2-repair-heading"><div><h1>检修办理</h1><p>维修项目与跟进</p></div><div class="v2-repair-actions">
        <NButton data-v2-navigation :disabled="r.loading||r.v2Writing" :loading="r.loading" @click="r.v2RefreshView"><RefreshCw :size="16" />刷新显示</NButton>
        <NDropdown trigger="click" :options="moreOptions" @select="value=>more(value,r)"><NButton data-v2-navigation aria-label="更多检修操作" :disabled="r.v2Writing"><Ellipsis :size="18" /></NButton></NDropdown>
        <NButton data-v2-navigation type="primary" :disabled="r.v2Writing" @click="r.requestStartCreate"><Plus :size="17" />新建维修单</NButton>
      </div></header>
      <div class="v2-repair-filters">
        <NInput :value="r.searchText" placeholder="搜索维修名称、设备、位置或专业" clearable :input-props="{'aria-label':'搜索维修项目'}" @update:value="r.searchText=$event"><template #prefix><Search :size="16" /></template></NInput>
        <span v-if="r.v2RecordsReady" class="v2-repair-total">{{r.total}} 项</span><NSkeleton v-else width="64px" height="18px" />
      </div>
      <div class="v2-repair-state-row">
        <NRadioGroup :value="r.recordState" aria-label="维修项目状态" :disabled="r.v2Writing" @update:value="r.changeRecordState"><NRadioButton value="active">未完成 <small>{{pendingCount(r)}}</small></NRadioButton><NRadioButton value="completed">已完成 <small>{{stat(r,'completed_total')}}</small></NRadioButton><NRadioButton value="all">全部</NRadioButton></NRadioGroup>
        <span v-if="r.v2StatsLoading" role="status" class="v2-repair-muted"><NSpin :size="13" />读取状态数量</span><span v-else-if="r.v2StatsError" class="v2-repair-muted v2-repair-stats-error" role="status">数量暂不可用 <NButton text size="tiny" data-v2-navigation @click="r.v2LoadStats">重试</NButton></span><span v-else-if="r.v2StatsAt" class="v2-repair-muted">状态更新 {{r.v2StatsAt}}</span>
        <NRadioGroup v-if="r.recordState==='completed'" :value="r.recordHistoryPeriod" aria-label="已完成时间范围" :disabled="r.loading||r.v2Writing" @update:value="r.changeRecordHistoryPeriod"><NRadioButton v-for="period in r.recordHistoryPeriodOptions" :key="period.value" :value="period.value">{{period.label}}</NRadioButton></NRadioGroup>
      </div>
      <NAlert v-if="r.v2RecordError" type="error" :bordered="false" role="alert">{{r.v2RecordError}}<template v-if="r.v2RecordsReady">；暂显示上次读取的“{{r.v2AppliedState}}”列表。</template><NButton text data-v2-navigation @click="r.v2RefreshView">重新读取</NButton></NAlert>
      <NAlert v-else-if="r.repairHealthVisible" :type="r.repairHealthFailedCount?'warning':'info'" :bordered="false">{{r.repairHealthText}}<NButton text data-v2-navigation @click="r.openFailureCenter">查看任务</NButton><NButton v-if="r.repairHealthRepairableCount&&r.repairHealth?.can_repair" text :loading="r.integrityRepairing" @click="r.repairIntegrity">修复关联</NButton></NAlert>
      <NAlert v-if="r.messageText && !r.projectDrawerOpen && !r.v2RecordError" :type="tone(r.messageTone)" :bordered="false" role="status">{{r.messageText}}</NAlert>
      <div class="v2-repair-body">
        <section ref="listElement" class="v2-repair-list" aria-label="维修项目列表">
          <div v-if="r.loading && r.records.length" class="v2-repair-refreshing" role="status"><NSpin :size="15" />正在读取“{{r.v2StateLabel}}”，暂显示原列表</div>
          <div class="record-list" :aria-busy="r.loading">
            <div v-if="r.loading&&!r.records.length" class="v2-repair-skeleton"><NSkeleton v-for="i in 5" :key="i" height="90px" :sharp="false" /></div>
            <NEmpty v-else-if="!r.records.length" :description="r.v2RecordError?'列表暂不可用，请重试':r.recordEmptyText" />
            <div v-else class="record-page-list">
              <article v-for="record in r.records" :key="record.record_id" class="record-row" :class="{active:r.selectedRecord?.record_id===record.record_id,completed:r.repairRecordIsCompleted(record),'is-loading':r.loading}" tabindex="0" role="button" :aria-label="'打开维修项目：'+record.title" @click="r.requestSelectRecord(record,'followups')" @keydown.enter.prevent="r.requestSelectRecord(record,'followups')" @keydown.space.prevent="r.requestSelectRecord(record,'followups')">
                <div class="v2-repair-row-copy"><strong :title="record.title">{{record.title || '未命名维修项目'}}</strong><div class="v2-repair-row-meta"><span>{{r.recordBuildingLabel(record)}}</span><span>{{r.v2SpecialtyLabel(record)}}</span><span><Clock3 :size="12" />{{r.recordFaultTimeLabel(record)}}</span></div></div>
                <div class="v2-repair-row-state"><NTag size="small" :bordered="false" :type="r.repairRecordStateLocked(record)?'warning':r.repairRecordIsCompleted(record)?'success':'info'">{{r.repairRecordStateLocked(record)?'状态待核验':record.workflow||'状态未填写'}}</NTag><div class="v2-repair-row-progress"><NProgress v-if="r.v2ProgressKnown(record)" type="line" :height="4" :percentage="r.recordProgressPercent(record)" :show-indicator="false" /><small>{{r.v2ProgressKnown(record)?r.recordProgressPercent(record)+'%':'进度待核验'}}</small></div></div>
                <NButton data-v2-navigation secondary type="primary" size="small" class="record-followup-button" :disabled="r.loading||r.v2Writing" @click.stop="r.requestSelectRecord(record,'followups')"><MessageSquarePlus :size="15" /><span>跟进 {{record.followup_count ?? '—'}}</span></NButton>
              </article>
            </div>
          </div>
          <footer class="v2-repair-pager"><span v-if="r.v2RecordsReady">{{r.total}} 项</span><NPagination :page="r.recordPage" :page-count="r.recordPageCount" :disabled="r.loading||r.v2Writing" :page-slot="5" @update:page="value=>changePage(r,value)" /></footer>
        </section>
        <Transition name="v2-repair-detail">
          <section v-if="r.projectDrawerOpen" :ref="r.v2SetDetail" class="repair-project-drawer v2-repair-detail" tabindex="-1" role="region" aria-labelledby="repair-detail-title" @keydown="r.handleProjectDrawerKeydown">
            <header class="v2-repair-detail-head"><div><small>{{r.editingRecordId?'维修项目':'新建维修单'}}</small><h2 id="repair-detail-title">{{r.editingRecordId?r.selectedRecordHeaderTitle:'填写维修项目'}}</h2></div><NButton data-v2-navigation quaternary circle aria-label="关闭维修项目" :disabled="r.v2Writing" @click="r.requestCloseProjectDrawer"><X :size="21" /></NButton></header>
            <div v-if="r.editingRecordId" class="v2-repair-detail-summary"><NTag :bordered="false" :type="r.selectedRecordStateLocked?'warning':r.repairRecordIsCompleted(r.selectedRecord||{})?'success':'info'">{{r.selectedRecordStateLocked?'状态待核验':r.selectedRecord?.workflow||'状态未填写'}}</NTag><span>{{r.recordBuildingLabel(r.selectedRecord||{})}} · {{r.v2SpecialtyLabel(r.selectedRecord||{})}}</span><span>{{r.v2ProgressKnown(r.selectedRecord||{})?'当前进度 '+r.recordProgressPercent(r.selectedRecord||{})+'%':'进度待核验'}}</span></div>
            <div class="project-drawer-body v2-repair-detail-body">
              <NSkeleton v-if="r.recordDetailLoading" text :repeat="8" />
              <template v-else>
                <NAlert v-if="r.selectedRecordReadOnly" type="warning" :bordered="false">{{r.selectedRecordStateLocked?'跟进状态尚未核验，当前只读。':'当前项目只读。'}}</NAlert>
                <NAlert v-if="r.projectSyncStatus?.status==='pending'||r.projectSyncStatus?.status==='failed'" :type="r.projectSyncStatus.status==='failed'?'warning':'info'" :bordered="false"><NSpin v-if="r.projectSyncStatus.status==='pending'" :size="14" />{{r.projectSyncStatusText}}<NButton v-if="r.projectSyncStatus.status==='failed'" text :loading="r.syncRetrying" @click="r.retryProjectSync">重试同步</NButton></NAlert>
                <NAlert v-if="r.submission.pending.value" :type="r.submission.status.value==='failed'?'error':'warning'" :bordered="false">{{r.submission.message.value}}<NButton text :disabled="r.submission.checking.value" @click="r.submission.check(true,true)">核验原任务</NButton></NAlert>
                <NAlert v-if="r.projectConflict" type="warning" :bordered="false">{{r.projectConflict.message}}<div class="v2-repair-inline-actions"><NButton data-v2-navigation size="small" @click="r.copyCurrentProjectDraft">复制当前填写</NButton><NButton size="small" :loading="r.recordDetailLoading" @click="r.reloadLatestProject">读取最新内容</NButton></div></NAlert>
                <NAlert v-else-if="r.messageText" :type="tone(r.messageTone)" :bordered="false">{{r.messageText}}</NAlert>
                <NRadioGroup :value="r.activeWorkspaceTab" aria-label="维修办理内容" :disabled="r.v2Writing" @update:value="r.openWorkspaceTab"><NRadioButton v-if="r.editingRecordId" value="followups">跟进记录 {{r.selectedFollowupCount}}</NRadioButton><NRadioButton value="project">维修单信息</NRadioButton></NRadioGroup>
                <NAlert v-if="r.activeWorkspaceTab==='followups'&&!r.selectedRecordReadOnly&&(!r.sourceEventId||!r.selectedRepairIds.length)" type="info" :bordered="false" class="v2-repair-source-hint"><div class="v2-repair-inline-actions"><span>{{!r.sourceEventId?'尚未关联事件与检修通告':'尚未关联检修通告'}}</span><NButton data-v2-navigation type="primary" secondary @click="r.openWorkspaceTab('project')">关联并自动填入</NButton></div></NAlert>
                <div v-if="r.editingRecordId&&r.followupPanelMounted" v-show="r.activeWorkspaceTab==='followups'"><RepairFollowups :ref="value=>r.v2FollowupApi=value" embedded :scope="scope" :user-id="userId" :summary-record-id="r.editingRecordId" :summary-title="r.selectedRecordTitle" :read-only="r.selectedRecordReadOnly" :can-delete="canDelete" :refresh-version="r.repairSourceRefreshVersion" @changed="r.handleFollowupChanged" @source-refreshed="r.handleRepairSourceRefreshed" @dirty-changed="r.followupHasUnsavedChanges=$event" @count-changed="r.updateSelectedFollowupCount" @busy="r.v2FollowupBusy=$event" /></div>
                <div v-show="r.activeWorkspaceTab==='project'" class="v2-repair-project-form">
                  <section class="v2-repair-source-flow" aria-label="关联资料自动填写" :aria-busy="r.prefillLoading">
                    <header><div><h3>关联资料</h3><span>事件与检修通告中的资料会自动带入维修单</span></div><NTag :bordered="false" :type="r.sourceEventId&&r.selectedRepairIds.length?'success':'warning'">{{Number(Boolean(r.sourceEventId))+Number(Boolean(r.selectedRepairIds.length))}} / 2 已关联</NTag></header>
                    <ol>
                      <li :class="{'is-complete':r.sourceEventId}"><span class="v2-repair-source-step"><Check v-if="r.sourceEventId" :size="17" /><template v-else>1</template></span><div class="v2-repair-source-copy"><strong>关联事件</strong><p>{{r.sourceEventId?r.selectedEventSummary:'待选择事件'}}</p><div v-if="!r.selectedRecordReadOnly" class="v2-repair-source-actions"><NButton data-v2-navigation :type="r.sourceEventId?'default':'primary'" :disabled="r.saving||r.prefillLoading" @click="r.openSourcePicker('event')">{{r.sourceEventId?'更换事件':'选择事件并带入资料'}}</NButton><NButton v-if="r.sourceEventId" data-v2-navigation quaternary :disabled="r.saving||r.prefillLoading" aria-label="解除关联事件" @click="r.clearEventSelection">解除</NButton></div></div></li>
                      <li :class="{'is-complete':r.selectedRepairIds.length}"><span class="v2-repair-source-step"><Check v-if="r.selectedRepairIds.length" :size="17" /><template v-else>2</template></span><div class="v2-repair-source-copy"><strong>关联检修通告</strong><p>{{r.selectedRepairIds.length?r.selectedRepairSummary:r.sourceEventId?'待选择检修通告':'待完成事件关联'}}</p><div v-if="!r.selectedRecordReadOnly" class="v2-repair-source-actions"><NButton data-v2-navigation :type="r.selectedRepairIds.length?'default':'primary'" :disabled="r.saving||r.prefillLoading||!r.sourceEventId" @click="r.openSourcePicker('repair')">{{r.selectedRepairIds.length?'更换检修通告':'选择检修通告并带入资料'}}</NButton><NButton v-if="r.selectedRepairIds.length" data-v2-navigation quaternary :disabled="r.saving||r.prefillLoading" aria-label="解除关联检修通告" @click="r.clearRepairSelection">解除</NButton></div></div></li>
                    </ol>
                    <div v-if="r.prefillLoading" class="v2-repair-source-status" role="status"><NSpin :size="15" />正在自动填入关联资料</div>
                  </section>
                  <NAlert v-if="r.prefillWarnings.length" type="warning" :bordered="false">{{r.prefillWarnings.join('；')}}</NAlert>
                  <NAlert v-if="r.validationAttempted&&r.missingRequiredEditableFields.length" type="warning" :bordered="false">还缺 {{r.missingRequiredEditableFields.length}} 项：{{r.missingRequiredEditableFields.join('、')}}</NAlert>
                  <NSpin :show="r.prefillLoading"><div class="v2-repair-field-sections"><section v-for="group in r.projectFieldGroups" :key="group.key"><h3 v-if="group.label">{{group.label}}</h3><div class="v2-repair-field-grid"><template v-for="field in group.fields" :key="field.field_name"><RepairPeoplePicker v-if="r.isProjectWorkerField(field)" :scope="scope" :input-id="r.fieldInputId(field)" :label="r.projectFieldLabel(field.field_name)" :model-value="r.projectWorkerPeople" :disabled="r.saving||r.selectedRecordReadOnly" @update:model-value="r.updateProjectWorkerPeople" @edited="r.markFieldDirty(field.field_name)" /><RepairField v-else :field="field" :input-id="r.fieldInputId(field)" :label="r.projectFieldLabel(field.field_name)" :model-value="r.fieldDraft[field.field_name]" :required="r.isRequiredField(field.field_name)" :error="r.fieldValidationError(field.field_name)" :disabled="r.saving||r.selectedRecordReadOnly||r.projectFieldIsSourceControlled(field)||r.isCurrentProjectProgressField(field)" :percentage="r.isCurrentProjectProgressField(field)" :wide="r.usesTextarea(field.field_name)" @update:model-value="r.fieldDraft[field.field_name]=$event" @edited="r.markFieldDirty(field.field_name)" /></template></div></section></div></NSpin>
                </div>
              </template>
            </div>
            <footer v-if="!r.recordDetailLoading" class="v2-repair-detail-footer">
              <div class="v2-repair-save-state" :class="r.projectSaveStateTone"><span v-if="r.activeWorkspaceTab==='project'">{{r.projectSaveStateText}}</span><span v-else-if="r.followupHasUnsavedChanges">跟进有未保存修改</span></div>
              <div class="v2-repair-inline-actions">
                <NButton v-if="r.editingRecordId&&noticeAllowed&&!r.selectedRecordReadOnly" data-v2-navigation :disabled="r.v2Writing" @click="r.requestOpenRepairNoticeWorkbench"><FileText :size="15" />检修通告</NButton>
                <NDropdown v-if="r.activeWorkspaceTab==='project'&&!r.selectedRecordReadOnly" trigger="click" :options="recordOptions(r)" @select="value=>recordMore(value,r)"><NButton data-v2-navigation :disabled="r.v2Writing" aria-label="维修单更多操作"><Ellipsis :size="17" /></NButton></NDropdown>
                <NButton v-if="r.activeWorkspaceTab==='project'&&!r.selectedRecordReadOnly" type="primary" :loading="r.saving" :disabled="!r.canSaveRecord" @click="r.saveRecord"><Save :size="16" />{{r.editingRecordId?'保存修改':'保存维修单'}}</NButton>
                <NButton v-if="r.activeWorkspaceTab==='followups'&&r.v2FollowupApi?.action.canDelete" data-v2-navigation quaternary type="error" :disabled="r.v2Writing" @click="r.v2FollowupApi.remove">删除这条</NButton>
                <NButton v-if="r.activeWorkspaceTab==='followups'&&r.v2FollowupApi?.action.editable" type="primary" :loading="r.v2FollowupApi.action.busy" :disabled="r.v2FollowupApi.action.disabled" @click="r.v2FollowupApi.save"><Save :size="16" />{{r.v2FollowupApi.action.label}}</NButton>
              </div>
            </footer>
          </section>
        </Transition>
      </div>
      <RepairTaskCenter :open="r.failureCenterOpen" :loading="r.failureCenterLoading" :items="r.failureCenterItems" :failed-count="r.failureCenterFailedCount" :pending-count="r.failureCenterPendingCount" :integrity-count="r.repairHealthIntegrityCount" :can-reconcile="r.failureCenterCanReconcile" :message="r.failureCenterMessage" :message-tone="r.failureCenterMessageTone" :retrying="r.globalSyncRetrying" :integration-checking="r.integrationChecking" :reconciling="r.reconcilingRemote" @close="r.failureCenterOpen=false" @retry="r.retryAllRepairSyncFromCenter" @integration-check="r.runIntegrationCheck" @reconcile="r.runRemoteReconcile" />
      <ConfirmDialog :open="r.confirmDialog.open" :tone="r.confirmDialog.tone" :kicker="r.confirmDialog.kicker" :title="r.confirmDialog.title" :message="r.confirmDialog.message" :details="r.confirmDialog.details" :confirm-label="r.confirmDialog.confirmLabel" @resolve="r.resolveConfirmation" />
      <RecordPickerDialog :open="r.activePicker==='event'" title="选择关联事件单" :records="r.eventCandidates" :columns="r.eventPickerColumns" :selected-ids="r.eventPickerSelectedIds" :multiple="false" :loading="r.eventLoading" :has-more="r.eventCandidatesHaveMore" :result-note="r.eventCandidateNote" :query="r.eventSearchText" search-placeholder="搜索事件名称、楼栋或专业" @update:query="r.eventSearchText=$event" @search="r.loadEventCandidates(true)" @load-more="r.loadEventCandidates(false)" @close="r.activePicker=''" @confirm="r.confirmEventSelection" />
      <RecordPickerDialog :open="r.activePicker==='repair'" title="选择设备检修通告" :records="r.repairCandidates" :columns="r.repairPickerColumns" :selected-ids="r.repairPickerSelectedIds" :multiple="false" :loading="r.repairCandidateLoading" :has-more="r.repairCandidatesHaveMore" :result-note="r.repairCandidateNote" :query="r.repairSearchText" search-placeholder="搜索通告名称、设备或位置" @update:query="r.repairSearchText=$event" @search="r.loadRepairCandidates(true)" @load-more="r.loadRepairCandidates(false)" @close="r.activePicker=''" @confirm="r.confirmRepairSelection" />
    </section>
  </RepairController>
</template>
<script setup lang="ts">
import { computed,ref } from 'vue';
import { NAlert,NButton,NDropdown,NEmpty,NInput,NPagination,NProgress,NRadioButton,NRadioGroup,NSkeleton,NSpin,NTag } from 'naive-ui';
import { Check,Clock3,Ellipsis,FileText,MessageSquarePlus,Plus,RefreshCw,Save,Search,X } from 'lucide-vue-next';
import RepairController from '../components/RepairManagementPage.vue';
import RepairFollowups from './RepairFollowups.vue';
import RepairField from './RepairField.vue';
import RepairPeoplePicker from '../components/RepairPeoplePicker.vue';
import RepairTaskCenter from '../components/RepairTaskCenter.vue';
import RecordPickerDialog from '../components/RecordPickerDialog.vue';
import ConfirmDialog from './RepairConfirmation.vue';
import { canUseModule } from '../api/modulePermissions';
import { logicalUrl } from './version';
import type { ScopeOption } from '../types';
import './repair.css';
const props=defineProps<{scope:string;scopeOptions:ScopeOption[];focusRecordId?:string;canDelete?:boolean;userId?:string}>();
defineOptions({inheritAttrs:false});
const noticeAllowed=computed(()=>canUseModule('repair'));
const statusEntry=computed(()=>logicalUrl().pathname==='/repair-status');
const listElement=ref<HTMLElement|null>(null);
function changePage(r:any,value:number){r.changeRecordPage(value-r.recordPage);listElement.value?.scrollIntoView({block:'start',behavior:'auto'});}
const moreOptions=[{key:'tasks',label:'任务中心'},{key:'sync',label:'同步飞书数据'}];
const tone=(value:string)=>value==='failed'?'error':value==='warning'?'warning':'success';
function stat(r:any,key:string){return r.v2Count(key) ?? '—';}
function pendingCount(r:any){return stat(r,'total');}
function more(key:string,r:any){if(key==='tasks')r.openFailureCenter();if(key==='sync')void r.refreshRepairSources();}
function recordOptions(r:any){return [{key:'reset',label:'放弃本次修改'},...(props.canDelete&&r.editingRecordId?[{key:'delete',label:'删除维修项目'}]:[])];}
function recordMore(key:string,r:any){if(key==='reset')r.requestResetDraft();if(key==='delete'&&props.canDelete)r.requestDeleteRecord();}
</script>
