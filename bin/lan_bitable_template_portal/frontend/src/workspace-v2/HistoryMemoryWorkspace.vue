<template>
  <section class="v2-history" :class="{'is-reviewing':r.reviewOpen&&r.activeSource}">
    <header class="v2-history-head"><div><History :size="22" aria-hidden="true" /><h1>历史通告记忆导入</h1></div><span v-if="scanned">{{ scanScope }}</span></header>
    <NSpin v-if="checking" size="small"><div class="v2-history-auth">正在校验管理员身份</div></NSpin>
    <NAlert v-else-if="!loggedIn" type="info" :bordered="false">请先登录后使用历史记忆导入。<a :href="loginUrl">登录灯塔</a></NAlert>
    <NAlert v-else-if="!isAdmin" type="warning" :bordered="false">仅管理员可使用历史通告记忆导入。</NAlert>
    <template v-else>
      <form class="v2-history-scan" @submit.prevent="scan">
        <NFormItem label="历史范围" :show-feedback="false"><NRadioGroup v-model:value="r.months" :disabled="r.busy" aria-label="历史范围"><NRadioButton :value="3">近 3 个月</NRadioButton><NRadioButton :value="6">近 6 个月</NRadioButton></NRadioGroup></NFormItem>
        <NFormItem label="通告类型" :show-feedback="false"><NCheckboxGroup v-model:value="r.selectedWorkTypes" :disabled="r.busy" aria-label="扫描通告类型"><NCheckbox v-for="type in r.workTypes" :key="type.value" :value="type.value">{{ type.label }}</NCheckbox></NCheckboxGroup></NFormItem>
        <NButton type="primary" attr-type="submit" data-v2-navigation :loading="r.scanBusy" :disabled="!!r.scanDisabledReason" :title="r.scanDisabledReason"><template #icon><ScanSearch :size="17" /></template>{{ r.scanButtonLabel }}</NButton>
      </form>
      <NAlert v-if="r.message&&r.message!==scanMessage" :type="r.messageType==='failed'?'error':r.messageType==='success'?'success':'info'" :bordered="false" role="status">{{ r.message }}</NAlert>
      <NAlert v-if="r.warnings.length" type="warning" :bordered="false" title="需要注意"><ul class="v2-history-warnings"><li v-for="(warning,index) in r.warnings" :key="index">{{ warning }}</li></ul></NAlert>
      <div v-if="!scanned&&!r.sourceItems.length" class="v2-history-initial">
        <template v-if="r.scanBusy"><NSkeleton text :repeat="5" /><span>扫描中 · {{ r.scanElapsedSeconds }} 秒</span></template>
        <NEmpty v-else :description="r.messageType==='failed'?'历史扫描未完成':'尚未扫描历史通告'"><template #icon><FolderSearch :size="42" /></template></NEmpty>
      </div>
      <template v-else>
        <div class="v2-history-filters">
          <NInput v-model:value="r.query" clearable placeholder="搜索事项名称、楼栋或专业" :input-props="{'aria-label':'搜索当前月事项'}" :disabled="r.busy"><template #prefix><Search :size="16" /></template></NInput>
          <NSelect v-model:value="r.filterBuilding" :options="[{label:'全部楼栋',value:''},...r.buildingOptions.map((value:string)=>({label:value,value}))]" :disabled="r.busy" aria-label="筛选楼栋" :to="true" />
          <NSelect v-model:value="r.filterType" :options="[{label:'全部类型',value:''},...r.workTypes]" :disabled="r.busy" aria-label="筛选通告类型" :to="true" />
          <NSelect v-model:value="r.filterMatch" :options="matchOptions" :disabled="r.busy" aria-label="筛选匹配状态" :to="true" />
          <NButton v-if="r.historyFilterCount" quaternary :disabled="r.busy" @click="r.clearHistoryFilters">清空筛选</NButton>
        </div>
        <div class="v2-history-summary"><span>当前月事项 <b>{{ r.sourceItems.length }}</b></span><span>历史候选 <b>{{ r.candidates.length }}</b></span><span>推荐可填充 <b>{{ r.recommendedMatchCount }}</b></span></div>
        <div class="v2-history-workspace" :class="{'is-reviewing':r.reviewOpen&&r.activeSource}" :aria-busy="r.busy">
          <section class="v2-history-sources" aria-label="当前月事项">
            <header><h2>当前月事项</h2><span>{{ r.filteredSources.length }} 条</span><NCheckbox :checked="allVisibleSelected" :indeterminate="someVisibleSelected&&!allVisibleSelected" :disabled="r.busy||!r.filteredSources.length" @update:checked="selectVisible">勾选筛选结果</NCheckbox></header>
            <NVirtualList v-if="r.filteredSources.length" class="v2-history-list" :items="r.filteredSources" :item-size="102" key-field="id">
              <template #default="{item}">
                <article :key="item.id" class="v2-history-row" :class="{'is-active':item.id===r.activeSourceId,'is-selected':r.isSelected(item.id)}">
                  <NCheckbox v-model:checked="r.selectedMap[item.id]" :disabled="r.busy" :aria-label="'勾选保存：'+(item.title||item.memory_name)" @update:checked="dirty=true" />
                  <button type="button" :disabled="r.busy" :aria-pressed="item.id===r.activeSourceId" @click="r.selectSource(item)"><strong :title="item.title||item.memory_name">{{ item.title||item.memory_name }}</strong><span>{{ r.workTypeLabel(item.work_type) }} · {{ item.building||'楼栋未填写' }} · {{ item.specialty||'专业未填写' }}</span><small><i :class="r.candidateIdFor(item.id)?'has-candidate':'no-candidate'" />{{ r.candidateIdFor(item.id)?'有历史候选':'未匹配候选' }}<span v-if="r.sourceHasMemory(item)">已有记忆</span></small></button>
                  <ChevronRight :size="16" class="v2-history-row-arrow" aria-hidden="true" />
                </article>
              </template>
            </NVirtualList>
            <NEmpty v-else :description="r.sourceItems.length?'没有符合筛选条件的事项':'本次扫描没有当前月事项'" />
          </section>
          <section v-if="r.activeSource" class="v2-history-review" aria-labelledby="history-review-heading">
            <NButton class="v2-history-back" quaternary :disabled="r.busy" @click="r.closeReview"><template #icon><ArrowLeft :size="17" /></template>事项列表</NButton>
            <header><div><span>{{ r.workTypeLabel(r.activeSource.work_type) }} · {{ r.activeSource.building||'楼栋未填写' }}<template v-if="r.activeSource.source_status"> · {{ r.activeSource.source_status }}</template></span><h2 id="history-review-heading">{{ r.activeSource.title||r.activeSource.memory_name }}</h2></div><NTag size="small" :bordered="false">{{ r.activeFieldOriginLabel }}</NTag></header>
            <div class="v2-history-candidate">
              <NFormItem label="历史候选" :show-feedback="false"><NSelect v-model:value="candidateChoice" filterable :options="candidateOptions" :disabled="r.busy||!candidateOptions.length" :placeholder="candidateOptions.length?'选择历史候选':'暂无同类型、同楼栋候选'" aria-label="选择历史候选" :to="true" /></NFormItem>
              <NButton secondary type="primary" :disabled="r.busy||!candidateChoice" @click="applyCandidate"><template #icon><CopyPlus :size="16" /></template>套用此候选</NButton>
              <span v-if="r.activeCandidateHint">{{ r.activeCandidateHint }}</span>
            </div>
            <NForm class="v2-history-fields" :disabled="r.busy" :show-feedback="false" label-placement="top">
              <NFormItem v-for="field in r.primaryFieldDefs" :key="field.key" :label="field.label" :label-for="'history-field-'+field.key"><NInput v-model:value="r.activeFields[field.key]" :input-props="{id:'history-field-'+field.key}" :type="field.multi?'textarea':'text'" :autosize="field.multi?{minRows:2,maxRows:4}:undefined" :placeholder="field.label" @update:value="edited" /></NFormItem>
            </NForm>
            <NCollapse v-if="r.extraFieldDefs.length" class="v2-history-extra"><NCollapseItem :title="'其他字段 · '+r.extraFieldDefs.length+' 项'" name="extra"><NForm class="v2-history-fields" :disabled="r.busy" :show-feedback="false" label-placement="top"><NFormItem v-for="field in r.extraFieldDefs" :key="field.key" :label="field.label" :label-for="'history-field-'+field.key"><NInput v-model:value="r.activeFields[field.key]" :input-props="{id:'history-field-'+field.key}" :type="field.multi?'textarea':'text'" :autosize="field.multi?{minRows:2,maxRows:4}:undefined" :placeholder="field.label" @update:value="edited" /></NFormItem></NForm></NCollapseItem></NCollapse>
          </section>
        </div>
        <footer v-if="r.sourceItems.length||r.candidates.length" class="v2-history-footer">
          <div><strong>已勾选 {{ r.selectedCount }} 条</strong><span>覆盖已有记忆 {{ r.selectedOverwriteCount }} 条</span></div>
          <div><NButton secondary :disabled="!!r.fillRecommendedDisabledReason" :title="r.fillRecommendedDisabledReason" @click="fillRecommended"><template #icon><CopyPlus :size="16" /></template>填充推荐并预览</NButton><NButton type="primary" :disabled="!!r.saveSelectedDisabledReason" :title="r.saveSelectedDisabledReason" @click="r.saveSelected"><template #icon><Save :size="16" /></template>预览并保存</NButton></div>
        </footer>
      </template>
      <NModal :show="r.saveConfirmOpen" preset="card" title="确认保存历史记忆" class="v2-history-confirm" :style="{width:'min(760px,calc(100vw - 24px))'}" :mask-closable="!r.busy" :close-on-esc="!r.busy" :closable="!r.busy" @update:show="value=>{if(!value)r.cancelSaveConfirm();}">
        <div class="v2-history-confirm-summary"><span>本次保存 <b>{{ r.saveSummary.total }}</b></span><span>候选填充 <b>{{ r.saveSummary.candidate }}</b></span><span>覆盖已有记忆 <b>{{ r.saveSummary.overwrite }}</b></span></div>
        <NAlert v-if="r.saveSummary.overwrite" type="warning" :bordered="false">{{ r.saveSummary.overwrite }} 条已有记忆将被覆盖。</NAlert>
        <NAlert v-if="r.messageType==='failed'" type="error" :bordered="false" role="alert">{{ r.message }}</NAlert>
        <ul class="v2-history-save-list"><li v-for="item in r.savePreviewItems" :key="item.key"><strong>{{ item.title }}</strong><span>{{ item.type }} · {{ item.building||'楼栋未填写' }} · {{ item.origin }}</span></li></ul>
        <p v-if="r.saveSummary.total>r.savePreviewItems.length" class="v2-history-muted">另有 {{ r.saveSummary.total-r.savePreviewItems.length }} 条包含在本次保存中。</p>
        <template #footer><div class="v2-history-confirm-actions"><NButton :disabled="r.busy" @click="r.cancelSaveConfirm">继续核对</NButton><NButton type="primary" :loading="r.busy" :disabled="r.busy" data-v2-submit @click="save"><template #icon><Save :size="16" /></template>确认保存 {{ r.saveSummary.total }} 条</NButton></div></template>
      </NModal>
    </template>
  </section>
</template>

<script setup lang="ts">
import {computed,onBeforeUnmount,ref,watch} from 'vue';
import {NAlert,NButton,NCheckbox,NCheckboxGroup,NCollapse,NCollapseItem,NEmpty,NForm,NFormItem,NInput,NModal,NRadioButton,NRadioGroup,NSelect,NSkeleton,NSpin,NTag,NVirtualList,useDialog,useMessage} from 'naive-ui';
import {ArrowLeft,ChevronRight,CopyPlus,FolderSearch,History,Save,ScanSearch,Search} from 'lucide-vue-next';
import {registerNavigationGuard} from './navigation';
import type {Dict} from '../api/client';
import './history.css';

const props=defineProps<{checking:boolean;loggedIn:boolean;isAdmin:boolean;loginUrl:string;r:any}>();
const dialog=useDialog(),message=useMessage();
const scanned=ref(false),scanScope=ref(''),scanMessage=ref(''),dirty=ref(false),candidateChoice=ref<string|null>(null);
const editedSources=new Set<string>();
const matchOptions=[{label:'全部匹配状态',value:''},{label:'已勾选保存',value:'selected'},{label:'有历史候选',value:'matched'},{label:'未匹配候选',value:'unmatched'}];
const candidateOptions=computed(()=>props.r.candidateOptions.map((item:Dict)=>({value:String(item.id),label:[item.title,item.building,item.business_time].filter(Boolean).join(' · ')})));
const allVisibleSelected=computed(()=>props.r.filteredSources.length>0&&props.r.filteredSources.every((item:Dict)=>props.r.isSelected(item.id)));
const someVisibleSelected=computed(()=>props.r.filteredSources.some((item:Dict)=>props.r.isSelected(item.id)));
watch(()=>props.r.activeSourceId,()=>{candidateChoice.value=props.r.candidateIdFor(props.r.activeSourceId)||null;},{immediate:true});
function confirmReplace(title:string,content:string,proceed:()=>void){dialog.warning({title,content,positiveText:'继续',negativeText:'保留当前填写',onPositiveClick:proceed});}
async function runScan(){
  const r=props.r;if(r.scanDisabledReason)return;
  const scope=`近 ${r.months} 个月 · ${r.workTypes.filter((type:Dict)=>r.selectedWorkTypes.includes(type.value)).map((type:Dict)=>type.label).join('、')}`;
  await r.scanHistory();
  if(r.messageType==='success'){scanned.value=true;scanScope.value=scope;scanMessage.value=r.message;dirty.value=false;editedSources.clear();candidateChoice.value=r.candidateIdFor(r.activeSourceId)||null;}
}
function scan(){if(props.r.busy)return;if(dirty.value)confirmReplace('重新扫描历史通告？','当前未保存的填写和勾选将被替换。',()=>{void runScan();});else void runScan();}
function edited(){dirty.value=true;editedSources.add(props.r.activeSourceId);props.r.touchFields();}
function selectVisible(value:boolean){if(props.r.busy)return;for(const item of props.r.filteredSources)props.r.selectedMap[item.id]=value;dirty.value=true;}
function applyCandidate(){
  const r=props.r,candidate=r.candidateOptions.find((item:Dict)=>item.id===candidateChoice.value),id=r.activeSourceId;if(r.busy||!candidate)return;
  const apply=()=>{if(r.busy||r.activeSourceId!==id)return;r.chooseCandidate(candidate);dirty.value=true;editedSources.delete(id);};
  if(editedSources.has(id))confirmReplace('替换当前填写？','套用历史候选会替换此事项尚未保存的字段。',apply);else apply();
}
function fillRecommended(){
  const r=props.r;if(r.fillRecommendedDisabledReason)return;
  const apply=()=>{if(r.busy)return;r.fillCandidatesAndConfirmSave();dirty.value=true;candidateChoice.value=r.candidateIdFor(r.activeSourceId)||null;};
  if(editedSources.size)confirmReplace('填充推荐候选？','推荐候选可能覆盖本页手工修改；确认后仍需预览并保存。',apply);else apply();
}
async function save(){const r=props.r;if(r.busy)return;await r.confirmSaveSelected();if(r.messageType==='success'){dirty.value=false;editedSources.clear();}}
const stopGuard=registerNavigationGuard((_target,proceed)=>{
  if(props.r.busy){message.warning('正在扫描或保存，请等待操作完成。');return false;}
  if(!dirty.value)return true;
  dialog.warning({title:'历史记忆尚未保存',content:'离开将丢弃本页尚未保存的填写和勾选。',positiveText:'放弃并离开',negativeText:'继续核对',onPositiveClick:()=>{dirty.value=false;proceed();}});return false;
});
function beforeUnload(event:BeforeUnloadEvent){if(dirty.value||props.r.busy){event.preventDefault();event.returnValue='';}}
window.addEventListener('beforeunload',beforeUnload);
onBeforeUnmount(()=>{stopGuard();window.removeEventListener('beforeunload',beforeUnload);});
</script>
