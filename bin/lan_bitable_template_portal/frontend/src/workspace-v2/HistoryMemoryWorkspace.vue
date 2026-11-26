<script setup lang="ts">
import {computed,onBeforeUnmount,ref,watch} from 'vue';
import {NButton,NModal,NSelect,useDialog,useMessage} from 'naive-ui';
import {ArrowLeft,ScanSearch} from 'lucide-vue-next';
import {registerNavigationGuard} from './navigation';
import type {Dict} from '../api/client';


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

const historyPage=ref(1);
const pageSize=9;
const historyPages=computed(()=>Math.max(1,Math.ceil(props.r.filteredSources.length/pageSize)));
const historyRows=computed(()=>props.r.filteredSources.slice((historyPage.value-1)*pageSize,historyPage.value*pageSize));
watch(()=>[props.r.query,props.r.filterType,props.r.filterMatch,props.r.filterBuilding,props.r.sourceItems],()=>{historyPage.value=1;});
watch(historyPages,pages=>{if(historyPage.value>pages)historyPage.value=pages;});
</script>

<template>
  <section class="hm-work v2-history" aria-label="历史通告记忆导入">
    <p v-if="checking" role="status">正在检查账号…</p>
    <div v-else-if="!loggedIn"><h2>历史通告记忆导入</h2><a :href="loginUrl">登录灯塔</a></div>
    <p v-else-if="!isAdmin">仅管理员可使用历史通告记忆导入。</p>
    <template v-else>
      <section class="hm-panel">
        <header class="hm-bar v2-history-scan"><h1>历史通告记忆</h1>
          <label>回溯<select v-model.number="r.months" :disabled="r.busy"><option v-for="m in 12" :key="m" :value="m">近 {{ m }} 个月</option></select></label>
          <div class="hm-kinds" aria-label="扫描通告类型"><label v-for="t in r.workTypes" :key="t.value"><input v-model="r.selectedWorkTypes" type="checkbox" :value="t.value" :disabled="r.busy"/>{{ t.label }}</label></div>
          <button class="hm-primary" :disabled="!!r.scanDisabledReason" :title="r.scanDisabledReason" @click="scan"><ScanSearch :size="15"/>{{ r.scanButtonLabel }}</button>
        </header>
        <p v-if="scanned" class="hm-status">{{ scanScope }} · {{ scanMessage }}</p>
        <p v-if="r.message && (!scanned || r.message!==scanMessage) && !r.saveConfirmOpen" :role="r.messageType==='failed'?'alert':'status'">{{ r.message }}</p>
        <p v-for="(warning,index) in r.warnings" :key="index" role="alert">{{ warning }}</p>
        <p v-if="r.scanDisabledReason && !r.busy">{{ r.scanDisabledReason }}</p>
        <p v-if="scanned && !r.sourceItems.length">本次扫描没有当前月事项</p>
      </section>
      <template v-if="r.sourceItems.length">
        <section class="hm-panel hm-directory" :class="{ 'reviewing':r.reviewOpen }">
          <header class="hm-bar"><h2>当前月事项</h2><small>{{ r.filteredSources.length }} / {{ r.sourceItems.length }} 条</small>
            <input v-model="r.query" aria-label="搜索当前月事项" placeholder="名称、楼栋或专业" :disabled="r.busy"/>
            <NSelect class="hm-select" aria-label="筛选楼栋" v-model:value="r.filterBuilding" :disabled="r.busy" :options="[{label:'全部楼栋',value:''},...r.buildingOptions.map((value:string)=>({label:value,value}))]"/>
            <select v-model="r.filterType" aria-label="筛选通告类型" :disabled="r.busy"><option value="">全部类型</option><option v-for="t in r.workTypes" :key="t.value" :value="t.value">{{ t.label }}</option></select>
            <select v-model="r.filterMatch" aria-label="筛选匹配状态" :disabled="r.busy"><option v-for="m in matchOptions" :key="m.value" :value="m.value">{{ m.label }}</option></select>
            <button v-if="r.historyFilterCount" :disabled="r.busy" @click="r.clearHistoryFilters">清空筛选</button>
          </header>
          <div class="hm-line"><label><input type="checkbox" aria-label="勾选筛选结果" :checked="allVisibleSelected" :indeterminate="someVisibleSelected && !allVisibleSelected" :disabled="r.busy || !r.filteredSources.length" @change="selectVisible(($event.target as HTMLInputElement).checked)"/>勾选筛选结果</label>
            <span>已选 {{ r.selectedCount }} 条 · 推荐候选 {{ r.recommendedMatchCount }} 条</span>
            <template v-if="historyPages>1"><button aria-label="上一页事项" :disabled="historyPage<=1" @click="historyPage--">上一页</button><span>{{ historyPage }} / {{ historyPages }}</span><button aria-label="下一页事项" :disabled="historyPage>=historyPages" @click="historyPage++">下一页</button></template>
          </div>
          <div class="hm-grid"><article v-for="item in historyRows" :key="item.id" class="v2-history-row" :class="{current:r.activeSourceId===item.id}">
            <input v-model="r.selectedMap[item.id]" type="checkbox" :aria-label="'勾选 '+item.title" :disabled="r.busy" @change="dirty=true"/>
            <button :disabled="r.busy" @click="r.selectSource(item)"><strong>{{ item.title || item.memory_name || '未命名事项' }}</strong><small>{{ [r.workTypeLabel(item.work_type),item.building,item.specialty,r.candidateIdFor(item.id)?'有历史候选':'未匹配'].filter(Boolean).join(' · ') }}</small></button>
          </article></div>
          <p v-if="!r.filteredSources.length">没有符合筛选条件的事项</p>
        </section>
        <section v-if="r.activeSource" class="hm-panel hm-review v2-history-review" :class="{reviewing:r.reviewOpen}">
          <header class="hm-bar"><button class="hm-listback" @click="r.closeReview"><ArrowLeft :size="14"/>事项列表</button><h2>{{ r.activeSource.title || r.activeSource.memory_name }}</h2>
            <small>{{ r.activeFieldOriginLabel }} · {{ r.activeSource.building }}</small>
            <label><input v-model="r.selectedMap[r.activeSourceId]" type="checkbox" :disabled="r.busy" @change="dirty=true"/>勾选保存</label></header>
          <div class="hm-line v2-history-candidate"><strong>历史候选</strong>
            <NSelect v-model:value="candidateChoice" class="hm-candidates" aria-label="选择历史候选" filterable :disabled="r.busy || !candidateOptions.length" :options="candidateOptions" placeholder="暂无匹配时可手工填写"/>
            <button :disabled="r.busy || !candidateChoice" @click="applyCandidate">套用此候选</button>
            <span>{{ r.activeCandidateHint }}</span></div>
          <div class="hm-fields"><label v-for="f in r.primaryFieldDefs" :key="f.key">{{ f.label }}
            <textarea v-if="f.multi" :id="'history-field-'+f.key" v-model="r.activeFields[f.key]" :disabled="r.busy" rows="2" @input="edited"/>
            <input v-else :id="'history-field-'+f.key" v-model="r.activeFields[f.key]" :disabled="r.busy" @input="edited"/></label></div>
          <div v-if="r.extraFieldDefs.length" class="hm-extra"><button :aria-expanded="r.showExtraFields" @click="r.showExtraFields=!r.showExtraFields">{{ r.showExtraFields?'收起':'展开' }}其他字段（{{ r.extraFieldDefs.length }}）</button>
            <div v-if="r.showExtraFields" class="hm-fields"><label v-for="f in r.extraFieldDefs" :key="f.key">{{ f.label }}
              <textarea v-if="f.multi" v-model="r.activeFields[f.key]" :disabled="r.busy" rows="2" @input="edited"/><input v-else v-model="r.activeFields[f.key]" :disabled="r.busy" @input="edited"/></label></div></div>
        </section>
        <footer class="hm-footer v2-history-footer"><strong>已选 {{ r.selectedCount }} 条</strong><span>将覆盖已有记忆 {{ r.selectedOverwriteCount }} 条</span>
          <div class="hm-save-actions">
            <NButton type="primary" class="hm-primary" :disabled="!!r.saveSelectedDisabledReason" :title="r.saveSelectedDisabledReason" @click="r.saveSelected">预览并保存</NButton>
            <NButton :disabled="!!r.fillRecommendedDisabledReason" :title="r.fillRecommendedDisabledReason" @click="fillRecommended">填充推荐并预览</NButton>
          </div></footer>
      </template>
      <NModal :show="r.saveConfirmOpen" :mask-closable="!r.busy" :close-on-esc="!r.busy" @update:show="(v:boolean)=>{if(!v)r.cancelSaveConfirm()}">
        <div role="dialog" aria-modal="true" :aria-label="r.saveConfirmTitle" class="hm-confirm v2-history-confirm" data-v2-inline-feedback>
          <h2>{{ r.saveConfirmTitle }}</h2><p>保存 {{ r.saveSummary.total }} 条 · 套用历史候选 {{ r.saveSummary.candidate }} 条 · 覆盖已有记忆 {{ r.saveSummary.overwrite }} 条</p>
          <ul><li v-for="item in r.savePreviewItems" :key="item.key"><strong>{{ item.title }}</strong><small>{{ item.type }} · {{ item.building }} · {{ item.origin }}</small></li></ul>
          <p v-if="r.saveSummary.total>r.savePreviewItems.length">另有 {{ r.saveSummary.total-r.savePreviewItems.length }} 条</p>
          <p v-if="r.messageType==='failed'" role="alert">{{ r.message }}</p>
          <div class="hm-line"><NButton :disabled="r.busy" @click="r.cancelSaveConfirm">继续核对</NButton><NButton type="primary" :loading="r.busy" :disabled="r.busy" @click="save">确认保存 {{ r.saveSummary.total }} 条</NButton></div>
        </div>
      </NModal>
    </template>
  </section>
</template>

<style scoped src="./SerenityHistoryWorkspace.css"></style>
