<template>
  <section class="inspection-page" aria-label="日常巡检" data-v2-inline-feedback>
    <header class="inspection-heading">
      <NButton v-if="!embedded || historyOpen || sheetOpen" quaternary aria-label="返回" @click="back"><ArrowLeft :size="18" /></NButton>
      <div class="inspection-title"><h1>日常巡检</h1><p>{{ duty ? dutyLabel(duty) : '设备参数与测点读数' }}</p></div>
      <div class="inspection-actions inspection-heading-tools">
        <NButton v-if="state?.history_scopes?.length && !sheetOpen" size="small" :type="historyOpen ? 'primary' : 'default'"
          :aria-pressed="historyOpen" :aria-label="historyOpen ? '返回本班' : '历史巡检'" :disabled="!!busy"
          @click="toggleHistory">{{ historyOpen ? '返回' : '历史' }}</NButton>
        <NButton v-if="state?.history_scopes?.length && !historyOpen && !sheetOpen" size="small" aria-label="巡检表与导出" :disabled="!!busy" @click="sheetOpen=true">巡检表</NButton>
        <NButton v-if="!historyOpen && !sheetOpen && (!active || pollError)" size="small" :disabled="!!busy || autoSaving" :loading="busy==='load'" @click="load">刷新</NButton>
      </div>
    </header>
    <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" role="status">{{ message }}</NAlert>
    <details v-if="recoveries.length" class="inspection-recovery" open>
      <summary>有 {{ recoveries.length }} 轮本机修改需要核对</summary>
      <p>本轮结束时仍有未确认的本机修改，请在历史巡检核对是否已归档。</p>
      <div v-for="recovery in recoveries" :key="recovery.key" class="inspection-recovery-item">
        <strong>{{ dutyLabel(recovery.duty) }}第 {{ recovery.round }} 次</strong>
        <div v-for="(draft,id) in recovery.drafts" :key="id">
          <b>{{ draft.label || id }}</b>
          <span v-for="(value,parameter) in draft.values" :key="parameter"> · {{ draft.parameterLabels?.[parameter] || parameter }}：{{ value || '空' }}</span>
          <span v-if="draft.result"> · {{ draft.result }}</span><span v-if="draft.note"> · {{ draft.note }}</span>
        </div>
        <NButton size="small" @click="recoveryRemoval=recovery">已核对，清除本机留存</NButton>
      </div>
    </details>
    <p v-if="storageError && hasDrafts && active && !active.pending" class="warning" role="status">浏览器暂存不可用，请保持页面打开，等待自动保存完成。</p>
    <p v-if="!state && busy">正在读取巡检状态…</p>
    <DailyInspectionHistory v-show="historyOpen" v-if="historyVisited && state?.history_scopes?.length"
      ref="historyPage" :scopes="state.history_scopes" :default-scope="duty?.scope" @close="historyOpen=false" />
    <DailyInspectionSheet v-if="sheetOpen && state?.history_scopes?.length" :scopes="state.history_scopes"
      :default-scope="state.view_scope || duty?.scope" :default-date="duty?.date" :active="active || undefined"
      :drafts="submissionReviews()" :read-only="readOnly || inputsLocked" :save-label="autoLabel"
      :save-error="autoStatus==='error'" :can-retry="!conflicts.size && !orphanedDrafts.length && !autoSaving"
      :conflict-devices="[...conflicts].map(id=>({id,name:byId.get(id)?.name || '原表补填'}))"
      @resolve-conflict="resolveConflict" @retry-save="retryAutoSave" @update-reading="setReadingValue" @close="sheetOpen=false" />
    <template v-if="state && !historyOpen && !sheetOpen">
      <div v-if="state.view_scopes?.length" class="inspection-actions inspection-view-scopes" aria-label="查看楼栋">
        <span>查看楼栋</span>
        <NButton v-for="scope in state.view_scopes" :key="scope" size="small"
          :type="state.view_scope===scope?'primary':'default'" :aria-pressed="state.view_scope===scope"
          :disabled="!!busy" @click="switchView(scope)">{{ scope }}楼</NButton>
        <span v-if="readOnly" class="muted">查看模式</span>
      </div>
      <p v-if="!duty">{{ state.duty_warning || '当前排班没有可巡检的 D 楼或 E 楼。如排班有变更，请刷新状态。' }}</p>
      <section v-else-if="!active" class="inspection-summary">
        <template v-if="readOnly">
          <h2>{{ duty.scope }}楼当前巡检</h2>
          <p>本楼暂无进行中的巡检。</p>
        </template>
        <template v-else>
          <div class="inspection-actions">
            <h2>{{ duty.scope }}楼本班巡检</h2>
            <span>已完成 {{ state.completed_rounds }} / {{ state.round_limit }} 次</span>
          </div>
          <p v-if="state.completed_rounds>=state.round_limit">本楼本班巡检已全部完成。</p>
          <p v-else>选择本轮专业；全部确认后统一提交归档。</p>
        </template>
        <p v-if="previewBusy && !preview" role="status">正在读取最新设备数据…</p>
        <p v-else-if="previewError && !preview" class="warning" role="alert">{{ previewError }}</p>
        <template v-else-if="preview">
          <p v-if="previewError" class="warning" role="alert">{{ previewError }}</p>
          <p v-if="preview.cache_stale" class="warning" role="status">当前显示上次核验的数据（{{ time((preview.cache_refreshed_at || 0)*1000) }}），后台会重试更新；开始巡检前会再次核验。</p>
          <template v-if="!readOnly && state.completed_rounds<state.round_limit">
            <div class="inspection-actions profession-choices" aria-label="本轮巡检专业">
              <label v-for="item in availableProfessions" :key="item.name">
                <input v-model="selectedProfessions" type="checkbox" :value="item.name"> {{ item.name }} · {{ item.count }} 台
              </label>
            </div>
            <NButton type="primary" :loading="busy==='start'" :disabled="!!busy || !selectedProfessions.length"
              @click="start">开始第 {{ state.completed_rounds+1 }} 次巡检</NButton>
          </template>
          <p>最新参考数据 · {{ time(preview.batch) }}<span v-if="!readOnly && state.completed_rounds<state.round_limit"> · 开始巡检后可修改</span></p>
          <p v-if="!previewDevices.length">请选择专业查看设备。</p>
          <div v-if="previewDevices.length" class="room-navigation" aria-label="按楼层和房间查看">
            <div class="inspection-actions"><strong>楼层</strong><button v-for="floor in floors" :key="floor" type="button" :aria-pressed="selectedFloor===floor" @click="selectFloor(floor)">{{ floor }}</button></div>
            <button v-if="unplacedCount" class="unplaced-button" type="button" @click="selectFloor('待核对')">位置待核对 {{ unplacedCount }} 台</button>
            <div v-if="selectedFloor" class="inspection-actions"><strong>房间</strong><button v-for="room in rooms" :key="room" type="button" :aria-pressed="selectedRoom===room" @click="selectedRoom=room">{{ room }}</button></div>
          </div>
          <div class="inspection-devices">
            <article v-for="item in visiblePreview" :key="item.id" class="inspection-device">
              <h2>{{ item.name }}</h2>
              <p>{{ [item.profession,devicePlace(item).unit,item.sections[0]?.location || item.points[0]?.location].filter(Boolean).join(' · ') }}</p>
              <div class="inspection-readings">
                <div v-for="row in item.parameters" :key="row.id" class="inspection-preview-row">
                  <span>{{ row.name }}</span><strong>{{ shownValue(row.reference) }}</strong>
                </div>
              </div>
              <p v-if="!item.parameters.length" class="muted">暂无可显示参数。</p>
            </article>
          </div>
        </template>
      </section>
      <template v-else>
        <section class="inspection-summary">
          <div class="inspection-actions">
            <strong>{{ active.duty.scope }}楼 · {{ active.duty.shift }}班第 {{ active.round }} / {{ state.round_limit }} 次</strong>
            <span v-if="active.pending" class="warning">待归档</span>
            <span v-else>{{ devices.length }} 台设备</span>
            <span v-if="missingDevices" class="warning">{{ missingDevices }} 台数据待核对</span>
            <div v-if="!readOnly && !active.pending" class="inspection-actions auto-status" role="status">
              <span :class="autoStatus==='error'?'warning':'muted'">{{ autoLabel }}</span>
              <NButton v-if="autoStatus==='error' && !conflicts.size && !orphanedDrafts.length" size="small" :disabled="autoSaving || !!busy" @click="retryAutoSave">重试保存</NButton>
            </div>
            <NButton size="small" :disabled="!!busy || !!active.pending || autoSaving" @click="openOrder">排序</NButton>
          </div>
          <div v-if="active.professions?.length" class="inspection-actions profession-progress">
            <span v-for="item in active.professions" :key="item" :class="professionConfirmed(item)?'done':'muted'">
              {{ item }} {{ professionConfirmed(item) ? '已确认' : '待确认' }}
            </span>
          </div>
          <p class="inspection-batch" :title="'数据批次 '+time(batch?.batch)+' · '+batch?.count+' 条源数据；可直接修改，最后统一确认。'">数据批次 {{ time(batch?.batch) }} · {{ batch?.count }} 条；可直接修改，最后统一确认。</p>
          <p v-if="active.duty.date!==duty.date || active.duty.shift!==duty.shift" class="warning">
            此轮始于 {{ dutyLabel(active.duty) }}，归档计入原班次。
          </p>
          <div class="inspection-filters">
            <input v-model="search" type="search" placeholder="搜索设备、位置或测点" aria-label="搜索设备">
            <select v-model="profession" aria-label="专业筛选" data-native-select>
              <option value="">全部专业</option><option v-for="item in professions" :key="item">{{ item }}</option>
            </select>
            <label title="只看数据缺失"><input v-model="onlyMissing" type="checkbox" aria-label="只看数据缺失"> 缺失</label>
          </div>
          <div class="room-navigation" aria-label="按楼层和房间查看">
            <div class="inspection-actions"><strong>楼层</strong><button v-for="floor in floors" :key="floor" type="button" :aria-pressed="selectedFloor===floor" @click="selectFloor(floor)">{{ floor }}</button></div>
            <button v-if="unplacedCount" class="unplaced-button" type="button" @click="selectFloor('待核对')">位置待核对 {{ unplacedCount }} 台</button>
            <div v-if="selectedFloor" class="inspection-actions"><strong>房间</strong><button v-for="room in rooms" :key="room" type="button" :aria-pressed="selectedRoom===room" @click="selectedRoom=room">{{ room }}</button></div>
          </div>
          <p v-if="pollError" class="warning" role="status">{{ pollError }}</p>
        </section>
        <p v-if="active.pending" class="warning" role="status">{{ readOnly ? '本轮已提交，正在等待归档确认。' : '本轮数据已锁定，可重试同一轮归档。' }}</p>
        <section v-if="filtered.length" class="inspection-sheet" aria-label="设备参数核对">
          <div class="inspection-devices">
            <article v-for="item in filtered" :id="deviceAnchor(item.id)" :key="item.id" class="inspection-device"
              @focusin="focusedDevice=item.id" @focusout="leaveDevice(item.id,$event)">
              <header class="inspection-device-heading">
                <h2 tabindex="-1"><span class="sequence">{{ positions.get(item.id) }}</span>{{ item.name }}</h2>
                <span v-if="devicePlace(item).unit" class="device-profession">{{ devicePlace(item).unit }}</span>
                <span class="device-profession">{{ item.profession }}</span>
                <span v-if="conflicts.has(item.id)" class="warning">待核对共享修改</span>
                <span v-else-if="missingByDevice.get(item.id)" class="warning">{{ missingByDevice.get(item.id) }}</span>
                <span v-else-if="deviceModified(item)" class="warning">已修改</span>
              </header>
              <p v-if="!item.parameters.length" class="muted">源表暂无参数。</p>
              <div class="inspection-readings">
              <div v-for="row in item.parameters" :key="row.id" class="inspection-reading">
                <div><strong>{{ row.name }}</strong><small v-if="item.parameters.filter(value=>value.name===row.name).length>1">{{ parameterLocation(item,row) }}</small><small v-if="mappingIssue(item,row)" class="warning" :title="mappingIssue(item,row)">原表位置待核对</small></div>
                <div class="inspection-value">
                  <span v-if="row.read_only" class="muted">{{ shownValue(row.reference) }} · 已停用</span>
                  <select v-else-if="row.value_type==='状态'" :value="readingFor(item.id,row)" :disabled="inputsLocked" data-native-select
                    :class="{'missing-reading':!readingFor(item.id,row).trim()}" :aria-label="item.name+' '+row.name+'巡检状态'" @change="setReading(item.id,row.id,$event)">
                    <option v-if="readingFor(item.id,row) && !['√','×','△'].includes(readingFor(item.id,row))" :value="readingFor(item.id,row)">原值 {{ readingFor(item.id,row) }}（待核对）</option>
                    <option value="">待核对</option><option value="√">√ 正常</option><option value="×">× 异常</option><option value="△">△ 未运行</option>
                  </select>
                  <input v-else :value="readingFor(item.id,row)" :disabled="inputsLocked"
                    :inputmode="numericValue(row.reference)===null ? 'text' : 'decimal'" maxlength="160"
                    class="numeric" :class="{'missing-reading':!readingFor(item.id,row).trim()}"
                    placeholder="待填" :aria-label="item.name+' '+row.name+'巡检数据'"
                    :title="readingFor(item.id,row)!==referenceText(row.reference) ? '原值：'+shownValue(row.reference) : ''"
                    @input="setReading(item.id,row.id,$event)">
                </div>
                <small v-if="row.value_type==='状态' && !row.reference && row.source_reference!==null && row.source_reference!==undefined && String(row.source_reference).trim()" class="warning">原始值 {{ row.source_reference }} 待核对</small>
              </div>
              </div>
              <label v-if="item.parameters.some(row=>row.value_type==='状态' && readingFor(item.id,row)==='×')" class="inspection-note">异常备注
                <textarea :value="drafts[item.id]?.note ?? savedReview(item.id).note" :disabled="inputsLocked" maxlength="1000" rows="2" placeholder="请说明异常情况" @input="setNote(item.id,$event)" />
              </label>
              <div v-if="!readOnly && !active.pending && conflicts.has(item.id)" class="inspection-actions warning">
                <span>本台在其他页面有新填写，请选择要保留的版本。</span>
                <NButton size="small" :disabled="inputsLocked" @click="resolveConflict(item.id,false)">使用共享填写</NButton>
                <NButton size="small" type="primary" secondary :disabled="inputsLocked" @click="resolveConflict(item.id,true)">
                  {{drafts[item.id]?.base ? '合并我的改动' : '用本机旧草稿覆盖本台'}}
                </NButton>
              </div>
              <p v-else-if="blockedDevice===item.id && missingByDevice.get(item.id)" class="warning">
                {{ missingByDevice.get(item.id) }}
              </p>
            </article>
          </div>
        </section>
        <p v-else>{{ selectedRoom ? '没有匹配的设备。' : '请选择楼层和房间查看设备。' }}</p>
        <footer class="inspection-actions inspection-footer">
          <p v-if="error" class="warning">{{ error }}</p>
          <template v-if="!readOnly">
            <NButton v-if="active.pending" type="primary" :loading="busy==='finish'" :disabled="!!busy || autoSaving" @click="confirmFinish">重试归档</NButton>
            <template v-else-if="active.professions?.length">
              <NButton v-for="item in active.professions" :key="item" :type="professionConfirmed(item)?'default':'primary'"
                :disabled="!!busy || autoSaving || professionConfirmed(item)" @click="confirmProfession(item)">
                {{ item }}{{ professionConfirmed(item) ? '已确认' : '确认' }}
              </NButton>
              <NButton v-if="active.professions.every(professionConfirmed)" type="primary" :disabled="!!busy || autoSaving" @click="confirmFinish">继续归档</NButton>
            </template>
            <NButton v-else type="primary" :loading="busy==='finish'" :disabled="!!busy || autoSaving" @click="confirmFinish">确认本轮并归档</NButton>
          </template>
        </footer>
      </template>
      <section v-if="state.history.length && !active" class="inspection-summary">
        <h2>最近归档</h2>
        <a v-for="row in state.history" :key="row.id" class="inspection-history" :href="row.url" target="_blank" rel="noopener noreferrer">
          <span>{{ dutyLabel(row.duty) }}第 {{ row.round }} 次 · {{ row.actor }} · {{ time(row.completed) }}</span><ChevronRight :size="17" />
        </a>
      </section>
    </template>
    <NModal v-model:show="orderOpen" preset="card" :title="'排序 · '+(orderTargetView==='ALL'?'全部专业':orderTargetView)" class="inspection-order">
      <p>全部专业与各专业的顺序按个人分别保存，可导出并分享给同楼人员。</p>
      <div class="inspection-actions order-toolbar">
        <label>移动到第 <input v-model.number="position" type="number" inputmode="numeric" min="1" :max="lastPosition" :disabled="!!busy" aria-label="设备目标位置"> 位</label>
        <NButton size="small" :disabled="!!busy || !selected.length || !validPosition" @click="moveOrder">移动</NButton>
        <span>{{ selected.length }} 台已选</span>
      </div>
      <div class="inspection-actions order-toolbar">
        <input ref="orderFile" type="file" accept=".json,application/json" hidden @change="importOrder">
        <NButton size="small" :disabled="!!busy" @click="orderFile?.click()">导入顺序</NButton>
        <NButton size="small" :disabled="!order.length || !!busy" @click="exportOrder">导出顺序</NButton>
      </div>
      <p v-if="orderFeedback" :class="{warning:orderImportError}" role="status">{{ orderFeedback }}</p>
      <div class="order-list">
        <label v-for="(item,index) in orderedDevices" :key="item.id">
          <input v-model="selected" type="checkbox" :value="item.id" :disabled="!!busy"><span>{{ index+1 }}. {{ item.name }} <small>{{ item.profession }}</small></span>
        </label>
      </div>
      <template #footer>
        <div class="inspection-actions">
          <span v-if="busy==='order'" role="status">正在保存顺序</span>
          <span v-else-if="error" class="warning" role="alert">{{ error }}</span>
          <NButton :disabled="!!busy" @click="orderOpen=false">取消</NButton>
          <NButton type="primary" :loading="busy==='order'" :disabled="!!busy" @click="saveOrder">保存顺序</NButton>
        </div>
      </template>
    </NModal>
    <NModal :show="!!confirmation" preset="dialog" type="warning"
      :title="confirmationTitle" :aria-label="confirmationTitle"
      :content="confirmation==='profession' ? '确认该专业的设备数据已核对；全部专业确认后还需统一提交归档。' : '将把多维表原值及本轮修改归档到飞书「巡检记录」；源数据缺失项保留为空。'"
      :positive-text="confirmation==='profession' ? '确认该专业' : '确认并归档'" negative-text="返回查看"
      @positive-click="applyConfirmation" @update:show="show=>{if(!show)confirmation='';}" />
    <NModal :show="!!leaveAction" preset="dialog" type="warning" title="本机填写尚未保存" aria-label="本机填写尚未保存"
      content="浏览器暂存不可用，离开后本机尚未共享的填写会丢失。"
      positive-text="仍要离开" negative-text="继续填写"
      @positive-click="leaveWithoutDraft" @update:show="show=>{if(!show)leaveAction=undefined;}" />
    <NModal :show="!!recoveryRemoval" preset="dialog" type="warning" title="清除本机留存？" aria-label="清除本机留存？"
      content="请先确认历史巡检中的对应内容已核对。清除后无法从本机恢复这些修改。"
      positive-text="确认清除" negative-text="保留" @positive-click="clearRecovery"
      @update:show="show=>{if(!show)recoveryRemoval=undefined;}" />
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NModal } from 'naive-ui';
import { ArrowLeft, ChevronRight } from 'lucide-vue-next';
import { ApiError, requestJson } from '../api/client';
import { registerNavigationGuard } from '../navigation';
import DailyInspectionHistory from './DailyInspectionHistory.vue';
import DailyInspectionSheet from './DailyInspectionSheet.vue';

defineProps<{embedded?:boolean}>();
const emit = defineEmits<{close:[]}>();

type Value = string | number | boolean | null;
type Reading = {location:string; form:string; integrity:string; missing:string; note:string};
type Point = {id:string; point_id:string; name:string; sampled:number|null; integrity:string; location:string; path:string; note:string; template:string; sheet:string; cell:string; template_known:boolean;mapping_issue?:string};
type Parameter = {id:string; name:string; reference:Value; source_reference?:Value; value_type?:string; section?:number; point?:string; read_only?:boolean};
type Device = {id:string; name:string; scope?:string; profession:string; sections:Reading[]; points:Point[]; parameters:Parameter[]};
type Batch = {batch:number; count:number; devices:Device[];form_devices?:Device[];cache_stale?:boolean;cache_refreshed_at?:number};
type Duty = {scope:string; date:string; shift:string};
type Review = {values:Record<string,string>; result:''|'正常'|'异常'; note:string; revision:number};
type Reviews = Record<string,Review>;
type DraftReview = Review & {base?:Review;pending?:Review;label?:string;parameterLabels?:Record<string,string>};
type Recovery = {key:string;duty:Duty;round:number;drafts:Record<string,DraftReview>};
type ProfessionConfirmation = {actor:string;confirmed:number;version:string};
type State = {duty:Duty|null; completed_rounds:number; round_limit:number; history_scopes?:string[]; duty_warning?:string;
  view_scopes?:string[]; view_scope?:string; can_edit?:boolean;
  active:null|{id:string; duty:Duty; round:number; pending:boolean; draft_key:string; reviews:Reviews; review_version:number; batch:Batch;
    professions?:string[]|null;profession_confirmations?:Record<string,ProfessionConfirmation>;profession_versions?:Record<string,string>;
    orders?:Record<string,string[]>};
  history:{id:string; duty:Duty; round:number; actor:string; completed:number; url:string;draft_key?:string}[]};

const api = '/api/daily-inspection';
const state = ref<State>();
const busy = ref('');
const error = ref('');
const message = ref('');
const search = ref('');
const profession = ref('');
const selectedFloor = ref('');
const selectedRoom = ref('');
const selectedProfessions = ref<string[]>([]);
const onlyMissing = ref(false);
const focusedDevice = ref('');
const historyOpen = ref(false);
const sheetOpen = ref(false);
const historyVisited = ref(false);
const historyPage = ref<InstanceType<typeof DailyInspectionHistory>>();
const pollError = ref('');
watch(historyOpen,open=>{if(open)historyVisited.value=true;});
const orderOpen = ref(false);
const orderTargetView = ref('ALL');
const order = ref<string[]>([]);
const orderFile = ref<HTMLInputElement>();
const orderFeedback = ref('');
const orderImportError = ref(false);
const orderFormat = 'clipflow-daily-inspection-order-v2';
const selected = ref<string[]>([]);
const position = ref<number|string>(1);
const confirmation = ref<''|'finish'|'profession'>('');
const blockedDevice = ref('');
const drafts = ref<Record<string,DraftReview>>({});
const recoveries = ref<Recovery[]>([]);
const recoveryRemoval = ref<Recovery>();
const autoStatus = ref<'idle'|'waiting'|'saving'|'saved'|'error'>('idle');
const autoError = ref('');
const storageError = ref(false);
const leaveAction = shallowRef<() => void>();
const preview = ref<(Batch & {scope:string;orders?:Record<string,string[]>})>();
const previewBusy = ref(false);
const previewError = ref('');
const emptyReview:Review = {values:{},result:'',note:'',revision:0};

const active = computed(()=>state.value?.active);
const readOnly = computed(()=>state.value?.can_edit===false);
const duty = computed(()=>state.value?.duty);
const batch = computed(()=>active.value?.batch);
const devices = computed(()=>batch.value?.devices || []);
const byId = computed(()=>new Map([...devices.value,...(batch.value?.form_devices || [])].map(device=>[device.id,device])));
const inputsLocked = computed(()=>!!busy.value || !!active.value?.pending || readOnly.value);
const hasDrafts = computed(()=>Object.keys(drafts.value).length>0);
const orphanedDrafts = computed(()=>Object.keys(drafts.value).filter(id=>!byId.value.has(id)));
const autoSaving = computed(()=>autoStatus.value==='saving');
const autoLabel = computed(()=>autoStatus.value==='saving' ? '正在自动保存…'
  : conflicts.value.size ? '多人修改待核对，本机内容已保留'
  : autoStatus.value==='error' ? autoError.value || '自动保存未完成，本机内容已保留'
  : hasDrafts.value && storageError.value ? '本机暂存不可用，请保持页面打开'
  : hasDrafts.value ? '本机已暂存，等待自动保存' : autoStatus.value==='saved' ? '已自动保存' : '修改后自动保存');
const conflicts = computed(()=>new Set(active.value?.pending || readOnly.value ? [] : Object.entries(drafts.value)
  .filter(([id,review])=>review.revision!==savedReview(id).revision).map(([id])=>id)));
const missingByDevice = computed(()=>new Map(devices.value.map(item=>{
  const missing=item.parameters.filter(row=>!row.read_only && !readingFor(item.id,row).trim()).length;
  const issue=missing ? missing+' 项数据缺失' : !item.parameters.length ? '源表暂无参数' : '';
  return [item.id,issue] as const;
})));
const missingDevices = computed(()=>devices.value.filter(item=>!!missingByDevice.value.get(item.id)).length);
const professionName = (device:Device)=>device.profession?.trim() || '未分类';
const professions = computed(()=>[...new Set(devices.value.map(professionName))]);
const availableProfessions = computed(()=>{
  const counts=new Map<string,number>();
  for (const device of preview.value?.devices || []) {const name=professionName(device);counts.set(name,(counts.get(name) || 0)+1);}
  return [...counts].map(([name,count])=>({name,count}));
});
const previewDevices = computed(()=>{
  const choosing=!readOnly.value && (state.value?.completed_rounds ?? 0)<(state.value?.round_limit ?? 2);
  const view=choosing && selectedProfessions.value.length===1 ? selectedProfessions.value[0] : 'ALL';
  const ranks=new Map((preview.value?.orders?.[view] || []).map((id,index)=>[id,index]));
  return (preview.value?.devices || []).map((device,index)=>({device,index}))
    .filter(item=>!choosing || selectedProfessions.value.includes(professionName(item.device)))
    .sort((a,b)=>(ranks.get(a.device.id) ?? Number.MAX_SAFE_INTEGER)-(ranks.get(b.device.id) ?? Number.MAX_SAFE_INTEGER) || a.index-b.index)
    .map(item=>item.device);
});
const professionConfirmed = (name:string)=>Boolean(active.value?.profession_confirmations?.[name]?.version) &&
  active.value?.profession_confirmations?.[name]?.version===active.value?.profession_versions?.[name] &&
  !Object.keys(drafts.value).some(id=>{const device=byId.value.get(id);return device && professionName(device)===name;});
const viewDevices = computed(()=>{
  const view=profession.value || 'ALL';
  const ranks=new Map((active.value?.orders?.[view] || []).map((id,index)=>[id,index]));
  return devices.value.map((device,index)=>({device,index}))
    .filter(item=>!profession.value || professionName(item.device)===profession.value)
    .sort((a,b)=>(ranks.get(a.device.id) ?? Number.MAX_SAFE_INTEGER)-(ranks.get(b.device.id) ?? Number.MAX_SAFE_INTEGER) || a.index-b.index)
    .map(item=>item.device);
});
// Keep unrecognised locations visible for manual verification instead of guessing.
function devicePlace(device:Device) {
  const location=device.sections.map(row=>row.location).find(Boolean) || device.points.map(row=>row.location).find(Boolean) || '';
  const path=device.points.map(row=>row.path).find(Boolean) || '';
  const parts=path.split('/').map(part=>part.trim()).filter(Boolean);
  const floorIndex=parts.findIndex(part=>/^[一二三四五六七八九\d]+层$/.test(part));
  const code=(location+' '+device.name+' '+path).match(/(?:^|[^\d])(?:[DE][-\s]?)?([1-9]\d{2})(?=[-\s/]|$)/i)?.[1] || '';
  const explicit=(floorIndex>=0 ? parts[floorIndex] : location).match(/(?:^|[/\s])([一二三四五六七八九\d]+)层/);
  const context=[location,path,device.name].join(' ');
  const isCold=/(?:冷冻站|冷站|冷机|冷冻泵|冷却泵|板换|冷塔)/.test(context);
  const isAuxPump=/辅助水泵/.test(context);
  const diesel=String(device.scope || duty.value?.scope || preview.value?.scope || '')==='E' ? device.name.match(/(?:^|[^\d])(1[0-4]|[1-9])\s*(?:#|号)\s*(?:柴发|柴油发电)/) : null;
  const dieselRoom=diesel ? (Number(diesel[1])<=7?'178':'279') : '';
  const eScope=String(device.scope || duty.value?.scope || preview.value?.scope || '')==='E';
  const substation=eScope ? (device.name+' '+path).match(/M([1-6])变电所环境/i)?.[1] : '';
  const substationRoom=substation ? ({'1':'245','2':'218','3':'345','4':'317','5':'445','6':'417'} as Record<string,string>)[substation] : '';
  const number=explicit ? ({'一':'1','二':'2','三':'3','四':'4'} as Record<string,string>)[explicit[1]] || explicit[1] : dieselRoom?.[0] || substationRoom?.[0] || code?.[0] || '';
  const floor=isCold || isAuxPump ? '1层' : ['1','2','3','4'].includes(number) ? number+'层' : '待核对';
  const room=isCold ? '冷冻站' : isAuxPump ? '辅助水泵系统' : dieselRoom || substationRoom || (floorIndex>=0 ? parts[floorIndex+1] : '') || (code ? code : location.trim() || '未标注房间');
  const unitNumber=isCold ? context.match(/(?:^|[^\d])([1-9]\d*)\s*(?:#|号)\s*(?:冷机|冷冻泵|冷却泵|板换|冷塔)/)?.[1] || context.match(/(?:冷机|冷冻泵|冷却泵|板换|冷塔)[^\d]{0,3}([1-9]\d*)\s*(?:#|号)?/)?.[1] || device.name.match(/(?:^|[-_])CH([1-6])(?:$|[^\d])/i)?.[1] : '';
  return {floor,room,unit:unitNumber ? unitNumber+'号制冷单元' : ''};
}
const navigationDevices=computed(()=>active.value ? viewDevices.value : previewDevices.value);
const floors=computed(()=>['1层','2层','3层','4层'].filter(floor=>navigationDevices.value.some(item=>devicePlace(item).floor===floor)));
const unplacedCount=computed(()=>navigationDevices.value.filter(item=>devicePlace(item).floor==='待核对').length);
const rooms=computed(()=>[...new Set(navigationDevices.value.filter(item=>devicePlace(item).floor===selectedFloor.value).map(item=>devicePlace(item).room))].sort((a,b)=>a.localeCompare(b,'zh-CN',{numeric:true})));
function selectFloor(floor:string) {selectedFloor.value=floor;selectedRoom.value='';}
const visiblePreview=computed(()=>previewDevices.value.filter(item=>!!selectedRoom.value && devicePlace(item).floor===selectedFloor.value && devicePlace(item).room===selectedRoom.value));
watch([()=>state.value?.view_scope,()=>active.value?.id],()=>{selectedFloor.value='';selectedRoom.value='';});
watch(floors,items=>{if (selectedFloor.value!=='待核对'&&!items.includes(selectedFloor.value)) {selectedFloor.value='';selectedRoom.value='';}});
watch(rooms,items=>{if (!items.includes(selectedRoom.value)) selectedRoom.value='';});
const positions = computed(()=>new Map(viewDevices.value.map((device,index)=>[device.id,index+1])));
const filtered = computed(()=>{
  const q=search.value.trim().toLowerCase();
  return viewDevices.value.filter(item=>!!selectedRoom.value && devicePlace(item).floor===selectedFloor.value && devicePlace(item).room===selectedRoom.value && (!onlyMissing.value || focusedDevice.value===item.id || !!missingByDevice.value.get(item.id) || conflicts.value.has(item.id)) &&
    (!q || [item.name,item.profession,...item.sections.map(row=>row.location),...item.points.flatMap(row=>[row.name,row.path])]
      .some(value=>value.toLowerCase().includes(q))));
});
const orderedDevices = computed(()=>order.value.map(id=>byId.value.get(id)).filter((item):item is Device=>!!item));
const lastPosition = computed(()=>order.value.length-selected.value.length+1);
const validPosition = computed(()=>Number.isInteger(position.value) && Number(position.value)>=1 && Number(position.value)<=lastPosition.value);
const formatter = new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',dateStyle:'short',timeStyle:'medium',hour12:false});
const time = (value?:number|null)=>value ? formatter.format(value) : '—';
const dutyLabel = (value:Duty)=>value.scope+'楼 · '+value.date+' '+value.shift+'班';
const shownValue = (value:Value)=>value==null || value==='' ? '—' : typeof value==='boolean' ? (value?'是':'否') : value;
const deviceAnchor = (id:string)=>'inspection-'+encodeURIComponent(id);
const storageKey = (key:string)=>'clipflow:daily-inspection:'+key;
const savedReview = (id:string)=>active.value?.reviews?.[id] || emptyReview;
const reviewFor = (id:string)=>(!readOnly.value && !active.value?.pending && drafts.value[id]) || savedReview(id);
const referenceText = (value:Value)=>value==null ? '' : typeof value==='boolean' ? (value?'是':'否') : String(value);
const readingFor = (id:string,row:Parameter)=>Object.prototype.hasOwnProperty.call(reviewFor(id).values,row.id)
  ? reviewFor(id).values[row.id] : referenceText(row.reference);
const deviceModified = (device:Device)=>device.parameters.some(row=>readingFor(device.id,row)!==referenceText(row.reference));

let controller:AbortController|undefined;
let statusController:AbortController|undefined;
let previewController:AbortController|undefined;
let previewTimer:number|undefined;
let previewScope='';
let previewChoiceScope='';
let autoController:AbortController|undefined;
let statusTimer:number|undefined;
let confirmationTarget:{id:string;version:number;profession?:string;professionVersion?:string}|undefined;
const confirmationTitle=computed(()=>confirmation.value==='profession' ? '确认'+(confirmationTarget?.profession || '')+'专业？'
  : '确认整轮巡检？');
let disposed=false;
let saveTimer:number|undefined;
let autoTimer:number|undefined;
let draftKey='';
let orderImportRevision=0;
watch(confirmation,(value,previous)=>{if (!value && previous) queueAutoSave();});

function numericValue(value:Value|undefined) {
  if (typeof value==='number') return Number.isFinite(value) ? value : null;
  return typeof value==='string' && /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value.trim()) && Number.isFinite(Number(value)) ? Number(value) : null;
}
function leaveDevice(id:string,event:FocusEvent) {
  if (focusedDevice.value===id && !(event.currentTarget as HTMLElement).contains(event.relatedTarget as Node | null)) {
    focusedDevice.value='';
  }
}
function parameterLocation(device:Device,row:Parameter) {
  if (row.section!==undefined) return device.sections.length>1 ? device.sections[row.section]?.form || device.sections[row.section]?.location : '';
  return '测点';
}
function mappingIssue(device:Device,row:Parameter) {
  return row.point ? device.points.find(point=>point.id===row.point)?.mapping_issue || '' : '';
}
function persistDrafts() {
  window.clearTimeout(saveTimer);
  if (!draftKey) return;
  try {
    if (hasDrafts.value) localStorage.setItem(storageKey(draftKey),JSON.stringify(drafts.value));
    else localStorage.removeItem(storageKey(draftKey));
    storageError.value=false;
  } catch {storageError.value=true;}
}
function validReview(item?:Review) {
  return item && Number.isInteger(item.revision) && ['', '正常', '异常'].includes(item.result) &&
    typeof item.note==='string' && item.values && !Array.isArray(item.values) && typeof item.values==='object' &&
    Object.values(item.values).every(value=>typeof value==='string');
}
function restoreDrafts(key:string) {
  if (!key) return {};
  try {
    const saved=JSON.parse(localStorage.getItem(storageKey(key)) || '{}') as Record<string,DraftReview>;
    return Object.fromEntries(Object.entries(saved).filter(([,item])=>validReview(item))
      .map(([id,item])=>[id,{...item,base:validReview(item.base) ? item.base : undefined,
        pending:validReview(item.pending) ? item.pending : undefined}]));
  } catch {return {};}
}
function sameReview(left:Review,right:Review) {
  const values=Object.entries(left.values);
  return left.result===right.result && left.note.trim()===right.note &&
    values.length===Object.keys(right.values).length && values.every(([key,value])=>right.values[key]===value.trim());
}
function edit(id:string) {
  if (!drafts.value[id]) {
    const base=savedReview(id);
    const device=byId.value.get(id);
    drafts.value[id]={...base,values:{...base.values},base,label:device?.name,
      parameterLabels:Object.fromEntries((device?.parameters || []).map(row=>[row.id,row.name]))};
  }
  window.clearTimeout(saveTimer);
  saveTimer=window.setTimeout(persistDrafts,250);
  return drafts.value[id];
}
function setReading(id:string,parameter:string,event:Event) {
  setReadingValue(id,parameter,(event.target as HTMLInputElement).value);
}
function setReadingValue(id:string,parameter:string,value:string) {
  edit(id).values[parameter]=value;
  queueAutoSave();
}
function setNote(id:string,event:Event) {
  edit(id).note=(event.target as HTMLTextAreaElement).value;
  queueAutoSave();
}
function resolveConflict(id:string,keep:boolean) {
  const draft=drafts.value[id];
  if (!draft) return;
  if (!keep) {
    delete drafts.value[id];
  } else {
    const latest=savedReview(id), base=draft.base;
    if (!base) {
      draft.revision=latest.revision;
      draft.base=latest;
    } else {
      const values={...latest.values};
      for (const [key,value] of Object.entries(draft.values)) {
        if (!Object.prototype.hasOwnProperty.call(base.values,key) || value.trim()!==base.values[key].trim()) values[key]=value;
      }
      drafts.value[id]={
        ...latest, values, base:latest,
        label:draft.label,parameterLabels:draft.parameterLabels,
        result:draft.result===base.result ? latest.result : draft.result,
        note:draft.note.trim()===base.note.trim() ? latest.note : draft.note,
      };
    }
  }
  persistDrafts();
  if (!conflicts.value.size) {autoStatus.value='waiting';autoError.value='';}
  queueAutoSave();
}
function submissionReviews():Reviews {
  return Object.fromEntries(Object.entries(drafts.value).map(([id,{values,result,note,revision}])=>
    [id,{values,result,note,revision}]));
}
function refreshRecoveries(next:State) {
  recoveries.value=next.history.flatMap(row=>{
    if (!row.draft_key || row.id===next.active?.id) return [];
    const saved=restoreDrafts(row.draft_key);
    return Object.keys(saved).length ? [{key:row.draft_key,duty:row.duty,round:row.round,drafts:saved}] : [];
  });
}
function clearRecovery() {
  const target=recoveryRemoval.value;
  recoveryRemoval.value=undefined;
  if (!target) return;
  try {
    localStorage.removeItem(storageKey(target.key));
    recoveries.value=recoveries.value.filter(item=>item.key!==target.key);
  } catch {error.value='无法清除本机留存，请检查浏览器存储设置。';}
}
function queueAutoSave(delay=1400) {
  window.clearTimeout(autoTimer);
  if (disposed || !active.value || active.value.pending || readOnly.value || !hasDrafts.value || confirmation.value) return;
  if (orphanedDrafts.value.length) {
    autoStatus.value='error';
    autoError.value='设备清单已变化，本机修改仍保留，请刷新后核对。';
    return;
  }
  if (conflicts.value.size) {
    autoStatus.value='error';
    autoError.value='共享数据已有修改，请先在对应设备处核对。';
    return;
  }
  if (autoController || busy.value) return;
  if (autoStatus.value!=='error') autoStatus.value='waiting';
  autoTimer=window.setTimeout(()=>void saveAutomatically(),delay);
}
function retryAutoSave() {
  autoStatus.value='waiting';
  autoError.value='';
  queueAutoSave(0);
}
async function saveAutomatically() {
  const current=active.value;
  if (disposed || !current || current.pending || readOnly.value || !hasDrafts.value || conflicts.value.size || orphanedDrafts.value.length || autoController || busy.value) return;
  const request=autoController=new AbortController();
  const scope=state.value?.view_scope;
  const suffix=scope ? '?'+new URLSearchParams({view_scope:scope}) : '';
  autoStatus.value='saving';
  autoError.value='';
  let retryable=true;
  try {
    if (Object.values(drafts.value).some(review=>review.pending)) {
      // Resolve a previously unconfirmed write before sending newer edits.
      const latest=await requestJson(api+suffix,{cache:'no-store',fresh:true,signal:request.signal,timeoutMs:30000}) as State;
      if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
      useState(latest);
      if (active.value?.id!==current.id || !hasDrafts.value) {autoStatus.value='saved';return;}
      if (conflicts.value.size) {
        autoStatus.value='error';
        autoError.value='共享数据已有修改，请先在对应设备处核对。';
        retryable=false;
        return;
      }
    }
    const submitted=JSON.parse(JSON.stringify(submissionReviews())) as Reviews;
    for (const [id,review] of Object.entries(submitted)) {
      if (drafts.value[id]) drafts.value[id].pending=review;
    }
    // Keep the exact in-flight revision so a restart can reconcile a lost response.
    persistDrafts();
    const next=await requestJson(api+'/draft'+suffix,{method:'POST',body:JSON.stringify({id:current.id,reviews:submitted}),
      signal:request.signal,timeoutMs:30000}) as State;
    if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
    if (next.active?.id!==current.id) throw new Error('本轮巡检已变化，请刷新核对。');
    useState(next);
    autoStatus.value=hasDrafts.value ? 'waiting' : 'saved';
  } catch (value) {
    if (disposed || request.signal.aborted || active.value?.id!==current.id) return;
    autoStatus.value='error';
    autoError.value=value instanceof Error ? value.message : '自动保存未完成，本机内容已保留。';
    if (value instanceof ApiError && value.status>=400 && value.status<500 && ![408,429].includes(value.status)) retryable=false;
    if (value instanceof ApiError && value.status===409) {
      try {
        const latest=await requestJson(api+suffix,{cache:'no-store',fresh:true,signal:request.signal}) as State;
        if (!disposed && !request.signal.aborted && active.value?.id===current.id) useState(latest);
      } catch { /* The local draft remains available for the next retry. */ }
    }
  } finally {
    if (autoController===request) {
      autoController=undefined;
      if (!disposed && retryable && hasDrafts.value && !conflicts.value.size) queueAutoSave(autoStatus.value==='error'?15000:1400);
    }
  }
}
function useState(next:State,discardPrevious=false) {
  const nextKey=next.active?.draft_key || '';
  if (draftKey!==nextKey) {
    window.clearTimeout(autoTimer);
    if (autoController) {const previous=autoController;autoController=undefined;previous.abort();}
    if (discardPrevious) drafts.value={};
    persistDrafts();
    draftKey=nextKey;
    drafts.value=restoreDrafts(nextKey);
    autoStatus.value='idle';
    autoError.value='';
    previewChoiceScope='';
    selectedProfessions.value=[];
    search.value='';
    profession.value='';
    onlyMissing.value=false;
    focusedDevice.value='';
    orderOpen.value=false;
    confirmation.value='';
    blockedDevice.value='';
  }
  state.value=next;
  if (!next.history_scopes?.length) { historyOpen.value=false; sheetOpen.value=false; }
  for (const [id,review] of Object.entries(drafts.value)) {
    const saved=savedReview(id);
    if (review.pending && saved.revision>=review.pending.revision && sameReview(review.pending,saved)) {
      review.revision=saved.revision;
      review.base=saved;
      delete review.pending;
    }
    if (byId.value.has(id) && sameReview(review,savedReview(id))) delete drafts.value[id];
  }
  persistDrafts();
  refreshRecoveries(next);
  if (hasDrafts.value) queueAutoSave();
}
async function fetchState(action:string, body?:object, selectedScope?:string) {
  if (busy.value) return false;
  statusController?.abort();
  const request=controller=new AbortController();
  const roundId=active.value?.id;
  busy.value=action;
  error.value='';
  message.value='';
  pollError.value='';
  try {
    const scope=selectedScope ?? state.value?.view_scope ?? '';
    const suffix=scope ? '?'+new URLSearchParams({view_scope:scope}) : '';
    const next=await requestJson(api+(body?'/'+action:'')+suffix,{
      ...(body?{method:'POST',body:JSON.stringify(body)}:{cache:'no-store',fresh:true}),
      signal:request.signal,timeoutMs:120000,
    }) as State;
    if (disposed || request.signal.aborted) return false;
    useState(next,action==='finish' || action==='confirm-profession' && !next.active);
    if (action==='finish') message.value='本轮巡检已归档。';
    if (action==='confirm-profession') message.value=next.active?.professions?.every(name=>next.active?.profession_confirmations?.[name]?.version===next.active?.profession_versions?.[name]) ? '所有专业已确认，可统一提交归档。' : '该专业已确认，等待其他已选专业。';
    if (action==='order') message.value='设备顺序已保存。';
    if (action==='draft') message.value='本轮草稿已共享给本楼当班人员。';
    return true;
  } catch (value) {
    if (disposed || request.signal.aborted) return false;
    error.value=value instanceof Error ? value.message : '巡检请求未完成。';
    if (value instanceof ApiError && [401,403].includes(value.status)) useState({duty:null,active:null,history:[],completed_rounds:0,round_limit:2});
    else if (action==='finish' || action==='confirm-profession' || (value instanceof ApiError && value.status===409)) {
      try {
        const viewScope=state.value?.view_scope;
        const latest=await requestJson(api+(viewScope ? '?'+new URLSearchParams({view_scope:viewScope}) : ''),
          {cache:'no-store',fresh:true,signal:request.signal}) as State;
        if (!disposed && !request.signal.aborted) {
          const archived=['finish','confirm-profession'].includes(action) && latest.history.some(row=>row.id===roundId);
          const submitted=body as {profession?:string;profession_version?:string}|undefined;
          const confirmed=action==='confirm-profession' && submitted?.profession && latest.active?.id===roundId &&
            latest.active?.profession_confirmations?.[submitted.profession]?.version===submitted.profession_version;
          useState(latest,archived);
          if (archived) {error.value='';message.value='本轮巡检已归档。';}
          else if (confirmed && latest.active?.pending) {error.value='所有专业已确认，但归档尚未完成，请重试归档。';return false;}
          else if (confirmed) {error.value='';message.value='该专业已确认，等待其他已选专业。';}
          if (archived || confirmed) return true;
        }
      } catch { /* Keep the original error and draft. */ }
    }
    return false;
  } finally {
    if (controller===request) {busy.value='';if (hasDrafts.value) queueAutoSave();}
  }
}
async function refreshSharedStatus() {
  const current=state.value;
  if (disposed || !current || (!current.duty && !current.history_scopes?.length)
      || document.visibilityState==='hidden' || busy.value || statusController || autoController) return;
  const request=statusController=new AbortController();
  const params=new URLSearchParams({date:current.duty?.date || '',shift:current.duty?.shift || ''});
  if (current.view_scope) params.set('view_scope',current.view_scope);
  if (current.active) {
    params.set('id',current.active.id);
    params.set('version',String(current.active.review_version));
    params.set('pending',current.active.pending?'1':'0');
  }
  try {
    const next=await requestJson(api+'/status?'+params,{cache:'no-store',fresh:true,signal:request.signal,timeoutMs:8000});
    if (disposed || request.signal.aborted || state.value!==current) return;
    pollError.value='';
    if (!next.unchanged) {
      useState(next as State);
    } else if (Array.isArray(next.history_scopes) && next.history_scopes.join()!==current.history_scopes?.join()) {
      useState({...current,history_scopes:next.history_scopes});
    }
  } catch (value) {
    if (disposed || request.signal.aborted) return;
    if (value instanceof ApiError && [401,403].includes(value.status)) {
      useState({duty:null,active:null,history:[],completed_rounds:0,round_limit:2});
      error.value=value.message;
    } else pollError.value='共享状态暂未刷新，本机填写已保留。';
  } finally {
    if (statusController===request) statusController=undefined;
  }
}
async function refreshPreview() {
  const scope=state.value?.view_scope || duty.value?.scope;
  if (previewController && previewScope===scope && !active.value) return;
  previewController?.abort();
  if (!scope || active.value) {
    preview.value=undefined;
    previewBusy.value=false;
    previewError.value='';
    return;
  }
  const request=previewController=new AbortController();
  previewScope=scope;
  if (preview.value?.scope!==scope) preview.value=undefined;
  previewBusy.value=true;
  previewError.value='';
  try {
    const data=await requestJson(api+'/source?'+new URLSearchParams({view_scope:scope}),
      {cache:'no-store',fresh:true,signal:request.signal,timeoutMs:120000}) as Batch & {scope:string;orders?:Record<string,string[]>};
    if (!disposed && !request.signal.aborted && (state.value?.view_scope || duty.value?.scope)===scope && !active.value) {
      preview.value=data;
      const available=[...new Set(data.devices.map(professionName))];
      selectedProfessions.value=previewChoiceScope===scope ? selectedProfessions.value.filter(item=>available.includes(item)) : available;
      previewChoiceScope=scope;
      window.clearTimeout(previewTimer);
      if (data.cache_stale) previewTimer=window.setTimeout(()=>void refreshPreview(),30000);
    }
  } catch (value) {
    if (!disposed && !request.signal.aborted) previewError.value=value instanceof Error ? value.message : '最新设备数据暂时无法读取。';
  } finally {
    if (previewController===request) {previewController=undefined;previewScope='';previewBusy.value=false;}
  }
}
watch([()=>state.value?.view_scope,()=>state.value?.duty?.scope,()=>state.value?.can_edit,()=>state.value?.active?.id],()=>void refreshPreview());
const load = async()=>{
  if (await fetchState('load') && !active.value) void refreshPreview();
};
function switchView(scope:string) {
  if (scope!==state.value?.view_scope) void fetchState('load',undefined,scope);
}
const start = ()=>fetchState('start',{duty:duty.value,professions:selectedProfessions.value});
async function confirmProfession(name:string) {
  if (!active.value || autoController || busy.value || professionConfirmed(name)) return;
  window.clearTimeout(autoTimer);
  if (orphanedDrafts.value.length || conflicts.value.size) {error.value='本机修改与共享数据待核对，请先处理后再确认。';return;}
  if (hasDrafts.value) {
    await saveAutomatically();
    if (hasDrafts.value || autoStatus.value==='error' || autoController) {error.value='修改尚未同步，请保存后再确认。';return;}
  }
  const current=active.value;
  if (!current || current.pending || !current.professions?.includes(name)) return;
  const version=current.profession_versions?.[name];
  if (!version) {error.value='专业状态尚未就绪，请刷新后重试。';return;}
  confirmationTarget={id:current.id,version:current.review_version,profession:name,professionVersion:version};
  confirmation.value='profession';
}
function confirmFinish() {
  if (!active.value || autoController) return;
  window.clearTimeout(autoTimer);
  if (orphanedDrafts.value.length) {error.value='设备清单已变化，本机修改仍保留，请刷新后核对。';return;}
  if (active.value.pending) {void fetchState('finish',{id:active.value.id});return;}
  const missing=devices.value.find(item=>conflicts.value.has(item.id));
  if (missing) {
    blockedDevice.value=missing.id;
    error.value=missing.name+' · 请先核对共享修改';
    search.value='';
    profession.value='';
    onlyMissing.value=false;
    const place=devicePlace(missing);
    selectedFloor.value=place.floor;
    selectedRoom.value=place.room;
    void nextTick(()=>{
      const element=document.getElementById(deviceAnchor(missing.id));
      element?.scrollIntoView({block:'start'});
      element?.querySelector('h2')?.focus({preventScroll:true});
    });
    return;
  }
  blockedDevice.value='';
  promptConfirmation('finish');
}
function promptConfirmation(action:'finish') {
  const current=active.value;
  if (!current || autoController) return;
  window.clearTimeout(autoTimer);
  confirmationTarget={id:current.id,version:current.review_version};
  confirmation.value=action;
}
function applyConfirmation() {
  const action=confirmation.value;
  const current=active.value;
  const target=confirmationTarget;
  confirmation.value='';
  confirmationTarget=undefined;
  if (!current || !action || !target) return;
  const changed=action==='profession' ? !target.profession || current.profession_versions?.[target.profession]!==target.professionVersion
    : current.review_version!==target.version;
  if (current.id!==target.id || changed || current.pending) {
    error.value='本轮巡检已更新，请核对最新状态后再确认。';
    return;
  }
  const body={id:target.id,review_version:target.version,
    ...(action==='finish' ? {reviews:submissionReviews()} : action==='profession' ?
      {profession:target.profession,profession_version:target.professionVersion,reviews:{}} : {})};
  void fetchState(action==='profession' ? 'confirm-profession' : action,body);
}
function openOrder() {
  orderImportRevision++;
  orderTargetView.value=profession.value || 'ALL';
  order.value=viewDevices.value.map(item=>item.id);
  selected.value=[];
  position.value=1;
  orderFeedback.value='';
  orderImportError.value=false;
  error.value='';
  orderOpen.value=true;
}
function moveOrder() {
  if (!validPosition.value || !selected.value.length) return;
  const chosen=new Set(selected.value);
  const moving=order.value.filter(id=>chosen.has(id));
  const rest=order.value.filter(id=>!chosen.has(id));
  rest.splice(Number(position.value)-1,0,...moving);
  order.value=rest;
}
function exportOrder() {
  const scope=active.value?.duty.scope;
  if (!scope || !order.value.length) return;
  const blob=new Blob([JSON.stringify({format:orderFormat,scope,view:orderTargetView.value,order:order.value},null,2)],{type:'application/json'});
  const link=document.createElement('a');
  link.href=URL.createObjectURL(blob);
  const name=(orderTargetView.value==='ALL'?'全部专业':orderTargetView.value).replace(/[\\/:*?"<>|]/g,'_').slice(0,40);
  link.download=scope+'楼日常巡检-'+name+'排序.json';
  link.click();
  window.setTimeout(()=>URL.revokeObjectURL(link.href),1000);
}
async function importOrder(event:Event) {
  const input=event.target as HTMLInputElement;
  const file=input.files?.[0];
  input.value='';
  const roundId=active.value?.id;
  const scope=active.value?.duty.scope;
  const selectedView=orderTargetView.value;
  if (!file || !roundId || !scope || busy.value) return;
  const revision=++orderImportRevision;
  const current=()=>!disposed && orderOpen.value && active.value?.id===roundId && orderTargetView.value===selectedView && revision===orderImportRevision && !busy.value;
  orderFeedback.value='';
  orderImportError.value=false;
  try {
    if (file.size>1024*1024) throw new Error('排序文件超过 1 MB。');
    const data=JSON.parse(await file.text()) as {format?:unknown;scope?:unknown;view?:unknown;order?:unknown}|null;
    if (!current()) return;
    if (![orderFormat,'clipflow-daily-inspection-order-v1'].includes(String(data?.format)) || !Array.isArray(data?.order) || !data.order.every(id=>typeof id==='string') ||
        new Set(data.order).size!==data.order.length) throw new Error('请选择有效的日常巡检排序文件。');
    if (data.scope!==scope) throw new Error('排序文件不属于本楼，请选择 '+scope+' 楼的文件。');
    const fileView=data.format==='clipflow-daily-inspection-order-v1'?'ALL':data.view;
    if (fileView!==selectedView) throw new Error('排序文件属于'+(fileView==='ALL'?'全部专业':String(fileView || '未知专业'))+'，请切换到对应专业后导入。');
    const currentIds=new Set(order.value);
    const matching=data.order.filter(id=>currentIds.has(id));
    if (!matching.length) throw new Error('文件中的设备与本楼当前设备没有匹配项。');
    const imported=new Set(matching);
    const remaining=order.value.filter(id=>!imported.has(id));
    order.value=[...matching,...remaining];
    selected.value=[];
    position.value=1;
    orderFeedback.value='已预览 '+matching.length+' 台；'+remaining.length+' 台当前设备保留在末尾。点击保存顺序后生效。';
  } catch (value) {
    if (!current()) return;
    orderImportError.value=true;
    orderFeedback.value=value instanceof SyntaxError ? '排序文件无法读取，请使用系统导出的 JSON 文件。'
      : value instanceof Error ? value.message : '导入顺序未完成。';
  }
}
async function saveOrder() {
  if (active.value && await fetchState('order',{id:active.value.id,view:orderTargetView.value,order:order.value})) orderOpen.value=false;
}
function toggleHistory() {
  if (historyOpen.value) {
    if (historyPage.value) historyPage.value.requestClose();
    else historyOpen.value=false;
  }
  else historyOpen.value=true;
}
function back() {
  if (sheetOpen.value) sheetOpen.value=false;
  else if (historyOpen.value) {
    if (historyPage.value) historyPage.value.requestBack();
    else historyOpen.value=false;
  }
  else emit('close');
}
function leaveWithoutDraft() {
  const proceed=leaveAction.value;
  leaveAction.value=undefined;
  proceed?.();
}
const unregister=registerNavigationGuard((_target,proceed)=>{
  if (busy.value && busy.value!=='load') {
    error.value='巡检操作正在处理，请稍后再离开。';
    return false;
  }
  if (historyOpen.value && historyPage.value && !historyPage.value.allowNavigation(proceed)) return false;
  if (hasDrafts.value) {
    persistDrafts();
    if (storageError.value) {
      leaveAction.value=proceed;
      return false;
    }
  }
  return true;
});
function beforeUnload(event:BeforeUnloadEvent) {
  if (hasDrafts.value) persistDrafts();
  if ((busy.value && busy.value!=='load') || (storageError.value && hasDrafts.value)
      || historyPage.value?.hasUnsavedChanges()) event.preventDefault();
}
onMounted(()=>{
  window.addEventListener('pagehide',persistDrafts);
  window.addEventListener('beforeunload',beforeUnload);
  document.addEventListener('visibilitychange',refreshSharedStatus);
  statusTimer=window.setInterval(refreshSharedStatus,8000);
  void load();
});
onBeforeUnmount(()=>{
  disposed=true;
  unregister();
  controller?.abort();
  statusController?.abort();
  previewController?.abort();
  window.clearTimeout(previewTimer);
  autoController?.abort();
  window.clearTimeout(autoTimer);
  window.clearInterval(statusTimer);
  document.removeEventListener('visibilitychange',refreshSharedStatus);
  persistDrafts();
  window.removeEventListener('pagehide',persistDrafts);
  window.removeEventListener('beforeunload',beforeUnload);
});
</script>

<style scoped>
.inspection-page,.inspection-order{
  --inspection-ink:var(--cf-text);--inspection-muted:var(--cf-muted);--inspection-line:var(--cf-border);
  --inspection-surface:var(--cf-surface);--inspection-accent:var(--cf-brand-blue);
  --inspection-soft:color-mix(in srgb,var(--inspection-surface) 93%,var(--inspection-accent));
}
.inspection-page{display:grid;gap:10px;max-width:1500px;margin:auto;padding:10px 12px;min-width:0;background:var(--inspection-surface);color:var(--inspection-ink)}
.inspection-recovery{padding:7px;border:1px solid var(--cf-warning);color:var(--inspection-ink);font-size:13px}
.inspection-recovery summary{cursor:pointer;color:var(--cf-warning);font-weight:600}
.inspection-recovery-item{padding:6px 0;border-top:1px solid var(--inspection-line);overflow-wrap:anywhere}
.inspection-heading,.inspection-actions{display:flex;align-items:center;flex-wrap:wrap;gap:6px 10px}
.inspection-heading{padding-bottom:8px;border-bottom:1px solid var(--inspection-line)}
.inspection-title{min-width:136px;flex:1}
.inspection-heading-tools{margin-inline-start:auto}
h1,h2,p{margin:0;overflow-wrap:anywhere}
h1{font-size:20px}h2{font-size:16px}p,small{color:var(--inspection-muted);font-size:12px}
.inspection-summary{display:grid;gap:6px}
.profession-choices label{display:flex;align-items:center;gap:4px;padding:4px 7px;border:1px solid var(--inspection-line);cursor:pointer;font-size:13px}
.profession-progress{gap:4px 12px;font-size:12px}
.inspection-page :is(input:not([type=checkbox]),select,textarea),.order-toolbar input{
  box-sizing:border-box;min-width:0;min-height:36px;padding:4px 8px;border:1px solid var(--inspection-line);
  border-radius:4px;color:var(--inspection-ink);background:var(--inspection-surface);font:inherit
}
.inspection-page select{appearance:auto}
.inspection-page :is(input,select,textarea):disabled,.order-toolbar input:disabled{opacity:.62}
.inspection-page :is(input,select,textarea,a):focus-visible,.order-toolbar input:focus-visible,.order-list input:focus-visible{
  outline:2px solid var(--inspection-accent);outline-offset:2px
}
.inspection-page input::placeholder,.inspection-page textarea::placeholder{color:var(--inspection-muted)}
.inspection-page input[type=checkbox],.order-list input{width:17px;height:17px;margin:0;accent-color:var(--inspection-accent)}
.inspection-page :deep(.n-button),.inspection-order :deep(.n-button){min-height:34px;max-width:100%;font-weight:500}
.inspection-page :deep(.n-button__content){white-space:normal}
.inspection-filters{display:grid;grid-template-columns:minmax(0,1fr) minmax(90px,140px) auto;gap:6px;align-items:center}
.room-navigation{display:grid;gap:6px;margin:8px 0}.room-navigation button{border:1px solid var(--border-color,#ccd5df);background:transparent;color:inherit;padding:5px 10px;cursor:pointer}.room-navigation button[aria-pressed=true]{background:var(--primary-color,#1764c0);color:#fff;border-color:transparent}
.room-navigation .unplaced-button{justify-self:start;color:var(--cf-warning,#a65b00)}
.inspection-note{display:grid;gap:4px;margin-top:8px}.inspection-note textarea{width:100%;box-sizing:border-box;color:inherit;background:transparent;border:1px solid var(--border-color,#ccd5df);font:inherit;padding:6px}
.inspection-filters label{display:flex;align-items:center;gap:6px;min-height:34px;font-size:13px;cursor:pointer}
.inspection-devices{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-items:start;gap:10px 16px}
.inspection-device{min-width:0;border-top:1px solid var(--inspection-line);padding:3px 0;scroll-margin-top:30px}
.inspection-readings{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,150px),1fr));gap:0 8px;align-items:start}
.inspection-preview-row{display:flex;justify-content:space-between;gap:8px;padding:5px 3px;border-bottom:1px solid var(--inspection-line);font-size:13px;min-width:0;overflow-wrap:anywhere}
.inspection-preview-row strong{text-align:right;font-variant-numeric:tabular-nums}
.inspection-device-heading{display:flex;align-items:baseline;gap:5px;font-size:12px}
.inspection-device-heading h2{min-width:0;flex:1}.device-profession{color:var(--inspection-muted);font-size:11px}
.inspection-device-heading>span{flex:none}
.sequence{margin-right:7px;color:var(--inspection-muted);font-size:12px}
.numeric{text-align:right;font-variant-numeric:tabular-nums}
.inspection-reading{display:grid;grid-template-columns:minmax(0,1fr) minmax(58px,42%);align-items:center;gap:4px;min-width:0;min-height:36px;padding:1px 0;border-bottom:1px solid var(--inspection-line);font-size:14px}
.inspection-reading>*{min-width:0;overflow-wrap:anywhere}
.inspection-reading strong{font-size:13px}
.inspection-reading small{display:block}
.inspection-reading input{width:100%;min-height:32px}
.inspection-value{text-align:right;min-width:0}.inspection-value small{font-size:11px;overflow-wrap:anywhere}
.inspection-page .inspection-reading input.missing-reading{
  border-color:color-mix(in srgb,var(--cf-warning) 52%,var(--inspection-line));
  background:color-mix(in srgb,var(--inspection-surface) 94%,var(--cf-warning))
}
.auto-status{font-size:12px;gap:4px}.inspection-more{font-size:13px;color:var(--inspection-muted)}
.inspection-more summary{cursor:pointer}.inspection-more :deep(.n-button){margin-top:4px}
.inspection-footer{padding:8px 0;border-top:1px solid var(--inspection-line);justify-content:flex-end}
.inspection-footer p{flex-basis:100%}
.inspection-history{display:flex;justify-content:space-between;align-items:center;gap:5px;padding:6px 0;border-bottom:1px solid var(--inspection-line);color:var(--inspection-accent)}
.inspection-order{width:min(510px,94vw)}
.order-list{max-height:48dvh;overflow:auto}
.order-list label{display:flex;align-items:center;gap:7px;min-height:38px;border-bottom:1px solid var(--inspection-line);cursor:pointer}
.order-list input{flex:none}
.order-toolbar{padding:5px 0}
.order-toolbar input{width:58px}
.muted{color:var(--inspection-muted)}.warning{color:var(--cf-warning)}.done{color:var(--cf-success)}
@media(min-width:1260px){.inspection-devices{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:760px){
  .inspection-page{padding:6px 8px;gap:6px}
  .inspection-heading{flex-wrap:nowrap;gap:4px;padding-bottom:5px}
  .inspection-title{min-width:0;flex:1}.inspection-title h1{font-size:16px}
  .inspection-title p{font-size:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .inspection-heading-tools{flex-wrap:nowrap;gap:3px}
  .inspection-heading-tools :deep(.n-button){min-height:36px;padding-inline:7px;font-size:12px;white-space:nowrap}
  .inspection-batch{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .inspection-devices{grid-template-columns:minmax(0,1fr);gap:0}
  .inspection-page :is(input,select,textarea),.order-toolbar input{font-size:16px}
  .inspection-page :deep(.n-button),.inspection-order :deep(.n-button){min-height:42px}
}
@media(max-width:430px){
  .inspection-filters{grid-template-columns:minmax(0,1fr) 82px auto}
  .inspection-filters label{font-size:11px;white-space:nowrap;gap:3px}
  .inspection-heading-tools{gap:6px}
  .inspection-footer>*:not(p){flex:1 1 auto}
}
</style>
