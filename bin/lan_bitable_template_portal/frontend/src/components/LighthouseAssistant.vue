<template>
  <Teleport to="body">
    <aside
      ref="root"
      class="lighthouse"
      @keydown.esc.stop="requestPanelClose"
      @dragover="onFileDragOver"
      @drop="onFileDrop"
    >
      <div v-if="!open" class="launcher-wrap">
        <button
          ref="launcher"
          class="assistant-launcher"
          aria-label="打开灯塔助手"
          title="点击打开；拖动移动位置；Alt+方向键移动"
          @click="onLauncherClick"
          @pointerdown="onLauncherPointerDown"
          @keydown="onLauncherKeydown"
          @lostpointercapture="onDragCaptureLost"
        >
          <Bot :size="22" /><span>灯塔助手</span>
        </button>
      </div>

      <section v-else class="assistant-panel" aria-label="灯塔助手" role="dialog" aria-labelledby="assistant-title">
        <header
          class="assistant-header"
          tabindex="0"
          aria-label="助手面板"
          title="拖动顶部移动位置"
          @pointerdown="onHeaderPointerDown"
          @keydown="onHeaderKeydown"
          @lostpointercapture="onDragCaptureLost"
        >
          <div class="panel-title">
            <Bot :size="21" /><h2 id="assistant-title">灯塔助手</h2>
          </div>
          <div class="tools">
            <button v-if="state.can_manage_settings" class="icon" :disabled="busy || settingsLoading" :aria-pressed="settingsOpen" title="模型设置" aria-label="模型设置" @click="showSettings">
              <Loader2 v-if="settingsLoading" :size="17" class="spin" /><Settings v-else :size="17" />
            </button>
            <button v-if="!settingsOpen" class="icon" :disabled="busy || uploading || !state.turns?.length || state.turns.some((t: Dict) => ['running', 'submitted'].includes(t.plan?.status))" title="清空会话" aria-label="清空会话" @click="clearOpen = true"><Trash2 :size="17" /></button>
            <button class="icon" :disabled="settingsOpen && settingBusy" title="收起助手" aria-label="收起助手" @click="requestPanelClose"><X :size="19" /></button>
          </div>
        </header>

        <form v-if="settingsOpen" class="model-settings" @submit.prevent="saveModel()">
          <div class="settings-scroll">
            <div class="settings-header-row">
              <h3>模型设置</h3>
              <label class="enabled"><input type="checkbox" :checked="!!modelSettings.enabled" :disabled="busy || settingBusy" @change="toggleEnabled" /><span>启用助手</span></label>
            </div>

            <div class="settings-section-label">已配置模型</div>
            <div class="model-list">
              <article v-for="m in adminModels" :key="m.id" class="model-card compact-row" :class="{ active: isSelectedForEdit(m) }">
                <button type="button" class="list-row" :disabled="busy" @click="beginEditAction({ kind: 'edit', id: String(m.id) })">
                  <span class="list-name">{{ m.name }}</span>
                  <span class="list-model">{{ m.model }}</span>
                  <span v-if="isDefault(m)" class="model-flag">默认</span>
                  <span v-else-if="!m.configured" class="model-flag warn">未配置</span>
                </button>
                <div class="model-actions">
                  <button
                    type="button"
                    class="icon-tool"
                    :disabled="busy"
                    title="编辑模型"
                    aria-label="编辑"
                    @click="beginEditAction({ kind: 'edit', id: String(m.id) })"
                  ><Pencil :size="14" /></button>
                  <button
                    type="button"
                    class="icon-tool"
                    :disabled="busy || isDefault(m)"
                    :title="isDefault(m) ? '已经是默认模型' : '设为默认'"
                    :aria-label="isDefault(m) ? '已经是默认模型' : '设为默认'"
                    @click="setDefault(m)"
                  ><Star :size="14" /></button>
                  <button
                    type="button"
                    class="icon-tool danger"
                    :disabled="busy || settingBusy"
                    title="删除模型"
                    aria-label="删除"
                    @click="requestDelete(m)"
                  ><Trash2 :size="14" /></button>
                </div>
              </article>
              <div v-if="!adminModels.length" class="model-empty">尚无已配置模型</div>
            </div>

            <button type="button" class="add-model" :disabled="busy" @click="addStart"><Plus :size="15" />添加模型</button>

            <section v-if="editProfile" class="profile-form">
              <div class="settings-section-label">{{ editingExisting ? '编辑模型' : '添加模型' }}</div>
              <label class="field-label">显示名称<input v-model="editProfile.name" type="text" maxlength="60" :disabled="busy" /></label>
              <label class="field-label">接口地址<input v-model="editProfile.endpoint" type="text" maxlength="2000" :disabled="busy" placeholder="https://…/v1/chat/completions" /></label>
              <label class="field-label">模型名称<input v-model="editProfile.model" type="text" maxlength="200" :disabled="busy" /></label>
              <label class="field-label">API Key<input v-model="editProfile.api_key" type="password" autocomplete="new-password" :disabled="busy" :placeholder="editingExisting ? '留空保留原凭证' : '请输入 API Key'" maxlength="500" /></label>
            </section>
            <p v-if="settingsError" class="failure" role="alert">{{ settingsError }}</p>
          </div>
          <div class="settings-actions">
            <button type="button" :disabled="busy || settingBusy" @click="requestCloseSettings">返回会话</button>
            <button v-if="editProfile" type="submit" class="primary save" :disabled="busy || !canSaveProfile">
              <Loader2 v-if="savingProfile" :size="15" class="spin" />{{ savingProfile ? '保存中…' : '保存' }}
            </button>
          </div>
        </form>

        <template v-else>
          <div ref="thread" class="thread" aria-live="polite" :aria-busy="busy" @scroll="onThreadScroll">
            <button v-if="state.turns?.length >= 30 && hasOlder" class="history-more" :disabled="historyLoading" @click="loadHistory">{{ historyLoading ? '读取中…' : '更早的消息' }}</button>
            <div v-if="loading" class="empty"><Loader2 :size="22" class="spin" /><p>正在读取会话…</p></div>
            <div v-else-if="!state.turns?.length" class="empty"><Bot :size="30" /><p>你好，{{ userName || '欢迎' }}</p><p class="muted">今天有什么需要处理的？</p></div>
            <article v-for="turn in state.turns || []" :key="turn.operation_id" class="turn">
              <div class="message-row user">
                <div class="bubble user-bubble"><p>{{ turn.question }}</p><div v-if="turn.attachments?.length" class="message-files"><a v-for="file in turn.attachments" :key="file.id" :href="safeAttachmentUrl(file.url)" target="_blank" rel="noopener"><img v-if="file.is_image" :src="safeAttachmentUrl(file.url)" :alt="file.name" /><FileText v-else :size="20" /><span>{{ file.name }}</span></a></div></div>
              </div>
              <div class="message-row assistant">
                <div class="answer">
                  <details v-if="(turn.process || []).length" class="process">
                    <summary>
                      <ChevronRight :size="13" class="process-chevron" aria-hidden="true" />
                      <Loader2 v-if="turn.status === 'pending'" :size="13" class="spin" />
                      <strong>处理过程</strong>
                      <span v-if="turn.status === 'pending' && processLatest(turn)" class="process-latest">{{ processLatest(turn) }}</span>
                    </summary>
                    <ol class="process-rows">
                      <li v-for="(item, i) in turn.process || []" :key="i" :class="{ current: turn.status === 'pending' && i === (turn.process || []).length - 1 }">
                        <span class="process-dot" /><span class="process-label">{{ item.label }}</span>
                      </li>
                    </ol>
                  </details>
                  <LighthouseReply v-if="turn.answer" :text="turn.answer" />
                  <p v-else-if="turn.status === 'pending' && !(turn.process || []).length" class="pending"><Loader2 :size="15" class="spin" />{{ pendingText(turn) }}</p>
                  <p v-else-if="turn.status === 'superseded'" class="muted">已结合补充信息继续处理</p>
                  <p v-else-if="turn.status !== 'pending'" class="failure">{{ turn.error || '上次回答已中断，可重试。' }}</p>
                  <p v-if="turn.status === 'pending' && turn.answer && !(turn.process || []).length" class="pending"><Loader2 :size="14" class="spin" />{{ pendingText(turn) }}</p>
                  <p v-if="turn.answer && ['stopped', 'failed'].includes(turn.status)" class="muted">{{ turn.error || '回答未完成' }}</p>
                  <span v-if="turn.model_name && turn.answer" class="used-model">由 {{ turn.model_name }} 回答</span>
                  <div v-if="turn.output_files?.length" class="message-files output-files"><a v-for="file in turn.output_files" :key="file.id" :href="safeAttachmentUrl(file.url)" target="_blank" rel="noopener"><FileText :size="18" /><span>{{ file.name }}</span><Download :size="15" /></a></div>
                  <section v-if="turn.plan" class="operation-plan" :data-plan-status="turn.plan.status">
                    <div class="plan-title"><ListChecks :size="16" /><strong>{{ turn.plan.title }}</strong><span>{{ planStatusLabel(turn.plan.status) }}</span></div>
                    <p v-if="turn.plan.explanation" class="plan-description">{{ turn.plan.explanation }}</p>
                    <ol class="plan-steps"><li v-for="(op, i) in turn.plan.operations || []" :key="i"><strong>{{ op.name || '业务操作' }}</strong><dl><template v-for="item in operationPreview(op)" :key="item.label"><dt>{{ item.label }}</dt><dd>{{ item.value }}</dd></template></dl></li></ol>
                    <form v-if="turn.plan.status === 'needs_input'" class="plan-form" @submit.prevent="amendPlan(turn)">
                      <div v-for="field in (turn.plan.fields || []).filter((f: Dict) => fieldVisible(turn.plan, f))" :key="field.name" class="plan-field" :class="{ 'checkbox-field': field.type === 'checkbox' }"><label :for="planFieldId(turn.plan, field)">{{ field.label }}<b v-if="field.required"> *</b></label>
                        <div v-if="field.options_source" class="option-search"><input v-model="planValues[turn.plan.id + ':search:' + field.name]" type="search" maxlength="120" placeholder="按名称查找" aria-label="查找可选记录" /><button type="button" :disabled="planBusy" @click="loadPlanOptions(turn, field)"><Search :size="15" />查找</button></div>
                        <small v-if="field.options_total > 40 || field.options_has_more" class="muted">候选较多，可按名称缩小范围。</small>
                        <textarea v-if="field.type === 'textarea'" :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" :disabled="planBusy" :required="field.required" :maxlength="field.maxlength" rows="3" />
                        <select v-else-if="field.type === 'select'" :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" :disabled="planBusy" :required="field.required"><option value="">请选择</option><option v-for="option in field.options || []" :key="String(option.value)" :value="option.value">{{ option.label }}</option></select>
                        <fieldset v-else-if="field.type === 'multiselect'" :id="planFieldId(turn.plan, field)" class="plan-choices" :disabled="planBusy"><legend class="sr-only">{{ field.label }}</legend><label v-for="option in field.options || []" :key="String(option.value)"><input type="checkbox" :checked="planChoices(turn.plan, field).includes(option.value)" :disabled="field.maxItems && planChoices(turn.plan, field).length >= field.maxItems && !planChoices(turn.plan, field).includes(option.value)" @change="togglePlanChoice(turn.plan, field, option.value, ($event.target as HTMLInputElement).checked)" /><span>{{ option.label }}</span></label><span v-if="!(field.options || []).length" class="muted">{{ field.options_source ? '候选待读取' : '暂无可选项' }}</span><div class="choice-summary"><span>已选 {{ planChoices(turn.plan, field).length }}{{ field.maxItems ? ' / ' + field.maxItems : '' }}</span><button v-if="planChoices(turn.plan, field).length" type="button" class="icon" title="清空选择" :aria-label="'清空' + field.label" @click="planValues[turn.plan.id + ':' + field.name] = []"><X :size="14" /></button></div></fieldset>
                        <div v-else-if="['date', 'time', 'month', 'datetime-local'].includes(field.type)" class="plan-date"><input :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" :type="field.type" :disabled="planBusy" :required="field.required" :step="field.step" :min="field.min" :max="field.max" /><button type="button" class="icon" :disabled="planBusy" :title="'选择' + field.label" :aria-label="'选择' + field.label" @click="openPlanPicker($event)"><Clock3 v-if="field.type === 'time'" :size="17" /><CalendarDays v-else :size="17" /></button></div>
                        <input v-else-if="field.type === 'file'" :id="planFieldId(turn.plan, field)" type="file" multiple :disabled="planBusy" :required="field.required && !planValues[turn.plan.id + ':' + field.name]?.length" @change="uploadPlanFiles($event, turn.plan, field)" />
                        <input v-else-if="field.type === 'checkbox'" :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" type="checkbox" :disabled="planBusy" />
                        <input v-else :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" :type="field.type || 'text'" :disabled="planBusy" :required="field.required" :min="field.min" :max="field.max" :step="field.step" :maxlength="field.maxlength" />
                      </div>
                      <button class="primary" :disabled="planBusy || uploading"><Loader2 v-if="planBusy" :size="15" class="spin" />补充并继续</button>
                    </form>
                    <p v-if="turn.plan.status === 'awaiting_second_confirmation'" class="plan-risk">此操作涉及正式提交、删除或覆盖。请再次确认操作清单和目标。</p>
                    <p v-if="turn.plan.error" class="failure">{{ turn.plan.error }}</p>
                    <div v-if="['needs_input', 'awaiting_confirmation', 'awaiting_second_confirmation'].includes(turn.plan.status)" class="plan-actions"><button type="button" :disabled="planBusy" @click="cancelPlan(turn)">取消操作</button><button v-if="turn.plan.status !== 'needs_input'" type="button" class="primary" :disabled="planBusy" @click="confirmPlan(turn)">{{ turn.plan.status === 'awaiting_second_confirmation' ? '再次确认并执行' : '确认操作清单' }}</button></div>
                    <div v-if="turn.plan.results?.length" class="plan-results"><div v-for="(result, i) in turn.plan.results" :key="i"><Check v-if="result.ok" :size="15" /><AlertCircle v-else :size="15" /><span>{{ result.ok ? (result.job_result ? '业务处理已完成' : result.status === 202 ? '已提交后台处理' : '接口已完成') : (result.error || '操作未完成') }}</span><a v-if="safeAttachmentUrl(result.data?.url)" :href="safeAttachmentUrl(result.data.url)" target="_blank" rel="noopener">{{ result.data.name || '下载文件' }}</a></div></div>
                  </section>
                  <div v-if="turnIsComplete(turn) && visibleInteractions(turn).length" class="interactions">
                    <button v-for="interaction in visibleInteractions(turn)" :key="interaction.url" type="button" class="interaction" :title="interaction.title" @click="runInteraction(interaction)">
                      <ArrowUpRight :size="14" />{{ interaction.label }}
                    </button>
                  </div>
                  <details v-if="turn.sources?.length" class="sources"><summary>参考资料 {{ turn.sources.length }}</summary><a v-for="source in turn.sources" :key="source.number" :href="safeLink(source.url)" @click="openSource($event, source.url)">[{{ source.number }}] {{ source.title }}</a></details>
                  <p v-for="warning in turn.warnings || []" :key="warning" class="source-warning">{{ warning }}</p>
                  <div class="message-actions">
                    <button v-if="turn.answer" type="button" class="icon" title="复制回答" aria-label="复制回答" @click="copyAnswer(turn)"><Copy :size="15" /></button>
                    <button type="button" class="icon" title="编辑后重新提问" aria-label="编辑后重新提问" @click="editQuestion(turn)"><Pencil :size="15" /></button>
                    <button v-if="['failed', 'stopped'].includes(turn.status) && !sending" type="button" class="retry" @click="send(turn)"><RotateCcw :size="14" />{{ turn.status === 'stopped' ? '继续回答' : '重试' }}</button>
                    <button v-if="turn.status === 'completed' && !turn.plan && !state.busy" type="button" class="icon" title="重新生成回答" aria-label="重新生成回答" @click="send(turn, true)"><RefreshCw :size="15" /></button>
                  </div>
                </div>
              </div>
            </article>
          </div>
          <button v-if="hasNewContent" type="button" class="new-content" @click="scrollBottom()"><ArrowDown :size="14" />新消息</button>
          <div v-if="error" class="assistant-error" role="alert"><span>{{ error }}</span><button v-if="!sending" class="icon" title="重新读取会话" aria-label="重新读取会话" @click="readState(true, true)"><RefreshCw :size="16" /></button></div>
          <div v-if="!loading && (!state.configured || !state.enabled)" class="config-warning">{{ state.enabled === false ? '助手暂未启用' : '请管理员配置模型凭证' }}</div>

          <form class="composer" @submit.prevent="send()">
            <label class="sr-only" for="assistant-question">询问灯塔助手</label>
            <div v-if="draftFiles.length" class="draft-files"><div v-for="file in draftFiles" :key="file.localId" class="draft-file"><img v-if="file.preview" :src="file.preview" :alt="file.name" /><FileText v-else :size="22" /><span>{{ file.name }}<small>{{ file.uploading ? '读取中…' : file.error || '已就绪' }}</small></span><button type="button" class="icon" :disabled="file.uploading" :aria-label="'移除附件 ' + file.name" @click="removeDraftFile(file.localId)"><X :size="14" /></button></div></div>
            <textarea id="assistant-question" ref="input" v-model="draft" :disabled="loading || !state.configured || !state.enabled" rows="2" maxlength="12000" :placeholder="state.busy ? '可以继续补充信息…' : '询问信息，或描述要办理的事项'" @paste="onComposerPaste" @keydown.enter.exact="submitOnEnter" />
            <div class="composer-footer">
              <div class="composer-model"><button type="button" class="icon" title="添加图片或文件" aria-label="添加图片或文件" :disabled="sending || uploading" @click="attachmentInput?.click()"><Paperclip :size="18" /></button><input ref="attachmentInput" class="sr-only" type="file" multiple accept=".png,.jpg,.jpeg,.webp,.gif,.bmp,.pdf,.txt,.md,.csv,.log,.json,.xml,.docx,.xlsx,.xlsm" @change="onAttachmentChange" />
              <div v-if="modelNames.length" class="model-select-small">
                <label class="sr-only" for="assistant-model">选择模型</label>
                <select id="assistant-model" class="native-model-select" :value="state.model_name || ''" :title="state.model_name || '选择模型'" :disabled="sending || selecting" @change="switchModel(($event.target as HTMLSelectElement).value)"><option v-for="name in modelNames" :key="name" :value="name">{{ name }}</option></select>
              </div>
              </div>
              <button v-if="state.active_run_id && state.busy" type="button" class="stop" :disabled="stopping" title="停止生成" aria-label="停止生成" @click="stopAnswer"><Loader2 v-if="stopping" :size="16" class="spin" /><Square v-else :size="16" /><span>{{ stopping ? '正在停止' : '停止回答' }}</span></button>
              <button class="send primary" :class="{ round: true }" :title="state.busy ? '补充信息' : '发送问题'" aria-label="发送问题" :disabled="sending || loading || uploading || (!draft.trim() && !draftFiles.some(f => f.id)) || !state.configured || !state.enabled">
                <Loader2 v-if="sending" :size="20" class="spin" /><ArrowUp v-else :size="20" />
              </button>
            </div>
          </form>
        </template>
      </section>

      <ConfirmDialog :open="clearOpen" title="清空会话" message="清空当前账号的助手聊天记录？" tone="danger" @resolve="clearConversation" />
      <ConfirmDialog :open="deleteModelOpen" title="删除模型" :message="deleteMessage" tone="danger" @resolve="confirmDeleteModel" />
      <ConfirmDialog :open="editSwitchOpen" title="切换编辑模型" message="当前编辑有未保存的内容，切换后这些修改将被丢弃。" tone="warning" @resolve="confirmEditSwitch" />
      <ConfirmDialog :open="settingsExitOpen" title="返回会话" message="当前编辑有未保存的内容，返回后这些修改将被丢弃。" tone="warning" @resolve="confirmSettingsExit" />
      <ConfirmDialog :open="panelExitOpen" title="收起助手" message="当前编辑有未保存的内容，收起后这些修改将被丢弃。" tone="warning" @resolve="confirmPanelExit" />
    </aside>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, shallowRef, watch } from 'vue';
import type { Chat } from '@ai-sdk/vue';
import type { UIMessage } from 'ai';
import { AlertCircle, ArrowDown, ArrowUp, ArrowUpRight, Bot, CalendarDays, Check, ChevronRight, Clock3, Copy, Download, FileText, ListChecks, Loader2, Paperclip, Pencil, Plus, RefreshCw, RotateCcw, Search, Settings, Square, Star, Trash2, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { navigate } from '../navigation';
import ConfirmDialog from './ConfirmDialog.vue';
import LighthouseReply from './LighthouseReply.vue';

const props = withDefaults(defineProps<{ userName: string; userId?: string }>(), { userName: '', userId: '' });

type EditProfile = { id: string; name: string; endpoint: string; model: string; api_key: string };
type EditAction = { kind: 'edit'; id: string } | { kind: 'add' };
type TurnInteraction = { kind: 'navigate'; label: string; url: string; title: string };
type DraftFile = { localId: string; id?: string; name: string; mime?: string; size?: number; url?: string; preview?: string; uploading: boolean; error: string; is_image?: boolean };

const open = ref(false), settingsOpen = ref(false), loading = ref(false), sending = ref(false), selecting = ref(false), settingBusy = ref(false), settingsLoading = ref(false), savingProfile = ref(false);
const state = ref<Dict>({ turns: [], can_manage_settings: false }), modelSettings = ref<Dict>({ models: [], enabled: false, active_model_id: '' });
const draft = ref(''), error = ref(''), settingsError = ref('');
const draftFiles = ref<DraftFile[]>([]), attachmentInput = ref<HTMLInputElement | null>(null), planBusy = ref(false);
const uploading = computed(() => draftFiles.value.some(file => file.uploading));
const stopping = ref(false), hasNewContent = ref(false), historyLoading = ref(false), hasOlder = ref(true);
const streamChat = shallowRef<Chat<UIMessage> | null>(null);
let streamRun = '', streamSerial = 0, renderTimer = 0;
const planValues = reactive<Dict>({});
const clearOpen = ref(false);
const editProfile = ref<EditProfile | null>(null), editingExisting = ref(false);
const deleteModelOpen = ref(false), deleteTarget = ref<Dict | null>(null);
const editSwitchOpen = ref(false);
const settingsExitOpen = ref(false), panelExitOpen = ref(false);
let pendingEditAction: EditAction | null = null;
const input = ref<HTMLTextAreaElement | null>(null), launcher = ref<HTMLButtonElement | null>(null), thread = ref<HTMLElement | null>(null), root = ref<HTMLElement | null>(null);

const busy = computed(() => sending.value || !!state.value.busy || selecting.value || settingBusy.value || settingsLoading.value || planBusy.value);
const adminModels = computed<Dict[]>(() => (modelSettings.value.models || []));
const modelNames = computed<string[]>(() => (state.value.model_options || []).map((m: Dict) => String(m.name)).filter(Boolean));
const canSaveProfile = computed(() => Boolean(editProfile.value && editProfile.value.name.trim() && editProfile.value.endpoint.trim() && editProfile.value.model.trim()));
const deleteMessage = computed(() => {
  if (!deleteTarget.value) return '';
  const editingThis = Boolean(editProfile.value && String(editProfile.value.id) === String(deleteTarget.value.id) && isDirty());
  const base = `删除模型「${String(deleteTarget.value.name || '')}」？删除后无法恢复。`;
  return editingThis ? `${base}该模型当前有未保存的编辑内容，将一并丢弃。` : base;
});

let timer = 0, disposed = false, readInFlight = false;
let readController: AbortController | null = null;
let settingsController: AbortController | null = null;
let settingsCallSerial = 0;
let shapeEpoch = 0; // bumps on every open/close so stale async positional callbacks (delayed GET tail, close nextTick) are invalidated

const PANEL_W = 520, PANEL_H = 680, LAUNCHER_W = 180, LAUNCHER_H = 50, MARGIN = 12, STORAGE_PREFIX = 'lighthouse_pos:';
type Shape = 'launcher' | 'panel';
let launcherPos = { x: 0, y: 0 };
let panelPos = { x: 0, y: 0 };
let posAppliedLauncher = false; // true when explicit left/top mode is active for the launcher
let posAppliedPanel = false; // true when explicit left/top mode is active for the panel
let dragging = false;
let dragShape: Shape = 'launcher'; // shape a drag gesture was started on (must not write to a different shape)
let dragPointerId: number | null = null;
let dragStartClientX = 0, dragStartClientY = 0, dragOriginX = 0, dragOriginY = 0;
let dragRaf = 0;
let dragLatestX = 0, dragLatestY = 0;
let dragCaptureTarget: HTMLElement | null = null;

let launcherOpenBlocked = false;
let press: { target: HTMLElement; id: number; x: number; y: number; launcher: boolean } | null = null;
let pressTimer = 0;

function call(path: string, method = 'GET', body?: Dict, timeoutMs = 20000, signal?: AbortSignal): Promise<Dict> {
  const options: {
    method: string;
    timeoutMs?: number;
    body?: string;
    signal?: AbortSignal;
  } = { method, timeoutMs: timeoutMs || 20000 };
  if (body) options.body = JSON.stringify(body);
  if (signal) options.signal = signal;
  return requestJson('/api/assistant/' + path, options);
}

function submitOnEnter(event: KeyboardEvent): void {
  if (event.isComposing || event.keyCode === 229) return;
  event.preventDefault(); void send();
}
function scrollBottom(): void { hasNewContent.value = false; void nextTick(() => { if (thread.value) thread.value.scrollTop = thread.value.scrollHeight; }); }
function onThreadScroll(): void { if (isNearBottom()) hasNewContent.value = false; }
function isNearBottom(): boolean {
  return !thread.value || thread.value.scrollHeight - thread.value.scrollTop - thread.value.clientHeight < 80;
}
function pendingText(turn: Dict): string {
  const phase = String(state.value.phase || '');
  if (phase === 'agent_planning') return '正在理解问题并准备处理…';
  if (phase === 'agent_querying') return '正在查询业务资料…';
  if (phase === 'compressing') return '正在压缩上下文…';
  if (phase === 'searching') return '正在查询资料并回答…';
  if (phase === 'answering') return '正在生成回答…';
  return phase || '正在回答…';
}
function processLatest(turn: Dict): string {
  const rows = Array.isArray(turn?.process) ? turn.process : [];
  if (!rows.length) return '';
  const last = rows[rows.length - 1];
  return String(last?.label || '');
}
function replaceState(result: Dict): void {
  const follow = isNearBottom();
  const changedConversation = !!state.value.conversation_id && result.conversation_id !== state.value.conversation_id;
  if (changedConversation) { disconnectStream(); hasOlder.value = true; }
  const pending = changedConversation ? [] : (state.value.turns || []).filter((turn: Dict) => turn.client_pending && !(result.turns || []).some((saved: Dict) => saved.operation_id === turn.operation_id));
  for (const turn of result.turns || []) {
    const live = (state.value.turns || []).find((item: Dict) => item.operation_id === turn.operation_id && item.run_id === turn.run_id);
    if (!live) continue;
    if (turn.status === 'pending' && (live.answer || '').length > (turn.answer || '').length) turn.answer = live.answer;
    if (turn.status === 'pending' && Number(live.process?.at(-1)?.at || 0) > Number(turn.process?.at(-1)?.at || 0)) turn.process = live.process;
  }
  const older = changedConversation ? [] : (state.value.turns || []).filter((turn: Dict) => turn.history_loaded && !(result.turns || []).some((saved: Dict) => saved.operation_id === turn.operation_id));
  result.turns = [...older, ...(result.turns || []), ...pending].sort((a: Dict, b: Dict) => Number(a.at || 0) - Number(b.at || 0));
  state.value = result;
  restoreOutgoing();
  error.value = '';
  if (follow) scrollBottom();
}
function abortRead(): void {
  if (readController) { readController.abort(); readController = null; }
  readInFlight = false;
}
function scheduleRead(): void {
  window.clearTimeout(timer);
  if (open.value && !disposed && (state.value.busy || sending.value || state.value.turns?.some((t: Dict) => ['running', 'submitted'].includes(t.plan?.status)))) {
    timer = window.setTimeout(() => { void readState(false, false); }, 2200);
  }
}
async function readState(showLoading = true, force = false): Promise<void> {
  if (!showLoading && readInFlight && !force) { scheduleRead(); return; }
  abortRead();
  const controller = new AbortController();
  readController = controller;
  readInFlight = true;
  if (showLoading) loading.value = true;
  try {
    const result = await call('conversation', 'GET', undefined, 20000, controller.signal);
    if (disposed || readController !== controller) return;
    // 响应到达后、替换 state 之前再测跟随，避免长 GET 返回时被强制跳底（首次打开 showLoading 可强制）。
    const follow = showLoading || isNearBottom();
    replaceState(result);
    if (result.active_run_id && open.value) attachStream(result.active_run_id);
    if (follow) scrollBottom();
  } catch (e) {
    if (disposed || readController !== controller) return;
    if (controller.signal.aborted) return;
    if (showLoading) error.value = e instanceof Error ? e.message : '会话读取失败';
  } finally {
    if (readController === controller) {
      readController = null;
      readInFlight = false;
      loading.value = false;
      if (!disposed && open.value) scheduleRead();
    }
  }
}
async function openAssistant(): Promise<void> {
  if (open.value || disposed) return;
  const epoch = ++shapeEpoch;
  stopGesture(); // 打开前结束任何进行中的手势，避免形状切换后手势回写旧坐标。
  open.value = true;
  saveOpenState();
  await nextTick();
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  applyShapePosition('panel');
  // busy 下也允许重开查看进度：只重新 GET + 轮询显示原请求，不产生新的 POST。
  await readState(true, true);
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  await nextTick();
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  ensureInBounds('panel');
  input.value?.focus();
}
function onLauncherClick(event: MouseEvent): void {
  if (launcherOpenBlocked) {
    launcherOpenBlocked = false;
    event.preventDefault();
    event.stopPropagation();
    return;
  }
  void openAssistant();
}
function close(): void {
  const epoch = ++shapeEpoch;
  stopGesture(); // 先结束手势：仍在面板/启动器形态时保存末帧坐标，再切换形状。保留 epoch 守卫。
  open.value = false; settingsOpen.value = false; editProfile.value = null; settingsError.value = ''; settingsLoading.value = false;
  saveOpenState();
  window.clearTimeout(timer);
  ++settingsCallSerial;
  if (settingsController) { settingsController.abort(); settingsController = null; }
  abortRead();
  disconnectStream();
  void nextTick(() => { if (!disposed && open.value === false && epoch === shapeEpoch) { applyShapePosition('launcher'); launcher.value?.focus(); } });
}
async function send(retry?: Dict, regenerate = false): Promise<void> {
  if (sending.value || loading.value || uploading.value || (!retry && !draft.value.trim() && !draftFiles.value.some(f => f.id))) return;
  const question = retry?.question || draft.value.trim();
  const attachments = retry?.attachments || draftFiles.value.filter(f => f.id).map(({ preview, localId, uploading, error, ...file }) => file);
  const operation = retry?.operation_id || (window.crypto?.randomUUID?.() || `${Date.now()}_${Math.random().toString(36).slice(2)}_assistant`);
  const attempt = retry?.client_pending && !regenerate ? (retry.attempt_id || operation) : (window.crypto?.randomUUID?.() || `${Date.now()}_${Math.random().toString(36).slice(2)}_attempt`);
  const at = retry?.at || Date.now() / 1000;
  sending.value = true; error.value = '';
  abortRead();
  rememberOutgoing({ conversation_id: state.value.conversation_id, operation_id: operation, attempt_id: attempt, question: question || '请处理附件', attachments, at });
  if (!retry) {
    state.value.turns.push({ question: question || '请处理附件', attachments, operation_id: operation, attempt_id: attempt, status: 'pending', client_pending: true, at });
    draft.value = '';
    for (const file of [...draftFiles.value]) if (attachments.some((sent: Dict) => sent.id === file.id)) removeDraftFile(file.localId);
  } else { retry.status = 'pending'; retry.error = ''; retry.attempt_id = attempt; retry.client_pending = true; }
  scrollBottom();
  scheduleRead();
  try {
    const result = await call('messages', 'POST', { question, file_ids: attachments.map((file: Dict) => file.id), operation_id: operation, attempt_id: attempt, conversation_id: state.value.conversation_id }, 20000);
    if (disposed) return;
    abortRead();
    forgetOutgoing(operation, attempt);
    for (const turn of state.value.turns) {
      if (turn.operation_id === operation) Object.assign(turn, result.turn, { client_pending: false });
      else if (turn.status === 'pending') turn.status = 'superseded';
    }
    state.value.active_run_id = result.run_id;
    state.value.busy = result.turn.status === 'pending';
    attachStream(result.run_id);
  } catch (e) {
    if (disposed) return;
    const message = e instanceof Error ? e.message : '回答失败，问题已保留';
    const item = state.value.turns.find((turn: Dict) => turn.operation_id === operation);
    if (item) Object.assign(item, { status: 'failed', error: message });
    await readState(false, true);
    if (!disposed && item?.client_pending) error.value = message;
  } finally {
    sending.value = false;
    if (!disposed) { scheduleRead(); void nextTick(() => input.value?.focus()); }
  }
}

function outgoing(): Dict[] {
  try { const value = JSON.parse(sessionStorage.getItem(storageKey() + ':outgoing') || '[]'); return Array.isArray(value) ? value : []; }
  catch { return []; }
}
function rememberOutgoing(item: Dict): void {
  try { sessionStorage.setItem(storageKey() + ':outgoing', JSON.stringify([...outgoing().filter(old => old.operation_id !== item.operation_id), item])); }
  catch { /* The visible message remains available when browser storage is full. */ }
}
function forgetOutgoing(operation: string, attempt: string): void {
  try { sessionStorage.setItem(storageKey() + ':outgoing', JSON.stringify(outgoing().filter(item => item.operation_id !== operation || item.attempt_id !== attempt))); }
  catch { /* Storage may be disabled. */ }
}
function restoreOutgoing(): void {
  for (const item of outgoing()) {
    if (item.conversation_id !== state.value.conversation_id) { forgetOutgoing(item.operation_id, item.attempt_id); continue; }
    const saved = state.value.turns.find((turn: Dict) => turn.operation_id === item.operation_id);
    if (saved?.attempt_id === item.attempt_id && !saved.client_pending) { forgetOutgoing(item.operation_id, item.attempt_id); continue; }
    if (!saved) state.value.turns.push({ ...item, status: 'failed', client_pending: true, error: '这条消息尚未确认送达，可重试原消息。' });
  }
}

function disconnectStream(): void {
  ++streamSerial;
  void streamChat.value?.stop();
  streamChat.value = null;
  streamRun = '';
  window.clearTimeout(renderTimer);
}
function renderStream(): void {
  const follow = isNearBottom();
  for (const message of streamChat.value?.messages || []) {
    if (message.role !== 'assistant') continue;
    const metadata = message.metadata as Dict | undefined;
    const turn = state.value.turns?.find((item: Dict) => item.run_id === metadata?.run_id);
    if (!turn || turn.status !== 'pending') continue;
    const answer = message.parts.filter(part => part.type === 'text').map(part => part.text).join('');
    if (answer.length >= (turn.answer || '').length) turn.answer = answer;
  }
  if (follow) scrollBottom(); else hasNewContent.value = true;
}
watch(() => streamChat.value?.messages, () => {
  if (!renderTimer) renderTimer = window.setTimeout(() => { renderTimer = 0; renderStream(); }, 100);
}, { deep: true });
async function attachStream(runId: string): Promise<void> {
  if (!runId || disposed || !open.value || (streamRun === runId && streamChat.value?.status !== 'error')) return;
  disconnectStream();
  streamRun = runId;
  const serial = streamSerial;
  try {
  const [{ Chat: ChatClient }, { DefaultChatTransport }] = await Promise.all([import('@ai-sdk/vue'), import('ai')]);
  if (disposed || serial !== streamSerial) return;
  const client = new ChatClient<UIMessage>({
    id: String(state.value.conversation_id),
    transport: new DefaultChatTransport({
      api: '/api/assistant/messages', credentials: 'same-origin',
      prepareReconnectToStreamRequest: () => ({ api: `/api/assistant/runs/${encodeURIComponent(runId)}/stream` }),
    }),
    onData: part => {
      if (disposed || serial !== streamSerial) return;
      const value = part.data as Dict;
      if (value.run_id !== runId) return;
      const turn = state.value.turns?.find((item: Dict) => item.operation_id === value.operation_id && item.run_id === runId);
      if (part.type === 'data-progress') {
        state.value.phase = value.label || '';
        if (turn && Array.isArray(value.process)) turn.process = value.process;
      } else if (part.type === 'data-turn' && turn) {
        Object.assign(turn, value);
      }
    },
    onFinish: () => { if (!disposed && serial === streamSerial) { renderStream(); void readState(false, true); } },
    onError: () => { if (!disposed && serial === streamSerial) { streamRun = ''; error.value = '连接暂时中断，正在恢复原回答。'; scheduleRead(); } },
  });
  streamChat.value = client;
  void client.resumeStream().catch(() => { if (!disposed && serial === streamSerial) scheduleRead(); });
  } catch {
    if (!disposed && serial === streamSerial) { streamRun = ''; error.value = '会话组件加载未完成，请重新打开助手。'; scheduleRead(); }
  }
}
async function stopAnswer(): Promise<void> {
  const runId = state.value.active_run_id;
  if (!runId || stopping.value) return;
  stopping.value = true;
  try {
    await call(`runs/${encodeURIComponent(runId)}/cancel`, 'POST', {});
    // Only disconnect the stream we captured; a newer run submitted during stop must be left intact.
    if (streamRun === runId) disconnectStream();
    await readState(false, true);
  }
  catch (e) { error.value = e instanceof Error ? e.message : '停止请求未完成'; }
  finally { stopping.value = false; }
}
async function copyAnswer(turn: Dict): Promise<void> {
  try { await navigator.clipboard.writeText(String(turn.answer || '')); }
  catch { error.value = '浏览器未允许复制，请选中文字后复制。'; }
}
function editQuestion(turn: Dict): void { draft.value = String(turn.question || ''); void nextTick(() => input.value?.focus()); }
async function loadHistory(): Promise<void> {
  if (historyLoading.value) return;
  historyLoading.value = true;
  const height = thread.value?.scrollHeight || 0;
  try {
    const before = Math.min(...state.value.turns.map((turn: Dict) => Number(turn.at || Date.now() / 1000)));
    const result = await call('history?before=' + before);
    const known = new Set(state.value.turns.map((turn: Dict) => turn.operation_id));
    state.value.turns.unshift(...(result.turns || []).filter((turn: Dict) => !known.has(turn.operation_id)).map((turn: Dict) => ({ ...turn, history_loaded: true })));
    hasOlder.value = !!result.has_more;
    await nextTick(); if (thread.value) thread.value.scrollTop += thread.value.scrollHeight - height;
  } catch (e) { error.value = e instanceof Error ? e.message : '历史读取失败'; }
  finally { historyLoading.value = false; }
}

function safeAttachmentUrl(value: unknown): string {
  return typeof value === 'string' && /^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(value) ? value : '';
}
function removeDraftFile(localId: string): void {
  const file = draftFiles.value.find(f => f.localId === localId);
  if (file?.preview) URL.revokeObjectURL(file.preview);
  draftFiles.value = draftFiles.value.filter(f => f.localId !== localId);
}
async function uploadFile(file: File): Promise<Dict> {
  if (!file.size || file.size > 20 * 1024 * 1024) throw new Error('单文件最多20MiB');
  const form = new FormData(); form.append('files', file);
  const result = await requestJson('/api/assistant/files', { method: 'POST', body: form, timeoutMs: 180000 });
  if (!result.files?.[0]?.id) throw new Error('文件上传未完成');
  return result.files[0];
}
async function addDraftFiles(files: File[]): Promise<void> {
  if (sending.value || disposed) return;
  if (draftFiles.value.length + files.length > 10 || draftFiles.value.reduce((n, f) => n + (f.size || 0), 0) + files.reduce((n, f) => n + f.size, 0) > 100 * 1024 * 1024) { error.value = '每次最多10个文件，合计最多100MiB'; return; }
  const entries = files.map(file => ({ file, entry: { localId: window.crypto?.randomUUID?.() || Date.now() + '_' + Math.random().toString(36), name: file.name, size: file.size, uploading: true, error: '', preview: /^image\/(png|jpeg|webp|gif|bmp)$/.test(file.type) ? URL.createObjectURL(file) : undefined } as DraftFile }));
  draftFiles.value.push(...entries.map(item => item.entry));
  for (const { file, entry } of entries) {
    if (disposed) break;
    const active = draftFiles.value.find(item => item.localId === entry.localId);
    try { const result = await uploadFile(file); if (!disposed && active) Object.assign(active, result); }
    catch (e) { if (!disposed && active) active.error = e instanceof Error ? e.message : '上传失败，请重新添加'; }
    finally { if (active) active.uploading = false; }
  }
}
function onAttachmentChange(event: Event): void { const el = event.target as HTMLInputElement; void addDraftFiles(Array.from(el.files || [])); el.value = ''; }
function onComposerPaste(event: ClipboardEvent): void { const files = Array.from(event.clipboardData?.files || []); if (files.length) { event.preventDefault(); void addDraftFiles(files); } }
function onFileDragOver(event: DragEvent): void { if (event.dataTransfer?.types.includes('Files')) event.preventDefault(); }
function onFileDrop(event: DragEvent): void { if (!event.dataTransfer?.files.length) return; event.preventDefault(); void addDraftFiles(Array.from(event.dataTransfer.files)); }
async function uploadPlanFiles(event: Event, plan: Dict, field: Dict): Promise<void> {
  if (planBusy.value || disposed) return;
  const input = event.target as HTMLInputElement, files = Array.from(input.files || []);
  if (!files.length) return;
  if (files.length > 10 || files.some(file => !file.size || file.size > 20 * 1024 * 1024) || files.reduce((n, file) => n + file.size, 0) > 100 * 1024 * 1024) { error.value = '每次最多10个文件，单份20MiB、合计100MiB'; return; }
  planBusy.value = true;
  try {
    const results = [];
    for (const file of files) { if (disposed) return; results.push(await uploadFile(file)); }
    if (!disposed) planValues[plan.id + ':' + field.name] = results.map(file => file.id);
  }
  catch (e) { if (!disposed) error.value = e instanceof Error ? e.message : '附件上传失败'; }
  finally { planBusy.value = false; }
}
function planStatusLabel(status: string): string { return ({ needs_input: '待补充', awaiting_confirmation: '待确认', awaiting_second_confirmation: '再次确认', running: '执行中', submitted: '后台处理中', completed: '已完成', cancelled: '已取消', failed: '未完成' } as Dict)[status] || status; }
function planFieldId(plan: Dict, field: Dict): string { return 'plan-' + plan.id + '-' + field.name.replace(/[^\w-]/g, '_'); }
function planChoices(plan: Dict, field: Dict): (string | number)[] { const value = planValues[plan.id + ':' + field.name]; return Array.isArray(value) ? value : []; }
function togglePlanChoice(plan: Dict, field: Dict, value: string | number, checked: boolean): void { const old = planChoices(plan, field); planValues[plan.id + ':' + field.name] = checked ? Array.from(new Set([...old, value])) : old.filter(item => item !== value); }
function openPlanPicker(event: Event): void { const input = (event.currentTarget as HTMLElement).parentElement?.querySelector('input'); if (!input) return; input.focus(); try { input.showPicker?.(); } catch { /* Native keyboard editing remains available. */ } }
function fieldVisible(plan: Dict, field: Dict): boolean { if (!field.when) return true; const parent = plan.fields.find((f: Dict) => f.operation_index === field.operation_index && f.path === field.when.path); const value = parent ? planValues[plan.id + ':' + parent.name] : plan.operations[field.operation_index || 0]?.body?.[field.when.path]; return value === field.when.equals; }
function replacePlan(plan: Dict): void {
  if (disposed) return;
  abortRead();
  const turn = state.value.turns.find((item: Dict) => item.plan?.id === plan.id);
  if (turn) {
    turn.plan = plan;
    const answer: Dict = { completed: '操作已完成。', failed: '操作未完成：' + (plan.error || ''), cancelled: '操作已取消。', submitted: '操作已提交，后台仍在处理。' };
    if (answer[plan.status]) turn.answer = answer[plan.status];
  }
}
async function loadPlanOptions(turn: Dict, field: Dict): Promise<void> {
  if (planBusy.value) return;
  planBusy.value = true;
  const scopeField = (turn.plan.fields || []).find((f: Dict) => f.path === 'scope' && f.operation_index === field.operation_index);
  const scope = scopeField ? planValues[turn.plan.id + ':' + scopeField.name] : '';
  try { replacePlan(await call('plans/' + turn.plan.id + '/options?' + new URLSearchParams({ field: field.name, q: planValues[turn.plan.id + ':search:' + field.name] || '', ...(scope ? { scope } : {}) }).toString())); }
  catch (e) { if (!disposed) error.value = e instanceof Error ? e.message : '读取未完成'; }
  finally { planBusy.value = false; if (!disposed) scheduleRead(); }
}
function operationPreview(op: Dict): { label: string; value: string }[] {
  const labels: Dict = { scope: '楼栋范围', action: '操作', work_type: '通告类型', title: '名称', name: '名称', source_record_id: '源表事项', target_record_id: '目标记录', record_id: '记录', start_time: '开始时间', end_time: '结束时间', expected: '期望时间', actual: '实际时间', content: '内容', reason: '原因', impact: '影响', progress: '进度', version: '数据版本', manual_binding_choice: '计划通告关联', signatures: '签署人员', signature_time: '演练审核人签名时间', evaluation_time: '演练评估人评估时间', commander: '指挥人', evaluator: '评估人', participants: '参演人', step_signers: '步骤执行人' };
  const valueLabels: Dict = { start: '开始', update: '更新', end: '结束', maintenance: '维保', change: '变更', event: '事件', repair: '检修', power: '上下电', polling: '轮巡', adjustment: '调整', bind: '绑定计划通告', unbound: '作为独立通告' };
  const result: { label: string; value: string }[] = [];
  const add = (object: Dict) => {
    for (const [key, value] of Object.entries(object || {})) {
      if (value == null || value === '' || /operation_id|token|secret|password|auth|api_key|^_|command_format|manual_id|manual_binding_required|^manual$/.test(key)) continue;
      if (['fields', 'form', 'draft', 'patch'].includes(key) && typeof value === 'object' && !Array.isArray(value)) { add(value as Dict); continue; }
      const text = typeof value === 'object' ? (value.$result ? `使用第${Number(value.$result.step) + 1}步的处理结果` : value.$reference ? '已选人员' : JSON.stringify(value)) : String(value);
      result.push({ label: labels[key] || key, value: op.selected_labels?.[key] || (['action', 'work_type', 'manual_binding_choice'].includes(key) ? valueLabels[text] || text : text) });
    }
  };
  add(op.path_params); add(op.params); add(op.body);
  for (const [name, files] of Object.entries(op.files || {})) result.push({ label: '附件', value: `${name} · ${(files as unknown[]).length}个文件` });
  return result;
}
async function planCall(turn: Dict, suffix: string, method: string, payload: Dict): Promise<void> {
  if (planBusy.value) return;
  planBusy.value = true; error.value = '';
  try { replacePlan(await call('plans/' + turn.plan.id + suffix, method, payload, 180000)); }
  catch (e) { if (!disposed) error.value = e instanceof Error ? e.message : '操作未完成，填写已保留'; }
  finally { planBusy.value = false; if (!disposed) scheduleRead(); }
}
function amendPlan(turn: Dict): Promise<void> { const values: Dict = {}; for (const field of turn.plan.fields || []) values[field.name] = planValues[turn.plan.id + ':' + field.name]; return planCall(turn, '', 'PATCH', { version: turn.plan.version, values }); }
function confirmPlan(turn: Dict): Promise<void> { return planCall(turn, '/confirm', 'POST', { version: turn.plan.version, stage: turn.plan.status === 'awaiting_second_confirmation' ? 'execute' : 'review' }); }
function cancelPlan(turn: Dict): Promise<void> { return planCall(turn, '/cancel', 'POST', {}); }
async function switchModel(name: string): Promise<void> {
  if (sending.value || selecting.value) return;
  const option = (state.value.model_options || []).find((m: Dict) => m.name === name);
  if (!option || String(option.id) === String(state.value.model_id)) return;
  selecting.value = true; error.value = '';
  try {
    const result = await call('conversation', 'PATCH', { conversation_id: state.value.conversation_id, model_id: option.id });
    if (disposed || !open.value) return;
    abortRead();
    replaceState(result);
  } catch (e) { if (!disposed) error.value = e instanceof Error ? e.message : '模型切换失败'; }
  finally { selecting.value = false; }
}
async function showSettings(): Promise<void> {
  if (busy.value || settingsLoading.value) return;
  if (settingsOpen.value) { requestCloseSettings(); return; }
  settingsLoading.value = true; settingsError.value = ''; error.value = '';
  const callSerial = ++settingsCallSerial;
  if (settingsController) settingsController.abort();
  const controller = new AbortController();
  settingsController = controller;
  try {
    const result = await call('settings', 'GET', undefined, 20000, controller.signal);
    if (disposed || !open.value || callSerial !== settingsCallSerial || settingsController !== controller) return;
    modelSettings.value = result;
    settingsOpen.value = true;
    primeEditing(modelSettings.value);
  } catch (e) {
    if (disposed || !open.value || callSerial !== settingsCallSerial || settingsController !== controller) return;
    if (controller.signal.aborted) return;
    error.value = e instanceof Error ? e.message : '设置读取失败';
  } finally {
    if (callSerial === settingsCallSerial) settingsLoading.value = false;
    if (settingsController === controller) settingsController = null;
  }
}
function primeEditing(settings: Dict): void {
  const models: Dict[] = settings?.models || [];
  const active = models.find((m: Dict) => String(m.id) === String(settings.active_model_id));
  const target = active || models[0];
  if (target) startEdit(target);
  else { editProfile.value = null; editingExisting.value = false; }
}
function newModelId(): string {
  const taken = new Set((modelSettings.value.models || []).map((m: Dict) => String(m.id)));
  let id = window.crypto?.randomUUID?.() || `${Date.now()}_model`;
  while (taken.has(id)) id = window.crypto?.randomUUID?.() || `${Date.now()}_${Math.random().toString(36).slice(2)}_model`;
  return id;
}
function startEdit(m: Dict): void {
  editProfile.value = { id: String(m.id || ''), name: String(m.name || ''), endpoint: String(m.endpoint || ''), model: String(m.model || ''), api_key: '' };
  editingExisting.value = true;
}
function startAdd(): void {
  editProfile.value = { id: newModelId(), name: '', endpoint: '', model: '', api_key: '' };
  editingExisting.value = false;
}
function isDirty(): boolean {
  if (!editProfile.value) return false;
  if (!editingExisting.value) return Boolean(editProfile.value.name || editProfile.value.endpoint || editProfile.value.model || editProfile.value.api_key);
  const m = (modelSettings.value.models || []).find((x: Dict) => String(x.id) === editProfile.value!.id);
  if (!m) return Boolean(editProfile.value.api_key);
  return editProfile.value.name !== String(m.name || '')
    || editProfile.value.endpoint !== String(m.endpoint || '')
    || editProfile.value.model !== String(m.model || '')
    || Boolean(editProfile.value.api_key);
}
function applyEditAction(action: EditAction): void {
  if (action.kind === 'add') { startAdd(); return; }
  const m = (modelSettings.value.models || []).find((x: Dict) => String(x.id) === action.id);
  if (m) startEdit(m);
}
function beginEditAction(action: EditAction): void {
  if (busy.value) return;
  const current = action.kind === 'edit' ? (modelSettings.value.models || []).find((x: Dict) => String(x.id) === action.id) : null;
  if (current && isSelectedForEdit(current)) return;
  if (isDirty()) { pendingEditAction = action; editSwitchOpen.value = true; return; }
  applyEditAction(action);
}
function addStart(): void {
  beginEditAction({ kind: 'add' });
}
function confirmEditSwitch(yes: boolean): void {
  editSwitchOpen.value = false;
  if (!yes) { pendingEditAction = null; return; }
  const action = pendingEditAction;
  pendingEditAction = null;
  if (action) applyEditAction(action);
}
function isDefault(m: Dict): boolean {
  return String(m.id) === String(modelSettings.value.active_model_id);
}
function isSelectedForEdit(m: Dict): boolean {
  return Boolean(editProfile.value && String(editProfile.value.id) === String(m.id)) && editingExisting.value;
}
function requestCloseSettings(): void {
  if (settingBusy.value) return;
  if (isDirty()) { settingsExitOpen.value = true; return; }
  closeSettings();
}
function confirmSettingsExit(yes: boolean): void {
  settingsExitOpen.value = false;
  if (yes) closeSettings();
}
function closeSettings(): void {
  settingsOpen.value = false; editProfile.value = null; settingsError.value = '';
}
function requestPanelClose(): void {
  if (!open.value) return;
  if (settingsOpen.value && settingBusy.value) return;
  if (settingsOpen.value && settingsLoading.value) {
    ++settingsCallSerial;
    if (settingsController) { settingsController.abort(); settingsController = null; }
    settingsLoading.value = false;
    close();
    return;
  }
  if (settingsOpen.value && isDirty()) { panelExitOpen.value = true; return; }
  close();
}
function confirmPanelExit(yes: boolean): void {
  panelExitOpen.value = false;
  if (yes) close();
}
async function saveModel(): Promise<void> {
  if (busy.value || !editProfile.value || !canSaveProfile.value) return;
  const p = editProfile.value;
  settingBusy.value = true; savingProfile.value = true; settingsError.value = '';
  try {
    const profile: Dict = { id: p.id, name: p.name.trim(), endpoint: p.endpoint.trim(), model: p.model.trim() };
    if (p.api_key.trim()) profile.api_key = p.api_key.trim();
    const result = await call('settings', 'PUT', { action: 'upsert', profile });
    if (disposed) return;
    modelSettings.value = result;
    const saved = (result.models || []).find((m: Dict) => String(m.id) === String(p.id));
    if (saved) startEdit(saved);
    settingsError.value = '';
    await readState(false, true);
  } catch (e) { if (!disposed) settingsError.value = e instanceof Error ? e.message : '保存失败，输入已保留'; }
  finally { settingBusy.value = false; savingProfile.value = false; }
}
async function setDefault(m: Dict): Promise<void> {
  if (busy.value || isDefault(m)) return;
  settingBusy.value = true; settingsError.value = '';
  try {
    const result = await call('settings', 'PUT', { action: 'select', id: m.id });
    if (disposed) return;
    modelSettings.value = result;
    await readState(false, true);
  } catch (e) { if (!disposed) settingsError.value = e instanceof Error ? e.message : '设置默认模型失败'; }
  finally { settingBusy.value = false; }
}
function requestDelete(m: Dict): void {
  if (busy.value) return;
  deleteTarget.value = m; deleteModelOpen.value = true;
}
async function confirmDeleteModel(yes: boolean): Promise<void> {
  deleteModelOpen.value = false;
  const target = deleteTarget.value; deleteTarget.value = null;
  if (!yes || !target || busy.value) return;
  settingBusy.value = true; settingsError.value = '';
  try {
    const result = await call('settings', 'PUT', { action: 'delete', id: target.id });
    if (disposed) return;
    modelSettings.value = result;
    if (editProfile.value && String(editProfile.value.id) === String(target.id)) {
      // 明确删除意图：丢弃该目标的未保存编辑，而非跳到其它模型的编辑态。
      editProfile.value = null;
      editingExisting.value = false;
    }
    await readState(false, true);
  } catch (e) { if (!disposed) settingsError.value = e instanceof Error ? e.message : '删除模型失败'; }
  finally { settingBusy.value = false; }
}
async function toggleEnabled(event: Event): Promise<void> {
  const checkbox = event.target as HTMLInputElement;
  if (busy.value) { checkbox.checked = !!modelSettings.value.enabled; return; }
  settingBusy.value = true; settingsError.value = '';
  try {
    const result = await call('settings', 'PUT', { action: 'toggle', enabled: checkbox.checked });
    if (disposed) return;
    modelSettings.value = result;
    await readState(false, true);
  } catch (e) {
    if (!disposed) settingsError.value = e instanceof Error ? e.message : '开关保存失败';
    checkbox.checked = !!modelSettings.value.enabled;
  } finally { settingBusy.value = false; }
}
async function clearConversation(yes: boolean): Promise<void> {
  clearOpen.value = false;
  if (!yes || busy.value || uploading.value) return;
  settingBusy.value = true;
  try {
    const result = await call('conversation', 'DELETE');
    if (disposed) return;
    abortRead();
    replaceState(result); draft.value = ''; error.value = '';
    for (const file of [...draftFiles.value]) removeDraftFile(file.localId);
    for (const key of Object.keys(planValues)) delete planValues[key];
  }
  catch (e) { if (!disposed) error.value = e instanceof Error ? e.message : '清空失败'; }
  finally { settingBusy.value = false; }
}
function isSafeLocalPath(value: unknown): boolean {
  const raw = String(value || '');
  if (!raw || raw.includes('\\')) return false;
  let decoded: string;
  try { decoded = decodeURIComponent(raw); } catch { return false; }
  if (!decoded.startsWith('/')) return false;
  if (decoded.startsWith('//')) return false;
  if (decoded.includes('\\')) return false;
  if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(decoded)) return false;
  if (/[\u0000-\u001f\u007f]/.test(decoded)) return false;
  return true;
}
function safeLink(value: unknown): string {
  const path = String(value || '');
  return isSafeLocalPath(path) ? path : '/';
}
function openSource(event: MouseEvent, value: unknown): void {
  if (event.ctrlKey || event.metaKey || event.shiftKey) return;
  event.preventDefault(); navigate(safeLink(value));
}
function turnIsComplete(turn: Dict): boolean {
  return Boolean(turn.answer) && turn.status === 'completed';
}
function visibleInteractions(turn: Dict): TurnInteraction[] {
  const raw = Array.isArray(turn.interactions) ? turn.interactions : [];
  const result: TurnInteraction[] = [];
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    const kind = String((item as Dict).kind || '');
    if (kind !== 'navigate') continue;
    const url = String((item as Dict).url || '');
    if (!isSafeLocalPath(url)) continue;
    const label = String((item as Dict).label || '打开相关页面');
    const title = String((item as Dict).title || label);
    result.push({ kind: 'navigate', label, url, title });
  }
  return result;
}
function runInteraction(interaction: TurnInteraction): void {
  if (interaction.kind !== 'navigate') return;
  const target = String(interaction.url || '');
  if (!isSafeLocalPath(target)) return;
  navigate(target);
}

function storageKey(): string {
  const id = String(props.userId).trim() || String(props.userName).trim() || 'anonymous';
  return STORAGE_PREFIX + id;
}
function shapeKey(shape: Shape): string {
  return storageKey() + ':' + shape;
}
function readPos(key: string): { x: number; y: number } | null {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    const x = Number((parsed as { x?: unknown }).x);
    const y = Number((parsed as { y?: unknown }).y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
    return { x, y };
  } catch { return null; }
}
function loadStoredPos(shape: Shape): { x: number; y: number } | null {
  return readPos(shapeKey(shape));
}
function saveStoredPos(shape: Shape, x: number, y: number): void {
  try { window.localStorage.setItem(shapeKey(shape), JSON.stringify({ x, y })); } catch { /* ignore quota/private */ }
}
function migrateLegacyPos(): void {
  // 旧版本只保存一个共享位置（无后缀 key）。平滑迁移到两个形态各自的存储，不删除旧 key。
  const legacy = readPos(storageKey());
  if (!legacy) return;
  if (loadStoredPos('launcher') === null) saveStoredPos('launcher', legacy.x, legacy.y);
  if (loadStoredPos('panel') === null) saveStoredPos('panel', legacy.x, legacy.y);
}
function saveOpenState(): void {
  try { window.sessionStorage.setItem(storageKey() + ':open', String(open.value)); } catch { /* private mode */ }
}
function clampPosForSize(x: number, y: number, w: number, h: number): { x: number; y: number } {
  const maxX = Math.max(MARGIN, window.innerWidth - w - MARGIN);
  const maxY = Math.max(MARGIN, window.innerHeight - h - MARGIN);
  return { x: Math.min(Math.max(x, MARGIN), maxX), y: Math.min(Math.max(y, MARGIN), maxY) };
}
function activeShape(): Shape {
  return open.value ? 'panel' : 'launcher';
}
function shapePosOf(shape: Shape): { x: number; y: number } {
  return shape === 'panel' ? panelPos : launcherPos;
}
function isShapeApplied(shape: Shape): boolean {
  return shape === 'panel' ? posAppliedPanel : posAppliedLauncher;
}
function markShapeApplied(shape: Shape): void {
  if (shape === 'panel') posAppliedPanel = true;
  else posAppliedLauncher = true;
}
function shapeSize(shape: Shape): { w: number; h: number } {
  const el = root.value;
  if (shape === 'panel') {
    // 仅当面板当前实际渲染时读取其真实尺寸；否则用面板预期尺寸，避免在初始化时用另一形态（launcher）的 DOM 尺寸夹紧面板坐标。
    return shape === activeShape() && el
      ? { w: el.offsetWidth || PANEL_W, h: el.offsetHeight || PANEL_H }
      : { w: PANEL_W, h: PANEL_H };
  }
  return shape === activeShape() && el
    ? { w: el.offsetWidth || LAUNCHER_W, h: el.offsetHeight || LAUNCHER_H }
    : { w: LAUNCHER_W, h: LAUNCHER_H };
}
function applyRootPos(shape: Shape): void {
  const el = root.value;
  if (!el) return;
  const p = shapePosOf(shape);
  el.style.left = `${p.x}px`;
  el.style.top = `${p.y}px`;
  el.style.right = 'auto';
  el.style.bottom = 'auto';
  markShapeApplied(shape);
}
function resetRootToCssDefault(): void {
  const el = root.value;
  if (!el) return;
  el.style.left = '';
  el.style.top = '';
  el.style.right = '';
  el.style.bottom = '';
}
function applyShapePosition(shape: Shape): void {
  const el = root.value;
  if (!el) return;
  if (isShapeApplied(shape)) applyRootPos(shape);
  else resetRootToCssDefault();
  ensureInBounds(shape);
}
function ensureInBounds(shape?: Shape): void {
  const el = root.value;
  if (!el || disposed) return;
  const s = shape || activeShape();
  const { w, h } = shapeSize(s);
  const rect = el.getBoundingClientRect();
  const next = clampPosForSize(rect.left, rect.top, w, h);
  if (isShapeApplied(s) || open.value || next.x !== rect.left || next.y !== rect.top) {
    const p = shapePosOf(s);
    p.x = next.x;
    p.y = next.y;
    applyRootPos(s);
    saveStoredPos(s, p.x, p.y);
  }
}
function onWindowResize(): void {
  ensureInBounds();
}
function currentOrigin(): { x: number; y: number } {
  const s = activeShape();
  if (isShapeApplied(s)) {
    const p = shapePosOf(s);
    return { x: p.x, y: p.y };
  }
  const el = root.value;
  const rect = el?.getBoundingClientRect();
  return { x: rect ? rect.left : 0, y: rect ? rect.top : 0 };
}
function beginDrag(target: HTMLElement, pointerId: number, clientX: number, clientY: number): void {
  if (dragging) return;
  dragging = true;
  dragShape = activeShape();
  dragPointerId = pointerId;
  dragStartClientX = clientX;
  dragStartClientY = clientY;
  const origin = currentOrigin();
  dragOriginX = origin.x;
  dragOriginY = origin.y;
  dragLatestX = dragOriginX;
  dragLatestY = dragOriginY;
  dragCaptureTarget = target;
  try { target.setPointerCapture(pointerId); } catch { /* unsupported */ }
  window.addEventListener('pointermove', onDragMove, { passive: false });
  window.addEventListener('pointerup', onDragEnd, true);
  window.addEventListener('pointercancel', onDragEnd, true);
}
function onDragMove(event: PointerEvent): void {
  if (!dragging || event.pointerId !== dragPointerId) return;
  event.preventDefault();
  dragLatestX = dragOriginX + (event.clientX - dragStartClientX);
  dragLatestY = dragOriginY + (event.clientY - dragStartClientY);
  if (!dragRaf) {
    dragRaf = window.requestAnimationFrame(() => {
      dragRaf = 0;
      if (!dragging || dragShape !== activeShape()) return;
      const s = dragShape;
      const { w, h } = shapeSize(s);
      const p = shapePosOf(s);
      const next = clampPosForSize(dragLatestX, dragLatestY, w, h);
      p.x = next.x;
      p.y = next.y;
      applyRootPos(s);
    });
  }
}
function onDragEnd(event: PointerEvent): void {
  if (!dragging || event.pointerId !== dragPointerId) return;
  if (dragRaf) { window.cancelAnimationFrame(dragRaf); dragRaf = 0; }
  const s = dragShape;
  // 形状已切换时不再回写该手势的坐标：被 stopGesture / close 中途中止时避免把旧形状写进当前元素。
  if (s === activeShape()) {
    const { w, h } = shapeSize(s);
    // 按最后一次事件坐标同步提交，避免 rAF 尚未执行而丢失末帧。
    const p = shapePosOf(s);
    const next = event.type === 'pointerup'
      ? clampPosForSize(dragOriginX + (event.clientX - dragStartClientX), dragOriginY + (event.clientY - dragStartClientY), w, h)
      : clampPosForSize(dragLatestX, dragLatestY, w, h);
    p.x = next.x;
    p.y = next.y;
    applyRootPos(s);
    saveStoredPos(s, p.x, p.y);
  }
  dragging = false;
  dragPointerId = null;
  window.removeEventListener('pointermove', onDragMove, false);
  window.removeEventListener('pointerup', onDragEnd, true);
  window.removeEventListener('pointercancel', onDragEnd, true);
  if (dragCaptureTarget) {
    try { dragCaptureTarget.releasePointerCapture(event.pointerId); } catch { /* already released */ }
    dragCaptureTarget = null;
  }
}
function stopGesture(): void {
  if (dragging && dragPointerId !== null) {
    onDragEnd(new PointerEvent('pointercancel', { pointerId: dragPointerId }));
  }
  cancelPress();
}
function isInteractiveTarget(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false;
  return Boolean(target.closest('button, input, textarea, select, a, [contenteditable="true"]'));
}
function onHeaderPointerDown(event: PointerEvent): void {
  if (isInteractiveTarget(event.target)) return;
  startPress(event, false);
}
function onDragCaptureLost(event: PointerEvent): void {
  if (dragging && event.pointerId === dragPointerId) onDragEnd(event);
  else if (event.pointerId === press?.id) cancelPress();
}
function onHeaderKeydown(event: KeyboardEvent): void {
  if (isInteractiveTarget(event.target)) return;
  nudgeByArrow(event);
}
function onLauncherPointerDown(event: PointerEvent): void {
  startPress(event, true);
}
function startPress(event: PointerEvent, isLauncher: boolean): void {
  if (dragging || press || !event.isPrimary || (event.pointerType === 'mouse' && event.button !== 0)) return;
  launcherOpenBlocked = false;
  press = { target: event.currentTarget as HTMLElement, id: event.pointerId, x: event.clientX, y: event.clientY, launcher: isLauncher };
  try { press.target.setPointerCapture(event.pointerId); } catch { /* unsupported */ }
  pressTimer = window.setTimeout(() => startPressDrag(), 350);
  window.addEventListener('pointermove', onPressMove, { passive: false });
  window.addEventListener('pointerup', onPressEnd, true);
  window.addEventListener('pointercancel', onPressEnd, true);
  if (!isLauncher) event.preventDefault();
}
function startPressDrag(): void {
  if (!press) return;
  const gesture = press;
  if (gesture.launcher) launcherOpenBlocked = true;
  cancelPress(false);
  beginDrag(gesture.target, gesture.id, gesture.x, gesture.y);
}
function onPressMove(event: PointerEvent): void {
  if (event.pointerId !== press?.id || Math.hypot(event.clientX - press.x, event.clientY - press.y) <= 6) return;
  startPressDrag();
  onDragMove(event);
}
function onPressEnd(event: PointerEvent): void {
  if (event.pointerId !== press?.id) return;
  if (event.type === 'pointercancel' && press.launcher) launcherOpenBlocked = true;
  cancelPress();
}
function cancelPress(release = true): void {
  const gesture = press;
  press = null;
  window.clearTimeout(pressTimer); pressTimer = 0;
  window.removeEventListener('pointermove', onPressMove);
  window.removeEventListener('pointerup', onPressEnd, true);
  window.removeEventListener('pointercancel', onPressEnd, true);
  if (release && gesture) { try { gesture.target.releasePointerCapture(gesture.id); } catch { /* already released */ } }
}
function onLauncherKeydown(event: KeyboardEvent): void {
  if (!event.altKey) return;
  nudgeByArrow(event);
}
function nudgeByArrow(event: KeyboardEvent): void {
  const step = event.shiftKey ? 40 : 16;
  let dx = 0, dy = 0;
  if (event.key === 'ArrowLeft') dx = -step;
  else if (event.key === 'ArrowRight') dx = step;
  else if (event.key === 'ArrowUp') dy = -step;
  else if (event.key === 'ArrowDown') dy = step;
  else return;
  event.preventDefault();
  const s = activeShape();
  const { w, h } = shapeSize(s);
  const origin = currentOrigin();
  const next = clampPosForSize(origin.x + dx, origin.y + dy, w, h);
  const p = shapePosOf(s);
  p.x = next.x;
  p.y = next.y;
  applyRootPos(s);
  saveStoredPos(s, p.x, p.y);
}

onMounted(async () => {
  try { open.value = window.sessionStorage.getItem(storageKey() + ':open') === 'true'; } catch { /* private mode */ }
  await nextTick();
  migrateLegacyPos();
  const lp = loadStoredPos('launcher');
  if (lp) {
    launcherPos = lp;
    posAppliedLauncher = true;
  }
  const pp = loadStoredPos('panel');
  if (pp) {
    panelPos = pp;
    posAppliedPanel = true;
  }
  // Clamp only the visible shape, using its real dimensions after layout.
  await nextTick();
  applyShapePosition(activeShape());
  window.addEventListener('resize', onWindowResize);
  window.addEventListener('blur', stopGesture);
  if (open.value) await readState(true, true);
});
onBeforeUnmount(() => {
  disposed = true;
  disconnectStream();
  stopGesture();
  for (const file of draftFiles.value) if (file.preview) URL.revokeObjectURL(file.preview);
  ++settingsCallSerial;
  if (readController) { readController.abort(); readController = null; }
  if (settingsController) { settingsController.abort(); settingsController = null; }
  readInFlight = false;
  window.clearTimeout(timer);
  if (dragRaf) { window.cancelAnimationFrame(dragRaf); dragRaf = 0; }
  window.removeEventListener('resize', onWindowResize);
  window.removeEventListener('blur', stopGesture);
  window.removeEventListener('pointermove', onDragMove, false);
  window.removeEventListener('pointerup', onDragEnd, true);
  window.removeEventListener('pointercancel', onDragEnd, true);
  if (dragCaptureTarget && dragPointerId !== null) {
    try { dragCaptureTarget.releasePointerCapture(dragPointerId); } catch { /* noop */ }
    dragCaptureTarget = null;
  }
  dragging = false;
});
</script>

<style scoped>
.lighthouse {
  --lh-charcoal: #e2e8e2;
  --lh-charcoal-strong: #f2f5ef;
  --lh-charcoal-soft: #c2cdc5;
  --lh-muted: #b3bfb6;
  --lh-faint-muted: #8f9c93;
  --lh-border: #3c4841;
  --lh-border-strong: #566359;
  --lh-surface: #232a25;
  --lh-surface-subtle: #29322c;
  --lh-surface-hover: #354238;
  --lh-accent: #b9d0b1;
  --lh-accent-strong: #d0e4c8;
  --lh-accent-soft: #354c40;
  --lh-accent-ring: rgba(185, 208, 177, 0.22);
  --lh-danger: #eea99b;
  --lh-danger-soft: #47342f;
  --lh-warn: #dec58f;
  --lh-warn-soft: #443d2e;
  --lh-input-border: #536258;
  position: fixed;
  right: 24px;
  bottom: 24px;
  z-index: 10000;
  color: var(--lh-charcoal);
  font-size: 14px;
  letter-spacing: 0;
  color-scheme: dark;
}
.lighthouse *, .lighthouse *::before, .lighthouse *::after { box-sizing: border-box; }
.lighthouse :deep(.confirm-backdrop) { background: rgba(12, 18, 14, .5); backdrop-filter: blur(4px); }
.lighthouse :deep(.confirm-modal) { background: var(--lh-surface); border-color: var(--lh-border-strong); border-radius: 12px; color: var(--lh-charcoal); box-shadow: 0 20px 70px rgba(0, 0, 0, .3); }
.lighthouse :deep(.confirm-content header strong) { color: var(--lh-charcoal-strong); }
.lighthouse :deep(.confirm-content p), .lighthouse :deep(.confirm-content header span) { color: var(--lh-muted); }
.lighthouse :deep(.confirm-icon) { background: var(--lh-accent-soft); color: var(--lh-accent); box-shadow: none; }
.lighthouse :deep(.confirm-close), .lighthouse :deep(.confirm-content .btn.ghost) { background: var(--lh-surface-subtle); border-color: var(--lh-border-strong); color: var(--lh-charcoal); }
.lighthouse :deep(.confirm-content .btn:not(.ghost)) { background: var(--lh-accent); border-color: var(--lh-accent); color: #162519; }
.lighthouse :deep(.confirm-content .btn.danger) { background: var(--lh-danger); border-color: var(--lh-danger); color: #30201c; }
@media print { .lighthouse { display: none !important; } }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 8px 12px; min-height: 34px; font: inherit; border: 1px solid var(--lh-border-strong); border-radius: 8px; color: var(--lh-charcoal); background: var(--lh-surface); cursor: pointer; }
button:hover:not(:disabled) { background: var(--lh-surface-hover); }button:disabled { opacity: .5; cursor: not-allowed; }button:focus-visible, textarea:focus-visible, input:focus-visible, a:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
.launcher-wrap { display: flex; align-items: center; gap: 6px; }
.assistant-launcher {
  border-radius: 999px;
  padding: 12px 18px;
  min-height: 50px;
  color: #ffffff;
  background: #29352c;
  border-color: #708471;
  box-shadow: 0 6px 18px rgba(24, 30, 38, 0.18);
  touch-action: none;
  user-select: none;
  cursor: grab;
}
.assistant-launcher:hover:not(:disabled) { background: #364338; }
.assistant-launcher:active { cursor: grabbing; }
.assistant-launcher svg { color: #d6dec0; }
.assistant-panel { width: min(520px, calc(100vw - 48px)); height: min(680px, calc(100dvh - max(48px, env(safe-area-inset-bottom) + 24px))); display: flex; flex-direction: column; border: 1px solid var(--lh-border-strong); border-radius: 12px; background: var(--lh-surface); box-shadow: 0 14px 40px rgba(24, 30, 38, 0.2); overflow: hidden; }
.assistant-header { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 12px 14px; border-bottom: 1px solid var(--lh-border); background: var(--lh-surface-subtle); cursor: grab; touch-action: none; user-select: none; }
.assistant-header:active { cursor: grabbing; }
.assistant-header:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: -2px; border-radius: 10px 10px 0 0; }
.panel-title, .tools { display: flex; align-items: center; gap: 6px; }
.panel-title { color: var(--lh-charcoal-strong); }
.panel-title svg { color: var(--lh-accent); }
.panel-title h2 { font-size: 16px; margin: 0; }
.icon { padding: 6px; width: 32px; height: 32px; min-height: 32px; border-color: transparent; background: transparent; touch-action: manipulation; }
.tools button { border-color: transparent; }
.tools button:hover:not(:disabled) { background: var(--lh-surface-hover); }
.thread { flex: 1; min-height: 0; overflow-y: auto; overscroll-behavior: contain; padding: 16px 14px; }.empty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; min-height: 240px; color: var(--lh-muted); }.empty p { margin: 0; }.muted { color: var(--lh-faint-muted); font-size: 13px; }
.turn { margin-bottom: 20px; }
.message-row { display: flex; }.message-row.user { justify-content: flex-end; }.message-row.assistant { margin-top: 14px; }
.turn p { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.7; }
.bubble { max-width: 86%; padding: 9px 12px; border-radius: 12px; }.user-bubble { background: var(--lh-accent-soft); color: var(--lh-charcoal-strong); margin-left: 28px; }.user-bubble p { font-size: 13.5px; }
.answer { min-width: 0; max-width: 100%; flex: 1; color: var(--lh-charcoal); }.answer .pending { display: flex; gap: 6px; align-items: center; color: var(--lh-muted); }.used-model { display: inline-block; margin-top: 6px; font-size: 12px; color: var(--lh-faint-muted); }.failure, .danger { color: var(--lh-danger); }.retry { margin-top: 8px; padding: 4px 8px; font-size: 12px; min-height: 30px; }
.sources { margin-top: 10px; font-size: 12px; color: var(--lh-muted); }.sources summary { cursor: pointer; }.sources a { display: block; margin-top: 5px; color: var(--lh-accent-strong); overflow-wrap: anywhere; text-decoration: none; }.sources a:hover { text-decoration: underline; }.answer .source-warning { font-size: 12px; color: var(--lh-warn); margin-top: 8px; }
.interactions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }.interaction { padding: 3px 9px; font-size: 12px; min-height: 30px; border-radius: 8px; color: var(--lh-accent-strong); background: var(--lh-accent-soft); max-width: 100%; overflow-wrap: anywhere; }
.assistant-error, .config-warning { flex-shrink: 0; padding: 8px 14px; font-size: 12px; line-height: 1.6; }.assistant-error { display: flex; align-items: center; gap: 6px; color: var(--lh-danger); background: var(--lh-danger-soft); }.assistant-error span { flex: 1; overflow-wrap: anywhere; }.config-warning { color: var(--lh-warn); background: var(--lh-warn-soft); }
.composer { flex-shrink: 0; border-top: 1px solid var(--lh-border); padding: 10px 12px 12px; display: flex; flex-direction: column; gap: 8px; background: var(--lh-surface); }
.composer textarea { font: inherit; resize: none; border: 1px solid var(--lh-input-border); border-radius: 8px; padding: 9px 11px; line-height: 1.6; min-width: 0; width: 100%; max-height: 140px; color: var(--lh-charcoal); }
.composer textarea:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 0; }
.composer-footer { display: flex; align-items: center; justify-content: space-between; gap: 10px; min-height: 34px; }
.model-select-small { min-width: 0; flex: 0 1 auto; max-width: 220px; }
.native-model-select { height: 34px; max-width: 220px; width: 100%; padding: 0 8px; border: 1px solid var(--lh-input-border); border-radius: 8px; font: inherit; font-size: 13px; text-overflow: ellipsis; }
.send { width: 40px; height: 40px; padding: 0; flex-shrink: 0; }.send.round { border-radius: 50%; }.primary { color: #162519; border-color: var(--lh-accent); background: var(--lh-accent); }.primary:hover:not(:disabled) { background: var(--lh-accent-strong); }
.model-settings { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.settings-scroll { flex: 1; min-height: 0; overflow-y: auto; overscroll-behavior: contain; display: flex; flex-direction: column; gap: 12px; padding: 16px 18px 8px; }
.model-settings h3 { font-size: 15px; margin: 0; }.settings-header-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; }.settings-section-label { font-size: 12px; color: var(--lh-faint-muted); }.model-settings .enabled { display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--lh-charcoal); }
.model-list { display: flex; flex-direction: column; gap: 6px; overflow-y: auto; max-height: 220px; padding-right: 2px; }
.model-card.compact-row { display: flex; align-items: center; gap: 8px; border: 1px solid var(--lh-border); border-radius: 8px; padding: 5px 8px; background: var(--lh-surface); }.model-card.compact-row.active { border-color: var(--lh-accent); background: var(--lh-accent-soft); }
.list-row { display: flex; align-items: center; gap: 8px; min-width: 0; flex: 1; padding: 2px 0; border: 0; background: transparent; color: var(--lh-charcoal-strong); text-align: left; }.list-name { min-width: 0; overflow-wrap: anywhere; font-weight: 600; }.list-model { min-width: 0; overflow-wrap: anywhere; color: var(--lh-muted); font-size: 12px; }.model-flag { margin-left: auto; flex-shrink: 0; font-size: 11px; color: var(--lh-accent-strong); background: var(--lh-accent-soft); border-radius: 999px; padding: 2px 8px; }.model-flag.warn { color: var(--lh-warn); background: var(--lh-warn-soft); }
.model-actions { display: flex; flex-shrink: 0; align-items: center; gap: 2px; }.icon-tool { width: 30px; height: 30px; min-height: 30px; padding: 0; border-color: transparent; background: transparent; }.icon-tool.danger { color: var(--lh-danger); }
.model-empty { padding: 10px; text-align: center; color: var(--lh-faint-muted); font-size: 12px; border: 1px dashed var(--lh-border-strong); border-radius: 8px; }
.add-model { border-style: dashed; color: var(--lh-accent-strong); min-height: 34px; }.add-model:hover:not(:disabled) { background: var(--lh-accent-soft); }
.profile-form { display: grid; gap: 12px; border-top: 1px solid var(--lh-border); padding-top: 14px; }.profile-form .field-label { display: grid; gap: 6px; font-size: 13px; color: var(--lh-charcoal); }.profile-form input[type=text], .profile-form input[type=password] { padding: 9px 10px; font: inherit; border: 1px solid var(--lh-input-border); border-radius: 8px; width: 100%; min-width: 0; color: var(--lh-charcoal); }
.profile-form input:disabled { background: var(--lh-surface-subtle); color: var(--lh-faint-muted); }
.settings-actions { flex: 0 0 auto; display: flex; justify-content: flex-end; gap: 8px; padding: 10px 18px; border-top: 1px solid var(--lh-border); background: var(--lh-surface-subtle); }
.lighthouse input:not([type=checkbox]):not([type=file]), .lighthouse textarea, .lighthouse select { color: var(--lh-charcoal); background: var(--lh-surface-subtle); }
.composer-model { min-width: 0; display: flex; align-items: center; gap: 6px; }
.draft-files { display: flex; gap: 6px; overflow-x: auto; max-height: 100px; }
.draft-file { min-width: 0; flex: 0 0 220px; display: flex; align-items: center; gap: 8px; padding: 7px; border: 1px solid var(--lh-border); border-radius: 8px; }
.draft-file img { width: 42px; height: 42px; object-fit: cover; border-radius: 4px; flex-shrink: 0; }
.draft-file > span { min-width: 0; flex: 1; overflow-wrap: anywhere; font-size: 12px; }
.draft-file small { display: block; margin-top: 3px; color: var(--lh-muted); }
.message-files { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.message-files a { color: inherit; display: flex; gap: 6px; align-items: center; font-size: 12px; text-decoration: none; max-width: 100%; overflow-wrap: anywhere; }
.message-files img { width: 60px; height: 60px; object-fit: cover; border-radius: 6px; }
.operation-plan { border: 1px solid var(--lh-border-strong); border-radius: 8px; padding: 12px; margin-top: 12px; background: var(--lh-surface-subtle); font-size: 13px; }
.plan-title { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }
.plan-title strong { flex: 1; min-width: 0; overflow-wrap: anywhere; }
.plan-title span { color: var(--lh-accent); font-size: 12px; }
.turn .plan-description { color: var(--lh-muted); margin-top: 8px; }
.plan-steps { padding-left: 20px; margin: 12px 0; }
.plan-steps li + li { margin-top: 12px; }
.plan-steps dl { display: grid; grid-template-columns: minmax(60px, 100px) minmax(0, 1fr); gap: 5px 8px; margin: 8px 0; }
.plan-steps dt { color: var(--lh-muted); overflow-wrap: anywhere; }
.plan-steps dd { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
.plan-form { display: grid; gap: 10px; }
.plan-field { display: grid; gap: 6px; }
.plan-date { display: flex; align-items: center; gap: 6px; min-width: 0; }
.plan-date input { flex: 1; }
.plan-choices { margin: 0; padding: 8px; border: 1px solid var(--lh-input-border); border-radius: 6px; max-height: 240px; overflow: auto; }
.plan-choices label { display: flex; align-items: flex-start; gap: 8px; padding: 6px 0; overflow-wrap: anywhere; }
.plan-form .plan-choices input { width: auto; flex: none; margin-top: 3px; }
.choice-summary { display: flex; align-items: center; justify-content: space-between; color: var(--lh-muted); font-size: 12px; }
.option-search { display: flex; gap: 6px; }
.option-search input { flex: 1; min-width: 0; }
.option-search button { flex-shrink: 0; }
.plan-form b { color: var(--lh-warn); }
.plan-form input, .plan-form textarea, .plan-form select { border: 1px solid var(--lh-input-border); border-radius: 6px; padding: 8px; font: inherit; width: 100%; min-width: 0; }
.plan-form .checkbox-field { display: flex; align-items: center; gap: 10px; }
.plan-form .checkbox-field input { width: auto; }
.plan-actions { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; margin-top: 12px; }
.turn .plan-risk { margin-top: 10px; padding: 9px; color: var(--lh-warn); background: var(--lh-warn-soft); border-radius: 6px; }
.plan-results { display: grid; gap: 6px; margin-top: 10px; color: var(--lh-muted); }
.plan-results > div { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
.plan-results a { color: var(--lh-accent); }
.model-list { flex-shrink: 0; }
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0,0,0,0); }.spin { animation: rotate 1s linear infinite; }@keyframes rotate { to { transform: rotate(360deg); } }@media (prefers-reduced-motion: reduce) { .spin { animation: none; } }
.message-actions { display: flex; align-items: center; gap: 4px; margin-top: 6px; }
.message-actions .icon { width: 28px; height: 28px; min-height: 28px; color: var(--lh-muted); }
.message-actions .retry { margin-top: 0; }
.history-more { align-self: center; margin: 0 auto 12px; font-size: 12px; }
.new-content { align-self: center; gap: 5px; margin: 0 0 6px; font-size: 12px; min-height: 28px; }
.composer-footer .stop {
  flex: 0 0 auto;
  padding: 6px 11px;
  min-height: 34px;
  white-space: nowrap;
  color: #fff;
  background: var(--lh-danger-soft);
  border: 1px solid var(--lh-danger);
  border-radius: 8px;
  gap: 6px;
}
.composer-footer .stop:hover:not(:disabled) { background: var(--lh-danger); color: #30201c; }
.composer-footer .stop:disabled { color: var(--lh-charcoal); background: var(--lh-danger-soft); opacity: 1; }
.process { margin-top: 8px; font-size: 12px; color: var(--lh-muted); }
.process summary { display: flex; align-items: center; gap: 6px; cursor: pointer; user-select: none; min-height: 22px; }
.process summary .process-chevron { color: var(--lh-faint-muted); transition: transform .15s ease; }
.process[open] summary .process-chevron { transform: rotate(90deg); }
.process summary strong { color: var(--lh-charcoal-strong); font-size: 12px; }
.process-latest { color: var(--lh-accent); overflow-wrap: anywhere; }
.process-rows { margin: 8px 0 2px; padding-left: 18px; display: grid; gap: 5px; }
.process-rows li { position: relative; padding-left: 4px; color: var(--lh-muted); line-height: 1.6; }
.process-dot { position: absolute; left: -13px; top: 7px; width: 7px; height: 7px; border-radius: 50%; background: var(--lh-border-strong); }
.process-rows li.current .process-dot { background: var(--lh-accent); box-shadow: 0 0 0 3px var(--lh-accent-ring); }
.process-label { overflow-wrap: anywhere; }
@media (max-width: 600px) {
  .assistant-header { padding: 10px 12px; }
  .thread { padding: 14px 10px; }
  .settings-scroll { padding: 14px 14px 8px; }
  .settings-actions { padding: 10px 14px; }
  .assistant-launcher { padding: 11px 15px; }
  .model-select-small { max-width: none; flex: 1 1 auto; min-width: 0; }
  .composer-footer { gap: 8px; }
  .send { flex: 0 0 auto; }
  .model-card.compact-row { flex-wrap: wrap; align-items: stretch; gap: 6px 8px; }
  .model-card .list-row { flex-direction: column; align-items: flex-start; gap: 2px; min-width: 0; }
  .model-card .list-name, .model-card .list-model { max-width: 100%; overflow-wrap: anywhere; white-space: normal; }
  .model-actions { margin-left: auto; }
}
</style>
