<template>
  <Teleport :to="nativeModal || 'body'">
    <div ref="launcherLayer" class="lighthouse-launcher" :popover="nativeModal ? 'manual' : undefined">
      <button ref="launcher" class="assistant-launcher" :aria-label="open ? '收起灯塔助手' : '打开灯塔助手'" :aria-pressed="open" title="灯塔助手" @click="onLauncherClick">
        <LighthouseBot :appearance="shownAppearance" :position="botPosition" :mood="botMood" :motion-key="userId" interactive @moved="saveBotPosition" />
      </button>
    </div>
  </Teleport>
  <Teleport :to="panelHost || 'body'">
    <div ref="assistantLayer" class="lighthouse-layer" :popover="panelHost ? 'manual' : undefined">
    <aside
      ref="root"
      class="lighthouse"
      :style="{ '--bot-color': botColor, '--lh-panel-max-width': conversationWidthLimit ? conversationWidthLimit + 'px' : undefined, '--notice-panel-width': noticeWidth + 'px', '--notice-panel-height': (panelExpanded ? 820 : 700) + 'px' }"
      :class="{ resizing: panelResizing, 'drag-active': dragActive, 'is-open': open, 'notice-visible': noticeVisible }"
      @keydown.esc.stop="requestPanelClose"
      @dragover="onFileDragOver"
      @drop="onFileDrop"
    >
      <UiTransition name="ui-popover" @before-leave="pinClosingPanel" @before-enter="unpinPanel" @leave-cancelled="unpinPanel">
      <section v-if="open" class="assistant-shell" aria-label="灯塔助手" role="dialog" aria-labelledby="assistant-title">
      <div v-if="desktopNotices" id="assistant-notice-sidebar" class="assistant-sidebar" :class="{ expanded: noticeVisible }" :inert="!noticeVisible || undefined" :aria-hidden="!noticeVisible">
        <LighthouseNoticePanel :key="userId" :user-id="userId" :active="noticeDockOpen" @availability="noticeAvailable = $event" @edition="onNoticeEdition" />
      </div>
      <section class="assistant-panel" :class="{ expanded: panelExpanded }" aria-label="助手会话">
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
            <button v-if="desktopNotices" class="icon sidebar-toggle" :title="noticeVisible ? '收起通告待办' : '展开通告待办'" aria-label="通告待办" :aria-expanded="noticeVisible" aria-controls="assistant-notice-sidebar" @click="noticeDockOpen = !noticeVisible; noticeUserOpened = true"><PanelLeftClose v-if="noticeVisible" :size="18" /><PanelLeftOpen v-else :size="18" /></button>
            <span class="assistant-mark"><Bot :size="20" /></span><div class="panel-heading"><h2 id="assistant-title">灯塔助手</h2>
            <span class="header-status" :class="{ active: loading || busy, warning: !loading && !!error }" role="status">{{ headerStatus }}</span></div>
          </div>
          <div class="tools">
            <button class="icon" :title="panelExpanded ? '还原会话' : '展开会话'" :aria-label="panelExpanded ? '还原会话' : '展开会话'" :aria-expanded="panelExpanded" @click="panelExpanded = !panelExpanded"><Minimize2 v-if="panelExpanded" :size="17" /><Maximize2 v-else :size="17" /></button>
            <button class="icon" :disabled="appearanceLoading" title="图标设置" aria-label="图标设置" :aria-pressed="appearanceOpen" @click="showAppearanceSettings"><Palette :size="17" /></button>
            <button class="icon" :disabled="settingsOpen" title="技能与工具" aria-label="技能与工具" :aria-pressed="skillsOpen" @click="skillsOpen = !skillsOpen"><BookOpen :size="17" /></button>
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
                <button type="button" class="list-row" :disabled="busy || settingBusy" @click="canEditModel(m) ? beginEditAction({ kind: 'edit', id: String(m.id) }) : setDefault(m)">
                  <span class="list-name">{{ m.name }}</span>
                  <span class="list-model">{{ m.model }}</span>
                  <span v-if="m.shared" class="model-flag">共享默认</span>
                  <span v-if="isDefault(m)" class="model-flag">默认</span>
                  <span v-else-if="!m.configured" class="model-flag warn">未配置</span>
                </button>
                <div class="model-actions">
                  <button
                    v-if="canEditModel(m)"
                    type="button"
                    class="icon-tool"
                    :disabled="busy || settingBusy"
                    title="编辑模型"
                    aria-label="编辑"
                    @click="beginEditAction({ kind: 'edit', id: String(m.id) })"
                  ><Pencil :size="14" /></button>
                  <button
                    type="button"
                    class="icon-tool"
                    :disabled="busy || settingBusy || isDefault(m)"
                    :title="isDefault(m) ? '已经是默认模型' : '设为默认'"
                    :aria-label="isDefault(m) ? '已经是默认模型' : '设为默认'"
                    @click="setDefault(m)"
                  ><Star :size="14" /></button>
                  <button
                    v-if="canEditModel(m)"
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

            <button type="button" class="add-model" :disabled="busy || settingBusy" @click="addStart(false)"><Plus :size="15" />添加模型</button>
            <button v-if="modelSettings.can_manage_shared" type="button" class="add-model" :disabled="busy || settingBusy" @click="addStart(true)"><Plus :size="15" />添加默认模型</button>

            <section v-if="editProfile" class="profile-form">
              <div class="settings-section-label">{{ editingExisting ? '编辑' : '添加' }}{{ editProfile.shared ? '共享默认模型' : '个人模型' }}</div>
              <label class="field-label">显示名称<input v-model="editProfile.name" type="text" maxlength="60" :disabled="busy || settingBusy" /></label>
              <label class="field-label">接口地址<input v-model="editProfile.endpoint" type="text" maxlength="2000" :disabled="busy || settingBusy" placeholder="https://…/v1/chat/completions" /></label>
              <label class="field-label">模型名称<input v-model="editProfile.model" type="text" maxlength="200" :disabled="busy || settingBusy" /></label>
              <label class="field-label">API Key<input v-model="editProfile.api_key" type="password" autocomplete="new-password" :disabled="busy || settingBusy" :placeholder="editingExisting ? '留空保留原凭证' : '请输入 API Key'" maxlength="500" spellcheck="false" @copy.prevent @cut.prevent @contextmenu.prevent @dragstart.prevent /></label>
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

        <LighthouseSkills v-else-if="skillsOpen" @close="skillsOpen = false" @use="chooseManagedSkill" @changed="commandsMenu?.refresh()" />
        <template v-else>
          <nav v-if="activePlans.length" class="active-tasks" aria-label="办理中的任务"><span>办理中 {{ activePlans.length }}</span><button v-for="plan in activePlans" :key="plan.id" type="button" :title="plan.title" @click="locatePlan(plan.id)"><Loader2 :size="13" class="spin" /><span>{{ plan.title }}</span></button></nav>
          <div ref="thread" class="thread" :aria-busy="loading" @scroll="onThreadScroll">
            <button v-if="state.turns?.length >= 30 && hasOlder" class="history-more" :disabled="historyLoading" @click="loadHistory"><LoadingIndicator v-if="historyLoading">读取中…</LoadingIndicator><template v-else>更早的消息</template></button>
            <div v-if="loading" class="empty"><Loader2 :size="22" class="spin" /><p>正在读取会话…</p></div>
            <div v-else-if="!state.turns?.length" class="empty"><Bot :size="30" /><p>你好，{{ userName || '欢迎' }}</p><p class="muted">今天有什么需要处理的？</p></div>
            <article v-for="turn in state.turns || []" :key="turn.operation_id" class="turn" :class="{ 'is-pending': turn.status === 'pending' }">
              <div class="message-row user">
                <div class="bubble user-bubble"><div v-if="turn.commands?.length" class="turn-commands"><span v-for="command in turn.commands" :key="command.kind + command.id"><BookOpen v-if="command.kind === 'skill'" :size="12" /><Wrench v-else :size="12" />{{ command.label }}</span></div><p>{{ turn.question }}</p><div v-if="turn.attachments?.length" class="message-files"><a v-for="file in turn.attachments" :key="file.id" :href="safeAttachmentUrl(file.url)" target="_blank" rel="noopener"><img v-if="file.is_image" :src="safeAttachmentUrl(file.url)" :alt="file.name" /><FileText v-else :size="20" /><span>{{ file.name }}</span></a></div></div>
              </div>
              <div class="message-row assistant">
                <div class="answer">
                  <div class="answer-heading"><Bot :size="15" aria-hidden="true" /><span>灯塔助手</span><span v-if="turn.status === 'pending'" class="answer-state"><Loader2 :size="12" class="spin" />回答中</span></div>
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
                  <LighthouseReply v-if="turn.answer && turn.plan?.status !== 'failed'" :text="turn.answer" />
                  <p v-else-if="turn.status === 'pending' && !(turn.process || []).length" class="pending"><Loader2 :size="15" class="spin" />{{ pendingText(turn) }}</p>
                  <p v-else-if="turn.status === 'superseded'" class="muted">已结合补充信息继续处理</p>
                  <p v-else-if="turn.status !== 'pending' && !turn.plan" class="failure">{{ turn.error || '上次回答已中断，可重试。' }}</p>
                  <p v-if="turn.status === 'pending' && turn.answer && !(turn.process || []).length" class="pending"><Loader2 :size="14" class="spin" />{{ pendingText(turn) }}</p>
                  <p v-if="turn.answer && ['stopped', 'failed'].includes(turn.status)" class="muted">{{ turn.error || '回答未完成' }}</p>
                  <div v-if="turn.output_files?.length" class="message-files output-files"><a v-for="file in turn.output_files" :key="file.id" :href="safeAttachmentUrl(file.url)" target="_blank" rel="noopener"><FileText :size="18" /><span>{{ file.name }}</span><Download :size="15" /></a></div>
                  <LighthouseDownloads :items="turn.downloads" />
                  <section v-if="turn.plan" class="operation-plan" :data-plan-id="turn.plan.id" :data-plan-status="turn.plan.status">
                    <div class="plan-title"><ListChecks :size="16" /><strong>{{ turn.plan.title }}</strong><span>{{ planStatusText(turn.plan) }}</span><button v-if="planIsFinished(turn.plan)" class="icon" type="button" :aria-expanded="!!expandedPlans[turn.plan.id]" :title="expandedPlans[turn.plan.id] ? '收起办理详情' : '展开办理详情'" :aria-label="expandedPlans[turn.plan.id] ? '收起办理详情' : '展开办理详情'" @click="expandedPlans[turn.plan.id] = !expandedPlans[turn.plan.id]"><ChevronRight :size="16" :class="{ 'rotate-chevron': expandedPlans[turn.plan.id] }" /></button></div>
                    <div v-if="!planIsFinished(turn.plan) || expandedPlans[turn.plan.id]" class="plan-body">
                    <p v-if="turn.plan.explanation" class="plan-description">{{ turn.plan.explanation }}</p>
                    <div v-if="turn.plan.planned_notice?.selected" class="planned-notice-selected">
                      <strong>{{ turn.plan.planned_notice.selected.title }}</strong>
                      <span>{{ turn.plan.planned_notice.selected.building }} · {{ turn.plan.planned_notice.month }}</span>
                      <template v-if="['needs_input', 'awaiting_confirmation', 'awaiting_second_confirmation'].includes(turn.plan.status)">
                        <button v-if="!plannedReset[turn.plan.id]" type="button" :disabled="isPlanBusy(turn.plan)" @click="plannedReset[turn.plan.id] = true"><RotateCcw :size="14" />重新选择计划</button>
                        <div v-else role="alert"><p>更换计划将清除当前填写，是否继续？</p><button type="button" @click="plannedReset[turn.plan.id] = false">保留填写</button><button type="button" :disabled="isPlanBusy(turn.plan)" @click="resetPlannedNotice(turn)">清除并重新选择</button></div>
                      </template>
                    </div>
                    <ol class="plan-steps"><li v-for="(op, i) in turn.plan.operations || []" :key="i"><strong>{{ op.name || '业务操作' }}</strong><dl><template v-for="item in operationPreview(op, turn.plan.status === 'needs_input' && (turn.plan.fields || []).some((field: Dict) => (field.operation_index || 0) === i))" :key="item.label"><dt>{{ item.label }}</dt><dd>{{ item.value }}</dd></template></dl></li></ol>
                    <form v-if="turn.plan.status === 'needs_input'" class="plan-form" @submit.prevent="amendPlan(turn)">
                      <div v-for="field in visiblePlanFields(turn.plan)" :key="turn.plan.id + ':' + field.name" class="plan-field" :class="{ 'checkbox-field': field.type === 'checkbox', 'wide-field': ['object', 'array', 'textarea', 'multiselect', 'file'].includes(field.type) }"><component :is="planFieldIsGroup(field) ? 'span' : 'label'" :for="planFieldIsGroup(field) ? undefined : planFieldId(turn.plan, field)">{{ field.label }}<b v-if="field.required" aria-hidden="true"> *</b></component>
                        <p v-if="field.question_text" class="plan-question">{{ field.question_text }}</p>
                        <div v-if="field.options_source && !field.native_cabinet_correct && !field.native_notice_sop" class="option-search"><input v-if="!field.native_drill" v-model="planValues[turn.plan.id + ':search:' + field.name]" type="search" maxlength="120" placeholder="按名称查找" aria-label="查找可选记录" /><button type="button" :disabled="isPlanBusy(turn.plan)" @click="loadPlanOptions(turn, field)"><Search :size="15" />{{ field.native_drill ? '读取人员目录' : '查找' }}</button></div>
                        <small v-if="field.options_total > 40 || field.options_has_more" class="muted">候选较多，可按名称缩小范围。</small>
                        <small v-if="field.options_warning" class="muted">{{ field.options_warning }}</small>
                        <div v-if="(field.native_repair_prefill || field.native_notice_prefill) && repairPrefills[turn.plan.id + ':' + field.name]" class="repair-prefill-state" role="status">
                          <span v-if="repairPrefills[turn.plan.id + ':' + field.name].loading">正在读取关联资料…</span>
                          <template v-else-if="repairPrefills[turn.plan.id + ':' + field.name].error"><span>{{ repairPrefills[turn.plan.id + ':' + field.name].error }}</span><button type="button" :disabled="isPlanBusy(turn.plan)" @click="field.native_notice_prefill ? prefillNotice(turn.plan, field) : prefillRepair(turn.plan, field)"><RefreshCw :size="14" />重新读取关联资料</button></template>
                          <span v-else-if="repairPrefills[turn.plan.id + ':' + field.name].warning">{{ repairPrefills[turn.plan.id + ':' + field.name].warning }}</span>
                        </div>
                        <LighthouseStructuredField v-if="['object', 'array'].includes(field.type)" :field="planControl(turn.plan, field)" :id="planFieldId(turn.plan, field)" :plan-id="turn.plan.id" :plan-version="turn.plan.version" v-model="planValues[turn.plan.id + ':' + field.name]" :disabled="isPlanBusy(turn.plan)" @load-options="loadPlanOptions(turn, field, $event.scope, $event.q)" />
                        <LighthousePlannedChoices v-else-if="field.native_planned_choice && field.type === 'select'" :field="field" :id="planFieldId(turn.plan, field)" v-model="planValues[turn.plan.id + ':' + field.name]" :disabled="isPlanBusy(turn.plan)" />
                        <textarea v-else-if="field.type === 'textarea'" :id="planFieldId(turn.plan, field)" :aria-label="field.label" v-model="planValues[turn.plan.id + ':' + field.name]" :disabled="isPlanBusy(turn.plan)" :required="field.required" :maxlength="field.maxlength" rows="3" />
                        <fieldset v-else-if="field.type === 'select' && !field.options_source && smallSingleChoice(field.options)" :id="planFieldId(turn.plan, field)" class="plan-single-choices" :disabled="isPlanBusy(turn.plan)" :aria-label="field.label"><label v-for="option in field.options" :key="String(option.value)" :class="{ selected: planValues[turn.plan.id + ':' + field.name] === option.value }"><input type="radio" :name="planFieldId(turn.plan, field)" :value="option.value" :checked="planValues[turn.plan.id + ':' + field.name] === option.value" @change="choosePlanOption(turn.plan, field, option.value)" /><span>{{ option.label }}</span></label></fieldset>
                        <VnetSelect v-else-if="field.type === 'select'" :input-id="planFieldId(turn.plan, field)" :label="field.label" :model-value="selectedLabel(field.options || [], planValues[turn.plan.id + ':' + field.name])" :options="labelledOptions(field.options || []).filter(option => !option.disabled).map(option => option.label)" :disabled="isPlanBusy(turn.plan)" :required="!!field.required" :menu-z-index="10010" @update:model-value="choosePlanOption(turn.plan, field, selectedValue(field.options || [], $event))" />
                        <fieldset v-else-if="field.type === 'multiselect'" :id="planFieldId(turn.plan, field)" class="plan-choices" :disabled="isPlanBusy(turn.plan)"><legend class="sr-only">{{ field.label }}</legend><label v-for="option in field.options || []" :key="String(option.value)"><input type="checkbox" :checked="planChoices(turn.plan, field).includes(option.value)" :disabled="field.maxItems && planChoices(turn.plan, field).length >= field.maxItems && !planChoices(turn.plan, field).includes(option.value)" @change="togglePlanChoice(turn.plan, field, option.value, ($event.target as HTMLInputElement).checked)" /><span>{{ option.label }}</span></label><span v-if="!(field.options || []).length" class="muted">{{ field.options_source ? '候选待读取' : '暂无可选项' }}</span><div class="choice-summary"><span>已选 {{ planChoices(turn.plan, field).length }}{{ field.maxItems ? ' / ' + field.maxItems : '' }}</span><button v-if="planChoices(turn.plan, field).length" type="button" class="icon" title="清空选择" :aria-label="'清空' + field.label" @click="clearPlanChoice(turn.plan, field)"><X :size="14" /></button></div></fieldset>
                        <div v-else-if="['date', 'time', 'month', 'datetime-local'].includes(field.type)" class="plan-date"><input :id="planFieldId(turn.plan, field)" :aria-label="field.label" v-model="planValues[turn.plan.id + ':' + field.name]" :type="field.type" :disabled="isPlanBusy(turn.plan)" :required="field.required" :step="field.step" :min="field.min" :max="field.max" @change="planSelectionChanged(turn.plan, field)" /><button type="button" class="icon" :disabled="isPlanBusy(turn.plan)" :title="'选择' + field.label" :aria-label="'选择' + field.label" @click="openPlanPicker($event)"><Clock3 v-if="field.type === 'time'" :size="17" /><CalendarDays v-else :size="17" /></button></div>
                        <div v-else-if="field.type === 'file'" class="plan-upload" :tabindex="field.native_water_photos ? 0 : undefined" :aria-label="field.label + '上传区域'" @paste="pastePlanFiles($event, turn.plan, field)" @dragover="onFileDragOver" @drop="dropPlanFiles($event, turn.plan, field)">
                          <input class="sr-only" :id="planFieldId(turn.plan, field)" :aria-label="field.label" type="file" :accept="field.accept" :multiple="field.maxItems !== 1" :disabled="isPlanBusy(turn.plan)" @change="uploadPlanFiles($event, turn.plan, field)" />
                          <button type="button" :disabled="isPlanBusy(turn.plan)" @click="($event.currentTarget as HTMLElement).parentElement?.querySelector('input')?.click()"><Paperclip :size="15" />{{ field.maxItems === 1 && planChoices(turn.plan, field).length ? '替换附件' : '添加附件' }}</button>
                          <span v-if="planUploadProgress[turn.plan.id]?.key === turn.plan.id + ':' + field.name" class="muted" role="status">正在上传 {{ planUploadProgress[turn.plan.id]?.name }} · {{ planUploadProgress[turn.plan.id]?.done }}/{{ planUploadProgress[turn.plan.id]?.total }}</span>
                          <ul v-if="planChoices(turn.plan, field).length" class="plan-files" :aria-label="field.label + '已选文件'">
                            <li v-for="file in selectedPlanFiles(turn.plan, field)" :key="file.id" class="plan-file">
                              <a v-if="safeAttachmentUrl(file.url)" :href="safeAttachmentUrl(file.url)" target="_blank" rel="noopener"><img v-if="file.is_image" :src="safeAttachmentUrl(file.url)" alt="" loading="lazy" /><FileText v-else :size="18" /><span>{{ file.name }}</span></a>
                              <span v-else>{{ file.name }}</span>
                              <button type="button" class="icon" :disabled="isPlanBusy(turn.plan)" :title="'移除' + file.name" :aria-label="'移除' + file.name" @click="planValues[turn.plan.id + ':' + field.name] = planChoices(turn.plan, field).filter(id => id !== file.id)"><X :size="15" /></button>
                            </li>
                          </ul>
                        </div>
                        <input v-else-if="field.type === 'checkbox'" :id="planFieldId(turn.plan, field)" :aria-label="field.label" v-model="planValues[turn.plan.id + ':' + field.name]" type="checkbox" :disabled="isPlanBusy(turn.plan)" />
                        <input v-else :id="planFieldId(turn.plan, field)" :aria-label="field.label" v-model="planValues[turn.plan.id + ':' + field.name]" :type="field.type || 'text'" :disabled="isPlanBusy(turn.plan)" :required="field.required" :min="field.min" :max="field.max" :step="field.step" :maxlength="field.maxlength" />
                      </div>
                      <footer class="plan-form-footer"><p v-if="planErrors[turn.plan.id]" class="failure" role="alert">{{ planErrors[turn.plan.id] }}</p><button type="button" :disabled="isPlanBusy(turn.plan)" @click="cancelPlan(turn)">取消操作</button><button class="primary" :disabled="isPlanBusy(turn.plan) || planInputIncomplete(turn.plan)"><Loader2 v-if="isPlanBusy(turn.plan)" :size="15" class="spin" />补充并继续</button></footer>
                    </form>
                    <section v-if="turn.plan.planned_notice?.text && turn.plan.status !== 'needs_input'" class="planned-notice-text"><h4>通告全文</h4><pre>{{ turn.plan.planned_notice.text }}</pre></section>
                    <p v-if="turn.plan.status === 'awaiting_second_confirmation'" class="plan-risk">此操作涉及正式提交、删除或覆盖。请再次确认操作清单和目标。</p>
                    <p v-if="turn.plan.error && !turn.plan.results?.some((result: Dict) => !result.ok && result.error === turn.plan.error)" class="failure">{{ turn.plan.error }}</p>
                    <p v-if="turn.plan.status !== 'needs_input' && planErrors[turn.plan.id]" class="failure" role="alert">{{ planErrors[turn.plan.id] }}</p>
                    <div v-if="turn.plan.can_retry" class="plan-actions"><button type="button" class="primary" :disabled="isPlanBusy(turn.plan)" @click="retryPlan(turn)"><Loader2 v-if="isPlanBusy(turn.plan)" :size="15" class="spin" /><RotateCcw v-else :size="15" />重试原任务</button></div>
                    <div v-if="['awaiting_confirmation', 'awaiting_second_confirmation'].includes(turn.plan.status)" class="plan-actions"><button type="button" :disabled="isPlanBusy(turn.plan)" @click="cancelPlan(turn)">取消操作</button><button v-if="turn.plan.can_edit" type="button" :disabled="isPlanBusy(turn.plan)" @click="editPlan(turn)"><Pencil :size="15" />返回修改</button><button type="button" class="primary" :disabled="isPlanBusy(turn.plan)" @click="confirmPlan(turn)">{{ turn.plan.status === 'awaiting_second_confirmation' ? '再次确认并执行' : '确认操作清单' }}</button></div>
                    <div v-if="turn.plan.results?.length" class="plan-results"><div v-for="(result, i) in turn.plan.results" :key="i"><Loader2 v-if="result.ok && ['running','submitted'].includes(turn.plan.status)" :size="15" class="spin" /><Check v-else-if="result.ok" :size="15" /><AlertCircle v-else :size="15" /><span>{{ resultStatusText(result, turn.plan) }}</span><a v-if="safeAttachmentUrl(result.data?.url)" :href="safeAttachmentUrl(result.data.url)" target="_blank" rel="noopener">{{ result.data.name || '下载文件' }}</a><a v-if="!result.downloads?.length && morningDownloadUrl(result)" :href="morningDownloadUrl(result)" target="_blank" rel="noopener">下载晨会表格</a><LighthouseDownloads :items="result.downloads" /><LighthouseReply v-if="result.query_reply" :text="result.query_reply" /></div></div>
                    </div>
                  </section>
                  <div v-if="turnIsComplete(turn) && visibleInteractions(turn).length" class="interactions">
                    <button v-for="interaction in visibleInteractions(turn)" :key="interaction.url" type="button" class="interaction" :title="interaction.title" @click="runInteraction(interaction)">
                      <ArrowUpRight :size="14" />{{ interaction.label }}
                    </button>
                  </div>
                  <details v-if="turn.sources?.length" class="sources"><summary>参考资料 {{ turn.sources.length }}</summary><a v-for="source in turn.sources" :key="source.number" :href="safeSourceLink(source.url) || undefined" :target="isSafeLocalPath(String(source.url || '')) ? undefined : '_blank'" rel="noopener noreferrer" @click="openSource($event, source.url)">[{{ source.number }}] {{ source.title }}<small v-if="source.queried_at">{{ source.queried_at.replace('T', ' ').replace('+08:00', '') }}</small></a></details>
                  <p v-for="warning in turn.warnings || []" :key="warning" class="source-warning">{{ warning }}</p>
                  <div class="message-actions">
                    <button v-if="turn.answer" type="button" class="icon" title="复制回答" aria-label="复制回答" @click="copyAnswer(turn)"><Copy :size="15" /></button>
                    <button type="button" class="icon" title="编辑后重新提问" aria-label="编辑后重新提问" @click="editQuestion(turn)"><Pencil :size="15" /></button>
                    <button v-if="['failed', 'stopped'].includes(turn.status) && !sending" type="button" class="retry" @click="send(turn)"><RotateCcw :size="14" />{{ turn.status === 'stopped' ? '继续回答' : '重试' }}</button>
                    <button v-if="turn.status === 'completed' && !turn.plan && !state.busy" type="button" class="icon" title="重新生成回答" aria-label="重新生成回答" @click="send(turn, true)"><RefreshCw :size="15" /></button>
                    <span v-if="turn.model_name && turn.answer" class="used-model" :title="'由 ' + turn.model_name + ' 回答'">{{ turn.model_name }}</span>
                  </div>
                </div>
              </div>
            </article>
          </div>
          <button v-if="hasNewContent" type="button" class="new-content" @click="scrollBottom()"><ArrowDown :size="14" />新消息</button>
          <div v-if="error" class="assistant-error" role="alert"><span>{{ error }}</span><button v-if="!sending" class="icon" title="重新读取会话" aria-label="重新读取会话" @click="readState(true, true)"><RefreshCw :size="16" /></button></div>
          <p v-if="draftWarning" class="assistant-error" role="status">{{ draftWarning }}</p>
          <div v-if="!loading && (!state.configured || !state.enabled)" class="config-warning">{{ state.enabled === false ? '助手暂未启用' : '请在模型设置中配置你的模型' }}</div>

          <form ref="composerElement" class="composer" @submit.prevent="send()">
            <LighthouseCommands ref="commandsMenu" v-model="selectedCommands" v-model:draft="draft" @manage="skillsOpen = true" @focus="focusComposer" />
            <label class="sr-only" for="assistant-question">询问灯塔助手</label>
            <div v-if="draftFiles.length" class="draft-files"><div v-for="file in draftFiles" :key="file.localId" class="draft-file"><img v-if="file.preview" :src="file.preview" :alt="file.name" /><FileText v-else :size="22" /><span>{{ file.name }}<small>{{ file.uploading ? '读取中…' : file.error || '已就绪' }}</small></span><button type="button" class="icon" :disabled="file.uploading" :aria-label="'移除附件 ' + file.name" @click="removeDraftFile(file.localId)"><X :size="14" /></button></div></div>
            <textarea id="assistant-question" ref="input" v-model="draft" :disabled="loading || !state.configured || !state.enabled" rows="2" maxlength="12000" :placeholder="state.busy ? '可以继续补充信息…' : '询问信息，或描述要办理的事项'" @paste="onComposerPaste" @keydown="composerKeydown" :aria-expanded="!!commandsMenu?.visible" aria-controls="assistant-commands" :aria-activedescendant="commandsMenu?.visible ? 'assistant-command-' + commandsMenu.active : undefined" />
            <div class="composer-footer">
              <div class="composer-model"><button type="button" class="icon" title="添加图片或文件" aria-label="添加图片或文件" :disabled="sending || uploading" @click="attachmentInput?.click()"><Paperclip :size="18" /></button><input ref="attachmentInput" class="sr-only" type="file" multiple accept=".png,.jpg,.jpeg,.webp,.gif,.bmp,.pdf,.txt,.md,.csv,.log,.json,.xml,.docx,.xlsx,.xlsm" @change="onAttachmentChange" />
              <button type="button" class="icon" title="选择技能或工具" aria-label="选择技能或工具" :disabled="sending || loading" @click="insertCommand"><Slash :size="17" /></button>
              <div v-if="modelNames.length" class="model-select-small">
                <label class="sr-only" for="assistant-model">选择模型</label>
                <select id="assistant-model" class="native-model-select" :value="state.model_name || ''" :title="state.model_name || '选择模型'" :disabled="sending || selecting" @change="switchModel(($event.target as HTMLSelectElement).value)"><option v-for="name in modelNames" :key="name" :value="name">{{ name }}</option></select>
                <ChevronDown class="model-chevron" :size="14" aria-hidden="true" />
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
      </section>
      </UiTransition>

      <ConfirmDialog :open="clearOpen" title="清空会话" message="清空聊天记录并丢弃未发送或失败的操作？清空后旧操作不可重试，已写入的业务数据不会删除。" tone="danger" @resolve="clearConversation" />
      <ConfirmDialog :open="deleteModelOpen" title="删除模型" :message="deleteMessage" tone="danger" @resolve="confirmDeleteModel" />
      <ConfirmDialog :open="editSwitchOpen" title="切换编辑模型" message="当前编辑有未保存的内容，切换后这些修改将被丢弃。" tone="warning" @resolve="confirmEditSwitch" />
      <ConfirmDialog :open="settingsExitOpen" title="返回会话" message="当前编辑有未保存的内容，返回后这些修改将被丢弃。" tone="warning" @resolve="confirmSettingsExit" />
      <ConfirmDialog :open="panelExitOpen" title="收起助手" message="当前编辑有未保存的内容，收起后这些修改将被丢弃。" tone="warning" @resolve="confirmPanelExit" />
    </aside>
    <LighthouseBotSettings :open="appearanceOpen" :appearance="appearance" :target="assistantLayer || 'body'" @close="appearanceOpen = false" @saved="saveAppearance" @preview="previewAppearance = $event" />
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, reactive, ref, shallowRef, watch } from 'vue';
import type { Chat } from '@ai-sdk/vue';
import type { UIMessage } from 'ai';
import { AlertCircle, ArrowDown, ArrowUp, ArrowUpRight, BookOpen, Bot, CalendarDays, Check, ChevronDown, ChevronRight, Clock3, Copy, Download, FileText, ListChecks, Loader2, Maximize2, Minimize2, Palette, PanelLeftClose, PanelLeftOpen, Paperclip, Pencil, Plus, RefreshCw, RotateCcw, Search, Settings, Slash, Square, Star, Trash2, Wrench, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { repairDeviceDependentPatch, repairDraftInputValue } from '../repairManagementUtils';
import { navigate } from '../navigation';
import ConfirmDialog from './ConfirmDialog.vue';
import LighthouseReply from './LighthouseReply.vue';
import LighthouseDownloads from './LighthouseDownloads.vue';
import LoadingIndicator from './LoadingIndicator.vue';
import AsyncPageState from './AsyncPageState.vue';
import LighthouseBot from './LighthouseBot.vue';
import LighthouseBotSettings from './LighthouseBotSettings.vue';
import LighthouseCommands from './LighthouseCommands.vue';
import LighthousePlannedChoices from './LighthousePlannedChoices.vue';
import VnetSelect from './VnetSelect.vue';
import { labelledOptions, selectedLabel, selectedValue, smallSingleChoice } from '../lighthouseSelect';
import { readAssistantDraft, writeAssistantDraft, planDraftValues } from '../lighthouseDrafts';
const LighthouseSkills = defineAsyncComponent({ loader: () => import('./LighthouseSkills.vue'), loadingComponent: LoadingIndicator, delay: 0 });
const LighthouseNoticePanel = defineAsyncComponent(() => import('./LighthouseNoticePanel.vue'));
import { BOT_COLORS, normalizeBot, type BotAppearance, type BotMood } from '../botAppearance';
const structuredFieldsReady = ref(false);
const LighthouseStructuredField = defineAsyncComponent({
  loader: () => import('./LighthouseStructuredField.vue').then(module => { structuredFieldsReady.value = true; return module; }),
  loadingComponent: LoadingIndicator,
  errorComponent: AsyncPageState,
  delay: 0,
});

const props = withDefaults(defineProps<{ userName: string; userId?: string }>(), { userName: '', userId: '' });
const appearance = ref(normalizeBot(readAppearanceCache())), appearanceOpen = ref(false), appearanceLoading = ref(false);
const previewAppearance = ref({ ...appearance.value });
const shownAppearance = computed(() => appearanceOpen.value ? { ...previewAppearance.value, snap_back: appearance.value.snap_back } : appearance.value);
const botColor = computed(() => BOT_COLORS.find(([id]) => id === shownAppearance.value.color)?.[2] || '#0a0a0c');
function showAppearanceSettings(): void { previewAppearance.value = { ...appearance.value }; appearanceOpen.value = true; }
const botPosition = ref<{ x: number; y: number } | null>(null);
let appearanceController: AbortController | undefined;
function appearanceKey(): string { return 'lighthouse_appearance:' + props.userId; }
function readAppearanceCache(): unknown {
  if (!props.userId) return null;
  try { return JSON.parse(window.localStorage.getItem(appearanceKey()) || 'null'); } catch { return null; }
}
function saveBotPosition(position: { x: number; y: number }): void {
  botPosition.value = position;
  saveStoredPos('launcher', position.x, position.y);
}
function saveAppearance(value: BotAppearance, resetPosition = true): void {
  appearanceController?.abort(); appearanceController = undefined; appearanceLoading.value = false;
  if (resetPosition && appearance.value.snap_back !== value.snap_back) {
    botPosition.value = null;
    try { window.localStorage.removeItem(shapeKey('launcher')); } catch { /* private mode */ }
  }
  appearance.value = value;
  if (props.userId) try { window.localStorage.setItem(appearanceKey(), JSON.stringify(value)); } catch { /* private mode */ }
}
async function loadAppearance(): Promise<void> {
  if (!props.userId) return;
  const controller = new AbortController(); appearanceController = controller; appearanceLoading.value = true;
  try {
    const response = await call('appearance', 'GET', undefined, 12000, controller.signal);
    if (!disposed && appearanceController === controller) saveAppearance(normalizeBot(response), false);
  } catch { /* The cached icon remains usable while the service is unavailable. */ }
  finally { if (appearanceController === controller) { appearanceController = undefined; appearanceLoading.value = false; } }
}

type EditProfile = { id: string; name: string; endpoint: string; model: string; api_key: string; shared: boolean };
type EditAction = { kind: 'edit'; id: string } | { kind: 'add'; shared?: boolean };
type TurnInteraction = { kind: 'navigate'; label: string; url: string; title: string };
type DraftFile = { localId: string; id?: string; name: string; mime?: string; size?: number; url?: string; preview?: string; uploading: boolean; error: string; is_image?: boolean };

const open = ref(false), settingsOpen = ref(false), loading = ref(false), sending = ref(false), selecting = ref(false), settingBusy = ref(false), settingsLoading = ref(false), savingProfile = ref(false);
const state = ref<Dict>({ turns: [], can_manage_settings: false }), modelSettings = ref<Dict>({ models: [], enabled: false, active_model_id: '' });
const draft = ref(''), error = ref(''), settingsError = ref('');
const skillsOpen = ref(false), selectedCommands = ref<Dict[]>([]), commandsMenu = ref<InstanceType<typeof LighthouseCommands> | null>(null);
const draftFiles = ref<DraftFile[]>([]), attachmentInput = ref<HTMLInputElement | null>(null);
const planBusy = reactive<Record<string, boolean>>({}), planErrors = reactive<Record<string, string>>({});
const planOptionStates = reactive<Record<string, { loading: boolean; error: string }>>({});
function isPlanBusy(plan: Dict): boolean { return !!planBusy[plan.id]; }
const uploading = computed(() => draftFiles.value.some(file => file.uploading));
const stopping = ref(false), hasNewContent = ref(false), historyLoading = ref(false), hasOlder = ref(true);
const streamChat = shallowRef<Chat<UIMessage> | null>(null);
let streamRun = '', streamSerial = 0, renderTimer = 0, lastStreamUpdateAt = 0;
const planValues = reactive<Dict>({});
const plannedReset = reactive<Record<string, boolean>>({});
const expandedPlans = reactive<Record<string, boolean>>({});
const activePlans = computed<Dict[]>(() => (state.value.turns || []).map((turn: Dict) => turn.plan).filter((plan: Dict) => plan && ['running', 'submitted'].includes(plan.status)));
function planIsFinished(plan: Dict): boolean { return ['completed', 'cancelled', 'superseded'].includes(plan.status); }
function locatePlan(id: string): void {
  const element = Array.from(thread.value?.querySelectorAll<HTMLElement>('[data-plan-id]') || []).find(item => item.dataset.planId === id);
  element?.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth', block: 'start' });
}
const repairPrefills = reactive<Dict>({});
const planFiles = reactive<Dict>({});
const planUploadProgress = reactive<Dict>({});
let draftConversation = '', draftTimer = 0;
const draftWarning = ref('');
function restoreDrafts(): void {
  const conversation = String(state.value.conversation_id || '');
  if (!conversation || conversation === draftConversation) return;
  draftConversation = conversation;
  let saved;
  try { saved = readAssistantDraft(window.sessionStorage, props.userId, conversation); } catch { return; }
  if (!saved) return;
  if (!draft.value && !draftFiles.value.length && !selectedCommands.value.length) {
    draft.value = saved.text;
    selectedCommands.value = saved.commands;
    draftFiles.value = saved.files.filter((file: Dict) => file.id && file.name).map((file: Dict) => ({ ...file, localId: file.id, uploading: false, error: '' } as DraftFile));
  }
  for (const turn of state.value.turns || []) {
    if (!turn.plan) continue;
    const values = planDraftValues(saved, turn.plan);
    if (!values) continue;
    for (const [name, value] of Object.entries(values)) planValues[turn.plan.id + ':' + name] = value;
    for (const file of saved.plans[turn.plan.id].files || []) if (file.id && file.name) planFiles[file.id] = file;
  }
}
function saveDrafts(): void {
  window.clearTimeout(draftTimer);
  if (!draftConversation || draftConversation !== state.value.conversation_id) return;
  const plans: Dict = {};
  const fileReference = (file: Dict) => ({ id: file.id, name: file.name, mime: file.mime, size: file.size, url: file.url, is_image: file.is_image });
  for (const turn of state.value.turns || []) {
    const plan = turn.plan;
    if (plan?.status !== 'needs_input') continue;
    const values = Object.fromEntries((plan.fields || []).filter((field: Dict) => Object.prototype.hasOwnProperty.call(planValues, plan.id + ':' + field.name)).map((field: Dict) => [field.name, planValues[plan.id + ':' + field.name]]));
    if (!(plan.fields || []).some((field: Dict) => JSON.stringify(values[field.name]) !== JSON.stringify(field.value))) continue;
    const files = (plan.fields || []).filter((field: Dict) => field.type === 'file').flatMap((field: Dict) => selectedPlanFiles(plan, field)).map(fileReference);
    plans[plan.id] = { version: plan.version, values, files };
  }
  try {
    const saved = writeAssistantDraft(window.sessionStorage, props.userId, { conversation: draftConversation, at: Date.now(), text: draft.value, commands: selectedCommands.value, files: draftFiles.value.filter(file => file.id).map(fileReference), plans });
    draftWarning.value = saved ? '' : '浏览器未能暂存填写，请勿刷新页面。';
  } catch { draftWarning.value = '浏览器未能暂存填写，请勿刷新页面。'; }
}
function scheduleDraftSave(): void { window.clearTimeout(draftTimer); draftTimer = window.setTimeout(saveDrafts, 500); }
watch([draft, selectedCommands, draftFiles, planValues], scheduleDraftSave, { deep: true });
watch(() => state.value.turns?.map((turn: Dict) => turn.plan), plans => {
  for (const plan of plans || []) for (const field of plan?.fields || []) {
    const key = plan.id + ':' + field.name;
    if (!(key in planValues) && field.value !== undefined) planValues[key] = JSON.parse(JSON.stringify(field.value));
    for (const file of field.selected_files || []) planFiles[file.id] = file;
  }
  scheduleDraftSave();
});
const clearOpen = ref(false);
const editProfile = ref<EditProfile | null>(null), editingExisting = ref(false);
const deleteModelOpen = ref(false), deleteTarget = ref<Dict | null>(null);
const editSwitchOpen = ref(false);
const settingsExitOpen = ref(false), panelExitOpen = ref(false);
let pendingEditAction: EditAction | null = null;
const input = ref<HTMLTextAreaElement | null>(null), launcher = ref<HTMLButtonElement | null>(null), thread = ref<HTMLElement | null>(null), root = ref<HTMLElement | null>(null);
const composerElement = ref<HTMLElement | null>(null);

const busy = computed(() => sending.value || !!state.value.busy || selecting.value || settingBusy.value || settingsLoading.value || Object.values(planBusy).some(Boolean));
const headerStatus = computed(() => loading.value ? '读取中' : error.value ? '待恢复' : state.value.enabled === false ? '已关闭'
  : !state.value.configured ? '待配置' : busy.value ? '处理中' : '就绪');
const nativeOverlays = shallowRef<Element[]>([]), engaged = ref(false), feedback = ref<BotMood>('idle');
let modalOrder: HTMLDialogElement[] = [];
const nativeModal = shallowRef<HTMLDialogElement | null>(null), launcherLayer = ref<HTMLElement | null>(null), assistantLayer = ref<HTMLElement | null>(null);
const panelHost = computed(() => {
  const modals = nativeOverlays.value.filter((element): element is HTMLDialogElement => element instanceof HTMLDialogElement && element.matches(':modal') && !assistantLayer.value?.contains(element));
  return modals[modals.length - 1] || null;
});
const botMood = computed<BotMood>(() => busy.value || loading.value || uploading.value ? 'thinking' : feedback.value !== 'idle' ? feedback.value : engaged.value ? 'engaged' : 'idle');
let interactionTimer = 0, feedbackTimer = 0, overlayObserver: MutationObserver | undefined, nativeDialogObserver: MutationObserver | undefined;
function showFeedback(value: BotMood): void {
  window.clearTimeout(feedbackTimer); feedback.value = value;
  feedbackTimer = window.setTimeout(() => { feedback.value = 'idle'; }, value === 'error' ? 3500 : 2200);
}
watch(() => state.value.turns?.at(-1) && { id: state.value.turns.at(-1).operation_id, status: state.value.turns.at(-1).status }, (value, previous) => {
  if (value?.id === previous?.id && previous?.status === 'pending') {
    if (value.status === 'completed') showFeedback('success');
    else if (value.status === 'failed') showFeedback('error');
  }
});
watch(error, (value, previous) => { if (value && value !== previous) showFeedback('error'); });
function isTyping(target: EventTarget | null): boolean {
  return target instanceof Element && !!target.closest('input:not([type=button]):not([type=submit]):not([type=checkbox]),textarea,select,[contenteditable=true]');
}
function onPageFocus(): void { engaged.value = isTyping(document.activeElement); }
function onPageBlur(): void { queueMicrotask(onPageFocus); }
function onPageInteraction(event: MouseEvent): void {
  if (!(event.target instanceof Element) || event.target.closest('.lighthouse-launcher')) return;
  if (!event.target.closest('button,a,input,textarea,select,[contenteditable=true]')) return;
  engaged.value = true; window.clearTimeout(interactionTimer);
  interactionTimer = window.setTimeout(onPageFocus, 1400);
}
function refreshNativeBlock(preferred?: HTMLDialogElement): void {
  const live = [...document.querySelectorAll<HTMLDialogElement>('dialog[open]')].filter(element => element.matches(':modal'));
  modalOrder = modalOrder.filter(element => live.includes(element));
  for (const element of live) if (!modalOrder.includes(element)) modalOrder.push(element);
  if (preferred && live.includes(preferred)) modalOrder = [...modalOrder.filter(element => element !== preferred), preferred];
  if (nativeOverlays.value.length !== modalOrder.length || modalOrder.some((element, i) => element !== nativeOverlays.value[i])) nativeOverlays.value = [...modalOrder];
  nativeModal.value = modalOrder[modalOrder.length - 1] || null;
}
// Native modal dialogs have a separate top layer; their descendants stay interactive.
watch(nativeModal, async value => {
  nativeDialogObserver?.disconnect();
  if (value) {
    nativeDialogObserver = new MutationObserver(() => { if (!value.isConnected || !value.open) refreshNativeBlock(); });
    nativeDialogObserver.observe(value, { attributes: true, attributeFilter: ['open'] });
    if (value.parentElement) nativeDialogObserver.observe(value.parentElement, { childList: true });
  }
  await nextTick();
  if (disposed || value !== nativeModal.value || !value?.open) return;
  if (panelHost.value === value) assistantLayer.value?.showPopover?.();
  launcherLayer.value?.showPopover?.();
});
function onNativeToggle(event: Event): void {
  const target = event.target;
  if (target instanceof HTMLDialogElement) queueMicrotask(() => refreshNativeBlock(target.open ? target : undefined));
}
const adminModels = computed<Dict[]>(() => (modelSettings.value.models || []));
const modelNames = computed<string[]>(() => (state.value.model_options || []).map((m: Dict) => String(m.name)).filter(Boolean));
const canSaveProfile = computed(() => Boolean(editProfile.value && editProfile.value.name.trim() && editProfile.value.endpoint.trim() && editProfile.value.model.trim()));
const deleteMessage = computed(() => {
  if (!deleteTarget.value) return '';
  const editingThis = Boolean(editProfile.value && String(editProfile.value.id) === String(deleteTarget.value.id) && isDirty());
  const base = `删除模型「${String(deleteTarget.value.name || '')}」？${deleteTarget.value.shared ? '所有账号将无法再选择此默认模型，个人模型和会话保留。' : '删除后无法恢复。'}`;
  return editingThis ? `${base}该模型当前有未保存的编辑内容，将一并丢弃。` : base;
});

let timer = 0, disposed = false, readInFlight = false;
let closedThreadTop = 0, closedThreadAtBottom = true;
let readController: AbortController | null = null;
let settingsController: AbortController | null = null;
let settingsCallSerial = 0;
let shapeEpoch = 0; // bumps on every open/close so stale async positional callbacks (delayed GET tail, close nextTick) are invalidated

const PANEL_W = 620, PANEL_H = 700, MARGIN = 12, STORAGE_PREFIX = 'lighthouse_pos:';
const panelExpanded = ref(false);
const panelWidthLimit = ref<number | null>(null);
const noticeAvailable = ref(false), noticeDockOpen = ref(true), noticeUserOpened = ref(false);
const desktopNotices = ref(window.innerWidth >= 1000);
let noticeEdition = '';
const noticeVisible = computed(() => desktopNotices.value && noticeDockOpen.value && (noticeAvailable.value || noticeUserOpened.value));
const noticeWidth = computed(() => Math.min(360, Math.max(240, Math.floor((panelWidthLimit.value || window.innerWidth - 48) * .4))));
const conversationWidthLimit = computed(() => panelWidthLimit.value ? Math.max(1, panelWidthLimit.value - (noticeVisible.value ? noticeWidth.value : 0) - 2) : null);
function onNoticeEdition(id: string): void { if (noticeEdition && id !== noticeEdition) noticeDockOpen.value = true; noticeEdition = id; }
const panelResizing = ref(false), dragActive = ref(false);
let panelResizeTimer = 0, panelResizeSerial = 0;
function expectedPanelSize(): { w: number; h: number } {
  return { w: Math.min(panelExpanded.value ? 760 : PANEL_W, conversationWidthLimit.value || Infinity, window.innerWidth - 50) + (noticeVisible.value ? noticeWidth.value : 0) + 2, h: Math.min(panelExpanded.value ? 820 : PANEL_H, window.innerHeight - (window.innerWidth <= 700 ? 112 : 48)) };
}
watch([panelExpanded, noticeVisible], async () => {
  if (!open.value || disposed || !root.value) return;
  const epoch = shapeEpoch, serial = ++panelResizeSerial, rect = root.value.getBoundingClientRect();
  const { w, h } = expectedPanelSize();
  const botRect = launcher.value?.getBoundingClientRect();
  const x = botRect && rect.right <= botRect.left ? rect.right - w : rect.left;
  panelPos = clampPosForSize(x, rect.top, w, h);
  panelResizing.value = true;
  window.clearTimeout(panelResizeTimer);
  await nextTick();
  if (disposed || !open.value || epoch !== shapeEpoch || serial !== panelResizeSerial || dragActive.value) return;
  applyPanelPosition();
  saveStoredPos('panel', panelPos.x, panelPos.y);
  panelResizeTimer = window.setTimeout(() => { panelResizing.value = false; }, 360);
});
let panelPos = { x: 0, y: 0 };
let posAppliedPanel = false; // true when explicit left/top mode is active for the panel
let dragging = false;
let dragPointerId: number | null = null;
let dragStartClientX = 0, dragStartClientY = 0, dragOriginX = 0, dragOriginY = 0;
let dragRaf = 0;
let dragLatestX = 0, dragLatestY = 0;
let dragCaptureTarget: HTMLElement | null = null;
let dragSize = { w: PANEL_W, h: PANEL_H };

let press: { target: HTMLElement; id: number; x: number; y: number } | null = null;
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
function composerKeydown(event: KeyboardEvent): void {
  if (commandsMenu.value?.handleKey(event)) return;
  if (event.key === 'Enter' && !event.shiftKey && !event.ctrlKey && !event.altKey && !event.metaKey) submitOnEnter(event);
}
function focusComposer(): void { void nextTick(() => input.value?.focus()); }
function insertCommand(): void {
  const text = draft.value.replace(/(?:^|\n)\/[^\n]*$/, '').trimEnd();
  draft.value = text + (text ? '\n/' : '/');
  void nextTick(() => commandsMenu.value?.reopen());
  focusComposer();
}
function chooseManagedSkill(item: Dict): void {
  if (!selectedCommands.value.some(old => old.kind === item.kind && old.id === item.id)) {
    if (selectedCommands.value.length >= 4) { error.value = '每条消息最多选择4个技能或工具'; return; }
    selectedCommands.value.push(item);
  }
  skillsOpen.value = false; draft.value = draft.value.replace(/(?:^|\n)\/[^\n]*$/, '').trimEnd(); focusComposer();
}
function resizeComposer(): void {
  const element = input.value;
  if (!element) return;
  const follow = isNearBottom();
  element.style.height = 'auto';
  const height = Math.min(140, Math.max(52, element.scrollHeight + 2));
  element.style.height = height + 'px';
  element.style.overflowY = element.scrollHeight > 138 ? 'auto' : 'hidden';
  if (follow) scrollBottom();
}
watch([draft, open, settingsOpen, panelExpanded], () => { void nextTick(resizeComposer); });
function scrollBottom(): void {
  hasNewContent.value = false;
  void nextTick(() => {
    const target = thread.value;
    if (!target) return;
    const focused = document.activeElement;
    if (focused instanceof Element && target.contains(focused) && focused.closest('.plan-form')) { hasNewContent.value = true; return; }
    const forms = state.value.turns?.at(-1)?.plan?.status === 'needs_input' ? target.querySelectorAll<HTMLElement>('.plan-form') : [];
    const form = forms[forms.length - 1];
    if (form) target.scrollTop += form.getBoundingClientRect().top - target.getBoundingClientRect().top - 8;
    else target.scrollTop = target.scrollHeight;
  });
}
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
  const previousTurns = new Map<string, Dict>((state.value.turns || []).map((turn: Dict) => [turn.operation_id, turn]));
  const contentChanged = (result.turns || []).some((turn: Dict) => {
    const before = previousTurns.get(turn.operation_id);
    return !before || before.status !== turn.status || before.answer !== turn.answer || before.plan?.status !== turn.plan?.status || before.plan?.version !== turn.plan?.version;
  });
  const previousPlans = new Map<string, Dict>((state.value.turns || []).filter((turn: Dict) => turn.plan).map((turn: Dict) => [turn.plan.id, turn.plan]));
  const received = new Set((result.turns || []).map((turn: Dict) => turn.operation_id));
  for (const turn of result.turns || []) {
    const previous = previousPlans.get(turn.plan?.id);
    if (turn.plan?.status === 'completed' && previous && previous.status !== 'completed') window.dispatchEvent(new Event('clipflow-business-changed'));
  }
  const follow = isNearBottom();
  const changedConversation = !!state.value.conversation_id && result.conversation_id !== state.value.conversation_id;
  if (changedConversation) {
    disconnectStream(); hasOlder.value = true;
    draft.value = ''; selectedCommands.value = [];
    for (const file of [...draftFiles.value]) removeDraftFile(file.localId);
    for (const values of [planValues, planErrors, planFiles, repairPrefills, expandedPlans]) for (const key of Object.keys(values)) delete values[key];
  }
  const pending = changedConversation ? [] : (state.value.turns || []).filter((turn: Dict) => turn.client_pending && !received.has(turn.operation_id));
  for (const turn of result.turns || []) {
    const live = changedConversation ? undefined : previousTurns.get(turn.operation_id);
    if (!live || live.run_id !== turn.run_id) continue;
    if (turn.status === 'pending' && (live.answer || '').length > (turn.answer || '').length) turn.answer = live.answer;
    if (turn.status === 'pending' && Number(live.process?.at(-1)?.at || 0) > Number(turn.process?.at(-1)?.at || 0)) turn.process = live.process;
  }
  const older = changedConversation ? [] : (state.value.turns || []).filter((turn: Dict) => turn.history_loaded && !received.has(turn.operation_id));
  result.turns = [...older, ...(result.turns || []), ...pending].sort((a: Dict, b: Dict) => Number(a.at || 0) - Number(b.at || 0));
  state.value = result;
  restoreDrafts();
  restoreOutgoing();
  if (contentChanged) { if (follow) scrollBottom(); else hasNewContent.value = true; }
}
function abortRead(): void {
  if (readController) { readController.abort(); readController = null; }
  readInFlight = false;
}
function scheduleRead(): void {
  window.clearTimeout(timer);
  if (open.value && !disposed && (state.value.busy || sending.value || state.value.turns?.some((t: Dict) => ['running', 'submitted'].includes(t.plan?.status)))) {
    timer = window.setTimeout(() => {
      const hasRunningPlan = state.value.turns?.some((t: Dict) => ['running', 'submitted'].includes(t.plan?.status));
      if (!hasRunningPlan && streamRun && streamChat.value?.status === 'streaming' && performance.now() - lastStreamUpdateAt < 6000) scheduleRead();
      else void readState(false, false);
    }, 2200);
  }
}
async function readState(showLoading = true, force = false): Promise<void> {
  if (!showLoading && readInFlight && !force) { scheduleRead(); return; }
  abortRead();
  const controller = new AbortController();
  readController = controller;
  readInFlight = true;
  if (showLoading) { loading.value = true; error.value = ''; }
  try {
    const result = await call('conversation', 'GET', undefined, 20000, controller.signal);
    if (disposed || readController !== controller) return;
    // 响应到达后、替换 state 之前再测跟随，避免长 GET 返回时被强制跳底（首次打开 showLoading 可强制）。
    replaceState(result);
    if (result.active_run_id && open.value) attachStream(result.active_run_id);
    if (showLoading) scrollBottom();
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
  placeBesideLauncher();
  open.value = true;
  saveOpenState();
  await nextTick();
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  applyPanelPosition();
  if (closedThreadAtBottom) scrollBottom();
  else if (thread.value) thread.value.scrollTop = closedThreadTop;
  // busy 下也允许重开查看进度：只重新 GET + 轮询显示原请求，不产生新的 POST。
  input.value?.focus();
  await readState(!state.value.conversation_id, true);
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  await nextTick();
  if (disposed || !open.value || epoch !== shapeEpoch) return;
  ensureInBounds();
  input.value?.focus();
}
function onLauncherClick(): void {
  if (open.value) requestPanelClose();
  else void openAssistant();
}
let closingPanelRect: DOMRect | undefined;
function pinClosingPanel(element: Element): void {
  if (!(element instanceof HTMLElement) || !closingPanelRect) return;
  const rect = closingPanelRect;
  Object.assign(element.style, { position: 'fixed', left: `${rect.left}px`, top: `${rect.top}px`, width: `${rect.width}px`, height: `${rect.height}px` });
}
function unpinPanel(element: Element): void {
  if (element instanceof HTMLElement) for (const property of ['position', 'left', 'top', 'width', 'height']) element.style.removeProperty(property);
}
function close(): void {
  closedThreadAtBottom = isNearBottom();
  closedThreadTop = thread.value?.scrollTop || 0;
  closingPanelRect = root.value?.querySelector('.assistant-shell')?.getBoundingClientRect();
  const epoch = ++shapeEpoch;
  stopGesture(); // 先结束手势：仍在面板/启动器形态时保存末帧坐标，再切换形状。保留 epoch 守卫。
  window.clearTimeout(panelResizeTimer); panelResizing.value = false; ++panelResizeSerial;
  open.value = false; settingsOpen.value = false; skillsOpen.value = false; appearanceOpen.value = false; editProfile.value = null; settingsError.value = ''; settingsLoading.value = false;
  saveOpenState();
  window.clearTimeout(timer);
  ++settingsCallSerial;
  if (settingsController) { settingsController.abort(); settingsController = null; }
  abortRead();
  disconnectStream();
  void nextTick(() => { if (!disposed && open.value === false && epoch === shapeEpoch) launcher.value?.focus(); });
}
async function send(retry?: Dict, regenerate = false): Promise<void> {
  if (sending.value || loading.value || uploading.value || (!retry && !draft.value.trim() && !draftFiles.value.some(f => f.id))) return;
  if (!retry && commandsMenu.value?.visible) return;
  const question = retry?.question || draft.value.trim();
  if (!retry && /^(?:请)?确认发送[。！!\s]*$/.test(question) && !draftFiles.value.length) {
    const pending = state.value.turns.filter((turn: Dict) => ['needs_input', 'awaiting_confirmation', 'awaiting_second_confirmation'].includes(turn.plan?.status));
    if (pending.some((turn: Dict) => turn.plan.planned_notice)) {
      if (pending.length !== 1) { error.value = '有多个待处理操作，请在要发送的那条通告下确认。'; return; }
      const turn = pending[0];
      if (turn.plan.status === 'needs_input') { error.value = '请先补齐下方表单并核对通告全文，再确认发送。'; return; }
      draft.value = ''; saveDrafts();
      await confirmPlan(turn);
      return;
    }
  }
  const commands = retry?.commands || selectedCommands.value;
  const submittedCommands = retry?.submitted_commands || commands.map((item: Dict) => ({ kind: item.kind, id: item.id }));
  const attachments = retry?.attachments || draftFiles.value.filter(f => f.id).map(({ preview, localId, uploading, error, ...file }) => file);
  const operation = retry?.operation_id || (window.crypto?.randomUUID?.() || `${Date.now()}_${Math.random().toString(36).slice(2)}_assistant`);
  const attempt = retry?.client_pending && !regenerate ? (retry.attempt_id || operation) : (window.crypto?.randomUUID?.() || `${Date.now()}_${Math.random().toString(36).slice(2)}_attempt`);
  const at = retry?.at || Date.now() / 1000;
  sending.value = true; error.value = '';
  abortRead();
  rememberOutgoing({ conversation_id: state.value.conversation_id, operation_id: operation, attempt_id: attempt, question: question || '请处理附件', attachments, commands, submitted_commands: submittedCommands, at });
  if (!retry) {
    state.value.turns.push({ question: question || '请处理附件', attachments, commands: [...commands], submitted_commands: submittedCommands, operation_id: operation, attempt_id: attempt, status: 'pending', client_pending: true, at });
    draft.value = '';
    selectedCommands.value = [];
    for (const file of [...draftFiles.value]) if (attachments.some((sent: Dict) => sent.id === file.id)) removeDraftFile(file.localId);
  } else { retry.status = 'pending'; retry.error = ''; retry.attempt_id = attempt; retry.client_pending = true; }
  saveDrafts();
  scrollBottom();
  scheduleRead();
  try {
    const result = await call('messages', 'POST', { question, commands: submittedCommands, file_ids: attachments.map((file: Dict) => file.id), operation_id: operation, attempt_id: attempt, conversation_id: state.value.conversation_id }, 20000);
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
  renderTimer = 0;
  lastStreamUpdateAt = 0;
}
function renderStream(): void {
  const follow = isNearBottom();
  let changed = false;
  for (const message of streamChat.value?.messages || []) {
    if (message.role !== 'assistant') continue;
    const metadata = message.metadata as Dict | undefined;
    const turn = state.value.turns?.find((item: Dict) => item.run_id === metadata?.run_id);
    if (!turn || turn.status !== 'pending') continue;
    const answer = message.parts.filter(part => part.type === 'text').map(part => part.text).join('');
    if (answer !== turn.answer && answer.length >= (turn.answer || '').length) { turn.answer = answer; changed = true; }
  }
  if (changed) { if (follow) scrollBottom(); else hasNewContent.value = true; }
}
// The SDK replaces the active message for every chunk; do not traverse all its parts.
watch(() => { const messages = streamChat.value?.messages; return messages?.[messages.length - 1]; }, () => {
  lastStreamUpdateAt = performance.now();
  if (!document.hidden && !renderTimer) renderTimer = window.setTimeout(() => { renderTimer = 0; renderStream(); }, 160);
});
function resumeStreamRendering(): void { if (!document.hidden && open.value) renderStream(); }
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
      lastStreamUpdateAt = performance.now();
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
function editQuestion(turn: Dict): void { draft.value = String(turn.question || ''); selectedCommands.value = [...(turn.commands || [])]; focusComposer(); }
async function loadHistory(): Promise<void> {
  if (historyLoading.value) return;
  historyLoading.value = true;
  const conversationId = state.value.conversation_id;
  const height = thread.value?.scrollHeight || 0;
  try {
    const before = Math.min(...state.value.turns.map((turn: Dict) => Number(turn.at || Date.now() / 1000)));
    const result = await call('history?before=' + before);
    if (disposed || conversationId !== state.value.conversation_id) return;
    const known = new Set(state.value.turns.map((turn: Dict) => turn.operation_id));
    state.value.turns.unshift(...(result.turns || []).filter((turn: Dict) => !known.has(turn.operation_id)).map((turn: Dict) => ({ ...turn, history_loaded: true })));
    hasOlder.value = !!result.has_more;
    await nextTick();
    if (!disposed && conversationId === state.value.conversation_id && thread.value) thread.value.scrollTop += thread.value.scrollHeight - height;
  } catch (e) { if (!disposed && conversationId === state.value.conversation_id) error.value = e instanceof Error ? e.message : '历史读取失败'; }
  finally { historyLoading.value = false; }
}

function safeAttachmentUrl(value: unknown): string {
  return typeof value === 'string' && /^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(value) ? value : '';
}
function morningDownloadUrl(result: Dict): string {
  return result.ok && result.api_id === 'POST /api/daily-tasks/morning-meeting/generate'
    && typeof result.download_url === 'string' && /^\/api\/daily-tasks\/morning-meeting\/download\?date=\d{4}-\d{2}-\d{2}$/.test(result.download_url) ? result.download_url : '';
}
function removeDraftFile(localId: string): void {
  const file = draftFiles.value.find(f => f.localId === localId);
  if (file?.preview) URL.revokeObjectURL(file.preview);
  draftFiles.value = draftFiles.value.filter(f => f.localId !== localId);
}
async function uploadFile(file: File, purpose = ''): Promise<Dict> {
  const maxMiB = purpose === 'drill_template' ? 64 : purpose === 'water_photo' ? 8 : 20;
  if (!file.size || file.size > maxMiB * 1024 * 1024) throw new Error(`单文件须非空，最多${maxMiB}MiB`);
  if (purpose === 'drill_template' && !file.name.toLowerCase().endsWith('.xlsx')) throw new Error('演练模板仅支持 .xlsx 文件');
  if (purpose === 'water_photo' && !file.type.startsWith('image/')) throw new Error('水表照片仅支持图片');
  const form = new FormData(); form.append('files', file);
  const result = await requestJson('/api/assistant/files' + (['drill_template', 'water_photo'].includes(purpose) ? '?purpose=' + purpose : ''), { method: 'POST', body: form, timeoutMs: 180000 });
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
  const input = event.target as HTMLInputElement, files = Array.from(input.files || []);
  input.value = '';
  await addPlanFiles(files, plan, field);
}
function pastePlanFiles(event: ClipboardEvent, plan: Dict, field: Dict): void {
  const files = Array.from(event.clipboardData?.files || []);
  if (!files.length) return;
  event.preventDefault(); event.stopPropagation();
  void addPlanFiles(files, plan, field);
}
function dropPlanFiles(event: DragEvent, plan: Dict, field: Dict): void {
  const files = Array.from(event.dataTransfer?.files || []);
  if (!files.length) return;
  event.preventDefault(); event.stopPropagation();
  void addPlanFiles(files, plan, field);
}
async function addPlanFiles(files: File[], plan: Dict, field: Dict): Promise<void> {
  if (isPlanBusy(plan) || disposed) return;
  if (!files.length) return;
  const key = plan.id + ':' + field.name, limit = field.maxItems ?? 10;
  const purpose = field.native_water_photos ? 'water_photo' : field.purpose === 'drill_template' ? 'drill_template' : '', maxMiB = purpose === 'drill_template' ? 64 : purpose === 'water_photo' ? 8 : 20;
  const maxBytes = Math.min(Number(field.max_bytes) || maxMiB * 1024 * 1024, maxMiB * 1024 * 1024);
  const existing = limit === 1 ? [] : planChoices(plan, field);
  if (existing.length + files.length > limit) { planErrors[plan.id] = `此处最多选择 ${limit} 个文件`; return; }
  if (files.some(file => !file.size || file.size > maxBytes) || files.reduce((n, file) => n + file.size, 0) + existing.reduce<number>((n, id) => n + (planFiles[id]?.size || 0), 0) > 100 * 1024 * 1024) { planErrors[plan.id] = `单份须非空，最多${maxBytes / 1024 / 1024}MiB、合计最多100MiB`; return; }
  if (field.native_water_photos && files.some(file => !file.type.startsWith('image/'))) { planErrors[plan.id] = '水表照片仅支持图片'; return; }
  planBusy[plan.id] = true;
  delete planErrors[plan.id];
  const progress = planUploadProgress[plan.id] = { key, name: '', done: 0, total: files.length };
  try {
    const selected = [...existing];
    for (const file of files) {
      if (disposed) return;
      progress.name = file.name;
      const result = await uploadFile(file, purpose);
      if (disposed) return;
      planFiles[result.id] = result;
      selected.push(result.id);
      planValues[key] = [...selected];
      progress.done++;
    }
  }
  catch (e) { if (!disposed) planErrors[plan.id] = `${progress.name}：${e instanceof Error ? e.message : '附件上传失败'}。已上传 ${progress.done}/${files.length}，已有选择已保留，请重新添加未成功的文件。`; }
  finally { delete planBusy[plan.id]; delete planUploadProgress[plan.id]; }
}
function selectedPlanFiles(plan: Dict, field: Dict): Dict[] {
  return planChoices(plan, field).map(id => planFiles[id] || { id, name: '已选附件' });
}
function planStatusLabel(status: string): string { return ({ needs_input: '待补充', awaiting_confirmation: '待确认', awaiting_second_confirmation: '再次确认', running: '执行中', submitted: '后台处理中', completed: '已完成', cancelled: '已取消', failed: '未完成', superseded: '已由新版替代' } as Dict)[status] || status; }
function nativeTaskText(result: Dict): string {
  const job = result.job_result?.data || {};
  const phase = String(job.phase || job.status || '');
  const labels: Dict = { accepted:'已受理', queued:'排队中', upload_queued:'等待上传', qt_queued:'等待执行', upload_waiting:'等待上传多维', remote_intent:'正在写入多维', uploading:'正在上传多维', remote_written:'多维已写入，正在同步页面', sending_message:'正在发送飞书消息', message_sent:'飞书消息已发送', success:'发送成功', succeeded:'已完成', failed:'操作未完成' };
  const started = Number(job.upload_started_at || 0);
  const elapsed = Math.max(0, Math.floor(Date.now() / 1000 - started));
  if (started > 0 && elapsed >= 30 && ['uploading', 'remote_intent'].includes(phase)) return `${labels[phase]}（已等待 ${Math.floor(elapsed / 60)}分${elapsed % 60}秒）`;
  return labels[phase] || '';
}
function planStatusText(plan: Dict): string {
  if (['running','submitted'].includes(plan.status)) return nativeTaskText(plan.results?.at(-1) || {}) || planStatusLabel(plan.status);
  return planStatusLabel(plan.status);
}
function resultStatusText(result: Dict, plan: Dict): string {
  if (!result.ok) return result.error || '操作未完成';
  if (result.job_result) return nativeTaskText(result) || (plan.status === 'completed' ? '业务处理已完成' : '正在查询原任务状态');
  return result.status === 202 ? '已提交后台处理' : '接口已完成';
}
function planFieldId(plan: Dict, field: Dict): string { return 'plan-' + plan.id + '-' + encodeURIComponent(field.name); }
function planFieldIsGroup(field: Dict): boolean { return ['object', 'array', 'multiselect'].includes(field.type) || (field.type === 'select' && (field.native_planned_choice || !field.options_source && smallSingleChoice(field.options))); }
function planInputIncomplete(plan: Dict): boolean {
  if (!structuredFieldsReady.value && visiblePlanFields(plan).some((field: Dict) => ['object', 'array'].includes(field.type))) return true;
  if (visiblePlanFields(plan).some((field: Dict) => (field.native_repair_prefill || field.native_notice_prefill) && repairPrefills[plan.id + ':' + field.name]?.error)) return true;
  return (plan.fields || []).some((field: Dict) => {
    const value = planValues[plan.id + ':' + field.name];
    if (field.native_water_photos) {
      const form = plan.fields.find((item: Dict) => item.native_water_record && item.operation_index === field.operation_index);
      if (!planValues[plan.id + ':' + form?.name]?.retained_image_ids?.length && !planChoices(plan, field).length && !plan.operations?.[field.operation_index]?.body?.upload_ids?.length) return true;
    }
    if (field.type === 'file') return (field.required && !planChoices(plan, field).length) || selectedPlanFiles(plan, field).some(file => file.unavailable);
    if (field.native_cabinet_text_fill || field.native_cabinet_text_create) return !value?.rows?.length;
    if (field.native_usage_confirmation) return !Array.isArray(value) || !value.length;
    if (field.native_cabinet_proof && (!value?.image_id || !value?.row_id)) return true;
    if (field.native_notice_sop && !value?.exempt) {
      const scope = planControl(plan, field).notice_scope;
      const sop = (field.sops || []).find((item: Dict) => item.sop_id === value?.sop_id);
      return !scope || value?.scope !== scope || field.directory_scope !== scope || !sop || !!sop.blocked_reason || !value?.operator_record_id || !value?.reviewer_record_id;
    }
    return field.native_cabinet_correct && (field.directory_scope !== value?.scope || !(field.rows || []).some((row: Dict) => row.row_id === value?.row_id));
  });
}
function planChoices(plan: Dict, field: Dict): (string | number)[] { const value = planValues[plan.id + ':' + field.name]; return Array.isArray(value) ? value : []; }
function togglePlanChoice(plan: Dict, field: Dict, value: string | number, checked: boolean): void { const old = planChoices(plan, field); planValues[plan.id + ':' + field.name] = checked ? Array.from(new Set([...old, value])) : old.filter(item => item !== value); planSelectionChanged(plan, field); }
function clearPlanChoice(plan: Dict, field: Dict): void { planValues[plan.id + ':' + field.name] = []; planSelectionChanged(plan, field); }
function planControl(plan: Dict, field: Dict): Dict {
  if (field.native_notice_sop) {
    const notice = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.native_notice);
    const buildings = notice ? planValues[plan.id + ':' + notice.name]?.building_codes : [];
    const options = planOptionStates[plan.id + ':' + field.name];
    return { ...field, notice_scope: Array.isArray(buildings) && buildings.length === 1 && field.scopes.includes(buildings[0]) ? buildings[0] : '', directory_loading: !!options?.loading, directory_error: options?.error || '' };
  }
  if (!field.native_repair || !field.unlinked_children) return field;
  const relation = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.path === 'source_repair_ids');
  const selected = relation ? planValues[plan.id + ':' + relation.name] : plan.operations[field.operation_index || 0]?.body?.source_repair_ids;
  return { ...field, children: selected?.length ? field.linked_children || field.children : field.unlinked_children };
}
function choosePlanOption(plan: Dict, field: Dict, value: unknown): void {
  if (isPlanBusy(plan) || value === undefined) return;
  planValues[plan.id + ':' + field.name] = value;
  if (field.path === 'manual_binding_choice' && value === 'bind') {
    const source = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.options_source === 'notice_sources');
    const turn = state.value.turns.find((item: Dict) => item.plan?.id === plan.id);
    if (source && turn && !source.options?.length) { void loadPlanOptions(turn, source); return; }
  }
  planSelectionChanged(plan, field);
}
function planSelectionChanged(plan: Dict, field: Dict): void {
  if (field.native_notice_identity && field.path === 'source_month') {
    const related = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.native_notice_identity && item.options_source);
    if (related) { planValues[plan.id + ':' + related.name] = ''; related.options = []; }
    return;
  }
  if (field.native_notice_binding) {
    const source = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.options_source === 'notice_sources');
    if (source) {
      if (field.path === 'manual_binding_choice' && planValues[plan.id + ':' + field.name] !== 'bind') planValues[plan.id + ':' + source.name] = '';
      if (source.native_notice_prefill) void prefillNotice(plan, source);
    }
    return;
  }
  if (['scope', 'month'].includes(field.path)) {
    const event = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.native_event_transfer);
    if (event) { planValues[plan.id + ':' + event.name] = ''; event.options = []; }
  }
  if (field.options_source === 'repair_events') {
    const related = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.path === 'source_repair_ids');
    if (related) { planValues[plan.id + ':' + related.name] = []; related.options = []; }
  }
  const draft = plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.native_repair);
  if (!draft) return;
  if (draft.native_repair_prefill && ['repair_events', 'repair_notices'].includes(field.options_source)) {
    void prefillRepair(plan, draft);
    return;
  }
  if (field.options_source === 'repair_notices' && planChoices(plan, field).length) {
    const key = plan.id + ':' + draft.name;
    const value = { ...(planValues[key] || draft.value || {}) };
    const editable = new Set((draft.linked_children || draft.children || []).map((child: Dict) => child.path));
    for (const child of draft.unlinked_children || []) {
      if (!editable.has(child.path)) value[child.path] = draft.value?.[child.path];
    }
    planValues[key] = value;
  }
  if (field.options_source !== 'repair_devices') return;
  const selected = planChoices(plan, field), options = field.options || [];
  const names = selected.map(id => options.find((option: Dict) => option.value === id)?.device_name);
  if (names.some(name => !name)) return;
  const key = plan.id + ':' + draft.name;
  const value = { ...(planValues[key] || draft.value || {}) };
  for (const name of ['设备名称', '设备编号']) {
    if (draft.children?.some((child: Dict) => child.path === name)) value[name] = [...new Set(names)].join('、');
  }
  if (draft.repair_catalog) {
    const editable = new Set(draft.children.map((child: Dict) => child.path));
    const patch = repairDeviceDependentPatch(value, '设备名称', draft.repair_catalog.devices, draft.repair_catalog.brands);
    Object.assign(value, Object.fromEntries(Object.entries(patch).filter(([name]) => editable.has(name))));
  }
  planValues[key] = value;
}
async function prefillNotice(plan: Dict, source: Dict): Promise<void> {
  if (isPlanBusy(plan)) return;
  const form = plan.fields.find((item: Dict) => item.operation_index === source.operation_index && item.native_notice);
  if (!form) return;
  const key = plan.id + ':' + source.name, formKey = plan.id + ':' + form.name;
  const previous = repairPrefills[key] || { automatic: {}, before: {} };
  const selected = planValues[key] || '', version = plan.version;
  repairPrefills[key] = { ...previous, loading: true, error: '', warning: '' };
  planBusy[plan.id] = true;
  try {
    const buildings = planValues[formKey]?.building_codes || [];
    const scope = buildings.length === 1 ? buildings[0] : plan.operations[source.operation_index || 0]?.body?.scope;
    const data = selected ? await call('plans/' + plan.id + '/notice-prefill', 'POST', {
      version, operation_index: source.operation_index || 0, source_record_id: selected, scope,
    }) : { version, fields: {} };
    if (disposed || !state.value.turns.some((turn: Dict) => turn.plan?.id === plan.id && turn.plan.version === version && turn.plan.status === 'needs_input')) return;
    if (data.version !== version) throw new Error('关联资料版本已变化，请重新读取。');
    const current = { ...(planValues[formKey] || form.value || {}) }, automatic: Dict = {}, before: Dict = {};
    const manual = new Set<string>(previous.manual || []);
    for (const child of form.children || []) {
      const name = child.path;
      const wasAutomatic = Object.prototype.hasOwnProperty.call(previous.automatic, name);
      if (wasAutomatic ? JSON.stringify(current[name]) !== JSON.stringify(previous.automatic[name]) : ![undefined, null, ''].includes(current[name])) manual.add(name);
      if (manual.has(name)) continue;
      if (Object.prototype.hasOwnProperty.call(data.fields, name)) {
        before[name] = wasAutomatic ? previous.before[name] : current[name] ?? '';
        automatic[name] = current[name] = data.fields[name];
      } else if (wasAutomatic) current[name] = previous.before[name] ?? '';
    }
    planValues[formKey] = current;
    Object.assign(repairPrefills[key], { automatic, before, manual: [...manual] });
  } catch (failure) {
    if (!disposed) repairPrefills[key].error = failure instanceof Error ? failure.message : '关联资料读取失败，请重试。';
  } finally {
    if (repairPrefills[key]) repairPrefills[key].loading = false;
    delete planBusy[plan.id];
    if (!disposed) scheduleRead();
  }
}
async function prefillRepair(plan: Dict, form: Dict): Promise<void> {
  if (isPlanBusy(plan)) return;
  const key = plan.id + ':' + form.name;
  const previous = repairPrefills[key] || { automatic: {} };
  const body = plan.operations[form.operation_index || 0]?.body || {};
  const valueFor = (path: string) => {
    const field = plan.fields.find((item: Dict) => item.operation_index === form.operation_index && item.path === path);
    return field ? planValues[plan.id + ':' + field.name] ?? field.value : body[path];
  };
  const event = valueFor('source_event_id') === '__empty__' ? '' : valueFor('source_event_id') || '';
  const repairs = valueFor('source_repair_ids') || [];
  const version = plan.version;
  repairPrefills[key] = { ...previous, loading: true, error: '', warning: '' };
  planBusy[plan.id] = true;
  try {
    const data = await call('plans/' + plan.id + '/repair-prefill', 'POST', {
      version, operation_index: form.operation_index || 0, source_event_id: event, source_repair_ids: repairs,
      scope: valueFor('scope') || 'ALL', source_month: valueFor('source_month') || '',
    });
    if (disposed || !state.value.turns.some((turn: Dict) => turn.plan?.id === plan.id && turn.plan.version === version && turn.plan.status === 'needs_input')) return;
    if (data.version !== version) throw new Error('关联资料版本已变化，请重新读取。');
    repairPrefills[key].warning = (data.warnings || []).join('；');
    if (data.skip) return;
    const fields = data.fields || {};
    const current = { ...(planValues[key] || form.value || {}) };
    const controlled = new Set<string>(data.controlled_fields || []);
    if (event !== (body.source_event_id || '') || JSON.stringify(repairs) !== JSON.stringify(body.source_repair_ids || [])) {
      for (const name of data.source_field_names || []) controlled.add(name);
    }
    const children = planControl(plan, form).children || [];
    const automatic: Dict = {};
    for (const child of children) {
      const name = child.path;
      const wasAutomatic = Object.prototype.hasOwnProperty.call(previous.automatic, name);
      const baseline = wasAutomatic ? previous.automatic[name] : form.value?.[name];
      const dirty = (!wasAutomatic && form.dirty_fields?.includes(name)) || JSON.stringify(current[name]) !== JSON.stringify(baseline);
      if (dirty && !controlled.has(name)) continue;
      if (Object.prototype.hasOwnProperty.call(fields, name)) {
        current[name] = child.repair_field ? repairDraftInputValue(child.repair_field, fields[name]) : fields[name];
      } else if (Object.prototype.hasOwnProperty.call(previous.automatic, name)) {
        current[name] = ['array', 'multiselect'].includes(child.type) ? [] : '';
      } else continue;
      automatic[name] = current[name];
    }
    const editable = new Set(children.map((child: Dict) => child.path));
    for (const child of form.unlinked_children || []) {
      if (!editable.has(child.path)) current[child.path] = form.value?.[child.path];
    }
    planValues[key] = current;
    repairPrefills[key].automatic = automatic;
  } catch (failure) {
    if (!disposed && repairPrefills[key]) repairPrefills[key].error = failure instanceof Error ? failure.message : '关联资料读取失败，请重试。';
  } finally {
    if (repairPrefills[key]) repairPrefills[key].loading = false;
    delete planBusy[plan.id];
    if (!disposed) scheduleRead();
  }
}
function openPlanPicker(event: Event): void { const input = (event.currentTarget as HTMLElement).parentElement?.querySelector('input'); if (!input) return; input.focus(); try { input.showPicker?.(); } catch { /* Native keyboard editing remains available. */ } }
function fieldVisible(plan: Dict, field: Dict): boolean { if (!field.when) return true; const parent = plan.fields.find((f: Dict) => f.operation_index === field.operation_index && f.path === field.when.path); const value = parent ? planValues[plan.id + ':' + parent.name] : plan.operations[field.operation_index || 0]?.body?.[field.when.path]; return value === field.when.equals; }
function visiblePlanFields(plan: Dict): Dict[] {
  const fields = (plan.fields || []).filter((field: Dict) => fieldVisible(plan, field));
  for (const form of fields.filter((field: Dict) => field.native_repair || field.native_notice)) {
    const positions = fields.flatMap((field: Dict, index: number) => field.operation_index === form.operation_index ? [index] : []);
    const rank: Dict = { scope: -3, source_event_id: -2, summary_record_id: -2, manual_binding_choice: -2, source_record_id: -1, source_repair_ids: -1, cmdb_record_ids: -1 };
    const ordered = positions.map((index: number) => fields[index]).sort((a: Dict, b: Dict) => (rank[a.path] || 0) - (rank[b.path] || 0));
    positions.forEach((position: number, index: number) => { fields[position] = ordered[index]; });
  }
  return fields;
}
function replacePlan(plan: Dict): void {
  if (disposed) return;
  abortRead();
  const turn = state.value.turns.find((item: Dict) => item.plan?.id === plan.id);
    if (turn) {
      if (plan.status === 'completed' && turn.plan?.status !== 'completed') window.dispatchEvent(new Event('clipflow-business-changed'));
    turn.plan = plan;
    const answer: Dict = { completed: '操作已完成。', failed: '操作未完成：' + (plan.error || ''), cancelled: '操作已取消。', submitted: '操作已提交，后台仍在处理。', superseded: '已由最新提交替代，旧任务不再重试。' };
    if (answer[plan.status]) turn.answer = answer[plan.status];
  }
}
async function loadPlanOptions(turn: Dict, field: Dict, selectedScope = '', selectedKeyword?: string): Promise<void> {
  if (isPlanBusy(turn.plan)) return;
  const optionState = reactive({ loading: true, error: '' });
  if (field.native_notice_sop) planOptionStates[turn.plan.id + ':' + field.name] = optionState;
  planBusy[turn.plan.id] = true;
  delete planErrors[turn.plan.id];
  const scopeField = (turn.plan.fields || []).find((f: Dict) => f.path === 'scope' && f.operation_index === field.operation_index);
  const notice = field.native_notice_binding && turn.plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.native_notice);
  const buildings = notice ? planValues[turn.plan.id + ':' + notice.name]?.building_codes || [] : [];
  const scope = selectedScope || (buildings.length === 1 ? buildings[0] : scopeField ? planValues[turn.plan.id + ':' + scopeField.name] : '');
  const filters: Record<string, string> = {};
  if (field.native_event_transfer) {
    const month = turn.plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.path === 'month');
    filters.month = month ? planValues[turn.plan.id + ':' + month.name] : '';
  }
  if (field.native_notice_identity && field.source_binding_only) {
    const month = turn.plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.path === 'source_month');
    filters.month = month ? planValues[turn.plan.id + ':' + month.name] : '';
  }
  if (['repair_events', 'repair_notices', 'repair_projects', 'repair_devices', 'notice_sources', 'notice_targets', 'notice_identity_sources', 'notice_identity_targets', 'event_records'].includes(field.options_source)) {
    const selected = planValues[turn.plan.id + ':' + field.name];
    filters.selected = JSON.stringify(Array.isArray(selected) ? selected : selected ? [selected] : []);
  }
  if (field.options_source === 'repair_notices') {
    const event = turn.plan.fields.find((item: Dict) => item.operation_index === field.operation_index && item.path === 'source_event_id');
    const selected = event ? planValues[turn.plan.id + ':' + event.name] : turn.plan.operations[field.operation_index || 0]?.body?.source_event_id;
    filters.source_event_id = typeof selected === 'string' ? selected : '';
  }
  try { replacePlan(await call('plans/' + turn.plan.id + '/options?' + new URLSearchParams({ field: field.name, q: selectedKeyword ?? planValues[turn.plan.id + ':search:' + field.name] ?? '', ...(scope ? { scope } : {}), ...filters }).toString())); }
  catch (e) { if (!disposed) { optionState.error = e instanceof Error ? e.message : '读取未完成'; planErrors[turn.plan.id] = optionState.error; } }
  finally { optionState.loading = false; delete planBusy[turn.plan.id]; if (!disposed) scheduleRead(); }
}
function operationPreview(op: Dict, editing = false): { label: string; value: string }[] {
  if (op.api_id === 'POST /api/message-delivery/send') return editing ? [] : [
    { label: '收件人', value: op.selected_labels?.recipient_names || '尚未选择' },
    { label: '选定内容', value: op.selected_labels?.message_content || '' },
    { label: '文字', value: op.body?.text || '' },
    { label: '附件', value: (op.selected_files || []).map((file: Dict) => file.name).join('、') },
  ].filter(item => item.value);
  if (op.body?.command_format === 'notice_command') {
    if (editing) return [];
    const body = op.body || {}, draft = body.patch || {}, work = body.work_type;
    const result = [{ label: '通告类型', value: draft.notice_type || ({ maintenance: '维保', change: '变更', repair: '检修', power: '上下电', polling: '轮巡', adjust: '调整' } as Dict)[work] || work },
      { label: '操作', value: ({ start: '开始', update: '更新', end: '结束' } as Dict)[body.action] || body.action }];
    if (body.action === 'start') result.push({ label: '计划通告关联', value: body.manual_binding_choice === 'unbound' ? '不绑定，作为独立通告' : op.selected_labels?.source_record_id || '尚未选择' });
    for (const { path: key, label } of op.notice_fields || []) {
      const value = draft[key];
      if (value == null || value === '' || Array.isArray(value) && !value.length) continue;
      result.push({ label, value: key === 'building_codes' ? value.map((code: string) => code === '110' ? '110站' : code + '楼').join('、') : String(value) });
    }
    if (!draft.building_codes?.length && draft.building) result.push({ label: '楼栋/范围', value: draft.building });
    if (op.selected_labels?.sop) result.push({ label: 'SOP', value: [op.selected_labels.sop, [op.selected_labels.operator, op.selected_labels.reviewer].filter(Boolean).join(' / ')].filter(Boolean).join(' · ') });
    if (op.selected_files?.length) result.push({ label: '附件', value: `${op.selected_files.length} 份` });
    return result;
  }
  if (['POST /api/capacity/water/records', 'PATCH /api/capacity/water/records/{record_id}'].includes(op.api_id)) return editing ? [] : [
    { label: '楼栋', value: op.body?.scope + '楼' }, { label: '水表', value: op.body?.meter || '' },
    { label: '统计日期', value: op.body?.statistic_date || '' }, { label: '频次 / 班次', value: [op.body?.frequency, op.body?.shift].filter(Boolean).join(' / ') },
    { label: '水表数值', value: String(op.body?.meter_value ?? '') },
    { label: '当期耗水量（修正）', value: op.body?.corrected_usage == null || op.body?.corrected_usage === '' ? '使用公式结果' : String(op.body.corrected_usage) },
    { label: '水表照片', value: `保留 ${op.selected_labels?.retained_photo_count || 0} 张，新增 ${op.selected_files?.length || 0} 张` },
    ...(op.body?.abnormal_note ? [{ label: '异常原因', value: op.body.abnormal_note }] : []),
  ];
  if (op.api_id === 'POST /api/drills') return editing ? [] : [
    { label: '演练名称', value: op.body?.name || '使用模板文件名' },
    { label: '月份', value: op.body?.month || '' },
    { label: '参演楼栋', value: (op.body?.assigned_scopes || []).map((scope: string) => scope + '楼').join('、') },
    { label: '操作', value: '上传模板为草稿，核对配置后再发布' },
  ];
  if (op.api_id === 'POST /api/daily-tasks/morning-meeting/generate') return editing ? [] : [
    { label: '日期', value: op.body?.date || '' },
    { label: '天气', value: op.body?.weather_condition || '未填写' },
    ...['dry_bulb_temperature', 'wet_bulb_temperature'].map((key, index) => ({ label: index ? '湿球温度' : '干球温度', value: op.body?.[key] == null ? '未填写' : `${op.body[key]} ℃` })),
  ];
  if (op.api_id === 'POST /api/notice-identity/bind') return editing ? [] : [
    { label: '当前通告', value: op.selected_labels?.original_notice || op.body?.title || '所选通告' },
    { label: op.body?.source_binding_only ? '绑定源表事项' : '绑定目标多维', value: op.selected_labels?.[op.body?.source_binding_only ? 'source_record_id' : 'target_record_id'] || '所选记录' },
    { label: '操作', value: '仅保存关联关系，不发送通告' },
  ];
  if (op.api_id === 'POST /api/events/transfer-repair') return editing ? [] : [
    { label: '事件', value: op.selected_labels?.record_id || '所选事件' },
    { label: '月份', value: op.body?.month || '' },
    { label: '操作', value: '标记转检修，未填写维修单' },
  ];
  if (editing && op.body?.command_format === 'notice_command' && op.body?.work_type !== 'event') return [];
  if (editing && /^(POST|PUT) \/api\/repair-management\/(records|followups)(\/\{record_id\})?$/.test(op.api_id || '')) return [];
  if (op.api_id === 'POST /api/signatures/usage-confirmations/send') return [
    { label: op.body?.context_type === 'critical_guard' ? '重保任务' : '维护通告', value: op.body?.notice_title || '已选择的用途' },
    { label: '文件', value: op.selected_labels?.selected_document || '' },
    ...(!editing ? [{ label: '收件人', value: (op.body?.signatures || []).map((person: Dict) => `${person.name || '所选人员'}（${({ implementer: '维护实施人', auditor: '维护审核人', inspector: '检查人' } as Dict)[person.role] || ''}）`).join('、') }] : []),
    { label: '操作', value: '发送签名使用确认，由本人批准，不代为签名' },
  ];
  if (op.api_id === 'PUT /api/drills/{drill_id}/configuration') {
    return editing ? [] : [
      { label: '演练', value: op.selected_labels?.name || '所选演练模板' },
      { label: '记录工作表', value: op.selected_labels?.record_sheet || '未选择' },
      { label: '评估工作表', value: op.selected_labels?.assessment_sheet || '未选择' },
      { label: '步骤签名人数', value: op.selected_labels?.step_slots || '保留原人数' },
      { label: '操作', value: '保存模板配置为草稿，不发布演练' },
    ];
  }
  if (op.api_id === 'POST /api/cabinet-power/batches' && op.body?.source === 'text') {
    return editing ? [] : [
      { label: '来源', value: op.selected_labels?.sources || '粘贴文本' },
      { label: '待办', value: op.selected_labels?.rows || `${op.body.rows?.length || 0} 条机柜记录` },
      { label: '操作', value: '创建待办，尚不写入正式台账' },
    ];
  }
  if (['POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/apply', 'POST /api/cabinet-power/batches/{batch_id}/images/{image_id}/correct'].includes(op.api_id)) {
    if (editing) return [];
    const correct = op.api_id.endsWith('/correct');
    const names: Dict = { expected: '期望完成时间', actual: '实际完成时间', action: '操作类型', result: '结果', failure_reason: '失败原因', supplier_rack: '供应商机柜号', rack_type: '机柜类型', type_detail: '类型明细' };
    return [
      { label: '截图', value: op.selected_labels?.selected_document || '所选截图' },
      { label: '机柜', value: op.selected_labels?.row_id || '所选机柜' },
      { label: '操作', value: correct ? '新增本批待办并关联截图，尚不写入台账' : op.body?.attach === false ? '移除本柜截图关联' : '关联本柜截图' },
      ...Object.entries(op.body?.fields || {}).filter(([key]) => !correct || !['scope', 'room', 'rack'].includes(key)).map(([key, value]) => ({ label: names[key] || key, value: value === '' ? (correct ? '未填写' : '清空') : String(value) })),
    ];
  }
  if (op.selected_labels?.plan_detail) return [{ label: '屏蔽明细', value: op.selected_labels.plan_detail }];
  const labels: Dict = { scope: '楼栋范围', action: '操作', work_type: '通告类型', title: '名称', name: '名称', source_record_id: '源表事项', target_record_id: '目标记录', record_id: '记录', start_time: '开始时间', end_time: '结束时间', expected: '期望时间', actual: '实际时间', content: '内容', reason: '原因', impact: '影响', progress: '进度', version: '数据版本', manual_binding_choice: '计划通告关联', signatures: '签署人员', signature_time: '演练审核人签名时间', evaluation_time: '演练评估人评估时间', commander: '指挥人', evaluator: '评估人', participants: '参演人', step_signers: '步骤执行人', cells: '检查表填写', generate_image: '生成检查文件', date: '日期', recipient_open_ids: '收件人', statistic_date: '统计日期', meter: '水表', meter_value: '水表数值', frequency: '统计频次', shift: '班次', abnormal_note: '异常原因', large_change_confirmed: '异常已核对', corrected_usage: '修正耗水量', retained_image_ids: '保留图片', upload_ids: '上传图片' };
  const valueLabels: Dict = { start: '开始', update: '更新', end: '结束', maintenance: '维保', change: '变更', event: '事件', repair: '检修', power: '上下电', polling: '轮巡', adjustment: '调整', bind: '绑定计划通告', unbound: '作为独立通告' };
  const result: { label: string; value: string }[] = [];
  if (op.selected_labels?.selected_document) result.push({ label: '截图', value: op.selected_labels.selected_document });
  if (op.selected_labels?.recipient_names) result.push({ label: '收件人', value: op.selected_labels.recipient_names });
  const add = (object: Dict) => {
    for (const [key, value] of Object.entries(object || {})) {
      if (value == null || value === '' || /operation_id|token|secret|password|auth|api_key|^_|command_format|manual_id|manual_binding_required|^manual$|^version$|_version$/.test(key)) continue;
      if (['fields', 'form', 'draft', 'patch'].includes(key) && typeof value === 'object' && !Array.isArray(value)) { add(value as Dict); continue; }
      let text = typeof value === 'object' ? (value.$result ? `使用第${Number(value.$result.step) + 1}步的处理结果` : value.$reference ? '已选人员' : JSON.stringify(value)) : String(value);
      if (key === 'cells' && typeof value === 'object' && !Array.isArray(value)) {
        const checks = Object.values(value.checks || {}) as Dict[];
        text = [value.check_date, checks.length ? `${checks.length} 项检查 · ${checks.filter(item => item.status === 'abnormal').length} 项异常` : '保留原清单文件'].filter(Boolean).join(' · ');
      } else if (['signatures', 'participants'].includes(key) && Array.isArray(value)) text = value.map(person => person.name || person.display_name || '已选人员').join('、') || '未选择';
      else if (['fields', 'checkboxes', 'cell_edits', 'items'].includes(key) && Array.isArray(value)) text = `${value.length} 项`;
      else if (key === 'sources' && Array.isArray(value)) text = `${value.length} 段粘贴内容`;
      else if (key === 'rows' && Array.isArray(value)) text = `${value.length} 条记录`;
      else if (['retained_image_ids', 'upload_ids'].includes(key) && Array.isArray(value)) text = `${value.length} 张`;
      else if (['commander', 'evaluator'].includes(key)) text = value.name || '未选择';
      else if (key === 'step_signers') text = `${Object.keys(value).length} 步 · ${Object.values(value).reduce((count: number, people: any) => count + (people || []).filter(Boolean).length, 0)} 个签名位已选`;
      else if (typeof value === 'boolean') text = value ? '是' : '否';
      const planLabel: Dict = { id: '规则集/屏蔽记录', block_id: '屏蔽记录', record_id: '检修核对范围', scenarios: '场景工作表' };
      if (op.api_id?.includes('/api/plan-convergence/') && planLabel[key]) {
        result.push({ label: planLabel[key], value: op.selected_labels?.[key] || '待选择' });
      } else result.push({ label: labels[key] || ({ items: '检查/规则项', batch_id: '待办批次', sources: '粘贴内容', rows: '匹配记录', row_ids: '所选机柜', all: '整批操作' } as Dict)[key] || key, value: op.selected_labels?.[key] || (['action', 'work_type', 'manual_binding_choice'].includes(key) ? valueLabels[text] || text : text) });
    }
  };
  add(op.path_params); add(op.params); if (!editing) add(op.body);
  const fileCount = Object.values(op.files || {}).reduce<number>((count, files) => count + (files as unknown[]).length, 0);
  if (fileCount && !editing) result.push({ label: '附件', value: (op.selected_files || []).map((file: Dict) => file.name).join('、') || `${fileCount} 个文件` });
  return result;
}
async function planCall(turn: Dict, suffix: string, method: string, payload: Dict): Promise<void> {
  if (isPlanBusy(turn.plan)) return;
  planBusy[turn.plan.id] = true; delete planErrors[turn.plan.id];
  try {
    const plan = await call('plans/' + turn.plan.id + suffix, method, payload, 180000);
    if (disposed) return;
    if (payload.action === 'planned-reset' || turn.plan.planned_notice?.stage !== 'preview' && plan.planned_notice?.stage === 'preview') {
      for (const key of Object.keys(planValues)) if (key.startsWith(plan.id + ':')) delete planValues[key];
    }
    if (payload.action === 'edit') for (const field of plan.fields || []) planValues[plan.id + ':' + field.name] = field.value === undefined ? '' : JSON.parse(JSON.stringify(field.value));
    replacePlan(plan);
  }
  catch (e) { if (!disposed) planErrors[turn.plan.id] = e instanceof Error ? e.message : '操作未完成，填写已保留'; }
  finally { delete planBusy[turn.plan.id]; if (!disposed) scheduleRead(); }
}
function amendPlan(turn: Dict): Promise<void> { const values: Dict = {}; for (const field of turn.plan.fields || []) values[field.name] = planValues[turn.plan.id + ':' + field.name]; return planCall(turn, '', 'PATCH', { version: turn.plan.version, values }); }
async function resetPlannedNotice(turn: Dict): Promise<void> {
  await planCall(turn, '', 'PATCH', { version: turn.plan.version, action: 'planned-reset', reset_confirmed: true });
  plannedReset[turn.plan.id] = false;
}
function editPlan(turn: Dict): Promise<void> { return planCall(turn, '', 'PATCH', { version: turn.plan.version, action: 'edit' }); }
function confirmPlan(turn: Dict): Promise<void> { return planCall(turn, '/confirm', 'POST', { version: turn.plan.version, stage: turn.plan.status === 'awaiting_second_confirmation' ? 'execute' : 'review' }); }
function cancelPlan(turn: Dict): Promise<void> { return planCall(turn, '/cancel', 'POST', {}); }
function retryPlan(turn: Dict): Promise<void> { return planCall(turn, '/retry', 'POST', { version: turn.plan.version }); }
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
    skillsOpen.value = false;
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
  const target = active && canEditModel(active) ? active : models.find(canEditModel);
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
  if (!canEditModel(m)) return;
  editProfile.value = { id: String(m.id || ''), name: String(m.name || ''), endpoint: String(m.endpoint || ''), model: String(m.model || ''), api_key: '', shared: !!m.shared };
  editingExisting.value = true;
}
function canEditModel(m: Dict): boolean { return !m.shared || !!modelSettings.value.can_manage_shared; }
function startAdd(shared = false): void {
  if (shared && !modelSettings.value.can_manage_shared) return;
  editProfile.value = { id: (shared ? 'shared_' : '') + newModelId(), name: '', endpoint: '', model: '', api_key: '', shared };
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
  if (action.kind === 'add') { startAdd(!!action.shared); return; }
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
function addStart(shared = false): void {
  beginEditAction({ kind: 'add', shared });
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
    const result = await call('settings', 'PUT', { action: 'upsert', profile, scope: p.shared ? 'shared' : 'personal' });
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
  if (busy.value || !canEditModel(m)) return;
  deleteTarget.value = m; deleteModelOpen.value = true;
}
async function confirmDeleteModel(yes: boolean): Promise<void> {
  deleteModelOpen.value = false;
  const target = deleteTarget.value; deleteTarget.value = null;
  if (!yes || !target || busy.value) return;
  settingBusy.value = true; settingsError.value = '';
  try {
    const result = await call('settings', 'PUT', { action: 'delete', id: target.id, scope: target.shared ? 'shared' : 'personal' });
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
    replaceState(result); draft.value = ''; selectedCommands.value = []; error.value = '';
    for (const file of [...draftFiles.value]) removeDraftFile(file.localId);
    for (const key of Object.keys(planValues)) delete planValues[key];
    for (const key of Object.keys(planOptionStates)) delete planOptionStates[key];
    for (const key of Object.keys(repairPrefills)) delete repairPrefills[key];
    for (const key of Object.keys(planFiles)) delete planFiles[key];
    saveDrafts();
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
function safeSourceLink(value: unknown): string {
  const href = String(value || '');
  if (isSafeLocalPath(href)) return href;
  try {
    if (/[\\\u0000-\u0020\u007f]/.test(href) || href.length > 4000) return '';
    const url = new URL(href);
    return ['https:', 'http:'].includes(url.protocol) && !url.username && !url.password ? url.href : '';
  } catch { return ''; }
}
function openSource(event: MouseEvent, value: unknown): void {
  if (!safeSourceLink(value)) { event.preventDefault(); return; }
  if (!isSafeLocalPath(String(value || ''))) return;
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
function shapeKey(shape: 'launcher' | 'panel'): string {
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
function loadStoredPos(shape: 'launcher' | 'panel'): { x: number; y: number } | null {
  return readPos(shapeKey(shape));
}
function saveStoredPos(shape: 'launcher' | 'panel', x: number, y: number): void {
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
  const maxY = Math.max(MARGIN, window.innerHeight - h - (window.innerWidth <= 700 ? 100 : MARGIN));
  return { x: Math.min(Math.max(x, MARGIN), maxX), y: Math.min(Math.max(y, MARGIN), maxY) };
}
function panelSize(): { w: number; h: number } {
  const el = root.value;
  if (panelResizing.value) return expectedPanelSize();
  return open.value && el ? { w: el.offsetWidth || PANEL_W, h: el.offsetHeight || PANEL_H } : expectedPanelSize();
}
function applyRootPos(): void {
  const el = root.value;
  if (!el) return;
  const p = panelPos;
  if (dragging) {
    el.style.transform = `translate3d(${p.x - dragOriginX}px, ${p.y - dragOriginY}px, 0)`;
    return;
  }
  el.style.transform = '';
  el.style.left = `${p.x}px`;
  el.style.top = `${p.y}px`;
  el.style.right = 'auto';
  el.style.bottom = 'auto';
  posAppliedPanel = true;
}
function resetRootToCssDefault(): void {
  const el = root.value;
  if (!el) return;
  el.style.left = '';
  el.style.top = '';
  el.style.right = '';
  el.style.bottom = '';
}
function applyPanelPosition(): void {
  const el = root.value;
  if (!el) return;
  placeBesideLauncher();
  if (posAppliedPanel) applyRootPos();
  else resetRootToCssDefault();
  ensureInBounds();
}
function placeBesideLauncher(): void {
  const actual = launcher.value?.getBoundingClientRect();
  if (!actual || window.innerWidth <= 700) { panelWidthLimit.value = null; return; }
  const size = shownAppearance.value.size;
  const stored = !shownAppearance.value.snap_back ? botPosition.value : null;
  const left = stored ? Math.max(MARGIN, Math.min(stored.x, window.innerWidth - size - MARGIN)) : actual.left;
  const top = stored ? Math.max(MARGIN, Math.min(stored.y, window.innerHeight - size - MARGIN)) : actual.top;
  const right = left + size, gap = 12;
  const leftSpace = Math.max(0, left - gap - MARGIN), rightSpace = Math.max(0, window.innerWidth - right - gap - MARGIN);
  panelWidthLimit.value = Math.max(leftSpace, rightSpace);
  const { w, h } = expectedPanelSize();
  const retained = clampPosForSize(panelPos.x, panelPos.y, w, h);
  // Keep manually placed panels only when they are still wholly beside the bot.
  if (posAppliedPanel && (retained.x + w <= left - gap || retained.x >= right + gap)) panelPos = retained;
  else panelPos = clampPosForSize(leftSpace >= rightSpace ? left - gap - w : right + gap, top + size / 2 - h / 2, w, h);
  applyRootPos();
}
function ensureInBounds(): void {
  const el = root.value;
  if (!el || disposed || !open.value) return;
  const { w, h } = panelSize();
  const rect = el.getBoundingClientRect();
  const origin = panelResizing.value ? panelPos : { x: rect.left, y: rect.top };
  const next = clampPosForSize(origin.x, origin.y, w, h);
  panelPos = next;
  applyRootPos();
  saveStoredPos('panel', next.x, next.y);
}
watch(() => [appearance.value.size, appearanceOpen.value], () => { if (open.value) void nextTick(applyPanelPosition); });
function onWindowResize(): void {
  desktopNotices.value = window.innerWidth >= 1000;
  if (dragging) stopGesture();
  if (open.value) applyPanelPosition();
}
function currentOrigin(): { x: number; y: number } {
  if (posAppliedPanel) return { ...panelPos };
  const el = root.value;
  const rect = el?.getBoundingClientRect();
  return { x: rect ? rect.left : 0, y: rect ? rect.top : 0 };
}
function beginDrag(target: HTMLElement, pointerId: number, clientX: number, clientY: number): void {
  if (dragging || !open.value) return;
  dragging = true;
  dragActive.value = true; panelResizing.value = false; window.clearTimeout(panelResizeTimer); ++panelResizeSerial;
  dragPointerId = pointerId;
  dragStartClientX = clientX;
  dragStartClientY = clientY;
  const origin = currentOrigin();
  dragOriginX = origin.x;
  dragOriginY = origin.y;
  dragSize = panelSize();
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
      if (!dragging || !open.value) return;
      const { w, h } = dragSize;
      const p = panelPos;
      const next = clampPosForSize(dragLatestX, dragLatestY, w, h);
      p.x = next.x;
      p.y = next.y;
      applyRootPos();
    });
  }
}
function onDragEnd(event: PointerEvent): void {
  if (!dragging || event.pointerId !== dragPointerId) return;
  if (dragRaf) { window.cancelAnimationFrame(dragRaf); dragRaf = 0; }
  if (open.value) {
    const { w, h } = dragSize;
    // 按最后一次事件坐标同步提交，避免 rAF 尚未执行而丢失末帧。
    const p = panelPos;
    const next = event.type === 'pointerup'
      ? clampPosForSize(dragOriginX + (event.clientX - dragStartClientX), dragOriginY + (event.clientY - dragStartClientY), w, h)
      : clampPosForSize(dragLatestX, dragLatestY, w, h);
    p.x = next.x;
    p.y = next.y;
    saveStoredPos('panel', p.x, p.y);
  }
  dragging = false;
  applyRootPos();
  dragActive.value = false;
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
  startPress(event);
}
function onDragCaptureLost(event: PointerEvent): void {
  if (dragging && event.pointerId === dragPointerId) onDragEnd(event);
  else if (event.pointerId === press?.id) cancelPress();
}
function onHeaderKeydown(event: KeyboardEvent): void {
  if (isInteractiveTarget(event.target)) return;
  nudgeByArrow(event);
}
function startPress(event: PointerEvent): void {
  if (dragging || press || !event.isPrimary || (event.pointerType === 'mouse' && event.button !== 0)) return;
  press = { target: event.currentTarget as HTMLElement, id: event.pointerId, x: event.clientX, y: event.clientY };
  try { press.target.setPointerCapture(event.pointerId); } catch { /* unsupported */ }
  pressTimer = window.setTimeout(() => startPressDrag(), 350);
  window.addEventListener('pointermove', onPressMove, { passive: false });
  window.addEventListener('pointerup', onPressEnd, true);
  window.addEventListener('pointercancel', onPressEnd, true);
  event.preventDefault();
}
function startPressDrag(): void {
  if (!press) return;
  const gesture = press;
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
function nudgeByArrow(event: KeyboardEvent): void {
  const step = event.shiftKey ? 40 : 16;
  let dx = 0, dy = 0;
  if (event.key === 'ArrowLeft') dx = -step;
  else if (event.key === 'ArrowRight') dx = step;
  else if (event.key === 'ArrowUp') dy = -step;
  else if (event.key === 'ArrowDown') dy = step;
  else return;
  event.preventDefault();
  const { w, h } = panelSize();
  const origin = currentOrigin();
  const next = clampPosForSize(origin.x + dx, origin.y + dy, w, h);
  const p = panelPos;
  p.x = next.x;
  p.y = next.y;
  applyRootPos();
  saveStoredPos('panel', p.x, p.y);
}

onMounted(async () => {
  window.addEventListener('pagehide', saveDrafts);
  document.addEventListener('visibilitychange', resumeStreamRendering);
  let restoreOpen = false;
  try { restoreOpen = window.sessionStorage.getItem(storageKey() + ':open') === 'true'; } catch { /* private mode */ }
  await nextTick();
  migrateLegacyPos();
  botPosition.value = loadStoredPos('launcher');
  void loadAppearance();
  const pp = loadStoredPos('panel');
  if (pp) {
    panelPos = pp;
    posAppliedPanel = true;
  }
  // Clamp only the visible shape, using its real dimensions after layout.
  await nextTick();
  if (restoreOpen) {
    placeBesideLauncher(); open.value = true;
    await nextTick(); applyPanelPosition();
  }
  window.addEventListener('resize', onWindowResize);
  window.addEventListener('blur', stopGesture);
  document.addEventListener('click', onPageInteraction);
  document.addEventListener('focusin', onPageFocus);
  document.addEventListener('focusout', onPageBlur);
  document.addEventListener('toggle', onNativeToggle, true);
  refreshNativeBlock();
  overlayObserver = new MutationObserver(records => {
    if (records.some(record => record.type === 'attributes' && record.target instanceof HTMLDialogElement
      || [...record.addedNodes, ...record.removedNodes].some(node => node instanceof Element && (node.matches('dialog') || node.querySelector('dialog'))))) {
      const opened = [...records].reverse().find(record => record.type === 'attributes' && record.target instanceof HTMLDialogElement && record.target.open);
      refreshNativeBlock(opened?.target as HTMLDialogElement | undefined);
    }
  });
  overlayObserver.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['open'] });
  if (open.value) await readState(true, true);
});
onBeforeUnmount(() => {
  saveDrafts();
  window.removeEventListener('pagehide', saveDrafts);
  document.removeEventListener('visibilitychange', resumeStreamRendering);
  disposed = true;
  disconnectStream();
  stopGesture();
  for (const file of draftFiles.value) if (file.preview) URL.revokeObjectURL(file.preview);
  ++settingsCallSerial;
  if (readController) { readController.abort(); readController = null; }
  if (settingsController) { settingsController.abort(); settingsController = null; }
  appearanceController?.abort();
  overlayObserver?.disconnect();
  nativeDialogObserver?.disconnect();
  document.removeEventListener('click', onPageInteraction);
  document.removeEventListener('focusin', onPageFocus);
  document.removeEventListener('focusout', onPageBlur);
  document.removeEventListener('toggle', onNativeToggle, true);
  window.clearTimeout(interactionTimer); window.clearTimeout(feedbackTimer);
  readInFlight = false;
  window.clearTimeout(timer);
  window.clearTimeout(panelResizeTimer);
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
  --lh-muted: #c2cdc5;
  --lh-faint-muted: #b3bfb6;
  --lh-border: color-mix(in srgb, var(--bot-color) 25%, #526259);
  --lh-border-strong: color-mix(in srgb, var(--bot-color) 32%, #6b7b73);
  --lh-surface: color-mix(in srgb, var(--bot-color) 12%, #252c2a);
  --lh-surface-subtle: color-mix(in srgb, var(--bot-color) 14%, #262f2a);
  --lh-surface-hover: color-mix(in srgb, var(--bot-color) 14%, #242e29);
  --lh-accent: color-mix(in srgb, var(--bot-color) 42%, #e9edea);
  --lh-accent-strong: color-mix(in srgb, var(--bot-color) 32%, #fafdfb);
  --lh-accent-soft: color-mix(in srgb, var(--bot-color) 34%, #26342d);
  --lh-accent-ring: color-mix(in srgb, var(--lh-accent) 25%, transparent);
  --lh-action: color-mix(in srgb, var(--bot-color) 15%, #08917c);
  --lh-action-hover: color-mix(in srgb, var(--bot-color) 10%, #087c6b);
  --lh-danger: #eea99b;
  --lh-danger-soft: #47342f;
  --lh-warn: #dec58f;
  --lh-warn-soft: #443d2e;
  --lh-input-border: var(--lh-border-strong);
  position: fixed;
  right: 100px;
  bottom: 24px;
  z-index: 10000;
  color: var(--lh-charcoal);
  font-size: 14px;
  letter-spacing: 0;
  color-scheme: dark;
}
.lighthouse *, .lighthouse *::before, .lighthouse *::after { box-sizing: border-box; }
.lighthouse { pointer-events: auto; }
.lighthouse { display: flex; align-items: stretch; }
.assistant-shell { display: flex; min-width: 0; height: min(var(--notice-panel-height, 700px), calc(100dvh - 48px)); overflow: hidden; border: 1px solid var(--lh-border); border-radius: var(--lh-panel-radius, 12px); background: var(--lh-surface); box-shadow: var(--lh-shadow, 0 14px 40px #181e2633); transition: height 320ms cubic-bezier(.22, 1, .36, 1); }
.assistant-shell > .assistant-panel { flex: none; height: 100%; max-width: calc(100vw - 50px); border: 0; border-radius: 0; box-shadow: none; }
.assistant-sidebar { flex: none; width: 0; min-width: 0; overflow: hidden; visibility: hidden; transition: width 320ms cubic-bezier(.22, 1, .36, 1), visibility 0s linear 320ms; }
.assistant-sidebar.expanded { width: var(--notice-panel-width, 360px); visibility: visible; transition-delay: 0s; }
.assistant-sidebar :deep(.notice-panel) { height: 100%; max-height: none; border: 0; border-right: 1px solid var(--lh-border); border-radius: 0; box-shadow: none; opacity: 0; transform: translateX(-8px); transition: opacity 180ms ease, transform 320ms cubic-bezier(.22, 1, .36, 1); }
.assistant-sidebar.expanded :deep(.notice-panel) { opacity: 1; transform: translateX(0); }
@media (max-width: 700px) { .assistant-shell { height: min(var(--notice-panel-height, 700px), calc(100dvh - max(112px, env(safe-area-inset-bottom) + 88px))); } }
@media (prefers-reduced-motion: reduce) { .assistant-shell, .assistant-sidebar, .assistant-sidebar :deep(.notice-panel) { transition: none; } }
.lighthouse :deep(.confirm-backdrop) { background: rgba(12, 18, 14, .5); backdrop-filter: blur(4px); }
.lighthouse :deep(.confirm-modal) { background: var(--lh-surface); border-color: var(--lh-border-strong); border-radius: 12px; color: var(--lh-charcoal); box-shadow: 0 20px 70px rgba(0, 0, 0, .3); }
.lighthouse :deep(.confirm-content header strong) { color: var(--lh-charcoal-strong); }
.lighthouse :deep(.confirm-content p), .lighthouse :deep(.confirm-content header span) { color: var(--lh-muted); }
.lighthouse :deep(.confirm-icon) { background: var(--lh-accent-soft); color: var(--lh-accent); box-shadow: none; }
.lighthouse :deep(.confirm-close), .lighthouse :deep(.confirm-content .btn.ghost) { background: var(--lh-surface-subtle); border-color: var(--lh-border-strong); color: var(--lh-charcoal); }
.lighthouse :deep(.confirm-content .btn:not(.ghost)) { background: var(--lh-action); border-color: var(--lh-action); color: #fff; }
.lighthouse :deep(.confirm-content .btn:not(.ghost):not(.danger):hover:not(:disabled)) { background: var(--lh-action-hover); }
.lighthouse :deep(.confirm-content .btn.danger) { background: var(--lh-danger); border-color: var(--lh-danger); color: #30201c; }
.lighthouse :deep(.async-page-state) { background: var(--lh-surface-subtle); color: var(--lh-charcoal); border-color: var(--lh-border-strong); box-shadow: none; }
.lighthouse :deep(.async-page-state strong) { color: var(--lh-charcoal-strong); }
.lighthouse :deep(.async-page-state p) { color: var(--lh-muted); }
.lighthouse :deep(.async-page-state .btn) { background: var(--lh-surface-hover); color: var(--lh-charcoal); border-color: var(--lh-border-strong); }
.lighthouse :deep(.repair-field-control input:not([type=range])), .lighthouse :deep(.repair-field-control textarea),
.lighthouse :deep(.repair-people-search), .lighthouse :deep(.repair-people-popover) {
  background: var(--lh-surface-subtle); color: var(--lh-charcoal); border-color: var(--lh-input-border);
}
.lighthouse :deep(.repair-field-control input:disabled), .lighthouse :deep(.repair-field-control textarea:disabled) { color: var(--lh-faint-muted); }
.lighthouse :deep(.repair-field-label), .lighthouse :deep(.repair-people-label) { color: var(--lh-charcoal); }
.lighthouse :deep(.repair-field-label i), .lighthouse :deep(.repair-field-error), .lighthouse :deep(.repair-people-state.failed) { color: var(--lh-danger); }
.lighthouse :deep(.repair-field-label span), .lighthouse :deep(.repair-person-info small), .lighthouse :deep(.repair-people-state) { color: var(--lh-muted); }
.lighthouse :deep(.repair-people-search input) { background: transparent; color: var(--lh-charcoal); }
.lighthouse :deep(.repair-person-info b) { color: var(--lh-charcoal); }
.lighthouse :deep(.repair-people-search input::placeholder) { color: var(--lh-faint-muted); }
.lighthouse :deep(.repair-field-control input:focus), .lighthouse :deep(.repair-field-control textarea:focus), .lighthouse :deep(.repair-people-search.active) {
  border-color: var(--lh-accent); box-shadow: 0 0 0 3px var(--lh-accent-ring);
}
.lighthouse :deep(.repair-person-chip), .lighthouse :deep(.repair-person-avatar), .lighthouse :deep(.repair-selected-toggle), .lighthouse :deep(.repair-people-load-more) {
  background: var(--lh-accent-soft); color: var(--lh-accent-strong); border-color: var(--lh-border-strong);
}
.lighthouse :deep(.repair-person-chip button), .lighthouse :deep(.date-control-button) { color: var(--lh-muted); }
.lighthouse :deep(.repair-people-results > button) { color: var(--lh-charcoal); }
.lighthouse :deep(.repair-people-results > button:hover), .lighthouse :deep(.repair-people-results > button.active),
.lighthouse :deep(.repair-person-chip button:hover), .lighthouse :deep(.repair-selected-toggle:hover), .lighthouse :deep(.date-control-button:hover) { background: var(--lh-surface-hover); color: var(--lh-accent-strong); }
.lighthouse :deep(.repair-people-results > button.selected) { background: var(--lh-accent-soft); color: var(--lh-accent-strong); }
.lighthouse :deep(.repair-people-results > button.active) { outline-color: var(--lh-accent); }
.lighthouse :deep(.repair-person-unavailable), .lighthouse :deep(.repair-people-result-note) { color: var(--lh-warn); }
.lighthouse :deep(.repair-percentage-control) {
  --repair-progress-color: var(--lh-accent); --repair-progress-soft: var(--lh-accent-soft);
  background: var(--lh-surface-subtle); border-color: var(--lh-input-border);
}
.lighthouse :deep(.repair-percentage-control.is-idle) { --repair-progress-color: var(--lh-muted); --repair-progress-soft: var(--lh-surface-hover); }
.lighthouse :deep(.repair-percentage-control.is-near) { --repair-progress-color: var(--lh-warn); --repair-progress-soft: var(--lh-warn-soft); }
.lighthouse :deep(.percentage-range) { --repair-progress-track: var(--lh-border-strong); }
.lighthouse :deep(.percentage-readonly-track) { background: var(--lh-border-strong); }
.lighthouse :deep(.percentage-number-wrap .percentage-output) { background: var(--lh-surface-subtle); }
@media print { .lighthouse, .lighthouse-launcher, .lighthouse-layer { display: none !important; } }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 8px 12px; min-height: 34px; font: inherit; border: 1px solid var(--lh-border-strong); border-radius: 8px; color: var(--lh-charcoal); background: var(--lh-surface); cursor: pointer; }
button:hover:not(:disabled) { background: var(--lh-surface-hover); }button:disabled { opacity: .5; cursor: not-allowed; }button:focus-visible, textarea:focus-visible, input:focus-visible, a:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
.lighthouse-layer { position: fixed; inset: 0; margin: 0; padding: 0; border: 0; background: transparent; overflow: visible; pointer-events: none; z-index: 10020; }
.lighthouse-launcher { position: fixed; display: flex; inset: auto 24px max(24px, env(safe-area-inset-bottom)) auto; margin: 0; padding: 0; border: 0; background: transparent; overflow: visible; z-index: 10200; pointer-events: none; }
.lighthouse-launcher { view-transition-name: lighthouse-bot; }
:global(::view-transition-group(lighthouse-bot)) { animation-duration: 360ms; animation-timing-function: cubic-bezier(.22, 1, .36, 1); }
:global(::view-transition-old(lighthouse-bot)), :global(::view-transition-new(lighthouse-bot)) { animation-duration: 240ms; }
.lighthouse-layer::backdrop, .lighthouse-launcher::backdrop { pointer-events: none; background: transparent; }
.assistant-launcher {
  border-radius: 50%;
  padding: 0;
  min-height: 0;
  background: transparent;
  border: 0;
  pointer-events: auto;
  touch-action: none;
  user-select: none;
  cursor: grab;
}
.assistant-launcher:hover:not(:disabled) { background: transparent; }
.assistant-launcher:focus-visible { outline: 2px solid #397554; outline-offset: 0; }
.assistant-launcher:active { cursor: grabbing; }
@media (max-width: 700px) { .lighthouse { right: 24px; } }
.assistant-panel { --lh-panel-height: 700px; width: min(620px, var(--lh-panel-max-width, 620px), calc(100vw - 48px)); height: min(var(--lh-panel-height), calc(100dvh - max(48px, env(safe-area-inset-bottom) + 24px))); transition: width 320ms cubic-bezier(.22, 1, .36, 1), height 320ms cubic-bezier(.22, 1, .36, 1); display: flex; flex-direction: column; border: 1px solid var(--lh-border-strong); border-radius: 12px; background: var(--lh-surface); box-shadow: 0 14px 40px rgba(24, 30, 38, 0.2); overflow: hidden; }
.assistant-panel.expanded { --lh-panel-height: 820px; width: min(760px, var(--lh-panel-max-width, 760px), calc(100vw - 48px)); }
@media (max-width: 700px) { .lighthouse { bottom: max(100px, calc(env(safe-area-inset-bottom) + 76px)); } .assistant-panel, .assistant-panel.expanded { height: min(var(--lh-panel-height), calc(100dvh - max(112px, env(safe-area-inset-bottom) + 88px))); } }
.lighthouse.resizing { transition: left 320ms cubic-bezier(.22, 1, .36, 1), top 320ms cubic-bezier(.22, 1, .36, 1); }
.lighthouse.drag-active, .lighthouse.drag-active .assistant-panel { transition: none; }
@media (prefers-reduced-motion: reduce) { .assistant-panel, .lighthouse.resizing { transition: none; } }
@media (min-width: 800px) { .assistant-panel.expanded .plan-form { grid-template-columns: repeat(2, minmax(0, 1fr)); } .assistant-panel.expanded .wide-field, .assistant-panel.expanded .plan-form > button { grid-column: 1 / -1; } }
.assistant-header { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 12px 14px; border-bottom: 1px solid var(--lh-border); background: var(--lh-surface-subtle); cursor: grab; touch-action: none; user-select: none; }
.assistant-header:active { cursor: grabbing; }
.notice-visible .assistant-header { flex-wrap: wrap; }
.notice-visible .tools { flex-wrap: wrap; margin-left: auto; }
.assistant-header:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: -2px; border-radius: 10px 10px 0 0; }
.panel-title, .tools { display: flex; align-items: center; gap: 6px; }
.panel-title { color: var(--lh-charcoal-strong); }
.assistant-mark, .panel-heading { display: contents; }
.model-chevron { display: none; }
.panel-title svg { color: var(--lh-accent); }
.panel-title h2 { font-size: 16px; margin: 0; }
.header-status { display: inline-flex; align-items: center; gap: 5px; color: var(--lh-muted); font-size: 11px; margin-left: 4px; }
.header-status::before { content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--lh-accent); }
.header-status.active::before { animation: status-pulse 1.4s ease-in-out infinite; }
.header-status.warning { color: var(--lh-warn); }
.header-status.warning::before { background: var(--lh-warn); }
@keyframes status-pulse { 50% { opacity: .35; } }
.icon { padding: 6px; width: 32px; height: 32px; min-height: 32px; border-color: transparent; background: transparent; touch-action: manipulation; }
.tools button { border-color: transparent; }
.tools button:hover:not(:disabled) { background: var(--lh-surface-hover); }
.thread { flex: 1; min-height: 0; overflow-y: auto; overscroll-behavior: contain; padding: 20px; scrollbar-gutter: stable; }.empty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; min-height: 240px; color: var(--lh-muted); }.empty p { margin: 0; }.muted { color: var(--lh-faint-muted); font-size: 13px; }
.turn { margin-bottom: 20px; content-visibility: auto; contain-intrinsic-size: auto 260px; }
.turn:focus-within, .turn.is-pending { content-visibility: visible; }
.active-tasks { display: flex; align-items: center; gap: 8px; padding: 8px 14px; border-bottom: 1px solid var(--lh-input-border); overflow-x: auto; flex: none; font-size: 12px; }
.active-tasks > span { white-space: nowrap; color: var(--lh-muted); }
.active-tasks button { display: inline-flex; align-items: center; gap: 5px; max-width: 220px; flex: none; padding: 5px 8px; }
.active-tasks button span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.rotate-chevron { transform: rotate(90deg); }
.message-row { display: flex; }.message-row.user { justify-content: flex-end; }.message-row.assistant { margin-top: 14px; }
.turn p { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.7; }
.bubble { max-width: 86%; padding: 9px 12px; border-radius: 12px; }.user-bubble { background: var(--lh-accent-soft); color: var(--lh-charcoal-strong); margin-left: 28px; }.user-bubble p { font-size: 13.5px; }
.answer { min-width: 0; max-width: 100%; flex: 1; color: var(--lh-charcoal); }.answer .pending { display: flex; gap: 6px; align-items: center; color: var(--lh-muted); }.used-model { margin-left: auto; min-width: 0; max-width: 55%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 11px; color: var(--lh-faint-muted); }.failure, .danger { color: var(--lh-danger); }.retry { margin-top: 8px; padding: 4px 8px; font-size: 12px; min-height: 30px; }
.sources { margin-top: 10px; font-size: 12px; color: var(--lh-muted); }.sources summary { cursor: pointer; }.sources a { display: block; margin-top: 5px; color: var(--lh-accent-strong); overflow-wrap: anywhere; text-decoration: none; }.sources a:hover { text-decoration: underline; }.answer .source-warning { font-size: 12px; color: var(--lh-warn); margin-top: 8px; }
.sources a small { display: block; color: var(--lh-muted); font-size: 11px; margin-top: 2px; }
.interactions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }.interaction { padding: 3px 9px; font-size: 12px; min-height: 30px; border-radius: 8px; color: var(--lh-accent-strong); background: var(--lh-accent-soft); max-width: 100%; overflow-wrap: anywhere; }
.assistant-error, .config-warning { flex-shrink: 0; padding: 8px 14px; font-size: 12px; line-height: 1.6; }.assistant-error { display: flex; align-items: center; gap: 6px; color: var(--lh-danger); background: var(--lh-danger-soft); }.assistant-error span { flex: 1; overflow-wrap: anywhere; }.config-warning { color: var(--lh-warn); background: var(--lh-warn-soft); }
.composer { flex-shrink: 0; border-top: 1px solid var(--lh-border); padding: 10px 12px 12px; display: flex; flex-direction: column; gap: 8px; background: var(--lh-surface); position: relative; }
.turn-commands { display: flex; flex-wrap: wrap; gap: 4px 9px; padding-bottom: 5px; color: var(--lh-muted); font-size: 11px; }
.turn-commands span { display: inline-flex; align-items: center; gap: 4px; overflow-wrap: anywhere; }
.composer textarea { font: inherit; resize: none; border: 1px solid var(--lh-input-border); border-radius: 8px; padding: 9px 11px; line-height: 1.6; min-width: 0; width: 100%; max-height: 140px; color: var(--lh-charcoal); }
.composer textarea:focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 0; }
.composer-footer { display: flex; align-items: center; justify-content: space-between; gap: 10px; min-height: 34px; }
.model-select-small { min-width: 0; flex: 0 1 auto; max-width: 220px; }
.native-model-select { height: 34px; max-width: 220px; width: 100%; padding: 0 8px; border: 1px solid var(--lh-input-border); border-radius: 8px; font: inherit; font-size: 13px; text-overflow: ellipsis; }
.send { width: 40px; height: 40px; padding: 0; flex-shrink: 0; }.send.round { border-radius: 50%; }.primary { color: #fff; border-color: var(--lh-action); background: var(--lh-action); }.primary:hover:not(:disabled) { background: var(--lh-action-hover); }
.primary:disabled { color: var(--lh-faint-muted); border-color: var(--lh-border); background: var(--lh-surface-hover); }
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
.planned-notice-selected { display: grid; gap: 7px; padding: 10px 0; border-bottom: 1px solid var(--lh-border); }
.planned-notice-selected > span { color: var(--lh-muted); font-size: 12px; }
.planned-notice-selected button { justify-self: start; display: inline-flex; align-items: center; gap: 5px; }
.planned-notice-text { margin: 12px 0; }
.planned-notice-text h4 { margin: 0 0 8px; font-size: 13px; }
.planned-notice-text pre { white-space: pre-wrap; overflow-wrap: anywhere; font: inherit; line-height: 1.65; margin: 0; }
.plan-steps { padding-left: 20px; margin: 12px 0; }
.plan-steps li + li { margin-top: 12px; }
.plan-steps dl { display: grid; grid-template-columns: minmax(60px, 100px) minmax(0, 1fr); gap: 5px 8px; margin: 8px 0; }
.plan-steps dt { color: var(--lh-muted); overflow-wrap: anywhere; }
.plan-steps dd { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
.plan-form { display: grid; gap: 10px; max-height: 460px; overflow-y: auto; overscroll-behavior: contain; padding: 2px 3px; scrollbar-gutter: stable; }
.plan-form-footer { grid-column: 1 / -1; position: sticky; bottom: -2px; z-index: 2; display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; padding: 10px 0 2px; background: var(--lh-surface, #fff); border-top: 1px solid var(--lh-input-border); }
.plan-form-footer .failure { flex-basis: 100%; }
.plan-field { display: grid; gap: 6px; }
.repair-prefill-state { display: flex; align-items: center; flex-wrap: wrap; gap: 6px; font-size: 12px; color: var(--lh-warn); overflow-wrap: anywhere; }
.repair-prefill-state:empty { display: none; }
.plan-upload { display: grid; gap: 8px; min-width: 0; }
.plan-upload > button { justify-self: start; }
.plan-upload input[type=file] { width: 1px; min-height: 0; padding: 0; border: 0; }
.plan-files { display: grid; gap: 6px; padding: 0; margin: 0; list-style: none; }
.plan-file { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 8px; min-width: 0; border-bottom: 1px solid var(--lh-border); padding: 5px 0; font-size: 12px; overflow-wrap: anywhere; }
.plan-file a { display: flex; align-items: center; gap: 8px; color: inherit; min-width: 0; text-decoration: none; }
.plan-file img { width: 36px; height: 36px; object-fit: cover; border-radius: 4px; flex-shrink: 0; }
.plan-file svg { flex-shrink: 0; }
.plan-question { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.6; }
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
.plan-single-choices { margin: 0; padding: 0; border: 0; display: flex; flex-wrap: wrap; gap: 7px 16px; }
.plan-single-choices label { display: inline-flex; align-items: center; gap: 6px; cursor: pointer; padding: 5px 0; font-size: 13px; }
.plan-single-choices label.selected { color: var(--lh-accent); font-weight: 600; }
.plan-form .plan-single-choices input { width: 15px; height: 15px; margin: 0; padding: 0; accent-color: var(--lh-accent); }
.operation-plan[data-plan-status="superseded"] { opacity: .75; }
.operation-plan :deep(.vnet-select-trigger) { min-height: 36px; font: inherit; border-radius: 6px; }
.turn .plan-risk { margin-top: 10px; padding: 9px; color: var(--lh-warn); background: var(--lh-warn-soft); border-radius: 6px; }
.plan-results { display: grid; gap: 6px; margin-top: 10px; color: var(--lh-muted); }
.plan-results > div { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
.plan-results :deep(.assistant-rich-reply) { flex-basis: 100%; color: var(--lh-charcoal-strong); }
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
  background: #ad2938;
  border: 1px solid #ad2938;
  border-radius: 8px;
  gap: 6px;
}
.composer-footer .stop:hover:not(:disabled) { background: #8c1f2c; color: #fff; }
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
@media (min-width: 701px) {
  .lighthouse {
    --lh-panel-radius: 18px;
    --lh-control-radius: 8px;
    --lh-surface: color-mix(in srgb, var(--bot-color) 10%, #24282e);
    --lh-surface-subtle: color-mix(in srgb, var(--bot-color) 10%, #2c3138);
    --lh-surface-hover: color-mix(in srgb, var(--bot-color) 10%, #353d46);
    --lh-border: color-mix(in srgb, var(--bot-color) 12%, #46505a);
    --lh-border-strong: color-mix(in srgb, var(--bot-color) 14%, #65717d);
    --lh-charcoal: #e7edf2;
    --lh-charcoal-strong: #f6f8fa;
    --lh-muted: #b8c2cd;
    --lh-faint-muted: #a8b4c0;
    --lh-action: color-mix(in srgb, var(--bot-color) 10%, #087367);
    --lh-action-hover: color-mix(in srgb, var(--bot-color) 10%, #086659);
    --lh-shadow: 0 18px 48px #10182038, 0 3px 10px #10182024;
  }
  .assistant-panel { border-radius: var(--lh-panel-radius); border-color: var(--lh-border); box-shadow: var(--lh-shadow); }
  .assistant-header { min-height: 68px; padding: 12px 16px; background: var(--lh-surface); gap: 10px; }
  .panel-title { gap: 10px; flex: 0 0 auto; }
  .assistant-mark { display: grid; place-items: center; width: 34px; height: 34px; border: 1px solid var(--lh-border); border-radius: 10px; background: var(--lh-surface-subtle); }
  .panel-heading { display: flex; flex-direction: column; align-items: flex-start; gap: 3px; }
  .panel-title h2 { font-size: 15px; line-height: 1.35; font-weight: 650; }
  .header-status { margin-left: 0; font-size: 11px; line-height: 1.35; }
  .tools { gap: 3px; }
  .tools .icon { border-radius: var(--lh-control-radius); color: var(--lh-muted); transition: background 160ms ease, color 160ms ease; }
  .tools .icon:hover:not(:disabled), .tools .icon[aria-pressed="true"] { color: var(--lh-charcoal-strong); background: var(--lh-surface-hover); }
  .tools .icon[aria-pressed="true"] { box-shadow: inset 0 0 0 1px var(--lh-border); }
  .thread { padding: 22px 22px 8px; scrollbar-width: thin; scrollbar-color: var(--lh-border-strong) transparent; }
  .turn { padding-bottom: 10px; margin-bottom: 22px; }
  .user-bubble { max-width: 88%; padding: 11px 15px; border-radius: 14px 14px 4px 14px; background: var(--lh-surface-subtle); border: 1px solid var(--lh-border); }
  .user-bubble p { font-size: 14px; }
  .message-row.assistant { margin-top: 18px; }
  .answer-heading { display: flex; align-items: center; gap: 7px; color: var(--lh-charcoal-strong); font-size: 12px; font-weight: 600; margin-bottom: 10px; }
  .answer-heading > svg { color: var(--lh-accent); }
  .answer-state { display: inline-flex; align-items: center; gap: 5px; margin-left: auto; color: var(--lh-muted); font-weight: 400; }
  .message-actions { margin-top: 12px; gap: 5px; }
  .message-actions .icon { border-radius: 7px; }
  .answer :deep(.assistant-rich-reply th), .answer :deep(.assistant-rich-reply td) { border: 0; border-bottom: 1px solid var(--lh-border); padding: 8px 10px; }
  .answer :deep(.assistant-rich-reply th) { background: var(--lh-surface-subtle); font-size: 12px; }
  .sources { padding-top: 9px; border-top: 1px solid var(--lh-border); }
  .sources summary { min-height: 26px; line-height: 26px; }
  .process summary { min-height: 28px; }
  .composer { margin: 6px 16px 16px; padding: 12px; gap: 10px; border: 1px solid var(--lh-input-border); border-radius: 14px; background: var(--lh-surface-subtle); transition: border-color 160ms ease, box-shadow 160ms ease; }
  .composer:focus-within { border-color: var(--lh-accent); box-shadow: 0 0 0 2px var(--lh-accent-ring); }
  .composer textarea { border: 0; padding: 5px 2px; min-height: 66px; font-size: 14px; background: transparent; }
  .composer textarea:focus-visible { outline: none; }
  .composer textarea::placeholder { color: var(--lh-faint-muted); }
  .composer-footer { gap: 8px; }
  .composer-model { flex: 1; gap: 4px; }
  .model-select-small { position: relative; flex: 0 1 auto; }
  .native-model-select { appearance: none; min-height: 32px; padding: 0 28px 0 10px; background: var(--lh-surface); border-color: transparent; border-radius: var(--lh-control-radius); font-size: 12px; cursor: pointer; }
  .native-model-select:hover:not(:disabled) { border-color: var(--lh-border); }
  .model-chevron { display: block; position: absolute; right: 9px; top: 50%; transform: translateY(-50%); pointer-events: none; color: var(--lh-muted); }
  .send { width: 36px; height: 36px; }
  .composer-footer .stop { min-height: 36px; }
  .operation-plan { border-color: var(--lh-border); padding: 14px; }
  .plan-title { gap: 8px; line-height: 1.6; }
  .plan-form { gap: 13px; }
  .plan-field > label, .plan-field > span { color: var(--lh-muted); font-size: 12px; }
  .plan-form input, .plan-form textarea, .plan-form select, .operation-plan :deep(.vnet-select-trigger) { border-radius: var(--lh-control-radius); min-height: 34px; }
  .plan-form input[type=checkbox], .plan-form input[type=radio] { min-height: 0; }
  .plan-form textarea { line-height: 1.6; }
  .plan-form :is(input, textarea, select):focus-visible { outline-offset: -2px; }
  .plan-actions > .primary, .plan-form-footer > .primary { min-height: 36px; padding-inline: 16px; font-weight: 600; }
  .primary { transition: background 160ms ease, border-color 160ms ease; }
  .assistant-error, .config-warning { margin: 4px 16px; padding: 8px 10px; border-radius: 8px; }
  .lighthouse.drag-active { will-change: transform; }
}
@media (prefers-reduced-motion: reduce) { .composer, .tools .icon, .primary { transition: none; } }
@media (max-width: 700px) { .answer-heading { display: none; } }
@media (max-width: 600px) {
  .assistant-header { padding: 10px 12px; }
  .thread { padding: 14px 10px; }
  .settings-scroll { padding: 14px 14px 8px; }
  .settings-actions { padding: 10px 14px; }
  .model-select-small { max-width: none; flex: 1 1 auto; min-width: 0; }
  .composer-footer { gap: 8px; }
  .send { flex: 0 0 auto; }
  .model-card.compact-row { flex-wrap: wrap; align-items: stretch; gap: 6px 8px; }
  .model-card .list-row { flex-direction: column; align-items: flex-start; gap: 2px; min-width: 0; }
  .model-card .list-name, .model-card .list-model { max-width: 100%; overflow-wrap: anywhere; white-space: normal; }
  .model-actions { margin-left: auto; }
}
</style>
