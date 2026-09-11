<template>
  <main class="guard-page">
    <header class="guard-page__header">
      <VnetBackButton :disabled="saving" @click="requestPageExit" />
      <div>
        <span>风险管理</span>
        <h1>{{ pageTitle }}</h1>
      </div>
      <button
        v-if="viewMode !== 'landing'"
        type="button"
        class="icon-button"
        :disabled="loading || saving"
        title="刷新"
        @click="requestReload"
      >
        <RefreshCw :size="18" :class="{ spinning: loading }" />
      </button>
    </header>

    <section v-if="loading && !bootstrap" class="page-state">
      <span class="loader"></span>
      正在读取重保任务
    </section>

    <section v-else-if="error && !bootstrap" class="page-state error" role="alert">
      <AlertTriangle :size="22" />
      <strong>{{ error }}</strong>
      <button type="button" @click="loadBootstrap">重新读取</button>
    </section>

    <template v-else>
      <section v-if="error" class="inline-message error" role="alert">
        {{ error }}
      </section>
      <section v-if="message" class="inline-message" :class="messageTone" role="status">
        {{ message }}
      </section>

      <section v-if="viewMode === 'landing'" class="landing-view">
        <div class="landing-title">
          <div>
            <h2>选择楼栋</h2>
          </div>
          <span>{{ authorizedScopes.length }} 个可用入口</span>
        </div>
        <div class="building-grid">
          <button
            v-for="item in bootstrapScopes"
            :key="item.value"
            type="button"
            class="building-card"
            :class="{ disabled: !item.authorized }"
            :disabled="!item.authorized"
            @click="openBuilding(item.value)"
          >
            <span class="building-card__icon"><Building2 :size="25" /></span>
            <strong>{{ item.label }}</strong>
            <div>
              <span>待填写 {{ item.pending || 0 }}</span>
              <span>已完成 {{ item.completed || 0 }}</span>
            </div>
            <b>{{ item.authorized ? "进入" : "无权限" }} <ChevronRight :size="17" /></b>
          </button>
          <button v-if="isAdmin" type="button" class="building-card admin" @click="openAdmin">
            <span class="building-card__icon"><Settings2 :size="25" /></span>
            <strong>管理员入口</strong>
            <div>
              <span>发布任务</span>
              <span>结果汇总</span>
            </div>
            <b>进入 <ChevronRight :size="17" /></b>
          </button>
        </div>
      </section>

      <section v-else-if="viewMode === 'admin'" class="admin-view">
        <div class="admin-toolbar">
          <div class="summary-pills">
            <span>任务 {{ tasks.length }}</span>
            <span>表单 {{ adminResponseTotal }}</span>
            <span>已生成 {{ adminSubmittedTotal }}</span>
          </div>
          <button type="button" class="primary-button" @click="publishOpen = !publishOpen">
            <Plus :size="17" /> 发布任务
          </button>
        </div>

        <section
          class="weather-automation"
          :class="{ running: weatherRunning, paused: weatherPaused, failed: weatherStatus?.last_error }"
          role="status"
          aria-live="polite"
        >
          <div class="weather-automation__identity">
            <span class="weather-automation__icon"><CloudSun :size="22" /></span>
            <div>
              <strong>天气预警自动重保</strong>
              <span>{{ weatherHeadline }}</span>
            </div>
          </div>
          <div class="weather-automation__times">
            <span>最近查询 <b>{{ formatWeatherTime(weatherStatus?.last_query_at) }}</b></span>
            <span>最近成功 <b>{{ formatWeatherTime(weatherStatus?.last_success_at) }}</b></span>
            <span>下次查询 <b>{{ weatherPaused ? "已暂停" : formatWeatherTime(weatherStatus?.next_query_at) }}</b></span>
          </div>
          <div class="weather-automation__action">
            <span :class="weatherStatus?.last_error ? 'error' : ''">{{ weatherPhaseText }}</span>
            <div class="weather-automation__buttons">
              <button
                type="button"
                class="primary-button weather-refresh-button"
                :disabled="weatherPaused || weatherRunning || weatherActionBusy"
                @click="refreshWeatherNow"
              >
                <RefreshCw :size="16" :class="{ spinning: weatherRunning || weatherActionBusy }" />
                {{ weatherRunning ? "正在检查天气" : "立即检查天气" }}
              </button>
              <button
                type="button"
                class="secondary-button weather-pause-button"
                :disabled="weatherActionBusy"
                @click="toggleWeatherPause"
              >
                <Play v-if="weatherPaused" :size="16" />
                <Pause v-else :size="16" />
                {{ weatherPaused ? "恢复自动查询" : "暂停自动查询" }}
              </button>
            </div>
          </div>
          <div v-if="weatherResultText" class="weather-automation__result">
            {{ weatherResultText }}
          </div>
          <div v-if="weatherArchiveFailures.length" class="weather-archive-failures">
            <div v-for="item in weatherArchiveFailures" :key="item.task_id">
              <span>{{ item.warning_title }} · 归档失败：{{ item.archive_error }}</span>
              <button type="button" :disabled="weatherActionBusy" @click="retryWeatherArchive(item)">
                重试归档
              </button>
            </div>
          </div>
        </section>

        <form v-if="publishOpen" class="publish-panel" @submit.prevent="publishTask">
          <label class="field full">
            <span>任务名称</span>
            <input v-model.trim="publishForm.name" maxlength="160" placeholder="例如：气象台发布台风橙色预警" />
          </label>
          <fieldset>
            <legend>检查表</legend>
            <div class="choice-grid sheets">
              <label v-for="sheet in sheetTypes" :key="sheet">
                <input v-model="publishForm.sheetTypes" type="checkbox" :value="sheet" />
                <span>{{ sheet }}</span>
              </label>
            </div>
          </fieldset>
          <fieldset>
            <legend>填写楼栋</legend>
            <div class="choice-grid scopes">
              <label v-for="item in bootstrapScopes" :key="item.value">
                <input v-model="publishForm.targetScopes" type="checkbox" :value="item.value" />
                <span>{{ item.label }}</span>
              </label>
            </div>
          </fieldset>
          <div class="publish-actions">
            <button type="button" class="secondary-button" @click="publishOpen = false">取消</button>
            <button type="submit" class="primary-button" :disabled="saving">
              {{ saving ? "发布中" : "确认发布" }}
            </button>
          </div>
        </form>

        <div class="admin-layout">
          <aside class="task-list">
            <div
              v-for="task in tasks"
              :key="task.task_id"
              class="admin-task-item"
            >
              <button
                type="button"
                class="task-select"
                :class="{ active: selectedTask?.task_id === task.task_id }"
                :disabled="loading || saving || Boolean(deletingTaskId)"
                @click="selectTask(task.task_id, true)"
              >
                <strong>{{ task.name }}</strong>
                <span>{{ formatDateTime(task.created_at) }}</span>
                <small>{{ task.submitted_count || 0 }}/{{ task.response_count || 0 }} 已生成</small>
              </button>
              <button
                type="button"
                class="task-delete"
                :class="{ deleting: deletingTaskId === task.task_id }"
                :disabled="loading || saving || Boolean(deletingTaskId)"
                :aria-label="`删除任务：${task.name}`"
                :title="deletingTaskId === task.task_id ? '正在删除' : '删除任务'"
                @click.stop="requestDeleteTask(task)"
              >
                <Trash2 :size="16" />
              </button>
            </div>
            <div v-if="!tasks.length" class="empty-inline">暂无重保任务</div>
          </aside>

          <section class="admin-results">
            <template v-if="selectedTask">
              <header>
                <div>
                  <h2>{{ selectedTask.name }}</h2>
                  <span>{{ selectedTask.target_scopes?.join("、") }} 楼</span>
                </div>
                <button
                  type="button"
                  class="secondary-button"
                  :disabled="!adminActiveSheet || !adminSheetHasImages"
                  @click="downloadAdminSheet"
                >
                  <Download :size="17" /> 下载同类型
                </button>
              </header>
              <nav class="sheet-tabs">
                <button
                  v-for="sheet in selectedTask.sheet_types || []"
                  :key="sheet"
                  type="button"
                  :class="{ active: adminActiveSheet === sheet }"
                  @click="adminActiveSheet = sheet"
                >
                  {{ sheet }}
                  <span>{{ adminSheetResponses(sheet).filter((item) => item.status === 'submitted').length }}/{{ adminSheetResponses(sheet).length }}</span>
                </button>
              </nav>
              <div class="result-grid">
                <article v-for="response in adminSheetResponses(adminActiveSheet)" :key="response.response_id">
                  <header>
                    <strong>{{ response.scope }}楼</strong>
                    <span :class="statusClass(response.status)">{{ statusText(response.status) }}</span>
                  </header>
                  <button
                    v-if="response.image_url && !adminImageFailed(response)"
                    type="button"
                    class="result-thumbnail"
                    @click="openImage(response.image_url, `${response.scope}楼 · ${response.sheet_type}`)"
                  >
                    <img
                      :key="adminImageSrc(response)"
                      :src="adminImageSrc(response)"
                      :alt="`${response.scope}楼${response.sheet_type}`"
                      loading="lazy"
                      @error="markAdminImageFailed(response)"
                    />
                    <ZoomIn :size="20" />
                  </button>
                  <div v-else-if="response.image_url" class="result-empty image-error">
                    <RefreshCw :size="23" />
                    <span>图片加载失败</span>
                    <button type="button" @click="retryAdminImage(response)">重新加载</button>
                  </div>
                  <div v-else class="result-empty"><FileSpreadsheet :size="25" /> 尚未生成</div>
                  <footer>{{ response.signature_names || response.signature_name || "未签名" }} · {{ formatDateTime(response.updated_at) }}</footer>
                  <a v-if="response.workbook_url" class="workbook-link compact" :href="response.workbook_url" download>
                    <Download :size="14" /> 下载原表
                  </a>
                </article>
              </div>
            </template>
            <div v-else class="page-state compact">选择左侧任务查看汇总</div>
          </section>
        </div>
      </section>

      <section v-else class="building-view">
        <div class="building-summary">
          <span><Building2 :size="19" /> {{ activeScope }}楼</span>
          <strong>待填写 {{ buildingPendingCount }}</strong>
          <b>已完成 {{ buildingCompletedCount }}</b>
        </div>
        <div class="fill-layout">
          <aside class="task-list building-tasks">
            <button
              v-for="task in tasks"
              :key="task.task_id"
              type="button"
              class="task-select"
              :class="{ active: selectedTask?.task_id === task.task_id }"
              :disabled="loading || saving"
              @click="requestTaskSwitch(task.task_id)"
            >
              <strong>{{ task.name }}</strong>
              <span>{{ task.sheet_types?.length || 0 }} 张检查表</span>
              <small :class="task.complete ? 'done' : ''">{{ task.complete ? "已完成" : `待完成 ${task.pending_count + task.draft_count}` }}</small>
            </button>
            <div v-if="!tasks.length" class="empty-inline">当前楼栋暂无重保任务</div>
          </aside>

          <section class="sheet-workspace">
            <template v-if="selectedTask && activeResponse && activeDefinition">
              <header class="workspace-header">
                <div>
                  <h2>{{ selectedTask.name }}</h2>
                  <span>{{ activeDefinition.title }}</span>
                </div>
                <span :class="statusClass(activeResponse.status)">{{ statusText(activeResponse.status) }}</span>
              </header>
              <div v-if="templateOutdated" class="inline-message error" role="alert">
                检查模板已更新。为避免检查项错位，请管理员按最新模板重新发布任务。
              </div>
              <nav class="sheet-tabs fill-tabs">
                <button
                  v-for="response in orderedResponses"
                  :key="response.response_id"
                  type="button"
                  :class="{ active: activeResponse.response_id === response.response_id }"
                  :disabled="loading || saving"
                  @click="requestResponseSwitch(response.response_id)"
                >
                  {{ response.sheet_type }}
                  <CheckCircle2 v-if="response.status === 'submitted'" :size="14" />
                </button>
              </nav>

              <div v-if="activeDefinition.input_mode !== 'file'" class="sheet-meta">
                <label>
                  <span>检查机房</span>
                  <input :value="cells.machine_room" readonly />
                </label>
                <label>
                  <span>检查日期</span>
                  <input v-model="cells.check_date" type="date" @input="markDirty" />
                </label>
                <div v-if="activeDefinition.has_signature" class="signer-box" :class="{ missing: !allSelectedSignersReady }">
                  <div class="signer-previews" aria-hidden="true">
                    <UsersRound :size="22" />
                    <b v-if="selectedSigners.length">{{ selectedSigners.length }}</b>
                  </div>
                  <div>
                    <span>检查人签名</span>
                    <strong>{{ signerSummaryText }}</strong>
                  </div>
                  <button type="button" class="manage-signature-button" :disabled="templateOutdated" @click="signatureDrawerOpen = true">
                    <PenLine :size="15" /> 选择签名人员
                  </button>
                </div>
              </div>

              <div v-if="activeDefinition.has_weather" class="weather-fields">
                <label v-for="field in activeDefinition.weather_fields" :key="field.key">
                  <span>{{ field.label }}</span>
                  <input v-model="cells.weather[field.key]" @input="markDirty" />
                </label>
              </div>

              <div
                v-if="activeDefinition.input_mode === 'file'"
                class="scope-file-workspace"
              >
                <section
                  class="scope-file-panel"
                  :class="{ dragging: sourceFileDragging }"
                  @dragenter.prevent="sourceFileDragging = true"
                  @dragover.prevent="sourceFileDragging = true"
                  @dragleave.prevent="sourceFileDragging = false"
                  @drop.prevent="handleSourceFileDrop"
                >
                  <input
                    ref="sourceFileInput"
                    class="visually-hidden"
                    type="file"
                    accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    @change="handleSourceFileChange"
                  />
                  <div class="scope-file-icon"><FileSpreadsheet :size="28" /></div>
                  <div class="scope-file-copy">
                    <span>{{ activeScope }}楼 · {{ activeResponse.sheet_type }}</span>
                    <strong>{{ effectiveSourceFile?.file_name || "尚未上传楼栋文件" }}</strong>
                    <small v-if="effectiveSourceFile">
                      {{ activeResponse.source_file?.file_id ? "当前任务已绑定" : "使用楼栋最近文件" }}
                      · {{ formatFileSize(effectiveSourceFile.size) }}
                      · {{ formatDateTime(effectiveSourceFile.updated_at) }}
                    </small>
                    <small v-else>支持 .xlsx，文件按楼栋和清单类型独立保存</small>
                  </div>
                  <div class="scope-file-actions">
                    <a
                      v-if="effectiveSourceFile?.download_url"
                      class="secondary-button"
                      :href="effectiveSourceFile.download_url"
                      download
                    >
                      <Download :size="16" /> 下载文件
                    </a>
                    <button type="button" class="primary-button" :disabled="saving || templateOutdated" @click="openSourceFilePicker">
                      <UploadCloud :size="17" />
                      {{ sourceFileUploading ? "上传中" : effectiveSourceFile ? "替换文件" : "上传文件" }}
                    </button>
                  </div>
                </section>

                <section v-if="effectiveSourceFile" class="scope-file-preview-card">
                  <header>
                    <div class="scope-file-preview-heading">
                      <span class="scope-file-preview-kicker">
                        <FileSpreadsheet :size="15" /> 上传文件核对
                        <b>非生成结果</b>
                      </span>
                      <strong>{{ effectiveSourceFile.file_name }}</strong>
                    </div>
                    <button
                      v-if="!sourceFilePreviewFailed"
                      type="button"
                      class="secondary-button"
                      @click="openImage(sourceFilePreviewUrl, sourceFileViewerTitle)"
                    >
                      <ZoomIn :size="16" /> 查看源文件
                    </button>
                  </header>
                  <button
                    v-if="!sourceFilePreviewFailed"
                    type="button"
                    class="scope-file-preview-image"
                    @click="openImage(sourceFilePreviewUrl, sourceFileViewerTitle)"
                  >
                    <img
                      :key="sourceFilePreviewKey"
                      :src="sourceFilePreviewUrl"
                      :alt="`${activeScope}楼${activeResponse.sheet_type}上传源文件核对预览`"
                      loading="lazy"
                      @error="sourceFilePreviewFailed = true"
                    />
                    <span class="scope-file-preview-stamp">源文件预览 · 非生成结果</span>
                    <span class="scope-file-preview-zoom">点击核对</span>
                  </button>
                  <div v-else class="scope-file-preview-error">
                    <span>文件已保存，预览暂时加载失败。</span>
                    <button type="button" class="secondary-button" @click="retrySourceFilePreview">重新加载</button>
                  </div>
                </section>
              </div>

              <section v-else-if="activeDefinition.kind === 'check'" class="sheet-table-shell">
                <div class="table-actions">
                  <div>
                    <span>异常 {{ abnormalCount }} 项</span>
                    <small :class="{ customized: activeTemplateCustomized }">
                      {{ activeTemplateCustomized ? `${activeScope}楼模板` : "默认模板" }} · {{ activeCheckItems.length }} 项
                    </small>
                  </div>
                  <div>
                    <button type="button" :disabled="templateOutdated || saving" @click="openTemplateEditor"><Settings2 :size="16" /> 编辑模板</button>
                    <button type="button" :disabled="templateOutdated" @click="markAllNormal"><ClipboardCheck :size="16" /> 一键全正常</button>
                  </div>
                </div>
                <table class="check-table">
                  <thead><tr><th>检查项</th><th>检查内容</th><th>检查结果</th><th>备注</th></tr></thead>
                  <tbody>
                    <tr v-for="item in activeCheckItems" :key="item.key" :class="{ abnormal: cells.checks[item.key]?.status === 'abnormal' }">
                      <td>{{ item.category }}</td>
                      <td>{{ item.content }}</td>
                      <td>
                        <div class="result-toggle">
                          <button type="button" :class="{ active: cells.checks[item.key]?.status === 'normal' }" @click="setCheckStatus(item.key, 'normal')">正常</button>
                          <button type="button" class="abnormal-option" :class="{ active: cells.checks[item.key]?.status === 'abnormal' }" @click="setCheckStatus(item.key, 'abnormal')">异常</button>
                        </div>
                      </td>
                      <td>
                        <input
                          v-model="cells.checks[item.key].note"
                          :class="{ required: cells.checks[item.key]?.status === 'abnormal' && !cells.checks[item.key]?.note }"
                          :placeholder="cells.checks[item.key]?.status === 'abnormal' ? '异常备注必填' : ''"
                          @input="markDirty"
                        />
                      </td>
                    </tr>
                  </tbody>
                </table>
                <label class="suggestions-field">
                  <span>检查意见及建议</span>
                  <textarea v-model="cells.suggestions" rows="2" @input="markDirty"></textarea>
                </label>
              </section>

              <section v-else-if="activeDefinition.kind === 'materials'" class="structured-sections">
                <article v-for="section in activeDefinition.sections" :key="section.key">
                  <h3>{{ section.title }}</h3>
                  <table class="entry-table">
                    <thead><tr><th>序号</th><th v-for="column in section.columns" :key="column.key">{{ column.label }}</th></tr></thead>
                    <tbody>
                      <tr v-for="(row, rowIndex) in cells[section.key]" :key="rowIndex">
                        <td>{{ rowIndex + 1 }}</td>
                        <td v-for="column in section.columns" :key="column.key"><input v-model="row[column.key]" @input="markDirty" /></td>
                      </tr>
                    </tbody>
                  </table>
                </article>
              </section>

              <section v-else class="structured-sections">
                <article>
                  <h3>{{ activeDefinition.duty.title }}</h3>
                  <table class="entry-table">
                    <thead><tr><th>序号</th><th v-for="column in activeDefinition.duty.columns" :key="column.key">{{ column.label }}</th></tr></thead>
                    <tbody>
                      <tr v-for="(row, rowIndex) in cells.duty" :key="rowIndex"><td>{{ rowIndex + 1 }}</td><td v-for="column in activeDefinition.duty.columns" :key="column.key"><input v-model="row[column.key]" @input="markDirty" /></td></tr>
                    </tbody>
                  </table>
                </article>
                <article v-for="group in activeDefinition.groups" :key="group.key">
                  <h3>{{ group.title }}</h3>
                  <table class="entry-table">
                    <thead><tr><th>序号</th><th v-for="column in activeDefinition.group_columns" :key="column.key">{{ column.label }}</th></tr></thead>
                    <tbody>
                      <tr v-for="(row, rowIndex) in cells.groups[group.key]" :key="rowIndex"><td>{{ rowIndex + 1 }}</td><td v-for="column in activeDefinition.group_columns" :key="column.key"><input v-model="row[column.key]" @input="markDirty" /></td></tr>
                    </tbody>
                  </table>
                </article>
              </section>

              <section v-if="activeResponse.image_url" class="generated-preview">
                <header>
                  <div>
                    <span class="generated-preview-kicker"><CheckCircle2 :size="15" /> 正式生成结果</span>
                    <strong>已生成图片</strong>
                    <span>{{ activeResponse.scope }}楼 · {{ activeResponse.sheet_type }}</span>
                  </div>
                  <a v-if="activeResponse.workbook_url" class="workbook-link" :href="activeResponse.workbook_url" download>
                    <Download :size="15" /> 下载原表
                  </a>
                </header>
                <button type="button" @click="openImage(activeResponse.image_url, `正式生成图片 · ${activeResponse.scope}楼 · ${activeResponse.sheet_type}`)">
                  <img :src="activeResponse.image_url" :alt="activeResponse.sheet_type" />
                  <ZoomIn :size="20" />
                </button>
              </section>

              <footer class="workspace-actions">
                <span :class="{ dirty }">{{ sourceFileUploading ? "文件上传中" : saving ? "保存中" : dirty ? "有未保存修改" : `版本 ${activeResponse.version}` }}</span>
                <div>
                  <button v-if="activeDefinition.input_mode !== 'file'" type="button" class="secondary-button" :disabled="saving || !dirty || templateOutdated" @click="saveResponse(false)"><Save :size="17" /> 保存</button>
                  <button type="button" class="primary-button" :disabled="Boolean(generateDisabledReason)" :title="generateDisabledReason" @click="saveResponse(true)"><ImageIcon :size="17" /> {{ generateButtonText }}</button>
                </div>
              </footer>
            </template>
            <div v-else class="page-state compact">选择左侧任务开始填写</div>
          </section>
        </div>
      </section>
    </template>

    <Teleport to="body">
      <div v-if="imageViewerUrl" class="image-viewer" @click.self="closeImage">
        <header><strong>{{ imageViewerTitle }}</strong><button type="button" aria-label="关闭" @click="closeImage"><X :size="22" /></button></header>
        <div><img :src="imageViewerUrl" :alt="imageViewerTitle" /></div>
      </div>
    </Teleport>

    <Teleport to="body">
      <div v-if="templateEditorOpen" class="template-editor-overlay" @click.self="requestCloseTemplateEditor">
        <section class="template-editor-dialog" role="dialog" aria-modal="true" aria-labelledby="guard-template-editor-title">
          <header>
            <div>
              <span>{{ activeScope }}楼 · {{ String(activeResponse?.sheet_type || "检查表") }}</span>
              <h3 id="guard-template-editor-title">检查模板</h3>
            </div>
            <button type="button" class="template-editor-close" aria-label="关闭" :disabled="templateSaving" @click="requestCloseTemplateEditor"><X :size="20" /></button>
          </header>
          <div class="template-editor-body" :aria-busy="templateLoading">
            <div v-if="templateLoading" class="template-editor-loading"><RefreshCw :size="20" class="spinning" /> 正在读取楼栋模板</div>
            <template v-else>
            <div class="template-editor-table-head">
              <span>序号</span><span>检查项</span><span>检查内容</span><span>操作</span>
            </div>
            <div class="template-editor-rows">
              <div v-for="(item, index) in templateRows" :key="item.key" class="template-editor-row">
                <b>{{ index + 1 }}</b>
                <input v-model="item.category" maxlength="200" aria-label="检查项" @input="templateEditorTouched = true" />
                <textarea v-model="item.content" rows="2" maxlength="2000" aria-label="检查内容" @input="templateEditorTouched = true"></textarea>
                <button type="button" class="template-row-delete" :disabled="templateSaving || templateRows.length <= 1" :aria-label="`删除第 ${index + 1} 行`" @click="removeTemplateRow(index)"><Trash2 :size="17" /></button>
              </div>
            </div>
            <button type="button" class="template-add-row" :disabled="templateSaving || templateRows.length >= 200" @click="addTemplateRow"><Plus :size="16" /> 添加一行</button>
            </template>
          </div>
          <footer>
            <button type="button" class="template-reset-button" :disabled="templateSaving || templateLoading || !templateEditorCustomized" @click="templateResetConfirmOpen = true">恢复默认模板</button>
            <div>
              <button type="button" class="secondary-button" :disabled="templateSaving" @click="requestCloseTemplateEditor">取消</button>
              <button type="button" class="primary-button" :disabled="templateSaving || templateLoading || !templateEditorDirty" @click="saveScopeTemplate"><Save :size="16" /> {{ templateSaving ? "保存中" : "保存并应用" }}</button>
            </div>
          </footer>
        </section>
      </div>
    </Teleport>

    <ConfirmDialog
      :open="confirmOpen"
      tone="warning"
      title="当前表格有未保存修改"
      message="继续切换会丢失这些修改。"
      confirm-label="放弃并继续"
      @resolve="resolveSwitch"
    />

    <ConfirmDialog
      :open="normalConfirmOpen"
      tone="warning"
      title="确认将全部检查项设为正常"
      message="当前异常状态和异常备注会被清空。"
      confirm-label="确认清空"
      @resolve="resolveMarkAllNormal"
    />

    <ConfirmDialog
      :open="deleteConfirmOpen"
      tone="danger"
      title="确认删除重保任务"
      :message="`删除“${String(pendingDeleteTask?.name || '')}”后不可恢复。`"
      :details="deleteTaskDetails"
      confirm-label="删除任务"
      @resolve="resolveDeleteTask"
    />

    <ConfirmDialog
      :open="templateResetConfirmOpen"
      tone="warning"
      title="恢复默认检查模板"
      :message="`恢复后，${activeScope}楼后续发布的${String(activeResponse?.sheet_type || '检查表')}将使用系统默认检查项。`"
      confirm-label="恢复默认"
      @resolve="resolveTemplateReset"
    />

    <ConfirmDialog
      :open="templateCloseConfirmOpen"
      tone="warning"
      title="模板修改尚未保存"
      message="关闭后，本次检查项和检查内容修改将丢失。"
      confirm-label="放弃修改"
      @resolve="resolveTemplateClose"
    />

    <ConfirmDialog
      :open="weatherNoTaskDialogOpen"
      tone="primary"
      kicker="天气检查完成"
      title="暂无需要发布的重保任务"
      :message="weatherNoTaskDialogMessage"
      confirm-label="知道了"
      :hide-cancel="true"
      @resolve="weatherNoTaskDialogOpen = false"
    />

    <CriticalGuardSignatureDrawer
      :open="signatureDrawerOpen"
      :scope="activeScope"
      :context-key="signatureContextKey"
      :task-title="String(selectedTask?.name || '')"
      :current-user-open-id="currentUserOpenId"
      :initial-signers="selectedSigners"
      @close="signatureDrawerOpen = false"
      @change="applySignatureSelection"
      @refresh="refreshSignatureSelection"
      @status="handleSignatureStatus"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  AlertTriangle,
  Building2,
  CheckCircle2,
  ChevronRight,
  CloudSun,
  ClipboardCheck,
  Download,
  FileSpreadsheet,
  Image as ImageIcon,
  Pause,
  PenLine,
  Play,
  Plus,
  RefreshCw,
  Save,
  Settings2,
  Trash2,
  UploadCloud,
  UsersRound,
  X,
  ZoomIn,
} from "lucide-vue-next";
import { requestJson, type Dict } from "../api/client";
import { navigate, navigateBack } from "../navigation";
import ConfirmDialog from "./ConfirmDialog.vue";
import CriticalGuardSignatureDrawer from "./CriticalGuardSignatureDrawer.vue";
import VnetBackButton from "./VnetBackButton.vue";

const props = defineProps<{
  scope: string;
  scopeOptions: Array<{ value: string; label: string }>;
  isAdmin: boolean;
  user: Dict;
  adminMode?: boolean;
}>();

const emit = defineEmits<{
  status: [text: string];
  "switch-scope": [scope: string];
}>();

const bootstrap = ref<Dict | null>(null);
const tasks = ref<Dict[]>([]);
const selectedTask = ref<Dict | null>(null);
const activeResponse = ref<Dict | null>(null);
const adminActiveSheet = ref("");
const cells = ref<Dict>({});
const loading = ref(false);
const saving = ref(false);
const dirty = ref(false);
const error = ref("");
const message = ref("");
const messageTone = ref("info");
const publishOpen = ref(false);
const weatherStatus = ref<Dict | null>(null);
const weatherActionBusy = ref(false);
const weatherNoTaskDialogOpen = ref(false);
const weatherNoTaskDialogMessage = ref("");
const imageViewerUrl = ref("");
const imageViewerTitle = ref("");
const confirmOpen = ref(false);
const normalConfirmOpen = ref(false);
const deleteConfirmOpen = ref(false);
const pendingDeleteTask = ref<Dict | null>(null);
const deletingTaskId = ref("");
const templateEditorOpen = ref(false);
const templateLoading = ref(false);
const templateSaving = ref(false);
const templateRows = ref<Dict[]>([]);
const templateInitialRows = ref<Dict[]>([]);
const templateEditorTouched = ref(false);
const templateEditorRevision = ref(0);
const templateEditorCustomized = ref(false);
const templateResetConfirmOpen = ref(false);
const templateCloseConfirmOpen = ref(false);
const signatureDrawerOpen = ref(false);
const sourceFileInput = ref<HTMLInputElement | null>(null);
const sourceFileDragging = ref(false);
const sourceFileUploading = ref(false);
const sourceFilePreviewFailed = ref(false);
const sourceFilePreviewRevision = ref(0);
const adminImageFailures = ref<Record<string, boolean>>({});
const adminImageRetryTokens = ref<Record<string, number>>({});
const selectedSigners = ref<Dict[]>([]);
const pendingSwitch = ref<null | (() => void)>(null);
let editRevision = 0;
let bootstrapGeneration = 0;
let listGeneration = 0;
let detailGeneration = 0;
let templateLoadGeneration = 0;
let templatePendingOperationFingerprint = "";
let templatePendingOperationId = "";
let publishOperationId = "";
let bootstrapController: AbortController | null = null;
let listController: AbortController | null = null;
let detailController: AbortController | null = null;
let weatherPollTimer: ReturnType<typeof setTimeout> | null = null;
let adminDetailPollTimer: ReturnType<typeof setTimeout> | null = null;
let adminDetailPollController: AbortController | null = null;
let adminDetailPollInFlight = false;
let adminDetailPollFailureReported = false;
let componentUnmounted = false;
let weatherStatusLoading = false;
let weatherTaskRefreshJobId = "";
let manualWeatherJobId = "";
const publishForm = ref({
  name: "",
  sheetTypes: ["设备安全", "环境安全"],
  targetScopes: ["A", "B", "C", "D", "E"],
});

const activeScope = computed(() => String(props.scope || "").trim().toUpperCase());
const viewMode = computed<"landing" | "admin" | "building">(() => (
  props.adminMode && props.isAdmin ? "admin" : activeScope.value ? "building" : "landing"
));
const pageTitle = computed(() => (
  viewMode.value === "admin" ? "重保任务管理" : viewMode.value === "building" ? `${activeScope.value}楼重保检查` : "重保管理"
));
const backTarget = computed(() => (viewMode.value === "landing" ? "/" : "/critical-guard"));
const bootstrapScopes = computed<Dict[]>(() => Array.isArray(bootstrap.value?.scopes) ? bootstrap.value!.scopes : []);
const authorizedScopes = computed(() => bootstrapScopes.value.filter((item) => item.authorized));
const sheetTypes = computed<string[]>(() => Array.isArray(bootstrap.value?.sheet_types) ? bootstrap.value!.sheet_types : []);
const catalogSheets = computed<Dict[]>(() => Array.isArray(bootstrap.value?.catalog?.sheets) ? bootstrap.value!.catalog.sheets : []);
const currentUserOpenId = computed(() => String(props.user?.open_id || "").trim());
const templateOutdated = computed(() => Boolean(selectedTask.value?.template_outdated));
const orderedResponses = computed(() => {
  if (!selectedTask.value) return [];
  const order = new Map((selectedTask.value.sheet_types || []).map((name: string, index: number) => [name, index]));
  return [...(selectedTask.value.responses || [])].sort((left, right) => Number(order.get(left.sheet_type) ?? 99) - Number(order.get(right.sheet_type) ?? 99));
});
const activeDefinition = computed<Dict | null>(() => (
  activeResponse.value ? catalogSheets.value.find((item) => item.name === activeResponse.value?.sheet_type) || null : null
));
const activeCheckItems = computed<Dict[]>(() => {
  const responseItems = Array.isArray(cells.value?.template_items)
    ? cells.value.template_items
    : [];
  if (responseItems.length) return responseItems;
  return Array.isArray(activeDefinition.value?.items) ? activeDefinition.value!.items : [];
});
const activeTemplateRevision = computed(() => Math.max(
  0,
  Number(cells.value?.template_revision ?? activeResponse.value?.template_revision ?? 0) || 0,
));
const activeTemplateCustomized = computed(() => Boolean(
  cells.value?.template_customized
  ?? activeResponse.value?.template_customized
  ?? (activeTemplateRevision.value > 0),
));
const templateEditorDirty = computed(() => (
  templateEditorTouched.value
  || JSON.stringify(templateRows.value) !== JSON.stringify(templateInitialRows.value)
));
const abnormalCount = computed(() => Object.values(cells.value.checks || {}).filter((item: any) => item?.status === "abnormal").length);
const buildingPendingCount = computed(() => tasks.value.filter((item) => !item.complete).length);
const buildingCompletedCount = computed(() => tasks.value.filter((item) => item.complete).length);
const adminResponseTotal = computed(() => selectedTask.value?.responses?.length || tasks.value.reduce((total, item) => total + Number(item.response_count || 0), 0));
const adminSubmittedTotal = computed(() => selectedTask.value ? selectedTask.value.responses.filter((item: Dict) => item.status === "submitted").length : tasks.value.reduce((total, item) => total + Number(item.submitted_count || 0), 0));
const adminSheetHasImages = computed(() => adminSheetResponses(adminActiveSheet.value).some((item) => item.image_url));
const weatherRunning = computed(() => Boolean(weatherStatus.value?.running));
const weatherPaused = computed(() => Boolean(weatherStatus.value?.paused));
const weatherWarnings = computed<Dict[]>(() => (
  Array.isArray(weatherStatus.value?.current_warnings)
    ? weatherStatus.value!.current_warnings
    : []
));
const weatherHeadline = computed(() => {
  const warning = weatherWarnings.value[0];
  if (warning) {
    return `${String(warning.title || "当前预警")} · ${String(warning.guard_level || weatherStatus.value?.current_guard_level || "")}`;
  }
  if (weatherStatus.value?.last_error) return "最近查询失败，已保留上次成功数据";
  return "当前无新增天气重保任务";
});
const weatherPhaseText = computed(() => {
  if (weatherPaused.value) {
    return weatherRunning.value ? "已暂停后续查询，当前检查正在收尾" : "天气自动查询已暂停";
  }
  if (weatherStatus.value?.last_error && !weatherRunning.value) {
    return String(weatherStatus.value.last_error);
  }
  const phase = String(weatherStatus.value?.phase || "idle");
  return ({
    queued: "已加入天气检查队列",
    fetching: "正在读取天气",
    evaluating: "正在判断戒备任务",
    notifying: "正在检查通知与归档",
    completed: "已检查通知与归档",
    failed: "查询失败，仍保留上次成功数据",
    idle: "等待自动查询",
  } as Record<string, string>)[phase] || "等待自动查询";
});
const weatherResultText = computed(() => {
  const result = weatherStatus.value?.last_result;
  if (!result || typeof result !== "object" || weatherRunning.value || weatherStatus.value?.last_error) return "";
  const created = Number(result.new_tasks || 0);
  const existing = Number(result.existing_tasks || 0);
  const processed = Number(result.processed_tasks ?? existing);
  const sent = Number(result.notifications_sent || 0);
  const failed = Number(result.notifications_failed || 0);
  const archived = Number(result.archived || 0);
  const archiveFailed = Number(result.archive_failed || 0);
  const taskFailed = Number(result.task_failed || 0);
  const warnings = [
    failed ? `通知失败 ${failed} 次` : "",
    archiveFailed ? `归档失败 ${archiveFailed} 条` : "",
    taskFailed ? `任务处理失败 ${taskFailed} 个` : "",
  ].filter(Boolean).join("，");
  return created
    ? `本次新建 ${created} 个任务，处理 ${processed} 个任务，通知成功 ${sent} 次，归档 ${archived} 条${warnings ? `，${warnings}` : ""}`
    : `数据已刷新，无新增任务；处理 ${processed} 个任务，通知成功 ${sent} 次，归档 ${archived} 条${warnings ? `，${warnings}` : ""}`;
});
const weatherArchiveFailures = computed<Dict[]>(() => (
  Array.isArray(weatherStatus.value?.tasks)
    ? weatherStatus.value!.tasks.filter((item: Dict) => item.archive_status === "failed")
    : []
));
const signatureContextKey = computed(() => (
  activeResponse.value
    ? `critical_guard:${String(activeResponse.value.task_id || selectedTask.value?.task_id || "")}:${activeScope.value}`
    : ""
));
const readySignerCount = computed(() => selectedSigners.value.filter(signaturePersonReady).length);
const allSelectedSignersReady = computed(() => (
  selectedSigners.value.length > 0 && readySignerCount.value === selectedSigners.value.length
));
const signerSummaryText = computed(() => {
  if (!selectedSigners.value.length) return "未选择";
  if (allSelectedSignersReady.value) return `${selectedSigners.value.length} 人全部可用`;
  return `可用 ${readySignerCount.value}/${selectedSigners.value.length}`;
});
const effectiveSourceFile = computed<Dict | null>(() => {
  const bound = activeResponse.value?.source_file;
  if (bound?.file_id) return bound;
  const reusable = activeResponse.value?.reusable_source_file;
  return reusable?.file_id ? reusable : null;
});
const sourceFilePreviewUrl = computed(() => {
  const base = String(effectiveSourceFile.value?.preview_url || "").trim();
  if (!base || sourceFilePreviewRevision.value <= 0) return base;
  return `${base}${base.includes("?") ? "&" : "?"}view=${sourceFilePreviewRevision.value}`;
});
const sourceFilePreviewKey = computed(() => [
  String(effectiveSourceFile.value?.file_id || ""),
  String(effectiveSourceFile.value?.sha256 || ""),
  String(sourceFilePreviewRevision.value),
].join(":"));
const sourceFileViewerTitle = computed(() => (
  `上传源文件核对 · ${activeScope.value}楼 · ${String(activeResponse.value?.sheet_type || "清单")}（非生成结果）`
));
const generateDisabledReason = computed(() => {
  if (saving.value) return "正在保存";
  if (templateOutdated.value) return "检查模板已更新，请管理员重新发布任务";
  if (activeDefinition.value?.input_mode === "file" && !effectiveSourceFile.value) {
    return `请先上传${activeScope.value}楼的${String(activeResponse.value?.sheet_type || "清单")}文件`;
  }
  if (!activeDefinition.value?.has_signature) return "";
  if (!selectedSigners.value.length) return "请至少选择一名检查人签名";
  if (!allSelectedSignersReady.value) return "所有已选检查人完成签名和确认后才可生成图片";
  return "";
});
const hasGeneratedArtifact = computed(() => Boolean(
  activeResponse.value?.image_url
  || activeResponse.value?.has_image
  || activeResponse.value?.workbook_url
  || activeResponse.value?.has_workbook
  || activeResponse.value?.status === "submitted"
));
const generateButtonText = computed(() => (hasGeneratedArtifact.value ? "重新生成图片" : "生成图片"));
const deleteTaskDetails = computed(() => {
  const task = pendingDeleteTask.value;
  if (!task) return [];
  const scopes = Array.isArray(task.target_scopes) ? task.target_scopes.join("、") : "";
  return [
    scopes ? `发布楼栋：${scopes}` : "任务将从所有已发布楼栋移除",
    `同时删除 ${Number(task.response_count || 0)} 份楼栋填报和已生成文件`,
    "楼栋上传模板与同名任务填写记忆会保留",
  ];
});

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? {}));
}

function operationId(): string {
  return typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function setMessage(text: string, tone = "info"): void {
  message.value = text;
  messageTone.value = tone;
  emit("status", text);
}

async function loadBootstrap(): Promise<void> {
  const generation = ++bootstrapGeneration;
  bootstrapController?.abort();
  const controller = new AbortController();
  bootstrapController = controller;
  loading.value = true;
  error.value = "";
  try {
    const data = await requestJson("/api/critical-guard/bootstrap", {
      cache: "no-store",
      signal: controller.signal,
    });
    if (generation !== bootstrapGeneration) return;
    bootstrap.value = data;
    await loadTasks();
    if (viewMode.value === "admin") {
      void loadWeatherStatus(true);
      scheduleAdminDetailRefresh(5_000);
    }
  } catch (loadError: any) {
    if (controller.signal.aborted || generation !== bootstrapGeneration) return;
    error.value = loadError?.message || "重保管理读取失败。";
  } finally {
    if (generation === bootstrapGeneration) loading.value = false;
  }
}

function syncAdminTaskSummary(detail: Dict): void {
  const taskId = String(detail?.task_id || "");
  const index = tasks.value.findIndex((item) => String(item.task_id || "") === taskId);
  if (index < 0) return;
  const responses = Array.isArray(detail?.responses) ? detail.responses : [];
  const submittedCount = responses.filter((item: Dict) => item.status === "submitted").length;
  const draftCount = responses.filter((item: Dict) => item.status === "draft").length;
  const pendingCount = responses.filter((item: Dict) => item.status === "pending").length;
  tasks.value[index] = {
    ...tasks.value[index],
    response_count: responses.length,
    submitted_count: submittedCount,
    draft_count: draftCount,
    pending_count: pendingCount,
    complete: Boolean(responses.length) && submittedCount === responses.length,
  };
}

function scheduleAdminDetailRefresh(delay = 5_000): void {
  if (adminDetailPollTimer) window.clearTimeout(adminDetailPollTimer);
  adminDetailPollTimer = null;
  if (componentUnmounted || viewMode.value !== "admin") return;
  adminDetailPollTimer = window.setTimeout(
    () => void refreshSelectedAdminTask(),
    delay,
  );
}

async function refreshSelectedAdminTask(): Promise<void> {
  if (viewMode.value !== "admin" || adminDetailPollInFlight) return;
  if (document.visibilityState === "hidden") {
    scheduleAdminDetailRefresh(15_000);
    return;
  }
  const taskId = String(selectedTask.value?.task_id || "");
  if (!taskId) {
    scheduleAdminDetailRefresh();
    return;
  }
  adminDetailPollInFlight = true;
  adminDetailPollController?.abort();
  const controller = new AbortController();
  adminDetailPollController = controller;
  let nextDelay = 5_000;
  try {
    const detail = await requestJson(
      `/api/critical-guard/tasks/${encodeURIComponent(taskId)}?admin=1`,
      { cache: "no-store", signal: controller.signal, timeoutMs: 15_000 },
    );
    if (
      controller.signal.aborted
      || viewMode.value !== "admin"
      || String(selectedTask.value?.task_id || "") !== taskId
    ) return;
    const currentSheet = adminActiveSheet.value;
    selectedTask.value = detail;
    adminActiveSheet.value = (detail.sheet_types || []).includes(currentSheet)
      ? currentSheet
      : String(detail.sheet_types?.[0] || "");
    syncAdminTaskSummary(detail);
    adminDetailPollFailureReported = false;
  } catch (refreshError: any) {
    nextDelay = 15_000;
    if (!controller.signal.aborted && !adminDetailPollFailureReported) {
      adminDetailPollFailureReported = true;
      setMessage(
        `管理员汇总暂未同步：${refreshError?.message || "请稍后重试"}`,
        "warning",
      );
    }
  } finally {
    if (adminDetailPollController === controller) adminDetailPollController = null;
    adminDetailPollInFlight = false;
    scheduleAdminDetailRefresh(nextDelay);
  }
}

function handlePageVisibilityChange(): void {
  if (document.visibilityState === "visible" && viewMode.value === "admin") {
    scheduleAdminDetailRefresh(100);
  }
}

async function loadTasks(): Promise<void> {
  if (viewMode.value === "landing") return;
  const generation = ++listGeneration;
  listController?.abort();
  const controller = new AbortController();
  listController = controller;
  const mode = viewMode.value;
  const scope = activeScope.value;
  const path = viewMode.value === "admin"
    ? "/api/critical-guard/tasks?admin=1"
    : `/api/critical-guard/tasks?scope=${encodeURIComponent(activeScope.value)}`;
  const data = await requestJson(path, { cache: "no-store", signal: controller.signal });
  if (
    generation !== listGeneration
    || mode !== viewMode.value
    || scope !== activeScope.value
  ) return;
  tasks.value = Array.isArray(data.tasks) ? data.tasks : [];
  if (
    selectedTask.value
    && !tasks.value.some((item) => item.task_id === selectedTask.value?.task_id)
  ) {
    clearTaskSelection();
  }
  if (!selectedTask.value && tasks.value.length) {
    const requestedTaskId = new URLSearchParams(window.location.search).get("task_id") || "";
    const requested = tasks.value.find((item) => item.task_id === requestedTaskId);
    await selectTask((requested || tasks.value[0]).task_id, viewMode.value === "admin");
  }
}

function scheduleWeatherStatus(delay = 30_000): void {
  if (weatherPollTimer) window.clearTimeout(weatherPollTimer);
  weatherPollTimer = null;
  if (componentUnmounted || viewMode.value !== "admin") return;
  weatherPollTimer = window.setTimeout(() => void loadWeatherStatus(true), delay);
}

async function loadWeatherStatus(silent = false): Promise<void> {
  if (viewMode.value !== "admin" || weatherStatusLoading) return;
  weatherStatusLoading = true;
  try {
    const nextStatus = await requestJson("/api/critical-guard/weather-status", {
      cache: "no-store",
      timeoutMs: 15_000,
    });
    weatherStatus.value = nextStatus;
    const jobId = String(nextStatus?.job?.job_id || "");
    const jobFinished = ["completed", "failed"].includes(String(nextStatus?.job?.status || ""));
    if (jobId && jobFinished && jobId !== weatherTaskRefreshJobId) {
      weatherTaskRefreshJobId = jobId;
      await loadTasks();
    }
    if (jobId && jobFinished && jobId === manualWeatherJobId) {
      manualWeatherJobId = "";
      const jobStatus = String(nextStatus?.job?.status || "");
      const result = nextStatus?.job?.result && typeof nextStatus.job.result === "object"
        ? nextStatus.job.result
        : {};
      if (jobStatus === "completed" && Number(result.new_tasks || 0) === 0) {
        const existingTasks = Number(result.existing_tasks || 0);
        weatherNoTaskDialogMessage.value = existingTasks > 0
          ? `本次未发布新任务。检测到的相关预警已有 ${existingTasks} 个重保任务，系统未重复创建。`
          : "本次天气数据中没有达到重保任务发布条件的预警，系统未创建任务。";
        weatherNoTaskDialogOpen.value = true;
      }
    }
  } catch (loadError: any) {
    if (!silent) setMessage(loadError?.message || "天气任务状态读取失败。", "error");
  } finally {
    weatherStatusLoading = false;
    scheduleWeatherStatus(weatherRunning.value ? 1_000 : 30_000);
  }
}

async function refreshWeatherNow(): Promise<void> {
  if (weatherPaused.value || weatherRunning.value || weatherActionBusy.value) return;
  weatherActionBusy.value = true;
  setMessage("正在检查天气。", "info");
  try {
    const job = await requestJson("/api/critical-guard/weather-refresh", {
      method: "POST",
      body: JSON.stringify({ operation_id: operationId() }),
      timeoutMs: 15_000,
    });
    manualWeatherJobId = String(job.job_id || "");
    weatherStatus.value = {
      ...(weatherStatus.value || {}),
      running: true,
      phase: String(job.phase || "queued"),
      job,
    };
    scheduleWeatherStatus(300);
  } catch (refreshError: any) {
    setMessage(refreshError?.message || "天气检查启动失败。", "error");
  } finally {
    weatherActionBusy.value = false;
  }
}

async function toggleWeatherPause(): Promise<void> {
  if (weatherActionBusy.value) return;
  const paused = !weatherPaused.value;
  weatherActionBusy.value = true;
  try {
    weatherStatus.value = await requestJson("/api/critical-guard/weather-pause", {
      method: "POST",
      body: JSON.stringify({ paused, operation_id: operationId() }),
      timeoutMs: 15_000,
    });
    setMessage(
      paused
        ? weatherRunning.value
          ? "已暂停后续天气查询，当前检查完成后不再自动请求。"
          : "天气自动查询已暂停。"
        : "天气自动查询已恢复。",
      "success",
    );
    scheduleWeatherStatus(paused ? 30_000 : 1_000);
  } catch (pauseError: any) {
    setMessage(pauseError?.message || "天气自动查询状态修改失败。", "error");
  } finally {
    weatherActionBusy.value = false;
  }
}

async function retryWeatherArchive(item: Dict): Promise<void> {
  if (weatherActionBusy.value) return;
  weatherActionBusy.value = true;
  try {
    await requestJson(
      `/api/critical-guard/tasks/${encodeURIComponent(String(item.task_id || ""))}/archive-retry`,
      { method: "POST", timeoutMs: 120_000 },
    );
    setMessage("重保汇总已归档。", "success");
    await loadWeatherStatus(true);
  } catch (retryError: any) {
    setMessage(retryError?.message || "归档重试失败。", "error");
  } finally {
    weatherActionBusy.value = false;
  }
}

function clearTaskSelection(): void {
  detailGeneration += 1;
  detailController?.abort();
  selectedTask.value = null;
  activeResponse.value = null;
  adminActiveSheet.value = "";
  cells.value = {};
  selectedSigners.value = [];
  adminImageFailures.value = {};
  adminImageRetryTokens.value = {};
  dirty.value = false;
}

async function selectTask(taskId: string, admin = false): Promise<void> {
  const generation = ++detailGeneration;
  detailController?.abort();
  const controller = new AbortController();
  detailController = controller;
  loading.value = true;
  try {
    const path = admin
      ? `/api/critical-guard/tasks/${encodeURIComponent(taskId)}?admin=1`
      : `/api/critical-guard/tasks/${encodeURIComponent(taskId)}?scope=${encodeURIComponent(activeScope.value)}`;
    const detail = await requestJson(path, { cache: "no-store", signal: controller.signal });
    if (generation !== detailGeneration) return;
    selectedTask.value = detail;
    adminImageFailures.value = {};
    adminImageRetryTokens.value = {};
    adminActiveSheet.value = selectedTask.value?.sheet_types?.[0] || "";
    if (!admin) {
      const first = orderedResponses.value[0] || null;
      applyResponse(first);
    }
  } catch (loadError: any) {
    if (controller.signal.aborted || generation !== detailGeneration) return;
    setMessage(loadError?.message || "重保任务读取失败。", "error");
  } finally {
    if (generation === detailGeneration) loading.value = false;
  }
}

function applyResponse(response: Dict | null): void {
  activeResponse.value = response;
  const nextCells = clone(response?.cells || {});
  const definition = catalogSheets.value.find((item) => item.name === response?.sheet_type);
  if (definition?.kind === "check") {
    const items = Array.isArray(nextCells.template_items) && nextCells.template_items.length
      ? nextCells.template_items
      : clone(Array.isArray(definition.items) ? definition.items : []);
    nextCells.template_items = items;
    nextCells.template_revision = Math.max(
      0,
      Number(nextCells.template_revision ?? response?.template_revision ?? 0) || 0,
    );
    nextCells.template_customized = Boolean(
      nextCells.template_customized
      ?? response?.template_customized
      ?? (nextCells.template_revision > 0),
    );
    if (!nextCells.checks || typeof nextCells.checks !== "object") nextCells.checks = {};
    const validChecks: Dict = {};
    for (const item of items) {
      const key = String(item?.key || "");
      if (!key) continue;
      const saved = nextCells.checks[key] && typeof nextCells.checks[key] === "object"
        ? nextCells.checks[key]
        : {};
      validChecks[key] = {
        status: saved.status === "abnormal" ? "abnormal" : "normal",
        note: String(saved.note || ""),
      };
    }
    nextCells.checks = validChecks;
  }
  cells.value = nextCells;
  selectedSigners.value = clone(
    Array.isArray(response?.selected_signers)
      ? response.selected_signers
      : Array.isArray(response?.signatures)
        ? response.signatures
        : [],
  );
  signatureDrawerOpen.value = false;
  templateEditorOpen.value = false;
  templateResetConfirmOpen.value = false;
  templateCloseConfirmOpen.value = false;
  dirty.value = false;
  editRevision += 1;
}

function signaturePersonReady(person: Dict): boolean {
  if (typeof person?.ready === "boolean") return person.ready;
  const hasSignature = Boolean(person?.has_signature);
  if (!hasSignature) return false;
  const source = String(person?.source || "staff");
  if (source === "temporary" || source === "external" || person?.temp_id) return true;
  return Boolean(person?.usage_confirmed || person?.is_current_user || String(person?.open_id || "") === currentUserOpenId.value);
}

function responseUsesSharedSignatures(response: Dict): boolean {
  const definition = catalogSheets.value.find((item) => item.name === response?.sheet_type);
  return Boolean(definition?.has_signature);
}

function propagateSharedSignerState(
  people: Dict[],
  references?: Dict[],
  invalidatedResponseIds: string[] = [],
  invalidatedResponseVersions: Dict = {},
): void {
  const invalidated = new Set(invalidatedResponseIds.map(String));
  for (const response of selectedTask.value?.responses || []) {
    if (!responseUsesSharedSignatures(response)) continue;
    response.selected_signers = clone(people);
    if (references) response.signatures = clone(references);
    if (invalidated.has(String(response.response_id || ""))) {
      const nextVersion = Number(
        invalidatedResponseVersions[String(response.response_id || "")] || 0,
      );
      if (nextVersion > 0) response.version = nextVersion;
      response.status = response.status === "pending" ? "pending" : "draft";
      response.has_image = false;
      response.image_url = "";
      response.has_workbook = false;
      response.workbook_url = "";
    }
  }
}

function applySignatureSelection(people: Dict[]): void {
  selectedSigners.value = clone(people);
  propagateSharedSignerState(people);
  markDirty();
}

function refreshSignatureSelection(people: Dict[]): void {
  selectedSigners.value = clone(people);
  propagateSharedSignerState(people);
}

function handleSignatureStatus(text: string, tone: string): void {
  setMessage(text, tone || "info");
}

function requestTaskSwitch(taskId: string): void {
  requestSwitch(() => void selectTask(taskId, false));
}

function requestResponseSwitch(responseId: string): void {
  const response = orderedResponses.value.find((item) => item.response_id === responseId) || null;
  requestSwitch(() => applyResponse(response));
}

function requestSwitch(action: () => void): void {
  if (!dirty.value) {
    action();
    return;
  }
  pendingSwitch.value = action;
  confirmOpen.value = true;
}

function resolveSwitch(confirmed: boolean): void {
  confirmOpen.value = false;
  const action = pendingSwitch.value;
  pendingSwitch.value = null;
  if (confirmed && action) action();
}

function markDirty(): void {
  dirty.value = true;
  editRevision += 1;
  if (activeResponse.value?.image_url) {
    activeResponse.value.image_url = "";
    activeResponse.value.has_image = false;
  }
}

function setCheckStatus(key: string, status: "normal" | "abnormal"): void {
  if (!cells.value.checks?.[key]) return;
  cells.value.checks[key].status = status;
  if (status === "normal") cells.value.checks[key].note = "";
  markDirty();
}

function markAllNormal(): void {
  if (templateOutdated.value) return;
  if (abnormalCount.value > 0) {
    normalConfirmOpen.value = true;
    return;
  }
  applyAllNormal();
}

function applyAllNormal(): void {
  for (const item of Object.values(cells.value.checks || {}) as Dict[]) {
    item.status = "normal";
    item.note = "";
  }
  markDirty();
  setMessage("已全部设为正常，异常备注已清空。", "success");
}

function resolveMarkAllNormal(confirmed: boolean): void {
  normalConfirmOpen.value = false;
  if (confirmed) applyAllNormal();
}

function newTemplateItemKey(): string {
  const suffix = typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  return `custom_${suffix.replace(/[^0-9A-Za-z_-]/g, "")}`;
}

async function openTemplateEditor(): Promise<void> {
  if (templateOutdated.value || saving.value || activeDefinition.value?.kind !== "check") return;
  const generation = ++templateLoadGeneration;
  const fallbackRows = activeCheckItems.value.map((item) => ({
    key: String(item.key || newTemplateItemKey()),
    category: String(item.category || ""),
    content: String(item.content || ""),
  }));
  templateRows.value = clone(fallbackRows);
  templateInitialRows.value = clone(fallbackRows);
  templateEditorRevision.value = activeTemplateRevision.value;
  templateEditorCustomized.value = activeTemplateCustomized.value;
  templateEditorTouched.value = false;
  templateEditorOpen.value = true;
  templateLoading.value = true;
  try {
    const query = new URLSearchParams({
      scope: activeScope.value,
      sheet_type: String(activeResponse.value?.sheet_type || ""),
    });
    const template = await requestJson(`/api/critical-guard/scope-template?${query.toString()}`, {
      cache: "no-store",
    });
    if (generation !== templateLoadGeneration || !templateEditorOpen.value) return;
    const rows = Array.isArray(template.items) ? template.items.map((item: Dict) => ({
      key: String(item.key || newTemplateItemKey()),
      category: String(item.category || ""),
      content: String(item.content || ""),
    })) : fallbackRows;
    templateRows.value = clone(rows);
    templateInitialRows.value = clone(rows);
    templateEditorRevision.value = Math.max(0, Number(template.revision || 0));
    templateEditorCustomized.value = Boolean(template.customized);
    templateEditorTouched.value = false;
  } catch (loadError: any) {
    if (generation !== templateLoadGeneration) return;
    closeTemplateEditor();
    setMessage(loadError?.message || "楼栋检查模板读取失败。", "error");
  } finally {
    if (generation === templateLoadGeneration) templateLoading.value = false;
  }
}

function addTemplateRow(): void {
  if (templateSaving.value || templateRows.value.length >= 200) return;
  templateRows.value.push({ key: newTemplateItemKey(), category: "", content: "" });
  templateEditorTouched.value = true;
}

function removeTemplateRow(index: number): void {
  if (templateSaving.value || templateRows.value.length <= 1) return;
  templateRows.value.splice(index, 1);
  templateEditorTouched.value = true;
}

function closeTemplateEditor(): void {
  templateLoadGeneration += 1;
  templateEditorOpen.value = false;
  templateLoading.value = false;
  templateRows.value = [];
  templateInitialRows.value = [];
  templateEditorTouched.value = false;
  templateEditorRevision.value = 0;
  templateEditorCustomized.value = false;
}

function requestCloseTemplateEditor(): void {
  if (templateSaving.value) return;
  if (templateEditorDirty.value) {
    templateCloseConfirmOpen.value = true;
    return;
  }
  closeTemplateEditor();
}

function resolveTemplateClose(confirmed: boolean): void {
  templateCloseConfirmOpen.value = false;
  if (confirmed) closeTemplateEditor();
}

function normalizedTemplateRows(): Dict[] | null {
  if (!templateRows.value.length) {
    setMessage("检查模板至少需要保留一条检查内容。", "error");
    return null;
  }
  const rows = templateRows.value.map((item) => ({
    key: String(item.key || newTemplateItemKey()),
    category: String(item.category || "").trim(),
    content: String(item.content || "").trim(),
  }));
  const emptyIndex = rows.findIndex((item) => !item.content);
  if (emptyIndex >= 0) {
    setMessage(`第 ${emptyIndex + 1} 条检查内容不能为空。`, "error");
    return null;
  }
  return rows;
}

function scopeTemplateOperationId(kind: "save" | "reset", rows: Dict[]): string {
  const fingerprint = JSON.stringify({
    kind,
    scope: activeScope.value,
    sheetType: String(activeResponse.value?.sheet_type || ""),
    responseId: String(activeResponse.value?.response_id || ""),
    responseVersion: Number(activeResponse.value?.version || 0),
    templateRevision: templateEditorRevision.value,
    rows,
  });
  if (
    fingerprint !== templatePendingOperationFingerprint
    || !templatePendingOperationId
  ) {
    templatePendingOperationFingerprint = fingerprint;
    templatePendingOperationId = operationId();
  }
  return templatePendingOperationId;
}

function clearScopeTemplateOperation(): void {
  templatePendingOperationFingerprint = "";
  templatePendingOperationId = "";
}

function applyTemplateUpdate(result: Dict): void {
  if (result?.template) {
    templateEditorRevision.value = Math.max(0, Number(result.template.revision || 0));
    templateEditorCustomized.value = Boolean(result.template.customized);
  }
  const updated = result?.response;
  if (!updated?.response_id) return;
  const index = selectedTask.value?.responses?.findIndex(
    (item: Dict) => item.response_id === updated.response_id,
  ) ?? -1;
  if (selectedTask.value && index >= 0) selectedTask.value.responses[index] = updated;
  if (String(activeResponse.value?.response_id || "") === String(updated.response_id)) {
    applyResponse(updated);
  }
}

async function saveScopeTemplate(): Promise<void> {
  if (!activeResponse.value || templateSaving.value || !templateEditorDirty.value) return;
  const rows = normalizedTemplateRows();
  if (!rows) return;
  const pendingOperationId = scopeTemplateOperationId("save", rows);
  let savedSuccessfully = false;
  templateSaving.value = true;
  try {
    const result = await requestJson("/api/critical-guard/scope-template", {
      method: "PUT",
      body: JSON.stringify({
        scope: activeScope.value,
        sheet_type: String(activeResponse.value.sheet_type || ""),
        items: rows,
        response_id: String(activeResponse.value.response_id || ""),
        cells: clone(cells.value),
        expected_revision: templateEditorRevision.value,
        expected_response_version: activeResponse.value.version,
        operation_id: pendingOperationId,
      }),
      timeoutMs: 60_000,
    });
    applyTemplateUpdate(result);
    clearScopeTemplateOperation();
    savedSuccessfully = true;
    setMessage(`${activeScope.value}楼检查模板已保存并应用。`, "success");
  } catch (saveError: any) {
    setMessage(saveError?.message || "楼栋检查模板保存失败。", "error");
  } finally {
    templateSaving.value = false;
  }
  if (savedSuccessfully) {
    try {
      await loadTasks();
    } catch (refreshError: any) {
      setMessage(
        `检查模板已保存，但任务统计刷新失败：${refreshError?.message || "请稍后刷新"}`,
        "warning",
      );
    }
  }
}

function resolveTemplateReset(confirmed: boolean): void {
  templateResetConfirmOpen.value = false;
  if (confirmed) void resetScopeTemplate();
}

async function resetScopeTemplate(): Promise<void> {
  if (!activeResponse.value || templateSaving.value) return;
  const pendingOperationId = scopeTemplateOperationId("reset", []);
  let resetSuccessfully = false;
  templateSaving.value = true;
  try {
    const result = await requestJson("/api/critical-guard/scope-template/reset", {
      method: "POST",
      body: JSON.stringify({
        scope: activeScope.value,
        sheet_type: String(activeResponse.value.sheet_type || ""),
        items: [],
        response_id: String(activeResponse.value.response_id || ""),
        cells: clone(cells.value),
        expected_revision: templateEditorRevision.value,
        expected_response_version: activeResponse.value.version,
        operation_id: pendingOperationId,
      }),
      timeoutMs: 60_000,
    });
    applyTemplateUpdate(result);
    clearScopeTemplateOperation();
    resetSuccessfully = true;
    setMessage(`${activeScope.value}楼已恢复系统默认检查模板。`, "success");
  } catch (resetError: any) {
    setMessage(resetError?.message || "恢复默认检查模板失败。", "error");
  } finally {
    templateSaving.value = false;
  }
  if (resetSuccessfully) {
    try {
      await loadTasks();
    } catch (refreshError: any) {
      setMessage(
        `已恢复默认模板，但任务统计刷新失败：${refreshError?.message || "请稍后刷新"}`,
        "warning",
      );
    }
  }
}

function openSourceFilePicker(): void {
  if (saving.value || templateOutdated.value) return;
  sourceFileInput.value?.click();
}

function handleSourceFileChange(event: Event): void {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0] || null;
  input.value = "";
  if (file) void uploadSourceFile(file);
}

function handleSourceFileDrop(event: DragEvent): void {
  sourceFileDragging.value = false;
  if (saving.value || templateOutdated.value) return;
  const file = Array.from(event.dataTransfer?.files || []).find((item) => item.name.toLowerCase().endsWith(".xlsx"));
  if (!file) {
    setMessage("请拖入 .xlsx 楼栋清单文件。", "error");
    return;
  }
  void uploadSourceFile(file);
}

function retrySourceFilePreview(): void {
  sourceFilePreviewFailed.value = false;
  sourceFilePreviewRevision.value += 1;
}

async function uploadSourceFile(file: File): Promise<void> {
  if (saving.value || templateOutdated.value || !activeResponse.value || activeDefinition.value?.input_mode !== "file") return;
  if (!file.name.toLowerCase().endsWith(".xlsx")) {
    setMessage("楼栋清单只支持 .xlsx 文件。", "error");
    return;
  }
  if (file.size <= 0 || file.size > 20 * 1024 * 1024) {
    setMessage(file.size <= 0 ? "上传文件为空。" : "楼栋清单文件不能超过 20MB。", "error");
    return;
  }
  const responseId = String(activeResponse.value.response_id || "");
  const body = new FormData();
  body.append("file", file, file.name);
  body.append("scope", activeScope.value);
  body.append("response_id", responseId);
  body.append("expected_version", String(activeResponse.value.version || ""));
  saving.value = true;
  sourceFileUploading.value = true;
  try {
    const updated = await requestJson("/api/critical-guard/source-files", {
      method: "POST",
      body,
      timeoutMs: 60_000,
    });
    const index = selectedTask.value?.responses?.findIndex((item: Dict) => item.response_id === updated.response_id) ?? -1;
    if (selectedTask.value && index >= 0) selectedTask.value.responses[index] = updated;
    if (String(activeResponse.value?.response_id || "") === responseId) {
      applyResponse(updated);
      sourceFilePreviewFailed.value = false;
      sourceFilePreviewRevision.value = Date.now();
      if (imageViewerUrl.value) closeImage();
    }
    setMessage(`${file.name} 已保存为${activeScope.value}楼${String(updated.sheet_type || "清单")}文件。`, "success");
    try {
      await loadTasks();
    } catch (refreshError: any) {
      setMessage(`文件已上传，但任务统计刷新失败：${refreshError?.message || "请稍后刷新"}`, "warning");
    }
  } catch (uploadError: any) {
    setMessage(uploadError?.message || "楼栋清单上传失败。", "error");
  } finally {
    sourceFileUploading.value = false;
    saving.value = false;
  }
}

async function saveResponse(generateImage: boolean): Promise<void> {
  if (!activeResponse.value || saving.value) return;
  const regeneratingImage = generateImage && hasGeneratedArtifact.value;
  const responseId = String(activeResponse.value.response_id || "");
  const submittedRevision = editRevision;
  const submittedCells = clone(cells.value);
  saving.value = true;
  try {
    const updated = await requestJson(`/api/critical-guard/responses/${encodeURIComponent(responseId)}`, {
      method: "PUT",
      body: JSON.stringify({
        scope: activeScope.value,
        cells: submittedCells,
        signatures: activeDefinition.value?.has_signature
          ? selectedSigners.value.map((person) => ({
            source: String(person.source || (person.temp_id ? "temporary" : "staff")),
            role: "inspector",
            record_id: String(person.record_id || ""),
            temp_id: String(person.temp_id || ""),
            name: String(person.name || person.display_name || ""),
          }))
          : [],
        signature_record_id: "",
        generate_image: generateImage,
        expected_version: activeResponse.value.version,
        operation_id: publishOperationId || (publishOperationId = operationId()),
      }),
      timeoutMs: 60_000,
    });
    const index = selectedTask.value?.responses?.findIndex((item: Dict) => item.response_id === updated.response_id) ?? -1;
    if (selectedTask.value && index >= 0) selectedTask.value.responses[index] = updated;
    if (activeDefinition.value?.has_signature) {
      propagateSharedSignerState(
        Array.isArray(updated.selected_signers) ? updated.selected_signers : selectedSigners.value,
        Array.isArray(updated.signatures) ? updated.signatures : undefined,
        Array.isArray(updated.invalidated_response_ids) ? updated.invalidated_response_ids : [],
        updated.invalidated_response_versions && typeof updated.invalidated_response_versions === "object"
          ? updated.invalidated_response_versions
          : {},
      );
    }
    if (String(activeResponse.value?.response_id || "") === responseId) {
      if (editRevision === submittedRevision) {
        applyResponse(updated);
      } else {
        activeResponse.value = updated;
        activeResponse.value.image_url = "";
        activeResponse.value.has_image = false;
        dirty.value = true;
      }
    }
    setMessage(
      generateImage
        ? regeneratingImage ? "检查图片已重新生成。" : "检查图片已生成。"
        : "填报内容已保存。",
      "success",
    );
  } catch (saveError: any) {
    setMessage(saveError?.message || "保存失败。", "error");
    return;
  } finally {
    saving.value = false;
  }
  try {
    await loadTasks();
  } catch (refreshError: any) {
    setMessage(
      `${generateImage ? regeneratingImage ? "检查图片已重新生成" : "检查图片已生成" : "填报内容已保存"}，但任务统计刷新失败：${refreshError?.message || "请稍后刷新"}`,
      "warning",
    );
  }
}

async function publishTask(): Promise<void> {
  if (saving.value) return;
  const taskName = String(publishForm.value.name || "").trim();
  if (!taskName) {
    setMessage("请填写重保任务名称。", "error");
    return;
  }
  if (!publishForm.value.sheetTypes.length) {
    setMessage("请至少选择一张检查表。", "error");
    return;
  }
  if (!publishForm.value.targetScopes.length) {
    setMessage("请至少选择一个填写楼栋。", "error");
    return;
  }
  saving.value = true;
  let created: Dict | null = null;
  try {
    created = await requestJson("/api/critical-guard/tasks", {
      method: "POST",
      body: JSON.stringify({
        operation_id: operationId(),
        name: taskName,
        sheet_types: publishForm.value.sheetTypes,
        target_scopes: publishForm.value.targetScopes,
      }),
    });
    publishOpen.value = false;
    publishForm.value.name = "";
    publishOperationId = "";
    setMessage("重保任务已发布。", "success");
  } catch (publishError: any) {
    setMessage(publishError?.message || "发布失败。", "error");
    return;
  } finally {
    saving.value = false;
  }
  try {
    await loadTasks();
    if (created?.task_id) await selectTask(created.task_id, true);
  } catch (refreshError: any) {
    setMessage(
      `重保任务已发布，但列表刷新失败：${refreshError?.message || "请稍后刷新"}`,
      "warning",
    );
  }
}

function requestDeleteTask(task: Dict): void {
  if (loading.value || saving.value || deletingTaskId.value) return;
  pendingDeleteTask.value = task;
  deleteConfirmOpen.value = true;
}

async function resolveDeleteTask(confirmed: boolean): Promise<void> {
  deleteConfirmOpen.value = false;
  const task = pendingDeleteTask.value;
  pendingDeleteTask.value = null;
  if (!confirmed || !task || deletingTaskId.value) return;

  const taskId = String(task.task_id || "").trim();
  if (!taskId) return;
  const deletedIndex = tasks.value.findIndex((item) => item.task_id === taskId);
  deletingTaskId.value = taskId;
  try {
    const result = await requestJson(
      `/api/critical-guard/tasks/${encodeURIComponent(taskId)}?operation_id=${encodeURIComponent(operationId())}`,
      { method: "DELETE" },
    );
    tasks.value = tasks.value.filter((item) => item.task_id !== taskId);
    const deletedSelected = selectedTask.value?.task_id === taskId;
    if (deletedSelected) clearTaskSelection();
    setMessage(
      String(result.cleanup_warning || result.message || "重保任务已删除。"),
      result.cleanup_warning ? "warning" : "success",
    );
    if (deletedSelected && tasks.value.length) {
      const nextIndex = Math.min(Math.max(0, deletedIndex), tasks.value.length - 1);
      await selectTask(String(tasks.value[nextIndex].task_id || ""), true);
    }
  } catch (deleteError: any) {
    setMessage(deleteError?.message || "重保任务删除失败。", "error");
  } finally {
    deletingTaskId.value = "";
  }
}

function adminSheetResponses(sheet: string): Dict[] {
  return (selectedTask.value?.responses || [])
    .filter((item: Dict) => item.sheet_type === sheet)
    .sort((left: Dict, right: Dict) => String(left.scope).localeCompare(String(right.scope)));
}

function downloadAdminSheet(): void {
  if (!selectedTask.value || !adminActiveSheet.value) return;
  const url = `/api/critical-guard/tasks/${encodeURIComponent(selectedTask.value.task_id)}/download?sheet_type=${encodeURIComponent(adminActiveSheet.value)}`;
  const link = document.createElement("a");
  link.href = url;
  link.download = "";
  document.body.appendChild(link);
  link.click();
  link.remove();
}

function openBuilding(scope: string): void {
  navigate(`/critical-guard?scope=${encodeURIComponent(scope)}`);
}

function openAdmin(): void {
  navigate("/critical-guard?mode=admin");
}

function openImage(url: string, title: string): void {
  imageViewerUrl.value = url;
  imageViewerTitle.value = title;
}

function adminImageSrc(response: Dict): string {
  const responseId = String(response?.response_id || "");
  const source = String(response?.image_url || "");
  const retryToken = Number(adminImageRetryTokens.value[responseId] || 0);
  if (!source || !retryToken) return source;
  return `${source}${source.includes("?") ? "&" : "?"}retry=${retryToken}`;
}

function adminImageFailed(response: Dict): boolean {
  return Boolean(adminImageFailures.value[adminImageSrc(response)]);
}

function markAdminImageFailed(response: Dict): void {
  const source = adminImageSrc(response);
  if (!source) return;
  adminImageFailures.value = { ...adminImageFailures.value, [source]: true };
}

function retryAdminImage(response: Dict): void {
  const responseId = String(response?.response_id || "");
  if (!responseId) return;
  adminImageRetryTokens.value = {
    ...adminImageRetryTokens.value,
    [responseId]: Date.now(),
  };
}

function closeImage(): void {
  imageViewerUrl.value = "";
  imageViewerTitle.value = "";
}

function statusText(status: string): string {
  return status === "submitted" ? "已生成" : status === "draft" ? "已保存" : "待填写";
}

function statusClass(status: string): string {
  return `status-chip ${status || "pending"}`;
}

function formatDateTime(value: unknown): string {
  const numeric = Number(value || 0);
  if (!numeric) return "";
  return new Date(numeric * 1000).toLocaleString("zh-CN", { hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function formatWeatherTime(value: unknown): string {
  const numeric = Number(value || 0);
  if (!numeric) return "未查询";
  return new Date(numeric * 1000).toLocaleString("zh-CN", {
    hour12: false,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatFileSize(value: unknown): string {
  const size = Math.max(0, Number(value || 0));
  if (size < 1024) return `${Math.round(size)} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

async function reloadCurrentView(): Promise<void> {
  if (loading.value) return;
  loading.value = true;
  try {
    await loadTasks();
    if (selectedTask.value) await selectTask(selectedTask.value.task_id, viewMode.value === "admin");
    setMessage("已刷新。", "success");
  } catch (reloadError: any) {
    setMessage(reloadError?.message || "刷新失败。", "error");
  } finally {
    loading.value = false;
  }
}

function requestReload(): void {
  requestSwitch(() => void reloadCurrentView());
}

function requestPageExit(): void {
  requestSwitch(() => navigateBack(backTarget.value));
}

watch(
  () => `${String(effectiveSourceFile.value?.file_id || "")}:${String(effectiveSourceFile.value?.sha256 || "")}`,
  () => {
    sourceFilePreviewFailed.value = false;
    sourceFilePreviewRevision.value = Date.now();
  },
);

watch(
  publishForm,
  () => {
    if (!saving.value) publishOperationId = "";
  },
  { deep: true },
);

watch(() => [props.scope, props.adminMode], () => {
  listGeneration += 1;
  detailGeneration += 1;
  listController?.abort();
  detailController?.abort();
  tasks.value = [];
  selectedTask.value = null;
  activeResponse.value = null;
  dirty.value = false;
  weatherStatus.value = null;
  weatherTaskRefreshJobId = "";
  manualWeatherJobId = "";
  weatherNoTaskDialogOpen.value = false;
  if (weatherPollTimer) window.clearTimeout(weatherPollTimer);
  weatherPollTimer = null;
  if (adminDetailPollTimer) window.clearTimeout(adminDetailPollTimer);
  adminDetailPollTimer = null;
  adminDetailPollController?.abort();
  adminDetailPollController = null;
  adminDetailPollInFlight = false;
  adminDetailPollFailureReported = false;
  void loadBootstrap();
});

onMounted(() => {
  componentUnmounted = false;
  document.addEventListener("visibilitychange", handlePageVisibilityChange);
  void loadBootstrap();
});

onBeforeUnmount(() => {
  componentUnmounted = true;
  bootstrapGeneration += 1;
  listGeneration += 1;
  detailGeneration += 1;
  bootstrapController?.abort();
  listController?.abort();
  detailController?.abort();
  adminDetailPollController?.abort();
  if (weatherPollTimer) window.clearTimeout(weatherPollTimer);
  weatherPollTimer = null;
  if (adminDetailPollTimer) window.clearTimeout(adminDetailPollTimer);
  adminDetailPollTimer = null;
  document.removeEventListener("visibilitychange", handlePageVisibilityChange);
});
</script>

<style scoped>
.guard-page {
  width: min(1680px, calc(100vw - 32px));
  margin: 18px auto 48px;
  color: #10213d;
}

.guard-page__header {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  margin-bottom: 14px;
}

.guard-page__header span,
.landing-title span,
.workspace-header span,
.admin-results header span {
  color: #6b7e99;
  font-size: 12px;
  font-weight: 800;
}

.guard-page__header h1,
.landing-title h2,
.workspace-header h2,
.admin-results h2 {
  margin: 2px 0 0;
  font-size: 21px;
  letter-spacing: 0;
}

.icon-button {
  display: inline-grid;
  width: 40px;
  height: 40px;
  place-items: center;
  border: 1px solid #cfe0f7;
  border-radius: 12px;
  background: #fff;
  color: #195ed1;
  cursor: pointer;
}

.icon-button:disabled { opacity: .55; cursor: wait; }
.spinning { animation: spin .8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

.inline-message {
  margin-bottom: 12px;
  border: 1px solid #cfe0ff;
  border-radius: 10px;
  padding: 9px 13px;
  background: #f4f8ff;
  color: #1f5ebd;
  font-size: 13px;
  font-weight: 800;
}
.inline-message.success { border-color: #bce8d5; background: #effaf5; color: #087f5b; }
.inline-message.error { border-color: #fecaca; background: #fff1f2; color: #b42318; }

.page-state {
  min-height: 320px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  border: 1px solid #dce7f5;
  border-radius: 12px;
  background: #fff;
  color: #60728d;
  font-weight: 800;
}
.page-state.compact { min-height: 240px; border: 0; }
.page-state.error { flex-direction: column; color: #b42318; }
.page-state button { border: 1px solid #bdd3f3; border-radius: 10px; padding: 8px 14px; background: #fff; color: #195ed1; font-weight: 800; }
.loader { width: 22px; height: 22px; border: 3px solid #dbeafe; border-top-color: #2563eb; border-radius: 50%; animation: spin .8s linear infinite; }

.landing-view,
.admin-view,
.building-view {
  border: 1px solid #d8e4f3;
  border-radius: 14px;
  background: #f8fbff;
  padding: 20px;
  box-shadow: 0 14px 38px rgba(26, 79, 151, .08);
}

.landing-title,
.admin-toolbar,
.building-summary,
.workspace-header,
.admin-results > header,
.generated-preview > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
}

.building-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 14px;
  margin-top: 16px;
}

.building-card {
  position: relative;
  min-height: 160px;
  display: grid;
  grid-template-columns: 48px 1fr;
  grid-template-rows: auto auto 1fr;
  align-items: center;
  gap: 8px 14px;
  overflow: hidden;
  border: 1px solid #cfe0f6;
  border-radius: 12px;
  padding: 18px;
  background: #fff;
  color: #112b52;
  text-align: left;
  cursor: pointer;
  transition: border-color .15s ease, box-shadow .15s ease, transform .15s ease;
}
.building-card::before { content: ""; position: absolute; inset: 0 0 auto; height: 4px; background: #2f73e7; }
.building-card:hover:not(:disabled) { border-color: #83b5f5; box-shadow: 0 12px 26px rgba(36, 99, 190, .12); transform: translateY(-2px); }
.building-card.disabled { opacity: .48; cursor: not-allowed; }
.building-card.admin::before { background: #e64f68; }
.building-card__icon { display: grid; width: 48px; height: 48px; place-items: center; border-radius: 10px; background: #e9f2ff; color: #2368d9; }
.building-card.admin .building-card__icon { background: #fff0f2; color: #d93655; }
.building-card > strong { font-size: 20px; }
.building-card > div { grid-column: 1 / -1; display: flex; gap: 8px; }
.building-card > div span { border-radius: 999px; padding: 5px 9px; background: #f0f5fb; color: #4e6481; font-size: 12px; font-weight: 800; }
.building-card > b { grid-column: 1 / -1; display: inline-flex; align-items: center; justify-content: flex-end; color: #1e63d8; font-size: 13px; }

.primary-button,
.secondary-button,
.table-actions button {
  min-height: 38px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 7px;
  border-radius: 10px;
  padding: 0 14px;
  font: inherit;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
}
.primary-button { border: 1px solid #1760dc; background: #1764e8; color: #fff; }
.secondary-button { border: 1px solid #c8d9ef; background: #fff; color: #1d5ebc; }
.primary-button:disabled,
.secondary-button:disabled { opacity: .5; cursor: not-allowed; }

.summary-pills { display: flex; gap: 8px; }
.summary-pills span,
.building-summary strong,
.building-summary b { border: 1px solid #d6e3f3; border-radius: 999px; padding: 7px 11px; background: #fff; color: #4f6582; font-size: 12px; }
.building-summary > span { display: inline-flex; align-items: center; gap: 8px; color: #164f9c; font-weight: 900; }
.building-summary strong { margin-left: auto; color: #b45309; }
.building-summary b { color: #087f5b; }

.weather-automation {
  display: grid;
  grid-template-columns: minmax(250px, 1fr) minmax(390px, 1.45fr) auto;
  align-items: center;
  gap: 14px 20px;
  margin-top: 14px;
  border: 1px solid #c9ddf5;
  border-left: 4px solid #2f73e7;
  border-radius: 11px;
  padding: 12px 14px;
  background: #fff;
}
.weather-automation.running { border-left-color: #0ea5a8; background: #f8ffff; }
.weather-automation.paused { border-left-color: #71839a; background: #f7f9fc; }
.weather-automation.failed { border-left-color: #e05263; }
.weather-automation__identity { min-width: 0; display: flex; align-items: center; gap: 10px; }
.weather-automation__identity > div { min-width: 0; }
.weather-automation__identity strong,
.weather-automation__identity span { display: block; }
.weather-automation__identity strong { color: #173259; font-size: 14px; }
.weather-automation__identity > div span { margin-top: 3px; overflow: hidden; color: #607590; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
.weather-automation__icon { display: grid !important; width: 38px; height: 38px; flex: 0 0 auto; place-items: center; border-radius: 9px; background: #e9f3ff; color: #2368d9 !important; }
.weather-automation__times { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
.weather-automation__times span { min-width: 0; border-left: 1px solid #e1eaf5; padding-left: 10px; color: #71839a; font-size: 11px; }
.weather-automation__times b { display: block; margin-top: 2px; overflow: hidden; color: #29486f; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }
.weather-automation__action { display: flex; align-items: center; justify-content: flex-end; gap: 10px; }
.weather-automation__action > span { max-width: 260px; overflow-wrap: anywhere; color: #3970b9; font-size: 12px; font-weight: 850; }
.weather-automation__action > span.error { color: #b42318; }
.weather-automation__buttons { display: flex; align-items: center; gap: 8px; }
.weather-refresh-button { white-space: nowrap; }
.weather-pause-button { white-space: nowrap; }
.weather-automation__result { grid-column: 1 / -1; border-top: 1px solid #e5edf7; padding-top: 9px; color: #426181; font-size: 12px; }
.weather-archive-failures { grid-column: 1 / -1; display: grid; gap: 6px; }
.weather-archive-failures > div { display: flex; align-items: center; justify-content: space-between; gap: 10px; border: 1px solid #fecdd3; border-radius: 8px; padding: 7px 9px; background: #fff5f6; color: #a8293b; font-size: 12px; }
.weather-archive-failures button { flex: 0 0 auto; border: 1px solid #e99aa8; border-radius: 7px; padding: 5px 9px; background: #fff; color: #ad263d; font: inherit; font-size: 11px; font-weight: 850; cursor: pointer; }

.publish-panel {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) minmax(340px, 1.4fr) minmax(260px, .8fr);
  gap: 14px;
  margin-top: 14px;
  border: 1px solid #cfe0f7;
  border-radius: 12px;
  padding: 16px;
  background: #fff;
}
.field,
.publish-panel fieldset { min-width: 0; margin: 0; border: 0; padding: 0; }
.field span,
.publish-panel legend,
.sheet-meta label span,
.weather-fields label span,
.signer-box span,
.suggestions-field span { display: block; margin-bottom: 6px; color: #536987; font-size: 12px; font-weight: 850; }
input,
textarea { width: 100%; box-sizing: border-box; border: 1px solid #c9d9ed; border-radius: 8px; background: #fff; color: #14233b; font: inherit; font-size: 13px; outline: none; }
input { height: 36px; padding: 0 10px; }
textarea { padding: 9px 10px; resize: vertical; }
input:focus,
textarea:focus { border-color: #4f8ff2; box-shadow: 0 0 0 3px rgba(47, 115, 231, .1); }
input[readonly] { background: #f3f7fc; color: #536987; }
.choice-grid { display: grid; gap: 7px; }
.choice-grid.sheets { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.choice-grid.scopes { grid-template-columns: repeat(5, minmax(0, 1fr)); }
.choice-grid label { position: relative; }
.choice-grid input { position: absolute; opacity: 0; }
.choice-grid label span { display: grid; min-height: 36px; place-items: center; border: 1px solid #cfddf0; border-radius: 8px; background: #f9fbfe; color: #516783; font-size: 12px; font-weight: 850; cursor: pointer; }
.choice-grid input:checked + span { border-color: #4f8ff2; background: #eaf3ff; color: #165dc7; box-shadow: inset 3px 0 0 #2f73e7; }
.publish-actions { grid-column: 1 / -1; display: flex; justify-content: flex-end; gap: 8px; }

.admin-layout,
.fill-layout { display: grid; grid-template-columns: 260px minmax(0, 1fr); gap: 14px; margin-top: 14px; align-items: start; }
.task-list { display: grid; gap: 7px; max-height: calc(100vh - 250px); overflow: auto; padding-right: 4px; }
.task-list .task-select { width: 100%; display: grid; gap: 6px; border: 1px solid #d6e3f2; border-radius: 10px; padding: 11px 12px; background: #fff; color: #183252; text-align: left; cursor: pointer; }
.task-list .task-select.active { border-color: #62a0f4; background: #eaf3ff; box-shadow: inset 4px 0 0 #2f73e7; }
.task-list .task-select strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }
.task-list .task-select span { color: #77879c; font-size: 11px; }
.task-list .task-select small { color: #b45309; font-weight: 850; }
.task-list .task-select small.done { color: #087f5b; }
.admin-task-item { position: relative; min-width: 0; }
.admin-task-item .task-select { padding-right: 46px; }
.task-delete { position: absolute; top: 8px; right: 8px; display: inline-grid; width: 30px; height: 30px; place-items: center; border: 1px solid #f3c8cf; border-radius: 8px; padding: 0; background: #fff7f8; color: #c9364f; cursor: pointer; transition: border-color .15s ease, background .15s ease, color .15s ease; }
.task-delete:hover:not(:disabled) { border-color: #e76a7d; background: #fff0f2; color: #a91934; }
.task-delete:disabled { opacity: .5; cursor: not-allowed; }
.task-delete.deleting { animation: pulse-delete .8s ease-in-out infinite alternate; }
@keyframes pulse-delete { to { opacity: .42; } }
.empty-inline { padding: 24px 10px; color: #7b8ca2; text-align: center; font-size: 13px; }

.admin-results,
.sheet-workspace { min-width: 0; border: 1px solid #d8e5f4; border-radius: 12px; background: #fff; }
.admin-results { padding: 16px; }
.sheet-workspace { overflow: hidden; }
.workspace-header { padding: 14px 16px 10px; }
.workspace-header h2,
.admin-results h2 { font-size: 17px; }

.sheet-tabs { display: flex; gap: 6px; overflow-x: auto; margin: 12px 0; padding-bottom: 3px; }
.fill-tabs { margin: 0; border-top: 1px solid #e5edf8; border-bottom: 1px solid #e5edf8; padding: 8px 12px; background: #f8fbff; }
.sheet-tabs button { flex: 0 0 auto; min-height: 34px; display: inline-flex; align-items: center; gap: 6px; border: 1px solid #d4e1f1; border-radius: 9px; padding: 0 11px; background: #fff; color: #526882; font: inherit; font-size: 12px; font-weight: 850; cursor: pointer; }
.sheet-tabs button.active { border-color: #4f8ff2; background: #eaf3ff; color: #165dc7; }
.sheet-tabs button span { border-radius: 999px; padding: 2px 5px; background: #edf2f8; color: inherit; font-size: 10px; }

.result-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.result-grid article { min-width: 0; border: 1px solid #d8e5f4; border-radius: 10px; overflow: hidden; background: #fbfdff; }
.result-grid article > header { display: flex; justify-content: space-between; padding: 9px 10px; }
.result-grid article > footer { overflow: hidden; padding: 8px 10px 4px; color: #75869d; font-size: 11px; text-overflow: ellipsis; white-space: nowrap; }
.result-thumbnail { position: relative; width: 100%; height: 160px; display: grid; place-items: center; overflow: hidden; border: 0; border-top: 1px solid #e5edf8; border-bottom: 1px solid #e5edf8; background: #eef4fb; cursor: zoom-in; }
.result-thumbnail img { width: 100%; height: 100%; object-fit: contain; }
.result-thumbnail svg { position: absolute; right: 8px; bottom: 8px; border-radius: 8px; padding: 6px; box-sizing: content-box; background: rgba(18, 74, 153, .82); color: #fff; }
.result-empty { height: 160px; display: grid; place-items: center; align-content: center; gap: 7px; color: #8999ac; font-size: 12px; }
.result-empty.image-error { color: #9a5b13; background: #fffaf2; }
.result-empty.image-error button { min-height: 30px; border: 1px solid #e6c48f; border-radius: 8px; padding: 0 10px; background: #fff; color: #8b5312; font: inherit; font-size: 11px; font-weight: 850; cursor: pointer; }
.result-empty.image-error button:hover { border-color: #ce9c50; background: #fff5e5; }

.status-chip { display: inline-flex; border-radius: 999px; padding: 5px 8px; background: #fff7ed; color: #b45309 !important; font-size: 11px !important; font-weight: 900 !important; }
.status-chip.submitted { background: #ecfdf5; color: #087f5b !important; }
.status-chip.draft { background: #eef5ff; color: #1d5ebc !important; }

.building-summary { min-height: 42px; border-bottom: 1px solid #dce7f4; padding: 0 0 12px; }
.sheet-meta { display: grid; grid-template-columns: minmax(180px, 1fr) 180px minmax(220px, .8fr); gap: 10px; padding: 12px 16px; background: #fbfdff; }
.signer-box { min-height: 55px; display: flex; align-items: center; gap: 9px; border: 1px solid #cfe0f3; border-radius: 9px; padding: 6px 9px; background: #fff; }
.signer-box.missing { border-color: #fed7aa; background: #fff9f2; }
.signer-box > div { min-width: 0; }
.signer-box span { margin: 0; }
.signer-box strong { display: block; overflow: hidden; color: #173259; font-size: 13px; text-overflow: ellipsis; white-space: nowrap; }
.signer-previews { position: relative; flex: 0 0 auto; min-width: 58px; height: 42px; display: flex; align-items: center; color: #52719a; }
.signer-previews b { min-width: 26px; height: 26px; display: grid; place-items: center; margin-left: 7px; border: 2px solid #fff; border-radius: 999px; background: #1d63d8; color: #fff; font-size: 10px; }
.manage-signature-button { margin-left: auto; min-height: 34px; display: inline-flex; align-items: center; gap: 6px; border: 1px solid #9fc2f2; border-radius: 9px; padding: 0 10px; background: #eef5ff; color: #175ab9; font: inherit; font-size: 12px; font-weight: 900; cursor: pointer; white-space: nowrap; }
.manage-signature-button:hover { border-color: #4f8ff2; background: #e1efff; }
.weather-fields { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; padding: 0 16px 12px; }

.visually-hidden {
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  overflow: hidden !important;
  clip: rect(0 0 0 0) !important;
  white-space: nowrap !important;
}
.scope-file-panel {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
  min-height: 132px;
  margin: 14px 16px 18px;
  border: 2px dashed #9fc2ef;
  border-radius: 10px;
  padding: 18px;
  background: #f7fbff;
  transition: border-color .16s ease, background .16s ease;
}
.scope-file-panel.dragging { border-color: #2f73e7; background: #eaf3ff; }
.scope-file-icon { display: grid; width: 54px; height: 54px; place-items: center; border-radius: 10px; background: #e5f0ff; color: #1d63d8; }
.scope-file-copy { min-width: 0; }
.scope-file-copy span,
.scope-file-copy strong,
.scope-file-copy small { display: block; }
.scope-file-copy span { color: #55708f; font-size: 12px; font-weight: 850; }
.scope-file-copy strong { margin-top: 4px; overflow: hidden; color: #142d52; font-size: 15px; text-overflow: ellipsis; white-space: nowrap; }
.scope-file-copy small { margin-top: 5px; color: #73859c; font-size: 12px; }
.scope-file-actions { display: flex; align-items: center; gap: 8px; }
.scope-file-actions a { text-decoration: none; }
.scope-file-preview-card {
  margin: -6px 16px 18px;
  overflow: hidden;
  border: 1px dashed #bdcad9;
  border-radius: 10px;
  background: #fbfcfe;
}
.scope-file-preview-card > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-height: 56px;
  padding: 9px 12px;
  border-bottom: 1px solid #e0eaf5;
  background: #f4f6f9;
}
.scope-file-preview-heading { min-width: 0; }
.scope-file-preview-card > header span,
.scope-file-preview-card > header strong { display: block; }
.scope-file-preview-kicker { display: flex !important; align-items: center; gap: 6px; color: #52657b; font-size: 12px; font-weight: 850; }
.scope-file-preview-kicker b { border: 1px solid #cbd5e1; border-radius: 999px; padding: 2px 7px; background: #fff; color: #64748b; font-size: 10px; line-height: 1.2; }
.scope-file-preview-card > header strong { margin-top: 2px; overflow: hidden; color: #17345d; font-size: 13px; text-overflow: ellipsis; white-space: nowrap; }
.scope-file-preview-image {
  position: relative;
  width: 100%;
  min-height: 180px;
  max-height: 380px;
  display: grid;
  place-items: start center;
  overflow: hidden;
  border: 0;
  padding: 12px;
  background: #eef3f8;
  cursor: zoom-in;
}
.scope-file-preview-image img {
  display: block;
  max-width: 100%;
  max-height: 350px;
  object-fit: contain;
  object-position: top center;
  border: 1px solid #d3deea;
  background: #fff;
  box-shadow: 0 4px 14px rgb(31 66 110 / 10%);
}
.scope-file-preview-stamp,
.scope-file-preview-zoom {
  position: absolute;
  border-radius: 999px;
  padding: 5px 9px;
  color: #fff;
  font-size: 11px;
  font-weight: 850;
}
.scope-file-preview-stamp { top: 18px; left: 18px; background: rgb(71 85 105 / 88%); }
.scope-file-preview-zoom { right: 18px; bottom: 16px; background: rgb(17 53 96 / 82%); }
.scope-file-preview-error { min-height: 150px; display: grid; place-items: center; align-content: center; gap: 10px; padding: 20px; color: #8a5b16; font-size: 13px; }

.sheet-table-shell,
.structured-sections { padding: 0 16px 16px; }
.table-actions { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin: 2px 0 8px; }
.table-actions > div { display: flex; align-items: center; gap: 8px; }
.table-actions span { color: #b45309; font-size: 12px; font-weight: 900; }
.table-actions small { border: 1px solid #d6e1ed; border-radius: 999px; padding: 3px 8px; background: #f8fafc; color: #64748b; font-size: 11px; font-weight: 800; }
.table-actions small.customized { border-color: #b7d4fa; background: #eef6ff; color: #1f65c2; }
.table-actions button { min-height: 32px; border: 1px solid #bdd5f3; background: #fff; color: #1d5ebc; }
.check-table,
.entry-table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 12px; }
.check-table th,
.check-table td,
.entry-table th,
.entry-table td { border: 1px solid #d8e4f1; padding: 6px 7px; vertical-align: middle; }
.check-table th,
.entry-table th { position: sticky; top: 0; z-index: 2; background: #e9f2fc; color: #245ca5; font-weight: 900; }
.check-table th:nth-child(1) { width: 15%; }
.check-table th:nth-child(2) { width: 45%; }
.check-table th:nth-child(3) { width: 18%; }
.check-table th:nth-child(4) { width: 22%; }
.check-table tr.abnormal td { background: #fff7f7; }
.check-table td:first-child { background: #f4f8fd; color: #355679; font-weight: 850; }
.check-table input,
.entry-table input { height: 31px; border-radius: 6px; }
.check-table input.required { border-color: #ef4444; background: #fff1f2; }
.result-toggle { display: grid; grid-template-columns: 1fr 1fr; gap: 4px; }
.result-toggle button { min-height: 30px; border: 1px solid #cfe0ed; border-radius: 7px; background: #fff; color: #63758b; font: inherit; font-size: 11px; font-weight: 850; cursor: pointer; }
.result-toggle button.active { border-color: #34a77d; background: #eaf9f3; color: #087f5b; }
.result-toggle .abnormal-option.active { border-color: #f87171; background: #fff0f0; color: #b42318; }
.suggestions-field { display: block; margin-top: 10px; }
.structured-sections { display: grid; gap: 16px; }
.structured-sections article h3 { margin: 0 0 7px; font-size: 14px; }
.entry-table th:first-child,
.entry-table td:first-child { width: 54px; text-align: center; background: #f4f8fd; }

.generated-preview { margin: 0 16px 14px; border: 1px solid #a9dfca; border-left: 4px solid #16a477; border-radius: 10px; padding: 10px; background: #f3fbf7; box-shadow: 0 6px 18px rgba(16, 137, 98, .08); }
.generated-preview > header { margin-bottom: 8px; }
.generated-preview > header > div { min-width: 0; }
.generated-preview > header strong,
.generated-preview > header span { display: block; }
.generated-preview-kicker { display: flex !important; align-items: center; gap: 5px; margin-bottom: 3px; color: #087f5b; font-size: 11px; font-weight: 900; }
.workbook-link { min-height: 32px; display: inline-flex; align-items: center; justify-content: center; gap: 6px; border: 1px solid #b9d2f2; border-radius: 8px; padding: 0 10px; background: #fff; color: #175ab9; font-size: 12px; font-weight: 900; text-decoration: none; white-space: nowrap; }
.workbook-link:hover { border-color: #4f8ff2; background: #eaf3ff; }
.workbook-link.compact { min-height: 29px; margin: 0 10px 9px; }
.generated-preview button { position: relative; width: 170px; height: 120px; display: grid; place-items: center; overflow: hidden; border: 1px solid #ccdbed; border-radius: 8px; background: #fff; cursor: zoom-in; }
.generated-preview img { width: 100%; height: 100%; object-fit: contain; }
.generated-preview button svg { position: absolute; right: 7px; bottom: 7px; border-radius: 7px; padding: 5px; box-sizing: content-box; background: rgba(17, 71, 151, .84); color: #fff; }
.workspace-actions { position: sticky; bottom: 0; z-index: 5; display: flex; align-items: center; justify-content: space-between; gap: 12px; border-top: 1px solid #dce7f4; padding: 10px 16px; background: rgba(255, 255, 255, .96); box-shadow: 0 -8px 22px rgba(32, 82, 145, .08); }
.workspace-actions > span { color: #71839b; font-size: 12px; font-weight: 850; }
.workspace-actions > span.dirty { color: #b45309; }
.workspace-actions > div { display: flex; gap: 8px; }

.image-viewer { position: fixed; inset: 0; z-index: 1100; display: grid; grid-template-rows: auto minmax(0, 1fr); padding: 18px; background: rgba(7, 22, 45, .82); backdrop-filter: blur(8px); }
.image-viewer header { display: flex; align-items: center; justify-content: space-between; color: #fff; padding: 0 0 10px; }
.image-viewer header button { display: grid; width: 40px; height: 40px; place-items: center; border: 1px solid rgba(255,255,255,.35); border-radius: 50%; background: rgba(255,255,255,.12); color: #fff; cursor: pointer; }
.image-viewer > div { overflow: auto; text-align: center; }
.image-viewer img { max-width: 100%; height: auto; border-radius: 8px; background: #fff; box-shadow: 0 20px 80px rgba(0,0,0,.35); }

.template-editor-overlay {
  position: fixed;
  inset: 0;
  z-index: 1120;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgb(7 25 52 / 58%);
  backdrop-filter: blur(5px);
}
.template-editor-dialog {
  width: min(1120px, calc(100vw - 48px));
  max-height: min(820px, calc(100vh - 48px));
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  overflow: hidden;
  border: 1px solid #c9d9ec;
  border-radius: 12px;
  background: #fff;
  box-shadow: 0 24px 70px rgb(8 35 75 / 26%);
}
.template-editor-dialog > header,
.template-editor-dialog > footer { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 14px 18px; }
.template-editor-dialog > header { border-bottom: 1px solid #dce7f3; background: #f8fbff; }
.template-editor-dialog > header span { color: #5e7189; font-size: 12px; font-weight: 800; }
.template-editor-dialog > header h3 { margin: 2px 0 0; color: #142f54; font-size: 18px; }
.template-editor-close,
.template-row-delete { display: grid; place-items: center; border: 1px solid #d5e0ec; background: #fff; color: #52677f; cursor: pointer; }
.template-editor-close { width: 36px; height: 36px; border-radius: 50%; }
.template-editor-body { min-height: 0; overflow: auto; padding: 14px 18px 18px; background: #f5f8fc; }
.template-editor-loading { min-height: 260px; display: flex; align-items: center; justify-content: center; gap: 9px; color: #4f6680; font-size: 13px; font-weight: 850; }
.template-editor-table-head,
.template-editor-row { display: grid; grid-template-columns: 54px minmax(170px, .7fr) minmax(360px, 1.6fr) 48px; align-items: center; gap: 8px; }
.template-editor-table-head { position: sticky; top: -14px; z-index: 2; padding: 10px 8px; border-bottom: 1px solid #cbd9e8; background: #eaf2fb; color: #315f96; font-size: 12px; font-weight: 900; }
.template-editor-rows { display: grid; gap: 7px; margin-top: 8px; }
.template-editor-row { border: 1px solid #d8e3ef; border-radius: 8px; padding: 7px 8px; background: #fff; }
.template-editor-row > b { color: #6b7f96; font-size: 12px; text-align: center; }
.template-editor-row input,
.template-editor-row textarea { width: 100%; border: 1px solid #cbd8e7; border-radius: 7px; padding: 7px 9px; background: #fff; color: #17304f; font: inherit; font-size: 13px; resize: vertical; }
.template-editor-row input { height: 36px; }
.template-editor-row textarea { min-height: 48px; max-height: 120px; }
.template-editor-row input:focus,
.template-editor-row textarea:focus { border-color: #3b82f6; outline: 3px solid rgb(59 130 246 / 12%); }
.template-row-delete { width: 34px; height: 34px; border-radius: 8px; color: #b42318; }
.template-row-delete:hover:not(:disabled) { border-color: #f2b8b5; background: #fff2f1; }
.template-add-row { min-height: 36px; display: inline-flex; align-items: center; gap: 6px; margin-top: 10px; border: 1px dashed #7fb0ee; border-radius: 8px; padding: 0 13px; background: #f3f8ff; color: #1c64bd; font-weight: 850; cursor: pointer; }
.template-editor-dialog > footer { border-top: 1px solid #dce7f3; background: #fff; }
.template-editor-dialog > footer > div { display: flex; gap: 8px; }
.template-reset-button { min-height: 36px; border: 1px solid #f0c4bd; border-radius: 8px; padding: 0 12px; background: #fff8f6; color: #a53a2d; font-weight: 850; cursor: pointer; }
.template-editor-dialog button:disabled { cursor: not-allowed; opacity: .5; }

@media (max-width: 1180px) {
  .building-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .publish-panel { grid-template-columns: 1fr; }
  .publish-actions { grid-column: auto; }
  .result-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sheet-meta { grid-template-columns: 1fr 180px; }
  .signer-box { grid-column: 1 / -1; }
  .weather-automation { grid-template-columns: 1fr auto; }
  .weather-automation__times { grid-column: 1 / -1; grid-row: 2; }
}

@media (max-width: 820px) {
  .guard-page { width: min(100% - 16px, 1680px); margin-top: 10px; }
  .landing-view,
  .admin-view,
  .building-view { padding: 12px; }
  .building-grid,
  .admin-layout,
  .fill-layout,
  .result-grid,
  .weather-fields,
  .sheet-meta { grid-template-columns: 1fr; }
  .weather-automation { grid-template-columns: 1fr; }
  .weather-automation__times { grid-column: auto; grid-row: auto; grid-template-columns: 1fr; }
  .weather-automation__action { align-items: stretch; flex-direction: column; }
  .weather-automation__buttons { display: grid; grid-template-columns: 1fr; }
  .weather-refresh-button,
  .weather-pause-button { width: 100%; }
  .task-list { max-height: 240px; }
  .choice-grid.sheets { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .check-table { min-width: 980px; }
  .sheet-table-shell,
  .structured-sections { overflow-x: auto; padding: 0 10px 12px; }
  .workspace-actions { align-items: flex-start; flex-direction: column; }
  .workspace-actions > div { width: 100%; }
  .workspace-actions button { flex: 1; }
  .scope-file-panel { grid-template-columns: auto minmax(0, 1fr); margin-inline: 10px; padding: 14px; }
  .scope-file-actions { grid-column: 1 / -1; width: 100%; }
  .scope-file-actions > * { flex: 1; }
  .scope-file-preview-card { margin-inline: 10px; }
  .scope-file-preview-card > header { align-items: flex-start; flex-direction: column; }
  .scope-file-preview-card > header button { width: 100%; }
  .template-editor-overlay { padding: 8px; }
  .template-editor-dialog { width: calc(100vw - 16px); max-height: calc(100vh - 16px); }
  .image-viewer { padding: 10px; }
  .image-viewer header button { width: 44px; height: 44px; }
  .template-editor-table-head { display: none; }
  .template-editor-row { grid-template-columns: 44px minmax(0, 1fr) 44px; }
  .template-editor-row textarea { grid-column: 2 / -1; }
  .template-editor-close,
  .template-row-delete { width: 44px; height: 44px; }
  .template-editor-dialog > footer { align-items: stretch; flex-direction: column; }
  .template-editor-dialog > footer > div { display: grid; grid-template-columns: 1fr 1fr; }
}
</style>
