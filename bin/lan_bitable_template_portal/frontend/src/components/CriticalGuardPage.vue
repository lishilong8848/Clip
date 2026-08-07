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
                    v-if="response.image_url"
                    type="button"
                    class="result-thumbnail"
                    @click="openImage(response.image_url, `${response.scope}楼 · ${response.sheet_type}`)"
                  >
                    <img :src="response.image_url" :alt="`${response.scope}楼${response.sheet_type}`" loading="lazy" />
                    <ZoomIn :size="20" />
                  </button>
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
                    <PenLine :size="15" /> 管理签名
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
                    <div>
                      <span>文件内容预览</span>
                      <strong>{{ effectiveSourceFile.file_name }}</strong>
                    </div>
                    <button
                      v-if="!sourceFilePreviewFailed"
                      type="button"
                      class="secondary-button"
                      @click="openImage(sourceFilePreviewUrl, `${activeScope}楼 · ${activeResponse.sheet_type}`)"
                    >
                      <ZoomIn :size="16" /> 放大查看
                    </button>
                  </header>
                  <button
                    v-if="!sourceFilePreviewFailed"
                    type="button"
                    class="scope-file-preview-image"
                    @click="openImage(sourceFilePreviewUrl, `${activeScope}楼 · ${activeResponse.sheet_type}`)"
                  >
                    <img
                      :src="sourceFilePreviewUrl"
                      :alt="`${activeScope}楼${activeResponse.sheet_type}文件预览`"
                      loading="lazy"
                      @error="sourceFilePreviewFailed = true"
                    />
                    <span>点击放大</span>
                  </button>
                  <div v-else class="scope-file-preview-error">
                    <span>文件已保存，预览暂时加载失败。</span>
                    <button type="button" class="secondary-button" @click="retrySourceFilePreview">重新加载</button>
                  </div>
                </section>
              </div>

              <section v-else-if="activeDefinition.kind === 'check'" class="sheet-table-shell">
                <div class="table-actions">
                  <span>异常 {{ abnormalCount }} 项</span>
                  <button type="button" :disabled="templateOutdated" @click="markAllNormal"><ClipboardCheck :size="16" /> 一键全正常</button>
                </div>
                <table class="check-table">
                  <thead><tr><th>检查项</th><th>检查内容</th><th>检查结果</th><th>备注</th></tr></thead>
                  <tbody>
                    <tr v-for="item in activeDefinition.items" :key="item.key" :class="{ abnormal: cells.checks[item.key]?.status === 'abnormal' }">
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
                  <div><strong>已生成图片</strong><span>{{ activeResponse.scope }}楼 · {{ activeResponse.sheet_type }}</span></div>
                  <a v-if="activeResponse.workbook_url" class="workbook-link" :href="activeResponse.workbook_url" download>
                    <Download :size="15" /> 下载原表
                  </a>
                </header>
                <button type="button" @click="openImage(activeResponse.image_url, `${activeResponse.scope}楼 · ${activeResponse.sheet_type}`)">
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
  ClipboardCheck,
  Download,
  FileSpreadsheet,
  Image as ImageIcon,
  PenLine,
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
import { navigate } from "../navigation";
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
const imageViewerUrl = ref("");
const imageViewerTitle = ref("");
const confirmOpen = ref(false);
const normalConfirmOpen = ref(false);
const deleteConfirmOpen = ref(false);
const pendingDeleteTask = ref<Dict | null>(null);
const deletingTaskId = ref("");
const signatureDrawerOpen = ref(false);
const sourceFileInput = ref<HTMLInputElement | null>(null);
const sourceFileDragging = ref(false);
const sourceFileUploading = ref(false);
const sourceFilePreviewFailed = ref(false);
const sourceFilePreviewRevision = ref(0);
const selectedSigners = ref<Dict[]>([]);
const pendingSwitch = ref<null | (() => void)>(null);
let editRevision = 0;
let bootstrapGeneration = 0;
let listGeneration = 0;
let detailGeneration = 0;
let publishOperationId = "";
let bootstrapController: AbortController | null = null;
let listController: AbortController | null = null;
let detailController: AbortController | null = null;
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
const abnormalCount = computed(() => Object.values(cells.value.checks || {}).filter((item: any) => item?.status === "abnormal").length);
const buildingPendingCount = computed(() => tasks.value.filter((item) => !item.complete).length);
const buildingCompletedCount = computed(() => tasks.value.filter((item) => item.complete).length);
const adminResponseTotal = computed(() => selectedTask.value?.responses?.length || tasks.value.reduce((total, item) => total + Number(item.response_count || 0), 0));
const adminSubmittedTotal = computed(() => selectedTask.value ? selectedTask.value.responses.filter((item: Dict) => item.status === "submitted").length : tasks.value.reduce((total, item) => total + Number(item.submitted_count || 0), 0));
const adminSheetHasImages = computed(() => adminSheetResponses(adminActiveSheet.value).some((item) => item.image_url));
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
  return `${base}${base.includes("?") ? "&" : "?"}retry=${sourceFilePreviewRevision.value}`;
});
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
  } catch (loadError: any) {
    if (controller.signal.aborted || generation !== bootstrapGeneration) return;
    error.value = loadError?.message || "重保管理读取失败。";
  } finally {
    if (generation === bootstrapGeneration) loading.value = false;
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
    await selectTask(tasks.value[0].task_id, viewMode.value === "admin");
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
  cells.value = clone(response?.cells || {});
  selectedSigners.value = clone(
    Array.isArray(response?.selected_signers)
      ? response.selected_signers
      : Array.isArray(response?.signatures)
        ? response.signatures
        : [],
  );
  signatureDrawerOpen.value = false;
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
    if (String(activeResponse.value?.response_id || "") === responseId) applyResponse(updated);
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
  requestSwitch(() => navigate(backTarget.value));
}

watch(
  () => String(effectiveSourceFile.value?.file_id || ""),
  () => {
    sourceFilePreviewFailed.value = false;
    sourceFilePreviewRevision.value = 0;
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
  void loadBootstrap();
});

onMounted(loadBootstrap);

onBeforeUnmount(() => {
  bootstrapGeneration += 1;
  listGeneration += 1;
  detailGeneration += 1;
  bootstrapController?.abort();
  listController?.abort();
  detailController?.abort();
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
  border: 1px solid #d7e5f3;
  border-radius: 10px;
  background: #fff;
}
.scope-file-preview-card > header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-height: 56px;
  padding: 9px 12px;
  border-bottom: 1px solid #e0eaf5;
  background: #f7faff;
}
.scope-file-preview-card > header div { min-width: 0; }
.scope-file-preview-card > header span,
.scope-file-preview-card > header strong { display: block; }
.scope-file-preview-card > header span { color: #607793; font-size: 12px; font-weight: 850; }
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
.scope-file-preview-image span {
  position: absolute;
  right: 18px;
  bottom: 16px;
  border-radius: 999px;
  padding: 5px 9px;
  background: rgb(17 53 96 / 82%);
  color: #fff;
  font-size: 11px;
  font-weight: 850;
}
.scope-file-preview-error { min-height: 150px; display: grid; place-items: center; align-content: center; gap: 10px; padding: 20px; color: #8a5b16; font-size: 13px; }

.sheet-table-shell,
.structured-sections { padding: 0 16px 16px; }
.table-actions { display: flex; align-items: center; justify-content: space-between; margin: 2px 0 8px; }
.table-actions span { color: #b45309; font-size: 12px; font-weight: 900; }
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

.generated-preview { margin: 0 16px 14px; border: 1px solid #d7e5f3; border-radius: 10px; padding: 10px; background: #f8fbff; }
.generated-preview > header { margin-bottom: 8px; }
.generated-preview > header > div { min-width: 0; }
.generated-preview > header strong,
.generated-preview > header span { display: block; }
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

@media (max-width: 1180px) {
  .building-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .publish-panel { grid-template-columns: 1fr; }
  .publish-actions { grid-column: auto; }
  .result-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sheet-meta { grid-template-columns: 1fr 180px; }
  .signer-box { grid-column: 1 / -1; }
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
}
</style>
