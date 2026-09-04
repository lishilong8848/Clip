<template>
  <main v-if="printMode" class="drill-print-page" :class="`sheet-${printSheet}`">
    <section v-if="printLoading" class="print-state" role="status">
      <Loader2 :size="24" class="spinning" /> 正在准备打印内容
    </section>
    <section v-else-if="printError" class="print-state error" role="alert">
      <AlertTriangle :size="24" /> {{ printError }}
    </section>
    <section v-else class="print-sheet-frame" :style="printFrameStyle" :aria-label="printSheet === 'record' ? '演练记录表' : '演练评估表'">
      <div class="print-sheet" :style="printSheetStyle">
        <SheetTable :model="printModel" />
      </div>
    </section>
  </main>

  <main v-else class="drill-page">
    <header class="drill-page__header">
      <VnetBackButton @click="leavePage" />
      <div class="drill-page__title">
        <span>演练管理</span>
        <h1>{{ pageTitle }}</h1>
      </div>
      <button
        v-if="viewMode !== 'landing'"
        type="button"
        class="icon-button"
        :disabled="busy"
        aria-label="刷新演练数据"
        title="刷新"
        @click="reloadCurrent"
      >
        <RefreshCw :size="18" :class="{ spinning: loading }" />
      </button>
    </header>

    <section v-if="loading && !bootstrap" class="page-state" role="status">
      <Loader2 :size="24" class="spinning" /> 正在读取演练数据
    </section>
    <section v-else-if="error && !bootstrap" class="page-state error" role="alert">
      <AlertTriangle :size="22" />
      <strong>{{ error }}</strong>
      <button type="button" class="secondary-button" @click="loadBootstrap">重新读取</button>
    </section>

    <template v-else>
      <MessageBanner v-if="error" tone="failed" title="操作未完成" :text="error" />
      <MessageBanner v-if="message" :tone="messageTone" :text="message" />

      <section v-if="viewMode === 'landing'" class="landing-view">
        <div class="section-heading">
          <div>
            <span>执行入口</span>
            <h2>选择楼栋</h2>
          </div>
          <b>{{ landingScopes.filter((item) => item.authorized).length }} 个可用入口</b>
        </div>
        <div class="building-grid">
          <button
            v-for="item in landingScopes"
            :key="item.value"
            type="button"
            class="building-card"
            :disabled="!item.authorized"
            @click="openBuilding(item.value)"
          >
            <span class="building-icon"><Building2 :size="25" /></span>
            <strong>{{ item.label }}</strong>
            <small>{{ item.authorized ? `${item.pending || 0} 项待填写` : "当前账号无权限" }}</small>
            <b>{{ item.authorized ? "进入" : "无权限" }} <ChevronRight :size="17" /></b>
          </button>
          <button v-if="isAdmin" type="button" class="building-card admin" @click="openAdmin">
            <span class="building-icon"><Settings2 :size="25" /></span>
            <strong>管理员入口</strong>
            <small>上传模板、校对布局并发布</small>
            <b>进入 <ChevronRight :size="17" /></b>
          </button>
        </div>
      </section>

      <section v-else-if="viewMode === 'admin'" class="admin-view">
        <div class="command-bar">
          <div class="summary-pills">
            <span>演练 {{ drills.length }}</span>
            <span>草稿 {{ drills.filter((item) => item.status === 'draft').length }}</span>
            <span>已发布 {{ drills.filter((item) => item.status === 'published').length }}</span>
          </div>
          <label class="admin-month-filter">
            <span>查看月份</span>
            <input v-model="selectedMonth" type="month" :disabled="busy" @change="changeAdminMonth" />
          </label>
          <button type="button" class="primary-button" @click="uploadOpen = !uploadOpen">
            <UploadCloud :size="17" /> 上传演练文件
          </button>
        </div>

        <form v-if="uploadOpen" class="upload-panel" @submit.prevent="uploadDrill">
          <label>
            <span>年份</span>
            <input v-model.trim="uploadForm.year" inputmode="numeric" maxlength="4" required pattern="\d{4}" />
          </label>
          <label>
            <span>月份</span>
            <input v-model="uploadForm.month" type="month" required />
          </label>
          <label class="wide">
            <span>演练名称</span>
            <input v-model.trim="uploadForm.name" maxlength="160" required placeholder="请输入演练名称" />
          </label>
          <fieldset class="drill-scope-picker wide" :disabled="busy">
            <legend>需要填写的楼栋</legend>
            <div>
              <label v-for="building in ['A', 'B', 'C', 'D', 'E']" :key="building">
                <input v-model="uploadForm.assigned_scopes" type="checkbox" :value="building" />
                <span>{{ building }}楼</span>
              </label>
            </div>
            <small>{{ uploadForm.assigned_scopes.length ? '未选中的楼栋无需填写本演练' : '请至少选择一个楼栋' }}</small>
          </fieldset>
          <label class="file-drop wide" :class="{ active: uploadDragActive }" @dragover.prevent="uploadDragActive = true" @dragleave.prevent="uploadDragActive = false" @drop.prevent="handleUploadDrop">
            <input ref="uploadInput" type="file" accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" @change="handleUploadFile" />
            <FileSpreadsheet :size="24" />
            <strong>{{ uploadFile?.name || "选择或拖入 Excel 文件" }}</strong>
            <small>仅支持 .xlsx，最大 64MiB</small>
          </label>
          <div class="form-actions wide">
            <button type="button" class="secondary-button" :disabled="busy" @click="resetUpload">取消</button>
            <button type="submit" class="primary-button" :disabled="busy || !uploadFile || !uploadForm.assigned_scopes.length">
              <Loader2 v-if="uploading" :size="16" class="spinning" />
              {{ uploading ? "正在上传" : "上传并识别" }}
            </button>
          </div>
        </form>

        <div class="admin-layout">
          <aside class="drill-list" aria-label="演练模板列表">
            <button
              v-for="item in drills"
              :key="item.drill_id"
              type="button"
              class="drill-list__item"
              :class="{ active: selectedDrillId === item.drill_id }"
              @click="selectAdminDrill(item.drill_id)"
            >
              <strong>{{ item.name || item.source?.name || "未命名演练" }}</strong>
              <span>{{ monthLabel(item.year, item.month) }}</span>
              <span>填写楼栋：{{ assignedScopeLabel(item) }}</span>
              <small :class="statusClass(item.status)">{{ statusLabel(item.status) }}</small>
            </button>
            <div v-if="!drills.length" class="empty-inline">当前月份暂无演练文件</div>
          </aside>

          <section v-if="selectedDrill" class="configuration-panel">
            <header class="configuration-panel__header">
              <div>
                <span>模板配置</span>
                <h2>{{ selectedDrill.name }}</h2>
                <small>{{ selectedDrill.source?.name }} · {{ formatBytes(selectedDrill.source?.size) }}</small>
                <small>填写楼栋：{{ assignedScopeLabel(selectedDrill) }}</small>
              </div>
              <span :class="['status-chip', statusClass(selectedDrill.status)]">{{ statusLabel(selectedDrill.status) }}</span>
            </header>

            <fieldset :disabled="configurationLocked || busy">
              <legend>工作表</legend>
              <div class="field-grid two">
                <label>
                  <span>演练记录表</span>
                  <select v-model="configDraft.record_sheet">
                    <option value="">请选择</option>
                    <option v-for="sheet in sheetNames" :key="sheet" :value="sheet">{{ sheet }}</option>
                  </select>
                </label>
                <label>
                  <span>演练评估表</span>
                  <select v-model="configDraft.assessment_sheet">
                    <option value="">请选择</option>
                    <option v-for="sheet in sheetNames" :key="sheet" :value="sheet">{{ sheet }}</option>
                  </select>
                </label>
              </div>
            </fieldset>

            <details open :class="{ locked: configurationLocked }">
              <summary>记录表字段映射</summary>
              <div class="mapping-grid">
                <label v-for="item in recordMappingFields" :key="item.path">
                  <span>{{ item.label }}</span>
                  <input :value="mappingValue(item.path)" :disabled="configurationLocked || busy" placeholder="例如 C5:E5" @input="setMappingValue(item.path, inputValue($event))" />
                </label>
              </div>
            </details>

            <details :class="{ locked: configurationLocked }">
              <summary>步骤表格映射</summary>
              <div class="mapping-grid compact">
                <label v-for="item in stepMappingFields" :key="item.path">
                  <span>{{ item.label }}</span>
                  <input :value="mappingValue(item.path)" :disabled="configurationLocked || busy" @input="setMappingValue(item.path, inputValue($event))" />
                </label>
              </div>
            </details>

            <details :class="{ locked: configurationLocked }">
              <summary>评估表字段映射</summary>
              <div class="mapping-grid">
                <label v-for="item in assessmentMappingFields" :key="item.path">
                  <span>{{ item.label }}</span>
                  <input :value="mappingValue(item.path)" :disabled="configurationLocked || busy" @input="setMappingValue(item.path, inputValue($event))" />
                </label>
              </div>
              <div v-if="assessmentScoreRows.length" class="step-table-wrap">
                <table class="step-table score-table">
                  <thead><tr><th>行</th><th>分值单元格</th><th>得分单元格</th><th>分值</th></tr></thead>
                  <tbody>
                    <tr v-for="item in assessmentScoreRows" :key="item.row">
                      <td>{{ item.row }}</td>
                      <td><input v-model.trim="item.value_cell" :disabled="configurationLocked || busy" aria-label="分值单元格" /></td>
                      <td><input v-model.trim="item.score_cell" :disabled="configurationLocked || busy" aria-label="得分单元格" /></td>
                      <td><input v-model.number="item.score" type="number" min="0" max="100" step="1" :disabled="configurationLocked || busy" aria-label="分值" /></td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </details>

            <details open :class="{ locked: configurationLocked }">
              <summary>步骤与签名人数</summary>
              <div class="step-table-wrap">
                <table class="step-table">
                  <thead><tr><th>行</th><th>演练位置</th><th>操作内容</th><th>预估时间</th><th>签名人数</th></tr></thead>
                  <tbody>
                    <tr v-for="step in configSteps" :key="step.row">
                      <td>{{ step.row }}</td>
                      <td>{{ step.location || "—" }}</td>
                      <td>{{ step.content || "—" }}</td>
                      <td>{{ step.duration_text || `${step.duration_minutes || 0}分钟` }}</td>
                      <td>
                        <input
                          v-model.number="step.signature_slots"
                          type="number"
                          min="1"
                          max="10"
                          :disabled="configurationLocked || busy"
                          aria-label="步骤签名人数"
                        />
                      </td>
                    </tr>
                    <tr v-if="!configSteps.length"><td colspan="5">未识别到步骤，请检查步骤区域映射。</td></tr>
                  </tbody>
                </table>
              </div>
            </details>

            <MessageBanner
              v-if="configurationLocked"
              tone="warning"
              text="已有楼栋保存执行数据，当前模板布局已锁定；如需修改请重新上传为新演练。"
            />

            <footer class="sticky-actions">
              <button type="button" class="danger-button" :disabled="busy" @click="requestDeleteSelected">
                <Trash2 :size="16" /> {{ selectedDrill.status === "draft" ? "删除" : "归档" }}
              </button>
              <span class="action-spacer"></span>
              <button type="button" class="secondary-button" :disabled="configurationLocked || busy" @click="saveConfiguration">
                {{ saving ? "保存中" : "保存配置" }}
              </button>
              <button type="button" class="primary-button" :disabled="configurationLocked || busy || selectedDrill.status === 'published'" @click="publishSelected">
                <Send :size="16" /> 发布至 {{ assignedScopeLabel(selectedDrill) }}
              </button>
            </footer>
          </section>
          <section v-else class="empty-panel">
            <FileSpreadsheet :size="36" />
            <strong>请选择演练文件</strong>
            <span>上传后可核对工作表、字段位置和每步签名人数。</span>
          </section>
        </div>
      </section>

      <section v-else class="building-view">
        <div class="building-toolbar">
          <div class="toolbar-title">
            <span>{{ activeScope }}楼</span>
            <h2>演练执行</h2>
          </div>
          <label>
            <span>月份</span>
            <input v-model="selectedMonth" type="month" :disabled="busy" @change="changeMonth" />
          </label>
          <label v-if="authorizedBuildingScopes.length > 1">
            <span>楼栋</span>
            <select :value="activeScope" :disabled="busy" @change="changeScope">
              <option v-for="item in authorizedBuildingScopes" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </label>
        </div>

        <div class="building-layout">
          <aside class="drill-list" aria-label="本月演练列表">
            <button
              v-for="item in buildingDrills"
              :key="item.drill_id"
              type="button"
              class="drill-list__item"
              :class="{ active: selectedDrillId === item.drill_id }"
              @click="selectBuildingDrill(item.drill_id)"
            >
              <strong>{{ item.name }}</strong>
              <span>{{ item.source?.name || monthLabel(item.year, item.month) }}</span>
              <small :class="statusClass(drillExecutionStatus(item))">{{ statusLabel(drillExecutionStatus(item)) }}</small>
            </button>
            <div v-if="!buildingDrills.length" class="empty-inline">当前月份暂无已发布演练</div>
          </aside>

          <section v-if="selectedDrill && execution" class="execution-panel">
            <header class="execution-header">
              <div>
                <span>{{ activeScope }}楼 · {{ monthLabel(selectedDrill.year, selectedDrill.month) }}</span>
                <h2>{{ selectedDrill.name }}</h2>
              </div>
              <span :class="['status-chip', statusClass(execution.status)]">{{ executionStatusText }}</span>
            </header>

            <nav class="sheet-tabs" aria-label="演练表格">
              <button type="button" :class="{ active: activeSheet === 'record' }" @click="switchSheet('record')">
                <ClipboardCheck :size="17" /> 演练记录表
              </button>
              <button type="button" :class="{ active: activeSheet === 'assessment' }" @click="switchSheet('assessment')">
                <BadgeCheck :size="17" /> 演练评估表
              </button>
              <button type="button" class="print-button" :disabled="!canUseGeneratedFile" :title="generatedDisabledReason" @click="printCurrent">
                <Printer :size="17" /> 打印当前表
              </button>
            </nav>

            <section v-if="activeSheet === 'record'" class="record-editor">
              <div class="field-grid four">
                <label>
                  <span>演练日期 <b>*</b></span>
                  <input v-model="execution.drill_date" type="date" :disabled="busy" @input="markDirty" />
                </label>
                <label>
                  <span>首步开始时间 <b>*</b></span>
                  <input v-model="execution.first_start_time" type="time" :disabled="busy" @input="markDirty" />
                </label>
                <label class="span-two">
                  <span>指挥人 <b>*</b></span>
                  <VnetSelect
                    input-id="drill-commander"
                    label="指挥人"
                    :model-value="personOptionLabel(commanderId)"
                    :options="commanderPersonOptionLabels"
                    :disabled="busy"
                    required
                    @update:model-value="selectCommanderByLabel"
                  />
                </label>
              </div>

              <label v-if="selectedDrill?.configuration?.mapping?.scenario" class="scenario-field">
                <span>模拟场景（包括故障点与故障现象）</span>
                <textarea
                  v-model="execution.simulation_scenario"
                  rows="3"
                  maxlength="5000"
                  :disabled="busy"
                  @input="markDirty"
                />
              </label>

              <section class="people-panel">
                <header>
                  <div><Users :size="18" /><strong>参演人员</strong><span>{{ participantIds.length }}/10</span></div>
                  <div class="people-filter-actions">
                    <input v-model.trim="peopleSearch" type="search" placeholder="搜索姓名、工号、楼栋或岗位" :disabled="busy" />
                    <button type="button" class="secondary-button compact" :disabled="busy" @click="peopleExpanded = !peopleExpanded">
                      {{ peopleExpanded ? `仅看${activeScope}楼` : "展开其他人员" }}
                    </button>
                  </div>
                </header>
                <div class="selected-people">
                  <span v-for="person in selectedParticipants" :key="personId(person)" :class="{ missing: !person.has_signature }">
                    {{ personName(person) }}
                    <small>{{ personId(person) === commanderId ? "指挥人" : person.has_signature ? "已签名" : "缺签名" }}</small>
                    <button v-if="personId(person) !== commanderId" type="button" :disabled="busy" :aria-label="`移除${personName(person)}`" @click="removeParticipant(person)">×</button>
                  </span>
                  <small v-if="!selectedParticipants.length">请先选择指挥人和参演人员</small>
                </div>
                <div class="people-options">
                  <label v-for="person in filteredPeople" :key="personId(person)" :class="{ selected: participantIds.includes(personId(person)) }">
                    <input
                      type="checkbox"
                      :checked="participantIds.includes(personId(person))"
                      :disabled="busy || personId(person) === commanderId || (!participantIds.includes(personId(person)) && participantIds.length >= 10)"
                      @change="toggleParticipant(person)"
                    />
                    <span><strong>{{ personName(person) }}</strong><small>{{ personMeta(person) }}</small></span>
                    <i :class="person.has_signature ? 'ready' : 'missing'">{{ person.has_signature ? "已签名" : "缺签名" }}</i>
                  </label>
                  <div v-if="!filteredPeople.length" class="empty-inline">没有匹配人员</div>
                </div>
              </section>

              <section class="steps-panel">
                <header><ListChecks :size="19" /><strong>步骤执行人</strong><span>{{ executionSteps.length }} 步</span></header>
                <article v-for="step in executionSteps" :key="step.row" class="execution-step">
                  <div class="step-number">{{ stepIndex(step) }}</div>
                  <div class="step-copy">
                    <span>{{ step.location || "未填写位置" }} · {{ step.duration_text || `${step.duration_minutes || 0}分钟` }}</span>
                    <p>{{ step.content || "未填写操作内容" }}</p>
                  </div>
                  <div class="signer-grid">
                    <label v-for="slot in Number(step.signature_slots || 1)" :key="`${step.row}-${slot}`">
                      <span>执行人 {{ slot }}</span>
                      <VnetSelect
                        :input-id="`drill-step-${step.row}-${slot}`"
                        :label="`步骤${stepIndex(step)}执行人${slot}`"
                        :model-value="personOptionLabel(stepSignerIds(step.row)[slot - 1] || '')"
                        :options="stepSignerOptionLabels(step.row, slot - 1)"
                        :disabled="busy"
                        @update:model-value="updateStepSigner(step, slot - 1, $event)"
                      />
                    </label>
                  </div>
                </article>
                <div v-if="!executionSteps.length" class="empty-inline">模板没有可执行步骤，请联系管理员检查配置。</div>
              </section>

              <MessageBanner
                v-if="missingSignaturePeople.length"
                tone="warning"
                title="存在缺失签名"
                :text="`${missingSignaturePeople.map(personName).join('、')} 尚未保存签名，可保存草稿，但生成前必须补齐。`"
              />
              <div class="signature-actions">
                <button
                  type="button"
                  class="secondary-button compact"
                  :disabled="peopleRefreshing"
                  @click="refreshSignaturePeople"
                >
                  <RefreshCw :size="15" :class="{ spinning: peopleRefreshing }" />
                  {{ peopleRefreshing ? "正在刷新" : "刷新签名" }}
                </button>
              </div>

              <SheetTable v-if="previewModel && !previewLoading" :model="previewModel" compact />
            </section>

            <section v-else class="assessment-view">
              <div v-if="previewLoading" class="inline-state"><Loader2 :size="20" class="spinning" /> 正在读取评估表</div>
              <MessageBanner v-else-if="previewError" tone="failed" :text="previewError" />
              <SheetTable v-else-if="previewModel" :model="previewModel" />
              <div v-else class="empty-panel compact">暂无评估表预览</div>
            </section>

            <MessageBanner v-if="conflict" tone="warning" title="检测到其他人已保存" text="当前页面内容已保留。请刷新演练数据后核对，再重新保存。" />
            <MessageBanner v-if="execution.last_error" tone="failed" title="生成或同步失败" :text="execution.last_error" />

            <footer class="sticky-actions execution-actions">
              <span class="save-indicator" :class="{ dirty, saved: !dirty && lastSavedAt }">
                {{ dirty ? "有未保存修改" : lastSavedAt ? `已保存 ${lastSavedAt}` : "尚未保存" }}
              </span>
              <button type="button" class="secondary-button" :disabled="busy || !dirty" @click="saveDraft()">
                <Save :size="16" /> {{ saving ? "保存中" : "保存草稿" }}
              </button>
              <button v-if="execution.status === 'sync_pending' || (execution.last_error && execution.generated)" type="button" class="secondary-button" :disabled="busy" @click="retrySync">
                <RefreshCw :size="16" /> 重试同步
              </button>
              <button type="button" class="secondary-button" :disabled="!canUseGeneratedFile" :title="generatedDisabledReason" @click="downloadGenerated">
                <Download :size="16" /> 下载文件
              </button>
              <button type="button" class="primary-button" :disabled="busy || !canGenerate" :title="generateDisabledReason" @click="generateWorkbook">
                <FileCheck2 :size="16" /> {{ generating ? "正在生成" : canUseGeneratedFile ? "重新生成" : "生成并归档" }}
              </button>
            </footer>
          </section>

          <section v-else class="empty-panel">
            <ClipboardCheck :size="36" />
            <strong>请选择一项演练</strong>
            <span>填写记录表后可生成完整 Excel，并自动同步至飞书。</span>
          </section>
        </div>
      </section>
    </template>

    <ConfirmDialog
      :open="Boolean(discardPrompt)"
      tone="warning"
      kicker="未保存修改"
      title="放弃当前修改？"
      :message="discardPrompt?.message || ''"
      :details="discardPrompt?.details || []"
      confirm-label="放弃修改并继续"
      cancel-label="继续编辑"
      confirm-class="danger"
      @resolve="resolveDiscardPrompt"
    />


    <ConfirmDialog
      :open="Boolean(deleteTarget)"
      tone="danger"
      :title="deleteTarget?.status === 'draft' ? '删除演练模板' : '归档演练模板'"
      :message="deleteTarget?.status === 'draft' ? '删除后本地源文件不可恢复。' : '已有执行数据的演练只会归档，不会删除楼栋记录。'"
      :details="deleteTarget ? [deleteTarget.name || deleteTarget.source?.name || '未命名演练'] : []"
      :confirm-label="deleteTarget?.status === 'draft' ? '确认删除' : '确认归档'"
      @resolve="resolveDelete"
    />
  </main>
</template>

<script setup lang="ts">
import {
  computed,
  defineComponent,
  h,
  nextTick,
  onBeforeUnmount,
  onMounted,
  reactive,
  ref,
  watch,
  type PropType,
} from "vue";
import {
  AlertTriangle,
  BadgeCheck,
  Building2,
  ChevronRight,
  ClipboardCheck,
  Download,
  FileCheck2,
  FileSpreadsheet,
  ListChecks,
  Loader2,
  Printer,
  RefreshCw,
  Save,
  Send,
  Settings2,
  Trash2,
  UploadCloud,
  Users,
} from "lucide-vue-next";
import { ApiError, requestJson, type Dict } from "../api/client";
import { navigate } from "../navigation";
import { refreshSignatureDirectory } from "../mopSignatureApi";
import ConfirmDialog from "./ConfirmDialog.vue";
import MessageBanner from "./MessageBanner.vue";
import VnetBackButton from "./VnetBackButton.vue";
import VnetSelect from "./VnetSelect.vue";

type SheetKind = "record" | "assessment";
type BannerTone = "info" | "success" | "warning" | "failed";
type MappingField = { path: string; label: string };
type SheetCell = Dict & { key: string; text: string; colspan: number; rowspan: number; hidden: boolean; rowIndex: number; columnIndex: number };
type DiscardPrompt = { message: string; details: string[]; action: () => void };

const props = withDefaults(defineProps<{
  scope?: string;
  scopeOptions: Array<{ value: string; label: string }>;
  isAdmin?: boolean;
  currentUser?: Dict;
  adminMode?: boolean;
  printMode?: boolean;
}>(), {
  scope: "",
  isAdmin: false,
  currentUser: () => ({}),
  adminMode: false,
  printMode: false,
});

const emit = defineEmits<{
  status: [text: string];
  "switch-scope": [scope: string];
}>();

const SheetTable = defineComponent({
  name: "DrillSheetTable",
  props: {
    model: { type: Object as PropType<Dict>, required: true },
    compact: { type: Boolean, default: false },
  },
  setup(tableProps) {
    return () => {
      const rows = normalizedSheetRows(tableProps.model).map((row, rowIndex) => h("tr", {
        key: `row-${rowIndex}`,
        style: rowHeightStyle(tableProps.model, rowIndex),
      }, row.filter((cell) => !cell.hidden).map((cell) => h("td", {
        key: cell.key,
        class: { "sheet-cell-has-image": arrayFrom(cell.template_images).length > 0 },
        colspan: cell.colspan,
        rowspan: cell.rowspan,
        style: cellStyle(cell, tableProps.model),
      }, sheetCellContent(cell)))));
      const columns = Array.from({ length: Number(tableProps.model.column_count || 0) }, (_, columnIndex) => h("col", {
        key: `column-${columnIndex}`,
        style: columnWidthStyle(tableProps.model, columnIndex),
      }));
      return h("div", { class: ["sheet-table-wrap", { compact: tableProps.compact }] }, [
        h("table", { class: "sheet-preview-table", style: sheetTableStyle(tableProps.model) }, [h("colgroup", columns), h("tbody", rows)]),
        tableProps.model.images_truncated
          ? h("p", { class: "sheet-image-warning", role: "status" }, "模板图片较多，当前预览仅显示前 20 张；生成的 Excel 不受影响。")
          : null,
      ]);
    };
  },
});

const currentDate = new Date();
const currentMonth = `${currentDate.getFullYear()}-${String(currentDate.getMonth() + 1).padStart(2, "0")}`;
const routeParams = new URLSearchParams(window.location.search);
const selectedMonth = ref(normalizeMonth(routeParams.get("month") || currentMonth));
const loadedMonth = ref(selectedMonth.value);
const bootstrap = ref<Dict | null>(null);
const drills = ref<Dict[]>([]);
const people = ref<Dict[]>([]);
const selectedDrillId = ref(String(routeParams.get("drill_id") || ""));
const execution = ref<Dict | null>(null);
const configDraft = ref<Dict>({});
const previewModel = ref<Dict | null>(null);
const printModel = ref<Dict>({});
const loading = ref(false);
const saving = ref(false);
const uploading = ref(false);
const generating = ref(false);
const previewLoading = ref(false);
const printLoading = ref(false);
const error = ref("");
const message = ref("");
const messageTone = ref<BannerTone>("info");
const previewError = ref("");
const printError = ref("");
const peopleRefreshing = ref(false);
const uploadOpen = ref(false);
const uploadDragActive = ref(false);
const uploadFile = ref<File | null>(null);
const uploadInput = ref<HTMLInputElement | null>(null);
const dirty = ref(false);
const conflict = ref(false);
const peopleSearch = ref("");
const peopleExpanded = ref(false);
const lastSavedAt = ref("");
const deleteTarget = ref<Dict | null>(null);
const discardPrompt = ref<DiscardPrompt | null>(null);
const peopleScopeLoaded = ref("");
const configBaseline = ref("{}");
const activeSheet = ref<SheetKind>(routeParams.get("sheet") === "assessment" ? "assessment" : "record");
const uploadForm = reactive({
  year: String(currentDate.getFullYear()),
  month: currentMonth,
  name: "",
  assigned_scopes: ["A", "B", "C", "D", "E"],
});
let statusPollTimer: number | null = null;
let statusPollKey = "";
let statusPollStartVersion = -1;
let statusPollObservedVersion = -1;
let disposed = false;
let printStyle: HTMLStyleElement | null = null;
let restoringHistory = false;
let stablePageUrl = `${window.location.pathname}${window.location.search}`;

const recordMappingFields: MappingField[] = [
  { path: "mapping.machine_room", label: "机房名称" },
  { path: "mapping.drill_date", label: "演练日期" },
  { path: "mapping.drill_name", label: "演练名称" },
  { path: "mapping.area", label: "涉及区域" },
  { path: "mapping.scenario", label: "模拟场景" },
  { path: "mapping.commander", label: "总指挥人" },
  { path: "mapping.participants", label: "参演人员" },
  { path: "mapping.predicted_total", label: "预估总用时" },
  { path: "mapping.actual_total", label: "演练总用时" },
  { path: "mapping.participant_signatures", label: "参演人签名区" },
  { path: "mapping.recorder_signature", label: "记录人签名区" },
];
const stepMappingFields: MappingField[] = [
  { path: "mapping.steps.start_row", label: "起始行" },
  { path: "mapping.steps.header_row", label: "表头行" },
  { path: "mapping.steps.end_row", label: "结束行" },
  { path: "mapping.steps.location_col", label: "位置列" },
  { path: "mapping.steps.content_col", label: "操作内容列" },
  { path: "mapping.steps.duration_col", label: "预估时间列" },
  { path: "mapping.steps.start_col", label: "开始时间列" },
  { path: "mapping.steps.end_col", label: "结束时间列" },
  { path: "mapping.steps.executor_col", label: "执行人列" },
  { path: "mapping.steps.result_col", label: "确认结果列" },
];
const assessmentMappingFields: MappingField[] = [
  { path: "mapping.assessment.drill_name", label: "演练名称" },
  { path: "mapping.assessment.drill_date", label: "演练日期" },
  { path: "mapping.assessment.participants", label: "参演人员" },
  { path: "mapping.assessment.start_time", label: "开始时间" },
  { path: "mapping.assessment.end_time", label: "结束时间" },
  { path: "mapping.assessment.total_score", label: "总分" },
];
const blankStepSignerOption = "留空（事后签名）";

const activeScope = computed(() => normalizeBuilding(props.scope || routeParams.get("scope") || ""));
const printSheet = computed<SheetKind>(() => routeParams.get("sheet") === "assessment" ? "assessment" : "record");
const viewMode = computed<"landing" | "admin" | "building">(() => {
  if (props.adminMode && props.isAdmin) return "admin";
  if (activeScope.value) return "building";
  return "landing";
});
const pageTitle = computed(() => viewMode.value === "admin" ? "模板与发布" : viewMode.value === "building" ? `${activeScope.value}楼演练` : "选择入口");
const adminDirty = computed(() => Boolean(selectedDrill.value && JSON.stringify(configDraft.value) !== configBaseline.value));
const hasUnsavedChanges = computed(() => dirty.value || adminDirty.value);
const busy = computed(() => loading.value || saving.value || uploading.value || generating.value || ["queued", "generating", "syncing"].includes(String(execution.value?.status || "")));
const landingScopes = computed(() => {
  const source = Array.isArray(bootstrap.value?.scopes) ? bootstrap.value?.scopes : [];
  if (source.length) {
    const byScope = new Map<string, Dict>();
    for (const raw of source) {
      const item = raw && typeof raw === "object" ? raw as Dict : { value: raw };
      const value = normalizeBuilding(item.value || item.scope);
      if (value) byScope.set(value, item);
    }
    return ["A", "B", "C", "D", "E"].map((value) => {
      const item = byScope.get(value) || {};
      return {
        ...item,
        value,
        label: String(item.label || `${value}楼`),
        authorized: props.isAdmin || byScope.has(value) && item.authorized !== false,
        pending: Number(item.pending || 0),
      };
    });
  }
  const authorized = new Set(props.scopeOptions.map((item) => normalizeBuilding(item.value)).filter(Boolean));
  return ["A", "B", "C", "D", "E"].map((value) => ({ value, label: `${value}楼`, authorized: props.isAdmin || authorized.has(value), pending: 0 }));
});
const authorizedBuildingScopes = computed(() => landingScopes.value.filter((item: Dict) => item.authorized));
const buildingDrills = computed(() => drills.value.filter((item) =>
  (item.status === "published" || item.execution || item.execution_status)
  && (item.assigned_scopes === undefined || Array.isArray(item.assigned_scopes) && item.assigned_scopes.includes(activeScope.value)),
));
const selectedDrill = computed(() => (viewMode.value === "building" ? buildingDrills.value : drills.value).find((item) => String(item.drill_id) === selectedDrillId.value) || null);
const sheetNames = computed(() => (Array.isArray(selectedDrill.value?.sheets) ? selectedDrill.value?.sheets : []).map((item: Dict | string) => String(typeof item === "string" ? item : item.name || "")).filter(Boolean));
const configSteps = computed<Dict[]>(() => Array.isArray(configDraft.value.steps) ? configDraft.value.steps : []);
const assessmentScoreRows = computed<Dict[]>(() => Array.isArray(configDraft.value?.mapping?.assessment?.score_rows) ? configDraft.value.mapping.assessment.score_rows : []);
const executionSteps = computed<Dict[]>(() => {
  const steps = execution.value?.steps || selectedDrill.value?.configuration?.steps || selectedDrill.value?.steps || [];
  return Array.isArray(steps) ? steps : [];
});
const configurationLocked = computed(() => Boolean(selectedDrill.value?.configuration_locked || selectedDrill.value?.has_executions));
const commanderId = computed(() => String(execution.value?.commander?.record_id || ""));
const participantIds = computed<string[]>(() => (Array.isArray(execution.value?.participants) ? execution.value?.participants : []).map((person: Dict) => personId(person)).filter(Boolean));
const selectedParticipants = computed<Dict[]>(() => participantIds.value.map((id) => personById(id) || execution.value?.participants?.find((person: Dict) => personId(person) === id) || { record_id: id, name: id }));
const currentBuildingPeople = computed(() => people.value.filter((person) => personBelongsToScope(person, activeScope.value)));
const searchedPeople = computed(() => {
  const query = peopleSearch.value.toLocaleLowerCase("zh-CN");
  return query
    ? people.value.filter((person) => `${personName(person)} ${personMeta(person)} ${personId(person)}`.toLocaleLowerCase("zh-CN").includes(query))
    : [];
});
const commanderPeople = computed(() => uniquePeople([
  ...currentBuildingPeople.value,
  ...people.value,
  ...(personById(commanderId.value) ? [personById(commanderId.value) as Dict] : []),
]));
const commanderPersonOptionLabels = computed(() => commanderPeople.value.map(personOptionLabelFromPerson));
const filteredPeople = computed(() => {
  const source = peopleSearch.value
    ? searchedPeople.value
    : peopleExpanded.value ? people.value : currentBuildingPeople.value;
  return source;
});
const missingSignaturePeople = computed(() => selectedParticipants.value.filter((person) => !person.has_signature));
const executionStatusText = computed(() => statusLabel(execution.value?.status || "draft"));
const generationCurrent = computed(() => execution.value?.generation_rule_current !== false && Number(execution.value?.generated_version || 0) > 0 && Number(execution.value?.generated_version || 0) === Number(execution.value?.execution_version || execution.value?.version || 0));
const canUseGeneratedFile = computed(() => Boolean(execution.value?.generated && generationCurrent.value && !dirty.value));
const generatedDisabledReason = computed(() => canUseGeneratedFile.value ? "" : dirty.value || !generationCurrent.value ? "内容已修改，请重新生成后使用。" : "尚未生成演练文件。" );
const generateValidationErrors = computed(() => validateExecution());
const canGenerate = computed(() => Boolean(execution.value && executionSteps.value.length && !generateValidationErrors.value.length));
const generateDisabledReason = computed(() => generateValidationErrors.value[0] || "");
const printFrameStyle = computed<Dict>(() => {
  const portrait = String(printModel.value.orientation || (printSheet.value === "record" ? "portrait" : "landscape")) !== "landscape";
  const margins = printPageMargins();
  return {
    width: `${Math.max(1, (portrait ? 793.7 : 1122.5) - margins.left - margins.right)}px`,
    height: `${Math.max(1, (portrait ? 1122.5 : 793.7) - margins.top - margins.bottom)}px`,
  };
});
const printSheetStyle = computed<Dict>(() => {
  const availableWidth = Number.parseFloat(String(printFrameStyle.value.width || 1));
  const availableHeight = Number.parseFloat(String(printFrameStyle.value.height || 1));
  const sheetWidth = Math.max(1, Number(printModel.value.sheet_width_px || 1));
  const sheetHeight = Math.max(1, Number(printModel.value.sheet_height_px || 1));
  const configuredScale = Number(printModel.value.page_scale || 0) / 100;
  const fitScale = Math.min(availableWidth / sheetWidth, availableHeight / sheetHeight);
  const scale = configuredScale > 0 ? Math.min(configuredScale, fitScale) : fitScale;
  return {
    width: `${sheetWidth}px`,
    height: `${sheetHeight}px`,
    transform: `scale(${Math.max(0.05, scale)})`,
  };
});

function normalizeBuilding(value: unknown): string {
  const text = String(value || "").trim().toUpperCase();
  return /^[ABCDE]$/.test(text) ? text : "";
}

function normalizeMonth(value: string): string {
  const match = String(value || "").match(/(20\d{2})[-年/]?(0?[1-9]|1[0-2])/);
  return match ? `${match[1]}-${String(match[2]).padStart(2, "0")}` : currentMonth;
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? {})) as T;
}

function inputValue(event: Event): string {
  return (event.target as HTMLInputElement).value;
}

function setNotice(text: string, tone: BannerTone = "info"): void {
  message.value = text;
  messageTone.value = tone;
  error.value = "";
  emit("status", text);
}

function setFailure(value: unknown, fallback: string): void {
  error.value = value instanceof Error ? value.message : fallback;
  message.value = "";
  emit("status", error.value);
}

function apiResultDrill(data: Dict): Dict | null {
  return data.drill || data.definition || data.item || (data.drill_id ? data : null);
}

function mergeDrill(next: Dict | null): void {
  if (!next?.drill_id) return;
  const index = drills.value.findIndex((item) => String(item.drill_id) === String(next.drill_id));
  if (index >= 0) drills.value.splice(index, 1, next);
  else drills.value.unshift(next);
  selectedDrillId.value = String(next.drill_id);
  resetConfigDraft(next.configuration || {});
}

function resetConfigDraft(value: Dict): void {
  configDraft.value = deepClone(value || {});
  configBaseline.value = JSON.stringify(configDraft.value);
}

function clearUnsavedState(): void {
  dirty.value = false;
  conflict.value = false;
  configBaseline.value = JSON.stringify(configDraft.value);
}

function requestDiscardChanges(
  message: string,
  action: () => void,
  details: string[] = [],
): void {
  if (!hasUnsavedChanges.value) {
    action();
    return;
  }
  if (discardPrompt.value) return;
  discardPrompt.value = { message, details, action };
}

function resolveDiscardPrompt(confirmed: boolean): void {
  const pending = discardPrompt.value;
  discardPrompt.value = null;
  if (!confirmed || !pending) return;
  clearUnsavedState();
  pending.action();
}

async function loadBootstrap(): Promise<void> {
  if (loading.value || disposed) return;
  loading.value = true;
  error.value = "";
  try {
    const query = new URLSearchParams({ month: selectedMonth.value });
    if (activeScope.value) query.set("scope", activeScope.value);
    const data = await requestJson(`/api/drills/bootstrap?${query.toString()}`, { cache: "no-store" });
    if (disposed) return;
    loadedMonth.value = selectedMonth.value;
    stablePageUrl = `${window.location.pathname}${window.location.search}`;
    bootstrap.value = data;
    drills.value = arrayFrom(data.drills || data.items);
    if (Array.isArray(data.people)) {
      people.value = uniquePeople(data.people);
      if (activeScope.value) peopleScopeLoaded.value = activeScope.value;
    }
    if (selectedDrillId.value && !selectedDrill.value) selectedDrillId.value = "";
    if (viewMode.value === "admin" && !selectedDrillId.value && drills.value.length) selectedDrillId.value = String(drills.value[0].drill_id || "");
    if (viewMode.value === "admin" && selectedDrill.value) resetConfigDraft(selectedDrill.value.configuration || {});
    if (viewMode.value === "building") {
      await Promise.all([loadPeople(), refreshList()]);
      if (disposed) return;
      if (selectedDrillId.value && !buildingDrills.value.some((item) => String(item.drill_id) === selectedDrillId.value)) selectedDrillId.value = "";
      if (!selectedDrillId.value && buildingDrills.value.length) selectedDrillId.value = String(buildingDrills.value[0].drill_id || "");
      if (selectedDrillId.value) await loadExecution();
    }
  } catch (caught) {
    if (!disposed) setFailure(caught, "演练数据读取失败");
  } finally {
    loading.value = false;
  }
}

async function refreshList(): Promise<void> {
  const query = new URLSearchParams({ month: selectedMonth.value });
  if (activeScope.value) query.set("scope", activeScope.value);
  const data = await requestJson(`/api/drills?${query.toString()}`, { cache: "no-store" });
  if (disposed) return;
  drills.value = arrayFrom(data.items || data.drills);
}

function reloadCurrent(): void {
  if (busy.value) return;
  requestDiscardChanges(
    "刷新后将恢复服务器中最后一次保存的内容，当前未保存修改不会保留。",
    () => void reloadCurrentConfirmed(),
    [selectedDrill.value?.name ? `演练：${selectedDrill.value.name}` : "当前演练"],
  );
}

async function reloadCurrentConfirmed(): Promise<void> {
  loading.value = true;
  error.value = "";
  try {
    await refreshList();
    if (viewMode.value === "admin" && selectedDrill.value) resetConfigDraft(selectedDrill.value.configuration || {});
    if (viewMode.value === "building") await loadPeople({ force: true, refresh: true });
    if (viewMode.value === "building" && selectedDrillId.value) await loadExecution();
    setNotice("演练数据已刷新", "success");
  } catch (caught) {
    setFailure(caught, "刷新失败");
  } finally {
    loading.value = false;
  }
}

async function loadPeople(options: { force?: boolean; refresh?: boolean } = {}): Promise<boolean> {
  const scope = activeScope.value;
  if (!scope) return false;
  if (!options.force && peopleScopeLoaded.value === scope) return true;
  try {
    const query = new URLSearchParams({ scope, month: selectedMonth.value });
    if (options.refresh) query.set("refresh_people", "1");
    const data = await requestJson(`/api/drills/bootstrap?${query.toString()}`, { cache: "no-store" });
    if (!Array.isArray(data.people)) throw new Error("人员目录响应不完整");
    people.value = uniquePeople(data.people);
    peopleScopeLoaded.value = scope;
    return true;
  } catch {
    if (options.force) throw new Error("人员签名状态刷新失败，请稍后重试。");
    return false;
  }
}

async function refreshSignaturePeople(): Promise<void> {
  if (peopleRefreshing.value || !activeScope.value) return;
  peopleRefreshing.value = true;
  try {
    const scopeAtStart = activeScope.value;
    const data = await refreshSignatureDirectory();
    if (disposed || activeScope.value !== scopeAtStart) return;
    people.value = uniquePeople((data.people || []).map((p: Dict) => ({
      ...p, source_record_id: p.record_id, record_id: p.source === "external" ? "external:" + p.record_id : p.record_id,
    })));
    if (Object.values(data.sources || {}).some((item: any) => !item.ok)) {
      setNotice("部分人员表刷新失败，已保留上次数据，请稍后重试。", "warning");
      return;
    }
    const pending = missingSignaturePeople.value.length;
    setNotice(
      pending ? "签名状态已刷新，仍有人员未完成签名。" : "签名状态已刷新，可生成演练文件。",
      pending ? "warning" : "success",
    );
  } catch (caught) {
    setFailure(caught, "签名状态刷新失败");
  } finally {
    peopleRefreshing.value = false;
  }
}

async function loadExecution(options: { silent?: boolean } = {}): Promise<void> {
  if (!selectedDrillId.value || !activeScope.value) return;
  if (!options.silent) {
    loading.value = true;
    error.value = "";
  }
  try {
    const query = new URLSearchParams({ scope: activeScope.value, create: "1" });
    const data = await requestJson(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/execution?${query.toString()}`, { cache: "no-store" });
    if (disposed) return;
    const remoteDrill = apiResultDrill(data);
    if (remoteDrill) mergeDrill(remoteDrill);
    const incoming = normalizeExecution(data.execution || data.data || {}, selectedDrill.value, activeScope.value);
    statusPollObservedVersion = Number(incoming.version || 0);
    const preserveDraft = Boolean(options.silent && dirty.value && execution.value);
    if (preserveDraft) {
      Object.assign(execution.value as Dict, {
        status: incoming.status,
        generated_version: incoming.generated_version,
        generated: incoming.generated,
        sync: incoming.sync,
        last_error: incoming.last_error,
      });
    } else {
      execution.value = incoming;
      dirty.value = false;
      conflict.value = false;
      ensureExecutionSelectionShape();
    }
    scheduleStatusPoll();
    if (!preserveDraft) await loadPreview(activeSheet.value, { silent: true });
  } catch (caught) {
    if (!disposed && !options.silent) setFailure(caught, "演练执行记录读取失败");
  } finally {
    if (!options.silent) loading.value = false;
  }
}

function normalizeExecution(value: Dict, drill: Dict | null, scope: string): Dict {
  const today = new Date().toISOString().slice(0, 10);
  const next = deepClone(value || {});
  next.drill_id ||= drill?.drill_id || selectedDrillId.value;
  next.scope ||= scope;
  next.version = Number(next.version || 0);
  next.execution_version = Number(next.execution_version || next.version || 0);
  next.generated_version = Number(next.generated_version || 0);
  next.status ||= "draft";
  next.drill_date ||= today;
  next.first_start_time ||= "09:00";
  next.simulation_scenario = String(next.simulation_scenario ?? drill?.configuration?.scenario_default_text ?? "");
  next.commander = next.commander && typeof next.commander === "object" ? next.commander : {};
  next.participants = Array.isArray(next.participants) ? next.participants : [];
  next.step_signers = next.step_signers && typeof next.step_signers === "object" ? next.step_signers : {};
  next.steps = Array.isArray(next.steps) ? next.steps : deepClone(drill?.configuration?.steps || drill?.steps || []);
  return next;
}

function ensureExecutionSelectionShape(): void {
  if (!execution.value) return;
  const commander = commanderId.value ? personById(commanderId.value) || execution.value.commander : null;
  const participantMap = new Map<string, Dict>();
  if (commander) participantMap.set(personId(commander), compactPerson(commander));
  for (const person of execution.value.participants || []) {
    const id = personId(person);
    if (id && participantMap.size < 10) participantMap.set(id, compactPerson(personById(id) || person));
  }
  execution.value.participants = [...participantMap.values()];
  for (const step of executionSteps.value) {
    const key = String(step.row);
    const count = Math.max(1, Number(step.signature_slots || 1));
    const ids = arrayFrom(execution.value.step_signers[key]).map(String).filter(Boolean).slice(0, count);
    while (ids.length < count) ids.push("");
    execution.value.step_signers[key] = ids;
  }
}

function personId(person: Dict | null | undefined): string {
  return String(person?.record_id || person?.id || "").trim();
}

function personName(person: Dict | null | undefined): string {
  return String(person?.name || person?.display_name || person?.user_name || "未命名").trim() || "未命名";
}

function personMeta(person: Dict): string {
  return [person.source === "external" || personId(person).startsWith("external:") ? "临时人员签名" : "正式人员", person.scope_text || person.scope || person.building || person.building_name, person.employee_no || person.staff_no || person.job_number, person.position || person.role_name || person.job_title]
    .map((item) => String(item || "").trim()).filter(Boolean).join(" · ") || "人员表";
}

function personBelongsToScope(person: Dict, scope: string): boolean {
  if (!scope) return false;
  const building = [person.building, person.scope_text, person.building_name, person.scope]
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean)
    .join(" ");
  return new Set(building.match(/[ABCDEH](?=楼|\b)/g) || []).has(scope);
}

function compactPerson(person: Dict): Dict {
  return { record_id: personId(person), name: personName(person) };
}

function uniquePeople(items: Dict[]): Dict[] {
  const map = new Map<string, Dict>();
  for (const person of items) {
    const id = personId(person);
    if (id && !map.has(id)) map.set(id, person);
  }
  return [...map.values()];
}

function personById(id: string): Dict | null {
  const person = people.value.find((person) => personId(person) === String(id || "")
    || (person.record_aliases || []).includes(id.startsWith("external:") ? id : "staff:" + id));
  return person ? { ...person, record_id: id } : null;
}

function personOptionLabelFromPerson(person: Dict): string {
  return `${personName(person)}｜${personMeta(person)}｜${personId(person)}`;
}

function personOptionLabel(id: string): string {
  const person = personById(id) || selectedParticipants.value.find((item) => personId(item) === id);
  return person ? personOptionLabelFromPerson(person) : "";
}

function personFromOptionLabel(label: string): Dict | null {
  return people.value.find((person) => personOptionLabelFromPerson(person) === label) || null;
}


function selectCommanderByLabel(label: string): void {
  if (!execution.value) return;
  const person = personFromOptionLabel(label);
  if (!person) return;
  const oldId = commanderId.value;
  const nextId = personId(person);
  execution.value.commander = compactPerson(person);
  execution.value.participants = [
    compactPerson(person),
    ...execution.value.participants.filter((item: Dict) => ![oldId, nextId].includes(personId(item))),
  ].slice(0, 10);
  for (const step of executionSteps.value) {
    const ids = stepSignerIds(step.row).map((id) => id === oldId ? "" : id);
    execution.value.step_signers[String(step.row)] = ids;
  }
  markDirty();
}

function toggleParticipant(person: Dict): void {
  if (!execution.value) return;
  const id = personId(person);
  if (!id || id === commanderId.value) return;
  const exists = participantIds.value.includes(id);
  if (!exists && participantIds.value.length >= 10) {
    setNotice("参演人员最多选择 10 人。", "warning");
    return;
  }
  if (exists) removeParticipant(person);
  else {
    execution.value.participants.push(compactPerson(person));
    markDirty();
  }
}

function removeParticipant(person: Dict): void {
  if (!execution.value) return;
  const id = personId(person);
  if (!id || id === commanderId.value) return;
  execution.value.participants = execution.value.participants.filter((item: Dict) => personId(item) !== id);
  for (const key of Object.keys(execution.value.step_signers || {})) {
    execution.value.step_signers[key] = arrayFrom(execution.value.step_signers[key]).map((item) => String(item) === id ? "" : item);
  }
  markDirty();
}

function stepSignerIds(row: unknown): string[] {
  if (!execution.value) return [];
  return arrayFrom(execution.value.step_signers?.[String(row)]).map((item) => String(item || ""));
}

function stepSignerOptionLabels(row: unknown, slot: number): string[] {
  const ids = stepSignerIds(row);
  const current = String(ids[slot] || "");
  const occupied = new Set(ids.filter((id, index) => index !== slot && id));
  return [blankStepSignerOption, ...selectedParticipants.value
    .filter((person) => personId(person) === current || !occupied.has(personId(person)))
    .map(personOptionLabelFromPerson)];
}

function updateStepSigner(step: Dict, slot: number, label: string): void {
  if (!execution.value) return;
  if (label === blankStepSignerOption) {
    const ids = stepSignerIds(step.row);
    ids[slot] = "";
    execution.value.step_signers[String(step.row)] = ids;
    markDirty();
    return;
  }
  const person = personFromOptionLabel(label);
  if (!person) return;
  const id = personId(person);
  const ids = stepSignerIds(step.row);
  if (ids.some((value, index) => index !== slot && value === id)) {
    setNotice("同一步骤不能重复选择同一人。", "warning");
    return;
  }
  ids[slot] = id;
  execution.value.step_signers[String(step.row)] = ids;
  markDirty();
}

function isEccStep(step: Dict): boolean {
  return /ECC/i.test(String(step.location || ""));
}

function stepIndex(step: Dict): number {
  return executionSteps.value.findIndex((item) => String(item.row) === String(step.row)) + 1;
}

function markDirty(): void {
  dirty.value = true;
  if (execution.value?.generated_version) execution.value.status = "ready";
}

function validateExecution(): string[] {
  if (!execution.value) return ["请先选择演练"];
  const errors: string[] = [];
  if (!execution.value.drill_date) errors.push("请选择演练日期");
  if (!execution.value.first_start_time) errors.push("请选择首步开始时间");
  if (!commanderId.value) errors.push("请选择指挥人");
  if (!participantIds.value.length) errors.push("请选择参演人员");
  if (participantIds.value.length > 10) errors.push("参演人员不能超过 10 人");
  for (const step of executionSteps.value) {
    const needed = Math.max(1, Number(step.signature_slots || 1));
    const ids = stepSignerIds(step.row).filter(Boolean);
    if (ids.length > needed || new Set(ids).size !== ids.length) errors.push(`步骤 ${stepIndex(step)} 最多选择 ${needed} 名不同的执行人`);
  }
  if (missingSignaturePeople.value.length) errors.push(`${missingSignaturePeople.value.map(personName).join("、")}尚未保存签名`);
  return errors;
}

function buildExecutionPayload(): Dict {
  if (!execution.value) return {};
  return {
    expected_version: Number(execution.value.version || 0),
    drill_date: execution.value.drill_date,
    first_start_time: execution.value.first_start_time,
    simulation_scenario: String(execution.value.simulation_scenario || ""),
    commander: compactPerson(execution.value.commander || {}),
    participants: selectedParticipants.value.map(compactPerson),
    step_signers: deepClone(execution.value.step_signers || {}),
  };
}

async function saveDraft(options: { quiet?: boolean } = {}): Promise<boolean> {
  if (!execution.value || saving.value) return false;
  saving.value = true;
  error.value = "";
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/execution?scope=${encodeURIComponent(activeScope.value)}`, {
      method: "PUT",
      body: JSON.stringify(buildExecutionPayload()),
    });
    execution.value = normalizeExecution(data.execution || data.data || data, selectedDrill.value, activeScope.value);
    ensureExecutionSelectionShape();
    dirty.value = false;
    conflict.value = false;
    lastSavedAt.value = new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" });
    if (!options.quiet) setNotice("演练草稿已保存", "success");
    await loadPreview(activeSheet.value, { silent: true });
    return true;
  } catch (caught) {
    if (caught instanceof ApiError && caught.status === 409) conflict.value = true;
    setFailure(caught, "演练草稿保存失败");
    return false;
  } finally {
    saving.value = false;
  }
}

async function generateWorkbook(): Promise<void> {
  if (!execution.value || generating.value) return;
  const validation = validateExecution();
  if (validation.length) {
    setNotice(validation[0], "warning");
    return;
  }
  if (dirty.value && !(await saveDraft({ quiet: true }))) return;
  generating.value = true;
  error.value = "";
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/generate?scope=${encodeURIComponent(activeScope.value)}`, {
      method: "POST",
      body: JSON.stringify({ expected_version: Number(execution.value.version || 0) }),
      timeoutMs: 120_000,
    });
    if (data.execution) execution.value = normalizeExecution(data.execution, selectedDrill.value, activeScope.value);
    setNotice("已提交生成任务，完成后会自动同步飞书。", "success");
    scheduleStatusPoll(true);
  } catch (caught) {
    setFailure(caught, "生成任务提交失败");
  } finally {
    generating.value = false;
  }
}

async function retrySync(): Promise<void> {
  if (!execution.value || busy.value) return;
  saving.value = true;
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/retry-sync?scope=${encodeURIComponent(activeScope.value)}`, { method: "POST", body: "{}" });
    if (data.execution) execution.value = normalizeExecution(data.execution, selectedDrill.value, activeScope.value);
    setNotice("已重新提交飞书同步", "success");
    scheduleStatusPoll(true);
  } catch (caught) {
    setFailure(caught, "重新同步失败");
  } finally {
    saving.value = false;
  }
}

function scheduleStatusPoll(immediate = false): void {
  clearStatusPoll();
  const key = `${selectedDrillId.value}:${activeScope.value}`;
  if (immediate) {
    statusPollKey = key;
    statusPollStartVersion = Number(execution.value?.version || 0);
  }
  const status = String(execution.value?.status || "");
  const active = statusPollKey === key;
  const hardFinal = ["synced", "completed", "error", "failed"].includes(status);
  const final = hardFinal
    || (status === "sync_pending" && Boolean(execution.value?.last_error));
  if (immediate && hardFinal) {
    clearStatusPoll(true);
    return;
  }
  if (final && (!active || statusPollObservedVersion > statusPollStartVersion)) {
    if (active) {
      statusPollKey = "";
      statusPollStartVersion = -1;
      statusPollObservedVersion = -1;
    }
    return;
  }
  if (!["queued", "generating", "syncing", "sync_pending"].includes(status) && !active) return;
  statusPollTimer = window.setTimeout(async () => {
    statusPollTimer = null;
    if (disposed || statusPollKey !== key) return;
    await loadExecution({ silent: true });
    if (!statusPollTimer && statusPollKey === key) scheduleStatusPoll();
  }, immediate ? 1200 : 5000);
}

function clearStatusPoll(resetKey = false): void {
  if (statusPollTimer !== null) {
    window.clearTimeout(statusPollTimer);
    statusPollTimer = null;
  }
  if (resetKey) {
    statusPollKey = "";
    statusPollStartVersion = -1;
    statusPollObservedVersion = -1;
  }
}


async function loadPreview(sheet: SheetKind, options: { silent?: boolean } = {}): Promise<void> {
  if (!selectedDrillId.value || !activeScope.value) return;
  if (!options.silent) previewLoading.value = true;
  previewError.value = "";
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/preview?scope=${encodeURIComponent(activeScope.value)}&sheet=${sheet}`, { cache: "no-store" });
    if (activeSheet.value === sheet) previewModel.value = data.model || data.preview || data;
  } catch (caught) {
    if (!options.silent) previewError.value = caught instanceof Error ? caught.message : "表格预览读取失败";
  } finally {
    if (!options.silent) previewLoading.value = false;
  }
}

function switchSheet(sheet: SheetKind): void {
  activeSheet.value = sheet;
  previewModel.value = null;
  void loadPreview(sheet);
}

function printCurrent(): void {
  if (!canUseGeneratedFile.value) return;
  const query = new URLSearchParams({ drill_id: selectedDrillId.value, scope: activeScope.value, sheet: activeSheet.value });
  window.open(`/drill-management/print?${query.toString()}`, "_blank", "noopener,noreferrer");
}

function downloadGenerated(): void {
  if (!canUseGeneratedFile.value) return;
  window.location.assign(`/api/drills/${encodeURIComponent(selectedDrillId.value)}/download?scope=${encodeURIComponent(activeScope.value)}`);
}

async function loadPrintModel(): Promise<void> {
  const drillId = String(routeParams.get("drill_id") || "").trim();
  const scope = normalizeBuilding(routeParams.get("scope") || "");
  if (!drillId || !scope) {
    printError.value = "打印参数不完整。";
    return;
  }
  printLoading.value = true;
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(drillId)}/print-model?scope=${encodeURIComponent(scope)}&sheet=${printSheet.value}`, { cache: "no-store", timeoutMs: 120_000 });
    printModel.value = data.model || data;
    installPrintStyle();
    printLoading.value = false;
    await nextTick();
    await waitForPrintImages();
    window.setTimeout(() => window.print(), 80);
  } catch (caught) {
    printError.value = caught instanceof Error ? caught.message : "打印内容读取失败";
  } finally {
    printLoading.value = false;
  }
}

function installPrintStyle(): void {
  printStyle?.remove();
  printStyle = document.createElement("style");
  const orientation = String(printModel.value.orientation || (printSheet.value === "assessment" ? "landscape" : "portrait"));
  const margins = printModel.value.page_margins || {};
  printStyle.textContent = `@page { size: A4 ${orientation}; margin: ${Number(margins.top ?? .2)}in ${Number(margins.right ?? .2)}in ${Number(margins.bottom ?? .2)}in ${Number(margins.left ?? .2)}in; }`;
  document.head.appendChild(printStyle);
}

function markPrintImageLoaded(): void {
  // Image completion is checked as a group before window.print().
}

async function waitForPrintImages(): Promise<void> {
  const images = [...document.querySelectorAll<HTMLImageElement>(".drill-print-page img")];
  await Promise.all(images.map((image) => image.complete ? Promise.resolve() : new Promise<void>((resolve) => {
    image.addEventListener("load", () => resolve(), { once: true });
    image.addEventListener("error", () => resolve(), { once: true });
  })));
  if (document.fonts?.ready) await document.fonts.ready;
}

function openBuilding(scope: string): void {
  const query = new URLSearchParams({ scope: normalizeBuilding(scope), month: selectedMonth.value });
  navigate(`/drill-management?${query.toString()}`);
}

function openAdmin(): void {
  navigate("/drill-management?mode=admin");
  stablePageUrl = `${window.location.pathname}${window.location.search}`;
}

function leavePage(): void {
  requestDiscardChanges("离开会放弃当前未保存修改。", () => {
    clearStatusPoll(true);
    if (viewMode.value === "landing") navigate("/");
    else navigate("/drill-management");
  });
}

function changeScope(event: Event): void {
  const select = event.target as HTMLSelectElement;
  const scope = normalizeBuilding((event.target as HTMLSelectElement).value);
  if (!scope) return;
  select.value = activeScope.value;
  requestDiscardChanges("切换楼栋会放弃当前未保存修改。", () => {
    clearStatusPoll(true);
    emit("switch-scope", scope);
  }, [`切换至 ${scope}楼`]);
}

function changeMonth(): void {
  const month = selectedMonth.value;
  selectedMonth.value = loadedMonth.value;
  requestDiscardChanges("切换月份会放弃当前未保存修改。", () => {
    selectedMonth.value = month;
    clearStatusPoll(true);
    const query = new URLSearchParams({ scope: activeScope.value, month });
    navigate(`/drill-management?${query.toString()}`);
    selectedDrillId.value = "";
    execution.value = null;
    void loadBootstrap();
  }, [`切换至 ${month}`]);
}

function changeAdminMonth(): void {
  const month = selectedMonth.value;
  selectedMonth.value = loadedMonth.value;
  requestDiscardChanges("切换月份会放弃当前模板配置修改。", () => {
    selectedMonth.value = month;
    navigate(`/drill-management?mode=admin&month=${encodeURIComponent(month)}`);
    selectedDrillId.value = "";
    resetConfigDraft({});
    void loadBootstrap();
  }, [`切换至 ${month}`]);
}

function selectAdminDrill(drillId: unknown): void {
  const nextId = String(drillId || "");
  if (nextId === selectedDrillId.value) return;
  requestDiscardChanges("切换演练会放弃当前模板配置修改。", () => {
    selectedDrillId.value = nextId;
    resetConfigDraft(selectedDrill.value?.configuration || {});
    error.value = "";
    message.value = "";
  });
}

function selectBuildingDrill(drillId: unknown): void {
  const nextId = String(drillId || "");
  if (nextId === selectedDrillId.value) return;
  requestDiscardChanges("切换演练会放弃当前未保存修改。", () => {
    void selectBuildingDrillConfirmed(nextId);
  });
}

async function selectBuildingDrillConfirmed(drillId: string): Promise<void> {
  clearStatusPoll(true);
  selectedDrillId.value = drillId;
  execution.value = null;
  previewModel.value = null;
  await loadExecution();
}

function handleUploadFile(event: Event): void {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  chooseUploadFile(file);
}

function handleUploadDrop(event: DragEvent): void {
  uploadDragActive.value = false;
  chooseUploadFile(event.dataTransfer?.files?.[0] || null);
}

function chooseUploadFile(file: File | null): void {
  if (!file) return;
  if (!/\.xlsx$/i.test(file.name)) {
    setNotice("仅支持 .xlsx 文件。", "warning");
    return;
  }
  if (file.size > 64 * 1024 * 1024) {
    setNotice("文件不能超过 64MiB。", "warning");
    return;
  }
  uploadFile.value = file;
  if (!uploadForm.name) uploadForm.name = file.name.replace(/\.xlsx$/i, "");
}

function resetUpload(): void {
  uploadOpen.value = false;
  uploadFile.value = null;
  uploadForm.name = "";
  uploadForm.assigned_scopes = ["A", "B", "C", "D", "E"];
  if (uploadInput.value) uploadInput.value.value = "";
}

async function uploadDrill(): Promise<void> {
  if (!uploadFile.value || uploading.value) return;
  if (!uploadForm.assigned_scopes.length) {
    setNotice("请至少选择一个需要填写演练的楼栋。", "warning");
    return;
  }
  uploading.value = true;
  error.value = "";
  try {
    const form = new FormData();
    form.append("year", uploadForm.year);
    form.append("month", String(Number(uploadForm.month.split("-")[1] || 0)));
    form.append("name", uploadForm.name);
    form.append("assigned_scopes", JSON.stringify(uploadForm.assigned_scopes));
    form.append("file", uploadFile.value);
    const data = await requestJson("/api/drills", { method: "POST", body: form, timeoutMs: 180_000 });
    selectedMonth.value = uploadForm.month;
    loadedMonth.value = selectedMonth.value;
    navigate(`/drill-management?mode=admin&month=${encodeURIComponent(selectedMonth.value)}`);
    stablePageUrl = `${window.location.pathname}${window.location.search}`;
    await refreshList();
    mergeDrill(apiResultDrill(data));
    resetUpload();
    setNotice("文件已上传并完成初步识别，请核对配置后发布。", "success");
  } catch (caught) {
    setFailure(caught, "演练文件上传失败");
  } finally {
    uploading.value = false;
  }
}

function assignedScopeLabel(drill: Dict): string {
  const scopes = Array.isArray(drill.assigned_scopes) ? drill.assigned_scopes : ["A", "B", "C", "D", "E"];
  return scopes.map((scope: string) => `${scope}楼`).join("、");
}

function mappingValue(path: string): string {
  const value = path.split(".").reduce<unknown>((current, key) => current && typeof current === "object" ? (current as Dict)[key] : "", configDraft.value);
  return String(value ?? "");
}

function setMappingValue(path: string, value: string): void {
  const keys = path.split(".");
  let target = configDraft.value;
  for (const key of keys.slice(0, -1)) {
    if (!target[key] || typeof target[key] !== "object") target[key] = {};
    target = target[key];
  }
  const finalKey = keys[keys.length - 1];
  target[finalKey] = ["header_row", "start_row", "end_row"].includes(finalKey) && value ? Number(value) : value;
}

async function saveConfiguration(): Promise<boolean> {
  if (!selectedDrill.value || saving.value || configurationLocked.value) return false;
  if (!configDraft.value.record_sheet || !configDraft.value.assessment_sheet) {
    setNotice("请选择记录表和评估表。", "warning");
    return false;
  }
  if (configDraft.value.record_sheet === configDraft.value.assessment_sheet) {
    setNotice("记录表和评估表不能选择同一个 Sheet。", "warning");
    return false;
  }
  saving.value = true;
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(String(selectedDrill.value.drill_id))}/configuration`, {
      method: "PUT",
      body: JSON.stringify({ expected_version: Number(selectedDrill.value.version || 0), configuration: configDraft.value }),
    });
    mergeDrill(apiResultDrill(data) || { ...selectedDrill.value, configuration: deepClone(configDraft.value), version: data.version || selectedDrill.value.version });
    setNotice("模板配置已保存", "success");
    return true;
  } catch (caught) {
    setFailure(caught, "模板配置保存失败");
    return false;
  } finally {
    saving.value = false;
  }
}

async function publishSelected(): Promise<void> {
  if (!selectedDrill.value || saving.value || configurationLocked.value) return;
  if (!(await saveConfiguration())) return;
  saving.value = true;
  try {
    const data = await requestJson(`/api/drills/${encodeURIComponent(String(selectedDrill.value.drill_id))}/publish`, {
      method: "POST",
      body: JSON.stringify({ expected_version: Number(selectedDrill.value.version || 0) }),
    });
    mergeDrill(apiResultDrill(data) || { ...selectedDrill.value, status: "published" });
    setNotice(`演练已发布至 ${assignedScopeLabel(selectedDrill.value || {})}`, "success");
  } catch (caught) {
    setFailure(caught, "演练发布失败");
  } finally {
    saving.value = false;
  }
}

function requestDeleteSelected(): void {
  deleteTarget.value = selectedDrill.value;
}

async function resolveDelete(confirmed: boolean): Promise<void> {
  const target = deleteTarget.value;
  deleteTarget.value = null;
  if (!confirmed || !target || saving.value) return;
  saving.value = true;
  try {
    const query = new URLSearchParams({ expected_version: String(Number(target.version || 0)) });
    await requestJson(`/api/drills/${encodeURIComponent(String(target.drill_id))}?${query.toString()}`, { method: "DELETE" });
    await refreshList();
    selectedDrillId.value = drills.value[0]?.drill_id || "";
    resetConfigDraft(selectedDrill.value?.configuration || {});
    setNotice(target.status === "draft" ? "演练模板已删除" : "演练已归档", "success");
  } catch (caught) {
    setFailure(caught, "删除或归档失败");
  } finally {
    saving.value = false;
  }
}

function drillExecutionStatus(item: Dict): string {
  return String(item.execution?.status || item.execution_status || item.scope_status || (item.status === "published" ? "draft" : item.status || "draft"));
}

function statusLabel(status: unknown): string {
  const labels: Record<string, string> = {
    draft: "待填写",
    ready: "需生成",
    published: "已发布",
    archived: "已归档",
    queued: "等待生成",
    generating: "生成中",
    syncing: "正在同步（可返回）",
    sync_pending: "等待同步",
    synced: "已同步",
    completed: "已完成",
    error: "失败",
    failed: "失败",
  };
  return labels[String(status || "")] || String(status || "未知状态");
}

function statusClass(status: unknown): string {
  const value = String(status || "");
  if (["synced", "completed", "published"].includes(value)) return "success";
  if (["error", "failed"].includes(value)) return "failed";
  if (["queued", "generating", "syncing", "sync_pending", "ready"].includes(value)) return "warning";
  return "neutral";
}

function monthLabel(year: unknown, month: unknown): string {
  const yearText = String(year || "").trim();
  const monthText = String(month || "").trim();
  if (/^20\d{2}-\d{2}$/.test(monthText)) return monthText.replace("-", "年") + "月";
  if (yearText && monthText) return `${yearText}年${monthText.replace(/月$/, "")}月`;
  return monthText || yearText || "未设置月份";
}

function formatBytes(value: unknown): string {
  const bytes = Number(value || 0);
  if (!bytes) return "大小未知";
  return bytes >= 1024 * 1024 ? `${(bytes / 1024 / 1024).toFixed(1)} MiB` : `${Math.ceil(bytes / 1024)} KiB`;
}

function arrayFrom(value: unknown): any[] {
  return Array.isArray(value) ? value : [];
}

function normalizedSheetRows(model: Dict): SheetCell[][] {
  const sourceRows = arrayFrom(model.rows);
  if (sourceRows.length) {
    const rows = sourceRows.map((rawRow, rowIndex) => {
      const cells = Array.isArray(rawRow) ? rawRow : arrayFrom(rawRow?.cells);
      return cells.map((rawCell: unknown, cellIndex: number) => normalizeSheetCell(rawCell, rowIndex, cellIndex));
    });
    applySheetMerges(rows, model);
    applySignatureCells(rows, model);
    applyTemplateImages(rows, model);
    return rows;
  }
  const cells = model.cells && typeof model.cells === "object" ? model.cells as Dict : {};
  const entries = Object.entries(cells);
  if (!entries.length) return [];
  const positions = entries.map(([address, value]) => ({ ...cellCoordinates(address), address, value }));
  const maxRow = Math.max(...positions.map((item) => item.row));
  const maxCol = Math.max(...positions.map((item) => item.col));
  return Array.from({ length: maxRow }, (_, rowIndex) => Array.from({ length: maxCol }, (_, colIndex) => {
    const item = positions.find((entry) => entry.row === rowIndex + 1 && entry.col === colIndex + 1);
    return normalizeSheetCell(item?.value || "", rowIndex, colIndex, item?.address);
  }));
}

function normalizeSheetCell(value: unknown, rowIndex: number, cellIndex: number, fallbackKey = ""): SheetCell {
  const cell = value && typeof value === "object" ? value as Dict : { value };
  return {
    ...cell,
    key: String(cell.key || cell.address || fallbackKey || `${rowIndex}-${cellIndex}`),
    text: String(cell.text ?? cell.display_value ?? cell.value ?? ""),
    colspan: Math.max(1, Number(cell.colspan || cell.col_span || 1)),
    rowspan: Math.max(1, Number(cell.rowspan || cell.row_span || 1)),
    hidden: Boolean(cell.hidden || cell.merged_into),
    rowIndex,
    columnIndex: cellIndex,
  };
}

function applySheetMerges(rows: SheetCell[][], model: Dict): void {
  for (const item of arrayFrom(model.merges)) {
    const startRow = Math.max(0, Number(item.row ?? item.start_row ?? 0));
    const startCol = Math.max(0, Number(item.col ?? item.start_col ?? 0));
    const rowspan = Math.max(1, Number(item.rowspan || (Number(item.end_row) - startRow + 1) || 1));
    const colspan = Math.max(1, Number(item.colspan || (Number(item.end_col) - startCol + 1) || 1));
    const anchor = rows[startRow]?.[startCol];
    if (!anchor) continue;
    anchor.rowspan = rowspan;
    anchor.colspan = colspan;
    for (let row = startRow; row < startRow + rowspan; row += 1) {
      for (let col = startCol; col < startCol + colspan; col += 1) {
        if (row !== startRow || col !== startCol) {
          const covered = rows[row]?.[col];
          if (covered) covered.hidden = true;
        }
      }
    }
  }
}

function applySignatureCells(rows: SheetCell[][], model: Dict): void {
  for (const item of arrayFrom(model.signature_cells)) {
    const range = String(item.range || "").split(":")[0];
    const position = cellCoordinates(range);
    const anchor = rows[position.row - 1]?.[position.col - 1];
    if (!anchor) continue;
    anchor.signature_cell = item;
  }
}

function applyTemplateImages(rows: SheetCell[][], model: Dict): void {
  for (const item of arrayFrom(model.images)) {
    const row = Math.max(0, Number(item.row || 0));
    const col = Math.max(0, Number(item.col || 0));
    const anchor = rows[row]?.[col];
    if (!anchor || !String(item.data_url || "").startsWith("data:image/")) continue;
    anchor.template_images = [...arrayFrom(anchor.template_images), item];
  }
}

function cellCoordinates(address: string): { row: number; col: number } {
  const match = String(address).match(/^([A-Z]+)(\d+)$/i);
  if (!match) return { row: 1, col: 1 };
  let col = 0;
  for (const char of match[1].toUpperCase()) col = col * 26 + char.charCodeAt(0) - 64;
  return { row: Number(match[2]), col };
}

function sheetCellContent(cell: SheetCell): ReturnType<typeof h>[] | string {
  const signature = cell.signature_cell && typeof cell.signature_cell === "object" ? cell.signature_cell as Dict : {};
  const directUrl = String(signature.image_data_url || signature.image_url || signature.signature_url || cell.image_url || cell.signature_url || cell.src || "");
  const content: ReturnType<typeof h>[] = [];
  if (directUrl) {
    content.push(h("img", { class: "sheet-signature-image", src: directUrl, alt: cell.text || "签名", onLoad: markPrintImageLoaded }));
  } else if (arrayFrom(signature.signers).length) {
    content.push(h("div", { class: "sheet-signers" }, arrayFrom(signature.signers).map((signer: Dict, index: number) => {
      const imageUrl = String(signer.image_url || signer.signature_url || "");
      return imageUrl
        ? h("img", { key: personId(signer) || index, src: imageUrl, alt: personName(signer), onLoad: markPrintImageLoaded })
        : h("span", { key: personId(signer) || index }, personName(signer));
    })));
  } else if (cell.text) {
    content.push(h("span", { class: "sheet-cell-text" }, cell.text));
  }
  for (const [index, image] of arrayFrom(cell.template_images).entries()) {
    content.push(h("img", {
      key: `template-${cell.key}-${index}`,
      class: "sheet-template-image",
      src: String(image.data_url || ""),
      alt: String(image.name || "模板图片"),
      style: {
        left: `${Number(image.x_offset_px || 0)}px`,
        top: `${Number(image.y_offset_px || 0)}px`,
        width: `${Math.max(1, Number(image.width_px || 1))}px`,
        height: `${Math.max(1, Number(image.height_px || 1))}px`,
      },
      onLoad: markPrintImageLoaded,
    }));
  }
  return content.length ? content : "";
}

function cellStyle(cell: SheetCell, model: Dict): Dict {
  const style: Dict = {};
  const configured = model.cell_styles?.[sheetCellAddress(cell.rowIndex, cell.columnIndex)] || {};
  const width = model.column_widths?.[String(cell.columnIndex)] ?? model.column_widths?.[cell.columnIndex];
  if (cell.width || width) style.width = `${Number(cell.width || width)}px`;
  const fill = configured.fill || cell.background || cell.fill;
  const align = configured.align || cell.align;
  const verticalAlign = configured.vertical_align || cell.vertical_align;
  const fontSize = configured.font_size || cell.font_size;
  if (fill) style.background = fill;
  if (align) style.textAlign = align === "centerContinuous" ? "center" : align;
  if (verticalAlign) style.verticalAlign = verticalAlign === "center" ? "middle" : verticalAlign;
  if (fontSize) style.fontSize = `${Number(fontSize)}pt`;
  if (configured.font_color || cell.font_color) style.color = configured.font_color || cell.font_color;
  if (configured.font_name) style.fontFamily = configured.font_name;
  if (configured.bold || cell.bold) style.fontWeight = "700";
  if (configured.italic) style.fontStyle = "italic";
  if (configured.underline) style.textDecoration = "underline";
  if (configured.wrap_text) style.whiteSpace = "pre-wrap";
  const lineStyles: Record<string, string> = {
    dashDot: "dashed", dashDotDot: "dashed", dashed: "dashed", dotted: "dotted",
    double: "double", hair: "solid", medium: "solid", mediumDashDot: "dashed",
    mediumDashDotDot: "dashed", mediumDashed: "dashed", slantDashDot: "dashed",
    thick: "solid", thin: "solid",
  };
  const lineWidths: Record<string, string> = { hair: "1px", medium: "2px", thick: "3px" };
  const borders = configured.borders && typeof configured.borders === "object" ? configured.borders : {};
  style.border = "none";
  for (const edge of ["top", "right", "bottom", "left"]) {
    const border = borders[edge];
    if (!border?.style) continue;
    style[`border${edge[0].toUpperCase()}${edge.slice(1)}`] = `${lineWidths[border.style] || "1px"} ${lineStyles[border.style] || "solid"} ${border.color || "#000"}`;
  }
  return style;
}

function sheetCellAddress(rowIndex: number, columnIndex: number): string {
  let column = "";
  for (let value = columnIndex + 1; value > 0; value = Math.floor((value - 1) / 26)) {
    column = String.fromCharCode(65 + (value - 1) % 26) + column;
  }
  return `${column}${rowIndex + 1}`;
}

function columnWidthStyle(model: Dict, columnIndex: number): Dict {
  const width = model.column_widths?.[String(columnIndex)] ?? model.column_widths?.[columnIndex];
  return width ? { width: `${Number(width)}px` } : {};
}

function sheetTableStyle(model: Dict): Dict {
  return {
    width: `${Math.max(1, Number(model.sheet_width_px || 1))}px`,
    tableLayout: "fixed",
  };
}

function printPageMargins(): Record<"top" | "right" | "bottom" | "left", number> {
  const margins = printModel.value.page_margins || {};
  return {
    top: Number(margins.top ?? .2) * 96,
    right: Number(margins.right ?? .2) * 96,
    bottom: Number(margins.bottom ?? .2) * 96,
    left: Number(margins.left ?? .2) * 96,
  };
}

function rowHeightStyle(model: Dict, rowIndex: number): Dict {
  const row = Array.isArray(model.row_heights)
    ? model.row_heights[rowIndex]
    : model.row_heights?.[String(rowIndex)] ?? model.row_heights?.[rowIndex];
  const height = Number(row || model.default_row_height_px || 0);
  return height ? { height: `${height}px` } : {};
}

function handleBeforeUnload(event: BeforeUnloadEvent): void {
  if (!hasUnsavedChanges.value) return;
  event.preventDefault();
  event.returnValue = "";
}

function handleHistoryNavigation(): void {
  if (restoringHistory) {
    restoringHistory = false;
    return;
  }
  const targetUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (!hasUnsavedChanges.value) {
    stablePageUrl = targetUrl;
    return;
  }
  window.history.pushState({}, "", stablePageUrl);
  requestDiscardChanges("离开会放弃当前未保存修改。", () => {
    restoringHistory = true;
    stablePageUrl = targetUrl;
    navigate(targetUrl);
  });
}

watch(() => props.scope, (value, previous) => {
  if (value !== previous && !props.printMode) void loadBootstrap();
});

onMounted(() => {
  disposed = false;
  window.addEventListener("beforeunload", handleBeforeUnload);
  window.addEventListener("popstate", handleHistoryNavigation);
  if (props.printMode) void loadPrintModel();
  else void loadBootstrap();
});

onBeforeUnmount(() => {
  disposed = true;
  clearStatusPoll(true);
  window.removeEventListener("beforeunload", handleBeforeUnload);
  window.removeEventListener("popstate", handleHistoryNavigation);
  printStyle?.remove();
});
</script>

<style scoped>
.drill-page {
  width: min(1800px, 100%);
  min-height: calc(100vh - 96px);
  margin: 0 auto;
  padding: 24px 30px 42px;
  color: #0f172a;
}

.drill-page__header {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  margin-bottom: 18px;
}

.drill-page__title span,
.section-heading span,
.toolbar-title span,
.configuration-panel__header > div > span,
.execution-header > div > span {
  color: #1e63ff;
  font-size: 12px;
  font-weight: 900;
}

.drill-page__title h1,
.section-heading h2,
.toolbar-title h2,
.configuration-panel__header h2,
.execution-header h2 {
  margin: 3px 0 0;
  color: #09204a;
  line-height: 1.25;
}

.drill-page__title h1 { font-size: 24px; }
.section-heading h2,
.toolbar-title h2,
.configuration-panel__header h2,
.execution-header h2 { font-size: 18px; }

.icon-button,
.primary-button,
.secondary-button,
.danger-button {
  min-height: 42px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  border: 1px solid #cfe0ff;
  border-radius: 15px;
  padding: 0 16px;
  background: #fff;
  color: #0f4fb8;
  font: inherit;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
}

.icon-button { width: 42px; padding: 0; }
.primary-button { border-color: transparent; background: linear-gradient(135deg, #1e63ff, #1554df); color: #fff; }
.danger-button { border-color: #fecdd3; background: #fff1f2; color: #be123c; }
.secondary-button.compact { min-height: 36px; padding: 0 12px; }
button:disabled { cursor: not-allowed; opacity: .58; }
button:focus-visible, input:focus-visible, select:focus-visible { outline: 3px solid rgba(30, 99, 255, .18); outline-offset: 1px; }

.page-state,
.print-state {
  width: min(720px, calc(100% - 32px));
  min-height: 150px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  margin: 48px auto;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255,255,255,.94);
  color: #31516f;
  box-shadow: 0 14px 34px rgba(0,47,135,.1);
}
.page-state.error, .print-state.error { border-color: #fecaca; color: #b91c1c; }
.spinning { animation: spin .9s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

.landing-view,
.admin-view,
.building-view { display: grid; gap: 16px; }

.section-heading,
.command-bar,
.building-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  border: 1px solid #d8e5f7;
  border-radius: 20px;
  padding: 16px 18px;
  background: rgba(255,255,255,.86);
  box-shadow: 0 12px 28px rgba(0,47,135,.07);
}
.section-heading > b { color: #64748b; font-size: 12px; }

.building-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}
.building-card {
  min-height: 176px;
  display: grid;
  grid-template-columns: auto minmax(0,1fr);
  align-content: center;
  gap: 9px 14px;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  padding: 22px;
  background: linear-gradient(145deg, rgba(255,255,255,.98), rgba(239,246,255,.88));
  color: #0f172a;
  text-align: left;
  box-shadow: 0 14px 32px rgba(0,47,135,.08);
  cursor: pointer;
}
.building-card.admin { background: linear-gradient(145deg, #0c4bb7, #052f83); color: #fff; }
.building-card:disabled { filter: grayscale(.55); }
.building-icon { width: 48px; height: 48px; display: grid; place-items: center; grid-row: span 2; border-radius: 17px; background: #e7f0ff; color: #1e63ff; }
.building-card.admin .building-icon { background: rgba(255,255,255,.14); color: #fff; }
.building-card strong { font-size: 18px; }
.building-card small { color: #64748b; }
.building-card.admin small { color: rgba(255,255,255,.72); }
.building-card > b { grid-column: 1 / -1; display: flex; align-items: center; justify-content: flex-end; gap: 5px; color: #1e63ff; font-size: 13px; }
.building-card.admin > b { color: #fff; }

.summary-pills { display: flex; flex-wrap: wrap; gap: 8px; }
.admin-month-filter { width: 180px; margin-left: auto; }
.summary-pills span, .status-chip { border: 1px solid #cfe0ff; border-radius: 999px; padding: 6px 10px; background: #eff6ff; color: #1d4ed8; font-size: 12px; font-weight: 850; }
.status-chip.success, small.success { color: #047857; background: #ecfdf5; border-color: #a7f3d0; }
.status-chip.warning, small.warning { color: #92400e; background: #fffbeb; border-color: #fde68a; }
.status-chip.failed, small.failed { color: #b91c1c; background: #fef2f2; border-color: #fecaca; }
.status-chip.neutral, small.neutral { color: #475569; background: #f8fafc; border-color: #e2e8f0; }

.upload-panel {
  display: grid;
  grid-template-columns: 180px 220px minmax(300px, 1fr);
  gap: 14px;
  border: 1px solid #bdd2f4;
  border-radius: 22px;
  padding: 18px;
  background: rgba(255,255,255,.94);
  box-shadow: 0 16px 36px rgba(0,47,135,.09);
}
label { display: grid; gap: 6px; min-width: 0; color: #475569; font-size: 12px; font-weight: 800; }
label > span b { color: #e11d48; }
input, select, textarea {
  width: 100%; min-height: 42px; border: 1px solid #d8e5f7; border-radius: 14px; padding: 0 11px;
  background: #fff; color: #0f172a; font: inherit;
}
textarea { resize: vertical; min-height: 82px; padding-block: 10px; line-height: 1.6; }
.scenario-field { border: 1px solid #dbe7f5; border-radius: 17px; padding: 14px; background: #fbfdff; }
.wide { grid-column: 1 / -1; }
.drill-scope-picker { margin: 0; padding: 10px 12px; border: 1px solid #d8e5f7; border-radius: 14px; }
.drill-scope-picker legend { color: #475569; font-size: 12px; font-weight: 800; }
.drill-scope-picker > div { display: flex; flex-wrap: wrap; gap: 10px 18px; }
.drill-scope-picker label { display: flex; align-items: center; gap: 6px; min-height: 36px; cursor: pointer; }
.drill-scope-picker input { width: 16px; min-height: 16px; accent-color: #1e63ff; }
.drill-scope-picker small { display: block; margin-top: 4px; color: #64748b; }
.file-drop { min-height: 116px; place-items: center; align-content: center; border: 1px dashed #9cc7ff; border-radius: 17px; padding: 18px; background: #f5faff; color: #1e63ff; text-align: center; cursor: pointer; }
.file-drop.active { background: #e6f1ff; box-shadow: 0 0 0 4px rgba(30,99,255,.12); }
.file-drop input { position: absolute; width: 1px; height: 1px; opacity: 0; }
.file-drop small { color: #64748b; }
.form-actions { display: flex; justify-content: flex-end; gap: 10px; }

.admin-layout,
.building-layout { display: grid; grid-template-columns: 290px minmax(0,1fr); gap: 16px; align-items: start; }
.drill-list { display: grid; gap: 8px; position: sticky; top: 14px; max-height: calc(100vh - 170px); overflow: auto; border: 1px solid #d8e5f7; border-radius: 20px; padding: 10px; background: rgba(255,255,255,.86); }
.drill-list__item { display: grid; gap: 5px; border: 1px solid transparent; border-radius: 15px; padding: 12px; background: #f7fbff; color: #334155; text-align: left; cursor: pointer; }
.drill-list__item.active { border-color: #9cc7ff; background: #edf5ff; box-shadow: inset 0 0 0 1px rgba(30,99,255,.1); }
.drill-list__item strong { color: #0f2f6a; line-height: 1.45; }
.drill-list__item span { color: #64748b; font-size: 12px; }
.drill-list__item small { justify-self: start; border: 1px solid; border-radius: 999px; padding: 3px 7px; font-size: 11px; }

.configuration-panel,
.execution-panel,
.empty-panel { min-width: 0; display: grid; gap: 15px; border: 1px solid #d8e5f7; border-radius: 22px; padding: 18px; background: rgba(255,255,255,.9); box-shadow: 0 14px 34px rgba(0,47,135,.08); }
.configuration-panel__header,
.execution-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }
.configuration-panel__header small { display: block; margin-top: 6px; color: #64748b; }
fieldset, details { min-width: 0; border: 1px solid #dbe7f5; border-radius: 17px; padding: 14px; background: #fbfdff; }
fieldset { margin: 0; }
legend, summary { color: #0f2f6a; font-size: 14px; font-weight: 900; }
summary { cursor: pointer; }
details > :not(summary) { margin-top: 13px; }
details.locked { opacity: .75; }
.field-grid, .mapping-grid { display: grid; gap: 12px; }
.field-grid.two { grid-template-columns: repeat(2, minmax(0,1fr)); }
.field-grid.four { grid-template-columns: repeat(4, minmax(0,1fr)); }
.field-grid .span-two { grid-column: span 2; }
.mapping-grid { grid-template-columns: repeat(3, minmax(0,1fr)); }
.mapping-grid.compact { grid-template-columns: repeat(5, minmax(100px,1fr)); }

.step-table-wrap, .sheet-table-wrap { min-width: 0; overflow: auto; border: 1px solid #dbe7f5; border-radius: 14px; background: #fff; }
.step-table, .sheet-preview-table { border-collapse: collapse; }
.step-table { width: 100%; }
.step-table th, .step-table td { border: 1px solid #dbe3ee; padding: 8px; color: #334155; font-size: 12px; line-height: 1.5; vertical-align: middle; }
.step-table th { background: #eff6ff; color: #0f4fb8; text-align: left; white-space: nowrap; }
.step-table td:nth-child(3) { min-width: 300px; }
.step-table input { width: 76px; }
.sheet-preview-table td { box-sizing: border-box; padding: 1px 3px; color: #000; font-size: 11pt; line-height: 1.2; white-space: pre-wrap; text-align: center; vertical-align: middle; }
.sheet-preview-table img { display: block; max-width: 100%; max-height: 76px; margin: auto; object-fit: contain; }
.sheet-preview-table img.sheet-signature-image { width: 100%; height: 100%; max-height: none; margin: 0; }
.sheet-preview-table td.sheet-cell-has-image { position: relative; overflow: visible; }
.sheet-cell-text { position: relative; z-index: 1; }
.sheet-preview-table img.sheet-template-image { position: absolute; z-index: 2; max-width: none; max-height: none; margin: 0; object-fit: contain; pointer-events: none; }
.sheet-image-warning { position: sticky; left: 0; margin: 0; padding: 8px 10px; border-top: 1px solid #fde68a; background: #fffbeb; color: #92400e; font-size: 11px; font-weight: 800; }
.sheet-signers { min-height: 34px; display: flex; align-items: center; justify-content: flex-start; gap: 2px; overflow: hidden; }
.sheet-signers span { color: #31516f; font-family: "KaiTi", "STKaiti", serif; font-size: 13px; font-weight: 700; white-space: nowrap; }
.sheet-signers img { min-width: 0; flex: 1 1 0; max-width: 120px; margin: 0; object-fit: contain; }
.sheet-table-wrap.compact { max-height: 480px; }

.sticky-actions { position: sticky; bottom: 10px; z-index: 5; display: flex; align-items: center; flex-wrap: wrap; gap: 9px; border: 1px solid #cfe0ff; border-radius: 18px; padding: 10px; background: rgba(255,255,255,.96); box-shadow: 0 14px 34px rgba(0,47,135,.14); backdrop-filter: blur(10px); }
.action-spacer { flex: 1; }

.building-toolbar { justify-content: flex-start; }
.toolbar-title { margin-right: auto; }
.building-toolbar label { width: 190px; }
.sheet-tabs { display: flex; align-items: center; gap: 8px; border-bottom: 1px solid #e2ebf7; padding-bottom: 10px; overflow-x: auto; }
.sheet-tabs button { min-height: 40px; display: inline-flex; align-items: center; gap: 7px; border: 1px solid #d8e5f7; border-radius: 14px; padding: 0 14px; background: #f8fbff; color: #52667e; font: inherit; font-weight: 850; white-space: nowrap; cursor: pointer; }
.sheet-tabs button.active { border-color: #9cc7ff; background: #eaf3ff; color: #155dfc; }
.sheet-tabs .print-button { margin-left: auto; }
.record-editor, .assessment-view { display: grid; gap: 15px; min-width: 0; }

.people-panel,
.steps-panel { display: grid; gap: 12px; border: 1px solid #dbe7f5; border-radius: 18px; padding: 14px; background: #fbfdff; }
.people-panel > header,
.steps-panel > header { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
.people-panel > header > div,
.steps-panel > header { color: #0f2f6a; }
.people-panel > header > div { display: flex; align-items: center; gap: 7px; }
.people-panel > header input { width: min(380px, 48%); }
.people-filter-actions { display: flex; align-items: center; justify-content: flex-end; gap: 8px; min-width: min(520px, 58%); }
.people-filter-actions input { min-width: 220px; flex: 1 1 auto; }
.people-panel header span,
.steps-panel header span { border-radius: 999px; padding: 3px 8px; background: #eaf3ff; color: #1e63ff; font-size: 11px; }
.selected-people { display: flex; flex-wrap: wrap; gap: 7px; min-height: 34px; align-items: center; }
.selected-people > span { display: inline-flex; align-items: center; gap: 6px; border: 1px solid #bfdbfe; border-radius: 999px; padding: 5px 6px 5px 10px; background: #eff6ff; color: #1d4ed8; font-size: 12px; font-weight: 800; }
.selected-people > span.missing { border-color: #fde68a; background: #fffbeb; color: #92400e; }
.selected-people small { font-size: 10px; opacity: .78; }
.selected-people button { width: 24px; height: 24px; border: 0; border-radius: 999px; background: rgba(255,255,255,.8); color: inherit; cursor: pointer; }
.people-options { display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 8px; max-height: 270px; overflow: auto; }
.people-options label { grid-template-columns: auto minmax(0,1fr) auto; align-items: center; border: 1px solid #e2e8f0; border-radius: 13px; padding: 9px; background: #fff; cursor: pointer; }
.people-options label.selected { border-color: #93c5fd; background: #eff6ff; }
.people-options input { width: 16px; min-height: 16px; }
.people-options label span { display: grid; min-width: 0; }
.people-options label strong, .people-options label small { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.people-options label small { color: #64748b; font-size: 10px; }
.people-options i { font-size: 10px; font-style: normal; }
.people-options i.ready { color: #047857; }
.people-options i.missing { color: #b45309; }

.execution-step { display: grid; grid-template-columns: 38px minmax(240px,1fr) minmax(320px,1.2fr); gap: 12px; align-items: center; border: 1px solid #e2ebf7; border-radius: 15px; padding: 12px; background: #fff; }
.step-number { width: 34px; height: 34px; display: grid; place-items: center; border-radius: 12px; background: linear-gradient(135deg,#1e63ff,#1554df); color: #fff; font-weight: 900; }
.step-copy { min-width: 0; }
.step-copy span { color: #1e63ff; font-size: 11px; font-weight: 850; }
.step-copy p { margin: 4px 0 0; color: #334155; font-size: 13px; line-height: 1.55; }
.signer-grid { display: grid; grid-template-columns: repeat(2,minmax(0,1fr)); gap: 8px; }
.signature-actions { display: flex; flex-wrap: wrap; gap: 8px; }
.drill-signature-canvas { position: relative; min-height: 360px; overflow: hidden; border: 1px dashed #93c5fd; border-radius: 18px; background: linear-gradient(90deg, rgba(37,99,235,.05) 1px, transparent 1px), linear-gradient(rgba(37,99,235,.05) 1px, transparent 1px), #fff; background-size: 22px 22px; touch-action: none; user-select: none; }
.drill-signature-canvas canvas { position: relative; z-index: 2; display: block; width: 100%; height: min(56vh, 520px); min-height: 360px; cursor: crosshair; touch-action: none; }
.sign-clear-inline { position: absolute; z-index: 5; top: 10px; right: 10px; border: 1px solid rgba(148,163,184,.38); border-radius: 999px; padding: 5px 10px; background: rgba(255,255,255,.92); color: #3156c9; font: inherit; font-size: 12px; font-weight: 850; cursor: pointer; }
.sign-clear-inline:disabled { opacity: .55; cursor: not-allowed; }
.sign-placeholder { position: absolute; z-index: 3; inset: 0; display: grid; place-items: center; pointer-events: none; color: #94a3b8; font-weight: 850; }
.execution-actions { justify-content: flex-end; }
.save-indicator { margin-right: auto; color: #64748b; font-size: 12px; font-weight: 800; }
.save-indicator.dirty { color: #b45309; }
.save-indicator.saved { color: #047857; }
.inline-state { min-height: 160px; display: flex; align-items: center; justify-content: center; gap: 9px; color: #64748b; }
.empty-panel { min-height: 260px; place-items: center; align-content: center; color: #64748b; text-align: center; }
.empty-panel strong { color: #0f2f6a; }
.empty-panel.compact { min-height: 160px; }
.empty-inline { padding: 20px 10px; color: #64748b; font-size: 12px; text-align: center; }

.drill-print-page { min-height: 100vh; padding: 0; overflow: hidden; background: #fff; color: #111827; }
.print-sheet-frame { position: relative; overflow: hidden; margin: 0 auto; }
.print-sheet { transform-origin: top left; }
.drill-print-page .sheet-table-wrap { overflow: visible; border: 0; border-radius: 0; }
.drill-print-page .sheet-preview-table { width: 100%; table-layout: fixed; }
.drill-print-page .sheet-preview-table td { padding: 1px 3px; color: #000; font-size: 10pt; line-height: 1.2; }
.drill-print-page .sheet-preview-table img { max-height: 18mm; }

@media print {
  .drill-print-page { width: 100%; height: 100%; min-height: auto; }
  .print-state { display: none !important; }
  .print-sheet-frame { break-inside: avoid; page-break-inside: avoid; }
  .sheet-image-warning { display: none !important; }
}

@media (max-width: 1100px) {
  .building-grid { grid-template-columns: repeat(2,minmax(0,1fr)); }
  .admin-layout, .building-layout { grid-template-columns: 1fr; }
  .drill-list { position: static; max-height: 300px; }
  .mapping-grid, .mapping-grid.compact { grid-template-columns: repeat(2,minmax(0,1fr)); }
  .people-options { grid-template-columns: repeat(2,minmax(0,1fr)); }
  .execution-step { grid-template-columns: 38px minmax(0,1fr); }
  .signer-grid { grid-column: 2; }
}

@media (max-width: 720px) {
  .drill-page { padding: 14px 12px 28px; }
  .drill-page__header { gap: 10px; }
  .drill-page__title h1 { font-size: 19px; }
  .building-grid, .mapping-grid, .mapping-grid.compact, .field-grid.two, .field-grid.four, .people-options { grid-template-columns: 1fr; }
  .field-grid .span-two { grid-column: auto; }
  .command-bar, .building-toolbar { align-items: stretch; flex-direction: column; }
  .admin-month-filter { width: 100%; margin-left: 0; }
  .building-toolbar label { width: 100%; }
  .upload-panel { grid-template-columns: 1fr; }
  .wide { grid-column: auto; }
  .people-panel > header { align-items: stretch; flex-direction: column; }
  .people-panel > header input { width: 100%; }
  .people-filter-actions { width: 100%; min-width: 0; flex-wrap: wrap; }
  .execution-step { grid-template-columns: 34px minmax(0,1fr); }
  .signer-grid { grid-template-columns: 1fr; grid-column: 1 / -1; }
  .sticky-actions { position: static; }
  .save-indicator, .action-spacer { width: 100%; }
  .sheet-tabs .print-button { margin-left: 0; }
}
</style>
