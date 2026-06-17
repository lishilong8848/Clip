<template>
  <main class="app-shell" :class="{ 'signature-link-shell': signatureLinkMode }">
    <header v-if="!signatureLinkMode" class="topbar">
      <div class="brand">
        <img class="brand-logo" :src="brandLogoSrc" alt="世纪互联官方标识" />
        <div>
          <h1>南通基地-运维灯塔工作台</h1>
          <p>{{ headerSubtitle }}</p>
        </div>
      </div>
      <div class="topbar-actions">
        <span v-if="auth.loggedIn" class="user-chip">{{ auth.user?.name || auth.user?.open_id || "已登录" }}</span>
        <button v-if="auth.loggedIn && (isWorkbench || isEngineerMopPage)" class="btn ghost" @click="returnToHome">功能选择</button>
        <label v-if="auth.loggedIn && isWorkbench && visibleScopeOptions.length > 1" class="scope-switch">
          <span>切换楼栋</span>
          <select :value="currentScope" :disabled="loading" @change="switchScope(($event.target as HTMLSelectElement).value)">
            <option v-for="item in visibleScopeOptions" :key="item.value" :value="normalizeScopeValue(item.value)">
              {{ item.label }}
            </option>
          </select>
        </label>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="loading || refreshCooldown.workbench" @click="manualRefreshWorkbench">
          {{ loading ? "刷新中" : refreshCooldown.workbench ? "稍后再刷" : "刷新本页" }}
        </button>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="repairRefreshing || refreshCooldown.repair" @click="refreshRepair">
          {{ repairRefreshing ? "检修刷新中" : refreshCooldown.repair ? "稍后再刷" : "刷新检修" }}
        </button>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="changeRefreshing || refreshCooldown.change" @click="refreshChange">
          {{ changeRefreshing ? "变更刷新中" : refreshCooldown.change ? "稍后再刷" : "刷新变更" }}
        </button>
        <button v-if="isAdmin" class="btn ghost" @click="showAdminTools = true">管理/诊断</button>
        <button v-if="auth.loggedIn" class="btn danger-text" @click="logout">退出</button>
      </div>
    </header>

    <div v-if="!signatureLinkMode && connectionNotice" class="status-banner" :class="connectionNotice.tone">
      <span>{{ connectionNotice.text }}</span>
      <button
        v-if="connectionNotice.action"
        class="btn ghost small"
        type="button"
        @click="connectionNotice.action"
      >
        {{ connectionNotice.actionLabel }}
      </button>
    </div>
    <div v-if="pageStatusText" class="page-status">
      {{ pageStatusText }}
    </div>

    <AdminTools
      :open="showAdminTools"
      :scope-options="requestableScopes"
      @close="showAdminTools = false"
    />

    <HistoryMemoryPage
      v-if="isHistoryMemoryPage"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :is-admin="isAdmin"
      :login-url="auth.loginUrl"
    />

    <SignaturePage
      v-else-if="isSignaturePage"
      :default-scope="currentScope"
    />

    <EngineerMopPage
      v-else-if="isEngineerMopPage"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :login-url="auth.loginUrl"
      :scope-options="visibleScopeOptions"
    />

    <AuthPanels
      v-else-if="showPermissionRequestPanel || authChecking || !auth.loggedIn || (auth.loggedIn && !auth.scopeOptions.length)"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :user="auth.user"
      :login-url="auth.loginUrl"
      :busy="permissionBusy"
      :request="permissionRequest"
      :requestable-scopes="permissionPanelRequestableScopes"
      :title="permissionPanelTitle"
      :description="permissionPanelDescription"
      :empty-text="permissionPanelEmptyText"
      :show-back="showPermissionRequestPanel && auth.scopeOptions.length > 0"
      @update-request="updatePermissionRequest"
      @submit="submitPermissionRequest"
      @confirm="confirmPermissionRequest"
      @back="closePermissionRequestPanel"
    />

    <ScopeHome
      v-else-if="!isWorkbench"
      :scope-options="visibleScopeOptions"
      :overview="scopeOverview"
      :handover-links="handoverLinks"
      :can-request-more-scopes="additionalRequestableScopes.length > 0"
      @enter="enterScope"
      @engineer="enterEngineerMop"
      @request-permission="openAdditionalPermissionRequest"
    />

    <section v-else class="workbench">
      <div v-if="loading" class="loading-line">
        正在加载 {{ scopeLabel(currentScope) }} 数据...
      </div>
      <div class="summary-strip">
        <article>
          <span>已发起</span>
          <strong>{{ liveDailyStats.started || 0 }}</strong>
        </article>
        <article>
          <span>有更新</span>
          <strong>{{ liveDailyStats.updated || 0 }}</strong>
        </article>
        <article>
          <span>已结束</span>
          <strong>{{ liveDailyStats.ended || 0 }}</strong>
        </article>
        <article>
          <span>进行中</span>
          <strong>{{ liveOngoingCount }}</strong>
        </article>
      </div>

      <div class="toolbar">
        <div class="segmented">
          <button
            v-for="type in workTypes"
            :key="type.value"
            :class="{ active: workType === type.value }"
            @click="selectWorkType(type.value)"
          >
            {{ type.label }} {{ recordTypeCounts[type.value] || 0 }}
          </button>
        </div>
        <input v-model="searchText" class="search" placeholder="搜索标题、楼栋、专业" />
        <label class="specialty-filter">
          <span>专业</span>
          <select v-model="specialtyFilter">
            <option value="">全部</option>
            <option v-for="item in specialtyFilterOptions" :key="item" :value="item">{{ item }}</option>
          </select>
        </label>
        <div class="manual-create">
          <button class="btn ghost" @click="showManualTypePicker = !showManualTypePicker">纯手填</button>
          <div v-if="showManualTypePicker" class="manual-type-popover">
            <strong>选择纯手填通告类型</strong>
            <p>先选类型再填写，避免在维保页误发成维保通告。</p>
            <div v-if="manualRecentTypeOptions.length" class="manual-recent">
              <span>最近使用</span>
              <button
                v-for="type in manualRecentTypeOptions"
                :key="type.value"
                type="button"
                @click="addManualDraft(type.value)"
              >
                {{ type.label }}
              </button>
            </div>
            <div class="manual-type-grid">
              <button
                v-for="type in workTypes"
                :key="type.value"
                type="button"
                @click="addManualDraft(type.value)"
              >
                <span>{{ type.label }}</span>
                <small v-if="manualPrefillWorkTypes.has(type.value)">带入上次内容</small>
                <small v-else>空白模板</small>
              </button>
            </div>
          </div>
        </div>
        <button class="btn ghost" @click="showPasteParser = !showPasteParser">解析粘贴通告</button>
        <button v-if="isAdmin" class="btn ghost" @click="showMemoryImporter = !showMemoryImporter">导入历史记忆</button>
        <span v-if="draftSaveText" class="draft-save-status" :class="{ failed: draftSaveFailed }">
          {{ draftSaveText }}
        </span>
      </div>

      <section v-if="showPasteParser" class="paste-panel">
        <textarea v-model="pasteText" placeholder="粘贴完整维保、变更、检修、上电、轮巡或调整通告文本"></textarea>
        <div class="card-actions">
          <span class="job-line" :class="{ failed: pasteParseStatus === 'failed', success: pasteParseStatus === 'success' }">
            {{ pasteParseLine }}
          </span>
          <button class="btn blue" :disabled="pasteParseBusy" @click="parsePastedNotice">
            {{ pasteParseBusy ? "解析中" : "解析到待发起通告" }}
          </button>
        </div>
        <div v-if="pendingChangeTargetSelection" class="target-choice-panel">
          <div>
            <strong>请选择要{{ pendingChangeTargetSelection.actionLabel }}的{{ workTypeLabel(pendingChangeTargetSelection.type) }}记录</strong>
            <p>原文状态为“{{ pendingChangeTargetSelection.actionLabel }}”。可同时选择目标多维记录和源表记录；如果缺少主界面条目，也能用这两类记录继续上传。</p>
            <p v-if="pendingChangeTargetSelection.totalMatched" class="target-count-line">
              当前显示 {{ pendingChangeTargetSelection.returnedCount }} 条，目标表共匹配 {{ pendingChangeTargetSelection.totalMatched }} 条{{ pendingChangeTargetSelection.limited ? "，请结合匹配原因选择" : "" }}。
            </p>
          </div>
          <div class="target-choice-layout">
            <div class="target-choice-column">
              <label class="candidate-search">
                <span>搜索目标记录</span>
                <input v-model="changeTargetSearchText" type="search" placeholder="标题、楼栋、时间、匹配原因、字段内容" />
              </label>
              <p class="candidate-count">当前显示 {{ filteredChangeTargetCandidates.length }} / {{ pendingChangeTargetSelection.candidates.length }} 条</p>
              <div class="target-choice-list">
              <p v-if="!pendingChangeTargetSelection.candidates.length" class="target-empty-line">未找到同名目标多维记录，可先选择源表记录继续尝试关联。</p>
              <p v-else-if="!filteredChangeTargetCandidates.length" class="target-empty-line">没有匹配的目标记录，请调整搜索条件。</p>
              <button
                v-for="item in filteredChangeTargetCandidates"
                :key="changeTargetCandidateId(item)"
                class="target-choice"
                :class="{ active: selectedChangeTargetId === changeTargetCandidateId(item) }"
                @mouseenter="previewChangeTarget(item)"
                @focus="previewChangeTarget(item)"
                @click="selectChangeTarget(item)"
              >
                <strong>{{ item.title || item.record_id }}</strong>
                <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
                <small>{{ item.match_reason || (item.date_matched ? "时间匹配" : "按名称匹配") }}</small>
              </button>
              </div>
            </div>
            <aside v-if="visibleActiveChangeTargetCandidate" class="target-detail-popover">
              <div class="target-detail-head">
                <strong>{{ visibleActiveChangeTargetCandidate.title || `${workTypeLabel(pendingChangeTargetSelection.type)}记录` }}</strong>
                <span>{{ visibleActiveChangeTargetCandidate.building || "-" }} · {{ visibleActiveChangeTargetCandidate.status || "未标记状态" }}</span>
                <small>{{ visibleActiveChangeTargetCandidate.match_reason || "" }}</small>
              </div>
              <dl class="target-detail-grid">
                <template v-for="row in changeTargetDetailRows(visibleActiveChangeTargetCandidate)" :key="row.label">
                  <dt>{{ row.label }}</dt>
                  <dd>{{ row.value }}</dd>
                </template>
              </dl>
              <button class="btn blue target-confirm" :disabled="changeTargetConfirming || !selectedChangeTargetVisible" @click="confirmPastedChangeTarget">
                {{ changeTargetConfirming ? "确认中" : "确认关联这条记录" }}
              </button>
            </aside>
          </div>
          <div v-if="changeSourceCandidates.length" class="source-choice-panel">
            <div>
              <strong>对应源表记录</strong>
              <p>选择源表记录后，后续状态、闭环和来源追踪会更准确；不选择也可用目标多维记录继续上传。</p>
            </div>
            <label class="candidate-search">
              <span>搜索源表记录</span>
              <input v-model="changeSourceSearchText" type="search" placeholder="标题、楼栋、状态、时间、字段内容" />
            </label>
            <p class="candidate-count">当前显示 {{ filteredChangeSourceCandidates.length }} / {{ changeSourceCandidates.length }} 条</p>
            <div class="source-choice-list">
              <p v-if="!filteredChangeSourceCandidates.length" class="target-empty-line">没有匹配的源表记录，请调整搜索条件。</p>
              <button
                v-for="item in filteredChangeSourceCandidates"
                :key="changeSourceCandidateId(item)"
                class="source-choice"
                :class="{ active: selectedChangeSourceId === changeSourceCandidateId(item) }"
                @click="selectedChangeSourceId = changeSourceCandidateId(item)"
              >
                <strong>{{ item.title || item.record_id }}</strong>
                <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
              </button>
            </div>
            <button
              v-if="!pendingChangeTargetSelection.candidates.length"
              class="btn blue target-confirm"
              :disabled="changeTargetConfirming || !selectedChangeSourceVisible"
              @click="confirmPastedChangeTarget"
            >
              {{ changeTargetConfirming ? "确认中" : "确认关联源表记录" }}
            </button>
          </div>
        </div>
      </section>

      <section v-if="showMemoryImporter && isAdmin" class="paste-panel">
        <div class="panel-head compact-head">
          <h2>导入历史通告记忆</h2>
          <span>只写入记忆，不发送、不上传</span>
        </div>
        <textarea
          v-model="memoryImportText"
          placeholder="可一次粘贴多条历史维保、变更、检修通告。导入后，同楼栋同标题/同维护总项的本月事项会自动回填。"
        ></textarea>
        <div class="card-actions">
          <span class="job-line" :class="{ success: memoryImportLineType === 'success', failed: memoryImportLineType === 'failed' }">
            {{ memoryImportLine }}
          </span>
          <button class="btn blue" :disabled="memoryImportBusy" @click="importHistoricalMemory">
            {{ memoryImportBusy ? "导入中" : "导入到记忆库" }}
          </button>
        </div>
      </section>

      <section class="workspace">
        <aside class="panel records-panel">
          <div class="panel-head">
            <h2>待发起事项</h2>
            <span>{{ filteredRows.length }}</span>
          </div>
          <VirtualNoticeList
            :rows="filteredRows"
            :selected-id="activeDraftKey"
            show-status
            empty-text="当前筛选下没有待发起事项"
            @select="toggleRecordSelection"
          />
        </aside>

        <section class="panel drafts-panel">
          <div class="panel-head">
            <h2>待发起通告</h2>
            <span>{{ selectedDraftRows.length }}</span>
          </div>
          <div v-if="selectedDraftRows.length === 0" class="empty-block">
            {{ specialtyFilter ? "当前专业下没有待发起通告，可切换专业或选择全部。" : "从左侧选择事项，或使用纯手填、解析粘贴通告。" }}
          </div>
          <div v-else ref="draftStackRef" class="draft-stack">
            <DraftNoticeCard
              v-for="row in selectedDraftRows"
              :key="row.key"
              :row-key="row.key"
              :record="row.record"
              :draft="row.draft"
              :title="row.title"
              :active="row.key === activeDraftKey"
              :busy="isLineBusy(row.key)"
              :meta="draftCardMeta(row.record, row.draft, row.key === activeDraftKey)"
              :summary="draftSummary(row.record, row.draft)"
              :warning-text="row.record.manual && !row.draft.validation_touched ? draftTypeConflictText(row.record, row.draft) : ''"
              :missing-text="draftMissingText(row.record, row.draft)"
              :work-type="draftWorkType(row.record, row.draft)"
              :requestable-scopes="requestableScopes"
              :maintenance-cycle-options="maintenanceCycleOptions"
              :zhihang-records="zhihangRecords"
              :upload-preview-rows="draftUploadPreviewRows(row.record, row.draft)"
              :notice-preview-text="noticePreviewText(row.record, row.draft)"
              :preview-visible="previewDraftKey === row.key"
              :type-override-visible="canToggleWorkTypeOverride(row.record)"
              :type-override-busy="typeOverrideBusyKey === row.key"
              :type-override-label="workTypeOverrideButtonLabel(row.record)"
              :send-label="sendDraftButtonLabel(row.record, row.draft)"
              :field-class="(field) => draftFieldClass(row.record, row.draft, field)"
              :job-text="jobText"
              :job-class="jobClass"
              @activate="activeDraftKey = row.key"
              @pin="pinDraftInMiddlePanel(row.key)"
              @remove="removeDraft(row.key)"
              @set-draft="(field, value) => setDraftField(row.draft, field, value)"
              @manual-type-change="onManualDraftTypeChange(row.draft)"
              @building-change="onDraftBuildingChange(row.draft)"
              @bind-zhihang="bindZhihang(row.draft)"
              @toggle-preview="previewDraftKey = previewDraftKey === row.key ? '' : row.key"
              @send="sendStart(row.key)"
              @toggle-work-type-override="toggleWorkTypeOverride(row.record)"
            />
          </div>
        </section>

        <aside class="panel ongoing-panel">
          <div class="panel-head">
            <h2>已开始未结束</h2>
            <span>{{ filteredOngoing.length }}</span>
          </div>
          <div v-if="filteredOngoing.length === 0" class="empty-block">
            {{ specialtyFilter ? "当前专业下没有进行中通告" : "当前没有进行中通告" }}
          </div>
          <div v-else class="ongoing-list">
            <OngoingNoticeCard
              v-for="item in filteredOngoing"
              :key="ongoingLineKey(item)"
              :item="item"
              :draft="ongoingDraft(item)"
              :title="ongoingTitle(item)"
              :meta="ongoingMeta(item)"
              :compact-summary="ongoingCompactSummary(item)"
              :line-key="ongoingLineKey(item)"
              :undo-line-key="undoLineKey(item)"
              :expanded="isOngoingExpanded(item)"
              :busy="isLineBusy(ongoingLineKey(item))"
              :undo-busy="isLineBusy(undoLineKey(item))"
              :needs-binding="ongoingNeedsBinding(item)"
              :photo-count="ongoingPhotoCount(item)"
              :site-photo-required="ongoingEndRequiresSitePhoto(item)"
              :maintenance-cycle-options="maintenanceCycleOptions"
              :zhihang-records="zhihangRecords"
              :job-text="jobText"
              :job-class="jobClass"
              @expand="expandOngoingCard(item)"
              @toggle="toggleOngoingCard(item)"
              @set-edit="(key, value) => setOngoingEdit(item, key, value)"
              @bind-zhihang="(recordId) => bindOngoingZhihang(item, recordId)"
              @photo-input="(event) => handleOngoingPhotoInput(item, event)"
              @photo-paste="(event) => handleOngoingPhotoPaste(item, event)"
              @remove-photo="(index) => removeOngoingPhoto(item, index)"
              @send="(action) => sendOngoing(item, action)"
              @delete="deleteOngoing(item)"
              @bind-target="bindOngoingTarget(item)"
              @apply-undo="applyUndo(item)"
            />
          </div>
          <RecentUndoPanel
            v-model="undoFilter"
            :items="recentUndoItems"
            :job-text="jobText"
            :job-class="jobClass"
            :is-line-busy="isLineBusy"
            @apply="applyUndo"
          />
          <div v-if="closedSummaryItems.length" class="closed-today">
            <div class="panel-head compact">
              <h3>今日结束通告</h3>
              <span>{{ closedSummaryItems.length }}</span>
            </div>
            <article v-for="item in closedSummaryItems" :key="closedLineKey(item)" class="closed-card">
              <div>
                <strong>{{ item.title || "未命名通告" }}</strong>
                <p>{{ workTypeLabel(item.work_type) }} · {{ item.building || "-" }} · {{ item.ended_at || item.updated_at || "-" }}</p>
              </div>
              <button v-if="item.undo_available" class="btn ghost" :disabled="isLineBusy(undoLineKey(item))" @click="applyUndo(item)">回退</button>
              <span class="job-line" :class="jobClass(undoLineKey(item))">{{ jobText(undoLineKey(item)) }}</span>
            </article>
          </div>
        </aside>
      </section>
    </section>

    <TargetRecordSelectionModal
      :selection="ongoingBindSelection"
      :candidates="ongoingBindCandidates"
      :selected-id="selectedOngoingBindId"
      :active-candidate="activeOngoingBindCandidate"
      :candidate-id="changeTargetCandidateId"
      :detail-rows-for="changeTargetDetailRows"
      @cancel="cancelOngoingBindSelection"
      @confirm="confirmOngoingBindSelection"
      @preview="previewOngoingBindCandidate"
      @select="selectOngoingBindCandidate"
    />
    <div v-if="actionConfirm.open" class="action-confirm-backdrop" @click.self="resolveActionConfirm(false)">
      <section
        class="action-confirm-modal"
        :class="`tone-${actionConfirm.tone}`"
        role="dialog"
        aria-modal="true"
        aria-labelledby="action-confirm-title"
      >
        <header>
          <div>
            <span>{{ actionConfirm.kicker }}</span>
            <strong id="action-confirm-title">{{ actionConfirm.title }}</strong>
          </div>
          <button class="icon-btn" type="button" aria-label="关闭确认弹窗" @click="resolveActionConfirm(false)">×</button>
        </header>
        <p>{{ actionConfirm.message }}</p>
        <ul v-if="actionConfirm.details.length">
          <li v-for="detail in actionConfirm.details" :key="detail">{{ detail }}</li>
        </ul>
        <footer>
          <button class="btn ghost" type="button" @click="resolveActionConfirm(false)">{{ actionConfirm.cancelLabel }}</button>
          <button class="btn" :class="actionConfirm.confirmClass" type="button" @click="resolveActionConfirm(true)">
            {{ actionConfirm.confirmLabel }}
          </button>
        </footer>
      </section>
    </div>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import AdminTools from "./components/AdminTools.vue";
import AuthPanels from "./components/AuthPanels.vue";
import DraftNoticeCard from "./components/DraftNoticeCard.vue";
import EngineerMopPage from "./components/EngineerMopPage.vue";
import HistoryMemoryPage from "./components/HistoryMemoryPage.vue";
import OngoingNoticeCard from "./components/OngoingNoticeCard.vue";
import RecentUndoPanel from "./components/RecentUndoPanel.vue";
import ScopeHome from "./components/ScopeHome.vue";
import SignaturePage from "./components/SignaturePage.vue";
import TargetRecordSelectionModal from "./components/TargetRecordSelectionModal.vue";
import VirtualNoticeList, { type NoticeRow } from "./components/VirtualNoticeList.vue";
import { AUTH_EXPIRED_EVENT, requestBinaryJson, requestJson } from "./api/client";
import { filterCandidatesBySearch } from "./candidateSearch";
import {
  loadDraftStorage,
  loadManualRecentTypesFromStorage,
  loadManualTemplateMemoryFromStorage,
  saveDraftStorage,
  saveManualRecentTypeToStorage,
  saveManualTemplateMemoryToStorage,
} from "./draftStorage";
import { backendJobStatusPatch, friendlyFailureText, terminalPhase } from "./jobStatus";
import {
  isKnownWorkType,
  manualPrefillWorkTypes,
  normalizeWorkType,
  noticeDurationError,
  noticeFieldLabel,
  noticeTemplate,
  noticeTypeKeywordRules,
  workTypeLabel,
  workTypes,
} from "./noticeTemplates";
import {
  inferBuildingText,
  parseSections,
  parsedActionFromStatus,
  parsedActionLabel,
  pastedNoticeStatus,
  pastedNoticeWorkType,
  rawSectionValue,
  sectionValue,
  splitNoticeTimeRange,
  toDatetimeLocal,
} from "./noticeParsing";
import { createCrossTabStreamCoordinator, type CrossTabStreamCoordinator } from "./streamCoordinator";

type Dict = Record<string, any>;
type ScopeOption = { value: string; label: string };
type ActionConfirmTone = "danger" | "warning" | "primary";
type ActionConfirmState = {
  open: boolean;
  tone: ActionConfirmTone;
  kicker: string;
  title: string;
  message: string;
  details: string[];
  confirmLabel: string;
  cancelLabel: string;
  confirmClass: string;
  resolve?: (confirmed: boolean) => void;
};
const brandLogoSrc = "/assets/vnet-logo.png";
const buildingScopeCodes = ["110", "A", "B", "C", "D", "E", "H"];
const maintenanceCycleOptions = ["/", "每月", "每季", "每年", "半年", "每两年", "每三年", "每五年", "冬季保温每日"];
const nonPlanTitleSuffix = "（非计划性）";
const authKeepaliveMs = 30 * 60 * 1000;
const authKeepaliveRetryMs = 3 * 60 * 1000;
const manualRefreshCooldownMs = 1500;
const workbenchRetryDelaysMs = [3000, 8000, 20000];
const streamLeaderHeartbeatMs = 5000;
const streamLeaderTtlMs = 15000;
const requestableScopes: ScopeOption[] = [
  { value: "110", label: "110站" },
  { value: "A", label: "A楼" },
  { value: "B", label: "B楼" },
  { value: "C", label: "C楼" },
  { value: "D", label: "D楼" },
  { value: "E", label: "E楼" },
  { value: "H", label: "H楼" },
  { value: "CAMPUS", label: "园区" },
];

const placeholderFieldValues = new Set(["", "-", "--", "—", "——", "/", "无", "暂无"]);
const repairSpecialtyOptionFallback: Record<string, string> = {
  optAssNaw3: "电气",
  opt3jdJJb7: "暖通",
  opt509Mxgr: "弱电",
  optyLRPdQS: "消防",
};

const authChecking = ref(true);
const loading = ref(false);
const isHistoryMemoryPage = ref(window.location.pathname.replace(/\/$/, "") === "/admin/history-memory");
const isEngineerMopPage = ref(window.location.pathname.replace(/\/$/, "") === "/engineer/mop");
const isSignaturePage = ref(window.location.pathname.replace(/\/$/, "") === "/signature");
const signatureLinkMode = computed(() => (
  isSignaturePage.value && Boolean(new URLSearchParams(window.location.search).get("record_id"))
));
const repairRefreshing = ref(false);
const changeRefreshing = ref(false);
const isWorkbench = ref(false);
const currentScope = ref(new URLSearchParams(window.location.search).get("scope") || "");
const syncText = ref("准备中");
const workType = ref("maintenance");
const userSelectedWorkType = ref(false);
const searchText = ref("");
const specialtyFilter = ref("");
const activeDraftKey = ref("");
const activeOngoingKey = ref("");
const showPasteParser = ref(false);
const showManualTypePicker = ref(false);
const showMemoryImporter = ref(false);
const showAdminTools = ref(false);
const showPermissionRequestPanel = ref(false);
const undoFilter = ref("all");
const pasteText = ref("");
const pasteParseBusy = ref(false);
const pasteParseLine = ref("粘贴通告后解析。");
const pasteParseStatus = ref("");
const pendingChangeTargetSelection = ref<Dict | null>(null);
const selectedChangeTargetId = ref("");
const hoveredChangeTargetId = ref("");
const selectedChangeSourceId = ref("");
const changeTargetSearchText = ref("");
const changeSourceSearchText = ref("");
const changeTargetConfirming = ref(false);
const ongoingBindSelection = ref<Dict | null>(null);
const selectedOngoingBindId = ref("");
const hoveredOngoingBindId = ref("");
const typeOverrideBusyKey = ref("");
const memoryImportText = ref("");
const memoryImportBusy = ref(false);
const memoryImportLine = ref("粘贴历史通告后导入。");
const memoryImportLineType = ref("");
const eventSource = ref<EventSource | null>(null);
const sseConnected = ref(false);
const sharedJobStreamAvailable = ref(false);
const activeItemsEventSource = ref<EventSource | null>(null);
const activeItemsConnected = ref(false);
const sharedActiveItemsStreamAvailable = ref(false);
const activeItemsUpdatePending = ref(false);
const pageVisible = ref(typeof document === "undefined" ? true : !document.hidden);
const pendingHiddenRefresh = ref(false);
const draftSavedAt = ref(0);
const draftSaveFailed = ref(false);

const auth = reactive({
  loggedIn: false,
  user: {} as Dict,
  scopeOptions: [] as ScopeOption[],
  loginUrl: "/api/auth/login",
});
const permissionRequest = reactive({
  scopes: [] as string[],
  reason: "",
  code: "",
  requestId: "",
  message: "",
});
const permissionBusy = ref(false);
const refreshCooldown = reactive<Record<string, boolean>>({
  workbench: false,
  repair: false,
  change: false,
});
const backendStatus = reactive({
  offline: false,
  message: "",
  lastErrorAt: 0,
});

const records = ref<Dict[]>([]);
const ongoing = ref<Dict[]>([]);
const zhihangRecords = ref<Dict[]>([]);
const dailySummary = ref<Dict>({ date: "", items: [], stats: {} });
const availableUndoItems = ref<Dict[]>([]);
const scopeOverview = ref<Record<string, Dict>>({});
const handoverLinks = ref<Record<string, string>>({});
const selectedKeys = reactive(new Set<string>());
const drafts = reactive(new Map<string, Dict>());
const ongoingEdits = reactive(new Map<string, Dict>());
const jobStates = reactive(new Map<string, Dict>());
const defaults = reactive({ impact: "无", progress: "" });
const localSummaryAdjustments = reactive({ started: 0, updated: 0, ended: 0, ongoing: 0 });
const actionConfirm = reactive<ActionConfirmState>({
  open: false,
  tone: "primary",
  kicker: "操作确认",
  title: "",
  message: "",
  details: [],
  confirmLabel: "确认",
  cancelLabel: "取消",
  confirmClass: "blue",
});
const draftStackRef = ref<HTMLElement | null>(null);
const fallbackPollTimers = new Map<string, number>();
const pollingJobs = new Map<string, string>();
let batchPollTimer: number | null = null;
let batchPollActive = false;
const MAX_SITE_PHOTO_COUNT = 6;
const MAX_SITE_PHOTO_BYTES = 8 * 1024 * 1024;
const recentUndoSeconds = 3 * 24 * 60 * 60;
const staleSourceSnapshotSeconds = 2 * 60 * 60;
const manualRecentTypes = ref<string[]>([]);
const previewDraftKey = ref("");
const pendingFocusNotice = ref<Dict | null>(null);
let workbenchLoadSeq = 0;
let workbenchRefreshTimer: number | null = null;
let sseReconnectTimer: number | null = null;
let activeItemsReconnectTimer: number | null = null;
let authKeepaliveTimer: number | null = null;
let workbenchRetryTimer: number | null = null;
let workbenchRetryAttempt = 0;
const refreshCooldownTimers = new Map<string, number>();
let lastActiveItemsSignature = "";
let activeItemsStreamScope = "";
let appDisposed = false;
let resolveOngoingBindSelection: ((candidate: Dict | null) => void) | null = null;
let jobStreamCoordinator: CrossTabStreamCoordinator<Dict> | null = null;
let activeItemsStreamCoordinator: CrossTabStreamCoordinator<Dict> | null = null;

const visibleScopeOptions = computed(() => auth.scopeOptions.length ? auth.scopeOptions : requestableScopes);
const isAdmin = computed(() => String(auth.user?.role || "").toLowerCase() === "admin");
const ownedScopeValues = computed(() => new Set(auth.scopeOptions.map((item) => normalizeScopeValue(item.value))));
const additionalRequestableScopes = computed(() => {
  if (!auth.loggedIn || !auth.scopeOptions.length) return requestableScopes;
  const owned = ownedScopeValues.value;
  return requestableScopes.filter((item) => !owned.has(normalizeScopeValue(item.value)));
});
const permissionPanelRequestableScopes = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? additionalRequestableScopes.value
    : requestableScopes
));
const permissionPanelTitle = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "申请其他楼栋权限"
    : "当前账号暂无门户权限"
));
const permissionPanelDescription = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "选择还需要访问的楼栋或园区，管理员确认验证码后会追加到当前账号。"
    : "请选择需要访问的楼栋或园区，提交后由管理员发放验证码。"
));
const permissionPanelEmptyText = computed(() => (
  showPermissionRequestPanel.value && auth.scopeOptions.length
    ? "当前账号已经拥有全部可申请入口。"
    : "当前没有可申请的楼栋权限。"
));
const headerSubtitle = computed(() => {
  if (isHistoryMemoryPage.value) return "管理工具 · 历史通告记忆导入";
  if (isEngineerMopPage.value) return `${scopeLabel(currentScope.value)} · 工程师 MOP 填写`;
  if (isSignaturePage.value) return "线上签名 · 手机手写保存";
  if (authChecking.value) return "功能选择 · 正在检查登录";
  if (!auth.loggedIn) return "功能选择 · 请先登录";
  if (!auth.scopeOptions.length) return "功能选择 · 申请访问权限";
  if (isWorkbench.value) return `${scopeLabel(currentScope.value)} · 通告工作台`;
  return "功能选择 · 请选择功能";
});
const pageStatusText = computed(() => {
  if (signatureLinkMode.value) return "";
  const text = String(syncText.value || "").trim();
  if (!text || ["准备中", "请选择功能", "切换中"].includes(text)) return "";
  if (/^HTTP\s+\d+/i.test(text)) return "后端服务暂未就绪，页面会在连接恢复后自动刷新。";
  return text;
});
const hasActiveJobs = computed(() => {
  for (const state of jobStates.values()) {
    const phase = String(state?.phase || "");
    if (phase && !terminalPhase(phase)) return true;
  }
  return false;
});
const jobRealtimeUnavailable = computed(() => (
  isWorkbench.value
  && hasActiveJobs.value
  && !(sseConnected.value || sharedJobStreamAvailable.value)
));
const connectionNotice = computed(() => {
  if (!auth.loggedIn) return null;
  if (backendStatus.offline) {
    return {
      tone: "failed",
      text: backendStatus.message || "后端连接异常，页面会保留当前数据。",
      actionLabel: "重新连接",
      action: retryFrontendConnections,
    };
  }
  if (pendingHiddenRefresh.value) {
    return {
      tone: "info",
      text: "后台有新数据，页面恢复可见后会自动刷新。",
      actionLabel: "立即刷新",
      action: flushPendingHiddenRefresh,
    };
  }
  if (jobRealtimeUnavailable.value) {
    return {
      tone: "warning",
      text: "任务实时状态正在重连，当前会自动轮询查询。",
      actionLabel: "重连",
      action: retryFrontendConnections,
    };
  }
  return null;
});
const dailyStats = computed(() => dailySummary.value?.stats || {});
const closedSummaryItems = computed(() => {
  const items = Array.isArray(dailySummary.value?.items) ? dailySummary.value.items : [];
  return items.filter((item: Dict) => String(item?.status || "") === "已结束" || Boolean(item?.ended_at));
});
const recentUndoItems = computed(() => [...availableUndoItems.value].sort((a, b) => Number(b.undo_created_at || 0) - Number(a.undo_created_at || 0)));
const liveDailyStats = computed(() => ({
  ...dailyStats.value,
  started: Math.max(0, Number(dailyStats.value.started || 0) + localSummaryAdjustments.started),
  updated: Math.max(0, Number(dailyStats.value.updated || 0) + localSummaryAdjustments.updated),
  ended: Math.max(0, Number(dailyStats.value.ended || 0) + localSummaryAdjustments.ended),
}));
const liveOngoingCount = computed(() => Math.max(0, ongoing.value.length + localSummaryAdjustments.ongoing));
const scopedRecords = computed(() => records.value.filter((record) => recordMatchesCurrentScope(record)));
const manualRecentTypeOptions = computed(() => manualRecentTypes.value
  .map((value) => workTypes.find((item) => item.value === value))
  .filter(Boolean)
  .slice(0, 2) as Array<{ value: string; label: string }>);
const draftSaveText = computed(() => {
  if (!selectedKeys.size && !drafts.size) return "";
  if (draftSaveFailed.value) return "草稿保存失败";
  if (!draftSavedAt.value) return "草稿自动保存";
  return `草稿已保存 ${formatTimeOfDay(draftSavedAt.value)}`;
});
const recordTypeCounts = computed(() => {
  const counts: Record<string, number> = Object.fromEntries(workTypes.map((item) => [item.value, 0]));
  for (const record of scopedRecords.value) {
    const type = record.work_type || "maintenance";
    if (Object.prototype.hasOwnProperty.call(counts, type)) counts[type] += 1;
  }
  return counts;
});
const filteredRecords = computed(() => {
  const query = searchText.value.trim().toLowerCase();
  return scopedRecords.value.filter((record) => {
    if ((record.work_type || "maintenance") !== workType.value) return false;
    if (!matchesSpecialtyFilter(specialtyForRecord(record))) return false;
    if (!query) return true;
    return [recordCardTitle(record), buildingForRecord(record), specialtyForRecord(record), sourceProgressForRecord(record)]
      .join(" ")
      .toLowerCase()
      .includes(query);
  });
});
const filteredRows = computed<NoticeRow[]>(() => filteredRecords.value.map((record) => ({
  id: recordKey(record),
  title: recordCardTitle(record),
  type: workTypeLabel(record.work_type),
  meta: [buildingForRecord(record), specialtyForRecord(record), sourceProgressForRecord(record)].filter(Boolean).join(" · "),
  status: recordStatusLabel(record),
  statusTone: recordStatusTone(record),
  selected: selectedKeys.has(recordKey(record)),
  disabled: isRecordOngoing(record),
  disabledReason: "已在进行中，请在右侧更新、结束或删除",
  raw: record,
})));
const selectedDraftRowsAll = computed(() => {
  const keys = Array.from(selectedKeys);
  const pinned = activeDraftKey.value;
  if (pinned && keys.includes(pinned)) {
    keys.splice(keys.indexOf(pinned), 1);
    keys.unshift(pinned);
  }
  return keys.map((key) => {
    const record = draftRecordForKey(key);
    if (!record) return null;
    return {
      key,
      record,
      draft: getDraft(record),
      title: recordCardTitle(record),
    };
  }).filter(Boolean) as Array<{ key: string; record: Dict; draft: Dict; title: string }>;
});
const selectedDraftRows = computed(() => selectedDraftRowsAll.value.filter((row) => matchesSpecialtyFilter(draftSpecialtyForRow(row))));
const filteredOngoing = computed(() => ongoing.value.filter((item) => matchesSpecialtyFilter(ongoingSpecialtyForItem(item))));
const specialtyFilterOptions = computed(() => {
  const values = new Set<string>();
  for (const record of scopedRecords.value) {
    const value = normalizeSpecialtyValue(specialtyForRecord(record));
    if (value) values.add(value);
  }
  for (const row of selectedDraftRowsAll.value) {
    const value = normalizeSpecialtyValue(draftSpecialtyForRow(row));
    if (value) values.add(value);
  }
  for (const item of ongoing.value) {
    const value = normalizeSpecialtyValue(ongoingSpecialtyForItem(item));
    if (value) values.add(value);
  }
  return Array.from(values).sort((a, b) => a.localeCompare(b, "zh-CN"));
});

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function normalizedRecordBuildingCodes(record: Dict): string[] {
  const raw = Array.isArray(record?.building_codes) ? record.building_codes : [];
  const codes: string[] = [];
  for (const item of raw) {
    const code = String(item || "").trim().toUpperCase();
    if (buildingScopeCodes.includes(code) && !codes.includes(code)) codes.push(code);
  }
  return buildingScopeCodes.filter((code) => codes.includes(code));
}

function recordMatchesCurrentScope(record: Dict): boolean {
  const scope = normalizeScopeValue(currentScope.value || "ALL");
  if (scope === "ALL") return true;
  const codes = normalizedRecordBuildingCodes(record);
  if (!codes.length) return true;
  if (scope === "CAMPUS") return codes.length >= 2;
  return codes.length === 1 && codes[0] === scope;
}

function scopeLabel(value: string): string {
  const normalized = normalizeScopeValue(value, "ALL");
  const found = [...visibleScopeOptions.value, { value: "ALL", label: "全部" }].find((item) => normalizeScopeValue(item.value, "") === normalized);
  return found?.label || normalized;
}

function defaultBuildingForCurrentScope(): string {
  const scope = normalizeScopeValue(currentScope.value || "", "");
  if (!scope || scope === "ALL") return "";
  return scopeLabel(scope);
}

function buildingCodesFromText(value: string): string[] {
  const code = normalizeScopeValue(value, "");
  if (buildingScopeCodes.includes(code)) return [code];
  if (code === "CAMPUS") return ["A", "B", "C"];
  return [];
}

function onDraftBuildingChange(draft: Dict): void {
  draft.building_codes = buildingCodesFromText(draft.building || "");
  saveDrafts();
}

function fieldsOf(record: Dict | undefined): Dict {
  return record?.display_fields || {};
}

function cleanDisplayText(value: any): string {
  const text = String(value ?? "").trim();
  if (placeholderFieldValues.has(text)) return "";
  if (/^opt[A-Za-z0-9]{6,}$/.test(text)) return "";
  return text;
}

function normalizeSpecialtyValue(value: any): string {
  return cleanDisplayText(value).replace(/\s+/g, "");
}

function matchesSpecialtyFilter(value: any): boolean {
  const expected = normalizeSpecialtyValue(specialtyFilter.value);
  if (!expected) return true;
  return normalizeSpecialtyValue(value) === expected;
}

function draftSpecialtyForRow(row: { record: Dict; draft: Dict }): string {
  return cleanDisplayText(row.draft?.specialty) || specialtyForRecord(row.record);
}

function ongoingSpecialtyForItem(item: Dict): string {
  const edit = ongoingEdits.get(ongoingLineKey(item));
  return cleanDisplayText(edit?.specialty) || cleanDisplayText(item.specialty);
}

function repairSpecialtyForRecord(record: Dict): string {
  const fields = fieldsOf(record);
  const direct = cleanDisplayText(fields["所属专业"]);
  if (direct) return direct;
  const pushedRaw = String(fields["专业（推送消息用）"] ?? "").trim();
  return repairSpecialtyOptionFallback[pushedRaw] || cleanDisplayText(pushedRaw);
}

function todayInput(hour: number, minute: number): string {
  const d = new Date();
  d.setHours(hour, minute, 0, 0);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function firstRepairField(record: Dict, names: string[]): string {
  const fields = fieldsOf(record);
  for (const name of names) {
    const value = cleanDisplayText(fields[name]);
    if (value) return value;
  }
  return "";
}

function repairLevelFromEventLevel(value: string): string {
  const text = String(value || "").trim().toUpperCase();
  if (/(^|[^A-Z0-9])I3([^A-Z0-9]|$)/.test(text) || text === "低") return "低";
  if (/(^|[^A-Z0-9])I2([^A-Z0-9]|$)/.test(text) || text === "中") return "中";
  return "";
}

function repairDeviceText(record: Dict): string {
  const f = fieldsOf(record);
  const no = String(f["设备编号"] || "").trim();
  const name = String(f["设备名称"] || "").trim();
  return no && name ? `${no}${name}` : no || name || firstRepairField(record, ["维修设备", "资产名称", "设备"]);
}

function titleForRecord(record: Dict): string {
  if (record.manual) return fieldsOf(record)["手动标题"] || record.title || `手动${workTypeLabel(record.work_type)}通告`;
  const f = fieldsOf(record);
  const type = record.work_type || "maintenance";
  if (type === "change") return f["变更简述"] || record.title || record.record_id;
  if (type === "repair") return record.title || f["检修通告名称"] || f["维修名称"] || record.record_id;
  return `EA118机房${f["楼栋"] || ""}${f["维护总项"] || ""}`;
}

function recordCardTitle(record: Dict): string {
  const title = titleForRecord(record);
  if ((record.work_type || "maintenance") !== "maintenance") return title;
  const period = String(fieldsOf(record)["维护周期"] || record.maintenance_cycle || "").trim();
  return period ? `${title}-${period}` : title;
}

function specialtyForRecord(record: Dict): string {
  const f = fieldsOf(record);
  if (record.manual) return cleanDisplayText(f["专业类别"]) || cleanDisplayText(f["专业"]) || cleanDisplayText(f["所属专业"]) || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return cleanDisplayText(f["专业"]);
  if (type === "repair") return repairSpecialtyForRecord(record);
  return cleanDisplayText(f["专业类别"]);
}

function stripNonPlanTitleSuffix(title: string): string {
  return String(title || "").trim().replace(/（非计划性）$|（非计划）$/u, "").trim();
}

function appendNonPlanTitleSuffix(title: string, enabled: boolean): string {
  const value = String(title || "").trim();
  if (!enabled || !value) return value;
  return `${stripNonPlanTitleSuffix(value)}${nonPlanTitleSuffix}`;
}

function manualDraftTitle(draft: Dict, type: string): string {
  const baseTitle = String(draft.title || "").trim();
  const supplement = String(draft.content || "").trim();
  const title = type === "repair"
    ? combineTitleSupplement(baseTitle, supplement)
    : (baseTitle || supplement);
  return type === "maintenance" ? appendNonPlanTitleSuffix(title, Boolean(draft.non_plan)) : title;
}

function combineTitleSupplement(title: string, supplement: string): string {
  const base = String(title || "").trim();
  const extra = String(supplement || "").trim();
  if (!base) return extra;
  if (!extra || base.includes(extra)) return base;
  return `${base}${extra}`;
}

function payloadTitleForDraft(record: Dict, draft: Dict, type: string): string {
  if (type === "repair") {
    const baseTitle = String(draft.title || "").trim();
    if (baseTitle) return baseTitle;
    if (!record.manual) return titleForRecord(record);
    return String(draft.content || "").trim();
  }
  return record.manual ? manualDraftTitle(draft, type) : titleForRecord(record);
}

function inferManualNoticeWorkType(draft: Dict, fallback = "maintenance"): string {
  return normalizeWorkType(fallback);
}

function draftWorkType(record: Dict, draft: Dict): string {
  return record.manual
    ? inferManualNoticeWorkType(draft, draft.work_type || record.work_type || "maintenance")
    : String(record.work_type || "maintenance");
}

function buildingForRecord(record: Dict): string {
  const f = fieldsOf(record);
  if (record.manual) return f["楼栋"] || f["变更楼栋"] || f["所属数据中心/楼栋-使用"] || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return f["变更楼栋"] || "";
  if (type === "repair") return f["所属数据中心/楼栋-使用"] || f["所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"] || "";
  return f["楼栋"] || "";
}

function levelForRecord(record: Dict): string {
  if ((record.work_type || "maintenance") === "repair") return repairLevelFromEventLevel(firstRepairField(record, ["对应事件等级"]));
  return fieldsOf(record)["变更等级（阿里）"] || "";
}

function sourceProgressForRecord(record: Dict): string {
  if (record.manual) return "未开始";
  const type = record.work_type || "maintenance";
  if (record.source_progress || record.source_status) return record.source_progress || record.source_status;
  if (type === "change") return fieldsOf(record)["变更进度"] || "";
  if (type === "repair") return firstRepairField(record, ["维修开始时间"]) ? "进行中" : "未开始";
  return fieldsOf(record)["维护实施状态"] || "";
}

function sourceActionForRecord(record: Dict): string {
  return sourceProgressForRecord(record) === "未开始" ? "start" : "update";
}

function sourceActionLabel(record: Dict): string {
  return sourceActionForRecord(record) === "start" ? "开始" : "更新";
}

function draftActionForRecord(record: Dict, draft: Dict): string {
  if (record?.manual) {
    const action = String(draft?.parsed_action || "").toLowerCase();
    if (["start", "update", "end"].includes(action)) return action;
  }
  return sourceActionForRecord(record);
}

function draftActionLabel(record: Dict, draft: Dict): string {
  const action = draftActionForRecord(record, draft);
  if (action === "end") return "结束";
  if (action === "update") return "更新";
  return "开始";
}

function recordStatusLabel(record: Dict): string {
  const key = recordKey(record);
  const job = jobStates.get(key);
  if (job?.phase && !terminalPhase(job.phase)) return "提交中";
  if (job?.phase === "failed") return "提交失败";
  if (job?.phase === "success") return "已提交";
  if (selectedKeys.has(key)) return "已加入待发起";
  if (isRecordOngoing(record)) return "进行中可更新 · 右侧处理";
  const progress = sourceProgressForRecord(record);
  if (!progress || progress === "未开始") return "待发起";
  return `${progress} · 可更新`;
}

function recordStatusTone(record: Dict): string {
  const key = recordKey(record);
  const job = jobStates.get(key);
  if (job?.phase && !terminalPhase(job.phase)) return "ongoing";
  if (job?.phase === "failed") return "failed";
  if (job?.phase === "success") return "queued";
  if (selectedKeys.has(key)) return "queued";
  if (isRecordOngoing(record)) return "ongoing";
  const progress = sourceProgressForRecord(record);
  if (!progress || progress === "未开始") return "pending";
  return "update";
}

function sourceWorkTypeForRecord(record: Dict): string {
  return String(record.source_work_type || record.converted_from_work_type || record.original_work_type || record.work_type || "maintenance").trim();
}

function canToggleWorkTypeOverride(record: Dict): boolean {
  if (!record || record.manual) return false;
  const sourceType = sourceWorkTypeForRecord(record);
  const displayType = String(record.work_type || "maintenance");
  return sourceType === "maintenance" && (displayType === "maintenance" || displayType === "change");
}

function targetOverrideWorkType(record: Dict): string {
  return String(record.work_type || "maintenance") === "change" && sourceWorkTypeForRecord(record) === "maintenance"
    ? "maintenance"
    : "change";
}

function workTypeOverrideButtonLabel(record: Dict): string {
  return targetOverrideWorkType(record) === "change" ? "转为变更" : "转为维保";
}

function targetRecordIdForRecord(record: Dict): string {
  const summary = record?.work_summary || {};
  return String(summary.target_record_id || summary.feishu_record_id || summary.raw_record_id || record?.target_record_id || record?.feishu_record_id || record?.raw_record_id || "").trim();
}

function targetRecordIdForOngoing(item: Dict): string {
  return String(item.target_record_id || item.feishu_record_id || item.raw_record_id || "").trim();
}

function ongoingLineKey(item: Dict): string {
  return String(item.active_item_id || item.target_record_id || item.feishu_record_id || item.raw_record_id || item.source_record_id || item.record_id || "").trim();
}

function ongoingBusinessTimeKey(item: Dict): string {
  const text = [item.start_time, item.time_str, item.time, item.end_time]
    .map((value) => String(value || ""))
    .join("");
  return Array.from(text.matchAll(/\d+/g))
    .map((match) => match[0].length <= 2 ? match[0].padStart(2, "0") : match[0])
    .join("");
}

function ongoingBusinessIdentityKeys(item: Dict, workType: string): string[] {
  const title = normalizeDraftSignatureText(String(item.title || item.content || item.name || ""));
  if (!title) return [];
  const building = normalizeDraftSignatureText(String(item.building || ""));
  const reason = normalizeDraftSignatureText(String(item.reason || ""));
  const cycle = workType === "maintenance" ? normalizeDraftSignatureText(String(item.maintenance_cycle || fieldsOf(item)["维护周期"] || "")) : "";
  const timeKey = ongoingBusinessTimeKey(item);
  const keys: string[] = [];
  if (building && reason) keys.push(`${workType}:business:title-building-reason:${building}:${title}:${cycle}:${reason}`);
  if (building && timeKey && reason) {
    keys.push(`${workType}:business:title-time-reason:${building}:${title}:${cycle}:${timeKey}:${reason}`);
  } else if (building && timeKey) {
    keys.push(`${workType}:business:title-time:${building}:${title}:${cycle}:${timeKey}`);
  }
  return keys;
}

function ongoingIdentityKeys(item: Dict): string[] {
  const workType = String(item.work_type || item.lan_work_type || "maintenance").trim();
  const noticeType = String(item.notice_type || "").trim();
  const target = String(item.target_record_id || item.feishu_record_id || item.raw_record_id || "").trim();
  const source = String(item.source_record_id || "").trim();
  const active = String(item.active_item_id || "").trim();
  const keys: string[] = [];
  if (target) keys.push(`${workType}:target:${target}`);
  if (source) keys.push(`${workType}:source:${source}`);
  if (active) keys.push(`${workType}:active:${active}`);
  const title = normalizeDraftSignatureText(String(item.title || item.content || ""));
  if (title) {
    const start = String(item.start_time || item.time_str || item.time || "").slice(0, 16);
    const end = String(item.end_time || "").slice(0, 16);
    const reason = normalizeDraftSignatureText(String(item.reason || ""));
    const building = normalizeDraftSignatureText(String(item.building || ""));
    keys.push(`${workType}:fallback:${noticeType}:${building}:${title}:${start}:${end}:${reason}`);
  }
  keys.push(...ongoingBusinessIdentityKeys(item, workType));
  return Array.from(new Set(keys.filter(Boolean)));
}

function ongoingCompletenessScore(item: Dict): number {
  let score = 0;
  if (String(item.target_record_id || item.feishu_record_id || item.raw_record_id || "").trim()) score += 12;
  if (String(item.source_record_id || "").trim()) score += 8;
  if (String(item.active_item_id || "").trim()) score += 4;
  for (const field of ["title", "building", "specialty", "maintenance_cycle", "start_time", "end_time", "location", "content", "reason", "impact", "progress", "text"]) {
    if (String(item[field] || "").trim()) score += 1;
  }
  if (Array.isArray(item.extra_images) && item.extra_images.length) score += 1;
  return score;
}

function mergeOngoingItem(existing: Dict, incoming: Dict): Dict {
  const primary = ongoingCompletenessScore(incoming) >= ongoingCompletenessScore(existing) ? incoming : existing;
  const supplement = primary === incoming ? existing : incoming;
  const merged: Dict = { ...primary };
  for (const [key, value] of Object.entries(supplement || {})) {
    const current = merged[key];
    if (current === undefined || current === null || current === "" || (Array.isArray(current) && current.length === 0)) {
      merged[key] = value;
    }
  }
  return merged;
}

function dedupeOngoingItems(items: Dict[]): Dict[] {
  const result: Dict[] = [];
  const indexByKey = new Map<string, number>();
  for (const raw of items || []) {
    if (!raw || typeof raw !== "object") continue;
    const item = { ...raw };
    const keys = ongoingIdentityKeys(item);
    let matchIndex = -1;
    for (const key of keys) {
      const index = indexByKey.get(key);
      if (index !== undefined) {
        matchIndex = index;
        break;
      }
    }
    if (matchIndex < 0) {
      matchIndex = result.length;
      result.push(item);
    } else {
      result[matchIndex] = mergeOngoingItem(result[matchIndex], item);
    }
    for (const key of [...keys, ...ongoingIdentityKeys(result[matchIndex])]) {
      indexByKey.set(key, matchIndex);
    }
  }
  return result;
}

function isOngoingExpanded(item: Dict): boolean {
  const key = ongoingLineKey(item);
  return Boolean(key && activeOngoingKey.value === key);
}

function expandOngoingCard(item: Dict): void {
  const key = ongoingLineKey(item);
  if (key) activeOngoingKey.value = key;
}

function toggleOngoingCard(item: Dict): void {
  const key = ongoingLineKey(item);
  if (!key) return;
  activeOngoingKey.value = activeOngoingKey.value === key ? "" : key;
}

function closedLineKey(item: Dict): string {
  return `closed:${item.key || item.active_item_id || item.target_record_id || item.feishu_record_id || item.title || ""}`;
}

function undoLineKey(item: Dict): string {
  return `undo:${item.undo_id || item.active_item_id || item.target_record_id || item.record_id || item.key || item.title || ""}`;
}

function sourceRecordIdForOngoing(item: Dict, targetRecordId = ""): string {
  const source = String(item.source_record_id || "").trim();
  if (source && source !== targetRecordId) return source;
  return "";
}

function ongoingNeedsBinding(item: Dict): boolean {
  const edit = ongoingEdits.get(ongoingLineKey(item));
  const editedTarget = String(edit?.target_record_id || "").trim();
  if (editedTarget) return false;
  return String(item?.binding_status || "") === "needs_binding" || !targetRecordIdForOngoing(item);
}

function ongoingTimeRange(item: Dict): { start: string; end: string } {
  const timeText = String(item.time_str || item.time || "").trim();
  const parts = timeText.split(/~|至|到/).map((part) => part.trim()).filter(Boolean);
  const isRepair = (item.work_type || "maintenance") === "repair";
  const start =
    toDatetimeLocal(isRepair ? (item.expected_time || item.start_time) : item.start_time) ||
    toDatetimeLocal(parts[0] || "") ||
    todayInput(isRepair ? 23 : 9, isRepair ? 50 : 30);
  const end =
    toDatetimeLocal(isRepair ? (item.fault_time || item.end_time) : item.end_time) ||
    toDatetimeLocal(parts[1] || "") ||
    todayInput(isRepair ? 0 : 18, isRepair ? 0 : 30);
  return { start, end };
}

function maintenanceCycleForRecord(record: Dict): string {
  if ((record.work_type || "maintenance") !== "maintenance") return "";
  return normalizeDraftSignatureText(String(fieldsOf(record)["维护周期"] || record.maintenance_cycle || ""));
}

function maintenanceCycleForOngoing(item: Dict): string {
  if ((item.work_type || "maintenance") !== "maintenance") return "";
  return normalizeDraftSignatureText(String(item.maintenance_cycle || fieldsOf(item)["维护周期"] || ""));
}

function isRecordOngoing(record: Dict): boolean {
  const titleCandidates = [
    titleForRecord(record),
    recordCardTitle(record),
  ].map((value) => normalizeDraftSignatureText(value)).filter(Boolean);
  const sourceId = record.source_record_id || record.record_id;
  const targetId = targetRecordIdForRecord(record);
  const recordCycle = maintenanceCycleForRecord(record);
  return ongoing.value.some((item) => {
    if ((item.work_type || "maintenance") !== (record.work_type || "maintenance")) return false;
    const itemSource = String(item.source_record_id || "").trim();
    const itemTarget = targetRecordIdForOngoing(item);
    const itemRecordId = String(item.record_id || "").trim();
    if (sourceId && (itemSource === sourceId || itemRecordId === sourceId)) return true;
    if (targetId && (itemTarget === targetId || itemRecordId === targetId)) return true;
    const ongoingTitles = [
      ongoingTitle(item),
      item.title || "",
      item.content || "",
    ].map((value) => normalizeDraftSignatureText(value)).filter(Boolean);
    const itemCycle = maintenanceCycleForOngoing(item);
    if ((record.work_type || "maintenance") === "maintenance" && (recordCycle || itemCycle) && recordCycle !== itemCycle) {
      return false;
    }
    return titleCandidates.some((title) => ongoingTitles.includes(title));
  });
}

function isManualKey(key: string): boolean {
  return key.startsWith("manual:");
}

function recordKey(record: Dict): string {
  return record?.manual_key || `${record.work_type || "maintenance"}:${record.record_id}`;
}

function draftRecordForKey(key: string): Dict | null {
  const record = records.value.find((item) => recordKey(item) === key);
  if (record) return record;
  const draft = drafts.get(key);
  if (isManualKey(key) && draft) return manualRecordFromDraft(key, draft);
  return null;
}

function manualDraftDefaults(type: string): Dict {
  const building = defaultBuildingForCurrentScope();
  const normalizedType = normalizeWorkType(type);
  return {
    manual: true,
    work_type: normalizedType,
    title: "",
    building,
    building_codes: buildingCodesFromText(building),
    specialty: "",
    level: normalizedType === "change" ? "I3" : "",
    maintenance_cycle: "",
    non_plan: false,
    start_time: "",
    end_time: "",
    location: "",
    content: "",
    reason: "",
    impact: "",
    progress: "",
    zhihang_involved: false,
    zhihang_record_id: "",
    zhihang_title: "",
    zhihang_progress: "",
    repair_device: "",
    repair_fault: "",
    fault_type: "",
    repair_mode: "",
    discovery: "",
    symptom: "",
    solution: "",
    spare_parts: "",
    device: "",
    cabinet: "",
    quantity: "",
  };
}

function manualRecordFromDraft(key: string, draft: Dict): Dict {
  const type = inferManualNoticeWorkType(draft, draft.work_type || "maintenance");
  if (draft.work_type !== type) draft.work_type = type;
  const title = manualDraftTitle(draft, type);
  const noticeTypeMap: Record<string, string> = {
    maintenance: "维保通告",
    change: "设备变更",
    repair: "设备检修",
    power: "上下电通告",
    polling: "设备轮巡",
    adjust: "设备调整",
  };
  const buildingCodes = Array.isArray(draft.building_codes) && draft.building_codes.length
    ? draft.building_codes
    : buildingCodesFromText(draft.building || "");
  return {
    manual: true,
    manual_key: key,
    record_id: key,
    source_record_id: draft.source_record_id || "",
    work_type: type,
    notice_type: noticeTypeMap[type] || "维保通告",
    title: title || `手动${workTypeLabel(type)}通告`,
    display_fields: {
      "手动标题": title,
      "楼栋": draft.building || "",
      "变更楼栋": draft.building || "",
      "所属数据中心/楼栋-使用": draft.building || "",
      "专业类别": draft.specialty || "",
      "专业": draft.specialty || "",
      "所属专业": draft.specialty || "",
      "维护周期": draft.maintenance_cycle || "",
      "非计划性": draft.non_plan ? "是" : "",
      "设备": draft.device || "",
      "柜号": draft.cabinet || "",
      "数量": draft.quantity || "",
      "备件更换情况": draft.spare_parts || "",
    },
    target_record_id: draft.target_record_id || draft.feishu_record_id || draft.raw_record_id || "",
    building_codes: buildingCodes,
  };
}

function onManualDraftTypeChange(draft: Dict): void {
  const type = normalizeWorkType(draft.work_type);
  draft.work_type = type;
  if (type === "change" && !draft.level) draft.level = "I3";
  if (type !== "maintenance") draft.non_plan = false;
  saveDrafts();
}

function repairDraftDefaults(record: Dict): Dict {
  const memory = record.memory || {};
  return {
    start_time: todayInput(23, 50),
    end_time: toDatetimeLocal(firstRepairField(record, ["故障发生时间", "发现故障时间"])) || "",
    title: memory.title || titleForRecord(record),
    location: memory.location || "",
    content: memory.content || firstRepairField(record, ["标题/补充内容", "标题补充内容"]),
    level: memory.level || levelForRecord(record),
    specialty: cleanDisplayText(memory.specialty) || specialtyForRecord(record),
    reason: memory.reason || firstRepairField(record, ["故障原因", "故障维修原因"]),
    impact: memory.impact || "",
    progress: memory.progress || "",
    repair_device: memory.repair_device || repairDeviceText(record),
    repair_fault: memory.repair_fault || firstRepairField(record, ["维修故障", "故障维修原因"]),
    fault_type: memory.fault_type || firstRepairField(record, ["故障类型"]) || "设备故障",
    repair_mode: memory.repair_mode || firstRepairField(record, ["维修方式", "维修方", "供应商名称"]),
    discovery: cleanDisplayText(memory.discovery) || firstRepairField(record, ["对应来源"]),
    symptom: memory.symptom || firstRepairField(record, ["故障发生现象描述", "故障现象"]),
    solution: memory.solution || firstRepairField(record, ["解决方案", "维修方案", "后续整改措施"]),
    spare_parts: memory.spare_parts || firstRepairField(record, ["备件更换情况", "备件使用情况"]),
  };
}

function rememberedZhihang(memory: Dict): Dict {
  const rememberedId = String(memory.zhihang_record_id || "").trim();
  if (!rememberedId) return {};
  const item = zhihangRecords.value.find((record) => record.record_id === rememberedId);
  if (!item) return {};
  return {
    zhihang_involved: true,
    zhihang_record_id: rememberedId,
    zhihang_title: item.title || memory.zhihang_title || "",
    zhihang_progress: item.progress || memory.zhihang_progress || "",
  };
}

function getDraft(record: Dict): Dict {
  const key = recordKey(record);
  if (!drafts.has(key)) {
    if (record.manual) {
      drafts.set(key, manualDraftDefaults(record.work_type));
    } else if ((record.work_type || "maintenance") === "repair") {
      drafts.set(key, repairDraftDefaults(record));
    } else {
      const f = fieldsOf(record);
      const memory = record.memory || {};
      const isChange = (record.work_type || "maintenance") === "change";
      const zhihangMemory = isChange ? rememberedZhihang(memory) : {};
      drafts.set(key, {
        title: titleForRecord(record),
        specialty: cleanDisplayText(memory.specialty) || specialtyForRecord(record),
        level: memory.level || levelForRecord(record) || (isChange ? "I3" : ""),
        maintenance_cycle: f["维护周期"] || "",
        start_time: isChange
          ? (toDatetimeLocal(f["变更开始日期（阿里）"] || f["计划开始日期（阿里）"] || f["计划开始"] || f["计划开始时间"] || f["计划延迟开始日期"]) || todayInput(9, 30))
          : todayInput(9, 30),
        end_time: isChange
          ? (toDatetimeLocal(f["变更结束日期（阿里）"] || f["计划结束日期（阿里）"] || f["计划结束"] || f["计划结束时间"] || f["计划延迟结束日期"]) || todayInput(18, 30))
          : todayInput(18, 30),
        location: memory.location || "",
        content: isChange ? (memory.content || titleForRecord(record)) : (memory.content || ""),
        reason: memory.reason || "",
        impact: memory.impact || defaults.impact,
        progress: memory.progress || defaults.progress,
        zhihang_involved: Boolean(zhihangMemory.zhihang_involved),
        zhihang_record_id: zhihangMemory.zhihang_record_id || "",
        zhihang_title: zhihangMemory.zhihang_title || "",
        zhihang_progress: zhihangMemory.zhihang_progress || "",
      });
    }
    saveDrafts();
  }
  return drafts.get(key) || {};
}

function currentOpenId(): string {
  return String(auth.user?.open_id || auth.user?.openid || "anonymous").trim() || "anonymous";
}

function currentDraftScope(): string {
  return currentScope.value || "ALL";
}

function loadManualRecentTypes(): void {
  manualRecentTypes.value = loadManualRecentTypesFromStorage(currentOpenId(), currentDraftScope());
}

function saveManualRecentType(type: string): void {
  manualRecentTypes.value = saveManualRecentTypeToStorage(
    currentOpenId(),
    currentDraftScope(),
    type,
    manualRecentTypes.value,
  );
}

function loadManualTemplateMemory(type: string): Dict | null {
  return loadManualTemplateMemoryFromStorage(currentOpenId(), currentDraftScope(), type);
}

function saveManualTemplateMemory(type: string, draft: Dict): void {
  saveManualTemplateMemoryToStorage(currentOpenId(), currentDraftScope(), type, draft);
}

function loadDrafts(): void {
  loadManualRecentTypes();
  const payload = loadDraftStorage(currentOpenId(), currentDraftScope());
  selectedKeys.clear();
  for (const key of payload.selected) selectedKeys.add(key);
  drafts.clear();
  for (const [key, value] of Object.entries(payload.drafts)) drafts.set(key, value);
}

function saveDrafts(): void {
  const payload: Record<string, Dict> = {};
  for (const [key, value] of drafts.entries()) payload[key] = value;
  if (saveDraftStorage(currentOpenId(), currentDraftScope(), Array.from(selectedKeys), payload)) {
    draftSaveFailed.value = false;
    draftSavedAt.value = Date.now();
  } else {
    draftSaveFailed.value = true;
    syncText.value = "草稿保存失败，请减少待发起通告数量后重试";
  }
}

function setDraftField(draft: Dict, field: string, value: unknown): void {
  draft[field] = value;
  saveDrafts();
}

function formatTimeOfDay(timestamp: number): string {
  if (!timestamp) return "";
  const date = new Date(timestamp);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function startRefreshCooldown(key: keyof typeof refreshCooldown): void {
  refreshCooldown[key] = true;
  const existing = refreshCooldownTimers.get(key);
  if (existing) window.clearTimeout(existing);
  const timer = window.setTimeout(() => {
    refreshCooldown[key] = false;
    refreshCooldownTimers.delete(key);
  }, manualRefreshCooldownMs);
  refreshCooldownTimers.set(key, timer);
}

function clearWorkbenchRetry(): void {
  if (workbenchRetryTimer !== null) {
    window.clearTimeout(workbenchRetryTimer);
    workbenchRetryTimer = null;
  }
  workbenchRetryAttempt = 0;
}

function scheduleWorkbenchRetry(reason = "加载失败"): void {
  if (appDisposed || !auth.loggedIn || !isWorkbench.value || !currentScope.value) return;
  if (workbenchRetryTimer !== null) return;
  if (!pageVisible.value) {
    pendingHiddenRefresh.value = true;
    syncText.value = "加载失败，页面恢复可见后自动重试";
    return;
  }
  const index = Math.min(workbenchRetryAttempt, workbenchRetryDelaysMs.length - 1);
  const delay = workbenchRetryDelaysMs[index];
  workbenchRetryAttempt += 1;
  syncText.value = `${reason}，${Math.round(delay / 1000)}秒后重试`;
  workbenchRetryTimer = window.setTimeout(() => {
    workbenchRetryTimer = null;
    if (appDisposed || !auth.loggedIn || !isWorkbench.value) return;
    void loadWorkbench();
  }, delay);
}

function clearAuthKeepalive(): void {
  if (authKeepaliveTimer !== null) {
    window.clearTimeout(authKeepaliveTimer);
    authKeepaliveTimer = null;
  }
}

function closeDirectJobEventSource(): void {
  if (eventSource.value) eventSource.value.close();
  eventSource.value = null;
  if (sseReconnectTimer !== null) {
    window.clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }
}

function stopJobSse(): void {
  if (jobStreamCoordinator) {
    jobStreamCoordinator.stop();
    jobStreamCoordinator = null;
  }
  closeDirectJobEventSource();
  sseConnected.value = false;
  sharedJobStreamAvailable.value = false;
}

function stopRealtimeConnections(): void {
  stopJobSse();
  stopActiveItemsSse();
}

function pauseJobStatusChecksForHiddenPage(): void {
  for (const timer of fallbackPollTimers.values()) window.clearTimeout(timer);
  fallbackPollTimers.clear();
  clearOngoingEdits();
  if (batchPollTimer !== null) {
    window.clearTimeout(batchPollTimer);
    batchPollTimer = null;
  }
}

function resumePendingJobStatusChecks(): void {
  for (const [key, state] of jobStates.entries()) {
    const jobId = String(state.job_id || "").trim();
    if (jobId && !terminalPhase(state.phase)) {
      watchJob(jobId, key);
    }
  }
  if (pollingJobs.size > 0) scheduleBatchJobPoll(0);
}

function markAuthExpired(message = "登录已过期，请重新扫码登录。"): void {
  auth.loggedIn = false;
  auth.user = {};
  auth.scopeOptions = [];
  clearAuthKeepalive();
  clearWorkbenchRetry();
  stopRealtimeConnections();
  syncText.value = message;
}

function handleGlobalAuthExpired(event: Event): void {
  const detail = (event as CustomEvent<{ message?: string }>).detail || {};
  markAuthExpired(detail.message || "登录已过期，请重新扫码登录。");
}

const api = (path: string, options: RequestInit = {}): Promise<Dict> =>
  requestJson(path, options, {
    onOnline: () => {
      backendStatus.offline = false;
      backendStatus.message = "";
    },
    onOffline: (message) => {
      backendStatus.offline = true;
      backendStatus.lastErrorAt = Date.now();
      backendStatus.message = message;
    },
    onAuthExpired: (message) => {
      markAuthExpired(message);
    },
    onServerError: (message) => {
      backendStatus.offline = true;
      backendStatus.lastErrorAt = Date.now();
      backendStatus.message = message;
    },
  });

function scheduleAuthKeepalive(delayMs = authKeepaliveMs): void {
  clearAuthKeepalive();
  if (!auth.loggedIn || appDisposed) return;
  authKeepaliveTimer = window.setTimeout(async () => {
    authKeepaliveTimer = null;
    if (appDisposed || !auth.loggedIn) return;
    try {
      await loadAuthStatus({ silent: true });
    } catch {
      if (!appDisposed && auth.loggedIn) {
        authKeepaliveTimer = window.setTimeout(() => {
          authKeepaliveTimer = null;
          void loadAuthStatus({ silent: true }).catch(() => scheduleAuthKeepalive(authKeepaliveRetryMs));
        }, authKeepaliveRetryMs);
      }
    }
  }, delayMs);
}

async function loadAuthStatus(options: { silent?: boolean } = {}): Promise<void> {
  if (!options.silent) authChecking.value = true;
  try {
    const data = await api(`/api/auth/status?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    auth.loggedIn = Boolean(data.logged_in);
    auth.user = data.user || {};
    auth.scopeOptions = data.scope_options || [];
    auth.loginUrl = data.login_url || "/api/auth/login";
    if (auth.loggedIn) scheduleAuthKeepalive();
    else clearAuthKeepalive();
  } catch (error: any) {
    if (!options.silent) {
      syncText.value = error?.message || "登录状态检查失败";
    }
    throw error;
  } finally {
    if (!options.silent) authChecking.value = false;
  }
}

async function loadOverview(): Promise<void> {
  try {
    const data = await api("/api/scope-overview");
    scopeOverview.value = data.scopes || data.items || {};
  } catch {
    scopeOverview.value = {};
  }
}

async function loadCurrentPermissionRequest(): Promise<void> {
  try {
    const data = await api("/api/auth/permission-requests/current");
    const request = data.request || {};
    permissionRequest.requestId = request.request_id || "";
    permissionRequest.scopes = Array.isArray(request.requested_scopes) ? request.requested_scopes : permissionRequest.scopes;
    if (permissionRequest.requestId) {
      permissionRequest.message = "已恢复待确认申请，请输入管理员提供的验证码。";
    }
  } catch {
    // No permission request to restore.
  }
}

async function loadHandoverLinks(): Promise<void> {
  try {
    const data = await api("/api/handover-links");
    handoverLinks.value = data.links || {};
  } catch {
    handoverLinks.value = {};
  }
}

async function loadWorkbench(): Promise<void> {
  if (!currentScope.value) return;
  const requestSeq = ++workbenchLoadSeq;
  const requestScope = currentScope.value;
  loading.value = true;
  try {
    const data = await api(`/api/workbench?scope=${encodeURIComponent(requestScope)}`);
    if (requestSeq !== workbenchLoadSeq || requestScope !== currentScope.value) return;
    records.value = data.records || [];
    ongoing.value = dedupeOngoingItems(data.ongoing || []);
    pruneOngoingExpansion();
    zhihangRecords.value = data.zhihang_change_records || [];
    dailySummary.value = data.daily_summary || { date: "", items: [], stats: {} };
    resetLocalSummaryAdjustments();
    defaults.impact = data.defaults?.impact || defaults.impact;
    defaults.progress = data.defaults?.progress || defaults.progress;
    if (!userSelectedWorkType.value) {
      workType.value = resolveInitialWorkType(data.default_work_type || workType.value);
    }
    syncText.value = workbenchSyncText(data);
    clearWorkbenchRetry();
    await loadAvailableUndos(requestScope, requestSeq);
    focusPendingNoticeResult();
    pruneSelection();
  } catch (error: any) {
    if (requestSeq !== workbenchLoadSeq || requestScope !== currentScope.value) return;
    const message = error?.message || "加载失败";
    syncText.value = message;
    scheduleWorkbenchRetry(message);
  } finally {
    if (requestSeq === workbenchLoadSeq && requestScope === currentScope.value) {
      loading.value = false;
    }
  }
}

function workbenchSyncText(data: Dict): string {
  if (data.source_snapshot_ready === false) return "后台正在准备数据";
  const ts = Number(data.last_loaded_ts || 0);
  if (ts > 0) {
    const ageSeconds = Math.max(0, Date.now() / 1000 - ts);
    if (ageSeconds >= staleSourceSnapshotSeconds) {
      const loadedAt = String(data.last_loaded_at || "").trim();
      return loadedAt ? `显示上次成功数据：${loadedAt}` : "显示上次成功数据";
    }
  }
  return "数据已就绪";
}

async function manualRefreshWorkbench(): Promise<void> {
  if (loading.value || refreshCooldown.workbench) return;
  startRefreshCooldown("workbench");
  clearWorkbenchRetry();
  await loadWorkbench();
}

async function loadAvailableUndos(scope = currentScope.value, requestSeq = workbenchLoadSeq): Promise<void> {
  if (!scope) {
    availableUndoItems.value = [];
    return;
  }
  try {
    const data = await api(`/api/notice-undo/available?scope=${encodeURIComponent(scope)}&since_seconds=${recentUndoSeconds}`);
    if (requestSeq !== workbenchLoadSeq || scope !== currentScope.value) return;
    availableUndoItems.value = Array.isArray(data.items) ? data.items : [];
  } catch {
    if (requestSeq === workbenchLoadSeq && scope === currentScope.value) {
      availableUndoItems.value = [];
    }
  }
}

function resolveInitialWorkType(preferred: string): string {
  const preferredType = normalizeWorkType(preferred);
  if (recordTypeCounts.value[preferredType] > 0) return preferredType;
  const fallback = workTypes.find((item) => recordTypeCounts.value[item.value] > 0);
  return fallback?.value || preferredType;
}

function selectWorkType(value: string): void {
  workType.value = value;
  userSelectedWorkType.value = true;
}

async function toggleWorkTypeOverride(record: Dict): Promise<void> {
  if (!canToggleWorkTypeOverride(record)) return;
  const oldKey = recordKey(record);
  if (typeOverrideBusyKey.value) return;
  const targetType = targetOverrideWorkType(record);
  typeOverrideBusyKey.value = oldKey;
  try {
    await api("/api/notice-work-type-override", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        record_id: record.record_id || record.source_record_id || "",
        source_work_type: sourceWorkTypeForRecord(record),
        target_work_type: targetType,
      }),
    });
    selectedKeys.delete(oldKey);
    drafts.delete(oldKey);
    activeDraftKey.value = "";
    workType.value = targetType;
    userSelectedWorkType.value = true;
    syncText.value = targetType === "change" ? "已转为变更通告" : "已转回维保通告";
    await loadWorkbench();
    const nextRecord = records.value.find((item) => {
      return String(item.record_id || "") === String(record.record_id || "")
        && String(item.work_type || "maintenance") === targetType;
    });
    if (nextRecord) {
      const nextKey = recordKey(nextRecord);
      selectedKeys.add(nextKey);
      activeDraftKey.value = nextKey;
      nextTick(() => {
        document.querySelector(".draft-card.active")?.scrollIntoView({ block: "center", behavior: "smooth" });
      });
    }
  } catch (error: any) {
    syncText.value = error?.message || "通告类型切换失败";
  } finally {
    if (typeOverrideBusyKey.value === oldKey) typeOverrideBusyKey.value = "";
  }
}

function focusPendingNoticeResult(): void {
  const pending = pendingFocusNotice.value;
  if (!pending) return;
  const pendingTitle = normalizeDraftSignatureText(String(pending.title || ""));
  const found = ongoing.value.find((item) => {
    if (pending.work_type && (item.work_type || "maintenance") !== pending.work_type) return false;
    const target = String(item.target_record_id || item.feishu_record_id || item.raw_record_id || item.record_id || "").trim();
    const source = String(item.source_record_id || "").trim();
    if (pending.target_record_id && target && target === pending.target_record_id) return true;
    if (pending.source_record_id && source && source === pending.source_record_id) return true;
    return Boolean(pendingTitle && normalizeDraftSignatureText(ongoingTitle(item)) === pendingTitle);
  });
  if (!found) return;
  const key = ongoingLineKey(found);
  if (!key) return;
  activeOngoingKey.value = key;
  pendingFocusNotice.value = null;
  syncText.value = "已定位到右侧进行中通告";
  nextTick(() => {
    document.querySelector(".ongoing-card.active")?.scrollIntoView({ block: "center", behavior: "smooth" });
  });
}

function pruneSelection(): void {
  const valid = new Set(records.value.map(recordKey));
  for (const key of Array.from(selectedKeys)) {
    if (!valid.has(key) && !isManualKey(key)) {
      selectedKeys.delete(key);
      continue;
    }
    const record = records.value.find((item) => recordKey(item) === key);
    if (record && isRecordOngoing(record)) selectedKeys.delete(key);
  }
  saveDrafts();
  pruneRuntimeState();
}

function resetLocalSummaryAdjustments(): void {
  localSummaryAdjustments.started = 0;
  localSummaryAdjustments.updated = 0;
  localSummaryAdjustments.ended = 0;
  localSummaryAdjustments.ongoing = 0;
}

function bumpLocalSummary(field: keyof typeof localSummaryAdjustments, delta = 1): void {
  localSummaryAdjustments[field] += delta;
}

function removeOngoingLine(key: string): boolean {
  if (!key) return false;
  const before = ongoing.value.length;
  ongoing.value = ongoing.value.filter((item) => ongoingLineKey(item) !== key);
  deleteOngoingEdit(key);
  if (activeOngoingKey.value === key) activeOngoingKey.value = "";
  return ongoing.value.length !== before;
}

function pruneOngoingExpansion(): void {
  if (!activeOngoingKey.value) return;
  const exists = ongoing.value.some((item) => ongoingLineKey(item) === activeOngoingKey.value);
  if (!exists) activeOngoingKey.value = "";
}

function activeLineKeys(): Set<string> {
  const keys = new Set<string>(Array.from(selectedKeys));
  for (const item of ongoing.value) {
    const key = ongoingLineKey(item);
    if (key) keys.add(key);
  }
  for (const item of closedSummaryItems.value) {
    if (item.undo_available) keys.add(undoLineKey(item));
  }
  for (const item of recentUndoItems.value) {
    keys.add(undoLineKey(item));
  }
  return keys;
}

function clearFallbackPoll(key: string): void {
  const timer = fallbackPollTimers.get(key);
  if (timer) window.clearTimeout(timer);
  fallbackPollTimers.delete(key);
  pollingJobs.delete(key);
}

function scheduleWorkbenchReload(delay = 350): void {
  if (appDisposed) return;
  if (workbenchRefreshTimer !== null) return;
  workbenchRefreshTimer = window.setTimeout(() => {
    workbenchRefreshTimer = null;
    if (appDisposed) return;
    if (!pageVisible.value) {
      pendingHiddenRefresh.value = true;
      activeItemsUpdatePending.value = true;
      return;
    }
    if (isUserEditing()) {
      activeItemsUpdatePending.value = true;
      syncText.value = "后台有更新，完成输入后自动刷新";
      scheduleWorkbenchReload(3000);
      return;
    }
    activeItemsUpdatePending.value = false;
    void loadWorkbench();
  }, delay);
}

function isUserEditing(): boolean {
  const element = document.activeElement as HTMLElement | null;
  if (!element) return false;
  const tag = element.tagName.toLowerCase();
  if (["input", "textarea", "select"].includes(tag)) return true;
  return Boolean(element.isContentEditable);
}

function flushPendingHiddenRefresh(): void {
  if (!isWorkbench.value || !currentScope.value) {
    pendingHiddenRefresh.value = false;
    activeItemsUpdatePending.value = false;
    return;
  }
  pendingHiddenRefresh.value = false;
  activeItemsUpdatePending.value = false;
  void loadWorkbench();
}

function retryFrontendConnections(): void {
  backendStatus.offline = false;
  backendStatus.message = "";
  clearWorkbenchRetry();
  if (auth.loggedIn) {
    scheduleAuthKeepalive(1000);
    startJobSse();
    if (isWorkbench.value) startActiveItemsSse();
  }
  if (isWorkbench.value && currentScope.value) {
    void loadWorkbench();
  } else {
    void loadAuthStatus({ silent: true }).catch(() => null);
  }
}

function handleVisibilityChange(): void {
  pageVisible.value = !document.hidden;
  if (!pageVisible.value) {
    clearAuthKeepalive();
    pauseJobStatusChecksForHiddenPage();
    stopRealtimeConnections();
    return;
  }
  if (auth.loggedIn) {
    scheduleAuthKeepalive(1000);
    if (!isJobStreamStarted()) startJobSse();
    if (isWorkbench.value && !isActiveItemsStreamStarted()) startActiveItemsSse();
    resumePendingJobStatusChecks();
  }
  if (pendingHiddenRefresh.value) {
    flushPendingHiddenRefresh();
  }
}

function pruneRuntimeState(): void {
  const visibleKeys = activeLineKeys();
  const now = Date.now();
  const staleBefore = now - 30 * 60 * 1000;
  for (const key of Array.from(ongoingEdits.keys())) {
    if (!visibleKeys.has(key)) deleteOngoingEdit(key);
  }
  for (const [key, state] of Array.from(jobStates.entries())) {
    const updatedAt = Date.parse(String(state.updated_at || ""));
    const stale = Number.isFinite(updatedAt) && updatedAt < staleBefore;
    if (!visibleKeys.has(key) && (terminalPhase(state.phase) || stale)) {
      jobStates.delete(key);
      clearFallbackPoll(key);
    }
  }
}

function enterScope(scope: string): void {
  switchScope(scope);
}

function enterEngineerMop(scope: string): void {
  const url = new URL("/engineer/mop", window.location.origin);
  url.searchParams.set("scope", normalizeScopeValue(scope, "ALL"));
  window.location.href = url.toString();
}

function returnToHome(): void {
  if (isEngineerMopPage.value || isHistoryMemoryPage.value) {
    window.location.href = "/";
    return;
  }
  if (currentScope.value) saveDrafts();
  stopActiveItemsSse();
  clearWorkbenchRetry();
  isWorkbench.value = false;
  currentScope.value = "";
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  selectedKeys.clear();
  clearOngoingEdits();
  records.value = [];
  ongoing.value = [];
  zhihangRecords.value = [];
  dailySummary.value = { date: "", items: [], stats: {} };
  availableUndoItems.value = [];
  resetLocalSummaryAdjustments();
  syncText.value = "请选择功能";
  const url = new URL(window.location.href);
  url.searchParams.delete("scope");
  window.history.replaceState({}, "", url);
}

function switchScope(scope: string): void {
  const nextScope = normalizeScopeValue(scope, "ALL");
  if (!nextScope) return;
  if (nextScope === currentScope.value && isWorkbench.value) return;
  clearWorkbenchRetry();
  if (currentScope.value && nextScope !== currentScope.value) {
    saveDrafts();
  }
  currentScope.value = nextScope;
  isWorkbench.value = true;
  userSelectedWorkType.value = false;
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  clearOngoingEdits();
  records.value = [];
  ongoing.value = [];
  zhihangRecords.value = [];
  dailySummary.value = { date: "", items: [], stats: {} };
  availableUndoItems.value = [];
  resetLocalSummaryAdjustments();
  syncText.value = "切换中";
  const url = new URL(window.location.href);
  url.searchParams.set("scope", currentScope.value);
  window.history.replaceState({}, "", url);
  loadDrafts();
  loadWorkbench();
  startActiveItemsSse();
}

function pinDraftInMiddlePanel(key: string): void {
  if (!key) return;
  activeDraftKey.value = key;
  nextTick(() => {
    draftStackRef.value?.scrollTo({ top: 0, behavior: "smooth" });
  });
}

function draftSummary(record: Dict, draft: Dict): string {
  const timeRange = [draft.start_time, draft.end_time].filter(Boolean).join("~");
  return [
    draft.specialty || specialtyForRecord(record),
    draft.maintenance_cycle || fieldsOf(record)["维护周期"],
    draft.non_plan ? "非计划性" : "",
    draft.location,
    timeRange,
  ].filter(Boolean).join(" · ");
}

function draftOriginLabel(record: Dict, draft: Dict): string {
  if (!record.manual) return "源表事项";
  if (draft.manual_origin === "paste") return "解析粘贴";
  if (draft.prefilled_from_last) return "纯手填 · 已带入上次内容";
  return "纯手填";
}

function draftCardMeta(record: Dict, draft: Dict, editing: boolean): string {
  return [
    workTypeLabel(record.work_type),
    draftOriginLabel(record, draft),
    editing ? "正在编辑" : "点击编辑",
  ].filter(Boolean).join(" · ");
}

function requiredDraftFields(record: Dict, draft: Dict): string[] {
  const type = draftWorkType(record, draft);
  const fields = new Set<string>(["title", "start_time", "end_time", "progress"]);
  if (record.manual) fields.add("building");
  if (type === "maintenance") {
    ["location", "content", "reason", "impact"].forEach((field) => fields.add(field));
    if (record.manual) fields.add("maintenance_cycle");
  } else if (type === "change") {
    ["level", "location", "content", "reason", "impact"].forEach((field) => fields.add(field));
  } else if (type === "repair") {
    ["location", "level", "specialty", "repair_device", "repair_fault", "impact", "reason", "solution"].forEach((field) => fields.add(field));
  } else if (type === "power") {
    ["cabinet", "quantity"].forEach((field) => fields.add(field));
  } else if (type === "polling") {
    ["device", "content", "impact"].forEach((field) => fields.add(field));
  } else if (type === "adjust") {
    ["location", "content", "reason", "impact"].forEach((field) => fields.add(field));
  }
  requiredUploadFields(type, draft).forEach((field) => fields.add(field));
  return Array.from(fields);
}

function draftFieldValue(record: Dict, draft: Dict, field: string): string {
  if (field === "title") return manualDraftTitle(draft, draftWorkType(record, draft));
  return String(draft[field] ?? "").trim();
}

function requiredUploadFields(type: string, values: Dict): string[] {
  const result: string[] = [];
  noticeTemplate(type).uploadFields.forEach((field) => {
    if (field === "non_plan") return;
    if (field === "zhihang") {
      if (Boolean(values.zhihang_involved)) result.push("zhihang_record_id");
      return;
    }
    result.push(field);
  });
  return result;
}

function missingUploadFields(type: string, values: Dict): string[] {
  return requiredUploadFields(type, values).filter((field) => !String(values[field] ?? "").trim());
}

function uploadFieldsMissingText(type: string, missing: string[]): string {
  return missing.length ? `请补充多维上传字段：${missing.map((field) => noticeFieldLabel(type, field)).join("、")}` : "";
}

function draftMissingFields(record: Dict, draft: Dict): string[] {
  return requiredDraftFields(record, draft).filter((field) => !draftFieldValue(record, draft, field));
}

function draftTypeConflict(record: Dict, draft: Dict): { expectedType: string; actualType: string; keyword: string } | null {
  if (!record.manual) return null;
  const expectedType = draftWorkType(record, draft);
  const parsedType = String(draft.parsed_work_type || "").trim();
  if (parsedType && isKnownWorkType(parsedType) && parsedType !== expectedType) {
    return {
      expectedType,
      actualType: parsedType,
      keyword: `原粘贴通告识别为${workTypeLabel(parsedType)}`,
    };
  }
  const title = manualDraftTitle(draft, expectedType) || recordCardTitle(record);
  const normalizedTitle = String(title || "").replace(/\s+/g, "");
  if (!normalizedTitle) return null;
  for (const type of workTypes.map((item) => item.value)) {
    if (type === expectedType) continue;
    for (const pattern of noticeTypeKeywordRules[type] || []) {
      const match = normalizedTitle.match(pattern);
      if (match) {
        return {
          expectedType,
          actualType: type,
          keyword: match[0] || workTypeLabel(type),
        };
      }
    }
  }
  return null;
}

function draftTypeConflictText(record: Dict, draft: Dict): string {
  const conflict = draftTypeConflict(record, draft);
  if (!conflict) return "";
  return `当前选择的是${workTypeLabel(conflict.expectedType)}，但标题/名称包含“${conflict.keyword}”，像是${workTypeLabel(conflict.actualType)}通告。请改标题或重新选择通告类型。`;
}

function draftFieldInvalid(record: Dict, draft: Dict, field: string): boolean {
  if (!draft.validation_touched) return false;
  if (field === "title" && draftTypeConflict(record, draft)) return true;
  return Boolean(requiredDraftFields(record, draft).includes(field) && !draftFieldValue(record, draft, field));
}

function draftFieldClass(record: Dict, draft: Dict, field: string): Dict {
  return { "field-missing": draftFieldInvalid(record, draft, field) };
}

function draftMissingText(record: Dict, draft: Dict): string {
  if (!draft.validation_touched) return "";
  const conflict = draftTypeConflictText(record, draft);
  if (conflict) return conflict;
  const type = draftWorkType(record, draft);
  const missing = draftMissingFields(record, draft).map((field) => noticeFieldLabel(type, field));
  return missing.length ? `请补充：${missing.join("、")}` : "";
}

function noticePreviewText(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  const template = noticeTemplate(type);
  const actionLabel = draftActionLabel(record, draft);
  const lines = [`【${template.heading}】状态：${actionLabel}`];
  const append = (field: string, value: string) => {
    lines.push(`【${noticeFieldLabel(type, field)}】${String(value || "").trim()}`);
  };
  append("title", manualDraftTitle(draft, type));
  if (type === "repair") {
    for (const field of template.messageFields.filter((item) => item !== "title")) append(field, draft[field] || "");
  } else {
    if (type === "change") append("level", draft.level || "");
    lines.push(`【时间】${draft.start_time || ""}~${draft.end_time || ""}`);
    for (const field of template.messageFields.filter((item) => !["title", "level", "start_time", "end_time"].includes(item))) {
      append(field, draft[field] || "");
    }
  }
  return lines.join("\n");
}

function draftUploadPreviewRows(record: Dict, draft: Dict): Array<{ label: string; value: string }> {
  const type = draftWorkType(record, draft);
  const rows: Array<{ label: string; value: string }> = [
    { label: "通告类型", value: workTypeLabel(type) },
    { label: "楼栋/范围", value: record.manual ? draft.building : buildingForRecord(record) },
    { label: "专业", value: draft.specialty || specialtyForRecord(record) },
  ];
  if (type === "maintenance") {
    rows.push({ label: "维保周期", value: draft.maintenance_cycle || fieldsOf(record)["维护周期"] || "" });
    rows.push({ label: "非计划性", value: draft.non_plan ? "是" : "" });
  }
  if (type === "change") {
    rows.push({ label: "变更等级", value: draft.level || levelForRecord(record) || "I3" });
    rows.push({ label: "涉及智航", value: draft.zhihang_involved ? (draft.zhihang_title || "是") : "" });
  }
  if (type === "repair") {
    rows.push({ label: "紧急程度", value: draft.level || "" });
    rows.push({ label: "维修设备", value: draft.repair_device || "" });
  }
  if (type === "power") {
    rows.push({ label: "柜号", value: draft.cabinet || "" });
    rows.push({ label: "数量", value: draft.quantity || "" });
  }
  if (type === "polling") {
    rows.push({ label: "设备", value: draft.device || "" });
  }
  return rows.filter((row) => String(row.value || "").trim());
}

function sendDraftButtonLabel(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  return `发送${workTypeLabel(type)}${draftActionLabel(record, draft)}`;
}

function payloadDurationError(payload: Dict): string {
  const workType = String(payload?.work_type || "maintenance");
  if (workType === "repair") {
    return noticeDurationError(
      workType,
      payload?.fault_time || payload?.end_time,
      payload?.expected_time || payload?.start_time,
    );
  }
  return noticeDurationError(workType, payload?.start_time, payload?.end_time);
}

function validatePayloadDuration(payload: Dict, lineKey: string): boolean {
  const error = payloadDurationError(payload);
  if (!error) return true;
  rememberJob(lineKey, {
    text: error,
    status: "failed",
    phase: "failed",
  });
  syncText.value = error;
  return false;
}

function normalizeDraftSignatureText(value: string): string {
  return String(value || "").replace(/\s+/g, "").replace(/[；;。,.，、：:（）()《》【】]/g, "").toLowerCase();
}

function draftSignature(record: Dict, draft: Dict): string {
  const type = draftWorkType(record, draft);
  const title = normalizeDraftSignatureText(manualDraftTitle(draft, type) || recordCardTitle(record));
  const start = String(draft.start_time || "").slice(0, 16);
  const end = String(draft.end_time || "").slice(0, 16);
  return [type, title, start, end].join("|");
}

function findDuplicateDraftKey(record: Dict, draft: Dict, excludeKey = ""): string {
  const signature = draftSignature(record, draft);
  if (!signature.split("|")[1]) return "";
  for (const key of selectedKeys) {
    if (key === excludeKey) continue;
    const otherRecord = draftRecordForKey(key);
    const otherDraft = drafts.get(key);
    if (!otherRecord || !otherDraft) continue;
    if (draftSignature(otherRecord, otherDraft) === signature) return key;
  }
  return "";
}

function toggleRecordSelection(row: NoticeRow | undefined): void {
  const key = row?.id || "";
  if (!key) return;
  const record = draftRecordForKey(key);
  if (row?.disabled || (record && isRecordOngoing(record))) {
    syncText.value = "该事项已在进行中，请在右侧卡片更新、结束或删除";
    return;
  }
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  if (record) getDraft(record);
  saveDrafts();
}

function addManualDraft(type = workType.value): void {
  const normalizedType = normalizeWorkType(type);
  const key = `manual:${normalizedType}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const draft = manualDraftDefaults(normalizedType);
  const memory = loadManualTemplateMemory(normalizedType);
  if (memory) {
    Object.assign(draft, memory, {
      manual: true,
      work_type: normalizedType,
      prefilled_from_last: true,
    });
    if (!draft.building) draft.building = defaultBuildingForCurrentScope();
    if (!Array.isArray(draft.building_codes) || !draft.building_codes.length) {
      draft.building_codes = buildingCodesFromText(draft.building || "");
    }
  }
  draft.manual_origin = "manual";
  drafts.set(key, draft);
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  showManualTypePicker.value = false;
  workType.value = normalizedType;
  saveManualRecentType(normalizedType);
  saveDrafts();
}

function removeDraft(key: string): void {
  selectedKeys.delete(key);
  if (isManualKey(key)) drafts.delete(key);
  jobStates.delete(key);
  clearFallbackPoll(key);
  if (activeDraftKey.value === key) activeDraftKey.value = "";
  saveDrafts();
}

function changeTargetCandidateId(item: Dict): string {
  return String(item?.record_id || item?.target_record_id || "").trim();
}

function changeSourceCandidateId(item: Dict): string {
  return String(item?.source_record_id || item?.record_id || "").trim();
}

const filteredChangeTargetCandidates = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  const candidates = Array.isArray(pending?.candidates) ? pending.candidates : [];
  return filterCandidatesBySearch(candidates, changeTargetSearchText.value);
});

const selectedChangeTargetVisible = computed(() => {
  const selected = selectedChangeTargetId.value;
  if (!selected) return false;
  return filteredChangeTargetCandidates.value.some((item: Dict) => changeTargetCandidateId(item) === selected);
});

const visibleActiveChangeTargetCandidate = computed(() => {
  const detailId = hoveredChangeTargetId.value || selectedChangeTargetId.value;
  if (detailId) {
    const visible = filteredChangeTargetCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === detailId);
    if (visible) return visible;
  }
  return filteredChangeTargetCandidates.value[0] || null;
});

const changeSourceCandidates = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  return Array.isArray(pending?.sourceCandidates) ? pending.sourceCandidates : [];
});

const filteredChangeSourceCandidates = computed(() => {
  return filterCandidatesBySearch(changeSourceCandidates.value, changeSourceSearchText.value);
});

const selectedChangeSourceVisible = computed(() => {
  const selected = selectedChangeSourceId.value;
  if (!selected) return false;
  return filteredChangeSourceCandidates.value.some((item: Dict) => changeSourceCandidateId(item) === selected);
});

const selectedChangeSourceCandidate = computed(() => {
  const id = selectedChangeSourceId.value;
  if (!id) return null;
  return changeSourceCandidates.value.find((item: Dict) => changeSourceCandidateId(item) === id) || null;
});

const ongoingBindCandidates = computed(() => {
  const pending = ongoingBindSelection.value;
  return Array.isArray(pending?.candidates) ? pending.candidates : [];
});

const activeOngoingBindCandidate = computed(() => {
  const candidates = ongoingBindCandidates.value;
  if (!candidates.length) return null;
  const detailId = hoveredOngoingBindId.value || selectedOngoingBindId.value;
  return candidates.find((item: Dict) => changeTargetCandidateId(item) === detailId) || candidates[0];
});

function changeTargetDetailRows(item: Dict | null): Array<{ label: string; value: string }> {
  if (!item) return [];
  const source = Array.isArray(item.field_items)
    ? item.field_items
    : Object.entries(item.fields || {}).map(([label, value]) => ({ label, value }));
  const rows = source
    .map((row: Dict) => ({
      label: String(row.label || "").trim(),
      value: String(row.value ?? "").trim(),
    }))
    .filter((row: { label: string; value: string }) => row.label && row.value);
  if (rows.length) return rows;
  return [
    { label: "名称", value: String(item.title || "") },
    { label: "楼栋", value: String(item.building || "") },
    { label: "状态", value: String(item.status || "") },
    { label: "开始时间", value: String(item.start_time || "") },
    { label: "结束时间", value: String(item.end_time || "") },
  ].filter((row) => row.value);
}

function previewChangeTarget(item: Dict): void {
  hoveredChangeTargetId.value = changeTargetCandidateId(item);
}

function selectChangeTarget(item: Dict): void {
  const id = changeTargetCandidateId(item);
  selectedChangeTargetId.value = id;
  hoveredChangeTargetId.value = id;
}

function previewOngoingBindCandidate(item: Dict): void {
  hoveredOngoingBindId.value = changeTargetCandidateId(item);
}

function selectOngoingBindCandidate(item: Dict): void {
  const id = changeTargetCandidateId(item);
  selectedOngoingBindId.value = id;
  hoveredOngoingBindId.value = id;
}

function closeOngoingBindSelection(candidate: Dict | null): void {
  const resolver = resolveOngoingBindSelection;
  resolveOngoingBindSelection = null;
  ongoingBindSelection.value = null;
  selectedOngoingBindId.value = "";
  hoveredOngoingBindId.value = "";
  if (resolver) resolver(candidate);
}

function cancelOngoingBindSelection(): void {
  closeOngoingBindSelection(null);
}

function confirmOngoingBindSelection(): void {
  const selected =
    ongoingBindCandidates.value.find((item: Dict) => changeTargetCandidateId(item) === selectedOngoingBindId.value) ||
    activeOngoingBindCandidate.value;
  closeOngoingBindSelection(selected || null);
}

function firstCandidateField(fields: Dict, names: string[]): string {
  for (const name of names) {
    const value = fields?.[name];
    const text = cleanDisplayText(value);
    if (text) return text;
  }
  return "";
}

function fillDraftBlank(draft: Dict, key: string, value: string): void {
  const text = String(value || "").trim();
  if (!text) return;
  if (String(draft[key] ?? "").trim()) return;
  draft[key] = text;
}

function fillDraftBlankDatetime(draft: Dict, key: string, value: string): void {
  const normalized = toDatetimeLocal(value);
  fillDraftBlank(draft, key, normalized || value);
}

function applyChangeTargetCandidateDefaults(draft: Dict, candidate: Dict): Dict {
  const fields = candidate?.fields || {};
  const next = { ...draft };
  fillDraftBlank(next, "title", firstCandidateField(fields, ["名称", "标题", "变更简述"]) || candidate.title || "");
  fillDraftBlank(next, "building", firstCandidateField(fields, ["楼栋", "变更楼栋"]) || candidate.building || "");
  fillDraftBlank(next, "specialty", firstCandidateField(fields, ["专业", "专业类别"]));
  fillDraftBlank(next, "maintenance_cycle", firstCandidateField(fields, ["维保周期", "维护周期"]));
  fillDraftBlank(next, "level", firstCandidateField(fields, ["阿里-变更等级", "智航-变更等级", "变更等级", "变更等级（阿里）", "紧急程度", "等级"]));
  fillDraftBlankDatetime(next, "start_time", firstCandidateField(fields, ["变更开始时间", "计划开始时间", "计划开始", "开始时间", "期望完成时间"]) || candidate.start_time || "");
  fillDraftBlankDatetime(next, "end_time", firstCandidateField(fields, ["计划结束时间", "计划结束", "结束时间", "发生故障时间", "故障发生时间"]) || candidate.end_time || "");
  fillDraftBlank(next, "location", firstCandidateField(fields, ["位置", "地点"]));
  fillDraftBlank(next, "content", firstCandidateField(fields, ["内容", "变更内容", "变更简述"]));
  fillDraftBlank(next, "reason", firstCandidateField(fields, ["原因", "变更原因"]));
  fillDraftBlank(next, "impact", firstCandidateField(fields, ["影响", "影响范围"]));
  fillDraftBlank(next, "progress", firstCandidateField(fields, ["进度", "完成情况"]));
  fillDraftBlank(next, "repair_device", firstCandidateField(fields, ["维修设备"]));
  fillDraftBlank(next, "repair_fault", firstCandidateField(fields, ["维修故障"]));
  fillDraftBlank(next, "fault_type", firstCandidateField(fields, ["故障类型"]));
  fillDraftBlank(next, "repair_mode", firstCandidateField(fields, ["维修方式"]));
  fillDraftBlank(next, "discovery", firstCandidateField(fields, ["故障发现方式（来源）", "故障发现方式"]));
  fillDraftBlank(next, "symptom", firstCandidateField(fields, ["故障现象"]));
  fillDraftBlank(next, "solution", firstCandidateField(fields, ["解决方案"]));
  fillDraftBlank(next, "spare_parts", firstCandidateField(fields, ["备件更换情况", "备件使用情况"]));
  fillDraftBlank(next, "device", firstCandidateField(fields, ["设备", "维修设备"]));
  fillDraftBlank(next, "cabinet", firstCandidateField(fields, ["柜号"]));
  fillDraftBlank(next, "quantity", firstCandidateField(fields, ["数量（个）", "数量"]));
  return next;
}

function completeParsedNoticeDraft(type: string, draft: Dict, options: Dict = {}): void {
  const key = `manual:${type}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  Object.assign(draft, options, { manual_origin: "paste" });
  const candidateRecord = manualRecordFromDraft(key, draft);
  const duplicateKey = findDuplicateDraftKey(candidateRecord, draft, key);
  if (duplicateKey) {
    activeDraftKey.value = duplicateKey;
    pasteParseLine.value = "待发起通告中已有相同类型、标题和时间的草稿。";
    pasteParseStatus.value = "failed";
    return;
  }
  drafts.set(key, draft);
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  workType.value = type;
  pasteText.value = "";
  pendingChangeTargetSelection.value = null;
  selectedChangeTargetId.value = "";
  hoveredChangeTargetId.value = "";
  selectedChangeSourceId.value = "";
  changeTargetSearchText.value = "";
  changeSourceSearchText.value = "";
  showPasteParser.value = false;
  pasteParseLine.value = `已解析为${workTypeLabel(type)}${parsedActionLabel(draft.parsed_action || "start")}通告。`;
  pasteParseStatus.value = "success";
  saveDrafts();
}

async function parsePastedNotice(): Promise<void> {
  const text = pasteText.value.trim();
  if (!text) return;
  pasteParseBusy.value = true;
  pasteParseLine.value = "正在解析通告...";
  pasteParseStatus.value = "";
  pendingChangeTargetSelection.value = null;
  selectedChangeTargetId.value = "";
  hoveredChangeTargetId.value = "";
  selectedChangeSourceId.value = "";
  changeTargetSearchText.value = "";
  changeSourceSearchText.value = "";
  try {
    const sections = parseSections(text);
    if (/事件通告/.test(text)) {
      throw new Error("前端暂不支持事件通告纯手填或解析，请在 Qt 主界面处理事件通告。");
    }
    const type = pastedNoticeWorkType(text, sections);
    const draft = manualDraftDefaults(type);
    draft.parsed_work_type = type;
    const status = pastedNoticeStatus(text);
    const action = parsedActionFromStatus(status);
    const timeRange = splitNoticeTimeRange(sectionValue(sections, ["时间"]));
    draft.parsed_action = action;
    draft.title = type === "change"
      ? sectionValue(sections, ["名称", "标题"])
      : sectionValue(sections, ["标题", "名称", "维修名称"]);
    draft.non_plan = /（非计划性）|（非计划）/.test(draft.title);
    draft.location = sectionValue(sections, ["地点", "位置"]) || rawSectionValue(text, ["地点", "位置"]);
    draft.specialty = sectionValue(sections, ["专业", "专业类别"]) || rawSectionValue(text, ["专业", "专业类别"]);
    draft.reason = sectionValue(sections, ["原因", "故障原因"]) || rawSectionValue(text, ["原因", "故障原因"]);
    draft.impact = sectionValue(sections, ["影响", "影响范围"]) || rawSectionValue(text, ["影响", "影响范围"]);
    draft.progress = sectionValue(sections, ["进度", "完成情况"]) || rawSectionValue(text, ["进度", "完成情况"]);
    draft.maintenance_cycle = sectionValue(sections, ["维保周期", "维护周期"]) || rawSectionValue(text, ["维保周期", "维护周期"]);
    draft.level = sectionValue(sections, ["等级", "变更等级", "紧急程度"]) || rawSectionValue(text, ["等级", "变更等级", "紧急程度"]) || (type === "change" ? "I3" : "");
    draft.start_time = timeRange.start || draft.start_time;
    draft.end_time = timeRange.end || draft.end_time;
    draft.building = sectionValue(sections, ["楼栋", "变更楼栋", "所属楼栋"])
      || inferBuildingText(draft.title, draft.location, text)
      || defaultBuildingForCurrentScope();
    draft.building_codes = buildingCodesFromText(draft.building);
    draft.content = type === "repair"
      ? sectionValue(sections, ["标题/补充内容", "标题补充内容", "补充内容", "内容"])
      : sectionValue(sections, ["内容"], draft.title);
    draft.repair_device = sectionValue(sections, ["维修设备"]) || rawSectionValue(text, ["维修设备"]);
    draft.repair_fault = sectionValue(sections, ["维修故障"]) || rawSectionValue(text, ["维修故障"]);
    draft.fault_type = sectionValue(sections, ["故障类型"]) || rawSectionValue(text, ["故障类型"]);
    draft.repair_mode = sectionValue(sections, ["维修方式"]) || rawSectionValue(text, ["维修方式"]);
    draft.discovery = sectionValue(sections, ["故障发现方式"]) || rawSectionValue(text, ["故障发现方式"]);
    draft.symptom = sectionValue(sections, ["故障现象"]) || rawSectionValue(text, ["故障现象"]);
    draft.solution = sectionValue(sections, ["解决方案"]) || rawSectionValue(text, ["解决方案"]);
    draft.spare_parts = sectionValue(sections, ["备件更换情况", "备件使用情况"]) || rawSectionValue(text, ["备件更换情况", "备件使用情况"]);
    draft.device = sectionValue(sections, ["设备"]) || rawSectionValue(text, ["设备"]);
    draft.cabinet = sectionValue(sections, ["柜号"]) || rawSectionValue(text, ["柜号"]);
    draft.quantity = sectionValue(sections, ["数量"]) || rawSectionValue(text, ["数量"]);
    if (type === "repair") {
      const expectedTime = sectionValue(sections, ["期望完成时间"]);
      const faultTime = sectionValue(sections, ["发现故障时间", "故障发生时间", "发生故障时间"]);
      draft.start_time = toDatetimeLocal(expectedTime) || timeRange.end || timeRange.start || draft.start_time;
      draft.end_time = toDatetimeLocal(faultTime) || timeRange.start || draft.end_time;
      draft.reason = sectionValue(sections, ["故障原因", "原因"], draft.reason);
      draft.impact = sectionValue(sections, ["影响范围", "影响"], draft.impact);
      draft.progress = sectionValue(sections, ["完成情况", "进度"], draft.progress);
    }
    if (action !== "start") {
      if (!draft.title) {
        throw new Error(`${workTypeLabel(type)}更新/结束通告必须包含【名称】或【标题】。`);
      }
      const data = await api(type === "change" ? "/api/change-target-candidates" : "/api/notice-target-candidates", {
        method: "POST",
        body: JSON.stringify({
          work_type: type,
          scope: currentScope.value || "ALL",
          title: draft.title,
          start_time: draft.start_time,
          end_time: draft.end_time,
          action,
        }),
      });
      const candidates = Array.isArray(data.candidates) ? data.candidates : [];
      const sourceCandidates = Array.isArray(data.source_candidates) ? data.source_candidates : [];
      if (candidates.length > 0 || sourceCandidates.length > 0) {
        pendingChangeTargetSelection.value = {
          type,
          draft,
          action,
          actionLabel: parsedActionLabel(action),
          candidates,
          sourceCandidates,
          returnedCount: Number(data.returned_count ?? data.count ?? candidates.length),
          totalMatched: Number(data.total_matched ?? candidates.length),
          limit: Number(data.limit ?? candidates.length),
          limited: Boolean(data.limited),
        };
        changeTargetSearchText.value = "";
        changeSourceSearchText.value = "";
        selectedChangeTargetId.value = changeTargetCandidateId(candidates[0]);
        hoveredChangeTargetId.value = selectedChangeTargetId.value;
        selectedChangeSourceId.value = sourceCandidates.length ? changeSourceCandidateId(sourceCandidates[0]) : "";
        pasteParseLine.value = candidates.length
          ? `找到 ${Number(data.total_matched ?? candidates.length)} 条同名${workTypeLabel(type)}目标记录，当前显示 ${candidates.length} 条，请确认要${parsedActionLabel(action)}的记录。`
          : `未找到同名目标记录，但找到 ${sourceCandidates.length} 条源表记录；可先关联源表记录后继续提交。`;
        pasteParseStatus.value = "";
        return;
      }
      throw new Error(`未在${workTypeLabel(type)}目标表或源表中找到同名记录，不能作为更新/结束通告发送。`);
    }
    completeParsedNoticeDraft(type, draft);
  } catch (error: any) {
    pasteParseLine.value = error?.message || "解析失败";
    pasteParseStatus.value = "failed";
  } finally {
    pasteParseBusy.value = false;
  }
}

function choosePastedChangeTarget(candidate: Dict): void {
  const pending = pendingChangeTargetSelection.value;
  if (!pending) return;
  const source = selectedChangeSourceCandidate.value || {};
  const targetId = candidate.record_id || candidate.target_record_id || "";
  const draft = applyChangeTargetCandidateDefaults(
    applyChangeTargetCandidateDefaults({ ...(pending.draft || {}) }, candidate),
    source,
  );
  completeParsedNoticeDraft(String(pending.type || "change"), draft, {
    target_record_id: targetId,
    record_id: targetId || source.source_record_id || source.record_id || "",
    source_record_id: source.source_record_id || source.record_id || "",
    source_app_token: source.source_app_token || "",
    source_table_id: source.source_table_id || "",
    building: draft.building || candidate.building || "",
    building_codes: source.building_codes || candidate.building_codes || [],
  });
}

async function confirmPastedChangeTarget(): Promise<void> {
  const pending = pendingChangeTargetSelection.value;
  if (!pending || changeTargetConfirming.value) return;
  const candidates = filteredChangeTargetCandidates.value;
  const candidate =
    candidates.find((item: Dict) => changeTargetCandidateId(item) === selectedChangeTargetId.value) ||
    visibleActiveChangeTargetCandidate.value;
  const sourceOnly = !candidate && selectedChangeSourceVisible.value && selectedChangeSourceCandidate.value;
  if (!candidate && !sourceOnly) {
    pasteParseLine.value = "请先选择一条目标多维记录或源表记录。";
    pasteParseStatus.value = "failed";
    return;
  }
  changeTargetConfirming.value = true;
  pasteParseLine.value = "正在确认目标记录...";
  pasteParseStatus.value = "";
  try {
    let selected = candidate || {};
    if (candidate && String(pending.type || "") === "change" && String(pending.action || "") === "update") {
      const data = await api("/api/change-target-candidates/confirm", {
        method: "POST",
        body: JSON.stringify({
          scope: currentScope.value || "ALL",
          title: pending.draft?.title || candidate.title || "",
          start_time: pending.draft?.start_time || "",
          end_time: pending.draft?.end_time || "",
          action: pending.action || "update",
          record_id: changeTargetCandidateId(candidate || {}),
        }),
      });
      selected = data.candidate || candidate;
      if (Array.isArray(data.source_candidates)) {
        pending.sourceCandidates = data.source_candidates;
        if (
          pending.sourceCandidates.length &&
          !pending.sourceCandidates.some((item: Dict) => changeSourceCandidateId(item) === selectedChangeSourceId.value)
        ) {
          selectedChangeSourceId.value = changeSourceCandidateId(pending.sourceCandidates[0]);
        }
      }
      const cleared = data.clear_actual_end?.cleared;
      pasteParseLine.value = cleared ? "已确认目标记录，并清空原实际结束时间。" : "已确认目标记录。";
    }
    choosePastedChangeTarget(selected || {});
  } catch (error: any) {
    pasteParseLine.value = error?.message || "确认目标记录失败";
    pasteParseStatus.value = "failed";
  } finally {
    changeTargetConfirming.value = false;
  }
}

async function importHistoricalMemory(): Promise<void> {
  const text = memoryImportText.value.trim();
  if (!text) {
    memoryImportLine.value = "请先粘贴历史通告。";
    memoryImportLineType.value = "failed";
    return;
  }
  memoryImportBusy.value = true;
  memoryImportLine.value = "正在导入历史记忆...";
  memoryImportLineType.value = "";
  try {
    const data = await api("/api/notice-memory/import", {
      method: "POST",
      body: JSON.stringify({ scope: currentScope.value || "ALL", text }),
    });
    const imported = Number(data.imported_count || 0);
    const skipped = Number(data.skipped_count || 0);
    memoryImportLine.value = `已导入 ${imported} 条记忆${skipped ? `，跳过 ${skipped} 条` : ""}。`;
    memoryImportLineType.value = imported > 0 ? "success" : "failed";
    if (imported > 0) {
      memoryImportText.value = "";
      showMemoryImporter.value = false;
      await loadWorkbench();
    }
  } catch (error: any) {
    memoryImportLine.value = error?.message || "导入失败";
    memoryImportLineType.value = "failed";
  } finally {
    memoryImportBusy.value = false;
  }
}

function bindZhihang(draft: Dict): void {
  const item = zhihangRecords.value.find((record) => record.record_id === draft.zhihang_record_id);
  draft.zhihang_title = item?.title || "";
  draft.zhihang_progress = item?.progress || "";
  saveDrafts();
}

function bindOngoingZhihang(item: Dict, recordId: string): void {
  const change = zhihangRecords.value.find((record) => record.record_id === recordId);
  setOngoingEdit(item, "zhihang_record_id", recordId);
  setOngoingEdit(item, "zhihang_title", change?.title || "");
  setOngoingEdit(item, "zhihang_progress", change?.progress || "");
}

function opId(key: string): string {
  return `${key}:${Date.now()}`;
}

function payloadSendsPersonalMessage(payload: Dict): boolean {
  const type = String(payload?.work_type || "maintenance");
  if (type === "maintenance") return true;
  if (["power", "polling", "adjust"].includes(type)) return true;
  return Boolean(payload?.manual);
}

function buildStartPayload(key: string): Dict | null {
  const record = draftRecordForKey(key);
  const draft = drafts.get(key);
  if (!record || !draft) return null;
  const type = draftWorkType(record, draft);
  if (record.manual && draft.work_type !== type) draft.work_type = type;
  const action = record.manual ? draftActionForRecord(record, draft) : sourceActionForRecord(record);
  const targetRecordId = record.manual ? String(draft.target_record_id || draft.feishu_record_id || draft.raw_record_id || "").trim() : targetRecordIdForRecord(record);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: type,
    notice_type: record.notice_type || "",
    manual: Boolean(record.manual),
    manual_id: record.manual ? key : "",
    source_app_token: record.manual ? (draft.source_app_token || "") : (record.source_app_token || ""),
    source_table_id: record.manual ? (draft.source_table_id || "") : (record.source_table_id || ""),
    maintenance_cycle: record.manual ? (draft.maintenance_cycle || "") : (fieldsOf(record)["维护周期"] || ""),
    specialty: cleanDisplayText(draft.specialty) || specialtyForRecord(record),
    record_id: action === "start" ? record.record_id : targetRecordId,
    source_record_id: record.manual ? (draft.source_record_id || "") : record.record_id,
    source_work_type: record.manual ? (draft.source_work_type || type) : sourceWorkTypeForRecord(record),
    converted_from_work_type: record.manual ? (draft.converted_from_work_type || "") : (record.converted_from_work_type || ""),
    target_record_id: action !== "start" ? targetRecordId : "",
    source_progress: sourceProgressForRecord(record),
    building_codes: record.manual ? (draft.building_codes || []) : (record.building_codes || []),
    building: record.manual ? (draft.building || "") : buildingForRecord(record),
    title: payloadTitleForDraft(record, draft, type),
    non_plan: record.manual && type === "maintenance" ? Boolean(draft.non_plan) : false,
    level: record.manual ? (draft.level || (type === "change" ? "I3" : "")) : (type === "repair" ? (draft.level || "") : (type === "change" ? (levelForRecord(record) || "I3") : "")),
    start_time: draft.start_time,
    end_time: draft.end_time,
    location: draft.location,
    content: draft.content,
    reason: draft.reason,
    impact: draft.impact,
    progress: draft.progress,
    zhihang_involved: type === "change" ? Boolean(draft.zhihang_involved) : false,
    zhihang_record_id: type === "change" ? (draft.zhihang_record_id || "") : "",
    zhihang_title: type === "change" ? (draft.zhihang_title || "") : "",
    zhihang_progress: type === "change" ? (draft.zhihang_progress || "") : "",
    repair_device: type === "repair" ? (draft.repair_device || "") : "",
    repair_fault: type === "repair" ? (draft.repair_fault || "") : "",
    fault_type: type === "repair" ? (draft.fault_type || "") : "",
    repair_mode: type === "repair" ? (draft.repair_mode || "") : "",
    discovery: type === "repair" ? (draft.discovery || "") : "",
    symptom: type === "repair" ? (draft.symptom || "") : "",
    solution: type === "repair" ? (draft.solution || "") : "",
    spare_parts: type === "repair" ? (draft.spare_parts || "") : "",
    device: type === "polling" ? (draft.device || "") : "",
    cabinet: type === "power" ? (draft.cabinet || "") : "",
    quantity: type === "power" ? (draft.quantity || "") : "",
    fault_time: type === "repair" ? (draft.end_time || "") : "",
    expected_time: type === "repair" ? (draft.start_time || "") : "",
    operation_id: draft.operation_id || (draft.operation_id = opId(`${key}:${action}`)),
  };
}

async function sendStart(key: string): Promise<void> {
  const payload = buildStartPayload(key);
  if (!payload) return;
  const draft = drafts.get(key);
  const record = draftRecordForKey(key);
  if (record && draft) {
    draft.validation_touched = true;
    const typeConflict = draftTypeConflictText(record, draft);
    if (typeConflict) {
      rememberJob(key, {
        text: typeConflict,
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
    const missing = draftMissingFields(record, draft);
    if (missing.length) {
      rememberJob(key, {
        text: draftMissingText(record, draft) || "请补充必填字段",
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
    const duplicateKey = findDuplicateDraftKey(record, draft, key);
    if (duplicateKey) {
      activeDraftKey.value = duplicateKey;
      rememberJob(key, {
        text: "待发起通告中已有相同类型、标题和时间的草稿，请核对后再发送",
        status: "failed",
        phase: "failed",
      });
      saveDrafts();
      return;
    }
  }
  if (record && !recordMatchesCurrentScope(record)) {
    rememberJob(key, { text: "当前入口与通告楼栋不匹配，请切换到对应楼栋或园区后再发送", status: "failed", phase: "failed" });
    return;
  }
  if (payload.work_type === "maintenance" && payload.manual && !payload.maintenance_cycle) {
    rememberJob(key, { text: "纯手填维保必须选择维保周期", status: "failed", phase: "failed" });
    return;
  }
  if (!validatePayloadDuration(payload, key)) {
    saveDrafts();
    return;
  }
  if (payload.manual && draft?.manual_origin === "manual") {
    saveManualTemplateMemory(String(payload.work_type || ""), draft);
    saveManualRecentType(String(payload.work_type || ""));
  }
  await sendAction(payload, key);
  saveDrafts();
}

function ongoingDraft(item: Dict): Dict {
  const id = ongoingLineKey(item);
  if (!ongoingEdits.has(id)) {
    const timeRange = ongoingTimeRange(item);
    const workType = item.work_type || "maintenance";
    let title = item.title || item.content || "";
    const content = workType === "repair" && item.content === title ? "" : item.content || "";
    if (workType === "repair" && content && title.endsWith(content)) {
      title = title.slice(0, -content.length).trim() || title;
    }
    ongoingEdits.set(id, {
      title,
      specialty: cleanDisplayText(item.specialty),
      maintenance_cycle: item.maintenance_cycle || "",
      level: item.level || "",
      start_time: timeRange.start,
      end_time: timeRange.end,
      location: item.location || "",
      content,
      reason: item.reason || "",
      impact: item.impact || "",
      progress: item.progress || item.content || "",
      zhihang_involved: Boolean(item.zhihang_involved || item.zhihang_record_id),
      zhihang_record_id: item.zhihang_record_id || "",
      zhihang_title: item.zhihang_title || "",
      zhihang_progress: item.zhihang_progress || "",
      repair_device: item.repair_device || "",
      repair_fault: item.repair_fault || "",
      fault_type: item.fault_type || "",
      repair_mode: item.repair_mode || "",
      discovery: cleanDisplayText(item.discovery),
      symptom: item.symptom || "",
      solution: item.solution || "",
      spare_parts: item.spare_parts || "",
      device: item.device || "",
      cabinet: item.cabinet || "",
      quantity: item.quantity || "",
      extra_images: Array.isArray(item.extra_images) ? [...item.extra_images] : [],
      target_record_id: targetRecordIdForOngoing(item),
      source_record_id: sourceRecordIdForOngoing(item, targetRecordIdForOngoing(item)),
    });
  }
  return ongoingEdits.get(id) || {};
}

function setOngoingEdit(item: Dict, key: string, value: any): void {
  const id = ongoingLineKey(item);
  const current = ongoingDraft(item);
  current[key] = value;
  ongoingEdits.set(id, current);
}

function draftValue(edit: Dict, key: string, fallback = ""): string {
  if (Object.prototype.hasOwnProperty.call(edit, key)) return String(edit[key] ?? "");
  return String(fallback ?? "");
}

function ongoingExtraImages(item: Dict): Dict[] {
  const edit = ongoingDraft(item);
  return Array.isArray(edit.extra_images) ? edit.extra_images : [];
}

function ongoingPhotoCount(item: Dict): number {
  return ongoingExtraImages(item).filter((photo) => String(photo?.upload_id || photo?.bytes_b64 || photo?.file_token || "").trim()).length;
}

function ongoingEndRequiresSitePhoto(item: Dict): boolean {
  return ["maintenance", "change", "repair"].includes(String(item?.work_type || "maintenance"));
}

async function uploadSitePhotoFile(file: File): Promise<Dict> {
  const fileName = file.name || `site_photo_${Date.now()}.png`;
  try {
    const data = await requestBinaryJson(
      `/api/notice-attachments?file_name=${encodeURIComponent(fileName)}`,
      file,
      {
        headers: { "Content-Type": file.type || "image/png" },
      },
      {
        onOnline: () => {
          backendStatus.offline = false;
          backendStatus.message = "";
        },
        onOffline: (message) => {
          backendStatus.offline = true;
          backendStatus.lastErrorAt = Date.now();
          backendStatus.message = message;
        },
        onAuthExpired: (message) => {
          markAuthExpired(message);
        },
        onServerError: (message) => {
          backendStatus.offline = true;
          backendStatus.lastErrorAt = Date.now();
          backendStatus.message = message;
        },
      },
    );
    return {
      upload_id: data.upload_id,
      file_name: data.file_name || fileName,
      mime_type: data.mime_type || file.type || "image/png",
      size: data.size || file.size || 0,
      expires_at: data.expires_at || 0,
      preview_url: URL.createObjectURL(file),
    };
  } catch (error: any) {
    throw new Error(error?.message || "现场照片上传失败");
  }
}

function isValidSitePhotoFile(file: File): boolean {
  const type = String(file.type || "");
  if (type && !type.startsWith("image/")) return false;
  if (file.size > MAX_SITE_PHOTO_BYTES) return false;
  return true;
}

function pastedImageFiles(event: ClipboardEvent): File[] {
  const clipboard = event.clipboardData;
  if (!clipboard) return [];
  const files: File[] = [];
  Array.from(clipboard.items || []).forEach((item) => {
    if (!item.type.startsWith("image/")) return;
    const file = item.getAsFile();
    if (file) files.push(file);
  });
  if (files.length) return files;
  return Array.from(clipboard.files || []).filter((file) => {
    const type = String(file.type || "");
    return !type || type.startsWith("image/");
  });
}

async function addOngoingPhotoFiles(item: Dict, files: File[], invalidMessage: string): Promise<void> {
  if (!files.length) return;
  const key = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const existing = ongoingExtraImages(item);
  const availableSlots = Math.max(0, MAX_SITE_PHOTO_COUNT - existing.length);
  if (availableSlots <= 0) {
    rememberJob(key, { text: `现场照片最多添加 ${MAX_SITE_PHOTO_COUNT} 张`, status: "failed", phase: "failed" });
    return;
  }
  const validFiles = files
    .filter(isValidSitePhotoFile)
    .slice(0, availableSlots);
  if (!validFiles.length) {
    rememberJob(key, { text: invalidMessage, status: "failed", phase: "failed" });
    return;
  }
  try {
    rememberJob(key, { text: "正在上传现场照片", status: "busy", phase: "uploading_attachment" });
    const photos = await Promise.all(validFiles.map(uploadSitePhotoFile));
    edit.extra_images = [...existing, ...photos];
    ongoingEdits.set(key, edit);
    rememberJob(key, { text: `已添加现场照片 ${edit.extra_images.length} 张`, status: "", phase: "" });
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "现场照片读取失败", status: "failed", phase: "failed" });
  }
}

async function handleOngoingPhotoInput(item: Dict, event: Event): Promise<void> {
  const input = event.target as HTMLInputElement;
  const files = Array.from(input.files || []);
  input.value = "";
  await addOngoingPhotoFiles(item, files, "请选择不超过 8MB 的图片文件");
}

async function handleOngoingPhotoPaste(item: Dict, event: ClipboardEvent): Promise<void> {
  const files = pastedImageFiles(event);
  if (!files.length) {
    rememberJob(ongoingLineKey(item), { text: "剪贴板中没有图片，请先复制图片后再粘贴", status: "failed", phase: "failed" });
    return;
  }
  event.preventDefault();
  await addOngoingPhotoFiles(item, files, "剪贴板图片不是有效图片或超过 8MB");
}

function removeOngoingPhoto(item: Dict, index: number): void {
  const key = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const images = ongoingExtraImages(item).slice();
  revokePhotoPreview(images[index]);
  images.splice(index, 1);
  edit.extra_images = images;
  ongoingEdits.set(key, edit);
}

function revokePhotoPreview(photo: Dict | undefined): void {
  const url = String(photo?.preview_url || "");
  if (url.startsWith("blob:")) {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // Best effort only; failing to revoke should not block user actions.
    }
  }
}

function revokeOngoingEditPreviews(edit: Dict | undefined): void {
  const images = Array.isArray(edit?.extra_images) ? edit.extra_images : [];
  for (const photo of images) revokePhotoPreview(photo);
}

function deleteOngoingEdit(key: string): void {
  if (!key) return;
  revokeOngoingEditPreviews(ongoingEdits.get(key));
  ongoingEdits.delete(key);
}

function clearOngoingEdits(): void {
  for (const edit of ongoingEdits.values()) revokeOngoingEditPreviews(edit);
  ongoingEdits.clear();
}

function ongoingExtraImagesForUpload(item: Dict): Dict[] {
  return ongoingExtraImages(item).map((photo) => {
    const { preview_url: _previewUrl, data_url: _dataUrl, ...rest } = photo || {};
    return rest;
  });
}

function buildOngoingPayload(item: Dict, action: string): Dict {
  const edit = ongoingDraft(item);
  const targetRecordId = draftValue(edit, "target_record_id", targetRecordIdForOngoing(item));
  const sourceRecordId = draftValue(edit, "source_record_id", sourceRecordIdForOngoing(item, targetRecordId));
  const workType = item.work_type || "maintenance";
  const simpleManual = ["power", "polling", "adjust"].includes(workType);
  const startTime = draftValue(edit, "start_time", ongoingTimeRange(item).start);
  const endTime = draftValue(edit, "end_time", ongoingTimeRange(item).end);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: workType,
    notice_type: item.notice_type || "",
    manual: simpleManual ? true : Boolean(item.manual),
    manual_id: simpleManual ? String(item.manual_id || item.active_item_id || targetRecordId || sourceRecordId || ongoingLineKey(item)) : "",
    record_id: targetRecordId,
    target_record_id: targetRecordId,
    active_item_id: item.active_item_id || "",
    source_record_id: sourceRecordId,
    source_work_type: String(item.source_work_type || item.converted_from_work_type || workType),
    converted_from_work_type: String(item.converted_from_work_type || ""),
    title: draftValue(edit, "title", item.title || item.content || ""),
    specialty: cleanDisplayText(draftValue(edit, "specialty", item.specialty || "")),
    building: item.building || "",
    building_codes: Array.isArray(item.building_codes) ? item.building_codes : [],
    maintenance_cycle: draftValue(edit, "maintenance_cycle", item.maintenance_cycle || ""),
    level: draftValue(edit, "level", item.level || ""),
    start_time: startTime,
    end_time: endTime,
    location: draftValue(edit, "location", item.location || ""),
    content: draftValue(edit, "content", item.content || ""),
    reason: draftValue(edit, "reason", item.reason || ""),
    impact: draftValue(edit, "impact", item.impact || ""),
    progress: draftValue(edit, "progress", item.progress || ""),
    zhihang_involved: workType === "change" ? Boolean(edit.zhihang_involved) : false,
    zhihang_record_id: workType === "change" ? draftValue(edit, "zhihang_record_id", item.zhihang_record_id || "") : "",
    zhihang_title: workType === "change" ? draftValue(edit, "zhihang_title", item.zhihang_title || "") : "",
    zhihang_progress: workType === "change" ? draftValue(edit, "zhihang_progress", item.zhihang_progress || "") : "",
    repair_device: workType === "repair" ? draftValue(edit, "repair_device", item.repair_device || "") : "",
    repair_fault: workType === "repair" ? draftValue(edit, "repair_fault", item.repair_fault || "") : "",
    fault_type: workType === "repair" ? draftValue(edit, "fault_type", item.fault_type || "") : "",
    repair_mode: workType === "repair" ? draftValue(edit, "repair_mode", item.repair_mode || "") : "",
    discovery: workType === "repair" ? draftValue(edit, "discovery", item.discovery || "") : "",
    symptom: workType === "repair" ? draftValue(edit, "symptom", item.symptom || "") : "",
    solution: workType === "repair" ? draftValue(edit, "solution", item.solution || "") : "",
    spare_parts: workType === "repair" ? draftValue(edit, "spare_parts", item.spare_parts || "") : "",
    device: workType === "polling" ? draftValue(edit, "device", item.device || "") : "",
    cabinet: workType === "power" ? draftValue(edit, "cabinet", item.cabinet || "") : "",
    quantity: workType === "power" ? draftValue(edit, "quantity", item.quantity || "") : "",
    fault_time: workType === "repair" ? endTime : "",
    expected_time: workType === "repair" ? startTime : "",
    extra_images: ongoingExtraImagesForUpload(item),
    operation_id: opId(`${item.active_item_id || targetRecordId || sourceRecordId}:${action}`),
  };
}

function chooseCandidateInModal(candidates: Dict[], label: string, title: string): Promise<Dict | null> {
  if (!candidates.length) return Promise.resolve(null);
  if (candidates.length === 1) return Promise.resolve(candidates[0]);
  if (resolveOngoingBindSelection) closeOngoingBindSelection(null);
  const visibleCandidates = candidates.slice(0, 50);
  const firstId = changeTargetCandidateId(visibleCandidates[0]);
  ongoingBindSelection.value = {
    label,
    title,
    candidates: visibleCandidates,
  };
  selectedOngoingBindId.value = firstId;
  hoveredOngoingBindId.value = firstId;
  return new Promise((resolve) => {
    resolveOngoingBindSelection = resolve;
  });
}

async function bindOngoingTarget(item: Dict): Promise<void> {
  const lineKey = ongoingLineKey(item);
  const edit = ongoingDraft(item);
  const workType = String(item.work_type || "maintenance");
  const title = draftValue(edit, "title", item.title || item.content || "");
  if (!title) {
    rememberJob(lineKey, { text: "缺少标题，无法查找目标记录", status: "failed", phase: "failed" });
    return;
  }
  rememberJob(lineKey, { text: "正在查找目标记录", status: "busy", phase: "binding" });
  try {
    const data = await api(workType === "change" ? "/api/change-target-candidates" : "/api/notice-target-candidates", {
      method: "POST",
      body: JSON.stringify({
        work_type: workType,
        scope: currentScope.value || "ALL",
        title,
        start_time: draftValue(edit, "start_time", ongoingTimeRange(item).start),
        end_time: draftValue(edit, "end_time", ongoingTimeRange(item).end),
        action: "update",
      }),
    });
    const candidates = Array.isArray(data.candidates) ? data.candidates : [];
    const sourceCandidates = Array.isArray(data.source_candidates) ? data.source_candidates : [];
    const selected = await chooseCandidateInModal(
      candidates,
      workTypeLabel(workType),
      `找到 ${Number(data.total_matched ?? candidates.length)} 条同名目标记录，当前显示 ${candidates.length} 条，请选择要关联到当前进行中通告的一条。`,
    );
    if (!selected) {
      rememberJob(lineKey, {
        text: candidates.length ? "已取消目标记录关联" : "未找到同名目标记录",
        status: candidates.length ? "" : "failed",
        phase: candidates.length ? "" : "failed",
      });
      return;
    }
    let candidate = selected;
    if (workType === "change") {
      const confirmed = await api("/api/change-target-candidates/confirm", {
        method: "POST",
        body: JSON.stringify({
          scope: currentScope.value || "ALL",
          title,
          start_time: draftValue(edit, "start_time", ongoingTimeRange(item).start),
          end_time: draftValue(edit, "end_time", ongoingTimeRange(item).end),
          action: "update",
          record_id: changeTargetCandidateId(selected),
        }),
      });
      candidate = confirmed.candidate || selected;
    }
    const source = sourceCandidates[0] || {};
    const next = applyChangeTargetCandidateDefaults({ ...edit }, candidate);
    next.target_record_id = candidate.record_id || candidate.target_record_id || "";
    if (source.source_record_id || source.record_id) {
      next.source_record_id = source.source_record_id || source.record_id;
    }
    ongoingEdits.set(lineKey, next);
    rememberJob(lineKey, { text: "已关联目标记录，可继续更新或结束", status: "success", phase: "bound" });
  } catch (error: any) {
    rememberJob(lineKey, { text: error?.message || "关联目标记录失败", status: "failed", phase: "failed" });
  }
}

async function sendOngoing(item: Dict, action: string): Promise<void> {
  const key = ongoingLineKey(item);
  const payload = buildOngoingPayload(item, action);
  const missingUpload = missingUploadFields(String(payload.work_type || item.work_type || ""), payload);
  if (missingUpload.length) {
    rememberJob(key, { text: uploadFieldsMissingText(String(payload.work_type || item.work_type || ""), missingUpload), status: "failed", phase: "failed" });
    return;
  }
  if (!validatePayloadDuration(payload, key)) return;
  if (action === "end" && ongoingEndRequiresSitePhoto(item) && !ongoingPhotoCount(item)) {
    rememberJob(key, { text: "结束通告前必须添加至少一张现场照片", status: "failed", phase: "failed" });
    return;
  }
  await sendAction(payload, key);
}

async function sendAction(payload: Dict, lineKey: string): Promise<void> {
  try {
    rememberJob(lineKey, { text: "已受理，正在进入后台任务", status: "busy", phase: "accepted" });
    const data = await api("/api/workbench-actions", { method: "POST", body: JSON.stringify(payload) });
    rememberJob(lineKey, {
      job_id: data.job_id,
      payload,
      text: payloadSendsPersonalMessage(payload) ? "已受理，正在发送飞书消息" : "已受理，等待主界面显示",
      status: "busy",
      phase: data.initial_phase || "accepted",
    });
    watchJob(data.job_id, lineKey);
  } catch (error: any) {
    rememberJob(lineKey, { text: friendlyFailureText(error, "提交失败"), status: "failed", phase: "failed" });
  }
}

function rememberJob(key: string, patch: Dict): void {
  jobStates.set(key, { ...(jobStates.get(key) || {}), ...patch, updated_at: new Date().toISOString() });
}

function applySuccessfulJobState(lineKey: string): void {
  const state = jobStates.get(lineKey) || {};
  if (state.local_applied) return;
  const payload = state.payload || {};
  const action = String(payload.action || "").toLowerCase();
  if (selectedKeys.has(lineKey)) {
    selectedKeys.delete(lineKey);
    if (isManualKey(lineKey)) drafts.delete(lineKey);
    if (activeDraftKey.value === lineKey) activeDraftKey.value = "";
    saveDrafts();
  }
  if (action === "start") {
    bumpLocalSummary("started", 1);
    bumpLocalSummary("ongoing", 1);
    pendingFocusNotice.value = {
      action,
      work_type: payload.work_type || "",
      target_record_id: payload.target_record_id || payload.record_id || "",
      source_record_id: payload.source_record_id || "",
      title: payload.title || "",
    };
  } else if (action === "update") {
    bumpLocalSummary("updated", 1);
    pendingFocusNotice.value = {
      action,
      work_type: payload.work_type || "",
      target_record_id: payload.target_record_id || payload.record_id || "",
      source_record_id: payload.source_record_id || "",
      title: payload.title || "",
    };
  } else if (action === "end") {
    bumpLocalSummary("ended", 1);
    if (removeOngoingLine(lineKey)) {
      // The row was already removed locally; no additional ongoing delta needed.
    } else {
      bumpLocalSummary("ongoing", -1);
    }
    syncText.value = "已结束，可在今日结束通告或近三天可回退中查看";
  }
  rememberJob(lineKey, { local_applied: true });
}

function handleTerminalJob(lineKey: string, phase: string): void {
  if (phase === "success") {
    applySuccessfulJobState(lineKey);
    scheduleWorkbenchReload(0);
  }
}

function jobText(key: string): string {
  return jobStates.get(key)?.text || "";
}

function jobClass(key: string): string {
  return jobStates.get(key)?.status || "";
}

function isLineBusy(key: string): boolean {
  const phase = jobStates.get(key)?.phase || "";
  return Boolean(phase && !terminalPhase(phase));
}

function applyJobStatusToLine(lineKey: string, job: Dict): void {
  const { phase, status, text } = backendJobStatusPatch(job);
  rememberJob(lineKey, { phase, status, text });
  if (terminalPhase(phase)) {
    clearFallbackPoll(lineKey);
    handleTerminalJob(lineKey, phase);
  }
}

function isJobRealtimeAvailable(): boolean {
  return sseConnected.value
    || Boolean(eventSource.value && eventSource.value.readyState !== EventSource.CLOSED);
}

function isJobStreamStarted(): boolean {
  return Boolean(jobStreamCoordinator || eventSource.value);
}

function isActiveItemsStreamStarted(): boolean {
  return Boolean(activeItemsStreamCoordinator || activeItemsEventSource.value);
}

function watchJob(jobId: string, lineKey: string): void {
  clearFallbackPoll(lineKey);
  if (isJobRealtimeAvailable()) {
    const delay = sseConnected.value ? 15000 : 6000;
    const timer = window.setTimeout(() => {
      fallbackPollTimers.delete(lineKey);
      if (!isLineBusy(lineKey)) return;
      queueJobPoll(jobId, lineKey);
    }, delay);
    fallbackPollTimers.set(lineKey, timer);
    return;
  }
  queueJobPoll(jobId, lineKey);
}

function queueJobPoll(jobId: string, lineKey: string): void {
  if (!jobId || !lineKey || appDisposed) return;
  pollingJobs.set(lineKey, jobId);
  scheduleBatchJobPoll(0);
}

function scheduleBatchJobPoll(delayMs = 2000): void {
  if (appDisposed || batchPollTimer !== null || pollingJobs.size === 0) return;
  batchPollTimer = window.setTimeout(() => {
    batchPollTimer = null;
    void pollJobsBatch();
  }, Math.max(0, delayMs));
}

async function pollJobsBatch(): Promise<void> {
  if (batchPollActive || appDisposed || pollingJobs.size === 0) return;
  batchPollActive = true;
  try {
    const entries = Array.from(pollingJobs.entries());
    const ids = Array.from(new Set(entries.map(([, jobId]) => jobId).filter(Boolean))).slice(0, 100);
    if (!ids.length) return;
    try {
      const data = await api(`/api/jobs/batch?ids=${encodeURIComponent(ids.join(","))}`);
      const byId = new Map<string, Dict>();
      for (const item of Array.isArray(data.items) ? data.items : []) {
        if (item?.job_id) byId.set(String(item.job_id), item);
      }
      const missing = new Set<string>(Array.isArray(data.missing) ? data.missing.map(String) : []);
      const denied = new Set<string>(Array.isArray(data.denied) ? data.denied.map(String) : []);
      for (const [lineKey, jobId] of entries) {
        const job = byId.get(jobId);
        if (job) {
          applyJobStatusToLine(lineKey, job);
          continue;
        }
        if (missing.has(jobId) || denied.has(jobId)) {
          rememberJob(lineKey, {
            phase: "failed",
            status: "failed",
            text: denied.has(jobId) ? "无权查看该任务状态" : "任务状态已丢失，请刷新核对后重试",
          });
          clearFallbackPoll(lineKey);
        }
      }
    } catch {
      for (const key of pollingJobs.keys()) {
        rememberJob(key, { text: "后台处理中，等待状态同步", status: "busy" });
      }
    }
  } finally {
    batchPollActive = false;
    if (!appDisposed && pollingJobs.size > 0) scheduleBatchJobPoll(2000);
  }
}

function applyJobStreamPayload(job: Dict): void {
  if (!job?.job_id) return;
  for (const [key, value] of jobStates.entries()) {
    if (value.job_id === job.job_id) {
      applyJobStatusToLine(key, job);
    }
  }
}

function openDirectJobEventSource(coordinator: CrossTabStreamCoordinator<Dict> | null): void {
  closeDirectJobEventSource();
  if (appDisposed) return;
  try {
    const source = new EventSource("/api/jobs/stream");
    const handleJobEvent = (event: MessageEvent) => {
      let payload: Dict;
      try {
        payload = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      const job = payload.job || payload;
      applyJobStreamPayload(job);
      coordinator?.broadcast({ kind: "job", job });
    };
    source.onopen = () => {
      sseConnected.value = true;
      coordinator?.broadcast({ kind: "status", connected: true });
    };
    source.onmessage = handleJobEvent;
    source.addEventListener("job", handleJobEvent);
    source.onerror = () => {
      sseConnected.value = false;
      coordinator?.broadcast({ kind: "status", connected: false });
      if (eventSource.value === source && source.readyState === EventSource.CLOSED) {
        eventSource.value = null;
        if (!appDisposed && sseReconnectTimer === null && (!coordinator || coordinator.isLeader())) {
          sseReconnectTimer = window.setTimeout(() => {
            sseReconnectTimer = null;
            openDirectJobEventSource(coordinator);
          }, 5000);
        }
      }
    };
    eventSource.value = source;
  } catch {
    eventSource.value = null;
    sseConnected.value = false;
    coordinator?.broadcast({ kind: "status", connected: false });
  }
}

function startJobSse(): void {
  if (appDisposed || jobStreamCoordinator) return;
  const coordinator = createCrossTabStreamCoordinator<Dict>({
    channelName: "clipflow-jobs-stream-v1",
    leaderKey: "clipflow:jobs-stream-leader",
    heartbeatMs: streamLeaderHeartbeatMs,
    leaderTtlMs: streamLeaderTtlMs,
  });
  jobStreamCoordinator = coordinator;
  coordinator.start(
    (message) => {
      if (message.kind === "job") {
        applyJobStreamPayload(message.job || {});
      } else if (message.kind === "status") {
        sharedJobStreamAvailable.value = Boolean(message.connected);
      }
    },
    (leader) => {
      if (leader) {
        sharedJobStreamAvailable.value = false;
        openDirectJobEventSource(coordinator);
      } else {
        closeDirectJobEventSource();
        sseConnected.value = false;
        sharedJobStreamAvailable.value = coordinator.supported;
      }
    },
  );
}

function closeDirectActiveItemsEventSource(): void {
  if (activeItemsEventSource.value) activeItemsEventSource.value.close();
  activeItemsEventSource.value = null;
  if (activeItemsReconnectTimer !== null) {
    window.clearTimeout(activeItemsReconnectTimer);
    activeItemsReconnectTimer = null;
  }
}

function applyActiveItemsStreamPayload(payload: Dict, scope: string): void {
  if (String(payload.scope || "") !== scope) return;
  const signature = String(payload.display_signature || payload.scope_signature || "");
  if (!signature) return;
  if (!lastActiveItemsSignature) {
    lastActiveItemsSignature = signature;
    return;
  }
  if (signature !== lastActiveItemsSignature) {
    lastActiveItemsSignature = signature;
    scheduleWorkbenchReload(isUserEditing() ? 3000 : 250);
  }
}

function openDirectActiveItemsEventSource(
  scope: string,
  coordinator: CrossTabStreamCoordinator<Dict> | null,
): void {
  closeDirectActiveItemsEventSource();
  if (appDisposed || !isWorkbench.value || !scope) return;
  try {
    const source = new EventSource(`/api/qt-active-items/stream?scope=${encodeURIComponent(scope)}`);
    source.onopen = () => {
      activeItemsConnected.value = true;
      coordinator?.broadcast({ kind: "status", connected: true, scope });
    };
    source.addEventListener("qt_active_items", (event: MessageEvent) => {
      let payload: Dict;
      try {
        payload = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
      applyActiveItemsStreamPayload(payload, scope);
      coordinator?.broadcast({ kind: "qt_active_items", payload, scope });
    });
    source.addEventListener("error", () => {
      activeItemsConnected.value = false;
      coordinator?.broadcast({ kind: "status", connected: false, scope });
      if (activeItemsEventSource.value === source && source.readyState === EventSource.CLOSED) {
        activeItemsEventSource.value = null;
        if (
          !appDisposed
          && isWorkbench.value
          && activeItemsReconnectTimer === null
          && (!coordinator || coordinator.isLeader())
        ) {
          activeItemsReconnectTimer = window.setTimeout(() => {
            activeItemsReconnectTimer = null;
            openDirectActiveItemsEventSource(scope, coordinator);
          }, 5000);
        }
      }
    });
    activeItemsEventSource.value = source;
  } catch {
    activeItemsEventSource.value = null;
    activeItemsConnected.value = false;
    coordinator?.broadcast({ kind: "status", connected: false, scope });
  }
}

function startActiveItemsSse(): void {
  if (appDisposed || !isWorkbench.value || !currentScope.value) return;
  const scope = currentScope.value || "ALL";
  if (activeItemsStreamCoordinator && activeItemsStreamScope === scope) return;
  stopActiveItemsSse();
  lastActiveItemsSignature = "";
  activeItemsStreamScope = scope;
  const coordinator = createCrossTabStreamCoordinator<Dict>({
    channelName: `clipflow-active-items-stream-v1:${scope}`,
    leaderKey: `clipflow:active-items-stream-leader:${scope}`,
    heartbeatMs: streamLeaderHeartbeatMs,
    leaderTtlMs: streamLeaderTtlMs,
  });
  activeItemsStreamCoordinator = coordinator;
  coordinator.start(
    (message) => {
      if (String(message.scope || scope) !== scope) return;
      if (message.kind === "qt_active_items") {
        applyActiveItemsStreamPayload(message.payload || {}, scope);
      } else if (message.kind === "status") {
        sharedActiveItemsStreamAvailable.value = Boolean(message.connected);
      }
    },
    (leader) => {
      if (leader) {
        sharedActiveItemsStreamAvailable.value = false;
        openDirectActiveItemsEventSource(scope, coordinator);
      } else {
        closeDirectActiveItemsEventSource();
        activeItemsConnected.value = false;
        sharedActiveItemsStreamAvailable.value = coordinator.supported;
      }
    },
  );
}

function stopActiveItemsSse(): void {
  if (activeItemsStreamCoordinator) {
    activeItemsStreamCoordinator.stop();
    activeItemsStreamCoordinator = null;
  }
  closeDirectActiveItemsEventSource();
  activeItemsConnected.value = false;
  sharedActiveItemsStreamAvailable.value = false;
  activeItemsUpdatePending.value = false;
  lastActiveItemsSignature = "";
  activeItemsStreamScope = "";
}

function requestActionConfirm(options: {
  tone?: ActionConfirmTone;
  kicker?: string;
  title: string;
  message: string;
  details?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
}): Promise<boolean> {
  return new Promise((resolve) => {
    const tone = options.tone || "primary";
    Object.assign(actionConfirm, {
      open: true,
      tone,
      kicker: options.kicker || "操作确认",
      title: options.title,
      message: options.message,
      details: Array.isArray(options.details) ? options.details : [],
      confirmLabel: options.confirmLabel || "确认",
      cancelLabel: options.cancelLabel || "取消",
      confirmClass: tone === "danger" ? "danger" : tone === "warning" ? "green" : "blue",
      resolve,
    });
  });
}

function resolveActionConfirm(confirmed: boolean): void {
  const resolver = actionConfirm.resolve;
  Object.assign(actionConfirm, {
    open: false,
    title: "",
    message: "",
    details: [],
    resolve: undefined,
  });
  resolver?.(confirmed);
}

async function deleteOngoing(item: Dict): Promise<void> {
  const key = ongoingLineKey(item);
  const targetRecordId = targetRecordIdForOngoing(item);
  const sourceRecordId = sourceRecordIdForOngoing(item, targetRecordId);
  const confirmed = await requestActionConfirm({
    tone: "danger",
    kicker: "删除进行中通告",
    title: ongoingTitle(item),
    message: "确认删除这条进行中通告？",
    details: [
      "会同步移除前端和 Qt 界面的进行中条目。",
      targetRecordId ? "会尝试删除对应目标多维记录。" : "该条暂无目标多维记录，只删除本地显示状态。",
      "删除后可在近三天可回退中恢复。",
    ],
    confirmLabel: "确认删除",
  });
  if (!confirmed) return;
  try {
    rememberJob(key, { text: "删除中", status: "busy", phase: "deleting" });
    await api("/api/ongoing-items/delete", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        work_type: item.work_type || "maintenance",
        notice_type: item.notice_type || "",
        active_item_id: item.active_item_id || "",
        record_id: targetRecordId,
        target_record_id: targetRecordId,
        source_record_id: sourceRecordId,
        title: item.title || item.content || "",
        building: item.building || "",
        building_codes: Array.isArray(item.building_codes) ? item.building_codes : [],
      }),
    });
    removeOngoingLine(key);
    rememberJob(key, { text: "已删除", status: "success", phase: "success" });
    await loadWorkbench();
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "删除失败", status: "failed", phase: "failed" });
  }
}

async function applyUndo(item: Dict): Promise<void> {
  const undoId = String(item.undo_id || "").trim();
  if (!undoId) return;
  const key = undoLineKey(item);
  if (isLineBusy(key)) return;
  const title = String(item.title || ongoingTitle(item) || "该通告").trim();
  const label = String(item.undo_label || "回退上一步").trim();
  const confirmed = await requestActionConfirm({
    tone: "warning",
    kicker: "回退通告操作",
    title,
    message: `确认执行${label}？`,
    details: [
      "会同步恢复 SQLite 本地状态。",
      "会刷新前端和 Qt 展示。",
      "会按回退记录恢复或重建目标多维记录。",
    ],
    confirmLabel: label,
  });
  if (!confirmed) return;
  try {
    rememberJob(key, { text: "已受理，正在回退", status: "busy", phase: "undo_queued" });
    const data = await api(`/api/notice-undo/${encodeURIComponent(undoId)}/apply`, {
      method: "POST",
      body: JSON.stringify({ scope: currentScope.value || "ALL" }),
    });
    rememberJob(key, {
      job_id: data.job_id,
      payload: { action: "undo", undo_id: undoId },
      text: "已受理，正在回退",
      status: "busy",
      phase: data.initial_phase || "undo_queued",
    });
    watchJob(data.job_id, key);
  } catch (error: any) {
    rememberJob(key, { text: error?.message || "回退失败", status: "failed", phase: "failed" });
  }
}

function ongoingTitle(item: Dict): string {
  if ((item.work_type || "maintenance") === "repair") {
    const source = records.value.find((record) => record.record_id === item.source_record_id);
    if (source) return titleForRecord(source);
  }
  return item.title || item.content || "未命名通告";
}

function ongoingMeta(item: Dict): string {
  const boundText = item.binding_status === "needs_binding" ? "待自动关联目标记录" : "";
  return [item.building, item.specialty, item.maintenance_cycle, item.time_str || item.start_time, boundText].filter(Boolean).join(" · ");
}

function ongoingCompactSummary(item: Dict): string {
  const edit = ongoingEdits.get(ongoingLineKey(item)) || {};
  const progress = draftValue(edit, "progress", item.progress || "");
  const location = draftValue(edit, "location", item.location || "");
  return [location, progress].filter(Boolean).join(" · ");
}

async function refreshRepair(): Promise<void> {
  if (repairRefreshing.value || refreshCooldown.repair) return;
  startRefreshCooldown("repair");
  repairRefreshing.value = true;
  try {
    await api(`/api/repair-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
  } catch (error: any) {
    syncText.value = error?.message || "刷新检修失败";
    scheduleWorkbenchRetry(syncText.value);
  } finally {
    repairRefreshing.value = false;
  }
}

async function refreshChange(): Promise<void> {
  if (changeRefreshing.value || refreshCooldown.change) return;
  startRefreshCooldown("change");
  changeRefreshing.value = true;
  try {
    await api(`/api/change-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
  } catch (error: any) {
    syncText.value = error?.message || "刷新变更失败";
    scheduleWorkbenchRetry(syncText.value);
  } finally {
    changeRefreshing.value = false;
  }
}

async function logout(): Promise<void> {
  clearAuthKeepalive();
  stopRealtimeConnections();
  await api("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
  window.location.href = "/";
}

async function submitPermissionRequest(): Promise<void> {
  if (!permissionPanelRequestableScopes.value.length) {
    permissionRequest.message = "当前没有可申请的楼栋权限。";
    return;
  }
  permissionBusy.value = true;
  try {
    const data = await api("/api/auth/permission-requests", {
      method: "POST",
      body: JSON.stringify({ scopes: permissionRequest.scopes, reason: permissionRequest.reason }),
    });
    permissionRequest.requestId = data.request_id || data.request?.request_id || "";
    permissionRequest.message = "申请已发送给管理员，请输入验证码。";
  } catch (error: any) {
    permissionRequest.message = error?.message || "提交失败";
  } finally {
    permissionBusy.value = false;
  }
}

function updatePermissionRequest(patch: Partial<typeof permissionRequest>): void {
  Object.assign(permissionRequest, patch);
}

async function openAdditionalPermissionRequest(): Promise<void> {
  Object.assign(permissionRequest, {
    scopes: [],
    reason: "",
    code: "",
    requestId: "",
    message: "",
  });
  showPermissionRequestPanel.value = true;
  await loadCurrentPermissionRequest();
}

function closePermissionRequestPanel(): void {
  showPermissionRequestPanel.value = false;
  Object.assign(permissionRequest, {
    scopes: [],
    reason: "",
    code: "",
    requestId: "",
    message: "",
  });
  syncText.value = "请选择功能";
}

async function confirmPermissionRequest(): Promise<void> {
  permissionBusy.value = true;
  try {
    await api("/api/auth/permission-requests/confirm", {
      method: "POST",
      body: JSON.stringify({ request_id: permissionRequest.requestId, code: permissionRequest.code }),
    });
    await loadAuthStatus();
    if (auth.scopeOptions.length) {
      await Promise.all([loadOverview(), loadHandoverLinks()]);
      isWorkbench.value = false;
      showPermissionRequestPanel.value = false;
      Object.assign(permissionRequest, { scopes: [], reason: "", code: "", requestId: "", message: "" });
      syncText.value = "请选择功能";
      if (!isJobStreamStarted()) startJobSse();
    }
  } catch (error: any) {
    permissionRequest.message = error?.message || "验证码错误";
  } finally {
    permissionBusy.value = false;
  }
}

watch(workType, () => {
  activeDraftKey.value = "";
});

onMounted(async () => {
  appDisposed = false;
  pageVisible.value = !document.hidden;
  document.addEventListener("visibilitychange", handleVisibilityChange);
  window.addEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  if (isSignaturePage.value) {
    authChecking.value = false;
    syncText.value = "";
    void loadAuthStatus({ silent: true }).catch(() => null);
    return;
  }
  await loadAuthStatus();
  if (auth.loggedIn && !auth.scopeOptions.length) {
    await loadCurrentPermissionRequest();
  }
  if (auth.loggedIn && auth.scopeOptions.length) {
    await Promise.all([loadOverview(), loadHandoverLinks()]);
    if (currentScope.value && !isEngineerMopPage.value && !isHistoryMemoryPage.value) {
      isWorkbench.value = true;
      loadDrafts();
      await loadWorkbench();
      startActiveItemsSse();
    } else {
      syncText.value = "请选择功能";
    }
    startJobSse();
  }
});

onBeforeUnmount(() => {
  appDisposed = true;
  document.removeEventListener("visibilitychange", handleVisibilityChange);
  window.removeEventListener(AUTH_EXPIRED_EVENT, handleGlobalAuthExpired);
  clearAuthKeepalive();
  clearWorkbenchRetry();
  stopRealtimeConnections();
  if (workbenchRefreshTimer !== null) window.clearTimeout(workbenchRefreshTimer);
  for (const timer of refreshCooldownTimers.values()) window.clearTimeout(timer);
  refreshCooldownTimers.clear();
  for (const timer of fallbackPollTimers.values()) window.clearTimeout(timer);
  fallbackPollTimers.clear();
  if (batchPollTimer !== null) {
    window.clearTimeout(batchPollTimer);
    batchPollTimer = null;
  }
  pollingJobs.clear();
});
</script>

<style scoped>
:global(*) {
  box-sizing: border-box;
}

.app-shell {
  min-height: 100vh;
  background: #eef3f8;
  color: #0f172a;
  font-family: "Microsoft YaHei", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.app-shell.signature-link-shell {
  min-height: 100dvh;
  overflow: hidden;
}

.topbar {
  position: sticky;
  top: 0;
  z-index: 10;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 14px 20px;
  border-bottom: 1px solid #dbe3ee;
  background: rgba(255, 255, 255, 0.94);
  backdrop-filter: blur(10px);
}

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.brand-logo {
  width: 132px;
  height: 42px;
  flex: 0 0 auto;
  object-fit: contain;
}

h1,
h2,
p {
  margin: 0;
}

h1 {
  font-size: 22px;
  line-height: 1.25;
}

.brand p,
.hint {
  color: #64748b;
  font-size: 13px;
}

.topbar-actions,
.row-actions,
.card-actions,
.toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.scope-switch {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  white-space: nowrap;
  color: #475569;
  font-size: 13px;
}

.scope-switch select {
  width: auto;
  min-width: 104px;
  max-width: 148px;
  padding: 7px 30px 7px 10px;
  font-size: 14px;
}

.specialty-filter {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 38px;
  padding: 4px 6px 4px 12px;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  background: #ffffff;
  color: #475569;
  font-size: 13px;
  white-space: nowrap;
}

.specialty-filter span {
  color: #64748b;
  font-weight: 700;
}

.specialty-filter select {
  width: auto;
  min-width: 112px;
  max-width: 168px;
  padding: 7px 30px 7px 10px;
  font-size: 14px;
}

.btn,
button,
a.btn {
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 8px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 14px;
  line-height: 1;
  text-decoration: none;
  cursor: pointer;
}

.btn:disabled,
button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.small {
  padding: 6px 9px;
  font-size: 12px;
}

.btn.blue {
  border-color: #2563eb;
  background: #2563eb;
  color: #ffffff;
}

.btn.green {
  border-color: #16a34a;
  background: #16a34a;
  color: #ffffff;
}

.btn.danger {
  border-color: #dc2626;
  background: #dc2626;
  color: #ffffff;
}

.danger-text {
  color: #b91c1c;
}

.user-chip {
  padding: 7px 10px;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  background: #f8fafc;
  color: #334155;
  font-size: 13px;
}

.status-banner {
  position: sticky;
  top: 71px;
  z-index: 9;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  min-height: 38px;
  padding: 8px 16px;
  border-bottom: 1px solid #dbe3ee;
  background: #ffffff;
  color: #334155;
  font-size: 13px;
}

.status-banner.info {
  background: #f8fafc;
}

.status-banner.warning {
  border-color: #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.status-banner.failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #991b1b;
}

.center-state {
  width: min(720px, calc(100vw - 32px));
  margin: 80px auto;
  display: grid;
  gap: 14px;
  justify-items: start;
  padding: 28px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.spinner {
  width: 28px;
  height: 28px;
  border: 3px solid #dbeafe;
  border-top-color: #2563eb;
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.home-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 14px;
  padding: 24px;
}

.scope-card {
  min-height: 112px;
  display: grid;
  align-content: center;
  gap: 10px;
  padding: 18px;
  text-align: left;
  border: 1px solid #dbe3ee;
  background: #ffffff;
}

.scope-card strong {
  font-size: 22px;
}

.scope-card span {
  color: #64748b;
  line-height: 1.5;
}

.workbench {
  padding: 16px;
}

.loading-line {
  margin-bottom: 12px;
  padding: 9px 12px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 13px;
}

.summary-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}

.summary-strip article,
.panel,
.paste-panel {
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.summary-strip article {
  padding: 10px 14px;
}

.summary-strip span {
  color: #64748b;
  font-size: 13px;
}

.summary-strip strong {
  display: block;
  margin-top: 3px;
  font-size: 21px;
}

.toolbar,
.paste-panel {
  margin-bottom: 12px;
  padding: 10px;
}

.draft-save-status {
  margin-left: auto;
  padding: 6px 9px;
  border: 1px solid #dbe3ee;
  border-radius: 999px;
  background: #f8fafc;
  color: #64748b;
  font-size: 12px;
  white-space: nowrap;
}

.draft-save-status.failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #991b1b;
}

.target-choice-panel {
  display: grid;
  gap: 8px;
  margin-top: 10px;
  padding: 10px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #eff6ff;
}

.target-choice-panel p {
  margin: 4px 0 0;
  color: #475569;
  font-size: 13px;
}

.target-choice-panel .target-count-line {
  color: #2563eb;
  font-size: 12px;
}

.target-choice-layout {
  display: grid;
  grid-template-columns: minmax(220px, 0.9fr) minmax(280px, 1.1fr);
  gap: 10px;
  align-items: start;
}

.target-choice-column {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.candidate-search {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 12px;
}

.candidate-search input {
  width: 100%;
  min-width: 0;
  border: 1px solid #dbe3ee;
  border-radius: 7px;
  padding: 8px 10px;
  background: #ffffff;
  color: #0f172a;
  font-size: 13px;
  outline: none;
}

.candidate-search input:focus {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.1);
}

.candidate-count {
  margin: 0;
  color: #64748b;
  font-size: 12px;
}

.target-choice-list {
  display: grid;
  gap: 7px;
  max-height: 360px;
  overflow: auto;
}

.target-choice {
  display: grid;
  gap: 4px;
  width: 100%;
  padding: 9px 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
}

.target-choice:hover {
  border-color: #2563eb;
}

.target-choice.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.target-choice span {
  color: #64748b;
  font-size: 12px;
}

.target-choice small {
  color: #2563eb;
  font-size: 12px;
}

.target-detail-popover {
  position: sticky;
  top: 10px;
  padding: 10px;
  border: 1px solid #bfdbfe;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
}

.target-detail-head {
  display: grid;
  gap: 3px;
  margin-bottom: 8px;
}

.target-detail-head span {
  color: #64748b;
  font-size: 12px;
}

.target-detail-head small {
  color: #2563eb;
  font-size: 12px;
}

.target-detail-grid {
  display: grid;
  grid-template-columns: 96px 1fr;
  gap: 6px 10px;
  max-height: 300px;
  margin: 0;
  overflow: auto;
}

.target-detail-grid dt {
  color: #64748b;
  font-size: 12px;
}

.target-detail-grid dd {
  margin: 0;
  color: #0f172a;
  font-size: 13px;
  line-height: 1.45;
  word-break: break-word;
}

.target-confirm {
  width: 100%;
  margin-top: 10px;
}

.source-choice-panel {
  display: grid;
  gap: 8px;
  padding-top: 10px;
  border-top: 1px solid #bfdbfe;
}

.source-choice-panel p {
  margin: 4px 0 0;
  color: #475569;
  font-size: 13px;
}

.source-choice-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 8px;
}

.source-choice {
  display: grid;
  gap: 4px;
  padding: 8px 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
}

.source-choice.active {
  border-color: #0f766e;
  background: #f0fdfa;
  box-shadow: 0 0 0 2px rgba(15, 118, 110, 0.1);
}

.source-choice span {
  color: #64748b;
  font-size: 12px;
}

.segmented {
  display: inline-flex;
  gap: 4px;
  padding: 4px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
  background: #f8fafc;
}

.segmented button {
  border: 0;
  background: transparent;
}

.segmented button.active {
  background: #2563eb;
  color: #ffffff;
}

.manual-create {
  position: relative;
  display: inline-flex;
}

.manual-type-popover {
  position: absolute;
  top: calc(100% + 8px);
  left: 0;
  z-index: 30;
  width: min(360px, calc(100vw - 32px));
  padding: 12px;
  border: 1px solid #cbd5e1;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 14px 36px rgba(15, 23, 42, 0.14);
  display: grid;
  gap: 8px;
}

.manual-type-popover strong {
  font-size: 14px;
  color: #0f172a;
}

.manual-type-popover p {
  margin: 0;
  color: #64748b;
  font-size: 12px;
  line-height: 1.5;
}

.manual-recent {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  padding: 8px;
  border: 1px solid #dbeafe;
  border-radius: 8px;
  background: #eff6ff;
}

.manual-recent span {
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 700;
}

.manual-recent button {
  min-height: 32px;
  padding: 6px 9px;
  border-color: #bfdbfe;
  background: #ffffff;
  color: #1d4ed8;
  font-size: 13px;
}

.manual-type-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.manual-type-grid button {
  min-height: 52px;
  padding: 9px 10px;
  display: grid;
  gap: 4px;
  justify-items: start;
  text-align: left;
  border-color: #dbe3ee;
  background: #f8fafc;
}

.manual-type-grid button:hover {
  border-color: #2563eb;
  background: #eff6ff;
}

.manual-type-grid span {
  font-weight: 700;
}

.manual-type-grid small {
  color: #64748b;
  font-size: 12px;
}

.search {
  flex: 1 1 260px;
  min-width: 180px;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 9px 11px;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(280px, 0.88fr) minmax(430px, 1.35fr) minmax(320px, 0.95fr);
  gap: 12px;
  min-height: calc(100vh - 230px);
}

.panel {
  min-width: 0;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.panel-head {
  position: sticky;
  top: 0;
  z-index: 2;
  margin: -12px -12px 0;
  padding: 12px 12px 8px;
  border-bottom: 1px solid #eef2f7;
  background: rgba(255, 255, 255, 0.96);
  backdrop-filter: blur(6px);
}

.panel-head h2 {
  font-size: 17px;
}

.panel-head.compact {
  position: static;
  margin: 4px 0 0;
  padding: 10px 0 6px;
  backdrop-filter: none;
}

.panel-head.compact h3 {
  font-size: 14px;
}

.panel-head span {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
  font-size: 12px;
}

.draft-stack,
.ongoing-list {
  overflow: auto;
  display: grid;
  align-content: start;
  gap: 10px;
  scroll-behavior: smooth;
}

.closed-today {
  display: grid;
  gap: 8px;
}

.closed-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  padding: 9px 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #fbfdff;
}

.closed-card p {
  margin: 3px 0 0;
  color: #64748b;
  font-size: 12px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
select,
textarea {
  width: 100%;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 7px 9px;
  background: #ffffff;
  color: #0f172a;
  font: inherit;
}

textarea {
  min-height: 58px;
  resize: vertical;
}

.card-actions {
  margin-top: 10px;
}

.job-line {
  flex: 1 1 auto;
  color: #64748b;
  font-size: 13px;
}

.job-line.busy {
  color: #1d4ed8;
}

.job-line.success {
  color: #15803d;
}

.job-line.failed {
  color: #b91c1c;
}

.empty-block {
  display: grid;
  place-items: center;
  min-height: 140px;
  padding: 18px;
  color: #64748b;
  text-align: center;
  line-height: 1.7;
  background: #f8fafc;
  border-radius: 6px;
}

.paste-panel textarea,
.request-panel textarea {
  min-height: 100px;
}

.scope-checks {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.verify-box {
  display: grid;
  gap: 8px;
}

@media (max-width: 1120px) {
  .workspace {
    grid-template-columns: 1fr;
  }

  .panel {
    min-height: 360px;
  }
}

@media (max-width: 720px) {
  .topbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .summary-strip {
    grid-template-columns: 1fr;
  }
}

/* VNET blue-white production skin */
.app-shell {
  --brand-blue: #0757d7;
  --brand-blue-2: #0d7df2;
  --brand-blue-dark: #06349a;
  --brand-cyan: #21c6e7;
  --ink: #071634;
  --muted: #64748b;
  --line: #d8e7f8;
  --panel: rgba(255, 255, 255, 0.95);
  --panel-soft: #f7fbff;
  --shadow: 0 18px 42px rgba(22, 78, 151, 0.12);
  --shadow-strong: 0 24px 68px rgba(8, 52, 132, 0.18);
  --radius-panel: 20px;
  --radius-card: 18px;
  --radius-control: 12px;
  --radius-popover: 22px;
  background:
    linear-gradient(180deg, #f4f9ff 0, #f8fbff 260px, #eef6ff 100%),
    radial-gradient(circle at 18% 18%, rgba(31, 129, 255, 0.12), transparent 28%);
  color: var(--ink);
}

.topbar {
  min-height: 128px;
  padding: 22px 38px;
  border-bottom: 0;
  flex-wrap: wrap;
  background:
    linear-gradient(90deg, rgba(4, 46, 145, 0.98), rgba(10, 103, 224, 0.98) 46%, rgba(0, 116, 236, 0.96)),
    linear-gradient(180deg, rgba(255, 255, 255, 0.12), transparent);
  box-shadow: 0 16px 42px rgba(3, 55, 140, 0.18);
  overflow: hidden;
  isolation: isolate;
}

.topbar::before {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background:
    linear-gradient(90deg, transparent 0 18%, rgba(255, 255, 255, 0.18) 18.05% 18.14%, transparent 18.2%),
    linear-gradient(135deg, transparent 0 58%, rgba(255, 255, 255, 0.18) 58.1% 58.25%, transparent 58.35%),
    repeating-linear-gradient(90deg, rgba(255, 255, 255, 0.08) 0 1px, transparent 1px 56px),
    repeating-linear-gradient(0deg, rgba(255, 255, 255, 0.06) 0 1px, transparent 1px 42px);
  mask-image: linear-gradient(90deg, transparent, #000 22%, #000 86%, transparent);
  opacity: 0.42;
}

.topbar::after {
  content: "";
  position: absolute;
  left: 56%;
  bottom: 8px;
  width: 74px;
  height: 96px;
  pointer-events: none;
  opacity: 0.34;
  background:
    linear-gradient(#fff, #fff) 34px 18px / 6px 56px no-repeat,
    linear-gradient(#fff, #fff) 24px 74px / 26px 5px no-repeat,
    linear-gradient(#fff, #fff) 20px 84px / 34px 5px no-repeat,
    radial-gradient(circle at 37px 12px, transparent 13px, rgba(255, 255, 255, 0.95) 14px 15px, transparent 16px),
    linear-gradient(135deg, transparent 36%, rgba(255, 255, 255, 0.95) 36.5% 41%, transparent 41.5%),
    linear-gradient(45deg, transparent 36%, rgba(255, 255, 255, 0.95) 36.5% 41%, transparent 41.5%);
}

.brand {
  position: relative;
  z-index: 1;
  flex: 0 0 auto;
  gap: 26px;
}

.brand > div {
  min-width: 0;
}

.brand-logo {
  width: 146px;
  height: 56px;
  padding-right: 26px;
  border-right: 1px solid rgba(255, 255, 255, 0.42);
  filter: brightness(0) invert(1);
}

h1 {
  color: #ffffff;
  font-size: 28px;
  font-weight: 800;
  letter-spacing: 0;
  white-space: nowrap;
}

.brand p,
.hint {
  margin-top: 8px;
  color: rgba(255, 255, 255, 0.78);
  font-size: 15px;
}

.topbar-actions {
  position: relative;
  z-index: 1;
  flex: 1 1 560px;
  min-width: 360px;
  margin-left: auto;
  justify-content: flex-end;
}

.topbar .btn,
.topbar button,
.topbar a.btn,
.topbar .user-chip,
.scope-switch {
  min-height: 44px;
  border-color: rgba(255, 255, 255, 0.35);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.13);
  color: #ffffff;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
  backdrop-filter: blur(10px);
}

.topbar .btn:hover,
.topbar button:hover,
.topbar a.btn:hover {
  background: rgba(255, 255, 255, 0.21);
}

.topbar .danger-text {
  border-color: rgba(255, 255, 255, 0.88);
  background: #ffffff;
  color: #d03535;
  font-weight: 700;
}

.scope-switch {
  display: inline-flex;
  padding: 4px 8px 4px 12px;
}

.scope-switch select {
  border-color: rgba(255, 255, 255, 0.42);
  background: rgba(255, 255, 255, 0.96);
  color: var(--brand-blue-dark);
  font-weight: 700;
}

.status-banner {
  top: 128px;
  border: 1px solid var(--line);
  border-left: 0;
  border-right: 0;
  background:
    linear-gradient(90deg, rgba(255, 255, 255, 0.96), rgba(247, 251, 255, 0.96)),
    linear-gradient(90deg, rgba(7, 87, 215, 0.08), transparent 42%);
  color: #24456f;
  box-shadow: 0 8px 24px rgba(36, 86, 148, 0.08);
}

.status-banner.info {
  background: #f5faff;
}

.status-banner.warning {
  border-color: #d8e7f8;
  background:
    linear-gradient(90deg, rgba(255, 255, 255, 0.96), rgba(247, 251, 255, 0.96)),
    linear-gradient(90deg, rgba(245, 158, 11, 0.14), transparent 42%);
  color: #8a4b08;
}

.status-banner.failed {
  border-color: #d8e7f8;
  background:
    linear-gradient(90deg, rgba(255, 255, 255, 0.96), rgba(247, 251, 255, 0.96)),
    linear-gradient(90deg, rgba(220, 38, 38, 0.13), transparent 42%);
  color: #9f1d1d;
}

.status-banner .btn {
  min-height: 28px;
  padding: 6px 12px;
  border-color: #c5d9f2;
  background: #ffffff;
  color: #0757d7;
  box-shadow: 0 6px 16px rgba(22, 78, 151, 0.1);
}

.page-status {
  width: min(1180px, calc(100vw - 48px));
  margin: 16px auto 0;
  padding: 10px 14px;
  border: 1px solid #d8e7f8;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.82);
  color: #31577f;
  font-size: 13px;
  box-shadow: 0 10px 28px rgba(22, 78, 151, 0.08);
}

.center-state,
.summary-strip article,
.panel,
.paste-panel,
.target-detail-popover,
.closed-card {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--panel);
  box-shadow: var(--shadow);
}

.center-state {
  margin-top: 48px;
  padding: 34px;
}

.workbench {
  padding: 28px 34px 34px;
}

.summary-strip {
  gap: 16px;
  margin-bottom: 18px;
}

.summary-strip article {
  position: relative;
  min-height: 94px;
  padding: 18px 18px 16px 78px;
  overflow: hidden;
  isolation: isolate;
}

.summary-strip article::before {
  content: "";
  position: absolute;
  left: 18px;
  top: 19px;
  width: 42px;
  height: 42px;
  border-radius: 12px;
  background: linear-gradient(135deg, var(--brand-blue), var(--brand-blue-2));
  box-shadow: 0 10px 22px rgba(25, 105, 224, 0.26);
}

.summary-strip article::after {
  content: "";
  position: absolute;
  right: -38px;
  bottom: -54px;
  z-index: 0;
  width: 140px;
  height: 104px;
  opacity: 0.44;
  background:
    repeating-linear-gradient(0deg, rgba(22, 120, 255, 0.12) 0 1px, transparent 1px 15px),
    repeating-linear-gradient(90deg, rgba(22, 120, 255, 0.08) 0 1px, transparent 1px 15px);
  transform: rotate(-14deg);
}

.summary-strip article > * {
  position: relative;
  z-index: 1;
}

.summary-strip article:nth-child(2)::before {
  background: linear-gradient(135deg, #11a8c9, #36d0e5);
}

.summary-strip article:nth-child(3)::before {
  background: linear-gradient(135deg, #22b66b, #3dd887);
}

.summary-strip article:nth-child(4)::before {
  background: linear-gradient(135deg, #6b78ff, #2f75ff);
}

.summary-strip span {
  color: var(--muted);
  font-size: 14px;
}

.summary-strip strong {
  color: var(--brand-blue-dark);
  font-size: 26px;
}

.toolbar,
.paste-panel {
  margin-bottom: 16px;
  padding: 14px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 32px rgba(22, 78, 151, 0.08);
}

.workspace {
  gap: 18px;
}

.panel {
  position: relative;
  overflow: hidden;
  padding: 14px;
}

.panel-head {
  margin: -14px -14px 0;
  padding: 14px 16px 10px;
  border-bottom-color: #e7f0fb;
  border-radius: 14px 14px 0 0;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(248, 251, 255, 0.96));
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.04);
}

.panel-head h2 {
  color: #09204a;
  font-size: 18px;
  font-weight: 800;
}

.panel-head h2::before {
  content: "";
  display: inline-block;
  width: 4px;
  height: 17px;
  margin-right: 8px;
  border-radius: 999px;
  vertical-align: -3px;
  background: linear-gradient(180deg, var(--brand-blue), var(--brand-cyan));
  box-shadow: 0 6px 14px rgba(22, 120, 255, 0.18);
}

.panel-head span,
.card-title span {
  background: #eaf3ff;
  color: #0757d7;
  font-weight: 700;
}

.btn,
button,
a.btn {
  min-height: 36px;
  border-color: #c5d9f2;
  border-radius: 9px;
  background: #ffffff;
  color: #09204a;
  font-weight: 650;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease, background 0.12s ease;
}

.btn:focus-visible,
button:focus-visible,
a.btn:focus-visible,
input:focus-visible,
select:focus-visible,
textarea:focus-visible {
  outline: 3px solid rgba(22, 120, 255, 0.22);
  outline-offset: 2px;
}

.btn:hover:not(:disabled),
button:hover:not(:disabled),
a.btn:hover {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.13);
  transform: translateY(-1px);
}

.btn.blue,
.segmented button.active {
  border-color: transparent;
  background: linear-gradient(135deg, var(--brand-blue), #1678ff);
  color: #ffffff;
  box-shadow: 0 12px 24px rgba(20, 103, 226, 0.24);
}

.btn.green {
  border-color: transparent;
  background: linear-gradient(135deg, #16a36d, #2fd083);
}

.btn.danger {
  border-color: transparent;
  background: linear-gradient(135deg, #dc2626, #f05656);
}

.segmented {
  padding: 5px;
  border-color: #cde0f6;
  border-radius: 12px;
  background: #edf5ff;
}

.segmented button {
  border-radius: 9px;
}

input,
select,
textarea {
  border-color: #c8dcf3;
  border-radius: 9px;
  background: #fbfdff;
  transition: border-color 0.14s ease, box-shadow 0.14s ease, background-color 0.14s ease;
}

input:focus,
select:focus,
textarea:focus {
  border-color: var(--brand-blue-2);
  outline: none;
  box-shadow: 0 0 0 3px rgba(22, 120, 255, 0.12);
}

.empty-block,
.virtual-list,
.target-choice-panel,
.manual-type-popover,
.closed-card {
  background: #f7fbff;
}

.draft-stack,
.ongoing-list {
  padding-right: 2px;
  scrollbar-color: #9cc7ff #eef6ff;
  scrollbar-width: thin;
}

.draft-stack::-webkit-scrollbar,
.ongoing-list::-webkit-scrollbar {
  width: 8px;
}

.draft-stack::-webkit-scrollbar-track,
.ongoing-list::-webkit-scrollbar-track {
  background: #eef6ff;
  border-radius: 999px;
}

.draft-stack::-webkit-scrollbar-thumb,
.ongoing-list::-webkit-scrollbar-thumb {
  background: linear-gradient(180deg, #9cc7ff, #1678ff);
  border-radius: 999px;
}

.closed-card {
  border-radius: 12px;
  box-shadow: 0 8px 18px rgba(22, 78, 151, 0.06);
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.closed-card:hover {
  border-color: #9cc7ff;
  background: #f5faff;
  box-shadow: 0 12px 26px rgba(22, 78, 151, 0.1);
}

.manual-type-popover,
.target-choice-panel,
.target-detail-popover {
  border-color: var(--line);
  border-radius: 14px;
  box-shadow: var(--shadow-strong);
}

/* Rounded VNET polish pass */
.center-state,
.summary-strip article,
.panel,
.paste-panel,
.toolbar,
.page-status,
.target-choice-panel,
.manual-type-popover,
.closed-card,
.empty-block {
  border-radius: var(--radius-panel);
}

.target-detail-popover {
  border-radius: var(--radius-popover);
}

.panel-head {
  border-radius: var(--radius-panel) var(--radius-panel) 0 0;
}

.topbar .btn,
.topbar button,
.topbar a.btn,
.topbar .user-chip,
.scope-switch,
.scope-switch select,
.btn,
button,
a.btn,
input,
select,
textarea,
.segmented button,
.status-banner .btn {
  border-radius: var(--radius-control);
}

.segmented {
  border-radius: 16px;
}

.summary-strip article::before {
  border-radius: 14px;
}

/* Softer text integration for graphic surfaces */
.summary-strip span,
.panel-head span,
.card-title span,
.status-banner,
.page-status {
  letter-spacing: 0;
}

.summary-strip span {
  color: #5f7189;
  font-weight: 650;
}

.summary-strip strong {
  color: #0a4db8;
  font-size: 25px;
  font-weight: 820;
  letter-spacing: 0;
}

.panel-head h2 {
  font-weight: 820;
  letter-spacing: 0;
}

.panel-head span,
.card-title span {
  border: 1px solid #d8e7f8;
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-weight: 720;
}

.btn,
button,
a.btn {
  font-weight: 720;
}

/* Panorama construction-management polish: lighter slate canvas, softer cards, rounder controls */
.app-shell {
  --line: #dbe6f5;
  --panel: rgba(255, 255, 255, 0.97);
  --panel-soft: #f8fbff;
  --shadow: 0 14px 34px rgba(20, 70, 138, 0.09);
  --shadow-strong: 0 22px 58px rgba(15, 58, 128, 0.15);
  --radius-panel: 24px;
  --radius-card: 20px;
  --radius-control: 14px;
  background:
    linear-gradient(180deg, #f7faff 0, #f9fbfd 280px, #eef5fc 100%),
    radial-gradient(circle at 14% 12%, rgba(48, 128, 255, 0.12), transparent 30%),
    radial-gradient(circle at 86% 16%, rgba(0, 183, 215, 0.08), transparent 26%);
}

.topbar {
  min-height: 116px;
  padding: 20px 40px;
  border-bottom: 1px solid rgba(191, 219, 254, 0.25);
  box-shadow: 0 14px 34px rgba(3, 55, 140, 0.16);
}

.topbar::before {
  opacity: 0.34;
}

.topbar::after {
  bottom: 5px;
  opacity: 0.28;
}

.brand {
  gap: 24px;
}

.brand-logo {
  width: 142px;
  height: 54px;
}

h1 {
  font-size: 29px;
  font-weight: 820;
}

.brand p,
.hint {
  color: rgba(255, 255, 255, 0.72);
  font-size: 14px;
}

.topbar .btn,
.topbar button,
.topbar a.btn,
.topbar .user-chip,
.scope-switch {
  min-height: 42px;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.12);
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.2),
    0 10px 22px rgba(4, 46, 145, 0.08);
}

.topbar .danger-text {
  border-radius: 16px;
  box-shadow: 0 12px 24px rgba(4, 46, 145, 0.12);
}

.status-banner {
  top: 116px;
}

.page-status {
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.9);
}

.workbench {
  padding: 30px 40px 46px;
}

.summary-strip {
  gap: 18px;
  margin-bottom: 20px;
}

.summary-strip article {
  min-height: 88px;
  padding: 18px 18px 16px 74px;
  border-color: rgba(207, 224, 255, 0.9);
  box-shadow: var(--shadow);
}

.summary-strip article::before {
  width: 40px;
  height: 40px;
  border-radius: 16px;
}

.summary-strip strong {
  font-size: 24px;
}

.toolbar,
.paste-panel {
  border-color: rgba(207, 224, 255, 0.92);
  background: rgba(255, 255, 255, 0.9);
  box-shadow: var(--shadow);
  backdrop-filter: blur(10px);
}

.toolbar {
  position: relative;
  z-index: 20;
}

.panel {
  border-color: rgba(207, 224, 255, 0.92);
  box-shadow: var(--shadow);
}

.panel-head {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(248, 251, 255, 0.98)),
    linear-gradient(90deg, rgba(48, 128, 255, 0.06), transparent 42%);
}

.panel-head h2 {
  font-size: 17px;
}

.panel-head span,
.card-title span {
  border-color: #d8e7f8;
  border-radius: 999px;
  background: rgba(239, 246, 255, 0.9);
  color: #155dfc;
}

.segmented {
  border-color: #d8e7f8;
  background: rgba(239, 246, 255, 0.86);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9);
}

.btn,
button,
a.btn,
input,
select,
textarea {
  border-radius: var(--radius-control);
}

.btn,
button,
a.btn {
  box-shadow: 0 8px 18px rgba(15, 86, 228, 0.06);
}

.btn.blue,
.segmented button.active {
  background: linear-gradient(135deg, #155dfc, #3080ff);
  box-shadow: 0 12px 24px rgba(21, 93, 252, 0.24);
}

input,
select,
textarea {
  background: rgba(255, 255, 255, 0.96);
}

.empty-block,
.virtual-list,
.target-choice-panel,
.manual-type-popover,
.closed-card {
  background: rgba(248, 251, 255, 0.94);
}

.closed-card {
  border-radius: 18px;
}

.manual-type-popover {
  z-index: 80;
}

/* Panorama construction-management command-center skin */
.app-shell {
  --brand-blue: #1e63ff;
  --brand-blue-2: #005bff;
  --brand-blue-dark: #012a7d;
  --brand-cyan: #00b7d7;
  --ink: #0f172a;
  --muted: #64748b;
  --line: #d8e5f7;
  --panel: rgba(255, 255, 255, 0.86);
  --panel-soft: rgba(255, 255, 255, 0.72);
  --shadow: 0 14px 34px rgba(0, 47, 135, 0.1);
  --shadow-strong: 0 24px 64px rgba(0, 47, 135, 0.18);
  --radius-panel: 22px;
  --radius-card: 18px;
  --radius-control: 14px;
  background: linear-gradient(180deg, #eef4ff 0, #f8fbff 44%, #eef5ff 100%);
}

.topbar {
  min-height: 112px;
  padding: 20px 40px;
  background:
    linear-gradient(115deg, #064fc5 0%, #00359b 52%, #012a7d 100%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.14), transparent);
  box-shadow: 0 18px 42px rgba(0, 47, 135, 0.28);
}

.topbar::before {
  background:
    linear-gradient(90deg, transparent 0 18%, rgba(255, 255, 255, 0.16) 18.05% 18.14%, transparent 18.2%),
    linear-gradient(135deg, transparent 0 58%, rgba(255, 255, 255, 0.14) 58.1% 58.25%, transparent 58.35%),
    repeating-linear-gradient(90deg, rgba(255, 255, 255, 0.07) 0 1px, transparent 1px 62px),
    repeating-linear-gradient(0deg, rgba(255, 255, 255, 0.05) 0 1px, transparent 1px 46px);
}

.topbar::after {
  opacity: 0.24;
}

h1 {
  font-size: 28px;
  font-weight: 650;
}

.brand p,
.hint {
  color: rgba(255, 255, 255, 0.7);
}

.topbar .btn,
.topbar button,
.topbar a.btn,
.topbar .user-chip,
.scope-switch {
  border-color: rgba(255, 255, 255, 0.32);
  background: rgba(255, 255, 255, 0.14);
  color: #ffffff;
  backdrop-filter: blur(10px);
}

.topbar .btn:hover,
.topbar button:hover,
.topbar a.btn:hover {
  background: rgba(255, 255, 255, 0.2);
}

.topbar .danger-text {
  background: #ffffff;
  color: #d03535;
}

.status-banner {
  top: 112px;
  min-height: 34px;
  background: rgba(255, 255, 255, 0.88);
  backdrop-filter: blur(10px);
}

.workbench {
  width: min(1800px, 100%);
  margin: 0 auto;
  padding: 28px 32px 42px;
}

.summary-strip article,
.panel,
.paste-panel,
.toolbar,
.target-choice-panel,
.manual-type-popover,
.closed-card,
.page-status {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.82);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.summary-strip article {
  min-height: 86px;
}

.summary-strip article::before {
  background: linear-gradient(135deg, #1e63ff, #005bff);
}

.summary-strip article:nth-child(2)::before {
  background: linear-gradient(135deg, #0ea5e9, #00b7d7);
}

.summary-strip article:nth-child(3)::before {
  background: linear-gradient(135deg, #059669, #10b981);
}

.summary-strip article:nth-child(4)::before {
  background: linear-gradient(135deg, #475569, #1e63ff);
}

.panel-head {
  background: rgba(255, 255, 255, 0.86);
}

.panel-head h2 {
  font-size: 16px;
  font-weight: 650;
}

.panel-head h2::before {
  background: linear-gradient(180deg, #1e63ff, #00b7d7);
}

.panel-head span,
.card-title span,
.draft-save-status {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
}

.segmented {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.72);
}

.btn.blue,
.segmented button.active {
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.24);
}

.btn.blue:hover:not(:disabled),
.segmented button.active:hover:not(:disabled) {
  background: #1554df;
}

.action-confirm-backdrop {
  position: fixed;
  inset: 0;
  z-index: 90;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(9, 32, 74, 0.34);
  backdrop-filter: blur(8px);
}

.action-confirm-modal {
  width: min(520px, 100%);
  overflow: hidden;
  border: 1px solid #d8e5f7;
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 28px 80px rgba(0, 47, 135, 0.22);
}

.action-confirm-modal header {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  padding: 18px 20px;
  border-bottom: 1px solid #edf3fb;
  background: linear-gradient(135deg, #f8fbff, #ffffff);
}

.action-confirm-modal header span {
  display: block;
  color: #155dfc;
  font-size: 12px;
  font-weight: 850;
}

.action-confirm-modal header strong {
  display: block;
  margin-top: 5px;
  color: #09204a;
  font-size: 18px;
  line-height: 1.35;
}

.action-confirm-modal.tone-danger header span {
  color: #e11d48;
}

.action-confirm-modal.tone-warning header span {
  color: #b45309;
}

.action-confirm-modal p {
  margin: 0;
  padding: 18px 20px 8px;
  color: #334155;
  line-height: 1.65;
}

.action-confirm-modal ul {
  display: grid;
  gap: 8px;
  margin: 0;
  padding: 0 20px 18px 38px;
  color: #64748b;
  font-size: 14px;
  line-height: 1.55;
}

.action-confirm-modal footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  padding: 14px 20px 18px;
  border-top: 1px solid #edf3fb;
  background: rgba(248, 251, 255, 0.72);
}

.icon-btn {
  display: inline-grid;
  width: 34px;
  height: 34px;
  place-items: center;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  color: #64748b;
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
}

.btn.green {
  background: #059669;
}

.btn.danger {
  background: #e11d48;
}

input,
select,
textarea,
.search {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.86);
}

.specialty-filter {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.78);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.82);
}

.specialty-filter span {
  color: #64748b;
}

.specialty-filter select {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.92);
  color: #0f172a;
}

input:focus,
select:focus,
textarea:focus,
.search:focus {
  border-color: #005bff;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.topbar,
.status-banner {
  position: relative;
  top: auto;
}

@media (max-width: 920px) {
  .topbar {
    min-height: auto;
    padding: 20px;
  }

  .brand-logo {
    width: 118px;
    height: 46px;
    padding-right: 18px;
  }

  h1 {
    font-size: 22px;
  }

  .status-banner {
    top: 0;
  }
}
</style>
