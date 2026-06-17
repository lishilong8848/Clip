<template>
  <section class="engineer-mop" :class="{ 'preview-open': previewMode }">
    <header v-if="!previewMode" class="mop-head">
      <div>
        <strong>工程师 MOP 填写</strong>
        <p>选择当天维保通告，绑定已有 MOP 表格后在右侧预览 Sheet 内容。</p>
      </div>
      <div class="head-actions">
        <label v-if="scopeOptions.length" class="scope-select">
          楼栋
          <select v-model="scope" :disabled="loading || previewLoading">
            <option v-for="item in scopeOptions" :key="item.value" :value="normalizeScope(item.value)">
              {{ item.label }}
            </option>
          </select>
        </label>
        <button class="btn ghost" type="button" :disabled="loading" @click="loadPage">刷新</button>
        <a class="btn ghost" href="/">功能选择</a>
      </div>
    </header>

    <div v-if="checking" class="notice-box">正在检查登录状态...</div>
    <div v-else-if="!loggedIn" class="notice-box">
      请先登录飞书后再使用工程师 MOP 页面。
      <a class="btn blue" :href="loginUrl">飞书登录</a>
    </div>

    <template v-else>
      <div v-if="message" class="message" :class="{ failed: messageType === 'failed', success: messageType === 'success' }">
        {{ message }}
      </div>
      <div v-if="warnings.length" class="warning-list">
        <span v-for="warning in warnings" :key="warning">{{ warning }}</span>
      </div>

      <section v-if="previewMode && preview" class="mop-preview-page">
        <header class="preview-head">
          <button class="btn ghost preview-back" type="button" @click="backToBinding">返回</button>
          <div>
            <strong>{{ previewTitle }}</strong>
            <p>
              <template v-if="selectedNotice">{{ selectedNotice.title }}</template>
              <template v-if="activeSheet"> · {{ activeSheet.name }} · {{ activeSheet.row_count || 0 }} 行</template>
            </p>
          </div>
        </header>
        <div v-if="preview.local_file" class="mop-file-status">
          <span>已下载到本机</span>
          <strong>{{ preview.local_file.relative_path || preview.local_file.file_name }}</strong>
          <small>{{ preview.local_file.path }}</small>
        </div>
        <div
          v-if="activeSheet && (activeSheet.is_cover || (!activeSheetCheckboxCells.length && !activeSheetMaintenanceFields.length))"
          class="mop-detect-panel"
        >
          <div class="detect-summary">
            <span :class="{ muted: !activeSheet.is_cover }">{{ activeSheet.is_cover ? "封面页" : "非封面页" }}</span>
            <strong>选择项 {{ activeSheetCheckboxCells.length }} 个</strong>
            <strong>日期/维护字段 {{ activeSheetMaintenanceFields.length }} 个</strong>
            <em v-if="!activeSheet.is_cover && mopFilledCount">已填写 {{ mopFilledCount }} 项</em>
          </div>
          <div v-if="activeSheet.is_cover" class="detect-empty">当前 Sheet 识别为封面页，不提取填写项。</div>
          <div v-else-if="!activeSheetCheckboxCells.length && !activeSheetMaintenanceFields.length" class="detect-empty">
            当前 Sheet 暂未识别到选择项、日期占位或维护实施/审核字段；普通单元格仍可点击编辑。
          </div>
        </div>
        <section v-if="activeSheet && !activeSheet.is_cover" class="mop-sign-panel">
          <div class="sign-panel-head">
            <div>
              <strong>维护人员签名</strong>
              <p>选择签名表人员后手写保存，签名会写入该人员记录的“手写签名”附件字段。</p>
            </div>
            <div class="sign-role-tabs">
              <button
                type="button"
                :class="{ active: signatureRole === 'implementer' }"
                @click="signatureRole = 'implementer'"
              >
                维护实施人
              </button>
              <button
                type="button"
                :class="{ active: signatureRole === 'auditor' }"
                @click="signatureRole = 'auditor'"
              >
                维护审核人
              </button>
            </div>
          </div>
          <div class="sign-workspace">
            <div class="sign-people">
              <label class="field">
                <span>搜索签名人员</span>
                <div class="inline-search">
                  <input
                    v-model="signatureSearch"
                    enterkeyhint="search"
                    placeholder="姓名、工号、楼栋"
                    @keyup.enter="loadSignaturePeople()"
                  />
                  <button
                    class="btn ghost signature-refresh"
                    type="button"
                    :disabled="signatureLoading"
                    title="重新读取签名人员"
                    @click="loadSignaturePeople()"
                  >
                    {{ signatureLoading ? "读取中" : "刷新" }}
                  </button>
                </div>
                <small class="search-inline-status">{{ signatureSearchStatus }}</small>
              </label>
              <div class="sign-person-list">
                <button
                  v-for="person in signaturePeople"
                  :key="person.record_id"
                  type="button"
                  class="sign-person"
                  :class="{
                    active: (signatureSelectedRecords[signatureRole] || []).includes(person.record_id),
                    current: person.record_id === activeSignatureRecordId
                  }"
                  @click="selectSignaturePerson(person.record_id)"
                >
                  <span>{{ personInitial(person) }}</span>
                  <strong>{{ person.name || "未命名人员" }}</strong>
                  <small>
                    <template v-if="person.employee_no">{{ person.employee_no }} · </template>
                    <template v-if="person.building">{{ person.building }} · </template>
                    {{ person.position || person.team || "签名人员" }}
                  </small>
                  <em :class="{ ok: personHasUsableSignature(person) }">{{ personHasUsableSignature(person) ? "已有签名" : "待签名" }}</em>
                </button>
                <div v-if="!signatureLoading && !signaturePeople.length" class="empty-box compact">
                  暂未找到签名人员。
                </div>
              </div>
            </div>
            <div class="sign-canvas-card">
              <div class="selected-sign-person">
                <span>{{ signatureRole === 'implementer' ? '维护实施人' : '维护审核人' }}</span>
                <strong>{{ selectedRoleSignaturePeople.length ? `已选 ${selectedRoleSignaturePeople.length} 人` : "请选择人员" }}</strong>
                <small>点击左侧人员会加入当前角色；已有签名会直接显示，可继续手写覆盖。</small>
                <div v-if="selectedRoleSignaturePeople.length" class="selected-signatures">
                  <span
                    v-for="person in selectedRoleSignaturePeople"
                    :key="`${signatureRole}:${person.record_id}`"
                    class="selected-signature-chip"
                    :class="{ active: person.record_id === activeSignatureRecordId }"
                    @click="setActiveSignaturePerson(person.record_id)"
                  >
                    <img
                      v-if="personHasUsableSignature(person)"
                      :src="person.signature_preview_url"
                      alt="已有签名"
                      @error="handleSignatureImageError(person.record_id)"
                    />
                    <strong>{{ person.name || "未命名" }}</strong>
                    <button type="button" @click.stop="removeSignaturePerson(signatureRole, person.record_id)">移除</button>
                  </span>
                </div>
              </div>
              <div class="mop-sign-canvas" :class="{ disabled: !activeSignaturePerson }">
                <button
                  v-if="activeSignaturePerson"
                  class="sign-clear-inline"
                  type="button"
                  :disabled="signatureSaving"
                  @click="clearSignatureCanvas"
                >
                  清空
                </button>
                <img
                  v-if="personHasUsableSignature(activeSignaturePerson) && !signatureHasInk"
                  class="mop-sign-preview-img"
                  :src="activeSignaturePerson?.signature_preview_url"
                  alt="已有手写签名"
                  @error="handleSignatureImageError(activeSignaturePerson?.record_id)"
                />
                <canvas
                  ref="signatureCanvasRef"
                  aria-label="MOP手写签名区域"
                  @pointerdown="startSignatureDraw"
                  @pointermove="moveSignatureDraw"
                  @pointerup="endSignatureDraw"
                  @pointercancel="endSignatureDraw"
                  @pointerleave="endSignatureDraw"
                ></canvas>
                <div v-if="!signatureHasInk && !personHasUsableSignature(activeSignaturePerson)" class="sign-placeholder">在此处手写签名</div>
              </div>
              <div class="mop-completion-panel">
                <span
                  v-for="item in mopCompletionItems"
                  :key="item.key"
                  :class="{ done: item.done, pending: !item.done }"
                >
                  <strong>{{ item.label }}</strong>
                  <em>{{ item.text }}</em>
                </span>
              </div>
              <div class="sign-actions">
                <span class="sign-status" :class="{ failed: signatureMessageType === 'failed', success: signatureMessageType === 'success' }">
                  {{ signatureMessage || signatureRoleHint }}
                </span>
                <div class="action-group">
                  <strong>签名库</strong>
                  <div>
                    <button
                      class="btn ghost"
                      type="button"
                      :disabled="signatureLinkSending || !activeSignaturePerson || !activeSignaturePerson.open_id"
                      :title="signatureLinkDisabledReason"
                      @click="sendSignatureLink"
                    >
                      {{ signatureLinkSending ? "发送中" : "发送签名链接" }}
                    </button>
                    <button
                      class="btn blue"
                      type="button"
                      :disabled="signatureSaving || Boolean(saveSignatureDisabledReason)"
                      :title="saveSignatureDisabledReason"
                      @click="saveMopSignature"
                    >
                      {{ signatureSaving ? "保存中" : "保存签名" }}
                    </button>
                  </div>
                  <small v-if="saveSignatureDisabledReason">{{ saveSignatureDisabledReason }}</small>
                </div>
                <div class="action-group file-action-group">
                  <strong>当前 MOP 文件</strong>
                  <div>
                    <button
                      class="btn blue"
                      type="button"
                      :disabled="mopFillSaving || Boolean(fillMopDisabledReason)"
                      :title="fillMopDisabledReason"
                      @click="fillMopSignatures"
                    >
                      {{ mopFillSaving ? "生成中" : "生成已签名 MOP" }}
                    </button>
                    <button
                      v-if="filledMopResult"
                      class="btn ghost reset-clean"
                      type="button"
                      :disabled="mopResetting || mopFillSaving"
                      title="会删除当前已签名文件，并重新下载一份干净 MOP"
                      @click="resetMopSigning"
                    >
                      {{ mopResetting ? "重新下载中" : "重新下载干净 MOP" }}
                    </button>
                  </div>
                  <small v-if="fillMopDisabledReason">{{ fillMopDisabledReason }}</small>
                  <small v-else-if="filledMopResult" class="warning-hint">重新下载会删除当前已签名文件。</small>
                </div>
              </div>
            </div>
          </div>
        </section>
        <div class="sheet-tabs preview-tabs">
          <button
            v-for="sheet in preview.sheets || []"
            :key="sheet.name"
            type="button"
            :class="{ active: sheet.name === activeSheetName }"
            @click="activeSheetName = sheet.name"
          >
            {{ sheet.name }}
          </button>
        </div>
        <div v-if="activeSheet?.truncated" class="sheet-note">
          表格较大，当前预览已限制显示前 {{ activeSheet.row_count }} 行 / {{ activeSheet.column_count }} 列。
        </div>
        <div class="sheet-scroll preview-scroll">
          <div
            v-if="activeSheet && !activeSheet.is_cover"
            class="table-fill-toolbar"
          >
            <span>点击表格单元格可固定显示填写操作</span>
            <button class="btn blue" type="button" :disabled="!activeSheetCheckboxCells.length" @click="markAllCheckboxes('normal')">
              全部正常/开启/已完成
            </button>
            <em v-if="mopFilledCount">已填写 {{ mopFilledCount }} 项</em>
          </div>
          <table v-if="activeSheet">
            <thead>
              <tr>
                <th class="corner-cell"></th>
                <th
                  v-for="colIndex in activeSheetColumnIndexes"
                  :key="`head:${colIndex}`"
                  class="column-head"
                >
                  {{ columnLabel(colIndex) }}
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, rowIndex) in activeSheet.rows || []" :key="rowIndex">
                <th class="row-head">{{ rowIndex + 1 }}</th>
                <template
                  v-for="colIndex in activeSheetColumnIndexes"
                  :key="`${rowIndex}:${colIndex}`"
                >
                  <td
                    v-if="!cellMergeSpan(rowIndex, colIndex).hidden"
                    :rowspan="cellMergeSpan(rowIndex, colIndex).rowspan"
                    :colspan="cellMergeSpan(rowIndex, colIndex).colspan"
                    :class="{
                      merged: cellMergeSpan(rowIndex, colIndex).rowspan > 1 || cellMergeSpan(rowIndex, colIndex).colspan > 1,
                      fillable: Boolean(checkboxCellAt(rowIndex, colIndex)),
                      'field-fillable': Boolean(maintenanceFieldAt(rowIndex, colIndex)),
                      'raw-editable': Boolean(editableCellAt(rowIndex, colIndex)),
                      normal: checkboxCellAt(rowIndex, colIndex) ? checkboxStateLabel(checkboxCellAt(rowIndex, colIndex) as Dict).includes('正常') : false,
                      abnormal: checkboxCellAt(rowIndex, colIndex) ? checkboxStateLabel(checkboxCellAt(rowIndex, colIndex) as Dict).includes('异常') : false
                    }"
                    @click.stop="activateMopCell(rowIndex, colIndex)"
                  >
                    <div v-if="cellSignatures(rowIndex, colIndex).length" class="sheet-cell-signatures">
                      <img
                        v-for="person in cellSignatures(rowIndex, colIndex)"
                        :key="`${rowIndex}:${colIndex}:${person.record_id}`"
                        :src="person.signature_preview_url"
                        :alt="person.name || '签名'"
                      />
                    </div>
                    <template v-else>{{ cellOverrideValue(rowIndex, colIndex) || row[colIndex] || "" }}</template>
                    <div
                      v-if="checkboxCellAt(rowIndex, colIndex)"
                      class="cell-fill-popover"
                      :class="{ pinned: activeMopCellKey === mopCellKey(rowIndex, colIndex) }"
                    >
                      <span>{{ checkboxCellAt(rowIndex, colIndex)?.cell_ref || columnLabel(colIndex) + (rowIndex + 1) }}</span>
                      <button
                        v-for="option in checkboxOptions(checkboxCellAt(rowIndex, colIndex) as Dict)"
                        :key="option.key || option.label"
                        type="button"
                        :class="{ active: checkboxState(checkboxCellAt(rowIndex, colIndex) as Dict) === checkboxOptionValue(option) }"
                        @click.stop="setCheckboxState(checkboxCellAt(rowIndex, colIndex) as Dict, checkboxOptionValue(option))"
                      >
                        {{ option.label || option.key }}
                      </button>
                    </div>
                    <div
                      v-if="maintenanceFieldAt(rowIndex, colIndex)"
                      class="cell-field-popover"
                      :class="{ pinned: activeMopCellKey === mopCellKey(rowIndex, colIndex) }"
                    >
                      <span>{{ maintenanceFieldAt(rowIndex, colIndex)?.label }}</span>
                      <template v-if="maintenanceFieldIsTime(maintenanceFieldAt(rowIndex, colIndex) as Dict)">
                        <input v-model="mopFillDateTime" type="datetime-local" step="3600" />
                        <button type="button" @click.stop="fillMaintenanceField(maintenanceFieldAt(rowIndex, colIndex) as Dict, formatMopDateTime(mopFillDateTime))">填入</button>
                      </template>
                      <template v-else-if="maintenanceFieldIsCompletion(maintenanceFieldAt(rowIndex, colIndex) as Dict)">
                        <button type="button" @click.stop="fillMaintenanceField(maintenanceFieldAt(rowIndex, colIndex) as Dict, '已完成[√] 未完成[ ]')">已完成</button>
                        <button type="button" @click.stop="fillMaintenanceField(maintenanceFieldAt(rowIndex, colIndex) as Dict, '已完成[ ] 未完成[√]')">未完成</button>
                      </template>
                    </div>
                    <div
                      v-else-if="editableCellAt(rowIndex, colIndex) && activeMopCellKey === mopCellKey(rowIndex, colIndex)"
                      class="cell-field-popover raw-cell-popover pinned"
                      @click.stop
                    >
                      <span>{{ columnLabel(colIndex) }}{{ rowIndex + 1 }} 普通单元格</span>
                      <textarea
                        :value="editableCellValue(rowIndex, colIndex)"
                        @input="setEditableCellValue(rowIndex, colIndex, ($event.target as HTMLTextAreaElement).value)"
                      ></textarea>
                      <button type="button" @click.stop="clearEditableCell(rowIndex, colIndex)">还原</button>
                    </div>
                  </td>
                </template>
              </tr>
            </tbody>
          </table>
          <div v-else class="empty-box">该附件没有可显示的 Sheet。</div>
        </div>
      </section>

      <template v-else>
        <section class="mop-summary">
          <article>
            <span>维保通告</span>
            <strong>{{ notices.length }}</strong>
          </article>
          <article>
            <span>已绑定</span>
            <strong>{{ boundNoticeCount }}</strong>
          </article>
          <article>
            <span>MOP 表格</span>
            <strong>{{ mopCandidates.length }}</strong>
          </article>
          <article>
            <span>当前范围</span>
            <strong>{{ scopeLabel }}</strong>
          </article>
        </section>

        <section class="mop-layout">
          <aside class="panel notice-panel">
            <div class="panel-head">
              <div>
                <h2>当天维保通告</h2>
                <p>包含今日进行中与已闭环维保通告。</p>
              </div>
              <span>{{ filteredNotices.length }}</span>
            </div>
            <div class="filters">
              <input v-model="noticeSearch" placeholder="搜索通告、楼栋、专业" />
              <select v-model="noticeStatusFilter">
                <option value="">全部状态</option>
                <option value="ongoing">未完成</option>
                <option value="closed">已完成</option>
                <option value="bound">已绑定</option>
                <option value="unbound">未绑定</option>
              </select>
            </div>
            <div class="list notice-list">
              <button
                v-for="notice in filteredNotices"
                :key="notice.notice_key"
                type="button"
                class="notice-row"
                :class="{ active: notice.notice_key === selectedNoticeKey, closed: notice.status === '已结束' }"
                @click="selectNotice(notice.notice_key)"
              >
                <span class="row-status" :class="{ closed: notice.status === '已结束' }">{{ notice.status || "进行中" }}</span>
                <strong>{{ notice.title || "未命名维保通告" }}</strong>
                <small>
                  {{ notice.building || "未识别楼栋" }}
                  <template v-if="notice.specialty"> · {{ notice.specialty }}</template>
                  <template v-if="notice.maintenance_cycle"> · {{ notice.maintenance_cycle }}</template>
                </small>
                <em v-if="notice.mop_binding">
                  {{ notice.mop_binding.inherited ? "已继承绑定" : "已绑定" }}：{{ notice.mop_binding.mop_title || notice.mop_binding.mop_record_id }}
                </em>
                <em v-else>未绑定 MOP 表格</em>
              </button>
            </div>
          </aside>

          <section class="panel binding-panel">
            <div class="panel-head">
              <div>
                <h2>MOP 对应关系</h2>
                <p>先选左侧通告，再选择现有 MOP 表格。</p>
              </div>
              <span>{{ filteredMopCandidates.length }}</span>
            </div>

            <div v-if="!selectedNotice" class="empty-box">请选择一条维保通告。</div>
            <template v-else>
              <article class="selected-notice">
                <span>{{ selectedNotice.status || "进行中" }}</span>
                <strong>{{ selectedNotice.title }}</strong>
                <p>
                  {{ selectedNotice.building || "-" }}
                  <template v-if="selectedNotice.start_time || selectedNotice.end_time">
                    · {{ selectedNotice.start_time || "未填开始" }} ~ {{ selectedNotice.end_time || "未填结束" }}
                  </template>
                </p>
              </article>

              <label class="field">
                <span>选择 MOP 表格</span>
                <input v-model="mopSearch" placeholder="搜索 MOP 名称、文件编号、专业" />
              </label>
              <div class="mop-candidate-list">
                <button
                  v-for="mop in filteredMopCandidates"
                  :key="mop.record_id"
                  type="button"
                  class="mop-row"
                  :class="{ active: mop.record_id === selectedMopRecordId }"
                  @click="selectMop(mop.record_id)"
                >
                  <strong>{{ mop.title || "未命名 MOP" }}</strong>
                  <small>
                    <template v-if="mop.file_no">{{ mop.file_no }} · </template>
                    <template v-if="mop.specialty">{{ mop.specialty }} · </template>
                    <template v-if="mop.maintenance_type">{{ mop.maintenance_type }} · </template>
                    <template v-if="mop.version">{{ mop.version }} · </template>
                    <template v-if="mop.file_status">{{ mop.file_status }} · </template>
                    附件 {{ mop.attachment_count || 0 }} 个
                  </small>
                </button>
              </div>

              <label v-if="selectedMopAttachments.length" class="field attachment-field">
                <span>表格附件</span>
                <select v-model="selectedAttachmentToken">
                  <option
                    v-for="attachment in selectedMopAttachments"
                    :key="attachmentKey(attachment)"
                    :value="attachment.file_token || attachment.url || attachment.name"
                  >
                    {{ attachment.name || "MOP表格" }}
                  </option>
                </select>
              </label>
              <div v-else-if="selectedMop" class="empty-box warning">
                该 MOP 记录暂未识别到 xlsx/csv 附件。
              </div>

              <div v-if="selectedMop" class="selected-mop-strip">
                <div>
                  <span>已选 MOP 表格</span>
                  <strong>{{ selectedMop.title || "未命名 MOP" }}</strong>
                  <small>
                    <template v-if="selectedAttachment?.name">{{ selectedAttachment.name }}</template>
                    <template v-else-if="selectedMopAttachments.length">请选择表格附件</template>
                    <template v-else>暂无可预览附件</template>
                  </small>
                  <em v-if="mopBindingStatus" class="mop-bind-status success">{{ mopBindingStatus }}</em>
                  <em v-else-if="mopBindingError" class="mop-bind-status failed">{{ mopBindingError }}</em>
                </div>
                <div class="selected-mop-actions">
                  <button
                    class="btn blue"
                    type="button"
                    :disabled="!canPreview || openMopBusy"
                    :title="openMopDisabledReason"
                    @click="startMopPreview"
                  >
                    {{ openMopButtonText }}
                  </button>
                  <small v-if="openMopDisabledReason && !openMopBusy">{{ openMopDisabledReason }}</small>
                </div>
              </div>
            </template>
          </section>
        </section>
      </template>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { requestJson, type Dict } from "../api/client";

type ScopeOption = { value: string; label: string };

const props = defineProps<{
  checking: boolean;
  loggedIn: boolean;
  loginUrl: string;
  scopeOptions: ScopeOption[];
}>();

const scope = ref(normalizeScope(new URLSearchParams(window.location.search).get("scope") || props.scopeOptions[0]?.value || "ALL"));
const loading = ref(false);
const saving = ref(false);
const previewLoading = ref(false);
const message = ref("");
const messageType = ref("");
const mopBindingStatus = ref("");
const mopBindingError = ref("");
const warnings = ref<string[]>([]);
const notices = ref<Dict[]>([]);
const mopCandidates = ref<Dict[]>([]);
const selectedNoticeKey = ref("");
const selectedMopRecordId = ref("");
const selectedAttachmentToken = ref("");
const noticeSearch = ref("");
const noticeStatusFilter = ref("");
const mopSearch = ref("");
const preview = ref<Dict | null>(null);
const previewMode = ref(false);
const activeSheetName = ref("");
const signatureRole = ref<"implementer" | "auditor">("implementer");
const signatureSearch = ref("");
const signaturePeople = ref<Dict[]>([]);
const signaturePeopleById = ref<Record<string, Dict>>({});
const signaturePeopleTotal = ref(0);
const signatureLoading = ref(false);
const signatureSaving = ref(false);
const signatureLinkSending = ref(false);
const mopFillSaving = ref(false);
const mopResetting = ref(false);
const filledMopResult = ref<Dict | null>(null);
const mopCheckboxStates = ref<Record<string, string>>({});
const mopMaintenanceValues = ref<Record<string, string>>({});
const mopCellEdits = ref<Record<string, string>>({});
const mopFillDateTime = ref(defaultDateTimeLocal());
const activeMopCellKey = ref("");
const signatureMessage = ref("");
const signatureMessageType = ref("");
const signatureSelectedRecords = ref<Record<string, string[]>>({ implementer: [], auditor: [] });
const signatureCanvasRef = ref<HTMLCanvasElement | null>(null);
const signatureHasInk = ref(false);
let signatureDrawing = false;
let signatureResizeObserver: ResizeObserver | null = null;
let signatureSearchTimer: ReturnType<typeof setTimeout> | null = null;
let signatureSearchRequestSeq = 0;

const scopeLabel = computed(() => {
  const found = props.scopeOptions.find((item) => normalizeScope(item.value) === scope.value);
  return found?.label || scope.value || "全部";
});

const boundNoticeCount = computed(() => notices.value.filter((item) => item.mop_binding).length);

const filteredNotices = computed(() => {
  const query = compactText(noticeSearch.value);
  return notices.value.filter((item) => {
    const status = String(item.status || "");
    if (noticeStatusFilter.value === "ongoing" && status === "已结束") return false;
    if (noticeStatusFilter.value === "closed" && status !== "已结束") return false;
    if (noticeStatusFilter.value === "bound" && !item.mop_binding) return false;
    if (noticeStatusFilter.value === "unbound" && item.mop_binding) return false;
    if (!query) return true;
    return compactText([
      item.title,
      item.building,
      item.specialty,
      item.maintenance_cycle,
      item.location,
      item.content,
      item.reason,
    ].join(" ")).includes(query);
  });
});

const selectedNotice = computed(() => notices.value.find((item) => item.notice_key === selectedNoticeKey.value) || null);
const selectedMop = computed(() => mopCandidates.value.find((item) => item.record_id === selectedMopRecordId.value) || null);
const selectedMopAttachments = computed(() => {
  const items = selectedMop.value?.attachments;
  return Array.isArray(items) ? items : [];
});
const selectedAttachment = computed(() => {
  const key = selectedAttachmentToken.value;
  return selectedMopAttachments.value.find((item) => attachmentKey(item) === key) || selectedMopAttachments.value[0] || null;
});
const filteredMopCandidates = computed(() => {
  const query = compactText(mopSearch.value);
  if (!query) return mopCandidates.value;
  return mopCandidates.value.filter((item) => compactText([
    item.title,
    ...Object.values(item.fields || {}),
  ].join(" ")).includes(query));
});
const canBind = computed(() => Boolean(selectedNotice.value && selectedMop.value));
const canPreview = computed(() => Boolean(selectedNotice.value && selectedMop.value && selectedAttachment.value));
const openMopDisabledReason = computed(() => {
  if (openMopBusy.value) return "";
  if (!selectedNotice.value) return "请先选择左侧通告";
  if (!selectedMop.value) return "请先选择 MOP 表格";
  if (!selectedAttachment.value) return "当前 MOP 没有可打开的表格附件";
  return "";
});
const openMopBusy = computed(() => saving.value || previewLoading.value);
const openMopButtonText = computed(() => {
  if (saving.value) return "自动绑定中";
  if (previewLoading.value) return "加载表格中";
  return "打开填写";
});
const signatureSearchStatus = computed(() => {
  if (signatureLoading.value) return "搜索中";
  const count = signaturePeople.value.length;
  if (count > 0) {
    return signaturePeopleTotal.value > count ? `已找到 ${count} / ${signaturePeopleTotal.value} 人` : `已找到 ${count} 人`;
  }
  return signatureSearch.value.trim() ? "暂未找到人员" : "输入姓名、工号或楼栋自动搜索";
});
const previewTitle = computed(() => {
  if (!preview.value) return "显示选中的 xlsx/csv 表格内容，支持 Sheet 切换。";
  return `${preview.value.mop_title || selectedMop.value?.title || "MOP表格"} · ${preview.value.attachment?.name || ""}`;
});
const activeSignatureRecordId = computed(() => {
  const records = signatureSelectedRecords.value[signatureRole.value] || [];
  return records[records.length - 1] || "";
});
const selectedRoleSignaturePeople = computed(() => selectedSignaturePeople(signatureRole.value));
const activeSignaturePerson = computed(() => selectedRoleSignaturePeople.value.find((item) => item.record_id === activeSignatureRecordId.value) || null);
const signatureRoleHint = computed(() => {
  const label = signatureRole.value === "implementer" ? "维护实施人" : "维护审核人";
  return activeSignaturePerson.value ? `${label}已选择，可手写后保存。` : `请选择${label}。`;
});
const signatureLinkDisabledReason = computed(() => {
  if (!activeSignaturePerson.value) return "请先选择签名人员";
  if (!activeSignaturePerson.value.open_id) return "该人员缺少 openid，无法发送链接";
  return "";
});
const saveSignatureDisabledReason = computed(() => {
  if (!activeSignaturePerson.value) return "请先选择签名人员";
  if (!signatureHasInk.value) return "请先在签名区域手写签名";
  return "";
});
const canFillMopSignatures = computed(() => {
  if (!preview.value?.local_file?.path || !activeSheet.value) return false;
  return (
    selectedSignaturePeople("implementer").some((person) => personHasUsableSignature(person))
    || selectedSignaturePeople("auditor").some((person) => personHasUsableSignature(person))
  );
});
const fillMopDisabledReason = computed(() => {
  if (!preview.value?.local_file?.path) return "请先打开 MOP 表格";
  if (!activeSheet.value) return "请先选择需要填写的 Sheet";
  if (!canFillMopSignatures.value) return "请至少选择一个已有可用签名的人员";
  return "";
});
const activeSheet = computed(() => {
  const sheets = Array.isArray(preview.value?.sheets) ? preview.value?.sheets : [];
  return sheets.find((item: Dict) => item.name === activeSheetName.value) || sheets[0] || null;
});
const activeSheetCheckboxCells = computed(() => {
  const items = activeSheet.value?.checkbox_cells;
  return Array.isArray(items) ? items : [];
});
const activeSheetMaintenanceFields = computed(() => {
  const items = activeSheet.value?.maintenance_fields;
  return Array.isArray(items) ? items : [];
});
const mopFilledCount = computed(() => {
  const sheetName = String(activeSheet.value?.name || "");
  if (!sheetName) return 0;
  const checkboxCount = activeSheetCheckboxCells.value.filter((cell) => Boolean(mopCheckboxStates.value[checkboxKey(cell)])).length;
  const fieldCount = activeSheetMaintenanceFields.value.filter((field) => Boolean(mopMaintenanceValues.value[maintenanceKey(field)])).length;
  const editCount = Object.keys(mopCellEdits.value).filter((key) => key.startsWith(`${sheetName}:`)).length;
  return checkboxCount + fieldCount + editCount;
});
const mopCompletionItems = computed(() => {
  const implementerCount = selectedSignaturePeople("implementer").filter((person) => personHasUsableSignature(person)).length;
  const auditorCount = selectedSignaturePeople("auditor").filter((person) => personHasUsableSignature(person)).length;
  const fillTotal = activeSheetCheckboxCells.value.length + activeSheetMaintenanceFields.value.length;
  return [
    {
      key: "file",
      label: "表格文件",
      text: preview.value?.local_file?.path ? "已打开" : "未打开",
      done: Boolean(preview.value?.local_file?.path),
    },
    {
      key: "implementer",
      label: "实施人签名",
      text: implementerCount ? `已选 ${implementerCount} 人` : "未选",
      done: implementerCount > 0,
    },
    {
      key: "auditor",
      label: "审核人签名",
      text: auditorCount ? `已选 ${auditorCount} 人` : "未选",
      done: auditorCount > 0,
    },
    {
      key: "fields",
      label: "表格填写",
      text: fillTotal ? `${mopFilledCount.value}/${fillTotal}` : "无待填项",
      done: fillTotal === 0 || mopFilledCount.value >= fillTotal,
    },
  ];
});
const activeSheetColumnIndexes = computed(() => {
  const count = Math.max(0, Number(activeSheet.value?.column_count || 0));
  return Array.from({ length: count }, (_value, index) => index);
});
const activeSheetMergeMap = computed(() => {
  const map = new Map<string, { hidden: boolean; rowspan: number; colspan: number }>();
  const merges = Array.isArray(activeSheet.value?.merges) ? activeSheet.value?.merges : [];
  for (const merge of merges) {
    const row = Number(merge?.row || 0);
    const col = Number(merge?.col || 0);
    const rowspan = Math.max(1, Number(merge?.rowspan || 1));
    const colspan = Math.max(1, Number(merge?.colspan || 1));
    map.set(`${row}:${col}`, { hidden: false, rowspan, colspan });
    for (let rowOffset = 0; rowOffset < rowspan; rowOffset += 1) {
      for (let colOffset = 0; colOffset < colspan; colOffset += 1) {
        if (rowOffset === 0 && colOffset === 0) continue;
        map.set(`${row + rowOffset}:${col + colOffset}`, { hidden: true, rowspan: 1, colspan: 1 });
      }
    }
  }
  return map;
});

function columnLabel(index: number): string {
  const columns = Array.isArray(activeSheet.value?.columns) ? activeSheet.value?.columns : [];
  const existing = String(columns[index] || "").trim();
  if (existing) return existing;
  let value = Math.max(1, index + 1);
  let label = "";
  while (value > 0) {
    const remainder = (value - 1) % 26;
    label = String.fromCharCode(65 + remainder) + label;
    value = Math.floor((value - 1) / 26);
  }
  return label || String(index + 1);
}

function cellMergeSpan(rowIndex: number, colIndex: number): { hidden: boolean; rowspan: number; colspan: number } {
  return activeSheetMergeMap.value.get(`${rowIndex}:${colIndex}`) || { hidden: false, rowspan: 1, colspan: 1 };
}

function normalizeScope(value: string | null | undefined, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function compactText(value: unknown): string {
  return String(value || "").replace(/\s+/g, "").toLowerCase();
}

function escapeRegExp(value: string): string {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function attachmentKey(attachment: Dict): string {
  return String(attachment?.file_token || attachment?.url || attachment?.name || "").trim();
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

function defaultDateTimeLocal(): string {
  const now = new Date();
  now.setMinutes(0, 0, 0);
  return `${now.getFullYear()}-${pad2(now.getMonth() + 1)}-${pad2(now.getDate())}T${pad2(now.getHours())}:00`;
}

function formatMopDateTime(value: string): string {
  const text = String(value || "").trim();
  if (!text) return "";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text.replace("T", " ");
  return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日${pad2(date.getHours())}时`;
}

function checkboxKey(cell: Dict): string {
  return `${activeSheet.value?.name || ""}:${cell.cell_ref || `${cell.row}:${cell.col}`}`;
}

function maintenanceKey(field: Dict): string {
  return `${activeSheet.value?.name || ""}:${field.label || ""}:${field.value_cell_ref || field.label_cell_ref || `${field.row}:${field.value_col}`}`;
}

function checkboxState(cell: Dict): string {
  return mopCheckboxStates.value[checkboxKey(cell)] || "";
}

function checkboxOptions(cell: Dict): Dict[] {
  const items = Array.isArray(cell?.options) ? cell.options : [];
  if (items.length) return items;
  return [
    { key: "normal", label: "正常" },
    { key: "abnormal", label: "异常" },
  ];
}

function checkboxOptionValue(option: Dict): string {
  return String(option?.label || option?.key || "").trim();
}

function checkboxStateLabel(cell: Dict): string {
  const state = checkboxState(cell);
  if (!state) return "";
  return checkboxOptions(cell).find((option) => checkboxOptionValue(option) === state)?.label || state;
}

function mopCellKey(rowIndex: number, colIndex: number): string {
  return `${activeSheetName.value || activeSheet.value?.name || "sheet"}:${rowIndex}:${colIndex}`;
}

function activateMopCell(rowIndex: number, colIndex: number): void {
  if (!checkboxCellAt(rowIndex, colIndex) && !maintenanceFieldAt(rowIndex, colIndex) && !editableCellAt(rowIndex, colIndex)) return;
  activeMopCellKey.value = mopCellKey(rowIndex, colIndex);
}

function setCheckboxState(cell: Dict, state: string): void {
  mopCheckboxStates.value = {
    ...mopCheckboxStates.value,
    [checkboxKey(cell)]: state,
  };
  filledMopResult.value = null;
}

function checkboxCellAt(rowIndex: number, colIndex: number): Dict | null {
  return activeSheetCheckboxCells.value.find((cell) => Number(cell.row) === rowIndex && Number(cell.col) === colIndex) || null;
}

function maintenanceFieldAt(rowIndex: number, colIndex: number): Dict | null {
  return activeSheetMaintenanceFields.value.find((field) => (
    Number(field.row) === rowIndex
    && Number(field.value_col) === colIndex
    && !roleForMaintenanceLabel(field.label)
    && (maintenanceFieldIsTime(field) || maintenanceFieldIsCompletion(field))
  )) || null;
}

function rawCellKey(rowIndex: number, colIndex: number): string {
  return `${activeSheet.value?.name || ""}:${rowIndex}:${colIndex}`;
}

function protectedMopCell(rowIndex: number, colIndex: number): boolean {
  if (!activeSheet.value || activeSheet.value.is_cover) return true;
  if (cellMergeSpan(rowIndex, colIndex).hidden) return true;
  if (checkboxCellAt(rowIndex, colIndex)) return true;
  if (activeSheetMaintenanceFields.value.some((field) => (
    Number(field.row) === rowIndex
    && (Number(field.value_col) === colIndex || Number(field.label_col) === colIndex)
  ))) return true;
  if (cellSignatures(rowIndex, colIndex).length) return true;
  return false;
}

function editableCellAt(rowIndex: number, colIndex: number): boolean {
  return !protectedMopCell(rowIndex, colIndex);
}

function editableCellValue(rowIndex: number, colIndex: number): string {
  const key = rawCellKey(rowIndex, colIndex);
  if (Object.prototype.hasOwnProperty.call(mopCellEdits.value, key)) {
    return mopCellEdits.value[key];
  }
  const row = Array.isArray(activeSheet.value?.rows?.[rowIndex]) ? activeSheet.value?.rows?.[rowIndex] : [];
  return String(row?.[colIndex] || "");
}

function setEditableCellValue(rowIndex: number, colIndex: number, value: string): void {
  if (!editableCellAt(rowIndex, colIndex)) return;
  mopCellEdits.value = {
    ...mopCellEdits.value,
    [rawCellKey(rowIndex, colIndex)]: String(value || ""),
  };
  filledMopResult.value = null;
}

function clearEditableCell(rowIndex: number, colIndex: number): void {
  const next = { ...mopCellEdits.value };
  delete next[rawCellKey(rowIndex, colIndex)];
  mopCellEdits.value = next;
  filledMopResult.value = null;
}

function maintenanceFieldIsTime(field: Dict): boolean {
  const label = String(field?.label || "");
  return field?.kind === "datetime_placeholder"
    || label.includes("维护开始时间")
    || label.includes("维护完成时间")
    || label.includes("审核确认时间")
    || label.includes("日期")
    || label.includes("时间");
}

function maintenanceFieldIsCompletion(field: Dict): boolean {
  return String(field?.label || "").includes("维护完成情况");
}

function fillMaintenanceField(field: Dict, value: string): void {
  const text = String(value || "").trim();
  if (!text) return;
  mopMaintenanceValues.value = {
    ...mopMaintenanceValues.value,
    [maintenanceKey(field)]: text,
  };
  filledMopResult.value = null;
}

function clearMopFillState(options: { clearSignatures?: boolean } = {}): void {
  mopCheckboxStates.value = {};
  mopMaintenanceValues.value = {};
  mopCellEdits.value = {};
  filledMopResult.value = null;
  if (options.clearSignatures) {
    signatureSelectedRecords.value = { implementer: [], auditor: [] };
    signatureRole.value = "implementer";
    clearSignatureCanvas();
  }
}

function markAllCheckboxes(state: string): void {
  const next = { ...mopCheckboxStates.value };
  for (const cell of activeSheetCheckboxCells.value) {
    const options = checkboxOptions(cell);
    const preferred = options.find((option) => String(option.label || "").includes("正常"))
      || options.find((option) => String(option.label || "").includes("开启"))
      || options.find((option) => String(option.label || "").includes("已完成"))
      || options[0];
    next[checkboxKey(cell)] = checkboxOptionValue(preferred) || state;
  }
  mopCheckboxStates.value = next;
  filledMopResult.value = null;
}

function checkboxDisplayValue(cell: Dict): string {
  const state = checkboxState(cell);
  const original = String(cell.value || "");
  if (!state) return original;
  const labels = checkboxOptions(cell).map((option) => String(option.label || option.key || "").trim()).filter(Boolean);
  let text = original || labels.map((label) => `□${label}`).join(" ");
  for (const label of labels) {
    const mark = label === state ? "☑" : "□";
    const prefixPattern = new RegExp(`[□☐■☑√✔✓]\\s*${escapeRegExp(label)}`, "g");
    const bracketPattern = new RegExp(`${escapeRegExp(label)}\\[[^\\]]*\\]`, "g");
    if (prefixPattern.test(text)) {
      text = text.replace(prefixPattern, `${mark}${label}`);
    } else if (bracketPattern.test(text)) {
      text = text.replace(bracketPattern, `${label}[${label === state ? "√" : " "}]`);
    }
  }
  return text;
}

function cellOverrideValue(rowIndex: number, colIndex: number): string {
  const checkbox = activeSheetCheckboxCells.value.find((cell) => Number(cell.row) === rowIndex && Number(cell.col) === colIndex);
  if (checkbox) return checkboxDisplayValue(checkbox);
  const field = activeSheetMaintenanceFields.value.find((item) => Number(item.row) === rowIndex && Number(item.value_col) === colIndex);
  if (field) {
    const value = mopMaintenanceValues.value[maintenanceKey(field)] || "";
    if (value) {
      const labelCol = Number(field.label_col);
      return labelCol === colIndex ? `${field.label || ""}：${value}` : value;
    }
  }
  const editKey = rawCellKey(rowIndex, colIndex);
  if (Object.prototype.hasOwnProperty.call(mopCellEdits.value, editKey)) {
    return mopCellEdits.value[editKey];
  }
  return "";
}

function buildMopCheckboxPayload(): Dict[] {
  return activeSheetCheckboxCells.value
    .map((cell) => ({ ...cell, selection: checkboxState(cell), selected_label: checkboxState(cell) }))
    .filter((cell) => cell.selection);
}

function buildMopFieldPayload(): Dict[] {
  return activeSheetMaintenanceFields.value.map((field) => ({
    ...field,
    fill_value: mopMaintenanceValues.value[maintenanceKey(field)] || "",
  }));
}

function buildMopCellEditPayload(): Dict[] {
  return Object.entries(mopCellEdits.value).map(([key, value]) => {
    const parts = key.split(":");
    return {
      sheet: parts.slice(0, -2).join(":"),
      row: Number(parts[parts.length - 2] || 0),
      col: Number(parts[parts.length - 1] || 0),
      value,
    };
  });
}

function personInitial(person: Dict): string {
  const text = String(person?.name || person?.employee_no || "?").trim();
  return text.slice(0, 1).toUpperCase() || "?";
}

function personHasUsableSignature(person: Dict | null | undefined): boolean {
  return Boolean(person?.has_signature && String(person?.signature_preview_url || "").trim());
}

function rememberSignaturePeople(items: Dict[]): void {
  if (!items.length) return;
  const next = { ...signaturePeopleById.value };
  for (const item of items) {
    const recordId = String(item?.record_id || "");
    if (!recordId) continue;
    next[recordId] = { ...(next[recordId] || {}), ...item };
  }
  signaturePeopleById.value = next;
}

function updateRememberedSignaturePerson(recordId: string, patch: Dict): void {
  const recordText = String(recordId || "");
  if (!recordText) return;
  const next = {
    ...signaturePeopleById.value,
    [recordText]: { ...(signaturePeopleById.value[recordText] || {}), ...patch },
  };
  signaturePeopleById.value = next;
  signaturePeople.value = signaturePeople.value.map((item) => (
    item.record_id === recordText ? { ...item, ...patch } : item
  ));
}

function markSignatureUnavailable(recordId: unknown): void {
  const id = String(recordId || "").trim();
  if (!id) return;
  updateRememberedSignaturePerson(id, {
    has_signature: false,
    signature_count: 0,
    signature_preview_url: "",
    signature_version: "",
  });
}

function handleSignatureImageError(recordId: unknown): void {
  markSignatureUnavailable(recordId);
  signatureMessage.value = "该人员签名附件不可用，请重新手写保存。";
  signatureMessageType.value = "failed";
}

function signatureContext(): CanvasRenderingContext2D | null {
  const canvas = signatureCanvasRef.value;
  if (!canvas) return null;
  const ctx = canvas.getContext("2d");
  if (!ctx) return null;
  ctx.lineWidth = 4;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = "#0f172a";
  return ctx;
}

function resizeSignatureCanvas(): void {
  const canvas = signatureCanvasRef.value;
  if (!canvas) return;
  const rect = canvas.getBoundingClientRect();
  const ratio = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
  const width = Math.max(320, Math.floor(rect.width * ratio));
  const height = Math.max(170, Math.floor(rect.height * ratio));
  if (canvas.width === width && canvas.height === height) return;
  const previous = document.createElement("canvas");
  const previousHasInk = signatureHasInk.value && canvas.width > 0 && canvas.height > 0;
  if (previousHasInk) {
    previous.width = canvas.width;
    previous.height = canvas.height;
    previous.getContext("2d")?.drawImage(canvas, 0, 0);
  }
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    signatureHasInk.value = false;
    return;
  }
  if (previousHasInk) {
    ctx.drawImage(previous, 0, 0, previous.width, previous.height, 0, 0, width, height);
  }
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.lineWidth = 4;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = "#0f172a";
  signatureHasInk.value = Boolean(previousHasInk);
}

function ensureSignatureCanvasObserver(): void {
  if (!signatureCanvasRef.value || !("ResizeObserver" in window)) return;
  if (signatureResizeObserver) return;
  signatureResizeObserver = new ResizeObserver(() => resizeSignatureCanvas());
  signatureResizeObserver.observe(signatureCanvasRef.value);
}

function signaturePointFromEvent(event: PointerEvent): { x: number; y: number } {
  const rect = signatureCanvasRef.value?.getBoundingClientRect();
  return {
    x: event.clientX - (rect?.left || 0),
    y: event.clientY - (rect?.top || 0),
  };
}

function startSignatureDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!activeSignaturePerson.value || signatureSaving.value) return;
  const canvas = signatureCanvasRef.value;
  const ctx = signatureContext();
  if (!canvas || !ctx) return;
  canvas.setPointerCapture?.(event.pointerId);
  signatureDrawing = true;
  signatureHasInk.value = true;
  const point = signaturePointFromEvent(event);
  ctx.beginPath();
  ctx.moveTo(point.x, point.y);
}

function moveSignatureDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!signatureDrawing) return;
  const ctx = signatureContext();
  if (!ctx) return;
  const point = signaturePointFromEvent(event);
  ctx.lineTo(point.x, point.y);
  ctx.stroke();
  signatureHasInk.value = true;
}

function endSignatureDraw(event: PointerEvent): void {
  event.preventDefault();
  if (!signatureDrawing) return;
  signatureDrawing = false;
  signatureCanvasRef.value?.releasePointerCapture?.(event.pointerId);
}

function clearSignatureCanvas(): void {
  const canvas = signatureCanvasRef.value;
  const ctx = signatureContext();
  if (canvas && ctx) {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
  }
  signatureHasInk.value = false;
}

function selectSignaturePerson(recordId: string): void {
  const recordText = String(recordId || "");
  if (!recordText) return;
  const person = signaturePeople.value.find((item) => item.record_id === recordText)
    || signaturePeopleById.value[recordText];
  if (person) rememberSignaturePeople([person]);
  const existing = signatureSelectedRecords.value[signatureRole.value] || [];
  const next = existing.includes(recordText) ? existing.filter((item) => item !== recordText) : [...existing, recordText];
  if (existing.includes(recordText)) next.push(recordText);
  signatureSelectedRecords.value = {
    ...signatureSelectedRecords.value,
    [signatureRole.value]: [...new Set(next)],
  };
  signatureMessage.value = "";
  signatureMessageType.value = "";
  clearSignatureCanvas();
}

function setActiveSignaturePerson(recordId: string): void {
  selectSignaturePerson(recordId);
}

function removeSignaturePerson(role: "implementer" | "auditor", recordId: string): void {
  const recordText = String(recordId || "");
  signatureSelectedRecords.value = {
    ...signatureSelectedRecords.value,
    [role]: (signatureSelectedRecords.value[role] || []).filter((item) => item !== recordText),
  };
  clearSignatureCanvas();
}

function selectedSignaturePeople(role: "implementer" | "auditor"): Dict[] {
  const ids = signatureSelectedRecords.value[role] || [];
  return ids
    .map((id) => signaturePeopleById.value[id] || signaturePeople.value.find((item) => item.record_id === id))
    .filter((item): item is Dict => Boolean(item));
}

function roleForMaintenanceLabel(label: unknown): "implementer" | "auditor" | "" {
  const text = String(label || "");
  if (text.includes("维护实施人")) return "implementer";
  if (text.includes("维护审核人")) return "auditor";
  return "";
}

function signatureBaseColumn(role: "implementer" | "auditor"): number {
  return role === "implementer" ? 2 : 3;
}

function cellSignatures(rowIndex: number, colIndex: number): Dict[] {
  const implementerField = activeSheetMaintenanceFields.value.find((item) => (
    Number(item?.row) === rowIndex && roleForMaintenanceLabel(item?.label) === "implementer"
  ));
  const auditorField = activeSheetMaintenanceFields.value.find((item) => (
    Number(item?.row) === rowIndex && roleForMaintenanceLabel(item?.label) === "auditor"
  ));
  let role: "implementer" | "auditor" | "" = "";
  let signatureIndex = -1;
  if (implementerField) {
    const index = colIndex - signatureBaseColumn("implementer");
    if (index >= 0 && index % 2 === 0) {
      role = "implementer";
      signatureIndex = index / 2;
    }
  }
  if (!role && auditorField) {
    const index = colIndex - signatureBaseColumn("auditor");
    if (index >= 0 && index % 2 === 0) {
      role = "auditor";
      signatureIndex = index / 2;
    }
  }
  if (!role || signatureIndex < 0) return [];
  const person = selectedSignaturePeople(role).filter((item) => personHasUsableSignature(item))[signatureIndex];
  return person ? [person] : [];
}

function scheduleSignaturePeopleSearch(): void {
  if (signatureSearchTimer) {
    clearTimeout(signatureSearchTimer);
  }
  signatureSearchTimer = setTimeout(() => {
    signatureSearchTimer = null;
    void loadSignaturePeople({ silent: true });
  }, 300);
}

async function loadSignaturePeople(options: { silent?: boolean } = {}): Promise<void> {
  const requestSeq = ++signatureSearchRequestSeq;
  signatureLoading.value = true;
  if (!options.silent) {
    signatureMessage.value = "";
    signatureMessageType.value = "";
  }
  try {
    const url = new URL("/api/signatures/people", window.location.origin);
    url.searchParams.set("scope", scope.value);
    if (signatureSearch.value.trim()) url.searchParams.set("q", signatureSearch.value.trim());
    if (!options.silent) url.searchParams.set("refresh", "1");
    url.searchParams.set("limit", "60");
    const data = await requestJson(`${url.pathname}${url.search}`);
    if (requestSeq !== signatureSearchRequestSeq) return;
    signaturePeople.value = Array.isArray(data.people) ? data.people : [];
    signaturePeopleTotal.value = Number(data.count || data.total || signaturePeople.value.length || 0);
    rememberSignaturePeople(signaturePeople.value);
    const current = activeSignatureRecordId.value;
    if (!current && signaturePeople.value.length === 1) {
      selectSignaturePerson(String(signaturePeople.value[0].record_id || ""));
    }
  } catch (error) {
    if (requestSeq !== signatureSearchRequestSeq) return;
    signatureMessage.value = error instanceof Error ? error.message : "读取签名人员失败";
    signatureMessageType.value = "failed";
  } finally {
    if (requestSeq === signatureSearchRequestSeq) {
      signatureLoading.value = false;
      await nextTick();
      resizeSignatureCanvas();
    }
  }
}

async function saveMopSignature(): Promise<void> {
  if (!activeSignaturePerson.value || !signatureCanvasRef.value || !signatureHasInk.value) return;
  signatureSaving.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await requestJson("/api/signatures/save", {
      method: "POST",
      body: JSON.stringify({
        record_id: activeSignaturePerson.value.record_id,
        signer_name: activeSignaturePerson.value.name || "",
        signature_png: signatureCanvasRef.value.toDataURL("image/png"),
      }),
    });
    signatureMessage.value = `${data.name || activeSignaturePerson.value.name || "签名"} 已保存到签名库。`;
    signatureMessageType.value = "success";
    updateRememberedSignaturePerson(activeSignaturePerson.value.record_id, {
      has_signature: true,
      signature_count: 1,
      signature_preview_url: data.signature_preview_url || activeSignaturePerson.value.signature_preview_url || "",
      signature_version: data.signature_version || activeSignaturePerson.value.signature_version || "",
    });
    clearSignatureCanvas();
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "保存签名失败";
    signatureMessageType.value = "failed";
  } finally {
    signatureSaving.value = false;
  }
}

async function fillMopSignatures(): Promise<void> {
  if (!canFillMopSignatures.value || !preview.value?.local_file?.path) return;
  mopFillSaving.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const signatures = [
      ...selectedSignaturePeople("implementer").filter((person) => personHasUsableSignature(person)).map((person) => ({
        role: "implementer",
        label: "维护实施人",
        record_id: person.record_id,
      })),
      ...selectedSignaturePeople("auditor").filter((person) => personHasUsableSignature(person)).map((person) => ({
        role: "auditor",
        label: "维护审核人",
        record_id: person.record_id,
      })),
    ];
    const data = await requestJson("/api/engineer/mop/fill", {
      method: "POST",
      body: JSON.stringify({
        scope: scope.value,
        local_file_path: preview.value.local_file.path,
        mop_record_id: preview.value.mop_record_id || selectedMop.value?.record_id || "",
        mop_title: preview.value.mop_title || selectedMop.value?.title || "",
        sheet_name: activeSheet.value?.name || "",
        fields: buildMopFieldPayload(),
        checkboxes: buildMopCheckboxPayload(),
        cell_edits: buildMopCellEditPayload(),
        signatures,
      }),
    });
    filledMopResult.value = data;
    signatureMessage.value = `已生成已签名 MOP：${data.relative_path || data.file_name || "本地文件"}`;
    signatureMessageType.value = "success";
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "生成已签名 MOP 失败";
    signatureMessageType.value = "failed";
  } finally {
    mopFillSaving.value = false;
  }
}

async function resetMopSigning(): Promise<void> {
  if (!filledMopResult.value || !preview.value || !selectedMop.value) return;
  mopResetting.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const data = await requestJson("/api/engineer/mop/reset", {
      method: "POST",
      body: JSON.stringify({
        scope: scope.value,
        filled_file_path: filledMopResult.value.path || "",
        mop_record_id: preview.value.mop_record_id || selectedMop.value.record_id || "",
        file_token: attachmentKey(attachment),
        file_name: attachment.name || "",
      }),
    });
    clearMopFillState({ clearSignatures: true });
    preview.value = data;
    const sheets = Array.isArray(data.sheets) ? data.sheets : [];
    activeSheetName.value = String(sheets[0]?.name || activeSheetName.value || "");
    signatureMessage.value = "已删除旧签名文件，并重新下载干净 MOP。";
    signatureMessageType.value = "success";
    await nextTick();
    ensureSignatureCanvasObserver();
    resizeSignatureCanvas();
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "重新下载干净 MOP 失败";
    signatureMessageType.value = "failed";
  } finally {
    mopResetting.value = false;
  }
}

async function sendSignatureLink(): Promise<void> {
  if (!activeSignaturePerson.value || !activeSignaturePerson.value.open_id) return;
  signatureLinkSending.value = true;
  signatureMessage.value = "";
  signatureMessageType.value = "";
  try {
    const data = await requestJson("/api/signatures/send-link", {
      method: "POST",
      body: JSON.stringify({
        record_id: activeSignaturePerson.value.record_id,
        signer_name: activeSignaturePerson.value.name || "",
        scope: scope.value,
      }),
    });
    const person = data.person || activeSignaturePerson.value;
    signatureMessage.value = `签名链接已发送给 ${person.name || activeSignaturePerson.value.name || "该人员"}。`;
    signatureMessageType.value = "success";
  } catch (error) {
    signatureMessage.value = error instanceof Error ? error.message : "发送签名链接失败";
    signatureMessageType.value = "failed";
  } finally {
    signatureLinkSending.value = false;
  }
}

function applyNoticeBinding(notice: Dict): void {
  const binding = notice?.mop_binding;
  if (!binding) return;
  selectedMopRecordId.value = String(binding.mop_record_id || "");
  selectedAttachmentToken.value = String(binding.mop_attachment_token || "");
  activeSheetName.value = String(binding.selected_sheet || "");
}

function selectNotice(noticeKey: string): void {
  selectedNoticeKey.value = noticeKey;
  preview.value = null;
  filledMopResult.value = null;
  previewMode.value = false;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  const notice = selectedNotice.value;
  if (notice) applyNoticeBinding(notice);
}

function selectMop(recordId: string): void {
  selectedMopRecordId.value = recordId;
  preview.value = null;
  filledMopResult.value = null;
  previewMode.value = false;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  const first = selectedMopAttachments.value[0];
  selectedAttachmentToken.value = first ? attachmentKey(first) : "";
}

function backToBinding(): void {
  previewMode.value = false;
}

async function loadPage(): Promise<void> {
  if (!props.loggedIn) return;
  loading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const data = await requestJson(`/api/engineer/mop/bootstrap?scope=${encodeURIComponent(scope.value)}`);
    notices.value = Array.isArray(data.notices) ? data.notices : [];
    mopCandidates.value = Array.isArray(data.mop_candidates) ? data.mop_candidates : [];
    warnings.value = Array.isArray(data.warnings) ? data.warnings.map((item: unknown) => String(item)) : [];
    if (!selectedNoticeKey.value || !notices.value.some((item) => item.notice_key === selectedNoticeKey.value)) {
      selectedNoticeKey.value = notices.value[0]?.notice_key || "";
    }
    if (selectedNotice.value) applyNoticeBinding(selectedNotice.value);
    if (!selectedMopRecordId.value && mopCandidates.value.length) {
      selectMop(mopCandidates.value[0].record_id);
    }
  } catch (error) {
    message.value = error instanceof Error ? error.message : "加载工程师 MOP 数据失败";
    messageType.value = "failed";
  } finally {
    loading.value = false;
  }
}

async function saveBinding(options: { silent?: boolean } = {}): Promise<Dict | null> {
  if (!canBind.value || !selectedNotice.value || !selectedMop.value) return null;
  saving.value = true;
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  if (!options.silent) {
    message.value = "";
    messageType.value = "";
  }
  try {
    const attachment = selectedAttachment.value || {};
    const payload = {
      scope: scope.value,
      notice_key: selectedNotice.value.notice_key,
      template_key: selectedNotice.value.mop_template_key || "",
      mop_template_key: selectedNotice.value.mop_template_key || "",
      notice_title: selectedNotice.value.title,
      notice_status: selectedNotice.value.status,
      building: selectedNotice.value.building,
      maintenance_total: selectedNotice.value.maintenance_total,
      maintenance_cycle: selectedNotice.value.maintenance_cycle,
      source_record_id: selectedNotice.value.source_record_id,
      target_record_id: selectedNotice.value.target_record_id,
      active_item_id: selectedNotice.value.active_item_id,
      mop_app_token: selectedMop.value.app_token,
      mop_table_id: selectedMop.value.table_id,
      mop_record_id: selectedMop.value.record_id,
      mop_title: selectedMop.value.title,
      mop_attachment_token: attachment.file_token || attachment.url || "",
      mop_attachment_name: attachment.name || "",
      selected_sheet: activeSheetName.value,
    };
    const data = await requestJson("/api/engineer/mop/bind", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const binding = data.binding || {};
    const notice = selectedNotice.value;
    if (notice) notice.mop_binding = binding;
    mopBindingStatus.value = "已自动绑定，下次同类通告会自动带出";
    if (!options.silent) {
      message.value = "已自动绑定。";
      messageType.value = "success";
    }
    return binding;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "自动绑定 MOP 失败";
    mopBindingError.value = errorMessage;
    if (!options.silent) {
      message.value = errorMessage;
      messageType.value = "failed";
    }
    return null;
  } finally {
    saving.value = false;
  }
}

async function startMopPreview(): Promise<void> {
  if (!canPreview.value || !selectedMop.value) return;
  const binding = await saveBinding({ silent: true });
  if (!binding) return;
  previewLoading.value = true;
  message.value = "";
  messageType.value = "";
  try {
    const attachment = selectedAttachment.value || {};
    const params = new URLSearchParams({
      scope: scope.value,
      mop_record_id: String(selectedMop.value.record_id || ""),
      file_token: attachmentKey(attachment),
      file_name: String(attachment.name || ""),
    });
    const data = await requestJson(`/api/engineer/mop/preview?${params.toString()}`);
    clearMopFillState({ clearSignatures: true });
    preview.value = data;
    const sheets = Array.isArray(data.sheets) ? data.sheets : [];
    if (!sheets.some((sheet: Dict) => sheet.name === activeSheetName.value)) {
      activeSheetName.value = String(sheets[0]?.name || "");
    }
    previewMode.value = true;
    if (Array.isArray(data.warnings)) {
      warnings.value = [...new Set([...warnings.value, ...data.warnings.map((item: unknown) => String(item))])];
    }
    await nextTick();
    ensureSignatureCanvasObserver();
    resizeSignatureCanvas();
    if (!signaturePeople.value.length) {
      if (!signatureSearch.value && selectedNotice.value?.building) {
        signatureSearch.value = String(selectedNotice.value.building || "");
      }
      void loadSignaturePeople();
    }
  } catch (error) {
    message.value = error instanceof Error ? error.message : "读取 MOP 表格失败";
    messageType.value = "failed";
  } finally {
    previewLoading.value = false;
  }
}

watch(scope, () => {
  const url = new URL(window.location.href);
  url.searchParams.set("scope", scope.value);
  window.history.replaceState({}, "", url);
  selectedNoticeKey.value = "";
  selectedMopRecordId.value = "";
  selectedAttachmentToken.value = "";
  mopBindingStatus.value = "";
  mopBindingError.value = "";
  preview.value = null;
  previewMode.value = false;
  void loadPage();
});

watch(selectedMopAttachments, (items) => {
  if (!items.length) {
    selectedAttachmentToken.value = "";
    return;
  }
  if (!items.some((item) => attachmentKey(item) === selectedAttachmentToken.value)) {
    selectedAttachmentToken.value = attachmentKey(items[0]);
  }
});

onMounted(() => {
  ensureSignatureCanvasObserver();
  if (props.loggedIn) void loadPage();
});

onBeforeUnmount(() => {
  if (signatureSearchTimer) {
    clearTimeout(signatureSearchTimer);
    signatureSearchTimer = null;
  }
  signatureResizeObserver?.disconnect();
  signatureResizeObserver = null;
});

watch(signatureRole, () => {
  signatureMessage.value = "";
  signatureMessageType.value = "";
  clearSignatureCanvas();
  void nextTick(() => resizeSignatureCanvas());
});

watch(signatureSearch, () => {
  scheduleSignaturePeopleSearch();
});

watch(() => props.loggedIn, (value) => {
  if (value) void loadPage();
});

watch(() => props.scopeOptions, (items) => {
  if (!items.length) return;
  const allowed = items.map((item) => normalizeScope(item.value));
  if (!allowed.includes(scope.value)) {
    scope.value = allowed[0] || "ALL";
  }
}, { immediate: true, deep: true });
</script>

<style scoped>
.engineer-mop {
  width: min(1800px, 100%);
  margin: 0 auto;
  padding: 28px 32px 42px;
  display: grid;
  gap: 20px;
}

.engineer-mop.preview-open {
  width: min(1920px, 100%);
  padding: 18px 24px 28px;
  gap: 12px;
}

.mop-head,
.mop-summary article,
.panel,
.notice-box,
.message,
.warning-list {
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background: rgba(255, 255, 255, 0.88);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.mop-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 18px;
  padding: 22px 24px;
}

.mop-head strong {
  display: block;
  color: #0f172a;
  font-size: 24px;
  font-weight: 700;
}

.mop-head p,
.panel-head p,
.selected-notice p {
  margin: 6px 0 0;
  color: #64748b;
}

.head-actions,
.actions,
.filters {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.scope-select {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #475569;
  font-size: 13px;
  font-weight: 700;
}

select,
input {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 11px;
  color: #0f172a;
  background: #ffffff;
  outline: none;
}

input:focus,
select:focus {
  border-color: #1e63ff;
  box-shadow: 0 0 0 3px rgba(30, 99, 255, 0.12);
}

.btn {
  min-height: 38px;
  border: 1px solid #d8e5f7;
  border-radius: 12px;
  padding: 8px 14px;
  color: #0f172a;
  font-weight: 750;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.btn:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.btn.blue {
  border-color: transparent;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.mop-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
}

.mop-summary article {
  padding: 18px 20px;
}

.mop-summary span {
  color: #64748b;
  font-size: 13px;
}

.mop-summary strong {
  display: block;
  margin-top: 8px;
  color: #005bff;
  font-size: 26px;
  font-weight: 800;
}

.mop-layout {
  display: grid;
  grid-template-columns: minmax(320px, 0.82fr) minmax(520px, 1.18fr);
  gap: 18px;
  align-items: start;
}

.panel {
  height: min(760px, calc(100vh - 300px));
  min-height: min(560px, calc(100vh - 220px));
  overflow: hidden;
  padding: 18px;
  display: grid;
  grid-template-rows: auto auto 1fr;
  gap: 14px;
}

.binding-panel {
  min-height: min(720px, calc(100vh - 180px));
  height: auto;
  max-height: none;
  overflow: visible;
  grid-template-rows: auto auto auto minmax(120px, 1fr) auto auto;
  align-content: stretch;
}

.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}

.panel-head h2 {
  margin: 0;
  color: #0f172a;
  font-size: 18px;
}

.panel-head > span {
  padding: 5px 10px;
  border: 1px solid #cfe0ff;
  border-radius: 999px;
  color: #005bff;
  background: #eff6ff;
  font-weight: 800;
}

.list,
.mop-candidate-list {
  min-height: 0;
  overflow: auto;
  display: grid;
  gap: 10px;
  align-content: start;
  padding-right: 4px;
}

.notice-row,
.mop-row,
.selected-notice {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  padding: 13px 14px;
  text-align: left;
  color: #0f172a;
  background: #ffffff;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.notice-row,
.mop-row {
  cursor: pointer;
}

.notice-row:hover,
.mop-row:hover,
.notice-row.active,
.mop-row.active {
  border-color: #1e63ff;
  box-shadow: 0 10px 24px rgba(30, 99, 255, 0.13);
  transform: translateY(-1px);
}

.notice-row strong,
.mop-row strong,
.selected-notice strong {
  display: block;
  margin-top: 8px;
  line-height: 1.45;
}

.mop-row {
  display: grid;
  gap: 6px;
  padding: 10px 12px;
  border-radius: 14px;
  line-height: 1.45;
}

.mop-row strong {
  display: -webkit-box;
  margin-top: 0;
  overflow: hidden;
  line-height: 1.45;
  text-overflow: ellipsis;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.mop-candidate-list {
  max-height: clamp(240px, 42vh, 480px);
  min-height: 220px;
  overflow: auto;
}

.selected-mop-strip {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  min-height: 92px;
  padding: 12px;
  border: 1px solid #cfe0ff;
  border-radius: 16px;
  background: linear-gradient(135deg, #f8fbff 0%, #eef6ff 100%);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9);
}

.selected-mop-strip > div {
  min-width: 0;
}

.selected-mop-strip .btn {
  align-self: center;
  white-space: nowrap;
}

.selected-mop-actions {
  display: grid;
  justify-items: end;
  gap: 7px;
  min-width: 150px;
}

.selected-mop-actions small {
  max-width: 210px;
  color: #b45309;
  text-align: right;
}

.selected-mop-strip span,
.selected-mop-strip small {
  display: block;
  color: #64748b;
  font-size: 12px;
  line-height: 1.55;
  overflow-wrap: anywhere;
}

.selected-mop-strip strong {
  display: block;
  margin: 4px 0;
  color: #0f172a;
  font-size: 14px;
  line-height: 1.45;
  overflow-wrap: anywhere;
  white-space: normal;
}

.mop-bind-status {
  display: inline-flex;
  align-items: center;
  width: fit-content;
  max-width: 100%;
  margin-top: 8px;
  padding: 4px 9px;
  border-radius: 999px;
  font-size: 12px;
  font-style: normal;
  font-weight: 800;
  line-height: 1.35;
  overflow-wrap: anywhere;
}

.mop-bind-status.success {
  color: #047857;
  background: #ecfdf5;
  border: 1px solid #bbf7d0;
}

.mop-bind-status.failed {
  color: #b45309;
  background: #fffbeb;
  border: 1px solid #fde68a;
}

.binding-panel .actions {
  position: sticky;
  bottom: 0;
  z-index: 2;
  align-self: end;
  padding-top: 10px;
  border-top: 1px solid #e2ecfb;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.76), #ffffff 38%);
}

.binding-panel .actions .btn {
  flex: 1 1 180px;
  justify-content: center;
}

.notice-row small,
.mop-row small,
.notice-row em {
  display: block;
  margin-top: 7px;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
}

.notice-row em {
  color: #2563eb;
}

.mop-row small {
  margin-top: 0;
  line-height: 1.5;
  white-space: normal;
  word-break: break-word;
}

.row-status,
.selected-notice span {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 4px 9px;
  border-radius: 999px;
  color: #047857;
  background: #ecfdf5;
  font-size: 12px;
  font-weight: 800;
}

.row-status.closed,
.notice-row.closed .row-status {
  color: #475569;
  background: #f1f5f9;
}

.field {
  display: grid;
  gap: 7px;
  color: #475569;
  font-size: 13px;
  font-weight: 750;
}

.empty-box,
.notice-box,
.message,
.warning-list {
  padding: 18px;
  color: #475569;
}

.empty-box {
  border: 1px dashed #cfe0ff;
  border-radius: 16px;
  background: #f8fbff;
}

.empty-box.warning,
.message.failed {
  color: #b45309;
  background: #fffbeb;
  border-color: #fde68a;
}

.message.success {
  color: #047857;
  background: #ecfdf5;
  border-color: #a7f3d0;
}

.warning-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.warning-list span {
  padding: 7px 10px;
  border-radius: 999px;
  color: #92400e;
  background: #fffbeb;
}

.mop-sign-panel {
  display: grid;
  gap: 12px;
  padding: 14px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.sign-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.sign-panel-head strong,
.selected-sign-person strong {
  display: block;
  color: #0f172a;
  font-size: 16px;
  font-weight: 850;
}

.sign-panel-head p,
.selected-sign-person small {
  margin: 4px 0 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.55;
}

.sign-role-tabs {
  display: inline-flex;
  gap: 4px;
  padding: 4px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #f8fbff;
}

.sign-role-tabs button {
  min-height: 32px;
  border: 0;
  border-radius: 999px;
  padding: 6px 12px;
  background: transparent;
  color: #475569;
  font-weight: 800;
  cursor: pointer;
}

.sign-role-tabs button.active {
  color: #ffffff;
  background: #1e63ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.2);
}

.sign-workspace {
  display: grid;
  grid-template-columns: minmax(260px, 360px) minmax(0, 1fr);
  gap: 12px;
}

.sign-people,
.sign-canvas-card {
  display: grid;
  align-content: start;
  gap: 10px;
  min-width: 0;
}

.inline-search {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
}

.inline-search input {
  min-width: 0;
}

.signature-refresh {
  min-width: 64px;
  padding-inline: 12px;
}

.search-inline-status {
  display: block;
  margin-top: 7px;
  color: #64748b;
  font-size: 12px;
  line-height: 1.4;
}

.sign-person-list {
  display: grid;
  gap: 8px;
  max-height: 260px;
  overflow: auto;
  padding-right: 4px;
}

.sign-person {
  display: grid;
  grid-template-columns: 38px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  border: 1px solid #dbe7f6;
  border-radius: 14px;
  background: #ffffff;
  padding: 9px 10px;
  color: #0f172a;
  text-align: left;
  cursor: pointer;
}

.sign-person:hover,
.sign-person.active {
  border-color: #1e63ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.12);
}

.sign-person.current {
  background: linear-gradient(135deg, #f8fbff, #eef6ff);
}

.sign-person > span {
  grid-row: span 2;
  display: grid;
  width: 38px;
  height: 38px;
  place-items: center;
  border-radius: 13px;
  color: #ffffff;
  background: linear-gradient(135deg, #1e63ff, #22c1dc);
  font-weight: 900;
}

.sign-person strong,
.sign-person small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sign-person strong {
  font-size: 14px;
}

.sign-person small {
  color: #64748b;
  font-size: 12px;
}

.sign-person em {
  grid-row: span 2;
  border-radius: 999px;
  background: #eef2ff;
  padding: 5px 8px;
  color: #3156c9;
  font-size: 12px;
  font-style: normal;
  font-weight: 850;
}

.sign-person em.ok {
  color: #047857;
  background: #ecfdf5;
}

.selected-sign-person {
  display: grid;
  gap: 6px;
}

.selected-sign-person span {
  color: #2563eb;
  font-size: 12px;
  font-weight: 850;
}

.selected-signatures {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 4px;
}

.selected-signature-chip {
  display: inline-grid;
  grid-template-columns: 74px minmax(52px, auto) auto;
  align-items: center;
  gap: 7px;
  max-width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  padding: 5px 7px;
  cursor: pointer;
}

.selected-signature-chip.active {
  border-color: #1e63ff;
  box-shadow: 0 8px 18px rgba(30, 99, 255, 0.13);
}

.selected-signature-chip img {
  width: 70px;
  height: 24px;
  object-fit: contain;
}

.selected-signature-chip strong {
  overflow: hidden;
  color: #0f172a;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selected-signature-chip button {
  border: 0;
  border-radius: 999px;
  background: #eef2ff;
  color: #3156c9;
  font-size: 12px;
  font-weight: 850;
  cursor: pointer;
}

.mop-sign-canvas {
  position: relative;
  min-height: 180px;
  border: 1px dashed #93c5fd;
  border-radius: 16px;
  background:
    linear-gradient(90deg, rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    linear-gradient(rgba(37, 99, 235, 0.05) 1px, transparent 1px),
    #ffffff;
  background-size: 22px 22px;
  overflow: hidden;
  user-select: none;
  -webkit-user-select: none;
  touch-action: none;
}

.mop-sign-canvas.disabled {
  opacity: 0.65;
}

.mop-sign-canvas canvas {
  position: relative;
  z-index: 2;
  display: block;
  width: 100%;
  height: 180px;
  cursor: crosshair;
  touch-action: none;
  user-select: none;
  -webkit-user-select: none;
}

.sign-clear-inline {
  position: absolute;
  z-index: 5;
  top: 10px;
  right: 10px;
  border: 1px solid rgba(148, 163, 184, 0.38);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.92);
  color: #3156c9;
  padding: 5px 10px;
  font-size: 12px;
  font-weight: 850;
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
  cursor: pointer;
}

.sign-clear-inline:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.mop-sign-preview-img {
  position: absolute;
  z-index: 1;
  inset: 12%;
  width: 76%;
  height: 76%;
  object-fit: contain;
  pointer-events: none;
}

.sign-placeholder {
  position: absolute;
  z-index: 3;
  inset: 0;
  display: grid;
  place-items: center;
  pointer-events: none;
  color: #94a3b8;
  font-weight: 850;
}

.sheet-cell-signatures {
  display: flex;
  align-items: center;
  gap: 6px;
  min-height: 32px;
}

.sheet-cell-signatures img {
  width: 88px;
  height: 30px;
  object-fit: contain;
}

.sign-actions {
  display: grid;
  grid-template-columns: minmax(220px, 1fr) minmax(240px, auto) minmax(260px, auto);
  align-items: start;
  gap: 10px;
}

.sign-status {
  min-width: 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.6;
}

.sign-actions .btn {
  flex: 0 0 auto;
}

.action-group {
  display: grid;
  gap: 7px;
  min-width: 0;
  padding: 10px;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: rgba(248, 251, 255, 0.72);
}

.action-group > strong {
  color: #3156c9;
  font-size: 12px;
  font-weight: 900;
}

.action-group > div {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.action-group small {
  color: #64748b;
  font-size: 12px;
  line-height: 1.4;
}

.file-action-group {
  background: linear-gradient(135deg, rgba(239, 246, 255, 0.88), rgba(255, 255, 255, 0.86));
}

.reset-clean {
  border-color: #fde68a;
  color: #92400e;
  background: #fffbeb;
}

.warning-hint {
  color: #b45309 !important;
}

.sign-status.success {
  color: #047857;
}

.sign-status.failed {
  color: #b45309;
}

.empty-box.compact {
  padding: 12px;
  font-size: 13px;
}

.mop-completion-panel {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.mop-completion-panel span {
  display: grid;
  gap: 3px;
  min-width: 0;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  background: #ffffff;
  padding: 9px 10px;
}

.mop-completion-panel span.done {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.mop-completion-panel span.pending {
  border-color: #fde68a;
  background: #fffbeb;
}

.mop-completion-panel strong {
  color: #0f172a;
  font-size: 12px;
  font-weight: 900;
}

.mop-completion-panel em {
  overflow: hidden;
  color: #64748b;
  font-size: 12px;
  font-style: normal;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sheet-tabs {
  display: flex;
  gap: 8px;
  overflow-x: auto;
  padding-bottom: 2px;
}

.sheet-tabs button {
  flex: 0 0 auto;
  min-height: 34px;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  padding: 7px 12px;
  background: #ffffff;
  color: #475569;
  font-weight: 750;
  cursor: pointer;
}

.sheet-tabs button.active {
  border-color: #1e63ff;
  color: #ffffff;
  background: #1e63ff;
}

.sheet-note {
  padding: 10px 12px;
  border: 1px solid #fde68a;
  border-radius: 12px;
  color: #92400e;
  background: #fffbeb;
  font-size: 13px;
}

.sheet-scroll {
  min-height: 470px;
  max-height: 68vh;
  overflow: auto;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: #ffffff;
}

.mop-preview-page {
  display: grid;
  grid-template-rows: auto auto auto auto auto minmax(0, 1fr);
  gap: 12px;
  min-height: calc(100vh - 46px);
}

.preview-head {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 14px;
  padding: 14px 16px;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.08);
}

.preview-head strong {
  display: block;
  overflow: hidden;
  color: #0f172a;
  font-size: 18px;
  font-weight: 800;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-head p {
  margin: 4px 0 0;
  overflow: hidden;
  color: #64748b;
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-file-status,
.mop-detect-panel {
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 8px 22px rgba(0, 47, 135, 0.06);
}

.mop-file-status {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 4px 12px;
  padding: 10px 14px;
}

.mop-file-status span {
  color: #2563eb;
  font-size: 12px;
  font-weight: 850;
}

.mop-file-status strong,
.mop-file-status small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mop-file-status strong {
  color: #0f172a;
  font-size: 13px;
}

.mop-file-status small {
  grid-column: 2;
  color: #64748b;
  font-size: 12px;
}

.mop-detect-panel {
  display: grid;
  gap: 10px;
  padding: 12px 14px;
}

.detect-summary {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.detect-summary span,
.detect-summary strong,
.detect-summary em {
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 12px;
}

.detect-summary span {
  color: #92400e;
  background: #fffbeb;
  font-weight: 850;
}

.detect-summary span.muted {
  color: #047857;
  background: #ecfdf5;
}

.detect-summary strong {
  color: #1d4ed8;
  background: #eff6ff;
}

.detect-summary em {
  color: #15803d;
  background: #dcfce7;
  font-style: normal;
  font-weight: 850;
}

.detect-empty {
  color: #64748b;
  font-size: 13px;
}

.preview-back {
  min-width: 76px;
}

.preview-tabs {
  padding: 4px;
  border: 1px solid #d8e5f7;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.72);
}

.preview-scroll {
  position: relative;
  min-height: 0;
  max-height: calc(100vh - 330px);
  border-radius: 18px;
}

.table-fill-toolbar {
  position: sticky;
  top: 0;
  z-index: 8;
  display: flex;
  align-items: center;
  gap: 8px;
  border-bottom: 1px solid #e2e8f0;
  background: rgba(248, 251, 255, 0.96);
  padding: 8px 10px;
  backdrop-filter: blur(10px);
}

.table-fill-toolbar span {
  color: #64748b;
  font-size: 12px;
  font-weight: 800;
}

.table-fill-toolbar em {
  margin-left: auto;
  border-radius: 999px;
  background: #dcfce7;
  padding: 5px 9px;
  color: #15803d;
  font-size: 12px;
  font-style: normal;
  font-weight: 850;
}

table {
  border-collapse: collapse;
  min-width: 100%;
  font-size: 13px;
}

th,
td {
  min-width: 96px;
  max-width: 360px;
  border: 1px solid #e2e8f0;
  padding: 7px 9px;
  vertical-align: top;
  white-space: pre-wrap;
  word-break: break-word;
}

.row-head,
.corner-cell,
.column-head {
  color: #64748b;
  background: #f8fafc;
  text-align: center;
  font-weight: 750;
}

.row-head {
  position: sticky;
  left: 0;
  z-index: 2;
  min-width: 44px;
  width: 44px;
}

.corner-cell {
  position: sticky;
  top: 0;
  left: 0;
  z-index: 4;
  min-width: 44px;
  width: 44px;
}

.column-head {
  position: sticky;
  top: 0;
  z-index: 3;
  min-width: 96px;
}

td.merged {
  background: #fbfdff;
}

td.fillable,
td.field-fillable {
  position: relative;
  background: #f8fbff;
}

td.fillable.normal {
  background: #ecfdf5;
}

td.fillable.abnormal {
  background: #fff7ed;
}

.cell-fill-popover {
  position: absolute;
  z-index: 6;
  right: 6px;
  bottom: 6px;
  display: none;
  align-items: center;
  gap: 4px;
  border: 1px solid rgba(37, 99, 235, 0.18);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.96);
  padding: 4px;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.16);
}

td.fillable:hover .cell-fill-popover,
td.fillable:focus-within .cell-fill-popover,
.cell-fill-popover.pinned {
  display: inline-flex;
}

.cell-fill-popover span {
  padding: 0 5px;
  color: #3156c9;
  font-size: 12px;
  font-weight: 900;
}

.cell-fill-popover button {
  border: 0;
  border-radius: 999px;
  background: #eef2ff;
  padding: 4px 8px;
  color: #3156c9;
  font-size: 12px;
  font-weight: 850;
  cursor: pointer;
}

.cell-fill-popover button.active {
  background: #1e63ff;
  color: #ffffff;
}

.cell-field-popover {
  position: absolute;
  z-index: 6;
  right: 6px;
  bottom: 6px;
  display: none;
  align-items: center;
  gap: 4px;
  max-width: min(520px, calc(100vw - 120px));
  border: 1px solid rgba(37, 99, 235, 0.18);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.97);
  padding: 4px;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.16);
}

td.field-fillable:hover .cell-field-popover,
td.field-fillable:focus-within .cell-field-popover,
.cell-field-popover.pinned {
  display: inline-flex;
}

.cell-field-popover span {
  padding: 0 5px;
  color: #3156c9;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.cell-field-popover input {
  height: 28px;
  box-sizing: border-box;
  border: 1px solid #dbe3ee;
  border-radius: 999px;
  padding: 0 8px;
  color: #0f172a;
  font: inherit;
  font-size: 12px;
  outline: none;
}

.cell-field-popover button {
  border: 0;
  border-radius: 999px;
  background: #eef2ff;
  padding: 5px 9px;
  color: #3156c9;
  font-size: 12px;
  font-weight: 850;
  white-space: nowrap;
  cursor: pointer;
}

.cell-field-popover button:hover {
  background: #1e63ff;
  color: #ffffff;
}

@media (max-width: 1180px) {
  .mop-summary,
  .mop-layout {
    grid-template-columns: 1fr;
  }

  .panel {
    height: auto;
    min-height: auto;
  }

  .binding-panel {
    min-height: 0;
    overflow: visible;
  }

  .mop-candidate-list {
    max-height: 420px;
  }

  .selected-mop-strip {
    grid-template-columns: 1fr;
  }

  .selected-mop-actions {
    justify-items: stretch;
  }

  .selected-mop-actions small {
    max-width: none;
    text-align: left;
  }

  .sign-panel-head {
    align-items: stretch;
    flex-direction: column;
  }

  .sign-actions {
    grid-template-columns: 1fr;
  }

  .mop-completion-panel {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .sign-workspace {
    grid-template-columns: 1fr;
  }

  .sign-status {
    margin-right: 0;
  }

  .sign-person-list {
    max-height: 220px;
  }

  .engineer-mop.preview-open {
    padding: 14px;
  }

  .preview-head {
    grid-template-columns: 1fr;
  }

  .preview-scroll {
    max-height: calc(100vh - 230px);
  }
}
</style>
