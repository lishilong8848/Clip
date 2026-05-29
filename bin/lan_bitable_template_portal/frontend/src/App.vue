<template>
  <main class="app-shell">
    <header class="topbar">
      <div class="brand">
        <img class="brand-logo" :src="brandLogoSrc" alt="世纪互联官方标识" />
        <div>
          <h1>南通基地-运维灯塔工作台</h1>
          <p>{{ isHistoryMemoryPage ? "历史记忆导入" : isWorkbench ? scopeLabel(currentScope) : "功能选择" }} · {{ syncText }}</p>
        </div>
      </div>
      <div class="topbar-actions">
        <span v-if="auth.loggedIn" class="user-chip">{{ auth.user?.name || auth.user?.open_id || "已登录" }}</span>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" @click="returnToHome">功能选择</button>
        <label v-if="auth.loggedIn && isWorkbench && visibleScopeOptions.length > 1" class="scope-switch">
          <span>切换楼栋</span>
          <select :value="currentScope" :disabled="loading" @change="switchScope(($event.target as HTMLSelectElement).value)">
            <option v-for="item in visibleScopeOptions" :key="item.value" :value="normalizeScopeValue(item.value)">
              {{ item.label }}
            </option>
          </select>
        </label>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="loading" @click="loadWorkbench()">
          {{ loading ? "刷新中" : "刷新本页" }}
        </button>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="repairRefreshing" @click="refreshRepair">
          {{ repairRefreshing ? "检修刷新中" : "刷新检修" }}
        </button>
        <button v-if="auth.loggedIn && isWorkbench" class="btn ghost" :disabled="changeRefreshing" @click="refreshChange">
          {{ changeRefreshing ? "变更刷新中" : "刷新变更" }}
        </button>
        <button v-if="isAdmin" class="btn ghost" @click="showAdminTools = true">管理/诊断</button>
        <button v-if="auth.loggedIn" class="btn danger-text" @click="logout">退出</button>
      </div>
    </header>

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

    <AuthPanels
      v-else-if="authChecking || !auth.loggedIn || (auth.loggedIn && !auth.scopeOptions.length)"
      :checking="authChecking"
      :logged-in="auth.loggedIn"
      :user="auth.user"
      :login-url="auth.loginUrl"
      :busy="permissionBusy"
      :request="permissionRequest"
      :requestable-scopes="requestableScopes"
      @update-request="updatePermissionRequest"
      @submit="submitPermissionRequest"
      @confirm="confirmPermissionRequest"
    />

    <ScopeHome
      v-else-if="!isWorkbench"
      :scope-options="visibleScopeOptions"
      :overview="scopeOverview"
      :handover-links="handoverLinks"
      @enter="enterScope"
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
        <button class="btn ghost" @click="addManualDraft">纯手填</button>
        <button class="btn ghost" @click="showPasteParser = !showPasteParser">解析粘贴通告</button>
        <button v-if="isAdmin" class="btn ghost" @click="showMemoryImporter = !showMemoryImporter">导入历史记忆</button>
      </div>

      <section v-if="showPasteParser" class="paste-panel">
        <textarea v-model="pasteText" placeholder="粘贴完整维保、变更或检修通告文本"></textarea>
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
          </div>
          <div class="target-choice-layout">
            <div class="target-choice-list">
              <p v-if="!pendingChangeTargetSelection.candidates.length" class="target-empty-line">未找到同名目标多维记录，可先选择源表记录继续尝试关联。</p>
              <button
                v-for="item in pendingChangeTargetSelection.candidates"
                :key="changeTargetCandidateId(item)"
                class="target-choice"
                :class="{ active: selectedChangeTargetId === changeTargetCandidateId(item) }"
                @mouseenter="previewChangeTarget(item)"
                @focus="previewChangeTarget(item)"
                @click="selectChangeTarget(item)"
              >
                <strong>{{ item.title || item.record_id }}</strong>
                <span>{{ item.building || "-" }} · {{ item.status || "未标记状态" }} · {{ item.start_time || "-" }} 至 {{ item.end_time || "-" }}</span>
                <small>{{ item.date_matched ? "时间匹配" : "按名称匹配" }}</small>
              </button>
            </div>
            <aside v-if="activeChangeTargetCandidate" class="target-detail-popover">
              <div class="target-detail-head">
                <strong>{{ activeChangeTargetCandidate.title || `${workTypeLabel(pendingChangeTargetSelection.type)}记录` }}</strong>
                <span>{{ activeChangeTargetCandidate.building || "-" }} · {{ activeChangeTargetCandidate.status || "未标记状态" }}</span>
              </div>
              <dl class="target-detail-grid">
                <template v-for="row in changeTargetDetailRows(activeChangeTargetCandidate)" :key="row.label">
                  <dt>{{ row.label }}</dt>
                  <dd>{{ row.value }}</dd>
                </template>
              </dl>
              <button class="btn blue target-confirm" :disabled="changeTargetConfirming || !selectedChangeTargetId" @click="confirmPastedChangeTarget">
                {{ changeTargetConfirming ? "确认中" : "确认关联这条记录" }}
              </button>
            </aside>
          </div>
          <div v-if="changeSourceCandidates.length" class="source-choice-panel">
            <div>
              <strong>对应源表记录</strong>
              <p>选择源表记录后，后续状态、闭环和来源追踪会更准确；不选择也可用目标多维记录继续上传。</p>
            </div>
            <div class="source-choice-list">
              <button
                v-for="item in changeSourceCandidates"
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
              :disabled="changeTargetConfirming || !selectedChangeSourceId"
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
            从左侧选择事项，或使用纯手填、解析粘贴通告。
          </div>
          <div v-else ref="draftStackRef" class="draft-stack">
            <article
              v-for="row in selectedDraftRows"
              :key="row.key"
              class="draft-card"
              :class="{ active: row.key === activeDraftKey, collapsed: row.key !== activeDraftKey }"
              @click="activeDraftKey = row.key"
            >
              <div class="card-title">
                <strong>{{ row.title }}</strong>
                <span>{{ workTypeLabel(row.record.work_type) }}{{ row.key === activeDraftKey ? " · 正在编辑" : " · 点击编辑" }}</span>
              </div>
              <div v-if="row.key !== activeDraftKey" class="draft-compact">
                <p>{{ draftSummary(row.record, row.draft) || "已加入待发起通告，点击展开编辑。" }}</p>
                <div class="card-actions compact-actions">
                  <span class="job-line" :class="jobClass(row.key)">{{ jobText(row.key) }}</span>
                  <button class="btn ghost" :disabled="isLineBusy(row.key)" @click.stop="pinDraftInMiddlePanel(row.key)">编辑</button>
                  <button class="btn ghost" :disabled="isLineBusy(row.key)" @click.stop="removeDraft(row.key)">移除</button>
                </div>
              </div>
              <template v-else>
                <div class="form-grid">
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "title") }}
                    <input v-model="row.draft.title" placeholder="通告标题" @input="saveDrafts" />
                  </label>
                  <label v-if="row.record.manual">
                    楼栋/范围
                    <select v-model="row.draft.building" @change="onDraftBuildingChange(row.draft)">
                      <option value="">请选择</option>
                      <option v-for="item in requestableScopes" :key="item.value" :value="item.label">
                        {{ item.label }}
                      </option>
                    </select>
                  </label>
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "specialty") }}
                    <input v-model="row.draft.specialty" placeholder="专业" @input="saveDrafts" />
                  </label>
                  <label v-if="row.record.work_type === 'maintenance'">
                    {{ row.record.manual ? "维护周期" : "维保周期" }}
                    <select v-model="row.draft.maintenance_cycle" @change="saveDrafts">
                      <option value="">请选择</option>
                      <option v-for="item in maintenanceCycleOptions" :key="item" :value="item">{{ item }}</option>
                    </select>
                  </label>
                  <label v-if="row.record.manual && row.record.work_type === 'maintenance'" class="checkbox-field span-2">
                    <input v-model="row.draft.non_plan" type="checkbox" @change="saveDrafts" />
                    <span>非计划，发送时标题末尾自动追加“（非计划性）”</span>
                  </label>
                  <label v-if="row.record.work_type !== 'maintenance'">
                    {{ noticeFieldLabel(row.record.work_type, "level") }}
                    <input v-model="row.draft.level" placeholder="等级" @input="saveDrafts" />
                  </label>
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "start_time") }}
                    <input v-model="row.draft.start_time" type="datetime-local" @input="saveDrafts" />
                  </label>
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "end_time") }}
                    <input v-model="row.draft.end_time" type="datetime-local" @input="saveDrafts" />
                  </label>
                  <label class="span-2">
                    {{ noticeFieldLabel(row.record.work_type, "location") }}
                    <input v-model="row.draft.location" placeholder="地点" @input="saveDrafts" />
                  </label>
                  <label class="span-2">
                    {{ noticeFieldLabel(row.record.work_type, "content") }}
                    <textarea v-model="row.draft.content" placeholder="内容" @input="saveDrafts"></textarea>
                  </label>
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "reason") }}
                    <textarea v-model="row.draft.reason" placeholder="原因" @input="saveDrafts"></textarea>
                  </label>
                  <label>
                    {{ noticeFieldLabel(row.record.work_type, "impact") }}
                    <textarea v-model="row.draft.impact" placeholder="影响" @input="saveDrafts"></textarea>
                  </label>
                  <label class="span-2">
                    {{ noticeFieldLabel(row.record.work_type, "progress") }}
                    <textarea v-model="row.draft.progress" placeholder="进度" @input="saveDrafts"></textarea>
                  </label>
                </div>
                <section v-if="row.record.work_type === 'power'" class="repair-fields">
                  <h3>上电字段</h3>
                  <div class="form-grid">
                    <label><span>柜号</span><input v-model="row.draft.cabinet" @input="saveDrafts" /></label>
                    <label><span>数量</span><input v-model="row.draft.quantity" @input="saveDrafts" /></label>
                  </div>
                </section>
                <section v-if="row.record.work_type === 'polling'" class="repair-fields">
                  <h3>轮巡字段</h3>
                  <div class="form-grid">
                    <label class="span-2"><span>设备</span><input v-model="row.draft.device" @input="saveDrafts" /></label>
                  </div>
                </section>
                <section v-if="row.record.work_type === 'repair'" class="repair-fields">
                  <h3>检修字段</h3>
                  <div class="form-grid">
                    <label><span>维修设备</span><input v-model="row.draft.repair_device" @input="saveDrafts" /></label>
                    <label><span>维修故障</span><input v-model="row.draft.repair_fault" @input="saveDrafts" /></label>
                    <label><span>故障类型</span><input v-model="row.draft.fault_type" @input="saveDrafts" /></label>
                    <label><span>维修方式</span><input v-model="row.draft.repair_mode" @input="saveDrafts" /></label>
                    <label><span>故障发现方式</span><input v-model="row.draft.discovery" @input="saveDrafts" /></label>
                    <label><span>故障现象</span><input v-model="row.draft.symptom" @input="saveDrafts" /></label>
                    <label class="span-2"><span>解决方案</span><textarea v-model="row.draft.solution" @input="saveDrafts"></textarea></label>
                  </div>
                </section>
                <div v-if="row.record.work_type === 'change'" class="zhihang-line">
                  <label>
                    <input v-model="row.draft.zhihang_involved" type="checkbox" @change="saveDrafts" />
                    涉及智航
                  </label>
                  <select v-if="row.draft.zhihang_involved" v-model="row.draft.zhihang_record_id" @change="bindZhihang(row.draft)">
                    <option value="">选择智航变更</option>
                    <option v-for="item in zhihangRecords" :key="item.record_id" :value="item.record_id">
                      {{ item.title || item.record_id }}
                    </option>
                  </select>
                </div>
                <div class="card-actions">
                  <span class="job-line" :class="jobClass(row.key)">{{ jobText(row.key) }}</span>
                  <button class="btn blue" :disabled="isLineBusy(row.key)" @click.stop="sendStart(row.key)">
                    发送{{ draftActionLabel(row.record, row.draft) }}
                  </button>
                  <button class="btn ghost" :disabled="isLineBusy(row.key)" @click.stop="removeDraft(row.key)">移除</button>
                </div>
              </template>
            </article>
          </div>
        </section>

        <aside class="panel ongoing-panel">
          <div class="panel-head">
            <h2>已开始未结束</h2>
            <span>{{ liveOngoingCount }}</span>
          </div>
          <div v-if="ongoing.length === 0" class="empty-block">当前没有进行中通告</div>
          <div v-else class="ongoing-list">
            <article
              v-for="item in ongoing"
              :key="ongoingLineKey(item)"
              class="ongoing-card"
              :class="{ active: isOngoingExpanded(item), collapsed: !isOngoingExpanded(item) }"
              @click="expandOngoingCard(item)"
            >
              <div class="card-title" @click.stop="toggleOngoingCard(item)">
                <strong>{{ ongoingTitle(item) }}</strong>
                <span>{{ workTypeLabel(item.work_type) }}{{ isOngoingExpanded(item) ? " · 正在编辑" : " · 点击展开编辑" }}</span>
              </div>
              <p>{{ ongoingMeta(item) }}</p>
              <div v-if="!isOngoingExpanded(item)" class="ongoing-compact">
                <p>{{ ongoingCompactSummary(item) || "已开始未结束，点击展开后可更新、结束或删除。" }}</p>
                <div class="card-actions compact-actions">
                  <span class="job-line" :class="jobClass(ongoingLineKey(item))">{{ jobText(ongoingLineKey(item)) }}</span>
                </div>
              </div>
              <template v-else>
                <div class="ongoing-expanded" @click.stop>
                  <div class="form-grid">
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "title") }}
                      <input
                        :value="ongoingDraft(item).title"
                        placeholder="通告标题"
                        @input="setOngoingEdit(item, 'title', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "specialty") }}
                      <input
                        :value="ongoingDraft(item).specialty"
                        placeholder="专业"
                        @input="setOngoingEdit(item, 'specialty', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label v-if="(item.work_type || 'maintenance') === 'maintenance'">
                      维保周期
                      <select
                        :value="ongoingDraft(item).maintenance_cycle"
                        @change="setOngoingEdit(item, 'maintenance_cycle', ($event.target as HTMLSelectElement).value)"
                      >
                        <option value="">请选择</option>
                        <option v-for="cycle in maintenanceCycleOptions" :key="cycle" :value="cycle">{{ cycle }}</option>
                      </select>
                    </label>
                    <label v-if="(item.work_type || 'maintenance') !== 'maintenance'">
                      {{ noticeFieldLabel(item.work_type || "maintenance", "level") }}
                      <input
                        :value="ongoingDraft(item).level"
                        placeholder="等级"
                        @input="setOngoingEdit(item, 'level', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "start_time") }}
                      <input
                        :value="ongoingDraft(item).start_time"
                        type="datetime-local"
                        @input="setOngoingEdit(item, 'start_time', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "end_time") }}
                      <input
                        :value="ongoingDraft(item).end_time"
                        type="datetime-local"
                        @input="setOngoingEdit(item, 'end_time', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label class="span-2">
                      {{ noticeFieldLabel(item.work_type || "maintenance", "location") }}
                      <input
                        :value="ongoingDraft(item).location"
                        placeholder="地点"
                        @input="setOngoingEdit(item, 'location', ($event.target as HTMLInputElement).value)"
                      />
                    </label>
                    <label class="span-2">
                      {{ noticeFieldLabel(item.work_type || "maintenance", "content") }}
                      <textarea
                        :value="ongoingDraft(item).content"
                        placeholder="内容"
                        @input="setOngoingEdit(item, 'content', ($event.target as HTMLTextAreaElement).value)"
                      ></textarea>
                    </label>
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "reason") }}
                      <textarea
                        :value="ongoingDraft(item).reason"
                        placeholder="原因"
                        @input="setOngoingEdit(item, 'reason', ($event.target as HTMLTextAreaElement).value)"
                      ></textarea>
                    </label>
                    <label>
                      {{ noticeFieldLabel(item.work_type || "maintenance", "impact") }}
                      <textarea
                        :value="ongoingDraft(item).impact"
                        placeholder="影响"
                        @input="setOngoingEdit(item, 'impact', ($event.target as HTMLTextAreaElement).value)"
                      ></textarea>
                    </label>
                    <label class="span-2">
                      {{ noticeFieldLabel(item.work_type || "maintenance", "progress") }}
                      <textarea
                        :value="ongoingDraft(item).progress"
                        placeholder="进度"
                        @input="setOngoingEdit(item, 'progress', ($event.target as HTMLTextAreaElement).value)"
                      ></textarea>
                    </label>
                  </div>
                  <section v-if="(item.work_type || 'maintenance') === 'power'" class="repair-fields">
                    <h3>上电字段</h3>
                    <div class="form-grid">
                      <label><span>柜号</span><input :value="ongoingDraft(item).cabinet" @input="setOngoingEdit(item, 'cabinet', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>数量</span><input :value="ongoingDraft(item).quantity" @input="setOngoingEdit(item, 'quantity', ($event.target as HTMLInputElement).value)" /></label>
                    </div>
                  </section>
                  <section v-if="(item.work_type || 'maintenance') === 'polling'" class="repair-fields">
                    <h3>轮巡字段</h3>
                    <div class="form-grid">
                      <label class="span-2"><span>设备</span><input :value="ongoingDraft(item).device" @input="setOngoingEdit(item, 'device', ($event.target as HTMLInputElement).value)" /></label>
                    </div>
                  </section>
                  <section v-if="(item.work_type || 'maintenance') === 'repair'" class="repair-fields">
                    <h3>检修字段</h3>
                    <div class="form-grid">
                      <label><span>维修设备</span><input :value="ongoingDraft(item).repair_device" @input="setOngoingEdit(item, 'repair_device', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>维修故障</span><input :value="ongoingDraft(item).repair_fault" @input="setOngoingEdit(item, 'repair_fault', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>故障类型</span><input :value="ongoingDraft(item).fault_type" @input="setOngoingEdit(item, 'fault_type', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>维修方式</span><input :value="ongoingDraft(item).repair_mode" @input="setOngoingEdit(item, 'repair_mode', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>故障发现方式</span><input :value="ongoingDraft(item).discovery" @input="setOngoingEdit(item, 'discovery', ($event.target as HTMLInputElement).value)" /></label>
                      <label><span>故障现象</span><input :value="ongoingDraft(item).symptom" @input="setOngoingEdit(item, 'symptom', ($event.target as HTMLInputElement).value)" /></label>
                      <label class="span-2"><span>解决方案</span><textarea :value="ongoingDraft(item).solution" @input="setOngoingEdit(item, 'solution', ($event.target as HTMLTextAreaElement).value)"></textarea></label>
                    </div>
                  </section>
                  <div v-if="(item.work_type || 'maintenance') === 'change'" class="zhihang-line">
                    <label>
                      <input
                        :checked="Boolean(ongoingDraft(item).zhihang_involved)"
                        type="checkbox"
                        @change="setOngoingEdit(item, 'zhihang_involved', ($event.target as HTMLInputElement).checked)"
                      />
                      涉及智航
                    </label>
                    <select
                      v-if="ongoingDraft(item).zhihang_involved"
                      :value="ongoingDraft(item).zhihang_record_id"
                      @change="bindOngoingZhihang(item, ($event.target as HTMLSelectElement).value)"
                    >
                      <option value="">选择智航变更</option>
                      <option v-for="change in zhihangRecords" :key="change.record_id" :value="change.record_id">
                        {{ change.title || change.record_id }}
                      </option>
                    </select>
                  </div>
                  <div class="card-actions">
                    <span class="job-line" :class="jobClass(ongoingLineKey(item))">
                      {{ jobText(ongoingLineKey(item)) }}
                    </span>
                    <button class="btn blue" :disabled="isLineBusy(ongoingLineKey(item))" @click="sendOngoing(item, 'update')">更新</button>
                    <button class="btn green" :disabled="isLineBusy(ongoingLineKey(item))" @click="sendOngoing(item, 'end')">结束</button>
                    <button class="btn danger" :disabled="isLineBusy(ongoingLineKey(item))" @click="deleteOngoing(item)">删除</button>
                    <button v-if="item.undo_available" class="btn ghost" :disabled="isLineBusy(undoLineKey(item))" @click="applyUndo(item)">回退</button>
                  </div>
                  <div v-if="item.undo_available" class="undo-line">
                    {{ item.undo_label || "可回退上一步" }}
                    <span :class="jobClass(undoLineKey(item))">{{ jobText(undoLineKey(item)) }}</span>
                  </div>
                </div>
              </template>
            </article>
          </div>
          <div v-if="deletedUndoItems.length" class="closed-today">
            <div class="panel-head compact">
              <h3>最近删除可回退</h3>
              <span>{{ deletedUndoItems.length }}</span>
            </div>
            <article v-for="item in deletedUndoItems" :key="undoLineKey(item)" class="closed-card">
              <div>
                <strong>{{ item.title || "未命名通告" }}</strong>
                <p>{{ workTypeLabel(item.work_type) }} · {{ item.building || "-" }} · {{ formatUndoTime(item.undo_created_at) }}</p>
              </div>
              <button class="btn ghost" :disabled="isLineBusy(undoLineKey(item))" @click="applyUndo(item)">回退删除</button>
              <span class="job-line" :class="jobClass(undoLineKey(item))">{{ jobText(undoLineKey(item)) }}</span>
            </article>
          </div>
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
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import AdminTools from "./components/AdminTools.vue";
import AuthPanels from "./components/AuthPanels.vue";
import HistoryMemoryPage from "./components/HistoryMemoryPage.vue";
import ScopeHome from "./components/ScopeHome.vue";
import VirtualNoticeList, { type NoticeRow } from "./components/VirtualNoticeList.vue";

type Dict = Record<string, any>;
type ScopeOption = { value: string; label: string };

const workTypes = [
  { value: "maintenance", label: "维保" },
  { value: "change", label: "变更" },
  { value: "repair", label: "检修" },
  { value: "power", label: "上电" },
  { value: "polling", label: "轮巡" },
  { value: "adjust", label: "调整" },
];
const brandLogoSrc = "/assets/vnet-logo.png";
const buildingScopeCodes = ["110", "A", "B", "C", "D", "E", "H"];
const maintenanceCycleOptions = ["每月", "每季", "每年", "半年", "每两年", "每三年", "每五年", "冬季保温每日", "/"];
const nonPlanTitleSuffix = "（非计划性）";
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

const authChecking = ref(true);
const loading = ref(false);
const isHistoryMemoryPage = ref(window.location.pathname.replace(/\/$/, "") === "/admin/history-memory");
const repairRefreshing = ref(false);
const changeRefreshing = ref(false);
const isWorkbench = ref(false);
const currentScope = ref(new URLSearchParams(window.location.search).get("scope") || "");
const syncText = ref("准备中");
const workType = ref("maintenance");
const userSelectedWorkType = ref(false);
const searchText = ref("");
const activeDraftKey = ref("");
const activeOngoingKey = ref("");
const showPasteParser = ref(false);
const showMemoryImporter = ref(false);
const showAdminTools = ref(false);
const pasteText = ref("");
const pasteParseBusy = ref(false);
const pasteParseLine = ref("粘贴通告后解析。");
const pasteParseStatus = ref("");
const pendingChangeTargetSelection = ref<Dict | null>(null);
const selectedChangeTargetId = ref("");
const hoveredChangeTargetId = ref("");
const selectedChangeSourceId = ref("");
const changeTargetConfirming = ref(false);
const memoryImportText = ref("");
const memoryImportBusy = ref(false);
const memoryImportLine = ref("粘贴历史通告后导入。");
const memoryImportLineType = ref("");
const eventSource = ref<EventSource | null>(null);
const sseConnected = ref(false);
const activeItemsEventSource = ref<EventSource | null>(null);
const activeItemsConnected = ref(false);
const activeItemsUpdatePending = ref(false);

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
const draftStackRef = ref<HTMLElement | null>(null);
const fallbackPollTimers = new Map<string, number>();
const pollingJobs = new Set<string>();
let workbenchLoadSeq = 0;
let workbenchRefreshTimer: number | null = null;
let sseReconnectTimer: number | null = null;
let activeItemsReconnectTimer: number | null = null;
let lastActiveItemsSignature = "";
let activeItemsStreamScope = "";
let appDisposed = false;

const visibleScopeOptions = computed(() => auth.scopeOptions.length ? auth.scopeOptions : requestableScopes);
const isAdmin = computed(() => String(auth.user?.role || "").toLowerCase() === "admin");
const dailyStats = computed(() => dailySummary.value?.stats || {});
const closedSummaryItems = computed(() => {
  const items = Array.isArray(dailySummary.value?.items) ? dailySummary.value.items : [];
  return items.filter((item: Dict) => String(item?.status || "") === "已结束" || Boolean(item?.ended_at));
});
const deletedUndoItems = computed(() => availableUndoItems.value.filter((item: Dict) => String(item?.undo_action_type || item?.action_type || "").toLowerCase() === "delete"));
const liveDailyStats = computed(() => ({
  ...dailyStats.value,
  started: Math.max(0, Number(dailyStats.value.started || 0) + localSummaryAdjustments.started),
  updated: Math.max(0, Number(dailyStats.value.updated || 0) + localSummaryAdjustments.updated),
  ended: Math.max(0, Number(dailyStats.value.ended || 0) + localSummaryAdjustments.ended),
}));
const liveOngoingCount = computed(() => Math.max(0, ongoing.value.length + localSummaryAdjustments.ongoing));
const scopedRecords = computed(() => records.value.filter((record) => recordMatchesCurrentScope(record)));
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
const selectedDraftRows = computed(() => {
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

function workTypeLabel(value: string): string {
  return workTypes.find((item) => item.value === value)?.label || "维保";
}

function noticeFieldLabel(type: string, field: string): string {
  const workType = type || "maintenance";
  const labels: Record<string, Record<string, string>> = {
    maintenance: {
      title: "名称",
      specialty: "专业",
      start_time: "计划开始时间",
      end_time: "计划结束时间",
      location: "位置",
      content: "内容",
      reason: "原因",
      impact: "影响",
      progress: "进度",
    },
    change: {
      title: "名称",
      specialty: "专业",
      level: "变更等级",
      start_time: "计划开始时间",
      end_time: "计划结束时间",
      location: "位置",
      content: "内容",
      reason: "原因",
      impact: "影响",
      progress: "进度",
    },
    repair: {
      title: "标题",
      specialty: "专业",
      level: "紧急程度",
      start_time: "期望完成时间",
      end_time: "发现故障时间",
      location: "地点",
      content: "标题/补充内容",
      reason: "故障原因",
      impact: "影响范围",
      progress: "完成情况",
    },
    power: {
      title: "名称",
      specialty: "专业",
      start_time: "计划开始时间",
      end_time: "计划结束时间",
      location: "位置",
      content: "内容",
      reason: "原因",
      impact: "影响",
      progress: "进度",
    },
    polling: {
      title: "标题",
      specialty: "专业",
      start_time: "计划开始时间",
      end_time: "计划结束时间",
      location: "位置",
      content: "内容",
      reason: "原因",
      impact: "影响",
      progress: "进度",
    },
    adjust: {
      title: "名称",
      specialty: "专业",
      start_time: "计划开始时间",
      end_time: "计划结束时间",
      location: "位置",
      content: "内容",
      reason: "原因",
      impact: "影响",
      progress: "进度",
    },
  };
  return labels[workType]?.[field] || labels.maintenance[field] || field;
}

function fieldsOf(record: Dict | undefined): Dict {
  return record?.display_fields || {};
}

function todayInput(hour: number, minute: number): string {
  const d = new Date();
  d.setHours(hour, minute, 0, 0);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function toDatetimeLocal(value: string): string {
  const text = String(value || "").trim();
  const m = text.match(/(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})日?\D+(\d{1,2})[：:点](\d{1,2})?/);
  if (!m) return text.includes("T") ? text.slice(0, 16) : "";
  return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}T${m[4].padStart(2, "0")}:${(m[5] || "00").padStart(2, "0")}`;
}

function firstRepairField(record: Dict, names: string[]): string {
  const fields = fieldsOf(record);
  for (const name of names) {
    const value = String(fields[name] || "").trim();
    if (value && !["-", "--", "—", "——", "/", "无", "暂无"].includes(value)) return value;
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
  if (record.manual) return f["专业类别"] || f["专业"] || f["所属专业"] || "";
  const type = record.work_type || "maintenance";
  if (type === "change") return f["专业"] || "";
  if (type === "repair") return f["所属专业"] || f["专业（推送消息用）"] || "";
  return f["专业类别"] || "";
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
  const title = String(draft.title || draft.content || "").trim();
  return type === "maintenance" ? appendNonPlanTitleSuffix(title, Boolean(draft.non_plan)) : title;
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
  if (isRecordOngoing(record)) return "已在进行中 · 右侧处理";
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

function targetRecordIdForRecord(record: Dict): string {
  const summary = record?.work_summary || {};
  return String(summary.target_record_id || summary.feishu_record_id || summary.record_id || record?.target_record_id || "").trim();
}

function targetRecordIdForOngoing(item: Dict): string {
  return String(item.target_record_id || item.feishu_record_id || item.raw_record_id || item.record_id || "").trim();
}

function ongoingLineKey(item: Dict): string {
  return String(item.active_item_id || item.record_id || item.target_record_id || item.feishu_record_id || item.raw_record_id || "").trim();
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
  if (source) return source;
  const recordId = String(item.record_id || "").trim();
  return recordId && recordId !== targetRecordId ? recordId : "";
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

function isRecordOngoing(record: Dict): boolean {
  const title = titleForRecord(record).replace(/\s+/g, "");
  const sourceId = record.source_record_id || record.record_id;
  return ongoing.value.some((item) => {
    if ((item.work_type || "maintenance") !== (record.work_type || "maintenance")) return false;
    if (sourceId && item.source_record_id && item.source_record_id === sourceId) return true;
    return String(item.title || "").replace(/\s+/g, "") === title;
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
  const normalizedType = workTypes.some((item) => item.value === type) ? type : "maintenance";
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
    device: "",
    cabinet: "",
    quantity: "",
  };
}

function manualRecordFromDraft(key: string, draft: Dict): Dict {
  const type = draft.work_type || "maintenance";
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
    },
    target_record_id: draft.target_record_id || draft.record_id || "",
    building_codes: buildingCodes,
  };
}

function repairDraftDefaults(record: Dict): Dict {
  const memory = record.memory || {};
  return {
    start_time: todayInput(23, 50),
    end_time: toDatetimeLocal(firstRepairField(record, ["故障发生时间", "发现故障时间"])) || "",
    location: memory.location || "",
    content: memory.content || titleForRecord(record),
    level: memory.level || levelForRecord(record),
    specialty: memory.specialty || specialtyForRecord(record),
    reason: memory.reason || firstRepairField(record, ["故障原因", "故障维修原因"]),
    impact: memory.impact || "",
    progress: memory.progress || "",
    repair_device: memory.repair_device || repairDeviceText(record),
    repair_fault: memory.repair_fault || firstRepairField(record, ["维修故障", "故障维修原因"]),
    fault_type: memory.fault_type || firstRepairField(record, ["故障类型"]) || "设备故障",
    repair_mode: memory.repair_mode || firstRepairField(record, ["维修方式", "维修方", "供应商名称"]),
    discovery: memory.discovery || firstRepairField(record, ["对应来源"]),
    symptom: memory.symptom || firstRepairField(record, ["故障发生现象描述", "故障现象"]),
    solution: memory.solution || firstRepairField(record, ["解决方案", "维修方案", "后续整改措施"]),
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
        specialty: memory.specialty || specialtyForRecord(record),
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

function storageKey(): string {
  return `clipflow-vue-workbench:${currentOpenId()}:${currentScope.value || "ALL"}`;
}

function legacyStorageKey(): string {
  return `clipflow-vue-workbench:${currentScope.value || "ALL"}`;
}

function loadDrafts(): void {
  try {
    const raw = localStorage.getItem(storageKey()) || localStorage.getItem(legacyStorageKey()) || "{}";
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== "object") throw new Error("invalid draft payload");
    selectedKeys.clear();
    for (const key of Array.isArray(payload.selected) ? payload.selected : []) selectedKeys.add(String(key));
    drafts.clear();
    const draftPayload = payload.drafts && typeof payload.drafts === "object" ? payload.drafts : {};
    for (const [key, value] of Object.entries(draftPayload)) {
      if (value && typeof value === "object") drafts.set(key, value as Dict);
    }
  } catch {
    selectedKeys.clear();
    drafts.clear();
  }
}

function saveDrafts(): void {
  const payload: Dict = { selected: Array.from(selectedKeys), drafts: {} };
  for (const [key, value] of drafts.entries()) payload.drafts[key] = value;
  try {
    localStorage.setItem(storageKey(), JSON.stringify(payload));
  } catch {
    syncText.value = "草稿保存失败，请减少待发起通告数量后重试";
  }
}

async function api(path: string, options: RequestInit = {}): Promise<Dict> {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload.data || payload;
}

async function loadAuthStatus(): Promise<void> {
  authChecking.value = true;
  try {
    const data = await api(`/api/auth/status?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    auth.loggedIn = Boolean(data.logged_in);
    auth.user = data.user || {};
    auth.scopeOptions = data.scope_options || [];
    auth.loginUrl = data.login_url || "/api/auth/login";
  } finally {
    authChecking.value = false;
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
    ongoing.value = data.ongoing || [];
    pruneOngoingExpansion();
    zhihangRecords.value = data.zhihang_change_records || [];
    dailySummary.value = data.daily_summary || { date: "", items: [], stats: {} };
    resetLocalSummaryAdjustments();
    defaults.impact = data.defaults?.impact || defaults.impact;
    defaults.progress = data.defaults?.progress || defaults.progress;
    if (!userSelectedWorkType.value) {
      workType.value = resolveInitialWorkType(data.default_work_type || workType.value);
    }
    syncText.value = data.source_snapshot_ready === false ? "后台正在准备数据" : `数据 ${data.last_loaded_at || "已就绪"}`;
    await loadAvailableUndos(requestScope, requestSeq);
    pruneSelection();
  } catch (error: any) {
    if (requestSeq !== workbenchLoadSeq || requestScope !== currentScope.value) return;
    syncText.value = error?.message || "加载失败";
  } finally {
    if (requestSeq === workbenchLoadSeq && requestScope === currentScope.value) {
      loading.value = false;
    }
  }
}

async function loadAvailableUndos(scope = currentScope.value, requestSeq = workbenchLoadSeq): Promise<void> {
  if (!scope) {
    availableUndoItems.value = [];
    return;
  }
  try {
    const data = await api(`/api/notice-undo/available?scope=${encodeURIComponent(scope)}`);
    if (requestSeq !== workbenchLoadSeq || scope !== currentScope.value) return;
    availableUndoItems.value = Array.isArray(data.items) ? data.items : [];
  } catch {
    if (requestSeq === workbenchLoadSeq && scope === currentScope.value) {
      availableUndoItems.value = [];
    }
  }
}

function resolveInitialWorkType(preferred: string): string {
  const preferredType = workTypes.some((item) => item.value === preferred) ? preferred : "maintenance";
  if (recordTypeCounts.value[preferredType] > 0) return preferredType;
  const fallback = workTypes.find((item) => recordTypeCounts.value[item.value] > 0);
  return fallback?.value || preferredType;
}

function selectWorkType(value: string): void {
  workType.value = value;
  userSelectedWorkType.value = true;
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
  ongoingEdits.delete(key);
  if (activeOngoingKey.value === key) activeOngoingKey.value = "";
  return ongoing.value.length !== before;
}

function pruneOngoingExpansion(): void {
  if (!activeOngoingKey.value) return;
  const exists = ongoing.value.some((item) => ongoingLineKey(item) === activeOngoingKey.value);
  if (!exists) activeOngoingKey.value = "";
}

function terminalPhase(phase: string): boolean {
  return ["success", "failed"].includes(String(phase || ""));
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
  for (const item of deletedUndoItems.value) {
    keys.add(undoLineKey(item));
  }
  return keys;
}

function clearFallbackPoll(key: string): void {
  const timer = fallbackPollTimers.get(key);
  if (timer) window.clearTimeout(timer);
  fallbackPollTimers.delete(key);
}

function scheduleWorkbenchReload(delay = 350): void {
  if (appDisposed) return;
  if (workbenchRefreshTimer !== null) return;
  workbenchRefreshTimer = window.setTimeout(() => {
    workbenchRefreshTimer = null;
    if (appDisposed) return;
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

function pruneRuntimeState(): void {
  const visibleKeys = activeLineKeys();
  const now = Date.now();
  const staleBefore = now - 30 * 60 * 1000;
  for (const key of Array.from(ongoingEdits.keys())) {
    if (!visibleKeys.has(key)) ongoingEdits.delete(key);
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

function returnToHome(): void {
  if (currentScope.value) saveDrafts();
  stopActiveItemsSse();
  isWorkbench.value = false;
  currentScope.value = "";
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  selectedKeys.clear();
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
  if (currentScope.value && nextScope !== currentScope.value) {
    saveDrafts();
  }
  currentScope.value = nextScope;
  isWorkbench.value = true;
  userSelectedWorkType.value = false;
  activeDraftKey.value = "";
  activeOngoingKey.value = "";
  ongoingEdits.clear();
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

function addManualDraft(): void {
  const key = `manual:${workType.value}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  drafts.set(key, manualDraftDefaults(workType.value));
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
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

function normalizeNoticeLabel(label: string): string {
  return String(label || "").replace(/\s+/g, "").replace(/[：:]/g, "");
}

function parseSections(text: string): Dict {
  const sections: Dict = {};
  const re = /【([^】]+)】([\s\S]*?)(?=(?:\n\s*)*【[^】]+】|$)/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(text))) sections[normalizeNoticeLabel(match[1])] = String(match[2] || "").trim();
  return sections;
}

function sectionValue(sections: Dict, names: string[], fallback = ""): string {
  for (const name of names) {
    const value = sections[normalizeNoticeLabel(name)];
    if (String(value || "").trim()) return String(value).trim();
  }
  return fallback;
}

function pastedNoticeStatus(text: string): string {
  const match = String(text || "").match(/状态\s*[：:]\s*(开始|更新|结束)/);
  return match?.[1] || "开始";
}

function inferBuildingText(...values: string[]): string {
  const text = values.filter(Boolean).join("\n").toUpperCase();
  const patterns: Array<[RegExp, string]> = [
    [/110\s*(?:站|KV)?|110站/i, "110站"],
    [/(?:A楼|A栋|\bA\b)/i, "A楼"],
    [/(?:B楼|B栋|\bB\b)/i, "B楼"],
    [/(?:C楼|C栋|\bC\b)/i, "C楼"],
    [/(?:D楼|D栋|\bD\b)/i, "D楼"],
    [/(?:E楼|E栋|\bE\b)/i, "E楼"],
    [/(?:H楼|H栋|\bH\b)/i, "H楼"],
    [/园区|ABC|A\/B\/C|A、B、C/i, "园区"],
  ];
  for (const [pattern, label] of patterns) if (pattern.test(text)) return label;
  return "";
}

function splitNoticeTimeRange(value: string): { start: string; end: string } {
  const text = String(value || "").trim();
  if (!text) return { start: "", end: "" };
  const sameDayRange = text.match(
    /(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)\s*(\d{1,2}[：:点]\d{1,2})\s*(?:-|至|~|～|—|--)\s*(\d{1,2}[：:点]\d{1,2})/
  );
  if (sameDayRange) {
    const datePrefix = sameDayRange[1];
    return {
      start: toDatetimeLocal(`${datePrefix} ${sameDayRange[2]}`),
      end: toDatetimeLocal(`${datePrefix} ${sameDayRange[3]}`),
    };
  }
  const parts = text.split(/\s*(?:至|~|～|—|--)\s*/).filter(Boolean);
  if (parts.length >= 2) {
    const startRaw = parts[0];
    const endRaw = parts.slice(1).join(" ");
    const start = toDatetimeLocal(startRaw);
    let end = toDatetimeLocal(endRaw);
    if (!end && start) {
      const datePrefix = startRaw.match(/(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/)?.[1] || "";
      const endClock = endRaw.match(/(\d{1,2})[：:点](\d{1,2})?/)?.[0] || "";
      if (datePrefix && endClock) end = toDatetimeLocal(`${datePrefix} ${endClock}`);
    }
    return { start, end };
  }
  const matches = [...text.matchAll(/(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?\s*\d{1,2}[：:点]\d{0,2})/g)].map((item) => item[1]);
  if (matches.length >= 2) return { start: toDatetimeLocal(matches[0]), end: toDatetimeLocal(matches[1]) };
  return { start: toDatetimeLocal(text), end: "" };
}

function parsedActionFromStatus(status: string): string {
  if (status === "更新") return "update";
  if (status === "结束") return "end";
  return "start";
}

function parsedActionLabel(action: string): string {
  if (action === "update") return "更新";
  if (action === "end") return "结束";
  return "开始";
}

function pastedNoticeWorkType(text: string): string {
  if (/设备检修|检修通告/.test(text)) return "repair";
  if (/设备变更|变更通告/.test(text)) return "change";
  if (/上电通告|上下电通告|下电通告/.test(text)) return "power";
  if (/设备轮巡|轮巡通告/.test(text)) return "polling";
  if (/设备调整|调整通告/.test(text)) return "adjust";
  return "maintenance";
}

function changeTargetCandidateId(item: Dict): string {
  return String(item?.record_id || item?.target_record_id || "").trim();
}

function changeSourceCandidateId(item: Dict): string {
  return String(item?.source_record_id || item?.record_id || "").trim();
}

const activeChangeTargetCandidate = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  const candidates = Array.isArray(pending?.candidates) ? pending.candidates : [];
  if (!candidates.length) return null;
  const detailId = hoveredChangeTargetId.value || selectedChangeTargetId.value;
  return candidates.find((item: Dict) => changeTargetCandidateId(item) === detailId) || candidates[0];
});

const changeSourceCandidates = computed(() => {
  const pending = pendingChangeTargetSelection.value;
  return Array.isArray(pending?.sourceCandidates) ? pending.sourceCandidates : [];
});

const selectedChangeSourceCandidate = computed(() => {
  const id = selectedChangeSourceId.value;
  if (!id) return null;
  return changeSourceCandidates.value.find((item: Dict) => changeSourceCandidateId(item) === id) || null;
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

function firstCandidateField(fields: Dict, names: string[]): string {
  for (const name of names) {
    const value = fields?.[name];
    if (String(value ?? "").trim()) return String(value).trim();
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
  fillDraftBlank(next, "device", firstCandidateField(fields, ["设备", "维修设备"]));
  fillDraftBlank(next, "cabinet", firstCandidateField(fields, ["柜号"]));
  fillDraftBlank(next, "quantity", firstCandidateField(fields, ["数量（个）", "数量"]));
  return next;
}

function completeParsedNoticeDraft(type: string, draft: Dict, options: Dict = {}): void {
  const key = `manual:${type}:${Date.now()}-${Math.random().toString(16).slice(2)}`;
  Object.assign(draft, options);
  drafts.set(key, draft);
  selectedKeys.add(key);
  pinDraftInMiddlePanel(key);
  workType.value = type;
  pasteText.value = "";
  pendingChangeTargetSelection.value = null;
  selectedChangeTargetId.value = "";
  hoveredChangeTargetId.value = "";
  selectedChangeSourceId.value = "";
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
  try {
    const sections = parseSections(text);
    if (/事件通告/.test(text)) {
      throw new Error("前端暂不支持事件通告纯手填或解析，请在 Qt 主界面处理事件通告。");
    }
    const type = pastedNoticeWorkType(text);
    const draft = manualDraftDefaults(type);
    const status = pastedNoticeStatus(text);
    const action = parsedActionFromStatus(status);
    const timeRange = splitNoticeTimeRange(sectionValue(sections, ["时间"]));
    draft.parsed_action = action;
    draft.title = type === "change"
      ? sectionValue(sections, ["名称", "标题"])
      : sectionValue(sections, ["标题", "名称", "维修名称"]);
    draft.non_plan = /（非计划性）|（非计划）/.test(draft.title);
    draft.location = sectionValue(sections, ["地点", "位置"]);
    draft.specialty = sectionValue(sections, ["专业", "专业类别"]);
    draft.reason = sectionValue(sections, ["原因", "故障原因"]);
    draft.impact = sectionValue(sections, ["影响", "影响范围"]);
    draft.progress = sectionValue(sections, ["进度", "完成情况"]);
    draft.maintenance_cycle = sectionValue(sections, ["维保周期", "维护周期"]);
    draft.level = sectionValue(sections, ["等级", "变更等级", "紧急程度"]) || (type === "change" ? "I3" : "");
    draft.start_time = timeRange.start || draft.start_time;
    draft.end_time = timeRange.end || draft.end_time;
    draft.building = sectionValue(sections, ["楼栋", "变更楼栋", "所属楼栋"])
      || inferBuildingText(draft.title, draft.location, text)
      || defaultBuildingForCurrentScope();
    draft.building_codes = buildingCodesFromText(draft.building);
    draft.content = type === "repair" ? (sectionValue(sections, ["内容"], draft.title) || draft.title) : sectionValue(sections, ["内容"], draft.title);
    draft.repair_device = sectionValue(sections, ["维修设备"]);
    draft.repair_fault = sectionValue(sections, ["维修故障"]);
    draft.fault_type = sectionValue(sections, ["故障类型"]);
    draft.repair_mode = sectionValue(sections, ["维修方式"]);
    draft.discovery = sectionValue(sections, ["故障发现方式"]);
    draft.symptom = sectionValue(sections, ["故障现象"]);
    draft.solution = sectionValue(sections, ["解决方案"]);
    draft.device = sectionValue(sections, ["设备"]);
    draft.cabinet = sectionValue(sections, ["柜号"]);
    draft.quantity = sectionValue(sections, ["数量"]);
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
        };
        selectedChangeTargetId.value = changeTargetCandidateId(candidates[0]);
        hoveredChangeTargetId.value = selectedChangeTargetId.value;
        selectedChangeSourceId.value = sourceCandidates.length ? changeSourceCandidateId(sourceCandidates[0]) : "";
        pasteParseLine.value = candidates.length
          ? `找到 ${candidates.length} 条同名${workTypeLabel(type)}目标记录，请确认要${parsedActionLabel(action)}的记录。`
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
  const candidates = Array.isArray(pending.candidates) ? pending.candidates : [];
  const candidate =
    candidates.find((item: Dict) => changeTargetCandidateId(item) === selectedChangeTargetId.value) ||
    activeChangeTargetCandidate.value;
  const sourceOnly = !candidate && selectedChangeSourceCandidate.value;
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

function buildStartPayload(key: string): Dict | null {
  const record = draftRecordForKey(key);
  const draft = drafts.get(key);
  if (!record || !draft) return null;
  const type = record.work_type || "maintenance";
  const action = record.manual ? draftActionForRecord(record, draft) : sourceActionForRecord(record);
  const targetRecordId = record.manual ? String(draft.target_record_id || draft.record_id || "").trim() : targetRecordIdForRecord(record);
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
    specialty: draft.specialty || specialtyForRecord(record),
    record_id: action === "start" ? record.record_id : targetRecordId,
    source_record_id: record.manual ? (draft.source_record_id || "") : record.record_id,
    target_record_id: action !== "start" ? targetRecordId : "",
    source_progress: sourceProgressForRecord(record),
    building_codes: record.manual ? (draft.building_codes || []) : (record.building_codes || []),
    building: record.manual ? (draft.building || "") : buildingForRecord(record),
    title: record.manual ? manualDraftTitle(draft, type) : (type === "repair" ? draft.content : titleForRecord(record)),
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
  const record = draftRecordForKey(key);
  if (record && !recordMatchesCurrentScope(record)) {
    rememberJob(key, { text: "当前入口与通告楼栋不匹配，请切换到对应楼栋或园区后再发送", status: "failed", phase: "failed" });
    return;
  }
  if (payload.work_type === "maintenance" && payload.manual && !payload.maintenance_cycle) {
    rememberJob(key, { text: "纯手填维保必须选择维保周期", status: "failed", phase: "failed" });
    return;
  }
  await sendAction(payload, key);
  saveDrafts();
}

function ongoingDraft(item: Dict): Dict {
  const id = ongoingLineKey(item);
  if (!ongoingEdits.has(id)) {
    const timeRange = ongoingTimeRange(item);
    ongoingEdits.set(id, {
      title: item.title || item.content || "",
      specialty: item.specialty || "",
      maintenance_cycle: item.maintenance_cycle || "",
      level: item.level || "",
      start_time: timeRange.start,
      end_time: timeRange.end,
      location: item.location || "",
      content: item.content || "",
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
      discovery: item.discovery || "",
      symptom: item.symptom || "",
      solution: item.solution || "",
      device: item.device || "",
      cabinet: item.cabinet || "",
      quantity: item.quantity || "",
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

function buildOngoingPayload(item: Dict, action: string): Dict {
  const edit = ongoingDraft(item);
  const targetRecordId = targetRecordIdForOngoing(item);
  const sourceRecordId = sourceRecordIdForOngoing(item, targetRecordId);
  const workType = item.work_type || "maintenance";
  const startTime = draftValue(edit, "start_time", ongoingTimeRange(item).start);
  const endTime = draftValue(edit, "end_time", ongoingTimeRange(item).end);
  return {
    action,
    scope: currentScope.value || "ALL",
    work_type: workType,
    notice_type: item.notice_type || "",
    record_id: sourceRecordId || targetRecordId,
    target_record_id: targetRecordId,
    active_item_id: item.active_item_id || "",
    source_record_id: sourceRecordId,
    title: draftValue(edit, "title", item.title || item.content || ""),
    specialty: draftValue(edit, "specialty", item.specialty || ""),
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
    device: workType === "polling" ? draftValue(edit, "device", item.device || "") : "",
    cabinet: workType === "power" ? draftValue(edit, "cabinet", item.cabinet || "") : "",
    quantity: workType === "power" ? draftValue(edit, "quantity", item.quantity || "") : "",
    fault_time: workType === "repair" ? endTime : "",
    expected_time: workType === "repair" ? startTime : "",
    operation_id: opId(`${item.active_item_id || item.record_id}:${action}`),
  };
}

async function sendOngoing(item: Dict, action: string): Promise<void> {
  const key = ongoingLineKey(item);
  await sendAction(buildOngoingPayload(item, action), key);
}

async function sendAction(payload: Dict, lineKey: string): Promise<void> {
  try {
    rememberJob(lineKey, { text: "已受理，正在进入后台任务", status: "busy", phase: "accepted" });
    const data = await api("/api/workbench-actions", { method: "POST", body: JSON.stringify(payload) });
    rememberJob(lineKey, {
      job_id: data.job_id,
      payload,
      text: payload.work_type === "maintenance" ? "已受理，正在发送个人消息" : "已受理，等待主界面显示",
      status: "busy",
      phase: data.initial_phase || "accepted",
    });
    watchJob(data.job_id, lineKey);
  } catch (error: any) {
    rememberJob(lineKey, { text: error?.message || "提交失败", status: "failed", phase: "failed" });
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
  } else if (action === "update") {
    bumpLocalSummary("updated", 1);
  } else if (action === "end") {
    bumpLocalSummary("ended", 1);
    if (removeOngoingLine(lineKey)) {
      // The row was already removed locally; no additional ongoing delta needed.
    } else {
      bumpLocalSummary("ongoing", -1);
    }
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

function watchJob(jobId: string, lineKey: string): void {
  clearFallbackPoll(lineKey);
  if (eventSource.value && eventSource.value.readyState !== EventSource.CLOSED) {
    const delay = sseConnected.value ? 15000 : 6000;
    const timer = window.setTimeout(() => {
      fallbackPollTimers.delete(lineKey);
      if (!isLineBusy(lineKey)) return;
      void pollJob(jobId, lineKey);
    }, delay);
    fallbackPollTimers.set(lineKey, timer);
    return;
  }
  void pollJob(jobId, lineKey);
}

async function pollJob(jobId: string, lineKey: string): Promise<void> {
  const pollKey = `${jobId}:${lineKey}`;
  if (pollingJobs.has(pollKey)) return;
  pollingJobs.add(pollKey);
  try {
    for (let i = 0; i < 120; i += 1) {
      if (appDisposed) return;
      try {
        const data = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
        const phase = data.phase || data.status || "";
        const text = data.message || data.upload_message || phase || "处理中";
        rememberJob(lineKey, {
          phase,
          status: phase === "success" ? "success" : phase === "failed" ? "failed" : "busy",
          text: phase === "success" ? "成功" : phase === "failed" ? (data.error || text || "失败") : text,
        });
        if (terminalPhase(phase)) {
          clearFallbackPoll(lineKey);
          handleTerminalJob(lineKey, phase);
          return;
        }
      } catch {
        rememberJob(lineKey, { text: "后台处理中，等待状态同步", status: "busy" });
      }
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  } finally {
    pollingJobs.delete(pollKey);
  }
}

function startJobSse(): void {
  if (appDisposed) return;
  if (eventSource.value) eventSource.value.close();
  if (sseReconnectTimer !== null) {
    window.clearTimeout(sseReconnectTimer);
    sseReconnectTimer = null;
  }
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
      if (!job?.job_id) return;
      for (const [key, value] of jobStates.entries()) {
        if (value.job_id === job.job_id) {
          const phase = job.phase || "";
          rememberJob(key, {
            phase,
            status: phase === "success" ? "success" : phase === "failed" ? "failed" : "busy",
            text: phase === "success" ? "成功" : job.error || job.message || job.upload_message || phase,
          });
          if (terminalPhase(phase)) {
            clearFallbackPoll(key);
            handleTerminalJob(key, phase);
          }
        }
      }
    };
    source.onopen = () => {
      sseConnected.value = true;
    };
    source.onmessage = handleJobEvent;
    source.addEventListener("job", handleJobEvent);
    source.onerror = () => {
      sseConnected.value = false;
      if (eventSource.value === source && source.readyState === EventSource.CLOSED) {
        eventSource.value = null;
        if (!appDisposed && sseReconnectTimer === null) {
          sseReconnectTimer = window.setTimeout(() => {
            sseReconnectTimer = null;
            startJobSse();
          }, 5000);
        }
      }
    };
    eventSource.value = source;
  } catch {
    eventSource.value = null;
    sseConnected.value = false;
  }
}

function startActiveItemsSse(): void {
  if (appDisposed || !isWorkbench.value || !currentScope.value) return;
  const previousSource = activeItemsEventSource.value;
  activeItemsEventSource.value = null;
  if (previousSource) previousSource.close();
  if (activeItemsReconnectTimer !== null) {
    window.clearTimeout(activeItemsReconnectTimer);
    activeItemsReconnectTimer = null;
  }
  const scope = currentScope.value || "ALL";
  if (activeItemsStreamScope !== scope) {
    lastActiveItemsSignature = "";
    activeItemsStreamScope = scope;
  }
  try {
    const source = new EventSource(`/api/qt-active-items/stream?scope=${encodeURIComponent(scope)}`);
    source.onopen = () => {
      activeItemsConnected.value = true;
    };
    source.addEventListener("qt_active_items", (event: MessageEvent) => {
      let payload: Dict;
      try {
        payload = JSON.parse(event.data || "{}");
      } catch {
        return;
      }
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
    });
    source.addEventListener("error", () => {
      activeItemsConnected.value = false;
      if (activeItemsEventSource.value === source && source.readyState === EventSource.CLOSED) {
        activeItemsEventSource.value = null;
        if (!appDisposed && isWorkbench.value && activeItemsReconnectTimer === null) {
          activeItemsReconnectTimer = window.setTimeout(() => {
            activeItemsReconnectTimer = null;
            startActiveItemsSse();
          }, 5000);
        }
      }
    });
    activeItemsEventSource.value = source;
  } catch {
    activeItemsEventSource.value = null;
    activeItemsConnected.value = false;
  }
}

function stopActiveItemsSse(): void {
  if (activeItemsEventSource.value) activeItemsEventSource.value.close();
  activeItemsEventSource.value = null;
  activeItemsConnected.value = false;
  activeItemsUpdatePending.value = false;
  lastActiveItemsSignature = "";
  activeItemsStreamScope = "";
  if (activeItemsReconnectTimer !== null) {
    window.clearTimeout(activeItemsReconnectTimer);
    activeItemsReconnectTimer = null;
  }
}

async function deleteOngoing(item: Dict): Promise<void> {
  const key = ongoingLineKey(item);
  const targetRecordId = targetRecordIdForOngoing(item);
  const sourceRecordId = sourceRecordIdForOngoing(item, targetRecordId);
  if (!window.confirm(`确认删除「${ongoingTitle(item)}」？将同步删除 Qt 条目和多维记录。`)) return;
  try {
    rememberJob(key, { text: "删除中", status: "busy", phase: "deleting" });
    await api("/api/ongoing-items/delete", {
      method: "POST",
      body: JSON.stringify({
        scope: currentScope.value || "ALL",
        work_type: item.work_type || "maintenance",
        notice_type: item.notice_type || "",
        active_item_id: item.active_item_id || "",
        record_id: targetRecordId || sourceRecordId,
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
  if (!window.confirm(`确认对「${title}」执行${label}？\n\n回退会同步恢复本地状态、Qt/前端展示和多维目标表记录。`)) return;
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

function formatUndoTime(value: any): string {
  const numeric = Number(value || 0);
  const date = numeric > 0 ? new Date(numeric * 1000) : new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("zh-CN", { hour12: false });
}

function ongoingTitle(item: Dict): string {
  if ((item.work_type || "maintenance") === "repair") {
    const source = records.value.find((record) => record.record_id === item.source_record_id);
    if (source) return titleForRecord(source);
  }
  return item.title || item.content || "未命名通告";
}

function ongoingMeta(item: Dict): string {
  return [item.building, item.specialty, item.maintenance_cycle, item.time_str || item.start_time].filter(Boolean).join(" · ");
}

function ongoingCompactSummary(item: Dict): string {
  const edit = ongoingEdits.get(ongoingLineKey(item)) || {};
  const progress = draftValue(edit, "progress", item.progress || "");
  const location = draftValue(edit, "location", item.location || "");
  return [location, progress].filter(Boolean).join(" · ");
}

async function refreshRepair(): Promise<void> {
  repairRefreshing.value = true;
  try {
    await api(`/api/repair-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
  } catch (error: any) {
    syncText.value = error?.message || "刷新检修失败";
  } finally {
    repairRefreshing.value = false;
  }
}

async function refreshChange(): Promise<void> {
  changeRefreshing.value = true;
  try {
    await api(`/api/change-refresh?scope=${encodeURIComponent(currentScope.value || "ALL")}`);
    await loadWorkbench();
  } catch (error: any) {
    syncText.value = error?.message || "刷新变更失败";
  } finally {
    changeRefreshing.value = false;
  }
}

async function logout(): Promise<void> {
  await api("/api/auth/logout", { method: "POST", body: "{}" }).catch(() => null);
  window.location.href = "/";
}

async function submitPermissionRequest(): Promise<void> {
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
      syncText.value = "请选择功能";
      if (!eventSource.value) startJobSse();
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
  await loadAuthStatus();
  if (auth.loggedIn && !auth.scopeOptions.length) {
    await loadCurrentPermissionRequest();
  }
  if (auth.loggedIn && auth.scopeOptions.length) {
    await Promise.all([loadOverview(), loadHandoverLinks()]);
    if (currentScope.value) {
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
  if (eventSource.value) eventSource.value.close();
  if (activeItemsEventSource.value) activeItemsEventSource.value.close();
  sseConnected.value = false;
  activeItemsConnected.value = false;
  if (sseReconnectTimer !== null) window.clearTimeout(sseReconnectTimer);
  if (activeItemsReconnectTimer !== null) window.clearTimeout(activeItemsReconnectTimer);
  if (workbenchRefreshTimer !== null) window.clearTimeout(workbenchRefreshTimer);
  for (const timer of fallbackPollTimers.values()) window.clearTimeout(timer);
  fallbackPollTimers.clear();
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

.target-choice-layout {
  display: grid;
  grid-template-columns: minmax(220px, 0.9fr) minmax(280px, 1.1fr);
  gap: 10px;
  align-items: start;
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

.panel-head,
.card-title {
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

.panel-head span,
.card-title span {
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

.draft-card,
.ongoing-card {
  padding: 10px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.draft-card {
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.draft-card.collapsed {
  padding: 9px 10px;
  cursor: pointer;
  background: #fbfdff;
}

.draft-card.collapsed:hover {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.ongoing-card {
  display: grid;
  gap: 8px;
  transition: border-color 0.14s ease, background-color 0.14s ease, box-shadow 0.14s ease;
}

.ongoing-card.collapsed {
  padding: 9px 10px;
  cursor: pointer;
  background: #fbfdff;
}

.ongoing-card.collapsed:hover {
  border-color: #bfdbfe;
  background: #f8fbff;
}

.ongoing-card.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.ongoing-card p {
  color: #334155;
  font-size: 13px;
  line-height: 1.45;
}

.ongoing-compact {
  display: grid;
  gap: 6px;
}

.ongoing-compact p {
  display: -webkit-box;
  margin: 0;
  overflow: hidden;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  color: #475569;
}

.ongoing-expanded {
  display: grid;
  gap: 8px;
}

.ongoing-card textarea {
  min-height: 54px;
}

.undo-line {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: #64748b;
  font-size: 12px;
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

.draft-card.active {
  border-color: #2563eb;
  box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.12);
}

.draft-compact {
  display: grid;
  gap: 6px;
  margin-top: 6px;
}

.draft-compact p {
  overflow: hidden;
  margin: 0;
  color: #64748b;
  font-size: 13px;
  line-height: 1.45;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.compact-actions {
  margin-top: 0;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  margin-top: 8px;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

.checkbox-field {
  display: flex;
  align-items: center;
  gap: 8px;
}

.checkbox-field input {
  width: auto;
  flex: 0 0 auto;
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

.span-2 {
  grid-column: 1 / -1;
}

.repair-fields,
.zhihang-line,
.card-actions {
  margin-top: 10px;
}

.zhihang-line {
  display: grid;
  gap: 8px;
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

  .summary-strip,
  .form-grid {
    grid-template-columns: 1fr;
  }
}
</style>
