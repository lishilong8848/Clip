<template>
  <section ref="root" class="tool-check" aria-label="工具管理" data-v2-inline-feedback>
    <header class="tool-header">
      <div class="tool-title">
        <NButton v-if="!embedded || order" quaternary :aria-label="order ? '返回工具管理' : '返回专项功能'" :disabled="busy || !!workspace?.busy" @click="order ? leave(closeOrder) : back()"><ArrowLeft :size="19" /></NButton>
        <div>
          <p>{{ order ? `${order.scope}楼 · ${kindLabel(order)}工单` : '专项功能' }}</p>
          <h2>{{ order ? order.tool_name : '工具管理' }}</h2>
        </div>
      </div>
      <div class="tool-bar">
        <NButton v-if="board && !order && !workspace?.busy" :disabled="busy || addingTool" @click="openNewTool">添加工具</NButton>
        <NButton class="tool-refresh" :disabled="busy || !!workspace?.busy" :loading="loading || workspace?.busy === 'read'" @click="refresh"><RefreshCw :size="16" />刷新</NButton>
      </div>
    </header>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false" role="status">{{ message }}</NAlert>

    <section v-if="addingTool" class="tool-library" aria-label="添加工具">
      <h3>添加工具到底表</h3><p>仅填写需要的字段；结果未确认时请先核对底表，避免重复添加。</p>
      <div class="tool-fields"><label v-for="field in toolFields" :key="field.name">{{field.name}}
        <select v-if="field.type===3" v-model="newTool[field.name]"><option value="">请选择</option><option v-for="choice in field.options" :key="choice" :value="choice">{{choice}}</option></select>
        <input v-else-if="field.type===7" v-model="newTool[field.name]" type="checkbox" />
        <input v-else-if="field.type===2" v-model.number="newTool[field.name]" type="number" min="0" />
        <input v-else-if="field.type===5" v-model="newTool[field.name]" type="date" />
        <input v-else v-model="newTool[field.name]" maxlength="500" />
      </label></div>
      <div class="tool-bar"><NButton :disabled="busy" @click="addingTool=false">取消</NButton><NButton type="primary" :disabled="busy" @click="createTool">保存工具</NButton></div>
    </section>

    <NSpin v-if="!order" :show="loading && !linkedSession">
      <template v-if="board || linkedSession">
        <nav class="tool-nav" aria-label="工具管理分区">
          <NButton v-for="tab in tabs" :key="tab.value" :type="view === tab.value ? 'primary' : 'default'" :bordered="false"
            :aria-pressed="view === tab.value" :disabled="busy || !!workspace?.busy" @click="selectView(tab.value)">
            <component :is="tab.icon" :size="16" />{{ tab.label }}
            <span v-if="tab.count" class="tool-nav-count">{{ tab.count }}</span>
          </NButton>
        </nav>
        <NAlert v-if="view === 'settings' && board?.inventory_error && board.inventory_error !== error" class="tool-source-error" type="warning" :bordered="false">{{ board.inventory_error }}</NAlert>
        <ToolWorkspace v-if="view === 'workspace'" :key="linkedSession + ':' + linkedAsset" ref="workspace"
          :session-id="linkedSession" :asset-id="linkedAsset" :scope="scope" @asset-history="historyAsset = $event"
          @inventory-status="updateInventoryStatus" @records="selectView('followups')" />
        <div v-else-if="view === 'settings'" class="tool-settings-layout">
          <ToolManagementSettings />
          <section class="tool-library" aria-label="自检模板">
            <div class="tool-panel-heading">
              <div><h3>自检模板库</h3><span>{{ library?.templates.length ?? 0 }} 份 · XLSX</span></div>
              <div class="tool-bar">
                <NButton :disabled="busy" @click="chooseImport()"><Upload :size="16" />导入模板</NButton>
                <NButton :disabled="busy" @click="folderInput?.click()"><Upload :size="16" />导入文件夹</NButton>
              </div>
            </div>
            <p v-if="library?.warning" class="tool-muted" role="status">{{ library.warning }}</p>
            <p v-if="folderProgress" class="tool-muted" role="status">{{ folderProgress }}</p>
            <details v-if="folderFailures.length" open>
              <summary>{{ folderFailures.length }} 份模板未导入</summary>
              <ul><li v-for="(item,index) in folderFailures" :key="index">{{ item.file }} · {{ item.reason }}</li></ul>
            </details>
            <article v-for="template in library?.templates || []" :key="template.id" class="tool-template">
              <div><strong>{{ template.name }}</strong><small>V{{ template.version }} · {{ template.schema.checks.length }} 项 · {{ template.active ? '已启用' : '已停用' }} · {{ template.linked_tools_count || 0 }} 台已关联</small></div>
              <div class="tool-bar">
                <NButton size="small" :disabled="busy" @click="startTemplateClone(template)">基于此模板新建</NButton>
                <NButton size="small" :disabled="busy" @click="chooseImport(template)">导入新版</NButton>
                <NButton size="small" :disabled="busy" @click="toggleTemplate(template)">{{ template.active ? '停用' : '启用' }}</NButton>
                <NButton v-if="template.linked_tools_count" size="small" :disabled="busy" @click="startRebind(template)">批量换绑</NButton>
                <NPopconfirm positive-text="确认删除" negative-text="取消" :positive-button-props="{ type: 'error' }"
                  @positive-click="deleteTemplate(template)">
                  <template #trigger><NButton size="small" type="error" quaternary :disabled="busy || !!template.linked_tools_count">删除</NButton></template>
                  删除「{{ template.name }}」？<template v-if="template.open_usage">该模板正被 {{ template.open_usage }} 张未提交工单引用：删除后这些工单仍按已保存的版本继续填写和提交，但不能再新选此模板。</template><template v-else>当前没有未提交工单引用它。</template>已有自检记录仍可查看和导出。
                </NPopconfirm>
              </div>
            </article>
            <NModal :show="!!rebindTemplate" preset="card" title="批量换绑自检模板" class="tool-workspace-dialog"
              @update:show="closeRebind">
              <p>将授权楼栋内仍关联「{{ rebindTemplate?.name }}」的工具关联至目标模板。已有草稿和历史继续使用原版本。</p>
              <NSelect v-model:value="rebindTarget" :options="rebindOptions" aria-label="目标自检模板" placeholder="选择目标模板" :disabled="busy" />
              <p v-if="rebindProgress" role="status">{{ rebindProgress }}</p>
              <p v-if="error" role="alert">{{ error }}</p>
              <ul v-if="rebindFailures.length"><li v-for="(failure, index) in rebindFailures" :key="index">{{ failure.scope }}楼 · {{ failure.name }} · {{ failure.error }}</li></ul>
              <template #footer><div class="tool-bar">
                <NButton :disabled="busy" @click="rebindTemplate = undefined">关闭</NButton>
                <NButton type="primary" :disabled="busy || !rebindTarget" :loading="busy" @click="applyRebind">确认换绑</NButton>
              </div></template>
            </NModal>
            <details v-if="library?.legacy_templates.length" class="tool-context">
              <summary>原有本地模板（{{ library.legacy_templates.length }} 份）</summary>
              <article v-for="template in library.legacy_templates" :key="template.id" class="tool-template">
                <div><strong>{{ template.name }}</strong><small>V{{ template.version }} · {{ template.open_usage || 0 }} 张未提交工单引用</small></div>
                <NButton size="small" :disabled="busy" @click="startTemplateClone(template)">另存到模板库</NButton>
              </article>
            </details>
            <section v-if="templateClone" class="tool-edit-panel" aria-label="新建自检模板">
              <h3>新建自检模板</h3>
              <label>模板名称 <input v-model="templateClone.name" maxlength="100" /></label>
              <div v-for="(item,index) in templateClone.checks" :key="index" class="tool-edit-panel">
                <strong>检查项 {{ index+1 }}</strong>
                <label v-if="!item.sharedName">自检项目 <input v-model="item.name" maxlength="300" /></label>
                <strong v-else>{{ item.name }}</strong>
                <label>操作方法 <textarea v-model="item.method" maxlength="1500" /></label>
                <label v-if="!templateClone.hvac">合格标准 <textarea v-model="item.standard" maxlength="1500" /></label>
              </div>
              <div class="tool-bar"><NButton :disabled="busy" @click="templateClone=null">取消</NButton><NButton type="primary" :disabled="busy" @click="saveTemplateClone">保存新模板</NButton></div>
            </section>
            <input ref="fileInput" type="file" accept=".xlsx" hidden @change="importTemplate" />
            <input ref="folderInput" type="file" accept=".xlsx" webkitdirectory multiple hidden @change="importTemplateFolder" />
          </section>
        </div>
        <ToolWorkspace v-else key="records" ref="workspace" records-only :scope="scope"
          :followup-order-ids="followupOrderIds" :inventory-warning="board?.inventory_error"
          @records-refresh="loadBoard()" @inventory-status="updateInventoryStatus" @choose="selectView('workspace')">
        <template #followups>
        <div class="tool-worklist" aria-label="检修与报废">
          <div class="tool-list-head">
            <h4>检修与报废</h4>
            <div class="tool-counts">
              <span><strong>{{ repairCount }}</strong> 检修中</span>
              <span><strong>{{ approvalCount }}</strong> 待报废审批</span>
              <span><strong>{{ completedCount }}</strong> 已处置</span>
            </div>
          </div>
          <div class="tool-filters" aria-label="筛选工单">
            <NInput v-model:value="search" class="tool-search" clearable :disabled="busy" :input-props="{ 'aria-label': '搜索工具、楼栋、位置或领取人' }" placeholder="搜索工具或楼栋">
              <template #prefix><Search :size="16" /></template>
            </NInput>
            <NSelect v-model:value="stateFilter" aria-label="工单状态" :options="stateOptions" :disabled="busy" />
          </div>
          <div v-if="visibleOrders.length" class="tool-orders" role="table" aria-label="检修和报废记录">
            <div class="tool-column-labels" role="row">
              <span role="columnheader">工具与位置</span><span role="columnheader">楼栋与类型</span>
              <span role="columnheader">办理人员</span><span role="columnheader">状态</span><span role="columnheader">操作</span>
            </div>
            <article v-for="row in pagedOrders" :key="row.id" class="tool-order" role="row">
              <div class="tool-order-name" role="cell">
                <div>
                  <strong>{{ row.tool_name }}</strong>
                  <small v-if="row.model || row.location">{{ [row.model, row.location].filter(Boolean).join(' · ') }}</small>
                  <NButton v-if="row.asset_id" text size="tiny" :disabled="busy" @click="historyAsset = row.asset_id || ''">履历</NButton>
                  <NPopconfirm v-if="row.can_remove_followup && !row.deletion_pending" positive-text="删除" negative-text="取消"
                    :positive-button-props="{ type: 'error' }" @positive-click="removeFollowup(row)">
                    <template #trigger><NButton size="small" type="error" text :disabled="busy">删除</NButton></template>
                    删除「{{ row.tool_name }}」的本次办理及对应飞书台账记录？
                  </NPopconfirm>
                  <small v-if="row.deletion_pending" class="tool-pending" role="status">{{ row.deletion_error || '等待飞书确认删除' }}</small>
                </div>
              </div>
              <div class="tool-order-kind" role="cell"><strong>{{ row.scope }}楼</strong><small>{{ kindLabel(row) }}工单</small></div>
              <div class="tool-order-meta" role="cell">
                <small>{{ assignment(row) }}</small>
                <small v-if="row.submitted_at">提交 {{ row.submitted_at.slice(0,10) }}</small>
              </div>
              <div class="tool-order-state" role="cell">
                <NTag :type="row.result_label === '合格' ? 'success' : 'warning'" :bordered="false">{{ row.tool_status_label || outcomeLabels[row.outcome] }}</NTag>
                <small v-if="row.result_label">{{ row.result_label }}<span v-if="row.outcome === 'scrap'"> · 任一经理同意即可</span></small>
                <small v-if="row.sync.length" :class="{ 'tool-pending': row.sync.some(item => item.state !== 'done') }">
                  {{ row.sync.some(item => item.verifying) ? '飞书待核验' : row.sync.some(item => item.error) ? '同步失败 · 自动重试'
                    : row.sync.some(item => item.state !== 'done') ? '飞书待同步' : '飞书已同步' }}
                </small>
              </div>
              <div class="tool-order-actions" role="cell">
                <NButton size="small" :type="row.can_claim || row.can_edit || row.can_approve || row.can_repair ? 'primary' : 'default'"
                  :disabled="busy || row.deletion_pending" @click="openOrder(row, row.can_claim)">{{ orderAction(row) }}</NButton>
                <NPopconfirm v-if="row.can_release" @positive-click="release(row)">
                  <template #trigger><NButton size="small" :disabled="busy">释放领取</NButton></template>
                  草稿将保留，工单可重新领取。
                </NPopconfirm>
              </div>
            </article>
          </div>
          <NEmpty v-else class="tool-empty" :description="emptyDescription" />
          <footer v-if="visibleOrders.length" class="tool-list-foot">
            <span>共 {{ visibleOrders.length }} 张</span>
            <NPagination v-if="visibleOrders.length > pageSize" :page="page" :item-count="visibleOrders.length" :page-size="pageSize" :disabled="busy || loading" simple @update:page="changePage" />
          </footer>
        </div>
        </template>
        </ToolWorkspace>
      </template>
    </NSpin>

    <div v-else class="tool-form">
      <section class="tool-overview" aria-label="工单概况">
        <div class="tool-bar">
          <NTag :class="{ 'tool-status-closed': order.status !== 'pending' }" :type="order.status === 'pending' ? 'success' : 'default'" :bordered="false">{{ status(order) }}</NTag>
          <span>{{ assignment(order) }}</span>
          <span v-if="order.submitted_at" class="tool-muted">{{ displayTime(order.submitted_at) }} 提交</span>
          <NButton v-if="order.status === 'submitted'" class="tool-download" size="small" :disabled="busy" @click="download(orderPath(order) + '/download')"><Download :size="16" />下载本单</NButton>
        </div>
        <dl class="tool-overview-fields">
          <div><dt>计划日期</dt><dd>{{ order.due }}</dd></div>
          <div v-if="order.model"><dt>规格型号</dt><dd>{{ order.model }}</dd></div>
          <div v-if="order.location"><dt>存放位置</dt><dd>{{ order.location }}</dd></div>
        </dl>
        <p v-if="order.result_notice" class="tool-pending tool-context" role="status">{{ order.result_notice }}</p>
        <div v-if="order.sync.length" class="tool-bar tool-context">
          <NTag v-for="item in order.sync" :key="item.label" :type="item.state === 'done' ? 'success' : 'warning'"
            :title="item.error" :bordered="false">{{ item.label }} · {{ item.state === 'done' ? '已同步' : '待同步' }}</NTag>
        </div>
        <p v-for="item in order.sync.filter(item => item.error)" :key="item.label" class="tool-pending tool-context">{{ item.label }} · {{ item.error }}</p>
      </section>
      <div class="tool-form-layout" :class="{ 'tool-has-aside': order.kind === 'self' && (order.can_edit || order.outcome || order.status==='review_pending') }">
        <section class="tool-edit-panel" :aria-label="order.kind === 'external' ? '送检报告' : '自检记录'">
          <template v-if="order.kind === 'external'">
            <div class="tool-panel-heading"><h3>送检报告</h3></div>
            <div class="tool-report-layout">
              <label v-if="order.can_edit">实际检测日期 <input v-model="actual" type="date" :disabled="busy" /></label>
              <div v-else-if="order.actual"><span class="tool-muted">实际检测日期</span><strong>{{ order.actual }}</strong></div>
              <div class="tool-report-file">
                <FileText :size="19" />
                <div><strong>{{ order.report_name || '尚未上传送检报告' }}</strong><small v-if="order.can_edit && !order.report_name">支持 PDF、Word、Excel 或 JPG／PNG，最多 12 MB</small></div>
                <div v-if="order.report_name || order.can_edit" class="tool-bar tool-report-actions">
                  <NButton v-if="order.report_name" tag="a" :href="api + orderPath(order) + '/report-file'" target="_blank" rel="noopener" size="small">查看报告</NButton>
                  <NButton v-if="order.can_edit" size="small" type="primary" secondary :disabled="busy" @click="reportInput?.click()"><Upload :size="16" />{{ order.report_name ? '更换报告' : '上传送检报告' }}</NButton>
                </div>
                <input ref="reportInput" type="file" accept=".pdf,.docx,.xlsx,.jpg,.jpeg,.png" hidden @change="uploadReport" />
              </div>
            </div>
            <ToolInspectionReportFields :model="reportCheck" :asset-ids="reportAssets" :actual="actual"
              :expires-on="order.inspection_expires_on" :editable="order.can_edit" :disabled="busy || !order.report_name" />
          </template>
          <div v-else class="tool-panel-heading"><h3>自检记录</h3><span v-if="records.length">{{ records.length }} 份 · 已填 {{ answered }}/{{ checkCount }} 项</span></div>
          <div v-if="order.can_edit && order.kind === 'self'" class="tool-bar tool-template-picker">
            <NSelect v-model:value="templateId" :options="templateOptions" :disabled="busy" filterable aria-label="选用自检模板" placeholder="选择适用的工具模板" />
            <NButton type="primary" secondary :disabled="busy || !templateId || records.length >= 20" @click="addRecord"><Plus :size="16" />选用模板</NButton>
          </div>
          <div v-if="records.length" class="tool-bar tool-records" aria-label="工单自检记录">
            <button v-for="(record,index) in records" :key="record.entry_id" type="button" :aria-pressed="selected === record.entry_id" @click="selected = record.entry_id">{{ index + 1 }}. {{ record.template.name }}</button>
          </div>
          <NEmpty v-if="order.kind === 'self' && !active" :description="order.can_edit ? '选用工具模板后开始填写' : '暂无填写记录'" />
          <template v-if="active">
            <div class="tool-bar tool-record-heading">
              <strong>{{ active.template.name }}</strong><span>模板 V{{ active.version }}</span>
              <NButton v-if="order.can_edit" size="small" :disabled="busy" @click="removeRecord(active.entry_id)"><Trash2 :size="15" />移除本份</NButton>
            </div>
            <p v-if="[active.template.schema.applicable, active.template.schema.specialty, active.template.schema.cycle].some(Boolean)" class="tool-muted tool-context">{{ [active.template.schema.applicable, active.template.schema.specialty, active.template.schema.cycle].filter(Boolean).join(' · ') }}</p>
            <p v-if="active.template.schema.precautions" class="tool-context">{{ active.template.schema.precautions }}</p>
            <fieldset class="tool-fields" :disabled="busy || !order.can_edit">
              <label>工具存放位置 <input v-model="active.location" maxlength="240" required /></label>
              <label>检查人 <input v-model="active.inspector" maxlength="80" required /></label>
              <label class="tool-time">检查开始时间 <input v-model="active.checked_at" type="datetime-local" required /></label>
            </fieldset>
            <fieldset v-if="active.template.schema.fields && active.template_values" class="tool-fields" :disabled="busy || !order.can_edit">
              <label v-for="(label,address) in active.template.schema.fields" :key="address">{{ label }}
                <input v-model="active.template_values[address]" maxlength="200" />
              </label>
            </fieldset>
            <div class="tool-grid" aria-label="自检项目">
              <div class="tool-grid-head" aria-hidden="true"><b>自检项目</b><b>操作方法</b><b>合格标准</b><b>完成情况</b></div>
              <article v-for="(check,index) in active.template.schema.checks" :key="`${active.entry_id}-${check.row}`" class="tool-grid-row">
                <strong>{{ index + 1 }}. {{ check.name }}</strong>
                <p><span>操作方法</span>{{ check.method }}</p>
                <p><span>合格标准</span>{{ check.standard || '—' }}</p>
                <NRadioGroup v-if="order.can_edit" v-model:value="active.checks[index]" :name="`${active.entry_id}-${index}`" :disabled="busy" :aria-label="check.name + '完成情况'">
                  <NRadioButton value="是">是</NRadioButton><NRadioButton value="否">否</NRadioButton>
                </NRadioGroup>
                <span v-else>{{ active.checks[index] || '未填写' }}</span>
              </article>
            </div>
            <div v-if="active.template.schema.calibration?.length && active.template_values">
              <h3>校准记录</h3>
              <fieldset v-for="(entry,index) in active.template.schema.calibration" :key="entry.row"
                class="tool-fields" :disabled="busy || !order.can_edit" :aria-label="entry.label+' '+(index+1)">
                <label v-for="(label,address) in entry.fields" :key="address">{{ label }}
                  <input v-model="active.template_values[address]" maxlength="200" />
                </label>
              </fieldset>
            </div>
            <details v-if="order.can_edit || active.reviewer || active.reviewed_at || active.notes" class="tool-audit">
              <summary>自检说明与审核</summary>
              <fieldset class="tool-fields" :disabled="busy || !order.can_edit">
                <label class="tool-notes">自检说明 <textarea v-model="active.notes" rows="2" maxlength="1500" /></label>
              </fieldset>
              <p v-if="active.reviewer">审核人：{{ active.reviewer }} · {{ displayTime(active.reviewed_at) }}</p>
            </details>
          </template>
        </section>
        <aside v-if="order.kind === 'self' && (order.can_edit || order.outcome || order.status==='review_pending')" class="tool-result" aria-label="处理结果">
          <div class="tool-panel-heading"><h3>处理结果</h3></div>
          <div v-if="order.can_edit" class="tool-result-fields">
            <label>工具状态 <NSelect v-model:value="outcome" aria-label="工具状态" placeholder="请选择检查结果" :options="outcomeOptions" :disabled="busy" /></label>
            <label v-if="outcome !== 'normal'">处理原因 <textarea v-model="reason" rows="2" maxlength="1000" :disabled="busy" /></label>
            <label v-if="outcome === 'scrap'">报废数量 <input v-model.number="quantity" type="number" min="1" :max="order.quantity" :disabled="busy" /></label>
          </div>
          <div v-if="order.outcome && !order.can_edit" class="tool-context">
            <NTag :type="order.result_label === '合格' ? 'success' : 'warning'">{{ order.tool_status_label || outcomeLabels[order.outcome] || '已取消' }}</NTag>
            <p v-if="order.reason">{{ order.reason }}</p><p v-if="order.resolution">{{ order.resolution }}</p>
            <p v-if="order.scrap_approvals?.length">{{ order.scrap_approvals.filter(vote => vote.decision || order?.outcome === 'scrap').map(vote => vote.name + ' · ' + voteLabel(vote.decision)).join('　') }}</p>
          </div>
          <div v-if="order.status==='review_pending'" class="tool-context"><strong>待审核结果：{{ outcomeLabels[outcome] || outcome }}</strong><p v-if="reason">处理原因：{{reason}}</p><p v-if="outcome==='scrap'">报废数量：{{quantity}}</p></div>
          <div v-if="order.can_repair || order.can_approve" class="tool-result-fields">
            <label>处理说明 <textarea v-model="reason" rows="2" maxlength="1000" :disabled="busy" /></label>
            <label v-if="order.can_repair">转报废数量 <input v-model.number="quantity" type="number" min="1" :max="order.quantity" :disabled="busy" /></label>
            <label v-if="order.can_repair">报废现场照片（最多三张 JPG 或 PNG）
              <input type="file" accept="image/jpeg,image/png" multiple :disabled="busy" @change="selectScrapPhotos" />
            </label>
            <div v-if="repairPhotos.length" class="tool-bar">
              <img v-for="(photo, index) in repairPhotos" :key="index" :src="photo" alt="报废现场照片" width="56" height="56" />
              <NButton size="small" :disabled="busy" @click="repairPhotos = []">清除照片</NButton>
            </div>
            <NPopconfirm v-for="step in transitions" :key="step.action" @positive-click="advance(step.action)">
              <template #trigger><NButton :type="step.type" :disabled="busy">{{ step.label }}</NButton></template>
              {{ step.action === 'repair-scrap' ? '提交检修后报废，梅冰冰、马进宇任一位同意即可。' : '确认' + step.label + '？' }}
            </NPopconfirm>
          </div>
        </aside>
      </div>
      <details v-if="order.history.length" class="tool-audit">
        <summary>操作记录 · {{ order.history.length }} 条</summary>
        <p v-for="(entry,index) in order.history" :key="index">{{ displayTime(entry.at) }} · {{ entry.name }} · {{ entry.action }}<span v-if="entry.detail"> · {{ entry.detail }}</span></p>
      </details>
      <section v-if="order.can_review" class="tool-result" aria-label="自检审核">
        <strong>本楼楼长审核</strong><p>审核通过后才会同步对应台账；驳回后可由填报人修改。</p>
        <label>审核意见 <textarea v-model="reviewReason" rows="2" maxlength="1000" :disabled="busy" /></label>
        <div class="tool-bar"><NButton :disabled="busy" @click="advance('review-reject')">驳回修改</NButton><NButton type="primary" :disabled="busy" @click="advance('review-approve')">审核通过</NButton></div>
      </section>
      <footer v-if="order.can_edit" class="tool-bar tool-submit">
        <span v-if="order.kind === 'self'">{{ records.length }} 份记录 · {{ answered }} / {{ checkCount }} 项已填写</span>
        <span v-else>{{ order.report_name ? '送检报告已保存' : '上传报告后可提交' }}</span>
        <NButton v-if="order.kind === 'self'" :disabled="busy || !dirty" @click="save('draft')">保存草稿</NButton>
        <NPopconfirm @positive-click="save('submit')">
          <template #trigger><NButton type="primary" :disabled="busy || (order.kind === 'self' ? !records.length : !order.report_name || !inspectionReady(reportCheck, reportAssets, actual))">提交工单</NButton></template>
          提交后锁定工单并纳入本月归档。
        </NPopconfirm>
      </footer>
    </div>
    <ToolAssetHistory :asset-id="historyAsset" @close="historyAsset = ''" @open-order="openWorkspaceOrder" @open-workspace="openHistoryWorkspace" />
    <NModal :show="!!afterDiscard" preset="dialog" title="尚未保存" content="离开后，本次未保存的填写将丢失。" positive-text="放弃填写" negative-text="继续填写" :mask-closable="false" @positive-click="discard" @negative-click="afterDiscard = null" @update:show="show => { if (!show) afterDiscard = null; }" />
  </section>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NEmpty, NInput, NModal, NPagination, NPopconfirm, NRadioButton, NRadioGroup, NSelect, NSpin, NTag } from 'naive-ui';
import { ArrowLeft, ClipboardList, Download, FileText, Plus, RefreshCw, Search, Settings2, Trash2, Upload, Wrench } from 'lucide-vue-next';
import { ApiError, downloadFile, requestBinaryJson, requestJson } from '../api/client';
import { navigateBack, registerNavigationGuard } from '../navigation';
import ToolInspectionReportFields, { emptyInspectionReport, inspectionReady, type InspectionReport } from './ToolInspectionReportFields.vue';
import ToolAssetHistory from './ToolAssetHistory.vue';
import ToolWorkspace from './ToolWorkspace.vue';
const ToolManagementSettings = defineAsyncComponent(() => import('./ToolManagementSettings.vue'));

const props = defineProps<{ scope?: string; embedded?: boolean; toolView?: string; toolSession?: string; toolAsset?: string; toolOrder?: string }>();
const api = '/api/tool-self-check';
interface Template {
  id: string; version: number; name: string; active: boolean; open_usage?: number; linked_tools_count?: number;
  schema: { specialty: string; cycle: string; applicable: string; precautions: string;
    checks: { row: number; name: string; method: string; standard: string; group_row?:number }[];
    layout?:'hvac'; fields?:Record<string,string>; calibration?:{row:number;label:string;fields:Record<string,string>}[] };
}
interface Entry {
  entry_id: string; template_id: string; version: number; checks: string[]; template: Template;
  location: string; inspector: string; checked_at: string; notes: string; reviewer: string; reviewed_at: string;
  template_values?:Record<string,string>;
}
interface Order {
  workspace_id?: string; asset_id?: string;
  id: string; month: string; scope: string; revision: number; status: 'pending' | 'review_pending' | 'submitted' | 'cancelled'; owner_name: string;
  submitted_at: string | null; can_claim: boolean; can_edit: boolean; can_release: boolean;
  kind: 'self' | 'external'; due: string; tool_name: string; model: string; location: string; quantity: number;
  supervisor_assigned: boolean; assigned_to_me: boolean; can_approve: boolean; can_repair: boolean; can_review: boolean; can_delete:boolean;
  can_remove_followup?: boolean; deletion_pending?: boolean; deletion_error?: string;
  actual: string; outcome: string; reason: string; resolution: string; report_name: string;
  result_label?: string; tool_status_label?: string; from_repair?: boolean;
  scrap_approvals?: { name: string; decision: string; at: string; note: string }[];
  can_restore?: boolean; inspection?: InspectionReport; inspection_expires_on?: string;
  sync: { label: string; state: string; error: string; verifying?: boolean }[]; result_notice?: string;
}
interface Detail extends Order {
  records: Entry[]; actor_name: string; history: { action: string; at: string; name: string; detail: string }[];
  draft: { outcome?: string; reason?: string; quantity?: number };
}
interface Board {
  month: string; followups: Order[]; templates: Template[]; can_manage_templates: boolean;
  inventory_error: string;
}
const outcomeOptions = [{ value: 'normal', label: '正常' }, { value: 'repair', label: '转检修' }, { value: 'scrap', label: '报废' }];
const outcomeLabels: Record<string, string> = { normal: '正常', repair: '检修中', repaired: '检修完成', scrap: '待报废审批',
  scrapped: '已批准报废', rejected: '报废未通过' };
const view = ref('workspace');
const historyAsset = ref('');
const workspace = ref<InstanceType<typeof ToolWorkspace>>();
const linkedSession = ref(/^[a-f0-9]{32}$/.test(props.toolSession || '') && /^rec[A-Za-z0-9]{1,119}$/.test(props.toolAsset || '')
  ? props.toolSession! : '');
const linkedAsset = ref(linkedSession.value ? props.toolAsset! : '');
const stateFilter = ref('all');
const reportCheck = ref(emptyInspectionReport());
const reportAssets = computed(() => order.value?.asset_id ? [order.value.asset_id] : []);
const search = ref('');
const page = ref(1);
const pageSize = 20;
const root = ref<HTMLElement>();
const actual = ref('');
const outcome = ref('');
const reason = ref('');
const reviewReason = ref('');
const quantity = ref(1);
const repairPhotos = ref<string[]>([]);
const addingTool=ref(false),toolFields=ref<{name:string;type:number;options:string[]}[]>([]),newTool=ref<Record<string,any>>({}),toolOperation=ref('');
async function openNewTool(){await mutate(async()=>{const data=await requestJson(api+'/inventory/fields') as {fields:typeof toolFields.value;scopes:string[]};toolFields.value=data.fields.map(f=>f.name==='楼栋'?{...f,options:f.options.filter(o=>data.scopes.some(s=>o===s+'楼'||o==='南通'+s+'楼'))}:f);newTool.value={'一级分类':'B工具类'};toolOperation.value=crypto.randomUUID().replace(/-/g,'');addingTool.value=true;});}
async function createTool(){await mutate(async()=>{await requestJson(api+'/inventory/new',{method:'POST',body:JSON.stringify({operation_id:toolOperation.value,fields:newTool.value})});addingTool.value=false;message.value='工具已添加，库存将在下次同步后显示。';});}
const reportInput = ref<HTMLInputElement>();
const month = ref('');
const board = ref<Board>();
const library = ref<{ templates: Template[]; legacy_templates: Template[]; warning?: string }>();
const order = ref<Detail>();
const records = ref<Entry[]>([]);
const selected = ref('');
const templateId = ref<string | null>(null);
const loading = ref(false);
const busy = ref(false);
const error = ref('');
const message = ref('');
const saved = ref('[]');
const fileInput = ref<HTMLInputElement>();
const folderInput = ref<HTMLInputElement>();
const folderProgress = ref('');
const folderFailures = ref<{file:string;reason:string}[]>([]);
const replacing = shallowRef<Template>();
const rebindTemplate = shallowRef<Template>();
const rebindTarget = ref<string | null>(null);
const rebindProgress = ref('');
const rebindFailures = ref<{name:string; scope:string; error:string}[]>([]);
const rebindOptions = computed(() => (library.value?.templates || []).filter(template =>
  template.active && template.id !== rebindTemplate.value?.id).map(template => ({value:template.id, label:template.name})));
const afterDiscard = shallowRef<(() => void) | null>(null);
let reader: AbortController | undefined;
let disposed = false;
let listPosition = 0;

const active = computed(() => records.value.find(record => record.entry_id === selected.value));
const scopedFollowups = computed(() => (board.value?.followups || []).filter(row =>
  !props.scope || ['ALL', 'CAMPUS'].includes(props.scope) || row.scope === props.scope));
const repairCount = computed(() => scopedFollowups.value.filter(row => row.outcome === 'repair').length);
const approvalCount = computed(() => scopedFollowups.value.filter(row => row.outcome === 'scrap').length);
const completed = (row: Order) => ['repaired', 'scrapped', 'rejected'].includes(row.outcome);
const completedCount = computed(() => scopedFollowups.value.filter(completed).length);
const followupOrderIds = computed(() => scopedFollowups.value.map(row => row.id));
const voteLabel = (decision: string) => ({ approve: '已同意', reject: '已驳回' }[decision] || '待审批');
const tabs = computed(() => [
  { value: 'workspace', label: '工作台', icon: ClipboardList, count: 0 },
  { value: 'followups', label: '办理记录', icon: Wrench, count: repairCount.value + approvalCount.value },
  ...(board.value?.can_manage_templates ? [{ value: 'settings', label: '工具与模板', icon: Settings2, count: 0 }] : []),
]);
const stateOptions = [{ value: 'pending', label: '待处理' }, { value: 'repair', label: '检修中' },
  { value: 'scrap', label: '待报废审批' }, { value: 'done', label: '已处置' }, { value: 'all', label: '全部状态' }];
const visibleOrders = computed(() => {
  const terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return scopedFollowups.value.filter(row =>
    (stateFilter.value === 'all' || row.outcome === stateFilter.value
      || (stateFilter.value === 'done' ? completed(row) : stateFilter.value === 'pending' && !completed(row))) &&
    terms.every(term => [row.tool_name, row.model, row.location, row.scope + '楼', row.owner_name]
      .join(' ').toLowerCase().includes(term)))
    .sort((left, right) => Number(completed(left)) - Number(completed(right)));
});
const pagedOrders = computed(() => visibleOrders.value.slice((page.value - 1) * pageSize, page.value * pageSize));
const emptyDescription = computed(() => search.value.trim() || stateFilter.value !== 'all'
  ? '没有符合筛选条件的记录' : '暂无检修或报废记录');
watch(view, () => {
  stateFilter.value = 'all';
  if (view.value === 'settings') void loadLibrary().catch(fail);
});
watch([view, search, stateFilter], () => { page.value = 1; });
watch(() => visibleOrders.value.length, count => { page.value = Math.min(page.value, Math.max(1, Math.ceil(count / pageSize))); });
const transitions = computed<{ action: string; label: string; type: 'primary' | 'warning' }[]>(() => order.value?.can_repair
  ? [{ action: 'repaired', label: '检修后合格', type: 'primary' }, { action: 'repair-scrap', label: '申请检修后报废', type: 'warning' }]
  : [{ action: 'approve', label: '同意报废', type: 'primary' }, { action: 'reject', label: '驳回报废', type: 'warning' }]);
const availableTemplates = computed(() => library.value?.templates ?? board.value?.templates ?? []);
const templateOptions = computed(() => availableTemplates.value.filter(template => template.active)
  .map(template => ({ value: template.id, label: template.name })));
const templateClone=ref<{id:string;version:number;name:string;hvac:boolean;checks:{name:string;method:string;standard:string;sharedName:boolean}[]}|null>(null);
function startTemplateClone(template:Template){
  templateClone.value={id:template.id,version:template.version,name:template.name+'（新模板）',hvac:template.schema.layout==='hvac',
    checks:template.schema.checks.map(item=>({name:item.name,method:item.method,standard:item.standard,sharedName:!!item.group_row&&item.group_row!==item.row}))};
}
async function saveTemplateClone(){const value=templateClone.value;if(!value)return;await mutate(async()=>{await requestJson(`${api}/templates/${value.id}/clone`,{method:'POST',body:JSON.stringify(value)});templateClone.value=null;await loadLibrary();message.value='新模板已保存。';});}
const payload = computed(() => records.value.map(({ template: _template, ...record }) => record));
const form = computed(() => order.value?.kind === 'external' ? { actual: actual.value, inspection: reportCheck.value } :
  { records: payload.value, outcome: outcome.value, reason: reason.value, quantity: quantity.value });
const dirty = computed(() => !!order.value?.can_edit && JSON.stringify(form.value) !== saved.value);
const checkCount = computed(() => records.value.reduce((sum, record) => sum + record.checks.length, 0));
const answered = computed(() => records.value.reduce((sum, record) => sum + record.checks.filter(Boolean).length, 0));
const orderPath = (row: Order) => `/orders/${row.id}`;
const status = (row: Order) => row.status === 'submitted' ? '已提交' : row.status === 'cancelled' ? '已取消' : row.status === 'review_pending' ? '待楼长审核' : '待填写';
const kindLabel = (row: Order) => row.kind === 'self' ? '自检' : '送检';
const displayTime = (value: string) => value.slice(0, 19).replace('T', ' ');
function assignment(row: Order) {
  if (row.owner_name) return `领取人 ${row.owner_name}`;
  if (row.kind === 'self') return '本楼楼长领取';
  if (row.assigned_to_me) return '已分配给我';
  return row.supervisor_assigned ? '专业主管或本楼楼长领取' : '待配置专业主管 · 本楼楼长可领取';
}
function orderAction(row: Order) {
  if (row.can_claim) return '领取并填写';
  if (row.can_edit) return '继续填写';
  if (row.can_review) return '审核自检';
  if (row.can_approve) return '处理审批';
  if (row.can_repair) return '跟进检修';
  return '查看记录';
}
function scrollHost() {
  const main = root.value?.closest('.v2-main');
  return main && ['auto', 'scroll'].includes(getComputedStyle(main).overflowY) ? main : window;
}
async function scrollAfterRender(top: number) {
  await nextTick();
  if (!disposed) scrollHost().scrollTo({ top, behavior: 'instant' });
}
async function changePage(next: number) {
  page.value = next;
  await nextTick();
  root.value?.querySelector('.tool-worklist')?.scrollIntoView({ block: 'start' });
}

function fail(value: unknown) { error.value = value instanceof Error ? value.message : '操作暂未确认，请刷新核对。'; }
async function loadLibrary() {
  const data = await requestJson(api + '/templates', { cache: 'no-store' }) as NonNullable<typeof library.value>;
  if (!disposed) library.value = data;
}
async function loadBoard(target = month.value) {
  reader?.abort();
  const request = reader = new AbortController();
  loading.value = true;
  error.value = '';
  try {
    const data = await requestJson(api + (target ? `?month=${encodeURIComponent(target)}` : ''), { signal: request.signal, cache: 'no-store' }) as Board;
    if (!request.signal.aborted && !disposed) {
      board.value = data;
      month.value = data.month;
    }
  } catch (value) { if (!request.signal.aborted) fail(value); }
  finally { if (reader === request) loading.value = false; }
}
function applyOrder(data: Detail) {
  order.value = data;
  records.value = data.records.map(record => ({ ...record, checks: [...record.checks],
    ...(record.template.schema.layout==='hvac' ? {template_values:{...record.template_values}} : {}) }));
  actual.value = data.actual;
  reportCheck.value = emptyInspectionReport(data.inspection);
  outcome.value = data.draft?.outcome || '';
  reason.value = data.draft?.reason || '';
  quantity.value = data.draft?.quantity || 1;
  saved.value = JSON.stringify(form.value);
  if (!records.value.some(record => record.entry_id === selected.value)) selected.value = records.value[0]?.entry_id || '';
}
async function action(row: Order, kind: string, data: object = {}) {
  return requestJson(api + orderPath(row) + '/' + kind, { method: 'POST', body: JSON.stringify({ revision: row.revision, ...data }) });
}
async function removeFollowup(row: Order) {
  await mutate(async () => {
    const result = await action(row, 'remove-followup') as { deleted?: boolean };
    await loadBoard();
    await workspace.value?.reloadRecords();
    message.value = result.deleted ? '办理记录已删除。' : '已提交删除，飞书核验通过后自动移除。';
  });
}
async function openOrder(row: Order, claim = false) {
  if (busy.value) return;
  if (order.value?.id !== row.id) repairPhotos.value = [];
  if (row.workspace_id && (row.status === 'pending' || row.status === 'review_pending' || view.value !== 'followups')) {
    linkedSession.value = row.workspace_id;
    linkedAsset.value = row.asset_id || '';
    view.value = 'workspace';
    return;
  }
  const opening = !order.value;
  if (opening) {
    const host = scrollHost();
    listPosition = host instanceof Window ? host.scrollY : host.scrollTop;
  }
  busy.value = true;
  error.value = message.value = '';
  try {
    if (claim) await action(row, 'claim');
    const data = await requestJson(api + orderPath(row), { cache: 'no-store' }) as Detail;
    if (!disposed) {
      if (data.workspace_id && (data.status === 'pending' || data.status === 'review_pending' || view.value !== 'followups')) {
        order.value = undefined;
        linkedSession.value = data.workspace_id;
        linkedAsset.value = data.asset_id || '';
        view.value = 'workspace';
        return;
      }
      applyOrder(data);
      if (data.kind === 'self' && data.can_edit) void loadLibrary().catch(fail);
      if (opening) void scrollAfterRender(0);
    }
  } catch (value) { fail(value); }
  finally { busy.value = false; }
}
function openWorkspaceOrder(id: string, orderMonth: string) {
  if (!id || busy.value) return;
  historyAsset.value = '';
  const proceed = () => {
    month.value = orderMonth;
    void openOrder({id} as Order);
  };
  if (workspace.value) workspace.value.leave(proceed);
  else leave(proceed);
}
function openHistoryWorkspace(id: string) {
  const asset = historyAsset.value;
  historyAsset.value = '';
  const proceed = () => {
    order.value = undefined;
    linkedSession.value = id;
    linkedAsset.value = asset;
    view.value = 'workspace';
  };
  if (workspace.value) workspace.value.leave(proceed);
  else leave(proceed);
}
function closeOrder() {
  order.value = undefined;
  records.value = [];
  selected.value = '';
  templateId.value = null;
  message.value = '';
  void loadBoard();
  void scrollAfterRender(listPosition);
}
function back() {
  const legacy = !document.querySelector('.v2-shell');
  navigateBack(legacy ? '/?entry=special' : '/?view=functions');
}
function refresh() {
  if (order.value) { const row = order.value; leave(() => void openOrder(row)); }
  else if (workspace.value) void workspace.value.refresh();
  else if (view.value === 'settings') void mutate(loadLibrary);
  else void loadBoard();
}
function updateInventoryStatus(warning: string) {
  if (board.value) board.value = { ...board.value, inventory_error: warning };
}
function selectView(next: string) {
  if (next === view.value) return;
  const proceed = () => {
    linkedSession.value = linkedAsset.value = '';
    view.value = next;
    if (next === 'followups') void loadBoard();
  };
  if (workspace.value) workspace.value.leave(proceed);
  else proceed();
}
function addRecord() {
  const template = availableTemplates.value.find(item => item.id === templateId.value && item.active);
  if (!template || !order.value?.can_edit || records.value.length >= 20) return;
  const id = Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('');
  records.value.push({ entry_id: id, template_id: template.id, version: template.version, template,
    checks: template.schema.checks.map(() => ''), location: order.value.location, inspector: order.value.actor_name,
    checked_at: '', notes: '', reviewer: '', reviewed_at: '',
    ...(template.schema.layout==='hvac' ? {template_values:{C3:order.value.tool_name,C4:order.value.model}} : {}) });
  selected.value = id;
  templateId.value = null;
}
function removeRecord(id: string) {
  records.value = records.value.filter(record => record.entry_id !== id);
  selected.value = records.value[0]?.entry_id || '';
}
async function save(kind: 'draft' | 'submit') {
  if (!order.value?.can_edit || busy.value) return;
  const row = order.value;
  busy.value = true;
  error.value = message.value = '';
  try {
    const receipt = await action(row, kind, form.value) as Order;
    if (!disposed) {
      order.value = { ...row, ...receipt };
      saved.value = JSON.stringify(form.value);
      message.value = kind === 'submit' ? '工单已提交。' : '草稿已保存。';
      if (kind === 'submit') {
        try { applyOrder(await requestJson(api + orderPath(row), { cache: 'no-store' }) as Detail); }
        catch { /* The submit receipt already confirmed the persisted result. */ }
      }
    }
  } catch (value) { fail(value); }
  finally { busy.value = false; }
}
async function uploadReport(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  input.value = '';
  if (!file || !order.value?.can_edit) return;
  const row = order.value;
  const params = new URLSearchParams({ name: file.name, revision: String(row.revision) });
  await mutate(async () => {
    const receipt = await requestBinaryJson(api + orderPath(row) + '/report?' + params, file) as Order;
    if (!disposed) {
      order.value = { ...row, ...receipt };
      reportCheck.value.confirmed_asset_ids = [];
    }
    message.value = '送检报告已保存。';
  });
}
async function advance(step: string) {
  if (!order.value) return;
  const row = order.value;
  await mutate(async () => {
    const receipt = await action(row, step, { reason: step.startsWith('review-') ? reviewReason.value : reason.value,
      quantity: quantity.value, ...(step === 'repair-scrap' ? { scrap_photos: repairPhotos.value } : {}) }) as Order;
    if (step === 'repair-scrap') repairPhotos.value = [];
    if (!disposed) order.value = { ...row, ...receipt };
    message.value = '处理结果已保存。';
    try { applyOrder(await requestJson(api + orderPath(row), { cache: 'no-store' }) as Detail); }
    catch { /* Preserve the confirmed transition. */ }
  });
}
async function selectScrapPhotos(event: Event) {
  const input = event.target as HTMLInputElement;
  const files = [...(input.files || [])];
  input.value = '';
  await mutate(async () => {
    if (files.length > 3 || files.some(file => file.size > 1024 * 1024 || !['image/jpeg', 'image/png'].includes(file.type))) {
      throw new Error('请选择最多三张 1 MB 以内的 JPG 或 PNG 照片。');
    }
    repairPhotos.value = await Promise.all(files.map(file => new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => reject(new Error('读取现场照片失败。'));
      reader.readAsDataURL(file);
    })));
  });
}
async function release(row: Order) {
  await mutate(async () => { await action(row, 'release'); await loadBoard(); });
}
async function mutate(operation: () => Promise<void>) {
  if (busy.value) return;
  busy.value = true;
  error.value = message.value = '';
  try { await operation(); }
  catch (value) { fail(value); }
  finally { busy.value = false; }
}
function download(path: string) { return mutate(() => downloadFile(api + path)); }
function chooseImport(template?: Template) {
  replacing.value = template;
  fileInput.value?.click();
}
async function importTemplate(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  input.value = '';
  if (!file) return;
  const previous = replacing.value;
  const params = new URLSearchParams({ name: previous?.name || file.name.replace(/\.xlsx$/i, '') });
  if (previous) { params.set('id', previous.id); params.set('version', String(previous.version)); }
  await mutate(async () => {
    await requestBinaryJson(api + '/templates?' + params, file);
    await loadLibrary();
    message.value = '模板已保存。';
  });
}
async function importTemplateFolder(event:Event){
  const input=event.target as HTMLInputElement;
  const files=Array.from(input.files || []).filter(file=>/\.xlsx$/i.test(file.name)&&!file.name.startsWith('~$'));
  input.value='';
  if(!files.length){error.value='所选文件夹没有可导入的 XLSX 模板。';return;}
  await mutate(async()=>{
    folderFailures.value=[];
    let imported=0,skipped=0,processed=0;
    for(const file of files){
      if(disposed)break;
      folderProgress.value=`正在导入 ${processed+1}/${files.length} · ${file.name}`;
      try{
        const params=new URLSearchParams({name:file.name.replace(/\.xlsx$/i,'')});
        const receipt = await requestBinaryJson(api+'/templates?'+params,file) as { existing?: boolean };
        if (receipt.existing) skipped++;
        else imported++;
      }catch(failure){
        if(failure instanceof ApiError && failure.status===409 && failure.message.includes('相同模板已导入'))skipped++;
        else folderFailures.value.push({file:file.webkitRelativePath || file.name,reason:failure instanceof Error?failure.message:'上传未确认'});
      }
      processed++;
    }
    if(disposed)return;
    await loadLibrary();
    folderProgress.value=`已导入 ${imported} 份，跳过重复 ${skipped} 份，失败 ${folderFailures.value.length} 份。`;
  });
}
async function toggleTemplate(template: Template) {
  await mutate(async () => {
    await requestJson(`${api}/templates/${template.id}/availability`, { method: 'POST',
      body: JSON.stringify({ version: template.version, active: !template.active }) });
    await loadLibrary();
  });
}
function startRebind(template: Template) {
  rebindTemplate.value = template;
  rebindTarget.value = null;
  rebindProgress.value = '';
  rebindFailures.value = [];
}
function closeRebind(show: boolean) {
  if (!show && !busy.value) rebindTemplate.value = undefined;
}
async function applyRebind() {
  const template = rebindTemplate.value;
  const target = rebindTarget.value;
  if (!template || !target) return;
  await mutate(async () => {
    let total = 0;
    rebindFailures.value = [];
    for (;;) {
      const result = await requestJson(`${api}/templates/${encodeURIComponent(template.id)}/rebind`, {
        method: 'POST', timeoutMs: 120000, body: JSON.stringify({ version: template.version, target_id: target }),
      }) as { updated: number; remaining: number; failed: typeof rebindFailures.value };
      total += result.updated;
      rebindFailures.value = result.failed;
      rebindProgress.value = `已核验 ${total} 台${result.failed.length ? `，本批 ${result.failed.length} 台待处理` : ''}${result.remaining ? `，另有 ${result.remaining} 台未处理` : ''}。`;
      if (!result.remaining || result.failed.length || !result.updated || disposed) break;
    }
    if (!disposed) await loadLibrary();
  });
}
async function deleteTemplate(template: Template) {
  await mutate(async () => {
    await requestJson(`${api}/templates/${template.id}/delete`, {
      method: 'POST', body: JSON.stringify({ version: template.version }),
    });
    if (board.value) board.value.templates = board.value.templates.filter(item => item.id !== template.id);
    if (templateClone.value?.id === template.id) templateClone.value = null;
    if (templateId.value === template.id) templateId.value = null;
    await loadLibrary();
    message.value = '模板已删除。';
  });
}
function leave(proceed: () => void) {
  if (busy.value) return;
  if (dirty.value) afterDiscard.value = proceed;
  else proceed();
}
function discard() {
  const proceed = afterDiscard.value;
  afterDiscard.value = null;
  saved.value = JSON.stringify(form.value);
  proceed?.();
}
const unregister = registerNavigationGuard((_target, proceed) => {
  if (workspace.value?.busy) return false;
  if (workspace.value?.dirty) {
    workspace.value.leave(proceed);
    return false;
  }
  if (busy.value) return false;
  if (!dirty.value) return true;
  afterDiscard.value = proceed;
  return false;
});
function beforeUnload(event: BeforeUnloadEvent) { if (dirty.value || busy.value || workspace.value?.dirty || workspace.value?.busy) { event.preventDefault(); event.returnValue = ''; } }
onMounted(() => {
  window.addEventListener('beforeunload', beforeUnload);
  void loadBoard();
  if (props.toolView === 'followups' || /^[a-f0-9]{32}$/.test(props.toolOrder || '')) view.value = 'followups';
  if (/^[a-f0-9]{32}$/.test(props.toolOrder || '')) void openOrder({ id: props.toolOrder! } as Order);
});
onBeforeUnmount(() => { disposed = true; reader?.abort(); unregister(); window.removeEventListener('beforeunload', beforeUnload); });
</script>

<style scoped>
.tool-check {
  --tool-ink:var(--v2-ink,var(--cf-text,#243244));
  --tool-muted:var(--v2-muted,var(--cf-muted,#52647a));
  --tool-line:var(--v2-border,var(--cf-border,#dce2e9));
  --tool-surface:var(--cf-surface,#fff);
  --tool-soft:color-mix(in srgb,var(--tool-surface),var(--tool-ink) 4%);
  --tool-primary:var(--v2-blue,var(--cf-brand-blue,#2563eb));
  --tool-tint:color-mix(in srgb,var(--tool-surface),var(--tool-primary) 7%);
  display:grid; grid-template-columns:minmax(0,1fr); gap:12px; container-type:inline-size; box-sizing:border-box;
  width:100%; min-width:0; max-width:1520px; padding:14px 16px; margin:auto; color:var(--tool-ink);
}
.tool-header,.tool-overview,.tool-worklist,.tool-library,.tool-edit-panel,.tool-result {
  min-width:0; border:1px solid var(--tool-line); border-radius:10px; background:var(--tool-surface);
}
.tool-header {
  display:flex; justify-content:space-between; align-items:center; gap:10px; padding:10px 14px 10px 8px;
  background:linear-gradient(105deg,var(--tool-tint),var(--tool-surface));
}
.tool-title { display:flex; min-width:0; align-items:center; gap:8px; }
.tool-title p,.tool-title h2 { margin:0; overflow-wrap:anywhere; }
.tool-title p { font-size:12px; color:var(--tool-muted); }
.tool-title h2 { font-size:20px; line-height:1.4; }
.tool-title .n-button { width:40px; padding:0; flex:none; }
.tool-refresh { flex:none; }
.tool-bar,.tool-list-head,.tool-panel-heading {
  display:flex; align-items:center; flex-wrap:wrap; gap:8px 12px; min-width:0;
}
.tool-nav { display:flex; gap:4px; margin-bottom:12px; border-bottom:1px solid var(--tool-line); }
.tool-nav>.n-button { min-width:0; border-radius:8px 8px 0 0; }
.tool-nav :deep(.n-button__content) { gap:4px; }
.tool-nav-count { font-size:12px; opacity:.88; }
.tool-nav-count::before { content:'·'; margin-right:4px; }
.tool-source-error { margin-bottom:12px; }
.tool-worklist { overflow:hidden; }
.tool-list-head,.tool-filters,.tool-list-foot { padding:10px 14px; }
.tool-list-head,.tool-list-foot { display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }
.tool-list-foot { color:var(--tool-muted); font-size:13px; border-top:1px solid var(--tool-line); }
.tool-counts { display:flex; gap:8px; flex-wrap:wrap; }
.tool-counts span { display:inline-flex; gap:6px; align-items:baseline; padding:4px 8px; border-radius:6px; background:var(--tool-soft); font-size:13px; }
.tool-counts strong { font-size:17px; font-variant-numeric:tabular-nums; }
.tool-counts span:first-child { background:var(--tool-tint); }
.tool-filters {
  display:grid; grid-template-columns:minmax(200px,1fr) minmax(0,155px); gap:8px; align-items:stretch;
  border-top:1px solid var(--tool-line);
}
input,.tool-fields textarea,.tool-result textarea {
  box-sizing:border-box; min-width:0; max-width:100%; min-height:34px; padding:7px 9px;
  border:1px solid var(--tool-line); border-radius:6px; background:var(--tool-surface); color:inherit; font:inherit;
}
.tool-column-labels,.tool-order {
  display:grid; grid-template-columns:minmax(130px,2fr) minmax(72px,.8fr) minmax(135px,1.25fr) minmax(108px,.85fr) minmax(130px,1fr);
  align-items:center; gap:8px 12px; padding:10px 14px;
}
.tool-column-labels { background:var(--tool-soft); font-weight:650; font-size:12px; color:var(--tool-muted); }
.tool-order { border-top:1px solid var(--tool-line); font-size:13px; }
.tool-order>div { min-width:0; overflow-wrap:anywhere; }
.tool-order small,.tool-template small,.tool-report-file small { display:block; margin-top:2px; font-size:12px; color:var(--tool-muted); }
.tool-order-name { display:flex; align-items:center; gap:8px; }
.tool-order-name>div { min-width:0; }
.tool-order-name strong { font-size:14px; }
.tool-order-actions { display:flex; gap:5px; flex-wrap:wrap; }
.tool-order-state small.tool-pending,.tool-pending { color:var(--v2-warning,#855509); }
.tool-muted { color:var(--tool-muted); }
.tool-status-closed {
  --n-color:color-mix(in srgb,var(--tool-surface),var(--tool-muted) 13%) !important;
  --n-text-color:var(--tool-muted) !important;
}
.tool-empty { padding:28px 12px; }
.tool-settings-layout,.tool-form-layout { display:grid; gap:12px; align-items:start; min-width:0; }
.tool-settings-layout { grid-template-columns:minmax(0,1.25fr) minmax(0,1fr); }
.tool-library,.tool-edit-panel,.tool-result,.tool-overview { padding:12px 14px; }
.tool-panel-heading { justify-content:space-between; }
.tool-panel-heading h3 { margin:0; font-size:15px; }
.tool-panel-heading span,.tool-library p { font-size:12px; color:var(--tool-muted); }
.tool-library p { margin:6px 0; }
.tool-template { display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px 12px; padding:8px 0; border-top:1px solid var(--tool-line); }
.tool-template>div:first-child { min-width:0; overflow-wrap:anywhere; font-size:13px; }
.tool-template .tool-bar { gap:5px; }
.tool-form { display:grid; gap:12px; min-width:0; }
.tool-overview { display:grid; gap:8px; font-size:13px; }
.tool-download { margin-inline-start:auto; }
.tool-overview-fields { margin:0; display:flex; gap:8px 22px; flex-wrap:wrap; }
.tool-overview-fields div { display:flex; flex-wrap:wrap; gap:4px 7px; min-width:0; }
.tool-overview-fields dt { color:var(--tool-muted); }
.tool-overview-fields dd { margin:0; overflow-wrap:anywhere; }
.tool-form-layout.tool-has-aside { grid-template-columns:minmax(0,1fr) minmax(198px,236px); }
.tool-edit-panel,.tool-result { display:grid; gap:10px; }
.tool-report-layout { display:grid; grid-template-columns:minmax(0,180px) minmax(0,1fr); align-items:center; gap:10px; }
.tool-report-layout>div:first-child { display:grid; gap:4px; font-size:13px; }
.tool-report-file {
  display:grid; grid-template-columns:auto minmax(0,1fr) auto; align-items:center; gap:8px; min-width:0; padding:10px;
  border:1px solid var(--tool-line); border-radius:8px; background:var(--tool-tint); overflow-wrap:anywhere;
}
.tool-report-file>svg { color:var(--tool-primary); }
.tool-result-fields { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; }
.tool-result-fields label { grid-column:1/-1; }
.tool-report-layout label,.tool-result label,.tool-fields label { display:grid; gap:4px; min-width:0; font-size:13px; }
.tool-template-picker>.n-select { flex:1 1 190px; max-width:440px; }
.tool-records { gap:6px; }
.tool-records button {
  max-width:100%; padding:6px 9px; color:inherit; background:var(--tool-soft);
  border:1px solid var(--tool-line); border-radius:6px; cursor:pointer; text-align:left; overflow-wrap:anywhere;
}
.tool-records button[aria-pressed=true] { color:var(--tool-primary); border-color:var(--tool-primary); background:var(--tool-tint); font-weight:700; }
.tool-record-heading { gap:4px 9px; }
.tool-record-heading strong { overflow-wrap:anywhere; }
.tool-record-heading span { color:var(--tool-muted); font-size:12px; }
.tool-context { margin:0; font-size:13px; overflow-wrap:anywhere; }
.tool-context p { margin:6px 0 0; white-space:pre-wrap; }
.tool-fields { min-width:0; border:0; padding:0; margin:0; display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,160px),1fr)); gap:8px 12px; }
.tool-notes { grid-column:1/-1; }
.tool-grid { min-width:0; border:1px solid var(--tool-line); border-radius:7px; }
.tool-grid-head,.tool-grid-row { display:grid; grid-template-columns:minmax(90px,.9fr) minmax(0,1.25fr) minmax(0,1.25fr) minmax(84px,auto); gap:8px; padding:8px 9px; align-items:start; }
.tool-grid-head { background:var(--tool-soft); font-size:12px; }
.tool-grid-row { border-top:1px solid var(--tool-line); }
.tool-grid-row p,.tool-grid-row strong { min-width:0; margin:0; overflow-wrap:anywhere; white-space:pre-line; }
.tool-grid-row p>span { display:none; }
.tool-audit { border:1px solid var(--tool-line); border-radius:8px; padding:8px 10px; background:var(--tool-surface); font-size:13px; }
.tool-audit summary { cursor:pointer; }
.tool-audit .tool-fields { margin-top:8px; }
.tool-audit p { overflow-wrap:anywhere; }
.tool-submit {
  position:sticky; bottom:0; z-index:2; background:var(--tool-surface); border:1px solid var(--tool-line);
  border-radius:8px; padding:8px 10px; justify-content:flex-end; backdrop-filter:blur(8px);
}
.tool-submit>span { margin-right:auto; color:var(--tool-muted); font-size:13px; }
:global(.workspace-v2 .tool-check .tool-submit) { padding-inline-end:64px; }
@container (max-width:840px) {
  .tool-form-layout.tool-has-aside,.tool-settings-layout { grid-template-columns:minmax(0,1fr); }
}
@container (max-width:680px) {
  .tool-nav>.n-button { flex:1 1 0; padding-inline:4px; }
  .tool-nav-count { display:none; }
  .tool-list-head,.tool-filters,.tool-list-foot { padding:8px 10px; }
  .tool-filters { grid-template-columns:minmax(0,1fr) minmax(0,124px); }
  .tool-column-labels { position:absolute; width:1px; height:1px; padding:0; overflow:hidden; clip-path:inset(50%); }
  .tool-orders { display:grid; gap:7px; padding:0 8px 8px; }
  .tool-order {
    grid-template-columns:minmax(0,1fr) auto; align-items:start; gap:5px 8px; padding:9px;
    border:1px solid var(--tool-line); border-radius:8px;
  }
  .tool-order-name { grid-area:1/1; }
  .tool-order-state { grid-area:1/2; justify-self:end; text-align:end; }
  .tool-order-kind { grid-area:2/1/3/-1; display:flex; flex-wrap:wrap; gap:5px; align-items:baseline; }
  .tool-order-meta { grid-area:3/1; }
  .tool-order-actions { grid-area:3/2; justify-content:flex-end; align-self:end; }
  .tool-order-actions:has(> :nth-child(2)) { grid-area:4/1/auto/-1; }
  .tool-library,.tool-edit-panel,.tool-result,.tool-overview { padding:9px 10px; }
  .tool-fields { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .tool-time { grid-column:1/-1; }
  .tool-report-layout { grid-template-columns:minmax(0,1fr); }
  .tool-report-file { grid-template-columns:auto minmax(0,1fr); }
  .tool-report-actions { grid-column:1/-1; justify-self:start; }
  .tool-grid-head { display:none; }
  .tool-grid-row { grid-template-columns:minmax(0,1fr) auto; gap:6px 8px; }
  .tool-grid-row:nth-child(2) { border-top:0; }
  .tool-grid-row p { grid-column:1/-1; font-size:13px; }
  .tool-grid-row p>span { display:inline; font-weight:650; margin-right:8px; }
  .tool-grid-row :deep(.n-radio-group),.tool-grid-row>span { grid-column:2; grid-row:1; }
  .tool-grid-row :deep(.n-radio-group) { display:inline-flex; --n-height:44px !important; white-space:nowrap; }
  .tool-order-actions .n-button,.tool-report-file .n-button,.tool-submit .n-button { min-height:44px; }
}
@media(max-width:760px),(hover:none) and (max-width:1024px) {
  .tool-check { padding:6px 4px; gap:8px; }
  .tool-title h2 { font-size:17px; }
  .tool-header { padding:6px 8px 6px 4px; }
  :global(.v2-shell.is-mobile-workspace .tool-check .tool-submit) { bottom:calc(62px + env(safe-area-inset-bottom,0px)); }
}
@container (max-width:360px) {
  .tool-filters { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .tool-nav>.n-button :deep(svg) { display:none; }
  .tool-submit>span { flex:1 1 100%; }
}
</style>
