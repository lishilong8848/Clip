<template>
  <main class="cabinet-page">
    <header class="heading">
      <VnetBackButton :to="scope ? '/cabinet-power' : '/?entry=capacity'" />
      <div class="heading-title"><h1>{{ scope ? scope + '楼机柜上下电' : '机柜上下电' }}</h1><p>机柜台账 <span v-if="overview.updated_at">· 更新于 {{ overview.updated_at }}</span></p></div>
      <div class="actions">
        <button :disabled="loading || busy || bootstrapActive || allExportBusy" @click="refresh"><RefreshCw :size="16" :class="{ spin: loading || busy || bootstrapActive }" />刷新</button>
        <button @click="navigate(batchCreateUrl)"><Files :size="16" />批量登记</button>
        <button @click="openTodoBatches"><ClipboardList :size="16" />上下电待办<span v-if="batchPendingCount" class="count-badge">{{ batchPendingCount }}</span></button>
        <button v-if="!scope" class="primary" :disabled="!allExportReady || allExportBusy || allExportItems.some(item => ['pending','running','checking','unknown'].includes(item.status))" :title="allExportItems.some(item => ['checking','unknown'].includes(item.status)) ? '请先核对未确认的导出任务' : ''" @click="startAllExports"><CloudUpload :size="16" />{{ allExportBusy ? '各楼正在导出' : '一键导出/上传所有楼栋' }}</button>
        <template v-if="scope">
          <a v-if="overview.table_url" :href="overview.table_url" target="_blank" rel="noopener"><ExternalLink :size="16" />多维表</a>
          <button :disabled="!overview.rooms || racksLoading || busy || saving" @click="['D','E'].includes(scope) ? changeTab('records') : openEditor()"><Plus :size="16" />{{ ['D','E'].includes(scope) ? '登记机柜操作' : '新增记录' }}</button>
          <button class="primary" :disabled="!overview.rooms || busy" @click="startJob('exports')"><FileSpreadsheet :size="16" />{{ pendingExportRequest ? '继续上次导出' : '导出' }}</button>
      <button :disabled="exportHistoryLoading" @click="showExports()"><History :size="16" />{{ exportHistoryLoading ? '读取历史中' : '导出历史' }}</button>
        </template>
      </div>
    </header>
    <div v-if="error" class="notice danger" role="alert">{{ error }}<button @click="error = ''" aria-label="关闭错误"><X :size="16" /></button></div>
    <div v-if="bootstrapVisible" class="notice bootstrap-notice" :class="{ danger: bootstrapFailed }" role="status" aria-live="polite">
      <Loader2 v-if="bootstrapActive" class="spin" :size="18" />
      <div class="bootstrap-copy">
        <strong>{{ bootstrapMessage }}</strong>
        <progress :value="bootstrap.ready || 0" :max="bootstrap.total || 5">{{ bootstrap.ready || 0 }}/{{ bootstrap.total || 5 }}</progress>
        <div class="bootstrap-buildings"><span v-for="item in bootstrap.buildings || []" :key="item.scope" :class="'status-' + item.status" :title="item.error || ''">{{ item.scope }}楼 {{ bootstrapStatusLabel(item.status) }}</span></div>
      </div>
      <button v-if="bootstrapHasFailures" :disabled="bootstrapActive" @click="startBootstrap(true)">重试初始化</button>
    </div>
    <div v-if="loading && !bootstrapActive" class="notice" role="status"><Loader2 class="spin" :size="18" />正在读取台账…</div>
    <div v-if="busy" class="notice" role="status"><Loader2 class="spin" :size="18" />{{ startingJob ? (startingKind === 'exports' ? '正在准备导出数据…' : '正在启动同步…') : job.kind === 'export' ? '正在生成原模板表格并上传多维…' : '正在同步飞书…' }}</div>
    <div v-if="job.status === 'failed'" class="notice danger">{{ job.error }}<button @click="startJob(job.kind === 'export' ? 'exports' : 'refresh')">重试</button></div>
    <div v-if="exported.export_id" class="notice" :class="exported.cloud_upload_status === 'failed' ? 'danger' : 'success'"><FileCheck2 :size="18" />{{ exported.filename }}<span>{{ cloudUploadLabel(exported) }}</span><a :href="api + '/exports/' + exported.export_id + '/download'"><Download :size="16" />下载</a><a v-if="exported.archive_url && exported.cloud_upload_status === 'succeeded'" :href="exported.archive_url" target="_blank" rel="noopener"><ExternalLink :size="16" />归档表</a><button v-if="!['succeeded','skipped'].includes(String(exported.cloud_upload_status || ''))" :disabled="exported._retrying" @click="retryExportUpload(exported)"><CloudUpload :size="16" />{{ exported._retrying ? '上传中' : '重试上传' }}</button></div>
    <div v-if="scope && overview.export_state?.is_stale" class="notice danger" role="status"><TriangleAlert :size="18" /><span>最近导出已过期：{{ overview.export_state.stale_reason }}。请重新导出。</span></div>
    <div v-if="storageWarning" class="notice danger" role="status">{{ storageWarning }}</div>
    <div v-if="message" class="notice success" role="status">{{ message }}</div>
    <div v-if="saving && !editorOpen" class="notice" role="status"><Loader2 class="spin" :size="16" /><span>{{ saveStepLabel }}</span><button v-if="saveStatus.operation_id" @click="showSubmission(saveStatus.operation_id)">查看提交内容</button></div>
    <div v-for="pending in pendingWrites.filter(p => !saving || p.operation_id !== saveStatus.operation_id)" :key="pending.operation_id" class="notice danger" role="alert"><span>{{ pending.status === 'conflict' ? '上传存在冲突' : '上传待完成' }}：{{ pending.error || pending.error_stage }}</span><button @click="showSubmission(pending.operation_id)">查看提交内容</button><button :disabled="saving" @click="resumePending(pending.operation_id)">继续核验</button><button :disabled="saving" @click="reconcilePending(pending.operation_id)">载入云端版本</button></div>
    <section v-if="allExportItems.length" class="all-export-panel" aria-live="polite">
      <div class="section-title"><h3>全部楼栋导出/上传</h3><span>{{ allExportCompleted }}/{{ allExportItems.length }} 栋完成</span></div>
      <div class="all-export-items"><div v-for="item in allExportItems" :key="item.scope">
        <strong>{{ item.scope }}楼</strong><span class="export-item-icon"><Loader2 v-if="['pending','running','checking'].includes(item.status)" class="spin" :size="16" /></span>
        <span>{{ allExportLabel(item) }}</span>
        <div class="export-item-actions">
          <a v-if="item.result?.export_id && !item.result?.deleted" :href="api + '/exports/' + item.result.export_id + '/download'"><Download :size="15" />下载</a>
          <a v-if="item.result?.archive_url && item.result?.cloud_upload_status === 'succeeded'" :href="item.result.archive_url" target="_blank" rel="noopener"><ExternalLink :size="15" />归档表</a>
          <button v-if="item.status === 'checking'" :disabled="item._retrying" @click="retryAllExportStatus(item)"><RefreshCw :size="15" />{{ item._retrying ? '查询中' : '继续查询' }}</button>
          <button v-if="['failed','unknown'].includes(item.status)" :disabled="item._retrying" @click="retryAllExportItem(item)"><RefreshCw :size="15" />{{ item._retrying ? '核验中' : item.status === 'unknown' ? '核验并继续' : '重试导出' }}</button>
          <button v-if="item.result?.cloud_upload_status === 'failed' && !item.result?.deleted" :disabled="item.result._retrying" @click="retryExportUpload(item.result,item)"><CloudUpload :size="15" />{{ item.result._retrying ? '上传中' : '重试上传' }}</button>
        </div>
        <small v-if="item.error || item.result?.cloud_upload_error">{{ item.error || item.result.cloud_upload_error }}</small>
      </div></div>
    </section>
    <section v-if="exportListOpen" class="table-wrap mobile-card-table"><div class="section-title"><h3>导出历史</h3><button @click="exportListOpen = false" aria-label="关闭导出历史"><X :size="16" /></button></div><p v-if="exportHistoryLoading" class="notice" role="status">正在读取导出历史…</p><p v-else-if="!exportList.length && !error" class="empty">暂无导出文件</p><table v-else-if="exportList.length"><thead><tr><th>文件</th><th>生成时间</th><th>统计状态</th><th>云端归档</th><th>下载</th><th>操作</th></tr></thead><tbody><tr v-for="item in exportList" :key="item.export_id"><td data-label="文件">{{ item.filename }}</td><td data-label="生成时间">{{ item.created_at }}</td><td data-label="统计状态"><span :class="item.is_stale ? 'danger-text' : ''">{{ item.is_stale ? '已过期' : '当前版本' }}</span><small v-if="item.is_stale">{{ item.stale_reason }}</small></td><td data-label="云端归档"><span>{{ cloudUploadLabel(item) }}</span><a v-if="item.archive_url && item.cloud_upload_status === 'succeeded'" :href="item.archive_url" target="_blank" rel="noopener">打开归档表</a><button v-if="!['succeeded','skipped'].includes(String(item.cloud_upload_status || '')) && item.file_available !== false" :disabled="item._retrying" @click="retryExportUpload(item)">{{ item._retrying ? '上传中' : '重试上传' }}</button></td><td data-label="下载"><a v-if="item.file_available !== false" :href="api + '/exports/' + item.export_id + '/download'">下载</a><span v-else>本地已清理</span></td><td data-label="操作"><button v-if="item.file_available !== false" title="清理导出文件" aria-label="清理导出文件" :disabled="item.cloud_upload_status === 'uploading' || item._retrying || item._cleaning" @click="confirmCleanup(item)"><Trash2 :size="16" /></button></td></tr></tbody></table><footer v-if="exportTotal > 20" class="pagination"><span>共 {{ exportTotal }} 个文件</span><button :disabled="exportPage <= 1 || exportHistoryLoading" @click="showExports(exportPage - 1)"><ChevronLeft :size="16" /></button><button :disabled="exportPage >= exportPageCount || exportHistoryLoading" @click="showExports(exportPage + 1)"><ChevronRight :size="16" /></button></footer></section>

    <section v-if="!scope" class="buildings">
      <button v-for="building in buildings" :key="building.scope" class="building" :disabled="building.bootstrap_status !== 'succeeded'" @click="navigate('/cabinet-power?scope=' + building.scope)">
        <div class="building-title"><Building2 :size="26" /><h2>{{ building.scope }}楼</h2><ArrowUpRight :size="18" /></div>
        <template v-if="building.bootstrap_status === 'succeeded'"><strong>{{ building.counts.total }}<small>机柜</small></strong><div class="building-stats"><span>正式电<b>{{ building.counts.formal }}</b></span><span>测试电<b>{{ building.counts.test }}</b></span><span>未上电/已下电<b>{{ building.counts.off }}</b></span></div><p>{{ building.record_count }} 条台账 · {{ building.inventory_only }} 柜无操作 · {{ building.counts.unknown }} 柜待核实</p></template>
        <div v-else class="building-loading"><Loader2 v-if="['pending','running'].includes(building.bootstrap_status)" class="spin" :size="20" /><strong>{{ bootstrapStatusLabel(building.bootstrap_status) }}</strong><small>{{ building.bootstrap_error || '正在等待本楼资料初始化' }}</small></div>
      </button>
    </section>
    <template v-else-if="overview.rooms">
      <section class="metrics">
        <div v-for="item in metricItems" :key="item.key" :class="item.key"><button class="metric-link" :aria-label="'查看' + item.label + '机柜'" @click="showStateRacks(item.key)"><span>{{ item.label }}</span><strong>{{ overview.counts[item.key] }}</strong></button></div>
      </section>
      <nav class="tabs" aria-label="机柜视图">
        <button v-for="item in tabs" :key="item.key" :class="{ active: tab === item.key }" @click="changeTab(item.key)">{{ item.label }}</button>
        <span>{{ overview.record_count }} 条台账</span>
      </nav>
      <section v-if="tab === 'overview'" class="table-wrap mobile-card-table">
        <table><thead><tr><th>包间</th><th>总数</th><th>已上电</th><th>正式电</th><th>测试电</th><th>未上电/已下电</th><th>待核实</th><th>网络机柜</th><th>服务器机柜</th><th>平面图</th></tr></thead><tbody>
          <tr v-for="room in overview.rooms" :key="room.id">
            <td data-label="包间"><button class="link" @click="showRoomRecords(room.id)">{{ room.carrier ? 'B-' + room.id + '运营商机房' : room.name }}<small>{{ room.id }} 包间</small></button></td><td data-label="总数"><button class="link" @click="showStateRacks('total',room.id)">{{ room.total }}</button></td>
            <td data-label="已上电"><button class="link" :aria-label="room.id + '包间已上电'" @click="showStateRacks('powered',room.id)">{{ room.counts.formal + room.counts.test }}</button></td>
            <td v-for="state in ['formal','test','off','unknown']" :key="state" :data-label="stateLabels[state]"><button class="link" :class="state + '-text'" :aria-label="room.id + '包间' + stateLabels[state]" @click="showStateRacks(state,room.id)">{{ room.counts[state] }}</button></td>
            <td data-label="网络机柜">{{ room.types['网络机柜'] }}</td><td data-label="服务器机柜">{{ room.types['服务器机柜'] }}</td><td data-label="平面图"><button v-if="room.carrier" class="link" @click="tab = 'carrier'; rackRoom = room.id; rackState = ''">运营商机房</button><button v-else class="link" @click="tab = 'layout'; selectRoom(room.id)">{{ room.sheet ? '查看平面图' : '查看目录' }}</button></td>
          </tr>
        </tbody></table>
      </section>
      <section v-else-if="tab === 'racks' || tab === 'carrier'">
        <div class="filter-bar"><select v-model="rackState" aria-label="筛选机柜状态"><option value="">全部状态</option><option value="powered">已上电</option><option v-for="(label, state) in stateLabels" :key="state" :value="state">{{ label }}</option></select><select v-model="rackRoom" aria-label="筛选机柜包间"><option value="">全部包间</option><option v-for="room in rackRooms" :key="room.id" :value="room.id">{{ room.carrier ? 'B-' + room.id + '运营商机房' : room.name }}</option></select><label class="search"><Search :size="16" /><input v-model="rackSearch" placeholder="机柜号" aria-label="筛选机柜号" /></label></div>
        <div v-if="tab === 'carrier'" class="carrier-summary"><div v-for="room in rackRooms" :key="room.id"><h3>B-{{ room.id }}运营商机房</h3><span>共 {{ room.total }} 柜</span><button v-for="state in ['formal','test','off','unknown']" :key="state" class="link" @click="rackState = state; rackRoom = room.id">{{ stateLabels[state] }} {{ room.counts[state] }}</button><button :disabled="!room.unlocated || saving" @click="registerCarrier(room.id)"><Plus :size="16" />补登机柜编号</button></div></div>
        <div class="table-wrap mobile-card-table"><table aria-label="机柜状态明细"><thead><tr><th>包间</th><th>机柜</th><th>状态</th><th>类型</th><th>最近成功操作</th><th>操作</th></tr></thead><tbody><tr v-for="rack in stateRacks.slice((rackPage-1)*50,rackPage*50)" :key="rack.room + rack.rack"><td data-label="包间">{{ rack.room }}</td><td data-label="机柜"><button class="link" @click="openHistory(rack.room,rack.rack)">{{ rack.rack }}</button></td><td data-label="状态"><span><i :style="{ background: stateColors[rack.state] }" /> {{ stateLabels[rack.state] }}</span></td><td data-label="类型">{{ rack.rack_type || '未填写' }}</td><td data-label="最近成功操作">{{ rack.last_operation || '无已完成操作' }}</td><td data-label="操作"><button class="link" @click="openHistory(rack.room,rack.rack)">操作历史</button><button v-if="rack.positions?.length" class="link rack-map-link" @click="locateRack(rack)">查看平面图</button><span v-else class="rack-map-link">无平面图</span></td></tr><tr v-for="room in unnumberedRooms" :key="room.id + '-unnumbered'"><td data-label="包间">{{ room.id }}</td><td data-label="机柜">编号未提供（{{ unnumberedCount(room) }} 柜）</td><td data-label="状态">{{ rackState ? stateLabels[rackState] : '原表汇总' }}</td><td data-label="类型">网络机柜</td><td data-label="最近成功操作">原汇总数量</td><td data-label="操作"><button class="link" @click="tab = 'carrier'; rackRoom = room.id">运营商机房</button></td></tr></tbody></table><p v-if="!stateRacks.length && !unnumberedRooms.length" class="empty">没有符合条件的机柜</p></div>
        <footer class="pagination"><span>{{ stateRacks.length }} 个已编号机柜<span v-if="unnumberedRooms.length">，另有 {{ unnumberedRooms.reduce((n: number,r: Dict) => n + unnumberedCount(r),0) }} 柜编号未提供</span></span><template v-for="(page,i) in rackPageNumbers" :key="page"><span v-if="i && page-rackPageNumbers[i-1]>1">…</span><button class="page-number" :class="{ active: page === rackPage }" :aria-label="'第 ' + page + ' 页'" @click="rackPage = page">{{ page }}</button></template></footer>
      </section>
      <section v-else-if="tab === 'layout'" class="map-shell">
        <aside class="room-sidebar"><label class="search"><Search :size="16" /><input v-model="mapSearch" placeholder="包间或机架" aria-label="搜索平面图" /></label><button v-for="room in filteredRooms" :key="room.id" :class="{ active: currentRoom === room.id }" @click="selectRoom(room.id)"><b>{{ room.id }} 包间</b><small>{{ room.total }} 柜</small></button></aside>
        <div class="map-main">
          <div class="map-toolbar"><div class="legend"><span v-for="(label, state) in stateLabels" :key="state"><i :style="{ background: stateColors[state] }" />{{ label }}</span></div><div class="actions"><button aria-label="缩小" title="缩小" @click="zoom = Math.max(.25, zoom - .15)"><ZoomOut :size="16" /></button><button @click="fitMap">适应窗口</button><button aria-label="放大" title="放大" @click="zoom = Math.min(3, zoom + .15)"><ZoomIn :size="16" /></button></div></div>
          <p v-if="layoutLoading">正在读取平面图…</p>
          <div v-else-if="layout.layout" ref="viewport" class="map-viewport">
            <div :style="{ width: layout.layout.width * zoom + 'px', height: layout.layout.height * zoom + 'px', position: 'relative' }">
              <div class="map-canvas" :style="{ width: layout.layout.width + 'px', height: layout.layout.height + 'px', transform: 'scale(' + zoom + ')' }">
                <component :is="cell.rack || cell.metric && !cell.metric_building ? 'button' : 'div'" v-for="cell in layout.layout.cells" :key="cell.ref" class="map-cell" :class="{ found: cell.rack && mapSearch.toUpperCase() === cell.rack }" :style="cellStyle(cell)" :title="cell.rack ? cell.rack + ' ' + stateLabels[cell.state] : cell.metric && !cell.metric_building ? '查看' + (cell.metric === 'total' ? '全部机柜' : cell.metric === 'powered' ? '已上电机柜' : stateLabels[cell.metric] + '机柜') : cell.text" @click="cell.rack ? openHistory(currentRoom, cell.rack) : cell.metric && !cell.metric_building && showStateRacks(cell.metric,currentRoom)">{{ cell.text }}</component>
              </div>
            </div>
          </div>
          <div v-else class="rack-list"><button v-for="rack in layout.racks || []" :key="rack.rack" @click="openHistory(currentRoom, rack.rack)"><i :style="{ background: stateColors[rack.state] }" />{{ rack.rack }} {{ stateLabels[rack.state] }}</button></div>
        </div>
      </section>
      <section v-else>
        <nav class="sheet-tabs" aria-label="原始工作表"><button v-for="format in overview.sheet_formats || []" :key="format.sheet" :class="{ active: query.sheet === format.sheet }" @click="query.sheet = format.sheet; loadRecords(1)">{{ format.sheet }} <small>{{ format.count }}</small></button></nav>
        <div class="filter-bar">
          <label class="search"><Search :size="16" /><input v-model="query.q" placeholder="机架、包间、来源或操作" @keyup.enter="loadRecords(1)" /></label>
          <select v-model="query.room" aria-label="筛选包间" @change="loadRecords(1)"><option value="">全部包间</option><option v-for="room in overview.rooms" :key="room.id" :value="room.id">{{ room.id }}</option></select>
          <select v-model="query.direction" aria-label="筛选历史" @change="loadRecords(1)"><option value="">全部记录</option><option value="up">含上电/转换历史</option><option value="down">含下电历史</option><option value="empty">无操作机柜</option></select>
          <input v-model="query.from" type="date" aria-label="开始日期" /><input v-model="query.to" type="date" aria-label="结束日期" />
          <label class="checkbox"><input v-model="onlyIssues" type="checkbox" @change="loadRecords(1)" />待核实</label><button @click="loadRecords(1)">查询</button><button class="link" @click="resetFilters">重置</button>
        </div>
        <p v-if="recordsLoading" role="status">正在读取记录…</p>
        <div class="table-wrap source-table-wrap mobile-card-table"><table class="source-table" :style="{ width: sourceTableWidth + 'px' }">
          <colgroup><col v-for="column in activeFormat?.columns || []" :key="column.column" :style="{ width: column.width + 'px' }" /><col style="width:150px" /></colgroup>
          <thead><tr><th v-for="column in activeFormat?.columns || []" :key="column.column" :class="{ frozen: column.column <= 4 }" :style="frozenStyle(column)">{{ column.label }}<small v-if="column.group !== undefined">{{ groupLabel(column.group, activeFormat) }}</small></th><th class="row-actions">操作</th></tr></thead>
          <tbody><tr v-for="op in records.items || []" :key="op.record_id" :class="{ 'source-issue': op.issues.length }">
            <td v-for="column in activeFormat?.columns || []" :key="column.column" :class="{ frozen: column.column <= 4, 'empty-cell': !sourceCell(op, column) }" :data-label="column.group === undefined ? column.label : groupLabel(column.group, activeFormat) + ' · ' + column.label" :style="frozenStyle(column)"><button v-if="column.field === 'rack'" :disabled="recordsLoading" class="link" @click="openHistory(op.room, op.rack)">{{ op.rack }}</button><span v-else>{{ sourceCell(op, column) }}</span></td>
            <td class="row-actions" data-label="操作"><button :disabled="recordsLoading" class="icon-button" title="查看完整历史" aria-label="查看完整历史" @click="openRecordDetails(op)"><History :size="16" /></button><button :disabled="recordsLoading" class="icon-button" title="编辑记录" aria-label="编辑记录" @click="openEditor(op)"><Pencil :size="16" /></button><small v-if="op.issues.length" class="test-text">待核实 {{ op.issues.length }} 项</small></td>
          </tr></tbody>
        </table><p v-if="!recordsLoading && !records.total" class="empty">没有符合条件的记录</p></div>
        <footer class="pagination"><span>共 {{ records.total || 0 }} 条记录</span><button :disabled="recordsLoading || records.page <= 1" aria-label="上一页" @click="loadRecords(records.page - 1)"><ChevronLeft :size="16" /></button><template v-for="(pageNumber, index) in pageNumbers" :key="pageNumber"><span v-if="index && pageNumber - pageNumbers[index - 1] > 1" class="ellipsis">…</span><button class="page-number" :class="{ active: pageNumber === records.page }" :disabled="recordsLoading" :aria-label="'第 ' + pageNumber + ' 页'" :aria-current="pageNumber === records.page ? 'page' : undefined" @click="loadRecords(pageNumber)">{{ pageNumber }}</button></template><button :disabled="recordsLoading || records.page >= totalPages" aria-label="下一页" @click="loadRecords(records.page + 1)"><ChevronRight :size="16" /></button></footer>
      </section>
      <details v-if="overview.issues.length && tab === 'overview'" class="issues"><summary>{{ overview.issues.length }} 项资料待核实</summary><div class="issue-scroll"><button v-for="(issue, i) in overview.issues" :key="i" class="issue-row" @click="editIssue(issue.record_id)"><span>{{ issue.room }} / {{ issue.rack }} · {{ issue.message }}</span><small>{{ issue.source }} {{ issue.source_row ? '第 ' + issue.source_row + ' 行' : '' }}</small><Pencil :size="14" /></button></div></details>
    </template>

    <div v-if="historyOpen" class="scrim" :inert="discardDialogOpen || restoreDialogOpen || editorOpen" @click.self="historyOpen = false">
      <section class="drawer modal" role="dialog" aria-modal="true" aria-label="机柜完整历史" tabindex="-1">
        <header><div><h2>{{ historyRoom }} / {{ historyRack }}</h2><p v-if="selectedRack">当前状态：{{ stateLabels[selectedRack.state] }} · {{ selectedRack.rack_type }}</p></div><button @click="historyOpen = false" aria-label="关闭历史"><X :size="20" /></button></header>
        <div class="drawer-body">
          <dl v-if="selectedRack?.latest_success" class="state-facts"><div><dt>最近成功操作</dt><dd>{{ selectedRack.latest_success.action }}</dd></div><div><dt>实际完成时间</dt><dd>{{ selectedRack.latest_success.actual }}</dd></div></dl>
          <div v-if="selectedRack" class="actions state-actions"><button v-for="target in ['formal','test','off']" :key="target" :disabled="saving || selectedRack.state === target || selectedRack.state === 'unknown'" @click="openStateSwitch(target)">{{ stateAction(target) }}</button></div>
          <p v-if="historyLoading">正在读取完整历史…</p>
          <article v-for="op in history.items || []" :key="op.record_id" class="history-record">
            <div class="section-title"><strong>{{ op.source || '飞书记录' }} {{ op.source_row ? '第 ' + op.source_row + ' 行' : '' }}</strong><button class="link" @click="openEditor(op)">编辑</button></div>
            <p v-for="issue in op.issues" :key="issue" class="test-text">{{ issue }}</p>
            <ol v-if="op.events.length" class="timeline"><li v-for="(event, i) in sortedEvents(op.events)" :key="event.id || i"><b>{{ event.action }} · {{ event.result || '待核实' }}</b><dl class="event-times"><div><dt>期望完成时间</dt><dd><time>{{ event.expected || '未填写' }}</time></dd></div><div><dt>实际完成时间</dt><dd><time>{{ event.actual || '未填写' }}</time></dd></div></dl><p v-if="event.failure_reason" class="event-failure">失败原因：{{ event.failure_reason }}</p><div v-if="event.evidence_images?.length" class="history-images"><button v-for="image in imagesWithIds(event)" :key="image.image_id" type="button" :aria-label="'查看确认截图 ' + event.action" @click="previewEvidence = evidenceUrl(op.record_id,image.image_id)"><img :src="evidenceUrl(op.record_id,image.image_id)" alt="上下电确认截图" loading="lazy" /></button></div></li></ol>
            <p v-else>机柜资料已登记，尚无操作。</p>
            <details v-if="op.issues.length"><summary>原始操作内容</summary><div v-for="(group, i) in op.groups" :key="i" class="raw-group"><div><small>操作类型</small><pre>{{ group.action }}</pre></div><div><small>期望完成时间</small><pre>{{ group.expected || '未填写' }}</pre><small>实际完成时间</small><pre>{{ group.actual || '未填写' }}</pre></div></div></details>
          </article>
          <button v-if="history.items?.length < history.total" @click="moreHistory">加载更多</button>
        </div>
      </section>
    </div>
    <div v-if="editorOpen" class="scrim editor-layer" :inert="discardDialogOpen || restoreDialogOpen">
      <section class="editor modal" role="dialog" aria-modal="true" aria-label="编辑机柜记录" tabindex="-1">
        <header><h2>{{ editingId ? '编辑机柜记录' : '新增机柜记录' }}</h2><button :disabled="saving" @click="closeEditor" aria-label="关闭编辑"><X :size="20" /></button></header>
        <form @submit.prevent="saveRecord">
          <div class="editor-body"><fieldset class="editor-fields" :disabled="saving || moveLoading">
            <div class="form-grid">
              <label v-if="isAdmin && editingId">楼栋<select :value="form.scope || scope" :disabled="!!form.target_state" @change="changeRecordScope(($event.target as HTMLSelectElement).value)"><option v-for="s in ['A','B','C','D','E']" :key="s" :value="s">{{ s }}楼</option></select></label>
              <label>包间<select v-model="form.room" required :disabled="!!form.target_state"><option value="">请选择</option><option v-for="room in editorOverview.rooms || []" :key="room.id" :value="room.id">{{ room.id }}</option></select></label>
              <label>机架<input v-model="form.rack" required pattern="[A-Za-z][0-9]{2}" :disabled="!!form.target_state" list="rack-options" /><datalist id="rack-options"><option v-for="rack in editorRacks" :key="rack.rack" :value="rack.rack" /></datalist></label>
              <label>机柜类型<select v-model="form.rack_type"><option value="">未填写</option><option>网络机柜</option><option>服务器机柜</option></select></label>
              <small v-if="editingId">当前基础类型：{{ form.current_rack_type || '未填写' }}</small>
              <label>功率（W）<input v-model="form.power" type="number" min="0" step="any" /></label>
              <label v-if="!editingId">工作表<select :value="form.source" @change="requestSheetChange"><option v-for="format in overview.sheet_formats || []" :key="format.sheet">{{ format.sheet }}</option></select></label>
            </div>
            <div class="section-title"><h3>{{ form.source || '操作明细' }}</h3><button type="button" @click="form.groups.push(newGroup())"><Plus :size="16" />添加一组</button></div>
            <div v-for="(group, i) in editableGroups" :key="group.id || i" class="group-editor" :class="{ 'current-operation': ['D','E'].includes(scope) && i === 0, 'history-operation': ['D','E'].includes(scope) && i > 0 }">
              <b>{{ editorGroupLabel(group, i) }}</b>
              <label>操作类型<select v-if="!group.action || actionOptions.includes(group.action)" v-model="group.action" :disabled="!!form.target_state && group._editing"><option value="">未填写</option><option v-for="action in actionOptions" :key="action">{{ action }}</option></select><textarea v-else v-model="group.action" rows="2" :disabled="!!form.target_state && group._editing" /></label>
              <label>期望完成时间<input v-if="singleDate(group.expected)" v-model="group.expected" type="datetime-local" step="1" /><textarea v-else v-model="group.expected" rows="2" /></label>
              <label>实际完成时间<input v-if="singleDate(group.actual)" v-model="group.actual" type="datetime-local" step="1" :required="Boolean(group.action)" /><textarea v-else v-model="group.actual" rows="2" :required="Boolean(group.action)" /></label>
              <label>操作结果<select v-model="group.result"><option value="">待核实</option><option>成功</option><option>失败</option></select></label>
              <button type="button" title="移除此组" aria-label="移除此组" :disabled="!!form.target_state && group._editing" @click="removeGroup(group)"><Trash2 :size="16" /></button>
              <div v-if="group.result === '失败' || imagesWithIds(group).length" class="group-evidence"><label v-if="group.result === '失败'">失败原因<input v-model="group.failure_reason" maxlength="1000" required placeholder="填写本次操作失败原因" /></label><div v-if="imagesWithIds(group).length" class="history-images"><button v-for="image in imagesWithIds(group)" :key="image.image_id" type="button" :aria-label="'查看确认截图 ' + image.image_id.slice(0,8)" @click="previewEvidence = evidenceUrl(editingId,image.image_id)"><img :src="evidenceUrl(editingId,image.image_id)" alt="上下电确认截图" loading="lazy" /></button></div></div>
            </div>
            <label v-if="form.original_scope && form.original_scope !== (form.scope || scope)" class="checkbox"><input v-model="form.confirm_scope_move" type="checkbox" required />将原 {{ form.original_scope }} 楼记录调整到 {{ form.scope }} 楼</label>
            <div v-if="saveError" class="notice danger" role="alert">{{ saveError }}</div>
            <div v-if="pendingWrites.some(p => p.operation_id === writeId)" class="actions"><button type="button" @click="resumePending(writeId)">继续核验</button><button type="button" @click="reconcilePending(writeId)">载入云端版本</button></div>
          </fieldset></div>
          <footer><span v-if="saving || moveLoading" role="status"><Loader2 :size="16" class="spin" />{{ moveLoading ? '正在读取目标楼栋…' : saveStepLabel }}</span><button v-if="saving && saveStatus.operation_id && saveStatus.status !== 'unknown'" type="button" @click="editorOpen = false; historyOpen = false">后台继续</button><button type="button" :disabled="saving" @click="closeEditor">取消</button><button class="primary" :disabled="saving || moveLoading"><Save :size="16" />保存</button></footer>
        </form>
      </section>
    </div>
    <ConfirmDialog
      :open="discardDialogOpen"
      tone="warning"
      kicker="未保存修改"
      title="放弃当前修改？"
      :message="discardMessage"
      confirm-label="放弃修改"
      cancel-label="继续编辑"
      confirm-class="danger"
      @resolve="resolveDiscardConfirmation"
    />
    <ConfirmDialog :open="Boolean(cleanupTarget)" tone="danger" title="清理导出文件？" message="清理后无法再次从本机下载此文件；机柜台账和已上传的云端归档不受影响。" confirm-label="清理文件" cancel-label="保留文件" @resolve="resolveExportCleanup" />
    <ConfirmDialog :open="restoreDialogOpen" title="恢复未保存的机柜记录？" message="检测到上次未完成的填写。" confirm-label="恢复编辑" cancel-label="丢弃草稿" @resolve="restoreDraft" />
    <div v-if="previewEvidence" class="evidence-preview" role="dialog" aria-modal="true" aria-label="上下电确认截图原图" @click.self="previewEvidence = ''"><button aria-label="关闭原图" @click="previewEvidence = ''"><X :size="20" /></button><img :src="previewEvidence" alt="上下电确认截图原图" /></div>
  </main>
</template>

<script setup lang="ts">
import { resilientStorage } from "../browserStorage";
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { ArrowUpRight, Building2, ChevronLeft, ChevronRight, ClipboardList, CloudUpload, Download, ExternalLink, FileCheck2, FileSpreadsheet, Files, History, Loader2, Pencil, Plus, RefreshCw, Save, Search, Trash2, TriangleAlert, X, ZoomIn, ZoomOut } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { navigate, registerNavigationGuard } from '../navigation';
import ConfirmDialog from './ConfirmDialog.vue';
import VnetBackButton from './VnetBackButton.vue';
const props = defineProps<{ scope: string; isAdmin: boolean; userId?: string }>();
const api = '/api/cabinet-power';
const read = (path: string, params: Dict = {}, timeoutMs = 90000, signal?: AbortSignal) => requestJson(api + '/' + path + '?' + new URLSearchParams({ scope: props.scope, ...params }), { timeoutMs, signal });
const write = (path: string, data: Dict, method = 'POST') => requestJson(api + '/' + path, { method, body: JSON.stringify({ ...data, scope: data.scope || props.scope }), timeoutMs: 90000 });
const storageWarning = ref('');
const storageFailed = () => { storageWarning.value = '浏览器无法保存恢复信息，请保持页面打开直到任务完成；未保存草稿在关闭页面后可能丢失。'; };
const taskStorage = resilientStorage('localStorage', storageFailed);
const draftStorage = resilientStorage('sessionStorage', storageFailed);
const overview = ref<Dict>({}), buildings = ref<Dict[]>([]), loading = ref(false), error = ref(''), message = ref(''), bootstrap = ref<Dict>({});
const batchPendingCount = ref(0);
const batchCreateUrl = computed(() => `/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? { scope:props.scope } : {}), mode:'new' })}`);
function openTodoBatches():void{navigate(`/cabinet-power/batches?${new URLSearchParams({...(props.scope?{scope:props.scope}:{}),status:'todo'})}`);}
const bootstrapActive = computed(() => ['starting','pending','running'].includes(String(bootstrap.value.status || '')));
const bootstrapTarget = computed(() => props.scope ? (bootstrap.value.buildings || []).find((item: Dict) => item.scope === props.scope) : null);
const bootstrapTargetReady = computed(() => props.scope ? bootstrapTarget.value?.status === 'succeeded' : bootstrap.value.status === 'succeeded');
const bootstrapHasFailures = computed(() => Number(bootstrap.value.failed || 0) > 0);
const bootstrapFailed = computed(() => ['failed','partial'].includes(String(bootstrap.value.status || '')) && !bootstrapTargetReady.value);
const bootstrapVisible = computed(() => bootstrapActive.value || ['failed','partial'].includes(String(bootstrap.value.status || '')));
const bootstrapMessage = computed(() => {
  if (bootstrapActive.value) return `正在从多维表初始化本地资料，已完成 ${bootstrap.value.ready || 0}/${bootstrap.value.total || 5} 栋`;
  const failures = (bootstrap.value.buildings || []).filter((item: Dict) => item.status === 'failed').map((item: Dict) => `${item.scope}楼：${item.error || '拉取失败'}`);
  return bootstrapTargetReady.value ? `当前楼栋已可用；${failures.join('；')}` : failures.join('；') || '本地资料初始化失败';
});
const bootstrapStatusLabel = (status: string) => ({ succeeded:'完成', running:'拉取中', pending:'等待', failed:'失败', idle:'等待' }[status] || '等待');
const tab = ref('overview'), tabs = computed(() => [{ key: 'overview', label: '包间汇总' }, { key: 'layout', label: '原始平面图' }, { key: 'records', label: '机柜台账' }, { key: 'racks', label: '机柜状态' }, ...(props.scope === 'B' ? [{ key: 'carrier', label: '运营商机房' }] : [])]);
const rackState = ref(''), rackRoom = ref(''), rackSearch = ref(''), rackPage = ref(1);
const racksLoading = ref(false);
const rackRooms = computed(() => (overview.value.rooms || []).filter((r: Dict) => tab.value !== 'carrier' || r.carrier));
const stateRacks = computed(() => (overview.value.racks || []).filter((r: Dict) => (!rackState.value || r.state === rackState.value || rackState.value === 'powered' && ['formal','test'].includes(r.state)) && (!rackRoom.value || r.room === rackRoom.value) && r.rack.includes(rackSearch.value.toUpperCase()) && rackRooms.value.some((room: Dict) => room.id === r.room)));
const paginationPages = (current: number,total: number) => [...new Set([1,total,...Array.from({length:5},(_,i)=>current+i-2)])].filter(p=>p>=1 && p<=total).sort((a,b)=>a-b);
const rackPageNumbers = computed(() => paginationPages(rackPage.value,Math.max(1,Math.ceil(stateRacks.value.length/50))));
watch(() => stateRacks.value.length, n => { rackPage.value = Math.min(rackPage.value,Math.max(1,Math.ceil(n/50))); });
const unnumberedCount = (room: Dict) => room.unlocated_counts?.[rackState.value || 'total'] || 0;
const unnumberedRooms = computed(() => rackRooms.value.filter((room: Dict) => !rackSearch.value && (!rackRoom.value || room.id === rackRoom.value) && unnumberedCount(room)));
watch([rackState,rackRoom,rackSearch,tab],() => { rackPage.value = 1; });
function showStateRacks(state: string, room = ''): void { rackState.value = state === 'total' ? '' : state; rackRoom.value = room; rackSearch.value = ''; tab.value = 'racks'; }
async function locateRack(rack: Dict): Promise<void> { tab.value = 'layout'; mapSearch.value = rack.rack; await selectRoom(rack.room); await nextTick(); viewport.value?.querySelector('.found')?.scrollIntoView({ block: 'center', inline: 'center' }); }
function registerCarrier(room: string): void { openEditor(); Object.assign(form,{ room, rack: '', rack_type: '网络机柜', add_inventory: true }); editBaseline = JSON.stringify(form); }
const metricItems = [{ key: 'total', label: '机柜总数' }, { key: 'powered', label: '已上电' }, { key: 'formal', label: '正式电' }, { key: 'test', label: '测试电' }, { key: 'off', label: '未上电 / 已下电' }, { key: 'unknown', label: '待核实' }];
const stateLabels: Dict = { formal: '正式电', test: '测试电', off: '未上电/已下电', unknown: '待核实' };
const stateColors: Dict = { formal: '#ff0000', test: '#ffc000', off: '#00b050', unknown: '#94a3b8' };
const actionOptions = ['上正式电', '上测试电', '测试电转正式电', '正式电转测试电', '下正式电', '下测试电'];
const singleDate = (value: string) => !value || /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$/.test(value);
let disposed = false, pollTimer: number | undefined, bootstrapTimer: number | undefined, recordSequence = 0, mapSequence = 0, historySequence = 0, initialDataLoaded = false, loadedReadyCount = -1;
let recordAbort: AbortController | undefined, mapAbort: AbortController | undefined, historyAbort: AbortController | undefined;
const scheduleVisible = (callback: () => void, delay: number) => window.setTimeout(() => document.hidden ? scheduleVisible(callback, Math.max(delay, 2000)) : callback(), delay);
const cancelled = (exc: any) => String(exc?.message || '').includes('已取消');
function fail(exc: any): void { error.value = exc?.message || '读取失败，请重试'; }
async function applyBootstrap(data: Dict): Promise<void> {
  bootstrap.value = data;
  if (error.value.includes('初始化状态暂未读取到')) error.value = '';
  if (props.scope && bootstrapTargetReady.value && !initialDataLoaded) { await load(); initialDataLoaded = Boolean(overview.value.rooms); }
  if (!props.scope && Number(data.ready || 0) !== loadedReadyCount) { await load(); loadedReadyCount = Number(data.ready || 0); initialDataLoaded = loadedReadyCount > 0; }
  if (bootstrapActive.value && !disposed) bootstrapTimer = scheduleVisible(pollBootstrap,1000);
}
async function pollBootstrap(): Promise<void> {
  if (disposed) return;
  try { await applyBootstrap(await read('bootstrap',{},15000)); }
  catch { error.value = '初始化状态暂未读取到，后台仍在继续拉取…'; if (!disposed) bootstrapTimer = scheduleVisible(pollBootstrap,4000); }
}
async function startBootstrap(retryFailed = false): Promise<void> {
  if (bootstrapActive.value) return;
  error.value = ''; bootstrap.value = { status:'starting', total:5, ready:0, buildings:['A','B','C','D','E'].map(scope => ({ scope, status:'pending' })) };
  try { await applyBootstrap(await write('bootstrap',{ retry_failed:retryFailed })); }
  catch (exc: any) {
    if (exc?.status === 404) {
      const buildings = (bootstrap.value.buildings || ['A','B','C','D','E'].map(scope => ({ scope }))).map((item: Dict) => item.status === 'succeeded' ? item : ({ ...item, status:'failed', error:'后台未加载初始化接口' }));
      const ready = buildings.filter((item: Dict) => item.status === 'succeeded').length;
      bootstrap.value = { ...bootstrap.value, status:ready ? 'partial' : 'failed', total:5, completed:5, ready, failed:5-ready, buildings };
      error.value = '当前后台仍是旧版本，未加载机柜初始化接口。请彻底退出程序后重新启动；若仍出现此提示，请结束占用 18766 端口的旧后台进程。';
      return;
    }
    bootstrap.value = { ...bootstrap.value, status:'running' };
    error.value = '初始化请求暂未返回，后台仍在继续拉取…';
    if (!disposed) bootstrapTimer = scheduleVisible(pollBootstrap,2000);
  }
}
async function load(): Promise<void> {
  loading.value = true;
  try { if (props.scope) { overview.value = await read('overview',{summary:'1'}); if (!query.sheet) query.sheet = overview.value.sheet_formats?.[0]?.sheet || ''; racksLoading.value = true; const racks = await read('racks'); if (racks.version === overview.value.version) overview.value.racks = racks.items || []; } else buildings.value = (await read('buildings')).buildings; error.value = ''; }
  catch (exc) { fail(exc); } finally { loading.value = false; }
  racksLoading.value = false;
}
async function loadBatchCount(): Promise<void> { try { batchPendingCount.value = Number((await read('batches',{page_size:'1',...(props.scope?{scope:props.scope}:{})})).pending_count || 0); } catch {} }
const job = ref<Dict>({}), exported = ref<Dict>({}), startingJob = ref(false), startingKind = ref(''), pendingExportRequest = ref('');
const pendingWrites = ref<Dict[]>([]), exportList = ref<Dict[]>([]), exportListOpen = ref(false), exportHistoryLoading = ref(false), exportPage = ref(1), exportTotal = ref(0), cleanupTarget = ref<Dict | null>(null);
const exportPageCount = computed(() => Math.max(1,Math.ceil(exportTotal.value/20)));
const allExportItems = ref<Dict[]>([]), allExportBusy = ref(false), allExportBatchId = ref('');
const busy = computed(() => startingJob.value || ['pending', 'running'].includes(job.value.status));
const allExportReady = computed(() => Boolean(buildings.value.length) && buildings.value.every(item => item.bootstrap_status === 'succeeded'));
const allExportCompleted = computed(() => allExportItems.value.filter(item => ['succeeded','failed'].includes(item.status)).length);
const storageKey = 'cabinet-job:' + props.scope;
const exportRequestKey = 'cabinet-export-request:v1:' + props.scope + ':' + (props.userId || 'anonymous');
const allExportStorageKey = 'cabinet-all-export:v1:' + (props.userId || 'anonymous');
const cloudUploadLabel = (item: Dict) => ({ pending:'等待上传', uploading:'正在上传多维', succeeded:'已上传多维', failed:'上传失败', skipped:'未配置云端归档' }[String(item.cloud_upload_status || '')] || '等待上传');
function allExportLabel(item: Dict): string {
  if (item.result?.deleted) return '本地文件已清理';
  if (item.status === 'failed') return '导出失败';
  if (item.status === 'unknown') return '启动状态待核验';
  if (item.status === 'succeeded') return item.result?.cloud_upload_status === 'failed' ? '导出完成，上传失败' : item.result?.cloud_upload_status === 'skipped' ? '导出完成，未配置云端归档' : item.result?.cloud_upload_status === 'succeeded' ? '导出并上传完成' : '导出完成，归档状态待核对';
  if (item.status === 'checking') return '状态暂未确认';
  return item.status === 'running' ? '正在生成并上传' : '等待执行';
}
async function startJob(path: string): Promise<void> {
  if (busy.value) return; startingJob.value = true; startingKind.value = path; error.value = '';
  const requestId = path === 'exports' ? pendingExportRequest.value || 'single_' + crypto.randomUUID().replace(/-/g,'') : '';
  if (requestId) { pendingExportRequest.value = requestId; taskStorage.setItem(exportRequestKey,JSON.stringify({id:requestId,at:Date.now()})); }
  try { job.value = await write(path,requestId ? {batch_id:requestId} : {}); taskStorage.setItem(storageKey, job.value.job_id); void pollJob(); } catch (exc: any) { if (requestId && (!exc?.status || exc.status >= 500)) error.value = '导出启动响应未确认，后台可能仍在执行。请点击“继续上次导出”核验原任务。'; else fail(exc); } finally { startingJob.value = false; startingKind.value = ''; }
}
async function pollJob(): Promise<void> {
  if (disposed) return;
  try {
    job.value = await read('jobs/' + job.value.job_id);
    if (['pending', 'running'].includes(job.value.status)) { pollTimer = scheduleVisible(pollJob,1800); return; }
    taskStorage.removeItem(storageKey);
    if (job.value.kind === 'export' && job.value.status === 'succeeded') { pendingExportRequest.value = ''; taskStorage.removeItem(exportRequestKey); }
    if (job.value.status === 'succeeded') {
      if (job.value.kind === 'export') exported.value = job.value.result;
      await load();
      if (tab.value === 'records') await loadRecords(records.value.page || 1);
      if (tab.value === 'layout') await selectRoom(currentRoom.value);
    }
  } catch (exc) { fail(exc); if (!disposed) pollTimer = scheduleVisible(pollJob,4000); }
}
function saveAllExportState(): void {
  if (!allExportBatchId.value) return;
  const items = allExportItems.value.map(item => ({
    scope:item.scope, job_id:item.job_id || '', status:item.status, error:item.error || '',
    result:item.result?.export_id ? Object.fromEntries(['scope','export_id','filename','archive_url','cloud_upload_status','cloud_upload_error','deleted'].map(key => [key,item.result[key]])) : {},
  }));
  taskStorage.setItem(allExportStorageKey,JSON.stringify({batch_id:allExportBatchId.value,items}));
}
function finishAllExportRun(): void {
  const running = allExportItems.value.filter(item => ['pending','running'].includes(item.status)).length;
  const uncertain = allExportItems.value.filter(item => ['checking','unknown'].includes(item.status)).length;
  const failures = allExportItems.value.filter(item => item.status === 'failed' || item.result?.cloud_upload_status === 'failed').length;
  const archiveUnknown = allExportItems.value.filter(item => item.status === 'succeeded' && !['succeeded','failed','skipped'].includes(item.result?.cloud_upload_status)).length;
  error.value = ''; message.value = '';
  if (uncertain) error.value = `${uncertain} 栋导出状态尚未确认，请核验原请求，勿重新发起整批导出。`;
  else if (running) message.value = `${running} 栋仍在导出，其余楼栋可独立操作。`;
  else if (failures) error.value = `${failures} 栋上传或导出失败，可单独重试。`;
  else if (archiveUnknown) error.value = `${archiveUnknown} 栋云端归档状态尚未确认，请查看各楼导出历史。`;
  else if (allExportItems.value.some(item => item.result?.cloud_upload_status === 'skipped')) message.value = '所有楼栋已导出；未配置云端归档的楼栋未上传。';
  else message.value = '所有楼栋均已导出并上传多维表。';
}
async function pollAllExport(item: Dict): Promise<void> {
  let failures = 0;
  while (!disposed) {
    try {
      const current = await read('jobs/' + item.job_id,{ scope:item.scope });
      Object.assign(item,{ status:current.status,error:current.error || '',result:current.result || item.result });
      failures = 0; saveAllExportState();
      if (!['pending','running'].includes(current.status)) return;
      await new Promise(resolve => setTimeout(resolve,1800));
    } catch (exc: any) {
      item.status = 'checking'; item.error = exc?.status ? exc.message : '暂时无法读取任务状态，导出结果尚未确认'; saveAllExportState();
      if (++failures >= 3) return;
      await new Promise(resolve => setTimeout(resolve,4000));
    }
  }
}
async function startAllExportItem(item: Dict): Promise<void> {
  item.status = 'pending'; item.error = ''; item.job_id = ''; saveAllExportState();
  try {
    const started = await write('exports',{ scope:item.scope,batch_id:allExportBatchId.value });
    Object.assign(item,{ job_id:started.job_id,status:started.status || 'pending' }); saveAllExportState();
    await pollAllExport(item);
  } catch (exc: any) { const uncertain = !exc?.status || exc.status >= 500; item.status = uncertain ? 'unknown' : 'failed'; item.error = uncertain ? '启动响应未确认，原任务可能仍在执行；点击“核验并继续”恢复本楼请求' : exc.message; saveAllExportState(); }
}
function restoreAllExports(): void {
  const raw = taskStorage.getItem(allExportStorageKey);
  if (!raw) return;
  try {
    const saved = JSON.parse(raw);
    const items = saved?.items;
    if (!/^all_[a-f0-9]{32}$/.test(saved?.batch_id || '') || !Array.isArray(items) || items.length !== 5 ||
        new Set(items.map((item: Dict) => item.scope)).size !== 5 || items.some((item: Dict) => !/^[A-E]$/.test(String(item.scope)) || item.job_id && !/^[a-f0-9]{32}$/.test(item.job_id))) throw new Error('无效导出状态');
    allExportBatchId.value = saved.batch_id;
    allExportItems.value = items;
    const pending = items.filter((item: Dict) => ['pending','running','checking'].includes(item.status));
    if (pending.length) {
      allExportBusy.value = true;
      void Promise.all(pending.map((item: Dict) => item.job_id ? pollAllExport(item) : startAllExportItem(item)))
        .finally(() => { if (!disposed) { allExportBusy.value = false; finishAllExportRun(); } });
    }
  } catch { taskStorage.removeItem(allExportStorageKey); }
}
async function startAllExports(): Promise<void> {
  if (allExportBusy.value || !allExportReady.value || allExportItems.value.some(item => ['pending','running','checking','unknown'].includes(item.status))) return;
  allExportBatchId.value = 'all_' + crypto.randomUUID().replace(/-/g,'');
  allExportItems.value = buildings.value.map(item => ({ scope:item.scope,status:'pending',error:'',result:{} }));
  allExportBusy.value = true; error.value = ''; message.value = ''; saveAllExportState();
  try { await Promise.all(allExportItems.value.map(startAllExportItem)); }
  finally { if (!disposed) { allExportBusy.value = false; finishAllExportRun(); } }
}
async function retryAllExportItem(item: Dict): Promise<void> {
  if (item._retrying || !['failed','unknown'].includes(item.status)) return;
  item._retrying = true;
  try { await startAllExportItem(item); }
  finally { item._retrying = false; if (!disposed) finishAllExportRun(); }
}
async function retryAllExportStatus(item: Dict): Promise<void> {
  if (item._retrying || item.status !== 'checking' || !item.job_id) return;
  item._retrying = true;
  try { await pollAllExport(item); }
  finally { item._retrying = false; if (!disposed) finishAllExportRun(); }
}
async function retryExportUpload(record: Dict, allItem?: Dict): Promise<void> {
  if (record._retrying) return;
  record._retrying = true;
  record.cloud_upload_status = 'uploading'; record.cloud_upload_error = '';
  try {
    const result = await write('exports/' + record.export_id + '/upload',{ scope:record.scope });
    Object.assign(record,result); if (allItem) allItem.result = result;
    if (exported.value.export_id === result.export_id) exported.value = result;
    const history = exportList.value.find(item => item.export_id === result.export_id); if (history) Object.assign(history,result);
    if (result.cloud_upload_status === 'succeeded') message.value = `${result.scope}楼导出文件已上传多维表。`;
    else error.value = result.cloud_upload_error || `${result.scope}楼导出文件仍未上传成功。`;
  } catch (exc: any) { record.cloud_upload_status = 'failed'; record.cloud_upload_error = exc?.message || '上传失败'; fail(exc); }
  finally { record._retrying = false; if (allItem) { saveAllExportState(); finishAllExportRun(); } }
}
async function refresh(): Promise<void> { if (!initialDataLoaded || bootstrapHasFailures.value) await startBootstrap(true); else if (props.scope) await startJob('refresh'); else await load(); }
const query = reactive({ q: '', room: '', direction: '', from: '', to: '', sheet: '' }), onlyIssues = ref(false), records = ref<Dict>({}), recordsLoading = ref(false);
const activeFormat = computed(() => overview.value.sheet_formats?.find((f: Dict) => f.sheet === query.sheet));
const moveOverview = ref<Dict>({}), moveLoading = ref(false);
const editorOverview = computed(() => form.scope && form.scope !== props.scope ? moveOverview.value : overview.value);
const editorFormat = computed(() => editorOverview.value.sheet_formats?.find((f: Dict) => f.sheet === form.source));
const totalPages = computed(() => Math.max(1, Math.ceil((records.value.total || 0) / (records.value.page_size || 50))));
const pageNumbers = computed(() => paginationPages(records.value.page || 1,totalPages.value));
const sourceTableWidth = computed(() => (activeFormat.value?.columns || []).reduce((n: number, c: Dict) => n + c.width, 150));
const frozenStyle = (column: Dict) => column.column <= 4 ? { left: (activeFormat.value?.columns || []).filter((c: Dict) => c.column < column.column).reduce((n: number, c: Dict) => n + c.width, 0) + 'px' } : {};
function sourceCell(op: Dict, column: Dict): string {
  if (column.group !== undefined) return op.groups[column.group]?.[column.field] || '';
  if (column.field === 'ordinal') return String(op.meta?.cells?.['1'] || op.ordinal || op.source_row || '');
  if (column.field === 'site') return op.raw_fields?.['机房'] || 'EA118';
  return op[column.field] === undefined || op[column.field] === null ? '' : String(op[column.field]);
}
function groupLabel(index: number, format?: Dict): string {
  if (['D','E'].includes(props.scope)) return index === 0 ? '当前操作' : '历史操作 ' + index;
  const down = format?.sheet?.includes('下电') && !format?.sheet?.includes('上下电');
  if (!down) return index === 0 ? '上电信息' : '转换记录 ' + index;
  if (index === 0) return '上电信息';
  if (index === (format?.groups?.length || 2) - 1) return props.scope === 'C' ? '转换 / 下电信息' : '下电信息';
  return '转换记录';
}
function stateAction(target: string): string {
  const current = String(selectedRack.value?.state || '');
  if (current === target) return '当前为' + (stateLabels[target] || '该状态');
  const action: Dict = { 'off:formal': '上正式电', 'off:test': '上测试电', 'formal:test': '正式电转测试电', 'test:formal': '测试电转正式电', 'formal:off': '下正式电', 'test:off': '下测试电' };
  return action[current + ':' + target] ? '登记' + action[current + ':' + target] : '暂不可登记';
}
function editorGroupLabel(group: Dict, index: number): string {
  if (group?._editing) return group.action ? '本次操作 · ' + group.action : '本次操作';
  return group.action ? `操作记录 ${index + 1} · ${group.action}` : groupLabel(form.groups.indexOf(group), editorFormat.value);
}
async function loadRecords(page = 1): Promise<void> {
  recordAbort?.abort(); recordAbort = new AbortController(); const seq = ++recordSequence; recordsLoading.value = true;
  try { const data = await read('operations', { ...query, issues: String(onlyIssues.value), page, page_size: 50 },90000,recordAbort.signal); if (seq === recordSequence) records.value = data; } catch (exc) { if (!cancelled(exc)) fail(exc); } finally { if (seq === recordSequence) recordsLoading.value = false; }
}
function resetFilters(): void { Object.assign(query, { q: '', room: '', direction: '', from: '', to: '' }); onlyIssues.value = false; void loadRecords(1); }
async function changeTab(key: string): Promise<void> { tab.value = key; if (key === 'carrier') { rackRoom.value = ''; rackState.value = ''; rackSearch.value = ''; } if (key === 'records') await loadRecords(1); if (key === 'layout') await selectRoom(currentRoom.value || overview.value.rooms[0]?.id); }
function showRoomRecords(room: string): void { query.room = room; tab.value = 'records'; void loadRecords(1); }
const mapSearch = ref(''), currentRoom = ref(''), layout = ref<Dict>({}), layoutLoading = ref(false), viewport = ref<HTMLElement>(), zoom = ref(1);
const layoutCache = new Map<string,Dict>();
watch(() => overview.value.version, (version, previous) => { if (previous && version !== previous) layoutCache.clear(); });
const filteredRooms = computed(() => (overview.value.rooms || []).filter((r: Dict) => !mapSearch.value || (r.id + ' ' + r.name).includes(mapSearch.value) || (overview.value.racks || []).some((rack: Dict) => rack.room === r.id && rack.rack.includes(mapSearch.value.toUpperCase()))));
async function selectRoom(id: string): Promise<void> {
  if (!id) return; currentRoom.value = id; const cacheKey = `${overview.value.version || ''}:${id}`, cached = layoutCache.get(cacheKey); if (cached) { layout.value = cached; await nextTick(); fitMap(); return; }
  mapAbort?.abort(); mapAbort = new AbortController(); layoutLoading.value = true; const seq = ++mapSequence;
  try { const data = await read('rooms/' + id + '/layout',{},90000,mapAbort.signal); if (seq !== mapSequence) return; layoutCache.set(cacheKey,data); layout.value = data; layoutLoading.value = false; await nextTick(); fitMap(); } catch (exc) { if (!cancelled(exc)) fail(exc); } finally { if (seq === mapSequence) layoutLoading.value = false; }
}
function fitMap(): void { if (viewport.value && layout.value.layout) zoom.value = Math.max(.25, Math.min(1.2, (viewport.value.clientWidth - 20) / layout.value.layout.width)); }
function cellStyle(cell: Dict): Dict {
  const s = cell.style || {}, border: Dict = {};
  for (const [edge, value] of Object.entries(s.borders || {}) as [string, Dict][]) border['border-' + edge] = '1px solid ' + (value.color || '#bccadd');
  return { ...border, left: cell.x + 'px', top: cell.y + 'px', width: cell.width + 'px', height: cell.height + 'px', background: cell.color || s.fill || 'transparent', color: cell.rack && cell.state === 'formal' ? '#fff' : s.font_color || '#1e293b', fontSize: Math.max(9, (s.font_size || 10) * 4 / 3) + 'px', fontFamily: s.font_name || 'inherit', fontWeight: s.bold ? 700 : 400, whiteSpace: s.wrap_text ? 'pre-wrap' : 'pre' };
}
const historyOpen = ref(false), historyRoom = ref(''), historyRack = ref(''), history = ref<Dict>({}), historyLoading = ref(false);
const selectedRack = computed(() => history.value.rack_state || (overview.value.racks || []).find((r: Dict) => r.room === historyRoom.value && r.rack === historyRack.value));
const sortedEvents = (events: Dict[]) => [...events].sort((a, b) => (b.actual || '').localeCompare(a.actual || ''));
async function focusModal(): Promise<void> { await nextTick(); (document.querySelector('.cabinet-page .confirm-modal') || document.querySelector('.editor-layer .modal') || document.querySelector('.scrim .modal'))?.querySelector<HTMLElement>('button, input, select')?.focus(); }
async function openHistory(room: string, rack: string): Promise<void> {
  historyAbort?.abort(); historyAbort = new AbortController(); historyRoom.value = room; historyRack.value = rack; historyOpen.value = true; historyLoading.value = true; history.value = {}; const seq = ++historySequence; void focusModal();
  try { const data = await read('operations', { room, rack, page: 1 },90000,historyAbort.signal); if (seq === historySequence) history.value = data; } catch (exc) { if (!cancelled(exc)) fail(exc); } finally { if (seq === historySequence) historyLoading.value = false; }
}
function openRecordDetails(op: Dict): void { void openHistory(op.room,op.rack); }
async function moreHistory(): Promise<void> { try { const data = await read('operations', { room: historyRoom.value, rack: historyRack.value, page: history.value.page + 1 }); history.value = { ...data, items: [...history.value.items, ...data.items] }; } catch (exc) { fail(exc); } }
async function editIssue(id: string): Promise<void> {
  const issue = overview.value.issues.find((i: Dict) => i.record_id === id);
  if (!issue) return;
  await openHistory(issue.room, issue.rack);
  const record = history.value.items?.find((o: Dict) => o.record_id === id);
  if (record) openEditor(record);
}
const editorOpen = ref(false), discardDialogOpen = ref(false), editingId = ref(''), saving = ref(false), saveError = ref(''), form = reactive<Dict>({});
const previewEvidence = ref('');
function imagesWithIds(value:Dict):Dict[] { return (value.evidence_images || []).filter((item:Dict)=>item && item.image_id); }
function evidenceUrl(recordId:string,imageId:string):string { return `/api/cabinet-power/operations/${encodeURIComponent(recordId)}/evidence/${encodeURIComponent(imageId)}?scope=${encodeURIComponent(props.scope)}`; }
const saveStatus = ref<Dict>({}), saveQueryError = ref(false);
const saveStorageKey = 'cabinet-upload:' + (props.userId || 'session') + ':' + props.scope;
let savePollTimer: number | undefined;
const saveStepLabel = computed(() => {
  if (saveQueryError.value) return '状态暂未读到，继续核验中…';
  const s=saveStatus.value, stage=s.error_stage === 'directory' ? '机柜基础资料' : '机柜台账';
  const label = s.status === 'queued' ? '请求已受理，等待上传' : s.status === 'checking' ? (s.error_stage === 'schema' ? '正在校验字段' : '正在核对云端版本') : s.status === 'writing' ? '正在上传' + stage : s.status === 'readback' ? '正在回读核验' + stage : s.status === 'local_pending' ? '正在提交本地数据' : s.status === 'unknown' ? '正在查询提交结果' : '正在提交请求';
  return label + (s.elapsed_ms ? ' · ' + Math.floor(s.elapsed_ms/1000) + '秒' : '…');
});
const restoreDialogOpen = ref(false), discardMessage = ref('继续后，当前机柜记录中的修改会丢失。');
const newGroup = (editing = true) => ({ id: 'event_' + Array.from(crypto.getRandomValues(new Uint8Array(16)), b => b.toString(16).padStart(2, '0')).join(''), action: '', expected: '', actual: '', result: '成功', failure_reason: '', evidence_images: [], _editing: editing });
const groupHasBusinessData = (group: Dict) => Boolean(group?.action || group?.expected || group?.actual);
const editableGroups = computed(() => {
  const populated = (form.groups || []).filter((group: Dict) => groupHasBusinessData(group) || group?._editing);
  return populated.length ? populated : (form.groups || []).slice(0, 1);
});
function removeGroup(group: Dict): void { const index = form.groups.indexOf(group); if (index >= 0) form.groups.splice(index, 1); if (!form.groups.length) form.groups.push(newGroup()); }
const draftKey = 'cabinet-draft:' + (props.userId || 'session') + ':' + props.scope;
let pendingDiscard: (() => void) | undefined, draft: Dict | undefined, removeNavigationGuard: (() => void) | undefined, draftTimer: number | undefined;
function flushDraft(): void { window.clearTimeout(draftTimer); if (!editorOpen.value) return; try { draftStorage.setItem(draftKey, JSON.stringify({ scope: props.scope, editingId: editingId.value, form, editBaseline, writeId, writeHash })); } catch { saveError.value = '本机草稿保存失败，请保持页面打开直到保存完成。'; } }
function persistDraft(): void { window.clearTimeout(draftTimer); draftTimer = window.setTimeout(flushDraft,250); }
function clearDraft(): void { window.clearTimeout(draftTimer); draftStorage.removeItem(draftKey); }
function askDiscard(action: () => void, text = '继续后，当前机柜记录中的修改会丢失。'): void { pendingDiscard = action; discardMessage.value = text; discardDialogOpen.value = true; void focusModal(); }
async function restoreDraft(yes: boolean): Promise<void> { restoreDialogOpen.value = false; if (yes && draft) { Object.assign(form, draft.form); editingId.value = draft.editingId; editBaseline = draft.editBaseline; writeId = draft.writeId; writeHash = draft.writeHash; editorOpen.value = true; if (form.scope && form.scope !== props.scope) { moveLoading.value = true; try { moveOverview.value = await read('overview',{scope:form.scope}); } catch (e: any) { saveError.value = e.message; } finally { moveLoading.value = false; } } void focusModal(); } else clearDraft(); draft = undefined; }
watch(form, persistDraft, { deep: true, flush: 'post' });
const editorRacks = computed(() => (editorOverview.value.racks || []).filter((r: Dict) => r.room === form.room));
watch(() => [form.room,form.rack,form.scope],() => { if (editorOpen.value) form.current_rack_type = editorRacks.value.find((r: Dict) => r.rack === form.rack)?.rack_type || ''; });
async function changeRecordScope(scope: string): Promise<void> {
  moveLoading.value = true;
  try { const target = scope === props.scope ? overview.value : await read('overview',{ scope }); moveOverview.value = target; form.scope = scope; form.room = ''; form.rack = ''; form.confirm_scope_move = false; form.source = target.sheet_formats.find((f: Dict) => (f.sheet.includes('下电') && !f.sheet.includes('上下电')) === (form.category === 'down'))?.sheet || target.sheet_formats[0].sheet; }
  catch (e: any) { saveError.value = e.message; } finally { moveLoading.value = false; }
}
let editBaseline = '', writeId = '', writeHash = '';
function openEditor(op?: Dict): void {
  if (saving.value) { if (saveStatus.value.operation_id) void showSubmission(saveStatus.value.operation_id); return; }
  editingId.value = op?.record_id || ''; Object.keys(form).forEach(k => delete form[k]);
  form.scope = props.scope;
  Object.assign(form, { room: op?.room || historyRoom.value || currentRoom.value || '', rack: op?.rack || historyRack.value || '', rack_type: op?.rack_type || '', current_rack_type: op?.current_rack_type || '', power: op?.power ?? '', groups: op ? JSON.parse(JSON.stringify(op.groups)) : [newGroup()], source: op?.source || op?.display_sheet || query.sheet, category: op?.category || (['D','E'].includes(props.scope) ? 'mixed' : 'up'), expected_version: op?.version || '', original_scope: op?.scope || '', confirm_scope_move: false });
  if (!op) changeEditorSheet();
  if (!form.groups.length) form.groups = [newGroup()];
  writeId = ''; writeHash = ''; saveError.value = ''; editBaseline = JSON.stringify(form); discardDialogOpen.value = false; editorOpen.value = true; void focusModal();
}
function changeEditorSheet(): void {
  const format = overview.value.sheet_formats?.find((f: Dict) => f.sheet === form.source);
  form.groups = (format?.groups || [{}]).map(() => newGroup(false));
  form.category = form.source.includes('上下电') ? 'mixed' : form.source.includes('下电') ? 'down' : 'up';
}
function requestSheetChange(event: Event): void { const select = event.target as HTMLSelectElement, selected = select.value; select.value = form.source; const change = () => { form.source = selected; changeEditorSheet(); }; if (form.groups.some((g: Dict) => g.action || g.actual || g.expected)) askDiscard(change, '切换工作表会清空当前操作明细。'); else change(); }
function closeEditor(): void { if (saving.value) return; const close = () => { clearDraft(); editorOpen.value = false; if (historyOpen.value) void focusModal(); }; if (JSON.stringify(form) !== editBaseline) askDiscard(close); else close(); }
function resolveDiscardConfirmation(confirmed: boolean): void { discardDialogOpen.value = false; const action = pendingDiscard; pendingDiscard = undefined; if (confirmed) action?.(); void focusModal(); }
async function openStateSwitch(target: string): Promise<void> {
  const rack = selectedRack.value; if (!rack || rack.state === 'unknown') return;
  const actions: Dict = { 'off:formal': '上正式电', 'off:test': '上测试电', 'formal:test': '正式电转测试电', 'test:formal': '测试电转正式电', 'formal:off': '下正式电', 'test:off': '下测试电' };
  let existing: Dict | undefined;
  if (['D','E'].includes(props.scope)) { const rows = await read('operations', { room: rack.room, rack: rack.rack }); existing = rows.items[0]; }
  openEditor(existing);
  Object.assign(form, { room: rack.room, rack: rack.rack, rack_type: rack.rack_type, category: target === 'off' ? 'down' : 'up', target_state: target, expected_state: rack.state, expected_latest_time: rack.last_operation });
  const group = { ...newGroup(), action: actions[rack.state + ':' + target] };
  if (existing) { form.primary_index = 0; form.groups = [group, ...(existing.groups || []).filter((g: Dict) => g.action || g.actual || g.expected)]; }
  else {
    const format = overview.value.sheet_formats.find((f: Dict) => (f.sheet.includes('下电') && !f.sheet.includes('上下电')) === (target === 'off'));
    form.source = format.sheet; changeEditorSheet(); const index = target === 'off' ? form.groups.length - 1 : 0; form.groups[index] = group; form.primary_index = index;
  }
}
async function saveRecord(): Promise<void> {
  if (saving.value || moveLoading.value) return;
  const hash = JSON.stringify(form);
  if (writeId && hash === writeHash && pendingWrites.value.some(p=>p.operation_id === writeId)) { await resumePending(writeId); return; }
  saving.value = true; saveError.value = ''; saveStatus.value = {}; saveQueryError.value = false; message.value = '';
  if (hash !== writeHash) { writeId = Array.from(crypto.getRandomValues(new Uint8Array(16)), b => b.toString(16).padStart(2, '0')).join(''); writeHash = hash; }
  flushDraft();
  try {
    saveStatus.value = await write((editingId.value ? 'operations/' + editingId.value : 'operations') + '?defer=1', { ...form, operation_id: writeId }, editingId.value ? 'PATCH' : 'POST');
    saveStatus.value.operation_id = writeId; taskStorage.setItem(saveStorageKey,writeId); void pollSave(writeId);
  } catch (exc: any) {
    saveError.value = exc?.message || '提交结果待核实，输入已保留';
    if (!exc.status || exc.status < 400 || exc.status >= 500) { saveStatus.value = {operation_id:writeId,status:'unknown'}; taskStorage.setItem(saveStorageKey,writeId); void pollSave(writeId); }
    else { saving.value = false; await loadPending(); }
  }
}
async function refreshSavedViews(): Promise<void> {
  await Promise.all([load(),loadPending(),...(tab.value === 'records' ? [loadRecords(records.value.page || 1)] : []),...(tab.value === 'layout' ? [selectRoom(currentRoom.value)] : []),...(historyOpen.value ? [openHistory(historyRoom.value,historyRack.value)] : [])]);
}
async function pollSave(id: string): Promise<void> {
  if (disposed) return;
  try {
    const state = await read('writes/' + id,{},8000); if (disposed) return;
    saveStatus.value = state; saveQueryError.value = false;
    if (state.status === 'completed') {
      if (taskStorage.getItem(saveStorageKey) === id) taskStorage.removeItem(saveStorageKey);
      try { if (JSON.parse(draftStorage.getItem(draftKey) || '{}').writeId === id) clearDraft(); } catch {}
      if (writeId === id) editorOpen.value = false;
      saving.value = false; saveError.value = ''; message.value = '已保存到飞书，回读核验成功。'; await refreshSavedViews(); return;
    }
    if (state.status === 'cancelled') { if (taskStorage.getItem(saveStorageKey) === id) taskStorage.removeItem(saveStorageKey); saving.value = false; await loadPending(); return; }
    if (['pending','conflict'].includes(state.status)) { saving.value = false; saveError.value = state.error || '上传尚未完成，输入已保留'; await loadPending(); return; }
    savePollTimer = scheduleVisible(() => void pollSave(id),900);
  } catch (e: any) {
    if (disposed) return;
    if (e.status === 404) { saving.value = false; saveError.value = '暂未查到上传记录，输入已保留，请使用原操作再次保存。'; if (taskStorage.getItem(saveStorageKey) === id) taskStorage.removeItem(saveStorageKey); return; }
    saveQueryError.value = true; savePollTimer = scheduleVisible(() => void pollSave(id),4000);
  }
}
async function showSubmission(id: string): Promise<void> {
  try {
    const saved = await read('writes/' + id,{details:'1'}), payload = {...saved.request}; delete payload.operation_id;
    Object.keys(form).forEach(k => delete form[k]); Object.assign(form,payload); editingId.value = saved.record_id || ''; writeId = id; writeHash = JSON.stringify(form); editBaseline = writeHash;
    form.groups ||= []; editorOpen.value = true;
    if (form.scope && form.scope !== props.scope) moveOverview.value = await read('overview',{scope:form.scope});
    void focusModal();
  } catch (e) { fail(e); }
}
async function loadPending(): Promise<void> { if (props.scope) try { pendingWrites.value = (await read('writes')).items || []; } catch {} }
async function resumePending(id: string): Promise<void> { if (saving.value) return; saving.value = true; saveQueryError.value = false; try { saveStatus.value = await write('writes/' + id + '/resume?defer=1', {}); taskStorage.setItem(saveStorageKey,id); void pollSave(id); } catch (e) { saving.value = false; fail(e); await loadPending(); } }
function reconcilePending(id: string): void {
  askDiscard(() => { saving.value = true; saveStatus.value = {status:'checking',error_stage:'main'}; window.clearTimeout(savePollTimer);
    void write('writes/' + id + '/reconcile', {}).then(async () => { clearDraft(); editorOpen.value = false; if (taskStorage.getItem(saveStorageKey) === id) taskStorage.removeItem(saveStorageKey); await refreshSavedViews(); }).catch(fail).finally(()=>{saving.value = false; saveStatus.value = {};});
  }, '将载入云端最新记录。原提交内容保留在上传日志中。');
}
async function showExports(page=1): Promise<void> { exportListOpen.value = true; exportHistoryLoading.value = true; error.value = ''; try { const data=await read('export-history',{page:String(page),page_size:'20'});exportList.value=data.items||[];exportPage.value=Number(data.page||1);exportTotal.value=Number(data.total||0); } catch (e) { fail(e); } finally { exportHistoryLoading.value = false; } }
function confirmCleanup(item: Dict): void { cleanupTarget.value = item; }
async function resolveExportCleanup(confirmed: boolean): Promise<void> {
  const item = cleanupTarget.value; cleanupTarget.value = null;
  if (!confirmed || !item) return;
  item._cleaning = true;
  try {
    await write('exports/' + item.export_id + '/cleanup', { scope:item.scope });
    if (exported.value.export_id === item.export_id) exported.value = {};
    for (const entry of allExportItems.value) if (entry.result?.export_id === item.export_id) entry.result.deleted = true;
    saveAllExportState(); message.value = '本地导出文件已清理，云端归档不受影响。';
    await showExports();
  } catch (exc) { fail(exc); }
  finally { item._cleaning = false; }
}
function keyboard(e: KeyboardEvent): void {
  if (previewEvidence.value) { if (e.key === 'Escape') { e.preventDefault(); previewEvidence.value = ''; } else if (e.key === 'Tab') { e.preventDefault(); document.querySelector<HTMLElement>('.evidence-preview>button')?.focus(); } return; }
  if (!editorOpen.value && !historyOpen.value && !discardDialogOpen.value && !restoreDialogOpen.value) return;
  if (e.key === 'Escape') { e.preventDefault(); if (discardDialogOpen.value) resolveDiscardConfirmation(false); else if (restoreDialogOpen.value) restoreDraft(false); else if (editorOpen.value) closeEditor(); else historyOpen.value = false; return; }
  if (e.key !== 'Tab') return;
  const modal = document.querySelector('.cabinet-page .confirm-modal') || document.querySelector(editorOpen.value ? '.editor-layer .modal' : '.scrim .modal');
  const nodes = Array.from(modal?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), a[href]') || []);
  const first = nodes[0], last = nodes[nodes.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus(); }
  if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus(); }
}
watch(previewEvidence,value=>{if(value)void nextTick(()=>document.querySelector<HTMLElement>('.evidence-preview>button')?.focus());});
onMounted(async () => {
  window.addEventListener('keydown', keyboard);
  window.addEventListener('pagehide', flushDraft);
  removeNavigationGuard = registerNavigationGuard((_target, proceed) => { if (saving.value && editorOpen.value) return false; if (!editorOpen.value || JSON.stringify(form) === editBaseline) return true; askDiscard(() => { clearDraft(); editorOpen.value = false; proceed(); }); return false; });
  await startBootstrap(); if (disposed) return;
  if (props.scope) {
    try {
      const savedRequest = JSON.parse(taskStorage.getItem(exportRequestKey) || '{}');
      if (/^single_[a-f0-9]{32}$/.test(savedRequest.id) && Date.now() - Number(savedRequest.at) < 24*60*60*1000) pendingExportRequest.value = savedRequest.id;
      else taskStorage.removeItem(exportRequestKey);
    } catch { taskStorage.removeItem(exportRequestKey); }
  }
  if (!props.scope) restoreAllExports();
  void loadBatchCount();
  await loadPending(); if (disposed) return;
  const uploadId = taskStorage.getItem(saveStorageKey);
  if (uploadId && /^[A-Za-z0-9_-]{16,128}$/.test(uploadId)) { saving.value = true; saveStatus.value = {operation_id:uploadId,status:'queued'}; await pollSave(uploadId); if (disposed) return; }
  try { const savedDraft = draftStorage.getItem(draftKey); if (savedDraft && !saving.value) { draft = JSON.parse(savedDraft); if (draft?.scope === props.scope && Array.isArray(draft.form?.groups)) { restoreDialogOpen.value = true; void focusModal(); } } } catch { clearDraft(); }
  const saved = taskStorage.getItem(storageKey);
  if (saved && /^[a-f0-9]{32}$/.test(saved)) { job.value = { job_id: saved }; void pollJob(); } else if (saved) taskStorage.removeItem(storageKey);
});
onBeforeUnmount(() => { flushDraft(); disposed = true; recordAbort?.abort(); mapAbort?.abort(); historyAbort?.abort(); removeNavigationGuard?.(); window.removeEventListener('pagehide', flushDraft); window.removeEventListener('keydown', keyboard); window.clearTimeout(pollTimer); window.clearTimeout(bootstrapTimer); window.clearTimeout(savePollTimer); window.clearTimeout(draftTimer); });
</script>

<style scoped>
.table-wrap:has(>table[aria-label="机柜状态明细"]){max-height:60vh}.table-wrap table[aria-label="机柜状态明细"] th{position:sticky;top:0;z-index:1}.carrier-summary>div{flex-wrap:wrap}
.bootstrap-notice{align-items:flex-start}.bootstrap-copy{display:grid;flex:1;gap:8px;min-width:320px}.bootstrap-copy progress{width:100%;height:8px;accent-color:#1764dd}.bootstrap-buildings{display:flex;flex-wrap:wrap;gap:6px}.bootstrap-buildings span{border:1px solid #d4dfed;border-radius:6px;padding:3px 7px;background:#fff;color:#60768c;font-size:11px}.bootstrap-buildings .status-succeeded{border-color:#bee8d6;color:#167953}.bootstrap-buildings .status-running{border-color:#9fc5f5;color:#175dbb}.bootstrap-buildings .status-failed{border-color:#f8c9cd;color:#ae283e}
.metric-link{border:0;background:transparent;border-radius:0;display:flex;flex-direction:column;align-items:flex-start;width:100%;padding:0}.carrier-summary>div{display:flex;align-items:center;gap:20px;padding:16px 0;border-bottom:1px solid #dce6f1}.rack-map-link{margin-left:20px;color:#60768c}.building-loading{display:grid;place-items:center;gap:8px;flex:1;color:#60768c;text-align:center}.building-loading strong{font-size:16px}.building-loading small{max-width:100%;overflow-wrap:anywhere}.building:disabled{opacity:1;background:#f4f7fb}.building:disabled .building-title{color:#6c8198}
.cabinet-page{max-width:1800px;margin:auto;padding:24px;color:#203650;background:#f7f9fc;min-height:80vh;font-size:14px;letter-spacing:0}.heading{display:flex;align-items:center;gap:18px;margin-bottom:22px}.heading-title{flex:1;min-width:0}h1{font-size:26px;margin:0 0 8px}h2{font-size:20px;margin:0}h3{font-size:16px;margin:0}.heading p,.building p{margin:0;color:#63768c;font-size:12px}.actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}button,a,input,select,textarea{font:inherit}button,.actions a,.notice a{display:inline-flex;gap:6px;align-items:center;justify-content:center;border:1px solid #d4dfed;border-radius:6px;padding:9px 12px;min-height:38px;background:#fff;color:#214969;text-decoration:none;cursor:pointer}button:hover:not(:disabled){background:#edf5ff;border-color:#8db7ec}button:disabled{opacity:.45;cursor:not-allowed}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible{outline:3px solid #76aaf0;outline-offset:2px}.primary{background:#1764dd;color:white;border-color:#1764dd}.primary:hover:not(:disabled){background:#1154bc;color:white}.notice{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:12px 16px;margin:12px 0;background:#ebf4ff;border:1px solid #cbdffb;border-radius:6px;overflow-wrap:anywhere}.notice button,.notice a{margin-left:auto}.notice.danger{background:#fff0f1;border-color:#f8c9cd;color:#ae283e}.notice.success{background:#eaf9f2;border-color:#bee8d6;color:#167953}.buildings{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:18px}.building{display:flex;flex-direction:column;align-items:stretch;text-align:left;padding:22px;gap:24px;border-radius:8px;min-height:245px}.building-title{display:flex;align-items:center;gap:12px}.building-title svg:last-child{margin-left:auto}.building>strong{font-size:36px}.building strong small{display:inline;margin-left:8px;font-size:13px}.building-stats{display:flex;justify-content:space-between;gap:8px;font-size:12px}.building-stats b{display:block;margin-top:8px;font-size:22px}.metrics{display:grid;grid-template-columns:repeat(5,1fr);border-block:1px solid #dbe4ef;background:#fff;padding:18px 0;margin:20px 0}.metrics>div{display:flex;flex-direction:column;gap:8px;padding:0 20px;border-right:1px solid #edf1f6}.metrics strong{font-size:30px}.metrics .formal strong,.formal-text{color:#c42d48}.metrics .test strong,.test-text{color:#946800}.metrics .off strong,.off-text{color:#16855c}.metrics .unknown strong{color:#657990}.tabs{display:flex;align-items:center;gap:8px;margin:20px 0;border-bottom:1px solid #dbe4ef;padding-bottom:12px}.tabs .active,.room-sidebar .active{background:#eaf2ff;color:#175dbb;border-color:#bcd4f9}.tabs>span{margin-left:auto;color:#65768b;font-size:12px}.table-wrap{overflow:auto;background:#fff}table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:13px}th{text-align:left;background:#edf3fa;color:#425c77;padding:13px 12px;font-weight:600}td{padding:12px;border-bottom:1px solid #e8eef5}tbody tr:hover{background:#f8fbff}small{display:block;font-size:11px;color:#687e96;margin-top:5px}.link{padding:0;border:0;background:transparent;color:#175ebd;min-height:28px;text-align:left;display:inline-block}.icon-button{width:36px;height:36px;padding:0}.filter-bar{display:flex;flex-wrap:wrap;gap:9px;align-items:center;margin:16px 0}.search{display:flex;align-items:center;gap:7px;padding:0 10px;border:1px solid #ccd9e8;border-radius:6px;background:#fff}.search input{border:0;min-width:0;width:100%;padding-left:0}input,select,textarea{padding:9px;border:1px solid #ccd9e8;border-radius:5px;color:#263f5b;background:#fff;box-sizing:border-box;max-width:100%;min-height:38px}.checkbox{display:flex;align-items:center;gap:6px}.checkbox input{min-height:auto}.pagination{display:flex;gap:8px;align-items:center;justify-content:flex-end;margin-top:15px}.pagination>span:first-child{margin-right:auto;color:#60768c}.pagination .page-number{width:38px;padding:0}.pagination .page-number.active{background:#1764dd;border-color:#1764dd;color:#fff}.pagination .ellipsis{color:#60768c}.empty{padding:40px;text-align:center;color:#697f94}.map-shell{display:grid;grid-template-columns:185px minmax(0,1fr);gap:16px}.room-sidebar{display:flex;flex-direction:column;gap:8px}.room-sidebar>button{display:flex;justify-content:space-between;text-align:left}.room-sidebar small{margin:0}.map-main{min-width:0}.map-toolbar{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:12px}.legend{display:flex;gap:12px;flex-wrap:wrap;font-size:12px}.legend span{display:flex;align-items:center;gap:5px}i{display:inline-block;width:10px;height:10px;border-radius:2px;flex-shrink:0}.map-viewport{overflow:auto;max-height:70vh;min-height:350px;background:#fff;border:1px solid #d5e1ef;padding:8px}.map-canvas{position:absolute;transform-origin:top left}.map-cell{position:absolute;box-sizing:border-box;overflow:hidden;margin:0;padding:0 2px;border-radius:0;display:flex;align-items:center;justify-content:center;min-height:0;line-height:1.2;user-select:none;border:0}.map-cell.found{outline:3px solid #095fd4;outline-offset:1px;z-index:2}.rack-list{display:flex;flex-wrap:wrap;gap:8px}.issues{margin-top:20px;border-top:1px solid #e1d8c5;padding-top:15px}summary{cursor:pointer;color:#536a82;font-weight:600}.issue-scroll{max-height:320px;overflow:auto;margin-top:12px}.issue-row{display:flex;width:100%;text-align:left;justify-content:flex-start;gap:12px;border:0;border-bottom:1px solid #f1e6d2;background:#fffaf1;border-radius:0}.issue-row svg{margin-left:auto;flex-shrink:0}.issue-row small{margin:0}.scrim{position:fixed;inset:0;background:#152b4666;z-index:140;display:flex;justify-content:flex-end}.modal{background:#fff;display:flex;flex-direction:column;max-height:100%;outline:0;box-shadow:-12px 0 50px #12345820}.modal header{display:flex;justify-content:space-between;align-items:center;padding:20px;border-bottom:1px solid #dce6f1;gap:12px}.modal header p{margin:8px 0 0;color:#627b94}.drawer{width:min(680px,100vw);height:100%}.drawer-body{overflow:auto;padding:20px;flex:1}.section-title{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}.history-record{padding:20px 0;border-bottom:1px solid #dce6f1}.timeline{list-style:none;padding:0;margin:12px 0}.timeline li{position:relative;border-left:2px solid #ccdbec;padding:0 0 20px 18px;margin-left:4px;display:flex;flex-direction:column;gap:7px}.timeline li:before{content:'';position:absolute;left:-5px;top:5px;width:8px;height:8px;background:#427fd1;border-radius:50%}.timeline time{color:#60788f;font-size:12px}.raw-group{display:grid;grid-template-columns:1fr 1fr;gap:12px;border-bottom:1px solid #dce6f1}.raw-group pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}.editor-layer{z-index:160;justify-content:center;align-items:center}.editor{width:min(1050px,96vw);max-height:94vh;border-radius:8px}.editor form{min-height:0;display:flex;flex-direction:column}.editor-body{padding:20px;overflow:auto}.form-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;margin-bottom:24px}.form-grid label,.group-editor label{display:flex;flex-direction:column;gap:6px;font-size:12px;color:#4a6581}.group-editor{display:grid;grid-template-columns:65px minmax(140px,1fr) minmax(160px,1fr) minmax(160px,1fr) 38px;gap:10px;align-items:center;margin:14px 0;padding:12px 0;border-top:1px solid #e0e8f1}.group-editor textarea{resize:vertical;white-space:pre-wrap;min-height:70px;width:100%}.group-editor button{padding:0;width:36px;height:36px}.editor footer{display:flex;align-items:center;justify-content:flex-end;gap:10px;border-top:1px solid #dce6f1;padding:16px 20px;flex-shrink:0}.editor footer span{margin-right:auto;display:flex;gap:8px;align-items:center}.spin{animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}@media(max-width:900px){.heading{flex-wrap:wrap}.heading>.actions{width:100%}.metrics>div{padding:0 10px}.map-shell{grid-template-columns:140px minmax(0,1fr)}.form-grid{grid-template-columns:1fr 1fr}.group-editor{grid-template-columns:1fr 1fr}.group-editor>b{grid-column:1/-1}.group-editor button{justify-self:end}.group-editor label{min-width:0}.issue-row{flex-wrap:wrap}}@media(max-width:640px){.cabinet-page{padding:14px}.heading{gap:10px}h1{font-size:22px}.metrics{grid-template-columns:repeat(2,1fr);gap:18px}.metrics strong{font-size:26px}.metrics>div{border:0}.tabs{flex-wrap:wrap}.tabs>span{width:100%;margin:4px 0}.map-shell{display:flex;flex-direction:column}.room-sidebar{flex-direction:row;overflow:auto;align-items:center}.room-sidebar>.search{min-width:160px}.room-sidebar>button{min-width:110px;flex-direction:column}.map-toolbar .actions{width:100%}.editor{width:100vw;height:100dvh;max-height:100dvh;border-radius:0}.editor form{flex:1}.editor-body{padding:14px;flex:1}.group-editor{grid-template-columns:1fr}.group-editor>b{grid-column:auto}.form-grid{gap:12px}.pagination{gap:7px}.state-actions button{flex:1}.filter-bar>select{max-width:100%}.notice{font-size:13px}.building{min-height:230px}.issue-row{gap:6px}.issue-row>span{white-space:normal}.raw-group{grid-template-columns:1fr}}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
.count-badge{display:inline-grid;place-items:center;min-width:20px;height:20px;padding:0 4px;border-radius:10px;background:#c42d48;color:#fff;font-size:11px;font-weight:700}
.all-export-panel{margin:14px 0 18px;border-block:1px solid #dce6f1;padding:14px 0}.all-export-panel .section-title>span{color:#60768c;font-size:12px}.all-export-items{display:grid;gap:8px}.all-export-items>div{display:grid;grid-template-columns:56px 18px minmax(180px,1fr) auto auto;align-items:center;gap:9px;min-width:0;padding:9px 12px;background:#fff;border-bottom:1px solid #e8eef5}.all-export-items>div>span{grid-column:3;min-width:0}.all-export-items small{grid-column:3/-1;margin:0;color:#ae283e;white-space:normal;overflow-wrap:anywhere}.all-export-items a{display:inline-flex;align-items:center;gap:5px;color:#175ebd;text-decoration:none}
.all-export-items>div{grid-template-columns:56px 18px minmax(180px,1fr) minmax(0,auto)}.all-export-items>div>.export-item-icon{grid-column:2;min-height:18px}.export-item-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap;min-width:0}.export-item-actions a{display:inline-flex;align-items:center;gap:5px}
.sheet-tabs{display:flex;gap:8px;margin:16px 0;flex-wrap:wrap}.sheet-tabs button{border-radius:6px}.sheet-tabs .active{background:#eaf2ff;border-color:#9ebfe8;color:#175dbb}.sheet-tabs small{display:inline;margin:0;padding-left:5px}.source-table-wrap{max-height:66vh;position:relative;border:1px solid #dce6f1}.source-table{table-layout:fixed;min-width:100%;white-space:normal}.source-table th{position:sticky;top:0;z-index:3}.source-table td{white-space:pre-wrap;overflow-wrap:anywhere;vertical-align:top;font-size:12px;line-height:1.65}.source-table .frozen{position:sticky;z-index:2;background:#fff;border-right:1px solid #e3eaf3}.source-table th.frozen{z-index:4;background:#edf3fa}.source-table .row-actions{position:sticky;right:0;background:#fff;border-left:1px solid #e3eaf3;z-index:2;text-align:center}.source-table th.row-actions{z-index:4;background:#edf3fa}.source-table .source-issue td{background:#fffaf0}.source-table .icon-button{margin:0 3px}.group-editor{grid-template-columns:100px minmax(140px,1fr) minmax(160px,1fr) minmax(160px,1fr) 38px}.group-editor input{width:100%}
.group-editor.current-operation{padding:12px;border:1px solid #bcd4f9;border-left:4px solid #1764dd;border-radius:6px;background:#edf5ff}.group-editor.current-operation>b{color:#175dbb}.group-editor.history-operation{padding:12px;border:1px solid #e2e8f0;border-left:4px solid #94a3b8;border-radius:6px;background:#f8fafc}.group-editor.history-operation>b{color:#64748b}
.editor-body{border:0;margin:0;min-width:0}.group-editor{grid-template-columns:85px minmax(120px,1fr) minmax(180px,1.2fr) minmax(180px,1.2fr) 80px 36px}
@media(min-width:901px){.metrics{grid-template-columns:repeat(6,minmax(0,1fr))}}
.modal{min-width:0;min-height:0;overflow:hidden;box-sizing:border-box}.modal>header{flex:none}.modal header>div,.modal h2{min-width:0;overflow-wrap:anywhere}.modal header>button{flex:none}
.editor{width:min(1120px,calc(100vw - 48px));max-height:calc(100dvh - 48px)}.editor form{flex:1;min-width:0;min-height:0;overflow:hidden}.editor-body{flex:1;min-height:0;overflow:auto;overscroll-behavior:contain;scrollbar-gutter:stable}.editor-fields{border:0;margin:0;padding:0;min-width:0}.editor-fields:disabled{opacity:.75}.editor footer{flex-wrap:wrap}.editor footer>span{min-width:0;overflow-wrap:anywhere;flex:1}.editor footer>button{flex:none}
.drawer{max-height:100dvh}.drawer-body{min-height:0;min-width:0;overscroll-behavior:contain}.group-editor>*{min-width:0;max-width:100%}.group-editor b,.history-record strong,.timeline b{overflow-wrap:anywhere}.group-editor input,.group-editor textarea{min-width:0}.history-record .section-title>strong{flex:1;min-width:0}.history-record .section-title>button{flex:none}.raw-group{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.raw-group>*{min-width:0}
.editor .section-title>h3{min-width:0;overflow-wrap:anywhere}.editor .section-title>button{flex:none;white-space:nowrap}.group-editor{align-items:start}.group-editor>b,.group-editor>button{margin-top:22px}.drawer-body{overflow-wrap:anywhere}.editor footer svg{flex-shrink:0}
.event-times,.state-facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin:0}.event-times dt,.state-facts dt{font-size:12px;color:#627b94}.event-times dd,.state-facts dd{margin:5px 0 0;overflow-wrap:anywhere;font-size:13px}.event-times time{color:#203650}.state-facts{padding:0 0 18px}
:deep(.confirm-modal){box-sizing:border-box;max-height:calc(100dvh - 48px);overflow:auto;overscroll-behavior:contain}:deep(.confirm-content){min-width:0;overflow-wrap:anywhere}
@media(max-width:1100px){.group-editor{grid-template-columns:75px minmax(0,1fr) minmax(0,1fr) 36px}.group-editor>b{grid-column:1;grid-row:1 / span 2}.group-editor>button{grid-column:4;grid-row:1 / span 2}.group-editor>label{grid-column:auto}.editor{width:calc(100vw - 32px);max-height:calc(100dvh - 32px)}}
@media(max-width:820px){
  .all-export-items>div{grid-template-columns:48px 18px minmax(0,1fr)}.all-export-items>div>a,.all-export-items>div>button{grid-column:auto}.all-export-items small{grid-column:1/-1}
  .mobile-card-table{max-height:none!important;overflow-x:hidden;background:transparent}
  .mobile-card-table>table,.mobile-card-table>table>tbody{display:block;width:100%!important;min-width:0!important}
  .mobile-card-table colgroup,.mobile-card-table thead{display:none}
  .mobile-card-table tbody{display:grid!important;gap:10px}
  .mobile-card-table tbody tr{display:block;overflow:hidden;border:1px solid #dce6f1;border-radius:8px;background:#fff}
  .mobile-card-table tbody td{position:static!important;display:flex;width:auto;height:auto;min-height:42px;align-items:flex-start;gap:10px;padding:10px 12px;border-right:0;border-bottom:1px solid #e8eef5;background:transparent!important;white-space:normal;overflow-wrap:anywhere;text-align:left}
  .mobile-card-table tbody td:last-child{border-bottom:0}
  .mobile-card-table tbody td::before{content:attr(data-label);flex:0 0 104px;color:#60758b;font-size:12px;font-weight:700}
  .mobile-card-table tbody td>*{min-width:0}
  .mobile-card-table tbody td>span:not(.rack-map-link){flex:1;white-space:normal}
  .mobile-card-table tbody .row-actions{right:auto;flex-wrap:wrap;justify-content:flex-start}
  .mobile-card-table tbody .empty-cell{display:none}
  .source-table-wrap{border:0}
  .filter-bar>*{flex:1 1 100%;width:100%}
  .pagination{flex-wrap:wrap;justify-content:center}
  .pagination>span:first-child{width:100%;margin:0;text-align:center}
}
.event-failure{margin:4px 0;color:#a93242;overflow-wrap:anywhere}
.history-images{display:flex;gap:8px;flex-wrap:wrap}
.history-images button{width:84px;height:66px;min-height:0;padding:0;overflow:hidden}
.history-images img{width:100%;height:100%;object-fit:contain}
.group-editor .group-evidence{grid-column:2/-1;display:flex;align-items:center;gap:12px;flex-wrap:wrap;min-width:0;padding-top:12px;border-top:1px solid #dce6f1}
.group-editor .group-evidence label{display:flex;flex:1;align-items:center;flex-direction:row;gap:10px;min-width:240px;color:#61768b}
.group-editor .group-evidence input{flex:1;min-width:0;width:min(480px,65vw)}
@media(max-width:820px){.group-editor .group-evidence{grid-column:1/-1}.group-editor .group-evidence label{min-width:0;flex-wrap:wrap}.group-editor .group-evidence input{width:100%}}
.evidence-preview{position:fixed;inset:0;z-index:300;display:grid;place-items:center;padding:52px 24px 24px;background:rgba(15,28,43,.86)}
.evidence-preview img{max-width:100%;max-height:100%;object-fit:contain}
.evidence-preview>button{position:absolute;right:20px;top:12px;width:38px;padding:0}
</style>
