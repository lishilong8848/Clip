<template>
  <NConfigProvider :locale="zhCN" :date-locale="dateZhCN">
    <section class="document-library" :style="styles" aria-label="南通文件" data-v2-inline-feedback>
      <VnetBackButton v-if="!activeTable" to="/?entry=tools" :disabled="busy" />
      <header class="document-toolbar" data-v2-navigation>
        <div class="document-heading"><FolderOpen :size="21" aria-hidden="true" /><h1>南通文件</h1></div>
        <div class="document-actions">
          <NButton v-if="canConfigure" :disabled="busy || tablesLoading || !!tablesError" title="接入数据表" aria-label="接入数据表" @click="openSettings(true)"><Plus :size="16" /><span>接入表</span></NButton>
          <NButton v-if="canConfigure && activeTable" :disabled="busy || tablesLoading || !!tablesError" title="数据表配置" aria-label="数据表配置" @click="openSettings(false)"><Settings2 :size="16" /></NButton>
          <NButton v-if="canConfigure" :disabled="busy" title="本地附件缓存" aria-label="本地附件缓存" @click="openStorage"><HardDrive :size="16" /></NButton>
          <NButton :loading="tablesLoading || listLoading" :disabled="busy" title="刷新文件列表" aria-label="刷新文件列表" @click="reload"><RefreshCw :size="16" /></NButton>
          <NButton v-if="activeTable?.capabilities?.create" type="primary" :disabled="busy || !schema || !!tablesError" @click="createRecord"><Plus :size="16" />新建资料</NButton>
        </div>
      </header>
      <NAlert v-if="tablesError" type="error" :bordered="false" role="alert">{{ tablesError }} <NButton text :disabled="busy" @click="loadTables()">重新读取数据表</NButton></NAlert>
      <NAlert v-if="notice" type="success" :bordered="false" role="status">{{ notice }}</NAlert>
      <NAlert v-if="cleanupPending" type="info" :bordered="false" role="status">接入已移除，来源索引后台清理中（{{ cleanupPending }} 项）。</NAlert>
      <NAlert v-if="tables.cleanup_error" type="warning" :bordered="false" role="alert">后台清理尚未完成：{{ tables.cleanup_error }}</NAlert>
      <NAlert v-if="maintenanceIntent && !maintenanceOpen" type="warning" :bordered="false" role="alert">本地操作回执尚未确认。<NButton text :disabled="busy" @click="maintenanceOpen = true">继续原操作</NButton></NAlert>
      <details v-if="activeTable && (operations.length || operationError)" class="document-pending" open><summary>待核验操作<template v-if="operations.length"> · {{ operations.length }}</template></summary>
        <div v-for="operation in operations" :key="operation.operation_id" class="document-operation"><div><strong>{{ operation.kind === 'create' ? '新增资料' : operation.kind === 'attachment' ? '附件上传' : '资料修改' }}</strong><small>{{ operation.actor_name }} · {{ timeText(operation.created_at) }} · {{ operation.reason }}</small></div>
          <NInput v-if="!operation.record_id" v-model:value="operationTargets[operation.operation_id]" placeholder="核对飞书原表后填写记录 ID" :disabled="busy" :input-props="{ 'aria-label': '核验记录 ID' }" />
          <NButton :disabled="busy" :loading="verifying === operation.operation_id" @click="verifyOperation(operation)"><RefreshCw :size="16" />核验原操作</NButton>
        </div><p v-if="operationError" class="document-error" role="alert">{{ operationError }}</p>
      </details>
      <nav v-if="activeTable" class="document-table-context" aria-label="资料表位置">
        <NButton :disabled="busy" @click="selectTable('')"><ChevronLeft :size="16" />全部数据表</NButton>
        <strong>{{ activeTable.name }}</strong>
        <NButton v-if="canRemove(activeTable)" quaternary :disabled="busy" title="移除本地接入" :aria-label="'移除本地接入：' + activeTable.name" @click="confirmRemove(activeTable)"><Trash2 :size="16" /></NButton>
        <a v-if="sourceLink(activeTable)" :href="sourceLink(activeTable)" target="_blank" rel="noopener noreferrer" title="打开飞书原表"><ExternalLink :size="16" />飞书原表</a>
      </nav>
      <section v-else-if="tables.items.length" class="document-sources" aria-label="已接入数据表">
        <div class="document-source-heading"><h2>已接入数据表</h2><span>{{ tables.items.length }} 张</span></div>
        <div class="document-source-list">
          <article v-for="table in tables.items" :key="table.key" class="document-source">
            <div class="document-source-top"><button type="button" class="document-source-entry" :disabled="busy || tablesLoading" :aria-label="'打开数据表：' + table.name" @click="selectTable(table.key)">
              <FolderOpen :size="22" aria-hidden="true" />
              <span><strong>{{ table.name }}</strong><small>{{ tableScopes(table) }}</small><small v-if="table.summary">{{ table.summary.records }} 条缓存记录 · {{ timeText(table.summary.updated_at) }}<template v-if="table.summary.pending_items"> · {{ table.summary.pending_items }} 项待处理</template></small></span>
              <NTag size="small" :bordered="false">{{ table.capabilities?.create ? '可新增' : '可查看' }}</NTag>
              <ChevronRight :size="18" aria-hidden="true" />
            </button><NButton v-if="canRemove(table)" quaternary :disabled="busy || tablesLoading" title="移除本地接入" :aria-label="'移除本地接入：' + table.name" @click="confirmRemove(table)"><Trash2 :size="17" /></NButton></div>
            <a v-if="sourceLink(table)" class="document-source-link" :href="sourceLink(table)" target="_blank" rel="noopener noreferrer" :aria-label="'打开飞书原表：' + table.name"><ExternalLink :size="14" /><span>{{ sourceLink(table) }}</span></a>
            <p v-if="table.summary?.error" class="document-error" role="status">{{ table.summary.error }}</p>
          </article>
        </div>
      </section>
      <form v-if="activeTable" class="document-filters" @submit.prevent="searchRecords">
        <NInput v-model:value="query" clearable :disabled="busy" placeholder="搜索资料" :input-props="{ 'aria-label': '搜索资料', type: 'search' }"><template #prefix><Search :size="16" /></template></NInput>
        <NSelect v-if="scopeOptions.length" :value="scope" :options="[{ value: '', label: '全部授权楼栋' }, ...scopeOptions]" :disabled="busy" aria-label="资料楼栋" @update:value="changeScope" />
        <NButton attr-type="submit" :disabled="busy || listLoading"><Search :size="16" />查询</NButton>
        <NButton :disabled="busy || filters.length >= 20" @click="addFilter"><Plus :size="16" />字段筛选</NButton>
      </form>
      <div v-if="activeTable && filters.length" class="document-field-filters">
        <div v-for="(filter, index) in filters" :key="index" class="document-filter-row">
          <NSelect :value="filter.field" :options="filterFields" :disabled="busy" filterable :aria-label="'筛选字段 ' + (index + 1)" @update:value="value => changeFilterField(index, value)" />
          <NSelect v-model:value="filter.op" :options="filterOperators(filter)" :disabled="busy" :aria-label="'筛选条件 ' + (index + 1)" />
          <template v-if="!['empty', 'not_empty'].includes(filter.op)">
            <NSelect v-if="filterChoices(filter).length" v-model:value="filter.value" :options="filterChoices(filter)" :disabled="busy" filterable clearable :aria-label="'筛选值 ' + (index + 1)" />
            <NInputNumber v-else-if="filterKind(filter) === 'number'" v-model:value="filter.value" :disabled="busy" :aria-label="'筛选值 ' + (index + 1)" />
            <NDatePicker v-else-if="filterKind(filter) === 'date'" v-model:formatted-value="filter.value" type="date" value-format="yyyy-MM-dd" clearable :disabled="busy" :aria-label="'筛选值 ' + (index + 1)" />
            <NInput v-else v-model:value="filter.value" :disabled="busy" clearable :aria-label="'筛选值 ' + (index + 1)" placeholder="筛选值" @keydown.enter="searchRecords" />
          </template>
          <NButton quaternary :disabled="busy" :aria-label="'移除筛选条件 ' + (index + 1)" @click="filters.splice(index, 1)"><X :size="16" /></NButton>
        </div>
        <span v-if="filterError" class="document-error" role="alert">{{ filterError }}</span>
      </div>
      <div v-if="canReindex" class="document-selection" aria-label="资料选择">
        <NButton :disabled="busy || listLoading || !selectableRecords.length" @click="selectPage"><ListChecks :size="16" />选择本页</NButton>
        <NButton :disabled="busy || listLoading || total === undefined || total === 0" @click="selectAll"><ListChecks :size="16" />选择全部匹配记录（跨页）</NButton>
        <NButton :disabled="busy || !hasSelection" @click="clearSelection"><X :size="16" />清空选择</NButton>
        <NButton type="primary" :disabled="busy || listLoading || !hasSelection" @click="confirmReindex()"><RefreshCw :size="16" />重新向量化选中</NButton>
        <NSelect v-model:value="reindexMode" :options="[{ value: 'all', label: '全部重建' }, { value: 'failed', label: '仅重试失败项' }, { value: 'missing', label: '仅补齐缺失项' }]" :disabled="busy" aria-label="向量化处理方式" class="document-reindex-mode" />
        <span class="document-selection-summary" role="status">{{ selectionLabel }}</span>
      </div>
      <NAlert v-if="listPending" type="info" :bordered="false" role="status">来源资料正在后台同步{{ records.length ? '，当前显示已缓存资料。' : '，完成后自动显示列表。' }}</NAlert>
      <span v-if="activeTable && listCache?.updated_at" class="document-muted">本地记录更新于 {{ timeText(listCache.updated_at) }}</span>
      <NAlert v-if="listError" type="error" :bordered="false" role="alert">{{ listError }} <NButton text :disabled="busy || listLoading" @click="retryRecords">{{ syncFailed ? '重试同步并读取' : '重试读取列表' }}</NButton></NAlert>
      <div v-if="tablesLoading && !tables.items.length || listLoading && !records.length" class="document-loading" aria-label="正在读取资料" role="status"><NSkeleton v-for="index in 5" :key="index" height="48px" /></div>
      <NEmpty v-else-if="!tablesLoading && !tablesError && !tables.items.length" description="尚无可访问的数据表" />
      <template v-else-if="schema">
        <div v-if="records.length" class="document-results" :aria-busy="listLoading">
          <div class="document-table-wrap">
            <table class="document-table">
              <thead><tr><th v-if="canReindex" class="document-check" scope="col"><span class="document-sr-only">选择</span></th><th v-for="field in displayFields" :key="field.id" scope="col">{{ field.name }}</th><th scope="col">附件 / 本地向量</th><th scope="col"><span class="document-sr-only">操作</span></th></tr></thead>
              <tbody><tr v-for="record in records" :key="record.record_id">
                <td v-if="canReindex" class="document-check"><NCheckbox :checked="isSelected(record)" :disabled="busy || listLoading || !record.capabilities.reindex" :aria-label="'选择资料：' + title(record)" @update:checked="value => toggleRecord(record, value)" /></td>
                <td v-for="(field, index) in displayFields" :key="field.id">
                  <button v-if="index === 0" class="document-record-title" type="button" :disabled="busy" :aria-label="'查看资料：' + title(record)" @click="openRecord(record.record_id)"><FileText :size="17" /><DocumentLibraryField :field="field" :model-value="record.fields[field.id]" :interactive="false" /></button>
                  <DocumentLibraryField v-else :field="field" :model-value="record.fields[field.id]" />
                </td>
                <td><div class="document-record-meta"><span><Paperclip :size="14" />{{ record.attachments?.length || 0 }}</span><span>{{ cacheLabel(record) }}</span><NTag :type="vectorType(record)" :bordered="false" size="small">{{ indexLabel(record) }}</NTag><span>{{ vectorProgress(record) }}</span></div></td>
                <td><div class="document-row-actions"><NButton v-if="canReindex && record.capabilities.reindex" quaternary :disabled="busy" :aria-label="'重新向量化：' + title(record)" title="重新向量化" @click="confirmReindex(record)"><RefreshCw :size="17" /></NButton><NButton quaternary :disabled="busy" :aria-label="'打开资料：' + title(record)" title="查看详情" @click="openRecord(record.record_id)"><ChevronRight :size="17" /></NButton></div></td>
              </tr></tbody>
            </table>
          </div>
          <div class="document-mobile-list"><article v-for="record in records" :key="record.record_id">
            <div class="document-mobile-heading"><NCheckbox v-if="canReindex" :checked="isSelected(record)" :disabled="busy || listLoading || !record.capabilities.reindex" :aria-label="'选择资料：' + title(record)" @update:checked="value => toggleRecord(record, value)" /><button class="document-mobile-title" type="button" :disabled="busy" @click="openRecord(record.record_id)"><FileText :size="20" /><strong>{{ title(record) }}</strong><ChevronRight :size="17" /></button><NButton v-if="canReindex && record.capabilities.reindex" quaternary :disabled="busy" :aria-label="'重新向量化：' + title(record)" title="重新向量化" @click="confirmReindex(record)"><RefreshCw :size="16" /></NButton></div>
            <dl><template v-for="field in displayFields.slice(1, 4)" :key="field.id"><dt>{{ field.name }}</dt><dd><DocumentLibraryField :field="field" :model-value="record.fields[field.id]" /></dd></template></dl>
            <div class="document-record-meta"><span><Paperclip :size="14" />{{ record.attachments?.length || 0 }} 个附件</span><span>{{ cacheLabel(record) }}</span><NTag :type="vectorType(record)" size="small" :bordered="false">{{ indexLabel(record) }}</NTag><span>{{ vectorProgress(record) }}</span></div>
          </article></div>
        </div>
        <NEmpty v-else-if="listReady && !listLoading && !listError && !listPending" description="没有符合条件的资料" />
        <footer v-if="records.length || total" class="document-pagination"><span>本页 {{ records.length }} 条{{ total === undefined ? '' : ' · 匹配 ' + total + ' 条' }}{{ listLoading ? ' · 正在读取' : '' }}{{ listRefreshing ? ' · 后台更新中' : '' }}</span><nav aria-label="资料分页"><NButton :disabled="busy || listLoading || !hasPrevious" aria-label="首页资料" title="首页" @click="firstPage"><ChevronsLeft :size="16" /></NButton><NButton :disabled="busy || listLoading || !hasPrevious" aria-label="上一页资料" title="上一页" @click="previousPage"><ChevronLeft :size="16" /></NButton><template v-for="(page, index) in pageNumbers" :key="index"><span v-if="page === null" class="document-page-ellipsis">…</span><NButton v-else :type="page === currentPage ? 'primary' : 'default'" :aria-current="page === currentPage ? 'page' : undefined" :aria-label="'第 ' + page + ' 页资料'" :disabled="busy || listLoading" @click="goPage(page)">{{ page }}</NButton></template><NButton :disabled="busy || listLoading || !hasMore" aria-label="下一页资料" title="下一页" @click="nextPage"><ChevronRight :size="16" /></NButton><NButton :disabled="busy || listLoading || !hasMore || total === undefined" aria-label="末页资料" title="末页" @click="lastPage"><ChevronsRight :size="16" /></NButton></nav></footer>
      </template>

      <NDrawer :show="detailOpen" width="min(800px, 100vw)" :mask-closable="!busy" :close-on-esc="!busy" @update:show="value => { if (!value) closeDetail() }">
        <NDrawerContent :title="creating ? '新建资料' : detail ? title(detail) : '资料详情'" :closable="!busy" :style="styles">
          <div class="document-detail" data-v2-inline-feedback>
            <NAlert v-if="detailError" type="error" :bordered="false" role="alert">{{ detailError }} <NButton v-if="!detail && targetId" text :disabled="busy" @click="openRecord(targetId)">重试读取详情</NButton></NAlert>
            <NAlert v-if="pendingWrite || pendingUpload" type="warning" :bordered="false" role="alert">操作回执尚未确认，输入及原操作编号已保留。请核验后按原操作重试。</NAlert>
            <NAlert v-if="conflict" type="warning" :bordered="false" role="alert">资料已被更新，尚未保存的输入已保留。<NButton v-if="!fieldConflicts.length" text :disabled="busy" @click="readLatest">读取最新版本，保留输入</NButton><span v-else>请先选择每个冲突字段需要保留的值。</span></NAlert>
            <section v-if="fieldConflicts.length" class="document-conflicts" aria-label="字段冲突对比"><article v-for="item in fieldConflicts" :key="item.id"><strong>{{ schema?.fields.find(field => field.id === item.id)?.name || item.id }}</strong><div><span>我的修改：{{ valueText(item.mine) }}</span><span>最新值：{{ valueText(item.latest) }}</span></div><NButton :disabled="busy" @click="resolveConflict(item.id, true)">保留我的修改</NButton><NButton :disabled="busy" @click="resolveConflict(item.id, false)">使用最新值</NButton></article></section>
            <NAlert v-if="detail && indexFailed(detail)" type="warning" :bordered="false" role="status">资料记录已保存。{{ indexLabel(detail) }}，请查看下方各项原因。</NAlert>
            <NAlert v-else-if="detail && indexState(detail) === 'unsupported'" type="warning" :bordered="false" role="status">资料已保存，部分附件格式暂不支持正文解析。</NAlert>
            <NAlert v-else-if="notice" type="success" :bordered="false" role="status">{{ notice }}</NAlert>
            <div v-if="detail" class="document-record-meta"><NTag :type="vectorType(detail)" :bordered="false">{{ indexLabel(detail) }}</NTag><span>{{ vectorProgress(detail) }}</span></div>
            <details v-if="indexItems.length" class="document-index-issues"><summary>本地缓存与向量 · {{ indexItems.length }} 项</summary><ul><li v-for="item in indexItems" :key="item.kind + ':' + (item.attachment_id || item.name)"><strong>{{ item.kind === 'record' ? '记录全文' : item.name }}</strong><NTag size="small" :type="itemVectorized(item) ? 'success' : 'default'" :bordered="false">{{ indexItemLabel(item) }}</NTag><span>{{ item.cached === true ? '已缓存' : item.cached === false ? '未缓存' : '缓存状态待核对' }}</span><span v-if="item.error">{{ item.error }}</span></li></ul></details>
            <div v-if="detailLoading" class="document-loading" role="status" aria-label="正在读取详情"><NSkeleton v-for="index in 4" :key="index" height="44px" /></div>
            <template v-else-if="detail || creating">
              <div class="document-record-source"><FolderOpen :size="16" /><span>所属数据表</span><strong>{{ activeTable?.name }}</strong></div>
              <div v-if="detail" class="document-detail-bar"><NTag :bordered="false" size="small" :title="detail.revision">修订 {{ detail.revision.slice(0, 10) }}</NTag><NButton v-if="!editing && detail.capabilities.edit" :disabled="busy || !!pendingUpload" @click="editing = true"><Pencil :size="15" />编辑资料</NButton><NButton v-if="!editing && canReindex && detail.capabilities.reindex" :disabled="busy || !!pendingUpload" title="重新向量化此资料" aria-label="重新向量化此资料" @click="confirmReindex(detail)"><RefreshCw :size="15" />重新向量化</NButton><NButton :disabled="busy || detailLoading || !!pendingWrite || !!pendingUpload || !!fieldConflicts.length" :title="editing ? '重新读取资料，保留输入' : '重新读取资料'" aria-label="重新读取资料" @click="readLatest"><RefreshCw :size="15" /></NButton></div>
              <NTabs v-if="!creating" :value="detailTab" type="line" @update:value="selectDetailTab"><NTab name="fields">资料详情</NTab><NTab v-if="detail" name="versions" :disabled="editing || busy">附件版本</NTab></NTabs>
              <form v-if="detailTab === 'fields'" id="document-record-form" ref="recordForm" class="document-field-grid" :class="{ 'document-read-fields': !editing }" @submit.prevent="saveRecord">
                <label v-for="field in formFields" :key="field.id" class="document-detail-field" :class="{ 'document-wide': ['url', 'files', 'readonly'].includes(fieldKind(field)) }" :data-field-id="field.id"><span>{{ field.name }}<span v-if="editing && requiredField(field) && canEditField(field)" class="document-required"> *</span><LockKeyhole v-if="editing && !canEditField(field)" :size="12" aria-label="只读字段" /></span><DocumentLibraryField :field="field" :model-value="editing ? draft[field.id] : detail?.fields[field.id]" :editing="editing && canEditField(field)" :disabled="busy || !!pendingWrite || conflict" @update:model-value="value => updateField(field.id, value)" /><small v-if="field.read_only_reason" class="document-muted">{{ field.read_only_reason }}</small><small v-if="fieldErrors[field.id]" class="document-error" role="alert">{{ fieldErrors[field.id] }}</small></label>
                <label v-if="editing" class="document-detail-field document-wide"><span>变更原因 *</span><NInput v-model:value="reason" type="textarea" :autosize="{ minRows: 2, maxRows: 4 }" :disabled="busy || !!pendingWrite" :maxlength="500" :input-props="{ 'aria-label': '变更原因' }" /></label>
                <details v-if="deferredFields.length" class="document-system-fields document-wide"><summary>系统与只读字段 · {{ deferredFields.length }}</summary><div class="document-field-grid"><div v-for="field in deferredFields" :key="field.id" class="document-detail-field"><span><LockKeyhole :size="12" />{{ field.name }}</span><DocumentLibraryField v-if="!creating" :field="field" :model-value="detail?.fields[field.id]" /><small class="document-muted">{{ field.read_only_reason || (fieldKind(field) === 'files' ? '保存资料后添加附件' : '只读') }}</small></div></div></details>
              </form>
              <section v-if="detailTab === 'fields' && detail" class="document-attachments" aria-label="当前附件"><h2>当前附件 <span>{{ detail.attachments.length }}</span></h2><div v-for="file in detail.attachments" :key="file.id" class="document-attachment"><img v-if="file.mime_type?.startsWith('image/')" :src="previewUrl(file)" :alt="file.name" width="40" height="40" loading="lazy" /><FileText v-else :size="23" aria-hidden="true" /><div><strong>{{ file.name }}</strong><small>{{ schema?.fields.find(field => field.id === file.field_id)?.name }} · {{ sizeText(file.size) }}{{ file.version ? ' · V' + file.version : '' }}</small></div><NButton quaternary :aria-label="'预览附件：' + file.name" title="本页预览" @click="openPreview(file, detail.attachments)"><Eye :size="17" /></NButton><a :href="fileUrl(file)" target="_blank" rel="noopener noreferrer" :aria-label="'打开附件：' + file.name" title="下载附件"><Download :size="16" /></a></div><p v-if="!detail.attachments.length" class="document-muted">暂无附件</p>
                <div v-if="detail.capabilities.upload && !editing" class="document-upload"><div class="document-upload-controls"><NSelect v-model:value="uploadField" :options="attachmentFields.map(field => ({ value: field.id, label: field.name }))" :disabled="busy || !!pendingUpload" aria-label="上传附件字段" /><NSelect v-model:value="replaceId" :options="[{ value: '', label: '新增附件版本' }, ...detail.attachments.filter(file => file.field_id === uploadField).map(file => ({ value: file.id, label: '替换：' + file.name }))]" :disabled="busy || !!pendingUpload" aria-label="附件替换目标" /></div><label class="document-file-picker"><Upload :size="18" /><span>{{ pickedFile?.name || '选择附件' }}</span><input type="file" :disabled="busy || !!pendingUpload" aria-label="选择资料附件" @change="pickFile" /></label><label class="document-detail-field"><span>附件变更原因 *</span><NInput v-model:value="uploadReason" :disabled="busy || !!pendingUpload" :input-props="{ 'aria-label': '附件变更原因' }" /></label><NProgress v-if="uploading" type="line" :percentage="Math.min(progress, 99)" :show-indicator="true" /><NButton type="primary" :loading="uploading" :disabled="busy || !pickedFile || !uploadField || !uploadReason.trim() || conflict" @click="uploadAttachment"><Upload :size="16" />{{ pendingUpload ? '按原操作重试上传' : '上传附件' }}</NButton></div>
              </section>
              <section v-if="detailTab === 'versions'" class="document-versions" aria-label="附件版本记录">
                <NAlert v-if="versionsError" type="error" :bordered="false" role="alert">{{ versionsError }} <NButton text @click="loadVersions">重试读取版本</NButton></NAlert>
                <NSpin :show="versionsLoading">
                  <ol v-if="versions.length"><li v-for="file in versions" :key="file.id + ':' + file.version">
                    <History :size="17" /><div><strong>{{ file.name }}</strong><small>V{{ file.version || '—' }} · {{ file.created_at || '时间未提供' }}{{ file.created_by ? ' · ' + file.created_by : '' }}</small></div>
                    <NTag v-if="file.current" size="small" :bordered="false">当前</NTag><NButton quaternary :aria-label="'预览历史附件：' + file.name" title="本页预览" @click="openPreview(file, versions)"><Eye :size="17" /></NButton><a :href="fileUrl(file)" target="_blank" rel="noopener noreferrer" :aria-label="'打开历史附件：' + file.name" title="下载历史附件"><Download :size="16" /></a>
                  </li></ol>
                  <NEmpty v-else-if="!versionsLoading && !versionsError && versionsReady" description="暂无附件版本记录" />
                </NSpin>
              </section>
            </template>
          </div>
          <template #footer><div class="document-detail-footer"><NButton :disabled="busy" @click="closeDetail">{{ editing ? '取消' : '关闭' }}</NButton><NButton v-if="editing" type="primary" attr-type="submit" form="document-record-form" :loading="saving" :disabled="busy || conflict || !reason.trim() || !creating && !dirty">{{ pendingWrite ? '按原操作重试保存' : '保存资料' }}</NButton></div></template>
        </NDrawerContent>
      </NDrawer>
      <NModal :show="!!previewFile" preset="card" :title="previewFile?.name" :style="{ ...styles, width: 'min(960px, calc(100vw - 24px))' }" @update:show="value => { if (!value) previewFile = null }"><div v-if="previewFile" class="document-preview">
        <div class="document-preview-actions"><NButton :disabled="previewPosition <= 0" aria-label="上一个附件" @click="stepPreview(-1)"><ChevronLeft :size="18" /></NButton><span>{{ previewPosition + 1 }}/{{ previewFiles.length }} · {{ sizeText(previewFile.size) }}</span><NButton :disabled="previewPosition >= previewFiles.length - 1" aria-label="下一个附件" @click="stepPreview(1)"><ChevronRight :size="18" /></NButton><a :href="fileUrl(previewFile)" target="_blank" rel="noopener noreferrer"><Download :size="17" />下载</a></div>
        <NAlert v-if="previewError" type="error" :bordered="false">{{ previewError }}</NAlert><img v-if="previewKind === 'image'" :src="previewUrl(previewFile)" :alt="previewFile.name" @error="previewError = '图片读取失败，请重新打开或下载核对。'" /><iframe v-else-if="previewKind === 'pdf'" :src="previewUrl(previewFile)" :title="previewFile.name" referrerpolicy="no-referrer" /><NEmpty v-else description="此格式暂不支持本页预览，请下载查看。" />
      </div></NModal>
      <NModal v-model:show="storageOpen" preset="dialog" title="本地附件缓存" :mask-closable="!storageBusy" :close-on-esc="!storageBusy" :closable="!storageBusy"><NAlert v-if="storageError" type="error" :bordered="false">{{ storageError }}</NAlert><dl v-if="storageInfo" class="document-storage"><dt>已缓存</dt><dd>{{ storageInfo.files }} 个 · {{ sizeText(storageInfo.bytes) }}</dd><dt>可安全清理</dt><dd>{{ storageInfo.orphan_files }} 个 · {{ sizeText(storageInfo.orphan_bytes) }}</dd><dt>磁盘可用</dt><dd>{{ sizeText(storageInfo.free_bytes) }}</dd></dl><p class="document-muted">仅清理超过一天、且没有记录、历史版本或待处理任务引用的原件缓存，不删除飞书文件。</p><template #action><NButton :disabled="storageBusy" @click="storageOpen = false">关闭</NButton><NButton type="warning" :loading="storageBusy" :disabled="!storageInfo?.orphan_files || storageBusy" @click="cleanStorage">清理无引用缓存</NButton></template></NModal>
      <DocumentLibraryTableSettings :show="settingsOpen" :initial="settingsNew ? undefined : activeTable" :schema="settingsNew ? undefined : schema || undefined" :scopes="configurationScopes" @close="closeSettings" @saved="configurationSaved" />
      <NModal :show="maintenanceOpen" preset="dialog" class="document-maintenance-dialog" :style="{ ...styles, width: 'min(520px, calc(100vw - 32px))' }" :title="maintenanceIntent?.kind === 'remove' ? '移除本地接入？' : '确认重新向量化？'" :mask-closable="!maintenanceBusy" :close-on-esc="!maintenanceBusy" :closable="!maintenanceBusy" @update:show="value => { if (!value) closeMaintenance() }">
        <p>{{ maintenanceIntent?.label }}</p>
        <p v-if="maintenanceIntent?.kind === 'remove'">仅移除本地接入和该来源索引，不删除飞书记录，不影响手动知识库文件。来源索引可能继续在后台清理。</p>
        <p v-else>将在本机重新生成记录全文和全部附件的向量，可能占用较多 CPU；不修改飞书记录。</p>
        <NAlert v-if="maintenanceError" type="error" :bordered="false" role="alert">{{ maintenanceError }}</NAlert>
        <template #action><NButton :disabled="maintenanceBusy" @click="closeMaintenance">{{ maintenanceUncertain ? '稍后核验' : '取消' }}</NButton><NButton :type="maintenanceIntent?.kind === 'remove' ? 'error' : 'primary'" :loading="maintenanceBusy" :disabled="maintenanceBusy" @click="runMaintenance">{{ maintenanceUncertain ? '按原操作重试' : maintenanceIntent?.kind === 'remove' ? '确认移除本地接入' : '确认重新向量化' }}</NButton></template>
      </NModal>
      <NModal v-model:show="discardOpen" preset="dialog" title="离开未保存的资料？" :content="pendingWrite || pendingUpload ? '操作回执尚未确认，离开后请先核验资料，避免重复新增。' : '未保存的输入将被放弃。'" positive-text="离开" negative-text="继续填写" @positive-click="confirmDiscard" />
    </section>
  </NConfigProvider>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { NAlert, NButton, NCheckbox, NConfigProvider, NDatePicker, NDrawer, NDrawerContent, NEmpty, NInput, NInputNumber, NModal, NProgress, NSelect, NSkeleton, NSpin, NTab, NTabs, NTag, dateZhCN, zhCN, useThemeVars } from 'naive-ui';
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Download, Eye, HardDrive, ExternalLink, FileText, FolderOpen, History, ListChecks, LockKeyhole, Paperclip, Pencil, Plus, RefreshCw, Search, Settings2, Trash2, Upload, X } from 'lucide-vue-next';
import { registerNavigationGuard } from '../navigation';
import { AUTH_EXPIRED_EVENT, ApiError, requestJson } from '../api/client';
import { readCacheKey } from '../api/readCache';
import { cloneLibraryValue as clone, documentLibraryApi as api, fieldKind, fieldValue, newLibraryOperationId, valueText, writableField,
  type LibraryAttachment, type LibraryCache, type LibraryField, type LibraryFields, type LibraryFilter, type LibraryLocalOperation, type LibraryMutation, type LibraryPendingOperation, type LibraryStorage, type LibraryRecord, type LibraryReindex, type LibrarySchema, type LibraryTable, type LibraryTables } from '../api/documentLibrary';
import DocumentLibraryField from './DocumentLibraryField.vue';
import DocumentLibraryTableSettings from './DocumentLibraryTableSettings.vue';
import VnetBackButton from './VnetBackButton.vue';

const props = defineProps<{ scope?: string }>();
const theme = useThemeVars();
const styles = computed(() => ({ '--doc-ink': theme.value.textColor1, '--doc-muted': theme.value.textColor3,
  '--doc-line': theme.value.borderColor, '--doc-surface': theme.value.cardColor, '--doc-soft': theme.value.tableHeaderColor,
  '--doc-accent': theme.value.primaryColor, '--doc-error': theme.value.errorColor }));
const tables = ref<LibraryTables>({ items: [] }), activeKey = ref(''), schema = ref<LibrarySchema | null>(null);
const records = ref<LibraryRecord[]>([]), tablesLoading = ref(false), listLoading = ref(false), listReady = ref(false);
const normalizeScope = (value?: string) => value === 'ALL' ? '' : value || '';
const tablesError = ref(''), listError = ref(''), notice = ref(''), query = ref(''), appliedQuery = ref(''), scope = ref(normalizeScope(props.scope));
const listPending = ref(false), syncFailed = ref(false);
const listRefreshing = ref(false), filterError = ref('');
// Values stay typed in the request; the editor narrows them by the selected field.
const filters = ref<Array<{ field: string; op: string; value: any }>>([]), appliedFilters = ref<LibraryFilter[]>([]);
const total = ref<number>(), listCache = ref<LibraryCache>(), cleanupPending = ref(0);
const pageSize = 30;
const currentCursor = ref(''), cursorTrail = ref<string[]>([]), nextCursor = ref(''), hasMore = ref(false), actorScopes = ref<Array<{ value: string; label: string }>>([]);
const hasPrevious = computed(() => /^\d+$/.test(currentCursor.value) ? Number(currentCursor.value) > 0 : cursorTrail.value.length > 0);
const currentPage = computed(() => Math.floor(Number(currentCursor.value || 0) / pageSize) + 1);
const pageCount = computed(() => total.value === undefined ? 0 : Math.max(1, Math.ceil(total.value / pageSize)));
const pageNumbers = computed(() => {
  if (!pageCount.value || !Number.isFinite(currentPage.value)) return [];
  const start = Math.max(1, Math.min(currentPage.value - 1, pageCount.value - 2));
  const pages = [...new Set([1, start, start + 1, start + 2, pageCount.value])].filter(page => page <= pageCount.value).sort((a, b) => a - b);
  return pages.flatMap((page, index) => index && page - pages[index - 1] > 1 ? [null, page] : [page]);
});
const activeTable = computed(() => tables.value.items.find(table => table.key === activeKey.value));
const canConfigure = computed(() => typeof tables.value.can_configure === 'boolean' ? tables.value.can_configure
  : tables.value.capabilities?.configure_tables === true || tables.value.actor?.can_configure === true);
const canReindex = computed(() => activeTable.value?.capabilities?.reindex === true && !!activeTable.value.revision);
const selectedIds = ref<string[]>([]), allSelected = ref(false);
const selectableRecords = computed(() => records.value.filter(record => record.capabilities.reindex === true));
const hasSelection = computed(() => allSelected.value || selectedIds.value.length > 0);
const filterLabel = computed(() => `搜索：${appliedQuery.value || '不限'}；楼栋：${scopeOptions.value.find(option => option.value === scope.value)?.label || '全部授权楼栋'}`
  + appliedFilters.value.map(filter => `；${schema.value?.fields.find(field => field.id === filter.field)?.name || filter.field} ${filterOperators(filter).find(op => op.value === filter.op)?.label || ''} ${filter.value ?? ''}`).join(''));
const selectionLabel = computed(() => allSelected.value
  ? `已选择全部匹配记录（跨页，${listCache.value?.complete === false ? '当前缓存 ' : ''}${total.value ?? '未知'} 条）；${filterLabel.value}`
  : `已选择 ${selectedIds.value.length} 条；${filterLabel.value}`);
type MaintenanceIntent = { kind: 'reindex'; key: string; label: string; payload: LibraryReindex }
  | { kind: 'remove'; key: string; label: string; payload: LibraryLocalOperation };
const maintenanceIntent = ref<MaintenanceIntent | null>(null), maintenanceOpen = ref(false), maintenanceBusy = ref(false);
const maintenanceError = ref(''), maintenanceUncertain = ref(false);
const reindexMode = ref<'all' | 'failed' | 'missing'>('all');
const operations = ref<LibraryPendingOperation[]>([]), operationTargets = ref<Record<string, string>>({}), operationError = ref(''), verifying = ref('');
const storageOpen = ref(false), storageBusy = ref(false), storageInfo = ref<LibraryStorage | null>(null), storageError = ref('');
const previewFile = ref<LibraryAttachment | null>(null), previewFiles = ref<LibraryAttachment[]>([]), previewError = ref('');
const previewPosition = computed(() => previewFiles.value.findIndex(file => file.id === previewFile.value?.id && file.version === previewFile.value?.version));
const previewKind = computed(() => /\.(png|jpe?g|gif|webp)$/i.test(previewFile.value?.name || '') ? 'image' : /\.pdf$/i.test(previewFile.value?.name || '') ? 'pdf' : 'unsupported');
const fieldConflicts = ref<Array<{ id: string; mine: unknown; latest: unknown }>>([]);
const configurationScopes = computed(() => tables.value.scopes || actorScopes.value);
const scopeOptions = computed(() => configurationScopes.value.filter(scope => !activeTable.value?.read_scopes || activeTable.value.read_scopes.includes(scope.value)));
const displayFields = computed(() => (schema.value?.display_fields || []).map(id => schema.value!.fields.find(field => field.id === id)).filter((field): field is LibraryField => !!field));
const attachmentFields = computed(() => (schema.value?.attachment_fields || []).map(id => schema.value!.fields.find(field => field.id === id)).filter((field): field is LibraryField => !!field));
const detailOpen = ref(false), detail = ref<LibraryRecord | null>(null), targetId = ref(''), detailLoading = ref(false), detailError = ref('');
const editing = ref(false), creating = ref(false), detailTab = ref('fields'), draft = ref<LibraryFields>({}), baseline = ref<LibraryFields>({}), reason = ref('');
const recordForm = ref<HTMLFormElement | null>(null);
const formFields = computed(() => (schema.value?.fields || []).filter(field => canEditField(field)).sort((a, b) =>
  (displayFields.value.findIndex(field => field.id === a.id) + 1 || 1000) - (displayFields.value.findIndex(field => field.id === b.id) + 1 || 1000)));
const deferredFields = computed(() => (schema.value?.fields || []).filter(field => !canEditField(field) && (creating.value || fieldKind(field) !== 'files')));
const fieldErrors = ref<Record<string, string>>({}), conflict = ref(false), saving = ref(false), uploading = ref(false), progress = ref(0);
const pendingWrite = ref<LibraryMutation | null>(null);
type UploadIntent = { file: File; field_id: string; expected_revision: string; operation_id: string; reason: string; replace_attachment_id: string };
const pendingUpload = ref<UploadIntent | null>(null), pickedFile = ref<File | null>(null), uploadField = ref(''), replaceId = ref(''), uploadReason = ref('');
const versions = ref<LibraryAttachment[]>([]), versionsLoading = ref(false), versionsError = ref(''), versionsReady = ref(false);
const settingsOpen = ref(false), settingsNew = ref(false), discardOpen = ref(false);
const busy = computed(() => saving.value || uploading.value || maintenanceBusy.value || !!verifying.value || storageBusy.value);
const dirty = computed(() => JSON.stringify(draft.value) !== JSON.stringify(baseline.value));
let tablesRead: AbortController | undefined, listRead: AbortController | undefined, detailRead: AbortController | undefined, versionRead: AbortController | undefined;
let maintenanceRead: AbortController | undefined, pollTimer: number | undefined, contextEpoch = 0;
let identityKey = currentIdentity();
let leaveAction: (() => void) | undefined, unregisterNavigation: (() => void) | undefined, disposed = false, contextReady = false;
const filterFields = computed(() => (schema.value?.fields || []).map(field => ({ value: field.id, label: field.name })));
function filterKind(filter: LibraryFilter) {
  const field = schema.value?.fields.find(field => field.id === filter.field);
  return field ? [1001, 1002].includes(Number(field.type)) ? 'date' : fieldKind(field) : 'text';
}
function filterOperators(filter: LibraryFilter) {
  const kind = filterKind(filter);
  return [
    ...(!['number', 'date', 'select', 'multi', 'bool', 'person'].includes(kind) ? [{ value: 'contains', label: '包含' }] : []),
    { value: 'eq', label: '等于' },
    ...(['number', 'date'].includes(kind) ? [{ value: 'gte', label: '不小于' }, { value: 'lte', label: '不大于' }] : []),
    { value: 'empty', label: '未填写' }, { value: 'not_empty', label: '已填写' },
  ];
}
function filterChoices(filter: LibraryFilter) {
  if (filterKind(filter) === 'bool') return [{ value: 'true', label: '是' }, { value: 'false', label: '否' }];
  const field = schema.value?.fields.find(field => field.id === filter.field);
  return (field?.options || []).map(option => typeof option === 'string' ? { value: option, label: option }
    : { value: option.name || option.label || option.id || option.value || '', label: option.name || option.label || option.id || option.value || '' }).filter(option => option.value);
}
function addFilter() {
  if (filterFields.value.length && filters.value.length < 20) { filters.value.push({ field: filterFields.value[0].value, op: 'contains', value: '' }); changeFilterField(filters.value.length - 1, filterFields.value[0].value); }
}
function changeFilterField(index: number, field: string) {
  const filter = filters.value[index]; filter.field = field; filter.value = null; filter.op = filterOperators(filter)[0].value; filterError.value = '';
}
function errorText(error: unknown) { return error instanceof Error ? error.message : '请求未完成，请重试。'; }
function withdrawUnauthorized(error: unknown) {
  if (!(error instanceof ApiError) || ![401, 403, 404].includes(error.status)) return false;
  tablesRead?.abort(); tablesLoading.value = false; resetView(); settingsOpen.value = false;
  saving.value = false; uploading.value = false; listError.value = errorText(error);
  return true;
}
function sourceLink(table: LibraryTable) {
  try { const url = new URL(table.url || ''); return ['https:', 'http:'].includes(url.protocol) ? url.href : ''; } catch { return ''; }
}
function tableScopes(table: LibraryTable) {
  return configurationScopes.value.filter(option => (table.read_scopes || table.scopes || []).includes(option.value)).map(option => option.label).join('、') || '已授权资料';
}
function title(record: LibraryRecord) { return displayFields.value.length ? valueText(record.fields[displayFields.value[0].id]) : record.record_id; }
function indexState(record: LibraryRecord) { return typeof record.knowledge_status === 'string' ? record.knowledge_status : record.knowledge_status?.state || ''; }
type IndexItem = NonNullable<LibraryRecord['knowledge_items']>[number];
function indexFailed(record: LibraryRecord) { return ['failed', 'error', 'pending_retry', 'retrying', 'partial', 'blocked', 'unavailable'].includes(indexState(record)); }
function itemVectorized(item: IndexItem) { return item.vectorized === true && item.status === 'ready'; }
function vectorCounts(record: LibraryRecord) {
  const summary = record.knowledge_summary, items = record.knowledge_items;
  const total = Math.max(1 + record.attachments.length, items?.length || 0, summary?.total || 0);
  const ready = items?.length ? items.filter(itemVectorized).length : summary?.ready || 0;
  return { total, ready: Math.min(ready, total) };
}
function vectorized(record: LibraryRecord) {
  const summary = record.knowledge_summary, items = record.knowledge_items, counts = vectorCounts(record);
  if (!['ready', 'indexed', 'succeeded'].includes(indexState(record)) || counts.ready !== counts.total) return false;
  if (summary && (summary.vectorized !== true || summary.ready !== summary.total || summary.total !== counts.total)) return false;
  return items?.length ? items.every(itemVectorized) && items.some(item => item.kind === 'record')
    && record.attachments.every(file => items.some(item => item.kind === 'attachment' && item.attachment_id === file.id)) : summary?.vectorized === true;
}
function vectorProgress(record: LibraryRecord) { const counts = vectorCounts(record); return `已完成 ${counts.ready}/${counts.total} 项`; }
function vectorType(record: LibraryRecord): 'success' | 'warning' | 'default' { return vectorized(record) ? 'success' : indexFailed(record) || ['unsupported', 'text_only'].includes(indexState(record)) ? 'warning' : 'default'; }
function indexLabel(record: LibraryRecord) {
  if (vectorized(record)) return '向量化成功';
  const state = indexState(record);
  if (state === 'blocked') return '内容已拦截';
  if (state === 'unavailable') return '索引状态待核对';
  if (indexFailed(record)) return record.knowledge_items?.some(item => item.status === 'retrying' && item.stage === 'prepare') ? '原件缓存待重试' : '待索引重试';
  return state === 'caching' ? '正在缓存原件' : state === 'unsupported' ? '部分格式不支持向量化'
    : state === 'text_only' || record.knowledge_items?.some(item => item.status === 'text_only') ? '仅全文索引，未向量化'
    : ['indexed', 'ready', 'succeeded'].includes(state) ? '向量状态待核对' : state === 'indexing' ? '正在向量化' : state === 'queued' ? '向量化排队中' : '等待向量化';
}
function cacheLabel(record: LibraryRecord) {
  const items = record.knowledge_items || [];
  const files = items.filter(item => item.kind !== 'record');
  if (!items.length || items.some(item => item.cached === undefined)) return '缓存状态待核对';
  return files.length ? '原件已缓存 ' + files.filter(item => item.cached).length + '/' + files.length : '字段已缓存';
}
function indexItemLabel(item: IndexItem) {
  if (itemVectorized(item)) return '向量化成功';
  if (item.status === 'retrying') return item.stage === 'prepare' ? '下载或保存待重试' : '提交索引待重试';
  return ({ ready: '向量状态待核对', text_only: '仅全文索引，未向量化', blocked: '已阻止索引', failed: '解析或向量化失败', unsupported: '格式不支持，未向量化', unavailable: '向量状态待核对', caching: '本地准备中', indexing: '正在向量化', queued: '向量化排队中', partial: '部分向量化失败' } as Record<string, string>)[item.status] || '等待向量化';
}
const indexItems = computed(() => detail.value?.knowledge_items || []);
function sizeText(size?: number) { return size === undefined ? '大小未提供' : size < 1024 ? size + ' B' : size < 1048576 ? (size / 1024).toFixed(1) + ' KB' : (size / 1048576).toFixed(1) + ' MB'; }
function fileUrl(file: LibraryAttachment) { return api.fileUrl(activeKey.value, detail.value!.record_id, file); }
function previewUrl(file: LibraryAttachment) { const url = new URL(fileUrl(file), window.location.origin); url.searchParams.set('preview', '1'); return url.pathname + url.search; }
function timeText(value: number) { return value ? new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', dateStyle: 'short', timeStyle: 'short' }).format(value * 1000) : '尚未同步'; }
function openPreview(file: LibraryAttachment, files: LibraryAttachment[]) { previewFiles.value = files; previewFile.value = file; previewError.value = ''; }
function stepPreview(direction: number) { const file = previewFiles.value[previewPosition.value + direction]; if (file) { previewFile.value = file; previewError.value = ''; } }
async function openStorage() {
  storageOpen.value = true; storageBusy.value = true; storageError.value = '';
  const identity = identityKey;
  try { const value = await api.storage(); if (!disposed && identity === identityKey) storageInfo.value = value; }
  catch (error) { if (!disposed && identity === identityKey) storageError.value = errorText(error); }
  finally { if (identity === identityKey) storageBusy.value = false; }
}
async function cleanStorage() {
  if (!storageInfo.value || storageBusy.value) return;
  storageBusy.value = true; storageError.value = '';
  const identity = identityKey;
  try { const value = await api.storage(storageInfo.value.revision); if (!disposed && identity === identityKey) storageInfo.value = value; }
  catch (error) { if (!disposed && identity === identityKey) storageError.value = errorText(error); }
  finally { if (identity === identityKey) storageBusy.value = false; }
}
async function loadOperations() {
  const key = activeKey.value, epoch = contextEpoch;
  if (!key) return;
  try { const value = await api.operations(key); if (!disposed && key === activeKey.value && epoch === contextEpoch) { operations.value = value; operationError.value = ''; } }
  catch (error) { if (!disposed && epoch === contextEpoch) { if (error instanceof ApiError && [401, 403].includes(error.status)) operations.value = []; operationError.value = errorText(error); } }
}
async function verifyOperation(operation: LibraryPendingOperation) {
  if (busy.value) return;
  verifying.value = operation.operation_id; operationError.value = '';
  const epoch = contextEpoch, key = activeKey.value;
  try {
    const result = await api.verify(key, operation.operation_id, operationTargets.value[operation.operation_id]);
    if (disposed || epoch !== contextEpoch) return;
    if (result.status === 'completed') {
      notice.value = '原操作已核验成功，没有重复提交。';
      if (detail.value && result.record?.record_id === detail.value.record_id) saved(result.record);
      await loadOperations(); await readRecords(currentCursor.value, cursorTrail.value);
    } else operationError.value = result.message || '原操作仍待核验，本次未重复提交。';
  } catch (error) { if (!disposed && epoch === contextEpoch) operationError.value = errorText(error); }
  finally { if (epoch === contextEpoch) verifying.value = ''; }
}
function resolveConflict(id: string, mine: boolean) {
  const item = fieldConflicts.value.find(field => field.id === id);
  if (item) draft.value[id] = mine ? item.mine : item.latest;
  fieldConflicts.value = fieldConflicts.value.filter(field => field.id !== id);
  conflict.value = fieldConflicts.value.length > 0;
}
function hasUnsaved() { return settingsOpen.value || detailOpen.value && (editing.value && (dirty.value || !!reason.value.trim()) || !!pickedFile.value || !!pendingWrite.value || !!pendingUpload.value); }
function attemptLeave(action: () => void): boolean {
  if (busy.value) return false;
  if (hasUnsaved()) { leaveAction = action; discardOpen.value = true; return false; }
  action(); return true;
}
function confirmDiscard() { const action = leaveAction; leaveAction = undefined; discardOpen.value = false; action?.(); }
function beforeUnload(event: BeforeUnloadEvent) { if (busy.value || hasUnsaved()) { event.preventDefault(); event.returnValue = ''; } }
function clearSelection() { selectedIds.value = []; allSelected.value = false; }
function isSelected(record: LibraryRecord) { return record.capabilities.reindex === true && (allSelected.value || selectedIds.value.includes(record.record_id)); }
function selectPage() { allSelected.value = false; selectedIds.value = [...new Set([...selectedIds.value, ...selectableRecords.value.map(record => record.record_id)])]; }
function selectAll() { clearSelection(); allSelected.value = true; }
function toggleRecord(record: LibraryRecord, checked: boolean) {
  if (busy.value || !canReindex.value || !record.capabilities.reindex) return;
  if (allSelected.value) { allSelected.value = false; selectedIds.value = selectableRecords.value.map(record => record.record_id); }
  selectedIds.value = checked ? [...new Set([...selectedIds.value, record.record_id])] : selectedIds.value.filter(id => id !== record.record_id);
}
function resetMaintenance() {
  maintenanceRead?.abort(); maintenanceIntent.value = null; maintenanceOpen.value = false;
  maintenanceBusy.value = false; maintenanceError.value = ''; maintenanceUncertain.value = false;
}
function closeMaintenance() {
  if (maintenanceBusy.value) return;
  maintenanceOpen.value = false;
  if (!maintenanceUncertain.value) resetMaintenance();
}
function canRemove(table: LibraryTable) {
  // The new capability's presence is the compatibility signal for maintenance endpoints.
  return canConfigure.value && typeof table.capabilities?.reindex === 'boolean' && !!table.revision;
}
function confirmRemove(table: LibraryTable) {
  if (busy.value || !canRemove(table)) return;
  if (maintenanceIntent.value) { maintenanceOpen.value = true; return; }
  attemptLeave(() => {
    maintenanceIntent.value = { kind: 'remove', key: table.key, label: `数据表：${table.name}`, payload: { operation_id: newLibraryOperationId(), expected_revision: table.revision! } };
    maintenanceOpen.value = true;
  });
}
function confirmReindex(record?: LibraryRecord) {
  if (busy.value || !canReindex.value || (record ? !record.capabilities.reindex : !hasSelection.value)) return;
  if (maintenanceIntent.value) { maintenanceOpen.value = true; return; }
  const operation = { operation_id: newLibraryOperationId(), expected_revision: activeTable.value!.revision! };
  const payload: LibraryReindex = record ? { ...operation, record_ids: [record.record_id] }
    : allSelected.value ? { ...operation, all: true, q: appliedQuery.value, scope: scope.value, filters: clone(appliedFilters.value) }
    : { ...operation, record_ids: selectedIds.value.slice() };
  payload.mode = reindexMode.value;
  maintenanceIntent.value = { kind: 'reindex', key: activeKey.value, payload,
    label: `数据表：${activeTable.value!.name}；${record ? '单条资料：' + title(record) : selectionLabel.value}；${{ all: '全部重建', failed: '仅重试失败项', missing: '仅补齐缺失项' }[reindexMode.value]}` };
  maintenanceOpen.value = true;
}
async function runMaintenance() {
  const intent = maintenanceIntent.value;
  if (!intent || maintenanceBusy.value) return;
  const table = tables.value.items.find(table => table.key === intent.key);
  if (!table || (intent.kind === 'remove' ? !canRemove(table) : table.capabilities?.reindex !== true)) {
    maintenanceError.value = '操作权限已变化，请刷新后核验。'; return;
  }
  const read = maintenanceRead = new AbortController(); maintenanceBusy.value = true; maintenanceError.value = '';
  try {
    if (intent.kind === 'reindex') {
      const receipt = await api.reindex(intent.key, intent.payload, read.signal);
      if (disposed || read.signal.aborted || maintenanceIntent.value !== intent) return;
      notice.value = `已加入本地向量化队列：${receipt.records} 条记录，${receipt.items} 项。`;
      resetMaintenance(); clearSelection();
      await readRecords(currentCursor.value, cursorTrail.value);
    } else {
      const receipt = await api.remove(intent.key, intent.payload, read.signal);
      if (disposed || read.signal.aborted || maintenanceIntent.value !== intent) return;
      resetMaintenance(); clearSelection();
      if (receipt.cleanup_status === 'pending') cleanupPending.value = Math.max(1, cleanupPending.value);
      tables.value.items = tables.value.items.filter(table => table.key !== intent.key);
      if (activeKey.value === intent.key) { activeKey.value = ''; resetView(); }
      notice.value = receipt.cleanup_status === 'completed' ? '本地接入及该来源索引已移除。' : '';
      await loadTables();
    }
  } catch (error) {
    if (disposed || read.signal.aborted || maintenanceIntent.value !== intent) return;
    maintenanceUncertain.value = uncertain(error);
    maintenanceError.value = errorText(error) + (maintenanceUncertain.value ? ' 回执未确认，重试将复用原操作编号。' : ' 请取消并刷新后核验。');
  } finally { if (maintenanceRead === read) maintenanceBusy.value = false; }
}
function resetView(clearSchema = true) {
  operations.value = []; operationTargets.value = {}; operationError.value = ''; previewFile.value = null;
  contextEpoch++; listRead?.abort(); detailRead?.abort(); versionRead?.abort();
  clearSelection(); resetMaintenance(); resetDetail(); detailOpen.value = false; detail.value = null; targetId.value = '';
  draft.value = {}; baseline.value = {}; discardOpen.value = false; leaveAction = undefined;
  detailLoading.value = false; versionsLoading.value = false;
  records.value = []; if (clearSchema) schema.value = null;
  if (clearSchema) { filters.value = []; appliedFilters.value = []; filterError.value = ''; }
  listRefreshing.value = false;
  listReady.value = false; listLoading.value = false; listError.value = ''; listPending.value = false; syncFailed.value = false;
  total.value = undefined; listCache.value = undefined; currentCursor.value = ''; cursorTrail.value = []; nextCursor.value = ''; hasMore.value = false;
}
async function loadTables(preferred?: string, refreshSchema = false, silent = false) {
  tablesRead?.abort(); const read = tablesRead = new AbortController(); if (!silent) tablesLoading.value = true;
  try {
    const result = await api.tables(read.signal);
    if (!contextReady && !result.scopes) {
      const context = await api.context(read.signal);
      if (read.signal.aborted || disposed) return;
      actorScopes.value = context.scopes; contextReady = true;
    }
    if (read.signal.aborted || disposed) return;
    const nextKey = result.items.some(table => table.key === (preferred || activeKey.value)) ? preferred || activeKey.value : '';
    tablesError.value = '';
    const schemaChanged = !!activeTable.value && result.items.find(table => table.key === nextKey)?.revision !== activeTable.value.revision;
    if (nextKey !== activeKey.value) resetView();
    tables.value = result; activeKey.value = nextKey;
    if (typeof result.cleanup_pending === 'number') cleanupPending.value = result.cleanup_pending;
    if (!canReindex.value) { clearSelection(); if (maintenanceIntent.value?.kind === 'reindex') resetMaintenance(); }
    if (silent && (busy.value || hasUnsaved())) return;
    if (schemaChanged) { clearSelection(); filters.value = []; appliedFilters.value = []; }
    await loadTable(false, refreshSchema || schemaChanged, silent);
  } catch (error) { if (!read.signal.aborted && !disposed) { if (withdrawUnauthorized(error)) { tables.value = { items: [] }; activeKey.value = ''; } tablesError.value = errorText(error); } }
  finally { if (tablesRead === read) tablesLoading.value = false; }
}
async function loadTable(reset = false, refreshSchema = false, silent = false) {
  if (reset) resetView();
  if (!activeKey.value) return;
  void loadOperations();
  if (silent && schema.value && !refreshSchema) { await readRecords(currentCursor.value, cursorTrail.value, true); return; }
  listRead?.abort();
  const read = listRead = new AbortController(); const key = activeKey.value; listLoading.value = true;
  try {
    const result = await api.schema(key, read.signal, refreshSchema);
    if (read.signal.aborted || disposed) return;
    schema.value = result;
    if (result.cache) listPending.value = !result.cache.complete || result.cache.refreshing;
    await readRecords(currentCursor.value, cursorTrail.value, silent);
  } catch (error) { if (!read.signal.aborted && !disposed && !withdrawUnauthorized(error)) listError.value = errorText(error); }
  finally { if (listRead === read) listLoading.value = false; }
}
async function readRecords(cursor = '', trail: string[] = [], silent = false) {
  if (!schema.value) { await loadTable(); return; }
  listRead?.abort(); const read = listRead = new AbortController(); listLoading.value = !silent; listRefreshing.value = silent;
  if (!silent) { listError.value = ''; syncFailed.value = false; }
  const key = activeKey.value;
  try {
    const result = await api.records(key, { scope: scope.value, q: appliedQuery.value, filters: clone(appliedFilters.value), cursor, page_size: pageSize }, read.signal);
    if (read.signal.aborted || disposed || key !== activeKey.value) return;
    if (silent && (busy.value || hasUnsaved())) return;
    if (JSON.stringify(records.value) !== JSON.stringify(result.items)) records.value = result.items;
    hasMore.value = result.has_more; nextCursor.value = result.cursor;
    total.value = result.total; listCache.value = result.cache;
    listPending.value = !!result.cache && (!result.cache.complete || result.cache.refreshing);
    listError.value = result.cache?.error || '';
    syncFailed.value = !!result.cache?.error;
    currentCursor.value = cursor; cursorTrail.value = trail.slice(); listReady.value = true;
    const denied = new Set(result.items.filter(record => record.capabilities.reindex !== true).map(record => record.record_id));
    selectedIds.value = selectedIds.value.filter(id => !denied.has(id));
  } catch (error) {
    if (!read.signal.aborted && !disposed) {
      if (withdrawUnauthorized(error)) return;
      if (error instanceof ApiError && error.payload.code === 'document_library_sync_pending') listPending.value = true;
      else { listError.value = errorText(error); syncFailed.value = error instanceof ApiError && error.payload.code === 'document_library_sync_failed'; }
    }
  } finally { if (listRead === read) { listLoading.value = false; listRefreshing.value = false; } }
}
async function retryRecords() {
  if (busy.value || listLoading.value) return;
  if (!syncFailed.value) { await readRecords(currentCursor.value, cursorTrail.value); return; }
  listRead?.abort(); const read = listRead = new AbortController(); const key = activeKey.value; listLoading.value = true;
  try {
    await requestJson('/api/document-library/tables/' + encodeURIComponent(key) + '/sync', { method: 'POST', body: '{}', signal: read.signal });
    if (!read.signal.aborted && !disposed && key === activeKey.value) await readRecords(currentCursor.value, cursorTrail.value);
  } catch (error) { if (!read.signal.aborted && !disposed && !withdrawUnauthorized(error)) listError.value = errorText(error); }
  finally { if (listRead === read) listLoading.value = false; }
}
function reload() { if (!busy.value && !tablesLoading.value && !listLoading.value) void loadTables(undefined, true); }
function selectTable(key: string) {
  if (key === activeKey.value || key && !tables.value.items.some(table => table.key === key)) return;
  attemptLeave(() => {
    tablesRead?.abort(); tablesLoading.value = false;
    activeKey.value = key; appliedQuery.value = ''; query.value = ''; notice.value = '';
    if (!scopeOptions.value.some(option => option.value === scope.value)) scope.value = '';
    void loadTable(true);
  });
}
function searchRecords() {
  if (busy.value || listLoading.value) return;
  if (filters.value.some(filter => !['empty', 'not_empty'].includes(filter.op) && (filter.value === null || filter.value === ''))) {
    filterError.value = '请补齐筛选值，或移除不用的条件。'; return;
  }
  attemptLeave(() => { appliedQuery.value = query.value.trim(); appliedFilters.value = clone(filters.value); filterError.value = ''; resetView(false); void readRecords('', []); });
}
function changeScope(value: string) { if (!busy.value) attemptLeave(() => { scope.value = normalizeScope(value); resetView(false); void readRecords('', []); }); }
function nextPage() { if (!busy.value && !listLoading.value && hasMore.value && nextCursor.value) void readRecords(nextCursor.value, /^\d+$/.test(nextCursor.value) ? [] : [...cursorTrail.value, currentCursor.value]); }
function previousPage() {
  if (busy.value || listLoading.value || !hasPrevious.value) return;
  if (/^\d+$/.test(currentCursor.value)) { const offset = Math.max(0, Number(currentCursor.value) - pageSize); void readRecords(offset ? String(offset) : '', []); }
  else { const trail = cursorTrail.value.slice(); const cursor = trail.pop(); if (cursor !== undefined) void readRecords(cursor, trail); }
}
function firstPage() { if (!busy.value && !listLoading.value && hasPrevious.value) void readRecords('', []); }
function lastPage() {
  if (!busy.value && !listLoading.value && hasMore.value && total.value !== undefined)
    void readRecords(String(Math.max(0, Math.floor((total.value - 1) / pageSize) * pageSize)), []);
}
function goPage(page: number) {
  if (!busy.value && !listLoading.value && page >= 1 && page <= pageCount.value && page !== currentPage.value)
    void readRecords(page === 1 ? '' : String((page - 1) * pageSize), []);
}
function resetDetail() {
  previewFile.value = null; fieldConflicts.value = [];
  detailError.value = ''; notice.value = ''; reason.value = ''; editing.value = false; creating.value = false; fieldErrors.value = {}; conflict.value = false;
  pendingWrite.value = null; pendingUpload.value = null; pickedFile.value = null; replaceId.value = ''; uploadReason.value = '';
  detailTab.value = 'fields'; versions.value = []; versionsError.value = ''; versionsReady.value = false;
}
function applyDetail(record: LibraryRecord) {
  detail.value = record; draft.value = clone(record.fields); baseline.value = clone(record.fields);
  if (!attachmentFields.value.some(field => field.id === uploadField.value)) uploadField.value = attachmentFields.value[0]?.id || '';
}
function openRecord(id: string) {
  attemptLeave(() => {
    detailRead?.abort(); versionRead?.abort(); resetDetail(); detail.value = null; targetId.value = id; detailOpen.value = true;
    const read = detailRead = new AbortController(); detailLoading.value = true;
    void api.record(activeKey.value, id, read.signal).then(record => { if (!read.signal.aborted && !disposed) applyDetail(record); })
      .catch(error => { if (!read.signal.aborted && !disposed && !withdrawUnauthorized(error)) detailError.value = errorText(error); })
      .finally(() => { if (detailRead === read) detailLoading.value = false; });
  });
}
function createRecord() {
  if (!schema.value || !activeTable.value?.capabilities?.create || tablesError.value) return;
  attemptLeave(() => {
  detailRead?.abort(); versionRead?.abort(); resetDetail(); detail.value = null; targetId.value = ''; draft.value = {}; baseline.value = {};
  const allowed = (activeTable.value?.write_scopes || []).filter(scope => configurationScopes.value.some(option => option.value === scope));
  const field = schema.value?.fields.find(field => field.id === activeTable.value?.scope_field);
  if (field && writableField(field) && allowed.length === 1) draft.value[field.id] = fieldKind(field) === 'multi' ? allowed : allowed[0];
  detailLoading.value = false; creating.value = true; editing.value = true; detailOpen.value = true;
  void nextTick(() => recordForm.value?.querySelector<HTMLElement>('input, textarea, [tabindex="0"]')?.focus());
}); }
function closeDetail() { attemptLeave(() => { detailRead?.abort(); versionRead?.abort(); detailOpen.value = false; }); }
function canEditField(field: LibraryField) { return writableField(field); }
function requiredField(field: LibraryField) { return field.required || creating.value && field.id === activeTable.value?.scope_field; }
function updateField(id: string, value: unknown) { draft.value[id] = value; delete fieldErrors.value[id]; }
function changes(): LibraryFields {
  return Object.fromEntries((schema.value?.fields || []).filter(field => canEditField(field) && JSON.stringify(draft.value[field.id]) !== JSON.stringify(baseline.value[field.id])).map(field => [field.id, draft.value[field.id]]));
}
function validate() {
  const errors: Record<string, string> = {};
  for (const field of schema.value?.fields || []) {
    if (!canEditField(field)) continue;
    const value = fieldValue(draft.value[field.id]);
    if (requiredField(field) && (value === undefined || value === null || value === '' || Array.isArray(value) && !value.length)) errors[field.id] = '请填写' + field.name;
    if (fieldKind(field) === 'number' && value != null && value !== '' && !Number.isFinite(Number(value))) errors[field.id] = '请输入有效数字';
    if (fieldKind(field) === 'url' && value) { try { if (!['http:', 'https:'].includes(new URL(typeof value === 'string' ? value : value.link).protocol)) errors[field.id] = '请输入有效的 HTTP 或 HTTPS 链接'; } catch { errors[field.id] = '请输入有效链接'; } }
  }
  fieldErrors.value = errors;
  if (Object.keys(errors).length) void nextTick(() => {
    const field = Array.from(recordForm.value?.querySelectorAll<HTMLElement>('[data-field-id]') || []).find(element => errors[element.dataset.fieldId || '']);
    field?.scrollIntoView({ block: 'center' }); field?.querySelector<HTMLElement>('input, textarea, [tabindex="0"]')?.focus({ preventScroll: true });
  });
  return !Object.keys(errors).length;
}
function uncertain(error: unknown) { return !(error instanceof ApiError) || !error.status || error.status >= 500; }
function saved(record: LibraryRecord) {
  applyDetail(record); targetId.value = record.record_id; creating.value = false; editing.value = false; pendingWrite.value = null; pendingUpload.value = null; conflict.value = false;
  const index = records.value.findIndex(item => item.record_id === record.record_id); if (index >= 0) records.value[index] = record;
  notice.value = indexFailed(record) ? '资料保存成功，知识索引待重试。' : '资料保存成功。';
  void loadOperations();
  void readRecords(currentCursor.value, cursorTrail.value);
}
async function saveRecord() {
  if (busy.value || conflict.value || !reason.value.trim() || !validate()) return;
  if (!pendingWrite.value && !Object.keys(changes()).length) { detailError.value = '请先填写或修改资料字段。'; return; }
  if (!pendingWrite.value) pendingWrite.value = { fields: clone(changes()), expected_revision: detail.value?.revision, reason: reason.value.trim(), operation_id: newLibraryOperationId() };
  saving.value = true; detailError.value = ''; notice.value = '';
  const epoch = contextEpoch;
  try { const record = await api.save(activeKey.value, creating.value ? undefined : detail.value!.record_id, clone(pendingWrite.value)); if (!disposed && epoch === contextEpoch) saved(record); }
  catch (error) { if (!disposed && epoch === contextEpoch) { detailError.value = errorText(error); if (!uncertain(error)) pendingWrite.value = null; else void loadOperations(); if (error instanceof ApiError && error.status === 409) conflict.value = true; } }
  finally { if (epoch === contextEpoch) saving.value = false; }
}
async function readLatest() {
  if (!detail.value || busy.value || pendingWrite.value || pendingUpload.value || fieldConflicts.value.length) return;
  detailRead?.abort(); const read = detailRead = new AbortController(); detailLoading.value = true; detailError.value = '';
  const unsaved = clone(changes());
  const old = clone(baseline.value);
  try {
    const record = await api.record(activeKey.value, detail.value.record_id, read.signal, true);
    if (read.signal.aborted || disposed) return;
    applyDetail(record);
    fieldConflicts.value = [];
    if (editing.value) for (const [id, mine] of Object.entries(unsaved)) {
      if (JSON.stringify(old[id]) !== JSON.stringify(record.fields[id]) && JSON.stringify(mine) !== JSON.stringify(record.fields[id]))
        fieldConflicts.value.push({ id, mine, latest: record.fields[id] === undefined ? undefined : clone(record.fields[id]) });
      else draft.value[id] = mine;
    }
    conflict.value = fieldConflicts.value.length > 0;
  } catch (error) { if (!read.signal.aborted && !disposed && !withdrawUnauthorized(error)) detailError.value = errorText(error); }
  finally { if (detailRead === read) detailLoading.value = false; }
}
function pickFile(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  const limit = detail.value?.capabilities.max_upload_bytes;
  if (file && limit && file.size > limit) { detailError.value = '附件超过此表允许的文件大小：' + sizeText(limit); pickedFile.value = null; return; }
  pickedFile.value = file;
}
async function uploadAttachment() {
  if (busy.value || !detail.value || !pickedFile.value || !uploadField.value || !uploadReason.value.trim() || conflict.value) return;
  if (!pendingUpload.value) pendingUpload.value = { file: pickedFile.value, field_id: uploadField.value, expected_revision: detail.value.revision,
    reason: uploadReason.value.trim(), operation_id: newLibraryOperationId(), replace_attachment_id: replaceId.value };
  const intent = pendingUpload.value; const form = new FormData(); form.append('file', intent.file);
  for (const key of ['field_id', 'expected_revision', 'operation_id', 'reason', 'replace_attachment_id'] as const) if (intent[key]) form.append(key, intent[key]);
  uploading.value = true; detailError.value = ''; progress.value = 0; notice.value = '';
  const epoch = contextEpoch;
  try { const record = await api.upload(activeKey.value, detail.value.record_id, form, percent => { if (epoch === contextEpoch) progress.value = percent; }); if (!disposed && epoch === contextEpoch) { saved(record); pickedFile.value = null; uploadReason.value = ''; versionsReady.value = false; } }
  catch (error) { if (!disposed && epoch === contextEpoch) { detailError.value = errorText(error); if (!uncertain(error)) pendingUpload.value = null; else void loadOperations(); if (error instanceof ApiError && error.status === 409) conflict.value = true; } }
  finally { if (epoch === contextEpoch) uploading.value = false; }
}
function selectDetailTab(value: string) { detailTab.value = value; if (value === 'versions' && !versionsReady.value) void loadVersions(); }
async function loadVersions() {
  if (!detail.value) return;
  versionRead?.abort(); const read = versionRead = new AbortController(); versionsLoading.value = true; versionsError.value = '';
  try { const result = await api.versions(activeKey.value, detail.value.record_id, read.signal); if (!read.signal.aborted && !disposed) { versions.value = result; versionsReady.value = true; } }
  catch (error) { if (!read.signal.aborted && !disposed && !withdrawUnauthorized(error)) versionsError.value = errorText(error); }
  finally { if (versionRead === read) versionsLoading.value = false; }
}
function openSettings(isNew: boolean) { attemptLeave(() => { detailOpen.value = false; settingsNew.value = isNew; settingsOpen.value = true; }); }
function closeSettings(verify = false) { settingsOpen.value = false; if (verify) void loadTables(); }
async function configurationSaved(table: LibraryTable) {
  if (!settingsOpen.value || disposed) return;
  const message = settingsNew.value ? '数据表已接入，请核对显示字段及编辑范围。' : '数据表配置已保存。';
  settingsOpen.value = false;
  await loadTables(table.key);
  if (!disposed && activeKey.value === table.key) notice.value = message;
}
function currentIdentity() { return readCacheKey('/api/scope-overview', window.location.origin); }
function identityChanged() {
  const next = currentIdentity();
  if (next === identityKey) return;
  identityKey = next; clearIdentity();
  if (next) void loadTables();
}
function clearIdentity() {
  storageOpen.value = false; storageInfo.value = null; previewFile.value = null;
  verifying.value = ''; storageBusy.value = false;
  tablesRead?.abort(); resetView(); tables.value = { items: [] }; activeKey.value = ''; actorScopes.value = [];
  settingsOpen.value = false; contextReady = false; tablesLoading.value = false; tablesError.value = '';
  query.value = ''; appliedQuery.value = ''; scope.value = ''; notice.value = ''; cleanupPending.value = 0;
  saving.value = false; uploading.value = false;
}
let polling = false;
async function poll() {
  if (polling || disposed || document.hidden || busy.value || editing.value || settingsOpen.value || discardOpen.value || maintenanceIntent.value || hasUnsaved() || listLoading.value || tablesLoading.value || detailLoading.value) return;
  polling = true;
  try {
    await loadTables(undefined, false, true);
    if (disposed || editing.value || hasUnsaved() || busy.value || detailLoading.value || maintenanceIntent.value || !detailOpen.value || !detail.value) return;
    const read = detailRead = new AbortController();
    try {
      const record = await api.record(activeKey.value, detail.value.record_id, read.signal);
      if (!disposed && !read.signal.aborted && !editing.value && !hasUnsaved() && !busy.value
          && detailOpen.value && targetId.value === record.record_id) applyDetail(record);
    } catch (error) { if (!disposed && !read.signal.aborted) withdrawUnauthorized(error); }
  } finally { polling = false; }
}
function schedulePoll() {
  window.clearTimeout(pollTimer);
  const pending = listPending.value || records.value.some(record => ['queued', 'indexing', 'caching'].includes(indexState(record)));
  if (!disposed && !document.hidden) pollTimer = window.setTimeout(async () => { try { await poll(); } finally { schedulePoll(); } }, pending ? 30_000 : 120_000);
}
function visibilityChanged() { schedulePoll(); }
watch(() => props.scope, value => { if (value !== undefined && normalizeScope(value) !== scope.value && !busy.value) changeScope(value); });
watch(uploadField, () => replaceId.value = '');
onMounted(() => { unregisterNavigation = registerNavigationGuard((_target, proceed) => { if (busy.value) return false; if (!hasUnsaved()) return true; leaveAction = proceed; discardOpen.value = true; return false; }); window.addEventListener('beforeunload', beforeUnload); window.addEventListener('clipflow-read-cache-reset', identityChanged); window.addEventListener(AUTH_EXPIRED_EVENT, clearIdentity); document.addEventListener('visibilitychange', visibilityChanged); void loadTables(); schedulePoll(); });
onBeforeUnmount(() => { disposed = true; window.clearTimeout(pollTimer); maintenanceRead?.abort(); tablesRead?.abort(); listRead?.abort(); detailRead?.abort(); versionRead?.abort(); unregisterNavigation?.(); window.removeEventListener('beforeunload', beforeUnload); window.removeEventListener('clipflow-read-cache-reset', identityChanged); window.removeEventListener(AUTH_EXPIRED_EVENT, clearIdentity); document.removeEventListener('visibilitychange', visibilityChanged); });
</script>

<style scoped>
.document-library{width:100%;max-width:1600px;min-width:0;box-sizing:border-box;margin:auto;padding:16px 20px;color:var(--doc-ink);font-size:14px;letter-spacing:0;display:grid;gap:14px}.document-toolbar,.document-heading,.document-actions,.document-pagination,.document-pagination>div,.document-detail-bar,.document-detail-footer{display:flex;align-items:center;gap:8px}.document-toolbar{flex-wrap:wrap;padding-bottom:14px;border-bottom:1px solid var(--doc-line)}.document-heading{min-width:0}.document-heading h1{margin:0;font-size:19px;font-weight:650;white-space:nowrap}.document-heading>svg{color:var(--doc-accent)}.document-actions{margin-left:auto;flex-wrap:wrap}.document-library :deep(.n-button__content),.document-detail :deep(.n-button__content){gap:5px}.document-filters{display:grid;grid-template-columns:minmax(180px,260px) minmax(150px,1fr) minmax(140px,190px) auto;gap:10px;margin:0}.document-filters>.n-input{grid-column:auto}.document-loading{display:grid;gap:10px;padding:8px 0}.document-results{min-width:0}.document-table-wrap{overflow:auto;max-width:100%}.document-table{width:100%;border-collapse:collapse;text-align:left;table-layout:fixed}.document-table th{font-size:12px;color:var(--doc-muted);background:var(--doc-soft);font-weight:500}.document-table th,.document-table td{padding:12px 10px;border-bottom:1px solid var(--doc-line);overflow-wrap:anywhere;vertical-align:middle}.document-table th:last-child{width:44px}.document-table th:nth-last-child(2){width:160px}.document-table td{font-size:13px;line-height:1.6}.document-table tbody tr:hover{background:color-mix(in srgb,var(--doc-accent) 4%,transparent)}.document-record-title{display:flex;align-items:flex-start;gap:8px;text-align:left;font:inherit;font-weight:550;border:0;background:transparent;color:var(--doc-ink);cursor:pointer;padding:3px 0;max-width:100%;min-width:0}.document-record-title>svg{flex:none;margin-top:3px;color:var(--doc-accent)}.document-record-meta{display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-size:12px;color:var(--doc-muted)}.document-record-meta>span{display:flex;align-items:center;gap:4px}.document-pagination{justify-content:space-between;font-size:12px;color:var(--doc-muted)}.document-mobile-list{display:none}.document-detail{display:grid;gap:14px;min-width:0;color:var(--doc-ink);font-size:14px}.document-detail-bar{justify-content:flex-end}.document-detail-bar>.n-tag{margin-right:auto}.document-field-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px 20px}.document-detail-field{display:grid;align-content:start;gap:6px;min-width:0}.document-detail-field>span{display:flex;align-items:center;gap:5px;color:var(--doc-muted);font-size:12px}.document-wide{grid-column:1/-1}.document-required,.document-error{color:var(--doc-error)}.document-error{font-size:12px}.document-attachments{border-top:1px solid var(--doc-line);padding-top:14px;min-width:0}.document-attachments h2{display:flex;align-items:center;gap:7px;font-size:14px;margin:0 0 10px}.document-attachments h2>span{color:var(--doc-muted);font-size:12px;font-weight:400}.document-attachment{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--doc-line);min-width:0}.document-attachment>svg{flex:none;color:var(--doc-muted)}.document-attachment>img{object-fit:cover;border:1px solid var(--doc-line);border-radius:4px;flex:none}.document-attachment>div{min-width:0;flex:1}.document-attachment strong,.document-versions strong{display:block;font-size:13px;font-weight:500;overflow-wrap:anywhere}.document-attachment small,.document-versions small{display:block;color:var(--doc-muted);font-size:12px;margin-top:4px}.document-attachment>a,.document-versions a{display:flex;align-items:center;justify-content:center;color:var(--doc-accent);min-width:36px;min-height:36px}.document-upload{display:grid;gap:12px;margin-top:16px}.document-upload>.n-button{justify-self:end}.document-upload-controls{display:grid;grid-template-columns:1fr 1fr;gap:10px}.document-file-picker{position:relative;display:flex;align-items:center;gap:10px;min-height:54px;padding:10px 12px;box-sizing:border-box;border:1px dashed var(--doc-line);border-radius:6px;overflow-wrap:anywhere;cursor:pointer}.document-file-picker>span{min-width:0}.document-file-picker input{position:absolute;inset:0;opacity:0;width:100%;cursor:pointer}.document-versions ol{list-style:none;margin:0;padding:0}.document-versions li{display:flex;align-items:flex-start;gap:10px;padding:14px 0;border-bottom:1px solid var(--doc-line)}.document-versions li>div{flex:1;min-width:0}.document-versions li>svg{margin-top:3px;flex:none;color:var(--doc-muted)}.document-detail-footer{justify-content:flex-end;flex-wrap:wrap}.document-muted{color:var(--doc-muted);font-size:13px}.document-sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)}.document-library :focus-visible,.document-detail :focus-visible{outline:2px solid var(--doc-accent);outline-offset:3px}.document-record-title,.document-mobile-title{transition:color 160ms ease-out}.document-record-title:hover,.document-mobile-title:hover{color:var(--doc-accent)}@media(max-width:760px){.document-library{padding:12px;gap:12px}.document-toolbar{gap:10px}.document-toolbar>.n-button{order:0}.document-heading h1{font-size:17px}.document-actions{margin-left:auto;gap:6px}.document-filters{grid-template-columns:minmax(0,1fr) auto;gap:8px}.document-filters>.n-base-selection,.document-filters>.n-select{grid-column:1/-1}.document-filters>.n-input{grid-column:1}.document-filters>.n-button{grid-column:2;grid-row:2}.document-table-wrap{display:none}.document-mobile-list{display:grid}.document-mobile-list article{padding:12px 0;border-bottom:1px solid var(--doc-line)}.document-mobile-title{display:flex;align-items:center;gap:8px;width:100%;min-height:44px;padding:0;text-align:left;border:0;background:transparent;font:inherit;color:var(--doc-ink);cursor:pointer}.document-mobile-title>svg{flex:none;color:var(--doc-accent)}.document-mobile-title>strong{min-width:0;flex:1;overflow-wrap:anywhere;font-weight:550}.document-mobile-list dl{display:grid;grid-template-columns:88px minmax(0,1fr);gap:6px 10px;margin:8px 0 12px;font-size:13px}.document-mobile-list dt{color:var(--doc-muted);overflow-wrap:anywhere}.document-mobile-list dd{margin:0;min-width:0}.document-detail .n-button,.document-library :deep(.n-button),.document-detail-footer :deep(.n-button){min-height:44px}.document-field-grid{grid-template-columns:minmax(0,1fr);gap:14px}.document-upload-controls{grid-template-columns:minmax(0,1fr)}.document-attachment>a,.document-versions a{min-width:44px;min-height:44px}.document-actions>.n-button{min-width:40px}.document-pagination{padding-top:4px}}@media(max-width:400px){.document-actions{width:100%;justify-content:flex-end}.document-toolbar{row-gap:8px}}@media(prefers-reduced-motion:reduce){.document-library *,.document-detail *{transition:none!important;animation:none!important}}
.document-reindex-mode{width:170px;max-width:100%}.document-pending{border:1px solid var(--doc-line);border-radius:6px;padding:10px 12px}.document-pending summary{cursor:pointer;font-weight:600}.document-operation{display:flex;gap:10px;align-items:center;flex-wrap:wrap;padding:10px 0;border-top:1px solid var(--doc-line)}.document-operation>div:first-child{flex:1 1 200px;min-width:0}.document-operation small{display:block;color:var(--doc-muted);overflow-wrap:anywhere}.document-operation>.n-input{flex:1 1 210px;max-width:300px}.document-conflicts article{padding:10px 0;border-bottom:1px solid var(--doc-line)}.document-conflicts article>div{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:8px 0;overflow-wrap:anywhere}.document-conflicts article>.n-button{margin:0 8px 4px 0}.document-preview{display:grid;gap:12px;min-width:0}.document-preview-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.document-preview-actions a{margin-left:auto;display:flex;align-items:center;gap:5px;min-height:44px}.document-preview img{display:block;width:100%;height:65dvh;object-fit:contain}.document-preview iframe{border:0;width:100%;height:65dvh;min-height:220px}.document-storage{display:grid;grid-template-columns:110px 1fr;gap:12px}.document-storage dd{margin:0;overflow-wrap:anywhere}.document-table th:last-child{width:90px}.document-table th{position:sticky;top:0;z-index:1}.document-table-wrap{max-height:65vh}.document-system-fields summary{cursor:pointer;padding:8px 0}.document-source small{overflow-wrap:anywhere}@media(max-width:760px){.document-operation>.n-input{max-width:none}.document-operation>.n-button{min-height:44px}.document-conflicts article>div{grid-template-columns:1fr}.document-preview-actions .n-button{min-height:44px;min-width:44px}.document-reindex-mode{flex:1 1 160px;min-width:0}.document-storage{grid-template-columns:90px 1fr}}
@media(max-width:760px){.document-read-fields{grid-template-columns:repeat(2,minmax(0,1fr))}.document-read-fields>.document-detail-field:first-child{grid-column:1/-1}}
</style>

<style scoped>
.document-source-heading,.document-table-context,.document-record-source{display:flex;align-items:center;gap:8px;min-width:0}
.document-source-heading{margin-bottom:10px}.document-source-heading h2{margin:0;font-size:14px}.document-source-heading>span{color:var(--doc-muted);font-size:12px}
.document-source-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr));gap:12px}
.document-source{min-width:0;border:1px solid var(--doc-line);border-radius:6px;background:var(--doc-surface);overflow:hidden}
.document-source-entry{display:flex;align-items:center;gap:12px;width:100%;min-height:84px;padding:14px;text-align:left;border:0;background:transparent;color:var(--doc-ink);font:inherit;cursor:pointer}
.document-source-entry:hover{background:var(--doc-soft)}.document-source-entry:disabled{cursor:wait;opacity:.6}
.document-source-entry>svg{flex:none;color:var(--doc-accent)}.document-source-entry>span{min-width:0;flex:1}
.document-source-entry strong,.document-source-entry small{display:block;overflow-wrap:anywhere}.document-source-entry strong{font-size:15px}.document-source-entry small{margin-top:6px;color:var(--doc-muted);font-size:12px}
.document-source-link{display:flex;align-items:center;gap:6px;min-height:44px;padding:6px 14px;box-sizing:border-box;border-top:1px solid var(--doc-line);color:var(--doc-accent);font-size:12px;text-decoration:none}
.document-source-link>span{overflow-wrap:anywhere;min-width:0}.document-source-link>svg{flex:none}.document-source-link:hover{text-decoration:underline}
.document-table-context{flex-wrap:wrap}.document-table-context>strong{flex:1;min-width:120px;overflow-wrap:anywhere;font-size:15px}
.document-table-context>a{display:flex;align-items:center;gap:5px;min-height:44px;color:var(--doc-accent);font-size:13px;text-decoration:none}
.document-filters{grid-template-columns:minmax(150px,1fr) minmax(140px,190px) auto}
.document-record-source{flex-wrap:wrap;padding:10px 0;border-bottom:1px solid var(--doc-line)}.document-record-source>span{color:var(--doc-muted);font-size:12px}.document-record-source>strong{overflow-wrap:anywhere}
.document-system-fields{border-top:1px solid var(--doc-line);padding-top:8px;font-size:13px}.document-system-fields>summary{cursor:pointer;min-height:44px;align-content:center;color:var(--doc-muted)}.document-system-fields>.document-field-grid{padding:8px 0}
@media(max-width:760px){.document-filters{grid-template-columns:minmax(0,1fr) auto}.document-filters>.n-input{grid-column:1;grid-row:1}.document-filters>.n-button{grid-column:2;grid-row:1}.document-filters>.n-select{grid-column:1/-1;grid-row:2}.document-source-entry{padding:12px;gap:8px}.document-source-link{padding-inline:12px}.document-field-grid{gap:12px}}
.document-detail-field :deep(.document-field-value),
.document-detail-field > time {
  color: var(--doc-ink);
  font-size: 14px;
}
.document-detail-field > span:first-child {
  overflow-wrap: anywhere;
}
.document-detail-bar {
  flex-wrap: wrap;
}
.document-detail {
  padding-bottom: env(safe-area-inset-bottom, 0px);
}
.document-index-issues { font-size: 13px; overflow-wrap: anywhere; }
.document-index-issues summary { cursor: pointer; padding: 8px 0; }
.document-index-issues ul { margin: 0; padding-left: 20px; }
.document-index-issues li { padding: 6px 0; }
.document-index-issues strong, .document-index-issues span { display: block; }
.document-index-issues span { color: var(--doc-muted); margin-top: 4px; }
.document-source-top,.document-row-actions,.document-mobile-heading,.document-selection{display:flex;align-items:center;gap:8px;min-width:0}
.document-source-top>.document-source-entry{flex:1;min-width:0}.document-source-top>.n-button{flex:none;margin-right:8px}
.document-table th.document-check,.document-table td.document-check{width:28px}.document-table th:last-child{width:100px}.document-table th:nth-last-child(2){width:180px}
.document-row-actions{justify-content:flex-end}.document-row-actions>.n-button{flex:none;width:36px;min-width:36px;padding:0}
.document-selection{flex-wrap:wrap}.document-selection-summary{flex-basis:100%;font-size:12px;color:var(--doc-muted);overflow-wrap:anywhere}
.document-mobile-heading>.n-checkbox{flex:none;padding:12px 4px}.document-mobile-heading>.document-mobile-title{flex:1;min-width:0}
.document-mobile-heading>.n-button{flex:none;width:36px;min-width:36px;padding:0}
.document-pagination{flex-wrap:wrap}.document-pagination>div{gap:4px}.document-pagination :deep(.n-button){width:40px;padding:0}
.document-maintenance-dialog p{overflow-wrap:anywhere}.document-maintenance-dialog :deep(.n-dialog__action){flex-wrap:wrap}
@media(max-width:760px){.document-selection{gap:6px}.document-selection :deep(.n-button){font-size:12px;padding-inline:9px}.document-pagination{gap:8px}.document-pagination>span{flex:1;min-width:120px}.document-pagination :deep(.n-button){width:36px;min-width:36px}.document-row-actions{gap:4px}}
.document-filters{display:flex;flex-wrap:wrap;align-items:center;gap:8px}.document-filters>.n-input{flex:1;min-width:150px}.document-filters>.n-select{flex:0 1 190px;min-width:140px}
.document-field-filters{display:grid;gap:8px}.document-filter-row{display:flex;flex-wrap:wrap;align-items:center;gap:8px;min-width:0}.document-filter-row>:nth-child(1){flex:1 1 160px;min-width:130px}.document-filter-row>:nth-child(2){flex:0 1 120px;min-width:100px}.document-filter-row>:nth-child(3):not(.n-button){flex:1 1 190px;min-width:130px}.document-filter-row>.n-button{flex:none}
.document-pagination>nav{display:flex;align-items:center;flex-wrap:wrap;gap:4px;max-width:100%}.document-page-ellipsis{padding:0 2px}
@media(max-width:760px){.document-filters>.n-select{flex:1 1 150px}.document-filter-row{gap:6px}.document-filter-row>:nth-child(1){flex-basis:calc(60% - 8px);min-width:0}.document-filter-row>:nth-child(2){flex-basis:calc(40% - 8px);min-width:0}.document-filter-row>:nth-child(3):not(.n-button){flex-basis:calc(100% - 56px);min-width:0}.document-pagination>nav{width:100%;gap:4px}.document-pagination :deep(.n-button){width:44px;min-width:44px}}
</style>
