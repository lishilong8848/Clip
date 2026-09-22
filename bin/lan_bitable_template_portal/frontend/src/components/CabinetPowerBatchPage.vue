<template>
  <main class="batch-page" :class="{ 'has-detail': batchId, 'is-text-entry': mode==='new' && !batchId && createMode==='text' }">
    <header class="page-heading">
      <VnetBackButton :to="backTarget" />
      <div><h1>{{ batchId ? '上下电待办详情' : mode === 'new' ? '机柜批量登记' : '上下电待办' }}</h1></div>
      <div class="actions">
        <button v-if="mode !== 'new' && !batchId" class="primary" @click="openNew"><Files :size="16" />批量登记</button>
        <button v-if="batchId" @click="openList"><ClipboardList :size="16" />返回待办</button>
        <button :disabled="loading" @click="reload"><RefreshCw :size="16" :class="{ spin: loading }" />刷新</button>
      </div>
    </header>

    <p v-if="error" class="notice danger" role="alert">{{ error }}<button aria-label="关闭错误" @click="error = ''"><X :size="16" /></button></p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <section v-if="saveConflicts.length && !editingRowId" class="save-conflicts" role="alert">
      <strong>内容已被其他操作更新，未保存的输入仍保留</strong>
      <div v-for="conflict in saveConflicts" :key="conflict.row_id + conflict.key"><span>{{ conflict.title }} · {{ fieldLabels[conflict.key] }}</span><span>本次：{{ conflict.local || '空白' }}；已保存：{{ conflict.remote || '空白' }}</span><button :disabled="!conflict.canKeep" @click="resolveSaveConflict(conflict,true)">保留本次</button><button @click="resolveSaveConflict(conflict,false)">采用已保存</button></div>
    </section>

    <template v-if="mode === 'new' && !batchId">
      <nav class="tabs" aria-label="批量登记方式"><button :class="{ active: createMode === 'pdf' }" @click="createMode = 'pdf'">PDF识别</button><button :class="{ active: createMode === 'image' }" @click="createMode = 'image'">图片识别</button><button :class="{ active: createMode === 'text' }" @click="createMode = 'text'">粘贴文本识别</button><button :class="{ active: createMode === 'manual' }" @click="createMode = 'manual'; loadDirectory()">手工批量</button></nav>
      <section v-if="createMode === 'pdf'" class="entry-section">
        <h2>上传确认单</h2>
        <p>支持系统导出的文本PDF；每批最多10份、单份10MiB、合计30MiB。扫描件和加密PDF请使用手工批量。</p>
        <label class="file-drop" :class="{ dragging }" tabindex="0" aria-label="选择、拖入或粘贴PDF确认单" @dragenter.prevent="dragging = true" @dragover.prevent="dragging = true" @dragleave.prevent="dragging = false" @drop.prevent="dropFiles" @keydown.enter.prevent="fileInput?.click()" @keydown.space.prevent="fileInput?.click()"><Upload :size="24" /><strong>选择、拖入或粘贴PDF确认单</strong><span class="drop-hint">将文件拖到这里，或按 Ctrl+V 粘贴已复制的 PDF</span><input ref="fileInput" type="file" accept="application/pdf,.pdf" multiple @change="selectFiles" /></label>
        <ul v-if="pdfFiles.length" class="file-list"><li v-for="(file,index) in pdfFiles" :key="file.name + file.size + file.lastModified"><FileText :size="16" /><span>{{ file.name }}</span><small>{{ formatBytes(file.size) }}</small><button class="icon-button" title="移除文件" :aria-label="'移除' + file.name" @click="pdfFiles.splice(index,1)"><Trash2 :size="15" /></button></li></ul>
        <div class="actions"><button class="primary" :disabled="uploading || !pdfFiles.length" @click="recognize"><Loader2 v-if="uploading" class="spin" :size="16" /><ScanText v-else :size="16" />{{ uploading ? `正在上传 ${pdfFiles.length} 份文件` : '识别并创建待办' }}</button></div>
      </section>

      <section v-else-if="createMode === 'image'" class="entry-section">
        <h2>上传确认截图</h2>
        <p>支持 JPG、PNG、WebP；可粘贴、拖入、多选或选择文件夹。识别结果先进入待办，确认后才写入机柜台账。</p>
        <label class="file-drop" :class="{ dragging }" tabindex="0" aria-label="选择、拖入或粘贴确认截图" @dragenter.prevent="dragging = true" @dragover.prevent="dragging = true" @dragleave.prevent="dragging = false" @drop.prevent="dropCreateImages" @keydown.enter.prevent="createImageInput?.click()" @keydown.space.prevent="createImageInput?.click()"><Upload :size="24" /><strong>选择、拖入或粘贴确认截图</strong><span class="drop-hint">每张不超过 10 MiB；识别不清的字段可在待办详情修改</span><input ref="createImageInput" type="file" accept="image/jpeg,image/png,image/webp" multiple @change="selectCreateImages" /></label>
        <div class="actions"><label class="file-command">选择文件夹<input type="file" accept="image/jpeg,image/png,image/webp" multiple webkitdirectory @change="selectCreateImages" /></label><small v-if="imageFiles.length">已选择 {{ imageFiles.length }} 张</small></div>
        <ul v-if="imageFiles.length" class="file-list image-file-list"><li v-for="file in imageFiles" :key="file.name + file.size + file.lastModified"><img :src="filePreview(file)" :alt="file.name" /><span>{{ file.name }}</span><small>{{ formatBytes(file.size) }}</small><button class="icon-button" title="移除图片" :aria-label="'移除' + file.name" @click="removeCreateImage(file)"><Trash2 :size="15" /></button></li></ul>
        <div class="actions"><button class="primary" :disabled="uploading || !imageFiles.length" @click="createFromImages"><Loader2 v-if="uploading" class="spin" :size="16" /><ScanText v-else :size="16" />{{ uploading ? `已上传 ${imageUploadDone}/${imageUploadTotal} 张` : '识别并创建待办' }}</button><button v-if="imageBatchId && !uploading" @click="openBatch(imageBatchId)">打开已创建待办</button></div>
      </section>

      <section v-else-if="createMode === 'text'" class="entry-section text-entry">
        <div class="text-input-bar">
          <textarea v-model="textInput" rows="2" aria-label="粘贴机柜确认文本" :disabled="uploading" placeholder="粘贴机柜确认表格或文本…" @paste="pasteConfirmationText"></textarea>
          <button v-if="textInput.trim()" :disabled="uploading" @click="appendText(textInput)"><ScanText :size="16" />识别当前文本</button>
          <span v-if="textPending" role="status"><Loader2 class="spin" :size="15" />识别中 {{ textPending }} 段</span>
          <span v-else role="status">{{ textFragments.length }} 次粘贴 · {{ textRows.length }} 条记录</span>
        </div>
        <p v-if="textError" class="notice danger" role="alert">{{ textError }}</p>
        <div class="text-toolbar">
          <div class="text-source-picker"><select v-model="textSource" aria-label="筛选粘贴记录" @change="textPage=1"><option value="">全部粘贴（{{ textFragments.length }} 次）</option><option v-for="(fragment,index) in textFragments" :key="fragment.id" :value="fragment.id">第 {{ index+1 }} 次 · {{ fragment.loading ? '识别中' : fragment.rows.length+' 条' }}{{ fragment.rows[0] ? ' · '+fragment.rows[0].scope+'楼 '+fragment.rows[0].room : '' }}</option></select><button class="icon-button" :disabled="uploading || !textSource" title="移除所选粘贴及其全部记录" aria-label="移除所选粘贴记录" @click="textDeleteId=textSource"><Trash2 :size="15" /></button></div>
          <label class="text-search"><Search :size="15" /><input v-model="textSearch" aria-label="搜索粘贴记录" placeholder="搜索包间、机柜或操作" @input="textPage=1" /></label>
          <select v-model="textReview" aria-label="筛选核对状态" @change="textPage=1"><option value="">全部记录（{{ textScopeRows.length }}）</option><option value="issues">待核对（{{ textIssueCount }}）</option><option value="duplicates">重复（{{ textScopeDuplicateCount }}）</option></select>
        </div>
        <div v-if="textFilteredRows.length" ref="textTable" class="table-wrap text-preview"><table><thead><tr><th>粘贴</th><th>楼栋</th><th>包间</th><th>机柜</th><th>操作类型</th><th>期望完成时间</th><th>实际完成时间</th><th>结果</th><th>机柜类型</th><th v-if="textHasSupplier">运营商机柜编号</th><th>核对</th><th>操作</th></tr></thead><tbody><tr v-for="row in textPagedRows" :key="row.row_id" :class="{ 'text-attention': textRowMessage(row) }" @change="textEdited(row)"><td>{{ row._fragmentNumber }}</td><td><select v-model="row.scope" :disabled="uploading" aria-label="文本记录楼栋"><option v-for="option in buildingOptions" :key="option.value" :value="option.value">{{ option.label }}</option></select></td><td><input v-model="row.room" class="short-input" :disabled="uploading" aria-label="文本记录包间" /></td><td><input v-model="row.rack" class="short-input" :disabled="uploading" aria-label="文本记录机柜" /></td><td><select v-model="row.action" :disabled="uploading" aria-label="文本记录操作类型"><option value="">待核对</option><option v-for="action in actions" :key="action">{{ action }}</option></select></td><td><input :value="localDate(row.expected)" :disabled="uploading" type="datetime-local" step="1" aria-label="文本记录期望完成时间" @input="row.expected=inputDate($event)" /></td><td><input :value="localDate(row.actual)" :disabled="uploading" type="datetime-local" step="1" aria-label="文本记录实际完成时间" @input="row.actual=inputDate($event)" /></td><td><select v-model="row.result" :disabled="uploading" aria-label="文本记录结果"><option value="">待核对</option><option>成功</option><option>失败</option></select><input v-if="row.result==='失败'" v-model="row.failure_reason" :disabled="uploading" maxlength="1000" placeholder="失败原因" aria-label="文本记录失败原因" /></td><td>{{ row.rack_type || '待核对' }}</td><td v-if="textHasSupplier">{{ row.supplier_rack || '—' }}</td><td class="text-review">{{ textRowMessage(row) || '已识别' }}</td><td><button class="icon-button" :disabled="uploading" title="移除记录" aria-label="移除文本记录" @click="removeTextRow(row)"><Trash2 :size="15" /></button></td></tr></tbody></table></div>
        <div v-if="!textFilteredRows.length" class="text-empty"><span>{{ textPending ? '正在识别粘贴内容…' : textRows.length ? textReview==='duplicates' ? '当前筛选范围没有重复记录' : '没有符合筛选条件的记录' : '暂无粘贴记录' }}</span><button v-if="!textPending && textReview==='duplicates' && textDuplicateIds.size" class="link" @click="textSource='';textSearch='';textPage=1">查看全部重复（{{ textDuplicateIds.size }}）</button></div>
        <footer class="text-footer">
          <span>当前 {{ textFilteredRows.length }} 条 / 全部 {{ textRows.length }} 条</span>
          <nav class="pagination" aria-label="文本记录分页"><select v-model.number="textPageSize" aria-label="文本记录每页条数" @change="textPage=1"><option :value="25">25条/页</option><option :value="50">50条/页</option><option :value="100">100条/页</option></select><button :disabled="textPage===1" aria-label="文本记录上一页" title="上一页" @click="textPage--"><ChevronLeft :size="16" /></button><button v-for="page in pageNumbers(textPage,textPageCount)" :key="page" :class="{active:page===textPage}" :aria-current="page===textPage?'page':undefined" @click="textPage=page">{{ page }}</button><button :disabled="textPage===textPageCount" aria-label="文本记录下一页" title="下一页" @click="textPage++"><ChevronRight :size="16" /></button></nav>
          <button class="primary" :disabled="uploading || textPending>0 || !textRows.length" @click="createFromText"><Loader2 v-if="uploading" class="spin" :size="16" /><ClipboardPaste v-else :size="16" />{{ uploading ? '正在创建待办' : '识别并创建待办' }}<span v-if="!uploading && textRows.length">（{{ textRows.length }}）</span></button>
        </footer>
        <ConfirmDialog :open="Boolean(textDeleteId)" title="移除粘贴记录" :message="`将移除此段粘贴的 ${textFragments.find(item=>item.id===textDeleteId)?.rows.length || 0} 条机柜记录，不影响其他粘贴内容。`" confirm-label="确认移除" @resolve="resolveTextDelete" />
      </section>

      <section v-else class="entry-section">
        <h2>手工批量填写</h2>
        <div class="common-fields">
          <label>楼栋<select v-model="manualScope" @change="loadDirectory"><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select></label>
          <label>操作类型<select v-model="common.action"><option value="">请选择</option><option v-for="action in actions" :key="action">{{ action }}</option></select></label>
          <label>期望完成时间<input v-model="common.expected" type="datetime-local" step="1" /></label>
          <label>实际完成时间<input v-model="common.actual" type="datetime-local" step="1" @change="defaultExpected" /></label>
          <label>结果<select v-model="common.result"><option>成功</option><option>失败</option></select></label>
          <label v-if="common.result === '失败'">失败原因<input v-model="common.failure_reason" maxlength="1000" /></label>
        </div>
        <div class="manual-source">
          <div><label>从机柜目录多选</label><input v-model="directorySearch" placeholder="搜索包间或机柜" /><select v-model="pickedRacks" multiple size="9" aria-label="机柜目录多选"><option v-for="rack in filteredDirectory" :key="rack.room + '/' + rack.rack" :value="rack.room + '/' + rack.rack">{{ rack.room }}，{{ rack.rack }} · {{ rack.rack_type || '未填写类型' }}</option></select><button :disabled="!pickedRacks.length" @click="addPicked"><Plus :size="16" />添加所选机柜</button></div>
          <div><label>粘贴Excel双列（包间，机柜）</label><textarea v-model="pastedRows" rows="9" placeholder="201&#9;A01&#10;201&#9;A02"></textarea><button :disabled="!pastedRows.trim()" @click="addPasted"><ClipboardPaste :size="16" />解析并添加</button></div>
        </div>
        <div class="section-heading"><h3>待创建明细（{{ manualRows.length }}）</h3><button :disabled="!manualRows.length" @click="applyManualCommon">将公共字段应用到全部</button></div>
        <div class="table-wrap"><table><thead><tr><th>楼栋</th><th>包间</th><th>机柜</th><th>机柜类型</th><th>操作类型</th><th>期望完成时间</th><th>实际完成时间</th><th>结果</th><th>失败原因</th><th>操作</th></tr></thead><tbody><tr v-for="(row,index) in manualRows" :key="row._id"><td>{{ row.scope }}楼</td><td><input v-model="row.room" /></td><td><input v-model="row.rack" /></td><td><select v-model="row.rack_type"><option value="">请选择</option><option>网络机柜</option><option>服务器机柜</option></select></td><td><select v-model="row.action"><option value="">请选择</option><option v-for="action in actions" :key="action">{{ action }}</option></select></td><td><input v-model="row.expected" type="datetime-local" step="1" /></td><td><input v-model="row.actual" type="datetime-local" step="1" /></td><td><select v-model="row.result"><option>成功</option><option>失败</option></select></td><td><input v-if="row.result === '失败'" v-model="row.failure_reason" maxlength="1000" aria-label="失败原因" /></td><td><button class="icon-button" title="移除" aria-label="移除" @click="manualRows.splice(index,1)"><Trash2 :size="16" /></button></td></tr></tbody></table></div>
        <div class="actions end"><button class="primary" :disabled="uploading || !manualRows.length" @click="createManual"><Save :size="16" />创建待办</button></div>
      </section>
    </template>

    <template v-else-if="batchId && batch.batch_id">
      <section class="batch-summary">
        <div class="summary-main"><div class="summary-title"><strong>批次 {{ batch.batch_id.slice(0, 8) }}</strong><span :class="'status ' + batch.status">{{ batch.source_notice?.deleted_at ? '来源通告已删除' : recognizingEvidence ? '图片识别中' : statusLabel(batch.status) }}</span></div><small>{{ batch.created_at }}</small><small>{{ (batch.scopes || []).map((item: string) => item + '楼').join('、') || '识别中' }}</small></div>
        <dl class="metrics">
          <div><dt>机柜</dt><dd>{{ batch.stats?.total || 0 }}</dd></div>
          <div><dt>可确认</dt><dd>{{ batch.stats?.confirmable || 0 }}</dd></div>
          <div v-if="attentionCount" class="warning-metric"><dt>待核对</dt><dd>{{ attentionCount }}</dd></div>
          <div class="success-metric"><dt>已完成</dt><dd>{{ batch.stats?.completed || 0 }}</dd></div>
          <div v-if="batch.stats?.rolled_back"><dt>已回退</dt><dd>{{ batch.stats.rolled_back }}</dd></div>
          <div v-if="batch.stats?.rollback_failed || batch.stats?.rollback_blocked" class="danger-metric"><dt>回退受阻</dt><dd>{{ (batch.stats?.rollback_failed || 0) + (batch.stats?.rollback_blocked || 0) }}</dd></div>
        </dl>
      </section>
      <div v-if="batch.status === 'cancelled'" class="notice warning" role="status"><AlertTriangle :size="18" /><span>该批次已作废。已完成的机柜记录保留，未提交行可勾选后批量恢复。</span></div>
      <details v-if="batch.source === 'notice'" class="source-details">
        <summary>通告来源<span>{{ batch.source_notice?.title || batch.source_notice?.notice_type || '上下电通告' }}</span></summary>
        <div class="notice-source">
        <div><small>来源</small><strong>{{ batch.source_notice?.notice_type || '上下电通告' }}</strong></div>
        <div><small>通告名称</small><strong>{{ batch.source_notice?.title || '-' }}</strong></div>
        <div><small>计划时间</small><span>{{ batch.source_notice?.start_time || '-' }} ~ {{ batch.source_notice?.end_time || '-' }}</span></div>
        <div><small>开始通告实际发送</small><span>{{ batch.source_notice?.sent_at || '尚未核实' }}</span></div>
        <div><small>结束通告实际发送</small><span>{{ batch.source_notice?.ended_at || '尚未结束或未核实' }}</span></div>
        <div><small>数量核对</small><span>声明 {{ batch.notice_counts?.declared ?? '-' }} 柜 · 唯一 {{ batch.notice_counts?.unique ?? batch.stats?.total ?? 0 }} 柜 · 目录匹配 {{ batch.notice_counts?.directory_matched ?? 0 }} 柜</span></div>
        <div class="notice-source-cabinets"><small>原始柜号</small><span>{{ batch.source_notice?.cabinet || '-' }}</span></div>
        </div>
      </details>
      <div v-if="batch.source_notice?.deleted_at" class="notice warning" role="status"><AlertTriangle :size="18" /><span>来源通告已删除，本批次不再计入待办；如有机柜回退异常，可在“通告回退异常”中查看。</span></div>
      <section v-if="recognizingEvidence" class="notice recognition-progress" role="status" aria-live="polite"><Loader2 class="spin" :size="22" /><div class="progress-copy"><strong>图片识别中 · 已完成 {{ recognitionProgress.done }}/{{ recognitionProgress.total }} 张</strong><progress :value="recognitionProgress.done" :max="recognitionProgress.total || 1"></progress><span>已识别 {{ batch.stats?.total || 0 }} 个机柜{{ recognitionProgress.failed ? ` · ${recognitionProgress.failed} 张需重试或核对` : '' }}</span><small>{{ recognitionProgress.current ? `正在识别：${recognitionProgress.current}` : '图片已上传，正在排队识别' }}</small></div></section>
      <section v-if="batch.status === 'recognizing'" class="notice" role="status"><Loader2 class="spin" :size="18" /><div class="progress-copy"><strong>正在识别确认单</strong><progress :value="batch.progress?.pages_done || batch.progress?.files_done || 0" :max="batch.progress?.pages_total || batch.progress?.files_total || 1"></progress><small>文件 {{ batch.progress?.files_done || 0 }}/{{ batch.progress?.files_total || 0 }} · 页面 {{ batch.progress?.pages_done || 0 }}/{{ batch.progress?.pages_total || 0 }}</small></div></section>
      <section v-if="batch.status === 'running'" class="notice" role="status"><Loader2 class="spin" :size="18" /><div class="progress-copy"><strong>{{ rollbackProgress.active ? '正在逐柜回退' : '正在批量写入多维并核验' }}</strong><progress :value="rollbackProgress.active ? rollbackProgress.done : confirmProgress.done" :max="rollbackProgress.active ? rollbackProgress.total : confirmProgress.total"></progress><small>{{ rollbackProgress.active ? `${rollbackProgress.done}/${rollbackProgress.total} 柜已处理` : `${confirmProgress.done}/${confirmProgress.total} 条已处理` }}</small></div></section>
      <div v-if="batch.error" class="notice danger" role="alert"><span>{{ batch.error }}</span><button v-if="batch.validation_error" :disabled="saving" @click="revalidate">重新校验</button></div>
      <div v-if="batch.blocking_warnings?.length" class="notice warning" role="alert"><AlertTriangle :size="18" /><span><b>通告明细需要核对</b><small v-for="warning in batch.blocking_warnings" :key="warning.code">{{ warning.message }}</small></span><strong v-if="batch.warnings_acknowledged" class="success-text">已人工核对</strong><button v-else :disabled="saving" @click="confirmDialog = 'warnings'">以当前明细为准</button></div>
      <div v-if="batch.stats?.duplicate" class="notice warning" role="alert"><AlertTriangle :size="18" /><span>发现 {{ batch.stats.duplicate }} 条重复记录，清空重叠不影响已保存台账。</span><button @click="confirmDialog = 'overlap'">清空重叠数据</button></div>
      <details v-if="batch.files?.length" class="source-files source-details" :open="batch.files.some((file: Dict) => file.error)"><summary>来源文件<span>{{ batch.files.length }} 份</span></summary><div v-for="file in batch.files" :key="file.file_id"><FileText :size="16" /><a v-if="batch.can_download_files && !file.cleaned_at" :href="api + '/batches/' + batch.batch_id + '/files/' + file.file_id">{{ file.name }}</a><span v-else>{{ file.name }}{{ file.cleaned_at ? '（已清理）' : '' }}</span><small>{{ file.processed_pages || 0 }}/{{ file.pages || 0 }} 页 · {{ statusLabel(file.status) }}</small><b v-if="file.error">{{ file.error }}</b><button v-if="batch.can_download_files && !file.cleaned_at && ['completed','cancelled'].includes(batch.status)" class="icon-button" title="清理原确认单" aria-label="清理原确认单" @click="requestFileCleanup(file.file_id)"><Trash2 :size="16" /></button></div></details>
      <section class="evidence-section"><div class="evidence-upload" :class="{ dragging: evidenceDragging }" title="支持拖入截图或 Ctrl+V 粘贴" @dragover.prevent="evidenceDragging = true" @dragleave.prevent="evidenceDragging = false" @drop.prevent="dropEvidence"><div class="evidence-title"><h3>确认截图</h3><span>{{ activeImages.length }} 张</span></div><label class="file-command" title="选择、拖入或粘贴截图"><Upload :size="15" />选择图片<input ref="imageInput" type="file" accept="image/jpeg,image/png,image/webp" multiple @change="selectEvidence" /></label><label class="file-command">选择文件夹<input type="file" accept="image/jpeg,image/png,image/webp" multiple webkitdirectory @change="selectEvidence" /></label><small v-if="uploadingEvidence">已上传 {{ imageUploadDone }}/{{ imageUploadTotal }} 张，正在识别…</small></div>
        <div v-if="activeImages.length" class="evidence-grid">
          <article v-for="image in pagedActiveImages" :key="image.image_id" class="evidence-item">
            <button class="image-button" :aria-label="'查看原图 ' + image.name" @click="openImage(image.image_id)"><img :src="imageUrl(image.image_id)" :alt="image.name" loading="lazy" /></button>
            <div><div class="evidence-heading"><strong>{{ image.name }}</strong><button class="icon-button danger-icon" :disabled="saving || uploadingEvidence || batch.status === 'cancelled' || !batch.can_download_files || !batch.can_confirm_all || imageLocked(image)" :title="imageLocked(image) ? '已提交的机柜截图须先回退记录' : !batch.can_download_files || !batch.can_confirm_all ? '仅上传者或管理员可删除' : '删除截图'" :aria-label="'删除截图 ' + image.name" @click="requestImageDelete(image.image_id)"><Trash2 :size="16" /></button></div>
              <small>{{ image.status === 'recognizing' ? (image.phase === 'running' ? '正在识别' : '排队识别') : image.error || `${image.suggestions?.length || 0} 项识别结果` }}</small>
              <button v-if="image.status !== 'recognizing' && batch.can_download_files && batch.can_confirm_all && !imageLocked(image) && batch.status !== 'cancelled'" :disabled="saving" @click="retryImage(image)"><RefreshCw :size="14" />重新识别</button>
              <button v-if="batch.source === 'image' && image.status !== 'recognizing' && batch.status !== 'cancelled'" :disabled="saving" @click="openCorrection(image)"><Pencil :size="14" />补全机柜</button>
              <details v-if="image.suggestions?.length" class="recognition-details" :open="image.suggestions.some((item: Dict) => item.status !== 'applied')"><summary>识别明细</summary>
              <div v-for="(candidate,index) in image.suggestions || []" :key="index" class="evidence-match"><span>{{ candidate.scope }}楼 {{ candidate.room }} / {{ candidate.rack }} · {{ candidate.action || '操作待核对' }} · {{ candidate.result || '结果待核对' }}<br />期望 {{ candidate.expected || '未识别' }} · 实际 {{ candidate.actual || '未识别' }}</span><small>{{ candidate.time_conflict ? '证明已关联，时间冲突待核对' : candidate.status === 'applied' ? '已关联' : candidate.status === 'unauthorized' ? '无楼栋权限' : candidate.attached ? '证明已关联，内容待核对' : '待人工核对' }}</small><template v-if="!['applied','unauthorized'].includes(candidate.status)"><input v-model="imageSearches[image.image_id + ':' + index]" class="evidence-search" placeholder="搜索楼栋、包间或机柜" /><select v-model="imageSelections[image.image_id + ':' + index]" :aria-label="'选择截图对应机柜 ' + candidate.rack"><option value="">选择本批次机柜</option><option v-for="row in candidateRows(image.image_id + ':' + index)" :key="row.row_id" :value="row.row_id">{{ row.scope }}楼 {{ row.room }} / {{ row.rack }} · {{ row.action || '待选操作' }}</option></select><button :disabled="saving || !imageSelections[image.image_id + ':' + index]" @click="applyEvidence(image,imageSelections[image.image_id + ':' + index],candidate,index)">{{ candidate.time_conflict ? '采用此截图时间' : '核对并关联' }}</button><button v-if="batch.source === 'image' && !candidate.row_id" :disabled="saving" @click="openCorrection(image,candidate,Number(index))">更正机柜</button></template></div>
              </details>
              <div v-if="!image.suggestions?.length && image.status !== 'recognizing' && selectableImageRows.length" class="evidence-match"><input v-model="imageSearches[image.image_id]" class="evidence-search" placeholder="搜索楼栋、包间或机柜" /><select v-model="imageSelections[image.image_id]" aria-label="手动选择截图对应机柜"><option value="">选择本批次机柜</option><option v-for="row in candidateRows(image.image_id)" :key="row.row_id" :value="row.row_id">{{ row.scope }}楼 {{ row.room }} / {{ row.rack }}</option></select><button :disabled="saving || !imageSelections[image.image_id]" @click="applyEvidence(image,imageSelections[image.image_id],null,-1)">关联截图</button></div>
            </div>
          </article>
        </div><footer v-if="imagePageCount > 1" class="pagination evidence-pagination"><span>截图 {{ imagePage }}/{{ imagePageCount }}</span><button :disabled="imagePage <= 1" @click="imagePage--"><ChevronLeft :size="16" /></button><button :disabled="imagePage >= imagePageCount" @click="imagePage++"><ChevronRight :size="16" /></button></footer>
        <details v-if="deletedImages.length" class="deleted-images"><summary>已删除截图（{{ deletedImages.length }}）</summary><div v-for="image in deletedImages" :key="image.image_id"><img :src="imageUrl(image.image_id)" :alt="image.name" loading="lazy" /><span>{{ image.name }}</span><button :disabled="saving || !batch.can_download_files || !batch.can_confirm_all || batch.status === 'cancelled'" @click="restoreEvidence(image.image_id)">撤回删除</button></div></details>
      </section>
      <form v-if="correction" class="image-correction" aria-label="补全截图机柜" @submit.prevent="submitCorrection">
        <header><h3>核对截图机柜</h3><button type="button" class="icon-button" aria-label="关闭更正" @click="correction=null"><X :size="16" /></button></header>
        <div class="correction-body"><button type="button" class="proof-original" aria-label="查看待更正原图" @click="openImage(correction.image_id)"><img :src="imageUrl(correction.image_id,true)" alt="待更正的确认截图" /></button><div class="common-fields">
          <label>楼栋<select v-model="correction.scope" required @change="correction.room='';correction.rack='';loadCorrectionDirectory()"><option v-for="option in buildingOptions" :key="option.value" :value="option.value">{{ option.label }}</option></select></label>
          <label>包间<select v-model="correction.room" required @change="correction.rack='' "><option value="">请选择</option><option v-for="room in [...new Set(correctionDirectory.map(row=>row.room))]" :key="room">{{ room }}</option></select></label>
          <label>机柜<select v-model="correction.rack" required><option value="">请选择</option><option v-for="rack in correctionDirectory.filter(row=>row.room===correction?.room)" :key="rack.rack">{{ rack.rack }}</option></select></label>
          <label>供应商机柜号<input v-model="correction.supplier_rack" /></label>
          <label>操作类型<select v-model="correction.action"><option value="">待核对</option><option v-for="action in actions" :key="action">{{ action }}</option></select></label>
          <label>结果<select v-model="correction.result"><option value="">待核对</option><option>成功</option><option>失败</option></select></label>
          <label>期望完成时间<input v-model="correction.expected" type="datetime-local" step="1" /></label><label>实际完成时间<input v-model="correction.actual" type="datetime-local" step="1" /></label>
          <label v-if="correction.result === '失败'">失败原因<input v-model="correction.failure_reason" maxlength="1000" required /></label>
        </div></div><p v-if="correction.error" class="notice danger" role="alert">{{ correction.error }}</p><div class="actions end"><button type="button" @click="correction=null">取消</button><button class="primary" :disabled="saving || !correction.rack || !correction.room">补全并关联证明</button></div>
      </form>
      <div v-if="batch.source === 'image' && !batch.rows?.length && !recognizingEvidence && !uploadingEvidence" class="notice warning"><span>图片识别已结束，尚未生成机柜记录。可在截图旁点击“补全机柜”或重新识别。</span><button class="danger-ghost" :disabled="saving" @click="confirmDialog = 'delete_empty'">删除空批次</button></div>

      <section v-if="batch.rows?.length" class="batch-actions">
        <div class="filter-bar"><strong>机柜明细</strong><label v-if="batch.scopes?.length > 1"><span>楼栋</span><select v-model="rowScopeFilter"><option value="">全部楼栋</option><option v-for="item in batch.scopes || []" :key="item" :value="item">{{ item }}楼</option></select></label><label><span>状态</span><select v-model="rowStatusFilter"><option value="">全部状态</option><option value="ready">可确认</option><option value="rolled_back">已回退</option><option value="duplicate">重复</option><option value="conflict">冲突</option><option value="invalid">无效</option><option value="failed">失败</option><option value="completed">已完成</option><option value="excluded">已排除</option></select></label><label><span>快速核对</span><select v-model="rowQuickFilter"><option value="">全部记录</option><option value="missing_actual">未填写实际时间</option><option value="evidence_pending">截图待匹配</option><option value="issues">仅异常记录</option></select></label><span class="filter-count">当前 {{ filteredRows.length }} 条</span></div>
        <div v-if="selectedRows.length" class="selection-panel"><div class="selection-heading"><strong>已选 {{ selectedRows.length }} 条</strong><button class="link" type="button" @click="selectedRows = []">清除选择</button></div><div class="bulk-fields"><template v-if="batch.status !== 'cancelled'"><label><span>操作类型</span><select v-model="bulk.action"><option value="">不修改</option><option v-for="action in actions" :key="action">{{ action }}</option></select></label><label><span>期望完成时间</span><input v-model="bulk.expected" type="datetime-local" step="1" /></label><label><span>实际完成时间</span><input v-model="bulk.actual" type="datetime-local" step="1" /></label><label><span>结果</span><select v-model="bulk.result"><option value="">不修改</option><option>成功</option><option>失败</option></select></label><label v-if="bulk.result === '失败'"><span>失败原因</span><input v-model="bulk.failure_reason" maxlength="1000" /></label><label><span>类型处理</span><select v-model="bulk.type_resolution"><option value="">不修改</option><option value="keep_current">沿用当前机柜类型</option><option value="sync_current">同步修正当前机柜类型</option></select></label><button @click="applyBulk">应用更改</button><button class="danger-ghost" :disabled="saving || !excludableSelectedCount" @click="excludeSelected"><Trash2 :size="16" />排除已选</button></template><button :disabled="saving || !restorableSelectedCount" @click="restoreSelected">恢复已选 {{ restorableSelectedCount }} 条</button></div></div>
        <div class="commit-actions"><span v-if="saving || dirtyCount" class="save-indicator">{{ saving ? '处理中…' : `待自动保存 ${dirtyCount} 条` }}</span><button v-if="saveFailed" @click="saveChanges()">重试保存</button><button class="primary" :disabled="saving || uploadingEvidence || recognizingEvidence || batch.status === 'cancelled' || batch.source_notice?.deleted_at || !batch.stats?.confirmable || !batch.can_confirm_all || warningsPending" :title="recognizingEvidence ? '请等待截图识别完成' : warningsPending ? '请先核对通告明细异常' : batch.can_confirm_all ? '' : '当前账号无权确认本批次'" @click="confirmDialog = 'all'">确认整批</button><button :disabled="saving || !rollbackableCount || !batch.can_confirm_all" @click="requestRollback('')">回退本批已写入记录</button><button :disabled="saving || batch.status === 'completed' || batch.status === 'cancelled' || !cancellableCount" @click="confirmDialog = 'cancel'">{{ batch.status === 'cancelled' ? '已作废' : '作废未提交行' }}</button></div>
      </section>

      <div v-if="batch.rows?.length" class="table-wrap detail-table"><table class="records-table"><thead><tr><th class="select-column"><input type="checkbox" :checked="allVisibleSelected" aria-label="选择本页" @change="toggleVisible" /></th><th class="validation-cell">状态</th><th class="location-summary">位置</th><th v-if="showRackColumn" class="rack-summary">机柜</th><th v-if="showSupplierRackColumn" class="supplier-summary">供应商机柜号</th><th class="operation-summary">当前状态 / 操作</th><th v-if="showExpectedColumn" class="time-cell">期望完成</th><th class="time-cell">实际完成</th><th class="result-summary">结果</th><th class="proof-cell">证明</th><th class="action-column">操作</th></tr></thead><tbody>
        <template v-for="row in pagedRows" :key="row.row_id">
          <tr :class="[row.status, { expanded: editingRowId === row.row_id }]">
            <td><input v-if="row.editable || row.confirmable || row.restorable" v-model="selectedRows" type="checkbox" :value="row.row_id" :aria-label="'选择' + row.rack" /></td>
            <td class="validation-cell"><span :class="'status ' + row.status">{{ batch.source_notice?.deleted_at && row.status === 'ready' ? '来源通告已删除' : statusLabel(row.status) }}</span><small v-if="row.issues?.length">{{ row.issues[0].message }}</small><small v-if="row.issues?.length > 1">另有 {{ row.issues.length - 1 }} 项</small><small v-if="row.error" class="danger-text">{{ row.error }}</small><small v-if="row.notice_removed">最新通告已移除</small></td>
            <td class="location-summary"><strong>{{ row.scope || '-' }}楼</strong><small>{{ row.room ? row.room + '包间' : '包间未填写' }}</small></td>
            <td v-if="showRackColumn" class="rack-summary"><strong>{{ row.rack || '-' }}</strong></td>
            <td v-if="showSupplierRackColumn" class="supplier-summary"><span>{{ row.supplier_rack || '-' }}</span></td>
            <td class="operation-summary"><span v-if="row.current_power_state" :class="'power-state ' + row.current_power_state">{{ powerStateLabel(row.current_power_state) }}</span><strong :class="{ muted: !row.action }">{{ row.action || '待选择' }}</strong></td>
            <td v-if="showExpectedColumn" class="time-cell">{{ row.expected ? tableDate(row.expected) : '—' }}</td>
            <td class="time-cell">{{ tableDate(row.actual) }}</td>
            <td class="result-summary"><span :class="['result-badge', row.result === '成功' ? 'success' : row.result === '失败' ? 'failed' : 'pending']">{{ row.result || '待选择' }}</span></td>
            <td class="proof-cell">
              <div v-if="row.evidence_images?.length" class="proof-thumbs"><button v-for="(id,index) in row.evidence_images.slice(0,2)" :key="id" class="row-thumb" :aria-label="`查看 ${row.rack} 的证明截图 ${index+1}`" @click="openProofImages(row,index)"><img :src="imageUrl(id)" alt="证明截图" loading="lazy" /></button><button v-if="row.evidence_images.length > 2" class="proof-more" :aria-label="'查看全部 ' + row.evidence_images.length + ' 张证明'" @click="openProofImages(row,2)">+{{ row.evidence_images.length - 2 }}</button></div>
              <template v-for="file in row.proof_files || []" :key="file.file_id"><a v-if="file.can_download && file.available" class="proof-file" :title="file.name" :href="proofFileUrl(file.file_id)"><FileText :size="14" /><span>{{ file.name }}</span></a><span v-else class="proof-file muted" :title="file.available ? '无原件下载权限' : '原件已清理'"><FileText :size="14" /><span>{{ file.name }}</span></span></template>
              <span v-if="!row.evidence_images?.length && !row.proof_files?.length" class="muted">—</span>
            </td>
            <td class="row-actions"><button class="icon-button" :title="row.editable ? '编辑记录' : '查看记录'" :aria-label="row.editable ? '编辑记录' : '查看记录'" @click="toggleRowEditor(row,$event)"><Pencil v-if="row.editable" :size="16" /><Eye v-else :size="16" /></button><button v-if="row.rollbackable" class="link" @click="requestRollback(row.row_id)">回退</button><button v-if="row.editable && !String(row.status).startsWith('excluded_')" class="icon-button danger-icon" title="从待办排除" aria-label="从待办排除" @click="toggleExcluded(row,true)"><Trash2 :size="16" /></button><button v-if="row.restorable" class="link" @click="restoreRows([row.row_id])">恢复</button></td>
          </tr>
          <Teleport to="body"><div v-if="editingRowId === row.row_id" class="row-editor-overlay" @click.self="closeRowEditor">
            <section class="row-editor" role="dialog" aria-modal="true" :aria-label="`编辑 ${row.scope}楼 ${row.room} ${row.rack}`" tabindex="-1">
              <header><div><span>机柜记录</span><strong>{{ row.scope || '-' }}楼 · {{ row.room || '-' }}包间 · {{ row.rack || '机柜未填写' }}</strong></div><button class="icon-button" title="关闭编辑" aria-label="关闭编辑" @click="closeRowEditor"><X :size="17" /></button></header>
              <div v-if="row.evidence_time_conflict" class="notice warning"><span>证明截图时间不一致，待人工核对</span><button :disabled="saving || !row.editable || !row.actual || recognizingEvidence" @click="reviewProofTimes(row)">确认当前时间无误</button></div>
              <div v-if="row.evidence_business_conflict" class="notice warning"><span>截图的操作或结果与本行不一致，请核对原图及当前填写内容。</span><button :disabled="saving || !row.editable || recognizingEvidence" @click="reviewProofBusiness(row)">确认采用当前操作与结果</button></div>
              <div v-if="error" class="notice danger" role="alert"><span>{{ error }}</span><button v-if="saveFailed && !saveConflicts.length" :disabled="saving" @click="saveChanges()">重试保存</button></div>
              <section v-if="saveConflicts.length" class="save-conflicts" role="alert"><strong>请选择保留的内容</strong><div v-for="conflict in saveConflicts" :key="conflict.row_id + conflict.key"><span>{{ conflict.title }} · {{ fieldLabels[conflict.key] }}</span><span>本次：{{ conflict.local || '空白' }}；已保存：{{ conflict.remote || '空白' }}</span><button :disabled="!conflict.canKeep" @click="resolveSaveConflict(conflict,true)">保留本次</button><button @click="resolveSaveConflict(conflict,false)">采用已保存</button></div></section>
              <div class="editor-workspace" :class="{ 'with-proof': showProofPanel && row.evidence_images?.length }"><div class="editor-grid" @change="queueAutoSave">
                <fieldset><legend>机柜定位</legend><label><span>楼栋</span><select v-model="row.scope" :disabled="!row.editable" :class="{ corrected: corrected(row,'scope') }"><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select></label><label><span>包间</span><input v-model="row.room" :disabled="!row.editable" :class="{ corrected: corrected(row,'room') }" /></label><label><span>机柜</span><input v-model="row.rack" :disabled="!row.editable" :class="{ corrected: corrected(row,'rack') }" /></label><label><span>供应商机柜号</span><input v-model="row.supplier_rack" :disabled="!row.editable" :class="{ corrected: corrected(row,'supplier_rack') }" /></label></fieldset>
                <fieldset><legend>机柜资料</legend><label><span>机柜类型</span><select v-model="row.rack_type" :disabled="!row.editable" :class="{ corrected: corrected(row,'rack_type') }"><option value="">请选择</option><option>网络机柜</option><option>服务器机柜</option></select></label><label><span>类型明细</span><input v-model="row.type_detail" :disabled="!row.editable" :class="{ corrected: corrected(row,'type_detail') }" /></label><label v-if="row.current_rack_type && row.rack_type !== row.current_rack_type" class="wide-field"><span>类型差异处理</span><select v-model="row.type_resolution" :disabled="!row.editable"><option value="">请选择</option><option value="keep_current">沿用当前类型 {{ row.current_rack_type }}</option><option value="sync_current">同步修正当前机柜类型</option></select></label></fieldset>
                <fieldset><legend>操作信息</legend><div v-if="row.current_power_state" class="state-line"><span :class="'power-state ' + row.current_power_state">当前：{{ powerStateLabel(row.current_power_state) }}</span><small>{{ row.inference || '操作类型可人工调整' }}</small></div><label class="wide-field"><span>操作类型</span><select v-model="row.action" :disabled="!row.editable" :class="{ corrected: corrected(row,'action') }"><option value="">请选择</option><option v-if="row.action && !allowedRowActions(row).includes(row.action)" :value="row.action" disabled>{{ row.action }}（原文件值，当前状态不允许）</option><option v-for="action in allowedRowActions(row)" :key="action">{{ action }}</option></select></label><label><span>期望完成时间{{ batch.source === 'notice' ? '（选填）' : '' }}</span><input :value="localDate(row.expected)" type="datetime-local" step="1" :disabled="!row.editable" :class="{ corrected: corrected(row,'expected') }" @input="row.expected = inputDate($event)" /></label><label><span>实际完成时间</span><input :value="localDate(row.actual)" type="datetime-local" step="1" :disabled="!row.editable" :class="{ corrected: corrected(row,'actual') }" @input="row.actual = inputDate($event)" /></label><label><span>结果</span><select v-model="row.result" :disabled="!row.editable" :class="{ corrected: corrected(row,'result') }"><option value="">请选择</option><option>成功</option><option>失败</option></select></label><label v-if="row.result === '失败'" class="wide-field"><span>失败原因</span><input v-model="row.failure_reason" :disabled="!row.editable" maxlength="1000" required placeholder="填写本次操作失败原因" /></label></fieldset>
              </div>
              <aside v-if="showProofPanel && row.evidence_images?.length" class="proof-panel"><button v-for="(id,index) in row.evidence_images" :key="id" class="proof-original" :aria-label="'放大证明 ' + (index + 1)" @click="openProofImages(row,index)"><img :src="imageUrl(id,true)" alt="机柜证明原图" loading="lazy" /></button></aside></div>
              <div v-if="row.evidence_images?.length || row.proof_files?.length || row.attempts?.length" class="row-evidence-content"><div v-if="row.evidence_images?.length" class="row-image-list"><button v-for="(id,index) in row.evidence_images" :key="id" class="image-button" :aria-label="'查看机柜截图 ' + id.slice(0,8)" @click="openProofImages(row,index)"><img :src="imageUrl(id)" alt="机柜上下电确认截图" loading="lazy" /></button></div><div v-if="row.proof_files?.length" class="row-documents"><template v-for="file in row.proof_files" :key="file.file_id"><a v-if="file.can_download && file.available" class="proof-file" :href="proofFileUrl(file.file_id)"><FileText :size="15" /><span>{{ file.name }}</span></a><span v-else class="proof-file">{{ file.name }}</span></template></div><small v-if="row.attempts?.length">已保留 {{ row.attempts.length }} 次历史确认及回退记录；再次确认将使用新操作标识。</small></div>
              <div class="editor-footer"><div class="source-audit"><span>{{ row.file_name || '待办记录' }}</span><small v-if="row.application_ids?.length">申请单 {{ applicationSummary(row) }}</small><details v-if="row.raw_text" class="edit-audit"><summary>粘贴原文</summary><pre class="text-original">{{ row.raw_text }}</pre></details><details v-if="visibleEdits(row).length" class="edit-audit"><summary>查看 {{ visibleEdits(row).length }} 项更正记录</summary><small v-for="(edit,index) in visibleEdits(row)" :key="index">{{ fieldLabels[edit.field] || edit.field }}：原值“{{ edit.before || '未填写' }}” → “{{ edit.after === '' ? '已清空' : edit.after }}”</small></details></div><div class="actions"><button v-if="row.evidence_images?.length" @click="showProofPanel = !showProofPanel"><Eye :size="16" />{{ showProofPanel ? '收起原图' : '核对原图' }}</button><button :disabled="saving" @click="nextProblemRow">下一条异常</button><button @click="closeRowEditor">关闭</button><span class="save-indicator">{{ saving ? '正在保存…' : saveConflicts.length ? '等待冲突处理' : saveFailed ? '未保存，请重试' : dirtyCount ? '等待保存' : '已保存' }}</span></div></div>
            </section>
          </div></Teleport>
        </template>
      </tbody></table></div>
      <footer v-if="batch.rows?.length" class="pagination"><span>共 {{ filteredRows.length }} 条</span><button :disabled="rowPage <= 1" @click="rowPage--"><ChevronLeft :size="16" /></button><template v-for="page in rowPages" :key="page"><button :class="{ active: rowPage === page }" @click="rowPage = page">{{ page }}</button></template><button :disabled="rowPage >= rowPageCount" @click="rowPage++"><ChevronRight :size="16" /></button></footer>
    </template>

    <template v-else>
      <div v-if="list.handoff_errors?.length" class="notice danger" role="alert"><AlertTriangle :size="18" /><span><b>{{ list.handoff_errors.length }} 条上下电通告联动失败</b><small>{{ list.handoff_errors[0].notice_type }} · {{ list.handoff_errors[0].target_record_id }}：{{ list.handoff_errors[0].error }}</small></span><button v-if="isAdmin" :disabled="handoffRetrying" @click="retryHandoffs"><RefreshCw :size="14" :class="{spin:handoffRetrying}" />{{ handoffRetrying ? '重试中' : '重试联动' }}</button></div>
      <section class="filters"><select v-model="listScope"><option value="">全部楼栋</option><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select><select v-model="listStatus"><option value="todo">仅看待办</option><option value="notice_rollback_error">通告回退异常</option><option value="">全部状态</option><option value="pending">待处理</option><option value="running">提交中</option><option value="partial">部分完成</option><option value="completed">已完成</option><option value="rolled_back">已回退</option><option value="cancelled">已作废</option><option value="failed">失败</option></select><input v-model="listFrom" type="date" aria-label="开始日期" /><input v-model="listTo" type="date" aria-label="结束日期" /><button @click="loadList(1)"><Search :size="16" />查询</button></section>
      <div class="table-wrap"><table class="batch-list-table"><thead><tr><th>批次</th><th>来源</th><th>楼栋</th><th>状态</th><th>总数</th><th>待处理</th><th>可确认</th><th>异常</th><th>创建时间</th><th>操作</th></tr></thead><tbody><tr v-for="item in list.items || []" :key="item.batch_id" :class="{ 'todo-batch': item.is_todo }"><td class="batch-name"><strong>{{ item.title || item.batch_id.slice(0,8) }}</strong><small>{{ (item.rooms || []).join('、') }}</small><small v-if="item.actual_from">{{ item.actual_from }}{{ item.actual_to !== item.actual_from ? ' ~ ' + item.actual_to : '' }}</small><small>批次 {{ item.batch_id.slice(0,8) }}</small><span v-if="item.is_todo" class="todo-mark">待办</span></td><td>{{ sourceLabel(item.source) }}</td><td>{{ (item.scopes || []).map((value: string) => value + '楼').join('、') || '识别中' }}</td><td><span :class="'status ' + item.status">{{ item.source_notice_deleted ? '来源通告已删除' + (item.notice_rollback_error ? ' · 回退异常' : '') : item.notice_rollback_error ? '回退异常' : statusLabel(item.status) }}</span></td><td>{{ item.stats?.total || 0 }}</td><td><strong v-if="item.pending_rows" class="todo-count">{{ item.pending_rows }}</strong><span v-else-if="item.is_todo">{{ item.pending_label }}</span><span v-else>—</span></td><td>{{ item.stats?.confirmable || 0 }}</td><td>{{ (item.stats?.duplicate || 0) + (item.stats?.conflict || 0) + (item.stats?.invalid || 0) + (item.stats?.failed || 0) + (item.stats?.rollback_failed || 0) + (item.stats?.rollback_blocked || 0) }}</td><td>{{ item.created_at }}</td><td><button class="link" @click="openBatch(item.batch_id)">{{ item.is_todo ? '处理待办' : '查看历史' }}</button></td></tr></tbody></table><p v-if="!loading && !list.total" class="empty">{{ listStatus === 'todo' ? '当前没有待处理批次，可切换“全部状态”查看历史。' : '暂无上下电待办' }}</p></div>
      <footer v-if="list.total" class="pagination"><span>共 {{ list.total }} 个批次</span><button :disabled="list.page <= 1" @click="loadList(list.page - 1)"><ChevronLeft :size="16" /></button><button v-for="page in listPages" :key="page" :class="{ active: list.page === page }" @click="loadList(page)">{{ page }}</button><button :disabled="list.page >= listPageCount" @click="loadList(list.page + 1)"><ChevronRight :size="16" /></button></footer>
    </template>

    <ConfirmDialog :open="Boolean(confirmDialog)" tone="warning" :title="confirmTitle" :message="confirmMessage" :confirm-label="confirmLabel" @resolve="resolveConfirm" />
    <ConfirmDialog :open="discardOpen" tone="warning" title="放弃未保存的批次修改？" message="继续后，本页尚未保存的字段修改会丢失。" confirm-label="放弃修改" cancel-label="继续编辑" @resolve="resolveDiscard" />
    <ConfirmDialog :open="draftRestoreOpen" title="恢复未保存的批次更正？" message="检测到上次页面关闭前尚未成功保存的机柜字段。恢复后会继续自动保存；也可以丢弃草稿并使用服务器版本。" confirm-label="恢复更正" cancel-label="丢弃草稿" @resolve="resolveBatchDraft" />
    <div v-if="previewImage" class="image-preview" role="dialog" aria-modal="true" aria-label="确认截图原图" @click.self="previewImage = ''"><button class="image-close" aria-label="关闭原图" @click="previewImage = ''"><X :size="20" /></button><img :src="previewImage" alt="确认截图原图" /><div v-if="previewGallery.length > 1" class="preview-navigation"><button :disabled="previewIndex <= 0" aria-label="上一张证明" @click="moveProof(-1)"><ChevronLeft :size="20" /></button><span>{{ previewIndex + 1 }} / {{ previewGallery.length }}</span><button :disabled="previewIndex >= previewGallery.length - 1" aria-label="下一张证明" @click="moveProof(1)"><ChevronRight :size="20" /></button></div></div>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { AlertTriangle, ChevronLeft, ChevronRight, ClipboardList, ClipboardPaste, Eye, FileText, Files, Loader2, Pencil, Plus, RefreshCw, Save, ScanText, Search, Trash2, Upload, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { randomHexId, resilientStorage } from '../browserStorage';
import { navigate, registerNavigationGuard } from '../navigation';
import type { ScopeOption } from '../types';
import ConfirmDialog from './ConfirmDialog.vue';
import VnetBackButton from './VnetBackButton.vue';

const props = defineProps<{ scope: string; scopeOptions: ScopeOption[]; isAdmin: boolean; userId?: string }>();
const api = '/api/cabinet-power';
const params = new URLSearchParams(window.location.search);
const batchId = String(params.get('batch_id') || '');
const mode = String(params.get('mode') || '');
const actions = ['上正式电','上测试电','测试电转正式电','正式电转测试电','下正式电','下测试电'];
const editableFields = ['scope','room','rack','supplier_rack','rack_type','type_detail','action','expected','actual','result','failure_reason','type_resolution'];
const fieldLabels: Dict = {scope:'楼栋',room:'包间',rack:'机柜',supplier_rack:'供应商机柜号',rack_type:'机柜类型',type_detail:'类型明细',action:'操作类型',expected:'期望完成时间',actual:'实际完成时间',result:'结果',failure_reason:'失败原因',type_resolution:'类型处理',excluded:'排除状态',evidence_images:'证明截图',evidence_time_review:'截图时间核对'};
const read = (path: string, query: Dict = {}, timeoutMs = 90000, signal?: AbortSignal) => requestJson(`${api}/${path}?${new URLSearchParams(query as Record<string,string>)}`, { timeoutMs, signal });
const write = (path: string, body: Dict, method = 'POST') => requestJson(`${api}/${path}`, { method, body: JSON.stringify(body), timeoutMs: 90000 });
const backTarget = computed(() => params.get('origin') === 'cabinet' ? (props.scope ? `/cabinet-power?scope=${props.scope}` : '/cabinet-power') : batchId || mode === 'new' ? `/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? {scope:props.scope}:{}),...(params.get('status')==='todo'?{status:'todo'}:{}) })}` : props.scope ? `/cabinet-power?scope=${props.scope}` : '/cabinet-power');
const buildingOptions = computed(() => {
  const map = new Map<string,string>();
  for (const item of props.scopeOptions || []) { const value = String(item.value || '').toUpperCase(); if (/^[A-E]$/.test(value)) map.set(value,item.label || value + '楼'); }
  if (props.isAdmin) for (const value of 'ABCDE') map.set(value,value + '楼');
  if (/^[A-E]$/.test(props.scope)) map.set(props.scope,props.scope + '楼');
  return [...map].map(([value,label]) => ({ value,label }));
});
const loading = ref(false), uploading = ref(false), saving = ref(false), saveFailed = ref(false), error = ref(''), message = ref(''), handoffRetrying = ref(false);
const uploadingEvidence = ref(false), evidenceDragging = ref(false), imageUploadDone = ref(0), imageUploadTotal = ref(0), previewImage = ref('');
const imageSelections = reactive<Record<string,string>>({});
const imageSearches = reactive<Record<string,string>>({});
const saveConflicts = ref<Dict[]>([]), showProofPanel = ref(false);
const correction = ref<Dict | null>(null), correctionDirectory = ref<Dict[]>([]);
const imageInput = ref<HTMLInputElement>();
const batch = ref<Dict>({}), list = ref<Dict>({}), baseline = new Map<string,string>();
const createMode = ref('pdf'), pdfFiles = ref<File[]>([]), fileInput = ref<HTMLInputElement>(), dragging = ref(false), manualScope = ref(props.scope || buildingOptions.value[0]?.value || 'A');
const imageFiles = ref<File[]>([]), createImageInput = ref<HTMLInputElement>(), imageBatchId = ref('');
const imagePreviewUrls = new Map<File,string>();
const textInput=ref(''),textError=ref(''),textFragments=ref<Dict[]>([]),textPage=ref(1),textRequestId=ref(randomHexId());
const textSource=ref(''),textSearch=ref(''),textReview=ref(''),textPageSize=ref(25),textDeleteId=ref('');
const textTable=ref<HTMLElement>();
const textRows=computed<Dict[]>(()=>textFragments.value.flatMap((fragment,index)=>(fragment.rows||[]).map((row:Dict)=>{row._fragmentNumber=index+1;return row;})));
const textPending=computed(()=>textFragments.value.filter(item=>item.loading).length);
const textDuplicateIds=computed(()=>{const seen=new Set<string>(),duplicates=new Set<string>();for(const row of textRows.value){const key=[row.scope,row.room,row.rack,row.action,row.actual].join('|');if(row.actual&&seen.has(key))duplicates.add(row.row_id);seen.add(key);}return duplicates;});
const textHasSupplier=computed(()=>textRows.value.some(row=>row.supplier_rack));
const textScopeRows=computed(()=>{
  const query=textSearch.value.trim().toUpperCase();
  return textRows.value.filter(row=>(!textSource.value||row.text_id===textSource.value)
    &&(!query||`${row.scope} ${row.room} ${row.rack} ${row.supplier_rack} ${row.action} ${row.source_system_name}`.toUpperCase().includes(query)));
});
const textIssueCount=computed(()=>textScopeRows.value.filter(row=>textRowMessage(row)).length);
const textScopeDuplicateCount=computed(()=>textScopeRows.value.filter(row=>textDuplicateIds.value.has(row.row_id)).length);
const textFilteredRows=computed(()=>textScopeRows.value.filter(row=>!textReview.value
  ||(textReview.value==='duplicates'?textDuplicateIds.value.has(row.row_id):Boolean(textRowMessage(row)))));
const textPageCount=computed(()=>Math.max(1,Math.ceil(textFilteredRows.value.length/textPageSize.value)));
const textPagedRows=computed(()=>textFilteredRows.value.slice((textPage.value-1)*textPageSize.value,textPage.value*textPageSize.value));
watch(textPageCount,value=>{textPage.value=Math.min(textPage.value,value);},{flush:'sync'});
watch(textPage,async()=>{await nextTick();if(document.activeElement?.closest('.text-footer'))textTable.value?.scrollIntoView({block:'start'});});
const common = reactive({ action:'', expected:'', actual:'', result:'成功', failure_reason:'' }), bulk = reactive({ action:'', expected:'', actual:'', result:'', failure_reason:'', type_resolution:'' });
const directory = ref<Dict[]>([]), directorySearch = ref(''), pickedRacks = ref<string[]>([]), pastedRows = ref(''), manualRows = ref<Dict[]>([]);
const selectedRows = ref<string[]>([]), editingRowId = ref(''), rowPage = ref(1), rowScopeFilter = ref(props.scope || ''), rowStatusFilter = ref(''), rowQuickFilter = ref(''), imagePage = ref(1), listScope = ref(props.scope || ''), listStatus = ref(params.get('status')==='todo'?'todo':''), listFrom = ref(''), listTo = ref('');
const confirmDialog = ref(''), discardOpen = ref(false);
const draftRestoreOpen = ref(false);
const cleanupFileId = ref(''), rollbackRowId = ref(''), imageDeleteId = ref('');
let pollTimer: number | undefined, saveTimer: number | undefined, savePromise: Promise<boolean> | undefined, disposed = false, pendingNavigation: (() => void) | undefined, removeGuard: (() => void) | undefined;
let editorTrigger:HTMLElement|undefined;
let listSequence=0, listAbort:AbortController|undefined;
let draftChecked=false, discardDraft=false, pendingDraft:Dict|undefined;
const draftStorage=resilientStorage('sessionStorage',()=>{error.value='浏览器无法保存批次恢复信息，请保持页面打开直到保存完成。';});
const draftKey=`cabinet-batch-draft:${props.userId || 'session'}:${batchId}`;
const textDraftKey=`cabinet-text-draft:${props.userId||'session'}:${props.scope||'all'}`;
function persistTextDraft():void{
  if(batchId||mode!=='new')return;
  if(!textFragments.value.length){draftStorage.removeItem(textDraftKey);return;}
  draftStorage.setItem(textDraftKey,JSON.stringify({fragments:textFragments.value,request_id:textRequestId.value}));
}
function restoreTextDraft():void{
  try{const saved=JSON.parse(draftStorage.getItem(textDraftKey)||'{}');if(Array.isArray(saved.fragments)&&saved.fragments.length){textFragments.value=saved.fragments;textRequestId.value=saved.request_id||randomHexId();createMode.value='text';for(const fragment of textFragments.value)if(fragment.loading)void recognizeTextFragment(fragment);}}
  catch{draftStorage.removeItem(textDraftKey);}
}
function textEdited(row:Dict):void{row._edited=true;textRequestId.value=randomHexId();persistTextDraft();}
function textRowMessage(row:Dict):string{
  if(textDuplicateIds.value.has(row.row_id))return '本次粘贴中重复';
  if(!row.result)return '结果未复制，待核对';
  if(row.result==='失败'&&!row.failure_reason)return '需填写失败原因';
  if(row._edited)return '已更正，创建时复核';
  return (row.issues||[]).map((issue:Dict)=>issue.message).join('；');
}
function pasteConfirmationText(event:ClipboardEvent):void{
  const value=event.clipboardData?.getData('text/plain')||'';
  if(!value.trim()||uploading.value)return;event.preventDefault();void appendText(value);
}
async function appendText(value:string):Promise<void>{
  if(!value.trim()||uploading.value)return;
  if(textFragments.value.length>=2000||new TextEncoder().encode(value).length>200000){textError.value='每批最多2000段文本，单次不超过200KB';return;}
  if(new TextEncoder().encode(textFragments.value.map(item=>item.text).join('')+value).length>1024*1024){error.value='整批文本不能超过1MiB';return;}
  const fragment=reactive<Dict>({id:randomHexId(),text:value,rows:[],loading:true,error:''});
  textFragments.value.push(fragment);textInput.value='';textError.value='';textRequestId.value=randomHexId();persistTextDraft();
  await recognizeTextFragment(fragment);
}
async function recognizeTextFragment(fragment:Dict):Promise<void>{
  fragment.loading=true;fragment.error='';
  try{const data=await write('batches/text-preview',{sources:[{id:fragment.id,text:fragment.text}]});
    if(!textFragments.value.some(item=>item.id===fragment.id))return;
    if(textRows.value.length-fragment.rows.length+(data.rows||[]).length>2000)throw new Error('单批最多2000条记录，请移除部分文本');
    fragment.rows=data.rows||[];textSource.value='';textSearch.value='';textReview.value='';textPage.value=textPageCount.value;error.value='';
  }catch(exc:any){
    textError.value=exc.message||'文本识别失败，请重新粘贴';
    textFragments.value=textFragments.value.filter(item=>item.id!==fragment.id);
    textPage.value=Math.min(textPage.value,textPageCount.value);
  }finally{fragment.loading=false;persistTextDraft();}
}
function removeTextFragment(id:string):void{textFragments.value=textFragments.value.filter(item=>item.id!==id);if(textSource.value===id)textSource.value='';textPage.value=Math.min(textPage.value,textPageCount.value);textRequestId.value=randomHexId();persistTextDraft();}
function resolveTextDelete(confirmed:boolean):void{if(confirmed)removeTextFragment(textDeleteId.value);textDeleteId.value='';}
function removeTextRow(row:Dict):void{const fragment=textFragments.value.find(item=>item.id===row.text_id);if(!fragment)return;fragment.rows=fragment.rows.filter((item:Dict)=>item.row_id!==row.row_id);if(!fragment.rows.length)removeTextFragment(fragment.id);textPage.value=Math.min(textPage.value,textPageCount.value);textRequestId.value=randomHexId();persistTextDraft();}
async function createFromText():Promise<void>{
  if(uploading.value||textPending.value||!textRows.value.length)return;uploading.value=true;error.value='';persistTextDraft();
  try{const data=await write('batches',{source:'text',request_id:textRequestId.value,sources:textFragments.value.map(({id,text})=>({id,text})),
    rows:textRows.value.map(row=>({text_id:row.text_id,text_row:row.text_row,...Object.fromEntries(editableFields.map(key=>[key,row[key]||'']))}))});
    textFragments.value=[];draftStorage.removeItem(textDraftKey);openBatch(data.batch_id);
  }catch(exc:any){error.value=exc.message||'待办创建失败，输入已保留，可重试同一次创建';}finally{uploading.value=false;}
}

const filteredDirectory = computed(() => directory.value.filter(row => !directorySearch.value || `${row.room} ${row.rack}`.toUpperCase().includes(directorySearch.value.toUpperCase())).slice(0,500));
const filteredRows = computed<Dict[]>(() => (batch.value.rows || []).filter((row:Dict) =>
  (!rowScopeFilter.value || row.scope===rowScopeFilter.value)
  && (!rowStatusFilter.value || row.status===rowStatusFilter.value || rowStatusFilter.value==='excluded'&&String(row.status).startsWith('excluded_'))
  && (!rowQuickFilter.value || rowQuickFilter.value==='missing_actual'&&!row.actual || rowQuickFilter.value==='evidence_pending'&&!row.evidence_images?.length || rowQuickFilter.value==='issues'&&(row.issues?.length||row.error))));
const rowPageCount = computed(() => Math.max(1,Math.ceil(filteredRows.value.length/50)));
const pagedRows = computed<Dict[]>(() => filteredRows.value.slice((rowPage.value-1)*50,rowPage.value*50));
const rowPages = computed(() => pageNumbers(rowPage.value,rowPageCount.value));
const listPageCount = computed(() => Math.max(1,Math.ceil((list.value.total || 0)/(list.value.page_size || 20))));
const listPages = computed(() => pageNumbers(list.value.page || 1,listPageCount.value));
const allVisibleSelected = computed(() => {const rows=pagedRows.value.filter(row=>row.editable||row.confirmable||row.restorable);return rows.length>0&&rows.every(row=>selectedRows.value.includes(row.row_id));});
const dirtyRows = computed(() => (batch.value.rows || []).filter((row: Dict) => baseline.get(row.row_id) !== rowSignature(row) && row.editable));
const dirtyCount = computed(() => dirtyRows.value.length);
const rollbackableCount = computed(() => (batch.value.rows || []).filter((row:Dict) => row.rollbackable).length);
const restorableSelectedCount = computed(() => (batch.value.rows || []).filter((row:Dict)=>selectedRows.value.includes(row.row_id)&&row.restorable).length);
const excludableSelectedCount = computed(() => (batch.value.rows || []).filter((row:Dict)=>selectedRows.value.includes(row.row_id)&&row.editable&&!String(row.status).startsWith('excluded_')).length);
const cancellableCount = computed(() => (batch.value.rows || []).filter((row:Dict)=>!['completed','rollback_failed','rollback_blocked','excluded_cancelled'].includes(row.status)).length || (batch.value.source==='image' && !batch.value.rows?.length ? 1 : 0));
const activeImages = computed<Dict[]>(() => (batch.value.images || []).filter((item:Dict)=>!item.deleted_at));
const deletedImages = computed<Dict[]>(() => (batch.value.images || []).filter((item:Dict)=>item.deleted_at));
const selectableImageRows = computed<Dict[]>(() => (batch.value.rows || []).filter((row:Dict) => row.editable && !String(row.status).startsWith('excluded_')));
const imagePageCount = computed(() => Math.max(1,Math.ceil(activeImages.value.length/12)));
const pagedActiveImages = computed(() => activeImages.value.slice((imagePage.value-1)*12,imagePage.value*12));
const confirmProgress = computed(() => { const rows=batch.value.rows || []; const active=rows.filter((row:Dict)=>['queued','writing'].includes(row.status)); const done=rows.filter((row:Dict)=>['completed','failed'].includes(row.status)); return {done:done.length,total:Math.max(1,active.length+done.length)}; });
const rollbackProgress = computed(() => { const rows=batch.value.rows || []; const active=rows.filter((row:Dict)=>['rollback_queued','rolling_back'].includes(row.status)); const done=rows.filter((row:Dict)=>['rolled_back','rollback_failed','rollback_blocked'].includes(row.status)); return {active:active.length>0,done:done.length,total:Math.max(1,active.length+done.length)}; });
const warningsPending = computed(() => Boolean(batch.value.blocking_warnings?.length && !batch.value.warnings_acknowledged));
const attentionCount = computed(() => ['duplicate','conflict','invalid','failed'].reduce((total,key) => total + Number(batch.value.stats?.[key] || 0),0));
const recognizingEvidence = computed(() => activeImages.value.some((image: Dict) => image.status === 'recognizing'));
const recognitionProgress = computed(() => ({ total:activeImages.value.length,
  done:activeImages.value.filter(image=>image.status!=='recognizing').length,
  failed:activeImages.value.filter(image=>image.status==='failed').length,
  current:activeImages.value.find(image=>image.status==='recognizing'&&image.phase==='running')?.name || '' }));
const previewGallery = ref<string[]>([]);
const previewIndex = computed(() => previewGallery.value.indexOf(previewImage.value));
function openImage(id:string):void { previewGallery.value=[imageUrl(id,true)]; previewImage.value=previewGallery.value[0] || ''; }
function openProofImages(row:Dict,index=0):void { previewGallery.value=(row.evidence_images || []).map((id:string)=>imageUrl(id,true)); previewImage.value=previewGallery.value[index] || ''; }
function moveProof(offset:number):void { previewImage.value=previewGallery.value[previewIndex.value+offset] || previewImage.value; }
function proofFileUrl(id:string):string { return `${api}/batches/${encodeURIComponent(batchId)}/files/${encodeURIComponent(id)}`; }
const showExpectedColumn = computed(() => batch.value.source !== 'notice' || (batch.value.rows || []).some((row: Dict) => Boolean(row.expected)));
const showRackColumn = computed(() => (batch.value.rows || []).some((row: Dict) => hasCabinetValue(row.rack)));
const showSupplierRackColumn = computed(() => (batch.value.rows || []).some((row: Dict) => hasCabinetValue(row.supplier_rack)));
const confirmTitle = computed(() => confirmDialog.value === 'delete_empty' ? '删除这个空批次？' : confirmDialog.value === 'image' ? '删除这张确认截图？' : confirmDialog.value === 'rollback' ? '回退已写入的机柜记录？' : confirmDialog.value === 'overlap' ? '清空完全重叠的待办行？' : confirmDialog.value === 'all' ? '确认整批有效记录？' : confirmDialog.value === 'file' ? '清理原确认单？' : confirmDialog.value === 'warnings' ? '确认以当前机柜明细为准？' : '作废尚未提交的记录？');
const confirmMessage = computed(() => confirmDialog.value === 'delete_empty' ? '该批次没有机柜记录。删除后会从待办列表移除，原截图共享缓存不会影响其他批次。' : confirmDialog.value === 'image' ? '将从当前待办移除截图，并解除尚未提交的机柜关联。共享的本地图片缓存不会清理，其他批次或已确认记录不受影响。' : confirmDialog.value === 'rollback' ? '将逐柜核验云端并恢复到本批写入前。存在后续操作或云端冲突的机柜会跳过，其他机柜继续回退。' : confirmDialog.value === 'overlap' ? '只排除当前批次中的重复行，既有台账和多维表不会被修改；排除记录仍保留审计并可恢复。' : confirmDialog.value === 'all' ? `将确认本批共 ${batch.value.stats?.confirmable || 0} 条有效记录，异常行会留在待办。` : confirmDialog.value === 'file' ? '清理后无法再次下载原PDF，已保存的识别值、更正审计和正式台账不受影响。' : confirmDialog.value === 'warnings' ? '数量、重复或目录匹配存在异常。确认后将允许对当前有效行正式提交，后续修改机柜明细会自动取消本次确认。' : '已完成记录保留，其他尚未提交行将标记为已作废；之后仍可选择未提交行恢复。');
const confirmLabel = computed(() => confirmDialog.value === 'delete_empty' ? '删除空批次' : confirmDialog.value === 'image' ? '删除截图' : confirmDialog.value === 'rollback' ? '确认回退' : confirmDialog.value === 'overlap' ? '清空重叠数据' : confirmDialog.value === 'all' ? '确认整批' : confirmDialog.value === 'file' ? '清理原确认单' : confirmDialog.value === 'warnings' ? '确认已核对' : '作废未提交行');

function pageNumbers(current: number,total: number): number[] { return [...new Set([1,total,...Array.from({length:5},(_,index)=>current+index-2)])].filter(value=>value>=1&&value<=total).sort((a,b)=>a-b); }
function statusLabel(status: string): string { return ({ waiting:'等待',parsing:'解析中',recognizing:'识别中',pending:'待处理',ready:'可确认',duplicate:'重复',conflict:'冲突',invalid:'无效',queued:'排队中',writing:'写入中',running:'处理中',partial:'部分完成',completed:'已完成',failed:'失败',rolled_back:'已回退',rollback_queued:'回退排队中',rolling_back:'回退中',rollback_failed:'回退失败',rollback_blocked:'无法回退',cancelled:'已作废',excluded_duplicate:'已排除重复',excluded_manual:'已排除',excluded_image:'截图已删除',excluded_cancelled:'已作废' } as Dict)[status] || status || '未知'; }
function sourceLabel(source: string): string { return source === 'pdf' ? 'PDF识别' : source === 'image' ? '图片识别' : source === 'notice' ? '上下电通告' : source === 'text' ? '粘贴文本识别' : '手工批量'; }
function formatBytes(value: number): string { return value < 1024*1024 ? `${Math.ceil(value/1024)} KiB` : `${(value/1024/1024).toFixed(1)} MiB`; }
function localDate(value: string): string { return String(value || '').replace(' ','T'); }
function tableDate(value: string): string { return String(value || '').trim().replace('T',' ').replace(/(\d{2}:\d{2}):00$/, '$1') || '待填写'; }
function imageUrl(id:string,original=false):string { return `${api}/batches/${batchId}/images/${id}${original?'':'?thumbnail=1'}`; }
function imageLocked(image:Dict):boolean { return (batch.value.rows || []).some((row:Dict)=>row.evidence_images?.includes(image.image_id) && !row.editable); }
function inputDate(event: Event): string { return (event.target as HTMLInputElement).value.replace('T',' '); }
function closeRowEditor():void{editingRowId.value='';void saveChanges();void nextTick(()=>{if(editorTrigger?.isConnected)editorTrigger.focus();});}
function toggleRowEditor(row: Dict,event:MouseEvent): void { if(editingRowId.value===row.row_id)closeRowEditor();else{editorTrigger=event.currentTarget as HTMLElement;editingRowId.value=row.row_id;} }
function hasCabinetValue(value: unknown): boolean { return !['','-','/','null','[null]','none'].includes(String(value || '').trim().toLowerCase()); }
function powerStateLabel(value: string): string { return ({ formal:'正式电',test:'测试电',off:'未上电 / 已下电',unknown:'待核实' } as Dict)[value] || '待核实'; }
function allowedRowActions(row:Dict):string[] { if(!['pdf','image','text'].includes(batch.value.source))return actions; return ({off:['上正式电','上测试电'],test:['测试电转正式电','下测试电'],formal:['正式电转测试电','下正式电']} as Record<string,string[]>)[row.current_power_state] || []; }
function rowSignature(row: Dict): string { return JSON.stringify(Object.fromEntries(editableFields.map(key => [key,String(row[key] || '')]))); }
function corrected(row: Dict,key: string): boolean { return row.original && String(row[key] || '') !== String(row.original[key] || ''); }
function visibleEdits(row: Dict): Dict[] { return (row.edits || []).filter((edit: Dict) => edit.field !== 'order_time' && edit.field !== 'exclude_notice_summary'); }
function applicationSummary(row:Dict):string{return row.application_ids.length===1?row.application_ids[0]:`${row.application_ids[0]} 等 ${row.application_ids.length} 个`;}
function snapshotRows(): void { baseline.clear(); for (const row of batch.value.rows || []) baseline.set(row.row_id,rowSignature(row)); }
function rebaseBatch(fresh:Dict):void {
  const pending:{row_id:string,values:Dict}[]=[];
  const conflicts:Dict[]=[];
  for(const local of dirtyRows.value){
    const original=JSON.parse(baseline.get(local.row_id)||'{}');
    const remote=(fresh.rows||[]).find((row:Dict)=>row.row_id===local.row_id);
    const values:Dict={};
    for(const key of editableFields){
      const value=String(local[key]||''),before=String(original[key]||''),server=String(remote?.[key]||'');
      if(value===before || remote && value===server)continue;
      if(remote?.editable)values[key]=value;
      if(!remote?.editable || server!==before)conflicts.push({row_id:local.row_id,key,local:value,remote:server,canKeep:Boolean(remote?.editable),title:`${local.scope}楼 ${local.room}/${local.rack}`});
    }
    pending.push({row_id:local.row_id,values});
  }
  mergeBatchResponse(fresh,pending);saveConflicts.value=conflicts;syncImageSelections();persistBatchDraft();
}
function resolveSaveConflict(conflict:Dict,keep:boolean):void{
  const row=(batch.value.rows||[]).find((item:Dict)=>item.row_id===conflict.row_id);
  if(row && row.editable)row[conflict.key]=keep?conflict.local:conflict.remote;
  saveConflicts.value=saveConflicts.value.filter(item=>item!==conflict);persistBatchDraft();
  if(!saveConflicts.value.length){error.value='';saveFailed.value=false;queueAutoSave();}
}
async function nextProblemRow():Promise<void>{
  if(!await saveChanges())return;
  const rows=filteredRows.value, current=rows.findIndex(row=>row.row_id===editingRowId.value);
  const next=[...rows.slice(current+1),...rows.slice(0,current)].find(row=>row.issues?.length||row.error);
  if(next){rowPage.value=Math.floor(rows.indexOf(next)/50)+1;editingRowId.value=next.row_id;}
}
function mergeBatchResponse(saved:Dict,pending:{row_id:string,values:Dict}[]=[]):void{
  if(!saved.partial_rows)batch.value=saved;
  else{
    const rows=[...(batch.value.rows||[])],byId=new Map(rows.map((row:Dict,index:number)=>[row.row_id,index]));
    for(const row of saved.rows||[]){const index=byId.get(row.row_id);if(index===undefined)rows.push(row);else rows[index]=row;}
    const changedImages=new Map((saved.images || []).map((image:Dict)=>[image.image_id,image]));
    const images=(batch.value.images || []).map((image:Dict)=>changedImages.get(image.image_id) || image);
    const files=saved.files || batch.value.files;
    batch.value={...batch.value,...saved,rows,images,files};
  }
  snapshotRows();
  for(const item of pending){const row=(batch.value.rows||[]).find((entry:Dict)=>entry.row_id===item.row_id);if(row)Object.assign(row,item.values);}
}
function persistBatchDraft():void{
  if(!batchId){persistTextDraft();return;}
  if(discardDraft){draftStorage.removeItem(draftKey);return;}
  const rows=dirtyRows.value.map((row:Dict)=>({row_id:row.row_id,values:Object.fromEntries(editableFields.map(key=>[key,row[key] ?? '']))}));
  if(!rows.length&&!saveConflicts.value.length){draftStorage.removeItem(draftKey);return;}
  draftStorage.setItem(draftKey,JSON.stringify({batch_id:batchId,version:batch.value.version,rows,conflicts:saveConflicts.value,baseline:Object.fromEntries(baseline),saved_at:Date.now()}));
}
function offerBatchDraft():void{
  if(draftChecked||!batchId)return;
  draftChecked=true;
  try{
    const saved=JSON.parse(draftStorage.getItem(draftKey)||'{}');
    if(saved.batch_id===batchId&&Array.isArray(saved.rows)&&(saved.rows.length||saved.conflicts?.length)){pendingDraft=saved;draftRestoreOpen.value=true;}
  }catch{draftStorage.removeItem(draftKey);}
}
function resolveBatchDraft(restore:boolean):void{
  draftRestoreOpen.value=false;
  if(restore&&pendingDraft){
    const fresh=JSON.parse(JSON.stringify(batch.value));
    for(const item of pendingDraft.rows||[]){const row=(batch.value.rows||[]).find((entry:Dict)=>entry.row_id===item.row_id&&entry.editable);if(row&&item.values)Object.assign(row,item.values);}
    if(pendingDraft.baseline){baseline.clear();for(const [key,value] of Object.entries(pendingDraft.baseline))baseline.set(key,String(value));rebaseBatch(fresh);}
    saveConflicts.value.push(...(pendingDraft.conflicts||[]).filter((item:Dict)=>!saveConflicts.value.some(other=>other.row_id===item.row_id&&other.key===item.key)));
    message.value='已恢复未保存更正，正在继续自动保存';queueAutoSave();
  }else draftStorage.removeItem(draftKey);
  pendingDraft=undefined;
}
function syncImageSelections():void { for(const image of batch.value.images || [])for(const [index,item] of (image.suggestions || []).entries())if(item.row_id && !imageSelections[image.image_id + ':' + index])imageSelections[image.image_id + ':' + index]=item.row_id; }
function candidateRows(key:string):Dict[]{
  const query=String(imageSearches[key] || '').trim().toUpperCase();
  const selected=imageSelections[key];
  const matches=selectableImageRows.value.filter(row=>!query||`${row.scope} ${row.room} ${row.rack} ${row.action || ''}`.toUpperCase().includes(query)).slice(0,50);
  const current=selected&&selectableImageRows.value.find(row=>row.row_id===selected);
  return current&&!matches.some(row=>row.row_id===selected)?[current,...matches]:matches;
}
function openNew(): void { navigate(`/cabinet-power/batches?${new URLSearchParams({ ...((listScope.value || props.scope) ? {scope:listScope.value || props.scope}:{}),mode:'new',...(listStatus.value==='todo'?{status:'todo'}:{}) })}`); }
function openList(): void { navigate(`/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? {scope:props.scope}:{}), ...(params.get('status')==='todo'?{status:'todo'}:{}) })}`); }
function openBatch(id: string): void { navigate(`/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? {scope:props.scope}:{}),batch_id:id,...(listStatus.value==='todo'?{status:'todo'}:{}),...(params.get('origin')==='cabinet'?{origin:'cabinet'}:{}) })}`); }
function addPdfFiles(input: FileList | File[]): void {
  const incoming=Array.from(input); if (!incoming.length) return;
  if (incoming.some(file=>!file.name.toLowerCase().endsWith('.pdf') && file.type!=='application/pdf')) { error.value='只支持PDF确认单。'; return; }
  if (incoming.some(file=>file.size>10*1024*1024)) { error.value='单份PDF不能超过10MiB。'; return; }
  const unique=new Map(pdfFiles.value.map(file=>[`${file.name}\0${file.size}\0${file.lastModified}`,file]));
  for (const file of incoming) unique.set(`${file.name}\0${file.size}\0${file.lastModified}`,file);
  const selected=[...unique.values()];
  if (selected.length>10) { error.value='每批最多选择10份PDF。'; return; }
  if (selected.reduce((total,file)=>total+file.size,0)>30*1024*1024) { error.value='每批PDF合计不能超过30MiB。'; return; }
  pdfFiles.value=selected; error.value=''; message.value=`已选择 ${selected.length} 份PDF`;
}
function selectFiles(event: Event): void { const input=event.target as HTMLInputElement; addPdfFiles(input.files || []); input.value=''; }
function dropFiles(event: DragEvent): void { dragging.value=false; if (!uploading.value) addPdfFiles(event.dataTransfer?.files || []); }
function filePreview(file:File):string { let url=imagePreviewUrls.get(file);if(!url){url=URL.createObjectURL(file);imagePreviewUrls.set(file,url);}return url; }
function removeCreateImage(file:File):void {imageFiles.value=imageFiles.value.filter(item=>item!==file);const url=imagePreviewUrls.get(file);if(url)URL.revokeObjectURL(url);imagePreviewUrls.delete(file);}
function addCreateImages(input:FileList|File[]):void {
  const incoming=Array.from(input).filter(file=>/\.(png|jpe?g|webp)$/i.test(file.name)||['image/jpeg','image/png','image/webp'].includes(file.type));
  if(!incoming.length){error.value='只支持 JPG、PNG 或 WebP 图片';return;}
  if(incoming.some(file=>file.size>10*1024*1024)){error.value='单张图片不能超过10MiB';return;}
  const files=new Map(imageFiles.value.map(file=>[`${file.name}\0${file.size}\0${file.lastModified}`,file]));
  for(const file of incoming)files.set(`${file.name}\0${file.size}\0${file.lastModified}`,file);
  if(files.size>200){error.value='每批最多选择200张图片';return;}
  imageFiles.value=[...files.values()];error.value='';message.value=`已选择 ${imageFiles.value.length} 张图片`;
}
function selectCreateImages(event:Event):void{const input=event.target as HTMLInputElement;addCreateImages(input.files||[]);input.value='';}
function dropCreateImages(event:DragEvent):void{dragging.value=false;if(!uploading.value)addCreateImages(event.dataTransfer?.files||[]);}
function pasteFiles(event: ClipboardEvent): void {
  const target=event.target;
  if (target instanceof Element && target.closest('input,textarea,[contenteditable="true"]')) return;
  const data=event.clipboardData; if (!data) return;
  if(mode==='new'&&createMode.value==='text'&&!uploading.value){pasteConfirmationText(event);return;}
  const files=data.files.length ? Array.from(data.files) : Array.from(data.items).filter(item=>item.kind==='file').map(item=>item.getAsFile()).filter((file):file is File=>Boolean(file));
  if (!files.length) return;
  if (batchId) { const images=files.filter(file=>file.type.startsWith('image/')); if(images.length){event.preventDefault();void uploadEvidence(images);} return; }
  if (mode==='new' && createMode.value==='pdf' && !uploading.value) {event.preventDefault();addPdfFiles(files);}
  if (mode==='new' && createMode.value==='image' && !uploading.value && files.some(file=>file.type.startsWith('image/'))) {event.preventDefault();addCreateImages(files);}
}
function selectEvidence(event:Event):void { const input=event.target as HTMLInputElement;void uploadEvidence(Array.from(input.files || []));input.value=''; }
function dropEvidence(event:DragEvent):void { evidenceDragging.value=false;void uploadEvidence(Array.from(event.dataTransfer?.files || [])); }
async function uploadEvidence(files:File[]):Promise<void> {
  const images=files.filter(file=>/\.(png|jpe?g|webp)$/i.test(file.name) || ['image/jpeg','image/png','image/webp'].includes(file.type));
  if (!images.length) {error.value='未找到可识别的 JPG、PNG 或 WebP 图片';return;}
  if (uploadingEvidence.value || !await saveChanges()) return;
  if (images.some(file=>file.size>10*1024*1024)) {error.value='单张截图不能超过10MiB';return;}
  uploadingEvidence.value=true;imageUploadDone.value=0;imageUploadTotal.value=images.length;error.value='';
  try {
    batch.value=await uploadImageChunks(batchId,images);snapshotRows();syncImageSelections();
    message.value=`已接收 ${images.length} 张截图，正在识别`;schedulePoll();
  } catch(exc:any){error.value=exc.message||'截图上传失败';} finally{uploadingEvidence.value=false;}
}
async function uploadImageChunks(id:string,files:File[]):Promise<Dict>{
  imageUploadDone.value=0;imageUploadTotal.value=files.length;
  let data:Dict={};
  for(let offset=0;offset<files.length;){
    const chunk:File[]=[];let size=0;
    while(offset<files.length&&chunk.length<10&&size+files[offset].size<=30*1024*1024){chunk.push(files[offset]);size+=files[offset].size;offset++;}
    const form=new FormData();for(const file of chunk)form.append('files',file,file.name);
    data=await requestJson(`${api}/batches/${id}/images`,{method:'POST',body:form,timeoutMs:180000});
    imageUploadDone.value=offset;
  }
  return data;
}
async function applyEvidence(image:Dict,rowId:string,candidate:Dict|null,index:number):Promise<void> {
  if(!await saveChanges())return;
  const row=(batch.value.rows || []).find((item:Dict)=>item.row_id===rowId);
  if(!row)return;
  const sameRack=candidate && [candidate.scope,candidate.room,candidate.rack].join('/')===[row.scope,row.room,row.rack].join('/');
  const fields:Dict={};
  if(sameRack)for(const key of ['action','expected','actual','supplier_rack']){
    const value=String(candidate?.[key] || '');
    if(!value)continue;
    if(key==='expected'||key==='actual'){
      const parsed=new Date(value.replace(' ','T'));
      if(!Number.isNaN(parsed.getTime())&&(key==='expected'||parsed.getTime()<=Date.now()))fields[key]=value;
    }else if(!row[key])fields[key]=value;
  }
  saving.value=true;
  try{batch.value=await write(`batches/${batchId}/images/${image.image_id}/apply`,{version:batch.value.version,row_id:rowId,candidate_index:index,fields,attach:true});snapshotRows();message.value='截图已关联到机柜';}
  catch(exc:any){error.value=exc.message||'截图关联失败';}finally{saving.value=false;}
}
async function reviewProofTimes(row:Dict):Promise<void>{
  if(!await saveChanges())return;
  const current=(batch.value.rows || []).find((item:Dict)=>item.row_id===row.row_id);
  const imageId=current?.evidence_images?.[0];
  if(!current?.editable || !imageId)return;
  saving.value=true;
  try{batch.value=await write(`batches/${batchId}/images/${imageId}/apply`,{version:batch.value.version,row_id:row.row_id,candidate_index:-1,fields:{},review_times:true});snapshotRows();message.value='已记录时间核对结果';}
  catch(exc:any){error.value=exc.message||'时间核对失败';}finally{saving.value=false;}
}
async function reviewProofBusiness(row:Dict):Promise<void>{
  if(!await saveChanges())return;
  saving.value=true;
  try{batch.value=await write(`batches/${batchId}/images/${row.evidence_images[0]}/apply`,{version:batch.value.version,row_id:row.row_id,fields:{},review_business:true});snapshotRows();message.value='已记录操作与结果核对';}
  catch(exc:any){error.value=exc.message||'核对失败';}finally{saving.value=false;}
}
async function retryImage(image:Dict):Promise<void>{
  if(!await saveChanges())return;saving.value=true;
  try{batch.value=await write(`batches/${batchId}/images/${image.image_id}/retry`,{version:batch.value.version});snapshotRows();schedulePoll();}
  catch(exc:any){error.value=exc.message||'重新识别失败';}finally{saving.value=false;}
}
async function openCorrection(image:Dict,candidate:Dict={},index=-1):Promise<void>{
  correction.value={image_id:image.image_id,candidate_index:index,...Object.fromEntries(editableFields.map(key=>[key,candidate[key]||''])),error:''};
  correction.value.scope=candidate.scope||props.scope||buildingOptions.value[0]?.value||'';
  for(const key of ['expected','actual'])correction.value[key]=localDate(correction.value[key]);
  await loadCorrectionDirectory();
  await nextTick();document.querySelector('.image-correction')?.scrollIntoView({block:'center'});
}
async function loadCorrectionDirectory():Promise<void>{
  correctionDirectory.value=[];if(!correction.value?.scope)return;
  const scope=correction.value.scope;
  try{const data=await read('racks',{scope});if(correction.value?.scope===scope)correctionDirectory.value=data.items||[];}
  catch(exc:any){if(correction.value)correction.value.error=exc.message;}
}
async function submitCorrection():Promise<void>{
  if(!correction.value||!await saveChanges())return;saving.value=true;
  const current=correction.value;
  try{batch.value=await write(`batches/${batchId}/images/${current.image_id}/correct`,{version:batch.value.version,candidate_index:current.candidate_index,fields:Object.fromEntries(editableFields.map(key=>[key,current[key]||'']))});snapshotRows();syncImageSelections();correction.value=null;message.value='已补全机柜并关联原图';}
  catch(exc:any){current.error=exc.message||'更正失败';}finally{saving.value=false;}
}
async function restoreEvidence(imageId:string):Promise<void>{
  if(!await saveChanges())return;
  saving.value=true;
  try{batch.value=await write(`batches/${batchId}/images/${imageId}/restore`,{version:batch.value.version});snapshotRows();syncImageSelections();message.value='已撤回截图删除，原机柜关联已恢复';if(batch.value.images?.some((item:Dict)=>item.image_id===imageId&&item.status==='recognizing'))schedulePoll();}
  catch(exc:any){error.value=exc.message||'撤回删除失败';}finally{saving.value=false;}
}
function defaultExpected(): void { if (!common.expected) common.expected=common.actual; }

async function recognize(): Promise<void> {
  uploading.value=true; error.value='';
  try { const form=new FormData(); for (const file of pdfFiles.value) form.append('files',file,file.name); const data=await requestJson(`${api}/batches/recognize`,{method:'POST',body:form,timeoutMs:180000}); openBatch(data.batch_id); }
  catch (exc:any) { error.value=exc.message || 'PDF上传失败'; } finally { uploading.value=false; }
}
async function createFromImages():Promise<void>{
  if(!imageFiles.value.length)return;
  uploading.value=true;error.value='';
  try{
    if(!imageBatchId.value)imageBatchId.value=(await write('batches',{source:'image',scope:props.scope||''})).batch_id;
    await uploadImageChunks(imageBatchId.value,imageFiles.value);
    openBatch(imageBatchId.value);
  }catch(exc:any){error.value=(exc.message||'图片上传失败')+(imageBatchId.value?'；已创建的待办可继续重试':'');}
  finally{uploading.value=false;}
}
async function loadDirectory(): Promise<void> {
  if (!manualScope.value) return;
  try { directory.value=(await read('racks',{scope:manualScope.value})).items || []; }
  catch (exc:any) { error.value=exc.message || '机柜目录读取失败'; }
}
function makeManual(room: string,rack: string): Dict {
  const found=directory.value.find(item=>item.room===room&&item.rack===rack);
  return {_id:randomHexId(),scope:manualScope.value,room,rack,rack_type:found?.rack_type || '',supplier_rack:'',type_detail:'',action:common.action,expected:common.expected || common.actual,actual:common.actual,result:common.result,failure_reason:common.failure_reason};
}
function addPicked(): void { const existing=new Set(manualRows.value.map(row=>`${row.scope}/${row.room}/${row.rack}`)); for (const value of pickedRacks.value) { const [room,rack]=value.split('/'); if (!existing.has(`${manualScope.value}/${room}/${rack}`)) manualRows.value.push(makeManual(room,rack)); } pickedRacks.value=[]; }
function normalizeRoom(value: string): string { const text=value.trim().toUpperCase(); const match=text.match(/(?:EA118[-_.])?([A-E])([1-4])[-_](\d{1,2})/); return match ? `${match[2]}${String(Number(match[3])).padStart(2,'0')}` : text; }
function addPasted(): void { for (const line of pastedRows.value.split(/\r?\n/)) { const [room,rack]=line.trim().split(/[\t,，;；]+/).map(value=>value.trim()); if (room&&rack) manualRows.value.push(makeManual(normalizeRoom(room),rack.toUpperCase())); } pastedRows.value=''; }
function applyManualCommon(): void { for (const row of manualRows.value) Object.assign(row,{action:common.action || row.action,expected:common.expected || common.actual || row.expected,actual:common.actual || row.actual,result:common.result || row.result,failure_reason:common.result==='失败'?common.failure_reason:''}); }
async function createManual(): Promise<void> { uploading.value=true; error.value=''; try { const rows=manualRows.value.map(({_id,...row})=>row); const data=await write('batches',{rows}); openBatch(data.batch_id); } catch(exc:any){error.value=exc.message||'待办创建失败';} finally{uploading.value=false;} }

async function loadBatch(): Promise<void> {
  if (!batchId || dirtyCount.value || saving.value || saveConflicts.value.length) return; loading.value=true;
  try { batch.value=await read(`batches/${batchId}`); snapshotRows(); syncImageSelections(); offerBatchDraft(); error.value='';
    if (['recognizing','running'].includes(batch.value.status) || batch.value.images?.some((item:Dict)=>!item.deleted_at&&item.status==='recognizing')) schedulePoll(); }
  catch(exc:any){error.value=exc.message||'批次读取失败';if(['recognizing','running'].includes(batch.value.status) || batch.value.images?.some((item:Dict)=>!item.deleted_at&&item.status==='recognizing'))schedulePoll();} finally{loading.value=false;}
}
async function loadList(page=1): Promise<void> {
  if(disposed)return;
  const sequence=++listSequence;
  listAbort?.abort();listAbort=new AbortController();loading.value=true;
  try {
    const data=await read('batches',{scope:listScope.value,status:listStatus.value,from:listFrom.value,to:listTo.value,page:String(page),page_size:'20'},90000,listAbort.signal);
    if(disposed||sequence!==listSequence)return;
    list.value=data;error.value='';
  } catch(exc:any) { if(!disposed&&sequence===listSequence)error.value=exc.message||'待办读取失败'; }
  finally { if(!disposed&&sequence===listSequence)loading.value=false; }
}
function pollingActive(status:Dict=batch.value):boolean{return ['recognizing','running'].includes(String(status.status||''))||(status.images||[]).some((item:Dict)=>!item.deleted_at&&item.status==='recognizing');}
async function pollBatchStatus():Promise<void>{
  if(disposed||!batchId)return;
  try{
    const status=await read(`batches/${batchId}/status`,{},15000);
    if(Number(status.version)!==Number(batch.value.version)&&!saving.value&&!saveConflicts.value.length){
      rebaseBatch(await read(`batches/${batchId}`));if(dirtyCount.value&&!saveConflicts.value.length)queueAutoSave();
    }
    if(pollingActive(status)||Number(status.version)!==Number(batch.value.version))schedulePoll();
  }catch{if(pollingActive())window.clearTimeout(pollTimer),pollTimer=window.setTimeout(pollBatchStatus,4000);}
}
function schedulePoll(): void { window.clearTimeout(pollTimer); if (!disposed) pollTimer=window.setTimeout(pollBatchStatus,document.hidden?4000:1500); }
async function reload(): Promise<void> { if (batchId) { if(await saveChanges())await loadBatch(); } else await loadList(list.value.page || 1); }
function toggleVisible(event: Event): void { const ids=pagedRows.value.filter(row=>row.editable||row.confirmable||row.restorable).map(row=>row.row_id); if ((event.target as HTMLInputElement).checked) selectedRows.value=[...new Set([...selectedRows.value,...ids])]; else selectedRows.value=selectedRows.value.filter(id=>!ids.includes(id)); }
async function applyBulk():Promise<void>{
  if(!await saveChanges())return;
  const rowIds=(batch.value.rows||[]).filter((row:Dict)=>selectedRows.value.includes(row.row_id)&&row.editable).map((row:Dict)=>row.row_id);
  const common:Dict={};
  for(const key of ['action','expected','actual','result','failure_reason','type_resolution']){
    const value=String(bulk[key as keyof typeof bulk]||'');
    if(value)common[key]=value.replace('T',' ');
  }
  if(common.result==='成功')common.failure_reason='';
  if(!rowIds.length||!Object.keys(common).length)return;
  saving.value=true;
  try{const saved=await write(`batches/${batchId}`,{version:batch.value.version,row_ids:rowIds,common,response_mode:'delta'},'PATCH');mergeBatchResponse(saved);message.value=`已批量更新 ${rowIds.length} 条记录`;}
  catch(exc:any){error.value=exc.message||'批量更新失败';}
  finally{saving.value=false;}
}
function queueAutoSave(): void { window.clearTimeout(saveTimer); saveFailed.value=false;persistBatchDraft(); saveTimer=window.setTimeout(()=>{ void saveChanges(); },500); }
async function saveChanges(retryConflict=true): Promise<boolean> {
  window.clearTimeout(saveTimer);
  if(saveConflicts.value.length){error.value='存在字段冲突，请选择保留的内容';return false;}
  if (savePromise) { const finished=await savePromise; return finished && (!dirtyCount.value || await saveChanges()); }
  const rows=dirtyRows.value.map((row:Dict)=>({row_id:row.row_id,...Object.fromEntries(editableFields.map(key=>[key,row[key]||'']))}));
  if (!rows.length) return true;
  const sent=new Map<string,string>(rows.map((row:Dict)=>[row.row_id,rowSignature(row)]));
  let rebased=false;
  saving.value=true;
  savePromise=(async()=>{
    try {
      const saved=await write(`batches/${batchId}`,{version:batch.value.version,rows,response_mode:'delta'},'PATCH');
      const pending=(batch.value.rows || []).filter((row:Dict)=>baseline.get(row.row_id)!==rowSignature(row) && sent.get(row.row_id)!==rowSignature(row)).map((row:Dict)=>{
        const submitted=JSON.parse(sent.get(row.row_id)||baseline.get(row.row_id)||'{}');
        return {row_id:row.row_id,values:Object.fromEntries(editableFields.filter(key=>String(row[key]||'')!==String(submitted[key]||'')).map(key=>[key,row[key]]))};
      });
      mergeBatchResponse(saved,pending);
       saveFailed.value=false; error.value=''; message.value='更正已自动保存'; if(dirtyCount.value)persistBatchDraft();else draftStorage.removeItem(draftKey); return true;
    } catch(exc:any) {
      if(exc.status===409&&retryConflict){
        try{rebaseBatch(await read(`batches/${batchId}`));rebased=!saveConflicts.value.length;}
        catch(readError:any){exc=readError;}
      }
      saveFailed.value=true;persistBatchDraft();error.value=saveConflicts.value.length?'数据已更新，请核对冲突字段':`自动保存失败，请重试：${exc.message||'连接异常'}`;return false;
    }
    finally { saving.value=false; }
  })();
  const success=await savePromise; savePromise=undefined;
  if(rebased){saveFailed.value=false;error.value='';return saveChanges(false);}
  if (success && dirtyCount.value) queueAutoSave();
  return success;
}
async function acknowledgeWarnings(): Promise<void> { if(!await saveChanges())return; saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[],acknowledge_warnings:true},'PATCH');snapshotRows();message.value='已记录人工核对结果';}catch(exc:any){error.value=exc.message||'异常核对保存失败';}finally{saving.value=false;} }
async function revalidate(): Promise<void> { if(!await saveChanges())return; saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[]},'PATCH');snapshotRows();message.value='批次已重新校验';}catch(exc:any){error.value=exc.message||'重新校验失败';}finally{saving.value=false;} }
async function toggleExcluded(row: Dict,excluded: boolean): Promise<void> { if(!await saveChanges())return; saving.value=true; try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[{row_id:row.row_id,excluded}]},'PATCH');snapshotRows();}catch(exc:any){error.value=exc.message||'操作失败';}finally{saving.value=false;} }
async function excludeSelected(): Promise<void> { if(!await saveChanges())return; const ids=(batch.value.rows||[]).filter((row:Dict)=>selectedRows.value.includes(row.row_id)&&row.editable&&!String(row.status).startsWith('excluded_')).map((row:Dict)=>row.row_id);if(!ids.length)return;saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,row_ids:ids,common:{excluded:true}},'PATCH');selectedRows.value=[];snapshotRows();message.value=`已排除 ${ids.length} 条待办行`;}catch(exc:any){error.value=exc.message||'批量排除失败';}finally{saving.value=false;} }
async function restoreRows(ids:string[]):Promise<void>{if(!await saveChanges())return;const valid=(batch.value.rows||[]).filter((row:Dict)=>ids.includes(row.row_id)&&row.restorable).map((row:Dict)=>row.row_id);if(!valid.length)return;saving.value=true;message.value='正在恢复所选待办行…';try{batch.value=await write(`batches/${batchId}/restore-rows`,{version:batch.value.version,row_ids:valid});snapshotRows();selectedRows.value=selectedRows.value.filter(id=>!valid.includes(id));message.value=`已恢复 ${valid.length} 条待办行`;}catch(exc:any){message.value='';error.value=exc.message||'恢复失败';}finally{saving.value=false;}}
function restoreSelected():void{void restoreRows(selectedRows.value);}
function requestFileCleanup(fileId:string):void{cleanupFileId.value=fileId;confirmDialog.value='file';}
function requestImageDelete(imageId:string):void{imageDeleteId.value=imageId;confirmDialog.value='image';}
async function confirmRows(payload: Dict): Promise<void> { if(!await saveChanges())return; saving.value=true; try{batch.value=await write(`batches/${batchId}/confirm`,{version:batch.value.version,...payload});snapshotRows();selectedRows.value=[];message.value='已提交后台处理，正在写入多维并核验';schedulePoll();}catch(exc:any){error.value=exc.message||'确认失败';}finally{saving.value=false;} }
function requestRollback(rowId:string):void { rollbackRowId.value=rowId; confirmDialog.value='rollback'; }
async function rollbackRows(rowId:string):Promise<void> { if(!await saveChanges())return; saving.value=true; try{batch.value=await write(`batches/${batchId}/rollback`,{version:batch.value.version,...(rowId?{row_ids:[rowId]}:{all:true})});snapshotRows();message.value='已开始回退；后续有操作的机柜会跳过';schedulePoll();}catch(exc:any){error.value=exc.message||'回退未启动';}finally{saving.value=false;} }
async function cancelPending():Promise<void>{
  message.value='正在作废未提交行…';error.value='';
  if(!await saveChanges()){
    message.value='';
    try{const latest=await read(`batches/${batchId}`);if(latest.status==='cancelled'){batch.value=latest;snapshotRows();error.value='';message.value='未提交行已作废';}}catch{}
    return;
  }
  saving.value=true;message.value='正在作废未提交行…';
  let failure='';
  try{batch.value=await write(`batches/${batchId}/cancel`,{version:batch.value.version});snapshotRows();}
  catch(exc:any){failure=exc.message||'作废结果尚未确认';}
  finally{saving.value=false;}
  await loadBatch();
  if(batch.value.status==='cancelled'){error.value='';message.value='未提交行已作废；需要恢复时可勾选行并点击“恢复已选”';}
  else{message.value='';if(failure)error.value=failure;}
}
async function retryHandoffs():Promise<void>{
  handoffRetrying.value=true;error.value='';
  try{const result=await write('batches/retry-handoffs',{});message.value=`已重新提交 ${result.requeued || 0} 条联动任务`;await loadList(list.value.page||1);}
  catch(exc:any){error.value=exc.message||'联动重试失败';}
  finally{handoffRetrying.value=false;}
}
async function resolveConfirm(confirmed:boolean):Promise<void>{
  const action=confirmDialog.value,fileId=cleanupFileId.value,rowId=rollbackRowId.value,imageId=imageDeleteId.value;
  confirmDialog.value='';cleanupFileId.value='';rollbackRowId.value='';imageDeleteId.value='';
  if(!confirmed)return;
  if(action==='warnings'){await acknowledgeWarnings();return;}
  if(action==='rollback'){await rollbackRows(rowId);return;}
  if(action==='all'){await confirmRows({all:true});return;}
  if(action==='cancel'){await cancelPending();return;}
  if(action==='delete_empty'){
    saving.value=true;
    try{await requestJson(`${api}/batches/${batchId}?version=${batch.value.version}`,{method:'DELETE',timeoutMs:90000});openList();}
    catch(exc:any){error.value=exc.message||'删除空批次失败';}
    finally{saving.value=false;}
    return;
  }
  if(!await saveChanges())return;
  saving.value=true;
  try{
    if(action==='image'){
      batch.value=await requestJson(`${api}/batches/${batchId}/images/${imageId}?version=${batch.value.version}`,{method:'DELETE',timeoutMs:90000});
      for(const key of Object.keys(imageSelections))if(key===imageId||key.startsWith(imageId+':'))delete imageSelections[key];
      message.value='截图已从待办移除';
    }else if(action==='overlap')batch.value=await write(`batches/${batchId}/clear-overlaps`,{version:batch.value.version});
    else if(action==='file')batch.value=await write(`batches/${batchId}/files/${fileId}/cleanup`,{});
    snapshotRows();
  }catch(exc:any){error.value=exc.message||'操作失败';}
  finally{saving.value=false;}
}
function resolveDiscard(confirmed:boolean):void{discardOpen.value=false;const proceed=pendingNavigation;pendingNavigation=undefined;if(confirmed){discardDraft=true;draftStorage.removeItem(draftKey);if(!batchId){textFragments.value=[];draftStorage.removeItem(textDraftKey);}proceed?.();}}

watch(rowPageCount,count=>{rowPage.value=Math.min(rowPage.value,count);});
watch([rowScopeFilter,rowStatusFilter,rowQuickFilter],()=>{rowPage.value=1;if(editingRowId.value)closeRowEditor();});
watch(imagePageCount,count=>{imagePage.value=Math.min(imagePage.value,count);});
watch(rowPage,()=>{if(editingRowId.value)closeRowEditor();});
watch(editingRowId,value=>{if(value)void nextTick(()=>document.querySelector<HTMLElement>('.row-editor-overlay .row-editor')?.focus());});
watch(previewImage,value=>{if(value)void nextTick(()=>document.querySelector<HTMLElement>('.image-preview .image-close')?.focus());});
function closeImageOnEscape(event:KeyboardEvent):void{
  if(previewImage.value){
    if(event.key==='Escape'){event.preventDefault();previewImage.value='';}
    else if(event.key==='ArrowLeft'){event.preventDefault();moveProof(-1);}
    else if(event.key==='ArrowRight'){event.preventDefault();moveProof(1);}
    else if(event.key==='Tab'){
      const controls=[...document.querySelectorAll<HTMLButtonElement>('.image-preview button:not(:disabled)')];
      const index=controls.indexOf(document.activeElement as HTMLButtonElement);
      event.preventDefault();controls[(index+(event.shiftKey?-1:1)+controls.length)%controls.length]?.focus();
    }
    return;
  }
  if(!editingRowId.value)return;
  if(event.key==='Escape'){event.preventDefault();closeRowEditor();return;}
  if(event.key!=='Tab')return;
  const nodes=[...document.querySelectorAll<HTMLElement>('.row-editor-overlay button:not(:disabled),.row-editor-overlay input:not(:disabled),.row-editor-overlay select:not(:disabled),.row-editor-overlay textarea:not(:disabled)')];
  if(!nodes.length)return;
  if(event.shiftKey&&(document.activeElement===nodes[0]||document.activeElement===document.querySelector('.row-editor-overlay .row-editor'))){event.preventDefault();nodes[nodes.length-1].focus();}
  else if(!event.shiftKey&&document.activeElement===nodes[nodes.length-1]){event.preventDefault();nodes[0].focus();}
}
onMounted(async()=>{window.addEventListener('paste',pasteFiles);window.addEventListener('keydown',closeImageOnEscape);window.addEventListener('pagehide',persistBatchDraft);removeGuard=registerNavigationGuard((_target,proceed)=>{if(!batchId&&textFragments.value.length){pendingNavigation=proceed;discardOpen.value=true;return false;}if(!dirtyCount.value&&!saving.value&&!saveConflicts.value.length)return true;void saveChanges().then(ok=>{if(ok&&!dirtyCount.value)proceed();else{pendingNavigation=proceed;discardOpen.value=true;}});return false;});if(batchId)await loadBatch();else if(mode!=='new')await loadList();else restoreTextDraft();if(mode==='new'&&createMode.value==='manual')await loadDirectory();});
onBeforeUnmount(()=>{persistBatchDraft();disposed=true;listAbort?.abort();window.clearTimeout(pollTimer);window.clearTimeout(saveTimer);window.removeEventListener('paste',pasteFiles);window.removeEventListener('keydown',closeImageOnEscape);window.removeEventListener('pagehide',persistBatchDraft);persistTextDraft();for(const url of imagePreviewUrls.values())URL.revokeObjectURL(url);imagePreviewUrls.clear();removeGuard?.();});
</script>

<style scoped>
.batch-page{max-width:1800px;margin:auto;min-height:80vh;padding:24px;color:#203650;background:#f7f9fc;font-size:14px;letter-spacing:0}.page-heading{display:flex;align-items:center;gap:18px;margin-bottom:22px}.page-heading>div:nth-child(2){flex:1;min-width:0}.page-heading h1{margin:0 0 7px;font-size:26px}.page-heading p,.entry-section>p{margin:0;color:#63768c;font-size:12px}.actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.actions.end{justify-content:flex-end;margin-top:16px}button,a,input,select,textarea{font:inherit;box-sizing:border-box}button,.actions a{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:38px;padding:9px 12px;border:1px solid #d4dfed;border-radius:6px;background:#fff;color:#214969;text-decoration:none;cursor:pointer}button:hover:not(:disabled){border-color:#8db7ec;background:#edf5ff}button:disabled{opacity:.45;cursor:not-allowed}.primary{border-color:#1764dd;background:#1764dd;color:#fff}.primary:hover:not(:disabled){background:#1154bc;color:#fff}input,select,textarea{min-height:38px;max-width:100%;padding:8px;border:1px solid #ccd9e8;border-radius:5px;background:#fff;color:#263f5b}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,.file-drop:focus-visible{outline:3px solid #76aaf0;outline-offset:2px}.notice{display:flex;align-items:center;gap:10px;margin:12px 0;padding:12px 16px;border:1px solid #cbdffb;border-radius:6px;background:#ebf4ff;overflow-wrap:anywhere}.notice button{margin-left:auto}.notice.danger{border-color:#f8c9cd;background:#fff0f1;color:#ae283e}.notice.success{border-color:#bee8d6;background:#eaf9f2;color:#167953}.notice.warning{border-color:#f0d797;background:#fff9e8;color:#745400}.tabs{display:flex;gap:8px;margin:18px 0;border-bottom:1px solid #dbe4ef;padding-bottom:12px}.tabs .active{border-color:#9ebfe8;background:#eaf2ff;color:#175dbb}.entry-section{padding:20px 0}.entry-section h2{margin:0 0 8px;font-size:20px}.file-drop{display:grid;place-items:center;gap:8px;min-height:150px;margin:20px 0;border:1px dashed #8ca9c7;background:#fff;color:#285777;cursor:pointer}.file-drop.dragging{border-color:#1764dd;background:#edf5ff}.file-drop input{max-width:360px;border:0}.drop-hint{color:#657b91;font-size:12px}.file-list{max-height:220px;margin:0 0 16px;padding:0;overflow:auto;list-style:none;background:#fff}.file-list li,.source-files>div{display:flex;align-items:center;gap:9px;padding:10px 12px;border-bottom:1px solid #e5edf5}.file-list small,.source-files small{margin-left:auto;color:#647b91}.common-fields{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:18px 0}.common-fields label,.manual-source label{display:flex;flex-direction:column;gap:6px;color:#4a6581;font-size:12px}.manual-source{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin:20px 0}.manual-source>div{display:flex;flex-direction:column;gap:8px}.manual-source select,.manual-source textarea{width:100%;min-height:190px}.section-heading{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:20px 0 10px}.section-heading h3,.source-files h3{margin:0;font-size:16px}.table-wrap{overflow:auto;border:1px solid #dce6f1;background:#fff}.table-wrap table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:13px}.table-wrap th{position:sticky;top:0;z-index:1;padding:12px;background:#edf3fa;color:#425c77;text-align:left}.table-wrap td{padding:10px;border-bottom:1px solid #e8eef5;vertical-align:top}.table-wrap tbody tr:hover{background:#f8fbff}.table-wrap input,.table-wrap select{min-width:115px}.table-wrap td small{display:block;max-width:260px;margin-top:5px;white-space:normal;color:#6a7f94;font-size:11px}.icon-button{width:38px;padding:0}.empty{padding:42px;text-align:center;color:#697f94}.batch-summary{display:flex;align-items:center;gap:24px;padding:18px 0;border-block:1px solid #dbe4ef;background:#fff}.summary-main{display:flex;min-width:180px;flex-direction:column;gap:7px;padding:0 18px}.summary-main>strong{font-size:18px}.summary-main small{color:#647b91}.metrics{display:grid;flex:1;grid-template-columns:repeat(7,minmax(74px,1fr))}.metrics span{padding:0 14px;border-left:1px solid #e8eef5;color:#60758c;font-size:12px}.metrics b{display:block;margin-top:6px;color:#203650;font-size:24px}.metrics .duplicate b,.metrics .conflict b,.danger-text{color:#b72e43}.success-text{color:#167953!important}.progress-copy{display:grid;flex:1;gap:6px}.progress-copy progress{width:100%;height:8px}.source-files{margin:18px 0}.source-files>h3{padding:0 0 10px}.source-files b{max-width:50%;color:#ae283e;font-size:11px}.batch-actions{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:18px 0;flex-wrap:wrap}.bulk-fields{display:flex;gap:8px;flex-wrap:wrap}.detail-table{max-height:64vh}.detail-table tr.duplicate,.detail-table tr.invalid,.detail-table tr.conflict,.detail-table tr.failed{background:#fff9ed}.detail-table tr.completed{background:#f0fbf6}.detail-table tr[class^="excluded_"]{opacity:.65;background:#f3f5f7}.corrected{border-color:#d89b20!important;background:#fff9e8!important}.status{display:inline-flex;width:max-content;border:1px solid #cbd8e6;border-radius:10px;padding:2px 7px;background:#f3f6fa;color:#536b82;font-size:11px}.status.ready,.status.completed{border-color:#a9dfc8;background:#eaf9f2;color:#167953}.status.duplicate,.status.conflict,.status.invalid,.status.failed{border-color:#f3c6cb;background:#fff0f1;color:#ae283e}.status.running,.status.queued,.status.writing,.status.recognizing,.status.parsing{border-color:#a9c9f2;background:#eaf3ff;color:#175dbb}.pagination{display:flex;align-items:center;justify-content:flex-end;gap:8px;margin-top:15px}.pagination>span{margin-right:auto;color:#60768c}.pagination button{width:38px;padding:0}.pagination button.active{border-color:#1764dd;background:#1764dd;color:#fff}.filters{display:flex;gap:9px;margin:16px 0;flex-wrap:wrap}.link{min-height:28px;padding:0;border:0;background:transparent;color:#175ebd}.spin{animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}
.notice-source{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:14px 0;padding:14px 16px;border:1px solid #dbe4ef;background:#fff}.notice-source>div{display:flex;min-width:0;flex-direction:column;gap:5px}.notice-source small{color:#667c92}.notice-source strong,.notice-source span{overflow-wrap:anywhere}.notice-source-cabinets{grid-column:1/-1}.notice.warning>span{display:grid;gap:4px}.notice.warning>span small{display:block}.notice.warning>strong{margin-left:auto;white-space:nowrap}
@media(max-width:1000px){.page-heading{flex-wrap:wrap}.page-heading>.actions{width:100%}.common-fields,.notice-source{grid-template-columns:repeat(2,minmax(0,1fr))}.manual-source{grid-template-columns:1fr}.batch-summary{align-items:stretch;flex-direction:column}.metrics{grid-template-columns:repeat(4,1fr);row-gap:14px}.batch-actions{align-items:stretch;flex-direction:column}.bulk-fields,.batch-actions>.actions{width:100%}}
@media(max-width:640px){.batch-page{padding:14px}.page-heading h1{font-size:22px}.common-fields,.notice-source{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.notice{font-size:13px}.source-files>div{align-items:flex-start;flex-wrap:wrap}.source-files small{margin-left:0}.source-files b{max-width:100%}}
@media(prefers-reduced-motion:reduce){*{animation:none!important}}
.batch-summary{display:flex;align-items:center;flex-wrap:wrap;gap:12px 24px;padding:10px 0;border:0;border-block:1px solid #dbe4ef;border-radius:0;background:transparent}
.summary-main{min-width:0;flex-direction:row;align-items:center;gap:10px;padding:0;border:0;background:transparent}
.summary-title{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.summary-title strong{font-size:16px;white-space:nowrap}
.summary-main small{font-size:12px;line-height:1.4}
.metrics{display:flex;justify-content:flex-end;flex-wrap:wrap;gap:8px 18px;margin:0}
.metrics>div{display:flex;align-items:baseline;gap:6px;min-width:0;padding:0;border:0}
.metrics dt{color:#65778a;font-size:12px;line-height:1.4;white-space:nowrap}
.metrics dd{margin:0;color:#1d344c;font-size:17px;font-weight:650;line-height:1.4}
.metrics .warning-metric dd{color:#a76a12}
.metrics .success-metric dd{color:#167953}
.metrics .danger-metric dd{color:#ae283e}
.notice-source{border-color:#d4dfeb;border-radius:6px;box-shadow:none}
.batch-actions{display:flex;align-items:center;gap:8px 16px;margin:12px 0 8px;padding:8px 0;border:0;border-block:1px solid #dbe4ef;border-radius:0;background:transparent}
.filter-bar{display:flex;align-items:center;flex-wrap:wrap;gap:8px;min-width:0;padding:0;flex:1}
.filter-bar>strong{margin-right:8px;color:#29435c;font-size:13px;white-space:nowrap}
.filter-bar label,.commit-actions label,.bulk-fields label{display:flex;align-items:center;gap:8px;min-width:0;color:#60748a;font-size:12px;white-space:nowrap}
.filter-bar select{width:132px}
.filter-count{margin-left:auto;color:#6b7f93;font-size:12px;white-space:nowrap}
.selection-panel{order:2;flex-basis:100%;display:grid;gap:8px;padding:10px 0;border-top:1px solid #e1e8f0;background:transparent}
.selection-heading{display:flex;align-items:center;gap:14px}
.selection-heading strong{color:#1d4f8c}
.selection-heading .link{min-height:28px}
.bulk-fields{display:flex;align-items:end;gap:10px;min-width:0;flex-wrap:wrap}
.bulk-fields label{display:grid;gap:5px}
.bulk-fields select,.bulk-fields input{width:164px;min-width:0}
.bulk-fields label:first-child select{width:190px}
.bulk-fields .danger-ghost{color:#a72c3e}
.commit-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;padding:0;border:0;flex-wrap:wrap}
.commit-actions label{padding:0 8px;border-left:1px solid #e1e8f0}
.commit-actions select{width:116px}
.save-indicator{color:#58718a;font-size:12px;white-space:nowrap}
.status.rolled_back{border-color:#bed6cd;background:#edf6f1;color:#226e55}
.status.rollback_queued,.status.rolling_back{border-color:#a9c9f2;background:#eaf3ff;color:#175dbb}
.status.rollback_failed,.status.rollback_blocked{border-color:#f3c6cb;background:#fff0f1;color:#ae283e}
.detail-table{max-height:68vh;border-color:#d4dfeb;border-radius:6px;scrollbar-gutter:stable}
.detail-table .records-table{width:100%;min-width:1020px;table-layout:fixed;white-space:normal}
.records-table th{z-index:2;padding:11px 10px;background:#eaf0f6;color:#365069;font-size:12px;line-height:1.4;white-space:nowrap}
.records-table td{padding:8px 10px;vertical-align:middle;line-height:1.4;overflow-wrap:anywhere}
.records-table tbody>tr:not(.row-editor-row):nth-child(even):not(.duplicate):not(.invalid):not(.conflict):not(.failed):not(.completed){background:#fbfcfe}
.records-table tbody>tr.expanded{background:#edf5ff}
.records-table td>strong{display:block;color:#1e354d;font-size:13px;font-weight:650}
.records-table td>small{display:block;margin-top:4px;color:#718296;font-size:11px}
.records-table .select-column{width:44px}
.records-table .select-column input,.records-table tbody>tr:not(.row-editor-row) td:first-child input{width:17px;min-width:17px;height:17px;vertical-align:middle}
.records-table .validation-cell{width:132px}
.records-table .validation-cell small{max-width:128px;color:#a0671c}
.records-table .location-summary{width:88px}
.records-table .rack-summary{width:76px}
.records-table .supplier-summary{width:114px}
.records-table .operation-summary{width:150px}
.operation-summary strong{margin-top:5px}
.operation-summary .muted{color:#9a6b20}
.records-table .time-cell{width:144px;color:#35536f;font-variant-numeric:tabular-nums;white-space:nowrap}
.records-table .result-summary{width:74px}
.records-table .proof-cell{width:152px}
.proof-thumbs{display:flex;align-items:center;gap:4px}
.proof-more{min-width:28px!important;min-height:30px!important;padding:2px 5px!important;font-size:12px}
.proof-file{display:flex;align-items:flex-start;gap:4px;max-width:100%;margin-top:5px;color:#175ebd;font-size:12px;line-height:1.4;text-decoration:none}
.proof-file svg{flex:none;margin-top:1px}
.proof-file span{min-width:0;overflow-wrap:anywhere}
.proof-cell .proof-file span{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.proof-file.muted{color:#738497}
.row-documents{display:grid;gap:4px;max-width:100%}
.preview-navigation{position:absolute;bottom:12px;left:50%;transform:translateX(-50%);display:flex;align-items:center;gap:16px;color:#fff}
.preview-navigation button{width:36px;min-height:32px;padding:0}

.records-table .action-column,.records-table .row-actions{width:88px}
.row-actions{position:sticky;right:0;z-index:1;white-space:nowrap;background:#fff;box-shadow:-1px 0 #e1e8f0}
.records-table .action-column{right:0;z-index:3;box-shadow:-1px 0 #d4dfeb}
.row-actions .icon-button{width:34px;min-height:34px;padding:0}
.row-actions .danger-icon{color:#aa3846}
.result-badge{display:inline-block;color:#67798a;font-size:12px;white-space:nowrap}
.result-badge.success{color:#167953}
.result-badge.failed{color:#a93242}
.power-state{display:inline-block;max-width:100%;padding:2px 7px;border:1px solid #c9d6e4;border-radius:4px;background:#f4f7fa;color:#536a80;font-size:11px;line-height:1.45;white-space:normal}
.power-state.formal{border-color:#f3b8bf;background:#fff0f1;color:#a3263a}
.power-state.test{border-color:#efd18a;background:#fff7df;color:#785300}
.power-state.off{border-color:#acd9c5;background:#edf9f3;color:#147451}
.row-editor-row>td{padding:0!important;white-space:normal}
.row-editor{padding:0 20px 16px;border-block:1px solid #c5d9f2;background:#f8fbff}
.row-editor>header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 0;border-bottom:1px solid #dce7f3}
.row-editor>header>div{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.row-editor>header span{color:#60758b;font-size:12px}
.row-editor>header strong{color:#1d3f65;font-size:15px}
.row-editor>header .icon-button{min-height:32px}
.editor-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0;padding-top:16px}
.editor-grid fieldset{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-content:start;gap:12px;margin:0;min-width:0;padding:0 16px;border:0;border-right:1px solid #dce7f3}
.editor-grid fieldset:first-child{padding-left:0}
.editor-grid fieldset:last-child{padding-right:0;border-right:0}
.editor-grid legend{padding:0 0 12px;color:#365675;font-size:13px;font-weight:650}
.editor-grid label{display:grid;align-content:start;gap:5px;min-width:0;color:#60758b;font-size:12px}
.editor-grid label span{line-height:1.4}
.editor-grid input,.editor-grid select{width:100%;min-width:0;height:38px}
.editor-grid .wide-field,.editor-grid .state-line{grid-column:1/-1}
.state-line{display:flex;align-items:center;gap:8px;flex-wrap:wrap;min-width:0}
.state-line small{color:#60758b;font-size:11px;line-height:1.5}
.editor-footer{display:flex;align-items:end;justify-content:space-between;gap:16px;padding-top:14px}
.editor-footer .actions{flex-shrink:0}
.source-audit{display:flex;align-items:center;gap:10px;min-width:0;flex-wrap:wrap;color:#59738c;font-size:12px}
.source-audit small{overflow-wrap:anywhere}
.edit-audit{min-width:0;white-space:normal}
.edit-audit summary{color:#1b62ae;cursor:pointer}
.edit-audit small{display:block;margin-top:6px;overflow-wrap:anywhere}
.corrected{border-color:#d89b20!important;background:#fff9e8!important}
.evidence-section{margin:10px 0;padding:0;border:0;background:transparent}
.evidence-section .section-heading{margin:0 0 10px}
.evidence-upload{display:flex;align-items:center;gap:8px;flex-wrap:wrap;min-height:44px;padding:6px 10px;border:1px dashed #b3c4d5;background:#fff}
.evidence-upload.dragging{border-color:#1764dd;background:#eaf2ff}
.evidence-upload>span{margin-right:auto;color:#47647f}
.file-command{position:relative;display:inline-flex;align-items:center;min-height:36px;padding:7px 12px;border:1px solid #cbd8e6;background:#fff;color:#214969;cursor:pointer}
.file-command input{position:absolute;inset:0;width:100%;height:100%;opacity:0;cursor:pointer}
.evidence-upload small{width:100%;color:#526d86}
.evidence-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:8px;max-height:280px;overflow:auto;align-items:start}
.evidence-item{display:grid;grid-template-columns:68px minmax(0,1fr);gap:8px;min-width:0;padding:8px;border:1px solid #e0e8f1;border-radius:4px;background:#fff}
.evidence-item>div{min-width:0}.evidence-item strong,.evidence-item small{display:block;overflow-wrap:anywhere}.evidence-item small{margin-top:4px;color:#637a91}
.evidence-heading{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;min-width:0}
.evidence-heading strong{min-width:0;flex:1}
.evidence-heading .icon-button{min-height:32px;width:32px;flex:none;padding:0;color:#a93242}
.image-button{display:block;width:68px;height:56px;min-height:0;padding:0;border:1px solid #d7e1eb;background:#fff;overflow:hidden}
.image-button img{display:block;width:100%;height:100%;object-fit:contain}
.evidence-match{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:8px;padding-top:8px;border-top:1px solid #ebf0f5}
.evidence-match span{flex:1;min-width:0;flex-basis:100%;color:#36536e;font-size:12px;line-height:1.5}
.evidence-match select{max-width:260px}.evidence-search{width:210px;min-width:150px}
.evidence-match button{min-height:34px;padding:6px 9px}
.evidence-pagination{margin-top:10px}.evidence-pagination>span{margin-right:auto}
.row-evidence-content{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-top:12px;border-top:1px solid #dce7f3;padding-top:12px}
.row-evidence-content label{display:flex;align-items:center;gap:8px;color:#60758b}
.row-evidence-content input{width:min(480px,70vw)}
.row-image-list{display:flex;gap:8px;flex-wrap:wrap}.row-image-list .image-button{width:90px;height:70px}
.image-preview{position:fixed;inset:0;z-index:1200;display:grid;place-items:center;padding:55px 24px 56px;background:rgba(15,28,43,.86)}
.image-preview img{max-width:100%;max-height:100%;object-fit:contain}
.image-close{position:absolute;top:12px;right:20px;width:38px;padding:0}
@media(max-width:1100px){
  .batch-summary{grid-template-columns:1fr}
  .summary-main{border:0;flex-wrap:wrap}
  .editor-grid{grid-template-columns:1fr}
  .editor-grid fieldset{padding:12px 0;border-right:0;border-bottom:1px solid #dce7f3}
  .editor-grid fieldset:last-child{border-bottom:0}
}
@media(max-width:640px){
  .metrics{grid-template-columns:repeat(2,minmax(0,1fr))}
  .metrics{justify-content:flex-start}
  .filter-bar{flex-wrap:wrap}
  .filter-count{margin-left:0}
  .commit-actions{justify-content:flex-start}
  .editor-footer{align-items:start;flex-direction:column}
}
@media(max-width:900px){.evidence-grid{grid-template-columns:1fr}}
.image-file-list{margin-top:12px;border:1px solid #dce6f1}
.image-file-list li{min-width:0}
.image-file-list img{width:68px;height:52px;flex:none;object-fit:contain;border:1px solid #dce6f1;background:#fff}
.image-file-list span{min-width:0;overflow-wrap:anywhere}
.image-file-list .icon-button{min-height:32px;flex:none}
.deleted-images{margin-top:16px;padding-top:12px;border-top:1px solid #dce6f1}
.deleted-images h4{margin:0 0 8px;font-size:13px;color:#60758b}
.deleted-images>div{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #e8eef5;color:#60758b}
.deleted-images img{width:64px;height:48px;object-fit:contain;border:1px solid #dce6f1;opacity:.65}
.deleted-images span{min-width:0;flex:1;overflow-wrap:anywhere}
.batch-list-table tbody tr.todo-batch{background:#fff9eb;border-left:4px solid #d59624}
.todo-mark{display:inline-block;margin-left:8px;padding:2px 6px;border:1px solid #e3b65f;background:#fff2ce;color:#805a0c;font-size:11px;font-weight:650}
.todo-count{color:#945e0e;font-size:15px}
.row-thumb{display:block;width:48px;height:38px;min-height:0;margin-bottom:6px;padding:0;border:1px solid #d4dfeb;background:#fff;overflow:hidden}
.row-thumb img{width:100%;height:100%;object-fit:contain}
.row-editor-overlay{position:fixed;inset:0;z-index:1100;display:grid;place-items:center;padding:24px;background:rgba(18,35,53,.62);font-size:14px;color:#203650}
.row-editor-overlay .row-editor{box-sizing:border-box;width:min(1120px,calc(100vw - 48px));max-height:calc(100dvh - 48px);padding:0 20px 16px;border:1px solid #c5d9f2;border-radius:6px;background:#f8fbff;box-shadow:0 20px 60px #10273b45;overflow:auto;outline:0}
@media(max-width:760px){.row-editor-overlay{padding:0}.row-editor-overlay .row-editor{width:100vw;max-height:100dvh;border-radius:0}}

.has-detail{font-size:13px}
.has-detail .page-heading{margin-bottom:12px}
.has-detail .page-heading h1{font-size:22px;margin:0}
.has-detail button,.has-detail input,.has-detail select{min-height:32px;padding:6px 9px}
.has-detail .icon-button{padding:0;width:32px;height:32px}
.source-details{margin:8px 0;padding:0;border:0;border-bottom:1px solid #e3e9ef;border-radius:0;background:transparent}
.source-details>summary,.recognition-details>summary,.deleted-images>summary{padding:7px 0;color:#526b82;cursor:pointer;font-size:12px}
.source-details>summary span{margin-left:10px;color:#263f5b;overflow-wrap:anywhere}
.source-details>summary:focus-visible,.recognition-details>summary:focus-visible,.deleted-images>summary:focus-visible{outline:2px solid #76aaf0;outline-offset:2px}
.source-details .notice-source{margin:0;padding:8px 0 12px;gap:10px 16px;border:0;background:transparent}
.evidence-title{display:flex;align-items:center;gap:8px;margin-right:auto}
.evidence-title h3{margin:0;font-size:13px}
.evidence-title span{color:#687f93;font-size:12px}
.has-detail .file-command{gap:6px;min-height:32px;padding:5px 9px;border-radius:4px}
.evidence-heading strong{font-size:12px;line-height:1.4}
.recognition-details{grid-column:1/-1}
.deleted-images{margin-top:8px;padding-top:0}
.deleted-images summary{padding:5px 0}
.has-detail .row-thumb{width:40px;height:30px;min-height:0;padding:0;margin:0}
.has-detail .notice{margin:8px 0;padding:8px 10px}
.has-detail .pagination{margin-top:8px}
@media(max-width:1100px){.evidence-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:640px){.evidence-grid{grid-template-columns:1fr}.summary-main{width:100%}.source-details>summary span{display:block;margin:4px 0}.filter-bar{width:100%}}
.recognition-progress{border-left:4px solid #1764dd;background:#edf5ff}
.recognition-progress .progress-copy{flex:1;min-width:0;gap:7px}
.recognition-progress progress{width:min(540px,100%);height:10px}
.batch-name{min-width:200px;max-width:310px;white-space:normal;overflow-wrap:anywhere}
.save-conflicts{padding:12px;background:#fff7e6;border:1px solid #e5c57e;margin:8px 0}
.save-conflicts>div{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 0;border-bottom:1px solid #e8daba}
.save-conflicts>div>span{overflow-wrap:anywhere}
.editor-workspace{min-width:0}
.editor-workspace.with-proof{display:grid;grid-template-columns:minmax(0,1fr) minmax(280px,42%);gap:16px}
.with-proof .editor-grid{grid-template-columns:1fr}
.with-proof .editor-grid{row-gap:14px}
.with-proof .editor-grid fieldset{border-right:0;padding-bottom:10px}
.proof-panel{min-width:0;max-height:62vh;overflow:auto;border-left:1px solid #d9e2eb;padding:12px}
.proof-original{display:block;width:100%;min-height:0;padding:0;border:0;background:white}
.proof-original img{display:block;width:100%;height:auto;object-fit:contain}
.image-correction{padding:14px 0;border-block:1px solid #c5d9f2;margin:12px 0}
.image-correction>header{display:flex;align-items:center;justify-content:space-between}
.correction-body{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px;align-items:start}
.correction-body .common-fields{grid-template-columns:repeat(2,minmax(0,1fr))}
.has-detail .image-correction input,.has-detail .image-correction select{border-radius:4px;min-height:36px}
@media(max-width:800px){.editor-workspace.with-proof,.correction-body{grid-template-columns:1fr}.proof-panel{max-height:35vh;border-left:0}.correction-body .common-fields{grid-template-columns:1fr}}
.batch-page.is-text-entry{width:100%;box-sizing:border-box;padding-bottom:12px}
.is-text-entry>.page-heading{margin-bottom:12px;flex-shrink:0}.is-text-entry>.tabs{margin:8px 0 12px;flex-shrink:0}
.is-text-entry>.notice{flex-shrink:0;margin:0 0 8px}
.text-entry{display:flex;flex-direction:column;gap:10px;padding:0}
.text-input-bar{display:flex;align-items:center;gap:12px;flex-shrink:0}
.text-input-bar textarea{flex:1;min-width:0;resize:none;height:58px;line-height:1.5;border-radius:6px}
.text-input-bar>span{display:flex;gap:6px;align-items:center;color:#536b82;font-size:13px;white-space:nowrap}
.text-entry>.notice{margin:0;max-height:84px;overflow:auto;flex-shrink:0}
.text-toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;flex-shrink:0}
.text-source-picker{display:flex;align-items:center;gap:5px}.text-source-picker select{width:240px}
.text-search{display:flex;align-items:center;gap:7px;flex:1;min-width:160px;border:1px solid #ccd9e8;background:white;border-radius:5px;padding-left:10px}
.text-search input{border:0;min-width:0;flex:1;min-height:34px;padding:6px}.text-toolbar select{min-height:34px;padding:6px}
.text-toolbar .icon-button{width:34px;min-height:34px}
.text-original{white-space:pre-wrap;overflow-wrap:anywhere;max-height:200px;overflow:auto;font:12px/1.6 monospace;background:#fff;padding:10px;margin:6px 0}
.text-preview{flex:none;height:auto;max-height:none}.text-preview th{padding:8px}.text-preview td{padding:5px 7px;vertical-align:middle}
.text-preview input,.text-preview select{min-height:30px;height:30px;padding:4px 5px;border-radius:4px;min-width:0;font-size:12px}
.text-preview .short-input{width:58px}.text-preview input[type=datetime-local]{width:184px}
.text-preview select{max-width:150px}.text-preview .text-review{white-space:normal;min-width:130px;max-width:220px;font-size:12px;color:#875c13}
.text-preview .icon-button{width:30px;min-height:30px}.text-preview .text-attention{background:#fffbef}
.text-empty{min-height:160px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;margin:0;border:1px solid #dce6f1;color:#697f94;background:#fff}
.text-footer{position:sticky;bottom:0;z-index:5;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;flex-shrink:0;border-top:1px solid #dbe4ef;padding:9px 0;background:#f7f9fc}
.text-footer>span{color:#60768c;font-size:12px}.text-footer .pagination{margin:0;gap:4px}.text-footer .pagination button{min-height:30px;width:30px}.text-footer select{min-height:30px;padding:4px;font-size:12px}
.text-footer>.primary{min-height:36px;font-size:13px}
@media(max-width:1000px){.text-input-bar{flex-wrap:wrap}.text-input-bar textarea{flex-basis:100%}.text-source-picker select{width:190px}.text-toolbar{gap:6px}}
</style>
