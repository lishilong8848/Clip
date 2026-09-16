<template>
  <main class="batch-page">
    <header class="page-heading">
      <VnetBackButton :to="backTarget" />
      <div><h1>{{ batchId ? '上下电待办详情' : mode === 'new' ? '机柜批量登记' : '上下电待办' }}</h1><p>识别和手工录入先进入待办，人工确认后才写入正式台账。</p></div>
      <div class="actions">
        <button v-if="mode !== 'new' && !batchId" class="primary" @click="openNew"><Files :size="16" />批量登记</button>
        <button v-if="batchId" @click="openList"><ClipboardList :size="16" />返回待办</button>
        <button :disabled="loading" @click="reload"><RefreshCw :size="16" :class="{ spin: loading }" />刷新</button>
      </div>
    </header>

    <p v-if="error" class="notice danger" role="alert">{{ error }}<button aria-label="关闭错误" @click="error = ''"><X :size="16" /></button></p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>

    <template v-if="mode === 'new' && !batchId">
      <nav class="tabs" aria-label="批量登记方式"><button :class="{ active: createMode === 'pdf' }" @click="createMode = 'pdf'">PDF识别</button><button :class="{ active: createMode === 'manual' }" @click="createMode = 'manual'; loadDirectory()">手工批量</button></nav>
      <section v-if="createMode === 'pdf'" class="entry-section">
        <h2>上传确认单</h2>
        <p>支持系统导出的文本PDF；每批最多10份、单份10MiB、合计30MiB。扫描件和加密PDF请使用手工批量。</p>
        <label class="file-drop" :class="{ dragging }" tabindex="0" aria-label="选择、拖入或粘贴PDF确认单" @dragenter.prevent="dragging = true" @dragover.prevent="dragging = true" @dragleave.prevent="dragging = false" @drop.prevent="dropFiles" @keydown.enter.prevent="fileInput?.click()" @keydown.space.prevent="fileInput?.click()"><Upload :size="24" /><strong>选择、拖入或粘贴PDF确认单</strong><span class="drop-hint">将文件拖到这里，或按 Ctrl+V 粘贴已复制的 PDF</span><input ref="fileInput" type="file" accept="application/pdf,.pdf" multiple @change="selectFiles" /></label>
        <ul v-if="pdfFiles.length" class="file-list"><li v-for="(file,index) in pdfFiles" :key="file.name + file.size + file.lastModified"><FileText :size="16" /><span>{{ file.name }}</span><small>{{ formatBytes(file.size) }}</small><button class="icon-button" title="移除文件" :aria-label="'移除' + file.name" @click="pdfFiles.splice(index,1)"><Trash2 :size="15" /></button></li></ul>
        <div class="actions"><button class="primary" :disabled="uploading || !pdfFiles.length" @click="recognize"><Loader2 v-if="uploading" class="spin" :size="16" /><ScanText v-else :size="16" />{{ uploading ? `正在上传 ${pdfFiles.length} 份文件` : '识别并创建待办' }}</button></div>
      </section>

      <section v-else class="entry-section">
        <h2>手工批量填写</h2>
        <div class="common-fields">
          <label>楼栋<select v-model="manualScope" @change="loadDirectory"><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select></label>
          <label>操作类型<select v-model="common.action"><option value="">请选择</option><option v-for="action in actions" :key="action">{{ action }}</option></select></label>
          <label>期望完成时间<input v-model="common.expected" type="datetime-local" step="1" /></label>
          <label>实际完成时间<input v-model="common.actual" type="datetime-local" step="1" @change="defaultExpected" /></label>
          <label>结果<select v-model="common.result"><option>成功</option><option>失败</option></select></label>
        </div>
        <div class="manual-source">
          <div><label>从机柜目录多选</label><input v-model="directorySearch" placeholder="搜索包间或机柜" /><select v-model="pickedRacks" multiple size="9" aria-label="机柜目录多选"><option v-for="rack in filteredDirectory" :key="rack.room + '/' + rack.rack" :value="rack.room + '/' + rack.rack">{{ rack.room }}，{{ rack.rack }} · {{ rack.rack_type || '未填写类型' }}</option></select><button :disabled="!pickedRacks.length" @click="addPicked"><Plus :size="16" />添加所选机柜</button></div>
          <div><label>粘贴Excel双列（包间，机柜）</label><textarea v-model="pastedRows" rows="9" placeholder="201&#9;A01&#10;201&#9;A02"></textarea><button :disabled="!pastedRows.trim()" @click="addPasted"><ClipboardPaste :size="16" />解析并添加</button></div>
        </div>
        <div class="section-heading"><h3>待创建明细（{{ manualRows.length }}）</h3><button :disabled="!manualRows.length" @click="applyManualCommon">将公共字段应用到全部</button></div>
        <div class="table-wrap"><table><thead><tr><th>楼栋</th><th>包间</th><th>机柜</th><th>机柜类型</th><th>操作类型</th><th>期望完成时间</th><th>实际完成时间</th><th>结果</th><th>操作</th></tr></thead><tbody><tr v-for="(row,index) in manualRows" :key="row._id"><td>{{ row.scope }}楼</td><td><input v-model="row.room" /></td><td><input v-model="row.rack" /></td><td><select v-model="row.rack_type"><option value="">请选择</option><option>网络机柜</option><option>服务器机柜</option></select></td><td><select v-model="row.action"><option value="">请选择</option><option v-for="action in actions" :key="action">{{ action }}</option></select></td><td><input v-model="row.expected" type="datetime-local" step="1" /></td><td><input v-model="row.actual" type="datetime-local" step="1" /></td><td><select v-model="row.result"><option>成功</option><option>失败</option></select></td><td><button class="icon-button" title="移除" aria-label="移除" @click="manualRows.splice(index,1)"><Trash2 :size="16" /></button></td></tr></tbody></table></div>
        <div class="actions end"><button class="primary" :disabled="uploading || !manualRows.length" @click="createManual"><Save :size="16" />创建待办</button></div>
      </section>
    </template>

    <template v-else-if="batchId && batch.batch_id">
      <section class="batch-summary">
        <div class="summary-main"><strong>批次 {{ batch.batch_id.slice(0, 8) }}</strong><span :class="'status ' + batch.status">{{ statusLabel(batch.status) }}</span><small>{{ batch.created_at }} · {{ (batch.scopes || []).map((item: string) => item + '楼').join('、') || '识别中' }}</small></div>
        <div class="metrics"><span>总数<b>{{ batch.stats?.total || 0 }}</b></span><span>待新增<b>{{ batch.stats?.new || 0 }}</b></span><span>可确认<b>{{ batch.stats?.confirmable || 0 }}</b></span><span class="duplicate">重复<b>{{ batch.stats?.duplicate || 0 }}</b></span><span class="conflict">冲突<b>{{ batch.stats?.conflict || 0 }}</b></span><span>无效<b>{{ batch.stats?.invalid || 0 }}</b></span><span class="success-text">已完成<b>{{ batch.stats?.completed || 0 }}</b></span><span class="danger-text">失败<b>{{ batch.stats?.failed || 0 }}</b></span></div>
      </section>
      <section v-if="batch.source === 'notice'" class="notice-source">
        <div><small>来源</small><strong>{{ batch.source_notice?.notice_type || '上下电通告' }}</strong></div>
        <div><small>通告名称</small><strong>{{ batch.source_notice?.title || '-' }}</strong></div>
        <div><small>计划时间</small><span>{{ batch.source_notice?.start_time || '-' }} ~ {{ batch.source_notice?.end_time || '-' }}</span></div>
        <div><small>数量核对</small><span>声明 {{ batch.notice_counts?.declared ?? '-' }} 柜 · 唯一 {{ batch.notice_counts?.unique ?? batch.stats?.total ?? 0 }} 柜 · 目录匹配 {{ batch.notice_counts?.directory_matched ?? 0 }} 柜</span></div>
        <div class="notice-source-cabinets"><small>原始柜号</small><span>{{ batch.source_notice?.cabinet || '-' }}</span></div>
      </section>
      <section v-if="batch.status === 'recognizing'" class="notice" role="status"><Loader2 class="spin" :size="18" /><div class="progress-copy"><strong>正在识别确认单</strong><progress :value="batch.progress?.pages_done || batch.progress?.files_done || 0" :max="batch.progress?.pages_total || batch.progress?.files_total || 1"></progress><small>文件 {{ batch.progress?.files_done || 0 }}/{{ batch.progress?.files_total || 0 }} · 页面 {{ batch.progress?.pages_done || 0 }}/{{ batch.progress?.pages_total || 0 }}</small></div></section>
      <div v-if="batch.error" class="notice danger" role="alert"><span>{{ batch.error }}</span><button v-if="batch.validation_error" :disabled="saving" @click="revalidate">重新校验</button></div>
      <div v-if="batch.blocking_warnings?.length" class="notice warning" role="alert"><AlertTriangle :size="18" /><span><b>通告明细需要核对</b><small v-for="warning in batch.blocking_warnings" :key="warning.code">{{ warning.message }}</small></span><strong v-if="batch.warnings_acknowledged" class="success-text">已人工核对</strong><button v-else :disabled="saving" @click="confirmDialog = 'warnings'">以当前明细为准</button></div>
      <div v-if="batch.stats?.duplicate" class="notice warning" role="alert"><AlertTriangle :size="18" /><span>全部文件已完成重叠检测，发现 {{ batch.stats.duplicate }} 条完全重叠记录。可清空重叠行，既有台账不会被修改。</span><button @click="confirmDialog = 'overlap'">清空重叠数据</button></div>
      <section v-if="batch.files?.length" class="source-files"><h3>来源文件</h3><div v-for="file in batch.files" :key="file.file_id"><FileText :size="16" /><a v-if="batch.can_download_files && !file.cleaned_at" :href="api + '/batches/' + batch.batch_id + '/files/' + file.file_id">{{ file.name }}</a><span v-else>{{ file.name }}{{ file.cleaned_at ? '（已清理）' : '' }}</span><small>{{ file.processed_pages || 0 }}/{{ file.pages || 0 }} 页 · {{ statusLabel(file.status) }}</small><b v-if="file.error">{{ file.error }}</b><button v-if="batch.can_download_files && !file.cleaned_at && ['completed','cancelled'].includes(batch.status)" class="icon-button" title="清理原确认单" aria-label="清理原确认单" @click="requestFileCleanup(file.file_id)"><Trash2 :size="16" /></button></div></section>

      <section v-if="batch.rows?.length" class="batch-actions">
        <div class="row-filters"><select v-model="rowScopeFilter"><option value="">全部楼栋</option><option v-for="item in batch.scopes || []" :key="item" :value="item">{{ item }}楼</option></select><select v-model="rowStatusFilter"><option value="">全部状态</option><option value="ready">可确认</option><option value="duplicate">重复</option><option value="conflict">冲突</option><option value="invalid">无效</option><option value="failed">失败</option><option value="completed">已完成</option><option value="excluded">已排除</option></select></div>
        <div class="bulk-fields"><select v-model="bulk.action"><option value="">操作类型不修改</option><option v-for="action in actions" :key="action">{{ action }}</option></select><input v-model="bulk.expected" type="datetime-local" step="1" aria-label="批量期望完成时间" /><input v-model="bulk.actual" type="datetime-local" step="1" aria-label="批量实际完成时间" /><select v-model="bulk.result"><option value="">结果不修改</option><option>成功</option><option>失败</option></select><select v-model="bulk.type_resolution"><option value="">类型处理不修改</option><option value="keep_current">沿用当前机柜类型</option><option value="sync_current">同步修正当前机柜类型</option></select><button :disabled="!selectedRows.length" @click="applyBulk">应用到已选 {{ selectedRows.length }} 行</button><button :disabled="saving || !selectedRows.length" @click="excludeSelected"><Trash2 :size="16" />排除已选</button></div>
        <div class="actions"><button :disabled="saving || !dirtyCount" @click="saveChanges"><Save :size="16" />保存更正<span v-if="dirtyCount">（{{ dirtyCount }}）</span></button><select v-model="confirmScope" aria-label="按楼确认"><option value="">选择楼栋</option><option v-for="item in batch.allowed_scopes || []" :key="item" :value="item">{{ item }}楼</option></select><button :disabled="saving || !confirmScope || warningsPending" @click="confirmRows({ scope: confirmScope })">确认本楼</button><button :disabled="saving || !selectedRows.length || warningsPending" @click="confirmRows({ row_ids: selectedRows })">确认已选</button><button class="primary" :disabled="saving || !batch.stats?.confirmable || !batch.can_confirm_all || warningsPending" :title="warningsPending ? '请先核对通告明细异常' : batch.can_confirm_all ? '' : '整批确认需要拥有全部楼栋权限'" @click="confirmDialog = 'all'">确认整批</button><button :disabled="saving || batch.status === 'completed'" @click="confirmDialog = 'cancel'">作废未提交行</button></div>
      </section>

      <div v-if="batch.rows?.length" class="table-wrap detail-table"><table><thead><tr><th><input type="checkbox" :checked="allVisibleSelected" aria-label="选择本页" @change="toggleVisible" /></th><th>状态</th><th>楼栋</th><th>包间</th><th>机柜</th><th>供应商机柜号</th><th>机柜类型</th><th>类型明细</th><th>操作类型</th><th>期望完成时间</th><th>实际完成时间</th><th>结果</th><th>下单时间</th><th>来源</th><th>操作</th></tr></thead><tbody>
        <tr v-for="row in pagedRows" :key="row.row_id" :class="row.status">
          <td><input v-if="row.editable || row.confirmable" v-model="selectedRows" type="checkbox" :value="row.row_id" :aria-label="'选择' + row.rack" /></td>
          <td><span :class="'status ' + row.status">{{ statusLabel(row.status) }}</span><small v-for="issue in row.issues || []" :key="issue.code">{{ issue.message }}</small><small v-if="row.error" class="danger-text">{{ row.error }}</small></td>
          <td><select v-model="row.scope" :disabled="!row.editable" :class="{ corrected: corrected(row,'scope') }"><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select></td>
          <td><input v-model="row.room" :disabled="!row.editable" :class="{ corrected: corrected(row,'room') }" /></td>
          <td><input v-model="row.rack" :disabled="!row.editable" :class="{ corrected: corrected(row,'rack') }" /></td>
          <td><input v-model="row.supplier_rack" :disabled="!row.editable" :class="{ corrected: corrected(row,'supplier_rack') }" /></td>
          <td><select v-model="row.rack_type" :disabled="!row.editable" :class="{ corrected: corrected(row,'rack_type') }"><option value="">请选择</option><option>网络机柜</option><option>服务器机柜</option></select><select v-if="row.current_rack_type && row.rack_type !== row.current_rack_type" v-model="row.type_resolution" :disabled="!row.editable"><option value="">请选择类型处理方式</option><option value="keep_current">沿用当前类型 {{ row.current_rack_type }}</option><option value="sync_current">同步修正当前机柜类型</option></select></td>
          <td><input v-model="row.type_detail" :disabled="!row.editable" :class="{ corrected: corrected(row,'type_detail') }" /></td>
          <td><select v-model="row.action" :disabled="!row.editable" :class="{ corrected: corrected(row,'action') }"><option value="">请选择</option><option v-for="action in actions" :key="action">{{ action }}</option></select><small v-if="row.inference">{{ row.inference }}</small></td>
          <td><input :value="localDate(row.expected)" type="datetime-local" step="1" :disabled="!row.editable" :class="{ corrected: corrected(row,'expected') }" @input="row.expected = inputDate($event)" /></td>
          <td><input :value="localDate(row.actual)" type="datetime-local" step="1" :disabled="!row.editable" :class="{ corrected: corrected(row,'actual') }" @input="row.actual = inputDate($event)" /></td>
          <td><select v-model="row.result" :disabled="!row.editable" :class="{ corrected: corrected(row,'result') }"><option value="">请选择</option><option>成功</option><option>失败</option></select></td>
          <td><input :value="localDate(row.order_time)" type="datetime-local" step="1" :disabled="!row.editable" :class="{ corrected: corrected(row,'order_time') }" @input="row.order_time = inputDate($event)" /></td>
          <td><span>{{ row.file_name }}</span><small v-if="row.page">第 {{ row.page }} 页 · 源行 {{ row.source_row }}</small><small v-if="row.application_ids?.length" :title="row.application_ids.join('、')">申请单 {{ applicationSummary(row) }}</small><small v-if="row.application_time">申请于 {{ row.application_time }}</small><details v-if="row.edits?.length" class="edit-audit"><summary>已更正 {{ row.edits.length }} 项</summary><small v-for="(edit,index) in row.edits" :key="index">{{ fieldLabels[edit.field] || edit.field }}：原值“{{ edit.before || '未填写' }}” → “{{ edit.after === '' ? '已清空' : edit.after }}”</small></details></td>
          <td><button v-if="row.editable && !String(row.status).startsWith('excluded_')" class="icon-button" title="从待办排除" aria-label="从待办排除" @click="toggleExcluded(row,true)"><Trash2 :size="16" /></button><button v-else-if="row.editable" @click="toggleExcluded(row,false)">恢复</button></td>
        </tr>
      </tbody></table></div>
      <footer v-if="batch.rows?.length" class="pagination"><span>共 {{ filteredRows.length }} 条</span><button :disabled="rowPage <= 1" @click="rowPage--"><ChevronLeft :size="16" /></button><template v-for="page in rowPages" :key="page"><button :class="{ active: rowPage === page }" @click="rowPage = page">{{ page }}</button></template><button :disabled="rowPage >= rowPageCount" @click="rowPage++"><ChevronRight :size="16" /></button></footer>
    </template>

    <template v-else>
      <section class="filters"><select v-model="listScope"><option value="">全部楼栋</option><option v-for="item in buildingOptions" :key="item.value" :value="item.value">{{ item.label }}</option></select><select v-model="listStatus"><option value="">全部状态</option><option value="pending">待处理</option><option value="running">提交中</option><option value="partial">部分完成</option><option value="completed">已完成</option><option value="cancelled">已作废</option><option value="failed">失败</option></select><input v-model="listFrom" type="date" aria-label="开始日期" /><input v-model="listTo" type="date" aria-label="结束日期" /><button @click="loadList(1)"><Search :size="16" />查询</button></section>
      <div class="table-wrap"><table><thead><tr><th>批次</th><th>来源</th><th>楼栋</th><th>状态</th><th>总数</th><th>可确认</th><th>异常</th><th>创建时间</th><th>操作</th></tr></thead><tbody><tr v-for="item in list.items || []" :key="item.batch_id"><td>{{ item.batch_id.slice(0,8) }}</td><td>{{ sourceLabel(item.source) }}</td><td>{{ (item.scopes || []).map((value: string) => value + '楼').join('、') }}</td><td><span :class="'status ' + item.status">{{ statusLabel(item.status) }}</span></td><td>{{ item.stats?.total || 0 }}</td><td>{{ item.stats?.confirmable || 0 }}</td><td>{{ (item.stats?.duplicate || 0) + (item.stats?.conflict || 0) + (item.stats?.invalid || 0) + (item.stats?.failed || 0) }}</td><td>{{ item.created_at }}</td><td><button class="link" @click="openBatch(item.batch_id)">查看处理</button></td></tr></tbody></table><p v-if="!loading && !list.total" class="empty">暂无上下电待办</p></div>
      <footer v-if="list.total" class="pagination"><span>共 {{ list.total }} 个批次</span><button :disabled="list.page <= 1" @click="loadList(list.page - 1)"><ChevronLeft :size="16" /></button><button v-for="page in listPages" :key="page" :class="{ active: list.page === page }" @click="loadList(page)">{{ page }}</button><button :disabled="list.page >= listPageCount" @click="loadList(list.page + 1)"><ChevronRight :size="16" /></button></footer>
    </template>

    <ConfirmDialog :open="Boolean(confirmDialog)" tone="warning" :title="confirmTitle" :message="confirmMessage" :confirm-label="confirmLabel" @resolve="resolveConfirm" />
    <ConfirmDialog :open="discardOpen" tone="warning" title="放弃未保存的批次修改？" message="继续后，本页尚未保存的字段修改会丢失。" confirm-label="放弃修改" cancel-label="继续编辑" @resolve="resolveDiscard" />
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue';
import { AlertTriangle, ChevronLeft, ChevronRight, ClipboardList, ClipboardPaste, FileText, Files, Loader2, Plus, RefreshCw, Save, ScanText, Search, Trash2, Upload, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
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
const editableFields = ['scope','room','rack','supplier_rack','rack_type','type_detail','action','expected','actual','result','order_time','type_resolution'];
const fieldLabels: Dict = {scope:'楼栋',room:'包间',rack:'机柜',supplier_rack:'供应商机柜号',rack_type:'机柜类型',type_detail:'类型明细',action:'操作类型',expected:'期望完成时间',actual:'实际完成时间',result:'结果',order_time:'下单时间',type_resolution:'类型处理',excluded:'排除状态'};
const read = (path: string, query: Dict = {}, timeoutMs = 90000) => requestJson(`${api}/${path}?${new URLSearchParams(query as Record<string,string>)}`, { timeoutMs });
const write = (path: string, body: Dict, method = 'POST') => requestJson(`${api}/${path}`, { method, body: JSON.stringify(body), timeoutMs: 90000 });
const backTarget = computed(() => batchId ? `/cabinet-power/batches${props.scope ? '?scope=' + props.scope : ''}` : mode === 'new' ? `/cabinet-power/batches${props.scope ? '?scope=' + props.scope : ''}` : props.scope ? `/cabinet-power?scope=${props.scope}` : '/cabinet-power');
const buildingOptions = computed(() => {
  const map = new Map<string,string>();
  for (const item of props.scopeOptions || []) { const value = String(item.value || '').toUpperCase(); if (/^[A-E]$/.test(value)) map.set(value,item.label || value + '楼'); }
  if (props.isAdmin) for (const value of 'ABCDE') map.set(value,value + '楼');
  if (/^[A-E]$/.test(props.scope)) map.set(props.scope,props.scope + '楼');
  return [...map].map(([value,label]) => ({ value,label }));
});
const loading = ref(false), uploading = ref(false), saving = ref(false), error = ref(''), message = ref('');
const batch = ref<Dict>({}), list = ref<Dict>({}), baseline = new Map<string,string>();
const createMode = ref('pdf'), pdfFiles = ref<File[]>([]), fileInput = ref<HTMLInputElement>(), dragging = ref(false), manualScope = ref(props.scope || buildingOptions.value[0]?.value || 'A');
const common = reactive({ action:'', expected:'', actual:'', result:'成功' }), bulk = reactive({ action:'', expected:'', actual:'', result:'', type_resolution:'' });
const directory = ref<Dict[]>([]), directorySearch = ref(''), pickedRacks = ref<string[]>([]), pastedRows = ref(''), manualRows = ref<Dict[]>([]);
const selectedRows = ref<string[]>([]), confirmScope = ref(props.scope || ''), rowPage = ref(1), rowScopeFilter = ref(props.scope || ''), rowStatusFilter = ref(''), listScope = ref(props.scope || ''), listStatus = ref(''), listFrom = ref(''), listTo = ref('');
const confirmDialog = ref(''), discardOpen = ref(false);
const cleanupFileId = ref('');
let pollTimer: number | undefined, disposed = false, pendingNavigation: (() => void) | undefined, removeGuard: (() => void) | undefined;

const filteredDirectory = computed(() => directory.value.filter(row => !directorySearch.value || `${row.room} ${row.rack}`.toUpperCase().includes(directorySearch.value.toUpperCase())).slice(0,500));
const filteredRows = computed<Dict[]>(() => (batch.value.rows || []).filter((row:Dict) => (!rowScopeFilter.value || row.scope===rowScopeFilter.value) && (!rowStatusFilter.value || row.status===rowStatusFilter.value || rowStatusFilter.value==='excluded'&&String(row.status).startsWith('excluded_'))));
const rowPageCount = computed(() => Math.max(1,Math.ceil(filteredRows.value.length/50)));
const pagedRows = computed<Dict[]>(() => filteredRows.value.slice((rowPage.value-1)*50,rowPage.value*50));
const rowPages = computed(() => pageNumbers(rowPage.value,rowPageCount.value));
const listPageCount = computed(() => Math.max(1,Math.ceil((list.value.total || 0)/(list.value.page_size || 20))));
const listPages = computed(() => pageNumbers(list.value.page || 1,listPageCount.value));
const allVisibleSelected = computed(() => Boolean(pagedRows.value.length) && pagedRows.value.filter(row => row.editable || row.confirmable).every(row => selectedRows.value.includes(row.row_id)));
const dirtyRows = computed(() => (batch.value.rows || []).filter((row: Dict) => baseline.get(row.row_id) !== rowSignature(row) && row.editable));
const dirtyCount = computed(() => dirtyRows.value.length);
const warningsPending = computed(() => Boolean(batch.value.blocking_warnings?.length && !batch.value.warnings_acknowledged));
const confirmTitle = computed(() => confirmDialog.value === 'overlap' ? '清空完全重叠的待办行？' : confirmDialog.value === 'all' ? '确认整批有效记录？' : confirmDialog.value === 'file' ? '清理原确认单？' : confirmDialog.value === 'warnings' ? '确认以当前机柜明细为准？' : '作废尚未提交的记录？');
const confirmMessage = computed(() => confirmDialog.value === 'overlap' ? '只排除当前批次中的重复行，既有台账和多维表不会被修改；排除记录仍保留审计并可恢复。' : confirmDialog.value === 'all' ? `将并行确认各楼共 ${batch.value.stats?.confirmable || 0} 条有效记录，异常行会留在待办。` : confirmDialog.value === 'file' ? '清理后无法再次下载原PDF，已保存的识别值、更正审计和正式台账不受影响。' : confirmDialog.value === 'warnings' ? '数量、重复或目录匹配存在异常。确认后将允许对当前有效行正式提交，后续修改机柜明细会自动取消本次确认。' : '已完成记录保留，其他尚未提交的行将标记为作废。');
const confirmLabel = computed(() => confirmDialog.value === 'overlap' ? '清空重叠数据' : confirmDialog.value === 'all' ? '确认整批' : confirmDialog.value === 'file' ? '清理原确认单' : confirmDialog.value === 'warnings' ? '确认已核对' : '作废未提交行');

function pageNumbers(current: number,total: number): number[] { return [...new Set([1,total,...Array.from({length:5},(_,index)=>current+index-2)])].filter(value=>value>=1&&value<=total).sort((a,b)=>a-b); }
function statusLabel(status: string): string { return ({ waiting:'等待',parsing:'解析中',recognizing:'识别中',pending:'待处理',ready:'可确认',duplicate:'重复',conflict:'冲突',invalid:'无效',queued:'排队中',writing:'写入中',running:'提交中',partial:'部分完成',completed:'已完成',failed:'失败',cancelled:'已作废',excluded_duplicate:'已排除重复',excluded_manual:'已排除',excluded_cancelled:'已作废' } as Dict)[status] || status || '未知'; }
function sourceLabel(source: string): string { return source === 'pdf' ? 'PDF识别' : source === 'notice' ? '上下电通告' : '手工批量'; }
function formatBytes(value: number): string { return value < 1024*1024 ? `${Math.ceil(value/1024)} KiB` : `${(value/1024/1024).toFixed(1)} MiB`; }
function localDate(value: string): string { return String(value || '').replace(' ','T'); }
function inputDate(event: Event): string { return (event.target as HTMLInputElement).value.replace('T',' '); }
function rowSignature(row: Dict): string { return JSON.stringify(Object.fromEntries(editableFields.map(key => [key,String(row[key] || '')]))); }
function corrected(row: Dict,key: string): boolean { return row.original && String(row[key] || '') !== String(row.original[key] || ''); }
function applicationSummary(row:Dict):string{return row.application_ids.length===1?row.application_ids[0]:`${row.application_ids[0]} 等 ${row.application_ids.length} 个`;}
function snapshotRows(): void { baseline.clear(); for (const row of batch.value.rows || []) baseline.set(row.row_id,rowSignature(row)); }
function openNew(): void { navigate(`/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? {scope:props.scope}:{}),mode:'new' })}`); }
function openList(): void { navigate(`/cabinet-power/batches${props.scope ? '?scope='+props.scope : ''}`); }
function openBatch(id: string): void { navigate(`/cabinet-power/batches?${new URLSearchParams({ ...(props.scope ? {scope:props.scope}:{}),batch_id:id })}`); }
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
function pasteFiles(event: ClipboardEvent): void {
  const target=event.target;
  if (mode!=='new' || batchId || createMode.value!=='pdf' || uploading.value || target instanceof Element && target.closest('input,textarea,[contenteditable="true"]')) return;
  const data=event.clipboardData; if (!data) return;
  const files=data.files.length ? Array.from(data.files) : Array.from(data.items).filter(item=>item.kind==='file').map(item=>item.getAsFile()).filter((file):file is File=>Boolean(file));
  if (!files.length) return; event.preventDefault(); addPdfFiles(files);
}
function defaultExpected(): void { if (!common.expected) common.expected=common.actual; }

async function recognize(): Promise<void> {
  uploading.value=true; error.value='';
  try { const form=new FormData(); for (const file of pdfFiles.value) form.append('files',file,file.name); const data=await requestJson(`${api}/batches/recognize`,{method:'POST',body:form,timeoutMs:180000}); openBatch(data.batch_id); }
  catch (exc:any) { error.value=exc.message || 'PDF上传失败'; } finally { uploading.value=false; }
}
async function loadDirectory(): Promise<void> {
  if (!manualScope.value) return;
  try { directory.value=(await read('racks',{scope:manualScope.value})).items || []; }
  catch (exc:any) { error.value=exc.message || '机柜目录读取失败'; }
}
function makeManual(room: string,rack: string): Dict {
  const found=directory.value.find(item=>item.room===room&&item.rack===rack);
  return {_id:crypto.randomUUID(),scope:manualScope.value,room,rack,rack_type:found?.rack_type || '',supplier_rack:'',type_detail:'',action:common.action,expected:common.expected || common.actual,actual:common.actual,result:common.result,order_time:''};
}
function addPicked(): void { const existing=new Set(manualRows.value.map(row=>`${row.scope}/${row.room}/${row.rack}`)); for (const value of pickedRacks.value) { const [room,rack]=value.split('/'); if (!existing.has(`${manualScope.value}/${room}/${rack}`)) manualRows.value.push(makeManual(room,rack)); } pickedRacks.value=[]; }
function normalizeRoom(value: string): string { const text=value.trim().toUpperCase(); const match=text.match(/(?:EA118[-_.])?([A-E])([1-4])[-_](\d{1,2})/); return match ? `${match[2]}${String(Number(match[3])).padStart(2,'0')}` : text; }
function addPasted(): void { for (const line of pastedRows.value.split(/\r?\n/)) { const [room,rack]=line.trim().split(/[\t,，;；]+/).map(value=>value.trim()); if (room&&rack) manualRows.value.push(makeManual(normalizeRoom(room),rack.toUpperCase())); } pastedRows.value=''; }
function applyManualCommon(): void { for (const row of manualRows.value) Object.assign(row,{action:common.action || row.action,expected:common.expected || common.actual || row.expected,actual:common.actual || row.actual,result:common.result || row.result}); }
async function createManual(): Promise<void> { uploading.value=true; error.value=''; try { const rows=manualRows.value.map(({_id,...row})=>row); const data=await write('batches',{rows}); openBatch(data.batch_id); } catch(exc:any){error.value=exc.message||'待办创建失败';} finally{uploading.value=false;} }

async function loadBatch(): Promise<void> {
  if (!batchId) return; loading.value=true;
  try { batch.value=await read(`batches/${batchId}`); snapshotRows(); error.value=''; if (['recognizing','running'].includes(batch.value.status)) schedulePoll(); }
  catch(exc:any){error.value=exc.message||'批次读取失败';if(['recognizing','running'].includes(batch.value.status))schedulePoll();} finally{loading.value=false;}
}
async function loadList(page=1): Promise<void> { loading.value=true; try { list.value=await read('batches',{scope:listScope.value,status:listStatus.value,from:listFrom.value,to:listTo.value,page:String(page),page_size:'20'}); error.value=''; } catch(exc:any){error.value=exc.message||'待办读取失败';} finally{loading.value=false;} }
function schedulePoll(): void { window.clearTimeout(pollTimer); if (!disposed) pollTimer=window.setTimeout(async()=>{ if (!dirtyCount.value) await loadBatch(); else schedulePoll(); },1200); }
async function reload(): Promise<void> { if (batchId) await loadBatch(); else await loadList(list.value.page || 1); }
function toggleVisible(event: Event): void { const ids=pagedRows.value.filter(row=>row.editable||row.confirmable).map(row=>row.row_id); if ((event.target as HTMLInputElement).checked) selectedRows.value=[...new Set([...selectedRows.value,...ids])]; else selectedRows.value=selectedRows.value.filter(id=>!ids.includes(id)); }
function applyBulk(): void { for (const row of batch.value.rows || []) if (selectedRows.value.includes(row.row_id)&&row.editable) for (const key of ['action','expected','actual','result','type_resolution']) if (bulk[key as keyof typeof bulk]) row[key]=String(bulk[key as keyof typeof bulk]).replace('T',' '); }
async function saveChanges(): Promise<void> { const rows=dirtyRows.value.map((row:Dict)=>({row_id:row.row_id,...Object.fromEntries(editableFields.map(key=>[key,row[key]||'']))})); if(!rows.length)return; saving.value=true; try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows},'PATCH');snapshotRows();message.value='批次更正已保存';}catch(exc:any){error.value=exc.message||'保存失败';}finally{saving.value=false;} }
async function acknowledgeWarnings(): Promise<void> { if(dirtyCount.value){error.value='请先保存机柜明细更正，再确认异常。';return;} saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[],acknowledge_warnings:true},'PATCH');snapshotRows();message.value='已记录人工核对结果';}catch(exc:any){error.value=exc.message||'异常核对保存失败';}finally{saving.value=false;} }
async function revalidate(): Promise<void> { saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[]},'PATCH');snapshotRows();message.value='批次已重新校验';}catch(exc:any){error.value=exc.message||'重新校验失败';}finally{saving.value=false;} }
async function toggleExcluded(row: Dict,excluded: boolean): Promise<void> { saving.value=true; try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,rows:[{row_id:row.row_id,excluded}]},'PATCH');snapshotRows();}catch(exc:any){error.value=exc.message||'操作失败';}finally{saving.value=false;} }
async function excludeSelected(): Promise<void> { saving.value=true;try{batch.value=await write(`batches/${batchId}`,{version:batch.value.version,row_ids:selectedRows.value,common:{excluded:true}},'PATCH');selectedRows.value=[];snapshotRows();message.value='已排除所选待办行';}catch(exc:any){error.value=exc.message||'批量排除失败';}finally{saving.value=false;} }
function requestFileCleanup(fileId:string):void{cleanupFileId.value=fileId;confirmDialog.value='file';}
async function confirmRows(payload: Dict): Promise<void> { if(dirtyCount.value){error.value='请先保存字段更正，再确认正式写入。';return;} saving.value=true; try{batch.value=await write(`batches/${batchId}/confirm`,{version:batch.value.version,...payload});snapshotRows();selectedRows.value=[];message.value='已提交后台处理，各楼会并行写入';schedulePoll();}catch(exc:any){error.value=exc.message||'确认失败';}finally{saving.value=false;} }
async function resolveConfirm(confirmed: boolean): Promise<void> { const action=confirmDialog.value,fileId=cleanupFileId.value;confirmDialog.value='';cleanupFileId.value='';if(!confirmed)return;if(action==='warnings'){await acknowledgeWarnings();return;}saving.value=true;try{if(action==='overlap')batch.value=await write(`batches/${batchId}/clear-overlaps`,{version:batch.value.version});else if(action==='all'){saving.value=false;await confirmRows({all:true});return;}else if(action==='file')batch.value=await write(`batches/${batchId}/files/${fileId}/cleanup`,{});else batch.value=await write(`batches/${batchId}/cancel`,{});snapshotRows();}catch(exc:any){error.value=exc.message||'操作失败';}finally{saving.value=false;} }
function resolveDiscard(confirmed:boolean):void{discardOpen.value=false;const proceed=pendingNavigation;pendingNavigation=undefined;if(confirmed)proceed?.();}

watch(rowPageCount,count=>{rowPage.value=Math.min(rowPage.value,count);});
watch([rowScopeFilter,rowStatusFilter],()=>{rowPage.value=1;});
onMounted(async()=>{window.addEventListener('paste',pasteFiles);removeGuard=registerNavigationGuard((_target,proceed)=>{if(!dirtyCount.value)return true;pendingNavigation=proceed;discardOpen.value=true;return false;});if(batchId)await loadBatch();else if(mode!=='new')await loadList();if(mode==='new'&&createMode.value==='manual')await loadDirectory();});
onBeforeUnmount(()=>{disposed=true;window.clearTimeout(pollTimer);window.removeEventListener('paste',pasteFiles);removeGuard?.();});
</script>

<style scoped>
.batch-page{max-width:1800px;margin:auto;min-height:80vh;padding:24px;color:#203650;background:#f7f9fc;font-size:14px;letter-spacing:0}.page-heading{display:flex;align-items:center;gap:18px;margin-bottom:22px}.page-heading>div:nth-child(2){flex:1;min-width:0}.page-heading h1{margin:0 0 7px;font-size:26px}.page-heading p,.entry-section>p{margin:0;color:#63768c;font-size:12px}.actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.actions.end{justify-content:flex-end;margin-top:16px}button,a,input,select,textarea{font:inherit;box-sizing:border-box}button,.actions a{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:38px;padding:9px 12px;border:1px solid #d4dfed;border-radius:6px;background:#fff;color:#214969;text-decoration:none;cursor:pointer}button:hover:not(:disabled){border-color:#8db7ec;background:#edf5ff}button:disabled{opacity:.45;cursor:not-allowed}.primary{border-color:#1764dd;background:#1764dd;color:#fff}.primary:hover:not(:disabled){background:#1154bc;color:#fff}input,select,textarea{min-height:38px;max-width:100%;padding:8px;border:1px solid #ccd9e8;border-radius:5px;background:#fff;color:#263f5b}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,.file-drop:focus-visible{outline:3px solid #76aaf0;outline-offset:2px}.notice{display:flex;align-items:center;gap:10px;margin:12px 0;padding:12px 16px;border:1px solid #cbdffb;border-radius:6px;background:#ebf4ff;overflow-wrap:anywhere}.notice button{margin-left:auto}.notice.danger{border-color:#f8c9cd;background:#fff0f1;color:#ae283e}.notice.success{border-color:#bee8d6;background:#eaf9f2;color:#167953}.notice.warning{border-color:#f0d797;background:#fff9e8;color:#745400}.tabs{display:flex;gap:8px;margin:18px 0;border-bottom:1px solid #dbe4ef;padding-bottom:12px}.tabs .active{border-color:#9ebfe8;background:#eaf2ff;color:#175dbb}.entry-section{padding:20px 0}.entry-section h2{margin:0 0 8px;font-size:20px}.file-drop{display:grid;place-items:center;gap:8px;min-height:150px;margin:20px 0;border:1px dashed #8ca9c7;background:#fff;color:#285777;cursor:pointer}.file-drop.dragging{border-color:#1764dd;background:#edf5ff}.file-drop input{max-width:360px;border:0}.drop-hint{color:#657b91;font-size:12px}.file-list{max-height:220px;margin:0 0 16px;padding:0;overflow:auto;list-style:none;background:#fff}.file-list li,.source-files>div{display:flex;align-items:center;gap:9px;padding:10px 12px;border-bottom:1px solid #e5edf5}.file-list small,.source-files small{margin-left:auto;color:#647b91}.common-fields{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:18px 0}.common-fields label,.manual-source label{display:flex;flex-direction:column;gap:6px;color:#4a6581;font-size:12px}.manual-source{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin:20px 0}.manual-source>div{display:flex;flex-direction:column;gap:8px}.manual-source select,.manual-source textarea{width:100%;min-height:190px}.section-heading{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:20px 0 10px}.section-heading h3,.source-files h3{margin:0;font-size:16px}.table-wrap{overflow:auto;border:1px solid #dce6f1;background:#fff}.table-wrap table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:13px}.table-wrap th{position:sticky;top:0;z-index:1;padding:12px;background:#edf3fa;color:#425c77;text-align:left}.table-wrap td{padding:10px;border-bottom:1px solid #e8eef5;vertical-align:top}.table-wrap tbody tr:hover{background:#f8fbff}.table-wrap input,.table-wrap select{min-width:115px}.table-wrap td small{display:block;max-width:260px;margin-top:5px;white-space:normal;color:#6a7f94;font-size:11px}.icon-button{width:38px;padding:0}.empty{padding:42px;text-align:center;color:#697f94}.batch-summary{display:flex;align-items:center;gap:24px;padding:18px 0;border-block:1px solid #dbe4ef;background:#fff}.summary-main{display:flex;min-width:180px;flex-direction:column;gap:7px;padding:0 18px}.summary-main>strong{font-size:18px}.summary-main small{color:#647b91}.metrics{display:grid;flex:1;grid-template-columns:repeat(7,minmax(74px,1fr))}.metrics span{padding:0 14px;border-left:1px solid #e8eef5;color:#60758c;font-size:12px}.metrics b{display:block;margin-top:6px;color:#203650;font-size:24px}.metrics .duplicate b,.metrics .conflict b,.danger-text{color:#b72e43}.success-text{color:#167953!important}.progress-copy{display:grid;flex:1;gap:6px}.progress-copy progress{width:100%;height:8px}.source-files{margin:18px 0}.source-files>h3{padding:0 0 10px}.source-files b{max-width:50%;color:#ae283e;font-size:11px}.batch-actions{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:18px 0;flex-wrap:wrap}.bulk-fields{display:flex;gap:8px;flex-wrap:wrap}.detail-table{max-height:64vh}.detail-table tr.duplicate,.detail-table tr.invalid,.detail-table tr.conflict,.detail-table tr.failed{background:#fff9ed}.detail-table tr.completed{background:#f0fbf6}.detail-table tr[class^="excluded_"]{opacity:.65;background:#f3f5f7}.corrected{border-color:#d89b20!important;background:#fff9e8!important}.status{display:inline-flex;width:max-content;border:1px solid #cbd8e6;border-radius:10px;padding:2px 7px;background:#f3f6fa;color:#536b82;font-size:11px}.status.ready,.status.completed{border-color:#a9dfc8;background:#eaf9f2;color:#167953}.status.duplicate,.status.conflict,.status.invalid,.status.failed{border-color:#f3c6cb;background:#fff0f1;color:#ae283e}.status.running,.status.queued,.status.writing,.status.recognizing,.status.parsing{border-color:#a9c9f2;background:#eaf3ff;color:#175dbb}.pagination{display:flex;align-items:center;justify-content:flex-end;gap:8px;margin-top:15px}.pagination>span{margin-right:auto;color:#60768c}.pagination button{width:38px;padding:0}.pagination button.active{border-color:#1764dd;background:#1764dd;color:#fff}.filters{display:flex;gap:9px;margin:16px 0;flex-wrap:wrap}.link{min-height:28px;padding:0;border:0;background:transparent;color:#175ebd}.spin{animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}
.notice-source{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:14px 0;padding:14px 16px;border:1px solid #dbe4ef;background:#fff}.notice-source>div{display:flex;min-width:0;flex-direction:column;gap:5px}.notice-source small{color:#667c92}.notice-source strong,.notice-source span{overflow-wrap:anywhere}.notice-source-cabinets{grid-column:1/-1}.notice.warning>span{display:grid;gap:4px}.notice.warning>span small{display:block}.notice.warning>strong{margin-left:auto;white-space:nowrap}
@media(max-width:1000px){.page-heading{flex-wrap:wrap}.page-heading>.actions{width:100%}.common-fields,.notice-source{grid-template-columns:repeat(2,minmax(0,1fr))}.manual-source{grid-template-columns:1fr}.batch-summary{align-items:stretch;flex-direction:column}.metrics{grid-template-columns:repeat(4,1fr);row-gap:14px}.batch-actions{align-items:stretch;flex-direction:column}.bulk-fields,.batch-actions>.actions{width:100%}}
@media(max-width:640px){.batch-page{padding:14px}.page-heading h1{font-size:22px}.common-fields,.notice-source{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.notice{font-size:13px}.source-files>div{align-items:flex-start;flex-wrap:wrap}.source-files small{margin-left:0}.source-files b{max-width:100%}}
@media(prefers-reduced-motion:reduce){*{animation:none!important}}
.metrics{grid-template-columns:repeat(8,minmax(70px,1fr))}.metrics span{padding-inline:12px}
.row-filters{display:flex;gap:8px;flex-wrap:wrap}
.edit-audit{max-width:280px;white-space:normal}.edit-audit summary{margin-top:5px;color:#9b6b00;cursor:pointer}.edit-audit small{overflow-wrap:anywhere}
@media(max-width:1000px){.metrics{grid-template-columns:repeat(4,1fr)}}
@media(max-width:640px){.metrics{grid-template-columns:repeat(2,1fr)}}
</style>
