<template>
  <section ref="root" class="tool-check" aria-label="工具管理" data-v2-inline-feedback>
    <header class="tool-header">
      <div class="tool-title">
        <NButton quaternary :aria-label="order ? '返回工单列表' : '返回专项功能'" :disabled="busy" @click="order ? leave(closeOrder) : back()"><ArrowLeft :size="19" /></NButton>
        <div>
          <p>{{ order ? `${order.scope}楼 · ${kindLabel(order)}工单` : '专项功能' }}</p>
          <h2>{{ order ? order.tool_name : '工具管理' }}</h2>
        </div>
      </div>
      <NButton class="tool-refresh" :disabled="busy" :loading="loading" @click="refresh"><RefreshCw :size="16" />刷新</NButton>
    </header>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NAlert v-if="message" type="success" :bordered="false" role="status">{{ message }}</NAlert>

    <NSpin v-if="!order" :show="loading">
      <template v-if="board">
        <nav class="tool-nav" aria-label="工具管理分区">
          <NButton v-for="tab in tabs" :key="tab.value" :type="view === tab.value ? 'primary' : 'default'" :bordered="false"
            :aria-pressed="view === tab.value" :disabled="busy" @click="view = tab.value">
            <component :is="tab.icon" :size="16" />{{ tab.label }}
            <span v-if="tab.count" class="tool-nav-count">{{ tab.count }}</span>
          </NButton>
        </nav>
        <NAlert v-if="board.inventory_error" class="tool-source-error" type="warning" :bordered="false">{{ board.inventory_error }}</NAlert>
        <div v-if="view === 'settings'" class="tool-settings-layout">
          <ToolManagementSettings />
          <section class="tool-library" aria-label="自检模板">
            <div class="tool-panel-heading">
              <div><h3>自检模板</h3><span>{{ board.templates.length }} 份 · XLSX</span></div>
              <NButton :disabled="busy" @click="chooseImport()"><Upload :size="16" />导入模板</NButton>
            </div>
            <p class="tool-muted">模板需沿用现有记录单布局。</p>
            <article v-for="template in board.templates" :key="template.id" class="tool-template">
              <div><strong>{{ template.name }}</strong><small>V{{ template.version }} · {{ template.schema.checks.length }} 项 · {{ template.active ? '已启用' : '已停用' }}</small></div>
              <div class="tool-bar">
                <NButton size="small" :disabled="busy" @click="chooseImport(template)">导入新版</NButton>
                <NButton size="small" :disabled="busy" @click="toggleTemplate(template)">{{ template.active ? '停用' : '启用' }}</NButton>
              </div>
            </article>
            <input ref="fileInput" type="file" accept=".xlsx" hidden @change="importTemplate" />
          </section>
        </div>
        <div v-else class="tool-worklist">
          <div class="tool-list-head">
            <div class="tool-counts">
              <template v-if="view === 'orders'">
                <span><strong>{{ pendingCount }}</strong> 待填写</span>
                <span><strong>{{ submittedCount }}</strong> 已提交</span>
              </template>
              <template v-else>
                <span><strong>{{ repairCount }}</strong> 检修中</span>
                <span><strong>{{ board.followups.length - repairCount }}</strong> 待报废审批</span>
              </template>
            </div>
            <div v-if="view === 'orders' && board.can_download_month" class="tool-archive">
              <NButton v-if="board.archived_at" :disabled="busy" @click="download(`/archive/${month}`)"><Download :size="16" />下载月度归档</NButton>
              <span v-else class="tool-muted">{{ board.orders.length && !pendingCount ? '正在生成月度归档' : '当月工单办结后自动归档' }}</span>
            </div>
          </div>
          <div class="tool-filters" aria-label="筛选工单">
            <NInput v-model:value="search" class="tool-search" clearable :input-props="{ 'aria-label': '搜索工具、楼栋、位置或领取人' }" placeholder="搜索工具或楼栋">
              <template #prefix><Search :size="16" /></template>
            </NInput>
            <input v-if="view === 'orders'" class="tool-month" type="month" aria-label="工单月份" :value="month" :disabled="busy" @change="changeMonth" />
            <NSelect v-if="view === 'orders'" v-model:value="kindFilter" aria-label="工单类型" :options="kindOptions" />
            <NSelect v-model:value="stateFilter" aria-label="工单状态" :options="stateOptions" />
          </div>
          <div v-if="visibleOrders.length" class="tool-orders" role="table" aria-label="工单列表">
            <div class="tool-column-labels" role="row">
              <span role="columnheader">工具与位置</span><span role="columnheader">楼栋与类型</span>
              <span role="columnheader">计划与人员</span><span role="columnheader">状态</span><span role="columnheader">操作</span>
            </div>
            <article v-for="row in pagedOrders" :key="row.id" class="tool-order" role="row">
              <div class="tool-order-name" role="cell">
                <strong>{{ row.tool_name }}</strong>
                <small v-if="row.model || row.location">{{ [row.model, row.location].filter(Boolean).join(' · ') }}</small>
              </div>
              <div class="tool-order-kind" role="cell"><strong>{{ row.scope }}楼</strong><small>{{ kindLabel(row) }}工单</small></div>
              <div class="tool-order-meta" role="cell">
                <span>计划 {{ row.due }}</span><small>{{ assignment(row) }}</small>
                <small v-if="row.submitted_at">提交 {{ row.submitted_at.slice(0,10) }}</small>
              </div>
              <div class="tool-order-state" role="cell">
                <NTag :class="{ 'tool-status-closed': row.status !== 'pending' && view === 'orders' }" :type="view === 'followups' ? 'warning' : row.status === 'pending' ? 'success' : 'default'" :bordered="false">{{ view === 'followups' ? outcomeLabels[row.outcome] : status(row) }}</NTag>
                <small v-if="row.outcome && view === 'orders'">{{ outcomeLabels[row.outcome] }}</small>
                <small v-if="row.sync.some(item => item.state !== 'done')" class="tool-pending">飞书待同步</small>
              </div>
              <div class="tool-order-actions" role="cell">
                <NButton size="small" :type="row.can_claim || row.can_edit || row.can_approve || row.can_repair ? 'primary' : 'default'"
                  :disabled="busy" @click="openOrder(row, row.can_claim)">{{ orderAction(row) }}</NButton>
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
            <NPagination v-if="visibleOrders.length > pageSize" :page="page" :item-count="visibleOrders.length" :page-size="pageSize" simple @update:page="changePage" />
          </footer>
        </div>
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
        <div v-if="order.sync.length" class="tool-bar tool-context">
          <NTag v-for="item in order.sync" :key="item.label" :type="item.state === 'done' ? 'success' : 'warning'"
            :title="item.error" :bordered="false">{{ item.label }} · {{ item.state === 'done' ? '已同步' : '待同步' }}</NTag>
        </div>
        <p v-for="item in order.sync.filter(item => item.error)" :key="item.label" class="tool-pending tool-context">{{ item.label }} · {{ item.error }}</p>
      </section>
      <div class="tool-form-layout" :class="{ 'tool-has-aside': order.kind === 'self' && (order.can_edit || order.outcome) }">
        <section class="tool-edit-panel" :aria-label="order.kind === 'external' ? '送检报告' : '自检记录'">
          <template v-if="order.kind === 'external'">
            <div class="tool-panel-heading"><h3>送检报告</h3></div>
            <div class="tool-report-layout">
              <label v-if="order.can_edit">实际检测日期 <input v-model="actual" type="date" :disabled="busy" /></label>
              <div v-else-if="order.actual"><span class="tool-muted">实际检测日期</span><strong>{{ order.actual }}</strong></div>
              <div class="tool-report-file">
                <FileText :size="19" />
                <div><strong>{{ order.report_name || '尚未上传送检报告' }}</strong><small v-if="order.can_edit && !order.report_name">支持 PDF、Word、Excel 或 JPG／PNG，最多 12 MB</small></div>
                <NButton v-if="order.can_edit" type="primary" secondary :disabled="busy" @click="reportInput?.click()"><Upload :size="16" />{{ order.report_name ? '更换报告' : '上传送检报告' }}</NButton>
                <input ref="reportInput" type="file" accept=".pdf,.docx,.xlsx,.jpg,.jpeg,.png" hidden @change="uploadReport" />
              </div>
            </div>
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
            <div class="tool-grid" aria-label="自检项目">
              <div class="tool-grid-head" aria-hidden="true"><b>自检项目</b><b>操作方法</b><b>合格标准</b><b>完成情况</b></div>
              <article v-for="(check,index) in active.template.schema.checks" :key="`${active.entry_id}-${check.row}`" class="tool-grid-row">
                <strong>{{ index + 1 }}. {{ check.name }}</strong>
                <p><span>操作方法</span>{{ check.method }}</p>
                <p><span>合格标准</span>{{ check.standard }}</p>
                <NRadioGroup v-if="order.can_edit" v-model:value="active.checks[index]" :name="`${active.entry_id}-${index}`" :disabled="busy" :aria-label="check.name + '完成情况'">
                  <NRadioButton value="是">是</NRadioButton><NRadioButton value="否">否</NRadioButton>
                </NRadioGroup>
                <span v-else>{{ active.checks[index] || '未填写' }}</span>
              </article>
            </div>
            <details v-if="order.can_edit || active.reviewer || active.reviewed_at || active.notes" class="tool-audit">
              <summary>审核与说明（选填）</summary>
              <fieldset class="tool-fields" :disabled="busy || !order.can_edit">
                <label>审核人 <input v-model="active.reviewer" maxlength="80" /></label>
                <label class="tool-time">审核确认时间 <input v-model="active.reviewed_at" type="datetime-local" /></label>
                <label class="tool-notes">自检说明 <textarea v-model="active.notes" rows="2" maxlength="1500" /></label>
              </fieldset>
            </details>
          </template>
        </section>
        <aside v-if="order.kind === 'self' && (order.can_edit || order.outcome)" class="tool-result" aria-label="处理结果">
          <div class="tool-panel-heading"><h3>处理结果</h3></div>
          <div v-if="order.can_edit" class="tool-result-fields">
            <label>工具状态 <NSelect v-model:value="outcome" aria-label="工具状态" :options="outcomeOptions" :disabled="busy" /></label>
            <label v-if="outcome !== 'normal'">处理原因 <textarea v-model="reason" rows="2" maxlength="1000" :disabled="busy" /></label>
            <label v-if="outcome === 'scrap'">报废数量 <input v-model.number="quantity" type="number" min="1" :max="order.quantity" :disabled="busy" /></label>
          </div>
          <div v-if="order.outcome && !order.can_edit" class="tool-context">
            <NTag :type="order.can_repair || order.can_approve ? 'warning' : 'default'">{{ outcomeLabels[order.outcome] || '已取消' }}</NTag>
            <p v-if="order.reason">{{ order.reason }}</p><p v-if="order.resolution">{{ order.resolution }}</p>
          </div>
          <div v-if="order.can_repair || order.can_approve" class="tool-result-fields">
            <label>处理说明 <textarea v-model="reason" rows="2" maxlength="1000" :disabled="busy" /></label>
            <label v-if="order.can_repair">转报废数量 <input v-model.number="quantity" type="number" min="1" :max="order.quantity" :disabled="busy" /></label>
            <NPopconfirm v-for="step in transitions" :key="step.action" @positive-click="advance(step.action)">
              <template #trigger><NButton :type="step.type" :disabled="busy">{{ step.label }}</NButton></template>
              确认{{ step.label }}？
            </NPopconfirm>
          </div>
        </aside>
      </div>
      <details v-if="order.history.length" class="tool-audit">
        <summary>操作记录 · {{ order.history.length }} 条</summary>
        <p v-for="(entry,index) in order.history" :key="index">{{ displayTime(entry.at) }} · {{ entry.name }} · {{ entry.action }}<span v-if="entry.detail"> · {{ entry.detail }}</span></p>
      </details>
      <footer v-if="order.can_edit" class="tool-bar tool-submit">
        <span v-if="order.kind === 'self'">{{ records.length }} 份记录 · {{ answered }} / {{ checkCount }} 项已填写</span>
        <span v-else>{{ order.report_name ? '送检报告已保存' : '上传报告后可提交' }}</span>
        <NButton v-if="order.kind === 'self'" :disabled="busy || !dirty" @click="save('draft')">保存草稿</NButton>
        <NPopconfirm @positive-click="save('submit')">
          <template #trigger><NButton type="primary" :disabled="busy || (order.kind === 'self' ? !records.length : !order.report_name || !actual)">提交工单</NButton></template>
          提交后锁定工单并纳入本月归档。
        </NPopconfirm>
      </footer>
    </div>
    <NModal :show="!!afterDiscard" preset="dialog" title="尚未保存" content="离开后，本次未保存的填写将丢失。" positive-text="放弃填写" negative-text="继续填写" :mask-closable="false" @positive-click="discard" @negative-click="afterDiscard = null" @update:show="show => { if (!show) afterDiscard = null; }" />
  </section>
</template>

<script setup lang="ts">
import { computed, defineAsyncComponent, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NEmpty, NInput, NModal, NPagination, NPopconfirm, NRadioButton, NRadioGroup, NSelect, NSpin, NTag } from 'naive-ui';
import { ArrowLeft, ClipboardList, Download, FileText, Plus, RefreshCw, Search, Settings2, Trash2, Upload, Wrench } from 'lucide-vue-next';
import { downloadFile, requestBinaryJson, requestJson } from '../api/client';
import { navigateBack, registerNavigationGuard } from '../navigation';
const ToolManagementSettings = defineAsyncComponent(() => import('./ToolManagementSettings.vue'));

defineProps<{ scope?: string }>();
const api = '/api/tool-self-check';
interface Template {
  id: string; version: number; name: string; active: boolean;
  schema: { specialty: string; cycle: string; applicable: string; precautions: string;
    checks: { row: number; name: string; method: string; standard: string }[] };
}
interface Entry {
  entry_id: string; template_id: string; version: number; checks: string[]; template: Template;
  location: string; inspector: string; checked_at: string; notes: string; reviewer: string; reviewed_at: string;
}
interface Order {
  id: string; month: string; scope: string; revision: number; status: 'pending' | 'submitted' | 'cancelled'; owner_name: string;
  submitted_at: string | null; can_claim: boolean; can_edit: boolean; can_release: boolean;
  kind: 'self' | 'external'; due: string; tool_name: string; model: string; location: string; quantity: number;
  supervisor_assigned: boolean; assigned_to_me: boolean; can_approve: boolean; can_repair: boolean;
  actual: string; outcome: string; reason: string; resolution: string; report_name: string;
  sync: { label: string; state: string; error: string }[];
}
interface Detail extends Order {
  records: Entry[]; actor_name: string; history: { action: string; at: string; name: string; detail: string }[];
  draft: { outcome?: string; reason?: string; quantity?: number };
}
interface Board {
  month: string; orders: Order[]; followups: Order[]; templates: Template[]; can_manage_templates: boolean;
  can_download_month: boolean; archived_at: string | null; actor_name: string;
  inventory_at: string; inventory_error: string;
}
const kindOptions = [{ value: 'all', label: '全部工单' }, { value: 'self', label: '自检工单' }, { value: 'external', label: '送检工单' }];
const outcomeOptions = [{ value: 'normal', label: '正常' }, { value: 'repair', label: '转检修' }, { value: 'scrap', label: '报废' }];
const outcomeLabels: Record<string, string> = { normal: '正常', repair: '检修中', repaired: '检修完成', scrap: '待报废审批',
  scrapped: '已批准报废', rejected: '报废未通过' };
const view = ref('orders');
const kindFilter = ref('all');
const stateFilter = ref('all');
const search = ref('');
const page = ref(1);
const pageSize = 20;
const root = ref<HTMLElement>();
const actual = ref('');
const outcome = ref('normal');
const reason = ref('');
const quantity = ref(1);
const reportInput = ref<HTMLInputElement>();
const month = ref('');
const board = ref<Board>();
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
const replacing = shallowRef<Template>();
const afterDiscard = shallowRef<(() => void) | null>(null);
let reader: AbortController | undefined;
let disposed = false;
let listPosition = 0;

const active = computed(() => records.value.find(record => record.entry_id === selected.value));
const pendingCount = computed(() => board.value?.orders.filter(row => row.status === 'pending').length || 0);
const submittedCount = computed(() => board.value?.orders.filter(row => row.status === 'submitted').length || 0);
const repairCount = computed(() => board.value?.followups.filter(row => row.outcome === 'repair').length || 0);
const tabs = computed(() => [
  { value: 'orders', label: '月度工单', icon: ClipboardList, count: pendingCount.value },
  { value: 'followups', label: '检修与报废', icon: Wrench, count: board.value?.followups.length || 0 },
  ...(board.value?.can_manage_templates ? [{ value: 'settings', label: '工具与模板', icon: Settings2, count: 0 }] : []),
]);
const stateOptions = computed(() => [{ value: 'all', label: '全部状态' }, ...(view.value === 'followups'
  ? [{ value: 'repair', label: '检修中' }, { value: 'scrap', label: '待报废审批' }]
  : [{ value: 'pending', label: '待填写' }, { value: 'submitted', label: '已提交' }, { value: 'cancelled', label: '已取消' }])]);
const visibleOrders = computed(() => {
  const followups = view.value === 'followups';
  const terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return (followups ? board.value?.followups || [] : board.value?.orders || []).filter(row => {
    if (!followups && kindFilter.value !== 'all' && row.kind !== kindFilter.value) return false;
    if (stateFilter.value !== 'all' && (followups ? row.outcome : row.status) !== stateFilter.value) return false;
    const searchable = [row.tool_name, row.location, row.model, row.scope + '楼', row.owner_name].join(' ').toLowerCase();
    return terms.every(term => searchable.includes(term));
  });
});
const pagedOrders = computed(() => visibleOrders.value.slice((page.value - 1) * pageSize, page.value * pageSize));
const emptyDescription = computed(() => {
  if (search.value.trim() || stateFilter.value !== 'all' || view.value === 'orders' && kindFilter.value !== 'all') return '没有符合筛选条件的工单';
  if (view.value === 'followups') return '暂无待处理的检修或报废审批';
  return board.value?.inventory_at ? '本月暂无工单' : '正在等待工具底表首次同步';
});
watch(view, () => { stateFilter.value = 'all'; });
watch([view, search, stateFilter, kindFilter, month], () => { page.value = 1; });
watch(() => visibleOrders.value.length, count => { page.value = Math.min(page.value, Math.max(1, Math.ceil(count / pageSize))); });
const transitions = computed<{ action: string; label: string; type: 'primary' | 'warning' }[]>(() => order.value?.can_repair
  ? [{ action: 'repaired', label: '检修完成', type: 'primary' }, { action: 'repair-scrap', label: '检修转报废', type: 'warning' }]
  : [{ action: 'approve', label: '批准报废', type: 'primary' }, { action: 'reject', label: '驳回报废', type: 'warning' }]);
const templateOptions = computed(() => (board.value?.templates || []).filter(template => template.active)
  .map(template => ({ value: template.id, label: template.name })));
const payload = computed(() => records.value.map(({ template: _template, ...record }) => record));
const form = computed(() => order.value?.kind === 'external' ? { actual: actual.value } :
  { records: payload.value, outcome: outcome.value, reason: reason.value, quantity: quantity.value });
const dirty = computed(() => !!order.value?.can_edit && JSON.stringify(form.value) !== saved.value);
const checkCount = computed(() => records.value.reduce((sum, record) => sum + record.checks.length, 0));
const answered = computed(() => records.value.reduce((sum, record) => sum + record.checks.filter(Boolean).length, 0));
const orderPath = (row: Order) => `/orders/${row.id}`;
const status = (row: Order) => row.status === 'submitted' ? '已提交' : row.status === 'cancelled' ? '已取消' : '待填写';
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
function changeMonth(event: Event) {
  const input = event.target as HTMLInputElement;
  const target = input.value;
  input.value = month.value;
  if (target && target !== month.value) { message.value = ''; void loadBoard(target); }
}
function applyOrder(data: Detail) {
  order.value = data;
  records.value = data.records.map(record => ({ ...record, checks: [...record.checks] }));
  actual.value = data.actual;
  outcome.value = data.draft?.outcome || 'normal';
  reason.value = data.draft?.reason || '';
  quantity.value = data.draft?.quantity || 1;
  saved.value = JSON.stringify(form.value);
  if (!records.value.some(record => record.entry_id === selected.value)) selected.value = records.value[0]?.entry_id || '';
}
async function action(row: Order, kind: string, data: object = {}) {
  return requestJson(api + orderPath(row) + '/' + kind, { method: 'POST', body: JSON.stringify({ revision: row.revision, ...data }) });
}
async function openOrder(row: Order, claim = false) {
  if (busy.value) return;
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
      applyOrder(data);
      if (opening) void scrollAfterRender(0);
    }
  } catch (value) { fail(value); }
  finally { busy.value = false; }
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
  else void loadBoard();
}
function addRecord() {
  const template = board.value?.templates.find(item => item.id === templateId.value && item.active);
  if (!template || !order.value?.can_edit || records.value.length >= 20) return;
  const id = Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('');
  records.value.push({ entry_id: id, template_id: template.id, version: template.version, template,
    checks: template.schema.checks.map(() => ''), location: order.value.location, inspector: order.value.actor_name,
    checked_at: '', notes: '', reviewer: '', reviewed_at: '' });
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
    if (!disposed) order.value = { ...row, ...receipt };
    message.value = '送检报告已保存。';
  });
}
async function advance(step: string) {
  if (!order.value) return;
  const row = order.value;
  await mutate(async () => {
    const receipt = await action(row, step, { reason: reason.value, quantity: quantity.value }) as Order;
    if (!disposed) order.value = { ...row, ...receipt };
    message.value = '处理结果已保存。';
    try { applyOrder(await requestJson(api + orderPath(row), { cache: 'no-store' }) as Detail); }
    catch { /* Preserve the confirmed transition. */ }
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
    await loadBoard();
    message.value = '模板已保存。';
  });
}
async function toggleTemplate(template: Template) {
  await mutate(async () => {
    await requestJson(`${api}/templates/${template.id}/availability`, { method: 'POST',
      body: JSON.stringify({ version: template.version, active: !template.active }) });
    await loadBoard();
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
  if (busy.value) return false;
  if (!dirty.value) return true;
  afterDiscard.value = proceed;
  return false;
});
function beforeUnload(event: BeforeUnloadEvent) { if (dirty.value || busy.value) { event.preventDefault(); event.returnValue = ''; } }
onMounted(() => { window.addEventListener('beforeunload', beforeUnload); void loadBoard(); });
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
  display:grid; gap:12px; container-type:inline-size; box-sizing:border-box;
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
.tool-archive { font-size:13px; }
.tool-filters {
  display:grid; grid-template-columns:minmax(200px,1fr) repeat(3,minmax(0,132px)); gap:8px; align-items:stretch;
  border-top:1px solid var(--tool-line);
}
.tool-filters:not(:has(.tool-month)) { grid-template-columns:minmax(200px,1fr) minmax(0,155px); }
.tool-month,input,.tool-fields textarea,.tool-result textarea {
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
  .tool-filters { grid-template-columns:minmax(0,1fr) minmax(0,140px); }
  .tool-filters:not(:has(.tool-month)) { grid-template-columns:minmax(0,1fr) minmax(0,124px); }
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
  .tool-report-file .n-button { grid-column:2; justify-self:start; }
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
