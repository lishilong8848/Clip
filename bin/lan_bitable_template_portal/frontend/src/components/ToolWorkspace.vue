<template>
  <section class="tool-workspace" aria-label="工具工作台" data-v2-inline-feedback>
    <NAlert v-if="error" type="error" :bordered="false" role="alert">{{ error }}</NAlert>
    <NAlert v-if="board?.warning" type="warning" :bordered="false" role="status">{{ board.warning }}</NAlert>
    <div v-if="!session" class="workspace-grid" :class="{ 'is-external': kind === 'external' }">
      <section class="workspace-panel" aria-labelledby="tool-assets-title">
        <header class="panel-heading">
          <h3 id="tool-assets-title" tabindex="-1">工具台账</h3><small>已选 {{ selectedIds.length }} 台</small>
        </header>
        <NRadioGroup :value="kind" size="small" aria-label="办理类型" :disabled="!!busy" @update:value="chooseKind">
          <NRadioButton value="self">维护</NRadioButton><NRadioButton value="external">送检</NRadioButton>
        </NRadioGroup>
        <div class="asset-filters">
          <NSelect v-model:value="scope" :options="scopeOptions" aria-label="筛选楼栋" size="small" />
          <NInput v-model:value="search" size="small" clearable placeholder="搜索工具、型号、品牌" :input-props="{ 'aria-label': '搜索工具、型号或品牌' }" />
        </div>
        <NSpin :show="loading && !board">
          <div v-if="visibleAssets.length" class="asset-list">
            <table class="assets">
              <thead><tr>
                <th scope="col"><NCheckbox :checked="allVisibleSelected" :indeterminate="someVisibleSelected && !allVisibleSelected"
                  :disabled="!!busy || !visibleEligible.length" aria-label="选择筛选后的工具" @update:checked="toggleVisible" /></th>
                <th scope="col">工具 / 楼栋</th><th scope="col">型号 / 品牌</th><th scope="col">周期</th>
              </tr></thead>
              <tbody>
                <tr v-for="asset in visibleAssets" :key="asset.id" :class="{ 'is-selected': selected.has(asset.id) }">
                  <td><NCheckbox :checked="selected.has(asset.id)" :disabled="!!busy || !eligible(asset)"
                    :aria-label="'选择工具 ' + asset.name" @update:checked="toggleAsset(asset.id, $event)" /></td>
                  <td><strong>{{ asset.name }}</strong><small>{{ scopeLabel(asset.scope) }}</small></td>
                  <td>{{ asset.model || '—' }}<small>{{ asset.brand || '—' }}</small></td>
                  <td>{{ cycleText(asset) }}<small v-if="reason(asset)" class="asset-reason">{{ reason(asset) }}</small></td>
                </tr>
              </tbody>
            </table>
          </div>
          <NEmpty v-else-if="!loading" description="没有匹配的工具" />
        </NSpin>
      </section>

      <section v-if="kind === 'self'" class="workspace-panel" aria-labelledby="tool-templates-title">
        <header class="panel-heading"><h3 id="tool-templates-title">飞书自检模板</h3></header>
        <p v-if="mixedTemplates" class="workspace-muted">所选工具的已关联模板不同，可分别沿用或选用同一模板。</p>
        <p v-else-if="selectedIds.length && !allAssociated" class="workspace-muted">部分工具还没有可用的模板关联，请选择统一模板。</p>
        <NRadioGroup v-if="templates.length" :value="templateChoice" :disabled="!!busy" aria-label="选择自检模板" @update:value="chooseTemplate">
          <div class="template-list">
            <NRadio v-if="allAssociated" :value="existingTemplate">使用各工具已关联模板</NRadio>
            <table class="templates" aria-label="自检模板记录">
              <thead><tr><th scope="col">选择</th><th scope="col">模板名称</th></tr></thead>
              <tbody><tr v-for="template in templates" :key="template.id" :class="{ 'is-selected': templateChoice === template.id }">
                <td><NRadio :value="template.id" :aria-label="'选用模板 ' + (template.label || template.name)" /></td>
                <td><label @click="!busy && chooseTemplate(template.id)">{{ template.label || template.name }}</label></td>
              </tr></tbody>
            </table>
          </div>
        </NRadioGroup>
        <NEmpty v-else description="暂无可用的飞书模板" />
      </section>

      <aside class="workspace-panel opening-panel" aria-label="办理操作">
        <NButton type="primary" :disabled="!!busy || !canOpen" :loading="busy === 'open'" @click="requestOpen">{{ kind === 'self' ? '打开填写' : '打开送检' }}</NButton>
        <label v-if="sessionOptions.length">继续已有记录
          <NSelect :value="null" :options="sessionOptions" size="small" placeholder="选择记录" aria-label="继续已有记录" :disabled="!!busy" @update:value="openSaved" />
        </label>
      </aside>
    </div>
    <section v-else class="workspace-panel editor-panel" aria-labelledby="tool-editor-title">
        <header class="panel-heading">
          <div class="workspace-actions">
            <NButton size="small" :disabled="!!busy" @click="returnToSelection">返回选择</NButton>
            <h3 id="tool-editor-title" tabindex="-1">{{ session.kind === 'self' ? '自检记录' : '送检报告' }}</h3>
          </div>
          <NTag size="small" :type="session.status === 'archived' ? 'success' : session.status === 'pending' ? 'warning' : 'default'" role="status">{{ sessionStatus(session) }}</NTag>
        </header>
          <div v-if="session.kind === 'self'" class="workspace-editor">
            <div class="tool-toolbar">
              <div class="item-tabs tool-tabs" aria-label="本批工具" tabindex="0">
                <NButton v-for="entry in session.items" :key="entry.id" size="small" :aria-pressed="entry.id === itemId"
                  :disabled="!!busy" :type="entry.id === itemId ? 'primary' : 'default'" @click="selectItem(entry.id)">
                  {{ entry.asset.name }} · {{ scopeLabel(entry.asset.scope) }}
                </NButton>
              </div>
              <NButton v-if="editable && session.items.length > 1" size="small" :disabled="!!busy" @click="openBatchReview">批量填写审核信息</NButton>
            </div>
            <template v-if="item">
              <div class="review-line">
                <label>审核人 <NInput v-model:value="item.reviewer" size="small" :disabled="!editable || !!busy" :input-props="{ 'aria-label': '审核人' }" /></label>
                <label>审核时间 <input v-model="item.reviewed_at" aria-label="审核时间" type="datetime-local" :disabled="!editable || !!busy" /></label>
                <NButton size="small" :disabled="!editable || !!busy" @click="openSignature(false)">{{ item.signature ? '重签' : '手绘签名' }}</NButton>
                <img v-if="isPng(item.signature)" class="signature-preview" :src="item.signature" alt="审核签名" />
              </div>
              <div v-if="sheets.length > 1" class="item-tabs" aria-label="模板工作表">
                <NButton v-for="sheet in sheets" :key="sheet.name" size="small" :aria-pressed="sheet.name === activeSheet?.name"
                  :type="sheet.name === activeSheet?.name ? 'primary' : 'default'" @click="sheetName = sheet.name">{{ sheet.name }}</NButton>
              </div>
              <div v-if="activeSheet" class="item-tabs sheet-tools" aria-label="原表缩放">
                <NButton size="tiny" aria-label="缩小原表" :disabled="zoom <= 60" @click="zoom -= 10">−</NButton>
                <NButton size="tiny" aria-label="恢复原表比例" @click="zoom = 100">{{ zoom }}%</NButton>
                <NButton size="tiny" aria-label="放大原表" :disabled="zoom >= 150" @click="zoom += 10">+</NButton>
              </div>
              <div v-if="activeSheet" :key="item.id + '!' + activeSheet.name" class="sheet-scroll" tabindex="0" :aria-label="activeSheet.name + '原表'">
                <div class="sheet-frame" :style="{ width: sheetWidth + 'px', zoom: zoom / 100 }">
                  <table class="sheet-table">
                    <colgroup><col v-for="(width, index) in activeSheet.columns" :key="index" :style="{ width: width + 'px' }" /></colgroup>
                    <tbody>
                      <tr v-for="row in activeSheet.rows" :key="row.index" :style="{ height: row.height + 'px' }">
                        <td v-for="cell in row.cells" :key="cell.address" :rowspan="cell.rowspan" :colspan="cell.colspan"
                          :style="cell.style" :class="{ 'fillable-cell': cell.editable || cell.check }">
                          <select v-if="cell.check" :value="item.checks[cellKey(cell)] || ''" :disabled="!editable || !!busy" :aria-label="'检查 ' + cellKey(cell)" @change="setCheck(cell, $event)">
                            <option value="">请选择</option><option value="是">是</option><option value="否">否</option>
                          </select>
                          <input v-else-if="cell.editable" :value="item.values[cellKey(cell)] ?? cell.value ?? ''"
                            :disabled="!editable || !!busy" :aria-label="'填写 ' + cellKey(cell)" @input="setCell(cell, $event)" />
                          <template v-else-if="reviewCell(cell)">
                            <span>{{ reviewText(cell) }}</span>
                            <img v-if="isSignatureCell(cell) && isPng(item.signature)" class="signature-in-cell" :src="item.signature" alt="表内审核签名" />
                          </template>
                          <span v-else>{{ cell.value }}</span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                  <img v-for="image in sheetImages" :key="image.key" :src="image.src" :style="image.style" alt="" />
                </div>
              </div>
              <NAlert v-else type="warning" :bordered="false">未读取到该工具的原表，请刷新记录核对。</NAlert>
              <div class="review-line">
                <span id="tool-qualified-label">检查结果合格</span>
                <NRadioGroup class="qualified-options" :value="item.qualified === null ? null : item.qualified ? 'yes' : 'no'" size="small"
                  aria-labelledby="tool-qualified-label" :disabled="!editable || !!busy" @update:value="item.qualified = $event === 'yes'">
                  <NRadioButton value="yes">是</NRadioButton><NRadioButton value="no">否</NRadioButton>
                </NRadioGroup>
              </div>
            </template>
          </div>
          <div v-else class="workspace-editor">
            <p class="workspace-muted">{{ session.items.length }} 台工具共用一份送检报告。</p>
            <ul class="archive-list"><li v-for="entry in session.items" :key="entry.id">{{ entry.asset.name }} · {{ scopeLabel(entry.asset.scope) }} · {{ entry.asset.model || '—' }}</li></ul>
            <p v-if="session.report_name" role="status">已保存报告 · {{ session.report_name }}</p>
            <div v-if="editable" class="workspace-actions">
              <input ref="reportInput" type="file" accept=".pdf,.docx,.xlsx,.jpg,.jpeg,.png" hidden @change="pickReport" />
              <NButton size="small" :disabled="!!busy" @click="reportInput?.click()">{{ session.report_name ? '更换报告' : '选择报告' }}</NButton>
              <span v-if="report">{{ report.name }}</span>
              <NButton v-if="report" size="small" :loading="busy === 'report'" :disabled="!!busy" @click="uploadReport">上传报告</NButton>
              <NButton v-if="report" size="small" :disabled="!!busy" @click="report = undefined">取消选择报告</NButton>
            </div>
          </div>
          <ul v-if="session.status !== 'draft'" class="archive-list">
            <li v-for="entry in session.items" :key="entry.id">{{ entry.asset.name }} · {{ archiveStatus(entry.archive_status) }}<span v-if="entry.archive_error"> · {{ entry.archive_error }}</span></li>
          </ul>
          <footer class="workspace-actions editor-footer">
            <span v-if="editable" class="workspace-muted" role="status">{{ dirty ? '有未保存修改' : '草稿已保存' }}</span>
            <NButton v-if="editable && session.kind === 'self'" size="small" :disabled="!!busy || !dirty" :loading="busy === 'save'" @click="saveDraft">保存草稿</NButton>
            <NPopconfirm v-if="editable" positive-text="确认提交" negative-text="取消" @positive-click="submit">
              <template #trigger><NButton size="small" type="primary" :loading="busy === 'submit'"
                :disabled="!!busy || !!report || (session.kind === 'external' && !session.report_name)">提交归档</NButton></template>
              将提交本批 {{ session.items.length }} 台工具归档。
            </NPopconfirm>
            <NButton v-if="session.status === 'archived'" tag="a" size="small" :href="sessionUrl + '/download'">下载归档</NButton>
          </footer>
    </section>
    <NModal :show="!!confirmAction" preset="card" :title="confirmAction?.title" class="tool-workspace-dialog" @update:show="closeConfirm">
      <p>{{ confirmAction?.text }}</p>
      <template #footer><div class="workspace-actions dialog-actions">
        <NButton @click="closeConfirm(false)">取消</NButton><NButton type="primary" @click="runConfirm">确认</NButton>
      </div></template>
    </NModal>
    <NModal v-model:show="batchOpen" preset="card" title="批量填写审核信息" class="tool-workspace-dialog">
      <div class="review-line">
        <label>批量审核人 <NInput v-model:value="batch.reviewer" size="small" :input-props="{ 'aria-label': '批量审核人' }" /></label>
        <label>批量审核时间 <input v-model="batch.reviewed_at" aria-label="批量审核时间" type="datetime-local" /></label>
        <NButton size="small" @click="openSignature(true)">{{ batch.signature ? '重签批量签名' : '绘制批量签名' }}</NButton>
        <img v-if="isPng(batch.signature)" class="signature-preview" :src="batch.signature" alt="批量审核签名" />
      </div>
      <p class="workspace-muted">将覆盖本批所有工具的审核人、审核时间和签名。应用后可逐台调整。</p>
      <template #footer><div class="workspace-actions dialog-actions"><NButton @click="batchOpen = false">取消</NButton>
        <NButton type="primary" :disabled="!batch.reviewer.trim() || !batch.reviewed_at || !batch.signature" @click="applyBatchReview">覆盖并应用全批</NButton>
      </div></template>
    </NModal>
    <NModal v-model:show="signatureOpen" preset="card" title="手绘审核签名" class="tool-workspace-dialog" @after-enter="resetCanvas">
      <canvas ref="pad" width="480" height="170" aria-label="手绘审核签名画板"
        @pointerdown="startStroke" @pointermove="moveStroke" @pointerup="stopStroke" @pointercancel="stopStroke" @lostpointercapture="stopStroke" />
      <template #footer><div class="workspace-actions dialog-actions"><NButton @click="resetCanvas">清空</NButton>
        <NButton @click="signatureOpen = false">取消</NButton><NButton type="primary" :disabled="!signed" @click="applySignature">使用签名</NButton>
      </div></template>
    </NModal>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, shallowRef, watch } from 'vue';
import { NAlert, NButton, NCheckbox, NEmpty, NInput, NModal, NPopconfirm, NRadio, NRadioButton, NRadioGroup, NSelect, NSpin, NTag } from 'naive-ui';
import { requestBinaryJson, requestJson } from '../api/client';

type Kind = 'self' | 'external';
type Asset = { id: string; name: string; scope: string; model?: string; brand?: string; location?: string;
  self_cycle?: string | number; external_cycle?: string | number; template_id?: string; eligible_kinds?: Kind[];
  disabled_reason?: string };
type Template = { id: string; name?: string; label?: string };
type Cell = { address: string; value: string | number | boolean | null; rowspan: number; colspan: number;
  style?: Record<string, string | number>; editable?: boolean; check?: boolean };
type Sheet = { name: string; columns: number[]; rows: { index: number; height: number; cells: Cell[] }[];
  review?: { reviewer_address: string; reviewed_at_address: string; signature_address: string };
  images?: { address: string; src: string; offsetX: number; offsetY: number; width: number; height: number }[] };
type Review = { reviewer: string; reviewed_at: string; signature: string };
type Item = Review & { id: string; asset_id: string; asset: Asset; template?: { sheets: Sheet[]; schema?: { sheet: string; review_row: number; result_row: number } } | null;
  values: Record<string, string>; checks: Record<string, string>; qualified: boolean | null; archive_status?: string; archive_error?: string };
type Summary = { id: string; kind: Kind; status: string; items?: Item[]; created_at?: string; item_count?: number; archived_count?: number };
type Session = Summary & { revision: number; can_edit: boolean; items: Item[]; report_name?: string };
type Board = { assets: Asset[]; templates: Template[]; sessions: Summary[]; warning?: string };

const api = '/api/tool-self-check/workspace';
const existingTemplate = '__associated__';
const board = shallowRef<Board>();
const session = ref<Session>();
const kind = ref<Kind>('self');
const scope = ref('');
const search = ref('');
const selectedIds = ref<string[]>([]);
const selected = computed(() => new Set(selectedIds.value));
const templateChoice = ref<string | null>(null);
const itemId = ref('');
const sheetName = ref('');
const zoom = ref(100);
const error = ref('');
const busy = ref('');
const loading = ref(false);
const saved = ref('');
const report = shallowRef<File>();
const reportInput = ref<HTMLInputElement>();
const confirmAction = shallowRef<{ title: string; text: string; run: () => void }>();
const batchOpen = ref(false);
const batch = reactive<Review>({ reviewer: '', reviewed_at: '', signature: '' });
const signatureOpen = ref(false);
const signatureForBatch = ref(false);
const pad = ref<HTMLCanvasElement>();
const signed = ref(false);
const lifetime = new AbortController();
let pollTimer: ReturnType<typeof setTimeout> | undefined;
let boardRead: AbortController | undefined;
let openOperation: { key: string; id: string } | undefined;
let strokeId: number | undefined;

const assets = computed(() => board.value?.assets || []);
const templates = computed(() => board.value?.templates || []);
const scopeOptions = computed(() => [{ value: '', label: '全部楼栋' },
  ...[...new Set(assets.value.map(asset => asset.scope).filter(Boolean))].sort().map(value => ({ value, label: scopeLabel(value) }))]);
const visibleAssets = computed(() => {
  const terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return assets.value.filter(asset => (!scope.value || asset.scope === scope.value)
    && (!onlyCycle(asset) || onlyCycle(asset) === kind.value)
    && terms.every(term => [asset.name, asset.model, asset.brand, asset.location, scopeLabel(asset.scope)].join(' ').toLowerCase().includes(term)));
});
const visibleEligible = computed(() => visibleAssets.value.filter(eligible));
const allVisibleSelected = computed(() => !!visibleEligible.value.length && visibleEligible.value.every(asset => selected.value.has(asset.id)));
const someVisibleSelected = computed(() => visibleEligible.value.some(asset => selected.value.has(asset.id)));
const selectedAssets = computed(() => assets.value.filter(asset => selected.value.has(asset.id)));
const associations = computed(() => [...new Set(selectedAssets.value.map(asset => asset.template_id || ''))]);
const allAssociated = computed(() => selectedAssets.value.length > 0 && associations.value.every(id => templates.value.some(template => template.id === id)));
const mixedTemplates = computed(() => allAssociated.value && associations.value.length > 1);
const canOpen = computed(() => selectedIds.value.length > 0 && (kind.value === 'external' ||
  (templateChoice.value === existingTemplate ? allAssociated.value : templates.value.some(template => template.id === templateChoice.value))));
const sessionOptions = computed(() => (board.value?.sessions || []).filter(entry => entry.kind === kind.value)
  .map(entry => ({ value: entry.id, label: `${sessionStatus(entry)} · ${entry.item_count ?? entry.items?.length ?? 0} 台 · ${entry.created_at?.slice(0, 16).replace('T', ' ') || entry.id.slice(0, 8)}` })));
const sessionUrl = computed(() => api + '/' + encodeURIComponent(session.value?.id || ''));
const item = computed(() => session.value?.items.find(entry => entry.id === itemId.value));
const sheets = computed(() => item.value?.template?.sheets || []);
const activeSheet = computed(() => {
  return sheets.value.find(sheet => sheet.name === sheetName.value) || sheets.value.find(sheet => sheet.rows.some(row => row.cells.some(cell => cell.check || cell.editable))) || sheets.value[0];
});
const sheetWidth = computed(() => activeSheet.value?.columns.reduce((sum, width) => sum + width, 0) || 0);
const sheetImages = computed(() => {
  const sheet = activeSheet.value;
  return (sheet?.images || []).flatMap((image, index) => {
    const anchor = /^([A-Z]+)(\d+)$/.exec(image.address);
    if (!sheet || !anchor || !/^data:image\/(?:png|jpeg|webp|gif);base64,/.test(image.src)) return [];
    const col = [...anchor[1]].reduce((number, letter) => number * 26 + letter.charCodeAt(0) - 64, 0);
    const left = sheet.columns.slice(0, col - 1).reduce((sum, width) => sum + width, image.offsetX);
    const top = sheet.rows.filter(row => row.index < Number(anchor[2])).reduce((sum, row) => sum + row.height, image.offsetY);
    return [{ key: image.address + index, src: image.src, style: { left: left + 'px', top: top + 'px', width: image.width + 'px', height: image.height + 'px' } }];
  });
});
const editable = computed(() => session.value?.status === 'draft' && session.value.can_edit);
const dirty = computed(() => !!report.value || !!editable.value && JSON.stringify(fields()) !== saved.value);
watch([allAssociated, templates], () => {
  if (!templates.value.some(template => template.id === templateChoice.value)) {
    templateChoice.value = allAssociated.value ? existingTemplate : null;
  }
});

function scopeLabel(value: string) {
  if (!value) return '—';
  if (value === '110') return '110站';
  return /^[A-Z]$/.test(value) ? value + '楼' : value;
}
function onlyCycle(asset: Asset): Kind | undefined {
  if (!!asset.self_cycle === !!asset.external_cycle) return;
  return asset.self_cycle ? 'self' : 'external';
}
function eligible(asset: Asset) {
  return onlyCycle(asset) === kind.value && (asset.eligible_kinds?.includes(kind.value) ?? true);
}
function reason(asset: Asset) {
  if (asset.disabled_reason) return asset.disabled_reason;
  if (asset.self_cycle && asset.external_cycle) return '同时设置了两种周期';
  if (!asset.self_cycle && !asset.external_cycle) return '未设置周期';
  return eligible(asset) ? '' : '当前账号不可办理';
}
function cycleText(asset: Asset) {
  return (kind.value === 'self' ? asset.self_cycle : asset.external_cycle) || '—';
}
function sessionStatus(entry: Summary) {
  return ({ opening: '继续打开', draft: '草稿', pending: '归档处理中', archived: '已归档' } as Record<string, string>)[entry.status] || entry.status;
}
function archiveStatus(status?: string) {
  return ({ archived: '已归档', verifying: '正在核对归档结果', retrying: '归档失败，待重试', pending: '待归档' } as Record<string, string>)[status || ''] || '待归档';
}
function chooseKind(value: Kind) {
  if (value === kind.value) return;
  leave(() => {
    kind.value = value;
    selectedIds.value = [];
    templateChoice.value = null;
    clearSession();
  });
}
function chooseTemplate(value: string) {
  templateChoice.value = value;
}
function toggleAsset(id: string, checked: boolean) {
  if (checked && !selected.value.has(id) && selectedIds.value.length >= 200) {
    error.value = '每批最多选择 200 台工具，请分批办理。';
    return;
  }
  selectedIds.value = checked ? [...selectedIds.value, id] : selectedIds.value.filter(value => value !== id);
}
function toggleVisible(checked: boolean) {
  const ids = new Set(visibleEligible.value.map(asset => asset.id));
  const selection = checked ? [...new Set([...selectedIds.value, ...ids])] : selectedIds.value.filter(id => !ids.has(id));
  if (selection.length > 200) {
    error.value = '筛选结果超过每批 200 台上限，请缩小筛选范围。';
    return;
  }
  selectedIds.value = selection;
}
function cellKey(cell: Cell) {
  return activeSheet.value?.name + '!' + cell.address;
}
function reviewCell(cell: Cell) {
  const schema = item.value?.template?.schema;
  const sheet = activeSheet.value;
  if (!sheet || schema?.sheet !== sheet.name) return '';
  if (cell.address === (sheet.review?.reviewer_address || 'E' + schema.review_row)) return 'reviewer';
  if (cell.address === (sheet.review?.reviewed_at_address || 'G' + schema.review_row)) return 'time';
  if (cell.address === 'E' + schema.result_row) return 'result';
  if (isSignatureCell(cell)) return 'signature';
  return '';
}
function isSignatureCell(cell: Cell) {
  const schema = item.value?.template?.schema;
  const sheet = activeSheet.value;
  return !!sheet && schema?.sheet === sheet.name && cell.address === (sheet.review?.signature_address || 'E' + schema.review_row);
}
function reviewText(cell: Cell) {
  const current = item.value;
  const sheet = activeSheet.value;
  if (!current || !sheet) return cell.value;
  switch (reviewCell(cell)) {
    case 'reviewer': return current.reviewer || cell.value;
    case 'time': return current.reviewed_at?.replace('T', ' ') || cell.value;
    case 'result': {
      const checks = sheet.rows.flatMap(row => row.cells.filter(entry => entry.check)).map(entry => current.checks[cellKey(entry)]);
      if (!checks.length || checks.some(value => value !== '是' && value !== '否')) return cell.value;
      return checks.every(value => value === '是') ? '已完成[√]　未完成[ ]' : '已完成[ ]　未完成[√]';
    }
    default: return cell.value;
  }
}
function setCell(cell: Cell, event: Event) {
  if (item.value && editable.value) item.value.values[cellKey(cell)] = (event.target as HTMLInputElement).value;
}
function setCheck(cell: Cell, event: Event) {
  if (!item.value || !editable.value) return;
  const value = (event.target as HTMLSelectElement).value;
  if (value) item.value.checks[cellKey(cell)] = value;
  else delete item.value.checks[cellKey(cell)];
}
function selectItem(id: string) {
  itemId.value = id;
  sheetName.value = '';
}
function fields() {
  return (session.value?.items || []).map(({ id, asset_id, values, checks, reviewer, reviewed_at, signature, qualified }) =>
    ({ id, asset_id, values, checks, reviewer, reviewed_at, signature, qualified }));
}
function clearSession() {
  clearTimeout(pollTimer);
  session.value = undefined;
  report.value = undefined;
  itemId.value = '';
  sheetName.value = '';
}
function returnToSelection() {
  leave(() => {
    clearSession();
    error.value = '';
    void nextTick(() => {
      const heading = document.getElementById('tool-assets-title');
      heading?.focus({ preventScroll: true });
      heading?.scrollIntoView({ block: 'nearest' });
    });
  });
}
function applySession(value: Session) {
  const opened = session.value?.id !== value.id;
  if (opened) clearSession();
  session.value = value;
  if (!value.items.some(entry => entry.id === itemId.value)) selectItem(value.items[0]?.id || '');
  saved.value = JSON.stringify(fields());
  if (board.value) board.value = { ...board.value, sessions: [value, ...board.value.sessions.filter(entry => entry.id !== value.id)] };
  if (opened) void nextTick(() => {
    const heading = document.getElementById('tool-editor-title');
    heading?.focus({ preventScroll: true });
    heading?.scrollIntoView({ block: 'nearest' });
  });
  schedulePoll();
}
function message(cause: unknown) {
  return cause instanceof Error ? cause.message : '操作尚未确认，请刷新核对。';
}
function read(path: string, signal = lifetime.signal) {
  return requestJson(path, { signal, cache: 'no-store', fresh: true });
}
function post(path: string, payload: object, timeoutMs?: number) {
  return requestJson(path, { method: 'POST', body: JSON.stringify(payload), signal: lifetime.signal, timeoutMs });
}
async function readBoard(refresh = false) {
  boardRead?.abort();
  const controller = new AbortController();
  boardRead = controller;
  loading.value = true;
  try {
    const data = await (refresh
      ? requestJson(api + '/refresh', { method: 'POST', signal: controller.signal })
      : read(api, controller.signal)) as Board;
    if (!controller.signal.aborted) {
      board.value = data;
      const ids = new Set(data.assets.filter(eligible).map(asset => asset.id));
      selectedIds.value = selectedIds.value.filter(id => ids.has(id));
    }
  } catch (cause) {
    if (!controller.signal.aborted) error.value = message(cause);
  } finally {
    if (boardRead === controller) loading.value = false;
  }
}
async function perform(action: string, operation: () => Promise<void>) {
  if (busy.value || lifetime.signal.aborted) return;
  busy.value = action;
  error.value = '';
  try { await operation(); }
  catch (cause) { if (!lifetime.signal.aborted) error.value = message(cause); }
  finally { busy.value = ''; schedulePoll(); }
}
function requestOpen() {
  if (!canOpen.value) return;
  leave(() => {
    const choice = kind.value === 'self' && templateChoice.value !== existingTemplate ? templateChoice.value : null;
    const changed = choice ? selectedAssets.value.filter(asset => asset.template_id && asset.template_id !== choice) : [];
    const run = () => void openSelection(choice);
    if (changed.length) ask('更换所选工具的模板', `将覆盖以下工具的原模板关联：${changed.map(asset => asset.name + '（' + scopeLabel(asset.scope) + '）').join('、')}。`, run);
    else run();
  });
}
async function openSelection(template_id: string | null) {
  await perform('open', async () => {
    const payload = { asset_ids: [...selectedIds.value].sort(), template_id, kind: kind.value };
    const key = JSON.stringify(payload);
    if (openOperation?.key !== key) openOperation = { key, id: Array.from(crypto.getRandomValues(new Uint8Array(16)), part => part.toString(16).padStart(2, '0')).join('') };
    let result: Session;
    try {
      result = await post(api + '/open', { ...payload, operation_id: openOperation.id }, 120000) as Session;
    } catch (cause) {
      await readBoard();
      throw cause;
    }
    openOperation = undefined;
    report.value = undefined;
    applySession(result);
    if (template_id && board.value) board.value = { ...board.value, assets: board.value.assets.map(asset =>
      payload.asset_ids.includes(asset.id) ? { ...asset, template_id } : asset) };
  });
}
function openSaved(id: string) {
  if (!id || id === session.value?.id) return;
  leave(() => void perform('read', async () => {
    const path = api + '/' + encodeURIComponent(id);
    let data = await read(path) as Session;
    if (data.status === 'opening') {
      try {
        data = await requestJson(path + '/resume', { method: 'POST', signal: lifetime.signal, timeoutMs: 120000 }) as Session;
      } catch (cause) {
        await readBoard();
        throw cause;
      }
    }
    clearSession();
    applySession(data);
  }));
}
function refresh() {
  leave(() => void perform('read', async () => {
    await readBoard(true);
    if (session.value) {
      const data = await read(sessionUrl.value) as Session;
      report.value = undefined;
      applySession(data);
    }
  }));
}
async function save() {
  if (!session.value || !editable.value || !dirty.value) return;
  applySession(await post(sessionUrl.value + '/save', { revision: session.value.revision, items: fields() }) as Session);
}
function saveDraft() {
  return perform('save', save);
}
function pickReport(event: Event) {
  const input = event.target as HTMLInputElement;
  report.value = input.files?.[0] || report.value;
  input.value = '';
}
function uploadReport() {
  const file = report.value;
  const current = session.value;
  if (!file || !current || !editable.value) return;
  return perform('report', async () => {
    const query = new URLSearchParams({ name: file.name, revision: String(current.revision) });
    const data = await requestBinaryJson(sessionUrl.value + '/report?' + query, file, {
      headers: { 'Content-Type': file.type || 'application/octet-stream' }, signal: lifetime.signal,
    }) as Session;
    report.value = undefined;
    applySession(data);
  });
}
function submit() {
  if (!editable.value || report.value) return;
  return perform('submit', async () => {
    if (session.value?.kind === 'self') {
      for (const entry of session.value.items) {
        const missing = !entry.reviewer.trim() || !entry.reviewed_at || !isPng(entry.signature) || entry.qualified === null;
        const incomplete = !entry.template?.sheets.length || entry.template.sheets.some(sheet => sheet.rows.some(row =>
          row.cells.some(cell => cell.check && !['是', '否'].includes(entry.checks[sheet.name + '!' + cell.address] || ''))));
        const inconsistent = entry.qualified === true && Object.values(entry.checks).includes('否');
        if (missing || incomplete || inconsistent) {
          selectItem(entry.id);
          throw new Error(`「${entry.asset.name}」${missing ? '请补齐审核人、时间、签名及检测结果。' : incomplete ? '请完成原表中的所有检查项。' : '检查项存在「否」，检测结果不能选「合格」。'}`);
        }
      }
    }
    await save();
    if (session.value && editable.value) applySession(await post(sessionUrl.value + '/submit', { revision: session.value.revision }) as Session);
  });
}
function schedulePoll(delay = 3000) {
  clearTimeout(pollTimer);
  const current = session.value;
  if (!current || current.status !== 'pending' || lifetime.signal.aborted) return;
  pollTimer = setTimeout(async () => {
    if (busy.value) return schedulePoll();
    try {
      const data = await read(api + '/' + encodeURIComponent(current.id)) as Session;
      if (session.value?.id === current.id && !lifetime.signal.aborted) {
        error.value = '';
        applySession(data);
      }
    } catch (cause) {
      if (session.value?.id === current.id && !lifetime.signal.aborted) {
        error.value = message(cause);
        schedulePoll(6000);
      }
    }
  }, delay);
}

function openBatchReview() {
  Object.assign(batch, { reviewer: '', reviewed_at: '', signature: '' });
  batchOpen.value = true;
}
function applyBatchReview() {
  if (!editable.value || !batch.reviewer.trim() || !batch.reviewed_at || !batch.signature) return;
  session.value?.items.forEach(entry => Object.assign(entry, batch, { reviewer: batch.reviewer.trim() }));
  batchOpen.value = false;
}
function isPng(value: string) {
  return /^data:image\/png;base64,/.test(value);
}
function openSignature(forBatch: boolean) {
  signatureForBatch.value = forBatch;
  signed.value = false;
  signatureOpen.value = true;
}
function resetCanvas() {
  const canvas = pad.value;
  canvas?.getContext('2d')?.clearRect(0, 0, canvas.width, canvas.height);
  signed.value = false;
  strokeId = undefined;
}
function point(event: PointerEvent, canvas: HTMLCanvasElement) {
  const box = canvas.getBoundingClientRect();
  return [(event.clientX - box.left) * canvas.width / box.width, (event.clientY - box.top) * canvas.height / box.height] as const;
}
function startStroke(event: PointerEvent) {
  const canvas = pad.value;
  const context = canvas?.getContext('2d');
  if (!canvas || !context || strokeId !== undefined || event.button !== 0) return;
  strokeId = event.pointerId;
  canvas.setPointerCapture(event.pointerId);
  const [x, y] = point(event, canvas);
  context.strokeStyle = '#111';
  context.lineWidth = 4;
  context.lineCap = 'round';
  context.lineJoin = 'round';
  context.beginPath();
  context.moveTo(x, y);
}
function moveStroke(event: PointerEvent) {
  const canvas = pad.value;
  const context = canvas?.getContext('2d');
  if (!canvas || !context || strokeId !== event.pointerId) return;
  const [x, y] = point(event, canvas);
  context.lineTo(x, y);
  context.stroke();
  context.beginPath();
  context.moveTo(x, y);
  signed.value = true;
}
function stopStroke(event: PointerEvent) {
  if (strokeId !== event.pointerId) return;
  strokeId = undefined;
  const canvas = pad.value;
  const pixels = canvas?.getContext('2d')?.getImageData(0, 0, canvas.width, canvas.height).data;
  signed.value = !!pixels?.some((value, index) => index % 4 === 3 && value > 0);
}
function applySignature() {
  const value = signed.value ? pad.value?.toDataURL('image/png') : '';
  if (!value) return;
  if (signatureForBatch.value) batch.signature = value;
  else if (item.value && editable.value) item.value.signature = value;
  signatureOpen.value = false;
}

function ask(title: string, text: string, run: () => void) {
  confirmAction.value = { title, text, run };
}
function closeConfirm(show: boolean) {
  if (!show) confirmAction.value = undefined;
}
function runConfirm() {
  const action = confirmAction.value;
  confirmAction.value = undefined;
  action?.run();
}
function askDiscard(proceed: () => void) {
  ask('放弃未保存内容？', '请先保存草稿，或确认放弃本次修改。', proceed);
}
function leave(proceed: () => void) {
  if (busy.value) return;
  if (dirty.value) askDiscard(proceed);
  else proceed();
}
defineExpose({ refresh, leave, busy, dirty });
onMounted(() => void readBoard());
onBeforeUnmount(() => {
  lifetime.abort();
  boardRead?.abort();
  clearTimeout(pollTimer);
});
</script>

<style scoped>
.tool-workspace { --tool-border: var(--v2-border, #dbe3e9); --tool-muted: var(--v2-muted, #617180); min-width: 0; }
.tool-workspace > .n-alert { margin-bottom: 10px; }
.workspace-grid { display: grid; grid-template-columns: minmax(0, 6fr) minmax(0, 3fr) minmax(128px, 1.4fr); gap: 12px; align-items: start; }
.workspace-grid.is-external { grid-template-columns: minmax(0, 1fr) minmax(138px, 22%); }
.workspace-panel { min-width: 0; padding: 11px; border: 1px solid var(--tool-border); border-radius: 8px; background: var(--v2-card, var(--cf-surface, transparent)); }
.panel-heading, .workspace-actions, .review-line, .item-tabs { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.panel-heading { justify-content: space-between; min-height: 24px; margin-bottom: 8px; }
.panel-heading h3 { font-size: 15px; margin: 0; }
.panel-heading small, .workspace-muted, .assets small { color: var(--tool-muted, #617180); }
.workspace-muted { margin: 4px 0; font-size: 13px; }
.asset-filters { display: grid; grid-template-columns: minmax(87px, .58fr) minmax(0, 1fr); gap: 6px; margin: 8px 0; }
.asset-list { overflow: auto; max-height: max(280px, calc(100dvh - 290px)); }
.assets, .templates { border-collapse: collapse; width: 100%; text-align: left; font-size: 12px; }
.assets th, .assets td, .templates th, .templates td { padding: 6px 4px; border-bottom: 1px solid var(--tool-border); vertical-align: top; overflow-wrap: anywhere; }
.templates th:first-child { width: 32px; }
.templates label { cursor: pointer; }
.assets th:first-child, .assets td:first-child { width: 22px; }
.assets small { display: block; margin-top: 3px; }
.assets tr.is-selected, .templates tr.is-selected { background: color-mix(in srgb, var(--v2-blue, #2364d2) 9%, transparent); }
.asset-reason { font-weight: 500; }
.template-list { display: grid; gap: 8px; max-height: max(280px, calc(100dvh - 280px)); overflow: auto; padding-block: 6px; }
.template-list :deep(.n-radio__label) { white-space: normal; overflow-wrap: anywhere; }
.workspace-panel > .n-radio-group { width: 100%; }
.workspace-editor { display: grid; gap: 9px; margin-top: 12px; min-width: 0; }
.tool-toolbar { display: flex; align-items: center; gap: 8px; min-width: 0; }
.tool-tabs { flex: 1; min-width: 0; flex-wrap: nowrap; overflow-x: auto; padding-block: 3px; }
.tool-tabs > .n-button, .tool-toolbar > .n-button { flex-shrink: 0; }
.review-line > label { display: grid; gap: 3px; min-width: 0; font-size: 12px; }
.qualified-options { display: inline-flex; align-items: center; flex-wrap: nowrap; }
.qualified-options :deep(.n-radio-button) { display: inline-flex; align-items: center; }
.review-line label .n-input { max-width: 142px; }
.review-line input[type=datetime-local] { border: 1px solid var(--tool-border, #dbe3e9); color: inherit; color-scheme: inherit; background: transparent; border-radius: 4px; min-height: 28px; max-width: 100%; font: inherit; }
.signature-preview { width: 90px; height: 36px; object-fit: contain; background: #fff; border: 1px solid #dbe3e9; }
.opening-panel { display: grid; gap: 10px; }
.opening-panel label { display: grid; gap: 4px; font-size: 12px; }
.editor-footer { justify-content: flex-end; margin-top: 12px; }
.sheet-scroll { overflow: auto; max-height: min(60vh, 620px); max-width: 100%; }
.sheet-tools { justify-content: flex-end; }
.sheet-frame { position: relative; }
.sheet-table { border-collapse: collapse; table-layout: fixed; width: 100%; background: #fff; color: #141414; font-size: 13px; }
.sheet-table td { border: 1px solid #cbd1d8; vertical-align: middle; white-space: pre-wrap; overflow-wrap: anywhere; }
.sheet-table input, .sheet-table select { box-sizing: border-box; width: 100%; min-width: 0; min-height: 26px; padding: 2px 4px; border: 1px solid #9ab6e7; border-radius: 2px !important; color: #182333; background: #eff6ff; font: inherit; }
.sheet-frame > img { position: absolute; object-fit: contain; pointer-events: none; }
.signature-in-cell { display: block; max-width: 100%; height: 35px; object-fit: contain; }
.archive-list { margin: 10px 0; padding-left: 20px; font-size: 13px; }
.dialog-actions { justify-content: flex-end; }
canvas { display: block; width: 100%; height: auto; touch-action: none; background: #fff; border: 1px solid #8592a1; }
@media (max-width: 680px) {
  .workspace-grid, .workspace-grid.is-external { grid-template-columns: minmax(0, 1fr); }
  .asset-list { max-height: 52dvh; }
  .template-list { max-height: 28dvh; }
  .tool-workspace :deep(.n-button) { min-height: 40px; }
  .assets :deep(.n-checkbox), .templates :deep(.n-radio) { padding-block: 10px; }
  .review-line input[type=datetime-local], .sheet-table input, .sheet-table select { min-height: 40px; }
}
</style>

<style>
.tool-workspace-dialog { width: min(520px, calc(100vw - 24px)); }
</style>
