<template>
  <NModal :show="!!assetId" preset="card" title="工具履历" class="tool-history-modal"
    style="width: min(680px, calc(100vw - 24px))" @update:show="show => { if (!show) emit('close') }">
    <NSpin :show="loading">
      <NAlert v-if="error" type="error" role="alert">{{ error }}</NAlert>
      <template v-else-if="history">
        <h3>{{ history.asset.name }}</h3>
        <p>{{ history.asset.scope }}楼 · {{ history.asset.profession || '专业未填写' }}<span v-if="history.asset.model"> · {{ history.asset.model }}</span></p>
        <div v-if="history.certificate || history.next_external_due" class="history-facts">
          <span v-if="history.certificate">最近检测 {{ history.certificate.actual }} · {{ validity(history.certificate.validity) }}</span>
          <span v-if="history.certificate?.valid_until">有效至 {{ history.certificate.valid_until }}</span>
          <span v-if="history.next_external_due">下次送检 {{ history.next_external_due }}</span>
        </div>
        <section v-for="order in history.orders" :key="order.id" class="history-entry">
          <div class="history-facts">
            <strong>{{ order.month }} · {{ order.kind === 'external' ? '送检' : '自检' }}</strong>
            <span>{{ states[order.status] || order.status }}</span>
            <span v-if="order.outcome && outcomes[order.outcome]">{{ outcomes[order.outcome] }}</span>
            <span v-if="order.sync.some(item => item.state !== 'done')">飞书待同步</span>
            <NButton v-if="order.workspace_id" text size="small" @click="emit('open-workspace', order.workspace_id)">查看原归档</NButton>
            <NButton v-else text size="small" @click="emit('open-order', order.id, order.month)">查看工单</NButton>
          </div>
          <p v-if="order.due || order.owner_name">
            <span v-if="order.due">计划 {{ order.due }}<template v-if="order.actual"> · 实际 {{ order.actual }}</template></span>
            <span v-if="order.owner_name">{{ order.due ? ' · ' : '' }}{{ order.owner_name }}</span>
          </p>
          <p v-if="order.inspection.result">{{ validity(order.inspection.result) }} · {{ order.inspection.report_no }} · {{ order.inspection.institution }}</p>
          <p v-if="order.reason || order.resolution || order.scrap_reason">{{ [order.reason, order.resolution, order.scrap_reason].filter(Boolean).join(' · ') }}</p>
          <details v-if="order.history?.length">
            <summary>办理记录</summary>
            <ul><li v-for="(item, index) in order.history" :key="index">{{ item.at.slice(0, 16).replace('T', ' ') }} · {{ item.name }} · {{ item.action }}</li></ul>
          </details>
        </section>
        <NEmpty v-if="!history.orders.length" description="暂无办理记录" />
        <p v-if="history.has_more">仅显示最近 50 张工单，可在月度工单中按月份查看其余记录。</p>
      </template>
    </NSpin>
  </NModal>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';
import { NAlert, NButton, NEmpty, NModal, NSpin } from 'naive-ui';
import { requestJson } from '../api/client';
import type { InspectionReport } from './ToolInspectionReportFields.vue';

type History = {
  asset: { name: string; scope: string; model: string; profession?: string };
  certificate?: { actual: string; validity: string; valid_until: string };
  next_external_due?: string; has_more?: boolean;
  orders: { id: string; workspace_id?: string; month: string; kind: string; status: string; outcome?: string; actual: string; due: string; owner_name: string;
    reason?: string; resolution?: string; scrap_reason?: string; inspection: Partial<InspectionReport>; sync: { state: string }[];
    history?: { action: string; name: string; at: string }[] }[];
};
const props = defineProps<{ assetId: string }>();
const emit = defineEmits<{ close: []; 'open-order': [id: string, month: string]; 'open-workspace': [id: string] }>();
const history = ref<History>();
const loading = ref(false);
const error = ref('');
const states: Record<string, string> = { pending: '待填写', review_pending: '待审核', submitted: '已提交', cancelled: '已取消' };
const outcomes: Record<string, string> = { normal: '正常', repair: '检修中', repaired: '检修完成', scrap: '待报废审批', scrapped: '已报废', rejected: '报废未通过' };
function validity(value: string) {
  return ({ qualified: '合格', unqualified: '不合格', valid: '有效', expired: '已过期' } as Record<string, string>)[value] || '有效期待核对';
}
watch(() => props.assetId, async (id, _, onCleanup) => {
  history.value = undefined;
  error.value = '';
  loading.value = !!id;
  if (!id) return;
  const controller = new AbortController();
  onCleanup(() => controller.abort());
  try {
    history.value = await requestJson('/api/tool-self-check/inventory/' + encodeURIComponent(id) + '/history',
      { signal: controller.signal, cache: 'no-store' }) as History;
  } catch (cause) {
    if (!controller.signal.aborted) error.value = cause instanceof Error ? cause.message : '工具履历暂不可用。';
  } finally {
    if (!controller.signal.aborted) loading.value = false;
  }
}, { immediate: true });
</script>

<style scoped>
.history-facts { display:flex; align-items:center; flex-wrap:wrap; gap:4px 12px; }
.history-entry { border-top:1px solid var(--n-border-color, currentColor); padding:8px 0; margin-top:8px; overflow-wrap:anywhere; }
.history-entry p { margin:4px 0; }
.history-entry summary { cursor:pointer; }
</style>
