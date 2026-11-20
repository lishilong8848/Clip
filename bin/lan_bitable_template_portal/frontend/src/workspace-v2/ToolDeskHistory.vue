<script setup lang="ts">
import ToolDeskDialog from './ToolDeskDialog.vue';
defineProps<{r:Record<string,any>}>();
</script>
<template>
  <ToolDeskDialog v-if="r.props.assetId" :title="(r.history?.asset.name || '工具')+' · 工具履历'" @close="r.emit('close')">
    <p v-if="r.loading">正在读取履历…</p><p v-if="r.error" role="alert">{{ r.error }}</p>
    <template v-if="r.history"><div class="td-notice td-row"><strong>{{ r.history.asset.scope }}楼 · {{ r.history.asset.model }}</strong>
      <p v-if="r.history.certificate">{{ r.validity(r.history.certificate.validity) }} · 检测日期 {{ r.history.certificate.actual || '—' }} · 有效期至 {{ r.history.certificate.valid_until || '—' }}</p>
      <p v-if="r.history.next_external_due">下次送检 {{ r.history.next_external_due }}</p></div>
      <div class="td-record-grid"><article v-for="o in r.history.orders" :key="o.id" class="td-stack"><div class="td-heading"><h3>{{ o.month }} · {{ o.kind==='self'?'维护自检':'工具送检' }}</h3><div class="td-actions"><span class="td-chip">{{ r.outcomes[o.outcome] || r.states[o.status] }}</span><button @click="o.workspace_id ? r.emit('open-workspace',o.workspace_id) : r.emit('open-order',o.id,o.month)">打开办理详情</button></div></div>
        <p>{{ [o.owner_name,o.actual || o.due,o.reason,o.scrap_reason,o.resolution].filter(Boolean).join(' · ') }}</p>
        <p v-if="o.inspection?.report_no" class="td-muted">{{ o.inspection.report_no }} · {{ o.inspection.institution }} · {{ r.validity(o.inspection.result) }}</p>
        <details v-if="o.history?.length"><summary>操作记录</summary><p v-for="(h,i) in o.history" :key="i">{{ h.at }} · {{ h.name }} · {{ h.action }}</p></details>
        </article></div>
      <p v-if="!r.history.orders.length" class="td-empty">暂无办理履历</p><p v-if="r.history.has_more" class="td-muted">这里显示最近的办理履历。</p>
    </template>
  </ToolDeskDialog>
</template>