<script setup lang="ts">
import ToolDeskSelection from './ToolDeskSelection.vue';
import ToolDeskSession from './ToolDeskSession.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
import ToolDeskSurface from './ToolDeskSurface.vue';
defineProps<{ r: Record<string,any> }>();
</script>
<template>
  <ToolDeskSurface>
    <div class="td-stack">
      <p v-if="r.error" role="alert" class="td-notice td-error">{{ r.error }}</p>
      <p v-for="w in r.sourceWarnings" :key="w" class="td-notice">{{ w }}</p>
      <p v-if="r.busy" role="status" class="td-muted">正在处理，请稍候…</p>
      <fieldset :disabled="!!r.busy" class="td-stack">
        <ToolDeskSession v-if="r.session" :r="r"/>
        <section v-else-if="r.recordsOpen" class="td-panel">
          <div class="td-heading"><div><h2 id="tool-records-title" tabindex="-1">{{ r.recordAsset ? r.recordAsset.name+'的办理记录' : '自检与送检记录' }}</h2>
            <small v-if="r.recordAsset">{{ r.scopeLabel(r.recordAsset.scope) }} · {{ r.recordAsset.model }}</small></div>
            <button v-if="!r.props.recordsOnly" @click="r.recordsOpen=false">返回选取工具</button></div>
          <p v-if="!r.visibleRecordSessions.length" class="td-empty">{{ r.busy?'正在读取记录…':'暂无办理记录' }}</p>
          <div class="td-list td-records">
            <template v-for="batch in r.visibleRecordSessions" :key="batch.id">
              <article v-for="tool in batch.history_items" :key="tool.id" class="td-record">
                <div><strong>{{ tool.asset_name }}</strong><p class="td-muted">{{ [r.scopeLabel(tool.scope),tool.location,tool.model].filter(Boolean).join(' · ') }}</p></div>
                <div><strong>{{ batch.kind==='self'?'维护自检':'工具送检' }}</strong><p class="td-muted">{{ [batch.owner_name,batch.created_at?.slice(0,16).replace('T',' ')].filter(Boolean).join(' · ') }}</p></div>
                <div><span class="td-chip">{{ r.recordStatus(tool) }}</span><p class="td-muted">{{ [tool.result_label,tool.tool_status_label].filter(Boolean).join(' · ') }}</p></div>
                <div class="td-actions"><button @click="r.openSaved(batch.id,tool.id)">打开办理</button><button @click="r.emit('asset-history',tool.id)">工具履历</button>
                  <button v-if="tool.can_release && !tool.deletion_pending" @click="r.manageRecord(batch,tool,'release')">取消办理</button>
                  <button v-if="tool.can_delete && !tool.deletion_pending" class="td-danger" @click="r.manageRecord(batch,tool,'delete')">删除记录</button></div>
                <p v-if="tool.deletion_pending || tool.deletion_error || tool.result_notice" class="td-notice">{{ tool.deletion_error || tool.result_notice || '正在核验删除结果' }}</p>
              </article>
            </template>
          </div>
          <button v-if="r.recordCursor" @click="r.loadRecords(true)">更多记录</button>
        </section>
        <ToolDeskSelection v-else :r="r"/>
      </fieldset>
      <ToolDeskDialog v-if="r.confirmAction" :title="r.confirmAction.title" @close="r.closeConfirm(false)">
        <div class="td-row"><p>{{ r.confirmAction.text }}</p><div class="td-actions"><button @click="r.closeConfirm(false)">返回</button><button class="td-primary" @click="r.runConfirm()">确认</button></div></div>
      </ToolDeskDialog>
    </div>
  </ToolDeskSurface>
</template>
<style scoped>
.td-records{container:records / inline-size}.td-record{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr) minmax(6rem,.65fr) minmax(11rem,1.2fr);align-items:center;gap:.5rem 1rem}.td-record .td-actions{justify-content:flex-end}.td-record .td-notice{grid-column:1/-1}
@container records (max-width:750px){.td-record{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.td-record .td-actions{justify-content:start}}
@container records (max-width:440px){.td-record .td-actions{grid-column:1/-1}}
</style>
