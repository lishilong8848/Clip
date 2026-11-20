<script setup lang="ts">
import { NPopconfirm } from 'naive-ui';
import { ArrowLeft, RefreshCw } from 'lucide-vue-next';
import ToolWorkspace from '../components/ToolWorkspace.vue';
import ToolAssetHistory from '../components/ToolAssetHistory.vue';
import ToolDeskSurface from './ToolDeskSurface.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
import ToolDeskLibrary from './ToolDeskLibrary.vue';
import ToolDeskOrder from './ToolDeskOrder.vue';
const props=defineProps<{r:Record<string,any>}>();
function goBack() {
  if(props.r.order) props.r.leave(props.r.closeOrder);
  else props.r.back();
}
</script>
<template>
  <ToolDeskSurface><section :ref="el=>r.root=el" class="td-page">
    <header class="td-heading td-page-title"><div><h1>{{ r.order?'办理详情':'工具管理' }}</h1>
      <small v-if="r.order">{{ [r.order.tool_name,r.order.model,r.assignment(r.order)].filter(Boolean).join(' · ') }}</small></div>
      <nav v-if="!r.order" class="td-tabs td-section-nav" aria-label="工具管理分区">
        <button :aria-current="r.view==='workspace'?'page':undefined" :disabled="r.busy || !!r.workspace?.busy" @click="r.selectView('workspace')">工作台</button>
        <button :aria-current="r.view==='followups'?'page':undefined" :disabled="r.busy || !!r.workspace?.busy" @click="r.selectView('followups')">办理记录</button>
        <button v-if="r.board?.can_manage_templates" :aria-current="r.view==='settings'?'page':undefined" :disabled="r.busy || !!r.workspace?.busy"
          @click="r.selectView('settings')">工具与模板</button>
      </nav>
      <div class="td-actions">
        <button v-if="r.order || !r.props.embedded" class="td-link" @click="goBack"><ArrowLeft :size="15"/>{{ r.order?'返回列表':'返回' }}</button>
        <button :disabled="r.busy || r.loading || !!r.workspace?.busy" @click="r.refresh()"><RefreshCw :size="15"/>刷新</button>
        <button v-if="r.board?.can_create_orders && !r.order && r.view==='workspace'" :disabled="r.busy" @click="r.openNewTool()">新增工具</button>
      </div>
    </header>
    <p v-if="r.error" class="td-notice td-error" role="alert">{{ r.error }}</p><p v-if="r.message" class="td-notice" role="status">{{ r.message }}</p>
    <ToolDeskOrder v-if="r.order" :r="r"/>
    <ToolDeskLibrary v-else-if="r.view==='settings' && r.board?.can_manage_templates" :r="r"/>
    <template v-else-if="r.view==='workspace' || r.view==='followups'">
      <section v-if="r.view==='followups'" class="td-panel tool-worklist">
        <div class="td-heading"><h2>检修与报废</h2><label class="td-field">办理月份<input type="month" :value="r.month" :disabled="r.loading || r.busy" @change="r.loadBoard(($event.target as HTMLInputElement).value)"/></label></div>
        <div class="td-row"><div class="td-tabs" aria-label="处置状态"><button v-for="s in r.stateOptions" :key="s.value" :aria-pressed="r.stateFilter===s.value" @click="r.stateFilter=s.value">{{ s.label }}</button></div>
          <input v-model="r.search" type="search" aria-label="搜索检修与报废记录" placeholder="工具、楼栋或领取人"/></div>
        <p v-if="!r.pagedOrders.length" class="td-empty">{{ r.loading?'正在读取处置记录…':r.emptyDescription }}</p>
        <div class="td-record-grid"><article v-for="o in r.pagedOrders" :key="o.id" class="td-row"><div><strong>{{ o.tool_name }}</strong>
          <p class="td-muted">{{ [o.scope+'楼',o.location,o.model,r.assignment(o)].filter(Boolean).join(' · ') }}</p>
          <span class="td-chip">{{ o.tool_status_label || r.outcomeLabels[o.outcome] }}</span>
          <p v-if="o.deletion_pending || o.deletion_error || o.result_notice">{{ o.deletion_error || o.result_notice || '删除结果核验中' }}</p></div>
          <div class="td-actions"><button :disabled="r.busy" @click="r.workspace?.leave(()=>r.openOrder(o,o.can_claim))">{{ r.orderAction(o) }}</button>
            <NPopconfirm v-if="o.can_remove_followup && !o.deletion_pending" @positive-click="r.removeFollowup(o)">
              <template #trigger><button :disabled="r.busy" class="td-danger">删除记录</button></template>确认删除这条处置记录及对应台账？
            </NPopconfirm></div></article></div>
        <div class="td-row"><span class="td-muted">共 {{ r.visibleOrders.length }} 条 · 第 {{ r.page }} 页</span><div class="td-actions">
          <button :disabled="r.page<=1" @click="r.changePage(r.page-1)">上一页</button><button :disabled="r.page*r.pageSize>=r.visibleOrders.length" @click="r.changePage(r.page+1)">下一页</button>
          <button v-if="r.board?.can_download_month && r.board?.archived_at" :disabled="r.busy" @click="r.download('/archive/'+r.month)">下载月度归档</button></div></div>
      </section>
      <ToolWorkspace :key="[r.view,r.linkedSession,r.linkedAsset,r.props.scope].join('/')" :ref="el=>r.workspace=el" :scope="r.props.scope==='CAMPUS'?'ALL':r.props.scope"
        :session-id="r.linkedSession" :asset-id="r.linkedAsset" :records-only="r.view==='followups'" :followup-order-ids="r.followupOrderIds"
        :inventory-warning="r.board?.inventory_error" @inventory-status="r.updateInventoryStatus" @records-refresh="r.loadBoard()"
        @asset-history="r.historyAsset=$event" @records="r.selectView('followups')" @choose="r.selectView('workspace')"/>
    </template>
    <ToolAssetHistory v-if="r.historyAsset" :asset-id="r.historyAsset" @close="r.historyAsset=''" @open-workspace="r.openHistoryWorkspace" @open-order="r.openWorkspaceOrder"/>
    <ToolDeskDialog v-if="r.afterDiscard" title="放弃未保存内容？" @close="r.afterDiscard=null"><p>离开后将丢失本次修改。</p><div class="td-actions">
      <button @click="r.afterDiscard=null">继续填写</button><button class="td-primary" @click="r.discard()">放弃修改并继续</button></div></ToolDeskDialog>
    <ToolDeskDialog v-if="r.addingTool" title="新增库存工具" :locked="r.busy" @close="r.addingTool=false">
      <form class="td-stack" @submit.prevent="r.createTool()"><fieldset :disabled="r.busy" class="td-grid">
        <label v-for="f in r.toolFields" :key="f.name" class="td-field">{{ f.name }}
          <select data-native-select v-if="f.type===3" v-model="r.newTool[f.name]" :disabled="f.name==='一级分类'"><option value="">请选择</option><option v-for="v in f.options" :key="v">{{ v }}</option></select>
          <select data-native-select v-else-if="f.type===7" v-model="r.newTool[f.name]"><option :value="undefined">未设置</option><option :value="true">是</option><option :value="false">否</option></select>
          <input v-else-if="f.type===2" v-model.number="r.newTool[f.name]" type="number" min="0" step="any"/>
          <input v-else v-model="r.newTool[f.name]" :type="f.type===5?'date':'text'" :readonly="f.name==='一级分类'"/>
        </label></fieldset><p v-if="r.error" role="alert">{{ r.error }}</p><button class="td-primary" :disabled="r.busy">确认新增工具</button></form>
    </ToolDeskDialog>
  </section></ToolDeskSurface>
</template>

<style scoped>
.td-section-nav{flex:1 1 auto;min-width:0}
@media(max-width:680px){.td-section-nav{order:1;flex-basis:100%}}
</style>
