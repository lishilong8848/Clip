<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import ToolDeskSelection from './ToolDeskSelection.vue';
import ToolDeskSession from './ToolDeskSession.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
import ToolDeskSurface from './ToolDeskSurface.vue';
type Row = {asset_name:string;scope:string;model?:string;location?:string;result_label?:string;tool_status_label?:string};
type RecordGroup = {id:string;kind:'self'|'external';owner_name?:string;created_at?:string;history_items:(Row & Record<string,any>)[]};
const props=defineProps<{ r: Record<string,any> }>();
const recordQuery=ref(''), recordKind=ref(''), recordState=ref('');
const allGroups=computed<RecordGroup[]>(()=>props.r.visibleRecordSessions);
const states=computed(()=>[...new Set(allGroups.value.flatMap(group=>group.history_items.map(row=>String(props.r.recordStatus(row)))))]);
const filteredGroups=computed(()=>{
  const terms=recordQuery.value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return allGroups.value.filter(group=>!recordKind.value||group.kind===recordKind.value).map(group=>({
    ...group,
    history_items:group.history_items.filter(row=>(!recordState.value||props.r.recordStatus(row)===recordState.value) &&
      terms.every(term=>[row.asset_name,props.r.scopeLabel(row.scope),row.model,row.location,group.owner_name]
        .filter(Boolean).join(' ').toLocaleLowerCase().includes(term))),
  })).filter(group=>group.history_items.length);
});
const recordCount=computed(()=>filteredGroups.value.reduce((total,group)=>total+group.history_items.length,0));
const hasFilters=computed(()=>!!(recordQuery.value.trim()||recordKind.value||recordState.value));
watch(()=>props.r.recordAsset?.id,()=>{recordQuery.value=recordKind.value=recordState.value='';});
</script>
<template>
  <ToolDeskSurface>
    <div class="td-stack">
      <p v-if="r.error" role="alert" class="td-notice td-error">{{ r.error }}</p>
      <p v-for="w in r.sourceWarnings" :key="w" class="td-notice">{{ w }}</p>
      <p v-if="r.busy" role="status" class="td-muted">正在处理，请稍候…</p>
      <fieldset :disabled="!!r.busy" class="td-stack">
        <ToolDeskSession v-if="r.session" :r="r"/>
        <section v-else-if="r.recordsOpen" class="td-panel records-panel" aria-labelledby="tool-records-title">
          <div class="td-heading"><div><h2 id="tool-records-title" tabindex="-1">{{ r.recordAsset ? r.recordAsset.name+'的办理记录' : '自检与送检记录' }}</h2>
            <small v-if="r.recordAsset">{{ r.scopeLabel(r.recordAsset.scope) }} · {{ r.recordAsset.model }}</small></div>
            <button v-if="!r.props.recordsOnly" @click="r.recordsOpen=false">返回选取工具</button></div>
          <div class="td-row record-toolbar">
            <div class="td-tabs" role="group" aria-label="办理类型">
              <button type="button" :aria-pressed="!recordKind" @click="recordKind=''">全部</button>
              <button type="button" :aria-pressed="recordKind==='self'" @click="recordKind='self'">维护自检</button>
              <button type="button" :aria-pressed="recordKind==='external'" @click="recordKind='external'">送检</button>
            </div>
            <div class="td-actions record-filters">
              <input v-model="recordQuery" type="search" aria-label="搜索办理记录" placeholder="工具、楼栋或办理人"/>
              <select data-native-select v-model="recordState" aria-label="办理状态"><option value="">全部状态</option>
                <option v-if="recordState && !states.includes(recordState)" :value="recordState">{{ recordState }}</option>
                <option v-for="state in states" :key="state" :value="state">{{ state }}</option>
              </select>
            </div>
          </div>
          <p v-if="!recordCount" class="td-muted" role="status">{{ r.busy?'正在读取记录…':hasFilters?'已加载记录中没有匹配项':'暂无办理记录' }}</p>
          <div class="td-list td-records">
            <template v-for="batch in filteredGroups" :key="batch.id">
              <article v-for="tool in batch.history_items" :key="tool.id" class="td-record">
                <div class="record-asset"><strong>{{ tool.asset_name }}</strong><p class="td-muted">{{ [r.scopeLabel(tool.scope),tool.location,tool.model].filter(Boolean).join(' · ') }}</p></div>
                <div class="record-meta"><strong>{{ batch.kind==='self'?'维护自检':'工具送检' }}</strong><p class="td-muted">{{ [batch.owner_name,batch.created_at?.slice(0,16).replace('T',' ')].filter(Boolean).join(' · ') }}</p></div>
                <div class="record-state"><span class="td-chip" :data-done="tool.archive_status==='archived'">{{ r.recordStatus(tool) }}</span>
                  <p v-if="tool.result_label || tool.tool_status_label" class="td-muted">{{ [...new Set([tool.result_label,tool.tool_status_label].filter(Boolean))].join(' · ') }}</p></div>
                <div class="td-actions record-actions"><button :disabled="tool.deletion_pending" @click="r.openSaved(batch.id,tool.id)">打开办理</button><button @click="r.emit('asset-history',tool.id)">工具履历</button>
                  <button v-if="tool.can_release && !tool.deletion_pending" @click="r.manageRecord(batch,tool,'release')">取消办理</button>
                  <button v-if="tool.can_delete && !tool.deletion_pending" class="td-danger" @click="r.manageRecord(batch,tool,'delete')">删除记录</button></div>
                <p v-if="tool.deletion_pending || tool.deletion_error || tool.result_notice" class="td-notice">{{ tool.deletion_error || tool.result_notice || '正在核验删除结果' }}</p>
              </article>
            </template>
          </div>
          <div v-if="recordCount || r.recordCursor" class="td-row">
            <small v-if="recordCount">{{ hasFilters?'匹配':r.recordCursor?'已加载':'共' }} {{ recordCount }} 条</small>
            <button v-if="r.recordCursor" @click="r.loadRecords(true)">加载更早记录</button>
          </div>
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
.tool-desk .records-panel{gap:.65rem}
.tool-desk .record-filters{display:grid;grid-template-columns:minmax(0,1fr) minmax(6rem,auto);width:min(100%,23.5rem);min-width:0}
.record-filters input{width:100%}
.td-records{container:records / inline-size}
.td-record{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,1fr) minmax(5.5rem,.65fr) minmax(11rem,1.3fr);align-items:center;gap:.45rem .8rem}
.td-record .record-actions{justify-content:flex-end}.td-record .td-notice{grid-column:1/-1}
.record-state [data-done=false]{background:var(--td-sky);color:var(--td-ink)}
@container records (max-width:750px){.td-record{grid-template-columns:minmax(0,1fr) minmax(0,.8fr)}.td-record .record-actions{justify-content:start}}
@container records (max-width:505px){
  .td-record{grid-template-columns:minmax(0,1fr) auto}
  .record-asset{grid-area:1/1}.record-state{grid-area:1/2;max-width:8rem;align-self:start;text-align:right}
  .record-meta,.record-actions{grid-column:1/-1}
}
@media(pointer:coarse){.tool-desk .records-panel button{min-height:44px}}
</style>
