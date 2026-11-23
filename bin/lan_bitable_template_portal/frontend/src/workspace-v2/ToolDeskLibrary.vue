<script setup lang="ts">
import { computed, ref, useId } from 'vue';
import { NPopconfirm } from 'naive-ui';
import { ClipboardList, FolderUp, Search, Upload, UserRound } from 'lucide-vue-next';
import ToolManagementSettings from '../components/ToolManagementSettings.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
const props=defineProps<{r:Record<string,any>}>();
const cloneForm = useId();
const search=ref('');
const section=ref<'library'|'people'>('library');
const status=ref('all');
const localIds=computed(()=>new Set((props.r.library?.legacy_templates || []).map((t:{id:string})=>t.id)));
const templateStatus=(template:{id:string;active:boolean})=>localIds.value.has(template.id)?'legacy':template.active?'active':'inactive';
const statusLabel:Record<string,string>={active:'启用中',inactive:'已停用',legacy:'历史模板'};
const statuses=[{value:'all',label:'全部'}, {value:'active',label:'启用中'}, {value:'inactive',label:'已停用'}, {value:'legacy',label:'历史模板'}];
const shown=computed(()=>{
  const terms=search.value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return [...(props.r.library?.templates || []),...(props.r.library?.legacy_templates || [])].filter(t=>
    (status.value==='all' || status.value===templateStatus(t)) &&
    terms.every(term=>[t.name,t.schema?.specialty,t.schema?.applicable].join(' ').toLocaleLowerCase().includes(term)));
});
</script>
<template>
  <div class="td-stack" v-if="r.board?.can_manage_templates">
    <section class="td-panel template-desk">
      <nav class="td-tabs" aria-label="工具与模板分区">
        <button type="button" :aria-current="section==='library'?'page':undefined" :disabled="r.busy" @click="section='library'"><ClipboardList :size="16" aria-hidden="true"/>自检模板库</button>
        <button type="button" :aria-current="section==='people'?'page':undefined" :disabled="r.busy" @click="section='people'"><UserRound :size="16" aria-hidden="true"/>工具责任人</button>
      </nav>
      <ToolManagementSettings v-if="section==='people'"/>
      <section v-else class="td-stack template-library" aria-label="自检模板库">
      <div class="td-heading">
        <label class="template-search"><Search :size="16" aria-hidden="true"/><input type="search" v-model="search" aria-label="搜索模板" placeholder="名称、专业或适用范围"/></label>
        <div class="td-actions">
          <button type="button" :disabled="r.busy || r.libraryLoading" @click="r.refresh()">刷新模板</button>
          <button type="button" :disabled="r.busy" @click="r.folderInput?.click()"><FolderUp :size="16" aria-hidden="true"/>导入文件夹</button>
          <button type="button" class="td-primary" :disabled="r.busy" @click="r.chooseImport()"><Upload :size="16" aria-hidden="true"/>导入模板</button>
        </div>
      </div>
      <div class="td-row"><div class="td-tabs" role="group" aria-label="模板状态">
        <button v-for="item in statuses" :key="item.value" type="button" :aria-pressed="status===item.value" @click="status=item.value">{{ item.label }}</button>
      </div><small v-if="r.library">{{ shown.length }} 份模板</small></div>
      <input :ref="el=>r.fileInput=el" hidden type="file" accept=".xlsx" @change="r.importTemplate"/>
      <input :ref="el=>r.folderInput=el" hidden type="file" accept=".xlsx" webkitdirectory multiple @change="r.importTemplateFolder"/>
      <p v-if="r.library?.warning" class="td-notice">{{ r.library.warning }}</p>
      <p v-if="r.folderProgress" class="td-notice" role="status">{{ r.folderProgress }}</p>
      <ul v-if="r.folderFailures.length" class="template-failures"><li v-for="f in r.folderFailures" :key="f.file">{{ f.file }} · {{ f.reason }}</li></ul>
      <p v-if="!shown.length && !r.error" class="td-empty">{{ r.libraryLoading?'正在读取模板…':r.library?'没有符合条件的模板':'暂无模板' }}</p>
      <div class="td-list"><article v-for="t in shown" :key="t.id" class="template-row">
        <div><h3>{{ t.name }}</h3><small>{{ ['V'+t.version,t.schema.specialty,t.schema.cycle].filter(Boolean).join(' · ') }}</small></div>
        <div class="template-status"><span class="td-chip" :data-template-status="templateStatus(t)">{{ statusLabel[templateStatus(t)] }}</span>
          <small v-if="!localIds.has(t.id)">关联 {{ t.linked_tools_count || 0 }} 台<span v-if="t.open_usage"> · 办理中 {{ t.open_usage }} 项</span></small></div>
        <div class="template-actions">
          <button v-if="!localIds.has(t.id) && t.linked_tools_count" type="button" :disabled="r.busy" @click="r.startRebind(t)">批量换绑</button>
          <details><summary>检查内容 · {{ t.schema.checks.length }} 项</summary>
            <p v-if="t.schema.applicable">{{ t.schema.applicable }}</p><p v-if="t.schema.precautions">{{ t.schema.precautions }}</p>
            <ol><li v-for="c in t.schema.checks" :key="c.row"><strong>{{ c.name }}</strong><span v-if="c.method"> · {{ c.method }}</span><span v-if="c.standard"> · {{ c.standard }}</span></li></ol>
          </details>
          <details><summary>模板操作</summary>
            <div class="td-actions">
              <button type="button" :disabled="r.busy" @click="r.startTemplateClone(t)">另存为新模板</button>
              <template v-if="!localIds.has(t.id)">
                <button type="button" :disabled="r.busy" @click="r.chooseImport(t)">替换文件</button>
                <NPopconfirm @positive-click="r.toggleTemplate(t)"><template #trigger><button type="button" :disabled="r.busy">{{ t.active?'停用':'启用' }}</button></template>确认{{ t.active?'停用':'启用' }}「{{ t.name }}」？</NPopconfirm>
                <NPopconfirm @positive-click="r.deleteTemplate(t)"><template #trigger><button type="button" :disabled="r.busy || !!t.linked_tools_count" class="td-danger">删除</button></template>确认删除「{{ t.name }}」？已填写记录保留原版本。</NPopconfirm>
              </template>
            </div>
          </details>
        </div>
      </article></div>
      </section>
    </section>
    <ToolDeskDialog v-if="r.templateClone" title="另存为新模板" :locked="r.busy" @close="r.templateClone=null">
      <template #actions><button type="submit" :form="cloneForm" class="td-primary" :disabled="r.busy">保存新模板</button></template>
      <form :id="cloneForm" class="td-stack" @submit.prevent="r.saveTemplateClone()"><fieldset class="td-stack" :disabled="r.busy">
        <label class="td-field">新模板名称<input v-model="r.templateClone.name" maxlength="100" required/></label>
        <div v-for="(c,i) in r.templateClone.checks" :key="i" class="td-grid"><label class="td-field">检查项 {{ Number(i)+1 }}<input v-model="c.name" :disabled="c.sharedName" required/></label>
          <label class="td-field">方法<textarea v-model="c.method" required/></label><label class="td-field">标准<textarea v-model="c.standard"/></label></div>
      </fieldset><p v-if="r.error" role="alert">{{ r.error }}</p></form>
    </ToolDeskDialog>
    <ToolDeskDialog v-if="r.rebindTemplate" title="批量更换关联模板" :locked="r.busy" @close="r.closeRebind(false)">
      <template #actions><button class="td-primary" :disabled="!r.rebindTarget || r.busy" @click="r.applyRebind()">确认换绑</button></template>
      <p>将「{{ r.rebindTemplate.name }}」关联的工具换绑到所选模板。</p>
      <label class="td-field">目标模板<select data-native-select v-model="r.rebindTarget" :disabled="r.busy"><option :value="null">选择已启用模板</option><option v-for="o in r.rebindOptions" :key="o.value" :value="o.value">{{ o.label }}</option></select></label>
      <p v-if="r.rebindProgress" role="status">{{ r.rebindProgress }}</p><p v-for="(f,i) in r.rebindFailures" :key="i">{{ f.scope }} · {{ f.name }} · {{ f.error }}</p>
      <p v-if="r.error" role="alert">{{ r.error }}</p>
    </ToolDeskDialog>
  </div>
</template>

<style scoped>
.tool-desk .template-desk,.tool-desk .template-library{gap:.75rem}
.template-search{display:flex;align-items:center;gap:.4rem;flex:1 1 220px;max-width:460px;min-width:0;color:var(--td-muted)}
.template-search input{flex:1;width:100%}
.template-row{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(0,.68fr) minmax(0,1fr);align-items:start;gap:.5rem 1rem}
.template-status{display:flex;flex-wrap:wrap;align-items:center;gap:.25rem .5rem}
.template-status [data-template-status=inactive],.template-status [data-template-status=legacy]{background:var(--td-field);color:var(--td-muted)}
.template-actions{display:flex;flex-wrap:wrap;align-items:start;justify-content:flex-end;gap:.35rem .55rem;min-width:0}
.template-actions details{max-width:100%;border:1px solid var(--td-line);border-radius:9px;background:var(--td-face)}
.template-actions details[open]{flex:1 1 100%;padding:0 .6rem .6rem}
.template-actions summary{padding:.46rem .6rem;min-height:37px;color:var(--td-blue);font-size:.85rem}
.template-actions ol,.template-failures{margin:.25rem 0 0;padding-inline-start:1.3rem;overflow-wrap:anywhere}
@container(max-width:840px){.template-row{grid-template-columns:minmax(0,1fr) auto}.template-actions{grid-column:1/-1;justify-content:start}}
@container(max-width:400px){.template-row{grid-template-columns:minmax(0,1fr)}.template-actions{grid-column:auto}}
@media(pointer:coarse){.tool-desk .template-desk :is(button,summary){min-height:44px}}
</style>
