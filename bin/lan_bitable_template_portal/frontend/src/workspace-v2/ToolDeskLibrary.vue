<script setup lang="ts">
import { computed, ref, useId } from 'vue';
import { NPopconfirm } from 'naive-ui';
import ToolManagementSettings from '../components/ToolManagementSettings.vue';
import ToolDeskDialog from './ToolDeskDialog.vue';
const props=defineProps<{r:Record<string,any>}>();
const cloneForm = useId();
const search=ref('');
const section=ref('library');
const shown=computed(()=>[...(props.r.library?.templates || []),...(props.r.library?.legacy_templates || [])]
  .filter(t=>[t.name,t.schema?.specialty,t.schema?.applicable].join(' ').toLowerCase().includes(search.value.trim().toLowerCase())));
const localIds=computed(()=>new Set((props.r.library?.legacy_templates || []).map((t:{id:string})=>t.id)));
</script>
<template>
  <div class="td-stack" v-if="r.board?.can_manage_templates">
    <section class="td-panel">
      <div class="td-heading"><div><h2>{{ section==='library'?'自检模板库':'工具责任人' }}</h2><small v-if="r.library && section==='library'">{{ r.library.templates.length }} 份在库模板 · {{ r.library.legacy_templates.length }} 份历史模板</small></div>
        <select data-native-select v-model="section" aria-label="管理内容"><option value="library">自检模板库</option><option value="people">工具责任人</option></select>
        <div v-if="section==='library'" class="td-actions"><input type="search" v-model="search" aria-label="搜索模板" placeholder="搜索模板名称、专业或适用范围"/>
          <button class="td-primary" :disabled="r.busy" @click="r.chooseImport()">导入模板</button><button :disabled="r.busy" @click="r.folderInput?.click()">导入文件夹</button></div>
      </div>
      <ToolManagementSettings v-if="section==='people'"/>
      <template v-else>
      <input :ref="el=>r.fileInput=el" hidden type="file" accept=".xlsx" @change="r.importTemplate"/>
      <input :ref="el=>r.folderInput=el" hidden type="file" accept=".xlsx" webkitdirectory multiple @change="r.importTemplateFolder"/>
      
      <p v-if="r.library?.warning" class="td-notice">{{ r.library.warning }}</p><p v-if="r.folderProgress" role="status">{{ r.folderProgress }}</p>
      <ul v-if="r.folderFailures.length"><li v-for="f in r.folderFailures" :key="f.file">{{ f.file }} · {{ f.reason }}</li></ul>
      <p v-if="!shown.length" class="td-empty">{{ r.library?'没有符合条件的模板':'正在读取模板…' }}</p>
      <div class="td-record-grid"><article v-for="t in shown" :key="t.id" class="td-stack">
        <div class="td-heading"><div><h3>{{ t.name }}</h3><small>版本 {{ t.version }} · {{ t.schema.specialty }} · {{ t.schema.cycle }}</small></div>
          <div class="td-actions"><span class="td-chip">{{ localIds.has(t.id)?'历史模板':t.active?'启用中':'已停用' }}</span><button v-if="!localIds.has(t.id) && t.linked_tools_count" :disabled="r.busy" @click="r.startRebind(t)">批量换绑</button></div></div>
        <div class="td-row"><small>关联 {{ t.linked_tools_count || 0 }} 台工具 · 办理中 {{ t.open_usage || 0 }} 项</small>
          <button :disabled="r.busy" @click="r.startTemplateClone(t)">另存为新模板</button>
        </div>
        <details v-if="!localIds.has(t.id)"><summary>文件与启停管理</summary>
          <div class="td-actions">
              <button :disabled="r.busy" @click="r.chooseImport(t)">替换文件</button>

              <NPopconfirm @positive-click="r.toggleTemplate(t)"><template #trigger><button :disabled="r.busy">{{ t.active?'停用':'启用' }}</button></template>确认{{ t.active?'停用':'启用' }}「{{ t.name }}」？</NPopconfirm>
              <NPopconfirm @positive-click="r.deleteTemplate(t)"><template #trigger><button :disabled="r.busy || !!t.linked_tools_count" class="td-danger">删除</button></template>确认删除「{{ t.name }}」？已填写记录保留原版本。</NPopconfirm>
          </div></details>
        <details><summary>检查内容 · {{ t.schema.checks.length }} 项</summary>
          <p>{{ t.schema.applicable }}</p><p>{{ t.schema.precautions }}</p><ol><li v-for="c in t.schema.checks" :key="c.row"><strong>{{ c.name }}</strong> · {{ c.method }} · {{ c.standard }}</li></ol></details>
      </article></div>
      </template>
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