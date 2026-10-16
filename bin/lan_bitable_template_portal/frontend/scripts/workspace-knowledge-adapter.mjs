import {parse} from '@vue/compiler-sfc';
export function adaptKnowledgeFolders(source,filename){
  if(!filename.endsWith('/KnowledgeBasePage.vue'))return source;
  let code=source.replaceAll('\r\n','\n');
  const {descriptor}=parse(code);
  function findDiv(className){
    let node=null;
    (function visit(n){
      if(node)return;
      if(n.tag==='div'&&n.props?.some(p=>p.name==='class'&&p.value?.content.split(/\s+/).includes(className))){node=n;return;}
      for(const child of n.children||[])visit(child);
    })(descriptor.template.ast);
    return node;
  }
  function findButton(title){
    let node=null;
    (function visit(n){
      if(node)return;
      if(n.tag==='button'&&n.props?.some(p=>p.name==='title'&&p.value?.content===title)){node=n;return;}
      for(const child of n.children||[])visit(child);
    })(descriptor.template.ast);
    return node;
  }
  const filter=findDiv('kb-group-filter');
  const panel=findDiv('kb-groups-panel');
  const manage=findButton('管理分组');
  if(!filter)throw new Error('Knowledge group filter anchor changed');
  if(!panel)throw new Error('Knowledge groups panel anchor changed');
  // Replace the flat top pills + hidden management drawer with an always-visible folder
  // tree pane.  Also drop the now-dead "管理分组" toolbar toggle (the tree carries the
  // management actions itself).  Removals are applied largest offset first so every
  // earlier anchor keeps its original offset while the final insert uses a fresh marker.
  const removals=[
    [panel.loc.start.offset, panel.loc.end.offset],
    ...(manage?[[manage.loc.start.offset, manage.loc.end.offset]]:[]),
    [filter.loc.start.offset, filter.loc.end.offset],
  ].sort((a,b)=>b[0]-a[0]);
  for(const [start,end] of removals)code=code.slice(0,start)+code.slice(end);
  const pane=[
    '<aside v-if="!recycle" class="kb-tree-pane v2-kb-tree-pane" aria-label="文件夹导航">',
    '  <div class="kb-tree-pane-head" role="group" aria-label="文档范围">',
    '    <button type="button" :class="{ active: groupFilter === \'all\' }" :aria-pressed="groupFilter === \'all\'" @click="switchGroupFilter(\'all\')">全部</button>',
    '    <button type="button" :class="{ active: groupFilter === \'ungrouped\' }" :aria-pressed="groupFilter === \'ungrouped\'" @click="switchGroupFilter(\'ungrouped\')">未分组</button>',
    '  </div>',
    '  <KnowledgeFolders',
    '    :groups="groups"',
    '    :busy="groupBusy"',
    '    :selected-id="groupFilter !== \'all\' && groupFilter !== \'ungrouped\' ? groupFilter : \'\'"',
    '    @select="(id) => switchGroupFilter(id)"',
    '    @create="(name, parentId) => createFolder(name, parentId)"',
    '    @rename="(id, name, parentId) => moveRenameFolder(id, name, parentId)"',
    '    @delete="(g) => deleteGroupTarget = g"',
    '  />',
    '</aside>',
  ].join('\n');
  const paneMarker='<div class="kb-pane">';
  const paneIdx=code.indexOf(paneMarker);
  if(paneIdx<0)throw new Error('Knowledge pane anchor changed');
  code=code.slice(0,paneIdx)+pane+code.slice(paneIdx);
  code=code.replace('const groups = ref<GroupItem[]>([]);','import KnowledgeFolders from "../workspace-v2/KnowledgeFolders.vue";\nconst groups = ref<GroupItem[]>([]);');
  code=code.replace("'删除分组「' + deleteGroupTarget.name + '」？其中文件将退回未分组，文件本身与索引不受影响。'","'删除分组「' + deleteGroupTarget.name + '」？其中文件将退回未分组（文件与本索引均不受影响）；其下子文件夹会移动到本文件夹的上级目录，不会被删除。'");
  const wrappers=`async function createFolder(name,parentId){if(!name||!String(name).trim()||groupBusy.value)return;groupBusy.value=true;clearNotice();try{await requestJson(\`\${BASE}/groups\`,{method:"POST",body:JSON.stringify({name:name.trim(),...(parentId?{parent_id:parentId}:{})})});notice.value=\`已创建分组「\${name.trim()}」。\`;await loadGroups();}catch(cause){error.value=cause instanceof Error?cause.message:"创建分组失败。";}finally{groupBusy.value=false;}}

async function moveRenameFolder(id,name,parentId){if(!id||!String(name).trim()||groupBusy.value)return;groupBusy.value=true;clearNotice();try{await requestJson(\`\${BASE}/groups/\${encodeURIComponent(id)}\`,{method:"PATCH",body:JSON.stringify({name:name.trim(),parent_id:parentId||null})});notice.value=\`已更新分组「\${name.trim()}」。\`;await loadGroups();if(groupFilter.value!=="all"&&groupFilter.value!=="ungrouped")await loadList();}catch(cause){error.value=cause instanceof Error?cause.message:"更新分组失败。";}finally{groupBusy.value=false;}}

`;
  code=code.replace('async function createGroup(): Promise<void> {',wrappers+'async function createGroup(): Promise<void> {');
  return code;
}
export function adaptKnowledgeReader(source,filename){
  if(!filename.endsWith('/KnowledgeBasePage.vue'))return source;
  let code=source.replaceAll('\r\n','\n');
  const {descriptor}=parse(code);let preview;
  function visit(node){if(node.tag==='aside'&&node.props?.some(p=>p.name==='class'&&p.value?.content==='kb-detail'))preview=node;for(const child of node.children||[])visit(child);}
  visit(descriptor.template.ast);if(!preview)throw new Error('Knowledge preview anchor changed');
  const content=preview.loc.source.replace('<aside v-if="detail" class="kb-detail">','<section v-if="detail" class="kb-detail v2-kb-reader-body">').replace(/<\/aside>$/,'</section>')
    .replace('<template v-else>','<p v-else-if="v2DetailError" class="alert error" role="alert">{{v2DetailError}}<button type="button" @click="reloadDetail">重新读取</button></p><template v-else>');
  const modal=`<NModal :show="!!detail" :mask-closable="true" class="v2-kb-reader" @update:show="value=>{if(!value)closeDetail()}"><div role="dialog" aria-modal="true" :aria-label="detail?.name || '知识库原文'">${content}</div></NModal>`;
  code=code.slice(0,preview.loc.start.offset)+modal+code.slice(preview.loc.end.offset);
  code=code.replace('<script setup lang="ts">','<script setup lang="ts">\nimport {NModal} from "naive-ui";\nimport "../workspace-v2/knowledge.css";');
  code=code.replace('let detailRequest = 0;','let detailRequest = 0;\nlet v2DetailAbort:AbortController|undefined;\nconst v2DetailError=ref("");\nonBeforeUnmount(()=>v2DetailAbort?.abort());');
  code=code.replace('  if (detailLoading.value) return;\n  focusedOrdinal.value = null;','  focusedOrdinal.value = null;');
  code=code.replace('  const requestSeq = detailRequest;\n  detailLoading.value = true;',`  const requestSeq = detailRequest;
  v2DetailAbort?.abort();const controller=new AbortController();v2DetailAbort=controller;
  if(detail.value?.id!==item.id){sections.value=[];detailPage.value=targetPage;detailTotal.value=0;}
  detail.value={...item};v2DetailError.value="";
  detailLoading.value = true;`);
  code=code.replace('requestJson(`${BASE}/documents/${encodeURIComponent(id)}?${params}`, { cache: "no-store" })','requestJson(`${BASE}/documents/${encodeURIComponent(id)}?${params}`, { cache: "no-store", signal:controller.signal })');
  code=code.replace('if (requestSeq === detailRequest) error.value = cause instanceof Error ? cause.message : "原文读取失败";','if (requestSeq === detailRequest && !controller.signal.aborted) v2DetailError.value = cause instanceof Error ? cause.message : "原文读取失败";');
  code=code.replace('function closeDetail(): void {\n  detailRequest += 1;','function closeDetail(): void {\n  detailRequest += 1;\n  v2DetailAbort?.abort();detailLoading.value=false;v2DetailError.value="";');
  return code;
}
