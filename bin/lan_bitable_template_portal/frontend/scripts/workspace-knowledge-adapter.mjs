import {parse} from '@vue/compiler-sfc';
export function adaptKnowledgeFolders(source,filename){
  if(!filename.endsWith('/KnowledgeBasePage.vue'))return source;
  // The folder tree and CRUD are now shared by legacy and V2; do not inject duplicates.
  if(!source.includes('<KnowledgeFolders')||!source.includes('kb-tree-pane'))throw new Error('Shared knowledge folders anchor changed');
  return source;
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
