import {parse} from '@vue/compiler-sfc';
export function adaptKnowledgeFolders(source,filename){
  if(!filename.endsWith('/KnowledgeBasePage.vue'))return source;
  // Keep the shared tree and CRUD; only V2 gets the responsive navigation wrapper.
  if(!source.includes('<KnowledgeFolders')||!source.includes('kb-tree-pane'))throw new Error('Shared knowledge folders anchor changed');
  const {descriptor}=parse(source);let selection,pane,folders,navigation,fileName;
  function visit(node){
    const classes=node.props?.find(p=>p.type===6&&p.name==='class')?.value?.content?.split(/\s+/)||[];
    if(classes.includes('kb-select-all'))selection=node;
    if(classes.includes('kb-pane'))pane=node;
    if(classes.includes('kb-tree-pane'))navigation=node;
    if(node.tag==='KnowledgeFolders')folders=node;
    if(classes.includes('kb-row-text'))fileName=node.children?.find(child=>child.tag==='strong');
    for(const child of node.children||[])visit(child);
  }
  visit(descriptor.template.ast);
  if(!selection||!pane||!navigation||!folders||!fileName)throw new Error('Knowledge navigation or file list anchor changed');
  const offset=pane.loc.start.offset+pane.loc.source.indexOf('>')+1;
  const location='<div class="v2-kb-location" role="status"><FolderOpen :size="16" /><strong>{{ recycle?\'回收站 · 全部已删除文件\':groupFilter===\'all\'?\'全部文件\':groupFilter===\'ungrouped\'?\'未分组\':groupOptions.find(option=>option.value===groupFilter)?.label||\'文件夹\' }}</strong><span>{{ total }} 个文件</span></div>';
  // Reuse every existing event binding, including the original confirmation targets.
  const folderNavigation=folders.loc.source.replace('<KnowledgeFolders ', '<KnowledgeFolderNavigation v-if="!recycle" ');
  const edits=[
    [navigation.loc.start.offset,navigation.loc.end.offset,folderNavigation],
    [fileName.loc.start.offset,fileName.loc.end.offset,'<KnowledgeFileName :name="item.name" :version="item.active_version" />'],
    [offset,offset,location],
  ];
  let code=source;
  for(const [start,end,replacement] of edits.sort((a,b)=>b[0]-a[0]))code=code.slice(0,start)+replacement+code.slice(end);
  const folderImport="import KnowledgeFolders from '../workspace-v2/KnowledgeFolders.vue';";
  if(!code.includes(folderImport))throw new Error('Shared knowledge folders import anchor changed');
  code=code.replace(folderImport,"import KnowledgeFolderNavigation from '../workspace-v2/KnowledgeFolderNavigation.vue';\nimport KnowledgeFileName from '../workspace-v2/KnowledgeFileName.vue';");
  code=code.replace(": '尚未上传文档'", ": groupFilter==='all'?'尚未上传文档':groupFilter==='ungrouped'?'当前没有未分组文件':'当前文件夹暂无文件，可展开子文件夹查看'");
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
