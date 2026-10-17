import {parse} from '@vue/compiler-sfc';
import ts from 'typescript';
import {adaptBrowserReferences} from './workspace-build-adapter.mjs';

// Each mounted module resolves its own view and filter target without changing the dashboard route.
export function adaptEmbeddedNavigation(source,filename){
  if(filename.endsWith('/ModuleWidget.vue')||source.includes('embedded-navigation-adapted'))return source;
  const block=parse(source).descriptor.scriptSetup;if(!block)return source;
  const script=block.content,tree=ts.createSourceFile(filename+'.ts',script,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),edits=[],bindings=[];
  for(const node of tree.statements){
    if(!ts.isImportDeclaration(node)||!ts.isStringLiteral(node.moduleSpecifier))continue;
    const path=node.moduleSpecifier.text;if(!/(?:^|\/)navigation(?:\.ts)?$|(?:^|\/)version(?:\.ts)?$/.test(path))continue;
    for(const spec of node.importClause?.namedBindings?.elements||[]){
      const imported=spec.propertyName?.text||spec.name.text;
      if(!['navigate','navigateBack','logicalUrl'].includes(imported)||spec.name.text!==imported)continue;
      const alias='__embedded_'+imported;
      edits.push([spec.getStart(tree),spec.end,imported+' as '+alias]);
      bindings.push(`const ${imported}=${imported==='navigate'?'useEmbeddedNavigation':imported==='navigateBack'?'useEmbeddedBack':'useEmbeddedRoute'}(${alias});`);
    }
  }
  let next=script;for(const [start,end,value] of edits.sort((a,b)=>b[0]-a[0]))next=next.slice(0,start)+value+next.slice(end);
  const path=filename.replaceAll('\\','/').includes('/workspace-v2/')?'./embeddedContext':'../workspace-v2/embeddedContext';
  const business=!filename.endsWith('/LighthouseAssistant.vue')&&(!filename.replaceAll('\\','/').includes('/workspace-v2/')||/(?:NoticeWorkspace|NoticeSopManager|WaterWorkspace|RepairWorkspace|MopWorkspace)\.vue$/.test(filename));
  if(business)next=adaptBrowserReferences(next,filename,path.replace('embeddedContext','browser.ts'),true)||next;
  if(filename.endsWith('/CriticalGuardPage.vue')||filename.endsWith('/SignaturePage.vue')){
    const signature=filename.endsWith('/SignaturePage.vue');
    const navigation=path.replace('embeddedContext','navigation');
    if(signature)next=next.replace('document.body.classList.add("signature-link-active");','if(!embeddedLeaveContext) document.body.classList.add("signature-link-active");').replace('document.body.classList.remove("signature-link-active");','if(!embeddedLeaveContext) document.body.classList.remove("signature-link-active");').replace('canvas && rect && linkMode.value &&','canvas && rect && !embeddedLeaveContext && linkMode.value &&');
    next=`import {registerNavigationGuard as registerEmbeddedLeave} from '${navigation}';\n${signature?"import {useDialog as useEmbeddedConfirm} from 'naive-ui';\n":''}`+next+`
const embeddedLeaveContext=currentEmbedded();
${signature?'const embeddedLeaveDialog=useEmbeddedConfirm();':''}
const releaseEmbeddedLeave=embeddedLeaveContext?registerEmbeddedLeave((_target,proceed)=>{
  if(${signature?'saving.value':'saving.value||sourceFileUploading.value'})return false;
  if(!${signature?'hasInk.value':'dirty.value'})return true;
  ${signature?"embeddedLeaveDialog.warning({title:'签名尚未保存',content:'离开将丢弃本次手写签名。',positiveText:'放弃并离开',negativeText:'继续签名',onPositiveClick:proceed});":'requestSwitch(proceed);'}
  return false;
}):()=>{};
function embeddedBeforeUnload(event:BeforeUnloadEvent){if(embeddedLeaveContext&&(${signature?'saving.value||hasInk.value':'saving.value||sourceFileUploading.value||dirty.value'})){event.preventDefault();event.returnValue='';}}
window.addEventListener('beforeunload',embeddedBeforeUnload);
onBeforeUnmount(()=>{releaseEmbeddedLeave();window.removeEventListener('beforeunload',embeddedBeforeUnload);});
`;
  }
  if(!edits.length&&next===script)return source;
  next=`\n// embedded-navigation-adapted\nimport {useEmbeddedNavigation,useEmbeddedBack,useEmbeddedRoute,currentEmbedded} from '${path}';\n`+bindings.join('\n')+'\n'+next;
  return source.slice(0,block.loc.start.offset)+next+source.slice(block.loc.end.offset);
}
