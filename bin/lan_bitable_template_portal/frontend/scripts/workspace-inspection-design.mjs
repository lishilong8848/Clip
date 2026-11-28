import {parse} from '@vue/compiler-sfc';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';

const designDir=new URL('../src/workspace-v2/',import.meta.url);
function scrollSurface(node) {
  if(node.props?.some(p=>p.name==='ref' && p.value?.content==='scrollElement'))return node;
  for(const child of node.children || []) {
    const found=scrollSurface(child);
    if(found)return found;
  }
}
function worksheetEditors(node) {
  if(node.props?.some(p=>p.name==='class' && p.value?.content==='cell-editor-tools')){
    return node.children.filter(child=>child.tag==='template').map(child=>child.loc.source);
  }
  return (node.children||[]).flatMap(worksheetEditors);
}
// Mount before workspace-business-imports. The host still owns API and navigation resolution.
export default function workspaceInspectionDesign(){
  return {name:'workspace-inspection-design',enforce:'pre',transform(source,id){
    const path=id.replaceAll('\\','/');
    if(!/\/src\/components\/DailyInspection(?:Page|Workbook)\.vue$/.test(path))return null;
    const {descriptor}=parse(source,{filename:id});
    if(!descriptor.scriptSetup || !descriptor.template)throw new Error('Missing inspection controller: '+id);
    const read=name=>{
      const file=fileURLToPath(new URL(name,designDir));this.addWatchFile(file);
      return readFileSync(file,'utf8');
    };
    const page=path.endsWith('DailyInspectionPage.vue');
    let view;
    if(page)view=read('views/DailyInspectionPage.html');
    else {
      const paper=scrollSurface(descriptor.template.ast);
      if(!paper || !paper.loc.source.includes('data-address'))throw new Error('Worksheet scroll/edit surface changed');
      const editors=worksheetEditors(descriptor.template.ast);
      if(editors.length!==2)throw new Error('Worksheet editor contract changed');
      view=read('InspectionDeskWorkbook.html').replace('<!-- worksheet cell editor -->',()=>editors.join('\n')).replace('<!-- settings -->',()=>read('InspectionDeskSettings.html'))
        .replace('<!-- dialogs -->',()=>read('InspectionDeskDialogs.html')).replace('<!-- worksheet -->',()=>paper.loc.source);
    }
    const styles=page?'':descriptor.styles.map(s=>'<style'+(s.scoped?' scoped':'')+'>'+s.content+'</style>').join('\n');
    const imports=page?"import InspectionDeskStart from '../workspace-v2/InspectionDeskStart.vue';\nimport InspectionDeskArchive from '../workspace-v2/InspectionDeskArchive.vue';":
      "import InspectionDeskDialog from '../workspace-v2/InspectionDeskDialog.vue';";
    const confirmation=page?'':"\nconst deskProfession=ref('');\nwatch(awaitingStart,(waiting,previous)=>{if(previous && !waiting && currentActive.value)void load(true)});\nwatch(()=>[currentActive.value?.id,currentActive.value?.review_version,scope.value,day.value],()=>{deskProfession.value='';confirmSubmit.value=false});\n";
    return {code:'<template>'+view+'</template>\n<script setup lang="ts">\n'+imports+
      "\nimport '../workspace-v2/InspectionDesk.css';\n"+descriptor.scriptSetup.content+confirmation+'\n</script>\n'+styles,map:null};
  }};
}
