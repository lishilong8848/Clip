import {parse} from '@vue/compiler-sfc';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const component=fileURLToPath(new URL('../src/workspace-v2/NativeSingleSelect.vue',import.meta.url));
export function adaptSingleSelects(source,filename=''){
  if(/\/(?:NativeSingleSelect|SingleSelect)\.vue$/.test(filename)||!source.includes('<select'))return source;
  const {descriptor}=parse(source),edits=[];
  function visit(node){
    if(node.tag==='NativeSingleSelectV2')return;
    if(node.type===1&&node.tag==='select'&&!node.props.some(p=>p.name==='multiple'||p.name==='bind'&&(!p.arg||p.arg.content==='multiple'))){
      const structural=node.props.filter(p=>p.type===7&&['if','else-if','else','for'].includes(p.name)||p.type===7&&p.name==='bind'&&p.arg?.content==='key');
      let original=node.loc.source;for(const prop of structural)original=original.replace(prop.loc.source,'');
      edits.push([node.loc.start.offset,node.loc.end.offset,`<NativeSingleSelectV2 ${structural.map(p=>p.loc.source).join(' ')}>${original}</NativeSingleSelectV2>`]);return;
    }
    for(const child of node.children||[])visit(child);
  }
  visit(descriptor.template?.ast||{});if(!edits.length)return source;
  if(!descriptor.scriptSetup)throw new Error('Single select setup missing: '+filename);
  const relative=path.relative(path.dirname(filename),component).replace(/\\/g,'/');
  edits.push([descriptor.scriptSetup.loc.start.offset,descriptor.scriptSetup.loc.start.offset,`\nimport NativeSingleSelectV2 from ${JSON.stringify(relative.startsWith('.')?relative:'./'+relative)};`]);
  for(const [start,end,text]of edits.sort((a,b)=>b[0]-a[0]))source=source.slice(0,start)+text+source.slice(end);
  return source;
}
