import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import vm from 'node:vm';
import ts from 'typescript';

const source=await readFile(new URL('../src/workspace-v2/NoticeWorkspace.vue',import.meta.url),'utf8');
const script=source.split('<script setup lang="ts">')[1].split('</script>')[0];
const parsed=ts.createSourceFile('notice.ts',script,ts.ScriptTarget.ES2022,true);
const save=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='saveDraft');
assert.ok(save);
const writes=[];
const scopeWarning='旧草稿已保留，可继续办理当前通告';
const context=vm.createContext({
  URLSearchParams,action:{value:'update'},
  form:{value:{values:{target_record_id:'rec-same',scope:'D',work_type:'change',building_codes:['D'],progress:'现场已核对'},fields:[{key:'progress'}]}},
  saving:{value:false},detailError:{value:''},draftWarning:{value:scopeWarning},saved:{value:''},
  draftVersion:8,persist:()=>true,snapshot:()=> 'saved-current',message:{success:()=>{}},
  requestJson:async(path,options)=>{
    if(!options?.method)return {exists:false,draft_base:{version:8,client_revision:14},warning:scopeWarning};
    const body=JSON.parse(options.body);writes.push(body);
    return {draft:{version:9,client_revision:body.client_revision,fields:body.fields}};
  },
});
const compiled=ts.transpileModule(save.getText(parsed),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
vm.runInContext(compiled,context);
assert.equal(await vm.runInContext('saveDraft()',context),true,context.detailError.value);
assert.equal(writes[0].client_revision,15);
assert.equal(writes[0].scope,'D');
assert.equal(writes[0].fields.progress,'现场已核对');
assert.equal(context.draftVersion,9);
assert.equal(context.draftWarning.value,'');
context.draftVersion=7;
assert.equal(await vm.runInContext('saveDraft()',context),false);
assert.equal(writes.length,1,'a concurrent draft edit cannot be overwritten');

const legacy=await readFile(new URL('../../workbench_lite.py',import.meta.url),'utf8');
const restore=legacy.slice(legacy.indexOf('    async function restoreLiteDraft('),legacy.indexOf('    function deletePendingNoticeImage('))
  .replaceAll('{{','{').replaceAll('}}','}');
const legacyContext=vm.createContext({
  document:{getElementById:()=>({})},URLSearchParams,
  liteDraftRevision:0,liteDraftSavedRevision:0,liteDraftServerVersion:0,liteDraftContextKey:'same',
  resetLiteDraftTracking:()=>{},liteDraftContext:()=>({scope:'D',work_type:'change',target_record_id:'rec-same'}),liteDraftKey:()=> 'same',
  handleLiteAuthRequired:()=>false,setLiteStatus:value=>{legacyContext.notice=value;},
  fetch:async()=>({ok:true,json:async()=>({ok:true,data:{exists:false,draft_base:{version:8,client_revision:14},warning:scopeWarning}})}),
});
vm.runInContext(restore,legacyContext,{filename:fileURLToPath(new URL('../../workbench_lite.py',import.meta.url))});
await vm.runInContext('restoreLiteDraft()',legacyContext);
assert.equal(legacyContext.liteDraftRevision,15);
assert.equal(legacyContext.liteDraftSavedRevision,14);
assert.equal(legacyContext.liteDraftServerVersion,8);
assert.equal(legacyContext.notice,scopeWarning);
console.log('PASS: new/legacy draft scope fallback retains version counters, sends current values, and rejects concurrent overwrite. In-memory mocked requests only.');
