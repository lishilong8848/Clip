// Static contract check for the V2 knowledge folder tree:
//  - KnowledgeFolders.vue uses the real Naive UI TreeOption/TreeRenderProps contracts
//    (no FolderTreeOption-as-TreeOption aliasing and no `any` masking).
//  - The workspace-knowledge-adapter replaces the flat pill filter + hidden management
//    drawer with an always-visible tree pane beside the document list.
//  - The reader modal and ConfirmDialog are preserved.
// This is intentionally build-free (no Vite/Playwright) so it can run in CI for rapid
// feedback on the adapter and the SFC contract.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { parse } from '@vue/compiler-sfc';
import { transform } from 'esbuild';
import { adaptKnowledgeFolders, adaptKnowledgeReader } from './workspace-knowledge-adapter.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const foldersVue = readFileSync(path.join(root, 'src/workspace-v2/KnowledgeFolders.vue'), 'utf8');
const foldersTs = readFileSync(path.join(root, 'src/workspace-v2/knowledgeFolders.ts'), 'utf8');
const pageSource = readFileSync(path.join(root, 'src/components/KnowledgeBasePage.vue'), 'utf8');
const helper=await transform(foldersTs,{loader:'ts',format:'esm',target:'es2022'});
const {buildFolderTree}=await import('data:text/javascript;base64,'+Buffer.from(helper.code).toString('base64'));
const nested=buildFolderTree([{id:'root',name:'根'},{id:'child',parent_id:'root',name:'子'},{id:'grand',parent_id:'child',name:'孙'}]);
assert.equal(nested.length,1);assert.equal(nested[0].children[0].children[0].id,'grand');
assert.equal(buildFolderTree([{id:'dangling',parent_id:'absent',name:'悬空'}])[0].id,'dangling');
assert.equal(buildFolderTree([{id:'a',parent_id:'b',name:'甲'},{id:'b',parent_id:'a',name:'乙'}]).length,2,'bad historical cycles must not recurse or hide documents');

// 1) renderLabel must receive Naive UI's TreeRenderProps shape ({option, checked, selected}),
//    never a bare FolderTreeOption (the compile-time bug that broke runtime tree labels).
assert.match(foldersVue, /function renderLabel\(props: \{ option: TreeOption; checked: boolean; selected: boolean \}\)/, 'renderLabel must be typed with the Naive UI TreeRenderProps contract');
assert.doesNotMatch(foldersVue, /function renderLabel\(option: FolderTreeOption\)/, 'renderLabel must not take a bare FolderTreeOption');
assert.ok(foldersVue.includes('const option = props.option as FolderTreeOption;'), 'renderLabel must unwrap props.option into the folder option');

// 2) The tree data must satisfy TreeOption (`TreeOptionBase & Record<string, unknown>`),
//    so FolderTreeOption needs an index signature rather than a cast masking soundness.
assert.ok(foldersTs.includes('export interface FolderTreeOption {'), 'FolderTreeOption is declared');
assert.match(foldersTs, /\[key: string\]: unknown;/, 'FolderTreeOption must carry the Record<string, unknown> index signature for TreeOption');

// 3) The modal must be a simple explicit shape (kind + node), not a broken reactive union.
assert.ok(foldersVue.includes('interface ModalState {'), 'explicit modal shape interface exists');
assert.doesNotMatch(foldersVue, /type ModalState =/, 'reactive union modal must be replaced by an explicit shape');

// 4) Move options must use a Naive UI select option type (SelectOption), not a local
//    incompatible FolderSelectOption.
assert.ok(foldersVue.includes('computed<SelectOption[]>(() => {'), 'move options typed with Naive UI SelectOption');
assert.doesNotMatch(foldersVue, /interface FolderSelectOption/, 'local FolderSelectOption must be removed');

// 5) The adapter must turn the flat pills + hidden drawer into a main tree pane.
let adapted = adaptKnowledgeReader(pageSource, '/components/KnowledgeBasePage.vue');
adapted = adaptKnowledgeFolders(adapted, '/components/KnowledgeBasePage.vue');
const template = parse(adapted).descriptor.template;
function classes(node) { return (node.tag ? [{ tag: node.tag, cls: (node.props?.find(p => p.name === 'class')?.value?.content || '').split(/\s+/) }] : []).concat((node.children || []).flatMap(classes)); }
const elements = classes(template.ast);
assert.equal(elements.filter(e => e.cls.includes('kb-group-filter')).length, 0, 'flat kb-group-filter pills must be removed');
assert.equal(elements.filter(e => e.cls.includes('kb-groups-panel')).length, 0, 'hidden kb-groups-panel drawer must be removed');
assert.ok(elements.some(e => e.cls.includes('kb-tree-pane')), 'always-visible kb-tree-pane must be inserted');
assert.ok(adapted.includes('<KnowledgeFolders'), 'KnowledgeFolders must be rendered in the pane');
assert.ok(adapted.includes('@select="(id) => switchGroupFilter(id)"'), 'tree select must read controller filter');
for (const event of ['@create="(name, parentId) => createFolder(name, parentId)"', '@rename="(id, name, parentId) => moveRenameFolder(id, name, parentId)"', '@delete="(g) => deleteGroupTarget = g"']) {
  assert.ok(adapted.includes(event), `tree must wire ${event.slice(0, 8)} handler`);
}
// The tree pane must be beside the document list (main navigation) and the tree renderer kept.
assert.ok(adapted.includes('<aside v-if="!recycle" class="kb-tree-pane v2-kb-tree-pane"'), 'tree pane is a top-level visible pane');
assert.ok(adapted.includes('<div class="kb-pane">'), 'document list pane is preserved beside the tree');
assert.equal(adapted.includes('title="管理分组"'), false, 'redundant management toggle button is removed');

// 6) Reader modal + ConfirmDialog must survive the folder-tree adaptation.
assert.ok(adapted.includes('<NModal :show="!!detail"'), 'reader modal is preserved');
assert.ok(adapted.includes('<ConfirmDialog'), 'ConfirmDialog is preserved');
assert.ok(adapted.includes(':open="!!deleteGroupTarget"') || adapted.includes(':open=\"!!deleteGroupTarget\"'), 'group delete confirm dialog is preserved');

// 7) KnowledgeFolders must keep the root/child create + move/rename helpers wired by the adapter.
assert.ok(adapted.includes('async function createFolder(name,parentId)'), 'root/child create wrapper injected');
assert.ok(adapted.includes('async function moveRenameFolder(id,name,parentId)'), 'folder move/rename wrapper injected');

console.log('PASS: KnowledgeFolders renderLabel/SelectOption/modal contracts + tree-pane adapter and reader/confirm preservation.');
