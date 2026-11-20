import { readFileSync } from 'node:fs';
import { basename, dirname, resolve } from 'node:path';
import { parse } from '@vue/compiler-sfc';
import ts from 'typescript';
import { adaptCompactWorkspaces } from './workspace-compact-adapter.mjs';

const views = new Map([
  ['KnowledgeBasePage.vue', 'SerenityKnowledge'],
  ['LinkDirectoryPage.vue', 'SerenityDirectory'],
  ['LifeGuidePage.vue', 'SerenityGuide'],
]);

// These two full-page templates compile in the original controller's scope.
// Filter values, template refs, dialog ownership and request epochs stay local
// to each mounted page (including embedded workspace widgets).
const directoryViews = new Map([
  ['DocumentLibraryPage.vue', 'SerenityDocumentDirectory'],
  ['SignatureManagementPage.vue', 'SerenitySignatureDirectory'],
]);

function directoryView(source, path, name, watchFile) {
  const templatePath = resolve(dirname(path), '../workspace-v2/views', basename(path, '.vue') + '.html');
  const cssPath = resolve(dirname(path), '../workspace-v2', name + '.css');
  watchFile?.(templatePath);
  watchFile?.(cssPath);
  const setup = parse(source, { filename: path }).descriptor.scriptSetup;
  if (!setup || setup.lang !== 'ts') throw new Error('Missing directory controller in ' + path);
  let controller = setup.content;
  if (name === 'SerenityDocumentDirectory') {
    // A newly created record has no local detail ID before its first receipt.
    // Reconcile only the operation the open form is waiting for.
    controller = replaceOne(controller,
      'if (detail.value && result.record?.record_id === detail.value.record_id) saved(result.record);',
      `const ownWrite = pendingWrite.value?.operation_id === operation.operation_id;
      const ownUpload = pendingUpload.value?.operation_id === operation.operation_id;
      if (result.record && detailOpen.value && (ownWrite || ownUpload) &&
          (creating.value || detail.value?.record_id === result.record.record_id)) {
        saved(result.record);
        if (ownUpload) { pickedFile.value = null; uploadReason.value = ''; versionsReady.value = false; }
        detailError.value = '';
        notice.value = '原操作已核验成功，没有重复提交。';
      }`, 'verified document receipt');
  }
  const presentation = name === 'SerenityDocumentDirectory' ? `
const activeReceipt = computed(() => operations.value.find(op =>
  op.operation_id === (pendingWrite.value?.operation_id || pendingUpload.value?.operation_id)));
const tableQuery = ref('');
const visibleTables = computed(() => {
  const target = tableQuery.value.trim().toLocaleLowerCase();
  return tables.value.items.filter(table => !target || (table.name + ' ' + tableScopes(table)).toLocaleLowerCase().includes(target));
});
const documentTabs = [
  {id:'fields', name:'资料字段'}, {id:'attachments', name:'附件管理'},
  {id:'versions', name:'附件版本'}, {id:'index', name:'索引状态'}
];
` : "import NativeSingleSelect from '../workspace-v2/NativeSingleSelect.vue';";
  return '<script setup lang="ts">\n' + controller + '\n' + presentation + '\n</script>\n'
    + '<template>\n' + readFileSync(templatePath, 'utf8') + '\n</template>\n'
    + '<style scoped src="../workspace-v2/' + name + '.css"></style>\n';
}

function replaceOne(source, anchor, replacement, description) {
  const index = source.indexOf(anchor);
  if (index < 0 || source.indexOf(anchor, index + anchor.length) >= 0) {
    throw new Error(`Workspace library: expected one ${description}`);
  }
  return source.slice(0, index) + replacement + source.slice(index + anchor.length);
}

// The view's TS interface is also its runtime allowlist. Refs stay owned by the
// original controller; reactive() unwraps them and writes through on assignment.
function viewMembers(view, filename) {
  const script = parse(view, { filename }).descriptor.script;
  const tree = ts.createSourceFile(filename + '.ts', script?.content || '', ts.ScriptTarget.Latest, true);
  const contract = tree.statements.find(node => ts.isInterfaceDeclaration(node) && node.name.text === 'LibraryController');
  if (!contract?.members.length) throw new Error('Missing LibraryController in ' + filename);
  return contract.members.map(member => {
    if (!member.name || !ts.isIdentifier(member.name)) throw new Error('Invalid view binding in ' + filename);
    return member.name.text;
  });
}

// Keep version selection and page reads in the shared controller. Open the
// reader immediately and remember the exact failed request, including citations.
function prepareReader(script) {
  let result = replaceOne(script, 'let detailRequest = 0;', `let detailRequest = 0;
let readerAbort: AbortController | undefined;
const readerError = ref("");
let retryRead = () => {};
function readPage(page: number) {
  if (detail.value && !detailLoading.value && page >= 1 && page <= detailPages.value)
    void loadDetail(detail.value, undefined, page, "", detailViewVersion.value);
}
onBeforeUnmount(() => readerAbort?.abort());`, 'reader request state');
  result = replaceOne(result, 'const requestSeq = detailRequest;', `const requestSeq = detailRequest;
  readerAbort?.abort();
  const signal = (readerAbort = new AbortController()).signal;
  retryRead = () => { void loadDetail(item, chunk, targetPage, preserveError, viewVersion); };
  readerError.value = "";
  if (!detail.value || detail.value.id !== item.id) {
    sections.value = []; detailTotal.value = 0; detailPage.value = targetPage;
  }
  detail.value = item;`, 'reader request start');
  result = replaceOne(result, '${encodeURIComponent(id)}?${params}`, { cache: "no-store" }', '${encodeURIComponent(id)}?${params}`, { cache: "no-store", signal }', 'reader signal');
  result = replaceOne(result, 'if (requestSeq === detailRequest) error.value = cause', 'if (requestSeq === detailRequest && !signal.aborted) readerError.value = cause', 'reader error');
  result = replaceOne(result, 'function closeDetail(): void {', 'function closeDetail(): void {\n  readerAbort?.abort(); detailLoading.value = false; readerError.value = "";', 'reader dismissal');
  // Call the current closure. Capturing its initial value in reactive() would
  // leave the retry button connected to the initial no-op.
  return result + '\nfunction retryReaderRequest() { retryRead(); }\n';
}

function guideScript(blocks, predicate, name) {
  const matches = blocks.filter(predicate);
  if (matches.length !== 1) throw new Error('Offline guide is missing its unique ' + name);
  return matches[0];
}

function guideDocument(original, shell, layout) {
  const blocks = [...original.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script\s*>/gi)].map(match => match[1]);
  const storage = guideScript(blocks, body => body.trimStart().startsWith('const guideStorage ='), 'storage bridge');
  const corpus = guideScript(blocks, body => body.trimStart().startsWith('window.__CORPUS__='), 'offline corpus');
  const reader = guideScript(blocks, body => body.includes('function parseReadme(md)') && body.includes('function build(sections)'), 'reader controller');
  const start = reader.indexOf('function build(sections)');
  const end = reader.indexOf('function buildGlossary()', start);
  if (end < 0) throw new Error('Offline guide section renderer boundary changed');
  let runtime = reader.slice(0, start) + layout + '\n' + reader.slice(end);
  runtime = replaceOne(runtime, "document.getElementById('q').value = state.q;", "document.getElementById('quick-section').value = [...state.sec][0] || '';\n  document.getElementById('q').value = state.q;", 'chapter selector sync');
  runtime = replaceOne(runtime, "innerWidth > 960 ? '1' : '0'", "innerWidth > 960 && TODOS.length ? '1' : '0'", 'todo default');
  // about:srcdoc cannot rewrite its pathname or query. Keep filtering local;
  // native history entries can still hold the state used by citation Back.
  runtime = runtime.replace(/\bhistory\.(pushState|replaceState)\(/g, "guideHistory.$1(");
  const historyBridge = `function guideHistoryWrite(method, data, title, url) {
    try { history[method](data, title, url); }
    catch (reason) {
      if (reason.name !== 'SecurityError') throw reason;
      history[method](data, title);
    }
  }
  const guideHistory = {
    pushState: (...args) => guideHistoryWrite('pushState', ...args),
    replaceState: (...args) => guideHistoryWrite('replaceState', ...args)
  };`;
  const script = body => '<script>' + body + '</script>';
  const theme = "try {const mode=guideStorage.getItem('theme'); document.documentElement.classList.toggle('dark',mode ? mode==='dark' : matchMedia('(prefers-color-scheme:dark)').matches);} catch {}";
  return replaceOne(replaceOne(shell, '<!--SERENITY_STORAGE-->', script(storage + '\n' + theme), 'storage slot'),
    '<!--SERENITY_READER-->', script(corpus) + script(historyBridge + '\n' + runtime), 'reader slot');
}

/**
 * Run on original SFC source at the start of the V2 pre-transform. Return null
 * for unrelated files or query subrequests. Forward addWatchFile for Vite watch.
 */
export function adaptWorkspaceLibraryDesign(source, filename, addWatchFile) {
  const path = filename.replaceAll('\\', '/');
  if (/\/src\/components\/PlanConvergence(?:Page|Rules)\.vue$/.test(path)) {
    // Retain the V2 tab history, navigation guard and refresh handlers.
    const setup = parse(adaptCompactWorkspaces(source, path)).descriptor.scriptSetup;
    const view = resolve(dirname(path), '../workspace-v2/views', basename(path, '.vue') + '.html');
    addWatchFile?.(view);
    return '<script setup lang="ts">\n' + setup.content + '\n</script>\n<template>\n'
      + readFileSync(view, 'utf8') + '\n</template>\n<style scoped src="../workspace-v2/SerenityPlanWorkspace.css"></style>';
  }
  const directory = directoryViews.get(basename(path));
  if (directory && path.endsWith('/src/components/' + basename(path))) return directoryView(source, path, directory, addWatchFile);
  // Owned views already provide their complete template and scoped styles.
  // Returning their source allows the caller to bypass legacy layout rewrites.
  if (/\/src\/workspace-v2\/Serenity(?:Knowledge|Directory|Guide)\w*\.vue$/.test(path)) return source;
  const viewName = views.get(basename(path));
  if (!viewName || !path.endsWith('/src/components/' + basename(path))) return null;
  const read = relative => {
    const file = resolve(dirname(path), relative);
    addWatchFile?.(file);
    return readFileSync(file, 'utf8');
  };
  const viewPath = '../workspace-v2/' + viewName + '.vue';
  const members = viewMembers(read(viewPath), viewName + '.vue');
  const setup = parse(source, { filename }).descriptor.scriptSetup;
  if (!setup || setup.lang !== 'ts') throw new Error('Missing library page controller in ' + filename);
  let script = setup.content.replaceAll('\r\n', '\n');
  if (viewName === 'SerenityKnowledge') script = prepareReader(script);
  if (viewName === 'SerenityGuide') {
    const html = guideDocument(read('../assets/HowToLiveBetter.html'), read('../workspace-v2/SerenityGuideShell.html'), read('../workspace-v2/SerenityGuideLayout.js'));
    // Embedded HTML is a JS string here. Escape HTML delimiters before handing
    // this SFC to Vue, so corpus text cannot terminate its script element.
    const inline = JSON.stringify(html).replaceAll('<', '\\u003c');
    script = replaceOne(script, 'import guideHtml from "../assets/HowToLiveBetter.html?raw";', 'const guideHtml = ' + inline + ';', 'guide import');
  }
  const fields = members.map(name => viewName === 'SerenityKnowledge' && name === 'retryRead' ? 'retryRead: retryReaderRequest' : name);
  return '<script setup lang="ts">\n' + script +
    '\nimport { reactive as createLibraryModel } from "vue";\nimport LibraryView from ' + JSON.stringify(viewPath) + ';' +
    '\nconst libraryModel = createLibraryModel({' + fields.join(', ') + '});\n</script>\n' +
    '<template><LibraryView :model="libraryModel" /></template>\n';
}

