import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {parse} from '@vue/compiler-sfc';
import ts from 'typescript';

const source=await readFile(new URL('../src/workspace-v2/NoticeWorkspace.vue',import.meta.url),'utf8');
const script=parse(source).descriptor.scriptSetup.content;
const ast=ts.createSourceFile('notice.ts',script,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const declaration=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='revealActiveType');
assert(declaration,'The active-category visibility handler must exist');
const code=ts.transpileModule(declaration.getText(ast),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
let checks=0;
for(const width of [402,440,1440]){
  for(const [left,right,expected] of [[800,900,900-(width-12)],[-60,20,-72],[24,100,0]]){
    const nav={scrollWidth:2000,clientWidth:width-24,scrollLeft:100,
      getBoundingClientRect:()=>({left:12,right:width-12}),
      querySelector:()=>({getBoundingClientRect:()=>({left,right})})};
    new Function('typeNavigation',code+';revealActiveType();')({value:nav});
    assert.equal(nav.scrollLeft,100+(width===1440&&left===800?0:expected));checks++;
  }
}
const fitting={scrollWidth:500,clientWidth:600,scrollLeft:0,querySelector:()=>({})};
new Function('typeNavigation',code+';revealActiveType();')({value:fitting});
assert.equal(fitting.scrollLeft,0);checks++;
new Function('typeNavigation',code+';revealActiveType();')({value:null});checks++;
assert(source.includes('ref="typeNavigation"'));
assert(!declaration.getText(ast).includes('scrollIntoView'),'Do not move the whole page to reveal a category');checks++;
const daily=await readFile(new URL('../src/components/DailyTaskChecklistPage.vue',import.meta.url),'utf8');
assert.match(daily,/\.daily-header__actions\s*\{[^}]*flex-wrap:\s*wrap/);
assert.match(daily,/\.daily-header__actions\s*>\s*button\s*\{[^}]*flex:\s*none;[^}]*white-space:\s*nowrap/);checks++;
console.log('Responsive boundaries: '+checks+' checks passed; no browser or network.');
