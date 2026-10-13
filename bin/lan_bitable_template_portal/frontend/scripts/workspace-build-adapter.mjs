import ts from 'typescript';

export function adaptBrowserReferences(code, filename, adapterPath) {
  const component=filename.replaceAll('\\', '/').split('?')[0];
  const eventPage=component.endsWith('/EventManagementPage.vue');
  if (!eventPage&&!/\b(?:location|history|open)\b/.test(code)) return null;
  const tree = ts.createSourceFile(filename, code, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  // Bind the compiled module so local refs/imports named history or location stay untouched.
  const host = ts.createCompilerHost({ noLib: true, noResolve: true });
  host.getSourceFile = name => name === filename ? tree : undefined;
  const checker = ts.createProgram([filename], { noLib: true, noResolve: true }, host).getTypeChecker();
  const isGlobal = node => !checker.getSymbolAtLocation(node);
  const replacements = [];
  const aliases = { location: '__v2Location', history: '__v2History', open: '__v2Open' };
  const chat = component.endsWith('/LighthouseAssistant.vue');
  let chatUsed = false, eventBackUsed=false;
  function visit(node) {
    if(eventPage&&ts.isFunctionDeclaration(node)&&node.name?.text==='returnToOverview'&&node.body){
      replacements.push([node.body.getStart(tree),node.body.end,"{ __v2ReturnHome('/'); }"]);eventBackUsed=true;return;
    }
    if(eventPage&&ts.isStringLiteral(node)&&node.text==='事件态势与楼栋入口'){
      replacements.push([node.getStart(tree),node.end,JSON.stringify('事件管理')]);return;
    }
    if (chat && ts.isFunctionDeclaration(node) && node.name?.text === 'isNearBottom' && node.body) {
      replacements.push([node.body.getStart(tree), node.body.end, '{ return __v2ShouldFollow(thread.value); }']);chatUsed=true;return;
    }
    if (chat && ts.isBinaryExpression(node) && node.operatorToken.kind===ts.SyntaxKind.EqualsToken && node.left.getText(tree)==='target.scrollTop' && node.right.getText(tree)==='target.scrollHeight') {
      replacements.push([node.getStart(tree),node.end,'__v2FollowBottom(target)']);chatUsed=true;return;
    }
    if (ts.isPropertyAccessExpression(node)) {
      const owner = node.expression;
      if (ts.isIdentifier(owner) && owner.text === 'window' && isGlobal(owner) && aliases[node.name.text]) {
        replacements.push([node.getStart(tree), node.end, aliases[node.name.text]]); return;
      }
      if (ts.isIdentifier(owner) && isGlobal(owner) && ['location', 'history'].includes(owner.text)) {
        replacements.push([owner.getStart(tree), owner.end, aliases[owner.text]]); return;
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(tree);
  if (!replacements.length) return null;
  let result = code;
  for (const [start, end, value] of replacements.sort((a, b) => b[0] - a[0])) result = result.slice(0, start) + value + result.slice(end);
  const chatImport=chatUsed?`import { shouldFollow as __v2ShouldFollow, followBottom as __v2FollowBottom } from ${JSON.stringify(adapterPath.replace(/browser\.ts$/, 'chatMotion.ts'))};\n`:'';
  const eventImport=eventBackUsed?`import { navigate as __v2ReturnHome } from ${JSON.stringify(adapterPath.replace(/browser\.ts$/, 'navigation.ts'))};\n`:'';
  return eventImport + chatImport + `import { workspaceLocation as __v2Location, workspaceHistory as __v2History, workspaceOpen as __v2Open } from ${JSON.stringify(adapterPath)};\n` + result;
}
