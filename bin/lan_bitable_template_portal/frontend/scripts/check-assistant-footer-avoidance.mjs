import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import ts from 'typescript';
import { parse } from '@vue/compiler-sfc';

const root = new URL('../', import.meta.url);
const source = await readFile(new URL('src/components/LighthouseAssistant.vue', root), 'utf8');
assert.match(source, /const MODAL_FOOTER = '[^']*\.v2-notice-editor-footer[^']*\.floating-upload-box/, 'notice and MOP action bars participate in modal avoidance');
const bundled = await build({ entryPoints: [fileURLToPath(new URL('src/assistantFooterAvoidance.ts', root))], bundle: true, write: false, format: 'esm' });
const { footerSafePosition, temporaryLauncherPlacement } = await import('data:text/javascript;base64,' + Buffer.from(bundled.outputFiles[0].text).toString('base64'));
let checks = 0;
const check = (condition, message) => { checks++; assert.ok(condition, message); };
const rect = (left, top, width, height) => ({ left, top, right: left + width, bottom: top + height, width, height });
const overlaps = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
const appearance = { shape: 'weather-lighthouse-mini', size: 56, snap_back: true };

for (const [width, height] of [[402, 874], [440, 956], [1440, 900]]) {
  const viewport = rect(0, 0, width, height), home = rect(width - 80, height - 136, 56, 56);
  const footer = rect(24, height - 148, width - 48, 124);
  const position = footerSafePosition(home, footer, viewport);
  check(position && !overlaps(rect(position.x, position.y, 56, 56), footer), width + ': footer is not obscured');
  check(position.x >= 12 && position.y >= 12 && position.x + 56 <= width - 12 && position.y + 56 <= height - 12, width + ': position stays on screen');
  const stored = { x: home.left, y: home.top }, original = JSON.stringify(stored);
  for (const snap_back of [true, false]) {
    const settings = { ...appearance, snap_back };
    const active = temporaryLauncherPlacement(settings, stored, position);
    check(active.appearance.snap_back === false && active.position === position, width + ': reuse normal moveTo position path');
    const restored = temporaryLauncherPlacement(settings, stored, footerSafePosition(home, null, viewport));
    check(restored.appearance === settings && restored.position === stored, width + ': original appearance/position restored');
    check(JSON.stringify(stored) === original && settings.snap_back === snap_back, width + ': user preference not mutated');
  }
  check(footerSafePosition(rect(12, 12, 56, 56), footer, viewport) === null, width + ': no collision means no movement');
  const keyboardViewport = rect(0, 100, width, 430), keyboardFooter = rect(24, 410, width - 48, 100);
  const keyboard = footerSafePosition(home, keyboardFooter, keyboardViewport);
  check(keyboard && keyboard.y >= 112 && !overlaps(rect(keyboard.x, keyboard.y, 56, 56), keyboardFooter), width + ': visual viewport / keyboard');
}
check(footerSafePosition(rect(1360, 740, 56, 56), rect(100, 730, 1240, 150), rect(0, 0, 1440, 900)) === null, 'desktop free side gutter remains unchanged');
check(footerSafePosition(rect(1300, 740, 56, 56), rect(100, 730, 1220, 150), rect(0, 0, 1440, 900))?.x === 1320 + 12, 'nearest free edge uses side gutter');

// Execute the real component's bounded placement functions against synthetic DOM geometry.
// No browser, server, storage or model process is used.
const setup = parse(source).descriptor.scriptSetup.content;
const tree = ts.createSourceFile('assistant.ts', setup, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const names = new Set(['visibleModalElement', 'visibleBottomSubmitBar', 'syncFooterAvoidance', 'scheduleFooterAvoidance', 'modalLayoutChanged', 'modalScrolled', 'saveBotPosition']);
const declarations = tree.statements.filter(node => ts.isFunctionDeclaration(node) && names.has(node.name?.text)).map(node => node.getText(tree)).join('\n');
const javascript = ts.transpileModule(declarations, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.None } }).outputText;
class FakeElement {
  constructor(box, z = 0) { this.box = box; this.style = { display: 'block', visibility: 'visible', opacity: '1', zIndex: String(z), position: 'static', bottom: 'auto' }; this.parentElement = null; this.owned = false; this.native = false; this.children = []; }
  getBoundingClientRect() { return this.box; }
  closest(selector) { return selector.includes('lighthouse') ? this.owned ? this : this.parentElement?.closest(selector) || null : (selector.includes('dialog') && this.modal || selector.includes('sticky-actions') && this.submitBar) || this.parentElement?.closest(selector) || null; }
  querySelectorAll() { return this.children; }
  matches(selector) { return selector === ':modal' ? this.native : !!this.modal; }
  querySelector(selector) { return selector === 'button' ? this.button || null : this.children.find(child => child.modal || child.submitBar) || null; }
  contains(node) { return node === this || this.children.includes(node); }
}
function harness(width = 402, height = 874) {
  const home = rect(width - 80, height - 136, 56, 56), icon = new FakeElement(home), modals = [], bars = [], frames = [];
  const observer = { observed: [], disconnect() { this.observed = []; }, observe(element) { this.observed.push(element); } };
  const document = { querySelectorAll: selector => selector.includes('dialog') ? modals : bars, querySelector: () => bars[0] || null }, window = { visualViewport: null }, writes = [];
  const factory = new Function('footerSafePosition', 'document', 'window', 'innerWidth', 'innerHeight', 'Element', 'getComputedStyle', 'requestAnimationFrame', 'icon', 'observer', 'writes', `
    const BUSINESS_MODAL = ${JSON.stringify('dialog[open],[role="dialog"][aria-modal="true"],.n-modal[role="dialog"],.n-dialog[role="dialog"]')};
    const MODAL_FOOTER = 'footer', ASSISTANT_LAYER = '.lighthouse,.lighthouse-layer,.lighthouse-launcher,[data-assistant-control="true"]';
    const BOTTOM_SUBMIT_BAR = '.drill-page .sticky-actions,.v2-learning-answerbar';
    let disposed=false, avoidanceFrame=0, avoidanceHome=null, observedModal, observedFooter, footerResize=observer, modalOrder=[];
    const launcher={value:icon}, footerAvoidance={value:null}, botPosition={value:null};
    function saveStoredPos(...args){writes.push(args);}
    ${javascript}
    return {sync:syncFooterAvoidance,schedule:scheduleFooterAvoidance,mutated:modalLayoutChanged,scrolled:modalScrolled,save:saveBotPosition,
      point:()=>footerAvoidance.value,base:()=>avoidanceHome,modal:()=>observedModal,stored:()=>botPosition.value,
      native:elements=>modalOrder=elements,dispose:()=>disposed=true};`);
  const api = factory(footerSafePosition, document, window, width, height, FakeElement, element => element.style,
    callback => { frames.push(callback); return frames.length; }, icon, observer, writes);
  const addModal = (box, footerBox, z = 4000) => {
    const modal = new FakeElement(box, z); modal.modal = modal;
    if (footerBox) { const footer = new FakeElement(footerBox); footer.parentElement = modal; modal.children.push(footer); }
    modals.push(modal); return modal;
  };
  const addBar = (box, position = 'fixed') => {
    const bar = new FakeElement(box, 100); bar.submitBar = bar; bar.button = new FakeElement(box);
    Object.assign(bar.style, {position, bottom:'62px'}); bars.push(bar); return bar;
  };
  return { ...api, icon, modals, bars, frames, observer, window, writes, addModal, addBar, home };
}
for (const [width, height] of [[402, 874], [440, 956], [1440, 900]]) {
  for (const positioning of ['fixed', 'sticky']) {
    const h = harness(width, height), bar = h.addBar(rect(0, height - 130, width, 68), positioning);
    h.sync();
    check(h.point() && !h.modal() && !overlaps(rect(h.point().x,h.point().y,56,56),bar.box), width + ': visible '+positioning+' submit bar avoided');
    check(h.observer.observed.includes(bar), width + ': bar ResizeObserver attached');
    h.addModal(rect(24, 20, width - 48, 300), null, 5000); h.sync();
    check(h.point() === null, width + ': modal without footer takes priority over page submit bar');
    h.modals.length=0; h.sync(); check(h.point(), width + ': return from modal avoids page bar again');
    bar.style.display='none'; h.sync(); check(h.point() === null, width + ': hidden submit bar ignored');
    bar.style.display='block'; bar.style.position='static'; h.sync(); check(h.point() === null, width + ': normal document footer ignored');
    bar.style.position=positioning; bar.box=rect(0,100,width,68); h.sync(); check(h.point() === null, width + ': non-bottom bar ignored');
    bar.box=rect(0,height-130,width,68); h.sync(); h.bars.length=0; h.sync();
    check(h.point() === null && h.base() === null && !h.writes.length, width + ': submit bar removal restores without saving');
  }
}
for (const [width, height] of [[402, 874], [440, 956], [1440, 900]]) {
  const h = harness(width, height), footer = rect(24, height - 148, width - 48, 124);
  const modal = h.addModal(rect(24, 12, width - 48, height - 24), footer);
  h.sync();
  check(h.point() && h.modal() === modal && h.observer.observed.includes(modal.children[0]), width + ': real sync tracks only modal/footer');
  h.icon.box = rect(h.point().x, h.point().y, 56, 56);
  const point = { ...h.point() }; h.sync();
  check(JSON.stringify(h.point()) === JSON.stringify(point), width + ': no oscillation after moveTo');
  h.save({ x: 40, y: 40 });
  check(h.writes.length === 0 && h.stored() === null, width + ': temporary drag never writes saved coordinates');
  const top = h.addModal(rect(60, 40, width - 120, 280), rect(60, 240, width - 120, 80), 5100);
  h.sync();
  check(h.modal() === top && h.point() === null, width + ': top modal wins, lower footer ignored');
  h.modals.pop(); h.sync();
  check(h.point(), width + ': returning to parent reinstates avoidance');
  h.modals.length = 0; h.sync();
  check(h.point() === null && h.base() === null && h.observer.observed.length === 0, width + ': close restores normal placement and releases observer');
  check(h.writes.length === 0, width + ': open/close does not persist coordinates');
  h.save({ x: 35, y: 90 });
  check(h.writes.length === 1 && h.stored().x === 35, width + ': normal user drag is unchanged');
}
const h = harness();
const native = h.addModal(rect(24, 24, 354, 826), rect(24, 720, 354, 130), 1); native.native = true; h.native([native]);
h.addModal(rect(24, 24, 354, 826), rect(24, 720, 354, 130), 99999);
h.sync(); check(h.modal() === native, 'native top layer wins over CSS z-index');
native.owned = true; h.sync(); check(h.modal() !== native, 'assistant-owned dialog is not an obstacle');
h.modals.forEach(modal => { modal.style.display = 'none'; }); h.sync(); check(h.point() === null, 'hidden/cached modals ignored');
const pageFooter = new FakeElement(rect(0, 700, 402, 174));
check(!h.mutated([{target:pageFooter,addedNodes:[],removedNodes:[]}]), 'ordinary page footer is not observed as a modal');
const owned = new FakeElement(rect(0, 0, 56, 56)); owned.owned = true;
check(!h.mutated([{target:owned,addedNodes:[],removedNodes:[]}]), 'own animation changes cannot start a RAF loop');
const clean = harness(); clean.schedule(); clean.schedule(); check(clean.frames.length === 1, 'layout changes coalesce to one RAF');
clean.frames[0](); clean.schedule(); check(clean.frames.length === 2, 'a later event can schedule another single frame');
clean.dispose(); clean.frames[1](); clean.schedule(); check(clean.frames.length === 2, 'no new work after disposal');
check(source.includes('footerResize?.disconnect()') && source.includes("removeEventListener('resize', scheduleFooterAvoidance)") && source.includes("removeEventListener('scroll', modalScrolled, true)"), 'observers and viewport listeners cleaned up');
check(!source.slice(source.indexOf('function syncFooterAvoidance'), source.indexOf('function showFeedback')).includes('open.value ='), 'placement never closes an existing assistant conversation');
console.log('Assistant footer avoidance: ' + checks + ' pure geometry/lifecycle checks passed (402x874, 440x956, 1440x900). No browser or network.');
