// Pure renderer checks with lightweight SVG nodes, no browser automation.
import assert from 'node:assert/strict';
import { build } from 'esbuild';
import { Module } from 'node:module';
let writes = 0, layouts = 0, shifts = 0;
class SvgNode {
  constructor(tagName) {
    this.tagName = tagName; this.children = []; this.attributes = new Map();
    this.classList = { add() {}, remove() {} };
    this.style = new Proxy({}, { set: (target, key, value) => { if (key === 'transform') shifts++; target[key] = value; return true; } });
  }
  setAttribute(key, value) { writes++; this.attributes.set(key, value); }
  getAttribute(key) { return this.attributes.get(key) ?? null; }
  appendChild(child) { this.children.push(child); child.parent = this; return child; }
  append(...children) { for (const child of children) this.appendChild(child); }
  remove() { if (this.parent) this.parent.children.splice(this.parent.children.indexOf(this), 1); }
  replaceWith(child) { const index = this.parent.children.indexOf(this); this.parent.children[index] = child; child.parent = this.parent; }
  get childElementCount() { return this.children.length; }
  get lastElementChild() { return this.children.at(-1); }
  getBoundingClientRect() { layouts++; return { left: 1200, top: 650, width: parseFloat(this.style.width) || 64, height: parseFloat(this.style.height) || 64 }; }
  addEventListener() {}
  removeEventListener() {}
  setPointerCapture() {}
  releasePointerCapture() {}
}
globalThis.document = { createElementNS: (_, tag) => new SvgNode(tag), addEventListener() {}, removeEventListener() {} };
globalThis.window = { innerWidth: 1440, innerHeight: 900, addEventListener() {}, removeEventListener() {} };
let serial = 0;
const raf = new Set();
globalThis.requestAnimationFrame = () => { raf.add(++serial); return serial; };
globalThis.cancelAnimationFrame = id => raf.delete(id);
const { outputFiles } = await build({ entryPoints: ['src/vendor/bloub/entry.ts'], bundle: true, write: false, format: 'cjs', platform: 'node' });
const compiled = new Module('bot-rendering-fixture.cjs');
compiled._compile(outputFiles[0].text, 'bot-rendering-fixture.cjs');
const { BloubBot } = compiled.exports;
const bot = new BloubBot(new SvgNode('span'), { size: 64, follow: true, cycle: ['idle'], snapBack: false });
bot.tick(0);
const first = bot.frame;
writes = 0;
bot.render(first);
assert.equal(writes, 0, 'identical SVG frame must not rewrite attributes');
layouts = 0;
bot.followPointer(100, 600);
bot.tick(40); bot.tick(80);
assert.equal(layouts, 0, 'animation frames must not force layout reads');
assert.notDeepEqual(bot.frame.eyes, first.eyes, 'eyes still follow the pointer');
bot.fnDown({ button: 0, pointerId: 7, clientX: 1230, clientY: 680 });
shifts = 0;
for (let i = 0; i < 20; i++) bot.fnDrag({ pointerId: 7, clientX: 1200 - i, clientY: 600 - i });
assert.equal(shifts, 0, 'pointer bursts should be merged into one animation frame');
bot.tick(120);
assert.equal(shifts, 1);
assert.match(bot.svg.style.transform, /^translate3d/);
bot.fnDrag({ pointerId: 7, clientX: 1100, clientY: 500 });
bot.fnUp();
assert.equal(shifts, 2, 'release must flush the final pointer position');
bot.setState('orbit');
const orbital = bot.engineInstance.sample(bot.clock + 2);
assert.ok(orbital.arcs.length);
bot.render(orbital);
assert.equal(bot.layerArcBack.children[0].getAttribute('d'), orbital.arcs[0].back);
assert.equal(bot.layerArcFront.children[0].getAttribute('d'), orbital.arcs[0].front);
bot.setActive(false);
writes = 0;
bot.tick(160); bot.tick(200);
assert.equal(writes, 0, 'hidden page must not keep rendering');
bot.destroy();

const states = ['idle', 'wink', 'wide', 'play', 'orbit', 'swirl', 'burst', 'comet', 'egg', 'hexagon'];
const idleBot = new BloubBot(new SvgNode('span'), { size: 64, cycle: states });
let ms = 0;
const nextFrame = () => idleBot.tick(ms += 40);
const finishBlock = () => { idleBot.blockStart = idleBot.clock - idleBot.blockDuration(idleBot.block); nextFrame(); };
const random = Math.random;
let seed = 42;
try {
  Math.random = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 2 ** 32; };
  idleBot.setCycle(states, true);
  const seen = new Set();
  let previous = '';
  for (let i = 0; i < 100; i++) {
    assert.equal(idleBot.engineInstance.state, 'idle');
    assert.ok(idleBot.blockDuration(0) >= 6 && idleBot.blockDuration(0) <= 10);
    nextFrame();
    assert.equal(idleBot.engineInstance.state, 'idle', 'rest interval must not be skipped');
    finishBlock();
    const action = idleBot.engineInstance.state;
    assert.notEqual(action, 'idle');
    assert.notEqual(action, previous, 'consecutive idle actions must differ');
    seen.add(action); previous = action;
    const frame = idleBot.engineInstance.sample(idleBot.clock + 0.8);
    assert.ok(frame.bodyPath && !/NaN|Infinity/.test(frame.bodyPath), action);
    finishBlock();
    assert.equal(idleBot.engineInstance.state, 'idle', 'every action must return to rest');
  }
  assert.deepEqual([...seen].sort(), states.slice(1).sort(), 'random cycle must include native animations, not only expressions');
  finishBlock();
  const restored = new BloubBot(new SvgNode('span'), { size: 64, cycle: states });
  restored.setCycle(states, true);
  const snapshot = idleBot.playback();
  assert.equal(restored.restorePlayback(snapshot), true);
  assert.equal(restored.engineInstance.state, snapshot.state);
  assert.equal(restored.lastIdleAction, snapshot.lastIdleAction);
  assert.equal(restored.idleDuration, snapshot.idleDuration);
  restored.destroy();
  idleBot.fnDown({ button: 0, pointerId: 9, clientX: 1230, clientY: 680 });
  assert.equal(idleBot.engineInstance.state, 'idle', 'dragging interrupts decorative animation');
  finishBlock();
  assert.equal(idleBot.engineInstance.state, 'idle');
  idleBot.fnUp();
  idleBot.pause(); idleBot.setState('thinking');
  finishBlock();
  assert.equal(idleBot.engineInstance.state, 'thinking', 'task state takes priority over random animation');
  idleBot.setCycle(['idle', 'wink', 'wide'], true);
  for (let i = 0; i < 12; i++) {
    finishBlock();
    assert.ok(['idle', 'wink', 'wide'].includes(idleBot.engineInstance.state), 'quiet mode must not play decorative actions');
  }
  idleBot.setActive(false);
  const clock = idleBot.clock;
  writes = 0;
  for (let i = 0; i < 10; i++) nextFrame();
  assert.equal(idleBot.clock, clock, 'hidden page must not advance random animation');
  assert.equal(writes, 0);
} finally {
  Math.random = random;
  idleBot.destroy();
}
console.log('[BotRenderingCheck] unchanged-frame writes=0, per-frame layout reads=0, 20 pointer events=1 transform, hidden rendering=0; random idle actions, rest, handoff and interaction checks passed');
