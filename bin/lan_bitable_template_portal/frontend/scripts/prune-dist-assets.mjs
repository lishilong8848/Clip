import { access, readdir, readFile, unlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { isAbsolute, join } from "node:path";

const distDir = new URL("../dist/", import.meta.url);
const assetsDir = new URL("./assets/", distDir);
const indexPath = new URL("./index.html", distDir);
const assetsPath = fileURLToPath(assetsDir);

const indexHtml = await readFile(indexPath, "utf8");
const reachable = new Set();
const pending = [];
const references = /["'`(](?:\/?assets\/|\.\/)([^"'`)\s?#,;]+)/g;

function addAsset(name) {
  if (!name || reachable.has(name)) return;
  if (isAbsolute(name) || name.split('/').includes('..') || /[\\:]/.test(name)) throw new Error("Unsafe frontend asset reference");
  reachable.add(name);
  if (/\.(js|css)$/.test(name)) {
    pending.push(name);
  }
}

for (const match of indexHtml.matchAll(references)) {
  addAsset(match[1]);
}

while (pending.length) {
  const name = pending.shift();
  const text = await readFile(join(assetsPath, name), "utf8");
  for (const match of text.matchAll(references)) {
    addAsset(match[1]);
  }
}

for (const name of reachable) await access(join(assetsPath, name));

let removed = 0;
for (const name of await readdir(assetsDir)) {
  if (!/\.(js|css)$/.test(name)) continue;
  if (reachable.has(name)) continue;
  await unlink(join(assetsPath, name));
  removed += 1;
}

if (removed) {
  console.log(`pruned stale dist assets: ${removed}`);
}
