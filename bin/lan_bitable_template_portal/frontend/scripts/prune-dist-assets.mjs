import { readdir, readFile, unlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const distDir = new URL("../dist/", import.meta.url);
const assetsDir = new URL("./assets/", distDir);
const indexPath = new URL("./index.html", distDir);
const assetsPath = fileURLToPath(assetsDir);

const indexHtml = await readFile(indexPath, "utf8");
const reachable = new Set();
const pending = [];

function addAsset(name) {
  if (!name || reachable.has(name)) return;
  reachable.add(name);
  if (/\.(js|css)$/.test(name)) {
    pending.push(name);
  }
}

for (const match of indexHtml.matchAll(/\/assets\/([^"')\s>]+)/g)) {
  addAsset(match[1]);
}

while (pending.length) {
  const name = pending.shift();
  let text = "";
  try {
    text = await readFile(join(assetsPath, name), "utf8");
  } catch {
    continue;
  }
  for (const match of text.matchAll(/(?:^|["'`(,])\/?assets\/([^"'`),\s]+)/g)) {
    addAsset(match[1]);
  }
}

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
