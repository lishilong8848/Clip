import { readdir, readFile, unlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const distDir = new URL("../dist/", import.meta.url);
const assetsDir = new URL("./assets/", distDir);
const indexPath = new URL("./index.html", distDir);
const assetsPath = fileURLToPath(assetsDir);

const indexHtml = await readFile(indexPath, "utf8");
const referenced = new Set();
for (const match of indexHtml.matchAll(/\/assets\/([^"')\s>]+)/g)) {
  referenced.add(match[1]);
}

let removed = 0;
for (const name of await readdir(assetsDir)) {
  if (!/^index-[A-Za-z0-9_-]+\.(js|css)$/.test(name)) continue;
  if (referenced.has(name)) continue;
  await unlink(join(assetsPath, name));
  removed += 1;
}

if (removed) {
  console.log(`pruned stale dist assets: ${removed}`);
}
