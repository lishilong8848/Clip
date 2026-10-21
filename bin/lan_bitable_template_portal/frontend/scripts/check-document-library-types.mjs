import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/document-library');
await mkdir(output, { recursive: true });
const config = path.join(output, 'tsconfig.document-library.json');
await writeFile(config, JSON.stringify({
  extends: path.join(root, 'tsconfig.json'), compilerOptions: { types: [] },
  include: [path.join(root, 'src/components/DocumentLibrary*.vue'), path.join(root, 'src/api/documentLibrary.ts')],
}, null, 2));
const result = spawnSync(process.execPath, [path.join(root, 'node_modules/vue-tsc/bin/vue-tsc.js'), '--noEmit', '-p', config], {
  cwd: root, encoding: 'utf8', windowsHide: true, maxBuffer: 8 * 1024 * 1024,
});
if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);
if (result.error) throw result.error;
assert.equal(result.status, 0, 'Document-library files and their real imports must typecheck');
console.log('Document-library scoped typecheck passed.');
