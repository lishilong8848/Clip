// The portal owns installation, configuration and state migration. Run the
// pinned SDK's foreground gateway without its unrelated general CLI bootstrap.
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = process.env.LIGHTHOUSE_SDK_ROOT;
const pin = JSON.parse(readFileSync(new URL('./runtime.json', import.meta.url), 'utf8'));
const installed = root && JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'));
const port = Number(process.argv[2]);
if (!installed || installed.version !== pin.openclaw_version || !Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Verified gateway runtime or port is unavailable');
}
const { runGatewayCommand } = await import(pathToFileURL(resolve(root, 'dist/run-GhayMR-l.js')).href);
await runGatewayCommand({ port: String(port), bind: 'loopback', auth: 'token', wsLog: 'auto',
  allowUnconfigured: false, dev: false, force: false });
