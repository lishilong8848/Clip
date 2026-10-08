import { registerHooks } from 'node:module';
const modules = new Set();
registerHooks({ load(url, context, nextLoad) {
  const result = nextLoad(url, context);
  if (url.includes('/node_modules/')) modules.add(url);
  if (url.endsWith('/native-paths.mjs') && result.source) {
    const source = Buffer.from(result.source).toString('utf8').replace(
      'let bytes = 0;',
      'let bytes = 0; globalThis.__fixtureCache = () => ({bytes, reads: reads.size, existence: existence.size});');
    return {...result, source};
  }
  return result;
} });
const timer = setInterval(() => {
  process.stderr.write('[RuntimeFootprint] ' + JSON.stringify({pid:process.pid, uptime:process.uptime(),
    memory:process.memoryUsage(), modules:modules.size,
    cache:globalThis.__fixtureCache?.() || {bytes:0, reads:0, existence:0}}) + '\n');
}, 5000);
timer.unref();
