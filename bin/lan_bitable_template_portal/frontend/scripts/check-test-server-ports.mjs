import assert from 'node:assert/strict';
import {Server} from 'node:net';
import {fileURLToPath} from 'node:url';
import {createServer, preview} from 'vite';
import {listenTestServer} from './test-server.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const servers = [], attempted = [], originalListen = Server.prototype.listen;
let deniedPort = 5173;
Server.prototype.listen = function (...args) {
  const port = typeof args[0] === 'object' ? args[0].port : args[0];
  attempted.push(port);
  if (port === deniedPort) {
    queueMicrotask(() => this.emit('error', Object.assign(new Error('synthetic denied port'), {code:'EACCES'})));
    return this;
  }
  return originalListen.apply(this, args);
};
async function dev() {
  const server = await createServer({root, configFile:false, logLevel:'silent',
    server:{host:'127.0.0.1',port:0,hmr:false},
    plugins:[{name:'port-check',configureServer(s){s.middlewares.use('/__port',(_req,res)=>res.end('isolated'));}}]});
  servers.push(server);
  return server;
}
try {
  const legacy = await dev();
  await assert.rejects(legacy.listen(), {code:'EACCES'});
  assert.equal(attempted.at(-1),5173,'reproduce Vite dev port 0 fallback');
  await legacy.close();

  const [first,second] = await Promise.all([dev(),dev()]);
  await Promise.all([listenTestServer(first),listenTestServer(second)]);
  const ports = [first,second].map(s=>s.httpServer.address().port);
  assert.notEqual(ports[0],ports[1]);assert(!ports.includes(5173));
  for (const port of ports) assert.equal(await (await fetch(`http://127.0.0.1:${port}/__port`)).text(),'isolated');

  const retry = await dev(), actual = retry.listen.bind(retry);
  let tries = 0, previous;
  retry.listen = async port => {
    if (++tries === 1) {previous=port;deniedPort=port;return actual(port);}
    assert.notEqual(port,previous);deniedPort=5173;return actual(port);
  };
  await listenTestServer(retry);assert.equal(tries,2,'retry a denied port selected after probing');

  const unavailable = await dev();let failed=0;
  unavailable.listen = async () => {failed++;throw Object.assign(new Error('synthetic busy port'),{code:'EADDRINUSE'});};
  await assert.rejects(listenTestServer(unavailable),{code:'EADDRINUSE'});
  assert.equal(failed,3,'port recovery must be bounded');

  const broken = await dev();let unrelated=0;
  broken.listen = async()=>{unrelated++;throw new Error('broken fixture');};
  await assert.rejects(listenTestServer(broken),/broken fixture/);assert.equal(unrelated,1);

  const explicit = await dev();explicit.config.server.port=5173;
  const before=attempted.length;
  await assert.rejects(listenTestServer(explicit),{code:'EACCES'});
  assert.deepEqual(attempted.slice(before),[5173],'explicitly requested ports never silently change');

  const built=await preview({root,configFile:false,logLevel:'silent',preview:{host:'127.0.0.1',port:0,strictPort:true}});
  try {assert(built.httpServer.address().port>0);assert.notEqual(built.httpServer.address().port,5173);}
  finally {await new Promise(resolve=>built.httpServer.close(resolve));}
  console.log('PASS: denied default port, OS-assigned ports, concurrent servers, bounded bind recovery, explicit ports and preview.');
} finally {
  Server.prototype.listen=originalListen;
  await Promise.all(servers.map(s=>s.close()));
}
