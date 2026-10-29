import {createServer} from 'node:net';

// Vite 6 treats dev port 0 as 5173; ask the OS for a usable port first.
export async function listenTestServer(server) {
  try {
    const requested = server.config.server.port;
    if (requested) return await server.listen(requested);
    for (let attempt = 0; attempt < 3; attempt++) {
      const probe = createServer();
      const port = await new Promise((resolve, reject) => {
        probe.once('error', reject);
        probe.listen(0, '127.0.0.1', () => resolve(probe.address().port));
      });
      await new Promise((resolve, reject) => probe.close(error => error ? reject(error) : resolve()));
      try {
        return await server.listen(port);
      } catch (error) {
        // Another process may take the port after the probe is closed.
        if (attempt === 2 || !(['EACCES', 'EADDRINUSE'].includes(error.code)
            || error.message === `Port ${port} is already in use`)) throw error;
      }
    }
  } catch (error) {
    await server.close();
    throw error;
  }
}
