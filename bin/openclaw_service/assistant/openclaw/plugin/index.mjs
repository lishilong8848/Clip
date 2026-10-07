import { readFileSync } from 'node:fs';
import { definePluginEntry } from 'openclaw/plugin-sdk/plugin-entry';

const definitions = JSON.parse(readFileSync(new URL('./tools.json', import.meta.url), 'utf8'));
const bridge = new URL(process.env.LIGHTHOUSE_BRIDGE_URL);
if (bridge.protocol !== 'http:' || bridge.hostname !== '127.0.0.1' || !['/api/assistant/openclaw-tools', '/bridge'].includes(bridge.pathname)) {
  throw new Error('Invalid Lighthouse bridge');
}

export default definePluginEntry({
  id: 'lighthouse-tools',
  name: 'Lighthouse Business Tools',
  description: 'Permission-bound business queries and proposals, never direct business writes.',
  register(api) {
    const runKey = '__lighthouse_host_run_id';
    api.on('before_tool_call', (event, context) => {
      if (!definitions.some(definition => definition.name === event.toolName)) return;
      const runId = event.runId || context.runId;
      if (typeof runId !== 'string' || !runId) return { block: true, blockReason: 'Missing host run identity' };
      return { params: { ...event.params, [runKey]: runId } };
    });
    for (const definition of definitions) {
      api.registerTool(context => ({
        name: definition.name,
        label: definition.label,
        description: definition.description,
        parameters: definition.parameters,
        async execute(toolCallId, params, signal) {
          signal?.throwIfAborted();
          context.assertInvocationCurrent?.();
          const { [runKey]: runId, ...businessParams } = params;
          if (typeof runId !== 'string' || !runId) throw new Error('Lighthouse invocation expired');
          if (typeof context.agentId !== 'string' || !context.sessionKey?.startsWith(`agent:${context.agentId}:`)) {
            throw new Error('Lighthouse agent identity mismatch');
          }
          const response = await fetch(bridge, {
            method: 'POST',
            headers: { 'content-type': 'application/json', authorization: `Bearer ${process.env.LIGHTHOUSE_BRIDGE_TOKEN}` },
            body: JSON.stringify({ tool: definition.name, params: businessParams, call_id: toolCallId, session_key: context.sessionKey, run_id: runId, agent_id: context.agentId }),
            signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(320000)]) : AbortSignal.timeout(320000),
            redirect: 'error',
          });
          if (!response.ok) throw new Error('Lighthouse tool unavailable');
          const result = await response.json();
          signal?.throwIfAborted();
          context.assertInvocationCurrent?.();
          return { content: [{ type: 'text', text: JSON.stringify(result) }], details: result };
        },
      }), { name: definition.name });
    }
  },
});
