import { createApp } from 'vue';
import { LighthouseAssistant as Assistant } from './lighthouseEntry';
import LoadingIndicator from './components/LoadingIndicator.vue';
import UiTransition from './components/UiTransition.vue';
import { requestJson } from './api/client';
import { setModulePermissions } from './api/modulePermissions';
import './lighthouseWidget.css';

const host = document.getElementById('clipflow-lighthouse-widget');
if (host?.dataset.userId && !host.hasChildNodes() && !(window.parent !== window && new URLSearchParams(location.search).get('_assistant_frame') === '1')) {
  let app: ReturnType<typeof createApp> | null = null, loading = false;
  async function refreshPermissions() {
    if (loading || document.hidden) return;
    loading = true;
    try {
      const status = await requestJson('/api/auth/status');
      const modules = status.logged_in && status.user?.open_id === host!.dataset.userId ? status.user.modules || [] : [];
      setModulePermissions(modules);
      if (modules.includes('assistant')) {
        if (!app) {
          app = createApp(Assistant, { userId: host!.dataset.userId, userName: host!.dataset.userName || '' });
          app.component('LoadingIndicator', LoadingIndicator).component('UiTransition', UiTransition).mount(host!);
        }
      } else if (app) { app.unmount(); app = null; }
    } catch { /* The existing connection indicator handles transient network errors. */ }
    finally { loading = false; }
  }
  void refreshPermissions();
  const timer = window.setInterval(refreshPermissions, 60000);
  window.addEventListener('pagehide', () => window.clearInterval(timer), { once: true });
}
