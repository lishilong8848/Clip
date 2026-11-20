import { createApp } from 'vue';
import CabinetDeskScanner from '../src/workspace-v2/CabinetDeskScanner.vue';

// Invoked by the V2 view only. The scanner uses the already-authorized rack directory.
export function openCabinetScanner(trigger, scope, racks, openHistory) {
  const host = trigger?.closest('.cab-desk');
  if (!host?.isConnected || host.querySelector('[data-cabinet-scanner]')) return;
  const mount = document.createElement('div');
  mount.dataset.cabinetScanner = '';
  host.append(mount);
  let closed = false;
  function close() {
    if (closed) return;
    closed = true;
    observer.disconnect();
    app.unmount();
    mount.remove();
    if (trigger.isConnected) trigger.focus({ preventScroll: true });
  }
  const observer = new MutationObserver(() => { if (!mount.isConnected) close(); });
  const app = createApp(CabinetDeskScanner, {
    scope, racks, onClose: close,
    onSelect(room, rack) {
      close();
      if (host.isConnected) return openHistory(room, rack);
    }
  });
  app.mount(mount);
  observer.observe(document.body, { childList: true, subtree: true });
}
