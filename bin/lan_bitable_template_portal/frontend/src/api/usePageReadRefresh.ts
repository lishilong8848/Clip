import { onActivated, onBeforeUnmount, onDeactivated, onMounted } from 'vue';
import { READ_CACHE_UPDATED } from './readCache';

// Reload the current read-only view from the newly refreshed cache, never its editor.
export function usePageReadRefresh(matches: (url: URL) => boolean, refresh: () => Promise<void>, canRefresh: () => boolean): void {
  let active = true, running = false;
  function update(event: Event): void {
    const path = (event as CustomEvent).detail?.path;
    if (!active || running || typeof path !== 'string' || !canRefresh() || !matches(new URL(path, window.location.origin))) return;
    running = true;
    void refresh().catch(() => undefined).finally(() => { running = false; });
  }
  onMounted(() => window.addEventListener(READ_CACHE_UPDATED, update));
  onActivated(() => { active = true; });
  onDeactivated(() => { active = false; });
  onBeforeUnmount(() => { active = false; window.removeEventListener(READ_CACHE_UPDATED, update); });
}
