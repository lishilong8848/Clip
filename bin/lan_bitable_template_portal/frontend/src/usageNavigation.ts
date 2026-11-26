import { computed, inject, onBeforeUnmount, onMounted, provide, watch, type InjectionKey } from 'vue';
import { knownPage, recordPage, startUsage, type UsageClient } from './usageEvents';

export const usageClientKey: InjectionKey<UsageClient> = Symbol('usage-client');
type Scope = { owner: () => string; enter: (key: string) => void };
const scopeKey: InjectionKey<Scope> = Symbol('usage-page');

// A bare owner delegates page selection to the component that actually renders its tabs.
// The shell keeps the last page across remounts caused by scope, filters and permission refreshes.
export function provideUsage(client: UsageClient, screen: () => string, enabled = () => true) {
  provide(usageClientKey, client);
  const current = computed(screen);
  let last = '';
  function enter(key: string) {
    const [module,page] = key.split('/');
    if (!page || !knownPage(module,page)) { last = ''; return; }
    if (last !== key) { last = key; if (enabled()) recordPage(client,module,page); }
  }
  provide(scopeKey, {owner:() => current.value, enter});
  watch(current, (key, previous) => {
    if (key.includes('/')) enter(key);
    else if (key !== previous) last = '';
  }, {immediate:true,flush:'post'});
  function restored(event: PageTransitionEvent) {
    if (event.persisted && last) { const key = last; last = ''; enter(key); }
  }
  onMounted(() => { startUsage(); window.addEventListener('pageshow',restored); });
  onBeforeUnmount(() => window.removeEventListener('pageshow',restored));
}

export function useUsagePage(owner: string, screen: () => string) {
  const scope = inject(scopeKey,null);
  if (scope) watch(() => scope.owner() === owner ? screen() : null, key => {
    if (key) scope.enter(key);
  }, {immediate:true,flush:'post'});
}

// Overlays report only an actual open. Closing/repainting an overlay does not re-enter its host.
export function useUsageOverlay(owner: string, key: string, open: () => boolean) {
  const scope = inject(scopeKey,null);
  const client = inject(usageClientKey,null);
  const [module,page] = key.split('/');
  if (scope && client) watch(() => scope.owner() === owner && open(), visible => {
    if (visible) recordPage(client,module,page);
  }, {flush:'post'});
}
