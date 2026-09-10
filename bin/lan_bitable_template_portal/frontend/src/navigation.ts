export type NavigateOptions = {
  replace?: boolean;
  hard?: boolean;
  bypassGuard?: boolean;
};

let navigationGuard: ((target: string, proceed: () => void) => boolean) | null = null;
let stableUrl = typeof window === 'undefined' ? '' : window.location.href;
export function registerNavigationGuard(guard: (target: string, proceed: () => void) => boolean): () => void {
  navigationGuard = guard; stableUrl = window.location.href;
  return () => { if (navigationGuard === guard) navigationGuard = null; };
}

// Register before App's route listener, which can unmount the active editor.
if (typeof window !== 'undefined') window.addEventListener('popstate', (event) => {
  const guard = navigationGuard, target = window.location.href;
  if (!guard || target === stableUrl) return;
  if (guard(target, () => navigate(target, { replace: true, bypassGuard: true }))) { stableUrl = target; return; }
  event.stopImmediatePropagation(); window.history.pushState(window.history.state, '', stableUrl);
}, true);

export function toAbsoluteUrl(target: string | URL): string {
  if (target instanceof URL) return target.toString();
  return new URL(target, window.location.origin).toString();
}

export function navigate(target: string | URL, options: NavigateOptions = {}): void {
  const url = toAbsoluteUrl(target);
  if (!options.bypassGuard && navigationGuard && !navigationGuard(url, () => navigate(target, { ...options, bypassGuard: true }))) return;
  if (options.hard) {
    window.location.assign(url);
    return;
  }
  const next = new URL(url);
  if (next.origin !== window.location.origin) {
    window.location.assign(url);
    return;
  }
  const nextPath = `${next.pathname}${next.search}${next.hash}`;
  const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (nextPath === currentPath) return;
  if (options.replace) {
    window.history.replaceState({}, "", nextPath);
  } else {
    window.history.pushState({}, "", nextPath);
  }
  stableUrl = window.location.href;
  window.dispatchEvent(new Event("popstate"));
}

export function navigateHard(target: string | URL): void {
  navigate(target, { hard: true });
}

export function replaceRoute(target: string | URL): void {
  navigate(target, { replace: true });
}
