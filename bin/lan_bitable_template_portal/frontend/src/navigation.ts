export type NavigateOptions = {
  replace?: boolean;
  hard?: boolean;
};

export function toAbsoluteUrl(target: string | URL): string {
  if (target instanceof URL) return target.toString();
  return new URL(target, window.location.origin).toString();
}

export function navigate(target: string | URL, options: NavigateOptions = {}): void {
  const url = toAbsoluteUrl(target);
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
  window.dispatchEvent(new Event("popstate"));
}

export function navigateHard(target: string | URL): void {
  navigate(target, { hard: true });
}

export function replaceRoute(target: string | URL): void {
  navigate(target, { replace: true });
}
