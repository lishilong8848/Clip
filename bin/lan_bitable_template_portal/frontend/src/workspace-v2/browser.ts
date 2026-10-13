import { destination, navigate } from './navigation';
import { logicalUrl } from './version';

// Only the independent build imports these adapters; browser globals and legacy files are never patched.
export const workspaceLocation = {
  get href() { return logicalUrl().href; },
  set href(value: string) { navigate(value, { hard: true }); },
  get pathname() { return logicalUrl().pathname; },
  get search() { return logicalUrl().search; },
  get hash() { return window.location.hash; },
  set hash(value: string) { window.location.hash = value; },
  get origin() { return window.location.origin; },
  get protocol() { return window.location.protocol; },
  get host() { return window.location.host; },
  get hostname() { return window.location.hostname; },
  get port() { return window.location.port; },
  assign(value: string | URL) { navigate(value, { hard: true }); },
  replace(value: string | URL) { window.location.replace(destination(value)); },
  reload() { window.location.reload(); },
  toString() { return this.href; },
};
export const workspaceHistory = {
  get length() { return window.history.length; },
  get state() { return window.history.state; },
  pushState(data: unknown, unused: string, url?: string | URL | null) { window.history.pushState(data, unused, url == null ? url : destination(url)); },
  replaceState(data: unknown, unused: string, url?: string | URL | null) { window.history.replaceState(data, unused, url == null ? url : destination(url)); },
  back() { window.history.back(); }, forward() { window.history.forward(); }, go(delta?: number) { window.history.go(delta); },
};
export function workspaceOpen(url?: string | URL, target?: string, features?: string): Window | null {
  return window.open(url ? destination(url) : url, target, features);
}
