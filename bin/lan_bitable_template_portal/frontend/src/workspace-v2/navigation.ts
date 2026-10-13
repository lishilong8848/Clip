import { logicalUrl, workspaceUrl, WORKSPACE_ENTRY } from './version';
export type NavigateOptions = { replace?: boolean; hard?: boolean; bypassGuard?: boolean; legacy?: boolean };
const pagePaths = new Set(['/', '/admin/settings', '/admin/history-memory', '/signature', '/signature-management',
  '/engineer/mop', '/repair-management', '/repair-status', '/learning', '/plan-convergence', '/life-guide',
  '/link-directory', '/knowledge-base', '/daily-tasks', '/daily-tasks/morning-meeting/print', '/water-management',
  '/critical-guard', '/cabinet-power', '/cabinet-power/batches', '/drill-management', '/drill-management/print',
  '/workbench-lite', '/polling-work-order', '/polling-work-order/steps']);
type NavigationGuard=(target:string,proceed:()=>void)=>boolean;
let guard:NavigationGuard|null=null;
const guards=new Set<NavigationGuard>();
let stable = window.location.href;
for (const type of ['pageswap', 'pagereveal']) window.addEventListener(type, event => {
  const transition = (event as Event & { viewTransition?: { ready: Promise<void> } }).viewTransition;
  void transition?.ready.catch(() => undefined);
});
export function isWorkspacePage(target: string | URL): boolean {
  const url = new URL(target, window.location.origin);
  return url.origin === window.location.origin && (url.pathname === WORKSPACE_ENTRY || pagePaths.has(url.pathname.replace(/\/$/, '') || '/'));
}
export function destination(target: string | URL): string {
  const url = new URL(target, window.location.origin);
  if (isWorkspacePage(url)) return workspaceUrl(url.href);
  if (url.origin === window.location.origin && url.pathname === '/api/auth/login') {
    const next = url.searchParams.get('next') || '/';
    if (isWorkspacePage(next)) url.searchParams.set('next', workspaceUrl(next).replace(url.origin, ''));
  }
  return url.href;
}
export function registerNavigationGuard(value: typeof guard): () => void {
  if(value)guards.add(value);guard=value;stable=window.location.href;
  return()=>{if(value)guards.delete(value);const live=[...guards];guard=live[live.length-1]||null;};
}
export function toAbsoluteUrl(target: string | URL): string { return logicalUrl(new URL(target, window.location.origin).href).href; }
export function navigate(target: string | URL, options: NavigateOptions = {}): void {
  const logical = toAbsoluteUrl(target);
  const proceed = () => navigate(target, { ...options, bypassGuard: true });
  if (!options.bypassGuard && guard && !guard(logical, proceed)) return;
  const next = options.legacy ? new URL(target, window.location.origin).href : destination(target);
  if (options.hard || !isWorkspacePage(target)) { window.location.assign(next); return; }
  if (next === window.location.href) return;
  window.history[options.replace ? 'replaceState' : 'pushState']({}, '', next);
  stable = next;
  window.dispatchEvent(new Event('popstate'));
}
export function navigateHard(target: string | URL) { navigate(target, { hard: true }); }
export function navigateBack(fallback = '/', hardFallback = false) {
  const target=new URL(fallback,window.location.origin);
  // V2 opens building data directly, so the old building-selector return would loop.
  if(target.origin===window.location.origin && target.pathname===logicalUrl().pathname && !target.search && ['/drill-management','/critical-guard','/cabinet-power'].includes(target.pathname))fallback='/';
  navigate(fallback, { replace: !hardFallback, hard: hardFallback });
}
export function replaceRoute(target: string | URL) { navigate(target, { replace: true }); }
export function requestPageReload(confirm: (proceed: () => void) => void) {
  const proceed = () => window.location.reload();
  if (!guard) confirm(proceed); else if (guard(toAbsoluteUrl(window.location.href), proceed)) proceed();
}
window.addEventListener('popstate', event => {
  const next = window.location.href;
  if (!guard || next === stable) { stable = next; return; }
  if (guard(toAbsoluteUrl(next), () => navigate(next, { replace: true, bypassGuard: true }))) { stable = next; return; }
  event.stopImmediatePropagation(); window.history.pushState({}, '', stable);
}, true);

export function installLinkNavigation(): () => void {
  const click = (event: MouseEvent) => {
    const link = (event.target as Element)?.closest?.('a[href]') as HTMLAnchorElement | null;
    if (!link || event.defaultPrevented || link.hasAttribute('download') || link.origin !== window.location.origin) return;
    if (!isWorkspacePage(link.href) && link.pathname !== '/api/auth/login') return;
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.button !== 0 || link.target === '_blank') {
      link.href = destination(link.href); return;
    }
    event.preventDefault(); navigate(link.href);
  };
  document.addEventListener('click', click);
  return () => document.removeEventListener('click', click);
}
