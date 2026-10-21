import { logicalUrl, workspaceUrl, WORKSPACE_ENTRY } from './version';
import { moduleForRoute, modules, NOTICE_IDS } from './modules';
import { currentEmbedded } from './embeddedContext';
export type NavigateOptions = { replace?: boolean; hard?: boolean; bypassGuard?: boolean; legacy?: boolean };
const pagePaths = new Set(['/', '/admin/settings', '/admin/history-memory', '/signature', '/signature-management',
  '/engineer/mop', '/repair-management', '/repair-status', '/learning', '/plan-convergence', '/life-guide',
  '/link-directory', '/knowledge-base', '/document-library', '/daily-tasks', '/daily-tasks/morning-meeting/print', '/water-management', '/work-handover',
  '/critical-guard', '/risk-inspections', '/cabinet-power', '/cabinet-power/batches', '/drill-management', '/drill-management/print',
  '/workbench-lite', '/polling-work-order', '/polling-work-order/steps']);
type NavigationGuard=(target:string,proceed:()=>void)=>boolean;
const guards=new Set<NavigationGuard>();
const guardOwners=new Map<NavigationGuard,string>();
let stable = window.location.href;
for (const type of ['pageswap', 'pagereveal']) window.addEventListener(type, event => {
  const transition = (event as Event & { viewTransition?: Partial<Record<'ready'|'finished'|'updateCallbackDone',Promise<void>>> }).viewTransition;
  for(const state of ['ready','finished','updateCallbackDone'] as const)void transition?.[state]?.catch(() => undefined);
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
export function registerNavigationGuard(value: NavigationGuard|null): () => void {
  if(value){guards.add(value);const owner=currentEmbedded()?.id;if(owner)guardOwners.set(value,owner);}stable=window.location.href;
  return()=>{if(value){guards.delete(value);guardOwners.delete(value);}};
}
function allowNavigation(target:string,proceed:()=>void,owner?:string):boolean {
  const pending=[...guards].filter(guard=>owner===undefined?true:owner==='*'?guardOwners.has(guard):guardOwners.get(guard)===owner).reverse();
  function check(index:number):boolean {
    for(let i=index;i<pending.length;i++){
      const guard=pending[i]!;
      if(guards.has(guard)&&!guard(target,()=>{if(check(i+1))proceed();}))return false;
    }
    return true;
  }
  return check(0);
}
export function requestEmbeddedChange(owner:string,proceed:()=>void){if(allowNavigation(toAbsoluteUrl(window.location.href),proceed,owner))proceed();}
export function toAbsoluteUrl(target: string | URL): string { return logicalUrl(new URL(target, window.location.origin).href).href; }
export function navigate(target: string | URL, options: NavigateOptions = {}): void {
  const logical = toAbsoluteUrl(target);
  const proceed = () => navigate(target, { ...options, bypassGuard: true });
  if (!options.bypassGuard && !allowNavigation(logical, proceed)) return;
  const next = options.legacy ? new URL(target, window.location.origin).href : destination(target);
  if (options.hard || !isWorkspacePage(target)) { window.location.assign(next); return; }
  if (next === window.location.href) return;
  window.history[options.replace ? 'replaceState' : 'pushState']({}, '', next);
  stable = next;
  window.dispatchEvent(new Event('popstate'));
}
export function navigateHard(target: string | URL) { navigate(target, { hard: true }); }
export function moduleKey(url:URL):string {
  const key=moduleForRoute(url);
  if(NOTICE_IDS.includes(key)||key==='all')return 'notices';
  if(url.pathname.startsWith('/polling-work-order'))return 'work-order';
  if(url.pathname==='/'&&(url.searchParams.get('account')==='password'||['my','permission'].includes(url.searchParams.get('view')||'')))return 'personal';
  return key;
}
export function moduleBackTarget(fallback='/',current=logicalUrl()):string|null {
  const key=moduleKey(current);if(!key)return null;
  let target=logicalUrl(new URL(fallback,current.origin).href);
  if(target.origin!==current.origin||moduleKey(target)!==key){
    const root=key==='notices'?'/workbench-lite':key==='personal'?'/?view=my':key==='sops'?'/?view=sops':key==='work-order'?'/polling-work-order':modules.find(m=>m.id===key)?.path;
    if(!root)return null;target=new URL(root,current.origin);
    if(key==='event')target.searchParams.delete('detail');
  }
  for(const name of ['scope',...(key==='notices'?['work_type','month']:[]),...(key==='work-order'?['token']:[])]){
    if(!target.searchParams.has(name)&&current.searchParams.has(name))target.searchParams.set(name,current.searchParams.get(name)!);
  }
  return target.href===current.href?null:target.pathname+target.search+target.hash;
}
export function navigateBack(fallback = '/', hardFallback = false) {
  const target=moduleBackTarget(fallback);
  if(target)navigate(target, { replace: !hardFallback, hard: hardFallback });
}
export function replaceRoute(target: string | URL) { navigate(target, { replace: true }); }
export function requestPageReload(confirm: (proceed: () => void) => void) {
  const proceed = () => window.location.reload();
  if (!guards.size) confirm(proceed); else if (allowNavigation(toAbsoluteUrl(window.location.href), proceed)) proceed();
}
window.addEventListener('popstate', event => {
  const next = window.location.href;
  if (!guards.size || next === stable) { stable = next; return; }
  if (allowNavigation(toAbsoluteUrl(next), () => navigate(next, { replace: true, bypassGuard: true }))) { stable = next; return; }
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
