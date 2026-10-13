export const WORKSPACE_ENTRY = '/assets/workspace-v2/index.html';
export type FrontendVersion = 'legacy' | 'workspace';
const prefix = 'clipflow:frontend-version:';
let preparing:Promise<void>|undefined;
export function prepareWorkspaceEntry():Promise<void>{
  if(preparing)return preparing;
  preparing=(async()=>{
    const base=WORKSPACE_ENTRY.slice(0,WORKSPACE_ENTRY.lastIndexOf('/')+1);
    const {requestJson}=await import('../api/client');
    const manifest=await requestJson(base+'entry.json',{cache:'no-store',timeoutMs:5000});
    const files=[manifest.entry,...(Array.isArray(manifest.css)?manifest.css:[])];
    for(const name of files){
      if(typeof name!=='string'||!/^[a-zA-Z0-9_-]+\.(js|css)$/.test(name))continue;
      if(document.querySelector(`link[data-workspace-preload="${name}"]`))continue;
      const link=document.createElement('link');link.dataset.workspacePreload=name;link.href=base+name;
      link.rel=name.endsWith('.js')?'modulepreload':'preload';if(name.endsWith('.css'))link.as='style';
      document.head.append(link);
    }
  })().catch(()=>{preparing=undefined;});
  return preparing;
}

export function readVersion(identity: string): FrontendVersion {
  if (!identity) return 'legacy';
  try { return localStorage.getItem(prefix + identity) === 'workspace' ? 'workspace' : 'legacy'; }
  catch { return 'legacy'; }
}
export function saveVersion(identity: string, version: FrontendVersion): void {
  if (!identity) return;
  try { localStorage.setItem(prefix + identity, version); } catch { /* A blocked preference store must not block navigation. */ }
}
export function workspaceUrl(target = '/', origin = window.location.origin): string {
  const original = new URL(target, origin);
  if (original.origin !== origin || original.pathname.startsWith('/api/')) return original.href;
  if (original.pathname === WORKSPACE_ENTRY) return original.href;
  const next = new URL(WORKSPACE_ENTRY, origin);
  next.search = original.search;
  next.searchParams.set('__page', original.pathname);
  next.hash = original.hash;
  return next.href;
}

export function logicalUrl(value = window.location.href): URL {
  const url = new URL(value, window.location.origin);
  if (url.origin === window.location.origin && url.pathname === WORKSPACE_ENTRY) {
    const path = url.searchParams.get('__page') || '/';
    url.pathname = /^\/(?!\/)[^?#\\]*$/.test(path) ? path : '/';
    url.searchParams.delete('__page');
    if(url.pathname==='/'&&url.searchParams.get('mode')==='events'&&!url.searchParams.has('detail'))url.searchParams.set('detail','1');
  }
  return url;
}

export function loginVersionUrl(version: FrontendVersion): string {
  const current = logicalUrl();
  const selected = document.querySelector('.login-tabs [aria-selected="true"]')?.id.replace('login-tab-', '');
  if (selected && ['feishu', 'password', 'guest'].includes(selected)) current.searchParams.set('login', selected);
  current.searchParams.set('frontend', version);
  const fallback = new URL(current);
  for (const key of ['login', 'next', 'auth_notice']) fallback.searchParams.delete(key);
  const next = current.searchParams.get('next') || fallback.href;
  if (next) {
    // Keep the business destination in the chosen UI, never redirect outside the portal.
    try {
      const target = logicalUrl(new URL(next, current.origin).href);
      if (target.origin !== current.origin || target.pathname.startsWith('/api/')) throw new Error('Invalid login return');
      target.searchParams.set('frontend', version);
      const mapped = new URL(version === 'workspace' ? workspaceUrl(target.href) : target.href);
      current.searchParams.set('next', mapped.pathname + mapped.search + mapped.hash);
    } catch { current.searchParams.delete('next'); }
  }
  return version === 'workspace' ? workspaceUrl(current.href) : current.href;
}
