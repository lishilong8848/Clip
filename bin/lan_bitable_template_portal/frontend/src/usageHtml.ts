import { knownPage, recordPage, setUsageAccess, startUsage } from './usageEvents';

// The legacy iframe's visible parent owns the visit, including navigation forwarded by the frame.
if (!(window.parent !== window && new URLSearchParams(location.search).get('_assistant_frame') === '1')) {
  let last = '';
  function sync() {
    const {usageModule: module, usagePage: page, usageAccess} = document.body.dataset;
    setUsageAccess(usageAccess === 'member' || usageAccess === 'guest' ? usageAccess : 'public');
    if (!module || !page || !knownPage(module,page)) { last = ''; return; }
    const key = module + '/' + page;
    if (last !== key) {
      last = key;
      recordPage('web_standalone',module,page);
    }
  }
  startUsage();
  document.addEventListener('portal-usage-page',sync);
  window.addEventListener('pageshow',event => { if (event.persisted) { last = ''; sync(); } });
  sync();
}
