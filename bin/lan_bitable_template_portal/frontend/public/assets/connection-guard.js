/* Shared by Vue, notice editing and LAN work-order pages. No business retries. */
(() => {
  if (window.ClipFlowConnectionGuard) return;
  let checking = false, failures = 0, instance = '', blocked = false, timer = 0;
  const style = document.createElement('style');
  style.textContent = `html.clipflow-connection-lost body>*:not(#clipflow-connection-guard){display:none!important}
    #clipflow-connection-guard{position:fixed;inset:0;z-index:2147483647;display:grid;place-items:center;background:linear-gradient(145deg,#edf4ff,#fff);color:#213b5d;font:15px 'Microsoft YaHei',Arial,sans-serif}
    #clipflow-connection-guard section{max-width:580px;margin:24px;padding:40px;border:1px solid #d9e5f4;border-radius:20px;background:#fff;box-shadow:0 18px 60px #0b438514;line-height:1.8}
    #clipflow-connection-guard h1{font-size:25px;margin:10px 0;color:#a92d42}#clipflow-connection-guard p{color:#637b98}
    #clipflow-connection-guard button{font:inherit;margin:12px 10px 0 0;padding:11px 22px;border-radius:10px;border:1px solid #cbdcf4;color:#255480;background:white;cursor:pointer}
    #clipflow-connection-guard button:first-of-type{background:#1763d8;color:white;border-color:#1763d8}
    #clipflow-connection-guard button:focus-visible{outline:3px solid #8ab7fa;outline-offset:3px}`;
  document.head.append(style);
  function block(reason) {
    blocked = true;
    document.documentElement.classList.add('clipflow-connection-lost');
    let panel = document.getElementById('clipflow-connection-guard');
    if (!panel) {
      panel = document.createElement('div'); panel.id = 'clipflow-connection-guard';
      panel.setAttribute('role', 'alertdialog'); panel.setAttribute('aria-modal', 'true');
      panel.setAttribute('aria-labelledby', 'clipflow-connection-title');
      panel.innerHTML = '<section><span>南通基地 · 运维灯塔工作台</span><h1 id="clipflow-connection-title">页面连接已中断</h1><p data-connection-reason></p><p>业务页面已暂停显示。恢复连接后请刷新页面；此前正在提交的通告，请先核对处理结果，不要重复发送。</p><button type="button" data-reload>刷新页面</button><button type="button" data-check>重新检测连接</button><p data-connection-status role="status"></p></section>';
      document.body.append(panel);
      panel.querySelector('[data-reload]').onclick = () => location.reload();
      panel.querySelector('[data-check]').onclick = () => check();
      panel.querySelector('[data-reload]').focus();
    }
    panel.querySelector('[data-connection-reason]').textContent = reason;
  }
  async function check() {
    if (checking) return;
    clearTimeout(timer);
    checking = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    try {
      const response = await fetch('/api/health?probe=1', { cache: 'no-store', credentials: 'same-origin', signal: controller.signal });
      const data = await response.json();
      if (!response.ok || !data.ok || data.service !== 'clipflow_backend') throw new Error('backend unavailable');
      failures = 0;
      if (instance && data.instance_id && instance !== data.instance_id) block('当前程序已重启，原页面状态可能已失效，请刷新后继续。');
      if (!instance) instance = data.instance_id || '';
      if (blocked) document.querySelector('#clipflow-connection-guard [data-connection-status]').textContent = '连接已恢复，请点击“刷新页面”重新加载。';
    } catch (_) {
      failures += 1;
      if (failures >= 2 || blocked) {
        block('无法连接当前程序。请检查当前程序是否运行、电脑网络及页面访问地址。');
        document.querySelector('#clipflow-connection-guard [data-connection-status]').textContent = '连接尚未恢复。';
      }
    } finally {
      clearTimeout(timeout); checking = false;
      timer = setTimeout(check, failures === 1 ? 1000 : document.hidden ? 30000 : 10000);
    }
  }
  window.ClipFlowConnectionGuard = { check };
  window.addEventListener('offline', check);
  window.addEventListener('online', check);
  window.addEventListener('pageshow', check);
  window.addEventListener('clipflow-api-offline', check);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) check(); });
  window.addEventListener('pagehide', () => clearTimeout(timer));
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', check, { once: true });
  else check();
})();
