import DOMPurify from 'dompurify';
import { marked } from 'marked';

export function renderLighthouseReply(text: string): string {
  const fragment = DOMPurify.sanitize(marked.parse(text, { async: false, gfm: true, breaks: true }), {
    ALLOWED_TAGS: ['p', 'br', 'strong', 'b', 'em', 'i', 'del', 's', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'blockquote', 'pre', 'code', 'hr', 'table', 'thead', 'tbody', 'tr', 'th', 'td', 'span', 'font', 'a'],
    ALLOWED_ATTR: ['href', 'title', 'style', 'color', 'start'],
    ALLOW_DATA_ATTR: false,
    ALLOW_ARIA_ATTR: false,
    RETURN_DOM_FRAGMENT: true,
  });
  for (const node of fragment.querySelectorAll<HTMLElement>('*')) {
    const color = node.style.color || node.getAttribute('color') || '';
    node.removeAttribute('style');
    node.removeAttribute('color');
    // Only text color survives; no positioning, background URLs or custom CSS.
    if (/^(?:#[\da-f]{3,8}|[a-z]+|(?:rgb|hsl)a?\([\d.,% /+-]+\))$/i.test(color) && CSS.supports('color', color)) node.style.color = color;
  }
  for (const link of fragment.querySelectorAll<HTMLAnchorElement>('a')) {
    const href = link.getAttribute('href') || '';
    try {
      let decoded = href;
      for (let n = 0; n < 3 && /%[\da-f]{2}/i.test(decoded); n++) decoded = decodeURIComponent(decoded);
      if (!decoded || /[\\\u0000-\u001f\u007f]/.test(decoded) || decoded.trimStart().startsWith('//')) throw new Error();
      const target = new URL(decoded, window.location.origin);
      if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password) throw new Error();
      if (target.origin === window.location.origin && /^\/api(?:\/|$)/i.test(target.pathname) && !/^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(target.pathname)) throw new Error();
      const original = new URL(href, window.location.origin);
      link.setAttribute('href', target.origin === window.location.origin ? original.pathname + original.search + original.hash : original.href);
      if (target.origin !== window.location.origin || target.pathname.startsWith('/api/assistant/files/')) {
        link.setAttribute('target', '_blank');
        link.setAttribute('rel', 'noopener noreferrer');
      }
    } catch {
      link.removeAttribute('href');
    }
  }
  const container = document.createElement('div');
  container.append(fragment);
  return container.innerHTML;
}
