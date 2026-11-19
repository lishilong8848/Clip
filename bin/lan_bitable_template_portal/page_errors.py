"""UTF-8 error pages for direct browser navigation, including Feishu's web view."""
from html import escape

from starlette.responses import HTMLResponse


def page_error(status, message):
    temporary = status in {429, 500, 502, 503, 504}
    title = "请稍后重试" if temporary else "暂时无法打开页面"
    retry = '<a href="">重试</a>' if temporary else ""
    return HTMLResponse(
        f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>{title}</title><style>
        body{{margin:0;padding:12vh 20px 24px;background:#f3f6fb;color:#182846;
        font:16px/1.6 system-ui,"Microsoft YaHei",sans-serif}}
        main{{max-width:440px;margin:auto;padding:24px;border:1px solid #dbe4f0;
        border-radius:12px;background:#fff}}h1{{margin:0;font-size:22px}}
        p{{margin:12px 0 20px;overflow-wrap:anywhere}}
        nav{{display:flex;flex-wrap:wrap;gap:10px}}
        a{{display:inline-grid;min-height:44px;place-items:center;padding:0 14px;
        border:1px solid #b6c9e8;border-radius:6px;color:#17489c;text-decoration:none}}
        </style></head><body><main><h1>{title}</h1><p>{escape(message)}</p>
        <nav>{retry}<a href="/">返回灯塔</a></nav></main></body></html>""",
        status_code=status, headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )
