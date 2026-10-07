"""Account-gated Lighthouse widget injection for the portal HTML.

Exposes a single pure function :func:`inject_lighthouse_widget`.  It mounts the
Lighthouse assistant widget before ``</body>`` only when the session is formally
authenticated and the local Vite ``assistant.html`` chunk yields at least one
safe, existing, non-empty module ``.js`` asset.  Any suspicious or missing
resource and any non-dict ``session.user`` leaves the announcement page byte-for-byte
untouched.
"""

from __future__ import annotations

import html as _html
import re
from html.parser import HTMLParser
from pathlib import Path

WIDGET_ID = "clipflow-lighthouse-widget"
ASSET_PREFIX = "/assets/"
_BASENAME = re.compile(r"^[A-Za-z0-9_.-]+$")
_WIDGET_ID_RE = re.compile(r'id\s*=\s*["\']clipflow-lighthouse-widget["\']', re.IGNORECASE)
_BODY_CLOSE_RE = re.compile(r"</body\s*>", re.IGNORECASE)


class _ArtifactParser(HTMLParser):
    """Collect module-script srcs and stylesheet hrefs from assistant.html."""

    def __init__(self):
        super().__init__()
        self.scripts: list[str] = []
        self.stylesheets: list[str] = []
        self.valid = False

    def _tag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "html":
            self.valid = True
        elif tag == "script":
            if attrs.get("type", "").strip().lower() == "module" and attrs.get("src"):
                self.scripts.append(attrs["src"])
        elif tag == "link":
            if "stylesheet" in {t.strip().lower() for t in attrs.get("rel", "").split()} and attrs.get("href"):
                self.stylesheets.append(attrs["href"])

    def handle_starttag(self, tag, attrs):
        self._tag(tag, attrs)

    def handle_startendtag(self, tag, attrs):
        self._tag(tag, attrs)

    def handle_decl(self, decl):
        if decl.strip().lower().startswith("doctype html"):
            self.valid = True


def _basename(src) -> str | None:
    """Return a plain asset basename or ``None`` when the reference is unsafe."""
    if not isinstance(src, str) or not src.startswith(ASSET_PREFIX):
        return None
    name = src[len(ASSET_PREFIX):]
    if not name or "/" in name or "\\" in name or ".." in name:
        return None
    if not _BASENAME.fullmatch(name):
        return None
    return name


def _read_assets(widget_index_path) -> dict | None:
    """Extract existing, non-empty, safe asset refs from assistant.html."""
    if not widget_index_path:
        return None
    try:
        base_dir = Path(widget_index_path).resolve().parent
    except (OSError, ValueError):
        return None
    try:
        text = (base_dir / "assistant.html").read_text(encoding="utf-8")
    except (OSError, UnicodeError, ValueError):
        return None

    parser = _ArtifactParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception:
        return None
    if not parser.valid:
        return None

    scripts, styles = [], []
    for ref in parser.scripts:
        name = _basename(ref)
        if name is None or name in scripts:
            if name is None:
                return None
            continue
        scripts.append(name)
    for ref in parser.stylesheets:
        name = _basename(ref)
        if name is None:
            return None
        if name not in styles:
            styles.append(name)

    assets_dir = base_dir / "assets"
    if not assets_dir.is_dir():
        return None
    for name in scripts + styles:
        try:
            if not (assets_dir / name).is_file():
                return None
        except (OSError, ValueError):
            return None
    for name in scripts:
        try:
            if (assets_dir / name).stat().st_size == 0:
                return None
        except (OSError, ValueError):
            return None
    if not any(name.endswith(".js") for name in scripts):
        return None

    return {
        "scripts": [ASSET_PREFIX + name for name in scripts],
        "stylesheets": [ASSET_PREFIX + name for name in styles],
    }


def _session_identity(session) -> tuple | None:
    """Return ``(user_id, user_name)`` for a formally logged-in account."""
    if not isinstance(session, dict) or not session or session.get("is_guest"):
        return None
    user = session.get("user")
    if user is None:
        user = {}
    elif not isinstance(user, dict):
        return None
    if str(session.get("role") or "").strip().lower() == "guest" or str(user.get("role") or "").strip().lower() == "guest":
        return None
    user_id = str(session.get("open_id") or user.get("open_id") or "").strip()
    if not user_id:
        return None
    user_name = str(session.get("name") or user.get("name") or "").strip()
    return user_id, user_name


def inject_lighthouse_widget(html_text, session, widget_index_path) -> str:
    """Mount the widget before ``</body>`` or return ``html_text`` untouched."""
    if not isinstance(html_text, str):
        return html_text
    identity = _session_identity(session)
    if identity is None or _WIDGET_ID_RE.search(html_text):
        return html_text
    refs = _read_assets(widget_index_path)
    if refs is None:
        return html_text

    user_id, user_name = identity
    block = (
        f'<div id="{WIDGET_ID}" '
        f'data-user-id="{_html.escape(user_id, quote=True)}" '
        f'data-user-name="{_html.escape(user_name, quote=True)}"></div>'
        + "".join(
            f'<link rel="stylesheet" href="{_html.escape(h, quote=True)}">' for h in refs["stylesheets"]
        )
        + "".join(
            f'<script type="module" src="{_html.escape(s, quote=True)}"></script>' for s in refs["scripts"]
        )
    )
    match = _BODY_CLOSE_RE.search(html_text)
    if match is None:
        return html_text + block
    return html_text[: match.start()] + block + html_text[match.start():]