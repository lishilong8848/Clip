"""Frontend resource generations used by the portable patch installer."""
from pathlib import Path
import json
import posixpath
import re
from urllib.parse import urlsplit

FRONTEND_DIST = Path("bin/lan_bitable_template_portal/frontend/dist")
FRONTEND_INDEX = FRONTEND_DIST / "index.html"
FRONTEND_ASSETS = FRONTEND_DIST / "assets"
_REFERENCES = re.compile(r'''["'`(](?:/?assets/|\./)([^"'`)\s?#,;]+)''')

# V2 workspace generation lives under dist/assets/workspace-v2, driven by an
# entry.json manifest plus per-chunk relative imports.  It is closed over
# separately from the legacy index so installer cleanup keeps V2 builds safe.
V2_DIR = FRONTEND_ASSETS / "workspace-v2"
V2_INDEX = V2_DIR / "index.html"
V2_MANIFEST = V2_DIR / "entry.json"
_SAFE_CHUNK = re.compile(r"^[A-Za-z0-9_-]+\.(?:js|css)$")
_RELATIVE_CHUNK = re.compile(r'''["'`(]((?:\.\.?/)[A-Za-z0-9_\-./]+\.(?:js|css))["'`]''')
# Only explicit rooted "/assets/" refs (or the quoted "assets/" form) are root
# assets; URL path fragments and "./assets/" are never rooted.
_ROOT_ASSET = re.compile(r'''["'`(](?:/?assets/)([^"'`)\s?#,;]+)''')
_CSS_URL = re.compile(r'''url\(\s*(["']?)([^"')]+?)\1\s*\)''', re.IGNORECASE)
_MAP_DEPS = re.compile(r'm\.f=\[([^\]]*)\]')
_MAP_NAMES = re.compile(r'''["'`]([A-Za-z0-9_-]+\.(?:js|css))["'`]''')


def is_frontend_asset(path: Path) -> bool:
    return path.is_relative_to(FRONTEND_DIST / "assets") and path.suffix in {".js", ".css"}


def _unsafe_reference(name: str) -> bool:
    # Absolute/rooted paths, drive refs, and backslashes are never local assets.
    return Path(name).is_absolute() or name.startswith("/") or "\\" in name or ":" in name


def _css_asset_path(path: str) -> str | None:
    """Validate a CSS url() target.  Returns a relative asset path, or None when
    the reference is external/inline (and thus outside the closure), and raises
    for unsafe absolute paths or backslashes."""
    path = path.strip()
    if not path:
        return None
    if "\\" in path:
        raise ValueError(f"Unsafe V2 frontend asset reference: {path}")
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or not parsed.path:
        return None
    path = parsed.path
    if path.startswith("/"):
        if path.startswith("/assets/"):
            return None  # rooted asset, handled by _ROOT_ASSET
        raise ValueError(f"Unsafe V2 frontend asset reference: {path}")
    if ":" in path:
        raise ValueError(f"Unsafe V2 frontend asset reference: {path}")
    return path


def _v2_closure(source, *, strict: bool) -> set[Path]:
    """Close over the V2 workspace generation (index + manifest + chunks)."""
    if not source(V2_INDEX).is_file():
        return set()

    found: set[Path] = {V2_INDEX}
    pending: list[tuple[str, Path, bool]] = []  # (text, dir, is_css)

    def add_reference(base_rel: Path, name: str) -> None:
        name = name.rstrip("/")
        if not name:
            return
        if _unsafe_reference(name):
            raise ValueError(f"Unsafe V2 frontend asset reference: {name}")
        norm = posixpath.normpath((base_rel / name).as_posix())
        rel = Path(norm)
        if not rel.is_relative_to(FRONTEND_ASSETS):
            raise ValueError(f"Unsafe V2 frontend asset reference (escapes assets root): {name}")
        if rel in found:
            return
        asset = source(rel)
        if asset.is_dir():
            return
        found.add(rel)
        if not asset.is_file():
            if strict:
                raise ValueError(f"Missing V2 frontend asset: {rel}")
            return
        if asset.suffix in {".js", ".css"}:
            pending.append((asset.read_text(encoding="utf-8"), rel.parent, asset.suffix == ".css"))

    manifest_asset = source(V2_MANIFEST)
    if not manifest_asset.is_file():
        if strict:
            raise ValueError(f"Missing V2 frontend manifest: {V2_MANIFEST}")
    else:
        found.add(V2_MANIFEST)
        try:
            manifest = json.loads(manifest_asset.read_text(encoding="utf-8"))
        except Exception:
            if strict:
                raise ValueError(f"Invalid V2 frontend manifest: {V2_MANIFEST}") from None
            manifest = None
        if manifest is not None:
            if (
                not isinstance(manifest, dict)
                or not isinstance(manifest.get("entry"), str)
                or not isinstance(manifest.get("css"), list)
                or not all(isinstance(x, str) for x in manifest.get("css", []))
            ):
                if strict:
                    raise ValueError(f"Invalid V2 frontend manifest: {V2_MANIFEST}")
                manifest = None
            else:
                entry = manifest["entry"]
                if not (_SAFE_CHUNK.match(entry) and entry.endswith(".js")):
                    if strict:
                        raise ValueError(f"Invalid V2 frontend manifest: {V2_MANIFEST}")
                    entry = None
                valid_css: list[str] = []
                for css in manifest["css"]:
                    if _SAFE_CHUNK.match(css) and css.endswith(".css"):
                        valid_css.append(css)
                    elif strict:
                        raise ValueError(f"Invalid V2 frontend manifest: {V2_MANIFEST}")
                if entry:
                    add_reference(V2_DIR, entry)
                for css in valid_css:
                    add_reference(V2_DIR, css)

    # Scan the V2 index plus every reachable js/css chunk.  Relative references
    # resolve against the containing directory; rooted assets target assets/.
    pending.append((source(V2_INDEX).read_text(encoding="utf-8"), V2_DIR, False))
    while pending:
        text, base, is_css = pending.pop()
        for name in _ROOT_ASSET.findall(text):
            add_reference(FRONTEND_ASSETS, name)
        for name in _RELATIVE_CHUNK.findall(text):
            add_reference(base, name)
        if is_css:
            for path in _CSS_URL.findall(text):
                if isinstance(path, tuple):
                    path = path[1]
                css_path = _css_asset_path(path)
                if css_path:
                    add_reference(base, css_path)
        for block in _MAP_DEPS.findall(text):
            for name in _MAP_NAMES.findall(block):
                add_reference(base, name)
    return found


def referenced_assets(root: Path, overlay: Path | None = None, *, strict: bool = True) -> set[Path]:
    def source(relative):
        candidate = overlay / relative if overlay is not None else root / relative
        return candidate if candidate.is_file() else root / relative

    found: set[Path] = set()

    # Legacy (V1) index + assistant.html closure - resolve against assets root.
    index = source(FRONTEND_INDEX)
    if index.is_file():
        pending = [index.read_text(encoding="utf-8")]
        widget = FRONTEND_DIST / "assistant.html"
        if source(widget).is_file():
            found.add(widget)
            pending.append(source(widget).read_text(encoding="utf-8"))
        while pending:
            for name in _REFERENCES.findall(pending.pop()):
                relative = FRONTEND_DIST / "assets" / name
                if Path(name).is_absolute() or ".." in Path(name).parts or "\\" in name or ":" in name:
                    raise ValueError("Unsafe frontend asset reference")
                if relative in found:
                    continue
                found.add(relative)
                asset = source(relative)
                if not asset.is_file():
                    if strict:
                        raise ValueError(f"Missing frontend asset: {name}")
                    continue
                if relative.suffix in {".js", ".css"}:
                    pending.append(asset.read_text(encoding="utf-8"))

    # V2 workspace generation closure.
    found |= _v2_closure(source, strict=strict)
    return found


def patch_deletions(root: Path, overlay: Path, requested: list[Path]) -> list[Path]:
    # An unchanged entry can still be in use; only rotate on an entry update.
    keep = referenced_assets(root, strict=False)
    entry_update = (
        (overlay / FRONTEND_INDEX).is_file()
        or (overlay / V2_INDEX).is_file()
        or (overlay / V2_MANIFEST).is_file()
    )
    if not entry_update:
        return [path for path in requested if not is_frontend_asset(path)]
    keep |= referenced_assets(root, overlay)
    stale = {
        path.relative_to(root)
        for path in (root / FRONTEND_DIST / "assets").rglob("*")
        if path.is_file() and is_frontend_asset(path.relative_to(root))
        and path.relative_to(root) not in keep
    }
    return sorted(({path for path in requested if path not in keep and not is_frontend_asset(path)} | stale), key=str)
