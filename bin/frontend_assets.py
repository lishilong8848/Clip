"""Frontend resource generations used by the portable patch installer."""
from pathlib import Path
import re

FRONTEND_DIST = Path("bin/lan_bitable_template_portal/frontend/dist")
FRONTEND_INDEX = FRONTEND_DIST / "index.html"
_REFERENCES = re.compile(r'''["'`(](?:/?assets/|\./)([^"'`)\s?#,;]+)''')


def is_frontend_asset(path: Path) -> bool:
    return path.is_relative_to(FRONTEND_DIST / "assets") and path.suffix in {".js", ".css"}


def referenced_assets(root: Path, overlay: Path | None = None, *, strict: bool = True) -> set[Path]:
    def source(relative):
        candidate = overlay / relative if overlay is not None else root / relative
        return candidate if candidate.is_file() else root / relative

    index = source(FRONTEND_INDEX)
    if not index.is_file():
        return set()
    pending = [index.read_text(encoding="utf-8")]
    found: set[Path] = set()
    while pending:
        for name in _REFERENCES.findall(pending.pop()):
            relative = FRONTEND_DIST / "assets" / name
            if Path(name).is_absolute() or ".." in Path(name).parts or "\\" in name or ":" in name:
                raise ValueError("Unsafe frontend asset reference")
            if relative.suffix not in {".js", ".css"} or relative in found:
                continue
            found.add(relative)
            asset = source(relative)
            if not asset.is_file():
                if strict:
                    raise ValueError(f"Missing frontend asset: {name}")
                continue
            pending.append(asset.read_text(encoding="utf-8"))
    return found


def patch_deletions(root: Path, overlay: Path, requested: list[Path]) -> list[Path]:
    # An unchanged entry can still be in use; only rotate on an entry update.
    keep = referenced_assets(root, strict=False)
    if not (overlay / FRONTEND_INDEX).is_file():
        return [path for path in requested if not is_frontend_asset(path)]
    keep |= referenced_assets(root, overlay)
    stale = {
        path.relative_to(root)
        for path in (root / FRONTEND_DIST / "assets").rglob("*")
        if path.is_file() and is_frontend_asset(path.relative_to(root))
        and path.relative_to(root) not in keep
    }
    return sorted(({path for path in requested if path not in keep and not is_frontend_asset(path)} | stale), key=str)
