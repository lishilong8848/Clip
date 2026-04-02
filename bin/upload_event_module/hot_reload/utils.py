from pathlib import Path


def find_python_root(path: Path) -> Path | None:
    for parent in path.parents:
        if parent.name == "bin":
            return parent
    return None


def path_to_module(path: Path) -> str | None:
    path = path.resolve()
    py_root = find_python_root(path)
    if not py_root:
        return None
    try:
        rel = path.relative_to(py_root)
    except ValueError:
        return None
    if rel.suffix != ".py":
        return None
    if rel.name == "__init__.py":
        rel = rel.parent
    module_name = ".".join(rel.with_suffix("").parts)
    return module_name
