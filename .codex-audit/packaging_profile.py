"""Read-only packaging scan profile; does not build or publish a patch."""
import cProfile
import ctypes
import io
import json
import pstats
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ctypes.windll.kernel32.SetPriorityClass(ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)
import package_portable as package

profile = cProfile.Profile()
profile.enable()
for label, root in (("source", ROOT), ("baseline", ROOT / "build_output/ClipFlow_V2")):
    started = time.monotonic()
    paths = package._iter_project_files(root, exclude_venv=True)
    print(json.dumps({"stage": label, "files": len(paths), "seconds": round(time.monotonic() - started, 3)}), flush=True)
profile.disable()
out = io.StringIO()
pstats.Stats(profile, stream=out).sort_stats("cumulative").print_stats(25)
print(out.getvalue(), flush=True)
