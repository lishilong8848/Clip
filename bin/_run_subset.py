import os
import subprocess
import sys

args = [
    sys.executable, "-m", "pytest",
    "-p", "no:cacheprovider", "--tb=short", "--no-header", "-q",
]
args += open("_present_subset.txt", encoding="utf-8").read().splitlines()
r = subprocess.run(
    args,
    capture_output=True,
    text=True,
    cwd=os.path.dirname(os.path.abspath(__file__)),
)
sys.stdout.write(r.stdout)
sys.stdout.write(r.stderr)