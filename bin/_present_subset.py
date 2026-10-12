import os
import subprocess
import sys

tests = open("_lan_subset.txt", encoding="utf-8").read().splitlines()
r = subprocess.run(
    [sys.executable, "-m", "pytest", "--collect-only", "-q", "test_lan_template_work_status.py"],
    capture_output=True,
    text=True,
    cwd=os.path.dirname(os.path.abspath(__file__)),
)
collect = set()
for line in r.stdout.splitlines():
    line = line.strip()
    if line.startswith("test_lan_template_work_status.py::"):
        collect.add(line)
missing = [t for t in tests if t not in collect]
present = [t for t in tests if t in collect]
print("PRESENT count:", len(present))
for t in present:
    print("  P:", t.split("::")[-1])
print("MISSING count:", len(missing))
for t in missing:
    print("  M:", t.split("::")[-1])
with open("_present_subset.txt", "w", encoding="utf-8", newline="\n") as f:
    for t in present:
        f.write(t + "\n")