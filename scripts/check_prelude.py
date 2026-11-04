"""Reject legacy io module declarations in project Rust sources."""

from pathlib import Path
import re
import subprocess
import sys


root = Path(__file__).resolve().parents[1]
paths = subprocess.check_output(
    ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
    cwd=root,
).decode().split("\0")
legacy_module = re.compile(r"\bmod\s+io\b")
violations = []
for name in sorted(set(paths)):
    path = root / name
    if path.suffix != ".rs" or not path.is_file():
        continue
    source = path.read_text()
    for match in legacy_module.finditer(source):
        line = source.count("\n", 0, match.start()) + 1
        violations.append(f"{name}:{line}: use a prelude module instead of io")

if violations:
    print("\n".join(violations), file=sys.stderr)
    sys.exit(1)
print("Public interface naming check passed")
