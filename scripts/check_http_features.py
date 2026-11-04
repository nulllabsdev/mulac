"""Compile the independent core feature matrix and reject dependency leakage."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = str(ROOT / "libs/Cargo.toml")
CORES = ("eventing", "commanding", "inbox", "outbox")
FEATURES = (None, "", "api", "api-tokio", "api-testing", "axum", "axum-openapi", "poem", "axum,poem", "axum-openapi,poem")
for core in CORES:
    for features in FEATURES:
        args = ["--manifest-path", MANIFEST, "-p", f"mulac-{core}"]
        if features is not None:
            args += ["--no-default-features"]
        if features:
            args += ["--features", features]
        print(f"Checking {core}: {features if features is not None else 'defaults'}", flush=True)
        subprocess.run(["cargo", "check", "--lib", *args], check=True)
        tree = subprocess.check_output(["cargo", "tree", "--edges", "normal", "--prefix", "none", *args], text=True)
        packages = {line.split()[0] for line in tree.splitlines() if line.strip()}
        forbidden = {"lapin"}
        if features is not None:
            forbidden |= {"diesel", "mulac_diesel"}
        enabled = set((features or "").split(","))
        if not enabled & {"axum", "axum-openapi"}:
            forbidden.add("axum")
        if "poem" not in enabled:
            forbidden |= {"poem", "poem-openapi"}
        if "axum-openapi" not in enabled:
            forbidden.add("utoipa")
        if not enabled & {"api-tokio", "axum", "axum-openapi", "poem"}:
            forbidden.add("tokio")
        assert not packages & forbidden, (core, features, packages & forbidden)

for feature in ("", "axum", "axum-openapi", "poem", "axum-openapi,poem"):
    args = ["--manifest-path", MANIFEST, "-p", "mulac_http", "--no-default-features"]
    if feature:
        args += ["--features", feature]
    subprocess.run(["cargo", "check", "--lib", *args], check=True)
    tree = subprocess.check_output(["cargo", "tree", "--edges", "normal", "--prefix", "none", *args], text=True)
    assert not any(f"mulac-{core} " in tree for core in CORES)
    assert "diesel v" not in tree and "lapin v" not in tree
print("HTTP feature isolation checks passed")

for core in ("inbox", "outbox"):
    for features in ("amqp", "amqp,axum,poem"):
        subprocess.run(["cargo", "check", "--lib", "--manifest-path", MANIFEST,
                        "-p", f"mulac-{core}", "--no-default-features", "--features", features], check=True)
print("AMQP feature combinations passed")
