#!/usr/bin/env python3
"""Package into a static registry and optionally push its GitHub Pages deployment."""
import argparse
import functools
import hashlib
import http.server
import json
import os
import shutil
from pathlib import Path
import subprocess
import threading
import tomllib

ROOT = Path(__file__).resolve().parents[1]
# Each dependency must be added before Cargo resolves the next package.
PACKAGES = [
    "libs/mulac_http", "libs/mulac_diesel", "libs/eventing", "libs/commanding",
    "libs/outbox", "libs/inbox", "kernel",
]


def index_path(name):
    name = name.lower()
    if len(name) <= 2:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def existing_checksum(registry, name, version):
    path = registry / index_path(name)
    if path.exists():
        for line in path.read_text().splitlines():
            entry = json.loads(line)
            if entry["vers"] == version:
                return entry["cksum"]
    return None


def add_archive(registry, archive, name, version, margo):
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    existing = existing_checksum(registry, name, version)
    if existing is not None and existing != checksum:
        raise RuntimeError(f"{name} {version} already exists with different bytes; bump its version")
    if existing is None:
        subprocess.run([margo, "add", "--registry", str(registry), str(archive)], check=True)
    stored = registry / "crates" / index_path(name) / f"{version}.crate"
    if (existing_checksum(registry, name, version) != checksum or not stored.is_file()
            or hashlib.sha256(stored.read_bytes()).hexdigest() != checksum):
        raise RuntimeError(f"Registry verification failed for {name} {version}")
    print(f"Verified {name} {version}", flush=True)


class RegistryHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/config.json":
            # Resolve unpublished dependencies locally without rewriting hosted metadata.
            data = json.dumps({"dl": f"http://127.0.0.1:{self.server.server_port}/crates/"
                              "{lowerprefix}/{crate}/{version}.crate",
                              "api": None, "auth-required": False}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            super().do_GET()

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--margo", default="margo")
    parser.add_argument("--deploy", action="store_true", help="commit registry artifacts and push main for GitHub Pages")
    parser.add_argument("--push-url", default="origin", help="registry Git remote or HTTPS URL")
    args = parser.parse_args()
    registry = args.registry.expanduser().resolve()
    for filename in ("config.json", "margo-config.toml"):
        if not (registry / filename).is_file():
            parser.error(f"Missing {registry / filename}; use an initialized margo registry")
    if args.deploy:
        branch = subprocess.check_output(["git", "-C", str(registry), "branch", "--show-current"], text=True).strip()
        if branch != "main":
            parser.error("Deployment requires the registry checkout on main")
        if subprocess.check_output(["git", "-C", str(registry), "diff", "--cached", "--name-only"], text=True).strip():
            parser.error("Registry has staged changes; commit or unstage them before deploying")
    artifacts = ["index.html", "assets"]
    workspace = tomllib.loads((ROOT / "libs/Cargo.toml").read_text())
    config = tomllib.loads((ROOT / ".cargo/config.toml").read_text())
    index = config["registries"]["margo"]["index"]
    handler = functools.partial(RegistryHandler, directory=str(registry))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        for directory in PACKAGES:
            manifest = ROOT / directory / "Cargo.toml"
            package = tomllib.loads(manifest.read_text())["package"]
            version = package["version"]
            if isinstance(version, dict):
                version = workspace["workspace"]["package"]["version"]
            name = package["name"]
            target = ROOT / ("libs" if directory.startswith("libs/") else "kernel") / "target"
            env = dict(os.environ, CARGO_TARGET_DIR=str(target))
            subprocess.run([
                "cargo", "package", "--no-verify", "--manifest-path", str(manifest),
                "--config", f'source.margo.registry="{index}"',
                "--config", 'source.margo.replace-with="publish-local"',
                "--config", f'source.publish-local.registry="sparse+http://127.0.0.1:{server.server_port}/"',
            ], cwd=ROOT, env=env, check=True)
            archive = target / "package" / f"{name}-{version}.crate"
            add_archive(registry, archive, name, version, args.margo)
            artifacts.extend([index_path(name), f"crates/{index_path(name)}/{version}.crate"])
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    if args.deploy:
        git = ["git", "-C", str(registry)]
        subprocess.run([*git, "add", "--", *artifacts], check=True)
        changed = subprocess.run([*git, "diff", "--cached", "--quiet"], check=False)
        if changed.returncode == 1:
            subprocess.run([*git, "commit", "-m", "chore(registry): publish mulac crates"], check=True)
        elif changed.returncode != 0:
            raise RuntimeError("Could not inspect staged registry changes")
        # Reuse an authenticated GitHub CLI for HTTPS without changing remotes.
        if args.push_url.startswith("https://github.com/") and shutil.which("gh"):
            git += ["-c", "credential.helper=!gh auth git-credential"]
        subprocess.run([*git, "push", args.push_url, "main"], check=True)
        print("Registry pushed. GitHub Pages must finish deploying before new crates are live.")
    else:
        print(f"All packages verified in {registry}. Commit and push that checkout to deploy.")


if __name__ == "__main__":
    main()
