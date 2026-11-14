#!/usr/bin/env python3
"""Check a prebuilt console image using a disposable, isolated Compose project."""
import json
import os
from pathlib import Path
import subprocess
import uuid
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "indexer-smoke-" + uuid.uuid4().hex[:12]
PORT = os.environ.get("INDEXER_CONSOLE_PORT", "8765")


def compose(*args):
    return subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "compose.yaml"), "-p", PROJECT, *args],
        cwd=ROOT, env={**os.environ, "INDEXER_CONSOLE_PORT": PORT},
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=120, check=True,
    ).stdout


def snapshot():
    code = '''import hashlib,json,os,shutil
from pathlib import Path
p=Path('/app/data/session')
assert os.geteuid()==10001
assert all(shutil.which(tool) is None for tool in ['cargo','rustc','gcc','cc'])
assert 'HELIUS_API_KEY' not in os.environ
assert not Path('/app/.env').exists() and not Path('/app/.git').exists()
assert p.joinpath('persistence-check').read_text()=='survives'
print(json.dumps({'receipt_mtime':p.joinpath('session.json').stat().st_mtime_ns,'binary_sha256':hashlib.sha256(p.joinpath('helius-indexer').read_bytes()).hexdigest()}))'''
    return json.loads(compose("exec", "-T", "console", "python3", "-c", code))


def get(path):
    with urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=10) as response:
        return json.load(response)


def main():
    host = os.environ.get("DOCKER_HOST")
    if not host:
        host = subprocess.check_output(["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], text=True).strip()
    if not host.startswith(("unix://", "npipe://")):
        raise RuntimeError("Smoke tests require a local Docker engine")
    try:
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
        health = get("/healthz")
        assert health == {"status": "ready", "mode": "captured data", "coverage": "unknown"}
        initial = get("/api/transfers")
        assert len(initial["transfers"]) == 303 and initial["coverage"] == "unknown"
        compose("exec", "-T", "console", "python3", "-c", "from pathlib import Path; Path('/app/data/session/persistence-check').write_text('survives')")
        before = snapshot()
        compose("stop")
        container = compose("ps", "--all", "--quiet", "console").strip()
        stopped = subprocess.check_output(["docker", "inspect", "--format", "{{.State.ExitCode}} {{.State.OOMKilled}}", container], text=True).strip()
        assert stopped == "0 false", "Console did not stop gracefully"
        compose("down")
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
        assert snapshot() == before, "Restart replaced the session receipt or binary"
        assert get("/api/transfers") == initial, "Restart changed transfer output"
        print("Docker smoke passed: non-root, no runtime Cargo, health, graceful stop, persistent session and identical queries")
        subprocess.run(["docker", "run", "--rm", "--network", "none", "helius-indexer-console:local", "--seed-only"],
                       check=True, timeout=120, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Network-disabled seed/replay passed")
    finally:
        # Only the randomly named smoke project's disposable volume is removed.
        compose("down", "--volumes")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr or error.stdout or "Docker command failed")
        raise
