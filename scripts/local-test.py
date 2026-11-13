#!/usr/bin/env python3
"""An offline, loopback-only captured-data console. No provider credentials required."""
import argparse
import errno
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent.parent
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
MINT = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}\Z")


def valid_mint(value):
    if not MINT.fullmatch(value):
        return False
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = 0
    for char in value:
        number = number * 58 + alphabet.index(char)
    return len(value) - len(value.lstrip("1")) + (number.bit_length() + 7) // 8 == 32


def command(session, *args):
    with (session / "events.jsonl").open("ab") as log:
        result = subprocess.run(
            [str(session / "helius-indexer"), *map(str, args)],
            stdout=subprocess.PIPE, stderr=log, timeout=30, check=False,
        )
    if result.returncode:
        raise RuntimeError("Indexer command failed; inspect the session events.jsonl")
    return json.loads(result.stdout)


def captures():
    fixtures = ROOT / "tests" / "fixtures"
    yield json.loads((fixtures / "mainnet-transfer.json").read_text())
    for filename in ["mainnet-coverage.json", "mainnet-maintenance.json"]:
        for case in json.loads((fixtures / filename).read_text()):
            yield case["capture"]


def seed(session, binary):
    shutil.copyfile(binary, session / "helius-indexer")
    (session / "helius-indexer").chmod(0o700)
    inputs = session / "captures"
    inputs.mkdir()
    database = session / "index.sqlite"
    files = []
    for index, capture in enumerate(captures()):
        path = inputs / f"{index:03d}.json"
        path.write_text(json.dumps(capture))
        files.append(path)
        command(session, "ingest", database, path)
    before = command(session, "query", database)
    if len(before["transfers"]) >= before["limit"]:
        raise RuntimeError("Demo reached the query limit; choose a smaller seed corpus")
    for path in files:
        result = command(session, "ingest", database, path)
        if result["inserted"] != 0:
            raise RuntimeError("Fixture replay unexpectedly inserted a transfer")
    after = command(session, "query", database)
    if before != after or after["coverage"] != "unknown":
        raise RuntimeError("Replay changed query results or coverage disclosure")
    mints = sorted({row["mint"] for row in after["transfers"]})
    metadata = {
        "mode": "captured data", "coverage": "unknown", "fixture_count": len(files),
        "transfer_count": len(after["transfers"]), "mints": mints,
        "query_limit": after["limit"], "replay_verified": True,
        "slot_start": min(row["slot"] for row in after["transfers"]),
        "slot_end": max(row["slot"] for row in after["transfers"]),
    }
    (session / "session.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def handler_for(session, port):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, *_args):
            # Do not persist arbitrary request URLs, queries, headers or bodies.
            pass

        def send(self, status, content, kind="application/json"):
            data = content if isinstance(content, bytes) else json.dumps(content).encode()
            self.send_response(status)
            self.send_header("Content-Type", kind + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.headers.get("Host") not in {f"127.0.0.1:{port}", f"localhost:{port}"}:
                self.send(403, {"error": "Use the local console URL"})
                return
            if len(self.path) > 2048:
                self.send(414, {"error": "Request too long"})
                return
            url = urlsplit(self.path)
            if url.path == "/":
                self.send(200, (ROOT / "scripts/local-console/index.html").read_bytes(), "text/html")
            elif url.path == "/api/session":
                self.send(200, json.loads((session / "session.json").read_text()))
            elif url.path == "/api/transfers":
                try:
                    query = parse_qs(url.query, keep_blank_values=True, max_num_fields=4)
                except ValueError:
                    self.send(400, {"error": "Too many query fields"})
                    return
                mint = query.get("mint", [""])[0]
                if (mint and not valid_mint(mint)) or set(query) - {"mint"} or len(query.get("mint", [])) > 1:
                    self.send(400, {"error": "Use one valid mint address"})
                    return
                try:
                    args = ["query", session / "index.sqlite"] + ([mint] if mint else [])
                    self.send(200, command(session, *args))
                except (RuntimeError, subprocess.TimeoutExpired, ValueError):
                    self.send(503, {"error": "Query failed; inspect session events.jsonl"})
            else:
                self.send(404, {"error": "Not found"})

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--session", type=Path, help="Reopen an existing local test session")
    parser.add_argument("--binary", type=Path, help="Use an existing build instead of building")
    parser.add_argument("--seed-only", action="store_true", help="Create/check a session without starting HTTP")
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("port must be between 1024 and 65535")
    if args.session:
        session = args.session.resolve()
        if not all((session / name).is_file() for name in ["session.json", "index.sqlite", "helius-indexer"]):
            parser.error("session must contain a seeded local test database and binary")
        metadata = json.loads((session / "session.json").read_text())
    else:
        binary = args.binary.resolve() if args.binary else ROOT / "target/debug/helius-indexer"
        if not args.binary:
            print("Building the local CLI (offline, locked dependencies)…", flush=True)
            subprocess.run(["cargo", "build", "--offline", "--locked"], cwd=ROOT, check=True,
                           env={**os.environ, "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_INCREMENTAL": "0"})
        if not binary.is_file():
            parser.error("binary not found")
        base = ROOT / "data/local-test"
        base.mkdir(parents=True, exist_ok=True)
        session = Path(tempfile.mkdtemp(prefix="run-", dir=base))
        print("Seeding captured transfers and checking replay…", flush=True)
        metadata = seed(session, binary)
    print(f"Ready: {metadata['transfer_count']} unique transfers / {metadata['fixture_count']} captures. Replay verified.")
    print(f"Session: {session}")
    print(f"CLI: {session / 'helius-indexer'} query {session / 'index.sqlite'}")
    if args.seed_only:
        return
    server = HTTPServer(("127.0.0.1", args.port), handler_for(session, args.port))
    print(f"Open http://127.0.0.1:{args.port} — captured data; coverage unknown. Ctrl+C stops the console.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, ValueError) as error:
        print(f"Local test setup failed: {error}", file=sys.stderr)
        if isinstance(error, OSError) and error.errno == errno.EADDRINUSE:
            print("Stop the previous console or choose another port with --port 8766.", file=sys.stderr)
        elif isinstance(error, subprocess.CalledProcessError):
            print("For missing build dependencies, run `cargo fetch --locked` once, then retry.", file=sys.stderr)
        sys.exit(1)
