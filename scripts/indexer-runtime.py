#!/usr/bin/env python3
"""Continuous provider-backed intake keeping only the newest transfers; never seeds fixtures."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
from urllib.request import Request, urlopen

SPEC = importlib.util.spec_from_file_location("console", Path(__file__).with_name("local-test.py"))
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)


def integer(env, name, default, low, high):
    value = int(env.get(name) or default)
    if not low <= value <= high:
        raise ValueError("Invalid " + name)
    return value


def configuration(env, tip):
    mint = env.get("INDEXER_MINT", CONSOLE.USDC)
    if not CONSOLE.valid_mint(mint):
        raise ValueError("Invalid INDEXER_MINT")
    end = integer(env, "INDEXER_BACKFILL_END", tip, 0, tip)
    slots = integer(env, "INDEXER_BACKFILL_SLOTS", 50, 1, 128)
    start = integer(env, "INDEXER_BACKFILL_START", max(0, end - slots + 1), 0, end)
    if end - start >= 128:
        raise ValueError("Backfill must contain at most 128 slots")
    return {"mint": mint, "mints": [mint], "slot_start": start, "slot_end": end,
            "live_from": integer(env, "INDEXER_LIVE_FROM", max(0, tip - 4), 0, tip),
            "seconds": integer(env, "INDEXER_LIVE_SECONDS", 60, 1, 300),
            "keep": integer(env, "INDEXER_KEEP_TRANSFERS", 1000, 1, 1_000_000),
            "mode": "live and backfill", "coverage": "unknown", "query_limit": 1000,
            # A PostgreSQL session schema on DATABASE_URL's server, never a file.
            "database": env.get("INDEXER_DB_SESSION", "runtime")}


def credentials(path):
    # Read only the supported key; never evaluate a dotenv file as shell code.
    values = {}
    for line in Path(path).read_text().splitlines():
        if line.startswith("HELIUS_API_KEY="):
            values["HELIUS_API_KEY"] = line.split("=", 1)[1].strip().strip("\"'")
    key = values.get("HELIUS_API_KEY", "")
    if not key or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in key):
        raise ValueError("Missing or invalid HELIUS_API_KEY in runtime secret")
    return key


def finalized_tip(endpoint):
    def rpc(method, params):
        request = Request(endpoint, json.dumps({"jsonrpc": "2.0", "id": 1,
                          "method": method, "params": params}).encode(),
                          {"Content-Type": "application/json"})
        with urlopen(request, timeout=10) as response:
            data = response.read(4097)
        if len(data) > 4096:
            raise ValueError("Invalid reference response")
        result = json.loads(data)
        if result.get("id") != 1 or result.get("jsonrpc") != "2.0" or "error" in result:
            raise ValueError("Reference unavailable")
        return result.get("result")
    if rpc("getGenesisHash", []) != "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d":
        raise ValueError("Mainnet reference required")
    tip = rpc("getSlot", [{"commitment": "finalized"}])
    if type(tip) is not int or not 0 <= tip <= 2**63 - 1:
        raise ValueError("Invalid finalized reference")
    return tip


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def resume_slot(live, mint):
    """Kafka mode resumes from the producer's resume_from (cursor minus the reorder margin),
    never the raw cursor; the PostgreSQL path resumes from the newest retained slot."""
    if os.environ.get("INDEXER_KAFKA_BROKERS"):
        return next((c["resume_from"] for c in live.get("producer_cursors", []) if c["stream"] == mint), None)
    return live.get("last_received_slot")


def initialize(session, binary, database):
    import shutil
    session.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = session / "helius-indexer"
    if not target.exists():
        temporary = session / ".binary.tmp"
        shutil.copyfile(binary, temporary)
        temporary.chmod(0o700)
        temporary.replace(target)
    # Creates the shared schema and retries retained unresolved input, offline and bounded.
    CONSOLE.command(session, "live-retry", database)


def execute_cycle(session, args, env, timeout, on_start, report_name="report.json"):
    report_path = session / report_name
    with report_path.open("wb") as report, (session / "events.jsonl").open("ab") as events:
        child = subprocess.Popen([str(session / "helius-indexer"), *map(str, args)],
            stdout=report, stderr=events, env=env, start_new_session=True)
        on_start(child)
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            child.kill()
            code = child.wait()
    return code, json.loads(report_path.read_text()) if code == 0 else None


def main():
    import fcntl
    session = Path(os.environ.get("INDEXER_SESSION", "/app/data/runtime"))
    session.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (session / ".runtime.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        inspect_only = os.environ.get("INDEXER_RUN", "1") == "0"
        if inspect_only:
            config = json.loads((session / "session.json").read_text())
            env = {}
        else:
            configuration(os.environ, 2**63 - 1)
            key = credentials(os.environ.get("INDEXER_SECRET_FILE", "/run/secrets/helius"))
            endpoint = "https://mainnet.helius-rpc.com/?api-key=" + key
            config = configuration(os.environ, finalized_tip(endpoint))
            env = {**os.environ, "HELIUS_API_KEY": key, "INDEXER_LIVE_OPT_IN": "1", "SOLANA_RPC_URL": endpoint}
        CONSOLE.require_database_url()
        initialize(session, Path(os.environ.get("INDEXER_BINARY", "/usr/local/bin/helius-indexer")), config["database"])
        if not inspect_only:
            atomic_json(session / "session.json", config)
            atomic_json(session / "worker.json", {"state": "starting", "coverage": "unknown"})
        stopped = threading.Event()
        process = None
        def cycle(args, timeout=None, report_name="report.json"):
            def track(child):
                nonlocal process
                process = child
            return execute_cycle(session, args, env, timeout or config["seconds"] + 60, track, report_name)

        def worker():
            # Cycle 1 is the bounded live+backfill run; later cycles resume live intake only.
            db, resume, number, last = config["database"], config["live_from"], 0, {}
            while not stopped.is_set():
                number += 1
                atomic_json(session / "worker.json", {**last, "state": "running", "cycle": number, "coverage": "unknown"})
                try:
                    code, result = cycle(["monitor", db, config["mint"], resume, config["slot_start"],
                                          config["slot_end"], config["seconds"]] if number == 1
                                         else ["live", db, resume, config["seconds"], config["mint"]])
                    failed = not result or code != 0 or any(
                        result.get(source, {}).get("outcome") == "failed" for source in ["live", "historical"])
                    # A large first prune measured ~30 s; a timeout rolls the prune back.
                    if stopped.is_set():
                        atomic_json(session / "worker.json", {"state": "stopped", "cycle": number, "coverage": "unknown"})
                        return
                    prune_code, pruned = cycle(["prune", db, config["keep"]], timeout=300, report_name="prune.json")
                    if prune_code != 0:
                        raise RuntimeError("Prune failed")
                    resume = resume_slot(CONSOLE.command(session, "live-status", db), config["mint"]) or resume
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                    code, result, failed, pruned = None, None, True, None
                last = {"exit_code": code, "report": result, "pruned": pruned, "resume_from": resume}
                atomic_json(session / "worker.json", {**last, "state": "stopped" if stopped.is_set() else "retrying" if failed else "running",
                            "cycle": number, "coverage": "unknown"})
                if failed:
                    stopped.wait(30)  # bounded backoff before reconnecting
        def status():
            return {"worker": json.loads((session / "worker.json").read_text()), "configuration": config,
                    "summary": CONSOLE.command(session, "summary", config["database"], config["mint"]),
                    "live": CONSOLE.command(session, "live-status", config["database"]),
                    "historical": CONSOLE.command(session, "coverage", config["database"], config["mint"], config["slot_start"], config["slot_end"])}
        port = integer(os.environ, "INDEXER_CONSOLE_PORT", 8767, 1024, 65535)
        server = CONSOLE.HTTPServer(("0.0.0.0", port), CONSOLE.handler_for(session, port, runtime_status=status))
        thread = threading.Thread(target=worker) if not inspect_only else None
        def stop(_signum, _frame):
            stopped.set()
            if process is not None and process.poll() is None:
                process.terminate()
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        if thread:
            thread.start()
        print("Provider-backed console ready; " + ("inspection only" if inspect_only else "continuous intake starting") + "; coverage unknown", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            stopped.set()
            if process is not None and process.poll() is None:
                process.terminate()
            if thread:
                thread.join(timeout=10)
            if thread and thread.is_alive() and process is not None:
                process.kill()
                thread.join()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Provider exceptions can contain the credential-bearing endpoint. Never print them.
        print("Runtime setup failed; check secret, bounds, connectivity and session lock", file=sys.stderr)
        sys.exit(1)
