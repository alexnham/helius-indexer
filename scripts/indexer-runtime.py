#!/usr/bin/env python3
"""Continuous provider-backed intake keeping only the newest transfers; never seeds fixtures."""
import importlib.util
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
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
    # Unset is an exact dataset; 1-100 keeps that percentage of the mint's transfers.
    percent = integer(env, "INDEXER_SAMPLE_PERCENT", 0, 1, 100) if env.get("INDEXER_SAMPLE_PERCENT") else None
    mint = env.get("INDEXER_MINT", CONSOLE.USDC)
    if not CONSOLE.valid_mint(mint):
        raise ValueError("Invalid INDEXER_MINT")
    end = integer(env, "INDEXER_BACKFILL_END", tip, 0, tip)
    slots = integer(env, "INDEXER_BACKFILL_SLOTS", 50, 1, 128)
    start = integer(env, "INDEXER_BACKFILL_START", max(0, end - slots + 1), 0, end)
    if end - start >= 128:
        raise ValueError("Backfill must contain at most 128 slots")
    job=env.get("INDEXER_REPLAY_JOB", "recent")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}",job):
        raise ValueError("Invalid INDEXER_REPLAY_JOB")
    return {"historical_source":"laserstream_replay","replay_job":job,"mint": mint, "mints": [mint], "sample_percent": percent, "slot_start": start, "slot_end": end,
            "live_from": integer(env, "INDEXER_LIVE_FROM", max(0, tip - 4), 0, tip),
            "seconds": integer(env, "INDEXER_LIVE_SECONDS", 60, 1, 300),
            "keep": integer(env, "INDEXER_KEEP_TRANSFERS", 1000, 1, 1_000_000),
            "mode": "live and backfill", "coverage": "unknown", "query_limit": 1000,
            # A PostgreSQL session schema on DATABASE_URL's server, never a file.
            "database": env.get("INDEXER_DB_SESSION", "runtime")}


def resume_configuration(session, env, tip):
    receipt=session / "session.json"
    if not receipt.exists():
        return configuration(env,tip)
    saved=json.loads(receipt.read_text())
    if saved.get("historical_source")!="laserstream_replay":
        raise ValueError("Use a fresh replay runtime directory; legacy data is preserved")
    requested=dict(env)
    requested.setdefault("INDEXER_MINT",saved["mint"])
    requested.setdefault("INDEXER_DB_SESSION",saved["database"])
    requested.setdefault("INDEXER_REPLAY_JOB",saved["replay_job"])
    requested.setdefault("INDEXER_SAMPLE_PERCENT",str(saved.get("sample_percent") or ""))
    requested["INDEXER_BACKFILL_END"]=env.get("INDEXER_BACKFILL_END") or str(saved["slot_end"])
    requested["INDEXER_BACKFILL_START"]=env.get("INDEXER_BACKFILL_START") or str(saved["slot_start"])
    if env.get("INDEXER_BACKFILL_SLOTS") and not env.get("INDEXER_BACKFILL_START"):
        requested["INDEXER_BACKFILL_START"]=str(int(requested["INDEXER_BACKFILL_END"])-int(env["INDEXER_BACKFILL_SLOTS"])+1)
    current=configuration(requested,tip)
    for field in ["mint","database","replay_job","slot_start","slot_end","sample_percent"]:
        if current[field]!=saved.get(field):
            raise ValueError("Frozen replay scope changed; choose a new directory and job ID")
    return current


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


def digest(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def binary_state(session, binary):
    """The session keeps the binary it first copied; a rebuilt image never replaces it implicitly."""
    target = session / "helius-indexer"
    if not target.exists():
        return "fresh"
    # Without an image binary to compare, the session's copy is the one to run.
    if not Path(binary).exists():
        return "current"
    return "current" if digest(target) == digest(binary) else "upgrade_required"


def install_binary(session, binary, upgrade):
    """Copy on first start; replace a different copy only on explicit request, keeping a backup."""
    import shutil
    import time
    state = binary_state(session, binary)
    target = session / "helius-indexer"
    if state == "upgrade_required" and not upgrade:
        return state
    if state == "upgrade_required":
        shutil.copy2(target, session / ("helius-indexer.backup-" + str(time.time_ns())))
    if state != "current":
        temporary = session / ".binary.tmp"
        shutil.copyfile(binary, temporary)
        temporary.chmod(0o700)
        temporary.replace(target)
    return "upgraded" if state == "upgrade_required" else state


def initialize(session, binary, database, mint=None, percent=None, upgrade=False):
    session.mkdir(parents=True, exist_ok=True, mode=0o700)
    state = install_binary(session, binary, upgrade)
    if state == "upgrade_required":
        return state
    # Recovery runs behind live-priority admission inside the native paired worker.
    CONSOLE.command(session, "migrate", database)
    # The dataset policy is immutable; it must be set before any input is admitted.
    if percent is not None:
        CONSOLE.command(session, "sample-init", database, mint, percent)
    policy = CONSOLE.command(session, "sample-status", database)
    sampled = policy.get("policy") != "exact"
    if sampled != (percent is not None) or (sampled and (policy.get("mint"), policy.get("numerator")) != (mint, percent)):
        raise ValueError("Dataset sampling mode or mint does not match runtime configuration")
    return state


ROUTE_INTERVAL_SECONDS = 3600


def should_route(status, since_attempt=None, interval=ROUTE_INTERVAL_SECONDS):
    """Route once; re-measure only when replay fell out of its window, the archive tip was
    unknown, or uncovered slots are still unverified (a later archive epoch may now serve
    them; an RPC-filled gap no longer needs one), and
    at most once per interval. Each revision resets overlap evidence.
    `since_attempt` (seconds since the last attempt, failed or not) bounds retries."""
    if since_attempt is not None and since_attempt < interval:
        return False
    routing = status.get("routing")
    if not routing:
        return True
    age = routing.get("measurement_age_seconds")
    if age is None or age < interval:
        return False
    attempts = status.get("attempts") or []
    return (bool(attempts) and attempts[0].get("status") == "outside_replay_window") \
        or (routing.get("archive") or {}).get("error_class") is not None \
        or (status.get("verified") or {}).get("uncovered_unverified_slots", 0) > 0


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
    session = Path(os.environ.get("INDEXER_SESSION", "/app/data/runtime-replay"))
    session.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (session / ".runtime.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        inspect_only = os.environ.get("INDEXER_RUN", "1") == "0"
        image = Path(os.environ.get("INDEXER_BINARY", "/usr/local/bin/helius-indexer"))
        # A mismatched binary needs no credentials or provider calls just to refuse running.
        if binary_state(session, image) == "upgrade_required" and os.environ.get("INDEXER_UPGRADE_BINARY") != "1":
            inspect_only = True
        if inspect_only:
            config = json.loads((session / "session.json").read_text())
            env = {}
        else:
            configuration(os.environ, 2**63 - 1)
            key = credentials(os.environ.get("INDEXER_SECRET_FILE", "/run/secrets/helius"))
            endpoint = "https://mainnet.helius-rpc.com/?api-key=" + key
            config = resume_configuration(session, os.environ, finalized_tip(endpoint))
            env = {**os.environ, "HELIUS_API_KEY": key, "INDEXER_LIVE_OPT_IN": "1", "SOLANA_RPC_URL": endpoint}
        archive = not inspect_only and os.environ.get("INDEXER_ARCHIVE", "0") == "1"
        cache = session / "archive-cache"
        if archive:
            cache.mkdir(exist_ok=True, mode=0o700)
            env["INDEXER_ARCHIVE_OPT_IN"] = "1"
        CONSOLE.require_database_url()
        binary = initialize(session, image, config["database"],
                            upgrade=not inspect_only and os.environ.get("INDEXER_UPGRADE_BINARY") == "1",
                            mint=config["mint"], percent=config.get("sample_percent"))
        if binary == "upgrade_required":
            # Serve the existing session for inspection; run nothing with a binary it was not started with.
            inspect_only = True
            atomic_json(session / "worker.json", {"state": "binary_upgrade_required", "coverage": "unknown"})
        if not inspect_only:
            CONSOLE.command(session,"replay-create",config["database"],config["replay_job"],config["mint"],config["slot_start"],config["slot_end"])
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
            # Both workers resume independently; replay budgets never reset between cycles.
            db, resume, number, last, routed_at = config["database"], config["live_from"], 0, {}, None
            while not stopped.is_set():
                number += 1
                atomic_json(session / "worker.json", {**last, "state": "running", "cycle": number, "coverage": "unknown"})
                try:
                    since = None if routed_at is None else time.monotonic() - routed_at
                    if archive and should_route(CONSOLE.command(session, "replay-status", db, config["replay_job"]), since):
                        routed_at = time.monotonic()
                        cycle(["replay-route", db, config["replay_job"], cache], timeout=180, report_name="route.json")
                    code, result = cycle(["replay-monitor",db,config["replay_job"],resume,config["seconds"],*([cache] if archive else [])])
                    # Historical expiry/exhaustion remains visible without delaying live reconnects.
                    failed=not result or code!=0 or result.get("live",{}).get("outcome")=="failed"
                    # A large first prune measured ~30 s; a timeout rolls the prune back.
                    if stopped.is_set():
                        atomic_json(session / "worker.json", {"state": "stopped", "cycle": number, "coverage": "unknown"})
                        return
                    if config.get("sample_percent") is not None:
                        pruned = None  # A sampled demo keeps every raw input for decoder repair.
                    else:
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
            return {"worker": json.loads((session / "worker.json").read_text()), "configuration": {**config, "archive": archive, "binary": binary},
                    "summary": CONSOLE.command(session, "summary", config["database"], config["mint"], "--all-stored"),
                    "live": CONSOLE.command(session, "live-status", config["database"]),
                    "historical": CONSOLE.command(session, "replay-status", config["database"], config["replay_job"]) if config.get("replay_job") else CONSOLE.command(session,"coverage",config["database"],config["mint"],config["slot_start"],config["slot_end"])}
        port = integer(os.environ, "INDEXER_CONSOLE_PORT", 8765, 1024, 65535)
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
