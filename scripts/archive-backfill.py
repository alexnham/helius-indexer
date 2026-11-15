#!/usr/bin/env python3
"""Bounded, resumable driver for sampled Old Faithful USDC history."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SAMPLE_PERCENT = 10
BATCH_RESERVE = 704 * 1024 * 1024  # archive 128 MiB + nine RPC blocks at 64 MiB
DEFAULT_RESERVE_LIMIT = 2 * 1024**3
DEFAULT_MIN_FREE = 10 * 1024**3
# Above the native per-batch bounds: 120 s archive plus nine 20 s RPC block requests.
CHILD_TIMEOUT_SECONDS = 360
MANIFEST_NAME = "archive-backfill.json"


class SafeError(RuntimeError):
    pass


def write_manifest(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def read_json_output(stdout):
    try:
        value = json.loads(stdout)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise SafeError("indexer returned invalid JSON") from None
    if not isinstance(value, dict):
        raise SafeError("indexer returned an invalid report")
    return value


def child(session, argv, env, timeout=None):
    if timeout is None:
        timeout = CHILD_TIMEOUT_SECONDS
    process = subprocess.Popen(
        [str(session.binary), *map(str, argv)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=env, cwd=ROOT,
    )

    def reap():
        process.terminate()
        try:
            return process.communicate(timeout=5)[1]
        except subprocess.TimeoutExpired:
            process.kill()
            return process.communicate()[1]

    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Keep the killed command's diagnostics; they are the only evidence of the timeout.
        keep_events(session.directory, reap())
        raise SafeError("indexer command timed out; actual bytes are unknown") from None
    except KeyboardInterrupt:
        keep_events(session.directory, reap())
        raise
    keep_events(session.directory, stderr)
    if process.returncode:
        # Error classes are fixed identifiers from the CLI, never inputs or credentials.
        classes = re.findall(rb'"error_class":"([a-z0-9_]{1,64})"', stderr)
        detail = ": " + classes[-1].decode() if classes else ""
        raise SafeError(f"indexer command failed{detail}; actual bytes are unknown; inspect events.jsonl")
    return read_json_output(stdout)


def keep_events(directory, stderr):
    # The indexer's stderr is its redacted structured event stream, as in scripts/local-test.py.
    if stderr:
        with (directory / "events.jsonl").open("ab") as log:
            log.write(stderr)


def session_args(parser):
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--session", required=True)
    parser.add_argument("--mint", default=DEFAULT_MINT)
    parser.add_argument("--allow-provider", action="store_true")


def parser_for():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="initialize 10%% sampling and freeze source bounds")
    session_args(plan)
    scope = plan.add_mutually_exclusive_group(required=True)
    scope.add_argument("--start-slot", type=int)
    scope.add_argument("--days", type=int)
    scope.add_argument("--window-file", type=Path)

    run = sub.add_parser("run", help="run a bounded number of bridge batches")
    session_args(run)
    run.add_argument("--max-batches", required=True, type=int)
    run.add_argument("--max-reserved-bytes", type=int, default=DEFAULT_RESERVE_LIMIT)
    run.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE)

    status = sub.add_parser("status", help="read durable bridge and sampling status")
    session_args(status)

    serve = sub.add_parser("serve", help="serve a read-only dashboard on loopback")
    session_args(serve)
    serve.add_argument("--port", type=int, default=8768)
    return parser


def validate(args):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", args.session):
        raise SafeError("invalid session name")
    days = getattr(args, "days", None)
    start_slot = getattr(args, "start_slot", None)
    if days is not None and days <= 0:
        raise SafeError("days must be positive")
    if start_slot is not None and start_slot <= 0:
        raise SafeError("start slot must be positive")
    if args.command == "serve" and not 1024 <= args.port <= 65535:
        raise SafeError("port must be between 1024 and 65535")
    if args.command == "run" and (args.max_batches <= 0 or args.max_reserved_bytes < BATCH_RESERVE or args.min_free_bytes < 0):
        raise SafeError("invalid run limits")
    args.directory = args.directory.expanduser().resolve()
    args.binary = args.binary.expanduser().resolve()
    window_file = getattr(args, "window_file", None)
    if window_file is not None:
        args.window_file = window_file.expanduser().resolve()
    args.directory.mkdir(parents=True, exist_ok=True)
    if not args.binary.is_file():
        raise SafeError("binary does not exist")


def setup(args, provider):
    args.directory.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if provider:
        if not args.allow_provider and env.get("INDEXER_ARCHIVE_OPT_IN") != "1":
            raise SafeError("provider access requires --allow-provider or INDEXER_ARCHIVE_OPT_IN=1")
        env["INDEXER_ARCHIVE_OPT_IN"] = "1"
    elif env.get("INDEXER_ARCHIVE_OPT_IN") != "1":
        env.pop("INDEXER_ARCHIVE_OPT_IN", None)
    return env


def manifest_paths(directory):
    return directory / MANIFEST_NAME, directory / (MANIFEST_NAME + ".lock")


def locked(directory):
    _, lock_path = manifest_paths(directory)
    lock = lock_path.open("a+")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise SafeError("another archive-backfill command holds the session lock") from None
    return lock


def load_manifest(path, args):
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise SafeError("manifest is missing or invalid; run plan") from None
    if (manifest.get("session"), manifest.get("mint")) != (args.session, args.mint):
        raise SafeError("manifest scope does not match arguments")
    if args.command == "run" and args.max_reserved_bytes < manifest["budget"]["reserved_bytes"]:
        raise SafeError("reserve limit is below the already consumed reservation")
    return manifest


def concise_bridge(report):
    historical = report.get("historical", {})
    return {
        "start": report.get("start"), "archive_tip": report.get("archive_tip"),
        "finalized_reference": report.get("finalized_reference"),
        "boundary_snapshot": report.get("boundary_snapshot"),
        "live_tail": report.get("live_tail"),
        "historical": {key: historical.get(key) for key in (
            "next_pending_slot", "contiguous_through", "complete", "pending_slots",
            "missing_ranges", "overlap_verified", "overlap_recovered", "boundary_ready",
        )},
    }


def plan(args):
    env = setup(args, provider=True)
    path, _ = manifest_paths(args.directory)
    if path.exists():
        current = load_manifest(path, args)
        if args.start_slot is not None and current.get("start_slot") != args.start_slot:
            raise SafeError("plan already exists with a different start slot")
        if args.days is not None and (current.get("window") or {}).get("days") != args.days:
            raise SafeError("plan already exists with a different window length")
        if args.window_file is not None:
            requested = read_window_file(args.window_file)
            frozen = current.get("window") or {}
            if any(frozen.get(key) != requested.get(key)
                   for key in ("start_slot", "cutoff_unix_seconds")):
                raise SafeError("plan already exists with a different time boundary")
        frozen_binary(args, current)
        print(json.dumps({"manifest": str(path), "plan": current["plan"]}, sort_keys=True))
        return

    binary_hash = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    stored_binary = args.directory / "helius-indexer"
    if stored_binary.exists() and hashlib.sha256(stored_binary.read_bytes()).hexdigest() != binary_hash:
        raise SafeError("session already contains a different indexer binary")

    window = None
    if args.days is not None:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            from archive_window import discover_window
            window = discover_window(args.days, env.get("SOLANA_RPC_URL"))
        except Exception:
            # Provider exceptions may contain credential-bearing URLs.
            raise SafeError("time-window discovery failed") from None
        start = window.get("start_slot")
        if not isinstance(start, int) or start <= 0:
            raise SafeError("time-window discovery returned no start slot")
    elif args.window_file is not None:
        window = read_window_file(args.window_file)
        start = window["start_slot"]
    else:
        start = args.start_slot

    if window is not None:
        window = sanitize_window(window)

    child(args, ["sample-init", args.session, args.mint, SAMPLE_PERCENT], env)
    report = child(args, ["bridge-plan", args.session,
                                 args.directory / "archive-cache", args.mint, start], env)
    sampling = child(args, ["sample-status", args.session], env)
    if sampling.get("policy") != "sha256-transfer-id-v1" or sampling.get("numerator") != SAMPLE_PERCENT or sampling.get("mint") != args.mint:
        raise SafeError("sampling policy did not confirm the requested 10% sample")
    if not stored_binary.exists():
        temporary_binary = args.directory / ".helius-indexer.tmp"
        shutil.copyfile(args.binary, temporary_binary)
        temporary_binary.chmod(0o700)
        os.replace(temporary_binary, stored_binary)
    args.binary = stored_binary
    write_manifest(args.directory / "session.json", {
        "mode": "live and backfill", "database": args.session, "mint": args.mint,
        "mints": [args.mint], "keep": None, "sample_percent": SAMPLE_PERCENT,
        "historical_source": "archive_bridge", "slot_start": start,
        "slot_end": report.get("finalized_reference"),
    })
    manifest = {
        "schema_version": 1, "session": args.session, "mint": args.mint,
        "start_slot": start, "sampling": sampling, "window": window,
        "binary_sha256": binary_hash,
        "plan": concise_bridge(report), "created_at_unix_seconds": int(time.time()),
        "budget": {"per_batch_reserved_bytes": BATCH_RESERVE,
                   "reserved_bytes": 0, "max_reserved_bytes": DEFAULT_RESERVE_LIMIT,
                   "unknown_actual_batches": 0, "actual_response_bytes": 0},
        "batches": [],
    }
    write_manifest(path, manifest)
    print(json.dumps({"manifest": str(path), "plan": manifest["plan"], "window": window}, sort_keys=True))


def read_window_file(path):
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise SafeError("window file is missing or invalid") from None
    required_ints = ("start_slot", "cutoff_unix_seconds", "measured_at_unix_seconds", "reference_slot")
    if (not isinstance(raw, dict)
            or raw.get("boundary_status") != "conservative_time_probe_not_verified_coverage"
            or any(not isinstance(raw.get(key), int) for key in required_ints)
            or not isinstance(raw.get("observations"), list)):
        raise SafeError("window file metadata is invalid")
    safe = sanitize_window(raw)
    if safe["start_slot"] <= 0 or safe["reference_slot"] <= 0:
        raise SafeError("window file has invalid slot bounds")
    return safe


def sanitize_window(raw):
    allowed = {"cutoff_unix_seconds", "measured_at_unix_seconds", "reference_slot",
               "start_slot", "boundary_status", "days"}
    safe = {key: raw[key] for key in allowed if key in raw}
    observations = raw.get("observations", [])
    if isinstance(observations, list):
        safe["observations"] = [
            {key: row[key] for key in ("slot", "block_time") if key in row}
            for row in observations if isinstance(row, dict)
        ]
    return safe


def status(args):
    env = setup(args, provider=False)
    path, _ = manifest_paths(args.directory)
    # Read-only, so no lock: a run holds it for its whole loop, and the manifest is
    # replaced atomically.
    manifest = load_manifest(path, args)
    frozen_binary(args, manifest)
    bridge = child(args, ["bridge-status", args.session, args.mint, manifest["start_slot"]], env)
    sampling = child(args, ["sample-status", args.session], env)
    output = {
        "manifest": {key: manifest.get(key) for key in (
            "session", "mint", "start_slot", "state", "stop_reason", "window", "sampling",
            "budget", "batches",
        )},
        "bridge": bridge, "sampling": sampling,
    }
    print(json.dumps(output, sort_keys=True))


def source_bytes(report):
    diagnostics = report.get("source_diagnostics", {})
    archive = diagnostics.get("archive", {}).get("response_body_bytes")
    rpc = diagnostics.get("rpc", {}).get("response_body_bytes_read")
    if isinstance(archive, int) and isinstance(rpc, int):
        return archive + rpc
    return None


def run(args):
    env = setup(args, provider=True)
    path, _ = manifest_paths(args.directory)
    cancel_file = args.directory / "CANCEL"
    env["BACKFILL_CANCEL_FILE"] = str(cancel_file)
    with locked(args.directory):
        manifest = load_manifest(path, args)
        frozen_binary(args, manifest)
        # Starting a run is the operator's explicit resume; a CANCEL left by an earlier
        # run would otherwise stop every later one before its first batch.
        stale_cancel = cancel_file.exists()
        cancel_file.unlink(missing_ok=True)
        manifest["state"] = "running"
        manifest["stop_reason"] = None
        manifest["budget"]["max_reserved_bytes"] = args.max_reserved_bytes
        # A previous process may have died after its durable reservation.
        for batch in manifest["batches"]:
            if batch.get("state") == "running":
                batch.update(state="interrupted_unknown", actual_response_bytes=None)
                manifest["budget"]["unknown_actual_batches"] += 1
        write_manifest(path, manifest)

        initial_total_batches = manifest.get("total_batches", 0)
        try:
            run_batches(args, env, path, manifest, cancel_file)
        except KeyboardInterrupt:
            manifest["stop_reason"] = manifest["stop_reason"] or "interrupted"
        except BaseException:
            # Never leave a dead run recorded as running without a reason.
            manifest["state"] = "stopped"
            manifest["stop_reason"] = manifest["stop_reason"] or "error"
            write_manifest(path, manifest)
            raise

        if manifest["state"] != "complete":
            manifest["state"] = "stopped"
            if manifest["stop_reason"] is None:
                manifest["stop_reason"] = "max_batches"
        write_manifest(path, manifest)

    print(json.dumps({"manifest": str(path), "budget": manifest["budget"],
                      "batches_run": manifest.get("total_batches", 0) - initial_total_batches,
                      "latest_progress": manifest["batches"][-1].get("progress") if manifest["batches"] else None,
                      "cleared_stale_cancel": stale_cancel,
                      "cancel_requested": cancel_file.exists()}, sort_keys=True))


def run_batches(args, env, path, manifest, cancel_file):
    for _ in range(args.max_batches):
        if cancel_file.exists():
            manifest["stop_reason"] = "cancel_file"
            return
        bridge = child(args, ["bridge-status", args.session, args.mint,
                              manifest["start_slot"]], env)
        progress = bridge.get("historical", {})
        if progress.get("complete") is True or progress.get("pending_slots") == 0:
            manifest["state"] = "complete"
            manifest["stop_reason"] = "coverage_complete"
            return
        free_bytes = shutil.disk_usage(args.directory).free
        if free_bytes < args.min_free_bytes:
            manifest["stop_reason"] = "free_disk_below_threshold"
            return
        budget = manifest["budget"]
        if budget["reserved_bytes"] + BATCH_RESERVE > args.max_reserved_bytes:
            manifest["stop_reason"] = "byte_reservation_limit"
            return

        manifest["total_batches"] = manifest.get("total_batches", 0) + 1
        attempt = {"number": manifest["total_batches"],
                   "state": "running", "reserved_bytes": BATCH_RESERVE,
                   "actual_response_bytes": None, "started_at_unix_seconds": int(time.time())}
        manifest["batches"].append(attempt)
        budget["reserved_bytes"] += BATCH_RESERVE
        write_manifest(path, manifest)  # reservation survives a crash before the child starts
        try:
            report = child(args, ["bridge", args.session,
                                  args.directory / "archive-cache", args.mint,
                                  manifest["start_slot"]], env)
        except KeyboardInterrupt:
            attempt.update(state="cancelled_unknown", finished_at_unix_seconds=int(time.time()))
            budget["unknown_actual_batches"] += 1
            manifest["stop_reason"] = "interrupted"
            return
        except SafeError:
            attempt.update(state="failed_unknown", failure="child_failed",
                           finished_at_unix_seconds=int(time.time()))
            budget["unknown_actual_batches"] += 1
            manifest["stop_reason"] = "child_failed"
            raise

        actual = source_bytes(report)
        attempt.update(state="complete", actual_response_bytes=actual,
                       finished_at_unix_seconds=int(time.time()))
        if actual is None:
            budget["unknown_actual_batches"] += 1
        else:
            budget["actual_response_bytes"] += actual
            # A measured batch is charged what it read; only unknown batches keep the worst case.
            budget["reserved_bytes"] += actual - BATCH_RESERVE
            attempt["reserved_bytes"] = actual
        attempt["progress"] = concise_bridge(report)["historical"]
        manifest["batches"] = manifest["batches"][-256:]
        write_manifest(path, manifest)
        if report.get("cancelled") is True:
            manifest["stop_reason"] = "cancelled_by_indexer"
            return
        if report.get("historical", {}).get("pending_slots", 0) >= progress.get("pending_slots", 0):
            manifest["stop_reason"] = "batch_did_not_reduce_pending_slots"
            return


def serve(args):
    env = setup(args, provider=False)
    path, _ = manifest_paths(args.directory)
    manifest = load_manifest(path, args)
    frozen_binary(args, manifest)
    try:
        import importlib.util
        from http.server import HTTPServer
        spec = importlib.util.spec_from_file_location("indexer_local_test", ROOT / "scripts" / "local-test.py")
        local_test = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(local_test)
    except Exception:
        raise SafeError("local dashboard helper is unavailable") from None

    def runtime_status():
        current = json.loads(path.read_text(encoding="utf-8"))
        bridge = child(args, ["bridge-status", args.session, args.mint,
                                     current["start_slot"]], env)
        sampling = child(args, ["sample-status", args.session], env)
        summary = child(args, ["summary", args.session, args.mint], env)
        return {
            "worker": {"state": current.get("state", "stopped"),
                       "stop_reason": current.get("stop_reason")},
            "configuration": {"historical_source": "archive_bridge",
                              "slot_start": current["start_slot"],
                              "slot_end": bridge.get("finalized_reference"),
                              "mint": args.mint, "mints": [args.mint],
                              "sample_percent": SAMPLE_PERCENT, "keep": None},
            "historical": bridge.get("historical", {}), "bridge": bridge,
            "sampling": sampling,
            "backfill": {"state": current.get("state", "stopped"),
                         "window": current.get("window"), "budget": current.get("budget"),
                         "batches": current.get("batches", []),
                         "stop_reason": current.get("stop_reason")},
            "live": {"coverage": "unknown"}, "summary": summary,
        }

    server = HTTPServer(("127.0.0.1", args.port),
                        local_test.handler_for(args.directory, args.port, runtime_status, windowed=True))
    print(f"Open http://127.0.0.1:{args.port} — sampled history; coverage is source-verified only.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def frozen_binary(args, manifest):
    stored = args.directory / "helius-indexer"
    try:
        digest = hashlib.sha256(stored.read_bytes()).hexdigest()
    except OSError:
        raise SafeError("frozen plan binary is missing") from None
    if digest != manifest.get("binary_sha256"):
        raise SafeError("frozen plan binary does not match its manifest")
    args.binary = stored


def main(argv=None):
    def terminate(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    args = parser_for().parse_args(argv)
    try:
        validate(args)
        if args.command == "plan":
            with locked(args.directory):
                plan(args)
        elif args.command == "run":
            run(args)
        elif args.command == "serve":
            serve(args)
        else:
            status(args)
    except SafeError as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
