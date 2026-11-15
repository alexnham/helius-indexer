"""Provider console boundaries against the real Rust CLI and local PostgreSQL; no network."""
import importlib.util
import json
import contextlib
import io
import os
import secrets
import sys
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("runtime", ROOT / "scripts/indexer-runtime.py")
RUNTIME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNTIME)
# The local test container; never a hosted database.
os.environ.setdefault("DATABASE_URL", os.environ.get(
    "INDEXER_TEST_DATABASE_URL", "postgres://indexer:indexer@127.0.0.1:55432/indexer"))


CONSOLE_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def database():
    return "pytest_" + secrets.token_hex(6)


class RuntimeConsole(unittest.TestCase):
    def test_configurable_mint_and_independent_live_and_backfill_ranges(self):
        default = RUNTIME.configuration({}, 1000)
        self.assertEqual((default["slot_start"], default["slot_end"], default["seconds"], default["keep"]), (951, 1000, 60, 1000))
        custom = RUNTIME.configuration({"INDEXER_MINT": "So11111111111111111111111111111111111111112",
            "INDEXER_BACKFILL_START": "100", "INDEXER_BACKFILL_END": "149",
            "INDEXER_LIVE_FROM": "990", "INDEXER_LIVE_SECONDS": "10"}, 1000)
        self.assertEqual((custom["slot_start"], custom["slot_end"], custom["live_from"]), (100, 149, 990))
        for options in [{"INDEXER_MINT": "bad"}, {"INDEXER_BACKFILL_SLOTS": "129"},
                        {"INDEXER_LIVE_SECONDS": "301"}, {"INDEXER_BACKFILL_END": "1001"},
                        {"INDEXER_BACKFILL_START": "0"}, {"INDEXER_LIVE_FROM": "-1"},
                        {"INDEXER_KEEP_TRANSFERS": "0"}]:
            with self.assertRaises(ValueError):
                RUNTIME.configuration(options, 1000)

    def test_restart_keeps_frozen_replay_scope_and_rejects_changed_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            session=Path(directory)
            original=RUNTIME.resume_configuration(session,{},1000)
            RUNTIME.atomic_json(session / "session.json",original)
            restarted=RUNTIME.resume_configuration(session,{},2000)
            self.assertEqual((restarted["slot_start"],restarted["slot_end"]),(951,1000))
            self.assertEqual(restarted["live_from"],1996)
            self.assertEqual(restarted["replay_job"],original["replay_job"])
            for changed in [{"INDEXER_BACKFILL_SLOTS":"49"},{"INDEXER_BACKFILL_START":"952"},
                            {"INDEXER_REPLAY_JOB":"different"},{"INDEXER_MINT":"So11111111111111111111111111111111111111112"}]:
                with self.assertRaises(ValueError):
                    RUNTIME.resume_configuration(session,changed,2000)
            self.assertEqual(json.loads((session / "session.json").read_text()),original)

    def test_replay_job_queries_disclose_budget_and_unknown_coverage_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            session=Path(directory);db=database()
            RUNTIME.initialize(session,ROOT / "target/debug/helius-indexer",db)
            first=RUNTIME.CONSOLE.command(session,"replay-create",db,"recent",RUNTIME.CONSOLE.USDC,100,149)
            again=RUNTIME.CONSOLE.command(session,"replay-create",db,"recent",RUNTIME.CONSOLE.USDC,100,149)
            self.assertEqual(first["budget"],again["budget"])
            self.assertEqual(again["budget"]["seconds_remaining"],60)
            self.assertEqual(again["retained"]["transfer_count"],0)
            self.assertEqual(again["coverage"],"unknown")
            self.assertIsNone(again["verified_boundary"])
            RUNTIME.CONSOLE.command(session,"prune",db,1000)
            self.assertEqual(RUNTIME.CONSOLE.command(session,"replay-status",db,"recent")["budget"],first["budget"])

    def test_runtime_bootstrap_is_empty_resumable_and_serves_recent_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            binary = ROOT / "target/debug/helius-indexer"
            config = RUNTIME.configuration({"INDEXER_DB_SESSION": database()}, 1000)
            db = config["database"]
            RUNTIME.initialize(session, binary, db)
            RUNTIME.atomic_json(session / "session.json", config)
            self.assertEqual(RUNTIME.CONSOLE.command(session, "query", db, "--all-stored")["transfers"], [])
            capture = ROOT / "tests/fixtures/mainnet-transfer.json"
            RUNTIME.CONSOLE.command(session, "ingest", db, capture)
            before = RUNTIME.CONSOLE.command(session, "query-recent", db, "--all-stored")
            RUNTIME.initialize(session, Path("missing-image-binary"), db)
            self.assertEqual(before, RUNTIME.CONSOLE.command(session, "query-recent", db, "--all-stored"))
            server = RUNTIME.CONSOLE.HTTPServer(("127.0.0.1", 0), RUNTIME.CONSOLE.BaseHTTPRequestHandler)
            server.RequestHandlerClass = RUNTIME.CONSOLE.handler_for(session, server.server_port,
                runtime_status=lambda: {"worker": {"state": "finished"}, "coverage": "unknown"})
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
            thread.start()
            try:
                def get(route):
                    with urlopen(f"http://127.0.0.1:{server.server_port}" + route, timeout=5) as response:
                        return json.load(response)
                self.assertEqual(get("/api/transfers")["order"], "newest_first")
                self.assertEqual(get("/api/status")["worker"]["state"], "finished")
                self.assertEqual(get("/healthz")["mode"], "live and backfill")
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_independent_sample_checks_retained_instruction_values(self):
        spec = importlib.util.spec_from_file_location("comparison", ROOT / "scripts/compare-captured-usdc.py")
        comparison = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(comparison)
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            db = database()
            RUNTIME.initialize(session, ROOT / "target/debug/helius-indexer", db)
            fixture = ROOT / "tests/fixtures/mainnet-transfer.json"
            RUNTIME.CONSOLE.command(session, "ingest", db, fixture)
            capture = json.loads(fixture.read_text())
            slot = capture["result"]["slot"]
            # Stored rows come from the Rust decoder; the blocks feed the independent parser.
            stored = [row for row in RUNTIME.CONSOLE.command(session, "query", db, comparison.MINT, "--all-stored")["transfers"]]
            blocks = [{"slot": slot + index, "response": {"result": {"transactions": [capture["result"]] if index == 0 else []}}}
                      for index in range(16)]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertFalse(comparison.compare({"blocks": blocks, "transfers": stored}))
                result = json.loads(output.getvalue())
                self.assertEqual(result["blocks"], 16)
                self.assertGreater(result["expected"], 0)
                self.assertEqual((result["missing"], result["extra"], result["wrong"]), (0, 0, 0))

    def test_kafka_mode_resumes_from_resume_from_never_the_raw_cursor(self):
        live = {"last_received_slot": 900, "producer_cursors": [
            {"stream": "other", "slot": 50, "resume_from": 0},
            {"stream": CONSOLE_MINT, "slot": 1000, "resume_from": 850}]}
        os.environ.pop("INDEXER_KAFKA_BROKERS", None)
        self.assertEqual(RUNTIME.resume_slot(live, CONSOLE_MINT), 900)
        os.environ["INDEXER_KAFKA_BROKERS"] = "127.0.0.1:59092"
        try:
            self.assertEqual(RUNTIME.resume_slot(live, CONSOLE_MINT), 850)
            self.assertIsNone(RUNTIME.resume_slot({"producer_cursors": []}, CONSOLE_MINT))
        finally:
            os.environ.pop("INDEXER_KAFKA_BROKERS", None)

    def test_supervised_child_is_killed_and_reaped_on_timeout_or_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            binary = session / "helius-indexer"
            binary.write_text("#!" + sys.executable + "\nimport time\ntime.sleep(20)\n")
            binary.chmod(0o700)
            children = []
            code, result = RUNTIME.execute_cycle(session, [], os.environ, 0.05, children.append)
            self.assertNotEqual(code, 0)
            self.assertIsNone(result)
            self.assertIsNotNone(children[0].poll())
            def stop(child):
                children.append(child)
                child.terminate()
            code, result = RUNTIME.execute_cycle(session, [], os.environ, 2, stop, "prune.json")
            self.assertNotEqual(code, 0)
            self.assertIsNone(result)
            self.assertIsNotNone(children[-1].poll())

    def test_a_rebuilt_binary_is_never_used_without_an_explicit_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session, image = root / "session", root / "image"
            session.mkdir()
            image.write_bytes(b"v1")
            self.assertEqual(RUNTIME.install_binary(session, image, False), "fresh")
            self.assertEqual(RUNTIME.install_binary(session, image, False), "current")
            image.write_bytes(b"v2")
            self.assertEqual(RUNTIME.install_binary(session, image, False), "upgrade_required")
            self.assertEqual((session / "helius-indexer").read_bytes(), b"v1")
            self.assertEqual(RUNTIME.install_binary(session, image, True), "upgraded")
            self.assertEqual((session / "helius-indexer").read_bytes(), b"v2")
            backups = list(session.glob("helius-indexer.backup-*"))
            self.assertEqual([b.read_bytes() for b in backups], [b"v1"])
            self.assertEqual(RUNTIME.binary_state(session, image), "current")

    def test_routes_once_and_re_routes_only_on_stale_window_evidence(self):
        self.assertTrue(RUNTIME.should_route({"routing": None}))
        fresh = {"measurement_age_seconds": 10, "archive": {"error_class": "archive_tip_unknown"}}
        self.assertFalse(RUNTIME.should_route({"routing": fresh}))
        quiet = {"measurement_age_seconds": 7200, "archive": {"error_class": None}}
        self.assertFalse(RUNTIME.should_route({"routing": quiet, "attempts": [{"status": "time_limit"}]}))
        self.assertTrue(RUNTIME.should_route({"routing": quiet, "attempts": [{"status": "outside_replay_window"}]}))
        self.assertTrue(RUNTIME.should_route({"routing": {**quiet, "archive": {"error_class": "archive_tip_unknown"}}}))
        self.assertFalse(RUNTIME.should_route({"routing": {"measurement_age_seconds": None, "archive": {}}}))
        # A recent attempt, even a failed one that recorded nothing, defers the next.
        self.assertFalse(RUNTIME.should_route({"routing": None}, since_attempt=60))
        self.assertTrue(RUNTIME.should_route({"routing": None}, since_attempt=3600))

    def test_secret_file_is_not_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "secret"
            path.write_text('OTHER=ignored\nHELIUS_API_KEY="test-key"\n')
            self.assertEqual(RUNTIME.credentials(path), "test-key")
            path.write_text('HELIUS_API_KEY=$(touch /tmp/should-not-exist)\n')
            with self.assertRaises(ValueError):
                RUNTIME.credentials(path)


if __name__ == "__main__":
    unittest.main()
