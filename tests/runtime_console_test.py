"""Provider console boundaries against the real Rust CLI and SQLite; no network."""
import importlib.util
import json
import contextlib
import io
import sqlite3
import os
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

    def test_runtime_bootstrap_is_empty_resumable_and_serves_recent_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            binary = ROOT / "target/debug/helius-indexer"
            RUNTIME.initialize(session, binary)
            config = RUNTIME.configuration({}, 1000)
            RUNTIME.atomic_json(session / "session.json", config)
            self.assertEqual(RUNTIME.CONSOLE.command(session, "query", session / "index.sqlite")["transfers"], [])
            capture = ROOT / "tests/fixtures/mainnet-transfer.json"
            RUNTIME.CONSOLE.command(session, "ingest", session / "index.sqlite", capture)
            before = RUNTIME.CONSOLE.command(session, "query-recent", session / "index.sqlite")
            RUNTIME.initialize(session, Path("missing-image-binary"))
            self.assertEqual(before, RUNTIME.CONSOLE.command(session, "query-recent", session / "index.sqlite"))
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
            RUNTIME.initialize(session, ROOT / "target/debug/helius-indexer")
            fixture = ROOT / "tests/fixtures/mainnet-transfer.json"
            RUNTIME.CONSOLE.command(session, "ingest", session / "index.sqlite", fixture)
            capture = json.loads(fixture.read_text())
            slot = capture["result"]["slot"]
            with sqlite3.connect(session / "index.sqlite") as db:
                db.execute("CREATE TABLE historical_inputs(slot INTEGER,attempt INTEGER,response BLOB)")
                for index in range(20):
                    response = {"result": {"transactions": [capture["result"]] if index == 0 else []}}
                    db.execute("INSERT INTO historical_inputs VALUES(?,0,?)", (slot + index, json.dumps(response).encode()))
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertFalse(comparison.compare(db, db, sample=True))
                result = json.loads(output.getvalue())
                self.assertEqual(result["blocks"], 16)
                self.assertGreater(result["expected"], 0)
                self.assertEqual((result["missing"], result["extra"], result["wrong"]), (0, 0, 0))

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
