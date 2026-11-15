"""Console event feed: bounded, filtered and sanitized from events.jsonl; offline, no database."""
import importlib.util
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("console", ROOT / "scripts/local-test.py")
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)
RUNTIME_SPEC = importlib.util.spec_from_file_location("runtime", ROOT / "scripts/indexer-runtime.py")
RUNTIME = importlib.util.module_from_spec(RUNTIME_SPEC)
RUNTIME_SPEC.loader.exec_module(RUNTIME)
SIGNATURE = "5" * 87


def event(name, mode="live", severity="info", **fields):
    return {"timestamp_unix_ms": 1, "severity": severity, "component": "live_ingestion", "operation": "stream",
            "outcome": "success", "error_class": None, "source_kind": "laserstream", "mode": mode, "event": name, **fields}


def write(session, *lines):
    with (session / "events.jsonl").open("a") as log:
        for line in lines:
            log.write((line if isinstance(line, str) else json.dumps(line)) + "\n")


class EventFeed(unittest.TestCase):
    def test_missing_log_is_unavailable_not_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            feed = CONSOLE.recent_events(Path(directory))
            self.assertEqual((feed["available"], feed["events"]), (False, []))

    def test_newest_first_skips_noise_and_classifies_categories(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            write(session, event("connection_attempt"), event("stage_completed"), event("durable_range_completed", "historical"),
                  "thread 'main' panicked at src/live.rs", event("replay_attempt_finished", "historical"),
                  event("retention_pruned", "retention", component="storage"),
                  event("connection_failed", severity="error", outcome="failed", error_class="stream_unavailable"))
            feed = CONSOLE.recent_events(session)
            self.assertEqual([e["event"] for e in feed["events"]],
                             ["connection_failed", "retention_pruned", "replay_attempt_finished", "connection_attempt"])
            self.assertEqual(feed["unparsed_lines"], 1)
            self.assertEqual(feed["events"][0]["categories"], ["live", "errors"])
            for category, expected in [("live", ["connection_failed", "connection_attempt"]), ("backfill", ["replay_attempt_finished"]),
                                       ("storage", ["retention_pruned"]), ("errors", ["connection_failed"])]:
                self.assertEqual([e["event"] for e in CONSOLE.recent_events(session, category)["events"]], expected)

    def test_strings_other_than_identifiers_and_chain_values_are_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            write(session, event("connection_failed", severity="error", signature=SIGNATURE,
                  endpoint="https://mainnet.helius-rpc.com/?api-key=0f1e2d3c-aaaa-bbbb-cccc-1234567890ab",
                  api_key="0f1e2d3c-aaaa-bbbb-cccc-1234567890ab", message="Connection refused by host",
                  details={"nested": "value"}, slot=42))
            shown = CONSOLE.recent_events(session)["events"][0]
            self.assertEqual((shown["signature"], shown["slot"]), (SIGNATURE, 42))
            for field in ["endpoint", "api_key", "message", "details"]:
                self.assertNotIn(field, shown)
            self.assertNotIn("api-key", json.dumps(shown))

    def test_reads_only_a_bounded_tail_and_limits_results(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            write(session, *[event("connection_attempt", slot=n) for n in range(CONSOLE.EVENT_TAIL_BYTES // 100)])
            feed = CONSOLE.recent_events(session)
            self.assertTrue(feed["truncated"])
            self.assertEqual(len(feed["events"]), CONSOLE.EVENT_LIMIT)
            self.assertEqual(feed["unparsed_lines"], 0)  # the partial first line is discarded, not counted

    def test_runtime_restart_and_retry_events_reach_the_feed(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            RUNTIME.record_event(session, "runtime_started", "startup", inspect_only=False, binary="current", archive=False)
            RUNTIME.record_event(session, "worker_retrying", "retry", cycle=3, exit_code=None, resume_from=100, backoff_seconds=30)
            RUNTIME.record_event(session, "runtime_started", "startup", inspect_only=False, binary="current", archive=False)
            feed = CONSOLE.recent_events(session, "live")["events"]
            self.assertEqual([e["event"] for e in feed], ["runtime_started", "worker_retrying", "runtime_started"])
            self.assertEqual((feed[1]["cycle"], feed[1]["resume_from"], feed[1]["severity"]), (3, 100, "info"))

    def test_endpoint_validates_category(self):
        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            write(session, event("connection_attempt"))
            server = CONSOLE.HTTPServer(("127.0.0.1", 0), CONSOLE.BaseHTTPRequestHandler)
            server.RequestHandlerClass = CONSOLE.handler_for(session, server.server_port)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_port}/api/events"
                with urlopen(base + "?category=live", timeout=5) as response:
                    self.assertEqual(json.load(response)["events"][0]["event"], "connection_attempt")
                for bad in ["?category=everything", "?category=live&category=errors", "?other=1"]:
                    with self.assertRaises(HTTPError) as error:
                        urlopen(base + bad, timeout=5)
                    self.assertEqual(error.exception.code, 400)
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


if __name__ == "__main__":
    unittest.main()
