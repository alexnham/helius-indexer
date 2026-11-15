import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "archive-backfill.py"
SPEC = importlib.util.spec_from_file_location("archive_backfill", SCRIPT)
app = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(app)

FAKE = '''#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
cmd=sys.argv[1]; p=Path(os.environ["FAKE_STATE"])
s=json.loads(p.read_text()) if p.exists() else {"pending":2,"batches":0}
def report():
 return {"start":100,"archive_tip":101,"finalized_reference":102,"historical":{"next_pending_slot":100 if s["pending"] else None,"contiguous_through":None,"complete":s["pending"]==0,"pending_slots":s["pending"],"missing_ranges":1,"overlap_verified":False,"overlap_recovered":False,"boundary_ready":False}}
if cmd=="sample-status": print(json.dumps({"policy":"sha256-transfer-id-v1","numerator":10,"denominator":100,"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}))
elif cmd in ("sample-init","bridge-plan","bridge-status"): print(json.dumps(report()))
elif cmd=="bridge":
 pid=os.environ.get("FAKE_PID")
 if pid: Path(pid).write_text(str(os.getpid()))
 if os.environ.get("FAKE_SLEEP"): time.sleep(10)
 if os.environ.get("FAKE_FAIL"): print(os.environ.get("SOLANA_RPC_URL","secret"),file=sys.stderr); sys.exit(3)
 s["pending"]=max(0,s["pending"]-1); s["batches"]+=1; p.write_text(json.dumps(s)); r=report()
 r["source_diagnostics"]={"archive":{"response_body_bytes":5},"rpc":{"response_body_bytes_read":7}}
 print(json.dumps(r))
elif cmd=="summary": print(json.dumps({"transfer_count":3}))
else: print(json.dumps({"ok":True}))
'''


class ArchiveBackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.directory = self.root / "session"
        self.binary = self.root / "fake-indexer"
        self.binary.write_text(FAKE)
        self.binary.chmod(0o700)
        self.state = self.root / "fake-state.json"
        self.state.write_text(json.dumps({"pending": 2, "batches": 0}))
        self.env = os.environ.copy()
        os.environ.update({"DATABASE_URL": "postgres://localhost/test",
                           "INDEXER_ARCHIVE_OPT_IN": "1", "FAKE_STATE": str(self.state)})
        self.common = ["--binary", str(self.binary), "--directory", str(self.directory),
                       "--session", "sample_test"]
        self.assertEqual(app.main(["plan", *self.common, "--start-slot", "100"]), 0)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.env)
        self.tmp.cleanup()

    def manifest(self):
        return json.loads((self.directory / "archive-backfill.json").read_text())

    def test_reservation_caps_batches_and_status_is_read_only(self):
        ceiling = app.BATCH_RESERVE
        self.assertEqual(app.main(["run", *self.common, "--max-batches", "4",
                                   "--max-reserved-bytes", str(ceiling), "--min-free-bytes", "0"]), 0)
        m = self.manifest()
        self.assertEqual(m["budget"]["reserved_bytes"], ceiling)
        self.assertEqual(m["budget"]["actual_response_bytes"], 12)
        self.assertEqual(m["total_batches"], 1)
        self.assertEqual(json.loads(self.state.read_text())["batches"], 1)
        self.assertEqual(app.main(["status", *self.common]), 0)
        self.assertEqual(self.manifest()["budget"]["reserved_bytes"], ceiling)

    def test_failed_child_keeps_reservation_and_endpoint_is_not_printed(self):
        os.environ["FAKE_FAIL"] = "1"
        os.environ["SOLANA_RPC_URL"] = "https://user:secret@example.test/token"
        captured = io.StringIO()
        try:
            with contextlib.redirect_stderr(captured):
                self.assertEqual(app.main(["run", *self.common, "--max-batches", "1",
                                           "--max-reserved-bytes", str(app.BATCH_RESERVE),
                                           "--min-free-bytes", "0"]), 2)
        finally:
            os.environ.pop("FAKE_FAIL", None)
            os.environ.pop("SOLANA_RPC_URL", None)
        self.assertNotIn("secret@example.test", captured.getvalue())
        m = self.manifest()
        self.assertEqual(m["budget"]["reserved_bytes"], app.BATCH_RESERVE)
        self.assertEqual(m["budget"]["unknown_actual_batches"], 1)
        self.assertEqual(m["batches"][-1]["state"], "failed_unknown")

    def test_cancel_file_prevents_work_and_reservation(self):
        (self.directory / "CANCEL").touch()
        self.assertEqual(app.main(["run", *self.common, "--max-batches", "2",
                                   "--min-free-bytes", "0"]), 0)
        self.assertEqual(self.manifest()["budget"]["reserved_bytes"], 0)
        self.assertEqual(self.manifest()["stop_reason"], "cancel_file")
        self.assertEqual(json.loads(self.state.read_text())["batches"], 0)

    def test_interrupted_batch_is_not_reported_as_max_batches(self):
        def interrupt(*_args, **_kwargs):
            raise KeyboardInterrupt
        original = app.child
        app.child = lambda binary, args, env, timeout=None: (
            interrupt() if args[0] == "bridge" else original(binary, args, env, timeout))
        try:
            self.assertEqual(app.main(["run", *self.common, "--max-batches", "2",
                                       "--min-free-bytes", "0"]), 0)
        finally:
            app.child = original
        m = self.manifest()
        self.assertEqual(m["stop_reason"], "interrupted")
        self.assertEqual(m["batches"][-1]["state"], "cancelled_unknown")
        self.assertEqual(m["budget"]["reserved_bytes"], app.BATCH_RESERVE)

    def test_timeout_terminates_and_reaps_child(self):
        pidfile = self.root / "child.pid"
        os.environ.update({"FAKE_SLEEP": "1", "FAKE_PID": str(pidfile)})
        old_timeout = app.CHILD_TIMEOUT_SECONDS
        app.CHILD_TIMEOUT_SECONDS = 3.0
        try:
            self.assertEqual(app.main(["run", *self.common, "--max-batches", "1",
                                       "--max-reserved-bytes", str(app.BATCH_RESERVE),
                                       "--min-free-bytes", "0"]), 2)
        finally:
            app.CHILD_TIMEOUT_SECONDS = old_timeout
            os.environ.pop("FAKE_SLEEP", None)
            os.environ.pop("FAKE_PID", None)
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pidfile.read_text()), 0)
        self.assertEqual(self.manifest()["budget"]["unknown_actual_batches"], 1)


if __name__ == "__main__":
    unittest.main()
