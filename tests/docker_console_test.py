"""Persistent bootstrap and readiness checks; no Docker daemon or provider needed."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("docker_console", ROOT / "scripts/local-test.py")
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)
BINARY = ROOT / "target/debug/helius-indexer"


class PersistentConsole(unittest.TestCase):
    def test_resume_keeps_binary_and_data_and_refuses_parallel_initializer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session"
            with CONSOLE.persistent_session(path, BINARY) as (session, metadata):
                before = CONSOLE.command(session, "query", session / "index.sqlite")
                self.assertTrue(metadata["replay_verified"])
                self.assertEqual(CONSOLE.readiness(session)["coverage"], "unknown")
                with self.assertRaises(RuntimeError):
                    with CONSOLE.persistent_session(path, BINARY):
                        pass
            with CONSOLE.persistent_session(path, Path("missing-new-image-binary")) as (session, metadata):
                self.assertEqual(before, CONSOLE.command(session, "query", session / "index.sqlite"))
                self.assertEqual(metadata["transfer_count"], len(before["transfers"]))

    def test_interrupted_initial_seed_replays_without_resetting_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session"
            original = CONSOLE.command
            attempted = 0
            def interrupted(*args):
                nonlocal attempted
                attempted += 1
                if attempted == 4:
                    raise RuntimeError("injected interruption")
                return original(*args)
            with patch.object(CONSOLE, "command", side_effect=interrupted):
                with self.assertRaises(RuntimeError):
                    with CONSOLE.persistent_session(path, BINARY):
                        pass
            self.assertFalse((path / "session.json").exists())
            self.assertTrue((path / "index.sqlite").exists())
            with CONSOLE.persistent_session(path, Path("missing-new-image-binary")) as (session, metadata):
                self.assertTrue(metadata["replay_verified"])
                rows = CONSOLE.command(session, "query", session / "index.sqlite")["transfers"]
                self.assertEqual(len(rows), metadata["transfer_count"])
                self.assertEqual(len({r["transfer_id"] for r in rows}), len(rows))

    def test_health_does_not_create_missing_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "session.json").write_text(json.dumps({"coverage": "unknown"}))
            with self.assertRaises(sqlite3.Error):
                CONSOLE.readiness(path)
            self.assertFalse((path / "index.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
