"""Persistent bootstrap and readiness checks; no Docker daemon or provider needed."""
import importlib.util
import json
import os
import secrets
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("docker_console", ROOT / "scripts/local-test.py")
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)
BINARY = ROOT / "target/debug/helius-indexer"
# The local test container; never a hosted database.
os.environ.setdefault("DATABASE_URL", os.environ.get(
    "INDEXER_TEST_DATABASE_URL", "postgres://indexer:indexer@127.0.0.1:55432/indexer"))


def fresh_database():
    return patch.dict(os.environ, {"INDEXER_DB_SESSION": "pytest_" + secrets.token_hex(6)})


class PersistentConsole(unittest.TestCase):
    def test_resume_keeps_binary_and_data_and_refuses_parallel_initializer(self):
        with tempfile.TemporaryDirectory() as directory, fresh_database():
            path = Path(directory) / "session"
            with CONSOLE.persistent_session(path, BINARY) as (session, metadata):
                before = CONSOLE.command(session, "query", metadata["database"])
                self.assertTrue(metadata["replay_verified"])
                self.assertEqual(CONSOLE.readiness(session)["coverage"], "unknown")
                with self.assertRaises(RuntimeError):
                    with CONSOLE.persistent_session(path, BINARY):
                        pass
            with CONSOLE.persistent_session(path, Path("missing-new-image-binary")) as (session, metadata):
                self.assertEqual(before, CONSOLE.command(session, "query", metadata["database"]))
                self.assertEqual(metadata["transfer_count"], len(before["transfers"]))

    def test_interrupted_initial_seed_replays_without_resetting_existing_output(self):
        with tempfile.TemporaryDirectory() as directory, fresh_database():
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
            partial = original(path, "query", os.environ["INDEXER_DB_SESSION"])["transfers"]
            self.assertTrue(partial)
            with CONSOLE.persistent_session(path, Path("missing-new-image-binary")) as (session, metadata):
                self.assertTrue(metadata["replay_verified"])
                rows = CONSOLE.command(session, "query", metadata["database"])["transfers"]
                self.assertEqual(len(rows), metadata["transfer_count"])
                self.assertEqual(len({r["transfer_id"] for r in rows}), len(rows))

    def test_health_requires_a_seeded_session_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "session.json").write_text(json.dumps({"coverage": "unknown"}))
            # No database name recorded: readiness fails before any connection is made.
            with self.assertRaises(KeyError):
                CONSOLE.readiness(path)


if __name__ == "__main__":
    unittest.main()
