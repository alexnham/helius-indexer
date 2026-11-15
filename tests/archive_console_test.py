"""The archive dashboard serves windowed queries, not the captured-console exception."""
import importlib.util
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
import unittest
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("console", ROOT / "scripts/local-test.py")
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)
os.environ.setdefault("DATABASE_URL", os.environ.get("INDEXER_TEST_DATABASE_URL", "postgres://indexer:indexer@127.0.0.1:55432/indexer"))


class ArchiveConsole(unittest.TestCase):
    def test_served_archive_query_excludes_rows_older_than_retention(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "helius-indexer").symlink_to(ROOT / "target/debug/helius-indexer")
            session = "archive_console_" + secrets.token_hex(5)
            (directory / "session.json").write_text(json.dumps({"database": session}))
            capture = json.loads((ROOT / "tests/fixtures/mainnet-transfer.json").read_text())
            capture["result"]["blockTime"] = 1
            fixture = directory / "old.json"
            fixture.write_text(json.dumps(capture))
            CONSOLE.command(directory, "ingest", session, fixture)
            self.assertGreater(len(CONSOLE.command(directory, "query", session, "--all-stored")["transfers"]), 0)
            server = CONSOLE.HTTPServer(("127.0.0.1", 0), CONSOLE.BaseHTTPRequestHandler)
            server.RequestHandlerClass = CONSOLE.handler_for(directory, server.server_port, lambda: {}, windowed=True)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
            thread.start()
            try:
                with urlopen(f"http://127.0.0.1:{server.server_port}/api/transfers", timeout=10) as response:
                    result = json.load(response)
                self.assertEqual(result["transfers"], [])
                self.assertIn("window", result)
                self.assertIn("sampling", result)
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


if __name__ == "__main__":
    unittest.main()
