"""Local console acceptance checks using the real Rust CLI, never a provider."""
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("local_test", ROOT / "scripts/local-test.py")
CONSOLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONSOLE)


class LocalConsole(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.session = Path(cls.directory.name)
        cls.metadata = CONSOLE.seed(cls.session, ROOT / "target/debug/helius-indexer")
        cls.server = CONSOLE.HTTPServer(("127.0.0.1", 0), CONSOLE.BaseHTTPRequestHandler)
        cls.server.RequestHandlerClass = CONSOLE.handler_for(cls.session, cls.server.server_port)
        cls.thread = threading.Thread(target=cls.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.directory.cleanup()

    def get(self, path):
        with urlopen(self.url + path, timeout=10) as response:
            return json.load(response)

    def test_seed_replay_and_exact_captured_values(self):
        session = self.get("/api/session")
        self.assertTrue(session["replay_verified"])
        self.assertEqual(session["coverage"], "unknown")
        self.assertGreater(session["transfer_count"], 100)
        result = self.get("/api/transfers")
        rows = {r["transfer_id"]: r for r in result["transfers"]}
        self.assertEqual(len(rows), session["transfer_count"])
        expected = json.loads((ROOT / "tests/fixtures/mainnet-expected.json").read_text())
        for row in expected:
            self.assertEqual(rows[row["transfer_id"]]["amount_raw"], row["amount_raw"])
            self.assertEqual(rows[row["transfer_id"]]["fee_raw"], "0")
        filtered = self.get("/api/transfers?mint=" + CONSOLE.USDC)
        self.assertTrue(filtered["transfers"])
        self.assertTrue(all(r["mint"] == CONSOLE.USDC for r in filtered["transfers"]))
        self.assertTrue(any(r["fee_raw"] is None for r in rows.values()))

    def test_http_boundaries_do_not_expose_files_or_execute_commands(self):
        for path, status in [("/api/transfers?mint=invalid", 400),
                             ("/api/transfers?mint=" + "a" * 32, 400),
                             ("/api/transfers?mint=a&mint=b", 400),
                             ("/api/transfers?a=1&b=2&c=3&d=4&e=5", 400),
                             ("/../.env", 404)]:
            with self.assertRaises(HTTPError) as error:
                self.get(path)
            self.assertEqual(error.exception.code, status)
        with self.assertRaises(HTTPError) as error:
            urlopen(Request(self.url, headers={"Host": "foreign.example"}), timeout=5)
        self.assertEqual(error.exception.code, 403)
        with self.assertRaises(HTTPError) as error:
            urlopen(Request(self.url + "/api/transfers", data=b"write", method="POST"), timeout=5)
        self.assertEqual(error.exception.code, 501)

    def test_console_is_read_only_and_local(self):
        with urlopen(self.url, timeout=5) as response:
            html = response.read().decode()
            self.assertIn("Coverage is unknown", html)
            self.assertIn("BigInt", html)
            self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
        self.assertEqual(self.server.server_address[0], "127.0.0.1")
        before = CONSOLE.command(self.session, "query", self.session / "index.sqlite")
        self.get("/api/session")
        self.get("/api/transfers")
        after = CONSOLE.command(self.session, "query", self.session / "index.sqlite")
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
