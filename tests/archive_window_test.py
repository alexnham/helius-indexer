"""Offline tests: unknown/skipped time never becomes zero or verified coverage."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("archive_window", Path(__file__).resolve().parent.parent / "scripts/archive_window.py")
WINDOW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WINDOW)


class ArchiveWindow(unittest.TestCase):
    def test_observed_times_select_conservative_epoch_before_cutoff(self):
        calls = []
        def rpc(method, args):
            calls.append((method, args))
            return {"getGenesisHash": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                    "getSlot": 10000, "getEpochSchedule": {"slotsPerEpoch": 1000}}.get(method, 2_000_000 - (10000 - args[0]) * 100 if method == "getBlockTime" else None)
        result = WINDOW.discover_window(1, rpc=rpc, now=2_000_000)
        self.assertEqual(result["start_slot"], 8000)
        self.assertEqual(result["cutoff_unix_seconds"], 1_913_600)
        self.assertEqual(result["boundary_status"], "conservative_time_probe_not_verified_coverage")
        self.assertLess(len(calls), 20)

    def test_missing_times_remain_unknown_and_are_bounded(self):
        def rpc(method, args):
            return {"getGenesisHash": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d", "getSlot": 10000,
                    "getEpochSchedule": {"slotsPerEpoch": 1000}}.get(method)
        with self.assertRaisesRegex(ValueError, "block_time_unavailable"):
            WINDOW.discover_window(30, rpc=rpc, now=3_000_000)

    def test_untrusted_reference_and_network_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "mainnet_required"):
            WINDOW.discover_window(1, rpc=lambda *_: "other", now=2_000_000)
        with self.assertRaisesRegex(ValueError, "invalid_history_days"):
            WINDOW.discover_window(31)
        with self.assertRaisesRegex(ValueError, "rpc_https_required"):
            WINDOW.Rpc("http://localhost")


if __name__ == "__main__":
    unittest.main()
