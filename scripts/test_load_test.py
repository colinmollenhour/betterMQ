"""Offline correctness/exit-code regression tests for the manual load test."""
import argparse
import contextlib
import io
import unittest
from unittest.mock import patch

import load_test


class LoadTestTests(unittest.TestCase):
    def run_mode(self, accepted, unique, errors=0):
        args = argparse.Namespace(sink_url="http://sink", base="http://broker", token="test",
                                  ordered=False, workers=1, lanes=1, drain_timeout=0,
                                  broker_pid=None)
        stats = dict(unique=unique, count=10, inversions=0, errors=errors)
        with patch.object(load_test, "http_json"), \
             patch.object(load_test, "ingest", return_value=(1, accepted)), \
             patch.object(load_test, "wait_delivered", return_value=(stats, 0)), \
             contextlib.redirect_stdout(io.StringIO()):
            return load_test.run_mode(args, "publish", 10, None)

    def test_incomplete_and_sink_error_outcomes_fail(self):
        for accepted, unique, errors in [(9, 10, 0), (10, 9, 0), (10, 10, 1), (10, 0, 0)]:
            result = self.run_mode(accepted, unique, errors)
            self.assertFalse(result["passed"])
            self.assertTrue(result["failures"])
        self.assertTrue(self.run_mode(10, 10)["passed"])

    def test_timeout_and_stall_return_incomplete_stats(self):
        with contextlib.redirect_stdout(io.StringIO()):
            stats, _ = load_test.wait_delivered("http://sink", 10, 0, None)
            self.assertLess(stats.get("unique", 0), 10)
            with patch.object(load_test, "http_json", return_value=(200, {"unique": 1, "count": 1})), \
                 patch.object(load_test.time, "sleep"), \
                 patch.object(load_test.time, "time", side_effect=[0, 0, 0, 46, 46, 46]):
                stats, elapsed = load_test.wait_delivered("http://sink", 10, 100, None)
            self.assertEqual(stats["unique"], 1)
            self.assertEqual(elapsed, 46)

    def test_main_returns_nonzero_after_printing_failed_summary(self):
        argv = ["load_test", "--base", "http://broker", "--token", "test", "--sink-url", "http://sink", "--modes", "publish"]
        with patch("sys.argv", argv), \
             patch.object(load_test, "http_json", return_value=(200, {})), \
             patch.object(load_test, "run_mode", return_value={"passed": False}), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(load_test.main(), 1)
            self.assertIn("SUMMARY", output.getvalue())


if __name__ == "__main__":
    unittest.main()
