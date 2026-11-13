"""Offline regression tests for interleaved benchmark resume bookkeeping."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.out = self.root / "out"
        self.log = self.root / "calls"
        self.bench = self.root / "bench"
        self.bench.write_text('''#!/usr/bin/env python3
import os
from pathlib import Path
import sys
args = sys.argv[1:]
if args[0] == "run":
    out = Path(args[args.index("--out") + 1])
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text("{}")
    side = out.parent.name
    with open(os.environ["AB_TEST_LOG"], "a") as log:
        log.write(side + "\\n")
    if side == os.environ.get("AB_TEST_FAIL"):
        sys.exit(1)
''')
        self.bench.chmod(0o755)
        self.env = {**os.environ, "BENCH": str(self.bench), "AB_TEST_LOG": str(self.log)}

    def run_ab(self):
        subprocess.run(
            ["bash", str(Path(__file__).with_name("ab.sh")),
             "--a", str(self.bench), "--b", str(self.bench),
             "--out", str(self.out), "--trials", "1", "--scenarios", "idle"],
            env=self.env, check=True, capture_output=True, text=True,
        )

    def test_partial_results_are_rerun_and_completed_invocations_are_skipped(self):
        partial = self.out / "runs/a/t0-idle"
        partial.mkdir(parents=True)
        (partial / "results.json").write_text("{}")
        self.run_ab()
        self.assertEqual(self.log.read_text().splitlines(), ["a", "b"])
        self.run_ab()
        self.assertEqual(self.log.read_text().splitlines(), ["a", "b"])

    def test_failed_invocations_do_not_get_completion_markers(self):
        self.env["AB_TEST_FAIL"] = "a"
        self.run_ab()
        self.assertFalse((self.out / "runs/a/t0-idle/.ab-complete").exists())
        self.run_ab()
        self.assertEqual(self.log.read_text().splitlines(), ["a", "b", "a"])


if __name__ == "__main__":
    unittest.main()
