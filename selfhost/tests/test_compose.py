"""Deployment config and failure-path tests (Docker Compose CLI required)."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SELFHOST = Path(__file__).resolve().parents[1]
ENV = dict(os.environ, BETTERMQ_CLUSTER_SECRET="c" * 64,
           BETTERMQ_METRICS_TOKEN="m" * 64, MINIO_ROOT_USER="ci",
           MINIO_ROOT_PASSWORD="ci-secret-not-default", BETTERMQ_VERSION="ci")


def config(name):
    return json.loads(subprocess.check_output(
        ["docker", "compose", "-f", str(SELFHOST / name), "config", "--format", "json"],
        env=ENV))


class ComposeTests(unittest.TestCase):
    def test_fleet_bootstrap_gates_on_liveness(self):
        for name in ("docker-compose.ha-fleet.yml", "docker-compose.ha-slate-fleet.yml"):
            services = config(name)["services"]
            for dependency in services["fleet"]["depends_on"]:
                self.assertIn("http://localhost:8080/healthz", services[dependency]["healthcheck"]["test"])

    def test_slate_init_failure_is_fatal(self):
        for name in ("docker-compose.slate.yml", "docker-compose.ha-slate.yml",
                     "docker-compose.ha-slate-fleet.yml"):
            with self.subTest(name=name):
                services = config(name)["services"]
                self.assertFalse(services["minio"].get("ports"))
                broker = services.get("broker1", services.get("bettermq"))
                self.assertEqual(broker["depends_on"]["minio-init"]["condition"],
                                 "service_completed_successfully")
                with tempfile.TemporaryDirectory() as tmp:
                    mc = Path(tmp) / "mc"
                    mc.write_text('#!/bin/sh\n[ "$1" != mb ]\n')
                    mc.chmod(0o700)
                    # `compose config` re-escapes dollars for round-tripping.
                    entrypoint = [arg.replace("$$", "$") for arg in services["minio-init"]["entrypoint"]]
                    result = subprocess.run(entrypoint,
                                            env=dict(ENV, PATH=tmp + ":" + os.environ["PATH"]))
                    self.assertNotEqual(result.returncode, 0)
                    mc.write_text('#!/bin/sh\nexit 0\n')
                    result = subprocess.run(entrypoint,
                                            env=dict(ENV, PATH=tmp + ":" + os.environ["PATH"]))
                    self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
