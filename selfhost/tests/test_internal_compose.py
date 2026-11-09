"""Keep HA cluster listeners private while advertising routable peer URLs."""
import json
import os
from pathlib import Path
import subprocess
import unittest

SELFHOST = Path(__file__).resolve().parents[1]


class InternalComposeTests(unittest.TestCase):
    def test_internal_listeners_are_private_and_advertised(self):
        env = dict(os.environ, BETTERMQ_CLUSTER_SECRET="c" * 64,
                   BETTERMQ_METRICS_TOKEN="m" * 64, MINIO_ROOT_USER="ci",
                   MINIO_ROOT_PASSWORD="ci-test-secret", BETTERMQ_VERSION="ci",
                   MINIO_IMAGE="minio/minio:config-test", MINIO_MC_IMAGE="minio/mc:config-test")
        for path in SELFHOST.glob("docker-compose.ha*.yml"):
            services = json.loads(subprocess.check_output(
                ["docker", "compose", "-f", str(path), "config", "--format", "json"],
                env=env))["services"]
            for name in ("broker1", "broker2", "broker3"):
                broker = services[name]
                self.assertEqual(broker["environment"]["BETTERMQ_INTERNAL_URL"], f"http://{name}:9080")
                self.assertEqual(broker["environment"]["BETTERMQ_INTERNAL_LISTEN"], "0.0.0.0:9080")
                self.assertTrue(broker["environment"]["BETTERMQ_CLUSTER_SECRET"])
                self.assertFalse(any(port["target"] == 9080 for port in broker.get("ports", [])))


if __name__ == "__main__":
    unittest.main()
