"""Offline installer integrity regression tests; no release downloads."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]


class InstallerTests(unittest.TestCase):
    def test_bash_entrypoints_are_identical(self):
        self.assertEqual((ROOT / "install").read_bytes(), (ROOT / "install.sh").read_bytes())

    def test_missing_metadata_entry_tool_and_mismatch_fail_closed(self):
        for mode in ("missing", "entry", "near-entry", "mismatch", "no-sha", "valid"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                asset = "bettermq-linux-amd64"
                archive = tmp / "asset.zip"
                with zipfile.ZipFile(archive, "w") as bundle:
                    bundle.writestr(asset, "#!/bin/sh\nexit 0\n")
                digest = hashlib.sha256(archive.read_bytes()).hexdigest()
                checksum = f"{digest}  {asset}.zip\n"
                if mode == "entry":
                    checksum = checksum.replace(asset, "wrong-asset")
                if mode == "near-entry":
                    checksum = checksum.replace(".zip", "Xzip")
                if mode == "mismatch":
                    checksum = f"{'0' * 64}  {asset}.zip\n"
                (tmp / "checksums.txt").write_text(checksum)
                commands = tmp / "commands"
                commands.mkdir()
                for name in ("uname", "mkdir", "mktemp", "rm", "awk", "grep", "sed",
                             "unzip", "chmod", "mv", "ln", "cat", "dirname"):
                    (commands / name).symlink_to(shutil.which(name))
                if mode != "no-sha":
                    (commands / "sha256sum").symlink_to(shutil.which("sha256sum"))
                curl = commands / "curl"
                curl.write_text('''#!/usr/bin/python3
import os, shutil, sys
from pathlib import Path
url = sys.argv[2]
dest = sys.argv[sys.argv.index('-o') + 1]
if url.endswith('checksums.txt'):
    if os.environ['MODE'] == 'missing': sys.exit(22)
    source = 'checksums.txt'
else: source = 'asset.zip'
shutil.copyfile(Path(os.environ['FIXTURE']) / source, dest)
''')
                curl.chmod(0o700)
                env = dict(os.environ, PATH=str(commands), FIXTURE=str(tmp), MODE=mode,
                           HOME=str(tmp), BETTERMQ_INSTALL_DIR=str(tmp / "installed"),
                           BETTERMQ_BIN_DIR=str(tmp / "bin"), BETTERMQ_NO_START="1")
                result = subprocess.run(["/bin/bash", str(ROOT / "install"), "0.6.2"],
                                        env=env, capture_output=True)
                if mode == "valid":
                    self.assertEqual(result.returncode, 0, result.stderr.decode())
                else:
                    self.assertNotEqual(result.returncode, 0)
                self.assertEqual((tmp / "installed/bin/bettermq").exists(), mode == "valid")


if __name__ == "__main__":
    unittest.main()
