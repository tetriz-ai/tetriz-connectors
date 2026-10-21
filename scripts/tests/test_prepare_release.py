# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
import importlib.util
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("release", Path(__file__).parents[1] / "prepare-release.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleaseSafetyTests(unittest.TestCase):
    def test_only_stable_versions_are_accepted(self):
        for version in ("../outside", "v1.2.3; touch nope", "v01.2.3", "1.2.3", "v1.2.3-rc1"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                release.check_source(version)

    def test_non_main_and_dirty_sources_are_rejected(self):
        with patch.object(release, "run", return_value="initial-import"), self.assertRaises(ValueError):
            release.check_source("v0.1.0")
        with patch.object(release, "run", side_effect=["main", " M README.md"]), self.assertRaises(ValueError):
            release.check_source("v0.1.0")

    def test_archives_are_reproducible_with_legal_files_and_executable(self):
        files = [("tetriz-connectors", b"binary", True), ("LICENSE", b"license", False),
                 ("third_party/licenses/upstream/LICENSE", b"upstream", False)]
        with tempfile.TemporaryDirectory() as folder:
            for extension in (".tar.gz", ".zip"):
                first, second = [Path(folder) / (name + extension) for name in ("one", "two")]
                release.archive(first, files)
                release.archive(second, files)
                self.assertEqual(first.read_bytes(), second.read_bytes())
                with self.assertRaises(FileExistsError):
                    release.archive(first, files)
                if extension == ".tar.gz":
                    with tarfile.open(first) as output:
                        self.assertEqual(output.getnames(), [entry[0] for entry in files])
                        self.assertEqual(output.getmember("tetriz-connectors").mode, 0o755)
                else:
                    with zipfile.ZipFile(first) as output:
                        self.assertEqual(output.namelist(), [entry[0] for entry in files])
                        self.assertEqual(output.getinfo("tetriz-connectors").external_attr >> 16, 0o100755)


if __name__ == "__main__":
    unittest.main()
