#!/usr/bin/env python3
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
"""Build local release archives; never create tags, releases, or upload anything."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent
TARGETS = [(system, arch) for system in ("linux", "darwin", "windows") for arch in ("amd64", "arm64")]
GO_VERSION = "go1.27.3"


def run(*args, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True).strip()


def check_source(version):
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
        raise ValueError("version must be a stable semver tag, for example v0.1.0")
    if run("git", "branch", "--show-current") != "main":
        raise ValueError("prepare the release from main")
    if run("git", "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("commit or remove local changes before preparing the release")
    return run("git", "rev-parse", "HEAD")


def archive(path, files):
    """Use fixed timestamps and permissions; include exactly the supplied files."""
    if path.suffix == ".zip":
        with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as output:
            for name, data, executable in files:
                info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                info.external_attr = (0o100755 if executable else 0o100644) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                output.writestr(info, data)
    else:
        # tarfile's gzip wrapper uses today's timestamp; use GzipFile explicitly.
        import gzip

        with path.open("xb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w") as output:
                for name, data, executable in files:
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    info.mode = 0o755 if executable else 0o644
                    info.mtime = 0
                    output.addfile(info, io.BytesIO(data))


def prepare(version):
    commit = check_source(version)
    destination = ROOT / "dist" / version
    # Fail closed instead of deleting an existing release or unknown local output.
    destination.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, CGO_ENABLED="0", GOTOOLCHAIN=GO_VERSION)
    actual_go = run("go", "env", "GOVERSION", env=env)
    if actual_go != GO_VERSION:
        raise ValueError(f"expected {GO_VERSION}, got {actual_go}")
    source = {"version": version, "commit": commit, "go": actual_go, "targets": TARGETS}
    source_bytes = (json.dumps(source, indent=2) + "\n").encode()
    legal_paths = [ROOT / name for name in ("LICENSE", "NOTICE", "README.md")]
    legal_paths += sorted(path for path in (ROOT / "third_party").rglob("*") if path.is_file())
    legal = [(str(path.relative_to(ROOT)), path.read_bytes(), False) for path in legal_paths]
    legal.append(("SOURCE.json", source_bytes, False))
    outputs = []
    for system, arch in TARGETS:
        binary = destination / ("tetriz-connectors.exe" if system == "windows" else "tetriz-connectors")
        target_env = dict(env, GOOS=system, GOARCH=arch)
        subprocess.run(
            ["go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags",
             f"-s -w -X main.version={version}", "-o", str(binary), "./cmd/tetriz-connectors"],
            cwd=ROOT, env=target_env, check=True,
        )
        suffix = ".zip" if system == "windows" else ".tar.gz"
        output = destination / f"tetriz-connectors_{version[1:]}_{system}_{arch}{suffix}"
        archive(output, [(binary.name, binary.read_bytes(), True)] + legal)
        binary.unlink()
        outputs.append(output)
    # Detect a source mutation during the build before declaring the output ready.
    if check_source(version) != commit:
        raise ValueError("source changed during preparation; discard this incomplete output")
    (destination / "SOURCE.json").write_bytes(source_bytes)
    outputs.append(destination / "SOURCE.json")
    (destination / "checksums.txt").write_text("".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(outputs)
    ))
    print(f"Prepared {destination}; no tag, GitHub release, or container was published.")
    print("Run the local CI and follow docs/releasing.md before publishing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="stable release tag, e.g. v0.1.0")
    arguments = parser.parse_args()
    try:
        prepare(arguments.version)
    except (ValueError, FileExistsError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Release preparation failed: {error}\n")
