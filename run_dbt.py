"""Dataproc runner for the StableFlow dbt-spark project.

Mirrors the working Solana-Data runner: install dbt from an offline wheel bundle,
unpack the project via `--archives=...zip#dbt_spark_v2`, then run against the
batch's Spark session with the Iceberg catalog configured at submit time.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tarfile

WHEELS = os.environ.get("SF_DBT_WHEELS", "gs://term-flow/wheels/wheels.tar.gz")
PROJECT_DIR = os.environ.get("SF_DBT_PROJECT_DIR", "dbt_spark_v2")


def main() -> int:
    subprocess.run(["gsutil", "cp", WHEELS, "/tmp/wheels.tar.gz"], check=True)
    with tarfile.open("/tmp/wheels.tar.gz") as tf:
        tf.extractall("/tmp")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--no-index",
            "--find-links=/tmp/wheels",
            "dbt-core==1.8.0",
            "dbt-spark==1.8.0",
        ],
        check=True,
    )
    # Optional first arg selects the dbt command; default stays `run` so the
    # documented Dataproc submit needs no change. `-- test` runs data tests.
    args = sys.argv[1:]
    command = "run"
    if args and args[0] in {"run", "test", "build", "compile", "parse"}:
        command, args = args[0], args[1:]
    os.chdir(PROJECT_DIR)
    return subprocess.run(
        [
            "dbt",
            command,
            "--target",
            "serverless",
            "--profiles-dir",
            ".",
            "--project-dir",
            ".",
            *args,
        ],
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
