"""Run the pytest suite on Databricks serverless (``mdl-tests`` job, dev only).

Used for the tests that need Spark file I/O and Delta (``-m "delta or localfs"``),
which cannot run on a Windows laptop without winutils.

The scratch schema and its Volume exist only while the job runs: they are
created on demand here and dropped (CASCADE) at the end, whatever the outcome.

Parameters: --root <synced bundle files> --scratch-volume /Volumes/<cat>/<schema>/<volume>
            --scratch-schema <cat>.<schema> [-- <pytest arguments>]
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True)
parser.add_argument("--scratch-volume", required=True)
parser.add_argument("--scratch-schema", required=True)
known, pytest_args = parser.parse_known_args(sys.argv[1:])
if pytest_args[:1] == ["--"]:
    pytest_args = pytest_args[1:]

parts = known.scratch_volume.strip("/").split("/")
if len(parts) != 4 or parts[0] != "Volumes" or f"{parts[1]}.{parts[2]}" != known.scratch_schema:
    raise SystemExit("--scratch-volume must be /Volumes/<catalog>/<schema>/<volume> inside --scratch-schema")
volume = f"{known.scratch_schema}.{parts[3]}"

spark = SparkSession.builder.getOrCreate()
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {known.scratch_schema} "
    "COMMENT 'Dev-only scratch for the mdl-tests job; dropped at the end of each run.'"
)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume} COMMENT 'Scratch files for the mdl-tests job.'")

sys.dont_write_bytecode = True  # the synced workspace folder is not a place for __pycache__
os.environ["MDL_TEST_SCRATCH"] = known.scratch_volume
os.environ["MDL_TEST_SCHEMA"] = known.scratch_schema
os.chdir(known.root)
sys.path.insert(0, known.root)

import pytest  # noqa: E402 - after sys.path and environment are prepared

try:
    code = pytest.main(
        ["-p", "no:cacheprovider", "-o", "addopts=-ra --strict-markers", "tests", *pytest_args]
    )
finally:
    spark.sql(f"DROP SCHEMA IF EXISTS {known.scratch_schema} CASCADE")
    print(f"scratch schema {known.scratch_schema} dropped")
if code:
    raise SystemExit(int(code))
