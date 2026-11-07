"""Run the pytest suite on Databricks serverless (``mdl-tests`` job, dev only).

Used for the tests that need Spark file I/O and Delta (``-m "delta or localfs"``),
which cannot run on a Windows laptop without winutils. Scratch files go to a UC
Volume and scratch Delta tables to a dedicated schema; both are dev-only bundle
resources.

Parameters: --root <synced bundle files> --scratch-volume <path> --scratch-schema <cat.schema>
            [-- <pytest arguments>]
"""

import argparse
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True)
parser.add_argument("--scratch-volume", required=True)
parser.add_argument("--scratch-schema", required=True)
known, pytest_args = parser.parse_known_args(sys.argv[1:])
if pytest_args[:1] == ["--"]:
    pytest_args = pytest_args[1:]

sys.dont_write_bytecode = True  # the synced workspace folder is not a place for __pycache__
os.environ["MDL_TEST_SCRATCH"] = known.scratch_volume
os.environ["MDL_TEST_SCHEMA"] = known.scratch_schema
os.chdir(known.root)
sys.path.insert(0, known.root)

import pytest  # noqa: E402 - after sys.path and environment are prepared

code = pytest.main(["-p", "no:cacheprovider", "-o", "addopts=-ra --strict-markers", "tests", *pytest_args])
if code:
    raise SystemExit(int(code))
