"""Lakeflow Jobs entry point for ingestion tasks: ``ingest.py <mdl CLI arguments>``.

Example task parameters: ``ingest-ecb --landing /Volumes/mdl_dev/landing/raw``.
"""

import sys

from mdl.cli import main

code = main(sys.argv[1:])
if code:
    raise SystemExit(code)
