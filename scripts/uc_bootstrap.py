"""Create the environment catalog once, before the first bundle deploy.

On Databricks Free Edition the metastore has no storage root and catalogs use
Default Storage, which the catalogs REST API (and so the bundle `catalogs`
resource) cannot create: only SQL or the UI can. Schemas, volumes and
everything else are managed by the bundle.

Usage: python scripts/uc_bootstrap.py --profile mdl --env dev
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from typing import Any

ENVIRONMENTS = ("dev", "prod")


def _cli() -> str:
    exe = os.environ.get("DATABRICKS_CLI") or shutil.which("databricks")
    if exe is None:
        raise SystemExit("databricks CLI not found; set DATABRICKS_CLI")
    return exe


def _api(profile: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    cmd = [_cli(), "api", method, path, "--profile", profile]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(out.stdout or "{}")


def _warehouse(profile: str) -> str:
    warehouses = _api(profile, "get", "/api/2.0/sql/warehouses").get("warehouses", [])
    if not warehouses:
        raise SystemExit("no SQL warehouse found")
    return str(warehouses[0]["id"])


def run_sql(profile: str, warehouse: str, statement: str) -> None:
    body = {"statement": statement, "warehouse_id": warehouse, "wait_timeout": "50s"}
    res = _api(profile, "post", "/api/2.0/sql/statements", body)
    while res.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(3)
        res = _api(profile, "get", f"/api/2.0/sql/statements/{res['statement_id']}")
    state = res.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise SystemExit(f"{statement!r} failed: {res.get('status', {}).get('error', {}).get('message')}")
    print(f"[{state}] {statement}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--env", choices=ENVIRONMENTS, required=True)
    args = parser.parse_args(argv)
    catalog = f"mdl_{args.env}"
    warehouse = _warehouse(args.profile)
    run_sql(
        args.profile,
        warehouse,
        f"CREATE CATALOG IF NOT EXISTS {catalog} "
        f"COMMENT 'Market data lakehouse ({args.env}). Medallion layers as schemas.'",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
