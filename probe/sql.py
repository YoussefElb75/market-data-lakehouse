"""Run SQL statements on the workspace SQL warehouse through the Databricks CLI.

Step 0 probe helper. Authentication comes from the local CLI profile
(``--profile``); no host or credential is stored in this repository.

Usage: python probe/sql.py --profile mdl --warehouse <id> "SELECT 1" ["..."]
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


def _cli() -> str:
    exe = os.environ.get("DATABRICKS_CLI") or shutil.which("databricks")
    if exe is None:
        raise SystemExit("databricks CLI not found; set DATABRICKS_CLI")
    return exe


def _api(profile: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    cmd = [_cli(), "api", method, path, "--profile", profile]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        return {"status": {"state": "CLI_ERROR", "error": {"message": out.stderr.strip()}}}
    return json.loads(out.stdout or "{}")


def run_sql(profile: str, warehouse: str, statement: str) -> dict[str, Any]:
    """Execute one statement and wait for a terminal state."""
    res = _api(
        profile,
        "post",
        "/api/2.0/sql/statements",
        {"statement": statement, "warehouse_id": warehouse, "wait_timeout": "50s"},
    )
    while res.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(3)
        res = _api(profile, "get", f"/api/2.0/sql/statements/{res['statement_id']}")
    return dict(res)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("statements", nargs="+")
    args = parser.parse_args(argv)
    failures = 0
    for stmt in args.statements:
        res = run_sql(args.profile, args.warehouse, stmt)
        state = res.get("status", {}).get("state")
        rows = res.get("result", {}).get("data_array", [])[:5]
        err = res.get("status", {}).get("error", {}).get("message", "")
        print(f"[{state}] {stmt[:90]}")
        if rows:
            print(f"    rows: {rows}")
        if err:
            print(f"    error: {err[:300]}")
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
