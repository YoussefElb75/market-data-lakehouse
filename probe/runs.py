"""Step 0 probe helper: wait for the latest run of a job and print task outputs.

Usage: python probe/runs.py --profile mdl --job-name-contains mdl-probe-job
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

TERMINAL = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}


def _cli(profile: str, *args: str) -> Any:
    exe = os.environ.get("DATABRICKS_CLI") or shutil.which("databricks")
    if exe is None:
        raise SystemExit("databricks CLI not found; set DATABRICKS_CLI")
    out = subprocess.run(
        [exe, *args, "--profile", profile, "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout or "null")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--job-name-contains", required=True)
    parser.add_argument("--timeout-s", type=int, default=520)
    args = parser.parse_args(argv)

    jobs = _cli(args.profile, "jobs", "list")
    job_ids = [j["job_id"] for j in jobs if args.job_name_contains in j["settings"]["name"]]
    if not job_ids:
        print("no matching job")
        return 2
    job_id = str(job_ids[0])
    deadline = time.monotonic() + args.timeout_s
    run: dict[str, Any] = {}
    while time.monotonic() < deadline:
        runs = _cli(args.profile, "jobs", "list-runs", "--job-id", job_id, "--limit", "1")
        if not runs:
            print("no runs yet")
            return 2
        run = runs[0]
        state = run["state"]
        print(time.strftime("%H:%M:%S"), state.get("life_cycle_state"), state.get("result_state", ""))
        if state.get("life_cycle_state") in TERMINAL:
            break
        time.sleep(15)
    else:
        print("still running at timeout; rerun this script to keep waiting")
        return 3

    detail = _cli(args.profile, "jobs", "get-run", str(run["run_id"]))
    for task in detail.get("tasks", []):
        tstate = task["state"]
        print(f"task {task['task_key']}: {tstate.get('result_state')} {tstate.get('state_message', '')[:300]}")
        out = _cli(args.profile, "jobs", "get-run-output", str(task["run_id"]))
        for key in ("logs", "error", "error_trace"):
            if out.get(key):
                print(f"  {key}:\n{str(out[key])[:3000]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
