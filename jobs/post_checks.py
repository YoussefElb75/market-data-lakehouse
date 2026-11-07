"""Post-update job task: data-quality KPIs, contract checks and a run summary.

1. ``gold.dq_expectations_daily`` is recomputed from the pipeline event log.
2. Every gold table is checked against its data contract (schema, non-null
   columns, primary key, quality rules). Results are MERGEd into
   ``ops.contract_results`` keyed by (job run, contract), and the task fails if
   any contract is broken.
3. Row counts per layer, quarantine counts per rule and per-dataset freshness
   (lag versus SLA) are printed for the run log.

Both writes are idempotent (see ``mdl.checks``): a retry of this task within the
same job run leaves the tables exactly as one run would.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdl import checks, contracts


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--job-run-id", required=True, help="{{job.run_id}}: makes retries idempotent")
    parser.add_argument("--event-log-table", default="silver.fx_rates", help="any table of the pipeline")
    args = parser.parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    cat = args.catalog

    log = spark.sql(
        f"SELECT timestamp, event_type, details FROM event_log(TABLE({cat}.{args.event_log_table}))"
    )
    dq_table = f"{cat}.gold.dq_expectations_daily"
    checks.write_dq_kpis(log, dq_table, spark)
    comment = contracts.table_comment(contracts.load("dq_expectations_daily"))
    spark.sql(f"COMMENT ON TABLE {dq_table} IS {_sql_str(comment)}")

    tables = {name: f"{cat}.gold.{name}" for name in contracts.load_all()}
    results = checks.check_contracts(spark, tables, args.job_run_id, datetime.now(UTC)).cache()
    checks.upsert_results(spark, results, f"{cat}.ops.contract_results")
    rows = results.collect()

    summary = {
        "job_run_id": args.job_run_id,
        "contracts": {
            r.contract: {"rows": r.row_count, "passed": r.passed, "schema": r.schema_problems,
                         "data": json.loads(r.data_problems)}
            for r in rows
        },
        "layers": _layer_counts(spark, cat),
        "quarantine_by_rule": _quarantine_counts(spark, cat),
        "freshness": _freshness(spark, cat),
    }  # fmt: skip
    print(json.dumps(summary, indent=2, default=str))
    broken = checks.failed([tuple(r) for r in rows])
    if broken:
        print(f"broken contracts: {broken}", file=sys.stderr)
    return 1 if broken else 0


def _sql_str(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _layer_counts(spark: SparkSession, cat: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for layer in ("bronze", "silver", "gold", "ops"):
        tables = spark.sql(f"SHOW TABLES IN {cat}.{layer}").where("NOT isTemporary").collect()
        out[layer] = {t.tableName: spark.read.table(f"{cat}.{layer}.{t.tableName}").count() for t in tables}
    return out


def _freshness(spark: SparkSession, cat: str) -> dict[str, dict[str, object]]:
    """Per-dataset lag versus SLA, so the run log shows freshness without extra table access."""
    rows = spark.read.table(f"{cat}.gold.dataset_freshness").orderBy("source", "dataset").collect()
    return {
        f"{r.source}.{r.dataset}": {
            "last_period": r.last_period,
            "lag_minutes": round(float(r.fetch_lag_hours) * 60, 1),
            "sla_hours": r.sla_hours,
            "within_sla": r.within_sla,
        }
        for r in rows
    }


def _quarantine_counts(spark: SparkSession, cat: str) -> dict[str, int]:
    rows = (
        spark.read.table(f"{cat}.ops.quarantine")
        .select("source_table", F.explode("failed_rules").alias("rule"))
        .groupBy("source_table", "rule")
        .count()
        .collect()
    )
    return {f"{r.source_table.rsplit('.', 1)[-1]}.{r.rule}": int(r["count"]) for r in rows}


code = main(sys.argv[1:])
if code:
    raise SystemExit(code)
