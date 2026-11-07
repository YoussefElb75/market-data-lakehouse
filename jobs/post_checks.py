"""Post-update job task: data-quality KPIs, contract checks and a run summary.

1. ``gold.dq_expectations_daily`` from the pipeline event log (expectation
   pass/fail counts per dataset and rule, per day).
2. Every gold table is checked against its data contract (schema, non-null
   columns, primary key, quality rules). Results are appended to
   ``ops.contract_results``, and the task fails if any contract is broken.
3. Row counts per layer and quarantine counts per rule are printed for the run log.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdl import contracts
from mdl.transforms import gold


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--event-log-table", default="silver.fx_rates", help="any table of the pipeline")
    args = parser.parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    cat = args.catalog
    run_at = datetime.now(UTC)

    log = spark.sql(
        f"SELECT timestamp, event_type, details FROM event_log(TABLE({cat}.{args.event_log_table}))"
    )
    dq_table = f"{cat}.gold.dq_expectations_daily"
    gold.dq_expectations_daily(log).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        dq_table
    )
    dq_contract = contracts.load("dq_expectations_daily")
    spark.sql(f"COMMENT ON TABLE {dq_table} IS {_sql_str(contracts.table_comment(dq_contract))}")

    results = []
    broken = 0
    for name, contract in sorted(contracts.load_all().items()):
        table = f"{cat}.gold.{name}"
        df = spark.read.table(table)
        schema_problems = contracts.schema_violations(contract, df.schema)
        data_problems = contracts.data_violations(contract, df) if not schema_problems else {}
        rows = df.count()
        ok = not schema_problems and not data_problems
        broken += 0 if ok else 1
        results.append(
            (
                run_at,
                contract.id,
                contract.version,
                table,
                rows,
                ok,
                schema_problems,
                json.dumps(data_problems),
            )
        )
    spark.createDataFrame(
        results,
        "checked_at TIMESTAMP, contract STRING, version STRING, table_name STRING, row_count BIGINT, "
        "passed BOOLEAN, schema_problems ARRAY<STRING>, data_problems STRING",
    ).write.mode("append").saveAsTable(f"{cat}.ops.contract_results")

    summary = {
        "contracts": {
            r[1]: {"rows": r[4], "passed": r[5], "schema": r[6], "data": json.loads(r[7])} for r in results
        },
        "layers": _layer_counts(spark, cat),
        "quarantine_by_rule": _quarantine_counts(spark, cat),
    }
    print(json.dumps(summary, indent=2, default=str))
    return 1 if broken else 0


def _sql_str(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _layer_counts(spark: SparkSession, cat: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for layer in ("bronze", "silver", "gold", "ops"):
        tables = spark.sql(f"SHOW TABLES IN {cat}.{layer}").where("NOT isTemporary").collect()
        out[layer] = {t.tableName: spark.read.table(f"{cat}.{layer}.{t.tableName}").count() for t in tables}
    return out


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
