"""Post-update checks: DQ KPIs from the event log and gold contract results.

Both writes are idempotent, so a task retry (serverless retries failed tasks
on its own) changes nothing:

* ``dq_expectations_daily`` is fully recomputed from the event log and
  overwritten;
* contract results are MERGEd on ``(job_run_id, contract)``: a retry within
  one job run replaces its own rows, and a new job run adds rows.

Targets are SQL references, a table name (``cat.ops.contract_results``) or a
path-based Delta table (``delta.`/tmp/x```), so the same code runs on
Databricks and in local tests.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from mdl import contracts
from mdl.transforms import gold

RESULTS_DDL = (
    "job_run_id STRING, checked_at TIMESTAMP, contract STRING, version STRING, table_name STRING, "
    "row_count BIGINT, passed BOOLEAN, schema_problems ARRAY<STRING>, data_problems STRING"
)


def check_contracts(
    spark: SparkSession, tables: dict[str, str], job_run_id: str, checked_at: datetime
) -> DataFrame:
    """One result row per contract. ``tables`` maps contract table name -> SQL reference."""
    rows = []
    for name, contract in sorted(contracts.load_all().items()):
        ref = tables.get(name)
        if ref is None:
            continue
        df = spark.read.table(ref) if not ref.startswith("delta.`") else spark.sql(f"SELECT * FROM {ref}")
        schema_problems = contracts.schema_violations(contract, df.schema)
        data_problems = contracts.data_violations(contract, df) if not schema_problems else {}
        rows.append(
            (
                job_run_id,
                checked_at,
                contract.id,
                contract.version,
                ref,
                df.count(),
                not schema_problems and not data_problems,
                schema_problems,
                json.dumps(data_problems, sort_keys=True),
            )
        )
    return spark.createDataFrame(rows, RESULTS_DDL)


def upsert_results(spark: SparkSession, results: DataFrame, target: str) -> None:
    """MERGE results into ``target`` keyed by (job_run_id, contract): idempotent per job run."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({RESULTS_DDL}) USING DELTA")
    view = "mdl_contract_results_src"
    results.createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON t.job_run_id = s.job_run_id AND t.contract = s.contract
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def write_dq_kpis(event_log: DataFrame, target: str, spark: SparkSession) -> DataFrame:
    """Recompute daily expectation KPIs from the event log and overwrite ``target``."""
    kpis = gold.dq_expectations_daily(event_log)
    writer = kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if target.startswith("delta.`"):
        writer.save(target[len("delta.`") : -1])
    else:
        writer.saveAsTable(target)
    return kpis


def failed(results: Iterable[tuple[object, ...]] | DataFrame) -> list[str]:
    rows = results.collect() if isinstance(results, DataFrame) else results
    return [str(r[2]) for r in rows if not r[6]]
