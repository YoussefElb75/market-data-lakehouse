"""post_checks must be idempotent: a retry of the same job run changes nothing."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from mdl import checks
from mdl.transforms import gold, silver
from tests import builders
from tests.storage import DeltaStore

pytestmark = [pytest.mark.spark, pytest.mark.delta]
T0 = datetime(2024, 1, 5, 18, tzinfo=UTC)


def _snapshot(df: DataFrame) -> list[tuple[object, ...]]:
    cols = [c for c in df.columns if c != "checked_at"]
    return sorted(tuple(r) for r in df.select(*cols).collect())


def _gold_fx(spark: SparkSession, scratch: Path, delta_store: DeltaStore) -> dict[str, str]:
    days = builders.business_days(date(2024, 1, 1), 2)
    builders.write_csv(scratch / "exr", "exr.csv", builders.EXR_HEADER, builders.exr_rows(days))
    fx = silver.fx_rates(builders.read_bronze_csv(spark, scratch / "exr", "fx_reference_rates"))
    delta_store.write(gold.fx_daily(fx), "fx_daily")
    return {"fx_daily": delta_store.ref("fx_daily")}


def test_contract_results_upsert_is_idempotent_per_job_run(
    spark: SparkSession, scratch: Path, delta_store: DeltaStore
) -> None:
    tables = _gold_fx(spark, scratch, delta_store)
    target = delta_store.ref("contract_results")
    delta_store.track("contract_results")

    checks.upsert_results(spark, checks.check_contracts(spark, tables, "run-1", T0), target)
    first = spark.sql(f"SELECT * FROM {target}")
    snap1, count1 = _snapshot(first), first.count()
    assert count1 == 1
    assert snap1[0][1] == "gold.fx_daily"
    assert json.loads(str(snap1[0][-1])) == {}

    # The retry runs later (new checked_at) but in the same job run: nothing is added.
    retry = checks.check_contracts(spark, tables, "run-1", T0 + timedelta(minutes=2))
    checks.upsert_results(spark, retry, target)
    again = spark.sql(f"SELECT * FROM {target}")
    assert again.count() == count1
    assert _snapshot(again) == snap1

    # A new job run is recorded next to the first one.
    checks.upsert_results(spark, checks.check_contracts(spark, tables, "run-2", T0), target)
    assert spark.sql(f"SELECT DISTINCT job_run_id FROM {target}").count() == 2


def test_dq_kpis_overwrite_is_idempotent(spark: SparkSession, delta_store: DeltaStore) -> None:
    details = json.dumps(
        {"flow_progress": {"data_quality": {"expectations": [
            {"name": "r", "dataset": "d", "passed_records": 9, "failed_records": 1}
        ]}}}
    )  # fmt: skip
    log = spark.createDataFrame(
        [(T0, "flow_progress", details)], "timestamp TIMESTAMP, event_type STRING, details STRING"
    )
    target = delta_store.ref("dq")
    delta_store.track("dq")
    checks.write_dq_kpis(log, target, spark)
    first = _snapshot(spark.sql(f"SELECT * FROM {target}"))
    checks.write_dq_kpis(log, target, spark)
    assert _snapshot(spark.sql(f"SELECT * FROM {target}")) == first
    assert len(first) == 1


def test_failed_lists_broken_contracts() -> None:
    rows: list[tuple[object, ...]] = [
        ("r", T0, "gold.a", "1", "t", 1, True, [], "{}"),
        ("r", T0, "gold.b", "1", "t", 1, False, [], "{}"),
    ]
    assert checks.failed(rows) == ["gold.b"]
