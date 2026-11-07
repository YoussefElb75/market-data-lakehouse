"""Delta Lake behaviour the design relies on (Linux CI only: needs local file I/O)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdl import contracts
from mdl.transforms import gold, silver
from tests import builders

pytestmark = [pytest.mark.spark, pytest.mark.delta]


def test_bronze_is_append_only_with_schema_evolution(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "bronze_fx")
    days = builders.business_days(date(2024, 1, 1), 2)
    builders.write_csv(tmp_path / "in1", "a.csv", builders.EXR_HEADER, builders.exr_rows(days[:1]))
    first = builders.read_bronze_csv(spark, tmp_path / "in1", "fx_reference_rates")
    first.write.format("delta").save(path)

    # A later file carries an extra column: bronze accepts it (mergeSchema), never rewrites.
    header = [*builders.EXR_HEADER, "NEW_ATTR"]
    rows = [[*r, "x"] for r in builders.exr_rows(days[1:])]
    builders.write_csv(tmp_path / "in2", "b.csv", header, rows)
    second = builders.read_bronze_csv(spark, tmp_path / "in2", "fx_reference_rates")
    second.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

    table = DeltaTable.forPath(spark, path)
    ops = [r.operation for r in table.history().select("operation").collect()]
    assert sorted(ops) == ["APPEND", "WRITE"]
    df = spark.read.format("delta").load(path)
    assert "new_attr" in df.columns
    assert df.count() == 2 * len(builders.FAKE_FX)
    assert df.where("new_attr IS NULL").count() == len(builders.FAKE_FX)


def test_late_revision_merge_keeps_latest_and_history(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "silver_fx")
    t1, t2, t0 = (datetime(2024, 1, d, 16, tzinfo=UTC) for d in (2, 3, 1))
    base = spark.createDataFrame(
        [("EXR.D.USD.EUR.SP00.A", date(2024, 1, 2), 1.10, t1)],
        "series_key STRING, obs_date DATE, obs_value DOUBLE, _ingest_ts TIMESTAMP",
    )
    base.write.format("delta").save(path)
    updates = spark.createDataFrame(
        [
            ("EXR.D.USD.EUR.SP00.A", date(2024, 1, 2), 1.11, t2),  # revision, newer ingest: wins
            ("EXR.D.USD.EUR.SP00.A", date(2024, 1, 2), 1.09, t0),  # stale replay, older ingest: ignored
            ("EXR.D.USD.EUR.SP00.A", date(2024, 1, 1), 1.08, t2),  # late-arriving earlier date: inserted
        ],
        base.schema,
    )
    latest = silver.latest_per_key(updates, ["series_key", "obs_date"], ["_ingest_ts"])
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(latest.alias("s"), "t.series_key = s.series_key AND t.obs_date = s.obs_date")
        .whenMatchedUpdateAll(condition="s._ingest_ts > t._ingest_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )
    now = {r.obs_date: r.obs_value for r in spark.read.format("delta").load(path).collect()}
    assert now == {date(2024, 1, 2): 1.11, date(2024, 1, 1): 1.08}
    before = spark.read.format("delta").option("versionAsOf", 0).load(path).collect()
    assert [r.obs_value for r in before] == [1.10]


def test_contract_types_survive_delta_roundtrip(spark: SparkSession, tmp_path: Path) -> None:
    days = builders.business_days(date(2024, 1, 1), 2)
    builders.write_csv(tmp_path / "exr", "exr.csv", builders.EXR_HEADER, builders.exr_rows(days))
    fx = silver.fx_rates(builders.read_bronze_csv(spark, tmp_path / "exr", "fx_reference_rates"))
    path = str(tmp_path / "gold_fx_daily")
    gold.fx_daily(fx).write.format("delta").save(path)
    back = spark.read.format("delta").load(path)
    contract = contracts.load("fx_daily")
    assert contracts.schema_violations(contract, back.schema) == []
    assert contracts.data_violations(contract, back) == {}
    assert back.agg(F.countDistinct("currency")).collect()[0][0] == len(builders.FAKE_FX)
