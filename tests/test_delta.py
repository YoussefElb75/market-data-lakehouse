"""Delta Lake behaviour the design relies on (Linux CI and the Databricks test job)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdl import contracts
from mdl.transforms import gold, silver
from tests import builders
from tests.storage import DeltaStore

pytestmark = [pytest.mark.spark, pytest.mark.delta]


def test_bronze_is_append_only_with_schema_evolution(
    spark: SparkSession, scratch: Path, delta_store: DeltaStore
) -> None:
    days = builders.business_days(date(2024, 1, 1), 2)
    builders.write_csv(scratch / "in1", "a.csv", builders.EXR_HEADER, builders.exr_rows(days[:1]))
    first = builders.read_bronze_csv(spark, scratch / "in1", "fx_reference_rates")
    delta_store.write(first, "bronze_fx")

    # A later file carries an extra column: bronze accepts it (mergeSchema), never rewrites.
    header = [*builders.EXR_HEADER, "NEW_ATTR"]
    rows = [[*r, "x"] for r in builders.exr_rows(days[1:])]
    builders.write_csv(scratch / "in2", "b.csv", header, rows)
    second = builders.read_bronze_csv(spark, scratch / "in2", "fx_reference_rates")
    delta_store.write(second, "bronze_fx", mode="append", mergeSchema="true")

    history = delta_store.table("bronze_fx").history().select("version", "operation", "operationParameters")
    latest = max(history.collect(), key=lambda r: r.version)
    assert history.count() == 2
    # Delta records a DataFrame append as a WRITE in Append mode (never an update or delete).
    assert latest.operation == "WRITE"
    assert latest.operationParameters["mode"] == "Append"
    df = delta_store.read("bronze_fx")
    assert "new_attr" in df.columns
    assert df.count() == 2 * len(builders.FAKE_FX)
    assert df.where("new_attr IS NULL").count() == len(builders.FAKE_FX)


def test_late_revision_merge_keeps_latest_and_history(spark: SparkSession, delta_store: DeltaStore) -> None:
    t1, t2, t0 = (datetime(2024, 1, d, 16, tzinfo=UTC) for d in (2, 3, 1))
    base = spark.createDataFrame(
        [("EXR.D.USD.EUR.SP00.A", date(2024, 1, 2), 1.10, t1)],
        "series_key STRING, obs_date DATE, obs_value DOUBLE, _ingest_ts TIMESTAMP",
    )
    delta_store.write(base, "silver_fx")
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
        delta_store.table("silver_fx")
        .alias("t")
        .merge(latest.alias("s"), "t.series_key = s.series_key AND t.obs_date = s.obs_date")
        .whenMatchedUpdateAll(condition="s._ingest_ts > t._ingest_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )
    now = {r.obs_date: r.obs_value for r in delta_store.read("silver_fx").collect()}
    assert now == {date(2024, 1, 2): 1.11, date(2024, 1, 1): 1.08}
    before = delta_store.read("silver_fx", version=0).collect()
    assert [r.obs_value for r in before] == [1.10]


def test_contract_types_survive_delta_roundtrip(
    spark: SparkSession, scratch: Path, delta_store: DeltaStore
) -> None:
    days = builders.business_days(date(2024, 1, 1), 2)
    builders.write_csv(scratch / "exr", "exr.csv", builders.EXR_HEADER, builders.exr_rows(days))
    fx = silver.fx_rates(builders.read_bronze_csv(spark, scratch / "exr", "fx_reference_rates"))
    delta_store.write(gold.fx_daily(fx), "gold_fx_daily")
    back = delta_store.read("gold_fx_daily")
    contract = contracts.load("fx_daily")
    assert contracts.schema_violations(contract, back.schema) == []
    assert contracts.data_violations(contract, back) == {}
    assert back.agg(F.countDistinct("currency")).collect()[0][0] == len(builders.FAKE_FX)
