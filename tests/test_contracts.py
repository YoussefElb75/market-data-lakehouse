from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mdl import contracts, quality, synth
from mdl.transforms import gold, silver
from tests import builders

GOLD_TABLES = {
    "fx_daily",
    "fx_cross_rates",
    "rates_timeseries",
    "curve_snapshot_latest",
    "curve_metrics_daily",
    "ohlcv_1m",
    "ohlcv_1h",
    "dq_expectations_daily",
    "dataset_freshness",
}


def test_every_gold_table_has_a_valid_contract() -> None:
    all_contracts = contracts.load_all()
    assert set(all_contracts) == GOLD_TABLES
    for c in all_contracts.values():
        assert c.layer == "gold"
        assert c.owner
        assert c.license
        assert c.tags["layer"] == "gold"
        assert c.max_lag_hours > 0
        assert all(col.description for col in c.columns)
        if c.tags.get("source") == "ecb":
            assert "ECB statistics" in c.description + c.license


def test_contract_parser_rejects_bad_documents() -> None:
    base = {
        "apiVersion": "mdl/v1", "kind": "DataContract", "id": "gold.x", "version": 1, "owner": "o",
        "description": "d", "source": "s", "license": "l", "freshness": {"dataset": "d", "max_lag_hours": 1},
        "schema": [{"name": "a", "type": "int", "nullable": False}], "primary_key": ["a"],
    }  # fmt: skip
    assert contracts._parse(base).primary_key == ("a",)
    with pytest.raises(ValueError, match="primary key columns not in schema"):
        contracts._parse({**base, "primary_key": ["b"]})
    with pytest.raises(ValueError, match="non-nullable"):
        contracts._parse({**base, "schema": [{"name": "a", "type": "int", "nullable": True}]})
    with pytest.raises(ValueError, match="DataContract"):
        contracts._parse({**base, "kind": "Other"})


@pytest.fixture(scope="module")
def gold_outputs(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> dict[str, DataFrame]:
    days = builders.business_days(date(2024, 1, 1), 3)
    root = tmp_path_factory.mktemp("contracts")
    builders.write_csv(root / "exr", "exr.csv", builders.EXR_HEADER, builders.exr_rows(days))
    builders.write_csv(root / "yc", "yc.csv", builders.YC_HEADER, builders.yc_rows(days))
    builders.write_csv(root / "est", "est.csv", builders.EST_HEADER, builders.est_rows(days))
    fx = silver.fx_rates(builders.read_bronze_csv(spark, root / "exr", "fx_reference_rates"))
    yc = silver.yield_curve(builders.read_bronze_csv(spark, root / "yc", "yield_curve_aaa"))
    est = silver.estr(builders.read_bronze_csv(spark, root / "est", "estr"))
    cfg = synth.SynthConfig(start=datetime(2030, 1, 2, 9, tzinfo=UTC), seconds=120, seed=8)
    trades_raw, _, _ = builders.synth_bronze(spark, root / "synth", cfg)
    trades = silver.dedupe_trades(
        quality.clean(silver.trades(trades_raw), quality.TRADE_RULES), streaming=False
    )
    bars = gold.ohlcv(trades)
    log = spark.createDataFrame(
        [(datetime(2030, 1, 2, tzinfo=UTC), "flow_progress",
          '{"flow_progress":{"data_quality":{"expectations":'
          '[{"name":"r","dataset":"d","passed_records":1,"failed_records":0}]}}}')],
        "timestamp TIMESTAMP, event_type STRING, details STRING",
    )  # fmt: skip
    manifests = spark.createDataFrame(
        [("b1", "ecb", "estr", "2030-01-02T16:00:00+00:00", 200, 10, "2030-01-02")],
        "batch_id STRING, source STRING, dataset STRING, fetched_at STRING, http_status INT, "
        "row_count BIGINT, last_period STRING",
    )
    now = F.lit(datetime(2030, 1, 2, 18, tzinfo=UTC))
    return {
        "fx_daily": gold.fx_daily(fx),
        "fx_cross_rates": gold.fx_cross_rates(fx),
        "rates_timeseries": gold.rates_timeseries(yc, est),
        "curve_snapshot_latest": gold.curve_snapshot_latest(yc),
        "curve_metrics_daily": gold.curve_metrics_daily(yc),
        "ohlcv_1m": bars,
        "ohlcv_1h": gold.rollup_bars(bars, "1 hour"),
        "dq_expectations_daily": gold.dq_expectations_daily(log),
        "dataset_freshness": gold.dataset_freshness(manifests, {"estr": 30.0}, now=now),
    }


@pytest.mark.spark
@pytest.mark.parametrize("table", sorted(GOLD_TABLES))
def test_transform_output_honours_contract(table: str, gold_outputs: dict[str, DataFrame]) -> None:
    contract = contracts.load(table)
    df = gold_outputs[table]
    assert contracts.schema_violations(contract, df.schema) == []
    assert contracts.data_violations(contract, df) == {}


@pytest.mark.spark
def test_violations_are_detected(spark: SparkSession) -> None:
    contract = contracts.load("fx_daily")
    bad = spark.createDataFrame(
        [
            (date(2030, 1, 2), "USD", "EUR", 1.2, 1 / 1.2, "A"),
            (date(2030, 1, 2), "USD", "EUR", -1.0, -1.0, "A"),
            (None, "JPY", "USD", 150.0, 1 / 150, "A"),
        ],
        "obs_date DATE, currency STRING, base_currency STRING, rate_per_eur DOUBLE, eur_per_unit DOUBLE, "
        "obs_status STRING",
    )
    assert contracts.data_violations(contract, bad) == {
        "not_null:obs_date": 1,
        "rule:positive_rate": 1,
        "rule:base_is_eur": 1,
        "primary_key:duplicates": 1,
    }
    renamed = bad.withColumnRenamed("obs_status", "status").withColumn(
        "rate_per_eur", F.col("rate_per_eur").cast("float")
    )
    problems = contracts.schema_violations(contract, renamed.schema)
    assert "missing column obs_status" in problems
    assert "unexpected column status" in problems
    assert "column rate_per_eur: expected double, got float" in problems


def test_table_comment_mentions_contract() -> None:
    comment = contracts.table_comment(contracts.load("ohlcv_1m"))
    assert "contract gold.ohlcv_1m v1.0.0" in comment
