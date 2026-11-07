from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mdl import quality, synth
from mdl.transforms import silver
from tests import builders

pytestmark = pytest.mark.spark
DAYS = builders.business_days(date(2024, 1, 1), 3)


def test_fx_rates_typed_and_bad_values_caught(spark: SparkSession, tmp_path: Path) -> None:
    rows = builders.exr_rows(DAYS)
    rows.append(
        ["EXR.D.USD.EUR.SP00.A", "D", "USD", "EUR", "SP00", "A", "2024-13-45", "1.1", "A", "F", "t", "USD", 4]
    )
    rows.append(
        ["EXR.D.CHF.EUR.SP00.A", "D", "CHF", "EUR", "SP00", "A", "2024-01-08", "n/a", "A", "F", "t", "CHF", 4]
    )
    builders.write_csv(tmp_path, "exr.csv", builders.EXR_HEADER, rows)
    fx = silver.fx_rates(builders.read_bronze_csv(spark, tmp_path, "fx_reference_rates"))

    assert dict(fx.dtypes)["obs_date"] == "date"
    assert dict(fx.dtypes)["obs_value"] == "double"
    clean = quality.clean(fx, quality.FX_RULES)
    assert clean.count() == len(DAYS) * len(builders.FAKE_FX)
    bad = quality.quarantine(fx, quality.FX_RULES, "silver.fx_rates").collect()
    assert sorted(tuple(r.failed_rules) for r in bad) == [("numeric_value",), ("valid_date",)]
    assert all(r._source_file.endswith("exr.csv") for r in bad)


def test_yield_curve_maps_curve_and_tenor(spark: SparkSession, tmp_path: Path) -> None:
    builders.write_csv(tmp_path, "yc.csv", builders.YC_HEADER, builders.yc_rows(DAYS[:1]))
    yc = silver.yield_curve(builders.read_bronze_csv(spark, tmp_path, "yield_curve_aaa"))
    got = {(r.curve, r.tenor): r.tenor_years for r in yc.collect()}
    assert len(got) == 2 * len(silver.TENOR_YEARS)
    assert got[("AAA", "3M")] == 0.25
    assert got[("ALL", "30Y")] == 30.0
    assert quality.clean(yc, quality.YIELD_RULES).count() == yc.count()


def test_estr_handles_conf_status_and_units(spark: SparkSession, tmp_path: Path) -> None:
    builders.write_csv(tmp_path, "est.csv", builders.EST_HEADER, builders.est_rows(DAYS))
    est = silver.estr(builders.read_bronze_csv(spark, tmp_path, "estr"))
    measures = {r.measure: (r.unit, r.unit_mult) for r in est.collect()}
    assert measures == {"rate_pct": ("PC", 0), "volume": ("EUR", 6)}
    assert quality.clean(est, quality.ESTR_RULES).count() == 2 * len(DAYS)


def test_latest_per_key_keeps_revision(spark: SparkSession) -> None:
    t1, t2 = datetime(2030, 1, 2, 16, tzinfo=UTC), datetime(2030, 1, 3, 16, tzinfo=UTC)
    df = spark.createDataFrame(
        [
            ("S", date(2030, 1, 2), 1.00, t1),
            ("S", date(2030, 1, 2), 1.01, t2),
            ("S", date(2030, 1, 1), 0.9, t1),
        ],
        "series_key STRING, obs_date DATE, obs_value DOUBLE, _ingest_ts TIMESTAMP",
    )
    out = silver.latest_per_key(df, ["series_key", "obs_date"], ["_ingest_ts"])
    assert {(r.obs_date.day, r.obs_value) for r in out.collect()} == {(2, 1.01), (1, 0.9)}


def test_scd2_versions_only_on_tracked_change(spark: SparkSession) -> None:
    changes = spark.createDataFrame(
        [
            ("USD", "US dollar", 1),
            ("USD", "US dollar", 2),
            ("USD", "US dollar (renamed)", 3),
            ("JPY", "Yen", 1),
        ],
        "code STRING, name STRING, seq INT",
    )
    out = silver.scd2(changes, ["code"], "seq", ["name"]).orderBy("code", "__START_AT").collect()
    assert [(r["code"], r["name"], r["__START_AT"], r["__END_AT"]) for r in out] == [
        ("JPY", "Yen", 1, None),
        ("USD", "US dollar", 1, 3),
        ("USD", "US dollar (renamed)", 3, None),
    ]


def test_series_attributes_picks_present_columns(spark: SparkSession, tmp_path: Path) -> None:
    builders.write_csv(tmp_path, "exr.csv", builders.EXR_HEADER, builders.exr_rows(DAYS))
    attrs = silver.series_attributes(builders.read_bronze_csv(spark, tmp_path, "fx_reference_rates"))
    assert {"series_key", "title", "unit", "decimals", "_ingest_ts"} <= set(attrs.columns)
    assert attrs.count() == len(builders.FAKE_FX)


def test_trades_quality_catches_every_injected_malformed_event(spark: SparkSession, tmp_path: Path) -> None:
    cfg = synth.SynthConfig(
        start=datetime(2030, 1, 2, 9, tzinfo=UTC),
        seconds=60,
        seed=3,
        anomalies=synth.Anomalies(duplicate=0.03, out_of_order=0.03, late=0.03, malformed=0.03),
    )
    trades_raw, _, batch = builders.synth_bronze(spark, tmp_path, cfg)
    trades = silver.trades(trades_raw)
    quarantined = {
        r.event_id
        for r in quality.quarantine(trades, quality.TRADE_RULES, "t")
        .select(F.get_json_object("record", "$.event_id").alias("event_id"))
        .collect()
    }
    malformed_trades = {
        e["event_id"]
        for e in batch.stream("trade")
        if any(k.startswith("malformed") for k in batch.truth.get(e["event_id"], []))
    }
    assert malformed_trades
    assert malformed_trades <= quarantined
    deduped = silver.dedupe_trades(quality.clean(trades, quality.TRADE_RULES), streaming=False)
    assert deduped.count() == deduped.select("event_id").distinct().count()
    assert deduped.count() == len({e["event_id"] for e in batch.stream("trade")} - quarantined)


def test_book_top_metrics(spark: SparkSession, tmp_path: Path) -> None:
    cfg = synth.SynthConfig(
        start=datetime(2030, 1, 2, 9, tzinfo=UTC), seconds=5, seed=4, anomalies=synth.Anomalies(0, 0, 0, 0)
    )
    _, books_raw, _ = builders.synth_bronze(spark, tmp_path, cfg)
    top = silver.book_top(books_raw)
    rows = top.collect()
    assert len(rows) == 5 * len(synth.DEFAULT_INSTRUMENTS)
    assert all(r.best_bid < r.best_ask and r.spread > 0 and r.bid_levels == 10 for r in rows)
    assert all(abs(r.mid - (r.best_bid + r.best_ask) / 2) < 1e-12 for r in rows)
    assert quality.clean(top, quality.BOOK_RULES).count() == len(rows)
