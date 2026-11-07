from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mdl import quality, synth
from mdl.sources import ecb
from mdl.sources.landing import Landing
from mdl.transforms import bronze, gold, silver
from tests import builders
from tests.fakes import FAKE_EXR_CSV, FAKE_NO_DATA, FakeResponse, FakeSession

pytestmark = pytest.mark.spark
DAYS = builders.business_days(date(2024, 1, 1), 3)


@pytest.fixture(scope="module")
def silver_fx(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> DataFrame:
    folder = tmp_path_factory.mktemp("exr")
    builders.write_csv(folder, "exr.csv", builders.EXR_HEADER, builders.exr_rows(DAYS))
    return silver.fx_rates(builders.read_bronze_csv(spark, folder, "fx_reference_rates")).cache()


@pytest.fixture(scope="module")
def silver_yc(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> DataFrame:
    folder = tmp_path_factory.mktemp("yc")
    builders.write_csv(folder, "yc.csv", builders.YC_HEADER, builders.yc_rows(DAYS))
    return silver.yield_curve(builders.read_bronze_csv(spark, folder, "yield_curve_aaa")).cache()


@pytest.fixture(scope="module")
def silver_estr(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> DataFrame:
    folder = tmp_path_factory.mktemp("est")
    builders.write_csv(folder, "est.csv", builders.EST_HEADER, builders.est_rows(DAYS))
    return silver.estr(builders.read_bronze_csv(spark, folder, "estr")).cache()


def test_fx_daily_keeps_ecb_value(silver_fx: DataFrame) -> None:
    out = {(r.obs_date, r.currency): r for r in gold.fx_daily(silver_fx).collect()}
    usd = out[(DAYS[0], "USD")]
    assert usd.rate_per_eur == builders.FAKE_FX["USD"]
    assert usd.eur_per_unit == pytest.approx(1 / builders.FAKE_FX["USD"])


def test_cross_rates_via_eur(silver_fx: DataFrame) -> None:
    out = {(r.obs_date, r.quote_currency): r.cross_rate for r in gold.fx_cross_rates(silver_fx).collect()}
    assert out[(DAYS[0], "JPY")] == pytest.approx(150.0 / 1.20)
    assert not any(q == "USD" for _, q in out)
    assert len(out) == len(DAYS) * (len(builders.FAKE_FX) - 1)


def test_rates_timeseries_and_curve_marts(silver_yc: DataFrame, silver_estr: DataFrame) -> None:
    ts = gold.rates_timeseries(silver_yc, silver_estr)
    series = {r.series for r in ts.select("series").distinct().collect()}
    assert {"ESTR", "AAA_10Y", "ALL_3M"} <= series
    assert ts.where("series = 'ESTR'").count() == len(DAYS)

    snap = gold.curve_snapshot_latest(silver_yc).collect()
    assert {r.snapshot_date for r in snap} == {DAYS[-1]}
    assert len(snap) == 2 * len(silver.TENOR_YEARS)

    metrics = {(r.obs_date, r.curve): r for r in gold.curve_metrics_daily(silver_yc).collect()}
    m = metrics[(DAYS[0], "AAA")]
    assert m.points == len(silver.TENOR_YEARS)
    assert m.slope_2s10s_bp == pytest.approx((m.y_10y - m.y_2y) * 100)
    assert m.butterfly_2s5s10s_bp == pytest.approx((2 * m.y_5y - m.y_2y - m.y_10y) * 100)


def _python_bars(trades: list[dict[str, object]]) -> dict[tuple[str, datetime], tuple[float, ...]]:
    buckets: dict[tuple[str, datetime], list[dict[str, object]]] = defaultdict(list)
    for t in trades:
        ts = datetime.fromisoformat(str(t["event_ts"]).replace("Z", "+00:00"))
        buckets[(str(t["symbol"]), ts.replace(second=0, microsecond=0))].append(t)
    out: dict[tuple[str, datetime], tuple[float, ...]] = {}
    for key, rows in buckets.items():
        rows.sort(key=lambda r: (str(r["event_ts"]), int(str(r["seq"]))))
        prices = [float(str(r["price"])) for r in rows]
        qty = [float(str(r["qty"])) for r in rows]
        out[key] = (prices[0], max(prices), min(prices), prices[-1], sum(qty), len(rows))
    return out


def test_ohlcv_matches_reference_and_rollup(spark: SparkSession, tmp_path: Path) -> None:
    cfg = synth.SynthConfig(
        start=datetime(2030, 1, 2, 9, 58, tzinfo=UTC),
        seconds=240,
        seed=5,
        anomalies=synth.Anomalies(0, 0, 0, 0),
    )
    trades_raw, _, batch = builders.synth_bronze(spark, tmp_path, cfg)
    trades = silver.dedupe_trades(
        quality.clean(silver.trades(trades_raw), quality.TRADE_RULES), streaming=False
    )
    bars = gold.ohlcv(trades).cache()
    expected = _python_bars(batch.stream("trade"))
    # Collected timestamps are converted to the machine's local zone; format in the
    # session zone (UTC) instead so the comparison does not depend on where tests run.
    utc_bars = bars.withColumn("bar_start_utc", F.date_format("bar_start", "yyyy-MM-dd'T'HH:mm:ssXXX"))
    got = {
        (r.symbol, datetime.fromisoformat(r.bar_start_utc.replace("Z", "+00:00"))): (
            r.open, r.high, r.low, r.close, r.volume, r.trade_count
        )
        for r in utc_bars.collect()
    }  # fmt: skip
    assert got.keys() == expected.keys()
    for key, values in expected.items():
        assert got[key] == pytest.approx(values)

    hourly_direct = {(r.symbol, r.bar_start): r for r in gold.ohlcv(trades, "1 hour").collect()}
    hourly_rolled = {(r.symbol, r.bar_start): r for r in gold.rollup_bars(bars, "1 hour").collect()}
    assert hourly_direct.keys() == hourly_rolled.keys()
    assert len({k[1] for k in hourly_direct}) == 2  # the run spans 09:58-10:02
    for key, direct in hourly_direct.items():
        rolled = hourly_rolled[key]
        for field in ("open", "high", "low", "close", "volume", "vwap", "trade_count"):
            assert getattr(rolled, field) == pytest.approx(getattr(direct, field)), field


def test_dq_expectations_from_event_log(spark: SparkSession) -> None:
    exps = [
        {"name": "positive_rate", "dataset": "c.silver.fx_rates", "passed_records": 90, "failed_records": 10},
        {"name": "iso_currency", "dataset": "c.silver.fx_rates", "passed_records": 100, "failed_records": 0},
    ]
    details = json.dumps({"flow_progress": {"data_quality": {"expectations": exps}}})
    later = json.dumps({"flow_progress": {"data_quality": {"expectations": exps[:1]}}})
    log = spark.createDataFrame(
        [
            (datetime(2030, 1, 2, 10, tzinfo=UTC), "flow_progress", details),
            (datetime(2030, 1, 2, 11, tzinfo=UTC), "flow_progress", later),
            (datetime(2030, 1, 2, 11, tzinfo=UTC), "flow_progress", json.dumps({"flow_progress": {}})),
            (datetime(2030, 1, 2, 11, tzinfo=UTC), "create_update", "{}"),
        ],
        "timestamp TIMESTAMP, event_type STRING, details STRING",
    )
    out = {r.rule: r for r in gold.dq_expectations_daily(log).collect()}
    assert out["positive_rate"].passed_records == 180
    assert out["positive_rate"].failed_records == 20
    assert out["positive_rate"].pass_rate == pytest.approx(0.9)
    assert out["iso_currency"].pass_rate == 1.0


def test_freshness_from_manifests(spark: SparkSession, tmp_path: Path) -> None:
    landing = Landing(tmp_path)
    ds = ecb.DATASETS["fx_reference_rates"]
    t0 = datetime(2030, 1, 4, 16, tzinfo=UTC)
    session = FakeSession([FakeResponse(200, FAKE_EXR_CSV), FakeResponse(404, FAKE_NO_DATA)])
    ecb.ingest(ds, landing, backfill_start="2030-01-01", session=session, now=lambda: t0)
    ecb.ingest(ds, landing, session=session, now=lambda: t0 + timedelta(hours=24))
    lines = [f.read_text(encoding="utf-8") for f in sorted((tmp_path / "_manifests").rglob("*.json"))]
    manifests = spark.read.schema(bronze.MANIFEST_SCHEMA_DDL).json(spark.sparkContext.parallelize(lines, 1))
    now = F.lit(datetime(2030, 1, 6, 16, tzinfo=UTC))
    row = gold.dataset_freshness(manifests, {"fx_reference_rates": 30.0}, now=now).collect()[0]
    assert row.batches == 2
    assert row.rows_total == 3
    assert row.last_batch_rows == 0
    assert row.last_period == "2030-01-03"
    assert row.fetch_lag_hours == pytest.approx(24.0)
    assert row.within_sla is True
