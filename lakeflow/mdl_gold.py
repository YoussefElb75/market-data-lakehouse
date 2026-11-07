"""Gold: curated marts, each bound to a data contract (comment and properties from it).

``dq_expectations_daily`` is not built here: it reads this pipeline's own event
log, so the post-run job task writes it after the update has finished.
"""

from collections.abc import Callable
from typing import Any

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from mdl import contracts
from mdl.lakeflow import PipelineConfig
from mdl.transforms import gold

spark = SparkSession.active()
CFG = PipelineConfig.from_spark(spark)
CONTRACTS = contracts.load_all()

# Freshness SLA per landed dataset, in hours. ECB publishes on TARGET business days,
# so 80 h covers a weekend; the synthetic stream is expected at least daily.
SLA_HOURS = {
    "fx_reference_rates": 80.0,
    "yield_curve_aaa": 80.0,
    "yield_curve_all": 80.0,
    "estr": 80.0,
    "currency_codelist": 24.0 * 8,
    "trades": 24.0,
    "book_snapshots": 24.0,
}


def _props(table: str) -> dict[str, str]:
    c = CONTRACTS[table]
    return {
        "quality": "gold",
        "mdl.contract": c.id,
        "mdl.contract_version": c.version,
        "mdl.owner": c.owner,
        "mdl.freshness_max_lag_hours": str(c.max_lag_hours),
    }


def _silver(name: str) -> DataFrame:
    return spark.read.table(CFG.table("silver", name))


def _mv(table: str) -> Callable[[Callable[[], DataFrame]], Any]:
    return dp.materialized_view(
        name=CFG.table("gold", table),
        comment=contracts.table_comment(CONTRACTS[table]),
        table_properties=_props(table),
    )


@_mv("fx_daily")
def fx_daily() -> DataFrame:
    return gold.fx_daily(_silver("fx_rates"))


@_mv("fx_cross_rates")
def fx_cross_rates() -> DataFrame:
    return gold.fx_cross_rates(_silver("fx_rates"))


@_mv("rates_timeseries")
def rates_timeseries() -> DataFrame:
    return gold.rates_timeseries(_silver("yield_curve"), _silver("estr"))


@_mv("curve_snapshot_latest")
def curve_snapshot_latest() -> DataFrame:
    return gold.curve_snapshot_latest(_silver("yield_curve"))


@_mv("curve_metrics_daily")
def curve_metrics_daily() -> DataFrame:
    return gold.curve_metrics_daily(_silver("yield_curve"))


@dp.table(
    name=CFG.table("gold", "ohlcv_1m"),
    comment=contracts.table_comment(CONTRACTS["ohlcv_1m"]),
    table_properties=_props("ohlcv_1m"),
)
def ohlcv_1m() -> DataFrame:
    # Streaming aggregation in append mode: a bar is emitted once the watermark passes its end,
    # so trades later than 2 minutes after the bar closes are not counted (see silver not_late).
    trades = spark.readStream.table(CFG.table("silver", "trades")).withWatermark("event_ts", "2 minutes")
    return gold.ohlcv(trades, "1 minute")


@_mv("ohlcv_1h")
def ohlcv_1h() -> DataFrame:
    return gold.rollup_bars(spark.read.table(CFG.table("gold", "ohlcv_1m")), "1 hour")


@_mv("dataset_freshness")
def dataset_freshness() -> DataFrame:
    manifests = spark.read.table(CFG.table("bronze", "ingest_manifests"))
    return gold.dataset_freshness(manifests, SLA_HOURS)
