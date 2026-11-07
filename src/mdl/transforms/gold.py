"""Gold: curated marts with a data contract each (see ``mdl/contracts``).

ECB values are carried unchanged. Derived measures (cross rates, slopes, bars,
KPIs) are new, named columns, in line with the ECB "no modification" reuse
condition (DATA.md).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ATTRIBUTION = "Source: ECB statistics"


# --- FX ----------------------------------------------------------------------------------------


def fx_daily(silver_fx: DataFrame) -> DataFrame:
    """ECB reference rates (units of currency per 1 EUR) and their inverse."""
    return silver_fx.select(
        "obs_date",
        "currency",
        "base_currency",
        F.col("obs_value").alias("rate_per_eur"),
        (F.lit(1.0) / F.col("obs_value")).alias("eur_per_unit"),
        "obs_status",
    )


def fx_cross_rates(silver_fx: DataFrame, quote_base: str = "USD") -> DataFrame:
    """Derived crosses against ``quote_base`` (e.g. JPY per USD) via the EUR legs."""
    fx = silver_fx.select("obs_date", "currency", F.col("obs_value").alias("rate"))
    base = fx.where(F.col("currency") == quote_base).select("obs_date", F.col("rate").alias("base_rate"))
    return (
        fx.where(F.col("currency") != quote_base)
        .join(base, "obs_date", "inner")
        .select(
            "obs_date",
            F.lit(quote_base).alias("base_currency"),
            F.col("currency").alias("quote_currency"),
            (F.col("rate") / F.col("base_rate")).alias("cross_rate"),
        )
    )


# --- rates -------------------------------------------------------------------------------------


def rates_timeseries(silver_yc: DataFrame, silver_estr: DataFrame) -> DataFrame:
    """Long daily series: €STR and every curve point, in percent."""
    estr_rate = silver_estr.where(F.col("measure") == "rate_pct").select(
        "obs_date",
        F.lit("ESTR").alias("series"),
        F.lit("overnight").alias("tenor"),
        F.lit(1.0 / 365.0).alias("tenor_years"),
        F.col("obs_value").alias("value_pct"),
    )
    curve = silver_yc.select(
        "obs_date",
        F.concat_ws("_", F.col("curve"), F.col("tenor")).alias("series"),
        "tenor",
        "tenor_years",
        F.col("obs_value").alias("value_pct"),
    )
    return estr_rate.unionByName(curve)


def curve_snapshot_latest(silver_yc: DataFrame) -> DataFrame:
    """The full spot curve on the latest available date of each curve."""
    latest = silver_yc.groupBy("curve").agg(F.max("obs_date").alias("snapshot_date"))
    return (
        silver_yc.join(latest, "curve")
        .where(F.col("obs_date") == F.col("snapshot_date"))
        .select("snapshot_date", "curve", "tenor", "tenor_years", F.col("obs_value").alias("yield_pct"))
    )


def _point(tenor: str) -> Column:
    return F.max(F.when(F.col("tenor") == tenor, F.col("obs_value")))


def curve_metrics_daily(silver_yc: DataFrame) -> DataFrame:
    """Key points and slopes per curve and day; slopes in basis points."""
    pts = silver_yc.groupBy("obs_date", "curve").agg(
        _point("3M").alias("y_3m"),
        _point("2Y").alias("y_2y"),
        _point("5Y").alias("y_5y"),
        _point("10Y").alias("y_10y"),
        _point("30Y").alias("y_30y"),
        F.count("*").alias("points"),
    )
    return pts.select(
        "*",
        ((F.col("y_10y") - F.col("y_2y")) * 100).alias("slope_2s10s_bp"),
        ((F.col("y_10y") - F.col("y_3m")) * 100).alias("slope_3m10y_bp"),
        ((2 * F.col("y_5y") - F.col("y_2y") - F.col("y_10y")) * 100).alias("butterfly_2s5s10s_bp"),
    )


# --- bars --------------------------------------------------------------------------------------


def ohlcv(trades: DataFrame, window: str = "1 minute") -> DataFrame:
    """OHLCV bars per symbol. Open and close are ordered by (event_ts, seq), so ties are deterministic."""
    order = F.struct(F.col("event_ts"), F.col("seq"))
    bars = trades.groupBy("symbol", F.window("event_ts", window).alias("w")).agg(
        F.min_by("price", order).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order).alias("close"),
        F.sum("qty").alias("volume"),
        F.sum(F.col("price") * F.col("qty")).alias("_notional"),
        F.count("*").alias("trade_count"),
    )
    return bars.select(
        "symbol",
        F.col("w.start").alias("bar_start"),
        F.col("w.end").alias("bar_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        (F.col("_notional") / F.col("volume")).alias("vwap"),
        "trade_count",
    )


def rollup_bars(bars: DataFrame, window: str = "1 hour") -> DataFrame:
    """Aggregate finer bars into coarser ones (e.g. 1m -> 1h)."""
    out = bars.groupBy("symbol", F.window("bar_start", window).alias("w")).agg(
        F.min_by("open", F.col("bar_start")).alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.max_by("close", F.col("bar_start")).alias("close"),
        F.sum("volume").alias("volume"),
        F.sum(F.col("vwap") * F.col("volume")).alias("_notional"),
        F.sum("trade_count").alias("trade_count"),
    )
    return out.select(
        "symbol",
        F.col("w.start").alias("bar_start"),
        F.col("w.end").alias("bar_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        (F.col("_notional") / F.col("volume")).alias("vwap"),
        "trade_count",
    )


# --- data quality and freshness ----------------------------------------------------------------

_EXPECTATIONS = "array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>"


def dq_expectations_daily(event_log: DataFrame) -> DataFrame:
    """Daily pass/fail counts per dataset and rule from a pipeline event log.

    Input: the ``event_log()`` table-valued function output (``timestamp``,
    ``event_type``, ``details`` as a JSON string).
    """
    exps = (
        event_log.where(F.col("event_type") == "flow_progress")
        .select(
            F.col("timestamp").cast("date").alias("event_date"),
            F.from_json(
                F.get_json_object("details", "$.flow_progress.data_quality.expectations"), _EXPECTATIONS
            ).alias("exps"),
        )
        .where(F.col("exps").isNotNull())
        .select("event_date", F.explode("exps").alias("e"))
    )
    agg = exps.groupBy("event_date", F.col("e.dataset").alias("dataset"), F.col("e.name").alias("rule")).agg(
        F.sum("e.passed_records").alias("passed_records"),
        F.sum("e.failed_records").alias("failed_records"),
    )
    total = F.col("passed_records") + F.col("failed_records")
    return agg.select(
        "*",
        F.when(total > 0, F.col("passed_records") / total).alias("pass_rate"),
    )


def dataset_freshness(
    manifests: DataFrame, sla_hours: dict[str, float], now: Column | None = None
) -> DataFrame:
    """Last successful fetch and last observation per dataset, against an SLA in hours."""
    now_col = now if now is not None else F.current_timestamp()
    fetched = F.to_timestamp(F.col("fetched_at"))
    w = Window.partitionBy("source", "dataset")
    sla_pairs: list[Column] = []
    for name, hours in sla_hours.items():
        sla_pairs += [F.lit(name), F.lit(float(hours))]
    sla = F.create_map(*sla_pairs)[F.col("dataset")] if sla_pairs else F.lit(None).cast("double")
    last = (
        manifests.where(F.col("http_status").isin(200, 404))
        .withColumn("_fetched", fetched)
        .withColumn("_rows_total", F.sum("row_count").over(w))
        .groupBy("source", "dataset")
        .agg(
            F.max("_fetched").alias("last_fetched_at"),
            F.max("last_period").alias("last_period"),
            F.max_by("row_count", "_fetched").alias("last_batch_rows"),
            F.max("_rows_total").alias("rows_total"),
            F.count("*").alias("batches"),
        )
    )
    lag_hours = (F.unix_timestamp(now_col) - F.unix_timestamp("last_fetched_at")) / 3600.0
    return last.select(
        "*",
        lag_hours.alias("fetch_lag_hours"),
        sla.alias("sla_hours"),
        (lag_hours <= sla).alias("within_sla"),
    )
