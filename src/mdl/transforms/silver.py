"""Silver: typed, deduplicated, conformed tables.

* ECB observations are typed with ``try_cast`` and ``try_to_timestamp``, so bad
  values become NULL and the expectations catch them instead of crashing the
  update. They are deduplicated on ``(series_key, obs_date)``, keeping the
  latest ingested version. That covers revisions and late-arriving data. In
  the pipeline this is an AUTO CDC flow (SCD type 1, sequenced by
  ``_ingest_ts``), and ``latest_per_key`` is its batch equivalent.
* Reference data (currency names, series attributes) is kept as SCD type 2.
  The pipeline uses AUTO CDC with ``stored_as_scd_type=2``, and ``scd2`` is
  the batch reference semantics the tests pin down.
* Trades are deduplicated on ``event_id`` within a watermark. Book snapshots
  are reduced to top-of-book metrics.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ISO_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

TENOR_YEARS: dict[str, float] = {
    "3M": 0.25, "6M": 0.5, "9M": 0.75, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0, "4Y": 4.0, "5Y": 5.0,
    "6Y": 6.0, "7Y": 7.0, "8Y": 8.0, "9Y": 9.0, "10Y": 10.0, "15Y": 15.0, "20Y": 20.0,
    "25Y": 25.0, "30Y": 30.0,
}  # fmt: skip
CURVES = {"G_N_A": "AAA", "G_N_C": "ALL"}
ESTR_MEASURES = {"WT": "rate_pct", "TT": "volume"}


def _map(mapping: dict[str, str] | dict[str, float], col: Column) -> Column:
    pairs: list[Column] = []
    for key, value in mapping.items():
        pairs += [F.lit(key), F.lit(value)]
    return F.create_map(*pairs)[col]


def _date(col: str) -> Column:
    return F.try_to_timestamp(F.col(col), F.lit("yyyy-MM-dd")).cast("date")


def _ts(col: str) -> Column:
    return F.try_to_timestamp(F.col(col), F.lit(ISO_MILLIS))


_OBS_COLUMNS = ("key", "time_period", "obs_value", "obs_status", "obs_conf", "conf_status")


def ecb_observations(bronze: DataFrame) -> DataFrame:
    """Common typed columns for any ECB ``csvdata`` bronze table (lower-cased headers).

    The confidentiality flag is ``OBS_CONF`` in most dataflows and ``CONF_STATUS`` in EST.
    """
    cols = set(bronze.columns)
    conf = next((F.col(c) for c in ("obs_conf", "conf_status") if c in cols), F.lit(None).cast("string"))
    return bronze.select(
        F.col("key").alias("series_key"),
        _date("time_period").alias("obs_date"),
        F.col("obs_value").try_cast("double").alias("obs_value"),
        F.col("obs_status"),
        conf.alias("obs_conf"),
        *[c for c in bronze.columns if c not in _OBS_COLUMNS],
    )


def fx_rates(bronze: DataFrame) -> DataFrame:
    obs = ecb_observations(bronze)
    return obs.select(
        "series_key",
        "obs_date",
        F.col("currency"),
        F.col("currency_denom").alias("base_currency"),
        "obs_value",
        "obs_status",
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )


def yield_curve(bronze: DataFrame) -> DataFrame:
    obs = ecb_observations(bronze)
    tenor = F.regexp_extract(F.col("data_type_fm"), r"^SR_(\w+)$", 1)
    return obs.select(
        "series_key",
        "obs_date",
        _map(CURVES, F.col("instrument_fm")).alias("curve"),
        tenor.alias("tenor"),
        _map(TENOR_YEARS, tenor).alias("tenor_years"),
        "obs_value",
        "obs_status",
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )


def estr(bronze: DataFrame) -> DataFrame:
    obs = ecb_observations(bronze)
    return obs.select(
        "series_key",
        "obs_date",
        _map(ESTR_MEASURES, F.col("data_type_est")).alias("measure"),
        "obs_value",
        F.col("unit_measure").alias("unit"),
        F.col("unit_mult").try_cast("int").alias("unit_mult"),
        "obs_status",
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )


def latest_per_key(df: DataFrame, keys: Sequence[str], order_by: Sequence[str]) -> DataFrame:
    """Keep the last row per key by ``order_by`` (descending). Batch SCD type 1."""
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


SERIES_ATTRIBUTES = ("title", "title_compl", "unit", "unit_mult", "decimals")


def series_attributes(bronze: DataFrame) -> DataFrame:
    """Descriptive attributes per series and landed file: the SCD2 input.

    Conformed across dataflows: ``unit`` is ``UNIT`` (EXR) or ``UNIT_MEASURE``
    (EST, YC). Attributes a dataflow lacks are NULL. One row per series and
    file, sequenced by ``change_sequence()``.
    """
    cols = set(bronze.columns)

    def pick(*names: str) -> Column:
        present = [F.col(n) for n in names if n in cols]
        return F.coalesce(*present) if present else F.lit(None).cast("string")

    return bronze.select(
        F.col("key").alias("series_key"),
        pick("title").alias("title"),
        pick("title_compl").alias("title_compl"),
        pick("unit", "unit_measure").alias("unit"),
        pick("unit_mult").alias("unit_mult"),
        pick("decimals").alias("decimals"),
        "_file_modified_at",
        "_source_file",
        "_ingest_ts",
    ).dropDuplicates(["series_key", "_source_file"])


def change_sequence() -> Column:
    """Ordering of landed versions: file time, then file name (which embeds a UTC stamp).

    ``_ingest_ts`` is the same for every row of a micro-batch, so two versions
    landing in one batch would tie; the file metadata does not.
    """
    return F.struct(F.col("_file_modified_at"), F.col("_source_file"))


def scd2(
    changes: DataFrame,
    keys: Sequence[str],
    sequence_by: str,
    tracked: Sequence[str],
) -> DataFrame:
    """SCD type 2 from a change feed: one row per version, ``__START_AT``/``__END_AT``.

    A new version starts only when a tracked column changes (null-safe), the
    same semantics as AUTO CDC ``stored_as_scd_type=2`` with
    ``track_history_column_list=tracked``. The current version has a NULL ``__END_AT``.
    """
    ordered = Window.partitionBy(*keys).orderBy(F.col(sequence_by))
    signature = F.struct(*[F.col(c) for c in tracked])
    marked = changes.withColumn("_sig", signature).withColumn(
        "_changed",
        F.coalesce(~F.lag("_sig").over(ordered).eqNullSafe(F.col("_sig")), F.lit(True)),
    )
    versions = marked.where("_changed").drop("_changed", "_sig")
    return versions.withColumn("__START_AT", F.col(sequence_by)).withColumn(
        "__END_AT", F.lead(F.col(sequence_by)).over(ordered)
    )


def currencies(bronze_rows: DataFrame) -> DataFrame:
    return bronze_rows.select(
        F.trim(F.col("code")).alias("code"),
        F.trim(F.col("name")).alias("name"),
        "codelist_id",
        "codelist_version",
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )


def trades(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        "event_id",
        "symbol",
        F.col("seq").try_cast("bigint").alias("seq"),
        _ts("event_ts").alias("event_ts"),
        _ts("emit_ts").alias("emit_ts"),
        F.col("price").try_cast("double").alias("price"),
        F.col("qty").try_cast("double").alias("qty"),
        "side",
        "venue",
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )


def dedupe_trades(df: DataFrame, *, streaming: bool, watermark: str = "10 minutes") -> DataFrame:
    """Drop redelivered trades by ``event_id`` (bounded state when streaming)."""
    if streaming:
        return df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(["event_id"])
    return df.dropDuplicates(["event_id"])


_LEVELS = "array<array<double>>"


def book_top(bronze: DataFrame, depth: int = 10) -> DataFrame:
    """Top-of-book metrics from a 10-level snapshot (levels stored as JSON arrays)."""
    bids = F.from_json(F.col("bids"), _LEVELS)
    asks = F.from_json(F.col("asks"), _LEVELS)

    def level_sum(levels: Column) -> Column:
        sliced = F.slice(levels, 1, depth)
        return F.aggregate(sliced, F.lit(0.0), lambda acc, lv: acc + F.coalesce(lv[1], F.lit(0.0)))

    best_bid = F.element_at(bids, 1)[0]
    best_ask = F.element_at(asks, 1)[0]
    return bronze.select(
        "event_id",
        "symbol",
        F.col("seq").try_cast("bigint").alias("seq"),
        _ts("event_ts").alias("event_ts"),
        best_bid.alias("best_bid"),
        best_ask.alias("best_ask"),
        ((best_bid + best_ask) / 2).alias("mid"),
        (best_ask - best_bid).alias("spread"),
        level_sum(bids).alias("bid_depth_qty"),
        level_sum(asks).alias("ask_depth_qty"),
        F.size(bids).alias("bid_levels"),
        F.size(asks).alias("ask_levels"),
        "_ingest_ts",
        "_source_file",
        "_file_modified_at",
    )
