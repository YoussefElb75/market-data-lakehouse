"""Silver: typed and validated views, AUTO CDC targets (SCD1 facts, SCD2 reference data),
deduplicated ticks, and one quarantine table for every rejected row."""

from collections.abc import Callable
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from mdl import quality
from mdl.lakeflow import PipelineConfig, with_rules
from mdl.quality import Rule
from mdl.transforms import silver

spark = SparkSession.active()
CFG = PipelineConfig.from_spark(spark)

QUARANTINE = CFG.table("ops", "quarantine")
dp.create_streaming_table(
    name=QUARANTINE,
    comment="Rows rejected by a drop expectation, from every silver table: source table, failed "
    "rule names, the record as JSON and its lineage columns.",
    table_properties={"quality": "quarantine"},
)


def _bronze(name: str) -> DataFrame:
    return spark.readStream.table(CFG.table("bronze", name))


def _union(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b), frames)


def _ecb_fact(
    target: str,
    sources: list[str],
    typer: Callable[[DataFrame], DataFrame],
    rules: tuple[Rule, ...],
    comment: str,
) -> None:
    """Typed view with expectations -> AUTO CDC SCD1 target, plus a quarantine flow."""
    view = f"{target}_clean"

    @dp.temporary_view(name=view, comment=f"Validated input of silver.{target}")
    @with_rules(rules)
    def _clean() -> DataFrame:
        return _union([typer(_bronze(s)) for s in sources])

    dp.create_streaming_table(
        name=CFG.table("silver", target),
        comment=comment,
        table_properties={"quality": "silver"},
    )
    dp.create_auto_cdc_flow(
        target=CFG.table("silver", target),
        source=view,
        keys=["series_key", "obs_date"],
        sequence_by=silver.change_sequence(),
        stored_as_scd_type=1,
        name=f"{target}_cdc",
    )

    @dp.append_flow(target=QUARANTINE, name=f"quarantine_{target}")
    def _quarantine() -> DataFrame:
        typed = _union([typer(_bronze(s)) for s in sources])
        return quality.quarantine(typed, rules, CFG.table("silver", target))


LATEST = "Latest version per (series_key, obs_date): revisions and late data applied (AUTO CDC SCD1)."
_ecb_fact(
    "fx_rates",
    ["ecb_fx_reference_rates"],
    silver.fx_rates,
    quality.FX_RULES,
    f"ECB euro reference rates, typed. {LATEST} Source: ECB statistics.",
)
_ecb_fact(
    "yield_curve",
    ["ecb_yield_curve_aaa", "ecb_yield_curve_all"],
    silver.yield_curve,
    quality.YIELD_RULES,
    f"ECB euro area spot curves (AAA and all issuers), typed. {LATEST} Source: ECB statistics.",
)
_ecb_fact(
    "estr",
    ["ecb_estr"],
    silver.estr,
    quality.ESTR_RULES,
    f"Euro short-term rate (percent) and volume, typed. {LATEST} Source: ECB statistics.",
)


# --- reference data: SCD type 2 ----------------------------------------------------------------


@dp.temporary_view(name="currencies_clean", comment="Validated currency code list rows")
@with_rules(quality.CURRENCY_RULES)
def currencies_clean() -> DataFrame:
    return silver.currencies(_bronze("ecb_currency_codelist"))


dp.create_streaming_table(
    name=CFG.table("silver", "currencies"),
    comment="ECB currency codes and names with full history (AUTO CDC SCD2 on name). Source: ECB statistics.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=CFG.table("silver", "currencies"),
    source="currencies_clean",
    keys=["code"],
    sequence_by=silver.change_sequence(),
    stored_as_scd_type=2,
    track_history_column_list=["name"],
    name="currencies_cdc",
)


@dp.temporary_view(name="series_attributes_changes", comment="Series attributes per landed file")
def series_attributes_changes() -> DataFrame:
    sources = ["ecb_fx_reference_rates", "ecb_yield_curve_aaa", "ecb_yield_curve_all", "ecb_estr"]
    return _union([silver.series_attributes(_bronze(s)) for s in sources])


dp.create_streaming_table(
    name=CFG.table("silver", "series_attributes"),
    comment="Descriptive attributes of every ECB series (title, unit, decimals) with full history "
    "(AUTO CDC SCD2). Source: ECB statistics.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=CFG.table("silver", "series_attributes"),
    source="series_attributes_changes",
    keys=["series_key"],
    sequence_by=silver.change_sequence(),
    stored_as_scd_type=2,
    track_history_column_list=list(silver.SERIES_ATTRIBUTES),
    name="series_attributes_cdc",
)


# --- synthetic ticks: streaming path -----------------------------------------------------------


@dp.temporary_view(name="trades_clean", comment="Typed and validated synthetic trades")
@with_rules(quality.TRADE_RULES)
def trades_clean() -> DataFrame:
    return silver.trades(_bronze("synth_trades"))


@dp.table(
    name=CFG.table("silver", "trades"),
    comment="Synthetic trades, typed, validated and deduplicated on event_id within a 10-minute "
    "watermark. Synthetic data only.",
    table_properties={"quality": "silver"},
)
def trades() -> DataFrame:
    return silver.dedupe_trades(spark.readStream.table("trades_clean"), streaming=True)


@dp.append_flow(target=QUARANTINE, name="quarantine_trades")
def quarantine_trades() -> DataFrame:
    typed = silver.trades(_bronze("synth_trades"))
    return quality.quarantine(typed, quality.TRADE_RULES, CFG.table("silver", "trades"))


@dp.table(
    name=CFG.table("silver", "book_top"),
    comment="Top-of-book metrics (best bid/ask, mid, spread, 10-level depth) from synthetic "
    "snapshots. Synthetic data only.",
    table_properties={"quality": "silver"},
)
@with_rules(quality.BOOK_RULES)
def book_top() -> DataFrame:
    return silver.book_top(_bronze("synth_book_snapshots"))


@dp.append_flow(target=QUARANTINE, name="quarantine_book_top")
def quarantine_book_top() -> DataFrame:
    typed = silver.book_top(_bronze("synth_book_snapshots"))
    return quality.quarantine(typed, quality.BOOK_RULES, CFG.table("silver", "book_top"))


@dp.append_flow(target=QUARANTINE, name="quarantine_currencies")
def quarantine_currencies() -> DataFrame:
    typed = silver.currencies(_bronze("ecb_currency_codelist"))
    return quality.quarantine(typed, quality.CURRENCY_RULES, CFG.table("silver", "currencies"))
