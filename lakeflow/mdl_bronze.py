"""Bronze: Auto Loader streaming tables over the landing Volume (raw, append-only)."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from mdl.lakeflow import PipelineConfig, autoloader
from mdl.transforms import bronze

spark = SparkSession.active()
CFG = PipelineConfig.from_spark(spark)

ECB_DATASETS = {
    "fx_reference_rates": "ECB euro foreign exchange reference rates (EXR)",
    "yield_curve_aaa": "ECB euro area AAA spot curve (YC, G_N_A)",
    "yield_curve_all": "ECB euro area all-issuer spot curve (YC, G_N_C)",
    "estr": "ECB euro short-term rate and volume (EST)",
}
RAW = "Raw rows exactly as landed (all strings) plus ingestion metadata. Source: ECB statistics."


def _ecb_csv(dataset: str, description: str) -> None:
    @dp.table(
        name=CFG.table("bronze", f"ecb_{dataset}"),
        comment=f"{description}. {RAW}",
        table_properties={"quality": "bronze", "mdl.source": "ecb"},
    )
    def _bronze() -> DataFrame:
        raw = autoloader(spark, CFG.path("ecb", dataset), "csv", options=bronze.CSV_OPTIONS)
        return bronze.normalise_columns(bronze.with_ingest_metadata(raw, "ecb", dataset))


for _name, _description in ECB_DATASETS.items():
    _ecb_csv(_name, _description)


@dp.table(
    name=CFG.table("bronze", "ecb_currency_codelist"),
    comment=f"ECB currency code list (CL_CURRENCY), one row per code. {RAW}",
    table_properties={"quality": "bronze", "mdl.source": "ecb"},
)
def ecb_currency_codelist() -> DataFrame:
    raw = autoloader(spark, CFG.path("ecb", "currency_codelist_rows"), "json", options=bronze.JSON_OPTIONS)
    return bronze.with_ingest_metadata(raw, "ecb", "currency_codelist")


@dp.table(
    name=CFG.table("bronze", "ingest_manifests"),
    comment="One row per landed batch (ECB requests and synthetic bursts): request, status, size, "
    "SHA-256, row count and period range. Drives freshness monitoring.",
    table_properties={"quality": "bronze"},
)
def ingest_manifests() -> DataFrame:
    raw = autoloader(spark, CFG.path("_manifests"), "json", schema=bronze.MANIFEST_SCHEMA_DDL)
    return bronze.with_ingest_metadata(raw, "mdl", "ingest_manifests")


@dp.table(
    name=CFG.table("bronze", "synth_trades"),
    comment="Synthetic trades as delivered, including injected duplicates, late, out-of-order and "
    "malformed events. Synthetic data only.",
    table_properties={"quality": "bronze", "mdl.source": "synthetic"},
)
def synth_trades() -> DataFrame:
    raw = autoloader(spark, CFG.path("synth", "trades"), "json", schema=bronze.TRADE_RAW_SCHEMA)
    return bronze.with_ingest_metadata(raw, "synth", "trades")


@dp.table(
    name=CFG.table("bronze", "synth_book_snapshots"),
    comment="Synthetic 10-level order-book snapshots as delivered. Synthetic data only.",
    table_properties={"quality": "bronze", "mdl.source": "synthetic"},
)
def synth_book_snapshots() -> DataFrame:
    raw = autoloader(spark, CFG.path("synth", "book_snapshots"), "json", schema=bronze.BOOK_RAW_SCHEMA)
    return bronze.with_ingest_metadata(raw, "synth", "book_snapshots")
