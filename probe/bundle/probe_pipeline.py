"""Step 0 probe pipeline: Auto Loader on a Volume, expectations, quarantine,
AUTO CDC SCD2 and a watermarked streaming aggregate."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/mdl_probe/p/landing"


@dp.table(comment="bronze probe: reference rows via Auto Loader")
def bronze_ref():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "code STRING, name STRING, seq LONG")
        .load(f"{LANDING}/ref/")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(comment="silver probe: valid reference rows")
@dp.expect_or_drop("code_present", "code IS NOT NULL")
@dp.expect("name_present", "name IS NOT NULL")
def silver_ref_clean():
    return spark.readStream.table("bronze_ref")


@dp.table(comment="quarantine probe: rows failing code_present")
def quarantine_ref():
    return spark.readStream.table("bronze_ref").where("code IS NULL")


dp.create_streaming_table("silver_ref_scd2", comment="SCD2 probe")
dp.create_auto_cdc_flow(
    target="silver_ref_scd2",
    source="silver_ref_clean",
    keys=["code"],
    sequence_by=F.col("seq"),
    stored_as_scd_type=2,
    except_column_list=["_ingest_ts", "_source_file", "_rescued_data"],
)


@dp.table(comment="streaming probe: 1-minute bars with watermark")
def ticks_1m():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "sym STRING, ts TIMESTAMP, px DOUBLE, qty DOUBLE")
        .load(f"{LANDING}/ticks/")
        .withWatermark("ts", "2 minutes")
        .groupBy("sym", F.window("ts", "1 minute"))
        .agg(
            F.first("px").alias("open"),
            F.max("px").alias("high"),
            F.min("px").alias("low"),
            F.last("px").alias("close"),
            F.sum("qty").alias("volume"),
        )
    )


@dp.materialized_view(comment="gold probe: current reference rows")
def gold_ref_current():
    return spark.read.table("silver_ref_scd2").where("__END_AT IS NULL")
