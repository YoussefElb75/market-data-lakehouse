"""Bronze: raw rows exactly as landed, plus ingestion metadata.

Bronze never casts or cleans. ECB CSV columns stay strings and synthetic JSON
keeps its raw shape, so every later step can be replayed from bronze. In the
pipeline the input is an Auto Loader stream; locally it is a batch read of
the same files. Both expose the hidden ``_metadata`` file column.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Reader options shared by Auto Loader (``cloudFiles.*`` added in the pipeline) and local tests.
CSV_OPTIONS = {"header": "true", "inferSchema": "false", "mode": "PERMISSIVE", "encoding": "UTF-8"}
JSON_OPTIONS = {"mode": "PERMISSIVE", "primitivesAsString": "true"}

METADATA_COLUMNS = ("_source", "_dataset", "_source_file", "_file_modified_at", "_ingest_ts")

TRADE_RAW_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("symbol", StringType()),
        StructField("seq", StringType()),
        StructField("event_ts", StringType()),
        StructField("emit_ts", StringType()),
        StructField("price", StringType()),
        StructField("qty", StringType()),
        StructField("side", StringType()),
        StructField("venue", StringType()),
    ]
)

BOOK_RAW_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("symbol", StringType()),
        StructField("seq", StringType()),
        StructField("event_ts", StringType()),
        StructField("emit_ts", StringType()),
        StructField("bids", StringType()),
        StructField("asks", StringType()),
        StructField("venue", StringType()),
    ]
)

MANIFEST_SCHEMA_DDL = (
    "batch_id STRING, source STRING, dataset STRING, fetched_at STRING, request_url STRING, "
    "request_params MAP<STRING, STRING>, http_status INT, bytes BIGINT, sha256 STRING, "
    "row_count BIGINT, payload_path STRING, first_period STRING, last_period STRING, attempts INT, "
    "extra MAP<STRING, STRING>"
)


def with_ingest_metadata(
    df: DataFrame,
    source: str,
    dataset: str,
    *,
    source_file: Column | None = None,
    file_modified_at: Column | None = None,
) -> DataFrame:
    """Append lineage columns.

    By default the file path and modification time come from the hidden
    ``_metadata`` column of file sources (Auto Loader, ``spark.read``). Callers
    reading from memory (unit tests) pass explicit columns instead.
    """
    path = source_file if source_file is not None else F.col("_metadata.file_path")
    mtime = file_modified_at if file_modified_at is not None else F.col("_metadata.file_modification_time")
    return df.select(
        "*",
        F.lit(source).alias("_source"),
        F.lit(dataset).alias("_dataset"),
        path.cast("string").alias("_source_file"),
        mtime.cast("timestamp").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingest_ts"),
    )


def normalise_columns(df: DataFrame) -> DataFrame:
    """Lower-case column names (ECB headers are upper case) without other changes."""
    return df.toDF(*[c.lower() if not c.startswith("_") else c for c in df.columns])
