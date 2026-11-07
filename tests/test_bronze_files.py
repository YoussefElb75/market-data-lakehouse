"""File-based bronze reads, as Auto Loader does them (``_metadata`` columns).

Spark's local file I/O needs winutils on Windows, so these run in Linux CI.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from mdl.transforms import bronze, silver
from tests import builders

pytestmark = [pytest.mark.spark, pytest.mark.localfs]


def test_csv_read_exposes_file_metadata(spark: SparkSession, tmp_path: Path) -> None:
    days = builders.business_days(date(2030, 1, 1), 2)
    builders.write_csv(tmp_path, "a.csv", builders.EXR_HEADER, builders.exr_rows(days[:1]))
    builders.write_csv(tmp_path, "b.csv", builders.EXR_HEADER, builders.exr_rows(days[1:]))
    raw = spark.read.options(**bronze.CSV_OPTIONS).csv(str(tmp_path))
    df = bronze.normalise_columns(bronze.with_ingest_metadata(raw, "ecb", "fx_reference_rates"))
    assert set(bronze.METADATA_COLUMNS) <= set(df.columns)
    files = {r._source_file.rsplit("/", 1)[-1] for r in df.select("_source_file").distinct().collect()}
    assert files == {"a.csv", "b.csv"}
    assert all(t == "string" for c, t in df.dtypes if not c.startswith("_"))  # bronze never casts
    assert silver.fx_rates(df).count() == 2 * len(builders.FAKE_FX)


def test_json_read_with_raw_schema(spark: SparkSession, tmp_path: Path) -> None:
    folder = tmp_path / "t"
    folder.mkdir()
    (folder / "x.json").write_text(
        '{"event_id":"E1","symbol":"SYN-EQ1","price":101.5,"qty":2,"event_ts":"2030-01-02T09:00:00.000Z"}\n',
        encoding="utf-8",
    )
    raw = spark.read.options(**bronze.JSON_OPTIONS).schema(bronze.TRADE_RAW_SCHEMA).json(str(folder))
    row = bronze.with_ingest_metadata(raw, "synth", "trades").collect()[0]
    assert row.price == "101.5"
    assert row._source_file.endswith("x.json")
