"""Build bronze-like DataFrames from handcrafted files (made-up values, ECB-shaped).

Files are written to a temporary folder and read back with the same reader
options as the pipeline, so the hidden ``_metadata`` column exists as it does
under Auto Loader.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from mdl import synth
from mdl.sources.landing import Landing
from mdl.transforms import bronze
from mdl.transforms.silver import TENOR_YEARS

EXR_HEADER = [
    "KEY", "FREQ", "CURRENCY", "CURRENCY_DENOM", "EXR_TYPE", "EXR_SUFFIX", "TIME_PERIOD", "OBS_VALUE",
    "OBS_STATUS", "OBS_CONF", "TITLE", "UNIT", "DECIMALS",
]  # fmt: skip
YC_HEADER = [
    "KEY", "FREQ", "REF_AREA", "CURRENCY", "PROVIDER_FM", "INSTRUMENT_FM", "PROVIDER_FM_ID",
    "DATA_TYPE_FM", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "OBS_CONF", "TITLE",
]  # fmt: skip
EST_HEADER = [
    "KEY", "FREQ", "BENCHMARK_ITEM", "DATA_TYPE_EST", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS",
    "CONF_STATUS", "TITLE", "UNIT_MEASURE", "UNIT_MULT",
]  # fmt: skip

FAKE_FX = {"USD": 1.20, "JPY": 150.0, "GBP": 0.85}


def business_days(start: date, n: int) -> list[date]:
    out: list[date] = []
    day = start
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def _csv(header: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


def exr_rows(days: Sequence[date], fx: dict[str, float] = FAKE_FX) -> list[list[object]]:
    rows: list[list[object]] = []
    for i, day in enumerate(days):
        for ccy, level in fx.items():
            value = round(level * (1 + 0.001 * i), 6)
            rows.append(
                [f"EXR.D.{ccy}.EUR.SP00.A", "D", ccy, "EUR", "SP00", "A", day.isoformat(), value, "A", "F",
                 f"Fake {ccy}/EUR", ccy, 4]
            )  # fmt: skip
    return rows


def yc_rows(days: Sequence[date], tenors: Sequence[str] = tuple(TENOR_YEARS)) -> list[list[object]]:
    rows: list[list[object]] = []
    for i, day in enumerate(days):
        for issuer, spread in (("G_N_A", 0.0), ("G_N_C", 0.3)):
            for tenor in tenors:
                value = round(2.0 + 0.1 * TENOR_YEARS[tenor] ** 0.5 + spread + 0.01 * i, 6)
                key = f"YC.B.U2.EUR.4F.{issuer}.SV_C_YM.SR_{tenor}"
                rows.append(
                    [key, "B", "U2", "EUR", "4F", issuer, "SV_C_YM", f"SR_{tenor}", day.isoformat(), value,
                     "A", "F", f"Fake curve {issuer} {tenor}"]
                )  # fmt: skip
    return rows


def est_rows(days: Sequence[date]) -> list[list[object]]:
    rows: list[list[object]] = []
    for i, day in enumerate(days):
        rows.append(["EST.B.EU000A2X2A25.WT", "B", "EU000A2X2A25", "WT", day.isoformat(), 2.0 + 0.001 * i,
                     "A", "F", "Fake rate", "PC", 0])  # fmt: skip
        rows.append(["EST.B.EU000A2X2A25.TT", "B", "EU000A2X2A25", "TT", day.isoformat(), 40000 + i,
                     "A", "F", "Fake volume", "EUR", 6])  # fmt: skip
    return rows


def write_csv(folder: Path, name: str, header: Sequence[str], rows: Sequence[Sequence[object]]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(_csv(header, rows), encoding="utf-8")
    return path


def _from_files(
    spark: SparkSession, files: list[Path], read: Callable[[RDD[str]], DataFrame], source: str, dataset: str
) -> DataFrame:
    """Read files through Python and parse the lines in Spark: no Hadoop file I/O.

    Each file gets its own literal ``_source_file``, as ``_metadata.file_path`` would.
    """
    if not files:
        raise ValueError("no input files")
    frames = []
    for path in sorted(files):
        lines = path.read_text(encoding="utf-8-sig").splitlines()
        raw = read(spark.sparkContext.parallelize(lines, 1))
        frames.append(
            bronze.with_ingest_metadata(
                raw,
                source,
                dataset,
                source_file=F.lit(path.as_posix()),
                file_modified_at=F.lit(datetime.fromtimestamp(path.stat().st_mtime, UTC)),
            )
        )
    return reduce(DataFrame.unionByName, frames)


def read_bronze_csv(spark: SparkSession, folder: Path, dataset: str) -> DataFrame:
    def read(rdd: RDD[str]) -> DataFrame:
        return spark.read.options(**bronze.CSV_OPTIONS).csv(rdd)

    files = list(folder.glob("*.csv"))
    return bronze.normalise_columns(_from_files(spark, files, read, "ecb", dataset))


def read_bronze_json(spark: SparkSession, folder: Path, schema: StructType, dataset: str) -> DataFrame:
    def read(rdd: RDD[str]) -> DataFrame:
        return spark.read.options(**bronze.JSON_OPTIONS).schema(schema).json(rdd)

    return _from_files(spark, list(folder.rglob("*.json")), read, "synth", dataset)


def synth_bronze(
    spark: SparkSession, folder: Path, cfg: synth.SynthConfig
) -> tuple[DataFrame, DataFrame, synth.SynthBatch]:
    batch = synth.generate(cfg)
    synth.write_bursts(batch, Landing(folder), burst_size=400)
    trades = read_bronze_json(spark, folder / "synth" / "trades", bronze.TRADE_RAW_SCHEMA, "trades")
    books = read_bronze_json(
        spark, folder / "synth" / "book_snapshots", bronze.BOOK_RAW_SCHEMA, "book_snapshots"
    )
    return trades, books, batch
