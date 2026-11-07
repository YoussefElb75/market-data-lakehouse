"""Delta tables for tests: path-based locally and in CI, managed tables on Databricks."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


class DeltaStore:
    def __init__(self, spark: SparkSession, *, base: Path | None = None, schema: str | None = None) -> None:
        if (base is None) == (schema is None):
            raise ValueError("give exactly one of base (path tables) or schema (managed tables)")
        self.spark = spark
        self.base = base
        self.schema = schema
        self.prefix = f"t{uuid.uuid4().hex[:10]}"
        self._created: set[str] = set()

    def ref(self, name: str) -> str:
        """SQL reference usable in spark.sql and mdl.checks."""
        if self.schema is not None:
            return f"{self.schema}.{self.prefix}_{name}"
        assert self.base is not None
        return f"delta.`{(self.base / name).as_posix()}`"

    def write(self, df: DataFrame, name: str, mode: str = "overwrite", **options: str) -> None:
        writer = df.write.format("delta").mode(mode).options(**options)
        self._created.add(name)
        if self.schema is not None:
            writer.saveAsTable(self.ref(name))
        else:
            assert self.base is not None
            writer.save((self.base / name).as_posix())

    def read(self, name: str, version: int | None = None) -> DataFrame:
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        if self.schema is not None:
            return reader.table(self.ref(name))
        assert self.base is not None
        return reader.load((self.base / name).as_posix())

    def table(self, name: str) -> Any:
        from delta.tables import DeltaTable  # noqa: PLC0415 - only for Delta-enabled sessions

        if self.schema is not None:
            return DeltaTable.forName(self.spark, self.ref(name))
        assert self.base is not None
        return DeltaTable.forPath(self.spark, (self.base / name).as_posix())

    def track(self, name: str) -> None:
        """Register a table created outside ``write`` (e.g. by SQL) for cleanup."""
        self._created.add(name)

    def cleanup(self) -> None:
        if self.schema is None:
            return
        for name in self._created:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.ref(name)}")
