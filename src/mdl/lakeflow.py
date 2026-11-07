"""Helpers shared by the Lakeflow Declarative Pipeline source files in ``lakeflow/``.

The pipeline files stay thin: they declare datasets and call ``mdl.transforms``.
Configuration comes from the pipeline settings (``mdl.catalog``,
``mdl.landing``), set per bundle target, so the same code serves dev and prod.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from mdl.quality import Action, Rule, expectations

F_ = TypeVar("F_", bound=Callable[..., Any])


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    landing: str

    @classmethod
    def from_spark(cls, spark: SparkSession) -> PipelineConfig:
        catalog = spark.conf.get("mdl.catalog")
        landing = spark.conf.get("mdl.landing")
        if not catalog or not landing:
            raise ValueError("pipeline configuration must set mdl.catalog and mdl.landing")
        return cls(catalog, landing.rstrip("/"))

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"

    def path(self, *parts: str) -> str:
        return "/".join([self.landing, *parts])


def autoloader(
    spark: SparkSession,
    path: str,
    fmt: str,
    *,
    options: dict[str, str] | None = None,
    schema: StructType | str | None = None,
) -> DataFrame:
    """An Auto Loader stream over a landing folder (``cloudFiles``)."""
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt)
    for key, value in (options or {}).items():
        reader = reader.option(key, value)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.load(path)


def with_rules(rules: Sequence[Rule]) -> Callable[[F_], F_]:
    """Attach ``fail``, ``drop`` and ``warn`` expectations from a rule set to a dataset."""
    # Expectation decorators exist only in the Databricks runtime implementation of
    # pyspark.pipelines, not in the open-source package used locally, hence the dynamic lookup.
    dp: Any = importlib.import_module("pyspark.pipelines")
    fail = expectations(rules, Action.FAIL)
    drop = expectations(rules, Action.DROP)
    warn = expectations(rules, Action.WARN)

    def decorate(fn: F_) -> F_:
        out: Any = fn
        if warn:
            out = dp.expect_all(warn)(out)
        if drop:
            out = dp.expect_all_or_drop(drop)(out)
        if fail:
            out = dp.expect_all_or_fail(fail)(out)
        result: F_ = out
        return result

    return decorate
