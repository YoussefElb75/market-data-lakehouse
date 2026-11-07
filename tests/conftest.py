"""Shared fixtures: a small local SparkSession, the CPU thermal guard and a run budget."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from mdl.thermal import Budget, ThermalGuard

IS_WINDOWS = sys.platform == "win32"
# Spark's local file I/O (Hadoop RawLocalFileSystem: listing, reads, Delta writes)
# needs Hadoop's winutils on Windows, which this project does not install. Tests
# marked `localfs` or `delta` run in Linux CI only; the rest build DataFrames in memory.
DELTA_AVAILABLE = not IS_WINDOWS or bool(os.environ.get("HADOOP_HOME"))

_GUARD = ThermalGuard.from_env()
_BUDGET = Budget(float(os.environ.get("MDL_TEST_BUDGET_S", "570")))
_state: dict[str, str] = {"module": ""}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if DELTA_AVAILABLE:
        return
    skip = pytest.mark.skip(reason="Spark local file I/O needs winutils on Windows; runs in Linux CI")
    for item in items:
        if "delta" in item.keywords or "localfs" in item.keywords:
            item.add_marker(skip)


def pytest_runtest_setup(item: pytest.Item) -> None:
    module = item.nodeid.split("::", 1)[0]
    if module != _state["module"]:
        _state["module"] = module
        _GUARD.check()
    if _BUDGET.exhausted:
        pytest.exit(f"foreground budget of {_BUDGET.seconds:.0f} s exhausted", returncode=3)


def _ensure_java() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    if IS_WINDOWS:
        candidates = sorted(Path(r"C:\Program Files\Eclipse Adoptium").glob("jdk-21*"), reverse=True)
        if candidates:
            os.environ["JAVA_HOME"] = str(candidates[0])


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-wide local Spark (2 cores, 2 shuffle partitions, UTC, no UI)."""
    _ensure_java()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("mdl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.ansi.enabled", "true")
    )
    if DELTA_AVAILABLE:
        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
