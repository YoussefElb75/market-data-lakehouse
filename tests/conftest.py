"""Shared fixtures: Spark session, scratch storage, the CPU thermal guard and a run budget.

See ``tests/env.py`` for the three places these tests run (Windows, Linux CI,
Databricks serverless) and what each can do.
"""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from mdl.thermal import Budget, ThermalGuard
from tests.env import FILE_IO, IS_WINDOWS, ON_DATABRICKS
from tests.storage import DeltaStore

_GUARD = ThermalGuard.from_env()
_BUDGET = Budget(float(os.environ.get("MDL_TEST_BUDGET_S", "570")))
_state: dict[str, str] = {"module": ""}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if FILE_IO:
        return
    skip = pytest.mark.skip(
        reason="Spark local file I/O needs winutils on Windows; runs in CI and on Databricks"
    )
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
    """Databricks: the serverless session. Elsewhere: local Spark (2 cores, UTC, no UI)."""
    if ON_DATABRICKS:
        yield SparkSession.builder.getOrCreate()
        return
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
    if FILE_IO:
        from delta import configure_spark_with_delta_pip  # noqa: PLC0415 - local Delta only

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def scratch(tmp_path: Path) -> Path:
    """A folder both Python and Spark can read and write (a UC Volume on Databricks)."""
    if ON_DATABRICKS:
        folder = Path(os.environ["MDL_TEST_SCRATCH"]) / uuid.uuid4().hex
        folder.mkdir(parents=True)
        return folder
    return tmp_path


@pytest.fixture
def delta_store(spark: SparkSession, tmp_path: Path) -> Iterator[DeltaStore]:
    if ON_DATABRICKS:
        store = DeltaStore(spark, schema=os.environ["MDL_TEST_SCHEMA"])
    else:
        store = DeltaStore(spark, base=tmp_path / "delta")
    yield store
    store.cleanup()
