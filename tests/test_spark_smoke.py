from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tests.conftest import DELTA_AVAILABLE


@pytest.mark.spark
def test_local_spark_matches_pinned_version(spark: SparkSession) -> None:
    assert spark.version.startswith("4.")
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["k", "v"])
    assert df.dropDuplicates().count() == 2
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.spark
@pytest.mark.delta
def test_delta_roundtrip(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> None:
    assert DELTA_AVAILABLE
    path = str(tmp_path_factory.mktemp("delta") / "t")
    spark.range(5).write.format("delta").save(path)
    spark.range(5, 8).write.format("delta").mode("append").save(path)
    assert spark.read.format("delta").load(path).count() == 8
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 5
