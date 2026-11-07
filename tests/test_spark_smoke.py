from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tests.env import ON_DATABRICKS
from tests.storage import DeltaStore


@pytest.mark.spark
def test_spark_session_works(spark: SparkSession) -> None:
    assert spark.version.startswith("4.")
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["k", "v"])
    assert df.dropDuplicates().count() == 2
    if not ON_DATABRICKS:
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.spark
@pytest.mark.delta
def test_delta_roundtrip_and_time_travel(spark: SparkSession, delta_store: DeltaStore) -> None:
    delta_store.write(spark.range(5), "t")
    delta_store.write(spark.range(5, 8), "t", mode="append")
    assert delta_store.read("t").count() == 8
    assert delta_store.read("t", version=0).count() == 5
