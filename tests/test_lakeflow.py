from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from mdl.lakeflow import PipelineConfig


@pytest.mark.spark
def test_config_from_spark_conf(spark: SparkSession) -> None:
    spark.conf.set("mdl.catalog", "mdl_dev")
    spark.conf.set("mdl.landing", "/Volumes/mdl_dev/landing/raw/")
    try:
        cfg = PipelineConfig.from_spark(spark)
        assert cfg.table("silver", "fx_rates") == "mdl_dev.silver.fx_rates"
        assert cfg.path("ecb", "estr") == "/Volumes/mdl_dev/landing/raw/ecb/estr"
    finally:
        spark.conf.unset("mdl.catalog")
        spark.conf.unset("mdl.landing")


@pytest.mark.spark
def test_config_requires_settings(spark: SparkSession) -> None:
    spark.conf.set("mdl.catalog", "")
    spark.conf.set("mdl.landing", "")
    try:
        with pytest.raises(ValueError, match=r"mdl\.catalog"):
            PipelineConfig.from_spark(spark)
    finally:
        spark.conf.unset("mdl.catalog")
        spark.conf.unset("mdl.landing")
