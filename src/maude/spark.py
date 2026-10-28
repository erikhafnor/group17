"""SparkSession built from the environment's [spark] settings."""

from __future__ import annotations

from pyspark.sql import SparkSession

from maude.config import Config


def get_spark(cfg: Config) -> SparkSession:
    builder = SparkSession.builder.appName(f"maude-pipeline-{cfg.env}")
    for key, value in cfg.spark.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
