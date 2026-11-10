"""Gold: monthly report counts per product code and event type.

This is the table that answers the project question for hospital
medical-technology departments: which device types account for reports of
deaths, injuries and malfunctions, and how does that develop month by month.

Grain: month received x product code x event type, with the product's name,
device class and medical specialty. Each report is counted once, under its
primary device (lowest device sequence number). Reports without a device row,
or whose device has no product code, are counted with a null product code.
That keeps the table reconcilable: its total equals the number of dated
Silver reports, overall and per event type, and it is only published when
both checks hold.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from maude.config import Config
from maude.gold.event_counts import EVENT_TYPES
from maude.quality import QualityCheckError

log = logging.getLogger(__name__)


def build_monthly_product_events(spark: SparkSession, cfg: Config) -> dict:
    labels = F.create_map(*[F.lit(x) for pair in EVENT_TYPES.items() for x in pair])
    reports = (
        spark.read.parquet(str(cfg.silver_table("reports")))
        .filter(F.col("date_received").isNotNull())
        .select(
            "mdr_report_key",
            F.trunc("date_received", "month").alias("month"),
            F.coalesce(labels[F.col("event_type")], F.lit("unknown")).alias("event_type"),
        )
    )

    devices = spark.read.parquet(str(cfg.silver_table("devices")))
    by_sequence = Window.partitionBy("mdr_report_key").orderBy("device_sequence_no")
    primary = (
        devices.withColumn("_n", F.row_number().over(by_sequence))
        .filter(F.col("_n") == 1)
        .select("mdr_report_key", "product_code")
    )
    classification = spark.read.parquet(str(cfg.silver_table("product_classification"))).select(
        "product_code", "device_name", "device_class", "medical_specialty"
    )

    per_report = reports.join(primary, "mdr_report_key", "left").join(
        F.broadcast(classification), "product_code", "left"
    )
    counts = per_report.groupBy(
        "month", "product_code", "device_name", "device_class", "medical_specialty", "event_type"
    ).agg(F.count(F.lit(1)).alias("reports"))

    # Reconcile with Silver before publishing: in total and per event type.
    expected = dict(reports.groupBy("event_type").count().collect())
    actual = dict(counts.groupBy("event_type").agg(F.sum("reports")).collect())
    if actual != expected:
        raise QualityCheckError(
            f"gold.monthly_product_events does not reconcile with Silver: {actual} != {expected}"
        )

    counts.write.mode("overwrite").parquet(str(cfg.gold_table("monthly_product_events")))
    gold = spark.read.parquet(str(cfg.gold_table("monthly_product_events")))
    reports_per_key = devices.groupBy("mdr_report_key").agg(
        F.countDistinct("product_code").alias("codes")
    )
    metrics = {
        "silver_reports_with_date": sum(expected.values()),
        "reports_counted": gold.agg(F.sum("reports")).first()[0] or 0,
        "reconciled": True,
        "gold_rows": gold.count(),
        "product_codes": gold.filter(F.col("product_code").isNotNull())
        .select("product_code")
        .distinct()
        .count(),
        "reports_without_product_code": gold.filter(F.col("product_code").isNull())
        .agg(F.sum("reports"))
        .first()[0]
        or 0,
        "reports_with_several_product_codes": reports_per_key.filter(F.col("codes") > 1).count(),
    }
    log.info("gold.monthly_product_events %s", metrics)
    return metrics
