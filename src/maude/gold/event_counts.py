"""Gold: monthly report counts by event type.

A first Gold table that proves the Bronze -> Silver -> Gold path end to end.
The consumer-facing tables we are building towards (per product code and
manufacturer, for hospital medical-technology departments) are planned in
docs/backlog.md.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maude.config import Config
from maude.quality import QualityCheckError

log = logging.getLogger(__name__)

EVENT_TYPES = {"D": "death", "IN": "injury", "M": "malfunction", "O": "other", "*": "no answer"}


def build_monthly_event_counts(spark: SparkSession, cfg: Config) -> dict:
    reports = spark.read.parquet(str(cfg.silver_table("reports")))
    dated = reports.filter(F.col("date_received").isNotNull())
    labels = F.create_map(*[F.lit(x) for pair in EVENT_TYPES.items() for x in pair])

    counts = dated.groupBy(
        F.trunc("date_received", "month").alias("month"),
        F.coalesce(labels[F.col("event_type")], F.lit("unknown")).alias("event_type"),
    ).agg(F.count(F.lit(1)).alias("reports"))

    # Publish only when the aggregate reconciles with Silver.
    expected = dated.count()
    total = counts.agg(F.sum("reports")).first()[0] or 0
    if total != expected:
        raise QualityCheckError(f"gold.monthly_event_counts sums to {total}, Silver has {expected}")

    counts.write.mode("overwrite").parquet(str(cfg.gold_table("monthly_event_counts")))
    metrics = {
        "silver_rows_with_date": expected,
        "gold_rows": counts.count(),
        "reports_counted": total,
    }
    log.info("gold.monthly_event_counts %s", metrics)
    return metrics
