"""Silver: one current row per adverse event report.

Grain: one row per MDR_REPORT_KEY, holding the newest version FDA has
delivered. The monthly change file re-delivers reports that already exist
(405,111 of the 672,141 keys in the October 2026 change file were also in that
month's add file), so versions are resolved here instead of being appended.

This is our first vertical slice. The remaining columns, rules and
enrichments are tracked in docs/backlog.md.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from maude.config import Config
from maude.quality import QualityCheckError
from maude.silver.cleaning import clean_string, parse_date
from maude.sources import KIND_RANK

log = logging.getLogger(__name__)

REPORT_COLUMNS = [
    "mdr_report_key",
    "report_number",
    "report_source_code",
    "event_type",
    "date_received",
    "date_of_event",
    "date_changed",
]


def build_reports(spark: SparkSession, cfg: Config) -> dict:
    bronze = spark.read.option("mergeSchema", "true").parquet(str(cfg.bronze_table("master")))
    source_rank = F.create_map(*[F.lit(x) for pair in KIND_RANK.items() for x in pair])

    typed = bronze.select(
        F.col("MDR_REPORT_KEY").cast("long").alias("mdr_report_key"),
        clean_string("REPORT_NUMBER").alias("report_number"),
        clean_string("REPORT_SOURCE_CODE").alias("report_source_code"),
        clean_string("EVENT_TYPE").alias("event_type"),
        parse_date("DATE_RECEIVED").alias("date_received"),
        parse_date("DATE_OF_EVENT").alias("date_of_event"),
        parse_date("DATE_CHANGED").alias("date_changed"),
        "_is_well_formed",
        "_raw_record",
        "_source_file_id",
        source_rank[F.col("_source_kind")].alias("_source_rank"),
        "_source_modified_at",
    ).withColumn(
        "_reject_reason",
        F.when(~F.col("_is_well_formed"), "malformed_record").when(
            F.col("mdr_report_key").isNull(), "invalid_report_key"
        ),
    )

    rejected = typed.filter(F.col("_reject_reason").isNotNull())
    newest_first = Window.partitionBy("mdr_report_key").orderBy(
        F.col("date_changed").desc_nulls_last(),
        F.col("_source_rank").desc(),
        F.col("_source_modified_at").desc_nulls_last(),
    )
    current = (
        typed.filter(F.col("_reject_reason").isNull())
        .withColumn("_version", F.row_number().over(newest_first))
        .filter(F.col("_version") == 1)
        .select(*REPORT_COLUMNS, "_source_file_id")
    )

    current.write.mode("overwrite").parquet(str(cfg.silver_table("reports")))
    rejected.select(
        "_reject_reason", "_raw_record", "mdr_report_key", "_source_file_id"
    ).write.mode("overwrite").parquet(str(cfg.quarantine_table("reports")))

    written = spark.read.parquet(str(cfg.silver_table("reports")))
    metrics = {
        "input_rows": bronze.count(),
        "rejected_rows": spark.read.parquet(str(cfg.quarantine_table("reports"))).count(),
        "output_rows": written.count(),
        "distinct_keys": written.select("mdr_report_key").distinct().count(),
        "missing_date_received": written.filter(F.col("date_received").isNull()).count(),
    }
    metrics["superseded_versions"] = (
        metrics["input_rows"] - metrics["rejected_rows"] - metrics["output_rows"]
    )

    if metrics["output_rows"] == 0:
        raise QualityCheckError("silver.reports is empty")
    if metrics["distinct_keys"] != metrics["output_rows"]:
        raise QualityCheckError("silver.reports has more than one row per mdr_report_key")
    log.info("silver.reports %s", metrics)
    return metrics
