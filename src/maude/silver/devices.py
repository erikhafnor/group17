"""Silver: the devices named in each adverse event report.

Grain: one row per (mdr_report_key, device_sequence_no) in the newest delivery
of each report.

DEVICE_SEQUENCE_NO is not a stable identity across files. In the November 2026
files, all 288,316 reports present in both deviceadd and devicechange had a
different sequence number in each file (and the same product code), so
de-duplicating on (key, sequence) would count every re-delivered device twice.
Versions are therefore resolved per report: all device rows of a report come
from the newest file that delivered it, ordered by file kind and then
publication time, as for Silver reports.

Enrichment: device class, medical specialty and implant/life-sustaining flags
from silver.product_classification (broadcast join).
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

# FDA writes a missing product code as "---".
MISSING_CODES = ("---",)

DEVICE_COLUMNS = [
    "mdr_report_key",
    "device_sequence_no",
    "date_received",
    "product_code",
    "brand_name",
    "generic_name",
    "manufacturer_name",
    "model_number",
    "udi_di",
]
ENRICHMENT_COLUMNS = [
    "device_name",
    "device_class",
    "medical_specialty",
    "is_implant",
    "is_life_sustaining",
]


def build_devices(spark: SparkSession, cfg: Config) -> dict:
    bronze = spark.read.option("mergeSchema", "true").parquet(str(cfg.bronze_table("device")))
    source_rank = F.create_map(*[F.lit(x) for pair in KIND_RANK.items() for x in pair])
    code = F.upper(clean_string("DEVICE_REPORT_PRODUCT_CODE"))

    typed = bronze.select(
        F.col("MDR_REPORT_KEY").cast("long").alias("mdr_report_key"),
        F.col("DEVICE_SEQUENCE_NO").cast("long").alias("device_sequence_no"),
        parse_date("DATE_RECEIVED").alias("date_received"),
        F.when(code.isin(*MISSING_CODES), None).otherwise(code).alias("product_code"),
        clean_string("BRAND_NAME").alias("brand_name"),
        clean_string("GENERIC_NAME").alias("generic_name"),
        clean_string("MANUFACTURER_D_NAME").alias("manufacturer_name"),
        clean_string("MODEL_NUMBER").alias("model_number"),
        clean_string("`UDI-DI`").alias("udi_di"),
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

    newest_delivery = Window.partitionBy("mdr_report_key").orderBy(
        F.col("_source_rank").desc(),
        F.col("_source_modified_at").desc_nulls_last(),
        F.col("_source_file_id").desc(),
    )
    latest = (
        typed.filter(F.col("_reject_reason").isNull())
        .withColumn("_delivery", F.dense_rank().over(newest_delivery))
        .filter(F.col("_delivery") == 1)
        .dropDuplicates(["mdr_report_key", "device_sequence_no"])
    )

    classification = spark.read.parquet(str(cfg.silver_table("product_classification")))
    enriched = latest.join(
        F.broadcast(classification.select("product_code", *ENRICHMENT_COLUMNS)),
        on="product_code",
        how="left",
    ).select(
        *DEVICE_COLUMNS,
        *ENRICHMENT_COLUMNS,
        (F.col("product_code").isNotNull() & F.col("device_name").isNull()).alias(
            "_unknown_product_code"
        ),
        "_source_file_id",
    )

    enriched.write.mode("overwrite").parquet(str(cfg.silver_table("devices")))
    rejected.select(
        "_reject_reason", "_raw_record", "mdr_report_key", "_source_file_id"
    ).write.mode("overwrite").parquet(str(cfg.quarantine_table("devices")))

    written = spark.read.parquet(str(cfg.silver_table("devices")))
    reports = spark.read.parquet(str(cfg.silver_table("reports"))).select("mdr_report_key")
    metrics = {
        "input_rows": bronze.count(),
        "rejected_rows": spark.read.parquet(str(cfg.quarantine_table("devices"))).count(),
        "output_rows": written.count(),
        "distinct_reports": written.select("mdr_report_key").distinct().count(),
        "distinct_keys": written.select("mdr_report_key", "device_sequence_no").distinct().count(),
        "missing_product_code": written.filter(F.col("product_code").isNull()).count(),
        "unknown_product_code": written.filter("_unknown_product_code").count(),
        "devices_without_report": written.join(reports, "mdr_report_key", "left_anti").count(),
    }
    metrics["superseded_rows"] = (
        metrics["input_rows"] - metrics["rejected_rows"] - metrics["output_rows"]
    )

    if metrics["output_rows"] == 0:
        raise QualityCheckError("silver.devices is empty")
    if metrics["distinct_keys"] != metrics["output_rows"]:
        raise QualityCheckError("silver.devices has more than one row per report and sequence")
    log.info("silver.devices %s", metrics)
    return metrics
