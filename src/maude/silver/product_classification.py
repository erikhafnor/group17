"""Silver: FDA product classification, one row per product code.

Source: foiclass.txt from FDA's premarket area. FDA republishes the whole file
(it changed daily in November 2026), so Bronze holds one partition per
downloaded version. Silver uses only the newest version, so a product code
FDA has retired disappears here too.

The table is small (about 7,100 codes) and is broadcast when Silver devices
and Gold join on it.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from maude.config import Config
from maude.quality import QualityCheckError
from maude.silver.cleaning import clean_string

log = logging.getLogger(__name__)

CLASSIFICATION_COLUMNS = [
    "product_code",
    "device_name",
    "device_class",
    "medical_specialty",
    "review_panel",
    "regulation_number",
    "is_implant",
    "is_life_sustaining",
]


def yes_no(name: str) -> Column:
    """Y/N flag to boolean; anything else becomes null."""
    value = F.upper(clean_string(name))
    return F.when(value == "Y", True).when(value == "N", False)


def build_product_classification(spark: SparkSession, cfg: Config) -> dict:
    bronze = spark.read.option("mergeSchema", "true").parquet(
        str(cfg.bronze_table("product_classification"))
    )
    newest_file = bronze.agg(
        F.max(F.struct("_source_modified_at", "_ingested_at", "_source_file_id")).alias("v")
    ).first()["v"]["_source_file_id"]
    latest = bronze.filter(F.col("_source_file_id") == newest_file)

    typed = latest.select(
        F.upper(clean_string("PRODUCTCODE")).alias("product_code"),
        clean_string("DEVICENAME").alias("device_name"),
        F.upper(clean_string("DEVICECLASS")).alias("device_class"),
        clean_string("MEDICALSPECIALTY").alias("medical_specialty"),
        clean_string("REVIEW_PANEL").alias("review_panel"),
        clean_string("REGULATIONNUMBER").alias("regulation_number"),
        yes_no("Implant_Flag").alias("is_implant"),
        yes_no("Life_Sustain_support_flag").alias("is_life_sustaining"),
        "_is_well_formed",
        "_raw_record",
        "_source_file_id",
    ).withColumn(
        "_reject_reason",
        F.when(~F.col("_is_well_formed"), "malformed_record").when(
            F.col("product_code").isNull(), "missing_product_code"
        ),
    )

    accepted = typed.filter(F.col("_reject_reason").isNull())
    current = accepted.dropDuplicates(["product_code"]).select(
        *CLASSIFICATION_COLUMNS, "_source_file_id"
    )
    current.write.mode("overwrite").parquet(str(cfg.silver_table("product_classification")))
    typed.filter(F.col("_reject_reason").isNotNull()).select(
        "_reject_reason", "_raw_record", "product_code", "_source_file_id"
    ).write.mode("overwrite").parquet(str(cfg.quarantine_table("product_classification")))

    written = spark.read.parquet(str(cfg.silver_table("product_classification")))
    rejected = spark.read.parquet(str(cfg.quarantine_table("product_classification")))
    metrics = {
        "source_file_id": newest_file,
        "input_rows": latest.count(),
        "rejected_rows": rejected.count(),
        "duplicate_codes": accepted.count() - written.count(),
        "output_rows": written.count(),
        "missing_device_class": written.filter(F.col("device_class").isNull()).count(),
    }
    if metrics["output_rows"] == 0:
        raise QualityCheckError("silver.product_classification is empty")
    log.info("silver.product_classification %s", metrics)
    return metrics
