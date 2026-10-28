"""Bronze parsing: turn each landed zip into rows of its family's Bronze table.

Values are kept exactly as delivered and typed as strings; only the text
encoding is normalised (FDA ships cp1252, Spark reads UTF-8). Three facts
about the source files shape this code:

- Records end with CRLF, but some free-text fields contain bare LF
  characters. Splitting on CRLF only keeps those records whole.
- Fields are never quoted, so a '|' inside a value shifts every later field.
  Such records are kept, flagged as malformed, and their raw line preserved.
- foidevproblem files have no header line; their columns come from the catalogue.

Each source file is written to its own partition (_source_file_id), so
re-parsing a file replaces exactly that file's rows and nothing else.
"""

from __future__ import annotations

import codecs
import csv
import io
import logging
import shutil
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from maude.bronze.manifest import Manifest
from maude.config import Config
from maude.run_context import RunContext

log = logging.getLogger(__name__)

RECORD_SEPARATOR = "\r\n"
CHUNK_SIZE = 1 << 20

_undecodable_bytes = 0


def _latin1_fallback(err: UnicodeDecodeError) -> tuple[str, int]:
    """cp1252 leaves five byte values undefined; keep them as their latin-1 code point."""
    global _undecodable_bytes
    _undecodable_bytes += err.end - err.start
    return err.object[err.start : err.end].decode("latin-1"), err.end


codecs.register_error("maude-latin1-fallback", _latin1_fallback)


def _stage_utf8(zip_path: Path, encoding: str, dest: Path) -> int:
    """Decode the single file inside a zip archive to UTF-8, keeping line endings as-is.

    Returns the number of bytes that could not be decoded with the source encoding.
    """
    global _undecodable_bytes
    with zipfile.ZipFile(zip_path) as archive:
        members = [m for m in archive.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise ValueError(
                f"{zip_path.name}: expected one file in the archive, found {len(members)}"
            )
        _undecodable_bytes = 0
        dest.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(members[0]) as raw, dest.open("w", encoding="utf-8", newline="") as out:
            text = io.TextIOWrapper(
                raw, encoding=encoding, errors="maude-latin1-fallback", newline=""
            )
            shutil.copyfileobj(text, out, CHUNK_SIZE)
    return _undecodable_bytes


def _first_record(path: Path) -> str:
    with path.open(encoding="utf-8", newline="") as fh:
        return fh.readline().rstrip("\r\n")


def _parse_pipe(
    spark: SparkSession, staged: Path, columns: list[str], header: str | None
) -> DataFrame:
    lines = spark.read.option("lineSep", RECORD_SEPARATOR).text(str(staged))
    if header is not None:
        lines = lines.filter(F.col("value") != header)
    lines = lines.filter(F.col("value") != "")

    fields = lines.select("value", F.split("value", r"\|", -1).alias("_fields"))
    well_formed = F.size("_fields") == len(columns)
    return fields.select(
        *[F.when(well_formed, F.col("_fields")[i]).alias(name) for i, name in enumerate(columns)],
        F.size("_fields").alias("_field_count"),
        well_formed.alias("_is_well_formed"),
        F.when(~well_formed, F.col("value")).alias("_raw_record"),
    )


def _parse_csv(spark: SparkSession, staged: Path, header: str) -> DataFrame:
    columns = next(csv.reader([header]))
    schema = StructType(
        [StructField(name, StringType()) for name in columns]
        + [StructField("_raw_record", StringType())]
    )
    rows = (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_raw_record")
        .csv(str(staged))
    )
    return rows.select(
        *columns,
        F.lit(None).cast("int").alias("_field_count"),
        F.col("_raw_record").isNull().alias("_is_well_formed"),
        "_raw_record",
    )


def _metadata(event: dict) -> list[Column]:
    modified = event["last_modified"]
    modified_iso = parsedate_to_datetime(modified).isoformat() if modified else None
    return [
        F.lit(event["name"]).alias("_source_name"),
        F.lit(event["kind"]).alias("_source_kind"),
        F.lit(modified_iso).cast("timestamp").alias("_source_modified_at"),
        F.lit(event["run_id"]).alias("_ingest_run_id"),
        F.lit(event["at"]).cast("timestamp").alias("_ingested_at"),
        F.lit(event["file_id"]).alias("_source_file_id"),
    ]


def parse_file(spark: SparkSession, cfg: Config, event: dict) -> dict:
    staged = cfg.staging_dir / f"{event['file_id']}.txt"
    try:
        undecodable = _stage_utf8(cfg.data_root / event["path"], event["encoding"], staged)
        first = _first_record(staged)

        if event["fmt"] == "csv":
            rows = _parse_csv(spark, staged, first)
        elif event["columns"] and not first.startswith(event["columns"][0]):
            rows = _parse_pipe(spark, staged, event["columns"], header=None)
        else:
            rows = _parse_pipe(spark, staged, first.split("|"), header=first)

        table = cfg.bronze_table(event["family"])
        (
            rows.select("*", *_metadata(event))
            .write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("_source_file_id")
            .parquet(str(table))
        )

        # A file with a header and no records (an empty monthly file) writes no partition.
        partition = table / f"_source_file_id={event['file_id']}"
        rows, malformed = 0, 0
        if partition.exists():
            counts = (
                spark.read.parquet(str(partition))
                .agg(
                    F.count(F.lit(1)).alias("rows"),
                    F.sum(F.when(F.col("_is_well_formed"), 0).otherwise(1)).alias("malformed"),
                )
                .first()
            )
            rows, malformed = counts["rows"], counts["malformed"]
    finally:
        staged.unlink(missing_ok=True)

    return {
        "file_id": event["file_id"],
        "family": event["family"],
        "rows": rows,
        "malformed": malformed,
        "undecodable_bytes": undecodable,
    }


def parse_pending(spark: SparkSession, cfg: Config, run: RunContext) -> list[dict]:
    """Parse every landed file that has no "parsed" event yet (safe to rerun)."""
    manifest = Manifest(cfg.manifest_path)
    results = []
    for event in manifest.pending_parse():
        result = parse_file(spark, cfg, event)
        manifest.append({"event": "parsed", **result, "run_id": run.run_id, "at": run.started_iso})
        log.info(
            "%-36s %10d rows, %d malformed", event["file_id"], result["rows"], result["malformed"]
        )
        results.append(result)
    return results
