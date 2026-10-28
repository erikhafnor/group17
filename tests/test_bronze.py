import hashlib
import os
import time

from pyspark.sql import functions as F

from conftest import MASTER_HEADER, write_source
from maude.bronze.ingest import ingest
from maude.bronze.manifest import Manifest
from maude.bronze.parse import parse_file, parse_pending
from maude.run_context import RunContext
from maude.sources import DEVICE_PROBLEM_COLUMNS, SourceFile, files_for


def _run() -> RunContext:
    return RunContext.start("test")


def test_ingest_lands_each_file_once_unchanged_and_read_only(cfg):
    results = ingest(cfg, _run(), files_for(cfg))

    assert {r["status"] for r in results} == {"landed"}
    landed = Manifest(cfg.manifest_path).landed()
    assert len(landed) == len(files_for(cfg))
    for event in landed:
        path = cfg.data_root / event["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == event["sha256"]
        assert not os.access(path, os.W_OK)


def test_rerun_does_not_download_unchanged_files(cfg):
    ingest(cfg, _run(), files_for(cfg))
    again = ingest(cfg, _run(), files_for(cfg))

    assert {r["status"] for r in again} == {"unchanged"}
    assert len(Manifest(cfg.manifest_path).landed()) == len(files_for(cfg))


def test_touched_but_identical_file_is_discarded_by_checksum(cfg, source_dir):
    ingest(cfg, _run(), files_for(cfg))
    later = time.time() + 3600
    os.utime(source_dir / "mdrfoiadd.zip", (later, later))

    statuses = {r["name"]: r["status"] for r in ingest(cfg, _run(), files_for(cfg))}
    assert statuses["mdrfoiadd"] == "duplicate"


def test_new_version_lands_beside_the_old_one(cfg, source_dir):
    ingest(cfg, _run(), files_for(cfg))
    write_source(source_dir, "mdrfoiadd", [MASTER_HEADER, "2001|R9|10/31/2026||M|M|NEW|10/31/2026"])

    statuses = {r["name"]: r["status"] for r in ingest(cfg, _run(), files_for(cfg))}
    assert statuses["mdrfoiadd"] == "landed"
    versions = [e for e in Manifest(cfg.manifest_path).landed() if e["name"] == "mdrfoiadd"]
    assert len(versions) == 2
    assert all((cfg.data_root / v["path"]).exists() for v in versions)


def test_parse_keeps_embedded_line_feeds_and_flags_malformed_records(spark, cfg):
    run = _run()
    ingest(cfg, run, files_for(cfg))
    parse_pending(spark, cfg, run)

    master = spark.read.parquet(str(cfg.bronze_table("master")))
    assert master.count() == 6

    added = {r["MDR_REPORT_KEY"]: r for r in master.filter("_source_name = 'mdrfoiadd'").collect()}
    assert added["1002"]["MANUFACTURER_NAME"] == "LINE\nBREAK INC"
    assert added["1001"]["MANUFACTURER_NAME"] == "ACME® MEDICAL"
    assert added["1001"]["_source_kind"] == "add"
    assert added["1001"]["_ingest_run_id"] == run.run_id
    assert added["1001"]["_source_file_id"].startswith("mdrfoiadd-")

    [malformed] = master.filter(~F.col("_is_well_formed")).collect()
    assert malformed["_raw_record"].startswith("1003|")
    assert malformed["_field_count"] == 9
    assert malformed["MDR_REPORT_KEY"] is None


def test_parse_decodes_lookups(spark, cfg):
    run = _run()
    ingest(cfg, run, files_for(cfg))
    parse_pending(spark, cfg, run)

    codes = spark.read.parquet(str(cfg.bronze_table("device_problem_codes")))
    assert "FDA_CODE" in codes.columns  # byte order mark removed
    assert codes.filter("FDA_CODE = '1005'").first()["TERM"] == "Measurements, inaccurate"

    classes = spark.read.parquet(str(cfg.bronze_table("product_classification")))
    assert classes.first()["DEVICENAME"] == "Protector’s Dental"  # cp1252 0x92


def test_header_less_file_uses_catalogue_columns(spark, cfg):
    run = _run()
    src = SourceFile("foidevproblem", "device_problem", "current", columns=DEVICE_PROBLEM_COLUMNS)
    ingest(cfg, run, [src])
    parse_pending(spark, cfg, run)

    rows = (
        spark.read.parquet(str(cfg.bronze_table("device_problem")))
        .orderBy("MDR_REPORT_KEY")
        .collect()
    )
    assert [(r["MDR_REPORT_KEY"], r["DEVICE_PROBLEM_CODE"]) for r in rows] == [
        ("1001", "1456"),
        ("1002", "3189"),
    ]


def test_parsing_again_replaces_rows_instead_of_duplicating_them(spark, cfg):
    run = _run()
    ingest(cfg, run, files_for(cfg))
    parse_pending(spark, cfg, run)
    assert parse_pending(spark, cfg, run) == []

    event = next(e for e in Manifest(cfg.manifest_path).landed() if e["name"] == "mdrfoiadd")
    parse_file(spark, cfg, event)
    assert spark.read.parquet(str(cfg.bronze_table("master"))).count() == 6
