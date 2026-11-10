from datetime import date

import pytest

from maude.bronze.ingest import ingest
from maude.bronze.parse import parse_pending
from maude.gold.event_counts import build_monthly_event_counts
from maude.gold.product_events import build_monthly_product_events
from maude.pipeline import run_pipeline
from maude.run_context import RunContext
from maude.silver.cleaning import parse_date
from maude.silver.devices import build_devices
from maude.silver.product_classification import build_product_classification
from maude.silver.reports import build_reports
from maude.sources import files_for


@pytest.fixture
def bronze(spark, cfg):
    run = RunContext.start("test")
    ingest(cfg, run, files_for(cfg))
    parse_pending(spark, cfg, run)
    return cfg


def test_parse_date_accepts_both_maude_patterns(spark):
    df = spark.createDataFrame([("09/30/2026",), ("2026/09/30",), ("",), ("31/31/2026",)], ["d"])
    assert [r[0] for r in df.select(parse_date("d")).collect()] == [
        date(2026, 9, 30),
        date(2026, 9, 30),
        None,
        None,
    ]


def test_reports_keep_the_newest_version_of_each_report(spark, bronze):
    metrics = build_reports(spark, bronze)

    rows = {
        r["mdr_report_key"]: r
        for r in spark.read.parquet(str(bronze.silver_table("reports"))).collect()
    }
    assert set(rows) == {900, 1001, 1002}
    assert rows[1001]["event_type"] == "D"  # the change file's version wins
    assert rows[1001]["date_changed"] == date(2026, 10, 2)
    assert rows[900]["date_received"] == date(2001, 1, 15)
    assert metrics == {
        "input_rows": 6,
        "rejected_rows": 2,
        "output_rows": 3,
        "distinct_keys": 3,
        "missing_date_received": 0,
        "superseded_versions": 1,
    }


def test_rejected_records_are_quarantined_with_a_reason(spark, bronze):
    build_reports(spark, bronze)
    quarantine = spark.read.parquet(str(bronze.quarantine_table("reports")))
    assert sorted(r["_reject_reason"] for r in quarantine.collect()) == [
        "invalid_report_key",
        "malformed_record",
    ]


def test_gold_counts_reconcile_with_silver(spark, bronze):
    build_reports(spark, bronze)
    metrics = build_monthly_event_counts(spark, bronze)

    gold = {
        (r["month"], r["event_type"]): r["reports"]
        for r in spark.read.parquet(str(bronze.gold_table("monthly_event_counts"))).collect()
    }
    assert gold == {
        (date(2026, 9, 1), "death"): 1,
        (date(2026, 9, 1), "malfunction"): 1,
        (date(2001, 1, 1), "malfunction"): 1,
    }
    assert metrics["reports_counted"] == 3


def test_pipeline_runs_end_to_end_and_writes_a_run_report(spark, cfg):
    run, metrics, summary = run_pipeline(cfg, spark=spark)

    assert set(metrics) == {
        "bronze.ingest",
        "bronze.parse",
        "silver.reports",
        "silver.product_classification",
        "silver.devices",
        "gold.monthly_event_counts",
        "gold.monthly_product_events",
    }
    assert summary == cfg.reports_dir / run.run_id / "summary.md"
    assert "**Status:** succeeded" in summary.read_text(encoding="utf-8")
    assert (cfg.logs_dir / f"{run.run_id}.log").exists()


@pytest.fixture
def silver(spark, bronze):
    build_reports(spark, bronze)
    build_product_classification(spark, bronze)
    build_devices(spark, bronze)
    return bronze


def test_product_classification_has_one_row_per_code_with_boolean_flags(spark, bronze):
    metrics = build_product_classification(spark, bronze)

    rows = {
        r["product_code"]: r
        for r in spark.read.parquet(str(bronze.silver_table("product_classification"))).collect()
    }
    assert set(rows) == {"BRW", "FRN"}
    assert rows["FRN"]["device_class"] == "2"
    assert rows["FRN"]["is_life_sustaining"] is True
    assert rows["BRW"]["is_implant"] is False
    assert metrics["input_rows"] == 4
    assert metrics["rejected_rows"] == 2
    assert metrics["output_rows"] == 2
    quarantine = spark.read.parquet(str(bronze.quarantine_table("product_classification")))
    assert sorted(r["_reject_reason"] for r in quarantine.collect()) == [
        "malformed_record",
        "missing_product_code",
    ]


def test_devices_come_from_the_newest_delivery_of_each_report(spark, silver):
    rows = {
        (r["mdr_report_key"], r["device_sequence_no"]): r
        for r in spark.read.parquet(str(silver.silver_table("devices"))).collect()
    }
    # 1001 was re-delivered under a new sequence number: only the change file's row stays.
    assert set(rows) == {(1001, 21), (1002, 12), (5555, 14), (900, 22), (900, 23)}
    assert rows[(1001, 21)]["product_code"] == "FRN"
    assert rows[(1001, 21)]["device_class"] == "2"
    assert rows[(1001, 21)]["date_received"] == date(2026, 9, 30)
    assert rows[(1002, 12)]["product_code"] is None  # "---" means missing
    assert rows[(900, 22)]["_unknown_product_code"] is True
    assert rows[(900, 23)]["medical_specialty"] == "AN"


def test_device_metrics_count_rejects_versions_and_integrity(spark, bronze):
    build_reports(spark, bronze)
    build_product_classification(spark, bronze)
    metrics = build_devices(spark, bronze)

    assert metrics == {
        "input_rows": 7,
        "rejected_rows": 1,
        "output_rows": 5,
        "distinct_reports": 4,
        "distinct_keys": 5,
        "missing_product_code": 1,
        "unknown_product_code": 1,
        "devices_without_report": 1,
        "superseded_rows": 1,
    }


def test_product_events_count_each_report_once_and_reconcile(spark, silver):
    metrics = build_monthly_product_events(spark, silver)

    gold = {
        (r["month"], r["product_code"], r["device_class"], r["event_type"]): r["reports"]
        for r in spark.read.parquet(str(silver.gold_table("monthly_product_events"))).collect()
    }
    assert gold == {
        (date(2026, 9, 1), "FRN", "2", "death"): 1,
        (date(2026, 9, 1), None, None, "malfunction"): 1,
        (date(2001, 1, 1), "ZZZ", None, "malfunction"): 1,
    }
    assert metrics["reconciled"] is True
    assert metrics["reports_counted"] == metrics["silver_reports_with_date"] == 3
    assert metrics["reports_without_product_code"] == 1
    assert metrics["reports_with_several_product_codes"] == 1
