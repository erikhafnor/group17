from datetime import date

import pytest

from maude.bronze.ingest import ingest
from maude.bronze.parse import parse_pending
from maude.gold.event_counts import build_monthly_event_counts
from maude.pipeline import run_pipeline
from maude.run_context import RunContext
from maude.silver.cleaning import parse_date
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
        "gold.monthly_event_counts",
    }
    assert summary == cfg.reports_dir / run.run_id / "summary.md"
    assert "**Status:** succeeded" in summary.read_text(encoding="utf-8")
    assert (cfg.logs_dir / f"{run.run_id}.log").exists()
