"""Run the Bronze -> Silver -> Gold stages for one environment."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from maude.bronze.ingest import ingest
from maude.bronze.parse import parse_pending
from maude.config import Config
from maude.gold.event_counts import build_monthly_event_counts
from maude.quality import write_run_report
from maude.run_context import RunContext
from maude.silver.reports import build_reports
from maude.sources import files_for
from maude.spark import get_spark

STAGES = ("bronze", "silver", "gold")

log = logging.getLogger("maude")


def _configure_logging(cfg: Config, run: RunContext) -> Path:
    cfg.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = cfg.logs_dir / f"{run.run_id}.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
    log.handlers[:] = handlers
    log.setLevel(logging.INFO)
    return log_file


def run_pipeline(
    cfg: Config, stages: tuple[str, ...] = STAGES, spark: SparkSession | None = None
) -> tuple[RunContext, dict, Path]:
    """Run the requested stages in order; returns the run, its metrics and the summary path.

    A Spark session passed in by the caller is left running; one created here is stopped.
    """
    run = RunContext.start(cfg.env)
    log_file = _configure_logging(cfg, run)
    log.info("run %s: stages=%s data_root=%s", run.run_id, ",".join(stages), cfg.data_root)

    metrics: dict = {}
    status = "failed"
    owns_spark = spark is None
    try:
        if "bronze" in stages:
            metrics["bronze.ingest"] = ingest(cfg, run, files_for(cfg))
        if spark is None:
            # Created after ingestion so a download failure surfaces before Spark starts.
            spark = get_spark(cfg)
        if "bronze" in stages:
            metrics["bronze.parse"] = parse_pending(spark, cfg, run)
        if "silver" in stages:
            metrics["silver.reports"] = build_reports(spark, cfg)
        if "gold" in stages:
            metrics["gold.monthly_event_counts"] = build_monthly_event_counts(spark, cfg)
        status = "succeeded"
    finally:
        summary = write_run_report(cfg.reports_dir, run.run_id, status, metrics)
        log.info("run %s %s; log %s; summary %s", run.run_id, status, log_file, summary)
        if owns_spark and spark is not None:
            spark.stop()
    return run, metrics, summary
