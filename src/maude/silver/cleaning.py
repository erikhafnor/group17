"""Reusable column rules shared by the Silver tables."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# MAUDE writes most dates as MM/dd/yyyy, but the device file uses yyyy/MM/dd.
DATE_PATTERNS = ("MM/dd/yyyy", "yyyy/MM/dd")


def clean_string(name: str) -> Column:
    """Trim whitespace and turn empty strings into nulls."""
    value = F.trim(F.col(name))
    return F.when(value == "", None).otherwise(value)


def parse_date(name: str) -> Column:
    """Parse a MAUDE date in any of the known patterns; unparseable values become null."""
    value = clean_string(name)
    return F.coalesce(*[F.to_date(value, pattern) for pattern in DATE_PATTERNS])
