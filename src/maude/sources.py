"""Catalogue of the FDA source files and the file sets each environment ingests.

FDA publishes MAUDE as pipe-delimited text files inside zip archives:
https://www.fda.gov/medical-devices/medical-device-reporting-mdr-how-report-medical-device-problems/mdr-data-files
"""

from __future__ import annotations

from dataclasses import dataclass

from maude.config import Config

FILE_SETS = ("lookups", "history", "current", "monthly")

# foidevproblem*.txt is the only data file delivered without a header line.
DEVICE_PROBLEM_COLUMNS = ("MDR_REPORT_KEY", "DEVICE_PROBLEM_CODE")

# When one report appears in several files, the later kind is the newer version.
KIND_RANK = {"lookup": 0, "history": 1, "annual": 1, "current": 2, "add": 3, "change": 4}


@dataclass(frozen=True)
class SourceFile:
    name: str  # zip file name without extension, e.g. "mdrfoiadd"
    family: str  # the Bronze table the file is parsed into
    kind: str  # one of KIND_RANK
    fmt: str = "pipe"  # "pipe" or "csv"
    encoding: str = "cp1252"
    columns: tuple[str, ...] | None = None  # set only when the file may lack a header
    classification: bool = False  # served from FDA's premarket area, not the MAUDE area


# (zip stem, Bronze family) for the four report families that have monthly add/change files
_MONTHLY_FAMILIES = (
    ("mdrfoi", "master"),
    ("device", "device"),
    ("patient", "patient"),
    ("foitext", "text"),
)


def files_for(cfg: Config) -> list[SourceFile]:
    unknown = set(cfg.file_sets) - set(FILE_SETS)
    if unknown:
        raise ValueError(f"unknown file sets {sorted(unknown)}; expected a subset of {FILE_SETS}")

    files: list[SourceFile] = []
    if "lookups" in cfg.file_sets:
        files += [
            SourceFile(
                cfg.device_problem_codes,
                "device_problem_codes",
                "lookup",
                fmt="csv",
                encoding="utf-8-sig",
            ),
            SourceFile(
                cfg.patient_problem_codes,
                "patient_problem_codes",
                "lookup",
                fmt="csv",
                encoding="utf-8-sig",
            ),
            SourceFile("foiclass", "product_classification", "lookup", classification=True),
        ]
    if "history" in cfg.file_sets:
        through = cfg.history_through
        files += [
            SourceFile(f"mdrfoithru{through}", "master", "history"),
            SourceFile(f"patientthru{through}", "patient", "history"),
            SourceFile(
                f"foidevproblem_thru{through}",
                "device_problem",
                "history",
                columns=DEVICE_PROBLEM_COLUMNS,
            ),
            SourceFile(f"patientproblemcode_thru{through}", "patient_problem", "history"),
        ]
        for year in cfg.history_years:
            files += [
                SourceFile(f"device{year}", "device", "annual"),
                SourceFile(f"foitext{year}", "text", "annual"),
            ]
    if "current" in cfg.file_sets:
        files += [SourceFile(stem, family, "current") for stem, family in _MONTHLY_FAMILIES]
        files += [
            SourceFile(
                "foidevproblem", "device_problem", "current", columns=DEVICE_PROBLEM_COLUMNS
            ),
            SourceFile("patientproblemcode", "patient_problem", "current"),
        ]
    if "monthly" in cfg.file_sets:
        for stem, family in _MONTHLY_FAMILIES:
            files += [
                SourceFile(f"{stem}add", family, "add"),
                SourceFile(f"{stem}change", family, "change"),
            ]
    return files
