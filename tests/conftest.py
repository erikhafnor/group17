"""Shared fixtures: a local Spark session and a miniature FDA download area.

The fixture files are small, synthetic, and reproduce the source quirks we
found in the real October 2026 files: cp1252 text, CRLF record endings with
bare LF inside a field, unquoted '|' inside a value, and lookups with a BOM.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from maude.config import Config, load_config
from maude.spark import get_spark

MASTER_HEADER = (
    "MDR_REPORT_KEY|REPORT_NUMBER|DATE_RECEIVED|DATE_OF_EVENT|EVENT_TYPE|"
    "REPORT_SOURCE_CODE|MANUFACTURER_NAME|DATE_CHANGED"
)

SOURCE_FILES: dict[str, tuple[str, str, list[str]]] = {
    # zip stem: (member name, encoding, records)
    "mdrfoiadd": (
        "mdrfoiAdd.txt",
        "cp1252",
        [
            MASTER_HEADER,
            "1001|R1|09/30/2026|09/22/2026|IN|M|ACME® MEDICAL|09/30/2026",
            "1002|R2|09/30/2026||M|D|LINE\nBREAK INC|09/30/2026",
            "1003|R3|09/30/2026|09/01/2026|M|M|PIPE|IN NAME CO|09/30/2026",
            "ABC|R4|09/30/2026||M|M|BAD KEY CORP|09/30/2026",
        ],
    ),
    "mdrfoichange": (
        "mdrfoiChange.txt",
        "cp1252",
        [
            MASTER_HEADER,
            "1001|R1|09/30/2026|09/22/2026|D|M|ACME® MEDICAL|10/02/2026",
            "900|R0|01/15/2001|01/01/2001|M|M|OLD CO|10/02/2026",
        ],
    ),
    "deviceadd": (
        "DEVICEAdd.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DATE_RECEIVED|BRAND_NAME|MANUFACTURER_D_NAME",
            "1001|1|2026/09/30|Pump Pro®|ACME MEDICAL",
        ],
    ),
    "devicechange": (
        "DEVICEChange.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|DEVICE_SEQUENCE_NO|DATE_RECEIVED|BRAND_NAME|MANUFACTURER_D_NAME",
        ],
    ),
    "patientadd": (
        "patientAdd.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER|DATE_RECEIVED|PATIENT_AGE|PATIENT_SEX",
            "1001|1|09/30/2026|34 YR|Male",
        ],
    ),
    "patientchange": (
        "patientChange.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|PATIENT_SEQUENCE_NUMBER|DATE_RECEIVED|PATIENT_AGE|PATIENT_SEX",
        ],
    ),
    "foitextadd": (
        "foitextAdd.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|PATIENT_SEQUENCE_NUMBER|DATE_REPORT|FOI_TEXT",
            "1001|5001|D|1||PUMP STOPPED.\nALARM SOUNDED.",
        ],
    ),
    "foitextchange": (
        "foitextChange.txt",
        "cp1252",
        [
            "MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|PATIENT_SEQUENCE_NUMBER|DATE_REPORT|FOI_TEXT",
        ],
    ),
    "foidevproblem": ("foidevproblem.txt", "cp1252", ["1001|1456", "1002|3189"]),
    "deviceproblemcodes2025": (
        "deviceproblemcodes2025.csv",
        "utf-8-sig",
        [
            "FDA_CODE,TERM,NCIT_CODE,IMDRF_CODE",
            '1005,"Measurements, inaccurate",,',
            "1456,Failure to Pump,,A0601",
        ],
    ),
    "patientproblemcodes2026": (
        "patientproblemcodes2026.csv",
        "utf-8-sig",
        [
            "FDA_CODE,TERM,NCIT_CODE,IMDRF_CODE",
            "1125,Contraindicated patient,,",
        ],
    ),
    "foiclass": (
        "foiclass.txt",
        "cp1252",
        [
            "REVIEW_PANEL|MEDICALSPECIALTY|PRODUCTCODE|DEVICENAME|DEVICECLASS",
            "AN|AN|BRW|Protector’s Dental|1",
        ],
    ),
}


def write_source(directory: Path, stem: str, records: list[str] | None = None) -> Path:
    member, encoding, default_records = SOURCE_FILES[stem]
    text = "".join(r + "\r\n" for r in (records if records is not None else default_records))
    path = directory / f"{stem}.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text.encode(encoding))
    return path


@pytest.fixture(scope="session")
def spark():
    session = get_spark(load_config("test"))
    yield session
    session.stop()


@pytest.fixture
def source_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "source"
    directory.mkdir()
    for stem in SOURCE_FILES:
        write_source(directory, stem)
    return directory


@pytest.fixture
def cfg(tmp_path: Path, source_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    monkeypatch.setenv("MAUDE_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("MAUDE_SOURCE_BASE_URL", str(source_dir))
    return load_config("test")
