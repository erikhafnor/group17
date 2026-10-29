# Source profile: what the MAUDE files actually look like

We profiled the monthly files FDA published on 20 October 2026, before writing any parsing code,
plus the headers of the history files. Every finding below shaped a design decision; the right
column names it.

| Finding | Evidence | Design decision |
|---|---|---|
| Text is cp1252, not UTF-8 | `®` stored as byte 0xAE in device brand names; `foiclass` uses cp1252 smart quotes (0x92–0x94) | Decode as cp1252 in Bronze; bytes undefined in cp1252 fall back to latin-1 and are counted |
| Records end with CRLF, yet some fields contain a bare LF | 48 master records in `mdrfoiadd`, 135 in `mdrfoichange`; a reader that splits on any line break turns each into two broken rows (61 + 26 fields) | Split records on CRLF only; all 183 stay whole |
| Fields are never quoted | 0 double quotes in `mdrfoiadd`; 2 of 432,022 patient records contain a `|` inside free text (11 instead of 10 fields) | Flag the record as malformed, keep its raw line, quarantine it in Silver |
| The change file re-delivers reports | 405,111 of the 672,141 report keys in `mdrfoichange` are also in the same month's `mdrfoiadd`; the patient and text change files also update reports received in 2001 | Resolve versions in Silver (newest version per key), never append |
| Two date formats | `MM/dd/yyyy` in master and patient files, `yyyy/MM/dd` in the device file; `yyyy/MM/dd HH:mm:ss` in patient problem codes | One date parser that accepts every known pattern |
| Ages and weights are free text with units | Patient age is blank in 68% of records; the rest end in `YR` (138,768), `MO` (242) or `DA` (14); weights look like `191 LB` | Convert to numeric years and kilograms in Silver |
| Manufacturer names vary | 1,552 distinct manufacturer names in one month of device records, 47 of them starting with "MEDTRONIC" | Canonical manufacturer table in Silver |
| Device types are skewed | 1,478 product codes in one month; the largest (`QBJ`) covers 86,162 of 432,036 device records (20%) | Watch for skew in product-code aggregations and joins |
| Headers are irregular | `MANUFACTURER_LINK_FLAG_` (trailing underscore), `UDI-DI` (hyphen), `DEVICE_EVALUATED_BY_MANUFACTUR` (truncated) | Keep original names in Bronze, rename to snake_case in Silver |
| FDA documentation is out of date | Documented with 2 fields, `patientproblemcode` has 5 (`MDR_REPORT_KEY`, `PATIENT_SEQUENCE_NO`, `PROBLEM_CODE`, `DATE_ADDED`, `DATE_CHANGED`); `foidevproblem` has no header at all | Read headers from the files; catalogue columns only for header-less files |
| Lookups differ in format | Problem-code lookups are comma-separated with a byte order mark and quoted terms (`"Measurements, inaccurate"`) | Separate CSV path in Bronze parsing |

## Volumes (October 2026 release)

| File | Compressed | Records |
|---|---:|---:|
| `mdrfoiadd` | 13.7 MB | 432,036 |
| `mdrfoichange` | 25.8 MB | 672,141 |
| `deviceadd` | 13.8 MB | 432,036 |
| `devicechange` | 24.1 MB | 672,177 |
| `patientadd` | 1.9 MB | 432,022 |
| `patientchange` | 2.5 MB | 506,060 |
| `foitextadd` | 29.0 MB | 362,847 |
| `foitextchange` | 73.1 MB | 663,481 |
| `mdrfoithru2025` (history) | 700 MB | 23.6 million (FDA's figure) |

Record counts for the monthly files come from our own Bronze run (`python -m maude --env dev`),
which took 45 seconds end to end on a laptop and produced 699,066 Silver reports.
