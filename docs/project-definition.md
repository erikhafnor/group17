# Project definition: FDA MAUDE medallion pipeline

**Group 17:** Erik Hafnor, Martin Sævareid Lauritsen · **Status:** draft for the project check-in

The six sections follow the course's *Data Engineering Project Definition Guide*. The Silver
section carries the most detail, followed by Bronze and then Gold.

## 1. Project statement

We build a batch pipeline that turns the U.S. Food and Drug Administration's public MAUDE
database of medical device adverse event reports into trusted, analysis-ready tables. Our
intended consumer is a hospital medical-technology department or a device safety officer who
needs to know which device types and manufacturers generate reports of deaths, injuries and
malfunctions, how this develops month by month, and which device problems dominate for a given
device type. The final outcome is a small set of Gold tables that feed a dashboard.

Outside our scope: real-time processing, language modelling of the narrative texts, and any
claim about incidence or causality. MAUDE has no denominator (devices in use), and reporting is
passive and incomplete, so our outputs describe *reports*, not risk.

## 2. Dataset

- **Source:** FDA MDR data files (MAUDE), pipe-delimited text inside zip archives, downloadable
  without registration. Six families share the key `MDR_REPORT_KEY`: master (one row per report;
  23.6 million up to 2025), device, patient, narrative text, device problem codes and patient
  problem codes. Lookups give problem-code terms, and FDA's product classification file
  (`foiclass`) maps product codes to device name, class and medical specialty.
- **Refresh pattern:** cumulative history files, current-year files, and a monthly `add` and
  `change` file per family. The files published on 20 October 2026 held 432,036 new and 672,141
  changed reports.
- **Volume:** the master history alone is 6.6 GB uncompressed; one year of device records is
  about 2.9 million rows and one year of narratives about 6 million rows. Joins on
  `MDR_REPORT_KEY` between multi-million-row families, version resolution and aggregation by
  product code make this a natural fit for Spark.
- **Quality risks we have already measured** (details in [source-profile.md](source-profile.md)):
  cp1252 encoding; records that end in CRLF but contain bare LF inside fields; unquoted `|`
  inside values; two date formats; free-text ages and weights with units; 47 spellings of
  "Medtronic" in one month of device records; change files that re-deliver existing reports; and
  FDA documentation that no longer matches the files.
- **Schema change:** fields were added in 2006, pre-2009 device records carry an extra baseline
  section, and file names roll over every year (`mdrfoithru2025` becomes `mdrfoithru2026`).
- **Access and reproducibility:** every download is recorded with URL, Last-Modified, size and
  SHA-256, and the original zip files are kept read-only, so Bronze can always be rebuilt.

## 3. Bronze layer: ingest and preserve

- **Preserved unchanged:** every source zip, byte for byte, read-only, at
  `bronze/landing/<family>/<name>/<sha256[:12]>/<name>.zip`.
- **Method and schedule:** batch HTTP download. FDA updates monthly on no fixed day, so we check
  weekly. A HEAD request compares Last-Modified and size with earlier downloads; unchanged files
  are not downloaded again, and a file with an already-known checksum is discarded.
- **Organisation:** one Parquet table per family, partitioned by source file, with every value
  kept as the original string. Only the text encoding is normalised (cp1252 to UTF-8).
- **Traceability metadata:** each row carries `_source_file_id`, `_source_name`, `_source_kind`,
  `_source_modified_at`, `_ingest_run_id` and `_ingested_at`; an append-only manifest records
  each file's `landed` and `parsed` events.
- **Duplicates, late data, schema changes:** the same content never lands twice; a new version of
  a file lands beside the old one; late corrections arrive in change files and are resolved in
  Silver; headers are read per file, so new or renamed columns flow into Bronze, and Silver reads
  with schema merging.
- **Structural validation:** records are split on CRLF only, so embedded line feeds stay inside
  their record. A record with the wrong number of fields is kept, flagged
  `_is_well_formed = false`, and its raw line preserved for Silver to quarantine.
- **Test and deploy:** unit tests with synthetic fixtures that reproduce each quirk; CI on every
  pull request; the dev pipeline runs on every push to `dev`, and production runs after an
  approved deployment from `main`.

**Outcome:** raw data is reproducibly ingested, traceable to its source, and ready for controlled
processing.

## 4. Silver layer: primary focus

**Data understanding.** A master record is one *version* of one report: an initial report or a
follow-up that replaces it. A device record is one device in a report; a patient record is one
patient; a text record is one narrative section. Essential fields: report key, report number,
date received, event type, report source, product code, manufacturer, brand and model.
Optional: UDI, lot and catalogue numbers. Derived: event-to-receipt delay, age in years.
Unreliable: date of event (often blank), reporter details, and free-text age and weight.

**Data model and grain.**

| Table | Grain |
|---|---|
| `reports` | one row per `mdr_report_key`, newest version |
| `devices` | one row per (`mdr_report_key`, `device_sequence_no`) |
| `patients` | one row per (`mdr_report_key`, `patient_sequence_number`) |
| `report_device_problems`, `report_patient_problems` | one row per report and problem code |
| `narratives` (prod only) | one row per `mdr_text_key` |
| `product_classification`, `problem_codes`, `manufacturers` | reference tables keyed by code or raw name |

Silver keeps the current version of each report (type 1 history); every delivered version stays
in Bronze, so the history can be rebuilt if we need it.

**Cleaning.**
- *Rejected to quarantine, with a reason:* malformed field count, non-numeric key, missing or
  unparseable date received, date received in the future.
- *Corrected:* trimmed whitespace, empty strings to null, upper-case codes, line breaks inside
  short text fields replaced by spaces.
- *Flagged, kept:* date of event after date received, age above 120 years, product code missing
  from the classification, event type outside {D, IN, M, O, \*}.
- *Versions:* the newest version of each report wins, ordered by date changed, then file kind
  (change > add > current > history), then file publication time.

**Standardisation.** snake_case names; dates to `DATE` from `MM/dd/yyyy` and `yyyy/MM/dd`; ages
(`YR`, `MO`, `DA`) to years; weights (`LB`, `KG`) to kilograms; Y/N flags to booleans;
multi-valued fields (report type, patient outcomes) to arrays; manufacturer names to a canonical
form (upper case, punctuation and legal suffixes removed, then a curated mapping for the largest
manufacturers).

**Enrichment.** Product classification (device class, medical specialty, implant and
life-sustaining flags) and problem-code terms by broadcast join; derived report month, event
flags (death, injury, malfunction) and event-to-receipt delay.

**Quality evidence.** Every run writes a quality report with, per table: input rows, malformed
and rejected rows by reason, superseded versions, output rows, distinct keys = rows, null rates of
essential fields, and referential integrity (devices with a known report, product codes found in
the classification). We collect these across runs as the before/after evidence for the report.

**Implementation.** Bronze parsing uses basic transformations on text lines (custom record
separator, split, size check) and actions for counts. Silver and Gold use the DataFrame API and
Spark SQL, so the optimiser can prune columns and push down filters. We use built-in functions
and broadcast joins instead of Python UDFs on hot paths.

**Performance.** We expect the costs to sit in full scans of the master and text files, the
shuffle on `mdr_report_key` for version resolution, and the large report–device join. Product
codes are skewed: one code accounted for 20% of a month's device records. We will measure
first (stage timings in the run report, Spark UI, `explain()` plans) and then try only the
justified improvements: column pruning, partitioning Silver by received month, co-partitioning
reports and devices by key, broadcast lookups, and adaptive skew handling.

**Lifecycle.** Each rule has a unit test with a fixture; every change goes through a pull request
reviewed by the other group member; runs are idempotent (Silver is rebuilt by overwrite); the
GitHub job summary and the uploaded quality report make each run's results visible.

**Scalability.** Larger volumes mean more partitions and, if needed, a bigger VM or cluster,
with no code change. Late data is absorbed by version resolution. New source columns land in
Bronze automatically, while Silver selects columns explicitly, so they cannot break downstream
tables.

**Outcome:** trusted, validated, documented and reusable tables that support several downstream
uses.

## 5. Gold layer: serve the outcome

- **Question:** which device types and manufacturers account for reports of deaths, injuries and
  malfunctions, how does this develop month by month, and which device problems are typical for
  each device type?
- **Tables:** `monthly_product_events` (month × product code × event type, with device class and
  specialty); `manufacturer_product_summary` (last 12 months against the previous 12 months);
  `product_problem_profile` (product code × device problem, with share of reports and a
  proportional reporting ratio).
- **Why Gold differs from Silver:** small, denormalised, pre-aggregated tables sized for a
  dashboard; Silver stays at report level for other uses.
- **Correctness:** totals reconcile with Silver before publishing; selected product codes are
  spot-checked against FDA's MAUDE web search.
- **Refresh and failure handling:** Gold refreshes after each successful scheduled run. If an
  upstream check fails, the run stops and the previous Gold tables stay in place.

**Outcome:** a curated dataset that feeds a defined dashboard for hospital device safety work.

## 6. End-to-end delivery

- **Reproducible:** another engineer can clone, `make venv`, `make test` and `make run ENV=dev`;
  data never enters git.
- **Environments:** `config/dev.toml` and `config/prod.toml`, separate data areas on the runner,
  and GitHub environments `dev` and `main`.
- **Automated checks:** lint and tests on GitHub-hosted runners for every pull request; the
  report's LaTeX build is checked as well.
- **Deployment and schedule:** a push to `dev` runs the dev pipeline; a merge to `main` runs
  production after the other member approves; a weekly scheduled production run picks up FDA's
  monthly release.
- **Visibility:** each run's status, quality report and log appear in the GitHub job summary and
  as artifacts; rejected rows sit in quarantine tables.
- **Ownership:** each of us owns a stream of work (see [backlog.md](backlog.md)) and reviews the
  other's pull requests.
