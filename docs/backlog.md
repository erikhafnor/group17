# Backlog

What exists today is a thin vertical slice: Bronze for every source family, one Silver table
(`reports`) and one Gold table (`monthly_event_counts`), all running in CI and on the runner.
The work below builds it out to the design in [project-definition.md](project-definition.md).

We split the work into two streams so we rarely edit the same files. Each item is one branch and
one pull request, reviewed by the other person.

## Stream A: reports, patients and quality evidence

- [ ] `silver.reports`: add the remaining essential columns (report type as array, manufacturer
      name, PMA/510(k) number, flags) and the rules from section 4 (future dates, event type
      domain)
- [ ] `silver.patients`: age to years, weight to kg, outcomes to an array; quarantine the two
      malformed records per month
- [ ] `silver.report_patient_problems` and `silver.problem_codes` (device and patient terms)
- [ ] Null-rate and referential-integrity metrics in the run report
- [ ] Collect quality metrics across runs (one table per run under `reports/`), for the report's
      before/after evidence
- [ ] Spark profiling job that reproduces the figures in [source-profile.md](source-profile.md)

## Stream B: devices, manufacturers and Gold

- [ ] `silver.devices`: one row per report and device sequence; product code, brand, model, UDI
- [ ] `silver.product_classification` from `foiclass` (class, specialty, implant and
      life-sustaining flags)
- [ ] `silver.manufacturers`: canonical names (normalisation, plus a curated map for the largest
      manufacturers)
- [ ] `silver.report_device_problems`
- [ ] `gold.monthly_product_events`
- [ ] `gold.manufacturer_product_summary`
- [ ] `gold.product_problem_profile` with proportional reporting ratio

## Shared

- [ ] Check disk space on `group17-runner`, then widen `history_years` in `config/prod.toml`
- [ ] Performance: profile the first full prod run (stage timings, Spark UI, `explain()`), then
      test one improvement at a time and record before/after
- [ ] Dashboard or notebook on the Gold tables for the demonstration
- [ ] Report (written outside the repository)
- [ ] Ten-minute presentation
- [ ] Yearly rollover: update `history_through` and the lookup names when FDA publishes the 2026
      history files
