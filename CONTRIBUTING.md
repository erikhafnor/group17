# How we work

This is our working agreement for the project. Both of us have write access, and every change
reaches `main` through a reviewed pull request.

## Branches

```text
feature branch ──PR──▶ dev ──PR──▶ main
 (one task)       CI      dev run   approval ▶ prod run
```

- `main` is production. Changes arrive only through a pull request from `dev`.
- `dev` is where our work comes together. Every push runs the dev pipeline on the runner.
- Feature branches start from `dev` and hold one backlog item each, for example
  `feat/silver-devices`, `fix/patient-age-units` or `docs/report-bronze`.

## Day to day

```bash
git switch dev && git pull
git switch -c feat/silver-devices
# ... work, with `make lint test` before each commit ...
git push -u origin feat/silver-devices
gh pr create --base dev --fill
```

1. Open the pull request against `dev` and fill in the template.
2. CI (ruff and pytest) has to pass.
3. The other member reviews and approves; the author merges.
4. The merge to `dev` triggers the dev pipeline. Check its run summary.

## Releasing to production

1. Open a pull request from `dev` to `main` once the dev pipeline is green.
2. The other member reviews and merges.
3. The production run waits for approval in the `main` environment. The approval has to come
   from the member who did not trigger the run.
4. After approval, the production pipeline runs and publishes Gold.

The weekly scheduled production run needs no approval, because it only runs code that is already
on `main`.

## Conventions

- Commit messages: `type(scope): summary`, for example `feat(silver): add devices table`.
  Types: `feat`, `fix`, `docs`, `test`, `ci`, `refactor`, `perf`, `chore`.
- New cleaning rules come with a test and, where possible, a fixture row that shows the problem.
- Data, credentials and generated outputs never go into git. `.gitignore` excludes zip and
  Parquet files.
- Configuration lives in `config/*.toml`, not in code.
- `make format` before pushing keeps reviews free of formatting noise.
- AI use goes into [docs/ai-use.md](docs/ai-use.md) in the same pull request.

## Report

The report is LaTeX in `report/` (ACM `acmart` template, `sigconf` format). Each section is its
own file under `report/sections/`, so we can edit in parallel. The Report workflow builds the PDF
on every change; `make report` builds it locally if TeX Live is installed. Overleaf can also open
the folder, but `report/` in git stays the version we hand in.
