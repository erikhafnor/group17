# AI use disclosure

The course requires us to disclose AI use with tool, purpose, scope and significant generated
artifacts. We remain responsible for understanding and validating everything in this repository.
We add an entry whenever we use an AI tool for something that ends up in the project.

| Date | Tool | Purpose and scope | Significant generated artifacts | How we validated it |
|---|---|---|---|---|
| 2026-10-26 | Claude Code (Anthropic, Claude Opus 5.5) | Shortlisting candidate datasets; profiling the October 2026 MAUDE files; scaffolding the repository after we chose MAUDE | `src/maude/` (Bronze ingest and parse, first Silver and Gold tables), `tests/`, `config/`, `.github/workflows/`, `docs/` (including the `project-overview.excalidraw` drawing), `report/` skeleton | Tests pass locally; a dev run on the real October 2026 files completed; both of us still have to review all generated code and text |
