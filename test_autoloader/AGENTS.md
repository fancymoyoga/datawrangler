# Declarative Automation Bundles Project

This project uses Declarative Automation Bundles (formerly Databricks Asset Bundles) for deployment.

It runs one job, `autoloader_job` ([resources/autoloader.job.yml](resources/autoloader.job.yml)), which
uses Auto Loader ([src/load_files.py](src/load_files.py)) to load the clinical_trials landing JSON
files into `clinicaltrials.autoloader`:

- `study_files_raw`: one row per `studies/NCT*/*.json` file
- `search_studies_raw`: one row per study in a `studies/search/*.json` page

Task `flatten_studies` ([src/flatten_studies.py](src/flatten_studies.py)) then streams both into
`studies` (one row per snapshot, nested fields as columns) and one `study_<array>` table per
array in `protocolSection`, keyed by `nct_id` + `ingest_id`. Snapshots repeat: take the latest
`ingested_at` per `nct_id`.

Checkpoints and inferred schemas live in the `clinicaltrials.autoloader.checkpoints` volume.
Delete a stream's folder there (and drop its table) to reload all of its files from scratch.

Schema evolution: `payload`/`study` are inferred structs. New fields stop the stream
(`addNewColumns`); `write_table` restarts it and `mergeSchema` adds the column to the table.

```bash
databricks bundle validate -t dev --profile monicaenders
databricks bundle deploy -t dev --profile monicaenders
databricks bundle run -t dev autoloader_job --profile monicaenders
```

## Prerequisites

Install the Databricks CLI (>= v0.288.0) if not already installed:
- macOS: `brew tap databricks/tap && brew install databricks`
- Linux: `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh`
- Windows: `winget install Databricks.DatabricksCLI`

Verify: `databricks -v`

## For AI Agents

Read the `databricks-core` skill for CLI basics, authentication, and deployment workflow.
Read the `databricks-jobs` skill for job-specific guidance.

If skills are not available, install them: `databricks aitools install`
