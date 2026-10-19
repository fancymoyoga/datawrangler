# Clinical Trials Lakehouse

## Goal

Ingest clinical trial records from the ClinicalTrials.gov v2 API into a medallion lakehouse on
Databricks. The gold table feeds an AI model.

- Source: `https://clinicaltrials.gov/api/v2/studies/{NCT_ID}` (JSON, one study per call)
- First study: `NCT03506373`; the design must scale to a list of NCT IDs
- Layers: bronze (raw) → silver (validated, deduplicated, PII-tagged) → gold (PII-free, AI-ready)

## Constraints

- **Databricks Free Edition**: serverless compute only. Never add cluster configs
  (`new_cluster`, `job_clusters`, `clusters`, instance pools, `node_type_id`).
  Pipelines use `serverless: true`; job tasks use serverless environments.
- **Catalog**: `workspace`.
- **Packaging**: everything is a Databricks Asset Bundle (`databricks.yml` + `resources/`).
  The `dev` target uses `mode: development`.
- **Bundle-owned storage**: the schema `clinical_trials` and the managed volume `landing` are
  bundle resources. Always reference them by resource interpolation, never by literal name, so
  dev-mode prefixes (`dev_<user>_clinical_trials`) flow through:
  - schema: `${resources.schemas.clinical_trials.name}`
  - volume: `${resources.volumes.landing.name}`
  - landing path: `/Volumes/${var.catalog}/${resources.schemas.clinical_trials.name}/${resources.volumes.landing.name}`
- **Transformations**: Lakeflow Spark Declarative Pipelines (SDP) for bronze/silver/gold, using
  the modern API (`from pyspark import pipelines as dp` / `CREATE OR REFRESH ...`), not legacy `dlt`.
- Workspace profile: `monicaenders` (`https://dbc-2831e5fc-ccb1.cloud.databricks.com`).
  Pass `--profile monicaenders` on every CLI call.

## Layer design

### Landing (ingestion, outside the pipeline)

A serverless job task calls the API for each NCT ID and writes the raw response unchanged to
`<landing>/studies/<NCT_ID>/<fetch_timestamp>.json`. Fetches are append-only; re-fetching a study
writes a new file rather than overwriting, so history is kept and bronze stays replayable.
The NCT ID list is a job parameter (default: `NCT03506373`).

Verified 2026-10-18: Free Edition serverless compute reaches the API directly (`requests.get`,
no extra dependencies; HTTP 200, ~40 KB JSON, ~0.1 s for NCT03506373). The response has
top-level `protocolSection`, `resultsSection`, `documentSection`, `derivedSection`, `hasResults`.

### Bronze: `bronze_studies` (streaming table)

- Auto Loader (`read_files` / `cloudFiles`, JSON) over the `studies/` folder of the landing volume.
- One row per fetched file: the raw study JSON kept intact (whole document as a VARIANT or
  string column), plus `_source_file`, `_file_modified_at`, `_ingested_at`.
- No parsing or filtering beyond what's needed to read the file. Rescued data kept.

### Silver: `silver_studies` (and child tables as needed)

- Parse the nested `protocolSection` / `derivedSection` / `hasResults` into typed columns:
  NCT ID, titles, status, phases, study type, conditions, interventions, enrollment, key dates,
  eligibility criteria, sponsors, outcome measures, brief summary / detailed description.
- **Validate** with expectations: `nct_id` present and matching `^NCT\d{8}$` (drop),
  parseable dates, known `overall_status` values (warn).
- **Deduplicate** to one current row per `nct_id` with Auto CDC (SCD Type 1), sequenced by the
  study's `lastUpdatePostDate`, then by fetch time.
- **PII tagging**: columns holding personal data get a Unity Catalog column tag `pii` (with a
  `pii_type` such as `name`, `email`, `phone`) and a column comment. PII in this source:
  `contactsLocationsModule.centralContacts` and `overallOfficials` (names, emails, phones,
  affiliations), per-site `locations[].contacts`, and the `responsibleParty` investigator name.
  Site facility/city/country is not personal data.

### Gold: `gold_studies_ai` (materialized view)

- Built from silver with **every PII-tagged column excluded**. A single study per row.
- Flattened, model-friendly fields: identifiers, status, phase, conditions, interventions,
  enrollment, dates, eligibility (structured fields + criteria text), outcomes, and a
  concatenated `document_text` field (title + summary + description + eligibility) for
  embedding / LLM input.
- Expectation guard: fail the update if any known PII column name appears in the output.

## As built

The design above is implemented with these refinements:

- Landing files: `<volume>/studies/{NCT_ID}/{yyyyMMddTHHmmssZ}_{NCT_ID}.json`, an envelope of
  `requested_nct_id, source_url, http_status, ingested_at, ingest_id, payload`. Non-200 responses
  are landed too. Files are written as `_*.tmp` and renamed, so Auto Loader never sees partial files.
- Bronze `bronze_studies_raw`: Auto Loader `binaryFile`, so nothing is parsed or dropped.
- Silver: `silver_studies_validated` (critical rules via `expect_all_or_drop`, warnings via
  `expect_all`), `silver_studies_quarantine` (same parse, `failed_rules` array),
  `silver_studies` (one row per study), `silver_study_contacts` (PII), `silver_study_locations`.
  Dedup is a window over validated (newest `last_update_posted_date`, then `ingested_at`), not Auto CDC.
  Rules live in `src/pipeline/transforms.py`; NULL rule results count as failures.
- PII tagging is a separate notebook (`02_tag_pii.py`), not in the pipeline: UC column tags
  `pii` / `pii_type` / `pii_detection`, table tag `contains_pii`, audit in `pii_tag_audit`. It
  removes tags a run no longer confirms and skips pipeline-internal `__materialization_*` tables.
- Gold `gold_study_documents`: allow-listed columns only; free text and `document_text` scrubbed of
  emails/phones (`EMAIL_PATTERN` / `PHONE_PATTERN`, shared with the PII scanner); rows under
  200 chars dropped; CDF on for Vector Search.
- Vector Search: endpoint `clinical-trials-vs` (STANDARD), Delta Sync index
  `<schema>.gold_study_documents_index` on `document_text` with `databricks-gte-large-en`.
- Agent: `src/agent/agent.py` (MLflow `ResponsesAgent`): retrieves from the gold index, answers
  with `databricks-meta-llama-3-3-70b-instruct`, registered as `<schema>.clinical_trials_agent`.
  Declared resources are the gold index and the two model endpoints only, never silver.

## Layout

```
clinical_trials/
├── databricks.yml                   # bundle, variables, sync excludes, dev target
├── resources/
│   ├── clinical_trials.schema.yml   # schema clinical_trials
│   ├── landing.volume.yml           # managed volume landing
│   ├── clinical_trials.pipeline.yml # SDP pipeline (serverless, triggered)
│   ├── clinical_trials.job.yml      # ingest → pipeline → tag PII → sync vector index
│   └── clinical_trials_agent.job.yml# log, register and test the agent
├── src/
│   ├── ctgov_client.py              # stdlib API client + envelope writer (shared)
│   ├── 01_ingest_api.py             # notebook: API → landing volume
│   ├── 02_tag_pii.py                # notebook: UC PII tags + audit, gold gate
│   ├── 03_vector_index.py           # notebook: VS endpoint + index, sync
│   ├── 04_log_agent.py              # notebook: log/register/test agent
│   ├── agent/agent.py               # ResponsesAgent
│   └── pipeline/                    # bronze.py, silver.py, gold.py, transforms.py
├── scripts/fetch_and_upload.py      # local fallback if compute can't reach the API
├── tests/test_transforms.py         # local PySpark test of transforms
├── load_study_contacts.py           # study_contacts FHIR loader (restored from clinicaltrials-001)
└── fetch_study_fhir.py
```

## Commands

```
databricks bundle validate --strict -t dev --profile monicaenders
databricks bundle deploy -t dev --profile monicaenders
databricks bundle run clinical_trials_job -t dev --profile monicaenders
databricks bundle run clinical_trials_agent_job -t dev --profile monicaenders

# Local transform test: needs PySpark 4.0+, Java 17+, Python <= 3.13
python tests/test_transforms.py
```

Deploying deletes any job in this bundle's state that isn't in the config, so check
`databricks bundle summary` before removing resources.
