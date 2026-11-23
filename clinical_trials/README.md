# Clinical trials lakehouse

Pulls clinical trial records from the [ClinicalTrials.gov REST API v2](https://clinicaltrials.gov/data-api/api)
into a medallion lakehouse on Databricks (bronze → silver → gold). The gold layer feeds a
retrieval agent, a CLL / Waldenström macroglobulinemia (WM) study cohort, and a model that scores
how likely an ongoing CLL trial is to stop early.

Everything deploys as a Databricks Asset Bundle and runs on serverless compute, so it works on
Databricks Free Edition.

## How it works

```
ClinicalTrials.gov /studies
        │  01_ingest_api.py (paged search, retries, /version check)
        ▼
landing volume  ──►  bronze_studies_raw          one row per landed page (Auto Loader, binaryFile)
                        │
                        ▼
                     silver_studies_validated / _quarantine   data-quality rules
                     silver_studies                            SCD Type 1 on nct_id
                     silver_study_{contacts, locations, conditions,
                                   eligibility_criteria, outcomes, outcome_results}
                        │
                        ▼
                     gold_study_documents          PII-free, AI-ready text (CDF on)
                     gold_cll_wm_*                 CLL / WM cohort and child tables
                     gold_cll_trial_features       model features
                        │
        ┌───────────────┼──────────────────────────┐
        ▼               ▼                          ▼
 02_tag_pii.py    03_vector_index.py         06_train_cll_trial_model.py
 UC PII tags,     Delta Sync index for       completion model + risk scores
 gold PII gate    the agent (04/05)
```

| Layer | What it holds |
|---|---|
| **Landing** | One JSON envelope per API page (`source_url`, `http_status`, `ingested_at`, `ingest_id`, `query`, `page`, `payload`). Written as `_*.tmp` and renamed, so Auto Loader never reads a partial file. |
| **Bronze** | Raw files, nothing parsed or dropped. |
| **Silver** | One row per study, validated (critical rules drop, warnings are recorded), failures in a quarantine table, deduplicated on `nct_id`. Child tables split out contacts (PII), locations, conditions, individual inclusion/exclusion criteria, planned outcomes and posted results per arm. |
| **Gold** | Allow-listed columns only, emails and phone numbers scrubbed, short documents dropped. The PII tagging task fails the run if a direct identifier reaches any `gold_*` table. |

### Incremental pulls

Each ingest run is recorded in `ingest_runs`, keyed on condition + search term. Once a search has
a complete run (it reached the last page with no error or cap), the next run with
`incremental=true` asks only for studies updated since then, with one day of overlap. Set
`incremental=false` to re-fetch everything, for example after adding fields.

### CLL trial completion model

`06_train_cll_trial_model.py` predicts whether a CLL trial ends early (terminated or withdrawn)
rather than completing, using design-time features only: phase, design, sponsor type, drug class
flags and eligibility criteria counts. Enrollment, results, later dates and site lists are left out
because they leak the outcome, and training is limited to trials started by `max_train_start_year`
(default 2019). It picks logistic regression or gradient boosting by cross-validated AUC, registers
the winner as `cll_trial_completion_model@champion`, and writes `gold_cll_trial_risk_scores` and
`gold_cll_drug_class_summary`.

It models trial operations, not patient outcomes or how well a treatment works.

## Project layout

```
clinical_trials/
├── databricks.yml              bundle, variables, dev target
├── resources/                  schemas, landing volume, pipeline, jobs
├── src/
│   ├── ctgov_client.py         stdlib API client and envelope writer
│   ├── 01_ingest_api.py        land search pages in the volume
│   ├── 02_tag_pii.py           Unity Catalog PII tags and gold PII gate
│   ├── 03_vector_index.py      Vector Search source table and index
│   ├── 04_log_agent.py         log, register and test the agent
│   ├── 05_test_agent.py        ask the agent questions
│   ├── 06_train_cll_trial_model.py
│   ├── agent/agent.py          MLflow ResponsesAgent (retrieval + Llama 3.3 70B)
│   └── pipeline/               bronze.py, silver.py, gold.py, transforms.py
├── scripts/fetch_and_upload.py local fallback when serverless can't reach the API
├── tests/test_transforms.py    local PySpark test of the transforms
├── trial_match/                screen open WM trials against a personal profile
├── study_dataframe/            one study → pandas DataFrames
├── fetch_study_fhir.py         one study as a FHIR Bundle or CSV row
└── load_study_contacts.py      FHIR loader run by the study_contacts job
```

## Getting started

### Prerequisites

- Databricks CLI (tested with v1.21), authenticated to your workspace
  (`databricks auth login --host <workspace-url> --profile <name>`)
- A Unity Catalog catalog named `clinicaltrials` (bronze, silver and the CLL / WM tables), plus the
  default `workspace` catalog. The bundle creates the schemas and the landing volume itself.
- Python 3.10+ for the local scripts

Check the API is up and still on the spec version this code was written against (`2.0.5`):

```bash
curl -s "https://clinicaltrials.gov/api/v2/version"
```

### Deploy and run

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle plan -t dev --profile <profile>      # look for unexpected deletes
databricks bundle deploy -t dev --profile <profile>

databricks bundle run clinical_trials_job -t dev --profile <profile>
databricks bundle run clinical_trials_agent_job -t dev --profile <profile>
databricks bundle run cll_trial_model_job -t dev --profile <profile>   # after clinical_trials_job
```

A deploy deletes any resource in the bundle's state that is no longer in the config, so always
read the `plan` output first.

In the `dev` target, resource names get a `dev_<user>_` prefix (for example
`dev_<user>_clinical_trials`).

### Job parameters (`clinical_trials_job`)

| Parameter | Default | Meaning |
|---|---|---|
| `condition` | `lung cancer` | `query.cond` search (Essie syntax) |
| `search` | blank | `query.term` free-text search. With both set, studies must match both. At least one is required. |
| `max_studies` | blank | Cap on studies per run. Blank lands every match. A capped run never counts as complete. |
| `incremental` | `true` | After a complete run, fetch only studies updated since it |
| `use_ai_pii_check` | `true` | After the name and regex rules, sample string columns with `ai_classify` to catch more PII (skipped if the function is unavailable) |

Example: pull CLL trials.

```bash
databricks bundle run clinical_trials_job -t dev --profile <profile> \
  --params condition="chronic lymphocytic leukemia"
```

## Local tools

These run without Databricks.

```bash
# One study as a FHIR Bundle, or as a one-row CSV (needs pandas)
python fetch_study_fhir.py NCT04280705 --out raw/NCT04280705.fhir.json
python fetch_study_fhir.py NCT04280705 --csv raw/NCT04280705_study.csv

# One study as pandas DataFrames
cd study_dataframe && python -m venv .venv && .venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python study_dataframe.py NCT04280705 --csv-dir raw

# Screen open WM trials against a profile (copy trial_match/profile.example.json to raw/ first)
python trial_match/match_wm_trials.py raw/wm_profile.json --out-dir raw
```

`trial_match` sorts open WM trials into *possible now*, *possible when treated*, *likely not
eligible* and *not eligible* by reading each trial's eligibility criteria. It's a rule-based screen,
not medical advice; only a trial team can decide eligibility. Profiles and reports hold health data,
so keep them in `raw/`, which git ignores.

## Tests

```bash
python tests/test_transforms.py    # needs PySpark 4.0+, Java 17+, Python <= 3.13
```

## Notes and known gaps

- Results are unsorted by the API unless `sort` is set; ingestion uses `sort=@relevance`.
- Studies landed before posted results were added to the requested fields have no results until
  re-fetched with `incremental=false`.
- The `known_overall_status` silver rule hard-codes the status list instead of loading
  `/studies/enums` at run time.
- Free Edition can't build a Vector Search index on a materialized view, so gold is copied into the
  Delta table `gold_study_documents_vs_source` and the index syncs from that.
- Raw API dumps go in `raw/` (git-ignored) and are never committed.

See [CLAUDE.md](CLAUDE.md) for the detailed API rules and build notes.
