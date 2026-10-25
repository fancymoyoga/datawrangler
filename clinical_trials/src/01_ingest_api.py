# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest studies from the ClinicalTrials.gov v2 API
# MAGIC
# MAGIC Searches `GET /studies?query.cond=<condition>` (sorted by relevance, following
# MAGIC `nextPageToken`, 1,000 studies per page) and lands each page as one JSON envelope at
# MAGIC `{landing_path}/search/{utc_timestamp}_{ingest_id}_page{n}.json`. A failed page is landed
# MAGIC too, so silver can quarantine it.
# MAGIC
# MAGIC **Incremental pulls:** every run is recorded in `ingest_runs`. Once a run has fetched every
# MAGIC match for a condition, later runs only ask for studies whose last update was posted since
# MAGIC then (`filter.advanced=AREA[LastUpdatePostDate]RANGE[<date>,MAX]`). Studies fetched twice are
# MAGIC harmless: silver's Auto CDC keeps one current row per study.
# MAGIC
# MAGIC Parameters:
# MAGIC - `condition`: condition or disease to search for, e.g. `lung cancer` (Essie syntax)
# MAGIC - `max_studies`: cap on studies landed per run; blank lands every match. A capped run is
# MAGIC   never complete, so it doesn't start incremental pulls.
# MAGIC - `incremental`: `true` to fetch only updates since the last complete run, `false` to fetch
# MAGIC   every match again
# MAGIC - `landing_path`: folder in the landing volume, e.g. `/Volumes/workspace/<schema>/landing/studies`
# MAGIC - `catalog`, `schema`: where `ingest_runs` is kept

# COMMAND ----------

import json
import os
from datetime import timedelta

from ctgov_client import fetch_search

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "clinical_trials")
dbutils.widgets.text("condition", "lung cancer")
dbutils.widgets.text("max_studies", "")
dbutils.widgets.text("incremental", "true")
dbutils.widgets.text("landing_path", "")

condition = dbutils.widgets.get("condition").strip()
max_studies_text = dbutils.widgets.get("max_studies").strip()
max_studies = int(max_studies_text) if max_studies_text else None
incremental = dbutils.widgets.get("incremental").strip().lower() == "true"
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
if not landing_path.startswith("/Volumes/"):
    raise ValueError(f"landing_path must be a /Volumes/... path, got {landing_path!r}")

RUNS_TABLE = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('schema')}`.ingest_runs"
RUNS_SCHEMA = ("ingest_id STRING, condition STRING, started_at TIMESTAMP, updated_since DATE, "
               "max_studies INT, total_count INT, pages INT, studies_landed INT, files_landed INT, "
               "complete BOOLEAN, error STRING")
# LastUpdatePostDate is a US calendar date, so a run early in the UTC day can be "yesterday"
# there. Starting a day before the last complete run catches updates posted after it ran.
OVERLAP = timedelta(days=1)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {RUNS_TABLE} ({RUNS_SCHEMA}) "
          "COMMENT 'One row per ingest run; the newest complete run per condition starts the next "
          "incremental pull'")
last_complete = spark.sql(
    f"SELECT max(started_at) AS started_at FROM {RUNS_TABLE} WHERE condition = :condition AND complete",
    args={"condition": condition},
).first().started_at
updated_since = last_complete.date() - OVERLAP if incremental and last_complete else None
print(f"last complete run for {condition!r}: {last_complete}; "
      + (f"fetching studies updated since {updated_since}" if updated_since else "fetching every match"))

# COMMAND ----------

result, unreachable = fetch_search(condition, landing_path, max_studies, updated_since)
written = result["written"]
non_200 = [{"page": page, "http_status": status, "path": path}
           for page, status, _, path in written if status != 200]
complete = result["complete"] and not unreachable
if unreachable:
    error = str(unreachable)
elif non_200:
    error = f"HTTP {non_200[0]['http_status']} on page {non_200[0]['page']}"
else:
    error = None

spark.createDataFrame([(
    result["ingest_id"], condition, result["ingested_at"], updated_since, max_studies,
    result["total_count"], result["pages"], result["studies"], len(written), complete, error,
)], RUNS_SCHEMA).write.mode("append").saveAsTable(RUNS_TABLE)

# COMMAND ----------


def landed_file_count(root):
    """Count landed envelopes (skips in-progress '_' temp files)."""
    if not os.path.isdir(root):
        return 0
    return sum(
        1
        for _, _, files in os.walk(root)
        for name in files
        if name.endswith(".json") and not name.startswith("_")
    )


summary = {
    "condition": condition,
    "ingest_id": result["ingest_id"],
    "updated_since": str(updated_since) if updated_since else None,
    "total_matching": result["total_count"],
    "pages": result["pages"],
    "studies_landed": result["studies"],
    "files_landed": len(written),
    "complete": complete,
    "non_200": non_200,
}

if unreachable:
    existing = landed_file_count(landing_path)
    summary.update(api_unreachable=str(unreachable), existing_files=existing)
    if not existing:
        cap = f" --max-studies {max_studies}" if max_studies else ""
        raise RuntimeError(
            f"ClinicalTrials.gov API is unreachable from this compute ({unreachable}) and "
            f"{landing_path} has no landed files yet, so the pipeline has nothing to read.\n"
            "Free Edition can restrict outbound internet. Land the data from your machine instead:\n"
            f"  python scripts/fetch_and_upload.py --profile <PROFILE> --landing-path {landing_path} "
            f"--condition \"{condition}\"{cap}\n"
            "then re-run this job (it will pass once files exist) or run the pipeline directly."
        )
    print(
        f"WARNING: ClinicalTrials.gov API is unreachable ({unreachable}). "
        f"Landed {result['studies']} studies before it dropped; continuing with the "
        f"{existing} file(s) already in {landing_path}. "
        "Run scripts/fetch_and_upload.py locally to refresh them."
    )

print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
