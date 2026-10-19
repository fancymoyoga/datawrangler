# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest studies from the ClinicalTrials.gov v2 API
# MAGIC
# MAGIC Searches `GET /studies?query.cond=<condition>` (sorted by relevance, following
# MAGIC `nextPageToken`) and lands up to `max_studies` studies, one JSON envelope per study per run,
# MAGIC at `{landing_path}/{NCT_ID}/{utc_timestamp}_{NCT_ID}.json`. A failed page is landed too
# MAGIC (folder `QUERY_ERROR`) so silver can quarantine it.
# MAGIC
# MAGIC Parameters:
# MAGIC - `condition`: condition or disease to search for, e.g. `lung cancer` (Essie syntax)
# MAGIC - `max_studies`: cap on studies landed per run
# MAGIC - `landing_path`: folder in the landing volume, e.g. `/Volumes/workspace/<schema>/landing/studies`

# COMMAND ----------

import json
import os

from ctgov_client import fetch_search

dbutils.widgets.text("condition", "lung cancer")
dbutils.widgets.text("max_studies", "100")
dbutils.widgets.text("landing_path", "")

condition = dbutils.widgets.get("condition")
max_studies = int(dbutils.widgets.get("max_studies"))
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
if not landing_path.startswith("/Volumes/"):
    raise ValueError(f"landing_path must be a /Volumes/... path, got {landing_path!r}")

# COMMAND ----------

result, unreachable = fetch_search(condition, landing_path, max_studies)
written = result["written"]

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
    "total_matching": result["total_count"],
    "pages": result["pages"],
    "landed": len(written),
    "non_200": [{"path": p, "http_status": s} for _, s, p in written if s != 200],
}

if unreachable:
    existing = landed_file_count(landing_path)
    summary.update(api_unreachable=str(unreachable), existing_files=existing)
    if not existing:
        raise RuntimeError(
            f"ClinicalTrials.gov API is unreachable from this compute ({unreachable}) and "
            f"{landing_path} has no landed files yet, so the pipeline has nothing to read.\n"
            "Free Edition can restrict outbound internet. Land the data from your machine instead:\n"
            f"  python scripts/fetch_and_upload.py --profile <PROFILE> --landing-path {landing_path} "
            f"--condition \"{condition}\" --max-studies {max_studies}\n"
            "then re-run this job (it will pass once files exist) or run the pipeline directly."
        )
    print(
        f"WARNING: ClinicalTrials.gov API is unreachable ({unreachable}). "
        f"Landed {len(written)} studies before it dropped; continuing with the "
        f"{existing} file(s) already in {landing_path}. "
        "Run scripts/fetch_and_upload.py locally to refresh them."
    )

print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
