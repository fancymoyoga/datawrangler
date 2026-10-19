# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest studies from the ClinicalTrials.gov v2 API
# MAGIC
# MAGIC Fetches each NCT ID and lands one JSON envelope per study per run at
# MAGIC `{landing_path}/{NCT_ID}/{utc_timestamp}_{NCT_ID}.json`. Non-200 responses are landed too,
# MAGIC so silver can quarantine them.
# MAGIC
# MAGIC Parameters:
# MAGIC - `nct_ids`: comma-, semicolon- or space-separated NCT IDs
# MAGIC - `landing_path`: folder in the landing volume, e.g. `/Volumes/workspace/<schema>/landing/studies`

# COMMAND ----------

import json
import os

from ctgov_client import fetch_all, parse_nct_ids

dbutils.widgets.text("nct_ids", "NCT03506373")
dbutils.widgets.text("landing_path", "")

nct_ids = parse_nct_ids(dbutils.widgets.get("nct_ids"))
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
if not landing_path.startswith("/Volumes/"):
    raise ValueError(f"landing_path must be a /Volumes/... path, got {landing_path!r}")

# COMMAND ----------

written, unreachable = fetch_all(nct_ids, landing_path)

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
    "requested": len(nct_ids),
    "landed": len(written),
    "non_200": [{"nct_id": i, "http_status": s} for i, s, _ in written if s != 200],
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
            f"--nct-ids \"{','.join(nct_ids)}\"\n"
            "then re-run this job (it will pass once files exist) or run the pipeline directly."
        )
    print(
        f"WARNING: ClinicalTrials.gov API is unreachable ({unreachable}). "
        f"Skipped {len(nct_ids) - len(written)} of {len(nct_ids)} IDs; continuing with the "
        f"{existing} file(s) already in {landing_path}. "
        "Run scripts/fetch_and_upload.py locally to refresh them."
    )

print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
