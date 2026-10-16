"""Databricks job entry point: load FHIR contacts/locations for NCT IDs into a Delta table.

Run by the `study_contacts` job in databricks.yml (spark_python_task). Parameters:
    --nct-ids  comma- or pipe-separated NCT IDs, e.g. "NCT04280705,NCT07649629"
    --table    three-part Unity Catalog name, e.g. "clinical_trials.fhir_studies.study_contacts_locations"

For each NCT ID: fetch GET /studies/{nctId}?format=fhir.json, flatten it with
fhir_contacts_locations_dataframe(), and replace that study's rows in the table. Re-running
for the same IDs is idempotent. Fails the run if any ID can't be fetched, after loading the rest.
"""

import argparse
import os
import re
import sys

# spark_python_task doesn't put the script's folder on sys.path; add it so the sibling
# module (synced by the bundle) can be imported.
sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0])))

import pandas as pd  # noqa: E402
import requests  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import current_timestamp  # noqa: E402
from pyspark.sql.types import DoubleType, StringType, StructField, StructType  # noqa: E402

from fetch_study_fhir import (  # noqa: E402
    CONTACT_COLUMNS,
    ApiError,
    check_api_version,
    fetch_study_fhir,
    fhir_contacts_locations_dataframe,
    validate_nct_id,
)

TABLE_NAME_PATTERN = r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+"

# Explicit schema: Spark can't infer a type for columns that are all null (common for
# lat/lon, site_status, affiliation in small studies). loaded_at is added by Spark in
# write_table, not from pandas: serverless (Spark Connect) rejects tz-naive pandas Timestamps.
SCHEMA = StructType(
    [StructField(c, DoubleType() if c in ("lat", "lon") else StringType()) for c in CONTACT_COLUMNS]
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--nct-ids", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args(argv)

    ids = [i for i in re.split(r"[,|\s]+", args.nct_ids) if i]
    if not ids:
        parser.error("--nct-ids is empty")
    # Validate every ID before any request; normalise to upper case so replaceWhere matches.
    args.nct_ids = list(dict.fromkeys(validate_nct_id(i).upper() for i in ids))
    if not re.fullmatch(TABLE_NAME_PATTERN, args.table):
        parser.error(f"--table must be catalog.schema.table, got {args.table!r}")
    return args


def build_frame(nct_ids: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """Fetch and flatten each study. Returns (rows for all fetched studies, failed IDs)."""
    frames, failed = [], []
    with requests.Session() as session:
        check_api_version(session)  # stop the whole run on spec drift
        for nct_id in nct_ids:
            try:
                frame = fhir_contacts_locations_dataframe(fetch_study_fhir(session, nct_id))
            except (ApiError, requests.RequestException) as exc:
                print(f"FAILED {nct_id}: {exc}", file=sys.stderr)
                failed.append(nct_id)
                continue
            # Key rows by the requested ID; an alias ID redirects to the current record.
            frame["nct_id"] = frame["nct_id"].fillna(nct_id)
            print(f"{nct_id}: {len(frame)} rows")
            frames.append(frame)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CONTACT_COLUMNS)
    return combined, failed


def write_table(spark: SparkSession, frame: pd.DataFrame, table: str) -> None:
    """Replace the rows for the studies in `frame`, leaving other studies untouched."""
    records = frame[CONTACT_COLUMNS].astype(object).where(frame.notna(), None).to_dict("records")
    sdf = spark.createDataFrame(records, schema=SCHEMA).withColumn("loaded_at", current_timestamp())

    loaded_ids = sorted(frame["nct_id"].dropna().unique())
    if not spark.catalog.tableExists(table):
        sdf.write.format("delta").saveAsTable(table)
        return
    id_list = ", ".join(f"'{i}'" for i in loaded_ids)  # IDs already pattern-validated
    (
        sdf.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"nct_id IN ({id_list})")
        .saveAsTable(table)
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    frame, failed = build_frame(args.nct_ids)

    if not frame.empty:
        write_table(SparkSession.builder.getOrCreate(), frame, args.table)
        print(f"Wrote {len(frame)} rows for {frame['nct_id'].nunique()} studies to {args.table}")

    if failed:
        print(f"{len(failed)} of {len(args.nct_ids)} studies failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
