"""Databricks job entry point: load one row per study (from FHIR) into a Delta table.

Run by the `study_contacts` job in databricks.yml (spark_python_task). Parameters:
    --nct-ids  comma- or pipe-separated NCT IDs, e.g. "NCT04280705,NCT07649629"
    --table    three-part Unity Catalog name, e.g. "clinicaltrials.fhir_studies.studies"

For each NCT ID: fetch GET /studies/{nctId}?format=fhir.json, parse it with
parse_study_fhir(), and replace that study's row in the table. `locations`,
`central_contacts` and `overall_officials` are array<struct> columns. Re-running for the
same IDs is idempotent. Fails the run if any ID can't be fetched, after loading the rest.
"""

import argparse
import os
import re
import sys

# spark_python_task doesn't put the script's folder on sys.path; add it so the sibling
# module (synced by the bundle) can be imported.
sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0])))

import requests  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import current_timestamp  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fetch_study_fhir import (  # noqa: E402
    STUDY_COLUMNS,
    ApiError,
    check_api_version,
    fetch_study_fhir,
    parse_study_fhir,
    validate_nct_id,
)

TABLE_NAME_PATTERN = r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+"


def _strings(*names: str) -> list[StructField]:
    return [StructField(n, StringType()) for n in names]


CONTACT = StructType(_strings("name", "role", "phone", "email"))
LOCATION = StructType(
    _strings("facility", "site_status", "city", "state", "zip", "country")
    + [
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("contacts", ArrayType(CONTACT)),
    ]
)
OFFICIAL = StructType(_strings("name", "role", "affiliation"))

# Explicit schema: Spark can't infer types for null fields or empty lists. Field order
# must match STUDY_COLUMNS. loaded_at is added by Spark in write_table: serverless
# (Spark Connect) rejects tz-naive pandas/Python timestamps.
SCHEMA = StructType(
    _strings("nct_id", "brief_title", "official_title", "eligibility_criteria")
    + [
        StructField("location_count", IntegerType()),
        StructField("locations", ArrayType(LOCATION)),
        StructField("central_contacts", ArrayType(CONTACT)),
        StructField("overall_officials", ArrayType(OFFICIAL)),
    ]
)
assert [f.name for f in SCHEMA.fields] == STUDY_COLUMNS


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


def build_records(nct_ids: list[str]) -> tuple[list[dict], list[str]]:
    """Fetch and parse each study. Returns (one record per fetched study, failed IDs)."""
    records, failed = [], []
    with requests.Session() as session:
        check_api_version(session)  # stop the whole run on spec drift
        for nct_id in nct_ids:
            try:
                record = parse_study_fhir(fetch_study_fhir(session, nct_id))
            except (ApiError, requests.RequestException) as exc:
                print(f"FAILED {nct_id}: {exc}", file=sys.stderr)
                failed.append(nct_id)
                continue
            # Key rows by the requested ID; an alias ID redirects to the current record.
            record["nct_id"] = record["nct_id"] or nct_id
            print(f"{nct_id}: {record['location_count']} locations")
            records.append(record)
    return records, failed


def write_table(spark: SparkSession, records: list[dict], table: str) -> None:
    """Replace the rows for the studies in `records`, leaving other studies untouched."""
    sdf = spark.createDataFrame(
        [[r[c] for c in STUDY_COLUMNS] for r in records], schema=SCHEMA
    ).withColumn("loaded_at", current_timestamp())

    if not spark.catalog.tableExists(table):
        sdf.write.format("delta").saveAsTable(table)
        return
    id_list = ", ".join(f"'{r['nct_id']}'" for r in records)  # IDs already pattern-validated
    (
        sdf.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"nct_id IN ({id_list})")
        .option("mergeSchema", "true")  # adds new columns to the table
        .saveAsTable(table)
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    records, failed = build_records(args.nct_ids)

    if records:
        write_table(SparkSession.builder.getOrCreate(), records, args.table)
        print(f"Wrote {len(records)} studies to {args.table}")

    if failed:
        print(f"{len(failed)} of {len(args.nct_ids)} studies failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    # Databricks runs spark_python_task inside IPython, where any sys.exit() (even 0) is
    # reported as an exception and fails the run. Return normally on success; raise on failure.
    if main() != 0:
        raise RuntimeError("study_contacts load failed; see the FAILED lines above")
