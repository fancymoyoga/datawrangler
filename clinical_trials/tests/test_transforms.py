"""Local PySpark test of the pipeline transforms against synthetic API envelopes.

Needs PySpark 4.0+ and Java 17+. Run from the bundle root:

    python tests/test_transforms.py

Files are written with the real ingest envelope code, read back with binaryFile like
Auto Loader does, and pushed through bronze → silver → gold.
"""

import copy
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from functools import reduce

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "src"), os.path.join(ROOT, "src", "pipeline")]

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import transforms as t  # noqa: E402
from ctgov_client import build_envelope, write_envelope  # noqa: E402

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def study(nct_id, title="Ibrutinib in Waldenstrom Macroglobulinemia", status="RECRUITING",
          last_update="2024-01-10", **extra):
    s = {
        "protocolSection": {
            "identificationModule": {
                "nctId": nct_id,
                "briefTitle": title,
                "officialTitle": f"A Phase II Study of {title}",
                "organization": {"fullName": "Example Cancer Center", "class": "OTHER"},
            },
            "statusModule": {
                "overallStatus": status,
                "startDateStruct": {"date": "2018-05"},
                "primaryCompletionDateStruct": {"date": "2022-11-30"},
                "completionDateStruct": {"date": "2023-12"},
                "studyFirstPostDateStruct": {"date": "2018-04-23"},
                "lastUpdatePostDateStruct": {"date": last_update},
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "Example Cancer Center", "class": "OTHER"},
                "responsibleParty": {
                    "type": "PRINCIPAL_INVESTIGATOR",
                    "investigatorFullName": "John Smith",
                    "investigatorTitle": "Professor of Medicine",
                    "investigatorAffiliation": "Example Cancer Center",
                },
            },
            "descriptionModule": {
                "briefSummary": "This phase II trial studies how well ibrutinib and ixazomib work "
                                "in treating patients with Waldenstrom macroglobulinemia.",
                "detailedDescription": "PRIMARY OBJECTIVE: To evaluate the overall response rate. "
                                       "Patients receive treatment from 2018-05-15 to 2020-01-01.",
            },
            "conditionsModule": {"conditions": ["Waldenstrom Macroglobulinemia"],
                                 "keywords": ["ibrutinib", "ixazomib"]},
            "designModule": {"studyType": "INTERVENTIONAL", "phases": ["PHASE2"],
                             "enrollmentInfo": {"count": 26, "type": "ACTUAL"}},
            "armsInterventionsModule": {"interventions": [
                {"type": "DRUG", "name": "Ibrutinib", "description": "Given PO"},
                {"type": "DRUG", "name": "Ixazomib Citrate"},
                {"type": "OTHER", "name": "Laboratory Biomarker Analysis"},
            ]},
            "outcomesModule": {
                "primaryOutcomes": [{"measure": "Overall response rate", "timeFrame": "Up to 2 years"}],
                "secondaryOutcomes": [{"measure": "Progression-free survival"}],
            },
            "eligibilityModule": {
                "eligibilityCriteria": "Inclusion Criteria:\n* Age >= 18 years\n"
                                       "* Questions: call 555-123-4567 or email trials@example.org",
                "healthyVolunteers": False,
                "sex": "ALL",
                "minimumAge": "18 Years",
                "maximumAge": "75 Years",
            },
            "contactsLocationsModule": {
                "centralContacts": [{"name": "  Jane   DOE ", "role": "CONTACT",
                                     "phone": "(555) 123-4567", "phoneExt": "x12",
                                     "email": "Jane.Doe@Example.ORG"}],
                "overallOfficials": [{"name": "Dr. John Smith", "affiliation": "Example Cancer Center",
                                      "role": "PRINCIPAL_INVESTIGATOR"}],
                "locations": [
                    {"facility": "Example Cancer Center", "status": "RECRUITING", "city": "Boston",
                     "state": "Massachusetts", "zip": "02215", "country": "United States",
                     "geoPoint": {"lat": 42.3375, "lon": -71.1081},
                     "contacts": [{"name": "Jane Doe", "role": "CONTACT",
                                   "email": "jane.doe@example.org", "phone": "555.123.4567"}]},
                    {"facility": "Example Cancer Center", "status": "RECRUITING", "city": "Boston",
                     "state": "Massachusetts", "zip": "02215", "country": "United States",
                     "geoPoint": {"lat": 42.3375, "lon": -71.1081}},
                    {"facility": "Hospital Without Coordinates", "city": "Lyon", "country": "France"},
                ],
            },
        },
        "hasResults": True,
    }
    for path, value in extra.items():
        node = s
        *parents, leaf = path.split(".")
        for p in parents:
            node = node.setdefault(p, {})
        node[leaf] = value
    return s


def land(root, nct_id, status, payload, minutes):
    """Write an envelope the same way the ingest job does."""
    at = T0 + timedelta(minutes=minutes)
    url = f"https://clinicaltrials.gov/api/v2/studies/{nct_id}"
    body = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
    env = build_envelope(nct_id, url, status, body, at, f"run-{minutes}")
    write_envelope(root, env, at)


def build_landing(root):
    v1 = study("NCT00000001")
    v2 = study("NCT00000001", status="ACTIVE_NOT_RECRUITING", last_update="2024-06-01")
    land(root, "NCT00000001", 200, v1, 0)                       # version 1
    land(root, "NCT00000001", 200, v2, 10)                      # version 2
    land(root, "NCT00000001", 200, copy.deepcopy(v2), 20)       # repeat pull of version 2
    land(root, "NCT00000404", 404, "NCT number NCT00000404 not found", 0)
    land(root, "NCT00000002", 200, study("NCT00000003"), 0)     # ID mismatch
    land(root, "NCT00000005", 200, study("NCT00000005", title="   "), 0)  # blank title
    land(root, "NCT00000006", 200, study(                       # warnings, other age units
        "NCT00000006", status="SOMETHING_NEW",
        **{"protocolSection.eligibilityModule.minimumAge": "6 Months",
           "protocolSection.eligibilityModule.maximumAge": "N/A",
           "protocolSection.statusModule.completionDateStruct": {"date": "2017-01"},
           "protocolSection.designModule.enrollmentInfo": {"count": -1}}), 0)
    land(root, "NCT00000007", 200, {"protocolSection": {        # valid but too short for gold
        "identificationModule": {"nctId": "NCT00000007", "briefTitle": "Tiny"},
        "statusModule": {"overallStatus": "COMPLETED",
                         "lastUpdatePostDateStruct": {"date": "2025-02"}}}}, 0)
    folder = os.path.join(root, "NCT00000008")                  # malformed JSON file
    os.makedirs(folder)
    with open(os.path.join(folder, "20261001T120000Z_NCT00000008.json"), "w") as f:
        f.write('{"requested_nct_id": "NCT00000008", "http_status": 200, "payload": {"protocolSe')


def binary_files(spark, root):
    """Rows shaped like the binaryFile source (Auto Loader's input to bronze).

    Built in Python because Spark's local file listing needs Hadoop native binaries on Windows.
    """
    rows = []
    for folder, _, names in os.walk(root):
        for name in names:
            if name.endswith(".json") and not name.startswith("_"):
                path = os.path.join(folder, name)
                with open(path, "rb") as f:
                    content = f.read()
                modified = datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)
                rows.append(("file:/" + path.replace(os.sep, "/"), modified, len(content), bytearray(content)))
    return literal_df(spark, rows, ["path", "modificationTime", "length", "content"])


def literal_df(spark, rows, names):
    """DataFrame from literals, without starting a Python worker (they crash on Windows)."""
    frames = [spark.range(1).select(*[F.lit(v).alias(n) for v, n in zip(row, names)]) for row in rows]
    return reduce(DataFrame.unionByName, frames)


def show(title, df, *cols, truncate=60):
    print(f"\n=== {title} ({df.count()} rows)")
    (df.select(*cols) if cols else df).show(50, truncate=truncate, vertical=False)


def main():
    spark = (SparkSession.builder.master("local[2]").appName("transforms-test")
             .config("spark.sql.ansi.enabled", "true")          # serverless default
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    root = tempfile.mkdtemp(prefix="ctgov_test_")
    build_landing(root)
    files = binary_files(spark, root)

    bronze = t.bronze_from_binary(files).cache()
    parsed = t.parse_studies(bronze).cache()
    validated = t.apply_critical_rules(t.validated(parsed)).cache()
    quarantine = t.quarantine(parsed)
    studies = t.apply_scd1(validated).cache()
    contacts = t.contacts(studies)
    locations = t.locations(studies)
    gold_all = t.documents(studies).cache()
    gold = gold_all.where(" AND ".join(t.GOLD_RULES.values()))

    show("bronze_studies_raw", bronze, F.regexp_extract("source_file", r"([^/\\]+)$", 1).alias("file"),
         "file_bytes", F.substring("file_sha256", 1, 12).alias("sha256"))
    show("silver_studies_validated", validated, "nct_id", "overall_status", "last_update_posted_date",
         "ingested_at", F.substring("payload_hash", 1, 12).alias("payload_hash"), "dq_warnings",
         truncate=False)
    show("silver_studies_quarantine", quarantine, "requested_nct_id", "nct_id", "http_status",
         "failed_rules", F.substring("raw_json_snippet", 1, 50).alias("raw_json_snippet"), truncate=False)
    show("silver_studies", studies, "nct_id", "overall_status", "start_date", "completion_date",
         "last_update_posted_date", "ingested_at", "minimum_age",
         "minimum_age_years", "maximum_age", "maximum_age_years", truncate=False)
    show("silver_studies (arrays)", studies, "nct_id", "phases", "intervention_names",
         "intervention_types", "primary_outcome_measures", "responsible_party_investigator_name")
    show("silver_study_contacts", contacts, F.substring("contact_id", 1, 12).alias("contact_id"),
         "nct_id", "name", "email", "phone", "phone_ext", "sources", truncate=False)
    show("silver_study_locations", locations, "nct_id", "facility", "city", "zip", "country", "lat", "lon",
         F.expr(t.LOCATION_RULES["valid_coordinates"]).alias("valid_coordinates"), truncate=False)
    show("gold_study_documents (before min-length rule)", gold_all, "nct_id", "document_chars")
    show("gold_study_documents", gold, "nct_id", "document_chars")
    doc = gold.where("nct_id = 'NCT00000001'").first()["document_text"]
    print("\n=== document_text for NCT00000001\n" + doc)

    # --- assertions -------------------------------------------------------
    q = {r["requested_nct_id"]: set(r["failed_rules"]) for r in quarantine.collect()}
    assert set(q) == {"NCT00000404", "NCT00000002", "NCT00000005", "NCT00000008"}, q
    assert "http_status_200" in q["NCT00000404"]
    assert q["NCT00000002"] == {"nct_id_matches_request"}
    assert q["NCT00000005"] == {"title_present"}
    assert "valid_json" in q["NCT00000008"]
    assert bronze.count() == parsed.count() == validated.count() + quarantine.count()

    s = {r["nct_id"]: r for r in studies.collect()}
    assert set(s) == {"NCT00000001", "NCT00000006", "NCT00000007"}
    one = s["NCT00000001"]
    assert one["overall_status"] == "ACTIVE_NOT_RECRUITING"  # newest of 3 pulls (2 versions)
    assert str(one["start_date"]) == "2018-05-01" and str(one["completion_date"]) == "2023-12-01"
    assert one["minimum_age_years"] == 18.0 and one["dq_warnings"] == []
    six = s["NCT00000006"]
    assert six["minimum_age_years"] == 0.5 and six["maximum_age_years"] is None
    assert set(six["dq_warnings"]) == {"known_overall_status", "enrollment_non_negative",
                                       "completion_after_start", "min_age_not_above_max_age"}

    c = contacts.where("nct_id = 'NCT00000001'").collect()
    jane = [r for r in c if r["email"] == "jane.doe@example.org"]
    assert len(jane) == 1 and jane[0]["name"].lower() == "jane doe" and jane[0]["phone"] == "5551234567"
    assert jane[0]["sources"] == ["central_contact", "location_contact"]
    assert len(c) == 3, c  # Jane Doe, Dr. John Smith, John Smith (responsible party)
    assert locations.where("nct_id = 'NCT00000001'").count() == 2  # duplicate site removed

    assert {r["nct_id"] for r in gold.collect()} == {"NCT00000001", "NCT00000006"}
    assert set(gold.columns).isdisjoint({"responsible_party_investigator_name", "payload", "raw_json"})
    assert "555-123-4567" not in doc and "trials@example.org" not in doc
    assert "[phone removed]" in doc and "[email removed]" in doc
    assert "2018-05-15 to 2020-01-01" in doc  # dates survive the phone scrub

    phone_cases = {"555-123-4567": True, "(555) 123-4567": True, "+1 555 123 4567": True,
                   "1-800-422-6237": True, "5551234567": True, "2018-05-15": False,
                   "2018-05": False, "05/15/2018": False, "2018-05-15 to 2020-01-01": False,
                   "NCT03506373": False, "20180515": False}
    rows = literal_df(spark, [(k,) for k in phone_cases], ["v"]).select(
        "v", t.scrub(F.col("v")).alias("s")).collect()
    for r in rows:
        assert (r["s"] != r["v"]) == phone_cases[r["v"]], (r["v"], r["s"])
    print("\nphone scrub cases:", {r["v"]: r["s"] for r in rows})

    print("\nALL CHECKS PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
