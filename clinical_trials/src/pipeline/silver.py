from pyspark import pipelines as dp

import transforms


@dp.temporary_view(name="silver_studies_parsed")
def silver_studies_parsed():
    # Shared by validated and quarantine, so both always see the same parse and rule results.
    return transforms.parse_studies(spark.readStream.table("bronze_studies_raw"))


@dp.table(
    name="silver_studies_validated",
    comment="Every study version that passed the critical rules; raw payload kept as VARIANT",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop(transforms.CRITICAL_RULES)
@dp.expect_all(transforms.WARNING_RULES)
def silver_studies_validated():
    return transforms.validated(spark.readStream.table("silver_studies_parsed"))


@dp.table(
    name="silver_studies_quarantine",
    comment="Envelopes that failed at least one critical rule, with the rules they failed",
)
def silver_studies_quarantine():
    return transforms.quarantine(spark.readStream.table("silver_studies_parsed"))


@dp.temporary_view(name="silver_studies_latest")
def silver_studies_latest():
    return transforms.latest_versions(spark.read.table("silver_studies_validated"))


@dp.materialized_view(
    name="silver_studies",
    comment="Current version of each study (newest last update, then newest pull)",
)
def silver_studies():
    return transforms.current_studies(spark.read.table("silver_studies_latest"))


@dp.materialized_view(
    name="silver_study_contacts",
    comment="PII: one row per person per study (central contacts, officials, site contacts, responsible party)",
)
def silver_study_contacts():
    return transforms.contacts(spark.read.table("silver_studies_latest"))


@dp.materialized_view(
    name="silver_study_locations",
    comment="One row per study site",
)
@dp.expect_all(transforms.LOCATION_RULES)
def silver_study_locations():
    return transforms.locations(spark.read.table("silver_studies_latest"))
